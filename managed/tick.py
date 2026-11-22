"""Cooperative tick deadline and content-free progress; never cancel an operation."""
from contextlib import contextmanager, nullcontext
from .run_log import phase_timing
from contextvars import ContextVar
import json
import math
import os
import sys
import time

from .queue import canonical, require

# Seconds a reserved stage may run past the tick deadline to use its own slice.
RESERVE_GRACE=float(os.environ.get('OUTREACH_RESERVE_GRACE_SECONDS','30'))


_artifact_time_left=ContextVar('artifact_time_left',default=None)


@contextmanager
def artifact_io_budget(clock,deadline):
    token=_artifact_time_left.set(lambda:deadline-clock())
    try:yield
    finally:_artifact_time_left.reset(token)


def artifact_io_timeout(default=None):
    time_left=_artifact_time_left.get()
    if time_left is None:return default
    remaining=time_left()
    if remaining<=0:raise TimeoutError('Artifact collection deadline reached')
    return remaining if default is None else min(remaining,default)


class Tick:
    def __init__(self,seconds=None,clock=None):
        self.clock=clock or time.monotonic;self.started=self.clock()
        seconds=float(os.environ.get('OUTREACH_TICK_BUDGET_SECONDS','540') if seconds is None else seconds)
        require(math.isfinite(seconds) and seconds>=0,'Invalid tick budget seconds')
        self.deadline=self.started+seconds;self.previous={};self.pending={}
        self.count=0;self.exhausted=False;self.resume=None;self.stages=[];self.current=None;self.timings={}
        self.reserves={};self.finished=set();self.stage_started={};self.floors=set();self.dedicated=set();self.unheld=set()

    def bind(self,ledger):
        self.ledger=ledger;ledger.tick=self
        row=ledger.tasks.db.execute("SELECT payload FROM pipeline_state WHERE key='tick_deferred'").fetchone()
        saved=json.loads(row[0]) if row else {}
        self.previous=saved.get('units',{});self.pending={k:list(v) for k,v in self.previous.items()}
        self.resume=saved.get('resume')

    def order(self,names):
        offset=names.index(self.resume) if self.resume in names else 0
        self.stages=names[offset:]+names[:offset]
        return self.stages

    def reserve(self,stage,seconds,floor=False,*,dedicated=False,hold=True):
        """Keep `seconds` of the budget for a stage until it has run.

        A floor extends the shared limit within RESERVE_GRACE. A dedicated
        slice uses its own deadline from the first unit, even after an overrun.
        """
        require(math.isfinite(seconds) and seconds>=0,'Invalid stage reserve seconds')
        self.reserves[stage]=seconds
        if floor:self.floors.add(stage)
        if dedicated:self.dedicated.add(stage)
        # A short dedicated slice may run on its own deadline without taking
        # shared time from the stages before it.
        if not hold:self.unheld.add(stage)

    def limit(self):
        if self.current in self.dedicated or self.current in self.floors:
            if self.current not in self.stage_started:
                self.stage_started[self.current]=self.clock()
            stage_limit=self.stage_started[self.current]+self.reserves[self.current]
            if self.current in self.dedicated:
                return stage_limit
        # Shared work leaves time for the stages that still have reserves.
        held=sum(seconds for stage,seconds in self.reserves.items()
                 if stage!=self.current and stage not in self.finished and stage not in self.unheld)
        limit=self.deadline-held
        # A floor stage always gets its own slice from its first unit, even
        # when setup plus the other stages' reserves already exceed the budget
        # (2026-11-15: sent-events ran 0 pages on every tick). The extension is
        # capped at RESERVE_GRACE past the deadline to stay inside the job timeout.
        if self.current in self.floors:
            limit=max(limit,min(stage_limit,self.deadline+RESERVE_GRACE))
        return limit

    def allow(self,lane,key):
        if self.clock()>=self.limit():
            if not self.exhausted:
                self.exhausted=True
                # Give later phases a turn before another large result batch.
                # A stage exhausted while running last only got leftover time;
                # start the next tick with it so it cannot stay last forever.
                if self.current in self.stages:
                    index=self.stages.index(self.current)
                    self.resume=(self.current if index==len(self.stages)-1 else
                                 self.stages[(index+1)%len(self.stages)])
            pending=self.pending.setdefault(lane,[])
            if key not in pending:pending.append(key)
            return False
        pending=self.pending.get(lane,[])
        if key in pending:pending.remove(key)
        self.count+=1
        return True

    @contextmanager
    def phase(self,name):
        start=self.clock();count=self.count
        try:yield
        finally:
            seconds, units = self.clock()-start, self.count-count
            print(f'tick-phase {name} {seconds:.3f}s units={units}', file=sys.stderr, flush=True)
            phase_timing(name, seconds, units)

    @contextmanager
    def measure(self,name):
        start=self.clock()
        try:yield
        finally:
            seconds,count=self.timings.get(name,(0,0))
            self.timings[name]=(seconds+self.clock()-start,count+1)

    def report(self,name):
        seconds,count=self.timings.pop(name,(0,0))
        print(f'tick-phase {name} {seconds:.3f}s units={count}',file=sys.stderr,flush=True)
        phase_timing(name, seconds, count)

    @contextmanager
    def unit(self,lane,key,step=None):
        if not self.allow(lane,key):
            yield False;return
        start=self.clock()
        try:yield True
        finally:
            elapsed=self.clock()-start
            if step and elapsed>20:
                print(f'tick-slow {key} {step} {elapsed:.3f}s',file=sys.stderr,flush=True)

    def finish(self):
        if self.clock()>=self.deadline:self.exhausted=True
        pending={k:v for k,v in self.pending.items() if v}
        db=self.ledger.tasks.db
        saved=db.execute("SELECT payload FROM pipeline_state WHERE key='tick_deferred'").fetchone()
        value=canonical({'units':pending,'resume':self.resume})
        if (pending or self.resume) and (not saved or saved[0]!=value):
            db.execute('INSERT OR REPLACE INTO pipeline_state VALUES(?,?)',('tick_deferred',value))
        elif saved and not pending and not self.resume:
            db.execute("DELETE FROM pipeline_state WHERE key='tick_deferred'")
        if self.exhausted:
            count=len({key for keys in pending.values() for key in keys})
            print(f'tick-budget exhausted after {self.clock()-self.started:.3f}s; deferred {count} tasks',
                  file=sys.stderr,flush=True)


def phase(ledger,name):
    tick=getattr(ledger,'tick',None)
    return tick.phase(name) if tick else nullcontext()


def measure(ledger,name):
    tick=getattr(ledger,'tick',None)
    return tick.measure(name) if tick else nullcontext()


def unit(ledger,lane,key,step=None):
    tick=getattr(ledger,'tick',None)
    return tick.unit(lane,key,step) if tick else nullcontext(True)


def allowed(ledger,lane,key):
    tick=getattr(ledger,'tick',None)
    return tick.allow(lane,key) if tick else True


def time_left(ledger):
    """Whether the current stage could still start a unit; records nothing."""
    tick=getattr(ledger,'tick',None)
    return tick is None or tick.clock()<tick.limit()


def units(ledger,lane,rows,key=lambda r:r['id'],timed=True,preserve_order=False,keep_pending=False):
    """Yield the rows the budget allows; the rest become the lane's deferred work.

    keep_pending: a later pass over the same lane in this tick adds to the
    deferred keys of the earlier pass instead of replacing them.
    """
    tick=getattr(ledger,'tick',None)
    if tick:
        priority={k:i for i,k in enumerate(tick.previous.get(lane,[]))}
        if not preserve_order:
            rows=sorted(rows,key=lambda r:priority.get(key(r),len(priority)))
        if not keep_pending:tick.pending[lane]=[]
    for row in rows:
        with unit(ledger,lane,key(row),lane if timed else None) as ready:
            if ready:yield row
