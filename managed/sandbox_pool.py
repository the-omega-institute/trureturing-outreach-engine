"""Ordered, ledger-backed CMA Sandbox rotation; no per-task inventory calls."""
import json
import math
import os

from .tick import units, unit
from .queue import canonical, require
from .tasks import ACTIVE, KINDS

HEALTHY={'active','paused','pausing','reviving'}
# A failed Sandbox retained because other Agents remain is rechecked at most
# this often. Each recheck lists Agents and Workspaces and often times out
# (~40-50 s), and three such Sandboxes at 30 min made almost every tick pay one.
# Their leftover Agents need an owner decision, so a slow cadence suffices.
RETAINED_RECHECK_SECONDS=6*3600
# A pool Sandbox outside HEALTHY this long (all three stayed credential_rotating
# for more than 40 h on 2026-11-08..10) no longer blocks opening another
# Sandbox within max_sandboxes; it still counts against that limit.
STALLED_SECONDS=2*3600
# An empty pool Sandbox that stays outside HEALTHY this long (one stayed
# credential_rotating for four days on 2026-11-09..13 while counting against
# max_sandboxes) is retired through the failed-Sandbox cleanup path, so the
# pool can open a working replacement.
RETIRE_STALLED_SECONDS=12*3600
# Retain account-quota evidence across ticks after the failed creation leaves
# ACTIVE, but let normal rotation resume if no fresh failure is observed.
CAPACITY_BLOCK_SECONDS=2*3600
# A lost/blocked creation must not reserve the next Sandbox indefinitely.
# After this bound, missing first-Workspace identity is quota evidence and the
# creation follows failed-Sandbox recovery instead of replaying its key forever.
POOL_WAIT_SECONDS=30*60


def group_config(config, kinds):
    priority = config.get('prioritize_correspondence_notes', False)
    require(type(priority) is bool, 'prioritize_correspondence_notes must be a boolean')
    if 'groups' not in config:
        require(not priority, 'Correspondence note priority requires a Sandbox group for reply')
        return {}, {}
    groups = config['groups']
    require(isinstance(groups, list) and groups, 'sandbox_pool.groups must be a nonempty list')
    limits = {}
    kind_groups = {}
    for group in groups:
        require(isinstance(group, dict), 'Each sandbox_pool group must be an object')
        name = group.get('name')
        limit = group.get('max_sandboxes')
        members = group.get('kinds')
        require(isinstance(name, str) and name.strip() and name not in limits,
                'Sandbox group names must be nonempty and unique')
        require(type(limit) is int and limit > 0, 'Sandbox group max_sandboxes must be positive')
        require(isinstance(members, list) and members, 'Sandbox group kinds must be a nonempty list')
        limits[name] = limit
        for kind in members:
            require(isinstance(kind, str) and kind in KINDS, 'Sandbox group kinds must be known')
            require(kind not in kind_groups, 'Task kind belongs to multiple Sandbox groups: '+kind)
            kind_groups[kind] = name
    missing = sorted(kind for kind, enabled in kinds.items() if enabled and kind not in kind_groups)
    require(not missing, 'Enabled task kinds missing from Sandbox groups: '+', '.join(missing))
    require(sum(limits.values()) <= config['max_sandboxes'],
            'Sandbox group limits exceed sandbox_pool.max_sandboxes')
    require(not priority or 'reply' in kind_groups,
            'Correspondence note priority requires a Sandbox group for reply')
    return limits, kind_groups


def task_group(row, kind_groups, correspondence):
    if row['state'] == 'queued':
        return kind_groups.get('reply' if row['id'] in correspondence else row['kind'])
    return row['data'].get('pool_group') or kind_groups.get(row['kind'])


def runtime_corrupt(agent):
    """CMA rejects every new Agent in a Sandbox whose runtime journal is corrupt.

    The Sandbox itself still reports active, so the pool must stop placing work
    there, wait for its live Agents and then retire it.
    """
    failure=(agent or {}).get('failure') or {}
    return ((agent or {}).get('status')=='failed' and failure.get('class')=='agent_runtime_rejected'
            and '_corrupt' in str(failure.get('detail','')) and str(failure.get('detail',''))[:200] or None)


def references(data, entry):
    return (entry['sandbox_id'] in {data.get('sandbox_id'),data.get('pool_sandbox_id'),
                                    data.get('owned_sandbox_id')} or
            (bool(entry.get('workspace_id')) and data.get('workspace_id')==entry['workspace_id']))


def live_reference(row, entry):
    data=row['data']
    # Reserve slots while lifecycle creation is still asynchronous. Held results
    # and failed tasks also occupy a slot until their Agent's stop is accepted
    # (stopping or later); it runs nothing more, and cleanup confirms deletion.
    cleanup=data.get('cleanup') or {}
    # CMA.retire records agent_id only after it has requested stop (or found the
    # Agent already stopping/deleting); errors mark a refused stop/delete.
    retiring=(cleanup.get('deleted') or cleanup.get('stopped') or
              cleanup.get('status') in {'stopping','stopped','deleting','deleted'} or
              (bool(cleanup.get('agent_id')) and not cleanup.get('errors')))
    return references(data,entry) and (row['state'] in ACTIVE or
                                      (bool(data.get('agent_id')) and not retiring))


def placements(rows, entry):
    """Backfill legacy usage, counting repeated execution snapshots only once."""
    count=0
    for row in rows:
        groups=[];matched=False;pending=[row['data']]
        while pending:
            data=pending.pop()
            if not isinstance(data,dict):continue
            pending.extend(data.get('execution_history',[]))
            pending.extend(data.get('sandbox_failure_history',[]))
            if not references(data,entry):continue
            matched=True
            identities={(key,data[key]) for key in ('agent_id','create_key','operation_id') if data.get(key)}
            if (data.get('cleanup') or {}).get('agent_id'):
                identities.add(('agent_id',data['cleanup']['agent_id']))
            if not identities:continue
            # Failure history may contain only agent_id, while an earlier
            # reservation has only create_key; a full snapshot connects them.
            overlapping=[group for group in groups if identities & group]
            while overlapping:
                for group in overlapping:
                    identities.update(group);groups.remove(group)
                overlapping=[group for group in groups if identities & group]
            groups.append(identities)
        count+=len(groups) or int(matched)
    return count


def ledger_agents(rows):
    """Every Agent id our ledger ever created; callers exclude live ones first."""
    found=set()
    for row in rows:
        data=row['data']
        for item in [data,*data.get('execution_history',[]),*data.get('sandbox_failure_history',[])]:
            if isinstance(item,dict):
                found.update(a for a in (item.get('agent_id'),(item.get('cleanup') or {}).get('agent_id')) if a)
    return frozenset(found)


def readoption_refusal(entry, rows):
    """Why a cleanup-list Sandbox may not return to the pool, or None.

    Only a soft rotation whose retirement CMA declined (other Agents or
    Workspaces remain) is still a usable Sandbox. Retired or retiring,
    failed, corrupt and stalled Sandboxes stay on the cleanup list, as does
    one with an unresolved cleanup error or a live Agent of ours.
    """
    cleanup = entry.get('cleanup') or {}
    if not entry.get('rotated') or entry.get('state') != 'retiring':
        return 'not a rotated Sandbox'
    if entry.get('corrupt') or entry.get('stalled'):
        return 'corrupt or stalled'
    if cleanup.get('sandbox_retired') is not False or not cleanup.get('reason') or entry.get('cleanup_error'):
        return 'CMA has not retained it'
    if any(live_reference(row, entry) for row in rows):
        return 'a live Agent of ours references it'
    return None


def summary(tasks, rows=None):
    record=tasks.db.execute("SELECT payload FROM pipeline_state WHERE key='sandbox_pool'").fetchone()
    rows=tasks.all() if rows is None else rows
    entries=json.loads(record[0]) if record else []
    return [{'sandbox_id':e['sandbox_id'],'state':e.get('state','unknown'),
             **({'group':e['group']} if 'group' in e else {}),
             'live_agents':sum(live_reference(r,e) for r in rows),'idle_since':e.get('idle_since')}
            for e in entries]


class SandboxPool:
    def __init__(self, ledger, config, kinds=None, *, paused=False, correspondence=(), rows=None):
        self.ledger=ledger
        self.kinds = kinds or {}
        self.paused = paused
        self.correspondence = correspondence
        self.max_sandboxes=config['max_sandboxes']
        self.agents_per_sandbox=config['agents_per_sandbox']
        self.rotate_after_agents=config.get('rotate_after_agents',24)
        self.quota_blocked_max_agents_placed=config.get('quota_blocked_max_agents_placed',96)
        require(type(self.max_sandboxes) is int and self.max_sandboxes>0,
                'sandbox_pool.max_sandboxes must be positive')
        require(type(self.agents_per_sandbox) is int and 0<self.agents_per_sandbox<=16,
                'sandbox_pool.agents_per_sandbox must be between 1 and 16')
        require(type(self.rotate_after_agents) is int and self.rotate_after_agents>0,
                'sandbox_pool.rotate_after_agents must be a positive integer')
        require(type(self.quota_blocked_max_agents_placed) is int and self.quota_blocked_max_agents_placed>0,
                'sandbox_pool.quota_blocked_max_agents_placed must be a positive integer')
        self.group_limits, self.kind_groups = group_config(config, self.kinds)
        self.idle_seconds=float(os.environ.get('OUTREACH_POOL_IDLE_PAUSE_SECONDS','600'))
        require(math.isfinite(self.idle_seconds) and self.idle_seconds>=0,
                'Invalid pool idle pause seconds')
        self.entries=self.read('sandbox_pool')
        self.failed=self.read('sandbox_pool_cleanup')
        rows=ledger.tasks.all() if rows is None else rows
        for entry in [*self.entries,*self.failed]:
            if 'agents_placed' not in entry:entry['agents_placed']=placements(rows,entry)
            self.draining(entry)
        if self.group_limits:
            self.migrate_groups(rows)
        self.write('sandbox_pool',self.entries)
        self.write('sandbox_pool_cleanup',self.failed)
        self.available=set()
        self.observed={};self.attempted=set();self.errors=[]

    def read(self, key):
        row=self.ledger.tasks.db.execute('SELECT payload FROM pipeline_state WHERE key=?',(key,)).fetchone()
        return json.loads(row[0]) if row else []

    def write(self, key, value):
        self.ledger.tasks.db.execute('INSERT OR REPLACE INTO pipeline_state VALUES(?,?)',(key,canonical(value)))

    def group(self, row):
        if not self.group_limits or row is None:
            return None
        return task_group(row, self.kind_groups, self.correspondence)

    def migrate_groups(self, rows):
        untagged = sorted((e for e in self.entries if not e.get('group')),
                          key=lambda e: (e['agents_placed'], e['sandbox_id']))
        counts = {name: sum(e.get('group') == name for e in self.entries)
                  for name in self.group_limits}
        # Reserve one existing Sandbox per group before filling any extra room.
        destinations = [name for name, count in counts.items() if count == 0]
        for name, limit in self.group_limits.items():
            destinations.extend([name] * max(0, limit - max(1, counts[name])))
        for entry, name in zip(untagged, destinations):
            entry['group'] = name
        # Preserve surplus and cleanup-only entries, including any live Agents.
        creators = {row['id']: self.group(row) for row in rows}
        default = next(iter(self.group_limits))
        for entry in [*untagged[len(destinations):], *self.failed]:
            if not entry.get('group'):
                entry['group'] = creators.get(entry.get('created_by_task')) or default

        capacity = self.read('sandbox_pool_capacity') or {}
        if 'capacity_blocked_at' in capacity:
            # Before groups, creation failures applied to the entire account.
            self.write('sandbox_pool_capacity',
                       {'groups': dict.fromkeys(self.group_limits, capacity['capacity_blocked_at'])})

    def record_capacity_failure(self, row=None):
        blocked_at = self.ledger.tasks.clock()
        if not self.group_limits:
            self.write('sandbox_pool_capacity', {'capacity_blocked_at': blocked_at})
        elif (group := self.group(row)) is not None:
            capacity = self.read('sandbox_pool_capacity') or {}
            capacity.setdefault('groups', {})[group] = blocked_at
            self.write('sandbox_pool_capacity', capacity)

    def clear_capacity_failure(self, row=None):
        if not self.group_limits:
            self.write('sandbox_pool_capacity', {})
        elif (group := self.group(row)) is not None:
            capacity = self.read('sandbox_pool_capacity') or {}
            if group in capacity.get('groups', {}):
                del capacity['groups'][group]
                self.write('sandbox_pool_capacity', capacity)

    def creation_waiting(self, row):
        data=row['data']
        return (row['state'] in ACTIVE and data.get('pool_create') and not data.get('workspace_id') and
                self.ledger.tasks.clock()-data.get('pool_create_started_at',row['updated'])<POOL_WAIT_SECONDS)

    def creation_stalled(self, row):
        data=row['data']
        return (row['state'] in ACTIVE and data.get('pool_create') and not data.get('workspace_id') and
                self.ledger.tasks.clock()-data.get('pool_create_started_at',row['updated'])>=POOL_WAIT_SECONDS)

    def capacity_blocked(self, rows, group=None):
        capacity = self.read('sandbox_pool_capacity') or {}
        blocked_at = (capacity.get('groups', {}).get(group) if self.group_limits
                      else capacity.get('capacity_blocked_at'))
        return ((blocked_at is not None and
                 0<=self.ledger.tasks.clock()-blocked_at<CAPACITY_BLOCK_SECONDS) or
                any(self.group(r) == group and (self.creation_stalled(r) or
                    (self.creation_waiting(r) and r['data'].get('capacity_retries',0)>0)) for r in rows))

    def drop(self, sandbox_id):
        self.entries[:]=[e for e in self.entries if e['sandbox_id']!=sandbox_id]
        self.available.discard(sandbox_id)
        self.observed.pop(sandbox_id,None)
        self.write('sandbox_pool',self.entries)

    def failed_entry(self, sandbox_id, workspace_id=None, corrupt=None, rotated=None, stalled=None, group=None):
        entry=next((e for e in [*self.entries,*self.failed] if e['sandbox_id']==sandbox_id),
                   {'sandbox_id':sandbox_id,'workspace_id':workspace_id})
        entry['state']='retiring' if rotated else 'failed'
        if self.group_limits and not entry.get('group'):
            entry['group'] = group or next(iter(self.group_limits))
        if rotated:entry['rotated']=rotated
        if corrupt:entry['corrupt']=corrupt
        if stalled:entry['stalled']=stalled
        self.drop(sandbox_id)
        if entry not in self.failed:self.failed.append(entry)
        self.write('sandbox_pool_cleanup',self.failed)
        self.ledger.save()
        return entry

    def observe_live(self, rows):
        now=self.ledger.tasks.clock()
        for entry in self.entries:
            if any(live_reference(r,entry) for r in rows):entry['idle_since']=now
            else:entry.setdefault('idle_since',now)  # Legacy entries start a full grace period.
        self.write('sandbox_pool',self.entries)

    def draining(self, entry):
        if entry['agents_placed']>=self.rotate_after_agents:entry['draining']=True
        return entry.get('draining',False)

    def assigned(self, entry):
        # Called with the task's provisioning reservation in the same ledger
        # checkpoint, before CMA creation. Replaying that creation adds nothing.
        entry['agents_placed']+=1
        self.draining(entry)
        self.write('sandbox_pool',self.entries)

    def rotate_empty(self, entry, rows, now):
        if any(live_reference(r,entry) for r in rows):return False
        group = entry.get('group') if self.group_limits else None
        if group is not None and group not in self.group_limits:
            self.failed_entry(entry['sandbox_id'], entry.get('workspace_id'), rotated='group-removed')
            return True
        # Keep the last usable capacity across empty ticks while the account
        # cannot create replacements. The placement ceiling still forces rotation.
        if (entry.get('state') == 'active' and not entry.get('intent') and
                entry['agents_placed'] < self.quota_blocked_max_agents_placed):
            if self.capacity_blocked(rows, group):
                return False
            # Keep a draining Workspace available through an empty tick while
            # its group needs a replacement or is waiting for its first Workspace.
            # Otherwise health could retire it before a quota rejection arrives.
            if (self.group_limits and self.draining(entry) and
                    not any(e.get('group') == group and not self.draining(e) for e in self.entries) and
                    any(self.group(r) == group and (self.creation_waiting(r) or
                        (not self.paused and r['state'] == 'queued' and self.kinds.get(r['kind']))) for r in rows)):
                return False
        reason=('draining' if self.draining(entry) else 'paused' if entry.get('state')=='paused' else
                'idle' if entry.get('state') in {'active','pausing'} and
                now-entry['idle_since']>=self.idle_seconds else None)
        if not reason:return False
        if reason=='idle' and self.awaited(entry,rows):return False
        self.failed_entry(entry['sandbox_id'],entry.get('workspace_id'),rotated=reason)
        return True

    def awaited(self, entry, rows):
        """The group's last usable Sandbox is not idle while its queued work waits.

        Placement can fall behind for longer than the idle grace (2026-11-23: the
        advance stage ran out of tick budget). Once the quota evidence expired,
        the notes group's only Sandbox was rotated 'idle' beside 258 queued
        tasks and the account could not create a replacement.
        A peer counts only once this health pass has observed it active: its
        saved state may predate a failure found later in the same pass.
        """
        if self.paused:return False
        group=entry.get('group') if self.group_limits else None
        if any(e is not entry and (e.get('group') if self.group_limits else None)==group and
               e.get('state')=='active' and not self.draining(e) and not e.get('intent') and
               (self.observed.get(e['sandbox_id']) or {}).get('status')=='active' for e in self.entries):
            return False
        return any(r['state']=='queued' and self.kinds.get(r['kind']) and self.group(r)==group for r in rows)

    def cleanup_entry(self, cma, entry, status, rows, now):
        sandbox_id=entry['sandbox_id']
        # Rotation and stalled runtimes use the same durable retirement path
        # as failed/corrupt Sandboxes, even while CMA still reports active.
        dead=(status=='failed' or (bool(entry.get('corrupt')) and status=='active') or
              ((bool(entry.get('stalled')) or bool(entry.get('rotated'))) and
               status not in {'retired','retiring'}))
        if dead and not any(live_reference(r,entry) for r in rows):
            with unit(self.ledger,'pool-health',sandbox_id) as ready:
                if not ready:return
                entry.setdefault('cleanup_intent',{'key':'outreach-pool-retire-'+sandbox_id})
                self.write('sandbox_pool_cleanup',self.failed)
                self.ledger.save(force=True)
                entry['cleanup']=cma.retire_sandbox(None,sandbox_id,
                    entry['cleanup_intent']['key'],{},dead_ok=True,finished=ledger_agents(rows))
                entry['cleanup_checked_at']=now
        elif status not in {'failed','retiring'} and not dead:
            self.write('sandbox_retired_'+sandbox_id,entry)
            self.failed.remove(entry)

    def error(self, sandbox_id, exc):
        self.errors.append({'sandbox_id':sandbox_id,'error':type(exc).__name__+': '+str(exc)[:300]})
        self.write('sandbox_pool_errors',self.errors)

    def health(self, cma, *, rows=None):
        self.available.clear();self.observed.clear();self.errors=[]
        rows=self.ledger.tasks.all() if rows is None else rows
        self.observe_live(rows)
        # Failed entries remain on a cleanup list until their last Agent retires
        # and Sandbox retirement is observed. This also replays a lost DELETE.
        entries={e['sandbox_id']:e for e in [*self.entries,*self.failed]}
        now=self.ledger.tasks.clock()
        for sandbox_id,entry in units(self.ledger,'pool-health',entries.items(),key=lambda r:r[0]):
            cleanup=entry.get('cleanup') or {}
            if (entry in self.failed and cleanup.get('reason') and not cleanup.get('sandbox_retired')
                    and now-entry.get('cleanup_checked_at',0)<RETAINED_RECHECK_SECONDS):
                continue
            try:
                sandbox=cma.request('/api/v1/sandboxes/'+sandbox_id)
                status=sandbox.get('status')
                if (status in {'retired','retiring','failed'} or
                        (sandbox.get('lifecycle_interruption') and status not in {'paused','pausing','reviving'})):
                    if status=='failed':entry=self.failed_entry(sandbox_id,entry.get('workspace_id'))
                    else:self.drop(sandbox_id)
                    self.write('sandbox_pool_cleanup',self.failed)
                    self.ledger.save()
                elif entry in self.entries:
                    self.observed[sandbox_id]=sandbox
                    intent=entry.get('intent',{})
                    if ((intent.get('action')=='pause' and status=='paused') or
                            (intent.get('action')=='revive' and status=='active')):
                        entry.pop('intent')
                        intent={}
                    entry['state']=('pausing' if intent['action']=='pause' else 'reviving') if intent else status
                    if entry['state'] in HEALTHY:entry.pop('unhealthy_since',None)
                    else:entry.setdefault('unhealthy_since',now)
                    if (entry['state'] not in HEALTHY and not intent and
                            now-entry['unhealthy_since']>=RETIRE_STALLED_SECONDS and
                            not any(live_reference(r,entry) for r in rows)):
                        entry['stalled']=status
                        entry=self.failed_entry(sandbox_id,entry.get('workspace_id'))
                    if entry in self.entries:self.rotate_empty(entry,rows,now)
                    if entry in self.entries and status=='active' and not intent and not self.draining(entry):
                        self.available.add(sandbox_id)
                if entry in self.failed:
                    self.cleanup_entry(cma,entry,status,rows,now)
            except Exception as exc:
                # An unavailable health read cannot justify replacement or reuse.
                self.available.discard(sandbox_id)
                if entry in self.failed:
                    entry['cleanup_error']=type(exc).__name__+': '+str(exc)[:300]
                self.error(sandbox_id,exc)
        self.write('sandbox_pool',self.entries)
        self.write('sandbox_pool_cleanup',self.failed)
        self.write('sandbox_pool_errors',self.errors)
        self.ledger.save()

    def transition(self, cma, entry, action):
        sandbox_id=entry['sandbox_id'];sandbox=self.observed[sandbox_id]
        if sandbox_id in self.attempted:return
        if sandbox.get('lifecycle',{}).get(action,{}).get('allowed') is False:return
        # A lost response is reconciled by health's GET before replaying the
        # original key. Never reverse an unfinished action or pause live work.
        intent=entry.get('intent')
        if intent and intent['action']!=action:return
        if action=='pause' and any(live_reference(r,entry) for r in self.ledger.tasks.all()):return
        # Share health's lane so next tick's reconciliation consumes deferred
        # lifecycle work even if the transition is no longer needed.
        with unit(self.ledger,'pool-health',sandbox_id,action) as ready:
            if not ready:return
            self.attempted.add(sandbox_id)
            if not intent:
                number=entry.get('lifecycle_sequence',0)+1
                entry['lifecycle_sequence']=number
                intent={'action':action,'key':f'outreach-pool-{action}-{sandbox_id}-{number}'}
                entry['intent']=intent
            entry['state']='pausing' if action=='pause' else 'reviving'
            self.available.discard(sandbox_id)
            self.write('sandbox_pool',self.entries)
            self.ledger.save(force=True)  # Original lifecycle intent/key must survive a lost response.
            try:
                result=cma.request('/api/v1/sandboxes/'+sandbox_id+'/'+action,'POST',None,intent['key'])
                if action=='pause' and result.get('status')=='paused':
                    entry['state']='paused';entry.pop('intent')
                # A revive response can be a cached view: only a later GET may
                # open admission, even when this POST reports active.
            except Exception as exc:
                reason=str(exc)
                if getattr(exc,'code',None)==409:
                    if hasattr(exc,'read'):reason+=' '+exc.read().decode(errors='replace')
                    if any(code in reason for code in ('sandbox_lifecycle_busy','sandbox_not_active','sandbox_not_paused')):
                        # A definite refusal has no side effect. GET again next
                        # tick before a fresh numbered attempt; conflicts and
                        # uncertain failures retain the original intent/key.
                        entry.pop('intent');entry['state']=sandbox.get('status')
                self.error(sandbox_id,exc)
            self.write('sandbox_pool',self.entries)
            self.ledger.save()

    def idle(self, cma, *, rows=None):
        rows=self.ledger.tasks.all() if rows is None else rows
        self.observe_live(rows)
        now=self.ledger.tasks.clock()
        for entry in list(self.entries):
            sandbox_id=entry['sandbox_id'];sandbox=self.observed.get(sandbox_id)
            if sandbox and self.rotate_empty(entry,rows,now):
                try:self.cleanup_entry(cma,entry,sandbox.get('status'),rows,now)
                except Exception as exc:
                    entry['cleanup_error']=type(exc).__name__+': '+str(exc)[:300]
                    self.error(sandbox_id,exc)
        self.write('sandbox_pool_cleanup',self.failed)
        self.ledger.save()

    def slot(self, row=None, *, cma=None, rows=None):
        rows=self.ledger.tasks.all() if rows is None else rows
        group = self.group(row)
        if self.group_limits and group not in self.group_limits:
            return False, None
        entries = [e for e in self.entries if e.get('group') == group] if self.group_limits else self.entries
        non_draining = [entry for entry in entries if not self.draining(entry)]
        candidates=[(sum(live_reference(r,entry) for r in rows),index,entry)
                    for index,entry in enumerate(non_draining)
                    if entry['sandbox_id'] in self.available]
        candidates=[candidate for candidate in candidates if candidate[0]<self.agents_per_sandbox]
        if candidates:
            return True,min(candidates,key=lambda candidate:candidate[:2])[2]
        # Only a pending first Workspace in this group serializes its creation.
        pending = [r for r in rows if self.creation_waiting(r)]
        waiting = any(self.group(r) == group for r in pending)
        group_count = len(non_draining) if self.group_limits else len(entries)
        can_create = (not waiting and group_count < self.group_limits.get(group, self.max_sandboxes)
                      and self.can_create_within_pool(entries, rows, pending))

        # Give a fresh attempt its bounded first-Workspace wait. Otherwise
        # a group with only draining entries can keep working when local limits
        # block creation, even after the cached account-quota evidence expires.
        if ((not self.group_limits or not non_draining) and
                (self.capacity_blocked(rows, group) or
                 (self.group_limits and not waiting and not can_create))):
            fallback=[(sum(live_reference(r,entry) for r in rows),index,entry)
                      for index,entry in enumerate(entries)
                      if entry.get('draining') and entry.get('state')=='active'
                      and entry['agents_placed']<self.quota_blocked_max_agents_placed
                      and (self.observed.get(entry['sandbox_id']) or {}).get('status')=='active'
                      and not entry.get('intent')]
            fallback=[candidate for candidate in fallback if candidate[0]<self.agents_per_sandbox]
            if fallback:
                return True,min(fallback,key=lambda candidate:candidate[:2])[2]
        for entry in non_draining:
            if (entry['sandbox_id'] in self.observed and
                    sum(live_reference(r,entry) for r in rows)<self.agents_per_sandbox and
                    entry.get('state') in {'paused','pausing','reviving'}):
                if cma is not None and self.observed[entry['sandbox_id']].get('status')=='paused':
                    self.transition(cma,entry,'revive')
                return False,None  # Sleeping/in-flight capacity reserves its place in the pool.
        return can_create, None

    def can_create_within_pool(self, entries, rows, pending):
        now=self.ledger.tasks.clock()
        if any(e['sandbox_id'] not in self.observed or
               (e.get('state') not in HEALTHY and now-e.get('unhealthy_since',now)<STALLED_SECONDS)
               for e in entries):
            return False
        # Legacy one-task Sandboxes still count against the same quota until
        # their tasks finish and retire them.
        pooled={e['sandbox_id'] for e in [*self.entries,*self.failed]}
        legacy={r['data']['owned_sandbox_id'] for r in rows
                if r['data'].get('owned_sandbox_id') and r['data']['owned_sandbox_id'] not in pooled
                and not r['data'].get('pool_sandbox_id')
                and not r['data'].get('cleanup',{}).get('sandbox_retired')
                and (r['state'] in ACTIVE or (r['data'].get('agent_id') and not r['data'].get('cleanup',{}).get('deleted')))}
        # A pending first Workspace in another group can consume a global
        # reservation, without serializing this group's Workspace creation.
        active_ids = {e['sandbox_id'] for e in self.entries}
        creating = sum(r['data'].get('sandbox_id') not in active_ids for r in pending)
        return len(self.entries) + len(legacy) + creating < self.max_sandboxes

    def remember(self, sandbox_id, workspace_id, row):
        entry={'sandbox_id':sandbox_id,'workspace_id':workspace_id,'created_by_task':row['id'],
               'state':'active','idle_since':self.ledger.tasks.clock(),'agents_placed':1}
        if self.group_limits:
            entry['group'] = self.group(row) or next(iter(self.group_limits))
        self.draining(entry)
        if not any(e['sandbox_id']==sandbox_id for e in self.entries):
            self.entries.append(entry)
            self.write('sandbox_pool',self.entries)
            self.ledger.save(force=True)  # Persist shared ownership before any reuse/admission.
        self.available.add(sandbox_id)
        self.observed[sandbox_id]={'status':'active'}

    def owned_sandbox(self, data):
        owned=data.get('owned_sandbox_id')
        if data.get('pool_sandbox_id') or any(e['sandbox_id']==owned for e in self.entries):
            return None
        return owned
