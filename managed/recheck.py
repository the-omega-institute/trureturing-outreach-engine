"""Ledger-backed delays for slow observations, retaining their original handles."""
from contextlib import contextmanager
import json

from .queue import canonical, digest


SLOW_SECONDS = 20
RECHECK_SECONDS = 30 * 60


def _key(lane, task_id):
    return 'recheck:' + lane + ':' + task_id


def next_at(tasks, lane, task_id, binding):
    row = tasks.db.execute('SELECT payload FROM pipeline_state WHERE key=?',
                           (_key(lane, task_id),)).fetchone()
    if row:
        saved = json.loads(row[0])
        if saved['binding'] == digest(binding):
            return saved['next_at']
    return None


def due(tasks, lane, task_id, binding):
    retry_at = next_at(tasks, lane, task_id, binding)
    return retry_at is None or tasks.clock() >= retry_at


def defer(tasks, lane, task_id, binding, seconds=RECHECK_SECONDS):
    now = tasks.clock()
    retry_at = now + seconds
    tasks.db.execute('INSERT OR REPLACE INTO pipeline_state VALUES(?,?)',
                     (_key(lane, task_id), canonical({
                         'binding': digest(binding), 'checked_at': now, 'next_at': retry_at})))
    return retry_at


def clear(tasks, lane, task_id):
    tasks.db.execute('DELETE FROM pipeline_state WHERE key=?', (_key(lane, task_id),))


@contextmanager
def attempt(ledger, lane, task_id, binding, *, timeouts=True):
    """Fast failures may retry next tick; slow calls/timeouts yield to other tasks.

    Wall-clock due times survive snapshot restoration. Durations use the tick's
    monotonic clock. No cancellation, handle replacement or permission is implied.
    """
    tick = getattr(ledger, 'tick', None)
    clock = tick.clock if tick else ledger.clock
    started = clock()
    timed_out = False
    deferred_by_budget = False

    def skip_backoff():
        nonlocal deferred_by_budget
        deferred_by_budget = True

    try:
        yield skip_backoff
    except (TimeoutError, ConnectionError):
        timed_out = timeouts
        raise
    finally:
        if not deferred_by_budget and (timed_out or clock() - started >= SLOW_SECONDS):
            defer(ledger.tasks, lane, task_id, binding)
            ledger.save()
