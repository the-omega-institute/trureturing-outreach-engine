#!/usr/bin/env python3
"""Compare the frozen read projections to SQL emitted by the live Python paths.

No database, private config, fixture payload or source receipt is retained.
"""
from pathlib import Path
from unittest.mock import patch
from vectors.generate import ROOT
from managed import pipeline
from managed.tasks import Tasks


class Cursor:
    def __init__(self, sql): self.sql = sql
    def fetchall(self): return []
    def fetchone(self): return (0, 0) if 'SUM(' in self.sql else None
    def __iter__(self): return iter([])


class Capture:
    def __init__(self): self.sql = []
    def execute(self, sql, *args):
        self.sql.append(sql)
        return Cursor(sql)


def live_sql():
    tasks = Tasks.__new__(Tasks)
    tasks.db = Capture()
    result = {}
    for method, args, name in [(tasks.lane_rows, (), 'lane_rows'),
                               (tasks.owner_questions, (), 'old_owner_questions'),
                               (tasks.counts, (), 'counts_state'),
                               (tasks.counts, ('kind',), 'counts_kind')]:
        tasks.db.sql.clear()
        method(*args)
        assert len(tasks.db.sql) == 1
        result[name] = tasks.db.sql[0]
    tasks.db.sql.clear()
    with patch.object(pipeline, 'read', return_value={'contacts': []}):
        pipeline.status(tasks, ROOT, {}, compact=True)
    result['pipeline_counts'] = next(sql for sql in tasks.db.sql if 'SUM(' in sql)
    return result


def main():
    folder = Path(__file__).resolve().parent / 'crates/outreach-ledger/src'
    for name, sql in live_sql().items():
        frozen = (folder / (name + '.sql')).read_text()
        assert ' '.join(sql.split()) == ' '.join(frozen.split()), name + ': live Python SQL changed'
    print('Live Python SQL: lane_rows, pipeline_counts, old_owner_questions, counts (state/kind): passed')


if __name__ == '__main__':
    main()
