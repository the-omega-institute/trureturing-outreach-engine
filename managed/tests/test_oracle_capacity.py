import json
import sqlite3
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from managed.control import (oracle_capacity_line, refresh_oracle_capacity,
                             summarize_oracle_capacity, task_prompt)
from managed.queue import canonical


class OracleCapacityTests(unittest.TestCase):
    now=datetime(2026, 11, 2, 2, 0, tzinfo=timezone.utc)

    def holder(self, value=None):
        db=sqlite3.connect(':memory:')
        db.execute('CREATE TABLE pipeline_state(key TEXT PRIMARY KEY, payload TEXT NOT NULL)')
        if value is not None:
            db.execute('INSERT INTO pipeline_state VALUES(?,?)',('oracle_capacity',canonical(value)))
        return SimpleNamespace(tasks=SimpleNamespace(db=db,all=lambda:[],reviews=lambda:[],owner_questions=lambda:[]))

    def test_snapshot_summarizes_recent_available_cooling_and_queue(self):
        payload={'workers':[
            {'id':'available-1','cooldown_until':None,'last_seen_at':'2026-11-02T01:59:00Z'},
            {'id':'available-2','last_seen_at':'2026-11-02T01:50:00Z'},
            {'id':'cooling','cooldown_until':'2026-11-03T02:06:00Z','last_seen_at':'2026-11-02T01:59:00Z'},
            {'id':'stale','last_seen_at':'2026-11-01T00:00:00Z'},
        ],'queue':{'queued':4}}
        self.assertEqual(summarize_oracle_capacity(payload,self.now),{
            'checked_at':'2026-11-02T02:00:00Z','workers':4,'available':2,
            'cooling':1,'earliest_recovery':'2026-11-03T02:06:00Z','queued':4})

    def test_fetch_failure_preserves_previous_snapshot(self):
        previous={'checked_at':'2026-11-02T01:45:00Z','workers':25,'available':8,
                  'cooling':17,'earliest_recovery':'2026-11-03T02:06:00Z','queued':0}
        holder=self.holder(previous)
        def fail():raise OSError('quota endpoint unavailable')
        try:
            self.assertIsNone(refresh_oracle_capacity(holder,fail,self.now))
            row=holder.tasks.db.execute("SELECT payload FROM pipeline_state WHERE key='oracle_capacity'").fetchone()
            self.assertEqual(json.loads(row[0]),previous)
            self.assertNotIn('COORDINATOR ORACLE CAPACITY',task_prompt({'kind':'research','request':{'id':'job','inputs':{}}}))
        finally:
            holder.tasks.db.close()

    def test_prompt_line_normal_and_constrained(self):
        row={'kind':'research','request':{'id':'job','inputs':{}}}
        normal={'checked_at':'2026-11-02T02:00:00Z','workers':25,'available':8,
                'cooling':5,'earliest_recovery':'2026-11-03T02:06:00Z','queued':0}
        constrained={**normal,'available':2}
        normal_prompt=task_prompt(row,oracle_capacity=normal)
        constrained_prompt=task_prompt(row,oracle_capacity=constrained)
        self.assertIn('COORDINATOR ORACLE CAPACITY: 8/25 workers available; 5 cooling; 0 queued.',normal_prompt)
        self.assertNotIn('Oracle capacity is constrained',normal_prompt)
        self.assertIn('COORDINATOR ORACLE CAPACITY: 2/25 workers available; 5 cooling; 0 queued.',constrained_prompt)
        self.assertIn('make at most one short Oracle attempt per required step',constrained_prompt)
        self.assertIn('mark editor_assessment pending in the evidence and self_audit',constrained_prompt)

    def test_work_exposes_snapshot(self):
        from managed.automation import work
        previous={'checked_at':'2026-11-02T02:00:00Z','workers':25,'available':8,
                  'cooling':17,'earliest_recovery':'2026-11-03T02:06:00Z','queued':0}
        holder=self.holder(previous)
        try:
            with patch('managed.daily_local.status',return_value={'status':'ready'}), \
                 patch('managed.author_contacts.pending',return_value=None):
                result=work(holder.tasks,'.')
        finally:
            holder.tasks.db.close()
        self.assertEqual(result['oracle_capacity'],previous)


if __name__=='__main__':unittest.main()


class MeasuredBrokerShapeTests(unittest.TestCase):
    def test_measured_active_workers_payload(self):
        # Shape measured from the live broker on 2026-11-02.
        payload={'queued':4,'dispatched':7,'max_workers':40,'active_workers':[
            {'worker_label':'a','last_seen_secs_ago':3,'cooldown_until':'2026-11-03T06:00:55.357+00:00'},
            {'worker_label':'b','last_seen_secs_ago':5,'cooldown_until':None},
            {'worker_label':'c','last_seen_secs_ago':4000}]}
        snap=summarize_oracle_capacity(payload,now='2026-11-02T07:00:00Z')
        self.assertEqual((snap['workers'],snap['available'],snap['cooling'],snap['queued']),(3,1,1,4))
        self.assertTrue(snap['earliest_recovery'].startswith('2026-11-03T06:00:55'))

