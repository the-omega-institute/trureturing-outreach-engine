"""Offline affairs inventory: obligations, aging fairness and existing work mapping."""
import copy
from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from fixture_config import use_example_config
from managed import affairs, automation
from managed.tasks import Tasks

NOW = datetime(2026, 11, 1, tzinfo=timezone.utc)


def thread(identity, **changes):
    return {'id': identity, 'correspondent': {'name': 'Professor Example', 'email': 'author@example.invalid'},
            'topic': 'A precise question', 'stage': 'exchange', 'ball': 'us',
            'last_in': {'date': '2026-10-31T00:00:00Z', 'summary': 'Can you prove X?'},
            'last_out': None, 'our_open_promises': [], 'their_open_requests': [],
            'next_action': 'Check X and send a readable proof', 'job_ids': [], **changes}


class AffairsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.root = Path(self.tmp.name)
        (self.root/'tables').mkdir()
        self.write('contacts', {'contacts': [{'id': 'contact-a', 'email': 'author@example.invalid',
            'name': 'Professor Example', 'relationship': {'aliases': ['alias@example.invalid']}}]})
        self.write('refusals', {'refusals': []})
        self.write('submissions', {'papers': []})
        self.write('threads', {'lines': []})

    def tearDown(self):
        self.tmp.cleanup()

    def write(self, name, value):
        (self.root/'tables'/(name + '.json')).write_text(json.dumps(value))

    def view(self, rows, work=None):
        self.write('threads', {'lines': rows})
        return affairs.inventory(self.root, work, NOW)

    def test_priority_order_and_explicit_score_reasons(self):
        rows = [thread('age'), thread('due', next_check='2026-10-31'),
                thread('joint', stage='joint_paper'),
                thread('request', their_open_requests=[{'quote': 'Please check X'}]),
                thread('promise', our_open_promises=[{'quote': 'We will check X'}])]
        view = self.view(rows)
        self.assertEqual([r['id'] for r in view['affairs']], ['promise', 'request', 'joint', 'due', 'age'])
        promise = view['affairs'][0]
        self.assertEqual(promise['gap'], ['We will check X'])
        self.assertEqual(promise['latest_result'], 'Can you prove X?')
        self.assertEqual(sum(r['points'] for r in promise['priority_reasons']), promise['priority_score'])
        self.assertTrue(promise['completion_condition'].startswith('reply sent answering'))

    def test_uncapped_fairness_allows_old_ordinary_work_to_surface(self):
        view = self.view([thread('new-promise', our_open_promises=['A promise']),
                          thread('old', last_in={'date': '2025-01-01'})])
        self.assertEqual(view['affairs'][0]['id'], 'old')
        self.assertGreater(view['affairs'][0]['priority_reasons'][-1]['points'], 500)

    def test_closed_and_declined_excluded_done_obligations_not_scored(self):
        rows = [thread('closed', stage='closed', our_open_promises=['promise']),
                thread('declined', stage='declined'), thread('waiting', ball='them'),
                thread('promise', ball='them', our_open_promises=['promise']),
                thread('duplicate', ball='none', their_open_requests=[{'quote': 'duplicate', 'done': '2026-10-30'}])]
        view = self.view(rows)
        self.assertEqual({r['id'] for r in view['affairs']}, {'promise', 'duplicate'})
        duplicate = next(r for r in view['affairs'] if r['id'] == 'duplicate')
        self.assertEqual(duplicate['gap'], [])
        self.assertFalse(any(r['reason'] == 'their explicit request' for r in duplicate['priority_reasons']))

    def test_live_task_email_owner_and_direct_thread_mapping_and_handles(self):
        def task(identity, inputs, owner='other'):
            return {'id': identity, 'state': 'running', 'request': {'owner': owner, 'kind': 'reply', 'inputs': inputs}}
        work = {'follow_up': [task('mail', {'mailbox_observation': {'email': 'alias@example.invalid'}}),
                              task('owner', {'owner_ids': ['contact-a']}),
                              task('direct', {'thread_ref': {'thread_id': 't'}}),
                              task('other-thread', {'thread_ref': {'thread_id': 'not-t'}}, 'contact-a'),
                              {**task('old', {}), 'state': 'superseded'}]}
        view = self.view([thread('t', job_ids=['historic'], running_job_id='active')], work)
        row = next(r for r in view['affairs'] if r['id'] == 't')
        self.assertEqual(row['handle'], ['active', 'direct', 'historic', 'mail', 'owner'])
        self.assertEqual({r['id'] for r in view['affairs']}, {'t', 'other-thread'})

    def test_submission_and_unmapped_task_and_json_markdown(self):
        paper = {'id': 'paper', 'title': 'A theorem', 'stage': 'needs_work', 'target_journal': 'Journal',
                 'version_date': '2026-10-28', 'blocker': 'Missing proof', 'next_step_owner': 'Avery Reed'}
        self.write('submissions', {'papers': [paper, {**paper, 'id': 'submitted', 'stage': 'submitted'}]})
        work = {'tasks': [{'id': 'paper-job', 'state': 'queued', 'request': {'kind': 'submission', 'inputs': {'paper': {'id': 'paper'}}}},
                          {'id': 'new-job', 'state': 'running', 'kind': 'shortnote', 'request': {'inputs': {}}}]}
        view = self.view([], work)
        row = next(r for r in view['affairs'] if r['id'] == 'paper')
        self.assertEqual(row['handle'], ['paper-job']); self.assertEqual(row['owner'], 'owner')
        self.assertIn('Oracle editorial review', row['completion_condition'])
        self.assertEqual({r['id'] for r in view['affairs']}, {'paper', 'new-job'})
        self.assertIn('paper', json.loads(json.dumps(view))['markdown'])

    def test_work_snapshot_includes_queued_tasks_without_new_state_table(self):
        from test_tasks import request
        tasks = Tasks(self.root/'tasks.sqlite3')
        try:
            tasks.add(request('queued', 'contact-a'))
            with patch('managed.daily_local.status', return_value={'status': 'fixture'}):
                snapshot = automation.work(tasks, self.root)
            self.assertEqual(snapshot['follow_up'], [])
            view = self.view([thread('t')], snapshot)
            self.assertIn('queued', view['affairs'][0]['handle'])
        finally:
            tasks.close()

    def test_task_owner_depends_on_current_state_not_decision_history(self):
        states=['running','awaiting-review','awaiting-mailbox','awaiting-user','owner-draft']
        work={'tasks':[{'id':state,'state':state,'request':{'kind':'reply',
                      'inputs':{'user_decision_ever':True}}} for state in states]}
        rows={row['id']:row for row in self.view([],work)['affairs']}
        for state in states:
            with self.subTest(state=state):
                self.assertEqual(rows[state]['owner'],
                                 'owner' if state in {'awaiting-user','owner-draft'} else 'us')

    def test_missing_or_future_dates_and_read_only(self):
        self.write('threads', {'lines': [thread('missing', last_in=None), thread('future', last_in={'date': '2027-01-01'})]})
        before = {p.name: p.read_bytes() for p in (self.root/'tables').iterdir()}
        view = affairs.inventory(self.root, now=NOW)
        indexed = {r['id']: r for r in view['affairs']}
        self.assertIsNone(indexed['missing']['days_waiting']); self.assertEqual(indexed['future']['days_waiting'], 0)
        self.assertEqual(before, {p.name: p.read_bytes() for p in (self.root/'tables').iterdir()})
        self.assertIn('offline tables only', view['work_source'])


if __name__ == '__main__':
    unittest.main()


def setUpModule():
    use_example_config()
