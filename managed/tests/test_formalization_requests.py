import hashlib
import hmac
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fixture_config import use_cli_config
from managed import automation, formalization_requests as filing
from managed.control import Ledger, main
from managed.queue import canonical, digest
from test_tasks import Releases, request


GAP = {'statement': 'For n ≥ 0, a(n) is even.', 'note_locator': 'Corollary 2',
       'source_doi': 'https://doi.org/10.1234/example', 'source_arxiv': None,
       'source_url': None, 'source_locator': 'Section 3, Lemma 2',
       'why_it_matters': 'The recurrence can reuse this parity invariant.'}


class FilingTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.remote = Releases()
        self.ledger = Ledger(self.remote, self.folder.name)
        self.addCleanup(self.ledger.tasks.close)
        self.config = {'paused': False}

    def note(self, task_id, *, legacy=False, gap=GAP):
        row = self.ledger.tasks.add(request(task_id, kind='shortnote'))
        result = {'task_id': task_id, 'outcome': 'completed', 'formalization_gaps': [gap]}
        if not legacy:
            result['value_assessment'] = {'verdict': 'incremental', 'reasons': 'Includes n = 0.'}
        tasks = self.ledger.tasks
        row = tasks.update(row['id'], row['revision'], 'awaiting-review', {'result': result})
        review = {'reviewer': 'local-claude-loop', 'verdict': 'PASS',
                  'reason': 'Verified proof, significance and scope.', 'evidence_ref': 'private:fixture',
                  'binding': {'task_id': task_id, 'result_hash': digest(result),
                              'workflow_hash': row['request']['workflow_hash'],
                              'task_revision': row['revision']}}
        if legacy:
            return tasks.update(task_id, row['revision'], 'handoff-ready',
                                {'result': result, 'review': review})
        review['signature'] = hmac.new(b'secret', canonical(review).encode(), hashlib.sha256).hexdigest()
        return tasks.verdict(review, 'secret')

    def test_closed_issue_reused_by_statement_and_doi_across_pages(self):
        note = self.note('checked-note')
        url = 'https://github.com/' + filing.REPO + '/issues/123'
        calls = []

        def github(path, method='GET', body=None):
            calls.append((path, method))
            if path.endswith('page=1'):
                return [{'title': 'Other issue'}] * 100
            return [{'title': '[Formalization] Parity invariant | DOI:10.1234/example | Lemma 2',
                     'state': 'closed', 'body': '## Exact mathematical scope\n\n' + GAP['statement'],
                     'html_url': url}]

        filing.advance(self.ledger, self.config, github)
        saved = self.ledger.tasks.get(note['id'])['data']['formalization_requests'][0]
        self.assertEqual(saved['issue_url'], url)
        self.assertEqual([method for _, method in calls], ['GET', 'GET'])
        self.assertIn('state=all', calls[0][0])

    def test_lost_post_is_reconciled_after_restoring_snapshot(self):
        note = self.note('checked-note')
        issues = []
        url = 'https://github.com/' + filing.REPO + '/issues/124'

        def github(path, method='GET', body=None):
            if method == 'GET':
                return issues
            self.assertEqual(path, filing.ISSUES)
            self.assertIsNotNone(self.remote.body['snapshot'])
            self.assertIn('| DOI:10.1234/example | Section 3, Lemma 2', body['title'])
            self.assertIn(GAP['note_locator'], body['body'])
            self.assertNotIn('private:fixture', body['body'])
            issues.append({**body, 'html_url': url})
            raise TimeoutError('Response was lost after creation')

        filing.advance(self.ledger, self.config, github)
        saved = self.ledger.tasks.get(note['id'])['data']['formalization_requests'][0]
        self.assertEqual(saved['state'], 'pending')
        self.assertEqual(saved['last_error'], 'TimeoutError')
        with tempfile.TemporaryDirectory() as folder:
            restored = Ledger(self.remote, folder)
            try:
                restored.tasks.clock = lambda: self.ledger.tasks.clock() + 3600
                filing.advance(restored, self.config, github)
                saved = restored.tasks.get(note['id'])['data']['formalization_requests'][0]
                self.assertEqual(saved['issue_url'], url)
            finally:
                restored.tasks.close()
        self.assertEqual(len(issues), 1)

    def test_unavailable_filing_leaves_pass_and_visible_pending_work(self):
        note = self.note('checked-note')
        with patch.dict('os.environ', {'CMA_ACCESS_TOKEN': ''}):
            filing.advance(self.ledger, self.config)
        work = automation.work(self.ledger.tasks, Path(self.folder.name))
        self.assertEqual(work['short_note_deliveries'][0]['task_id'], note['id'])
        self.assertEqual(work['formalization_requests'][0]['state'], 'pending')
        self.assertIn('connection unavailable', work['formalization_requests'][0]['last_error'])

    def test_legacy_backfill_requires_explicit_signed_request_and_is_replay_safe(self):
        row = self.note('legacy-note', legacy=True)
        filing.advance(self.ledger, self.config, lambda *args: self.fail('Legacy note auto-filed'))
        work = automation.work(self.ledger.tasks, Path(self.folder.name))
        path = Path(self.folder.name) / 'work.json'
        path.write_text(json.dumps(work))
        with patch('managed.control.stage') as stage, patch('builtins.print') as printer:
            main(['file-formalization-requests', '--work', str(path), '--dry-run'])
            plan = json.loads(printer.call_args.args[0])['notes']
            stage.assert_not_called()
        self.assertEqual(plan[0]['task_id'], row['id'])
        envelope = filing.backfill_envelope(plan, 'secret')
        with self.assertRaisesRegex(ValueError, 'signature failed'):
            filing.ingest(self.ledger.tasks, envelope, 'different')
        filing.ingest(self.ledger.tasks, envelope, 'secret')
        filing.ingest(self.ledger.tasks, envelope, 'secret')
        saved = self.ledger.tasks.get(row['id'])
        self.assertEqual(saved['state'], 'handoff-ready')
        self.assertEqual(len(saved['data']['formalization_requests']), 1)
        self.assertEqual(filing.backfill_plan({'tasks': [saved]}), [])

    def test_arxiv_gap_uses_no_doi_convention_and_distinguishes_sources(self):
        gap = {**GAP, 'source_doi': None, 'source_arxiv': '2501.01234v2'}
        item = filing.pending_requests({'formalization_gaps': [gap]})[0]
        payload = filing.issue_payload(item)
        self.assertIn(' | No DOI | ', payload['title'])
        self.assertIn('https://arxiv.org/abs/2501.01234v2', payload['body'])
        other, = filing.pending_requests({'formalization_gaps': [{**gap, 'source_arxiv': '2501.01235'}]})
        self.assertFalse(filing.same_issue(payload, other))

    def test_phase3d_failed_checkpoint_prevents_issue_creation(self):
        note = self.note('checkpoint-note')
        calls = []
        save = self.ledger.save
        def checkpoint(force=False):
            if force: raise OSError('Synthetic snapshot failure')
            return save()
        def github(path, method='GET', body=None):
            calls.append(method)
            self.assertEqual(method, 'GET')
            return []
        with patch.object(self.ledger, 'save', checkpoint):
            filing.advance(self.ledger, self.config, github)
        self.assertEqual(calls, ['GET'])
        item = self.ledger.tasks.get(note['id'])['data']['formalization_requests'][0]
        self.assertEqual((item['state'], item['last_error']), ('pending', 'OSError'))

    def test_phase3d_partial_or_invalid_inventory_never_authorizes_post(self):
        self.note('inventory-note')
        for invalid in (False, True):
            with self.subTest(invalid=invalid):
                calls = []
                self.ledger.tasks.db.execute("DELETE FROM pipeline_state WHERE key LIKE 'recheck:%'")
                def github(path, method='GET', body=None):
                    calls.append(method)
                    self.assertEqual(method, 'GET')
                    if invalid: return [{}]
                    if path.endswith('page=1'): return [{'title': 'Other'}] * 100
                    raise TimeoutError('Synthetic incomplete inventory')
                filing.advance(self.ledger, self.config, github)
                self.assertEqual(calls, ['GET'] if invalid else ['GET', 'GET'])

    def test_phase3d_uncertain_post_stops_all_remaining_creations_this_tick(self):
        from urllib.error import HTTPError
        self.note('first-gap')
        self.note('second-gap', gap={**GAP, 'statement': 'A second exact statement.'})
        calls = []
        def github(path, method='GET', body=None):
            calls.append(method)
            if method == 'GET': return []
            raise HTTPError('https://service.example.invalid', 503, 'Synthetic unavailable', {}, None)
        filing.advance(self.ledger, self.config, github)
        self.assertEqual(calls, ['GET', 'POST'])
        items = [r['data']['formalization_requests'][0] for r in self.ledger.tasks.all()]
        self.assertEqual([i['state'] for i in items], ['pending', 'pending'])
        self.assertEqual([i['last_error'] for i in items], ['GitHub HTTP 503', 'GitHub filing unavailable this tick'])

    def test_phase3d_invalid_receipt_remains_pending(self):
        note = self.note('receipt-note')
        calls = []
        def github(path, method='GET', body=None):
            calls.append(method)
            return [] if method == 'GET' else {'html_url': 'https://other.example.invalid/issues/1'}
        filing.advance(self.ledger, self.config, github)
        item = self.ledger.tasks.get(note['id'])['data']['formalization_requests'][0]
        self.assertEqual((item['state'], item['last_error']), ('pending', 'GateError'))
        self.assertEqual(calls, ['GET', 'POST'])

    def test_phase3d_paused_stage_does_not_read_or_checkpoint(self):
        self.note('paused-note')
        with patch.object(self.ledger, 'save', side_effect=AssertionError('Unexpected save')):
            filing.advance(self.ledger, {'paused': True}, lambda *a: self.fail('Unexpected GitHub read'))

    def test_phase3d_eight_candidate_bound_and_same_tick_deduplication(self):
        for index in range(9): self.note('bounded-' + str(index))
        calls = []
        def github(path, method='GET', body=None):
            calls.append(method)
            return [] if method == 'GET' else {'html_url': 'https://github.com/' + filing.REPO + '/issues/125'}
        filing.advance(self.ledger, self.config, github)
        states = [r['data']['formalization_requests'][0]['state'] for r in self.ledger.tasks.all()]
        self.assertEqual(states, ['filed'] * 8 + ['pending'])
        self.assertEqual(calls, ['GET', 'POST'])


def setUpModule():
    use_cli_config()
