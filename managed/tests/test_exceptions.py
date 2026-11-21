"""Small synthetic reconciliation cases; no mailbox or artifact reads."""
import copy
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from managed import control, exceptions as e
from managed.automation import paper_identity, paper_summary
from managed.queue import digest


def task(identity, kind='reply', state='running', inputs=None, **data):
    return {'id': identity, 'kind': kind, 'state': state, 'owner': 'contact-a',
            'request': {'id': identity, 'kind': kind, 'owner': 'contact-a',
                        'workflow_hash': 'current', 'inputs': inputs or {}}, 'data': data}


def passed(identity, kind='shortnote', inputs=None, **data):
    row = task(identity, kind, 'handoff-ready', inputs, **data)
    result = {'outcome': 'completed', 'report': 'PRIVATE REPORT',
              'artifacts': [{'contents': 'PRIVATE ARTIFACT'}]}
    row['data'].update(result=result, review={'verdict': 'PASS', 'reviewer': 'local-claude-loop',
        'binding': {'result_hash': digest(result), 'workflow_hash': 'current'}})
    return row


def paper(identity='paper'):
    return {'id': identity, 'title': 'A synthetic theorem', 'authors': ['Avery Reed'],
            'workspace': '../mail/example/submission/paper', 'version_date': '2026-11-01',
            'stage': 'needs_work', 'target_journal': 'Journal', 'blocker': '',
            'next_step_owner': 'operator'}


class ExceptionsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name); (self.root / 'tables').mkdir()
        self.write('contacts', {'contacts': [{'id': 'contact-a', 'name': 'Example',
            'email': 'a@example.invalid', 'relationship': {'aliases': ['alias@example.invalid']}}]})
        self.write('threads', {'lines': []})
        self.write('refusals', {'refusals': []})
        self.write('submissions', {'papers': []})
        self.write('decisions', {'holds': [], 'short_note_publication': {'enabled': True}})
        current = patch.object(control, 'workflow', return_value='current')
        current.start(); self.addCleanup(current.stop)

    def write(self, name, value):
        (self.root / 'tables' / (name + '.json')).write_text(json.dumps(value))

    def inventory(self, rows=(), **sections):
        return e.inventory({'tasks': list(rows), **sections}, self.root)

    def items(self, check, rows=(), **sections):
        return [i for i in self.inventory(rows, **sections)['items'] if i['check'] == check]

    def test_sent_receipt_requires_exact_outgoing_event_for_every_recipient(self):
        packet = {'to': ['alias@example.invalid'], 'cc': [], 'body': 'PRIVATE MAIL'}
        row = task('sent', state='verified', result={'packet': packet},
                   sent_receipt={'remote_id': 'sent-id', 'evidence_ref': 'graph:sent:<one>'})
        contacts = json.loads((self.root / 'tables/contacts.json').read_text())
        contact = contacts['contacts'][0]
        contact['hub'] = {'events': [{'kind': 'sent', 'evidence': 'graph:sent:<older>'}]}
        self.write('contacts', contacts)
        self.assertEqual(len(self.items('send-registry', [row])), 1)
        contact['hub']['events'].append({'kind': 'reply', 'evidence': 'graph:sent:<one>'})
        self.write('contacts', contacts)
        self.assertEqual(len(self.items('send-registry', [row])), 1)
        contact['hub']['events'].append({'kind': 'sent', 'evidence': 'graph:sent:<one>'})
        self.write('contacts', contacts)
        self.assertEqual(self.items('send-registry', [row]), [])
        packet['cc'].append('unregistered@example.invalid')
        self.assertEqual(len(self.items('send-registry', [row])), 1)
        self.assertNotIn('PRIVATE MAIL', json.dumps(self.inventory([row])))

    def test_sent_events_evidence_matches_by_task_id(self):
        packet = {'to': ['alias@example.invalid'], 'cc': []}
        row = task('sent-events-task', state='verified', result={'packet': packet},
                   sent_receipt={'evidence_ref': 'graph:sent:<message@example.invalid>'})
        contacts = json.loads((self.root / 'tables/contacts.json').read_text())
        contacts['contacts'][0]['hub'] = {'events': [{
            'kind': 'sent',
            'evidence': json.dumps({'source': 'sent-events', 'task_id': row['id']}),
        }]}
        self.write('contacts', contacts)
        self.assertEqual(self.items('send-registry', [row]), [])

    def test_sent_events_evidence_matches_by_bare_internet_message_id(self):
        packet = {'to': ['alias@example.invalid'], 'cc': []}
        row = task('sent-events-message', state='verified', result={'packet': packet},
                   sent_receipt={'evidence_ref': 'graph:sent:<message@example.invalid>'})
        contacts = json.loads((self.root / 'tables/contacts.json').read_text())
        contacts['contacts'][0]['hub'] = {'events': [{
            'kind': 'sent',
            'evidence': json.dumps({'source': 'sent-events',
                                    'internetMessageId': '<message@example.invalid>'}),
        }]}
        self.write('contacts', contacts)
        self.assertEqual(self.items('send-registry', [row]), [])

    def test_requested_recipient_excluded_from_sent_packet_is_not_required(self):
        packet = {'to': ['alias@example.invalid'], 'cc': []}
        row = task('sent-subset', state='verified', result={'packet': packet},
                   inputs={'recipient_emails': ['alias@example.invalid', 'excluded@example.invalid']},
                   sent_receipt={'evidence_ref': 'graph:sent:<subset@example.invalid>'})
        contacts = json.loads((self.root / 'tables/contacts.json').read_text())
        contacts['contacts'][0]['hub'] = {'events': [{
            'kind': 'sent', 'evidence': json.dumps({'task_id': row['id']})}]}
        self.write('contacts', contacts)
        self.assertEqual(self.items('send-registry', [row]), [])
        no_packet = task('sent-no-packet', state='verified',
                         inputs={'recipient_emails': ['excluded@example.invalid']},
                         sent_receipt={'evidence_ref': 'graph:sent:<none@example.invalid>'})
        self.assertEqual(len(self.items('send-registry', [no_packet])), 1)

    def test_sent_events_evidence_for_a_different_task_does_not_match(self):
        packet = {'to': ['alias@example.invalid'], 'cc': []}
        row = task('sent-events-target', state='verified', result={'packet': packet},
                   sent_receipt={'evidence_ref': 'graph:sent:<target@example.invalid>'})
        contacts = json.loads((self.root / 'tables/contacts.json').read_text())
        contacts['contacts'][0]['hub'] = {'events': [{
            'kind': 'sent',
            'evidence': json.dumps({'source': 'sent-events',
                                    'task_id': 'sent-events-other',
                                    'internetMessageId': '<other@example.invalid>'}),
        }]}
        self.write('contacts', contacts)
        self.assertEqual(len(self.items('send-registry', [row])), 1)

    def test_owner_drafts_require_receipts_exact_pass_and_current_workflow(self):
        row = passed('draft', 'reply'); row['state'] = 'owner-draft'
        row['data']['last_error'] = {'type': 'GateError', 'message': 'PRIVATE ERROR'}
        item = self.items('owner-draft', [row])[0]
        self.assertIn('receipt', item['reason']); self.assertIn('GateError', item['reason'])
        row['data']['draft_receipt'] = {'remote_id': 'draft-id'}
        self.assertEqual(self.items('owner-draft', [row]), [])
        row['request']['workflow_hash'] = 'old'
        row['data']['review']['binding']['workflow_hash'] = 'old'
        self.assertIn('workflow', self.items('owner-draft', [row])[0]['reason'])
        row['data']['review']['rebind_workflow_hash'] = 'current'
        self.assertEqual(self.items('owner-draft', [row]), [])
        row['data']['result']['report'] = 'changed after review'
        self.assertIn('review', self.items('owner-draft', [row])[0]['reason'])

    def test_late_expired_draft_remains_an_exception_with_a_valid_new_receipt(self):
        row = passed('draft', 'reply'); row['state'] = 'owner-draft'
        row['data'].update(draft_receipt={'remote_id': 'replacement'},
                           draft_intent_expired=[{'key': 'old-attempt', 'confirmed_at': 1}],
                           last_error='Expired draft attempt reappeared; reconcile manually: old-draft')
        # Exercise the same owner_drafts projection consumed by the operator.
        projected = {'task_id': row['id'], 'request': row['request'], 'kind': row['kind'],
                     'state': row['state'], **row['data']}
        items = self.items('owner-draft', owner_drafts=[projected])
        self.assertEqual(len(items), 1)
        self.assertIn('Expired draft intent mailbox reconciliation failed', items[0]['reason'])

    def test_publication_uses_exact_pass_exclusions_and_mathesis_descendants(self):
        note = passed('note')
        self.assertEqual(len(self.items('publication', [note])), 1)
        note['data']['publication_loop'] = {'phase': 'queued'}
        self.assertEqual(self.items('publication', [note]), [])
        for phase in ('pending', 'conflict', 'excluded'):
            note['data']['publication_loop'] = {'phase': phase, 'reason': 'selection awaits repair',
                                                'last_error': 'GateError'}
            self.assertIn(phase, self.items('publication', [note])[0]['reason'])
        note['data'].pop('publication_loop')
        child = task('child', 'mathesis', inputs={'pipeline': {'note_task_id': 'note'}})
        self.assertEqual(self.items('publication', [note, child]), [])
        child['state'] = 'failed'
        self.assertEqual(len(self.items('publication', [note, child])), 1)
        note['request']['inputs']['joint_work'] = True
        self.assertEqual(self.items('publication', [note]), [])
        note['data']['publication_loop'] = {'phase': 'excluded', 'reason': 'joint work'}
        self.assertIn('joint work', self.items('publication', [note])[0]['reason'])
        note['data']['result']['outcome'] = 'hold'
        self.assertEqual(self.items('publication', [note]), [])

    def test_mathesis_claim_confirmation_is_distinct_from_delivery_and_publication(self):
        row = task('delivery', 'mathesis', 'verified', mathesis_delivery={
            'phase': 'delivered', 'receipt': {'status': 'draft',
                'expected_claims_revision': 0, 'confirmed_claims_revision': None,
                'extracted': ['PRIVATE CLAIM']}})
        self.assertIn('needs-claims-confirmation', self.items('publication', [row])[0]['reason'])
        receipt = row['data']['mathesis_delivery']['receipt']; receipt['status'] = 'reviewing'
        self.assertEqual(len(self.items('publication', [row])), 1)
        receipt['confirmed_claims_revision'] = 0
        self.assertEqual(self.items('publication', [row]), [])
        for phase in ('conflict', 'pending'):
            row['data']['mathesis_delivery'].update(phase=phase, last_error={'type': 'HTTPError'})
            self.assertIn('HTTPError', self.items('publication', [row])[0]['reason'])
        self.assertNotIn('PRIVATE CLAIM', json.dumps(self.inventory([row])))

    def test_submission_board_matches_id_title_source_and_reports_version_stage_drift(self):
        p = paper(); self.write('submissions', {'papers': [p]})
        self.assertEqual(len(self.items('submissions')), 1)
        row = task('submission', 'submission', inputs={'paper': copy.deepcopy(p),
            'submission_source': {'paper_id': p['id'], 'identity': digest(paper_summary(p))}})
        self.assertEqual(self.items('submissions', [row]), [])
        row['request']['inputs']['submission_source']['identity'] = paper_identity(p)
        self.assertEqual(self.items('submissions', [row]), [])
        row['request']['inputs']['paper'].update(version_date='2026-10-01', stage='ready_to_submit')
        reason = self.items('submissions', [row])[0]['reason']
        self.assertIn('version', reason); self.assertIn('stage', reason)
        row['request']['inputs']['submission_source']['identity'] = 'wrong'
        self.assertIn('source identity', self.items('submissions', [row])[0]['reason'])
        row['request']['inputs']['paper']['id'] = 'wrong-id'
        self.assertIn('paper id', self.items('submissions', [row])[0]['reason'])
        row['request']['inputs'].pop('submission_source')
        row['request']['inputs']['paper'] = {'id': 'off-board', 'title': 'Other theorem'}
        self.assertEqual(len(self.items('submissions', [row])), 2)
        # Legacy submissions without a board link still match by title.
        row['request']['inputs']['paper'] = {'title': ' A SYNTHETIC theorem '}
        self.assertEqual(self.items('submissions', [row]), [])
        row['request']['inputs']['paper'] = {'id': p['id'], 'title': 'Wrong theorem'}
        self.assertIn('title mismatch', self.items('submissions', [row])[0]['reason'])

    def test_our_affairs_need_action_handle_or_explicit_wait_not_inventory_fallback(self):
        def thread(identity, **fields):
            return {'id': identity, 'stage': 'exchange', 'ball': 'us',
                    'correspondent': {'email': 'a@example.invalid'}, **fields}
        self.write('threads', {'lines': [thread('gap'), thread('action', next_action='Check proof'),
            thread('linked', job_ids=['original']), thread('waiting', waiting_for='author proof'),
            thread('dated', next_check='2026-11-03'), thread('owner', ball='owner'),
            thread('closed', stage='closed'), thread('theirs', ball='them')]})
        self.assertEqual([i['affair_id'] for i in self.items('affairs')], ['gap'])
        linked = task('work', inputs={'thread_ref': {'thread_id': 'gap'}})
        self.assertEqual(self.items('affairs', [linked]), [])

    def test_held_new_research_requires_a_child_and_failed_tasks_need_recovery_exit(self):
        held = task('held', state='held', review={'verdict': 'HOLD',
            'reason': 'Partial result only; resume with a new idea for the missing step.'})
        self.assertEqual(len(self.items('affairs', [held])), 1)
        child = task('research', 'research', inputs={'automation_parent': {'task_id': 'held'}})
        self.assertEqual(self.items('affairs', [held, child]), [])
        child['state'] = 'failed'
        self.assertEqual(len(self.items('affairs', [held, child])), 1)
        self.assertEqual(len(self.items('failed', [child])), 1)
        for field in ('requeue', 'recovery', 'continuation_id', 'successor'):
            child['data'] = {field: 'original-recovery-handle'}
            self.assertEqual(self.items('failed', [child]), [])
        child['data'] = {'capacity_retries': 2}
        self.assertEqual(len(self.items('failed', [child])), 1)
        held['data']['review']['reason'] = 'Checked deliverables complete; no rerun is needed.'
        self.assertEqual(self.items('affairs', [held]), [])

    def test_work_delivery_projections_agree_with_complete_status_rows(self):
        row = passed('mathesis', 'mathesis', publication_loop={
            'phase': 'pending', 'reason': 'source review changed', 'last_error': 'GateError'},
            mathesis_delivery={'phase': 'conflict', 'last_error': {'type': 'HTTPError'}})
        expected = self.inventory([row])
        flat = {k: v for k, v in row.items() if k != 'data'}
        flat.update(result=row['data']['result'], review=row['data']['review'])
        snapshot = {'affairs_tasks': [flat], 'publication_deliveries': [
            {'task_id': row['id'], **row['data']['publication_loop']}], 'mathesis_deliveries': [
            {'task_id': row['id'], 'state': row['state'], **row['data']['mathesis_delivery']}]}
        before = copy.deepcopy(snapshot)
        self.assertEqual(e.inventory(snapshot, self.root), expected)
        self.assertEqual(snapshot, before)
        self.assertEqual(expected['counts']['publication'], 2)

    def test_work_projections_coalesce_and_outputs_are_bounded_private_and_read_only(self):
        row = passed('draft', 'reply'); row['state'] = 'owner-draft'
        row['data']['reason'] = 'GateError: ' + 'PRIVATE ERROR ' * 1000
        flat = {k: v for k, v in row.items() if k != 'data'}; flat.update(row['data'])
        before = copy.deepcopy(flat)
        result = self.inventory(affairs_tasks=[flat], owner_drafts=[{'task_id': 'draft',
            'request': row['request'], 'state': 'owner-draft', 'result': row['data']['result']}])
        self.assertEqual(result['counts']['owner-draft'], 1)
        self.assertEqual(flat, before)
        encoded = e.render(result, 'json'); text = e.render(result, 'text')
        self.assertEqual(list(json.loads(encoded))[0], 'counts')
        self.assertTrue(text.startswith('counts:'))
        self.assertEqual(len(text.splitlines()), len(result['items']) + 1)
        for secret in ('PRIVATE ERROR', 'PRIVATE REPORT', 'PRIVATE ARTIFACT'):
            self.assertNotIn(secret, encoded + text)
        self.assertLess(len(encoded.encode()), 2000)

    def test_cli_saved_work_is_offline_and_default_restores_read_only(self):
        path = self.root / 'work.json'; path.write_text('{"tasks": []}')
        for fmt in ('json', 'text'):
            with patch.object(control, 'ROOT', self.root), patch.object(control, 'Ledger') as ledger, \
                    patch('managed.exceptions.inventory', return_value={'counts': {}, 'items': []}), \
                    patch('sys.stdout', new_callable=io.StringIO) as out:
                # config is still read by the common control CLI.
                (self.root / 'managed').mkdir(exist_ok=True)
                (self.root / 'config.json').write_text('{}')
                control.main(['exceptions', '--work', str(path), '--format', fmt])
                ledger.assert_not_called(); self.assertTrue(out.getvalue())
        with patch.object(control, 'ROOT', self.root), patch.object(control, 'Ledger') as ledger, \
                patch('managed.exceptions.inventory', return_value={'counts': {}, 'items': []}), \
                patch('sys.stdout', new_callable=io.StringIO):
            ledger.return_value.tasks.all.return_value = []
            control.main(['exceptions'])
            ledger.return_value.tasks.all.assert_called_once()
            ledger.return_value.save.assert_not_called()
            ledger.return_value.tasks.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()
