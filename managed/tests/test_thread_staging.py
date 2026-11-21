"""Immutable historical selection, Graph hydration and ordinary review/delivery gates."""
import copy
import base64
from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlsplit

from fixture_config import use_example_config
from managed import automation, delivery, mailbox, thread_staging
from managed.control import Ledger, advance, task_prompt, validate_result, workflow
from managed.queue import digest
from managed.tasks import validate_request
from test_tasks import CMA, Releases, use_fixture_workflow_revisions
from test_mailbox_attachments import AttachmentReleases


def addr(value):
    return {'emailAddress': {'address': value}}


class StagingTests(unittest.TestCase):
    def setUp(self):
        use_fixture_workflow_revisions(self)
        self.tmp = tempfile.TemporaryDirectory(); self.root = Path(self.tmp.name)
        (self.root/'tables').mkdir(); self.folder = self.root/'ledger'; self.folder.mkdir()
        self.record = {'id': 'historical', 'correspondent': {'name': 'Professor Example', 'email': 'author@example.invalid'},
                       'topic': 'Prove X', 'stage': 'exchange', 'ball': 'us',
                       'our_open_promises': [{'quote': 'We will check X'}],
                       'their_open_requests': [{'quote': 'Please prove X'}], 'next_action': 'Prove X rigorously',
                       'last_in': {'date': '2026-10-20T10:00:00Z', 'ROWID': 42, 'message_id': '<in>'},
                       'last_out': {'date': '2026-10-19T10:00:00Z', 'ROWID': 41, 'Message-ID': '<out>'},
                       'mail_match': {'emails': ['author@example.invalid'], 'subjects': ['Prove X']}}
        # Real Internet IDs contain @. Use exact header identities, never ROWIDs.
        self.record['last_in']['message_id'] = '<in@example.invalid>'
        self.record['last_out']['Message-ID'] = '<out@example.invalid>'
        self.write('threads', {'lines': [self.record]})
        self.write('contacts', {'contacts': [{'id': 'contact-a', 'name': 'Professor Example',
                                            'email': 'author@example.invalid'}]})
        self.write('refusals', {'refusals': []}); self.write('submissions', {'papers': []})
        self.remote = Releases(); self.remote.ensure = lambda: None
        self.ledger = Ledger(self.remote, self.folder); self.cma = CMA()
        self.cfg = {'paused': False, 'kinds': {'reply': True, 'research': True}, 'max_active': 2,
                    'agent_template': {}, 'automation': {'enabled': True, 'automatic_mail': True}}
        self.box = Mock()
        self.box.settings = {'reconciled': True}
        self.box.verify_owner.return_value = 'sender@example.invalid'
        self.box.conversation_for_message.return_value = 'graph-conversation'
        self.incoming = {'id': 'graph-in', 'internetMessageId': '<in@example.invalid>', 'conversationId': 'graph-conversation',
                         'isDraft': False, 'from': addr('author@example.invalid'),
                         'toRecipients': [addr('sender@example.invalid')], 'ccRecipients': [],
                         'receivedDateTime': '2026-10-20T10:00:00Z', 'body': {'contentType': 'text', 'content': 'Please prove X'}}
        self.sent = {**self.incoming, 'id': 'graph-out', 'internetMessageId': '<out@example.invalid>',
                     'from': addr('sender@example.invalid'), 'toRecipients': [addr('author@example.invalid')],
                     'sentDateTime': '2026-10-19T10:00:00Z'}
        self.box.research_thread.side_effect = lambda _: copy.deepcopy([self.sent, self.incoming])

    def tearDown(self):
        self.ledger.tasks.close(); self.tmp.cleanup()

    def write(self, name, value):
        (self.root/'tables'/(name+'.json')).write_text(json.dumps(value))

    def build(self, kind='reply', instructions=''):
        return thread_staging.build_request('historical', kind, instructions, self.root, 'b'*40)

    def tick(self, value=None, box=True):
        if value:
            self.ledger.tasks.add(value)
        # Hash computation uses actual protected files, while table gates use fixtures.
        hashes = {k: workflow(k) for k in ('reply', 'research')}
        with patch('managed.control.ROOT', self.root), patch('managed.control.workflow', side_effect=lambda k, revision=None: hashes[k]):
            advance(self.ledger, self.cma, self.cfg, self.folder, revision='b'*40, mailbox=self.box if box else None)
        return self.ledger.tasks.all()[0]

    def result(self, row, owner=False):
        packet = json.loads((Path(__file__).parent/'fixtures/letter.json').read_text())
        packet.update(kind='reply', thread_id='graph-conversation', latest_inbound_message_id='<in@example.invalid>',
                      user_decision_ever=owner)
        return {'schema_version': 1, 'task_id': row['id'], 'outcome': 'completed', 'report': 'Proof checked',
                'artifacts': [], 'packet': packet,
                'routing': {'mail': 'auto', 'questions': [], 'independent_submission': None}}

    def test_shape_and_exact_idempotent_staging_new_revision_on_changes(self):
        value = self.build(); validate_request(value)
        self.assertEqual(value['owner'], 'contact-a')
        self.assertEqual(value['inputs']['thread_record'], self.record)
        self.assertEqual(value['inputs']['thread_ref']['internetMessageIds'], ['<in@example.invalid>', '<out@example.invalid>'])
        self.assertNotIn('thread', value['inputs'])
        self.assertEqual(value, self.build())
        self.assertNotEqual(value['id'], self.build(instructions='Check a different subcase')['id'])
        self.assertNotEqual(value['id'], thread_staging.build_request('historical', 'reply', '', self.root, 'c'*40)['id'])
        with patch('managed.thread_staging.source_head', return_value='b'*40):
            a = thread_staging.stage_thread('historical', 'reply', '', self.root, self.remote, self.folder)
            b = thread_staging.stage_thread('historical', 'reply', '', self.root, self.remote, self.folder)
        self.assertEqual(a, b)
        self.assertEqual(json.loads(self.remote.assets[a['asset']]), value)

    def test_stage_inbound_reuses_thread_ref_hydration(self):
        value=thread_staging.build_inbound_request('<in@example.invalid>','contact-a',
                                                   'Please classify this message.',self.root,'b'*40)
        validate_request(value)
        self.assertEqual(value['owner'],'contact-a')
        self.assertEqual(value['inputs']['thread_ref'],
                         {'internetMessageIds':['<in@example.invalid>'],'inbound':True})
        self.assertNotIn('thread',value['inputs'])
        row=self.tick(value)
        self.assertEqual(row['state'],'running')
        self.assertEqual(row['data']['thread_inputs']['thread_id'],'graph-conversation')
        self.assertEqual(row['data']['thread_inputs']['latest_sent_message_id'],'<out@example.invalid>')

    def test_stage_inbound_refuses_active_coverage_and_replays_exact_request(self):
        with patch('managed.thread_staging.source_head', return_value='b'*40):
            args = ('in@example.invalid', 'contact-a', 'Check X', self.root, self.remote, self.folder)
            first = thread_staging.stage_inbound(*args)
            self.assertEqual(thread_staging.stage_inbound(*args), first)
            self.assertEqual(self.ledger.ingest(), [])
            row, = self.ledger.tasks.all()
            self.assertEqual(thread_staging.stage_inbound(*args), first)
            with self.assertRaisesRegex(ValueError, rf'{row["id"]} \(queued\)'):
                thread_staging.stage_inbound(*args[:2], 'Check Y', *args[3:])
        self.assertEqual([name for name in self.remote.assets if name.startswith('request-')], [first['asset']])

    def test_stage_inbound_allows_restaging_over_a_parked_task(self):
        with patch('managed.thread_staging.source_head', return_value='b'*40):
            args = ('in@example.invalid', 'contact-a', 'Check X', self.root, self.remote, self.folder)
            thread_staging.stage_inbound(*args)
            self.assertEqual(self.ledger.ingest(), [])
            row, = self.ledger.tasks.all()
            self.ledger.tasks.update(row['id'], row['revision'], 'held', {})
            self.ledger.save(force=True)
            second = thread_staging.stage_inbound(*args[:2], 'Check Y', *args[3:])
        self.assertEqual(self.ledger.ingest(), [])
        self.assertEqual(len(self.ledger.tasks.all()), 2)
        self.assertTrue(second['asset'].startswith('request-'))

    def test_ingest_rechecks_coverage_added_after_inbound_staging(self):
        with patch('managed.thread_staging.source_head', return_value='b'*40):
            staged = thread_staging.stage_inbound('in@example.invalid', 'contact-a', '',
                                                  self.root, self.remote, self.folder)
        row = self.ledger.tasks.add(self.build('reply'))
        rejected, = self.ledger.ingest()
        self.assertEqual(rejected['asset'], staged['asset'])
        self.assertIn(f'{row["id"]} (queued)', rejected['error'])
        self.assertEqual(self.ledger.tasks.all(), [row])

    def test_stage_inbound_with_owner_admits_registered_coauthor_in_conversation(self):
        self.incoming['ccRecipients'] = [addr('other@example.invalid')]
        self.write('contacts', {'contacts': [{'id': 'contact-a', 'name': 'Professor Example', 'email': 'author@example.invalid'},
                                            {'id': 'contact-b', 'name': 'Dr. Other', 'email': 'other@example.invalid'}]})
        value = thread_staging.build_inbound_request('<in@example.invalid>', 'contact-a', '', self.root, 'b'*40)
        row = self.tick(value)
        self.assertEqual(row['state'], 'running')
        self.assertEqual(row['data']['thread_inputs']['owner_ids'], ['contact-a', 'contact-b'])
        self.write('decisions', {'owner_controlled_threads': ['Dr. Other']})
        row = {'kind': 'reply', 'request': value}
        with self.assertRaisesRegex(ValueError, 'Owner-controlled'):
            thread_staging.resolve(row, self.box, self.ledger, self.root)

    def test_stage_inbound_without_owner_uses_stable_classification_owner(self):
        value=thread_staging.build_inbound_request('in@example.invalid',None,'',self.root,'b'*40)
        self.assertTrue(value['owner'].startswith('inbound-'))
        self.assertEqual(value['inputs']['owner_ids'],[])

    def test_resolves_full_current_thread_before_claim_without_mutating_request(self):
        value = self.build(); frozen = copy.deepcopy(value)
        row = self.tick(value)
        self.assertEqual(row['state'], 'running'); self.assertEqual(row['request'], frozen)
        resolved = row['data']['thread_inputs']
        self.assertEqual(resolved['thread_id'], 'graph-conversation')
        self.assertEqual(resolved['latest_sent_message_id'], '<out@example.invalid>')
        self.assertEqual(len(resolved['thread']), 2)
        self.assertIn('Please prove X', next(iter(self.cma.commands.values()))['prompt'])
        self.assertIn('COORDINATOR-RESOLVED', task_prompt(row))
        self.box.conversation_for_message.assert_any_call('<in@example.invalid>')
        self.assertIn('latest_inbound_message_id', thread_staging.effective_inputs(row))

    def test_attachment_selection_error_raises_for_explicit_resolution(self):
        self.box.research_thread.side_effect = mailbox.UnsupportedThreadAttachments([
            {'message_id': 'graph-in', 'name': 'forwarded.eml',
             'type': '#microsoft.graph.itemAttachment', 'size': 123}])
        with self.assertRaisesRegex(mailbox.UnsupportedThreadAttachments, 'forwarded.eml'):
            thread_staging.resolve({'kind': 'reply', 'request': self.build()},
                                   self.box, self.ledger, self.root)

    def test_coordinator_holds_attachment_failure_and_continues_other_staged_task(self):
        self.box.research_thread.side_effect = [mailbox.UnsupportedThreadAttachments([
            {'message_id': 'graph-in', 'name': 'forwarded.eml',
             'type': '#microsoft.graph.itemAttachment', 'size': 123}]),
            copy.deepcopy([self.sent, self.incoming])]
        first = self.build()
        second = self.build(kind='research', instructions='Check the separate mathematical question.')
        self.ledger.tasks.add(first); self.ledger.tasks.add(second)
        self.tick()
        held = self.ledger.tasks.get(first['id'])
        self.assertEqual(held['state'], 'held')
        self.assertIn('explicit supported input selection', held['data']['reason'])
        self.assertIn('forwarded.eml', held['data']['reason'])
        self.assertNotIn('thread_inputs', held['data'])
        self.assertEqual(self.ledger.tasks.get(second['id'])['state'], 'running')

    def test_newer_incoming_is_captured_and_packet_source_is_enforced(self):
        self.incoming['internetMessageId'] = '<new@example.invalid>'
        older = {**self.incoming, 'id': 'older', 'internetMessageId': '<in@example.invalid>',
                 'receivedDateTime': '2026-10-18T10:00:00Z'}
        self.box.research_thread.side_effect = lambda _: copy.deepcopy([self.sent, older, self.incoming])
        row = self.tick(self.build())
        self.assertEqual(row['data']['thread_inputs']['latest_inbound_message_id'], '<new@example.invalid>')
        result = self.result(row)
        with self.assertRaisesRegex(ValueError, 'changed its source'):
            validate_result(result, row)
        result['packet']['latest_inbound_message_id'] = '<new@example.invalid>'
        validate_result(result, row)

    def test_hydrated_new_inbound_supersedes_an_older_owner_draft(self):
        older = copy.deepcopy(self.incoming)
        request = self.build()
        request['id'] = 'older-owner-draft'
        request['inputs'].update(thread_id='graph-conversation',
                                 latest_inbound_message_id=older['internetMessageId'], thread=[older],
                                 user_decision_ever=True,
                                 user_answers=[{'answers': {'coauthorship': '同意，热情欢迎共同作者'},
                                                 'evidence_ref': 'fixture:marlow-answer'}])
        old = self.ledger.tasks.add(request)
        self.ledger.tasks.update(old['id'], old['revision'], 'owner-draft',
                                 {'draft_receipt': {'remote_id': 'saved-old-draft'}})
        self.incoming.update(internetMessageId='<new@example.invalid>', receivedDateTime='2026-10-21T10:00:00Z')
        self.box.research_thread.side_effect = lambda _: copy.deepcopy([self.sent, older, self.incoming])
        staged = self.build()
        self.tick(staged)
        old = self.ledger.tasks.get(old['id'])
        self.assertEqual(old['state'], 'superseded')
        self.assertEqual(old['data']['superseded_by'], staged['id'])
        self.assertEqual(old['data']['draft_receipt']['remote_id'], 'saved-old-draft')
        successor = self.ledger.tasks.get(staged['id'])
        self.assertEqual(successor['request']['inputs']['user_answers'][0]['evidence_ref'],
                         'fixture:marlow-answer')
        self.assertIn('Treat those questions as settled', successor['request']['inputs']['instructions'])

    def test_hydrated_chaser_carries_answers_from_held_continuation(self):
        older = copy.deepcopy(self.incoming)
        request = self.build()
        request['id'] = 'held-decision-continuation'
        request['inputs'].update(thread_id='graph-conversation',
                                 latest_inbound_message_id=older['internetMessageId'], thread=[older],
                                 user_decision_ever=True,
                                 user_answers=[{'answers': {'coauthorship': '同意'},
                                                 'evidence_ref': 'fixture:held-answer'},
                                                ],
                                 automation_parent={'purpose': 'user-answer'})
        old = self.ledger.tasks.add(request)
        self.ledger.tasks.update(old['id'], old['revision'], 'held', {'reason': 'Revision is not registered continuation'})
        self.incoming.update(internetMessageId='<new-held@example.invalid>',
                             receivedDateTime='2026-10-21T10:00:00Z')
        self.box.research_thread.side_effect = lambda _: copy.deepcopy([self.sent, older, self.incoming])
        staged = self.build()
        self.tick(staged)
        successor = self.ledger.tasks.get(staged['id'])
        self.assertEqual(successor['request']['inputs']['user_answers'][0]['evidence_ref'],
                         'fixture:held-answer')
        self.assertEqual(self.ledger.tasks.get(old['id'])['state'], 'superseded')

    def test_owner_decision_history_is_preserved_without_owning_later_letters(self):
        self.record['ball'] = 'owner'; self.write('threads', {'lines': [self.record]})
        value = self.build(); self.assertTrue(value['inputs']['user_decision_ever'])
        row = self.tick(value)
        with self.assertRaisesRegex(ValueError, 'cannot clear'):
            validate_result(self.result(row), row)
        result = self.result(row, owner=True); validate_result(result, row)
        row['data']['result'] = result
        self.assertEqual(automation.approved_route(row), 'awaiting-mailbox')
        current = self.ledger.tasks.get(row['id'])
        self.ledger.tasks.update(row['id'], current['revision'], 'bounced', {**row['data'], 'review': {'reason': 'Improve proof'}})
        automation.reconcile(self.ledger, self.cfg, self.root, revision='b'*40)
        child = self.ledger.tasks.all()[-1]
        self.assertTrue(child['request']['inputs']['user_decision_ever'])
        self.assertEqual(child['request']['inputs']['thread_id'], 'graph-conversation')

    def test_decision_history_after_staging_is_hydrated_without_owning_later_letter(self):
        value = self.build(); self.record['ball'] = 'owner'; self.write('threads', {'lines': [self.record]})
        row = self.tick(value)
        self.assertFalse(row['request']['inputs']['user_decision_ever'])
        self.assertTrue(thread_staging.effective_inputs(row)['user_decision_ever'])
        with self.assertRaisesRegex(ValueError, 'cannot clear'):
            validate_result(self.result(row), row)
        row['data']['result'] = self.result(row, owner=True)
        self.assertEqual(automation.approved_route(row), 'awaiting-mailbox')

    def test_delivery_uses_hydrated_sent_identity_and_preserves_owner_draft(self):
        self.record['ball'] = 'owner'; self.write('threads', {'lines': [self.record]})
        row = self.tick(self.build()); result = self.result(row, owner=True)
        result['routing']['mail'] = 'owner'
        self.ledger.tasks.update(row['id'], row['revision'], 'owner-draft',
                                 {**row['data'], 'result': result, 'review': {'verdict': 'PASS',
                                  'binding': {'task_id': row['id'], 'result_hash': digest(result),
                                              'workflow_hash': row['request']['workflow_hash']}}})
        self.box.save_draft.return_value = {'content_hash': digest(result['packet'])}
        self.box.preflight.return_value = {}
        delivery.advance(self.ledger, self.cfg, self.root, self.box)
        supplied = self.box.preflight.call_args.args[1]['inputs']
        self.assertEqual(supplied['latest_sent_message_id'], '<out@example.invalid>')
        self.box.save_draft.assert_called_once(); self.box.submit.assert_not_called()

    def test_resolution_failures_hold_visibly_before_any_cma_call(self):
        self.box.conversation_for_message.side_effect = ValueError('Message not found')
        row = self.tick(self.build())
        self.assertEqual(row['state'], 'held')
        self.assertIn('Thread resolution failed', row['data']['reason'])
        self.assertIn('Message not found', row['data']['reason']); self.assertFalse(self.cma.creates)

    def test_transient_graph_errors_stay_queued_then_resolve(self):
        from urllib.error import HTTPError
        self.box.conversation_for_message.side_effect = HTTPError('https://graph', 401, 'Unauthorized', {}, None)
        row = self.tick(self.build())
        self.assertEqual(row['state'], 'queued')
        self.assertEqual(row['data']['resolution_retries'], 1)
        self.assertIn('HTTP Error 401', row['data']['last_error']); self.assertFalse(self.cma.creates)

    def test_held_401_resolution_is_retried(self):
        row = self.ledger.tasks.add(self.build())
        self.ledger.tasks.update(row['id'], row['revision'], 'held',
                                 {'reason': 'Thread resolution failed: HTTPError: HTTP Error 401: Unauthorized'})
        row = self.tick()
        self.assertNotEqual(row['state'], 'held')

    def test_unconfigured_mailbox_holds_visibly(self):
        row = self.tick(self.build(), box=False)
        self.assertEqual(row['state'], 'held')
        self.assertIn('Configured GraphMailbox unavailable', row['data']['reason'])
        self.assertFalse(self.cma.creates)

    def test_review_inventory_includes_resolved_thread_and_prior_decision_survives(self):
        self.record['user_decisions'] = [{'question': 'Meet?', 'done': '2026-10-20'}]
        self.write('threads', {'lines': [self.record]})
        row = self.tick(self.build()); self.assertTrue(row['request']['inputs']['user_decision_ever'])
        self.ledger.tasks.update(row['id'], row['revision'], 'awaiting-review',
                                 {**row['data'], 'result': self.result(row, owner=True)})
        review = self.ledger.tasks.reviews()[0]
        self.assertEqual(review['request'], row['request'])
        self.assertEqual(review['resolved_inputs'], row['data']['thread_inputs'])
        self.assertIn('Please prove X', json.dumps(review['resolved_inputs']))

    def test_missing_message_id_is_staged_as_rowid_then_held_without_guessing(self):
        self.record['last_in'].pop('message_id'); self.record['last_out'].pop('Message-ID')
        self.write('threads', {'lines': [self.record]})
        value = self.build(); self.assertEqual(value['inputs']['thread_ref']['internetMessageIds'], [])
        row = self.tick(value)
        self.assertIn('No known internetMessageId', row['data']['reason'])
        self.assertFalse(self.cma.creates)

    def test_reuses_only_exact_latest_rowid_receipt_from_registry(self):
        self.record['last_in'].pop('message_id'); self.write('threads', {'lines': [self.record]})
        self.write('contacts', {'contacts': [{'id': 'contact-a', 'name': 'Professor Example', 'email': 'author@example.invalid',
            'hub': {'historical_inbound': [{'rowid': 42, 'message_id': 'known@example.invalid'},
                                          {'rowid': 40, 'message_id': 'old@example.invalid'}]}}]})
        ids = self.build()['inputs']['thread_ref']['internetMessageIds']
        self.assertIn('<known@example.invalid>', ids); self.assertNotIn('<old@example.invalid>', ids)

    def refusal(self):
        self.write('refusals', {'refusals': [{'id': 'refusal-a', 'name': 'Professor Example',
            'emails': ['author@example.invalid'], 'active': True}]})

    def test_refusal_blocks_staging_and_fresh_admission(self):
        value = self.build(); self.refusal()
        with self.assertRaisesRegex(ValueError, 'table-3 refusal'):
            self.build()
        row = self.tick(value)
        self.assertEqual(row['state'], 'held'); self.assertFalse(self.cma.creates)
        self.assertIn('table-3 refusal', row['data']['reason'])

    def test_owner_controlled_line_blocks_like_ordinary_reply(self):
        self.write('decisions', {'owner_controlled_threads': ['Professor Example']})
        with self.assertRaisesRegex(ValueError, 'Owner-controlled'):
            self.build()

    def test_owner_controlled_participant_in_prior_context_blocks_other_owner(self):
        # A co-recipient writes a new conversation; the earlier messages arrive
        # directly in prior_context and include an owner-controlled person.
        self.write('contacts', {'contacts': [{'id': 'contact-a', 'name': 'Professor Example', 'email': 'author@example.invalid'},
                                            {'id': 'contact-b', 'name': 'Dr. Other', 'email': 'other@example.invalid'}]})
        prior = {**self.sent, 'ccRecipients': [addr('other@example.invalid')]}
        request = {'owner': 'contact-a', 'inputs': {'owner_ids': ['contact-a'], 'thread': [self.incoming],
                                                    'prior_context': [prior]}}
        thread_staging.gate(request, self.root)
        self.write('decisions', {'owner_controlled_threads': ['Dr. Other']})
        with self.assertRaisesRegex(ValueError, 'Owner-controlled participant'):
            thread_staging.gate(request, self.root)

    def test_mismatched_conversations_or_existing_draft_hold(self):
        self.box.conversation_for_message.side_effect = ['one', 'two']
        row = self.tick(self.build())
        self.assertIn('mismatched current conversation', row['data']['reason']); self.assertFalse(self.cma.creates)
        self.box.conversation_for_message.side_effect = None
        self.incoming['isDraft'] = True
        with self.assertRaisesRegex(ValueError, 'server draft'):
            thread_staging.resolve(row, self.box, self.ledger, self.root)

    def split_conversations(self):
        self.threads = {'graph-conversation': [self.incoming],
                        'legacy-sent': [{**self.sent, 'conversationId': 'legacy-sent'}]}
        self.locations = {'<in@example.invalid>': 'graph-conversation', '<out@example.invalid>': 'legacy-sent'}
        self.box.conversation_for_message.side_effect = lambda i: self.locations.get(i)
        self.box.research_thread.side_effect = lambda c: copy.deepcopy(self.threads[c])

    def test_split_conversations_choose_incoming_and_keep_read_only_sent_context(self):
        self.split_conversations()
        # A newer unthreaded send must not select a Sent-only conversation.
        self.threads['legacy-sent'][0]['sentDateTime'] = '2026-10-21T10:00:00Z'
        value = self.build(); frozen = copy.deepcopy(value)
        row = self.tick(value)
        self.assertEqual(row['state'], 'running')
        self.assertEqual(row['request'], frozen)
        resolved = row['data']['thread_inputs']
        self.assertEqual(resolved['thread_id'], 'graph-conversation')
        self.assertEqual(resolved['latest_inbound_message_id'], '<in@example.invalid>')
        self.assertEqual(resolved['thread'], [self.incoming])
        self.assertEqual(resolved['prior_context'], [{'thread_id': 'legacy-sent', 'read_only': True,
                                                     'thread': self.threads['legacy-sent']}])
        self.assertEqual(resolved['sent_evidence'], {'thread_id': 'legacy-sent', 'message_id': '<out@example.invalid>'})
        self.assertNotIn('latest_sent_message_id', resolved)
        self.assertEqual(resolved['unresolved_message_ids'], [])
        self.assertIn('legacy-sent', task_prompt(row))
        # The fresh Sent check remains scoped to the target conversation.
        box = mailbox.GraphMailbox(Mock(), {'account': 'sender@example.invalid', 'reconciled': True})
        box.verify_owner = lambda: 'sender@example.invalid'
        box.thread = lambda _: copy.deepcopy(resolved['thread'])
        packet = self.result(row)['packet']
        packet.update(sender='sender@example.invalid', to=['author@example.invalid'], cc=[])
        box.preflight(packet, {**row['request'], 'inputs': thread_staging.effective_inputs(row)})
        box.thread = lambda _: copy.deepcopy([self.incoming, self.sent])
        with self.assertRaisesRegex(ValueError, 'Actual Sent changed'):
            box.preflight(packet, {**row['request'], 'inputs': thread_staging.effective_inputs(row)})

    def test_latest_incoming_across_hydrated_conversations_selects_target(self):
        self.split_conversations()
        newer = {**self.incoming, 'id': 'newer', 'internetMessageId': '<newer@example.invalid>',
                 'conversationId': 'legacy-sent', 'receivedDateTime': '2026-10-22T10:00:00Z'}
        self.threads['legacy-sent'].append(newer)
        row = self.tick(self.build())
        resolved = row['data']['thread_inputs']
        self.assertEqual(resolved['thread_id'], 'legacy-sent')
        self.assertEqual(resolved['latest_inbound_message_id'], '<newer@example.invalid>')
        self.assertEqual(resolved['latest_sent_message_id'], '<out@example.invalid>')
        self.assertEqual(resolved['prior_context'][0]['thread_id'], 'graph-conversation')

    def test_target_and_prior_attachments_are_preserved_and_read_back(self):
        self.split_conversations()
        self.remote = AttachmentReleases(); self.ledger.releases = self.remote
        payloads = [b'target attachment', b'prior attachment']
        for messages, raw in zip(self.threads.values(), payloads):
            messages[0]['attachments'] = [{'name': 'note.txt', 'contentBytes': base64.b64encode(raw).decode()}]
        row = self.tick(self.build())
        self.assertEqual(row['state'], 'running')
        resolved = row['data']['thread_inputs']
        attachments = [resolved['thread'][0]['attachments'][0],
                       resolved['prior_context'][0]['thread'][0]['attachments'][0]]
        for attachment, raw in zip(attachments, payloads):
            self.assertNotIn('contentBytes', attachment)
            ref = attachment['selected_input']
            self.assertEqual(self.remote.files[ref['release_tag'], ref['name']], raw)
            self.assertEqual(ref['bytes'], len(raw))
        self.assertEqual([op for op, _ in self.remote.calls], ['lookup', 'create', 'upload', 'list'] * 2)

    def test_unresolved_ids_are_recorded_when_an_incoming_id_resolves(self):
        self.locations = {'<in@example.invalid>': 'graph-conversation'}
        self.box.conversation_for_message.side_effect = lambda i: self.locations.get(i)
        row = self.tick(self.build())
        self.assertEqual(row['state'], 'running')
        self.assertEqual(row['data']['thread_inputs']['unresolved_message_ids'], ['<out@example.invalid>'])

    def test_no_resolved_incoming_id_holds_even_if_sent_context_contains_incoming(self):
        self.box.conversation_for_message.side_effect = lambda i: 'graph-conversation' if i == '<out@example.invalid>' else None
        row = self.tick(self.build())
        self.assertEqual(row['state'], 'held')
        self.assertIn('No known incoming internetMessageId resolves', row['data']['reason'])
        self.assertFalse(self.cma.creates)

    def test_all_unresolved_ids_hold_before_cma(self):
        self.box.conversation_for_message.return_value = None
        row = self.tick(self.build())
        self.assertEqual(row['state'], 'held'); self.assertFalse(self.cma.creates)
        self.box.research_thread.assert_not_called()

    def test_prior_context_refusal_blocks_but_its_owners_and_drafts_do_not_change_target(self):
        self.split_conversations()
        self.threads['legacy-sent'][0]['ccRecipients'] = [addr('other@example.invalid')]
        self.threads['legacy-sent'].append({**self.threads['legacy-sent'][0], 'id': 'prior-draft', 'isDraft': True})
        self.write('contacts', {'contacts': [{'id': 'contact-a', 'name': 'Professor Example', 'email': 'author@example.invalid'},
                                            {'id': 'contact-b', 'name': 'Dr. Other', 'email': 'other@example.invalid'}]})
        value = self.build()
        # Simulate a selection containing only the target owner.
        value['inputs']['owner_ids'] = ['contact-a']
        row = self.tick(value)
        self.assertEqual(row['state'], 'running')
        self.assertEqual(row['data']['thread_inputs']['owner_ids'], ['contact-a'])
        self.write('refusals', {'refusals': [{'id': 'prior-refusal', 'name': 'Dr. Other',
                                            'emails': ['other@example.invalid'], 'active': True}]})
        with self.assertRaisesRegex(ValueError, 'table-3 refusal: other@example.invalid'):
            thread_staging.resolve(row, self.box, self.ledger, self.root)

    def test_split_target_still_enforces_participants_owners_drafts_and_sent_evidence(self):
        self.split_conversations()
        row = {'kind': 'reply', 'request': self.build()}
        self.incoming['isDraft'] = True
        self.threads['graph-conversation'].append({**self.incoming, 'isDraft': False})
        with self.assertRaisesRegex(ValueError, 'server draft'):
            thread_staging.resolve(row, self.box, self.ledger, self.root)
        self.incoming['isDraft'] = False
        self.incoming['from'] = addr('stranger@example.invalid')
        with self.assertRaisesRegex(ValueError, 'outside the selected thread'):
            thread_staging.resolve(row, self.box, self.ledger, self.root)
        self.incoming['from'] = addr('author@example.invalid')
        self.incoming['ccRecipients'] = [addr('other@example.invalid')]
        self.write('contacts', {'contacts': [{'id': 'contact-a', 'name': 'Professor Example', 'email': 'author@example.invalid'},
                                            {'id': 'contact-b', 'name': 'Dr. Other', 'email': 'other@example.invalid'}]})
        with self.assertRaisesRegex(ValueError, 'adds registered owners'):
            thread_staging.resolve(row, self.box, self.ledger, self.root)
        self.incoming['ccRecipients'] = []
        self.threads['legacy-sent'][0]['from'] = addr('author@example.invalid')
        with self.assertRaisesRegex(ValueError, 'actual incoming and Sent evidence'):
            thread_staging.resolve(row, self.box, self.ledger, self.root)

    def test_prior_context_refusal_is_rechecked_on_result_and_before_mailbox_write(self):
        self.split_conversations()
        self.threads['legacy-sent'][0]['ccRecipients'] = [addr('other@example.invalid')]
        row = self.tick(self.build())
        self.write('refusals', {'refusals': [{'id': 'prior-refusal', 'name': 'Dr. Other',
                                            'emails': ['other@example.invalid'], 'active': True}]})
        result = self.result(row)
        self.cma.response = lambda _: {'status': 'completed', 'output': [{'type': 'message', 'content': [
            {'type': 'output_text', 'text': json.dumps(result)}]}]}
        row = self.tick()
        self.assertEqual(row['state'], 'held')
        self.assertIn('table-3 refusal: other@example.invalid', row['data']['repair_error'])
        self.assertNotIn('result', row['data'])
        # Even a previously reviewed owner draft cannot bypass a new boundary.
        result = self.result(row, owner=True)
        row = self.ledger.tasks.update(row['id'], row['revision'], 'owner-draft',
            {**row['data'], 'result': result, 'review': {'verdict': 'PASS',
             'binding': {'task_id': row['id'], 'result_hash': digest(result),
                         'workflow_hash': row['request']['workflow_hash']}}})
        delivery.advance(self.ledger, self.cfg, self.root, self.box)
        self.assertIn('table-3 refusal: other@example.invalid', self.ledger.tasks.get(row['id'])['data']['last_error'])
        self.box.preflight.assert_not_called(); self.box.save_draft.assert_not_called(); self.box.submit.assert_not_called()

    def test_legacy_hold_retries_automatically_but_unrelated_or_started_holds_do_not(self):
        self.split_conversations()
        row = self.ledger.tasks.add(self.build())
        row = self.ledger.tasks.update(row['id'], row['revision'], 'held', {'reason': thread_staging.LEGACY_RESOLUTION_REASON})
        self.cfg['paused'] = True
        self.assertEqual(self.tick()['state'], 'held')
        self.box.verify_owner.assert_not_called()
        self.cfg['paused'] = False; self.cfg['kinds']['reply'] = False
        self.assertEqual(self.tick()['state'], 'held')
        self.box.verify_owner.assert_not_called()
        self.cfg['kinds']['reply'] = True
        row = self.tick()
        self.assertEqual(row['state'], 'running')
        self.assertEqual(len(self.cma.creates), 1)
        self.assertNotIn('reason', row['data'])

    def test_other_resolution_holds_and_started_legacy_holds_are_not_retried(self):
        row = self.ledger.tasks.add(self.build())
        for data in ({'reason': 'Thread resolution failed: GateError: A server draft already exists'},
                     {'reason': thread_staging.LEGACY_RESOLUTION_REASON, 'agent_id': 'agt_existing'}):
            row = self.ledger.tasks.update(row['id'], row['revision'], 'held', data)
            row = self.tick()
            self.assertEqual(row['state'], 'held')
        self.box.verify_owner.assert_not_called(); self.assertFalse(self.cma.creates)

    def test_hydration_bounds_fail_closed(self):
        self.split_conversations()
        value = self.build()
        value['inputs']['thread_ref']['internetMessageIds'] += ['<third@example.invalid>', '<fourth@example.invalid>']
        self.locations.update({'<third@example.invalid>': 'third', '<fourth@example.invalid>': 'fourth'})
        row = {'kind': 'reply', 'request': value}
        with self.assertRaisesRegex(ValueError, 'exceeds 3 conversations'):
            thread_staging.resolve(row, self.box, self.ledger, self.root)
        self.box.research_thread.assert_not_called()
        row['request'] = self.build()
        self.threads['legacy-sent'] *= 101
        with self.assertRaisesRegex(ValueError, 'exceeds 100 messages'):
            thread_staging.resolve(row, self.box, self.ledger, self.root)

    def test_paused_tasks_never_read_mailbox(self):
        self.cfg['paused'] = True
        row = self.tick(self.build())
        self.assertEqual(row['state'], 'queued'); self.box.verify_owner.assert_not_called()

    def test_research_kind_uses_same_hydration_with_no_mail_authority(self):
        row = self.tick(self.build(kind='research'))
        self.assertEqual(row['kind'], 'research'); self.assertEqual(row['state'], 'running')
        result = self.result(row); result['routing']['mail'] = 'none'; result['packet'] = None
        validate_result(result, row)
        result['routing']['mail'] = 'auto'
        with self.assertRaisesRegex(ValueError, 'cannot send mail'):
            validate_result(result, row)


class GraphIdentityTests(unittest.TestCase):
    def test_bounded_prior_sent_context_selects_one_newest_conversation(self):
        graph=Mock()
        sent=[{'id':'older','internetMessageId':'<older@example.invalid>','conversationId':'old',
               'sentDateTime':'2026-10-20T10:00:00Z','toRecipients':[addr('author@example.invalid')],
               'ccRecipients':[],'isDraft':False},
              {'id':'newer','internetMessageId':'<newer@example.invalid>','conversationId':'new',
               'sentDateTime':'2026-10-30T10:00:00Z','toRecipients':[addr('author@example.invalid')],
               'ccRecipients':[],'isDraft':False}]
        thread=[{**sent[1],'body':{'contentType':'text','content':'latest'}}]
        graph.pages.side_effect=[sent,thread]
        box=mailbox.GraphMailbox(graph,{'account':'me@example.invalid'})
        self.assertEqual(box.sent_messages_to('author@example.invalid'),thread)
        query=parse_qs(urlsplit(graph.pages.call_args_list[0].args[0]).query)
        self.assertEqual(query['$top'],['25'])
        self.assertEqual(query['$search'],['"participants:author@example.invalid"'])
        self.assertNotIn('$filter',query)

    def test_inbound_header_links_to_registered_sent_recipient(self):
        graph=Mock();sent={'id':'sent','internetMessageId':'<sent@example.invalid>',
            'conversationId':'old','sentDateTime':'2026-10-30T10:00:00Z',
            'from':addr('me@example.invalid'),'toRecipients':[addr('author@example.invalid')],
            'ccRecipients':[],'subject':'A proof','isDraft':False}
        graph.pages.return_value=[sent]
        box=mailbox.GraphMailbox(graph,{'account':'me@example.invalid'})
        inbound={'internetMessageHeaders':[{'name':'In-Reply-To','value':'<sent@example.invalid>'}],
                 'receivedDateTime':'2026-10-31T10:00:00Z'}
        person={'id':'contact-a','name':'Professor Example','emails':['author@example.invalid']}
        match=box.match_inbound_sent(inbound,[person])
        self.assertEqual(match['person']['id'],'contact-a')
        self.assertEqual(match['match'],'internet-message-id')

    def test_inbound_subject_requires_one_recent_sent_message_and_a_substantive_reply_subject(self):
        graph=Mock();sent={
            'id':'sent','internetMessageId':'<sent@example.invalid>','conversationId':'old',
            'sentDateTime':'2026-08-02T10:00:00Z','from':addr('me@example.invalid'),
            'toRecipients':[addr('author@example.invalid')],'ccRecipients':[],
            'subject':'A result on sets','isDraft':False}
        graph.pages.return_value=[sent]
        box=mailbox.GraphMailbox(graph,{'account':'me@example.invalid'})
        person={'id':'contact-a','name':'Professor Example','emails':['author@example.invalid']}
        inbound={'subject':'Fwd: A result on sets','receivedDateTime':'2026-10-31T10:00:00Z'}
        self.assertEqual(box.match_inbound_sent(inbound,[person])['match'],'subject-within-90-days')
        for subject in ('A result on sets', 'Re: Hello', 'Re: Ultrametric spaces', 'Re: A short note'):
            with self.subTest(subject=subject):
                graph.pages.reset_mock()
                self.assertIsNone(box.match_inbound_sent({**inbound,'subject':subject},[person]))
                graph.pages.assert_not_called()
        for rows in ([{**sent,'sentDateTime':'2026-08-01T10:00:00Z'}],
                     [sent,{**sent,'internetMessageId':'<other@example.invalid>'}]):
            with self.subTest(rows=rows):
                graph.pages.return_value=rows
                self.assertIsNone(box.match_inbound_sent(inbound,[person]))

    def test_odata_identity_lookup_is_exact_escaped_and_unambiguous(self):
        graph = Mock(); identity = "<a'b@example.invalid>"
        graph.pages.return_value = [{'internetMessageId': identity, 'conversationId': 'c'}]
        box = mailbox.GraphMailbox(graph, {'account': 'me@example.invalid'})
        self.assertEqual(box.conversation_for_message(identity), 'c')
        query = parse_qs(urlsplit(graph.pages.call_args.args[0]).query)
        self.assertEqual(query['$filter'], ["internetMessageId eq '<a''b@example.invalid>'"])
        graph.pages.return_value = []
        self.assertIsNone(box.conversation_for_message(identity))
        graph.pages.return_value = [{'internetMessageId': identity, 'conversationId': c} for c in ['a', 'b']]
        with self.assertRaisesRegex(ValueError, 'ambiguous'):
            box.conversation_for_message(identity)


if __name__ == '__main__':
    unittest.main()


def setUpModule():
    use_example_config()
