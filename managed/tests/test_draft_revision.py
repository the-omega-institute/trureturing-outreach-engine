"""Owner-draft replacement safety and remote-checkpoint recovery, all offline."""
import base64
import copy
import hashlib
import hmac
import io
import json
from contextlib import redirect_stdout
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from fixture_config import use_cli_config, use_example_config
from managed import automation, control, delivery, mailbox
from managed.queue import GateError, canonical, digest, validate_packet
from test_reply_threading import DraftGraph
from test_tasks import Releases, request


SECRET = 'offline-draft-revision-secret'
OWNER_EDITED = 'Owner edited the draft; revision not applied'


class ProcessDeath(BaseException):
    """Bypass ordinary coordinator error handling and end-of-tick saves."""


class DraftRevisionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        (self.root / 'tables').mkdir()
        self.folder = self.root / 'ledger'
        self.folder.mkdir()
        self.remote = Releases()
        self.ledger = control.Ledger(self.remote, self.folder)
        self.addCleanup(self.temp.cleanup)
        self.addCleanup(lambda: self.ledger.tasks.close())
        self.config = {'paused': False, 'kinds': {'reply': True},
                       'automation': {'automatic_mail': True}}
        self.packet = json.loads((Path(__file__).parent / 'fixtures/letter.json').read_text())
        self.packet.update(kind='reply', thread_id='thread-1',
                           latest_inbound_message_id='<inbound-1>', user_decision_ever=True)
        # Exercise both To and Cc boundaries and exact attachment bytes.
        self.packet['cc'] = ['colleague@example.invalid']
        self.tables('contacts', {'contacts': [
            {'id': 'contact-' + str(i), 'name': 'Professor Example', 'email': address,
             'problem_ids': [], 'hub': {'workspace_paths': ['../mail/person']}}
            for i, address in enumerate(self.packet['to'] + self.packet['cc'])]})
        self.tables('refusals', {'refusals': []})
        req = request('owner-reply', 'contact-0', 'reply')
        req['inputs'] = {'user_decision_ever': True}
        row = self.ledger.tasks.add(req)
        self.result = {'schema_version': 1, 'task_id': row['id'], 'outcome': 'completed',
                       'report': 'Synthetic checked reply.', 'artifacts': [],
                       'packet': copy.deepcopy(self.packet),
                       'routing': {'mail': 'owner', 'questions': [], 'independent_submission': None}}
        row = self.ledger.tasks.update(row['id'], row['revision'], 'awaiting-review',
                                       {'result': self.result})
        envelope = control.sign_review({
            'binding': self.ledger.tasks.review_binding(row), 'reviewer': 'local-claude-loop',
            'verdict': 'PASS', 'reason': 'Synthetic independent review.',
            'evidence_ref': 'fixture:review'}, SECRET)
        row = self.ledger.tasks.verdict(envelope, SECRET)
        self.task_id = row['id']
        self.graph = DraftGraph(self.packet)
        self.graph.inbound['ccRecipients'] = [mailbox_participant(a) for a in self.packet['cc']]
        self.box = mailbox.GraphMailbox(self.graph, {'account': self.packet['sender'], 'reconciled': True})
        self.old_receipt = self.box.save_draft(self.packet, 'original-attempt')
        self.old_id = self.old_receipt['remote_id']
        self.ledger.tasks.update(row['id'], row['revision'], 'owner-draft',
                                 {**row['data'], 'draft_receipt': self.old_receipt})
        # An unrelated draft must survive every path, including uncertain DELETE.
        other = self.graph.message('unrelated-owner-draft', 'unrelated-attempt', wrong=True)
        self.graph.drafts[other['id']] = other
        self.graph.attachments[other['id']] = self.graph.packet_attachments()
        self.unrelated = copy.deepcopy(other)
        self.new_packet = copy.deepcopy(self.packet)
        self.new_packet['body'] = 'Dear Professor Example,\n\nThe corrected synthetic value is 7.\n'
        self.new_packet['attachments'] = [{
            'name': 'corrected-note.txt', 'media_type': 'text/plain',
            'content_b64': base64.b64encode(b'Corrected synthetic attachment.\n').decode()}]
        self.graph.calls.clear()
        self.ledger.save(force=True)

    def tables(self, name, value):
        (self.root / 'tables' / (name + '.json')).write_text(canonical(value))

    def row(self):
        return self.ledger.tasks.get(self.task_id)

    def update(self, **data):
        row = self.row()
        return self.ledger.tasks.update(row['id'], row['revision'], row['state'], {**row['data'], **data})

    def envelope(self, packet=None):
        value = {'schema_version': 1, 'task_id': self.task_id,
                 'old_packet_hash': digest(self.packet),
                 'new_packet_hash': digest(packet or self.new_packet),
                 'reason': 'Correct the synthetic calculation and note.',
                 'packet': copy.deepcopy(packet or self.new_packet)}
        value['signature'] = hmac.new(SECRET.encode(), canonical(value).encode(), hashlib.sha256).hexdigest()
        return value

    def stage(self, envelope=None):
        """Use immutable fake Release assets and real signature/ledger ingest."""
        value = envelope or self.envelope()
        name = 'draft-revision-' + digest(value) + '.json'
        self.remote.assets[name] = canonical(value).encode()
        self.assertEqual(self.ledger.ingest(SECRET), [])
        return value

    def cli_stage(self, packet=None):
        path = self.root / 'replacement.json'
        path.write_text(canonical(packet or self.new_packet))
        self.ledger.save(force=True)
        with patch.object(control, 'Releases', return_value=self.remote), \
                patch.object(self.remote, 'ensure', create=True), \
                patch.object(control, 'review_key', return_value=SECRET), \
                redirect_stdout(io.StringIO()) as output:
            control.main(['stage-draft-revision', '--task', self.task_id, '--packet', str(path),
                          '--reason', 'Correct the synthetic calculation and note.'])
        return json.loads(output.getvalue())

    def tick(self, config=None):
        delivery.advance(self.ledger, config or self.config, self.root, self.box)

    def restored_row(self):
        """Read the published snapshot, never the live/local SQLite database."""
        with tempfile.TemporaryDirectory(dir=self.root) as folder:
            recovered = control.Ledger(self.remote, folder)
            try:
                return recovered.tasks.get(self.task_id)
            finally:
                recovered.tasks.close()

    def restart(self):
        self.ledger.tasks.close()
        self.folder = Path(tempfile.mkdtemp(dir=self.root))
        self.ledger = control.Ledger(self.remote, self.folder)

    def assert_no_writes(self):
        self.assertEqual(len(self.graph.mimes), 1, 'A replacement was unexpectedly created')
        self.assertFalse([c for c in self.graph.calls if c[1] != 'GET'])

    def assert_applied(self):
        row = self.row()
        data = row['data']
        self.assertEqual(row['state'], 'owner-draft')
        self.assertEqual(data['result'], self.result)
        self.assertEqual(data['packet_override'], self.new_packet)
        self.assertNotIn('last_error', data)
        self.assertNotIn('draft_revision_intent', data)
        self.assertEqual(data['draft_revision_requests'], [])
        self.assertEqual(len(data['draft_revisions']), 1)
        record = data['draft_revisions'][0]
        self.assertEqual(record['old_packet_hash'], digest(self.packet))
        self.assertEqual(record['new_packet_hash'], digest(self.new_packet))
        self.assertEqual(record['reason'], self.envelope()['reason'])
        receipt = data['draft_receipt']
        self.assertEqual(record['new_draft_receipt'], receipt)
        self.assertEqual(receipt['content_hash'], digest(self.new_packet))
        self.assertNotEqual(receipt['attempt_key'], self.old_receipt['attempt_key'])
        self.assertEqual(record['old_draft_deleted']['remote_id'], self.old_id)
        self.assertIs(record['old_draft_deleted']['verified_absent'], True)
        self.assertNotIn(self.old_id, self.graph.drafts)
        self.assertEqual(set(self.graph.drafts), {receipt['remote_id'], self.unrelated['id']})
        self.assertEqual(self.graph.drafts[self.unrelated['id']], self.unrelated)
        replacement = self.box.message(receipt['remote_id'])
        self.assertTrue(self.box.matches(replacement, self.new_packet))
        self.assertEqual(replacement['conversationId'], self.packet['thread_id'])
        self.box.own_draft(replacement, self.new_packet, receipt['attempt_key'])
        self.assertEqual(len(self.graph.mimes), 2, 'More than one replacement was created')
        deletes = [c[0] for c in self.graph.calls if c[1] == 'DELETE']
        self.assertEqual(deletes, ['/me/messages/' + self.old_id])
        self.assertFalse(any(c[0].endswith('/send') for c in self.graph.calls))

    def test_cli_stages_body_and_attachment_revision_with_valid_signature(self):
        staged = self.cli_stage()
        self.assertTrue(staged['asset'].startswith('draft-revision-'))
        value = json.loads(self.remote.assets[staged['asset']])
        self.assertEqual(value, self.envelope())
        self.assertEqual(staged['sha256'], digest(value))
        signed = {k: v for k, v in value.items() if k != 'signature'}
        self.assertTrue(hmac.compare_digest(value['signature'], hmac.new(
            SECRET.encode(), canonical(signed).encode(), hashlib.sha256).hexdigest()))
        self.assertEqual(self.ledger.ingest(SECRET), [])
        self.assertEqual(len(self.row()['data']['draft_revision_requests']), 1)
        self.assert_no_writes()

    def test_staging_rejects_every_immutable_packet_field_in_cli_and_ingest(self):
        for field, value in [
                ('kind', 'firstcontact'), ('sender', 'other@example.invalid'),
                ('thread_id', 'other-thread'), ('latest_inbound_message_id', '<other-inbound>'),
                ('to', ['other@example.invalid']), ('cc', []), ('subject', 'Different subject'),
                ('user_decision_ever', False), ('problem_ids', ['other-problem'])]:
            with self.subTest(field=field):
                packet = copy.deepcopy(self.new_packet)
                packet[field] = value
                if field == 'kind':
                    for key in ('thread_id', 'latest_inbound_message_id', 'user_decision_ever'):
                        packet.pop(key)
                validate_packet(packet)  # Rejection must be about immutability.
                before = self.row()
                with self.assertRaises(GateError):
                    self.cli_stage(packet)
                with self.assertRaises(GateError):
                    self.ledger.tasks.draft_revision(self.envelope(packet), SECRET)
                self.assertEqual(self.row(), before)
        self.assert_no_writes()

    def test_staging_requires_owner_draft_and_matching_remote_receipt(self):
        original = self.row()
        for state, receipt in [('awaiting-mailbox', self.old_receipt), ('owner-draft', None),
                               ('owner-draft', {**self.old_receipt, 'remote_id': ''}),
                               ('owner-draft', {**self.old_receipt, 'content_hash': '0' * 64})]:
            with self.subTest(state=state, receipt=receipt):
                row = self.row()
                data = {**original['data']}
                if receipt is None:
                    data.pop('draft_receipt')
                else:
                    data['draft_receipt'] = receipt
                self.ledger.tasks.update(row['id'], row['revision'], state, data)
                with self.assertRaises(GateError):
                    self.cli_stage()
                with self.assertRaises(GateError):
                    self.ledger.tasks.draft_revision(self.envelope(), SECRET)
        self.assert_no_writes()

    def test_coordinator_ignores_bad_and_tampered_signatures(self):
        bad = self.envelope()
        bad['signature'] = '0' * 64
        tampered = self.envelope()
        tampered['packet']['body'] += 'Tampered after signing.'
        tampered['new_packet_hash'] = digest(tampered['packet'])
        for value in (bad, tampered):
            self.remote.assets['draft-revision-' + digest(value) + '.json'] = canonical(value).encode()
        errors = self.ledger.ingest(SECRET)
        self.assertEqual(len(errors), 2)
        self.assertTrue(all(e['error'] == 'Draft revision signature failed' for e in errors))
        self.tick()
        self.assertNotIn('draft_revision_requests', self.row()['data'])
        self.assertEqual(self.ledger.tasks.db.execute('SELECT COUNT(*) FROM control_events').fetchone()[0], 1)
        self.assert_no_writes()

    def test_unedited_draft_is_verified_then_old_is_reread_and_owned_before_delete(self):
        self.stage()
        events = []
        read = self.box.message
        own = self.box.own_draft
        delete = self.box.delete_draft

        def message(identity):
            events.append(('read', identity))
            return read(identity)

        def owned(draft, packet, key):
            events.append(('owned', draft['id']))
            return own(draft, packet, key)

        def remove(identity):
            # The last message read and ownership check must both target the old ID.
            self.assertEqual(events[-2:], [('read', self.old_id), ('owned', self.old_id)])
            replacement = next(d for d in self.graph.drafts.values()
                               if d['id'] not in {self.old_id, self.unrelated['id']})
            self.assertIn(('read', replacement['id']), events)
            self.assertTrue(self.box.matches(replacement, self.new_packet))
            self.assertEqual(replacement['conversationId'], self.packet['thread_id'])
            return delete(identity)

        with patch.object(self.box, 'message', side_effect=message), \
                patch.object(self.box, 'own_draft', side_effect=owned), \
                patch.object(self.box, 'delete_draft', side_effect=remove):
            self.tick()
        self.assert_applied()
        with patch('managed.daily_local.status', return_value={'status': 'offline-fixture'}):
            shown = automation.work(self.ledger.tasks, self.root)['owner_drafts']
        self.assertEqual(len(shown), 1)
        self.assertEqual(shown[0]['packet'], self.new_packet)
        self.assertEqual(shown[0]['result'], self.result)
        self.assertEqual(shown[0]['draft_receipt'], self.row()['data']['draft_receipt'])
        self.assertEqual(shown[0]['draft_revisions'], self.row()['data']['draft_revisions'])

    def test_owner_edited_body_is_preserved_without_any_write(self):
        self.stage()
        self.graph.drafts[self.old_id]['body']['content'] = 'Avery edited this in the web mailbox.'
        self.tick()
        self.assertEqual(self.row()['data']['last_error'], OWNER_EDITED)
        self.assert_no_writes()

    def test_sent_or_missing_old_draft_is_preserved_without_any_write(self):
        self.stage()
        original = copy.deepcopy(self.graph.drafts[self.old_id])
        for state in ('isDraft-false', 'sent-folder', 'absent-404'):
            with self.subTest(state=state):
                self.graph.drafts.pop(self.old_id, None)
                self.graph.sent.clear()
                if state == 'isDraft-false':
                    self.graph.drafts[self.old_id] = {**original, 'isDraft': False}
                elif state == 'sent-folder':
                    self.graph.sent[self.old_id] = {**original, 'isDraft': False}
                read = self.box.message

                def message(identity):
                    if state == 'absent-404' and identity == self.old_id:
                        error = HTTPError('fixture:old-draft', 404, 'Not Found', {}, io.BytesIO())
                        error.close()
                        raise error
                    return read(identity)

                with patch.object(self.box, 'message', side_effect=message):
                    self.tick()
                self.assertEqual(self.row()['data']['last_error'], OWNER_EDITED)
                self.assert_no_writes()

    def crash_and_recover(self, phase):
        self.stage()
        if phase == 'intent':
            crash = patch.object(self.box, 'create_draft', side_effect=ProcessDeath())
        elif phase == 'create':
            create = self.graph.mime

            def lost_create(*args, **kwargs):
                create(*args, **kwargs)
                raise ProcessDeath()

            crash = patch.object(self.graph, 'mime', side_effect=lost_create)
        elif phase == 'verified':
            crash = patch.object(self.box, 'delete_draft', side_effect=ProcessDeath())
        else:
            delete = self.box.delete_draft

            def lost_delete(identity):
                delete(identity)
                raise TimeoutError('DELETE succeeded; response lost')

            crash = patch.object(self.box, 'delete_draft', side_effect=lost_delete)
        with crash:
            if phase == 'deleted':
                self.tick()  # A normal exception is caught by the coordinator.
            else:
                with self.assertRaises(ProcessDeath):
                    self.tick()
        expected_phase = 'intent' if phase in {'intent', 'create'} else 'delete-intent'
        self.assertEqual(self.restored_row()['data']['draft_revision_intent']['phase'], expected_phase)
        self.restart()
        self.tick()
        self.tick()
        self.assert_applied()
        # The outer tick's final flush can also be lost. Replay its last durable
        # delete intent after a second process restart without duplicating history.
        self.restart()
        self.tick()
        self.assert_applied()

    def test_crash_after_persisted_intent_before_create(self):
        self.crash_and_recover('intent')

    def test_crash_after_create_before_verification(self):
        self.crash_and_recover('create')

    def test_crash_after_verification_before_delete(self):
        self.crash_and_recover('verified')

    def test_delete_response_lost_recovers_original_replacement(self):
        self.crash_and_recover('deleted')

    def test_failed_intent_checkpoint_prevents_create(self):
        self.stage()
        self.remote.fail_pointer = True
        self.tick()
        self.assertEqual(self.row()['data']['last_error'], 'Lost pointer write')
        self.assertNotIn('draft_revision_intent', self.restored_row()['data'])
        self.assert_no_writes()

    def test_failed_deletion_checkpoint_preserves_old_draft(self):
        self.stage()
        save = self.ledger.save

        def fail_at_delete(*args, **kwargs):
            if self.row()['data'].get('draft_revision_intent', {}).get('phase') == 'delete-intent':
                self.remote.fail_pointer = True
            return save(*args, **kwargs)

        with patch.object(self.ledger, 'save', side_effect=fail_at_delete):
            self.tick()
        self.assertIn(self.old_id, self.graph.drafts)
        self.assertFalse(any(c[1] == 'DELETE' for c in self.graph.calls))
        self.assertEqual(self.row()['data']['last_error'], 'Lost pointer write')
        self.remote.fail_pointer = False
        self.restart()
        self.tick()
        self.assert_applied()

    def test_pause_or_disabled_kind_prevents_all_revision_writes(self):
        self.stage()
        for config in ({**self.config, 'paused': True}, {**self.config, 'kinds': {'reply': False}}):
            with self.subTest(config=config):
                self.tick(config)
                self.assert_no_writes()
                self.assertNotIn('draft_revision_intent', self.row()['data'])

    def test_pause_also_defers_delete_of_an_already_verified_replacement(self):
        self.stage()
        with patch.object(self.box, 'delete_draft', side_effect=ProcessDeath()), \
                self.assertRaises(ProcessDeath):
            self.tick()
        self.restart()
        before = copy.deepcopy(self.graph.drafts)
        writes = [c for c in self.graph.calls if c[1] != 'GET']
        self.tick({**self.config, 'paused': True})
        self.assertEqual(self.graph.drafts, before)
        self.assertEqual([c for c in self.graph.calls if c[1] != 'GET'], writes)
        self.assertEqual(len(self.graph.mimes), 2)
        self.tick()
        self.assert_applied()

    def test_every_new_packet_recipient_is_checked_against_tables_one_and_three(self):
        self.stage()
        with patch.object(delivery, 'preflight', wraps=delivery.preflight) as registry, \
                patch.object(self.box, 'preflight', wraps=self.box.preflight) as fresh:
            self.tick()
        self.assert_applied()
        self.assertEqual({call.args[0] for call in registry.call_args_list},
                         set(self.new_packet['to'] + self.new_packet['cc']))
        self.assertTrue(all(call.args[0] == self.new_packet for call in fresh.call_args_list))
        self.assertTrue(all(call.kwargs['allow_draft_id'] == self.old_id for call in fresh.call_args_list))

    def test_table_three_refusal_for_to_or_cc_blocks_revision(self):
        self.stage()
        for address in self.packet['to'] + self.packet['cc']:
            with self.subTest(address=address):
                self.tables('refusals', {'refusals': [
                    {'id': 'stop-contact', 'active': True, 'emails': [address], 'name': 'Example'}]})
                self.tick()
                self.assertIn('table-3', self.row()['data'].get('last_error', ''))
                self.assert_no_writes()

    def test_unregistered_to_or_cc_without_thread_evidence_blocks_revision(self):
        self.stage()
        contacts = json.loads((self.root / 'tables/contacts.json').read_text())['contacts']
        for address in self.packet['to'] + self.packet['cc']:
            with self.subTest(address=address):
                self.tables('contacts', {'contacts': [c for c in contacts if c['email'] != address]})
                with patch.object(self.box, 'preflight', return_value={}):
                    self.tick()
                self.assertIn('not registered', self.row()['data']['last_error'])
                self.assert_no_writes()

    def test_missing_to_and_cc_register_from_thread_before_revision(self):
        self.tables('positive', {'entries': []})
        self.tables('submissions', {'schema_version': 1, 'updated_on': '2026-11-02',
                    'scope': 'independent-team', 'verification': {'mailbox_check': 'saved-evidence-only'},
                    'papers': []})
        self.tables('contacts', {'contacts': []})
        self.stage()
        self.tick()
        self.assert_applied()
        contacts=json.loads((self.root / 'tables/contacts.json').read_text())['contacts']
        self.assertEqual({c['email'] for c in contacts}, set(self.packet['to'] + self.packet['cc']))
        self.assertTrue(all(c['identity_evidence'] == 'mail-thread' for c in contacts))

    def test_recovery_rechecks_refusals_and_registration_before_deleting_old_draft(self):
        self.stage()
        with patch.object(self.box, 'delete_draft', side_effect=ProcessDeath()), \
                self.assertRaises(ProcessDeath):
            self.tick()
        self.restart()
        contacts = json.loads((self.root / 'tables/contacts.json').read_text())
        self.tables('contacts', {'contacts': []})
        self.tick()
        self.assertIn('not registered', self.row()['data'].get('last_error', ''))
        self.assertIn(self.old_id, self.graph.drafts)
        self.assertFalse(any(c[1] == 'DELETE' for c in self.graph.calls))
        self.tables('contacts', contacts)
        self.tables('refusals', {'refusals': [
            {'id': 'new-stop', 'active': True, 'emails': self.packet['cc'], 'name': 'Example'}]})
        self.tick()
        self.assertIn('table-3', self.row()['data'].get('last_error', ''))
        self.assertIn(self.old_id, self.graph.drafts)
        self.assertFalse(any(c[1] == 'DELETE' for c in self.graph.calls))
        self.tables('refusals', {'refusals': []})
        self.tick()
        self.assert_applied()

    def test_replacement_attempt_identity_must_match_before_old_draft_is_deleted(self):
        self.stage()
        with patch.object(self.box, 'delete_draft', side_effect=ProcessDeath()), \
                self.assertRaises(ProcessDeath):
            self.tick()
        new_id = self.row()['data']['draft_revision_intent']['new_remote_id']
        self.graph.drafts[new_id]['internetMessageHeaders'] = [
            {'name': 'X-Outreach-Attempt', 'value': 'someone-elses-attempt'}]
        self.tick()
        self.assertIn(self.old_id, self.graph.drafts)
        self.assertFalse(any(c[1] == 'DELETE' for c in self.graph.calls))
        self.assertNotIn('packet_override', self.row()['data'])

    def test_same_staged_revision_replayed_before_and_after_application_is_idempotent(self):
        envelope = self.stage()
        before = self.row()
        self.stage(envelope)
        self.assertEqual(self.row(), before)
        self.tick()
        self.assert_applied()
        before = self.row()
        self.stage(envelope)
        self.tick()
        self.tick()
        self.assertEqual(self.row(), before)
        self.assert_applied()

    def test_old_owner_edit_between_creation_and_delete_is_never_deleted(self):
        self.stage()
        create = self.box.create_draft

        def owner_edits(*args, **kwargs):
            draft = create(*args, **kwargs)
            self.graph.drafts[self.old_id]['body']['content'] = 'An intervening owner edit.'
            return draft

        with patch.object(self.box, 'create_draft', side_effect=owner_edits):
            self.tick()
        self.tick()
        self.assertEqual(self.row()['data']['last_error'], OWNER_EDITED)
        self.assertIn(self.old_id, self.graph.drafts)
        self.assertEqual(len(self.graph.mimes), 2)
        self.assertFalse(any(c[1] == 'DELETE' for c in self.graph.calls))
        self.assertNotIn('packet_override', self.row()['data'])

    def test_old_draft_with_wrong_or_duplicate_attempt_header_is_preserved(self):
        self.stage()
        for headers in ([{'name': 'X-Outreach-Attempt', 'value': 'someone-elses-attempt'}],
                        [{'name': 'X-Outreach-Attempt', 'value': 'original-attempt'}] * 2):
            with self.subTest(headers=headers):
                self.graph.drafts[self.old_id]['internetMessageHeaders'] = headers
                self.tick()
                self.assertEqual(self.row()['data']['last_error'], OWNER_EDITED)
                self.assert_no_writes()

    def test_wrong_conversation_or_mismatched_replacement_never_deletes_old_draft(self):
        self.stage()
        for field in ('conversationId', 'body', 'attachment'):
            with self.subTest(field=field):
                # Each attempt retries the same existing replacement, never creates another.
                if len(self.graph.mimes) == 1:
                    with patch.object(self.box, 'delete_draft', side_effect=ProcessDeath()), \
                            self.assertRaises(ProcessDeath):
                        self.tick()
                new_id = self.row()['data']['draft_revision_intent']['new_remote_id']
                draft = self.graph.drafts[new_id]
                original = copy.deepcopy(draft)
                attachments = copy.deepcopy(self.graph.attachments[new_id])
                if field == 'conversationId':
                    draft[field] = 'wrong-thread'
                elif field == 'body':
                    draft[field]['content'] = 'Different replacement body.'
                else:
                    self.graph.attachments[new_id][0]['contentBytes'] = 'Y2hhbmdlZA=='
                self.tick()
                self.assertIn(self.old_id, self.graph.drafts)
                self.assertFalse(any(c[1] == 'DELETE' for c in self.graph.calls))
                self.assertEqual(len(self.graph.mimes), 2)
                self.graph.drafts[new_id] = original
                self.graph.attachments[new_id] = attachments


def mailbox_participant(address):
    return {'emailAddress': {'address': address}}


if __name__ == '__main__':
    unittest.main()


def setUpModule():
    use_example_config()
    use_cli_config()
