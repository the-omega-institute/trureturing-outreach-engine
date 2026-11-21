"""Mailbox evidence permits only the documented reply recipient exceptions."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

from fixture_config import use_example_config
from managed import delivery, mailbox, registry
from managed.queue import digest
from managed.transports import Submission
import test_automation as scenarios


def participant(address, name=''):
    return {'emailAddress': {'address': address, 'name': name}}


def incoming(text='', cc=()):
    return {'id': 'graph-in', 'internetMessageId': '<inbound-1>',
            'conversationId': 'thread-1', 'isDraft': False,
            'receivedDateTime': '2026-11-02T01:00:00Z',
            'from': participant('author@example.invalid', 'Author Example'),
            'toRecipients': [participant('sender@example.invalid')],
            'ccRecipients': [participant(a, 'Colleague Example') for a in cc],
            'body': {'contentType': 'text', 'content': text}}


class ReplyRecipientTests(unittest.TestCase):
    def setUp(self):
        self.graph = Mock()
        self.graph.request.return_value = {'mail': 'sender@example.invalid'}
        self.box = mailbox.GraphMailbox(self.graph, {'account': 'sender@example.invalid', 'reconciled': True})
        self.packet = json.loads((Path(__file__).parent / 'fixtures/letter.json').read_text())
        self.packet.update(kind='reply', thread_id='thread-1', latest_inbound_message_id='<inbound-1>')

    def check(self, latest, to=None, cc=None, older=(), latest_sent=None):
        self.graph.pages.return_value = [*older, latest]
        packet = {**self.packet, 'to': to or self.packet['to'], 'cc': cc or []}
        inputs = {'latest_sent_message_id': latest_sent} if latest_sent else {}
        return self.box.preflight(packet, {'inputs': inputs})

    def test_body_address_can_be_added_with_evidence(self):
        policy = self.check(incoming('Please include Student Example <student@example.invalid>.'),
                            cc=['student@example.invalid'])
        exception = policy['recipient_exception']
        self.assertEqual(exception['added'], ['student@example.invalid'])
        self.assertEqual(exception['replaced'], [])
        self.assertEqual(exception['internet_message_id'], '<inbound-1>')

    def test_explicit_sender_redirect_preserves_every_other_recipient(self):
        for text in ('Please forward the archive to me at alternate@example.invalid.',
                     'Please reply to alternate@example.invalid instead.',
                     'Please use alternate@example.invalid instead.',
                     'Could you send future replies to alternate@example.invalid?',
                     'Please reply at alternate@example.invalid.'):
            with self.subTest(text=text):
                policy = self.check(incoming(text, ['colleague@example.invalid']),
                                    to=['alternate@example.invalid'], cc=['colleague@example.invalid'])
                self.assertEqual(policy['recipient_exception']['replaced'], ['author@example.invalid'])

    def test_arbitrary_extra_historical_address_and_substring_are_rejected(self):
        older = {**incoming(), 'id': 'older', 'internetMessageId': '<older>',
                 'receivedDateTime': '2026-11-01T01:00:00Z',
                 'ccRecipients': [participant('extra@example.invalid')]}
        for text in ('No additional recipients.', 'Use notextra@example.invalid.',
                     'Use extra@evil.other.example.invalid.'):
            with self.subTest(text=text), self.assertRaisesRegex(ValueError, 'Reply-all'):
                self.check(incoming(text), cc=['extra@example.invalid'], older=[older])

    def test_person_we_addressed_earlier_in_thread_may_be_readded(self):
        ours = {**incoming(), 'id': 'ours', 'internetMessageId': '<ours>',
                'receivedDateTime': '2026-10-31T01:00:00Z', 'sentDateTime': '2026-10-31T01:00:00Z',
                'from': participant('sender@example.invalid'),
                'toRecipients': [participant('author@example.invalid')],
                'ccRecipients': [participant('coauthor@example.invalid')]}
        policy = self.check(incoming('Thanks.'), cc=['coauthor@example.invalid'], older=[ours], latest_sent='<ours>')
        self.assertEqual(policy['recipient_exception']['added'], ['coauthor@example.invalid'])
        self.assertEqual(policy['recipient_exception']['basis'], 'earlier-sent-in-thread')
        with self.assertRaisesRegex(ValueError, 'Reply-all'):
            self.check(incoming('Thanks.'), cc=['coauthor@example.invalid', 'stranger@example.invalid'],
                       older=[ours], latest_sent='<ours>')

    def test_team_address_may_be_copied(self):
        self.box.settings['team_addresses'] = ['coauthor@team.example.invalid']
        policy = self.check(incoming('Thanks.'), cc=['coauthor@team.example.invalid'])
        self.assertEqual(policy['recipient_exception']['added'], ['coauthor@team.example.invalid'])
        with self.assertRaisesRegex(ValueError, 'Reply-all'):
            self.check(incoming('Thanks.'), cc=['coauthor@team.example.invalid', 'stranger@example.invalid'])

    def test_body_mention_does_not_authorize_dropping_sender_or_corecipient(self):
        for to, cc in [(['alternate@example.invalid'], ['colleague@example.invalid']),
                       (['author@example.invalid'], ['alternate@example.invalid'])]:
            with self.subTest(to=to), self.assertRaisesRegex(ValueError, 'Reply-all'):
                self.check(incoming('An address: alternate@example.invalid.', ['colleague@example.invalid']), to, cc)

    def test_negative_request_or_address_prefix_does_not_redirect_sender(self):
        for text in ('Do not reply to alternate@example.invalid.',
                     'Please do not reply to alternate@example.invalid.',
                     'Please reply to alternate@evil.other.example.invalid. Also alternate@example.invalid.'):
            with self.subTest(text=text), self.assertRaisesRegex(ValueError, 'Reply-all'):
                self.check(incoming(text), to=['alternate@example.invalid'])

    def test_body_of_an_older_message_cannot_authorize_addition(self):
        older = {**incoming('Please include extra@example.invalid.'), 'id': 'older',
                 'internetMessageId': '<older>', 'receivedDateTime': '2026-11-01T01:00:00Z'}
        with self.assertRaisesRegex(ValueError, 'Reply-all'):
            self.check(incoming(), cc=['extra@example.invalid'], older=[older])


class DeliveryThreadRecipientTests(unittest.TestCase):
    tearDown = scenarios.AutomationTests.tearDown
    tables = scenarios.AutomationTests.tables
    add = scenarios.AutomationTests.add
    result = scenarios.AutomationTests.result
    review = scenarios.AutomationTests.review
    route = scenarios.AutomationTests.route

    def setUp(self):
        scenarios.AutomationTests.setUp(self)
        self.tables('positive', {'entries': []})
        self.tables('submissions', {'schema_version': 1, 'updated_on': '2026-11-02',
                                  'scope': 'independent-team',
                                  'verification': {'mailbox_check': 'saved-evidence-only'}, 'papers': []})

    def run_delivery(self, route='owner', refused=False, tampered=False):
        row = self.add()
        result = self.result(row, route=route)
        result['packet']['cc'] = ['student@example.invalid']
        reviewed = self.review(row, result)
        self.route()
        latest = incoming('Please include Student Example <student@example.invalid>.')
        graph = Mock()
        graph.request.return_value = {'mail': result['packet']['sender']}
        graph.pages.return_value = [latest]
        box = mailbox.GraphMailbox(graph, {'account': result['packet']['sender'], 'reconciled': True})
        box.save_draft = Mock(return_value={'remote_id': 'draft', 'content_hash': digest(result['packet'])})
        box.submit = Mock(return_value=Submission('accepted'))
        # Exercise the ordinary queue transport with the real mailbox preflight.
        box.submit_tracked = None
        box.drafts_for = Mock(return_value=[])
        if refused:
            self.tables('refusals', {'refusals': [{'id': 'stop', 'active': True, 'name': 'Student Example',
                                                 'emails': ['student@example.invalid']}]})
        if tampered:
            current = self.ledger.tasks.get(row['id'])
            data = copy.deepcopy(current['data'])
            data['result']['packet']['body'] += '\nChanged after PASS.'
            self.ledger.tasks.update(row['id'], current['revision'], current['state'], data)
        delivery.advance(self.ledger, self.cfg, self.root, box)
        return self.ledger.tasks.get(row['id']), reviewed, box

    def check_delivery(self, route):
        row, reviewed, box = self.run_delivery(route)
        self.assertNotIn('last_error', row['data'])
        self.assertEqual(row['data']['result'], reviewed['data']['result'])
        self.assertEqual(row['data']['review'], reviewed['data']['review'])
        self.assertEqual(row['data']['recipient_exception']['added'], ['student@example.invalid'])
        self.assertEqual(box.save_draft.call_count if route == 'owner' else box.submit.call_count, 1)
        contact = next(r for r in registry.read(self.root, 'contacts')['contacts']
                       if r['email'] == 'student@example.invalid')
        self.assertEqual(contact['name'], 'Student Example')
        self.assertEqual(contact['identity_evidence'], 'mail-thread')
        self.assertFalse(contact['salutation']['verified'])

    def test_owner_draft_registers_and_records_without_changing_pass(self):
        self.check_delivery('owner')

    def test_automatic_send_registers_and_records_without_changing_pass(self):
        self.check_delivery('auto')

    def test_refusal_and_changed_packet_still_block_mail(self):
        row, _, box = self.run_delivery(refused=True)
        self.assertIn('Table 3', row['data']['last_error'])
        box.save_draft.assert_not_called()
        box.submit.assert_not_called()

    def test_refusal_still_blocks_automatic_send(self):
        row, _, box = self.run_delivery(route='auto', refused=True)
        self.assertIn('Table 3', row['data']['last_error'])
        box.submit.assert_not_called()

    def test_changed_packet_cannot_use_exact_pass(self):
        row, _, box = self.run_delivery(tampered=True)
        self.assertIn('review', row['data']['last_error'])
        box.save_draft.assert_not_called()
        box.submit.assert_not_called()


class ReconcileThreadRecipientTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / 'tables').mkdir()
        registry.write(self.root, 'contacts', {'contacts': []})
        registry.write(self.root, 'refusals', {'refusals': []})
        registry.write(self.root, 'positive', {'entries': []})
        registry.write(self.root, 'submissions', {
            'schema_version': 1, 'updated_on': '2026-11-02', 'scope': 'independent-team',
            'verification': {'mailbox_check': 'saved-evidence-only'}, 'papers': []})

    def work(self):
        latest = incoming('Please include Student Example <student@example.invalid>.', ['copied@example.invalid'])
        sent = {**latest, 'id': 'graph-sent', 'internetMessageId': '<sent>',
                'from': participant('sender@example.invalid'), 'body': {'contentType': 'text', 'content': ''},
                'toRecipients': [participant('former@example.invalid', 'Former Example')], 'ccRecipients': []}
        other = {**sent, 'conversationId': 'other-thread',
                 'toRecipients': [participant('unrelated@example.invalid')]}
        packet = {'kind': 'reply', 'sender': 'sender@example.invalid', 'to': ['author@example.invalid'],
                  'cc': ['copied@example.invalid', 'former@example.invalid', 'student@example.invalid',
                         'unrelated@example.invalid'], 'thread_id': 'thread-1',
                  'latest_inbound_message_id': '<inbound-1>'}
        task = {'id': 'reply-test', 'kind': 'reply',
                'request': {'inputs': {'thread': [sent, other, latest]}}, 'result': {'packet': packet}}
        update = {'task_id': 'reply-test', 'incoming': {
            'email': 'author@example.invalid', 'at': latest['receivedDateTime'],
            'message_id': '<inbound-1>', 'evidence_ref': 'graph:inbox:graph-in'},
            'subject': 'Synthetic incoming message', 'registry_patch': None}
        return {'affairs_tasks': [task], 'registry_updates': [update]}

    def test_reconcile_uses_from_to_cc_sent_and_body_with_no_public_url(self):
        work = self.work()
        before = (self.root / 'tables/contacts.json').read_bytes()
        registry.reconcile(work, root=self.root)
        self.assertEqual((self.root / 'tables/contacts.json').read_bytes(), before)
        report = registry.reconcile(work, apply=True, root=self.root)
        self.assertEqual(report['applied']['contacts'], 4)
        self.assertEqual(report['applied']['events'], 1)
        contacts = registry.read(self.root, 'contacts')['contacts']
        self.assertEqual({r['email'] for r in contacts}, {'author@example.invalid', 'copied@example.invalid',
                                                       'former@example.invalid', 'student@example.invalid'})
        for row in contacts:
            self.assertEqual(row['identity_evidence'], 'mail-thread')
            self.assertIn(row['mail_thread_evidence']['message_id'], {'graph-in', 'graph-sent'})
            self.assertIsNone(row['salutation']['title'])
        registry.reconcile(work, apply=True, root=self.root)
        self.assertEqual(registry.read(self.root, 'contacts')['contacts'], contacts)

    def test_public_identity_evidence_retains_its_verified_title(self):
        work = self.work()
        work['registry_updates'][0]['registry_patch'] = {
            'incoming_summary': 'They asked to include a student.', 'praise': [], 'refusal': None,
            'new_contact': {'name': 'Author Example', 'title': 'Professor', 'affiliation': 'Example University',
                            'title_evidence_url': 'https://example.invalid/profile',
                            'evidence_url': 'https://example.invalid/profile'}}
        registry.reconcile(work, apply=True, root=self.root)
        author = next(r for r in registry.read(self.root, 'contacts')['contacts']
                      if r['email'] == 'author@example.invalid')
        self.assertEqual(author['salutation']['title'], 'Professor')
        self.assertTrue(author['salutation']['verified'])

    def test_hydrated_evidence_survives_inventory_without_duplicate_thread_bodies(self):
        work = self.work()
        task = work['affairs_tasks'][0]
        hydrated = task['request']['inputs']
        row = {**task, 'request': {'inputs': {}}, 'data': {'thread_inputs': hydrated, 'result': task['result']}}
        contacts = registry.reply_mail_thread_contacts(row)
        work['registry_updates'][0]['mail_thread_contacts'] = contacts
        task['request']['inputs'] = {}
        report = registry.reconcile(work, apply=True, root=self.root)
        self.assertEqual(report['applied']['contacts'], 4)
        self.assertEqual(report['applied']['events'], 1)
        self.assertFalse(any('body' in c for c in contacts))


def setUpModule():
    use_example_config()


if __name__ == '__main__':
    unittest.main()


class NewerElsewhereTests(unittest.TestCase):
    """A reply is blocked when the recipient wrote again in another conversation."""

    def setUp(self):
        self.graph = Mock()
        self.graph.request.return_value = {'mail': 'sender@example.invalid'}
        self.box = mailbox.GraphMailbox(self.graph, {'account': 'sender@example.invalid', 'reconciled': True,
                                                     'cross_conversation_guard': True})
        self.packet = json.loads((Path(__file__).parent / 'fixtures/letter.json').read_text())
        self.packet.update(kind='reply', thread_id='thread-1', latest_inbound_message_id='<inbound-1>',
                           to=['author@example.invalid'], cc=[])

    def run_preflight(self, elsewhere):
        latest = incoming('Thanks.')
        def pages(url):
            if '/mailFolders/inbox/' in url:
                return [latest, *[r for r in elsewhere if r.get('_folder') == 'inbox']]
            if '/mailFolders/sentitems/' in url:
                return [r for r in elsewhere if r.get('_folder') == 'sentitems']
            if '/mailFolders/' in url:
                return []
            return [latest]
        self.graph.pages.side_effect = pages
        return self.box.preflight(self.packet, {'inputs': {}})

    def newer(self, at='2026-11-03T01:00:00Z'):
        return {**incoming('New questions.'), 'id': 'graph-new', 'internetMessageId': '<new>',
                'conversationId': 'thread-2', 'receivedDateTime': at, '_folder': 'inbox'}

    def test_unanswered_newer_message_in_other_conversation_blocks(self):
        with self.assertRaisesRegex(ValueError, 'another conversation'):
            self.run_preflight([self.newer()])

    def test_older_message_elsewhere_is_ignored(self):
        self.assertTrue(self.run_preflight([self.newer('2026-11-01T01:00:00Z')])['complete'])

    def test_answered_newer_message_elsewhere_is_ignored(self):
        sent = {'id': 'graph-sent', 'internetMessageId': '<sent>', 'conversationId': 'thread-2',
                'isDraft': False, 'sentDateTime': '2026-11-03T02:00:00Z',
                'receivedDateTime': '2026-11-03T02:00:00Z',
                'from': participant('sender@example.invalid'),
                'toRecipients': [participant('author@example.invalid')], 'ccRecipients': [],
                '_folder': 'sentitems'}
        self.assertTrue(self.run_preflight([self.newer(), sent])['complete'])

    def test_sent_elsewhere_after_latest_blocks_stale_reply(self):
        # Trevor Jack 2026-11-07: Avery answered from another conversation.
        sent = {'id': 'graph-sent', 'internetMessageId': '<sent>', 'conversationId': 'thread-3',
                'isDraft': False, 'sentDateTime': '2026-11-02T08:00:00Z',
                'receivedDateTime': '2026-11-02T08:00:00Z',
                'from': participant('sender@example.invalid'),
                'toRecipients': [participant('author@example.invalid')], 'ccRecipients': [],
                '_folder': 'sentitems'}
        with self.assertRaisesRegex(ValueError, 'Actual Sent to author@example.invalid'):
            self.run_preflight([sent])
        self.assertTrue(self.run_preflight([{**sent, 'sentDateTime': '2026-11-01T08:00:00Z'}])['complete'])
