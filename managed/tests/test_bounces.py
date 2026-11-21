"""Delivery-failure intake and address-specific registry history, entirely offline."""
import copy
from datetime import datetime, timezone
import io
import json
from pathlib import Path
import shlex
import tempfile
import unittest
from unittest.mock import Mock, patch

from fixture_config import use_example_config
from managed import automation, mailbox, pipeline, registry
from managed.control import Ledger
from managed.queue import GateError, canonical
from test_tasks import Releases


ACCOUNT = 'owner@example.invalid'
FAILED = 'scholar@example.invalid'


def message(sender='postmaster@college.example.invalid', subject='Undeliverable: A proof', body='', key='ndr'):
    return {'id': key, 'internetMessageId': '<' + key + '@example.invalid>',
            'conversationId': 'thread-' + key, 'receivedDateTime': '2026-11-11T09:00:00Z',
            'from': {'emailAddress': {'address': sender}},
            'toRecipients': [{'emailAddress': {'address': ACCOUNT}}], 'ccRecipients': [],
            'subject': subject, 'isDraft': False, 'body': {'contentType': 'text', 'content': body}}


class BounceParsingTests(unittest.TestCase):
    def test_ndr_sender_subject_and_header_signals(self):
        for sender in ('postmaster@college.example.invalid', 'postmaster@research.example.invalid',
                       'mailer-daemon@example.invalid', 'MAILER-DAEMON@example.invalid', 'Mail Delivery Subsystem'):
            with self.subTest(sender=sender):
                self.assertTrue(mailbox._delivery_failure(message(sender, 'Mail system report')))
                self.assertTrue(mailbox._machine_sender(sender))
        for subject in ('Undeliverable: A proof', 'Delivery Status Notification (Failure)',
                        'Mail delivery failed: returning message to sender', 'Returned mail: User unknown',
                        'Delivery has failed to these recipients or groups'):
            with self.subTest(subject=subject):
                self.assertTrue(mailbox._delivery_failure(message('other@example.invalid', subject)))
        for header, value in [('Final-Recipient', 'rfc822; ' + FAILED), ('X-Failed-Recipients', FAILED)]:
            m = message('no-reply@example.invalid', 'Report')
            m['internetMessageHeaders'] = [{'name': header, 'value': value}]
            self.assertTrue(mailbox._delivery_failure(m))
        m = message('other@example.invalid', 'Report')
        m['from']['emailAddress']['name'] = 'Mail Delivery Subsystem'
        self.assertTrue(mailbox._delivery_failure(m))

    def test_ordinary_machine_notices_are_not_ndrs(self):
        for sender, subject in [('no-reply@receipts.example.invalid', 'Your Grab receipt'),
                                ('no-reply@auth.example.invalid', 'Duo Security authentication'),
                                ('noreply@hosting.example.invalid', 'GitHub Actions workflow failed'),
                                ('no-reply@service.example.invalid', 'NSCC service notification'),
                                ('no-reply@publisher.example.invalid', 'Your manuscript transfer notice'),
                                ('no-reply@example.invalid', 'Privacy notice')]:
            with self.subTest(sender=sender, subject=subject):
                m = message(sender, subject, 'Contact support@example.invalid for assistance.')
                self.assertTrue(mailbox._machine_sender(sender))
                self.assertFalse(mailbox._delivery_failure(m))

    def test_recipient_diagnostics_precede_original_message_addresses(self):
        for body in ("Your message to scholar@example.invalid couldn't be delivered.",
                     "Your message to <scholar@example.invalid> couldn’t be delivered.",
                     "scholar@example.invalid wasn't found at example.invalid.",
                     "<scholar@example.invalid>... User unknown",
                     "550 5.1.1 <scholar@example.invalid>... User unknown",
                     'Final-Recipient: rfc822; scholar@example.invalid'):
            with self.subTest(body=body):
                m = message(body='Help: support@example.invalid\n' + body + '\nFrom: someone@example.invalid')
                m['internetMessageHeaders'] = [{'name': 'Delivered-To', 'value': 'forwarder@example.invalid'}]
                self.assertEqual(mailbox._bounce_recipient(m, ACCOUNT), FAILED)
        for name, value in [('Final-Recipient', 'rfc822; SCHOLAR@example.invalid'),
                            ('X-Failed-Recipients', '<scholar@example.invalid>')]:
            m = message(body='From: someone@example.invalid')
            m['internetMessageHeaders'] = [{'name': name, 'value': value}]
            self.assertEqual(mailbox._bounce_recipient(m, ACCOUNT), FAILED)
        # An unrelated address in a quoted original is insufficient for marking it.
        self.assertIsNone(mailbox._bounce_recipient(message(body='From: someone@example.invalid'), ACCOUNT))
        self.assertIsNone(mailbox._bounce_recipient(message(body='Final-Recipient: rfc822; ' + ACCOUNT), ACCOUNT))


class BounceRegistryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / 'tables').mkdir()
        for name, value in [('contacts', {'contacts': []}), ('refusals', {'refusals': []}),
                            ('positive', {'entries': []}), ('submissions', {
                                'schema_version': 1, 'updated_on': '2026-11-11',
                                'scope': 'independent-team',
                                'verification': {'mailbox_check': 'saved-evidence-only'}, 'papers': []})]:
            registry.write(self.root, name, value)
        self.contact = registry.register(FAILED, 'Scholar', root=self.root)
        (self.root / 'ledger').mkdir()
        self.ledger = Ledger(Releases(), self.root / 'ledger')
        self.addCleanup(self.ledger.tasks.close)
        self.config = {'paused': False, 'kinds': {'reply': True},
                       'automation': {'max_new_tasks_per_tick': 0}}
        self.box = Mock()
        self.box.settings = {'reconciled': True, 'live_since': '2026-11-10T00:00:00Z'}
        self.box.verify_owner.return_value = ACCOUNT
        self.box.messages.return_value = []

    def intake(self, incoming=()):
        self.box.messages.return_value = list(incoming)
        mailbox.intake(self.ledger, self.config, self.root, self.box, revision='a' * 40,
                       now=datetime(2026, 11, 11, 10, tzinfo=timezone.utc))
        return json.loads(self.ledger.tasks.db.execute(
            "SELECT payload FROM pipeline_state WHERE key='mailbox'").fetchone()[0])

    def inventory(self):
        with patch('managed.daily_local.status', return_value={}):
            return automation.work(self.ledger.tasks, self.root)

    def test_intake_classifies_without_reply_budget_and_keeps_bodies_out_of_state(self):
        failure = message(body="Your message to scholar@example.invalid couldn't be delivered.")
        # Another NDR in the same conversation must retain its own evidence.
        second = message('postmaster@research.example.invalid', body='<avcarver@research.example.invalid>... User unknown', key='second')
        second['conversationId'] = failure['conversationId']
        notice = message('no-reply@receipts.example.invalid', 'Your Grab receipt', 'private receipt', key='notice')
        state = self.intake([failure, second, notice])
        self.assertEqual(state['bounce_count'], 2)
        self.assertEqual(state['bounces'][0]['recipient'], FAILED)
        self.assertEqual(state['machine_notice_count'], 1)
        self.assertEqual(state['machine_notices'][0]['intake_reason'], 'machine_notice')
        self.assertEqual(state['unmapped'], [])
        self.assertEqual(self.ledger.tasks.all(), [])
        self.assertNotIn('body', canonical(state))
        self.box.research_thread.assert_not_called()

    def test_old_twenty_two_bounces_refilter_to_two_ndrs_and_twenty_notices(self):
        old = [message(body='<scholar@example.invalid>... User unknown'),
               message('postmaster@research.example.invalid', key='bgu')]
        old += [message('no-reply@receipts.example.invalid', 'Receipt ' + str(i), key='notice-' + str(i)) for i in range(20)]
        for m in old:
            m.pop('body')
            m.update(intake_reason='bounce', reason='bounce', original_recipient=FAILED)
        prior = {'through': '2026-11-10T00:00:00Z', 'bounces': old, 'bounce_count': 22}
        self.ledger.tasks.db.execute('INSERT INTO pipeline_state VALUES(?,?)', ('mailbox', canonical(prior)))
        state = self.intake()
        self.assertEqual(state['bounce_count'], 2)
        self.assertEqual(state['machine_notice_count'], 20)
        self.assertEqual(len(state['machine_notices']), 20)
        self.assertTrue(all('original_recipient' not in m for m in state['machine_notices']))
        # Replaying the inclusive cursor or a retained row cannot inflate counts.
        self.assertEqual(self.intake(old), state)
        work = self.inventory()
        self.assertEqual(work['mailbox_bounce_count'], 2)
        self.assertEqual(work['mailbox_machine_notices'], 20)

    def test_notice_rows_are_compact_bounded_and_count_survives_boundary_replay(self):
        notices = [message('noreply@hosting.example.invalid', 'Notification', key='notice-' + str(i)) for i in range(30)]
        for m in notices:
            m['receivedDateTime'] = '2026-11-11T10:00:00Z'
        state = self.intake(notices)
        self.assertEqual(state['machine_notice_count'], 30)
        self.assertEqual(len(state['machine_notices']), 20)
        self.assertEqual(self.intake(notices)['machine_notice_count'], 30)
        self.assertEqual(self.inventory()['mailbox_machine_notices'], 30)

    def test_body_dsn_classification_survives_compact_persistence(self):
        self.intake([message('no-reply@example.invalid', 'Mail system report', 'Final-Recipient: rfc822; ' + FAILED)])
        state = self.intake()
        self.assertEqual(state['bounce_count'], 1)
        self.assertEqual(state['bounces'][0]['recipient'], FAILED)

    def test_old_ndr_without_recipient_recovers_from_its_exact_message(self):
        failure = message(body="Your message to scholar@example.invalid couldn't be delivered.")
        old = {k: v for k, v in failure.items() if k != 'body'}
        old.update(reason='bounce', recipient=None, original_recipient=None)
        self.ledger.tasks.db.execute('INSERT INTO pipeline_state VALUES(?,?)', ('mailbox', canonical({
            'through': '2026-11-10T00:00:00Z', 'bounces': [old], 'bounce_count': 1})))
        self.box.research_thread.return_value = [failure]
        with patch('managed.mailbox._adapter_method', return_value=self.box.research_thread):
            state = self.intake()
        self.assertEqual(state['bounces'][0]['recipient'], FAILED)
        self.assertEqual(len(self.inventory()['registry_updates']), 1)

    def test_ndr_hydration_matches_exact_message_instead_of_latest_machine_message(self):
        failure = message(body="Your message to scholar@example.invalid couldn't be delivered.")
        later = message('no-reply@example.invalid', 'Ordinary notice', key='later')
        self.box.research_thread.return_value = [failure, later]
        projection = {k: v for k, v in failure.items() if k != 'body'}
        # A real adapter exposes research_thread; a generic Mock does not.
        with patch('managed.mailbox._adapter_method', return_value=self.box.research_thread):
            state = self.intake([projection])
        self.assertEqual(state['bounces'][0]['internetMessageId'], failure['internetMessageId'])
        self.assertEqual(state['bounces'][0]['recipient'], FAILED)

    def test_work_proposal_has_ndr_evidence_and_reconcile_is_idempotent(self):
        failure = message(body='<scholar@example.invalid>... User unknown')
        self.intake([failure, message(key='unknown', body='<unknown@example.invalid>... User unknown')])
        before = registry.read(self.root, 'contacts')
        work = self.inventory()
        proposal, = work['registry_updates']
        self.assertEqual(registry.read(self.root, 'contacts'), before)
        self.assertEqual(proposal['contact_id'], self.contact['id'])
        self.assertEqual(proposal['registry_patch']['undeliverable']['email'], FAILED)
        evidence = json.loads(proposal['registry_patch']['undeliverable']['evidence'])
        self.assertEqual(evidence, {k: failure[k] for k in ('internetMessageId', 'receivedDateTime', 'subject')})
        self.assertEqual(shlex.split(proposal['command'])[:6],
                         ['python3', '-m', 'managed.registry', 'mark-undeliverable', '--email', FAILED])
        registry.reconcile(work, root=self.root)
        self.assertEqual(registry.read(self.root, 'contacts'), before)
        applied = registry.reconcile(work, apply=True, root=self.root)
        self.assertEqual(applied['exceptions'], [])
        self.assertEqual(applied['applied']['undeliverable'], 1)
        self.assertEqual(registry.reconcile(work, apply=True, root=self.root)['applied']['undeliverable'], 0)
        self.assertTrue(self.inventory()['registry_updates'][0]['recorded'])
        self.assertFalse(registry.preflight(FAILED, root=self.root)['allowed'])
        # A forged patch is rejected before any address is changed.
        bad = copy.deepcopy(work)
        bad['registry_updates'][0]['registry_patch']['undeliverable']['evidence'] = 'other evidence'
        self.assertTrue(registry.reconcile(bad, apply=True, root=self.root)['exceptions'])

    def test_mark_command_is_idempotent_preserves_history_and_preflights_exact_address(self):
        registry.record_event(FAILED, 'sent', '2026-11-10', 'Actual sent', 'message:sent', self.root)
        data = registry.read(self.root, 'contacts')
        data['contacts'][0]['relationship'] = {'aliases': ['alias@example.invalid']}
        registry.write(self.root, 'contacts', data)
        args = ['mark-undeliverable', '--email', 'alias@example.invalid', '--evidence', 'NDR: <failure@example.invalid>']
        with patch('managed.registry.ROOT', self.root), patch('sys.stdout', new_callable=io.StringIO):
            registry.main(args)
            first_bytes = (self.root / 'tables/contacts.json').read_bytes()
            registry.main(args)
        self.assertEqual((self.root / 'tables/contacts.json').read_bytes(), first_bytes)
        row, = registry.read(self.root, 'contacts')['contacts']
        self.assertEqual(len(row['hub']['events']), 1)
        self.assertEqual(len(row['hub']['undeliverable_addresses']), 1)
        self.assertEqual(row['email'], FAILED)
        self.assertEqual(registry.preflight('alias@example.invalid', root=self.root)['reasons'],
                         ['undeliverable-address', 'already-contacted-use-the-existing-thread'])
        self.assertNotIn('undeliverable-address', registry.preflight(FAILED, root=self.root)['reasons'])
        self.assertIn('alias@example.invalid（投递失败）', (self.root / 'tables/CONTACTS.md').read_text())
        registry.mark_undeliverable('alias@example.invalid', 'NDR: <second@example.invalid>', self.root)
        self.assertEqual(len(registry.read(self.root, 'contacts')['contacts'][0]['hub']['undeliverable_addresses']), 2)
        with self.assertRaises(GateError):
            registry.mark_undeliverable('unregistered@example.invalid', 'NDR evidence', self.root)

    def test_firstcontact_excludes_failed_address_but_selects_healthy_verified_alias(self):
        data = registry.read(self.root, 'contacts')
        data['contacts'][0].update(problem_ids=['result'], relationship={'aliases': ['alias@example.invalid']})
        registry.write(self.root, 'contacts', data)
        registry.mark_undeliverable(FAILED, 'NDR evidence', self.root)
        note = {'request': {'inputs': {'pipeline': {'result_id': 'result'}}}, 'data': {'result': {
            'author_contacts': [{'emails': [{'email': FAILED}, {'email': 'alias@example.invalid'}]}]}}}
        selected, excluded = pipeline.outreach_recipients(note, data['contacts'], self.root)
        self.assertEqual(selected, [{'contact_id': self.contact['id'], 'email': 'alias@example.invalid'}])
        self.assertEqual(excluded[0]['email'], FAILED)
        self.assertEqual(excluded[0]['reason'], 'undeliverable-address')
        with self.assertRaisesRegex(GateError, 'undeliverable-address'):
            registry.firstcontact_gate({'kind': 'firstcontact', 'inputs': {'recipient_emails': [FAILED]}}, self.root)
        registry.firstcontact_gate({'kind': 'firstcontact', 'inputs': {'recipient_emails': ['alias@example.invalid']}}, self.root)


def setUpModule():
    use_example_config()


if __name__ == '__main__':
    unittest.main()
