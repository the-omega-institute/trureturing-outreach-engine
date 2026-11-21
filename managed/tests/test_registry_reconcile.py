"""One-pass registry reconciliation contract."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from fixture_config import use_example_config
from managed import registry
from managed import author_contacts
from managed.queue import digest


class RegistryReconcileTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        (self.root / 'tables').mkdir()
        registry.write(self.root, 'contacts', {'contacts': []})
        registry.write(self.root, 'refusals', {'refusals': []})
        registry.write(self.root, 'positive', {'entries': []})
        registry.write(self.root, 'submissions', {
            'schema_version': 1, 'updated_on': '2026-11-02',
            'scope': 'independent-team',
            'verification': {'mailbox_check': 'saved-evidence-only'}, 'papers': []})

    def tearDown(self):
        self.temp.cleanup()

    def update(self, patch=None, text='Excellent work.', address='author@example.invalid', task='reply-1'):
        incoming = {'email': address, 'at': '2026-11-02T10:00:00+08:00',
                    'message_id': '<in@example.invalid>', 'evidence_ref': 'graph:inbox:1'}
        return {'task_id': task, 'incoming': incoming, 'incoming_text': text,
                'subject': 'A checked result', 'registry_patch': patch,
                'recorded': False}

    def patch_value(self, *, refusal=None, praise=None, contact=True):
        return {'incoming_summary': 'They praised the checked result.',
                'praise': praise if praise is not None else
                [{'quote': 'Excellent work.', 'context': 'The checked result',
                  'story': 'They praised the checked result.'}],
                'refusal': refusal,
                'new_contact': ({'name': 'Professor Example', 'title': 'Professor',
                                'title_evidence_url': 'https://example.invalid/profile',
                                'affiliation': 'Example University',
                                'evidence_url': 'https://example.invalid/source'} if contact else None)}

    def test_dry_run_changes_nothing(self):
        patch_value = self.patch_value()
        work = {'registry_updates': [self.update(patch_value)], 'author_contacts_needed': []}
        before = {name: (self.root / 'tables' / (name + '.json')).read_bytes()
                  for name in ('contacts', 'positive', 'refusals')}
        result = registry.reconcile(work, root=self.root)
        self.assertTrue(result['dry_run'])
        self.assertEqual(result['applied'], {'events': 0, 'praise': 0, 'contacts': 0,
                                             'refusals': 0, 'author_contacts': 0, 'aliases': 0,
                                             'undeliverable': 0})
        self.assertEqual(before, {name: (self.root / 'tables' / (name + '.json')).read_bytes()
                                  for name in before})

    def test_apply_records_event_praise_and_new_contact(self):
        result = registry.reconcile({'registry_updates': [self.update(self.patch_value())],
                                     'author_contacts_needed': []}, apply=True, root=self.root)
        self.assertEqual(result['applied']['events'], 1)
        self.assertEqual(result['applied']['praise'], 1)
        self.assertEqual(result['applied']['contacts'], 1)
        self.assertEqual(len(registry.read(self.root, 'positive')['entries']), 1)

    def alias_work(self):
        registry.register('primary@example.invalid', 'Professor Example', root=self.root)
        contact_id = registry.read(self.root, 'contacts')['contacts'][0]['id']
        value = self.patch_value(contact=False, praise=[])
        value['alias_of'] = {'contact_id': contact_id, 'evidence_ref': 'graph:inbox:1',
                             'reason': 'The same display name continues the same institutional thread.'}
        return {'registry_updates': [self.update(value)]}

    def test_alias_apply_records_existing_person_event_and_evidence_once(self):
        work = self.alias_work()
        # Hydrated mailbox identities must not preempt the alias proposal.
        work['registry_updates'][0]['mail_thread_contacts'] = [{'email': 'author@example.invalid'}]
        report = registry.reconcile(work, apply=True, root=self.root)
        self.assertEqual(report['exceptions'], [])
        self.assertEqual(report['applied']['aliases'], 1)
        self.assertEqual(report['applied']['contacts'], 0)
        self.assertEqual(report['applied']['events'], 1)
        row, = registry.read(self.root, 'contacts')['contacts']
        alias = work['registry_updates'][0]['registry_patch']['alias_of']
        self.assertEqual(row['id'], alias['contact_id'])
        self.assertEqual(row['relationship']['aliases'], ['author@example.invalid'])
        self.assertEqual(row['relationship']['alias_evidence'], [{
            'email': 'author@example.invalid', 'evidence_ref': alias['evidence_ref'],
            'note': alias['reason'], 'task_id': 'reply-1'}])
        self.assertEqual(row['hub']['events'][0]['kind'], 'reply')
        again = registry.reconcile(work, apply=True, root=self.root)
        self.assertEqual(again['applied']['aliases'], 0)
        self.assertEqual(registry.read(self.root, 'contacts')['contacts'], [row])

    def test_alias_dry_run_reports_pending_without_writes(self):
        work = self.alias_work()
        before = {p: p.read_bytes() for p in self.root.rglob('*') if p.is_file()}
        report = registry.reconcile(work, root=self.root)
        self.assertEqual(report['exceptions'], [])
        self.assertEqual(report['pending_aliases'], [{'task_id': 'reply-1',
            'email': 'author@example.invalid', **work['registry_updates'][0]['registry_patch']['alias_of']}])
        self.assertEqual(before, {p: p.read_bytes() for p in self.root.rglob('*') if p.is_file()})

    def test_alias_unknown_contact_is_exception(self):
        work = self.alias_work()
        work['registry_updates'][0]['registry_patch']['alias_of']['contact_id'] = 'missing'
        for apply in (False, True):
            report = registry.reconcile(work, apply=apply, root=self.root)
            self.assertIn('contact_id does not exist', report['exceptions'][0]['reason'])
            self.assertEqual(report['applied']['events'], 0)

    def test_registered_sender_ignores_alias_target(self):
        work = self.alias_work()
        update = work['registry_updates'][0]
        update['incoming']['email'] = 'primary@example.invalid'
        update['registry_patch']['alias_of']['contact_id'] = 'missing'
        report = registry.reconcile(work, apply=True, root=self.root)
        self.assertEqual(report['exceptions'], [])
        self.assertEqual(report['applied']['aliases'], 0)
        self.assertEqual(report['applied']['events'], 1)

    def test_alias_requires_matching_incoming_evidence(self):
        work = self.alias_work()
        work['registry_updates'][0]['registry_patch']['alias_of']['evidence_ref'] = 'graph:other'
        report = registry.reconcile(work, apply=True, root=self.root)
        self.assertIn('identify the incoming message', report['exceptions'][0]['reason'])
        self.assertEqual(report['applied']['aliases'], 0)

    def test_alias_validators(self):
        from managed import control
        work = self.alias_work()
        value = work['registry_updates'][0]['registry_patch']
        row = {'id': 'reply-1', 'kind': 'reply', 'request': {'inputs': {
            'mailbox_observation': {'email': 'author@example.invalid'}}}}
        result = {'schema_version': 1, 'task_id': 'reply-1', 'outcome': 'hold',
                  'report': 'Identity checked.', 'packet': None, 'artifacts': [], 'registry_patch': value}
        with patch.object(control, 'ROOT', self.root):
            control.validate_result(result, row)
            value['new_contact'] = self.patch_value()['new_contact']
            for validate in (registry._registry_patch, lambda p: control.validate_result(result, row)):
                with self.assertRaisesRegex(ValueError, 'mutually exclusive'):
                    validate(value)
            value['new_contact'] = None
            row['request']['inputs']['mailbox_observation']['email'] = 'primary@example.invalid'
            with self.assertRaisesRegex(ValueError, 'only for an unregistered sender'):
                control.validate_result(result, row)
            row['request']['inputs']['mailbox_observation']['email'] = 'author@example.invalid'
            value['alias_of']['contact_id'] = 'missing'
            with self.assertRaisesRegex(ValueError, 'contact_id does not exist'):
                control.validate_result(result, row)

    def test_unverifiable_quote_is_an_exception(self):
        value = self.patch_value(praise=[{'quote': 'Not in message', 'context': 'x', 'story': 'y'}])
        result = registry.reconcile({'registry_updates': [self.update(value)],
                                     'author_contacts_needed': []}, apply=True, root=self.root)
        self.assertEqual(result['applied']['events'], 1)
        self.assertEqual(result['applied']['praise'], 0)
        self.assertIn('exact substring', result['exceptions'][0]['reason'])

    def test_refusal_requires_extra_flag_and_remains_exception(self):
        refusal = {'quote': 'Please stop contacting me.', 'scope': 'stop collaboration'}
        value = self.patch_value(refusal=refusal, praise=[])
        value['new_contact'] = None
        registry.register('author@example.invalid', 'Professor Example', root=self.root)
        work = {'registry_updates': [self.update(value, text='Please stop contacting me.')],
                'author_contacts_needed': []}
        result = registry.reconcile(work, apply=True, root=self.root)
        self.assertEqual(registry.read(self.root, 'refusals')['refusals'], [])
        self.assertTrue(result['exceptions'])
        result = registry.reconcile(work, apply=True, apply_refusals=True, root=self.root)
        self.assertEqual(len(registry.read(self.root, 'refusals')['refusals']), 1)
        self.assertEqual(result['applied']['refusals'], 1)
        self.assertTrue(result['exceptions'])
        result = registry.reconcile(work, apply=True, apply_refusals=True, root=self.root)
        self.assertEqual(result['applied']['refusals'], 0)

    def test_recorded_items_are_skipped_and_rerun_is_idempotent(self):
        value = self.patch_value()
        work = {'registry_updates': [self.update(value)], 'author_contacts_needed': []}
        first = registry.reconcile(work, apply=True, root=self.root)
        second = registry.reconcile(work, apply=True, root=self.root)
        self.assertEqual(first['applied']['events'], 1)
        self.assertEqual(second['applied']['events'], 0)
        self.assertEqual(len(registry.read(self.root, 'contacts')['contacts'][0]['hub']['events']), 1)
        self.assertEqual(len(registry.read(self.root, 'positive')['entries']), 1)

    def sent_work(self, kind='firstcontact', to=None, cc=None):
        packet = {'kind': kind, 'to': to or ['author@example.invalid'], 'cc': cc or [],
                  'subject': 'A checked result', 'body': 'Synthetic test letter.'}
        receipt = {'remote_id': 'graph-sent-1', 'evidence_ref': 'graph:sent:<sent@example.invalid>',
                   'at': '2026-11-02T02:00:00Z', 'content_hash': digest(packet),
                   'idempotency_key': 'send-1'}
        # These are the distinct shapes exported by managed.automation.work().
        task = {'id': 'sent-1', 'kind': kind, 'state': 'verified',
                'request': {'kind': kind, 'inputs': {'recipient_emails': ['unsent@example.invalid']}},
                'result': {'packet': packet}, 'sent_receipt': receipt}
        incoming = self.update(self.patch_value()) if kind == 'reply' else {
            'task_id': 'sent-1', 'incoming': dict.fromkeys(
                ['email', 'at', 'message_id', 'evidence_ref', 'subject', 'text']),
            'registry_patch': None, 'recorded': False}
        incoming.update(task_id='sent-1', sent_receipt=receipt)
        return {'affairs_tasks': [task], 'registry_updates': [incoming]}

    def test_sent_audit_lists_to_and_cc_without_writing(self):
        work = self.sent_work(cc=['copied@example.invalid'])
        before = {p: p.read_bytes() for p in self.root.rglob('*') if p.is_file()}
        result = registry.reconcile(work, root=self.root)
        self.assertEqual(result['exceptions'], [])
        pending = result['pending_outgoing']
        self.assertEqual({e['email'] for e in pending}, {'author@example.invalid', 'copied@example.invalid'})
        for event in pending:
            self.assertEqual(event['kind'], 'sent')
            self.assertEqual(event['at'], '2026-11-02T02:00:00Z')
            self.assertEqual(event['subject'], 'A checked result')
            self.assertEqual(event['remote_id'], 'graph-sent-1')
        self.assertEqual(result['applied']['events'], 0)
        self.assertEqual(before, {p: p.read_bytes() for p in self.root.rglob('*') if p.is_file()})

    def test_sent_receipt_records_every_recipient_once_and_no_judgements(self):
        work = self.sent_work(cc=['copied@example.invalid', 'AUTHOR@example.invalid'])
        first = registry.reconcile(work, apply=True, root=self.root)
        self.assertEqual(first['exceptions'], [])
        self.assertEqual(first['applied']['events'], 2)
        self.assertEqual(first['applied']['contacts'], 2)
        rows = registry.read(self.root, 'contacts')['contacts']
        self.assertEqual({r['email'] for r in rows}, {'author@example.invalid', 'copied@example.invalid'})
        for row in rows:
            event, = row['hub']['events']
            self.assertEqual(event['kind'], 'sent')
            self.assertEqual(event['at'], '2026-11-02T02:00:00Z')
            self.assertEqual(event['summary'], 'A checked result')
            self.assertIn('graph-sent-1', event['evidence'])
            self.assertEqual(row['name'], row['email'])
        # Identity is Graph message ID + recipient, independent of task ID,
        # mutable snapshot hints, timestamp and subject.
        task = work['affairs_tasks'][0]
        task['id'] = work['registry_updates'][0]['task_id'] = 'another-task'
        task['result']['packet']['subject'] = 'A revised snapshot subject'
        task['sent_receipt']['content_hash'] = digest(task['result']['packet'])
        task['sent_receipt']['at'] = '2026-11-02T03:00:00Z'
        second = registry.reconcile(work, apply=True, root=self.root)
        self.assertEqual(second['applied']['events'], 0)
        self.assertEqual(second['applied']['contacts'], 0)
        self.assertEqual(registry.read(self.root, 'contacts')['contacts'], rows)
        self.assertEqual(registry.reconcile(work, root=self.root)['pending_outgoing'], [])
        self.assertEqual(registry.read(self.root, 'positive')['entries'], [])
        self.assertEqual(registry.read(self.root, 'refusals')['refusals'], [])
        registry.validate(self.root)

    def test_reply_sent_is_independent_of_recorded_incoming_and_covers_cc(self):
        registry.register('author@example.invalid', 'Scholar Example', root=self.root)
        work = self.sent_work('reply', cc=['copied@example.invalid'])
        work['registry_updates'][0]['recorded'] = True
        result = registry.reconcile(work, apply=True, apply_refusals=True, root=self.root)
        self.assertEqual(result['exceptions'], [])
        self.assertEqual(result['applied']['events'], 2)
        self.assertEqual(registry.read(self.root, 'positive')['entries'], [])
        self.assertEqual(registry.read(self.root, 'refusals')['refusals'], [])

    def test_existing_sent_evidence_only_skips_its_own_recipient(self):
        registry.register('author@example.invalid', 'Scholar Example', root=self.root)
        registry.record_event('author@example.invalid', 'sent', '2026-11-02T02:00:00Z',
                              'Previously recorded Sent', 'graph:sent:<sent@example.invalid>', self.root)
        work = self.sent_work(cc=['copied@example.invalid'])
        result = registry.reconcile(work, apply=True, root=self.root)
        self.assertEqual(result['applied']['events'], 1)
        self.assertEqual(registry.reconcile(work, apply=True, root=self.root)['applied']['events'], 0)

    def test_sent_alias_recipients_link_to_one_person_but_keep_each_event(self):
        registry.register('author@example.invalid', 'Scholar Example', root=self.root)
        rows = registry.read(self.root, 'contacts')
        rows['contacts'][0]['relationship'] = {'aliases': ['alias@example.invalid']}
        registry.write(self.root, 'contacts', rows)
        work = self.sent_work(cc=['alias@example.invalid'])
        result = registry.reconcile(work, apply=True, root=self.root)
        self.assertEqual(result['applied']['events'], 2)
        self.assertEqual(result['applied']['contacts'], 0)
        row, = registry.read(self.root, 'contacts')['contacts']
        self.assertEqual(len(row['hub']['events']), 2)
        self.assertEqual(registry.reconcile(work, apply=True, root=self.root)['applied']['events'], 0)

    def test_unverified_or_incomplete_sends_never_invent_recipients(self):
        for invalid in ('no-receipt', 'draft', 'no-message-id', 'no-time', 'hash-mismatch',
                        'no-packet', 'no-task', 'no-recipients'):
            with self.subTest(invalid=invalid):
                work = self.sent_work()
                task = work['affairs_tasks'][0]
                receipt = task['sent_receipt']
                if invalid == 'no-receipt':
                    task['sent_receipt'] = work['registry_updates'][0]['sent_receipt'] = None
                elif invalid == 'draft': receipt['evidence_ref'] = 'graph:draft:1'
                elif invalid == 'no-message-id': receipt['remote_id'] = ''
                elif invalid == 'no-time': receipt['at'] = ''
                elif invalid == 'hash-mismatch': receipt['content_hash'] = 'unverified'
                elif invalid == 'no-packet': task['result']['packet'] = None
                elif invalid == 'no-task': work['affairs_tasks'] = []
                elif invalid == 'no-recipients':
                    task['result']['packet']['to'] = []
                    receipt['content_hash'] = digest(task['result']['packet'])
                result = registry.reconcile(work, apply=True, root=self.root)
                self.assertEqual(result['applied']['events'], 0)
                self.assertEqual(registry.read(self.root, 'contacts')['contacts'], [])
                if invalid != 'no-receipt': self.assertTrue(result['exceptions'])

    def test_author_contacts_proposal_is_saved_and_applied(self):
        proposal = {'request': {'id': 'note-1'}, 'result': {'task_id': 'note-1'},
                    'review': {'verdict': 'PASS'}}
        work = {'registry_updates': [], 'author_contacts_needed': [
            {'task_id': 'note-1', 'reviewed_proposal': proposal}]}
        applied = {'registered_or_linked': [{'email': 'author@example.invalid'}],
                   'skipped': [], 'unreachable': []}
        with patch.object(author_contacts, 'reviewed', return_value=[]), \
             patch.object(author_contacts, 'apply', return_value=applied) as apply, \
             patch('managed.control.review_key', return_value='secret'):
            result = registry.reconcile(work, apply=True, root=self.root)
        self.assertEqual(result['applied']['author_contacts'], 1)
        apply.assert_called_once_with(proposal, self.root, 'secret', render_views=False)
        path = Path(registry._reconcile_contact_path(self.root)) / 'note-1.json'
        self.assertEqual(json.loads(path.read_text()), proposal)
        path.unlink()


def setUpModule():
    use_example_config()


if __name__ == '__main__':
    unittest.main()
