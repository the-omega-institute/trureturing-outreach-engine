"""Contact facts and refusal boundaries must survive retries and CMA handoffs."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from fixture_config import use_example_config
from managed import automation, control, registry as r
from managed.queue import GateError, canonical
from test_tasks import CMA, Releases, request


class RegistryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        (self.root / 'tables').mkdir()
        r.write(self.root, 'contacts', {'contacts': []})
        r.write(self.root, 'refusals', {'refusals': []})
        r.write(self.root, 'positive', {'entries': []})
        r.write(self.root, 'submissions', {'schema_version': 1, 'updated_on': '2026-10-31',
                'scope': 'independent-team', 'verification': {'mailbox_check': 'saved-evidence-only'},
                'papers': []})

    def tearDown(self):
        self.temp.cleanup()

    def test_firstcontact_excludes_mainland_china_institutions_only(self):
        import json
        (self.root/'tables'/'decisions.json').write_text(json.dumps({'firstcontact_excluded_regions': {
            'domain_suffixes': ['.foreign.example.invalid'],
            'affiliation_pattern': r'(?i)(\bChina\b|中国)',
            'affiliation_exempt_pattern': r'(?i)(Hong\s?Kong|Macau|Macao|Taiwan)'}}))
        r.write(self.root, 'contacts', {'contacts': [
            {'id': 'contact-a', 'name': 'A', 'email': 'a@campus.foreign.example.invalid'},
            {'id': 'contact-b', 'name': 'B', 'email': 'b@mailbox.example.invalid', 'affiliation': 'Example Institute, Mainland China'},
            {'id': 'contact-c', 'name': 'C', 'email': 'c@global.example.invalid', 'affiliation': 'Example Institute, Hong Kong'},
            {'id': 'contact-d', 'name': 'D', 'email': 'd@institute.foreign.example.invalid'},
            {'id': 'contact-e', 'name': 'E', 'email': 'e@math.campus.example.invalid'}]})
        blocked = {a: 'excluded-region-mainland-china' in r.preflight(a, root=self.root)['reasons']
                   for a in ['a@campus.foreign.example.invalid', 'b@mailbox.example.invalid', 'c@global.example.invalid', 'd@institute.foreign.example.invalid', 'e@math.campus.example.invalid']}
        self.assertEqual(blocked, {'a@campus.foreign.example.invalid': True, 'b@mailbox.example.invalid': True, 'c@global.example.invalid': False,
                                   'd@institute.foreign.example.invalid': True, 'e@math.campus.example.invalid': False})
        # Replies to existing correspondents are unaffected.
        self.assertTrue(r.preflight('a@campus.foreign.example.invalid', root=self.root, new_outreach=False)['allowed'])

    def test_registry_generates_and_checks_submission_supporting_index(self):
        r.render(self.root)
        self.assertIn('tables/SUBMISSIONS.md', (self.root / 'README.md').read_text())
        self.assertTrue((self.root / 'tables/SUBMISSIONS.md').exists())
        (self.root / 'tables/SUBMISSIONS.md').write_text('stale board')
        with self.assertRaisesRegex(GateError, 'Regenerate submissions'): r.validate(self.root)

    def test_generated_homepage_renders_private_template(self):
        (self.root / 'registry-homepage.md').write_text('# Registry\nPublished contacts: $people\n')
        r.render(self.root)
        homepage = (self.root / 'README.md').read_text()
        self.assertEqual(homepage, '# Registry\nPublished contacts: 0\n')
        r.render(self.root, check=True)

    def register(self, address='a@example.invalid', name='Scholar A', **kwargs):
        return r.register(address, name, root=self.root, **kwargs)

    def event(self, kind='reply', at='2026-10-29T12:00:00+00:00', **kwargs):
        return r.record_event('a@example.invalid', kind, at, 'Checked result; answer the question',
                              'local:message-id-and-source', root=self.root, **kwargs)

    def contact_request(self, recipients=None):
        req = request(kind='firstcontact')
        req['inputs']['recipient_emails'] = recipients or ['a@example.invalid']
        return req

    def test_only_verified_aliases_merge_and_placeholder_does_not_hide_name(self):
        self.register()
        self.register('b@example.invalid', 'Scholar A')
        self.assertEqual(len(r.people(self.root)), 2)  # Same name is insufficient.
        data = r.read(self.root, 'contacts')
        data['contacts'][0]['name'] = 'a@example.invalid'
        data['contacts'][0]['relationship'] = {'aliases': ['b@example.invalid']}
        r.write(self.root, 'contacts', data)
        person, = r.people(self.root)
        self.assertEqual(person['name'], 'Scholar A')
        self.assertEqual(person['emails'], ['a@example.invalid', 'b@example.invalid'])
        self.assertEqual(len(person['records']), 2)

    def test_register_updates_existing_verified_alias_without_extra_identity(self):
        self.register()
        data = r.read(self.root, 'contacts')
        data['contacts'][0]['relationship'] = {'aliases': ['alias@example.invalid']}
        r.write(self.root, 'contacts', data)
        self.register('alias@example.invalid', workspace='../mail/scholar-a')
        data = r.read(self.root, 'contacts')['contacts']
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['hub']['workspace_paths'], ['../mail/scholar-a'])

    def incoming_name_work(self, name, address='z@example.invalid'):
        message = {'id': 'incoming-1', 'internetMessageId': '<incoming@example.invalid>',
                   'receivedDateTime': '2026-11-03T10:00:00+08:00',
                   'subject': 'A checked result', 'isDraft': False,
                   'from': {'emailAddress': {'address': address, 'name': name}},
                   'body': {'contentType': 'text', 'content': 'Thank you for the result.'}}
        # Hydrated messages live in task data, while the immutable request can
        # predate hydration. This is the same extractor used by work().
        task = {'request': {'inputs': {}}, 'data': {'thread_inputs': {
            'mailbox_observation': {'email': address, 'at': message['receivedDateTime'],
                                    'message_id': message['internetMessageId'],
                                    'evidence_ref': 'graph:inbox:incoming-1'},
            'thread': [message]}}}
        incoming = automation._incoming_details(task)
        return {'registry_updates': [{'task_id': 'reply-1', 'incoming': incoming,
                                      'incoming_text': incoming['text'], 'recorded': False}]}

    def test_reconcile_aliases_unique_normalized_from_name_match(self):
        self.register()
        self.register('b@example.invalid', 'Scholar A')
        self.register('z@example.invalid', 'z@example.invalid')
        data = r.read(self.root, 'contacts')
        data['contacts'][1]['relationship'] = {'alias_of': 'a@example.invalid'}
        data['contacts'][2]['relationship'] = {'status': 'existing-thread'}
        r.write(self.root, 'contacts', data)
        work = self.incoming_name_work('  sChOlAr\t A  ')
        result = r.reconcile(work, apply=True, root=self.root)
        self.assertEqual(result['exceptions'], [])
        action, = result['contact_names']
        self.assertEqual(action['action'], 'alias')
        self.assertEqual(action['email'], 'z@example.invalid')
        self.assertEqual(action['relationship']['alias_of'], 'a@example.invalid')
        row = r.read(self.root, 'contacts')['contacts'][2]
        self.assertEqual(row['name'], 'Scholar A')
        self.assertEqual(row['relationship']['status'], 'existing-thread')
        self.assertIn('graph:inbox:incoming-1', row['relationship']['evidence'])
        self.assertIn('<incoming@example.invalid>', row['relationship']['evidence'])
        person, = r.people(self.root)
        self.assertEqual(person['emails'], ['a@example.invalid', 'b@example.invalid', 'z@example.invalid'])
        self.assertEqual(result['applied']['events'], 1)
        self.assertEqual(r.reconcile(work, apply=True, root=self.root)['contact_names'], [])
        self.assertEqual(len(r.activity(r.people(self.root)[0])['events']), 1)
        r.validate(self.root)

    def test_reconcile_renames_placeholder_when_no_exact_name_match(self):
        self.register()
        self.register('z@example.invalid', 'z@example.invalid')
        result = r.reconcile(self.incoming_name_work('  Scholar A Junior  '),
                             apply=True, root=self.root)
        self.assertEqual(result['exceptions'], [])
        action, = result['contact_names']
        self.assertEqual(action['action'], 'rename')
        row = r.read(self.root, 'contacts')['contacts'][1]
        self.assertEqual(row['name'], 'Scholar A Junior')
        self.assertNotIn('alias_of', row['relationship'])
        self.assertIn('graph:inbox:incoming-1', row['relationship']['name_evidence'])
        self.assertIn('<incoming@example.invalid>', row['relationship']['name_evidence'])
        self.assertEqual(len(r.people(self.root)), 2)

    def test_reconcile_recovers_empty_name_on_sender_row_already_linked_to_person(self):
        self.register()
        self.register('z@example.invalid', 'z@example.invalid')
        data = r.read(self.root, 'contacts')
        data['contacts'][1].update(name='', relationship={'alias_of': 'a@example.invalid'})
        r.write(self.root, 'contacts', data)
        result = r.reconcile(self.incoming_name_work('Scholar A'), apply=True, root=self.root)
        self.assertEqual(result['exceptions'], [])
        action, = result['contact_names']
        self.assertEqual(action['action'], 'rename')  # The sender's own person is excluded.
        rows = r.read(self.root, 'contacts')['contacts']
        self.assertEqual(rows[0]['name'], 'Scholar A')
        self.assertNotIn('name_evidence', rows[0].get('relationship', {}))
        self.assertEqual(rows[1]['name'], 'Scholar A')
        self.assertEqual(rows[1]['relationship']['alias_of'], 'a@example.invalid')
        self.assertIn('name_evidence', rows[1]['relationship'])

    def test_reconcile_leaves_ambiguous_name_unchanged(self):
        self.register()
        self.register('b@example.invalid', 'Scholar A')
        self.register('z@example.invalid', 'z@example.invalid')
        result = r.reconcile(self.incoming_name_work('scholar  a'), apply=True, root=self.root)
        self.assertEqual(result['exceptions'], [])
        self.assertEqual(result['contact_names'], [])
        self.assertEqual(result['applied']['events'], 1)
        row = r.read(self.root, 'contacts')['contacts'][2]
        self.assertEqual(row['name'], 'z@example.invalid')
        self.assertNotIn('relationship', row)
        self.assertEqual(len(r.people(self.root)), 3)

    def test_reconcile_preserves_real_name(self):
        self.register()
        self.register('z@example.invalid', 'Previously Verified Name')
        result = r.reconcile(self.incoming_name_work('Scholar A'), apply=True, root=self.root)
        self.assertEqual(result['contact_names'], [])
        row = r.read(self.root, 'contacts')['contacts'][1]
        self.assertEqual(row['name'], 'Previously Verified Name')
        self.assertNotIn('relationship', row)
        self.assertEqual(result['applied']['events'], 1)

    def test_reconcile_name_dry_run_proposes_alias_and_rename_without_writes(self):
        self.register()
        self.register('z@example.invalid', 'z@example.invalid')
        before = {p: p.read_bytes() for p in self.root.rglob('*') if p.is_file()}
        for name, action in [('Scholar A', 'alias'), ('New Scholar', 'rename')]:
            with self.subTest(action=action):
                result = r.reconcile(self.incoming_name_work(name), root=self.root)
                self.assertTrue(result['dry_run'])
                self.assertEqual(result['contact_names'][0]['action'], action)
                self.assertEqual(result['applied']['events'], 0)
                self.assertEqual(before, {p: p.read_bytes() for p in self.root.rglob('*') if p.is_file()})

    def test_reconcile_ignores_missing_or_address_from_names(self):
        self.register('z@example.invalid', 'z@example.invalid')
        for name in (None, '', ' \t ', 'z@example.invalid', 'Scholar <z@example.invalid>'):
            with self.subTest(name=name):
                result = r.reconcile(self.incoming_name_work(name), root=self.root)
                self.assertEqual(result['contact_names'], [])

    def test_incoming_name_never_uses_another_message_or_sender(self):
        for field, value in [('internetMessageId', '<another@example.invalid>'),
                             ('from', {'emailAddress': {'address': 'other@example.invalid', 'name': 'Other'}})]:
            with self.subTest(field=field):
                task = {'request': {'inputs': {
                    'mailbox_observation': {'email': 'z@example.invalid', 'message_id': '<incoming@example.invalid>'},
                    'thread': [{'internetMessageId': '<incoming@example.invalid>',
                                'from': {'emailAddress': {'address': 'z@example.invalid', 'name': 'Scholar A'}},
                                field: value}]}}}
                self.assertIsNone(automation._incoming_details(task)['name'])
        self.assertIsNone(automation._incoming_details({'request': {'inputs': {}}})['name'])

    def test_copied_thread_does_not_count_corecipient_as_incoming_author(self):
        self.register()
        data = r.read(self.root, 'contacts')
        data['contacts'][0]['hub']['threads'] = [{'id': 'thread', 'stage': 'reply',
            'correspondent_email': 'other@example.invalid',
            'last_in': {'date': '2026-10-30', 'ROWID': 123}, 'last_out': None}]
        data['contacts'][0]['relationship'] = {'last_sent': '2026-10-29'}  # No receipt.
        r.write(self.root, 'contacts', data)
        detail = r.activity(r.people(self.root)[0])
        self.assertFalse(detail['last_in'])
        self.assertFalse(detail['last_out'])

    def test_old_praise_does_not_overwrite_newer_thread_action(self):
        self.register()
        data = r.read(self.root, 'contacts')
        data['contacts'][0]['hub']['threads'] = [{'id': 'thread', 'stage': 'reply',
            'last_in': {'date': '2026-10-30T11:00:00+08:00', 'ROWID': 123},
            'last_out': None, 'next_action': 'Answer the newer specific question'}]
        r.write(self.root, 'contacts', data)
        self.event()
        person = r.people(self.root)[0]
        self.assertEqual(r.current_summary(person, r.activity(person)), 'Answer the newer specific question')
        self.assertEqual(r.activity(person)['last_in'], '2026-10-30T11:00:00+08:00')

    def test_recovered_inbound_receipt_preserves_next_action_and_prevents_cold_recontact(self):
        self.register()
        data = r.read(self.root, 'contacts')
        hub = data['contacts'][0]['hub']
        hub['threads'] = [{'id': 'thread', 'stage': 'reply',
                          'last_in': {'date': '2026-10-29', 'ROWID': 122},
                          'last_out': None, 'next_action': 'Preserve the current task'}]
        hub['historical_inbound'] = [{'date': '2026-10-30', 'rowid': 123,
            'message_id': 'mail@example.invalid', 'correspondent_email': 'a@example.invalid',
            'source': {'commit': 'a' * 40, 'path': 'old/relations.json'}}]
        r.write(self.root, 'contacts', data)
        person, = r.people(self.root)
        self.assertEqual(r.activity(person)['last_in'], '2026-10-30')
        self.assertEqual(r.current_summary(person, r.activity(person)), 'Preserve the current task')
        self.assertFalse(r.preflight('a@example.invalid', root=self.root)['allowed'])
        self.assertTrue(r.preflight('a@example.invalid', root=self.root, new_outreach=False)['allowed'])
        r.render(self.root)
        self.assertEqual(r.validate(self.root)['with_inbound'], 1)
        hub['historical_inbound'][0]['correspondent_email'] = 'other@example.invalid'
        r.write(self.root, 'contacts', data)
        self.assertEqual(r.activity(r.people(self.root)[0])['last_in'], '2026-10-29')

    def test_event_order_compares_timezones(self):
        self.register()
        self.event(at='2026-10-30T10:00:00+08:00')
        self.event(at='2026-10-30T03:00:00+00:00')
        self.assertEqual(r.activity(r.people(self.root)[0])['last_in'], '2026-10-30T03:00:00+00:00')

    def test_refusal_blocks_aliases_names_and_registration(self):
        self.register()
        data = r.read(self.root, 'contacts')
        data['contacts'][0]['relationship'] = {'aliases': ['alias@example.invalid']}
        r.write(self.root, 'contacts', data)
        r.add_refusal('a@example.invalid', '2026-10-30', 'No invitations', 'No capacity', 'source:reply', self.root)
        for address, name in [('alias@example.invalid', ''), ('new@example.invalid', 'Scholar A')]:
            check = r.preflight(address, name, self.root)
            self.assertFalse(check['allowed'])
            with self.assertRaises(GateError): self.register(address, name or 'Scholar A')
        r.record_event('a@example.invalid', 'reply', '2026-10-31', 'Received factual correction', 'source:new-reply', self.root)
        self.assertEqual(len(r.read(self.root, 'contacts')['contacts'][0]['hub']['events']), 2)

    def test_existing_sent_receipt_routes_to_existing_thread(self):
        self.register()
        self.event(kind='sent')
        self.assertIn('already-contacted-use-the-existing-thread', r.preflight('a@example.invalid', root=self.root)['reasons'])
        self.assertTrue(r.preflight('a@example.invalid', root=self.root, new_outreach=False)['allowed'])

    def test_praise_updates_both_tables_idempotently_without_clearing_refusal(self):
        self.register()
        r.add_refusal('a@example.invalid', '2026-10-29', 'No capacity', '', 'source:decline', self.root)
        args = ('a@example.invalid', '2026-10-30', 'Congratulations', 'Also declined collaboration', 'Appreciated the result', 'source:exact-reply', self.root)
        r.add_praise(*args)
        r.add_praise(*args)
        self.assertEqual(len(r.read(self.root, 'positive')['entries']), 1)
        self.assertEqual(len(r.read(self.root, 'contacts')['contacts'][0]['hub']['events']), 2)
        self.assertFalse(r.preflight('a@example.invalid', root=self.root)['allowed'])
        conflicting = list(args); conflicting[3] = 'Different context'
        with self.assertRaises(GateError): r.add_praise(*conflicting)
        r.render(self.root)
        self.assertEqual(r.validate(self.root)['positive_quotes'], 1)

    def test_invalid_event_or_refusal_writes_nothing(self):
        self.register()
        before = {name: r.read(self.root, name) for name in ['contacts', 'refusals']}
        for at in ['2026-02-30', '2026-10-30Tgarbage', '2026-10-30T12:00:00']:
            with self.assertRaises(ValueError): self.event(at=at)
            with self.assertRaises(ValueError):
                r.add_refusal('a@example.invalid', at, 'No', '', 'source:reply', self.root)
        with self.assertRaises(GateError):
            r.record_event('a@example.invalid', 'sent', '2026-10-30', 'Sent', '', self.root)
        self.assertEqual(before, {name: r.read(self.root, name) for name in before})

    def test_local_pointer_rejects_absolute_paths_and_parent_traversal(self):
        for path in ['/home/example/mail/person', '../mail/person/../../elsewhere']:
            with self.assertRaises(GateError): self.register(workspace=path)
        self.assertEqual(r.read(self.root, 'contacts')['contacts'], [])

    def test_stale_generated_views_fail_and_public_facts_are_distinct(self):
        self.register()
        entry = {'id': 'public-1', 'kind': 'public_result', 'contact_ids': [], 'name': 'Joint paper',
                 'date': '2026-10-01', 'fact': 'Actual authors, preprint', 'context': 'Existing catalog',
                 'story': 'Published preprint', 'evidence': 'https://example.invalid/paper', 'public_use': 'public-fact'}
        r.write(self.root, 'positive', {'entries': [entry]})
        r.render(self.root)
        self.assertEqual(r.validate(self.root)['positive_quotes'], 0)
        self.event()
        with self.assertRaisesRegex(GateError, 'Regenerate'): r.validate(self.root)
        r.render(self.root)
        self.assertEqual(r.validate(self.root)['with_inbound'], 1)
        data = r.read(self.root, 'positive'); data['entries'][0]['evidence'] = ''
        r.write(self.root, 'positive', data)
        with self.assertRaisesRegex(GateError, 'evidence'): r.validate(self.root)

    def positive_entry(self, kind, contact, **fields):
        entry = {'id': kind + '-1', 'kind': kind, 'contact_ids': [contact['id']],
                 'name': contact['name'], 'date': '2026-10-29', 'context': 'Exact result',
                 'story': 'Substantive exchange about the result', 'public_use': 'internal-source-material',
                 'evidence': {'rowid': 123, 'message_id': 'mail@example.invalid', 'date': '2026-10-29'}}
        entry.update(fields)
        return entry

    def test_restored_sections_count_people_across_aliases_and_keep_offers_distinct(self):
        a = self.register()
        b = self.register('alias@example.invalid', 'Scholar A')
        data = r.read(self.root, 'contacts')
        data['contacts'][0]['relationship'] = {'aliases': ['alias@example.invalid']}
        r.write(self.root, 'contacts', data)
        r.add_refusal('a@example.invalid', '2026-10-28', 'No new invitations', '', 'source:boundary', self.root)
        entries = [
            self.positive_entry('collaboration', a, status='active',
                                evidence=[{'rowid': 123, 'message_id': 'mail@example.invalid', 'date': '2026-10-29'}]),
            self.positive_entry('praise', a, quote='A useful result'),
            self.positive_entry('praise', b, id='praise-2', quote='A second useful result'),
            self.positive_entry('credit', b, credit_kind='coauthorship_offered', credit_status='done',
                                our_work='The exact result', their_work=None, quote='Would you like to coauthor?')]
        r.write(self.root, 'positive', {'entries': entries})
        metrics = r.render(self.root)
        self.assertEqual(metrics['positive_quotes'], 2)
        self.assertEqual(metrics['positive_people'], 1)
        self.assertEqual(metrics['collaboration_people'], 1)
        self.assertEqual(metrics['credit_people'], 1)
        self.assertEqual(r.validate(self.root), metrics)
        view = (self.root / 'tables/POSITIVE.md').read_text()
        self.assertIn('合著邀请（已提出）', view)
        self.assertNotIn('已合著', view)
        self.assertIn('A second useful result', view)
        self.assertIn('ROWID 123', view)
        self.assertFalse(r.preflight('alias@example.invalid', root=self.root)['allowed'])

    def test_collaboration_and_credit_reject_missing_evidence_or_inflated_status(self):
        contact = self.register()
        cases = [
            self.positive_entry('collaboration', contact, status='joint_paper', evidence=[{'date': '2026-10-29'}]),
            self.positive_entry('collaboration', contact, status='accepted-coauthorship'),
            self.positive_entry('credit', contact, quote='Please coauthor', credit_kind='accepted',
                                credit_status='done', our_work='Result', their_work='Paper'),
            self.positive_entry('credit', contact, quote='', credit_kind='will_cite',
                                credit_status='promised', our_work='Result', their_work='Paper')]
        for entry in cases:
            with self.subTest(entry=entry):
                r.write(self.root, 'positive', {'entries': [entry]})
                with self.assertRaises(GateError): r.validate(self.root)

    def test_recovered_snapshot_detects_loss_but_allows_current_story_updates(self):
        contact = self.register()
        record = {'date': '2026-10-29', 'rowid': 123, 'message_id': 'mail@example.invalid',
                  'topic': 'Result', 'quote': 'A useful result', 'summary': 'Appreciated result', 'strength': 'moderate'}
        profile = {'email': contact['email'], 'name': contact['name'], 'aliases': []}
        header = {'schema_version': 2, 'updated_at': '2026-10-30T16:23:32+00:00'}
        original = dict(header, contacts={contact['email']: dict(profile, collaboration=None,
                         endorsements=[record], credit=[])})
        digest = hashlib.sha256(json.dumps(original, ensure_ascii=False, sort_keys=True,
                                          separators=(',', ':')).encode()).hexdigest()
        entry = self.positive_entry('praise', contact, quote=record['quote'], source_record=record,
                    source_ref={'email': contact['email'], 'field': 'endorsements', 'index': 0})
        data = {'profiles': {contact['email']: profile}, 'entries': [entry],
                'historical_source': {'header': header, 'commit': 'a' * 40, 'path': 'old/relations.json',
                                      'contact_records': 1, 'content_sha256': digest}}
        r.write(self.root, 'positive', data)
        r.render(self.root)
        r.validate(self.root)
        data['entries'][0]['story'] = 'Updated attribution from a later checked message'
        r.write(self.root, 'positive', data)
        r.render(self.root)
        r.validate(self.root)
        for broken in [dict(data, entries=[]), dict(data, profiles={})]:
            r.write(self.root, 'positive', broken)
            with self.assertRaisesRegex(GateError, 'Historical positive'): r.validate(self.root)
        data['entries'][0]['source_record']['quote'] = 'A fabricated quote'
        r.write(self.root, 'positive', data)
        with self.assertRaisesRegex(GateError, 'Historical positive'): r.validate(self.root)

    def test_firstcontact_gate_requires_registration_and_complete_recipient_list(self):
        with self.assertRaisesRegex(GateError, 'Register'): r.firstcontact_gate(self.contact_request(), self.root)
        self.register()
        r.firstcontact_gate(self.contact_request(), self.root)
        for inputs in [{}, {'recipient_emails': ['a@example.invalid', 'a@example.invalid']}]:
            req = self.contact_request(); req['inputs'] = inputs
            with self.assertRaises(GateError): r.firstcontact_gate(req, self.root)

    def test_ingestion_rejects_blocked_person_but_keeps_independent_task(self):
        self.register()
        r.add_refusal('a@example.invalid', '2026-10-30', 'No', '', 'source:reply', self.root)
        remote = Releases()
        remote.assets['request-first.json'] = canonical(self.contact_request()).encode()
        remote.assets['request-research.json'] = canonical(request('other', 'b')).encode()
        with tempfile.TemporaryDirectory() as folder, patch.object(control, 'ROOT', self.root):
            ledger = control.Ledger(remote, folder)
            try:
                errors = ledger.ingest()
                self.assertEqual(len(errors), 1)
                self.assertIn('refused', errors[0]['error'])
                self.assertEqual([t['id'] for t in ledger.tasks.all()], ['other'])
            finally: ledger.tasks.close()

    def test_new_boundary_holds_queued_and_approved_tasks_before_delivery(self):
        self.register()
        remote = Releases(); cma = CMA()
        cfg = {'paused': False, 'kinds': {'firstcontact': True}, 'max_active': 1, 'agent_template': {}}
        for state in ['queued', 'approved']:
            with self.subTest(state=state), tempfile.TemporaryDirectory() as folder:
                ledger = control.Ledger(Releases(), folder)
                try:
                    req = self.contact_request(); row = ledger.tasks.add(req)
                    if state == 'approved': ledger.tasks.update(row['id'], row['revision'], state, {})
                    r.add_refusal('a@example.invalid', '2026-10-30', 'No', '', 'source:reply', self.root)
                    with patch.object(control, 'ROOT', self.root), patch.object(control, 'workflow', return_value=req['workflow_hash']):
                        control.advance(ledger, cma, cfg, folder)
                    self.assertEqual(ledger.tasks.get(req['id'])['state'], 'held')
                    self.assertFalse(cma.creates)
                finally: ledger.tasks.close()

    def test_returned_packet_cannot_add_an_unchecked_cc(self):
        self.register()
        packet = json.loads((Path(__file__).parent / 'fixtures/letter.json').read_text())
        packet['to'] = ['a@example.invalid']; packet['cc'] = []
        row = {'id': 'job-1', 'kind': 'firstcontact', 'request': self.contact_request()}
        result = {'schema_version': 1, 'task_id': 'job-1', 'outcome': 'completed',
                  'report': 'Checked research', 'artifacts': [], 'packet': packet}
        with patch.object(control, 'ROOT', self.root):
            control.validate_result(result, row)
            changed = copy.deepcopy(result); changed['packet']['cc'] = ['unlisted@example.invalid']
            with self.assertRaisesRegex(GateError, 'To/Cc differ'): control.validate_result(changed, row)


def setUpModule():
    use_example_config()


if __name__ == '__main__': unittest.main()
