"""Inbox triage through serialized intake and the work inventory, with synthetic mail."""
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, call, patch
from urllib.parse import parse_qs, urlsplit

from managed import automation, mailbox, pipeline, thread_staging
from managed.control import Ledger
from managed.mailbox_triage import SENT_SUBJECT_WINDOW, normalized_subject, sent_subject_match
from managed.queue import canonical, digest
from managed.registry import people
from managed.tick import Tick
from test_tasks import Releases, use_fixture_workflow_revisions


OWNER = 'owner@example.invalid'
AUTHOR = 'author@example.invalid'
COLLEAGUE = 'colleague@example.invalid'
THIRD = 'third@example.invalid'
SENT_AT = '2026-11-17T09:00:00Z'
NOW = datetime(2026, 11, 18, 10, tzinfo=timezone.utc)


def addr(email, name=''):
    return {'emailAddress': {'address': email, 'name': name}}


def message(key, sender, subject, *, name='', cc=(), headers=(), minute=0):
    return {'id': key, 'internetMessageId': '<' + key + '@example.invalid>',
            'conversationId': key, 'receivedDateTime': f'2026-11-18T09:{minute:02d}:00Z',
            'from': addr(sender, name), 'toRecipients': [addr(OWNER)],
            'ccRecipients': [addr(a) for a in cc], 'subject': subject,
            'internetMessageHeaders': [{'name': k, 'value': v} for k, v in headers]}


def sent_message(key, subject, sent_at=SENT_AT):
    return {**message(key, OWNER, subject), 'toRecipients': [addr(AUTHOR)],
            'sentDateTime': sent_at, 'body': {'content': 'private fixture body'}}


class TriageTests(unittest.TestCase):
    def setUp(self):
        use_fixture_workflow_revisions(self)
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        (self.root / 'tables').mkdir()
        self.contacts = [{'id': 'author', 'email': AUTHOR, 'name': 'Writer Fixture'},
                         {'id': 'colleague', 'email': COLLEAGUE, 'name': 'Colleague Fixture'}]
        self.table('contacts', {'contacts': self.contacts})
        self.table('refusals', {'refusals': []})
        self.table('submissions', {'papers': []})
        self.releases = Releases()
        self.folder = self.root / 'ledger'
        self.folder.mkdir()
        self.ledger = Ledger(self.releases, self.folder)
        self.addCleanup(lambda: self.ledger.tasks.close())
        self.config = {'paused': False, 'kinds': {'reply': True},
                       'automation': {'max_new_tasks_per_tick': 6},
                       'mailbox': {'account': OWNER, 'editorial_domains': ['journal.example.invalid']}}
        self.box = mailbox.GraphMailbox(Mock(), {'reconciled': True, 'live_since': '2026-11-18T00:00:00Z',
                                                  'account': OWNER})
        self.box.verify_owner = Mock(return_value=OWNER)
        self.box.messages = Mock(return_value=[])
        self.box.sent_messages_to = Mock(return_value=[])
        self.sent_messages = []
        self.threads = {}
        self.box.research_thread = Mock(side_effect=self.threads.__getitem__)

    def table(self, name, value):
        (self.root / 'tables' / (name + '.json')).write_text(json.dumps(value))

    def add_sent_task(self, kind, subject, recipient, *, actual=True, cc=(),
                      owner='author', message_id='<out@example.invalid>'):
        request = pipeline.request(kind, (subject, owner), owner, {}, 'a' * 40)
        row = self.ledger.tasks.add(request)
        data = {'result': {'packet': {'subject': subject, 'to': [recipient], 'cc': list(cc)}}}
        if actual:
            data['sent_receipt'] = {'at': SENT_AT, 'evidence_ref': 'graph:sent:' + message_id}
        self.ledger.tasks.update(row['id'], row['revision'], 'queued', data)
        return row['id']

    def record_sent_history(self, *emails):
        for contact in self.contacts:
            if contact['email'] in emails:
                contact['hub'] = {'events': [{'kind': 'sent', 'at': SENT_AT,
                                             'summary': 'A prior letter', 'evidence': 'fixture:sent'}]}
        self.table('contacts', {'contacts': self.contacts})

    def tick(self, *messages, now=NOW):
        for thread_id in {row['conversationId'] for row in messages}:
            self.threads[thread_id] = [row for row in messages if row['conversationId'] == thread_id]
        self.box.graph.pages.return_value = self.sent_messages
        self.box.messages.side_effect = lambda folder, query: (
            list(messages) if folder == 'inbox' else mailbox.GraphMailbox.messages(self.box, folder, query))
        mailbox.intake(self.ledger, self.config, self.root, self.box, revision='b' * 40,
                       now=now)

    def sent_queries(self):
        return [parse_qs(urlsplit(args[0]).query) for args, _ in self.box.graph.pages.call_args_list]

    def sent_index(self):
        row = self.ledger.tasks.db.execute('SELECT payload FROM pipeline_state WHERE key=?',
                                          (mailbox.SENT_SUBJECTS_KEY,)).fetchone()
        return json.loads(row[0]) if row else None

    def inventory(self):
        return automation.work(self.ledger.tasks, self.root, self.config)

    def reply_tasks(self):
        return [r for r in self.ledger.tasks.all() if r['kind'] == 'reply'
                and 'mailbox_observation' in r['request']['inputs']]

    def retain(self, *messages):
        self.ledger.tasks.db.execute('INSERT INTO pipeline_state VALUES(?,?)',
                                     ('mailbox', canonical({'unmapped': list(messages)})))

    def assert_covered(self, incoming, task_id):
        work = self.inventory()
        self.assertEqual(work['mailbox_needs_classification'], [])
        self.assertEqual(work['mailbox_attention'], [])
        row, = work['mailbox_covered_rows']
        self.assertEqual(row['internetMessageId'], incoming['internetMessageId'])
        self.assertEqual(row['task_id'], task_id)
        self.assertIn(task_id, row['category_reason'])
        self.assertEqual(self.reply_tasks(), [])

    def test_intake_skips_operator_staged_inbound(self):
        incoming = message('selected', AUTHOR, 'A new question')
        self.record_sent_history(AUTHOR)
        value = thread_staging.build_inbound_request(' selected@example.invalid ', 'author',
                                                     'Check the boundary case', self.root, 'b' * 40)
        self.ledger.tasks.add(value)
        self.tick(incoming)
        self.assert_covered(incoming, value['id'])

    def test_intake_records_covered_unmapped_sender_as_handled(self):
        incoming = message('unknown', 'visitor@example.invalid', 'A new question')
        self.retain({**incoming, 'intake_reason': mailbox.UNREGISTERED,
                     'checked_at': self.ledger.tasks.clock()})
        value = thread_staging.build_inbound_request(incoming['internetMessageId'], None, '', self.root, 'b' * 40)
        self.ledger.tasks.add(value)
        self.tick(incoming)
        self.assert_covered(incoming, value['id'])

    def test_intake_skips_hydrated_reply_on_latest_inbound(self):
        incoming = message('hydrated', AUTHOR, 'A new question')
        self.record_sent_history(AUTHOR)
        value = pipeline.request('reply', 'hydrated', 'author', {
            'latest_inbound_message_id': '<earlier@example.invalid>',
            'thread_ref': {'internetMessageIds': ['<out@example.invalid>']},
        }, 'b' * 40)
        row = self.ledger.tasks.add(value)
        self.ledger.tasks.update(row['id'], row['revision'], 'running', {
            'thread_inputs': {'latest_inbound_message_id': 'hydrated@example.invalid'}})
        self.tick(incoming)
        self.assert_covered(incoming, row['id'])

    def test_intake_stages_reply_while_research_covers_the_message(self):
        incoming = message('researched', AUTHOR, 'A new question')
        self.record_sent_history(AUTHOR)
        self.ledger.tasks.add(pipeline.request('research', 'researched', 'author', {
            'thread_ref': {'internetMessageIds': [incoming['internetMessageId']]},
        }, 'b' * 40))
        self.tick(incoming)
        self.assertEqual([r['request']['inputs']['latest_inbound_message_id'] for r in self.reply_tasks()],
                         [incoming['internetMessageId']])

    def test_intake_stages_when_covering_task_is_terminal(self):
        self.record_sent_history(AUTHOR)
        messages = []
        for state in ('verified', 'no-reply', 'failed'):
            incoming = message(state, AUTHOR, 'A new question')
            value = thread_staging.build_inbound_request(incoming['internetMessageId'], 'author', '', self.root, 'b' * 40)
            row = self.ledger.tasks.add(value)
            self.ledger.tasks.update(row['id'], row['revision'], state, {})
            messages.append(incoming)
        self.tick(*messages)
        self.assertCountEqual([r['request']['inputs']['latest_inbound_message_id'] for r in self.reply_tasks()],
                              [m['internetMessageId'] for m in messages])
        self.assertEqual(self.inventory()['duplicate_inbound_tasks'], [])

    def test_legacy_cursor_replays_once_and_stages_newer_message_after_verified_reply(self):
        self.record_sent_history(AUTHOR)
        old = message('old', AUTHOR, 'A question')
        new = {**message('new', AUTHOR, 'A follow-up', minute=1), 'conversationId': old['conversationId']}
        prior = pipeline.request('reply', 'old', 'author', {
            'thread_id': old['conversationId'], 'latest_inbound_message_id': old['internetMessageId']}, 'b' * 40)
        row = self.ledger.tasks.add(prior)
        self.ledger.tasks.update(row['id'], row['revision'], 'verified', {})
        prior_cursor = '2026-11-18T09:30:00Z'
        self.ledger.tasks.db.execute('INSERT INTO pipeline_state VALUES(?,?)',
                                     ('mailbox', canonical({'through': prior_cursor})))
        self.tick(old, new)
        self.assertIn('receivedDateTime ge 2026-11-18T00:00:00Z', self.box.messages.call_args.args[1]['$filter'])
        self.assertEqual([r['request']['inputs']['latest_inbound_message_id'] for r in self.reply_tasks()],
                         [new['internetMessageId']])
        self.tick()
        self.assertIn('receivedDateTime ge 2026-11-18T10:00:00Z', self.box.messages.call_args.args[1]['$filter'])
        self.assertEqual(len(self.reply_tasks()), 1)

    def test_older_retained_message_cannot_hide_new_arrival_on_held_thread(self):
        self.record_sent_history(AUTHOR)
        old = message('unmapped', 'visitor@example.invalid', 'A question')
        new = {**message('new', AUTHOR, 'A follow-up', minute=1), 'conversationId': old['conversationId']}
        prior = pipeline.request('reply', 'held', 'author', {'thread_id': old['conversationId'],
                                 'latest_inbound_message_id': '<prior@example.invalid>'}, 'b' * 40)
        row = self.ledger.tasks.add(prior)
        self.ledger.tasks.update(row['id'], row['revision'], 'held', {'reason': 'New incoming message needs a reply'})
        self.retain({**old, 'intake_reason': mailbox.UNREGISTERED, 'checked_at': self.ledger.tasks.clock()})
        self.threads[old['conversationId']] = [old, new]
        self.box.messages.return_value = [new]
        mailbox.intake(self.ledger, self.config, self.root, self.box, revision='b' * 40, now=NOW)
        task, = self.reply_tasks()
        self.assertEqual(task['request']['inputs']['latest_inbound_message_id'], new['internetMessageId'])

    def test_known_new_thread_and_unknown_first_message_get_separate_dispositions(self):
        self.record_sent_history(AUTHOR)
        active = pipeline.request('reply', 'other case', 'author', {
            'thread_id': 'another-thread', 'latest_inbound_message_id': '<other@example.invalid>'}, 'b' * 40)
        row = self.ledger.tasks.add(active)
        self.ledger.tasks.update(row['id'], row['revision'], 'running', {})
        known = message('known', AUTHOR, 'A fresh topic')
        unknown = message('first', 'visitor@example.invalid', 'A first question')
        auto = message('automatic', AUTHOR, 'Away from office', headers=[('Auto-Submitted', 'auto-replied')])
        self.tick(known, unknown, auto)
        self.assertEqual([r['request']['inputs']['latest_inbound_message_id'] for r in self.reply_tasks()],
                         [known['internetMessageId']])
        reasons = {r['internetMessageId']:r['intake_reason']
                   for r in self.inventory()['mailbox_needs_classification']}
        self.assertEqual(reasons[unknown['internetMessageId']], mailbox.UNREGISTERED)
        self.assertEqual(reasons[auto['internetMessageId']], 'automated or bulk mail')

    def test_each_observed_message_gets_a_reason_when_latest_has_a_sent_answer(self):
        self.record_sent_history(AUTHOR)
        early = message('early', AUTHOR, 'A question')
        latest = {**message('latest', AUTHOR, 'A follow-up', minute=1), 'conversationId': early['conversationId']}
        sent = {**sent_message('sent', 'A reply', '2026-11-18T09:02:00Z'), 'conversationId': early['conversationId']}
        self.threads[early['conversationId']] = [early, latest, sent]
        self.box.messages.return_value = [early, latest]
        mailbox.intake(self.ledger, self.config, self.root, self.box, revision='b' * 40, now=NOW)
        reasons = {r['internetMessageId']:r for r in self.inventory()['mailbox_no_task']}
        self.assertEqual(reasons[early['internetMessageId']]['selected'], latest['internetMessageId'])
        self.assertEqual(reasons[latest['internetMessageId']]['sent'], sent['internetMessageId'])
        self.assertFalse(self.reply_tasks())

    def test_work_reports_active_reply_duplicates_by_message_id(self):
        message_id = '<same@example.invalid>'
        first = self.ledger.tasks.add(pipeline.request('reply', 'first', 'author', {
            'latest_inbound_message_id': message_id, 'thread_ref': {'internetMessageIds': [message_id]},
        }, 'b' * 40))
        self.ledger.tasks.add(pipeline.request('research', 'research', 'author', {
            'latest_inbound_message_id': message_id,
        }, 'b' * 40))
        second = self.ledger.tasks.add(pipeline.request('reply', 'second', 'author', {}, 'b' * 40))
        second = self.ledger.tasks.update(second['id'], second['revision'], 'running', {
            'thread_inputs': {'latest_inbound_message_id': 'same@example.invalid'}})
        self.assertEqual(self.inventory()['duplicate_inbound_tasks'], [{'message_id': message_id, 'tasks': [
            {'task_id': first['id'], 'state': 'queued'}, {'task_id': second['id'], 'state': 'running'},
        ]}])
        self.ledger.tasks.update(second['id'], second['revision'], 'no-reply', second['data'])
        self.assertEqual(self.inventory()['duplicate_inbound_tasks'], [])

    def test_home_domain_and_subdomains_only_stage_registered_senders(self):
        self.config['mailbox']['home_domains'] = ['University.example.invalid', 'u.campus.example.invalid']
        registered = 'info@lab.university.example.invalid'
        self.contacts.append({'id': 'faculty', 'email': registered, 'name': 'Faculty Fixture'})
        self.table('contacts', {'contacts': self.contacts})
        self.record_sent_history(registered)
        private = [
            message('root', 'circular@university.example.invalid', 'Circular'),
            message('lab', 'colleague@lab.university.example.invalid', 'Departmental reply', cc=[AUTHOR]),
            message('campus', 'staff@u.campus.example.invalid', 'Campus email'),
        ]
        outside = message('outside', 'faculty@otheruniversity.example.invalid', 'Other institution')
        self.retain(*({**row, 'category': 'person'} for row in private))
        self.assertEqual(self.inventory()['mailbox_category_counts'], {'institution': 3})
        self.assertEqual(self.inventory()['mailbox_attention'], [])

        self.tick(*private, outside, message('faculty-new', registered, 'New topic'),
                  message('faculty-linked', registered, 'RE: Prior note'))
        self.assertEqual([r['owner'] for r in self.reply_tasks()], ['faculty', 'faculty'])
        work = self.inventory()
        self.assertEqual(work['mailbox_category_counts'], {'institution': 3, 'person': 1})
        self.assertEqual([row['subject'] for row in work['mailbox_attention']], ['Other institution'])
        self.assertEqual(self.box.research_thread.call_count, 6)

    def test_service_senders_are_bulk_except_collaboration_service_notices(self):
        automated = [
            'iclr2099-notifications@example.invalid', 'info@example.invalid', 'support@example.invalid',
            'documents@example.invalid', 'antigravity-science-support@example.invalid',
            'worldofhyatt@loyalty.hotel.example.invalid', 'intellisys@example.invalid',
            'projectbilling@example.invalid', 'newsletter2027@example.invalid', 'lab.news+daily@example.invalid',
            'projectalerts@example.invalid', 'digest_weekly@example.invalid', 'marketing@example.invalid',
            'loyalty@example.invalid', 'service@example.invalid', 'mailer@example.invalid',
            'bounce@example.invalid', 'admin@example.invalid', 'system@example.invalid',
            'team@example.invalid', 'hello@example.invalid', 'project.do-not-reply@example.invalid',
            'noreplyproject@example.invalid',
            'staff@comms.example.invalid', 'staff@news.example.invalid', 'staff@notifications.example.invalid',
        ]
        self.config['mailbox']['service_attention_domains'] = ['writing.example.invalid']
        bulk = [message(f'bulk-{i}', sender, 'Update') for i, sender in enumerate(automated)]
        service = message('service', 'project-notices@mail.writing.example.invalid', 'Shared project update',
                          headers=[('Auto-Submitted', 'auto-generated')])
        outside = message('outside', 'staff@otherwriting.example.invalid', 'Outside service')
        ordinary = [message('mail-' + part, 'staff@' + part + '.example.invalid', part)
                    for part in ('mail', 'email')]
        self.retain(*({**row, 'category': 'person'} for row in [*bulk, service, outside, *ordinary]))
        for live in (False, True):
            with self.subTest(live=live):
                if live:
                    self.tick(*bulk, service, outside, *ordinary)
                work = self.inventory()
                self.assertEqual(work['mailbox_category_counts'],
                                 {'bulk': len(bulk), 'service-notice': 1, 'person': 3})
                self.assertEqual([row['subject'] for row in work['mailbox_attention']],
                                 ['Shared project update', 'Outside service', 'mail', 'email'])
        self.assertEqual(self.box.research_thread.call_count, 3)
        self.assertEqual(self.reply_tasks(), [])

    def test_departmental_reply_to_group_uses_sent_task_owner(self):
        self.contacts.append({'id': 'third', 'email': THIRD, 'name': 'Third Fixture'})
        self.table('contacts', {'contacts': self.contacts})
        subject = 'Group note on sets'
        task_id = self.add_sent_task('firstcontact', subject, COLLEAGUE, cc=[AUTHOR, THIRD])
        self.box.sent = [{**message('out', OWNER, subject), 'sentDateTime': SENT_AT,
                          'toRecipients': [addr(COLLEAGUE)], 'ccRecipients': [addr(AUTHOR), addr(THIRD)]}]
        self.sent_messages = self.box.sent
        incoming = message('dept', 'department@example.invalid', 'RE: ' + subject)
        self.retain(incoming)
        item, = self.inventory()['mailbox_attention']
        self.assertEqual((item['contact_id'], item['task_id']), ('author', task_id))
        self.assertEqual(self.reply_tasks(), [])

        self.tick(incoming)
        with patch.object(self.ledger.tasks, 'all', side_effect=AssertionError('full ledger scan')):
            self.tick()
        row, = self.reply_tasks()
        inputs = row['request']['inputs']
        self.assertEqual(row['owner'], 'author')
        self.assertEqual(inputs['owner_ids'], ['author', 'colleague', 'third'])
        self.assertNotIn('mailbox_triage', inputs)
        self.assertIn('linked_sent_message_id', inputs)
        self.assertIn('cannot be uniquely matched to a recipient', inputs['instructions'])
        self.assertNotIn('registry_proposal_hint', inputs)
        self.assertEqual(self.inventory()['mailbox_attention'], [])

    def test_same_subject_in_different_sent_tasks_stays_ambiguous(self):
        subject = 'Group note on sets'
        self.add_sent_task('firstcontact', subject, AUTHOR, cc=[COLLEAGUE])
        self.add_sent_task('reply', subject, AUTHOR, message_id='<other@example.invalid>')
        self.tick(message('dept', 'department@example.invalid', 'RE: ' + subject))
        item, = self.inventory()['mailbox_attention']
        self.assertEqual(item['category'], 'linked-reply')
        self.assertEqual(item['category_reason'], 'Sent subject matches multiple Sent messages')
        self.assertEqual(self.reply_tasks(), [])

    def test_registered_replies_stage_before_a_previously_deferred_unregistered_reply(self):
        self.record_sent_history(AUTHOR, COLLEAGUE)
        subject = 'A prior mathematical question'
        unknown = message('unknown', 'visitor@example.invalid', 'FW: ' + subject)
        self.sent_messages = [sent_message('out', subject)]
        self.retain({**unknown, 'intake_reason': 'tick budget; retained'})
        self.threads['unknown'] = [unknown]
        tick = Tick(seconds=60)
        tick.bind(self.ledger)
        tick.previous['mailbox-intake'] = [digest(unknown['internetMessageId'])]

        self.tick(message('registered-a', AUTHOR, 'A new question'),
                  message('registered-b', COLLEAGUE, 'Re: ' + subject))

        self.assertEqual(self.box.research_thread.call_args_list,
                         [call('registered-a'), call('registered-b'), call('unknown')])
        self.assertEqual([r['request']['inputs']['thread_id'] for r in self.reply_tasks()],
                         ['registered-a', 'registered-b', 'unknown'])
        self.assertEqual(tick.count, 4)
        self.assertEqual(len(self.sent_queries()), 1)

    def test_one_listing_matches_the_old_per_message_windows_and_is_reused_after_restart(self):
        subjects = ('A result with zero matches', 'A result with one match', 'A result with two matches')
        visitors = [message(str(i), 'visitor@example.invalid', 'RE: ' + subject, minute=i)
                    for i, subject in enumerate(subjects)]
        earliest = mailbox.instant(visitors[0]['receivedDateTime']) - SENT_SUBJECT_WINDOW
        boundary = mailbox.instant(visitors[1]['receivedDateTime']) - SENT_SUBJECT_WINDOW
        sent = [sent_message('too-old', subjects[1], earliest.isoformat()),
                sent_message('boundary', subjects[1], boundary.isoformat()),
                sent_message('postdated', subjects[1], visitors[2]['receivedDateTime']),
                sent_message('first', subjects[2]), sent_message('second', subjects[2],
                                                                visitors[2]['receivedDateTime'])]
        self.sent_messages = sent
        expected = {}
        for visitor in visitors:
            received = mailbox.instant(visitor['receivedDateTime'])
            candidates = [{'sent': row} for row in sent if
                          received - SENT_SUBJECT_WINDOW <= mailbox.instant(row['sentDateTime']) <= received
                          and normalized_subject(row['subject']) == normalized_subject(visitor['subject'])]
            _, link = sent_subject_match(visitor, people(self.root), OWNER, candidates)
            if link:
                expected[visitor['conversationId']] = link['sent']['internetMessageId']

        self.ledger.tasks.clock = lambda: 0
        self.tick(*visitors)
        self.assertEqual(expected, {'1': '<boundary@example.invalid>'})
        self.assertEqual({r['request']['inputs']['thread_id']: r['request']['inputs']['linked_sent_message_id']
                          for r in self.reply_tasks()}, expected)
        query, = self.sent_queries()
        self.assertEqual(query['$select'], [
            'id,sentDateTime,toRecipients,ccRecipients,subject,internetMessageId,conversationId'])
        self.assertIn('sentDateTime ge ' + mailbox.utc_stamp(earliest), query['$filter'][0])
        self.assertNotIn('private fixture body', json.dumps(self.sent_index()))

        self.ledger.save(force=True)
        self.ledger.tasks.close()
        self.ledger = Ledger(self.releases, self.folder)
        self.ledger.tasks.clock = lambda: mailbox.UNMAPPED_RECHECK_SECONDS
        self.box.graph.pages.reset_mock()
        self.box.research_thread.reset_mock()
        self.tick(now=NOW + timedelta(hours=6))
        self.assertEqual(self.box.research_thread.call_args_list, [call('0'), call('2')])
        self.assertEqual(self.sent_queries(), [])

    def test_incremental_refresh_links_new_sent_and_drops_rows_outside_the_window(self):
        subject = 'A newly sent mathematical question'
        old = message('old', 'visitor@example.invalid', 'Re: A prior mathematical question')
        boundary = mailbox.instant(old['receivedDateTime']) - SENT_SUBJECT_WINDOW
        self.sent_messages = [sent_message('old-sent', 'A prior mathematical question', boundary.isoformat())]
        self.tick(old)
        before = self.sent_index()
        self.assertEqual([s['id'] for rows in before['subjects'].values() for s in rows], ['old-sent'])

        self.box.graph.pages.reset_mock()
        sent_at = NOW + timedelta(minutes=5)
        self.sent_messages.append(sent_message('new-sent', subject, sent_at.isoformat()))
        new = {**message('new', 'visitor@example.invalid', 'Fwd: ' + subject),
               'receivedDateTime': (sent_at + timedelta(minutes=1)).isoformat()}
        self.tick(new, now=NOW + timedelta(minutes=10))
        query, = self.sent_queries()
        self.assertIn('sentDateTime gt ' + before['through'], query['$filter'][0])
        after = self.sent_index()
        self.assertEqual(after['built_at'], before['built_at'])
        self.assertEqual([s['id'] for rows in after['subjects'].values() for s in rows], ['new-sent'])
        self.assertEqual([r['request']['inputs']['linked_sent_message_id'] for r in self.reply_tasks()],
                         ['<old-sent@example.invalid>', '<new-sent@example.invalid>'])

    def test_malformed_or_day_old_index_is_rebuilt(self):
        subject = 'A prior mathematical question'
        self.sent_messages = [sent_message('out', subject)]
        self.tick(message('first', 'visitor@example.invalid', 'RE: ' + subject))
        valid = self.sent_index()
        for label, state, now in (
            ('malformed', {**valid, 'subjects': {normalized_subject(subject): [{'sentDateTime': SENT_AT}]}}, NOW),
            ('day-old', valid, NOW + timedelta(days=1)),
        ):
            with self.subTest(label=label):
                self.ledger.tasks.db.execute('INSERT OR REPLACE INTO pipeline_state VALUES(?,?)',
                                             (mailbox.SENT_SUBJECTS_KEY, canonical(state)))
                self.box.graph.pages.reset_mock()
                self.tick(message(label, 'visitor@example.invalid', 'RE: ' + subject), now=now)
                query, = self.sent_queries()
                self.assertTrue(query['$filter'][0].startswith('sentDateTime ge '))
                self.assertEqual(self.sent_index()['built_at'], mailbox.utc_stamp(now))
        self.assertEqual(len(self.reply_tasks()), 3)

    def test_slice_exhausted_at_index_defers_subject_fallback_after_registered_and_exact_links(self):
        self.record_sent_history(AUTHOR)
        subject = 'A prior mathematical question'
        sent = sent_message('out', subject)
        self.sent_messages = [sent]
        elapsed = 0
        tick = Tick(seconds=10, clock=lambda: elapsed)
        tick.bind(self.ledger)

        def read_thread(conversation):
            nonlocal elapsed
            if conversation == 'fallback':
                elapsed = 10
            return self.threads[conversation]

        self.box.research_thread.side_effect = read_thread
        self.tick(message('plain', 'visitor@example.invalid', subject),
                  message('exact', 'visitor@example.invalid', 'RE: ' + subject,
                          headers=[('In-Reply-To', sent['internetMessageId'])]),
                  message('fallback', 'visitor@example.invalid', 'RE: ' + subject),
                  message('registered', AUTHOR, 'RE: ' + subject))

        self.assertEqual([r['request']['inputs']['thread_id'] for r in self.reply_tasks()], ['registered', 'exact'])
        query, = self.sent_queries()
        self.assertEqual(query['$filter'], ["internetMessageId eq '<out@example.invalid>'"])
        retained = {r['id']: r for r in self.inventory()['mailbox_needs_classification']}
        self.assertEqual(retained['plain']['intake_reason'], mailbox.UNREGISTERED)
        self.assertEqual(retained['fallback']['category_reason'], 'Tick time budget exhausted')
        self.assertEqual(tick.pending['mailbox-intake'], [digest('<fallback@example.invalid>')])
        self.assertIsNone(self.sent_index())

    def test_likely_contact_from_unique_reversed_name_and_prior_sent(self):
        self.add_sent_task('firstcontact', 'Earlier note', AUTHOR)
        self.add_sent_task('firstcontact', 'Unsent packet', COLLEAGUE, owner='colleague', actual=False)
        inquiries = [message(key, 'personal@example.invalid', subject, name='FIXTURE, WRITER')
                     for key, subject in [('offer', 'New offer'), ('cite', 'Citation question')]]
        without_sent = message('no-sent', 'different@example.invalid', 'Hello', name='Fixture Colleague')
        self.retain(*({**row, 'category': 'person'} for row in [*inquiries, without_sent]))
        for live in (False, True):
            with self.subTest(live=live):
                if live:
                    self.tick(*inquiries, without_sent)
                attention = self.inventory()['mailbox_attention']
                self.assertEqual([(row['category'], row.get('contact_id')) for row in attention],
                                 [('likely-contact', 'author'), ('likely-contact', 'author'), ('person', None)])
                self.assertEqual(self.reply_tasks(), [])
        self.assertEqual(self.box.research_thread.call_count, 3)

    def test_likely_contact_requires_unique_name_with_at_least_two_tokens(self):
        self.contacts[1]['name'] = 'Fixture Writer'
        self.contacts.append({'id': 'third', 'email': THIRD, 'name': 'Singleton'})
        self.table('contacts', {'contacts': self.contacts})
        self.add_sent_task('firstcontact', 'Earlier note', AUTHOR)
        self.add_sent_task('firstcontact', 'Other note', COLLEAGUE, owner='colleague')
        self.add_sent_task('firstcontact', 'Earlier note', THIRD, owner='third')
        self.tick(message('duplicate', 'personal@example.invalid', 'New question', name='Fixture Writer'),
                  message('singleton', 'different@example.invalid', 'Another question', name='Singleton'))
        self.assertEqual(self.inventory()['mailbox_category_counts'], {'person': 2})
        self.assertEqual(self.reply_tasks(), [])

    def test_attention_orders_by_category_then_received_time(self):
        self.add_sent_task('firstcontact', 'Earlier note', AUTHOR)
        Tick(seconds=0).bind(self.ledger)
        self.config['mailbox']['service_attention_domains'] = ['writing.example.invalid']
        self.tick(
            message('newest', 'visitor@example.invalid', 'Person newer', minute=59),
            message('service', 'notices@writing.example.invalid', 'Service notice', minute=50),
            message('action', 'editor@journal.example.invalid', 'Manuscript decision', minute=40),
            message('likely', 'personal@example.invalid', 'Likely contact', name='Fixture Writer', minute=30),
            message('known', COLLEAGUE, 'Known contact', minute=20),
            message('linked', 'office@example.invalid', 'RE: Earlier note', minute=10),
            message('oldest', 'visitor@example.invalid', 'Person older', minute=0),
        )
        self.assertEqual([row['subject'] for row in self.inventory()['mailbox_attention']],
                         ['RE: Earlier note', 'Known contact', 'Likely contact', 'Manuscript decision',
                          'Service notice', 'Person newer', 'Person older'])

    def test_display_name_with_registered_cc_keeps_both_new_subjects_in_attention(self):
        self.add_sent_task('firstcontact', 'Earlier note', AUTHOR)
        inquiries = [message(key, 'personal@example.invalid', subject, name='FIXTURE, writer',
                             cc=[COLLEAGUE])
                     for key, subject in [('offer', 'Joint paper offer'), ('cite', 'May we cite your result?')]]
        self.tick(*inquiries)
        self.assertEqual(self.reply_tasks(), [])
        self.assertEqual([(row['category'], row['contact_id']) for row in self.inventory()['mailbox_attention']],
                         [('likely-contact', 'author'), ('likely-contact', 'author')])

    def test_departmental_reply_links_to_a_sent_copy(self):
        self.box.sent = {'messages': [{**message('out', OWNER, 'A result  on sets'),
                                       'toRecipients': [addr(AUTHOR)], 'sentDateTime': SENT_AT}]}
        self.tick(message('dept', 'department@example.invalid', '  RE: Fwd: AW: 回复: 答复: FW: a RESULT on sets ',
                          cc=[AUTHOR, COLLEAGUE]))
        row, = self.reply_tasks()
        inputs = row['request']['inputs']
        self.assertEqual(row['owner'], 'author')
        self.assertEqual(inputs['owner_ids'], ['author', 'colleague'])
        self.assertNotIn('registry_proposal_hint', inputs)

    def test_ledger_reply_subject_without_prefix_and_unsent_packet(self):
        task_id = self.add_sent_task('reply', 'Re: Earlier result on sets', AUTHOR)
        self.add_sent_task('firstcontact', 'Unsent topic', AUTHOR, actual=False)
        self.tick(message('plain', 'department@example.invalid', 'earlier result on sets'),
                  message('coincidence', 'unknown@example.invalid', 'Unsent topic'))
        self.assertEqual(self.reply_tasks(), [])
        plain, coincidence = self.inventory()['mailbox_attention']
        self.assertEqual(plain['task_id'], task_id)
        self.assertIn('lacks a reply or forward prefix', plain['category_reason'])
        self.assertEqual((coincidence['subject'], coincidence['category']), ('Unsent topic', 'person'))

    def test_sent_event_supplies_subject_after_adapter_restart(self):
        self.contacts[0]['hub'] = {'events': [{'kind': 'sent', 'at': SENT_AT,
            'summary': 'Earlier result on sets — Reviewed headline',
            'evidence': json.dumps({'source': 'sent-events', 'recipient': AUTHOR,
                                    'internetMessageId': '<event@example.invalid>'})}]}
        self.table('contacts', {'contacts': self.contacts})
        self.tick(message('from-event', 'department@example.invalid', 'Re: Earlier result on sets'))
        row, = self.reply_tasks()
        self.assertEqual(row['owner'], 'author')

    def test_sent_subject_with_no_registered_owner_needs_attention(self):
        self.box.sent = [{**message('out', OWNER, 'Earlier result'),
                          'toRecipients': [addr('unregistered@example.invalid')], 'sentDateTime': SENT_AT}]
        self.tick(message('from-unknown', 'department@example.invalid', 'Earlier result'))
        item, = self.inventory()['mailbox_attention']
        self.assertEqual(item['category'], 'linked-reply')
        self.assertIn('no registered owner', item['category_reason'])
        self.assertEqual(self.reply_tasks(), [])

    def test_registered_address_starts_new_thread_and_restart_is_idempotent(self):
        self.record_sent_history(AUTHOR)
        incoming = message('start', AUTHOR, 'A new collaboration', cc=[COLLEAGUE])
        self.tick(incoming)
        self.ledger.save(force=True)
        self.ledger.tasks.close()
        self.ledger = Ledger(self.releases, self.folder)
        self.box.research_thread.reset_mock()
        self.tick(incoming)
        row, = self.reply_tasks()
        self.assertEqual(row['owner'], 'author')
        self.assertEqual(row['request']['inputs']['owner_ids'], ['author', 'colleague'])
        self.box.research_thread.assert_called_once_with('start')
        self.assertEqual(self.inventory()['mailbox_attention'], [])

    def test_editorial_and_person_attention_with_bulk_counts(self):
        self.contacts[0]['hub'] = {'events': [{'kind': 'reply', 'at': SENT_AT, 'evidence': 'fixture:reply',
            'summary': 'The joint manuscript ABC-D-26-01234 was submitted'}]}
        self.table('contacts', {'contacts': self.contacts})
        self.tick(
            message('authorship', 'editorial-notices@journal.example.invalid', 'Confirm co-authorship: ABC-D-26-01234',
                    headers=[('Auto-Submitted', 'auto-generated')], minute=2),
            message('decision', 'editor@sub.journal.example.invalid', 'Decision on Manuscript ID: 98765', minute=3),
            message('receipt', 'editorial-notices@journal.example.invalid', 'Your PDF has been built'),
            message('list', 'digest@example.invalid', 'Weekly digest',
                    headers=[('List-Unsubscribe', '<mailto:list@example.invalid>')]),
            message('notification', 'notifications-project@example.invalid', 'Service notification'),
            message('unknown', 'stranger@example.invalid', 'A question', name='Visitor Fixture', minute=1))
        work = self.inventory()
        self.assertEqual([m['category'] for m in work['mailbox_attention']],
                         ['editorial-action', 'editorial-action', 'person'])
        decision, authorship, person = work['mailbox_attention']
        self.assertEqual(decision['manuscript_id'], '98765')
        self.assertEqual(authorship['manuscript_id'], 'ABC-D-26-01234')
        self.assertIs(authorship['joint_manuscript'], True)
        self.assertEqual(authorship['coauthor_contact_ids'], ['author'])
        self.assertEqual(person['from'], 'stranger@example.invalid')
        self.assertEqual(person['name'], 'Visitor Fixture')
        self.assertEqual(work['mailbox_category_counts'],
                         {'bulk': 2, 'editorial-info': 1, 'editorial-action': 2, 'person': 1})
        self.assertEqual(self.reply_tasks(), [])
        self.assertEqual(self.box.research_thread.call_count, 1)

    def test_manuscript_id_from_submission_task(self):
        request = pipeline.request('submission', 'fixture', 'author', {'paper': {
            'manuscript_id': 'JNL-1234', 'authors': ['Writer Fixture', 'Colleague Fixture']}}, 'a' * 40)
        self.ledger.tasks.add(request)
        self.tick(message('proof', 'editor@journal.example.invalid', 'Proof for JNL-1234'))
        action, = self.inventory()['mailbox_attention']
        self.assertTrue(action['joint_manuscript'])
        self.assertEqual(action['coauthor_contact_ids'], ['author', 'colleague'])
        self.assertEqual(self.reply_tasks(), [])

    def test_refusal_and_owner_controlled_are_not_staged(self):
        self.table('refusals', {'refusals': [{'id': 'declined', 'active': True,
                                              'name': 'Writer Fixture', 'emails': [AUTHOR]}]})
        self.table('decisions', {'owner_controlled_threads': ['Colleague Fixture']})
        self.record_sent_history(COLLEAGUE)
        self.add_sent_task('firstcontact', 'Earlier result on sets', AUTHOR)
        self.tick(message('declined', 'department@example.invalid', 'RE: Earlier result on sets'),
                  message('owner-controlled', COLLEAGUE, 'Another subject'))
        work = self.inventory()
        self.assertEqual(self.reply_tasks(), [])
        self.assertEqual(work['mailbox_category_counts'], {'linked-reply': 1, 'owner-controlled': 1})
        declined, = work['mailbox_attention']
        self.assertEqual(declined['contact_id'], 'author')
        self.assertIn('refusal', declined['category_reason'])

    def test_time_and_task_budgets_retain_candidates_then_stage_them(self):
        self.record_sent_history(AUTHOR)
        first = message('first', AUTHOR, 'First topic')
        second = message('second', AUTHOR, 'Second topic')
        self.config['automation']['max_new_tasks_per_tick'] = 1
        Tick(seconds=0).bind(self.ledger)
        self.tick(first, second)
        self.assertEqual(self.reply_tasks(), [])
        self.assertTrue(all('Tick time budget' in m['category_reason']
                            for m in self.inventory()['mailbox_attention']))
        del self.ledger.tick
        self.tick()
        one, = self.inventory()['mailbox_attention']
        self.assertIn('task limit', one['category_reason'])
        self.assertEqual(len(self.reply_tasks()), 1)
        self.tick()
        self.assertEqual(len(self.reply_tasks()), 2)
        self.assertEqual(self.inventory()['mailbox_attention'], [])

    def test_legacy_unmapped_row_keeps_its_intake_reason(self):
        row = message('older', 'digest@example.invalid', 'Digest', headers=[('Feedback-ID', '')])
        self.threads['older'] = [row]
        self.ledger.tasks.db.execute('INSERT INTO pipeline_state VALUES(?,?)', ('mailbox', canonical({
            'unmapped': [{**row, 'intake_reason': mailbox.UNREGISTERED}]})))
        for refresh in (False, True):
            if refresh:
                self.tick()
            retained, = self.inventory()['mailbox_needs_classification']
            self.assertEqual(retained['category'], 'bulk')
            self.assertEqual(retained['intake_reason'], mailbox.UNREGISTERED)
            self.assertEqual(self.inventory()['mailbox_attention'], [])

    def test_registered_sender_at_mail_subdomain_stages_with_registry_history(self):
        registered = 'staff@mail.school.example.invalid'
        self.contacts[0]['email'] = registered
        self.record_sent_history(registered)
        self.tick(message('mail-subdomain', registered, 'A new mathematical question'))
        row, = self.reply_tasks()
        self.assertEqual(row['owner'], 'author')
        self.assertEqual(row['request']['inputs']['prior_context'], 'registry-only')
        self.box.research_thread.assert_called_once_with('mail-subdomain')

    def test_registered_sender_with_auto_response_header_stages_before_triage(self):
        registered = 'support@journal.example.invalid'
        self.contacts[0]['email'] = registered
        self.record_sent_history(registered)
        self.tick(message('registered-editor', registered, 'Our manuscript decision',
                          headers=[('X-Auto-Response-Suppress', 'All')]))
        row, = self.reply_tasks()
        self.assertEqual(row['owner'], 'author')
        self.box.research_thread.assert_called_once_with('registered-editor')

    def test_registered_sender_owns_message_despite_subject_sent_only_to_other_contact(self):
        self.record_sent_history(AUTHOR)
        subject = 'A prior mathematical question'
        self.add_sent_task('firstcontact', subject, COLLEAGUE, owner='colleague')
        self.tick(message('registered', AUTHOR, 'Re: ' + subject))
        row, = self.reply_tasks()
        self.assertEqual(row['owner'], 'author')
        self.assertEqual(row['request']['inputs']['owner_ids'], ['author'])

    def test_hello_stranger_stays_in_attention(self):
        self.add_sent_task('firstcontact', 'Re: Hello', COLLEAGUE, owner='colleague')
        self.tick(message('stranger', 'visitor@example.invalid', 'Hello'),
                  message('short-reply', 'visitor@example.invalid', 'Re: Hello'))
        self.assertEqual(self.reply_tasks(), [])
        reasons = {item['subject']: item['category_reason'] for item in self.inventory()['mailbox_attention']}
        self.assertIn('lacks a reply or forward prefix', reasons['Hello'])
        self.assertIn('fewer than 16 characters or 3 words', reasons['Re: Hello'])

    def test_display_name_alias_with_exact_header_stages_with_alias_hint(self):
        self.record_sent_history(AUTHOR)
        sent = {**message('older-sent', OWNER, 'Different subject'), 'sentDateTime': SENT_AT,
                'toRecipients': [addr(AUTHOR)]}
        alias = 'personal@elsewhere.example.invalid'
        incoming = message('alias', alias, 'Hello', name='Fixture Writer',
                           headers=[('In-Reply-To', sent['internetMessageId'])])
        with patch.object(self.box, 'sent_message_by_internet_id', return_value=sent) as lookup:
            self.tick(incoming)
        lookup.assert_called_once_with(sent['internetMessageId'])
        self.box.research_thread.assert_called_once_with('alias')
        row, = self.reply_tasks()
        self.assertEqual(row['owner'], 'author')
        hint = row['request']['inputs']['registry_proposal_hint']
        self.assertEqual((hint['email'], hint['contact_id']), (alias, 'author'))

    def test_refusal_pauses_thread_reads_for_six_hours_even_with_registered_sender(self):
        self.record_sent_history(AUTHOR)
        self.table('refusals', {'refusals': [{'active': True, 'name': 'Writer Fixture', 'emails': [AUTHOR]}]})
        self.ledger.tasks.clock = lambda: 0
        self.tick(message('refused', AUTHOR, 'Another question'))
        retained, = self.inventory()['mailbox_needs_classification']
        self.assertEqual(retained['checked_at'], 0)
        for offset in (1, mailbox.UNMAPPED_RECHECK_SECONDS - 1):
            self.ledger.tasks.clock = lambda: offset
            self.tick()
        self.box.research_thread.assert_called_once_with('refused')
        self.ledger.tasks.clock = lambda: mailbox.UNMAPPED_RECHECK_SECONDS
        self.tick()
        self.assertEqual(self.box.research_thread.call_count, 2)
        self.assertEqual(self.reply_tasks(), [])

    def test_refusal_check_handles_missing_sender_email_address(self):
        self.table('refusals', {'refusals': [{'active': True, 'name': 'Writer Fixture', 'emails': [AUTHOR]}]})
        sent = {**message('out', OWNER, 'A prior letter'), 'sentDateTime': SENT_AT,
                'toRecipients': [addr(AUTHOR)]}
        incoming = message('no-sender', '', 'Hello', headers=[('References', sent['internetMessageId'])])
        incoming['from'] = {}
        with patch.object(self.box, 'sent_message_by_internet_id', return_value=sent):
            self.tick(incoming)
        item, = self.inventory()['mailbox_attention']
        self.assertIn('refusal', item['category_reason'])

    def test_owner_controlled_row_is_released_after_manual_reply(self):
        self.record_sent_history(AUTHOR)
        self.table('decisions', {'owner_controlled_threads': ['Writer Fixture']})
        incoming = message('manual', AUTHOR, 'A mathematical question')
        followup = {**message('manual-next', AUTHOR, 'A follow-up', minute=1), 'conversationId': 'manual'}
        self.tick(incoming, followup)
        self.assertEqual(self.inventory()['mailbox_category_counts'], {'owner-controlled': 2})
        sent = {**message('manual-sent', OWNER, 'Re: A mathematical question'),
                'conversationId': 'manual', 'sentDateTime': '2026-11-18T09:10:00Z',
                'toRecipients': [addr(AUTHOR)]}
        self.threads['manual'].append(sent)
        self.tick()
        self.assertEqual(self.inventory()['mailbox_needs_classification'], [])
        self.assertEqual(self.reply_tasks(), [])

    def test_owner_controlled_row_stages_after_name_removed_from_decisions(self):
        self.record_sent_history(AUTHOR)
        self.table('decisions', {'owner_controlled_threads': ['Writer Fixture']})
        self.tick(message('controlled', AUTHOR, 'A mathematical question'))
        self.assertEqual(self.reply_tasks(), [])
        self.table('decisions', {'owner_controlled_threads': []})
        self.tick()
        row, = self.reply_tasks()
        self.assertEqual(row['owner'], 'author')
        self.assertEqual(self.inventory()['mailbox_needs_classification'], [])

    def test_never_written_registered_sender_stays_in_attention(self):
        self.add_sent_task('firstcontact', 'A new mathematical question', AUTHOR, actual=False)
        self.tick(message('new-contact', AUTHOR, 'A new mathematical question'))
        self.assertEqual(self.reply_tasks(), [])
        item, = self.inventory()['mailbox_attention']
        self.assertEqual((item['category'], item['contact_id']), ('known-contact', 'author'))
        self.assertIn('no current thread Sent mail or registry history', item['category_reason'])

    def test_fifteen_day_old_exact_link_stays_in_attention(self):
        sent = {**message('old-sent', OWNER, 'A prior letter'), 'sentDateTime': '2026-11-02T09:00:00Z',
                'toRecipients': [addr(AUTHOR)]}
        incoming = {**message('backlog', 'personal@example.invalid', 'Re: A prior letter',
                              headers=[('In-Reply-To', sent['internetMessageId'])]),
                    'receivedDateTime': (NOW - timedelta(days=15)).isoformat()}
        self.retain(incoming)
        self.threads['backlog'] = [incoming]
        with patch.object(self.box, 'sent_message_by_internet_id', return_value=sent):
            self.tick()
        self.assertEqual(self.reply_tasks(), [])
        item, = self.inventory()['mailbox_attention']
        self.assertEqual(item['category'], 'linked-reply')
        self.assertIn('older than 14 days', item['category_reason'])
        with patch.object(self.box, 'sent_message_by_internet_id', return_value=sent):
            self.tick()
        self.box.research_thread.assert_called_once_with('backlog')
        self.assertEqual(self.reply_tasks(), [])
