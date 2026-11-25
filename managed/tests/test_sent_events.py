"""Sent-folder facts, privacy, bounded recovery and serialized table writes."""
from contextlib import redirect_stderr
from datetime import datetime, timedelta, timezone
import io
import json
import os
from pathlib import Path
import subprocess
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from managed import control, mailbox, registry, sent_events
from managed.queue import digest
from managed.tick import Tick
from test_tasks import Releases, request

SINCE = '2026-11-01T00:00:00Z'
SENT = '2026-11-04T01:00:00Z'
NOW = datetime(2026, 11, 5, tzinfo=timezone.utc)
ACCOUNT = 'owner@example.invalid'
CONTACT = 'scholar@example.invalid'
NEXT = mailbox.Graph.BASE + '/me/mailFolders/sentitems/messages?$skiptoken=next'


def participant(value):
    return {'emailAddress': {'address': value}}


def message(identity='one', recipients=None, **extra):
    return {'id': 'graph-' + identity, 'internetMessageId': '<' + identity + '@example.invalid>',
            'isDraft': False, 'sentDateTime': SENT, 'subject': 'A checked answer',
            'toRecipients': [participant(a) for a in (recipients or [CONTACT])],
            'ccRecipients': [], 'bccRecipients': [], **extra}


class Graph:
    def __init__(self, pages):
        self.responses = pages
        self.calls = []

    def request(self, path, method='GET', body=None):
        assert method == 'GET'
        self.calls.append(path)
        if path.startswith('/me?$select='):
            return {'mail': ACCOUNT}
        return self.responses[1 if '$skiptoken=next' in path or '$skip=100' in path else 0]


class SentEventsFixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / 'tables').mkdir()
        (self.root / 'ledger').mkdir()
        self.ledger = control.Ledger(Releases(), self.root / 'ledger')
        self.addCleanup(self.ledger.tasks.close)
        for name, value in [('contacts', {'contacts': []}), ('refusals', {'refusals': []}),
                            ('positive', {'entries': []})]:
            registry.write(self.root, name, value)
        registry.register(CONTACT, 'Professor Scholar', root=self.root)
        self.config = {'paused': False, 'mailbox': {'team_addresses': ['team@example.invalid']}}
        self.commit = self.enterContext(patch.object(sent_events, 'commit_tables'))
        self.needs_push = self.enterContext(patch.object(sent_events, 'tables_need_push', return_value=True))
        self.set_messages([message()])

    def set_messages(self, messages, pages=None):
        self.graph = Graph(pages or [{'value': messages}])
        self.box = mailbox.GraphMailbox(self.graph, {'account': ACCOUNT, 'reconciled': True, 'live_since': SINCE})

    def tick(self):
        return sent_events.reconcile(self.ledger, self.config, self.root, self.box, NOW)

    def events(self):
        return [event for row in registry.read(self.root, 'contacts')['contacts']
                for event in row.get('hub', {}).get('events', [])]

    def task(self, receipt='sent_receipt', brief=False, reviewed=True):
        req = request('reply-1', CONTACT, 'reply')
        row = self.ledger.tasks.add(req)
        result = {'headline': 'Answers the finite-rank question.', 'packet': {'body': 'PRIVATE BODY'},
                  'report': 'PRIVATE REPORT'}
        if brief:
            result.pop('headline')
            result['owner_brief'] = {'summary_zh': '回答有限秩的问题。'}
        data = {'result': result, receipt: {'remote_id': 'graph-one',
                                          'evidence_ref': 'graph:sent:<one@example.invalid>'}}
        if reviewed:
            data['review'] = {'verdict': 'PASS', 'binding': {'task_id': req['id'],
                              'workflow_hash': req['workflow_hash'], 'result_hash': digest(result)}}
        return self.ledger.tasks.update(row['id'], row['revision'], 'verified', data)


class SentEventsTests(SentEventsFixture):
    def test_owner_draft_sent_uses_exact_sent_receipt_and_reviewed_headline(self):
        self.task()
        self.assertEqual(self.tick()['events'], 1)
        event, = self.events()
        self.assertEqual(event['at'], SENT)
        self.assertEqual(event['summary'], 'A checked answer — Answers the finite-rank question.')
        evidence = json.loads(event['evidence'])
        self.assertEqual(evidence['internetMessageId'], '<one@example.invalid>')
        self.assertEqual(evidence['task_id'], 'reply-1')
        for file in sent_events.FILES:
            text = (self.root / file).read_text()
            self.assertNotIn('PRIVATE', text)

    def test_operator_created_draft_sent_without_ledger_task(self):
        self.assertEqual(self.tick()['events'], 1)
        event, = self.events()
        self.assertEqual(event['summary'], 'A checked answer')
        self.assertNotIn('task_id', json.loads(event['evidence']))

    def test_sent_evidence_preserves_conversation_account_and_sender(self):
        self.set_messages([message(conversationId='conversation', **{'from': participant(ACCOUNT)})])
        self.tick()
        evidence = json.loads(self.events()[0]['evidence'])
        self.assertEqual((evidence['id'], evidence['conversationId'], evidence['account'], evidence['from']),
                         ('graph-one', 'conversation', ACCOUNT, ACCOUNT))

    def test_operator_direct_send_with_exact_sent_receipt_uses_owner_brief(self):
        self.task(brief=True)
        self.assertEqual(self.tick()['events'], 1)
        self.assertEqual(self.events()[0]['summary'], 'A checked answer — 回答有限秩的问题。')

    def test_sent_message_matches_immutable_draft_receipt(self):
        self.task('draft_receipt', brief=True)
        self.assertEqual(self.tick()['events'], 1)
        self.assertEqual(self.events()[0]['summary'], 'A checked answer — 回答有限秩的问题。')

    def test_unreviewed_task_never_exports_report_or_packet(self):
        self.task(reviewed=False)
        self.tick()
        self.assertEqual(self.events()[0]['summary'], 'A checked answer')

    def test_internet_identity_matches_receipt_after_graph_id_changes(self):
        self.task()
        self.set_messages([message(id='new-id')])
        self.tick()
        self.assertIn('reply-1', self.events()[0]['evidence'])

    def test_unregistered_recipient_is_skipped_without_registration(self):
        self.set_messages([message(recipients=['unknown@example.invalid'])])
        before = registry.read(self.root, 'contacts')
        self.assertEqual(self.tick()['events'], 0)
        self.assertEqual(registry.read(self.root, 'contacts'), before)

    def test_team_account_and_system_recipients_skipped_even_if_registered(self):
        addresses = ['team@example.invalid', ACCOUNT, 'no-reply@example.invalid',
                     'mailer-daemon@example.invalid', 'notifications+abc@example.invalid', ('@'.join(('help', 'arxiv.org'))),
                     'system@example.invalid', 'bounce@example.invalid', ('@'.join(('submit', 'export.arxiv.org')))]
        for value in addresses:
            registry.register(value, value, root=self.root)
        self.set_messages([message(recipients=addresses)])
        self.assertEqual(self.tick()['events'], 0)

    def test_to_cc_bcc_and_verified_aliases_use_exact_addresses(self):
        rows = registry.read(self.root, 'contacts')
        rows['contacts'][0]['relationship'] = {'aliases': ['alias@example.invalid']}
        registry.write(self.root, 'contacts', rows)
        for field in ('toRecipients', 'ccRecipients', 'bccRecipients'):
            with self.subTest(field=field):
                self.set_messages([message(identity=field, toRecipients=[], **{
                    field: [participant('ALIAS@example.invalid'), participant('unknown@example.invalid')]})]
                    if field != 'toRecipients' else [message(identity=field, recipients=['ALIAS@example.invalid'])])
                self.assertEqual(self.tick()['events'], 1)
        self.assertEqual(len(registry.read(self.root, 'contacts')['contacts']), 1)
        self.assertEqual(len(self.events()), 3)

    def test_two_ticks_and_duplicate_page_rows_are_idempotent(self):
        self.set_messages([message(), message()])
        self.assertEqual(self.tick()['events'], 1)
        self.assertEqual(self.tick()['events'], 0)
        self.assertEqual(len(self.events()), 1)

    def test_two_clean_pages_skip_commit_and_forced_save(self):
        self.needs_push.return_value = False
        # A non-token query also exercises recording/replay without redacting
        # the pagination handle under the credential-token masking rules.
        link = NEXT.replace('$skiptoken=next', '$skip=100')
        self.set_messages([], pages=[{'value': [], '@odata.nextLink': link}, {'value': []}])
        with patch.object(self.ledger, 'save', wraps=self.ledger.save) as save:
            self.assertEqual(self.tick(), {'status': 'idle', 'events': 0, 'pages': 2})
        self.assertEqual(save.call_args_list, [unittest.mock.call(), unittest.mock.call()])
        self.commit.assert_not_called()

    def test_changed_since_discards_pending_before_deferred_page(self):
        stale = {'cursor': {'since': '2026-10-01T00:00:00Z',
                            'until': '2026-11-04T00:00:00Z', 'next_link': NEXT}}
        self.ledger.tasks.db.execute('INSERT INTO pipeline_state VALUES(?,?)',
                                     ('sent-events-pending', json.dumps(stale)))
        Tick(seconds=0).bind(self.ledger)
        self.assertEqual(self.tick()['status'], 'deferred')
        self.assertIsNone(self.ledger.tasks.db.execute(
            "SELECT payload FROM pipeline_state WHERE key='sent-events-pending'").fetchone())
        self.assertFalse(any('sentitems' in path for path in self.graph.calls))
        self.commit.assert_not_called()

    def test_operator_registry_reconcile_also_recognizes_automatic_events(self):
        self.tick()
        packet = {'to': [CONTACT], 'cc': [], 'subject': 'A checked answer'}
        task = {'id': 'reply-1', 'kind': 'reply', 'result': {'packet': packet},
                'sent_receipt': {'remote_id': 'changed-graph-id', 'at': SENT,
                                 'evidence_ref': 'graph:sent:<one@example.invalid>',
                                 'content_hash': digest(packet)}}
        report = registry.reconcile({'affairs_tasks': [task]}, root=self.root)
        self.assertEqual(report['exceptions'], [])
        self.assertEqual(report['pending_outgoing'], [])

    def test_manual_evidence_message_id_dedupes_regardless_of_time(self):
        registry.record_event(CONTACT, 'sent', SINCE, 'Manual fact', 'Sent <one@example.invalid>', self.root)
        self.assertEqual(self.tick()['events'], 0)

    def test_manual_event_dedupes_at_inclusive_ten_minute_boundary(self):
        registry.record_event(CONTACT, 'sent', '2026-11-04T09:10:00+08:00', 'Manual fact', 'manual', self.root)
        self.assertEqual(self.tick()['events'], 0)

    def test_manual_event_dedupes_send_to_another_address_of_same_person(self):
        contacts = registry.read(self.root, 'contacts')
        contacts['contacts'][0]['relationship'] = {'aliases': ['scholar@alias.example.invalid']}
        registry.write(self.root, 'contacts', contacts)
        registry.record_event(CONTACT, 'sent', SENT, 'Manual fact', 'manual', self.root)
        self.set_messages([message(recipients=['scholar@alias.example.invalid'])])
        self.assertEqual(self.tick()['events'], 0)

    def test_manual_event_outside_tolerance_and_reply_event_do_not_dedupe(self):
        registry.record_event(CONTACT, 'sent', '2026-11-04T01:10:01Z', 'Manual fact', 'manual', self.root)
        registry.record_event(CONTACT, 'reply', SENT, 'Reply', 'reply', self.root)
        self.assertEqual(self.tick()['events'], 1)

    def test_distinct_actual_messages_inside_ten_minutes_are_both_recorded(self):
        self.set_messages([message(), message('two')])
        self.assertEqual(self.tick()['events'], 2)

    def test_old_future_draft_missing_id_or_invalid_timestamp_are_skipped(self):
        self.set_messages([message(sentDateTime='2026-10-01T00:00:00Z'),
                           message(sentDateTime='2027-01-01T00:00:00Z'), message(isDraft=True),
                           message(internetMessageId=''), message(sentDateTime='invalid'),
                           message(sentDateTime='2026-11-04T01:00:00')])
        self.assertEqual(self.tick()['events'], 0)

    def test_newest_first_metadata_page_has_live_bounds_and_no_body(self):
        self.tick()
        query = parse_qs(urlsplit(self.graph.calls[-1]).query)
        self.assertEqual(query['$orderby'], ['sentDateTime desc'])
        self.assertEqual(query['$top'], ['100'])
        self.assertIn('sentDateTime ge ' + SINCE, query['$filter'][0])
        self.assertIn('sentDateTime le 2026-11-05T00:00:00Z', query['$filter'][0])
        self.assertNotIn('body', query['$select'][0])
        self.assertIn('conversationId', query['$select'][0])
        self.assertIn('from', query['$select'][0])
        self.assertFalse((self.root / 'tables/REFUSALS.md').exists())
        self.assertFalse((self.root / 'tables/SUBMISSIONS.md').exists())

    def test_bounded_paging_resumes_after_restoring_ledger(self):
        self.set_messages([], pages=[{'value': [message()], '@odata.nextLink': NEXT},
                                    {'value': [message('two')]}])
        with patch.object(sent_events, 'MAX_PAGES', 1):
            self.assertEqual(self.tick()['events'], 1)
        restored_dir = self.root / 'restored'
        restored_dir.mkdir()
        restored = control.Ledger(self.ledger.releases, restored_dir)
        self.addCleanup(restored.tasks.close)
        self.ledger = restored
        self.assertEqual(self.tick()['events'], 1)
        self.assertIn('$skiptoken=next', self.graph.calls[-1])
        start = len(self.graph.calls)
        self.assertEqual(self.tick()['events'], 0)
        self.assertNotIn('$skiptoken', self.graph.calls[start])

    def test_graph_key_form_pagination_link_is_followed(self):
        key_form = mailbox.Graph.BASE + "/me/mailFolders('sentitems')/messages?$skiptoken=next"
        self.set_messages([], pages=[{'value': [message()], '@odata.nextLink': key_form},
                                    {'value': [message('two')]}])
        with patch.object(sent_events, 'MAX_PAGES', 1):
            self.assertEqual(self.tick()['events'], 1)
            self.assertEqual(self.tick()['events'], 1)
        self.assertIn("mailFolders('sentitems')", self.graph.calls[-1])

    def test_foreign_pagination_link_is_rejected_before_use(self):
        for link in ('https://evil.example/page', mailbox.Graph.BASE + '/me/mailFolders/inbox/messages?skip=1',
                     mailbox.Graph.BASE + "/me/mailFolders('inbox')/messages?skip=1"):
            with self.subTest(link=link):
                self.set_messages([], pages=[{'value': [message()], '@odata.nextLink': link}])
                with self.assertRaisesRegex(ValueError, 'origin or folder'):
                    self.tick()
                self.assertEqual(self.events(), [])

    def test_oversized_metadata_page_is_rejected(self):
        self.set_messages([message()] * 101)
        with self.assertRaisesRegex(ValueError, 'exceeds its bound'):
            self.tick()
        self.assertEqual(self.events(), [])

    def test_push_failure_retries_persistence_before_advancing_cursor(self):
        self.commit.side_effect = RuntimeError('push failed')
        with self.assertRaises(RuntimeError):
            self.tick()
        self.assertEqual(len(self.events()), 1)
        self.assertIsNone(self.ledger.tasks.db.execute('SELECT payload FROM pipeline_state WHERE key=?',
                                                     (sent_events.CURSOR,)).fetchone())
        self.commit.side_effect = None
        self.assertEqual(self.tick()['events'], 0)
        self.assertEqual(self.commit.call_count, 2)

    def test_pause_missing_mailbox_and_exhausted_budget_defer_reads(self):
        self.config['paused'] = True
        self.assertEqual(self.tick()['status'], 'disabled')
        self.assertEqual(self.graph.calls, [])
        self.config['paused'] = False
        self.assertEqual(sent_events.reconcile(self.ledger, self.config, self.root, None)['status'], 'disabled')
        Tick(seconds=0).bind(self.ledger)
        self.assertEqual(self.tick()['status'], 'deferred')
        self.assertFalse(any('/sentitems/' in path for path in self.graph.calls))

    def test_failure_logs_status_without_mail_content(self):
        log = io.StringIO()
        with patch.object(self.box, 'sent_events_page', side_effect=RuntimeError('PRIVATE BODY')), redirect_stderr(log):
            report = sent_events.advance(self.ledger, self.config, self.root, self.box)
        self.assertEqual(report, {'status': 'error', 'error': 'RuntimeError'})
        self.assertNotIn('PRIVATE', log.getvalue())
        self.assertIn('sent-events', log.getvalue())


class Crash(BaseException):
    """Runner termination after the synthetic commit completed."""


class SentEventsRecoveryTests(SentEventsFixture):
    def state(self, key):
        row = self.ledger.tasks.db.execute('SELECT payload FROM pipeline_state WHERE key=?', (key,)).fetchone()
        return json.loads(row[0]) if row else None

    def restore(self):
        folder = Path(tempfile.mkdtemp(dir=self.root))
        self.ledger = control.Ledger(self.ledger.releases, folder)
        self.addCleanup(self.ledger.tasks.close)

    def test_two_pages_without_new_events_upload_no_forced_snapshots(self):
        # Seed a committed event, then replay it across two clean pages. Pin the
        # clock inside the normal debounce interval and count actual uploads.
        self.tick()
        self.ledger.clock = lambda: self.ledger.last_saved_at
        self.needs_push.return_value = False
        self.set_messages([], pages=[{'value': [message()], '@odata.nextLink': NEXT},
                                    {'value': [message()]}])
        self.commit.reset_mock()
        with patch.object(self.ledger, 'save', wraps=self.ledger.save) as save, \
                patch.object(self.ledger.releases, 'upload', wraps=self.ledger.releases.upload) as upload:
            self.assertEqual(self.tick(), {'status': 'idle', 'events': 0, 'pages': 2})
        self.assertEqual(save.call_args_list, [unittest.mock.call(), unittest.mock.call()])
        upload.assert_not_called()
        self.commit.assert_not_called()

    def test_new_events_force_saved_intent_before_commit(self):
        self.needs_push.return_value = False
        seen = []
        def commit(root):
            snapshot = self.ledger.releases.body['snapshot']['name']
            path = self.root / 'commit-intent.sqlite3'
            path.write_bytes(self.ledger.releases.assets[snapshot])
            with sqlite3.connect(path) as db:
                row = db.execute("SELECT payload FROM pipeline_state WHERE key='sent-events-pending'").fetchone()
                seen.append(json.loads(row[0]))
        self.commit.side_effect = commit
        self.assertEqual(self.tick()['events'], 1)
        self.assertEqual(seen[0]['cursor'], {'since': SINCE, 'until': '2026-11-05T00:00:00Z'})
        self.assertEqual(seen[0]['contacts_digest'], digest(registry.read(self.root, 'contacts')))
        self.assertIsNone(self.state('sent-events-pending'))

    def test_dirty_tables_or_unpushed_commits_force_intent_without_new_events(self):
        self.set_messages([])
        def commit(root):
            self.assertIsNotNone(self.state('sent-events-pending'))
            restored = control.Ledger(self.ledger.releases, Path(tempfile.mkdtemp(dir=self.root)))
            self.addCleanup(restored.tasks.close)
            self.assertIsNotNone(restored.tasks.db.execute(
                "SELECT payload FROM pipeline_state WHERE key='sent-events-pending'").fetchone())
        self.commit.side_effect = commit
        self.assertEqual(self.tick()['events'], 0)
        self.commit.assert_called_once()

    def test_crash_after_commit_recovers_without_duplicates_and_freezes_until(self):
        self.commit.side_effect = Crash()
        with self.assertRaises(Crash): self.tick()
        self.assertEqual(len(self.events()), 1)
        first_page = self.graph.calls[-1]
        self.restore()
        self.assertIsNotNone(self.state('sent-events-pending'))
        self.assertIsNone(self.state(sent_events.CURSOR))
        self.commit.side_effect = None
        self.needs_push.return_value = False  # the synthetic push already succeeded
        report = sent_events.reconcile(self.ledger, self.config, self.root, self.box, NOW + timedelta(hours=2))
        self.assertEqual(report, {'status': 'idle', 'events': 0, 'pages': 1})
        self.assertEqual(self.graph.calls[-1], first_page)
        self.assertEqual(len(self.events()), 1)
        self.assertEqual(self.state(sent_events.CURSOR),
                         {'since': SINCE, 'until': '2026-11-05T00:00:00Z', 'next_link': None})
        self.assertIsNone(self.state('sent-events-pending'))
        self.ledger.save(force=True)
        self.restore()
        self.assertIsNone(self.state('sent-events-pending'))
        self.assertIn('next_link', self.state(sent_events.CURSOR))

    def test_crash_after_second_commit_resumes_saved_page_handle(self):
        self.set_messages([], pages=[{'value': [message()], '@odata.nextLink': NEXT},
                                    {'value': [message('two')]}])
        self.commit.side_effect = [None, Crash()]
        with self.assertRaises(Crash): self.tick()
        self.restore()
        self.assertEqual(self.state('sent-events-pending')['cursor']['next_link'], NEXT)
        self.assertEqual(self.state(sent_events.CURSOR)['next_link'], NEXT)
        start = len(self.graph.calls)
        self.commit.side_effect = None
        self.needs_push.return_value = False
        report = sent_events.reconcile(self.ledger, self.config, self.root, self.box, NOW + timedelta(minutes=5))
        self.assertEqual(report['events'], 0)
        self.assertEqual([path for path in self.graph.calls[start:] if not path.startswith('/me?$select=')],
                         [NEXT.removeprefix(mailbox.Graph.BASE)])
        self.assertEqual(len(self.events()), 2)
        self.assertIsNone(self.state('sent-events-pending'))
        self.assertIsNone(self.state(sent_events.CURSOR)['next_link'])

    def test_since_change_deletes_stale_intent_before_failed_or_deferred_page(self):
        self.commit.side_effect = RuntimeError('synthetic push failure')
        with self.assertRaises(RuntimeError): self.tick()
        new_since = '2026-11-02T00:00:00Z'
        for deferred in (False, True):
            with self.subTest(deferred=deferred):
                self.restore()
                self.box.settings['live_since'] = new_since
                self.assertIsNotNone(self.state('sent-events-pending'))
                if deferred:
                    Tick(seconds=0).bind(self.ledger)
                    self.assertEqual(self.tick()['status'], 'deferred')
                else:
                    def fail(since, until, link):
                        self.assertEqual(since, new_since)
                        self.assertEqual(until, '2026-11-05T00:00:00Z')
                        self.assertIsNone(link)
                        self.assertIsNone(self.state('sent-events-pending'))
                        raise RuntimeError('synthetic page failure')
                    with patch.object(self.box, 'sent_events_page', side_effect=fail), self.assertRaises(RuntimeError):
                        self.tick()
                self.assertIsNone(self.state('sent-events-pending'))

        del self.ledger.tick
        self.commit.side_effect = None
        self.needs_push.return_value = False
        self.assertEqual(self.tick()['pages'], 1)
        self.assertEqual(self.state(sent_events.CURSOR)['since'], new_since)
        self.assertIsNone(self.state('sent-events-pending'))

    def test_forced_save_failure_prevents_commit_and_isolates_stage(self):
        self.ledger.releases.fail_pointer = True
        log = io.StringIO()
        with redirect_stderr(log):
            report = sent_events.advance(self.ledger, self.config, self.root, self.box)
        self.assertEqual(report, {'status': 'error', 'error': 'TimeoutError'})
        self.commit.assert_not_called()
        self.assertEqual(self.state('sent-events-status'), report)
        self.assertIsNotNone(self.state('sent-events-pending'))
        self.assertIsNone(self.state(sent_events.CURSOR))
        # A later tick stage can still read/write the same ledger connection.
        self.ledger.tasks.db.execute('INSERT INTO pipeline_state VALUES(?,?)', ('later-stage', '{}'))
        self.assertEqual(self.state('later-stage'), {})
        self.assertNotIn('Lost pointer write', log.getvalue())


class CommitTablesTests(unittest.TestCase):
    def test_tables_need_push_checks_scoped_status_and_unpushed_local_commits(self):
        from managed.location import DATA_BRANCH
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            def git(*args):
                return subprocess.check_output(['git', *args], cwd=root, stderr=subprocess.PIPE)
            git('init', '-b', DATA_BRANCH)
            git('config', 'user.name', 'Fixture')
            git('config', 'user.email', 'fixture@example.invalid')
            (root / 'tables').mkdir()
            for name in (*sent_events.FILES, 'unrelated.txt'): (root / name).write_text('before\n')
            git('add', '.'); git('commit', '-m', 'initial')
            remote = f'refs/remotes/origin/{DATA_BRANCH}'
            git('update-ref', remote, 'HEAD')
            (root / 'unrelated.txt').write_text('unrelated\n')
            git('add', 'unrelated.txt')
            self.assertFalse(sent_events.tables_need_push(root))
            (root / sent_events.FILES[0]).write_text('changed\n')
            self.assertTrue(sent_events.tables_need_push(root))
            git('add', sent_events.FILES[0])
            self.assertTrue(sent_events.tables_need_push(root))
            git('commit', '-m', 'local commit awaiting push')
            self.assertEqual(git('status', '--porcelain'), b'')
            self.assertTrue(sent_events.tables_need_push(root))
            git('update-ref', remote, 'HEAD')
            self.assertFalse(sent_events.tables_need_push(root))
            git('rm', sent_events.FILES[0]); git('commit', '-m', 'remove table fixture')
            git('update-ref', remote, 'HEAD')
            (root / sent_events.FILES[0]).write_text('untracked\n')
            self.assertTrue(sent_events.tables_need_push(root))

    def test_commit_and_push_only_scoped_files_preserves_unrelated_staged_change(self):
        with tempfile.TemporaryDirectory() as folder, patch.dict('os.environ', {'GITHUB_ACTIONS': 'true'}):
            root = Path(folder) / 'repo'
            root.mkdir()
            remote = Path(folder) / 'remote.git'
            def git(*args):
                return subprocess.check_output(['git', *args], cwd=root, text=True, stderr=subprocess.PIPE)
            subprocess.run(['git', 'init', '--bare', str(remote)], check=True, capture_output=True)
            git('init')
            git('config', 'user.name', 'Fixture')
            git('config', 'user.email', 'fixture@example.invalid')
            git('remote', 'add', 'origin', str(remote))
            (root / 'tables').mkdir()
            for name in (*sent_events.FILES, 'unrelated.txt'):
                (root / name).write_text('before\n')
            git('add', '.')
            git('commit', '-m', 'initial')
            for name in (*sent_events.FILES, 'unrelated.txt'):
                (root / name).write_text('after\n')
            git('add', 'unrelated.txt')
            sent_events.commit_tables(root)
            self.assertEqual(set(git('diff-tree', '--no-commit-id', '--name-only', '-r', 'HEAD').splitlines()),
                             set(sent_events.FILES))
            self.assertEqual(git('diff', '--cached', '--name-only').strip(), 'unrelated.txt')
            self.assertEqual(git('rev-parse', 'HEAD'), git('rev-parse', 'refs/remotes/origin/main'))
            # Retry after a lost push response / no remaining working changes.
            sent_events.commit_tables(root)
            self.assertEqual(git('config', 'user.email').strip(), 'fixture@example.invalid')


    def test_commit_tables_rebases_once_when_main_moved(self):
        with tempfile.TemporaryDirectory() as folder, patch.dict(os.environ, {'GITHUB_ACTIONS': 'true'}):
            root = Path(folder) / 'work'; other = Path(folder) / 'other'; root.mkdir()
            remote = Path(folder) / 'remote.git'
            def git(cwd, *args):
                return subprocess.check_output(['git', *args], cwd=cwd, text=True, stderr=subprocess.PIPE)
            subprocess.run(['git', 'init', '--bare', '-b', 'main', str(remote)], check=True, capture_output=True)
            git(root, 'init', '-b', 'main')
            for repo in (root,):
                git(repo, 'config', 'user.name', 'Fixture'); git(repo, 'config', 'user.email', 'fixture@example.invalid')
            git(root, 'remote', 'add', 'origin', str(remote))
            (root / 'tables').mkdir()
            for name in sent_events.FILES:
                (root / name).write_text('before\n')
            (root / 'other.txt').write_text('one\n')
            git(root, 'add', '.'); git(root, 'commit', '-m', 'initial'); git(root, 'push', 'origin', 'HEAD:main')
            subprocess.run(['git', 'clone', '-q', str(remote), str(other)], check=True)
            git(other, 'config', 'user.name', 'Other'); git(other, 'config', 'user.email', 'other@example.invalid')
            (other / 'other.txt').write_text('two\n'); git(other, 'commit', '-qam', 'operator'); git(other, 'push', '-q', 'origin', 'HEAD:main')
            for name in sent_events.FILES:
                (root / name).write_text('after\n')
            sent_events.commit_tables(root)
            log = git(root, 'log', '--format=%s', 'origin/main').splitlines()
            self.assertEqual(log[:2], ['Record actual Sent Items contact events', 'operator'])


if __name__ == '__main__':
    unittest.main()
