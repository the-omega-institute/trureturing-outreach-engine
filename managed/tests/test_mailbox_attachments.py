"""Attachment selection failures isolate one thread without losing Inbox work."""
import base64
import copy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock
from urllib.parse import parse_qs, urlsplit

from managed import automation, mailbox
from managed.control import Ledger, Releases as GitHubReleases
from managed.queue import GateError, digest
from managed.tick import Tick
from test_tasks import Releases, use_fixture_workflow_revisions


def addr(value):
    return {'emailAddress': {'address': value}}


def message(identity, conversation='blocked'):
    return {'id': identity, 'internetMessageId': '<' + identity + '>',
            'conversationId': conversation, 'receivedDateTime': '2026-11-18T09:00:00Z',
            'from': addr('author@example.invalid'), 'toRecipients': [addr('sender@example.invalid')],
            'subject': 'Question', 'body': {'content': 'private message body'}, 'isDraft': False}


class AttachmentReleases(Releases, GitHubReleases):
    def prune_snapshots(self,keep,protect=()):
        # Control snapshots use the in-memory adapter; run() below models only
        # the separate task-attachment Releases API.
        return 0

    def __init__(self):
        super().__init__()
        self.repo = 'fixture/repo'
        self.tags = set()
        self.files = {}
        self.calls = []
        self.digestless = False
        self.corrupt = False

    def run(self, *args, missing=False):
        if args[0] == 'api':
            tag = args[1].rsplit('/', 1)[1]
            self.calls.append(('list' if args[-1] == '.assets' else 'lookup', tag))
            if tag not in self.tags:
                return None
            if args[-1] == '.id':
                return b'1'
            return json.dumps([
                {'name': name, 'size': len(raw),
                 'digest': None if self.digestless else 'sha256:' + hashlib.sha256(raw).hexdigest()}
                for (release, name), raw in self.files.items() if release == tag]).encode()
        _, operation, tag, *_ = args
        self.calls.append((operation, tag))
        if operation == 'create':
            self.tags.add(tag)
        elif operation == 'upload':
            for file in args[3:args.index('--repo')]:
                path = Path(file)
                self.files[tag, path.name] = path.read_bytes()
        elif operation == 'download':
            name = args[args.index('--pattern') + 1]
            raw = self.files[tag, name]
            (Path(args[args.index('--dir') + 1]) / name).write_bytes(b'X' * len(raw) if self.corrupt else raw)
        else:
            raise AssertionError(args)


class AttachmentGateTests(unittest.TestCase):
    def setUp(self):
        self.graph = Mock()
        self.box = mailbox.GraphMailbox(self.graph, {})
        self.box.thread = Mock(return_value=[{**message('old'), 'hasAttachments': True},
                                             {**message('new'), 'hasAttachments': True}])

    def test_unsupported_types_report_metadata_from_every_message_without_content(self):
        attachments = [
            {'@odata.type': '#microsoft.graph.itemAttachment', 'name': 'forwarded.eml',
             'size': 120, 'item': {'body': 'private attached item'}},
            {'@odata.type': '#microsoft.graph.referenceAttachment', 'name': 'shared.pdf',
             'size': 240, 'sourceUrl': 'private reference URL'}]
        self.graph.pages.side_effect = [[a] for a in attachments]
        with self.assertRaises(mailbox.UnsupportedThreadAttachments) as raised:
            self.box.research_thread('blocked')
        self.assertIsInstance(raised.exception, GateError)
        self.assertEqual(raised.exception.attachments, [
            {'message_id': identity, 'name': a['name'], 'type': a['@odata.type'], 'size': a['size']}
            for identity, a in zip(('old', 'new'), attachments)])
        self.assertNotIn('private', str(raised.exception))
        self.assertIn('explicit supported input selection', str(raised.exception))
        self.graph.request.assert_not_called()

    def test_file_limit_uses_metadata_and_accepts_exactly_30_mib(self):
        self.box.thread.return_value = [{**message('file'), 'hasAttachments': True}]
        for size in (mailbox.MAX_ATTACHMENT_BYTES, mailbox.MAX_ATTACHMENT_BYTES + 1):
            with self.subTest(size=size):
                attachment = {'id': 'file-attachment', '@odata.type': mailbox.FILE_ATTACHMENT,
                              'name': 'large.pdf', 'size': size, 'contentType': 'application/pdf'}
                self.graph.pages.return_value = [attachment]
                if size == mailbox.MAX_ATTACHMENT_BYTES:
                    rows = self.box.research_thread('blocked')
                    self.assertEqual(rows[0]['attachments'], [attachment])
                else:
                    with self.assertRaises(mailbox.UnsupportedThreadAttachments) as raised:
                        self.box.research_thread('blocked')
                    self.assertEqual(raised.exception.attachments, [{'message_id': 'file',
                                     'name': 'large.pdf', 'type': mailbox.FILE_ATTACHMENT, 'size': size}])
                    self.assertNotIn('contentBytes', str(raised.exception))
                query = parse_qs(urlsplit(self.graph.pages.call_args.args[0]).query)
                self.assertEqual(query['$select'], ['id,name,size,contentType'])
        self.graph.request.assert_not_called()


class AttachmentIntakeTests(unittest.TestCase):
    def setUp(self):
        use_fixture_workflow_revisions(self)
        self.tmp = tempfile.TemporaryDirectory(); self.root = Path(self.tmp.name)
        (self.root / 'tables').mkdir(); (self.root / 'ledger').mkdir()
        for name, value in {
            'contacts': {'contacts': [{'id': 'contact-a', 'name': 'Professor Example',
                                       'email': 'author@example.invalid'}]},
            'refusals': {'refusals': []}, 'submissions': {'papers': []}}.items():
            (self.root / 'tables' / (name + '.json')).write_text(json.dumps(value))
        self.ledger = Ledger(Releases(), self.root / 'ledger')
        self.time = 100_000
        self.ledger.tasks.clock = lambda: self.time
        self.cfg = {'paused': False, 'kinds': {'reply': True},
                    'automation': {'max_new_tasks_per_tick': 4}}
        self.box = mailbox.GraphMailbox(Mock(), {'reconciled': True,
                                       'live_since': '2026-11-18T00:00:00Z'})
        self.box.verify_owner = Mock(return_value='sender@example.invalid')
        self.blocked = [message('blocked-1'), message('blocked-2')]
        self.good = message('good', 'good-thread')
        sent = {**self.good, 'id': 'sent', 'internetMessageId': '<sent>',
                'from': addr('sender@example.invalid'), 'toRecipients': [addr('author@example.invalid')],
                'sentDateTime': '2026-11-17T09:00:00Z'}
        self.threads = {'blocked': [{**self.blocked[0], 'hasAttachments': True}, self.blocked[1]],
                        'good-thread': [sent, self.good]}
        self.box.thread = Mock(side_effect=lambda c: copy.deepcopy(self.threads[c]))
        self.box.graph.pages.return_value = [
            {'@odata.type': '#microsoft.graph.itemAttachment', 'name': 'forwarded.eml',
             'size': 321, 'item': {'body': 'private attachment body'}}]
        self.box.messages = Mock(return_value=[*self.blocked, self.good])

    def tearDown(self):
        self.ledger.tasks.close(); self.tmp.cleanup()

    def tick(self):
        mailbox.intake(self.ledger, self.cfg, self.root, self.box, revision='b' * 40,
                       now=datetime(2026, 11, 18, 10, tzinfo=timezone.utc))

    def retained(self):
        return automation.work(self.ledger.tasks, self.root)['mailbox_needs_classification']

    def test_retain_each_message_continue_and_checkpoint_without_attachment_content(self):
        self.tick()
        retained = self.retained()
        self.assertEqual([m['internetMessageId'] for m in retained], ['<blocked-1>', '<blocked-2>'])
        for row in retained:
            self.assertEqual(row['intake_reason'], mailbox.UNSUPPORTED_ATTACHMENTS)
            self.assertEqual(row['checked_at'], self.time)
            self.assertEqual(row['attachment_issues'], [{'message_id': 'blocked-1',
                             'name': 'forwarded.eml', 'type': '#microsoft.graph.itemAttachment', 'size': 321}])
            self.assertNotIn('body', row)
            self.assertNotIn('attachments', row)
        self.assertNotIn('private', json.dumps(retained))
        tasks = self.ledger.tasks.all()
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0]['request']['inputs']['thread_id'], 'good-thread')
        state = json.loads(self.ledger.tasks.db.execute(
            "SELECT payload FROM pipeline_state WHERE key='mailbox'").fetchone()[0])
        self.assertEqual(state['through'], '2026-11-18T10:00:00Z')
        self.assertEqual(state['unmapped'], retained)
        self.assertEqual(self.box.thread.call_count, 2)

    def test_cooldown_survives_replay_new_message_and_checks_again_when_due(self):
        self.tick()
        self.box.thread.reset_mock(); self.box.graph.pages.reset_mock()
        newer = message('blocked-3')
        # Inclusive cursor replay plus a new message in the blocked conversation.
        self.box.messages.return_value = [*self.blocked, newer]
        self.tick()
        self.box.messages.return_value = []
        self.tick()
        self.box.thread.assert_not_called(); self.box.graph.pages.assert_not_called()
        self.assertEqual(len(self.retained()), 3)
        self.time += mailbox.UNMAPPED_RECHECK_SECONDS
        self.tick()
        self.assertEqual(self.box.thread.call_count, 1)
        self.assertEqual(self.box.graph.pages.call_count, 1)
        self.assertTrue(all(m['checked_at'] == self.time for m in self.retained()))

    def test_recheck_clears_retention_when_thread_becomes_supported(self):
        self.tick()
        self.box.messages.return_value = []
        # The operator has removed the unsupported attachment.
        self.threads['blocked'][0]['hasAttachments'] = False
        self.threads['blocked'].insert(0, {**self.threads['good-thread'][0], 'conversationId': 'blocked'})
        self.time += mailbox.UNMAPPED_RECHECK_SECONDS
        self.tick()
        self.assertEqual(self.retained(), [])
        self.assertEqual(len(self.ledger.tasks.all()), 2)

    def test_task_budget_deferral_preserves_attachment_diagnostics(self):
        self.tick()
        self.box.messages.return_value = []
        self.time += mailbox.UNMAPPED_RECHECK_SECONDS
        self.cfg['automation']['max_new_tasks_per_tick'] = 0
        self.box.thread.reset_mock()
        retained = self.retained()
        self.tick()
        self.box.thread.assert_not_called()
        self.assertEqual(self.retained(), retained)

    def test_other_gate_errors_still_propagate(self):
        self.box.thread.side_effect = GateError('Unrelated mailbox boundary failure')
        with self.assertRaisesRegex(GateError, 'Unrelated mailbox boundary failure'):
            self.tick()
        self.assertEqual(self.ledger.tasks.all(), [])

    def prepare_files(self, *payloads):
        remote = AttachmentReleases()
        self.ledger.releases = remote
        attachments = {f'file-{i}': raw for i, raw in enumerate(payloads)}
        self.threads['blocked'].insert(0, {**self.threads['good-thread'][0], 'conversationId': 'blocked'})
        self.box.messages.return_value = [self.blocked[0]]
        self.box.graph.pages.return_value = [
            {'id': name, 'name': name + '.pdf', 'size': len(raw), '@odata.type': mailbox.FILE_ATTACHMENT}
            for name, raw in attachments.items()]
        self.box.graph.request.side_effect = lambda path: {
            'contentBytes': base64.b64encode(attachments[path.rsplit('/', 1)[1]]).decode()}
        return remote

    def selected(self, inbound='<blocked-1>'):
        inputs = next(t['request']['inputs'] for t in self.ledger.tasks.all()
                      if t['request']['inputs']['latest_inbound_message_id'] == inbound)
        attachments = [a for m in inputs['thread'] for a in m.get('attachments', [])]
        self.assertTrue(all('contentBytes' not in a for a in attachments))
        return [a['selected_input'] for a in attachments]

    def newer_inbound(self):
        newer = {**message('newer'), 'receivedDateTime': '2026-11-18T09:30:00Z'}
        self.threads['blocked'].append(newer)
        self.box.messages.return_value = [newer]
        self.box.graph.request.reset_mock()

    def test_batch_preserves_distinct_bytes_with_one_upload_and_one_listing(self):
        remote = self.prepare_files(b'a', b'b', b'c', b'a')
        self.tick()
        refs = self.selected()
        self.assertEqual([op for op, _ in remote.calls], ['lookup', 'create', 'upload', 'list'])
        self.assertEqual(len(remote.files), 3)
        self.assertEqual(refs[0], refs[-1])
        self.assertEqual(self.box.graph.request.call_count, 4)
        for ref, raw in zip(refs, [b'a', b'b', b'c', b'a']):
            self.assertEqual(set(ref), {'release_tag', 'name', 'bytes', 'sha256'})
            self.assertEqual(remote.files[ref['release_tag'], ref['name']], raw)

    def test_next_inbound_reuses_verified_refs_after_ledger_restore(self):
        remote = self.prepare_files(b'a', b'b')
        self.tick()
        previous = self.selected()
        self.ledger.tasks.close()
        restore = self.root / 'restore'
        restore.mkdir()
        self.ledger = Ledger(remote, restore)
        remote.calls.clear()
        self.newer_inbound()
        self.tick()
        self.assertEqual(remote.calls, [('list', previous[0]['release_tag'])])
        self.box.graph.request.assert_not_called()
        self.assertEqual(self.selected('<newer>'), previous)

    def test_next_inbound_reuses_attachment_already_staged_before_index(self):
        remote = self.prepare_files(b'a')
        self.tick()
        previous = self.selected()
        self.ledger.tasks.db.execute("DELETE FROM pipeline_state WHERE key LIKE 'mailbox-attachments:%'")
        remote.calls.clear()
        self.newer_inbound()
        self.tick()
        self.assertEqual(remote.calls, [('list', previous[0]['release_tag'])])
        self.box.graph.request.assert_not_called()
        self.assertEqual(self.selected('<newer>'), previous)
        self.assertIsNotNone(self.ledger.tasks.db.execute(
            "SELECT 1 FROM pipeline_state WHERE key LIKE 'mailbox-attachments:%'").fetchone())

    def test_missing_release_asset_is_uploaded_again(self):
        remote = self.prepare_files(b'a', b'b')
        self.tick()
        first, second = self.selected()
        del remote.files[first['release_tag'], first['name']]
        remote.calls.clear()
        self.newer_inbound()
        self.tick()
        updated, reused = self.selected('<newer>')
        self.assertEqual(reused, second)
        self.assertNotEqual(updated['release_tag'], first['release_tag'])
        self.assertEqual(remote.files[updated['release_tag'], updated['name']], b'a')
        self.box.graph.request.assert_called_once_with('/me/messages/blocked-1/attachments/file-0')
        self.assertEqual(remote.calls.count(('list', first['release_tag'])), 1)

    def test_attachment_free_conversations_run_first_with_deferred_priority_among_equals(self):
        remote = self.prepare_files(b'a')
        other = message('other', 'other-thread')
        self.threads['other-thread'] = [{**self.threads['good-thread'][0], 'conversationId': 'other-thread'}, other]
        self.box.messages.return_value = [self.blocked[0], self.good, other]
        tick = Tick(seconds=60)
        tick.bind(self.ledger)
        tick.previous['mailbox-intake'] = [digest('<blocked-1>'), digest('<other>')]
        self.cfg['automation']['max_new_tasks_per_tick'] = 2
        self.tick()
        self.assertEqual([t['request']['inputs']['thread_id'] for t in self.ledger.tasks.all()],
                         ['other-thread', 'good-thread'])
        self.assertEqual([m['internetMessageId'] for m in self.retained()], ['<blocked-1>'])
        self.assertEqual(remote.calls, [])
        self.box.graph.request.assert_not_called()

    def test_digestless_listing_falls_back_to_download(self):
        remote = self.prepare_files(b'a')
        remote.digestless = True
        self.tick()
        self.assertEqual([op for op, _ in remote.calls], ['lookup', 'create', 'upload', 'list', 'download'])
        self.assertEqual(self.selected()[0]['bytes'], 1)

    def test_bad_readback_defers_and_reports_without_adding_a_task_or_index(self):
        remote = self.prepare_files(b'a')
        remote.digestless = True
        remote.corrupt = True
        self.tick()
        self.assertEqual(self.ledger.tasks.all(), [])
        self.assertEqual(self.retained()[0]['intake_reason'], 'thread attachment preservation failed; retained')
        self.assertIn('GateError', self.retained()[0]['category_reason'])
        self.assertIsNone(self.ledger.tasks.db.execute(
            "SELECT 1 FROM pipeline_state WHERE key LIKE 'mailbox-attachments:%'").fetchone())
        remote.corrupt = False
        remote.calls.clear()
        self.tick()
        self.assertEqual([op for op, _ in remote.calls], ['lookup', 'list', 'download'])
        self.assertEqual(self.selected()[0]['bytes'], 1)


if __name__ == '__main__':
    unittest.main()
