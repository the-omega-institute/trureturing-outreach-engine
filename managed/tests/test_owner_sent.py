"""Read-only reconciliation of owner drafts sent outside the delivery queue."""
import copy
from datetime import datetime
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from managed import automation, control, delivery, draft_status, mailbox, registry, sent_events
from managed.queue import digest
from managed.tick import Tick
from test_reply_threading import DraftGraph, participant
from test_tasks import Releases, request


OBSERVED = '2026-11-03T00:30:00Z'
SENT = '2026-11-03T00:55:00Z'


class OwnerSentGraph(DraftGraph):
    """A saved owner draft and independently populated Sent Items, GET only."""
    def request(self, path, method='GET', body=None):
        if method != 'GET':
            raise AssertionError('Owner-send reconciliation wrote to Graph: ' + method)
        if path == '/me/mailFolders/drafts?$select=id':
            self.calls.append((path, method, body))
            return {'id': 'drafts-folder'}
        try:
            return super().request(path, method, body)
        except KeyError:
            error = HTTPError(path, 404, 'Not Found', {}, io.BytesIO())
            error.close()
            raise error

    def pages(self, path):
        self.calls.append((path, 'GET', None))
        return super().pages(path)


class OwnerSentTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        (self.root / 'tables').mkdir()
        (self.root / 'ledger').mkdir()
        self.remote = Releases()
        self.ledger = control.Ledger(self.remote, self.root / 'ledger')
        self.addCleanup(self.temp.cleanup)
        self.addCleanup(self.ledger.tasks.close)
        for name, value in [('contacts', {'contacts': []}), ('refusals', {'refusals': []}),
                            ('positive', {'entries': []}), ('submissions', {'papers': []})]:
            (self.root / 'tables' / (name + '.json')).write_text(json.dumps(value))
        self.config = {'paused': False, 'kinds': {'reply': True, 'firstcontact': True},
                       'mailbox': {'team_addresses': ['team@example.invalid']}}
        self.packet = json.loads((Path(__file__).parent / 'fixtures/letter.json').read_text())
        self.packet.update(kind='reply', thread_id='thread-1',
                           latest_inbound_message_id='<inbound-1>', user_decision_ever=True)
        self.packet['cc'] = ['colleague@example.invalid']
        (self.root / 'tables/contacts.json').write_text(json.dumps({'contacts': [
            {'id': 'contact-' + str(i), 'name': 'Professor Example', 'email': address}
            for i, address in enumerate(self.packet['to'] + self.packet['cc'])]}))
        self.graph = OwnerSentGraph(self.packet)
        self.box = mailbox.GraphMailbox(self.graph, {'account': self.packet['sender'], 'reconciled': True})
        self.task_id = self.add_task('owner-reply', 'draft-1')

    def add_task(self, name, draft_id):
        req = request(name, 'contact-a', 'reply')
        req['inputs'] = {'mailbox_observation': {
            'at': OBSERVED, 'email': self.packet['to'][0], 'message_id': '<inbound-1>',
            'evidence_ref': 'graph:inbox:inbound-1', 'subject': 'Synthetic incoming request'},
            'user_decision_ever': True}
        row = self.ledger.tasks.add(req)
        result = {'schema_version': 1, 'task_id': name, 'outcome': 'completed',
                  'report': 'Synthetic checked reply.', 'artifacts': [],
                  'packet': copy.deepcopy(self.packet),
                  'routing': {'mail': 'owner', 'questions': [], 'independent_submission': None}}
        row = self.ledger.tasks.update(name, row['revision'], 'awaiting-review', {'result': result})
        review = control.sign_review({
            'binding': self.ledger.tasks.review_binding(row), 'reviewer': 'local-claude-loop',
            'verdict': 'PASS', 'reason': 'Synthetic independent review.',
            'evidence_ref': 'fixture:review'}, 'owner-sent-secret')
        row = self.ledger.tasks.verdict(review, 'owner-sent-secret')
        receipt = {'remote_id': draft_id, 'attempt_key': 'attempt-' + name,
                   'content_hash': digest(self.packet), 'evidence_ref': 'graph:draft:' + draft_id}
        self.ledger.tasks.update(name, row['revision'], 'owner-draft',
                                 {**row['data'], 'draft_receipt': receipt, 'sent_receipt': None})
        self.graph.drafts[draft_id] = {**self.graph.message(draft_id, receipt['attempt_key']),
                                       'parentFolderId': 'drafts-folder'}
        return name

    def row(self):
        return self.ledger.tasks.get(self.task_id)

    def update(self, **data):
        row = self.row()
        return self.ledger.tasks.update(row['id'], row['revision'], row['state'], {**row['data'], **data})

    def outlook_send(self, sent_id='sent-1', draft_id='draft-1', **changes):
        message = self.graph.drafts.pop(draft_id)
        message.update(id=sent_id, internetMessageId='<' + sent_id + '@example.invalid>',
                       isDraft=False, parentFolderId='sent-folder', sentDateTime=SENT,
                       internetMessageHeaders=[])
        message.update(changes)
        self.graph.sent[sent_id] = message
        return message

    def tick(self, config=None):
        delivery.advance(self.ledger, config or self.config, self.root, self.box)
        self.assertTrue(all(method == 'GET' for _, method, _ in self.graph.calls))
        self.assertEqual(self.graph.mimes, [])
        self.assertEqual(self.ledger.tasks.db.execute('SELECT COUNT(*) FROM attempts').fetchone()[0], 0)

    def test_todo_draft_observations_use_one_listing_and_strictly_later_sent(self):
        saved = '2026-11-03T00:40:00Z'
        self.update(draft_receipt={**self.row()['data']['draft_receipt'], 'at': saved})
        sent_task = self.add_task('other-owner-draft', 'draft-2')
        missing_task = self.add_task('missing-owner-draft', 'draft-3')
        same_time_task = self.add_task('same-time-owner-draft', 'draft-4')
        for task, conversation in [(sent_task, 'other-thread'), (missing_task, 'missing-thread'),
                                   (same_time_task, 'same-time-thread')]:
            row = self.ledger.tasks.get(task)
            receipt = {**row['data']['draft_receipt'], 'at': saved, 'conversationId': conversation}
            self.ledger.tasks.update(task, row['revision'], row['state'], {**row['data'], 'draft_receipt': receipt})
            del self.graph.drafts[receipt['remote_id']]
        draft = self.graph.drafts['draft-1']
        draft.update(lastModifiedDateTime=SENT, attachments=[{'id': 'a'}, {'id': 'b', 'isInline': True}])
        draft['body'] = {'contentType': 'text', 'content': 'Updated by Avery.\n【请填写日期】'}
        sent = [{'id': 'sent-2', 'conversationId': 'other-thread', 'sentDateTime': SENT},
                {'id': 'sent-3', 'conversationId': 'different-thread', 'sentDateTime': SENT},
                {'id': 'sent-4', 'conversationId': 'same-time-thread', 'sentDateTime': saved}]
        checked = datetime.fromisoformat('2026-11-03T01:00:00+00:00')
        self.box.settings['live_since'] = OBSERVED
        observed = []
        with patch.object(self.box, 'sent_events_page', return_value=(sent, None)) as sent_page, \
                patch.object(sent_events, 'commit_tables'), \
                patch.object(self.box, 'reply_drafts', wraps=self.box.reply_drafts) as listing:
            sent_events.reconcile(self.ledger, self.config, self.root, self.box, now=checked, observe=observed.extend)
            self.assertIsNone(draft_status.refresh(self.ledger, self.config, self.box, observed, now=checked))
        sent_page.assert_called_once()
        listing.assert_called_once_with()
        def work():
            return {r['task_id']: r for r in self.inventory()['owner_drafts']}
        rows = work()
        self.assertEqual({task: r['draft_status'] for task, r in rows.items()},
                         {self.task_id: 'present', sent_task: 'sent', missing_task: 'missing', same_time_task: 'missing'})
        present = rows[self.task_id]
        self.assertEqual(present['draft_checked_at'], checked.isoformat())
        self.assertEqual(present['lastModifiedDateTime'], SENT)
        self.assertEqual(present['attachment_count'], 1)
        self.assertTrue(present['owner_edited'])
        self.assertTrue(present['has_placeholder'])
        self.assertEqual(self.row()['state'], 'owner-draft')
        self.assertEqual(self.ledger.tasks.get(sent_task)['state'], 'owner-draft')
        self.assertNotIn('Updated by Avery', json.dumps(draft_status.inventory(self.ledger.tasks)))

        with patch.object(self.box, 'reply_drafts', side_effect=TimeoutError('PRIVATE')):
            self.assertEqual(draft_status.refresh(self.ledger, self.config, self.box, []), 'TimeoutError')
        rows = work()
        self.assertTrue(all(row['draft_stale'] for row in rows.values()))
        self.assertEqual(rows[self.task_id]['draft_status'], 'present')
        self.assertEqual(rows[self.task_id]['draft_checked_at'], checked.isoformat())

        draft['body']['content'] = self.packet['body'].replace('\n', '\r\n') + '\r\n'
        self.box.begin_history_tick()
        draft_status.refresh(self.ledger, self.config, self.box, [])
        rows = work()
        self.assertFalse(rows[self.task_id]['owner_edited'])
        self.assertFalse(rows[self.task_id]['draft_stale'])
        self.assertEqual(rows[sent_task]['draft_status'], 'sent')  # Page cursor has moved on.

    def test_detects_Chinese_and_bracketed_placeholders(self):
        for body in ['请删除占位文字', 'Please fill this in: TODO', 'Name: XXX', '[占位草稿：待补]',
                     '\n [placeholder: fill in time]', '\n【补充会议链接】']:
            with self.subTest(body=body):
                self.assertTrue(draft_status.PLACEHOLDER.search(body))
        for body in ['TO DO next week', 'An ordinary [reference] in this sentence', 'prefix 【a citation】']:
            with self.subTest(body=body):
                self.assertIsNone(draft_status.PLACEHOLDER.search(body))

    def test_partial_drafts_page_is_stale_after_tick_exhaustion(self):
        draft_status.refresh(self.ledger, self.config, self.box, [])
        clock = [0]
        Tick(seconds=5, clock=lambda: clock[0]).bind(self.ledger)
        graph = mailbox.Graph('offline-test')
        box = mailbox.GraphMailbox(graph, self.box.settings)
        def page(path):
            clock[0] = 5
            return {'value': [{'id': 'unrelated'}],
                    '@odata.nextLink': graph.BASE + '/me/mailFolders/drafts/messages?$skip=1'}
        with patch.object(box, 'verify_owner', return_value=self.packet['sender']), \
                patch.object(graph, 'request', side_effect=page) as read:
            self.assertIsNone(draft_status.refresh(self.ledger, self.config, box, []))
        read.assert_called_once()
        row, = self.inventory()['owner_drafts']
        self.assertEqual(row['draft_status'], 'present')
        self.assertTrue(row['draft_stale'])

    def assert_sent(self, message, packet=None):
        row = self.row()
        self.assertEqual(row['state'], 'verified')
        self.assertEqual(row['data']['sent_receipt'], {
            'idempotency_key': 'attempt-' + self.task_id, 'content_hash': digest(packet or self.packet),
            'remote_id': message['id'], 'evidence_ref': 'graph:sent:' + message['internetMessageId'],
            'at': message['sentDateTime'], 'sent_by': 'owner-outlook'})
        self.assertNotIn('owner_draft_status', row['data'])
        self.assertNotIn('owner_draft_note', row['data'])

    def assert_missing(self):
        row = self.row()
        self.assertEqual(row['state'], 'owner-draft')
        self.assertIsNone(row['data']['sent_receipt'])
        self.assertEqual(row['data']['owner_draft_status'], 'draft-missing-not-sent')

    def inventory(self):
        with patch('managed.daily_local.status', return_value={}):
            return automation.work(self.ledger.tasks, self.root)

    def test_sent_from_outlook_matches_conversation_and_clears_owner_inventory(self):
        message = self.outlook_send(subject='Subject edited in Outlook')
        self.tick()
        self.assert_sent(message)
        self.assertEqual(self.inventory()['owner_drafts'], [])
        before = self.row()
        calls = list(self.graph.calls)
        self.tick()
        self.assertEqual(self.row(), before)
        self.assertEqual(self.graph.calls, calls)

    def test_edited_owner_send_records_only_changed_sentences_and_projects_recent_edits(self):
        receipt = self.row()['data']['draft_receipt']
        self.update(draft_receipt={**receipt, 'saved_at': OBSERVED, 'conversationId': 'saved-thread'})
        original = 'This synthetic fixture checks that 2 + 3 = 5.'
        revision = 'We checked the calculation in the attached note.'
        html = self.packet['body'].replace(original, revision).replace('\n', '<br>')
        message = self.outlook_send(conversationId='saved-thread', uniqueBody={
            'contentType': 'html', 'content': html + '<blockquote>PRIVATE QUOTE.</blockquote>'})
        self.tick()
        self.assert_sent(message)
        pairs = [{'before': original, 'after': revision}]
        self.assertEqual(self.row()['data']['owner_edit'], {'sent_at': SENT, 'changed': pairs})
        before = self.row()
        calls = list(self.graph.calls)
        self.tick()
        self.assertEqual(self.row(), before)
        self.assertEqual(self.graph.calls, calls)
        now = datetime.fromisoformat('2026-11-17T00:55:00+00:00').timestamp()
        with patch.object(self.ledger.tasks, 'clock', return_value=now):
            self.assertEqual(self.inventory()['owner_edits'], [
                {'task_id': self.task_id, 'correspondent': 'Professor Example',
                 'sent_at': SENT, 'pairs': pairs}])
        with patch.object(self.ledger.tasks, 'clock', return_value=now + 1):
            self.assertEqual(self.inventory()['owner_edits'], [])
        self.assertEqual(self.row(), before)

    def test_unchanged_owner_send_ignores_whitespace_and_quoted_history(self):
        self.update(draft_receipt={**self.row()['data']['draft_receipt'], 'saved_at': OBSERVED})
        body = self.packet['body'].replace(' ', '  ').replace('\n', '\r\n')
        message = self.outlook_send(body={'contentType': 'text', 'content':
            body + '\r\nFrom: A prior correspondent\r\nSubject: Earlier mail\r\nPRIVATE QUOTE.'})
        self.tick()
        self.assert_sent(message)
        self.assertNotIn('owner_edit', self.row()['data'])

    def test_owner_edit_excludes_different_conversation_and_unbound_packet(self):
        receipt = self.row()['data']['draft_receipt']
        self.update(draft_receipt={**receipt, 'saved_at': OBSERVED, 'conversationId': 'saved-thread'})
        message = self.outlook_send(body={'contentType': 'text', 'content': 'Edited text.'})
        self.tick()
        self.assert_sent(message)  # The packet conversation still supports a delivery receipt.
        self.assertNotIn('owner_edit', self.row()['data'])
        other = self.add_task('unbound-packet', 'draft-2')
        row = self.ledger.tasks.get(other)
        self.ledger.tasks.update(other, row['revision'], row['state'], {
            **row['data'], 'draft_receipt': {**row['data']['draft_receipt'], 'saved_at': SENT},
            'packet_override': {**self.packet, 'body': 'A version never saved to Drafts.'}})
        self.outlook_send(sent_id='sent-2', draft_id='draft-2', sentDateTime='2026-11-03T01:00:00Z',
                          body={'contentType': 'text', 'content': 'Edited text.'})
        self.box.sent = None
        self.tick()
        row = self.ledger.tasks.get(other)
        self.assertEqual(row['state'], 'verified')
        self.assertNotIn('owner_edit', row['data'])

    def test_owner_edit_caps_pair_count_and_each_side(self):
        before = [f'Old sentence {i} ' + 'a' * 650 + '.' for i in range(20)]
        after = [f'New sentence {i} ' + 'b' * 650 + '.' for i in range(20)]
        packet = {**self.packet, 'body': ' '.join(before)}
        self.update(packet_override=packet, draft_receipt={**self.row()['data']['draft_receipt'],
                                                          'saved_at': OBSERVED, 'content_hash': digest(packet)})
        message = self.outlook_send(body={'contentType': 'text', 'content': ' '.join(after)})
        self.tick()
        self.assert_sent(message, packet)
        self.assertEqual(self.row()['data']['owner_edit']['changed'],
                         [{'before': old[:600], 'after': new[:600]}
                          for old, new in zip(before[:12], after[:12])])

    def test_sent_from_outlook_matches_in_reply_to(self):
        message = self.outlook_send(conversationId='other-thread', subject='Edited subject',
                                   internetMessageHeaders=[{'name': 'iN-rEpLy-To', 'value': '<inbound-1>'}])
        self.tick()
        self.assert_sent(message)

    def test_sent_from_outlook_matches_references(self):
        message = self.outlook_send(conversationId='other-thread', subject='Edited subject',
                                   internetMessageHeaders=[{'name': 'References',
                                                            'value': '<older@example.invalid> <inbound-1>'}])
        self.tick()
        self.assert_sent(message)

    def test_sent_from_outlook_matches_subject(self):
        message = self.outlook_send(conversationId='other-thread')
        self.tick()
        self.assert_sent(message)

    def test_superseded_draft_does_not_claim_a_newer_task_send_by_subject(self):
        # The stale draft (thread-1) was deleted; the new task's letter, with the
        # same subject, was sent in its own conversation (thread-2).
        self.packet['thread_id'] = 'thread-2'
        self.task_id = self.add_task('new-reply', 'draft-2')
        message = self.outlook_send(sent_id='sent-2', draft_id='draft-2', conversationId='thread-2')
        self.graph.drafts.pop('draft-1')
        self.tick()
        self.assert_sent(message)
        self.packet['thread_id'] = 'thread-1'
        self.task_id = 'owner-reply'
        self.assert_missing()

    def test_a_sent_message_is_never_claimed_twice(self):
        message = self.outlook_send()
        self.tick()
        self.assert_sent(message)
        self.task_id = self.add_task('second-reply', 'draft-2')
        self.graph.drafts.pop('draft-2')
        self.tick()
        self.assert_missing()

    def test_no_match_keeps_missing_draft_pending_without_deleting(self):
        self.outlook_send(conversationId='other-thread', subject='Unrelated subject')
        self.tick()
        self.assert_missing()
        before = self.row()
        self.tick()
        self.assertEqual(self.row(), before)

    def test_ambiguous_matches_are_not_receipts_and_can_resolve_on_a_later_tick(self):
        message = self.outlook_send()
        self.graph.sent['sent-2'] = {**message, 'id': 'sent-2', 'internetMessageId': '<sent-2@example.invalid>'}
        self.tick()
        row = self.row()
        self.assertEqual(row['state'], 'owner-draft')
        self.assertIsNone(row['data']['sent_receipt'])
        self.assertEqual(row['data']['owner_draft_status'], 'sent-ambiguous')
        self.assertIn('2 Sent Items messages', row['data']['owner_draft_note'])
        del self.graph.sent['sent-2']
        self.box.sent = None  # A new tick has fresh adapter caches.
        # Missing/ambiguous owner drafts are observed again after the cadence.
        with patch.object(self.ledger.tasks, 'clock', return_value=
                          self.ledger.tasks.clock() + delivery.OWNER_SENT_RECHECK_SECONDS):
            self.tick()
        self.assert_sent(message)

    def test_draft_still_in_drafts_is_untouched_even_with_matching_sent(self):
        original = copy.deepcopy(self.graph.drafts['draft-1'])
        self.outlook_send()
        self.graph.drafts['draft-1'] = original
        before = self.row()
        self.tick()
        self.assertEqual(self.row(), before)
        self.assertFalse(any('/sentitems/' in path for path, _, _ in self.graph.calls))

    def test_readable_recoverable_draft_is_missing_when_outside_drafts(self):
        self.graph.drafts['draft-1']['parentFolderId'] = 'recoverable-items'
        self.tick()
        self.assert_missing()
        self.assertIn('draft-1', self.graph.drafts)

    def test_sent_message_can_retain_the_draft_immutable_id(self):
        message = self.outlook_send(sent_id='draft-1')
        self.tick()
        self.assert_sent(message)

    def test_deleted_draft_403_still_needs_positive_sent_evidence(self):
        self.graph.drafts.clear()
        error = HTTPError('fixture:deleted', 403, 'Forbidden', {}, io.BytesIO())
        error.close()
        with patch.object(self.box, 'message', side_effect=error):
            self.tick()
        self.assert_missing()

    def test_uncertain_graph_read_keeps_task_pending_for_retry(self):
        self.outlook_send()
        with patch.object(self.box, 'message', side_effect=TimeoutError('Graph read timed out')):
            self.tick()
        self.assertEqual(self.row()['state'], 'owner-draft')
        self.assertIsNone(self.row()['data']['sent_receipt'])
        self.assertEqual(self.row()['data']['last_error'], 'Graph read timed out')
        self.tick()
        self.assertNotIn('last_error', self.row()['data'])
        self.assertEqual(self.row()['state'], 'verified')

    def test_snapshot_error_after_observation_retains_receipt_without_stale_update(self):
        message = self.outlook_send()
        with patch.object(self.ledger, 'save', side_effect=[TimeoutError('Snapshot write timed out'), None]):
            self.tick()
        self.assert_sent(message)
        self.assertEqual(self.row()['data']['last_error'], 'Snapshot write timed out')

    def test_recipient_set_combines_to_cc_casefolds_and_ignores_duplicates(self):
        message = self.outlook_send(toRecipients=[participant('COLLEAGUE@example.invalid')],
                                   ccRecipients=[participant('AUTHOR@example.invalid')] * 2)
        self.tick()
        self.assert_sent(message)

    def test_recipient_additions_and_omissions_do_not_match(self):
        message = self.outlook_send()
        for recipients in [[], [participant('different@example.invalid')],
                           message['toRecipients'] + [participant('extra@example.invalid')]]:
            with self.subTest(recipients=recipients):
                self.graph.sent['sent-1'] = {**message, 'toRecipients': recipients}
                self.box.sent = None
                self.tick()
                self.assert_missing()

    def test_team_cc_may_be_omitted_only_when_it_was_in_the_packet(self):
        packet = {**self.packet, 'cc': [*self.packet['cc'], 'TEAM@example.invalid']}
        self.update(packet_override=packet)
        message = self.outlook_send()
        self.tick()
        self.assert_sent(message, packet)

    def test_unrequested_team_cc_cannot_be_ignored(self):
        message = self.outlook_send(ccRecipients=[participant(a) for a in
                                   [*self.packet['cc'], 'team@example.invalid']])
        self.tick()
        self.assert_missing()
        self.assertIn('sent-1', self.graph.sent)

    def test_only_messages_strictly_after_receipt_time_can_match(self):
        self.update(draft_receipt={**self.row()['data']['draft_receipt'], 'at': SENT})
        self.outlook_send()
        self.tick()
        self.assert_missing()

    def test_numeric_draft_intent_time_precedes_mailbox_observation_fallback(self):
        self.update(draft_intent={'key': 'attempt-owner-reply',
                                  'at': mailbox.instant(SENT).timestamp()})
        self.outlook_send()
        self.tick()
        self.assert_missing()

    def test_revised_draft_uses_its_own_time_instead_of_original_intent(self):
        receipt = self.row()['data']['draft_receipt']
        self.update(draft_intent={'at': mailbox.instant(OBSERVED).timestamp()},
                    draft_revisions=[{'new_draft_receipt': receipt,
                                      'old_draft_deleted': {'at': mailbox.instant(SENT).timestamp()}}])
        self.outlook_send()
        self.tick()
        self.assert_missing()

    def test_missing_timestamp_or_sent_identity_is_not_evidence(self):
        message = self.outlook_send()
        for changes in [{'sentDateTime': ''}, {'sentDateTime': '2026-11-03T00:55:00'},
                        {'sentDateTime': None}, {'id': ''}, {'internetMessageId': ''}, {'isDraft': True}]:
            with self.subTest(changes=changes):
                self.graph.sent['sent-1'] = {**message, **changes}
                self.box.sent = None
                self.tick()
                self.assert_missing()

    def test_pause_and_disabled_kind_defer_observation(self):
        self.outlook_send()
        before = self.row()
        for config in [{**self.config, 'paused': True}, {**self.config, 'kinds': {'reply': False}}]:
            with self.subTest(config=config):
                self.tick(config)
                self.assertEqual(self.row(), before)
                self.assertEqual(self.graph.calls, [])

    def test_exhausted_tick_budget_defers_owner_sent_reads(self):
        self.outlook_send()
        before = self.row()
        Tick(seconds=0).bind(self.ledger)
        self.tick()
        self.assertEqual(self.row(), before)
        self.assertEqual(self.graph.calls, [])

    def test_sent_items_cache_is_shared_between_owner_tasks(self):
        self.add_task('owner-reply-2', 'draft-2')
        self.graph.drafts.clear()
        self.tick()
        self.assertEqual(sum('/sentitems/' in path for path, _, _ in self.graph.calls), 1)
        self.assert_missing()
        self.assertEqual(self.ledger.tasks.get('owner-reply-2')['data']['owner_draft_status'],
                         'draft-missing-not-sent')

    def test_sent_receipt_wins_over_a_pending_draft_revision(self):
        message = self.outlook_send()
        self.update(draft_revision_requests=[{'reason': 'A synthetic pending revision'}])
        self.tick()
        self.assert_sent(message)

    def test_missing_draft_never_starts_a_pending_revision(self):
        self.graph.drafts.clear()
        self.update(draft_revision_requests=[{'reason': 'A synthetic pending revision'}])
        self.tick()
        self.assert_missing()
        self.assertNotIn('draft_revision_intent', self.row()['data'])

    def test_receipt_reaches_registry_pending_outgoing_including_packet_override(self):
        packet = {**self.packet, 'body': 'The owner revised the checked reply.'}
        self.update(packet_override=packet)
        self.outlook_send()
        self.tick()
        work = self.inventory()
        before = {p: p.read_bytes() for p in (self.root / 'tables').iterdir()}
        result = registry.reconcile(work, root=self.root)
        self.assertEqual(result['exceptions'], [])
        self.assertEqual({event['email'] for event in result['pending_outgoing']},
                         set(packet['to'] + packet['cc']))
        self.assertTrue(all(event['at'] == SENT and event['kind'] == 'sent'
                            for event in result['pending_outgoing']))
        self.assertEqual(before, {p: p.read_bytes() for p in before})

    def test_reconciled_owner_send_automatically_reaches_table_one(self):
        message = self.outlook_send()
        self.tick()
        self.box.settings['live_since'] = OBSERVED
        with patch.object(self.box, 'sent_events_page', return_value=([message], None)), \
                patch.object(sent_events, 'commit_tables'):
            first = sent_events.reconcile(self.ledger, self.config, self.root, self.box)
            second = sent_events.reconcile(self.ledger, self.config, self.root, self.box)
        self.assertEqual(first['events'], len(self.packet['to'] + self.packet['cc']))
        self.assertEqual(second['events'], 0)
        for row in registry.read(self.root, 'contacts')['contacts']:
            event, = row['hub']['events']
            self.assertEqual(event['at'], SENT)
            self.assertIn(message['internetMessageId'], event['evidence'])
            self.assertIn(self.task_id, event['evidence'])


if __name__ == '__main__':
    unittest.main()


class SentWindowTests(unittest.TestCase):
    def test_owner_sent_reads_only_a_bounded_window_and_reuses_it(self):
        from datetime import datetime, timezone
        from urllib.parse import parse_qs, urlsplit

        class Pages:
            def __init__(self):
                self.paths = []
            def pages(self, path):
                self.paths.append(path)
                return [{'id': 'm', 'sentDateTime': '2026-11-15T00:00:00Z'}]

        graph = Pages()
        box = mailbox.GraphMailbox(graph, {'account': 'owner@example.invalid'})
        later = datetime(2026, 11, 14, 12, tzinfo=timezone.utc)
        earlier = datetime(2026, 11, 10, 12, tzinfo=timezone.utc)
        self.assertEqual(len(box.sent_items(later)), 1)
        query = parse_qs(urlsplit(graph.paths[-1]).query)
        self.assertEqual(query['$filter'], ['sentDateTime ge 2026-11-13T00:00:00Z'])
        self.assertIn('uniqueBody', query['$select'][0].split(','))
        box.sent_items(later)
        self.assertEqual(len(graph.paths), 1)
        box.sent_items(earlier)
        self.assertEqual(parse_qs(urlsplit(graph.paths[-1]).query)['$filter'],
                         ['sentDateTime ge 2026-11-09T00:00:00Z'])
        box.sent_items(later)
        self.assertEqual(len(graph.paths), 2)
        box.sent = None
        box.sent_items(later)
        self.assertEqual(len(graph.paths), 3)
