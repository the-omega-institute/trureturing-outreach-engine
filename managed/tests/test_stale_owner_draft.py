"""New inbound replies replace only unchanged receipt-owned drafts, offline."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.parse import unquote, urlsplit

from managed import automation, control, delivery, mailbox
from managed.queue import digest
from test_reply_threading import DraftGraph
from test_tasks import Releases, request


class StaleDraftGraph(DraftGraph):
    def __init__(self, packet):
        super().__init__(packet)
        self.deleted = {}
        self.previous = None
        self.lose_move = False

    def message(self, identity, key, wrong=False):
        return {**super().message(identity, key, wrong), 'parentFolderId': 'drafts-folder',
                'createdDateTime': '2026-11-01T06:00:00Z',
                'lastModifiedDateTime': '2026-11-01T06:00:00Z'}

    def request(self, path, method='GET', body=None):
        if path == '/me/mailFolders/drafts?$select=id':
            self.calls.append((path, method, body))
            return {'id': 'drafts-folder'}
        identity = unquote(urlsplit(path).path.split('/')[3]) if path.startswith('/me/messages/') else None
        if path.endswith('/move'):
            self.calls.append((path, method, body))
            assert method == 'POST' and body == {'destinationId': 'deleteditems'}
            draft = self.drafts.pop(identity)
            self.deleted[identity] = {**draft, 'parentFolderId': 'deleted-folder'}
            if self.lose_move:
                self.lose_move = False
                raise TimeoutError('Move accepted; response lost')
            return copy.deepcopy(self.deleted[identity])
        if identity in self.deleted:
            self.calls.append((path, method, body))
            return copy.deepcopy(self.deleted[identity])
        return super().request(path, method, body)

    def pages(self, path):
        rows = super().pages(path)
        if path.startswith('/me/messages?'):
            rows.extend(copy.deepcopy(list(self.deleted.values())))
            if self.previous:
                rows.append(copy.deepcopy(self.previous))
        return rows


class StaleOwnerDraftTests(unittest.TestCase):
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
        self.config = {'paused': False, 'kinds': {'reply': True}}
        self.old_packet = json.loads((Path(__file__).parent / 'fixtures/letter.json').read_text())
        self.old_packet.update(kind='reply', thread_id='conversation',
                               latest_inbound_message_id='<old-inbound>', user_decision_ever=True)
        contacts = [{'id': 'contact-' + str(i), 'name': 'Professor Example', 'email': address}
                    for i, address in enumerate(self.old_packet['to'] + self.old_packet['cc'])]
        for name, value in [('contacts', {'contacts': contacts}), ('refusals', {'refusals': []}),
                            ('submissions', {'papers': []})]:
            (self.root / 'tables' / (name + '.json')).write_text(json.dumps(value))
        self.graph = StaleDraftGraph(self.old_packet)
        self.box = mailbox.GraphMailbox(self.graph, {'account': self.old_packet['sender'], 'reconciled': True})
        self.receipt = self.box.save_draft(self.old_packet, 'old-attempt')
        self.old_id = self.receipt['remote_id']
        req = request('reply-old', 'contact-0', 'reply')
        req['inputs'] = self.inputs(self.graph.inbound)
        old = self.ledger.tasks.add(req)
        self.ledger.tasks.update(old['id'], old['revision'], 'owner-draft', {
            'result': {'packet': self.old_packet}, 'draft_receipt': self.receipt})
        self.graph.previous = {**copy.deepcopy(self.graph.inbound), 'id': 'old-inbound'}
        self.graph.inbound.update(internetMessageId='<new-inbound>', receivedDateTime='2026-11-02T05:00:00Z')
        self.packet = {**self.old_packet, 'latest_inbound_message_id': '<new-inbound>',
                       'body': 'Dear Professor Example,\n\nThe updated synthetic result is 7.\n'}
        self.graph.packet = self.packet
        self.graph.calls.clear()

    def inputs(self, incoming):
        return {'thread_id': incoming['conversationId'],
                'latest_inbound_message_id': incoming['internetMessageId'],
                'thread': [copy.deepcopy(incoming)], 'user_decision_ever': True}

    def stage(self):
        req = request('reply-new', 'contact-0', 'reply')
        req['inputs'] = self.inputs(self.graph.inbound)
        return self.ledger.tasks.add(req)

    def approve(self):
        row = self.stage()
        result = {'schema_version': 1, 'task_id': row['id'], 'outcome': 'completed',
                  'report': 'Synthetic checked update.', 'artifacts': [], 'packet': self.packet,
                  'routing': {'mail': 'owner', 'questions': [], 'independent_submission': None}}
        row = self.ledger.tasks.update(row['id'], row['revision'], 'awaiting-review', {'result': result})
        review = control.sign_review({'binding': self.ledger.tasks.review_binding(row),
            'reviewer': 'local-claude-loop', 'verdict': 'PASS', 'reason': 'Synthetic independent review.',
            'evidence_ref': 'fixture:review'}, 'stale-draft-secret')
        row = self.ledger.tasks.verdict(review, 'stale-draft-secret')
        return self.ledger.tasks.update(row['id'], row['revision'], 'owner-draft', row['data'])

    def tick(self, config=None):
        delivery.advance(self.ledger, config or self.config, self.root, self.box)

    def assert_replaced(self):
        old, new = (self.ledger.tasks.get(name) for name in ('reply-old', 'reply-new'))
        self.assertEqual(old['state'], 'superseded')
        self.assertEqual(old['data']['superseded_by'], new['id'])
        self.assertEqual(old['data']['draft_receipt']['remote_id'], self.old_id)
        self.assertIn(self.old_id, self.graph.deleted)
        self.assertNotIn(self.old_id, self.graph.drafts)
        receipt = new['data']['draft_receipt']
        self.assertEqual(receipt['content_hash'], digest(self.packet))
        self.assertTrue(self.box.matches(self.graph.drafts[receipt['remote_id']], self.packet))
        self.assertEqual(new['data']['stale_draft_replacement']['remote_id'], self.old_id)
        self.assertEqual(new['data']['stale_draft_replacement']['phase'], 'moved')
        self.assertTrue(new['data']['stale_draft_replacement']['verified_absent'])
        self.assertEqual(new['data']['stale_draft_replacement']['unchanged']['content_hash'], digest(self.old_packet))
        self.assertFalse(any(method == 'DELETE' or path.endswith('/send') for path, method, _ in self.graph.calls))

    def test_unchanged_stale_draft_is_moved_then_latest_draft_saved(self):
        self.approve()
        self.tick()
        self.assert_replaced()
        moves = [call for call in self.graph.calls if call[0].endswith('/move')]
        self.assertEqual(moves, [('/me/messages/' + self.old_id + '/move', 'POST',
                                 {'destinationId': 'deleteditems'})])

    def test_edited_or_uncertain_stale_draft_is_preserved_with_both_tasks_in_exception(self):
        changes = [
            {'lastModifiedDateTime': '2026-11-02T06:00:00Z'},
            {'body': {'contentType': 'text', 'content': 'Owner edits, even with unchanged timestamps.'}},
            {'createdDateTime': 'unknown', 'lastModifiedDateTime': 'unknown'},
        ]
        self.approve()
        original = copy.deepcopy(self.graph.drafts[self.old_id])
        for change in changes:
            with self.subTest(change=change):
                edited = {**copy.deepcopy(original), **change}
                self.graph.drafts[self.old_id] = edited
                self.tick()
                self.assertEqual(self.graph.drafts[self.old_id], edited)
                self.assertEqual(len(self.graph.mimes), 1)
                self.assertFalse([call for call in self.graph.calls if call[1] != 'GET'])
                with patch('managed.daily_local.status', return_value={}):
                    exceptions = automation.work(self.ledger.tasks, self.root)['owner_draft_exceptions']
                self.assertEqual(len(exceptions), 1)
                self.assertEqual(exceptions[0]['blocker'], 'stale-user-draft')
                self.assertEqual(exceptions[0]['task_ids'], ['reply-old', 'reply-new'])
                self.assertEqual(exceptions[0]['remote_id'], self.old_id)

    def test_missing_timestamps_use_exact_saved_content_hash(self):
        self.approve()
        draft = self.graph.drafts[self.old_id]
        draft.pop('createdDateTime'); draft.pop('lastModifiedDateTime')
        self.tick()
        self.assert_replaced()

    def test_missing_timestamps_and_content_hash_preserve_uncertain_draft(self):
        self.approve()
        draft = self.graph.drafts[self.old_id]
        draft.pop('createdDateTime'); draft.pop('lastModifiedDateTime')
        old = self.ledger.tasks.get('reply-old')
        receipt = {k: v for k, v in self.receipt.items() if k != 'content_hash'}
        self.ledger.tasks.update(old['id'], old['revision'], old['state'],
                                 {**old['data'], 'draft_receipt': receipt})
        self.tick()
        self.assertIn(self.old_id, self.graph.drafts)
        self.assertEqual(self.ledger.tasks.get('reply-new')['data']['stale_draft_conflict']['remote_id'], self.old_id)
        self.assertFalse([call for call in self.graph.calls if call[1] != 'GET'])

    def test_edit_during_preflight_is_preserved_before_move(self):
        self.approve()
        preflight = self.box.preflight
        def edited(*args, **kwargs):
            result = preflight(*args, **kwargs)
            self.graph.drafts[self.old_id]['body']['content'] = 'Owner edited during preflight.'
            return result
        with patch.object(self.box, 'preflight', side_effect=edited):
            self.tick()
        self.assertEqual(self.graph.drafts[self.old_id]['body']['content'], 'Owner edited during preflight.')
        self.assertFalse([call for call in self.graph.calls if call[1] != 'GET'])
        self.assertIn('stale_draft_conflict', self.ledger.tasks.get('reply-new')['data'])

    def test_pause_and_stale_review_prevent_moves(self):
        self.approve()
        self.tick({**self.config, 'paused': True})
        new = self.ledger.tasks.get('reply-new')
        result = {**new['data']['result'], 'report': 'Changed after PASS.'}
        self.ledger.tasks.update(new['id'], new['revision'], new['state'], {**new['data'], 'result': result})
        self.tick()
        self.assertIn(self.old_id, self.graph.drafts)
        self.assertFalse([call for call in self.graph.calls if call[1] != 'GET'])

    def test_unknown_draft_is_preserved(self):
        self.approve()
        old = self.ledger.tasks.get('reply-old')
        self.ledger.tasks.update(old['id'], old['revision'], old['state'],
                                 {**old['data'], 'draft_receipt': {'remote_id': 'another-id'}})
        self.tick()
        self.assertIn(self.old_id, self.graph.drafts)
        self.assertFalse([call for call in self.graph.calls if call[1] != 'GET'])
        self.assertIn('A server draft already exists', self.ledger.tasks.get('reply-new')['data']['last_error'])

    def test_lost_move_response_restores_intent_and_does_not_move_or_create_twice(self):
        self.approve()
        self.graph.lose_move = True
        self.tick()
        self.assertEqual(len(self.graph.mimes), 1)
        self.assertIn(self.old_id, self.graph.deleted)
        self.ledger.tasks.close()
        self.folder = Path(tempfile.mkdtemp(dir=self.root))
        self.ledger = control.Ledger(self.remote, self.folder)
        self.assertEqual(self.ledger.tasks.get('reply-new')['data']['stale_draft_replacement']['phase'], 'move-intent')
        self.tick()
        self.assert_replaced()
        self.tick()
        self.assertEqual(len(self.graph.mimes), 2)
        self.assertEqual(len([call for call in self.graph.calls if call[0].endswith('/move')]), 1)

    def test_new_inbound_supersedes_unsent_owner_draft_and_revising_tasks(self):
        old = self.ledger.tasks.get('reply-old')
        req = request('reply-revising', 'contact-0', 'reply')
        req['inputs'] = copy.deepcopy(old['request']['inputs'])
        revising = self.ledger.tasks.add(req)
        self.ledger.tasks.update(revising['id'], revising['revision'], 'revising', {'reason': 'Improve reply'})
        new = self.stage()
        for name in ('reply-old', 'reply-revising'):
            row = self.ledger.tasks.get(name)
            self.assertEqual(row['state'], 'superseded')
            self.assertEqual(row['data']['superseded_by'], new['id'])
        self.assertIn(self.old_id, self.graph.drafts, 'Staging alone must not change Drafts')

    def test_same_or_older_inbound_other_conversation_and_sent_tasks_are_not_superseded(self):
        original = self.ledger.tasks.get('reply-old')
        for name, changes in [
            ('same-inbound', {'internetMessageId': '<old-inbound>'}),
            ('older-inbound', {'receivedDateTime': '2026-10-31T05:00:00Z'}),
            ('other-conversation', {'conversationId': 'another-conversation'}),
        ]:
            with self.subTest(name=name):
                req = request(name, 'contact-0', 'reply')
                req['inputs'] = self.inputs({**self.graph.inbound, **changes})
                self.ledger.tasks.add(req)
                self.assertEqual(self.ledger.tasks.get('reply-old'), original)
        self.ledger.tasks.update(original['id'], original['revision'], 'owner-draft',
                                 {**original['data'], 'sent_receipt': {'remote_id': 'sent-copy'}})
        self.stage()
        self.assertEqual(self.ledger.tasks.get('reply-old')['state'], 'owner-draft')


if __name__ == '__main__':
    unittest.main()
