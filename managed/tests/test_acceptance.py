import json
from pathlib import Path
import tempfile
import unittest

from managed import acceptance as a


class Mailbox:
    def __init__(self):
        self.sent = None
        self.draft = {'id': 'draft-1', 'conversationId': 'conversation-1', 'isDraft': True}
        self.graph = self
        self.creates = 0
        self.sends = 0
        self.changed = False

    def verify_owner(self): return a.mailbox_address()
    def create_draft(self, packet, key):
        self.creates += 1
        return self.draft
    def message(self, identity): return self.draft
    def matches(self, message, packet): return not self.changed
    def request(self, *args): self.sends += 1
    def find(self, folder, key): return self.sent


class AcceptanceTests(unittest.TestCase):
    def test_reply_acceptance_cannot_target_research_correspondence(self):
        with tempfile.TemporaryDirectory() as root:
            parent = {'recipient':'professor@sample.example.invalid', 'phase':'sent-verified', 'test_id':'parent-test-v1'}
            with self.assertRaisesRegex(ValueError, 'self-test'):
                a.delivery(None, 'test-reply-20261031', a.mailbox_address(), Path(root)/'state.json', parent)

    def test_modified_reply_parent_stops_before_any_write(self):
        mailbox = Mailbox()
        parent = {'recipient':a.mailbox_address(), 'phase':'sent-verified', 'test_id':'parent-test-v1', 'sent_id':'parent'}
        with tempfile.TemporaryDirectory() as root:
            with self.assertRaisesRegex(ValueError, 'Reply parent differs'):
                a.delivery(mailbox, 'test-reply-20261031', a.mailbox_address(), Path(root)/'state.json', parent)
        self.assertEqual(mailbox.sends, 0)
        self.assertEqual(mailbox.creates, 0)

    def test_unauthorized_recipient_rejected_before_mailbox_access(self):
        with tempfile.TemporaryDirectory() as root:
            with self.assertRaises(ValueError):
                a.delivery(None, 'test-20261031-v1', 'someone@sample.example.invalid', Path(root)/'state.json')

    def test_uncertain_send_is_never_repeated(self):
        mailbox = Mailbox()
        with tempfile.TemporaryDirectory() as root:
            path = Path(root)/'state.json'
            first = a.delivery(mailbox, 'test-20261031-v1', a.mailbox_address(), path)
            self.assertEqual(first['phase'], 'send-attempted')
            mailbox.sent = {'id': 'sent-1', 'internetMessageId': '<sent-1>', 'isDraft': False}
            second = a.delivery(mailbox, 'test-20261031-v1', a.mailbox_address(), path)
            self.assertEqual(second['phase'], 'sent-verified')
        self.assertEqual(mailbox.sends, 1)
        self.assertEqual(mailbox.creates, 1)

    def test_user_changed_draft_is_preserved(self):
        mailbox = Mailbox()
        mailbox.changed = True
        with tempfile.TemporaryDirectory() as root:
            with self.assertRaisesRegex(ValueError, 'draft changed'):
                a.delivery(mailbox, 'test-20261031-v1', a.mailbox_address(), Path(root)/'state.json')
        self.assertEqual(mailbox.sends, 0)

    def test_unknown_draft_creation_is_not_repeated(self):
        mailbox = Mailbox()
        with tempfile.TemporaryDirectory() as root:
            path = Path(root)/'state.json'
            path.write_text(json.dumps({'test_id':'test-20261031-v1', 'recipient':a.mailbox_address(),
                                        'phase':'creating-draft', 'started_at':0}))
            with self.assertRaisesRegex(ValueError, 'uncertain'):
                a.delivery(mailbox, 'test-20261031-v1', a.mailbox_address(), path)
        self.assertEqual(mailbox.creates, 0)
        self.assertEqual(mailbox.sends, 0)


if __name__ == '__main__':
    unittest.main()
