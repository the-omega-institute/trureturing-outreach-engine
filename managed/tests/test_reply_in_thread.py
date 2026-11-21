"""A reply draft counts as in-thread by conversation or by exact In-Reply-To."""
import unittest

from managed.mailbox import replies_in_thread


class ReplyInThreadTests(unittest.TestCase):
    packet = {'thread_id': 'conv-original', 'latest_inbound_message_id': '<in-1@coremail>'}

    def message(self, conversation, in_reply_to=None):
        headers = [{'name': 'In-Reply-To', 'value': in_reply_to}] if in_reply_to else []
        return {'conversationId': conversation, 'internetMessageHeaders': headers}

    def test_same_conversation(self):
        self.assertTrue(replies_in_thread(self.message('conv-original'), self.packet))

    def test_new_conversation_with_exact_in_reply_to(self):
        self.assertTrue(replies_in_thread(self.message('conv-new', '<in-1@coremail>'), self.packet))

    def test_new_conversation_without_or_with_other_in_reply_to(self):
        self.assertFalse(replies_in_thread(self.message('conv-new'), self.packet))
        self.assertFalse(replies_in_thread(self.message('conv-new', '<other@x>'), self.packet))


if __name__ == '__main__':
    unittest.main()
