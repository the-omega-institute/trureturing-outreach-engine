"""A reviewed first contact may drop stale grouped recipients, never add them."""
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from managed.control import validate_result

A, B, C = 'a@example.invalid', 'b@example.invalid', 'c@example.invalid'


def row():
    history = {'fetched_at': '2026-11-09T00:00:00Z', 'complete': True, 'addresses': [A, B, C],
               'messages': [], 'attempts': [], 'recipient_addresses': {x: [x] for x in (A, B, C)}}
    return {'id': 'firstcontact-x', 'kind': 'firstcontact',
            'request': {'id': 'firstcontact-x', 'kind': 'firstcontact', 'owner': 'contact-1',
                        'inputs': {'automation_contract': 1, 'recipient_emails': [A, B, C],
                                   'to': [A], 'cc': [B, C]}},
            'data': {'mailbox_history': history}}


def result(to, cc, exclusions=None):
    packet = json.loads((Path(__file__).parent / 'fixtures/letter.json').read_text())
    packet.update(kind='firstcontact', to=to, cc=cc)
    for key in ('thread_id', 'latest_inbound_message_id', 'user_decision_ever'):
        packet.pop(key, None)
    value = {'schema_version': 1, 'task_id': 'firstcontact-x', 'outcome': 'completed',
             'report': 'Checked.', 'artifacts': [], 'packet': packet,
             'routing': {'mail': 'auto', 'questions': [], 'independent_submission': None}}
    if exclusions is not None:
        value['recipient_exclusions'] = exclusions
    return value


class RecipientExclusionTests(unittest.TestCase):
    def check(self, value):
        with patch('managed.control.firstcontact_gate'):
            validate_result(value, row())

    def test_full_group_still_required_without_exclusions(self):
        self.check(result([A], [B, C]))
        with self.assertRaisesRegex(Exception, 'preflighted recipients'):
            self.check(result([A], [B]))

    def test_listed_stale_address_may_be_dropped(self):
        self.check(result([A], [C], [{'email': B, 'reason': '2014 address, no current evidence'}]))

    def test_dropping_planned_to_promotes_first_remaining(self):
        self.check(result([B], [C], [{'email': A, 'reason': 'stale'}]))
        with self.assertRaisesRegex(Exception, 'To/Cc order'):
            self.check(result([C], [B], [{'email': A, 'reason': 'stale'}]))

    def test_exclusions_cannot_add_or_empty(self):
        with self.assertRaisesRegex(Exception, 'only remove'):
            self.check(result([A], [B], [{'email': 'new@example.invalid', 'reason': 'x'}]))
        with self.assertRaisesRegex(Exception, 'every recipient|Missing recipient'):
            self.check(result([], [], [{'email': x, 'reason': 'x'} for x in (A, B, C)]))
        with self.assertRaisesRegex(Exception, 'Invalid recipient_exclusions'):
            self.check(result([A], [C], [{'email': B}]))

    def test_unlisted_omission_still_rejected(self):
        with self.assertRaisesRegex(Exception, 'preflighted recipients'):
            self.check(result([A], [], [{'email': B, 'reason': 'stale'}]))


if __name__ == '__main__':
    unittest.main()
