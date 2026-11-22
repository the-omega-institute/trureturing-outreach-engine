import hashlib
import hmac
import unittest
from unittest.mock import patch

from fixture_config import use_cli_config
from managed import control
from managed.queue import canonical
from test_sandbox_pool import PoolFixture

SECRET = 'offline-recovery-key'
GROUPS = {'groups': [{'name': 'notes', 'kinds': ['firstcontact'], 'max_sandboxes': 1},
                     {'name': 'mail', 'kinds': ['reply'], 'max_sandboxes': 1}]}


class PoolAdoptTests(PoolFixture, unittest.TestCase):
    def envelope(self, **changes):
        value = {'schema_version': 1, 'sandbox_id': 'sbx_orphan', 'workspace_id': 'wks_orphan',
                 'group': 'mail', 'reason': 'Account quota held by retiring Sandboxes', **changes}
        value['signature'] = hmac.new(SECRET.encode(), canonical(value).encode(), hashlib.sha256).hexdigest()
        return value

    def adopt(self, value, secret=SECRET):
        with patch('managed.location.settings', return_value=GROUPS):
            control.adopt_pool_sandbox(self.ledger, value, secret)

    def test_signed_adoption_adds_an_idle_entry_to_the_named_group_once(self):
        value = self.envelope()
        self.adopt(value)
        self.adopt(value)
        entries = self.state()
        self.assertEqual(len(entries), 1)
        self.assertEqual({k: entries[0][k] for k in ('sandbox_id', 'workspace_id', 'group', 'state', 'agents_placed')},
                         {'sandbox_id': 'sbx_orphan', 'workspace_id': 'wks_orphan', 'group': 'mail',
                          'state': 'active', 'agents_placed': 0})
        self.assertTrue(entries[0]['created_by_task'].startswith('operator-adopt-'))

    def test_rejects_unsigned_tampered_unknown_group_or_duplicate(self):
        with self.assertRaisesRegex(ValueError, 'signature failed'):
            self.adopt({**self.envelope(), 'group': 'notes'})
        with self.assertRaisesRegex(ValueError, 'not configured'):
            self.adopt(self.envelope(), secret=None)
        with self.assertRaisesRegex(ValueError, 'Unknown pool group'):
            self.adopt(self.envelope(group='research'))
        self.seed_pool()
        with self.assertRaisesRegex(ValueError, 'already in the pool'):
            self.adopt(self.envelope(sandbox_id='sbx_existing0', workspace_id='wks_existing0'))
        self.assertEqual(len(self.state()), 1)

    def test_cli_signs_and_stages_the_request(self):
        staged = []
        with patch.object(control, 'Releases', return_value=self.remote), \
                patch.object(control, 'review_key', return_value=SECRET), \
                patch.object(control, 'stage', side_effect=lambda value, prefix, *a: staged.append((prefix, value)) or {}):
            control.main(['pool-adopt', '--sandbox', 'sbx_orphan', '--workspace', 'wks_orphan',
                          '--group', 'mail', '--reason', 'quota'])
        prefix, value = staged[0]
        self.assertEqual(prefix, 'pool-adopt')
        self.assertEqual(value, self.envelope(reason='quota'))


def setUpModule():
    use_cli_config()
