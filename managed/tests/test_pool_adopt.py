import hashlib
import hmac
import unittest
from unittest.mock import patch

from fixture_config import use_cli_config
from managed import control
from managed.queue import canonical
from managed.sandbox_pool import SandboxPool
from test_sandbox_pool import PoolFixture
from test_tasks import request

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

    def retained(self, **changes):
        """A Sandbox rotated idle whose retirement CMA declined (other Agents remain)."""
        return {'sandbox_id': 'sbx_orphan', 'workspace_id': 'wks_orphan', 'created_by_task': 'seed-0',
                'state': 'retiring', 'rotated': 'idle', 'group': 'notes', 'agents_placed': 7,
                'idle_since': self.now - 9000, 'cleanup_intent': {'key': 'outreach-pool-retire-sbx_orphan'},
                'cleanup_checked_at': self.now - 60,
                'cleanup': {'sandbox_id': 'sbx_orphan', 'sandbox_retired': False,
                            'reason': 'Other Agent work found; retained Sandbox'}, **changes}

    def set_cleanup(self, entries):
        self.ledger.tasks.db.execute('INSERT OR REPLACE INTO pipeline_state VALUES(?,?)',
                                     ('sandbox_pool_cleanup', canonical(entries)))

    def events(self):
        return self.ledger.tasks.db.execute('SELECT COUNT(*) FROM control_events').fetchone()[0]

    def test_retained_rotation_returns_from_the_cleanup_list_atomically_once(self):
        other = {**self.retained(), 'sandbox_id': 'sbx_other', 'workspace_id': 'wks_other'}
        self.set_cleanup([self.retained(), other])
        value = self.envelope(group='notes')
        self.adopt(value)
        self.adopt(value)
        self.assertEqual(self.state('sandbox_pool_cleanup'), [other])
        [entry] = self.state()
        self.assertEqual({k: entry[k] for k in ('sandbox_id', 'workspace_id', 'group', 'state', 'agents_placed',
                                                'idle_since', 'readopted')},
                         {'sandbox_id': 'sbx_orphan', 'workspace_id': 'wks_orphan', 'group': 'notes',
                          'state': 'active', 'agents_placed': 7, 'idle_since': self.now,
                          'readopted': {'rotated': 'idle',
                                        'cleanup_reason': 'Other Agent work found; retained Sandbox'}})
        self.assertTrue(entry['created_by_task'].startswith('operator-adopt-'))
        self.assertEqual(self.events(), 1)
        # Pool health observes it as usable capacity of its group again.
        pool = SandboxPool(self.ledger, {'max_sandboxes': 2, 'agents_per_sandbox': 6, **GROUPS},
                           {'firstcontact': True, 'reply': True})
        pool.health(self.cma)
        self.assertIn('sbx_orphan', pool.available)

    def test_every_other_cleanup_list_case_is_still_refused(self):
        retained = 'CMA has not retained it'
        cases = {
            'failed': (self.retained(state='failed', rotated=None), 'not a rotated Sandbox'),
            'rotated-then-failed': (self.retained(state='failed'), 'not a rotated Sandbox'),
            'corrupt': (self.retained(corrupt='runtime journal corrupt'), 'corrupt or stalled'),
            'stalled': (self.retained(stalled='credential_rotating'), 'corrupt or stalled'),
            'retired': (self.retained(cleanup={'sandbox_id': 'sbx_orphan', 'sandbox_retired': True}), retained),
            'deletion-requested': (self.retained(cleanup={'sandbox_id': 'sbx_orphan', 'sandbox_retired': False}),
                                   retained),
            'not-yet-checked': (self.retained(cleanup=None), retained),
            'cleanup-error': (self.retained(cleanup_error='TimeoutError: lost DELETE response'), retained),
            'other-workspace': (self.retained(workspace_id='wks_elsewhere'), 'workspace differs'),
            'live-agent': (self.retained(), 'a live Agent of ours references it'),
        }
        for name, (entry, reason) in cases.items():
            with self.subTest(name=name):
                if name == 'live-agent':
                    row = self.ledger.tasks.add(request('still-running', 'owner', 'firstcontact'))
                    self.ledger.tasks.update(row['id'], row['revision'], 'running',
                                             {'agent_id': 'agt_ours', 'pool_sandbox_id': 'sbx_orphan'})
                self.set_cleanup([entry])
                with self.assertRaisesRegex(ValueError, 'Sandbox is on the cleanup list: ' + reason + '$'):
                    self.adopt(self.envelope(group='notes'))
                self.assertEqual(self.state('sandbox_pool_cleanup'), [entry])
                self.assertEqual(self.state(), [])
                self.assertEqual(self.events(), 0)

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
