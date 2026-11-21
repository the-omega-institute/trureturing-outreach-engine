"""Local daily entry: preserve historical dates and require exact review."""
from contextlib import contextmanager
from datetime import datetime
import hashlib
import hmac
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from zoneinfo import ZoneInfo

from fixture_config import use_example_config
from managed import daily_local as d
from managed.queue import canonical


class Producer:
    def __init__(self):
        self.preparations = self.validations = self.publications = 0
        self.q = self
    @contextmanager
    def locked(self, path):
        yield
    def prepare(self, config, day):
        self.preparations += 1
        return Path(config['daily_state']) / day
    def validate(self, config, root): self.validations += 1
    def publish(self, config, root): self.publications += 1; return {'status': 'fixture-published'}


class DailyLocalTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.root = Path(self.temp.name)
        self.base = self.root / 'mail/outreach-maintenance/daily'
        self.legacy = self.root / 'mail/outreach-maintenance/legacy-daily/state'
        self.base.mkdir(parents=True); self.legacy.mkdir(parents=True)
        self.cfg = {'timezone': 'Asia/Singapore', 'daily_state': str(self.base / 'state'),
                    'historical_state': str(self.legacy), 'enabled': False, 'merged_days': {},
                    'account': {'id': '2001464981620166661', 'username': 'OmegaDesci'}}
        self.path = self.base / 'config.json'; d.save(self.path, self.cfg)
        self.now = datetime(2026, 11, 1, tzinfo=ZoneInfo('Asia/Singapore'))
        self.producer = Producer()
    def tearDown(self): self.temp.cleanup()
    def packet(self, day='2026-10-31'):
        root = Path(self.cfg['daily_state']) / day
        for name in ['candidate.json', 'stats.json', 'digest.json', 'catalogue.json', 'source-evidence.json',
                     'research-focus.json', 'history.json', 'cumulative.json', 'facts.json', 'sshx-transcript.json']:
            d.save(root / name, {'day': day, 'fixture': name})
        (root / 'body.txt').write_text('Synthetic body\n'); (root / 'card.png').write_bytes(b'synthetic-png')
        return root
    def review(self):
        env = {'binding': d.binding(self.cfg, '2026-10-31'), 'reviewer': 'local-claude-loop',
               'verdict': 'PASS', 'reason': 'Synthetic offline review fixture', 'evidence_ref': 'test:independent-review'}
        env['signature'] = hmac.new(b'secret', canonical(env).encode(), hashlib.sha256).hexdigest()
        return env

    def test_missing_day_is_visible_after_preserved_last_publication(self):
        d.save(self.legacy / '2026-10-30/receipt.json', {'id': 'daily-2026-10-30', 'url': 'https://x.com/OmegaDesci/status/1'})
        view = d.status(self.path, now=self.now)
        self.assertEqual(view['day'], '2026-10-31'); self.assertEqual(view['status'], 'not_started')
        self.assertEqual(view['pending_days_since_last_publication'], ['2026-10-31'])
        self.assertTrue(view['publication_paused']); self.assertEqual(view['last_published']['day'], '2026-10-30')
        with self.assertRaisesRegex(ValueError, 'complete'): d.status(self.path, '2026-11-01', self.now)

    def test_merged_historical_dates_block_preparation_and_publication(self):
        d.save(self.legacy / '2026-10-17/stats.json', {'first_day': '2026-10-16'})
        d.save(self.legacy / '2026-10-17/receipt.json', {'url': 'https://x.com/OmegaDesci/status/1'})
        for action in ['prepare', 'publish']:
            with self.assertRaisesRegex(ValueError, 'reserved'): d.operate(action, self.cfg, '2026-10-16', self.producer)
        self.assertEqual(self.producer.preparations + self.producer.publications, 0)

    def test_uncertain_historical_attempt_reserves_date(self):
        d.save(self.legacy / '2026-10-31/post-attempt.json', {'uncertain': True})
        with self.assertRaisesRegex(ValueError, 'reserved'): d.operate('prepare', self.cfg, '2026-10-31', self.producer)

    def test_preparation_can_run_while_publication_stays_paused(self):
        self.packet()
        result = d.operate('prepare', self.cfg, '2026-10-31', self.producer)
        self.assertEqual(result['status'], 'awaiting-claude-review')
        self.assertEqual((self.producer.preparations, self.producer.validations, self.producer.publications), (1, 1, 0))
        with self.assertRaisesRegex(ValueError, 'paused'): d.operate('publish', self.cfg, '2026-10-31', self.producer)
        self.assertEqual(self.producer.publications, 0)

    def test_changed_body_image_evidence_account_or_workflow_invalidates_review(self):
        root = self.packet(); env = self.review(); d.record_review(self.cfg, '2026-10-31', env, 'secret')
        for name in ['body.txt', 'card.png', 'facts.json', 'source-evidence.json']:
            with self.subTest(name=name):
                original = (root / name).read_bytes(); (root / name).write_bytes(original + b' ')
                with self.assertRaisesRegex(ValueError, 'changed'): d.record_review(self.cfg, '2026-10-31', env, 'secret')
                (root / name).write_bytes(original)
        changed = {**self.cfg, 'account': {'id': 'other'}}
        with self.assertRaisesRegex(ValueError, 'changed'): d.record_review(changed, '2026-10-31', env, 'secret')
        changed = {**self.cfg, 'merged_days': {'2026-10-31': '2026-10-30'}}
        with self.assertRaisesRegex(ValueError, 'coverage changed'): d.record_review(changed, '2026-10-31', env, 'secret')
        with patch('managed.daily_local.workflow', return_value='f' * 64), self.assertRaisesRegex(ValueError, 'changed'):
            d.record_review(self.cfg, '2026-10-31', env, 'secret')
        with self.assertRaisesRegex(ValueError, 'signature'): d.record_review(self.cfg, '2026-10-31', env, 'wrong')

    def test_live_publish_requires_current_pass_and_holds_unknown_create(self):
        root = self.packet(); self.cfg['enabled'] = True
        policy = self.root / 'managed/daily/workflow.json'; d.save(policy, {'enable_live_publication': True})
        with patch('managed.daily_local.DATA_ROOT', self.root), patch('managed.daily_local.review_key', return_value='secret'):
            with self.assertRaisesRegex(ValueError, 'PASS'): d.operate('publish', self.cfg, '2026-10-31', self.producer)
            env = self.review(); d.record_review(self.cfg, '2026-10-31', env, 'secret')
            d.save(root / 'post-attempt.json', {'uncertain': True})
            d.save(root / 'create.json', {'status': 402})
            with self.assertRaisesRegex(ValueError, 'never replay'): d.operate('publish', self.cfg, '2026-10-31', self.producer)
            self.assertEqual(self.producer.publications, 0)
            (root / 'post-attempt.json').unlink()
            d.operate('publish', self.cfg, '2026-10-31', self.producer)
            self.assertEqual(self.producer.publications, 1)

    def test_initialize_keeps_local_paths_and_receipts_without_enabling_legacy_timers(self):
        self.path.unlink()
        generic = self.root / 'generic.json'; d.save(generic, {'enabled': True, 'nyxid': '/test/nyxid'})
        d.save(self.legacy.parent / 'daily-config.json', {'publication_config': str(generic), 'enabled': True,
                                                       'timezone': 'Asia/Singapore', 'daily_state': '/old/moved/state'})
        d.save(self.legacy / '2026-10-30/stats.json', {'day': '2026-10-30'})
        d.save(self.legacy / '2026-10-30/receipt.json', {'id': 'daily-2026-10-30', 'url': 'https://x.com/OmegaDesci/status/1'})
        with patch('managed.daily_local.local_root', return_value=self.base):
            report = d.initialize(self.path); again = d.initialize(self.path)
        cfg = d.read(self.path)
        self.assertEqual(report['imported_receipts'], 1); self.assertFalse(again['created'])
        self.assertFalse(cfg['enabled']); self.assertEqual(cfg['daily_state'], str(self.base / 'state'))
        self.assertTrue((self.base / 'state/2026-10-30/stats.json').exists())
        self.assertNotIn('publication_config', cfg)


if __name__ == '__main__': unittest.main()


def setUpModule():
    use_example_config()
