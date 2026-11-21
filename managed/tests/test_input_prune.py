"""Ingested control inputs are pruned from the ledger release; nothing else is."""
from datetime import datetime, timezone
import hashlib
import subprocess
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from managed import automation, control
from test_tasks import Releases


class StubReleases(control.Releases):
    def __init__(self, assets):
        super().__init__('owner/repo', 'outreach-control')
        self.assets = assets
        self.deleted = []

    def release(self):
        return {'id': 7, 'assets': []}

    def run(self, *args, missing=False):
        if args[:2] == ('api', '--paginate'):
            return '\n'.join('\t'.join(a) for a in self.assets).encode()
        if args[:3] == ('api', '-X', 'DELETE'):
            self.deleted.append(args[3].rsplit('/', 1)[1])
            return b''
        raise AssertionError(args)


NOW = datetime(2026, 11, 7, 12, tzinfo=timezone.utc).timestamp()


class InputPruneTests(unittest.TestCase):
    def test_only_old_ingested_inputs_are_deleted(self):
        releases = StubReleases([
            ('1', 'review-a.json', '2026-11-05T00:00:00Z', 'sha256:a'),        # old, ingested
            ('2', 'review-b.json', '2026-11-07T11:00:00Z', 'sha256:b'),        # ingested but recent
            ('3', 'request-c.json', '2026-11-05T00:00:00Z', 'sha256:c'),       # old, not ingested
            ('4', 'state-d.sqlite3', '2026-11-01T00:00:00Z', 'sha256:d'),      # snapshot: never here
            ('5', 'decision-e.json', '2026-11-04T00:00:00Z', 'sha256:e'),      # old, ingested
            ('6', 'outreach-task-x.json', '2026-11-01T00:00:00Z', 'sha256:f'), # not a control input
        ])
        known = {'sha256:a', 'sha256:b', 'sha256:d', 'sha256:e', 'sha256:f'}
        self.assertEqual(releases.prune_ingested(known, now=NOW), 2)
        self.assertEqual(releases.deleted, ['1', '5'])

    def test_missing_release_is_a_no_op(self):
        releases = StubReleases([])
        releases.release = lambda: None
        self.assertEqual(releases.prune_ingested({'sha256:a'}, now=NOW), 0)


class IngestRejectionTests(unittest.TestCase):
    name = 'review-invalid.json'
    raw = b'{}'

    def setUp(self):
        self.releases = Releases()
        self.releases.assets[self.name] = self.raw
        self.sha = 'sha256:' + hashlib.sha256(self.raw).hexdigest()

    def test_retries_three_times_then_gives_up_until_asset_disappears(self):
        with patch.object(self.releases, 'download', wraps=self.releases.download) as download:
            for attempt in range(1, 5):
                with self.subTest(attempt=attempt), tempfile.TemporaryDirectory() as folder:
                    ledger = control.Ledger(self.releases, folder)
                    try:
                        errors = ledger.ingest('fixture-key')
                        if attempt < 4:
                            self.assertEqual(len(errors), 1)
                            self.assertIn('Invalid signed review', errors[0]['error'])
                            if attempt == 3:
                                self.assertIn('giving up after 3 attempts', errors[0]['error'])
                            else:
                                self.assertNotIn('giving up', errors[0]['error'])
                        else:
                            self.assertEqual(errors, [])
                        self.assertEqual(control.ingest_rejections(ledger.tasks)[self.sha],
                                         {'attempts': min(attempt, 3), 'error': 'Invalid signed review'})
                        self.assertEqual(self.sha in ledger.ingested_digests(), attempt >= 3)
                        work = automation.work(ledger.tasks, folder)
                        self.assertEqual(work['ingest_rejected_count'], int(attempt >= 3))
                    finally:
                        ledger.tasks.close()
            self.assertEqual(sum(call.args[0] == self.name for call in download.call_args_list), 3)

        del self.releases.assets[self.name]
        with tempfile.TemporaryDirectory() as folder:
            ledger = control.Ledger(self.releases, folder)
            try:
                self.assertEqual(ledger.ingest('fixture-key'), [])
                self.assertEqual(control.ingest_rejections(ledger.tasks), {})
                self.assertNotIn(self.sha, ledger.ingested_digests())
            finally:
                ledger.tasks.close()

    def test_transient_failures_remain_retryable(self):
        for error in (OSError('download failed'), HTTPError('https://example.invalid', 503, 'Unavailable', {}, None),
                      TimeoutError('timed out'), subprocess.TimeoutExpired('gh', 1),
                      RuntimeError('GitHub release operation failed: HTTP 502')):
            with self.subTest(error=type(error).__name__), tempfile.TemporaryDirectory() as folder:
                ledger = control.Ledger(self.releases, folder)
                try:
                    with patch.object(self.releases, 'download', side_effect=error) as download:
                        for _ in range(4):
                            with self.assertRaises(type(error)):
                                ledger.ingest('fixture-key')
                        self.assertEqual(download.call_count, 4)
                    self.assertEqual(control.ingest_rejections(ledger.tasks), {})
                    self.assertNotIn(self.sha, ledger.ingested_digests())
                finally:
                    ledger.tasks.close()
