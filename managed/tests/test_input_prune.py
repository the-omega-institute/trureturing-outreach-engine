"""Ingested control inputs are pruned from the ledger release; nothing else is."""
import hashlib
import subprocess
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from managed import automation, control
from test_tasks import Releases, request
from test_snapshot_prune import RetentionReleases


class StubReleases(control.Releases):
    def __init__(self, assets):
        super().__init__('owner/repo', 'outreach-control')
        self.assets = assets
        self.deleted = []
        self.covered = {}

    def release(self):
        return {'id': 7, 'assets': [],'body':control.canonical({
            'schema_version':1,'generation':7,'snapshot':{'name':'state-current.sqlite3'},
            'run_history':[{'name':'run-2.sqlite3'},{'name':'run-1.sqlite3'}]})}

    def snapshot_receipts(self,snapshot):return self.covered

    def run(self, *args, missing=False):
        if args[:2] == ('api', '--paginate'):
            return '\n'.join('\t'.join(a) for a in self.assets).encode()
        if args[:3] == ('api', '-X', 'DELETE'):
            self.deleted.append(args[3].rsplit('/', 1)[1])
            return b''
        raise AssertionError(args)


class InputPruneTests(unittest.TestCase):
    def test_ac_no_eligible_receipts_never_downloads_oldest_snapshot(self):
        for receipts in ({}, {'new-identity':6}):
            with self.subTest(receipts=receipts):
                releases = StubReleases([])
                release = releases.release()
                pointer = control.json.loads(release['body'])
                pointer['run_history'][-1]['generation'] = 5
                release['body'] = control.canonical(pointer)
                with patch.object(releases, 'release', return_value=release), \
                        patch.object(releases, 'snapshot_receipts', side_effect=AssertionError('snapshot downloaded')):
                    self.assertEqual(releases.prune_ingested(receipts), 0)

    def test_ac_capacity_path_with_empty_current_receipts_skips_oldest_download(self):
        releases = StubReleases([])
        with patch.object(releases, 'snapshot_receipts', return_value={}) as read:
            self.assertEqual(releases.prune_ingested(), 0)
        self.assertEqual([call.args[0]['name'] for call in read.call_args_list], ['state-current.sqlite3'])

    def test_am_legacy_ledger_seeds_giveups_without_readmission_or_counter_changes(self):
        remote = RetentionReleases(); value = request('legacy-given-up')
        raw = control.canonical(value).encode()
        sha = 'sha256:' + hashlib.sha256(raw).hexdigest()
        rejected = {sha:{'attempts':3, 'error':'former admission failure'}}
        with tempfile.TemporaryDirectory() as root:
            path = control.Path(root)/'legacy.sqlite3'; tasks = control.Tasks(path)
            try:
                tasks.db.execute('INSERT INTO pipeline_state VALUES(?,?)', ('ingest_rejected',control.canonical(rejected)))
                tasks.db.execute('DROP TABLE input_asset_receipts')
                # Publish actual legacy bytes, without the new table/history.
                legacy = control.Path(root)/'state-legacy.sqlite3'; tasks.snapshot(legacy)
            finally: tasks.close()
            remote.upload(legacy)
            remote.body = {'schema_version':1,'generation':17,
                           'snapshot':{'name':legacy.name,'sha256':hashlib.sha256(legacy.read_bytes()).hexdigest()}}
            remote.assets['request-legacy-given-up.json'] = raw
            identity = control.input_identity(next(a for a in remote.release()['assets'] if a['name'].startswith('request-')))
            for number in range(3):
                with tempfile.TemporaryDirectory() as folder:
                    ledger = control.Ledger(remote,folder); ledger.run_id = f'migrated-{number}'
                    try:
                        with patch.object(remote, 'download', wraps=remote.download) as download:
                            self.assertEqual(ledger.ingest(), [])
                        self.assertEqual(download.call_count, 0)
                        self.assertEqual(ledger.tasks.counts(), {})
                        self.assertEqual(control.ingest_rejections(ledger.tasks), rejected)
                        self.assertEqual(ledger.ingestion_receipts(), {identity:18})
                        self.assertEqual(remote.prune_ingested(ledger.ingestion_receipts()), int(number==2))
                    finally: ledger.tasks.close()

    def test_only_durably_covered_inputs_are_deleted_regardless_of_age(self):
        releases = StubReleases([
            ('1', 'review-a.json', '2026-11-05T00:00:00Z', 'sha256:a'),        # old, ingested
            ('2', 'review-b.json', '2026-11-07T11:00:00Z', 'sha256:b'),        # absent from oldest run final
            ('3', 'request-c.json', '2026-11-05T00:00:00Z', 'sha256:c'),       # old, not ingested
            ('4', 'state-d.sqlite3', '2026-11-01T00:00:00Z', 'sha256:d'),      # snapshot: never here
            ('5', 'decision-e.json', '2026-11-04T00:00:00Z', 'sha256:e'),      # old, ingested
            ('6', 'outreach-task-x.json', '2026-11-01T00:00:00Z', 'sha256:f'), # not a control input
        ])
        identities={a[3]:control.input_identity({'id':a[0],'name':a[1],'digest':a[3]}) for a in releases.assets}
        known = {identities[sha]:first for sha,first in
                 {'sha256:a':4, 'sha256:b':5, 'sha256:d':1, 'sha256:e':1, 'sha256:f':1}.items()}
        releases.covered={identity:first for identity,first in known.items() if identity!=identities['sha256:b']}
        self.assertEqual(releases.prune_ingested(known), 2)
        self.assertEqual(releases.deleted, ['1', '5'])
        releases.deleted.clear();releases.covered[identities['sha256:b']]=5
        self.assertEqual(releases.prune_ingested(known),3)
        self.assertIn('2',releases.deleted)

    def test_missing_release_is_a_no_op(self):
        releases = StubReleases([])
        releases.release = lambda: None
        self.assertEqual(releases.prune_ingested({'sha256:a':1}), 0)

    def test_ingestion_plus_two_later_runs_survives_restore_and_keeps_pending(self):
        remote=RetentionReleases();value=request()
        raw=control.canonical(value).encode();sha='sha256:'+hashlib.sha256(raw).hexdigest()
        remote.assets['request-consumed.json']=raw
        remote.assets['review-pending.json']=b'{}'
        identity=control.input_identity(remote.release()['assets'][0])
        with tempfile.TemporaryDirectory() as folder:
            ledger=control.Ledger(remote,folder)
            try:
                ledger.ingest()  # No review key: the review is still pending.
                first=ledger.generation
                self.assertEqual(ledger.ingestion_receipts(),{identity:first})
                for later in range(3):
                    self.assertEqual(remote.prune_ingested(ledger.ingestion_receipts()),int(later==2))
                    self.assertIn('review-pending.json',remote.assets)
                    if later==2:break
                    ledger.run_id=f'next-run-{later}'
                    row=ledger.tasks.get(value['id'])
                    ledger.tasks.update(row['id'],row['revision'],'held',{'reason':str(later)})
                    ledger.save(force=True)
                self.assertNotIn('request-consumed.json',remote.assets)
                with tempfile.TemporaryDirectory() as restored:
                    check=control.Ledger(remote,restored)
                    try:
                        self.assertEqual(check.ingestion_receipts(),{identity:first})
                        self.assertEqual(check.tasks.get(value['id'])['state'],'held')
                    finally:check.tasks.close()
            finally:ledger.tasks.close()

    def test_failed_upload_or_pointer_and_unchanged_saves_never_age_receipts(self):
        remote=RetentionReleases()
        with tempfile.TemporaryDirectory() as folder:
            ledger=control.Ledger(remote,folder)
            try:
                row=ledger.tasks.add(request());sha='sha256:'+row['request_hash']
                remote.assets['request-kept.json']=control.canonical(row['request']).encode()
                identity=control.input_identity(remote.release()['assets'][0])
                remote.fail_pointer=True
                with self.assertRaises(TimeoutError):ledger.ingest()
                self.assertEqual(remote.prune_ingested(ledger.ingestion_receipts()),0)
                remote.fail_pointer=False;ledger.save(force=True)
                for _ in range(3):self.assertFalse(ledger.save(force=True))
                for i in range(3):
                    row=ledger.tasks.update(row['id'],row['revision'],'held',{'reason':str(i)})
                    with patch.object(remote,'upload',side_effect=RuntimeError('upload failed')):
                        with self.assertRaises(RuntimeError):ledger.save(force=True)
                    self.assertEqual(ledger.generation,1)
                    self.assertEqual(remote.prune_ingested(ledger.ingestion_receipts()),0)
                self.assertEqual(ledger.ingestion_receipts(),{identity:1})
                self.assertIn('request-kept.json',remote.assets)
            finally:ledger.tasks.close()


class IngestRejectionTests(unittest.TestCase):
    name = 'review-invalid.json'
    raw = b'{}'

    def setUp(self):
        self.releases = Releases()
        self.releases.assets[self.name] = self.raw
        self.sha = 'sha256:' + hashlib.sha256(self.raw).hexdigest()
        self.identity = control.input_identity({'name':self.name,'digest':self.sha})

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
                        self.assertEqual(self.identity in ledger.ingestion_receipts(), attempt >= 3)
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
                self.assertNotIn(self.identity, ledger.ingestion_receipts())
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
                    self.assertNotIn(self.identity, ledger.ingestion_receipts())
                finally:
                    ledger.tasks.close()

    def test_reappearing_rejected_input_restarts_durable_coverage(self):
        with tempfile.TemporaryDirectory() as folder:
            ledger=control.Ledger(self.releases,folder)
            try:
                for _ in range(3):ledger.ingest('fixture-key')
                first=ledger.ingestion_receipts()[self.identity]
                del self.releases.assets[self.name]
                ledger.ingest('fixture-key')
                self.assertNotIn(self.identity,ledger.ingestion_receipts())
                self.releases.assets[self.name]=self.raw
                for _ in range(2):
                    ledger.ingest('fixture-key')
                    self.assertNotIn(self.identity,ledger.ingestion_receipts())
                ledger.ingest('fixture-key')
                self.assertGreater(ledger.ingestion_receipts()[self.identity],first)
                self.assertEqual(ledger.ingestion_receipts()[self.identity],ledger.generation)
            finally:ledger.tasks.close()
