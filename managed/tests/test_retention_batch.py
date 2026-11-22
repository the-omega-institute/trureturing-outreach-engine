"""Regressions for the Stage 1 retention review, using only synthetic assets."""
import io
import os
from pathlib import Path
import re
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import patch

from managed import control, run_log
from test_tasks import Releases as MemoryReleases, request


class SyntheticReleases(MemoryReleases, control.Releases):
    repo = 'synthetic/repo'

    def __init__(self):
        super().__init__()
        self.ids = {}; self.deleted = []; self.upload_attempts = []

    def require_private(self): pass
    def ensure(self): pass

    def release(self):
        value = super().release(); value['id'] = 1
        for asset in value['assets']:
            asset['id'] = self.ids.setdefault(asset['name'], len(self.ids) + 1)
            asset['created_at'] = '2026-11-23T00:00:00Z'
        return value

    def upload(self, path):
        self.upload_attempts.append(path.name)
        if path.name not in self.assets and len(self.assets) >= 1000:
            raise RuntimeError('file_count limited')
        super().upload(path)

    def run(self, *args, **kwargs):
        assets = self.release()['assets']
        if args[:3] == ('api', '-X', 'DELETE'):
            asset = next(a for a in assets if str(a['id']) == args[-1].rsplit('/', 1)[1])
            assert asset['name'] != (self.body.get('snapshot') or {}).get('name')
            self.deleted.append(asset['name']); del self.assets[asset['name']]
            return b''
        assert args[:2] == ('api', '--paginate'), args
        query = args[-1]
        if 'startswith("state-")' in query:
            assets = [a for a in assets if a['name'].startswith('state-')]
        elif 'startswith("tick-log-")' in query:
            assets = [a for a in assets if a['name'].startswith('tick-log-')]
        elif 'startswith("run-log-")' in query:
            assets = [a for a in assets if a['name'].startswith('run-log-')]
        keys = ['id', 'name']
        if '.created_at' in query: keys.append('created_at')
        if '.digest' in query: keys.append('digest')
        return '\n'.join('\t'.join(str(a.get(k, '')) for k in keys) for a in assets).encode()


def new_run(remote, folder, number):
    # Production gets this identity from the shared Actions run and attempt.
    with patch.dict(os.environ, {'GITHUB_RUN_ID': str(number), 'GITHUB_RUN_ATTEMPT': '1'}):
        return control.Ledger(remote, folder)


def checkpoint(ledger, reason):
    row = ledger.tasks.get('job-1')
    ledger.tasks.update(row['id'], row['revision'], 'held', {'reason': reason})
    ledger.save(force=True)
    return ledger.current


def production_log_name(number, step=0, *, tick=False, workflow='Coordinator', legacy=False):
    # Capture the actual producer's name, with deterministic times for ordering.
    captured = []
    stamp = f'202611{number:02}T0000{step:02}000000Z'
    with patch.dict(os.environ, {'GITHUB_RUN_ID':str(number), 'GITHUB_RUN_ATTEMPT':'1',
                                'GITHUB_WORKFLOW':workflow, 'GITHUB_WORKFLOW_REF':'',
                                'OUTREACH_PUBLIC_LOG':'1'}), \
            patch.object(run_log, 'datetime') as clock, \
            patch.object(run_log, '_upload', side_effect=lambda path,tick:captured.append(path.name)), \
            redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
        clock.now.return_value.strftime.return_value = stamp
        run_log.public_run(lambda: None, 'synthetic', tick=tick)
    # Batch 1's production format has run binding but no workflow field.
    return re.sub(r'-workflow-[a-f0-9]{24}', '', captured[0]) if legacy else captured[0]


class RetentionBatchTests(unittest.TestCase):
    maxDiff = 2000

    def test_q1_orphan_between_committed_uploads_never_becomes_rollback_point(self):
        remote = SyntheticReleases(); finals = []
        with tempfile.TemporaryDirectory() as root:
            for number in range(1, 4):
                folder = Path(root)/str(number); folder.mkdir()
                ledger = new_run(remote, folder, number)
                try:
                    if number == 1: ledger.tasks.add(request())
                    if number == 3:
                        remote.fail_pointer = True
                        with self.assertRaises(TimeoutError): checkpoint(ledger, 'orphan')
                        orphan = remote.upload_attempts[-1]
                        remote.fail_pointer = False
                        # A,B,O,C is the upload order, while A,B,C commit.
                    # Simulate a service with deferred pruning: all uploads
                    # A,B,O,C are still listed when the next process restores.
                    with patch.object(remote, 'prune_snapshots', return_value=0):
                        finals.append(checkpoint(ledger, f'run-{number}'))
                finally: ledger.tasks.close()
            folder = Path(root)/'4'; folder.mkdir()
            ledger = new_run(remote, folder, 4)
            try:
                current = checkpoint(ledger, 'run-4')
                self.assertEqual(set(remote.assets), {current, *finals[-2:]})
                self.assertNotIn(orphan, remote.assets)
            finally: ledger.tasks.close()

    def test_q1_legacy_pointer_never_infers_history_from_upload_order(self):
        remote = SyntheticReleases()
        with tempfile.TemporaryDirectory() as root:
            ledger = new_run(remote, root, 1)
            ledger.tasks.add(request()); ledger.save(force=True); live = ledger.current
            ledger.tasks.close()
            remote.assets = {'state-uncommitted.sqlite3': b'orphan', **remote.assets}
            remote.ids.clear()  # The orphan precedes the verified live target.
            remote.body.pop('run_history', None)
            with tempfile.TemporaryDirectory() as folder:
                ledger = new_run(remote, folder, 2)
                try:
                    self.assertEqual(ledger.run_history, [])
                    checkpoint(ledger, 'after-legacy')
                    self.assertIn(live, remote.assets)
                    self.assertNotIn('state-uncommitted.sqlite3', remote.assets)
                finally: ledger.tasks.close()

    def test_q2_equal_bytes_new_input_type_is_processed_and_preserved(self):
        remote = SyntheticReleases(); raw = control.canonical(request()).encode()
        remote.assets['request-consumed.json'] = raw
        with tempfile.TemporaryDirectory() as root:
            for number in range(1, 5):
                folder = Path(root)/str(number); folder.mkdir()
                ledger = new_run(remote, folder, number)
                try:
                    if number == 1: ledger.ingest()
                    checkpoint(ledger, f'run-{number}')
                    if number == 4:
                        remote.assets['review-new.json'] = raw
                        remote.prune_ingested(ledger.ingestion_receipts())
                        self.assertIn('review-new.json', set(remote.assets))
                        self.assertNotIn('request-consumed.json', set(remote.assets))
                        # A missing key leaves this distinct review pending.
                        ledger.ingest()
                        self.assertIn('review-new.json', remote.assets)
                        errors = ledger.ingest('fixture-key')
                        self.assertEqual([e['asset'] for e in errors], ['review-new.json'])
                finally: ledger.tasks.close()

    def inventory_at_cap(self, root, *, legacy_names=True):
        remote = SyntheticReleases()
        for number in range(1, 4):
            folder = Path(root)/str(number); folder.mkdir()
            ledger = new_run(remote, folder, number)
            try:
                if number == 1: ledger.tasks.add(request())
                checkpoint(ledger, f'run-{number}')
            finally: ledger.tasks.close()
        for number in range(1, 4):
            remote.assets[production_log_name(number, 8, tick=True, legacy=legacy_names)] = b'tick'
            for step in range(8): remote.assets[production_log_name(number, step, legacy=legacy_names)] = b'log'
        pending = {f'review-pending-{number:04}.json': b'{}' for number in range(970)}
        remote.assets.update(pending)
        self.assertEqual(len(remote.assets), 1000)
        return remote, pending

    def test_q3_snapshot_recovers_at_exact_1000_asset_inventory(self):
        with tempfile.TemporaryDirectory() as root:
            remote, pending = self.inventory_at_cap(root)
            protected = {n for n in remote.assets if n.startswith('state-')}
            rollback = {remote.body['snapshot']['name'],remote.body['run_history'][0]['name']}
            folder = Path(root)/'4'; folder.mkdir()
            with patch.dict(os.environ, {'GITHUB_RUN_ID':'4', 'GITHUB_RUN_ATTEMPT':'1',
                                        'GITHUB_WORKFLOW':'Coordinator', 'GITHUB_WORKFLOW_REF':''}):
                ledger = new_run(remote, folder, 4)
                try: current = checkpoint(ledger, 'capacity-recovered')
                finally: ledger.tasks.close()
            self.assertIn(current, remote.assets)
            self.assertEqual(len([n for n in remote.deleted if n.startswith('run-log-')]),8)
            self.assertEqual({n:remote.assets[n] for n in pending}, pending)
            self.assertEqual({n for n in remote.assets if n.startswith('state-')},{current,*rollback})
            self.assertTrue(rollback<=protected)

    def test_q3_newest_private_diagnostics_upload_at_exact_1000_asset_inventory(self):
        with tempfile.TemporaryDirectory() as root:
            remote, pending = self.inventory_at_cap(root)
            path = Path(root)/production_log_name(4); path.write_bytes(b'newest diagnostics')
            with patch.object(control, 'Releases', return_value=remote): run_log._upload(path, False)
            self.assertEqual(remote.assets[path.name], b'newest diagnostics')
            self.assertEqual({n:remote.assets[n] for n in pending}, pending)

    def test_b1_retained_run_step_snapshot_and_tick_uploads_evict_only_oldest_diagnostics(self):
        for kind in ('step', 'snapshot', 'tick'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as root:
                remote, pending = self.inventory_at_cap(root)
                protected = {n for n in remote.assets if n.startswith('state-')}
                oldest = min(n for n in remote.assets if n.startswith('tick-log-' if kind=='tick' else ('run-log-', 'tick-log-')))
                with patch.dict(os.environ, {'GITHUB_RUN_ID':'3', 'GITHUB_RUN_ATTEMPT':'1',
                                            'GITHUB_WORKFLOW':'Coordinator', 'GITHUB_WORKFLOW_REF':''}):
                    if kind == 'snapshot':
                        folder = Path(root)/'same-run'; folder.mkdir()
                        ledger = new_run(remote, folder, 3)
                        try: name = checkpoint(ledger, 'same-run-capacity')
                        finally: ledger.tasks.close()
                        protected.remove(next(n for n in protected if n not in remote.assets))
                    else:
                        path = Path(root)/production_log_name(3, 9, tick=kind=='tick')
                        path.write_bytes(b'newest diagnostics'); name = path.name
                        with patch.object(control, 'Releases', return_value=remote):
                            run_log._upload(path, kind=='tick')
                self.assertIn(name, remote.assets)
                self.assertEqual([n for n in remote.deleted if n.startswith(('run-log-', 'tick-log-'))], [oldest])
                self.assertTrue(protected <= remote.assets.keys())
                self.assertEqual({n:remote.assets[n] for n in pending}, pending)

    def test_b1_new_run_tick_upload_at_capacity_keeps_pending_and_snapshots(self):
        with tempfile.TemporaryDirectory() as root:
            remote, pending = self.inventory_at_cap(root)
            protected = {n for n in remote.assets if n.startswith('state-')}
            path = Path(root)/production_log_name(4, tick=True); path.write_bytes(b'new tick')
            with patch.object(control, 'Releases', return_value=remote): run_log._upload(path, True)
            self.assertIn(path.name, remote.assets)
            self.assertEqual(len(remote.deleted), 1)
            self.assertTrue(remote.deleted[0].startswith('tick-log-'))
            self.assertTrue(protected <= remote.assets.keys())
            self.assertEqual({n:remote.assets[n] for n in pending}, pending)

    def test_b1_oldest_diagnostic_is_selected_across_tick_and_step_classes(self):
        with tempfile.TemporaryDirectory() as root:
            remote, pending = self.inventory_at_cap(root,legacy_names=False)
            oldest_tick = min(n for n in remote.assets if n.startswith('tick-log-'))
            release = remote.release
            def created_times():
                value = release()
                for asset in value['assets']:
                    if asset['name']==oldest_tick:asset['created_at']='2026-11-01T00:00:00Z'
                return value
            path = Path(root)/production_log_name(3,9); path.write_bytes(b'diagnostics')
            with patch.object(remote, 'release', side_effect=created_times), \
                    patch.object(control, 'Releases', return_value=remote):
                run_log._upload(path,False)
            self.assertEqual(remote.deleted,[oldest_tick])
            self.assertIn(path.name,remote.assets)
            self.assertEqual({n:remote.assets[n] for n in pending},pending)

    def test_first_postdeploy_run_deletes_only_expected_preexisting_assets(self):
        for legacy in (True,False):
            with self.subTest(legacy_names=legacy), tempfile.TemporaryDirectory() as root:
                remote, pending = self.inventory_at_cap(root,legacy_names=legacy)
                original = set(remote.assets)
                oldest_snapshot = remote.body['run_history'][-1]['name']
                expected = {n for n in original if n.startswith(('run-log-20261101','tick-log-20261101'))}
                expected.add(oldest_snapshot)
                folder = Path(root)/'4'; folder.mkdir()
                with patch.dict(os.environ, {'GITHUB_RUN_ID':'4','GITHUB_RUN_ATTEMPT':'1',
                                            'GITHUB_WORKFLOW':'Coordinator','GITHUB_WORKFLOW_REF':''}), \
                        patch.object(control, 'Releases', return_value=remote):
                    ledger = new_run(remote,folder,4)
                    try: checkpoint(ledger,'first-postdeploy')
                    finally: ledger.tasks.close()
                    for step in range(8):
                        path = Path(root)/production_log_name(4,step); path.write_bytes(b'step diagnostics')
                        run_log._upload(path,False)
                    path = Path(root)/production_log_name(4,8,tick=True); path.write_bytes(b'tick diagnostics')
                    run_log._upload(path,True)
                self.assertEqual(set(remote.deleted)&original,expected)
                self.assertEqual(len(expected),10)  # 8 step logs, 1 tick log, 1 aged snapshot.
                self.assertEqual(len(remote.assets),1000)
                self.assertEqual({n:remote.assets[n] for n in pending},pending)

    def test_a1_post_commit_prune_failure_records_error_and_allows_side_effect(self):
        remote = SyntheticReleases(); effects = []
        with tempfile.TemporaryDirectory() as root:
            ledger = new_run(remote, root, 1)
            try:
                ledger.tasks.add(request())
                with patch.object(remote, 'prune_snapshots', side_effect=TimeoutError('temporary GitHub error')):
                    ledger.save(force=True)
                    effects.append(remote.body['snapshot']['name'])
                self.assertEqual(effects, [ledger.current])
                row = ledger.tasks.db.execute("SELECT payload FROM pipeline_state WHERE key='connector_errors'").fetchone()
                self.assertIsNotNone(row)
                self.assertIn('snapshot-prune', row[0])
            finally: ledger.tasks.close()

    def test_b2_committed_save_survives_sqlite_stderr_and_combined_reporting_failures(self):
        for fail_sql, fail_stderr in ((True,False), (False,True), (True,True)):
            with self.subTest(sqlite=fail_sql, stderr=fail_stderr), tempfile.TemporaryDirectory() as root:
                remote = SyntheticReleases(); ledger = new_run(remote, root, 1); effects = []
                db = ledger.tasks.db
                class DiagnosticDB:
                    def __getattr__(self, key): return getattr(db, key)
                    def execute(self, sql, *args):
                        if fail_sql and args and args[0][0] == 'connector_errors':
                            raise sqlite3.OperationalError('diagnostic insert unavailable')
                        return db.execute(sql, *args)
                try:
                    ledger.tasks.add(request())
                    with patch.object(remote, 'prune_snapshots', side_effect=TimeoutError('prune failed')), \
                            patch.object(ledger.tasks, 'db', DiagnosticDB()), \
                            patch.object(control.sys, 'stderr') as stderr:
                        if fail_stderr: stderr.write.side_effect = OSError('stderr unavailable')
                        self.assertTrue(ledger.save(force=True))
                        effects.append(remote.body['snapshot']['name'])
                    self.assertEqual(effects, [ledger.current])
                    self.assertIn(ledger.current, remote.assets)
                finally: ledger.tasks.close()

    def test_ar_each_workflow_retains_its_own_three_runs_of_production_logs(self):
        remote = SyntheticReleases(); expected = set()
        with tempfile.TemporaryDirectory() as root, patch.object(control, 'Releases', return_value=remote):
            for workflow, numbers in (('Coordinator',range(1,5)), ('Delivery',range(5,9)), ('Coverage',range(9,13))):
                names_by_run = []
                for number in numbers:
                    names = {production_log_name(number, step, workflow=workflow) for step in range(2)}
                    names_by_run.append(names)
                    for name in sorted(names):
                        path = Path(root)/name; path.write_bytes(b'diagnostics')
                        run_log._upload(path, False)
                expected.update(set().union(*names_by_run[-run_log.RUN_LOGS_KEPT:]))
                self.assertEqual(set(remote.assets), expected)

    def test_ar_legacy_run_names_age_out_as_workflow_bound_runs_arrive(self):
        remote = SyntheticReleases()
        legacy = {production_log_name(n, step, legacy=True) for n in range(1,4) for step in range(2)}
        remote.assets.update(dict.fromkeys(legacy,b'historical diagnostics'))
        with tempfile.TemporaryDirectory() as root, patch.object(control, 'Releases', return_value=remote):
            for number in range(4,7):
                name = production_log_name(number)
                path = Path(root)/name; path.write_bytes(b'diagnostics'); run_log._upload(path,False)
                self.assertEqual(len(legacy & remote.assets.keys()), 2*(6-number))

    def test_removed_ingested_digests_has_no_ledger_api(self):
        self.assertFalse(hasattr(control.Ledger, 'ingested_digests'))

    def test_removed_predecessor_alias_is_neither_loaded_nor_published(self):
        remote = SyntheticReleases()
        with tempfile.TemporaryDirectory() as root:
            ledger = new_run(remote, root, 1)
            try:
                ledger.tasks.add(request()); ledger.save(force=True)
                self.assertNotIn('predecessors', remote.body)
                self.assertFalse(hasattr(ledger, 'predecessors'))
            finally: ledger.tasks.close()
            # Old pointers may still contain the redundant alias.
            remote.body['predecessors'] = ['state-uncommitted.sqlite3']
            with tempfile.TemporaryDirectory() as restored:
                ledger = new_run(remote, restored, 2)
                try:
                    self.assertEqual(ledger.run_history, [])
                    self.assertFalse(hasattr(ledger, 'predecessors'))
                    self.assertFalse(hasattr(ledger, 'ingested_digests'))
                    ledger.save(force=True)
                    self.assertNotIn('predecessors', remote.body)
                finally: ledger.tasks.close()

    def test_a2_many_checkpoints_keep_prior_two_run_finals_and_do_not_age_inputs(self):
        remote = SyntheticReleases(); finals = []
        with tempfile.TemporaryDirectory() as root:
            for number in range(1, 4):
                folder = Path(root)/str(number); folder.mkdir()
                ledger = new_run(remote, folder, number)
                try:
                    if number == 1: ledger.tasks.add(request())
                    if number == 3:
                        raw = control.canonical(request('new-input')).encode()
                        remote.assets['request-current-run.json'] = raw
                        ledger.ingest()
                    for step in range(12):
                        current = checkpoint(ledger, f'run-{number}-step-{step}')
                        self.assertEqual(set(n for n in remote.assets if n.startswith('state-')),
                                         {current, *finals[-2:]})
                        remote.prune_ingested(ledger.ingestion_receipts())
                        if number == 3: self.assertIn('request-current-run.json', remote.assets)
                    finals.append(current)
                finally: ledger.tasks.close()

    def test_a2_run_logs_keep_all_logs_in_last_three_coordinator_runs(self):
        remote = SyntheticReleases(); runs = []
        with tempfile.TemporaryDirectory() as root, patch.object(control, 'Releases', return_value=remote):
            for number in range(1, 5):
                with patch.dict(os.environ, {'GITHUB_RUN_ID':str(number), 'GITHUB_RUN_ATTEMPT':'1'}):
                    runs.append(run_log.coordinator_run_id())
                    for step in range(10):
                        # Exercise the actual log name and session producer.
                        with redirect_stderr(io.StringIO()), redirect_stdout(io.StringIO()), \
                                patch.dict(os.environ, {'OUTREACH_PUBLIC_LOG':'1'}):
                            self.assertEqual(run_log.public_run(lambda: print('synthetic'), 'step'), 0)
                if number >= 3:
                    self.assertEqual(len([n for n in remote.assets if n.startswith('run-log-')]), 30)
                    self.assertEqual({run_log.log_run_id(n) for n in remote.assets},set(runs[-3:]))

    def test_a2_unchanged_new_run_advances_history_without_another_asset(self):
        remote = SyntheticReleases()
        with tempfile.TemporaryDirectory() as root:
            folder = Path(root)/'1'; folder.mkdir()
            ledger = new_run(remote, folder, 1)
            ledger.tasks.add(request()); ledger.save(force=True); current = ledger.current
            ledger.tasks.close()
            for number in (2, 3, 4):
                folder = Path(root)/str(number); folder.mkdir()
                ledger = new_run(remote, folder, number)
                try:
                    self.assertTrue(ledger.save(force=True))
                    self.assertFalse(ledger.save(force=True))
                    self.assertEqual(ledger.current,current)
                    self.assertEqual(len(ledger.run_history),min(number-1,2))
                    self.assertEqual(len(remote.assets),1)
                finally: ledger.tasks.close()

    def test_q2_reuploaded_name_with_a_new_asset_id_has_no_receipt(self):
        remote = SyntheticReleases(); raw = control.canonical(request()).encode()
        remote.assets['request-consumed.json'] = raw
        with tempfile.TemporaryDirectory() as root:
            for number in (1, 2, 3):
                folder = Path(root)/str(number); folder.mkdir()
                ledger = new_run(remote, folder, number)
                try:
                    if number==1:ledger.ingest()
                    checkpoint(ledger,f'run-{number}')
                    if number==3:
                        remote.ids['request-consumed.json']+=10000
                        self.assertEqual(remote.prune_ingested(ledger.ingestion_receipts()),0)
                        self.assertIn('request-consumed.json',set(remote.assets))
                finally: ledger.tasks.close()

    def test_a2_unverifiable_oldest_snapshot_never_authorizes_input_deletion(self):
        remote = SyntheticReleases(); raw = control.canonical(request()).encode()
        remote.assets['request-consumed.json'] = raw
        with tempfile.TemporaryDirectory() as root:
            for number in (1, 2, 3):
                folder = Path(root)/str(number); folder.mkdir()
                ledger = new_run(remote, folder, number)
                try:
                    if number==1:ledger.ingest()
                    checkpoint(ledger,f'run-{number}')
                    if number==3:
                        oldest=remote.body['run_history'][-1]
                        remote.assets[oldest['name']]=b'corrupt snapshot'
                        with self.assertRaisesRegex(ValueError,'snapshot mismatch'):
                            remote.prune_ingested(ledger.ingestion_receipts())
                        self.assertIn('request-consumed.json',set(remote.assets))
                finally: ledger.tasks.close()

    def test_s1_registered_multiline_secret_masks_native_crlf_and_excerpt_boundaries(self):
        secret = 'MULTILINE-SECRET-FIRST\nMULTILINE-SECRET-LAST'
        wire = secret.replace('\n', '\r\n').encode()
        public = io.StringIO(); captured = []
        def entry():
            run_log.register_secret(secret)
            os.write(1, wire + b'\n')
        with patch.dict(os.environ, {'OUTREACH_PUBLIC_LOG':'1'}), \
                patch.object(run_log, '_upload', side_effect=lambda path,tick:captured.append(path.read_bytes())), \
                redirect_stdout(public), redirect_stderr(public):
            self.assertEqual(run_log.public_run(entry, 'secret-test'), 0)
        self.assertNotIn(b'MULTILINE-SECRET', captured[0])
        limit = 1024; half = (limit-len(run_log.TRUNCATED))//2
        with tempfile.TemporaryDirectory() as root:
            raw = Path(root)/'raw'
            raw.write_bytes(b'A'*(half-10)+wire+b'x'*3000+wire+b'Z'*(half-10))
            excerpt = run_log._excerpt(raw, limit, {secret})
        self.assertNotIn(b'MULTILINE-', excerpt)
        self.assertNotIn(b'ECRET-LAST', excerpt)
        self.assertLessEqual(len(excerpt), limit)
