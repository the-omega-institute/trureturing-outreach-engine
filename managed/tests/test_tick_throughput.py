"""Slow-unit replay, admission fairness and observation cadence, without live I/O."""
import copy
import hashlib
import io
import json
from pathlib import Path
import subprocess
import tempfile
import threading
import unittest
from unittest.mock import patch
import zipfile

from managed import automation, control, delivery, publication, recheck
from managed.queue import canonical
from managed.sandbox_pool import SandboxPool, STALLED_SECONDS
from managed.tick import Tick
import test_artifact_preservation as artifact_tests
import test_automation as automation_tests
import test_owner_sent as owner_tests
import test_publication as publication_tests
import test_sandbox_pool as pool_tests
import test_tick_budget as budget_tests
from test_tasks import request


class ThroughputTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.folder = Path(self.temp.name)
        self.clock = budget_tests.Clock()
        self.remote = budget_tests.Remote()
        self.ledger = control.Ledger(self.remote, self.folder, clock=self.clock)
        self.addCleanup(lambda: self.ledger.tasks.close())
        self.ledger.tasks.clock = self.clock
        self.cma = budget_tests.Worker(self.clock)
        self.config = {'paused': False, 'kinds': {'research': True},
                       'max_active': 24, 'agent_template': {}}

    def running(self, name):
        row = self.ledger.tasks.add(request(name, name))
        return self.ledger.tasks.update(name, row['revision'], 'running', {
            'agent_id': 'agt_' + name, 'response_id': 'resp_' + name})

    def test_advance_reuses_rows_when_recovery_does_not_write(self):
        self.ledger.tasks.add(request('idle'))
        config = {**self.config, 'paused': True}
        with patch.object(self.ledger.tasks, 'all', wraps=self.ledger.tasks.all) as loads:
            control.advance(self.ledger, self.cma, config, self.folder)
        self.assertEqual(loads.call_count, 1)

    def test_advance_refreshes_snapshot_after_recovery_writes(self):
        self.running('recovered')
        def recover(ledger, *args, **kwargs):
            row = ledger.tasks.get('recovered')
            ledger.tasks.update(row['id'], row['revision'], 'held',
                                {**row['data'], 'execution_history': [{'agent_id': 'agt_history'}]})
        with patch.object(control, 'recover_execution_failures', side_effect=recover), \
                patch.object(self.ledger.tasks, 'all', wraps=self.ledger.tasks.all) as loads:
            control.advance(self.ledger, self.cma, self.config, self.folder)
        self.assertIn('agt_history', self.cma.retired)
        self.assertGreaterEqual(loads.call_count, 2)

    def test_advance_drops_snapshot_across_savepoint_rollback_and_same_generation_write(self):
        self.ledger.tasks.add(request('idle'))
        def speculative(ledger, *args, **kwargs):
            ledger.tasks.db.execute('SAVEPOINT speculative')
            ledger.tasks.db.execute("UPDATE tasks SET state='held' WHERE id='idle'")
        def recover(ledger, *args, **kwargs):
            ledger.tasks.db.execute('ROLLBACK TO speculative')
            ledger.tasks.db.execute("UPDATE tasks SET state='queued' WHERE id='idle'")
            ledger.tasks.db.execute('RELEASE speculative')
        def health(cma, *, rows):
            self.assertEqual(rows, self.ledger.tasks.all())
        with patch.object(control, 'recover_credit_failures', side_effect=speculative), \
                patch.object(control, 'recover_execution_failures', side_effect=recover), \
                patch.object(SandboxPool, 'health', side_effect=health) as check:
            control.advance(self.ledger, self.cma, {**self.config, 'paused': True}, self.folder)
        check.assert_called_once()

    def advance(self, seconds=600):
        tick = Tick(seconds, self.clock); tick.bind(self.ledger)
        control.advance(self.ledger, self.cma, self.config, self.folder)
        tick.finish(); self.ledger.save(force=True)
        return tick

    def restore(self):
        self.ledger.tasks.close()
        self.folder = self.folder / 'restored'; self.folder.mkdir()
        self.ledger = control.Ledger(self.remote, self.folder, clock=self.clock)
        self.ledger.tasks.clock = self.clock

    def artifact_result(self, count, *, outcome='completed', cost=10):
        row = self.running('bundle')
        blobs = {f'proof-{i:02d}.txt': str(i).encode() for i in range(count)}
        if outcome == 'hold':
            archive = io.BytesIO()
            raw = b'Working proof'
            with zipfile.ZipFile(archive, 'w') as bundle:
                bundle.writestr('working-proof.txt', raw)
                bundle.writestr('manifest.json', canonical([{'path': 'working-proof.txt',
                    'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()}]))
            blobs = {'checkpoint.zip': archive.getvalue(), **blobs}
        artifacts = [{**artifact_tests.artifact(name, raw,
                        'checkpoint' if name == 'checkpoint.zip' else 'evidence'),
                      'release_tag': 'outreach-task-bundle'} for name, raw in blobs.items()]
        result = {'schema_version': 1, 'task_id': row['id'], 'outcome': outcome,
                  'report': 'Checked result', 'packet': None, 'artifacts': artifacts}
        self.cma.response = lambda _: {'status': 'completed', 'output': [{'type': 'message',
            'content': [{'type': 'output_text', 'text': canonical(result)}]}]}
        reads = []
        def read(agent_id, path, *args):
            name = Path(path).name
            reads.append(name)
            self.clock.advance(cost)
            return blobs[name]
        self.cma.artifact = read
        return list(blobs), reads

    def test_forty_artifacts_resume_after_stage_budget_and_appear_in_work(self):
        names, reads = self.artifact_result(40)
        tick = Tick(75, self.clock)
        tick.order(['advance', 'later'])
        tick.current = 'advance'
        tick.reserve('later', 10)
        tick.bind(self.ledger)
        with patch.object(control, 'ARTIFACT_WORKERS', 1):
            control.advance(self.ledger, self.cma, self.config, self.folder)
            self.assertEqual(reads, names[:7])
            self.assertTrue(tick.exhausted)
            self.assertEqual(tick.pending['advance'], ['bundle'])
            self.assertEqual(self.cma.retired, [])
            # Restore the cutoff save before any final tick save can run.
            self.restore()
            row = self.ledger.tasks.get('bundle')
            self.assertEqual(row['state'], 'running')
            self.assertEqual(automation.work(self.ledger.tasks, self.folder)
                             ['artifact_collections_in_progress'], [
                {'task_id': 'bundle', 'total': 40, 'verified': 7,
                 'preserved': 0, 'seconds_spent': 70}])
            for _ in range(5):
                self.advance(seconds=65)
                self.restore()
        row = self.ledger.tasks.get('bundle')
        self.assertEqual(row['state'], 'awaiting-review')
        self.assertEqual(reads, names)
        self.assertEqual([item['name'] for item in row['data']['result']['artifacts']], names)
        self.assertEqual(automation.work(self.ledger.tasks, self.folder)
                         ['artifact_collections_in_progress'], [])

    def test_job_kill_after_periodic_save_resumes_from_remote_snapshot(self):
        names, reads = self.artifact_result(10)
        self.ledger.save(force=True)
        read = self.cma.artifact
        initial_generation = self.remote.body['generation']

        def interrupted(agent_id, path, *args):
            if Path(path).name == names[7]:
                raise SystemExit('simulated job kill before final save')
            return read(agent_id, path, *args)

        self.cma.artifact = interrupted
        tick = Tick(600, self.clock)
        tick.bind(self.ledger)
        with patch.object(control, 'ARTIFACT_WORKERS', 1), \
                patch.object(self.remote, 'pointer', wraps=self.remote.pointer) as saves:
            with self.assertRaises(SystemExit):
                control.advance(self.ledger, self.cma, self.config, self.folder)
            self.assertEqual(saves.call_count, 2)
        self.assertEqual(self.remote.body['generation'], initial_generation + 2)
        self.assertEqual(reads, names[:7])
        self.restore()
        progress = self.ledger.tasks.get('bundle')['data']['artifact_progress']
        self.assertEqual(list(progress.values()), ['verified'] * 6)
        self.cma.artifact = read
        with patch.object(control, 'ARTIFACT_WORKERS', 1):
            self.advance()
        self.assertEqual(reads, names[:7] + names[6:])
        self.assertEqual(self.ledger.tasks.get('bundle')['state'], 'awaiting-review')

    def test_unit_cap_stops_queued_cma_reads_even_with_stage_time_left(self):
        _, reads = self.artifact_result(16, cost=50)
        with patch.dict('os.environ', {'OUTREACH_RESULT_UNIT_BUDGET_SECONDS': '140'}):
            tick = self.advance()
        self.assertEqual(len(reads), 3)
        self.assertFalse(tick.exhausted)
        self.assertEqual(tick.pending['advance'], ['bundle'])
        self.restore()
        row = self.ledger.tasks.get('bundle')
        self.assertEqual(row['state'], 'running')
        self.assertEqual(row['data']['artifact_collection']['seconds_spent'], 150)
        self.assertTrue({path for _, path in self.remote.files} <= set(reads[:2]))
        self.assertIn(reads[0], {path for _, path in self.remote.files})
        self.assertIsNone(recheck.next_at(self.ledger.tasks, 'result', 'bundle', 'resp_bundle'))

    def test_unit_deadline_caps_release_readback_and_resumes_from_preserved(self):
        names, reads = self.artifact_result(1)
        download = self.remote.run

        def timed_download(*args):
            def stalled(command, *, timeout, **kwargs):
                self.assertEqual(timeout, 15)
                self.clock.advance(timeout)
                raise subprocess.TimeoutExpired(command, timeout)

            with patch('managed.control.subprocess.run', side_effect=stalled):
                return control.Releases.run(self.remote, *args)

        self.remote.run = timed_download
        with patch.dict('os.environ', {'OUTREACH_RESULT_UNIT_BUDGET_SECONDS': '25'}):
            self.advance()
        self.restore()
        self.assertEqual(automation.work(self.ledger.tasks, self.folder)
                         ['artifact_collections_in_progress'], [
            {'task_id': 'bundle', 'total': 1, 'verified': 0,
             'preserved': 1, 'seconds_spent': 25}])
        self.remote.run = download
        self.advance()
        self.assertEqual(reads, names)
        self.assertEqual(self.ledger.tasks.get('bundle')['state'], 'awaiting-review')

    def test_without_tick_the_unit_cap_is_inactive(self):
        names, reads = self.artifact_result(6, cost=50)
        with patch.dict('os.environ', {'OUTREACH_RESULT_UNIT_BUDGET_SECONDS': '1'}):
            control.advance(self.ledger, self.cma, self.config, self.folder)
        self.assertCountEqual(reads, names)
        self.assertEqual(self.ledger.tasks.get('bundle')['state'], 'awaiting-review')

    def test_hold_checkpoint_and_final_artifacts_share_budget_and_resume(self):
        names, reads = self.artifact_result(3, outcome='hold')
        with patch.object(control, 'ARTIFACT_WORKERS', 1):
            self.advance(seconds=15)
            self.restore()
            row = self.ledger.tasks.get('bundle')
            self.assertEqual(row['state'], 'running')
            self.assertEqual(reads, names[:2])
            self.assertEqual(self.cma.retired, [])
            self.advance()
        self.assertEqual(reads, names)
        row = self.ledger.tasks.get('bundle')
        self.assertEqual(row['state'], 'held')
        archive, *deliverables = row['data']['result']['artifacts']
        self.assertEqual((archive['name'], archive['role']), ('checkpoint.zip', 'checkpoint'))
        self.assertEqual(len(deliverables), 3)
        self.assertEqual(self.cma.retired, ['agt_bundle'])

    def test_result_window_stops_before_further_response_reads_and_rotates(self):
        for name in ('a', 'b', 'c'): self.running(name)
        self.cma.cost = 65
        tick = self.advance()
        self.assertEqual(self.cma.reads, ['a', 'b'])
        self.assertIn('c', tick.pending['advance'])
        self.restore()
        self.advance()
        self.assertEqual(self.cma.reads, ['a', 'b', 'c'])

    def test_admission_precedes_slow_collection_with_free_slot_or_fourth_sandbox(self):
        self.config['sandbox_pool'] = {'max_sandboxes': 4, 'agents_per_sandbox': 6}
        for stalled in (False, True):
            with self.subTest(stalled=stalled):
                cma = pool_tests.PoolCMA(); self.cma = cma
                count = 3 if stalled else 1
                entries = [{'sandbox_id': 'sbx_pool_' + str(i), 'workspace_id': 'wks_' + str(i),
                            'state': 'credential_rotating' if stalled else 'active',
                            'unhealthy_since': self.clock() - STALLED_SECONDS - 1}
                           for i in range(count)]
                self.ledger.tasks.db.execute('INSERT OR REPLACE INTO pipeline_state VALUES(?,?)',
                                             ('sandbox_pool', canonical(entries)))
                for entry in entries:
                    cma.statuses[entry['sandbox_id']] = {'status': entry['state']}
                name = 'slow-' + str(stalled)
                self.running(name)
                queued = self.ledger.tasks.add(request('queued-' + str(stalled), 'independent-' + str(stalled)))
                events = []
                admit = cma.admit
                def admission(*args):
                    events.append('admit')
                    return admit(*args)
                cma.admit = admission
                def response(identity):
                    if identity != 'resp_' + name: return {'status': 'in_progress'}
                    result = {'schema_version': 1, 'task_id': name, 'outcome': 'completed',
                              'report': 'Checked result', 'packet': None, 'artifacts': []}
                    return {'status': 'completed', 'output': [{'type': 'message', 'content': [
                        {'type': 'output_text', 'text': canonical(result)}]}]}
                cma.response = response
                def collect(*args, **kwargs):
                    events.append('collect'); self.clock.advance(200)
                    return True
                with patch('managed.control.verify_artifacts', side_effect=collect):
                    self.advance(seconds=150)
                self.assertIn('admit', events)
                self.assertLess(events.index('admit'), events.index('collect'))
                self.assertEqual(self.ledger.tasks.get(queued['id'])['state'], 'running')
                created = next(iter(cma.creates.values()))
                if stalled:
                    self.assertNotIn('workspace_id', created)
                else:
                    self.assertEqual(created['workspace_id'], 'wks_0')

    def test_full_capacity_is_retried_after_collection_and_stop(self):
        self.config['max_active'] = 1
        self.running('done'); self.ledger.tasks.add(request('queued', 'other'))
        self.cma.completed = True
        response = self.cma.response
        self.cma.response = lambda identity: (response(identity) if identity == 'resp_done'
                                               else {'status': 'in_progress'})
        self.advance()
        self.assertEqual(self.ledger.tasks.get('done')['state'], 'awaiting-review')
        self.assertEqual(self.ledger.tasks.get('queued')['state'], 'running')
        self.assertEqual(len(self.cma.creates), 1)

    def test_long_admission_pass_does_not_starve_completed_collection(self):
        # 2026-11-12..13: with ~190 queued rows the admission pass outlasted the
        # result window, so completed results stayed deferred and kept slots.
        self.config['max_active'] = 1
        self.running('done'); self.ledger.tasks.add(request('queued', 'other'))
        self.cma.completed = True
        response = self.cma.response
        self.cma.response = lambda identity: (response(identity) if identity == 'resp_done'
                                               else {'status': 'in_progress'})
        matches = control.matches_workflow
        def slow_admission_check(row, *args, **kwargs):
            if row['id'] == 'queued': self.clock.advance(130)
            return matches(row, *args, **kwargs)
        with patch('managed.control.matches_workflow', side_effect=slow_admission_check):
            self.advance()
        self.assertEqual(self.ledger.tasks.get('done')['state'], 'awaiting-review')

    def test_budget_deferred_collection_precedes_another_admission_pass(self):
        self.config['max_active'] = 1
        self.running('done'); self.ledger.tasks.add(request('queued', 'other'))
        self.cma.completed = True
        matches = control.matches_workflow
        def slow_admission_check(row, *args, **kwargs):
            if row['id'] == 'queued': self.clock.advance(6)
            return matches(row, *args, **kwargs)
        with patch('managed.control.matches_workflow', side_effect=slow_admission_check):
            self.advance(seconds=5)
            self.assertEqual(self.ledger.tasks.get('done')['state'], 'running')
            self.assertIn('completed_response', self.ledger.tasks.get('done')['data'])
            self.restore(); self.advance(seconds=5)
        self.assertEqual(self.ledger.tasks.get('done')['state'], 'awaiting-review')
        self.assertEqual(self.cma.reads, ['done'])

    def test_historical_cleanup_limit_defers_remaining_agents_fairly(self):
        row = self.ledger.tasks.add(request('finished', 'finished'))
        self.ledger.tasks.update(row['id'], row['revision'], 'verified', {
            'execution_history': [{'agent_id': 'agt_old_' + str(i)} for i in range(8)]})
        calls = []
        def retire(agent, *args):
            calls.append(agent)
            return {'status': 'deleting'}
        self.cma.retire = retire
        self.advance()
        self.assertEqual(calls, ['agt_old_' + str(i) for i in range(6)])
        self.restore(); self.advance()
        self.assertEqual(calls[6:8], ['agt_old_6', 'agt_old_7'])
        self.assertEqual(len(calls), 12)

    def test_partial_preservation_and_timeout_backoff_survive_snapshot_restore(self):
        self.running('note')
        blobs = {'a.txt': b'first', 'b.txt': b'second'}
        artifacts = [artifact_tests.artifact(name, raw) for name, raw in blobs.items()]
        for item in artifacts: item['release_tag'] = 'outreach-task-note'
        result = {'schema_version': 1, 'task_id': 'note', 'outcome': 'completed',
                  'report': 'Checked result', 'packet': None, 'artifacts': artifacts}
        reads = []; downloads = []; fail = True
        def response(identity):
            reads.append(identity)
            return {'status': 'completed', 'output': [{'type': 'message', 'content': [
                {'type': 'output_text', 'text': canonical(result)}]}]}
        self.cma.response = response
        source_reads = []
        def artifact(agent, path, *args):
            source_reads.append(path); return blobs[Path(path).name]
        self.cma.artifact = artifact
        download = self.remote.run
        def readback(*args):
            name = args[args.index('--pattern') + 1]; downloads.append(name)
            if fail and name == 'b.txt': raise TimeoutError('Release readback timed out')
            return download(*args)
        self.remote.run = readback
        self.advance()
        row = self.ledger.tasks.get('note')
        self.assertEqual(row['state'], 'running')
        self.assertCountEqual(row['data']['artifact_progress'].values(), ['verified', 'preserved'])
        self.assertFalse(self.cma.retired)
        self.restore(); self.advance()
        self.assertEqual(reads, ['resp_note'])
        self.assertEqual(downloads, ['a.txt', 'b.txt'])
        self.clock.advance(recheck.RECHECK_SECONDS); fail = False
        self.advance()
        row = self.ledger.tasks.get('note')
        self.assertEqual(row['state'], 'awaiting-review')
        self.assertNotIn('artifact_progress', row['data'])
        self.assertNotIn('completed_response', row['data'])
        self.assertEqual(source_reads, ['deliverables/a.txt', 'deliverables/b.txt'])
        self.assertEqual(downloads, ['a.txt', 'b.txt', 'b.txt'])
        self.assertEqual(self.cma.retired, ['agt_note'])

    def test_slow_retirement_is_not_replayed_each_tick(self):
        row = self.running('done')
        self.ledger.tasks.update(row['id'], row['revision'], 'awaiting-review', {
            **row['data'], 'result': {'report': 'Checked'}})
        calls = []
        def retire(*args):
            calls.append(args); self.clock.advance(30)
            return {'stopped': True, 'deleted': False, 'status': 'deleting'}
        self.cma.retire = retire
        self.advance(); self.restore(); self.advance()
        self.assertEqual(len(calls), 1)
        self.clock.advance(recheck.RECHECK_SECONDS); self.advance()
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0], calls[1])


class ArtifactResumeTests(unittest.TestCase):
    def test_packet_is_parsed_again_from_release_and_changed_hash_is_not_cached(self):
        releases = artifact_tests.FakeReleases(); calls = []
        raw = b'{"body":"original"}'
        item = artifact_tests.artifact('packet.json', raw, 'mail-packet')
        cma = artifact_tests.FakeCMA({'deliverables/packet.json': raw})
        read = cma.artifact
        def counted(*args): calls.append(args); return read(*args)
        cma.artifact = counted
        progress = {}
        with tempfile.TemporaryDirectory() as folder:
            for _ in range(2):
                result = {'artifacts': [copy.deepcopy(item)], 'packet': {'artifact': 'packet.json'}}
                control.verify_artifacts(result, releases, folder, cma, 'agt_x', progress=progress)
                self.assertEqual(result['packet'], {'body': 'original'})
            self.assertEqual(len(calls), 1)
            # Even a previously verified packet is expanded only from a fresh,
            # checksum-verified Release readback, never from cached JSON.
            releases.files[item['release_tag'], item['name']] = b'X' * len(raw)
            with self.assertRaises(ValueError):
                control.verify_artifacts({'artifacts': [copy.deepcopy(item)],
                                          'packet': {'artifact': 'packet.json'}},
                                         releases, folder, cma, 'agt_x', progress=progress)
            self.assertEqual(len(calls), 1)
            # A different immutable digest requires a fresh CMA read and Release readback.
            changed = b'{"body":"new"}'; cma.blobs['deliverables/packet.json'] = changed
            item.update(bytes=len(changed), sha256=hashlib.sha256(changed).hexdigest())
            result = {'artifacts': [item], 'packet': {'artifact': 'packet.json'}}
            control.verify_artifacts(result, releases, folder, cma, 'agt_x', progress=progress)
            self.assertEqual(len(calls), 2)
            self.assertEqual(result['packet'], {'body': 'new'})

    def test_failed_checksum_is_never_cached_as_verified(self):
        item = artifact_tests.artifact('a.txt', b'expected')
        progress = {}
        with tempfile.TemporaryDirectory() as folder, self.assertRaises(ValueError):
            control.verify_artifacts({'artifacts': [item]}, artifact_tests.FakeReleases(), folder,
                                     artifact_tests.FakeCMA({'deliverables/a.txt': b'wrong'}), 'agt_x',
                                     progress=progress)
        self.assertNotIn('verified', progress.values())

    def test_parallel_successes_are_checkpointed_on_coordinator_thread_despite_failure(self):
        releases = artifact_tests.FakeReleases()
        blobs = {'deliverables/' + name: name.encode() for name in ('a.txt', 'b.txt', 'c.txt')}
        items = [artifact_tests.artifact(Path(path).name, raw) for path, raw in blobs.items()]
        progress = {}; checkpoints = []; fail = True
        download = releases.run
        def readback(*args):
            if fail and args[args.index('--pattern') + 1] == 'b.txt':
                raise TimeoutError('One parallel readback timed out')
            return download(*args)
        releases.run = readback
        cma = artifact_tests.FakeCMA(blobs)
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaises(TimeoutError):
                control.verify_artifacts({'artifacts': copy.deepcopy(items)}, releases, folder, cma, 'agt_x',
                                         progress=progress,
                                         checkpoint=lambda: checkpoints.append(threading.get_ident()))
            self.assertCountEqual(progress.values(), ['verified', 'verified', 'preserved'])
            self.assertEqual(set(checkpoints), {threading.get_ident()})
            fail = False
            with patch.object(cma, 'artifact', side_effect=AssertionError('Already uploaded')):
                control.verify_artifacts({'artifacts': items}, releases, folder, cma, 'agt_x', progress=progress)
            self.assertEqual(set(progress.values()), {'verified'})


class ObservationCadenceTests(unittest.TestCase):
    def fixture(self, cls):
        fixture = cls(); fixture.setUp(); self.addCleanup(fixture.doCleanups)
        fixture.clock = budget_tests.Clock()
        fixture.ledger.clock = fixture.clock
        fixture.ledger.tasks.clock = fixture.clock
        return fixture

    def test_missing_owner_draft_skips_all_graph_reads_until_recheck_is_due(self):
        fixture = self.fixture(owner_tests.OwnerSentTests)
        fixture.graph.drafts.clear()
        fixture.update(draft_revision_requests=[{'reason': 'Must stay pending'}])
        fixture.tick(); fixture.assert_missing()
        calls = list(fixture.graph.calls)
        fixture.box.sent = None
        fixture.tick()
        self.assertEqual(fixture.graph.calls, calls)
        self.assertNotIn('draft_revision_intent', fixture.row()['data'])
        fixture.clock.advance(delivery.OWNER_SENT_RECHECK_SECONDS)
        fixture.tick()
        self.assertGreater(len(fixture.graph.calls), len(calls))

    def test_live_owner_draft_rechecks_on_expiry_or_changed_receipt(self):
        fixture = self.fixture(owner_tests.OwnerSentTests)
        fixture.tick(); calls = list(fixture.graph.calls)
        message = fixture.outlook_send()
        fixture.tick()
        self.assertEqual(fixture.graph.calls, calls)
        self.assertEqual(fixture.row()['state'], 'owner-draft')
        fixture.clock.advance(delivery.OWNER_SENT_RECHECK_SECONDS)
        fixture.tick(); fixture.assert_sent(message)

        fixture.task_id = fixture.add_task('other-owner', 'draft-2')
        fixture.tick(); calls = list(fixture.graph.calls)
        fixture.update(draft_receipt={**fixture.row()['data']['draft_receipt'], 'remote_id': 'replacement'})
        fixture.tick()
        self.assertGreater(len(fixture.graph.calls), len(calls))

    def test_slow_duplicate_publication_does_not_redownload_each_tick(self):
        fixture = self.fixture(publication_tests.PublicationTests)
        fixture.prepared(); fixture.run_publication()
        duplicate = fixture.prepared('duplicate')
        package = publication.package
        def slow(*args):
            fixture.clock.advance(25); return package(*args)
        with patch('managed.publication.package', side_effect=slow) as packages:
            fixture.run_publication(); fixture.run_publication()
            self.assertEqual(packages.call_count, 1)
            self.assertEqual(fixture.row('duplicate'), duplicate)
            fixture.clock.advance(recheck.RECHECK_SECONDS)
            fixture.run_publication()
            self.assertEqual(packages.call_count, 2)

    def test_publication_timeout_yields_and_changed_decision_rechecks(self):
        fixture = self.fixture(publication_tests.PublicationTests)
        fixture.prepared()
        with patch.object(fixture.gh, 'register_note', side_effect=TimeoutError('GitHub timed out')) as register:
            fixture.run_publication(); fixture.run_publication()
            self.assertEqual(register.call_count, 1)
            fixture.table['short_note_publication']['decision_ref'] += '-new'
            fixture.write_decisions(); fixture.run_publication()
            self.assertEqual(register.call_count, 2)
        self.assertNotIn('mathesis_delivery', fixture.row()['data'])

    def _owner_draft_delivery_fixture(self):
        fixture = self.fixture(automation_tests.AutomationTests)
        contacts = json.loads((fixture.root / 'tables' / 'contacts.json').read_text())
        contacts['contacts'].append({'id': 'contact-owner', 'name': 'Owner',
                                     'email': 'owner@example.invalid'})
        (fixture.root / 'tables' / 'contacts.json').write_text(json.dumps(contacts))
        owner = fixture.add('owner-fail')
        owner_result = fixture.result(owner, route='owner')
        owner_result['packet']['to'] = ['owner@example.invalid']
        fixture.review(owner, owner_result)
        fixture.route()
        sender = fixture.add('auto-send')
        fixture.review(sender, fixture.result(sender))
        fixture.route()
        fixture.clock = budget_tests.Clock()
        fixture.ledger.clock = fixture.clock
        fixture.ledger.tasks.clock = fixture.clock
        return fixture

    def test_unsaved_owner_draft_failure_is_deferred_while_reviewed_send_runs(self):
        fixture = self._owner_draft_delivery_fixture()

        class Mailbox(automation_tests.Mailbox):
            def __init__(self):
                super().__init__()
                self.preflights = []

            def preflight(self, packet, request):
                self.preflights.append(request['id'])
                if request['id'] == 'owner-fail':
                    fixture.clock.advance(25)
                    raise RuntimeError('persistent owner failure')
                return super().preflight(packet, request)

        box = Mailbox()
        tick = Tick(30, fixture.clock)
        tick.bind(fixture.ledger)
        delivery.advance(fixture.ledger, fixture.cfg, fixture.root, box)
        self.assertEqual(box.preflights, ['owner-fail', 'auto-send'])
        self.assertEqual(len(box.calls), 1)
        self.assertEqual(fixture.ledger.tasks.get('owner-fail')['data']['last_error'],
                         'persistent owner failure')

        tick = Tick(30, fixture.clock)
        tick.bind(fixture.ledger)
        delivery.advance(fixture.ledger, fixture.cfg, fixture.root, box)
        self.assertEqual(box.preflights, ['owner-fail', 'auto-send'])
        self.assertEqual(fixture.ledger.tasks.get('auto-send')['state'], 'verified')

    def test_changed_unsaved_owner_draft_binding_bypasses_backoff(self):
        fixture = self._owner_draft_delivery_fixture()

        class Mailbox(automation_tests.Mailbox):
            def __init__(self):
                super().__init__()
                self.preflights = []

            def preflight(self, packet, request):
                self.preflights.append(request['id'])
                raise RuntimeError('persistent owner failure')

        box = Mailbox()
        tick = Tick(30, fixture.clock)
        tick.bind(fixture.ledger)
        delivery.advance(fixture.ledger, fixture.cfg, fixture.root, box)
        self.assertEqual(box.preflights.count('owner-fail'), 1)

        row = fixture.ledger.tasks.get('owner-fail')
        fixture.ledger.tasks.update(row['id'], row['revision'], row['state'],
                                    {**row['data'], 'thread_inputs': {
                                        'latest_inbound_message_id': '<new-incoming>'}})
        tick = Tick(30, fixture.clock)
        tick.bind(fixture.ledger)
        delivery.advance(fixture.ledger, fixture.cfg, fixture.root, box)
        self.assertEqual(box.preflights.count('owner-fail'), 2)


if __name__ == '__main__': unittest.main()
