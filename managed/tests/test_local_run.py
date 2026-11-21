"""Local execution uses immutable task inputs and the coordinator's trust boundary."""
import copy
from contextlib import redirect_stdout
import fcntl
import hashlib
import io
import json
from pathlib import Path
import subprocess
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch

from fixture_config import test_config
from fixture_config import use_cli_config
from managed import control, local_run
from managed.queue import canonical, digest
from test_tasks import CMA, Releases, request, use_fixture_workflow_revisions


class LocalReleases(Releases):
    repo = 'fixture/repo'
    tag = 'outreach-control'

    def __init__(self):
        super().__init__()
        self.files = {}
        self.corrupt = False

    def ensure(self):
        pass

    def preserve_task(self, path, tag):
        self.files[tag, path.name] = path.read_bytes()

    def run(self, *args):
        raw = self.files[args[2], args[args.index('--pattern') + 1]]
        if self.corrupt:
            raw = b'x' * len(raw)
        (Path(args[args.index('--dir') + 1]) / args[args.index('--pattern') + 1]).write_bytes(raw)


class LocalRunTests(unittest.TestCase):
    def setUp(self):
        use_fixture_workflow_revisions(self)
        self.temp = tempfile.TemporaryDirectory()
        self.directory = Path(self.temp.name)
        self.remote = LocalReleases()
        self.ledger = control.Ledger(self.remote, self.directory)
        self.row = self.ledger.tasks.add(request())
        self.result = {'schema_version': 1, 'task_id': 'job-1', 'outcome': 'completed',
                       'report': 'Checked the selected question.', 'artifacts': [], 'packet': None}

    def tearDown(self):
        self.ledger.tasks.close()
        self.temp.cleanup()

    def envelope(self, **changes):
        value = {'schema_version': 1, 'task_id': self.row['id'], 'task_revision': self.row['revision'],
                 'request_hash': self.row['request_hash'], 'executor': 'local-codex',
                 'session_id': 'fixture-session', 'context': {}, 'result': copy.deepcopy(self.result)}
        value.update(changes)
        return control.sign_review(value, 'fixture-key')

    def failed_row(self, *, retired=True, suggested='local-codex'):
        data = {**self.row['data'], 'agent_id': 'agt-failed', 'response_id': 'resp-failed',
                'command_key': 'command-failed',
                'recovery': {'status': 'needs-operator', 'error_class': 'request-too-large',
                             'suggested_exit': suggested, 'reason': 'Take over this failed task'},
                'cleanup': {'stopped': retired, 'deleted': retired}}
        self.row = self.ledger.tasks.update(self.row['id'], self.row['revision'], 'failed', data)
        return self.row

    def takeover_envelope(self, **changes):
        row = self.row
        value = {'takeover': True,
                 'failed_execution': local_run.failed_execution_identity(row)}
        value.update(changes)
        value = self.envelope(**value)
        return value

    def stage(self, envelope):
        name = 'local-result-' + digest(envelope) + '.json'
        self.remote.assets[name] = canonical(envelope).encode()
        return name

    def test_list_mail_first_oldest_first_and_only_unstarted(self):
        tasks = self.ledger.tasks
        reply = tasks.add(request('reply-old', 'other', 'reply'))
        first = tasks.add(request('first-new', 'third', 'firstcontact'))
        waiting = tasks.add(request('credit-wait', 'fourth', 'reply'))
        tasks.update(waiting['id'], 1, 'credit-wait', {'credit_waiting': True})
        for index, field in enumerate(('agent_id', 'create_key', 'command_key', 'response_id', 'prompt', 'result', 'review')):
            row = tasks.add(request('started-' + str(index), 'x', 'reply'))
            tasks.update(row['id'], 1, 'queued', {field: 'retained-handle'})
        done = tasks.add(request('done', 'x', 'reply'))
        tasks.update(done['id'], 1, 'awaiting-review', {'result': {}})
        parent = tasks.add(request('parent', 'other', 'reply'))
        tasks.update(parent['id'], 1, 'revising', {'continuation_id': reply['id'], 'result': {}})
        created = {'reply-old': 1, 'first-new': 2, 'credit-wait': 3, 'job-1': 0}
        rows = tasks.all()
        self.assertEqual([r['id'] for r in local_run.selection(rows, created=created)],
                         ['reply-old', 'first-new', 'credit-wait', 'job-1'])
        self.assertEqual([r['id'] for r in local_run.selection(rows, {'firstcontact'})], ['first-new'])
        self.assertEqual(local_run.target(rows, 'parent')['id'], 'reply-old')
        with self.assertRaisesRegex(ValueError, 'already'):
            local_run.target(rows, 'started-0')

    def test_prompt_contains_exact_cma_prompt_including_private_context(self):
        row = copy.deepcopy(self.row)
        row['kind'] = row['request']['kind'] = 'reply'
        row['data'] = {'thread_inputs': {'thread': [{'id': 'fixture-inbound'}]},
                       'mailbox_history': {'complete': True, 'messages': []}}
        config = {'github': {'connection': 'fixture'}}
        capacity = {'available': 4, 'workers': 4, 'cooling': 0, 'queued': 0}
        expected = control.task_prompt(row, config['github'], capacity)
        self.assertEqual(local_run.prompt(row, config, capacity), local_run.PREAMBLE + expected)
        self.assertIn(canonical(row['data']['thread_inputs']), expected)
        self.assertIn(canonical(row['data']['mailbox_history']), expected)

    def test_missing_reply_history_uses_existing_local_adapter_and_resolver_without_db_write(self):
        row = self.ledger.tasks.add(request('reply-local', 'other', 'reply'))
        mailbox = Mock()
        with patch('managed.affairs._live_mailbox', return_value=mailbox) as adapter, \
             patch('managed.firstcontact_history.resolve_reply') as resolve:
            prepared = local_run.prepare(row, self.ledger, {})
        adapter.assert_called_once_with(control.ROOT, {})
        mailbox.begin_history_tick.assert_called_once()
        resolve.assert_called_once()
        self.assertIs(resolve.call_args.args[0], prepared)
        self.assertEqual(self.ledger.tasks.get(row['id']), row)

    def test_existing_context_is_not_refetched(self):
        row = copy.deepcopy(self.row)
        row['kind'] = row['request']['kind'] = 'reply'
        row['request']['workflow_hash'] = control.workflow('reply')
        row['data']['mailbox_history'] = {'complete': True}
        with patch('managed.affairs._live_mailbox') as adapter:
            self.assertEqual(local_run.prepare(row, self.ledger, {}), row)
        adapter.assert_not_called()

    def test_signed_ingest_is_reviewable_durable_idempotent_and_never_dispatches_cma(self):
        envelope = self.envelope()
        self.stage(envelope)
        self.assertEqual(self.ledger.ingest('fixture-key'), [])
        row = self.ledger.tasks.get(self.row['id'])
        self.assertEqual(row['state'], 'awaiting-review')
        self.assertEqual(row['data']['result'], self.result)
        self.assertEqual(row['data']['executor'], 'local-codex')
        self.assertEqual(row['data']['local_session_id'], 'fixture-session')
        self.assertEqual(self.ledger.tasks.reviews()[0]['binding']['result_hash'], digest(self.result))
        self.assertEqual(self.ledger.ingest('fixture-key'), [])
        self.assertEqual(self.ledger.tasks.get(row['id'])['revision'], row['revision'])
        cma = CMA()
        cfg = {'paused': False, 'kinds': {'research': True}, 'max_active': 2, 'agent_template': {}}
        control.advance(self.ledger, cma, cfg, self.directory)
        self.assertFalse(cma.creates)
        self.assertFalse(cma.commands)
        with tempfile.TemporaryDirectory() as folder:
            restored = control.Ledger(self.remote, folder)
            try:
                self.assertEqual(restored.tasks.get(row['id']), row)
            finally:
                restored.tasks.close()

    def test_signature_request_revision_and_started_rejections_are_admission_errors(self):
        for changes, key, reason in [({}, 'wrong-key', 'signature'),
                                     ({'request_hash': 'b' * 64}, 'fixture-key', 'hash'),
                                     ({'task_revision': 99}, 'fixture-key', 'revision')]:
            with self.subTest(reason=reason):
                self.remote.assets = {}
                self.stage(self.envelope(**changes))
                errors = self.ledger.ingest(key)
                self.assertEqual(len(errors), 1)
                self.assertIn(reason, errors[0]['error'])
                self.assertEqual(self.ledger.tasks.get(self.row['id'])['state'], 'queued')
        self.remote.assets = {}
        self.row = self.ledger.tasks.update(self.row['id'], 1, 'admitting', {'agent_id': 'cma-agent'})
        self.stage(self.envelope())
        errors = self.ledger.ingest('fixture-key')
        self.assertIn('already started', errors[0]['error'])
        self.assertEqual(self.ledger.tasks.get(self.row['id'])['data'], {'agent_id': 'cma-agent'})

    def test_cma_claim_after_local_snapshot_rejects_stale_result(self):
        envelope = self.envelope()
        self.ledger.tasks.claim(self.row['id'], 1)
        self.stage(envelope)
        self.assertIn('revision', self.ledger.ingest('fixture-key')[0]['error'])
        self.assertEqual(self.ledger.tasks.get(self.row['id'])['state'], 'provisioning')

    def test_same_revision_queued_row_with_uncertain_intent_is_rejected(self):
        self.row = self.ledger.tasks.update(self.row['id'], 1, 'queued', {'create_key': 'original'})
        with self.assertRaisesRegex(ValueError, 'already started'):
            local_run.ingest(self.ledger, self.envelope(), 'fixture-key')

    def test_revising_parent_runs_child_with_feedback_and_does_not_replace_parent(self):
        from managed.automation import reconcile
        row = self.ledger.tasks.update(self.row['id'], 1, 'awaiting-review', {'result': self.result})
        verdict = control.sign_review({'binding': self.ledger.tasks.review_binding(row),
            'reviewer': 'local-claude-loop', 'verdict': 'BOUNCE', 'reason': 'Clarify the scope.',
            'evidence_ref': 'fixture:review'}, 'fixture-key')
        self.ledger.tasks.verdict(verdict, 'fixture-key')
        cfg = test_config()
        reconcile(self.ledger, cfg, control.ROOT, revision='a' * 40)
        parent = self.ledger.tasks.get(row['id'])
        self.row = local_run.target(self.ledger.tasks.all(), parent['id'])
        self.assertEqual(self.row['request']['inputs']['review_feedback'], verdict)
        self.result['task_id'] = self.row['id']
        accepted = local_run.ingest(self.ledger, self.envelope(), 'fixture-key')
        self.assertEqual(accepted['state'], 'awaiting-review')
        self.assertEqual(self.ledger.tasks.get(parent['id']), parent)

    def artifact(self):
        path = self.directory / 'deliverables/evidence.txt'
        path.parent.mkdir(exist_ok=True)
        path.write_bytes(b'Checked fixture evidence.')
        return {'release_tag': 'outreach-task-job-1', 'name': 'evidence.txt', 'role': 'evidence',
                'agent_path': 'deliverables/evidence.txt', 'bytes': path.stat().st_size,
                'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}

    def test_artifacts_are_preserved_read_back_and_only_portable_metadata_is_admitted(self):
        artifact = self.artifact()
        self.result['artifacts'] = [artifact]
        result = local_run.preserve(self.result, self.row, self.remote, self.directory)
        expected = {k: v for k, v in artifact.items() if k != 'agent_path'}
        self.assertEqual(result['artifacts'], [expected])
        self.assertEqual(set(expected), {'release_tag', 'name', 'sha256', 'bytes', 'role'})
        self.assertIn((artifact['release_tag'], artifact['name']), self.remote.files)
        accepted = local_run.ingest(self.ledger, self.envelope(result=result), 'fixture-key')
        self.assertEqual(accepted['data']['result'], result)

    def test_invalid_artifact_identity_size_hash_path_and_readback_fail(self):
        artifact = self.artifact()
        for changes in [{'release_tag': 'outreach-task-other'}, {'bytes': 1}, {'sha256': '0' * 64},
                        {'agent_path': 'deliverables/../binding.json'}, {'name': '../escape'}]:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                local_run.preserve({**self.result, 'artifacts': [{**artifact, **changes}]},
                                   self.row, self.remote, self.directory)
        self.remote.corrupt = True
        with self.assertRaisesRegex(ValueError, 'readback'):
            local_run.preserve({**self.result, 'artifacts': [artifact]}, self.row, self.remote, self.directory)

    def test_symlink_cannot_export_outside_deliverables(self):
        artifact = self.artifact()
        outside = self.directory / 'outside.txt'
        outside.write_bytes(b'Checked fixture evidence.')
        path = self.directory / artifact['agent_path']
        path.unlink()
        path.symlink_to(outside)
        with self.assertRaisesRegex(ValueError, 'escapes'):
            local_run.preserve({**self.result, 'artifacts': [artifact]}, self.row, self.remote, self.directory)

    def test_ingest_checks_artifact_readback_again(self):
        artifact = self.artifact()
        self.result['artifacts'] = [artifact]
        result = local_run.preserve(self.result, self.row, self.remote, self.directory)
        self.remote.corrupt = True
        self.stage(self.envelope(result=result))
        self.assertIn('readback', self.ledger.ingest('fixture-key')[0]['error'])
        self.assertEqual(self.ledger.tasks.get(self.row['id'])['state'], 'queued')

    def test_ingest_rejects_runtime_paths_or_changed_coordinator_evidence(self):
        self.result['artifacts'] = [self.artifact()]
        with self.assertRaisesRegex(ValueError, 'preserved'):
            local_run.ingest(self.ledger, self.envelope(), 'fixture-key')
        self.result['artifacts'] = []
        self.row = self.ledger.tasks.update(self.row['id'], 1, 'queued', {'mailbox_history': {'complete': True}})
        with self.assertRaisesRegex(ValueError, 'replace'):
            local_run.ingest(self.ledger, self.envelope(context={'mailbox_history': {}}), 'fixture-key')

    def test_executor_logs_pid_session_and_reuses_staged_result_without_second_process(self):
        def launch(command, **kwargs):
            self.assertEqual(command[:5], ['codex', 'exec', '-s', 'workspace-write', '--skip-git-repo-check'])
            self.assertEqual(kwargs['stdin'], subprocess.DEVNULL)
            self.assertEqual(kwargs['cwd'], self.directory)
            self.assertNotIn('OUTREACH_REVIEW_KEY', kwargs['env'])
            kwargs['stdout'].write(canonical({'type': 'thread.started', 'thread_id': 'fixture-session'}) + '\n')
            (self.directory / 'result.json').write_text(canonical(self.result))
            return Mock(pid=12345, wait=Mock(return_value=0))
        with patch('managed.local_run.subprocess.Popen', side_effect=launch) as process, \
             patch('managed.control.review_key', return_value='fixture-key'):
            receipt = local_run.execute(self.row, self.remote, self.directory, {}, None)
            self.assertEqual(local_run.execute(self.row, self.remote, self.directory, {}, None), receipt)
        process.assert_called_once()
        self.assertEqual((self.directory / 'pid').read_text(), '12345\n')
        self.assertEqual((self.directory / 'prompt.txt').read_text(), local_run.prompt(self.row, {}, None))
        envelope = json.loads((self.directory / 'local-result.json').read_text())
        self.assertEqual(envelope['session_id'], 'fixture-session')
        self.assertTrue(receipt['asset'].startswith('local-result-'))
        self.assertEqual(self.ledger.tasks.get(self.row['id']), self.row)

    def test_executor_takeover_writes_failed_execution_binding(self):
        self.failed_row()

        def launch(command, **kwargs):
            kwargs['stdout'].write(canonical({'type': 'thread.started',
                                              'thread_id': 'takeover-session'}) + '\n')
            (self.directory / 'result.json').write_text(canonical(self.result))
            return Mock(pid=12346, wait=Mock(return_value=0))

        with patch('managed.local_run.subprocess.Popen', side_effect=launch), \
             patch('managed.control.review_key', return_value='fixture-key'):
            local_run.execute(self.row, self.remote, self.directory, {}, None, takeover=True)
        envelope = json.loads((self.directory / 'local-result.json').read_text())
        self.assertTrue(envelope['takeover'])
        self.assertEqual(envelope['failed_execution'],
                         local_run.failed_execution_identity(self.row))

    def test_workspace_index_contact_fallback_and_maintenance(self):
        root = self.directory / 'checkout'
        (root / 'tables').mkdir(parents=True)
        (root / 'tables/workspaces.json').write_text(canonical({'workspace_root': '../mail', 'workspaces': [
            {'id': 'person', 'path': '../mail/person', 'contact_ids': ['scholar-a']}]}))
        (root / 'tables/contacts.json').write_text(canonical({'contacts': [
            {'id': 'scholar-b', 'hub': {'workspace_paths': ['../mail/second']}}]}))
        self.assertEqual(local_run.workspace(self.row, root), (self.directory / 'mail/person').resolve())
        self.assertEqual(local_run.workspace({**self.row, 'owner': 'scholar-b'}, root), (self.directory / 'mail/second').resolve())
        self.assertEqual(local_run.workspace({**self.row, 'owner': 'missing'}, root), (self.directory / 'mail/outreach-maintenance').resolve())

    def test_mail_packet_artifact_is_expanded_before_signing_and_review(self):
        self.row = self.ledger.tasks.add(request('reply-packet', 'other', 'reply'))
        packet = json.loads((Path(__file__).parent / 'fixtures/letter.json').read_text())
        packet.update(kind='reply', thread_id='fixture-thread', latest_inbound_message_id='<fixture-inbound>',
                      user_decision_ever=False)
        raw = canonical(packet).encode()
        (self.directory / 'deliverables').mkdir()
        (self.directory / 'deliverables/mail.json').write_bytes(raw)
        artifact = {'release_tag': 'outreach-task-reply-packet', 'name': 'mail.json', 'role': 'mail-packet',
                    'agent_path': 'deliverables/mail.json', 'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()}
        self.result.update(task_id=self.row['id'], packet={'artifact': 'mail.json'}, artifacts=[artifact])
        result = local_run.preserve(self.result, self.row, self.remote, self.directory)
        self.assertEqual(result['packet'], packet)
        accepted = local_run.ingest(self.ledger, self.envelope(result=result), 'fixture-key')
        self.assertEqual(self.ledger.tasks.review_binding(accepted)['result_hash'], digest(result))

    def test_local_hold_uses_existing_reviewable_hold_state(self):
        self.result['outcome'] = 'hold'
        accepted = local_run.ingest(self.ledger, self.envelope(), 'fixture-key')
        self.assertEqual(accepted['state'], 'held')
        self.assertEqual(self.ledger.tasks.reviews()[0]['result'], self.result)

    def test_stale_workflow_and_missing_key_are_rejected_before_admission(self):
        self.stage(self.envelope())
        self.assertIn('key', self.ledger.ingest()[0]['error'])
        with patch('managed.control.workflow', return_value='f' * 64):
            self.assertIn('Workflow changed', self.ledger.ingest('fixture-key')[0]['error'])
        self.assertEqual(self.ledger.tasks.get(self.row['id'])['state'], 'queued')

    def test_batch_preparation_failure_keeps_independent_work_moving(self):
        self.ledger.tasks.add(request('job-2', 'other'))
        self.ledger.save(force=True)

        def prepare(row, *args):
            if row['id'] == 'job-1':
                raise ValueError('Fixture mailbox unavailable')
            return row

        with patch('managed.control.Releases', return_value=self.remote), \
             patch('managed.local_run.prepare', side_effect=prepare), \
             patch('managed.local_run.execute', return_value={'asset': 'fixture'}) as execute, \
             redirect_stdout(io.StringIO()), self.assertRaises(SystemExit) as error:
            local_run.main(['run', '--task', 'job-1', '--task', 'job-2', '--workspace', str(self.directory)])
        self.assertEqual(error.exception.code, 1)
        execute.assert_called_once()
        self.assertEqual(execute.call_args.args[0]['id'], 'job-2')

    def test_lock_prevents_overlapping_local_processes(self):
        with (self.directory / '.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with patch('managed.local_run.subprocess.Popen') as process, self.assertRaisesRegex(ValueError, 'already running'):
                local_run.execute(self.row, self.remote, self.directory, {}, None)
            process.assert_not_called()

    def test_run_parallel_is_bounded_deduplicated_and_uses_distinct_directories(self):
        for task_id in ('job-2', 'job-3'):
            self.ledger.tasks.add(request(task_id, task_id))
        self.ledger.save(force=True)
        pointer = copy.deepcopy(self.remote.body)
        lock = threading.Lock()
        barrier = threading.Barrier(2, timeout=5)
        active = maximum = started = 0
        directories = []

        def execute(row, releases, directory, config, capacity):
            nonlocal active, maximum, started
            with lock:
                active += 1
                started += 1
                index = started
                maximum = max(maximum, active)
                directories.append(directory)
            if index <= 2:
                barrier.wait()
            with lock:
                active -= 1
            return {'asset': 'fixture-' + row['id']}

        with patch('managed.control.Releases', return_value=self.remote), \
             patch('managed.local_run.prepare', side_effect=lambda row, *args: row), \
             patch('managed.local_run.execute', side_effect=execute), redirect_stdout(io.StringIO()):
            local_run.main(['run', '--task', 'job-1', '--task', 'job-2', '--task', 'job-3',
                            '--task', 'job-1', '--parallel', '2', '--workspace', str(self.directory)])
        self.assertEqual(maximum, 2)
        self.assertEqual(set(directories), {self.directory / 'local-runs' / task for task in ('job-1', 'job-2', 'job-3')})
        self.assertEqual(started, 3)
        self.assertEqual(self.remote.body, pointer)

    def test_list_cli_is_read_only_and_prints_only_identity_fields(self):
        self.ledger.tasks.add(request('reply-list', 'other', 'reply'))
        self.ledger.save(force=True)
        pointer = copy.deepcopy(self.remote.body)
        output = io.StringIO()
        with patch('managed.control.Releases', return_value=self.remote), redirect_stdout(output):
            local_run.main(['list', '--kinds', 'reply,firstcontact'])
        self.assertEqual(output.getvalue(), 'reply-list\treply\tother\tqueued\n')
        self.assertEqual(self.remote.body, pointer)

    def test_failed_local_listing_shows_recovery_fields_and_only_eligible_rows(self):
        self.failed_row()
        other = self.ledger.tasks.add(request('failed-rewrite', 'other', 'reply'))
        self.ledger.tasks.update(other['id'], other['revision'], 'failed', {
            'recovery': {'suggested_exit': 'rewrite-task', 'error_class': 'unknown',
                         'reason': 'Rewrite it'}})
        self.ledger.save(force=True)
        output = io.StringIO()
        with patch('managed.control.Releases', return_value=self.remote), redirect_stdout(output):
            local_run.main(['list', '--failed-local'])
        self.assertEqual(output.getvalue(),
                         'job-1\tresearch\trequest-too-large\tTake over this failed task\n')

    def test_takeover_refuses_non_local_failure_and_live_original_agent(self):
        self.failed_row(suggested='rewrite-task')
        with self.assertRaisesRegex(ValueError, 'suggested exit local-codex'):
            local_run.target(self.ledger.tasks.all(), self.row['id'], takeover=True)
        self.failed_row(retired=False)
        with self.assertRaisesRegex(ValueError, 'may still execute'):
            local_run.target(self.ledger.tasks.all(), self.row['id'], takeover=True)

    def test_takeover_envelope_binds_original_execution_identity(self):
        self.failed_row()
        envelope = self.takeover_envelope()
        self.assertIs(envelope['takeover'], True)
        self.assertEqual(envelope['failed_execution'], {
            'agent_id': 'agt-failed', 'response_id': 'resp-failed', 'command_key': 'command-failed'})
        self.stage(envelope)
        accepted = self.ledger.ingest('fixture-key')
        self.assertEqual(accepted, [])
        row = self.ledger.tasks.get(self.row['id'])
        self.assertTrue(row['data']['local_takeover'])
        self.assertEqual(row['data']['failed_execution'], envelope['failed_execution'])

    def test_takeover_admission_survives_cleanup_revision_bump(self):
        self.failed_row()
        envelope = self.takeover_envelope()
        # Cleanup bookkeeping on the still-failed row bumps its revision.
        row = self.ledger.tasks.get(self.row['id'])
        self.ledger.tasks.update(row['id'], row['revision'], 'failed',
                                 {**row['data'], 'cleanup_note': 'retire confirmed'})
        self.assertNotEqual(self.ledger.tasks.get(row['id'])['revision'], envelope['task_revision'])
        admitted = local_run.ingest(self.ledger, envelope, 'fixture-key')
        self.assertTrue(admitted['data']['local_takeover'])

    def test_ordinary_local_result_still_rejects_stale_revision(self):
        envelope = self.envelope()
        row = self.ledger.tasks.get(self.row['id'])
        self.ledger.tasks.update(row['id'], row['revision'], row['state'], {**row['data'], 'note': 'x'})
        with self.assertRaisesRegex(ValueError, 'revision is stale'):
            local_run.ingest(self.ledger, envelope, 'fixture-key')

    def test_takeover_admission_rejects_live_or_stale_failed_execution(self):
        self.failed_row(retired=False)
        with self.assertRaisesRegex(ValueError, 'may still execute'):
            local_run.ingest(self.ledger, self.takeover_envelope(), 'fixture-key')
        self.failed_row(retired=True)
        stale = self.takeover_envelope(failed_execution={'agent_id': 'other-agent',
                                                          'response_id': 'resp-failed',
                                                          'command_key': 'command-failed'})
        with self.assertRaisesRegex(ValueError, 'identity is stale'):
            local_run.ingest(self.ledger, stale, 'fixture-key')


if __name__ == '__main__':
    unittest.main()


def setUpModule():
    use_cli_config()
