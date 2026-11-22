"""Offline checks for the engine and private-data boundary."""
from contextlib import redirect_stderr, redirect_stdout
import base64
import io
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import unittest
from unittest.mock import patch

from managed import control, data_writes, location, run_log
from test_tasks import CMA, Releases, request


class LocationTests(unittest.TestCase):
    def test_defaults_and_explicit_split(self):
        root, data, branch, engine = location.resolve({})
        self.assertEqual((root, branch, engine), (location.ENGINE_ROOT, 'main', data))
        self.assertTrue(data)
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(location.resolve({
                'OUTREACH_DATA_ROOT': directory,
                'OUTREACH_DATA_REPO': 'sample/data',
                'OUTREACH_DATA_BRANCH': 'private/live',
                'OUTREACH_ENGINE_REPO': 'sample/engine',
            }), (Path(directory).resolve(), 'sample/data', 'private/live', 'sample/engine'))

    def test_production_needs_data_config_and_example_has_same_shape(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'managed').mkdir()
            config = location.example_config()
            (root / 'managed/config.example.json').write_text(json.dumps(config))
            with self.assertRaisesRegex(FileNotFoundError, 'OUTREACH_DATA_ROOT'):
                location.load_config(root, production=True)
            config['mailbox']['account'] = 'sample-owner@private.example.invalid'
            (root / 'config.json').write_text(json.dumps(config))
            self.assertEqual(location.load_config(root, production=True), config)
            self.assertEqual(set(config), set(location.example_config()))

    def test_prompt_selects_the_engine_for_new_pins(self):
        row = {'id': 'reply-a1', 'kind': 'reply', 'request': request('reply-a1', kind='reply'), 'data': {}}
        row['request']['policy_revision'] = 'b' * 40
        with patch.object(control, 'ENGINE_REPO', 'sample/engine'), patch.object(control, 'REPO', 'sample/data'), \
                patch.object(control, 'settings', side_effect=location.example_config().__getitem__):
            prompt = control.task_prompt(row)
        self.assertIn('https://github.com/sample/engine', prompt)
        self.assertIn('"repository":"sample/data"', prompt)


class DataBranchTests(unittest.TestCase):
    def test_contents_write_uses_the_same_branch_and_its_blob_sha(self):
        calls = []

        def api(*args):
            calls.append(args)
            if args[1] == '--method':
                calls.append(json.loads(Path(args[-1]).read_text()))
                return b'{}'
            return json.dumps({'sha': 'existing-sha', 'content': 'e30='})

        with patch.object(data_writes, 'DATA_BRANCH', 'automation/latest'):
            result = data_writes.contents_update(api, 'sample/data', 'tables/publications.json',
                                                 lambda old: (old + b'\n', 7), 'Update table')
        self.assertEqual(result, 7)
        self.assertEqual(calls[0], ('api', 'repos/sample/data/contents/tables/publications.json'
                                    '?ref=automation%2Flatest'))
        self.assertEqual(calls[2], {'message': 'Update table', 'sha': 'existing-sha',
                                    'branch': 'automation/latest',
                                    'content': base64.b64encode(b'{}\n').decode()})

    def test_git_writer_targets_configured_data_branch(self):
        with patch.object(data_writes, 'DATA_BRANCH', 'automation/latest'), \
                patch.object(data_writes, 'git') as git:
            data_writes.push(Path('/tmp/fixture-data'))
        git.assert_called_once_with(Path('/tmp/fixture-data'), 'push', 'origin',
                                    'HEAD:refs/heads/automation/latest', check=True)


class PublicLogTests(unittest.TestCase):
    def test_tick_keeps_mail_inputs_and_native_output_in_private_log(self):
        planted = ('Professor Tamsin Coppervale', 'planted-recipient@example.invalid', 'Unreleased subject',
                   'private-note-id', 'remote-Message-Id', 'Private Attachment.pdf')
        remote = Releases()
        remote.ensure = lambda: None
        remote.prune_snapshots = lambda *args: 0
        detailed = []

        def store(path, tick):
            self.assertTrue(tick)
            detailed.append(path.read_text())

        def intake(*args, **kwargs):
            os.write(2, planted[-1].encode() + b'\n')
            print(planted[0], file=sys.stderr)

        with tempfile.TemporaryDirectory() as directory:
            ledger = control.Ledger(remote, directory)
            value = request('reply-1a2b', kind='reply')
            value['inputs']['private_fixture'] = dict(zip(
                ('name', 'email', 'subject', 'note', 'mid', 'filename'), planted))
            ledger.tasks.add(value)
            ledger.save(force=True)
            ledger.tasks.close()
            output = io.StringIO()
            with patch.dict(os.environ, {'OUTREACH_PUBLIC_LOG': '1', 'GITHUB_ACTIONS': 'true'}), \
                    patch.object(control, 'load_config', return_value=location.example_config()), \
                    patch.object(control, 'Releases', return_value=remote), \
                    patch.object(control, 'CMA', return_value=CMA()), \
                    patch('managed.mailbox.configured', return_value=object()), \
                    patch('managed.mailbox.intake', side_effect=intake), \
                    patch('managed.pipeline.reconcile'), patch('managed.pipeline.status', return_value={}), \
                    patch.object(run_log, '_upload', side_effect=store), \
                    redirect_stdout(output), redirect_stderr(output):
                result = control.main(['tick'])
        public = output.getvalue()
        self.assertEqual(result, 0, detailed[0])
        self.assertRegex(public, r'tick-phase restore [\d.]+s units=\d+')
        self.assertRegex(public, r'tick complete [\d.]+s')
        for value in planted:
            self.assertNotIn(value, public)
            self.assertIn(value, detailed[0])

    def test_quiet_success_still_uploads_a_nonempty_private_log(self):
        public = io.StringIO()
        sizes = []
        with patch.dict(os.environ, {'OUTREACH_PUBLIC_LOG': '1'}), \
                patch.object(run_log, '_upload', side_effect=lambda path, tick: sizes.append(path.stat().st_size)), \
                redirect_stdout(public), redirect_stderr(public):
            self.assertEqual(run_log.public_run(lambda: 0, 'checkout'), 0)
        self.assertGreater(sizes[0], 0)
        self.assertRegex(public.getvalue(), r'^checkout complete [\d.]+s\n$')

    def test_uncaught_exception_has_an_opaque_incident_and_private_traceback(self):
        public = io.StringIO()
        detailed = []

        def fail():
            raise ValueError('unreleased subject and correspondence filename')

        with patch.dict(os.environ, {'OUTREACH_PUBLIC_LOG': '1'}), \
                patch.object(run_log, '_upload', side_effect=lambda path, tick: detailed.append(path.read_text())), \
                redirect_stdout(public), redirect_stderr(public):
            self.assertEqual(run_log.public_run(fail, 'control'), 1)
        self.assertRegex(public.getvalue(), r'^ValueError incident-[a-f\d]{12}\ncontrol failed [\d.]+s\n$')
        self.assertIn('Traceback', detailed[0])
        self.assertIn('unreleased subject and correspondence filename', detailed[0])


class HostedShortNoteImportTest(unittest.TestCase):
    def test_hosted_pages_and_notes_can_import_the_short_note_service(self):
        import subprocess
        import sys
        result = subprocess.run(
            [sys.executable, '-c', 'from scripts.short_notes import service; print(service.w.__name__)'],
            cwd=Path(__file__).resolve().parents[2], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(result.stdout.strip().endswith('workflow'))
