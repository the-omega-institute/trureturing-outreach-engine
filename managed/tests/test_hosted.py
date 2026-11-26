"""Disposable feed cache transport never writes or commits private data files."""
import copy
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch

from managed import control, data_writes, hosted
from scripts.short_notes import service


class HostedFeedTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name) / 'private-data'
        self.root.mkdir()
        self.remote = Mock(repo='private/example', tag='outreach-control')
        self.raw = None
        self.remote.release.side_effect = lambda: {'assets': (
            [{'name': service.FEED_CACHE_ASSET}] if self.raw is not None else [])}
        def download(name, directory):
            path = Path(directory) / name
            path.write_bytes(self.raw)
            return path
        self.remote.download.side_effect = download
        self.uploads = []
        def upload(*args):
            self.uploads.append(Path(args[3]).read_bytes())
        self.remote.run.side_effect = upload
        self.enterContext(patch.dict(os.environ, {
            'GITHUB_ACTIONS': 'true', 'OUTREACH_DATA_ROOT': str(self.root),
            service.FEED_CACHE_ENV: 'previous-cache-environment'}))
        self.enterContext(patch.object(hosted, 'DATA_ROOT', self.root))
        self.enterContext(patch.object(control, 'Releases', return_value=self.remote))
        self.enterContext(patch.object(hosted, 'restore_credentials'))
        self.commit = self.enterContext(patch.object(data_writes, 'commit_paths',
                side_effect=AssertionError('cache must never enter git')))
        self.push = self.enterContext(patch.object(data_writes, 'push',
                side_effect=AssertionError('cache must never push git')))

    def run_job(self, *, feed=None, action='pages'):
        paths = []
        def main(args):
            self.assertEqual(args, ['sync'] if action == 'pages' else ['ci'])
            path = Path(os.environ[service.FEED_CACHE_ENV])
            paths.append(path)
            self.assertFalse(path.is_relative_to(self.root))
            cache = service.FeedCache(path)
            self.loaded = copy.deepcopy(cache.value)
            if feed is not None:
                cache.save(feed)
            return 0
        with patch.object(service, 'main', side_effect=main) as main:
            self.assertEqual(hosted.command(action), 0)
        main.assert_called_once()
        self.assertFalse(paths[0].exists())
        self.assertEqual(os.environ[service.FEED_CACHE_ENV], 'previous-cache-environment')
        self.assertEqual(list(self.root.iterdir()), [])
        self.commit.assert_not_called()
        self.push.assert_not_called()

    @staticmethod
    def feed():
        return {'schema_version': 'research-updates.v1', 'publications': [],
                'collaborations': [], 'updated_at': '2026-11-23T00:00:00Z'}

    def test_cold_cache_uploads_only_a_disposable_control_release_asset(self):
        self.run_job(feed=self.feed())
        self.assertEqual(self.loaded, {})
        self.remote.require_private.assert_called_once_with()
        self.remote.download.assert_not_called()
        self.remote.ensure.assert_called_once_with()
        call = self.remote.run.call_args.args
        self.assertEqual(call[:3], ('release', 'upload', 'outreach-control'))
        self.assertEqual(Path(call[3]).name, service.FEED_CACHE_ASSET)
        self.assertEqual(call[4:], ('--repo', 'private/example', '--clobber'))
        self.assertEqual(json.loads(self.uploads[0])['feed'], self.feed())

    def test_absent_unused_cache_needs_no_upload(self):
        self.run_job()
        self.assertEqual(self.loaded, {})
        self.remote.ensure.assert_not_called()
        self.remote.run.assert_not_called()

    def test_explicit_note_inputs_ignore_the_entire_secret_and_stale_environment(self):
        environment = {'NOTE_USE_INPUTS': 'true', 'INPUT_NOTE_ACTION': 'publish',
                       'INPUT_NOTE_ID': 'alpha', 'INPUT_NOTE_REVISION': '3',
                       'INPUT_NOTE_RECOVER_RECORD': '42', 'INPUT_NOTE_REUSE_DRAFT_FROM': '2',
                       'OUTREACH_NOTE_REQUEST': 'unreadable secret is never parsed',
                       'NOTE_ACTION': 'drain', 'PREFERRED_PUBLICATION': 'stale', 'BATCH_LIMIT': '99'}
        with patch.dict(os.environ, environment), patch.object(service, 'main', return_value=0) as main:
            self.assertEqual(hosted.command('notes'), 0)
            self.assertEqual([os.environ[key] for key in ('NOTE_ACTION', 'NOTE_ID', 'NOTE_REVISION',
                             'RECOVER_RECORD', 'REUSE_DRAFT_FROM')], ['publish', 'alpha', '3', '42', '2'])
            self.assertNotIn('PREFERRED_PUBLICATION', os.environ)
            self.assertNotIn('BATCH_LIMIT', os.environ)
            main.assert_called_once_with(['ci'])

    def test_invalid_explicit_input_cannot_fall_back_to_secret_or_execute(self):
        with patch.dict(os.environ, {'NOTE_USE_INPUTS': 'true', 'INPUT_NOTE_ACTION': 'publish',
                                    'INPUT_NOTE_ID': 'alpha', 'INPUT_NOTE_REVISION': '0',
                                    'OUTREACH_NOTE_REQUEST': '{"id":"beta","revision":1}'}), \
                patch.object(service, 'main') as main:
            with self.assertRaises(ValueError):
                hosted.command('notes')
            main.assert_not_called()
            self.remote.run.assert_not_called()

    def test_status_input_and_scheduled_secret_path(self):
        with patch.dict(os.environ, {'NOTE_USE_INPUTS': 'true', 'INPUT_NOTE_ACTION': 'status',
                                    'INPUT_NOTE_ID': 'alpha', 'RECOVER_RECORD': '99',
                                    'OUTREACH_NOTE_REQUEST': 'not parsed'}), \
                patch.object(service, 'main', return_value=0):
            self.assertEqual(hosted.command('notes'), 0)
            self.assertEqual(os.environ['NOTE_ACTION'], 'status')
            self.assertNotIn('RECOVER_RECORD', os.environ)
        with patch.dict(os.environ, {'NOTE_USE_INPUTS': 'false', 'NOTE_ACTION': 'drain',
                                    'OUTREACH_NOTE_REQUEST': '{"id":"beta","revision":2,"limit":5}'}), \
                patch.object(service, 'main', return_value=0):
            self.assertEqual(hosted.command('notes'), 0)
            self.assertEqual(os.environ['NOTE_ACTION'], 'drain')
            self.assertEqual(os.environ['NOTE_ID'], 'beta')
            self.assertEqual(os.environ['BATCH_LIMIT'], '5')

    def test_warm_unchanged_cache_downloads_without_upload_or_git_writes(self):
        self.run_job(feed=self.feed())
        self.raw = self.uploads[0]
        self.remote.reset_mock()
        self.uploads.clear()
        self.run_job(feed=self.feed(), action='notes')
        self.assertEqual(self.loaded['feed'], self.feed())
        self.remote.download.assert_called_once()
        self.remote.ensure.assert_not_called()
        self.assertEqual(self.uploads, [])

    def test_changed_cache_replaces_only_its_release_asset(self):
        self.run_job(feed=self.feed())
        self.raw = self.uploads[0]
        self.remote.reset_mock()
        self.uploads.clear()
        changed = {**self.feed(), 'notes': [{'id': 'new-reading-link'}]}
        self.run_job(feed=changed)
        self.assertEqual(self.loaded['feed'], self.feed())
        self.assertEqual(json.loads(self.uploads[0])['feed'], changed)
        self.remote.run.assert_called_once()

    def test_failed_or_corrupt_download_is_a_cold_cache(self):
        for fail in (False, True):
            with self.subTest(fail=fail):
                self.raw = b'not-json'
                if fail:
                    self.remote.download.side_effect = RuntimeError('unavailable cache')
                self.run_job(feed=self.feed())
                self.assertEqual(self.loaded, {})

    def test_cache_upload_failure_does_not_change_delivery_result(self):
        for error in (RuntimeError('disposable upload failed'), subprocess.TimeoutExpired('gh', 1)):
            with self.subTest(error=type(error).__name__):
                self.remote.run.side_effect = error
                self.run_job(feed=self.feed())

    def test_unverified_private_release_uses_cold_cache_without_upload(self):
        self.remote.require_private.side_effect = RuntimeError('privacy lookup unavailable')
        self.run_job(feed=self.feed())
        self.assertEqual(self.loaded, {})
        self.remote.release.assert_not_called()
        self.remote.run.assert_not_called()

    def test_engine_checkout_does_not_load_or_persist_cache(self):
        from managed.location import ENGINE_ROOT
        with patch.object(hosted, 'DATA_ROOT', ENGINE_ROOT), \
                patch.object(control, 'Releases') as releases:
            with hosted.feed_cache():
                pass
        releases.assert_not_called()

    def test_service_uses_only_the_job_temporary_cache_and_no_data_branch_default(self):
        catalog = self.root / 'catalog.json'
        catalog.write_text(json.dumps({'schema_version': 'research-publication.v1',
            'publications': [], 'collaborations': [], 'notes': []}))
        args = service.parse_args(['--catalog', str(catalog), 'validate'])
        with patch.object(service, 'DATA_ROOT', self.root), \
                patch.object(service, 'load_submission_board', return_value=None), \
                patch.object(service.w, 'GitHub'), \
                patch.object(service, 'FeedCache') as cache:
            os.environ.pop(service.FEED_CACHE_ENV)
            service.execute(args)
            cache.assert_not_called()
            with hosted.feed_cache():
                service.execute(args)
                path = Path(os.environ[service.FEED_CACHE_ENV])
                cache.assert_called_once_with(str(path))
                self.assertFalse(path.is_relative_to(self.root))
