"""Acceptance checks for the file producer, durable queue and public projection."""
import contextlib
import copy
import hashlib
from datetime import datetime, timedelta, timezone
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

import service as s
import workflow as w
from test_workflow import FakeGitHub, FakeZenodo, metadata, empty_feed


def catalog(*ids):
    return {'schema_version': 'research-publication.v1', 'publications': [], 'collaborations': [],
            'notes': [{'id': note_id, 'problem_ids': [note_id], 'paused': False} for note_id in ids]}


def request(note_id, revision=1):
    return {'schema_version': 'short-note-request.v1', 'id': note_id,
            'action': 'publish', 'revision': revision}


class ServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.gh, self.pages, self.zenodo = FakeGitHub(), FakeGitHub(), FakeZenodo()
        self.enterContext(patch.object(s, 'urlopen', side_effect=OSError('offline test')))

    def package(self, note_id='alpha', revision=1):
        folder = self.root / (note_id + '-' + str(revision))
        folder.mkdir()
        meta = {**metadata(revision), 'id': note_id, 'problem_ids': [note_id]}
        (folder / 'metadata.json').write_bytes(w.encode(meta))
        (folder / 'note.pdf').write_bytes(b'%PDF-1.4\nselected compiled note')
        (folder / 'note.tex').write_text('The selected readable proof.')
        prepared = s.intake(folder / 'metadata.json', folder, None, folder / 'prepared')
        w.stage(prepared, self.gh)
        return meta, prepared

    def enqueue(self, note_id='alpha', revision=1):
        self.package(note_id, revision)
        return s.enqueue(self.catalog, request(note_id, revision), self.gh)

    def publish(self, note_id='alpha'):
        self.catalog = catalog(note_id)
        self.enqueue(note_id)
        result = s.drain(self.catalog, self.gh, self.zenodo, self.pages)
        self.assertEqual(result['failed'], [])
        return self.state(note_id)

    def state(self, note_id='alpha'):
        return w.release_state(self.gh.release(w.tag_for(note_id)), note_id)

    def publish_paper(self):
        state = self.publish()
        paper = {**state['publication'], 'url': 'https://arxiv.org/abs/2611.00992', 'venue': 'arXiv'}
        self.gh.save_state(w.tag_for('alpha'), w.promote(state, paper))
        s.record_mathesis(self.catalog, 'alpha', self.mathesis_receipt(), self.gh)
        s.synchronize(self.catalog, self.gh, self.pages)
        return self.state()

    def cached_feed(self):
        state = self.publish_paper()
        # Real GitHub listings give immutable asset IDs/digests. Model those
        # identities without changing the existing public Release format.
        release = self.gh.release
        def identified(tag):
            value = release(tag)
            if value:
                for asset in value['assets']:
                    asset['digest'] = 'sha256:' + hashlib.sha256(self.gh.releases[tag][asset['name']]).hexdigest()
            return value
        self.enterContext(patch.object(self.gh, 'release', side_effect=identified))
        self.cache = s.FeedCache(self.root / 'private-cache.json')
        index = self.public_index(state)
        s.synchronize_if_changed(self.catalog, self.gh, self.pages, cache=self.cache, fetch=lambda: index)
        return index

    def test_unchanged_deployed_feed_reuses_cache_across_jobs_and_reads_live_site(self):
        index = self.cached_feed()
        restored = s.FeedCache(self.cache.path)
        before = (list(self.pages.dispatches), list(self.pages.uploads), self.cache.path.read_bytes())
        with patch.object(s, 'projection', side_effect=AssertionError('unchanged projection rebuilt')), \
                patch.object(self.gh, 'download', side_effect=AssertionError('manifest redownloaded')), \
                patch.object(self.gh, 'list_releases', wraps=self.gh.list_releases) as listings, \
                patch.object(s, 'urlopen') as served:
            served.return_value.__enter__.return_value.read.return_value = w.encode(index)
            result = s.synchronize_if_changed(self.catalog, self.gh, self.pages, cache=restored)
        self.assertEqual(result['status'], 'deployed')
        self.assertEqual(listings.call_count, 1)
        self.assertEqual(served.call_count, 1)
        self.assertEqual(before, (self.pages.dispatches, self.pages.uploads, self.cache.path.read_bytes()))
        value = w.read(self.cache.path)
        self.assertEqual(value['feed_sha256'], s.feed_digest(value['feed']))
        self.assertNotIn('deployed_sha256', value)

    def test_idle_delivery_uses_one_listing_without_manifest_downloads_or_deploy(self):
        index = self.cached_feed()
        dispatches = len(self.pages.dispatches)
        with patch.object(self.gh, 'list_releases', wraps=self.gh.list_releases) as listings, \
                patch.object(self.gh, 'download', side_effect=AssertionError('idle download')):
            result = s.drain(self.catalog, self.gh, None, self.pages,
                             cache=s.FeedCache(self.cache.path), fetch=lambda: index)
        self.assertEqual(result, {'processed': 0, 'remaining': 0, 'failed': [], 'needs_attention': [], 'status': 'idle'})
        self.assertEqual(listings.call_count, 1)
        self.assertEqual(len(self.pages.dispatches), dispatches)

    def test_catalog_change_reprojects_from_cached_manifests_and_dispatches_once(self):
        index = self.cached_feed()
        self.catalog['publications'] = [{**self.state()['publication'], 'id': 'other',
                                         'url': 'https://arxiv.org/abs/2611.10000'}]
        dispatches = len(self.pages.dispatches)
        with patch.object(self.gh, 'download', side_effect=AssertionError('unchanged manifest downloaded')):
            result = s.synchronize_if_changed(self.catalog, self.gh, self.pages,
                                              cache=s.FeedCache(self.cache.path), fetch=lambda: index)
        self.assertEqual(result['status'], 'dispatch_accepted')
        self.assertEqual(len(self.pages.dispatches), dispatches + 1)
        served = json.loads(self.pages.releases['research-updates']['research-updates.v1.json'])
        self.assertEqual(len(served['publications']), 2)
        self.assertNotIn('deployed_sha256', w.read(self.cache.path))
        index = {**served, 'schema_version': 'publication-index.v1'}
        result = s.synchronize_if_changed(self.catalog, self.gh, self.pages,
                                          cache=s.FeedCache(self.cache.path), fetch=lambda: index)
        self.assertEqual(result['status'], 'deployed')
        self.assertEqual(len(self.pages.dispatches), dispatches + 1)

    def test_changed_note_receipt_uses_cached_manifest_and_invalidates_feed(self):
        index = self.cached_feed()
        state = self.state()
        state['publication']['summary'] = 'The checked paper now states the boundary case explicitly.'
        self.gh.save_state(w.tag_for('alpha'), state)
        with patch.object(self.gh, 'download', side_effect=AssertionError('unchanged manifest downloaded')):
            result = s.synchronize_if_changed(self.catalog, self.gh, self.pages,
                                              cache=s.FeedCache(self.cache.path), fetch=lambda: index)
        self.assertEqual(result['status'], 'dispatch_accepted')
        served = json.loads(self.pages.releases['research-updates']['research-updates.v1.json'])
        self.assertEqual(served['publications'][0]['summary'], state['publication']['summary'])

    def test_new_note_downloads_only_its_own_manifest(self):
        index = self.cached_feed()
        self.package('beta')
        self.catalog['notes'].append(catalog('beta')['notes'][0])
        w.operate('publish', 'beta', 1, self.gh, self.zenodo)
        beta = w.release_state(self.gh.release(w.tag_for('beta')), 'beta')
        beta['publication'] = {**beta['publication'], 'url': 'https://arxiv.org/abs/2611.10001', 'venue': 'arXiv'}
        self.gh.save_state(w.tag_for('beta'), beta)
        with patch.object(self.gh, 'download', wraps=self.gh.download) as downloads:
            result = s.synchronize_if_changed(self.catalog, self.gh, self.pages,
                                              cache=s.FeedCache(self.cache.path), fetch=lambda: index)
        self.assertEqual(result['status'], 'dispatch_accepted')
        self.assertEqual(downloads.call_count, 1)
        self.assertEqual(downloads.call_args.args[:2], ('short-note-beta', 'note-v1.json'))

    def test_delivery_and_pages_receipts_do_not_invalidate_projection_inputs(self):
        index = self.cached_feed()
        state = self.state()
        state['pages'].update(status='deployed', verified_at='2026-11-23T00:00:00Z')
        state['deliveries']['request-synthetic.json'] = {'status': 'dispatched', 'action': 'promote'}
        self.gh.save_state(w.tag_for('alpha'), state)
        with patch.object(s, 'projection', side_effect=AssertionError('receipt-only cache miss')):
            self.assertEqual(s.synchronize_if_changed(self.catalog, self.gh, self.pages,
                             cache=s.FeedCache(self.cache.path), fetch=lambda: index)['status'], 'deployed')

    def test_replaced_manifest_identity_forces_checked_download(self):
        index = self.cached_feed()
        tag = w.tag_for('alpha')
        package = json.loads(self.gh.releases[tag]['note-v1.json'])
        package['metadata']['title'] = 'A substituted unreviewed theorem'
        self.gh.releases[tag]['note-v1.json'] = w.encode(package)
        with patch.object(self.gh, 'download', wraps=self.gh.download) as downloads:
            with self.assertRaisesRegex(ValueError, 'differs from the archived'):
                s.synchronize_if_changed(self.catalog, self.gh, self.pages,
                                          cache=s.FeedCache(self.cache.path), fetch=lambda: index)
        self.assertEqual(downloads.call_count, 1)

    def test_corrupt_cache_is_rebuilt_and_revalidated(self):
        index = self.cached_feed()
        for raw in (b'not json', b'[]', w.encode({'inputs_sha256': self.cache.value['inputs_sha256'],
                                               'feed': [], 'manifests': 'corrupt'})):
            with self.subTest(raw=raw):
                self.cache.path.write_bytes(raw)
                with patch.object(self.gh, 'download', wraps=self.gh.download) as downloads:
                    result = s.synchronize_if_changed(self.catalog, self.gh, self.pages,
                                                      cache=s.FeedCache(self.cache.path), fetch=lambda: index)
                self.assertEqual(result['status'], 'deployed')
                self.assertEqual(downloads.call_count, 1)

    def test_missing_asset_identities_disable_cache_hits(self):
        state = self.publish_paper()
        index = self.public_index(state)
        cache = s.FeedCache(self.root / 'private-cache.json')
        for _ in range(2):
            with patch.object(self.gh, 'download', wraps=self.gh.download) as downloads:
                s.synchronize_if_changed(self.catalog, self.gh, self.pages, cache=cache, fetch=lambda: index)
            self.assertEqual(downloads.call_count, 1)
            self.assertIsNone(w.read(cache.path)['inputs_sha256'])

    def test_cached_feed_does_not_hide_stale_site_or_dispatch_failure(self):
        index = self.cached_feed()
        self.pages.fail_dispatch = True
        stale = {**index, 'notes': []}
        with self.assertRaisesRegex(RuntimeError, 'Dispatch'):
            s.synchronize_if_changed(self.catalog, self.gh, self.pages,
                                      cache=s.FeedCache(self.cache.path), fetch=lambda: stale)
        self.pages.fail_dispatch = False
        result = s.synchronize_if_changed(self.catalog, self.gh, self.pages,
                                          cache=s.FeedCache(self.cache.path), fetch=lambda: stale)
        self.assertEqual(result['status'], 'dispatch_accepted')
        def unavailable():
            raise OSError('site unavailable')
        result = s.synchronize_if_changed(self.catalog, self.gh, self.pages,
                                          cache=s.FeedCache(self.cache.path), fetch=unavailable)
        self.assertEqual(result['status'], 'dispatch_accepted')

    def test_deployed_cache_still_recovers_a_pending_feed_swap(self):
        index = self.cached_feed()
        assets = self.pages.releases['research-updates']
        assets['research-updates.pending.json'] = assets['research-updates.v1.json']
        dispatches = len(self.pages.dispatches)
        result = s.synchronize_if_changed(self.catalog, self.gh, self.pages,
                                          cache=s.FeedCache(self.cache.path), fetch=lambda: index)
        self.assertEqual(result['status'], 'dispatch_accepted')
        self.assertEqual(len(self.pages.dispatches), dispatches + 1)
        self.assertNotIn('research-updates.pending.json', assets)

    def test_feed_cache_key_covers_submission_board_and_projection_code(self):
        self.cached_feed()
        releases = s.note_releases(self.catalog, self.gh)
        first = self.cache.key(self.catalog, releases, {'papers': []})
        self.assertNotEqual(first, self.cache.key(self.catalog, releases, {'papers': [{'id': 'new'}]}))
        read_bytes = Path.read_bytes
        def changed_code(path):
            raw = read_bytes(path)
            return raw + b'\n# changed projection' if path == Path(s.__file__) else raw
        with patch.object(Path, 'read_bytes', changed_code):
            self.assertNotEqual(first, self.cache.key(self.catalog, releases, {'papers': []}))

    def test_publication_removal_requires_a_new_deploy(self):
        index = self.cached_feed()
        other = {**index['publications'][0], 'id': 'unrelated-paper',
                 'url': 'https://arxiv.org/abs/2611.10009'}
        self.pages.authored_publications = [copy.deepcopy(index['publications'][0]), other]
        index['publications'].append(other)
        state = self.state()
        state['publication'] = state['versions']['1']['publication']
        self.gh.save_state(w.tag_for('alpha'), state)
        result = s.synchronize_if_changed(self.catalog, self.gh, self.pages,
                                          cache=s.FeedCache(self.cache.path), fetch=lambda: index)
        self.assertEqual(result['status'], 'dispatch_accepted')
        served = json.loads(self.pages.releases['research-updates']['research-updates.v1.json'])
        self.assertEqual(served['publications'], [])

        # The dispatch was accepted and the Release changed, but the Pages
        # build failed. Its served index still contains the withdrawn paper.
        for cold in (False, True, False):
            with self.subTest(cold=cold):
                if cold:
                    self.cache.path.unlink(missing_ok=True)
                dispatches = len(self.pages.dispatches)
                result = s.synchronize_if_changed(self.catalog, self.gh, self.pages,
                        cache=s.FeedCache(self.cache.path), fetch=lambda: index)
                self.assertEqual(result['status'], 'dispatch_accepted')
                self.assertEqual(len(self.pages.dispatches), dispatches + 1)
        deployed = {**served, 'schema_version': 'publication-index.v1', 'publications': [other]}
        result = s.synchronize_if_changed(self.catalog, self.gh, self.pages,
                cache=s.FeedCache(self.cache.path), fetch=lambda: deployed)
        self.assertEqual(result['status'], 'deployed')
        self.assertEqual(deployed['publications'], [other])

    def test_authored_catalog_failure_keeps_verification_pending_and_dispatch_retrying(self):
        index = self.cached_feed()
        with patch.object(w, 'pages_authored_publications', side_effect=RuntimeError('catalog unavailable')):
            self.assertEqual(s.verify_feed(self.catalog, self.gh, pages=self.pages,
                    fetch=lambda: index)['status'], 'verification_pending')
            result = s.synchronize_if_changed(self.catalog, self.gh, self.pages,
                    cache=s.FeedCache(self.cache.path), fetch=lambda: index)
        self.assertEqual(result['status'], 'dispatch_accepted')

    def test_feed_cache_preserves_verification_of_sites_with_other_paper_sources(self):
        index = self.cached_feed()
        other = {**index['publications'][0], 'id': 'unrelated-paper',
                 'url': 'https://arxiv.org/abs/2611.10009'}
        self.pages.authored_publications = [other]
        index['publications'].append(other)
        dispatches = len(self.pages.dispatches)
        result = s.synchronize_if_changed(self.catalog, self.gh, self.pages,
                                          cache=s.FeedCache(self.cache.path), fetch=lambda: index)
        self.assertEqual(result['status'], 'deployed')
        self.assertEqual(len(self.pages.dispatches), dispatches)

    def test_extra_source_papers_are_pending_without_any_cached_history(self):
        index = self.cached_feed()
        index['publications'].append({**index['publications'][0], 'id': 'removed-catalog-paper',
                                      'url': 'https://arxiv.org/abs/2611.10008'})
        self.cache.path.unlink()
        self.assertEqual(s.verify_feed(self.catalog, self.gh, pages=self.pages, fetch=lambda: index)['status'], 'verification_pending')
        result = s.synchronize_if_changed(self.catalog, self.gh, self.pages,
                cache=s.FeedCache(self.cache.path), fetch=lambda: index)
        self.assertEqual(result['status'], 'dispatch_accepted')

    def test_prefixed_ids_still_match_and_duplicate_source_papers_are_pending(self):
        index = self.cached_feed()
        expected = copy.deepcopy(self.cache.value['feed'])
        expected['publications'][0]['id'] = 'paper-alpha'
        index['publications'][0]['id'] = 'paper-paper-alpha'
        self.assertEqual(s.verify_feed(self.catalog, self.gh, pages=self.pages,
                expected=expected, fetch=lambda: index)['status'], 'deployed')
        index['publications'].append(copy.deepcopy(index['publications'][0]))
        self.assertEqual(s.verify_feed(self.catalog, self.gh, pages=self.pages,
                expected=expected, fetch=lambda: index)['status'], 'verification_pending')

    def test_uncached_idle_worker_retries_a_failed_removal_deploy(self):
        index = self.cached_feed()
        state = self.state()
        state['publication'] = state['versions']['1']['publication']
        self.gh.save_state(w.tag_for('alpha'), state)
        for _ in range(3):
            dispatches = len(self.pages.dispatches)
            result = s.drain(self.catalog, self.gh, None, self.pages, fetch=lambda: index)
            self.assertEqual(result['status'], 'dispatch_accepted')
            self.assertEqual(len(self.pages.dispatches), dispatches + 1)

    def test_intake_archives_only_selected_sources_and_is_reproducible(self):
        meta, prepared = self.package()
        folder = prepared.parent
        (folder / 'outreach-polished.json').write_text('private correspondence')
        (folder / 'note.log').write_text('reproducible compiler log')
        repeat = s.intake(folder / 'metadata.json', folder, None, folder / 'repeat')
        self.assertEqual((repeat / 'source-v1.zip').read_bytes(), (prepared / 'source-v1.zip').read_bytes())
        with zipfile.ZipFile(repeat / 'source-v1.zip') as archive:
            self.assertEqual(archive.namelist(), ['note.tex'])
        (folder / 'references.bib').write_text('@article{checked}')
        selected = s.intake(folder / 'metadata.json', folder, ['note.tex', 'references.bib'], folder / 'selected')
        with zipfile.ZipFile(selected / 'source-v1.zip') as archive:
            self.assertEqual(archive.namelist(), ['note.tex', 'references.bib'])

    def test_intake_rejects_parent_paths_and_external_symlinks(self):
        _, prepared = self.package()
        (self.root / 'outside.tex').write_text('outside')
        (prepared.parent / 'linked.tex').symlink_to(self.root / 'outside.tex')
        for selected in ('../outside.tex', 'linked.tex'):
            with self.assertRaisesRegex(ValueError, 'inside the note directory'):
                s.intake(prepared.parent / 'metadata.json', prepared.parent, [selected], self.root / 'rejected')

    def test_registration_preserves_pauses_and_rejects_ambiguous_ownership(self):
        meta = {**metadata(), 'id': 'alpha', 'problem_ids': ['alpha']}
        result = s.register(catalog(), meta, 'joint-project')
        result['notes'][0]['paused'] = True
        self.assertEqual(s.register(result, meta), result)
        with self.assertRaises(ValueError):
            s.register({**catalog(), 'publications': [{**self.publish()['publication'], 'id': 'alpha'}]}, meta)

    def test_inventory_reports_missing_and_incomplete_notes_without_enqueuing(self):
        items = self.root / 'items'
        (items / 'alpha/polish').mkdir(parents=True)
        (items / 'alpha/polish/note.pdf').write_bytes(b'example')
        (items / 'beta/polish').mkdir(parents=True)
        for name in ('note.pdf', 'note.tex'):
            (items / 'beta/polish' / name).write_text('example')
        result = s.inventory(items, catalog('alpha'), {'results': [{'id': 'alpha'}, {'id': 'beta'}, {'id': 'missing'}]})
        self.assertEqual(result['counts'], {'present': 1, 'incomplete': 1, 'missing': 1})
        self.assertEqual(result['results'][0]['represented_by'], ['alpha'])
        self.assertEqual(self.gh.releases, {})

    def test_enqueuing_is_idempotent_and_does_not_write_the_worker_receipt(self):
        self.catalog = catalog('alpha')
        first = self.enqueue()
        body = self.gh.bodies[w.tag_for('alpha')]
        self.assertEqual(s.enqueue(self.catalog, request('alpha'), self.gh), first)
        self.assertEqual(body, w.INITIAL_NOTES)
        self.assertEqual(body, self.gh.bodies[w.tag_for('alpha')])
        self.assertEqual(len(s.queue(self.catalog, self.gh)), 1)
        self.assertEqual(self.zenodo.calls, [])

    def test_one_wakeup_drains_two_persisted_requests(self):
        self.catalog = catalog('alpha', 'beta')
        self.enqueue('alpha')
        self.enqueue('beta')
        result = s.drain(self.catalog, self.gh, self.zenodo, self.pages)
        self.assertEqual((result['processed'], result['remaining'], result['failed']), (2, 0, []))
        feed = json.loads(self.pages.releases['research-updates']['research-updates.v1.json'])
        self.assertEqual(feed['publications'], [])
        self.assertNotIn('zenodo', json.dumps(feed))
        self.assertEqual(self.state()['pages']['status'], 'not_applicable')
        self.assertTrue(all(row['status'] == 'dispatched' for row in s.queue(self.catalog, self.gh)))

    def test_batch_limit_leaves_durable_work_for_the_next_wakeup(self):
        self.catalog = catalog('alpha', 'beta')
        self.enqueue('alpha')
        self.enqueue('beta')
        first = s.drain(self.catalog, self.gh, self.zenodo, self.pages, limit=1)
        self.assertEqual((first['processed'], first['remaining']), (1, 1))
        second = s.drain(self.catalog, self.gh, self.zenodo, self.pages, limit=1)
        self.assertEqual((second['processed'], second['remaining']), (1, 0))
        self.assertEqual(len(self.zenodo.records), 2)

    def test_time_budget_leaves_the_request_untouched(self):
        self.catalog = catalog('alpha')
        self.enqueue()
        with patch('service.time.monotonic', side_effect=[0, 1]):
            result = s.drain(self.catalog, self.gh, self.zenodo, self.pages, budget_seconds=0.5)
        self.assertEqual((result['processed'], result['remaining']), (0, 1))
        self.assertEqual(self.zenodo.calls, [])

    def test_a_failed_note_does_not_prevent_an_independent_note(self):
        self.catalog = catalog('alpha', 'beta')
        self.enqueue('alpha')
        self.enqueue('beta')
        self.zenodo.fail_after = ('POST', '/deposit/depositions')
        result = s.drain(self.catalog, self.gh, self.zenodo, self.pages)
        self.assertEqual(result['processed'], 1)
        self.assertEqual(result['failed'][0]['id'], 'alpha')
        self.assertEqual(self.state('beta')['publication']['url'], 'https://doi.org/10.5281/zenodo.2')
        self.assertNotIn('publication', self.state('alpha'))

    def test_lost_publish_response_requires_explicit_retry_and_reuses_the_record(self):
        self.catalog = catalog('alpha')
        self.enqueue()
        self.zenodo.fail_after = ('POST', '/deposit/depositions/1/actions/publish')
        first = s.drain(self.catalog, self.gh, self.zenodo, self.pages)
        self.assertTrue(first['failed'])
        self.assertEqual(s.drain(self.catalog, self.gh, self.zenodo, self.pages)['processed'], 0)
        retried = s.drain(self.catalog, self.gh, self.zenodo, self.pages, retry_failed=True)
        self.assertEqual(retried['failed'], [])
        self.assertEqual(self.zenodo.calls.count(('POST', '/deposit/depositions/1/actions/publish')), 1)

    def ci(self, dispatch=False, **environment):
        path = self.root / 'ci-catalog.json'
        path.write_bytes(w.encode(self.catalog))
        output, public = io.StringIO(), []
        from managed.run_log import OUTPUT
        token = OUTPUT.set(public.append)
        self.addCleanup(OUTPUT.reset, token)
        with patch.dict(os.environ, {'NOTE_ACTION': 'drain', 'GITHUB_ACTIONS': 'true',
                                     'PAGES_TOKEN': 'fake-pages', 'ZENODO_TOKEN': 'fake-zenodo',
                                     **environment}, clear=True), \
                patch.object(w, 'GitHub', side_effect=lambda repo, *args: self.gh if repo == w.OUTREACH else self.pages), \
                patch.object(w, 'Zenodo', return_value=self.zenodo), \
                patch.object(s, 'load_submission_board', return_value=None), \
                contextlib.redirect_stdout(output):
            if dispatch:
                from managed import hosted
                from scripts.short_notes import service as packaged
                with patch.object(hosted, 'restore_credentials'), \
                        patch.object(hosted, 'feed_cache', side_effect=contextlib.nullcontext), \
                        patch.object(packaged, 'main', side_effect=lambda args: s.main(['--catalog', str(path), *args])):
                    code = hosted.command('notes')
            else:
                code = s.main(['--catalog', str(path), 'ci'])
        return code, json.loads(output.getvalue()), public

    def test_recorded_failure_is_visible_but_does_not_fail_every_scheduled_run(self):
        self.catalog = catalog('alpha', 'beta')
        self.enqueue('alpha')
        self.zenodo.fail_after = ('POST', '/deposit/depositions')
        code, first, public = self.ci()
        self.assertEqual(code, 1)
        self.assertEqual(len(first['needs_attention']), 1)
        self.assertIn('failed=1', public[0])
        retained = copy.deepcopy(self.state())
        self.enqueue('beta')
        code, next_run, public = self.ci()
        self.assertEqual(code, 0)
        self.assertEqual(next_run['status'], 'needs_attention')
        self.assertEqual(next_run['processed'], 1)
        self.assertEqual(next_run['failed'], [])
        self.assertEqual(next_run['needs_attention'][0]['id'], 'alpha')
        self.assertEqual(self.state(), retained)
        self.assertEqual(self.zenodo.calls.count(('POST', '/deposit/depositions')), 2)
        self.assertIn('needs_attention=1', public[0])
        self.assertNotIn('alpha', public[0])
        code, idle, public = self.ci()
        self.assertEqual((code, idle['processed'], len(idle['needs_attention'])), (0, 0, 1))
        self.assertEqual(s.note_status(self.catalog, 'alpha', self.gh)['requests'][0]['status'], 'failed')

    def test_ci_recovers_one_request_preserving_collection_package_and_history(self):
        self.catalog = catalog('alpha', 'beta')
        queued = self.enqueue('alpha')
        collection, responses = self.collection()
        with patch.object(s, 'public_mathesis', side_effect=responses.__getitem__):
            s.record_mathesis(self.catalog, 'alpha', collection, self.gh, revision=1)
        self.zenodo.fail_after = ('POST', '/deposit/depositions')
        self.assertEqual(self.ci()[0], 1)
        before = copy.deepcopy(self.state())
        assets = copy.deepcopy(self.gh.releases[w.tag_for('alpha')])
        self.enqueue('beta')
        with patch.object(s, 'record_mathesis', side_effect=AssertionError('collected manuscript redelivered')):
            code, result, _ = self.ci(dispatch=True, NOTE_USE_INPUTS='true', INPUT_NOTE_ACTION='publish',
                                     INPUT_NOTE_ID='alpha', INPUT_NOTE_REVISION='1', INPUT_NOTE_RECOVER_RECORD='1',
                                     OUTREACH_NOTE_REQUEST='must never be parsed')
        self.assertEqual(code, 0)
        self.assertEqual((result['processed'], result['remaining'], result['needs_attention']), (1, 1, []))
        after = self.state()
        self.assertEqual(after['versions']['1']['package_id'], before['versions']['1']['package_id'])
        self.assertEqual(after['mathesis_publication'], before['mathesis_publication'])
        self.assertEqual(after['deliveries'][queued['request']], before['deliveries'][queued['request']])
        for name, raw in assets.items():
            self.assertEqual(self.gh.releases[w.tag_for('alpha')][name], raw)
        self.assertEqual(len(self.zenodo.records), 1)
        self.assertEqual(self.zenodo.calls.count(('POST', '/deposit/depositions')), 1)
        self.assertEqual(s.queue(self.catalog, self.gh)[0]['status'], 'recovered')
        calls = list(self.zenodo.calls)
        self.assertEqual(self.ci(NOTE_ACTION='publish', NOTE_ID='alpha', NOTE_REVISION='1', RECOVER_RECORD='1')[0], 0)
        self.assertEqual(calls, self.zenodo.calls)

    def test_ci_retry_without_recovered_id_keeps_creation_fence(self):
        self.catalog = catalog('alpha')
        self.enqueue()
        self.zenodo.fail_after = ('POST', '/deposit/depositions')
        self.assertEqual(self.ci()[0], 1)
        code, result, _ = self.ci(NOTE_ACTION='publish', NOTE_ID='alpha', NOTE_REVISION='1')
        self.assertEqual(code, 1)
        self.assertIn('Uncertain Zenodo creation', result['failed'][0]['error'])
        self.assertEqual(self.zenodo.calls.count(('POST', '/deposit/depositions')), 1)

    def test_workflow_inputs_require_valid_complete_selection(self):
        valid = {'NOTE_USE_INPUTS': 'true', 'INPUT_NOTE_ACTION': 'publish',
                 'INPUT_NOTE_ID': 'alpha', 'INPUT_NOTE_REVISION': '3',
                 'INPUT_NOTE_RECOVER_RECORD': '42', 'INPUT_NOTE_REUSE_DRAFT_FROM': '2'}
        self.assertEqual(s.workflow_selection(valid), {**request('alpha', 3), 'recover_record': 42, 'reuse_draft_from': 2})
        for changes in ({'INPUT_NOTE_ID': ''}, {'INPUT_NOTE_ID': '../alpha'},
                        {'INPUT_NOTE_REVISION': ''}, {'INPUT_NOTE_REVISION': '0'},
                        {'INPUT_NOTE_REVISION': 'true'}, {'INPUT_NOTE_RECOVER_RECORD': '-1'},
                        {'INPUT_NOTE_RECOVER_RECORD': '1.0'}, {'INPUT_NOTE_REUSE_DRAFT_FROM': '3'},
                        {'INPUT_NOTE_ACTION': 'promote'}, {'INPUT_NOTE_ACTION': 'drain'}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                s.workflow_selection({**valid, **changes})
        self.assertIsNone(s.workflow_selection({**valid, 'NOTE_USE_INPUTS': 'false'}))
        with self.assertRaises(ValueError):
            s.validate_request({**request('alpha'), 'recover_record': True})

    def test_pages_failure_is_automatically_resumable_without_another_doi(self):
        self.catalog = catalog('alpha')
        self.enqueue()
        self.pages.fail_dispatch = True
        first = s.drain(self.catalog, self.gh, self.zenodo, self.pages)
        self.assertEqual(first['failed'][0]['id'], 'pages')
        self.assertEqual(s.queue(self.catalog, self.gh)[0]['status'], 'awaiting_pages')
        self.pages.fail_dispatch = False
        retried = s.drain(self.catalog, self.gh, self.zenodo, self.pages)
        self.assertEqual(retried['remaining'], 0)
        self.assertEqual(len(self.zenodo.records), 1)
        self.assertEqual(self.zenodo.calls.count(('POST', '/deposit/depositions/1/actions/publish')), 1)

    def test_missing_credentials_do_not_poison_pending_requests(self):
        self.catalog = catalog('alpha')
        self.enqueue()
        before = copy.deepcopy(self.gh.bodies)
        with self.assertRaisesRegex(ValueError, 'ZENODO_TOKEN'):
            s.drain(self.catalog, self.gh, None, self.pages)
        with self.assertRaisesRegex(ValueError, 'PAGES_TOKEN'):
            s.drain(self.catalog, self.gh, self.zenodo, None)
        self.assertEqual(before, self.gh.bodies)

    def test_pause_blocks_delivery_and_keeps_an_already_public_citation(self):
        self.publish()
        self.catalog['notes'][0]['paused'] = True
        self.assertFalse(s.queue(self.catalog, self.gh)[0]['eligible'])
        self.assertEqual(s.projection(self.catalog, self.gh)['publications'][0]['id'], 'alpha')
        with self.assertRaisesRegex(ValueError, 'paused'):
            s.enqueue(self.catalog, request('alpha', 2), self.gh)

    def test_full_replacement_removes_ended_collaborations_and_withdrawn_notes(self):
        state = self.publish()
        collaboration = {'id': 'joint-alpha', 'problem_ids': ['alpha'], 'topic': 'A definite joint project',
                         'status': 'confirmed_active', 'people': [{'name': 'A. Researcher', 'institution': 'Institute'}],
                         'evidence_url': 'https://sample.example.invalid/project'}
        self.catalog['collaborations'] = [collaboration]
        s.synchronize(self.catalog, self.gh, self.pages)
        self.catalog['notes'], self.catalog['collaborations'] = [], []
        s.synchronize(self.catalog, self.gh, self.pages)
        feed = json.loads(self.pages.releases['research-updates']['research-updates.v1.json'])
        self.assertEqual((feed['publications'], feed['collaborations']), ([], []))
        self.assertEqual(self.state()['versions'], state['versions'])
        self.assertEqual(len(self.zenodo.records), 1)

    def test_public_catalog_rejects_private_fields_and_unconfirmed_relationships(self):
        c = catalog()
        for changed in ({**c, 'contact_email': 'private@sample.example.invalid'},
                        {**c, 'collaborations': [{'status': 'active_joint_collaboration'}]}):
            with self.assertRaises(ValueError):
                s.validate_catalog(changed)

    def test_changing_catalog_problem_links_cannot_publish_an_old_queued_mapping(self):
        self.catalog = catalog('alpha')
        self.enqueue()
        self.catalog['notes'][0]['problem_ids'] = ['unrelated-problem']
        result = s.drain(self.catalog, self.gh, self.zenodo, self.pages)
        self.assertTrue(result['failed'])
        self.assertEqual(self.zenodo.calls, [])

    def test_latest_promotion_supersedes_an_older_failed_request(self):
        state = self.publish()
        old = {**state['publication'], 'url': 'https://doi.org/10.1234/old-paper', 'venue': 'First Journal'}
        new = {**old, 'url': 'https://doi.org/10.1234/new-paper', 'venue': 'Later Journal'}
        first = {'schema_version': 'short-note-request.v1', 'id': 'alpha', 'action': 'promote', 'publication': old}
        s.enqueue(self.catalog, first, self.gh)
        old_row = next(row for row in s.queue(self.catalog, self.gh) if row['operation']['action'] == 'promote')
        s.mark_delivery(self.gh, old_row, 'failed', 'temporary failure')
        s.enqueue(self.catalog, {**first, 'publication': new}, self.gh)
        result = s.drain(self.catalog, self.gh, self.zenodo, self.pages, retry_failed=True)
        self.assertEqual(result['failed'], [])
        self.assertEqual(self.state()['publication'], new)
        self.assertEqual(len(self.zenodo.records), 1)
        self.assertEqual(next(row['status'] for row in s.queue(self.catalog, self.gh) if row['request'] == old_row['request']), 'superseded')

    def test_revisions_keep_the_concept_and_handoff_contains_only_public_material(self):
        self.publish()
        self.enqueue('alpha', 2)
        s.drain(self.catalog, self.gh, self.zenodo, self.pages)
        state = self.state()
        state['internal_mail_id'] = 'private-correspondence'
        self.gh.save_state(w.tag_for('alpha'), state)
        exported = s.handoff(self.catalog, self.gh)
        self.assertEqual(len(exported['notes'][0]['versions']), 2)
        self.assertNotIn('private-correspondence', json.dumps(exported))
        self.assertTrue(all(url.startswith('https://zenodo.org/records/')
                            for version in exported['notes'][0]['versions'] for url in version['files'].values()))
        self.assertEqual(state['versions']['1']['concept_doi'], state['versions']['2']['concept_doi'])

    def test_sandbox_is_excluded_from_both_feed_and_handoff(self):
        state = self.publish()
        state['zenodo_api'] = 'https://sandbox.zenodo.org/api'
        self.gh.save_state(w.tag_for('alpha'), state)
        for operation in (s.projection, s.handoff):
            with self.assertRaisesRegex(ValueError, 'Sandbox'):
                operation(self.catalog, self.gh)

    def public_index(self, state):
        public = w.project_pages_feed(s.projection(self.catalog, self.gh))
        return {'schema_version': 'publication-index.v1', 'updated_at': state['pages']['feed_updated_at'],
                'publications': [{**p, 'id': 'paper-' + p['id']} for p in public['publications']],
                'notes': public['notes']}

    def test_reservation_never_publishes_or_projects_a_draft_and_publish_reuses_it(self):
        self.catalog = catalog('alpha')
        self.package('alpha')
        state = w.operate('reserve', 'alpha', 1, self.gh, self.zenodo)
        self.assertEqual(state['versions']['1']['reserved_doi'], '10.5281/zenodo.1')
        self.assertFalse(self.zenodo.records[1]['submitted'])
        self.assertEqual(s.projection(self.catalog, self.gh)['publications'], [])
        self.assertNotIn('notes', s.projection(self.catalog, self.gh))
        self.assertEqual(s.coverage(self.catalog, self.gh)['missing_doi'], ['alpha'])
        self.assertEqual(self.pages.calls, [])
        w.operate('reserve', 'alpha', 1, self.gh, self.zenodo)
        w.operate('publish', 'alpha', 1, self.gh, self.zenodo)
        self.assertEqual(len(self.zenodo.records), 1)
        self.assertTrue(self.zenodo.records[1]['submitted'])
        self.assertEqual(s.projection(self.catalog, self.gh)['notes'][0]['doi'], 'https://doi.org/10.5281/zenodo.1')

    def test_reservation_lost_creation_response_never_creates_a_second_deposit(self):
        self.package('alpha')
        self.zenodo.fail_after = ('POST', '/deposit/depositions')
        with self.assertRaises(TimeoutError):
            w.operate('reserve', 'alpha', 1, self.gh, self.zenodo)
        with self.assertRaisesRegex(RuntimeError, 'Uncertain Zenodo creation'):
            w.operate('reserve', 'alpha', 1, self.gh, self.zenodo)
        w.operate('reserve', 'alpha', 1, self.gh, self.zenodo, recover_record=1)
        self.assertEqual(len(self.zenodo.records), 1)
        self.assertFalse(self.zenodo.records[1]['submitted'])

    def mathesis_receipt(self):
        package = json.loads(self.gh.releases[w.tag_for('alpha')]['note-v1.json'])
        return {'schema': 'mathesis.manuscript-receipt.v1', 'producer': 'trureturing-outreach',
                'work_id': 'alpha', 'upstream_version': '1', 'version': 2,
                'source_sha256': package['files']['source-v1.zip'],
                'publication': {'public': True, 'version': 2, 'record': 'WP-2026-0123'}}

    def test_mathesis_public_source_links_survive_preferred_journal_citation(self):
        state = self.publish()
        s.record_mathesis(self.catalog, 'alpha', self.mathesis_receipt(), self.gh)
        journal = {**state['publication'], 'url': 'https://doi.org/10.1234/journal', 'status': 'published'}
        state = w.promote(self.state(), journal)
        self.gh.save_state(w.tag_for('alpha'), state)
        feed = s.projection(self.catalog, self.gh)
        self.assertEqual(feed['publications'][0]['url'], journal['url'])
        self.assertEqual(feed['notes'][0]['doi'], 'https://doi.org/10.5281/zenodo.1')
        self.assertIn('/papers/', feed['notes'][0]['mathesis_url'])
        self.assertEqual(s.coverage(self.catalog, self.gh)['missing_mathesis'], [])
        self.assertEqual(s.handoff(self.catalog, self.gh)['notes'][0]['versions'][0]['sha256'],
                         json.loads(self.gh.releases[w.tag_for('alpha')]['note-v1.json'])['files'])

    def test_private_wrong_version_and_wrong_source_mathesis_receipts_cannot_export(self):
        self.publish()
        for change in ({'publication': None}, {'producer': 'other'}, {'work_id': 'other'},
                       {'source_sha256': '0' * 64}, {'publication': {'public': False, 'version': 2}},
                       {'publication': {'public': True, 'version': 1}}):
            with self.assertRaises(ValueError):
                s.record_mathesis(self.catalog, 'alpha', {**self.mathesis_receipt(), **change}, self.gh)
        self.assertNotIn('mathesis_publication', self.state())

    def collection(self, version=1):
        files = self.gh.releases[w.tag_for('alpha')]
        package = json.loads(files['note-v1.json'])
        meta = package['metadata']
        endpoint = '/api/v1/collected-papers/alpha'
        receipt = {'schema': 'mathesis.pre-reviewed-receipt.v1', 'status': 'collected',
                   'work_id': 'alpha', 'version': version, 'public_path': f'/papers/alpha?version={version}',
                   'pdf_sha256': package['files']['note-v1.pdf'],
                   'source_sha256': package['files']['source-v1.zip']}
        paper = {'id': 'alpha', 'revision': str(version), 'title': meta['title'],
                 'abstract': meta['abstract'], 'authors': meta['creators'], 'license': 'CC-BY-4.0',
                 **{kind: {'url': endpoint + f'/files/{kind}?version={version}'} for kind in ('pdf', 'source')}}
        responses = {endpoint + f'?version={version}': w.encode(paper),
                     paper['pdf']['url']: files['note-v1.pdf'], paper['source']['url']: files['source-v1.zip']}
        return receipt, responses

    def test_collected_link_queue_exports_only_versioned_mathesis_reading_link(self):
        self.package(); self.catalog = catalog('alpha')
        receipt, responses = self.collection()
        endpoint = '/api/v1/collected-papers/alpha?version=1'
        paper = json.loads(responses[endpoint])
        paper['authors'][0]['name'] = 'A. Example'
        responses[endpoint] = w.encode(paper)
        request = {'schema_version': 'short-note-request.v1', 'id': 'alpha', 'action': 'link-mathesis',
                   'revision': 1, 'receipt': receipt}
        first = s.enqueue(self.catalog, request, self.gh)
        self.assertEqual(first, s.enqueue(self.catalog, request, self.gh))
        with patch.object(s, 'public_mathesis', side_effect=responses.__getitem__):
            result = s.drain(self.catalog, self.gh, None, self.pages)
        self.assertEqual(result['failed'], [])
        self.assertEqual(result['processed'], 1)
        self.assertEqual(s.queue(self.catalog, self.gh)[0]['status'], 'dispatched')
        feed = json.loads(self.pages.releases['research-updates']['research-updates.v1.json'])
        self.assertEqual(feed['publications'], [])
        self.assertEqual(feed['notes'][0]['mathesis_url'],
                         'https://mathesis.example.invalid/papers/alpha?version=1')
        self.assertNotIn('doi', feed['notes'][0])
        index = {**feed, 'schema_version': 'publication-index.v1'}
        self.assertEqual(s.verify_feed(self.catalog, self.gh, pages=self.pages, fetch=lambda: index)['status'], 'deployed')
        self.assertEqual(self.state()['mathesis_publication']['pdf_sha256'], receipt['pdf_sha256'])
        self.assertEqual(self.zenodo.records, {})

    def test_collection_receipt_requires_both_files_and_public_bibliography(self):
        self.package(); self.catalog = catalog('alpha')
        receipt, responses = self.collection()
        for changes in [{'status': 'pending'}, {'work_id': 'other'}, {'version': True},
                        {'public_path': '/papers/other?version=1'}, {'source_sha256': '0' * 64},
                        {'pdf_sha256': '0' * 64}]:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                s.record_mathesis(self.catalog, 'alpha', {**receipt, **changes}, self.gh,
                                  fetch=responses.__getitem__)
        endpoint = '/api/v1/collected-papers/alpha?version=1'
        for changes in [{'title': 'Other work'}, {'revision': '2'}, {'authors': [{'name': 'Other author'}]}]:
            changed = {**responses, endpoint: w.encode({**json.loads(responses[endpoint]), **changes})}
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                s.record_mathesis(self.catalog, 'alpha', receipt, self.gh, fetch=changed.__getitem__)
        corrupted = {**responses, '/api/v1/collected-papers/alpha/files/pdf?version=1': b'%PDF-wrong'}
        with self.assertRaisesRegex(ValueError, 'public files'):
            s.record_mathesis(self.catalog, 'alpha', receipt, self.gh, fetch=corrupted.__getitem__)
        self.assertIsNone(self.state())

    def test_collection_versions_do_not_rename_or_downgrade_the_outreach_revision(self):
        self.package(); self.catalog = catalog('alpha')
        receipt, responses = self.collection(3)
        s.record_mathesis(self.catalog, 'alpha', receipt, self.gh, revision=1, fetch=responses.__getitem__)
        public = s.projection(self.catalog, self.gh)['notes'][0]
        self.assertEqual(public['revision'], 1)
        self.assertTrue(public['mathesis_url'].endswith('/papers/alpha?version=3'))
        stale, responses = self.collection(2)
        with self.assertRaisesRegex(ValueError, 'newer Mathesis collection'):
            s.record_mathesis(self.catalog, 'alpha', stale, self.gh, revision=1, fetch=responses.__getitem__)
        self.assertEqual(self.state()['mathesis_publication']['mathesis_version'], 3)

    def test_collected_reading_urls_require_matching_work_and_exact_version(self):
        self.package(); self.catalog = catalog('alpha')
        receipt, responses = self.collection()
        s.record_mathesis(self.catalog, 'alpha', receipt, self.gh, fetch=responses.__getitem__)
        note = s.projection(self.catalog, self.gh)['notes'][0]
        for path in ['/papers/other?version=1', '/papers/alpha', '/papers/alpha?version=0',
                     '/papers/alpha?version=01', '/papers/alpha?version=1&private=true',
                     '/papers/alpha?version=1#other', '/submissions/alpha?version=1']:
            with self.subTest(path=path), self.assertRaises(ValueError):
                w.validate_public_note({**note, 'mathesis_url':
                                       'https://mathesis.example.invalid' + path})

    def test_deployment_requires_the_actual_note_links_as_well_as_paper(self):
        state = self.publish_paper()
        index = self.public_index(state)
        index.pop('notes')
        self.assertEqual(s.verify_deployment(self.catalog, 'alpha', self.gh, fetch=lambda: index)['status'],
                         'verification_pending')

    def test_whole_feed_readback_requires_actual_relationships_and_note_links(self):
        state = self.publish_paper()
        exchange = {'id': 'exchange', 'problem_ids': ['alpha'], 'topic': 'A next question',
                    'status': 'research_exchange', 'people': [{'name': 'A', 'institution': 'University'}],
                    'summary': 'Checking the equality case.', 'evidence_url': 'https://sample.example.invalid/science'}
        self.catalog['collaborations'] = [exchange]
        index = {**self.public_index(state), 'collaborations': [exchange]}
        self.assertEqual(s.verify_feed(self.catalog, self.gh, pages=self.pages, fetch=lambda: index)['status'], 'deployed')
        stale = {**index, 'collaborations': []}
        self.assertEqual(s.verify_feed(self.catalog, self.gh, pages=self.pages, fetch=lambda: stale)['status'], 'verification_pending')

    def test_zenodo_archive_is_retained_for_mathesis_without_pages_display(self):
        state = self.publish()
        self.assertEqual(s.coverage(self.catalog, self.gh)['notes'][0]['pages'], 'not_applicable')
        self.assertEqual(s.verify_deployment(self.catalog, 'alpha', self.gh,
                         fetch=lambda: self.fail('An archive needs no Pages lookup'))['status'], 'not_applicable')
        exported = s.handoff(self.catalog, self.gh)
        self.assertEqual(exported['notes'][0]['public_links']['doi'], 'https://doi.org/10.5281/zenodo.1')
        self.assertTrue(exported['notes'][0]['versions'][0]['files'])
        served = json.loads(self.pages.releases['research-updates']['research-updates.v1.json'])
        self.assertEqual(served['publications'], [])
        self.assertEqual(served['notes'], [])
        index = {**served, 'schema_version': 'publication-index.v1'}
        self.assertEqual(s.verify_feed(self.catalog, self.gh, pages=self.pages, fetch=lambda: index)['status'], 'deployed')
        index['publications'] = [state['publication']]
        self.assertEqual(s.verify_feed(self.catalog, self.gh, pages=self.pages, fetch=lambda: index)['status'], 'verification_pending')

    def test_mathesis_only_mapping_is_verified_without_a_research_publication(self):
        self.publish()
        s.record_mathesis(self.catalog, 'alpha', self.mathesis_receipt(), self.gh)
        s.synchronize(self.catalog, self.gh, self.pages)
        state = self.state()
        index = self.public_index(state)
        self.assertEqual(index['publications'], [])
        self.assertIn('mathesis_url', index['notes'][0])
        self.assertNotIn('doi', index['notes'][0])
        self.assertEqual(s.verify_deployment(self.catalog, 'alpha', self.gh, fetch=lambda: index)['status'], 'deployed')
        self.assertEqual(s.coverage(self.catalog, self.gh)['notes'][0]['pages'], 'deployed')
        stale = {**index, 'notes': []}
        self.assertEqual(s.verify_feed(self.catalog, self.gh, pages=self.pages, fetch=lambda: stale)['status'], 'verification_pending')

    def test_full_result_catalog_can_be_projected_and_delivered(self):
        state = self.publish()
        c = catalog()
        for n in range(1600):
            c['publications'].append({**state['publication'], 'id': f'result-{n}',
                'problem_ids': [f'result-{n}'], 'url': f'https://arxiv.org/abs/2610.{n + 10000}',
                'summary': 'A checked mathematical statement with its precise scope. ' * 6})
        feed = s.projection(c, self.gh)
        self.assertGreater(len(w.encode(feed)), 1024 * 1024)
        replacement = w.replace_publication(feed, {**feed['publications'][0], 'venue': 'Zenodo preprint'})
        w.deliver_feed(replacement, self.pages)
        served = json.loads(self.pages.releases['research-updates']['research-updates.v1.json'])
        self.assertEqual(served['publications'], replacement['publications'])
        c['publications'][0]['summary'] = 'x' * w.MAX_FEED_BYTES
        uploads = len(self.pages.uploads)
        with self.assertRaisesRegex(ValueError, 'Pages limit'):
            s.projection(c, self.gh)
        with self.assertRaisesRegex(ValueError, '8 MiB'):
            w.deliver_feed({**feed, 'publications': c['publications']}, self.pages)
        self.assertEqual(len(self.pages.uploads), uploads)

    def test_large_public_index_is_read_back_with_a_finite_budget(self):
        state = self.publish_paper()
        index = {**self.public_index(state), 'results': ['unselected result ' * 140000]}
        raw = w.encode(index)
        self.assertGreater(len(raw), 2 * 1024 * 1024)
        with patch('service.urlopen', side_effect=lambda *a, **kw: io.BytesIO(raw)):
            self.assertEqual(s.verify_deployment(self.catalog, 'alpha', self.gh)['status'], 'deployed')
            self.assertEqual(s.verify_feed(self.catalog, self.gh, pages=self.pages)['status'], 'deployed')
        oversized = w.encode({**index, 'results': ['x' * w.MAX_INDEX_BYTES]})
        with patch('service.urlopen', side_effect=lambda *a, **kw: io.BytesIO(oversized)):
            for verify in (lambda: s.verify_deployment(self.catalog, 'alpha', self.gh),
                           lambda: s.verify_feed(self.catalog, self.gh, pages=self.pages)):
                result = verify()
                self.assertEqual(result['status'], 'verification_pending')
                self.assertIn('budget', result['error'])

    def test_idle_worker_syncs_catalog_only_updates_without_another_publication(self):
        state = self.publish()
        self.catalog['notes'] = []
        self.catalog['publications'] = [state['publication']]
        self.catalog['collaborations'] = [{'id': 'exchange', 'problem_ids': ['alpha'], 'topic': 'A next question',
                                          'status': 'research_exchange', 'people': [{'name': 'A', 'institution': 'University'}],
                                          'evidence_url': 'https://sample.example.invalid/research'}]
        calls = len(self.zenodo.calls)
        result = s.drain(self.catalog, self.gh, None, self.pages)
        self.assertEqual(result['status'], 'dispatch_accepted')
        actual = json.loads(self.pages.releases['research-updates']['research-updates.v1.json'])
        self.assertEqual(actual['collaborations'], self.catalog['collaborations'])
        self.assertEqual(actual['notes'], [])
        dispatches = len(self.pages.dispatches)
        index = {**actual, 'schema_version': 'publication-index.v1'}
        self.assertEqual(s.drain(self.catalog, self.gh, None, self.pages, fetch=lambda: index)['status'], 'idle')
        self.assertEqual(len(self.pages.dispatches), dispatches)
        self.assertEqual(len(self.zenodo.calls), calls)

    def test_scientific_handoff_archive_excludes_surrounding_correspondence(self):
        from managed.handoff import prepare, archive
        _, directory = self.package('alpha')
        output = self.root / 'handoff'
        prepare(directory, output)
        (output / 'private-thread.txt').write_text('must stay local')
        first = archive(output, self.root / 'one.zip')
        second = archive(output, self.root / 'two.zip')
        self.assertEqual(first['sha256'], second['sha256'])
        with zipfile.ZipFile(first['archive']) as bundle:
            self.assertEqual(set(bundle.namelist()), {'handoff.json', 'note-v1.pdf', 'source-v1.zip'})
        (output / 'source-v1.zip').write_bytes(b'changed')
        with self.assertRaisesRegex(ValueError, 'immutable'):
            archive(output, self.root / 'reject.zip')

    def test_dispatch_is_distinct_from_verified_deployment(self):
        state = self.publish_paper()
        self.assertEqual(state['pages']['status'], 'dispatch_accepted')
        index = self.public_index(state)
        result = s.verify_deployment(self.catalog, 'alpha', self.gh, fetch=lambda: index)
        self.assertEqual(result['status'], 'deployed')
        self.assertEqual(self.state()['pages']['status'], 'deployed')

    def test_stale_or_unavailable_site_does_not_become_verified(self):
        state = self.publish_paper()
        for change in ('time', 'doi', 'title'):
            index = self.public_index(state)
            if change == 'time':
                index['updated_at'] = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
            elif change == 'doi':
                index['publications'][0]['url'] = 'https://doi.org/10.1234/another-paper'
            else:
                index['publications'][0]['title'] = 'A stale manuscript title'
            result = s.verify_deployment(self.catalog, 'alpha', self.gh, fetch=lambda: index)
            self.assertEqual(result['status'], 'verification_pending')
        def unavailable():
            raise OSError('HTTP 403')
        result = s.verify_deployment(self.catalog, 'alpha', self.gh, fetch=unavailable)
        self.assertEqual(result['status'], 'verification_pending')
        self.assertIn('403', result['error'])

    def test_initial_or_malformed_public_index_remains_pending(self):
        state = self.publish_paper()
        index = self.public_index(state)
        # Pages emits updated_at: null before its first released feed is consumed.
        for served in (None, [], {}, {**index, 'updated_at': None},
                       {**index, 'updated_at': '2026-10-28T00:00:00'}):
            result = s.verify_deployment(self.catalog, 'alpha', self.gh, fetch=lambda: served)
            self.assertEqual(result['status'], 'verification_pending')
            self.assertEqual(self.state()['pages']['status'], 'verification_pending')

    def test_idle_queue_needs_no_publication_credentials(self):
        result = s.drain(catalog(), self.gh, None, None)
        self.assertEqual(result, {'processed': 0, 'failed': [], 'remaining': 0, 'needs_attention': [], 'status': 'idle'})
        self.assertEqual(self.gh.releases, {})

    def test_doctor_missing_configuration_is_reported_without_mutations(self):
        result = s.doctor(catalog(), self.gh, remote=True)
        self.assertFalse(result['ready'])
        self.assertFalse(result['checks']['pages_token_configured'])
        self.assertEqual(self.gh.releases, {})

    def test_doctor_exposes_zenodo_status_without_response_or_credential_text(self):
        with patch.object(self.zenodo, 'request', side_effect=w.ZenodoHTTPError('GET', 403)):
            result = s.doctor(catalog(), self.gh, zenodo=self.zenodo, remote=True)
        self.assertFalse(result['checks']['zenodo_account_accessible'])
        self.assertEqual(result['diagnostics']['zenodo'],
                         {'error_type': 'ZenodoHTTPError', 'http_status': 403})
        self.assertEqual(self.zenodo.records, {})

    def test_fixture_catalog_can_be_projected_without_a_remote_note_release(self):
        actual = s.validate_catalog(catalog())
        feed = s.projection(actual, self.gh)
        w.validate_feed(feed)
        self.assertEqual(len(feed['publications']), len(actual['publications']))
        self.assertEqual(feed['collaborations'], sorted(actual['collaborations'], key=lambda row: row['id']))

    def test_verified_independent_submission_uses_the_same_public_feed(self):
        actual = s.validate_catalog(catalog())
        registered = {
            'id': 'registered-paper', 'problem_ids': ['problem-one'], 'topic': 'Graph theory',
            'title': 'A coloring result', 'authors': 'Blaire Mercer, Avery Reed',
            'date': '2026-10-31', 'url': 'https://arxiv.org/abs/2099.00001',
            'venue': 'arXiv:2099.00001', 'status': 'preprint', 'summary': 'A coloring result.',
            'evidence_url': 'https://arxiv.org/abs/2099.00001',
        }
        board = {
            'schema_version': 1, 'updated_on': '2026-11-04',
            'scope': 'independent-team',
            'papers': [{
                'id': 'coloring-example',
                'title': 'A coloring result',
                'authors': ['Blaire Mercer', 'Avery Reed'],
                'version_date': '2026-11-01', 'stage': 'submitted',
                'target_journal': 'Example Journal',
                'blocker': 'arXiv v1 public.', 'next_step_owner': 'Blaire Mercer',
                'workspace': '../mail/example-contact/submission/coloring-example',
                'arxiv_id': '2099.00001', 'arxiv_url': 'https://arxiv.org/abs/2099.00001',
                'arxiv_version': 'v1', 'arxiv_status': 'public',
                'arxiv_title': 'A coloring result',
                'arxiv_date': '2026-10-31', 'arxiv_evidence': 'https://arxiv.org/abs/2099.00001',
            }],
        }
        feed = s.projection(actual, self.gh, board)
        paper = next(row for row in feed['publications'] if row['id'] == 'coloring-example')
        self.assertEqual(paper['url'], 'https://arxiv.org/abs/2099.00001')
        self.assertEqual(paper['status'], 'preprint')
        self.assertEqual(paper['title'], board['papers'][0]['arxiv_title'])
        self.assertNotIn('workspace', json.dumps(feed))

        # Once the catalog registers the same arXiv paper, its entry wins.
        actual['publications'].append(registered)
        feed = s.projection(actual, self.gh, board)
        rows = [row for row in feed['publications'] if 'arxiv.org/abs/2099.00001' in row['url']]
        self.assertEqual([row['id'] for row in rows], [registered['id']])
        self.assertEqual(rows[0]['summary'], registered['summary'])

    def test_unpublished_submission_does_not_enter_public_feed(self):
        actual = s.validate_catalog(catalog())
        board = {'schema_version': 1, 'updated_on': '2026-11-04',
                 'scope': 'independent-team', 'papers': []}
        feed = s.projection(actual, self.gh, board)
        self.assertEqual(len(feed['publications']), len(actual['publications']))

    def test_cli_validation_and_offline_projection_emit_machine_readable_reports(self):
        path = self.root / 'catalog.json'
        path.write_bytes(w.encode(catalog()))
        for command in ('validate', 'project'):
            output = io.StringIO()
            with contextlib.redirect_stdout(output), patch.object(s, 'load_submission_board', return_value=None):
                self.assertEqual(s.main(['--catalog', str(path), command]), 0, output.getvalue())
            self.assertIsInstance(json.loads(output.getvalue()), dict)

    def test_idle_worker_does_not_download_completed_request_assets(self):
        self.publish()
        with patch.object(self.gh, 'download', side_effect=AssertionError('Completed request was downloaded again')):
            self.assertEqual(s.drain(self.catalog, self.gh, None, None)['status'], 'idle')

    def test_a_request_arriving_during_publication_is_preserved(self):
        self.catalog = catalog('alpha', 'beta')
        self.enqueue('alpha')
        self.package('beta')
        original = self.zenodo.request
        arrived = False
        def during_publish(method, path, data=None):
            nonlocal arrived
            result = original(method, path, data)
            if path == '/deposit/depositions' and method == 'POST' and not arrived:
                arrived = True
                s.enqueue(self.catalog, request('beta'), self.gh)
            return result
        self.zenodo.request = during_publish
        first = s.drain(self.catalog, self.gh, self.zenodo, self.pages)
        self.assertEqual((first['processed'], first['remaining']), (1, 1))
        self.assertEqual(s.drain(self.catalog, self.gh, self.zenodo, self.pages)['remaining'], 0)

    def test_status_distinguishes_archive_from_pages_display(self):
        state = self.publish()
        result = s.note_status(self.catalog, 'alpha', self.gh)
        self.assertEqual(result['preferred_publication']['url'], state['publication']['url'])
        self.assertEqual(result['pages']['status'], 'not_applicable')
        self.assertEqual(result['requests'][0]['status'], 'dispatched')

    def test_cli_failure_still_saves_a_structured_diagnostic_report(self):
        missing, report = self.root / 'missing.json', self.root / 'diagnostic.json'
        with contextlib.redirect_stdout(io.StringIO()):
            result = s.main(['--catalog', str(missing), '--output', str(report), 'validate'])
        self.assertEqual(result, 1)
        self.assertEqual(w.read(report)['status'], 'failed')


if __name__ == '__main__':
    unittest.main()


def setUpModule():
    from managed.location import example_config
    fixture = example_config()
    mocked = patch('managed.location.load_config', return_value=fixture)
    mocked.start()
    unittest.addModuleCleanup(mocked.stop)
