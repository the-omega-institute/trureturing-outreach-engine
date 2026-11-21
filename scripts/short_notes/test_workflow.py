"""Exercise delivery/recovery boundaries without creating remote publications."""
import base64
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.parse import urlsplit, unquote
import zipfile

import workflow as w


def metadata(revision=1):
    return {'id': 'example-conjecture', 'revision': revision,
            'problem_ids': ['example-problem'], 'topic': 'An exact example',
            'title': 'An example theorem', 'creators': [{'name': 'Example, A.'}],
            'abstract': 'We prove the stated example.', 'summary': 'The stated example is proved.',
            'license': 'cc-by-4.0', 'evidence_url': 'https://sample.example.invalid/proof.lean'}


class FakeZenodo:
    base = 'https://zenodo.org/api'

    def __init__(self):
        self.records, self.calls = {}, []
        self.fail_after = None

    def record(self, number, meta):
        number = int(number)
        meta = copy.deepcopy(meta)
        meta['prereserve_doi'] = {'doi': f'10.5281/zenodo.{number}', 'recid': number}
        self.records[number] = {'id': number, 'metadata': copy.deepcopy(meta), 'submitted': False,
                                'files': [], 'links': {'bucket': f'{self.base}/files/{number}',
                                'latest_draft': f'{self.base}/deposit/depositions/{number}'}}
        return self.records[number]

    def request(self, method, path, data=None):
        path = urlsplit(path).path.removeprefix('/api') if path.startswith('https:') else path
        self.calls.append((method, path))
        parts = path.strip('/').split('/')
        if path == '/deposit/depositions' and method == 'POST':
            result = self.record(len(self.records) + 1, data['metadata'])
        elif parts[0] == 'files' and method == 'PUT':
            record = self.records[int(parts[1])]
            name = unquote(parts[2])
            file = {'id': name, 'filename': name, 'checksum': hashlib.md5(data).hexdigest()}
            record['files'] = [f for f in record['files'] if f['filename'] != name] + [file]
            result = file
        else:
            record = self.records[int(parts[2])]
            result = record
            if method == 'PUT':
                reserved = record['metadata'].get('prereserve_doi')
                record['metadata'] = copy.deepcopy(data['metadata'])
                record['metadata']['prereserve_doi'] = reserved
            elif method == 'DELETE':
                name = unquote(parts[-1])
                record['files'] = [f for f in record['files'] if f['id'] != name]
                result = None
            elif method == 'POST' and parts[-1] == 'newversion':
                new = copy.deepcopy(self.record(len(self.records) + 1, record['metadata']))
                new['files'] = copy.deepcopy(record['files'])
                self.records[new['id']] = new
                record['links']['latest_draft'] = new['links']['latest_draft']
            elif method == 'POST' and parts[-1] == 'publish':
                record.update(submitted=True, doi=f"10.5281/zenodo.{record['id']}", conceptdoi='10.5281/zenodo.100')
                record['metadata']['publication_date'] = '2026-10-28'
        if self.fail_after == (method, path):
            self.fail_after = None
            raise TimeoutError('Response lost after the remote operation completed')
        return copy.deepcopy(result)


class FakeGitHub:
    def __init__(self):
        self.releases, self.calls = {}, []
        self.bodies = {}
        self.uploads = []
        self.fail_dispatch = False
        self.fail_upload = None

    def release(self, tag):
        if tag not in self.releases:
            return None
        return {'assets': [{'name': n} for n in self.releases[tag]], 'body': self.bodies.get(tag, '')}

    def list_releases(self):
        return [{'tag_name': tag, **self.release(tag)} for tag in self.releases]

    def create(self, tag, title):
        self.releases[tag] = {}
        self.bodies[tag] = w.INITIAL_NOTES

    def save_state(self, tag, state):
        self.bodies[tag] = w.encode(state).decode()

    def delete_asset(self, tag, name):
        del self.releases[tag][name]

    def upload(self, tag, path, replace=False):
        self.uploads.append((tag, Path(path).name))
        assets = self.releases[tag]
        if Path(path).name in assets and not replace:
            raise ValueError('Asset already exists')
        if self.fail_upload == Path(path).name:
            self.fail_upload = None
            assets.pop(Path(path).name, None)
            raise RuntimeError('Upload failed after clobber deleted the previous asset')
        assets[Path(path).name] = Path(path).read_bytes()

    def download(self, tag, name, directory):
        path = Path(directory) / name
        path.write_bytes(self.releases[tag][name])
        return path

    def run(self, *args):
        self.calls.append(args)
        if 'event_type=research-updates' in args:
            if self.fail_dispatch:
                raise RuntimeError('Dispatch temporarily unavailable')
            return b''
        return json.dumps({'content': base64.b64encode(w.encode(empty_feed())).decode()}).encode()


def empty_feed():
    return {'schema_version': 'research-updates.v1', 'publications': [], 'collaborations': []}


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.zenodo = FakeZenodo()
        self.saved = None

    def package(self, revision=1):
        meta = self.root / 'metadata.json'
        meta.write_bytes(w.encode(metadata(revision)))
        pdf, source = self.root / 'note.pdf', self.root / 'source.zip'
        pdf.write_bytes(b'%PDF-1.4\nselected compiled example')
        with zipfile.ZipFile(source, 'w') as archive:
            archive.writestr('note.tex', r'\documentclass{article}\begin{document}Example\end{document}')
        output = self.root / ('v' + str(revision))
        w.prepare(meta, pdf, source, output)
        return w.read(output / f'note-v{revision}.json'), output

    def save(self, state):
        self.saved = copy.deepcopy(state)

    def test_pasted_token_outer_whitespace_is_trimmed_and_invalid_headers_are_safe(self):
        self.assertEqual(w.Zenodo('  selected-token\n').token, 'selected-token')
        for value in ('', 'private\nheader', 'private\x00header', 'private\u200bheader'):
            with self.assertRaisesRegex(ValueError, 'single ASCII token'):
                w.Zenodo(value)

    def deliver(self, revision=1, recover=None):
        package, folder = self.package(revision)
        return w.publish(package, folder, copy.deepcopy(self.saved), self.save, self.zenodo, recover)

    def test_one_existing_note_reaches_published_preprint(self):
        state = self.deliver()
        self.assertEqual(state['publication']['status'], 'preprint')
        self.assertEqual(state['publication']['url'], 'https://doi.org/10.5281/zenodo.1')
        self.assertEqual(state['versions']['1']['phase'], 'published')
        self.assertEqual(self.zenodo.records[1]['metadata']['publication_type'], 'preprint')

    def test_repeat_delivery_does_not_create_or_republish(self):
        package, folder = self.package()
        state = w.publish(package, folder, None, self.save, self.zenodo)
        calls = list(self.zenodo.calls)
        self.assertEqual(w.publish(package, folder, state, self.save, self.zenodo), state)
        self.assertEqual(self.zenodo.calls, calls)

    def test_second_revision_uses_same_zenodo_concept_and_clears_inherited_files(self):
        self.deliver()
        state = self.deliver(2)
        self.assertIn(('POST', '/deposit/depositions/1/actions/newversion'), self.zenodo.calls)
        self.assertEqual({f['filename'] for f in self.zenodo.records[2]['files']}, {'note-v2.pdf', 'source-v2.zip'})
        self.assertEqual(len(state['versions']), 2)
        self.assertEqual(state['versions']['1']['concept_doi'], state['versions']['2']['concept_doi'])

    def test_changed_bytes_cannot_reuse_a_revision(self):
        package, folder = self.package()
        state = w.publish(package, folder, None, self.save, self.zenodo)
        changed = copy.deepcopy(package)
        changed['metadata']['title'] = 'A changed title'
        with self.assertRaisesRegex(ValueError, 'immutable'):
            w.publish(changed, folder, state, self.save, self.zenodo)

    def test_local_changed_artifact_is_rejected_before_network(self):
        package, folder = self.package()
        (folder / 'note-v1.pdf').write_bytes(b'changed')
        with self.assertRaisesRegex(ValueError, 'artifact changed'):
            w.publish(package, folder, None, self.save, self.zenodo)
        self.assertEqual(self.zenodo.calls, [])

    def test_first_public_revision_can_follow_unpublished_artifact_revisions(self):
        state = self.deliver(2)
        self.assertEqual(state['versions']['2']['record_id'], 1)
        self.assertEqual(self.zenodo.records[1]['metadata']['version'], '2')

    def test_save_failure_before_creation_does_not_create_a_deposit(self):
        package, folder = self.package()
        def failed_save(state):
            raise RuntimeError('Private release unavailable')
        with self.assertRaises(RuntimeError):
            w.publish(package, folder, None, failed_save, self.zenodo)
        self.assertEqual(self.zenodo.calls, [])

    def test_uncertain_initial_creation_never_repeats_post(self):
        package, folder = self.package()
        self.zenodo.fail_after = ('POST', '/deposit/depositions')
        with self.assertRaises(TimeoutError):
            w.publish(package, folder, None, self.save, self.zenodo)
        with self.assertRaisesRegex(RuntimeError, 'Uncertain'):
            w.publish(package, folder, self.saved, self.save, self.zenodo)
        self.assertEqual(self.zenodo.calls.count(('POST', '/deposit/depositions')), 1)
        state = w.publish(package, folder, self.saved, self.save, self.zenodo, recover_record=1)
        self.assertEqual(state['versions']['1']['record_id'], 1)

    def test_uncertain_new_version_can_recover_without_another_version(self):
        self.deliver()
        package, folder = self.package(2)
        self.zenodo.fail_after = ('POST', '/deposit/depositions/1/actions/newversion')
        with self.assertRaises(TimeoutError):
            w.publish(package, folder, self.saved, self.save, self.zenodo)
        with self.assertRaisesRegex(RuntimeError, 'Uncertain'):
            w.publish(package, folder, self.saved, self.save, self.zenodo)
        state = w.publish(package, folder, self.saved, self.save, self.zenodo, recover_record=2)
        self.assertEqual(state['publication']['url'], 'https://doi.org/10.5281/zenodo.2')
        self.assertEqual(self.zenodo.calls.count(('POST', '/deposit/depositions/1/actions/newversion')), 1)

    def test_lost_publish_response_recovers_published_record(self):
        package, folder = self.package()
        self.zenodo.fail_after = ('POST', '/deposit/depositions/1/actions/publish')
        with self.assertRaises(TimeoutError):
            w.publish(package, folder, None, self.save, self.zenodo)
        state = w.publish(package, folder, self.saved, self.save, self.zenodo)
        self.assertEqual(state['versions']['1']['phase'], 'published')
        self.assertEqual(self.zenodo.calls.count(('POST', '/deposit/depositions/1/actions/publish')), 1)

    def test_upload_interruption_reuses_same_draft(self):
        package, folder = self.package()
        self.zenodo.fail_after = ('PUT', '/files/1/note-v1.pdf')
        with self.assertRaises(TimeoutError):
            w.publish(package, folder, None, self.save, self.zenodo)
        w.publish(package, folder, self.saved, self.save, self.zenodo)
        self.assertEqual(len(self.zenodo.records), 1)

    def test_corrected_artifacts_explicitly_reuse_the_unpublished_draft(self):
        package, folder = self.package()
        self.zenodo.fail_after = ('PUT', '/files/1/note-v1.pdf')
        with self.assertRaises(TimeoutError):
            w.publish(package, folder, None, self.save, self.zenodo)
        corrected, corrected_folder = self.package(2)
        with self.assertRaisesRegex(ValueError, 'existing unpublished draft'):
            w.publish(corrected, corrected_folder, self.saved, self.save, self.zenodo)
        result = w.publish(corrected, corrected_folder, self.saved, self.save, self.zenodo, reuse_draft_from=1)
        self.assertEqual(result['versions']['1']['phase'], 'superseded')
        self.assertEqual(result['versions']['2']['record_id'], 1)
        self.assertEqual(self.zenodo.records[1]['metadata']['version'], '2')
        self.assertEqual(len(self.zenodo.records), 1)
        self.assertEqual({f['filename'] for f in self.zenodo.records[1]['files']}, {'note-v2.pdf', 'source-v2.zip'})

    def test_uncertain_first_draft_can_be_recovered_for_a_corrected_artifact_revision(self):
        package, folder = self.package()
        self.zenodo.fail_after = ('POST', '/deposit/depositions')
        with self.assertRaises(TimeoutError):
            w.publish(package, folder, None, self.save, self.zenodo)
        corrected, corrected_folder = self.package(2)
        result = w.publish(corrected, corrected_folder, self.saved, self.save, self.zenodo,
                           recover_record=1, reuse_draft_from=1)
        self.assertEqual(result['versions']['2']['record_id'], 1)
        self.assertEqual(self.zenodo.calls.count(('POST', '/deposit/depositions')), 1)

    def test_reuse_cannot_change_an_already_published_record(self):
        package, folder = self.package()
        self.zenodo.fail_after = ('POST', '/deposit/depositions/1/actions/publish')
        with self.assertRaises(TimeoutError):
            w.publish(package, folder, None, self.save, self.zenodo)
        corrected, corrected_folder = self.package(2)
        before = copy.deepcopy(self.zenodo.records)
        with self.assertRaisesRegex(ValueError, 'already published'):
            w.publish(corrected, corrected_folder, self.saved, self.save, self.zenodo, reuse_draft_from=1)
        self.assertEqual(self.zenodo.records, before)

    def test_reusing_a_later_draft_keeps_the_existing_published_concept(self):
        self.deliver()
        package, folder = self.package(2)
        self.zenodo.fail_after = ('PUT', '/files/2/note-v2.pdf')
        with self.assertRaises(TimeoutError):
            w.publish(package, folder, self.saved, self.save, self.zenodo)
        corrected, corrected_folder = self.package(3)
        result = w.publish(corrected, corrected_folder, self.saved, self.save, self.zenodo, reuse_draft_from=2)
        self.assertEqual(result['versions']['3']['record_id'], 2)
        self.assertEqual(result['versions']['1']['concept_doi'], result['versions']['3']['concept_doi'])
        self.assertEqual(self.zenodo.calls.count(('POST', '/deposit/depositions/1/actions/newversion')), 1)

    def test_definitive_creation_rejection_allows_a_corrected_unpublished_revision(self):
        package, folder = self.package()
        original = self.zenodo.request
        def rejected(method, path, data=None):
            if method == 'POST' and path == '/deposit/depositions':
                raise w.ZenodoHTTPError(method, 422)
            return original(method, path, data)
        self.zenodo.request = rejected
        with self.assertRaises(w.ZenodoHTTPError):
            w.publish(package, folder, None, self.save, self.zenodo)
        self.assertEqual(self.saved['versions']['1']['phase'], 'creation_rejected')
        self.assertEqual(self.zenodo.records, {})
        self.zenodo.request = original
        corrected, corrected_folder = self.package(2)
        result = w.publish(corrected, corrected_folder, self.saved, self.save, self.zenodo)
        self.assertEqual(result['versions']['2']['record_id'], 1)

    def test_wrong_recovery_record_is_rejected(self):
        package, folder = self.package()
        self.zenodo.fail_after = ('POST', '/deposit/depositions')
        with self.assertRaises(TimeoutError):
            w.publish(package, folder, None, self.save, self.zenodo)
        self.zenodo.records[1]['metadata']['keywords'] = ['another-note']
        with self.assertRaisesRegex(ValueError, 'not this note'):
            w.publish(package, folder, self.saved, self.save, self.zenodo, recover_record=1)

    def test_reserved_doi_does_not_qualify_as_publication(self):
        package, folder = self.package()
        real_request = self.zenodo.request
        def not_published(method, path, data=None):
            result = real_request(method, path, data)
            if method == 'POST' and path.endswith('/publish'):
                self.zenodo.records[1]['submitted'] = False
            return result
        self.zenodo.request = not_published
        with self.assertRaisesRegex(ValueError, 'published DOI'):
            w.publish(package, folder, None, self.save, self.zenodo)
        self.assertNotIn('publication', self.saved)

    def test_later_paper_replaces_link_without_deleting_note_history(self):
        state = self.deliver()
        later = {**state['publication'], 'title': 'An extended paper', 'url': 'https://doi.org/10.1000/example',
                 'venue': 'Example Journal', 'status': 'published'}
        result = w.promote(state, later)
        self.assertEqual(result['publication'], later)
        self.assertEqual(result['versions'], state['versions'])
        self.assertEqual(result['preferred'], 'paper')
        self.assertEqual(w.promote(result, later), result)
        self.saved = result
        revised = self.deliver(2)
        self.assertEqual(revised['publication'], later)

    def test_feed_replacement_keeps_one_id_and_preserves_other_publications_and_collaborations(self):
        paper = self.deliver()['publication']
        other = {**paper, 'id': 'other-note', 'url': 'https://doi.org/10.1000/other'}
        collaboration = {'id': 'joint-project', 'problem_ids': ['another-problem'], 'topic': 'Joint topic',
                         'status': 'confirmed_active', 'people': [{'name': 'Example', 'institution': 'Institute'}],
                         'evidence_url': 'https://sample.example.invalid/project'}
        feed = {**empty_feed(), 'publications': [paper, other], 'collaborations': [collaboration]}
        changed = {**paper, 'url': 'https://doi.org/10.1000/later', 'venue': 'Journal', 'status': 'published'}
        result = w.replace_publication(feed, changed)
        self.assertEqual(len(result['publications']), 2)
        self.assertEqual(result['collaborations'], [collaboration])
        self.assertNotIn(paper['url'], json.dumps(result))

    def test_private_fields_and_draft_status_are_rejected(self):
        paper = self.deliver()['publication']
        for changed in ({**paper, 'message_id': 'private-mail'}, {**paper, 'status': 'submitted'}):
            with self.assertRaises(ValueError):
                w.replace_publication(empty_feed(), changed)
        with self.assertRaises(ValueError):
            w.validate_metadata({**metadata(), 'email': 'private@sample.example.invalid'})

    def test_canonical_arxiv_doi_aliases_are_rejected_as_duplicates(self):
        paper = self.deliver()['publication']
        first = {**paper, 'url': 'https://arxiv.org/abs/2609.12345v2'}
        alias = {**paper, 'id': 'other', 'url': 'https://doi.org/10.48550/arXiv.2609.12345'}
        with self.assertRaisesRegex(ValueError, 'Duplicate publication'):
            w.validate_feed({**empty_feed(), 'publications': [first, alias]})

    def test_staging_is_immutable_and_repeatable(self):
        package, folder = self.package()
        gh = FakeGitHub()
        first = w.stage(folder, gh)
        self.assertEqual(w.stage(folder, gh), first)
        gh.releases[first['tag']]['note-v1.pdf'] = b'changed remotely'
        with self.assertRaisesRegex(ValueError, 'immutable'):
            w.stage(folder, gh)

    def test_full_operation_survives_pages_dispatch_failure_without_republishing(self):
        package, folder = self.package()
        gh, pages = FakeGitHub(), FakeGitHub()
        w.stage(folder, gh)
        pages.fail_dispatch = True
        with self.assertRaisesRegex(RuntimeError, 'Dispatch'):
            w.operate('publish', metadata()['id'], 1, gh, self.zenodo, pages=pages)
        persisted = json.loads(gh.bodies[w.tag_for(metadata()['id'])])
        self.assertEqual(persisted['versions']['1']['phase'], 'published')
        pages.fail_dispatch = False
        w.operate('publish', metadata()['id'], 1, gh, self.zenodo, pages=pages)
        self.assertEqual(self.zenodo.calls.count(('POST', '/deposit/depositions')), 1)
        self.assertEqual(self.zenodo.calls.count(('POST', '/deposit/depositions/1/actions/publish')), 1)
        feed = json.loads(pages.releases['research-updates']['research-updates.v1.json'])
        self.assertEqual(feed['publications'], [])

    def test_sandbox_never_reaches_public_feed(self):
        state = self.deliver()
        state['zenodo_api'] = 'https://sandbox.zenodo.org/api'
        with self.assertRaisesRegex(ValueError, 'Sandbox'):
            w.sync_pages(state, FakeGitHub())

    def test_pages_projection_accepts_formal_acceptance_but_not_archive_or_draft(self):
        archive = self.deliver()['publication']
        accepted = {**archive, 'id': 'accepted-paper', 'url': 'https://sample.example.invalid/paper',
                    'status': 'accepted', 'evidence_url': 'https://sample.example.invalid/acceptance'}
        draft = {**archive, 'id': 'draft-paper', 'url': 'https://sample.example.invalid/draft'}
        feed = {**empty_feed(), 'publications': [archive, accepted, draft]}
        result = w.project_pages_feed(feed)
        self.assertEqual(result['publications'], [accepted])
        self.assertEqual(len(feed['publications']), 3)
        self.assertTrue(w.is_pages_publication({**draft, 'url': 'https://arxiv.org/abs/math/0301234'}))
        self.assertFalse(w.is_pages_publication({**draft, 'url': 'https://arxiv.org/abs/not-an-id'}))

    def test_unchanged_pages_retry_redispatches_without_replacing_the_feed(self):
        state = self.deliver()
        state = w.promote(state, {**state['publication'], 'url': 'https://arxiv.org/abs/2611.00992'})
        pages = FakeGitHub()
        w.sync_pages(state, pages)
        before = pages.releases['research-updates']['research-updates.v1.json']
        uploads = list(pages.uploads)
        calls = len(pages.calls)
        w.sync_pages(state, pages)
        self.assertEqual(pages.uploads, uploads)
        self.assertEqual(pages.releases['research-updates']['research-updates.v1.json'], before)
        self.assertEqual(len(pages.calls), calls + 1)

    def test_feed_retry_recovers_release_created_before_asset_upload(self):
        state = self.deliver()
        state = w.promote(state, {**state['publication'], 'url': 'https://arxiv.org/abs/2611.00992'})
        pages = FakeGitHub()
        pages.create('research-updates', 'Incomplete first publication')
        w.sync_pages(state, pages)
        feed = json.loads(pages.releases['research-updates']['research-updates.v1.json'])
        self.assertEqual(feed['publications'], [state['publication']])

    def test_feed_retry_keeps_other_results_after_clobber_loses_the_main_asset(self):
        state = self.deliver()
        state = w.promote(state, {**state['publication'], 'url': 'https://arxiv.org/abs/2611.00992'})
        pages = FakeGitHub()
        pages.create('research-updates', 'Existing publications')
        other = {**state['publication'], 'id': 'other-note', 'url': 'https://arxiv.org/abs/2611.01065'}
        pages.releases['research-updates']['research-updates.v1.json'] = w.encode(
            {**empty_feed(), 'publications': [other]})
        pages.fail_upload = 'research-updates.v1.json'
        with self.assertRaisesRegex(RuntimeError, 'clobber'):
            w.sync_pages(state, pages)
        self.assertNotIn('research-updates.v1.json', pages.releases['research-updates'])
        self.assertIn('research-updates.pending.json', pages.releases['research-updates'])
        w.sync_pages(state, pages)
        result = json.loads(pages.releases['research-updates']['research-updates.v1.json'])
        self.assertEqual({p['id'] for p in result['publications']}, {other['id'], state['id']})
        self.assertNotIn('research-updates.pending.json', pages.releases['research-updates'])

    def test_foreign_release_receipt_does_not_create_a_deposit(self):
        package, folder = self.package()
        gh = FakeGitHub()
        w.stage(folder, gh)
        gh.bodies[w.tag_for(metadata()['id'])] = json.dumps(
            {'schema_version': 'short-note-state.v1', 'id': 'someone-else'})
        with self.assertRaisesRegex(ValueError, 'receipt identity'):
            w.operate('publish', metadata()['id'], 1, gh, self.zenodo)
        self.assertEqual(self.zenodo.calls, [])

    def test_recovery_cannot_switch_zenodo_concepts(self):
        self.deliver()
        package, folder = self.package(2)
        self.zenodo.fail_after = ('POST', '/deposit/depositions/1/actions/newversion')
        with self.assertRaises(TimeoutError):
            w.publish(package, folder, self.saved, self.save, self.zenodo)
        self.zenodo.record(99, self.zenodo.records[2]['metadata'])
        with self.assertRaisesRegex(ValueError, 'existing concept'):
            w.publish(package, folder, self.saved, self.save, self.zenodo, recover_record=99)

    def test_published_creator_mismatch_is_not_exported(self):
        package, folder = self.package()
        self.zenodo.fail_after = ('POST', '/deposit/depositions/1/actions/publish')
        with self.assertRaises(TimeoutError):
            w.publish(package, folder, None, self.save, self.zenodo)
        self.zenodo.records[1]['metadata']['creators'] = [{'name': 'Someone else'}]
        with self.assertRaisesRegex(ValueError, 'metadata differs'):
            w.publish(package, folder, self.saved, self.save, self.zenodo)

    def test_creator_mismatch_is_rejected_before_irreversible_publication(self):
        package, folder = self.package()
        original_request = self.zenodo.request
        def modified_metadata(method, path, data=None):
            result = original_request(method, path, data)
            if method == 'PUT' and path == '/deposit/depositions/1':
                self.zenodo.records[1]['metadata']['creators'] = [{'name': 'Someone else'}]
            return result
        self.zenodo.request = modified_metadata
        with self.assertRaisesRegex(ValueError, 'metadata differs'):
            w.publish(package, folder, None, self.save, self.zenodo)
        self.assertNotIn(('POST', '/deposit/depositions/1/actions/publish'), self.zenodo.calls)

    def test_source_zip_excludes_mail_and_path_traversal(self):
        for bad in ('../note.tex', '/note.tex', 'mail/thread.txt', 'outreach-polished.json'):
            source = self.root / 'bad.zip'
            with zipfile.ZipFile(source, 'w') as archive:
                archive.writestr('note.tex', 'example')
                archive.writestr(bad, 'must not publish')
            with self.assertRaises(ValueError, msg=bad):
                w.validate_source(source)

    def test_token_never_follows_external_bucket_or_redirect(self):
        zenodo = w.Zenodo('secret')
        with self.assertRaisesRegex(ValueError, 'outside'):
            zenodo.request('PUT', 'https://attacker.example/api/file', b'file')
        with self.assertRaisesRegex(ValueError, 'redirect'):
            w.NoRedirect().redirect_request(None, None, 302, '', {}, 'https://attacker.example')

if __name__ == '__main__':
    unittest.main()
