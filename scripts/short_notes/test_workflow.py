"""Exercise delivery/recovery boundaries without creating remote publications."""
import base64
import copy
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
import unicodedata
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.parse import urlsplit, unquote, parse_qs
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
        parsed = urlsplit(path)
        path = parsed.path.removeprefix('/api')
        self.calls.append((method, path + ('?' + parsed.query if parsed.query else '')))
        parts = path.strip('/').split('/')
        if path == '/deposit/depositions' and method == 'POST':
            result = self.record(len(self.records) + 1, data['metadata'])
        elif path == '/deposit/depositions' and method == 'GET':
            query = parse_qs(parsed.query)
            size, page = int(query['size'][0]), int(query['page'][0])
            rows = [row for row in self.records.values()
                    if row['submitted'] == (query['status'][0] == 'published')]
            result = rows[(page - 1) * size:page * size]
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
        self.authored_publications = []

    @property
    def dispatches(self):
        return [call for call in self.calls if 'event_type=research-updates' in call]

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
        if any('/contents/site/assets/research-news.json' in arg for arg in args):
            catalog = {'publications': self.authored_publications}
            return json.dumps({'content': base64.b64encode(w.encode(catalog)).decode()}).encode()
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

    def package(self, revision=1, **fields):
        meta = self.root / 'metadata.json'
        meta.write_bytes(w.encode({**metadata(revision), **fields}))
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

    def fence(self, package):
        state = {'schema_version': 'short-note-state.v1', 'id': package['metadata']['id'],
                 'zenodo_api': self.zenodo.base, 'preferred': 'note', 'versions': {
                     str(package['metadata']['revision']): {
                         'phase': 'creating', 'package_id': hashlib.sha256(w.encode(package)).hexdigest(),
                         'requested_at': '2026-01-01T00:00:00+00:00'}}}
        self.save(state)
        return copy.deepcopy(state)

    def test_discovery_requires_note_keyword_for_drafts_and_publications(self):
        package, folder = self.package()
        for submitted in (False, True):
            with self.subTest(submitted=submitted):
                self.zenodo = FakeZenodo()
                state = self.fence(package)
                unrelated = self.published_candidate(package, folder) if submitted else self.zenodo.record(
                    1, w.zenodo_metadata(package['metadata']))
                unrelated['metadata']['keywords'] = ['trureturing-note:other-example']
                before = copy.deepcopy(unrelated)
                if submitted:
                    self.published_candidate(package, folder, 2)
                else:
                    self.zenodo.record(2, w.zenodo_metadata(package['metadata']))
                result = w.publish(package, folder, state, self.save, self.zenodo, discover=True)
                self.assertEqual(result['versions']['1']['record_id'], 2)
                self.assertEqual(self.zenodo.records[1], before)

    def test_young_empty_discovery_cannot_recreate_even_with_old_empty_witness(self):
        package, folder = self.package()
        state = self.fence(package)
        state['versions']['1'].update(requested_at=w.now(), discovery_empty_at='2026-01-01T00:00:00+00:00')
        with self.assertRaisesRegex(RuntimeError, 'needs_attention'):
            w.publish(package, folder, state, self.save, self.zenodo, discover=True)
        self.assertEqual(self.saved['versions']['1']['phase'], 'needs_attention')
        self.assertEqual(self.saved['versions']['1']['discovery']['reason'], 'too_young')
        self.assertTrue(self.saved['versions']['1']['discovery_pending'])
        self.assertNotIn('creation_retry_attempted_at', self.saved['versions']['1'])
        self.assertTrue(all(method == 'GET' for method, _ in self.zenodo.calls))

    def test_discovery_threshold_rejects_below_floor_and_nonintegers(self):
        package, folder = self.package()
        for value in (0, 1799, True, 1800.5, '1800'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                w.publish(package, folder, self.fence(package), self.save, self.zenodo,
                          discover=True, recovery_seconds=value)
        self.assertEqual(self.zenodo.calls, [])

    def test_server_capped_pages_find_orphan_at_position_31(self):
        package, folder = self.package()
        state = self.fence(package)
        for number in range(1, 32):
            self.zenodo.record(number, w.zenodo_metadata(metadata(2) if number < 31 else package['metadata']))
        original = self.zenodo.request
        def capped(method, path, data=None):
            if method == 'GET' and '?' in path:
                path = path.replace('size=100', 'size=25')
            return original(method, path, data)
        self.zenodo.request = capped
        result = w.publish(package, folder, state, self.save, self.zenodo, discover=True)
        self.assertEqual(result['versions']['1']['record_id'], 31)
        self.assertNotIn(('POST', '/deposit/depositions'), self.zenodo.calls)

    def test_truncated_nonempty_page_must_continue_and_cap_is_incomplete(self):
        package, folder = self.package()
        for capped in (False, True):
            with self.subTest(capped=capped):
                self.zenodo = FakeZenodo()
                state = self.fence(package)
                original = self.zenodo.request
                def truncated(method, path, data=None):
                    if method == 'GET' and '?' in path:
                        self.zenodo.calls.append((method, path))
                        if parse_qs(urlsplit(path).query)['status'] == ['published']:
                            return []
                        page = int(parse_qs(urlsplit(path).query)['page'][0])
                        if page == 1 or capped:
                            return [{'id': page, 'metadata': w.zenodo_metadata(metadata(2)), 'submitted': False}]
                        return []
                    return original(method, path, data)
                self.zenodo.request = truncated
                with patch.object(w, 'MAX_DISCOVERY_PAGES', 3):
                    with self.assertRaisesRegex(RuntimeError, 'needs_attention'):
                        w.publish(package, folder, state, self.save, self.zenodo, discover=True)
                self.assertTrue(all(method == 'GET' for method, _ in self.zenodo.calls))
                self.assertEqual(self.saved['versions']['1']['discovery']['pages'], 3)
                self.assertEqual('discovery_empty_at' in self.saved['versions']['1'], not capped)

    def test_empty_discovery_requires_two_runs_separated_by_threshold(self):
        package, folder = self.package()
        state = self.fence(package)
        for stamp in ('2026-02-01T00:00:00+00:00', '2026-02-01T00:59:59+00:00'):
            with patch.object(w, 'now', return_value=stamp), self.assertRaisesRegex(RuntimeError, 'needs_attention'):
                w.publish(package, folder, state, self.save, self.zenodo, discover=True)
            state = copy.deepcopy(self.saved)
            self.assertEqual(state['versions']['1']['discovery_empty_at'], '2026-02-01T00:00:00+00:00')
            self.assertTrue(all(method == 'GET' for method, _ in self.zenodo.calls))
        with patch.object(w, 'now', return_value='2026-02-01T01:00:00+00:00'):
            result = w.publish(package, folder, state, self.save, self.zenodo, discover=True)
        self.assertEqual(result['versions']['1']['phase'], 'published')
        self.assertEqual(self.zenodo.calls.count(('POST', '/deposit/depositions')), 1)

    def test_delayed_indexing_adopts_on_second_inventory_without_create(self):
        package, folder = self.package()
        state = self.fence(package)
        with self.assertRaisesRegex(RuntimeError, 'needs_attention'):
            w.publish(package, folder, state, self.save, self.zenodo, discover=True)
        self.zenodo.record(1, w.zenodo_metadata(package['metadata']))
        result = w.publish(package, folder, copy.deepcopy(self.saved), self.save, self.zenodo, discover=True)
        self.assertEqual(result['versions']['1']['record_id'], 1)
        self.assertNotIn(('POST', '/deposit/depositions'), self.zenodo.calls)

    def test_lost_newversion_adopts_direct_latest_draft_with_inherited_metadata(self):
        self.deliver()
        package, folder = self.package(2)
        self.zenodo.fail_after = ('POST', '/deposit/depositions/1/actions/newversion')
        with self.assertRaises(TimeoutError):
            w.publish(package, folder, self.saved, self.save, self.zenodo)
        original = self.zenodo.request
        def unindexed(method, path, data=None):
            if method == 'GET' and '?' in path:
                self.zenodo.calls.append((method, path))
                return []
            return original(method, path, data)
        self.zenodo.request = unindexed
        result = w.publish(package, folder, copy.deepcopy(self.saved), self.save, self.zenodo, discover=True)
        self.assertEqual(result['versions']['2']['record_id'], 2)
        self.assertEqual(result['versions']['2']['phase'], 'published')
        self.assertEqual(self.zenodo.calls.count(('POST', '/deposit/depositions/1/actions/newversion')), 1)

    def test_direct_latest_draft_wrong_identity_is_parked_without_writes(self):
        self.deliver()
        package, folder = self.package(2)
        self.zenodo.fail_after = ('POST', '/deposit/depositions/1/actions/newversion')
        with self.assertRaises(TimeoutError):
            w.publish(package, folder, self.saved, self.save, self.zenodo)
        self.zenodo.records[2]['metadata']['keywords'] = ['trureturing-note:other-example']
        before = copy.deepcopy(self.zenodo.records)
        self.zenodo.calls.clear()
        with self.assertRaisesRegex(RuntimeError, 'needs_attention'):
            w.publish(package, folder, copy.deepcopy(self.saved), self.save, self.zenodo, discover=True)
        self.assertEqual(self.saved['versions']['2']['phase'], 'needs_attention')
        self.assertEqual(self.saved['versions']['2']['discovery']['reason'], 'identity_mismatch')
        self.assertFalse(self.saved['versions']['2'].get('discovery_pending'))
        self.assertEqual(self.zenodo.records, before)
        self.assertTrue(all(method == 'GET' for method, _ in self.zenodo.calls))

    def test_prior_read_failure_is_pending_but_wrong_prior_is_terminal(self):
        previous = copy.deepcopy(self.deliver()['versions']['1'])
        package, folder = self.package(2)
        original = self.zenodo.request
        for failure in ('read_error', 'malformed', 'identity_mismatch', 'prior_lookup'):
            with self.subTest(failure=failure):
                state = self.fence(package)
                state['versions']['1'] = copy.deepcopy(previous)
                self.zenodo.calls.clear()
                def prior_read(method, path, data=None):
                    record = original(method, path, data)
                    if path == '/deposit/depositions/1':
                        if failure == 'read_error':
                            raise TimeoutError('Synthetic private prior error')
                        if failure == 'malformed':
                            record['links'].pop('latest_draft')
                        elif failure == 'identity_mismatch':
                            record['metadata']['keywords'] = ['trureturing-note:other-example']
                    return record
                self.zenodo.request = prior_read
                with self.assertRaises(RuntimeError):
                    w.publish(package, folder, state, self.save, self.zenodo, discover=True)
                entry = self.saved['versions']['2']
                self.assertEqual(entry['discovery']['reason'], failure)
                self.assertEqual(bool(entry.get('discovery_pending')), failure in ('read_error', 'malformed'))
                self.assertNotIn('record_id', entry)
                self.assertTrue(all(method == 'GET' for method, _ in self.zenodo.calls))

    def test_opposite_status_rows_are_skipped_before_empty_witness(self):
        package, folder = self.package()
        state = self.fence(package)
        row = {'id': 1, 'metadata': w.zenodo_metadata(metadata(2)), 'submitted': True}
        original = self.zenodo.request
        def disagrees(method, path, data=None):
            if method == 'GET' and '?' in path:
                self.zenodo.calls.append((method, path))
                query = parse_qs(urlsplit(path).query)
                if query['page'] == ['1']:
                    return [{**row, 'submitted': query['status'] == ['draft']}]
                return []
            return original(method, path, data)
        self.zenodo.request = disagrees
        with self.assertRaisesRegex(RuntimeError, 'needs_attention'):
            w.publish(package, folder, state, self.save, self.zenodo, discover=True)
        entry = self.saved['versions']['1']
        self.assertEqual(entry['phase'], 'needs_attention')
        self.assertEqual(entry['discovery'], {'draft_candidates': 0, 'published_candidates': 0, 'pages': 4,
                                              'skipped_submitted': 2, 'reason': 'empty'})
        self.assertTrue(entry['discovery_pending'])
        self.assertIn('discovery_empty_at', entry)
        self.assertTrue(all(method == 'GET' for method, _ in self.zenodo.calls))

    def test_lost_draft_after_submitted_row_is_adopted_without_create(self):
        package, folder = self.package()
        state = self.fence(package)
        row = self.zenodo.record(1, w.zenodo_metadata(package['metadata']))
        original = self.zenodo.request
        def mixed(method, path, data=None):
            if method == 'GET' and '?' in path:
                self.zenodo.calls.append((method, path))
                query = parse_qs(urlsplit(path).query)
                self.assertEqual(query['status'], ['draft'])
                return ([{**row, 'id': 2, 'submitted': True}, row]
                        if query['page'] == ['1'] else [])
            return original(method, path, data)
        self.zenodo.request = mixed
        result = w.publish(package, folder, state, self.save, self.zenodo, discover=True)
        entry = result['versions']['1']
        self.assertEqual((entry['record_id'], entry['phase']), (1, 'published'))
        self.assertEqual(entry['discovery']['skipped_submitted'], 1)
        self.assertNotIn(('POST', '/deposit/depositions'), self.zenodo.calls)

    def test_inventory_failures_record_partial_counts_and_retry_without_empty_witness(self):
        package, folder = self.package()
        for failure in ('page_cap', 'duplicate', 'read_error', 'malformed'):
            with self.subTest(failure=failure):
                self.zenodo = FakeZenodo()
                state = self.fence(package)
                state['versions']['1']['discovery_empty_at'] = '2026-01-01T01:00:00+00:00'
                row = {'id': 1, 'metadata': w.zenodo_metadata(package['metadata']), 'submitted': False}
                def broken(method, path, data=None):
                    self.assertEqual(method, 'GET')
                    self.zenodo.calls.append((method, path))
                    page = int(parse_qs(urlsplit(path).query)['page'][0])
                    if page == 1:
                        return [row]
                    if failure == 'read_error':
                        raise TimeoutError('Synthetic private detail')
                    if failure == 'malformed':
                        return [{**row, 'submitted': None}]
                    return ([{**row, 'id': page, 'metadata': w.zenodo_metadata(metadata(2))}]
                            if failure == 'page_cap' else [row])
                self.zenodo.request = broken
                with patch.object(w, 'MAX_DISCOVERY_PAGES', 2), self.assertRaises(RuntimeError) as raised:
                    w.publish(package, folder, state, self.save, self.zenodo, discover=True)
                entry = self.saved['versions']['1']
                self.assertTrue(entry['discovery_pending'])
                self.assertEqual(entry['discovery']['reason'], failure)
                self.assertEqual(entry['discovery']['pages'], 2)
                self.assertEqual(entry['discovery']['draft_candidates'], 1)
                self.assertEqual(entry['discovery_attempts'], 1)
                self.assertIn('discovery_attempted_at', entry)
                self.assertNotIn('discovery_empty_at', entry)
                self.assertNotIn('creation_retry_attempted_at', entry)
                self.assertNotIn('reason', str(raised.exception))
                self.assertNotIn('Synthetic private detail', str(raised.exception))

    def test_duplicate_skipped_rows_still_fence_creation(self):
        package, folder = self.package()
        state = self.fence(package)
        row = {'id': 1, 'metadata': {}, 'submitted': True}
        def drifting(method, path, data=None):
            self.assertEqual(method, 'GET')
            return [row]
        self.zenodo.request = drifting
        with self.assertRaises(RuntimeError):
            w.publish(package, folder, state, self.save, self.zenodo, discover=True)
        entry = self.saved['versions']['1']
        self.assertEqual(entry['discovery']['reason'], 'duplicate')
        self.assertTrue(entry['discovery_pending'])
        self.assertNotIn('creation_retry_attempted_at', entry)

    def test_ambiguity_is_terminal_even_when_inventory_is_incomplete(self):
        package, folder = self.package()
        state = self.fence(package)
        for number in (1, 2):
            self.zenodo.record(number, w.zenodo_metadata(package['metadata']))
        with patch.object(w, 'MAX_DISCOVERY_PAGES', 1), self.assertRaises(RuntimeError):
            w.publish(package, folder, state, self.save, self.zenodo, discover=True)
        entry = self.saved['versions']['1']
        self.assertEqual(entry['discovery']['reason'], 'ambiguous')
        self.assertFalse(entry.get('discovery_pending'))
        self.assertTrue(all(method == 'GET' for method, _ in self.zenodo.calls))

    def test_non_os_read_errors_are_pending_and_interruptions_propagate(self):
        from http.client import IncompleteRead
        package, folder = self.package()
        for error in (IncompleteRead(b''), KeyboardInterrupt()):
            with self.subTest(error=type(error).__name__):
                state = self.fence(package)
                with patch.object(self.zenodo, 'request', side_effect=error):
                    with self.assertRaises(KeyboardInterrupt if isinstance(error, KeyboardInterrupt) else RuntimeError):
                        w.publish(package, folder, state, self.save, self.zenodo, discover=True)
                entry = self.saved['versions']['1']
                self.assertEqual(entry['discovery_attempts'], 1)
                if not isinstance(error, KeyboardInterrupt):
                    self.assertEqual(entry['discovery']['reason'], 'read_error')
                    self.assertTrue(entry['discovery_pending'])

    def test_candidate_readback_disagreement_parks_without_writes(self):
        package, folder = self.package()
        for change in ('id', 'submitted', 'metadata', 'http'):
            with self.subTest(change=change):
                self.zenodo = FakeZenodo()
                state = self.fence(package)
                self.zenodo.record(1, w.zenodo_metadata(package['metadata']))
                original = self.zenodo.request
                def changed(method, path, data=None):
                    row = original(method, path, data)
                    if method == 'GET' and path == '/deposit/depositions/1':
                        if change == 'http':
                            raise w.ZenodoHTTPError('GET', 503)
                        row[change] = {'id': 2, 'submitted': None, 'metadata': None}[change]
                    return row
                self.zenodo.request = changed
                with self.assertRaisesRegex(RuntimeError, 'needs_attention'):
                    w.publish(package, folder, state, self.save, self.zenodo, discover=True)
                self.assertEqual(self.saved['versions']['1']['phase'], 'needs_attention')
                entry = self.saved['versions']['1']
                self.assertEqual(entry['discovery']['reason'], {
                    'id': 'identity_mismatch', 'submitted': 'malformed',
                    'metadata': 'malformed', 'http': 'read_error'}[change])
                self.assertEqual(bool(entry.get('discovery_pending')), change != 'id')
                self.assertNotIn('record_id', self.saved['versions']['1'])
                self.assertTrue(all(method == 'GET' for method, _ in self.zenodo.calls))

    def test_incomplete_inventory_discards_earlier_empty_witness(self):
        package, folder = self.package()
        state = self.fence(package)
        state['versions']['1'].update(discovery_empty_at='2026-01-01T01:00:00+00:00', discovery_pending=True)
        self.zenodo.record(1, w.zenodo_metadata(metadata(2)))
        with patch.object(w, 'MAX_DISCOVERY_PAGES', 1), self.assertRaisesRegex(RuntimeError, 'needs_attention'):
            w.publish(package, folder, state, self.save, self.zenodo, discover=True)
        self.assertNotIn('discovery_empty_at', self.saved['versions']['1'])
        self.assertTrue(self.saved['versions']['1']['discovery_pending'])
        self.assertEqual(self.saved['versions']['1']['discovery']['reason'], 'page_cap')
        self.assertTrue(all(method == 'GET' for method, _ in self.zenodo.calls))

    def test_discovery_adopts_one_matching_draft_before_upload_and_publishes(self):
        package, folder = self.package()
        state = self.fence(package)
        self.zenodo.record(1, w.zenodo_metadata(package['metadata']))
        original = self.zenodo.request
        def checked(method, path, data=None):
            if method != 'GET':
                self.assertEqual(self.saved['versions']['1']['record_id'], 1)
            return original(method, path, data)
        self.zenodo.request = checked
        result = w.publish(package, folder, state, self.save, self.zenodo, discover=True)
        self.assertEqual(result['versions']['1']['phase'], 'published')
        self.assertEqual(result['versions']['1']['record_id'], 1)
        self.assertNotIn(('POST', '/deposit/depositions'), self.zenodo.calls)

    def test_discovery_matches_normalized_title_and_ordered_creator_names(self):
        package, folder = self.package(title='An “example” – café’s theorem',
                                       creators=[{'name': 'Émilie O’Neil'}])
        state = self.fence(package)
        meta = w.zenodo_metadata(package['metadata'])
        meta['title'] = unicodedata.normalize('NFD', 'An "example" - café\'s theorem')
        meta['creators'][0]['name'] = unicodedata.normalize('NFD', "O'Neil, Émilie")
        self.zenodo.record(1, meta)
        result = w.publish(package, folder, state, self.save, self.zenodo, discover=True)
        self.assertEqual(result['versions']['1']['record_id'], 1)
        self.assertEqual(result['versions']['1']['phase'], 'published')
        self.assertNotIn(('POST', '/deposit/depositions'), self.zenodo.calls)

    def test_ambiguous_discovery_records_counts_and_never_writes_zenodo(self):
        package, folder = self.package()
        state = self.fence(package)
        for number in (1, 2):
            self.zenodo.record(number, w.zenodo_metadata(package['metadata']))
        with self.assertRaisesRegex(RuntimeError, 'needs_attention: draft_candidates=2'):
            w.publish(package, folder, state, self.save, self.zenodo, discover=True)
        entry = self.saved['versions']['1']
        self.assertEqual(entry['phase'], 'needs_attention')
        self.assertEqual(entry['discovery'], {'draft_candidates': 2, 'published_candidates': 0, 'pages': 2,
                                              'skipped_submitted': 0, 'reason': 'ambiguous'})
        self.assertFalse(entry.get('discovery_pending'))
        self.assertNotIn('record_id', entry)
        self.assertTrue(all(method == 'GET' for method, _ in self.zenodo.calls))

    def test_empty_discovery_creates_once_and_recovers_crash_before_identity_save(self):
        package, folder = self.package()
        state = self.fence(package)
        state['versions']['1']['discovery_empty_at'] = '2026-01-01T01:00:00+00:00'
        class Crash(BaseException):
            pass
        def crash_after_create(value):
            if value['versions']['1'].get('record_id'):
                raise Crash()
            self.save(value)
        with self.assertRaises(Crash):
            w.publish(package, folder, state, crash_after_create, self.zenodo, discover=True)
        self.assertIn('creation_retry_attempted_at', self.saved['versions']['1'])
        self.assertNotIn('record_id', self.saved['versions']['1'])
        result = w.publish(package, folder, copy.deepcopy(self.saved), self.save, self.zenodo, discover=True)
        self.assertEqual(result['versions']['1']['phase'], 'published')
        self.assertEqual(self.zenodo.calls.count(('POST', '/deposit/depositions')), 1)

    def test_retry_marker_prevents_second_create_when_response_and_inventory_are_empty(self):
        package, folder = self.package()
        state = self.fence(package)
        state['versions']['1']['discovery_empty_at'] = '2026-01-01T01:00:00+00:00'
        original = self.zenodo.request
        def lost_create(method, path, data=None):
            if method == 'POST' and path == '/deposit/depositions':
                self.assertIn('creation_retry_attempted_at', self.saved['versions']['1'])
                self.zenodo.calls.append((method, path))
                raise TimeoutError('Synthetic uncertain response')
            return original(method, path, data)
        self.zenodo.request = lost_create
        with self.assertRaises(TimeoutError):
            w.publish(package, folder, state, self.save, self.zenodo, discover=True)
        with self.assertRaisesRegex(RuntimeError, 'needs_attention'):
            w.publish(package, folder, copy.deepcopy(self.saved), self.save, self.zenodo, discover=True)
        self.assertEqual(self.zenodo.calls.count(('POST', '/deposit/depositions')), 1)
        entry = self.saved['versions']['1']
        self.assertEqual(entry['discovery']['reason'], 'retry_consumed')
        self.assertTrue(entry['discovery_pending'])
        # A later visible draft is still adoptable after the retry was consumed.
        self.zenodo.record(1, w.zenodo_metadata(package['metadata']))
        result = w.publish(package, folder, copy.deepcopy(self.saved), self.save, self.zenodo, discover=True)
        self.assertEqual(result['versions']['1']['phase'], 'published')
        self.assertEqual(self.zenodo.calls.count(('POST', '/deposit/depositions')), 1)

    def test_recovery_marker_must_save_before_retry_create(self):
        package, folder = self.package()
        state = self.fence(package)
        state['versions']['1']['discovery_empty_at'] = '2026-01-01T01:00:00+00:00'
        def failed_save(value):
            if value['versions']['1'].get('creation_retry_attempted_at'):
                raise RuntimeError('Synthetic receipt failure')
            self.save(value)
        with self.assertRaisesRegex(RuntimeError, 'receipt failure'):
            w.publish(package, folder, state, failed_save, self.zenodo, discover=True)
        self.assertTrue(all(method == 'GET' for method, _ in self.zenodo.calls))

    def test_rejected_recovery_post_retains_reason_and_never_recreates(self):
        package, folder = self.package()
        state = self.fence(package)
        state['versions']['1']['discovery_empty_at'] = '2026-01-01T01:00:00+00:00'
        original = self.zenodo.request
        def rejected(method, path, data=None):
            if method == 'POST':
                self.zenodo.calls.append((method, path))
                raise w.ZenodoHTTPError('POST', 403)
            return original(method, path, data)
        self.zenodo.request = rejected
        with self.assertRaises(w.ZenodoHTTPError):
            w.publish(package, folder, state, self.save, self.zenodo, discover=True)
        entry = self.saved['versions']['1']
        marker = entry['creation_retry_attempted_at']
        self.assertEqual(entry['discovery']['reason'], 'retry_consumed')
        self.assertTrue(entry['discovery_pending'])
        with self.assertRaises(RuntimeError):
            w.publish(package, folder, copy.deepcopy(self.saved), self.save, self.zenodo, discover=True)
        self.assertEqual(self.saved['versions']['1']['creation_retry_attempted_at'], marker)
        self.assertEqual(self.zenodo.calls.count(('POST', '/deposit/depositions')), 1)

    def published_candidate(self, package, folder, number=1):
        record = self.zenodo.record(number, w.zenodo_metadata(package['metadata']))
        record.update(submitted=True, doi=f'10.5281/zenodo.{number}', conceptdoi='10.5281/zenodo.100')
        record['metadata']['publication_date'] = '2026-01-01'
        record['files'] = [{'filename': name, 'checksum': 'md5:' + hashlib.md5((folder / name).read_bytes()).hexdigest()}
                           for name in package['files']]
        return record

    def test_discovery_records_matching_verified_publication_without_writes(self):
        package, folder = self.package()
        state = self.fence(package)
        self.published_candidate(package, folder)
        result = w.publish(package, folder, state, self.save, self.zenodo, discover=True)
        self.assertEqual(result['versions']['1']['phase'], 'published')
        self.assertEqual(result['versions']['1']['doi'], '10.5281/zenodo.1')
        self.assertTrue(all(method == 'GET' for method, _ in self.zenodo.calls))

    def test_published_candidate_must_pass_all_metadata_and_checksums(self):
        package, folder = self.package(creators=[{'name': 'Example, A.', 'affiliation': 'Sample University'}])
        for mismatch in ('checksum', 'affiliation', 'license'):
            with self.subTest(mismatch=mismatch):
                self.zenodo = FakeZenodo()
                state = self.fence(package)
                record = self.published_candidate(package, folder)
                if mismatch == 'checksum':
                    record['files'][0]['checksum'] = 'md5:bad'
                elif mismatch == 'affiliation':
                    record['metadata']['creators'][0]['affiliation'] = 'Other University'
                else:
                    record['metadata']['license'] = 'other'
                with self.assertRaisesRegex(RuntimeError, 'needs_attention'):
                    w.publish(package, folder, state, self.save, self.zenodo, discover=True)
                self.assertEqual(self.saved['versions']['1']['discovery']['reason'], 'verification')
                self.assertFalse(self.saved['versions']['1'].get('discovery_pending'))
                self.assertNotIn('record_id', self.saved['versions']['1'])
                self.assertTrue(all(method == 'GET' for method, _ in self.zenodo.calls))

    def test_published_discovery_retains_the_existing_concept(self):
        prior_state = copy.deepcopy(self.deliver())
        package, folder = self.package(2)
        for same_concept in (True, False):
            with self.subTest(same_concept=same_concept):
                state = self.fence(package)
                state['versions']['1'] = copy.deepcopy(prior_state['versions']['1'])
                record = self.published_candidate(package, folder, 2)
                if not same_concept:
                    record['conceptdoi'] = '10.5281/zenodo.200'
                self.zenodo.calls.clear()
                if same_concept:
                    result = w.publish(package, folder, state, self.save, self.zenodo, discover=True)
                    self.assertEqual(result['versions']['2']['phase'], 'published')
                else:
                    with self.assertRaisesRegex(RuntimeError, 'needs_attention'):
                        w.publish(package, folder, state, self.save, self.zenodo, discover=True)
                    self.assertNotIn('record_id', self.saved['versions']['2'])
                self.assertTrue(all(method == 'GET' for method, _ in self.zenodo.calls))

    def test_multiple_published_candidates_never_allow_create_or_adoption(self):
        package, folder = self.package()
        state = self.fence(package)
        for number in (1, 2):
            self.published_candidate(package, folder, number)
        with self.assertRaisesRegex(RuntimeError, 'published_candidates=2'):
            w.publish(package, folder, state, self.save, self.zenodo, discover=True)
        self.assertNotIn('record_id', self.saved['versions']['1'])
        self.assertTrue(all(method == 'GET' for method, _ in self.zenodo.calls))

    def test_discovery_ignores_a_draft_for_another_revision(self):
        package, folder = self.package()
        state = self.fence(package)
        state['versions']['1']['discovery_empty_at'] = '2026-01-01T01:00:00+00:00'
        self.zenodo.record(1, w.zenodo_metadata(metadata(2)))
        result = w.publish(package, folder, state, self.save, self.zenodo, discover=True)
        self.assertEqual(result['versions']['1']['record_id'], 2)
        self.assertFalse(self.zenodo.records[1]['submitted'])
        self.assertEqual(self.zenodo.records[1]['metadata']['version'], '2')
        self.assertEqual(self.zenodo.calls.count(('POST', '/deposit/depositions')), 1)

    def test_discovery_follows_numbered_pages_before_adopting(self):
        package, folder = self.package()
        state = self.fence(package)
        self.zenodo.record(1, w.zenodo_metadata(metadata(2)))
        self.zenodo.record(2, w.zenodo_metadata(package['metadata']))
        with patch.object(w, 'DEPOSITION_PAGE_SIZE', 1):
            result = w.publish(package, folder, state, self.save, self.zenodo, discover=True)
        self.assertEqual(result['versions']['1']['record_id'], 2)
        listings = [parse_qs(urlsplit(path).query) for method, path in self.zenodo.calls if '?' in path]
        self.assertEqual([row['page'] for row in listings], [['1'], ['2'], ['3']])
        self.assertTrue(all(row['status'] == ['draft'] and row['all_versions'] == ['true'] for row in listings))

    def test_discovery_page_cap_is_shared_with_published_inventory(self):
        package, folder = self.package()
        state = self.fence(package)
        def full_inventory(method, path, data=None):
            self.assertEqual(method, 'GET')
            self.zenodo.calls.append((method, path))
            query = parse_qs(urlsplit(path).query)
            if query['status'] == ['draft']:
                return []
            page, size = int(query['page'][0]), int(query['size'][0])
            self.assertLessEqual(size, 100)
            return [{'id': page * size + i, 'metadata': w.zenodo_metadata(metadata(2)), 'submitted': True}
                    for i in range(size)]
        self.zenodo.request = full_inventory
        with self.assertRaisesRegex(RuntimeError, 'needs_attention'):
            w.publish(package, folder, state, self.save, self.zenodo, discover=True)
        self.assertEqual(len(self.zenodo.calls), w.MAX_DISCOVERY_PAGES)
        self.assertEqual(self.saved['versions']['1']['discovery']['pages'], w.MAX_DISCOVERY_PAGES)
        self.assertNotIn('creation_retry_attempted_at', self.saved['versions']['1'])

    def test_one_draft_at_the_page_cap_cannot_establish_uniqueness(self):
        package, folder = self.package()
        state = self.fence(package)
        self.zenodo.record(1, w.zenodo_metadata(package['metadata']))
        with patch.object(w, 'DEPOSITION_PAGE_SIZE', 1), patch.object(w, 'MAX_DISCOVERY_PAGES', 1):
            with self.assertRaisesRegex(RuntimeError, 'draft_candidates=1'):
                w.publish(package, folder, state, self.save, self.zenodo, discover=True)
        self.assertEqual(len(self.zenodo.calls), 1)
        self.assertNotIn('record_id', self.saved['versions']['1'])
        self.assertTrue(all(method == 'GET' for method, _ in self.zenodo.calls))

    def test_failed_or_unstable_inventory_never_authorizes_creation(self):
        package, folder = self.package()
        for failure in ('http', 'malformed', 'duplicate'):
            with self.subTest(failure=failure):
                self.zenodo = FakeZenodo()
                state = self.fence(package)
                def inventory(method, path, data=None):
                    self.assertEqual(method, 'GET')
                    self.zenodo.calls.append((method, path))
                    if failure == 'http':
                        raise w.ZenodoHTTPError('GET', 503)
                    if failure == 'malformed':
                        return {'unexpected': []}
                    return [{'id': 1, 'metadata': w.zenodo_metadata(package['metadata']), 'submitted': False}] * 100
                self.zenodo.request = inventory
                with self.assertRaises((ValueError, RuntimeError)):
                    w.publish(package, folder, state, self.save, self.zenodo, discover=True)
                self.assertNotIn('creation_retry_attempted_at', self.saved['versions']['1'])

    def test_uncertain_newversion_with_inherited_metadata_does_not_create_again(self):
        self.deliver()
        package, folder = self.package(2)
        self.zenodo.fail_after = ('POST', '/deposit/depositions/1/actions/newversion')
        with self.assertRaises(TimeoutError):
            w.publish(package, folder, self.saved, self.save, self.zenodo)
        result = w.publish(package, folder, copy.deepcopy(self.saved), self.save, self.zenodo, discover=True)
        self.assertEqual(result['versions']['2']['record_id'], 2)
        self.assertEqual(result['versions']['2']['phase'], 'published')
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

    def test_metadata_normalizes_only_title_and_creator_punctuation_and_nfc(self):
        note = {**metadata(), 'title': 'Döring’s “example” – café',
                'creators': [{'name': 'O’Neil, Émilie', 'affiliation': 'Université – A', 'orcid': '0000-0001'}]}
        original = copy.deepcopy(note)
        expected = w.zenodo_metadata(note)
        self.assertEqual(expected['title'], note['title'])
        self.assertEqual(expected['creators'], note['creators'])
        for title, name in (("Döring's \"example\" - café", "O'Neil, Émilie"),
                            (unicodedata.normalize('NFD', note['title']),
                             unicodedata.normalize('NFD', note['creators'][0]['name']))):
            with self.subTest(title=title):
                actual = copy.deepcopy(expected)
                actual['title'], actual['creators'][0]['name'] = title, name
                w.check_remote_metadata({'metadata': actual}, note)
        self.assertEqual(note, original)

    def test_null_remote_title_is_a_metadata_mismatch(self):
        note = metadata()
        actual = {**w.zenodo_metadata(note), 'title': None}
        with self.assertRaisesRegex(ValueError, 'metadata differs'):
            w.check_remote_metadata({'metadata': actual}, note)

    def test_metadata_real_differences_remain_errors(self):
        note = {**metadata(), 'title': 'An author’s café – theorem',
                'creators': [{'name': 'O’Neil, Émilie', 'affiliation': 'Université – A', 'orcid': '0000-0001'}]}
        for key, value in (('title', "An author's cafe - theorem"), ('version', '2'),
                           ('license', 'cc0-1.0'), ('upload_type', 'dataset'),
                           ('publication_type', 'article'), ('access_right', 'closed')):
            with self.subTest(key=key):
                actual = {**w.zenodo_metadata(note), key: value}
                with self.assertRaisesRegex(ValueError, 'metadata differs'):
                    w.check_remote_metadata({'metadata': actual}, note)
        for key, value in (('name', "O'Neill, Émilie"), ('affiliation', 'Université - A'), ('orcid', '0000-0002')):
            with self.subTest(creator=key):
                actual = w.zenodo_metadata(note)
                actual['creators'][0][key] = value
                with self.assertRaisesRegex(ValueError, 'metadata differs'):
                    w.check_remote_metadata({'metadata': actual}, note)

    def test_creation_and_update_preserve_metadata_and_accept_remote_nfd(self):
        package, folder = self.package(title=unicodedata.normalize('NFD', 'An author’s café – “theorem”'),
                                       creators=[{'name': unicodedata.normalize('NFD', 'O’Neil, Émilie')},
                                                 {'name': 'Anne–Marie Example'}])
        sent = []
        original = self.zenodo.request
        def transformed(method, path, data=None):
            if isinstance(data, dict) and 'metadata' in data:
                sent.append(copy.deepcopy(data['metadata']))
            result = original(method, path, data)
            if method == 'GET':
                result['metadata']['title'] = unicodedata.normalize('NFD', result['metadata']['title'])
                result['metadata']['creators'][0]['name'] = unicodedata.normalize('NFD', result['metadata']['creators'][0]['name'])
            return result
        self.zenodo.request = transformed
        state = w.publish(package, folder, None, self.save, self.zenodo)
        self.assertEqual(state['versions']['1']['phase'], 'published')
        self.assertEqual(len(sent), 2)
        self.assertEqual(sent[0], sent[1])
        self.assertEqual(sent[0]['title'], package['metadata']['title'])
        self.assertEqual(sent[0]['creators'], [{'name': package['metadata']['creators'][0]['name']},
                                              {'name': 'Example, Anne–Marie'}])
        self.assertEqual(state['publication']['title'], package['metadata']['title'])

    def test_publication_preserves_en_dashes_when_zenodo_rewrites_only_apostrophe(self):
        class ApostropheOnlyZenodo(FakeZenodo):
            def request(self, method, path, data=None):
                if isinstance(data, dict) and 'metadata' in data:
                    data = copy.deepcopy(data)
                    data['metadata']['title'] = data['metadata']['title'].replace('’', "'")
                    for creator in data['metadata']['creators']:
                        creator['name'] = creator['name'].replace('’', "'")
                return super().request(method, path, data)

        self.zenodo = ApostropheOnlyZenodo()
        title = 'Example–O’Neil–Smith-Jones’s café theorem'
        creators = [{'name': 'O’Neil, Émilie'}, {'name': 'Smith-Jones, Anne'}]
        for revision in (1, 2):
            with self.subTest(revision=revision):
                package, folder = self.package(revision, title=title, creators=creators)
                state = w.publish(package, folder, self.saved, self.save, self.zenodo)
                self.assertEqual(state['versions'][str(revision)]['phase'], 'published')
                stored = self.zenodo.records[revision]['metadata']
                self.assertEqual(stored['title'], title.replace('’', "'"))
                self.assertEqual(stored['title'].count('–'), 2)
                self.assertEqual(stored['creators'], [{'name': "O'Neil, Émilie"}, creators[1]])
                self.assertEqual(state['publication']['title'], title)
                self.assertEqual(package['metadata']['title'], title)

    def test_recovery_cannot_override_a_retained_draft_id(self):
        package, folder = self.package()
        self.zenodo.fail_after = ('PUT', '/deposit/depositions/1')
        with self.assertRaises(TimeoutError):
            w.publish(package, folder, None, self.save, self.zenodo)
        before = list(self.zenodo.calls)
        with self.assertRaisesRegex(ValueError, 'retain the recorded'):
            w.publish(package, folder, self.saved, self.save, self.zenodo, recover_record=99)
        self.assertEqual(before, self.zenodo.calls)

    def test_recovery_without_a_retained_intent_never_creates_a_deposit(self):
        package, folder = self.package()
        with self.assertRaisesRegex(ValueError, 'retained creation intent'):
            w.publish(package, folder, None, self.save, self.zenodo, recover_record=42)
        self.assertEqual(self.zenodo.calls, [])

    def test_reuse_cannot_override_the_earlier_retained_draft_id(self):
        package, folder = self.package()
        self.zenodo.fail_after = ('PUT', '/deposit/depositions/1')
        with self.assertRaises(TimeoutError):
            w.publish(package, folder, None, self.save, self.zenodo)
        corrected, corrected_folder = self.package(2)
        before = list(self.zenodo.calls)
        with self.assertRaisesRegex(ValueError, 'retain the recorded'):
            w.publish(corrected, corrected_folder, self.saved, self.save, self.zenodo,
                      recover_record=99, reuse_draft_from=1)
        self.assertEqual(before, self.zenodo.calls)

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

    def test_transient_zenodo_reads_retry_with_the_original_request(self):
        error=HTTPError('https://zenodo.org/api/deposit/depositions',502,'Bad Gateway',{},io.BytesIO())
        with patch.object(w.recording,'open',side_effect=[error,io.BytesIO(b'[]')]) as opened, \
                patch.object(w.time,'sleep') as pause:
            self.assertEqual(w.Zenodo('synthetic-secret').request('GET','/deposit/depositions'),[])
            self.assertEqual(opened.call_count,2)
            self.assertIs(opened.call_args_list[0].args[2],opened.call_args_list[1].args[2])
            pause.assert_called_once_with(1)

    def test_uncertain_writes_and_permanent_read_refusals_are_never_retried(self):
        for method,code in [('POST',502),('PUT',503),('DELETE',504),('GET',404)]:
            with self.subTest(method=method):
                error=HTTPError('https://zenodo.org/api/deposit/depositions',code,'Refused',{},io.BytesIO())
                with patch.object(w.recording,'open',side_effect=error) as opened,patch.object(w.time,'sleep') as pause:
                    with self.assertRaises(w.ZenodoHTTPError) as failure:
                        w.Zenodo('synthetic-secret').request(method,'/deposit/depositions')
                    self.assertEqual(failure.exception.code,code)
                    opened.assert_called_once()
                    pause.assert_not_called()

if __name__ == '__main__':
    unittest.main()
