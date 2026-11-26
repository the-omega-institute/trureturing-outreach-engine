#!/usr/bin/env python3
"""Callable note intake, durable requests and complete public research projection."""
from __future__ import annotations

import argparse
import copy
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile
import time
from urllib.request import Request, urlopen
import zipfile

ROOT = Path(__file__).resolve().parents[2]
# The service is invoked as ``python3 scripts/short_notes/service.py`` in CI.
# In that mode Python puts only the script directory on sys.path, so the
# repository-level ``managed`` package is otherwise not importable.
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ``managed.hosted`` imports this module as ``scripts.short_notes.service``;
# CI and the tests run it as a script from its own directory.
try:
    from . import workflow as w
except ImportError:
    import workflow as w
from managed.location import DATA_ROOT, hosted, settings

CATALOG = DATA_ROOT / 'tables/publications.json'
SUBMISSIONS = DATA_ROOT / 'tables/submissions.json'
SITE = 'https://www.trureturing.org/'
REQUEST_PREFIX = 'request-'
FEED_CACHE_ASSET = 'pages-feed-cache.json'
FEED_CACHE_ENV = 'OUTREACH_FEED_CACHE'


def feed_digest(feed):
    """Hash displayed content; a new clock timestamp is not a content change."""
    return hashlib.sha256(w.encode({key: feed.get(key, []) for key in
                                   ('publications', 'collaborations', 'notes')})).hexdigest()


def manifest_identity(asset):
    if not asset or not (asset.get('id') or asset.get('digest')):
        return None
    return hashlib.sha256(w.encode({k: asset.get(k) for k in
                                   ('name', 'id', 'digest', 'size', 'created_at', 'updated_at')})).hexdigest()


class FeedCache:
    """Private, disposable projection cache, never a delivery receipt.

    The key covers the catalog, board, projection code/config, note states and
    immutable manifest asset identities. Live site verification still runs on
    a hit. A missing/corrupt cache or an asset without an identity is a miss.
    """
    def __init__(self, path):
        self.path = Path(path)
        try:
            value = w.read(self.path)
            self.value = value if isinstance(value, dict) else {}
        except (OSError, ValueError):
            self.value = {}

    def key(self, catalog, releases, submissions):
        notes = {}
        for tag, release in sorted(releases.items()):
            state = w.release_state(release, tag.removeprefix('short-note-'))
            manifests = []
            for asset in release['assets']:
                if re.fullmatch(r'note-v[0-9]+\.json', asset['name']):
                    if not (asset.get('id') or asset.get('digest')):
                        return None
                    manifests.append({k: asset.get(k) for k in
                                      ('name', 'id', 'digest', 'size', 'created_at', 'updated_at')})
            notes[tag] = {'state': {k: v for k, v in (state or {}).items()
                                   if k not in {'pages', 'deliveries'}},
                          'manifests': sorted(manifests, key=lambda a: a['name'])}
        code = [hashlib.sha256(path.read_bytes()).hexdigest() for path in
                (Path(__file__), Path(w.__file__), ROOT / 'managed/location.py')]
        return hashlib.sha256(w.encode({'catalog': catalog, 'submissions': submissions,
                                        'notes': notes, 'code': code, 'mathesis': settings('mathesis')})).hexdigest()

    def project(self, catalog, gh, submissions, releases):
        key = self.key(catalog, releases, submissions)
        feed = self.value.get('feed')
        try:
            if key and key == self.value.get('inputs_sha256'):
                w.validate_feed(feed)
                if feed_digest(feed) == self.value.get('feed_sha256'):
                    return copy.deepcopy(feed)
        except (ValueError, TypeError, KeyError, AttributeError):
            pass
        manifests = self.value.get('manifests', {})
        manifests = {tag: entry for tag, entry in manifests.items() if tag in releases} if isinstance(manifests, dict) else {}
        feed = w.project_pages_feed(projection(catalog, gh, submissions, releases=releases, manifests=manifests))
        self.value = {'inputs_sha256': key, 'feed': feed, 'feed_sha256': feed_digest(feed), 'manifests': manifests}
        return feed

    def save(self, feed):
        self.value.update(feed=feed, feed_sha256=feed_digest(feed))
        raw = w.encode(self.value)
        if self.path.is_file() and self.path.read_bytes() == raw:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix('.json.tmp')
        temporary.write_bytes(raw)
        temporary.replace(self.path)


def validate_catalog(catalog):
    w.exact(catalog, {'schema_version', 'publications', 'collaborations', 'notes'})
    if catalog['schema_version'] != 'research-publication.v1':
        raise ValueError('Unsupported publication catalog')
    w.validate_feed({'schema_version': 'research-updates.v1',
                     'publications': catalog['publications'], 'collaborations': catalog['collaborations']})
    if not isinstance(catalog['notes'], list):
        raise ValueError('Managed notes must be a list')
    ids = {p['id'] for kind in ('publications', 'collaborations') for p in catalog[kind]}
    for note in catalog['notes']:
        w.exact(note, {'id', 'problem_ids', 'paused'}, {'project_id'})
        w.slug(note['id'])
        w.problem_ids(note['problem_ids'])
        if not note['problem_ids'] or type(note['paused']) is not bool or note['id'] in ids:
            raise ValueError('Managed note IDs must be distinct, with explicit problems and pause state')
        if 'project_id' in note:
            w.slug(note['project_id'])
        ids.add(note['id'])
    return catalog


def load_submission_board(path=SUBMISSIONS):
    """Load the compact independent-submission board for public projection."""
    from managed import submissions
    board = w.read(path)
    submissions.validate_data(board)
    return board


def submission_publications(board, existing=()):
    """Project only verified public arXiv rows into the existing Pages feed."""
    existing_ids = {paper['id'] for paper in existing}
    existing_identity = {w.publication_identity(paper['url']): paper for paper in existing}
    result = []
    if board is None:
        return result
    for row in board.get('papers', []):
        if row.get('arxiv_status') != 'public' or not row.get('arxiv_url'):
            continue
        arxiv_id = row['arxiv_id']
        version = row['arxiv_version']
        entry = {
            'id': row['id'],
            'problem_ids': [],
            'topic': 'Independent submission',
            'title': row.get('arxiv_title') or row['title'],
            'authors': ', '.join(row['authors']),
            'date': row.get('arxiv_date') or row['version_date'],
            'url': row['arxiv_url'],
            'venue': 'arXiv:' + arxiv_id + version,
            'status': 'preprint',
            'summary': 'Independent public preprint by ' + ', '.join(row['authors']) + '.',
            'evidence_url': row['arxiv_evidence'] or row['arxiv_url'],
        }
        identity = w.publication_identity(entry['url'])
        # A paper the maintained catalog already registers keeps its catalog
        # entry; the board only adds public preprints the catalog lacks.
        if identity in existing_identity:
            continue
        if row['id'] in existing_ids:
            raise ValueError('Independent submission reuses an existing publication ID: ' + row['id'])
        w.validate_publication(entry)
        result.append(entry)
        existing_ids.add(row['id'])
        existing_identity[identity] = entry
    return result


def catalog_note(catalog, note_id, require_active=False):
    for note in catalog['notes']:
        if note['id'] == w.slug(note_id):
            if require_active and note['paused']:
                raise ValueError('This note is paused in the publication catalog')
            return note
    raise ValueError('Register this stable note ID in the publication catalog first')


def register(catalog, metadata, project_id=None):
    w.validate_metadata(metadata)
    result = copy.deepcopy(validate_catalog(catalog))
    selected = {'id': metadata['id'], 'problem_ids': metadata['problem_ids'], 'paused': False}
    if project_id:
        selected['project_id'] = w.slug(project_id)
    previous = next((n for n in result['notes'] if n['id'] == selected['id']), None)
    if previous:
        if previous['problem_ids'] != selected['problem_ids']:
            raise ValueError('Existing note problem links differ; review the catalog change explicitly')
        return result  # Re-importing a note must preserve an existing pause.
    result['notes'].append(selected)
    result['notes'].sort(key=lambda n: n['id'])
    return validate_catalog(result)


def intake(metadata, item, includes, output):
    """Reuse the existing polish directory; archive selected source files only."""
    item = Path(item).resolve()
    names = sorted(set(includes or ['note.tex']))
    with tempfile.TemporaryDirectory() as temp:
        source = Path(temp) / 'source.zip'
        with zipfile.ZipFile(source, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
            for name in names:
                relative = Path(name)
                path = item / relative
                if (relative.is_absolute() or '..' in relative.parts or path.is_symlink()
                        or not path.is_file() or not path.resolve().is_relative_to(item)):
                    raise ValueError('Select regular source files inside the note directory')
                info = zipfile.ZipInfo(relative.as_posix(), (1980, 1, 1, 0, 0, 0))
                info.external_attr = 0o100644 << 16
                info.compress_type = zipfile.ZIP_DEFLATED
                archive.writestr(info, path.read_bytes())
        return w.prepare(metadata, item / 'note.pdf', source, output)


def inventory(items, catalog, results=None):
    """Report coverage; the existence of an attachment never enqueues publication."""
    root = Path(items)
    if not root.is_dir():
        raise ValueError('The existing item directory is missing')
    observed = {p.name: p / 'polish' for p in root.iterdir() if p.is_dir() and w.SLUG.fullmatch(p.name)}
    selected = set(observed)
    if results is not None:
        rows = results.get('results') if isinstance(results, dict) else results
        if not isinstance(rows, list):
            raise ValueError('Results input must be a list or an existing index with results[]')
        selected = {w.slug(row['id']) for row in rows}
    represented = {}
    for kind in ('publications', 'notes'):
        for row in catalog[kind]:
            for problem_id in row['problem_ids']:
                represented.setdefault(problem_id, []).append(row['id'])
    rows = []
    for problem_id in sorted(selected):
        folder = observed.get(problem_id)
        pdf, tex = (bool(folder and (folder / name).is_file()) for name in ('note.pdf', 'note.tex'))
        rows.append({'id': problem_id, 'note': 'present' if pdf and tex else 'incomplete' if pdf or tex else 'missing',
                     'represented_by': sorted(represented.get(problem_id, []))})
    return {'schema_version': 'short-note-inventory.v1', 'results': rows,
            'counts': {kind: sum(row['note'] == kind for row in rows) for kind in ('present', 'incomplete', 'missing')}}


def validate_request(request):
    w.exact(request, {'schema_version', 'id', 'action'}, {'revision', 'publication', 'recover_record', 'reuse_draft_from', 'receipt'})
    if request['schema_version'] != 'short-note-request.v1':
        raise ValueError('Unsupported note request')
    w.slug(request['id'])
    action = request['action']
    if action == 'publish':
        if (type(request.get('revision')) is not int or request['revision'] < 1
                or 'publication' in request or 'receipt' in request):
            raise ValueError('Publish requests need a positive staged revision')
        if 'recover_record' in request and (type(request['recover_record']) is not int or request['recover_record'] < 1):
            raise ValueError('Recovery needs an existing positive Zenodo record ID')
        if 'reuse_draft_from' in request and (type(request['reuse_draft_from']) is not int
                                            or not 0 < request['reuse_draft_from'] < request['revision']):
            raise ValueError('Reuse must select an earlier unpublished artifact revision')
    elif action == 'promote':
        if set(request) != {'schema_version', 'id', 'action', 'publication'}:
            raise ValueError('Promotion needs exactly the later public paper')
        w.validate_publication(request['publication'])
        if request['publication']['id'] != request['id']:
            raise ValueError('Promotion must retain the note ID')
    elif action == 'sync':
        if set(request) != {'schema_version', 'id', 'action'}:
            raise ValueError('Sync requests do not create or revise a manuscript')
    elif action == 'link-mathesis':
        if (set(request) != {'schema_version', 'id', 'action', 'revision', 'receipt'}
                or type(request['revision']) is not int or request['revision'] < 1):
            raise ValueError('Mathesis link requests need an exact staged revision and public receipt')
        collection_version(request['receipt'], request['id'])
    else:
        raise ValueError('Unsupported note request action')
    return request


def request_name(request):
    validate_request(request)
    return REQUEST_PREFIX + hashlib.sha256(w.encode(request)).hexdigest()[:24] + '.json'


def workflow_selection(inputs):
    """Explicit dispatch inputs replace the shared secret as a whole selection."""
    if inputs.get('action') != 'notes':
        return None
    selection = {'schema_version': 'short-note-request.v1',
                 'id': inputs.get('note_id', ''),
                 'action': inputs.get('note_action', '')}
    for key in ('revision', 'recover_record', 'reuse_draft_from'):
        value = inputs.get('note_revision' if key == 'revision' else key, '')
        if value:
            if not re.fullmatch(r'[1-9][0-9]*', value):
                raise ValueError(key + ' must be a positive decimal integer')
            selection[key] = int(value)
    if selection['action'] == 'status':
        w.slug(selection['id'])
        if set(selection) != {'schema_version', 'id', 'action'}:
            raise ValueError('Status needs only a note ID; leave revision and recovery fields empty')
        return selection
    # Operator recovery can only publish an existing staged package.
    if selection['action'] != 'publish':
        raise ValueError('Select publish or status for one note')
    return validate_request(selection)


def check_links(note, metadata):
    if note['id'] != metadata['id'] or set(note['problem_ids']) != set(metadata['problem_ids']):
        raise ValueError('Note and catalog must have the same explicit result mapping')


def note_releases(catalog, gh):
    if not catalog['notes']:
        return {}
    tags = {w.tag_for(note['id']) for note in catalog['notes']}
    return {release['tag_name']: release for release in gh.list_releases() if release['tag_name'] in tags}


def enqueue(catalog, request, gh):
    validate_request(request)
    note = catalog_note(catalog, request['id'], require_active=True)
    tag = w.tag_for(note['id'])
    release = gh.release(tag)
    if release is None:
        raise ValueError('Stage the note before enqueuing delivery')
    name = request_name(request)
    with tempfile.TemporaryDirectory() as temp:
        if request['action'] in {'publish', 'link-mathesis'}:
            revision = request['revision']
            required = {f'note-v{revision}.json', f'note-v{revision}.pdf', f'source-v{revision}.zip'}
            if not required.issubset({a['name'] for a in release['assets']}):
                raise ValueError('The selected note revision has not finished staging')
            package = w.read(gh.download(tag, f'note-v{revision}.json', temp))
            w.validate_metadata(package['metadata'])
            check_links(note, package['metadata'])
            if package['metadata']['revision'] != revision:
                raise ValueError('Staged manifest revision differs from the request')
        elif request['action'] == 'promote':
            check_links(note, request['publication'])
        if name in {a['name'] for a in release['assets']}:
            if w.read(gh.download(tag, name, temp)) != request:
                raise ValueError('Immutable request content differs')
        else:
            path = Path(temp) / name
            path.write_bytes(w.encode(request))
            gh.upload(tag, path)
    return {'id': note['id'], 'request': name, 'status': 'queued'}


def queue(catalog, gh, retry_failed=False, *, releases=None):
    rows = []
    releases = note_releases(catalog, gh) if releases is None else releases
    for note in catalog['notes']:
        release = releases.get(w.tag_for(note['id']))
        if release is None:
            continue
        state = w.release_state(release, note['id']) or {}
        note_rows = []
        for ordinal, asset in enumerate(release['assets']):
            name = asset['name']
            if not (name.startswith(REQUEST_PREFIX) and name.endswith('.json')):
                continue
            previous = state.get('deliveries', {}).get(name, {})
            status = previous.get('status', 'queued')
            if note['paused']:
                status = 'paused'
            if status == 'dispatched' or status == 'paused' or (status == 'failed' and not retry_failed):
                eligible = False
            else:
                eligible = True
            if not eligible and (previous.get('action') or note['paused']):
                request = {'action': previous.get('action', 'paused')}
                if 'revision' in previous:
                    request['revision'] = previous['revision']
            else:
                with tempfile.TemporaryDirectory() as temp:
                    request = validate_request(w.read(gh.download(w.tag_for(note['id']), name, temp)))
                if request_name(request) != name or request['id'] != note['id']:
                    raise ValueError('Request asset identity differs from its note')
            note_rows.append({'id': note['id'], 'request': name, 'status': status,
                              'eligible': eligible, 'operation': request,
                              'order': (asset.get('created_at', ''), asset.get('id', ordinal))})
        promotions = [row for row in note_rows if row['operation']['action'] == 'promote']
        latest = max(promotions, key=lambda row: row['order']) if promotions else None
        for row in note_rows:
            if row['operation']['action'] == 'promote' and row is not latest and row['status'] != 'dispatched':
                row.update(status='superseded', eligible=False)
            if (row['operation']['action'] == 'publish'
                    and state.get('versions', {}).get(str(row['operation']['revision']), {}).get('phase') == 'superseded'):
                row.update(status='superseded', eligible=False)
            if (row['operation']['action'] == 'publish' and row['status'] == 'failed'
                    and state.get('versions', {}).get(str(row['operation']['revision']), {}).get('publication')):
                # Preserve the failed historical receipt; a later request has
                # already recovered this exact immutable revision.
                row.update(status='recovered', eligible=False)
        rows.extend(note_rows)
    return sorted(rows, key=lambda row: (row['id'], row['operation'].get('revision', 10**9), row['order']))


def projection(catalog, gh, submissions=None, *, releases=None, manifests=None):
    """Full selected scientific metadata. Apply project_pages_feed for Pages."""
    feed = {'schema_version': 'research-updates.v1', 'updated_at': w.now(),
            'publications': copy.deepcopy(catalog['publications']),
            'collaborations': copy.deepcopy(catalog['collaborations'])}
    feed['publications'].extend(submission_publications(submissions, feed['publications']))
    releases = note_releases(catalog, gh) if releases is None else releases
    notes = []
    for note in catalog['notes']:
        release = releases.get(w.tag_for(note['id']))
        state = w.release_state(release, note['id']) if release else None
        if state and state.get('publication'):
            if state['zenodo_api'] != 'https://zenodo.org/api':
                raise ValueError('Sandbox output must never enter the public projection')
            check_links(note, state['publication'])
            feed['publications'].append(copy.deepcopy(state['publication']))
        if state:
            public_note = note_projection(note, state, gh, release=release, manifests=manifests)
            if public_note:
                notes.append(public_note)
    if notes:
        feed['notes'] = sorted(notes, key=lambda n: n['id'])
    feed['publications'].sort(key=lambda p: p['id'])
    feed['collaborations'].sort(key=lambda c: c['id'])
    w.validate_feed(feed)
    if len(w.encode(feed)) > w.MAX_FEED_BYTES:
        raise ValueError('Public projection exceeds the Pages limit')
    return feed


def note_projection(note, state, gh, *, release=None, manifests=None):
    """Keep note citations even after a journal paper becomes the preferred link."""
    published = [int(v) for v, entry in state['versions'].items() if entry.get('publication')]
    mathesis = state.get('mathesis_publication')
    revision = max(published + ([mathesis['revision']] if mathesis else []), default=0)
    if not revision:
        return None
    if state['zenodo_api'] != 'https://zenodo.org/api':
        raise ValueError('Sandbox output must never enter the public projection')
    tag, name = w.tag_for(note['id']), f'note-v{revision}.json'
    asset = next((a for a in (release or {}).get('assets', []) if a['name'] == name), None)
    identity = manifest_identity(asset)
    cached = manifests.get(tag, {}) if manifests is not None else {}
    package = cached.get('package') if isinstance(cached, dict) else None
    if (not identity or not isinstance(package, dict) or cached.get('identity') != identity
            or hashlib.sha256(w.encode(package)).hexdigest() != cached.get('sha256')):
        with tempfile.TemporaryDirectory() as temp:
            package = w.read(gh.download(tag, name, temp))
    if manifests is not None and identity:
        manifests[tag] = {'identity': identity, 'package': package, 'sha256': hashlib.sha256(w.encode(package)).hexdigest()}
    meta = w.validate_metadata(package['metadata'])
    check_links(note, meta)
    if meta['revision'] != revision:
        raise ValueError('Public note revision differs from its manifest')
    public = {key: copy.deepcopy(meta[key]) for key in ('id', 'problem_ids', 'title', 'revision', 'evidence_url')}
    entry = state['versions'].get(str(revision), {})
    if entry.get('publication'):
        if hashlib.sha256(w.encode(package)).hexdigest() != entry['package_id']:
            raise ValueError('Published note manifest differs from the archived version')
        record = int(entry['record_id'])
        public.update(doi='https://doi.org/' + entry['doi'],
                      pdf_url=f'https://zenodo.org/records/{record}/files/note-v{revision}.pdf',
                      source_url=f'https://zenodo.org/records/{record}/files/source-v{revision}.zip')
    if mathesis and mathesis['revision'] == revision:
        if package['files'][f'source-v{revision}.zip'] != mathesis['source_sha256']:
            raise ValueError('Mathesis source differs from the selected public note')
        if mathesis.get('pdf_sha256') and package['files'][f'note-v{revision}.pdf'] != mathesis['pdf_sha256']:
            raise ValueError('Mathesis PDF differs from the selected public note')
        public['mathesis_url'] = mathesis['url']
    return w.validate_public_note(public)


def collection_version(receipt, note_id):
    """A collection receipt identifies a public version, never a venue admission."""
    if not isinstance(receipt, dict):
        raise ValueError('Mathesis collection receipt must be an object')
    version = receipt.get('version')
    if (receipt.get('schema') != 'mathesis.pre-reviewed-receipt.v1'
            or receipt.get('status') != 'collected' or receipt.get('work_id') != note_id
            or type(version) is not int or not 0 < version <= 2**32 - 1
            or receipt.get('public_path') != f'/papers/{note_id}?version={version}'
            or any(not isinstance(receipt.get(key), str)
                   or not re.fullmatch(r'[0-9a-f]{64}', receipt[key])
                   for key in ('pdf_sha256', 'source_sha256'))):
        raise ValueError('Mathesis collection receipt does not identify this public note version')
    return version


def public_mathesis(path):
    """Read only the fixed public collection endpoint; no submitting credential."""
    target = settings('mathesis')['base_url'] + path
    with urlopen(Request(target, headers={'User-Agent': 'trureturing-outreach/1.0'}), timeout=30) as response:
        raw = response.read(30 * 1024 * 1024 + 1)
    if not raw or len(raw) > 30 * 1024 * 1024:
        raise ValueError('Invalid or oversized Mathesis public response')
    return raw


def record_mathesis(catalog, note_id, receipt, gh, revision=None, fetch=None):
    """Register an actual public receipt, never expose a submitting-user URL."""
    note = catalog_note(catalog, note_id)
    collected = receipt.get('schema') == 'mathesis.pre-reviewed-receipt.v1'
    if collected:
        version = collection_version(receipt, note_id)
        revision = version if revision is None else revision
        if type(revision) is not int or revision < 1:
            raise ValueError('Select the exact immutable Outreach note revision')
        record, path = note_id, receipt['public_path']
    else:
        if (receipt.get('schema') != 'mathesis.manuscript-receipt.v1'
                or receipt.get('producer') != 'trureturing-outreach' or receipt.get('work_id') != note_id):
            raise ValueError('Mathesis receipt does not identify this Outreach note')
        publication = receipt.get('publication') or {}
        if publication.get('public') is not True or publication.get('version') != receipt.get('version'):
            raise ValueError('Mathesis has not returned a public publication for this version')
        upstream_revision = int(receipt['upstream_version'])
        if (str(upstream_revision) != receipt['upstream_version'] or upstream_revision < 1
                or revision is not None and revision != upstream_revision):
            raise ValueError('Mathesis upstream revision must identify an immutable note')
        revision = upstream_revision
        record, path = publication['record'], '/papers/' + publication['record']
    with tempfile.TemporaryDirectory() as temp:
        package = w.read(gh.download(w.tag_for(note_id), f'note-v{revision}.json', temp))
    w.validate_metadata(package['metadata'])
    check_links(note, package['metadata'])
    if (package['metadata']['revision'] != revision or
            package['files'][f'source-v{revision}.zip'] != receipt.get('source_sha256')):
        raise ValueError('Mathesis did not publish the staged note source')
    if collected:
        if package['files'][f'note-v{revision}.pdf'] != receipt.get('pdf_sha256'):
            raise ValueError('Mathesis did not publish the staged note PDF')
        fetch = fetch or public_mathesis
        endpoint = f'/api/v1/collected-papers/{note_id}'
        paper = json.loads(fetch(endpoint + f'?version={version}'))
        meta = package['metadata']
        authors = [{k: v for k, v in author.items() if k in {'name', 'affiliation', 'orcid'}}
                   for author in meta['creators']]
        if (not isinstance(paper, dict) or paper.get('id') != note_id or paper.get('revision') != str(version)
                or paper.get('title') != meta['title'] or paper.get('abstract') != meta['abstract']
                or not w.same_collection_authors(authors, paper.get('authors')) or paper.get('license') != 'CC-BY-4.0'):
            raise ValueError('Mathesis public bibliography differs from the staged note')
        for kind in ('pdf', 'source'):
            file_path = endpoint + f'/files/{kind}?version={version}'
            if (paper.get(kind, {}).get('url') != file_path
                    or hashlib.sha256(fetch(file_path)).hexdigest() != receipt[kind + '_sha256']):
                raise ValueError('Mathesis public files differ from the staged note')
    result = {'revision': revision, 'source_sha256': receipt['source_sha256'],
              'record': record, 'url': settings('mathesis')['base_url'] + path}
    if collected:
        result.update(channel='collection', mathesis_version=version, pdf_sha256=receipt['pdf_sha256'])
    w.validate_public_note({**{k: package['metadata'][k] for k in ('id', 'problem_ids', 'title', 'revision', 'evidence_url')},
                            'mathesis_url': result['url']})
    def update(state):
        previous = state.get('mathesis_publication')
        if previous and previous['revision'] > revision:
            raise ValueError('Do not replace a newer public Mathesis note with an older receipt')
        if (previous and previous['revision'] == revision and collected
                and previous.get('mathesis_version', 0) > version):
            raise ValueError('Do not replace a newer Mathesis collection version with an older receipt')
        state['mathesis_publication'] = result
    update_receipt(gh, note_id, update)
    return result


def coverage(catalog, gh, results=None):
    releases = note_releases(catalog, gh)
    rows = []
    for note in catalog['notes']:
        release = releases.get(w.tag_for(note['id']))
        state = w.release_state(release, note['id']) if release else None
        public = note_projection(note, state, gh) if state else None
        pending = [v for v in (state or {}).get('versions', {}).values() if v.get('reserved_doi') and not v.get('publication')]
        rows.append({'id': note['id'], 'problem_ids': note['problem_ids'], 'paused': note['paused'],
                     'staged': release is not None, 'doi_reserved': bool(pending),
                     'doi_published': bool(public and public.get('doi')),
                     'mathesis_public': bool(public and public.get('mathesis_url')),
                     'pages': pages_coverage(state, public)})
    result_ids = {r['id'] for r in (results or {}).get('results', [])
                  if r.get('upstream_status', 'resolved-at-current-snapshot') == 'resolved-at-current-snapshot'}
    def linked(flag=None):
        return {p for row in rows if flag is None or row[flag] for p in row['problem_ids']}
    return {'schema_version': 'note-delivery-coverage.v1', 'notes': rows,
            'result_coverage': {'resolved_results': len(result_ids),
                                'with_registered_note': len(result_ids & linked()),
                                'with_published_doi': len(result_ids & linked('doi_published')),
                                'with_public_mathesis': len(result_ids & linked('mathesis_public')),
                                'missing_registered_note': len(result_ids - linked())},
            'missing_doi': [r['id'] for r in rows if not r['doi_published']],
            'missing_mathesis': [r['id'] for r in rows if not r['mathesis_public']]}


def update_receipt(gh, note_id, update):
    tag = w.tag_for(note_id)
    release = gh.release(tag)
    if release is None:
        raise ValueError('Missing private note release')
    state = w.release_state(release, note_id)
    if state is None:
        state = {'schema_version': 'short-note-state.v1', 'id': note_id,
                 'zenodo_api': 'https://zenodo.org/api', 'versions': {}, 'preferred': 'note'}
    update(state)
    gh.save_state(tag, state)
    return state


def mark_delivery(gh, row, status, error=None):
    def update(state):
        previous = state.setdefault('deliveries', {}).get(row['request'], {})
        value = {'status': status, 'updated_at': w.now(),
                 'attempts': previous.get('attempts', 0) + (status in ('archived', 'failed')),
                 'action': row['operation']['action']}
        if 'revision' in row['operation']:
            value['revision'] = row['operation']['revision']
        if error:
            value['error'] = str(error)[:500]
        state['deliveries'][row['request']] = value
    return update_receipt(gh, row['id'], update)


def pages_coverage(state, public):
    state = state or {}
    paper = state.get('publication')
    paper = paper if paper and w.is_pages_publication(paper) else None
    public = w.pages_note(public)
    if not paper and not public:
        return 'not_applicable' if state.get('publication') else 'not-dispatched'
    receipt = state.get('pages', {})
    return (receipt.get('status', 'not-dispatched')
            if receipt.get('public_note') == public and receipt.get('publication') == paper else 'outdated')


def synchronize(catalog, gh, pages, submissions=None, *, feed=None, releases=None):
    releases = note_releases(catalog, gh) if releases is None else releases
    feed = w.project_pages_feed(projection(catalog, gh, submissions, releases=releases)) if feed is None else feed
    receipt = w.deliver_feed(feed, pages)
    published = {p['id']: p for p in feed['publications']}
    public_notes = {n['id']: n for n in feed.get('notes', [])}
    for note in catalog['notes']:
        release = releases.get(w.tag_for(note['id']))
        state = w.release_state(release, note['id']) if release else None
        if state and (state.get('publication') or state.get('mathesis_publication')):
            eligible = note['id'] in published or note['id'] in public_notes
            if (state.get('pages', {}).get('publication') != published.get(note['id'])
                    or state.get('pages', {}).get('public_note') != public_notes.get(note['id'])
                    or not state.get('pages')
                    or (not eligible and state['pages'].get('status') != 'not_applicable')):
                recorded = {**receipt, 'publication': published.get(note['id']), 'public_note': public_notes.get(note['id'])}
                if not eligible:
                    recorded.update(status='not_applicable', reason='Zenodo archive belongs to Mathesis, not Pages Research')
                update_receipt(gh, note['id'], lambda saved: saved.update(pages=recorded))
    return receipt


def synchronize_if_changed(catalog, gh, pages, submissions=None, *, cache, releases=None, fetch=None, site=SITE):
    releases = note_releases(catalog, gh) if releases is None else releases
    desired = cache.project(catalog, gh, submissions, releases)
    verified = verify_feed(catalog, gh, site, fetch, submissions, expected=desired, pages=pages)
    release = pages.release('research-updates')
    pending = any(asset['name'] == 'research-updates.pending.json' for asset in (release or {}).get('assets', []))
    if verified['status'] == 'deployed' and not pending:
        cache.save(desired)
        return verified
    receipt = synchronize(catalog, gh, pages, submissions, feed=desired, releases=releases)
    cache.save(desired)
    return receipt


def drain(catalog, gh, zenodo, pages, limit=10, retry_failed=False, budget_seconds=900, submissions=None,
          *, cache=None, fetch=None, only_request=None):
    if not 1 <= limit <= 100 or budget_seconds <= 0:
        raise ValueError('Select a batch limit of 1..100 and a positive time budget')
    releases = note_releases(catalog, gh)
    planned = [row for row in queue(catalog, gh, retry_failed, releases=releases) if row['eligible']]
    if only_request is not None:
        planned = [row for row in planned if row['request'] == only_request]
    if any(row['operation']['action'] == 'publish' for row in planned) and zenodo is None:
        raise ValueError('ZENODO_TOKEN is required before processing queued publications')
    if planned and pages is None:
        raise ValueError('PAGES_TOKEN is required before processing delivery requests')
    accepted, failed = [], []
    attempted = 0
    started = time.monotonic()
    for row in planned[:limit]:
        if time.monotonic() - started >= budget_seconds:
            break
        request = row['operation']
        attempted += 1
        try:
            note = catalog_note(catalog, row['id'], require_active=True)
            if request['action'] == 'promote':
                check_links(note, request['publication'])
            if request['action'] == 'publish':
                with tempfile.TemporaryDirectory() as temp:
                    package = w.read(gh.download(w.tag_for(row['id']), f"note-v{request['revision']}.json", temp))
                check_links(note, package['metadata'])
            if request['action'] == 'link-mathesis':
                record_mathesis(catalog, row['id'], request['receipt'], gh, revision=request['revision'])
            else:
                w.operate(request['action'], row['id'], request.get('revision'), gh, zenodo,
                          request.get('publication'), request.get('recover_record'),
                          reuse_draft_from=request.get('reuse_draft_from'))
            mark_delivery(gh, row, 'archived')
            accepted.append(row)
        except (ValueError, RuntimeError, OSError, KeyError, zipfile.BadZipFile) as error:
            mark_delivery(gh, row, 'failed', error)
            failed.append({'id': row['id'], 'request': row['request'], 'error': str(error)[:500]})
    projection_changed = False
    if accepted:
        try:
            if cache is None:
                synchronize(catalog, gh, pages, submissions)
            else:
                synchronize_if_changed(catalog, gh, pages, submissions, cache=cache, fetch=fetch)
            for row in accepted:
                mark_delivery(gh, row, 'dispatched')
        except (ValueError, RuntimeError, OSError, KeyError) as error:
            for row in accepted:
                mark_delivery(gh, row, 'awaiting_pages', error)
            failed.append({'id': 'pages', 'error': str(error)[:500]})
    elif pages is not None:
        # Article/relationship updates do not require another note publication.
        # Do not redispatch an unchanged feed on every hourly wake.
        try:
            if attempted:
                releases = note_releases(catalog, gh)
            if cache is not None:
                receipt = synchronize_if_changed(catalog, gh, pages, submissions,
                                                 cache=cache, releases=releases, fetch=fetch)
                projection_changed = receipt['status'] != 'deployed'
            else:
                desired = w.project_pages_feed(projection(catalog, gh, submissions, releases=releases))
                with tempfile.TemporaryDirectory() as temp:
                    current, _, names = w.pages_feed(pages, temp)
                if ('research-updates.pending.json' in names or not current.get('updated_at')
                        or feed_digest(current) != feed_digest(desired)
                        or verify_feed(catalog, gh, site=SITE, fetch=fetch, submissions=submissions,
                                       expected=desired, pages=pages)['status'] != 'deployed'):
                    synchronize(catalog, gh, pages, submissions, feed=desired, releases=releases)
                    projection_changed = True
        except (ValueError, RuntimeError, OSError, KeyError) as error:
            failed.append({'id': 'pages', 'error': str(error)[:500]})
    current = queue(catalog, gh, releases=None if attempted else releases)
    remaining = sum(row['eligible'] for row in current)
    attention = [{k: row[k] for k in ('id', 'request', 'status')} for row in current if row['status'] == 'failed']
    return {'processed': len(accepted), 'failed': failed, 'remaining': remaining,
            'needs_attention': attention,
            'status': 'needs_attention' if failed or attention else 'dispatch_accepted' if accepted or projection_changed else 'idle'}


def handoff(catalog, gh):
    """Export public artifacts for downstream adaptation; do not submit to Mathesis."""
    result = {'schema_version': 'published-notes.v1', 'publications': projection(catalog, gh)['publications'], 'notes': []}
    releases = note_releases(catalog, gh)
    for note in catalog['notes']:
        release = releases.get(w.tag_for(note['id']))
        state = w.release_state(release, note['id']) if release else None
        if not state or not state.get('publication'):
            continue
        versions = []
        for number, version in sorted(state['versions'].items(), key=lambda pair: int(pair[0])):
            if not version.get('publication'):
                continue
            with tempfile.TemporaryDirectory() as temp:
                package = w.read(gh.download(w.tag_for(note['id']), f'note-v{number}.json', temp))
            if hashlib.sha256(w.encode(package)).hexdigest() != version['package_id']:
                raise ValueError('Published note manifest differs from the archived version')
            w.validate_metadata(package['metadata'])
            record = int(version['record_id'])
            versions.append({'metadata': package['metadata'], 'doi': version['doi'],
                             'sha256': package['files'],
                             'files': {name: f'https://zenodo.org/records/{record}/files/{name}' for name in package['files']}})
        result['notes'].append({'id': note['id'], 'preferred_publication': state['publication'], 'versions': versions,
                                'public_links': note_projection(note, state, gh)})
    return result


def verify_deployment(catalog, note_id, gh, site=SITE, fetch=None):
    catalog_note(catalog, note_id)
    release = gh.release(w.tag_for(note_id))
    state = w.release_state(release, note_id) if release else None
    if not state or not (state.get('publication') or state.get('mathesis_publication')):
        raise ValueError('Archive and dispatch this note before verifying deployment')
    expected = state.get('publication')
    expected = expected if expected and w.is_pages_publication(expected) else None
    public_note = w.pages_note(note_projection(catalog_note(catalog, note_id), state, gh))
    if not expected and not public_note:
        receipt = {'status': 'not_applicable', 'publication': None, 'public_note': None,
                   'reason': 'Zenodo archive belongs to Mathesis, not Pages Research'}
        update_receipt(gh, note_id, lambda saved: saved.update(pages=receipt))
        return receipt
    if not state.get('pages'):
        raise ValueError('Dispatch this paper or Mathesis reading link before verifying deployment')
    target = w.url(site.rstrip('/') + '/assets/publication-index.json')
    def download():
        with urlopen(Request(target, headers={'User-Agent': 'trureturing-outreach/1.0', 'Accept': 'application/json'}), timeout=30) as response:
            raw = response.read(w.MAX_INDEX_BYTES + 1)
        if len(raw) > w.MAX_INDEX_BYTES:
            raise ValueError('Public publication index exceeds the verification budget')
        return json.loads(raw)
    try:
        index = (fetch or download)()
        if (not isinstance(index, dict) or index.get('schema_version') != 'publication-index.v1'
                or not isinstance(index.get('publications'), list)):
            raise ValueError('Unexpected public publication index')
        if not isinstance(index.get('updated_at'), str):
            raise ValueError('The served publication index has no dispatched feed timestamp yet')
        matched = not expected or any(p['id'] in (note_id, 'paper-' + note_id)
                      and w.publication_identity(p['url']) == w.publication_identity(expected['url'])
                      and all(p.get(key) == expected[key] for key in ('title', 'authors', 'summary'))
                      for p in index['publications'])
        if public_note:
            matched = matched and public_note in index.get('notes', [])
        actual_time = datetime.fromisoformat(index['updated_at'].replace('Z', '+00:00'))
        required_time = datetime.fromisoformat(state['pages']['feed_updated_at'].replace('Z', '+00:00'))
        if actual_time.tzinfo is None:
            raise ValueError('The served publication index timestamp needs a timezone')
        if not matched or actual_time < required_time:
            raise ValueError('The served site has not displayed this dispatched citation yet')
        receipt = {**state['pages'], 'status': 'deployed', 'verified_at': w.now(), 'index_url': target}
    except (ValueError, OSError, KeyError, TypeError) as error:
        receipt = {**state['pages'], 'status': 'verification_pending', 'checked_at': w.now(), 'error': str(error)[:500]}
    update_receipt(gh, note_id, lambda saved: saved.update(pages=receipt))
    return receipt


def verify_feed(catalog, gh, site=SITE, fetch=None, submissions=None, *, expected=None, pages=None):
    """Verify papers, public note links and brief relationships on the served site."""
    expected = w.project_pages_feed(projection(catalog, gh, submissions)) if expected is None else expected
    target = w.url(site.rstrip('/') + '/assets/publication-index.json')
    try:
        if fetch:
            index = fetch()
        else:
            with urlopen(Request(target, headers={'User-Agent': 'trureturing-outreach/1.0', 'Accept': 'application/json'}), timeout=30) as response:
                raw = response.read(w.MAX_INDEX_BYTES + 1)
            if len(raw) > w.MAX_INDEX_BYTES:
                raise ValueError('Public publication index exceeds the verification budget')
            index = json.loads(raw)
        if (not isinstance(index, dict) or index.get('schema_version') != 'publication-index.v1'
                or not isinstance(index.get('publications'), list)
                or not isinstance(index.get('updated_at'), str) or not index['updated_at']
                or datetime.fromisoformat(index['updated_at'].replace('Z', '+00:00')).tzinfo is None):
            raise ValueError('The served public index has no valid feed identity')
        if any(not w.is_pages_publication(p) for p in index['publications']):
            raise ValueError('The served Research index contains an ineligible archive or draft')
        def identity(paper):
            return (paper['id'], w.publication_identity(paper['url']))
        def aliases(papers):
            return {(prefix + p['id'], w.publication_identity(p['url']))
                    for p in papers for prefix in ('', 'paper-')}
        # The index has no source tag. Pages merges its authored catalog with
        # this complete feed, retaining authored anchors and illustrations.
        # Only those independent catalog papers may be extra. In particular,
        # neither the released feed nor a disposable cache proves a removal.
        authored = w.pages_authored_publications(pages if pages is not None else w.GitHub(w.PAGES))
        desired_ids = aliases(expected['publications'])
        unrelated_ids = aliases(authored) - desired_ids
        # Managed notes and submission rows remain source-owned when their
        # paper is withdrawn or becomes archive-only, even if Pages also has
        # an authored card for the old citation.
        owned = {prefix + row['id'] for row in
                 [*catalog['notes'], *catalog['publications'], *(submissions or {}).get('papers', [])]
                 for prefix in ('', 'paper-')}
        actual = [p for p in index['publications']
                  if p['id'] in owned or identity(p) not in unrelated_ids]
        if (len(actual) != len(expected['publications'])
                or any(identity(p) not in desired_ids for p in actual)):
            raise ValueError('The served site has different publications from this feed')
        for paper in expected['publications']:
            if not any(p.get('id') in (paper['id'], 'paper-' + paper['id'])
                       and w.publication_identity(p['url']) == w.publication_identity(paper['url'])
                       and all(p.get(k) == value for k, value in paper.items() if k not in ('id', 'url', 'status'))
                       and p.get('status', '').lower() == paper['status']
                       for p in index['publications']):
                raise ValueError('The served site does not display the current paper: ' + paper['id'])
        for key in ('notes', 'collaborations'):
            if sorted(index.get(key, []), key=lambda n: n['id']) != sorted(expected.get(key, []), key=lambda n: n['id']):
                raise ValueError('The served site has different ' + key)
        return {'status': 'deployed', 'index_url': target, 'verified_at': w.now(),
                'publications': len(expected['publications']), 'notes': len(expected.get('notes', [])),
                'research_updates': len(expected['collaborations']), 'feed_updated_at': index['updated_at']}
    except (ValueError, RuntimeError, OSError, KeyError, TypeError) as error:
        return {'status': 'verification_pending', 'index_url': target, 'error': str(error)[:500]}


def note_status(catalog, note_id, gh):
    note = catalog_note(catalog, note_id)
    release = gh.release(w.tag_for(note_id))
    state = w.release_state(release, note_id) if release else None
    scoped = {**catalog, 'notes': [note]}
    pending = [{k: v for k, v in row.items() if k not in ('operation', 'order')} for row in queue(scoped, gh)]
    return {'id': note_id, 'paused': note['paused'], 'staged': release is not None,
            'versions': state.get('versions', {}) if state else {},
            'preferred_publication': state.get('publication') if state else None,
            'pages': state.get('pages') if state else None, 'requests': pending}


def doctor(catalog, gh, pages=None, zenodo=None, remote=False):
    checks = {'catalog_valid': bool(validate_catalog(catalog)), 'github_cli_installed': shutil.which('gh') is not None,
              'zenodo_token_configured': bool(zenodo), 'pages_token_configured': bool(pages)}
    diagnostics = {}
    if remote:
        for name, client, path in [('outreach_private', gh, f'repos/{w.OUTREACH}'),
                                   ('pages_write_access', pages, f'repos/{w.PAGES}')]:
            try:
                if client is None:
                    raise ValueError('Token is not configured')
                metadata = json.loads(client.run('api', path))
                checks[name] = metadata['private'] if name == 'outreach_private' else (
                    not metadata['private'] and metadata.get('permissions', {}).get('push') is True)
            except (ValueError, RuntimeError, OSError, KeyError):
                checks[name] = False
        try:
            checks['zenodo_account_accessible'] = zenodo is not None and isinstance(
                zenodo.request('GET', '/deposit/depositions?size=1'), list)
        except (ValueError, RuntimeError, OSError) as error:
            checks['zenodo_account_accessible'] = False
            # Do not include response bodies, exception text or credentials.
            diagnostics['zenodo'] = {'error_type': type(error).__name__}
            if isinstance(error, w.ZenodoHTTPError):
                diagnostics['zenodo']['http_status'] = error.code
    return {'checks': checks, 'diagnostics': diagnostics, 'remote': remote,
            'ready': all(checks.values()),
            'note': 'Token scope and actual publication/deployment are verified separately by their operations.'}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--catalog', default=str(CATALOG))
    parser.add_argument('--output', help='Save the JSON receipt/report in addition to stdout')
    commands = parser.add_subparsers(dest='command', required=True)
    commands.add_parser('validate')
    inv = commands.add_parser('inventory')
    inv.add_argument('--items', required=True)
    inv.add_argument('--results')
    imported = commands.add_parser('intake')
    for name in ('metadata', 'item', 'output-directory'):
        imported.add_argument('--' + name, required=True)
    imported.add_argument('--include', action='append')
    prepared = commands.add_parser('prepare', help='Accept an already selected scientific source ZIP')
    for name in ('metadata', 'pdf', 'source', 'output-directory'):
        prepared.add_argument('--' + name, required=True)
    reg = commands.add_parser('register')
    reg.add_argument('--metadata', required=True)
    reg.add_argument('--project-id')
    staged = commands.add_parser('stage')
    staged.add_argument('directory')
    staged.add_argument('--enqueue', action='store_true')
    for name in ('enqueue', 'publish', 'reserve', 'promote', 'sync', 'verify', 'status'):
        cmd = commands.add_parser(name)
        cmd.add_argument('--id', required=name != 'sync')
        if name in ('enqueue', 'publish', 'reserve'):
            cmd.add_argument('--revision', type=int, default=1)
            cmd.add_argument('--recover-record', type=int)
            cmd.add_argument('--reuse-draft-from', type=int)
        if name == 'enqueue':
            cmd.add_argument('--action', choices=['publish', 'sync', 'promote'], default='publish')
        if name in ('enqueue', 'promote'):
            cmd.add_argument('--publication')
        if name == 'verify':
            cmd.add_argument('--site', default=SITE)
    for name in ('queue', 'drain'):
        cmd = commands.add_parser(name)
        cmd.add_argument('--retry-failed', action='store_true')
        if name == 'drain':
            cmd.add_argument('--limit', type=int, default=10)
    commands.add_parser('project')
    commands.add_parser('handoff')
    commands.add_parser('coverage')
    verified = commands.add_parser('verify-feed')
    verified.add_argument('--site', default=SITE)
    linked = commands.add_parser('link-mathesis')
    linked.add_argument('--id', required=True)
    linked.add_argument('--receipt', required=True)
    health = commands.add_parser('doctor')
    health.add_argument('--remote', action='store_true')
    commands.add_parser('ci')
    return parser.parse_args(argv)


def execute(args):
    catalog = validate_catalog(w.read(args.catalog))
    submission_board = load_submission_board()
    gh = w.GitHub(w.OUTREACH)
    cache_path = os.environ.get(FEED_CACHE_ENV)
    cache = FeedCache(cache_path) if hosted() and DATA_ROOT != ROOT and cache_path else None
    if args.command == 'ci':
        args.command = os.environ.get('NOTE_ACTION') or 'drain'
        if args.command not in ('reserve', 'publish', 'promote', 'sync', 'drain', 'verify', 'verify-feed', 'doctor', 'status'):
            raise ValueError('Unsupported workflow action')
        args.id = os.environ.get('NOTE_ID')
        args.revision = int(os.environ.get('NOTE_REVISION') or 1)
        args.recover_record = int(os.environ['RECOVER_RECORD']) if os.environ.get('RECOVER_RECORD') else None
        args.reuse_draft_from = int(os.environ['REUSE_DRAFT_FROM']) if os.environ.get('REUSE_DRAFT_FROM') else None
        args.publication = json.loads(os.environ['PREFERRED_PUBLICATION']) if args.command == 'promote' else None
        args.retry_failed = os.environ.get('RETRY_FAILED') == 'true'
        args.limit = int(os.environ.get('BATCH_LIMIT') or 10)
        args.site, args.remote = SITE, True
    pages = (w.GitHub(w.PAGES, os.environ['PAGES_TOKEN']) if os.environ.get('PAGES_TOKEN') else
             w.GitHub(w.PAGES) if os.environ.get('GITHUB_ACTIONS') != 'true' else None)
    zenodo = w.Zenodo(os.environ['ZENODO_TOKEN']) if os.environ.get('ZENODO_TOKEN') else None
    if args.command == 'validate':
        result = {'catalog_valid': True, 'publications': len(catalog['publications']),
                  'collaborations': len(catalog['collaborations']), 'managed_notes': len(catalog['notes'])}
    elif args.command == 'register':
        catalog = register(catalog, w.read(args.metadata), args.project_id)
        Path(args.catalog).write_bytes(w.encode(catalog))
        result = {'registered': w.read(args.metadata)['id']}
    elif args.command == 'intake':
        path = intake(args.metadata, args.item, args.include, args.output_directory)
        result = {'prepared_directory': str(path)}
    elif args.command == 'prepare':
        path = w.prepare(args.metadata, args.pdf, args.source, args.output_directory)
        result = {'prepared_directory': str(path)}
    elif args.command == 'inventory':
        result = inventory(args.items, catalog, w.read(args.results) if args.results else None)
    elif args.command == 'stage':
        result = w.stage(args.directory, gh)
        if args.enqueue:
            result = enqueue(catalog, {'schema_version': 'short-note-request.v1', 'id': result['note_id'],
                                       'action': 'publish', 'revision': result['revision']}, gh)
    elif args.command == 'reserve':
        note = catalog_note(catalog, args.id, require_active=True)
        if zenodo is None:
            raise ValueError('ZENODO_TOKEN is required for DOI reservation')
        with tempfile.TemporaryDirectory() as temp:
            package = w.read(gh.download(w.tag_for(args.id), f'note-v{args.revision}.json', temp))
        check_links(note, package['metadata'])
        state = w.operate('reserve', args.id, args.revision, gh, zenodo,
                          recover_record=args.recover_record, reuse_draft_from=args.reuse_draft_from)
        entry = state['versions'][str(args.revision)]
        result = {'id': args.id, 'revision': args.revision, 'phase': entry['phase'],
                  'reserved_doi': entry.get('reserved_doi'), 'published_doi': entry.get('doi'),
                  'record_id': entry['record_id']}
    elif args.command in ('enqueue', 'publish', 'promote'):
        action = args.action if args.command == 'enqueue' else args.command
        request = {'schema_version': 'short-note-request.v1', 'id': args.id, 'action': action}
        if action == 'publish':
            request['revision'] = args.revision
            if args.recover_record is not None:
                request['recover_record'] = args.recover_record
            if args.reuse_draft_from is not None:
                request['reuse_draft_from'] = args.reuse_draft_from
        elif action == 'promote':
            request['publication'] = args.publication if isinstance(args.publication, dict) else w.read(args.publication)
        result = enqueue(catalog, request, gh)
        if args.command != 'enqueue':
            result = drain(catalog, gh, zenodo, pages, getattr(args, 'limit', 10),
                           True, submissions=submission_board, only_request=request_name(request))
    elif args.command == 'drain':
        result = drain(catalog, gh, zenodo, pages, args.limit, args.retry_failed,
                       submissions=submission_board, cache=cache)
    elif args.command == 'queue':
        result = [{k: v for k, v in row.items() if k not in ('operation', 'order')}
                  for row in queue(catalog, gh, args.retry_failed)]
    elif args.command == 'project':
        result = w.project_pages_feed(projection(catalog, gh, submission_board))
    elif args.command == 'handoff':
        result = handoff(catalog, gh)
    elif args.command == 'coverage':
        result = coverage(catalog, gh, w.read(DATA_ROOT / 'tables/results.json'))
    elif args.command == 'verify-feed':
        result = verify_feed(catalog, gh, args.site, submissions=submission_board, pages=pages)
    elif args.command == 'link-mathesis':
        result = record_mathesis(catalog, args.id, w.read(args.receipt), gh)
    elif args.command == 'sync':
        if pages is None:
            raise ValueError('PAGES_TOKEN is required for public feed delivery')
        result = (synchronize_if_changed(catalog, gh, pages, submission_board, cache=cache)
                  if cache is not None else synchronize(catalog, gh, pages, submission_board))
    elif args.command == 'verify':
        result = verify_deployment(catalog, args.id, gh, args.site)
    elif args.command == 'status':
        result = note_status(catalog, args.id, gh)
    else:
        result = doctor(catalog, gh, pages, zenodo, args.remote)
    return result


def main(argv=None):
    args = parse_args(argv)
    try:
        result = execute(args)
    except (ValueError, RuntimeError, OSError, KeyError, TypeError, zipfile.BadZipFile) as error:
        result = {'status': 'failed', 'error': str(error)[:500]}
    raw = w.encode(result)
    if args.output:
        Path(args.output).write_bytes(raw)
    print(raw.decode(), end='')
    if isinstance(result, dict) and 'needs_attention' in result:
        # Hosted stdout is private. Emit counts through its public boundary,
        # without note IDs, request handles, metadata or exception text.
        from managed.run_log import OUTPUT
        emit = OUTPUT.get()
        if emit is not None:
            emit(f"notes processed={result['processed']} failed={len(result['failed'])} "
                 f"remaining={result['remaining']} needs_attention={len(result['needs_attention'])}")
            if result['needs_attention']:
                emit(f"::warning::notes needs_attention={len(result['needs_attention'])}")
    if isinstance(result, dict) and (result.get('failed') or result.get('ready') is False
                                    or result.get('status') in ('verification_pending', 'failed')):
        return 1
    return 0


if __name__ == '__main__':
    from managed.run_log import public_run
    raise SystemExit(public_run(main, 'notes'))
