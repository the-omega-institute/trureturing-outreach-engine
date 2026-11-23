#!/usr/bin/env python3
"""Core file, GitHub, Zenodo and Pages adapters used by service.py."""
from __future__ import annotations

import base64
import copy
from datetime import date, datetime, timezone
import hashlib
import html
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
import sys
import tempfile
from urllib.error import HTTPError
from urllib.parse import quote, unquote, urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener
import zipfile

if str(Path(__file__).resolve().parents[2]) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from managed.location import DATA_BRANCH, DATA_REPO as OUTREACH, settings
PAGES = 'the-omega-institute/trureturing-pages'
SLUG = re.compile(r'[a-z0-9]+(?:-[a-z0-9]+)*')
PUBLICATION_FIELDS = {'id', 'problem_ids', 'topic', 'title', 'authors', 'date',
                      'url', 'venue', 'status', 'summary', 'evidence_url'}
METADATA_FIELDS = {'id', 'revision', 'problem_ids', 'topic', 'title', 'creators',
                   'abstract', 'summary', 'license', 'evidence_url'}
MAX_FILE = 30 * 1024 * 1024
MAX_FEED_BYTES = 8 * 1024 * 1024
MAX_INDEX_BYTES = 16 * 1024 * 1024
INITIAL_NOTES = 'Versioned short-note artifacts and publication receipts.'


def now():
    return datetime.now(timezone.utc).isoformat()


def encode(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + '\n').encode()


def read(path):
    return json.loads(Path(path).read_text())


def exact(value, required, optional=()):
    if not isinstance(value, dict) or set(value) - set(required) - set(optional) or set(required) - set(value):
        raise ValueError('Unexpected or missing fields: expected ' + ', '.join(sorted(required)))


def text(value):
    if not isinstance(value, str) or not value.strip():
        raise ValueError('Expected nonempty text')


def person_name(name):
    """'Mercer, Blaire' and 'Blaire Mercer' name the same person."""
    if ',' in name:
        family, given = name.split(',', 1)
        name = given + ' ' + family
    return ' '.join(name.split()).casefold()


def same_collection_authors(expected, observed):
    """Compare ordered bylines using the two supported name formats."""
    return (isinstance(observed, list) and len(expected) == len(observed)
            and all(isinstance(public, dict) and isinstance(public.get('name'), str)
                    and {**public, 'name': person_name(public['name'])}
                    == {**selected, 'name': person_name(selected['name'])}
                    for selected, public in zip(expected, observed)))


def slug(value):
    if not isinstance(value, str) or not SLUG.fullmatch(value):
        raise ValueError('Expected a stable lowercase problem/note ID')
    return value


def url(value):
    text(value)
    if re.search(r'[\s\x00-\x1f\x7f<>"\\]|%(?![0-9a-fA-F]{2})', value):
        raise ValueError('Expected a public HTTPS URL without whitespace or controls')
    parts = urlsplit(value)
    if parts.scheme != 'https' or not parts.hostname or parts.username or parts.password:
        raise ValueError('Expected a public HTTPS URL without credentials')
    parts.port
    if ':' not in parts.hostname:
        host = parts.hostname.encode('idna').decode('ascii').rstrip('.')
        if len(host) > 253 or any(not re.fullmatch(r'[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?', label)
                                for label in host.split('.')):
            raise ValueError('Invalid public HTTPS hostname')
    return value


def problem_ids(value):
    if not isinstance(value, list) or len(set(value)) != len(value):
        raise ValueError('Expected distinct explicit problem IDs')
    for item in value:
        slug(item)


def validate_metadata(note):
    exact(note, METADATA_FIELDS)
    slug(note['id'])
    if type(note['revision']) is not int or note['revision'] < 1:
        raise ValueError('Revision must be a positive integer')
    problem_ids(note['problem_ids'])
    if not note['problem_ids']:
        raise ValueError('A conjecture note needs an explicit problem ID')
    for key in ('topic', 'title', 'abstract', 'summary', 'license'):
        text(note[key])
    url(note['evidence_url'])
    if not isinstance(note['creators'], list) or not note['creators']:
        raise ValueError('Provide the actual manuscript authors')
    for creator in note['creators']:
        exact(creator, {'name'}, {'affiliation', 'orcid'})
        for value in creator.values():
            text(value)
    return note


def publication_identity(value):
    # Match Pages' existing canonical arXiv/DOI aliases without a runtime
    # dependency on its repository or its Markdown renderer.
    parts = urlsplit(url(value))
    if parts.hostname in ('doi.org', 'dx.doi.org'):
        doi = unquote(parts.path.lstrip('/')).lower()
        if not re.fullmatch(r'10\.\d{4,9}/[^\s<>"?#]+', doi):
            raise ValueError('Invalid DOI identifier')
        arxiv = re.fullmatch(r'10\.48550/arxiv\.(\d{4}\.\d{4,5})(?:v\d+)?', doi)
        return 'arxiv:' + arxiv[1] if arxiv else 'doi:' + doi
    if parts.hostname in ('arxiv.org', 'www.arxiv.org'):
        match = re.fullmatch(r'/(?:abs|html|pdf)/(\d{4}\.\d{4,5}|[a-z-]+(?:\.[A-Z]{2})?/\d{7})(?:v\d+)?(?:\.pdf)?/?', parts.path)
        if match:
            return 'arxiv:' + match[1]
    return 'url:' + urlunsplit((parts.scheme, parts.netloc.lower(), parts.path, parts.query, ''))


def validate_publication(paper):
    exact(paper, PUBLICATION_FIELDS)
    slug(paper['id'])
    problem_ids(paper['problem_ids'])
    for key in ('topic', 'title', 'authors', 'venue', 'summary'):
        text(paper[key])
    if date.fromisoformat(paper['date']) > date.today():
        raise ValueError('A public paper cannot have a future publication date')
    if paper['status'] not in ('preprint', 'accepted', 'published'):
        raise ValueError('Only an actual public preprint, accepted paper or publication belongs in the feed')
    publication_identity(paper['url'])
    url(paper['evidence_url'])
    return paper


def validate_public_note(note):
    exact(note, {'id', 'problem_ids', 'title', 'revision', 'evidence_url'},
          {'doi', 'pdf_url', 'source_url', 'mathesis_url'})
    slug(note['id'])
    problem_ids(note['problem_ids'])
    text(note['title'])
    url(note['evidence_url'])
    if not note['problem_ids'] or type(note['revision']) is not int or note['revision'] < 1:
        raise ValueError('A public note needs explicit problems and a positive revision')
    if not note.get('doi') and not note.get('mathesis_url'):
        raise ValueError('A staged or DOI-reserved draft is not a public note')
    if 'doi' in note and (not isinstance(note['doi'], str) or not re.fullmatch(r'https://doi\.org/10\.5281/zenodo\.\d+', note['doi'])):
        raise ValueError('Use the published production Zenodo DOI')
    for key in ('pdf_url', 'source_url'):
        if key in note:
            parts = urlsplit(url(note[key]))
            if parts.netloc != 'zenodo.org' or not re.fullmatch(r'/records/\d+/files/[^/]+', parts.path) or parts.query or parts.fragment:
                raise ValueError('Use the published Zenodo file URL')
    if 'mathesis_url' in note:
        parts = urlsplit(url(note['mathesis_url']))
        venue = bool(re.fullmatch(r'/papers/WP-\d{4}-\d{4,}', parts.path)) and not parts.query
        collected = (parts.path == '/papers/' + note['id']
                     and bool(re.fullmatch(r'version=[1-9]\d*', parts.query))
                     and int(parts.query[8:]) <= 2**32 - 1)
        if (parts.netloc != urlsplit(settings('mathesis')['base_url']).netloc
                or not (venue or collected) or parts.fragment):
            raise ValueError('Use a public Mathesis paper page, never a private submission')
    return note


def validate_feed(feed):
    exact(feed, {'schema_version', 'publications', 'collaborations'}, {'updated_at', 'notes'})
    if feed['schema_version'] != 'research-updates.v1':
        raise ValueError('Unsupported Pages feed')
    if 'updated_at' in feed and datetime.fromisoformat(feed['updated_at'].replace('Z', '+00:00')).tzinfo is None:
        raise ValueError('Feed timestamp needs a timezone')
    ids, identities = set(), set()
    for kind in ('publications', 'collaborations'):
        if not isinstance(feed[kind], list):
            raise ValueError('Feed entries must be arrays')
        for item in feed[kind]:
            if kind == 'publications':
                validate_publication(item)
                identity = publication_identity(item['url'])
                if identity in identities:
                    raise ValueError('Duplicate publication URL')
                identities.add(identity)
            else:
                exact(item, {'id', 'problem_ids', 'topic', 'status', 'people', 'evidence_url'}, {'summary', 'date'})
                slug(item['id'])
                problem_ids(item['problem_ids'])
                text(item['topic'])
                url(item['evidence_url'])
                if item['status'] not in ('confirmed_active', 'research_exchange') or not isinstance(item['people'], list) or not item['people'] or not item['problem_ids']:
                    raise ValueError('Invalid selected public research relationship')
                if 'summary' in item:
                    text(item['summary'])
                    if len(item['summary']) > 280:
                        raise ValueError('Keep the public research update brief')
                if 'date' in item and date.fromisoformat(item['date']) > date.today():
                    raise ValueError('A research update cannot have a future date')
                for person in item['people']:
                    exact(person, {'name', 'institution'})
                    text(person['name'])
                    text(person['institution'])
            if item['id'] in ids:
                raise ValueError('Duplicate public identity')
            ids.add(item['id'])
    note_ids = set()
    if not isinstance(feed.get('notes', []), list):
        raise ValueError('Public notes must be an array')
    for note in feed.get('notes', []):
        validate_public_note(note)
        if note['id'] in note_ids:
            raise ValueError('Duplicate public note identity')
        note_ids.add(note['id'])
    return feed


def replace_publication(feed, paper):
    """The stable ID survives a new version or a later paper with another DOI."""
    validate_feed(feed)
    validate_publication(paper)
    result = copy.deepcopy(feed)
    result['publications'] = [p for p in result['publications'] if p['id'] != paper['id']] + [paper]
    result['publications'].sort(key=lambda p: p['id'])
    if result != feed or not result.get('updated_at'):
        result['updated_at'] = now()
    validate_feed(result)
    if len(encode(result)) > MAX_FEED_BYTES:
        raise ValueError('Pages feed exceeds its 8 MiB limit')
    return result


def validate_source(path):
    """Check the selected source ZIP; never copy an entire mail/project folder."""
    with zipfile.ZipFile(path) as archive:
        names, tex, size = set(), False, 0
        for entry in archive.infolist():
            name = PurePosixPath(entry.filename)
            if (name.is_absolute() or '..' in name.parts or '\\' in entry.filename
                    or entry.filename in names or stat.S_ISLNK(entry.external_attr >> 16)):
                raise ValueError('Unsafe or duplicate source archive path')
            names.add(entry.filename)
            if any(part.lower() in ('.git', '.lake', '__pycache__', 'correspondence', 'mail') for part in name.parts):
                raise ValueError('Source archive contains private mail or build state')
            if name.suffix.lower() in ('.eml', '.emlx', '.mbox', '.log', '.aux', '.bin', '.npz', '.pkl'):
                raise ValueError('Source archive contains mail, logs or binary working data')
            if name.name.lower() in ('outreach.json', 'outreach-polished.json', 'review-polished.json', 'body.txt', 'meta.json', '.env'):
                raise ValueError('Source archive contains internal correspondence records')
            size += entry.file_size
            tex |= name.suffix == '.tex'
        if not tex or size > 64 * 1024 * 1024:
            raise ValueError('Source archive needs LaTeX and must expand to at most 64 MiB')
        if archive.testzip() is not None:
            raise ValueError('Corrupt source archive')


def prepare(metadata, pdf, source, output):
    note = validate_metadata(read(metadata))
    pdf, source, output = Path(pdf), Path(source), Path(output)
    for path in (pdf, source):
        if not path.is_file() or not 0 < path.stat().st_size <= MAX_FILE:
            raise ValueError('Each selected file must exist and be at most 30 MiB')
    with pdf.open('rb') as stream:
        if stream.read(5) != b'%PDF-':
            raise ValueError('The note must be a compiled PDF')
    validate_source(source)
    output.mkdir(parents=True, exist_ok=True)
    if list(output.iterdir()):
        raise ValueError('Use an empty output directory for this immutable package')
    version = note['revision']
    artifacts = {}
    for name, path in ((f'note-v{version}.pdf', pdf), (f'source-v{version}.zip', source)):
        shutil.copyfile(path, output / name)
        artifacts[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    package = {'schema_version': 'short-note.v1', 'metadata': note, 'files': artifacts}
    (output / f'note-v{version}.json').write_bytes(encode(package))
    return output


class GitHub:
    def __init__(self, repo, token=None):
        self.repo, self.token = repo, token

    def run(self, *args, allow_missing=False):
        env = dict(os.environ)
        if self.token:
            env['GH_TOKEN'] = self.token
        result = subprocess.run(['gh', *args], capture_output=True, env=env)
        if result.returncode:
            error = result.stderr.decode(errors='replace')
            if allow_missing and '(HTTP 404)' in error:
                return None
            raise RuntimeError('GitHub operation failed: ' + error.strip())
        return result.stdout

    def release(self, tag):
        raw = self.run('api', f'repos/{self.repo}/releases/tags/{tag}', allow_missing=True)
        return json.loads(raw) if raw is not None else None

    def list_releases(self):
        pages = json.loads(self.run('api', '--paginate', '--slurp', f'repos/{self.repo}/releases?per_page=100'))
        return [release for page in pages for release in page]

    def download(self, tag, name, directory):
        self.run('release', 'download', tag, '--repo', self.repo, '--pattern', name, '--dir', str(directory))
        return Path(directory) / name

    def upload(self, tag, path, replace=False):
        args = ['release', 'upload', tag, str(path), '--repo', self.repo]
        if replace:
            args.append('--clobber')
        self.run(*args)

    def create(self, tag, title):
        self.run('release', 'create', tag, '--repo', self.repo, '--title', title,
                 '--notes', INITIAL_NOTES, '--target', DATA_BRANCH, '--latest=false')

    def save_state(self, tag, state):
        # Editing the release body is one atomic API operation. Replacing a
        # state.json asset would delete the only receipt before uploading it.
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'state.json'
            path.write_bytes(encode(state))
            if path.stat().st_size > 50000:
                raise ValueError('Receipt exceeds the release description size budget')
            self.run('release', 'edit', tag, '--repo', self.repo, '--notes-file', str(path))

    def delete_asset(self, tag, name):
        self.run('release', 'delete-asset', tag, name, '--repo', self.repo, '--yes')


def tag_for(note_id):
    return 'short-note-' + slug(note_id)


def verify_package(package, directory):
    exact(package, {'schema_version', 'metadata', 'files'})
    if package['schema_version'] != 'short-note.v1':
        raise ValueError('Unsupported short-note package')
    note = validate_metadata(package['metadata'])
    expected = {f"note-v{note['revision']}.pdf", f"source-v{note['revision']}.zip"}
    if set(package['files']) != expected:
        raise ValueError('A package contains exactly its selected PDF and source ZIP')
    for name, digest in package['files'].items():
        path = Path(directory) / name
        if not path.is_file() or not 0 < path.stat().st_size <= MAX_FILE or hashlib.sha256(path.read_bytes()).hexdigest() != digest:
            raise ValueError('Package artifact changed: ' + name)
    validate_source(Path(directory) / f"source-v{note['revision']}.zip")
    return note


def stage(directory, gh):
    directory = Path(directory)
    manifests = list(directory.glob('note-v*.json'))
    if len(manifests) != 1:
        raise ValueError('Stage one prepared version at a time')
    package = read(manifests[0])
    note = verify_package(package, directory)
    if manifests[0].name != f"note-v{note['revision']}.json":
        raise ValueError('Manifest filename must match the note revision')
    tag = tag_for(note['id'])
    release = gh.release(tag)
    if release is None:
        gh.create(tag, note['title'])
        release = gh.release(tag)
    existing = {a['name'] for a in release['assets']}
    for path in [directory / name for name in package['files']] + manifests:
        if path.name in existing:
            with tempfile.TemporaryDirectory() as temp:
                if gh.download(tag, path.name, temp).read_bytes() != path.read_bytes():
                    raise ValueError('Staged versions are immutable; increment revision')
        else:
            gh.upload(tag, path)
    return {'tag': tag, 'note_id': note['id'], 'revision': note['revision']}


class ZenodoRedirectError(ValueError):
    pass


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ZenodoRedirectError('Refusing to forward Zenodo credentials through a redirect')


class ZenodoHTTPError(RuntimeError):
    def __init__(self, method, code):
        self.code = code
        super().__init__(f'Zenodo {method} returned HTTP {code}; retained the receipt for recovery')


class Zenodo:
    def __init__(self, token, base='https://zenodo.org/api'):
        if base not in ('https://zenodo.org/api', 'https://sandbox.zenodo.org/api'):
            raise ValueError('Use the production or sandbox Zenodo API')
        token = token.strip()
        if not token or not token.isascii() or any(ord(c) < 33 or ord(c) == 127 for c in token):
            raise ValueError('Zenodo token must be a single ASCII token without embedded whitespace')
        self.token, self.base = token, base

    def request(self, method, path, data=None):
        target = path if path.startswith('https:') else self.base + path
        if not target.startswith(self.base + '/'):
            raise ValueError('Zenodo returned a URL outside its API')
        headers = {'Authorization': 'Bearer ' + self.token, 'Accept': 'application/json'}
        if isinstance(data, (dict, list)):
            data = encode(data)
            headers['Content-Type'] = 'application/json'
        elif data is not None:
            headers['Content-Type'] = 'application/octet-stream'
        request = Request(target, method=method, data=data, headers=headers)
        try:
            with build_opener(NoRedirect).open(request, timeout=90) as response:
                raw = response.read()
        except HTTPError as error:
            raise ZenodoHTTPError(method, error.code) from None
        return json.loads(raw) if raw else None


def zenodo_name(name):
    """Keep explicit family names; otherwise use the final name as the family."""
    parts = name.split()
    return name if ',' in name or len(parts) < 2 else parts[-1] + ', ' + ' '.join(parts[:-1])


def zenodo_metadata(note):
    return {'title': note['title'], 'upload_type': 'publication', 'publication_type': 'preprint',
            'creators': [{**c, 'name': zenodo_name(c['name'])} for c in note['creators']],
            'description': html.escape(note['abstract']),
            'access_right': 'open', 'license': note['license'], 'version': str(note['revision']),
            'publication_date': date.today().isoformat(),
            'keywords': ['trureturing', 'short note', 'trureturing-note:' + note['id']],
            'related_identifiers': [{'relation': 'isDerivedFrom', 'identifier': note['evidence_url']}],
            'prereserve_doi': True}


def check_remote_files(record, package, directory):
    remote = {f['filename']: f for f in record.get('files', [])}
    if set(remote) != set(package['files']):
        raise ValueError('Zenodo file inventory differs from this note version')
    for name in package['files']:
        checksum = hashlib.md5((Path(directory) / name).read_bytes()).hexdigest()
        if remote[name].get('checksum', '').removeprefix('md5:') != checksum:
            raise ValueError('Zenodo uploaded file differs: ' + name)


def check_remote_metadata(record, note):
    actual = record['metadata']
    creators = actual.get('creators', [])
    creators_match = (len(creators) == len(note['creators'])
                      and all(all((zenodo_name(found.get(k, '')) == zenodo_name(v) if k == 'name'
                                   else found.get(k) == v) for k, v in expected.items())
                              for found, expected in zip(creators, note['creators'])))
    if (actual.get('version') != str(note['revision']) or actual.get('title') != note['title']
            or not creators_match or actual.get('upload_type') != 'publication'
            or actual.get('publication_type') != 'preprint'
            or actual.get('access_right') != 'open' or actual.get('license') != note['license']):
        raise ValueError('Zenodo metadata differs from this note')


def publish(package, directory, state, save, zenodo, recover_record=None, reuse_draft_from=None, reserve_only=False):
    """Save remote identity before upload/publish; never blindly repeat creation."""
    note = verify_package(package, directory)
    if state is None:
        state = {'schema_version': 'short-note-state.v1', 'id': note['id'],
                 'zenodo_api': zenodo.base, 'versions': {}, 'preferred': 'note'}
    if state['id'] != note['id'] or state['zenodo_api'] != zenodo.base:
        raise ValueError('Note identity or production/sandbox destination changed')
    revision = str(note['revision'])
    package_id = hashlib.sha256(encode(package)).hexdigest()
    entry = state['versions'].get(revision)
    if entry and entry['package_id'] != package_id:
        raise ValueError('A note revision is immutable; prepare a new revision')
    if entry and entry.get('publication'):
        return state
    published = [int(v) for v, value in state['versions'].items() if value.get('publication')]
    previous = state['versions'][str(max(published))] if published else None
    if any(int(v) > note['revision'] for v in state['versions']):
        raise ValueError('Cannot publish an older revision after a newer one')
    metadata = zenodo_metadata(note)
    if entry is None or entry['phase'] == 'creation_rejected':
        if reuse_draft_from is not None and (type(reuse_draft_from) is not int or not 0 < reuse_draft_from < note['revision']):
            raise ValueError('Reuse must name an earlier unpublished artifact revision')
        for number, pending in state['versions'].items():
            if (number != revision and not pending.get('publication')
                    and pending.get('phase') not in ('superseded', 'creation_rejected')
                    and number != str(reuse_draft_from)):
                raise ValueError('Resolve the existing unpublished draft or explicitly reuse it before another revision')
        entry = {'package_id': package_id, 'phase': 'creating', 'requested_at': now()}
        if reuse_draft_from is not None:
            old = state['versions'].get(str(reuse_draft_from))
            if not old or old.get('publication') or old.get('phase') == 'superseded':
                raise ValueError('Reuse must select an existing unpublished draft revision')
            record_id = old.get('record_id') or recover_record
            if not record_id:
                raise ValueError('Recover the uncertain draft ID before reusing it')
            record = zenodo.request('GET', f'/deposit/depositions/{int(record_id)}')
            if record.get('submitted'):
                raise ValueError('This draft was already published; recover its receipt before revising')
            if 'trureturing-note:' + note['id'] not in record.get('metadata', {}).get('keywords', []):
                raise ValueError('Draft reuse must retain the same note identity')
            if previous:
                prior = zenodo.request('GET', f"/deposit/depositions/{previous['record_id']}")
                draft = zenodo.request('GET', prior['links']['latest_draft'])
                if str(draft['id']) != str(record['id']):
                    raise ValueError('Draft reuse must retain the existing Zenodo concept')
            elif not old.get('record_id') and record['metadata'].get('version') != str(reuse_draft_from):
                raise ValueError('Recovered draft is not the requested original revision')
            old.update(phase='superseded', superseded_by=note['revision'], record_id=record['id'])
            entry.update(phase='uploading', record_id=record['id'], reused_draft_from=reuse_draft_from)
        state['versions'][revision] = entry
        save(state)
        if not entry.get('record_id'):
            try:
                response = (zenodo.request('POST', f"/deposit/depositions/{previous['record_id']}/actions/newversion")
                            if previous else zenodo.request('POST', '/deposit/depositions', {'metadata': metadata}))
            except ZenodoHTTPError as error:
                if error.code in (400, 401, 403, 404, 422):
                    entry.update(phase='creation_rejected', response_code=error.code)
                    save(state)
                raise
            record = zenodo.request('GET', response['links']['latest_draft']) if previous else response
            entry['record_id'] = record['id']
            entry['phase'] = 'uploading'
            save(state)
    elif not entry.get('record_id'):
        if recover_record is None:
            raise RuntimeError('Uncertain Zenodo creation: inspect the account and rerun with --recover-record ID; no second deposit was created')
        record = zenodo.request('GET', f'/deposit/depositions/{int(recover_record)}')
        marker = 'trureturing-note:' + note['id']
        if (marker not in record.get('metadata', {}).get('keywords', [])
                or (not previous and record.get('metadata', {}).get('version') != revision)):
            raise ValueError('Recovery record is not this note/version')
        if previous and str(record['id']) == str(previous['record_id']):
            raise ValueError('Recovery must name the new draft, not the old published version')
        if previous:
            prior_record = zenodo.request('GET', f"/deposit/depositions/{previous['record_id']}")
            draft = zenodo.request('GET', prior_record['links']['latest_draft'])
            if str(draft['id']) != str(record['id']):
                raise ValueError('Recovery must use the existing concept\'s latest draft')
        entry['record_id'], entry['phase'] = record['id'], 'uploading'
        save(state)
    record = zenodo.request('GET', f"/deposit/depositions/{entry['record_id']}")
    if not record.get('submitted'):
        record = zenodo.request('PUT', f"/deposit/depositions/{entry['record_id']}", {'metadata': metadata})
        # A new Zenodo version inherits old files. Replace its draft file set only.
        for old in record.get('files', []):
            zenodo.request('DELETE', f"/deposit/depositions/{entry['record_id']}/files/{quote(str(old['id']), safe='')}")
        bucket = record['links']['bucket']
        for name in package['files']:
            zenodo.request('PUT', bucket + '/' + quote(name), (Path(directory) / name).read_bytes())
        record = zenodo.request('GET', f"/deposit/depositions/{entry['record_id']}")
        check_remote_files(record, package, directory)
        check_remote_metadata(record, note)
        reserved = record.get('metadata', {}).get('prereserve_doi', {})
        if reserve_only:
            if not isinstance(reserved, dict) or not reserved.get('doi'):
                raise ValueError('Zenodo has not returned a reserved DOI')
            entry.update(phase='doi-reserved', reserved_doi=reserved['doi'], reserved_at=now())
            save(state)
            return state
        entry['phase'] = 'publishing'
        save(state)
        zenodo.request('POST', f"/deposit/depositions/{entry['record_id']}/actions/publish")
        record = zenodo.request('GET', f"/deposit/depositions/{entry['record_id']}")
    check_remote_files(record, package, directory)
    if not record.get('submitted') or not record.get('doi'):
        raise ValueError('Zenodo has not returned a published DOI')
    check_remote_metadata(record, note)
    paper = {'id': note['id'], 'problem_ids': note['problem_ids'], 'topic': note['topic'],
             'title': note['title'], 'authors': ', '.join(c['name'] for c in note['creators']),
             'date': record['metadata']['publication_date'], 'url': 'https://doi.org/' + record['doi'],
             'venue': 'Zenodo', 'status': 'preprint', 'summary': note['summary'],
             'evidence_url': note['evidence_url']}
    validate_publication(paper)
    entry.update(phase='published', doi=record['doi'], concept_doi=record.get('conceptdoi'),
                 publication=paper, completed_at=now())
    if state['preferred'] == 'note':
        state['publication'] = paper
    save(state)
    return state


def promote(state, paper):
    validate_publication(paper)
    if state is None or paper['id'] != state['id'] or not state.get('publication'):
        raise ValueError('A later paper must replace an existing stable note identity')
    if state['publication'] == paper and state['preferred'] == 'paper':
        return state
    state = copy.deepcopy(state)
    state.setdefault('previous_publications', []).append(state['publication'])
    state.update(preferred='paper', publication=paper)
    return state


def pages_feed(gh, directory):
    """Read the last recoverable public feed without changing remote state."""
    release = gh.release('research-updates')
    names = {a['name'] for a in release['assets']} if release else set()
    filename = ('research-updates.pending.json' if 'research-updates.pending.json' in names
                else 'research-updates.v1.json')
    if filename in names:
        feed = read(gh.download('research-updates', filename, directory))
    elif not names:
        raw = gh.run('api', f'repos/{PAGES}/contents/site/assets/research-updates.json?ref=dev')
        feed = json.loads(base64.b64decode(json.loads(raw)['content']))
    else:
        raise ValueError('Public release has no feed or recovery asset; inspect it before replacing content')
    validate_feed(feed)
    return feed, release, names


def pages_authored_publications(gh):
    """Read the independent paper catalog that Pages merges with our feed."""
    raw = gh.run('api', f'repos/{PAGES}/contents/site/assets/research-news.json?ref=dev')
    catalog = json.loads(base64.b64decode(json.loads(raw)['content']))
    if not isinstance(catalog, dict) or not isinstance(catalog.get('publications'), list):
        raise ValueError('Unexpected Pages authored publication catalog')
    return catalog['publications']


def is_pages_publication(paper):
    """Research shows arXiv or formal acceptance/publication, never Zenodo."""
    parts = urlsplit(url(paper['url']))
    identity = publication_identity(paper['url'])
    if (parts.hostname in ('zenodo.org', 'www.zenodo.org')
            or identity.startswith('doi:10.5281/zenodo.')):
        return False
    return (identity.startswith('arxiv:') or paper.get('status', '').lower().split(' / ')[0]
            in ('accepted', 'accepted poster', 'published'))


def pages_note(note):
    """Keep the verified Mathesis reading entry; Mathesis displays the DOI."""
    if not note or not note.get('mathesis_url'):
        return None
    return {key: copy.deepcopy(note[key]) for key in
            ('id', 'problem_ids', 'title', 'revision', 'evidence_url', 'mathesis_url')}


def project_pages_feed(feed):
    """Destination-specific view; do not alter the full scientific handoff."""
    validate_feed(feed)
    return {**copy.deepcopy(feed),
            'publications': [copy.deepcopy(p) for p in feed['publications'] if is_pages_publication(p)],
            'notes': [pages_note(n) for n in feed.get('notes', []) if n.get('mathesis_url')]}


def deliver_feed(replacement, gh):
    """Publish a complete, validated projection with a recoverable asset swap."""
    validate_feed(replacement)
    if not replacement.get('updated_at') or len(encode(replacement)) > MAX_FEED_BYTES:
        raise ValueError('Released feed needs a timestamp and must fit in 8 MiB')
    replacement = project_pages_feed(replacement)
    with tempfile.TemporaryDirectory() as temp:
        feed, release, names = pages_feed(gh, temp)
        asset, pending = 'research-updates.v1.json', 'research-updates.pending.json'
        def dispatch():
            gh.run('api', f'repos/{PAGES}/dispatches', '--method', 'POST',
                   '-f', 'event_type=research-updates')
        if pending in names:
            current = Path(temp) / asset
            current.write_bytes(encode(feed))
            gh.upload('research-updates', current, replace=True)
            dispatch()
            gh.delete_asset('research-updates', pending)
        replacement = copy.deepcopy(replacement)
        if all(feed.get(k, []) == replacement.get(k, []) for k in ('publications', 'collaborations', 'notes')):
            replacement = feed if feed.get('updated_at') else replacement
            if pending in names or asset in names:
                if pending not in names:
                    dispatch()
                return {'status': 'dispatch_accepted', 'feed_updated_at': replacement['updated_at'],
                        'dispatch_accepted_at': now()}
        path = Path(temp) / asset
        path.write_bytes(encode(replacement))
        if release is None:
            gh.create('research-updates', 'Public research updates')
        pending_path = Path(temp) / pending
        pending_path.write_bytes(path.read_bytes())
        gh.upload('research-updates', pending_path)
        gh.upload('research-updates', path, replace=True)
        dispatch()
        # This only acknowledges dispatch. A later build may fail; callers
        # must read the served publication index before considering it done.
        gh.delete_asset('research-updates', pending)
        return {'status': 'dispatch_accepted', 'feed_updated_at': replacement['updated_at'],
                'dispatch_accepted_at': now()}


def sync_pages(state, gh):
    if state['zenodo_api'] != 'https://zenodo.org/api':
        raise ValueError('Sandbox output must never enter the public Pages feed')
    with tempfile.TemporaryDirectory() as temp:
        feed, _, _ = pages_feed(gh, temp)
    return deliver_feed(replace_publication(feed, state['publication']), gh)


def release_state(release, note_id):
    body = (release.get('body') or '').strip()
    if body == INITIAL_NOTES:
        return None
    state = json.loads(body)
    if (not isinstance(state, dict) or state.get('schema_version') != 'short-note-state.v1'
            or state.get('id') != note_id or not isinstance(state.get('versions'), dict)):
        raise ValueError('Private release receipt identity mismatch')
    return state


def operate(action, note_id, revision, gh, zenodo=None, paper=None, recover_record=None, pages=None, reuse_draft_from=None):
    tag = tag_for(note_id)
    release = gh.release(tag)
    if release is None:
        raise ValueError('Stage the existing short note before publishing it')
    with tempfile.TemporaryDirectory() as temp:
        state = release_state(release, note_id)

        def save(value):
            gh.save_state(tag, value)

        if action in ('publish', 'reserve'):
            if revision is None or revision < 1:
                raise ValueError('Select a positive note revision')
            package = read(gh.download(tag, f'note-v{revision}.json', temp))
            if package['metadata']['id'] != note_id or package['metadata']['revision'] != revision:
                raise ValueError('Release/package identity mismatch')
            for name in package['files']:
                if name != Path(name).name or name not in (f'note-v{revision}.pdf', f'source-v{revision}.zip'):
                    raise ValueError('Invalid package asset name')
                gh.download(tag, name, temp)
            state = publish(package, temp, state, save, zenodo, recover_record, reuse_draft_from,
                            reserve_only=action == 'reserve')
            if action == 'reserve':
                return state
        elif action == 'promote':
            state = promote(state, paper)
            save(state)
        if state is None or not state.get('publication'):
            raise ValueError('There is no published note/paper to synchronize')
        if pages is not None:
            sync_pages(state, pages)
        return state
