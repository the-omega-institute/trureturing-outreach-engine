"""Watch the public arXiv author record for the Outreach publication catalog.

The watch is deliberately small and read-only with respect to arXiv.  It
queries the public Atom API, filters the result by the exact two author names,
and appends only genuinely uncovered public preprints to
``tables/publications.json``.  The serialized Actions writer may then commit
the catalog and run the existing Pages synchronizer.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime
import html
import json
from pathlib import Path
import re
import subprocess
import time
import unicodedata
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET

from .queue import canonical

try:
    from scripts.short_notes import workflow as w
except ImportError:  # pragma: no cover - useful when invoked from a checkout
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from scripts.short_notes import workflow as w


from .location import DATA_ROOT as ROOT, settings
CATALOG = ROOT / 'tables/publications.json'
SUBMISSIONS = ROOT / 'tables/submissions.json'
SITE = 'https://www.trureturing.org'
API = 'https://export.arxiv.org/api/query'
USER_AGENT = 'trureturing-arxiv-watch/1.0'
MAX_RESULTS = 25
TIMEOUT = 20
_UNSET = object()


def _tag(element, name):
    """Return an Atom child without depending on its namespace prefix."""
    for child in element:
        if child.tag.rsplit('}', 1)[-1] == name:
            return child
    return None


def _value(element, name, default=''):
    child = _tag(element, name)
    return (child.text or '').strip() if child is not None else default


def _entry_id(value):
    match = re.search(r'/abs/([^?#]+)', value.strip())
    if not match:
        raise ValueError('arXiv entry has no abs identifier')
    token = match[1]
    match = re.fullmatch(r'(\d{4}\.\d{4,5})(?:v(\d+))?', token)
    if match:
        return match[1], int(match[2] or 1)
    match = re.fullmatch(r'([a-z-]+(?:\.[A-Z]{2})?/\d{7})(?:v(\d+))?', token)
    if not match:
        raise ValueError('Unsupported arXiv identifier: ' + token)
    return match[1], int(match[2] or 1)


def _date(value):
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00')).date()
    except (TypeError, ValueError) as error:
        raise ValueError('Invalid arXiv submission date') from error


def _plain_text(value):
    """Turn the short abstract excerpt into plain text without TeX markup."""
    value = html.unescape(' '.join((value or '').split()))
    value = re.sub(r'\\\((.*?)\\\)', r'\1', value)
    value = re.sub(r'\\\[(.*?)\\\]', r'\1', value)
    value = re.sub(r'\$(.*?)\$', r'\1', value)
    value = value.replace('$', '')
    # Keep the mathematical words/symbols while removing the TeX commands and
    # grouping braces.  The public field is a short description, not a TeX
    # renderer input.
    value = re.sub(r'\\([A-Za-z]+)', r'\1', value)
    value = value.replace('{', '').replace('}', '')
    return ' '.join(value.split()).strip()


def summary(abstract, limit=500):
    """Return the first one or two plain-text sentences of an abstract."""
    text = _plain_text(abstract)
    if not text:
        return 'Public arXiv preprint by ' + ' and '.join(settings('identity')['team_authors']) + '.'
    pieces = re.split(r'(?<=[.!?])\s+', text)
    result = ' '.join(piece for piece in pieces[:2] if piece).strip()
    if len(result) > limit:
        result = result[:limit].rstrip()
    return result


def _slug(title, arxiv_id):
    value = unicodedata.normalize('NFKD', title).encode('ascii', 'ignore').decode().lower()
    value = value.replace('&', ' and ')
    value = re.sub(r'[^a-z0-9]+', '-', value).strip('-')
    return value or 'arxiv-' + arxiv_id.replace('.', '-')


def parse_entries(xml_bytes):
    """Parse and de-duplicate API entries, retaining the newest version."""
    root = ET.fromstring(xml_bytes)
    grouped = {}
    for element in root:
        if element.tag.rsplit('}', 1)[-1] != 'entry':
            continue
        try:
            arxiv_id, version = _entry_id(_value(element, 'id'))
            authors = [_value(author, 'name') for author in element
                       if author.tag.rsplit('}', 1)[-1] == 'author']
            if not all(name in authors for name in settings('identity')['team_authors']):
                continue
            item = {
                'arxiv_id': arxiv_id,
                'version': version,
                'title': ' '.join(_value(element, 'title').split()),
                'authors': authors,
                'date': _date(_value(element, 'published')),
                'abstract': _value(element, 'summary'),
            }
        except ValueError:
            continue
        prior = grouped.get(arxiv_id)
        if prior is None or (item['version'], item['date']) > (prior['version'], prior['date']):
            grouped[arxiv_id] = item
    return sorted(grouped.values(), key=lambda item: (item['date'], item['arxiv_id']), reverse=True)


def _identity_from_arxiv_id(arxiv_id):
    base, _ = _entry_id('https://arxiv.org/abs/' + arxiv_id)
    return 'arxiv:' + base


def _identity(value):
    try:
        return w.publication_identity(value)
    except (TypeError, ValueError):
        return None


def _covered_identities(root, served_index):
    identities = set()
    catalog = json.loads((Path(root) / 'tables/publications.json').read_text())
    for paper in catalog.get('publications', []):
        identity = _identity(paper.get('url'))
        if identity:
            identities.add(identity)
    submissions = json.loads((Path(root) / 'tables/submissions.json').read_text())
    for row in submissions.get('papers', []):
        if row.get('arxiv_status') != 'public':
            continue
        identity = _identity(row.get('arxiv_url'))
        if identity is None and row.get('arxiv_id'):
            identity = _identity_from_arxiv_id(row['arxiv_id'])
        if identity:
            identities.add(identity)
    if served_index:
        for paper in served_index.get('publications', []):
            identity = _identity(paper.get('url'))
            if identity:
                identities.add(identity)
    return identities


def _request_bytes(url, *, accept, timeout=TIMEOUT, retries=1, opener=None):
    request = Request(url, headers={'User-Agent': USER_AGENT, 'Accept': accept})
    opener = opener or urlopen
    last = None
    for attempt in range(retries + 1):
        try:
            with opener(request, timeout=timeout) as response:
                return response.read()
        except (HTTPError, URLError, OSError, TimeoutError) as error:
            last = error
            if attempt < retries:
                time.sleep(0.1)
    raise last


def fetch_api(fetch=None):
    query = urlencode({
        'search_query': ' AND '.join(f'au:"{name}"' for name in settings('identity')['team_authors']),
        'sortBy': 'submittedDate',
        'sortOrder': 'descending',
        'max_results': str(MAX_RESULTS),
    })
    url = API + '?' + query
    return (fetch(url) if fetch else
            _request_bytes(url, accept='application/atom+xml'))


def fetch_served_index(fetch=None, site=SITE):
    target = site.rstrip('/') + '/assets/publication-index.json'
    raw = fetch(target) if fetch else _request_bytes(target, accept='application/json')
    index = raw if isinstance(raw, dict) else json.loads(raw.decode() if isinstance(raw, bytes) else raw)
    if (not isinstance(index, dict) or index.get('schema_version') != 'publication-index.v1'
            or not isinstance(index.get('publications'), list)):
        raise ValueError('Unexpected served publication index')
    return index


def _publication(item):
    version = 'v' + str(item['version'])
    evidence = 'https://arxiv.org/abs/' + item['arxiv_id'] + version
    paper = {
        'id': _slug(item['title'], item['arxiv_id']),
        'problem_ids': [],
        'topic': item['title'].lower(),
        'title': item['title'],
        'authors': ', '.join(item['authors']),
        'date': item['date'].isoformat(),
        'url': 'https://arxiv.org/abs/' + item['arxiv_id'],
        'venue': 'arXiv:' + item['arxiv_id'] + (version if item['version'] > 1 else ''),
        'status': 'preprint',
        'summary': summary(item['abstract']),
        'evidence_url': evidence,
    }
    w.validate_publication(paper)
    return paper


def watch(root=ROOT, api_fetch=None, served_index=_UNSET, index_fetch=None, site=SITE):
    """Return ``(report, updated_catalog)`` without writing any file."""
    root = Path(root)
    try:
        raw = fetch_api(api_fetch)
        entries = parse_entries(raw)
    except (HTTPError, URLError, OSError, TimeoutError, ET.ParseError, ValueError) as error:
        return {'status': 'arxiv-unavailable', 'new_papers_added': [],
                'error': type(error).__name__ + ': ' + str(error)[:240]}, None
    if served_index is _UNSET:
        try:
            served_index = fetch_served_index(index_fetch, site)
        except (HTTPError, URLError, OSError, TimeoutError, ValueError, json.JSONDecodeError) as error:
            return {'status': 'served-index-unavailable', 'new_papers_added': [],
                    'error': type(error).__name__ + ': ' + str(error)[:240]}, None
    catalog = json.loads((root / 'tables/publications.json').read_text())
    identities = _covered_identities(root, served_index)
    existing_ids = {p['id'] for p in catalog.get('publications', [])}
    additions, ignored = [], []
    today = date.today()
    for item in entries:
        identity = _identity_from_arxiv_id(item['arxiv_id'])
        if identity in identities:
            ignored.append({'arxiv_id': item['arxiv_id'], 'reason': 'already-covered'})
            continue
        if item['date'] > today:
            ignored.append({'arxiv_id': item['arxiv_id'], 'reason': 'future-date'})
            continue
        paper = _publication(item)
        if paper['id'] in existing_ids:
            # A title-derived ID collision must not overwrite a different paper.
            paper['id'] = paper['id'] + '-' + item['arxiv_id'].replace('.', '-')
            w.validate_publication(paper)
        additions.append((paper, item['arxiv_id']))
        identities.add(identity)
        existing_ids.add(paper['id'])
    updated = dict(catalog)
    updated['publications'] = catalog.get('publications', []) + [paper for paper, _ in additions]
    updated['publications'].sort(key=lambda paper: paper['id'])
    report = {
        'status': 'updated' if additions else 'idle',
        'queried': len(entries),
        'new_papers_added': [{'id': paper['id'], 'arxiv_id': arxiv_id, 'title': paper['title']}
                             for paper, arxiv_id in additions],
        'ignored': ignored,
    }
    return report, updated


def write_catalog(root, catalog):
    path = Path(root) / 'tables/publications.json'
    # Keep the maintained file's readable layout (indented, sorted keys).
    path.write_text(json.dumps(catalog, ensure_ascii=False, indent=2, sort_keys=True) + '\n')


def commit_catalog(root=ROOT):
    """Commit and push only the maintained publication catalog."""
    from .data_writes import commit_paths, push

    if not commit_paths(('tables/publications.json',), 'Watch public arXiv author publications', root):
        return {'committed': False, 'reason': 'unchanged'}
    push(root)
    return {'committed': True}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--output', type=Path, help='Save the JSON report as well as printing it')
    parser.add_argument('--apply', action='store_true', help='Write newly discovered catalog publications')
    parser.add_argument('--push', action='store_true', help='Commit and push the catalog (Actions only)')
    args = parser.parse_args(argv)
    report, catalog = watch(args.root)
    if args.apply and catalog is not None and report['new_papers_added']:
        write_catalog(args.root, catalog)
        if args.push:
            if __import__('os').environ.get('GITHUB_ACTIONS') != 'true':
                raise ValueError('Catalog push is restricted to the serialized GitHub Actions writer')
            report['commit'] = commit_catalog(args.root)
    raw = canonical(report)
    if args.output:
        args.output.write_text(raw)
    print(raw, end='')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
