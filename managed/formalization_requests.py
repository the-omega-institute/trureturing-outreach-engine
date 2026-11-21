"""File reviewed scientific requests from the serialized coordinator."""
import hashlib
import hmac
import os
import re
from urllib.error import HTTPError

from . import note_closure, recheck
from .queue import canonical, digest, require
from .tasks import review_current
from .tick import unit, units

REPO = 'the-omega-institute/trureturing-requests'
ISSUES = 'repos/' + REPO + '/issues'
BACKFILL_PREFIX = 'formalization-backfill'
PENDING = 'pending'
TITLE = re.compile(r'^\[Formalization\]\s*(.*?)\s*\|\s*(DOI:\s*[^|]+|No DOI)\s*\|', re.I)
ISSUE_URL = re.compile(r'https://github\.com/' + REPO + r'/issues/[1-9]\d*')


def compact(text):
    return ' '.join(text.split())


def coordinates(gap):
    values = (('doi', note_closure.doi(gap['source_doi'])),
              ('arxiv', note_closure.arxiv(gap['source_arxiv'])),
              ('url', (gap['source_url'] or '').split('#', 1)[0].rstrip('/')))
    return [(kind, value) for kind, value in values if value]


def pending_requests(result):
    gaps = result.get('formalization_gaps', [])
    note_closure.validate_gaps(gaps)
    requests = {}
    for gap in gaps:
        key = digest([compact(gap['statement']), coordinates(gap)[0]])
        requests.setdefault(key, {'key': key, 'state': PENDING, 'gap': gap})
    return list(requests.values())


def issue_payload(item):
    gap = item['gap']
    statement = compact(gap['statement'])
    source_doi = note_closure.doi(gap['source_doi'])
    tag = 'DOI:' + source_doi if source_doi else 'No DOI'
    locator = compact(gap['source_locator'])[:100]
    available = 256 - len(f'[Formalization]  | {tag} | {locator}')
    if available < 25:
        locator = locator[:30]
        tag = tag[:140]
        available = 256 - len(f'[Formalization]  | {tag} | {locator}')
    if len(statement) > available:
        statement = statement[:available - 1] + '…'
    source_lines = [('- DOI: https://doi.org/' + source_doi) if source_doi else '- No DOI']
    if gap['source_arxiv']:
        source_lines.append('- arXiv: https://arxiv.org/abs/' + note_closure.arxiv(gap['source_arxiv']))
    if gap['source_url']:
        source_lines.append('- Public source: ' + gap['source_url'])
    source_lines.extend(['- Source location: ' + compact(gap['source_locator']),
                         '- Note location: ' + compact(gap['note_locator'])])
    return {
        'title': f'[Formalization] {statement} | {tag} | {locator}',
        'body': '\n\n'.join([
            '## Exact mathematical statement\n\n' + gap['statement'],
            '## Source and exact location\n\n' + '\n'.join(source_lines),
            '## Why formalize it\n\n' + gap['why_it_matters'],
            '<!-- outreach-formalization:' + item['key'] + ' -->']),
    }


def same_issue(issue, item):
    title = TITLE.match(issue.get('title') or '')
    if not title:
        return False
    body = issue.get('body') or ''
    if '<!-- outreach-formalization:' + item['key'] + ' -->' in body:
        return True
    gap = item['gap']
    statement = compact(gap['statement'])
    paragraphs = {compact(part) for part in re.split(r'\n\s*\n', body)}
    # Existing requests may use a short summary in the title and give the
    # exact statement under a scope or result heading in the body.
    if statement != compact(title[1]) and statement not in paragraphs:
        return False
    for kind, value in coordinates(gap):
        if kind == 'doi':
            if note_closure.doi(title[2]) == value or re.search(
                r'(?:doi:\s*|doi\.org/)' + re.escape(value) + r'(?![\w./-])', body, re.I
            ):
                return True
        elif kind == 'arxiv':
            if re.search(r'(?:arxiv:\s*|arxiv\.org/(?:abs|pdf)/)' + re.escape(value) +
                         r'(?![\w/-])', body, re.I):
                return True
        elif re.search(re.escape(value) + r'(?![\w/.-])', body):
            return True
    return False


def inventory(github, ledger, *, title=TITLE, lane='formalization-requests'):
    issues = []
    for page in range(1, 1000):
        # A partial or budget-interrupted scan cannot authorize a POST.
        with unit(ledger, lane, 'inventory', 'page-' + str(page)) as ready:
            if not ready:
                return None
            batch = github(f'{ISSUES}?state=all&sort=created&direction=asc&per_page=100&page={page}')
        require(isinstance(batch, list) and all(isinstance(i, dict) and 'title' in i for i in batch),
                'Invalid GitHub issue inventory')
        issues.extend(i for i in batch if 'pull_request' not in i and title.match(i['title'] or ''))
        if len(batch) < 100:
            return issues
    raise ValueError('GitHub issue inventory exceeds the scan limit')


def passed(row):
    result = row['data'].get('result') or {}
    return (row['kind'] == 'shortnote' and note_closure.eligible(row)
            and result.get('outcome') == 'completed'
            and review_current(row, row['request']['workflow_hash']))


def advance(ledger, config, github=None):
    if config['paused']:
        return
    tasks = ledger.tasks
    candidates = [(row, item) for row in tasks.all() if passed(row)
                  for item in row['data'].get('formalization_requests', [])
                  if item['state'] == PENDING and recheck.due(tasks, 'formalization-requests', item['key'], item['gap'])]
    if not candidates:
        return
    if github is None:
        from .credentials import SecretStore
        token = os.environ.get('CMA_ACCESS_TOKEN')
        if token:
            github = SecretStore(token).github

    issues = None
    connector_error = 'Coordinator GitHub connection unavailable' if github is None else None
    # Bounded work leaves the rest durable in the ledger for later ticks.
    for original, item in units(ledger, 'formalization-requests', candidates[:8],
                                key=lambda pair: pair[0]['id'] + '-' + pair[1]['key']):
        row = tasks.get(original['id'])
        saved = dict(item)
        try:
            require(connector_error is None, connector_error)
            if issues is None:
                try:
                    issues = inventory(github, ledger)
                except (OSError, ValueError):
                    connector_error = 'GitHub issue inventory unavailable'
                    raise
                if issues is None:
                    return
            existing = next((issue for issue in issues if same_issue(issue, item)), None)
            if existing is None:
                # Snapshot the pending key before the POST. A lost response
                # leads to a fresh scan for its hidden marker before a retry.
                recheck.defer(tasks, 'formalization-requests', item['key'], item['gap'])
                ledger.save(force=True)
                payload = issue_payload(item)
                existing = github(ISSUES, 'POST', payload)
                require(isinstance(existing, dict), 'Invalid GitHub issue receipt')
                issues.append({**existing, **payload})
            url = existing.get('html_url')
            require(isinstance(url, str) and ISSUE_URL.fullmatch(url), 'Missing GitHub issue receipt URL')
            saved.update(state='filed', issue_url=url)
            saved.pop('last_error', None)
        except (OSError, ValueError) as exc:
            saved['last_error'] = (f'GitHub HTTP {exc.code}' if isinstance(exc, HTTPError)
                                   else connector_error or type(exc).__name__)
            recheck.defer(tasks, 'formalization-requests', item['key'], item['gap'])
            # An uncertain POST may have succeeded. Do not use the old
            # inventory to attempt any remaining gap during this tick.
            issues = None
            connector_error = connector_error or 'GitHub filing unavailable this tick'
        if saved != item:
            items = [saved if record['key'] == item['key'] else record
                     for record in row['data']['formalization_requests']]
            tasks.update(row['id'], row['revision'], row['state'],
                         {**row['data'], 'formalization_requests': items})
            ledger.save()


def backfill_plan(snapshot):
    """Select exact PASSed versions from either a work or status snapshot."""
    if (rows := snapshot.get('tasks')) is None:
        rows = [{**row, 'data': {'result': row.get('result'), 'review': row.get('review') or {}}}
                for row in snapshot['affairs_tasks']]
    queued = {(item['task_id'], item['key']) for item in snapshot.get('formalization_requests', [])}
    return [{'task_id': row['id'], 'result_hash': digest(row['data']['result']), 'requests': requests}
            for row in rows if passed(row)
            if (requests := [item for item in pending_requests(row['data']['result'])
                             if (row['id'], item['key']) not in queued
                             and not row['data'].get('formalization_requests')])]


def backfill_envelope(plan, secret):
    value = {'schema_version': 1, 'notes': [
        {k: entry[k] for k in ('task_id', 'result_hash')} for entry in plan]}
    value['signature'] = hmac.new(secret.encode(), canonical(value).encode(), hashlib.sha256).hexdigest()
    return value


def ingest(tasks, value, secret):
    require(isinstance(value, dict) and set(value) == {'schema_version', 'notes', 'signature'}
            and value['schema_version'] == 1, 'Invalid formalization backfill')
    note_closure.validate_sources(value['notes'])
    require(bool(secret) and isinstance(value['signature'], str) and hmac.compare_digest(
        backfill_envelope(value['notes'], secret)['signature'], value['signature']),
        'Formalization backfill signature failed')
    event = digest(value)
    with tasks.tx():
        if tasks.db.execute('SELECT 1 FROM control_events WHERE id=?', (event,)).fetchone():
            return
        for link in value['notes']:
            row = tasks.get(link['task_id'])
            if not passed(row) or digest(row['data']['result']) != link['result_hash']:
                continue
            if not row['data'].get('formalization_requests'):
                requests = pending_requests(row['data']['result'])
                if requests:
                    tasks._update(row['id'], row['revision'], row['state'],
                                  {**row['data'], 'formalization_requests': requests})
        tasks.db.execute('INSERT INTO control_events VALUES(?,?)', (event, canonical(value)))
