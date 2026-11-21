"""Audit earlier resolutions through the serialized Outreach coordinator."""
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import tempfile
from urllib.error import HTTPError

from . import note_closure, prior_work_evidence as evidence, publication, recheck
from .formalization_requests import ISSUES, ISSUE_URL, inventory, passed
from .pipeline import read
from .queue import GateError, canonical, digest, require
from .tick import units

BACKFILL_PREFIX = 'prior-work-backfill'
LANE = 'prior-work-issues'
EVIDENCE_LANE = 'prior-work-evidence'
ISSUE_PREFIX = 'prior-work-issue:'
CHECK_PREFIX = 'prior-work-check:'
BACKFILL_KEY = 'prior-work-backfill:'
PER_TICK = 8
REVIEWED_STATES = {'approved', 'handoff-ready', 'verified'}
CHECK_VERSION = 2
BACKFILL_REQUIRED = 'Legacy prior-work evidence requires a signed backfill selection'
NEEDS_EVIDENCE = 'Reviewed evidence artifact requires readback'
EXCLUDED_TRACK = re.compile(r'(?:^|[^a-z0-9])(?:sair|cpp)(?:$|[^a-z0-9])', re.I)


def reviewed(row):
    return row['state'] in REVIEWED_STATES and passed(row)


def result_records(root):
    path = Path(root) / 'tables' / 'results.json'
    return {r['id']: r for r in read(root, 'results')['results']} if path.exists() else {}


def result_id(row, check):
    inputs = row['request']['inputs']
    pipeline = inputs.get('pipeline') or {}
    pinned = inputs.get('result') or pipeline.get('result') or {}
    rid = pipeline.get('result_id') or inputs.get('result_id') or pinned.get('id') or check.get('result_id')
    return rid if isinstance(rid, str) and re.fullmatch(r'[a-zA-Z0-9_.-]{1,200}', rid) else None


def result_context(row, check, records):
    inputs = row['request']['inputs']
    pipeline = inputs.get('pipeline') or {}
    pinned = inputs.get('result') or pipeline.get('result') or {}
    rid = result_id(row, check)
    require(rid, 'Missing or invalid upstream result ID')
    require(not check.get('result_id') or check['result_id'] == rid,
            'Prior-work check refers to a different result')
    current = records.get(rid, {})
    sources = (pinned.get('upstream') or {}, pinned, pipeline.get('upstream') or {}, pipeline,
               inputs.get('upstream') or {}, inputs, current.get('upstream') or {}, current, check)

    def first(*names, parse=evidence.line):
        return next((value for source in sources for name in names
                     if (value := parse(source.get(name)))), '')

    return {
        'result_id': rid,
        'statement': first('posed_statement', 'statement'),
        'posing_locator': first('posing_locator', 'source_locator'),
        'posing_url': first('posing_url', 'source_url', parse=evidence.public_url),
        'resolution_date': first('resolution_date', 'resolved_at',
                                 parse=lambda value: evidence.line(value) if evidence.date_bounds(value) else ''),
        'lean_url': first('lean_url', 'evidence_url', parse=lean_link),
        'declarations': first('declaration_gids', 'declaration_gid', parse=declarations),
    }


def lean_link(value):
    url = evidence.public_url(value)
    return url if re.fullmatch(r'https://github\.com/[^/]+/[^/]+/blob/[^/]+/[^?#]+\.lean(?:#.*)?', url) else ''


def declarations(value):
    values = value if isinstance(value, list) else [value]
    return [s for v in values if (s := evidence.line(v))]


def assess(row, check, records):
    item = reference(row, result_id(row, check))
    try:
        item.update(result_context(row, check, records))
    except ValueError as exc:
        return {**item, 'reason': str(exc)}
    if any(evidence.PRIVATE.search(item[field]) for field in ('statement', 'posing_locator')):
        return {**item, 'reason': 'Posed statement or posing locator contains private-source wording'}

    public = [citation for citation in check['items'] if not citation.get('private')]
    if not public:
        item['reason'] = ('Only private prior-work evidence; needs a public source' if check['items'] else
                          'Prior-exists evidence has no usable public citation')
        return item
    own = evidence.date_bounds(item['resolution_date'])
    if not own:
        return {**item, 'reason': 'Missing trureturing resolution date'}

    earlier = []
    overlaps = False
    for citation in public:
        released = evidence.date_bounds(citation['date'])
        if not released:
            continue
        if released[1] < own[0]:
            earlier.append(citation)
        elif released[0] <= own[1] and (released[0] != released[1] or own[0] != own[1]):
            overlaps = True
    if not earlier:
        if overlaps:
            return {**item, 'reason': 'A public citation date range overlaps the trureturing resolution date'}
        if any(citation['date'] == 'unrecognized date' for citation in public):
            return {**item, 'reason': 'A prior-work release date could not be read'}
        if any(not citation['date'] for citation in public):
            return {**item, 'reason': 'A public prior-work citation is missing its release date'}
        return None

    item['earlier'] = sorted(earlier, key=lambda x: (evidence.date_bounds(x['date']), x['url']))
    item['key'] = digest([item['result_id'], sorted({x['url'] for x in earlier})])
    if not (item['statement'] and item['posing_locator'] and (item['lean_url'] or item['declarations'])):
        item['reason'] = 'Missing posed statement, posing locator or trureturing declaration'
    return item


def marker(result_id):
    return '<!-- outreach-prior-work-result:' + digest(result_id) + ' -->'


def issue_payload(item):
    locator = item['posing_locator'].replace('|', ' / ')
    if len(locator) > 120:
        locator = locator[:119].rstrip() + '…'
    statement = item['statement'].replace('|', ' / ')
    available = 256 - len(f'[Prior work]  | {locator}')
    if len(statement) > available:
        statement = statement[:available - 1].rstrip() + '…'
    posing = item['posing_locator']
    if item['posing_url']:
        posing += '\n\n' + item['posing_url']
    own = ['Result ID: `' + item['result_id'] + '`']
    if item['resolution_date']:
        own.append('Resolution date: ' + item['resolution_date'])
    own.extend('`' + gid + '`' for gid in item['declarations'])
    if item['lean_url']:
        own.append(item['lean_url'])
    citations = []
    for citation in item['earlier']:
        if citation.get('private'):
            continue
        locator_text = ' · ' + citation['locator'] if citation['locator'] else ''
        citations.append(f"- {citation['authors']} · {citation['date']}{locator_text}. "
                         f"{citation['proves']}\n  {citation['url']}")
    require(citations, 'A prior-work issue requires an earlier public citation')
    payload = {'title': f'[Prior work] {statement} | {locator}', 'body': '\n\n'.join([
        '## Posed statement\n\n' + item['statement'],
        '## Posing locator\n\n' + posing,
        '## trureturing resolution\n\n' + '\n\n'.join(own),
        '## Prior-work evidence\n\n' + '\n'.join(dict.fromkeys(citations)),
        '## Requested upstream update\n\n'
        'Credit the earliest verified work in the trureturing records and Atlas. '
        'Mark the trureturing proof as an alternative or independent resolution, as appropriate. '
        'Audit the upstream prior-work search to establish why it missed these sources.',
        marker(item['result_id']),
        '<!-- outreach-prior-work:' + item['key'] + ' -->',
    ])}
    # A bare @name in a public issue notifies that GitHub account.
    return {key: re.sub(r'(?<![\w/.])@(?=\w)', '@\u200b', value) for key, value in payload.items()}


def evidence_files(row):
    return [a for a in row['data']['result'].get('artifacts', [])
            if a['role'] == 'evidence' and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,199}', a['name'])
            and a['name'].lower().endswith(('.md', '.txt'))
            and not any(word in a['name'].lower() for word in ('private', 'correspondence', 'mail'))]


def parse_file(path, artifact):
    raw = path.read_bytes()
    require(len(raw) == artifact['bytes'] and hashlib.sha256(raw).hexdigest() == artifact['sha256'],
            'Prior-work evidence differs from the exact PASS')
    try:
        return evidence.parse_text(raw.decode('utf-8'))
    except UnicodeDecodeError as exc:
        raise evidence.UnparseableEvidence('Prior-work evidence is not valid UTF-8') from exc


def check_progress(row, cache):
    saved = cache.get(row['id'], {})
    if saved.get('version') == CHECK_VERSION and saved.get('result_hash') == digest(row['data']['result']):
        return saved
    return {}


def checked(row, cache, backfill):
    result = row['data']['result']
    if 'prior_work_check' in result:
        return {'check': evidence.project(result['prior_work_check'])}
    if not evidence_files(row):
        return {'check': None}
    if backfill.get(row['id']) != digest(result):
        return {'reason': BACKFILL_REQUIRED}
    saved = check_progress(row, cache)
    if 'check' in saved or 'reason' in saved:
        return saved
    return {'reason': NEEDS_EVIDENCE}


def state(tasks, prefix):
    rows = tasks.db.execute('SELECT key, payload FROM pipeline_state WHERE substr(key,1,?)=? ORDER BY rowid',
                            (len(prefix), prefix))
    return {key[len(prefix):]: json.loads(payload) for key, payload in rows}


def save(tasks, prefix, key, value):
    tasks.db.execute('INSERT OR REPLACE INTO pipeline_state VALUES(?,?)', (prefix + key, canonical(value)))


class TaskSnapshot:
    """Provide the task reads used by the publication exclusion."""

    def __init__(self, rows):
        self.rows = {row['id']: row for row in rows}

    def all(self):
        return self.rows.values()

    def get(self, task_id):
        require(task_id in self.rows, 'Unknown publication source task')
        return self.rows[task_id]


def publication_reason(row, tasks, decisions, records, rid):
    try:
        reason = publication.exclusion(row, tasks, decisions, decisions.get('short_note_publication'),
                                       result_id=rid)
    except ValueError:
        return 'Publication eligibility could not be verified'
    if reason:
        return 'Excluded from public filing: ' + reason
    inputs = row['request']['inputs']
    pipeline = inputs.get('pipeline') or {}
    pinned = inputs.get('result') or pipeline.get('result') or {}
    current = records.get(rid, {})
    sources = (inputs, pipeline, pinned, pinned.get('upstream') or {}, current, current.get('upstream') or {})
    if EXCLUDED_TRACK.search(rid or '') or any(
            EXCLUDED_TRACK.search(evidence.line(source.get(field)))
            for source in sources for field in ('track', 'project', 'project_id', 'repository')):
        return 'Excluded from public filing: SAIR/CPP work'
    return None


def reference(row, rid):
    return {'task_id': row['id'], 'result_hash': digest(row['data']['result']),
            'result_id': rid, 'key': None}


def issue_state_key(item):
    return item['result_id'] or 'task:' + item['task_id']


def candidates(tasks, checks, records, decisions, existing, backfill):
    selected = {}
    for row in tasks.all():
        if not reviewed(row):
            continue
        inspected = checked(row, checks, backfill)
        check = inspected.get('check') or {}
        if check.get('verdict') != 'prior-exists' and not inspected.get('reason'):
            continue
        rid = result_id(row, check)
        if existing.get(rid, {}).get('state') == 'filed':
            continue
        reason = publication_reason(row, tasks, decisions, records, rid) or inspected.get('reason')
        item = {**reference(row, rid), 'reason': reason} if reason else assess(row, check, records)
        if item is None:
            continue
        # Prefer fully sourced notes when several exact PASSes cover one result.
        key = issue_state_key(item)
        if key not in selected or ('reason' in selected[key] and 'reason' not in item):
            selected[key] = item
    return selected.values()


def inventory_view(tasks):
    checks = state(tasks, CHECK_PREFIX)
    return {'prior_work_issues': list(state(tasks, ISSUE_PREFIX).values()),
            'prior_work_checks': checks,
            'prior_work_evidence_failure_count': sum('last_error' in check for check in checks.values()),
            'prior_work_backfill': state(tasks, BACKFILL_KEY)}


def decisions_table(root):
    path = Path(root) / 'tables' / 'decisions.json'
    return read(root, 'decisions') if path.exists() else {}


def read_local_check(row, directory):
    for artifact in evidence_files(row):
        path = Path(directory) / row['id'] / artifact['name']
        if not path.is_file():
            return None
        try:
            check = parse_file(path, artifact)
        except (OSError, ValueError) as exc:
            return {'reason': read_failure(exc)}
        if check is not None:
            return {'check': check}
    return {'check': None}


def plan(snapshot, root, directory=None):
    rows = snapshot.get('tasks')
    if rows is None:
        rows = [{**row, 'data': {'result': row.get('result') or {}, 'review': row.get('review') or {}}}
                for row in snapshot['affairs_tasks']]
    tasks = TaskSnapshot(rows)
    checks = dict(snapshot.get('prior_work_checks', {}))
    backfill = snapshot.get('prior_work_backfill', {})
    preview_backfill = dict(backfill)
    existing = {issue_state_key(item): item for item in snapshot.get('prior_work_issues', [])}
    records = result_records(root)
    decisions = decisions_table(root)
    if directory is not None:
        for row in rows:
            if not reviewed(row) or checked(row, checks, backfill).get('reason') not in (
                    BACKFILL_REQUIRED, NEEDS_EVIDENCE):
                continue
            inspected = read_local_check(row, directory)
            if inspected is not None:
                binding = digest(row['data']['result'])
                checks[row['id']] = {'version': CHECK_VERSION, 'result_hash': binding, **inspected}
                preview_backfill[row['id']] = binding

    unread = []
    notes = []
    for item in candidates(tasks, checks, records, decisions, existing, preview_backfill):
        visible = {key: item[key] for key in ('task_id', 'result_hash', 'result_id', 'key', 'reason') if key in item}
        if 'reason' not in item:
            visible['issue'] = issue_payload(item)
            if (backfill.get(item['task_id']) != item['result_hash']
                    and preview_backfill.get(item['task_id']) == item['result_hash']):
                visible['requires_backfill'] = True
        if item.get('reason') in (BACKFILL_REQUIRED, NEEDS_EVIDENCE):
            unread.append(visible)
        else:
            notes.append(visible)
    return {'count': sum('issue' in note for note in notes), 'notes': notes,
            'needs_evidence': unread}


def envelope(selection, secret):
    value = {'schema_version': 1, 'purpose': BACKFILL_PREFIX,
             'notes': [{k: item[k] for k in ('task_id', 'result_hash')} for item in selection]}
    value['signature'] = hmac.new(secret.encode(), canonical(value).encode(), hashlib.sha256).hexdigest()
    return value


def ingest(tasks, value, secret):
    require(isinstance(value, dict) and set(value) == {'schema_version', 'purpose', 'notes', 'signature'}
            and value['schema_version'] == 1 and value['purpose'] == BACKFILL_PREFIX,
            'Invalid prior-work backfill')
    note_closure.validate_sources(value['notes'])
    require(bool(secret) and isinstance(value['signature'], str)
            and hmac.compare_digest(envelope(value['notes'], secret)['signature'], value['signature']),
            'Prior-work backfill signature failed')
    event = digest(value)
    with tasks.tx():
        if tasks.db.execute('SELECT 1 FROM control_events WHERE id=?', (event,)).fetchone():
            return
        for link in value['notes']:
            row = tasks.get(link['task_id'])
            if reviewed(row) and digest(row['data']['result']) == link['result_hash']:
                save(tasks, BACKFILL_KEY, row['id'], link['result_hash'])
        tasks.db.execute('INSERT INTO control_events VALUES(?,?)', (event, canonical(value)))


def read_failure(exc):
    if isinstance(exc, (GateError, evidence.UnparseableEvidence)):
        return str(exc)
    if isinstance(exc, HTTPError):
        return f'Prior-work evidence download failed (HTTP {exc.code})'
    if isinstance(exc, RuntimeError):
        status = re.search(r'\bHTTP [45]\d\d\b', str(exc))
        return 'Prior-work evidence download failed' + (f' ({status[0]})' if status else '')
    return f'Prior-work evidence could not be read ({type(exc).__name__})'


def read_checks(ledger, rows, checks):
    tasks = ledger.tasks
    pending = [row for row in rows if 'check' not in check_progress(row, checks) and recheck.due(
        tasks, EVIDENCE_LANE, row['id'], digest(row['data']['result']))]
    for row in units(ledger, EVIDENCE_LANE, pending[:PER_TICK]):
        binding = digest(row['data']['result'])
        progress = check_progress(row, checks)
        index = progress.get('next_file', 0)
        artifacts = evidence_files(row)
        artifact = artifacts[index]
        try:
            with tempfile.TemporaryDirectory(prefix='prior-work-', dir=ledger.folder) as directory:
                path = publication.download(row, artifact['name'], directory, ledger.releases)
                check = parse_file(path, artifact)
        except evidence.UnparseableEvidence as exc:
            progress = {'check': None, 'reason': read_failure(exc)}
        except (OSError, RuntimeError, ValueError) as exc:
            failures = progress.get('failures', 0) + 1
            delay = min(recheck.RECHECK_SECONDS * 2 ** min(failures - 1, 6), 24 * 3600)
            recheck.defer(tasks, EVIDENCE_LANE, row['id'], binding, delay)
            reason = read_failure(exc)
            progress = {'next_file': index, 'failures': failures, 'last_error': reason, 'reason': reason}
        else:
            progress = ({'check': check} if check is not None or index + 1 == len(artifacts)
                        else {'next_file': index + 1})
        checks[row['id']] = {'version': CHECK_VERSION, 'result_hash': binding, **progress}
        save(tasks, CHECK_PREFIX, row['id'], checks[row['id']])
        ledger.save()


def advance(ledger, config, root, github=None):
    if config['paused']:
        return
    tasks = ledger.tasks
    snapshot = TaskSnapshot(tasks.all())
    backfill = state(tasks, BACKFILL_KEY)
    checks = state(tasks, CHECK_PREFIX)
    records = result_records(root)
    decisions = decisions_table(root)
    existing = state(tasks, ISSUE_PREFIX)
    legacy = []
    for row in snapshot.all():
        if (row['id'] not in backfill or not reviewed(row) or 'prior_work_check' in row['data']['result']
                or backfill[row['id']] != digest(row['data']['result']) or not evidence_files(row)):
            continue
        rid = result_id(row, {})
        if existing.get(rid, {}).get('state') != 'filed' and not publication_reason(
                row, snapshot, decisions, records, rid):
            legacy.append(row)
    read_checks(ledger, legacy, checks)

    pending = []
    for item in candidates(snapshot, checks, records, decisions, existing, backfill):
        key = issue_state_key(item)
        entry = {k: item[k] for k in ('task_id', 'result_id', 'key', 'result_hash')}
        entry['state'] = 'pending'
        if 'reason' in item:
            entry['reason'] = item['reason']
            if entry != existing.get(key):
                pending.append((item, entry))
        elif recheck.due(tasks, LANE, key, entry):
            pending.append((item, entry))
    if github is None and (token := os.environ.get('CMA_ACCESS_TOKEN')):
        from .credentials import SecretStore
        github = SecretStore(token).github

    issues = None
    connector_error = 'Coordinator GitHub connection unavailable' if github is None else None
    for item, entry in units(ledger, LANE, pending[:PER_TICK], key=lambda pair: issue_state_key(pair[1])):
        rid = entry['result_id']
        key = issue_state_key(entry)
        if entry != existing.get(key):
            save(tasks, ISSUE_PREFIX, key, entry)
            orphan = 'task:' + entry['task_id']
            if rid and orphan in existing:
                tasks.db.execute('DELETE FROM pipeline_state WHERE key=?', (ISSUE_PREFIX + orphan,))
            ledger.save()
        if 'reason' in entry:
            continue
        try:
            require(connector_error is None, connector_error)
            if issues is None:
                issues = inventory(github, ledger, title=re.compile(''), lane=LANE)
                if issues is None:
                    return
            receipt = next((issue for issue in issues if marker(rid) in (issue.get('body') or '')
                            or '<!-- outreach-prior-work:' + item['key'] + ' -->' in (issue.get('body') or '')), None)
            if receipt is None:
                recheck.defer(tasks, LANE, rid, entry)
                ledger.save(force=True)
                payload = issue_payload(item)
                receipt = github(ISSUES, 'POST', payload)
                require(isinstance(receipt, dict), 'Invalid GitHub issue receipt')
                issues.append({**receipt, **payload})
            url = receipt.get('html_url')
            require(isinstance(url, str) and ISSUE_URL.fullmatch(url), 'Missing GitHub issue receipt URL')
        except (OSError, ValueError) as exc:
            recheck.defer(tasks, LANE, rid, entry)
            entry['last_error'] = f'GitHub HTTP {exc.code}' if isinstance(exc, HTTPError) else (
                connector_error or type(exc).__name__)
            connector_error = connector_error or 'GitHub filing unavailable this tick'
            issues = None
        else:
            entry.update(state='filed', issue_url=url)
        save(tasks, ISSUE_PREFIX, rid, entry)
        ledger.save()
