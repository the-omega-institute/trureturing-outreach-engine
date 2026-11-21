"""Read-only reconciliation index. Never project mail bodies or artifact contents."""
from collections import Counter
import json
from pathlib import Path
import re

from . import affairs, publication, registry, submissions
from .queue import digest
from .tasks import matches_workflow

from .location import DATA_ROOT as ROOT
CHECKS = ('send-registry', 'owner-draft', 'publication', 'submissions', 'affairs', 'failed')
SECTIONS = ('tasks', 'affairs_tasks', 'follow_up', 'reviews', 'user_questions',
            'owner_drafts', 'short_note_deliveries', 'mathesis_deliveries_needed',
            'registry_updates', 'parked_holds')
RECOVERY = ('requeue', 'recovery', 'requeue_id', 'recovery_task_id', 'continuation_id', 'successor')
NEW_RESEARCH = re.compile(
    r'\b(?:new|further|additional|more)\s+(?:mathematical\s+)?research\b|'
    r'\bnew\s+(?:idea|argument|proof|approach)\b|\bresearch\s+(?:needed|required)\b', re.I)


def line(value, limit=180):
    text = ' '.join(str(value or '').split())
    return text if len(text) <= limit else text[:limit - 1] + '…'


def error_type(value):
    """Legacy errors are prose; expose only a recognizable exception type."""
    if isinstance(value, dict):
        name = value.get('type') or value.get('code') or ''
        return name if re.fullmatch(r'[A-Za-z][A-Za-z0-9_.]{0,49}', str(name)) else 'dict'
    match = re.search(r'\b[A-Za-z][A-Za-z0-9]*(?:Error|Exception)\b', str(value or ''))
    return match.group() if match else (type(value).__name__ if value else 'none')


def _sent_event_evidence(value):
    """Decode the metadata formats written by manual and Sent Items recorders."""
    evidence = value
    for _ in range(3):
        if not isinstance(evidence, str):
            break
        try:
            decoded = json.loads(evidence)
        except (TypeError, ValueError):
            break
        if decoded == evidence:
            break
        evidence = decoded
    return evidence


def _sent_event_matches(event, task_id, message_ids):
    """Match one sent event to a receipt using task or exact message evidence."""
    if event.get('task_id') == task_id:
        return True
    evidence_value = event.get('evidence', '')
    evidence = _sent_event_evidence(evidence_value)
    if isinstance(evidence, dict) and evidence.get('task_id') == task_id:
        return True
    if isinstance(evidence_value, str):
        evidence_text = evidence_value
    else:
        evidence_text = json.dumps(evidence, ensure_ascii=False, sort_keys=True)
    return any(message_id and message_id in evidence_text for message_id in message_ids)


class SnapshotTasks:
    """Coalesce the existing work projections into the row interface readers use."""
    def __init__(self, work):
        self.rows = {}
        for section in SECTIONS:
            for item in work.get(section, []):
                identity = item.get('id') or item.get('task_id') or item.get('binding', {}).get('task_id')
                if not identity:
                    continue
                row = self.rows.setdefault(identity, {'id': identity, 'data': {},
                    'request': {'inputs': {}, 'workflow_hash': ''}, 'kind': '', 'state': ''})
                for key in ('request', 'kind', 'state', 'owner', 'updated', 'revision'):
                    if item.get(key) is not None:
                        row[key] = item[key]
                row['data'].update(item.get('data') or {})
                for key in ('result', 'review', 'draft_receipt', 'draft_intent_expired', 'sent_receipt', 'last_error',
                            'reason', 'packet', 'packet_override', *RECOVERY):
                    if item.get(key) is not None:
                        row['data'][key] = item[key]
                row['kind'] = row['kind'] or row['request'].get('kind', '')
        for section, field in (('publication_deliveries', 'publication_loop'),
                               ('mathesis_deliveries', 'mathesis_delivery')):
            for item in work.get(section, []):
                identity = item['task_id']
                row = self.rows.setdefault(identity, {'id': identity, 'kind': 'mathesis',
                    'state': item.get('state', ''), 'request': {'inputs': {}, 'workflow_hash': ''}, 'data': {}})
                row['data'][field] = {k: v for k, v in item.items() if k not in {'task_id', 'state'}}

    def all(self):
        return list(self.rows.values())

    def get(self, identity):
        if identity not in self.rows:
            raise ValueError('Source task absent from snapshot: ' + str(identity))
        return self.rows[identity]


def inventory(work, root=ROOT):
    """Use supplied work/status JSON and current tables; perform no external I/O."""
    from .control import workflow
    root = Path(root)
    tasks = SnapshotTasks(work)
    rows = [r for r in tasks.all() if r['state'] != 'superseded']
    persons = registry.people(root)
    contacts = {a: p for p in persons for a in p['emails']}
    items = []

    def add(check, identity, reason, evidence, key='task_id'):
        items.append({'check': check, key: identity, 'reason': line(reason), 'evidence': evidence})

    current = {}
    for row in rows:
        data = row['data']; identity = row['id']
        result = data.get('result') or {}; review = data.get('review') or {}
        receipt = data.get('sent_receipt') or {}
        if row['kind'] in {'reply', 'firstcontact'} and receipt:
            packet = data.get('packet_override') or data.get('packet') or result.get('packet') or {}
            from .mailbox import address
            addresses = {registry.email(a) if isinstance(a, str) else address(a)
                         for a in [*packet.get('to', []), *packet.get('cc', [])]}
            # The sent packet names the actual recipients; requested addresses
            # that the letter excluded never received it.
            if not addresses:
                addresses.update(row['request']['inputs'].get('recipient_emails', []))
            needles = [str(receipt[k]) for k in ('remote_id',) if receipt.get(k)]
            for key in ('evidence_ref', 'internetMessageId', 'message_id'):
                value = receipt.get(key)
                if not value:
                    continue
                value = str(value)
                if key == 'evidence_ref':
                    value = re.sub(r'^graph:(?:sent|inbox):', '', value)
                needles.append(value)
            missing = set()
            for a in addresses:
                person = contacts.get(a.casefold())
                records = person['records'] if person else []
                recorded = any(event.get('kind') == 'sent' and
                    _sent_event_matches(event, identity, needles)
                    for record in records for event in record.get('hub', {}).get('events', []))
                if not recorded:
                    if records:
                        missing.update(r['id'] for r in records)
                    else:
                        missing.add('unregistered:' + a)
            if missing or not addresses:
                add('send-registry', identity, 'Verified send receipt lacks table-1 outgoing event'
                    + ('' if addresses else '; recipient identity unavailable'),
                    'data.sent_receipt; tables/contacts.json:' + ','.join(sorted(missing)))
        if row['state'] == 'owner-draft' and row['kind'] in {'reply', 'firstcontact'}:
            reasons = []
            if not (data.get('draft_receipt') or receipt):
                reasons.append('No draft_receipt or sent_receipt')
            if data.get('draft_intent_expired') and data.get('last_error'):
                reasons.append('Expired draft intent mailbox reconciliation failed')
            binding = review.get('binding', {})
            if (review.get('verdict') != 'PASS' or binding.get('result_hash') != digest(result)
                    or binding.get('workflow_hash') != row['request']['workflow_hash']):
                reasons.append('review no longer binds exact result')
            kind = row['kind']
            if kind not in current:
                current[kind] = workflow(kind)
            if not matches_workflow(row, current[kind]):
                reasons.append('workflow review is stale')
            if reasons:
                add('owner-draft', identity, '; '.join(reasons) + '; last_error=' +
                    error_type(data.get('last_error') or data.get('reason')), 'data.review/draft_receipt/sent_receipt')

    # Match the publication selector's live Mathesis ownership of source notes.
    delegated = set()
    for row in rows:
        if row['kind'] == 'mathesis' and row['state'] != 'failed':
            try:
                delegated.update(r['id'] for r in publication.lineage(row, tasks)[1:] if r['kind'] == 'shortnote')
            except ValueError:
                pass  # Its own exact-PASS check below retains the missing-source diagnostic.
    decisions = registry.read(root, 'decisions')
    decision = decisions.get('short_note_publication', {})
    for row in rows:
        data = row['data']; identity = row['id']
        loop = data.get('publication_loop') or {}; phase = loop.get('phase')
        if row['kind'] in {'shortnote', 'mathesis'} and publication.reviewed(row):
            if phase and phase != 'queued':
                reason = 'publication_loop ' + phase + ': ' + (loop.get('reason') or 'awaiting publication queue')
                if loop.get('last_error'):
                    reason += '; last_error=' + error_type(loop['last_error'])
                add('publication', identity, reason, 'data.publication_loop')
            elif identity not in delegated and (phase != 'queued' or
                    (loop.get('result_hash') and loop['result_hash'] != digest(data['result']))):
                try:
                    excluded = publication.exclusion(row, tasks, decisions, decision)
                except ValueError as error:
                    excluded = None
                    reason = line(error)
                else:
                    reason = ('Exact PASS has no current queued publication_loop' if decision.get('enabled')
                              else 'Exact PASS awaits an active standing publication decision')
                if not excluded:
                    add('publication', identity, reason, 'data.review/publication_loop; tables/decisions.json')
        delivery = data.get('mathesis_delivery') or {}
        if delivery.get('phase') in {'pending', 'conflict'}:
            add('publication', identity, 'mathesis_delivery ' + delivery['phase'] + '; last_error=' +
                error_type(delivery.get('last_error')), 'data.mathesis_delivery')
        receipt = delivery.get('receipt') or {}
        if delivery.get('phase') == 'delivered' and (receipt.get('status') == 'draft' or
                receipt.get('expected_claims_revision') != receipt.get('confirmed_claims_revision')):
            add('publication', identity, 'needs-claims-confirmation: delivered receipt; status=' +
                str(receipt.get('status', 'unknown')), 'data.mathesis_delivery.receipt')

    papers = registry.read(root, 'submissions')['papers']
    for issue in submissions.reconcile_tasks(papers, rows):
        add('submissions', issue.get('task_id') or issue['paper_id'], issue['reason'],
            'tables/submissions.json:' + issue['paper_id'] + ('; request.inputs.paper/submission_source'
                if issue.get('task_id') else ''), 'task_id' if issue.get('task_id') else 'paper_id')

    # inventory() synthesizes useful next-step suggestions. Check the source rows
    # instead of allowing those fallback suggestions to conceal missing actions.
    threads = registry.read(root, 'threads')['lines']
    sources = {r['id']: r for r in [*threads, *papers]}
    for affair in affairs.inventory(root, {'tasks': rows})['affairs']:
        source = sources.get(affair['id'])
        if not source or affair['owner'] != 'us' or affair['handle']:
            continue
        action = source.get('next_action') or source.get('next_step')
        action = action or any(affairs.describe(a) for a in affairs.active(source.get('us_actions')))
        waiting = any(source.get(k) for k in ('waiting_for', 'waiting_condition', 'waiting', 'wait', 'next_check'))
        if not action and not waiting:
            table = 'submissions' if affair['kind'] == 'submission' else 'threads'
            add('affairs', affair['id'], 'Our affair has no next action, linked execution or explicit wait',
                'tables/' + table + '.json:' + affair['id'], 'affair_id')

    children = {}
    for row in rows:
        parent = row['request']['inputs'].get('automation_parent', {}).get('task_id')
        if parent and row['state'] != 'failed':
            children.setdefault(parent, []).append(row)
    for row in rows:
        data = row['data']; review = data.get('review') or {}
        if row['state'] == 'held' and row['kind'] in {'reply', 'research'}:
            reason = review.get('reason') or data.get('reason') or data.get('last_error') or (data.get('result') or {}).get('report', '')
            if NEW_RESEARCH.search(str(reason)) and not any(r['kind'] == 'research' for r in children.get(row['id'], [])):
                add('affairs', row['id'], 'HOLD needs new research; no child research task', 'data.review/reason; request.inputs.automation_parent')
        if row['state'] == 'failed' and not any(data.get(k) for k in RECOVERY) and not children.get(row['id']):
            add('failed', row['id'], 'Failed task has no requeue/recovery exit; last_error=' +
                error_type(data.get('last_error') or data.get('reason')), 'data.reason/last_error/continuation_id')

    items.sort(key=lambda i: (CHECKS.index(i['check']), i.get('task_id') or i.get('paper_id') or i.get('affair_id'), i['reason']))
    counts = Counter(i['check'] for i in items)
    return {'counts': {check: counts[check] for check in CHECKS}, 'items': items}


def render(result, format='json'):
    if format == 'json':
        return json.dumps(result, ensure_ascii=False, separators=(',', ':')) + '\n'
    lines = ['counts: ' + ' '.join(f'{k}={v}' for k, v in result['counts'].items())]
    lines += [f"{i['check']} {i.get('task_id') or i.get('paper_id') or i.get('affair_id')}: "
              f"{i['reason']} [{i['evidence']}]" for i in result['items']]
    return '\n'.join(lines) + '\n'
