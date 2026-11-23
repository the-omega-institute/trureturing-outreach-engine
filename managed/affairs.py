"""Read-only open-affairs inventory from existing tables and an optional work snapshot."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import sys
from urllib.error import HTTPError

from .registry import email, people

from .location import DATA_ROOT as ROOT, load_config, owner_labels
CLOSED = {'closed', 'declined', 'stopped', 'cancelled'}
TERMINAL = {'superseded', 'answered-by-mail', 'verified', 'no-reply', 'owner-discarded',
            'handoff-ready', 'round-complete', 'submission-ready'}


def active(items):
    """Completed/superseded obligations remain evidence, not fresh promises."""
    return [i for i in items or [] if i and (not isinstance(i, dict) or
            not (i.get('done') or i.get('fulfilled') or i.get('superseded_by') or
                 i.get('status') in {'done', 'completed', 'cancelled', 'closed'}))]


def describe(value):
    if isinstance(value, dict):
        return str(next((value[k] for k in ('summary', 'quote', 'what', 'text', 'obligation', 'reason')
                         if value.get(k)), ''))
    return str(value or '')


def timestamp(value):
    if not value:
        return None
    try:
        if isinstance(value, (float, int)):
            return datetime.fromtimestamp(value, timezone.utc)
        point = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        return point.replace(tzinfo=point.tzinfo or timezone.utc).astimezone(timezone.utc)
    except (ValueError, TypeError, OverflowError):
        return None


def live_tasks(snapshot):
    """Accept control work/status output; coalesce tasks repeated in work sections."""
    if snapshot is None:
        return []
    rows = snapshot if isinstance(snapshot, list) else [
        r for key in ('tasks', 'affairs_tasks', 'follow_up', 'reviews', 'user_questions', 'owner_drafts',
                      'short_note_deliveries', 'submission_ready', 'mathesis_deliveries_needed')
        for r in snapshot.get(key, [])]
    result = {}
    for row in rows:
        identity = row.get('id') or row.get('task_id') or row.get('binding', {}).get('task_id')
        if not identity or row.get('state') in TERMINAL:
            continue
        old = result.get(identity, {})
        data = row.get('data', {})
        result[identity] = {**old, **row, 'id': identity,
                            'request': row.get('request') or old.get('request', {}),
                            'result': row.get('result') or data.get('result') or old.get('result'),
                            'reason': row.get('reason') or data.get('last_error') or data.get('reason') or old.get('reason')}
    return list(result.values())


def thread_keys(thread, persons):
    addresses = {email(p['email']) for p in
                 [thread.get('correspondent', {}), *thread.get('co_correspondents', [])] if p.get('email')}
    addresses.update(email(a) for a in thread.get('mail_match', {}).get('emails', []))
    addresses.discard('')
    ids = {thread['id'], *thread.get('owner_ids', [])}
    for person in persons:
        if addresses & set(person['emails']):
            addresses.update(person['emails'])
            ids.update([person['id'], *(r['id'] for r in person['records'])])
    return addresses, ids


def matches(task, thread, persons, keys=None):
    addresses, ids = keys if keys is not None else thread_keys(thread, persons)
    if task['id'] in {*thread.get('job_ids', []), thread.get('running_job_id')}:
        return True
    req = task.get('request', {})
    inputs = req.get('inputs', {})
    ref = inputs.get('thread_ref', {})
    recorded = inputs.get('thread_record', {})
    direct = ref.get('thread_id') or recorded.get('id')
    if direct:
        return direct == thread['id']
    task_ids = {task.get('owner'), req.get('owner'), *inputs.get('owner_ids', [])}
    observed = inputs.get('mailbox_observation', {}).get('email', '')
    counterpart = recorded.get('correspondent', {}).get('email', '')
    owner_email = req.get('owner', '') if '@' in req.get('owner', '') else ''
    task_addresses = {email(a) for a in [observed, counterpart, owner_email, *inputs.get('recipient_emails', [])] if a}
    return bool(ids & task_ids or addresses & task_addresses)


def kind_of(thread):
    stage = thread.get('stage', '').lower().replace('_', '-')
    if stage in {'joint-paper', 'joint-writing'}:
        return 'joint-paper'
    if stage == 'collaboration':
        return 'collaboration'
    if stage == 'oeis':
        return 'oeis'
    if stage == 'independent-submission':
        return 'submission'
    if any(a.get('kind') == 'research' for a in active(thread.get('us_actions', [])) if isinstance(a, dict)):
        return 'research'
    return 'reply' if stage in {'exchange', 'first-contact-waiting', 'waiting-reply'} else 'other'


def priority(affair, now, promises=False, requests=False, due=None):
    point = timestamp(affair['last_progress_at'])
    days = max(0, (now - point).days) if point else None
    reasons = []
    for yes, label, points in [(promises, 'open promise', 500),
                               (requests, 'their explicit request', 300),
                               (affair['kind'] == 'joint-paper', 'joint paper', 150),
                               (due is not None and due <= now, 'deadline/next_check due', 60)]:
        if yes:
            reasons.append({'reason': label, 'points': points})
    # The base age term is bounded; weekly fairness is deliberately unbounded.
    # Thus sufficiently old ordinary affairs can overtake newer high-base ones.
    reasons += [{'reason': 'age (days, capped at 30)', 'points': min(days or 0, 30)},
                {'reason': 'fairness (10 per full waiting week, uncapped)', 'points': (days or 0) // 7 * 10}]
    if point is None:
        reasons.append({'reason': 'unknown progress date; inspect evidence', 'points': 0})
    affair.update(days_waiting=days, priority_score=sum(r['points'] for r in reasons),
                  priority_reasons=reasons)


def inventory(root=ROOT, work=None, now=None):
    root = Path(root)
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError('Affairs time requires a timezone')
    threads = json.loads((root/'tables/threads.json').read_text())['lines']
    papers = json.loads((root/'tables/submissions.json').read_text())['papers']
    persons = people(root)
    tasks = live_tasks(work)
    affairs = []; mapped = set()
    for thread in threads:
        if thread.get('stage', '').lower() in CLOSED:
            continue
        if (thread.get('ball') not in {'us', owner_labels()[0]} and not thread.get('our_open_promises')
                and not thread.get('their_open_requests')):
            continue
        promises = active(thread.get('our_open_promises'))
        requests = active(thread.get('their_open_requests'))
        gap = [describe(i) for i in [*requests, *promises] if describe(i)]
        keys = thread_keys(thread, persons) if tasks else None
        matched = [t for t in tasks if matches(t, thread, persons, keys)]
        mapped.update(t['id'] for t in matched)
        events = [(timestamp((thread.get(k) or {}).get('date')), k) for k in ('last_in', 'last_out')]
        point, key = max(events, key=lambda p: p[0] or datetime.min.replace(tzinfo=timezone.utc))
        event = thread.get(key) or {}
        evidence = thread.get('evidence_review', {})
        latest = (event.get('summary') or evidence.get('latest_' + key.removeprefix('last_') + '_excerpt')
                  or ('{}: {}'.format(key, event.get('date', 'unknown'))))
        kind = kind_of(thread)
        if kind in {'joint-paper', 'submission'}:
            completion = 'manuscript passes Oracle editorial review and Claude package review; any submission needs its own receipt'
        elif kind == 'reply':
            completion = 'reply sent answering ' + ('; '.join(gap) or thread.get('topic', thread.get('subject', 'the latest message'))) + '; or reviewed no-reply judgment'
        else:
            completion = 'checked useful result addressing ' + ('; '.join(gap) or thread.get('topic', 'the recorded goal')) + ', with evidence and independent review'
        row = {'id': thread['id'], 'kind': kind, 'counterpart': thread.get('correspondent', {}),
               'goal': thread.get('topic') or thread.get('subject', ''),
               'owner': thread.get('ball') if thread.get('ball') in {'us', 'them', owner_labels()[0]} else 'us',
               'latest_result': latest, 'gap': gap,
               'next_step': thread.get('next_action') or 'Inspect full current thread and evidence; record one concrete next step',
               'handle': sorted(set([*thread.get('job_ids', []),
                                    *([thread['running_job_id']] if thread.get('running_job_id') else []),
                                    *(t['id'] for t in matched)])),
               'completion_condition': completion,
               'last_progress_at': point.isoformat() if point else None}
        priority(row, now, bool(promises), bool(requests), timestamp(thread.get('deadline') or thread.get('next_check')))
        affairs.append(row)
    for paper in papers:
        if paper.get('stage') in {'submitted', 'accepted', 'published', *CLOSED}:
            continue
        matched = [t for t in tasks if t.get('request', {}).get('inputs', {}).get('paper', {}).get('id') == paper['id']
                   or t.get('request', {}).get('inputs', {}).get('submission_source', {}).get('paper_id') == paper['id']]
        mapped.update(t['id'] for t in matched)
        journal = paper.get('target_journal', '')
        blocker = paper.get('blocker') or '; '.join(b['reason'] for b in paper.get('blocking_items', []))
        row = {'id': paper['id'], 'kind': 'submission', 'counterpart': journal,
               'goal': paper['title'], 'owner': owner_labels()[0] if paper.get('next_step_owner') in owner_labels() else 'us',
               'latest_result': paper['stage'], 'gap': [blocker] if blocker else [],
               'next_step': paper.get('next_action') or blocker or 'Verify current manuscript and reviews; stage selected submission package',
               'handle': sorted(t['id'] for t in matched),
               'completion_condition': 'manuscript passes Oracle editorial review and Claude package review; actual submission requires a verified journal receipt',
               'last_progress_at': paper.get('last_progress_at') or paper.get('version_date') or paper.get('version', {}).get('date')}
        priority(row, now, due=timestamp(paper.get('deadline') or paper.get('next_check')))
        affairs.append(row)
    for task in tasks:
        if task['id'] in mapped:
            continue
        req = task.get('request', {}); inputs = req.get('inputs', {})
        row = {'id': task['id'], 'kind': req.get('kind', task.get('kind', 'other')),
               'counterpart': inputs.get('recipient_emails') or req.get('owner', ''),
               'goal': inputs.get('topic') or inputs.get('paper', {}).get('title') or inputs.get('instructions', 'Inspect live task'),
               'owner': owner_labels()[0] if task.get('state') in {'awaiting-user', 'owner-draft'} else 'us',
               'latest_result': task.get('result', {}).get('report', '') if task.get('result') else task.get('state', 'needs review'),
               'gap': [task['reason']] if task.get('reason') else [],
               'next_step': 'Inspect original task handle and current state; advance or record an evidence-bearing wait',
               'handle': [task['id']], 'completion_condition': 'verified useful deliverable and independent review; delivery requires its own receipt',
               'last_progress_at': task.get('updated')}
        if row['kind'] not in {'reply', 'research', 'collaboration', 'joint-paper', 'submission', 'oeis', 'other'}:
            row['kind'] = 'other'
        priority(row, now); affairs.append(row)
    affairs.sort(key=lambda a: (-a['priority_score'], -(a['days_waiting'] or 0), a['id']))
    result = {'as_of': now.isoformat(), 'work_source': 'supplied snapshot' if work is not None else 'not loaded (offline tables only)',
              'count': len(affairs), 'affairs': affairs}
    result['markdown'] = markdown(result)
    return result


def compact(value, limit):
    if isinstance(value, dict):
        value = value.get('name') or value.get('email') or value.get('name', '')
    text = ' '.join(str(value).split()).replace('|', '/')
    return text if len(text) <= limit else text[:limit-1] + '…'


def markdown(result):
    lines = ['Open affairs — {} items; {}; as of {}'.format(result['count'], result['work_source'], result['as_of'][:10]),
             '| # / score / wait | Affair · owner · counterpart | Goal → next step |',
             '| --- | --- | --- |']
    for n, row in enumerate(result['affairs'], 1):
        wait = '?' if row['days_waiting'] is None else str(row['days_waiting']) + 'd'
        lines.append('| {} / {} / {} | {} · {} · {} | {} → {} |'.format(
            n, row['priority_score'], wait, row['id'], row['owner'], compact(row['counterpart'], 42),
            compact(row['goal'], 65), compact(row['next_step'], 135)))
    return '\n'.join(lines) + '\n'


def _addresses(value):
    """Extract bare addresses from packet/request recipient representations."""
    values = value if isinstance(value, list) else [value]
    found = []
    pending = list(values)
    while pending:
        item = pending.pop(0)
        if isinstance(item, list):
            pending[0:0] = item
            continue
        if isinstance(item, dict):
            item = (item.get('emailAddress') or {}).get('address') or item.get('address') or item.get('email')
        if not isinstance(item, str):
            continue
        for address in re.findall(r'[A-Za-z0-9.!#$%&\'*+/=?^_`{|}~-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}', item):
            try:
                found.append(email(address))
            except ValueError:
                pass
    return list(dict.fromkeys(found))


_ABBREVIATIONS = re.compile(
    r'(?<![\w])(?:dr|prof|mr|mrs|ms|sr|jr|e\.g|i\.e)\.|(?<![\w])et\ al\.',
    re.IGNORECASE)


def _first_sentence(value, limit=160):
    """Take a short report sentence without treating common abbreviations as stops."""
    text = ' '.join(str(value or '').split())
    if not text:
        return '—'
    abbreviations = [(match.start(), match.end()) for match in _ABBREVIATIONS.finditer(text)]
    end = None
    for index, character in enumerate(text):
        if character in '。！？!?':
            end = index + 1
            break
        if character != '.':
            continue
        if any(start <= index < stop for start, stop in abbreviations):
            continue
        # A period inside a decimal or an ellipsis is not a sentence boundary.
        if ((index and index + 1 < len(text) and text[index - 1].isdigit() and
             text[index + 1].isdigit()) or
                (index + 1 < len(text) and text[index + 1] == '.')):
            continue
        end = index + 1
        break
    sentence = text[:end] if end is not None else text
    if len(sentence) <= limit:
        return sentence
    clipped = sentence[:limit]
    # Keep a complete whitespace-delimited word when the report is long. For
    # scripts without spaces, the fixed character boundary is the only safe one.
    boundary = re.search(r'\s+\S*$', clipped)
    if boundary and boundary.start() > 0:
        clipped = clipped[:boundary.start()]
    return clipped.rstrip()


def _owner_summary(result):
    brief = result.get('owner_brief') if isinstance(result, dict) else None
    if isinstance(brief, dict):
        summary = brief.get('summary_zh')
        if isinstance(summary, str) and summary.strip():
            return summary.strip()
    return None


def _owner_brief(result, subject=''):
    summary = _owner_summary(result)
    if summary is not None:
        return summary
    if isinstance(result, dict):
        # Legacy results are intentionally summarized from the result report;
        # the reviewed letter body is not a substitute for an owner brief.
        sentence = _first_sentence(result.get('report'))
    else:
        sentence = '—'
    subject = str(subject or '').strip()
    return '{}：{}'.format(subject, sentence) if subject else sentence


def _notes(result, extra=()):
    brief = result.get('owner_brief') if isinstance(result, dict) else {}
    values = []
    if isinstance(brief, dict):
        values.extend(str(v).strip() for v in brief.get('decisions_zh', []) if str(v).strip())
        values.extend(str(v).strip() for v in brief.get('cautions_zh', []) if str(v).strip())
    values.extend(str(v).strip() for v in extra if str(v).strip())
    return '；'.join(values) or '—'


def _sent_evidence(value):
    """Conservatively recognize durable Sent evidence in work snapshots."""
    if not isinstance(value, dict):
        return False
    for nested in ('data', 'result'):
        if isinstance(value.get(nested), dict) and _sent_evidence(value[nested]):
            return True
    for key in ('sent_receipt', 'sentReceipt', 'sent_message', 'sent_message_id', 'sent_receipt_ref'):
        if value.get(key):
            return True
    state = str(value.get('state', '')).casefold()
    if state in {'verified', 'sent', 'sent-verified', 'sent_verified'}:
        return True
    for key in ('thread', 'messages', 'conversation'):
        messages = value.get(key)
        if isinstance(messages, dict):
            messages = messages.get('value') or messages.get('messages')
        if not isinstance(messages, list):
            continue
        for message in messages:
            if not isinstance(message, dict) or message.get('isDraft') is True:
                continue
            if (message.get('isSent') is True or message.get('folder') in {'sent', 'Sent'} or
                    message.get('mailbox') in {'sent', 'Sent'} or message.get('sentDateTime')):
                return True
    return False


def _later_thread_sent(value, thread_rows):
    """Use table-4 thread evidence when a later Sent timestamp is explicit."""
    request = value.get('request') or {}
    inputs = request.get('inputs') or {}
    packet = value.get('packet') or (value.get('result') or {}).get('packet') or {}
    if not isinstance(packet, dict):
        packet = {}
    thread_id = (inputs.get('thread_id') or (inputs.get('thread_ref') or {}).get('thread_id') or
                 packet.get('thread_id'))
    if not thread_id:
        return False
    thread = next((row for row in thread_rows if str(row.get('id')) == str(thread_id)), None)
    last_out = (thread or {}).get('last_out') or {}
    sent_at = last_out.get('date')
    receipt = value.get('draft_receipt') or {}
    saved_at = (receipt.get('saved_at') or receipt.get('at') or receipt.get('created_at')) if isinstance(receipt, dict) else None
    if not sent_at:
        return False
    if not saved_at:
        return False
    try:
        return timestamp(sent_at) > timestamp(saved_at)
    except (TypeError, ValueError):
        return str(sent_at) > str(saved_at)


def _owner_report_contacts(root):
    try:
        persons = people(root)
    except (OSError, ValueError, KeyError, TypeError):
        try:
            rows = json.loads((Path(root) / 'tables' / 'contacts.json').read_text()).get('contacts', [])
        except (OSError, ValueError, TypeError):
            rows = []
        persons = [{**row, 'emails': sorted(set([row.get('email')] +
                    list((row.get('relationship') or {}).get('aliases') or [])))}
                    for row in rows if isinstance(row, dict) and row.get('email')]
    by_address = {address: person for person in persons for address in person['emails']}
    by_id = {person['id']: person for person in persons}
    return persons, by_address, by_id


def _live_mailbox(root, config=None):
    """Build the configured read-only mailbox adapter from the local cache."""
    root = Path(root)
    if config is None:
        config = load_config(root)
    from .credentials import graph_access, local_graph_cache
    from .mailbox import configured

    # `configured` is the single adapter boundary used by the coordinator. It
    # consumes the runtime environment token, while local commands obtain that
    # token from the macOS Keychain-backed MSAL cache first.
    token = graph_access(local_graph_cache())
    previous = os.environ.get('OUTREACH_GRAPH_ACCESS_TOKEN')
    os.environ['OUTREACH_GRAPH_ACCESS_TOKEN'] = token
    try:
        return configured(config, root)
    finally:
        if previous is None:
            os.environ.pop('OUTREACH_GRAPH_ACCESS_TOKEN', None)
        else:
            os.environ['OUTREACH_GRAPH_ACCESS_TOKEN'] = previous


def owner_report(root=ROOT, work=None, check_mailbox=False, mailbox=None, config=None):
    """Build the owner's Chinese owner action table from a saved work snapshot."""
    root = Path(root)
    work = work if isinstance(work, dict) else {}
    persons, by_address, by_id = _owner_report_contacts(root)
    try:
        decisions = json.loads((root / 'tables' / 'decisions.json').read_text())
    except (OSError, ValueError):
        decisions = {}
    controlled = {' '.join(str(v).casefold().split()) for v in decisions.get('owner_controlled_threads', [])}
    try:
        thread_rows = json.loads((root / 'tables' / 'threads.json').read_text()).get('lines', [])
    except (OSError, ValueError):
        thread_rows = []
    controlled_ids = {str(row.get('id')) for row in thread_rows
                      if ' '.join(str((row.get('correspondent') or {}).get('name', '')).casefold().split()) in controlled}

    try:
        account = (load_config(root).get('mailbox') or {}).get('account')
    except Exception:
        account = None
    self_addresses = {email(account)} if account else set()

    def records_for(value):
        request = value.get('request') or {}
        result = value.get('result') or {}
        packet = value.get('packet') or result.get('packet') or {}
        if not isinstance(packet, dict):
            packet = {}
        inputs = request.get('inputs') or {}
        addresses = _addresses([packet.get('to'), packet.get('cc'), inputs.get('recipient_emails')])
        owner_ids = [request.get('owner'), *(inputs.get('owner_ids') or [])]
        for owner_id in owner_ids:
            person = by_id.get(owner_id)
            if person:
                addresses.extend(person['emails'])
        # the owner's own mailbox appears in owner_ids of every thread; it is
        # never a counterpart in her to-do table.
        addresses = [a for a in dict.fromkeys(addresses) if a not in self_addresses]
        thread_id = (inputs.get('thread_id') or
                     (inputs.get('thread_ref') or {}).get('thread_id') or
                     packet.get('thread_id'))
        if str(thread_id) in controlled_ids:
            return None
        names = []
        selected = []
        selected_people = set()
        for address in addresses:
            person = by_address.get(address)
            if person is None:
                continue
            if ' '.join(person['name'].casefold().split()) in controlled:
                continue
            if address not in selected:
                selected.append(address)
            if person['id'] not in selected_people:
                selected_people.add(person['id'])
                names.append(person['name'])
        return {'names': names, 'emails': selected} if selected else None

    rows = []
    seen = set()
    mailbox_removed = 0
    mailbox_error = None

    def mailbox_state(value):
        """Read one saved draft by remote ID, returning a conservative state."""
        nonlocal mailbox, mailbox_error
        if not check_mailbox:
            return None
        receipt = value.get('draft_receipt') or {}
        remote_id = receipt.get('remote_id') if isinstance(receipt, dict) else None
        if not remote_id:
            return None
        if mailbox_error is not None:
            return 'error'
        if mailbox is None:
            try:
                mailbox = _live_mailbox(root, config)
            except Exception as exc:
                # Credential/configuration and transient connector failures are
                # observational failures; keep the owner row visible.
                mailbox_error = exc
                return 'error'
        try:
            message = mailbox.message(remote_id)
        except HTTPError as exc:
            if exc.code == 404:
                return 'gone'
            return 'error'
        except Exception as exc:
            if getattr(exc, 'code', None) == 404 or getattr(exc, 'status', None) == 404:
                return 'gone'
            return 'error'
        if message is None:
            return 'gone'
        if not isinstance(message, dict) or 'isDraft' not in message:
            return 'error'
        if message.get('isDraft') is True:
            return 'draft'
        return 'gone'

    def add(value, *, status, extra_notes=(), force_result=None, brief_override=None):
        nonlocal mailbox_removed
        result = force_result if force_result is not None else (value.get('result') or {})
        packet = value.get('packet') or result.get('packet') or {}
        if not isinstance(packet, dict):
            packet = {}
        if isinstance(result, dict) and packet and 'packet' not in result:
            result = {**result, 'packet': packet}
        subject_value = packet.get('subject') or value.get('request', {}).get('inputs', {}).get('subject') or ''
        subject = subject_value or '—'
        target = records_for({**value, 'result': result, 'packet': packet})
        if target is None:
            return
        check = mailbox_state(value) if status == 'draft' else None
        if check == 'gone':
            mailbox_removed += 1
            return
        key = (value.get('task_id') or value.get('id') or value.get('request', {}).get('id'), status)
        if key in seen:
            return
        seen.add(key)
        save = value.get('draft_receipt') or {}
        saved_at = (save.get('saved_at') or save.get('at') or save.get('created_at') or save.get('timestamp')) if isinstance(save, dict) else ''
        notes = list(extra_notes)
        if saved_at:
            notes.append('草稿保存时间：' + str(saved_at))
        if check == 'error':
            notes.append('未核实')
        legacy = _owner_summary(result) is None
        brief = brief_override if brief_override is not None else _owner_brief(result, subject_value if legacy else '')
        row = {'task_id': value.get('task_id') or value.get('id') or value.get('request', {}).get('id'),
               'status': status, 'person': '、'.join(target['names']),
               'email': ', '.join(target['emails']), 'subject': subject,
               'brief': brief, 'content': brief,
               'notes': _notes(result, notes)}
        if check == 'error':
            row['mailbox_status'] = '未核实'
        rows.append(row)

    owner_drafts = work.get('owner_drafts', []) or []
    for item in owner_drafts:
        if _sent_evidence(item) or _later_thread_sent(item, thread_rows):
            continue
        if item.get('draft_receipt'):
            add(item, status='draft')
        elif (item.get('state') == 'owner-draft' or
              (item.get('result') or {}).get('routing', {}).get('mail') == 'owner'):
            add(item, status='reviewed-owner', extra_notes=('尚未保存为草稿',))

    # Newer work snapshots expose every task; use them to include reviewed
    # owner routes that have not entered owner_drafts yet.
    task_rows = work.get('affairs_tasks') or work.get('tasks') or []
    for item in task_rows:
        data = item.get('data') or {}
        result = item.get('result') or data.get('result') or {}
        if result.get('routing', {}).get('mail') != 'owner' or _sent_evidence(item) or _later_thread_sent(item, thread_rows):
            continue
        if item.get('state') not in {'approved', 'owner-draft'} and not (item.get('review') or data.get('review') or {}).get('verdict') == 'PASS':
            continue
        if any(d.get('task_id') == item.get('id') for d in owner_drafts):
            continue
        item = {**item, 'task_id': item.get('id'), 'result': result,
                'draft_receipt': item.get('draft_receipt') or data.get('draft_receipt')}
        if item.get('draft_receipt'):
            add(item, status='draft')
        else:
            add(item, status='reviewed-owner', extra_notes=('尚未保存为草稿',))

    for item in work.get('user_questions', []) or []:
        task_id = item.get('task_id')
        result = item.get('result') or {}
        request = item.get('request') or {}
        questions = item.get('questions') or result.get('routing', {}).get('questions', [])
        for question in questions:
            if isinstance(question, dict):
                text = question.get('question', '')
                quote = question.get('source_quote')
                extra = [('原文：' + quote) if quote else '']
            else:
                text, extra = str(question), []
            add({'task_id': task_id, 'request': request, 'result': result,
                 'packet': result.get('packet')}, status='question',
                extra_notes=extra, force_result={**result, 'report': text}, brief_override=text)

    rows.sort(key=lambda row: (row['person'], row['email'], row['task_id'] or '', row['status']))
    lines = ['| 人 | 邮箱 | 内容 | 注意事项 |', '| --- | --- | --- | --- |']
    for row in rows:
        def cell(value):
            return str(value or '—').replace('|', '\\|').replace('\n', ' ')
        content = row['brief']
        if (row['subject'] and row['status'] != 'question' and
                not content.startswith(row['subject'] + '：')):
            content = row['subject'] + '：' + content
        lines.append('| {} | {} | {} | {} |'.format(cell(row['person']), cell(row['email']), cell(content), cell(row['notes'])))
    if check_mailbox:
        lines.append('已发送或已删除: {}'.format(mailbox_removed))
    result = {'rows': rows, 'count': len(rows), 'markdown': '\n'.join(lines) + '\n'}
    if check_mailbox:
        result['sent_or_deleted'] = mailbox_removed
    return result


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    if argv and argv[0] == 'owner-report':
        parser = argparse.ArgumentParser(description='Owner action report')
        parser.add_argument('--work', type=Path, required=True)
        parser.add_argument('--format', choices=['md', 'json'], default='md')
        parser.add_argument('--check-mailbox', action='store_true',
                            help='Read saved draft IDs from the configured mailbox')
        args = parser.parse_args(argv[1:])
        result = owner_report(work=json.loads(args.work.read_text()), check_mailbox=args.check_mailbox)
        print(json.dumps(result, ensure_ascii=False, indent=2) if args.format == 'json' else result['markdown'], end='')
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--json', action='store_true', help='Include full fields, priority reasons and Markdown in JSON')
    parser.add_argument('--work', type=Path, help='Previously saved managed.control work/status JSON (no network access)')
    args = parser.parse_args(argv)
    result = inventory(work=json.loads(args.work.read_text()) if args.work else None)
    print(json.dumps(result, ensure_ascii=False, indent=2) if args.json else result['markdown'], end='\n' if args.json else '')


if __name__ == '__main__':
    main()
