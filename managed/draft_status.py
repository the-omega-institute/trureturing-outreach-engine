"""Read-only mailbox observations for the owner-draft to-do inventory."""
from datetime import date, datetime, timedelta, timezone
from difflib import SequenceMatcher
from itertools import zip_longest
import json
from pathlib import Path
import re

from .firstcontact_history import _body_content, _body_text, own_body_text
from .mailbox import DraftsScanDeferred, GraphMailbox
from .queue import canonical, digest, require
from .thread_staging import effective_inputs
from .tick import allowed, unit

KEY = 'owner-draft-observations'
MAX_EDIT_PAIRS = 12
MAX_EDIT_CHARACTERS = 600
SENTENCE_END = re.compile(
    r'(?<=[.!?][”’"\'])\s+|'
    r'(?<!\bMr\.)(?<!\bDr\.)(?<!\bMs\.)(?<!\bProf\.)(?<!\be\.g\.)(?<!\bi\.e\.)(?<!\bet al\.)'
    r'(?<=[.!?])\s+|(?<=[。！？])\s*', re.I)
GREETING = re.compile(r'^Dear [^,]+,\s+', re.I)
PLACEHOLDER = re.compile(
    r'占位|\b(?:TODO|XXX)\b|^[^\S\n]*(?:[\[［][^\S\n]*'
    r'(?:(?:placeholder|insert|fill in|replace)\b|填写|补充|待补)|【[^】\n]+】)',
    re.I | re.M,
)
EVENT = re.compile(r'\b(?:calls?|meet(?:ing)?|zoom|teams meeting|chat|appointment)\b|会议|会面|见面|通话|电话|线上交流', re.I)
MONTH = r'Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?'
DATE = re.compile(
    rf'(?<!\d)(?P<iso>\d{{4}}-\d{{2}}-\d{{2}})(?!\d)|'
    rf'(?:(?P<zy>\d{{4}})年\s*)?(?P<zm>\d{{1,2}})月\s*(?P<zd>\d{{1,2}})[日号]|'
    rf'\b(?P<mdm>{MONTH})\.?\s+(?P<mdd>\d{{1,2}})(?:st|nd|rd|th)?\b'
    rf'(?:,?\s+(?P<mdy>\d{{4}})\b)?|'
    rf'\b(?P<dmd>\d{{1,2}})(?:st|nd|rd|th)?\s+(?P<dmm>{MONTH})\b\.?(?:\s+(?P<dmy>\d{{4}})\b)?',
    re.I,
)
MONTH_NUMBERS = {name.lower(): number for number, name in enumerate(
    ('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'), 1)}


def timestamp(value):
    try:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return datetime.fromtimestamp(value, timezone.utc)
        point = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace('Z', '+00:00'))
    except (AttributeError, TypeError, ValueError, OSError, OverflowError):
        return None
    return point if point.tzinfo else point.replace(tzinfo=timezone.utc)


def event_date(text, anchor):
    """Extract a calendar date near a call/meeting reference."""
    dates = []
    for line in text.splitlines():
        for match in DATE.finditer(line):
            if not EVENT.search(line[max(0, match.start() - 90):match.end() + 55]):
                continue
            parts = match.groupdict()
            try:
                if parts['iso']:
                    day = date.fromisoformat(parts['iso'])
                else:
                    month = parts['mdm'] or parts['dmm']
                    year = parts['zy'] or parts['mdy'] or parts['dmy']
                    day = date(int(year or anchor.year),
                               MONTH_NUMBERS[month[:3].lower()] if month else int(parts['zm']),
                               int(parts['zd'] or parts['mdd'] or parts['dmd']))
                    if not year and (anchor.date() - day).days > 183:
                        day = day.replace(year=day.year + 1)
                    elif not year and (day - anchor.date()).days > 183:
                        day = day.replace(year=day.year - 1)
                dates.append(day)
            except ValueError:
                continue
    return min(dates).isoformat() if dates else None


def inventory(tasks):
    value = tasks.db.execute('SELECT payload FROM pipeline_state WHERE key=?', (KEY,)).fetchone()
    return json.loads(value[0]) if value else {}


def saved_at(data, receipt):
    if saved := timestamp(receipt.get('at') or receipt.get('saved_at')):
        return saved
    for revision in reversed(data.get('draft_revisions', [])):
        if revision.get('new_draft_receipt', {}).get('remote_id') == receipt.get('remote_id'):
            return timestamp(revision.get('old_draft_deleted', {}).get('at'))
    return timestamp((data.get('draft_intent') or {}).get('at'))


def _sentences(body):
    greeting = GREETING.match(body)
    if greeting:
        return [greeting[0].strip(), *SENTENCE_END.split(body[greeting.end():])]
    return SENTENCE_END.split(body)


def owner_edit(packet, message):
    """Compare the saved packet and the owner's Sent copy in reading order."""
    content, html = _body_content({'body': message.get('uniqueBody')})
    if content is None:
        content, html = _body_content(message)
    if content is None or not isinstance(packet.get('body'), str):
        return None
    before = ' '.join(own_body_text(packet['body'], False).split())
    after = ' '.join(own_body_text(content, html).split())
    if before == after:
        return None
    original = _sentences(before) if before else []
    sent = _sentences(after) if after else []
    changed = []
    for operation, i, j, k, end in SequenceMatcher(None, original, sent).get_opcodes():
        if operation == 'equal':
            continue
        limit = MAX_EDIT_PAIRS - len(changed)
        for old, new in zip_longest(original[i:min(j, i + limit)],
                                    sent[k:min(end, k + limit)], fillvalue=''):
            changed.append({'before': old[:MAX_EDIT_CHARACTERS], 'after': new[:MAX_EDIT_CHARACTERS]})
        if len(changed) == MAX_EDIT_PAIRS:
            break
    return {'sent_at': message['sentDateTime'], 'changed': changed}


def recent_owner_edits(tasks, rows, root):
    from .registry import people

    recent = []
    for row in rows:
        edit = row['data'].get('owner_edit') or {}
        sent_at = timestamp(edit.get('sent_at'))
        if sent_at:
            recent.append((sent_at, row, edit))
    if not recent:
        return []
    now = datetime.fromtimestamp(tasks.clock(), timezone.utc)
    cutoff = now - timedelta(days=14)
    recent = [item for item in recent if cutoff <= item[0] <= now]
    if not recent:
        return []
    persons = people(root) if (Path(root) / 'tables' / 'contacts.json').is_file() else []
    names = {key: person['name'] for person in persons
             for key in (*person['emails'], *(record['id'] for record in person['records']))}
    entries = []
    for _, row, edit in sorted(recent, key=lambda item: item[0], reverse=True):
        data = row['data']
        packet = data.get('packet_override') or (data.get('result') or {}).get('packet') or {}
        observation = effective_inputs(row).get('mailbox_observation') or {}
        address = (observation.get('email') or '').strip().casefold()
        name = names.get(address) or names.get(row['owner'])
        if not name:
            name = next((names[address] for address in packet.get('to', []) if address in names),
                        observation.get('name') or row['owner'])
        entries.append({'task_id': row['id'], 'correspondent': name,
                        'sent_at': edit['sent_at'], 'pairs': edit['changed']})
    return entries


def _present(draft, packet, receipt, anchor):
    count = None
    attachments = draft.get('attachments')
    if isinstance(attachments, list) and not draft.get('attachments@odata.nextLink'):
        count = sum(not item.get('isInline') for item in attachments)
    elif draft.get('hasAttachments') is False:
        count = 0
    content, html = _body_content(draft)
    edited = placeholder = event = None
    if content is not None:
        text = _body_text(content, html)
        original = packet.get('body')
        if isinstance(original, str) and receipt.get('content_hash') == digest(packet):
            edited = text != _body_text(original, False)
        placeholder = bool(PLACEHOLDER.search(text))
        event = event_date((draft.get('subject') or '') + '\n' + text, anchor)
    return {'draft_status': 'present', 'lastModifiedDateTime': draft.get('lastModifiedDateTime'),
            'owner_edited': edited, 'has_placeholder': placeholder, 'attachment_count': count,
            'event_date': event, 'draft_body_checked': content is not None}


def refresh(ledger, config, mailbox, sent_messages, now=None):
    """Project one shared Drafts listing and bounded Sent pages onto saved receipts."""
    tasks = ledger.tasks
    previous = inventory(tasks)
    rows = [tasks.get(task_id) for task_id, in tasks.db.execute("""
        SELECT id FROM tasks WHERE state='owner-draft' AND json_type(data, '$.draft_receipt')='object'
    """)]
    rows = [row for row in rows if row['data']['draft_receipt']]
    drafts = None
    error = None
    if rows and mailbox is not None and not config['paused']:
        with unit(ledger, KEY, 'drafts-folder') as ready:
            if ready:
                try:
                    mailbox.verify_owner()
                    if isinstance(mailbox, GraphMailbox) and getattr(ledger, 'tick', None):
                        listing = mailbox.reply_drafts(
                            before_next=lambda: allowed(ledger, KEY, 'drafts-folder'))
                    else:
                        listing = mailbox.reply_drafts()
                    require(isinstance(listing, list) and all(m.get('id') for m in listing),
                            'Incomplete Drafts listing')
                    drafts = {m['id']: m for m in listing if m.get('isDraft') is not False}
                except DraftsScanDeferred:
                    pass
                except Exception as exc:
                    error = type(exc).__name__
    now = now or datetime.fromtimestamp(tasks.clock(), timezone.utc)

    sent_by_id, latest_sent = {}, {}
    for message in sent_messages:
        at = timestamp(message.get('sentDateTime'))
        conversation = message.get('conversationId')
        if not conversation or not at or message.get('isDraft'):
            continue
        if message.get('id'):
            sent_by_id[message['id']] = message
        if conversation not in latest_sent or at > latest_sent[conversation]:
            latest_sent[conversation] = at

    current = {}
    for row in rows:
        data = row['data']
        receipt = data['draft_receipt']
        identity = receipt.get('remote_id')
        old = previous.get(row['id'], {})
        if old.get('remote_id') != identity:
            old = {}
        draft = drafts.get(identity, {}) if drafts is not None else {}
        sent_copy = sent_by_id.get(identity, {})
        packet = data.get('packet_override') or (data.get('result') or {}).get('packet') or {}
        conversation = (draft.get('conversationId') or receipt.get('conversationId') or
                        old.get('draft_conversation') or sent_copy.get('conversationId'))
        actual_conversation = conversation
        conversation = conversation or packet.get('thread_id') or effective_inputs(row).get('thread_id')
        saved = (timestamp(draft.get('createdDateTime')) or timestamp(old.get('draft_saved_at')) or
                 saved_at(data, receipt) or timestamp(sent_copy.get('createdDateTime')))
        sent_at = latest_sent.get(conversation)
        if conversation and conversation == old.get('sent_conversation'):
            sent_at = max(filter(None, (sent_at, timestamp(old.get('later_sent_at')))), default=None)
        observation = {**old, 'remote_id': identity, 'draft_conversation': actual_conversation,
                       'draft_saved_at': saved.isoformat() if saved else None,
                       'sent_conversation': conversation,
                       'later_sent_at': sent_at.isoformat() if saved and sent_at and sent_at > saved else None,
                       'draft_stale': drafts is None or not identity}
        if drafts is not None and identity:
            if draft:
                observation.update(_present(draft, packet, receipt, saved or now))
            else:
                for key in ('lastModifiedDateTime', 'owner_edited', 'has_placeholder',
                            'attachment_count', 'event_date', 'draft_body_checked'):
                    observation.pop(key, None)
                observation['draft_status'] = 'sent' if observation['later_sent_at'] else 'missing'
            observation['draft_checked_at'] = now.isoformat()
        current[row['id']] = observation
    if current != previous:
        tasks.db.execute('INSERT OR REPLACE INTO pipeline_state VALUES(?,?)', (KEY, canonical(current)))
    return error
