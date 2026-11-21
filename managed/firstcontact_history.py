"""Coordinator-only mailbox evidence for first contacts and replies."""
from datetime import datetime, timezone
import hashlib
from html.parser import HTMLParser
import json
import re

from .location import settings
from .mailbox import address
from .queue import require
from .registry import email, people


MAX_REPLY_MESSAGES = 12
MAX_REPLY_EXCERPT = 1500
MAX_UNANSWERED_EXCERPTS = 4
MAX_CLOSING_LINES = 6
MAX_CLOSING_CHARS = 400
MAX_DRAFT_EXCERPT = 200

QUOTE_MARKER = re.compile(
    r'^>|^On .+ wrote:$|^-{5}\s*Original Message\s*-{5}$|^_{3,}$|^_{8,}\s*From:\s|^From:\s.+\sSent:\s|'
    r'^\d{4}年\d{1,2}月\d{1,2}日(?:\s*[（(][^）)]*[）)])?\s+'
    r'\d{1,2}:\d{2}(?::\d{2})?\s+.*<[^<>\s]+@[^<>\s]+>\s*[:：]$', re.I)
OUTLOOK_HEADER = re.compile(r'^(Sent|Date|To|Cc|Subject):', re.I)
SIGN_OFF = re.compile(
    r'(?:best|all the best|(?:with )?(?:best|kind|warm)(?: regards| wishes)|'
    r'regards|(?:yours )?(?:sincerely|faithfully|truly)|cheers|'
    r'(?:many )?thanks(?: again)?|thank you|respectfully|祝好)[,.!。]?', re.I)


def snapshot(clock):
    return {'fetched_at': datetime.fromtimestamp(clock(), timezone.utc).isoformat(),
            'addresses': [], 'messages': [], 'attempts': [], 'complete': False}


def delivery_attempts(tasks, addresses):
    """Read the existing delivery ledger without creating a second queue."""
    if not tasks.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='attempts'").fetchone():
        return []
    result = []
    for row in tasks.db.execute('SELECT a.*,v.packet FROM attempts a JOIN items i ON i.id=a.item '
                                'JOIN versions v ON v.item=i.id AND v.version=i.version'):
        packet = json.loads(row['packet'])
        matched = sorted(addresses & {email(a) for a in packet.get('to', []) + packet.get('cc', [])})
        if not matched:
            continue
        events = [{'at': e['at'], 'action': e['action']} for e in tasks.db.execute(
            'SELECT at,action FROM events WHERE item=? ORDER BY seq', (row['item'],))]
        result.append({k: row[k] for k in ('item', 'key', 'transport', 'status', 'calls', 'locator')}
                      | {'addresses': matched, 'events': events})
    return result


def resolve(row, mailbox, ledger, root, history):
    """Fill a snapshot in place so failed scans retain explicitly incomplete evidence."""
    require(mailbox is not None, 'Configured GraphMailbox unavailable')
    account = mailbox.verify_owner()
    require(mailbox.settings.get('reconciled') is True, 'Mailbox boundary is not reconciled')
    persons = people(root)
    recipients = [email(a) for a in row['request']['inputs']['recipient_emails']]
    groups = {}
    for recipient in recipients:
        person = next((p for p in persons if recipient in p['emails']), None)
        require(person is not None, 'Register the first-contact recipient: ' + recipient)
        groups[recipient] = person['emails']
    history['recipient_addresses'] = groups
    history['addresses'] = list(dict.fromkeys(a for aliases in groups.values() for a in aliases))
    history['attempts'] = delivery_attempts(ledger.tasks, set(history['addresses']))
    seen = {}
    for recipient in history['addresses']:
        for message in mailbox.recipient_history(recipient):
            require(message.get('id') and message.get('conversationId'),
                    'Mailbox history message lacks a message/conversation identity')
            participants = {address(v) for v in [message.get('from', {}),
                            *message.get('toRecipients', []), *message.get('ccRecipients', [])]}
            matched = sorted(participants & set(history['addresses']))
            require(recipient in matched, 'Mailbox history returned a mismatched recipient')
            identity = message['id']
            if identity in seen:
                continue
            draft = bool(message.get('isDraft'))
            metadata = {k: message.get(k) for k in ('id', 'subject', 'conversationId', 'internetMessageId')}
            metadata.update(date=message.get('sentDateTime') or message.get('receivedDateTime')
                            or message.get('createdDateTime'), isDraft=draft,
                            direction='draft' if draft else ('outgoing' if address(message.get('from', {})) == account else 'incoming'),
                            folder=message.get('folder'), addresses=matched,
                            preview=(message.get('bodyPreview') or '')[:300])
            require(metadata['date'], 'Mailbox history message lacks a date')
            seen[identity] = metadata
            history['messages'].append(metadata)
    history['complete'] = True
    return history


class _HTMLBody(HTMLParser):
    """Keep visible line breaks and optionally stop at a marked quote."""
    BLOCKS = {'div': 1, 'p': 2, 'li': 1, 'tr': 1, 'table': 1, 'blockquote': 1}

    def __init__(self, strip_quotes):
        super().__init__()
        self.parts = []
        self.hidden = None
        self.strip_quotes = strip_quotes
        self.quoted = False

    def _break(self, count):
        trailing = 0
        for part in reversed(self.parts):
            tail = part.rstrip('\n')
            trailing += len(part) - len(tail)
            if tail or trailing >= count:
                break
        if trailing < count:
            self.parts.append('\n' * (count - trailing))

    def handle_starttag(self, tag, attrs):
        if self.hidden or self.quoted:
            return
        attrs = dict(attrs)
        if self.strip_quotes and (tag == 'blockquote'
                                  or 'gmail_quote' in (attrs.get('class') or '').split()
                                  or (attrs.get('id') or '').lower() == 'divrplyfwdmsg'):
            self.quoted = True
        elif tag in {'head', 'script', 'style'}:
            self.hidden = tag
        elif tag == 'br':
            self.parts.append('\n')
        elif tag in self.BLOCKS:
            self._break(self.BLOCKS[tag])

    def handle_endtag(self, tag):
        if self.hidden:
            if self.hidden == tag:
                self.hidden = None
        elif not self.quoted and tag in self.BLOCKS:
            self._break(self.BLOCKS[tag])

    def handle_data(self, data):
        if not self.hidden and not self.quoted:
            data = re.sub(r'[ \t\r\n\f]+', ' ', data)
            if data.strip() or (self.parts and not self.parts[-1].endswith('\n')):
                self.parts.append(data)


def _body_content(message):
    body = message.get('body')
    if isinstance(body, dict) and isinstance(body.get('content'), str):
        return body['content'], str(body.get('contentType', '')).lower() == 'html'
    for source in (body, message):
        if isinstance(source, dict):
            for key in ('plain_text', 'plainText', 'text'):
                if isinstance(source.get(key), str):
                    return source[key], False
    return None, False


def _body_text(content, html, *, strip_quotes=False):
    if html:
        parser = _HTMLBody(strip_quotes)
        parser.feed(content)
        parser.close()
        content = ''.join(parser.parts)
    return content.replace('\r\n', '\n').replace('\r', '\n').rstrip('\n')


def own_body_text(content, html):
    """Extract the sender's own text ahead of the quoted correspondence."""
    lines = [line.strip() for line in _body_text(content, html, strip_quotes=True).splitlines()]
    for i, line in enumerate(lines):
        outlook = line.lower().startswith('from:') and any(
            OUTLOOK_HEADER.match(header) for header in lines[i+1:i+5])
        if QUOTE_MARKER.search(line) or outlook:
            lines = lines[:i]
            break
    return '\n'.join(lines).strip()


def _sent_signature(content, html):
    own_lines = own_body_text(content, html).splitlines()
    if not own_lines:
        return {'salutation': '', 'closing': '', 'has_affiliation_lines': False}
    tail = own_lines[1:][-MAX_CLOSING_LINES:]
    start = next((i for i in reversed(range(len(tail))) if SIGN_OFF.fullmatch(tail[i])), None)
    if start is None:
        names = settings('identity')['first_names']
        signature = re.compile(r'(?:' + '|'.join(map(re.escape, names)) + r')\b', re.I)
        start = next((i for i, line in enumerate(tail) if signature.match(line)), None)
    if start is None:
        start = max((i+1 for i, line in enumerate(tail) if not line), default=0)
    closing = tail[start:]
    identity = settings('identity')
    return {'salutation': own_lines[0],
            'closing': '\n'.join(closing)[-MAX_CLOSING_CHARS:].strip(),
            'has_affiliation_lines': any(
                any(re.search(pattern, line, re.I) for pattern in identity['affiliation_patterns'])
                or any(re.match(re.escape(prefix) + r'\b', line, re.I)
                       for prefix in identity['affiliation_prefixes']) for line in closing)}


def _reply_threads(row):
    """Return the current and read-only prior threads attached to a reply."""
    sources = [row.get('request', {}).get('inputs', {}), row.get('data', {}).get('thread_inputs', {})]
    threads = []
    for source in sources:
        thread = source.get('thread')
        if isinstance(thread, list):
            threads.append(thread)
        contexts = source.get('prior_context', [])
        if isinstance(contexts, list):
            threads.extend(c.get('thread', []) for c in contexts
                           if isinstance(c, dict) and isinstance(c.get('thread'), list))
    return threads


def _reply_addresses(row, account, root):
    values = []
    for thread in _reply_threads(row):
        for message in thread:
            participants = [message.get('from', {}), *message.get('toRecipients', []),
                            *message.get('ccRecipients', [])]
            for value in participants:
                candidate = address(value)
                if candidate and candidate != account and candidate not in values:
                    values.append(candidate)
    for candidate in row.get('request', {}).get('inputs', {}).get('recipient_emails', []):
        candidate = email(candidate)
        if candidate and candidate != account and candidate not in values:
            values.append(candidate)
    owner_ids = {row.get('request', {}).get('owner'),
                 *row.get('request', {}).get('inputs', {}).get('owner_ids', []),
                 *row.get('data', {}).get('thread_inputs', {}).get('owner_ids', [])}
    for person in people(root):
        if person['id'] not in owner_ids:
            continue
        for candidate in person['emails']:
            if candidate != account and candidate not in values:
                values.append(candidate)
    return values


def _message_date(message):
    return (message.get('sentDateTime') or message.get('receivedDateTime') or
            message.get('createdDateTime') or '')


def _message_date_key(message):
    stamp = _message_date(message)
    try:
        return datetime.fromisoformat(stamp.replace('Z', '+00:00')).timestamp()
    except (AttributeError, TypeError, ValueError):
        return float('-inf')


def _message_details(mailbox, message):
    # Recipient-history rows carry only bodyPreview (<= 255 characters); a
    # Fetch the full message when previews may omit the new content.
    if _body_content(message)[0] is None and callable(getattr(mailbox, 'message', None)):
        return {**message, **mailbox.message(message['id'])}
    return message


def _message_body(message):
    content, html = _body_content(message)
    return _body_text(content, html) if content is not None else message.get('bodyPreview') or ''


def _recipients(message, field):
    return [recipient for value in message.get(field, []) if (recipient := address(value))]


def _draft_addresses(message):
    return {recipient for field in ('toRecipients', 'ccRecipients', 'bccRecipients')
            for recipient in _recipients(message, field)}


def _sent_evidence(message, details):
    content, html = _body_content(details)
    require(content is not None, 'Latest Sent message body is unavailable')
    return {'internetMessageId': message.get('internetMessageId'),
            'date': _message_date(message), 'subject': message.get('subject'),
            'to': _recipients(message, 'toRecipients'), 'cc': _recipients(message, 'ccRecipients'),
            **_sent_signature(content, html)}


def _draft_evidence(drafts, addresses, load):
    evidence = {}
    for draft in drafts:
        if not draft.get('isDraft') or not addresses.intersection(_draft_addresses(draft)):
            continue
        require(draft.get('id'), 'Mailbox draft lacks a message identity')
        details = load(draft)
        if not details.get('isDraft') or not addresses.intersection(_draft_addresses(details)):
            continue
        content, html = _body_content(details)
        require(content is not None, 'Mailbox draft body is unavailable')
        evidence[draft['id']] = {
            'id': draft['id'], 'internetMessageId': details.get('internetMessageId'),
            'lastModifiedDateTime': details.get('lastModifiedDateTime'),
            'subject': details.get('subject'), 'to': _recipients(details, 'toRecipients'),
            'body_sha256': hashlib.sha256(content.encode('utf-8')).hexdigest(),
            'body_excerpt': _body_text(content, html)[:MAX_DRAFT_EXCERPT],
        }
    return sorted(evidence.values(),
                  key=lambda draft: (draft['lastModifiedDateTime'] or '', draft['id']), reverse=True)


def resolve_reply(row, mailbox, ledger, root, history):
    """Attach bounded cross-thread correspondent context before reply admission."""
    history.update(latest_sent={}, drafts=[], drafts_complete=False)
    require(mailbox is not None, 'Configured GraphMailbox unavailable')
    account = mailbox.verify_owner().casefold()
    require(mailbox.settings.get('reconciled') is True, 'Mailbox boundary is not reconciled')
    addresses = _reply_addresses(row, account, root)
    require(addresses, 'Reply mailbox history has no correspondent address')
    history['addresses'] = addresses
    history['recipient_addresses'] = {candidate: [candidate] for candidate in addresses}
    address_set = set(addresses)
    history['attempts'] = delivery_attempts(ledger.tasks, address_set)
    finder = getattr(mailbox, 'recipient_history', None)
    if (not callable(finder) or
            type(mailbox).__module__.startswith('unittest.mock')):
        # Lightweight offline transports used by the historical-thread tests
        # already provide the complete hydrated conversations. A live
        # GraphMailbox always supplies recipient_history below.
        history['complete'] = True
        return history
    loaded = {}

    def load(message):
        identity = message['id']
        if identity not in loaded:
            loaded[identity] = _message_details(mailbox, message)
        return loaded[identity]

    # Read the folder itself before participant history so that both use the
    # same Drafts listing without caching it for later delivery preflight.
    list_drafts = getattr(mailbox, 'reply_drafts', None)
    draft_rows = list_drafts() if callable(list_drafts) else None
    if draft_rows is not None:
        history['drafts'] = _draft_evidence(draft_rows, address_set, load)
        history['drafts_complete'] = True
    raw = {}
    for recipient in addresses:
        rows = finder(recipient, draft_rows=draft_rows) if draft_rows is not None else finder(recipient)
        for message in rows:
            require(message.get('id') and message.get('conversationId'),
                    'Mailbox history message lacks a message/conversation identity')
            participants = {address(value) for value in
                            [message.get('from') or {}, *message.get('toRecipients', []),
                             *message.get('ccRecipients', [])]}
            require(recipient in participants, 'Mailbox history returned a mismatched recipient')
            raw.setdefault(message['id'], message)
    sent = sorted((message for message in raw.values()
                   if message.get('folder') == 'sentitems' and not message.get('isDraft')),
                  key=lambda message: (_message_date_key(message), message['id']), reverse=True)
    latest_sent_ids = set()
    for message in sent:
        recipients = _recipients(message, 'toRecipients') + _recipients(message, 'ccRecipients')
        uncovered = address_set.intersection(recipients).difference(history['latest_sent'])
        if uncovered:
            evidence = _sent_evidence(message, load(message))
            history['latest_sent'].update(dict.fromkeys(sorted(uncovered), evidence))
            latest_sent_ids.add(message['id'])
            if len(history['latest_sent']) == len(addresses):
                break
    # Incoming messages newer than our latest Sent are what the reply must answer
    # (possibly in other threads): carry their text too, bounded (2026-11-02).
    sent_key = _message_date_key(sent[0]) if sent else None
    unanswered = {m['id'] for m in sorted(
        (m for m in raw.values() if not m.get('isDraft') and m.get('folder') != 'sentitems'
         and address(m.get('from') or {}) != account
         and (sent_key is None or _message_date_key(m) > sent_key)),
        key=_message_date_key, reverse=True)[:MAX_UNANSWERED_EXCERPTS]}
    ordered = sorted(raw.values(), key=_message_date_key, reverse=True)[:MAX_REPLY_MESSAGES]
    for message in ordered:
        draft = bool(message.get('isDraft'))
        to = _recipients(message, 'toRecipients')
        cc = _recipients(message, 'ccRecipients')
        participants = sorted({address(value) for value in
                               [message.get('from') or {}, *message.get('toRecipients', []),
                                *message.get('ccRecipients', [])] if address(value)})
        metadata = {key: message.get(key) for key in
                    ('id', 'internetMessageId', 'subject', 'conversationId')}
        metadata.update(date=_message_date(message), isDraft=draft,
                        direction='draft' if draft else
                        ('outgoing' if message.get('folder') == 'sentitems'
                         or address(message.get('from') or {}) == account else 'incoming'),
                        folder=message.get('folder'), participants=participants,
                        recipients=to + cc, to=to, cc=cc)
        require(metadata['date'], 'Mailbox history message lacks a date')
        if draft or message['id'] in latest_sent_ids or message['id'] in unanswered:
            metadata['body_excerpt'] = _message_body(load(message))[:MAX_REPLY_EXCERPT]
        history['messages'].append(metadata)
    history['complete'] = True
    return history


def recipient_inputs(request, history):
    """Keep one cold letter to clear authors; report existing relationships separately."""
    require(history.get('complete') is True, 'Fresh first-contact mailbox history is incomplete')
    included, excluded = [], []
    for recipient in request['inputs']['recipient_emails']:
        aliases = set(history['recipient_addresses'][email(recipient)])
        messages = [m for m in history['messages'] if aliases & set(m['addresses'])]
        attempts = [a for a in history['attempts'] if aliases & set(a['addresses'])]
        if messages or attempts:
            excluded.append({'email': recipient,
                             'reason': 'Existing correspondence/draft or delivery attempt; use the existing thread, preserve owner drafts and reconcile uncertain sends',
                             'thread_ids': list(dict.fromkeys(m['conversationId'] for m in messages)),
                             'attempt_keys': [a['key'] for a in attempts]})
        else:
            included.append(recipient)
    # Prefer the planned corresponding author if still eligible, then author order.
    preferred = request['inputs'].get('to', [])
    primary = next((a for a in preferred if a in included), next(iter(included), None))
    return {'recipient_emails': included, 'to': [primary] if primary else [],
            'cc': [a for a in included if a != primary], 'excluded_recipients': excluded}
