"""Microsoft Graph mailbox boundary; credentials come from the runtime only.

This adapter is inactive until mailbox reconciliation and live authentication
are explicitly configured. It never creates an OAuth application or refreshes
an unsupported session. All reads use the server, including Sent and Drafts.
"""
import base64
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from functools import lru_cache
import hashlib
from http.client import HTTPException
import json
import os
from pathlib import Path
import re
import tempfile
import time
import unicodedata
from urllib.parse import parse_qs, quote, urlencode, urlsplit
from urllib.error import HTTPError
from urllib.request import Request, build_opener

from .location import source_head
from .cma import NoRedirect
from .queue import GateError, canonical, digest, require
from .transports import NotSent, Readback, Submission


FILE_ATTACHMENT = '#microsoft.graph.fileAttachment'
MAX_ATTACHMENT_BYTES = 30 * 1024 * 1024
UPLOAD_THRESHOLD = 3 * 1024 * 1024
DEFAULT_TOTAL_ATTACHMENT_BYTES = 25 * 1024 * 1024
UPLOAD_CHUNK_BYTES = 12 * 320 * 1024
UPLOAD_ATTEMPTS = 3
ATTACHMENT_SIZE_OVERHEAD = 64 * 1024
UPLOAD_HOSTS = (
    'outlook.office.com', 'outlook.office365.com', 'graph.microsoft.com',
    'outlook.office365.us', 'outlook-dod.office365.us', 'graph.microsoft.us',
    'dod-graph.microsoft.us', 'partner.outlook.cn', 'microsoftgraph.chinacloudapi.cn',
)


class _UploadRequestError(GateError):
    def __init__(self, method, status=None):
        self.status = status
        detail = 'connection failure' if status is None else f'HTTP {status}'
        super().__init__(f'Graph attachment upload {method} failed ({detail})')

    @property
    def retryable(self):
        return self.status in {None, 408, 416, 429, 500, 502, 503, 504}


def _upload_offset(response):
    ranges = response.get('nextExpectedRanges') if isinstance(response, dict) else None
    require(isinstance(ranges, list) and len(ranges) == 1 and isinstance(ranges[0], str),
            'Graph attachment upload did not return one expected range')
    match = re.fullmatch(r'([0-9]{1,12})(?:-[0-9]*)?', ranges[0])
    require(match, 'Graph attachment upload returned an invalid expected range')
    return int(match[1])


class UnsupportedThreadAttachments(GateError):
    """A whole thread needs operator selection; retain metadata, never bytes."""

    def __init__(self, attachments):
        self.attachments = attachments
        super().__init__('Thread attachments need explicit supported input selection: ' +
                         json.dumps(attachments, ensure_ascii=False))


def _attachment_issue(message, attachment, size):
    return {'message_id': message['id'], 'name': attachment.get('name'),
            'type': attachment.get('@odata.type'), 'size': size}


class DraftsScanDeferred(Exception):
    """The current tick has no time for another Drafts page."""


class SentSubjectsDeferred(Exception):
    """Subject matching needs another intake tick."""

    def __init__(self, intake_reason, category_reason):
        self.intake_reason = intake_reason
        super().__init__(category_reason)


SENT_SUBJECTS_KEY = 'mailbox-sent-subjects'
SENT_SUBJECT_FIELDS = ('id', 'sentDateTime', 'toRecipients', 'ccRecipients', 'subject',
                       'internetMessageId', 'conversationId')


class Graph:
    BASE = 'https://graph.microsoft.com/v1.0'

    def __init__(self, token):
        require(bool(token), 'A live Graph access token is not configured')
        self.token = token

    def request(self, path, method='GET', body=None, *, raw_response=False):
        require(path.startswith('/') and not path.startswith('//') and '..' not in path,
                'Use a Graph path on the configured origin')
        headers = {'Authorization':'Bearer '+self.token, 'User-Agent':'trureturing-outreach/managed',
                   'Prefer':'outlook.body-content-type="text", IdType="ImmutableId"'}
        if '$search' in parse_qs(urlsplit(path).query):
            headers['ConsistencyLevel'] = 'eventual'
        raw = None
        if body is not None:
            headers['Content-Type'] = 'application/json'
            raw = json.dumps(body,ensure_ascii=False).encode()
        # Graph $search over Sent Items/Drafts intermittently stalls past 30 s
        # (2026-11-13). Reads are idempotent: retry them; never repeat a write.
        attempts = 3 if method == 'GET' else 1
        for attempt in range(attempts):
            try:
                with build_opener(NoRedirect()).open(Request(self.BASE+path,data=raw,headers=headers,method=method),
                                                     timeout=60) as response:
                    data = response.read()
                    if raw_response:
                        return data
                    return json.loads(data) if data.strip() else {}
            except (TimeoutError, HTTPError) as exc:
                transient = isinstance(exc, TimeoutError) or getattr(exc, 'code', None) in {502, 503, 504}
                if not transient or attempt == attempts - 1:
                    raise
                time.sleep(2 * (attempt + 1))

    def upload_request(self, url, method, chunk=None, *, offset=0, size=0):
        # The session URL contains its own credential. Keep it out of errors,
        # and keep Graph's bearer token on the authenticated request path.
        require(isinstance(url, str) and not re.search(r'[\x00-\x20\x7f\\]', url),
                'Invalid Graph attachment upload URL')
        try:
            parsed = urlsplit(url)
            host = parsed.hostname or ''
            allowed = (parsed.scheme == 'https' and parsed.port in (None, 443)
                       and parsed.username is None and not parsed.fragment
                       and any(host == domain or host.endswith('.' + domain) for domain in UPLOAD_HOSTS))
        except ValueError:
            raise GateError('Invalid Graph attachment upload URL') from None
        require(allowed, 'Graph attachment upload URL must use HTTPS on an allowed Microsoft host')
        headers = {}
        if chunk is not None:
            headers = {'Content-Type': 'application/octet-stream', 'Content-Length': str(len(chunk)),
                       'Content-Range': f'bytes {offset}-{offset + len(chunk) - 1}/{size}'}
        try:
            request = Request(url, data=chunk, headers=headers, method=method)
            with build_opener(NoRedirect()).open(request, timeout=60) as response:
                status = response.status
                return status, {} if status == 201 else json.loads(response.read())
        except HTTPError as exc:
            exc.close()
            raise _UploadRequestError(method, exc.code) from None
        except (OSError, HTTPException):
            raise _UploadRequestError(method) from None
        except RuntimeError:
            raise GateError('Graph attachment upload redirect refused') from None
        except ValueError:
            raise GateError('Invalid Graph attachment upload response') from None

    def pages(self, path, *, before_next=None):
        rows = []
        for _ in range(200):
            page = self.request(path)
            rows.extend(page['value'])
            link = page.get('@odata.nextLink')
            if not link:
                return rows
            require(link.startswith(self.BASE+'/'), 'Graph pagination changed origin')
            if before_next is not None and not before_next():
                raise DraftsScanDeferred
            path = link[len(self.BASE):]
        raise ValueError('Mailbox scan incomplete; preserve the cursor and inspect pagination')

    def mime(self, path, content):
        require(path=='/me/messages', 'MIME creation is limited to mailbox drafts')
        headers={'Authorization':'Bearer '+self.token,'Content-Type':'text/plain',
                 'User-Agent':'trureturing-outreach/managed','Prefer':'IdType="ImmutableId"'}
        with build_opener(NoRedirect()).open(Request(self.BASE+path,data=content,headers=headers,method='POST'),timeout=30) as response:
            return json.loads(response.read())


def address(value):
    return (((value or {}).get('emailAddress') or {}).get('address') or '').strip().lower()


def _alias_name_tokens(value):
    value = unicodedata.normalize('NFKD', value.casefold())
    value = ''.join(c for c in value if not unicodedata.combining(c))
    tokens = re.findall(r'[^\W\d_]+', value)
    while tokens and tokens[0] in {'professor', 'prof', 'dr'}:
        tokens.pop(0)
    return tokens


def _alias_names_match(sender_name, registered_name):
    left, right = map(_alias_name_tokens, (sender_name, registered_name))
    if not left or not right:
        return False
    if left == right:
        return True
    if min(len(left), len(right)) < 2 or left[0] != right[0] or left[-1] != right[-1]:
        return False

    def middles_match(a, b):
        if not a or not b:
            return all(len(token) == 1 for token in a + b)
        compatible = (a[0] == b[0] or
                      (min(len(a[0]), len(b[0])) == 1 and a[0][0] == b[0][0]))
        return ((compatible and middles_match(a[1:], b[1:])) or
                (len(a) > len(b) and len(a[0]) == 1 and middles_match(a[1:], b)) or
                (len(b) > len(a) and len(b[0]) == 1 and middles_match(a, b[1:])))

    return middles_match(left[1:-1], right[1:-1])


@lru_cache(maxsize=1)
def _public_suffix_rules():
    # Pin the official PSL locally: intake must not depend on a live lookup or
    # mistake a shared suffix (co.uk, github.io, etc.) for an institution.
    rules = set()
    for line in Path(__file__).with_name('public_suffix_list.dat').read_text().splitlines():
        line = line.strip()
        if line and not line.startswith('//'):
            rules.add(line.encode('idna').decode('ascii').lower())
    return rules


def _registrable_domain(email):
    domain = email.rpartition('@')[2].rstrip('.')
    if not domain:
        return None
    try:
        labels = domain.encode('idna').decode('ascii').lower().split('.')
    except UnicodeError:
        return None
    if len(labels) < 2 or any(not re.fullmatch(r'[a-z0-9](?:[a-z0-9-]*[a-z0-9])?', p) for p in labels):
        return None
    rules = _public_suffix_rules()
    suffix_length = 1  # PSL's implicit default rule is *.
    for index in range(len(labels)):
        suffix = '.'.join(labels[index:])
        if '!' + suffix in rules:
            suffix_length = len(labels) - index - 1
            break
        if suffix in rules:
            suffix_length = max(suffix_length, len(labels) - index)
        if index > 0 and '*.' + suffix in rules:
            suffix_length = max(suffix_length, len(labels) - index + 1)
    return '.'.join(labels[-suffix_length-1:]) if len(labels) > suffix_length else None


def _alias_recipient(message, sent, persons, account):
    """Identify the alias among this Sent item's recipients, independently of owner."""
    recipients = {address(a) for a in sent.get('toRecipients', []) + sent.get('ccRecipients', [])}
    recipients.discard('')
    recipients.discard(account)
    candidates = [p for p in persons if recipients.intersection(p['emails'])]
    sender_name = ((message.get('from') or {}).get('emailAddress') or {}).get('name') or ''
    named = [p for p in candidates if _alias_names_match(sender_name, p['name'])]
    if len(named) == 1:
        return named[0]
    if named:  # A domain cannot disambiguate conflicting display-name matches.
        return None
    domain = _registrable_domain(address(message.get('from', {})))
    if domain is None:
        return None
    matching = {a for a in recipients if _registrable_domain(a) == domain}
    domain_people = [p for p in candidates if matching.intersection(p['emails'])]
    if len(domain_people) == 1 and matching <= set(domain_people[0]['emails']):
        return domain_people[0]
    return None


def _message_headers(message):
    return {str(h.get('name', '')).casefold(): str(h.get('value', '')).strip()
            for h in message.get('internetMessageHeaders', [])
            if h.get('name')}


def _header_message_ids(message):
    """Return Internet Message-IDs named by In-Reply-To or References."""
    headers = _message_headers(message)
    values = [headers.get(name, '') for name in ('in-reply-to', 'references')]
    result = []
    for value in values:
        # References is whitespace separated, while a few Graph imports use
        # comma separation.  Keep only real Internet IDs and normalize bare
        # values to the same angle-bracket form used by the registry.
        for raw in re.findall(r'<[^<>\s]+>|[^\s,<>]+@[^\s,<>]+', value):
            identity=raw if raw.startswith('<') else '<' + raw.rstrip('.,;') + '>'
            if identity not in result:
                result.append(identity)
    return result


def body_text(value):
    body = value.get('body', {})
    require(body.get('contentType', '').lower() == 'text', 'Graph did not return plain-text body')
    return body.get('content', '').replace('\r\n','\n').rstrip('\n')


def reply_recipient_exception(packet, latest, account, thread=(), team=()):
    """Keep reply-all, allowing literal latest-body additions/redirects and
    re-adding people we ourselves addressed earlier in the same thread."""
    from .registry import body_addresses
    sender = address(latest['from'])
    expected = {sender} | {address(a) for a in latest.get('toRecipients', []) + latest.get('ccRecipients', [])}
    expected.discard(account)
    actual = set(packet['to'] + packet['cc'])
    added, removed = actual - expected, expected - actual
    if not added and not removed:
        return None
    mentioned = body_addresses(latest)
    ours = {address(a) for m in thread if not m.get('isDraft') and address(m.get('from', {})) == account
            for a in m.get('toRecipients', []) + m.get('ccRecipients', [])}
    ours.discard(account)
    ours |= {a.lower() for a in team}  # our own co-author may always be copied
    text = (latest.get('body') or {}).get('content', '')
    # A mere mention permits an addition. Dropping the sender additionally
    # requires an explicit request to use one of the added addresses.
    redirected = any(re.search(
        r'(?:^|[.!?\n]\s*|\bplease\s+|\b(?:could|can) you\s+|\byou can\s+)'
        r'(?![^\n.!?]*\b(?:not|never|don\x27t)\b)'
        r'(?:(?:send|forward)\b[^\n.!?]{0,160}\bto\s+(?:me\s+(?:at|on)\s*)?|'
        r'(?:contact|reach|e-?mail)\s+me\s+(?:at|on)|'
        r'(?:reply|respond|write|use)\s+(?:(?:to|at)\s+)?(?:me\s+(?:at|on)\s*)?)\s*<?' + re.escape(a) +
        r'(?![\w@+-]|\.[\w-])',
        text, re.IGNORECASE) for a in added)
    require(added <= mentioned | ours and (not removed or (removed == {sender} and added and redirected)),
            'Reply-all recipients differ from the latest incoming message')
    return {'basis': 'latest-incoming-body' if added <= mentioned else 'earlier-sent-in-thread',
            'message_id': latest.get('id'),
            'internet_message_id': latest['internetMessageId'],
            'added': sorted(added), 'replaced': sorted(removed)}


def replies_in_thread(message, packet):
    """Our mailbox may file a reply to an external message without a
    Thread-Index (e.g. Coremail) under a new Exchange conversation. The
    recipient threads by In-Reply-To, so an exact In-Reply-To to the reviewed
    latest incoming message is equally sufficient."""
    if message.get('conversationId') == packet['thread_id']:
        return True
    values = [h['value'].strip() for h in message.get('internetMessageHeaders') or []
              if h['name'].lower() == 'in-reply-to']
    return values == [packet['latest_inbound_message_id']]


def instant(value):
    stamp=datetime.fromisoformat(value.replace('Z','+00:00'))
    require(stamp.tzinfo is not None,'Mailbox timestamps require a timezone')
    return stamp


def utc_stamp(value):
    return value.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')


def _compact_sent_subject_row(sent):
    row = {key: sent[key] for key in SENT_SUBJECT_FIELDS
           if key in sent and (sent[key] is None or isinstance(sent[key], str))}
    for key in ('toRecipients', 'ccRecipients'):
        row[key] = [{'emailAddress': {'address': address(value)}} for value in sent.get(key, [])]
    return row


def child_thread_index(value, now):
    """Append an MS-OXOMSG ConversationIndex response level to the parent."""
    try:
        parent=base64.b64decode(''.join(value.split()),validate=True)
    except (ValueError, TypeError) as exc:
        raise ValueError('Invalid inbound Thread-Index encoding') from exc
    require(len(parent)>=22 and (len(parent)-22)%5==0 and parent[0]==1,
            'Invalid inbound Thread-Index structure')
    # The root stores the high 40 bits of a Windows FILETIME. A response
    # stores a 31-bit delta from that root, shifted by 18 or 23 bits, with
    # the shift selected by the high delta-code bit (MS-OXOMSG 2.2.1.3).
    root_time=int.from_bytes(parent[1:6],'big') << 24
    delta=max(0,int(now*10_000_000)+116444736000000000-root_time)
    shift=18 if delta < (1 << 49) else 23
    require(delta >> shift < (1 << 31), 'Inbound Thread-Index timestamp is too old')
    encoded=((int(shift==23) << 31) | (delta >> shift)).to_bytes(4,'big')
    level=(len(parent)-22)//5
    child=encoded+bytes([(os.urandom(1)[0] & 0xf0) | (level & 0x0f)])
    return base64.b64encode(parent+child).decode('ascii')


class GraphMailbox:
    HISTORY_FIELDS = ('id,internetMessageId,conversationId,from,toRecipients,ccRecipients,subject,'
                      'receivedDateTime,sentDateTime,createdDateTime,isDraft,bodyPreview,internetMessageHeaders')
    DRAFT_HISTORY_FIELDS = HISTORY_FIELDS + ',lastModifiedDateTime,bccRecipients,body,hasAttachments'

    def __init__(self, graph, settings, clock=time.time):
        self.graph=graph; self.settings=settings; self.clock=clock
        self.sent=None; self.inbox=None; self.identity=None
        self._sent_subject_index = None
        self.attempt_started=clock()
        self.begin_history_tick()

    def begin_history_tick(self):
        """Discard recipient-history folder scans and failures from an earlier tick."""
        self._history_folders = {}
        self._reply_draft_rows = None

    def verify_owner(self):
        if self.identity is None:
            self.identity=self.graph.request('/me?$select=id,mail,userPrincipalName')
        account=self.settings['account'].lower()
        require(account in {str(self.identity.get(k) or '').lower() for k in ('mail','userPrincipalName')},
                'Connected Graph mailbox is not the configured owner')
        return account

    def messages(self, folder, extra=None, *, before_next=None, include_unique_body=False):
        fields='id,internetMessageId,conversationId,from,toRecipients,ccRecipients,subject,receivedDateTime,sentDateTime,isDraft,internetMessageHeaders'
        if include_unique_body:
            fields += ',uniqueBody'
        query={'$select':fields,'$top':'100'}
        if extra:query.update(extra)
        path = '/me/mailFolders/'+folder+'/messages?'+urlencode(query)
        if before_next is not None:
            return self.graph.pages(path, before_next=before_next)
        return self.graph.pages(path)

    def sent_items(self, since=None):
        """Sent Items with headers and the sender's own body in the same request.

        The unbounded listing pages the whole folder with message headers and
        took ~170 s per tick in production (2026-11-16). Owner-send
        reconciliation only needs messages after a draft was saved, so it asks
        for a window starting one day before ``since``. ``self.sent`` holds
        either the full listing or {'start': ..., 'messages': [...]}; resetting
        it to None after a send invalidates both.
        """
        if since is None:
            if not isinstance(self.sent, list):
                self.sent=list(self.messages('sentitems', include_unique_body=True))
            return self.sent
        if isinstance(self.sent, list):
            return self.sent
        start=(since-timedelta(days=1)).astimezone(timezone.utc).strftime('%Y-%m-%dT00:00:00Z')
        if not (isinstance(self.sent, dict) and self.sent['start'] <= start):
            self.sent={'start':start, 'messages':list(self.messages(
                'sentitems', {'$filter':'sentDateTime ge '+start}, include_unique_body=True))}
        return self.sent['messages']

    def sent_snapshot(self):
        """Reuse any Sent Items metadata already loaded by this adapter."""
        return self.sent.get('messages', []) if isinstance(self.sent, dict) else self.sent or []

    # Graph returns Sent pagination links in OData key form, mailFolders('sentitems').
    SENT_PAGES = tuple(Graph.BASE + p for p in ('/me/mailFolders/sentitems/messages?',
                                                "/me/mailFolders('sentitems')/messages?"))

    def sent_events_page(self, since, until, next_link=None):
        """One newest-first Sent metadata page, without bodies or attachments."""
        if next_link:
            require(next_link.startswith(self.SENT_PAGES),
                    'Sent pagination changed origin or folder')
            path = next_link[len(Graph.BASE):]
        else:
            query = {'$select': 'id,internetMessageId,conversationId,from,toRecipients,ccRecipients,bccRecipients,subject,sentDateTime,createdDateTime,isDraft',
                     '$filter': 'sentDateTime ge ' + since + ' and sentDateTime le ' + until,
                     '$orderby': 'sentDateTime desc', '$top': '100'}
            path = '/me/mailFolders/sentitems/messages?' + urlencode(query)
        page = self.graph.request(path)
        require(isinstance(page.get('value'), list) and len(page['value']) <= 100,
                'Sent metadata page exceeds its bound')
        link = page.get('@odata.nextLink')
        if link:
            require(link.startswith(self.SENT_PAGES),
                    'Sent pagination changed origin or folder')
        return page['value'], link

    def recipient_history(self, recipient, *, draft_rows=None):
        """Fresh, fully paginated Inbox/Sent/Drafts metadata for one address.

        Search results below Graph's 1000-row cap are complete. At the cap,
        read full folder metadata once, shared by all addresses in this tick.
        Delivery's sent_items/inbox caches are separate freshness boundaries.
        Reply admission can supply its own Drafts listing for this read.
        """
        recipient = recipient.strip().casefold()
        result = []
        for folder in ('inbox', 'sentitems', 'drafts'):
            rows = self._history_folders.get(folder)
            if folder == 'drafts' and draft_rows is not None:
                rows = draft_rows
            if rows is None:
                rows = self.messages(folder, {'$search': json.dumps('participants:' + recipient),
                                             '$select': self.HISTORY_FIELDS})
                if len(rows) >= 1000:
                    rows = self._read_history_folder(folder, self.HISTORY_FIELDS)
                    self._history_folders[folder] = rows
            if isinstance(rows, Exception):
                raise rows
            for row in rows:
                participants = {address(v) for v in
                                [row.get('from') or {}, *row.get('toRecipients', []),
                                 *row.get('ccRecipients', [])]}
                if recipient in participants:
                    result.append({**row, 'folder': folder})
        return result

    def _read_history_folder(self, folder, fields):
        try:
            return self.messages(folder, {'$select': fields})
        except Exception as exc:
            # Share failed full scans within the tick as well as successful ones.
            return exc

    def reply_drafts(self, *, before_next=None):
        """List Drafts without search-index lag, sharing the scan for the tick."""
        if self._reply_draft_rows is None:
            try:
                options = {'before_next': before_next} if before_next is not None else {}
                self._reply_draft_rows = self.messages('drafts', {
                    '$select': self.DRAFT_HISTORY_FIELDS, '$expand': 'attachments($select=id,isInline)'}, **options)
            except Exception as exc:
                self._reply_draft_rows = exc
        if isinstance(self._reply_draft_rows, Exception):
            raise self._reply_draft_rows
        return self._reply_draft_rows

    def drafts_for(self, recipient):
        """Return unsent Drafts items whose participants include ``recipient``."""
        recipient = recipient.strip().casefold()
        rows = self._history_folders.get('drafts')
        if rows is None:
            rows = self.messages('drafts', {'$search': json.dumps('participants:' + recipient),
                                            '$select': self.HISTORY_FIELDS})
            if len(rows) >= 1000:
                rows = self._read_history_folder('drafts', self.HISTORY_FIELDS)
                self._history_folders['drafts'] = rows
        if isinstance(rows, Exception):
            raise rows
        result = []
        for message in rows:
            participants = {_address for _address in
                            [address(message.get('from', {})),
                             *[address(v) for v in message.get('toRecipients', [])],
                             *[address(v) for v in message.get('ccRecipients', [])]] if _address}
            if recipient in participants:
                result.append({**message, 'folder': 'drafts'})
        return result

    def sent_messages_to(self, recipient):
        """Read one bounded, newest Sent conversation for a recipient.

        The query is capped before selecting the newest matching row. The
        return value is one complete conversation, never a merge of several
        conversations with the same correspondent.
        """
        recipient = recipient.strip().lower()
        # Graph rejects lambda filters on toRecipients/ccRecipients
        # (ErrorInvalidUrlQueryFilter, measured 2026-11-08), so search the
        # participants and select the newest conversation here.
        rows = self.messages('sentitems', {'$search': json.dumps('participants:' + recipient),
                                           '$top': '25'})
        rows = [m for m in rows if not m.get('isDraft') and recipient in
                {address(a) for a in m.get('toRecipients', []) + m.get('ccRecipients', [])}]
        if not rows:
            return []
        latest = max(rows, key=lambda m: instant(m.get('sentDateTime', '')))
        conversation = latest.get('conversationId')
        require(conversation, 'Sent context has no conversation identity')
        return self.thread(conversation)

    def sent_message_by_internet_id(self, identity):
        """Look up exactly one Internet ID in Sent Items."""
        require(isinstance(identity, str) and identity.strip(), 'Missing Sent Internet Message-ID')
        escaped = identity.replace("'", "''")
        rows = self.messages('sentitems', {
            '$filter': "internetMessageId eq '" + escaped + "'", '$top': '10'})
        rows = [m for m in rows if not m.get('isDraft') and m.get('internetMessageId') == identity]
        return rows[0] if len(rows) == 1 else None

    @staticmethod
    def _registered_recipient(message, persons, account):
        recipients = [address(a) for a in message.get('toRecipients', []) + message.get('ccRecipients', [])]
        for candidate in recipients:
            if not candidate or candidate == account:
                continue
            person = next((p for p in persons if candidate in p['emails']), None)
            if person is not None:
                return person
        return None

    def _read_sent_subjects(self, start, end, *, incremental=False):
        from .mailbox_triage import normalized_subject

        comparison = ' gt ' if incremental else ' ge '
        rows = self.messages('sentitems', {
            '$select': ','.join(SENT_SUBJECT_FIELDS),
            '$filter': 'sentDateTime' + comparison + utc_stamp(start) +
                       ' and sentDateTime le ' + utc_stamp(end) + ' and isDraft eq false',
            '$orderby': 'sentDateTime desc'})
        subjects = defaultdict(list)
        for sent in rows:
            subject = normalized_subject(sent.get('subject'))
            if not subject or sent.get('isDraft') or not sent.get('sentDateTime'):
                continue
            sent_at = instant(sent['sentDateTime'])
            if not start <= sent_at <= end or (incremental and sent_at == start):
                continue
            subjects[subject].append(_compact_sent_subject_row(sent))
        return subjects

    def _refresh_sent_subjects(self, db, start, needed_through, now):
        """Cover the intake window with one listing, or reuse saved coverage."""
        from .mailbox_triage import normalized_subject

        saved = db.execute('SELECT payload FROM pipeline_state WHERE key=?',
                           (SENT_SUBJECTS_KEY,)).fetchone()
        account = self.settings['account'].lower()
        subjects = defaultdict(list)
        through = built_at = now
        incremental = False
        if saved:
            try:
                state = json.loads(saved[0])
                lower, upper, built = (instant(state[k]) for k in ('from', 'through', 'built_at'))
                if (state['account'] == account and lower <= start
                        and lower <= upper <= max(now, needed_through) and built <= upper
                        and built <= now < built + timedelta(days=1)):
                    for subject, rows in state['subjects'].items():
                        require(isinstance(rows, list), 'Invalid Sent subject rows')
                        for row in rows:
                            sent_at = instant(row['sentDateTime'])
                            require(subject and normalized_subject(row['subject']) == subject
                                    and lower <= sent_at <= upper and row == _compact_sent_subject_row(row),
                                    'Invalid Sent subject row')
                            if sent_at >= start:
                                subjects[subject].append(row)
                    through, built_at = upper, built
                    incremental = True
            except (AttributeError, KeyError, TypeError, ValueError):
                subjects.clear()

        if not incremental:
            through = max(now, needed_through)
            subjects = self._read_sent_subjects(start, through)
        elif through < needed_through:
            refreshed_through = max(now, needed_through)
            fresh = self._read_sent_subjects(through, refreshed_through, incremental=True)
            for subject, rows in fresh.items():
                subjects[subject] = rows + subjects[subject]
            through = refreshed_through
        payload = canonical({'account': account, 'from': utc_stamp(start), 'through': utc_stamp(through),
                             'built_at': utc_stamp(built_at), 'subjects': subjects})
        if not saved or payload != saved[0]:
            db.execute('INSERT OR REPLACE INTO pipeline_state VALUES(?,?)', (SENT_SUBJECTS_KEY, payload))
        return through, subjects

    def match_inbound_sent(self, message, persons, now=None, *, subjects=None, sent_index=None):
        """Find a registered owner for a linked, otherwise-unregistered reply.

        Header links use exact Sent-folder Internet-ID lookups. For messages
        with a reply or forward prefix, the normalized subject fallback is
        bounded to one matching message in the 90 days before receipt.
        """
        account = self.settings['account'].lower()
        for identity in _header_message_ids(message):
            sent = self.sent_message_by_internet_id(identity)
            if sent is None:
                continue
            person = self._registered_recipient(sent, persons, account)
            if person is not None:
                return {'match': 'internet-message-id', 'sent': sent, 'person': person}

        from .mailbox_triage import (SENT_SUBJECT_WINDOW, normalized_subject,
                                     sent_subject_match, subject_requirement)
        if subject_requirement(message):
            return None
        received = instant(message['receivedDateTime'])
        start = received - SENT_SUBJECT_WINDOW
        index = sent_index(received) if sent_index else self._read_sent_subjects(start, received)
        subject = normalized_subject(message.get('subject'))
        candidates = subjects[subject] if subjects is not None else []
        for sent in index.get(subject, ()):
            if start <= instant(sent['sentDateTime']) <= received:
                candidates.append({'sent': sent})
        _, link = sent_subject_match(message, persons, account, candidates)
        return link

    def thread(self, conversation):
        query={'$filter':"conversationId eq '"+conversation.replace("'","''")+"'",'$top':'100',
               '$select':'id,internetMessageId,conversationId,from,toRecipients,ccRecipients,subject,body,receivedDateTime,sentDateTime,isDraft,hasAttachments,internetMessageHeaders,parentFolderId'}
        rows=self.graph.pages('/me/messages?'+urlencode(query))
        # Moving to Deleted Items retains isDraft=true and /me/messages still
        # lists it. Only a draft still in Drafts may block a new reply.
        if any(m.get('isDraft') and m.get('parentFolderId') for m in rows):
            folder=self.drafts_folder_id()
            rows=[m for m in rows if not m.get('isDraft') or not m.get('parentFolderId')
                  or m['parentFolderId'] == folder]
        return rows

    def research_thread(self, conversation):
        rows=self.thread(conversation)
        unsupported=[]
        # Graph supplies @odata.type as an annotation alongside the projection.
        query=urlencode({'$top':'100','$select':'id,name,size,contentType'})
        for row in rows:
            if row.get('hasAttachments'):
                path='/me/messages/'+quote(row['id'],safe='')+'/attachments'
                row['attachments']=self.graph.pages(path+'?'+query)
                for attachment in row['attachments']:
                    kind=attachment.get('@odata.type')
                    size=attachment.get('size')
                    if kind==FILE_ATTACHMENT and isinstance(size,int) and 0<=size<=MAX_ATTACHMENT_BYTES:
                        continue
                    unsupported.append(_attachment_issue(row, attachment, size))
        if unsupported:
            raise UnsupportedThreadAttachments(unsupported)
        return rows

    def attachment_content(self, message, attachment):
        path='/me/messages/'+quote(message['id'],safe='')+'/attachments/'+quote(attachment['id'],safe='')
        return self.graph.request(path)['contentBytes']

    def conversation_for_message(self, identity):
        require(isinstance(identity, str) and identity.strip(), 'Missing internetMessageId')
        query = {'$filter': "internetMessageId eq '" + identity.replace("'", "''") + "'",
                 '$select': 'id,internetMessageId,conversationId', '$top': '100'}
        rows = self.graph.pages('/me/messages?' + urlencode(query))
        conversations = {m.get('conversationId') for m in rows if m.get('internetMessageId') == identity}
        if not conversations:
            return None
        require(len(conversations) == 1 and all(conversations),
                'internetMessageId resolves to ambiguous conversations')
        return conversations.pop()

    def _require_no_newer_elsewhere(self, packet, latest, account):
        """Block a reply when a recipient wrote again in another conversation.

        Exchange may file a Gmail reply as a new conversation. A draft written
        against the older thread would then ignore the newest message. Only unanswered messages count: a later Sent to
        the same address means that message was already handled.
        """
        if self.settings.get('cross_conversation_guard') is not True:
            return
        team = {a.casefold() for a in self.settings.get('team_addresses', ())}
        since = instant(latest['receivedDateTime'])
        primary = {a.casefold() for a in packet['to']}
        for recipient in sorted({a.casefold() for a in packet['to'] + packet['cc']} - team - {account}):
            rows = self.recipient_history(recipient)
            replied = [instant(r['sentDateTime']) for r in rows
                       if r.get('folder') == 'sentitems' and not r.get('isDraft') and r.get('sentDateTime')]
            # the owner may already have answered the latest message from another
            # conversation: the stale draft must not
            # be saved. Same-thread Sent is checked by latest_sent_message_id.
            # A Sent that answers their newer message in that other conversation
            # is a separate exchange and does not count.
            if recipient in primary:
                newer_in = [(r.get('conversationId'), instant(r['receivedDateTime'])) for r in rows
                            if r.get('folder') == 'inbox' and not r.get('isDraft')
                            and address(r.get('from', {})) == recipient and r.get('receivedDateTime')
                            and instant(r['receivedDateTime']) > since]
                for r in rows:
                    if (r.get('folder') != 'sentitems' or r.get('isDraft') or not r.get('sentDateTime')
                            or r.get('conversationId') == packet['thread_id']):
                        continue
                    at = instant(r['sentDateTime'])
                    require(at <= since or any(c == r.get('conversationId') and t < at for c, t in newer_in),
                            'Actual Sent to ' + recipient + ' after the latest incoming message in another '
                            'conversation; re-check whether this reply is still needed')
            for row in rows:
                if (row.get('folder') != 'inbox' or row.get('isDraft')
                        or row.get('conversationId') == packet['thread_id']
                        or address(row.get('from', {})) != recipient
                        or not row.get('receivedDateTime')):
                    continue
                at = instant(row['receivedDateTime'])
                require(at <= since or any(sent > at for sent in replied),
                        'A newer incoming message from ' + recipient +
                        ' in another conversation requires a new reply/review')

    def _check_attachment_limit(self, total):
        limit = self.settings.get('max_total_attachment_bytes', DEFAULT_TOTAL_ATTACHMENT_BYTES)
        require(type(limit) is int and limit >= 0, 'Invalid mailbox.max_total_attachment_bytes')
        require(total <= limit, f'Total message attachment size {total} bytes exceeds the mailbox limit of {limit} bytes')

    def preflight(self, packet, request, allow_draft_id=None):
        account=self.verify_owner()
        require(account == packet['sender'], 'Approved sender differs from actual mailbox owner')
        require(self.settings.get('reconciled') is True, 'Complete Inbox/Sent reconciliation before any mailbox write')
        self._check_attachment_limit(sum(len(base64.b64decode(a['content_b64'], validate=True))
                                         for a in packet['attachments']))
        evidence = {}
        if packet['kind']=='firstcontact':
            # Per-recipient participant search (complete below Graph's cap, with
            # the shared full-scan fallback) instead of reading all Sent and Inbox
            # metadata: the full scans took ~12 min per tick on 2026-11-02.
            for recipient in sorted(set(packet['to']+packet['cc'])):
                rows=self.recipient_history(recipient)
                require(not any(r['folder']=='sentitems' for r in rows),
                        'Actual Sent already contains contact with this recipient')
                require(not any(r['folder']=='inbox' and address(r.get('from',{}))==recipient for r in rows),
                        'Actual Inbox already contains correspondence; use the existing thread')
        else:
            rows=self.thread(packet['thread_id'])
            require(not any(m.get('isDraft') and m.get('id') != allow_draft_id for m in rows),
                    'A server draft already exists; preserve the user draft')
            incoming=[m for m in rows if not m.get('isDraft') and address(m.get('from',{})) != account]
            require(incoming, 'Actual reply thread has no incoming message')
            latest=max(incoming,key=lambda m:instant(m['receivedDateTime']))
            require(latest['internetMessageId'] == packet['latest_inbound_message_id'], 'A newer incoming message requires a new reply/review')
            self._require_no_newer_elsewhere(packet, latest, account)
            exception = reply_recipient_exception(packet, latest, account, rows,
                                                  self.settings.get('team_addresses', ()))
            from .registry import mail_thread_contacts
            evidence['mail_thread_contacts'] = mail_thread_contacts(rows, latest, packet['to'] + packet['cc'])
            if exception:
                evidence['recipient_exception'] = exception
            sent=[m for m in rows if not m.get('isDraft') and address(m.get('from',{})) == account]
            latest_sent=max(sent,key=lambda m:instant(m['sentDateTime'])) if sent else None
            expected_sent=request['inputs'].get('latest_sent_message_id')
            require((latest_sent or {}).get('internetMessageId') == expected_sent,
                    'Actual Sent changed or its source identity was not captured')
        now=self.clock()
        return {'checked_at':now,'expires_at':now+60,'complete':True,'paused':False,
                'contacts':{a:'clear' for a in packet['to']+packet['cc']}, **evidence}

    def message(self, identity):
        return self.graph.request('/me/messages/'+quote(identity,safe='')+'?'+urlencode({
            '$select':'id,internetMessageId,conversationId,from,toRecipients,ccRecipients,bccRecipients,subject,body,isDraft,internetMessageHeaders,sentDateTime,parentFolderId,createdDateTime,lastModifiedDateTime'}))

    def drafts_folder_id(self):
        if getattr(self, '_drafts_folder', None) is None:
            self._drafts_folder = self.graph.request('/me/mailFolders/drafts?$select=id')['id']
        return self._drafts_folder

    def matches(self, message, packet, *, exact_large=False):
        if not (address(message.get('from',{}))==packet['sender']
                and sorted(address(a) for a in message.get('toRecipients',[]))==sorted(packet['to'])
                and sorted(address(a) for a in message.get('ccRecipients',[]))==sorted(packet['cc'])
                and message.get('subject')==packet['subject']
                and body_text(message)==packet['body'].replace('\r\n','\n').rstrip('\n')):
            return False
        expected = {a['name']: (a['media_type'], base64.b64decode(a['content_b64'], validate=True))
                    for a in packet['attachments']}
        path = '/me/messages/' + quote(message['id'], safe='') + '/attachments'
        query = '?$top=100'
        if any(len(raw) >= UPLOAD_THRESHOLD for _, raw in expected.values()):
            query += '&$select=id,name,size,contentType,isInline'
        for value in self.graph.pages(path + query):
            entry = expected.pop(value.get('name'), None)
            if entry is None or value.get('@odata.type') != FILE_ATTACHMENT or value.get('isInline'):
                return False
            media_type, raw = entry
            if len(raw) >= UPLOAD_THRESHOLD:
                # Graph reports the stored attachment size, which includes a
                # small encoding overhead (live 2026-11-20: 5,255,386 reported
                # for 5,255,225 raw bytes); exact bytes are compared below.
                size = value.get('size')
                if type(size) is not int or not len(raw) <= size <= len(raw) + ATTACHMENT_SIZE_OVERHEAD:
                    return False
                if exact_large:
                    content_path = path + '/' + quote(value['id'], safe='') + '/$value'
                    if self.graph.request(content_path, raw_response=True) != raw:
                        return False
            else:
                encoded = value.get('contentBytes')
                if encoded is None:
                    encoded = self.attachment_content(message, value)
                if (value.get('contentType') != media_type
                        or base64.b64decode(encoded, validate=True) != raw):
                    return False
        return not expected

    def find(self, folder, key):
        # A custom header is correlation only. Callers verify the message and
        # attachments against the reviewed packet before recording a receipt.
        since=datetime.fromtimestamp(self.attempt_started-300,timezone.utc).isoformat().replace('+00:00','Z')
        field='sentDateTime' if folder=='sentitems' else 'createdDateTime'
        rows=self.messages(folder,{'$filter':field+' ge '+since})
        found=[]
        for row in rows:
            message=self.message(row['id'])
            headers={h['name'].lower():h['value'] for h in message.get('internetMessageHeaders',[])}
            if headers.get('x-outreach-attempt')==key:found.append(message)
        require(len(found)<=1, 'Multiple mailbox copies share an attempt; reconcile manually')
        return found[0] if found else None

    def _upload_attachment(self, draft_id, attachment, raw):
        path = '/me/messages/' + quote(draft_id, safe='') + '/attachments/createUploadSession'
        size = len(raw)
        session = self.graph.request(path, 'POST', {'AttachmentItem': {
            'attachmentType': 'file', 'name': attachment['name'], 'size': size,
            'contentType': attachment['media_type'],
        }})
        url = session.get('uploadUrl')
        offset = 0
        while offset < size:
            end = min(offset + UPLOAD_CHUNK_BYTES, size)
            for attempt in range(UPLOAD_ATTEMPTS):
                try:
                    if attempt:
                        try:
                            _, state = self.graph.upload_request(url, 'GET')
                        except _UploadRequestError as exc:
                            if end == size and exc.status in (404, 410):
                                # Completion can close the session before a lost final response.
                                # The draft attachment readback still has to pass.
                                return
                            raise
                        resume = _upload_offset(state)
                        require(offset <= resume <= end, 'Graph attachment upload returned an unexpected offset')
                        if resume == end:
                            break
                        offset = resume
                        end = min(offset + UPLOAD_CHUNK_BYTES, size)
                    status, state = self.graph.upload_request(url, 'PUT', raw[offset:end], offset=offset, size=size)
                    if end == size:
                        require(status == 201, 'Graph attachment upload did not confirm completion')
                    else:
                        require(status in (200, 202) and _upload_offset(state) == end,
                                'Graph attachment upload did not confirm the chunk range')
                    break
                except _UploadRequestError as exc:
                    if not exc.retryable or attempt == UPLOAD_ATTEMPTS - 1:
                        raise
                    time.sleep(attempt + 1)
            offset = end

    def _discard_incomplete_draft(self, draft_id):
        try:
            observed = self.message(draft_id)
            require(observed.get('id') == draft_id and observed.get('isDraft') is True,
                    'Created message is no longer a draft')
            self.delete_draft(draft_id)
        except HTTPError as exc:
            if exc.code != 404:
                raise

    def create_draft(self, packet, key, replacing=None):
        decoded = [(a, base64.b64decode(a['content_b64'], validate=True)) for a in packet['attachments']]
        self._check_attachment_limit(sum(len(raw) for _, raw in decoded))
        small = [(a, raw) for a, raw in decoded if len(raw) < UPLOAD_THRESHOLD]
        large = [(a, raw) for a, raw in decoded if len(raw) >= UPLOAD_THRESHOLD]
        if packet['kind']=='reply':
            rows=self.thread(packet['thread_id'])
            require(not any(m.get('isDraft') and m.get('id') != replacing for m in rows),
                    'A server draft already exists; preserve the user draft')
            sources=[m for m in rows if m.get('internetMessageId')==packet['latest_inbound_message_id']
                     and not m.get('isDraft') and address(m.get('from',{}))!=packet['sender']]
            require(len(sources)==1, 'Latest inbound message is missing or ambiguous in the reply conversation')
            source=self.message(sources[0]['id'])
            require(source.get('internetMessageId')==packet['latest_inbound_message_id']
                    and source.get('conversationId')==packet['thread_id'] and not source.get('isDraft')
                    and address(source.get('from',{}))!=packet['sender'],
                    'Reply source identity changed while reading threading headers')
            headers={h['name'].lower():h['value'] for h in source.get('internetMessageHeaders',[])}
        else:
            for row in self.messages('drafts'):
                if row.get('id') == replacing:
                    continue
                require(not set(packet['to']+packet['cc']) &
                        {address(a) for a in row.get('toRecipients',[])+row.get('ccRecipients',[])},
                        'A server draft for this recipient already exists; preserve it')
        body={'subject':packet['subject'],'body':{'contentType':'Text','content':packet['body']},
              'from':{'emailAddress':{'address':packet['sender']}},
              'toRecipients':[{'emailAddress':{'address':a}} for a in packet['to']],
              'ccRecipients':[{'emailAddress':{'address':a}} for a in packet['cc']],
              'internetMessageHeaders':[{'name':'x-outreach-attempt','value':key}],
              'attachments':[{'@odata.type':FILE_ATTACHMENT,'name':a['name'],
                              'contentType':a['media_type'],'contentBytes':a['content_b64']} for a, _ in small]}
        # Use MIME for replies so the exact source Message-ID remains in both
        # In-Reply-To and References without an unreviewed server-added body.
        self._reply_draft_rows = None
        if packet['kind']=='reply':
            import email.policy
            from email.message import EmailMessage
            mime=EmailMessage(policy=email.policy.SMTP)
            for name,value in [('From',packet['sender']),('To',', '.join(packet['to'])),
                               ('Cc',', '.join(packet['cc'])),('Subject',packet['subject']),
                               ('In-Reply-To',packet['latest_inbound_message_id']),
                               ('References',packet['latest_inbound_message_id']),('X-Outreach-Attempt',key)]:
                if value:mime[name]=value
            if headers.get('thread-index'):
                # Zimbra and other non-Exchange senders put their own 22-byte value
                # in Thread-Index (live 2026-11-20: first byte 0xCD, not the 0x01
                # of an MS-OXOMSG ConversationIndex). It cannot be extended, so
                # omit the pair; In-Reply-To and References thread the reply and
                # replies_in_thread() accepts exactly that.
                try:
                    child=child_thread_index(headers['thread-index'],self.clock())
                except ValueError:
                    child=None
                if child is not None:
                    mime['Thread-Index']=child
                    mime['Thread-Topic']=headers.get('thread-topic') or source.get('subject') or packet['subject']
            mime.set_content(packet['body'])
            for a, raw in small:
                main,sub=a['media_type'].split('/',1)
                mime.add_attachment(raw,maintype=main,subtype=sub,filename=a['name'])
            # Keep longer conversation indices as raw base64 header values;
            # the default 78-column folding can turn them into RFC 2047 words.
            draft=self.graph.mime('/me/messages',base64.b64encode(
                mime.as_bytes(policy=email.policy.SMTP.clone(max_line_length=998))))
        else:
            draft=self.graph.request('/me/messages','POST',body)
        draft_id = draft['id']
        try:
            for attachment, raw in large:
                self._upload_attachment(draft_id, attachment, raw)
            observed = self.message(draft_id)
            require(self.matches(observed, packet) and (not large or observed.get('isDraft') is True),
                    'Created draft differs from the reviewed packet; do not send')
            if packet['kind']=='reply':
                require(replies_in_thread(observed, packet), 'Created reply is not in the original conversation')
        except Exception as exc:
            if large:
                try:
                    self._discard_incomplete_draft(draft_id)
                except Exception:
                    raise GateError(f'{exc}; incomplete draft {draft_id} could not be removed') from None
            raise
        return observed

    def submit(self, packet, key):
        return self.submit_tracked(packet,key,lambda draft:None)

    def submit_tracked(self, packet, key, before_send):
        try:
            draft=self.create_draft(packet,key)
        except Exception as exc:
            raise NotSent((str(exc) or type(exc).__name__)[:300]) from exc
        # The queue checkpoints this marker before /send. A crash after the
        # marker is always uncertain, even if the HTTP call never returned.
        before_send(draft['id'])
        self._reply_draft_rows = None
        self.graph.request('/me/messages/'+quote(draft['id'],safe='')+'/send','POST',{})
        self.sent=None
        return Submission(draft['id'])

    def own_draft(self, draft, packet, key):
        require(self.verify_owner()==packet['sender'], 'Draft cleanup mailbox owner changed')
        values=[h['value'] for h in draft.get('internetMessageHeaders',[])
                if h['name'].lower()=='x-outreach-attempt']
        require(values==[key] and draft.get('isDraft') is True and self.matches(draft,packet,exact_large=True),
                'Attempt draft is not our unchanged unsent draft; preserve the user version')

    def legacy_not_sent(self, packet, key):
        if packet['kind']!='reply':return None
        draft=self.find('drafts',key)
        if draft is None:return None
        self.own_draft(draft,packet,key)
        if not draft.get('conversationId') or replies_in_thread(draft, packet):return None
        return 'Created reply is not in the original conversation; verified own unsent draft '+draft['id']

    def discard_draft(self, packet, key):
        draft=self.find('drafts',key)
        if draft is not None:
            # Re-read immediately before deletion; correlation alone never
            # authorizes deleting an edited, sent or owner-created message.
            draft=self.message(draft['id'])
            self.own_draft(draft,packet,key)
            self._reply_draft_rows = None
            self.graph.request('/me/messages/'+quote(draft['id'],safe=''),'DELETE')
        require(self.find('drafts',key) is None, 'Attempt draft cleanup is not yet confirmed')
        return 'Own unsent attempt draft removed or confirmed absent'

    def delete_draft(self, remote_id):
        require(isinstance(remote_id, str) and remote_id.strip(), 'Missing draft remote ID')
        self._reply_draft_rows = None
        self.graph.request('/me/messages/'+quote(remote_id, safe=''), 'DELETE')

    def move_draft_to_deleted(self, remote_id):
        require(isinstance(remote_id, str) and remote_id.strip(), 'Missing draft remote ID')
        self._reply_draft_rows = None
        return self.graph.request('/me/messages/'+quote(remote_id, safe='')+'/move',
                                  'POST', {'destinationId':'deleteditems'})

    def lookup(self, packet, key):
        message=self.find('sentitems',key)
        if message is None:return None
        require(not message.get('isDraft') and self.matches(message,packet,exact_large=True),
                'Actual Sent differs from the reviewed packet')
        return Readback(key,digest(packet),message['id'],'graph:sent:'+message['internetMessageId'],message['sentDateTime'])

    def save_draft(self, packet, key, recovery=False):
        draft=self.find('drafts',key)
        existing = draft is not None
        if draft is None:
            require(not recovery, 'Draft outcome unknown; retain intent and check the original attempt')
            draft=self.create_draft(packet,key)
        require(draft.get('isDraft') and self.matches(draft,packet,exact_large=existing),
                'Server draft was edited; preserve the user version')
        return {'remote_id':draft['id'],'attempt_key':key,
                'evidence_ref':'graph:draft:'+draft['id'],'content_hash':digest(packet),
                'saved_at':draft.get('createdDateTime'), 'conversationId':draft.get('conversationId')}


def configured(config, root):
    if config.get('mailbox_adapter') != 'microsoft-graph':return None
    from pathlib import Path
    ops=json.loads((Path(root)/'tables/operations.json').read_text())
    require(ops['processing_boundary'].get('mailbox_reconciled') is True,
            'Imported Inbox/Sent boundary has not been reconciled')
    settings={**config.get('mailbox',{}),'reconciled':True}
    require(settings.get('account'), 'Configure and verify the mailbox owner')
    return GraphMailbox(Graph(os.environ.get('OUTREACH_GRAPH_ACCESS_TOKEN')),settings)


def _previous_attachment_refs(db, conversation):
    """Recover attachment IDs and inputs from replies staged before the index."""
    index = {}
    rows = db.execute("""SELECT json_extract(request, '$.inputs.thread') FROM tasks
                         WHERE kind='reply' AND json_extract(request, '$.inputs.thread_id')=?
                         ORDER BY rowid""", (conversation,))
    for (thread,) in rows:
        for message in json.loads(thread or '[]'):
            for attachment in message.get('attachments', []):
                ref = attachment.get('selected_input') or {}
                sha = ref.get('sha256')
                tag = ref.get('release_tag')
                if (attachment.get('id') and isinstance(sha, str) and re.fullmatch(r'[a-f0-9]{64}', sha)
                        and ref.get('name') == 'attachment-' + sha
                        and isinstance(tag, str) and re.fullmatch(r'outreach-input-[a-f0-9]{32}', tag)
                        and isinstance(ref.get('bytes'), int) and 0 < ref['bytes'] <= MAX_ATTACHMENT_BYTES):
                    index[attachment['id']] = ref
    return index


def preserve_attachments(thread, ledger, identity, *, fetch=None, listings=None, upload=True):
    """Bind verified assets, or return False if an upload pass is needed."""
    from .queue import canonical
    attachments=[(m,a) for m in thread for a in m.get('attachments',[])]
    if not attachments:
        return True
    db=ledger.tasks.db
    conversation=identity['thread_id']
    key='mailbox-attachments:'+digest(conversation)
    saved=db.execute('SELECT payload FROM pipeline_state WHERE key=?',(key,)).fetchone()
    index=json.loads(saved[0]) if saved else _previous_attachment_refs(db,conversation)
    references=[index.get(a.get('id')) for _,a in attachments]
    if not upload and not all(references):
        return False

    releases=ledger.releases
    listings={} if listings is None else listings
    tag='outreach-input-'+digest(identity)[:32]

    def verified(ref):
        release_tag,name=ref['release_tag'],ref['name']
        if release_tag not in listings:
            listings[release_tag]=releases.task_assets(release_tag)
        asset=(listings[release_tag] or {}).get(name)
        if asset is None or asset.get('size')!=ref['bytes']:
            return False
        expected='sha256:'+ref['sha256']
        if asset.get('digest'):
            return asset['digest']==expected
        with tempfile.TemporaryDirectory(dir=ledger.folder) as temp:
            releases.run('release','download',release_tag,'--repo',releases.repo,
                         '--pattern',name,'--dir',temp)
            raw=(Path(temp)/name).read_bytes()
        if len(raw)!=ref['bytes'] or hashlib.sha256(raw).hexdigest()!=ref['sha256']:
            return False
        asset['digest']=expected  # Cache the verified readback for this tick.
        return True

    references=[ref if ref and verified(ref) else None for ref in references]
    if not all(references):
        if not upload:
            return False
        by_sha={}
        for ref in index.values():
            candidates=by_sha.setdefault(ref['sha256'],{})
            candidates[ref['release_tag']]=ref
        reused={ref['sha256']:ref for ref in references if ref}
        unsupported=[]
        with tempfile.TemporaryDirectory(dir=ledger.folder) as temp:
            files={}
            for i, (message, attachment) in enumerate(attachments):
                if references[i]:
                    continue
                encoded=attachment.get('contentBytes')
                if encoded is None:
                    require(fetch is not None,'Thread attachment content is unavailable')
                    encoded=fetch(message,attachment)
                raw=base64.b64decode(encoded,validate=True)
                if len(raw)>MAX_ATTACHMENT_BYTES:
                    unsupported.append(_attachment_issue(message, attachment, len(raw)))
                    continue
                require(raw,'Select a supported thread attachment size')
                sha=hashlib.sha256(raw).hexdigest()
                if sha not in reused:
                    old=next((ref for ref in by_sha.get(sha,{}).values() if verified(ref)),None)
                    if old:
                        reused[sha]=old
                    else:
                        name='attachment-'+sha
                        path=Path(temp)/name
                        path.write_bytes(raw)
                        files[path]={'release_tag':tag,'name':name,'bytes':len(raw),'sha256':sha}
                        reused[sha]=files[path]
                references[i]=reused[sha]
            if unsupported:
                raise UnsupportedThreadAttachments(unsupported)
            if files:
                releases.upload_task_batch(files,tag,listings)
                require(all(verified(ref) for ref in files.values()),'Thread attachment input readback differs')
    updated={**index,**{a['id']:ref for (_,a),ref in zip(attachments,references) if a.get('id')}}
    if updated and (not saved or updated!=index):
        db.execute('INSERT OR REPLACE INTO pipeline_state VALUES(?,?)',(key,canonical(updated)))
    for (_,attachment),ref in zip(attachments,references):
        attachment.pop('contentBytes',None)
        attachment['selected_input']=ref
    return True


REPLY_OWNER_RULE=('Route routing.mail=owner (a draft for the owner to send herself) whenever the latest incoming message or the needed reply touches anything personal, asks about us or the project (who we are, affiliation, how the results are produced, tools/AI, funding, plans, motives), or concerns meetings/calls, invitations, collaboration terms, byline order or submission timing. Purely academic mathematics, including a letter produced from an answered decision, may use routing.mail=auto after exact independent review and fresh mailbox checks. When unsure, choose owner. (User 2026-11-18.) Academic includes: technical progress, checks and corrections on a joint manuscript; corrections to a paper, note or OEIS entry; adding requested references; welcoming further mathematical questions or ideas without new commitments. Reserve owner for the listed personal/about-us/meeting/byline/submission-timing topics; never route an academic letter owner merely because it concerns joint work or the correspondent offered more problems. (User 2026-11-19.)')


NDR_SENDERS={'postmaster', 'mailer-daemon', 'mail delivery subsystem'}
MACHINE_SENDERS={'no-reply', 'noreply'} | NDR_SENDERS
NDR_SUBJECT=re.compile(r'^\s*(?:Undeliverable|Delivery Status Notification|Mail delivery failed|Returned mail|Delivery has failed)\b', re.I)
FAILED_HEADER=re.compile(r'^\s*(?:Final-Recipient|X-Failed-Recipients)\s*:', re.I | re.M)
BOUNCE_EMAIL=r'[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}'


def _adapter_method(mailbox, name):
    """Get an adapter method without treating unittest.mock.Mock attrs as APIs."""
    if type(mailbox).__module__.startswith('unittest.mock'):
        return None
    method=getattr(mailbox, name, None)
    return method if callable(method) else None


def _machine_sender(value):
    local=value.strip().rsplit('@', 1)[0].casefold()
    return local in MACHINE_SENDERS


def _delivery_failure(message):
    """Machine origin alone is insufficient evidence of a failed delivery."""
    sender=(message.get('from') or {}).get('emailAddress') or {}
    local=address(message.get('from')).rsplit('@', 1)[0]
    headers=_message_headers(message)
    return (local in NDR_SENDERS or (sender.get('name') or '').strip().casefold() in NDR_SENDERS
            or bool(NDR_SUBJECT.match(message.get('subject') or ''))
            or any(name in headers for name in ('final-recipient', 'x-failed-recipients'))
            or any(name in message.get('dsn_fields', {}) for name in ('final-recipient', 'x-failed-recipients'))
            or bool(FAILED_HEADER.search(str(message.get('body', {}).get('content', '')))))


def _bounce_recipient(message, account):
    headers={**message.get('dsn_fields', {}), **_message_headers(message)}
    text=str(message.get('body', {}).get('content', ''))
    # Prefer delivery diagnostics over quoted original-message headers, which
    # can name other, reachable people. Delivered-To identifies this mailbox.
    values=[headers[name] for name in ('final-recipient', 'x-failed-recipients', 'original-recipient')
            if headers.get(name)]
    patterns=[r'^\s*(?:Final-Recipient|X-Failed-Recipients|Original-Recipient)\s*:\s*(?:rfc822\s*;\s*)?<?(' + BOUNCE_EMAIL + ')',
              r'Your message to\s+<?(' + BOUNCE_EMAIL + r")>?\s+couldn['’]t be delivered",
              r'<?(' + BOUNCE_EMAIL + r")>?\s+wasn['’]t found at",
              r'<?(' + BOUNCE_EMAIL + r')>?[^\n]{0,160}\bUser unknown\b']
    for pattern in patterns:
        values.extend(re.findall(pattern, text, re.I | re.M))
    sender=address(message.get('from',{}))
    for value in values:
        for candidate in re.findall(BOUNCE_EMAIL, value, re.I):
            candidate=candidate.strip('.').lower()
            if candidate not in {account.lower(), sender} and not _machine_sender(candidate):
                return candidate
    return None


UNREGISTERED='unregistered sender, no link to our Sent'
UNSUPPORTED_ATTACHMENTS='unsupported thread attachments; operator selection needed'
REFUSAL_BLOCKED='active refusal; retained'
AGED='older than 14 days'
UNMAPPED_RECHECK_SECONDS=6*3600
MAX_INBOUND_AGE=timedelta(days=14)


def _has_registry_history(person):
    from .registry import activity
    detail=activity(person)
    return bool(detail['threads'] or any(e.get('kind') == 'sent' for e in detail['events']))


def intake(ledger, config, root, mailbox, revision=None, now=None):
    """Actual Inbox -> one immutable reply task per current incoming message."""
    if mailbox is None or config['paused'] or not config['kinds'].get('reply'):return
    from pathlib import Path
    from .mailbox_triage import (ANNOTATION_FIELDS, EDITORIAL_CONFIRMATION, SENT_SUBJECT_WINDOW,
                                 Triage, category, recipients, subject_requirement)
    from .pipeline import request
    from .registry import people
    from .thread_staging import active_inbound_tasks, inbound_tasks, normalize_message_id
    tasks=ledger.tasks;db=tasks.db
    account=mailbox.verify_owner()
    require(mailbox.settings.get('reconciled') is True, 'Mailbox boundary is not reconciled')
    previous=db.execute("SELECT payload FROM pipeline_state WHERE key='mailbox'").fetchone()
    previous=json.loads(previous[0]) if previous else {}
    since=previous.get('through') or mailbox.settings.get('live_since')
    require(since, 'Set the verified live Inbox start after reconciling the historical boundary')
    now=now or datetime.now(timezone.utc)
    cutoff=now.isoformat().replace('+00:00','Z')
    require(datetime.fromisoformat(since.replace('Z','+00:00')) <=
            datetime.fromisoformat(cutoff.replace('Z','+00:00')), 'Mailbox cursor is in the future')
    incoming=mailbox.messages('inbox',{'$filter':'receivedDateTime ge '+since+' and receivedDateTime le '+cutoff})
    incoming=[*previous.get('unmapped',[]),*incoming]
    persons=people(root)
    snapshot=_adapter_method(mailbox,'sent_snapshot')
    triage=Triage(root,config,(),account,snapshot() if snapshot else (),persons=persons)
    triage.index_sent_tasks(db)
    decisions_path=Path(root)/'tables/decisions.json'
    controlled=json.loads(decisions_path.read_text()).get('owner_controlled_threads',[]) if decisions_path.exists() else []
    controlled={name.casefold() for name in controlled}
    revision=revision or source_head()
    pending={};bounces=[];bounce_keys=set();seen=set()
    attachment_holds={m['conversationId']:{'checked_at':m['checked_at'],
                                         'attachment_issues':m['attachment_issues']}
                      for m in previous.get('unmapped',[])
                      if m.get('intake_reason')==UNSUPPORTED_ATTACHMENTS and
                      m.get('checked_at') is not None and 'attachment_issues' in m}
    notices=[];notice_keys=set();notice_count=previous.get('machine_notice_count', 0)
    budget=config.get('automation',{}).get('max_new_tasks_per_tick',4)
    def message_key(message):
        return message.get('internetMessageId') or (message.get('conversationId'),message.get('receivedDateTime'))
    covered={message_key(m):m for m in previous.get('covered',[])}
    coverage=None
    def remember(message,reason,*,details=None,handled=False,**extra):
        fields={'id','internetMessageId','conversationId','from','toRecipients','ccRecipients',
                'subject','receivedDateTime','sentDateTime','internetMessageHeaders'}
        key=message_key(message)
        if key in pending and not handled:return
        annotation={} if details and 'category' in details else triage.classify(message,intake_reason=reason)
        if reason==UNSUPPORTED_ATTACHMENTS:
            annotation['category_reason']='Unsupported thread attachments need operator selection'
        row={**{k:v for k,v in message.items() if k in fields},
             'intake_reason':reason,**annotation,**(details or {}),**extra}
        if handled:
            pending.pop(key,None)
            covered[key]=row
        else:
            pending[key]=row
    def remember_covered(message):
        nonlocal coverage
        identity=normalize_message_id(message.get('internetMessageId'))
        if not identity:return False
        if coverage is None:coverage=active_inbound_tasks(tasks)
        group=coverage.get(identity)
        if not group:return False
        task_id=group[0]['task_id']
        remember(message,'covered',handled=True,
                 details=category('covered',f'Inbound message is covered by task {task_id}',task_id=task_id))
        return True
    def defer(message,reason,*,details=None):
        hold=attachment_holds.get(message.get('conversationId'))
        if hold is not None:
            remember(message,UNSUPPORTED_ATTACHMENTS,**hold)
        elif message.get('intake_reason') and message.get('checked_at') is not None:
            remember(message,message['intake_reason'],checked_at=message['checked_at'],
                     details={k:message[k] for k in ANNOTATION_FIELDS if k in message})
        else:
            remember(message,reason,details=details)
    def remember_bounce(message):
        key=message_key(message)
        if key in bounce_keys:return
        bounce_keys.add(key)
        remember_copy={k:v for k,v in message.items() if k in
                       {'id','internetMessageId','conversationId','from','toRecipients','ccRecipients',
                        'subject','receivedDateTime','sentDateTime','internetMessageHeaders','dsn_fields'}}
        dsn=dict(message.get('dsn_fields', {}))
        for name, value in re.findall(r'^\s*(Final-Recipient|Original-Recipient|X-Failed-Recipients)\s*:\s*([^\n]+)',
                                     str(message.get('body', {}).get('content', '')), re.I | re.M):
            dsn[name.casefold()]=value.strip()
        if dsn:remember_copy['dsn_fields']=dsn
        recipient=_bounce_recipient(message,account) or message.get('original_recipient') or message.get('recipient')
        remember_copy.update({'intake_reason':'bounce', 'reason':'bounce', 'recipient':recipient,
                              'original_recipient':recipient})
        bounces.append(remember_copy)
    def remember_notice(message, count=True):
        nonlocal notice_count
        key=message_key(message)
        if key in notice_keys:return
        notice_keys.add(key)
        notice_count+=int(count)
        notices.append({**{k:v for k,v in message.items() if k in
                          {'id','internetMessageId','conversationId','from','subject','receivedDateTime'}},
                        'intake_reason':'machine_notice'})
    for item in previous.get('machine_notices', []):
        remember_notice(item, count=False)
    def enrich_bounce(message):
        # The Inbox cursor projection omits body text. Fetch one complete
        # machine message when the adapter supports it so Final-Recipient and
        # ordinary bounce prose can identify the failed address.
        if message.get('body') is not None:
            return message
        research=_adapter_method(mailbox,'research_thread')
        if research is None:
            return message
        try:
            rows=research(message.get('conversationId'))
            return next((m for m in rows if
                         (message.get('internetMessageId') and m.get('internetMessageId')==message['internetMessageId'])
                         or (message.get('id') and m.get('id')==message['id'])), message)
        except Exception:
            return message
    # Reclassify the old catch-all bucket, without trusting its old reason or
    # the recipient guessed from unrelated notification text. Missing recipient
    # diagnostics can still be recovered from the exact retained NDR.
    for item in previous.get('bounces', []):
        if _delivery_failure(item):
            remember_bounce(item if item.get('original_recipient') or item.get('recipient') else enrich_bounce(item))
        else:remember_notice(item)
    from .tick import unit
    tick=getattr(ledger,'tick',None)
    deferred=tick.previous.get('mailbox-intake',[]) if tick else []
    if deferred:
        priority={key:i for i,key in enumerate(deferred)}
        incoming.sort(key=lambda m:priority.get(digest(message_key(m)),len(priority)))
    registered_addresses={email for person in persons for email in person['emails']}
    registered, unregistered = [], []
    for message in incoming:
        group = registered if address(message.get('from')) in registered_addresses else unregistered
        group.append(message)

    if isinstance(mailbox, GraphMailbox):
        mailbox._sent_subject_index = None

    def sent_index(received, message):
        if mailbox._sent_subject_index is None:
            key = digest(message_key(message))
            with unit(ledger, 'mailbox-intake', key, SENT_SUBJECTS_KEY) as ready:
                if not ready:
                    raise SentSubjectsDeferred('tick budget; retained', 'Tick time budget exhausted')
                try:
                    dates = [received, *(instant(row['receivedDateTime']) for row in unregistered
                             if row.get('receivedDateTime') and not subject_requirement(row))]
                    start = min(dates) - SENT_SUBJECT_WINDOW
                    through, subjects = mailbox._refresh_sent_subjects(db, start, max(dates), now)
                    mailbox._sent_subject_index = (start, through, subjects)
                except Exception as exc:
                    mailbox._sent_subject_index = exc
        if isinstance(mailbox._sent_subject_index, Exception):
            raise mailbox._sent_subject_index
        start, through, subjects = mailbox._sent_subject_index
        if received > through or received - SENT_SUBJECT_WINDOW < start:
            raise SentSubjectsDeferred('thread changed; retained', 'Thread changed outside the Sent subject scan window')
        return subjects

    fetch=_adapter_method(mailbox,'attachment_content')
    listings={}
    uploads=[]

    def stage(message, latest, value, *, upload):
        nonlocal budget
        inputs=value['inputs']
        link={k:inputs[k] for k in ('thread_id','latest_inbound_message_id')}
        try:
            ready=preserve_attachments(inputs['thread'],ledger,link,fetch=fetch,listings=listings,upload=upload)
        except UnsupportedThreadAttachments as exc:
            hold={'checked_at':tasks.clock(),'attachment_issues':exc.attachments}
            attachment_holds[link['thread_id']]=hold
            remember(message,UNSUPPORTED_ATTACHMENTS,**hold)
            return
        except Exception as exc:
            remember(message,'thread attachment preservation failed; retained',
                     details={'category_reason':f'Thread attachment preservation failed ({type(exc).__name__})'})
            return
        if ready:
            row=tasks.add(value)
            if coverage is not None:coverage.update(inbound_tasks([row]))
            budget-=1
        else:
            uploads.append((message,latest,value))

    def stage_uploads():
        for message,latest,value in uploads:
            with unit(ledger,'mailbox-intake',digest(message_key(message)),'mailbox-intake') as ready:
                if remember_covered(latest):
                    continue
                if not ready:
                    defer(message,'tick budget; retained',details={'category_reason':'Tick time budget exhausted'})
                elif budget<=0:
                    defer(message,'bounded; retained',details={'category_reason':'New reply task limit reached for this tick'})
                else:
                    stage(message,latest,value,upload=True)
        uploads.clear()

    for index,message in enumerate(registered + unregistered):
        if index == len(registered):
            # Finish registered replies with attachments before unregistered threads.
            stage_uploads()
        with unit(ledger,'mailbox-intake',digest(message_key(message)),'mailbox-intake') as ready:
            if not ready:
                defer(message,'tick budget; retained',details={'category_reason':'Tick time budget exhausted'})
                continue
            if message.get('isDraft'):continue
            if _delivery_failure(message):
                remember_bounce(enrich_bounce(message));continue
            if EDITORIAL_CONFIRMATION.search(message.get('subject') or ''):
                editorial = triage.classify(message, intake_reason='')
                if editorial['category'] == 'editorial-action':
                    remember(message, 'editorial-action', details=editorial)
                    continue
            if _machine_sender(address(message.get('from',{}))):
                # The inclusive Inbox cursor can repeat its boundary messages.
                # All previously scanned rows except retained unmapped work
                # have already contributed to the cumulative notice count.
                counted=(previous.get('through') and not message.get('intake_reason') and
                         instant(message['receivedDateTime'])<=instant(previous['through']))
                remember_notice(message, count=not counted);continue
            conversation=message['conversationId']
            hold=attachment_holds.get(conversation)
            if hold is not None and tasks.clock()-hold['checked_at']<UNMAPPED_RECHECK_SECONDS:
                remember(message,UNSUPPORTED_ATTACHMENTS,**hold)
                continue
            if message.get('conversationId') in seen:continue
            if budget<=0:
                defer(message,'bounded; retained',details={'category_reason':'New reply task limit reached for this tick'})
                continue
            # Unregistered senders are retained for later mapping. Re-reading
            # their threads every tick cost ~200 s for 98 messages on
            # 2026-11-08; recheck them only after a pause, or at once when
            # the sender has since been registered.
            reason=message.get('intake_reason')
            checked_at=message.get('checked_at')
            if reason in {UNREGISTERED,REFUSAL_BLOCKED,AGED} and checked_at is not None:
                sender0=address(message.get('from',{}))
                if (tasks.clock()-checked_at<UNMAPPED_RECHECK_SECONDS and
                        (reason!=UNREGISTERED or sender0 not in registered_addresses)):
                    if (reason==UNREGISTERED and message.get('receivedDateTime')
                            and instant(message['receivedDateTime']) >= now-MAX_INBOUND_AGE
                            and remember_covered(message)):
                        continue
                    remember(message,reason,checked_at=checked_at,
                             details={k:message[k] for k in ANNOTATION_FIELDS if k in message})
                    seen.add(message.get('conversationId'))
                    continue
            conversation=message['conversationId'];seen.add(conversation)
            try:
                thread=mailbox.research_thread(conversation)
            except UnsupportedThreadAttachments as exc:
                hold={'checked_at':tasks.clock(),'attachment_issues':exc.attachments}
                attachment_holds[conversation]=hold
                remember(message,UNSUPPORTED_ATTACHMENTS,**hold)
                continue
            attachment_holds.pop(conversation,None)
            actual=[m for m in thread if not m.get('isDraft') and address(m.get('from',{})) != account]
            if not actual:continue
            latest=max(actual,key=lambda m:instant(m['receivedDateTime']))
            sender=address(latest.get('from',{}))
            if _delivery_failure(latest):
                remember_bounce(latest);continue
            if _machine_sender(sender):
                remember_notice(latest);continue
            person=next((p for p in persons if sender in p['emails']),None)
            sent=[m for m in thread if not m.get('isDraft') and address(m.get('from',{}))==account]
            linked=None
            if person is None:
                matcher=_adapter_method(mailbox,'match_inbound_sent')
                if matcher is not None:
                    try:
                        if isinstance(mailbox,GraphMailbox):
                            linked=matcher(latest,persons,now=now,subjects=triage.subjects,
                                           sent_index=lambda received: sent_index(received, message))
                        else:
                            linked=matcher(latest,persons,now=now)
                    except SentSubjectsDeferred as exc:
                        remember(latest,exc.intake_reason,details={'category_reason':str(exc)})
                        continue
                    except Exception:linked=None
                if linked is not None:
                    person=linked['person']
            known_without_current_sent=bool(person is not None and not sent and _has_registry_history(person))
            if person is None or (not sent and not known_without_current_sent and linked is None):
                if instant(latest['receivedDateTime']) >= now-MAX_INBOUND_AGE and remember_covered(latest):continue
                details={'category_reason':'Registered sender has no current thread Sent mail or registry history'} if person else None
                remember(latest,UNREGISTERED,checked_at=tasks.clock(),details=details)
                continue
            latest_sent=max(sent,key=lambda m:instant(m['sentDateTime'])) if sent else None
            if latest_sent is not None and instant(latest_sent['sentDateTime']) > instant(latest['receivedDateTime']):continue
            contacts={address(value) for m in thread for value in
                      [m.get('from',{}),*m.get('toRecipients',[]),*m.get('ccRecipients',[])]}
            subject_link=linked is not None and linked.get('match')=='subject-within-90-days'
            linked_contacts=recipients(linked['sent']) if linked is not None else set()
            if subject_link:
                contacts.update(linked_contacts)
            contacts.discard(account)  # the mailbox owner is not a correspondent
            owners=sorted({p['id'] for p in persons if contacts & set(p['emails'])})
            if person['id'] not in owners:owners.append(person['id']);owners.sort()
            owner_controlled=any(p['name'].casefold() in controlled for p in persons if p['id'] in owners) or any(r.get('owner_controlled') is True or r.get('hub',{}).get('owner_controlled') is True
                                 for p in persons if p['id'] in owners for r in p['records'])
            if owner_controlled:
                for inbound in actual:
                    if latest_sent is None or instant(inbound['receivedDateTime']) >= instant(latest_sent['sentDateTime']):
                        remember(inbound,'owner-controlled',checked_at=tasks.clock())
                continue
            details={}
            if linked:
                details=category('linked-reply','Linked to our Sent message',contact_id=person['id'])
                if linked.get('task_id'):
                    details['task_id']=linked['task_id']
            sender_name=((latest.get('from') or {}).get('emailAddress') or {}).get('name')
            blocked=triage.blocked((contacts | linked_contacts)-{account},owners,sender_name)
            if blocked:
                remember(latest,REFUSAL_BLOCKED,checked_at=tasks.clock(),details={**details,'category_reason':blocked})
                continue
            if instant(latest['receivedDateTime']) < now-MAX_INBOUND_AGE:
                remember(latest,AGED,checked_at=tasks.clock(),
                         details={**details,'category_reason':'Inbound message is older than 14 days'})
                continue
            if remember_covered(latest):continue
            link={'thread_id':conversation,'latest_inbound_message_id':latest['internetMessageId']}
            inputs={'automation_contract':1,**link,**({'latest_sent_message_id':latest_sent['internetMessageId']} if latest_sent else {}),
                    'thread':thread,'owner_ids':owners,'user_decision_ever':False,
                    'mailbox_observation':{'email':sender,'at':latest['receivedDateTime'],
                                           'message_id':latest['internetMessageId'],'evidence_ref':'graph:inbox:'+latest['id']},
                    'instructions':'Read the complete current thread. Decide whether any reply is needed. '
                    'Complete useful research, ask genuine user decisions, preserve user drafts and classify '
                    'confirmed independent publication for automatic manuscript preparation. Return exact routing. '
                    +REPLY_OWNER_RULE}
            if known_without_current_sent:
                prior=_adapter_method(mailbox,'sent_messages_to')
                context=None
                if prior is not None:
                    try:context=prior(sender)
                    except Exception:context=None
                inputs['prior_context']=context if context else 'registry-only'
                inputs['instructions']=('This is a new conversation from a known correspondent. The current '
                                        'conversation has no Sent item; use the prior context when present and '
                                        'the registry when it is registry-only. ' + inputs['instructions'])
            if linked is not None:
                inputs['linked_sent_message_id']=linked['sent'].get('internetMessageId')
                alias_person=_alias_recipient(latest,linked['sent'],persons,account) if not subject_link else None
                if alias_person is not None:
                    hint={'email':sender,'name':alias_person['name'],'contact_id':alias_person['id'],
                          'evidence_ref':'graph:inbox:'+latest['id'],
                          'reason':'Incoming sender matches a recipient of our linked Sent message; register this alias after review.'}
                    inputs['registry_proposal_hint']=hint
                    inputs['instructions']=('The sender address is an unregistered alias linked to our Sent message. '
                                            'Preserve the reply task for the registered owner and return routing.registry_proposal '
                                            'with this exact alias and evidence so the operator can register it. ' + inputs['instructions'])
                else:
                    inputs['instructions']=('The unregistered sender is linked to our Sent message, but cannot be '
                                            'uniquely matched to a recipient. Preserve the reply task for the registered '
                                            'owner; do not infer an alias from the task owner. ' + inputs['instructions'])
            value=request('reply',link,person['id'],inputs,revision)
            if not db.execute('SELECT 1 FROM tasks WHERE id=?',(value['id'],)).fetchone():
                stage(message,latest,value,upload=False)
    stage_uploads()
    # Advance only after the entire paginated window was read and all observed
    # work was persisted. Unknown/owner-controlled mail remains in the inventory.
    db.execute('INSERT OR REPLACE INTO pipeline_state VALUES(?,?)',
               ('mailbox',canonical({'through':cutoff,'account':account,'unmapped':list(pending.values()),'bounces':bounces,
                                     'covered':list(covered.values())[-20:],
                                     'bounce_count':len(bounces),
                                     'machine_notice_count':notice_count,
                                     'machine_notices':sorted(notices, key=lambda m:m.get('receivedDateTime', ''))[-20:]})))
    ledger.save()
