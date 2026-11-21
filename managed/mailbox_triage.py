"""Deterministic classification of retained Inbox metadata."""
from collections import Counter, defaultdict
from datetime import timedelta
import json
from pathlib import Path
import re

from .mailbox import GraphMailbox, address, _message_headers, _header_message_ids
from .mail_domains import home_domains, matches_domain
from .registry import activity, date_order, name_key, people, read


REPLY_PREFIX = re.compile(r'^\s*(?:(?:re|fw|fwd|aw|回复|答复)\s*[:：]\s*)+', re.I)
EDITORIAL_ACTION = re.compile(
    r'\b(?:decisions?|confirm(?: your)? authorship|co[- ]authorship|requires? action|'
    r'please edit|incomplete|revisions?|revis(?:e|ed)|reject(?:ed|ion)?|'
    r'accept(?:ed|ance)?|proofs?)\b', re.I)
EDITORIAL_CONFIRMATION = re.compile(
    r'\b(?:confirm|verify)\b|\b(?:authorship|co[- ]authorship)\s+(?:confirmation|verification)\b|'
    r'\b(?:confirmation|verification)\s+(?:required|request(?:ed)?)\b|'
    r'\b(?:request|reminder)\b[^\n]{0,80}\b(?:confirmation|verification)\b|请(?:确认|验证)|(?:确认|验证)(?:共同)?作者',
    re.I)
MANUSCRIPT_ID = re.compile(
    r'(?<![\w.-])(?:arxiv:\s*\d{4}\.\d{4,5}(?:v\d+)?|'
    r'[a-z][a-z0-9]*(?:-[a-z0-9]+)*-\d[a-z0-9]*(?:[.-][a-z0-9]+)*)\b', re.I)
LABELLED_ID = re.compile(
    r'\bmanuscript\s*(?:id|no\.?|number|#)\s*[:#]?\s*'
    r'(?=[\w./-]*\d)([\w]+(?:[./-][\w]+)*)', re.I)
AUTOMATED_TOKENS = (
    'notification', 'notifications', 'support', 'info', 'documents', 'billing',
    'newsletter', 'newsletters', 'news', 'alert', 'alerts', 'digest', 'marketing', 'loyalty',
    'service', 'mailer', 'bounce', 'admin', 'system', 'team', 'hello',
    'notify', 'announcements', 'announcement',
)
REPLY_LOCAL = r'(?:do[._+-]?not|no|auto)[._+-]?reply'
AUTOMATED_REPLY = re.compile(rf'(?:^|[._+-]){REPLY_LOCAL}|{REPLY_LOCAL}(?=$|[._+-])', re.I)
AUTOMATED_SUBDOMAINS = {'loyalty', 'comms', 'news', 'notifications'}
ATTENTION_PRIORITY = {label: rank for rank, label in enumerate((
    'linked-reply', 'known-contact', 'likely-contact', 'editorial-action',
    'service-notice', 'person',
))}
SENT_SUBJECT_WINDOW = timedelta(days=90)
ANNOTATION_FIELDS = ('category', 'category_reason', 'contact_id', 'task_id',
                     'manuscript_id', 'joint_manuscript', 'coauthor_contact_ids')


def normalized_subject(subject):
    return ' '.join(REPLY_PREFIX.sub('', subject or '').split()).casefold()


def recipients(message):
    return {address(value) for key in ('toRecipients', 'ccRecipients')
            for value in message.get(key, [])} - {''}


def subject_requirement(message):
    subject = message.get('subject') or ''
    if not REPLY_PREFIX.match(subject):
        return 'Sent subject matches but the inbound subject lacks a reply or forward prefix'
    normalized = normalized_subject(subject)
    if len(normalized) < 16 or len(re.findall(r'\b\w+\b', normalized)) < 3:
        return 'Sent subject matches but has fewer than 16 characters or 3 words'
    return None


def sent_subject_match(message, persons, account, candidates):
    if not candidates:
        return None, None
    received = date_order(message.get('receivedDateTime'))
    previous = [m for m in candidates if date_order(m['sent']['sentDateTime']) <= received]
    if not previous:
        return category('linked-reply', 'Matching Sent messages postdate the inbound message'), None
    registered = {key: p for p in persons for key in {p['id'], *(r['id'] for r in p.get('records', []))}}
    recent = [m for m in previous if received - date_order(m['sent']['sentDateTime']) <= SENT_SUBJECT_WINDOW]
    # A receipt, registry event and Sent copy with the same Internet ID
    # describe one message. The ledger receipt supplies the task's owner.
    unique = {}
    for record in recent:
        sent = record['sent']
        key = sent.get('internetMessageId') or sent.get('id') or id(sent)
        existing = unique.get(key)
        if existing is None or (record.get('owner') in registered and existing.get('owner') not in registered):
            unique[key] = record
    if len(unique) != 1:
        reason = ('Sent subject matches multiple Sent messages' if unique else
                  'Matching Sent message is outside the 90 days before receipt')
        return category('linked-reply', reason), None
    record, = unique.values()
    sent = record['sent']
    person = registered.get(record.get('owner')) or GraphMailbox._registered_recipient(sent, persons, account)
    if person is None:
        return category('linked-reply', 'Sent subject matches but no registered owner was found'), None
    details = category('linked-reply', 'Subject matches one Sent message', contact_id=person['id'])
    if record.get('task_id'):
        details['task_id'] = record['task_id']
    reason = subject_requirement(message)
    if reason:
        return {**details, 'category_reason': reason}, None
    return details, {'match': 'subject-within-90-days', 'sent': sent, 'person': person,
                     'task_id': record.get('task_id')}


def name_tokens(value):
    return tuple(sorted(re.findall(r'[^\W_]+', name_key(value or ''))))


def category(label, reason, **details):
    return {'category': label, 'category_reason': reason, **details}


def manuscript_ids(value):
    return [m.group(1) for m in LABELLED_ID.finditer(value)] + [
        m.group().replace(' ', '') for m in MANUSCRIPT_ID.finditer(value)]


def bulk_reason(sender, headers):
    for label in ('list-unsubscribe', 'list-id', 'x-auto-response-suppress', 'feedback-id'):
        if label in headers:
            return 'Automation header ' + label
    if headers.get('precedence', '').casefold() in {'bulk', 'list', 'junk'}:
        return 'Bulk precedence header'
    if 'auto-submitted' in headers and headers['auto-submitted'].split(';', 1)[0].strip().casefold() != 'no':
        return 'Auto-Submitted header'
    local, _, domain = sender.partition('@')
    tokens = re.split(r'[._+-]+', local)
    if (local == 'intellisys' or AUTOMATED_REPLY.search(local)
            or any(token.startswith(AUTOMATED_TOKENS) or token.endswith(AUTOMATED_TOKENS)
                   for token in tokens)):
        return 'Automated sender address'
    if domain.rstrip('.').partition('.')[0] in AUTOMATED_SUBDOMAINS:
        return 'Automated sender subdomain'
    return None


class Triage:
    def __init__(self, root, config, rows, account, sent=(), persons=None):
        self.root = Path(root)
        self.account = account
        self.persons = people(root) if persons is None else persons
        self.by_address = {a: p for p in self.persons for a in p['emails'] if a != account}
        self.by_id = {key: p for p in self.persons for key in
                      {p['id'], *(r['id'] for r in p['records'])}}
        settings = config.get('mailbox', {})
        self.domains = {d.casefold() for d in settings.get('editorial_domains', [])}
        self.home_domains = home_domains(config)
        self.service_domains = {d.casefold() for d in settings.get('service_attention_domains', ['overleaf.com'])}
        self.subjects = defaultdict(list)
        self.written = defaultdict(list)
        self.names = defaultdict(set)
        self.manuscripts = defaultdict(set)
        self.refusals = [r for r in read(root, 'refusals')['refusals'] if r['active']]
        for person in self.persons:
            self.names[name_tokens(person['name'])].add(person['id'])
            history = activity(person)
            if history['last_out']:
                self.written[person['id']].append(history['last_out'])
            for event in history['events']:
                self.index_event(person, event)
        for row in rows:
            self.index_task(row)
        for message in sent:
            if not message.get('isDraft') and address(message.get('from', {})) == account:
                self.index_sent(message)
        if (self.root / 'tables' / 'submissions.json').exists():
            for paper in read(root, 'submissions').get('papers', []):
                self.index_paper(paper)

    def index_sent(self, message, task_id=None, owner=None):
        if not message.get('sentDateTime'):
            return
        subject = normalized_subject(message.get('subject'))
        if subject:
            self.subjects[subject].append({'sent': message, 'task_id': task_id, 'owner': owner})
        contact_ids = self.contact_ids(recipients(message))
        if owner in self.by_id:
            contact_ids.add(self.by_id[owner]['id'])
        for contact_id in contact_ids:
            self.written[contact_id].append(message['sentDateTime'])

    def index_event(self, person, event):
        summary = event.get('summary') or ''
        ids = manuscript_ids(summary)
        if event.get('manuscript_id'):
            ids.append(event['manuscript_id'])
        for identity in ids:
            matches = self.manuscripts[identity.casefold()]
            if event.get('joint_manuscript') or re.search(r'\b(?:joint manuscript|co-?authors?)\b', summary, re.I):
                matches.add(person['id'])
            matches.update(p for p in event.get('coauthor_contact_ids', []) if p in self.by_id)
        if event.get('kind') != 'sent':
            return
        self.written[person['id']].append(event['at'])
        evidence = event.get('evidence')
        if isinstance(evidence, str) and evidence.startswith('{'):
            try:
                evidence = json.loads(evidence)
            except ValueError:
                return
        if not isinstance(evidence, dict) or evidence.get('source') != 'sent-events':
            return
        if evidence.get('from', self.account) != self.account:
            return
        recipient = evidence.get('recipient', person['email'])
        self.index_sent({'subject': summary.split(' — ', 1)[0], 'sentDateTime': event['at'],
                         'internetMessageId': evidence.get('internetMessageId'),
                         'conversationId': evidence.get('conversationId'),
                         'toRecipients': [{'emailAddress': {'address': recipient}}]},
                        evidence.get('task_id'))

    def index_paper(self, paper):
        names = {name_key(n) for n in paper.get('authors', []) if isinstance(n, str)}
        ids = {p for p in paper.get('coauthor_contact_ids', []) if p in self.by_id}
        ids.update(p['id'] for p in self.persons if name_key(p['name']) in names)
        for field in ('manuscript_id', 'id', 'subject', 'title'):
            value = paper.get(field)
            if isinstance(value, str):
                matches = [value] if field == 'manuscript_id' else manuscript_ids(value)
                for identity in matches:
                    self.manuscripts[identity.casefold()].update(ids)

    def index_task(self, row):
        data, inputs = row['data'], row['request']['inputs']
        if row['kind'] == 'submission':
            for source in (inputs, data.get('result') or {}):
                self.index_paper({**(inputs.get('paper') or {}), **source})
        receipt = data.get('sent_receipt')
        packet = data.get('packet_override') or (data.get('result') or {}).get('packet')
        if row['kind'] not in {'reply', 'firstcontact'} or not receipt or not packet:
            return
        self.index_receipt(row['id'], row['request']['owner'], receipt.get('at'),
                           receipt.get('internetMessageId'), receipt.get('evidence_ref'),
                           packet.get('subject'), packet.get('to') or [], packet.get('cc') or [])

    def index_sent_tasks(self, db):
        # Intake scans only Sent receipt and packet metadata. Thread bodies
        # and attachments never leave SQLite in this query.
        for task_id, owner, sent_at, message_id, evidence, subject, to, cc in db.execute("""
            WITH sent AS (
                SELECT id, owner, data,
                       CASE WHEN json_type(data, '$.packet_override') = 'object'
                            THEN '$.packet_override' ELSE '$.result.packet' END AS packet
                FROM tasks
                WHERE kind IN ('reply', 'firstcontact')
                    AND json_extract(data, '$.sent_receipt.at') IS NOT NULL
            )
            SELECT id, owner, json_extract(data, '$.sent_receipt.at'),
                   json_extract(data, '$.sent_receipt.internetMessageId'),
                   json_extract(data, '$.sent_receipt.evidence_ref'),
                   json_extract(data, packet || '.subject'), json_extract(data, packet || '.to'),
                   json_extract(data, packet || '.cc')
            FROM sent WHERE json_type(data, packet) = 'object'
        """):
            self.index_receipt(task_id, owner, sent_at, message_id, evidence, subject,
                               json.loads(to) if to else [], json.loads(cc) if cc else [])

    def index_receipt(self, task_id, owner, sent_at, message_id, evidence, subject, to, cc):
        if not message_id and (evidence or '').startswith('graph:sent:'):
            message_id = evidence.removeprefix('graph:sent:')
        self.index_sent({'subject': subject, 'sentDateTime': sent_at, 'internetMessageId': message_id,
                         'toRecipients': [{'emailAddress': {'address': a}} for a in to],
                         'ccRecipients': [{'emailAddress': {'address': a}} for a in cc]},
                        task_id, owner)

    def contact_ids(self, addresses):
        return {self.by_address[a]['id'] for a in addresses if a in self.by_address}

    def blocked(self, addresses, owners, sender_name):
        addresses = addresses | {a for owner in owners for a in self.by_id[owner]['emails']}
        if any(addresses.intersection(r['emails']) or
               (sender_name and name_key(sender_name) == name_key(r['name'])) for r in self.refusals):
            return 'Active table-3 refusal in the conversation'
        return None

    def classify(self, message, *, intake_reason=None):
        sender = address(message.get('from', {}))
        subject = message.get('subject') or ''
        domain = sender.partition('@')[2].rstrip('.')
        if (intake_reason or message.get('intake_reason')) == 'owner-controlled':
            return category('owner-controlled', 'Owner-controlled conversation')
        person = self.by_address.get(sender)
        if person:
            return category('known-contact', 'Sender address is registered', contact_id=person['id'])
        if matches_domain(domain, self.home_domains):
            return category('institution', 'Sender is from the mailbox owner’s institution')
        if matches_domain(domain, self.domains):
            if not (EDITORIAL_ACTION.search(subject) or EDITORIAL_CONFIRMATION.search(subject)):
                return category('editorial-info', 'Journal or preprint notification')
            details = category('editorial-action', 'Journal or preprint request or decision')
            identities = manuscript_ids(subject)
            if identities:
                identity = next((i for i in identities if i.casefold() in self.manuscripts), identities[0])
                details['manuscript_id'] = identity
                if identity.casefold() in self.manuscripts:
                    details['joint_manuscript'] = True
                    coauthors = self.manuscripts[identity.casefold()]
                    if coauthors:
                        details['coauthor_contact_ids'] = sorted(coauthors)
            return details
        if matches_domain(domain, self.service_domains):
            return category('service-notice', 'Sender is from a configured collaboration service')
        reason = bulk_reason(sender, _message_headers(message))
        if reason:
            return category('bulk', reason)

        matches = self.subjects.get(normalized_subject(subject), [])
        details, _ = sent_subject_match(message, self.persons, self.account, matches)
        if details:
            return details
        received = date_order(message.get('receivedDateTime'))
        named = name_tokens(((message.get('from') or {}).get('emailAddress') or {}).get('name'))
        if len(named) >= 2:
            candidates = [i for i in self.names.get(named, ()) if
                          any(date_order(at) <= received for at in self.written.get(i, ()))]
            if len(candidates) == 1:
                return category('likely-contact', 'Display name matches one registered prior correspondent',
                                contact_id=candidates[0])
        if sender and (self.account in recipients(message) or not self.account or _header_message_ids(message)):
            return category('person', 'Unmapped sender addressed this mailbox directly')
        return category('bulk', 'No direct recipient or registered sender in mailbox metadata')


def inventory(mailbox, rows, root, config):
    unmapped = mailbox.get('unmapped', [])
    if unmapped:
        account = mailbox.get('account') or config.get('mailbox', {}).get('account', '').lower()
        triage = Triage(root, config, rows, account)
        refreshed = []
        for message in unmapped:
            details = triage.classify(message)
            if message.get('category') is None or message.get('intake_reason') is None:
                message = {**{k: v for k, v in message.items() if k not in ANNOTATION_FIELDS}, **details}
            elif message['category'] == details['category'] == 'editorial-action':
                message = {**message, **{k: v for k, v in details.items() if k != 'category_reason'}}
            elif message['category'] == 'editorial-info' and details['category'] == 'editorial-action':
                message = {**message, **details}
            refreshed.append(message)
        unmapped = refreshed
    attention = []
    for m in sorted(unmapped, key=lambda m: date_order(m.get('receivedDateTime')), reverse=True):
        if m['category'] not in ATTENTION_PRIORITY:
            continue
        sender = (m.get('from') or {}).get('emailAddress') or {}
        attention.append({'received': m.get('receivedDateTime'), 'from': sender.get('address'),
                          'name': sender.get('name'), 'subject': m.get('subject'),
                          **{k: m[k] for k in ('category', 'category_reason', 'manuscript_id',
                                               'joint_manuscript', 'coauthor_contact_ids', 'contact_id',
                                               'task_id') if k in m}})
    attention.sort(key=lambda m: ATTENTION_PRIORITY[m['category']])
    return {'mailbox_needs_classification': unmapped, 'mailbox_attention': attention,
            'mailbox_category_counts': dict(Counter(m['category'] for m in unmapped))}
