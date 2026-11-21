"""The human-readable Outreach map and its three maintained social tables.

These commands edit index records, never email or local research. Commit related
JSON changes and regenerated Markdown together. The hosted task ledger remains
owned by the serialized coordinator.
"""
import argparse
from collections import Counter
from datetime import date, datetime, timezone
from functools import lru_cache
import hashlib
import json
from pathlib import Path
import re
import shlex
import unicodedata

from .queue import digest, require

from .location import DATA_ROOT as ROOT, DATA_REPO, data_file, owner_labels


def read(root, name):
    return json.loads((Path(root) / 'tables' / (name + '.json')).read_text())


def write(root, name, value):
    path = Path(root) / 'tables' / (name + '.json')
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')


def identity(prefix, value):
    return prefix + '-' + hashlib.sha256(value.encode()).hexdigest()[:16]


def email(value):
    require(isinstance(value, str), 'Use a bare email address')
    value = value.strip().casefold()
    require(re.fullmatch(r'[^\s@<>]+@[^\s@<>]+\.[^\s@<>]+', value) is not None, 'Use a bare email address')
    return value


def body_addresses(message):
    """Extract whole, literal addresses from Graph's plain-text message body."""
    body = message.get('body') or {}
    if str(body.get('contentType', '')).casefold() != 'text':
        return set()
    return {value.rstrip('.').casefold() for value in re.findall(
        r"[\w.!#$%&'*+/=?^`{|}~-]+@[\w-]+(?:\.[\w-]+)+", body.get('content', ''))}


def mail_thread_contacts(messages, latest, addresses):
    """Return identity evidence only from this conversation's actual messages.

    Header names identify participants, not academic titles or verified aliases.
    Body-only identities retain an adjacent explicit name, or the address itself.
    """
    if not latest or not latest.get('conversationId'):
        return []
    found = {}
    wanted = {email(a) for a in addresses}
    for message in messages:
        if (message.get('conversationId') != latest['conversationId'] or message.get('isDraft') or
                not (message.get('id') or message.get('internetMessageId'))):
            continue
        evidence = {'message_id': message.get('id'), 'internet_message_id': message.get('internetMessageId'),
                    'thread_id': latest['conversationId']}
        for field in ('from', 'toRecipients', 'ccRecipients'):
            values = [message.get(field) or {}] if field == 'from' else message.get(field, [])
            for value in values:
                header = value.get('emailAddress') or {}
                address = str(header.get('address', '')).strip().casefold()
                if address in wanted:
                    name = str(header.get('name') or address).strip()
                    if address not in found or found[address]['name'] == address:
                        found[address] = {'email': address, 'name': name or address,
                                          'evidence': {**evidence, 'field': field}}
    if latest.get('id') or latest.get('internetMessageId'):
        text = (latest.get('body') or {}).get('content', '')
        for address in sorted(wanted & body_addresses(latest) - found.keys()):
            # Only an explicit adjacent name is used; prose is not a title source.
            match = re.search(r'([A-Z][\w\'-]*(?: [A-Z][\w\'-]*){1,5})\s*[<(]\s*' +
                              re.escape(address) + r'\s*[>)]', text)
            found[address] = {'email': address, 'name': match[1] if match else address,
                              'evidence': {'message_id': latest.get('id'),
                                           'internet_message_id': latest.get('internetMessageId'),
                                           'thread_id': latest['conversationId'], 'field': 'body'}}
    return list(found.values())


def register_mail_thread_contacts(contacts, root=ROOT, apply=True):
    """Reuse table-1 registration and table-3 checks, without claiming a title."""
    if not contacts:
        return []
    added = []
    known = {a for person in people(root) for a in person['emails']}
    for contact in contacts:
        address = email(contact['email'])
        if address in known:
            continue
        check = preflight(address, contact['name'], root, new_outreach=False)
        require(check['allowed'], 'Table 3 blocks mail-thread registration')
        if apply:
            register(address, contact['name'], root=root, mail_thread=contact['evidence'])
        known.add(address)
        added.append(address)
    return added


def reply_mail_thread_contacts(row):
    """Use hydrated inputs too; inventory requests can predate thread resolution."""
    from .thread_staging import effective_inputs
    inputs = effective_inputs(row)
    packet = (row.get('data', {}).get('result') or row.get('result') or {}).get('packet') or {}
    wanted = packet.get('latest_inbound_message_id') or inputs.get('latest_inbound_message_id')
    messages = inputs.get('thread') or []
    latest = next((m for m in messages if m.get('internetMessageId') == wanted and
                   not m.get('isDraft') and
                   m.get('conversationId') == (packet.get('thread_id') or inputs.get('thread_id'))), None)
    if not latest:
        return []
    sender = latest.get('from', {}).get('emailAddress', {}).get('address')
    addresses = [*packet.get('to', []), *packet.get('cc', []), *([sender] if sender else [])]
    return mail_thread_contacts(messages, latest, addresses)


def name_key(value):
    return ' '.join(unicodedata.normalize('NFKC', value).casefold().split())


def event_date(value):
    require(isinstance(value, str) and re.fullmatch(r'\d{4}-\d{2}-\d{2}(?:T.+)?', value), 'Use an ISO date/time')
    if len(value) == 10:
        date.fromisoformat(value)
    else:
        require(datetime.fromisoformat(value).tzinfo is not None, 'Event timestamps need a timezone')
    return value


def date_order(value):
    # Historical snapshots include dates and unzoned times. Preserve their text;
    # use UTC only as an ordering fallback for those imported values.
    if not value: return datetime.min.replace(tzinfo=timezone.utc)
    point = datetime.fromisoformat(str(value))
    return point.replace(tzinfo=point.tzinfo or timezone.utc).astimezone(timezone.utc)


def workspace_path(value):
    return (isinstance(value, str) and re.fullmatch(r'\.\./mail/[A-Za-z0-9][A-Za-z0-9._/-]*', value)
            and '..' not in value.removeprefix('../mail/').split('/'))


def people(root=ROOT):
    """Merge only explicitly recorded aliases, preserving all original contact IDs."""
    rows = read(root, 'contacts')['contacts']
    parent = {}

    def find(x):
        parent.setdefault(x, x)
        if parent[x] != x:
            parent[x] = find(parent[x])
        return parent[x]

    def union(a, b):
        parent[find(b)] = find(a)

    for row in rows:
        address = email(row['email']); find(address)
        rel = row.get('relationship') or {}
        aliases = list(rel.get('aliases') or [])
        aliases += [rel.get('alias_of'), rel.get('canonical_email')]
        title = row.get('salutation') or {}
        if title.get('verified'):
            aliases += title.get('emails', [])
        for alias in aliases:
            if isinstance(alias, str) and '@' in alias:
                union(address, email(alias))
    # Explicit duplicate entries in the imported refusal registry retain aliases.
    for item in read(root, 'refusals')['refusals']:
        for alias in item['emails'][1:]:
            union(item['emails'][0], alias)
    grouped = {}
    for row in rows:
        grouped.setdefault(find(email(row['email'])), []).append(row)
    # Group every address under its root once; a per-group scan is quadratic
    # and dominated admission gates (about 1 s per call with 1,500 contacts).
    members = {}
    for address in list(parent):
        members.setdefault(find(address), []).append(address)
    result = []
    for key, records in grouped.items():
        records.sort(key=lambda r: r['email'])
        # Prefer an explicitly recorded current address, otherwise keep a stable one.
        preferred = next(((r.get('relationship') or {}).get('canonical_email') for r in records
                          if (r.get('relationship') or {}).get('canonical_email')), None)
        primary = next((r for r in records if r['email'] == preferred), records[0])
        display = primary['name']
        if '@' in display or not display.strip():
            display = next((r['name'] for r in records if r['name'].strip() and '@' not in r['name']), display)
        aliases = sorted(members[key])
        result.append({'id': min(r['id'] for r in records), 'name': display,
                       'email': primary['email'], 'emails': aliases, 'records': records})
    return sorted(result, key=lambda p: (name_key(p['name']), p['email']))


def boundaries(person, refusals):
    addresses = set(person['emails'])
    return [r for r in refusals if r['active'] and addresses.intersection(r['emails'])]


def activity(person):
    incoming, outgoing, threads, events = [], [], {}, []
    for row in person['records']:
        rel = row.get('relationship') or {}
        if rel.get('last_sent') and any(rel.get(k) for k in ('last_sent_message_id', 'last_sent_mail_id', 'last_sent_receipt', 'verified_sent_record', 'verified_sent_message_id')):
            outgoing.append(str(rel['last_sent']))
        if rel.get('last_reply') and any(rel.get(k) for k in ('last_reply_message_id', 'last_reply_mail_id', 'last_reply_receipt')):
            incoming.append(str(rel['last_reply']))
        for thread in row.get('hub', {}).get('threads', []):
            # Co-recipient membership alone does not establish who wrote a reply.
            if thread.get('correspondent_email', row['email']) not in person['emails']:
                continue
            threads[thread['id']] = thread
            for key, target in [('last_in', incoming), ('last_out', outgoing)]:
                point = thread.get(key) or {}
                if point.get('date') and (point.get('ROWID') or point.get('message_id')):
                    target.append(str(point['date']))
        for receipt in row.get('hub', {}).get('historical_inbound', []):
            if (receipt.get('correspondent_email') in person['emails'] and receipt.get('date')
                    and receipt.get('rowid') and receipt.get('message_id')):
                incoming.append(receipt['date'])
        for event in row.get('hub', {}).get('events', []):
            events.append(event)
            if event['kind'] == 'sent': outgoing.append(event['at'])
            if event['kind'] == 'reply': incoming.append(event['at'])
    return {'last_in': max(incoming, key=date_order, default=''), 'last_out': max(outgoing, key=date_order, default=''),
            'threads': sorted(threads.values(), key=thread_date, reverse=True),
            'events': sorted(events, key=lambda e: date_order(e['at']), reverse=True)}


def thread_date(thread):
    return max(date_order((thread.get(k) or {}).get('date', '')) for k in ('last_in', 'last_out'))


def current_summary(person, detail):
    candidates = [(date_order(e['at']), e['summary']) for e in detail['events']]
    candidates += [(thread_date(t), t['next_action']) for t in detail['threads'] if t.get('next_action')]
    for row in person['records']:
        rel = row.get('relationship') or {}
        if rel.get('next_action'):
            candidates.append((max(date_order(rel.get(k, '')) for k in ('last_reply', 'last_sent')), rel['next_action']))
    return max(candidates, key=lambda c: c[0])[1] if candidates else ''


def preflight(address, name='', root=ROOT, new_outreach=True):
    address = email(address)
    persons = people(root)
    person = next((p for p in persons if address in p['emails']), None)
    aliases = set(person['emails']) if person else {address}
    matches = []
    for item in read(root, 'refusals')['refusals']:
        if item['active'] and (aliases.intersection(item['emails']) or
                              (name and name_key(item['name']) == name_key(name))):
            matches.append(item)
    reasons = []
    if matches: reasons.append('table-3-refusal-or-stop-boundary')
    if new_outreach and person and address in undeliverable_addresses(person):
        reasons.append('undeliverable-address')
    if new_outreach and person and (activity(person)['last_out'] or activity(person)['last_in']):
        reasons.append('already-contacted-use-the-existing-thread')
    if new_outreach and excluded_region(address, person, root):
        reasons.append('excluded-region-mainland-china')
    return {'allowed': not reasons, 'email': address, 'contact_id': person['id'] if person else None,
            'registered': person is not None, 'reasons': reasons,
            'refusal_ids': [r['id'] for r in matches],
            'scope': 'registry-preflight-only; mailbox freshness, workflow review and sending authority remain separate'}


def excluded_region(address, person, root=ROOT):
    """First contacts skip mainland-China universities and research institutes
    (tables/decisions.json firstcontact_excluded_regions). Existing threads are
    unaffected because replies call preflight with new_outreach=False."""
    path = Path(root) / 'tables/decisions.json'
    if not path.exists():
        return False
    rule = json.loads(path.read_text()).get('firstcontact_excluded_regions')
    if not rule:
        return False
    if any(address.endswith(s) for s in rule.get('domain_suffixes', [])):
        return True
    pattern, exempt = rule.get('affiliation_pattern'), rule.get('affiliation_exempt_pattern')
    if not pattern or not person:
        return False
    for record in person['records']:
        text = record.get('affiliation') or ''
        if isinstance(text, str) and re.search(pattern, text) and not (exempt and re.search(exempt, text)):
            return True
    return False


def firstcontact_gate(request, root=ROOT):
    if request['kind'] != 'firstcontact': return
    recipients = request['inputs'].get('recipient_emails')
    require(isinstance(recipients, list) and recipients, 'First contact requires all intended To/Cc recipient_emails for table 3 preflight')
    normalized = [email(a) for a in recipients]
    require(len(set(normalized)) == len(normalized), 'Duplicate first-contact recipient')
    for address in recipients:
        check = preflight(address, root=root)
        require(check['allowed'], 'First-contact index gate refused ' + address + ': ' + ', '.join(check['reasons']))
        require(check['registered'], 'Register the eligible recipient in table 1 before task admission: ' + address)


def register(address, name, topic='', workspace='', root=ROOT, problem_ids=None,
             title=None, title_evidence='', evidence='', affiliation='', mail_thread=None):
    address = email(address)
    check = preflight(address, name, root, new_outreach=False)
    require(check['allowed'], 'Table 3 blocks new outreach registration; retain incoming facts through record-event/refuse instead')
    rows = read(root, 'contacts')
    current = next((r for r in rows['contacts'] if r['email'] == address), None)
    person = next((p for p in people(root) if address in p['emails']), None)
    if current is None and person:
        current = next(r for r in rows['contacts'] if r['id'] == person['records'][0]['id'])
    # Validate the full edit before writing anything, including a new identity.
    from .author_contacts import problem_ids as validate_problem_ids, url
    if problem_ids:
        validate_problem_ids(problem_ids)
        new = set(problem_ids) - set(current.get('problem_ids', []) if current else [])
        require(not new or (isinstance(evidence, str) and evidence.strip()),
                'New problem links require --evidence establishing source authorship')
    if title is not None:
        require(title in {'Professor', 'Dr.', 'none'}, 'Invalid academic title')
        url(title_evidence)
    else:
        require(not title_evidence, '--title-evidence requires --title')
    if mail_thread is not None:
        require(isinstance(mail_thread, dict) and
                (mail_thread.get('message_id') or mail_thread.get('internet_message_id')) and
                mail_thread.get('thread_id') and mail_thread.get('field') in
                {'from', 'toRecipients', 'ccRecipients', 'body'}, 'Invalid mail-thread identity evidence')
    if current is None:
        require(bool(name.strip()), 'A contact needs a recorded name')
        current = {'id': identity('contact', address), 'name': name, 'email': address,
                   'problem_ids': [], 'policy_fresh': False,
                   'hub': {'threads': [], 'events': [], 'workspace_paths': []}}
        if mail_thread is not None:
            current.update(identity_evidence='mail-thread', mail_thread_evidence=mail_thread,
                           salutation={'title': None, 'verified': False})
        rows['contacts'].append(current)
    if problem_ids:
        current['problem_ids'] = sorted(set(current.get('problem_ids', [])) | set(problem_ids))
        links = current.setdefault('problem_links', [])
        for rid in sorted(set(problem_ids)):
            if evidence and not any(link['problem_id'] == rid for link in links):
                links.append({'problem_id':rid, 'evidence':evidence})
    if title is not None:
        current.setdefault('salutation', {}).update(
            title=None if title == 'none' else title, verified=True, evidence_url=title_evidence)
    if affiliation: current['affiliation'] = affiliation
    hub = current.setdefault('hub', {'threads': [], 'events': [], 'workspace_paths': []})
    if topic: hub['topic'] = topic
    if workspace:
        require(workspace_path(workspace), 'Use a portable ../mail/<person>/ path')
        if workspace not in hub['workspace_paths']: hub['workspace_paths'].append(workspace)
    rows['contacts'].sort(key=lambda r: r['email'])
    write(root, 'contacts', rows)
    return current


def link_problem(address, problem_ids, root=ROOT, **kwargs):
    check = preflight(address, root=root, new_outreach=False)
    require(check['allowed'], 'Table 3 blocks problem linking')
    person = next((p for p in people(root) if email(address) in p['emails']), None)
    require(person is not None, 'Register the contact before link-problem')
    require(problem_ids, 'link-problem needs at least one --problem-id')
    return register(address, person['name'], root=root, problem_ids=problem_ids, **kwargs)


def record_event(address, kind, at, summary, evidence, root=ROOT):
    require(kind in {'sent', 'reply', 'status'}, 'Unknown contact event')
    require(all(isinstance(x, str) and x.strip() for x in (at, summary, evidence)), 'An event needs date, summary and source evidence')
    event_date(at)
    person = next((p for p in people(root) if email(address) in p['emails']), None)
    require(person is not None, 'Record the contact before recording a communication event')
    rows = read(root, 'contacts')
    row = next(r for r in rows['contacts'] if r['id'] == person['records'][0]['id'])
    event = {'kind': kind, 'at': at, 'summary': summary, 'evidence': evidence}
    event['id'] = identity('event', json.dumps(event, sort_keys=True, ensure_ascii=False))
    events = row.setdefault('hub', {}).setdefault('events', [])
    if not any(e['id'] == event['id'] for e in events): events.append(event)
    write(root, 'contacts', rows)
    return event


def undeliverable_addresses(person):
    return {email(item['email']) for row in person['records']
            for item in row.get('hub', {}).get('undeliverable_addresses', [])}


def mark_undeliverable(address, evidence, root=ROOT):
    """Retain each NDR as address-specific history, including verified aliases."""
    address = email(address)
    require(isinstance(evidence, str) and evidence.strip(), 'An undeliverable address needs NDR evidence')
    person = next((p for p in people(root) if address in p['emails']), None)
    require(person is not None, 'Undeliverable address must match a table-1 contact or verified alias')
    data = read(root, 'contacts')
    row = next((r for r in data['contacts'] if r['email'] == address), None)
    if row is None:
        row = next(r for r in data['contacts'] if r['id'] == person['records'][0]['id'])
    entry = {'email': address, 'evidence': evidence}
    history = row.setdefault('hub', {}).setdefault('undeliverable_addresses', [])
    if entry not in history:
        history.append(entry)
        write(root, 'contacts', data)
    return entry


def bounce_updates(bounces, root=ROOT):
    """Build operator proposals in the work inventory's registry-update envelope."""
    from .mailbox import _delivery_failure
    if not bounces: return []
    persons = people(root)
    updates = []
    for bounce in bounces:
        if not _delivery_failure(bounce): continue
        address = bounce.get('original_recipient') or bounce.get('recipient')
        if not address: continue
        try: address = email(address)
        except ValueError: continue
        person = next((p for p in persons if address in p['emails']), None)
        if person is None: continue
        evidence = json.dumps({k: bounce.get(k) for k in
                               ('internetMessageId', 'receivedDateTime', 'subject')},
                              ensure_ascii=False, sort_keys=True)
        entry = {'email': address, 'evidence': evidence}
        recorded = any(entry in r.get('hub', {}).get('undeliverable_addresses', [])
                       for r in person['records'])
        evidence_ref = 'graph:inbox:' + str(bounce.get('id') or bounce.get('internetMessageId'))
        incoming = {'email': bounce.get('from', {}).get('emailAddress', {}).get('address'),
                    'at': bounce.get('receivedDateTime'), 'message_id': bounce.get('internetMessageId'),
                    'subject': bounce.get('subject'), 'evidence_ref': evidence_ref, 'text': None}
        updates.append({'task_id': None, 'incoming': incoming, 'incoming_text': None,
                        'subject': incoming['subject'], 'message_id': incoming['message_id'],
                        'evidence_ref': evidence_ref, 'sent_receipt': None,
                        'classification': {'kind': 'delivery-failure'},
                        'registry_patch': {'undeliverable': entry},
                        'delivery_failure': bounce, 'contact_id': person['id'], 'recorded': recorded,
                        'command': shlex.join(['python3', '-m', 'managed.registry', 'mark-undeliverable',
                                               '--email', address, '--evidence', evidence])})
    return updates


def add_praise(address, at, quote, context, story, evidence, root=ROOT, record=True):
    address = email(address)
    require(all(isinstance(x, str) and x.strip() for x in (quote, context, story, evidence)), 'Positive feedback needs exact words, context, story and evidence')
    person = next((p for p in people(root) if email(address) in p['emails']), None)
    require(person is not None, 'Positive feedback must link to table 1')
    event_date(at)
    entry = {'id': identity('positive', address + at + quote), 'kind': 'praise',
             'contact_ids': [person['id']], 'name': person['name'], 'date': at[:10],
             'quote': quote, 'context': context, 'story': story, 'evidence': evidence,
             'public_use': 'internal-source-material', 'review': 'operator-source-checked'}
    data = read(root, 'positive')
    old = next((r for r in data['entries'] if r['id'] == entry['id']), None)
    require(old is None or old == entry, 'This feedback already exists with different details; reconcile its source record explicitly')
    if record: record_event(address, 'reply', at, story, evidence, root)
    if not any(r['id'] == entry['id'] for r in data['entries']): data['entries'].append(entry)
    write(root, 'positive', data)
    return entry


def add_refusal(address, at, reason, quote, evidence, root=ROOT, scope='', record=True):
    address = email(address)
    require(all(isinstance(x, str) and x.strip() for x in (at, reason, evidence)), 'A refusal needs date, reason and source evidence')
    event_date(at)
    person = next((p for p in people(root) if email(address) in p['emails']), None)
    require(person is not None, 'Record the incoming correspondent in table 1 first')
    entry = {'id': identity('refusal', address + at + reason), 'contact_id': person['id'],
             'name': person['name'], 'emails': person['emails'], 'date': at[:10],
             'scope': scope or 'new-outreach-and-collaboration-invitations', 'active': True,
             'reason': reason, 'quote': quote, 'evidence': evidence, 'source': 'recorded-boundary', 'notes': {}}
    data = read(root, 'refusals')
    if not any(r['id'] == entry['id'] for r in data['refusals']): data['refusals'].append(entry)
    write(root, 'refusals', data)
    if record: record_event(address, 'reply', at, reason, evidence, root)
    return entry


def _validate_alias(patch):
    alias = patch.get('alias_of')
    if alias is not None:
        require(patch['new_contact'] is None,
                'registry_patch alias_of and new_contact are mutually exclusive')
        require(isinstance(alias, dict) and set(alias) == {'contact_id', 'evidence_ref', 'reason'} and
                all(isinstance(alias[k], str) and alias[k].strip() for k in alias),
                'Invalid registry_patch alias_of')
        reason = alias['reason']
        require(len(reason) <= 300 and len(reason.splitlines()) == 1 and
                re.search(r'[A-Za-z]', reason),
                'alias_of reason must be one short factual English sentence')
    return alias


def _registry_patch(patch):
    """Validate the operator-facing patch portion of a CMA result."""
    from .author_contacts import url
    require(isinstance(patch, dict) and set(patch) in (
            {'incoming_summary', 'praise', 'refusal', 'new_contact'},
            {'incoming_summary', 'praise', 'refusal', 'new_contact', 'alias_of'}),
            'Invalid registry_patch shape')
    summary = patch['incoming_summary']
    require(isinstance(summary, str) and summary.strip() and '\n' not in summary and
            re.search(r'[A-Za-z]', summary) and len(summary) <= 400,
            'incoming_summary must be one short factual English line')
    require(isinstance(patch['praise'], list), 'registry_patch praise must be a list')
    for item in patch['praise']:
        require(isinstance(item, dict) and set(item) == {'quote', 'context', 'story'} and
                all(isinstance(item[k], str) and item[k].strip() for k in item),
                'Invalid registry_patch praise entry')
    refusal = patch['refusal']
    require(refusal is None or (isinstance(refusal, dict) and set(refusal) == {'quote', 'scope'} and
                                all(isinstance(refusal[k], str) and refusal[k].strip() for k in refusal)),
            'Invalid registry_patch refusal')
    _validate_alias(patch)
    contact = patch['new_contact']
    if contact is not None:
        require(isinstance(contact, dict) and set(contact) ==
                {'name', 'title', 'title_evidence_url', 'affiliation', 'evidence_url'} and
                all(isinstance(contact[k], str) and contact[k].strip() for k in
                    ('name', 'title_evidence_url', 'affiliation', 'evidence_url')) and
                contact['title'] in {'Professor', 'Dr.', 'none'},
                'Invalid registry_patch new_contact')
        url(contact['title_evidence_url']); url(contact['evidence_url'])
    return patch


def _reconcile_contact_path(root):
    # Reviewed proposals are private operator files beside the repository:
    # ../mail/outreach-maintenance/author-contacts. Synthetic test roots keep
    # them inside their own temporary root.
    resolved = Path(root).resolve()
    mail = resolved.parent / 'mail'
    if mail.is_dir():
        return mail / 'outreach-maintenance' / 'author-contacts'
    return resolved / 'author-contacts'


def _event_recorded(root, message_id, evidence_ref):
    needles = [str(value) for value in (message_id, evidence_ref)
               if isinstance(value, str) and value]
    if not needles: return False
    for person in people(root):
        for row in person['records']:
            for event in row.get('hub', {}).get('events', []):
                evidence = event.get('evidence', '')
                if isinstance(evidence, (dict, list)):
                    evidence = json.dumps(evidence, ensure_ascii=False, sort_keys=True)
                if any(needle in str(evidence) for needle in needles): return True
    return False


def _refusal_recorded(root, address, at, reason):
    target = email(address)
    for item in read(root, 'refusals')['refusals']:
        if (item.get('date') == at[:10] and item.get('reason') == reason and
                target in item.get('emails', [])):
            return True
    return False


def _sent_event_recorded(root, address, receipt):
    for person in people(root):
        if address not in person['emails']: continue
        for row in person['records']:
            for event in row.get('hub', {}).get('events', []):
                if event.get('kind') != 'sent': continue
                source = event.get('evidence', '')
                try: evidence = json.loads(source) if isinstance(source, str) else source
                except ValueError: evidence = None
                if isinstance(evidence, dict) and 'recipient' in evidence:
                    if evidence['recipient'] == address and (
                            evidence.get('remote_id') == receipt['remote_id'] or
                            evidence.get('internetMessageId') == receipt['evidence_ref'].removeprefix('graph:sent:')):
                        return True
                # Retain manually recorded Sent evidence on its recipient row.
                # New evidence identifies the exact address even across aliases.
                elif email(row['email']) == address and any(value in str(source) for value in (
                        receipt['remote_id'], receipt['evidence_ref'],
                        receipt['evidence_ref'].removeprefix('graph:sent:'))):
                    return True
    return False


def _reconcile_contact_name(address, name, evidence, root, apply):
    """Recover a placeholder name from the incoming From header only."""
    if not isinstance(name, str) or not name.strip() or '@' in name:
        return None
    data = read(root, 'contacts')
    row = next((r for r in data['contacts'] if email(r['email']) == address), None)
    if row is None or name_key(row.get('name') or '') not in {'', name_key(address)}:
        return None
    matches = [p for p in people(root) if address not in p['emails'] and
               name_key(p['name']) == name_key(name)]
    if len(matches) > 1:
        return None
    if matches:
        name = matches[0]['name']
        relationship = {'alias_of': matches[0]['email'], 'evidence': evidence}
    else:
        name = ' '.join(name.split())
        relationship = {'name_evidence': evidence}
    action = {'email': address, 'action': 'alias' if matches else 'rename',
              'old_name': row.get('name'), 'name': name, 'relationship': relationship}
    if apply:
        row['name'] = name
        row['relationship'] = {**(row.get('relationship') or {}), **relationship}
        write(root, 'contacts', data)
    return action


def reconcile(work, apply=False, apply_refusals=False, root=ROOT):
    """Apply a saved ``managed.control work`` inventory in one serial pass.

    The default is a read-only audit.  When applying, all table mutations happen
    through the existing registry primitives and the generated views are rendered
    once after both registry and author-contact work is complete.
    """
    from . import author_contacts
    from .control import review_key
    root = Path(root)
    updates = work.get('registry_updates', []) if isinstance(work, dict) else []
    needed = work.get('author_contacts_needed', []) if isinstance(work, dict) else []
    counts = {'events': 0, 'praise': 0, 'contacts': 0, 'refusals': 0, 'undeliverable': 0,
              'author_contacts': 0, 'aliases': 0}
    exceptions, contact_names, pending_aliases = [], [], []

    def exception(task_id, address, reason):
        exceptions.append({'task_id': task_id, 'email': address or '', 'reason': str(reason)})

    # registry_updates contains receipts but no outgoing packet. affairs_tasks
    # carries the full result; the readback hash binds its subject and To/Cc.
    tasks = {item['id']: item for item in work.get('affairs_tasks', [])
             if isinstance(item, dict) and item.get('id')} if isinstance(work, dict) else {}
    # Register only identities proved by mailbox messages, including coauthors
    # on a reply that still fails the stricter latest-message recipient gate.
    contact_sources = {task_id: reply_mail_thread_contacts(task)
                       for task_id, task in tasks.items() if task.get('kind') == 'reply'}
    for item in updates:
        if isinstance(item, dict) and item.get('mail_thread_contacts'):
            contact_sources[item.get('task_id', '')] = item['mail_thread_contacts']
        patch = item.get('registry_patch') if isinstance(item, dict) else None
        if isinstance(patch, dict) and (patch.get('new_contact') or patch.get('alias_of')):
            # Let the explicit identity proposal retain a verified title or
            # attach an alias without creating a provisional duplicate row.
            task_id = item.get('task_id', '')
            sender = str((item.get('incoming') or {}).get('email') or '').casefold()
            contact_sources[task_id] = [c for c in contact_sources.get(task_id, []) if c['email'] != sender]
    for task_id, contacts in contact_sources.items():
        for contact in contacts:
            try:
                added = register_mail_thread_contacts([contact], root, apply=apply)
                if apply:
                    counts['contacts'] += len(added)
            except Exception as error:
                exception(task_id, contact.get('email'), str(error))
    receipts = {task_id: item['sent_receipt'] for task_id, item in tasks.items()
                if item.get('sent_receipt')}
    for item in updates:
        if isinstance(item, dict) and item.get('sent_receipt'):
            receipts.setdefault(item.get('task_id', ''), item['sent_receipt'])
    pending_outgoing, seen_sent = [], set()
    for task_id, receipt in receipts.items():
        try:
            task = tasks.get(task_id, {})
            require(task.get('kind') in {'firstcontact', 'reply'},
                    'Sent receipt requires a firstcontact or reply task in affairs_tasks')
            require(isinstance(receipt, dict) and all(
                isinstance(receipt.get(k), str) and receipt[k].strip()
                for k in ('remote_id', 'evidence_ref', 'at', 'content_hash')),
                'Sent receipt needs a Graph message ID, evidence, timestamp and content hash')
            require(receipt['evidence_ref'].startswith('graph:sent:') and
                    receipt['evidence_ref'].removeprefix('graph:sent:').strip(),
                    'Sent receipt must identify an actual Graph Sent message')
            packet = task.get('packet_override') or (task.get('result') or {}).get('packet')
            require(isinstance(packet, dict) and digest(packet) == receipt['content_hash'],
                    'Sent receipt does not match the task packet')
            at = event_date(receipt['at'])
            subject = packet.get('subject')
            require(isinstance(subject, str) and subject.strip(), 'Sent packet subject is missing')
            require(all(isinstance(packet.get(k), list) for k in ('to', 'cc')),
                    'Sent packet needs actual To/Cc recipient lists')
            addresses = list(dict.fromkeys(email(a) for a in packet['to'] + packet['cc']))
            require(addresses, 'Sent packet has no recipients')
        except Exception as error:
            exception(task_id, '', str(error)); continue
        for address in addresses:
            key = (receipt['remote_id'], address)
            if key in seen_sent or _sent_event_recorded(root, address, receipt): continue
            seen_sent.add(key)
            try:
                if not apply:
                    pending_outgoing.append({'task_id': task_id, 'email': address, 'kind': 'sent',
                                             'at': at, 'subject': subject,
                                             'remote_id': receipt['remote_id'],
                                             'evidence_ref': receipt['evidence_ref']})
                    continue
                if not any(address in p['emails'] for p in people(root)):
                    # A verified address is sufficient for a factual contact row;
                    # retain it as the display name until identity is researched.
                    register(address, address, root=root)
                    counts['contacts'] += 1
                evidence = json.dumps({**receipt, 'recipient': address}, ensure_ascii=False, sort_keys=True)
                record_event(address, 'sent', at, subject, evidence, root)
                counts['events'] += 1
            except Exception as error:
                exception(task_id, address, str(error))

    for item in updates:
        task_id = item.get('task_id', '') if isinstance(item, dict) else ''
        if not isinstance(item, dict):
            exception(task_id, '', 'registry update is not an object')
            continue
        patch = item.get('registry_patch')
        if isinstance(patch, dict) and 'undeliverable' in patch:
            try:
                # These proposals come from NDR intake, rather than an Agent's
                # incoming-summary patch. Recheck their source and current match.
                proposals = bounce_updates([item.get('delivery_failure') or {}], root)
                require(len(proposals) == 1 and patch == proposals[0]['registry_patch'],
                        'Undeliverable proposal does not match its NDR evidence and table-1 address')
                if not proposals[0]['recorded'] and apply:
                    mark_undeliverable(patch['undeliverable']['email'], patch['undeliverable']['evidence'], root)
                    counts['undeliverable'] += 1
            except Exception as error:
                entry = patch.get('undeliverable')
                exception(task_id, entry.get('email') if isinstance(entry, dict) else '', str(error))
            continue
        incoming = item.get('incoming') or {}
        if not isinstance(incoming, dict): incoming = {}
        # First contacts have an incoming object whose fields are all null.
        # Their Sent receipts were reconciled separately above.
        if not any(incoming.values()) and not item.get('registry_patch'):
            continue
        address = incoming.get('email') or item.get('email')
        message_id = incoming.get('message_id') or item.get('message_id')
        evidence_ref = incoming.get('evidence_ref') or item.get('evidence_ref')
        patch = item.get('registry_patch')
        already_recorded = item.get('recorded') or _event_recorded(root, message_id, evidence_ref)
        if already_recorded and not (apply and apply_refusals and isinstance(patch, dict) and patch.get('refusal')):
            continue
        at = incoming.get('at')
        text = item.get('incoming_text')
        if text is None: text = incoming.get('text')
        subject = item.get('subject') or incoming.get('subject')
        if patch is not None:
            try: _registry_patch(patch)
            except Exception as error:
                exception(task_id, address, str(error)); continue
        if not address:
            exception(task_id, '', 'incoming sender email is missing'); continue
        try: address = email(address)
        except Exception as error:
            exception(task_id, str(address), str(error)); continue
        if not at:
            exception(task_id, address, 'incoming timestamp is missing'); continue
        if not message_id or not evidence_ref:
            exception(task_id, address, 'incoming message_id and evidence_ref are required'); continue
        try: event_date(at)
        except Exception as error:
            exception(task_id, address, str(error)); continue
        summary = (patch or {}).get('incoming_summary') or subject
        if not isinstance(summary, str) or not summary.strip():
            exception(task_id, address, 'registry_patch.incoming_summary or subject is required'); continue
        evidence = str(evidence_ref) + '; ' + str(message_id)
        persons = people(root)
        person = next((p for p in persons if address in p['emails']), None)
        alias = (patch or {}).get('alias_of')
        if person is None and alias is not None:
            try:
                data = read(root, 'contacts')
                row = next((r for r in data['contacts'] if r['id'] == alias['contact_id']), None)
                require(row is not None, 'registry_patch alias_of contact_id does not exist')
                require(alias['evidence_ref'] in (message_id, evidence_ref),
                        'alias_of evidence_ref must identify the incoming message')
                if not apply:
                    pending_aliases.append({'task_id': task_id, 'email': address, **alias})
                else:
                    relationship = row.get('relationship') or {}
                    row['relationship'] = relationship
                    relationship['aliases'] = sorted(set(relationship.get('aliases') or []) | {address})
                    relationship.setdefault('alias_evidence', []).append({
                        'email': address, 'evidence_ref': alias['evidence_ref'],
                        'note': alias['reason'], 'task_id': task_id})
                    write(root, 'contacts', data)
                    counts['aliases'] += 1
                    person = next(p for p in people(root) if address in p['emails'])
            except Exception as error:
                exception(task_id, address, str(error)); continue
        if person is None and alias is None:
            contact = (patch or {}).get('new_contact')
            if contact is None:
                exception(task_id, address, 'sender is not registered and registry_patch.new_contact is missing')
                continue
            try:
                _registry_patch(patch)
                if apply:
                    register(address, contact['name'], root=root, title=contact['title'],
                             title_evidence=contact['title_evidence_url'],
                             affiliation=contact['affiliation'])
                    counts['contacts'] += 1
                    # Keep the independent identity source with the contact record.
                    data = read(root, 'contacts')
                    row = next(r for r in data['contacts'] if r['email'] == address)
                    row.setdefault('relationship', {})['identity_evidence_url'] = contact['evidence_url']
                    write(root, 'contacts', data)
                    person = next(p for p in people(root) if address in p['emails'])
            except Exception as error:
                exception(task_id, address, str(error)); continue
        if not already_recorded:
            try:
                if apply:
                    record_event(address, 'reply', at, summary.strip(), evidence, root)
                    counts['events'] += 1
                action = _reconcile_contact_name(address, incoming.get('name'), evidence, root, apply)
                if action:
                    contact_names.append({'task_id': task_id, **action})
            except Exception as error:
                exception(task_id, address, str(error)); continue

        patch = patch or {}
        for praise in patch.get('praise', []):
            quote = praise.get('quote', '')
            if not isinstance(text, str) or quote not in text:
                exception(task_id, address, 'praise quote is not an exact substring of the incoming plain text')
                continue
            if apply:
                try:
                    add_praise(address, at, quote, praise['context'], praise['story'], evidence,
                               root, record=False)
                    counts['praise'] += 1
                except Exception as error:
                    exception(task_id, address, str(error))
        refusal = patch.get('refusal')
        if refusal is not None:
            verified = isinstance(text, str) and refusal.get('quote', '') in text
            reason = 'refusal requires operator confirmation; use --apply-refusals'
            if not verified:
                reason = 'refusal quote is not an exact substring of the incoming plain text'
            elif apply and apply_refusals:
                try:
                    if not _refusal_recorded(root, address, at, refusal['scope']):
                        add_refusal(address, at, refusal['scope'], refusal['quote'], evidence,
                                    root, scope=refusal['scope'], record=False)
                        counts['refusals'] += 1
                    reason = 'refusal applied with --apply-refusals; operator confirmation remains required'
                except Exception as error:
                    reason = str(error)
            exception(task_id, address, reason)

    proposal_dir = _reconcile_contact_path(root)
    for item in needed:
        task_id = item.get('task_id', '') if isinstance(item, dict) else ''
        if isinstance(item, dict) and item.get('blocked'):
            continue
        value = item.get('reviewed_proposal') if isinstance(item, dict) else None
        address = ''
        if isinstance(item, dict):
            for proposal in item.get('proposals', []):
                address = (proposal.get('addresses') or [{}])[0].get('email', address)
                if address: break
        if not isinstance(value, dict):
            exception(task_id, address, 'author_contacts reviewed_proposal is missing'); continue
        # A stale snapshot can retain a proposal after every remaining address
        # has acquired a refusal/region boundary or an evidenced
        # omission.  Keep it visible in work() as blocked, without recurring
        # registry exceptions or a misleading registration failure.
        try:
            blocked = author_contacts.blocked(value, root)
            proposal_rows = item.get('proposals', []) if isinstance(item, dict) else []
            blocked_names = {entry.get('full_name') for entry in blocked}
            if ((not proposal_rows and blocked) or
                    (proposal_rows and all((proposal.get('proposal') or {}).get('full_name') in blocked_names
                                           for proposal in proposal_rows))):
                continue
        except (ValueError, KeyError, TypeError):
            pass
        try:
            authors = author_contacts.reviewed(value)
            if apply:
                # A stale work snapshot may still contain a proposal that a
                # prior pass fully registered.  Respect the same pending test
                # as work() so replaying it performs no new writes.
                pending = False
                for author in authors:
                    if not author.get('emails'):
                        pending = True
                        break
                    for item_email in author.get('emails', []):
                        target = email(item_email['email'])
                        person = next((p for p in people(root) if target in p['emails']), None)
                        linked = {rid for row in person['records'] for rid in row.get('problem_ids', [])} if person else set()
                        if person is None or set(author.get('problem_ids', [])) - linked:
                            pending = True
                            break
                    if pending: break
                if authors and not pending:
                    continue
                proposal_dir.mkdir(parents=True, exist_ok=True)
                path = proposal_dir / (str(task_id) + '.json')
                path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')
                result = author_contacts.apply(value, root, review_key(), render_views=False)
                counts['author_contacts'] += len(result.get('registered_or_linked', []))
                counts['author_contacts'] += len(result.get('linked_existing', []))
                for skipped in result.get('skipped', []):
                    exception(task_id, skipped.get('email', address), '; '.join(skipped.get('reasons', [])))
                for unreachable in result.get('unreachable', []):
                    exception(task_id, address, unreachable.get('email_omission_reason', 'author has no verified email'))
        except Exception as error:
            exception(task_id, address, str(error))

    if apply:
        render(root)
    return {'dry_run': not apply, 'applied': counts, 'pending_outgoing': pending_outgoing,
            'contact_names': contact_names, 'pending_aliases': pending_aliases, 'exceptions': exceptions}


def status(person, detail, blocked):
    if blocked: return '拒绝／停止新外联'
    stages = {t['stage'] for t in detail['threads']}
    if stages & {'joint-paper', 'joint_paper', 'joint_writing', 'collaboration'}: return '合作／合写'
    if undeliverable_addresses(person): return '投递异常'
    if any((r.get('relationship') or {}).get('status') in {'bounced', 'delivery_rejected', 'address_inactive'} for r in person['records']): return '投递异常'
    if detail['last_in']: return '已有来信／回应'
    if detail['last_out']: return '已联系·等待回应'
    return '在册·联系证据待核对'


def escape(value, limit=None):
    value = re.sub(r'\s+', ' ', str(value or '')).strip().replace('|', '\\|').replace('<', '&lt;').replace('>', '&gt;')
    if limit and len(value) > limit: value = value[:limit - 1] + '…'
    return value or '—'


@lru_cache(maxsize=12)
def id_lines(path, stamp):
    result = {}
    for number, line in enumerate(Path(path).read_text().splitlines(), 1):
        match = re.match(r'\s*"id": "([^\"]+)"', line)
        if match: result.setdefault(match[1], number)
    return result


def line_link(root, table, value):
    path = Path(root) / 'tables' / (table + '.json')
    number = id_lines(str(path), path.stat().st_mtime_ns).get(value)
    return table + '.json' + ('#L' + str(number) if number else '')


COLLABORATION_STATUS = {'active': '合作／研究交流', 'joint_paper': '联合写作',
                        'coauthorship_pending': '合著安排中', 'completed': '已完成合作'}
CREDIT_KIND = {'will_cite': '拟引用', 'cited': '已引用', 'will_acknowledge': '拟致谢',
               'acknowledged': '已致谢', 'coauthorship_offered': '合著邀请',
               'credited_other': '其他署名／归功'}


def positive_groups(entries, persons, kinds):
    """Group evidence by existing identities, never by a coincidentally equal name."""
    owner = {r['id']: p['id'] for p in persons for r in p['records']}
    groups = {}
    for entry in entries:
        if entry['kind'] not in kinds: continue
        for person_id in {owner[cid] for cid in entry['contact_ids']}:
            groups.setdefault(person_id, []).append(entry)
    return groups


def positive_view(root, persons, data, metrics):
    profiles = data.get('profiles', {})
    entries = data['entries']
    view = ['# 2 · 正面叙事与反馈', '',
        f'**合作与研究交流 {metrics["collaboration_people"]} 位 · 肯定我们工作的学者 {metrics["positive_people"]} 位 · 引用／致谢／合著邀请 {metrics["credit_people"]} 位**。各栏按明确邮箱别名去重，同一人可以出现在多栏。', '',
        f'保留 **{metrics["positive_quotes"]} 条反馈原话、{metrics["credit_records"]} 条引用／致谢等记录、{metrics["public_results"]} 项公开成果**。研究交流、合著安排、合著邀请与已发表成果保留各自状态。私人原话用于内部叙事素材，登记不触发发布；拒绝边界在[表 3](REFUSALS.md)继续生效。', '']
    if data.get('historical_source'):
        source = data['historical_source']
        view += [f'已恢复原维护表的全部记录（更新至 {source["header"]["updated_at"]}）；[原始版本](https://github.com/{DATA_REPO}/blob/{source["commit"]}/{source["path"]})与逐条邮件出处保留在数据中。此为历史快照，邮箱尚未重新对账。', '']

    def scholar(person):
        matching = [profiles[a] for a in person['emails'] if a in profiles]
        profile = max(matching, key=lambda p: sum(bool(p.get(k)) for k in
                      ('title', 'affiliation', 'research_area')), default={})
        name = profile.get('name') or person['name']
        text = f'[{escape(name)}]({line_link(root, "contacts", person["records"][0]["id"])})'
        background = ' · '.join(escape(profile[k]) for k in ('title', 'affiliation') if profile.get(k))
        if background: text += '<br>' + background
        if profile.get('research_area'): text += '<br>' + escape(profile['research_area'])
        return text

    def source_link(item):
        evidence = item.get('legacy_evidence') or item['evidence']
        if isinstance(evidence, str):
            try: evidence = json.loads(evidence)
            except ValueError: evidence = {}
        if isinstance(evidence, list):
            rowids = ', '.join(str(e['rowid']) for e in evidence if e.get('rowid'))
        else:
            rowids = str(evidence.get('rowid') or evidence.get('inbox_rowid') or '')
        label = item['date'] + (' · ROWID ' + rowids if rowids else '')
        return f'[{escape(label)}]({line_link(root, "positive", item["id"])})'

    for title, kinds, headings in [
        ('合作与研究交流', {'collaboration', 'collaboration_interest'},
         ['联系人／背景', '进展', '研究主题与合作内容', '出处']),
        ('肯定我们工作的学者', {'praise'},
         ['联系人／背景', '研究主题与评价', '对方原话', '出处']),
        ('引用、致谢与合著邀请', {'credit'},
         ['联系人／背景', '类型与涉及工作', '对方原话与具体记录', '出处'])]:
        groups = positive_groups(entries, persons, kinds)
        view += ['## ' + title, '', f'**{len(groups)} 位**；保留每项记录的具体内容与出处。', '',
                 '| ' + ' | '.join(headings) + ' |', '| --- | --- | --- | --- |']
        for person in persons:
            items = sorted(groups.get(person['id'], []), key=lambda e: e['date'], reverse=True)
            if not items: continue
            columns = [[], [], []]
            for item in items:
                if item['kind'] in {'collaboration', 'collaboration_interest'}:
                    columns[0].append(COLLABORATION_STATUS[item['status']] if item['kind'] == 'collaboration' else '合作意向')
                    detail = escape(item['context']) + '<br>' + escape(item['story'])
                    if item.get('project_ids'): detail += '<br>项目：' + escape(', '.join(item['project_ids']))
                    columns[1].append(detail)
                elif item['kind'] == 'praise':
                    topic = item.get('topic') or item.get('source_record', {}).get('topic') or item['context']
                    detail = escape(topic) + '<br>' + escape(item['story'])
                    columns[0].append(detail)
                    columns[1].append(escape(item['quote']))
                else:
                    label = CREDIT_KIND[item['credit_kind']]
                    # An offer being made does not establish an accepted authorship agreement.
                    state = ('已提出' if item['credit_status'] == 'done' else '拟议') if item['credit_kind'] == 'coauthorship_offered' else {'done': '已记录完成', 'promised': '已承诺'}[item['credit_status']]
                    columns[0].append(label + '（' + state + '）<br>我们的工作：' + escape(item['our_work']) + '<br>对方工作：' + escape(item['their_work']))
                    columns[1].append(escape(item['quote']) + '<br>' + escape(item['story']))
                columns[2].append(source_link(item))
            view.append('| ' + scholar(person) + ' | ' + ' | '.join('<br><br>'.join(c) for c in columns) + ' |')
        view.append('')
    view += ['## 已有公开成果', '', '| 成果 | 日期 | 已核实事实 | 公开来源 |', '| --- | --- | --- | --- |']
    for item in sorted(entries, key=lambda e: e['date'], reverse=True):
        if item['kind'] != 'public_result': continue
        view.append(f'| {escape(item["name"])} | {escape(item["date"])} | {escape(item["fact"])} | [公开来源]({item["evidence"]}) |')
    return view


def validate_historical_positives(data):
    """Keep the recovered snapshot intact while allowing current entries to evolve."""
    source = data.get('historical_source')
    if not source: return
    profiles = data.get('profiles', {})
    require(len(profiles) == source['contact_records'], 'Historical positive profiles were lost')
    buckets = {a: {'collaboration': {}, 'endorsements': {}, 'credit': {}} for a in profiles}
    for entry in data['entries']:
        ref = entry.get('source_ref')
        if not ref: continue
        require(ref.get('email') in buckets and ref.get('field') in buckets[ref['email']], 'Unknown historical positive source')
        bucket = buckets[ref['email']][ref['field']]
        index = ref.get('index')
        require(type(index) is int and index >= 0 and index not in bucket, 'Duplicate historical positive source')
        require(isinstance(entry.get('source_record'), dict), 'Missing historical positive record')
        bucket[index] = entry['source_record']
    recovered = {}
    for address, profile in profiles.items():
        fields = buckets[address]
        for records in fields.values():
            require(sorted(records) == list(range(len(records))), 'Historical positive records were lost')
        require(len(fields['collaboration']) <= 1, 'Duplicate historical collaboration')
        recovered[address] = dict(profile, collaboration=fields['collaboration'].get(0),
                                  endorsements=list(fields['endorsements'].values()),
                                  credit=list(fields['credit'].values()))
        # Entries may be reordered independently of their immutable source indices.
        for field in ('endorsements', 'credit'):
            recovered[address][field] = [fields[field][i] for i in sorted(fields[field])]
    value = dict(source['header'], contacts=recovered)
    digest = hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    require(digest == source['content_sha256'], 'Historical positive snapshot is incomplete or altered')


def registry_handoff(root):
    guide = data_file(root, 'registry-handoff.md')
    return guide.read_text().splitlines() if guide.is_file() else []


def render(root=ROOT, check=False, paths=None):
    root = Path(root); persons = people(root); refusals = read(root, 'refusals')['refusals']; positive_data = read(root, 'positive'); positives = positive_data['entries']
    rows = [(p, activity(p), boundaries(p, refusals)) for p in persons]
    counts = Counter(status(p, a, b) for p, a, b in rows)
    sent = sum(bool(a['last_out']) for _, a, _ in rows)
    inbound = sum(bool(a['last_in']) for _, a, _ in rows)
    quotes = [p for p in positives if p['kind'] == 'praise']
    metrics = {'people': len(persons), 'addresses': len(read(root, 'contacts')['contacts']), 'contacted': sent,
               'with_inbound': inbound, 'blocked_people': sum(bool(b) for _, _, b in rows), 'positive_quotes': len(quotes),
               'positive_stories': len(positives), 'public_results': sum(p['kind'] == 'public_result' for p in positives),
               'positive_people': len(positive_groups(positives, persons, {'praise'})),
               'collaboration_people': len(positive_groups(positives, persons, {'collaboration', 'collaboration_interest'})),
               'credit_people': len(positive_groups(positives, persons, {'credit'})),
               'credit_records': sum(p['kind'] == 'credit' for p in positives)}
    snapshot = '历史记录快照；本轮已核对邮箱处理边界，尚未更新新来信。下面的往来日期和待办不代表刚从服务器读取。'
    contacts = ['# 1 · 完整联络表', '', snapshot, '',
        f'在册 **{len(persons)} 位联系人 / {metrics["addresses"]} 条邮箱记录**；有历史发信证据 **{sent} 位**；有来信证据 **{inbound} 位**。仅合并来源明确的邮箱别名。', '',
        '状态、回应、合作方向和本地入口统一在这里维护。候选或证据待核对的记录不计入已联系人次。点击姓名查看机器记录；“本地”路径供本机访问，GitHub 不承载目录内容。', '',
        '| 联系人 | 邮箱（含已核实别名） | 动态 | 最近发信 | 最近来信 | 主题／下一步 | 本地工作区 |',
        '| --- | --- | --- | --- | --- | --- | --- |']
    for person, detail, blocked in rows:
        topic = next((r.get('hub', {}).get('topic') for r in person['records'] if r.get('hub', {}).get('topic')), '')
        latest = detail['threads'][0] if detail['threads'] else {}
        next_action = current_summary(person, detail)
        topic = topic or latest.get('subject') or next((r['problem_ids'][0] for r in person['records'] if r.get('problem_ids')), '')
        workspace_paths = sorted({p for r in person['records'] for p in r.get('hub', {}).get('workspace_paths', [])})
        local = '<br>'.join('`' + escape(p) + '`' for p in workspace_paths) or '尚无已核实路径'
        ref = line_link(root, 'contacts', person['records'][0]['id'])
        failed = undeliverable_addresses(person)
        addresses = ', '.join(a + ('（投递失败）' if a in failed else '') for a in person['emails'])
        contacts.append(f'| [{escape(person["name"])}]({ref}) | {escape(addresses)} | {status(person, detail, blocked)} | {escape(detail["last_out"][:10])} | {escape(detail["last_in"][:10])} | {escape(topic, 110)}<br>{escape(next_action, 180)} | {local} |')
    positive = positive_view(root, persons, positive_data, metrics)
    refusal_view = ['# 3 · 拒绝与停止外联表', '',
        '所有新外联先查本表，覆盖 To、Cc 和已核实别名。命中后停止新外联或再次邀约；不删除既有联络史。表中既有对方明确拒绝，也有原管线已登记的停止边界，来源类型逐条保留。新称赞不会自动解除拒绝。', '',
        '| 联系人 | 覆盖邮箱 | 日期 | 边界与理由 | 对方原话 | 依据 |', '| --- | --- | --- | --- | --- | --- |']
    for item in sorted(refusals, key=lambda p: name_key(p['name'])):
        if not item['active']: continue
        refusal_view.append(f'| {escape(item["name"])} | {escape(", ".join(item["emails"]))} | {escape(item["date"])} | {escape(item["reason"])} | {escape(item["quote"])} | [{escape(item["source"])}]({line_link(root, "refusals", item["id"])}) |')
    actions = ['# 待办与需决定事项', '', snapshot, '',
        '这是联络表和决策记录的阅读视图，不新增一套手工状态。新任务先查表 3；已有联系人继续更新表 1；好评进入表 2。', '',
        *registry_handoff(root),
        '- [用户归属、暂缓与决策记录](decisions.json)', '',
        '| 联系人 | 当前记录的下一步 | 归属／状态 |', '| --- | --- | --- |']
    for person, detail, blocked in rows:
        if blocked: continue
        pending = [t for t in detail['threads'] if t.get('ball') in {'us', 'user', owner_labels()[0]}]
        if not pending: continue
        t = pending[0]
        actions.append(f'| {escape(person["name"])} | {escape(t["next_action"], 240)} | {escape(t["ball"])}；历史快照，新来信未更新 |')
    template = data_file(root, 'registry-homepage.md')
    homepage = '# Outreach registry\n\n- [Submission board](tables/SUBMISSIONS.md)\n'
    if template.is_file():
        from string import Template
        homepage = Template(template.read_text()).substitute(
            **metrics, sent=sent, inbound=inbound, quote_count=len(quotes), snapshot=snapshot)

    outputs = {'README.md': homepage, 'tables/CONTACTS.md': '\n'.join(contacts) + '\n',
               'tables/POSITIVE.md': '\n'.join(positive) + '\n', 'tables/REFUSALS.md': '\n'.join(refusal_view) + '\n',
               'tables/ACTIONS.md': '\n'.join(actions) + '\n'}
    stale = []
    for name, text in outputs.items():
        if paths is not None and name not in paths: continue
        path = data_file(root, name) if name == 'README.md' else root / name
        if check:
            if not path.exists() or path.read_text() != text: stale.append(name)
        else: path.write_text(text)
    require(not stale, 'Regenerate the readable map: python3 -m managed.registry render; stale files: ' + ', '.join(stale))
    from .submissions import render as render_submissions
    if paths is None or 'tables/SUBMISSIONS.md' in paths:
        render_submissions(root, check=check)
    return metrics


def validate(root=ROOT):
    contacts = read(root, 'contacts')['contacts']
    ids = {r['id'] for r in contacts}
    require(len(ids) == len(contacts), 'Duplicate contact ID')
    for row in contacts:
        email(row['email'])
        from .author_contacts import validate as validate_authors, url
        require(isinstance(row.get('problem_ids', []), list)
                and all(isinstance(p, str) and p.strip() for p in row.get('problem_ids', []))
                and len(row.get('problem_ids', [])) == len(set(row.get('problem_ids', []))),
                'Invalid contact problem_ids')
        links = row.get('problem_links', [])
        require(isinstance(links, list), 'Invalid problem-link provenance')
        linked = set()
        for link in links:
            require(isinstance(link, dict) and set(link) == {'problem_id', 'evidence'}
                    and isinstance(link['problem_id'], str) and link['problem_id'] in row.get('problem_ids', [])
                    and link['problem_id'] not in linked
                    and isinstance(link['evidence'], str) and link['evidence'].strip(), 'Invalid problem-link evidence')
            linked.add(link['problem_id'])
        title = row.get('salutation') or {}
        if 'evidence_url' in title:
            url(title['evidence_url'])
            require(title.get('title') in {'Professor', 'Dr.', None} and title.get('verified') is True,
                    'Invalid verified title')
        if 'affiliation' in row:
            require(isinstance(row['affiliation'], str) and row['affiliation'].strip(), 'Invalid affiliation')
        if 'author_contact_sources' in row:
            require(isinstance(row['author_contact_sources'], list), 'Invalid author-contact sources')
            for source in row['author_contact_sources']:
                validate_authors({'author_contacts':[source]})
        hub = row.get('hub') or {}
        failures = hub.get('undeliverable_addresses', [])
        require(isinstance(failures, list), 'Invalid undeliverable-address history')
        for item in failures:
            require(isinstance(item, dict) and set(item) == {'email', 'evidence'}
                    and isinstance(item['evidence'], str) and item['evidence'].strip(),
                    'An undeliverable address needs NDR evidence')
            email(item['email'])
        require(all(workspace_path(p) for p in hub.get('workspace_paths', [])), 'Use relative mail workspace pointers')
        events = hub.get('events', [])
        require(len({e['id'] for e in events}) == len(events), 'Duplicate contact event')
        for e in events:
            event_date(e['at'])
            require(e['kind'] in {'sent', 'reply', 'status'} and e.get('summary') and e.get('evidence'), 'Incomplete contact event')
        for receipt in hub.get('historical_inbound', []):
            email(receipt.get('correspondent_email'))
            event_date(receipt.get('date'))
            require(receipt.get('rowid') and receipt.get('message_id') and receipt.get('source'), 'Incomplete historical inbound evidence')
    for table, key in [('refusals', 'refusals'), ('positive', 'entries')]:
        data = read(root, table)
        if table == 'positive': validate_historical_positives(data)
        rows = data[key]
        require(len({r['id'] for r in rows}) == len(rows), 'Duplicate ' + table + ' ID')
        for r in rows:
            require(bool(r.get('evidence')), 'Missing ' + table + ' evidence')
            if table == 'refusals':
                require(r['contact_id'] in ids and r['emails'] and r['reason'], 'Invalid refusal record')
                require(type(r['active']) is bool and r.get('scope'), 'Missing refusal boundary')
                for address in r['emails']: email(address)
            else:
                require(r['kind'] in {'praise', 'public_result', 'collaboration_interest', 'collaboration', 'credit'}, 'Unknown positive story kind')
                require(all(cid in ids for cid in r['contact_ids']), 'Unknown story contact')
                require(bool(r.get('context') and r.get('story')), 'Positive stories need scope and context')
                event_date(r['date'])
                require(r.get('public_use') in {'internal-source-material', 'public-fact'}, 'Record positive story publication scope')
                if r['kind'] in {'praise', 'collaboration_interest', 'credit'}: require(bool(r.get('quote') and r['contact_ids']), 'Feedback needs exact words and a correspondent')
                if r['kind'] == 'public_result': require(bool(r.get('fact')), 'A public result needs an actual fact')
                if r['kind'] == 'collaboration':
                    require(r['contact_ids'] and r.get('status') in COLLABORATION_STATUS, 'Record the actual collaboration status')
                    require(isinstance(r['evidence'], list) and r['evidence'], 'Collaboration needs message evidence')
                    for item in r['evidence']:
                        require(item.get('rowid') and item.get('message_id'), 'Collaboration needs message identifiers')
                        event_date(item.get('date'))
                if r['kind'] == 'credit':
                    require(r.get('credit_kind') in CREDIT_KIND and r.get('credit_status') in {'promised', 'done'}, 'Record the credit type and status')
                    require(r.get('our_work') or r.get('their_work'), 'Credit needs the work being credited')
    return render(root, check=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='action', required=True)
    p = commands.add_parser('render'); p.add_argument('--check', action='store_true')
    commands.add_parser('check')
    p = commands.add_parser('preflight'); p.add_argument('--email', required=True); p.add_argument('--name', default='')
    p = commands.add_parser('register'); p.add_argument('--email', required=True); p.add_argument('--name', required=True); p.add_argument('--topic', default=''); p.add_argument('--workspace', default='')
    p = commands.add_parser('link-problem'); p.add_argument('--email', required=True)
    for action in ('register', 'link-problem'):
        p = commands.choices[action]
        p.add_argument('--problem-id', dest='problem_ids', action='append', required=action == 'link-problem')
        p.add_argument('--title', choices=['Professor', 'Dr.', 'none'])
        p.add_argument('--title-evidence', default='')
        p.add_argument('--evidence', default='')
        p.add_argument('--affiliation', default='')
    p = commands.add_parser('apply-author-contacts'); p.add_argument('proposal', type=Path)
    p = commands.add_parser('reconcile'); p.add_argument('--work', required=True, type=Path)
    p.add_argument('--apply', action='store_true'); p.add_argument('--apply-refusals', action='store_true')
    p.add_argument('--push', action='store_true', help='Push reconciled views to the configured data branch')
    p = commands.add_parser('record-event'); p.add_argument('--email', required=True); p.add_argument('--kind', choices=['sent', 'reply', 'status'], required=True); p.add_argument('--at', required=True); p.add_argument('--summary', required=True); p.add_argument('--evidence', required=True)
    p = commands.add_parser('mark-undeliverable'); p.add_argument('--email', required=True); p.add_argument('--evidence', required=True)
    p = commands.add_parser('praise'); p.add_argument('--email', required=True); p.add_argument('--at', required=True); p.add_argument('--quote', required=True); p.add_argument('--context', required=True); p.add_argument('--story', required=True); p.add_argument('--evidence', required=True)
    p = commands.add_parser('refuse'); p.add_argument('--email', required=True); p.add_argument('--at', required=True); p.add_argument('--reason', required=True); p.add_argument('--quote', default=''); p.add_argument('--evidence', required=True)
    args = vars(parser.parse_args(argv)); action = args.pop('action')
    if action == 'render': result = render(ROOT, check=args['check'])
    elif action == 'check': result = validate(ROOT)
    elif action == 'apply-author-contacts':
        from .author_contacts import apply as apply_authors
        from .control import review_key
        result = apply_authors(json.loads(args['proposal'].read_text()), ROOT, review_key())
    elif action == 'reconcile':
        if args['push']:
            from .location import ENGINE_ROOT
            require(args['apply'] or args['apply_refusals'], '--push requires a reconciliation write')
            require(ROOT != ENGINE_ROOT, '--push requires a separate OUTREACH_DATA_ROOT checkout')
        result = reconcile(json.loads(args['work'].read_text()), apply=args['apply'],
                           apply_refusals=args['apply_refusals'], root=ROOT)
        if args['push']:
            from .data_writes import commit_paths, push
            paths = ('tables/contacts.json', 'tables/positive.json', 'tables/refusals.json',
                     'tables/CONTACTS.md', 'tables/POSITIVE.md', 'tables/REFUSALS.md',
                     'tables/ACTIONS.md', 'README.md')
            if commit_paths(paths, 'Reconcile registry records and views', ROOT):
                push(ROOT)
    else:
        address = args.pop('email')
        if action == 'preflight': result = preflight(address, root=ROOT, **args)
        else:
            function = {'register': register, 'link-problem': link_problem, 'record-event': record_event, 'mark-undeliverable': mark_undeliverable, 'praise': add_praise, 'refuse': add_refusal}[action]
            result = function(address, root=ROOT, **args); render(ROOT)
    if action == 'reconcile':
        print(json.dumps(result, ensure_ascii=False, separators=(',', ':')))
    else:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    if action == 'preflight' and not result['allowed']: raise SystemExit(2)


if __name__ == '__main__': main()
