"""Historical-thread requests and coordinator-only conversation hydration.

Requests are immutable. Verified Graph material lives in task data and is supplied
separately to CMA; effective_inputs is only an execution/readback view.
"""
import copy
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from .location import owner_labels, source_head
from .queue import digest, require
from .registry import email, people, preflight
from .tasks import ACTIVE, STATES


MAX_CONVERSATIONS = 3
MAX_MESSAGES = 100
LEGACY_RESOLUTION_REASON = 'Thread resolution failed: GateError: Known messages do not identify one conversation'
# A revising parent has handed off to its continuation, which carries the target.
INBOUND_STATES = STATES - {'verified', 'failed', 'no-reply', 'superseded', 'answered-by-mail',
                           'handoff-ready', 'round-complete', 'submission-ready', 'revising', 'owner-discarded'}
FINISHED_INBOUND_STATES = {'verified', 'no-reply', 'answered-by-mail', 'owner-discarded'}
# The operator may deliberately restage a message whose task is parked, saved as
# an owner draft or waiting for a user answer; only work still in flight blocks it.
IN_FLIGHT_STATES = ACTIVE | {'queued', 'awaiting-review', 'bounced', 'approved', 'awaiting-mailbox'}


def thread_record(root, identity):
    rows = json.loads((Path(root)/'tables/threads.json').read_text())['lines']
    row = next((r for r in rows if r['id'] == identity), None)
    require(row is not None, 'Unknown historical thread: ' + identity)
    return row


def participants(record):
    values = [record.get('correspondent', {}), *record.get('co_correspondents', [])]
    return {email(v.get('email', '')) for v in values if v.get('email')}


def user_owned(record):
    """Record user-decision history; the reviewed per-letter route owns delivery."""
    return bool(record.get('ball') == owner_labels()[0] or record.get('user_decision_ever') or record.get('user_decisions'))


def gate(request, root, resolved=None):
    """Check both the frozen selection and today's boundaries before execution."""
    from .automation import owner_gate, owner_participant_gate
    from .mailbox import address
    snapshot = resolved if resolved is not None else request['inputs']
    contexts = snapshot.get('prior_context', [])
    threads = [snapshot.get('thread', [])]
    if isinstance(contexts, list):
        threads += [c['thread'] for c in contexts if isinstance(c, dict) and 'thread' in c]
        # A new conversation from a known correspondent carries the earlier
        # messages directly; their participants share the same boundaries.
        threads.append([c for c in contexts if isinstance(c, dict) and 'thread' not in c])
    participants = {address(v) for thread in threads for m in thread if isinstance(m, dict) for v in
                    [m.get('from', {}), *m.get('toRecipients', []), *m.get('ccRecipients', [])]} - {''}
    for a in participants:
        check = preflight(a, root=root, new_outreach=False)
        require(not check['refusal_ids'], 'Current conversation has an active table-3 refusal: ' + a)
    owner_participant_gate(participants, root)
    record = request['inputs'].get('thread_record')
    if record is None:
        owner_gate(request, root)
        return
    current = thread_record(root, record['id'])
    for row in (record, current):
        require(row.get('stage', '').lower() not in {'closed', 'declined', 'stopped', 'cancelled'},
                'Historical thread is closed or declined')
        for person in [row.get('correspondent', {}), *row.get('co_correspondents', [])]:
            if not person.get('email'):
                continue
            check = preflight(person['email'], person.get('name', ''), root, new_outreach=False)
            require(not check['refusal_ids'], 'Historical thread blocked by active table-3 refusal: ' + person['email'])
        for a in row.get('mail_match', {}).get('emails', []):
            check = preflight(a, root=root, new_outreach=False)
            require(not check['refusal_ids'], 'Historical thread blocked by active table-3 refusal: ' + a)
    owner_gate(request, root)


def normalize_message_id(value):
    # A local ROWID or Graph object ID cannot stand in for an Internet ID.
    if isinstance(value, str) and '@' in value:
        value = value.strip()
        return value if value.startswith('<') else '<' + value + '>'
    return None


def message_ids(event):
    return [identity for key in ('internetMessageId', 'message_id', 'Message-ID', 'messageId')
            if (identity := normalize_message_id((event or {}).get(key)))]


def inbound_tasks(rows, *, include_finished=False):
    """Index active reply tasks by every covered Internet Message-ID.

    Research tasks are left out: one staged on a thread must not suppress the
    reply to a message that arrives while it runs.
    """
    groups = {}
    for row in rows:
        finished = include_finished and row['state'] in FINISHED_INBOUND_STATES
        if row['kind'] != 'reply' or not (row['state'] in INBOUND_STATES or finished):
            continue
        inputs = row['request']['inputs']
        resolved = row['data'].get('thread_inputs') or {}
        ref = inputs.get('thread_ref')
        ids = ref.get('internetMessageIds') if isinstance(ref, dict) else None
        values = [inputs.get('latest_inbound_message_id'), resolved.get('latest_inbound_message_id')]
        if finished:
            data = row['data']
            packet = data.get('packet_override') or (data.get('result') or {}).get('packet') or {}
            values.append(packet.get('latest_inbound_message_id'))
        if isinstance(ids, list) and not finished:
            values.extend(ids)
        for identity in dict.fromkeys(map(normalize_message_id, values)):
            if identity:
                groups.setdefault(identity, []).append({'task_id': row['id'], 'state': row['state']})
    return groups


def active_inbound_tasks(tasks, *, include_finished=False):
    states = INBOUND_STATES | (FINISHED_INBOUND_STATES if include_finished else set())
    marks = ','.join('?' for _ in states)
    rows = tasks.db.execute("SELECT id FROM tasks WHERE kind='reply' "
                            f"AND state IN ({marks}) ORDER BY rowid", tuple(states))
    return inbound_tasks((tasks.get(row[0]) for row in rows), include_finished=include_finished)


def require_uncovered_inbound(value, tasks):
    ref = value['inputs'].get('thread_ref')
    if value['kind'] != 'reply' or not isinstance(ref, dict) or ref.get('inbound') is not True:
        return
    exact = tasks.db.execute('SELECT request_hash FROM tasks WHERE id=?', (value['id'],)).fetchone()
    if exact and exact[0] == digest(value):
        return
    ids = ref.get('internetMessageIds')
    if not isinstance(ids, list):
        return
    groups = active_inbound_tasks(tasks)
    for identity in map(normalize_message_id, ids):
        covering = next((task for task in groups.get(identity, ())
                         if task['task_id'] != value['id'] and task['state'] in IN_FLIGHT_STATES), None)
        if covering:
            raise ValueError(f"Inbound message {identity} is already covered by task "
                             f"{covering['task_id']} ({covering['state']})")


def build_request(thread_id, kind, instructions, root, revision=None):
    from .pipeline import request
    from .mailbox import REPLY_OWNER_RULE
    require(kind in {'reply', 'research'}, 'Historical tasks support reply or research')
    record = copy.deepcopy(thread_record(root, thread_id))
    persons = people(root)
    addresses = participants(record)
    addresses.update(email(a) for a in record.get('mail_match', {}).get('emails', []))
    primary = email(record.get('correspondent', {}).get('email', ''))
    person = next((p for p in persons if primary in p['emails']), None)
    require(person is not None, 'Register the thread correspondent in table 1 before staging')
    owners = sorted({p['id'] for p in persons if addresses & set(p['emails'])})
    ref = {'thread_id': thread_id, 'internetMessageIds': [],
           'last_in': copy.deepcopy(record.get('last_in')), 'last_out': copy.deepcopy(record.get('last_out'))}
    for key in ('last_in', 'last_out'):
        event = record.get(key) or {}
        known = message_ids(event)
        # Reuse indexed receipts only when they name this exact latest ROWID.
        for p in persons:
            if not addresses & set(p['emails']):
                continue
            for r in p['records']:
                for receipt in r.get('hub', {}).get('historical_inbound', []):
                    if key == 'last_in' and event.get('ROWID') is not None and receipt.get('rowid') == event['ROWID']:
                        known += message_ids(receipt)
        ref['internetMessageIds'].extend(known)
    ref['internetMessageIds'] = list(dict.fromkeys(ref['internetMessageIds']))
    inputs = {'automation_contract': 1, 'thread_record': record, 'thread_ref': ref,
              'owner_ids': owners, 'user_decision_ever': user_owned(record),
              'instructions': ('Read the complete current conversation, promises, requests and recorded next action. '
                               'First decide whether any reply is needed. Deliver a checked useful result; preserve '
                               'user drafts, reply-all, refusals and owner decisions. ' + REPLY_OWNER_RULE +
                               ('\nOperator instructions: ' + instructions if instructions else ''))}
    revision = revision or source_head()
    # Same source revision/selection replays exactly. Any changed bytes get a new ID.
    value = request(kind, {'thread_id': thread_id, 'revision': revision, 'inputs': digest(inputs)}, person['id'], inputs, revision)
    gate(value, root)
    return value


def build_inbound_request(message_id, owner, instructions, root, revision=None):
    """Stage one unmapped Inbox message through the normal thread hydrator."""
    from .pipeline import request
    from .mailbox import REPLY_OWNER_RULE
    require(isinstance(message_id, str) and message_id.strip(), 'stage-inbound needs an Internet Message-ID')
    identities = message_ids({'internetMessageId': message_id})
    require(len(identities) == 1, 'stage-inbound needs one valid Internet Message-ID')
    require(owner is None or (isinstance(owner, str) and owner.strip()), 'Invalid inbound owner')
    if owner is not None:
        require(any(p['id'] == owner for p in people(root)), 'Unknown inbound owner contact')
    provided_owner = owner
    owner = owner or 'inbound-' + digest(identities[0])[:24]
    ref = {'internetMessageIds': identities, 'inbound': True}
    inputs = {'automation_contract': 1, 'thread_ref': ref,
              'owner_ids': [provided_owner] if provided_owner else [],
              'user_decision_ever': False,
              'instructions': ('Read the complete current conversation for this staged Inbox message. '
                               'The operator selected this remaining unmapped message for reply classification. '
                               'Resolve current identities, refusals, owner routing and whether a reply is needed. '
                               + REPLY_OWNER_RULE +
                               ('\nOperator instructions: ' + instructions if instructions else ''))}
    revision = revision or source_head()
    value = request('reply', {'inbound_message_id': identities[0], 'owner': owner,
                              'instructions': instructions},
                    owner, inputs, revision)
    gate(value, root)
    return value


def stage_thread(thread_id, kind, instructions, root, releases, folder):
    from .control import stage
    from .tasks import validate_request
    value = build_request(thread_id, kind, instructions, root)
    validate_request(value)
    return stage(value, 'request', releases, folder)


def stage_inbound(message_id, owner, instructions, root, releases, folder):
    from .control import Ledger, stage
    from .tasks import validate_request
    value = build_inbound_request(message_id, owner, instructions, root)
    validate_request(value)
    with TemporaryDirectory(dir=folder) as snapshot:
        ledger = Ledger(releases, snapshot)
        try:
            require_uncovered_inbound(value, ledger.tasks)
        finally:
            ledger.tasks.close()
    return stage(value, 'request', releases, folder)


def effective_inputs(row):
    inputs = {**row.get('request', {}).get('inputs', {}), **row.get('data', {}).get('thread_inputs', {})}
    history = row.get('data', {}).get('mailbox_history')
    if row.get('kind') == 'firstcontact' and history is not None:
        from .firstcontact_history import recipient_inputs
        if history.get('complete') is True:
            inputs.update(recipient_inputs(row['request'], history))
            inputs['excluded_recipients'] = [*row['request']['inputs'].get('excluded_recipients', []),
                                            *inputs['excluded_recipients']]
        inputs['mailbox_history'] = history
    # No hydrated snapshot or continuation can clear earlier user-decision history.
    if row.get('request', {}).get('inputs', {}).get('user_decision_ever'):
        inputs['user_decision_ever'] = True
    return inputs


def resolve(row, mailbox, ledger, root):
    """Hydrate the reply target and bounded read-only legacy conversations."""
    from .mailbox import _adapter_method, address, instant, preserve_attachments
    inputs = row['request']['inputs']
    if row['kind'] not in {'reply', 'research'} or not inputs.get('thread_ref') or inputs.get('thread'):
        return None
    gate(row['request'], root)
    require(mailbox is not None, 'Configured GraphMailbox unavailable')
    account = mailbox.verify_owner()
    require(mailbox.settings.get('reconciled') is True, 'Mailbox boundary is not reconciled')
    ref = inputs['thread_ref']
    identities = ref.get('internetMessageIds', [])
    require(isinstance(identities, list) and identities, 'No known internetMessageId; reconcile latest ROWIDs in the existing thread record')
    located = {i: mailbox.conversation_for_message(i) for i in identities}
    unresolved = [i for i, c in located.items() if c is None]
    conversations = list(dict.fromkeys(c for c in located.values() if c is not None))
    require(conversations, 'No known incoming internetMessageId resolves to a conversation')
    require(len(conversations) <= MAX_CONVERSATIONS, 'Historical selection exceeds 3 conversations')
    threads = {}
    for c in conversations:
        messages = mailbox.research_thread(c)
        require(messages and all(m.get('conversationId') == c for m in messages), 'Incomplete or mismatched current conversation')
        require(len(messages) <= MAX_MESSAGES, 'Historical conversation exceeds 100 messages')
        require({i for i, found in located.items() if found == c} <= {m.get('internetMessageId') for m in messages},
                'Current conversation omitted a referenced message')
        threads[c] = messages
    all_messages = [m for messages in threads.values() for m in messages]
    if len(threads) == 1:
        require(not any(m.get('isDraft') for m in all_messages), 'A server draft already exists; preserve the user draft')
    incoming = [m for m in all_messages if not m.get('isDraft') and address(m.get('from', {})) != account]
    require(any(located.get(m.get('internetMessageId')) == m['conversationId'] for m in incoming),
            'No known incoming internetMessageId resolves to a conversation')
    latest = max(incoming, key=lambda m: instant(m['receivedDateTime']))
    conversation = latest['conversationId']
    thread = threads[conversation]
    require(not any(m.get('isDraft') for m in thread), 'A server draft already exists; preserve the user draft')
    sent = [m for m in all_messages if not m.get('isDraft') and address(m.get('from', {})) == account]
    inbound = bool(ref.get('inbound'))
    if not inbound:
        require(sent, 'Historical thread needs actual incoming and Sent evidence')
    target_sent = [m for m in sent if m['conversationId'] == conversation]
    latest_sent = max(target_sent, key=lambda m: instant(m['sentDateTime'])) if target_sent else None
    record = inputs.get('thread_record')
    if record is None and not inbound:
        record = thread_record(root, ref['thread_id'])
    if record is not None:
        require(address(latest.get('from', {})) in participants(record), 'Latest sender is outside the selected thread correspondents')
    def addresses(messages):
        return {address(v) for m in messages for v in
                [m.get('from', {}), *m.get('toRecipients', []), *m.get('ccRecipients', [])]} - {account}
    actual = addresses(thread)
    for a in addresses(all_messages):
        check = preflight(a, root=root, new_outreach=False)
        require(not check['refusal_ids'], 'Current conversation has an active table-3 refusal: ' + a)
    actual_owners = {p['id'] for p in people(root) if actual & set(p['emails'])}
    if not inbound:
        require(actual_owners <= {row['request']['owner'], *inputs.get('owner_ids', [])},
                'Current conversation adds registered owners; stage a new thread selection')
    else:
        # An operator-staged Inbox message names one owner as a hint (or none:
        # a deterministic placeholder). Graph reveals the actual participants,
        # e.g. a co-author in cc. Re-run the
        # ordinary owner gate against every registered participant before any
        # CMA claim instead of holding the message.
        from .automation import owner_gate
        for owner_id in actual_owners:
            owner_gate({**row['request'], 'owner': owner_id,
                        'inputs': {**inputs, 'owner_ids': []}}, root)
    link = {'thread_id': conversation, 'latest_inbound_message_id': latest['internetMessageId']}
    fetch = _adapter_method(mailbox, 'attachment_content')
    listings = {}
    preserve_attachments(thread, ledger, link, fetch=fetch, listings=listings)
    current = thread_record(root, ref['thread_id']) if not inbound else None
    resolved = {**link, 'thread': thread, 'owner_ids': sorted(actual_owners),
            'user_decision_ever': bool(inputs.get('user_decision_ever') or user_owned(current or {})),
            'mailbox_observation': {'email': address(latest['from']), 'at': latest['receivedDateTime'],
                                    'message_id': latest['internetMessageId'], 'evidence_ref': 'graph:thread:' + latest['id']}}
    if latest_sent is not None:
        resolved['latest_sent_message_id'] = latest_sent['internetMessageId']
    from .mailbox import mathesis_introduced
    resolved['mathesis_introduced'] = mathesis_introduced(all_messages, account) or False
    if sent:
        evidence = max(sent, key=lambda m: instant(m['sentDateTime']))
        resolved['sent_evidence'] = {'thread_id': evidence['conversationId'], 'message_id': evidence['internetMessageId']}
    resolved['unresolved_message_ids'] = unresolved
    resolved['prior_context'] = []
    for c, messages in threads.items():
        if c == conversation:
            continue
        context = {'thread_id': c, 'read_only': True, 'thread': messages}
        preserve_attachments(messages, ledger, {'thread_id': c}, fetch=fetch, listings=listings)
        resolved['prior_context'].append(context)
    return resolved
