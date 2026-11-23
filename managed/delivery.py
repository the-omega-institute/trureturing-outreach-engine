"""Bridge exact signed task reviews to the existing transactional mail queue."""
import copy
from dataclasses import asdict
from datetime import datetime, timezone
import inspect
import json
from urllib.error import HTTPError

from .draft_status import owner_edit, saved_at
from .mail_domains import home_domains, home_recipient_override
from .tick import allowed, units, unit
from .queue import Authority, GateError, Queue, digest, require
from .registry import preflight, register_mail_thread_contacts, render
from .tasks import matches_workflow, review_current
from . import recheck


SEND_GUARD_REASON = 'Unsent owner draft to the same recipient exists; consolidate before sending'
OWNER_SENT_RECHECK_SECONDS = 30 * 60
# An unsaved owner draft can fail the same mailbox/preflight gates on every
# tick. Keep those failures out of the hot path until the reviewed binding or
# failure reason changes.
OWNER_DRAFT_RECHECK_SECONDS = recheck.RECHECK_SECONDS
# Allow two hours for Graph search indexing lag before confirming that an
# uncertain owner-draft create left neither a Drafts nor a Sent Items copy.
DRAFT_INTENT_SETTLE_SECONDS = 2 * 60 * 60


def _check_expired_draft_intents(mailbox, row):
    """Keep delayed copies visible even after a replacement has a receipt."""
    for intent in row['data'].get('draft_intent_expired', []):
        mailbox.attempt_started = intent['at']
        draft = mailbox.find('drafts', intent['key'])
        # Correlation is enough to flag a conflict, including an owner-edited
        # copy. It never authorizes deleting or overwriting either draft.
        require(draft is None, 'Expired draft attempt reappeared; reconcile manually: ' +
                str((draft or {}).get('id', intent['key'])))


def _packet_addresses(packet):
    return {str(value).strip().casefold() for value in
            packet.get('to', []) + packet.get('cc', []) if str(value).strip()}


def _message_address(value):
    if isinstance(value, str):
        return value.strip().casefold()
    if isinstance(value, dict):
        return str(value.get('emailAddress', {}).get('address') or value.get('address') or '').strip().casefold()
    return ''


def _attempt_key(message):
    direct = message.get('attempt_key') if isinstance(message, dict) else None
    if isinstance(direct, str) and direct:
        return direct
    headers = message.get('internetMessageHeaders', []) if isinstance(message, dict) else []
    for header in headers:
        if str(header.get('name', '')).casefold() == 'x-outreach-attempt':
            value = header.get('value')
            if isinstance(value, str) and value:
                return value
    return None


def _draft_rows(mailbox, recipient):
    finder = getattr(mailbox, 'drafts_for', None)
    if callable(finder):
        rows = finder(recipient)
    else:
        finder = getattr(mailbox, 'recipient_history', None)
        rows = ([message for message in finder(recipient)
                 if message.get('folder') in {None, 'drafts'} or message.get('isDraft') is True]
                if callable(finder) else [])
    return [row for row in (rows or [])
            if row.get('isDraft') is True or str(row.get('folder', '')).casefold() == 'drafts']


def _own_draft_refs(row, queue):
    """Task/attempt identities shared by the read-only draft conflict checks."""
    own_keys = set()
    attempt = queue.db.execute('SELECT key FROM attempts WHERE item=?', (row['id'],)).fetchone()
    if attempt:
        own_keys.add(attempt['key'])
    own_ids = set()
    receipt = row['data'].get('draft_receipt') or {}
    if isinstance(receipt, dict) and receipt.get('remote_id'):
        own_ids.add(receipt['remote_id'])
    events = getattr(queue, 'events', None)
    if callable(events):
        for event in events(row['id']):
            if event.get('action') != 'send-call':
                continue
            try:
                draft_id = json.loads(event.get('data', '{}')).get('draft_id')
            except (TypeError, ValueError):
                draft_id = None
            if draft_id:
                own_ids.add(draft_id)
    return own_ids, own_keys


def _delivery_terminal(exc, row, mailbox, packet, queue):
    """Recognize known blockers before sending or saving an unattempted draft."""
    if not isinstance(exc, GateError):
        return False
    if row['state'] == 'owner-draft':
        data = row['data']
        if (data.get('draft_receipt') or data.get('draft_intent')
                or data.get('draft_revision_intent') or data.get('stale_draft_conflict')
                or queue.db.execute('SELECT 1 FROM attempts WHERE item=?', (row['id'],)).fetchone()):
            return False
    elif row['state'] != 'awaiting-mailbox':
        return False
    blocker = str(exc)
    if blocker in {
            'A newer incoming message requires a new reply/review',
            'Reply-all recipients differ from the latest incoming message',
            'Actual Sent changed or its source identity was not captured'}:
        return True
    if blocker.startswith('Current conversation has an active table-3 refusal: '):
        return True
    if blocker.startswith('Mathesis was already introduced to this correspondent'):
        return True
    if (blocker.startswith('A newer incoming message from ')
            and blocker.endswith(' in another conversation requires a new reply/review')):
        return True
    if blocker == 'A server draft already exists; preserve the user draft':
        # Correlated copies remain on their existing recovery path. If this
        # read fails or ownership is uncertain, retain the original retry.
        try:
            own_ids, own_keys = _own_draft_refs(row, queue)
            return any(m.get('isDraft') and m.get('id')
                       and m['id'] not in own_ids and _attempt_key(m) not in own_keys
                       for m in mailbox.thread(packet['thread_id']))
        except Exception:
            return False
    return False


def _send_conflicts(tasks, mailbox, row, packet, queue):
    """Return recipient conflicts without retaining message bodies."""
    addresses = _packet_addresses(packet)
    # Until reconciled, owner-draft tasks can outlive a sent/deleted draft, so
    # a saved one conflicts only while still in Drafts (checked below); one
    # not yet saved to the mailbox always conflicts.
    candidates = []
    for candidate in tasks.all():
        if candidate['id'] == row['id'] or candidate['state'] != 'owner-draft':
            continue
        candidate_packet = candidate['data'].get('packet_override') or candidate['data'].get('result', {}).get('packet')
        if isinstance(candidate_packet, dict) and addresses & _packet_addresses(candidate_packet):
            candidates.append(candidate)

    own_ids, own_keys = _own_draft_refs(row, queue)
    draft_ids = []
    seen = set()
    for recipient in sorted(addresses):
        for draft in _draft_rows(mailbox, recipient):
            draft_id = draft.get('id')
            if not draft_id or draft_id in seen or draft_id in own_ids or _attempt_key(draft) in own_keys:
                continue
            participants = {_message_address(value) for value in
                            [draft.get('from', {}), *draft.get('toRecipients', []),
                             *draft.get('ccRecipients', [])]}
            if recipient not in participants:
                continue
            seen.add(draft_id)
            draft_ids.append(draft_id)
    task_ids = []
    for candidate in candidates:
        saved = (candidate['data'].get('draft_receipt') or {}).get('remote_id')
        if not saved or saved in seen:
            task_ids.append(candidate['id'])
    return {'task_ids': sorted(set(task_ids)), 'draft_ids': sorted(draft_ids)}


class Channel:
    name='microsoft-graph'

    def __init__(self, mailbox, kind):self.mailbox=mailbox;self.kind=kind
    def submit(self, packet, key):return self.mailbox.submit(packet,key)
    def lookup(self, packet, key):return self.mailbox.lookup(packet,key)
    @property
    def tracks_send_call(self):return callable(getattr(self.mailbox,'submit_tracked',None))
    def submit_tracked(self, packet, key, before_send):
        return self.mailbox.submit_tracked(packet,key,before_send)
    def legacy_not_sent(self, packet, key):
        check=getattr(self.mailbox,'legacy_not_sent',None)
        return check(packet,key) if callable(check) else None
    def discard_draft(self, packet, key):return self.mailbox.discard_draft(packet,key)


def _read_remote_draft(mailbox, remote_id):
    """Read one draft, treating an observed 404 as a confirmed absence."""
    require(isinstance(remote_id, str) and remote_id.strip(), 'Missing draft remote ID')
    try:
        return mailbox.message(remote_id)
    except HTTPError as exc:
        if exc.code == 404:
            return None
        raise
    except KeyError:
        # Small offline Graph fixtures model a deleted immutable ID as a key miss.
        return None


def _read_live_draft(mailbox, remote_id, after_delete=False):
    """A deleted Exchange draft stays readable by id (Recoverable Items) and may
    answer 403; only a message still in Drafts counts as present."""
    try:
        draft = _read_remote_draft(mailbox, remote_id)
    except HTTPError as exc:
        if after_delete and exc.code == 403:
            return None
        raise
    folder = getattr(mailbox, 'drafts_folder_id', None)
    if draft is not None and callable(folder) and draft.get('parentFolderId') \
            and draft['parentFolderId'] != folder():
        return None
    return draft


def _owner_draft_since(row):
    """Use the saved draft's time, retaining a stable bound across later ticks."""
    from .mailbox import instant
    from .thread_staging import effective_inputs
    data = row['data']; receipt = data['draft_receipt']
    if saved := saved_at(data, receipt):
        return saved
    at = (effective_inputs(row).get('mailbox_observation') or {}).get('at')
    if at is None:
        return None  # Never correlate unbounded historical Sent by subject alone.
    return (datetime.fromtimestamp(at, timezone.utc) if isinstance(at, (int, float))
            else instant(at))


def _owner_draft_recheck_binding(row):
    """Bind an unsaved owner-draft retry to the exact work that failed.

    The delivery error itself is part of the binding so a changed reason is
    retried immediately. The task revision is captured after recording that
    error; a later content, review, incoming, or revision update changes the
    binding and therefore bypasses the delay.
    """
    data = row['data']
    return {
        'task_revision': row['revision'],
        'result': data.get('result'),
        'packet': data.get('packet_override') or (data.get('result') or {}).get('packet'),
        'review': data.get('review'),
        'request_inputs': row['request'].get('inputs'),
        'thread_inputs': data.get('thread_inputs'),
        'mailbox_history': data.get('mailbox_history'),
        'draft_revision_requests': data.get('draft_revision_requests'),
        'draft_revision_intent': data.get('draft_revision_intent'),
        'reason': data.get('last_error') or data.get('reason'),
    }


def _defer_unsaved_owner_draft_failure(tasks, row):
    """Back off a failed group-0 row while it remains an unsaved draft."""
    if (row['state'] == 'owner-draft' and not row['data'].get('draft_receipt')
            and not row['data'].get('sent_receipt')):
        recheck.defer(tasks, 'owner-draft', row['id'],
                      _owner_draft_recheck_binding(row), OWNER_DRAFT_RECHECK_SECONDS)


def _reconcile_owner_sent(ledger, config, mailbox, row):
    """Observe an Outlook send without writing to Graph. Return true if gone.

    A missing draft stops all further delivery/revision work for this task, even
    when Sent is absent or ambiguous. Sent Items uses the adapter's existing
    folder cache; it is never inferred from an all-folder thread listing.
    """
    from .mailbox import _header_message_ids, instant
    receipt = row['data'].get('draft_receipt') or {}
    if not receipt.get('remote_id') or not callable(getattr(mailbox, 'message', None)) \
            or not callable(getattr(mailbox, 'sent_items', None)):
        return False
    since = _owner_draft_since(row)
    if since is None:
        return False
    absence = None
    try:
        draft = _read_remote_draft(mailbox, receipt['remote_id'])
    except HTTPError as exc:
        if exc.code != 403:
            raise
        draft = None  # A forbidden read alone cannot establish discard.
    else:
        if draft is None:
            absence = {'folder': 'missing', 'http_status': 404, 'lastModifiedDateTime': None}
    if draft is not None:
        parent = draft.get('parentFolderId')
        drafts_folder = getattr(mailbox, 'drafts_folder_id', None)
        if (draft.get('isDraft') is not False and
                (not parent or not callable(drafts_folder) or parent == drafts_folder())):
            return False
        deleted_folder = getattr(mailbox, 'deleted_items_folder_id', None)
        if parent and callable(deleted_folder) and parent == deleted_folder():
            absence = {'folder': 'deleteditems', 'parentFolderId': parent,
                       'lastModifiedDateTime': draft.get('lastModifiedDateTime')}
    data = dict(row['data'])
    packet = data.get('packet_override') or data.get('result', {}).get('packet')
    if not isinstance(packet, dict):
        return True
    conversation = receipt.get('conversationId') or packet.get('thread_id')
    addresses = _packet_addresses(packet)
    team = {str(a).strip().casefold() for a in config.get('mailbox', {}).get('team_addresses', [])}
    ignored = team & {str(a).strip().casefold() for a in packet.get('cc', [])}
    expected = addresses - ignored
    # One Sent message is evidence for at most one task. A subject-only match
    # also yields to a task whose own conversation contains the message: a
    # superseded draft with the same subject was deleted, not sent.
    claimed, owned = set(), set()
    for other in ledger.tasks.all():
        if other['id'] == row['id']:
            continue
        other_receipt = other['data'].get('sent_receipt') or {}
        if other_receipt.get('remote_id'):
            claimed.add(other_receipt['remote_id'])
        other_conversation = (other['data'].get('draft_receipt') or {}).get('conversationId')
        if other_conversation:
            owned.add(other_conversation)
        other_packet = other['data'].get('packet_override') or (other['data'].get('result') or {}).get('packet')
        if isinstance(other_packet, dict) and other_packet.get('thread_id'):
            owned.add(other_packet['thread_id'])
    matches, weak = {}, {}
    try:
        sent_messages = mailbox.sent_items(since)
    except TypeError:  # Adapters without a bounded window.
        sent_messages = mailbox.sent_items()
    for message in sent_messages:
        if (message.get('isDraft') or not message.get('id') or not message.get('internetMessageId')
                or not isinstance(message.get('sentDateTime'), str)):
            continue
        actual = {_message_address(a) for a in
                  message.get('toRecipients', []) + message.get('ccRecipients', [])}
        if not expected or actual - ignored != expected:
            continue
        try:
            if instant(message.get('sentDateTime', '')) <= since:
                continue
        except (ValueError, TypeError):
            continue
        if message['id'] in claimed:
            continue
        if ((conversation and message.get('conversationId') == conversation)
                or (packet.get('thread_id') and message.get('conversationId') == packet['thread_id'])
                or (packet.get('latest_inbound_message_id') and
                    packet['latest_inbound_message_id'] in _header_message_ids(message))):
            matches[message['id']] = message
        elif (message.get('subject') == packet.get('subject') and
              message.get('conversationId') not in owned):
            weak[message['id']] = message
    matches = matches or weak
    data.pop('owner_draft_note', None)
    if len(matches) == 1:
        message = next(iter(matches.values()))
        packet_hash = digest(packet)
        data['sent_receipt'] = {
            'idempotency_key': receipt.get('attempt_key') or digest(['owner-outlook', row['id'], receipt['remote_id']]),
            'content_hash': packet_hash, 'remote_id': message['id'],
            'evidence_ref': 'graph:sent:' + message['internetMessageId'],
            'at': message['sentDateTime'], 'sent_by': 'owner-outlook'}
        if (not data.get('owner_edit') and conversation and message.get('conversationId') == conversation
                and saved_at(data, receipt) and receipt.get('content_hash') == packet_hash):
            if edit := owner_edit(packet, message):
                data['owner_edit'] = edit
        data.pop('owner_draft_status', None)
        data.pop('last_error', None)
        state = 'verified'
    elif not matches and absence is not None:
        intent = data.get('draft_revision_intent') or {}
        if (intent.get('phase') == 'delete-intent' and intent.get('new_remote_id')
                and intent.get('old_remote_id') == receipt['remote_id']):
            return False  # Resume the coordinator's own checkpointed draft replacement.
        data['owner_draft_status'] = 'discarded'
        data['owner_draft_discard'] = {'remote_id': receipt['remote_id'],
                                       'observed_at': ledger.tasks.clock(), **absence}
        data.pop('last_error', None)
        state = 'owner-discarded'
    else:
        state = row['state']
        data['owner_draft_status'] = 'sent-ambiguous' if matches else 'draft-missing-not-sent'
        if matches:
            data['owner_draft_note'] = f'{len(matches)} Sent Items messages match the missing owner draft; reconcile manually'
    if data != row['data'] or state != row['state']:
        ledger.tasks.update(row['id'], row['revision'], state, data)
        ledger.save()
    return True


def _header_key(draft):
    values=[h.get('value') for h in draft.get('internetMessageHeaders', [])
            if str(h.get('name', '')).casefold() == 'x-outreach-attempt']
    return values[0] if len(values) == 1 and isinstance(values[0], str) and values[0] else None


def _verify_owned_draft(mailbox, draft, packet, receipt, check_thread=True):
    """Verify ownership and exact bytes immediately before a replacement/delete."""
    require(isinstance(draft, dict) and draft.get('isDraft') is True,
            'Owner edited the draft; revision not applied')
    require(callable(getattr(mailbox, 'verify_owner', None)),
            'Mailbox adapter cannot verify the owner; revision not applied')
    require(str(mailbox.verify_owner()).casefold() == packet['sender'].casefold(),
            'Owner edited the draft; revision not applied')
    require(callable(getattr(mailbox, 'matches', None)) and mailbox.matches(draft, packet),
            'Owner edited the draft; revision not applied')
    # A saved draft's threading was verified when it was saved, and Exchange
    # may file it under another conversation id; only a new replacement must
    # still prove it threads to the reviewed message.
    if packet['kind'] == 'reply' and check_thread:
        from .mailbox import replies_in_thread
        require(replies_in_thread(draft, packet), 'Owner edited the draft; revision not applied')
    key=(receipt or {}).get('attempt_key') or _header_key(draft)
    if callable(getattr(mailbox, 'own_draft', None)):
        require(key, 'Owner draft attempt identity is missing; revision not applied')
        require(mailbox.own_draft(draft, packet, key) is not False,
                'Owner edited the draft; revision not applied')
    return key


def _find_attempt_draft(mailbox, key, at):
    finder=getattr(mailbox, 'find', None)
    if callable(finder):
        if hasattr(mailbox, 'attempt_started'):
            mailbox.attempt_started=at
        found=finder('drafts', key)
        if found is not None:
            return found
        # ``find`` intentionally uses a five-minute bounded scan for ordinary
        # sends. A revision can be resumed much later, so widen only this exact
        # immutable attempt-key lookup before deciding to create again.
        messages=getattr(mailbox, 'messages', None)
        if callable(messages):
            rows=messages('drafts')
            matches=[row for row in rows if _header_key(row) == key]
            require(len(matches) <= 1, 'Multiple replacement drafts share an attempt; reconcile manually')
            return matches[0] if matches else None
        return None
    # Keep recovery safe for small adapters that predate ``find``: inspect the
    # bounded Drafts listing and correlate the exact attempt header before a
    # second create call.
    messages=getattr(mailbox, 'messages', None)
    if callable(messages):
        rows=messages('drafts')
        matches=[row for row in rows if _header_key(row) == key]
        require(len(matches) <= 1, 'Multiple replacement drafts share an attempt; reconcile manually')
        return matches[0] if matches else None
    drafts=getattr(mailbox, 'drafts', None)
    if isinstance(drafts, dict):
        matches=[row for row in drafts.values()
                 if _header_key(row) == key or row.get('key') == key]
        require(len(matches) <= 1, 'Multiple replacement drafts share an attempt; reconcile manually')
        return copy.deepcopy(matches[0]) if matches else None
    return None


def _create_replacement(mailbox, packet, key, old_remote_id):
    creator=getattr(mailbox, 'create_draft', None)
    require(callable(creator), 'Mailbox adapter cannot create a replacement draft')
    try:
        signature=inspect.signature(creator)
        accepts_replacing=('replacing' in signature.parameters or
                           any(p.kind == inspect.Parameter.VAR_KEYWORD
                               for p in signature.parameters.values()))
    except (TypeError, ValueError):
        accepts_replacing=True
    return creator(packet, key, replacing=old_remote_id) if accepts_replacing else creator(packet, key)


def _delete_remote_draft(mailbox, remote_id):
    deleter=getattr(mailbox, 'delete_draft', None)
    if callable(deleter):
        return deleter(remote_id)
    graph=getattr(mailbox, 'graph', None)
    require(graph is not None and callable(getattr(graph, 'request', None)),
            'Mailbox adapter cannot delete the original draft')
    from urllib.parse import quote
    return graph.request('/me/messages/'+quote(remote_id, safe=''), 'DELETE')


def _revision_recipient_preflight(packet, root):
    for recipient in packet['to']+packet['cc']:
        check=preflight(recipient, root=root)
        require(check['registered'], 'Mail recipient is not registered in table 1')
        require(not check['refusal_ids'], 'Recipient has an active table-3 boundary')


def _register_reply_evidence(policy, root):
    if register_mail_thread_contacts(policy.get('mail_thread_contacts', []), root=root):
        render(root)


def _revision_preflight(mailbox, row, packet, config, root, old_remote_id):
    """Repeat the exact registry and mailbox checks used by the first save."""
    from .thread_staging import effective_inputs
    request={**row['request'], 'inputs': effective_inputs(row)}
    try:
        policy=mailbox.preflight(packet, request, allow_draft_id=old_remote_id)
    except TypeError as exc:
        # Test transports and older adapters expose the original two-argument
        # boundary. Their normal preflight still supplies the registry-safe
        # checks; GraphMailbox implements the allow-old-draft refinement.
        if 'allow_draft_id' not in str(exc) and 'unexpected keyword' not in str(exc):
            raise
        policy=mailbox.preflight(packet, request)
    _register_reply_evidence(policy, root)
    _revision_recipient_preflight(packet, root)
    return policy


def _verify_stale_owner_draft(mailbox, draft, old, packet):
    from .mailbox import instant
    require(draft.get('conversationId') == packet['thread_id'], 'Stale draft conversation changed')
    data=old['data'];receipt=data['draft_receipt']
    previous=data.get('packet_override') or (data.get('result') or {}).get('packet')
    require(isinstance(previous, dict), 'Stale draft has no saved packet to compare')
    created, modified=draft.get('createdDateTime'), draft.get('lastModifiedDateTime')
    if created and modified:
        require(instant(created) == instant(modified), 'Stale draft was modified after creation')
    else:
        require(receipt.get('content_hash') == digest(previous), 'Stale draft has no unchanged-content evidence')
    _verify_owned_draft(mailbox, draft, previous, receipt, check_thread=False)
    return {'content_hash':digest(previous), 'created_at':created, 'modified_at':modified}


def _retire_stale_owner_draft(ledger, mailbox, row, packet, config, root):
    """Move only a receipt-owned, unchanged older reply to Deleted Items.

    Checkpoint the move intent separately from the new draft's create intent:
    a lost move response must never make a fresh draft create look uncertain.
    """
    from .tasks import newer_reply
    if packet['kind'] != 'reply' or not callable(getattr(mailbox, 'thread', None)):return row
    rows=mailbox.thread(packet['thread_id'])
    if not isinstance(rows, list):return row
    drafts=[m for m in rows if m.get('isDraft')]
    pending=row['data'].get('stale_draft_replacement') or {}
    old=None;draft=None
    if pending.get('phase') == 'move-intent':
        old=ledger.tasks.get(pending['task_id'])
        draft=_read_live_draft(mailbox, pending['remote_id'])
    else:
        pending={}
        if len(drafts) != 1:return row
        candidates=[r for r in ledger.tasks.all() if r['id'] != row['id']
                    and r['state'] in {'owner-draft','revising','superseded'}
                    and not r['data'].get('sent_receipt')
                    and (r['data'].get('draft_receipt') or {}).get('remote_id') == drafts[0].get('id')
                    and newer_reply(row, r)]
        if len(candidates) != 1:return row
        old=candidates[0]
        draft=_read_live_draft(mailbox, drafts[0]['id'])
    require(newer_reply(row, old) and not old['data'].get('sent_receipt'),
            'Stale draft no longer belongs to an older unsent reply')
    evidence=pending or {'task_id':old['id'], 'remote_id':old['data']['draft_receipt']['remote_id'],
                         'destination':'deleteditems', 'at':ledger.tasks.clock(), 'phase':'move-intent'}
    if draft is not None:
        def verify():
            try:
                return _verify_stale_owner_draft(mailbox, draft, old, packet)
            except Exception:
                current=ledger.tasks.get(row['id'])
                ledger.tasks.update(row['id'], current['revision'], current['state'],
                                    {**current['data'], 'stale_draft_conflict':{
                                        'task_ids':[old['id'], row['id']], 'remote_id':evidence['remote_id']}})
                ledger.save()
                require(False, 'Stale user draft must be merged with '+row['id']+'; preserve '+old['id'])
        evidence={**evidence, 'unchanged':verify()}
        _revision_preflight(mailbox, row, packet, config, root, evidence['remote_id'])
        mover=getattr(mailbox, 'move_draft_to_deleted', None)
        require(callable(mover), 'Mailbox adapter cannot move the stale draft to Deleted Items')
        row=ledger.tasks.update(row['id'], row['revision'], row['state'],
                                {**row['data'], 'stale_draft_replacement':evidence})
        ledger.save(force=True)
        # Re-read after preflight/checkpoint to protect edits during those reads.
        draft=_read_live_draft(mailbox, evidence['remote_id'])
        if draft is not None:
            verify()
            moved=mover(evidence['remote_id'])
            evidence={**evidence, 'moved_remote_id':moved['id']}
    require(_read_live_draft(mailbox, evidence['remote_id'], after_delete=True) is None,
            'Stale draft move is not yet confirmed')
    evidence={**evidence, 'phase':'moved', 'verified_absent':True}
    old=ledger.tasks.get(old['id'])
    ledger.tasks.update(old['id'], old['revision'], 'superseded',
                        {**old['data'], 'superseded_by':row['id'], 'stale_draft_replacement':evidence})
    row=ledger.tasks.get(row['id'])
    row=ledger.tasks.update(row['id'], row['revision'], row['state'],
                            {**row['data'], 'stale_draft_replacement':evidence})
    ledger.save(force=True)
    return row


def _revision_error(ledger, row, message):
    current=ledger.tasks.get(row['id'])
    data={**current['data'], 'last_error':message[:300]}
    ledger.tasks.update(current['id'], current['revision'], current['state'], data)
    ledger.save()


def _advance_draft_revision(ledger, config, root, mailbox, row):
    """Apply at most one verified staged revision and return the current row."""
    data=dict(row['data'])
    pending=list(data.get('draft_revision_requests', []))
    intent=data.get('draft_revision_intent')
    if intent is None and not pending:
        return row
    if config.get('paused') or not config.get('kinds', {}).get(row['kind']):
        return row
    from .note_closure import source_gate
    source_gate(row['request'], ledger.tasks, data.get('result'))
    current_packet=data.get('packet_override') or data.get('result', {}).get('packet')
    receipt=data.get('draft_receipt') or {}
    if not isinstance(current_packet, dict) or not isinstance(receipt, dict):
        return row
    event=intent or pending[0]
    old_hash=event.get('old_packet_hash');new_hash=event.get('new_packet_hash')
    new_packet=event.get('packet')
    if (receipt.get('content_hash') != old_hash or digest(current_packet) != old_hash
            or not isinstance(new_packet, dict) or digest(new_packet) != new_hash):
        _revision_error(ledger, row, 'Draft revision targets an obsolete draft receipt')
        return ledger.tasks.get(row['id'])
    old_remote_id=event.get('old_remote_id') or receipt.get('remote_id')
    if not old_remote_id:
        _revision_error(ledger, row, 'Owner draft receipt has no remote ID; revision not applied')
        return ledger.tasks.get(row['id'])

    # The first observation always protects the user's current draft. A
    # revision never turns an edited or sent owner draft into a replacement.
    if intent is None:
        old=_read_remote_draft(mailbox, old_remote_id)
        try:
            old_key=_verify_owned_draft(mailbox, old, current_packet, receipt, check_thread=False)
        except Exception:
            _revision_error(ledger, row, 'Owner edited the draft; revision not applied')
            return ledger.tasks.get(row['id'])
        policy=_revision_preflight(mailbox, row, new_packet, config, root, old_remote_id)
        event_id=event.get('event_id') or digest({k:v for k,v in event.items() if k != 'event_id'})
        intent={**event, 'event_id':event_id, 'old_remote_id':old_remote_id,
                'old_attempt_key':old_key, 'new_key':digest(['owner-draft-revision',row['id'],event_id]),
                'at':ledger.tasks.clock(), 'phase':'intent'}
        if policy.get('recipient_exception'):
            intent['recipient_exception']=policy['recipient_exception']
        current=ledger.tasks.get(row['id'])
        ledger.tasks.update(row['id'], current['revision'], current['state'],
                            {**current['data'], 'draft_revision_intent':intent})
        ledger.save(force=True)
        row=ledger.tasks.get(row['id']);data=dict(row['data'])
    else:
        old_remote_id=intent['old_remote_id']

    new_key=intent['new_key'];at=intent['at']
    new_remote_id=intent.get('new_remote_id')
    observed=_read_remote_draft(mailbox, new_remote_id) if new_remote_id else None
    known_new=bool(new_remote_id)
    if observed is None and not known_new:
        observed=_find_attempt_draft(mailbox, new_key, at)
        if observed is not None:new_remote_id=observed.get('id')
    if observed is None and known_new:
        raise ValueError('Replacement draft is not yet readable; retain revision intent')
    if observed is None:
        # Re-run freshness and table-3 checks after an interrupted preflight,
        # immediately before the first replacement write.
        old=_read_remote_draft(mailbox, old_remote_id)
        try:
            _verify_owned_draft(mailbox, old, current_packet,
                                {'attempt_key':intent.get('old_attempt_key') or receipt.get('attempt_key'),
                                 'remote_id':old_remote_id}, check_thread=False)
        except Exception:
            _revision_error(ledger, row, 'Owner edited the draft; revision not applied')
            return ledger.tasks.get(row['id'])
        _revision_preflight(mailbox, row, new_packet, config, root, old_remote_id)
        observed=_create_replacement(mailbox, new_packet, new_key, old_remote_id)
        new_remote_id=observed.get('id') if isinstance(observed, dict) else None
        require(new_remote_id, 'Replacement draft has no remote ID')
        # create_draft already reads back exact bytes; retain a durable phase
        # before any operation on the original draft.
        current=ledger.tasks.get(row['id'])
        intent={**intent, 'new_remote_id':new_remote_id, 'phase':'created'}
        ledger.tasks.update(row['id'], current['revision'], current['state'],
                            {**current['data'], 'draft_revision_intent':intent})
        ledger.save(force=True)
    else:
        new_remote_id=observed.get('id', new_remote_id)

    # Always re-read the replacement before deleting anything. A replacement
    # that is missing, edited or in a different conversation leaves the old
    # owner draft intact and the intent available for inspection/recovery.
    observed=_read_remote_draft(mailbox, new_remote_id)
    try:
        _verify_owned_draft(mailbox, observed, new_packet,
                            {'attempt_key':new_key, 'remote_id':new_remote_id})
    except Exception as exc:
        _revision_error(ledger, row, str(exc) or 'Replacement draft verification failed')
        return ledger.tasks.get(row['id'])
    # A recovered replacement can predate today's contact/refusal tables.
    # Recheck immediately before the deletion checkpoint on every attempt.
    _revision_recipient_preflight(new_packet, root)
    current=ledger.tasks.get(row['id']);data=dict(current['data'])
    intent={**intent, 'new_remote_id':new_remote_id, 'phase':'delete-intent'}
    ledger.tasks.update(row['id'], current['revision'], current['state'],
                        {**data, 'draft_revision_intent':intent})
    ledger.save(force=True)

    # The second old-draft read is the deletion gate. This is deliberately
    # repeated after every crash/restart so an intervening owner edit or send
    # can never be deleted by a stale intent.
    old=_read_live_draft(mailbox, old_remote_id)
    if old is not None:
        try:
            _verify_owned_draft(mailbox, old, current_packet,
                                {'attempt_key':intent.get('old_attempt_key') or receipt.get('attempt_key'),
                                 'remote_id':old_remote_id}, check_thread=False)
        except Exception:
            _revision_error(ledger, row, 'Owner edited the draft; revision not applied')
            return ledger.tasks.get(row['id'])
        _delete_remote_draft(mailbox, old_remote_id)
    # Confirm deletion (including a lost DELETE response) before publishing
    # the new receipt into the owner-facing task view.
    old_after=_read_live_draft(mailbox, old_remote_id, after_delete=True)
    require(old_after is None, 'Old draft deletion is not yet confirmed')
    new_receipt={'remote_id':new_remote_id, 'attempt_key':new_key,
                 'evidence_ref':'graph:draft:'+new_remote_id, 'content_hash':new_hash,
                 'saved_at':observed.get('createdDateTime'), 'conversationId':observed.get('conversationId')}
    evidence={'remote_id':old_remote_id, 'verified_absent':True,
              'evidence_ref':'graph:draft-deleted:'+old_remote_id,
              'at':ledger.tasks.clock()}
    record={'reason':event['reason'], 'old_packet_hash':old_hash,
            'new_packet_hash':new_hash, 'new_draft_receipt':new_receipt,
            'old_draft_deleted':evidence}
    if intent.get('recipient_exception'):
        record['recipient_exception']=intent['recipient_exception']
    pending=[item for item in pending
             if (item.get('event_id') or digest({k:v for k,v in item.items() if k != 'event_id'}))
             != intent.get('event_id')]
    current=ledger.tasks.get(row['id'])
    final={**current['data'], 'draft_receipt':new_receipt,
           'packet_override':new_packet, 'draft_revisions':[*(current['data'].get('draft_revisions', [])), record],
           'draft_revision_requests':pending}
    final.pop('draft_revision_intent', None);final.pop('last_error', None)
    final.pop('recipient_exception', None)
    if intent.get('recipient_exception'):
        final['recipient_exception']=intent['recipient_exception']
    ledger.tasks.update(row['id'], current['revision'], 'owner-draft', final)
    ledger.save()
    return ledger.tasks.get(row['id'])


def advance(ledger, config, root, mailbox=None):
    if mailbox is None:return
    from .control import workflow
    from .pipeline import source_gate
    from . import note_closure
    from .automation import approved_route, parent_gate, paper_gate, owner_gate
    tasks=ledger.tasks
    suffixes=home_domains(config)
    queue=Queue(tasks.path,{k:workflow(k) for k in ('firstcontact','reply')},
                # Queue checkpoints are durability barriers: send-intent,
                # send-call and the proof/cleanup of a definitely unsent draft.
                reviewer='local-claude-loop',checkpoint=lambda:ledger.save(force=True))
    try:
        rows=[r for r in tasks.all() if r['kind'] in {'firstcontact','reply'} and r['state'] in {'awaiting-mailbox','owner-draft'}]
        # Send reviewed mail before reconciling saved owner drafts: one
        # owner-sent check took 86 s on 2026-11-08, and 33 of them ahead of 11
        # reviewed first contacts used the whole delivery slice every tick.
        # Keep deferred-first rotation within each group (units() would
        # otherwise re-sort by carried-over priority alone).
        tick=getattr(ledger,'tick',None)
        previous={k:i for i,k in enumerate(tick.previous.get('delivery',[]))} if tick else {}
        def group(r):
            # 0: owner drafts still to save (send-conflict checks need them),
            # 1: reviewed mail to send, 2: slow owner-sent reconciliation.
            if r['state']=='awaiting-mailbox':return 1
            return 2 if r['data'].get('draft_receipt') else 0
        rows.sort(key=lambda r:(group(r),previous.get(r['id'],len(previous))))
        # Recheck before admission to units(): a deferred row must not consume
        # a delivery budget unit merely to be skipped in the loop body.
        rows=[r for r in rows
              if group(r) != 0 or recheck.due(tasks, 'owner-draft', r['id'],
                                              _owner_draft_recheck_binding(r))]
        for original in units(ledger,'delivery',rows,timed=False,preserve_order=True):
            row=tasks.get(original['id']);data=dict(row['data'])
            if row['state']=='owner-draft' and data.get('draft_receipt') and not config['paused'] \
                    and config['kinds'].get(row['kind']):
                binding=[data['draft_receipt'],data.get('packet_override') or data.get('result',{}).get('packet'),
                         data.get('draft_revision_requests'),data.get('draft_revision_intent')]
                # This cache only delays observation; it never authorizes a
                # mailbox write. A missing/ambiguous draft must still stop all
                # revision work. Changed draft/packet/revision intent rechecks.
                revising=bool(data.get('draft_revision_requests') or data.get('draft_revision_intent'))
                missing=data.get('owner_draft_status') in {'draft-missing-not-sent','sent-ambiguous'}
                if not recheck.due(tasks,'owner-sent',row['id'],binding) and (not revising or missing):continue
                try:
                    with unit(ledger,'delivery',row['id'],'owner-sent') as ready, \
                            recheck.attempt(ledger,'owner-sent',row['id'],binding,timeouts=False):
                        if not ready:continue
                        _check_expired_draft_intents(mailbox, row)
                        if data.get('draft_intent_expired') and data.get('last_error'):
                            data.pop('last_error')
                            row=tasks.update(row['id'],row['revision'],row['state'],data)
                        gone=_reconcile_owner_sent(ledger, config, mailbox, row)
                        if gone or not revising:
                            recheck.defer(tasks,'owner-sent',row['id'],binding,OWNER_SENT_RECHECK_SECONDS)
                            ledger.save()
                        if gone:continue
                except Exception as exc:
                    current=tasks.get(row['id'])
                    tasks.update(row['id'], current['revision'], current['state'],
                                 {**current['data'], 'last_error':(str(exc) or type(exc).__name__)[:300]})
                    _defer_unsaved_owner_draft_failure(tasks, tasks.get(row['id']))
                    ledger.save()
                    continue
            if row['state']=='owner-draft' and (data.get('draft_revision_requests') or
                                                data.get('draft_revision_intent')):
                try:
                    with unit(ledger,'delivery',row['id'],'draft-revision') as ready:
                        if not ready:continue
                        row=_advance_draft_revision(ledger, config, root, mailbox, row)
                    data=dict(row['data'])
                except Exception as exc:
                    current=tasks.get(row['id'])
                    tasks.update(row['id'], current['revision'], current['state'],
                                 {**current['data'], 'last_error':(str(exc) or type(exc).__name__)[:300]})
                    ledger.save()
                    continue
            with unit(ledger,'delivery',row['id'],'mail') as ready:
                if not ready:continue
                packet=data.get('packet_override') or data['result'].get('packet')
                if not packet:continue
                checking_delivery=False
                try:
                    if row['state']=='owner-draft' and not data.get('draft_receipt'):
                        _check_expired_draft_intents(mailbox, row)
                    existing=queue.db.execute('SELECT 1 FROM attempts WHERE item=?',(row['id'],)).fetchone()
                    # A migrated task may retain an old automatic attempt even
                    # after its replacement owner draft has its own receipt.
                    if existing and not data.get('draft_receipt'):
                        current=queue.get(row['id'])
                        transport=Channel(mailbox,row['kind'])
                        started=next(e['at'] for e in queue.events(row['id']) if e['action']=='send-intent')
                        mailbox.attempt_started=started
                        # Observe every original attempt first, also during pause.
                        # Only positive pre-send evidence permits a later retry.
                        if current['state'] in {'sending','send-unknown','sent','not-sent'}:
                            current=queue.reconcile(row['id'],transport)
                        if current['state']=='verified':
                            import json
                            receipt=json.loads(queue.db.execute('SELECT readback FROM attempts WHERE item=?',(row['id'],)).fetchone()[0])
                            data.pop('last_error',None)
                            tasks.update(row['id'],row['revision'],'verified',{**data,'sent_receipt':receipt})
                        if current['state']=='not-sent':
                            data['last_error']=current['reason'][:300]
                            row=tasks.update(row['id'],row['revision'],row['state'],data)
                        ledger.save()
                        if current['state']!='not-sent':continue
                    if config['paused'] or not config['kinds'].get(row['kind']):continue
                    require(data.get('review',{}).get('verdict')=='PASS','Mail requires exact independent review')
                    current_hash=workflow(row['kind'])
                    require(matches_workflow(row,current_hash),'Mail workflow changed; re-review the current version')
                    require(review_current(row,current_hash),'Mail review no longer binds the exact result; re-review')
                    queue.workflows[row['kind']]=current_hash
                    migrated=data.get('workflow_binding',{})
                    if (migrated.get('workflow_hash')==current_hash and
                            queue.db.execute('SELECT 1 FROM items WHERE id=?',(row['id'],)).fetchone() and
                            queue.get(row['id'])['workflow_hash']==migrated.get('legacy_hash')):
                        # Keep this packet's immutable legacy review. The task
                        # gate above still checks its scoped policy on every tick.
                        queue.workflows[row['kind']]=migrated['legacy_hash']
                    source_gate(row['request'],tasks,root);parent_gate(row['request'],tasks);paper_gate(row['request'],root)
                    note_closure.source_gate(row['request'],tasks,data.get('result'))
                    owner_gate(row['request'],root)
                    from .thread_staging import effective_inputs, gate as thread_gate
                    checking_delivery=True
                    thread_gate(row['request'],root,data.get('thread_inputs'))
                    checking_delivery=False
                    # The packet-only queue has no task routing or override.
                    route=approved_route(row,data.get('routing_override'))
                    require(route==row['state'],'Mail ownership or decision route changed')
                    if route=='owner-draft' and data.get('draft_receipt'):continue
                    expired_intent=None
                    if route=='owner-draft' and data.get('draft_intent'):
                        intent=data['draft_intent'];mailbox.attempt_started=intent['at']
                        ledger.save(force=True)
                        sent=mailbox.lookup(packet,intent['key'])
                        if sent is not None:
                            require(sent.content_hash==digest(packet),'Actual Sent differs from reviewed packet')
                            data.pop('last_error',None)
                            tasks.update(row['id'],row['revision'],'verified',{**data,'sent_receipt':asdict(sent)})
                            ledger.save()
                            continue
                        if tasks.clock()-intent['at'] > DRAFT_INTENT_SETTLE_SECONDS \
                                and mailbox.find('drafts',intent['key']) is None:
                            # Both fresh folder lookups succeeded. Persist this
                            # evidence with a new key before any new create call.
                            expired_intent={**intent,'confirmed_at':tasks.clock()}
                        if expired_intent is None:
                            receipt=mailbox.save_draft(packet,intent['key'],recovery=True)
                            require(receipt['content_hash']==digest(packet),'Saved draft differs from reviewed packet')
                            data.pop('last_error',None)
                            tasks.update(row['id'],row['revision'],'owner-draft',{**data,'draft_receipt':receipt});ledger.save()
                            continue
                    if existing:
                        # Cleanup is a write: pause/kind, exact review and ownership
                        # gates above still apply. Do this before fresh preflight,
                        # which correctly refuses any remaining server draft.
                        require(route=='awaiting-mailbox' or data.get('routing_override'),
                                'Automatic attempt cannot become an owner draft')
                        queue.cleanup_not_sent(row['id'],Channel(mailbox,row['kind']))
                    if route=='owner-draft':
                        if not allowed(ledger,'delivery',row['id']):continue
                        row=_retire_stale_owner_draft(ledger,mailbox,row,packet,config,root)
                        data=dict(row['data'])
                    checking_delivery=True
                    policy=mailbox.preflight(packet,{**row['request'],'inputs':effective_inputs(row)})
                    checking_delivery=False
                    if not allowed(ledger,'delivery',row['id']):continue
                    _register_reply_evidence(policy, root)
                    policy.pop('mail_thread_contacts',None)
                    exception=policy.pop('recipient_exception',None)
                    if exception:
                        data['recipient_exception']=exception
                        row=tasks.update(row['id'],row['revision'],row['state'],data)
                    recipient_keys=[]
                    for recipient in packet['to']+packet['cc']:
                        check=preflight(recipient,root=root)
                        require(check['registered'],'Mail recipient is not registered in table 1')
                        if route=='awaiting-mailbox':
                            require(not check['refusal_ids'],'Recipient has an active table-3 boundary')
                        if row['kind']=='firstcontact':
                            recipient_keys.append(digest(['firstcontact-scholar',check['contact_id']]))
                    data.pop('last_error',None)
                    data.pop('stale_draft_conflict',None)
                    if route=='owner-draft':
                        intent=data.get('draft_intent')
                        if intent is None or expired_intent is not None:
                            key=(digest(['owner-draft-retry',intent['key']]) if expired_intent is not None
                                 else digest(['owner-draft',row['id'],digest(packet)]))
                            if expired_intent is not None:
                                data['draft_intent_expired']=[*data.get('draft_intent_expired',[]),
                                                              {**expired_intent,'replacement_key':key}]
                            intent={'key':key,'at':tasks.clock()}
                            data['draft_intent']=intent
                            row=tasks.update(row['id'],row['revision'],'owner-draft',data)
                            recovery=False
                        else:recovery=True
                        mailbox.attempt_started=intent['at']
                        ledger.save(force=True)  # Persist draft intent before the mailbox write.
                        receipt=mailbox.save_draft(packet,intent['key'],recovery=recovery)
                        require(receipt['content_hash']==digest(packet),'Saved draft differs from reviewed packet')
                        tasks.update(row['id'],row['revision'],'owner-draft',{**data,'draft_receipt':receipt});ledger.save()
                        continue
                    override=home_recipient_override(packet,suffixes)
                    if override is not None:
                        tasks.update(row['id'],row['revision'],'owner-draft',{**data,'routing_override':override})
                        ledger.save()
                        continue
                    require(config.get('automation',{}).get('automatic_mail') is True,
                            'Automatic mail has not been activated after live acceptance')
                    conflicts=_send_conflicts(tasks,mailbox,row,packet,queue)
                    if conflicts['task_ids'] or conflicts['draft_ids']:
                        blocked={**data, 'reason':SEND_GUARD_REASON,
                                 'delivery_conflicts':conflicts,
                                 'conflicting_task_ids':conflicts['task_ids'],
                                 'conflicting_draft_ids':conflicts['draft_ids']}
                        tasks.update(row['id'],row['revision'],'held',blocked)
                        ledger.save()
                        continue
                    current=queue.add(row['id'],packet)
                    binding=queue.binding(row['id'])
                    if current['state']=='drafted':queue.research_checked(binding,data['review']['evidence_ref'])
                    if queue.get(row['id'])['state']=='research-checked':queue.enqueue(binding)
                    if queue.get(row['id'])['state']=='in-review':
                        queue.verdict({'schema_version':1,**binding,'reviewer':'local-claude-loop','verdict':'PASS',
                                       'reason':data['review']['reason'],'evidence_ref':data['review']['evidence_ref']})
                    old=queue.db.execute('SELECT revision FROM policies WHERE kind=?',(row['kind'],)).fetchone()
                    queue.set_policy(row['kind'],(old[0] if old else 0)+1,policy)
                    if not existing:mailbox.attempt_started=tasks.clock()
                    authority=Authority(row['kind'],packet['sender'],policy['expires_at'],
                                        'managed/automation.json:user-authorized-routine-academic-mail')
                    current=queue.dispatch(row['id'],Channel(mailbox,row['kind']),authority,recipient_keys)
                    if current['state'] in {'not-sent','send-unknown'}:
                        data['last_error']=current['reason'][:300]
                        row=tasks.update(row['id'],row['revision'],row['state'],data)
                    if current['state']=='not-sent':
                        ledger.save()
                        queue.cleanup_not_sent(row['id'],Channel(mailbox,row['kind']))
                    ledger.save()
                    # A later tick performs readback, also when the submit call timed
                    # out. This includes crashes between submit and snapshot save.
                except Exception as exc:
                    row=tasks.get(original['id'])
                    data={**row['data'],'last_error':(str(exc) or type(exc).__name__)[:300]}
                    terminal=checking_delivery and _delivery_terminal(exc,row,mailbox,packet,queue)
                    if terminal:
                        data.update(hold_reason=str(exc),hold_kind='delivery-terminal')
                    tasks.update(row['id'],row['revision'],'held' if terminal else row['state'],data)
                    _defer_unsaved_owner_draft_failure(tasks, tasks.get(row['id']))
                    ledger.save()
    finally:queue.close()
