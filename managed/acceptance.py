"""Real delivery acceptance, restricted to two user-authorized test inboxes.

This separate command never consumes production tasks or updates contact facts.
Its receipt is saved before every write. An uncertain send is only looked up.
"""
import argparse
import base64
import json
from pathlib import Path
import re
import time
from urllib.parse import quote

from .credentials import graph_access, local_graph_cache
from .location import settings
from .mailbox import Graph, GraphMailbox
from .queue import require

def mailbox_address():
    return settings('mailbox')['account']


def recipients():
    return {mailbox_address(), *settings('mailbox')['acceptance_recipients']}


def packet(test_id, recipient):
    require(recipient in recipients(), 'Only the two user-authorized test inboxes are allowed')
    require(re.fullmatch(r'[a-z0-9][a-z0-9-]{7,79}', test_id), 'Use a unique test identifier')
    return {'kind': 'firstcontact', 'sender': mailbox_address(), 'to': [recipient], 'cc': [],
            'subject': '[Outreach TEST] ' + test_id,
            'body': 'This is an authorized Outreach workflow delivery test.\n'
                    'It checks draft saving, exact content, attachments and Sent readback.\n'
                    'No research correspondence is being sent. No reply is needed.\n'
                    'Test ID: ' + test_id + '\n',
            'attachments': [{'name': 'outreach-test.txt', 'media_type': 'text/plain',
                             'content_b64': base64.b64encode(('Outreach delivery test: ' + test_id + '\n').encode()).decode()}]}


def delivery(mailbox, test_id, recipient, state_path, reply_to=None):
    approved = packet(test_id, recipient)
    if reply_to is not None:
        require(recipient == mailbox_address() and reply_to.get('recipient') == mailbox_address() and
                reply_to.get('phase') == 'sent-verified' and reply_to.get('test_id') != test_id,
                'Reply acceptance requires a different verified self-test, never a research thread')
        parent = mailbox.message(reply_to['sent_id'])
        require(not parent.get('isDraft') and
                mailbox.matches(parent, packet(reply_to['test_id'], mailbox_address())) and
                parent['internetMessageId'] == reply_to['internet_message_id'],
                'Reply parent differs from the actual self-test message')
        approved.update(kind='reply', subject='Re: ' + parent['subject'],
                        thread_id=parent['conversationId'],
                        latest_inbound_message_id=parent['internetMessageId'])
    key = 'outreach-test-' + test_id
    path = Path(state_path)
    state = json.loads(path.read_text()) if path.exists() else {
        'test_id': test_id, 'recipient': recipient, 'started_at': time.time(), 'phase': 'new'}
    require(state['test_id'] == test_id and state['recipient'] == recipient, 'Test identity changed')
    from .queue import digest
    binding = digest(approved)
    require(state.get('packet_hash', binding) == binding, 'Do not change an attempted test packet')
    state['packet_hash'] = binding
    mailbox.attempt_started = state['started_at']

    def save():
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix('.pending')
        temporary.write_text(json.dumps(state, indent=2) + '\n')
        temporary.chmod(0o600)
        temporary.replace(path)

    require(mailbox.verify_owner() == mailbox_address(), 'Wrong test sender')
    save()
    if state['phase'] == 'new':
        state['phase'] = 'creating-draft'
        save()
        draft = mailbox.create_draft(approved, key)
        state.update(phase='draft-verified', draft_id=draft['id'], conversation_id=draft['conversationId'])
        save()
    elif state['phase'] == 'creating-draft':
        draft = mailbox.find('drafts', key)
        require(draft is not None, 'Original draft outcome uncertain; no second draft will be created')
        require(mailbox.matches(draft, approved), 'Recovered draft differs from the test packet')
        state.update(phase='draft-verified', draft_id=draft['id'], conversation_id=draft['conversationId'])
        save()
    if state['phase'] == 'draft-verified':
        # Re-read the existing draft so a user edit can never be overwritten/sent.
        draft = mailbox.message(state['draft_id'])
        require(draft.get('isDraft') and mailbox.matches(draft, approved), 'Test draft changed; preserve it')
        state['phase'] = 'send-attempted'
        save()
        mailbox.graph.request('/me/messages/' + quote(state['draft_id'], safe='') + '/send', 'POST', {})
    if state['phase'] in {'send-attempted', 'sent-verified'}:
        sent = mailbox.find('sentitems', key)
        if sent is not None:
            require(not sent.get('isDraft') and mailbox.matches(sent, approved), 'Actual Sent copy differs from the test packet')
            if reply_to is not None:
                headers = {h['name'].lower(): h['value'] for h in sent.get('internetMessageHeaders', [])}
                require(sent['conversationId'] == approved['thread_id'] and
                        headers.get('in-reply-to') == approved['latest_inbound_message_id'] and
                        approved['latest_inbound_message_id'] in headers.get('references', ''),
                        'Actual Sent reply lost its original conversation or source headers')
                state['reply_thread_verified'] = True
            state.update(phase='sent-verified', sent_id=sent['id'], internet_message_id=sent['internetMessageId'],
                         sent_at=sent.get('sentDateTime'), attachment_verified=True,
                         verified_at=time.time())
            save()
    return state


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--test-id', required=True)
    p.add_argument('--recipient', required=True, choices=sorted(recipients()))
    p.add_argument('--state', required=True, type=Path)
    p.add_argument('--reply-to-test-state', type=Path,
                   help='Reply only within an actual previously verified self-test conversation')
    args = p.parse_args()
    # Validate the destination before acquiring a credential or making a call.
    packet(args.test_id, args.recipient)
    token = graph_access(local_graph_cache())
    mailbox = GraphMailbox(Graph(token), {'account': mailbox_address()})
    parent = json.loads(args.reply_to_test_state.read_text()) if args.reply_to_test_state else None
    result = delivery(mailbox, args.test_id, args.recipient, args.state, parent)
    print(json.dumps(result))


if __name__ == '__main__':
    main()
