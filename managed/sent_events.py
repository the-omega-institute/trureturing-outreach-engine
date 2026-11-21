"""Serialized, metadata-only Sent Items intake for the maintained contact map."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import sys

from . import registry
from .mailbox import address, instant, _machine_sender
from .queue import canonical, require
from .tasks import review_current
from .tick import unit

FILES = ('tables/contacts.json', 'tables/CONTACTS.md', 'tables/POSITIVE.md', 'README.md')
CURSOR = 'sent-events'
MAX_PAGES = 2


def system_recipient(value):
    """Reuse mailbox machine identities and exclude common system destinations."""
    local = value.rsplit('@', 1)[0]
    return (_machine_sender(value) or
            re.match(r'^(?:no[-_.]?reply|do[-_.]?not[-_.]?reply|mailer[-_.]?daemon|postmaster|notifications?'
                     r'|system|auto[-_.]?reply|bounces?)'
                     r'(?:[+._-].*)?$', local) is not None or
            value.rsplit('@', 1)[-1] in {'arxiv.org', 'export.arxiv.org'})


def task_context(rows, message):
    """Only exact receipt identities link a task; never correlate by subject/body."""
    matched = []
    for row in rows:
        data = row['data']
        for key in ('sent_receipt', 'draft_receipt'):
            receipt = data.get(key) or {}
            if ((message.get('id') and receipt.get('remote_id') == message['id']) or
                    receipt.get('internetMessageId') == message['internetMessageId'] or
                    receipt.get('evidence_ref') == 'graph:sent:' + message['internetMessageId']):
                matched.append(row)
                break
    if len(matched) != 1:
        return None, ''
    row = matched[0]
    result = row['data'].get('result') or {}
    summary = ''
    if review_current(row, row['request']['workflow_hash']):
        summary = result.get('headline') or (result.get('owner_brief') or {}).get('summary_zh') or ''
    return row['id'], ' '.join(summary.split())[:300]


def recorded(person, recipient, message_id, at):
    for row in person['records']:
        for event in row.get('hub', {}).get('events', []):
            if event.get('kind') != 'sent':
                continue
            source = event.get('evidence', '')
            try:
                evidence = json.loads(source) if isinstance(source, str) else source
            except ValueError:
                evidence = None
            if isinstance(evidence, dict) and evidence.get('recipient', recipient) != recipient:
                continue
            if message_id in (source if isinstance(source, str) else json.dumps(source)):
                return True
            # Precise events from this pass distinguish different messages even
            # when two actual sends occur within ten minutes. The time fallback
            # covers legacy/manual events without this source marker.
            if isinstance(evidence, dict) and evidence.get('source') == CURSOR:
                continue
            if not isinstance(evidence, dict) or not evidence.get('recipient'):
                # Legacy events name no address; any address of the same person counts.
                if recipient not in person.get('emails', [registry.email(row['email'])]):
                    continue
            try:
                if abs((instant(event['at']) - at).total_seconds()) <= 600:
                    return True
            except (ValueError, TypeError, KeyError):
                continue
    return False


def record_page(root, messages, rows, ignored, since, until, account=None):
    contacts = registry.read(root, 'contacts')
    persons = registry.people(root)
    people = {a: p for p in persons for a in p['emails']}
    targets = {r['id']: r for r in contacts['contacts']}
    # Deduplication must see additions earlier in this same page too.
    for person in persons:
        person['records'] = [targets[r['id']] for r in person['records']]
    count = 0
    for message in messages:
        message_id = message.get('internetMessageId')
        if message.get('isDraft') or not isinstance(message_id, str) or not message_id.strip():
            continue
        try:
            at = instant(message['sentDateTime'])
        except (KeyError, ValueError, TypeError, AttributeError):
            continue
        if not since <= at <= until:
            continue
        recipients = {address(r) for field in ('toRecipients', 'ccRecipients', 'bccRecipients')
                      for r in message.get(field, [])}
        recipients = sorted(r for r in recipients if r in people and r not in ignored and not system_recipient(r))
        if not recipients:
            continue
        task_id, context = task_context(rows, message)
        subject = ' '.join(str(message.get('subject') or '(no subject)').split())
        summary = subject + (' — ' + context if context else '')
        for recipient in recipients:
            person = people[recipient]
            if recorded(person, recipient, message_id, at):
                continue
            evidence = {'source': CURSOR, 'internetMessageId': message_id, 'recipient': recipient}
            for key in ('id', 'conversationId'):
                if message.get(key):
                    evidence[key] = message[key]
            if account:
                evidence['account'] = account
                # Preserve a non-owner From too: it cannot close a user question.
                evidence['from'] = address(message.get('from') or {})
            if task_id:
                evidence['task_id'] = task_id
            event = {'kind': 'sent', 'at': message['sentDateTime'], 'summary': summary,
                     'evidence': json.dumps(evidence, ensure_ascii=False, sort_keys=True)}
            event['id'] = registry.identity('event', json.dumps(event, sort_keys=True, ensure_ascii=False))
            targets[person['records'][0]['id']].setdefault('hub', {}).setdefault('events', []).append(event)
            count += 1
    if count:
        registry.write(root, 'contacts', contacts)
    return count


def commit_tables(root):
    """Same Actions writer as other tables, with an explicit commit path scope."""
    require(os.environ.get('GITHUB_ACTIONS') == 'true', 'Sent table push requires the serialized Actions writer')
    from .data_writes import commit_paths, fetch, git, push

    commit_paths(FILES, 'Record actual Sent Items contact events', root)
    if push(root, check=False).returncode == 0:
        return
    # Replay the local commit over the current data branch.
    source = fetch(root)
    result = git(root, 'rebase', '--autostash', source, check=False)
    if result.returncode:
        git(root, 'rebase', '--abort', check=False)
        result.check_returncode()
    push(root)


def reconcile(ledger, config, root, mailbox, now=None, *, observe=None):
    """At most two pages per tick. Checkpoint only after table push succeeds.

    A fixed-window sweep resumes newest-first pagination across ticks; after its
    last page the next tick starts another live-window sweep to catch new sends.
    """
    if mailbox is None or config.get('paused'):
        return {'status': 'disabled', 'events': 0, 'pages': 0}
    account = mailbox.verify_owner()
    require(mailbox.settings.get('reconciled') is True, 'Mailbox boundary is not reconciled')
    since = mailbox.settings.get('live_since')
    until = (now or datetime.now(timezone.utc)).isoformat().replace('+00:00', 'Z')
    require(since and instant(since) <= instant(until), 'Invalid Sent live window')
    db = ledger.tasks.db
    saved = db.execute('SELECT payload FROM pipeline_state WHERE key=?', (CURSOR,)).fetchone()
    cursor = json.loads(saved[0]) if saved else {}
    if cursor.get('since') != since or not cursor.get('next_link'):
        cursor = {'since': since, 'until': until}
    require(instant(cursor['until']) <= instant(until), 'Sent cursor is in the future')
    ignored = {str(a).strip().casefold() for a in config.get('mailbox', {}).get('team_addresses', [])}
    ignored.add(account)
    report = {'status': 'idle', 'events': 0, 'pages': 0}
    rows = ledger.tasks.all()
    for _ in range(MAX_PAGES):
        with unit(ledger, CURSOR, 'sent-page') as ready:
            if not ready:
                report['status'] = 'deferred'
                break
            messages, link = mailbox.sent_events_page(since, cursor['until'], cursor.get('next_link'))
            if observe is not None:
                observe(messages)
            report['events'] += record_page(root, messages, rows, ignored, instant(since), instant(cursor['until']), account)
            # Always repair views and retry persistence, including after a prior
            # partial failure when all events are now present in the working tree.
            registry.render(root, paths=FILES[1:])
            commit_tables(root)
            cursor['next_link'] = link
            db.execute('INSERT OR REPLACE INTO pipeline_state VALUES(?,?)', (CURSOR, canonical(cursor)))
            ledger.save()
            report['pages'] += 1
            if not link:
                break
    if report['events']:
        report['status'] = 'updated'
    return report


def advance(ledger, config, root, mailbox, *, observe=None):
    """Failure is visible and isolated from delivery and later tick stages."""
    try:
        report = reconcile(ledger, config, Path(root), mailbox, observe=observe)
    except Exception as exc:
        # Exception text can contain Graph URLs/response details; status is content-free.
        report = {'status': 'error', 'error': type(exc).__name__}
    print('sent-events ' + json.dumps(report, sort_keys=True), file=sys.stderr, flush=True)
    db = ledger.tasks.db
    db.execute('INSERT OR REPLACE INTO pipeline_state VALUES(?,?)', ('sent-events-status', canonical(report)))
    return report
