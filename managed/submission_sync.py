"""Reconcile arXiv system receipts into the compact submission board.

This is the read-only mailbox half of the submission-to-Pages bridge.  The
only files it may update are ``tables/submissions.json`` and its generated
``SUBMISSIONS.md`` view.  It never creates drafts, sends messages, or submits
anything to arXiv or a journal.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
from urllib.error import HTTPError, URLError

from . import submission_mail, submissions
from .queue import canonical

from .location import DATA_ROOT as ROOT, load_config


def _iso(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


def collect(mailbox, since, until=None):
    """Fetch only arXiv system threads from a bounded Inbox window."""
    until = until or datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    rows = mailbox.messages('inbox', {'$filter': 'receivedDateTime ge ' + since +
                                     ' and receivedDateTime le ' + until})
    result, seen = [], set()
    for row in rows:
        conversation = row.get('conversationId')
        if not conversation or conversation in seen:
            continue
        if not submission_mail.is_system_message(row):
            continue
        seen.add(conversation)
        thread = mailbox.thread(conversation)
        for message in thread:
            if submission_mail.is_system_message(message):
                result.append(message)
    return result


def reconcile(root=ROOT, mailbox=None, since=None, until=None, verify=None):
    """Return a safe report and an updated board copy; write nothing."""
    root = Path(root)
    board = json.loads((root / 'tables/submissions.json').read_text())
    submissions.validate_data(board)
    if mailbox is None:
        return {'updates': [], 'ambiguous': [], 'ignored': [], 'status': 'mailbox-unavailable'}, board
    since = since or mailbox.settings.get('live_since')
    if not since or _iso(since) > _iso(until or datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')):
        raise ValueError('Submission mailbox boundary is missing or in the future')
    messages = collect(mailbox, since, until)
    try:
        report = submission_mail.reconcile(board['papers'], messages, verify=verify)
    except HTTPError as error:
        # arXiv rate limits and transient server failures must leave the board
        # untouched and allow the serialized workflow to continue.
        if error.code == 429 or 500 <= error.code < 600:
            return {'updates': [], 'ambiguous': [], 'ignored': [], 'messages': len(messages),
                    'status': 'arxiv-unavailable',
                    'error': f'HTTPError {error.code}: {str(error.reason)[:200]}'}, board
        raise
    except (URLError, TimeoutError, OSError) as error:
        return {'updates': [], 'ambiguous': [], 'ignored': [], 'messages': len(messages),
                'status': 'arxiv-unavailable',
                'error': type(error).__name__ + ': ' + str(error)[:200]}, board
    if report['updates']:
        updated = submission_mail.apply_updates(board, report)
        updated['updated_on'] = datetime.now(timezone.utc).date().isoformat()
        submissions.validate_data(updated)
    else:
        updated = board
    report = {**report, 'messages': len(messages), 'status': 'updated' if report['updates'] else 'idle'}
    return report, updated


def write_board(root, board):
    root = Path(root)
    path = root / 'tables/submissions.json'
    path.write_text(canonical(board))
    submissions.render(root)


def commit_board(root=ROOT):
    """Commit only the two board files; refuse to absorb unrelated changes."""
    from .data_writes import commit_paths, push

    paths = ('tables/submissions.json', 'tables/SUBMISSIONS.md')
    if not commit_paths(paths, 'Reconcile public arXiv submission receipts', root):
        return {'committed': False, 'reason': 'unchanged'}
    push(root)
    return {'committed': True}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--apply', action='store_true', help='Write the board after unique verified matches')
    parser.add_argument('--push', action='store_true', help='Commit and push the two board files (Actions only)')
    args = parser.parse_args(argv)
    root = args.root
    config = load_config(root)
    if config.get('mailbox_adapter') != 'microsoft-graph':
        print(canonical({'status': 'disabled', 'reason': 'mailbox adapter is not Microsoft Graph'}), end='')
        return 0
    from .mailbox import configured
    mailbox = configured(config, root)
    report, board = reconcile(root, mailbox)
    if args.apply and report.get('updates'):
        write_board(root, board)
        if args.push:
            if __import__('os').environ.get('GITHUB_ACTIONS') != 'true':
                raise ValueError('Board push is restricted to the serialized GitHub Actions writer')
            report['commit'] = commit_board(root)
    print(canonical(report), end='')
    return 0


if __name__ == '__main__':
    from .run_log import public_run
    raise SystemExit(public_run(main, 'submissions'))
