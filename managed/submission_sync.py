"""Reconcile arXiv system receipts into the compact submission board.

This is the read-only mailbox half of the submission-to-Pages bridge.  The
files it may update are the submission board, its generated reading view, and
the public publication catalog. It never creates drafts, sends messages, or submits
anything to arXiv or a journal.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
from pathlib import Path
from urllib.error import HTTPError, URLError

from . import submission_mail, submissions
from .queue import canonical

from .location import DATA_ROOT as ROOT, load_config, settings


def _iso(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


def publication_catalog(root, records):
    """Add verified papers from mail, without publishing private mail fields."""
    path = Path(root) / 'tables/publications.json'
    if not path.exists():
        return None, []
    from .arxiv_watch import _publication
    from scripts.short_notes import workflow as w
    catalog = json.loads(path.read_text())
    w.validate_feed({'schema_version': 'research-updates.v1',
                    'publications': catalog['publications'], 'collaborations': catalog['collaborations']})
    identities = {w.publication_identity(paper['url']) for paper in catalog['publications']}
    ids = {item['id'] for kind in ('publications', 'collaborations', 'notes') for item in catalog[kind]}
    added = []
    for record in records:
        team = set(settings('identity')['team_authors'])
        if not team or not team <= set(record.get('authors', [])):
            continue
        identity = w.publication_identity(record['arxiv_url'])
        if identity in identities or date.fromisoformat(record['date']) > date.today():
            continue
        paper = _publication({'arxiv_id': record['arxiv_id'],
            'version': int(record['arxiv_version'][1:]), 'title': record['title'],
            'authors': record['authors'], 'date': date.fromisoformat(record['date']),
            'abstract': record.get('abstract', '')})
        if paper['id'] in ids:
            paper['id'] += '-' + record['arxiv_id'].replace('.', '-')
        catalog['publications'].append(paper)
        identities.add(identity); ids.add(paper['id']); added.append(paper)
    if not added:
        return None, []
    catalog['publications'].sort(key=lambda paper: paper['id'])
    w.validate_feed({'schema_version': 'research-updates.v1',
                    'publications': catalog['publications'], 'collaborations': catalog['collaborations']})
    return catalog, added


def collect(mailbox, since, until=None):
    """Read bounded arXiv receipts and explicit public links in incoming mail."""
    until = until or datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    rows = mailbox.messages('inbox', {'$filter': 'receivedDateTime ge ' + since +
                                     ' and receivedDateTime le ' + until}, include_unique_body=True)
    result, seen = [], set()
    for row in rows:
        conversation = row.get('conversationId')
        if not conversation or conversation in seen:
            continue
        if not submission_mail.is_system_message(row) and not submission_mail.parse_message(row):
            continue
        seen.add(conversation)
        thread = mailbox.thread(conversation)
        for message in thread:
            if submission_mail.parse_message(message):
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
    _, additions = publication_catalog(root, report.get('publications', []))
    report = {**report, 'messages': len(messages),
              'new_publications': additions,
              'status': 'updated' if report['updates'] or additions else 'idle'}
    return report, updated


def write_board(root, board):
    root = Path(root)
    path = root / 'tables/submissions.json'
    path.write_text(canonical(board))
    submissions.render(root)


def commit_board(root=ROOT):
    """Commit the board/view and public metadata through the selected data branch."""
    from .data_writes import commit_paths, push

    paths = ('tables/submissions.json', 'tables/SUBMISSIONS.md', 'tables/publications.json')
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
    if args.apply and (report.get('updates') or report.get('new_publications')):
        if report.get('updates'):
            write_board(root, board)
        catalog, _ = publication_catalog(root, report.get('publications', []))
        if catalog:
            (root / 'tables/publications.json').write_text(
                json.dumps(catalog, ensure_ascii=False, sort_keys=True, indent=2) + '\n')
        if args.push:
            if __import__('os').environ.get('GITHUB_ACTIONS') != 'true':
                raise ValueError('Board push is restricted to the serialized GitHub Actions writer')
            report['commit'] = commit_board(root)
    print(canonical(report), end='')
    return 0


if __name__ == '__main__':
    from .run_log import public_run
    raise SystemExit(public_run(main, 'submissions'))
