"""Local Claude entry for the existing broadcast-kit daily producer; no timer."""
from . import recording

import argparse
from datetime import date, datetime, timedelta
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import unicodedata
from zoneinfo import ZoneInfo

from .control import review_key, workflow, workflow_text
from .queue import canonical, digest, require

from .location import DATA_ROOT, ENGINE_ROOT as ROOT


def local_root():
    common = recording.check_output(['git', 'rev-parse', '--path-format=absolute', '--git-common-dir'],
                                     cwd=ROOT, text=True).strip()
    return Path(common).parent.parent / 'mail/outreach-maintenance/daily'


def read(path):
    return json.loads(Path(path).read_text())


def save(path, value):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')
    temporary.replace(path)


def featured_refusal_conflicts(packet_dir, root):
    """Match featured results against current contact IDs and refused surnames."""
    digest_path = Path(packet_dir) / 'digest.json'
    featured = (read(digest_path).get('featured') or []) if digest_path.exists() else []
    if not featured:
        return []
    contacts = {c['id']: c for c in read(Path(root) / 'tables/contacts.json')['contacts']}
    refusals = read(Path(root) / 'tables/refusals.json')['refusals']

    def fold(text):
        return ''.join(c for c in unicodedata.normalize('NFKD', text.casefold())
                       if not unicodedata.combining(c))

    conflicts = []
    for refusal in refusals:
        if not refusal.get('active'):
            continue
        contact = contacts.get(refusal.get('contact_id'), {})
        name = refusal.get('name') or contact.get('name', '')
        surname = fold(name.split()[-1]) if name.split() else ''
        for entry in featured:
            problem_id = entry.get('problem_id', '')
            matched_by = []
            if problem_id and problem_id in contact.get('problem_ids', []):
                matched_by.append('problem_id')
            text = '\n'.join([entry.get('text', ''), entry.get('card_title', ''),
                              re.sub(r'[-_]', ' ', problem_id)])
            if len(surname) >= 3 and re.search(r'\b' + re.escape(surname) + r'\b', fold(text)):
                matched_by.append('surname')
            if matched_by:
                conflicts.append({'name': name, 'refusal_id': refusal['id'],
                                  'contact_id': refusal.get('contact_id'),
                                  'problem_id': problem_id, 'matched_by': matched_by})
    return conflicts


def require_no_featured_refusals(packet_dir):
    conflicts = featured_refusal_conflicts(packet_dir, DATA_ROOT)
    require(not conflicts, 'Daily featured result conflicts with an active refusal: ' +
            '; '.join(c['name'] + ' (' + c['refusal_id'] + ')' for c in conflicts))


def initialize(path):
    """Import settings and small daily receipts, leaving historical evidence intact."""
    path = Path(path)
    if path.exists():
        return {'config': str(path), 'created': False}
    base = path.parent
    omega = local_root().parents[2]
    legacy = omega / 'mail/outreach-maintenance/legacy-daily'
    old = read(legacy / 'daily-config.json')
    generic = read(old['publication_config'])
    keys = ['nyxid', 'codex_cli', 'account', 'x_service', 'x_transport',
            'preparation_model', 'preparation_reasoning_effort', 'final_review_model',
            'final_review_reasoning_effort', 'allow_isolated_review_fallback']
    config = {**{k: generic[k] for k in keys if k in generic}, **old}
    config.pop('publication_config', None)
    config.update(enabled=False, workspace=str(base), campaign=str(base / 'campaign'),
                  daily_state=str(base / 'state'), historical_state=str(legacy / 'state'),
                  broadcast_kit=str(omega / 'broadcast-kit'),
                  python=str(omega / 'broadcast-kit/.venv/bin/python'),
                  catalogue_url='https://www.trureturing.org/assets/research-catalog.json',
                  overview_url='https://www.trureturing.org/research.html?lang=en',
                  inventory_path=str(base / 'published_inventory.jsonl'),
                  blocklist_path=str(base / 'do_not_republish.jsonl'))
    base.mkdir(parents=True, exist_ok=True)
    # Keep the same source tool/model settings. These are local editorial
    # workers, independent of the configured CMA research model.
    receipts = [read(p) for p in sorted((legacy / 'state').glob('????-??-??/receipt.json'))]
    for name, rows in [('published_inventory.jsonl', receipts), ('do_not_republish.jsonl', [])]:
        target = base / name
        require(not target.exists(), 'Do not overwrite an existing local publication ledger')
        target.write_text(''.join(canonical(r) + '\n' for r in rows))
    # The existing renderer reads previous audited counts beside the current
    # date. Copy statistics only; full history, reviews and PNGs stay archived.
    for source in sorted((legacy / 'state').glob('????-??-??/stats.json'))[-10:]:
        save(base / 'state' / source.parent.name / source.name, read(source))
    save(path, config)
    return {'config': str(path), 'created': True, 'imported_receipts': len(receipts), 'publication_enabled': False}


def covered(day, first=None):
    start, end = date.fromisoformat(first or day), date.fromisoformat(day)
    require(start <= end and (end - start).days < 366, 'Invalid daily coverage span')
    return {(start + timedelta(days=i)).isoformat() for i in range((end - start).days + 1)}


def reservations(config):
    rows = []
    for base in {config['daily_state'], config['historical_state']}:
        for root in sorted(Path(base).glob('????-??-??')):
            if not root.is_dir():
                continue
            status = read(root / 'status.json') if (root / 'status.json').exists() else {}
            receipt = read(root / 'receipt.json') if (root / 'receipt.json').exists() else None
            attempted = (root / 'post-attempt.json').exists()
            if receipt or status.get('status') in {'published', 'merged'} or attempted:
                stats = read(root / 'stats.json') if (root / 'stats.json').exists() else {}
                first = stats.get('first_day') or config.get('merged_days', {}).get(root.name)
                rows.append({'directory': str(root), 'day': root.name, 'covered_days': sorted(covered(root.name, first)),
                             'published': bool(receipt or status.get('status') == 'published'),
                             'url': (receipt or status).get('url'), 'attempted': attempted})
    return rows


def status(path=None, day=None, now=None):
    path = Path(path or local_root() / 'config.json')
    if not path.exists():
        return {'status': 'unconfigured', 'config': str(path), 'next_action': 'python3 -m managed.daily_local init'}
    config = read(path)
    now = now or datetime.now(ZoneInfo(config['timezone']))
    day = day or (now.date() - timedelta(days=1)).isoformat()
    require(date.fromisoformat(day) < now.date(), 'Select a complete Singapore calendar day')
    rows = reservations(config)
    requested = covered(day, config.get('merged_days', {}).get(day))
    overlap = [r for r in rows if requested & set(r['covered_days'])]
    published = [r for r in rows if r['published']]
    latest = max(published, key=lambda r: r['day']) if published else None
    packet = Path(config['daily_state']) / day
    phase = 'reserved' if overlap else 'prepared' if (packet / 'candidate.json').exists() else 'not_started'
    review = None
    if not overlap and (packet / 'sshx-transcript.json').exists():
        phase = 'awaiting-claude-review'
        current = binding(config, day)
        review_path = packet / ('claude-review-' + digest(current) + '.json')
        if review_path.exists():
            review = read(review_path)
            phase = {'PASS': 'reviewed', 'BOUNCE': 'needs-revision', 'HOLD': 'held'}[review['verdict']]
    pending = sorted(covered(day, (date.fromisoformat(latest['day']) + timedelta(days=1)).isoformat())) \
        if latest and latest['day'] < day else ([] if overlap else [day])
    return {'day': day, 'status': phase, 'pending_days_since_last_publication': pending,
            'config': str(path), 'directory': str(packet), 'reservations': overlap,
            **({'featured_refusal_conflicts': featured_refusal_conflicts(packet, DATA_ROOT)} if packet.exists() else {}),
            'last_published': latest, 'claude_review': review, 'publication_paused': not config.get('enabled') or
                not json.loads(workflow_text('daily/workflow.json'))['enable_live_publication'],
            'scheduler': 'user-started local Claude loop; no launchd or CMA daily job',
            'next_action': 'inspect original receipt/attempt' if overlap else
                'inspect Claude review and repair/hold this exact packet' if phase in {'held', 'needs-revision'} else
                'python3 -m managed.daily_local publish --day ' + day if phase == 'reviewed' else
                ('python3 -m managed.daily_local binding --day ' if phase == 'awaiting-claude-review' else
                 'python3 -m managed.daily_local prepare --day ') + day}


def binding(config, day):
    root = Path(config['daily_state']) / day
    stats = read(root / 'stats.json')
    require(stats['day'] == day and stats.get('first_day') == config.get('merged_days', {}).get(day),
            'Daily date coverage changed; prepare and review the actual selected span')
    names = ['candidate.json', 'body.txt', 'card.png', 'stats.json', 'digest.json', 'catalogue.json',
             'source-evidence.json', 'research-focus.json', 'history.json', 'cumulative.json', 'facts.json', 'sshx-transcript.json']
    hashes = {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in names}
    return {'task_id': 'daily-' + day, 'task_revision': 1,
            'result_hash': digest({'files': hashes, 'account': config['account'],
                                   'covered_days': sorted(covered(day, stats.get('first_day')))}),
            'workflow_hash': workflow('daily')}


def record_review(config, day, envelope, secret):
    require(set(envelope) == {'binding', 'reviewer', 'verdict', 'reason', 'evidence_ref', 'signature'}, 'Invalid daily Claude review')
    unsigned = {k: v for k, v in envelope.items() if k != 'signature'}
    expected = hmac.new(secret.encode(), canonical(unsigned).encode(), hashlib.sha256).hexdigest()
    require(hmac.compare_digest(expected, envelope['signature']), 'Daily Claude review signature failed')
    require(envelope['reviewer'] == 'local-claude-loop' and envelope['verdict'] in {'PASS', 'BOUNCE', 'HOLD'}
            and all(isinstance(envelope[k], str) and envelope[k].strip() for k in ['reason', 'evidence_ref']), 'Missing actual independent review')
    require(envelope['binding'] == binding(config, day), 'Daily packet or workflow changed; re-review')
    if envelope['verdict'] == 'PASS':
        require_no_featured_refusals(Path(config['daily_state']) / day)
    path = Path(config['daily_state']) / day / ('claude-review-' + digest(envelope['binding']) + '.json')
    require(not path.exists() or read(path) == envelope, 'Existing review is immutable; revise the packet for a new review')
    save(path, envelope)
    return {'day': day, 'verdict': envelope['verdict'], 'review': str(path)}


def operate(action, config, day, producer):
    """Consume existing producer APIs. Preparation never calls its publisher."""
    root = Path(config['daily_state']) / day
    requested = covered(day, config.get('merged_days', {}).get(day))
    with producer.q.locked(Path(config['daily_state']) / 'daily.lock'):
        # Preserve all historical published, merged and uncertain dates. The
        # current known-ID attempt may only be reconciled, never recreated.
        other = [r for r in reservations(config) if Path(r['directory']) != root and requested & set(r['covered_days'])]
        require(not other, 'Daily dates are already reserved; inspect original receipt/attempt')
        if (root / 'receipt.json').exists():
            return {'status': 'published', 'receipt': read(root / 'receipt.json')}
        if action == 'prepare':
            require(not (root / 'post-attempt.json').exists(), 'A prior X attempt must be reconciled before editing')
            root = producer.prepare(config, day)
            if not (root / 'sshx-transcript.json').exists():
                from broadcast_kit.research_daily_review import review
                review(config, root)
            producer.validate(config, root)
            save(root / 'status.json', {'status': 'awaiting-claude-review', 'day': day})
            return {'status': 'awaiting-claude-review', 'day': day, 'directory': str(root), 'binding': binding(config, day)}
        require(action == 'publish', 'Unknown local daily operation')
        require(config.get('enabled') and json.loads(workflow_text('daily/workflow.json'))['enable_live_publication'], 'Daily publication is paused')
        require_no_featured_refusals(root)
        current = binding(config, day)
        path = root / ('claude-review-' + digest(current) + '.json')
        require(path.exists(), 'Exact independent Claude PASS is required')
        review = read(path)
        record_review(config, day, review, review_key())
        require(review['verdict'] == 'PASS', 'The current Claude verdict is not PASS')
        if (root / 'post-attempt.json').exists():
            created = read(root / 'create.json') if (root / 'create.json').exists() else {}
            require(created.get('data', {}).get('id'), 'Unknown/rejected X attempt: reconcile original evidence, never replay POST')
        return producer.publish(config, root)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['init', 'status', 'prepare', 'binding', 'review', 'publish'])
    parser.add_argument('--config', type=Path)
    parser.add_argument('--day')
    parser.add_argument('--envelope', type=Path)
    args = parser.parse_args(argv)
    path = (args.config or local_root() / 'config.json').resolve()
    if args.action == 'init':
        result = initialize(path)
    else:
        view = status(path, args.day)
        require(view['status'] != 'unconfigured', 'Run python3 -m managed.daily_local init first')
        day, config = view['day'], read(path)
        if args.action == 'status':
            result = view
        elif args.action == 'binding':
            result = {'binding': binding(config, day), 'directory': view['directory']}
        elif args.action == 'review':
            require(args.envelope, 'Supply the actual independently signed review envelope')
            result = record_review(config, day, read(args.envelope), review_key())
        else:
            if args.action == 'publish':
                require(not view['publication_paused'], 'Daily publication is paused')
            # Reuse the existing local compiler/image dependencies without
            # installing an environment in the control repository.
            if Path(sys.executable).absolute() != Path(config['python']).absolute():
                env = dict(os.environ)
                env['PYTHONPATH'] = os.pathsep.join([str(ROOT), config['broadcast_kit'], env.get('PYTHONPATH', '')])
                command = [config['python'], '-m', 'managed.daily_local', args.action, '--config', str(path), '--day', day]
                raise SystemExit(recording.run(command, cwd=ROOT, env=env).returncode)
            sys.path.insert(0, config['broadcast_kit'])
            from broadcast_kit import research_daily
            result = operate(args.action, config, day, research_daily)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
