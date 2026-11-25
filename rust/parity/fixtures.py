"""Temporary synthetic ledgers built using the live engine and its test helpers."""
import copy
import io
import json
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

ENGINE = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ENGINE), str(ENGINE / 'managed/tests')]
os.environ['OUTREACH_DATA_ROOT'] = str(ENGINE / 'rust/vectors/nonexistent')
os.environ['GITHUB_ACTIONS'] = 'false'
from managed import control, exceptions
from managed.queue import canonical, digest
from managed.tasks import Tasks, STATES
from test_tasks import request
from test_review_binding import sign
import test_exceptions

NOW = 2_000_000_000.0


def write(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')


def base(root):
    (root / 'tables').mkdir(parents=True)
    config = {'paused': True, 'kinds': {k: True for k in sorted(control.KINDS)},
              'max_active': 5, 'kind_quota': 'equal', 'followup_lane': True,
              'identity': {'owner_labels': ['operator']}}
    write(root / 'config.json', config)
    for table, value in {'contacts': {'contacts': []}, 'refusals': {'refusals': []},
                         'decisions': {}, 'threads': {'lines': []}, 'submissions': {'papers': []}}.items():
        write(root / 'tables' / (table + '.json'), value)
    return config


def build(root, rows, work=None, *, bulk=False):
    path = root / 'snapshot # π.sqlite3'
    tasks = Tasks(path, clock=lambda: NOW - 4 * 86400)
    if bulk:
        # Reproducible temporary synthetic data; avoid fsync per history row.
        tasks.db.execute('PRAGMA synchronous=OFF')
    try:
        for n, original in enumerate(rows):
            row = copy.deepcopy(original)
            identity = row.get('id') or row.get('task_id') or f'synthetic-{n}'
            kind = row.get('kind') or row.get('request', {}).get('kind') or 'research'
            req = request(identity, kind=kind)
            req.update(row.get('request') or {})
            req.update(id=identity, kind=kind)
            if req.get('workflow_hash') == 'current':
                req['workflow_hash'] = control.workflow(kind)
            if len(req.get('workflow_hash', '')) != 64:
                req['workflow_hash'] = digest(req.get('workflow_hash', ''))
            data = row.get('data') or {}
            if data.get('review', {}).get('binding', {}).get('workflow_hash') == 'current':
                data['review']['binding']['workflow_hash'] = control.workflow(kind)
            tasks.add(req)
            state = row.get('state') or 'queued'
            if state != 'queued' or data:
                tasks.update(identity, 1, state, data)
            row.update(request=req, data=data)
        work_path = root / 'work.json'
        write(work_path, work if work is not None else {'reviews': tasks.reviews()})
    finally:
        tasks.close()
    return path, work_path


def generate(folder):
    fixtures = []
    root = folder / 'empty'; base(root)
    fixtures.append(('empty', root, *build(root, [])))
    root = folder / 'states'; config = base(root)
    rows = []
    for n, state in enumerate(sorted(STATES)):
        kind = sorted(control.KINDS)[n % len(control.KINDS)]
        result = {'outcome': 'hold' if state == 'held' else 'completed', 'report': 'Synthetic π. Further detail',
                  'artifacts': [], 'routing': {'mail': 'owner', 'questions': [{'id': 'q1'}, {'id': 'q2'}]},
                  'formalization_gaps': [], 'value_assessment': {'verdict': 'substantial', 'reasons': 'synthetic'}}
        req = request(f'state-{n}', kind=kind)
        req['inputs'] = {'pipeline': {'scenario': [2, 3, 4, 5][n % 4]}, 'owner_ids': ['scholar-b']}
        rows.append({'id': req['id'], 'kind': kind, 'request': req, 'state': state,
                     'data': {'result': result, 'future_field': {'z': 10**100, 'a': [True, 1.0]}}})
    path, work = build(root, rows)
    tasks = Tasks(path, clock=lambda: NOW)
    try:
        for key, value in [('baseline', {'z': 1, 'a': 'π'}), ('ingest_rejected', {'b': {'attempts': 3}, 'a': {'attempts': 2}}),
                           ('connector_errors', [{'connector': 'synthetic', 'error': 'unavailable'}]), ('future-key', {'opaque': 10**100})]:
            tasks.db.execute('INSERT INTO pipeline_state VALUES(?,?)', (key, canonical(value)))
        tasks.db.execute('INSERT INTO input_asset_receipts VALUES(?,?)', ('["synthetic","request-test.json",null]', 7))
        tasks.db.execute('INSERT INTO control_events VALUES(?,?)', ('synthetic-event', canonical({'future': 10**100})))
        tasks.db.execute('INSERT INTO pipeline_sources VALUES(?,?,?,?,?)', ('result-1', 'i', '{}', 0, None))
    finally:
        tasks.close()
    fixtures.append(('states', root, path, work))
    # Rich digest inputs use the same work-file contract as control review-digest.
    root = folder / 'digest'; base(root)
    contacts = [{'id': 'scholar-a', 'name': 'Professor Σ', 'email': 'a@example.invalid',
                 'relationship': {'aliases': ['alias@example.invalid']}}]
    write(root / 'tables/contacts.json', {'contacts': contacts})
    work = {'reviews': [], 'note_coverage_overlap': [{'task_id': 'digest-1', 'covering_note_task_id': 'note-1'}]}
    for n, kind in enumerate(sorted(control.KINDS)):
        req = request(f'digest-{n}', kind=kind)
        req['owner'] = 'ALIAS@EXAMPLE.INVALID' if n % 2 else 'scholar-a'
        req['workflow_hash'] = 'b' * 64
        req['inputs'] = {'owner_ids': ['scholar-a', 'scholar-a'], 'result_id': 'result-π',
                         'note_round': 'uplift', 'submission_round': 'finalize',
                         'review_feedback': {'reason': '1. First point 2. Second point 2. Duplicate'}}
        result = {'outcome': 'hold' if n % 2 else 'completed', 'report': '  Synthetic\x1c multi-line. Second sentence',
                  'self_audit': [{'check': 'feedback 1', 'status': 'pass'}, {'check': 'other', 'status': 'fail'}],
                  'owner_brief': {'summary_zh': '  合成\n证明  '}, 'routing': {'mail': 'owner'},
                  'packet': {'attachments': [{}, {}]}, 'artifacts': [{}]}
        work['reviews'].append({'binding': {'task_id': req['id'], 'task_revision': n+1}, 'request': req,
                                'result': result, 'submission_review_requested': n % 2 == 0,
                                'workflow_binding': {'legacy_hash': 'b'*64, 'workflow_hash': control.workflow(kind)} if n % 2 else {}})
    fixtures.append(('digest', root, *build(root, [], work)))

    # Grouped capacities, borrowing, followup lanes and correspondence ancestry.
    root = folder / 'groups'; config = base(root)
    config.update(paused=False, mailbox_adapter='microsoft-graph')
    config['sandbox_pool'] = {'max_sandboxes': 4, 'agents_per_sandbox': 2,
        'prioritize_correspondence_notes': True,
        'groups': [{'name': 'correspondents', 'max_sandboxes': 2, 'kinds': ['reply', 'firstcontact']},
                   {'name': 'notes', 'max_sandboxes': 2, 'kinds': ['shortnote', 'mathesis', 'research', 'daily', 'submission']}]}
    write(root / 'config.json', config)
    contact = {'id': 'contact-a', 'name': 'Professor Ω', 'email': 'a@example.invalid',
               'problem_ids': ['problem-a'], 'relationship': {'aliases': ['alias@example.invalid']},
               'hub': {'events': [{'kind': 'sent', 'at': '2026-01-01', 'task_id': 'other'}],
                       'undeliverable_addresses': [{'email': 'a@example.invalid'}]}}
    write(root / 'tables/contacts.json', {'contacts': [contact]})
    write(root / 'tables/refusals.json', {'refusals': [{'id': 'stop', 'active': True,
          'emails': ['alias@example.invalid'], 'name': 'Professor Ω'}]})
    write(root / 'tables/decisions.json', {'firstcontact_excluded_regions': {'domain_suffixes': ['@example.invalid']}})
    rows = []
    for n, (kind, state, inputs, data) in enumerate([
        ('reply', 'queued', {'mailbox_observation': {'email': 'alias@example.invalid'}}, {}),
        ('shortnote', 'queued', {'result_id': 'problem-a'}, {}),
        ('mathesis', 'queued', {'reply_origin_task_id': 'group-0'}, {}),
        ('research', 'running', {}, {'pool_group': 'notes'}),
        ('shortnote', 'running', {}, {'pool_group': 'correspondents', 'pool_lane': 'correspondence'}),
        ('shortnote', 'queued', {'owner_ids': ['contact-b']}, {}),
        ('shortnote', 'approved', {'pipeline': {'scenario': 3}}, {}),
    ]):
        req = request(f'group-{n}', owner='contact-a', kind=kind); req['inputs'] = inputs
        rows.append({'id': req['id'], 'kind': kind, 'request': req, 'state': state, 'data': data})
    paper = {'id': 'synthetic-paper', 'title': 'A synthetic theorem', 'authors': ['Synthetic A'],
             'workspace': '../mail/synthetic/paper', 'version_date': '2026-11-01', 'stage': 'needs_work',
             'target_journal': 'Synthetic Journal', 'blocker': '', 'next_step_owner': 'operator'}
    write(root / 'tables/submissions.json', {'papers': [paper]})
    req = request('submission-pinned', kind='submission')
    req['inputs'] = {'paper': {**paper, 'authors': ['Synthetic B']},
                     'submission_source': {'paper_id': paper['id'], 'identity': digest({k: paper[k] for k in ('id', 'authors', 'workspace')})}}
    rows.append({'id': req['id'], 'kind': 'submission', 'request': req, 'state': 'running', 'data': {}})
    path, work = build(root, rows)
    tasks = Tasks(path)
    try:
        for n, result_id in enumerate(['problem-a', 'unregistered']):
            tasks.db.execute('INSERT INTO pipeline_sources VALUES(?,?,?,?,?)', (result_id, str(n), '{}', 0, 'group-6'))
    finally: tasks.close()
    fixtures.append(('groups', root, path, work))

    # Verdict boundaries, contiguous cleanup revisions and changed-result holds.
    root = folder / 'binding'; base(root)
    path, work = build(root, [])
    tasks = Tasks(path, clock=lambda: NOW)
    try:
        for n in range(4):
            req = request(f'binding-{n}', kind='reply'); row = tasks.add(req)
            result = {'outcome': 'hold' if n >= 2 else 'completed', 'report': 'Synthetic checked result', 'artifacts': []}
            state = 'held' if n >= 2 else 'awaiting-review'
            row = tasks.update(row['id'], row['revision'], state, {'result': result})
            for step in ['stopped', 'deleted']:
                row = tasks.update(row['id'], row['revision'], state, {**row['data'], 'cleanup': step})
            if n >= 2:
                binding = tasks.review_binding(row)
                row = tasks.verdict(sign(binding, verdict='HOLD'), 'secret')
                if n == 3:
                    row = tasks.update(row['id'], row['revision'], 'held', {**row['data'], 'result': {**result, 'report': 'Changed hold'}})
            if n == 1:
                row = tasks.update(row['id'], row['revision'], 'running', {})
                row = tasks.update(row['id'], row['revision'], 'awaiting-review', {'result': result, 'thread_inputs': {'future': 10**100}, 'mailbox_history': {'complete': False}})
        write(work, {'reviews': tasks.reviews()})
    finally: tasks.close()
    fixtures.append(('binding', root, path, work))
    # Hosted reviews qualify for the same publication exceptions as local reviews.
    root = folder / 'hosted-review'; base(root)
    write(root / 'tables/decisions.json', {'short_note_publication': {'enabled': True}})
    row = test_exceptions.passed('hosted-note')
    current = control.workflow('shortnote')
    row['request']['workflow_hash'] = current
    row['data']['review']['binding']['workflow_hash'] = current
    row['data']['review']['reviewer'] = 'hosted-cma-review'
    fixtures.append(('hosted-review', root, *build(root, [row], {'tasks':[row]})))
    # Reuse every source snapshot exercised by the production exceptions suite.
    # Capture before later test edits; retain only in this TemporaryDirectory.
    real_inventory = exceptions.inventory
    captured = []
    def capture(work, root):
        value = real_inventory(work, root)
        tables = {p.name: json.loads(p.read_text()) for p in (root / 'tables').glob('*.json')}
        captured.append((copy.deepcopy(work), tables))
        return value
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(test_exceptions.ExceptionsTests)
    with patch.object(exceptions, 'inventory', side_effect=capture):
        result = unittest.TextTestRunner(stream=io.StringIO()).run(suite)
    if not result.wasSuccessful():
        raise AssertionError(result.errors + result.failures)
    for n, (work, tables) in enumerate(captured):
        root = folder / f'exceptions-{n:02d}'; base(root)
        for name, value in tables.items(): write(root / 'tables' / name, value)
        # Convert the synthetic test hash sentinel to this run's live workflow.
        def hashes(value, kind='research'):
            if isinstance(value, dict):
                kind = value.get('kind', kind)
                if isinstance(value.get('request'), dict): kind = value['request'].get('kind', kind)
                return {k: control.workflow(kind) if k == 'workflow_hash' and v == 'current' else hashes(v, kind)
                        for k, v in value.items()}
            if isinstance(value, list): return [hashes(v, kind) for v in value]
            return value
        work = hashes(work)
        rows = exceptions.SnapshotTasks(work).all()
        fixtures.append((root.name, root, *build(root, rows, work)))
    return fixtures
