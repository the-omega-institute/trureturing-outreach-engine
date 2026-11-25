#!/usr/bin/env python3
"""Live Python signing and 13-family ingestion differential tests. Synthetic only."""
import copy
from contextlib import redirect_stderr
import hashlib
import base64
import io
import json
import os
from pathlib import Path
import random
import re
import subprocess
import sys
import shutil
import tempfile
from unittest.mock import patch
from ingestion import ENGINE, RUST, assets, build, compare, control, location, Tasks, canonical, digest, fingerprint, workflow_reader

from managed.tasks import STATES

KEY = 'public synthetic phase two key π'
NOW = 2_000_000_000.0
RESOURCE_BYTES = {('outreach-task-local', 'evidence.txt'): b'Synthetic local evidence'}


def sign(v):
    # Every plain family uses the production byte signer, without validator stubs.
    return control.sign_review(v, KEY, 'reply' if 'rebind_workflow_hash' in v else None)


def fixture(root):
    root.mkdir()
    (root / 'tables').mkdir()
    config = location.example_config()
    config['paused'] = True
    config['identity']['firstcontact_opening_terms'] = ['synthetic author', 'synthetic proof']
    config['identity']['firstcontact_signature'] = 'Synthetic Author\nSynthetic Test Affiliation'
    for name, value in {'config.json': config, 'tables/contacts.json': {'contacts': [
            {'id': 'scholar-a', 'name': 'Synthetic Professor', 'email': 'a@example.invalid', 'problem_ids': ['p1']}]},
            'tables/refusals.json': {'refusals': []}, 'tables/decisions.json': {}, 'tables/threads.json': {'lines': []},
            'tables/submissions.json': {'papers': []}, 'tables/results.json': {'results': []}}.items():
        (root / name).write_text(canonical(value))
    snapshot = root / 'snapshot.sqlite3'
    tasks = Tasks(snapshot, clock=lambda: NOW - 100)
    rows = {}

    def add(identity, kind, state, data):
        req = {'schema_version': 1, 'id': identity, 'kind': kind, 'owner': 'scholar-a',
               'source_revision': 'a' * 40, 'inputs': {}, 'workflow_hash': control.workflow(kind)}
        row = tasks.add(req)
        if state != 'queued' or data:
            row = tasks.update(identity, row['revision'], state, data)
        rows[identity] = row
        return row

    research = {'outcome': 'completed', 'report': 'Synthetic checked result', 'artifacts': [], 'packet': None}
    for state in sorted(STATES):
        add('state-' + state, 'research', state, {'result': research})
    row = add('review', 'research', 'awaiting-review', {'result': research, 'future_payload': {'at': 10**100}})
    review = {'binding': tasks.review_binding(row), 'reviewer': 'local-claude-loop', 'verdict': 'PASS',
              'reason': 'Synthetic checked proof π', 'evidence_ref': 'synthetic:proof'}
    q = {'id': 'q1', 'question': 'Synthetic choice?', 'source_quote': 'Actual synthetic question', 'evidence_ref': 'synthetic:incoming'}
    row = add('decision', 'reply', 'awaiting-user', {'result': {**research, 'routing': {'questions': [q]}}})
    decision = {'schema_version': 1, 'task_id': row['id'], 'result_hash': digest(row['data']['result']),
                'evidence_ref': 'synthetic:answer', 'reviewer': 'local-claude-loop', 'answers': {'q1': 'Synthetic actual answer'}}
    close = {k: v for k, v in decision.items() if k != 'answers'}
    close.update(action='close', reason='  The synthetic question is settled.  ')
    packet = {'schema_version': 1, 'kind': 'reply', 'sender': 'operator@example.invalid', 'to': ['a@example.invalid'],
              'cc': [], 'subject': 'Synthetic π', 'body': 'Dear Professor Synthetic,\n\nChecked result.\n\nBest regards,\nA. Author and B. Author',
              'problem_ids': [], 'attachments': [], 'thread_id': 'synthetic-thread',
              'latest_inbound_message_id': '<synthetic@example.invalid>', 'user_decision_ever': False}
    result = {'schema_version': 1, 'task_id': 'edited', **research, 'packet': packet}
    row = add('edited', 'reply', 'awaiting-review', {'result': result})
    edit = {**review, 'binding': tasks.review_binding(row), 'edited_body': packet['body'].replace('Checked result.', 'Checked replacement π.\r\nFurther detail.')}
    policy = json.loads((ENGINE / 'managed/firstcontact/workflow.json').read_text())
    ending = next(rule for step in policy['steps'] if step['id'] == 'compose' for rule in step['instructions'] if rule.startswith('Ending:'))
    paragraphs = re.search(r'Closing paragraph 1: (.+) Closing paragraph 2: (.+) No work-plan paragraph', ending)
    closing = '\n\n'.join([policy['closing_suffix_exact'], *(p.replace('[configured Mathesis URL]', config['mathesis']['base_url'].rstrip('/') + '/') for p in paragraphs.groups()),
                              policy['signature_exact'].replace('[configured firstcontact_signature]', config['identity']['firstcontact_signature'])])
    first_body = 'Dear Professor Synthetic,\n\n' + ' '.join(config['identity']['firstcontact_opening_terms'] + [
        'https://github.com/the-omega-institute/trureturing', 'https://www.trureturing.org/', 'ai orchestration', 'connect', 'lean']) + '\n\n' + closing
    first_packet = {k: v for k, v in packet.items() if k not in ('thread_id', 'latest_inbound_message_id', 'user_decision_ever')}
    first_packet.update(kind='firstcontact', problem_ids=['p1'], body=first_body)
    row = add('edited-firstcontact', 'firstcontact', 'awaiting-review', {'result': {**result, 'task_id': 'edited-firstcontact', 'packet': first_packet}})
    req = row['request']; req['inputs'] = {'recipient_emails': ['a@example.invalid'], 'to': ['a@example.invalid'], 'cc': []}
    tasks.db.execute('UPDATE tasks SET request=?,request_hash=? WHERE id=?', (canonical(req), digest(req), row['id']))
    first_edit = {**review, 'binding': tasks.review_binding(tasks.get(row['id'])), 'edited_body': first_body.replace('synthetic proof', 'synthetic proof with a checked extension')}
    rows['first_edit'] = first_edit
    held = add('agent-held', 'research', 'held', {'result': {**research, 'outcome': 'hold'}})
    rows['held_review'] = {**review, 'binding': tasks.review_binding(held)}
    parked = add('parked', 'research', 'held', {'result': {**research, 'outcome': 'hold'}})
    tasks.verdict(sign({**review, 'binding': tasks.review_binding(parked), 'verdict': 'HOLD'}), KEY)
    rows['parked_review'] = {**review, 'binding': tasks.review_binding(tasks.get('parked'))}
    for state in ['owner-draft', 'awaiting-mailbox', 'approved', 'handoff-ready']:
        row = add('rebind-' + state, 'reply', 'awaiting-review', {'result': {**result, 'task_id': 'rebind-' + state}})
        row = tasks.verdict(sign({**review, 'binding': tasks.review_binding(row)}), KEY)
        old = copy.deepcopy(row['request']);old['workflow_hash'] = 'b' * 64
        data = row['data'];data['review']['binding']['workflow_hash'] = 'b' * 64
        tasks.db.execute('UPDATE tasks SET request=?,request_hash=? WHERE id=?', (canonical(old), digest(old), row['id']))
        row = tasks.update(row['id'], row['revision'], state, data)
        binding = tasks.review_binding(row) if state in {'owner-draft', 'awaiting-mailbox'} else data['review']['binding']
        rows['rebind-' + state] = {**review, 'binding': binding, 'rebind_workflow_hash': control.workflow('reply')}
    old = {**result, 'task_id': 'draft'}
    row = add('draft', 'reply', 'owner-draft', {'result': old, 'draft_receipt': {'content_hash': digest(packet), 'remote_id': 'synthetic-draft'}})
    new = {**packet, 'body': packet['body'] + '\nSynthetic replacement.'}
    draft = {'schema_version': 1, 'task_id': row['id'], 'old_packet_hash': digest(packet), 'new_packet_hash': digest(new),
             'reason': 'Synthetic correction', 'packet': new}
    row = add('submission', 'submission', 'submission-ready', {'result': research, 'awaiting_operator_review': True})
    req = row['request']; req['inputs'] = {'paper': {'id': 'paper-synthetic'}, 'submission_round': 'finalize'}
    tasks.db.execute('UPDATE tasks SET request=?,request_hash=? WHERE id=?', (canonical(req), digest(req), row['id']))
    sub = {'schema_version': 1, 'paper_id': 'paper-synthetic', 'task_id': row['id'], 'result_hash': digest(research)}
    raw = RESOURCE_BYTES['outreach-task-local', 'evidence.txt']
    result = {'schema_version': 1, 'task_id': 'local', 'outcome': 'hold', 'report': 'Synthetic held result', 'artifacts': [
        {'release_tag': 'outreach-task-local', 'name': 'evidence.txt', 'role': 'evidence', 'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()}], 'packet': None}
    row = add('local', 'research', 'queued', {})
    local = {'schema_version': 1, 'task_id': row['id'], 'request_hash': row['request_hash'], 'task_revision': row['revision'],
             'executor': 'local-codex', 'session_id': 'synthetic-session', 'context': {}, 'result': result}
    row = add('prompt', 'research', 'failed', {'error_class': 'request-too-large'})
    prompt = {'task_id': row['id'], 'request_hash': row['request_hash']}
    row = add('failed', 'research', 'failed', {})
    failed = {'schema_version': 1, 'task_id': row['id'], 'request_hash': row['request_hash'], 'failed_revision': row['revision'], 'reason': 'Synthetic retry'}
    group = config['sandbox_pool']['groups'][0]['name']
    pool = {'schema_version': 1, 'sandbox_id': 'sbx_synthetic', 'workspace_id': 'wks_synthetic', 'group': group, 'reason': 'Synthetic adoption'}
    gap = {'statement': 'Synthetic lemma', 'note_locator': 'Lemma 1', 'source_doi': '10.1234/synthetic',
           'source_arxiv': None, 'source_url': None, 'source_locator': 'Theorem 2', 'why_it_matters': 'Synthetic obstruction'}
    note = {**research, 'formalization_gaps': [gap], 'value_assessment': {'verdict': 'substantial', 'reasons': 'Synthetic contribution'}}
    row = add('note', 'shortnote', 'awaiting-review', {'result': note})
    env = sign({**review, 'binding': tasks.review_binding(row)})
    tasks.verdict(env, KEY)
    # Backfill tests start from a legacy PASS without its pending-request field.
    row = tasks.get('note'); data = row['data']; data.pop('formalization_requests', None)
    tasks.update('note', row['revision'], 'approved', data)
    links = [{'task_id': 'note', 'result_hash': digest(note)}]
    formal = {'schema_version': 1, 'notes': links}
    prior = {'schema_version': 1, 'purpose': 'prior-work-backfill', 'notes': links}
    source = {'release_tag': 'outreach-task-mathesis', 'name': 'source.zip', 'sha256': hashlib.sha256(b'Synthetic source').hexdigest(), 'bytes': 16, 'role': 'source-archive'}
    math_result = {**research, 'artifacts': [source]}
    row = add('mathesis', 'mathesis', 'awaiting-review', {'result': math_result})
    tasks.verdict(sign({**review, 'binding': tasks.review_binding(row)}), KEY)
    metadata = {'schema': 'mathesis.manuscript-delivery.v1', 'producer': 'trureturing-outreach', 'work_id': 'synthetic-note', 'version': '1',
                'paper': {'authors': [{'name': 'Synthetic Author'}], 'purpose': 'review_only', 'processing_consent': True, 'ai_disclosure': 'Synthetic disclosure'}}
    mathesis = {'schema_version': 1, 'task_id': 'mathesis', 'result_hash': digest(math_result), 'workflow_hash': row['request']['workflow_hash'],
                'decision_ref': 'synthetic:consent', 'metadata': metadata, 'source': source, 'reviewer': 'local-claude-loop'}
    from managed.mathesis_delivery import signature
    mathesis['signature'] = signature(mathesis, KEY)
    req = {'schema_version': 1, 'id': 'new-request', 'kind': 'research', 'owner': 'scholar-a', 'source_revision': 'a' * 40,
           'inputs': {}, 'workflow_hash': control.workflow('research')}
    from managed.independent_review import reset_binding
    parked_review = {'phase':'needs_attention', 'cleanup_complete':True,
                     'create_key':'synthetic-review-create', 'last_error':'Invalid reviewer output'}
    reset_row = add('review-reset', 'reply', 'awaiting-review', {'result':research,
                    'hosted_review':parked_review, 'hosted_review_preparation':{'phase':'old-failure'}})
    reset_request = sign({'schema_version':1, 'task_id':reset_row['id'],
                          'reason':'Synthetic operator corrected reviewer profile',
                          **reset_binding(tasks, reset_row['id'])})
    envelopes = {'request': req, 'review': sign(review), 'decision': sign(decision), 'draft-revision': sign(draft),
                 'submission-review': sign(sub), 'local-result': sign(local), 'prompt-requeue': prompt, 'failed-requeue': sign(failed),
                 'pool-adopt': sign(pool), 'independent-review-reset':reset_request, 'formalization-backfill': sign(formal), 'prior-work-backfill': sign(prior), 'mathesis': mathesis}
    tasks.close()
    from managed.queue import Queue
    queue = Queue(snapshot, {'reply': control.workflow('reply')}, clock=lambda: NOW - 50)
    try:
        queue.add('queue-synthetic', packet)
        queue.db.execute('INSERT INTO policies VALUES(?,?,?)', ('reply', 1, json.dumps({'synthetic': True, 'future': {'π': 10**100}})))
    finally:
        queue.close()
    return snapshot, envelopes, review, close, edit, rows


def write_inputs(directory, values):
    directory.mkdir()
    for (tag, name), raw in RESOURCE_BYTES.items():
        target = directory / 'resources' / tag / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(raw)
    for n, (family, value) in enumerate(values):
        (directory / f'{family}-{n:04d}-{digest(value)}.json').write_text(canonical(value))


def sent_revision_snapshot(snapshot, path, review, envelope, *, repair, link):
    """Real note PASS + verified sent attachment, exercising both revision gates."""
    shutil.copyfile(snapshot, path)
    tasks = Tasks(path, clock=lambda: NOW - 20)
    try:
        result_id = 'synthetic-sent-revision'
        raw = b'Synthetic sent PDF revision'
        sha = hashlib.sha256(raw).hexdigest()
        selected = {'release_tag': 'outreach-task-sent-reply', 'name': 'note-v2.pdf', 'sha256': sha, 'bytes': len(raw), 'role': 'note-pdf'}
        req = {'schema_version': 1, 'id': 'note-head', 'kind': 'shortnote', 'owner': 'scholar-a', 'source_revision': 'a' * 40,
               'workflow_hash': control.workflow('shortnote'), 'inputs': {'result_id': result_id, 'note_round': 'uplift',
                   **({'selected_inputs': [selected]} if repair else {})}}
        row = tasks.add(req)
        artifacts = [{'release_tag': 'outreach-task-note-head', 'name': name, 'sha256': hashlib.sha256(data).hexdigest(), 'bytes': len(data), 'role': role}
                     for name, role, data in [('note.pdf', 'note-pdf', b'Synthetic base PDF'), ('note.tex', 'note-tex', b'Synthetic base TeX')]]
        result = {'schema_version': 1, 'task_id': 'note-head', 'outcome': 'completed', 'report': 'Synthetic checked note', 'packet': None,
                  'artifacts': artifacts, 'formalization_gaps': [], 'value_assessment': {'verdict': 'substantial', 'reasons': 'Synthetic extension'}}
        row = tasks.update(row['id'], row['revision'], 'awaiting-review', {'result': result})
        row = tasks.verdict(sign({**review, 'binding': tasks.review_binding(row)}), KEY)
        tasks.update(row['id'], row['revision'], 'handoff-ready', row['data'])
        packet = {'schema_version': 1, 'kind': 'reply', 'sender': 'operator@example.invalid', 'to': ['a@example.invalid'], 'cc': [],
                  'subject': 'Synthetic revision', 'body': 'Synthetic checked reply', 'problem_ids': [result_id],
                  'attachments': [{'name': 'note-v2.pdf', 'media_type': 'application/pdf', 'content_b64': base64.b64encode(raw).decode()}],
                  'thread_id': 'synthetic-thread', 'latest_inbound_message_id': '<synthetic@example.invalid>', 'user_decision_ever': False}
        reply_req = {**req, 'id': 'sent-reply', 'kind': 'reply', 'workflow_hash': control.workflow('reply'), 'inputs': {'result_id': result_id}}
        reply = tasks.add(reply_req)
        tasks.update(reply['id'], reply['revision'], 'verified', {'result': {'outcome': 'completed', 'packet': packet, 'artifacts': []},
                     'sent_receipt': {'remote_id': 'synthetic-sent', 'evidence_ref': 'synthetic:sent', 'content_hash': digest(packet), 'at': '2026-11-24T01:02:03Z'}})
        row = tasks.get('mathesis');req = row['request'];req['inputs'] = {'result_id': result_id}
        if link:
            req['inputs']['note_sources'] = [{'task_id': 'note-head', 'result_hash': digest(result)}]
        tasks.db.execute('UPDATE tasks SET request=?,request_hash=? WHERE id=?', (canonical(req), digest(req), row['id']))
        value = copy.deepcopy(envelope);value.pop('signature');value['metadata']['origin'] = {'problem_ids': [result_id]}
        from managed.mathesis_delivery import signature
        value['signature'] = signature(value, KEY)
        return value
    finally:
        tasks.close()


def supersession_snapshot(snapshot, path, before, after):
    shutil.copyfile(snapshot, path)
    tasks = Tasks(path)
    try:
        old = tasks.get('draft');request = old['request']
        request['inputs'] = {'thread_id': 'synthetic-thread', 'latest_inbound_message_id': '<synthetic@example.invalid>',
            'mailbox_observation': {'message_id': '<synthetic@example.invalid>', 'at': before}}
        tasks.db.execute('UPDATE tasks SET request=?,request_hash=? WHERE id=?', (canonical(request), digest(request), old['id']))
        return {**request, 'id': 'newer-inbound', 'inputs': {'thread_id': 'synthetic-thread', 'latest_inbound_message_id': '<newer@example.invalid>',
            'mailbox_observation': {'message_id': '<newer@example.invalid>', 'at': after}}}
    finally:
        tasks.close()


def main():
    build()
    rng = random.Random(20261124)
    cases = signed_files = rejected = replay_passes = 0
    with tempfile.TemporaryDirectory(prefix='signing-ingestion-parity-') as temp:
        folder = Path(temp)
        root = folder / 'data'
        snapshot, envelopes, review, close, edit, rows = fixture(root)
        scenarios = [(f'family-{k}', [(k, v)]) for k, v in envelopes.items()]
        reset_value = envelopes['independent-review-reset']
        scenarios.append(('reset-replay', [('independent-review-reset', reset_value)] * 2))
        for field, value in [('review_digest','0'*64), ('row_revision',True), ('row_revision',999), ('reason','')]:
            unsigned = {k:v for k,v in reset_value.items() if k != 'signature'}
            scenarios.append(('reset-changed-' + field + '-' + str(value), [
                ('independent-review-reset', sign({**unsigned, field:value}))]))
        scenarios.append(('reset-forged', [('independent-review-reset', {**reset_value, 'reason':'forged'})]))
        from managed.mathesis_delivery import signature as mathesis_signature
        for family in ('review', 'mathesis', 'decision', 'draft-revision'):
            for n, reviewer in enumerate(('hosted-cma-review', 'unknown-reviewer', None, [], {})):
                value = {k:v for k,v in envelopes[family].items() if k != 'signature'}
                value['reviewer'] = reviewer
                if family == 'mathesis':
                    value['signature'] = mathesis_signature(value, KEY)
                else:
                    value = sign(value)
                scenarios.append((f'reviewer-{family}-{n}', [(family, value)]))
        scenarios += [('close', [('decision', sign(close))]), ('edit', [('review', sign(edit))]),
                      ('firstcontact-edit', [('review', sign(rows['first_edit']))]),
                      ('all-families', list(envelopes.items())), ('duplicate-event', [('review', envelopes['review']), ('review', envelopes['review'])])]
        for name in ['held_review', 'parked_review']:
            for verdict in ['PASS', 'BOUNCE', 'HOLD']:
                scenarios.append((name + '-' + verdict, [('review', sign({**rows[name], 'verdict': verdict}))]))
        for state in ['owner-draft', 'awaiting-mailbox', 'approved', 'handoff-ready']:
            scenarios.append(('rebind-' + state, [('review', sign(rows['rebind-' + state]))]))
        for body in ['', None, 4, [], 'Synthetic — punctuation', 'Synthetic – punctuation']:
            for name, base in [('reply', edit), ('firstcontact', rows['first_edit'])]:
                unsigned = {**base, 'edited_body': body}
                # Invalid edit values cannot be signed by sign_review, but the
                # verifier must also refuse an authenticated malformed input.
                import hmac
                unsigned['signature'] = hmac.new(KEY.encode(), canonical(unsigned).encode(), hashlib.sha256).hexdigest()
                scenarios.append((f'bad-edit-{name}-{len(scenarios)}', [('review', unsigned)]))
        for verdict in ['BOUNCE', 'HOLD']:
            unsigned = {**edit, 'verdict': verdict}
            import hmac
            unsigned['signature'] = hmac.new(KEY.encode(), canonical(unsigned).encode(), hashlib.sha256).hexdigest()
            scenarios.append(('edit-' + verdict, [('review', unsigned)]))
        for n in range(240):
            family = rng.choice(list(envelopes))
            value = copy.deepcopy(envelopes[family])
            mutation = n % 9
            if mutation == 0 and 'signature' in value:
                value['signature'] = 'f' * 64
            elif mutation == 1:
                value['extra'] = rng.choice([None, True, 'synthetic', [1, 2]])
            elif mutation == 2 and family == 'review':
                value['binding']['task_revision'] += 1
                value = sign({k: v for k, v in value.items() if k != 'signature'})
            elif mutation == 3:
                value['schema_version'] = rng.choice([True, 1.0, 0, None])
            elif mutation == 4 and 'reason' in value:
                value['reason'] = rng.choice(['', ' \x1c\n ', None])
            elif mutation == 5 and 'signature' in value:
                value['signature'] = rng.choice([None, 7, [], '', 'A' * 64])
            elif mutation == 6 and 'task_id' in value:
                value['task_id'] = 'unknown-synthetic'
            elif mutation == 7 and family == 'review':
                value['edited_body'] = 'Synthetic edit'
            if mutation in (1, 3, 4, 6, 7) and 'signature' in value and family != 'mathesis':
                value = sign({k: v for k, v in value.items() if k != 'signature'})
            scenarios.append((f'random-{n}-{family}', [(family, value)]))
        for state in sorted(STATES):
            value = {**review, 'binding': {**review['binding'], 'task_id': 'state-' + state}}
            # Use real review_binding wherever accepted, including held results.
            tasks = Tasks(snapshot)
            try:
                try:
                    value['binding'] = tasks.review_binding(tasks.get('state-' + state))
                except ValueError:
                    pass
            finally:
                tasks.close()
            scenarios.append(('state-' + state, [('review', sign(value))]))
        for family in ['review', 'decision', 'draft-revision', 'local-result']:
            for field, values in [('signature', [None, [], {}, 9, 'π', 'A' * 64]), ('schema_version', [True, 1.0])]:
                for value in values:
                    v = copy.deepcopy(envelopes[family]);v[field] = value
                    scenarios.append((f'type-{family}-{field}-{len(scenarios)}', [(family, v)]))
        for binding in [None, [], {}, {'task_revision': 2}]:
            v = {**review, 'binding': binding}
            scenarios.append((f'bad-binding-{len(scenarios)}', [('review', sign(v))]))
        for label, values in scenarios:
            directory = folder / label
            write_inputs(directory, values)
            expected, actual = compare(snapshot, directory, root, KEY, passes=4, generation=7, now=NOW)
            if canonical(expected) != canonical(actual):
                # Synthetic diagnostics only. The private script never emits diffs.
                for key in expected:
                    if canonical(expected[key]) != canonical(actual[key]):
                        print('Mismatch', label, key)
                        if key == 'errors':
                            print('Python', expected[key], 'Rust', actual[key])
                        else:
                            for table in expected[key]:
                                if canonical(expected[key][table]) != canonical(actual[key].get(table)):
                                    print(table, 'Python', expected[key][table], 'Rust', actual[key].get(table))
                raise AssertionError(label)
            assert next(r for r in actual['tables']['tasks']['rows'] if r['id'] == 'review')['data']['future_payload']['at'] == 10**100
            cases += 1
            replay_passes += 4
            rejected += sum(len(e) for e in expected['errors'])
        no_key = folder / 'no-key';write_inputs(no_key, list(envelopes.items()))
        expected, actual = compare(snapshot, no_key, root, '', passes=4, generation=7, now=NOW)
        assert canonical(expected) == canonical(actual), 'no-key replay'
        cases += 1
        replay_passes += 4
        rejected += sum(len(e) for e in expected['errors'])
        empty = folder / 'empty-window';write_inputs(empty, [])
        expected, actual = compare(snapshot, empty, root, KEY, passes=1, generation=7, now=NOW)
        assert canonical(expected) == canonical(actual), 'empty window'
        cases += 1
        replay_passes += 1
        protected = fingerprint(snapshot)
        process = subprocess.run([str(RUST / 'target/debug/outreach'), 'control', 'replay-ingest', '--snapshot', str(snapshot),
            '--inputs', str(empty), '--root', str(root), '--output', str(snapshot), '--no-key'], capture_output=True)
        assert process.returncode not in (0, 101) and process.returncode > 0 and protected == fingerprint(snapshot)
        private = subprocess.run([sys.executable, str(RUST / 'replay_private_inputs.py'), str(snapshot), str(empty), '--root', str(root),
            '--generation', '7', '--now', str(NOW)], capture_output=True, env={**os.environ, 'OUTREACH_REVIEW_KEY': KEY})
        assert private.returncode == 0 and not private.stderr
        counts = json.loads(private.stdout)
        assert counts['mismatches'] == 0 and all(type(value) is int for value in counts.values())
        for repair, link in [(False, False), (True, False), (True, True)]:
            label = f'sent-revision-{int(repair)}-{int(link)}'
            derived = folder / (label + '.sqlite3')
            value = sent_revision_snapshot(snapshot, derived, review, envelopes['mathesis'], repair=repair, link=link)
            directory = folder / label;write_inputs(directory, [('mathesis', value)])
            expected, actual = compare(derived, directory, root, KEY, passes=4, generation=7, now=NOW)
            assert canonical(expected) == canonical(actual), label
            assert bool(expected['errors'][0]) == (not link), label
            cases += 1; replay_passes += 4; rejected += sum(len(e) for e in expected['errors'])
        for n, (before, after) in enumerate([
            ('2026-11-24T01:02:03.000001Z', '2026-11-24T01:02:03.000002Z'),
            ('20261124T010203.000001+0000', '2026-W48-2T01:02:03.000002+00:00'),
            ('2026-11-24T06:32:03.000001+05:30', '2026-11-24T01:02:03.000002Z')]):
            derived = folder / f'supersession-{n}.sqlite3'
            value = supersession_snapshot(snapshot, derived, before, after)
            directory = folder / f'supersession-{n}';write_inputs(directory, [('request', value)])
            expected, actual = compare(derived, directory, root, KEY, passes=4, generation=7, now=NOW)
            assert canonical(expected) == canonical(actual), f'supersession-{n}'
            assert next(r for r in actual['tables']['tasks']['rows'] if r['id'] == 'draft')['state'] == 'superseded'
            cases += 1; replay_passes += 4
        # Sign-review stdout files use Python's insertion order, indent=2,
        # ensure_ascii=False and final newline; stage files are canonical.
        with patch.object(control, 'workflow_text', side_effect=workflow_reader(root)):
            for n in range(240):
                unsigned = copy.deepcopy(rng.choice([review, close, edit, rows['first_edit'],
                    *[{k: v for k, v in value.items() if k != 'signature'} for family, value in envelopes.items() if family != 'mathesis']]))
                if 'reason' in unsigned:
                    unsigned['reason'] = 'Synthetic π ' + str(rng.randrange(1 << 80))
                kind = ('firstcontact' if unsigned.get('binding', {}).get('task_id') == 'edited-firstcontact' else 'reply') if 'edited_body' in unsigned else rng.choice([None, 'research', 'reply'])
                source = folder / 'unsigned.json'; destination = folder / 'signed.json'
                source.write_text(json.dumps(unsigned, ensure_ascii=False, indent=2))
                with redirect_stderr(io.StringIO()):
                    expected = control.sign_review(unsigned, KEY, kind)
                args = [str(RUST / 'target/debug/outreach'), 'control', 'sign-review', str(source), '--root', str(root), '--output', str(destination), '--overwrite']
                if kind:
                    args += ['--kind', kind]
                rust = subprocess.run(args, capture_output=True, env={**os.environ, 'OUTREACH_REVIEW_KEY': KEY})
                assert rust.returncode == 0, rust.stderr
                assert destination.read_bytes() == (json.dumps(expected, ensure_ascii=False, indent=2) + '\n').encode()
                signed_files += 1
            key_file = folder / 'synthetic.key';key_file.write_text('\x1c' + KEY + '\x1f\n')
            with patch.dict(os.environ, {'OUTREACH_REVIEW_KEY': '', 'OUTREACH_REVIEW_KEY_FILE': str(key_file)}):
                assert control.review_key() == KEY
            source.write_text(json.dumps(review, ensure_ascii=False))
            rust = subprocess.run([str(RUST / 'target/debug/outreach'), 'control', 'sign-review', str(source), '--key-file', str(key_file),
                '--output', str(destination), '--overwrite', '--root', str(root)], capture_output=True)
            assert rust.returncode == 0 and destination.read_bytes() == (json.dumps(control.sign_review(review, KEY), ensure_ascii=False, indent=2) + '\n').encode()
            signed_files += 1
        from ingestion_regressions import run
        run(snapshot, root, folder, envelopes, review, KEY, NOW)
    print(f'Signing/ingestion parity: {signed_files} byte-equal signed files; {cases} replay cases / {replay_passes} passes; {rejected} matching rejections; {len(envelopes)} input families; originals unchanged')


if __name__ == '__main__':
    main()
