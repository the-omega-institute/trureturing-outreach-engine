#!/usr/bin/env python3
"""Regenerate public parity fixtures from the live Python engine, offline.

All inputs are synthetic. No configuration, ledger, data checkout, credentials,
network, or live workflow text is read. Live source parity is checked separately.
"""
import argparse
import copy
from contextlib import nullcontext
from datetime import datetime, timezone
import hmac
import json
import math
from pathlib import Path
import random
import struct
import sys
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

# Prevent location.resolve from consulting a configured data checkout, including
# when this generator is run from a production shell. Only the public example
# config may be read while importing these modules.
with patch.dict('os.environ', {'OUTREACH_DATA_ROOT': str(ROOT / 'rust/vectors/nonexistent'),
                              'GITHUB_ACTIONS': 'false'}):
    from managed import control, mathesis_delivery, pipeline, registry, sent_events
    from managed.queue import Queue, canonical, delivery_tokens, digest, workflow_digest
    from managed.tasks import Tasks

SEED = 0xC4A04E20261123
# This contract is CPython's default, independent of a developer shell override.
sys.set_int_max_str_digits(4300)
KEY = 'synthetic-test-key-only-π'
KINDS = sorted(control.KINDS)
PATHS = [
    'automation.json', 'runtime.py', 'replies/voice.md',
    'firstcontact/workflow.json', 'firstcontact/review.md',
    'replies/workflow.json', 'replies/review.md',
    'daily/workflow.json', 'daily/DESIGN-notes.md',
    'research/workflow.json', 'research/review.md',
    'submission/workflow.json', 'submission/review.md', 'submission/author-policy.json',
]


def input_json(value):
    return json.dumps(value, ensure_ascii=True, separators=(',', ':'), allow_nan=False)


def random_float(rng):
    while True:
        value = struct.unpack('>d', rng.getrandbits(64).to_bytes(8, 'big'))[0]
        if math.isfinite(value):
            return value


def random_value(rng, depth=0):
    choice = rng.randrange(6 if depth >= 4 else 8)
    if choice == 0:
        return None
    if choice == 1:
        return bool(rng.getrandbits(1))
    if choice == 2:
        return rng.getrandbits(rng.choice([1, 53, 64, 128, 512, 2048])) * rng.choice([-1, 1])
    if choice == 3:
        return random_float(rng)
    if choice == 4:
        return ''.join(rng.choice(['a', 'Ω', '中', 'é', 'e\u0301', '😀', '\U0010ffff',
                                   '\x00', '\x1f', '\n', '\t', '"', '\\', '/', '\u2028',
                                   '\ue000', '\ufeff', '\u2029', '\x7f'])
                       for _ in range(rng.randrange(16)))
    if choice == 5:
        return rng.randrange(-100000, 100000) / rng.choice([1.0, 1e-10, 1e10])
    if choice == 6:
        return [random_value(rng, depth + 1) for _ in range(rng.randrange(5))]
    return {str(i) + rng.choice(['z', 'é', 'Ω', '😀', '\ue000']): random_value(rng, depth + 1)
            for i in range(rng.randrange(5))}


def canonical_vectors():
    rng = random.Random(SEED)
    directed = [None, True, False, 0, -1, 2**64, -(2**256), 10**1000,
                0.0, -0.0, 1.0, 1e16, 1e15, 1e-4, 1e-5, 1e23,
                ''.join(chr(i) for i in range(128)), 'é e\u0301 Ω 中 😀 \u2028\u2029',
                {'😀': 1, '\ue000': 2, 'a': 3, '\uffff': 4, '\U0010ffff': 5},
                {'$serde_json::private::Number': '123'},
                {'$serde_json::private::Number': 'not a number', 'a': [1e16]}, [], {}]
    values = directed + [random_value(rng) for _ in range(4096)]
    inputs = [input_json(v) for v in values]
    inputs += ['-0', ' { "z":1,"a":2,"z":3 } ', '1.00000000000000011102230246251565404236316680908203125',
               '2.2250738585072012e-308', '1e-9999', '-1e-9999', '123456789012345678901234567890.0',
               '{"$serde_json::private::Number":"1","$serde_json::private::Number":"2"}']
    inputs += ['9' * 4300, '-' + '9' * 4300, '[' * 512 + '0' + ']' * 512]
    # Exercise actual UTF-8 input bytes, including astral keys, independently of
    # the usual ensure_ascii=True input transport. Rejections below combine raw
    # UTF-8 with escaped lone surrogates too.
    inputs += [json.dumps(v, ensure_ascii=False) for v in [
        'Ω 中 😀 𐐀 \u2028\u2029', {'😀': 1, '\ue000': 2, '𐐀': 3, '\U0010ffff': 4},
        {'raw 😀': ['é', 'e\u0301', '\\ud800', '\x00\x1f']}]]
    return [{'input': raw, 'canonical': canonical(json.loads(raw)), 'digest': digest(json.loads(raw))}
            for raw in inputs]


def float_vectors():
    rng = random.Random(SEED + 1)
    values = [0.0, -0.0, 1.0, -1.0, 1e16, 1e-5, sys.float_info.max,
              sys.float_info.min, math.ulp(0.0)]
    # Decimal notation boundaries and adjacent representable values, across the
    # entire finite exponent range. Exact halfway cases exercise tie-to-even.
    for exponent in range(-323, 309):
        value = float('1e' + str(exponent))
        for neighbor in [math.nextafter(value, 0.0), value, math.nextafter(value, math.inf)]:
            if math.isfinite(neighbor):
                values.extend([neighbor, -neighbor])
    for integer in range(1, 2048):
        values.append(integer + 0.5)
    values += [random_float(rng) for _ in range(10000)]
    return [{'bits': struct.pack('>d', v).hex(), 'canonical': canonical(v)} for v in values]


def signature(unsigned, family, key=KEY):
    if family == 'mathesis-delivery':
        return mathesis_delivery.signature(unsigned, key)
    # The generic byte signer is real production code. Bypass only its
    # review-specific edit/rebind prechecks to sign intentionally invalid test
    # envelopes; reference_gate independently exercises every real field gate.
    with patch.object(control, 'validate_review_edit'), patch.object(control, 'validate_review_rebind'), \
         patch.object(control, 'workflow', return_value=unsigned.get('rebind_workflow_hash')):
        return control.sign_review(unsigned, key, kind='reply')['signature']


class AtSignature(Exception):
    pass


def reference_gate(signed, family, key):
    """Observe the real Python pre-signature field checks and comparison.

    Stop at compare_digest, before a ledger or semantic admission can run. This
    deliberately tests the cryptographic gate, not permission to apply an effect.
    """
    comparison = []
    compare = hmac.compare_digest

    def intercept(expected, supplied):
        # Each family's inline verifier must compute the same bytes as the
        # production signer, before we stop execution ahead of ledger access.
        unsigned = {k: v for k, v in signed.items() if k != 'signature'}
        assert expected == signature(unsigned, family, key)
        comparison.append(compare(expected, supplied))
        raise AtSignature()

    try:
        with patch('hmac.compare_digest', side_effect=intercept):
            if family == 'review':
                Tasks.verdict(None, signed, key)
            elif family == 'decision':
                Tasks.decision(None, signed, key)
            elif family == 'pool-adopt':
                control.adopt_pool_sandbox(None, signed, key)
            elif family == 'failed-requeue':
                control.requeue_failed_task(None, signed, key)
            elif family == 'draft-revision':
                Tasks.draft_revision(None, signed, key)
            elif family == 'submission-review':
                Tasks.request_submission_review(None, signed, key)
            elif family == 'mathesis-delivery':
                mathesis_delivery.validate(signed, key)
            else:
                raise AssertionError(family)
    except AtSignature:
        return comparison[0]
    except (ValueError, TypeError, KeyError):
        return False
    raise AssertionError('Signature gate was not reached')


def envelope_vectors():
    signed_vectors, verification = [], []
    for i in range(24):
        task = f'synthetic-task-{i}'
        result_hash = digest({'synthetic_result': i})
        binding = {'task_id': task, 'task_revision': i + 1, 'result_hash': result_hash,
                   'workflow_hash': digest(['workflow', i])}
        reason = f'Checked synthetic evidence {i}: Ω 中 😀\n'
        packet = {'to': ['synthetic@example.invalid'], 'body': reason, 'weight': 1e16}
        bases = {
            'review': {'binding': binding, 'reviewer': 'local-claude-loop',
                       'verdict': ['PASS', 'BOUNCE', 'HOLD'][i % 3], 'reason': reason, 'evidence_ref': 'fixture:review'},
            'decision': {'schema_version': 1, 'task_id': task, 'result_hash': result_hash,
                         'answers': {'q1': reason}, 'reviewer': 'local-claude-loop', 'evidence_ref': 'fixture:decision'},
            'pool-adopt': {'schema_version': 1, 'sandbox_id': f'sbx_synthetic{i}',
                           'workspace_id': f'wks_synthetic{i}', 'group': 'synthetic', 'reason': reason},
            'failed-requeue': {'schema_version': 1, 'task_id': task, 'request_hash': digest(['request', i]),
                              'failed_revision': i + 1, 'reason': reason},
            'draft-revision': {'schema_version': 1, 'task_id': task, 'old_packet_hash': digest(['old', i]),
                               'new_packet_hash': digest(packet), 'reason': reason, 'packet': packet,
                               **({'reviewer': 'local-claude-loop'} if i % 2 else {})},
            'submission-review': {'schema_version': 1, 'paper_id': f'synthetic-paper-{i}',
                                   'task_id': task, 'result_hash': result_hash},
            'mathesis-delivery': {'schema_version': 1, 'task_id': task, 'result_hash': result_hash,
                                 'workflow_hash': binding['workflow_hash'], 'decision_ref': 'fixture:decision',
                                 'metadata': {'schema': 'mathesis.pre-reviewed-delivery.v1' if i % 2 else 'synthetic.v1'},
                                 'source': {'name': 'synthetic.tex', 'bytes': 42}, 'reviewer': 'local-claude-loop',
                                 **({'pdf': {'name': 'synthetic.pdf', 'bytes': 84}} if i % 2 else {})},
        }
        if i % 3 == 0:
            bases['review'].update(rebind_workflow_hash=digest(['rebind', i]), edited_body=reason)
        for family, unsigned in bases.items():
            signed = {**unsigned, 'signature': signature(unsigned, family)}
            signed_vectors.append({'family': family, 'key': KEY, 'unsigned': input_json(unsigned),
                                   'signed': canonical(signed), 'event_digest': digest(signed)})
            variants = [signed,
                        {**signed, 'signature': '0' * 64},
                        {**signed, 'signature': signed['signature'].upper()},
                        {**signed, 'signature': signed['signature'][:-1]},
                        {**signed, 'signature': signed['signature'] + '0'},
                        {**signed, 'signature': 'é' * 64},
                        {**signed, 'signature': None}, {**signed, 'signature': 5},
                        {k: v for k, v in signed.items() if k != 'signature'},
                        {**signed, 'synthetic_extra': 'also signed'},
                        {**signed, 'reviewer': 'changed'},
                        {k: v for k, v in signed.items() if k != next(iter(unsigned))}]
            # An extra field signed correctly must pass raw cryptography and fail
            # the engine's exact field set. Mutation of any signed field fails.
            extra = {**unsigned, 'synthetic_extra': 'also signed'}
            variants.append({**extra, 'signature': signature(extra, family)})
            for schema in [True, 1.0, 2]:
                if 'schema_version' in unsigned:
                    variant = {**unsigned, 'schema_version': schema}
                    variants.append({**variant, 'signature': signature(variant, family)})
            for variant, test_key in [(v, KEY) for v in variants] + [(signed, ''), (signed, 'wrong-key')]:
                supplied = variant.get('signature')
                raw_valid = bool(test_key) and isinstance(supplied, str) and supplied == signature(
                    {k: v for k, v in variant.items() if k != 'signature'}, family, test_key)
                verification.append({'family': family, 'key': test_key, 'signed': input_json(variant),
                                     'raw_valid': raw_valid, 'typed_valid': reference_gate(variant, family, test_key)})
    return signed_vectors, verification


def review_vectors():
    base = {'binding': {'task_id': 'synthetic-review', 'task_revision': 1,
                        'result_hash': 'a' * 64, 'workflow_hash': 'b' * 64},
            'reviewer': 'local-claude-loop', 'verdict': 'PASS',
            'reason': 'Synthetic checked result', 'evidence_ref': 'fixture:review'}
    vectors = []
    changes = [{}, {'edited_body': 'Synthetic edited body Ω\n'}, {'edited_body': '\x1c\x1f'},
               {'edited_body': ''}, {'edited_body': 1}, {'signature': 'already-signed'},
               {'verdict': 'BOUNCE'}, {'verdict': 'HOLD', 'edited_body': 'body'},
               {'rebind_workflow_hash': 'c' * 64}, {'rebind_workflow_hash': 'd' * 64},
               {'rebind_workflow_hash': 'invalid'}, {'verdict': 'HOLD', 'rebind_workflow_hash': 'c' * 64}]
    for kind in [None, *KINDS, 'unknown']:
        for current in ['b' * 64, 'c' * 64]:
            for change in changes:
                unsigned = {**base, **change}
                try:
                    with patch.object(control, 'workflow', return_value=current), patch('sys.stderr'):
                        signed = control.sign_review(unsigned, KEY, kind=kind)
                    expected, error = canonical(signed), None
                except ValueError as exc:
                    expected, error = None, str(exc)
                vectors.append({'kind': kind, 'current_hash': current, 'key': KEY,
                                'unsigned': input_json(unsigned), 'signed': expected, 'error': error})
    return vectors


def identity_vectors():
    vectors = []

    def add(operation, args, expected):
        vectors.append({'operation': operation, 'args': input_json(args), 'expected': expected})

    ids = [None, '', '0042', 42, 2**128, True, False, 1e16, -0.0, 'Ω-😀']
    for i in range(64):
        asset = {'name': f'request-synthetic-{i % 4}.json'}
        if i % 11:
            asset['id'] = ids[i % len(ids)]
        if i % 3:
            asset['digest'] = None if i % 5 == 0 else 'sha256:' + digest(['asset', i % 4])
        add('input_identity', asset, control.input_identity(asset))
        value = {'synthetic': i, 'float': 1e16, 'unicode': 'Ω😀', 'big': 2**128 + i}
        add('attempt_key', [f'item-{i}', i + 1, digest(value)],
            reference_attempt_key(f'item-{i}', i + 1, digest(value)))
        add('registry_identity', ['synthetic', canonical(value)], registry.identity('synthetic', canonical(value)))
        with patch.object(control, 'current_policy_revision', return_value=None), patch.object(control, 'request_binding', return_value={}):
            request = pipeline.request('research', value, 'synthetic-owner', {}, 'a' * 40)
        add('request_id', ['research', value], request['id'])
        class Stage:
            @staticmethod
            def ensure(): pass
            @staticmethod
            def upload(path): pass
        # No temporary files: the production name is prefix + digest + .json.
        # stage() also performs I/O, so only capture its name through a mocked Path.
        with patch.object(control, 'Path') as path:
            target = path.return_value.__truediv__.return_value
            target.name = 'synthetic-' + digest(value) + '.json'
            staged = control.stage(value, 'synthetic', Stage(), 'unused')
            name = path.return_value.__truediv__.call_args.args[0]
            assert staged['sha256'] == digest(value)
        add('staged_asset_name', ['synthetic', value], name)
        artifact = {'release_tag': 'synthetic-release', 'name': f'file-{i}.txt', 'bytes': 2**64 + i, 'sha256': digest(value)}
        repo = None if i % 2 else 'synthetic/repository'
        add('artifact_key', [repo, artifact], control.artifact_key(SimpleNamespace(repo=repo), artifact))
        row = {'id': f'synthetic-{i}', 'upstream': {'repository': 'synthetic/repository',
               'resolution_kind': 'proved', 'declaration_gids': ['Ω', 'z', 'a', 'a', '😀']}}
        add('source_identity', row, pipeline.source_identity(row))
        add('workflow_digest', [value, {'shared_review_contract': 'Synthetic rubric\r\n'}], workflow_digest(value, {'shared_review_contract': 'Synthetic rubric\r\n'}))
    for packet in [
        {'kind': 'reply', 'thread_id': 'synthetic-thread', 'latest_inbound_message_id': 'synthetic-inbound'},
        {'kind': 'firstcontact', 'to': ['a@example.invalid', 'b@example.invalid'], 'cc': ['c@example.invalid'], 'problem_ids': ['synthetic-1', 'synthetic-2']},
        {'kind': 'daily', 'account': 'synthetic-account', 'covered_days': ['2026-01-01', '2026-01-02']},
        {'kind': 'firstcontact', 'to': [], 'cc': [], 'problem_ids': []},
    ]:
        add('delivery_tokens', packet, delivery_tokens(packet))
    return vectors


def reference_attempt_key(item, version, content_hash):
    """Run the inline Queue.dispatch formula; stop before any transport call.

    This import-only fake queue has no database, file or network access. It
    observes the actual INSERT arguments and stops at the intent checkpoint.
    """
    captured = []

    class AtCheckpoint(Exception):
        pass

    def execute(sql, args):
        if sql.startswith('INSERT INTO attempts'):
            captured.append(args[1])
        row = {'envelope': canonical({'verdict': 'PASS'})} if sql.startswith('SELECT envelope') else None
        return SimpleNamespace(fetchone=lambda: row)

    def checkpoint():
        raise AtCheckpoint()

    row = {'id': item, 'version': version, 'content_hash': content_hash,
           'state': 'approved', 'kind': 'reply', 'packet': {
               'kind': 'reply', 'thread_id': 'synthetic', 'latest_inbound_message_id': 'synthetic'}}
    queue = SimpleNamespace(_tx=nullcontext, _bound=lambda _: row, binding=lambda _: None,
                            _policy=lambda *_: 1, db=SimpleNamespace(execute=execute),
                            _event=lambda *_: None, checkpoint=checkpoint)
    try:
        Queue.dispatch(queue, item, SimpleNamespace(kind='reply', name='synthetic'), SimpleNamespace())
    except AtCheckpoint:
        pass
    assert len(captured) == 1
    return captured[0]


def event_vectors():
    """Identity bytes from both actual registry and Sent Items event builders."""
    vectors = []
    for i in range(24):
        for family in ['registry', 'sent-events']:
            address = 'synthetic@example.invalid'
            row = {'id': 'synthetic-contact', 'email': address}
            person = {'emails': [address], 'records': [row]}
            contacts = {'contacts': [row]}
            with patch.object(registry, 'read', return_value=contacts), \
                 patch.object(registry, 'people', return_value=[person]), \
                 patch.object(registry, 'write'):
                at = f'2026-01-{i + 1:02d}T12:00:00Z'
                summary = f'Synthetic result {i}: Ω 中 😀\n"checked"'
                if family == 'registry':
                    event = registry.record_event(address, 'status', at, summary, 'fixture:synthetic', root=ROOT)
                else:
                    message = {'id': f'synthetic-{i}', 'internetMessageId': f'<synthetic-{i}@example.invalid>',
                               'conversationId': 'synthetic-conversation', 'sentDateTime': at,
                               'subject': summary, 'toRecipients': [{'emailAddress': {'address': address}}]}
                    count = sent_events.record_page(ROOT, [message], [], set(),
                        datetime(2026, 1, 1, tzinfo=timezone.utc), datetime(2026, 2, 1, tzinfo=timezone.utc))
                    assert count == 1
                    event = row['hub']['events'][0]
            unsigned = {k: v for k, v in event.items() if k != 'id'}
            vectors.append({'family': family, 'input': input_json(unsigned), 'id': event['id'],
                            'serialized': json.dumps(unsigned, sort_keys=True, ensure_ascii=False)})
    return vectors


def serialization_vectors():
    values = [{'😀': [1e16, -0.0, None, True, {}, []], 'a': 'Ω\x00\x7f\n'}, [], {},
              ['中', {'x': [1, 2]}]]
    vectors = []
    for value in values:
        for ensure_ascii in [False, True]:
            for sort_keys in [False, True]:
                for allow_nan in [False, True]:
                    for indent in [None, '', '  ', '\t']:
                        for separators in [None, (',', ':'), (', ', ': ')]:
                            # Until phase 2 retains input order, unsorted parity
                            # is limited to dictionaries already in stored order.
                            ordered = json.loads(canonical(value))
                            options = dict(ensure_ascii=ensure_ascii, sort_keys=sort_keys,
                                           allow_nan=allow_nan, indent=indent, separators=separators)
                            vectors.append({'input': input_json(ordered), 'options': options,
                                            'expected': json.dumps(ordered, **options)})
    return vectors


def workflow_vectors():
    synthetic = {p: f'Synthetic {p}\r\nΩ 😀\n' for p in PATHS}
    automation = {'schema_version': 1, 'routing_contract': {'route': 'synthetic'},
                  'continuation': [1e16, 2**128], 'mail': {'mode': 'synthetic'},
                  'publication': {'daily': {'mode': 'synthetic-daily'}, 'coverage': [True], 'public_updates': [False]},
                  'ignored': 'excluded from every hash'}
    synthetic['automation.json'] = input_json(automation)
    vectors = []

    def add(profile, kind, overrides=None, missing=(), historical=False):
        files = {**synthetic, **(overrides or {})}
        for path in missing:
            files.pop(path, None)

        def read(path, *_):
            if path not in files:
                raise FileNotFoundError(path)
            return files[path]

        try:
            with patch.object(control, 'workflow_text', side_effect=read):
                expected = control.workflow(kind, 'a' * 40 if historical else None)
            error = False
        except (KeyError, ValueError, FileNotFoundError):
            expected, error = None, True
        vectors.append({'profile': profile, 'kind': kind, 'overrides': overrides or {},
                        'missing': list(missing), 'historical': historical, 'digest': expected, 'error': error})

    for kind in KINDS + ['unknown']:
        add('synthetic', kind)
        for path in PATHS:
            if path != 'automation.json':
                add('synthetic', kind, {path: synthetic[path] + '\nchanged synthetic input\n'})
            add('synthetic', kind, missing=[path])
        for key in automation:
            changed = copy.deepcopy(automation)
            if key == 'publication':
                for part in automation[key]:
                    sub = copy.deepcopy(automation)
                    sub[key][part] = ['changed']
                    add('synthetic', kind, {'automation.json': input_json(sub)})
            else:
                changed[key] = ['changed']
                add('synthetic', kind, {'automation.json': input_json(changed)})
        # Reformatting automation leaves the selected JSON unchanged; reformatting
        # workflow.json changes the hash because its raw text is the contract.
        add('synthetic', kind, {'automation.json': json.dumps(automation, ensure_ascii=False, indent=2)})
    for kind in ['reply', 'firstcontact']:
        add('synthetic', kind, missing=['replies/voice.md'], historical=True)
    return {'file_sets': {'synthetic': synthetic},
            'vectors': vectors}


def generate():
    envelopes, verification = envelope_vectors()
    rejected = ['NaN', 'Infinity', '-Infinity', '1e400', '-1e400', '"\\ud800"',
                '"\\udfff"', '{"\\ud800":1}', '["\\ud800"]', '"\\ud800x"',
                '"\\udc00\\ud800"']
    rejected += ['9' * 4301, '-' + '9' * 4301, '{"a":' + '9' * 4301 + ',"a":1}',
                 '"raw 😀 \\ud800"', '{"😀":"\\udfff"}']
    for raw in rejected:
        try:
            digest(json.loads(raw))
        except (ValueError, UnicodeEncodeError):
            continue
        raise AssertionError('Python unexpectedly accepted ' + raw)
    families = {'canonical': canonical_vectors(), 'floats': float_vectors(),
                'envelopes': envelopes, 'verification': verification, 'reviews': review_vectors(),
                'identities': identity_vectors(), 'rejections': [{'input': raw} for raw in rejected],
                'events': event_vectors(), 'serialization': serialization_vectors(),
                # Python accepts this input; the explicit Rust resource bound
                # is a deliberate divergence, not a Python rejection oracle.
                'depth_limits': [{'input': '[' * 513 + '0' + ']' * 513,
                                  'python_digest': digest(json.loads('[' * 513 + '0' + ']' * 513))}]}
    result = {name + '.json': {'schema_version': 1, 'seed': SEED, 'vectors': vectors}
              for name, vectors in families.items()}
    result['workflows.json'] = {'schema_version': 1, **workflow_vectors()}
    return {name: (json.dumps(value, ensure_ascii=True, indent=2, allow_nan=False) + '\n').encode()
            for name, value in result.items()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true', help='compare without writing')
    parser.add_argument('--output', type=Path, default=Path(__file__).resolve().parent)
    args = parser.parse_args()
    generated = generate()
    for name, raw in generated.items():
        path = args.output / name
        if args.check:
            if not path.is_file() or path.read_bytes() != raw:
                raise SystemExit('Vectors are stale: ' + name)
        else:
            args.output.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)
        print(f'{name}: {len(json.loads(raw)["vectors"])} vectors')
    existing = {p.name for p in args.output.glob('*.json')}
    if existing != set(generated):
        raise SystemExit('Unexpected vector files: ' + ', '.join(sorted(existing - set(generated))))


if __name__ == '__main__':
    main()
