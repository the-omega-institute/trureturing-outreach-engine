#!/usr/bin/env python3
"""Seeded differential validation against the unmodified live Python gates."""
import copy
import json
import random
import subprocess
from datetime import datetime, timedelta, timezone
from ingestion import RUST, control, canonical
from managed.tasks import validate_request
from managed.queue import validate_packet
from managed.note_closure import validate_gaps
from managed.mailbox import instant


def main():
    subprocess.run(['cargo', 'build', '-q', '--locked', '-p', 'outreach-core', '--example', 'validate_inputs'], cwd=RUST, check=True)
    rng = random.Random(2026112402)
    requests = [{'schema_version': 1, 'id': 'synthetic-π', 'kind': kind, 'owner': 'synthetic-scholar',
                 'source_revision': 'a' * 40, 'workflow_hash': 'b' * 64, 'inputs': {}}
                for kind in sorted(control.KINDS)]
    first = {'schema_version': 1, 'kind': 'firstcontact', 'sender': 'operator@example.invalid',
             'to': ['a@example.invalid'], 'cc': [], 'subject': 'Synthetic π', 'body': 'Synthetic body',
             'problem_ids': ['synthetic-p1'], 'attachments': []}
    packets = [first, {**first, 'kind': 'reply', 'problem_ids': [], 'thread_id': 'thread',
                      'latest_inbound_message_id': '<synthetic@example.invalid>', 'user_decision_ever': False},
               {'schema_version': 1, 'kind': 'daily', 'account': 'synthetic-account', 'covered_days': ['2026-01-01', '2026-01-02'],
                'text': 'Synthetic text', 'media': [], 'evidence_fingerprint': 'c' * 64}]
    cases = [('request', r) for r in requests] + [('packet', p) for p in packets]
    values = [None, True, False, 0, 1, 1.0, -1, 10**100, '', ' \x1c\n', 'synthetic', [], {}, ['synthetic'], ['a', 'a']]
    for family, bases in [('request', requests), ('packet', packets)]:
        for n in range(1800):
            v = copy.deepcopy(rng.choice(bases))
            k = rng.choice(list(v))
            if n % 8 == 0:
                del v[k]
            elif n % 8 == 1:
                v['extra'] = rng.choice(values)
            else:
                v[k] = rng.choice(values)
            cases.append((family, v))
    for cp in [0x345, 0xB2, 0x216B, 0x1C89, 0x10D50, 0x1E4D0, 0x31350, 0x1F600]:
        cases.append(('request', {**requests[0], 'id': 'id-' + chr(cp)}))
    for date in ['20260101', '2026-W01-4', '2026W014', '2026-W014', '2026W01-4', '9999-W52-7', '+001-01-01', '2026-+1-01', '2026-02-29', '2024-02-29', '2026-1-1', '2026-01-01']:
        cases.append(('packet', {**packets[2], 'covered_days': [date]}))
    for media in [{'name': 'synthetic.txt', 'media_type': 'text/plain', 'content_b64': b64}
                  for b64 in ['', 'Zg==', 'Zh==', 'Zm8=', 'Zm9=', 'abc', 'abc=', 'π', None, 1, []]]:
        cases.append(('packet', {**packets[0], 'attachments': [media]}))
    for field in ['note_sources', 'owner_ids', 'reply_origin_task_id', 'supersedes_owner_questions']:
        for value in values:
            cases.append(('request', {**requests[0], 'inputs': {field: value}}))
    for value in values:
        cases.append(('request', {**next(r for r in requests if r['kind'] == 'shortnote'), 'inputs': {'note_round': value}}))
    gap = {'statement': 'Synthetic statement', 'note_locator': 'Lemma 1', 'source_locator': 'Theorem 1', 'why_it_matters': 'Synthetic significance',
           'source_doi': None, 'source_arxiv': None, 'source_url': 'https://example.invalid/source'}
    for n in range(500):
        v = copy.deepcopy(gap);v[rng.choice(list(v))] = rng.choice(values);cases.append(('gaps', [v]))
    for url in ['HTTPS://example.invalid', 'https://example.invalid#x', 'https://#x', 'https://?x', 'https://:123', 'https://@example.invalid',
                'https://:@example.invalid', 'https://user@example.invalid', 'https://[::1]', 'https://[127.0.0.1]', 'https://[bad]',
                'https://[v1.future]', 'https://[vbad]', 'https://[::1%zone]', 'https://[::1%]', 'https://[bad', 'https://example.invalid:bad',
                '\x00https://example.invalid', 'https://example.invalid／secret', 'https://example.invalid\x1c/source']:
        cases.append(('gaps', [{**gap, 'source_url': url}]))
    stamps = ['20260101T120000Z', '2026-W01T12:00Z', '2026-W01-4T12Z', '2026W014T12Z', '2026-W014T12Z', '2026W01-4T12Z',
        '2026-01-01T12.5Z', '2026-01-01T12:30.5Z', '2026-01-01T12:00+00:99', '2026-01-01T12:00+01:99',
        '2026-01-01T12:00+00:00:00.5', '2026-01-01T12:00+00.5', '2026-01-01T12:00+01.5', '2026-01-01T12:00+24:00',
        '2026-01-01T12:00+00:59:99', '2026-01-01_12:00Z', '2026-01-01T00:00:00.000001Z', '2026-01-01T00:00:00.000002Z',
        '9999-W52-7T12Z', '0001-01-01T00:00:00+23:59', '9999-12-31T23:59:59-23:59']
    for n in range(300):
        day = datetime(rng.randrange(1, 10000), rng.randrange(1, 13), rng.randrange(1, 28), rng.randrange(24), rng.randrange(60), rng.randrange(60), rng.randrange(1000000),
                       timezone(timedelta(seconds=rng.randrange(-86399, 86400), microseconds=rng.randrange(1000000))))
        text = day.isoformat(sep=rng.choice(['T', ' ', '_', '🐍', '\x1c']), timespec=rng.choice(['hours', 'minutes', 'seconds', 'milliseconds', 'microseconds']))
        if n % 4 == 0:
            text = text[:10].replace('-', '') + text[10:].replace(':', '')
        elif n % 4 in (1, 2):
            y, week, weekday = day.isocalendar()
            prefix = f'{y:04}-W{week:02}-{weekday}' if n % 4 == 1 else f'{y:04}W{week:02}{weekday}'
            text = prefix + text[10:]
        stamps.append(text)
    cases.extend(('instant', stamp) for stamp in stamps)
    cases.extend(('instant', value) for value in values)
    expected = []
    for family, value in cases:
        if family == 'instant':
            try:
                delta = instant(value) - datetime(1970, 1, 1, tzinfo=timezone.utc)
                micros = (delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds
                expected.append({'ok': True, 'micros': micros})
            except (ValueError, TypeError, AttributeError, OverflowError):
                expected.append({'ok': False, 'micros': None})
            continue
        try:
            {'request': validate_request, 'packet': validate_packet, 'gaps': validate_gaps}[family](value)
            expected.append({'ok': True})
        except (ValueError, KeyError, TypeError) as exc:
            expected.append({'ok': False, 'error': str(exc)})
    process = subprocess.run([str(RUST / 'target/debug/examples/validate_inputs')],
                             input=''.join(canonical({'family': family, 'value': value}) + '\n' for family, value in cases),
                             capture_output=True, text=True)
    assert process.returncode == 0, f'Validator panic/failure: {process.returncode}'
    actual = [json.loads(line) for line in process.stdout.splitlines()]
    assert len(actual) == len(expected)
    failures = [(n, cases[n], a, b) for n, (a, b) in enumerate(zip(expected, actual)) if canonical(a) != canonical(b)]
    for failure in failures[:12]:
        print('Synthetic mismatch:', failure)
    assert not failures, f'{len(failures)} validator mismatches'
    print(f'Input validators: {len(cases)} live Python comparisons; {sum(not r["ok"] for r in expected)} identical rejections; no panics')


if __name__ == '__main__':
    main()
