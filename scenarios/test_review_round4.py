"""Ancestry-free errors captured in Python before the round-four Rust fix.

Refresh only these fixtures: python3.12 scenarios/test_review_round4.py --extract
"""
import base64
from http.client import IncompleteRead
import json
import subprocess
import sys
import unittest

import runner
from test_review_regressions import BINARY, CASES, ROOT, source

TEST = 'test_review_round4.ReviewRound4.test_python_first_cases'


def named(case, name):
    case['id'] = case['stage'] + '-review-round4-' + name
    case['source_test'] = TEST
    return case


def candidates():
    partial = b'synthetic partial read'
    for error in (
        {'type': 'RemoteDisconnected', 'message': 'Synthetic remote disconnect'},
        {'type': 'IncompleteRead', 'message': str(IncompleteRead(partial, 17)),
         'partial': base64.b64encode(partial).decode(), 'expected': 17},
    ):
        case = source('intake-*unregistered_alias_linked_to_sent*-1')
        entry = case['script'][3]
        assert entry['method'] == 'match_inbound_sent'
        entry.pop('result')
        entry['error'] = error
        yield named(case, 'match-sent-no-bases-' + error['type'])

    for pattern, name in (
        ('delivery-*uncertain_owner_draft_lookup_errors_never_establish_absence-1', 'folder-lookup-no-bases-unknown'),
        ('pool-*unobserved_peer_does_not_make_the_last_sandbox_surplus-2', 'health-no-bases-unknown'),
    ):
        case = source(pattern)
        entry = next(e for e in case['script'] if e.get('error', {}).get('type') == 'TimeoutError')
        entry['error'] = {'type': 'SyntheticOrdinaryError', 'message': entry['error']['message']}
        yield named(case, name)


def capture(case):
    case.update(runner.execute(case, capture=True))
    assert case.pop('error') is None, case['id']
    runner.execute(case)
    return case


def extract():
    mapping = json.loads((ROOT / 'scenarios/test-map.json').read_text())
    ids = []
    for case in candidates():
        capture(case)
        (CASES / (case['id'] + '.json')).write_text(json.dumps(case, ensure_ascii=False, separators=(',', ':')) + '\n')
        ids.append(case['id'])
    mapping[TEST] = ids
    (ROOT / 'scenarios/test-map.json').write_text(json.dumps(mapping, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({'extracted': len(ids)}))


class ReviewRound4(unittest.TestCase):
    def test_python_first_cases(self):
        cases = list(candidates())
        self.assertEqual(len(cases), 4)
        for case in cases:
            with self.subTest(case=case['id']):
                errors = [e['error'] for e in case['script'] if 'error' in e]
                self.assertTrue(errors)
                self.assertTrue(all('bases' not in e for e in errors))
                stored = json.loads((CASES / (case['id'] + '.json')).read_text())
                self.assertEqual(capture(case), stored)

    def test_rust_matches_legacy_errors_and_recorded_base_only_interruption(self):
        paths = sorted(CASES.glob('*-review-round4-*.json'))
        self.assertEqual(len(paths), 4)
        paths.append(CASES / 'intake-review-round3-match-sent-base-only.json')
        result = subprocess.run([str(BINARY), '--jsonl'], input=''.join(p.read_text() for p in paths),
                                text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout), {'equal': 5, 'different': 0, 'skipped': 0})


if __name__ == '__main__':
    if sys.argv[1:] == ['--extract']: extract()
    else: unittest.main()
