"""Exception hierarchy regressions captured from Python before the Rust fixes.

Refresh only these fixtures: python3.12 scenarios/test_review_round3.py --extract
"""
import json
import sys
import unittest

import runner
from test_review_regressions import CASES, ROOT, source

TEST = 'test_review_round3.ReviewRound3.test_python_first_cases'


def named(case, name):
    case['id'] = case['stage'] + '-review-round3-' + name
    case['source_test'] = TEST
    return case


def inject(entry, kind, bases, message='Synthetic subclass failure', **fields):
    entry.pop('result', None)
    entry['error'] = dict(type=kind, message=message, bases=bases, **fields)


def candidates():
    for kind, bases, message, suffix in (
        ('NotImplementedError', ['RuntimeError', 'Exception', 'BaseException'], 'Synthetic unsupported download', 'runtime-subclass'),
        ('NotImplementedError', ['RuntimeError', 'Exception', 'BaseException'], 'Synthetic download HTTP 503 failure', 'runtime-subclass-http'),
        ('SyntheticGateError', ['GateError', 'ValueError', 'Exception', 'BaseException'], 'Synthetic evidence gate', 'gate-subclass'),
        ('SyntheticUnparseableEvidence', ['UnparseableEvidence', 'ValueError', 'Exception', 'BaseException'], 'Synthetic unparseable evidence', 'unparseable-subclass'),
    ):
        case = source('prior-work-*legacy_heading_required*-5')
        inject(case['script'][0], kind, bases, message)
        yield named(case, 'evidence-' + suffix)

    for location in ('match-sent', 'bounce-enrichment', 'prior-context'):
        case = source('intake-review-round2-' + location + '-Stopped')
        inject(case['script'][-1], 'SyntheticBaseOnly', ['BaseException'])
        yield named(case, location + '-base-only')

    case = source('intake-*next_inbound_reuses_attachment_already_staged_before_index-1')
    case['script'] = case['script'][:5]
    inject(case['script'][-1], 'SyntheticBaseOnly', ['BaseException'])
    yield named(case, 'attachment-preservation-base-only')

    original = source('intake-*retain_each_message_continue_and_checkpoint_without_attachment_content-1')
    error = original['script'][3]['error']
    case = source('intake-*retain_each_message_continue_and_checkpoint_without_attachment_content-1')
    inject(case['script'][3], 'SyntheticUnsupportedAttachments',
           ['UnsupportedThreadAttachments', 'GateError', 'ValueError', 'Exception', 'BaseException'],
           error['message'], attachments=error['attachments'])
    yield named(case, 'thread-unsupported-subclass')

    case = source('intake-*next_inbound_reuses_attachment_already_staged_before_index-1')
    case['script'] = case['script'][:5] + [case['script'][-1]]
    inject(case['script'][4], 'SyntheticUnsupportedAttachments',
           ['UnsupportedThreadAttachments', 'GateError', 'ValueError', 'Exception', 'BaseException'],
           error['message'], attachments=error['attachments'])
    yield named(case, 'attachment-unsupported-subclass')

    case = source('intake-*unregistered_alias_linked_to_sent*-1')
    inject(case['script'][3], 'SyntheticSentDeferred', ['SentSubjectsDeferred', 'Exception', 'BaseException'],
           intake_reason='Synthetic Sent cache deferred')
    yield named(case, 'sent-deferred-subclass')


def extract():
    mapping = json.loads((ROOT / 'scenarios/test-map.json').read_text())
    ids = []
    for case in candidates():
        case.update(runner.execute(case, capture=True))
        if case['error'] is None: case.pop('error')
        runner.execute(case)
        (CASES / (case['id'] + '.json')).write_text(json.dumps(case, ensure_ascii=False, separators=(',', ':')) + '\n')
        ids.append(case['id'])
    mapping[TEST] = ids
    (ROOT / 'scenarios/test-map.json').write_text(json.dumps(mapping, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({'extracted': len(ids)}))


class ReviewRound3(unittest.TestCase):
    def test_python_first_cases(self):
        paths = sorted(CASES.glob('*-review-round3-*.json'))
        self.assertEqual(len(paths), 11)
        for path in paths:
            with self.subTest(case=path.stem): runner.execute(json.loads(path.read_text()))


if __name__ == '__main__':
    if sys.argv[1:] == ['--extract']: extract()
    else: unittest.main()
