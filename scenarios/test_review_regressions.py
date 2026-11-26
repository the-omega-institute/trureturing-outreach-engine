"""Independent-review regressions, extracted from Python before changing Rust.

Refresh only these fixtures: python3.12 scenarios/test_review_regressions.py --extract
"""
import copy
import json
from pathlib import Path
import subprocess
import sys
import unittest

import runner
from managed.queue import digest

ROOT = Path(__file__).resolve().parents[1]
CASES = ROOT / 'scenarios/cases'
BINARY = ROOT / 'rust/target/debug/examples/scenarios'


def source(pattern):
    paths = list(CASES.glob(pattern + '.json'))
    assert len(paths) == 1, paths
    return json.loads(paths[0].read_text())


def named(case, name):
    case['id'] = case['stage'] + '-review-round2-' + name
    case['source_test'] = 'test_review_regressions.ReviewRegressions.test_python_first_cases'
    return case


def fail(entry, kind):
    entry.pop('result', None)
    entry['error'] = {'type': kind, 'message': 'Synthetic interruption'}


def candidates():
    for kind in ('Stopped', 'ProcessDeath', 'KeyboardInterrupt', 'SystemExit'):
        case = source('intake-*unregistered_alias_linked_to_sent*-1')
        case['script'] = case['script'][:4]
        fail(case['script'][-1], kind)
        yield named(case, 'match-sent-' + kind)

        case = source('intake-*machine_bounce_is_separate*-1')
        case['script'][1]['result'][0].pop('body')
        case['script'][1]['result'][0]['from']['emailAddress']['address'] = 'mailer-daemon@example.invalid'
        case['script'] = case['script'][:2] + [dict(adapter='mailbox', method='research_thread', args=['bounce-thread'], kwargs={})]
        fail(case['script'][-1], kind)
        yield named(case, 'bounce-enrichment-' + kind)

        case = source('intake-*known_correspondent_without_current_sent*-1')
        case['adapter_attributes']['mailbox']['sent_messages_to'] = True
        case['script'] = case['script'][:3] + [dict(adapter='mailbox', method='sent_messages_to', args=['author@example.invalid'], kwargs={})]
        fail(case['script'][-1], kind)
        yield named(case, 'prior-context-' + kind)

    case = source('formalization-*eight_candidate*-10')
    keys = [row['id'] + '-' + item['key'] for row in case['initial_rows']['tasks']['rows']
            for item in row['data'].get('formalization_requests', [])][:8]
    case['admissions'] = [dict(lane='formalization-requests', key=key, allowed=False) for key in keys]
    case['script'] = []
    yield named(case, 'eight-budget-refusals')

    case = source('prior-work-*lost_post*-1')
    original = case['initial_rows']['tasks']['rows'][0]
    rows = []
    for index in range(8):
        row = copy.deepcopy(original)
        row['id'] = row['request']['id'] = row['data']['result']['task_id'] = f'checked-note-{index}'
        row['request']['inputs']['pipeline']['result_id'] = f'example-parity-{index}'
        row['request']['inputs']['result']['id'] = f'example-parity-{index}'
        row['data']['review']['binding']['task_id'] = row['id']
        row['data']['review']['binding']['result_hash'] = digest(row['data']['result'])
        rows.append(row)
    case['initial_rows']['tasks']['rows'] = rows
    case['admissions'] = [dict(lane='prior-work-issues', key=f'example-parity-{index}', allowed=False) for index in range(8)]
    case['script'] = []
    yield named(case, 'eight-budget-refusals')

    errors = [
        dict(type='URLError', reason='Synthetic URL failure', message='<urlopen error Synthetic URL failure>', bases=['OSError','Exception','BaseException']),
        dict(type='JSONDecodeError', msg='Synthetic JSON failure', doc='?', pos=0, message='Synthetic JSON failure: line 1 column 1 (char 0)', bases=['ValueError','Exception','BaseException']),
        dict(type='ConnectionResetError', message='Synthetic reset', bases=['ConnectionError','OSError','Exception','BaseException']),
        dict(type='SyntheticValueError', message='Synthetic value failure', bases=['ValueError','Exception','BaseException']),
    ]
    for stage in ('formalization', 'prior-work'):
        for operation in ('GET','POST'):
            for error in errors:
                case = source('formalization-*eight_candidate*-10' if stage == 'formalization' else 'prior-work-*lost_post*-1')
                if operation == 'GET':
                    index = next(i for i,e in enumerate(case['script']) if e['adapter']=='github')
                    case['script'] = case['script'][:index+1] + [e for e in case['script'][index+1:] if e['adapter']=='ledger' and not e['kwargs']]
                entry = next(e for e in case['script'] if e['adapter']=='github' and (e['args'][1] if len(e['args'])>1 else 'GET')==operation)
                entry.pop('result',None);entry['error']=error
                yield named(case, operation.lower() + '-' + error['type'])

    for stage in ('formalization', 'prior-work'):
        case = source(stage + '-*lost_post*-2' if stage == 'formalization' else stage + '-*lost_post*-1')
        index = next(i for i,e in enumerate(case['script']) if e['adapter']=='github' and len(e['args'])>1)
        case['script'] = case['script'][:index+1]
        fail(case['script'][-1], 'Stopped')
        yield named(case, 'post-interruption')

    case = source('intake-*next_inbound_reuses_attachment_already_staged_before_index-1')
    case['initial_rows']['pipeline_state']['rows'] += [copy.deepcopy(row) for row in case['expected_final_rows']['pipeline_state']['rows'] if row['key'].startswith('mailbox-attachments:')]
    # Graph changes the remote attachment ID on a folder move. Keep the old
    # index/reference and identity; fetch the same bytes under the new ID.
    for row in case['initial_rows']['pipeline_state']['rows']:
        if row['key'].startswith('mailbox-attachments:'):
            row['payload'] = {'old-' + key: value for key,value in row['payload'].items()}
    upload = next(e for e in case['script'] if e['method']=='upload_task_batch')
    listing = dict(adapter='releases', method='task_assets', args=[upload['args'][1]], kwargs={}, result=upload['result']['listings'][upload['args'][1]])
    at = next(i for i,e in enumerate(case['script']) if e['method']=='upload_task_batch')
    case['script'][at] = listing
    yield named(case, 'folder-move-reuses-current-release')

    case = source('inbound-*split_conversations*-1')
    current, historical = case['script'][-2:]
    sent = copy.deepcopy(historical['result'][0])
    sent['conversationId'] = current['args'][0]
    sent['id'] = 'current-sent-first';sent['internetMessageId'] = '<current-first@example.invalid>'
    second = copy.deepcopy(sent);second['id'] = 'current-sent-second';second['internetMessageId'] = '<current-second@example.invalid>'
    current['result'] += [sent, second]
    # All four Sent timestamps tie. Both current latest_sent and global
    # sent_evidence must retain the first observation in adapter order.
    duplicate = copy.deepcopy(historical['result'][0]);duplicate['id']='legacy-second';duplicate['internetMessageId']='<legacy-second@example.invalid>'
    historical['result'].append(duplicate)
    yield named(case, 'equal-sent-times-keep-first')


def extract():
    mapping = json.loads((ROOT/'scenarios/test-map.json').read_text())
    ids = []
    for case in candidates():
        observation = runner.execute(case, capture=True)
        case.update(observation)
        if case['error'] is None:case.pop('error')
        runner.execute(case)
        (CASES/(case['id']+'.json')).write_text(json.dumps(case,ensure_ascii=False,separators=(',',':'))+'\n')
        ids.append(case['id'])
    mapping['test_review_regressions.ReviewRegressions.test_python_first_cases'] = ids
    (ROOT/'scenarios/test-map.json').write_text(json.dumps(mapping,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({'extracted':len(ids)}))


class ReviewRegressions(unittest.TestCase):
    def test_python_first_cases(self):
        for path in sorted(CASES.glob('*-review-round2-*.json')):
            with self.subTest(case=path.stem):runner.execute(json.loads(path.read_text()))

    def test_error_message_mismatch_is_rejected_for_each_new_stage(self):
        for stage in ('intake','inbound','formalization','prior-work'):
            paths = sorted(CASES.glob(stage+'-*.json'),key=lambda p:('review-round2' in p.name,p.name))
            case = next(json.loads(p.read_text()) for p in paths if json.loads(p.read_text()).get('error'))
            runner.execute(case)
            valid = subprocess.run([str(BINARY),'--jsonl'],input=json.dumps(case)+'\n',text=True,capture_output=True,timeout=10)
            self.assertEqual(valid.returncode,0,valid.stderr)
            case['error']['message'] += ' -- deliberately incorrect'
            with self.subTest(stage=stage):
                with self.assertRaisesRegex(AssertionError,'stage error'):runner.execute(case)
                result = subprocess.run([str(BINARY),'--jsonl'],input=json.dumps(case)+'\n',text=True,capture_output=True,timeout=10)
                self.assertNotEqual(result.returncode,0)
                self.assertEqual(json.loads(result.stdout)['different'],1)


if __name__ == '__main__':
    if sys.argv[1:] == ['--extract']:extract()
    else:unittest.main()
