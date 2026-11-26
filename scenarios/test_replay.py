"""Real recording hooks -> private JSONL -> Rust stage, with no live adapters."""
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / 'scenarios'), str(ROOT / 'rust')]
import runner
from replay_mathesis import replay
from managed import recording
from managed.tasks import Tasks


class ReplayTests(unittest.TestCase):
    def test_real_recording_roundtrip_and_snapshot_immutability(self):
        names = [
            'mathesis-pass_signed_selection_and_persisted_receipt_finish_without_platform_review-1',
            'publication-exact_pass_enqueues_durable_request_and_signs_decision-1',
            'publication-equal_priority_notes_preserve_arrival_order-1',
            'sent-owner_draft_sent_uses_exact_sent_receipt_and_reviewed_headline-1',
            'sent-two_clean_pages_skip_commit_and_forced_save-1',
        ]
        binary = ROOT / 'rust/target/debug/examples/scenarios'
        self.assertTrue(binary.is_file(), 'Build the local Rust scenario runner first')
        for name in names:
            with self.subTest(stage=name.split('-')[0]), tempfile.TemporaryDirectory() as directory:
                case = json.loads((ROOT / 'scenarios/cases' / (name + '.json')).read_text())
                root = Path(directory); records = root / 'records'; records.mkdir()
                snapshot = root / 'start.sqlite3'
                tasks = Tasks(snapshot); runner.load(tasks, case['initial_rows']); tasks.close()
                checksum = hashlib.sha256(snapshot.read_bytes()).hexdigest()
                with patch.dict(os.environ, {'OUTREACH_RECORD_DIR': str(records)}):
                    recording.pointer('start', {'schema_version': 1, 'snapshot': {'sha256': checksum}})
                    runner.execute(case)
                with contextlib.redirect_stdout(io.StringIO()) as stdout, contextlib.redirect_stderr(io.StringIO()) as stderr:
                    result = replay(records, snapshot, binary, 'secret')
                self.assertEqual(result, {'equal': 1, 'different': 0, 'skipped': 0})
                self.assertEqual(stdout.getvalue() + stderr.getvalue(), '')
                self.assertEqual(hashlib.sha256(snapshot.read_bytes()).hexdigest(), checksum)
                self.assertEqual(replay(records, snapshot, binary, None),
                    {'equal': int(case['stage'] == 'sent'), 'different': 0, 'skipped': int(case['stage'] != 'sent')})
                for path in records.glob('calls-*.jsonl'):
                    original = path.read_text()
                    events = [json.loads(line) for line in original.splitlines()]
                    changed = False
                    for event in events:
                        for call in event.get('case', {}).get('script', []):
                            if call.get('saved_snapshot_digest'):
                                call['saved_snapshot_digest'] = '0' * 64; changed = True; break
                    if changed:
                        path.write_text(''.join(json.dumps(event) + '\n' for event in events))
                        self.assertEqual(replay(records, snapshot, binary, 'secret')['different'], 1)
                        path.write_text(original)
                snapshot.write_bytes(snapshot.read_bytes() + b'changed')
                self.assertEqual(replay(records, snapshot, binary, 'secret')['different'], 1)

    def test_unported_boundary_delta_replays_from_the_original_snapshot(self):
        case = json.loads((ROOT / 'scenarios/cases/mathesis-pass_signed_selection_and_persisted_receipt_finish_without_platform_review-1.json').read_text())
        binary = ROOT / 'rust/target/debug/examples/scenarios'
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); records = root / 'records'; records.mkdir()
            snapshot = root / 'start.sqlite3'
            case['initial_rows']['pipeline_state']['rows'].extend([
                {'key': 'unported-delete', 'payload': {'version': 1}},
                {'key': 'unported-existing', 'payload': {'version': 1}},
            ])
            tasks = Tasks(snapshot); runner.load(tasks, case['initial_rows']); tasks.close()
            with patch.dict(os.environ, {'OUTREACH_RECORD_DIR': str(records)}):
                case['_recording_baseline'] = recording.snapshot_index(snapshot)
                changed = {'key': 'unported-observation', 'payload': {'synthetic': True}}
                revised = {'key': 'unported-existing', 'payload': {'version': 2}}
                case['initial_rows']['pipeline_state']['rows'] = [
                    r for r in case['initial_rows']['pipeline_state']['rows'] if not r['key'].startswith('unported-')
                ] + [revised, changed]
                case['expected_final_rows']['pipeline_state']['rows'].extend([revised, changed])
                case['expected_final_rows'] = runner.logical(case['expected_final_rows'])
                runner.execute(case)
                follow = json.loads(json.dumps(case))
                follow['initial_rows'] = json.loads(json.dumps(case['expected_final_rows']))
                follow['expected_final_rows'] = json.loads(json.dumps(follow['initial_rows']))
                follow['script'], follow['expected_writes'] = [], []
                follow['config']['paused'] = True
                after = root / 'after.sqlite3'
                tasks = Tasks(after); runner.load(tasks, follow['initial_rows']); tasks.close()
                follow['_recording_baseline'] = recording.snapshot_index(after)
                intervening = {'key': 'between-supported-stages', 'payload': {'synthetic': True}}
                follow['initial_rows']['pipeline_state']['rows'].append(intervening)
                follow['expected_final_rows']['pipeline_state']['rows'].append(intervening)
                follow['expected_final_rows'] = runner.logical(follow['expected_final_rows'])
                runner.execute(follow)
            frame = next(json.loads(line)['case'] for path in records.glob('calls-*.jsonl')
                         for line in path.read_text().splitlines() if json.loads(line).get('event') == 'mathesis_stage')
            self.assertEqual(len(frame['initial_delta']['pipeline_state']['changes']), 2)
            self.assertEqual(len(frame['initial_delta']['pipeline_state']['deleted']), 1)
            self.assertNotIn('initial_rows', frame)
            self.assertEqual(replay(records, snapshot, binary, 'secret'), {'equal': 2, 'different': 0, 'skipped': 0})

    def test_pool_and_mail_recording_roundtrips_without_signing_key(self):
        patterns = [
            ('pool-test_sandbox_pool-PoolLifecycleTests-test_idle_retirement_threshold_and_forced_checkpoint-*.json', 'retire_sandbox'),
            ('delivery-test_automation-AutomationTests-test_routine_reply_is_sent_once_after_pass_and_intent_is_remotely_preserved-*.json', 'submit'),
            ('delivery-test_automation-AutomationTests-test_owner_draft_intent_is_forced_before_save_and_recovers_same_key-*.json', 'save_draft'),
            ('delivery-test_draft_revision-DraftRevisionTests-test_crash_after_persisted_intent_before_create-2.json', 'create_draft'),
        ]
        binary = ROOT / 'rust/target/debug/examples/scenarios'
        for pattern, method in patterns:
            cases = [json.loads(p.read_text()) for p in sorted((ROOT/'scenarios/cases').glob(pattern))]
            case = next(c for c in cases if not c.get('error') and any(e['method']==method for e in c['script']))
            with self.subTest(stage=case['stage'], method=method), tempfile.TemporaryDirectory() as directory:
                root = Path(directory); records = root/'records'; records.mkdir(); snapshot=root/'start.sqlite3'
                tasks=Tasks(snapshot)
                if 'items' in case['initial_rows']:
                    from managed.queue import Queue
                    Queue(tasks.path, {'reply':'e'*64}).close()
                runner.load(tasks,case['initial_rows']); tasks.close()
                checksum=hashlib.sha256(snapshot.read_bytes()).hexdigest()
                with patch.dict(os.environ, {'OUTREACH_RECORD_DIR':str(records)}):
                    recording.pointer('start', {'schema_version':1,'snapshot':{'sha256':checksum}})
                    runner.execute(case)
                self.assertEqual(replay(records,snapshot,binary,None),{'equal':1,'different':0,'skipped':0})
                self.assertEqual(hashlib.sha256(snapshot.read_bytes()).hexdigest(),checksum)

    def test_malformed_record_is_content_free_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); snapshot = root / 'snapshot.sqlite3'; tasks = Tasks(snapshot); tasks.close()
            (root / 'calls-synthetic.jsonl').write_text('SYNTHETIC PRIVATE INPUT\n')
            with contextlib.redirect_stdout(io.StringIO()) as stdout, contextlib.redirect_stderr(io.StringIO()) as stderr:
                result = replay(root, snapshot, root / 'absent', 'secret')
            self.assertEqual(result, {'equal': 0, 'different': 1, 'skipped': 0})
            self.assertEqual(stdout.getvalue() + stderr.getvalue(), '')


if __name__ == '__main__': unittest.main()
