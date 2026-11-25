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
        case = json.loads((ROOT / 'scenarios/cases/mathesis-pass_signed_selection_and_persisted_receipt_finish_without_platform_review-1.json').read_text())
        binary = ROOT / 'rust/target/debug/examples/scenarios'
        self.assertTrue(binary.is_file(), 'Build the local Rust scenario runner first')
        with tempfile.TemporaryDirectory() as directory:
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
            self.assertEqual(replay(records, snapshot, binary, None), {'equal': 0, 'different': 0, 'skipped': 1})
            snapshot.write_bytes(snapshot.read_bytes() + b'changed')
            self.assertEqual(replay(records, snapshot, binary, 'secret')['different'], 1)

    def test_malformed_record_is_content_free_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); snapshot = root / 'snapshot.sqlite3'; tasks = Tasks(snapshot); tasks.close()
            (root / 'calls-synthetic.jsonl').write_text('SYNTHETIC PRIVATE INPUT\n')
            with contextlib.redirect_stdout(io.StringIO()) as stdout, contextlib.redirect_stderr(io.StringIO()) as stderr:
                result = replay(root, snapshot, root / 'absent', 'secret')
            self.assertEqual(result, {'equal': 0, 'different': 1, 'skipped': 0})
            self.assertEqual(stdout.getvalue() + stderr.getvalue(), '')


if __name__ == '__main__': unittest.main()
