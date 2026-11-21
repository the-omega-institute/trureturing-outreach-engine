"""The local executor's final language pass keeps every link and attachment."""
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from managed import local_run


class FakeProcess:
    hang = False

    def __init__(self, directory, body):
        self.pid = 1
        self.killed = False
        if body is not None:
            (Path(directory) / 'polish' / 'body.txt').write_text(body)

    def wait(self, timeout=None):
        if self.hang and not self.killed:
            raise local_run.subprocess.TimeoutExpired('codex', timeout)
        return 0

    def kill(self):
        self.killed = True


def result(body):
    return {'outcome': 'completed', 'report': 'Draft ready.', 'packet': {'body': body}}


class PolishTests(unittest.TestCase):
    def run_polish(self, original, polished):
        with tempfile.TemporaryDirectory() as temp:
            with patch.object(local_run.subprocess, 'Popen',
                              side_effect=lambda *a, **k: FakeProcess(temp, polished)):
                out = local_run.polish(result(original), temp, {})
            skipped = (Path(temp) / 'polish' / 'skipped.txt')
            return out, skipped.read_text() if skipped.exists() else None

    def test_polished_body_replaces_draft(self):
        original = 'Dear Ann,\n\nSee https://x.org/a.lean and note.pdf.\n\nBest,\nW'
        polished = 'Dear Ann,\n\nHere is an update: see https://x.org/a.lean and the attached note.pdf.\n\nBest,\nW'
        out, skipped = self.run_polish(original, polished)
        self.assertEqual(out['packet']['body'], polished)
        self.assertIsNone(skipped)
        self.assertIn('Final language pass', out['report'])

    def test_lost_link_or_attachment_keeps_original(self):
        original = 'Dear Ann,\n\nSee https://x.org/a.lean and note.pdf.\n\nBest,\nW'
        out, skipped = self.run_polish(original, 'Dear Ann,\n\nSee the attached note.\n\nBest,\nW')
        self.assertEqual(out['packet']['body'], original)
        self.assertIn('https://x.org/a.lean', skipped)

    def test_failed_codex_keeps_original(self):
        out, skipped = self.run_polish('Dear Ann,\n\nHi.\n\nBest,\nW', None)
        self.assertEqual(out['packet']['body'], 'Dear Ann,\n\nHi.\n\nBest,\nW')
        self.assertIn('original body kept', skipped)

    def test_timeout_after_body_written_uses_polished_body(self):
        original = 'Dear Ann,\n\nSee note.pdf.\n\nBest,\nW'
        polished = 'Dear Ann,\n\nPlease see the attached note.pdf.\n\nBest,\nW'
        FakeProcess.hang = True
        try:
            out, skipped = self.run_polish(original, polished)
        finally:
            FakeProcess.hang = False
        self.assertEqual(out['packet']['body'], polished)
        self.assertIsNone(skipped)

    def test_timeout_without_body_keeps_original(self):
        FakeProcess.hang = True
        try:
            out, skipped = self.run_polish('Dear Ann,\n\nHi.\n\nBest,\nW', None)
        finally:
            FakeProcess.hang = False
        self.assertEqual(out['packet']['body'], 'Dear Ann,\n\nHi.\n\nBest,\nW')
        self.assertIn('exit -9', skipped)

    def test_no_reply_results_are_untouched(self):
        value = {'outcome': 'no-reply', 'report': 'No reply needed.', 'packet': None}
        self.assertEqual(local_run.polish(value, '/nonexistent', {}), value)


if __name__ == '__main__':
    unittest.main()
