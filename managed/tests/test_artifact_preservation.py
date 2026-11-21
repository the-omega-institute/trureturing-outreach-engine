"""Artifacts are preserved and read back concurrently with identical checks."""
import hashlib
import json
from pathlib import Path
import tempfile
import threading
import unittest

from managed import control


class FakeReleases:
    repo = 'owner/repo'

    def __init__(self):
        self.files = {}
        self.lock = threading.Lock()

    def preserve_task(self, path, tag):
        with self.lock:
            self.files[tag, path.name] = path.read_bytes()

    def run(self, *args):
        tag, name, folder = args[2], args[args.index('--pattern') + 1], args[args.index('--dir') + 1]
        (Path(folder) / name).write_bytes(self.files[tag, name])


class FakeCMA:
    def __init__(self, blobs):
        self.blobs = blobs

    def artifact(self, agent_id, path, size, sha):
        return self.blobs[path]


def artifact(name, raw, role='evidence'):
    return {'name': name, 'agent_path': 'deliverables/' + name, 'role': role, 'release_tag': 'outreach-task-x',
            'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()}


class ArtifactPreservationTests(unittest.TestCase):
    def test_all_artifacts_preserved_and_packet_read_from_its_readback(self):
        packet = json.dumps({'to': ['a@example.invalid'], 'body': 'B'}).encode()
        blobs = {'deliverables/f%d.txt' % i: b'x' * (i + 1) for i in range(5)}
        blobs['deliverables/packet.json'] = packet
        items = [artifact('f0.txt', blobs['deliverables/f0.txt']), artifact('f1.txt', blobs['deliverables/f1.txt']),
                 artifact('packet.json', packet, 'mail-packet'),
                 artifact('f3.txt', blobs['deliverables/f3.txt']), artifact('f4.txt', blobs['deliverables/f4.txt'])]
        result = {'artifacts': items, 'packet': {'artifact': 'packet.json'}}
        releases = FakeReleases()
        with tempfile.TemporaryDirectory() as folder:
            control.verify_artifacts(result, releases, folder, FakeCMA(blobs), 'agt_x')
        self.assertEqual(len(releases.files), 5)
        self.assertEqual(result['packet'], {'to': ['a@example.invalid'], 'body': 'B'})
        self.assertTrue(all('agent_path' not in a for a in items))

    def test_checksum_mismatch_still_fails(self):
        blobs = {'deliverables/f0.txt': b'a', 'deliverables/f1.txt': b'b'}
        items = [artifact('f0.txt', b'a'), artifact('f1.txt', b'b')]
        items[1]['sha256'] = '0' * 64
        with tempfile.TemporaryDirectory() as folder, self.assertRaises(ValueError):
            control.verify_artifacts({'artifacts': items}, FakeReleases(), folder, FakeCMA(blobs), 'agt_x')


class SerialCMA(FakeCMA):
    """Refuses overlapping reads like CMA's file_capacity limit."""
    def __init__(self, blobs, refusals=0):
        super().__init__(blobs)
        self.active = 0
        self.overlap = False
        self.refusals = refusals
        self.lock = threading.Lock()

    def artifact(self, agent_id, path, size, sha):
        with self.lock:
            self.active += 1
            self.overlap = self.overlap or self.active > 1
        try:
            if self.refusals:
                self.refusals -= 1
                raise RuntimeError("CMA refused request: {'title': 'file_capacity'}")
            import time
            time.sleep(0.01)
            return self.blobs[path]
        finally:
            with self.lock:
                self.active -= 1


class SerialReadTests(unittest.TestCase):
    def setUp(self):
        self.delays = control.ARTIFACT_READ_DELAYS
        control.ARTIFACT_READ_DELAYS = (0, 0)

    def tearDown(self):
        control.ARTIFACT_READ_DELAYS = self.delays

    def items(self):
        blobs = {'deliverables/f%d.txt' % i: b'y' * (i + 1) for i in range(6)}
        return blobs, [artifact('f%d.txt' % i, blobs['deliverables/f%d.txt' % i]) for i in range(6)]

    def test_cma_reads_never_overlap(self):
        blobs, items = self.items()
        cma = SerialCMA(blobs)
        with tempfile.TemporaryDirectory() as folder:
            control.verify_artifacts({'artifacts': items}, FakeReleases(), folder, cma, 'agt_x')
        self.assertFalse(cma.overlap)

    def test_file_capacity_refusal_is_retried_then_raised(self):
        blobs, items = self.items()
        with tempfile.TemporaryDirectory() as folder:
            control.verify_artifacts({'artifacts': items[:1]}, FakeReleases(), folder, SerialCMA(blobs, 2), 'agt_x')
        blobs, items = self.items()
        with tempfile.TemporaryDirectory() as folder, self.assertRaises(RuntimeError):
            control.verify_artifacts({'artifacts': items[:1]}, FakeReleases(), folder, SerialCMA(blobs, 3), 'agt_x')


class RuntimeNotReadyTests(unittest.TestCase):
    def test_idle_agent_runtime_routes_to_result_repair(self):
        class Idle(FakeCMA):
            def artifact(self, agent_id, path, size, sha):
                raise RuntimeError("CMA refused request: {'code': 'file_runtime_not_ready', 'status': 409}")
        blobs = {'deliverables/f0.txt': b'a'}
        with tempfile.TemporaryDirectory() as folder, self.assertRaisesRegex(ValueError, 'file_runtime_not_ready'):
            control.verify_artifacts({'artifacts': [artifact('f0.txt', b'a')]}, FakeReleases(), folder, Idle(blobs), 'agt_x')


class TokenModeProblemCodeTests(unittest.TestCase):
    def test_http_error_body_code_survives(self):
        import io
        from urllib.error import HTTPError
        from managed.cma import CMA, CMARequestError
        cma = CMA(token='fake')
        body = json.dumps({'code': 'file_runtime_not_ready', 'status': 409}).encode()
        def request(path, method='GET', body_=None, key=None):
            if '/files/content' in path:
                raise HTTPError(path, 409, 'Conflict', {}, io.BytesIO(body))
            return {'worktree': {'path': '/home/cma/workspace/.worktrees/wt-1'}}
        cma.request = request
        with self.assertRaises(CMARequestError) as caught:
            cma.artifact('agt_x', 'deliverables/a.txt', 3, '0' * 64)
        self.assertIn('file_runtime_not_ready', str(caught.exception))
        self.assertEqual(caught.exception.code, 409)
