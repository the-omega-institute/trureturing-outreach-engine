import os
import tempfile
import unittest
from unittest import mock

from managed import recording


class _Request:
    full_url = 'https://example.invalid/api/items'
    data = None
    def header_items(self): raise RuntimeError('broken headers')
    def get_method(self): return 'GET'
    def get_header(self, name, default=None): return default


class _Response:
    status = 200
    def __enter__(self): return self
    def __exit__(self, *args): return False
    def read(self, *args): return b'{"ok": true}'


class _Opener:
    calls = 0
    def open(self, request, **kwargs):
        self.calls += 1
        return _Response()


class RecordingIsolationTest(unittest.TestCase):
    """Recording failures never change, prevent or repeat the recorded call."""

    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        patcher = mock.patch.dict(os.environ, {'OUTREACH_RECORD_DIR': self.folder.name})
        patcher.start(); self.addCleanup(patcher.stop); self.addCleanup(self.folder.cleanup)

    def test_descriptor_failure_still_calls_once(self):
        opener = _Opener()
        response = recording.open('synthetic', opener, _Request())
        self.assertEqual(opener.calls, 1)
        self.assertEqual(response.read(), b'{"ok": true}')

    def test_exit_recording_failure_does_not_mask_result(self):
        with mock.patch.object(recording, 'emit', side_effect=RuntimeError('disk full')):
            wrapped = recording.Response(_Response(), {'adapter': 'synthetic'}, [])
            with wrapped as response:
                self.assertEqual(response.read(), b'{"ok": true}')

    def test_run_records_failure_without_changing_result(self):
        with mock.patch.object(recording, 'command_call', side_effect=RuntimeError('bad args')):
            result = recording.run(['true'], capture_output=True)
        self.assertEqual(result.returncode, 0)

    def test_capture_encode_failure_calls_adapter_once(self):
        calls = []
        def function(*args, **kwargs):
            calls.append(args); return {'value': 1}
        frame = {'script': [], '_errors': {}}
        token = recording._STAGE.set(frame)
        try:
            with mock.patch('managed.scenario_contract.encode', side_effect=RuntimeError('unencodable')):
                result = recording._capture('synthetic', 'read', function, (1,), {})
        finally:
            recording._STAGE.reset(token)
        self.assertEqual(result, {'value': 1})
        self.assertEqual(len(calls), 1)
        self.assertTrue(frame.get('incomplete'))


if __name__ == '__main__':
    unittest.main()
