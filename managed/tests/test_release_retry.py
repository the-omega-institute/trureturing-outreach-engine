import subprocess
import unittest
from unittest.mock import patch

from managed.control import Releases


def result(code, stderr=b''):
    return subprocess.CompletedProcess([], code, b'ok' if not code else b'', stderr)


class ReleaseRetryTests(unittest.TestCase):
    def setUp(self):
        self.releases = Releases('owner/repo', 'tag')
        self.releases.RETRY_DELAYS = (0, 0)

    def test_download_retries_transient_5xx(self):
        calls = [result(1, b'HTTP 500 (https://api.github.com/x)'), result(0)]
        with patch('subprocess.run', side_effect=calls) as run:
            self.assertEqual(self.releases.run('release', 'download', 'tag'), b'ok')
        self.assertEqual(run.call_count, 2)

    def test_upload_and_client_errors_are_not_retried(self):
        with patch('subprocess.run', return_value=result(1, b'HTTP 500')) as run:
            with self.assertRaises(RuntimeError):
                self.releases.run('release', 'upload', 'tag', 'file')
        self.assertEqual(run.call_count, 1)
        with patch('subprocess.run', return_value=result(1, b'release not found (HTTP 404)')) as run:
            self.assertIsNone(self.releases.run('release', 'view', 'tag', missing=True))
        self.assertEqual(run.call_count, 1)

    def test_persistent_5xx_still_fails(self):
        with patch('subprocess.run', return_value=result(1, b'HTTP 502')) as run:
            with self.assertRaises(RuntimeError):
                self.releases.run('release', 'download', 'tag')
        self.assertEqual(run.call_count, 3)


if __name__ == '__main__':
    unittest.main()
