"""Transient gateway errors are retried only for idempotent credential requests."""
import io
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from fixture_config import test_config
from fixture_config import use_example_config
from managed import credentials


class FakeResponse(io.BytesIO):
    def __enter__(self):return self
    def __exit__(self,*a):return False


def opener(outcomes, calls):
    class Opener:
        def open(self, request, timeout=None):
            calls.append(request.get_method())
            item = outcomes.pop(0)
            if isinstance(item, int):
                raise HTTPError(request.full_url, item, 'err', {}, None)
            return FakeResponse(item)
    return lambda *a, **k: Opener()


URL = test_config()['credentials']['nyx_base_url'] + '/api/v1/test'


class CredentialRetryTests(unittest.TestCase):
    def run_with(self, outcomes, method='GET'):
        calls = []
        with patch.object(credentials, 'build_opener', opener(list(outcomes), calls)), \
                patch.object(credentials.time, 'sleep'):
            try:
                return credentials.request(URL, method, {'a': 1} if method != 'GET' else None), calls
            except HTTPError as exc:
                return exc, calls

    def test_get_retries_transient_gateway_error(self):
        result, calls = self.run_with([502, b'{"ok": true}'])
        self.assertEqual(result, {'ok': True}); self.assertEqual(len(calls), 2)

    def test_put_retries_then_gives_up(self):
        result, calls = self.run_with([503, 503, 503], 'PUT')
        self.assertIsInstance(result, HTTPError); self.assertEqual(len(calls), 3)

    def test_post_is_never_repeated(self):
        result, calls = self.run_with([503, b'{}'], 'POST')
        self.assertIsInstance(result, HTTPError); self.assertEqual(calls, ['POST'])

    def test_client_error_is_not_retried(self):
        result, calls = self.run_with([401, b'{}'])
        self.assertIsInstance(result, HTTPError); self.assertEqual(len(calls), 1)


if __name__ == '__main__':
    unittest.main()


def setUpModule():
    use_example_config()
