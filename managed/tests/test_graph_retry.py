import io
import unittest
from unittest.mock import patch

from managed import mailbox


class Response(io.BytesIO):
    def __enter__(self): return self
    def __exit__(self, *a): return False


class Opener:
    def __init__(self, failures):
        self.failures, self.calls = failures, 0
    def open(self, request, timeout):
        self.calls += 1
        if self.calls <= self.failures:
            raise TimeoutError('The read operation timed out')
        return Response(b'{"ok": true}')


class GraphRetryTests(unittest.TestCase):
    def test_get_retries_read_timeouts(self):
        opener = Opener(2)
        with patch.object(mailbox, 'build_opener', return_value=opener), patch.object(mailbox.time, 'sleep'):
            self.assertEqual(mailbox.Graph('t').request('/me/messages'), {'ok': True})
        self.assertEqual(opener.calls, 3)

    def test_get_gives_up_after_three_timeouts(self):
        opener = Opener(5)
        with patch.object(mailbox, 'build_opener', return_value=opener), patch.object(mailbox.time, 'sleep'):
            with self.assertRaises(TimeoutError):
                mailbox.Graph('t').request('/me/messages')
        self.assertEqual(opener.calls, 3)

    def test_get_retries_gateway_errors_but_not_client_errors(self):
        from urllib.error import HTTPError
        class Flaky(Opener):
            def __init__(self, code):
                super().__init__(0); self.code = code
            def open(self, request, timeout):
                self.calls += 1
                if self.calls == 1:
                    raise HTTPError(request.full_url, self.code, 'x', {}, None)
                return Response(b'{}')
        with patch.object(mailbox.time, 'sleep'):
            opener = Flaky(504)
            with patch.object(mailbox, 'build_opener', return_value=opener):
                self.assertEqual(mailbox.Graph('t').request('/me/messages'), {})
            self.assertEqual(opener.calls, 2)
            opener = Flaky(404)
            with patch.object(mailbox, 'build_opener', return_value=opener):
                with self.assertRaises(HTTPError):
                    mailbox.Graph('t').request('/me/messages')
            self.assertEqual(opener.calls, 1)

    def test_writes_are_never_repeated(self):
        opener = Opener(1)
        with patch.object(mailbox, 'build_opener', return_value=opener), patch.object(mailbox.time, 'sleep'):
            with self.assertRaises(TimeoutError):
                mailbox.Graph('t').request('/me/messages', method='POST', body={})
        self.assertEqual(opener.calls, 1)


if __name__ == '__main__':
    unittest.main()
