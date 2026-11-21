import unittest
from types import SimpleNamespace
from unittest.mock import patch
from fixture_config import use_example_config
from managed.cma import CMA, CMARequestError


class Response:
    headers = {}
    def __init__(self, lines): self.lines = lines
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def __iter__(self): return iter(self.lines)


class AdmissionTests(unittest.TestCase):
    def test_cli_preserves_http_rejection_status_for_workspace_requeue(self):
        for returncode in (0,1):
            with self.subTest(returncode=returncode), patch.dict('os.environ',{},clear=True), \
                    patch('managed.cma.subprocess.run',return_value=SimpleNamespace(
                        returncode=returncode,stdout='{"status":409,"code":"workspace_not_ready"}')):
                with self.assertRaises(CMARequestError) as refused:
                    CMA().create({'workspace_id':'wks_existing'},'original-key')
                self.assertEqual(refused.exception.code,409)

    def test_direct_token_origin_preserves_admission_identity_headers(self):
        response = Response([])
        response.headers = {'X-CMA-Response-ID':'resp_original', 'X-CMA-Command-ID':'cmd_original'}
        with patch('managed.cma.build_opener') as opener:
            opener.return_value.open.return_value = response
            result = CMA(token='test-token').admit('agt_test', 'same prompt', 'original-key')
        request = opener.return_value.open.call_args.args[0]
        self.assertEqual(request.full_url, 'https://worker.example.invalid/api/v2/responses')
        self.assertEqual(request.get_header('Idempotency-key'), 'original-key')
        self.assertEqual(result, {'response_id':'resp_original','command_id':'cmd_original'})

    def test_headerless_sse_preserves_original_admission(self):
        response = Response([b'event: response.created\n',
                             b'data: {"type":"response.created","response":{"id":"resp_original"},"command_id":"cmd_original"}\n'])
        with patch('managed.cma.build_opener') as opener:
            opener.return_value.open.return_value = response
            result = CMA(token='test-token').admit('agt_test', 'same prompt', 'original-key')
        self.assertEqual(result, {'response_id':'resp_original','command_id':'cmd_original'})
        self.assertEqual(opener.return_value.open.call_count, 1)

    def test_missing_durable_event_remains_uncertain(self):
        with patch('managed.cma.build_opener') as opener:
            opener.return_value.open.return_value = Response([b'data: {"type":"progress"}\n'])
            with self.assertRaisesRegex(RuntimeError, 'retain key'):
                CMA(token='test-token').admit('agt_test', 'same prompt', 'original-key')
        self.assertEqual(opener.return_value.open.call_count, 1)


if __name__ == '__main__': unittest.main()


def setUpModule():
    use_example_config()
