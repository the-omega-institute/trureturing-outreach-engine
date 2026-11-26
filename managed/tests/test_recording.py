"""Recording stays opt-in, preserves transport outcomes and never exposes tokens."""
from contextlib import contextmanager
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError
from urllib.request import Request

from managed import recording, run_log
from managed.tick import Tick


class Response(io.BytesIO):
    status = 207


class RecordingTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(); self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name)
        self.enterContext(patch.dict(os.environ, {'OUTREACH_RECORD_DIR': str(self.path)}))

    def records(self):
        return [json.loads(line) for path in self.path.glob('*.jsonl') for line in path.read_text().splitlines()]

    def test_unset_has_no_files_and_keeps_response_identity(self):
        opener = Mock(); response = object(); opener.open.return_value = response
        with patch.dict(os.environ, {'OUTREACH_RECORD_DIR': ''}):
            self.assertIs(recording.open('test', opener, Request('https://service.example.invalid')), response)
            recording.emit({'value': object()})
            with patch.object(subprocess, 'run', return_value=response):
                self.assertIs(recording.run(['git', 'status']), response)
        self.assertEqual(list(self.path.iterdir()), [])

    def test_header_url_and_body_token_are_redacted_using_existing_registry(self):
        token = 'synthetic-token-42/+secret'
        secret_context = run_log.SECRETS.set({token}); self.addCleanup(run_log.SECRETS.reset, secret_context)
        request = Request('https://service.example.invalid/me?access_token=' + token,
            headers={'Authorization': 'Bearer ' + token}, data=json.dumps({'echo': token, 'password': 'synthetic-password'}).encode(), method='GET')
        opener = Mock(); opener.open.return_value = Response(json.dumps({'echo': token, 'nested': {'refresh_token': token}}).encode())
        with recording.open('test', opener, request) as response: response.read()
        raw = next(self.path.glob('*.jsonl')).read_text()
        self.assertNotIn(token, raw); self.assertNotIn('synthetic-password', raw)
        row, = self.records(); self.assertEqual(row['status'], 207)
        self.assertEqual(row['request_body']['echo'], '[redacted]')
        self.assertEqual(row['response_body']['nested']['refresh_token'], '[redacted]')
        self.assertEqual(raw, recording.canonical(row) + '\n')

    def test_header_token_is_registered_even_without_public_log_context(self):
        token = 'synthetic-header-token'
        opener = Mock(); opener.open.return_value = Response(json.dumps({'echo': token}).encode())
        with recording.open('test', opener, Request('https://service.example.invalid/me', headers={'Authorization': 'Bearer ' + token})) as response:
            response.read()
        self.assertNotIn(token, next(self.path.glob('*.jsonl')).read_text())
        result = subprocess.CompletedProcess(['gh'], 0, token.encode(), b'')
        with patch.object(subprocess, 'run', return_value=result):
            recording.run(['gh', 'release', 'view', 'synthetic'], env={'GH_TOKEN': token})
        self.assertNotIn(token, next(self.path.glob('*.jsonl')).read_text())

    def test_write_records_digest_key_and_receipt_without_request_body(self):
        opener = Mock(); opener.open.return_value = Response(b'{"id":"receipt"}')
        req = Request('https://service.example.invalid/api/v2/responses', method='POST',
                      data=b'{"input":"synthetic prompt"}', headers={'Idempotency-Key': 'original-key'})
        with recording.open('cma', opener, req) as response: response.read()
        row, = self.records(); self.assertEqual(row['idempotency_key'], 'original-key')
        self.assertTrue(row['write']); self.assertEqual(len(row['body_digest']), 64)
        self.assertNotIn('request_body', row); self.assertNotIn('synthetic prompt', json.dumps(row))

    def test_http_failure_preserves_original_error_and_unread_body(self):
        error = HTTPError('https://service.example.invalid', 409, 'Conflict', {}, io.BytesIO(b'{"code":"conflict"}'))
        opener = Mock(); opener.open.side_effect = error
        with self.assertRaises(HTTPError) as caught: recording.open('test', opener, Request('https://service.example.invalid/me'))
        self.assertIs(caught.exception, error); self.assertEqual(error.read(), b'{"code":"conflict"}')
        self.assertEqual(self.records()[0]['status'], 409)

    def test_recording_failure_cannot_change_a_completed_effect(self):
        result = subprocess.CompletedProcess(['git', 'status'], 0, b'ok', b'')
        with patch.object(subprocess, 'run', return_value=result), patch.object(Path, 'mkdir', side_effect=OSError('synthetic')), patch('sys.stderr', io.StringIO()):
            self.assertIs(recording.run(['git', 'status']), result)

    def test_tick_allow_and_rejection_are_recorded_without_clock_changes(self):
        clock = Mock(side_effect=[0, 1, 5]); tick = Tick(seconds=3, clock=clock)
        self.assertTrue(tick.allow('mathesis', 'one')); self.assertFalse(tick.allow('mathesis', 'two'))
        self.assertEqual([r['allowed'] for r in self.records()], [True, False]); self.assertEqual(clock.call_count, 3)

    def test_pointer_boundaries_and_documented_hook_do_not_upload(self):
        recording.pointer('start', {'generation': 1}); recording.pointer('end', {'generation': 2})
        self.assertEqual([r['boundary'] for r in self.records()], ['start', 'end'])
        self.assertEqual(len(recording.pending_uploads()), 1)
        self.assertEqual(next(self.path.glob('*.jsonl')).stat().st_mode & 0o777, 0o600)

    def test_noop_and_failed_save_report_last_committed_pointer(self):
        from types import SimpleNamespace
        ledger = SimpleNamespace(snapshot={'name': 'state-original.sqlite3', 'sha256': 'a' * 64}, generation=3, published_run='synthetic-run', run_history=[])
        save = recording.save_pointer(lambda _: False)
        self.assertFalse(save(ledger))
        def failure(_): raise TimeoutError('synthetic pointer failure')
        with self.assertRaises(TimeoutError): recording.save_pointer(failure)(ledger)
        self.assertEqual([r['pointer']['generation'] for r in self.records()], [3, 3])
        self.assertEqual([r['boundary'] for r in self.records()], ['end', 'end'])

    def test_capability_fingerprint_streams_the_saved_input_in_arrival_order(self):
        import hashlib
        from managed.tasks import Tasks
        from managed.queue import canonical
        from managed.scenario_contract import dump, input_digest, logical
        tasks=Tasks(self.path/'state.sqlite3'); self.addCleanup(tasks.close)
        tasks.db.execute("INSERT INTO pipeline_state VALUES('z',?)",(canonical({'unicode':'α','n':1.0}),))
        tasks.db.execute("INSERT INTO pipeline_state VALUES('a','{}')")
        self.assertEqual(input_digest(tasks),hashlib.sha256(canonical(dump(tasks)).encode()).hexdigest())
        self.assertNotEqual(input_digest(tasks),hashlib.sha256(canonical(logical(dump(tasks))).encode()).hexdigest())

    def test_fingerprint_failure_cannot_change_a_completed_save(self):
        from types import SimpleNamespace
        ledger=SimpleNamespace(snapshot={'name':'state-synthetic.sqlite3','sha256':'a'*64},
            generation=4,published_run='synthetic',run_history=[],version=lambda:1,saved_version=1,tasks=None)
        with patch('managed.scenario_contract.input_digest',side_effect=OSError('synthetic')),patch('sys.stderr',io.StringIO()):
            self.assertTrue(recording.save_pointer(lambda _:True)(ledger))
        self.assertEqual(self.records()[-1]['pointer']['generation'],4)

    def test_process_records_each_retry_and_preserves_failure(self):
        result = subprocess.CompletedProcess(['gh'], 1, b'', b'HTTP 503')
        with patch.object(subprocess, 'run', return_value=result):
            for _ in range(2): self.assertIs(recording.run(['gh', 'release', 'view', 'synthetic']), result)
        self.assertEqual(len(self.records()), 2)
        error = subprocess.CalledProcessError(1, ['git'])
        with patch.object(subprocess, 'run', side_effect=error), self.assertRaises(subprocess.CalledProcessError) as caught:
            recording.run(['git', 'show', 'synthetic'])
        self.assertIs(caught.exception, error)

    def test_base64_read_files_are_redacted_internally(self):
        import base64
        token = 'synthetic-contained-credential'
        with patch.dict(os.environ, {'MATHESIS_ACCESS_TOKEN': token}):
            recording.emit({'response': {'base64': base64.b64encode(json.dumps({'echo': token, 'token': 'unregistered-credential'}).encode()).decode()}})
        value = self.records()[0]['response']['base64']
        decoded = base64.b64decode(value).decode()
        self.assertNotIn(token, decoded); self.assertNotIn('unregistered-credential', decoded)

    def test_real_adapters_use_recorded_http_boundary(self):
        from managed.cma import CMA
        from managed.mathesis import Mathesis
        from managed.mailbox import Graph
        from managed.publication import notes
        clients = [(CMA(token='synthetic-cma'), '/api/v2', 'managed.cma'),
                   (Mathesis('synthetic-mathesis'), '/api/v1/me', 'managed.mathesis'),
                   (Graph('synthetic-graph'), '/me', 'managed.mailbox')]
        for client, path, module in clients:
            opener = Mock(); opener.open.return_value = Response(b'{"ok":true}')
            with patch(module + '.build_opener', return_value=opener): self.assertEqual(client.request(path), {'ok': True})
        opener = Mock(); opener.open.return_value = Response(b'{"ok":true}')
        with patch.object(notes.w, 'build_opener', return_value=opener):
            self.assertEqual(notes.w.Zenodo('synthetic-zenodo').request('GET', '/deposit/depositions'), {'ok': True})
        self.assertEqual([r['adapter'] for r in self.records()], ['cma', 'mathesis', 'mailbox', 'zenodo'])

    def test_cli_body_digest_matches_http_without_temporary_path_identity(self):
        body = {'conversation': 'synthetic-agent', 'input': 'synthetic prompt', 'stream': True}
        for name in ('one.json', 'two.json'):
            path = self.path / name; path.write_text(json.dumps(body))
            call = recording.command_call(['nyxid', 'proxy', 'request', 'cma', 'api/v2/responses', '--method', 'POST', '--data', '@' + str(path), '--header', 'Idempotency-Key:original-key'])
            expected = recording.descriptor('cma', 'POST', '/api/v2/responses', body, 'original-key')
            self.assertEqual(call, expected)

    def test_path_template_retains_query_keys_and_erases_resource_ids(self):
        template = recording.path_template('/api/v1/submissions/synthetic/delivery?version=2')
        self.assertEqual(template, '/api/v1/submissions/{id}/delivery?version=%7Bvalue%7D')

    def test_hex_tokens_and_cli_headers_including_unregistered_values(self):
        token = 'synthetic-secret/42+'
        with patch.dict(os.environ, {'GH_TOKEN': token}):
            recording.emit({'value': [token.encode().hex(), token.encode().hex().upper(), ''.join(c.upper() if i % 2 else c for i,c in enumerate(token.encode().hex()))]})
        headers = ['Authorization: synthetic-auth', 'X-Api-Key: synthetic-key',
                   'Cookie: session=synthetic-cookie', 'X-Access-Token: synthetic-access']
        result = subprocess.CompletedProcess(['gh'], 0, b'synthetic-auth synthetic-key session=synthetic-cookie synthetic-access', b'')
        with patch.object(subprocess, 'run', return_value=result):
            recording.run(['gh', 'api', 'repos/fixture/repo', *sum((['-H', h] for h in headers), [])])
        raw = next(self.path.glob('*.jsonl')).read_text()
        for value in [token.encode().hex(), token.encode().hex().upper(),
                      'synthetic-auth', 'synthetic-key', 'synthetic-cookie', 'synthetic-access']:
            self.assertNotIn(value, raw)

    def test_known_hex_secret_masks_uppercase_and_mixed_case(self):
        token = 'a0b1c2d3e4f56789abcdef0123456789ab'
        mixed = ''.join(c.upper() if i % 2 else c for i, c in enumerate(token))
        secret_context = run_log.SECRETS.set({token})
        self.addCleanup(run_log.SECRETS.reset, secret_context)
        recording.emit({'value': [token, token.upper(), mixed]})
        row, = self.records()
        self.assertEqual(row['value'], ['[redacted]'] * 3)

    def test_unregistered_credentials_in_json_string_values_are_masked(self):
        values = [
            '{"Authorization":"token github_pat_synthetic_unregistered"}',
            '{"token":"synthetic-unregistered-token"}',
            json.dumps({'nested': ['{"api_key":"synthetic-unregistered-key"}'], 'count': 1}),
        ]
        recording.emit({'response': values, 'unchanged': '{ "count" : 1 }'})
        row, = self.records()
        self.assertEqual(json.loads(row['response'][0]), {'Authorization': '[redacted]'})
        self.assertEqual(json.loads(row['response'][1]), {'token': '[redacted]'})
        self.assertEqual(json.loads(json.loads(row['response'][2])['nested'][0]), {'api_key': '[redacted]'})
        self.assertEqual(row['unchanged'], '{ "count" : 1 }')
        raw = next(self.path.glob('*.jsonl')).read_text()
        for token in ('github_pat_synthetic_unregistered', 'synthetic-unregistered-token', 'synthetic-unregistered-key'):
            self.assertNotIn(token, raw)

    def test_short_cookie_values_do_not_mask_counts_or_digests(self):
        digest = 'a1' * 32
        for header in ('Cookie: session=1; flag=1', 'Set-Cookie: flag=1; Path=/', 'Cookie: 1'):
            with self.subTest(header=header):
                result = subprocess.CompletedProcess(['gh'], 0, json.dumps({
                    'count': 1, 'version': '1', 'digest': digest, 'echo': header}).encode(), b'')
                with patch.object(subprocess, 'run', return_value=result):
                    self.assertIs(recording.run(['gh', 'api', 'repos/fixture/repo', '-H', header]), result)
                row = self.records()[-1]
                self.assertEqual(row['schema_version'], 1)
                self.assertEqual(row['response_body']['count'], 1)
                self.assertEqual(row['response_body']['version'], '1')
                self.assertEqual(row['response_body']['digest'], digest)
                self.assertNotIn(header, json.dumps(row))
                self.assertIn('[redacted]', row['response_body']['echo'])
                self.assertNotIn('1', recording.command_secrets(['gh', '-H', header]))

    def test_large_ledger_frame_uses_digests_and_only_opened_files(self):
        from types import SimpleNamespace
        from managed.tasks import Tasks
        from managed.scenario_contract import table_digests, dump, logical
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            tasks = Tasks(root / 'state.sqlite3'); self.addCleanup(tasks.close)
            from test_tasks import request
            for index in range(300):
                row = tasks.add(request('synthetic-' + str(index)))
                tasks.update(row['id'], row['revision'], row['state'], {'payload': 'x' * 8192})
            (root / 'tables').mkdir(); (root / 'managed').mkdir()
            (root / 'tables/read.json').write_text('{"synthetic":true}')
            (root / 'managed/unread.txt').write_bytes(b'x' * (2 * recording.MAX_STAGE_FRAME_BYTES))
            ledger = SimpleNamespace(tasks=tasks, releases=SimpleNamespace(repo='fixture/repo', tag='synthetic'), save=lambda: None)
            @recording.mathesis_stage
            def stage(ledger, config, root, *args):
                self.assertEqual(json.loads((root / 'tables/read.json').read_text()), {'synthetic': True})
            stage(ledger, {}, root)
            record, = self.records()
            self.assertEqual(record['event'], 'mathesis_stage')
            self.assertLessEqual(len((recording.canonical(record) + '\n').encode()), recording.MAX_STAGE_FRAME_BYTES)
            self.assertNotIn('initial_rows', record['case'])
            self.assertEqual(set(record['case']['initial_files']), {'tables/read.json'})
            self.assertEqual(table_digests(tasks), {k: __import__('hashlib').sha256(recording.canonical(v).encode()).hexdigest() for k,v in logical(dump(tasks)).items()})

    def test_oversized_frame_is_explicitly_unavailable(self):
        from types import SimpleNamespace
        from managed.tasks import Tasks
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); tasks = Tasks(root / 'state.sqlite3'); self.addCleanup(tasks.close)
            ledger = SimpleNamespace(tasks=tasks, releases=SimpleNamespace(repo='fixture/repo', tag='synthetic'), save=lambda: None)
            @recording.mathesis_stage
            def stage(ledger, config, root, *args):
                recording._STAGE.get()['script'].append({'read': 'x' * recording.MAX_STAGE_FRAME_BYTES})
            stage(ledger, {}, root)
            record, = self.records()
            self.assertEqual(record['event'], 'mathesis_stage_unavailable')
            self.assertLess(len(recording.canonical(record)), 4096)


if __name__ == '__main__': unittest.main()
