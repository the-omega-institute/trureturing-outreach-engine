#!/usr/bin/env python3
"""Execute language-neutral scripts against the current Python engine, offline."""
import base64
from contextlib import ExitStack, redirect_stderr, redirect_stdout
from datetime import datetime
import io
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
from types import SimpleNamespace
from unittest.mock import patch
from urllib.error import HTTPError

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from managed import mathesis_delivery, mailbox, publication, sent_events, control
from managed.tasks import Tasks
from managed.queue import canonical, GateError

from managed.scenario_contract import NOW, dump, logical, load, encode, decode, files
KEY = 'secret'


def error_value(error):
    value = {'type': type(error).__name__, 'message': str(error)}
    if isinstance(error, HTTPError):
        value.update(status=error.code, body=base64.b64encode(error.read()).decode())
        if error.fp is not None: error.fp.seek(0)
    return value


def raise_error(value):
    if value['type'] == 'HTTPError':
        raise HTTPError('https://service.example.invalid', value['status'], 'scripted', {}, io.BytesIO(base64.b64decode(value['body'])))
    classes = {'TimeoutError': TimeoutError, 'ValueError': ValueError, 'GateError': GateError,
               'RuntimeError': RuntimeError, 'AssertionError': AssertionError}
    raise classes[value['type']](value['message'])



def writes(script):
    methods = {'deliver', 'deliver_pre_reviewed', 'withdraw', 'upload', 'create', 'register_note', 'save_state', 'delete_asset', 'commit_tables'}
    return [{k: v for k, v in e.items() if k in {'adapter', 'method', 'args', 'kwargs'}}
            for e in script if e['method'] in methods]


class Script:
    def __init__(self, entries, directory):
        self.entries, self.directory, self.index, self.calls = entries, directory, 0, []
        self.failure = False
    def call(self, adapter, method, args, kwargs):
        actual = {'adapter': adapter, 'method': method, 'args': encode(args), 'kwargs': encode(kwargs)}
        assert self.index < len(self.entries), f'unexpected {adapter}.{method}'
        expected = self.entries[self.index]; self.index += 1
        if actual != {k: expected[k] for k in actual}:
            self.failure = True
            raise AssertionError(f'script mismatch at {self.index}: {adapter}.{method}')
        self.calls.append(actual)
        if 'download' in expected:
            target = Path(args[args.index('--dir') + 1]) / expected['download']['name']
            target.write_bytes(base64.b64decode(expected['download']['base64']))
        if 'error' in expected: raise_error(expected['error'])
        return decode(expected.get('result'), self.directory)
    def adapter(self, name, attrs=None): return Fake(self, name, attrs or {})


class Fake:
    def __init__(self, script, name, attrs): self.script, self.name, self.attrs = script, name, attrs
    def __getattr__(self, method):
        if method in self.attrs: return self.attrs[method]
        if not any(e['adapter'] == self.name and e['method'] == method for e in self.script.entries):
            raise AttributeError(method)
        return lambda *a, **kw: self.script.call(self.name, method, a, kw)


def execute(case):
    with tempfile.TemporaryDirectory(prefix='outreach-scenario-') as temporary, ExitStack() as stack:
        root = Path(temporary).resolve(); folder = root / 'ledger'; folder.mkdir()
        for name, content in case['initial_files'].items():
            path = root / name
            assert path.resolve().is_relative_to(root)
            path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(base64.b64decode(content))
        tasks = Tasks(folder / 'state.sqlite3', clock=lambda: case['now'])
        stack.callback(tasks.close); load(tasks, case['initial_rows'])
        script = Script(case['script'], root)
        ledger = SimpleNamespace(clock=lambda: case['now'], tasks=tasks, releases=script.adapter('releases', case['adapter_attributes'].get('releases')), save=lambda *a, **kw: script.call('ledger', 'save', a, kw))
        for target, value in case.get('constants', {}).items(): stack.enter_context(patch(target, value))
        stack.enter_context(patch('managed.control.workflow', return_value=case['workflow']))
        stack.enter_context(patch('managed.sent_events.commit_tables', lambda *a, **kw: script.call('registry', 'commit_tables', (), {})))
        # Network/process calls are forbidden even if the script is incomplete.
        stack.enter_context(patch('urllib.request.OpenerDirector.open', side_effect=AssertionError('network forbidden')))
        stack.enter_context(patch('subprocess.run', side_effect=AssertionError('process forbidden')))
        stack.enter_context(patch('subprocess.check_output', side_effect=AssertionError('process forbidden')))
        box = script.adapter('mailbox', case['adapter_attributes'].get('mailbox'))
        error = None
        try:
            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                if case['stage'] == 'mathesis': mathesis_delivery.advance(ledger, case['config'], root, folder, script.adapter('mathesis'), KEY, script.adapter('publication'), **case.get('options', {}))
                elif case['stage'] == 'intake': mailbox.intake(ledger, case['config'], root, box, revision=case['revision'], now=datetime.fromisoformat(case['datetime']))
                elif case['stage'] == 'sent': sent_events.reconcile(ledger, case['config'], root, box, datetime.fromisoformat(case['datetime']))
                elif case['stage'] == 'publication': publication.advance(ledger, case['config'], root, folder, script.adapter('publication'), KEY)
                else: raise AssertionError('unknown stage')
        except Exception as exc: error = error_value(exc)
        assert error == case.get('error'), f"{case['id']}: stage error {error}"
        assert not script.failure and script.index == len(script.entries), f"{case['id']}: unused script entries"
        actual = logical(dump(tasks))
        assert canonical(actual) == canonical(case['expected_final_rows']), f"{case['id']}: final rows differ"
        assert files(root) == case['expected_final_files'], f"{case['id']}: final files differ"
        assert writes(script.calls) == case['expected_writes'], f"{case['id']}: writes differ"
        return actual


def main():
    cases = sorted((Path(__file__).parent / 'cases').glob('*.json'))
    counts = {}
    for path in cases:
        case = json.loads(path.read_text()); execute(case)
        counts[case['stage']] = counts.get(case['stage'], 0) + 1
    assert sum(counts.values()) >= 40
    print(canonical({'passed': sum(counts.values()), 'stages': counts}))


if __name__ == '__main__': main()
