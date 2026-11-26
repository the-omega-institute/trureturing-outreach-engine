#!/usr/bin/env python3
"""Execute language-neutral scripts against the current Python engine, offline."""
import base64
from contextlib import ExitStack, contextmanager, redirect_stderr, redirect_stdout
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
    diagnostic = getattr(error, 'github_diagnostic', None) or publication.github_failure(error)
    if diagnostic: value['diagnostic'] = diagnostic
    if isinstance(error, HTTPError):
        value.update(status=error.code, body=base64.b64encode(error.read()).decode())
        if error.fp is not None: error.fp.seek(0)
    return value


def raise_error(value):
    if value['type'] == 'CMARequestError':
        from managed.cma import CMARequestError
        raise CMARequestError(value['status'], value.get('cma_body', value['message']))
    if value['type'] == 'HTTPError':
        raise HTTPError('https://service.example.invalid', value['status'], value['message'].removeprefix('HTTP Error ' + str(value['status']) + ': '), {}, io.BytesIO(base64.b64decode(value['body'])))
    classes = {'TimeoutError': TimeoutError, 'ValueError': ValueError, 'GateError': GateError,
               'SystemExit': SystemExit, 'KeyboardInterrupt': KeyboardInterrupt, 'TypeError': TypeError, 'ConnectionError': ConnectionError, 'KeyError': KeyError, 'NotSent': __import__('managed.transports', fromlist=['NotSent']).NotSent, 'DefiniteRejection': __import__('managed.transports', fromlist=['DefiniteRejection']).DefiniteRejection, 'ProcessDeath': type('ProcessDeath', (BaseException,), {}), 'RuntimeError': RuntimeError, 'OSError': OSError, 'AssertionError': AssertionError, 'Stopped': type('Stopped', (BaseException,), {})}
    error = classes[value['type']](value['message'])
    if value.get('diagnostic'): error.github_diagnostic = value['diagnostic']
    raise error



def writes(script):
    methods = {'retire_sandbox', 'save_draft', 'submit', 'submit_tracked', 'discard_draft', 'delete_draft', 'move_draft_to_deleted', 'create_draft', 'deliver', 'deliver_pre_reviewed', 'withdraw', 'upload', 'create', 'register_note', 'save_state', 'delete_asset', 'commit_tables'}
    return [{k: v for k, v in e.items() if k in {'adapter', 'method', 'args', 'kwargs'}}
            for e in script if e['method'] in methods or (e['adapter'] == 'cma' and e['method'] == 'request' and len(e['args']) > 1 and e['args'][1] != 'GET')]


class Script:
    def __init__(self, entries, directory):
        self.entries, self.directory, self.index, self.calls = entries, directory, 0, []
        self.failure = False
    def call(self, adapter, method, args, kwargs):
        def normalized(v):
            if isinstance(v, (set, frozenset)): return sorted(v)
            if isinstance(v, dict): return {k: normalized(x) for k,x in v.items()}
            return v
        actual = {'adapter': adapter, 'method': method, 'args': encode(args), 'kwargs': encode(normalized(kwargs))}
        assert self.index < len(self.entries), f'unexpected {adapter}.{method}'
        expected = self.entries[self.index]; self.index += 1
        if actual != {k: expected[k] for k in actual}:
            self.failure = True
            raise AssertionError(f'script mismatch at {self.index}: {adapter}.{method}')
        self.calls.append(actual)
        if 'download' in expected:
            target = Path(args[args.index('--dir') + 1]) / expected['download']['name']
            target.write_bytes(base64.b64decode(expected['download']['base64']))
        if method == 'submit_tracked':
            while self.index < len(self.entries) and self.entries[self.index]['adapter'] == 'boundary' and self.entries[self.index]['method'] == 'send_call':
                entry = self.entries[self.index]
                self.call('boundary', 'send_call', tuple(entry['args']), {})
                args[2](entry['args'][0])
        if 'error' in expected: raise_error(expected['error'])
        value = decode(expected.get('result'), self.directory)
        if adapter == 'mailbox':
            from pool_delivery import mail_value
            value = mail_value(value)
        return value
    def adapter(self, name, attrs=None): return Fake(self, name, attrs or {})


class Fake:
    def __init__(self, script, name, attrs): self.script, self.name, self.attrs = script, name, attrs
    def __getattr__(self, method):
        if method in self.attrs:
            if self.name == 'mailbox' and type(self.attrs[method]) is bool:
                if not self.attrs[method]: raise AttributeError(method)
                return lambda *a, **kw: self.script.call(self.name, method, a, kw)
            return self.attrs[method]
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
        if 'items' in case['initial_rows']:
            from managed.queue import Queue
            Queue(tasks.path, {'reply': 'e'*64}).close()
        stack.callback(tasks.close); load(tasks, case['initial_rows'])
        if case['stage'] in {'pool', 'delivery'}:
            times = iter(case.get('update_times', [])); original_update = tasks.update
            def update(*a, **kw):
                pair = next(times); clock = tasks.clock; points = iter(pair); tasks.clock = lambda: next(points)
                try: return original_update(*a, **kw)
                finally: tasks.clock = clock
            stack.enter_context(patch.object(tasks, 'update', update))
        script = Script(case['script'], root)
        ledger = SimpleNamespace(clock=lambda: case['now'], tasks=tasks, releases=script.adapter('releases', case['adapter_attributes'].get('releases')), save=lambda *a, **kw: script.call('ledger', 'save', a, kw))
        if 'saved_input' in case: ledger._recording_saved_input = case['saved_input']
        if '_recording_baseline' in case: ledger._recording_baseline = case['_recording_baseline']
        for target, value in case.get('constants', {}).items(): stack.enter_context(patch(target, value))
        stack.enter_context(patch('managed.control.workflow', side_effect=lambda k, *a, **kw: case.get('workflows', {}).get(k, case['workflow'])))
        stack.enter_context(patch('managed.sent_events.commit_tables', lambda *a, **kw: script.call('registry', 'commit_tables', (), {})))
        stack.enter_context(patch('managed.sent_events.tables_need_push', lambda *a, **kw: script.call('registry', 'needs_push', (), {})))
        # Network/process calls are forbidden even if the script is incomplete.
        stack.enter_context(patch('urllib.request.OpenerDirector.open', side_effect=AssertionError('network forbidden')))
        stack.enter_context(patch('subprocess.run', side_effect=AssertionError('process forbidden')))
        stack.enter_context(patch('subprocess.check_output', side_effect=AssertionError('process forbidden')))
        if case['stage'] == 'publication':
            from managed import pre_reviewed
            from managed.queue import digest
            def package(row, directory, releases):
                from managed import recording
                args = (row['id'], digest(row['data']['result']))
                response = recording._capture('shortnote', 'package', lambda *a: script.call('shortnote', 'package', a, {}), args, {}) if recording.enabled() else script.call('shortnote', 'package', args, {})
                prepared = Path(directory) / 'prepared'; prepared.mkdir(exist_ok=True)
                for name, data in response['files'].items():
                    path = decode(data, prepared)
                    (prepared / name).write_bytes(path.read_bytes())
                    if name == response['source']: (Path(directory) / name).write_bytes(path.read_bytes())
                return response['metadata'], prepared, response['source']
            stack.enter_context(patch.object(publication, 'package', package))
            def boundary(module, method, args_key):
                function = getattr(module, method)
                def call(*a, **kw):
                    script.call('boundary', method, args_key(a, kw), {})
                    return function(*a, **kw)
                stack.enter_context(patch.object(module, method, call))
            enqueue = publication.notes.enqueue
            def enqueue_observed(*a, **kw):
                try: result = enqueue(*a, **kw)
                finally: script.call('boundary', 'enqueue', (), {})
                return result
            stack.enter_context(patch.object(publication.notes, 'enqueue', enqueue_observed))
            boundary(publication, 'source_gate', lambda a, k: (a[0]['id'],))
            boundary(pre_reviewed, 'select', lambda a, k: (a[0]['id'],))
            boundary(mathesis_delivery, 'ingest', lambda a, k: (a[1]['task_id'],))
            boundary(publication, 'record', lambda a, k: (a[1], a[2]['phase']))
        if case['stage'] == 'sent':
            from managed import registry
            def render(*a, **kw):
                response = script.call('registry', 'render', (registry.read(root, 'contacts'),), {})
                for name, content in response.items():
                    target = root / name; target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes(base64.b64decode(content))
            stack.enter_context(patch.object(registry, 'render', render))
        if case['stage'] == 'delivery':
            from managed import delivery
            def register(policy, directory):
                from managed import recording
                args = (policy.get('mail_thread_contacts', []),)
                response = recording._capture('registry','register',lambda *a: script.call('registry','register',a,{}),args,{}) if recording.enabled() else script.call('registry','register',args,{})
                for name, content in response.items():
                    target = root / name; target.parent.mkdir(parents=True, exist_ok=True); target.write_bytes(base64.b64decode(content))
            stack.enter_context(patch.object(delivery, '_register_reply_evidence', register))
        box = script.adapter('mailbox', case['adapter_attributes'].get('mailbox')) if case.get('options', {}).get('mailbox', True) else None
        stack.enter_context(patch.object(sent_events, 'MAX_PAGES', case.get('options', {}).get('max_pages', 2)))
        if case.get('admissions'):
            admissions = iter(case['admissions'])
            class Budget:
                pending = {}
                count = 0
                exhausted = False
                previous = {case['stage']: case.get('options', {}).get('previous', [])}
                clock = staticmethod(lambda: case['now'])
                @contextmanager
                def unit(self, lane, key, step=None):
                    yield self.allow(lane, key)
                def allow(self, lane, key):
                    entry = next(admissions)
                    assert (entry['lane'], entry['key']) == (lane, key)
                    return entry['allowed']
            ledger.tick = Budget()
        error = None
        try:
            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                if case['stage'] == 'pool':
                    from pool_delivery import pool
                    pool(case, ledger, script)
                elif case['stage'] == 'delivery' and case.get('operation') in {'mathesis-once','mathesis-introduced'}:
                    if case['operation'] == 'mathesis-once': result = mailbox.GraphMailbox._require_mathesis_not_introduced(box, *case['arguments'])
                    else: result = mailbox.mathesis_introduced(*case['arguments'])
                    assert encode(result) == case.get('expected_result')
                elif case['stage'] == 'delivery':
                    from managed import delivery
                    from managed.queue import Queue
                    queue_init = Queue.__init__
                    with patch.object(Queue, '__init__', lambda self, path, workflows, reviewer='claude-loop', clock=None, checkpoint=lambda: None: queue_init(self, path, workflows, reviewer, clock or (lambda: 2_000_000_000.0), checkpoint)):
                        delivery.advance(ledger, case['config'], root, box)
                elif case['stage'] == 'mathesis': mathesis_delivery.advance(ledger, case['config'], root, folder, script.adapter('mathesis'), KEY, script.adapter('publication'), **{k: v for k,v in case.get('options', {}).items() if k in {'again', 'attempted'}})
                elif case['stage'] == 'intake': mailbox.intake(ledger, case['config'], root, box, revision=case['revision'], now=datetime.fromisoformat(case['datetime']))
                elif case['stage'] == 'sent':
                    if case.get('options', {}).get('advance'):
                        actual = sent_events.reconcile
                        def reconcile(*a, **kw):
                            return actual(*a, now=datetime.fromisoformat(case['datetime']), **kw)
                        with patch.object(sent_events, 'reconcile', reconcile): sent_events.advance(ledger, case['config'], root, box)
                    else: sent_events.reconcile(ledger, case['config'], root, box, datetime.fromisoformat(case['datetime']))
                elif case['stage'] == 'publication': publication.advance(ledger, case['config'], root, folder, script.adapter('publication'), KEY)
                else: raise AssertionError('unknown stage')
        except BaseException as exc:
            if case['stage'] in {'pool','delivery'}:
                from pool_delivery import error_value as stage_error_value
                error = stage_error_value(exc)
            else: error = error_value(exc)
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
