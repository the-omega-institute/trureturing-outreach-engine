#!/usr/bin/env python3
"""Execute language-neutral scripts against the current Python engine, offline."""
import base64
from contextlib import ExitStack, contextmanager, redirect_stderr, redirect_stdout
from datetime import datetime
import io
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
from types import SimpleNamespace
from unittest.mock import patch
from urllib.error import HTTPError, URLError

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from managed import mathesis_delivery, mailbox, publication, sent_events, control
from managed.tasks import Tasks
from managed.queue import canonical, GateError

from managed.scenario_contract import NOW, dump, logical, load as load_tables, encode, decode, files
KEY = 'secret'


def load(tasks, tables):
    # pipeline_state.payload is JSON even when its logical value is a string
    # (signed prior-work backfills store the exact result digest that way).
    # Queue readback's historical empty SQL string remains a raw column value.
    tables = json.loads(json.dumps(tables))
    for row in tables.get('pipeline_state', {}).get('rows', []):
        if isinstance(row['payload'], str): row['payload'] = canonical(row['payload'])
    load_tables(tasks, tables)


def error_value(error):
    value = {'type': type(error).__name__, 'message': str(error)}
    for name in ('attachments','intake_reason'):
        if hasattr(error,name): value[name]=getattr(error,name)
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
    if value['type'] == 'URLError': raise URLError(value.get('reason', value['message'].removeprefix('<urlopen error ').removesuffix('>')))
    if value['type'] == 'JSONDecodeError' and 'doc' in value: raise json.JSONDecodeError(value['msg'], value['doc'], value['pos'])
    if value['type'] == 'UnsupportedThreadAttachments': raise mailbox.UnsupportedThreadAttachments(value['attachments'])
    if value['type'] == 'SentSubjectsDeferred': raise mailbox.SentSubjectsDeferred(value['intake_reason'], value['message'])
    if value['type'] == 'UnparseableEvidence': raise __import__('managed.prior_work_evidence',fromlist=['UnparseableEvidence']).UnparseableEvidence(value['message'])
    classes = {'FileNotFoundError':FileNotFoundError, 'PermissionError':PermissionError, 'TimeoutError' : TimeoutError, 'ValueError': ValueError, 'GateError': GateError,
               'SystemExit': SystemExit, 'KeyboardInterrupt': KeyboardInterrupt, 'TypeError': TypeError, 'ConnectionError': ConnectionError, 'KeyError': KeyError, 'NotSent': __import__('managed.transports', fromlist=['NotSent']).NotSent, 'DefiniteRejection': __import__('managed.transports', fromlist=['DefiniteRejection']).DefiniteRejection, 'ProcessDeath': type('ProcessDeath', (BaseException,), {}), 'RuntimeError': RuntimeError, 'OSError': OSError, 'AssertionError': AssertionError, 'Stopped': type('Stopped', (BaseException,), {})}
    if value['type'] not in classes and value.get('bases'):
        import builtins
        parents = {**vars(builtins), **classes,
                   'UnparseableEvidence': __import__('managed.prior_work_evidence', fromlist=['UnparseableEvidence']).UnparseableEvidence,
                   'UnsupportedThreadAttachments': mailbox.UnsupportedThreadAttachments,
                   'SentSubjectsDeferred': mailbox.SentSubjectsDeferred}
        parent = next(parents[name] for name in value['bases'] if name in parents)
        custom = type(value['type'], (parent,), {})
        if issubclass(custom, mailbox.UnsupportedThreadAttachments): error = custom(value['attachments'])
        elif issubclass(custom, mailbox.SentSubjectsDeferred): error = custom(value['intake_reason'], value['message'])
        else: error = custom(value['message'])
        raise error
    error = classes[value['type']](value['message'])
    if value.get('diagnostic'): error.github_diagnostic = value['diagnostic']
    raise error



def writes(script):
    methods = {'upload_task_batch', 'retire_sandbox', 'save_draft', 'submit', 'submit_tracked', 'discard_draft', 'delete_draft', 'move_draft_to_deleted', 'create_draft', 'deliver', 'deliver_pre_reviewed', 'withdraw', 'upload', 'create', 'register_note', 'save_state', 'delete_asset', 'commit_tables'}
    return [{k: v for k, v in e.items() if k in {'adapter', 'method', 'args', 'kwargs'}}
            for e in script if e['method'] in methods or (e['adapter'] in {'cma','github'} and e['method'] == 'request' and len(e['args']) > 1 and e['args'][1] != 'GET')]


class Script:
    def __init__(self, entries, directory):
        self.entries, self.directory, self.index, self.calls = entries, directory, 0, []
        self.failure = False
    def call(self, adapter, method, args, kwargs):
        def normalized(v):
            if isinstance(v, (set, frozenset)): return sorted(v)
            if isinstance(v, dict): return {k: normalized(x) for k,x in v.items()}
            return v
        encoded_args=encode(args) if method!='upload_task_batch' else [ [[encode(path),ref] for path,ref in args[0].items()], *encode(args[1:]) ]
        actual = {'adapter': adapter, 'method': method, 'args': encoded_args, 'kwargs': encode(normalized(kwargs))}
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
        if method=='upload_task_batch':
            args[2].clear();args[2].update(value['listings']); return None
        if adapter == 'mailbox':
            from pool_delivery import mail_value
            value = mail_value(value)
        return value
    def adapter(self, name, attrs=None): return Fake(self, name, attrs or {})


class Fake:
    def __init__(self, script, name, attrs): self.script, self.name, self.attrs = script, name, attrs
    @property
    def __class__(self):
        return mailbox.GraphMailbox if self.name=='mailbox' and self.attrs.get('graph_mailbox') else type(self)
    def __getattr__(self, method):
        if self.name=='mailbox' and self.attrs.get('native_matching') and method in {'match_inbound_sent','_refresh_sent_subjects'}:
            return getattr(mailbox.GraphMailbox,method).__get__(self)
        if self.name=='mailbox' and method=='_registered_recipient':return mailbox.GraphMailbox._registered_recipient
        if method in self.attrs:
            if self.name == 'mailbox' and type(self.attrs[method]) is bool:
                if not self.attrs[method]: raise AttributeError(method)
                return lambda *a, **kw: self.script.call(self.name, method, a, kw)
            return self.attrs[method]
        if not any(e['adapter'] == self.name and e['method'] == method for e in self.script.entries):
            raise AttributeError(method)
        return lambda *a, **kw: self.script.call(self.name, method, a, kw)


def execute(case, *, capture=False):
    """Replay checked fixtures; capture=True extracts observations from Python only."""
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
        used_times = []
        if case['stage'] in {'pool', 'delivery', 'intake','inbound','formalization','prior-work'}:
            times = iter(case.get('update_times', [])); original_update = tasks.update
            def update(*a, **kw):
                pair = next(times); used_times.append(pair); clock = tasks.clock; points = iter(pair); tasks.clock = lambda: next(points)
                try: return original_update(*a, **kw)
                finally: tasks.clock = clock
            stack.enter_context(patch.object(tasks, 'update', update))
        script = Script(case['script'], root)
        ledger = SimpleNamespace(folder=folder, clock=lambda: case['now'], tasks=tasks, releases=script.adapter('releases', case['adapter_attributes'].get('releases')), save=lambda *a, **kw: script.call('ledger', 'save', a, kw))
        if 'saved_input' in case: ledger._recording_saved_input = case['saved_input']
        if '_recording_baseline' in case: ledger._recording_baseline = case['_recording_baseline']
        for target, value in case.get('constants', {}).items(): stack.enter_context(patch(target, value))
        if case.get('binding'): stack.enter_context(patch('managed.control.request_binding',return_value=case['binding']))
        if 'data_repo' in case: stack.enter_context(patch('managed.prior_work_evidence.DATA_REPO',case['data_repo']))
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
        if case['stage']=='prior-work':
            def evidence_download(row,name,directory,releases):
                from managed import recording
                args=(row['id'],name,Path(directory))
                return recording._capture('evidence','download',lambda *a:script.call('evidence','download',a,{}),args,{}) if recording.enabled() else script.call('evidence','download',args,{})
            stack.enter_context(patch.object(publication,'download',evidence_download))
        box = script.adapter('mailbox', case['adapter_attributes'].get('mailbox')) if case.get('options', {}).get('mailbox', True) else None
        stack.enter_context(patch.object(sent_events, 'MAX_PAGES', case.get('options', {}).get('max_pages', 2)))
        if case.get('admissions'):
            admissions = iter(case['admissions'])
            class Budget:
                pending = {}
                count = 0
                exhausted = False
                previous = case.get('previous_units') or {{'intake':'mailbox-intake','formalization':'formalization-requests','prior-work':'prior-work-issues'}.get(case['stage'],case['stage']): case.get('options', {}).get('previous', [])}
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
        result = None
        try:
            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                if case['stage']=='inbound' and case.get('operation') in {'coverage','build_inbound_request'}:
                    from managed import thread_staging
                    if case['operation']=='coverage':result=thread_staging.require_uncovered_inbound(case['arguments'][0],tasks)
                    else:result=thread_staging.build_inbound_request(*case['arguments'],root,revision=case.get('binding',{}).get('source_revision','b'*40))
                    if not capture: assert encode(result)==case['expected_result']
                elif case['stage'] in {'formalization','prior-work'} and case.get('operation','advance')!='advance':
                    from managed import formalization_requests,prior_work_issues,prior_work_evidence
                    operation=case['operation']
                    if operation=='plan':
                        local=None
                        if case.get('local_files') is not None:
                            local=root/'evidence';local.mkdir()
                            for name,data in case['local_files'].items():
                                path=local/name;assert path.resolve().is_relative_to(local.resolve());path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(base64.b64decode(data))
                        result=prior_work_issues.plan(case['arguments'][0],root,local)
                    else:
                        target=formalization_requests if case['stage']=='formalization' else prior_work_evidence
                        result=getattr(target,operation)(*case['arguments'])
                    if not capture: assert encode(result)==case['expected_result']
                elif case['stage'] == 'pool':
                    from pool_delivery import pool
                    pool(case, ledger, script)
                elif case['stage'] == 'delivery' and case.get('operation') in {'mathesis-once','mathesis-introduced'}:
                    if case['operation'] == 'mathesis-once': result = mailbox.GraphMailbox._require_mathesis_not_introduced(box, *case['arguments'])
                    else: result = mailbox.mathesis_introduced(*case['arguments'])
                    if not capture: assert encode(result) == case.get('expected_result')
                elif case['stage'] == 'delivery':
                    from managed import delivery
                    from managed.queue import Queue
                    queue_init = Queue.__init__
                    with patch.object(Queue, '__init__', lambda self, path, workflows, reviewer='claude-loop', clock=None, checkpoint=lambda: None: queue_init(self, path, workflows, reviewer, clock or (lambda: 2_000_000_000.0), checkpoint)):
                        delivery.advance(ledger, case['config'], root, box)
                elif case['stage'] == 'mathesis': mathesis_delivery.advance(ledger, case['config'], root, folder, script.adapter('mathesis'), KEY, script.adapter('publication'), **{k: v for k,v in case.get('options', {}).items() if k in {'again', 'attempted'}})
                elif case['stage'] == 'intake': mailbox.intake(ledger, case['config'], root, box, revision=case['revision'], now=datetime.fromisoformat(case['datetime']))
                elif case['stage']=='inbound':
                    from managed import thread_staging
                    result=thread_staging.resolve(case['row'],box,ledger,root)
                    if not capture: assert encode(result)==case['expected_result']
                elif case['stage']=='formalization':
                    from managed import formalization_requests
                    github=(lambda *a,**kw:script.call('github','request',a,kw)) if case['options']['github'] else None
                    formalization_requests.advance(ledger,case['config'],github)
                elif case['stage']=='prior-work':
                    from managed import prior_work_issues
                    github=(lambda *a,**kw:script.call('github','request',a,kw)) if case['options']['github'] else None
                    prior_work_issues.advance(ledger,case['config'],root,github)
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
            if os.environ.get("OUTREACH_SCENARIO_DEBUG"): __import__("traceback").print_exc()
            if case['stage'] in {'pool','delivery'}:
                from pool_delivery import error_value as stage_error_value
                error = stage_error_value(exc)
                for name in ('attachments','intake_reason'):
                    if hasattr(exc,name):error[name]=getattr(exc,name)
            else: error = error_value(exc)
        if capture:
            assert not script.failure and script.index == len(script.entries)
            if case.get('admissions'): assert next(admissions, None) is None, 'unused admissions'
            return dict(update_times=used_times, error=error, expected_result=encode(result), expected_final_rows=logical(dump(tasks)),
                        expected_final_files=files(root), expected_writes=writes(script.calls))
        if case.get('admissions'): assert next(admissions, None) is None, 'unused admissions'
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
