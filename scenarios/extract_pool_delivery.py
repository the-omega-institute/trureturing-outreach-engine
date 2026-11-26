#!/usr/bin/env python3
"""Extract pool operations and mail stages from native tests, preserving assertions.

Pool operations retain their input runtime state as well as the ledger: this
makes placement, health, recovery and operator checks independently replayable.
"""
import copy
from contextlib import ExitStack
from dataclasses import asdict, is_dataclass
import importlib
import json
import os
import hashlib
import sqlite3
from types import SimpleNamespace
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

from runner import ROOT, NOW, dump, logical, files, writes, error_value, encode as base_encode
from extract import Capture
sys.path[:0] = [str(ROOT / 'managed/tests'), str(ROOT / 'scripts/short_notes')]
from managed import control, delivery, sandbox_pool, recording
from managed.queue import Queue
from managed.tasks import Tasks
from urllib.error import HTTPError
import base64

def error_value(error):
    result = {'type': type(error).__name__, 'message': str(error)}
    if isinstance(error, HTTPError):
        raw = error.read() if error.fp is not None and not error.fp.closed else b''
        if error.fp is not None and not error.fp.closed: error.fp.seek(0)
        result.update(status=error.code, body=base64.b64encode(raw).decode())
    elif hasattr(error, 'code'):
        result.update(status=error.code, cma_body=getattr(error, 'body', None))
    return result

class TransparentProxy:
    def __init__(self, capture, name, target):
        object.__setattr__(self, 'capture', capture); object.__setattr__(self, 'name', name); object.__setattr__(self, 'target', target)
    def __getattr__(self, name):
        attr = getattr(self.target, name)
        if callable(attr): return lambda *a, **kw: self.capture.call(self.name, name, attr, a, kw)
        return attr
    def __setattr__(self, name, value): setattr(self.target, name, value)


def encode(value):
    if is_dataclass(value): return {'$type': type(value).__name__, **asdict(value)}
    if isinstance(value, (set, frozenset)): return sorted(encode(x) for x in value)
    if isinstance(value, (tuple, list)): return [encode(x) for x in value]
    if isinstance(value, dict): return {k: encode(v) for k,v in value.items()}
    if hasattr(value, 'tasks'): return {'$ledger': True}
    if hasattr(value, 'request'): return {'$cma': True}
    return base_encode(value)

def workflows(ledger):
    result = {}
    for kind in {r['kind'] for r in ledger.tasks.all()} | {'reply'}:
        try: result[kind] = control.workflow(kind)
        except KeyError: pass
    return result

POOL_FIELDS = ('correspondence', 'max_sandboxes', 'agents_per_sandbox', 'reserve_review_slots',
               'group_limits', 'kind_groups', 'idle_seconds', 'entries', 'failed', 'available',
               'observed', 'attempted', 'errors', 'agent_cleanup_remaining')
POOL_OPS = ('__init__', 'health', 'idle', 'slot', 'assigned', 'remember', 'failed_entry',
            'record_capacity_failure', 'clear_capacity_failure', 'transition')

def runtime(pool):
    return {k: encode(sorted(v) if isinstance(v, (set, frozenset)) else v)
            for k in POOL_FIELDS if hasattr(pool, k) for v in [getattr(pool, k)]}

class StageCapture(Capture):
    def proxy(self, name, target): return TransparentProxy(self, name, target)
    def call(self, name, method, function, args, kwargs):
        # Dataclasses are part of the mailbox port, never opaque Python objects.
        def value(*a, **kw):
            result = function(*a, **kw)
            return result
        entry = {'adapter': name, 'method': method, 'args': encode(args), 'kwargs': encode(kwargs)}
        self.entries.append(entry)
        try:
            if method == 'submit_tracked':
                before = args[2]
                wrapped = (*args[:2], lambda draft: self.call('boundary', 'send_call', before, (draft,), {}))
                result = value(*wrapped, **kwargs)
            else: result = value(*args, **kwargs)
            entry['result'] = encode({'$type': type(result).__name__, **asdict(result)} if is_dataclass(result) else result)
            return result
        except BaseException as error:
            if name == 'boundary' and self.entries[-1] is not entry: entry['result'] = None
            else: entry['error'] = error_value(error)
            raise


def mail_policy(out, mapping):
    import tempfile
    from managed import mailbox
    cls = importlib.import_module('test_engine_split').MathesisOnceTest
    mapping['test_engine_split.MathesisOnceTest.test_reply_rule_tells_agents_to_introduce_mathesis_once'] = []
    for name in ('test_own_paragraph_counts_and_quoted_history_does_not', 'test_preflight_refuses_a_second_introduction_in_another_conversation'):
        identity = 'test_engine_split.MathesisOnceTest.' + name; mapping[identity] = []
        depth = 0
        def wrap(operation, function):
            def invoke(*args):
                nonlocal depth
                if depth: return function(*args)
                capture = StageCapture(); depth += 1
                original = args
                attrs = {}
                if operation == 'mathesis-once':
                    box = args[0]; args = (capture.proxy('mailbox', box), *args[1:])
                    attrs = {'settings': box.settings, 'recipient_history': True, 'message': True}
                case = {'schema_version': 1, 'id': 'delivery-' + name + '-' + str(len(mapping[identity])+1),
                    'source_test': identity, 'stage': 'delivery', 'operation': operation, 'now': NOW,
                    'workflow': 'e'*64, 'config': {}, 'initial_files': {}, 'expected_final_files': {},
                    'adapter_attributes': {'releases': {}, 'mailbox': attrs}, 'options': {'mailbox': True},
                    'arguments': encode(original[1:] if operation == 'mathesis-once' else original)}
                with tempfile.TemporaryDirectory() as directory:
                    tasks = Tasks(Path(directory)/'state.sqlite3'); case['initial_rows'] = dump(tasks)
                    case['expected_final_rows'] = logical(case['initial_rows']); tasks.close()
                caught = None
                try: case['expected_result'] = encode(function(*args))
                except BaseException as error: case['error'] = error_value(error); caught = error
                finally: depth -= 1
                case.update(script=capture.entries, expected_writes=[])
                (out/(case['id']+'.json')).write_text(json.dumps(case,ensure_ascii=False,separators=(',',':'))+'\n')
                mapping[identity].append(case['id'])
                if caught: raise caught
                return case['expected_result']
            return invoke
        with patch.object(mailbox,'mathesis_introduced',wrap('mathesis-introduced',mailbox.mathesis_introduced)), patch.object(mailbox.GraphMailbox,'_require_mathesis_not_introduced',wrap('mathesis-once',mailbox.GraphMailbox._require_mathesis_not_introduced)):
            result=unittest.TestResult(); cls(name).run(result)
            if result.errors or result.failures: raise AssertionError(str(result.errors+result.failures))


def main():
    out = Path(__file__).parent / 'cases'
    mapping_path = Path(__file__).parent / 'test-map.json'
    mapping = json.loads(mapping_path.read_text())
    for path in out.glob('*.json'):
        if json.loads(path.read_text())['stage'] in {'pool', 'delivery'}: path.unlink()
    modules = ['test_sandbox_pool', 'test_sandbox_groups', 'test_pool_adopt', 'test_cleanup_recovery',
               'test_automation', 'test_owner_sent', 'test_draft_revision', 'test_stale_owner_draft',
               'test_thread_staging', 'test_thread_recipients', 'test_correspondence', 'test_reviewer_edit']
    total = 0
    for module_name in modules:
        mod = importlib.import_module(module_name)
        if hasattr(mod, 'setUpModule'): mod.setUpModule()
        suite = unittest.defaultTestLoader.loadTestsFromModule(mod)
        def flatten(suite):
            for item in suite:
                if isinstance(item, unittest.TestSuite): yield from flatten(item)
                else: yield item
        for test in flatten(suite):
            identity = test.id()
            if not identity.startswith(module_name + '.'): continue
            mapping[identity] = []
            depth = 0
            def capture_operation(stage, operation, function, pool, ledger, cfg, root, args, kwargs):
                nonlocal total, depth
                capture = StageCapture()
                case = {'schema_version': 1, 'id': stage + '-' + identity.replace('.', '-') + '-' + str(len(mapping[identity])+1),
                        'source_test': identity, 'stage': stage, 'operation': operation,
                        'now': ledger.tasks.clock(), 'workflow': control.workflow('reply'), 'workflows': workflows(ledger),
                        'config': copy.deepcopy(cfg), 'initial_rows': dump(ledger.tasks),
                        'initial_files': files(root), 'adapter_attributes': {'releases': {}},
                        'options': {'previous': list(getattr(getattr(ledger, 'tick', None), 'previous', {}).get('delivery', []))}}
                snapshot = getattr(ledger, 'snapshot', None)
                raw = getattr(ledger.releases, 'assets', {}).get((snapshot or {}).get('name'))
                if raw is not None:
                    assert hashlib.sha256(raw).hexdigest() == snapshot['sha256']
                    db = sqlite3.connect(':memory:'); db.deserialize(raw)
                    try: saved = dump(SimpleNamespace(db=db))
                    finally: db.close()
                    case['saved_input'] = {'generation': ledger.generation, 'digest': snapshot['sha256'],
                                           'logical_digest': hashlib.sha256(__import__('managed.queue',fromlist=['canonical']).canonical(saved).encode()).hexdigest()}
                if stage == 'pool':
                    case['runtime'] = runtime(pool)
                    case['options']['group_order'] = list(getattr(pool, 'group_limits', {}))
                    case['options']['idle_pause_seconds'] = os.environ.get('OUTREACH_POOL_IDLE_PAUSE_SECONDS', '600')
                    case['arguments'] = encode(args); case['keywords'] = encode(kwargs)
                else:
                    box = args[0] if args else None
                    attrs = {name: callable(getattr(box, name, None)) for name in
                             ('submit_tracked', 'drafts_for', 'recipient_history', 'message', 'sent_items',
                              'drafts_folder_id', 'deleted_items_folder_id', 'verify_owner', 'matches', 'own_draft', 'thread', 'find', 'messages', 'create_draft', 'delete_draft', 'move_draft_to_deleted')}
                    case['adapter_attributes']['mailbox'] = attrs
                    case['options']['mailbox'] = box is not None
                old_save = ledger.save
                def save(*a, **kw):
                    result = capture.call('ledger', 'save', old_save, a, kw)
                    if ledger.snapshot:
                        capture.entries[-1]['saved_snapshot'] = {'generation': ledger.generation, 'digest': ledger.snapshot['sha256']}
                    return result
                frame = {'admissions': [], '_errors': {}}
                token = recording._STAGE.set(frame)
                caught = None
                depth += 1
                with ExitStack() as hooks:
                    hooks.enter_context(patch.object(ledger, 'save', save))
                    # Capture task clocks exactly, including tests that mutate time mid-stage.
                    update_times = []
                    original_update = ledger.tasks.update
                    def update(*a, **kw):
                        result = original_update(*a, **kw)
                        event = ledger.tasks.db.execute('SELECT at FROM task_events ORDER BY seq DESC LIMIT 1').fetchone()
                        update_times.append([result['updated'], event[0]])
                        return result
                    hooks.enter_context(patch.object(ledger.tasks, 'update', update))
                    # Each typed CMA operation is an adapter boundary. Retirement
                    # remains the CMA adapter's bounded stop/delete protocol.
                    if stage == 'delivery':
                        original_register = delivery._register_reply_evidence
                        def register(policy, directory):
                            def perform(evidence):
                                original_register(policy, directory)
                                return files(root)
                            return capture.call('registry', 'register', perform, (policy.get('mail_thread_contacts', []),), {})
                        hooks.enter_context(patch.object(delivery, '_register_reply_evidence', register))
                    if stage == 'pool' and args and operation in {'health', 'idle', 'transition'}:
                        cma = args[0]
                        for name in ('request', 'retire_sandbox'):
                            if hasattr(cma, name):
                                original = getattr(cma, name)
                                # A retirement is one port operation; its internal
                                # requests retain native adapter coverage.
                                def invoke(*a, _name=name, _original=original, **kw):
                                    if _name == 'retire_sandbox':
                                        request = cma.request
                                        with patch.object(cma, 'request', original_request):
                                            return capture.call('cma', _name, _original, a, kw)
                                    return capture.call('cma', _name, _original, a, kw)
                                if name == 'request': original_request = original
                                hooks.enter_context(patch.object(cma, name, invoke))
                    if stage == 'pool' and operation == 'slot' and kwargs.get('cma'):
                        cma = kwargs['cma']; original_request = cma.request
                        hooks.enter_context(patch.object(cma, 'request', lambda *a, **kw: capture.call('cma', 'request', original_request, a, kw)))
                    wrapped = list(args)
                    if stage == 'delivery' and wrapped[0] is not None:
                        wrapped[0] = capture.proxy('mailbox', wrapped[0])
                    try: result = function(*wrapped, **kwargs)
                    except BaseException as error:
                        case['error'] = error_value(error); caught = error; result = None
                depth -= 1
                recording._STAGE.reset(token)
                if stage == 'pool': case['expected_runtime'] = runtime(pool)
                case['expected_result'] = encode(result)
                case['update_times'] = update_times
                case.update(script=capture.entries, admissions=frame['admissions'],
                            expected_final_rows=logical(dump(ledger.tasks)), expected_final_files=files(root),
                            expected_writes=writes(capture.entries))
                # Adapter objects are handles, not serializable scenario inputs.
                if stage == 'pool':
                    if operation in {'health', 'idle', 'transition'}: case['arguments'][0] = {'$cma': True}
                    if 'cma' in case['keywords']: case['keywords']['cma'] = {'$cma': True} if kwargs['cma'] else None
                    if operation == '__init__': case['arguments'][0] = {'$ledger': True}
                try: raw = json.dumps(case, ensure_ascii=False, separators=(',', ':'), allow_nan=False)
                except ValueError:
                    if caught: raise caught
                    return result
                (out / (case['id'] + '.json')).write_text(raw + '\n')
                mapping[identity].append(case['id']); total += 1
                if caught: raise caught
                return result
            def wrap_pool(name, function):
                def call(pool, *a, **kw):
                    if depth or module_name not in {'test_sandbox_pool','test_sandbox_groups','test_pool_adopt','test_cleanup_recovery'}: return function(pool, *a, **kw)
                    ledger = a[0] if name == '__init__' else pool.ledger
                    if len(ledger.tasks.all()) > 40: return function(pool, *a, **kw)
                    ledger = a[0] if name == '__init__' else pool.ledger
                    config = a[1] if name == '__init__' else {}
                    # encode replaces the ledger/CMA handles before serialization.
                    return capture_operation('pool', name, lambda *x, **y: function(pool, *x, **y), pool, ledger,
                                             config, Path(ledger.tasks.path).parent, a, kw)
                return call
            def mail(ledger, config, root, *a, **kw):
                if depth or identity == 'test_automation.AutomationTests.test_owner_draft_receipts_intents_and_stale_conflicts_are_not_terminal': return original_delivery(ledger, config, root, *a, **kw)
                return capture_operation('delivery', 'advance', lambda *x, **y: original_delivery(ledger, config, root, *x, **y),
                                         None, ledger, config, Path(root), a or (None,), kw)
            original_delivery = delivery.advance
            with ExitStack() as hooks:
                original_init = Tasks.__init__
                hooks.enter_context(patch.object(Tasks, '__init__', lambda self, path, clock=None: original_init(self, path, clock=clock or (lambda: NOW-100))))
                from managed.mailbox import GraphMailbox
                box_init = GraphMailbox.__init__
                hooks.enter_context(patch.object(GraphMailbox, '__init__', lambda self, graph, settings, clock=None: box_init(self, graph, settings, clock or (lambda: NOW))))
                queue_init = Queue.__init__
                hooks.enter_context(patch.object(Queue, '__init__', lambda self, path, workflows, reviewer='claude-loop', clock=None, checkpoint=lambda: None: queue_init(self, path, workflows, reviewer, clock or (lambda: NOW), checkpoint)))
                hooks.enter_context(patch('time.time', return_value=NOW))
                for name in POOL_OPS: hooks.enter_context(patch.object(sandbox_pool.SandboxPool, name, wrap_pool(name, getattr(sandbox_pool.SandboxPool, name))))
                hooks.enter_context(patch.object(delivery, 'advance', mail))
                result = unittest.TestResult(); test.run(result)
                if result.errors or result.failures:
                    raise AssertionError(identity + '\n' + str(result.errors + result.failures))
    mail_policy(out, mapping)
    mapping_path.write_text(json.dumps(mapping, indent=2) + '\n')
    print(json.dumps({'new_scenarios': total, 'converted_tests': sum(bool(v) for k,v in mapping.items() if k.split('.')[0] in modules)}))

if __name__ == '__main__': main()
