"""Offline adapters for pool and mail fixtures; never imports source tests."""
from dataclasses import asdict
from unittest.mock import patch
from managed import sandbox_pool
from managed.transports import Readback, Submission

POOL_SETS = {'available', 'attempted'}

def pool(case, ledger, script, *, capture=False):
    instance = sandbox_pool.SandboxPool.__new__(sandbox_pool.SandboxPool)
    instance.ledger = ledger
    for key, value in case['runtime'].items():
        setattr(instance, key, set(value) if key in POOL_SETS else value)
    if case['options'].get('group_order'):
        instance.group_limits = {k: instance.group_limits[k] for k in case['options']['group_order']}
    args = case['arguments']; kw = case['keywords']
    def decode(value):
        if isinstance(value, dict) and '$ledger' in value: return ledger
        if isinstance(value, dict) and '$cma' in value: return script.adapter('cma')
        return value
    args = [decode(v) for v in args]; kw = {k: decode(v) for k,v in kw.items()}
    if case['operation'] in {'assigned','transition'}:
        index = 0 if case['operation'] == 'assigned' else 1
        identity = args[index]['sandbox_id']
        args[index] = next(e for e in instance.entries + instance.failed if e['sandbox_id'] == identity)
    if case['operation'] == '__init__':
        idle = case['options'].get('idle_pause_seconds', '600')
        with patch.dict('os.environ', {'OUTREACH_POOL_IDLE_PAUSE_SECONDS': str(idle)}):
            result = instance.__init__(*args, **kw)
    else: result = getattr(instance, case['operation'])(*args, **kw)
    runtime = {k: sorted(v) if isinstance(v, (set, frozenset)) else v
               for k,v in vars(instance).items() if k != 'ledger'}
    from managed.scenario_contract import encode
    if capture: return encode(runtime), encode(result)
    assert encode(runtime) == case['expected_runtime'], case['id'] + ': runtime differs'
    from managed.scenario_contract import encode
    assert encode(result) == case['expected_result'], case['id'] + ': result differs'


def mail_value(value):
    if isinstance(value, dict) and value.get('$type') in {'Readback', 'Submission'}:
        fields = {k:v for k,v in value.items() if k != '$type'}
        return {'Readback': Readback, 'Submission': Submission}[value['$type']](**fields)
    return value


def error_value(error):
    from urllib.error import HTTPError
    import base64
    result = {'type': type(error).__name__, 'message': str(error)}
    if isinstance(error, HTTPError):
        raw = error.read() if error.fp is not None and not error.fp.closed else b''
        if error.fp is not None and not error.fp.closed: error.fp.seek(0)
        result.update(status=error.code, body=base64.b64encode(raw).decode())
    elif hasattr(error, 'code'): result.update(status=error.code, cma_body=getattr(error, 'body', None))
    return result
