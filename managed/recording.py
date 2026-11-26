"""Opt-in private JSONL transport evidence. No uploads or production replay.

All hooks return immediately when OUTREACH_RECORD_DIR is absent. A recording
failure is reported by type only and cannot change a completed external effect.
"""
import base64
import functools
from email.parser import BytesParser
from email.policy import default as email_policy
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import threading
import time
from urllib.parse import quote, quote_plus, urlsplit, parse_qsl, urlencode


_LOCK = threading.Lock()
SECRET_KEYS = re.compile(r'(?i)(authorization|cookie|password|secret|token|credential|api[-_]?key|sig)$')


def enabled():
    return bool(os.environ.get('OUTREACH_RECORD_DIR'))


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False)


def secrets():
    from . import run_log
    return set(run_log.SECRETS.get() or ()) | {os.environ[k] for k in run_log.SECRET_ENV_KEYS if os.environ.get(k)}


def scrub(value, extra=()):
    values = secrets() | {s for s in extra if isinstance(s, str) and s}
    variants = set()
    hex_patterns = {item.encode().hex() for item in values}
    hex_patterns.update(item for item in values if re.fullmatch(r'[0-9a-fA-F]+', item))
    for item in values:
        variants.update((item, quote(item, safe=''), quote_plus(item),
                         base64.b64encode(item.encode()).decode(), json.dumps(item)[1:-1],
                         item.encode().hex(), item.encode().hex().upper()))
    def clean(v):
        if isinstance(v, dict):
            return {clean(str(k)): '[redacted]' if SECRET_KEYS.search(str(k)) else clean(x) for k, x in v.items()}
        if isinstance(v, (tuple, list)):
            return [clean(x) for x in v]
        if isinstance(v, bytes):
            try: return clean(json.loads(v))
            except (ValueError, UnicodeError): return clean(v.decode('utf-8', errors='replace'))
        if isinstance(v, str):
            # Read adapters can return JSON as text rather than decoded objects.
            # Apply the same credential-key rules inside those string values.
            try:
                parsed = json.loads(v)
                if isinstance(parsed, (dict, list)):
                    filtered = clean(parsed)
                    if parsed != filtered: return canonical(filtered)
            except (ValueError, UnicodeError): pass
            # Files in scenario frames are base64 read responses. Redact inside
            # them as well, retaining original bytes when nothing was masked.
            if len(v) >= 4 and len(v) % 4 == 0:
                try:
                    raw = base64.b64decode(v, validate=True)
                    cleaned = raw
                    for item in sorted(variants, key=len, reverse=True): cleaned = cleaned.replace(item.encode(), b'[redacted]')
                    try:
                        parsed = json.loads(raw)
                        filtered = clean(parsed)
                        if parsed != filtered: cleaned = canonical(filtered).encode()
                    except (ValueError, UnicodeError):
                        try: cleaned = clean(raw.decode('utf-8')).encode('utf-8')
                        except UnicodeError: pass
                    if cleaned != raw: return base64.b64encode(cleaned).decode()
                except (ValueError, UnicodeError): pass
            for item in sorted(variants, key=len, reverse=True):
                v = v.replace(item, '[redacted]')
            for pattern in hex_patterns:
                v = re.sub(re.escape(pattern), '[redacted]', v, flags=re.IGNORECASE)
            v = re.sub(r'(?i)(bearer|basic)\s+[^\s"\'<>]+', r'\1 [redacted]', v)
            v = re.sub(r'(?i)(\b(?:authorization|proxy-authorization|cookie|set-cookie|'
                       r'[\w-]*(?:token|secret|credential|api[-_]?key))\s*:\s*)[^\r\n]+',
                       r'\1[redacted]', v)
            v = re.sub(r'(?i)((?:access_token|token|sig|signature|api_key|password)=)[^&\s"\']+', r'\1[redacted]', v)
            v = re.sub(r'(https?://)[^/@\s]+:[^/@\s]+@', r'\1[redacted]@', v)
        return v
    return clean(value)


def emit(value, extra=()):
    if not enabled(): return
    try:
        line = (canonical(scrub({'schema_version': 1, **value}, extra)) + '\n').encode()
        directory = Path(os.environ['OUTREACH_RECORD_DIR'])
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        # One append per record, protected across threads and processes by the
        # file lock. Use a process file so concurrent hosted steps never mix runs.
        with _LOCK:
            fd = os.open(directory / f'calls-{os.getpid()}.jsonl', os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o600)
            try:
                import fcntl
                fcntl.flock(fd, fcntl.LOCK_EX)
                with os.fdopen(fd, 'ab', closefd=False) as output: output.write(line)
            finally: os.close(fd)
    except Exception as error:
        print('recording-error ' + type(error).__name__, file=sys.stderr)


def path_template(path):
    parts = urlsplit(path)
    path = parts.path
    path = re.sub(r'(?<=/)(?:submissions|agents|responses|agent-lifecycle|messages|attachments|depositions|records|assets)/[^/]+',
                  lambda m: m[0].split('/')[0] + '/{id}', path)
    query = [(k, '{value}') for k, _ in parse_qsl(parts.query, keep_blank_values=True)]
    return path + ('?' + urlencode(query) if query else '')


def descriptor(adapter, method, path, body=None, key=None):
    method = method.upper()
    raw = body
    if isinstance(raw, bytes):
        try: raw = json.loads(raw)
        except (ValueError, UnicodeError): raw = {'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()}
    result = {'event': 'call', 'adapter': adapter, 'method': method,
              'path_template': path_template(path), 'path': path,
              'body_digest': hashlib.sha256(canonical(raw).encode()).hexdigest(),
              'idempotency_key': key, 'write': method not in {'GET', 'HEAD', 'READ'}}
    if not result['write']: result['request_body'] = raw
    return result


class Response:
    def __init__(self, response, call, extra):
        self.response, self.call, self.extra = response, call, extra
        self.stream = response
        self.chunks = []
    def __getattr__(self, name): return getattr(self.stream, name)
    def __enter__(self):
        # Read from whatever the wrapped context manager yields, exactly as the
        # unrecorded caller would (urllib returns itself; test doubles may not).
        entered = self.response.__enter__()
        if entered is not None: self.stream = entered
        return self
    def read(self, *args):
        raw = self.stream.read(*args); self.chunks.append(raw); return raw
    def __iter__(self):
        for line in self.stream:
            self.chunks.append(line); yield line
    def __exit__(self, *args):
        try: return self.response.__exit__(*args)
        finally:
            try:
                value = {**self.call, 'status': getattr(self.stream, 'status', getattr(self.response, 'status', 200))}
                # Write bodies are digests only. Responses retain receipts needed to
                # replay known writes; the replay adapter never issues a write.
                value['response_body'] = b''.join(self.chunks)
                emit(value, self.extra)
            except Exception as error:
                print('recording-error ' + type(error).__name__, file=sys.stderr)


def open(adapter, opener, request, **kwargs):
    if not enabled(): return opener.open(request, **kwargs)
    try:
        call, extra = _open_call(adapter, request)
    except Exception as error:
        print('recording-error ' + type(error).__name__, file=sys.stderr)
        return opener.open(request, **kwargs)
    try: return Response(opener.open(request, **kwargs), call, extra)
    except Exception as error:
        try:
            value = {**call, 'status': getattr(error, 'code', None), 'error': type(error).__name__}
            # urllib errors carry a response stream. Rewind a local copy so callers
            # observe the same body, even after the recorder has captured it.
            from urllib.error import HTTPError
            if isinstance(error, HTTPError) and error.fp is not None:
                raw = error.fp.read()
                original = error.fp
                error.fp = error.file = io.BytesIO(raw)
                error._closer.file = error.fp
                original.close()
                error.__dict__.pop('read', None)
                value['response_body'] = raw
            emit(value, extra)
        except Exception as recording_error:
            print('recording-error ' + type(recording_error).__name__, file=sys.stderr)
        raise


def _open_call(adapter, request):
    headers = dict(request.header_items())
    extra = [v.split(' ', 1)[-1] for k, v in headers.items() if SECRET_KEYS.search(k)]
    body = request.data
    content_type = request.get_header('Content-type', '')
    if body and content_type.startswith('multipart/form-data'):
        message = BytesParser(policy=email_policy).parsebytes(('Content-Type: ' + content_type + '\r\nMIME-Version: 1.0\r\n\r\n').encode() + body)
        parts = []; metadata = None
        for part in message.iter_parts():
            raw = part.get_payload(decode=True)
            if part.get_param('name', header='content-disposition') == 'metadata': metadata = json.loads(raw)
            else: parts.append({'field': part.get_param('name', header='content-disposition'), 'filename': part.get_filename(), 'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()})
        body = {'metadata': metadata, 'files': parts}
    return descriptor(adapter, request.get_method(), request.full_url, body,
                      request.get_header('Idempotency-key')), extra


def command_call(args):
    args = [str(a) for a in args]
    adapter = Path(args[0]).name
    method, body, key = 'READ', {'argv': args[1:]}, None
    payload = None
    if adapter == 'gh':
        if args[1:2] == ['api']:
            for flag in ('--method', '-X'):
                if flag in args: method = args[args.index(flag) + 1].upper()
            if method == 'READ': method = 'POST' if any(f in args for f in ('--input', '-f', '-F', '--field', '--raw-field')) else 'GET'
        elif args[1:2] == ['release']:
            method = {'view':'GET', 'download':'GET', 'create':'POST', 'upload':'POST', 'edit':'PATCH', 'delete':'DELETE', 'delete-asset':'DELETE'}.get(args[2], 'WRITE')
    elif adapter == 'git':
        commands = {'add', 'commit', 'push', 'fetch', 'checkout', 'switch', 'init', 'clone', 'config', 'remote', 'sparse-checkout'}
        if any(a in commands for a in args[1:]): method = 'WRITE'
    else:
        if '--method' in args: method = args[args.index('--method') + 1]
    for flag in ('--input', '--notes-file', '--data'):
        if flag in args:
            name = args[args.index(flag) + 1].removeprefix('@')
            if Path(name).is_file():
                content = Path(name).read_bytes()
                try: payload = json.loads(content)
                except (ValueError, UnicodeError): payload = {'sha256': hashlib.sha256(content).hexdigest(), 'bytes': len(content)}
                body[flag] = content.decode('utf-8', errors='replace')
    for index, arg in enumerate(args):
        if arg.lower().startswith('idempotency-key:'): key = arg.split(':', 1)[1]
        if index and not arg.startswith('-') and len(arg) < 4096:
            try:
                path = Path(arg)
                if path.is_file(): body.setdefault('files', []).append({'name': path.name, 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
            except OSError: pass
    path = '/'.join(args[1:3])
    if adapter == 'nyxid' and args[1:3] == ['proxy', 'request'] and len(args) > 4:
        adapter, path = args[3], '/' + args[4].lstrip('/')
    if adapter == 'gh' and args[1:2] == ['api']:
        path = next((a for a in args[2:] if a.startswith(('repos/', '/repos/'))), path)
    if adapter == 'gh' and args[1:2] == ['release'] and len(args) > 3:
        repo = args[args.index('--repo') + 1] if '--repo' in args else '{repo}'
        path = f'/repos/{repo}/releases/tags/{args[3]}'
        if args[2] in {'upload', 'download', 'delete-asset'}: path += '/assets'
    request = payload if payload is not None else (body.get('files', body) if method not in {'READ', 'GET', 'HEAD'} else body)
    result = descriptor(adapter, method, path, request, key)
    if adapter == 'gh':
        result['path_template'] = re.sub(r'/?repos/[^/]+/[^/]+', '/repos/{owner}/{repo}', result['path_template'])
        result['path_template'] = re.sub(r'/tags/[^/]+', '/tags/{tag}', result['path_template'])
    return result


def run(args, *, record_adapter=None, **kwargs):
    if not enabled(): return subprocess.run(args, **kwargs)
    try:
        call = command_call(args)
        if record_adapter: call['adapter'] = record_adapter
        from .run_log import SECRET_ENV_KEYS
        child_secrets = [v for k, v in (kwargs.get('env') or {}).items() if k in SECRET_ENV_KEYS and v]
        child_secrets += command_secrets(args)
    except Exception as error:
        print('recording-error ' + type(error).__name__, file=sys.stderr)
        return subprocess.run(args, **kwargs)
    try:
        result = subprocess.run(args, **kwargs)
    except Exception as error:
        emit({**call, 'status': getattr(error, 'returncode', None), 'error': type(error).__name__}, child_secrets)
        raise
    try:
        value = {**call, 'status': result.returncode}
        if not call['write']: value['response_body'] = result.stdout
        emit(value, child_secrets)
    except Exception as error:
        print('recording-error ' + type(error).__name__, file=sys.stderr)
    return result


def check_output(args, **kwargs):
    if not enabled(): return subprocess.check_output(args, **kwargs)
    try: call = command_call(args)
    except Exception as error:
        print('recording-error ' + type(error).__name__, file=sys.stderr)
        return subprocess.check_output(args, **kwargs)
    try: result = subprocess.check_output(args, **kwargs)
    except Exception as error:
        emit({**call, 'status': getattr(error, 'returncode', None), 'error': type(error).__name__}, command_secrets(args)); raise
    emit({**call, 'status': 0, 'response_body': result}, command_secrets(args))
    return result


def command_secrets(args):
    """Header values also mask credential echoes in a command's read output."""
    values = []
    for arg in map(str, args):
        header = arg.split('=', 1)[-1] if arg.startswith(('--header=', '-H=')) else arg
        key, separator, value = header.partition(':')
        if separator and SECRET_KEYS.search(key.strip()):
            if key.strip().lower() in {'cookie', 'set-cookie'}:
                # Register the complete header value, never individual cookie
                # values such as "1" that also occur throughout valid evidence.
                values.append(header)
                if '=' in value: values.append(value.strip())
            else:
                values.append(value.strip())
                values.append(value.strip().split(' ', 1)[-1])
    return [value for value in values if value]


def decision(gate):
    @functools.wraps(gate)
    def wrapped(self, lane, key):
        result = gate(self, lane, key)
        try:
            if _STAGE.get() is not None: _STAGE.get()['admissions'].append({'lane': lane, 'key': key, 'allowed': result})
            emit({'event': 'admission', 'gate': 'tick', 'lane': lane, 'key': key, 'allowed': result})
        except Exception as error:
            print('recording-error ' + type(error).__name__, file=sys.stderr)
        return result
    return wrapped


def pointer(boundary, value):
    emit({'event': 'release_pointer', 'boundary': boundary, 'pointer': value})


def pending_uploads():
    """Private-control upload hook: caller must enforce private custody/retention.

    Phase 3a deliberately does not call this hook or upload any recording.
    """
    if not enabled(): return ()
    return tuple(sorted(Path(os.environ['OUTREACH_RECORD_DIR']).glob('calls-*.jsonl')))

# A stage frame relates transport evidence to its logical input/output. This is
# private recording data, never a production replay or a ledger save.
from contextvars import ContextVar
_STAGE = ContextVar('recorded_stage', default=None)
_WRITE_METHODS = {'deliver', 'deliver_pre_reviewed', 'withdraw', 'upload', 'create', 'register_note', 'save_state', 'delete_asset', 'commit_tables', 'retire_sandbox', 'save_draft', 'submit', 'submit_tracked', 'discard_draft', 'create_draft', 'delete_draft', 'move_draft_to_deleted', 'upload_task_batch'}


def _capture(adapter_name, method, function, args, kwargs):
    from .scenario_contract import encode
    frame = _STAGE.get()
    if frame is None: return function(*args, **kwargs)
    # Capture failures must never prevent, repeat or alter the adapter call.
    try:
        if frame.get('stage') in {'intake','inbound'} and mail_bodies((args,kwargs)): frame['incomplete'] = True
        entry = {'adapter': adapter_name, 'method': method, 'kwargs': encode(kwargs)}
        if method in _WRITE_METHODS or (adapter_name in {'cma', 'github'} and method == 'request' and len(args) > 1 and args[1] != 'GET'):
            entry['saved_snapshot_digest'] = frame.get('_saved_snapshot_digest')
            entry['args_digest'] = hashlib.sha256(canonical(encode(args)).encode()).hexdigest()
        else: entry['args'] = encode(args)
        frame['script'].append(entry)
    except Exception as error:
        print('recording-error ' + type(error).__name__, file=sys.stderr)
        frame['incomplete'] = True
        return function(*args, **kwargs)
    try:
        if method == 'submit_tracked' and frame.get('stage') == 'delivery':
            before = args[2]
            result = function(*args[:2], lambda draft: _capture('boundary', 'send_call', before, (draft,), {}), **kwargs)
        else: result = function(*args, **kwargs)
    except BaseException as error:
        # Do not read/seek HTTPError: observers must not change its body cursor.
        try:
            entry['error'] = {'type': type(error).__name__, 'message': str(error) if frame.get('stage') in {'pool', 'delivery', 'formalization', 'prior-work'} else ''}
            if adapter_name == 'boundary' and frame['script'][-1] is not entry:
                entry.pop('error', None); entry['result'] = None
            if 'error' in entry: frame['_errors'][id(error)] = entry['error']
            if hasattr(error, 'code') and 'error' in entry: entry['error']['status'] = error.code
            for field in ('attachments','intake_reason'):
                if hasattr(error,field) and 'error' in entry: entry['error'][field] = getattr(error,field)
        except Exception as recording_error:
            print('recording-error ' + type(recording_error).__name__, file=sys.stderr)
        raise
    try:
        if frame.get('stage') in {'intake','inbound'} and mail_bodies(result):
            frame['incomplete'] = True
            entry['result'] = None
        else: entry['result'] = encode(result)
        if adapter_name == 'releases' and method == 'run' and '--dir' in args:
            path = Path(args[args.index('--dir') + 1]) / args[args.index('--pattern') + 1]
            entry['download'] = {'name': path.name, 'base64': encode(path)['base64']}
    except Exception as error:
        print('recording-error ' + type(error).__name__, file=sys.stderr)
        frame['incomplete'] = True
    return result


def mail_bodies(value):
    """New read stages must not expand the mail-content recording boundary."""
    if isinstance(value, dict):
        if ('from' in value or 'internetMessageId' in value) and value.get('body'): return True
        return any(mail_bodies(v) for v in value.values())
    if isinstance(value, (list,tuple)): return any(map(mail_bodies,value))
    return False


class _StageAdapter:
    def __init__(self, name, target):
        object.__setattr__(self, 'name', name); object.__setattr__(self, 'target', target)
    @property
    def __class__(self): return self.target.__class__
    def __setattr__(self, name, value): setattr(self.target, name, value)
    def __call__(self, *args, **kwargs): return _capture(self.name, 'request', self.target, args, kwargs)
    def __getattr__(self, name):
        value = getattr(self.target, name)
        frame = _STAGE.get()
        if frame and frame.get('stage') == 'intake' and name in {'match_inbound_sent','_refresh_sent_subjects','_registered_recipient'}:
            from .mailbox import GraphMailbox
            if name == '_registered_recipient': return value
            if getattr(value,'__func__',None) is getattr(GraphMailbox,name): return getattr(GraphMailbox,name).__get__(self)
        if name == 'upload_task_batch' and frame and frame.get('stage') in {'intake','inbound'}:
            def upload(files, tag, listings):
                from .scenario_contract import encode
                result = None
                def perform(*_args):
                    nonlocal result
                    result = value(files, tag, listings)
                    return {'listings': listings}
                try: encoded = [[encode(path),ref] for path,ref in files.items()]
                except Exception as error:
                    frame['incomplete'] = True
                    print('recording-error ' + type(error).__name__, file=sys.stderr)
                    return value(files, tag, listings)
                _capture(self.name, name, perform, (encoded,tag,listings), {})
                return result
            return upload
        return (lambda *a, **kw: _capture(self.name, name, value, a, kw)) if callable(value) else value


def adapter(name, value):
    if not enabled() or _STAGE.get() is None or isinstance(value, _StageAdapter): return value
    if name == 'github': _STAGE.get().setdefault('options',{})['github'] = True
    return _StageAdapter(name, value)


def policy_read(function):
    """Capture the effective policy, including engine fallbacks and overlays."""
    @functools.wraps(function)
    def wrapped(path, *args, **kwargs):
        result = function(path, *args, **kwargs)
        frame = _STAGE.get()
        if enabled() and frame is not None:
            try:
                raw = result.encode('utf-8')
                if len(raw) > MAX_STAGE_FILE_BYTES: frame['incomplete'] = True
                else: frame['initial_files'][path] = base64.b64encode(raw).decode()
            except Exception: frame['incomplete'] = True
        return result
    return wrapped


# Hard ceiling includes the complete canonical JSONL event and its newline.
MAX_STAGE_FRAME_BYTES = 1024 * 1024
MAX_STAGE_FILE_BYTES = MAX_STAGE_FRAME_BYTES // 2
_AUDIT_INSTALLED = False


def _stage_file_open(event, args):
    if event != 'open': return
    frame = _STAGE.get()
    if frame is None or frame.get('_reading_file'): return
    try:
        path, mode, flags = args
        if not isinstance(path, (str, bytes, os.PathLike)): return
        path = Path(path).resolve()
        relative = path.relative_to(frame['_root']).as_posix()
        if not relative.startswith(('tables/', 'managed/')) or relative in frame['initial_files']: return
        # Capture the bytes before the first access (including a write). A
        # missing file is represented by absence, and its later write is in
        # final_files. No directory walk or unrelated policy file is recorded.
        frame['_reading_file'] = True
        if path.is_file():
            if path.stat().st_size > MAX_STAGE_FILE_BYTES:
                frame['incomplete'] = True
            else:
                raw = path.read_bytes()
                frame['initial_files'][relative] = base64.b64encode(raw).decode()
        frame['_touched_files'].add(relative)
    except Exception:
        pass
    finally:
        frame['_reading_file'] = False


def stage_frame(name):
    def decorate(stage):
        @functools.wraps(stage)
        def wrapped(ledger, config, root, *args, **kwargs):
            if not enabled(): return stage(ledger, config, root, *args, **kwargs)
            from .scenario_contract import table_digests, capture_tables
            from .location import example_config
            from .control import workflow
            global _AUDIT_INSTALLED
            try:
                if not _AUDIT_INSTALLED:
                    sys.addaudithook(_stage_file_open); _AUDIT_INSTALLED = True
                baseline = getattr(ledger, '_recording_baseline', None)
                initial, initial_index, delta, overflow = capture_tables(ledger.tasks, baseline)
                frame = {'schema_version': 2, 'id': 'recorded-' + name, 'stage': name,
                         'initial_digests': initial, 'initial_files': {},
                         'base_digests': (baseline or initial_index)['digests'], 'initial_delta': delta,
                         'incomplete': overflow,
                         'config': config, 'installation': example_config(), 'workflow': workflow('mathesis'),
                         'now': ledger.tasks.clock(), 'script': [], 'update_times': [], 'admissions': [],
                         '_errors': {}, '_root': Path(root).resolve(), '_touched_files': set(),
                         'adapter_attributes': {'releases': {k: getattr(ledger.releases, k, '') for k in ('repo', 'tag')}},
                         'options': {'previous': list(getattr(getattr(ledger, 'tick', None), 'previous', {}).get(name, []))}}
                if name in {'pool', 'delivery', 'intake', 'inbound', 'formalization', 'prior-work'}:
                    if getattr(ledger, '_recording_saved_input', None): frame['saved_input'] = ledger._recording_saved_input
                    frame['workflows'] = {kind: workflow(kind) for kind in {r['kind'] for r in ledger.tasks.all()}}
                if name in {'intake','inbound','formalization','prior-work'}:
                    from .location import DATA_REPO
                    frame['data_repo'] = DATA_REPO
                    frame['operation'] = 'resolve' if name == 'inbound' else 'advance'
                    frame['previous_units'] = getattr(getattr(ledger,'tick',None),'previous',{})
                if name in {'intake','inbound'}:
                    from .mailbox import GraphMailbox, _adapter_method
                    box = args[0] if args else kwargs.get('mailbox')
                    frame['adapter_attributes']['mailbox'] = {'settings': getattr(box,'settings',{}),
                        'graph_mailbox': isinstance(box,GraphMailbox),
                        'native_matching': getattr(getattr(box,'match_inbound_sent',None),'__func__',None) is GraphMailbox.match_inbound_sent,
                        **{method: _adapter_method(box,method) is not None for method in ('sent_snapshot','match_inbound_sent','sent_messages_to','attachment_content')}}
                    frame['options']['mailbox'] = box is not None
                    frame['options']['previous'] = list(getattr(getattr(ledger,'tick',None),'previous',{}).get('mailbox-intake',[]))
                    if name == 'intake':
                        frame['revision'] = kwargs.get('revision') or (args[1] if len(args)>1 else None)
                        explicit_now = kwargs.get('now') or (args[2] if len(args)>2 else None)
                        if explicit_now: frame['datetime'] = explicit_now.isoformat()
                    else: frame['row'] = config
                if name in {'formalization','prior-work'}: frame['options']['github'] = (args[0] if args else kwargs.get('github')) is not None
                if name == 'delivery':
                    box = args[0] if args else kwargs.get('mailbox')
                    frame['adapter_attributes']['mailbox'] = {method: callable(getattr(box, method, None)) for method in MAILBOX_FEATURES}
                    frame['options']['mailbox'] = box is not None
                    frame['_queue_seq'] = ledger.tasks.db.execute("SELECT COALESCE(MAX(seq),0) FROM events").fetchone()[0] if ledger.tasks.db.execute("SELECT 1 FROM sqlite_master WHERE name='events'").fetchone() else 0
                if name == 'mathesis':
                    frame['options'].update(again=kwargs.get('again', False), attempted=sorted(kwargs['attempted']) if kwargs.get('attempted') is not None else None)
                if name == 'sent':
                    from datetime import datetime, timezone
                    box = args[0] if args else kwargs.get('mailbox')
                    frame['adapter_attributes']['mailbox'] = {'settings': getattr(box, 'settings', {})}
                    frame['options'].update(max_pages=2, mailbox=box is not None)
                    frame['datetime'] = (args[1] if len(args) > 1 and args[1] else datetime.now(timezone.utc)).isoformat()
            except Exception as error:
                emit({'event': name + '_stage_unavailable', 'error': type(error).__name__})
                return stage(ledger, config, root, *args, **kwargs)
            token = _STAGE.set(frame)
            original_save, original_releases, original_update = ledger.save, ledger.releases, ledger.tasks.update
            def save(*a, **kw):
                remote = ledger.releases; ledger.releases = original_releases
                try:
                    result = _capture('ledger', 'save', original_save, a, kw)
                    snapshot = getattr(ledger, 'snapshot', None)
                    frame['_saved_snapshot_digest'] = snapshot['sha256'] if snapshot else hashlib.sha256(canonical(table_digests(ledger.tasks)).encode()).hexdigest()
                    frame['script'][-1]['saved_snapshot'] = {'generation': getattr(ledger, 'generation', 0), 'digest': frame['_saved_snapshot_digest']}
                    return result
                finally: ledger.releases = remote
            def update(*a, **kw):
                row = original_update(*a, **kw)
                try:
                    at = ledger.tasks.db.execute('SELECT at FROM task_events WHERE id=? AND revision=? ORDER BY seq DESC LIMIT 1', (row['id'], row['revision'])).fetchone()[0]
                    frame['update_times'].append([row['updated'], at])
                except Exception: frame['incomplete'] = True
                return row
            ledger.save = save; ledger.releases = adapter('releases', original_releases); ledger.tasks.update = update
            secret = kwargs.get('secret')
            values = list(args)
            if name == 'mathesis':
                for index, label in ((1, 'mathesis'), (3, 'publication')):
                    if len(values) > index and values[index] is not None: values[index] = adapter(label, values[index])
                if len(values) > 2: secret = values[2]
            elif name == 'publication':
                if len(values) > 1 and values[1] is not None: values[1] = adapter('publication', values[1])
                if len(values) > 2: secret = values[2]
            elif name in {'sent', 'delivery', 'intake', 'inbound'} and values and values[0] is not None: values[0] = adapter('mailbox', values[0])
            try:
                result = stage(ledger, config, root, *values, **kwargs)
                if name == 'inbound':
                    try:
                        from .scenario_contract import encode
                        if mail_bodies(result): frame['incomplete'] = True
                        else: frame['expected_result'] = encode(result)
                    except Exception: frame['incomplete'] = True
                return result
            except BaseException as error:
                frame['error'] = {'type': type(error).__name__, 'message': str(error) if name in {'pool','delivery','formalization','prior-work'} else ''}; raise
            finally:
                ledger.save, ledger.releases, ledger.tasks.update = original_save, original_releases, original_update
                _STAGE.reset(token)
                try:
                    final_digests, final_index, _, _ = capture_tables(ledger.tasks)
                    if name in {'intake','inbound'} and mail_bodies(frame): frame['incomplete'] = True
                    if name == 'intake' and 'datetime' not in frame:
                        cursor = ledger.tasks.db.execute("SELECT payload FROM pipeline_state WHERE key='mailbox'").fetchone()
                        if cursor: frame['datetime'] = json.loads(cursor[0])['through']
                        else: frame['incomplete'] = True
                    if name == 'delivery' and ledger.tasks.db.execute("SELECT 1 FROM sqlite_master WHERE name='events'").fetchone():
                        frame['queue_event_times'] = [r[0] for r in ledger.tasks.db.execute('SELECT at FROM events WHERE seq>? ORDER BY seq', (frame['_queue_seq'],))]
                    frame['expected_final_digests'] = final_digests
                    frame['now_end'] = ledger.tasks.clock()
                    touched = frame['_touched_files']
                    frame['expected_final_files'] = {p: base64.b64encode((Path(root) / p).read_bytes()).decode()
                        for p in touched if (Path(root) / p).is_file() and (Path(root) / p).stat().st_size <= MAX_STAGE_FILE_BYTES}
                    frame = {k: v for k, v in frame.items() if not k.startswith('_')}
                    safe = scrub(frame, (secret,) if secret else ())
                    if safe != frame: safe['redacted'] = True
                    event = {'event': name + '_stage', 'case': safe}
                    if frame.get('incomplete') or len((canonical({'schema_version': 1, **event}) + '\n').encode()) > MAX_STAGE_FRAME_BYTES:
                        emit({'event': name + '_stage_unavailable', 'reason': 'frame-size-or-capture',
                              'initial_digests': frame['initial_digests'], 'final_digests': frame['expected_final_digests']})
                    else:
                        emit(event)
                        ledger._recording_baseline = final_index
                except Exception as error: emit({'event': name + '_stage_unavailable', 'error': type(error).__name__})
        return wrapped
    return decorate


mathesis_stage = stage_frame('mathesis')


def package_read(function):
    """Record the filesystem service result once, including exact prepared bytes.

    Transport evidence remains independently recorded. Stage replay treats the
    validated package as a typed short-note service observation.
    """
    @functools.wraps(function)
    def wrapped(row, directory, releases):
        if not enabled() or _STAGE.get() is None or _STAGE.get()['stage'] != 'publication':
            return function(row, directory, releases)
        result = None
        def read(task_id, result_hash):
            nonlocal result
            result = function(row, directory, releases.target if isinstance(releases, _StageAdapter) else releases)
            metadata, prepared, source = result
            return {'metadata': metadata, 'source': source,
                    'files': {p.name: p for p in sorted(Path(prepared).iterdir())}}
        _capture('shortnote', 'package', read, (row['id'], hashlib.sha256(canonical(row['data']['result']).encode()).hexdigest()), {})
        return result
    return wrapped


def contact_render(root, function):
    if not enabled() or _STAGE.get() is None: return function()
    from . import registry
    from .scenario_contract import files
    def render(contacts):
        function()
        return {name: base64.b64encode((Path(root) / name).read_bytes()).decode() for name in ('tables/contacts.json', 'tables/CONTACTS.md', 'tables/POSITIVE.md') if (Path(root) / name).is_file()}
    return _capture('registry', 'render', render, (registry.read(root, 'contacts'),), {})


def contact_commit(function):
    if not enabled() or _STAGE.get() is None: return function()
    return _capture('registry', 'commit_tables', function, (), {})


def contact_needs_push(function):
    if not enabled() or _STAGE.get() is None: return function()
    return _capture('registry', 'needs_push', function, (), {})


def snapshot_index(path):
    if not enabled() or not Path(path).is_file(): return None
    try:
        import sqlite3
        from types import SimpleNamespace
        from .scenario_contract import capture_tables
        db = sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro&immutable=1', uri=True)
        try: return capture_tables(SimpleNamespace(db=db))[1]
        finally: db.close()
    except Exception as error:
        emit({'event': 'snapshot_index_unavailable', 'error': type(error).__name__})
        return None


def observed_error(error, summary):
    frame = _STAGE.get()
    if frame is not None and id(error) in frame['_errors']:
        frame['_errors'][id(error)]['summary'] = summary
    return summary


def credit_decision(gate):
    @functools.wraps(gate)
    def wrapped(self, row, *, reserve=True):
        result = gate(self, row, reserve=reserve)
        emit({'event': 'admission', 'gate': 'credit', 'key': row['id'], 'reserve': reserve, 'allowed': result})
        return result
    return wrapped


def admission(key, reason, allowed):
    if enabled(): emit({'event': 'admission', 'gate': 'coordinator', 'key': key, 'reason': reason, 'allowed': allowed})


def save_pointer(save):
    @functools.wraps(save)
    def wrapped(self, *args, **kwargs):
        if not enabled(): return save(self, *args, **kwargs)
        try:
            result = save(self, *args, **kwargs)
            try:
                if self.snapshot and callable(getattr(self, 'version', None)) and self.version() == self.saved_version:
                    from .scenario_contract import input_digest
                    self._recording_saved_input = {'generation': self.generation,
                        'digest': self.snapshot['sha256'], 'logical_digest': input_digest(self.tasks)}
            except Exception as error:
                print('recording-error ' + type(error).__name__, file=sys.stderr)
            return result
        finally:
            # Use the last committed local pointer, including a debounced/no-op
            # save or a failed new pointer write. Never issue an observing GET.
            try:
                snapshot = self.snapshot
                pointer('end', {'schema_version': 1, 'generation': self.generation,
                    'snapshot': {'name': snapshot['name'], 'sha256': snapshot['sha256']} if snapshot else None,
                    'run_id': self.published_run, 'run_history': self.run_history})
            except Exception as error:
                print('recording-error ' + type(error).__name__, file=sys.stderr)
    return wrapped


MAILBOX_FEATURES = ('submit_tracked', 'drafts_for', 'recipient_history', 'message', 'sent_items',
    'drafts_folder_id', 'deleted_items_folder_id', 'verify_owner', 'matches', 'own_draft',
    'thread', 'find', 'messages', 'create_draft', 'delete_draft', 'move_draft_to_deleted')
POOL_FIELDS = ('correspondence', 'max_sandboxes', 'agents_per_sandbox', 'reserve_review_slots',
    'group_limits', 'kind_groups', 'idle_seconds', 'entries', 'failed', 'available',
    'observed', 'attempted', 'errors', 'agent_cleanup_remaining')


def pool_operation(function):
    """Opt-in bounded pool operation frames; nested operations share their frame."""
    @functools.wraps(function)
    def wrapped(pool, *args, **kwargs):
        if not enabled() or _STAGE.get() is not None: return function(pool, *args, **kwargs)
        from .scenario_contract import encode
        ledger = args[0] if function.__name__ == '__init__' else pool.ledger
        config = args[1] if function.__name__ == '__init__' else {}
        from .control import ROOT
        @stage_frame('pool')
        def perform(ledger, config, root):
            frame = _STAGE.get()
            if frame is None: return function(pool, *args, **kwargs)
            def runtime(): return {k: encode(getattr(pool, k)) for k in POOL_FIELDS if hasattr(pool, k)}
            def argument(v):
                if v is ledger: return {'$ledger': True}
                if callable(getattr(v, 'request', None)): return {'$cma': True}
                return encode(v)
            try:
                frame.update(operation=function.__name__, runtime=runtime(),
                    arguments=[argument(v) for v in args], keywords={k: argument(v) for k,v in kwargs.items()})
                frame['options'].update(idle_pause_seconds=os.environ.get('OUTREACH_POOL_IDLE_PAUSE_SECONDS','600'),
                    group_order=list(getattr(pool, 'group_limits', {})))
            except Exception: frame['incomplete'] = True
            values = list(args); keywords = dict(kwargs)
            if function.__name__ in {'health','idle','transition'} and values: values[0] = adapter('cma', values[0])
            if keywords.get('cma') is not None: keywords['cma'] = adapter('cma', keywords['cma'])
            try:
                result = function(pool, *values, **keywords)
                frame['expected_result'] = encode(result)
                return result
            finally:
                try: frame['expected_runtime'] = runtime()
                except Exception: frame['incomplete'] = True
        return perform(ledger, config, ROOT)
    return wrapped


def mail_registration(function):
    @functools.wraps(function)
    def wrapped(policy, root):
        if not enabled() or _STAGE.get() is None or _STAGE.get()['stage'] != 'delivery':
            return function(policy, root)
        paths = ('contacts.json','CONTACTS.md','POSITIVE.md','REFUSALS.md','ACTIONS.md','SUBMISSIONS.md')
        def perform(evidence):
            function(policy, root)
            result = {}
            for name in paths:
                path = Path(root) / 'tables' / name
                if path.is_file():
                    if path.stat().st_size > MAX_STAGE_FILE_BYTES: _STAGE.get()['incomplete'] = True
                    else: result['tables/'+name] = base64.b64encode(path.read_bytes()).decode()
            return result
        _capture('registry', 'register', perform, (policy.get('mail_thread_contacts', []),), {})
    return wrapped


def snapshot_receipt(path, snapshot):
    """Read only the verified closed restore input; never infer from live rows."""
    if not snapshot: return None
    import sqlite3
    from types import SimpleNamespace
    from .scenario_contract import input_digest
    try:
        with Path(path).open('rb') as source:
            if hashlib.file_digest(source, 'sha256').hexdigest() != snapshot['sha256']: return None
        db = sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro&immutable=1',uri=True)
        try: logical = input_digest(SimpleNamespace(db=db))
        finally: db.close()
        return {'generation': snapshot['generation'], 'digest': snapshot['sha256'], 'logical_digest': logical}
    except Exception: return None


def queue_clock(clock):
    if not enabled() or _STAGE.get() is None or _STAGE.get().get('stage') != 'delivery': return clock
    frame = _STAGE.get()
    def observed():
        value = clock()
        try: frame.setdefault('queue_clock_values', []).append(value)
        except Exception: frame['incomplete'] = True
        return value
    return observed


def issue_stage(stage):
    @functools.wraps(stage)
    def invoke(ledger, config, root, github=None):
        return stage(ledger, config, github)
    observe = stage_frame('formalization')(invoke)
    @functools.wraps(stage)
    def wrapped(ledger, config, github=None):
        if not enabled(): return stage(ledger, config, github)
        return observe(ledger, config, ledger.folder, github)
    return wrapped


def inbound_stage(stage):
    def invoke(ledger, row, root, mailbox):
        return stage(row, mailbox, ledger, root)
    observe = stage_frame('inbound')(invoke)
    @functools.wraps(stage)
    def wrapped(row, mailbox, ledger, root):
        if not enabled(): return stage(row, mailbox, ledger, root)
        return observe(ledger, row, root, mailbox)
    return wrapped


def intake_binding(function):
    @functools.wraps(function)
    def wrapped(*args, **kwargs):
        result = function(*args, **kwargs)
        if enabled() and (frame := _STAGE.get()) is not None and frame.get('stage') == 'intake':
            try: frame['binding'] = result
            except Exception: frame['incomplete'] = True
        return result
    return wrapped


def evidence_download(function):
    @functools.wraps(function)
    def wrapped(row, name, directory, releases):
        if not enabled() or (_STAGE.get() or {}).get('stage') != 'prior-work':
            return function(row, name, directory, releases)
        result = None
        def read(*_args):
            nonlocal result
            result = function(row, name, directory, getattr(releases,'target',releases))
            return result
        _capture('evidence','download',read,(row['id'],name,Path(directory)),{})
        return result
    return wrapped
