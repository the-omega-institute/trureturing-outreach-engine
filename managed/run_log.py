"""Keep hosted command output in the private control Release."""
from contextlib import ExitStack, redirect_stderr, redirect_stdout
from contextvars import ContextVar
from datetime import datetime, timezone
import hashlib
import os
from pathlib import Path
import re
import sys
import tempfile
import time
import traceback
from uuid import uuid4

from .location import hosted

OUTPUT = ContextVar('public_output', default=None)
SECRETS = ContextVar('private_secrets', default=None)
PRIVATE_LOG_BYTES = 2 * 1024 * 1024
TICK_LOGS_KEPT = 3
RUN_LOGS_KEPT = 3  # Runs per workflow, including every step log in each run.
_LOCAL_RUN = uuid4().hex[:24]
TRUNCATED = b'\n[... private log truncated; beginning and end retained ...]\n'


def enabled():
    return hosted() or os.environ.get('OUTREACH_PUBLIC_LOG', '0') == '1'


def coordinator_run_id():
    """Share one retention unit across the separate Actions step processes."""
    run = os.environ.get('GITHUB_RUN_ID')
    if run:
        return hashlib.sha256((run+':'+os.environ.get('GITHUB_RUN_ATTEMPT','1')).encode()).hexdigest()[:24]
    return _LOCAL_RUN


LOG_NAME = re.compile(r'(?:run|tick)-log-\d{8}T\d{12}Z-(?:workflow-([a-f0-9]{24})-)?run-([a-f0-9]{24})-[a-f0-9]{12}\.log')


def log_run_id(name):
    match = LOG_NAME.fullmatch(name)
    # Legacy logs have no verified run binding; never group them by guesswork.
    return match[2] if match else name


def log_workflow_id(name):
    match = LOG_NAME.fullmatch(name)
    # Historical names use the shared migration window until they age out.
    return match[1] if match else None


def log_name(*, tick=False):
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    workflow = (os.environ.get('GITHUB_WORKFLOW_REF') or '').split('@',1)[0]
    workflow = workflow or os.environ.get('GITHUB_WORKFLOW') or 'local'
    workflow_id = hashlib.sha256(workflow.encode()).hexdigest()[:24]
    prefix = 'tick-log' if tick else 'run-log'
    return f'{prefix}-{stamp}-workflow-{workflow_id}-run-{coordinator_run_id()}-{uuid4().hex[:12]}.log'


def register_secret(value):
    secrets = SECRETS.get()
    if secrets is None:
        return False
    if value:
        secrets.add(value)
    return True


def phase_timing(name, seconds, count):
    emit = OUTPUT.get()
    if emit is not None and name in {
        'restore', 'ingest', 'oracle-capacity', 'mailbox-intake', 'pipeline',
        'credit-recovery', 'execution-recovery', 'pool-health', 'pool-idle',
        'approved', 'advance', 'firstcontact-history', 'automation', 'delivery',
        'sent-events', 'mathesis', 'mathesis-delivery', 'formalization-requests', 'prior-work-issues',
        'maintenance', 'owner-drafts', 'final-save', 'independent-review',
    }:
        emit(f'tick-phase {name} {seconds:.3f}s units={count:d}')


def _restore(fd, saved):
    os.dup2(saved, fd)
    os.close(saved)


def _upload(path, tick):
    from .control import Releases

    releases = Releases()
    releases.require_private()
    releases.ensure()
    # Reserve space before uploading, including at the release's asset cap.
    try:releases.prune_logs(TICK_LOGS_KEPT if tick else RUN_LOGS_KEPT, tick=tick,incoming=path.name)
    except Exception as exc:print('connector-error log-prune '+type(exc).__name__,file=sys.stderr)
    releases.upload_retained(path)


def _excerpt(raw, limit, secrets):
    """Read a bounded head/tail, redacting before cutting either boundary."""
    variants = set()
    for secret in secrets:
        normalized = secret.replace('\r\n','\n')
        variants.update(value.encode('utf-8') for value in
                        (secret, normalized, normalized.replace('\n','\r\n')) if value)
    def redact(value):
        for encoded in sorted(variants,key=len,reverse=True):
            # Equal byte lengths keep the head/tail boundary fixed even when
            # several secrets cross it. Mask before slicing off the overlap.
            replacement=(b'[redacted]'+b'*'*len(encoded))[:len(encoded)]
            value=value.replace(encoded,replacement)
        return value.decode('utf-8',errors='replace').encode('utf-8')
    def head(value,size):return value[:size].decode('utf-8',errors='ignore').encode('utf-8')
    def tail(value,size):return value[-size:].decode('utf-8',errors='ignore').encode('utf-8')
    # Read enough overlap to redact secrets crossing an excerpt boundary.
    overlap=max(map(len,variants),default=0)
    with raw.open('rb') as stream:
        size=stream.seek(0,os.SEEK_END);stream.seek(0)
        if size<=limit:
            value=redact(stream.read(limit))
            if len(value)<=limit:return value
            beginning=ending=value
        else:
            half=(limit-len(TRUNCATED))//2
            beginning=redact(stream.read(half+overlap))
            stream.seek(max(0,size-half-overlap))
            ending=redact(stream.read(half+overlap))
        half=(limit-len(TRUNCATED))//2
        return head(beginning,half)+TRUNCATED+tail(ending,half)


def public_run(entry, action, *, tick=False, persist=True):
    """Capture Python and native output at the file-descriptor boundary."""
    if not enabled() or OUTPUT.get() is not None:
        return entry()
    started = time.monotonic()
    name = log_name(tick=tick)
    stamp = name.split('-')[2]
    incident = name.rsplit('-',1)[1][:-4]
    diagnostic = None
    result = 0
    with tempfile.TemporaryDirectory() as directory:
        raw = Path(directory) / 'capture'
        path = Path(directory) / name
        with raw.open('w+', encoding='utf-8', errors='replace') as log, ExitStack() as stack:
            public = sys.stderr
            sys.stdout.flush()
            public.flush()
            # This descriptor stays open while descriptors 1 and 2 are private.
            if hasattr(public, 'fileno'):
                try:
                    public = stack.enter_context(os.fdopen(os.dup(public.fileno()), 'w', encoding='utf-8'))
                except (OSError, ValueError):
                    pass

            def emit(line):
                print(line, file=public, flush=True)

            for fd in (1, 2):
                saved = os.dup(fd)
                stack.callback(_restore, fd, saved)
                os.dup2(log.fileno(), fd)
            stack.enter_context(redirect_stdout(log))
            stack.enter_context(redirect_stderr(log))
            stack.callback(OUTPUT.reset, OUTPUT.set(emit))
            secrets = {value for key, value in os.environ.items()
                       if value and key in {'GH_TOKEN', 'GITHUB_TOKEN', 'OUTREACH_DATA_TOKEN',
                                            'CMA_ACCESS_TOKEN', 'OUTREACH_GRAPH_ACCESS_TOKEN',
                                            'MATHESIS_ACCESS_TOKEN', 'OUTREACH_REVIEW_KEY',
                                            'OUTREACH_NYX_SESSION', 'OUTREACH_GRAPH_CACHE',
                                            'OUTREACH_NOTE_REQUEST', 'ZENODO_TOKEN', 'PAGES_TOKEN'}}
            stack.callback(SECRETS.reset, SECRETS.set(secrets))
            try:
                result = entry() or 0
            except BaseException as exc:
                if isinstance(exc, SystemExit) and exc.code in (None, 0):
                    result = 0
                else:
                    traceback.print_exc(file=log)
                    kind = type(exc).__name__
                    if not re.fullmatch(r'[A-Za-z_][A-Za-z_0-9]{0,63}', kind):
                        kind = 'Exception'
                    diagnostic = f'{kind} incident-{incident}'
                    result = exc.code if isinstance(exc, SystemExit) and isinstance(exc.code, int) else 1
            sys.stdout.flush();sys.stderr.flush();log.flush()
            # GitHub rejects empty release assets, so a quiet successful step
            # still records which action ran, when, and how it ended.
            header = f'{action} {stamp} incident-{incident} exit={result}\n'
            header=header.encode('utf-8')
            path.write_bytes(header + _excerpt(raw,PRIVATE_LOG_BYTES-len(header),secrets))
            if persist:
                try:
                    _upload(path, tick)
                except Exception:
                    emit(f'log-upload failed incident-{incident}')
                    result = result or 1
            if diagnostic:
                emit(diagnostic)
            emit(f'{action} {"failed" if result else "complete"} {time.monotonic()-started:.3f}s')
    return result

