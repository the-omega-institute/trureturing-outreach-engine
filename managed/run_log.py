"""Keep hosted command output in the private control Release."""
from contextlib import ExitStack, redirect_stderr, redirect_stdout
from contextvars import ContextVar
from datetime import datetime, timezone
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


def enabled():
    return hosted() or os.environ.get('OUTREACH_PUBLIC_LOG', '0') == '1'


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
        'sent-events', 'mathesis', 'formalization-requests', 'prior-work-issues',
        'maintenance', 'owner-drafts', 'final-save',
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
    releases.upload(path)
    releases.prune_logs(200 if tick else 40, tick=tick)


def public_run(entry, action, *, tick=False, persist=True):
    """Capture Python and native output at the file-descriptor boundary."""
    if not enabled() or OUTPUT.get() is not None:
        return entry()
    started = time.monotonic()
    incident = uuid4().hex[:12]
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    prefix = 'tick-log' if tick else 'run-log'
    name = f'{prefix}-{stamp}-{incident}.log'
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
            log.seek(0)
            detailed = log.read()
            for secret in sorted(secrets, key=len, reverse=True):
                detailed = detailed.replace(secret, '[redacted]')
            path.write_text(detailed, encoding='utf-8')
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

