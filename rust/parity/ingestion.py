"""Local replay adapters to the live Python writer; no transport or original writes."""
from contextlib import ExitStack, redirect_stderr, redirect_stdout
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from unittest.mock import patch

ENGINE = Path(__file__).resolve().parents[2]
RUST = ENGINE / 'rust'
sys.path.insert(0, str(ENGINE))
os.environ['OUTREACH_DATA_ROOT'] = str(RUST / 'vectors/nonexistent')
os.environ['GITHUB_ACTIONS'] = 'false'
from managed import control, location, automation, policy
from managed.queue import canonical, digest
from managed.tasks import Tasks


def assets(directory):
    manifest = directory / 'assets.json'
    if manifest.is_file():
        return json.loads(manifest.read_text())
    return [{'id': p.name, 'name': p.name, 'digest': 'sha256:' + hashlib.sha256(p.read_bytes()).hexdigest()}
            for p in sorted(directory.glob('*.json'))]


def safe_child(root, name):
    root = root.resolve()
    path = (root / name).resolve()
    if not path.is_relative_to(root) or any(p in ('', '.', '..') for p in Path(name).parts) or Path(name).is_absolute():
        raise ValueError('Unsafe input asset path')
    return path


class LocalReleases:
    """Only recorded local bytes; effects are retained in temporary memory."""
    tag = 'offline-ingestion-replay'
    repo = 'synthetic/offline'

    def __init__(self, directory):
        self.directory = directory
        self.preserved = {}

    def release(self):
        return {'assets': assets(self.directory)}

    def download(self, name, folder):
        path = Path(folder) / name
        path.write_bytes(safe_child(self.directory, name).read_bytes())
        return path

    def preserve_task(self, path, tag):
        self.preserved[tag, Path(path).name] = Path(path).read_bytes()

    def run(self, *args):
        assert args[:2] == ('release', 'download'), 'Unrecorded transport operation'
        tag = args[2]
        name = args[args.index('--pattern') + 1]
        destination = Path(args[args.index('--dir') + 1]) / name
        raw = self.preserved.get((tag, name))
        if raw is None:
            try:
                path = safe_child(self.directory / 'resources' / tag, name)
                if not path.is_relative_to(self.directory.resolve()):
                    raise ValueError('Recorded artifact is unavailable')
                raw = path.read_bytes()
            except OSError:
                raise ValueError('Recorded artifact is unavailable') from None
        destination.write_bytes(raw)


def raw_dump(db):
    tables = {}
    for (name,) in db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
        quoted = '"' + name.replace('"', '""') + '"'
        info = list(db.execute(f'PRAGMA table_info({quoted})'))
        columns = [r[1] for r in info]
        pk = [r[1] for r in sorted(info, key=lambda r: r[5]) if r[5]]
        rows = []
        for row in db.execute(f'SELECT * FROM {quoted} ORDER BY rowid'):
            decoded = {}
            for column, value in zip(columns, row):
                if column in ('request', 'data', 'payload', 'packet', 'receipt', 'evidence', 'response', 'snapshot', 'envelope', 'authority', 'readback') and isinstance(value, str):
                    try:
                        value = json.loads(value)
                    except ValueError:
                        pass
                if isinstance(value, bytes):
                    value = {'$sqlite_blob': value.hex()}
                decoded[column] = value
            rows.append(decoded)
        tables[name] = {'columns': columns, 'primary_key': pk, 'rows': rows}
    return tables


def logical_dump(tables):
    fields = []
    def add(value, key):
        if isinstance(value, dict) and type(value.get(key)) in (int, float):
            fields.append((value, key))
    for name, table in tables.items():
        for row in table['rows']:
            for column in ('at', 'updated'):
                add(row, column)
            if name == 'tasks':
                data = row['data']
                add(data.get('reviewer_edit', {}), 'at')
                add(data, 'owner_question_opened_at')
            if name == 'pipeline_state' and row['key'] in ('sandbox_pool', 'sandbox_pool_cleanup'):
                for entry in row['payload']:
                    add(entry, 'idle_since'); add(entry, 'rotated'); add(entry.get('readopted', {}), 'rotated')
    ranks = {t: n for n, t in enumerate(sorted({value[key] for value, key in fields}))}
    for value, key in fields:
        value[key] = {'$time_order': ranks[value[key]]}
    for table in tables.values():
        def key(row):
            return tuple((0, row[k]) if type(row[k]) is int else (1, row[k] if isinstance(row[k], str) else canonical(row[k]))
                         for k in table['primary_key']) + ((2, canonical(row)),)
        table['rows'].sort(key=key)
    return tables


def workflow_reader(root, engine=ENGINE):
    def read(path, *unused):
        overlay = root / 'policy/managed' / path
        return (overlay if not path.endswith('.py') and overlay.is_file() else engine / 'managed' / path).read_bytes().decode('utf-8')
    return read


def python_replay(snapshot, directory, root, secret, *, passes=1, generation=0, now=2_000_000_000.0):
    with tempfile.TemporaryDirectory(prefix='python-ingestion-') as temp:
        path = Path(temp) / 'copy.sqlite3'
        source = sqlite3.connect(snapshot.resolve().as_uri() + '?mode=ro&immutable=1', uri=True)
        target = sqlite3.connect(path)
        try:
            source.backup(target)
        finally:
            source.close()
            target.close()
        ledger = control.Ledger.__new__(control.Ledger)
        ledger.tasks = Tasks(path, clock=lambda: now)
        ledger.folder = Path(temp)
        ledger.releases = LocalReleases(directory)
        ledger.generation = generation
        ledger.saved_version = None
        def save(**unused):
            version = ledger.version()
            if version != ledger.saved_version:
                ledger.generation += 1
                ledger.saved_version = version
        ledger.save = save
        config = json.loads((root / 'config.json').read_text())
        try:
            with ExitStack() as stack:
                stack.enter_context(patch.object(control, 'ROOT', root))
                stack.enter_context(patch.object(control, 'workflow_text', side_effect=workflow_reader(root)))
                stack.enter_context(patch.object(automation, 'policy_text', side_effect=lambda path, **kwargs:
                    policy.policy_text(path, data_root=root, engine_root=ENGINE, **kwargs)))
                stack.enter_context(patch.object(location, 'load_config', return_value=config))
                stack.enter_context(redirect_stdout(io.StringIO()))
                stack.enter_context(redirect_stderr(io.StringIO()))
                # Adapters alone are replaced. All field, result, signature,
                # workflow, state and ingestion gates are the production code.
                errors = [ledger.ingest(secret or None) for _ in range(passes)]
            return {'errors': errors, 'tables': logical_dump(raw_dump(ledger.tasks.db)), 'generation': ledger.generation}
        finally:
            ledger.tasks.close()


def fingerprint(snapshot):
    result = {}
    for path in [snapshot, *(Path(str(snapshot) + suffix) for suffix in ('-wal', '-shm', '-journal'))]:
        if path.exists():
            result[str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()
    return result


def compare(snapshot, directory, root, secret, *, passes=1, generation=0, now=2_000_000_000.0):
    before = fingerprint(snapshot)
    command = [str(RUST / 'target/debug/outreach'), 'control', 'replay-ingest',
               '--snapshot', str(snapshot), '--inputs', str(directory), '--root', str(root),
               '--engine-root', str(ENGINE), '--passes', str(passes), '--generation', str(generation), '--now', str(now)]
    env = {**os.environ, 'OUTREACH_REVIEW_KEY': secret}
    if not secret:
        command.append('--no-key')
    rust = subprocess.run(command, capture_output=True, env=env)
    assert rust.returncode == 0, f'Rust replay failed: exit={rust.returncode}'
    assert b'panicked' not in rust.stderr
    actual = json.loads(rust.stdout)
    expected = python_replay(snapshot, directory, root, secret, passes=passes, generation=generation, now=now)
    assert before == fingerprint(snapshot), 'Original snapshot or side file changed'
    return expected, actual


def build():
    subprocess.run(['cargo', 'build', '--locked', '-q', '-p', 'outreach-cli'], cwd=RUST, capture_output=True, check=True)
