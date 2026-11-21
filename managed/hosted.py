"""GitHub-hosted commands with one private output boundary."""
import base64
import json
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory

from .location import DATA_BRANCH, DATA_REPO, DATA_ROOT, load_config
from .run_log import public_run, register_secret


def data_checkout():
    from .control import KINDS, Releases, workflow
    Releases().require_private()
    if not DATA_ROOT.is_dir():
        DATA_ROOT.mkdir()
    token = os.environ['GH_TOKEN']
    auth = base64.b64encode(('x-access-token:' + token).encode()).decode()
    register_secret(auth)
    url = f'https://github.com/{DATA_REPO}.git'
    subprocess.run(['git', 'init', '--quiet', str(DATA_ROOT)], check=True)
    subprocess.run(['git', 'remote', 'add', 'origin', url], cwd=DATA_ROOT, check=True)
    subprocess.run(['git', '-c', f'http.https://github.com/.extraheader=AUTHORIZATION: basic {auth}',
                    'fetch', '--quiet', '--no-tags', 'origin', f'refs/heads/{DATA_BRANCH}'],
                   cwd=DATA_ROOT, check=True)
    subprocess.run(['git', 'checkout', '--quiet', '--detach', 'FETCH_HEAD'],
                   cwd=DATA_ROOT, check=True)
    # Save scoped authentication inside the private checkout for subsequent git operations.
    subprocess.run(['git', 'config', 'http.https://github.com/.extraheader', 'AUTHORIZATION: basic ' + auth],
                   cwd=DATA_ROOT, check=True)
    load_config(production=True)
    for kind in sorted(KINDS):
        workflow(kind)


def credentials():
    if not os.environ.get('OUTREACH_NYX_SESSION'):
        return
    subprocess.run([sys.executable, '-m', 'pip', 'install', '--quiet', 'PyNaCl==1.6.2', 'msal==1.37.0'],
                   check=True)
    from .credentials import main
    return main(['hosted'] + (['--force-refresh'] if os.environ.get('FORCE_REFRESH') == 'true' else []))


def restore_credentials():
    path = os.environ.get('OUTREACH_CREDENTIAL_FILE')
    if path and Path(path).is_file():
        for line in Path(path).read_text().splitlines():
            key, value = line.split('=', 1)
            register_secret(value)
            os.environ[key] = value


def coverage():
    from . import data_writes, maintenance
    paths = ('tables/results.json',)
    with TemporaryDirectory() as directory:
        upstream = Path(directory) / 'upstream'
        subprocess.run(['git', 'clone', '--quiet', '--filter=blob:none', '--depth=1', '--no-checkout',
                        '--single-branch', '--branch', 'dev',
                        'https://github.com/the-omega-institute/trureturing.git', str(upstream)], check=True)
        subprocess.run(['git', '-C', str(upstream), 'sparse-checkout', 'set', 'Blueprint'], check=True)
        subprocess.run(['git', '-C', str(upstream), 'checkout', '--quiet', 'dev'], check=True)
        for attempt in range(3):
            print(maintenance.upstream(upstream, 'HEAD'))
            print(maintenance.check())
            if not data_writes.commit_paths(paths, 'Refresh upstream short-note coverage'):
                return 0
            if data_writes.push(check=False).returncode == 0:
                return 0
            if attempt < 2:
                source = data_writes.fetch()
                data_writes.git(DATA_ROOT, 'reset', '--hard', source)
    return 1


def foundation(action):
    env = {**os.environ, 'OUTREACH_PUBLIC_LOG': '0', 'GIT_ALLOW_PROTOCOL': 'file'}
    if action == 'foundation':
        subprocess.run([sys.executable, '-m', 'managed.maintenance', 'check-engine'], env=env, check=True)
        directory = 'managed/tests'
    else:
        directory = 'scripts/short_notes'
    return subprocess.run([sys.executable, '-m', 'unittest', 'discover', '-s', directory], env=env).returncode


def command(action):
    if action in {'foundation', 'delivery-tests'}:
        return foundation(action)
    if action == 'checkout':
        return data_checkout()
    if action == 'credentials':
        return credentials()
    if action == 'coverage':
        return coverage()
    restore_credentials()
    if action == 'control':
        from .control import main
        return main([os.environ.get('CONTROL_ACTION') or 'tick'])
    if action == 'submissions':
        from .submission_sync import main
        return main(['--apply', '--push'])
    from scripts.short_notes import service
    if action == 'pages':
        try:
            result = service.execute(service.parse_args(['verify-feed']))
        except (ValueError, RuntimeError, OSError, KeyError, TypeError) as exc:
            print(type(exc).__name__, str(exc))
            result = {'status': 'failed'}
        print(service.w.encode(result).decode(), end='')
        if result.get('status') == 'deployed':
            return 0
        return service.main(['sync'])
    selection = json.loads(os.environ.get('OUTREACH_NOTE_REQUEST') or '{}')
    for key, variable in (('id', 'NOTE_ID'), ('revision', 'NOTE_REVISION'),
                          ('recover_record', 'RECOVER_RECORD'), ('reuse_draft_from', 'REUSE_DRAFT_FROM'),
                          ('limit', 'BATCH_LIMIT'), ('publication', 'PREFERRED_PUBLICATION')):
        if key in selection:
            value = json.dumps(selection[key]) if key == 'publication' else str(selection[key])
            os.environ[variable] = value
    return service.main(['ci'])


if __name__ == '__main__':
    action = sys.argv[1] if len(sys.argv) == 2 else ''
    if action not in {'checkout', 'credentials', 'control', 'coverage', 'submissions', 'pages', 'notes', 'foundation', 'delivery-tests'}:
        raise SystemExit(2)
    tick = action == 'control' and (os.environ.get('CONTROL_ACTION') or 'tick') == 'tick'
    persist = action not in {'foundation', 'delivery-tests'}
    raise SystemExit(public_run(lambda: command(action), action, tick=tick, persist=persist))
