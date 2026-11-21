"""Branch-aware persistence for maintained data tables."""
import base64
import json
from pathlib import Path
import subprocess
import tempfile
from urllib.parse import urlencode

from .location import DATA_BRANCH, DATA_ROOT

# Git requires an author address. This synthetic one carries no account identity.
AUTHOR = ('-c', 'user.name=Outreach automation', '-c', 'user.email=automation@example.invalid')


def git(root, *args, check=True):
    return subprocess.run(['git', *AUTHOR, *args], cwd=root, check=check)


def commit_paths(paths, message, root=DATA_ROOT):
    changes = subprocess.check_output(['git', 'status', '--porcelain', '--', *paths], cwd=root)
    if not changes:
        return False
    git(root, 'add', '--', *paths)
    git(root, 'diff', '--cached', '--check', '--', *paths)
    git(root, 'commit', '--only', '-m', message, '--', *paths)
    return True


def push(root=DATA_ROOT, *, check=True):
    return git(root, 'push', 'origin', f'HEAD:refs/heads/{DATA_BRANCH}', check=check)


def fetch(root=DATA_ROOT):
    git(root, 'fetch', 'origin', f'refs/heads/{DATA_BRANCH}')
    return 'FETCH_HEAD'


def contents_update(run, repo, path, transform, message):
    endpoint = f'repos/{repo}/contents/{path}'
    current = json.loads(run('api', endpoint + '?' + urlencode({'ref': DATA_BRANCH})))
    original = base64.b64decode(current['content'])
    updated, result = transform(original)
    if updated != original:
        body = {'message': message, 'sha': current['sha'], 'branch': DATA_BRANCH,
                'content': base64.b64encode(updated).decode('ascii')}
        with tempfile.TemporaryDirectory() as directory:
            request = Path(directory) / 'update.json'
            request.write_text(json.dumps(body), encoding='utf-8')
            run('api', '--method', 'PUT', endpoint, '--input', str(request))
    return result
