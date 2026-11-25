"""Resolve policy from the data overlay and executable source from the engine."""
from functools import lru_cache
from pathlib import Path
from . import recording

import re
import subprocess

from .location import DATA_ROOT, ENGINE_ROOT, hosted


class MissingPolicy(RuntimeError):
    """A required operative policy file is missing in hosted mode."""


@lru_cache(maxsize=512)
def verify_commit(root, revision):
    if not re.fullmatch(r'[0-9a-f]{40}', revision):
        raise ValueError('Pin the workflow commit')
    result = recording.run(['git', 'cat-file', '-t', revision], cwd=root, capture_output=True)
    if result.returncode or result.stdout.strip() != b'commit':
        raise ValueError(f'Workflow commit unavailable at {revision}')


@lru_cache(maxsize=512)
def committed_text(root, revision, path):
    if not re.fullmatch(r'[0-9a-f]{40}', revision):
        raise ValueError('Pin the workflow commit')
    object_name = f'{revision}:{path}'
    result = recording.run(['git', 'show', object_name], cwd=root, capture_output=True)
    if result.returncode:
        entry = recording.run(['git', 'ls-tree', revision, '--', path], cwd=root,
                               capture_output=True)
        if entry.returncode == 0 and not entry.stdout:
            raise FileNotFoundError(f'File absent at {revision}: {path}')
        raise ValueError(f'Workflow source unavailable at {revision}: {path}')
    return result.stdout.decode('utf-8')


@recording.policy_read
def policy_text(path, *, data_root=DATA_ROOT, engine_root=ENGINE_ROOT,
                source_revision=None, policy_revision=None, required=True, engine_path=None,
                read_commit=committed_text):
    """Read a repository-relative path using the working tree or the pinned trees."""
    if source_revision is not None and policy_revision is None:
        return read_commit(data_root, source_revision, path)

    if not path.endswith('.py'):
        overlay = f'policy/{path}'
        try:
            if policy_revision is not None:
                return read_commit(data_root, policy_revision, overlay)
            return (Path(data_root) / overlay).read_bytes().decode('utf-8')
        except FileNotFoundError as exc:
            if required and hosted():
                raise MissingPolicy(f'Operative policy missing: {path}') from exc

    if source_revision is not None:
        return read_commit(engine_root, source_revision, path)
    return (Path(engine_root) / (engine_path or path)).read_bytes().decode('utf-8')
