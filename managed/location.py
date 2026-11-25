"""Locations shared by the engine, private data checkout and GitHub clients."""
from . import recording

import json
import os
from pathlib import Path
import subprocess

ENGINE_ROOT = Path(__file__).resolve().parents[1]


def hosted(env=None):
    env = os.environ if env is None else env
    return env.get('GITHUB_ACTIONS') == 'true' and bool(env.get('OUTREACH_DATA_ROOT'))


def resolve(env=None):
    env = os.environ if env is None else env
    data_root = Path(env.get('OUTREACH_DATA_ROOT') or ENGINE_ROOT).resolve()
    repositories = {}
    if not hosted(env):
        config = data_root / 'config.json'
        if not config.is_file() and data_root == ENGINE_ROOT:
            config = data_root / 'private-data/config.json'
        if not config.is_file():
            config = ENGINE_ROOT / 'managed/config.example.json'
        repositories = json.loads(config.read_bytes()).get('repositories', {})
    data_repo = env.get('OUTREACH_DATA_REPO') or repositories.get('data', '')
    data_branch = env.get('OUTREACH_DATA_BRANCH') or 'main'
    engine_repo = env.get('OUTREACH_ENGINE_REPO') or repositories.get('engine') or data_repo
    return data_root, data_repo, data_branch, engine_repo


DATA_ROOT, DATA_REPO, DATA_BRANCH, ENGINE_REPO = resolve()


def load_config(root=DATA_ROOT, *, production=None):
    root = Path(root)
    if production is None:
        production = hosted()
    path = root / 'config.json'
    if not production and root == ENGINE_ROOT and not path.is_file():
        path = root / 'private-data/config.json'
    if not path.is_file():
        raise FileNotFoundError('Missing private data config: place config.json at OUTREACH_DATA_ROOT')
    with path.open(encoding='utf-8') as stream:
        return json.load(stream)


def source_head(root=None):
    return recording.check_output(['git', 'rev-parse', 'HEAD'], cwd=root or ENGINE_ROOT, text=True).strip()


def example_config():
    return json.loads((ENGINE_ROOT / 'managed/config.example.json').read_text(encoding='utf-8'))


def settings(section):
    """Read installation-specific settings lazily, also allowing offline tools."""
    try:
        return load_config()[section]
    except FileNotFoundError:
        if hosted():
            raise
        return example_config()[section]


def data_file(root, name):
    root = Path(root)
    if root == ENGINE_ROOT and (root / 'private-data').is_dir():
        return root / 'private-data' / name
    return root / name


def owner_labels():
    return settings('identity')['owner_labels']
