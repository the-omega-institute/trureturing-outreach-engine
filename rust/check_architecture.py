#!/usr/bin/env python3
"""Check workspace dependency direction without building or accessing data."""
from pathlib import Path
import json
import subprocess

ROOT = Path(__file__).resolve().parent
ALLOWED = {
    'outreach-canon': set(),
    'outreach-model': {'outreach-canon'},
    'outreach-core': {'outreach-model', 'outreach-canon'},
    'outreach-cli': {'outreach-canon', 'outreach-model', 'outreach-core'},
}


def check():
    metadata = json.loads(subprocess.check_output(
        ['cargo', 'metadata', '--locked', '--no-deps', '--format-version', '1'], cwd=ROOT, text=True))
    packages = {p['name']: p for p in metadata['packages'] if p['id'] in metadata['workspace_members']}
    assert set(packages) == set(ALLOWED), 'Update the explicit workspace boundary rules for new crates'
    for name, manifest in packages.items():
        for dep in manifest['dependencies']:
            if dep['kind'] == 'dev':
                continue
            # cargo metadata resolves aliases, workspace inheritance, paths and
            # target-specific declarations before this comparison.
            package = dep['name']
            assert package not in packages or package in ALLOWED[name], f'{name} -> {package}'
    print('Workspace dependency direction: passed')


if __name__ == '__main__':
    check()
