#!/usr/bin/env python3
"""Check workspace dependency direction without building or accessing data."""
from pathlib import Path
import json
import re
import subprocess

ROOT = Path(__file__).resolve().parent
ALLOWED = {
    'outreach-canon': set(),
    'outreach-model': {'outreach-canon'},
    'outreach-core': {'outreach-model', 'outreach-canon'},
    'outreach-ledger': {'outreach-model', 'outreach-canon'},
    'outreach-cli': {'outreach-canon', 'outreach-model', 'outreach-core', 'outreach-ledger'},
}


def check():
    metadata = json.loads(subprocess.check_output(
        ['cargo', 'metadata', '--locked', '--no-deps', '--format-version', '1'], cwd=ROOT, text=True))
    packages = {p['name']: p for p in metadata['packages'] if p['id'] in metadata['workspace_members']}
    assert set(packages) == set(ALLOWED), 'Update the explicit workspace boundary rules for new crates'
    for name, manifest in packages.items():
        for dep in manifest['dependencies']:
            assert name == 'outreach-canon' or dep['name'] != 'serde_json', f'{name}: JSON decoders belong in canon'
            if dep['kind'] == 'dev':
                continue
            # cargo metadata resolves aliases, workspace inheritance, paths and
            # target-specific declarations before this comparison.
            package = dep['name']
            assert package not in packages or package in ALLOWED[name], f'{name} -> {package}'
    # Incoming JSON in adapters/models must enter through CanonValue/parse.
    # Ban every serde_json decoder, not just from_str, outside the canon crate.
    for path in (ROOT / 'crates').rglob('*.rs'):
        if 'outreach-canon' in path.parts:
            continue
        assert not re.search(r'serde_json\s*::\s*(?:from_\w+|Deserializer)', path.read_text()), str(path)
    print('Workspace dependency direction and canon decoder boundary: passed')


if __name__ == '__main__':
    check()
