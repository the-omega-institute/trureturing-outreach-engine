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


def check_decoder_source(source, label):
    assert not re.search(r'serde_json\s*::\s*(?:from_\w+|Deserializer)', source), label
    aliases = {'Value'} | set(re.findall(r'\bValue\s+as\s+(\w+)', source))
    for alias in aliases:
        assert not re.search(r'\bparse\s*::\s*<[^>]*\b' + re.escape(alias) + r'\s*>', source), label


def check_decoder_guard():
    for source in ['raw.parse::<Value>()', 'str::parse::<outreach_canon::Value>(raw)',
                   'use outreach_canon::Value as Json; raw.parse::<Json>()',
                   'serde_json::from_slice(raw)']:
        try:
            check_decoder_source(source, 'synthetic forbidden decoder')
        except AssertionError:
            continue
        raise AssertionError('Decoder guard accepted ' + source)
    check_decoder_source('CanonValue::parse(raw)?; state.parse::<TaskState>()?', 'allowed decoders')


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
        check_decoder_source(path.read_text(), str(path))
    # Replay is an owned model in core, never a SQLite/network writer. Examples
    # are development adapters and may read stdin; production core cannot do I/O.
    for path in (ROOT / 'crates/outreach-core/src').rglob('*.rs'):
        source = path.read_text()
        for forbidden in ('std::fs', 'std::process', 'std::io', 'rusqlite',
                          'TcpStream', 'UdpSocket', 'reqwest'):
            assert forbidden not in source, f'{path}: pure core contains {forbidden}'
    ledger = (ROOT / 'crates/outreach-ledger/src/lib.rs').read_text()
    assert 'SQLITE_OPEN_READ_ONLY' in ledger and 'mode=ro&immutable=1' in ledger
    assert 'SQLITE_OPEN_READ_WRITE' not in ledger and 'SQLITE_OPEN_CREATE' not in ledger
    check_decoder_guard()
    print('Workspace dependency direction and canon decoder boundary: passed')


if __name__ == '__main__':
    check()
