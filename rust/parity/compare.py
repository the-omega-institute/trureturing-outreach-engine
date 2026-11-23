#!/usr/bin/env python3
"""Byte parity against live Python. All generated ledgers/output are temporary.

--snapshot is a local-only private comparison: no output payloads or diffs are
printed/saved, and the immutable input's bytes are checked before/after. Supply
--root (or OUTREACH_DATA_ROOT) for the same config/tables/policy as Python. A
--work file enables the two work-consuming commands on a private comparison.
"""
import argparse
import hashlib
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

RUST = Path(__file__).resolve().parents[1]
ENGINE = RUST.parent


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').digest()


def compare(binary, root, snapshot, work, now, config=None):
    commands = [['status', '--compact'], ['reviews'], ['exceptions'], ['exceptions', '--format', 'text']]
    if work:
        commands += [['review-digest', '--work', str(work)], ['review-digest', '--work', str(work), '--format', 'json'],
                     ['exceptions', '--work', str(work)], ['exceptions', '--work', str(work), '--format', 'text']]
    before = sha(snapshot)
    sidecars = [Path(str(snapshot) + suffix) for suffix in ('-wal', '-shm', '-journal')]
    original_files = {p for p in sidecars if p.exists()}
    for command in commands:
        common = ['--root', str(root), '--snapshot', str(snapshot), '--now', str(now)]
        if config: common += ['--config', str(config)]
        py = subprocess.run([sys.executable, str(RUST / 'parity/python_control.py'), *common, *command], capture_output=True)
        rs = subprocess.run([str(binary), 'control', *common, *command], capture_output=True)
        label = ' '.join(arg if arg != str(work) else '<work>' for arg in command)
        if py.returncode or rs.returncode:
            # Error streams may contain private input. Emit class/length only in
            # private mode; synthetic failures are safe for local debugging.
            raise AssertionError(f'{label}: Python exit={py.returncode}, Rust exit={rs.returncode}; '
                                 f'stderr bytes={len(py.stderr)}/{len(rs.stderr)}')
        if py.stdout != rs.stdout:
            offset = next((i for i, (a, b) in enumerate(zip(py.stdout, rs.stdout)) if a != b), min(len(py.stdout), len(rs.stdout)))
            raise AssertionError(f'{label}: byte mismatch at {offset}, stdout lengths Python={len(py.stdout)} Rust={len(rs.stdout)}')
    assert sha(snapshot) == before, 'Snapshot bytes changed'
    assert {p for p in sidecars if p.exists()} == original_files, 'Snapshot sidecar created'
    return len(commands)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot', type=Path)
    parser.add_argument('--root', type=Path)
    parser.add_argument('--config', type=Path)
    parser.add_argument('--work', type=Path)
    parser.add_argument('--now', type=float)
    parser.add_argument('--binary', type=Path, default=RUST / 'target/debug/outreach')
    parser.add_argument('--no-build', action='store_true')
    args = parser.parse_args()
    if not args.no_build:
        subprocess.run(['cargo', 'build', '--locked', '-p', 'outreach-cli'], cwd=RUST, check=True)
    if args.snapshot:
        root = args.root or (Path(os.environ['OUTREACH_DATA_ROOT']) if os.environ.get('OUTREACH_DATA_ROOT') else None)
        if root is None: parser.error('--snapshot requires --root or OUTREACH_DATA_ROOT')
        count = compare(args.binary.resolve(), root.resolve(), args.snapshot.resolve(), args.work,
                        args.now if args.now is not None else time.time(), args.config)
        print(f'Private local comparison: {count} command variants x 1 snapshot, all equal; snapshot unchanged; no output retained')
    else:
        from fixtures import NOW, generate
        with tempfile.TemporaryDirectory(prefix='outreach-rust-parity-') as folder:
            fixtures = generate(Path(folder))
            total = 0
            for name, root, snapshot, work in fixtures:
                total += compare(args.binary.resolve(), root, snapshot, work, NOW)
                print(f'{name}: 8 command variants, all equal')
            print(f'Parity: 8 command variants x {len(fixtures)} fixtures = {total} comparisons, all equal; snapshots unchanged')


if __name__ == '__main__':
    main()
