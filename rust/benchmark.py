#!/usr/bin/env python3
"""Local synthetic release benchmark; never run in CI or on a private ledger.

Uses live Python Tasks for 2,000 tasks with ~49 KB of nested request/result/history
JSON each (~98 MB closed SQLite ledger). Measures each child independently with
wait4, compares stdout hashes, and enforces <=2x Python wall time and peak RSS.
Only summary measurements are printed; fixtures/output disappear on completion.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import sys
import tempfile
import time

RUST = Path(__file__).resolve().parent
sys.path.insert(0, str(RUST / 'parity'))
from fixtures import NOW, base, request, Tasks


def ledger(root, count, payload_bytes):
    base(root)
    path = root / 'synthetic.sqlite3'
    tasks = Tasks(path, clock=lambda: NOW)
    tasks.db.execute('PRAGMA synchronous=OFF')
    # Numerous objects exercise allocation/ownership as well as string scanning.
    thread = [{'id': f'message-{i}', 'from': {'emailAddress': {'address': 'scholar@example.invalid'}},
               'body': {'contentType': 'text', 'content': 'synthetic body ' * 25}} for i in range(64)]
    history = [{'operation': f'synthetic-{i}', 'status': 'finished', 'receipt': {'id': i}} for i in range(64)]
    fixed_bytes = len(json.dumps(thread, separators=(',', ':'))) + len(json.dumps(history, separators=(',', ':')))
    padding = max(0, payload_bytes - fixed_bytes - 1600)
    try:
        for i in range(count):
            req = request(f'benchmark-{i:05}', kind='research')
            req['inputs'] = {'thread': thread, 'instructions': 'x' * (padding // 2),
                             'automation_parent': {'task_id': f'benchmark-{i-1:05}'} if i else {}}
            row = tasks.add(req)
            tasks.update(row['id'], row['revision'], 'awaiting-review',
                         {'result': {'outcome': 'completed', 'report': 'r' * (padding - padding // 2), 'artifacts': []},
                          'execution_history': history})
    finally:
        tasks.close()
    return path


def measure(command, folder, tag):
    output = folder / (tag + '.stdout')
    error = folder / (tag + '.stderr')
    start = time.perf_counter()
    with output.open('wb') as out, error.open('wb') as err:
        child = subprocess.Popen(command, stdout=out, stderr=err, cwd=RUST.parent)
        _, status, usage = os.wait4(child.pid, 0)
        child.returncode = os.waitstatus_to_exitcode(status)
    elapsed = time.perf_counter() - start
    if child.returncode:
        raise RuntimeError(f'{tag}: exit {child.returncode}: {error.read_text()[:200]}')
    rss = usage.ru_maxrss / (1024 * 1024 if sys.platform == 'darwin' else 1024)
    with output.open('rb') as stream: digest = hashlib.file_digest(stream, 'sha256').digest()
    output.unlink(); error.unlink()
    return elapsed, rss, digest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tasks', type=int, default=2000)
    parser.add_argument('--payload-bytes', type=int, default=49000)
    parser.add_argument('--runs', type=int, default=3)
    parser.add_argument('--no-build', action='store_true')
    args = parser.parse_args()
    if min(args.tasks, args.payload_bytes, args.runs) <= 0: parser.error('Sizes and runs must be positive')
    if not args.no_build:
        subprocess.run(['cargo', 'build', '--release', '--locked', '-p', 'outreach-cli'], cwd=RUST, check=True)
    import sqlite3
    print(f'{platform.system()} {platform.machine()}; Python {platform.python_version()}; Python SQLite {sqlite3.sqlite_version}', flush=True)
    with tempfile.TemporaryDirectory(prefix='outreach-synthetic-benchmark-') as folder:
        root = Path(folder)
        snapshot = ledger(root, args.tasks, args.payload_bytes)
        print(f'{args.tasks:,} tasks; closed ledger {snapshot.stat().st_size:,} bytes ({snapshot.stat().st_size / 1e6:.1f} MB); {args.runs} runs', flush=True)
        for command in ['exceptions', 'reviews']:
            measurements = {'Python': [], 'Rust': []}
            for run in range(args.runs):
                # Alternate order to reduce warm-cache bias.
                for language in (['Python', 'Rust'] if run % 2 == 0 else ['Rust', 'Python']):
                    prefix = [sys.executable, str(RUST / 'parity/python_control.py')] if language == 'Python' else [str(RUST / 'target/release/outreach'), 'control']
                    common = ['--root', str(root), '--snapshot', str(snapshot), '--now', str(NOW)]
                    measurements[language].append(measure([*prefix, *common, command], root, language))
            assert len({row[2] for rows in measurements.values() for row in rows}) == 1, command + ': stdout mismatch'
            stats = {lang: (statistics.median(r[0] for r in rows), max(r[1] for r in rows)) for lang, rows in measurements.items()}
            py, rs = stats['Python'], stats['Rust']
            print(f'{command}: Python {py[0]:.3f} s / {py[1]:.1f} MiB; Rust {rs[0]:.3f} s / {rs[1]:.1f} MiB; time {rs[0]/py[0]:.2f}x, RSS {rs[1]/py[1]:.2f}x; stdout equal', flush=True)
            assert rs[0] <= 2 * py[0] and rs[1] <= 2 * py[1], command + ': exceeds 2x target'


if __name__ == '__main__': main()
