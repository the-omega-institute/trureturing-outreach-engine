#!/usr/bin/env python3
"""Replay private recording frames locally. stdout/stderr contain counts only.

The supplied closed snapshot must match the recorded start Release pointer, or
(for a fixture without a Release pointer) the first stage frame. Earlier
unported stages are observations at the stage boundary, not Rust executions.
No output file, network client, production writer or private upload is used.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
from types import SimpleNamespace
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from managed.scenario_contract import dump, logical
from managed.queue import canonical


def replay(directory, snapshot, binary, key=None):
    counts = {'equal': 0, 'different': 0, 'skipped': 0}
    try:
        snapshot = snapshot.resolve(strict=True)
        if any(Path(str(snapshot) + suffix).exists() for suffix in ('-wal', '-journal')):
            raise ValueError('open snapshot')
        before = hashlib.sha256(snapshot.read_bytes()).hexdigest()
        connection = sqlite3.connect('file:' + quote(str(snapshot)) + '?mode=ro&immutable=1', uri=True)
        try: initial = dump(SimpleNamespace(db=connection))
        finally: connection.close()
        logical_initial = logical(initial)
        events = []
        for path in sorted(directory.glob('calls-*.jsonl')):
            for line in path.read_text().splitlines(): events.append(json.loads(line))
        starts = [e['pointer'] for e in events if e.get('event') == 'release_pointer' and e.get('boundary') == 'start' and (e.get('pointer') or {}).get('snapshot')]
        stages = {'mathesis', 'publication', 'sent'}
        cases = [e['case'] for e in events if e.get('event') in {s + '_stage' for s in stages}]
        counts['skipped'] += sum(e.get('event') in {s + '_stage_unavailable' for s in stages} for e in events)
        if starts:
            if not any(p['snapshot']['sha256'] == before for p in starts): raise ValueError('start snapshot mismatch')
        elif cases:
            if 'initial_rows' in cases[0]:
                if canonical(logical_initial) != canonical(logical(cases[0]['initial_rows'])): raise ValueError('fixture start mismatch')
            elif {k: hashlib.sha256(canonical(v).encode()).hexdigest() for k,v in logical_initial.items()} != cases[0].get('base_digests', cases[0]['initial_digests']):
                raise ValueError('fixture start mismatch')
        eligible = []
        for case in cases:
            if (not key and case['stage'] != 'sent') or case.get('redacted') or case.get('incomplete'):
                counts['skipped'] += 1; continue
            # Ordering and window checks are independent of the Rust replay.
            times = [t for pair in case.get('update_times', []) for t in pair]
            if any(t < case['now'] or t > case['now_end'] for t in times) or times != sorted(times):
                counts['different'] += 1; continue
            case['review_key'] = key or ''
            if not eligible and 'initial_rows' not in case: case['initial_rows'] = initial
            eligible.append(case)
        if eligible:
            result = subprocess.run([str(binary)], input=json.dumps(eligible), text=True, capture_output=True)
            value = json.loads(result.stdout)
            if set(value) != set(counts) or any(type(v) is not int or v < 0 for v in value.values()): raise ValueError('invalid replay counts')
            for k in counts: counts[k] += value[k]
            if result.returncode and not value['different']: counts['different'] += 1
        if not cases and not counts['skipped']: counts['skipped'] = 1
        if hashlib.sha256(snapshot.read_bytes()).hexdigest() != before: raise ValueError('snapshot changed')
    except Exception:
        counts['different'] += 1
    return counts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('recordings', type=Path)
    parser.add_argument('snapshot', type=Path)
    parser.add_argument('--binary', type=Path, default=ROOT / 'rust/target/debug/examples/scenarios')
    parser.add_argument('--key-file', type=Path)
    args = parser.parse_args()
    key = os.environ.get('OUTREACH_REVIEW_KEY')
    if args.key_file:
        try: key = args.key_file.read_text().strip()
        except OSError: print(canonical({'equal': 0, 'different': 1, 'skipped': 0})); return 1
    result = replay(args.recordings, args.snapshot, args.binary, key)
    print(canonical(result))
    return bool(result['different'])


if __name__ == '__main__': raise SystemExit(main())
