#!/usr/bin/env python3
"""Run the Rust stages on the same committed Python-first scenarios."""
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def main():
    subprocess.run(['cargo', 'build', '--locked', '--offline', '-p', 'outreach-core', '--example', 'scenarios'], cwd=ROOT / 'rust', check=True)
    cases = [json.loads(path.read_text()) for path in sorted((ROOT / 'scenarios/cases').glob('*.json'))]
    assert len(cases) >= 40
    result = subprocess.run([str(ROOT / 'rust/target/debug/examples/scenarios'), '--jsonl'], input=''.join(json.dumps(c, ensure_ascii=False) + '\n' for c in cases), text=True, capture_output=True)
    if result.returncode:
        sys.stderr.write(result.stderr)
    sys.stdout.write(result.stdout)
    assert result.returncode == 0
    counts = json.loads(result.stdout)
    assert counts['equal'] == sum(c['stage'] in {'mathesis','publication','sent','pool','delivery','intake','inbound','formalization','prior-work'} for c in cases) and counts['different'] == 0


if __name__ == '__main__': main()
