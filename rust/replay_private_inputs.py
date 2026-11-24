#!/usr/bin/env python3
"""Operator-run snapshot-window replay. Prints counts only, never private values."""
import argparse
import json
import os
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent / 'parity'))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('snapshot', type=Path)
    parser.add_argument('inputs', type=Path)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--generation', type=int, required=True)
    parser.add_argument('--now', type=float, required=True)
    parser.add_argument('--passes', type=int, default=1)
    parser.add_argument('--key-file', type=Path)
    args = parser.parse_args()
    # Bind production modules' default root arguments to the supplied root.
    os.environ['OUTREACH_DATA_ROOT'] = str(args.root.resolve())
    from ingestion import assets, build, compare, canonical
    try:
        secret = args.key_file.read_text().strip() if args.key_file else os.environ.get('OUTREACH_REVIEW_KEY', '')
        if not secret:
            raise ValueError('Missing local key')
        build()
        expected, actual = compare(args.snapshot, args.inputs, args.root, secret,
                                   passes=args.passes, generation=args.generation, now=args.now, key_file=args.key_file)
        counts = {'assets': len(assets(args.inputs)), 'passes': args.passes,
                  'tables': len(expected['tables']),
                  'rows': sum(len(t['rows']) for t in expected['tables'].values()),
                  'rejections': sum(len(e) for e in expected['errors']),
                  'mismatches': int(canonical(expected) != canonical(actual))}
        print(json.dumps(counts, sort_keys=True))
        return int(canonical(expected) != canonical(actual))
    except Exception:
        print(json.dumps({'errors': 1}, sort_keys=True))
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
