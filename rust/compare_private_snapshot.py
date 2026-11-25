#!/usr/bin/env python3
"""Local only: compare a private closed SQLite snapshot without retaining output.

Usage: python3 rust/compare_private_snapshot.py /path/snapshot.sqlite3 --root /path/to/data
Optional --work /path/to/work.json includes review-digest and work exceptions.
"""
from pathlib import Path
import subprocess
import sys

if len(sys.argv) < 2 or sys.argv[1].startswith('-'):
    raise SystemExit(__doc__)
raise SystemExit(subprocess.call([sys.executable, str(Path(__file__).with_name('parity') / 'compare.py'),
                                 '--snapshot', sys.argv[1], *sys.argv[2:]]))
