#!/usr/bin/env python3
"""Offline adapter to the live Python CLI; opens the supplied snapshot read-only.

Bypass only Release download/restore (and its migrations), not command dispatch,
view builders, or output serialization. Never instantiate Tasks(path), which is
an initializing/writing API. This file is also the oracle for private comparisons.
"""
import argparse
from contextlib import ExitStack
from pathlib import Path
import sqlite3
import sys
from types import SimpleNamespace
from unittest.mock import patch

ENGINE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ENGINE))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot', type=Path)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--engine-root', type=Path)
    parser.add_argument('--now', type=float, required=True)
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    # Set before importing location/policy; no private data fallback discovery.
    import os
    os.environ['OUTREACH_DATA_ROOT'] = str(args.root.resolve())
    os.environ['GITHUB_ACTIONS'] = 'false'
    from managed import control
    from managed.tasks import Tasks
    config = control.load_config(args.root)

    def ledger(*unused):
        tasks = Tasks.__new__(Tasks)
        tasks.path = str(args.snapshot)
        tasks.clock = lambda: args.now
        tasks.db = sqlite3.connect(args.snapshot.resolve().as_uri() + '?mode=ro&immutable=1', uri=True)
        tasks.db.row_factory = sqlite3.Row
        return SimpleNamespace(tasks=tasks)

    def workflow_text(path, *unused):
        engine = args.engine_root or ENGINE
        overlay = args.root / 'policy/managed' / path
        source = overlay if not path.endswith('.py') and overlay.is_file() else engine / 'managed' / path
        return source.read_bytes().decode('utf-8')

    with ExitStack() as stack:
        if args.engine_root:
            stack.enter_context(patch.object(control, 'workflow_text', side_effect=workflow_text))
        stack.enter_context(patch.object(control, 'Releases', return_value=object()))
        stack.enter_context(patch.object(control, 'Ledger', side_effect=ledger))
        stack.enter_context(patch.object(control, 'load_config', return_value=config))
        control._main(args.command)


if __name__ == '__main__':
    main()
