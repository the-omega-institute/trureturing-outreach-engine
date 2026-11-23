#!/usr/bin/env python3
"""Compare real control.workflow hashes of the current public managed/ tree.

The only substituted boundary is the text reader, pinned to this engine checkout
so no private policy overlay, config, git revision or data repository is accessed.
The real function selects all inputs and computes every expected digest anew.
No source snapshots or receipts are persisted in the committed vectors.
"""
import json
from pathlib import Path
import subprocess
from unittest.mock import patch

from vectors.generate import KINDS, ROOT, control


def live_vectors():
    vectors = []
    for kind in [*KINDS, 'unknown']:
        files = {}

        def read(path, *_):
            text = (ROOT / 'managed' / path).read_bytes().decode('utf-8')
            files[path] = text
            return text

        with patch.object(control, 'workflow_text', side_effect=read):
            expected = control.workflow(kind)
        vectors.append({'kind': kind, 'files': files, 'digest': expected})
    return vectors


def main():
    vectors = live_vectors()
    subprocess.run(['cargo', 'run', '--locked', '--quiet', '-p', 'outreach-canon',
                    '--example', 'check_workflows'],
                   cwd=Path(__file__).resolve().parent,
                   input=json.dumps(vectors, ensure_ascii=True), text=True, check=True)


if __name__ == '__main__':
    main()
