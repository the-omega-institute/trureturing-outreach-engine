#!/usr/bin/env python3
"""Synthetic replay measurement; no files/data are read and no network is used.

2,000 tasks (~64 MB payloads), 100,000 historical events, 100 signed recovery
assets. Compare row journaling against emulating the removed five-table copies.
Each mode runs in a fresh child; final table digests must match. Not a CI gate.
"""
import json
from pathlib import Path
import statistics
import subprocess
import tempfile
import time
import os
import sys

RUST = Path(__file__).resolve().parent


def measure(mode):
    with tempfile.TemporaryFile() as output, tempfile.TemporaryFile() as error:
        start = time.perf_counter()
        child = subprocess.Popen([str(RUST / 'target/release/examples/benchmark_ingest'), mode], stdout=output, stderr=error)
        _, status, usage = os.wait4(child.pid, 0)
        child.returncode = os.waitstatus_to_exitcode(status)
        elapsed = time.perf_counter() - start
        error.seek(0)
        assert child.returncode == 0, error.read().decode()
        output.seek(0)
        digest = output.read().strip()
        rss = usage.ru_maxrss / (1024 * 1024 if sys.platform == 'darwin' else 1024)
        return elapsed, rss, digest


def main():
    subprocess.run(['cargo', 'build', '--offline', '--locked', '--release', '-q', '-p', 'outreach-core', '--example', 'benchmark_ingest'], cwd=RUST, check=True)
    results = {mode: [measure(mode) for _ in range(3)] for mode in ('journal', 'legacy-copy')}
    assert len({r[2] for runs in results.values() for r in runs}) == 1
    print(json.dumps({mode: {'median_seconds': round(statistics.median(r[0] for r in runs), 3),
                            'median_peak_mib': round(statistics.median(r[1] for r in runs), 1)}
                      for mode, runs in results.items()}, sort_keys=True))


if __name__ == '__main__':
    main()
