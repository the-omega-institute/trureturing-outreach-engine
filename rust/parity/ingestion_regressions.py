"""Round-two synthetic decoder, command, output and adapter regressions."""
import copy
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import patch
from ingestion import ENGINE, RUST, Tasks, canonical, compare, control, fingerprint


def run(snapshot, root, folder, envelopes, review, key, now):
    binary = str(RUST / 'target/debug/outreach')
    env = {**os.environ, 'OUTREACH_REVIEW_KEY': key}
    checks = 0

    def cli(command, source=None, *args, environment=env):
        nonlocal checks
        checks += 1
        result = subprocess.run([binary, 'control', command, *([str(source)] if source else []),
            '--root', str(root), '--engine-root', str(ENGINE), *map(str, args)], capture_output=True, env=environment)
        assert result.returncode >= 0 and result.returncode != 101 and b'panicked' not in result.stderr, result.stderr
        return result

    findings = json.loads((Path(__file__).with_name('ingestion_findings.json')).read_text())
    for case in findings:
        directory = folder / ('decoder-' + case['id']); directory.mkdir()
        raw = bytes.fromhex(case['hex']) if 'hex' in case else case['text'].encode()
        (directory / 'review-synthetic.json').write_bytes(raw)
        expected, actual = compare(snapshot, directory, root, key, passes=4, now=now)
        assert canonical(expected) == canonical(actual), case['id']
        assert expected['errors'][0][0]['error'] == case['error'], case['id']
    # Syntax decoding precedes the no-key skip, and nonfinite values reach the
    # ordinary family validator. Duplicate-key replacement follows json.loads.
    for raw, secret in [(b'{', ''), (b'{"binding":NaN}', ''),
                        (b'{"binding":Infinity}', key), (b'{"binding":-Infinity}', key),
                        (b'{"binding":1e9999}', key), (b'1' * 4301, key), (b'{"binding":NaN,"binding":null}', key)]:
        directory = folder / ('decoder-extra-' + str(checks)); directory.mkdir()
        (directory / 'review-synthetic.json').write_bytes(raw)
        expected, actual = compare(snapshot, directory, root, secret, passes=4, now=now)
        assert canonical(expected) == canonical(actual), raw
        checks += 1

    for family in ('formalization-backfill', 'prior-work-backfill'):
        value = copy.deepcopy(envelopes[family]); value.pop('signature')
        value['notes'].append({'task_id': 'unknown-synthetic-note', 'result_hash': 'a' * 64})
        value = control.sign_review(value, key)
        directory = folder / ('rollback-' + family); directory.mkdir()
        (directory / (family + '-synthetic.json')).write_text(canonical(value))
        expected, actual = compare(snapshot, directory, root, key, passes=4, now=now)
        assert canonical(expected) == canonical(actual), family
        assert expected['errors'][0][0]['error'] == 'Unknown task'
        checks += 1

    unsigned = folder / 'unsigned-review.json'; unsigned.write_text(canonical(review))
    signed = folder / 'signed-review.json'; signed.write_text(canonical(envelopes['review']))
    destination = folder / 'command-output.json'
    before = fingerprint(snapshot)
    # The production Python dry-run is the oracle for an unsigned review.
    copied = folder / 'dry-run.sqlite3'; shutil.copyfile(snapshot, copied)
    tasks = Tasks(copied, clock=lambda: now)
    try:
        with patch.object(control, 'Ledger', return_value=SimpleNamespace(tasks=tasks)), patch.object(control, 'review_key', return_value=key):
            try:
                control.review_dry_run(review, object())
            except ValueError as exc:
                expected_error = str(exc)
            else:
                raise AssertionError('Python accepted unsigned review')
    finally:
        tasks.close()
    result = cli('review', unsigned, '--snapshot', snapshot, '--output', destination)
    assert result.returncode and expected_error.encode() in result.stderr and not destination.exists()
    for source in (signed, unsigned):
        result = cli('review', source, '--output', destination)
        assert result.returncode and b'requires --snapshot' in result.stderr and not destination.exists()
    result = cli('review', signed, '--snapshot', snapshot, '--output', destination)
    assert result.returncode == 0 and destination.read_bytes() == canonical(envelopes['review']).encode()
    destination.unlink()
    stale = copy.deepcopy(envelopes['review']); stale['binding']['task_revision'] += 1
    stale.pop('signature'); stale = control.sign_review(stale, key)
    stale_path = folder / 'stale-review.json'; stale_path.write_text(canonical(stale))
    result = cli('review', stale_path, '--snapshot', snapshot, '--output', destination)
    assert result.returncode and b'Review would be rejected' in result.stderr and not destination.exists()

    class Capture:
        tag = 'synthetic-local'
        def ensure(self): pass
        def upload(self, path): self.raw = path.read_bytes()
    # Decision stages the input as supplied, even without a key or valid shape.
    no_key = {**env, 'OUTREACH_REVIEW_KEY': '', 'OUTREACH_REVIEW_KEY_FILE': str(folder / 'absent.key'), 'HOME': str(folder)}
    for value in [{k: v for k, v in envelopes['decision'].items() if k != 'signature'},
                  envelopes['decision'], {'synthetic': True}]:
        source = folder / 'decision-input.json'; source.write_text(canonical(value))
        capture = Capture()
        control.stage(value, 'decision', capture, folder)
        result = cli('decision', source, '--output', destination, environment=no_key)
        assert result.returncode == 0 and destination.read_bytes() == capture.raw
        destination.unlink()
        result = cli('decision', source, '--snapshot', snapshot, '--output', destination, environment=no_key)
        assert result.returncode == 0 and destination.read_bytes() == capture.raw
        destination.unlink()

    # Exercise both publish_file and emit, including existing files, output
    # directories, symlink aliases, hard links, lexical aliases and overwrite.
    for command, source, extra in [('decision', folder / 'decision-input.json', []),
                                   ('sign-review', unsigned, [])]:
        destination.write_bytes(b'Synthetic existing output')
        result = cli(command, source, '--output', destination, *extra)
        assert result.returncode and destination.read_bytes() == b'Synthetic existing output'
        result = cli(command, source, '--output', destination, '--overwrite', *extra)
        assert result.returncode == 0 and destination.read_bytes() != b'Synthetic existing output'
        destination.unlink()
    outputs = folder / 'stage-directory'; outputs.mkdir()
    source = folder / 'decision-input.json'
    assert cli('decision', source, '--output', outputs).returncode == 0
    assert cli('decision', source, '--output', outputs).returncode
    assert cli('decision', source, '--output', outputs, '--overwrite').returncode == 0
    protected_decision = folder / 'protected-decision.json'; protected_decision.write_text(canonical(envelopes['decision']))
    aliases = [snapshot, snapshot.parent / 'tables' / '..' / snapshot.name]
    symlink = folder / 'snapshot-symlink'; symlink.symlink_to(snapshot); aliases.append(symlink)
    hardlink = folder / 'snapshot-hardlink'; os.link(snapshot, hardlink); aliases.append(hardlink)
    parent_link = folder / 'snapshot-parent'; parent_link.symlink_to(snapshot.parent, target_is_directory=True)
    aliases.append(parent_link / snapshot.name)
    for target in aliases:
        for overwrite in ([], ['--overwrite']):
            result = cli('decision', protected_decision, '--snapshot', snapshot, '--output', target, *overwrite)
            assert result.returncode and before == fingerprint(snapshot)
            assert b'Output resolves to the input snapshot' in result.stderr or b'File exists' in result.stderr
    # Corrupted persistent JSON must return an error and never panic.
    empty = folder / 'corrupt-inputs'; empty.mkdir()
    (empty / 'review-synthetic.json').write_text(canonical(envelopes['review']))
    for column, value in [('rejections', []), ('rejections', 'synthetic'), ('rejections', None), ('rejections', 1), ('data', []), ('data', 'synthetic'), ('data', 1), ('data', None)]:
        derived = folder / f'corrupt-{checks}.sqlite3'; shutil.copyfile(snapshot, derived)
        tasks = Tasks(derived)
        if column == 'rejections':
            tasks.db.execute('INSERT OR REPLACE INTO pipeline_state VALUES(?,?)', ('ingest_rejected', canonical(value)))
        else:
            tasks.db.execute('UPDATE tasks SET data=? WHERE id=?', (canonical(value), 'review'))
        tasks.db.commit(); tasks.close()
        result = cli('replay-ingest', None, '--snapshot', derived, '--inputs', empty)
        if column == 'rejections':
            assert result.returncode and b'Invalid ingest_rejected' in result.stderr
        else:
            assert result.returncode == 0
            assert 'Invalid task data' in json.loads(result.stdout)['errors'][0][0]['error']
    assert before == fingerprint(snapshot)

    # A fresh private-script process must bind module defaults to --root and
    # pass the key file to Rust even when an ambient key has a different value.
    key_file = folder / 'adapter.key'; key_file.write_text('\x1c' + key + '\x1f\n')
    script = '''import os, runpy, subprocess, sys
from pathlib import Path
from unittest.mock import patch
original = subprocess.run
def observed(command, *args, **kwargs):
    if command[0].endswith('/outreach'):
        assert '--key-file' in command
        assert kwargs['env']['OUTREACH_REVIEW_KEY'] == 'synthetic conflicting ambient key'
        from managed.local_run import workspace
        assert Path(workspace.__defaults__[0]).resolve() == Path(sys.argv[sys.argv.index('--root') + 1]).resolve()
    return original(command, *args, **kwargs)
with patch.object(subprocess, 'run', side_effect=observed):
    runpy.run_path(sys.argv.pop(1), run_name='__main__')
'''
    result = subprocess.run([sys.executable, '-c', script, str(RUST / 'replay_private_inputs.py'), str(snapshot), str(empty),
        '--root', str(root), '--generation', '7', '--now', str(now), '--key-file', str(key_file)],
        capture_output=True, env={**env, 'OUTREACH_REVIEW_KEY': 'synthetic conflicting ambient key', 'OUTREACH_DATA_ROOT': str(folder / 'wrong-root')})
    assert result.returncode == 0 and not result.stderr, result.stderr
    assert json.loads(result.stdout)['mismatches'] == 0
    checks += 1
    print(f'Round-two regressions: {len(findings)} decoder fixtures / 4 passes each; {checks} additional checks; originals unchanged')
