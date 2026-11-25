#!/usr/bin/env python3
"""Offline stage builders compared with live Python, including preserved files."""
from contextlib import ExitStack, redirect_stdout
import hashlib
import io
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
from types import SimpleNamespace
from unittest.mock import patch
from ingestion import ENGINE, RUST, build, compare, control, location, Tasks, canonical, digest, fingerprint
from signing_ingest import KEY, fixture, sign


class Capture:
    tag = 'synthetic-builder'
    repo = 'synthetic/offline'

    def __init__(self):
        self.assets = {}
        self.resources = {}

    def ensure(self):
        pass

    def upload(self, path):
        self.assets[Path(path).name] = Path(path).read_bytes()

    def preserve_task(self, path, tag):
        self.resources[tag, Path(path).name] = Path(path).read_bytes()

    def run(self, *args):
        assert args[:2] == ('release', 'download')
        name = args[args.index('--pattern') + 1]
        (Path(args[args.index('--dir') + 1]) / name).write_bytes(self.resources[args[2], name])


def git(root, *args):
    return subprocess.check_output(['git', *args], cwd=root, stderr=subprocess.DEVNULL, text=True).strip()


def main():
    build()
    count = failures = 0
    with tempfile.TemporaryDirectory(prefix='stage-builders-parity-') as temp:
        folder = Path(temp)
        root = folder / 'data'
        snapshot, envelopes, review, close, edit, rows = fixture(root)
        contacts = json.loads((root / 'tables/contacts.json').read_text())
        contacts['contacts'][0]['hub'] = {'workspace_paths': ['../mail/scholar-a']}
        (root / 'tables/contacts.json').write_text(canonical(contacts))
        thread = {'id': 'thread-synthetic', 'stage': 'active', 'correspondent': {'email': 'a@example.invalid'},
                  'ball': 'operator', 'last_in': {'internetMessageId': 'synthetic@example.invalid', 'ROWID': 4},
                  'last_out': {'message_id': '<outgoing@example.invalid>'}}
        (root / 'tables/threads.json').write_text(canonical({'lines': [thread]}))
        workspace = folder / 'mail/scholar-a/paper'
        workspace.mkdir(parents=True)
        (workspace / 'paper.tex').write_text('Synthetic TeX input π')
        (workspace / 'evidence.txt').write_text('Synthetic evidence')
        paper = {'id': 'paper-synthetic', 'title': 'Synthetic result', 'authors': ['Synthetic Author'],
                 'version_date': '2026-11-24', 'target_journal': 'Synthetic Journal',
                 'workspace': '../mail/scholar-a/paper', 'stage': 'needs_work'}
        (root / 'tables/submissions.json').write_text(canonical({'papers': [paper]}))
        git(root, 'init', '-q')
        git(root, 'add', 'config.json', 'tables')
        git(root, '-c', 'user.name=Synthetic Parity', '-c', 'user.email=synthetic@example.invalid', 'commit', '-qm', 'Synthetic data pins')
        revision = git(ENGINE, 'rev-parse', 'HEAD')
        source = folder / 'envelope.json'
        packet = folder / 'packet.json'; packet.write_text(canonical(envelopes['draft-revision']['packet']))
        commands = [
            ['stage-inbound', '--message-id', 'synthetic@example.invalid'],
            ['stage-inbound', '--message-id', '<synthetic@example.invalid>', '--owner', 'scholar-a', '--instructions', 'Synthetic instructions π'],
            ['stage-thread', '--thread', 'thread-synthetic'],
            ['stage-thread', '--thread', 'thread-synthetic', '--kind', 'research', '--instructions', 'Synthetic research'],
            ['stage-submission', '--paper', 'paper-synthetic', '--file', 'paper.tex', '--file', 'evidence.txt'],
            ['stage-submission', '--paper', 'paper-synthetic', '--round', 'finalize', '--file', 'paper.tex'],
            ['stage-submission', '--paper', 'paper-synthetic', '--file', str(workspace / 'paper.tex')],
            ['stage-draft-revision', '--task', 'draft', '--packet', str(packet), '--reason', 'Synthetic correction'],
            ['request-submission-review', '--paper', 'paper-synthetic'],
        ]
        tasks = Tasks(snapshot)
        try:
            for collection in [False, True]:
                identity = 'mathesis-stage-' + str(int(collection))
                directory = folder / identity;directory.mkdir()
                (directory / 'source.zip').write_bytes(b'PK\x03\x04Synthetic source')
                (directory / 'note.pdf').write_bytes(b'%PDF-Synthetic PDF')
                note = {'id': 'synthetic-note', 'revision': 1, 'title': 'Synthetic note', 'abstract': 'Synthetic abstract',
                        'creators': [{'name': 'Synthetic Author', 'affiliation': 'Synthetic Test Affiliation'}],
                        'license': 'CC-BY-4.0', 'problem_ids': ['p1'], 'evidence_url': 'https://example.invalid/proof'}
                manifest = {'schema_version': 'outreach-mathesis-handoff.v1', 'kind': 'short_note' if collection else 'manuscript',
                            'private_correspondence_included': False, 'source_note': note,
                            'files': {'source.zip': hashlib.sha256((directory / 'source.zip').read_bytes()).hexdigest()}}
                (directory / 'handoff.json').write_text(canonical(manifest))
                artifacts = [{'release_tag': 'outreach-task-' + identity, 'name': name,
                              'bytes': (directory / name).stat().st_size, 'sha256': hashlib.sha256((directory / name).read_bytes()).hexdigest(), 'role': role}
                             for name, role in [('source.zip', 'source-archive'), ('note.pdf', 'note-pdf'), ('handoff.json', 'handoff')]]
                req = {**rows['mathesis']['request'], 'id': identity}
                row = tasks.add(req)
                row = tasks.update(identity, row['revision'], 'awaiting-review', {'result': {'schema_version': 1, 'task_id': identity,
                    'outcome': 'completed', 'report': 'Synthetic checked manuscript', 'artifacts': artifacts, 'packet': None}})
                tasks.verdict(sign({**review, 'binding': tasks.review_binding(row)}), KEY)
                decisions = {'source_file': 'source.zip', 'purpose': 'publication' if collection else 'review_only',
                             'processing_consent': True, 'ai_disclosure': 'Synthetic disclosure', 'decision_ref': 'synthetic:authorization',
                             'make_public_after_acceptance': collection}
                decision_path = folder / (identity + '-decisions.json');decision_path.write_text(canonical(decisions))
                commands.append(['stage-mathesis', '--task', identity, '--directory', str(directory), '--decisions', str(decision_path)])
        finally:
            tasks.close()
        for family, value in [('review', envelopes['review']), ('decision', envelopes['decision']), ('decision', sign(close))]:
            name = folder / f'{family}-{len(commands)}.json';name.write_text(canonical(value));commands.append([family, str(name)])
        request = {'schema_version': 1, 'id': 'enqueue-synthetic', 'kind': 'research', 'owner': 'scholar-a', 'inputs': {}}
        with patch.object(control, 'ROOT', root):
            request.update(control.request_binding('research', revision, git(root, 'rev-parse', 'HEAD')))
        source.write_text(canonical(request));commands.append(['enqueue', str(source)])

        copies = []
        def ledger(*unused):
            path = folder / f'ledger-copy-{len(copies)}.sqlite3'
            copies.append(path)
            source = sqlite3.connect(snapshot.resolve().as_uri() + '?mode=ro&immutable=1', uri=True)
            destination = sqlite3.connect(path)
            try:
                source.backup(destination)
            finally:
                source.close(); destination.close()
            return SimpleNamespace(tasks=Tasks(path))

        before = fingerprint(snapshot)
        for n, args in enumerate(commands):
            capture = Capture()
            output = folder / f'output-{n}';output.mkdir()
            with ExitStack() as stack:
                stack.enter_context(patch.object(control, 'ROOT', root))
                stack.enter_context(patch.object(control, 'Releases', return_value=capture))
                stack.enter_context(patch.object(control, 'Ledger', side_effect=ledger))
                stack.enter_context(patch.dict(os.environ, {'OUTREACH_REVIEW_KEY': KEY}))
                stack.enter_context(patch.object(location, 'load_config', return_value=json.loads((root / 'config.json').read_text())))
                stack.enter_context(redirect_stdout(io.StringIO()))
                control._main(args)
            rust_args = [str(RUST / 'target/debug/outreach'), 'control', *args, '--root', str(root), '--output', str(output)]
            if args[0] in {'stage-draft-revision', 'request-submission-review', 'review', 'stage-mathesis'}:
                rust_args += ['--snapshot', str(snapshot)]
            process = subprocess.run(rust_args, capture_output=True, env={**os.environ, 'OUTREACH_REVIEW_KEY': KEY})
            assert process.returncode == 0, (args[0], process.stderr)
            actual = {p.name: p.read_bytes() for p in output.glob('*.json')}
            assert actual == capture.assets, (args[0], 'signed asset bytes differ')
            actual_resources = {(p.parent.name, p.name): p.read_bytes() for p in (output / 'resources').glob('*/*')}
            assert actual_resources == capture.resources, (args[0], 'resource bytes differ')
            expected_dump, actual_dump = compare(snapshot, output, root, KEY, passes=2)
            assert canonical(expected_dump) == canonical(actual_dump), (args[0], 'builder ingestion differs')
            assert not any(expected_dump['errors']), (args[0], 'Python rejected a Rust-built asset')
            count += 1
        assert before == fingerprint(snapshot)
        for args in [['stage-inbound', '--message-id', 'not-an-internet-id'], ['stage-inbound', '--message-id', 'a@b.invalid', '--owner', 'unknown'],
                     ['stage-thread', '--thread', 'unknown'], ['stage-draft-revision', '--task', 'review', '--packet', str(packet), '--reason', 'Synthetic']]:
            with ExitStack() as stack:
                stack.enter_context(patch.object(control, 'ROOT', root))
                stack.enter_context(patch.object(control, 'Releases', return_value=Capture()))
                stack.enter_context(patch.object(control, 'Ledger', side_effect=ledger))
                stack.enter_context(patch.dict(os.environ, {'OUTREACH_REVIEW_KEY': KEY}))
                stack.enter_context(patch.object(location, 'load_config', return_value=json.loads((root / 'config.json').read_text())))
                stack.enter_context(redirect_stdout(io.StringIO()))
                try:
                    control._main(args)
                except (ValueError, KeyError, TypeError) as exc:
                    error = str(exc)
                else:
                    raise AssertionError('Python accepted a negative builder case')
            process = subprocess.run([str(RUST / 'target/debug/outreach'), 'control', *args, '--snapshot', str(snapshot), '--root', str(root), '--output', str(folder / 'invalid.json')], capture_output=True, env={**os.environ, 'OUTREACH_REVIEW_KEY': KEY})
            assert process.returncode not in (0, 101) and process.returncode > 0 and b'panicked' not in process.stderr
            assert process.stderr.decode().strip() == error
            failures += 1
    print(f'Offline builders: {count} byte-identical stage outputs/resources accepted by Python; {failures} matching refusals without panic; originals unchanged')


if __name__ == '__main__':
    main()
