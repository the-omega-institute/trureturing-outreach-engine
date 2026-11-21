"""Local Codex execution; only the coordinator admits signed results to the ledger."""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import copy
import fcntl
import hashlib
import hmac
import json
import os
from pathlib import Path
import subprocess
import tempfile

from . import control
from .thread_staging import effective_inputs
from .queue import canonical, digest, require
from .tasks import KINDS, matches_workflow, unstarted


LOCAL_RUNTIME = """LOCAL RUNTIME:
You run locally, with the same task/result contract as CMA below.
"""
LOCAL_OUTPUT = """Never search for credentials or read the review key.
Write selected outputs below deliverables/ and the exact final result JSON required
by the contract below to result.json. Do not sign or stage control/review assets,
send or publicly publish; the host preserves selected artifacts after you finish.

"""
PREAMBLE = LOCAL_RUNTIME + """Read historical pins through the named repository's Git
trees and Contents APIs, using the existing gh session. This replaces CMA's NyxID
GitHub connection instructions.
""" + LOCAL_OUTPUT


# Local runs need network for gh/Release reads (selected attachments, pinned
# sources); without it they hold for "unread inputs". The result must match the
# CMA result contract exactly: extra or missing top-level keys fail staging.
RUN_INSTRUCTION=('Read prompt.txt in full and execute that task. Save the final result to result.json. '
                 'Before finishing, re-read the result contract in prompt.txt and check result.json '
                 'against it: exactly the required and permitted top-level keys, no additional keys '
                 '(for example no user_decision_ever at the top level), and every nested object in '
                 'the exact documented shape (registry_patch keeps all its keys, using null for absent '
                 'new_contact/alias_of). Fix any mismatch before you stop.')

def executable(row):
    return row['state'] in {'queued', 'credit-wait'} and unstarted(row)


def failed_local(row):
    recovery = row.get('data', {}).get('recovery') or {}
    return row['state'] == 'failed' and recovery.get('suggested_exit') == 'local-codex'


def failed_execution_identity(row):
    """The original CMA lifecycle handles bound into a local takeover."""
    return {key: row['data'][key] for key in
            ('agent_id', 'operation_id', 'response_id', 'create_key', 'command_key')
            if key in row['data']}


def target(rows, task_id, takeover=False):
    by_id = {r['id']: r for r in rows}
    require(task_id in by_id, 'Unknown task')
    row = by_id[task_id]
    if takeover:
        require(failed_local(row), 'Task is not failed with suggested exit local-codex')
        require(not row['data'].get('agent_id') or control.agent_retired(row['data']),
                'Local takeover refused while the original CMA Agent may still execute')
        return row
    seen = set()
    while row['state'] == 'revising':
        require(row['id'] not in seen, 'Continuation cycle')
        seen.add(row['id'])
        child = row['data'].get('continuation_id')
        require(child in by_id, 'Revision has no queued continuation')
        row = by_id[child]
    require(executable(row), 'Task is not unstarted; CMA may already have admitted it')
    return row


def selection(rows, kinds=None, created=None):
    created = created or {}
    return sorted((r for r in rows if executable(r) and (kinds is None or r['kind'] in kinds)),
                  key=lambda r: (r['kind'] not in {'reply', 'firstcontact'},
                                 created.get(r['id'], r['updated']), r['id']))


def failed_local_selection(rows, kinds=None):
    return sorted((r for r in rows if failed_local(r) and
                   (kinds is None or r['kind'] in kinds)),
                  key=lambda r: (r['updated'], r['id']))


def workspace(row, root=control.ROOT):
    root = Path(root)
    index = json.loads((root / 'tables/workspaces.json').read_text())
    mail = (root / index['workspace_root']).resolve()
    for entry in index['workspaces']:
        if row['owner'] == entry['id'] or row['owner'] in entry.get('contact_ids', []):
            return (root / entry['path']).resolve()
    contacts = json.loads((root / 'tables/contacts.json').read_text())['contacts']
    for contact in contacts:
        if contact['id'] == row['owner']:
            for path in contact.get('hub', {}).get('workspace_paths', []):
                candidate = (root / path).resolve()
                if candidate.is_relative_to(mail):
                    return candidate
    return mail / 'outreach-maintenance'


def gates(row, tasks, result=None):
    """The same source, parent, ownership and thread gates as CMA results."""
    from .automation import parent_gate, paper_gate, owner_gate
    from .pipeline import source_gate
    from . import note_closure
    from .thread_staging import gate
    require(matches_workflow(row, control.workflow(row['kind'])), 'Workflow changed; wait for coordinator re-pin')
    source_gate(row['request'], tasks, control.ROOT)
    note_closure.source_gate(row['request'], tasks, result)
    parent_gate(row['request'], tasks)
    paper_gate(row['request'], control.ROOT)
    owner_gate(row['request'], control.ROOT)
    control.firstcontact_gate(row['request'], control.ROOT)
    gate(row['request'], control.ROOT, row['data'].get('thread_inputs'))


def prepare(row, ledger, config, mailbox=None):
    """Hydrate missing private context in memory, without writing the restored DB."""
    from . import firstcontact_history
    from .thread_staging import resolve
    row = copy.deepcopy(row)
    inputs = row['request']['inputs']
    data = row['data']
    thread = (row['kind'] in {'reply', 'research'} and inputs.get('thread_ref')
              and not inputs.get('thread') and not data.get('thread_inputs'))
    history = row['kind'] in {'reply', 'firstcontact'} and not data.get('mailbox_history')
    if thread or history:
        if mailbox is None:
            from .affairs import _live_mailbox
            mailbox = _live_mailbox(control.ROOT, config)
        if thread:
            data['thread_inputs'] = resolve(row, mailbox, ledger, control.ROOT)
        if history:
            mailbox.begin_history_tick()
            data['mailbox_history'] = firstcontact_history.snapshot(ledger.tasks.clock)
            resolver = firstcontact_history.resolve_reply if row['kind'] == 'reply' else firstcontact_history.resolve
            resolver(row, mailbox, ledger, control.ROOT, data['mailbox_history'])
    gates(row, ledger.tasks)
    return row


def prompt(row, config, capacity):
    preamble = LOCAL_RUNTIME + LOCAL_OUTPUT if row['request'].get('policy_revision') else PREAMBLE
    return preamble + control.task_prompt(row, config.get('github'), capacity)


class LocalArtifacts:
    def __init__(self, directory):
        self.directory = Path(directory).resolve()

    def artifact(self, agent_id, name, size, sha):
        path = (self.directory / name).resolve()
        require(path.is_relative_to(self.directory / 'deliverables'), 'Local artifact escapes deliverables/')
        require(path.stat().st_size == size, 'Local artifact size mismatch')
        raw = path.read_bytes()
        require(hashlib.sha256(raw).hexdigest() == sha, 'Local artifact checksum mismatch')
        return raw


def preserve(result, row, releases, directory):
    result = copy.deepcopy(result)
    control.validate_result(result, row)
    control.verify_artifacts(result, releases, directory, LocalArtifacts(directory), 'local-codex')
    control.validate_result(result, row, allow_packet_artifact=False)
    return result


def ingest(ledger, envelope, secret):
    """Called exclusively by Ledger.ingest in the serialized Actions tick."""
    fields = {'schema_version', 'task_id', 'request_hash', 'task_revision', 'executor',
              'session_id', 'result', 'signature', 'context'}
    takeover_fields = {'takeover', 'failed_execution'}
    require(isinstance(envelope, dict) and
            (set(envelope) == fields or set(envelope) == fields | takeover_fields),
            'Invalid local result envelope')
    takeover = envelope.get('takeover', False)
    if takeover:
        require(envelope.get('takeover') is True and isinstance(envelope.get('failed_execution'), dict),
                'Invalid local takeover binding')
    else:
        require('takeover' not in envelope and 'failed_execution' not in envelope,
                'Invalid local takeover binding')
    require(bool(secret), 'Local result verification key is not configured')
    signed = {k: v for k, v in envelope.items() if k != 'signature'}
    expected = hmac.new(secret.encode(), canonical(signed).encode(), hashlib.sha256).hexdigest()
    require(isinstance(envelope['signature'], str) and hmac.compare_digest(expected, envelope['signature']),
            'Local result signature failed')
    require(envelope['schema_version'] == 1 and envelope['executor'] == 'local-codex'
            and isinstance(envelope['session_id'], str) and envelope['session_id'].strip(),
            'Invalid local executor/session identity')
    require(isinstance(envelope['task_id'], str) and isinstance(envelope['request_hash'], str)
            and type(envelope['task_revision']) is int and envelope['task_revision'] > 0,
            'Invalid local task identity/revision')
    event_id = digest(envelope)
    tasks = ledger.tasks
    if tasks.db.execute('SELECT 1 FROM control_events WHERE id=?', (event_id,)).fetchone():
        return tasks.get(envelope['task_id'])
    row = tasks.get(envelope['task_id'])
    require(row['request_hash'] == envelope['request_hash'], 'Local result request hash is stale')
    # A failed row keeps receiving cleanup bookkeeping (Agent retirement,
    # recovery notes) while it runs locally, which bumps its revision. A
    # takeover is bound by the failed execution identity instead.
    require(takeover or row['revision'] == envelope['task_revision'], 'Local result task revision is stale')
    if takeover:
        require(failed_local(row), 'Local takeover admission rejected: task is no longer eligible')
        require(envelope['failed_execution'] == failed_execution_identity(row),
                'Local takeover failed execution identity is stale')
        require(not row['data'].get('agent_id') or control.agent_retired(row['data']),
                'Local takeover admission rejected: original CMA Agent may still execute')
    else:
        require(executable(row), 'Local result admission rejected: task already started or has a result')
    context = envelope['context']
    require(isinstance(context, dict) and set(context) <= {'thread_inputs', 'mailbox_history'},
            'Invalid local execution context')
    require(all(isinstance(v, dict) and (k not in row['data'] or row['data'][k] == v)
                for k, v in context.items()), 'Local context cannot replace coordinator evidence')
    data = {**row['data'], **context}
    candidate = {**row, 'data': data}
    result = copy.deepcopy(envelope['result'])
    control.validate_result(result, candidate, allow_packet_artifact=False)
    gates(candidate, tasks, result)
    require(all('agent_path' not in a for a in result['artifacts']), 'Local result needs preserved Release artifacts')
    control.verify_artifacts(result, ledger.releases, ledger.folder)
    with tasks.tx():
        # Compare again at the actual ledger write boundary.
        current = tasks.get(row['id'])
        if takeover:
            require(current['revision'] == row['revision'] and failed_local(current),
                    'Local takeover admission raced coordinator')
            require(envelope['failed_execution'] == failed_execution_identity(current),
                    'Local takeover execution identity changed')
            require(not current['data'].get('agent_id') or control.agent_retired(current['data']),
                    'Local takeover admission raced original CMA execution')
        else:
            require(current['revision'] == row['revision'] and executable(current),
                    'Local result admission raced CMA')
        data.update(result=result, executor='local-codex', local_session_id=envelope['session_id'])
        if takeover:
            data['local_takeover'] = True
            data['failed_execution'] = envelope['failed_execution']
        tasks._update(row['id'], row['revision'], 'held' if result['outcome'] == 'hold' else 'awaiting-review', data)
        tasks.db.execute('INSERT INTO control_events VALUES(?,?)', (event_id, canonical(envelope)))
    return tasks.get(row['id'])


POLISH = """Final language pass on one outgoing email.
Read context.json (the complete correspondence: thread_inputs and mailbox_history),
prompt.txt (the task) and result.json (the draft; packet.body is the letter).
Rewrite packet.body as one coherent letter that reads naturally after the whole
thread: when it covers several points, open with one framing sentence, then follow
a logical order with smooth transitions instead of a stack of separate paragraphs.
Match the register and forms of address used in this thread; keep it light,
friendly and concise rather than stiffly academic. Write plainly: do not use em
dashes or en dashes as sentence punctuation (use a comma, a period or "and"
instead); hyphens inside words and minus signs in formulas are fine. Keep every fact, number,
formula, link, attachment mention, commitment and scope statement exactly; add
no new claims; keep the salutation; end with "Best regards," followed by "{signoff}" and no affiliation lines.
Write only the new body text to polish/body.txt and a two-line change note to polish/notes.txt.
"""


def env_without_review_key():
    return {k: v for k, v in os.environ.items()
            if k not in {'OUTREACH_REVIEW_KEY', 'OUTREACH_REVIEW_KEY_FILE', 'GITHUB_ACTIONS'}}


POLISH_TIMEOUT = 20 * 60


def polish_guard(original, polished):
    """Every link and attachment-like token of the draft must survive polishing."""
    import re
    tokens = set(re.findall(r'https?://\S+', original)) | set(re.findall(r'\b[\w.-]+\.(?:pdf|zip|lean|tex)\b', original))
    missing = sorted(t.rstrip('.,;)') for t in tokens if t.rstrip('.,;)') not in polished)
    return missing


def polish(result, directory, env):
    """Last step for every letter: one Codex pass over the full thread context."""
    packet = result.get('packet')
    if result.get('outcome') != 'completed' or not isinstance(packet, dict) or not packet.get('body'):
        return result
    directory = Path(directory)
    out = directory / 'polish' / 'body.txt'
    if not out.exists():
        (directory / 'polish').mkdir(exist_ok=True)
        signoff = ' and '.join(control.settings('identity')['first_names'])
        (directory / 'polish' / 'brief.txt').write_text(POLISH.format(signoff=signoff))
        with (directory / 'polish' / 'codex.jsonl').open('a') as log:
            process = subprocess.Popen(['codex', 'exec', '-s', 'workspace-write', '--skip-git-repo-check',
                                        '-c', 'approval_policy="never"',
                                        '-c', 'sandbox_workspace_write.network_access=true', '--json',
                                        'Read polish/brief.txt and follow it.'],
                                       cwd=directory, stdin=subprocess.DEVNULL, stdout=log,
                                       stderr=subprocess.STDOUT, env=env)
            (directory / 'polish' / 'pid').write_text(str(process.pid) + '\n')
            try:
                code = process.wait(timeout=POLISH_TIMEOUT)
            except subprocess.TimeoutExpired:
                # An overloaded upstream can keep Codex reconnecting long after
                # body.txt is written.
                process.kill(); process.wait(); code = -9
            if not out.exists():
                (directory / 'polish' / 'skipped.txt').write_text(
                    'Codex polish failed (exit ' + str(code) + '); original body kept.\n')
                return result
    body = out.read_text().strip('\n')
    missing = polish_guard(packet['body'], body)
    if not body.strip() or missing:
        (directory / 'polish' / 'skipped.txt').write_text('Guard kept the original body; missing: ' +
                                                         ', '.join(missing) + '\n')
        return result
    result = copy.deepcopy(result)
    result['packet']['body'] = body
    result['report'] = (result.get('report') or '') + '\n\nFinal language pass: Codex polished the letter over the full thread context.'
    return result


def execute(row, releases, directory, config, capacity, takeover=False):
    require(row['id'] not in {'.', '..'}, 'Unsafe local task directory')
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / '.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError('A local executor is already running this task') from None
        binding = {'task_id': row['id'], 'request_hash': row['request_hash'], 'task_revision': row['revision']}
        if takeover:
            require(failed_local(row), 'Task is not failed with suggested exit local-codex')
            require(not row['data'].get('agent_id') or control.agent_retired(row['data']),
                    'Local takeover refused while the original CMA Agent may still execute')
            binding.update(takeover=True, failed_execution=failed_execution_identity(row))
        path = directory / 'binding.json'
        if path.exists():
            require(json.loads(path.read_text()) == binding, 'Run directory belongs to an obsolete task revision')
        else:
            path.write_text(canonical(binding))
        staged = directory / 'staged.json'
        if staged.exists():
            return json.loads(staged.read_text())
        context = {k: row['data'][k] for k in ('thread_inputs', 'mailbox_history') if k in row['data']}
        context_path = directory / 'context.json'
        result_path = directory / 'result.json'
        if context_path.exists() and result_path.exists():
            # Resume: the draft was written against the stored context; a fresh
            # mailbox snapshot always differs in its fetch time. The coordinator's
            # save-time preflight still rejects a draft overtaken by newer mail.
            context = json.loads(context_path.read_text())
        else:
            context_path.write_text(canonical(context))
        prompt_path = directory / 'prompt.txt'
        if not prompt_path.exists():
            prompt_path.write_text(prompt(row, config, capacity))
        result_path = directory / 'result.json'
        events = directory / 'codex.jsonl'
        if not result_path.exists():
            env = {k: v for k, v in os.environ.items()
                   if k not in {'OUTREACH_REVIEW_KEY', 'OUTREACH_REVIEW_KEY_FILE', 'GITHUB_ACTIONS'}}
            with events.open('a') as output, (directory / 'codex.stderr.log').open('a') as errors:
                process = subprocess.Popen(['codex', 'exec', '-s', 'workspace-write', '--skip-git-repo-check',
                                            '-c', 'approval_policy="never"',
                                            '-c', 'sandbox_workspace_write.network_access=true', '--json',
                                            RUN_INSTRUCTION],
                                           cwd=directory, stdin=subprocess.DEVNULL, stdout=output, stderr=errors,
                                           env=env, pass_fds=(lock.fileno(),))
                (directory / 'pid').write_text(str(process.pid) + '\n')
                require(process.wait() == 0, 'Codex failed; inspect the private run logs')
        session = None
        for line in events.read_text().splitlines():
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if event.get('type') == 'thread.started':
                session = event.get('thread_id')
        require(bool(session), 'Missing Codex session id in private run log')
        raw = json.loads(result_path.read_text())
        packet = raw.get('packet')
        if isinstance(packet, dict) and set(packet) == {'artifact'}:
            # Some runs write the letter to deliverables/<file> and reference it.
            raw['packet'] = json.loads((directory / 'deliverables' / packet['artifact']).read_text())
        if isinstance(raw.get('packet'), dict):
            allowed = {'schema_version', 'kind', 'sender', 'to', 'cc', 'subject', 'body',
                       'problem_ids', 'attachments', 'thread_id', 'latest_inbound_message_id',
                       'user_decision_ever'}
            raw['packet'] = {k: v for k, v in raw['packet'].items() if k in allowed}
        # A registered sender needs no alias or new-contact proposal; the
        # coordinator rejects either one, which used to fail the whole staging.
        patch = raw.get('registry_patch')
        if isinstance(patch, dict) and (patch.get('alias_of') is not None or patch.get('new_contact') is not None):
            from .registry import people
            observation = effective_inputs(row).get('mailbox_observation', {})
            sender = observation.get('email') if isinstance(observation, dict) else None
            if sender and any(sender.casefold() in p['emails'] for p in people(control.ROOT)):
                if 'alias_of' in patch: patch['alias_of'] = None
                patch['new_contact'] = None
        if row['kind'] in {'reply', 'firstcontact'}:
            raw = polish(raw, directory, env_without_review_key())
        result = preserve(raw, row, releases, directory)
        envelope = control.sign_review({'schema_version': 1, **binding, 'executor': 'local-codex',
                                        'session_id': session, 'context': context, 'result': result}, control.review_key())
        (directory / 'local-result.json').write_text(canonical(envelope))
        receipt = control.stage(envelope, 'local-result', releases, directory)
        staged.write_text(canonical(receipt))
        return receipt


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='action', required=True)
    listing = commands.add_parser('list')
    listing.add_argument('--kinds', help='Comma-separated task kinds; mail is always first')
    listing.add_argument('--failed-local', action='store_true',
                         help="List failed tasks whose recovery suggests local-codex takeover")
    run = commands.add_parser('run')
    run.add_argument('--task', required=True, action='append', help='Task or revising parent ID; repeat for a batch')
    run.add_argument('--workspace', type=Path, help='Override the person workspace containing local-runs/')
    run.add_argument('--parallel', type=int, default=1)
    run.add_argument('--takeover', action='store_true',
                     help='Take over a failed task whose recovery suggests local-codex')
    args = parser.parse_args(argv)
    kinds = set(args.kinds.split(',')) if args.action == 'list' and args.kinds else None
    require(kinds is None or kinds <= KINDS, 'Unknown task kind')
    if args.action == 'run':
        require(args.parallel > 0, '--parallel must be positive')
    releases = control.Releases()
    failed = False
    with tempfile.TemporaryDirectory(prefix='outreach-local-') as folder:
        ledger = control.Ledger(releases, folder)
        try:
            rows = ledger.tasks.all()
            created = dict(ledger.tasks.db.execute('SELECT id, MIN(at) FROM task_events GROUP BY id'))
            if args.action == 'list':
                if args.failed_local:
                    for row in failed_local_selection(rows, kinds):
                        recovery = row['data'].get('recovery') or {}
                        print(row['id'], row['kind'], recovery.get('error_class', ''),
                              recovery.get('reason', row['data'].get('reason', '')), sep='\t')
                    return
                for row in selection(rows, kinds, created):
                    print(row['id'], row['kind'], row['owner'], row['state'], sep='\t')
                return
            config = control.load_config(control.ROOT)
            selected = {r['id']: r for task_id in args.task
                        for r in [target(rows, task_id, args.takeover)]}
            capacity = control.oracle_capacity_snapshot(ledger.tasks)
            prepared = []
            candidates = (list(selected.values()) if args.takeover else
                          selection(list(selected.values()), created=created))
            for row in candidates:
                try:
                    prepared.append(prepare(row, ledger, config))
                except Exception as exc:
                    failed = True
                    print(canonical({'task_id': row['id'], 'error': str(exc)[:300]}))
        finally:
            ledger.tasks.close()  # Never save, ingest or claim from this local copy.
    with ThreadPoolExecutor(max_workers=args.parallel) as pool:
        execute_args = {'takeover': args.takeover} if args.takeover else {}
        jobs = {pool.submit(execute, row, releases,
                            (args.workspace or workspace(row)) / 'local-runs' / row['id'], config, capacity,
                            **execute_args): row['id']
                for row in prepared}
        for job in as_completed(jobs):
            try:
                print(canonical({'task_id': jobs[job], **job.result()}))
            except Exception as exc:
                failed = True
                print(canonical({'task_id': jobs[job], 'error': str(exc)[:300]}))
    if failed:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
