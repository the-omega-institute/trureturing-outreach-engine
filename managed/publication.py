"""Apply the owner's standing note-publication decision in the single writer.

The hourly short-notes worker owns Zenodo/Pages. This adapter selects exact PASS
artifacts, registers their stable IDs and queues its existing immutable requests.
Mathesis selection uses the same signing/ingestion path as stage-mathesis.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import zipfile

from .policy import policy_text
from .queue import canonical, digest, require
from .correspondence import linked_contacts
from .github_diagnostics import failure as github_failure
from .tick import units
from . import recheck, recording
from .tasks import INDEPENDENT_REVIEWERS

# Reuse the existing delivery service, including its validators and file contract.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts/short_notes'))
import service as notes


FAILURE_BACKOFF = (recheck.RECHECK_SECONDS, 2 * 3600, 8 * 3600, 24 * 3600)
MAX_FIRST_FAILURES_PER_TICK = 2


class Publications(notes.w.GitHub):
    def run(self, *args, allow_missing=False):
        try:
            return super().run(*args, allow_missing=allow_missing)
        except RuntimeError as error:
            error.github_diagnostic = github_failure(error, args)
            raise

    def register_note(self, metadata):
        """Merge only the note enrollment into the current maintained table.

        Contents API's sha fences concurrent edits. A conflict retries next tick
        by reading the new table, without overwriting another operator's changes.
        No coordinator checkout push or workflow change is needed.
        """
        from .data_writes import contents_update

        def transform(raw):
            catalog = notes.validate_catalog(json.loads(raw))
            updated = notes.register(catalog, metadata)
            notes.catalog_note(updated, metadata['id'], require_active=True)
            return (raw if updated == catalog else notes.w.encode(updated)), updated

        return contents_update(self.run, self.repo, 'tables/publications.json', transform,
                               'Register reviewed short note ' + metadata['id'])


def standing(root):
    table = json.loads((Path(root) / 'tables/decisions.json').read_text())
    decision = table.get('short_note_publication')
    require(isinstance(decision, dict) and decision.get('enabled') is True,
            'No active standing publication decision')
    require(decision.get('decision_ref') and decision.get('purpose') == 'publication'
            and decision.get('processing_consent') and decision.get('make_public_after_acceptance') is True,
            'The standing decision must authorize publication and processing')
    policy = json.loads(policy_text('managed/submission/author-policy.json', data_root=root))
    disclosure = decision['ai_disclosure']
    require(disclosure.get('level') == 'assisted' and all(
        disclosure.get('statement_' + variant) == policy['locked_statement_' + variant]
        for variant in ('with_lean', 'without_lean')), 'Use the current locked AI statements verbatim')
    return table, decision


def lineage(row, tasks):
    rows, seen = [], set()
    while row['id'] not in seen:
        seen.add(row['id']); rows.append(row)
        inputs = row['request']['inputs']
        parent = (inputs.get('pipeline', {}).get('note_task_id') or
                  inputs.get('automation_parent', {}).get('task_id'))
        if not parent and row['kind'] == 'mathesis':
            links = inputs.get('note_sources', [])
            if len(links) == 1:
                parent = links[0]['task_id']
        if not parent:
            return rows
        row = tasks.get(parent)
    raise ValueError('Publication source cycle')


def identities(row, metadata=None):
    inputs = row['request']['inputs']
    values = {row['id'], inputs.get('result_id'),
              inputs.get('pipeline', {}).get('result_id'), inputs.get('result', {}).get('id')}
    values.update(row['data'].get('publication_loop', {}).get('problem_ids', []))
    values.add(row['data'].get('publication_loop', {}).get('note_id'))
    if metadata:
        values.update([metadata['id'], *metadata['problem_ids']])
    return values - {None}


def exclusion_scope(row, tasks, metadata=None, *, result_id=None):
    """The source rows, identities and related notes the exclusion check reads."""
    parents = lineage(row, tasks)
    selected = set().union(*(identities(r) for r in parents), identities(row, metadata))
    if result_id:
        selected.add(result_id)
    related = [task for task in tasks.all()
               if task['kind'] in {'shortnote', 'mathesis'} and identities(task) & selected]
    if result_id:
        # Public issue filing is result-wide, including holds pinned to another note for that result.
        selected.update(task['id'] for task in related)
    return parents, selected, related


def exclusion(row, tasks, table, decision, metadata=None, *, result_id=None):
    return excluded_by(*exclusion_scope(row, tasks, metadata, result_id=result_id), table, decision, metadata)


def exclusion_inputs(scope, table, decision, metadata=None):
    """Digest every value excluded_by() reads for one scope.

    The lineage, the selected identities, each source/related row's review
    verdict and joint-work fields, the holds table and the decision determine
    the answer. Creators come from the note metadata, which the exact result
    fixes, so callers compare the result hash separately.
    """
    parents, selected, related = scope

    def facts(task):
        data, inputs = task['data'], task['request']['inputs']
        return [task['id'], data.get('review', {}).get('verdict'),
                [[obj.get('joint_work'), obj.get('authorship')] for obj in (inputs, data, data.get('result', {}))]]

    note = {'id': metadata['id'], 'problem_ids': metadata['problem_ids']} if metadata else None
    return digest({'selected': sorted(canonical(value) for value in selected),
                   'parents': [facts(r) for r in parents],
                   'related': [facts(r) for r in related], 'holds': table.get('holds', []),
                   'decision': decision, 'note': note})


def check_exclusion(row, tasks, table, decision, metadata=None):
    """Return the exclusion reason and, when excluded, the digest of its inputs."""
    scope = exclusion_scope(row, tasks, metadata)
    reason = excluded_by(*scope, table, decision, metadata)
    if not reason:
        return None, None
    inputs = {'exclusion_inputs': exclusion_inputs(scope, table, decision, metadata)}
    if metadata:
        # Recomputing the digest needs the note identities the package supplied.
        inputs['exclusion_note'] = {'id': metadata['id'], 'problem_ids': metadata['problem_ids']}
    return reason, inputs


def exclusion_unchanged(old, row, tasks, table, decision, result_hash, decision_hash):
    """True while nothing the recorded exclusion read has changed.

    A lifted hold, a changed verdict, joint-work field, lineage or related
    note, a new decision or a new result changes the digest and is evaluated
    again. Exclusions recorded before the digest existed are evaluated once.
    """
    if not (old.get('phase') == 'excluded' and old.get('exclusion_inputs')
            and old.get('result_hash') == result_hash and old.get('decision_hash') == decision_hash):
        return False
    note = old.get('exclusion_note')
    try:
        return exclusion_inputs(exclusion_scope(row, tasks, note), table, decision, note) == old['exclusion_inputs']
    except (ValueError, KeyError, TypeError, AttributeError):
        return False


class Snapshot:
    """Read-only task view over rows already loaded for this pass."""

    def __init__(self, rows):
        self.rows = rows
        self.by_id = {row['id']: row for row in rows}

    def get(self, task_id):
        require(task_id in self.by_id, 'Unknown task')
        return self.by_id[task_id]

    def all(self):
        return self.rows


def excluded_by(parents, selected, related, table, decision, metadata=None):
    for hold in table.get('holds', []):
        targets = {hold.get(k) for k in ('legacy_id', 'task_id', 'note_id', 'id')}
        targets.update(hold.get('problem_ids', []))
        if selected & (targets - {None}) and hold.get('decision', '').upper() in {'HOLD', 'JOINT-WORK'}:
            return 'held in tables/decisions.json'
    for parent in parents + related:
        data, inputs = parent['data'], parent['request']['inputs']
        # An explicit HOLD verdict holds the result. Agent holds on superseded
        # revisions and parked operational rows do not.
        if data.get('review', {}).get('verdict') == 'HOLD':
            return 'task or source note is held'
        if any(obj.get('joint_work') is True or obj.get('authorship') in {'joint', 'joint-work'}
               for obj in (inputs, data, data.get('result', {}))):
            return 'joint work needs its own publication decision'
    if metadata:
        allowed = {notes.w.person_name(name) for name in decision['independent_authors']}
        if any(notes.w.person_name(a['name']) not in allowed for a in metadata['creators']):
            return 'joint work needs its own publication decision'
    return None


def reviewed(row):
    from .automation import REVIEWED, reviewed_shortnote
    result = row['data'].get('result', {})
    review = row['data'].get('review', {})
    binding = review.get('binding', {})
    return (row['state'] in REVIEWED and (row['kind'] == 'mathesis' or reviewed_shortnote(row))
            and result.get('outcome') == 'completed' and review.get('verdict') == 'PASS'
            and review.get('reviewer') in INDEPENDENT_REVIEWERS
            and binding.get('result_hash') == digest(result)
            and binding.get('workflow_hash') == row['request']['workflow_hash'])


def source_gate(row, tasks, root):
    from .automation import parent_gate
    from . import note_closure
    from .pipeline import source_gate as current_source
    current_source(row['request'], tasks, root)
    note_closure.source_gate(row['request'], tasks, row['data'].get('result'))
    parent_gate(row['request'], tasks)
    # Pipeline provenance pins the parent's result, and publication also checks
    # that its PASS still binds those exact bytes.
    # Only the nearest source note must still carry its PASS; earlier revisions
    # in its chain were bounced on the way there. A short note publishes its
    # own exact PASS (checked by the caller): its ancestors are earlier
    # revisions or guard continuations that were bounced or stopped, and never
    # need a PASS of their own (2026-11-14: a PASSed note whose parent was a
    # guard-stopped continuation was refused).
    if row['kind'] == 'shortnote':
        return
    for parent in lineage(row, tasks)[1:]:
        if parent['kind'] == 'shortnote' and parent['request']['inputs'].get('note_round') != 'draft':
            require(reviewed(parent), 'The source note no longer has an exact PASS')
            break


@recording.evidence_download
def download(row, name, directory, releases):
    require(re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,199}', name) is not None,
            'Select a safe reviewed artifact name')
    matches = [a for a in row['data']['result']['artifacts'] if a['name'] == name]
    require(len(matches) == 1, 'The package file is not an unambiguous reviewed artifact')
    artifact = matches[0]
    require(artifact['release_tag'] == 'outreach-task-' + row['id'], 'Preserve the task source identity')
    require(type(artifact['bytes']) is int and 0 < artifact['bytes'] <= notes.w.MAX_FILE,
            'Invalid scientific artifact size')
    path = Path(directory) / name

    def exact():
        return (path.exists() and not path.is_symlink() and path.stat().st_size == artifact['bytes']
                and hashlib.sha256(path.read_bytes()).hexdigest() == artifact['sha256'])

    # gh refuses to overwrite an existing file, so a second read of the same
    # evidence in one directory failed every attempt (2026-11-23..25). Reuse an
    # exact copy; replace anything else explicitly.
    if exact():
        return path
    releases.run('release', 'download', artifact['release_tag'], '--repo', releases.repo,
                 '--pattern', name, '--dir', str(directory), *(['--clobber'] if path.exists() or path.is_symlink() else []))
    require(exact(), 'The preserved artifact differs from the exact PASS')
    return path


@recording.package_read
def package(row, directory, releases):
    """Materialize only the reviewed scientific package; never repack its ZIP."""
    names = {a['name'] for a in row['data']['result']['artifacts']}
    if 'handoff.json' in names:
        manifest = json.loads(download(row, 'handoff.json', directory, releases).read_text())
        require(manifest.get('schema_version') == 'outreach-mathesis-handoff.v1'
                and manifest.get('private_correspondence_included') is False,
                'Select a scientific-only handoff')
        metadata, files = manifest['source_note'], manifest['files']
    else:
        manifests = sorted(n for n in names if re.fullmatch(r'note-v[1-9][0-9]*\.json', n))
        if manifests:
            require(len(manifests) == 1, 'Select exactly one reviewed note revision')
            manifest = json.loads(download(row, manifests[0], directory, releases).read_text())
            require(manifest.get('schema_version') == 'short-note.v1', 'Invalid reviewed note manifest')
            metadata, files = manifest['metadata'], manifest['files']
        else:
            metadata = json.loads(download(row, 'metadata.json', directory, releases).read_text())
            files = None
    notes.w.validate_metadata(metadata)
    if files is None:
        artifacts = row['data']['result']['artifacts']
        pdfs = [a['name'] for a in artifacts if a['role'] == 'note-pdf']
        sources = [a['name'] for a in artifacts if a['role'] == 'source-archive' and a['name'].endswith('.zip')]
        require(len(pdfs) == len(sources) == 1, 'Await the reviewed PDF/source ZIP package')
        pdf, source = pdfs[0], sources[0]
    else:
        pdf, source = f"note-v{metadata['revision']}.pdf", f"source-v{metadata['revision']}.zip"
        require(set(files) == {pdf, source}, 'Await the canonical reviewed PDF/source ZIP package')
    for name in (pdf, source):
        path = download(row, name, directory, releases)
        if files is not None:
            require(hashlib.sha256(path.read_bytes()).hexdigest() == files[name], 'The handoff package differs')
    meta = Path(directory) / 'selected-metadata.json'
    meta.write_bytes(notes.w.encode(metadata))
    prepared = notes.w.prepare(meta, Path(directory) / pdf, Path(directory) / source,
                               Path(directory) / 'prepared')
    return metadata, prepared, source


def choices(decision, metadata, source):
    # A pinned Lean source reference is scoped verification evidence in the
    # reviewed metadata. Source archives may carry Lean files directly as well.
    with zipfile.ZipFile(source) as archive:
        lean = any(name.endswith('.lean') for name in archive.namelist())
    lean |= bool(re.search(r'\.lean(?:$|[?#])', metadata['evidence_url']))
    disclosure = decision['ai_disclosure']
    return {'decision_ref': decision['decision_ref'], 'purpose': decision['purpose'],
            'processing_consent': decision['processing_consent'],
            'make_public_after_acceptance': decision['make_public_after_acceptance'],
            'ai_disclosure': {'level': disclosure['level'],
                              'statement': disclosure['statement_with_lean' if lean else 'statement_without_lean']}}


def record(ledger, task_id, value, force=False):
    row = ledger.tasks.get(task_id)
    ledger.tasks.update(task_id, row['revision'], row['state'], {**row['data'], 'publication_loop': value})
    ledger.save(force=force)


def published_doi(gh, metadata, prepared):
    release = gh.release(notes.w.tag_for(metadata['id']))
    state = notes.w.release_state(release, metadata['id']) if release else None
    entry = (state or {}).get('versions', {}).get(str(metadata['revision']), {})
    if not entry.get('publication'):
        return None  # A reserved DOI is deliberately excluded.
    manifest = Path(prepared) / f"note-v{metadata['revision']}.json"
    require(entry.get('package_id') == hashlib.sha256(manifest.read_bytes()).hexdigest(),
            'The published DOI belongs to another scientific package')
    return entry['doi']


def delivery_choices(ledger, gh, decision, metadata, prepared, selected):
    """Freeze one metadata selection across tasks packaging the same revision."""
    package_id = hashlib.sha256((Path(prepared) / f"note-v{metadata['revision']}.json").read_bytes()).hexdigest()
    key = 'publication-selection:' + digest([metadata['id'], metadata['revision']])
    previous = ledger.tasks.db.execute('SELECT payload FROM pipeline_state WHERE key=?', (key,)).fetchone()
    if previous:
        saved = json.loads(previous[0])
        require(saved['package_id'] == package_id and saved['decision_hash'] == digest(decision),
                'An existing publication selection is immutable; use a new note revision')
        return saved['decisions']
    doi = published_doi(gh, metadata, prepared)
    if doi:
        selected['doi'] = doi
    # A newer note revision uses the actual Mathesis version, not our revision
    # number. Pending older deliveries retain their handles and finish first.
    receipts = []
    for row in ledger.tasks.all():
        delivery = row['data'].get('mathesis_delivery', {})
        envelope = delivery.get('envelope', {})
        prior = envelope.get('metadata', {})
        if (prior.get('producer') == 'trureturing-outreach' and prior.get('work_id') == metadata['id']
                and prior.get('paper', {}).get('purpose') == 'publication'
                and prior.get('version') != str(metadata['revision'])):
            require(delivery.get('receipt'), 'Await the earlier Mathesis delivery receipt')
            receipts.append(delivery['receipt'])
    if receipts:
        selected['previous_version'] = max(r['version'] for r in receipts)
    ledger.tasks.db.execute('INSERT INTO pipeline_state VALUES(?,?)', (key, canonical({
        'package_id': package_id, 'decision_hash': digest(decision), 'decisions': selected})))
    ledger.save(force=True)
    return selected


def delivers(row, key):
    """True when the row's own Mathesis selection binds this revision and its current result.

    The signed selection is durable evidence; publication_loop is replaced by
    later failure and exclusion records. Every envelope is built for, and
    stored on, its own task, so the task identity needs no separate check.
    """
    envelope = row['data']['mathesis_delivery'].get('envelope', {})
    metadata = envelope.get('metadata', {})
    return ((metadata.get('work_id'), metadata.get('version')) == key
            and envelope.get('result_hash') == digest(row['data']['result']))


@recording.stage_frame('publication')
def advance(ledger, config, root, folder, gh=None, secret=None):
    from .note_revisions import Revisions
    if config['paused'] or not config['kinds'].get('mathesis') or config.get('mathesis', {}).get('test_only'):
        return
    path = Path(root) / 'tables/decisions.json'
    if not path.exists() or not json.loads(path.read_text()).get('short_note_publication', {}).get('enabled'):
        return  # Compatibility with historical/test control snapshots.
    table, decision = standing(root)
    if config.get('mathesis', {}).get('mode') != 'deliver':
        return
    secret = secret or os.environ.get('OUTREACH_REVIEW_KEY')
    if not secret:
        return
    gh = recording.adapter('publication', gh or Publications(ledger.releases.repo))
    all_rows = ledger.tasks.all()
    revisions = Revisions(ledger.tasks, all_rows)
    # Live Mathesis descendants own publication of their source note, including
    # while their separate review is pending. Failed/superseded attempts do not.
    mathesis_sources = set()
    for child in all_rows:
        if child['kind'] == 'mathesis' and child['state'] not in {'failed', 'superseded'}:
            try:
                parents = lineage(child, ledger.tasks)[1:]
            except ValueError:
                continue  # A malformed lineage stays subject to its own source gate below.
            mathesis_sources.update(parent['id'] for parent in parents if parent['kind'] == 'shortnote')
    queued = {(loop.get('note_id'), loop.get('revision')) for r in all_rows
              if (loop := r['data'].get('publication_loop', {})).get('phase') == 'queued'}
    # Revisions whose Mathesis selection a Mathesis row has already taken.
    taken = {(loop.get('note_id'), loop.get('revision')) for r in all_rows if r['kind'] == 'mathesis'
             and (loop := r['data'].get('publication_loop', {})).get('phase') == 'queued'}
    rows = [r for r in all_rows if r['kind'] in {'mathesis', 'shortnote'} and reviewed(r)
            and r['id'] not in mathesis_sources and not revisions.reason(r)]
    # Keep correspondence and owner priority, including for equal retry times.
    correspondence = linked_contacts(all_rows, root, config) if rows else {}
    tick = getattr(ledger, 'tick', None)
    deferred = {key: i for i, key in enumerate(tick.previous.get('publication', []))} if tick else {}
    rows.sort(key=lambda r: (r['id'] not in correspondence, not r['request']['inputs'].get('owner_ids'),
                             deferred.get(r['id'], len(deferred)), r['kind'] != 'mathesis'))
    decision_hash = digest(decision)
    snapshot = Snapshot(all_rows)
    attempts, retries = [], []
    for row in rows:
        old = row['data'].get('publication_loop', {})
        result_hash = digest(row['data']['result'])
        if old.get('phase') == 'queued' and old.get('result_hash') == result_hash:
            continue
        # Re-recording an unchanged exclusion took ~5 s per row on every tick
        # (2026-11-23) and starved Mathesis delivery in the same stage.
        if exclusion_unchanged(old, row, snapshot, table, decision, result_hash, decision_hash):
            continue
        binding = [result_hash, decision_hash]
        retry_at = recheck.next_at(ledger.tasks, 'publication', row['id'], binding)
        if retry_at is not None and ledger.tasks.clock() < retry_at:
            continue
        same_binding = (old.get('result_hash', result_hash) == result_hash
                        and old.get('decision_hash', decision_hash) == decision_hash)
        if same_binding and old.get('phase') == 'pending' and old.get('last_error'):
            # Older failure records may have no saved due time.
            retries.append((retry_at if retry_at is not None else row['updated'], row, binding))
        else:
            attempts.append((row, binding, False))
    if retries:
        # New rows keep precedence; then several due retries, oldest due
        # first, so a backlog of failures clears instead of one per tick.
        retries.sort(key=lambda item: item[0])
        limit = config['mathesis'].get('max_publication_retries_per_tick', 3)
        attempts += [(row, binding, True) for _, row, binding in retries[:limit]]

    remaining = config['mathesis'].get('max_publications_per_tick', 2)
    first_failures = 0
    clock = tick.clock if tick else ledger.clock
    for row, binding, retry in units(ledger, 'publication', attempts,
                                     key=lambda item: item[0]['id'], preserve_order=True):
        if remaining <= 0:
            break
        if not retry and first_failures >= MAX_FIRST_FAILURES_PER_TICK:
            continue
        started = clock()
        old = row['data'].get('publication_loop', {})
        state = {'phase': 'preparing', 'result_hash': binding[0],
                 'decision_ref': decision['decision_ref'], 'decision_hash': decision_hash}
        step = 'exclusion'
        try:
            reason, inputs = check_exclusion(row, ledger.tasks, table, decision)
            if reason:
                record(ledger, row['id'], {**state, 'phase': 'excluded', 'reason': reason, **inputs})
                continue
            step = 'source-gate'
            source_gate(row, ledger.tasks, root)
            step = 'package'
            with tempfile.TemporaryDirectory(prefix='publication-', dir=folder) as temp:
                directory = Path(temp)
                metadata, prepared, source = package(row, directory, ledger.releases)
                if reason := revisions.reason(row, metadata['problem_ids']):
                    record(ledger, row['id'], {**state, 'phase': 'pending', 'reason': reason})
                    continue
                key = (metadata['id'], metadata['revision'])
                # A revision queued by any non-Mathesis row, including one
                # queued earlier in this tick, is already submitted. A Mathesis
                # row still owes its own selection unless a Mathesis row took
                # it. A recorded row whose selection already binds this
                # revision and result completes its 'queued' record, whatever
                # record replaced it. Every other row with a queued revision,
                # including a delivered row without any record, is skipped.
                submitted = key in queued
                if submitted and (row['kind'] != 'mathesis' or key in taken or (
                        row['data'].get('mathesis_delivery') and not (old and delivers(row, key)))):
                    continue
                step = 'exclusion'
                reason, inputs = check_exclusion(row, ledger.tasks, table, decision, metadata)
                if reason:
                    record(ledger, row['id'], {**state, 'phase': 'excluded', 'reason': reason, **inputs})
                    continue
                state.update(note_id=metadata['id'], revision=metadata['revision'],
                             problem_ids=metadata['problem_ids'], creators=metadata['creators'])
                step = 'choices'
                selected = choices(decision, metadata, directory / source)
                selected['source_file'] = source
                # This barrier precedes every registration, upload and enqueue.
                # Retain the last failure across a restart during a retry.
                step = 'checkpoint'
                checkpoint = {**old, **state, 'phase': 'pending'} if retry else state
                record(ledger, row['id'], checkpoint, force=True)
                if not submitted:
                    step = 'register'
                    catalog = gh.register_note(metadata)
                    step = 'stage'
                    notes.w.stage(prepared, gh)
                    request = {'schema_version': 'short-note-request.v1', 'id': metadata['id'],
                               'action': 'publish', 'revision': metadata['revision']}
                    step = 'enqueue'
                    receipt = notes.enqueue(catalog, request, gh)
                    state['request'] = receipt['request']
                    step = 'request-readback'
                    with tempfile.TemporaryDirectory(dir=folder) as readback:
                        saved = gh.download(notes.w.tag_for(metadata['id']), receipt['request'], readback)
                        require(notes.w.read(saved) == request, 'Publication request readback differs')
                # Mathesis and Zenodo are independent consumers. The current
                # Mathesis contract explicitly permits delivery before any DOI.
                if row['kind'] == 'mathesis' and not row['data'].get('mathesis_delivery'):
                    from .mathesis_delivery import ingest
                    from .pre_reviewed import select, history_choices, cache_prior_work
                    step = 'prior-work-cache'
                    row = cache_prior_work(ledger, row, directory, ledger.releases)
                    step = 'delivery-choices'
                    selected = delivery_choices(ledger, gh, decision, metadata, prepared, selected)
                    step = 'history-choices'
                    selected = history_choices(ledger, metadata, selected)
                    prior_work = row['data']['mathesis_prior_work'].get('check')
                    step = 'select'
                    envelope = select(row, directory, selected, secret, ledger.tasks, ledger.releases, metadata,
                                      prior_work)
                    step = 'ingest'
                    ingest(ledger.tasks, envelope, secret)
                    ledger.save(force=True)
                step = 'checkpoint'
                recheck.clear(ledger.tasks, 'publication', row['id'])
                record(ledger, row['id'], {**state, 'phase': 'queued'}, force=True)
        except Exception as error:
            failures = old.get('consecutive_failures', 0) + 1 if retry else 1
            if not retry:
                first_failures += 1
            delay = FAILURE_BACKOFF[min(failures, len(FAILURE_BACKOFF)) - 1]
            retry_at = recheck.defer(ledger.tasks, 'publication', row['id'], binding, delay)
            error_class = type(error).__name__
            diagnostic = getattr(error, 'github_diagnostic', None) or github_failure(error) or error_class
            recording.observed_error(error, {'type': error_class, 'diagnostic': diagnostic,
                **({'reason': str(error)} if isinstance(error, ValueError) else {})})
            state.update(phase='pending', last_error=error_class, step=step, diagnostic=diagnostic,
                         consecutive_failures=failures, next_retry_at=retry_at)
            if isinstance(error, ValueError):
                state['reason'] = str(error)
            record(ledger, row['id'], state)
        else:
            state['phase'] = 'queued'
            remaining -= 1
            queued.add(key)
            if row['kind'] == 'mathesis':
                taken.add(key)
        finally:
            # Slow exclusions, duplicates and other no-op paths also yield.
            if state['phase'] == 'preparing' and clock() - started >= recheck.SLOW_SECONDS:
                recheck.defer(ledger.tasks, 'publication', row['id'], binding)
                ledger.save()
