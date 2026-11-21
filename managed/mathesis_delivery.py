"""Signed, receipt-bound Mathesis delivery inside the single coordinator."""
import copy
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import tempfile
from urllib.error import HTTPError

from .correspondence import delivery_problem_ids, linked_contacts
from . import note_revisions
from .mathesis import Mathesis, plan
from .tick import units, unit
from .queue import canonical, digest, require
from .tasks import matches_workflow

TEST_TAG = 'outreach-mathesis-test-20261031'
TEST_SOURCE_SHA = '1a07d345c39df2081ca4796666afb1f2a5628388c3e3e675bffce6d4813c17c2'


def signature(value, secret):
    require(bool(secret), 'The local operator signing key is required')
    return hmac.new(secret.encode(), canonical({'purpose': 'mathesis-delivery',
        'envelope': value}).encode(), hashlib.sha256).hexdigest()


def reviewed(row):
    result = row['data'].get('result', {})
    review = row['data'].get('review', {})
    binding = review.get('binding', {})
    require(row['kind'] == 'mathesis' and result.get('outcome') == 'completed'
            and review.get('verdict') == 'PASS'
            and binding.get('result_hash') == digest(result)
            and binding.get('workflow_hash') == row['request']['workflow_hash'],
            'Mathesis delivery requires the exact independently reviewed result')
    return result


def validate(value, secret, test=False):
    collection = value.get('metadata', {}).get('schema') == 'mathesis.pre-reviewed-delivery.v1'
    require(isinstance(value, dict) and set(value) == {'schema_version', 'task_id',
        'result_hash', 'workflow_hash', 'decision_ref', 'metadata', 'source',
        'reviewer', 'signature'} | ({'pdf'} if collection else set()), 'Invalid Mathesis delivery envelope')
    unsigned = {k: v for k, v in value.items() if k != 'signature'}
    require(hmac.compare_digest(signature(unsigned, secret), value['signature']),
            'Mathesis delivery signature failed')
    require(value['schema_version'] == 1 and value['reviewer'] == 'local-claude-loop'
            and isinstance(value['decision_ref'], str) and value['decision_ref'].strip(),
            'Preserve the actual author/processing decision reference')
    metadata, source = value['metadata'], value['source']
    require(isinstance(value['task_id'], str) and re.fullmatch(r'[A-Za-z0-9_.-]{1,120}', value['task_id']),
            'Invalid Mathesis task identity')
    require(isinstance(source, dict) and set(source) == {'release_tag', 'name', 'sha256', 'bytes', 'role'}
            and source['release_tag'] == 'outreach-task-' + value['task_id']
            and isinstance(source['name'], str) and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,199}', source['name'])
            and isinstance(source['sha256'], str) and re.fullmatch(r'[0-9a-f]{64}', source['sha256'])
            and source['role'] in {'source-archive', 'note-tex', 'manuscript-tex'}
            and type(source['bytes']) is int and 0 < source['bytes'] <= 30 * 1024 * 1024,
            'Select a preserved scientific source from this reviewed task')
    if collection:
        from .pre_reviewed import validate_metadata
        require(not test, 'Collection cannot use the private synthetic manuscript test')
        validate_metadata(metadata)
        pdf = value['pdf']
        require(isinstance(pdf, dict) and set(pdf) == set(source)
                and pdf['release_tag'] == source['release_tag']
                and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,195}\.pdf', pdf['name'])
                and pdf['role'] == 'note-pdf' and type(pdf['bytes']) is int
                and 0 < pdf['bytes'] <= 30 * 1024 * 1024
                and metadata['review']['pdf_sha256'] == pdf['sha256']
                and source['role'] == 'source-archive' and source['name'].endswith('.zip')
                and metadata['review']['source_sha256'] == source['sha256']
                and metadata['producer'] == 'trureturing-outreach', 'Invalid reviewed collection files')
        return value
    require(isinstance(metadata, dict) and metadata.get('schema') == 'mathesis.manuscript-delivery.v1'
            and metadata.get('work_id') and metadata.get('version')
            and isinstance(metadata.get('paper'), dict), 'Missing Mathesis source identity')
    paper = metadata['paper']
    require(paper.get('processing_consent') and paper.get('ai_disclosure')
            and paper.get('authors') and paper.get('purpose') in {'publication', 'review_only'},
            'Record actual author information, processing consent and disclosure')
    if test:
        require(metadata['producer'] == 'outreach-infrastructure-test'
                and metadata['work_id'] == 'test-runtime-20261031'
                and metadata['version'] == '1' and paper['purpose'] == 'review_only'
                and source['sha256'] == TEST_SOURCE_SHA
                and paper.get('make_public_after_acceptance') is False
                and paper.get('open_to_contributors') is False,
                'The acceptance entry only reuses the existing private synthetic manuscript')
    else:
        require(metadata['producer'] == 'trureturing-outreach', 'Invalid production producer')
    return value


def check_row(row, envelope):
    result = reviewed(row)
    require(envelope['task_id'] == row['id'] and envelope['result_hash'] == digest(result)
            and envelope['workflow_hash'] == row['request']['workflow_hash']
            and envelope['source'] in result['artifacts'],
            'The delivery package differs from the reviewed task')


def select(row, directory, decisions, secret):
    """Local operator selects an already reviewed source; no external delivery."""
    result = reviewed(row)
    manifest = Path(directory) / 'handoff.json'
    if (decisions.get('purpose') == 'publication'
            and (json.loads(manifest.read_text()).get('kind') == 'short_note'
                 or row['request']['inputs'].get('pipeline', {}).get('note_task_id'))):
        from .pre_reviewed import select as collection_select
        return collection_select(row, directory, decisions, secret)
    require(any(a['name'] == manifest.name and a['role'] == 'handoff'
                and a['bytes'] == manifest.stat().st_size
                and a['sha256'] == hashlib.sha256(manifest.read_bytes()).hexdigest()
                for a in result['artifacts']), 'The scientific handoff manifest was not independently reviewed')
    prepared = plan(directory, decisions)
    source = next((a for a in result['artifacts']
                   if a['name'] == prepared['source_file'] and a['sha256'] == prepared['source_sha256']), None)
    require(source is not None, 'The selected source is not an exact reviewed artifact')
    value = {'schema_version': 1, 'task_id': row['id'], 'result_hash': digest(result),
             'workflow_hash': row['request']['workflow_hash'], 'decision_ref': prepared['decision_ref'],
             'metadata': prepared['metadata'], 'source': source, 'reviewer': 'local-claude-loop'}
    value['signature'] = signature(value, secret)
    return validate(value, secret)


def ingest(tasks, envelope, secret, test=False):
    validate(envelope, secret, test)
    row = tasks.get(envelope['task_id'])
    check_row(row, envelope)
    note_revisions.gate(row, tasks, delivery_problem_ids(envelope['metadata']))
    previous = row['data'].get('mathesis_delivery')
    if previous:
        require(previous['envelope'] == envelope, 'An attempted delivery is immutable; use a new task/version')
        return row
    from .note_closure import source_gate
    source_gate(row['request'], tasks, row['data'].get('result'))
    require(row['state'] in {'approved', 'handoff-ready'}, 'Mathesis task is not ready for delivery selection')
    return tasks.update(row['id'], row['revision'], row['state'],
        {**row['data'], 'mathesis_delivery': {'phase': 'ready', 'envelope': envelope}})


def receipt_matches(receipt, envelope):
    metadata = envelope['metadata']
    require(receipt.get('schema') == 'mathesis.manuscript-receipt.v1'
            and isinstance(receipt.get('submission'), str) and receipt['submission']
            and type(receipt.get('version')) is int and receipt['version'] > 0
            and receipt.get('producer') == metadata['producer']
            and receipt.get('work_id') == metadata['work_id']
            and receipt.get('upstream_version') == metadata['version']
            and receipt.get('source_sha256') == envelope['source']['sha256'],
            'Mathesis did not acknowledge the exact delivered manuscript')
    if metadata['paper']['purpose'] == 'review_only':
        require(receipt.get('publication') is None, 'A private review unexpectedly has a publication')
    return receipt


def error_summary(error):
    """Keep only bounded problem codes/titles, never response detail or echoes."""
    summary = {'type': type(error).__name__}
    if isinstance(error, ValueError):
        message = ' '.join(str(error).split())[:200]
        if message:
            summary['reason'] = message
    if isinstance(error, HTTPError):
        summary['http_status'] = error.code
        try:
            problem = json.loads(error.read(16384))
            if isinstance(problem, dict):
                for key in ('code', 'title'):
                    if isinstance(problem.get(key), str):
                        summary[key] = ' '.join(problem[key].split())[:160]
        except (ValueError, OSError, AttributeError):
            pass
    return summary


def gate(row, tasks, root):
    from .control import workflow
    from .pipeline import source_gate
    from .automation import parent_gate, owner_gate
    from . import note_closure
    # Publication-loop selections deliver exact PASSed bytes under the standing
    # decision; later workflow edits do not reopen their review.
    require(row['data'].get('publication_loop') or matches_workflow(row, workflow('mathesis')),
            'Mathesis workflow changed; re-review')
    reviewed(row)
    note_revisions.gate(row, tasks)
    source_gate(row['request'], tasks, root)
    note_closure.source_gate(row['request'], tasks, row['data']['result'])
    parent_gate(row['request'], tasks)
    owner_gate(row['request'], root)
    if row['data'].get('publication_loop'):
        from .publication import standing, exclusion
        table, decision = standing(root)
        state = row['data']['mathesis_delivery']
        delivery = state.get('collection_envelope', state['envelope'])
        paper = delivery['metadata']['paper']
        metadata = delivery['metadata']
        note = {'id': metadata['work_id'], 'problem_ids': metadata.get('origin', {}).get('problem_ids',
                metadata.get('review', {}).get('record', {}).get('problem_ids', [])), 'creators': paper['authors']}
        require(exclusion(row, tasks, table, decision, note) is None,
                'The standing publication selection is now held or joint-excluded')


def _submission_status(value):
    """Extract Mathesis's stable submission status from either response shape."""
    if not isinstance(value, dict):
        return None
    for key in ('status', 'submission_status'):
        if isinstance(value.get(key), str):
            return value[key].lower().replace('-', '_')
    nested = value.get('submission')
    if isinstance(nested, dict):
        return _submission_status(nested)
    return None


def _legacy_migration_state(row):
    delivery = row['data'].get('mathesis_delivery', {})
    envelope = delivery.get('envelope', {})
    migration = delivery.get('legacy_migration', {})
    receipt = (migration.get('original_receipt') or delivery.get('receipt')
               or delivery.get('legacy_receipt') or {})
    if (row.get('kind') != 'mathesis'
            or envelope.get('metadata', {}).get('schema') != 'mathesis.manuscript-delivery.v1'
            or delivery.get('phase') not in {'delivered', 'pending'}
            or not receipt.get('submission')):
        return None
    return migration or {'phase': 'pending'}


def _migration_counts(rows):
    counts = {'pending': 0, 'withdrawn': 0, 'redelivered': 0, 'skipped': 0,
              'skipped_reasons': {}}
    for row in rows:
        migration = row['data'].get('mathesis_delivery', {}).get('legacy_migration')
        if not migration:
            continue
        phase = migration.get('phase', 'pending')
        if phase not in counts:
            phase = 'pending'
        counts[phase] += 1
        if phase == 'skipped':
            reason = migration.get('reason', 'unspecified')
            counts['skipped_reasons'][reason] = counts['skipped_reasons'].get(reason, 0) + 1
    return counts


def migrate_legacy_deliveries(ledger, config, root, folder, client, secret, limit=3):
    """Replay ordinary note submissions through collection before withdrawing them."""
    from . import pre_reviewed
    from .publication import download
    from .queue import GateError

    def save(row, delivery):
        current = ledger.tasks.get(row['id'])
        ledger.tasks.update(current['id'], current['revision'], current['state'],
                            {**current['data'], 'mathesis_delivery': delivery})
        ledger.save(force=True)

    def collection_receipt(metadata):
        return {'schema': 'mathesis.pre-reviewed-receipt.v1', 'status': 'collected',
                'work_id': metadata['work_id'], 'version': metadata['version'],
                'pdf_sha256': metadata['review']['pdf_sha256'],
                'source_sha256': metadata['review']['source_sha256']}

    candidates = []
    revisions = note_revisions.Revisions(ledger.tasks)
    for row in revisions.rows.values():
        migration = _legacy_migration_state(row)
        if (migration is not None and migration.get('phase') not in {'redelivered', 'skipped'}
                and not revisions.reason(row)):
            candidates.append(row)
    candidates.sort(key=lambda row: row['updated'])
    # A failed older row is the migration frontier. Retry failed rows, but do
    # not begin a fresh migration behind one until its error is resolved.
    blocked = False
    eligible = []
    for row in candidates:
        migration = row['data'].get('mathesis_delivery', {}).get('legacy_migration', {})
        if migration.get('last_error'):
            eligible.append(row)
            blocked = True
        elif not blocked:
            eligible.append(row)
    processed = 0
    for original in eligible[:max(0, int(limit))]:
        row = ledger.tasks.get(original['id'])
        delivery = copy.deepcopy(row['data']['mathesis_delivery'])
        migration = copy.deepcopy(delivery.get('legacy_migration', {'phase': 'pending'}))
        legacy_receipt = migration.get('original_receipt') or delivery.get('receipt')
        try:
            submission = legacy_receipt['submission']
            version = legacy_receipt.get('version')
            observed = client.status(submission, version)
            status = _submission_status(observed)
            if status in {'accepted', 'not_accepted'}:
                migration.update(phase='skipped', reason='submission is ' + status,
                                original_receipt=copy.deepcopy(legacy_receipt))
                delivery['legacy_migration'] = migration
                ledger.tasks.update(row['id'], row['revision'], row['state'],
                                    {**row['data'], 'mathesis_delivery': delivery})
                ledger.save(force=True); processed += 1; continue
            if status is None:
                migration['last_error'] = {'type': 'ValueError', 'reason': 'Mathesis status missing'}
                save(row, {**delivery, 'legacy_migration': migration})
                processed += 1; continue
            if status not in {'draft', 'in_review', 'withdrawn'}:
                migration.update(phase='pending', last_error={
                    'type': 'ValueError', 'reason': 'unexpected submission status: ' + status})
                save(row, {**delivery, 'legacy_migration': migration})
                processed += 1; continue

            # Build and validate the collection envelope while the ordinary
            # submission is still untouched. A GateError here is a safe skip.
            collection = delivery.get('collection_envelope')
            if collection is None:
                legacy = delivery['envelope']
                prior = [
                    r['data'].get('mathesis_delivery', {}).get('receipt', {}).get('version')
                    for r in ledger.tasks.all()
                    if r['id'] != row['id']
                    and r['data'].get('mathesis_delivery', {}).get('collection_envelope', {})
                    .get('metadata', {}).get('work_id') == legacy.get('metadata', {}).get('work_id')
                    and r['data'].get('mathesis_delivery', {}).get('legacy_migration', {}).get('phase') == 'redelivered']
                prior = max((value for value in prior if isinstance(value, int)), default=None)
                try:
                    collection = pre_reviewed.legacy_collection_envelope(row, legacy, secret, prior)
                    gate(row, ledger.tasks, root)
                except GateError as error:
                    migration.update(phase='skipped', reason=' '.join(str(error).split())[:200])
                    save(row, {**delivery, 'legacy_migration': migration})
                    processed += 1; continue
                migration['original_receipt'] = copy.deepcopy(legacy_receipt)
                delivery['collection_envelope'] = collection
                delivery.pop('receipt', None)
                delivery['legacy_migration'] = migration
                save(row, delivery)
            row = ledger.tasks.get(row['id'])
            delivery = copy.deepcopy(row['data']['mathesis_delivery'])
            migration = copy.deepcopy(delivery['legacy_migration'])
            metadata = collection['metadata']
            from .publication import standing, exclusion
            table, decision = standing(root)
            note = {'id': metadata['work_id'],
                    'problem_ids': metadata['review']['record'].get('problem_ids', []),
                    'creators': metadata['paper']['authors']}
            try:
                require(exclusion(row, ledger.tasks, table, decision, note) is None,
                        'Publication is held or joint-excluded')
            except GateError as error:
                migration.update(phase='skipped', reason=' '.join(str(error).split())[:200])
                save(row, {**delivery, 'legacy_migration': migration})
                processed += 1; continue

            # A redelivery intent is always reconciled with a public GET before
            # another POST. This handles both a lost response and a 503 after
            # Mathesis accepted the multipart request.
            receipt = migration.get('redelivery_receipt')
            collected = None
            if receipt is None and migration.get('phase') == 'redelivery-intent':
                try:
                    collected = pre_reviewed.readback(client, metadata)
                    receipt = collection_receipt(metadata)
                except Exception as read_error:
                    if not (isinstance(read_error, HTTPError) and read_error.code == 404):
                        # A transient file/readback failure remains retryable and
                        # must retain the persisted redelivery intent.
                        migration['last_error'] = error_summary(read_error)
                        save(row, {**delivery, 'legacy_migration': migration})
                        processed += 1; continue
            if receipt is None:
                with tempfile.TemporaryDirectory(prefix='mathesis-migration-', dir=folder) as temp:
                    pdf = download(row, collection['pdf']['name'], temp, ledger.releases)
                    source = download(row, collection['source']['name'], temp, ledger.releases)
                    migration.update(phase='redelivery-intent', request_hash=digest(collection))
                    delivery['request_hash'] = migration['request_hash']
                    delivery['legacy_migration'] = migration
                    save(row, delivery)
                    try:
                        receipt = pre_reviewed.collection_receipt(
                            client.deliver_pre_reviewed(metadata, pdf, source), metadata)
                    except HTTPError as error:
                        if error.code != 409:
                            raise
                        # An immutable same-version conflict is safe only if
                        # the public copy proves the exact selected metadata
                        # and both file hashes.
                        collected = pre_reviewed.readback(client, metadata)
                        receipt = collection_receipt(metadata)
            if collected is None:
                collected = pre_reviewed.readback(client, metadata)
            migration.update(phase='collection-confirmed', redelivery_receipt=copy.deepcopy(receipt),
                             collected_paper=copy.deepcopy(collected))
            delivery.update(legacy_migration=migration, receipt=copy.deepcopy(receipt))
            save(row, delivery)

            # Withdrawal is deliberately after the collection GET. Rows which
            # were already withdrawn take this idempotent path without POSTing.
            if status in {'draft', 'in_review'}:
                migration['phase'] = 'withdraw-intent'
                migration.pop('last_error', None)
                delivery['legacy_migration'] = migration
                save(row, delivery)
                try:
                    migration['withdrawal_response'] = copy.deepcopy(client.withdraw(submission))
                    observed = client.status(submission, version)
                    status = _submission_status(observed)
                except Exception as error:
                    migration['last_error'] = error_summary(error)
                    save(row, {**delivery, 'legacy_migration': migration})
                    processed += 1; continue
                if status != 'withdrawn':
                    raise ValueError('withdrawal not confirmed; submission remains ' + str(status))
                migration['withdrawal_status'] = copy.deepcopy(observed)
            migration.update(phase='redelivered', collected_paper=copy.deepcopy(collected))
            migration.pop('last_error', None)
            delivery.update(phase='delivered', collected_paper=copy.deepcopy(collected),
                            legacy_receipt=copy.deepcopy(legacy_receipt), legacy_migration=migration,
                            receipt=copy.deepcopy(receipt))
            row = ledger.tasks.get(row['id'])
            ledger.tasks.update(row['id'], row['revision'], 'verified',
                                {**row['data'], 'mathesis_delivery': delivery})
            ledger.save(force=True); processed += 1
        except Exception as error:
            row = ledger.tasks.get(original['id'])
            delivery = copy.deepcopy(row['data']['mathesis_delivery'])
            migration = copy.deepcopy(delivery.get('legacy_migration', {'phase': 'pending'}))
            migration['last_error'] = error_summary(error)
            if migration.get('phase') not in {'withdraw-intent', 'redelivery-intent', 'collection-confirmed'}:
                migration['phase'] = 'pending'
            delivery['legacy_migration'] = migration
            ledger.tasks.update(row['id'], row['revision'], row['state'],
                                {**row['data'], 'mathesis_delivery': delivery})
            ledger.save(force=True); processed += 1
    rows = [r for r in ledger.tasks.all() if _legacy_migration_state(r) is not None]
    summary = _migration_counts(rows); summary['processed_this_tick'] = processed
    ledger.tasks.db.execute('INSERT OR REPLACE INTO pipeline_state VALUES(?,?)',
                            ('mathesis-legacy-migration', canonical(summary)))
    ledger.save(force=True)
    return summary


def advance(ledger, config, root, folder, client=None, secret=None, note_gh=None):
    """Persist intent, deliver, GET the receipt, then finish. Never await review."""
    settings = config.get('mathesis', {})
    test = settings.get('test_only') is True
    require(not test or getattr(ledger.releases, 'tag', None) == TEST_TAG,
            'Synthetic acceptance cannot operate on the production ledger')
    active = not config['paused'] and config['kinds'].get('mathesis') and settings.get('mode') == 'deliver'
    secret = secret or os.environ.get('OUTREACH_REVIEW_KEY')
    identity_verified = False
    all_rows = ledger.tasks.all()
    revisions = note_revisions.Revisions(ledger.tasks, all_rows)
    legacy_candidates = [r for r in all_rows if _legacy_migration_state(r) is not None
                         and not revisions.reason(r)]
    if active and legacy_candidates and not test:
        if client is None:
            token = os.environ.get('MATHESIS_ACCESS_TOKEN') or os.environ.get('CMA_ACCESS_TOKEN')
            require(token, 'Mathesis submitting identity is unavailable')
            client = Mathesis(token)
        client.verify_owner()
        identity_verified = True
        migrate_legacy_deliveries(ledger, config, root, folder, client, secret,
                                  limit=settings.get('legacy_migration_per_tick', 3))
        all_rows = ledger.tasks.all()
    def needs_delivery(row):
        value = row['data'].get('mathesis_delivery', {})
        if revisions.reason(row) and value.get('phase') not in {'pending', 'conflict'}:
            return False
        migration = value.get('legacy_migration')
        if value.get('collection_envelope') and migration and migration.get('phase') != 'redelivered':
            # The migration owns the saved collection request until it finishes.
            return False
        if value.get('phase') in {'ready', 'pending', 'conflict'}:
            return True
        envelope = value.get('collection_envelope', value.get('envelope', {}))
        return (active and value.get('phase') == 'delivered'
                and envelope.get('metadata', {}).get('schema') == 'mathesis.pre-reviewed-delivery.v1'
                and value.get('public_link', {}).get('phase') != 'queued')
    rows = [r for r in all_rows if r['kind'] == 'mathesis' and needs_delivery(r)]
    correspondence = linked_contacts(all_rows, root, config) if rows else {}
    rows.sort(key=lambda r: (r['id'] not in correspondence, r['updated']))
    batch = rows[:4]
    if correspondence:
        tick = getattr(ledger, 'tick', None)
        deferred = {key: i for i, key in enumerate(tick.previous.get('mathesis', []))} if tick else {}
        batch.sort(key=lambda r: (r['id'] not in correspondence, deferred.get(r['id'], len(deferred))))
    for original in units(ledger, 'mathesis', batch, preserve_order=bool(correspondence)):
        row = ledger.tasks.get(original['id'])
        delivery = copy.deepcopy(row['data']['mathesis_delivery'])
        envelope = delivery.get('collection_envelope', delivery['envelope'])
        if delivery['phase'] == 'ready' and (not active or row['state'] != 'handoff-ready'):
            continue
        try:
            validate(envelope, secret, test)
            from . import pre_reviewed
            collection = envelope['metadata']['schema'] == pre_reviewed.SCHEMA
            own_note = (pre_reviewed.is_shortnote(row) and
                        envelope['metadata'].get('paper', {}).get('purpose', 'publication') == 'publication')
            if own_note and not collection and not test and not delivery.get('request_hash') and not delivery.get('receipt'):
                # Upgrade only never-attempted shortnote selections. Existing
                # manuscript receipts/uncertain attempts retain their handles.
                pre_reviewed.upgrade(ledger, row, root, folder, secret)
                row = ledger.tasks.get(row['id'])
                delivery = copy.deepcopy(row['data']['mathesis_delivery'])
                collection = True
            if client is None:
                token = os.environ.get('MATHESIS_ACCESS_TOKEN') or os.environ.get('CMA_ACCESS_TOKEN')
                require(token, 'Mathesis submitting identity is unavailable')
                client = Mathesis(token)
            if not identity_verified:
                client.verify_owner()
                identity_verified = True
            if collection:
                pre_reviewed.advance(ledger, row, config, root, folder, client, secret, note_gh)
                continue
            metadata = envelope['metadata']
            coordinate = 'mathesis:' + digest([metadata[k] for k in ('producer', 'work_id', 'version')]
                + [metadata['paper']['purpose']])
            request_hash = digest({'metadata': metadata, 'filename': envelope['source']['name'],
                                   'source': envelope['source']['sha256']})
            old = ledger.tasks.db.execute('SELECT payload FROM pipeline_state WHERE key=?', (coordinate,)).fetchone()
            reserved = json.loads(old[0]) if old else None
            require(not reserved or reserved['request_hash'] == request_hash,
                    'Mathesis manuscript/version already has a different delivery')
            if reserved and reserved.get('revision'):
                require(not delivery.get('revision') or delivery['revision'] == reserved['revision'],
                        'Keep the checkpointed Mathesis delivery version')
                delivery['revision'] = reserved['revision']
            # The signed selection stays immutable. Only transport version pins
            # may change; all scientific metadata and source remain exact.
            metadata = {**metadata, **delivery.get('revision', {})}
            expected = {**envelope, 'metadata': metadata}
            observed = delivery.get('receipt') or (reserved.get('receipt') if reserved else None)
            if delivery['phase'] in {'pending', 'conflict'} and not observed:
                # A lost POST response is reconciled by the original producer/work.
                try:
                    found = client.find(metadata)
                except HTTPError as error:
                    if error.code != 404:
                        raise
                    found = None
                if delivery['phase'] == 'conflict':
                    # find is scoped by account/producer/work/purpose and returns
                    # one CURRENT receipt, not a history. Never adopt foreign work.
                    if not (found and found.get('schema') == 'mathesis.manuscript-receipt.v1'
                            and found.get('producer') == metadata['producer']
                            and found.get('work_id') == metadata['work_id']
                            and found.get('current') is True
                            and type(found.get('version')) is int and found['version'] > 0
                            and found.get('current_version') == found['version']
                            and isinstance(found.get('upstream_version'), str) and found['upstream_version']):
                        continue
                    if found.get('source_sha256') == envelope['source']['sha256']:
                        delivery['revision'] = {'version': found['upstream_version']}
                        metadata = {**envelope['metadata'], **delivery['revision']}
                        expected = {**envelope, 'metadata': metadata}
                        observed = receipt_matches(found, expected)
                    elif not own_note and not delivery.get('revision') and active and row['state'] == 'handoff-ready':
                        delivery['revision'] = {
                            'version': f"outreach-{request_hash[:24]}-{found['version'] + 1}",
                            'previous_version': found['version']}
                        metadata = {**metadata, **delivery['revision']}
                        expected = {**envelope, 'metadata': metadata}
                    else:
                        continue  # No version churn on unrelated/continuing 409s.
                elif (found and found.get('upstream_version') == metadata['version']
                      and found.get('source_sha256') == envelope['source']['sha256']):
                    observed = receipt_matches(found, expected)
            reservation = {'request_hash': request_hash, 'task_id': row['id']}
            if delivery.get('revision'):
                reservation['revision'] = delivery['revision']
            if not observed:
                if not active or row['state'] != 'handoff-ready' or own_note:
                    continue
                gate(row, ledger.tasks, root)
                check_row(row, envelope)
                source = envelope['source']
                with tempfile.TemporaryDirectory(prefix='mathesis-source-', dir=folder) as temp:
                    ledger.releases.run('release', 'download', source['release_tag'], '--repo', ledger.releases.repo,
                                        '--pattern', source['name'], '--dir', temp)
                    path = Path(temp) / source['name']
                    require(path.stat().st_size == source['bytes']
                            and hashlib.sha256(path.read_bytes()).hexdigest() == source['sha256'],
                            'Mathesis source readback differs from the reviewed artifact')
                    with unit(ledger,'mathesis',row['id']) as ready:
                        if not ready:continue
                        delivery.update(phase='pending', request_hash=request_hash)
                        row = ledger.tasks.update(row['id'], row['revision'], row['state'],
                            {**row['data'], 'mathesis_delivery': delivery})
                        ledger.tasks.db.execute('INSERT OR REPLACE INTO pipeline_state VALUES(?,?)',
                            (coordinate, canonical(reservation)))
                        ledger.save(force=True)  # Intent/reservation must be remote before POST.
                        observed = receipt_matches(client.deliver(metadata, path), expected)
            # Keep the original handle even when the following GET times out or
            # another author later advances the manuscript's current version.
            delivery.update(phase='pending', receipt=receipt_matches(observed, expected))
            row = ledger.tasks.get(row['id'])
            row = ledger.tasks.update(row['id'], row['revision'], row['state'],
                {**row['data'], 'mathesis_delivery': delivery})
            ledger.tasks.db.execute('INSERT OR REPLACE INTO pipeline_state VALUES(?,?)',
                (coordinate, canonical({**reservation, 'receipt': observed})))
            # The work's current version can advance before our next tick.
            # Preserve this exact receipt handle before attempting its GET.
            ledger.save(force=True)
            persisted = receipt_matches(client.status(observed['submission'], observed['version']), expected)
            require(persisted['submission'] == observed['submission'] and persisted['version'] == observed['version'],
                    'Mathesis receipt changed its submission identity')
            delivery.update(phase='delivered', receipt=persisted)
            delivery.pop('last_error', None)
            row = ledger.tasks.get(row['id'])
            ledger.tasks.update(row['id'], row['revision'], 'verified' if row['state'] == 'handoff-ready' else row['state'],
                {**row['data'], 'mathesis_delivery': delivery})
            ledger.tasks.db.execute('INSERT OR REPLACE INTO pipeline_state VALUES(?,?)',
                (coordinate, canonical({**reservation, 'receipt': persisted})))
            ledger.save()
        except Exception as error:
            # Preserve the original envelope and only safe HTTP problem fields.
            row = ledger.tasks.get(original['id'])
            delivery = copy.deepcopy(row['data']['mathesis_delivery'])
            delivery['last_error'] = error_summary(error)
            if isinstance(error, HTTPError) and error.code == 409:
                delivery['phase'] = 'conflict'
            ledger.tasks.update(row['id'], row['revision'], row['state'],
                {**row['data'], 'mathesis_delivery': delivery})
            ledger.save()
        finally:
            # Unresolved conflicts must rotate through the four-row window too.
            latest = ledger.tasks.get(original['id'])
            if (latest['revision'] == original['revision']
                    and latest['data']['mathesis_delivery']['phase'] == 'conflict'):
                ledger.tasks.update(latest['id'], latest['revision'], latest['state'], latest['data'])
                ledger.save()
