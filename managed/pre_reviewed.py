"""Exact upstream PASS notes use Mathesis collection, never venue review."""
import copy
import hashlib
import hmac
import json
from pathlib import Path
import re
import tempfile

from .location import settings
from . import recording
from .mathesis import collection_receipt
from .queue import canonical, digest, require
from .tick import unit

SCHEMA = 'mathesis.pre-reviewed-delivery.v1'
PRIOR_WORK_NOT_RUN = {
    'verdict': 'not-run',
    'reason': 'reviewed before the 2026-11-04 Pro-check policy',
}


def is_shortnote(row):
    inputs = row['request']['inputs']
    return (row['kind'] == 'shortnote' or bool(inputs.get('pipeline', {}).get('note_task_id'))
            or bool(row['data'].get('publication_loop'))
            or row['data'].get('mathesis_delivery', {}).get('envelope', {}).get('metadata', {}).get('schema') == SCHEMA)


def prior_section_text(text):
    match = re.search(r'^(#{1,6}) +Prior[- ]work check[^\n]*\n', text, re.I | re.M)
    if not match:
        return ''
    return re.split(r'^#{1,' + str(len(match[1])) + r'} +', text[match.end():], maxsplit=1, flags=re.M)[0]


def prior_section(text):
    """Accept a verdict only inside the named evidence section, not a mention."""
    section = prior_section_text(text)
    verdict = re.search(r'\bverdict[*`_ ]*[:=]\s*[*`_ ]*(first-known|prior-exists|unclear)\b', section, re.I)
    if not verdict:
        return None
    # Preserve the response locator when present; the exact PASS/evidence binds
    # the prior-work consultation, not an inferred new Oracle approval.
    ref = re.search(r'https://chatgpt\.com/c/[A-Za-z0-9-]+', section)
    if not ref:
        ref = re.search(r'(?:response ref(?:erence)?|Oracle (?:response|task)(?: ID)?)\s*[:=]\s*`?([^\n`]+)', section, re.I)
    oracle = bool(re.search(r'\bOracle\b', section, re.I) and
                  re.search(r'\bPro\b|chatgpt-?6-?pro', section, re.I) and
                  not re.search(r'\bno Oracle\b|Oracle[^\n.]{0,50}\bnot (?:run|called|performed|obtained)\b',
                                section, re.I))
    return {'verdict': verdict[1].lower(), 'oracle_pro_check': oracle,
            **({'oracle_response_ref': ref[0] if ref[0].startswith('https://') else ref[1].strip()} if ref else {})}


def evidence(row, tasks, directory, releases):
    from .publication import download, lineage, reviewed
    parents = lineage(row, tasks) if tasks is not None else [row]
    # Never borrow a prior-work verdict from a superseded revision. A handoff
    # may reuse its nearest exact PASS source note's evidence.
    candidates = [row]
    parent = next((r for r in parents[1:] if r['kind'] == 'shortnote'), None)
    if parent is not None:
        require(reviewed(parent), 'Prior-work source no longer has its exact PASS')
        candidates.append(parent)
    for candidate in candidates:
        for artifact in candidate['data']['result']['artifacts']:
            if artifact['role'] != 'evidence' or not artifact['name'].endswith(('.md', '.txt')):
                continue
            target = Path(directory) / candidate['id']
            target.mkdir(exist_ok=True)
            if releases is not None:
                path = download(candidate, artifact['name'], target, releases)
            else:
                path = Path(directory) / artifact['name']
                require(path.stat().st_size == artifact['bytes']
                        and hashlib.sha256(path.read_bytes()).hexdigest() == artifact['sha256'],
                        'Prior-work evidence differs from the exact PASS')
            check = prior_section(path.read_text())
            if check:
                return {**check, 'task_id': candidate['id'], 'result_hash': digest(candidate['data']['result']),
                        'evidence': artifact}
    return None


def prior_ready(check):
    return bool(check and check.get('oracle_pro_check') is True
                and check.get('verdict') in {'first-known', 'prior-exists'})


def prior_work_record(check):
    """Return the immutable record stored in a collection review.

    Prior-work evidence is useful provenance when it exists, but it is no
    longer a delivery precondition.  A missing section is represented
    explicitly so old PASSed notes remain auditable without inventing a check.
    """
    return copy.deepcopy(check) if check is not None else copy.deepcopy(PRIOR_WORK_NOT_RUN)


def legacy_collection_envelope(row, legacy, secret, previous_version=None):
    """Build a collection selection from an exact ordinary delivery.

    This is used only by the bounded withdrawal migration.  It reads the PDF
    and source archive descriptors already bound to the reviewed result and
    never repacks or regenerates either file.
    """
    from .mathesis_delivery import reviewed, signature, validate
    result = reviewed(row)
    metadata = legacy.get('metadata', {})
    paper = metadata.get('paper', {})
    origin = metadata.get('origin', {})
    work_id = metadata.get('work_id')
    version = metadata.get('version')
    require(isinstance(work_id, str) and re.fullmatch(r'[a-z0-9-]{1,160}', work_id),
            'Legacy Mathesis work ID cannot enter the collection')
    try:
        version = int(version)
    except (TypeError, ValueError):
        raise ValueError('Legacy Mathesis version is not a collection revision')
    require(version > 0, 'Legacy Mathesis version is not a collection revision')
    authors = [{k: value for k, value in author.items() if k in {'name', 'affiliation', 'orcid'}}
               for author in paper.get('authors', [])]
    abstract = paper.get('abstract') or paper.get('abstract_text') or metadata.get('abstract')
    license_value = paper.get('license') or origin.get('license')
    require(paper.get('title') and abstract and authors and license_value
            and license_value.lower() == 'cc-by-4.0',
            'Legacy Mathesis package lacks collection metadata or CC-BY-4.0')
    pdfs = [a for a in result.get('artifacts', [])
            if a.get('role') == 'note-pdf' and str(a.get('name', '')).endswith('.pdf')]
    sources = [a for a in result.get('artifacts', [])
               if a.get('role') == 'source-archive' and str(a.get('name', '')).endswith('.zip')]
    # Prepared note artifacts use the public-link convention note-vN.pdf and
    # source-vN.zip.  A legacy result can contain archives from more than one
    # revision, so choose the pair matching the migrated collection revision.
    canonical_pdf = next((a for a in pdfs if a.get('name') == f'note-v{version}.pdf'), None)
    canonical_source = next((a for a in sources if a.get('name') == f'source-v{version}.zip'), None)
    if canonical_pdf is not None and canonical_source is not None:
        pdf, source = canonical_pdf, canonical_source
    else:
        require(len(pdfs) == 1 and len(sources) == 1,
                'Legacy Mathesis package lacks one exact reviewed PDF/source pair')
        pdf, source = pdfs[0], sources[0]
    prior = row['data'].get('mathesis_prior_work', {}).get('check')
    if prior is None:
        prior = ((metadata.get('review') or {}).get('record') or {}).get('prior_work_check')
    record = {'task_id': row['id'], 'result_hash': digest(result),
              'problem_ids': list(origin.get('problem_ids', [])),
              'signed_pass': copy.deepcopy(row['data']['review']),
              'prior_work_check': prior_work_record(prior),
              'publication_authorization': {
                  'decision_ref': legacy.get('decision_ref') or 'legacy-manuscript-delivery-migration',
                  'direct_display_decision_ref': settings('identity')['direct_display_decision_ref']}}
    collection_metadata = {
        'schema': SCHEMA, 'producer': 'trureturing-outreach', 'work_id': work_id,
        'version': version, 'previous_version': previous_version,
        'authorize_publication': True,
        'paper': {'title': paper['title'], 'abstract': abstract, 'authors': authors,
                  'license': 'CC-BY-4.0'},
        'review': {'verdict': 'PASS', 'pdf_sha256': pdf['sha256'],
                   'source_sha256': source['sha256'], 'record': record}}
    evidence_url = origin.get('source_url') or metadata.get('evidence_url', '')
    lean = re.fullmatch(r'https://github\.com/[^/]+/[^/]+/blob/([0-9a-fA-F]{40})/[^?#]+\.lean(?:#.*)?',
                        evidence_url)
    if lean:
        collection_metadata['paper']['lean'] = {'url': evidence_url, 'revision': lean[1]}
    envelope = {'schema_version': 1, 'task_id': row['id'], 'result_hash': digest(result),
                'workflow_hash': row['request']['workflow_hash'],
                'decision_ref': legacy.get('decision_ref') or 'legacy-manuscript-delivery-migration',
                'metadata': collection_metadata, 'pdf': pdf, 'source': source,
                'reviewer': 'local-claude-loop'}
    envelope['signature'] = signature(envelope, secret)
    return validate(envelope, secret)


def cache_prior_work(ledger, row, directory, releases):
    check = evidence(row, ledger.tasks, directory, releases)
    status = {'result_hash': digest(row['data']['result']), 'check': check,
              'phase': 'ready' if check is not None else 'not-run'}
    current = ledger.tasks.get(row['id'])
    updated = ledger.tasks.update(row['id'], current['revision'], current['state'],
        {**current['data'], 'mathesis_prior_work': status})
    ledger.save()
    return updated


def select(row, directory, decisions, secret, tasks=None, releases=None, metadata=None, prior_work=None):
    from .mathesis_delivery import reviewed, signature, validate
    reviewed(row)
    if metadata is None:
        manifest_path = Path(directory) / 'handoff.json'
        raw = manifest_path.read_bytes()
        require(any(a['name'] == 'handoff.json' and a['role'] == 'handoff'
                    and a['bytes'] == len(raw) and a['sha256'] == hashlib.sha256(raw).hexdigest()
                    for a in row['data']['result']['artifacts']), 'The handoff manifest differs from the exact PASS')
        manifest = json.loads(raw)
        require(manifest.get('private_correspondence_included') is False, 'Scientific-only handoff required')
        metadata = manifest['source_note']
    check = prior_work if prior_work is not None else evidence(row, tasks, directory, releases)
    check = prior_work_record(check)
    require(decisions.get('purpose') == 'publication' and decisions.get('decision_ref')
            and decisions.get('make_public_after_acceptance') is True,
            'Pre-reviewed collection requires explicit publication authorization')
    require(metadata['license'].lower() == 'cc-by-4.0', 'Pre-reviewed notes require CC-BY-4.0')
    artifacts = row['data']['result']['artifacts']
    pdfs = [a for a in artifacts if a['role'] == 'note-pdf' and a['name'].endswith('.pdf')]
    sources = [a for a in artifacts if a['role'] == 'source-archive' and a['name'] == decisions['source_file']
               and a['name'].endswith('.zip')]
    require(len(pdfs) == len(sources) == 1, 'Select one exact reviewed PDF and source ZIP')
    pdf, source = pdfs[0], sources[0]
    paper = {'title': metadata['title'], 'abstract': metadata['abstract'],
             'authors': [{k: v for k, v in a.items() if k in {'name', 'affiliation', 'orcid'}}
                         for a in metadata['creators']], 'license': 'CC-BY-4.0'}
    url = metadata.get('evidence_url', '')
    lean = re.fullmatch(r'https://github\.com/[^/]+/[^/]+/blob/([0-9a-fA-F]{40})/[^?#]+\.lean(?:#.*)?', url)
    if lean:
        paper['lean'] = {'url': url, 'revision': lean[1]}
    record = {'task_id': row['id'], 'result_hash': digest(row['data']['result']),
              'problem_ids': metadata['problem_ids'],
              'signed_pass': copy.deepcopy(row['data']['review']), 'prior_work_check': check,
              'publication_authorization': {'decision_ref': decisions['decision_ref'],
                                             'direct_display_decision_ref': settings('identity')['direct_display_decision_ref']}}
    review = {'verdict': 'PASS', 'pdf_sha256': pdf['sha256'], 'source_sha256': source['sha256'], 'record': record}
    envelope = {'schema_version': 1, 'task_id': row['id'], 'result_hash': digest(row['data']['result']),
                'workflow_hash': row['request']['workflow_hash'], 'decision_ref': decisions['decision_ref'],
                'metadata': {'schema': SCHEMA, 'producer': 'trureturing-outreach', 'work_id': metadata['id'],
                             'version': decisions.get('collection_version', metadata['revision']),
                             'previous_version': decisions.get('collection_previous_version', decisions.get('previous_version')),
                             'authorize_publication': True, 'paper': paper, 'review': review},
                'pdf': pdf, 'source': source, 'reviewer': 'local-claude-loop'}
    envelope['signature'] = signature(envelope, secret)
    return validate(envelope, secret)


def validate_metadata(value):
    require(set(value) == {'schema', 'producer', 'work_id', 'version', 'previous_version',
                           'authorize_publication', 'paper', 'review'}
            and value['schema'] == SCHEMA and value['authorize_publication'] is True,
            'Invalid pre-reviewed publication envelope')
    require(all(isinstance(value[k], str) and re.fullmatch(r'[a-z0-9-]{1,160}', value[k])
                for k in ('producer', 'work_id')), 'Invalid collection slug')
    previous, version = value['previous_version'], value['version']
    require(type(version) is int and 0 < version <= 2**32 - 1
            and (previous is None or type(previous) is int and 0 < previous < version),
            'Collection version must advance its predecessor')
    paper, review = value['paper'], value['review']
    require(paper.get('title') and paper.get('abstract') and paper.get('authors')
            and paper.get('license') == 'CC-BY-4.0'
            and all(a.get('name') and not a.get('person') for a in paper['authors']),
            'Supply ordered collection authors, title, abstract and license')
    require(review.get('verdict') == 'PASS' and isinstance(review.get('record'), dict) and review['record']
            and all(re.fullmatch(r'[0-9a-f]{64}', review.get(k, '')) for k in ('pdf_sha256', 'source_sha256')),
            'Collection requires an exact file-bound PASS')
    require(len(canonical(value).encode()) <= 128 * 1024, 'Collection metadata exceeds 128 KiB')


def check_row(row, envelope, secret):
    from .mathesis_delivery import reviewed
    result = reviewed(row)
    require(envelope['pdf'] in result['artifacts'] and envelope['source'] in result['artifacts'],
            'Collection files differ from the exact PASS')
    record = envelope['metadata']['review']['record']
    review = record['signed_pass']
    unsigned = {k: v for k, v in review.items() if k != 'signature'}
    require(review == row['data']['review'] and hmac.compare_digest(review.get('signature', ''),
            hmac.new(secret.encode(), canonical(unsigned).encode(), hashlib.sha256).hexdigest()),
            'Collection requires the authentic signed PASS for this exact result')
    require(review.get('reviewer') == 'local-claude-loop', 'Collection requires independent Claude review')
    require(isinstance(record.get('prior_work_check'), dict)
            and isinstance(record['prior_work_check'].get('verdict'), str),
            'Collection requires a Prior-work record')


def readback(client, metadata):
    """Public JSON omits hashes; verify the version and both published bytes."""
    from .publication import notes

    work, version = metadata['work_id'], metadata['version']
    paper = client.collected(work, version)
    require(paper.get('id') == work and paper.get('revision') == str(version)
            and paper.get('previous_version') == (str(metadata['previous_version'])
                if metadata['previous_version'] is not None else None), 'Collected paper version differs')
    expected = metadata['paper']
    require(all(paper.get(key) == value for key, value in expected.items() if key != 'authors')
            and notes.w.same_collection_authors(expected['authors'], paper.get('authors')),
            'Collected bibliographic metadata differs from the selected version')
    for kind in ('pdf', 'source'):
        expected_url = f'/api/v1/collected-papers/{work}/files/{kind}?version={version}'
        require(paper.get(kind, {}).get('url') == expected_url, 'Collected file must pin the exact version')
        require(hashlib.sha256(client.collected_file(work, version, kind)).hexdigest()
                == metadata['review'][kind + '_sha256'], 'Collected file hash differs from the exact PASS')
    return paper


def queue_public_link(envelope, receipt, root, gh=None):
    """Only enqueue immutable input; the existing note worker owns Release state."""
    from .publication import notes
    source = re.fullmatch(r'source-v([1-9]\d*)\.zip', envelope['source']['name'])
    require(source and envelope['pdf']['name'] == f'note-v{source[1]}.pdf',
            'Select the canonical reviewed note revision for its public reading link')
    metadata = envelope['metadata']
    request = {'schema_version': 'short-note-request.v1', 'id': metadata['work_id'],
               'action': 'link-mathesis', 'revision': int(source[1]), 'receipt': receipt}
    catalog = notes.w.read(Path(root) / 'tables/publications.json')
    gh = recording.adapter('publication', gh or notes.w.GitHub(notes.w.OUTREACH))
    queued = notes.enqueue(catalog, request, gh)
    with tempfile.TemporaryDirectory() as temp:
        require(notes.w.read(gh.download(notes.w.tag_for(request['id']), queued['request'], temp)) == request,
                'Mathesis reading-link request readback differs')
    return {'phase': 'queued', 'request': queued['request'], 'revision': request['revision']}


def advance(ledger, row, config, root, folder, client, secret, note_gh=None):
    from .mathesis_delivery import gate, validate, check_row as check_selection
    from .publication import download
    from .note_revisions import Revisions
    delivery = copy.deepcopy(row['data']['mathesis_delivery'])
    envelope = delivery.get('collection_envelope', delivery['envelope'])
    validate(envelope, secret)
    check_selection(row, envelope)
    check_row(row, envelope, secret)
    metadata = envelope['metadata']
    active = not config['paused'] and config['kinds'].get('mathesis') and config['mathesis'].get('mode') == 'deliver'
    coordinate = 'mathesis-collection:' + digest([metadata['producer'], metadata['work_id'], metadata['version']])
    # Release tag/task IDs are provenance, not public request identity. Filename,
    # metadata and both hashes are immutable under a collected version.
    binding = digest({'metadata': metadata, 'files': {k: {field: envelope[k][field] for field in ('name', 'sha256')}
                                                    for k in ('pdf', 'source')}})
    saved = ledger.tasks.db.execute('SELECT payload FROM pipeline_state WHERE key=?', (coordinate,)).fetchone()
    reserved = json.loads(saved[0]) if saved else {}
    require(not reserved or reserved['request_hash'] == binding, 'Collection version already has another selection')
    receipt = delivery.get('receipt') or reserved.get('receipt')
    if not receipt:
        if not active or row['state'] != 'handoff-ready':
            return
        gate(row, ledger.tasks, root)
        from .publication import standing, exclusion
        table, decision = standing(root)
        note = {'id': metadata['work_id'], 'problem_ids': metadata['review']['record']['problem_ids'],
                'creators': metadata['paper']['authors']}
        require(exclusion(row, ledger.tasks, table, decision, note) is None,
                'Publication is held or joint-excluded')
        with tempfile.TemporaryDirectory(prefix='mathesis-collection-', dir=folder) as temp:
            pdf = download(row, envelope['pdf']['name'], temp, ledger.releases)
            source = download(row, envelope['source']['name'], temp, ledger.releases)
            with unit(ledger, 'mathesis', row['id']) as ready:
                if not ready:
                    return
                delivery.update(phase='pending', request_hash=binding)
                ledger.tasks.update(row['id'], row['revision'], row['state'], {**row['data'], 'mathesis_delivery': delivery})
                ledger.tasks.db.execute('INSERT OR REPLACE INTO pipeline_state VALUES(?,?)',
                                       (coordinate, canonical({'request_hash': binding, 'task_id': row['id']})))
                ledger.save(force=True)
                receipt = collection_receipt(client.deliver_pre_reviewed(metadata, pdf, source), metadata)
    receipt = collection_receipt(receipt, metadata)
    delivery.update(phase='pending', receipt=receipt)
    current = ledger.tasks.get(row['id'])
    ledger.tasks.update(row['id'], current['revision'], current['state'], {**current['data'], 'mathesis_delivery': delivery})
    ledger.tasks.db.execute('INSERT OR REPLACE INTO pipeline_state VALUES(?,?)',
                           (coordinate, canonical({'request_hash': binding, 'task_id': row['id'], 'receipt': receipt})))
    ledger.save(force=True)
    observed = readback(client, metadata)
    if 'public_path' not in receipt and delivery.get('legacy_migration', {}).get('phase') == 'redelivered':
        # Older migrations reconstructed the receipt from the verified public files.
        receipt = {**receipt, 'public_path': f"/papers/{metadata['work_id']}?version={metadata['version']}"}
    sent_revision_hold = Revisions(ledger.tasks).reason(row)
    if active and not sent_revision_hold and delivery.get('public_link', {}).get('phase') != 'queued':
        delivery['public_link'] = queue_public_link(envelope, receipt, root, note_gh)
    elif delivery.get('public_link', {}).get('phase') != 'queued':
        delivery['public_link'] = {'phase': 'pending', 'reason': sent_revision_hold or
                                   'Resume to queue the public reading link'}
    delivery.update(phase='delivered', receipt=receipt, collected_paper=observed)
    delivery.pop('last_error', None)
    current = ledger.tasks.get(row['id'])
    ledger.tasks.update(row['id'], current['revision'], 'verified', {**current['data'], 'mathesis_delivery': delivery})
    ledger.save()


def history_choices(ledger, metadata, selected):
    """Only collected receipts are predecessors in the collection channel."""
    receipts = []
    for row in ledger.tasks.all():
        delivery = row['data'].get('mathesis_delivery', {})
        envelope = delivery.get('collection_envelope', delivery.get('envelope', {}))
        prior = envelope.get('metadata', {})
        if (prior.get('schema') == 'mathesis.manuscript-delivery.v1'
                and prior.get('work_id') == metadata['id'] and prior.get('version') == str(metadata['revision'])
                and delivery.get('phase') == 'delivered'):
            raise ValueError('This note revision already has a manuscript-deliveries receipt; do not resend')
        if prior.get('schema') != SCHEMA or prior.get('work_id') != metadata['id']:
            continue
        require(delivery.get('phase') == 'delivered', 'Await the earlier collected version readback')
        receipt = delivery['receipt']
        require(receipt['version'] < metadata['revision'], 'Collected revision already selected; retain its exact request')
        receipts.append(receipt['version'])
    return {**selected, 'collection_version': metadata['revision'],
            'collection_previous_version': max(receipts) if receipts else None}


def upgrade(ledger, row, root, folder, secret):
    """Adapt old unattempted signed shortnote selections without altering them."""
    from .publication import package, standing, exclusion, source_gate, choices
    delivery = copy.deepcopy(row['data']['mathesis_delivery'])
    if delivery.get('collection_envelope'):
        return
    table, decision = standing(root)
    require(exclusion(row, ledger.tasks, table, decision) is None, 'Publication is held or joint-excluded')
    source_gate(row, ledger.tasks, root)
    with tempfile.TemporaryDirectory(prefix='collection-upgrade-', dir=folder) as temp:
        metadata, prepared, source = package(row, temp, ledger.releases)
        require(exclusion(row, ledger.tasks, table, decision, metadata) is None, 'Publication is held or joint-excluded')
        selected = choices(decision, metadata, Path(temp) / source)
        selected['source_file'] = source
        row = cache_prior_work(ledger, row, temp, ledger.releases)
        selected = history_choices(ledger, metadata, selected)
        envelope = select(row, temp, selected, secret, ledger.tasks, ledger.releases, metadata,
                          row['data']['mathesis_prior_work']['check'])
    delivery['collection_envelope'] = envelope
    current = ledger.tasks.get(row['id'])
    ledger.tasks.update(row['id'], current['revision'], current['state'], {**current['data'], 'mathesis_delivery': delivery})
    ledger.save(force=True)
