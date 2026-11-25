"""Hosted acceptance of the coordinator, reusing one private synthetic paper."""
import copy
import hashlib
import hmac
import json
import os
from pathlib import Path
import subprocess
import tempfile

from . import mathesis_delivery as delivery
from .mathesis import Mathesis
from .queue import canonical, digest, require



def production_state(release):
    if release is None:
        return None
    return {'body': release['body'], 'assets': sorted((a['name'], a.get('digest')) for a in release['assets'])}


class CountedMathesis(Mathesis):
    def __init__(self, token):
        super().__init__(token)
        self.posts = 0

    def deliver(self, metadata, source):
        self.posts += 1
        return super().deliver(metadata, source)


def run(config, folder):
    from .control import ROOT, ENGINE_ROOT, source_head, Ledger, Releases, advance, stage, validate_result, verify_artifacts, current_policy_revision, request_binding
    require(os.environ.get('GITHUB_ACTIONS') == 'true', 'Acceptance runs in the serialized Actions writer')
    require(config['paused'] and not any(config['kinds'].values())
            and not config['automation']['automatic_mail'] and not config['pipeline']['daily'],
            'Production must remain paused throughout private acceptance')
    secret = os.environ.get('OUTREACH_REVIEW_KEY')
    require(secret, 'The hosted reviewer key is required')
    token = os.environ.get('MATHESIS_ACCESS_TOKEN') or os.environ.get('CMA_ACCESS_TOKEN')
    require(token, 'The supported hosted submitting identity is required')
    production = Releases()
    before = production_state(production.release())
    remote = Releases(tag=delivery.TEST_TAG)
    remote.ensure()
    fixtures = ENGINE_ROOT / 'managed/tests/fixtures/mathesis'
    metadata = json.loads((fixtures / 'delivery.json').read_text())
    source = fixtures / 'fixture.tex'
    require(hashlib.sha256(source.read_bytes()).hexdigest() == delivery.TEST_SOURCE_SHA,
            'Reuse the original private source bytes')
    revision = source_head()
    binding = request_binding('mathesis', revision, current_policy_revision())
    workflow_hash = binding['workflow_hash']
    task_id = 'mathesis-private-test-' + workflow_hash[:16]
    request = {'schema_version': 1, 'id': task_id, 'kind': 'mathesis', 'owner': 'outreach-test-fixture',
               **binding,
               'inputs': {'acceptance': 'private synthetic delivery; no scientific quality claim'}}
    ledger = Ledger(remote, folder)
    client = CountedMathesis(token)
    try:
        # The immutable request pins its first run; a later rerun uses that same
        # request while still executing the current coordinator code.
        if not ledger.tasks.db.execute('SELECT 1 FROM tasks WHERE id=?', (task_id,)).fetchone():
            stage(request, 'request', remote, folder)
        require(not ledger.ingest(secret), 'Acceptance queue ingestion failed')
        row = ledger.tasks.get(task_id)
        artifact = {'release_tag': 'outreach-task-' + task_id, 'name': source.name,
                    'sha256': delivery.TEST_SOURCE_SHA, 'bytes': source.stat().st_size, 'role': 'note-tex'}
        remote.preserve_task(source, artifact['release_tag'])
        result = {'schema_version': 1, 'task_id': task_id, 'outcome': 'completed',
                  'report': 'Synthetic transport fixture. No CMA research or independent scientific review is claimed.',
                  'artifacts': [artifact], 'packet': None}
        validate_result(result, row)
        verify_artifacts(result, remote, folder)
        if row['state'] == 'queued':
            row = ledger.tasks.update(task_id, row['revision'], 'awaiting-review', {'result': result})
            ledger.save()
        require(digest(row['data']['result']) == digest(result), 'The synthetic result changed')
        if row['state'] == 'awaiting-review':
            review = {'binding': ledger.tasks.review_binding(row), 'reviewer': 'hosted-cma-review',
                      'verdict': 'PASS', 'reason': 'Synthetic signed-review fixture for transport acceptance only.',
                      'evidence_ref': 'test-fixture:synthetic-review-v1; no human/Claude scientific review claimed'}
            review['signature'] = hmac.new(secret.encode(), canonical(review).encode(), hashlib.sha256).hexdigest()
            stage(review, 'review', remote, folder)
            require(not ledger.ingest(secret), 'Synthetic review ingestion failed')
        row = ledger.tasks.get(task_id)
        envelope = {'schema_version': 1, 'task_id': task_id, 'result_hash': digest(result),
                    'workflow_hash': row['request']['workflow_hash'],
                    'decision_ref': 'user-authorized private infrastructure test, 2026-10-31',
                    'metadata': metadata, 'source': artifact, 'reviewer': row['data']['review']['reviewer']}
        envelope['signature'] = delivery.signature(envelope, secret)
        stage(envelope, 'mathesis', remote, folder)
        require(not ledger.ingest(secret), 'Signed delivery ingestion failed')
        advance(ledger, None, config, folder)  # Routes PASS; production execution stays paused.
        test_config = copy.deepcopy(config)
        test_config.update(paused=False, kinds={'mathesis': True})
        test_config['mathesis'].update(mode='deliver', test_only=True)
        delivery.advance(ledger, test_config, ROOT, folder, client, secret)
        ledger.save(force=True)  # This entry also ends a coordinator pass.
        row = ledger.tasks.get(task_id)
        require(row['state'] == 'verified' and row['data']['mathesis_delivery']['phase'] == 'delivered',
                'Coordinator did not persist a verified Mathesis delivery')
        receipt = row['data']['mathesis_delivery']['receipt']
        require(receipt['submission'] == config['mathesis']['acceptance_submission'] and receipt['publication'] is None,
                'Acceptance must reuse the existing private submission')
        posts = client.posts
    finally:
        ledger.tasks.close()
    # Restore the durable snapshot and replay the exact queue through the same
    # adapter. Success must not depend on the first process's in-memory state.
    with tempfile.TemporaryDirectory(prefix='mathesis-replay-', dir=folder) as replay:
        ledger = Ledger(remote, replay)
        try:
            require(not ledger.ingest(secret), 'Replayed queue ingestion failed')
            delivery.advance(ledger, test_config, ROOT, replay, client, secret)
            restored = ledger.tasks.get(task_id)
            require(restored['state'] == 'verified' and client.posts == posts, 'Replay attempted another POST')
        finally:
            ledger.tasks.close()
    receipt = delivery.receipt_matches(client.status(receipt['submission'], receipt['version']), envelope)
    require(digest(production_state(production.release())) == digest(before), 'Acceptance changed the production ledger')
    report = {'schema_version': 1, 'test': 'coordinator-mathesis-private-delivery',
              'source_revision': revision, 'test_ledger': delivery.TEST_TAG, 'task_id': task_id,
              'synthetic_review': True, 'state': 'verified', 'receipt': receipt,
              'posts_this_run': posts, 'posts_on_restored_replay': client.posts - posts,
              'production_paused': True, 'production_ledger_unchanged': True,
              'scientific_mail_sent': 0, 'public_manuscripts_created': 0}
    path = Path(folder) / ('acceptance-' + os.environ['GITHUB_RUN_ID'] + '-' + os.environ.get('GITHUB_RUN_ATTEMPT', '1') + '.json')
    path.write_text(canonical(report))
    remote.upload(path)
    path.unlink()
    require(json.loads(remote.download(path.name, folder).read_text()) == report, 'Acceptance report readback differs')
    return report
