"""Receipt-bound delivery, restart recovery and production/test separation."""
import copy
import hashlib
import hmac
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from fixture_config import test_config
from fixture_config import use_example_config
from managed import automation, mathesis_delivery as m
from managed import mathesis_acceptance as acceptance
from managed.control import ROOT, Ledger, advance, sign_review, stage, validate_result, workflow_text
from managed.queue import canonical, digest
from test_tasks import Releases, request


class Remote(Releases):
    repo = 'fixture/repo'
    tag = 'outreach-control'
    def __init__(self):
        super().__init__()
        self.files = {}
    def ensure(self): pass
    def preserve_task(self, path, tag): self.files[tag, path.name] = path.read_bytes()
    def run(self, *args):
        tag = args[2]
        name = args[args.index('--pattern') + 1]
        folder = Path(args[args.index('--dir') + 1])
        target = folder / name
        # Like gh: an existing file is never overwritten without --clobber.
        if target.exists() and '--clobber' not in args:
            raise RuntimeError('gh release download operation failed: ' + name + ' already exists')
        target.write_bytes(self.files[tag, name])


class Client:
    def __init__(self):
        self.posts = 0; self.gets = []; self.owner_checks = 0
        self.receipts = {}; self.lose_response = False; self.fail_status = False
        self.change_post = lambda r: r; self.change_status = lambda r: r
        self.before_post = lambda: None
    def verify_owner(self): self.owner_checks += 1; return 'verified-owner'
    def deliver(self, metadata, source):
        self.before_post(); self.posts += 1
        receipt = {'schema': 'mathesis.manuscript-receipt.v1', 'submission': 'original-submission',
                   'version': 1, 'producer': metadata['producer'], 'work_id': metadata['work_id'],
                   'upstream_version': metadata['version'],
                   'source_sha256': hashlib.sha256(Path(source).read_bytes()).hexdigest(),
                   'publication': None, 'status': 'in_review', 'review': {'status': 'interrupted'}}
        self.receipts[metadata['work_id']] = receipt
        if self.lose_response: raise TimeoutError('Accepted; response lost')
        return self.change_post(copy.deepcopy(receipt))
    def find(self, metadata):
        self.gets.append('find')
        if metadata['work_id'] not in self.receipts:
            raise HTTPError('fixture', 404, 'Not found', {}, None)
        return copy.deepcopy(self.receipts[metadata['work_id']])
    def status(self, submission, version):
        self.gets.append((submission, version))
        if self.fail_status: raise TimeoutError('Status unavailable')
        return self.change_status(copy.deepcopy(next(r for r in self.receipts.values() if r['submission'] == submission)))


class VersionedClient(Client):
    """Mathesis keeps immutable versions; find returns only the current one."""
    def __init__(self, metadata, raw, producer=None):
        super().__init__()
        self.versions = []
        self.posted = []
        self.seed(metadata, raw, producer)

    def seed(self, metadata, raw, producer=None):
        number = len(self.versions) + 1
        receipt = {'schema': 'mathesis.manuscript-receipt.v1', 'submission': 'existing-submission',
                   'producer': producer or metadata['producer'], 'work_id': metadata['work_id'],
                   'upstream_version': metadata['version'], 'version': number,
                   'source_sha256': hashlib.sha256(raw).hexdigest(), 'publication': None,
                   'current': True, 'current_version': number}
        self.versions.append((copy.deepcopy(metadata), receipt))
        return copy.deepcopy(receipt)

    def deliver(self, metadata, source):
        self.before_post(); self.posts += 1
        self.posted.append(copy.deepcopy(metadata))
        raw = Path(source).read_bytes()
        current = self.versions[-1][1]
        if current['producer'] == metadata['producer']:
            for saved, receipt in self.versions:
                if saved['version'] == metadata['version']:
                    if saved == metadata and receipt['source_sha256'] == hashlib.sha256(raw).hexdigest():
                        return copy.deepcopy(receipt)
                    break
            else:
                if metadata.get('previous_version') == current['version']:
                    receipt = self.seed(metadata, raw)
                    if self.lose_response: raise TimeoutError('Accepted; response lost')
                    return receipt
        raise HTTPError('fixture', 409, 'Conflict', {}, io.BytesIO(json.dumps({
            'code': 'conflict', 'title': 'conflict', 'detail': 'PRIVATE REQUEST ECHO',
            'metadata': metadata}).encode()))

    def find(self, metadata):
        self.gets.append('find')
        return copy.deepcopy(self.versions[-1][1])

    def status(self, submission, version):
        self.gets.append((submission, version))
        return copy.deepcopy(self.versions[version - 1][1])


class AcceptanceTests(unittest.TestCase):
    def test_hosted_entry_uses_separate_ledger_and_replays_without_another_post(self):
        production, test = Remote(), Remote(); test.tag = m.TEST_TAG
        client = Client()
        original_deliver = client.deliver
        def deliver(metadata, source):
            receipt = original_deliver(metadata, source)
            receipt['submission'] = test_config()['mathesis']['acceptance_submission']
            client.receipts[metadata['work_id']] = copy.deepcopy(receipt)
            return receipt
        client.deliver = deliver
        config = {'paused': True, 'kinds': {'mathesis': False}, 'max_active': 4,
                  'agent_template': {}, 'mathesis': {'mode': 'deliver'},
                  'automation': {'automatic_mail': False}, 'pipeline': {'daily': False}}
        def releases(*args, **kwargs): return test if kwargs.get('tag') == m.TEST_TAG else production
        # The synthetic review pins HEAD. Model a hosted checkout at that revision
        # even when the offline suite runs with local edits to the review policy.
        head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
        def hosted_workflow_text(path, revision):
            return workflow_text(path, revision or head)

        with patch.dict('os.environ', {'GITHUB_ACTIONS': 'true', 'OUTREACH_REVIEW_KEY': 'secret',
                                      'CMA_ACCESS_TOKEN': 'fake', 'GITHUB_RUN_ID': 'fake-run'}), \
                patch('managed.control.workflow_text', side_effect=hosted_workflow_text), \
                patch('managed.control.Releases', side_effect=releases), \
                patch('managed.mathesis_acceptance.CountedMathesis', return_value=client):
            for _ in range(2):
                with tempfile.TemporaryDirectory() as folder:
                    report = acceptance.run({**config, 'mathesis': {**config['mathesis'], **test_config()['mathesis']}}, folder)
                    self.assertTrue(report['production_ledger_unchanged'])
                    self.assertEqual(report['posts_on_restored_replay'], 0)
        self.assertEqual(client.posts, 1)
        self.assertFalse(production.assets)


class DeliveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.root = Path(self.temp.name)
        self.folder = self.root / 'ledger'; self.folder.mkdir()
        self.remote = Remote(); self.ledger = Ledger(self.remote, self.folder); self.client = Client()
        self.config = {'paused': False, 'kinds': {'mathesis': True}, 'max_active': 4,
                       'agent_template': {}, 'mathesis': {'mode': 'deliver'}}
        fixtures = Path(__file__).parent / 'fixtures/mathesis'
        self.raw = (fixtures / 'fixture.tex').read_bytes()
        self.metadata = json.loads((fixtures / 'delivery.json').read_text())
        self.metadata.update(producer='trureturing-outreach', work_id='original-result')
    def tearDown(self): self.ledger.tasks.close(); self.temp.cleanup()
    def prepared(self, name='mathesis-1', metadata=None):
        tasks = self.ledger.tasks
        row = tasks.add(request(name, kind='mathesis'))
        artifact = {'release_tag': 'outreach-task-' + name, 'name': 'fixture.tex',
                    'bytes': len(self.raw), 'sha256': hashlib.sha256(self.raw).hexdigest(), 'role': 'note-tex'}
        self.remote.files[artifact['release_tag'], artifact['name']] = self.raw
        result = {'schema_version': 1, 'task_id': name, 'outcome': 'completed',
                  'report': 'Synthetic result', 'artifacts': [artifact], 'packet': None}
        validate_result(result, row)
        row = tasks.update(name, row['revision'], 'awaiting-review', {'result': result})
        review = {'binding': tasks.review_binding(row), 'reviewer': 'local-claude-loop', 'verdict': 'PASS',
                  'reason': 'Fixture review', 'evidence_ref': 'test:synthetic-review'}
        review['signature'] = hmac.new(b'secret', canonical(review).encode(), hashlib.sha256).hexdigest()
        tasks.verdict(review, 'secret')
        envelope = {'schema_version': 1, 'task_id': name, 'result_hash': digest(result),
                    'workflow_hash': row['request']['workflow_hash'], 'decision_ref': 'actual-author-decision',
                    'metadata': copy.deepcopy(metadata or self.metadata), 'source': artifact, 'reviewer': 'local-claude-loop'}
        envelope['signature'] = m.signature(envelope, 'secret')
        return envelope
    def ready(self, name='mathesis-1', metadata=None):
        envelope = self.prepared(name, metadata)
        m.ingest(self.ledger.tasks, envelope, 'secret')
        advance(self.ledger, None, {**self.config, 'paused': True}, self.folder)
        self.ledger.save(force=True)
        return envelope
    def run_delivery(self, config=None):
        m.advance(self.ledger, config or self.config, self.root, self.folder, self.client, 'secret')
        self.ledger.save(force=True)  # Match the production tick's final flush.
    def delivery(self, name='mathesis-1'):
        return self.ledger.tasks.get(name)['data']['mathesis_delivery']
    def restore(self):
        self.ledger.tasks.close()
        self.folder = self.root / ('restored-' + str(len(list(self.root.iterdir())))); self.folder.mkdir()
        self.ledger = Ledger(self.remote, self.folder)

    def test_empty_delivery_pass_does_not_load_the_full_ledger(self):
        self.ledger.tasks.add(request('mail', kind='reply'))
        with patch.object(self.ledger.tasks, 'all', side_effect=AssertionError('full ledger load')):
            for again in (False, True):
                m.advance(self.ledger, self.config, self.root, self.folder, self.client, 'secret',
                          attempted=set(), again=again)
        self.assertEqual(self.client.owner_checks, 0)

    def test_second_pass_skips_attempted_rows_but_finds_new_publication(self):
        self.ready()
        attempted = set()
        m.advance(self.ledger, self.config, self.root, self.folder, self.client, 'secret', attempted=attempted)
        self.assertEqual(attempted, {'mathesis-1'})
        # This synthetic fixture uses the manuscript channel. A completed legacy
        # migration is already settled; unfinished migration must still run.
        row = self.ledger.tasks.get('mathesis-1')
        row['data']['mathesis_delivery']['legacy_migration'] = {'phase': 'skipped'}
        self.ledger.tasks.update(row['id'], row['revision'], row['state'], row['data'])
        with patch.object(self.ledger.tasks, 'all', side_effect=AssertionError('full ledger load')):
            m.advance(self.ledger, self.config, self.root, self.folder, self.client, 'secret',
                      attempted=attempted, again=True)
        self.ready('mathesis-2', {**self.metadata, 'work_id': 'new-publication'})
        m.advance(self.ledger, self.config, self.root, self.folder, self.client, 'secret',
                  attempted=attempted, again=True)
        self.assertEqual(self.client.posts, 2)
        self.assertEqual(attempted, {'mathesis-1', 'mathesis-2'})

    def test_exhausted_slots_skip_full_load_without_posting(self):
        self.ready()
        with patch.object(self.ledger.tasks, 'all', side_effect=AssertionError('full ledger load')):
            m.advance(self.ledger, self.config, self.root, self.folder, self.client, 'secret',
                      attempted={'a', 'b', 'c', 'd'}, again=True)
        self.assertEqual(self.client.posts, 0)

    def test_empty_pass_preserves_deferred_work_rules(self):
        from managed.tick import Tick
        tick = Tick(60)
        tick.bind(self.ledger)
        tick.pending['mathesis'] = ['stale']
        m.advance(self.ledger, self.config, self.root, self.folder, self.client, 'secret', attempted=set())
        self.assertEqual(tick.pending['mathesis'], [])
        tick.pending['mathesis'] = ['deferred-in-first-pass']
        m.advance(self.ledger, self.config, self.root, self.folder, self.client, 'secret',
                  attempted=set(), again=True)
        self.assertEqual(tick.pending['mathesis'], ['deferred-in-first-pass'])

    def test_pass_signed_selection_and_persisted_receipt_finish_without_platform_review(self):
        env = self.ready()
        def preserved():
            with tempfile.TemporaryDirectory() as temp:
                saved = Ledger(self.remote, temp)
                try:
                    self.assertEqual(saved.tasks.get('mathesis-1')['data']['mathesis_delivery']['phase'], 'pending')
                    self.assertTrue(saved.tasks.db.execute("SELECT 1 FROM pipeline_state WHERE key LIKE 'mathesis:%'").fetchone())
                finally: saved.tasks.close()
        self.client.before_post = preserved
        self.run_delivery()
        self.assertEqual(self.ledger.tasks.get('mathesis-1')['state'], 'verified')
        self.assertEqual(self.delivery()['receipt']['review']['status'], 'interrupted')
        self.assertIsNone(self.delivery()['receipt']['publication'])
        self.assertIn(('original-submission', 1), self.client.gets)
        self.restore(); m.ingest(self.ledger.tasks, env, 'secret'); self.run_delivery()
        self.assertEqual(self.client.posts, 1)

    def test_lost_post_response_restores_original_identity_even_while_paused(self):
        self.ready(); self.client.lose_response = True; self.run_delivery()
        self.assertEqual(self.delivery()['phase'], 'pending')
        self.restore(); self.run_delivery({**self.config, 'paused': True})
        self.assertEqual(self.delivery()['phase'], 'delivered')
        self.assertIn('find', self.client.gets); self.assertEqual(self.client.posts, 1)

    def test_get_timeout_preserves_original_handle_before_another_current_version(self):
        self.ready(); self.client.fail_status = True; self.run_delivery()
        self.assertEqual(self.delivery()['receipt']['submission'], 'original-submission')
        self.restore(); self.client.fail_status = False
        self.client.find = lambda _: self.fail('Must use the saved original handle')
        self.run_delivery(); self.assertEqual(self.delivery()['phase'], 'delivered')
        self.assertEqual(self.client.posts, 1)

    def test_checkpoint_failure_prevents_external_post(self):
        self.ready(); self.remote.fail_pointer = True
        with self.assertRaises(TimeoutError): self.run_delivery()
        self.assertEqual(self.client.posts, 0)

    def test_pause_disabled_kind_and_prepare_mode_never_post(self):
        self.ready()
        for cfg in [{**self.config, 'paused': True}, {**self.config, 'kinds': {}},
                    {**self.config, 'mathesis': {'mode': 'prepare-handoff'}}]:
            self.run_delivery(cfg)
        self.assertEqual(self.client.posts, 0); self.assertEqual(self.client.owner_checks, 0)

    def test_same_work_in_two_tasks_reuses_persisted_receipt(self):
        self.ready('one'); self.ready('two'); self.run_delivery()
        self.assertEqual(self.client.posts, 1)
        self.assertEqual(self.delivery('two')['phase'], 'delivered')

    def test_changed_metadata_for_same_manuscript_version_cannot_post(self):
        self.ready('one')
        changed = copy.deepcopy(self.metadata); changed['paper']['title'] = 'Changed title'
        self.ready('two', changed); self.run_delivery()
        self.assertEqual(self.client.posts, 1)
        self.assertNotEqual(self.delivery('two')['phase'], 'delivered')

    def test_artifact_readback_hash_failure_cannot_post(self):
        env = self.ready(); self.remote.files[env['source']['release_tag'], 'fixture.tex'] = b'changed'
        self.run_delivery(); self.assertEqual(self.client.posts, 0)

    def test_wrong_post_or_get_receipt_cannot_finish(self):
        for stage_name, changes in [('post', {'source_sha256': 'f' * 64}),
                                    ('get', {'upstream_version': 'wrong'}),
                                    ('get', {'submission': 'other-submission'}),
                                    ('get', {'publication': {'record': 'unexpected-publication'}})]:
            with self.subTest(stage=stage_name, changes=changes):
                name = 'wrong-' + digest([stage_name, changes])[:8]
                md = copy.deepcopy(self.metadata); md['work_id'] = name
                self.ready(name, md)
                self.client.change_post = (lambda r: {**r, **changes}) if stage_name == 'post' else lambda r: r
                self.client.change_status = (lambda r: {**r, **changes}) if stage_name == 'get' else lambda r: r
                self.run_delivery()
                self.assertNotEqual(self.delivery(name)['phase'], 'delivered')
                self.assertNotEqual(self.ledger.tasks.get(name)['state'], 'verified')

    def test_http_conflict_stays_visible_and_does_not_retry(self):
        self.ready()
        def conflict(*args):
            self.client.posts += 1
            raise HTTPError('fixture', 409, 'Conflict', {}, None)
        self.client.deliver = conflict
        self.run_delivery(); self.run_delivery()
        self.assertEqual(self.delivery()['phase'], 'conflict'); self.assertEqual(self.client.posts, 1)
        self.assertEqual(automation.work(self.ledger.tasks, self.root)['mathesis_deliveries'][0]['last_error']['http_status'], 409)

    def existing_conflict(self, raw=b'earlier source', producer=None):
        env = self.ready()
        self.client = VersionedClient(self.metadata, raw, producer)
        self.run_delivery()
        self.assertEqual(self.delivery()['phase'], 'conflict')
        return env

    def test_conflict_revises_our_current_delivery_with_exact_passed_source(self):
        env = self.existing_conflict()
        self.restore()
        def preserved():
            with tempfile.TemporaryDirectory() as temp:
                saved = Ledger(self.remote, temp)
                try:
                    delivery = saved.tasks.get('mathesis-1')['data']['mathesis_delivery']
                    self.assertEqual(delivery['phase'], 'pending')
                    self.assertEqual(delivery['revision']['previous_version'], 1)
                    self.assertNotEqual(delivery['revision']['version'], self.metadata['version'])
                finally: saved.tasks.close()
        self.client.before_post = preserved
        self.run_delivery()
        self.assertEqual(self.delivery()['phase'], 'delivered')
        self.assertEqual(self.delivery()['envelope'], env)
        posted = self.client.posted[-1]
        self.assertEqual(posted['previous_version'], 1)
        self.assertNotEqual(posted['version'], env['metadata']['version'])
        self.assertEqual({k: v for k, v in posted.items() if k not in {'version', 'previous_version'}},
                         {k: v for k, v in env['metadata'].items() if k not in {'version', 'previous_version'}})
        self.assertEqual(self.delivery()['receipt']['source_sha256'], env['source']['sha256'])
        self.assertEqual(len(self.client.versions), 2)

    def test_conflict_identical_sha_adopts_current_receipt_without_posting_again(self):
        earlier = copy.deepcopy(self.metadata); earlier['paper']['title'] = 'Earlier metadata'
        self.ready(); self.client = VersionedClient(earlier, self.raw)
        self.run_delivery(); self.run_delivery()
        self.assertEqual(self.delivery()['phase'], 'delivered')
        self.assertEqual(self.client.posts, 1)
        self.assertEqual(self.delivery()['receipt']['version'], 1)

    def test_conflict_other_producer_keeps_diagnostic_and_never_revises(self):
        self.existing_conflict(raw=self.raw, producer='another-producer')
        self.run_delivery()
        self.assertEqual(self.delivery()['phase'], 'conflict')
        self.assertEqual(self.client.posts, 1)
        self.assertEqual(self.delivery()['last_error'], {'type': 'HTTPError', 'http_status': 409,
                                                       'code': 'conflict', 'title': 'conflict'})
        self.assertNotIn('PRIVATE', json.dumps(self.delivery()))

    def test_lost_conflict_revision_response_replays_without_another_version(self):
        self.existing_conflict()
        self.client.lose_response = True; self.run_delivery()
        self.assertEqual(self.delivery()['phase'], 'pending')
        self.assertEqual(len(self.client.versions), 2)
        self.restore(); self.run_delivery({**self.config, 'paused': True})
        self.assertEqual(self.delivery()['phase'], 'delivered')
        self.assertEqual(self.client.posts, 2)
        self.assertEqual(len(self.client.versions), 2)

    def test_another_task_reuses_the_saved_revision_and_receipt(self):
        self.existing_conflict(); self.run_delivery()
        self.restore(); self.ready('same-work-new-task'); self.run_delivery()
        self.assertEqual(self.delivery('same-work-new-task')['phase'], 'delivered')
        self.assertEqual(self.delivery('same-work-new-task')['receipt'], self.delivery()['receipt'])
        self.assertEqual(self.client.posts, 2)
        self.assertEqual(len(self.client.versions), 2)

    def test_unresolved_conflicts_do_not_starve_the_fifth_row(self):
        for index in range(5):
            metadata = {**self.metadata, 'work_id': 'work-' + str(index)}
            env = self.ready('task-' + str(index), metadata)
            row = self.ledger.tasks.get(env['task_id'])
            data = copy.deepcopy(row['data'])
            data['mathesis_delivery'].update(phase='conflict', last_error={'http_status': 409, 'type': 'HTTPError'})
            self.ledger.tasks.update(row['id'], row['revision'], row['state'], data)
        # The first four lookups cannot establish our work identity.
        self.client = VersionedClient(metadata, self.raw)
        self.run_delivery()
        self.assertEqual(self.delivery('task-4')['phase'], 'conflict')
        self.restore(); self.run_delivery()
        self.assertEqual(self.delivery('task-4')['phase'], 'delivered')
        self.assertEqual(self.client.posts, 0)

    def test_conflict_recovery_uses_actual_current_version_and_adopts_new_upstream_identity(self):
        self.existing_conflict()
        newer = copy.deepcopy(self.metadata); newer['version'] = 'earlier-transport-version'
        self.client.seed(newer, self.raw)
        self.run_delivery()
        self.assertEqual(self.delivery()['phase'], 'delivered')
        self.assertEqual(self.delivery()['receipt']['version'], 2)
        self.assertEqual(self.delivery()['receipt']['upstream_version'], newer['version'])
        self.assertEqual(self.client.posts, 1)

    def test_conflict_revision_uses_current_mathesis_number_not_note_revision(self):
        self.existing_conflict()
        self.client.seed({**self.metadata, 'version': 'other-earlier-version'}, b'other earlier source')
        self.run_delivery()
        self.assertEqual(self.delivery()['phase'], 'delivered')
        self.assertEqual(self.client.posted[-1]['previous_version'], 2)
        self.assertEqual(self.delivery()['receipt']['version'], 3)

    def test_lost_revision_response_with_newer_current_replays_original_version(self):
        self.existing_conflict()
        self.client.lose_response = True; self.run_delivery()
        original_version = self.client.posted[-1]['version']
        self.client.seed({**self.metadata, 'version': 'later-version'}, b'later source')
        self.restore(); self.run_delivery()
        self.assertEqual(self.delivery()['phase'], 'delivered')
        self.assertEqual(self.delivery()['receipt']['version'], 2)
        self.assertEqual(self.client.posted[-1]['version'], original_version)
        self.assertEqual(len(self.client.versions), 3)

    def test_http_diagnostics_discard_non_json_and_bound_code_and_title(self):
        error = HTTPError('fixture', 503, 'Unavailable', {}, io.BytesIO(json.dumps({
            'code': 'x' * 300, 'title': 'y' * 300, 'detail': 'PRIVATE',
            'request': {'metadata': 'PRIVATE'}}).encode()))
        self.assertEqual(m.error_summary(error), {'type': 'HTTPError', 'http_status': 503,
                                                 'code': 'x' * 160, 'title': 'y' * 160})
        error = HTTPError('fixture', 502, 'Bad gateway', {}, io.BytesIO(b'PRIVATE non-JSON'))
        self.assertEqual(m.error_summary(error), {'type': 'HTTPError', 'http_status': 502})

    def test_paused_conflict_can_adopt_but_cannot_post_a_revision(self):
        self.existing_conflict()
        self.run_delivery({**self.config, 'paused': True})
        self.assertEqual(self.client.posts, 1)
        self.assertEqual(self.delivery()['phase'], 'conflict')
        self.client.seed({**self.metadata, 'version': 'already-delivered'}, self.raw)
        self.run_delivery({**self.config, 'paused': True})
        self.assertEqual(self.delivery()['phase'], 'delivered')
        self.assertEqual(self.client.posts, 1)

    def test_invalid_signature_or_changed_review_result_cannot_stage(self):
        env = self.prepared()
        changed = copy.deepcopy(env); changed['metadata']['paper']['title'] = 'Tampered'
        with self.assertRaisesRegex(ValueError, 'signature'): m.ingest(self.ledger.tasks, changed, 'secret')
        row = self.ledger.tasks.get('mathesis-1'); data = copy.deepcopy(row['data'])
        data['result']['report'] = 'A different scientific result'
        self.ledger.tasks.update(row['id'], row['revision'], row['state'], data)
        with self.assertRaisesRegex(ValueError, 'reviewed result'): m.ingest(self.ledger.tasks, env, 'secret')

    def test_new_workflow_or_revoked_review_prevents_post(self):
        self.ready()
        with patch('managed.control.workflow', return_value='f' * 64): self.run_delivery()
        self.assertEqual(self.client.posts, 0)
        row = self.ledger.tasks.get('mathesis-1'); data = copy.deepcopy(row['data']); data['review']['verdict'] = 'HOLD'
        self.ledger.tasks.update(row['id'], row['revision'], row['state'], data)
        self.run_delivery(); self.assertEqual(self.client.posts, 0)

    def test_publication_loop_selection_survives_later_workflow_edit(self):
        self.ready()
        row = self.ledger.tasks.get('mathesis-1'); data = copy.deepcopy(row['data'])
        data['publication_loop'] = {'phase': 'queued'}
        self.ledger.tasks.update(row['id'], row['revision'], row['state'], data)
        # Only the workflow gate is under test; the standing decision is covered elsewhere.
        with patch('managed.control.workflow', return_value='f' * 64), \
             patch('managed.publication.standing', return_value=({}, {})), \
             patch('managed.publication.exclusion', return_value=None):
            self.run_delivery()
        self.assertEqual(self.client.posts, 1)

    def test_workflow_rebind_allows_only_the_current_mathesis_delivery(self):
        self.ready();row=self.ledger.tasks.get('mathesis-1')
        original=copy.deepcopy(row['request'])
        unsigned={k:v for k,v in row['data']['review'].items() if k!='signature'}
        with patch('managed.control.workflow',return_value='c'*64):
            review=sign_review({**unsigned,'rebind_workflow_hash':'c'*64},'secret',kind='mathesis')
            self.ledger.tasks.verdict(review,'secret')
        with patch('managed.control.workflow',return_value='d'*64):
            self.run_delivery();self.assertEqual(self.client.posts,0)
        with patch('managed.control.workflow',return_value='c'*64):
            self.run_delivery()
        self.assertEqual(self.client.posts,1)
        self.assertEqual(self.ledger.tasks.get('mathesis-1')['request'],original)

    def test_delivery_queue_ingests_review_before_signed_selection(self):
        env = self.prepared(); row = self.ledger.tasks.get('mathesis-1')
        review = row['data']['review']
        # Start from the snapshot just before review; assets arrive together.
        self.ledger.tasks.db.execute('DELETE FROM control_events')
        self.ledger.tasks.db.execute("UPDATE tasks SET state='awaiting-review',revision=? WHERE id=?",
                                     (review['binding']['task_revision'], row['id']))
        stage(env, 'mathesis', self.remote, self.folder); stage(review, 'review', self.remote, self.folder)
        self.assertEqual(self.ledger.ingest('secret'), [])
        self.assertEqual(self.delivery()['phase'], 'ready')

    def test_test_mode_cannot_write_production_ledger(self):
        with self.assertRaisesRegex(ValueError, 'production ledger'):
            self.run_delivery({**self.config, 'mathesis': {'mode': 'deliver', 'test_only': True}})

    def test_reviewed_note_inventory_uses_saved_binding(self):
        env = self.prepared()
        row = self.ledger.tasks.get('mathesis-1')
        self.ledger.tasks.db.execute("UPDATE tasks SET kind='shortnote' WHERE id=?", (row['id'],))
        items = automation.work(self.ledger.tasks, self.root)['short_note_deliveries']
        self.assertEqual(items[0]['binding']['result_hash'], env['result_hash'])

    def test_local_selection_preserves_exact_reviewed_source_and_decisions(self):
        self.prepared(); row = self.ledger.tasks.get('mathesis-1')
        directory = self.root / 'handoff'; directory.mkdir(); (directory / 'fixture.tex').write_bytes(self.raw)
        handoff = {'schema_version': 'outreach-mathesis-handoff.v1', 'private_correspondence_included': False,
                   'source_note': {'id': 'original-result', 'revision': 1, 'title': 'Result', 'abstract': 'Scope',
                                   'creators': [{'name': 'B'}, {'name': 'A'}], 'problem_ids': ['original-problem'],
                                   'evidence_url': 'https://sample.example.invalid/source', 'license': 'CC-BY-4.0'},
                   'files': {'fixture.tex': hashlib.sha256(self.raw).hexdigest()}}
        (directory / 'handoff.json').write_text(json.dumps(handoff))
        data = copy.deepcopy(row['data'])
        raw_manifest = (directory / 'handoff.json').read_bytes()
        data['result']['artifacts'].append({'release_tag': 'outreach-task-mathesis-1', 'name': 'handoff.json',
                                          'role': 'handoff', 'bytes': len(raw_manifest),
                                          'sha256': hashlib.sha256(raw_manifest).hexdigest()})
        # A changed manifest is part of a new result and needs its own review.
        row = self.ledger.tasks.update(row['id'], row['revision'], 'awaiting-review', data)
        review = copy.deepcopy(data['review']); review.pop('signature')
        review['binding'] = self.ledger.tasks.review_binding(row)
        review['signature'] = hmac.new(b'secret', canonical(review).encode(), hashlib.sha256).hexdigest()
        row = self.ledger.tasks.verdict(review, 'secret')
        decisions = {'decision_ref': 'actual-user-answer', 'processing_consent': 'ai-review-2026-10-11',
                     'ai_disclosure': {'level': 'assisted', 'statement': 'Actual assistance'},
                     'purpose': 'review_only', 'source_file': 'fixture.tex'}
        env = m.select(row, directory, decisions, 'secret')
        self.assertEqual([a['name'] for a in env['metadata']['paper']['authors']], ['B', 'A'])
        self.assertEqual(env['source'], row['data']['result']['artifacts'][0])
        original_manifest = (directory / 'handoff.json').read_text()
        (directory / 'handoff.json').write_text(original_manifest + ' ')
        with self.assertRaisesRegex(ValueError, 'manifest'): m.select(row, directory, decisions, 'secret')
        (directory / 'handoff.json').write_text(original_manifest)
        (directory / 'fixture.tex').write_text('Changed source')
        with self.assertRaises(ValueError): m.select(row, directory, decisions, 'secret')
        decisions.pop('decision_ref')
        with self.assertRaises(ValueError): m.select(row, directory, decisions, 'secret')


if __name__ == '__main__': unittest.main()


def setUpModule():
    use_example_config()
