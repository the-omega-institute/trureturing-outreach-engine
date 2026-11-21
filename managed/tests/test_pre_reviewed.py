"""Collection envelopes, exact bytes, prior-work provenance and retry/readback."""
import copy
from email.parser import BytesParser
from email.policy import default
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from fixture_config import test_config
from fixture_config import use_example_config
from managed import automation, mathesis, mathesis_delivery as delivery, pre_reviewed as p
from managed.control import Ledger
import test_publication as fixtures
from mathesis_collection_fake import CollectionClient


class LegacyMigrationClient(CollectionClient):
    def __init__(self, status='in_review', fail_withdraw=False):
        super().__init__()
        self.legacy_status = status
        self.fail_withdraw = fail_withdraw
        self.status_calls = []
        self.withdraw_calls = 0

    def status(self, submission, version=None):
        self.status_calls.append((submission, version))
        return {'status': self.legacy_status, 'submission': submission, 'version': version}

    def withdraw(self, submission):
        self.withdraw_calls += 1
        if self.fail_withdraw:
            self.fail_withdraw = False
            raise TimeoutError('withdraw response lost')
        self.legacy_status = 'withdrawn'
        return {'status': 'withdrawn', 'submission': submission}


class LandingErrorClient(LegacyMigrationClient):
    """Persist the collection version, then lose the first POST response."""
    def __init__(self, status='in_review', error_code=503):
        super().__init__(status)
        self.error_code = error_code
        self.landing_error = True

    def deliver_pre_reviewed(self, metadata, pdf, source):
        receipt = super().deliver_pre_reviewed(metadata, pdf, source)
        if self.landing_error:
            self.landing_error = False
            raise HTTPError('fixture', self.error_code, 'response lost', {}, None)
        return receipt


class PreReviewedTests(unittest.TestCase):
    setUp = fixtures.PublicationTests.setUp
    prepared = fixtures.PublicationTests.prepared
    artifact = fixtures.PublicationTests.artifact
    write_decisions = fixtures.PublicationTests.write_decisions
    run_publication = fixtures.PublicationTests.run_publication
    row = fixtures.PublicationTests.row

    def run_delivery(self, config=None):
        with patch('managed.mathesis_delivery.gate'):
            delivery.advance(self.ledger, config or self.config, self.root, self.folder, self.client, 'secret', self.gh)

    def selection(self):
        return self.row()['data']['mathesis_delivery']['envelope']

    def test_exact_envelope_pass_authors_hashes_decisions_and_pinned_lean(self):
        revision = 'ab' * 20
        self.metadata['evidence_url'] = f'https://github.com/example/proof/blob/{revision}/Note.lean#L4'
        self.prepared(); self.run_publication()
        env = self.selection(); metadata = env['metadata']
        self.assertEqual(metadata['paper']['lean'], {'url': self.metadata['evidence_url'], 'revision': revision})
        self.assertEqual(metadata['paper']['authors'], self.metadata['creators'])
        self.assertEqual(metadata['paper']['license'], 'CC-BY-4.0')
        self.assertEqual(metadata['review']['pdf_sha256'], env['pdf']['sha256'])
        self.assertEqual(metadata['review']['source_sha256'], env['source']['sha256'])
        record = metadata['review']['record']
        self.assertEqual(record['signed_pass'], self.row()['data']['review'])
        self.assertEqual(record['prior_work_check']['oracle_response_ref'], 'test:oracle')
        self.assertEqual(record['publication_authorization'], {
            'decision_ref': 'test:standing-decision', 'direct_display_decision_ref': test_config()['identity']['direct_display_decision_ref']})
        self.run_delivery()
        self.assertEqual(self.row()['state'], 'verified')
        self.assertEqual(self.client.posts, 1)
        state = self.row()['data']['mathesis_delivery']
        self.assertEqual(state['public_link']['phase'], 'queued')
        request = json.loads(self.gh.releases['short-note-alpha'][state['public_link']['request']])
        self.assertEqual(request['action'], 'link-mathesis')
        self.assertEqual(request['receipt'], state['receipt'])

    def test_lost_link_queue_response_retries_without_another_collection_post(self):
        self.prepared(); self.run_publication()
        upload = self.gh.upload
        def uncertain_upload(*args, **kwargs):
            upload(*args, **kwargs)
            raise TimeoutError('Link request persisted; response lost')
        with patch.object(self.gh, 'upload', side_effect=uncertain_upload):
            self.run_delivery()
        self.assertEqual(self.row()['data']['mathesis_delivery']['phase'], 'pending')
        self.assertEqual(self.client.posts, 1)
        self.run_delivery()
        self.assertEqual(self.row()['state'], 'verified')
        links = [r for r in fixtures.p.notes.queue(self.gh.catalog, self.gh)
                 if r['operation']['action'] == 'link-mathesis']
        self.assertEqual(len(links), 1)
        self.assertEqual(self.client.posts, 1)

    def test_release_file_mutation_never_posts(self):
        self.prepared(); self.run_publication()
        pdf = self.selection()['pdf']
        self.remote.files[pdf['release_tag'], pdf['name']] += b'changed'
        self.run_delivery()
        self.assertEqual(self.client.posts, 0)
        self.assertEqual(self.row()['data']['mathesis_delivery']['phase'], 'ready')

    def test_checkpoint_failure_prevents_collection_post(self):
        self.prepared(); self.run_publication()
        self.remote.fail_pointer = True
        self.run_delivery()
        self.assertEqual(self.client.posts, 0)
        self.assertEqual(self.row()['data']['mathesis_delivery']['last_error']['type'], 'TimeoutError')

    def test_conflicts_keep_the_original_version_without_allocating_another(self):
        self.prepared(); self.run_publication()
        metadata = copy.deepcopy(self.selection()['metadata'])
        metadata['paper']['title'] = 'Different immutable metadata'
        self.client.versions['alpha', 1] = (metadata, {}, ('note-v1.pdf', 'source-v1.zip'))
        self.run_delivery(); self.run_delivery()
        self.assertEqual(self.row()['data']['mathesis_delivery']['phase'], 'conflict')
        self.assertEqual(self.selection()['metadata']['version'], 1)
        self.assertEqual(len(self.client.versions), 1)

    def test_lost_post_response_replays_identical_selection_after_restore(self):
        self.prepared(); self.run_publication()
        original = copy.deepcopy(self.selection())
        self.client.lose_response = True; self.run_delivery()
        self.assertEqual(self.row()['data']['mathesis_delivery']['phase'], 'pending')
        self.ledger.save(force=True); self.ledger.tasks.close()
        folder = self.root / 'restored'; folder.mkdir()
        self.ledger = Ledger(self.remote, folder)
        self.client.lose_response = False; self.run_delivery()
        self.assertEqual(self.selection(), original)
        self.assertEqual(self.row()['state'], 'verified')
        self.assertEqual(self.client.posts, 2)
        self.assertEqual(len(self.client.versions), 1)

    def test_readback_hash_failure_keeps_receipt_and_reconciles_while_paused(self):
        self.prepared(); self.run_publication()
        self.client.corrupt = 'pdf'; self.run_delivery()
        self.assertEqual(self.row()['state'], 'handoff-ready')
        state = self.row()['data']['mathesis_delivery']
        self.assertEqual(state['phase'], 'pending')
        self.assertIn('receipt', state)
        self.assertIn('last_error', automation.work(self.ledger.tasks, self.root)['mathesis_deliveries'][0])
        self.client.corrupt = None
        self.run_delivery({**self.config, 'paused': True})
        self.assertEqual(self.row()['state'], 'verified')
        self.assertEqual(self.client.posts, 1)
        self.assertEqual(self.row()['data']['mathesis_delivery']['public_link']['phase'], 'pending')
        self.run_delivery()
        self.assertEqual(self.row()['data']['mathesis_delivery']['public_link']['phase'], 'queued')
        self.assertEqual(self.client.posts, 1)

    def test_wrong_version_or_source_hash_never_verifies(self):
        self.prepared(); self.run_publication()
        self.client.corrupt = 'source'; self.run_delivery()
        self.assertEqual(self.row()['state'], 'handoff-ready')
        self.client.corrupt = None
        real = self.client.collected
        self.client.collected = lambda *a: {**real(*a), 'revision': '2'}
        self.run_delivery()
        self.assertEqual(self.row()['state'], 'handoff-ready')
        self.assertEqual(self.client.posts, 1)

    def test_missing_prior_work_is_recorded_without_blocking_collection(self):
        for name, text in [('absent', None), ('unlabeled', b'Prior-work check: first-known'),
                           ('unclear', b'## Prior-work check\nVerdict: unclear\nOracle Pro response: test:oracle'),
                           ('no-oracle', b'## Prior-work check\nVerdict: prior-exists\nDirect search only.')]:
            self.prepared(name, metadata={**self.metadata, 'id': name}, prior_text=text)
        for _ in range(2):
            self.run_publication(); self.run_delivery()
        work = automation.work(self.ledger.tasks, self.root)
        self.assertEqual({x['task_id'] for x in work['mathesis_needs_prior_work_check']}, {'absent', 'unlabeled'})
        self.assertEqual(self.client.posts, 4)
        self.assertEqual(self.selection_for('absent')['metadata']['review']['record']['prior_work_check'],
                         p.PRIOR_WORK_NOT_RUN)
        self.assertTrue(all(self.row(n)['state'] == 'verified' for n in
                            ('absent', 'unlabeled', 'unclear', 'no-oracle')))

    def selection_for(self, name):
        return self.row(name)['data']['mathesis_delivery']['envelope']

    def test_explicitly_skipped_oracle_is_preserved_as_unresolved_provenance(self):
        check = p.prior_section('## Prior-work check\nVerdict: first-known\nNo Oracle GPT Pro call was made.')
        self.assertFalse(p.prior_ready(check))

    def test_prior_work_can_come_from_nearest_exact_pass_source_note(self):
        note = self.prepared('source-note', kind='shortnote', handoff=False)
        inputs = {'pipeline': {'note_task_id': note['id'], 'note_result_hash':
                              fixtures.digest(note['data']['result']), 'scenario': 4}}
        self.prepared(inputs=inputs, prior_text=None)
        # Test evidence reuse separately from pipeline's unrelated source index.
        with tempfile.TemporaryDirectory() as directory:
            check = p.evidence(self.row(), self.ledger.tasks, directory, self.remote)
        self.assertEqual(check['task_id'], 'source-note')
        self.assertTrue(p.prior_ready(check))

    def test_new_confidentiality_hold_blocks_already_signed_package(self):
        self.prepared(); self.run_publication()
        self.table['holds'].append({'problem_ids': ['problem-alpha'], 'decision': 'HOLD'})
        self.write_decisions(); self.run_delivery()
        self.assertEqual(self.client.posts, 0)
        self.assertEqual(self.row()['state'], 'handoff-ready')

    def test_v2_uses_exact_collection_predecessor_and_old_version_stays_readable(self):
        self.prepared(); self.run_publication(); self.run_delivery()
        self.prepared('v2', metadata={**self.metadata, 'revision': 2})
        self.run_publication(); self.run_delivery()
        env = self.row('v2')['data']['mathesis_delivery']['envelope']
        self.assertEqual(env['metadata']['previous_version'], 1)
        self.assertEqual(env['metadata']['version'], 2)
        p.readback(self.client, self.selection()['metadata'])
        self.assertEqual(self.row('v2')['state'], 'verified')

    def test_manual_selection_preserves_explicit_predecessor(self):
        self.prepared()
        decisions = {'purpose': 'publication', 'decision_ref': 'actual-author-decision',
                     'make_public_after_acceptance': True, 'source_file': 'source-v1.zip',
                     'collection_version': 2, 'previous_version': 1}
        with tempfile.TemporaryDirectory() as directory:
            env = p.select(self.row(), directory, decisions, 'secret', self.ledger.tasks,
                           self.remote, self.metadata)
        self.assertEqual(env['metadata']['previous_version'], 1)
        self.assertEqual(env['metadata']['version'], 2)

    def test_revoked_or_changed_signed_pass_cannot_post(self):
        self.prepared(); self.run_publication()
        row = self.row(); data = copy.deepcopy(row['data'])
        data['review']['reason'] += 'tampered'
        self.ledger.tasks.update(row['id'], row['revision'], row['state'], data)
        self.run_delivery()
        self.assertEqual(self.client.posts, 0)

    def legacy_selection(self, phase='ready'):
        from managed.mathesis_delivery import signature
        self.prepared(); self.run_publication()
        env = copy.deepcopy(self.selection())
        env.pop('signature'); env.pop('pdf')
        note = self.metadata
        env['metadata'] = {'schema': 'mathesis.manuscript-delivery.v1', 'producer': 'trureturing-outreach',
                           'work_id': note['id'], 'version': '1', 'previous_version': None,
                           'paper': {'title': note['title'], 'authors': note['creators'], 'purpose': 'publication',
                                     'processing_consent': 'actual-consent', 'ai_disclosure': {'level': 'assisted'}},
                           'origin': {'problem_ids': note['problem_ids']}}
        env['signature'] = signature(env, 'secret')
        state = {'phase': phase, 'envelope': env}
        if phase != 'ready':
            state['request_hash'] = 'old-attempt'
        row = self.row()
        self.ledger.tasks.update(row['id'], row['revision'], row['state'], {**row['data'], 'mathesis_delivery': state})
        return env

    def test_unattempted_old_handoff_upgrades_without_mutating_signed_selection(self):
        original = self.legacy_selection()
        self.run_delivery()
        state = self.row()['data']['mathesis_delivery']
        self.assertEqual(state['envelope'], original)
        self.assertEqual(state.get('collection_envelope', {}).get('metadata', {}).get('schema'), p.SCHEMA)
        self.assertEqual(self.client.posts, 1)
        self.assertEqual(self.row()['state'], 'verified')

    def test_previously_delivered_ordinary_note_is_never_resent(self):
        self.legacy_selection('delivered'); self.run_delivery()
        self.assertEqual(self.client.posts, 0)
        self.assertNotIn('collection_envelope', self.row()['data']['mathesis_delivery'])

    def test_uncertain_ordinary_attempt_keeps_handle_without_reposting(self):
        self.legacy_selection('pending')
        self.client.find = lambda _: None
        self.run_delivery()
        self.assertEqual(self.client.posts, 0)
        self.assertNotIn('collection_envelope', self.row()['data']['mathesis_delivery'])
        self.assertEqual(self.row()['data']['mathesis_delivery']['phase'], 'pending')

    def legacy_for_migration(self, status='in_review', fail_withdraw=False):
        self.legacy_selection('delivered')
        row = self.row()
        data = copy.deepcopy(row['data'])
        envelope = data['mathesis_delivery']['envelope']
        envelope['metadata']['paper'].update(abstract=self.metadata['abstract'], license='CC-BY-4.0')
        envelope['metadata']['origin']['license'] = 'CC-BY-4.0'
        envelope['signature'] = delivery.signature({k: v for k, v in envelope.items() if k != 'signature'}, 'secret')
        data['mathesis_delivery']['envelope'] = envelope
        data['mathesis_delivery']['receipt'] = {
            'schema': 'mathesis.manuscript-receipt.v1', 'submission': 'legacy-submission',
            'version': 1, 'producer': 'trureturing-outreach', 'work_id': self.metadata['id'],
            'upstream_version': '1', 'source_sha256': envelope['source']['sha256'],
            'publication': None}
        self.ledger.tasks.update(row['id'], row['revision'], row['state'], data)
        self.client = LegacyMigrationClient(status, fail_withdraw)

    def test_legacy_in_review_withdraws_redelivers_reads_back_and_is_idempotent(self):
        from managed.publication import notes

        self.legacy_for_migration()
        collected = self.client.collected
        self.client.collected = lambda *args: {
            **collected(*args), 'authors': [{'name': 'Reed, Avery'}, {'name': 'Mercer, Blaire'}]}
        with patch.object(notes.w, 'GitHub', return_value=self.gh) as github, patch('managed.mathesis_delivery.gate'):
            delivery.advance(self.ledger, self.config, self.root, self.folder, self.client, 'secret')
        github.assert_called_once_with(notes.w.OUTREACH)
        state = self.row()['data']['mathesis_delivery']
        self.assertEqual(state['legacy_migration']['phase'], 'redelivered')
        self.assertEqual(state['legacy_receipt']['submission'], 'legacy-submission')
        self.assertEqual(self.client.withdraw_calls, 1)
        self.assertEqual(self.client.posts, 1)
        self.assertEqual(automation.work(self.ledger.tasks, self.root)['mathesis_legacy_migration']['redelivered'], 1)
        self.assertNotIn('last_error', state)
        self.assertEqual(state['public_link']['phase'], 'queued')
        name = state['public_link']['request']
        request = json.loads(self.gh.releases['short-note-alpha'][name])
        self.assertEqual(request['action'], 'link-mathesis')
        self.assertEqual(request['receipt'], state['receipt'])
        self.run_delivery()
        self.assertEqual(self.row()['data']['mathesis_delivery']['public_link']['request'], name)
        self.assertEqual(self.client.withdraw_calls, 1)
        self.assertEqual(self.client.posts, 1)

    def test_legacy_accepted_is_skipped_with_operator_reason(self):
        self.legacy_for_migration(status='accepted')
        self.run_delivery()
        migration = self.row()['data']['mathesis_delivery']['legacy_migration']
        self.assertEqual(migration['phase'], 'skipped')
        self.assertIn('accepted', migration['reason'])
        self.assertEqual(self.client.posts, 0)

    def test_uncertain_withdrawal_retries_from_checkpoint(self):
        self.legacy_for_migration(fail_withdraw=True)
        self.run_delivery()
        self.assertEqual(self.row()['data']['mathesis_delivery']['legacy_migration']['phase'], 'withdraw-intent')
        self.run_delivery()
        self.assertEqual(self.row()['data']['mathesis_delivery']['legacy_migration']['phase'], 'redelivered')
        self.assertEqual(self.client.withdraw_calls, 2)

    def test_collection_409_with_matching_readback_is_adopted(self):
        self.legacy_for_migration()
        self.client = LandingErrorClient(error_code=409)
        self.run_delivery()
        state = self.row()['data']['mathesis_delivery']
        self.assertEqual(state['legacy_migration']['phase'], 'redelivered')
        self.assertEqual(state['legacy_migration']['collected_paper']['id'], 'alpha')
        self.assertEqual(self.client.posts, 1)
        self.assertEqual(self.client.withdraw_calls, 1)

    def test_redelivery_intent_readback_retries_without_a_second_post(self):
        self.legacy_for_migration()
        self.client = LandingErrorClient(error_code=503)
        self.run_delivery()
        first = self.row()['data']['mathesis_delivery']['legacy_migration']
        self.assertEqual(first['phase'], 'redelivery-intent')
        self.assertEqual(self.client.posts, 1)
        self.client.corrupt = None
        self.run_delivery()
        state = self.row()['data']['mathesis_delivery']
        self.assertEqual(state['legacy_migration']['phase'], 'redelivered')
        self.assertNotIn('public_path', state['legacy_migration']['redelivery_receipt'])
        self.assertEqual(state['receipt']['public_path'], '/papers/alpha?version=1')
        self.assertEqual(state['public_link']['phase'], 'queued')
        self.assertEqual(self.client.posts, 1)

    def test_legacy_pair_selection_prefers_revision_named_source(self):
        self.legacy_for_migration()
        row = self.row()
        data = copy.deepcopy(row['data'])
        extra = self.artifact(row, 'source-v9.zip', b'PK\x03\x04 older', 'source-archive')
        data['result']['artifacts'].append(extra)
        data['review']['binding']['result_hash'] = fixtures.digest(data['result'])
        self.ledger.tasks.update(row['id'], row['revision'], row['state'], data)
        row = self.row()
        envelope = data['mathesis_delivery']['envelope']
        selected = p.legacy_collection_envelope(row, envelope, 'secret')
        self.assertEqual(selected['source']['name'], 'source-v1.zip')
        self.assertEqual(selected['pdf']['name'], 'note-v1.pdf')

    def test_collection_is_delivered_and_read_back_before_withdrawal(self):
        self.legacy_for_migration()
        events = []
        deliver = self.client.deliver_pre_reviewed
        withdraw = self.client.withdraw
        self.client.deliver_pre_reviewed = lambda *args: (events.append('deliver'), deliver(*args))[1]
        self.client.withdraw = lambda *args: (events.append('withdraw'), withdraw(*args))[1]
        self.run_delivery()
        self.assertEqual(events, ['deliver', 'withdraw'])

    def test_gate_error_skips_without_withdrawal(self):
        from managed.queue import GateError
        self.legacy_for_migration()
        with patch('managed.mathesis_delivery.gate', side_effect=GateError('envelope is held')):
            delivery.migrate_legacy_deliveries(self.ledger, self.config, self.root, self.folder,
                                               self.client, 'secret', limit=3)
        migration = self.row()['data']['mathesis_delivery']['legacy_migration']
        self.assertEqual(migration['phase'], 'skipped')
        self.assertIn('envelope is held', migration['reason'])
        self.assertEqual(self.client.withdraw_calls, 0)

    def test_new_migration_rows_wait_behind_an_unresolved_error(self):
        self.legacy_for_migration()
        first = self.row()
        first_data = copy.deepcopy(first['data'])
        first_data['mathesis_delivery'].setdefault('legacy_migration', {'phase': 'pending'})['last_error'] = {
            'type': 'HTTPError', 'http_status': 503}
        self.ledger.tasks.update(first['id'], first['revision'], first['state'], first_data)
        self.prepared('mathesis-2', metadata={**self.metadata, 'id': 'beta'})
        self.run_publication()
        second = self.row('mathesis-2')
        second_data = copy.deepcopy(second['data'])
        second_env = second_data['mathesis_delivery']['envelope']
        second_data['mathesis_delivery'].update(phase='delivered', receipt={
            'schema': 'mathesis.manuscript-receipt.v1', 'submission': 'second-submission',
            'version': 1, 'producer': 'trureturing-outreach', 'work_id': 'beta',
            'upstream_version': '1', 'source_sha256': second_env['source']['sha256'],
            'publication': None})
        self.ledger.tasks.update(second['id'], second['revision'], second['state'], second_data)
        self.client = LegacyMigrationClient()
        self.run_delivery()
        self.assertNotIn('legacy_migration', self.row('mathesis-2')['data']['mathesis_delivery'])


class MultipartClientTests(unittest.TestCase):
    def test_multipart_exact_files_receipt_validation_and_readback_routes(self):
        client = mathesis.Mathesis('synthetic-token')
        raw_pdf, raw_zip = b'%PDF-1.4 exact', b'PK\x03\x04 exact'
        metadata = {'schema': p.SCHEMA, 'producer': 'trureturing-outreach', 'work_id': 'a-note', 'version': 1,
                    'review': {'pdf_sha256': hashlib.sha256(raw_pdf).hexdigest(),
                               'source_sha256': hashlib.sha256(raw_zip).hexdigest()}}
        receipt = {'schema': 'mathesis.pre-reviewed-receipt.v1', 'status': 'collected',
                   'work_id': 'a-note', 'version': 1, **metadata['review']}
        seen = []
        def open_request(request, timeout):
            seen.append(request)
            content_type = request.get_header('Content-type')
            message = BytesParser(policy=default).parsebytes(
                ('Content-Type: ' + content_type + '\r\nMIME-Version: 1.0\r\n\r\n').encode() + request.data)
            fields = {part.get_param('name', header='content-disposition'): part for part in message.iter_parts()}
            self.assertEqual(set(fields), {'metadata', 'pdf', 'source'})
            self.assertEqual(json.loads(fields['metadata'].get_payload(decode=True)), metadata)
            self.assertEqual(fields['pdf'].get_payload(decode=True), raw_pdf)
            self.assertEqual(fields['source'].get_payload(decode=True), raw_zip)
            self.assertEqual(fields['source'].get_filename(), 'source.zip')
            self.assertEqual(request.get_header('Authorization'), 'Bearer synthetic-token')
            return io.BytesIO(json.dumps(receipt).encode())
        with tempfile.TemporaryDirectory() as folder, patch.object(client, 'verify_owner'), \
                patch('managed.mathesis.build_opener') as opener:
            pdf, source = Path(folder) / 'note.pdf', Path(folder) / 'source.zip'
            pdf.write_bytes(raw_pdf); source.write_bytes(raw_zip)
            opener.return_value.open.side_effect = open_request
            self.assertEqual(client.deliver_pre_reviewed(metadata, pdf, source), receipt)
            self.assertEqual(client.deliver_pre_reviewed(metadata, pdf, source), receipt)
            source.write_bytes(raw_zip + b'changed')
            with self.assertRaisesRegex(ValueError, 'exact PASS'):
                client.deliver_pre_reviewed(metadata, pdf, source)
        self.assertTrue(all(r.full_url.endswith('/api/v1/pre-reviewed-deliveries') for r in seen))
        with patch.object(client, 'request', return_value={}) as request:
            client.collected(); client.collected('a-note', 2)
            self.assertEqual([call.args[0] for call in request.call_args_list],
                             ['/api/v1/collected-papers', '/api/v1/collected-papers/a-note?version=2'])
        with self.assertRaisesRegex(ValueError, 'receipt differs'):
            mathesis.collection_receipt({**receipt, 'pdf_sha256': '0' * 64}, metadata)


def setUpModule():
    use_example_config()
