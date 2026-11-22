"""Standing-decision publication through real selectors and offline transports."""
import base64
import copy
import hashlib
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from fixture_config import use_example_config
from managed import automation, control, mathesis_delivery, pipeline, publication as p
from managed.queue import canonical, digest
from test_mathesis_delivery import Remote
from mathesis_collection_fake import CollectionClient
from test_tasks import request
from test_workflow import FakeGitHub, FakeZenodo


ROOT = Path(__file__).resolve().parents[2]


class Publications(FakeGitHub):
    def __init__(self):
        super().__init__()
        self.catalog = {'schema_version': 'research-publication.v1', 'publications': [],
                        'collaborations': [], 'notes': []}
        self.registrations = 0

    def register_note(self, metadata):
        updated = p.notes.register(self.catalog, metadata)
        p.notes.catalog_note(updated, metadata['id'], require_active=True)
        self.registrations += updated != self.catalog
        self.catalog = updated
        return copy.deepcopy(updated)


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / 'tables').mkdir()
        (self.root / 'managed/submission').mkdir(parents=True)
        policy = json.loads((ROOT / 'managed/submission/author-policy.json').read_text())
        self.table = {
            'holds': [{'task_id': 'shortnote-11111111111111111111111111111111', 'decision': 'HOLD'}],
            'short_note_publication': {
                'enabled': True, 'decision_ref': 'test:standing-decision', 'purpose': 'publication',
                'processing_consent': 'test:consent', 'make_public_after_acceptance': True,
                'independent_authors': ['Avery Reed', 'Blaire Mercer'],
                'ai_disclosure': {'level': 'assisted', **{
                    'statement_' + key: policy['locked_statement_' + key]
                    for key in ('with_lean', 'without_lean')}},
            },
        }
        self.write_decisions()
        (self.root / 'managed/submission/author-policy.json').write_bytes(
            (ROOT / 'managed/submission/author-policy.json').read_bytes())
        self.folder = self.root / 'ledger'; self.folder.mkdir()
        self.remote, self.gh, self.client = Remote(), Publications(), CollectionClient()
        self.ledger = control.Ledger(self.remote, self.folder)
        self.addCleanup(lambda: self.ledger.tasks.close())
        self.config = {'paused': False, 'kinds': {'mathesis': True, 'shortnote': True},
                       'mathesis': {'mode': 'deliver'}}
        self.metadata = {'id': 'alpha', 'revision': 1, 'problem_ids': ['problem-alpha'],
                         'topic': 'Original conjecture', 'title': 'Proof of the conjecture',
                         'creators': [{'name': 'Avery Reed'}, {'name': 'Blaire Mercer'}],
                         'abstract': 'The precise result.', 'summary': 'The original conjecture is proved.',
                         'license': 'cc-by-4.0', 'evidence_url': 'https://sample.example.invalid/commit/proof.lean'}

    def write_decisions(self):
        (self.root / 'tables/decisions.json').write_text(json.dumps(self.table))

    def artifact(self, row, name, raw, role):
        tag = 'outreach-task-' + row['id']
        self.remote.files[tag, name] = raw
        return {'release_tag': tag, 'name': name, 'role': role,
                'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()}

    def prepared(self, name='mathesis-1', kind='mathesis', handoff=True, metadata=None, inputs=None,
                 workflow_hash=None, prior_text=b'## Prior-work check\nVerdict: first-known\nOracle GPT Pro response reference: test:oracle\n'):
        value = request(name, kind=kind)
        value['inputs'].update(inputs or {})
        if workflow_hash:
            value['workflow_hash'] = workflow_hash
        row = self.ledger.tasks.add(value)
        directory = self.root / name; directory.mkdir()
        meta = metadata or self.metadata
        (directory / 'metadata.json').write_bytes(p.notes.w.encode(meta))
        (directory / 'note.pdf').write_bytes(b'%PDF-1.4\nSynthetic checked note')
        with zipfile.ZipFile(directory / 'source.zip', 'w') as source:
            # Identical fixtures must keep identical bytes across ZIP's two-second clock boundary.
            source.writestr(zipfile.ZipInfo('note.tex'),
                            r'\documentclass{article}\begin{document}Proof.\end{document}')
        prepared = p.notes.w.prepare(directory / 'metadata.json', directory / 'note.pdf',
                                     directory / 'source.zip', directory / 'prepared')
        manifest = p.notes.w.read(prepared / f"note-v{meta['revision']}.json")
        artifacts = [self.artifact(row, n, (prepared / n).read_bytes(),
                                  'note-pdf' if n.endswith('.pdf') else 'source-archive')
                     for n in manifest['files']]
        if handoff:
            data = {'schema_version': 'outreach-mathesis-handoff.v1',
                    'private_correspondence_included': False, 'source_note': meta, 'files': manifest['files']}
            artifacts.append(self.artifact(row, 'handoff.json', p.notes.w.encode(data), 'handoff'))
        else:
            artifacts.append(self.artifact(row, f"note-v{meta['revision']}.json",
                                          p.notes.w.encode(manifest), 'evidence'))
        if prior_text is not None:
            artifacts.append(self.artifact(row, 'evidence.md', prior_text, 'evidence'))
        result = {'schema_version': 1, 'task_id': name, 'outcome': 'completed', 'report': 'Checked result',
                  'artifacts': artifacts, 'packet': None}
        if kind == 'shortnote':
            result.update(formalization_gaps=[], value_assessment={
                'verdict': 'incremental', 'reasons': 'Includes the missing boundary case.'})
        row = self.ledger.tasks.update(name, row['revision'], 'awaiting-review', {'result': result})
        review = {'binding': self.ledger.tasks.review_binding(row), 'reviewer': 'local-claude-loop',
                  'verdict': 'PASS', 'reason': 'Independent exact review', 'evidence_ref': 'test:review'}
        row = self.ledger.tasks.verdict(control.sign_review(review, 'secret'), 'secret')
        return self.ledger.tasks.update(name, row['revision'], 'handoff-ready', row['data'])

    def run_publication(self):
        p.advance(self.ledger, self.config, self.root, self.folder, self.gh, 'secret')
        (self.root / 'tables/publications.json').write_text(json.dumps(self.gh.catalog))

    def row(self, name='mathesis-1'):
        return self.ledger.tasks.get(name)

    def note_with_mathesis_inputs(self, note_inputs=None):
        result = {'id': 'problem-alpha', 'upstream_status': 'resolved-at-current-snapshot',
                  'upstream': {'repository': 'fixture/repo', 'resolution_kind': 'proof',
                               'declaration_gids': ['proof']}}
        (self.root / 'tables/results.json').write_text(json.dumps({'results': [result]}))
        link = {'scenario': 3, 'result_id': result['id'], 'source_identity': pipeline.source_identity(result)}
        note = self.prepared('note-1', kind='shortnote', handoff=False,
                             inputs={'pipeline': link, **(note_inputs or {})}, workflow_hash='b' * 64)
        return note, {'note': note['data']['result'], 'pipeline': {
            **link, 'scenario': 4, 'note_task_id': note['id'],
            'note_result_hash': digest(note['data']['result'])}}

    def test_old_workflow_pass_still_enqueues_exact_reviewed_bytes(self):
        for kind in ('shortnote', 'mathesis'):
            meta = {**self.metadata, 'id': kind, 'problem_ids': ['problem-' + kind]}
            row = self.prepared(kind, kind=kind, handoff=kind == 'mathesis', metadata=meta,
                                workflow_hash='b' * 64)
            self.assertNotEqual(row['request']['workflow_hash'], control.workflow(kind))
        self.run_publication()
        self.assertEqual(len(p.notes.queue(self.gh.catalog, self.gh)), 2)
        for kind in ('shortnote', 'mathesis'):
            self.assertEqual(self.row(kind)['data']['publication_loop']['phase'], 'queued')
        self.assertIn('mathesis_delivery', self.row('mathesis')['data'])

    def test_correspondent_note_is_published_before_backlog(self):
        for name in ('mathesis-a', 'mathesis-b'):
            meta = {**self.metadata, 'id': name, 'problem_ids': ['problem-' + name]}
            self.prepared(name, metadata=meta)
        meta = {**self.metadata, 'id': 'owned', 'problem_ids': ['problem-owned']}
        self.prepared('owned-note', kind='shortnote', handoff=False, metadata=meta,
                      inputs={'owner_ids': ['contact-1']})
        self.config['mathesis']['max_publications_per_tick'] = 1
        self.run_publication()
        self.assertEqual(self.row('owned-note')['data']['publication_loop']['phase'], 'queued')
        for name in ('mathesis-a', 'mathesis-b'):
            self.assertNotIn('publication_loop', self.row(name)['data'])

    def test_correspondent_note_precedes_previously_deferred_backlog(self):
        from managed.tick import Tick
        for name in ('mathesis-a', 'mathesis-b'):
            meta = {**self.metadata, 'id': name, 'problem_ids': ['problem-' + name]}
            self.prepared(name, metadata=meta)
        meta = {**self.metadata, 'id': 'owned', 'problem_ids': ['problem-owned']}
        self.prepared('owned-note', kind='shortnote', handoff=False, metadata=meta,
                      inputs={'owner_ids': ['contact-1']})
        tick = Tick(600); tick.bind(self.ledger)
        tick.previous = {'publication': ['mathesis-b', 'mathesis-a']}
        self.config['mathesis']['max_publications_per_tick'] = 1
        self.run_publication()
        self.assertEqual(self.row('owned-note')['data']['publication_loop']['phase'], 'queued')
        for name in ('mathesis-a', 'mathesis-b'):
            self.assertNotIn('publication_loop', self.row(name)['data'])

    def test_mathesis_child_publishes_instead_of_its_passed_source_note(self):
        note, inputs = self.note_with_mathesis_inputs()
        self.prepared(inputs=inputs)
        self.run_publication()
        self.assertEqual(self.row()['data']['publication_loop']['phase'], 'queued')
        self.assertIn('mathesis_delivery', self.row()['data'])
        self.assertEqual(self.row(note['id']), note)
        self.assertEqual(len(p.notes.queue(self.gh.catalog, self.gh)), 1)
        self.assertEqual(self.gh.registrations, 1)

    def test_bounced_or_agent_held_earlier_revisions_do_not_block_passed_note(self):
        earlier = self.ledger.tasks.add(request('note-0', kind='shortnote'))
        self.ledger.tasks.update(earlier['id'], earlier['revision'], 'revising', {
            'result': {'outcome': 'hold'}, 'review': {'verdict': 'BOUNCE'}})
        note, inputs = self.note_with_mathesis_inputs({'automation_parent': {
            'task_id': earlier['id'], 'result_hash': 'superseded', 'purpose': 'revision'}})
        value = request('mathesis-0', kind='mathesis'); value['inputs'] = inputs
        parked = self.ledger.tasks.add(value)  # an earlier parked attempt for the same note
        self.ledger.tasks.update(parked['id'], parked['revision'], 'held', {})
        self.prepared(inputs=inputs)
        self.run_publication()
        self.assertEqual(self.row()['data']['publication_loop']['phase'], 'queued')
        self.assertIn('mathesis_delivery', self.row()['data'])

    def test_passed_note_after_guard_stopped_continuation_is_published(self):
        from managed.queue import digest as _digest
        stopped = self.ledger.tasks.add(request('note-0', kind='shortnote'))
        result = {'outcome': 'hold', 'report': 'stopped by the runtime guard'}
        self.ledger.tasks.update(stopped['id'], stopped['revision'], 'revising',
                                 {'result': result, 'continuation_id': 'note-1'})
        meta = {**self.metadata, 'id': 'guarded', 'problem_ids': ['problem-guarded']}
        self.prepared('note-1', kind='shortnote', handoff=False, metadata=meta, inputs={
            'automation_parent': {'task_id': stopped['id'], 'result_hash': _digest(result),
                                  'purpose': 'guard-continuation'}})
        self.run_publication()
        self.assertEqual(self.row('note-1')['data']['publication_loop']['phase'], 'queued',
                         self.row('note-1')['data']['publication_loop'])

    def test_family_first_author_names_are_not_joint_work(self):
        meta = {**self.metadata, 'creators': [{'name': 'Mercer, Blaire'}, {'name': 'Reed,  Avery'}]}
        self.prepared(metadata=meta)
        self.run_publication()
        self.assertEqual(self.row()['data']['publication_loop']['phase'], 'queued')

    def test_shortnote_waits_untouched_for_non_passed_mathesis_child(self):
        note, inputs = self.note_with_mathesis_inputs()
        value = request('mathesis-child', kind='mathesis'); value['inputs'] = inputs
        child = self.ledger.tasks.add(value)
        for state in ('queued', 'running', 'awaiting-review', 'bounced', 'revising'):
            with self.subTest(state=state):
                child = self.ledger.tasks.update(child['id'], child['revision'], state, child['data'])
                self.run_publication()
                self.assertEqual(self.row(note['id']), note)
                self.assertEqual(self.row(child['id']), child)
                self.assertFalse(self.gh.registrations)
                self.assertFalse(self.gh.releases)

    def test_mathesis_is_preferred_for_same_revision_without_explicit_lineage(self):
        note = self.prepared('note-1', kind='shortnote', handoff=False)
        self.prepared()
        self.run_publication()
        self.assertEqual(self.row()['data']['publication_loop']['phase'], 'queued')
        self.assertIn('mathesis_delivery', self.row()['data'])
        self.assertEqual(self.row(note['id']), note)
        self.assertEqual(len(p.notes.queue(self.gh.catalog, self.gh)), 1)

    def test_existing_queued_revision_skips_later_duplicate_without_recording(self):
        self.prepared(); self.run_publication()
        duplicate = self.prepared('mathesis-2')
        uploads = list(self.gh.uploads)
        with patch.object(self.gh, 'register_note', wraps=self.gh.register_note) as register, \
                patch.object(p.notes, 'enqueue', wraps=p.notes.enqueue) as enqueue:
            self.run_publication()
        register.assert_not_called(); enqueue.assert_not_called()
        self.assertEqual(self.row(duplicate['id']), duplicate)
        self.assertEqual(self.gh.uploads, uploads)

    def test_default_cap_defers_untouched_rows_and_next_call_continues(self):
        rows = []
        for index in range(5):
            name = 'note-' + str(index)
            meta = {**self.metadata, 'id': name, 'problem_ids': ['problem-' + name]}
            rows.append(self.prepared(name, metadata=meta))
        for count in (2, 4, 5):
            self.run_publication()
            self.assertEqual(len(p.notes.queue(self.gh.catalog, self.gh)), count)
            for row in rows[:count]:
                self.assertEqual(self.row(row['id'])['data']['publication_loop']['phase'], 'queued')
            for row in rows[count:]:
                self.assertEqual(self.row(row['id']), row)

    def test_failed_attempt_leaves_both_success_slots_for_new_rows(self):
        self.prepared()
        for name in ('beta', 'gamma'):
            self.prepared(name, metadata={**self.metadata, 'id': name, 'problem_ids': [name]})
        download = self.gh.download
        live = p.Publications('fixture/repo')
        secret = 'ghp_fake_secret_do_not_record'

        def fail_request_readback(tag, name, directory):
            if tag == 'short-note-alpha' and name.startswith('request-'):
                return live.download(tag, name, directory)
            return download(tag, name, directory)

        failure = subprocess.CompletedProcess([], 1, b'', f'gh: Not Found (HTTP 404)\n{secret}'.encode())
        with patch.object(p.notes.w.subprocess, 'run', return_value=failure) as gh_run, \
                patch.object(self.gh, 'download', side_effect=fail_request_readback):
            self.run_publication()
        gh_run.assert_called_once()
        loop = self.row()['data']['publication_loop']
        self.assertEqual(loop['phase'], 'pending')
        self.assertEqual(loop['last_error'], 'RuntimeError')
        self.assertEqual(loop['step'], 'request-readback')
        self.assertEqual(loop['diagnostic'], 'gh release download HTTP 404')
        self.assertIn(('short-note-alpha', loop['request']), self.gh.uploads)
        for name in ('beta', 'gamma'):
            self.assertEqual(self.row(name)['data']['publication_loop']['phase'], 'queued')
            self.assertIn('mathesis_delivery', self.row(name)['data'])
        work = automation.work(self.ledger.tasks, self.root)
        by_task = {item['task_id']: item for item in work['publication_deliveries']}
        self.assertEqual(by_task['mathesis-1'], {'task_id': 'mathesis-1', **loop})
        self.assertEqual(work['publication_failures'], [{
            'task_id': 'mathesis-1', 'note_id': 'alpha', 'revision': 1, 'step': 'request-readback',
            'diagnostic': 'gh release download HTTP 404', 'consecutive_failures': 1,
            'next_retry_at': loop['next_retry_at'],
        }])
        self.assertNotIn(secret, canonical(work))
        self.assertNotIn(secret, '\n'.join(self.ledger.tasks.db.iterdump()))

    def test_new_rows_precede_due_failed_retries_oldest_first(self):
        now = 1_000
        self.ledger.tasks.clock = lambda: now
        register = self.gh.register_note
        calls = []

        def register_note(metadata):
            calls.append(metadata['id'])
            if metadata['id'] != 'untouched':
                raise RuntimeError('GitHub operation failed: already_exists')
            return register(metadata)

        with patch.object(self.gh, 'register_note', side_effect=register_note):
            for name in ('earlier', 'later'):
                self.prepared(name, metadata={**self.metadata, 'id': name, 'problem_ids': [name]},
                              inputs={'owner_ids': ['contact-1']} if name == 'later' else None)
                self.run_publication()
                now += 1
            self.assertEqual(calls, ['earlier', 'later'])
            self.prepared('untouched', metadata={**self.metadata, 'id': 'untouched', 'problem_ids': ['untouched']})
            now += p.recheck.RECHECK_SECONDS
            calls.clear()
            self.run_publication()
        self.assertEqual(calls, ['untouched', 'earlier', 'later'])
        self.assertEqual(self.row('untouched')['data']['publication_loop']['phase'], 'queued')
        self.assertEqual(self.row('earlier')['data']['publication_loop']['consecutive_failures'], 2)
        # Both due failures were retried this tick (bounded by max_publication_retries_per_tick).
        self.assertEqual(self.row('later')['data']['publication_loop']['consecutive_failures'], 2)

    def test_failure_backoff_grows_after_readback_and_resets_for_new_result(self):
        self.prepared(prior_text=b'No labeled prior-work check')
        now = 1_000
        self.ledger.tasks.clock = lambda: now
        run = self.remote.run
        live = control.Releases('fixture/repo')

        evidence_reads = 0
        checkpoint_counts = []

        def download(*args):
            nonlocal evidence_reads
            if args[args.index('--pattern') + 1] == 'evidence.md':
                evidence_reads += 1
                # The cache sees no structured verdict, so select scans again.
                if evidence_reads % 2 == 0:
                    checkpoint_counts.append(self.row()['data']['publication_loop'].get('consecutive_failures'))
                    return live.run(*args)
            return run(*args)

        failure = subprocess.CompletedProcess([], 1, b'', b'no assets match the file pattern')
        with patch.object(self.remote, 'run', side_effect=download), \
                patch.object(control.subprocess, 'run', return_value=failure) as gh_run:
            for count, delay in enumerate((1800, 7200, 28800, 86400, 86400), 1):
                self.run_publication()
                loop = self.row()['data']['publication_loop']
                self.assertEqual(loop['consecutive_failures'], count)
                self.assertEqual(loop['step'], 'select')
                self.assertEqual(loop['diagnostic'], 'gh release download no assets match')
                self.assertEqual(loop['next_retry_at'], now + delay)
                self.run_publication()
                self.assertEqual(gh_run.call_count, count)
                if count < 5:
                    now += delay
            row = self.row()
            data = copy.deepcopy(row['data'])
            data['result']['report'] += ' with revised evidence'
            data['review']['binding']['result_hash'] = digest(data['result'])
            data['review'].pop('signature')
            data['review'] = control.sign_review(data['review'], 'secret')
            self.ledger.tasks.update(row['id'], row['revision'], row['state'], data)
            self.run_publication()
            loop = self.row()['data']['publication_loop']
            self.assertEqual(loop['consecutive_failures'], 1)
            self.assertEqual(loop['next_retry_at'], now + 1800)
        self.assertEqual(checkpoint_counts, [None, 1, 2, 3, 4, None])
        now += 1800
        self.run_publication()
        self.assertEqual(self.row()['data']['publication_loop']['phase'], 'queued')
        self.assertEqual(automation.work(self.ledger.tasks, self.root)['publication_failures'], [])
        self.assertIsNone(p.recheck.next_at(self.ledger.tasks, 'publication', 'mathesis-1',
                                            [loop['result_hash'], loop['decision_hash']]))

    def test_exact_pass_enqueues_durable_request_and_signs_decision(self):
        self.prepared()
        self.run_publication()
        row = self.row()
        self.assertEqual(row['data']['publication_loop']['phase'], 'queued')
        envelope = row['data']['mathesis_delivery']['envelope']
        mathesis_delivery.validate(envelope, 'secret')
        decision = self.table['short_note_publication']
        self.assertEqual(envelope['decision_ref'], decision['decision_ref'])
        paper = envelope['metadata']['paper']
        self.assertEqual(envelope['metadata']['schema'], 'mathesis.pre-reviewed-delivery.v1')
        self.assertTrue(envelope['metadata']['authorize_publication'])
        self.assertEqual(paper['license'], 'CC-BY-4.0')
        record = envelope['metadata']['review']['record']
        self.assertEqual(record['signed_pass'], row['data']['review'])
        self.assertEqual(record['prior_work_check']['verdict'], 'first-known')
        self.assertEqual([a['name'] for a in paper['authors']], ['Avery Reed', 'Blaire Mercer'])
        queue = p.notes.queue(self.gh.catalog, self.gh)
        self.assertEqual(len(queue), 1)
        self.assertEqual(queue[0]['operation'], {'schema_version': 'short-note-request.v1',
                                              'id': 'alpha', 'revision': 1, 'action': 'publish'})
        self.assertIn('request-', queue[0]['request'])

    def test_restored_replay_and_two_tasks_share_one_request_and_delivery(self):
        with patch('zipfile.time.localtime', return_value=(2026, 11, 2, 0, 0, 0, 0, 0, 0)):
            self.prepared()
        with patch('zipfile.time.localtime', return_value=(2026, 11, 2, 0, 0, 2, 0, 0, 0)):
            self.prepared('mathesis-2')
        self.assertEqual(self.remote.files['outreach-task-mathesis-1', 'source-v1.zip'],
                         self.remote.files['outreach-task-mathesis-2', 'source-v1.zip'])
        self.run_publication()
        self.ledger.save(force=True)
        uploads = list(self.gh.uploads)
        self.ledger.tasks.close()
        folder = self.root / 'restored'; folder.mkdir()
        self.ledger = control.Ledger(self.remote, folder)
        self.run_publication()
        self.assertEqual(self.gh.uploads, uploads)
        self.assertEqual(self.gh.registrations, 1)
        self.assertEqual(len(p.notes.queue(self.gh.catalog, self.gh)), 1)
        with patch('managed.mathesis_delivery.gate'):
            for _ in range(2):
                mathesis_delivery.advance(self.ledger, self.config, self.root, folder, self.client, 'secret', self.gh)
        self.assertEqual(self.client.posts, 1)
        self.assertEqual(self.row()['data']['mathesis_delivery']['phase'], 'delivered')

    def test_delivery_does_not_wait_for_doi_and_worker_later_publishes(self):
        self.prepared(); self.run_publication()
        self.assertNotIn('doi', self.row()['data']['mathesis_delivery']['envelope']['metadata']['paper'])
        with patch('managed.mathesis_delivery.gate'):
            mathesis_delivery.advance(self.ledger, self.config, self.root, self.folder, self.client, 'secret', self.gh)
        self.assertEqual(self.client.posts, 1)
        zenodo, pages = FakeZenodo(), FakeGitHub()
        with patch.object(p.notes, 'public_mathesis', side_effect=self.client.public):
            report = p.notes.drain(self.gh.catalog, self.gh, zenodo, pages)
        self.assertEqual(report['processed'], 2)
        self.assertEqual(report['failed'], [])
        self.assertEqual(json.loads(pages.releases['research-updates']['research-updates.v1.json'])['notes'][0]['mathesis_url'],
                         'https://mathesis.example.invalid/papers/alpha?version=1')
        self.assertEqual(p.notes.handoff(self.gh.catalog, self.gh)['notes'][0]['versions'][0]['doi'],
                         '10.5281/zenodo.1')
        self.run_publication()
        self.assertEqual(self.client.posts, 1)

    def test_shortnote_pass_enqueues_without_fabricating_mathesis_review(self):
        self.prepared('note-1', kind='shortnote', handoff=False)
        self.run_publication()
        self.assertEqual(len(p.notes.queue(self.gh.catalog, self.gh)), 1)
        self.assertNotIn('mathesis_delivery', self.row('note-1')['data'])

    def test_disclosure_without_lean_and_decision_reference_come_from_record(self):
        self.metadata['evidence_url'] = 'https://sample.example.invalid/written-proof'
        self.table['short_note_publication']['decision_ref'] = 'test:actual-standing-decision'
        self.write_decisions(); self.prepared(); self.run_publication()
        env = self.row()['data']['mathesis_delivery']['envelope']
        self.assertEqual(env['decision_ref'], 'test:actual-standing-decision')
        self.assertNotIn('lean', env['metadata']['paper'])
        self.assertEqual(env['metadata']['review']['record']['publication_authorization']['decision_ref'],
                         'test:actual-standing-decision')

    def test_hold_joint_and_table_exclusions(self):
        for mode in ('state', 'review', 'joint', 'coauthor', 'note-table', 'problem-table', 'sherwood-parent'):
            with self.subTest(mode=mode):
                name = 'exclude-' + mode
                meta = {**self.metadata, 'id': name, 'problem_ids': ['problem-' + name]}
                if mode == 'coauthor':
                    meta['creators'] = [*meta['creators'], {'name': 'Joint Author'}]
                inputs = {'joint_work': True} if mode == 'joint' else {}
                if mode == 'sherwood-parent':
                    parent = self.ledger.tasks.add(request('shortnote-11111111111111111111111111111111', kind='shortnote'))
                    self.ledger.tasks.update(parent['id'], parent['revision'], 'held', {})
                    inputs = {'automation_parent': {'task_id': parent['id'], 'result_hash': 'not-used', 'purpose': 'revision'}}
                row = self.prepared(name, metadata=meta, inputs=inputs)
                if mode in {'state', 'review'}:
                    data = copy.deepcopy(row['data'])
                    if mode == 'review': data['review']['verdict'] = 'HOLD'
                    self.ledger.tasks.update(name, row['revision'], 'held' if mode == 'state' else row['state'], data)
                if mode.endswith('table'):
                    self.table['holds'].append({'note_id' if mode == 'note-table' else 'legacy_id':
                                               name if mode == 'note-table' else meta['problem_ids'][0], 'decision': 'HOLD'})
                    self.write_decisions()
                self.run_publication()
                self.assertNotIn('mathesis_delivery', self.row(name)['data'])
        self.assertEqual(self.gh.releases, {})
        self.assertEqual(automation.work(self.ledger.tasks, self.root)['publication_failures'], [])

    def checked_exclusions(self, runs=1):
        """Run publication and return the task IDs whose exclusion was evaluated."""
        calls, check = [], p.check_exclusion

        def observe(row, *args, **kwargs):
            calls.append(row['id'])
            return check(row, *args, **kwargs)

        with patch.object(p, 'check_exclusion', side_effect=observe):
            for _ in range(runs):
                self.run_publication()
        return calls

    def test_unchanged_exclusion_is_not_reevaluated_until_its_hold_is_lifted(self):
        self.prepared()
        self.table['holds'].append({'task_id': 'mathesis-1', 'decision': 'HOLD'}); self.write_decisions()
        self.assertEqual(self.checked_exclusions(), ['mathesis-1'])
        excluded = self.row()
        loop = excluded['data']['publication_loop']
        self.assertEqual((loop['phase'], loop['reason']), ('excluded', 'held in tables/decisions.json'))
        self.assertEqual(self.checked_exclusions(runs=2), [])
        self.assertEqual(self.row(), excluded)  # not re-recorded: same revision and updated time
        # An unrelated new note is evaluated (before and after packaging) on its
        # own; the exclusion stays settled.
        self.prepared('other', metadata={**self.metadata, 'id': 'other', 'problem_ids': ['other']})
        self.assertEqual(self.checked_exclusions(), ['other', 'other'])
        self.assertEqual(self.row(), excluded)
        # Records from before the input digest are evaluated once more.
        data = copy.deepcopy(excluded['data']); del data['publication_loop']['exclusion_inputs']
        excluded = self.ledger.tasks.update('mathesis-1', excluded['revision'], excluded['state'], data)
        self.assertEqual(self.checked_exclusions(runs=2), ['mathesis-1'])
        # Lifting the hold is a changed input: the row is evaluated and published.
        self.table['holds'].pop(); self.write_decisions()
        self.assertEqual(self.checked_exclusions(), ['mathesis-1', 'mathesis-1'])
        self.assertEqual(self.row()['data']['publication_loop']['phase'], 'queued')
        self.assertIn('mathesis_delivery', self.row()['data'])

    def test_changed_source_verdict_reevaluates_a_settled_exclusion(self):
        parent = self.ledger.tasks.add(request('note-0', kind='shortnote'))
        parent = self.ledger.tasks.update('note-0', parent['revision'], 'revising', {'review': {'verdict': 'HOLD'}})
        self.prepared(inputs={'automation_parent': {'task_id': 'note-0', 'result_hash': 'not-used',
                                                    'purpose': 'revision'}})
        self.assertEqual(self.checked_exclusions(), ['mathesis-1'])
        self.assertEqual(self.row()['data']['publication_loop']['reason'], 'task or source note is held')
        self.assertEqual(self.checked_exclusions(), [])
        self.ledger.tasks.update('note-0', parent['revision'], 'revising', {'review': {'verdict': 'BOUNCE'}})
        self.assertEqual(self.checked_exclusions(), ['mathesis-1'])
        self.assertNotEqual(self.row()['data']['publication_loop']['phase'], 'excluded')

    def test_package_exclusion_skips_downloads_until_the_decision_changes(self):
        creators = [*self.metadata['creators'], {'name': 'Joint Author'}]
        self.prepared(metadata={**self.metadata, 'creators': creators})
        self.run_publication()
        loop = self.row()['data']['publication_loop']
        self.assertEqual((loop['phase'], loop['reason']), ('excluded', 'joint work needs its own publication decision'))
        self.assertEqual(loop['exclusion_note'], {'id': 'alpha', 'problem_ids': ['problem-alpha']})
        with patch.object(p, 'package', wraps=p.package) as package:
            self.assertEqual(self.checked_exclusions(runs=2), [])
        package.assert_not_called()
        self.table['short_note_publication']['independent_authors'].append('Joint Author'); self.write_decisions()
        self.assertEqual(self.checked_exclusions(), ['mathesis-1', 'mathesis-1'])
        self.assertEqual(self.row()['data']['publication_loop']['phase'], 'queued')

    def test_exclusion_settles_after_its_own_record_drops_note_identities(self):
        with patch.object(self.gh, 'register_note', side_effect=RuntimeError('SHA conflict')):
            self.prepared(); self.run_publication()
        loop = self.row()['data']['publication_loop']
        self.assertEqual((loop['phase'], loop['note_id']), ('pending', 'alpha'))
        self.table['holds'].append({'note_id': 'alpha', 'decision': 'HOLD'}); self.write_decisions()
        with patch.object(self.ledger.tasks, 'clock', return_value=loop['next_retry_at']):
            # The saved note ID matches first; the excluded record no longer has
            # it, so the next pass reads the package's note ID; then it settles.
            self.assertEqual(self.checked_exclusions(runs=3), ['mathesis-1', 'mathesis-1', 'mathesis-1'])
            loop = self.row()['data']['publication_loop']
            self.assertEqual((loop['phase'], loop['exclusion_note']['id']), ('excluded', 'alpha'))
            self.assertEqual(self.checked_exclusions(runs=2), [])
        self.assertNotIn('mathesis_delivery', self.row()['data'])

    def test_stale_pass_changed_release_and_unreviewed_package_never_publish(self):
        row = self.prepared()
        original = copy.deepcopy(row['data'])
        changed = copy.deepcopy(original); changed['result']['report'] += ' changed'
        row = self.ledger.tasks.update(row['id'], row['revision'], row['state'], changed)
        self.run_publication(); self.assertFalse(self.gh.releases)
        row = self.ledger.tasks.update(row['id'], row['revision'], row['state'], original)
        self.remote.files['outreach-task-mathesis-1', 'source-v1.zip'] += b'changed'
        self.run_publication(); self.assertFalse(self.gh.releases)
        self.assertEqual(self.row()['data']['publication_loop']['phase'], 'pending')

    def test_pause_disabled_kind_and_missing_decision_prevent_writes(self):
        self.prepared()
        for config in ({**self.config, 'paused': True}, {**self.config, 'kinds': {'mathesis': False}}):
            p.advance(self.ledger, config, self.root, self.folder, self.gh, 'secret')
        self.table.pop('short_note_publication'); self.write_decisions(); self.run_publication()
        self.assertFalse(self.gh.releases)
        self.assertNotIn('mathesis_delivery', self.row()['data'])

    def test_checkpoint_failure_prevents_registration_upload_and_selection(self):
        self.prepared(); self.remote.fail_pointer = True
        with self.assertRaises(TimeoutError): self.run_publication()
        self.assertFalse(self.gh.releases)
        self.assertFalse(self.gh.registrations)
        self.assertNotIn('mathesis_delivery', self.row()['data'])

    def test_enqueue_response_loss_replays_original_immutable_request(self):
        self.prepared()
        enqueue = p.notes.enqueue
        def lose(*args):
            enqueue(*args)
            raise TimeoutError('Request saved; response lost')
        with patch.object(p.notes, 'enqueue', side_effect=lose): self.run_publication()
        self.assertEqual(self.row()['data']['publication_loop']['phase'], 'pending')
        # A lost response retains its immutable request and waits before retry.
        with patch.object(self.ledger.tasks, 'clock', return_value=
                          self.ledger.tasks.clock() + p.recheck.RECHECK_SECONDS):
            self.run_publication()
        self.assertEqual(self.row()['data']['publication_loop']['phase'], 'queued')
        requests = [n for _, n in self.gh.uploads if n.startswith('request-')]
        self.assertEqual(len(requests), 1)

    def test_existing_published_doi_is_included_but_reserved_doi_is_not(self):
        self.prepared('note-1', kind='shortnote', handoff=False)
        # Model packages already staged/published by the standalone note service.
        self.gh.register_note(self.metadata)
        p.notes.w.stage(self.root / 'note-1/prepared', self.gh)
        zenodo, pages = FakeZenodo(), FakeGitHub()
        p.notes.w.operate('reserve', 'alpha', 1, self.gh, zenodo)
        self.prepared('reserved-mathesis')
        self.run_publication()
        reserved = self.row('reserved-mathesis')['data']['mathesis_delivery']['envelope']
        self.assertNotIn('doi', reserved['metadata']['paper'])
        p.notes.drain(self.gh.catalog, self.gh, zenodo, pages)
        duplicate = self.prepared('published-mathesis')
        self.run_publication()
        self.assertEqual(self.row(duplicate['id']), duplicate)
        # The already signed selection remains immutable after DOI publication.
        self.assertEqual(self.row('reserved-mathesis')['data']['mathesis_delivery']['envelope'], reserved)
        beta = {**self.metadata, 'id': 'beta', 'problem_ids': ['problem-beta']}
        self.prepared('beta-note', kind='shortnote', handoff=False, metadata=beta)
        self.gh.register_note(beta)
        p.notes.w.stage(self.root / 'beta-note/prepared', self.gh)
        p.notes.enqueue(self.gh.catalog, {'schema_version': 'short-note-request.v1', 'id': 'beta',
                                        'revision': 1, 'action': 'publish'}, self.gh)
        p.notes.drain(self.gh.catalog, self.gh, zenodo, pages)
        self.prepared('beta-mathesis', metadata=beta)
        self.run_publication()
        published = self.row('beta-mathesis')['data']['mathesis_delivery']['envelope']
        # DOI provenance stays in the scientific publication selection; the
        # strict collection schema has no DOI field.
        self.assertNotIn('doi', published['metadata']['paper'])
        self.assertEqual(json.loads(self.ledger.tasks.db.execute(
            "SELECT payload FROM pipeline_state WHERE key LIKE 'publication-selection:%'").fetchall()[-1][0])
            ['decisions']['doi'], '10.5281/zenodo.2')

    def test_missing_reviewed_file_and_catalog_pause_prevent_selection(self):
        self.prepared()
        row = self.row(); data = copy.deepcopy(row['data'])
        data['result']['artifacts'] = [a for a in data['result']['artifacts'] if a['name'] != 'source-v1.zip']
        data['review']['binding']['result_hash'] = digest(data['result'])
        row = self.ledger.tasks.update(row['id'], row['revision'], row['state'], data)
        self.run_publication()
        self.assertFalse(self.gh.releases)
        self.assertNotIn('mathesis_delivery', self.row()['data'])
        # Restore the exact original bytes with a new exact review marker.
        self.prepared('paused-mathesis')
        self.gh.catalog = p.notes.register(self.gh.catalog, self.metadata)
        self.gh.catalog['notes'][0]['paused'] = True
        self.run_publication()
        self.assertNotIn('mathesis_delivery', self.row('paused-mathesis')['data'])
        self.assertFalse(self.gh.releases)

    def test_new_hold_after_signing_blocks_mathesis_post(self):
        self.prepared(); self.run_publication()
        self.table['holds'].append({'note_id': 'alpha', 'decision': 'HOLD'})
        self.write_decisions()
        # Use the real delivery gate; only unrelated fixture registry lookups
        # are replaced. This exercises a hold arriving after enqueue/signing.
        with patch('managed.pipeline.source_gate'), patch('managed.automation.owner_gate'):
            mathesis_delivery.advance(self.ledger, self.config, self.root, self.folder, self.client, 'secret', self.gh)
        self.assertEqual(self.client.posts, 0)
        self.assertEqual(self.row()['data']['mathesis_delivery']['last_error']['type'], 'GateError')

    def test_registration_conflict_retries_and_does_not_sign_early(self):
        self.prepared()
        with patch.object(self.gh, 'register_note', side_effect=RuntimeError('SHA conflict')):
            self.run_publication()
        self.assertNotIn('mathesis_delivery', self.row()['data'])
        self.assertFalse(self.gh.releases)
        retry_at = self.row()['data']['publication_loop']['next_retry_at']
        with patch.object(self.ledger.tasks, 'clock', return_value=retry_at):
            self.run_publication()
        self.assertEqual(self.row()['data']['publication_loop']['phase'], 'queued')

    def test_new_revision_waits_for_prior_receipt_and_preserves_mathesis_version(self):
        self.prepared(); self.run_publication()
        meta = {**self.metadata, 'revision': 2}
        self.prepared('revision-2', metadata=meta)
        self.run_publication()
        self.assertNotIn('mathesis_delivery', self.row('revision-2')['data'])
        loop = self.row('revision-2')['data']['publication_loop']
        self.assertEqual(loop['last_error'], 'GateError')
        self.assertEqual(loop['reason'], 'Await the earlier collected version readback')
        with patch('managed.mathesis_delivery.gate'):
            mathesis_delivery.advance(self.ledger, self.config, self.root, self.folder, self.client, 'secret', self.gh)
        with patch.object(self.ledger.tasks, 'clock', return_value=loop['next_retry_at']):
            self.run_publication()
        selected = self.row('revision-2')['data']['mathesis_delivery']['envelope']['metadata']
        self.assertEqual(selected['previous_version'], 1)
        self.assertEqual(selected['version'], 2)

    def test_tick_calls_publication_before_existing_delivery(self):
        from managed.tick import Tick
        order = []
        tick = Tick(); tick.bind(self.ledger)
        with patch('managed.control.ROOT', self.root), \
                patch('managed.control.subprocess.check_output', return_value='a' * 40), \
                patch('managed.control.CMA'), patch('managed.control.advance'), \
                patch('managed.control.refresh_oracle_capacity'), \
                patch('managed.mailbox.configured'), patch('managed.mailbox.intake'), \
                patch('managed.pipeline.reconcile'), patch('managed.automation.reconcile'), \
                patch('managed.delivery.advance'), \
                patch('managed.publication.advance', side_effect=lambda *a: order.append('select')), \
                patch('managed.mathesis_delivery.advance', side_effect=lambda *a: order.append('deliver')), \
                patch.object(self.remote, 'prune_snapshots', create=True):
            control.run_tick(self.ledger, self.config, self.folder, tick)
        self.assertEqual(order, ['select', 'deliver'])

    def tick_with_heavy_publication(self, environment=None):
        """Run one real tick whose publication backlog alone exhausts the budget."""
        from managed.tick import Tick
        self.prepared(); self.run_publication()
        self.assertEqual(self.row()['data']['mathesis_delivery']['phase'], 'ready')
        for index in range(5):
            name = f'backlog-{index}'
            self.prepared(name, kind='shortnote', handoff=False,
                          metadata={**self.metadata, 'id': name, 'problem_ids': ['problem-' + name]})
        self.config['mathesis']['max_publications_per_tick'] = 5
        now = [0.0]
        tick = Tick(600, clock=lambda: now[0]); tick.bind(self.ledger)
        register = self.gh.register_note

        def slow_registration(metadata):
            now[0] += 150  # GitHub work for each new publication
            return register(metadata)

        with patch.dict('os.environ', {'OUTREACH_REVIEW_KEY': 'secret', 'MATHESIS_ACCESS_TOKEN': 'fixture',
                                       **(environment or {})}), \
                patch('managed.control.ROOT', self.root), \
                patch('managed.control.subprocess.check_output', return_value='a' * 40), \
                patch('managed.control.CMA'), patch('managed.control.advance'), \
                patch('managed.control.refresh_oracle_capacity'), \
                patch('managed.mailbox.configured'), patch('managed.mailbox.intake'), \
                patch('managed.pipeline.reconcile'), patch('managed.automation.reconcile'), \
                patch('managed.delivery.advance'), \
                patch('managed.publication.Publications', return_value=self.gh), \
                patch.object(self.gh, 'register_note', side_effect=slow_registration), \
                patch.object(p.notes.w, 'GitHub', return_value=self.gh), \
                patch('managed.mathesis_delivery.Mathesis', return_value=self.client), \
                patch('managed.mathesis_delivery.gate'), \
                patch.object(self.remote, 'prune_snapshots', create=True), \
                patch('sys.stderr', new_callable=io.StringIO) as log:
            control.run_tick(self.ledger, self.config, self.folder, tick)
        # Publication alone used the whole budget and deferred part of its backlog.
        self.assertGreaterEqual(now[0], tick.deadline)
        self.assertTrue(tick.pending['publication'])
        return tick, log.getvalue()

    def test_ready_delivery_is_attempted_when_publication_exhausts_the_tick(self):
        tick, log = self.tick_with_heavy_publication()
        self.assertEqual(self.client.posts, 1)
        row = self.row()
        self.assertEqual(row['state'], 'verified')
        self.assertEqual(row['data']['mathesis_delivery']['phase'], 'delivered')
        self.assertFalse(tick.pending.get('mathesis'))
        self.assertIn('tick-phase mathesis-delivery', log)

    def test_without_the_delivery_slice_publication_starves_delivery(self):
        # The production regression: publication used the stage, delivery deferred.
        tick, _ = self.tick_with_heavy_publication({'OUTREACH_MATHESIS_DELIVERY_RESERVE_SECONDS': '0'})
        self.assertEqual(self.client.posts, 0)
        self.assertEqual(self.row()['data']['mathesis_delivery']['phase'], 'ready')
        self.assertEqual(tick.pending['mathesis'], ['mathesis-1'])

    def test_delivery_batch_takes_new_deliveries_oldest_first_before_repolls(self):
        now = [1_000.0]

        def clock():
            now[0] += 1
            return now[0]

        self.ledger.tasks.clock = clock
        self.prepared(); self.run_publication()
        with patch('managed.mathesis_delivery.gate'):
            mathesis_delivery.advance(self.ledger, self.config, self.root, self.folder, self.client, 'secret', self.gh)
        delivered = self.row()
        data = copy.deepcopy(delivered['data'])
        data['mathesis_delivery']['public_link'] = {'phase': 'pending', 'reason': 'Resume to queue the public reading link'}
        self.ledger.tasks.update(delivered['id'], delivered['revision'], delivered['state'], data)
        for name in ('beta', 'gamma'):
            self.prepared(name, metadata={**self.metadata, 'id': name, 'problem_ids': ['problem-' + name]})
        self.run_publication()
        self.config['mathesis']['deliveries_per_tick'] = 1
        phases = []
        for _ in range(3):
            with patch('managed.mathesis_delivery.gate'):
                mathesis_delivery.advance(self.ledger, self.config, self.root, self.folder, self.client, 'secret',
                                          self.gh)
            phases.append([self.row(name)['data']['mathesis_delivery'].get('public_link', {}).get('phase')
                           for name in ('beta', 'gamma', 'mathesis-1')])
        self.assertEqual(phases, [['queued', None, 'pending'], ['queued', 'queued', 'pending'],
                                  ['queued', 'queued', 'queued']])
        self.assertEqual(self.client.posts, 3)

    def test_registration_uses_current_table_sha_and_preserves_existing_pause(self):
        gh = p.Publications('fixture/repo')
        catalog = copy.deepcopy(self.gh.catalog)
        catalog['notes'] = [{'id': 'other', 'problem_ids': ['other'], 'paused': True}]
        def run(*args):
            if '--method' not in args:
                return canonical({'sha': 'fresh-remote-sha', 'content':
                                  base64.b64encode(p.notes.w.encode(catalog)).decode()}).encode()
            body = json.loads(Path(args[args.index('--input') + 1]).read_text())
            self.assertEqual(body['sha'], 'fresh-remote-sha')
            self.assertEqual(json.loads(base64.b64decode(body['content']))['notes'][1], catalog['notes'][0])
            return b'{}'
        with patch.object(gh, 'run', side_effect=run) as mocked:
            result = gh.register_note(self.metadata)
            self.assertEqual(mocked.call_count, 2)
        result['notes'][0]['paused'] = True
        raw = canonical({'sha': 'new-sha', 'content': base64.b64encode(p.notes.w.encode(result)).decode()}).encode()
        with patch.object(gh, 'run', return_value=raw) as mocked:
            with self.assertRaisesRegex(ValueError, 'paused'): gh.register_note(self.metadata)
            self.assertEqual(mocked.call_count, 1)


if __name__ == '__main__':
    unittest.main()


def setUpModule():
    use_example_config()
