"""Sent reply revisions are required inputs to the public note."""
import base64
from contextlib import ExitStack, redirect_stderr
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from managed import automation, control, correspondence, mathesis_delivery, note_revisions, pipeline, publication
from managed.queue import GateError, canonical, digest
from managed.tick import Tick
from test_tasks import Releases, request, use_fixture_workflow_revisions
from test_tick_budget import Clock, Worker


def artifact(task_id, name, role, content):
    return {'release_tag': 'outreach-task-' + task_id, 'name': name, 'role': role,
            'sha256': hashlib.sha256(content).hexdigest(), 'bytes': len(content)}


class NoteRevisionTests(unittest.TestCase):
    def setUp(self):
        use_fixture_workflow_revisions(self)
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        (self.root / 'tables').mkdir()
        self.remote = Releases()
        self.ledger = control.Ledger(self.remote, self.root)
        self.tasks = self.ledger.tasks
        self.addCleanup(self.tasks.close)
        self.cfg = {'paused': False, 'automation': {'enabled': True, 'max_new_tasks_per_tick': 1},
                    'pipeline': {'enabled': True}, 'kinds': {'shortnote': True, 'mathesis': True},
                    'sandbox_pool': {'prioritize_correspondence_notes': True}}
        self.science = {'id': 'bellamy-question15', 'upstream_status': 'resolved-at-current-snapshot',
                        'upstream': {'repository': 'fixture/math', 'resolution_kind': 'refuted',
                                     'declaration_gids': ['Counterexample.result']}}
        self.rid = self.science['id']
        for name, value in {'contacts': {'contacts': []}, 'refusals': {'refusals': []},
                            'results': {'source_commit': 'a' * 40, 'results': [self.science]}}.items():
            (self.root / 'tables' / (name + '.json')).write_text(canonical(value))

    def add(self, task_id, kind, owner, **inputs):
        return self.tasks.add({**request(task_id, owner, kind), 'inputs': inputs})

    def ready(self, row, *artifacts, state='handoff-ready'):
        result = {'outcome': 'completed', 'packet': None, 'artifacts': list(artifacts),
                  'formalization_gaps': [],
                  'value_assessment': {'verdict': 'incremental', 'reasons': 'Checked extension.'}}
        binding = {'task_id': row['id'], 'result_hash': digest(result),
                   'workflow_hash': row['request']['workflow_hash']}
        return self.tasks.update(row['id'], row['revision'], state, {'result': result,
            'review': {'verdict': 'PASS', 'binding': binding, 'reviewer': 'local-claude-loop'}})

    def head(self, task_id='prior-work-revision', *, rid=None, content=b'prior work', **inputs):
        row = self.add(task_id, 'shortnote', 'result-' + (rid or self.rid), **inputs)
        return self.ready(row, artifact(task_id, 'note-v4.pdf', 'note-pdf', content),
                          artifact(task_id, 'note-v4.tex', 'note-tex', b'prior work source'))

    def reply(self, task_id='sent-reply', *, rid=None, content=b'comments 20 to 26', at='2026-11-18T09:13:50Z',
              state='verified', provide_artifacts=True, selected_inputs=(), receipt=True):
        row = self.add(task_id, 'reply', 'bellamy', selected_inputs=list(selected_inputs))
        name = 'note_20261112.pdf'
        packet = {'problem_ids': [rid or self.rid] if rid != '' else [], 'attachments': [
            {'name': name, 'media_type': 'application/pdf', 'content_b64': base64.b64encode(content).decode()}]}
        artifacts = ([artifact(task_id, name, 'note-pdf', content),
                      artifact(task_id, 'note_20261112.tex', 'note-tex', b'corrected source')]
                     if provide_artifacts else [])
        data = {'result': {'outcome': 'completed', 'packet': packet, 'artifacts': artifacts}}
        if receipt:
            data['sent_receipt'] = {'remote_id': task_id, 'content_hash': digest(packet),
                                    'evidence_ref': 'graph:sent:' + task_id, 'at': at}
        return self.tasks.update(row['id'], row['revision'], state, data)

    def continue_work(self):
        automation.reconcile(self.ledger, self.cfg, self.root, revision='a' * 40)

    def derive(self):
        pipeline.reconcile(self.ledger, self.cfg, self.root, revision='a' * 40)

    def merges(self):
        return [r for r in self.tasks.all() if r['request']['inputs'].get('sent_revision_result_id')]

    def test_holds_stale_mathesis_and_stages_reviewed_successor_on_reply_lane(self):
        self.derive()
        old = self.head(result=self.science, pipeline={
            'scenario': 3, 'result_id': self.rid, 'source_identity': pipeline.source_identity(self.science)})
        self.tasks.db.execute('UPDATE pipeline_sources SET task_id=? WHERE result_id=?', (old['id'], self.rid))
        self.derive()
        stale = next(r for r in self.tasks.all() if r['kind'] == 'mathesis')
        sent = self.reply()
        index = note_revisions.Revisions(self.tasks)
        self.assertIsNotNone(index.reason(stale))
        with self.assertRaisesRegex(GateError, 'handoff-ready merged'):
            control.queued_gates(stale, self.tasks)
        self.continue_work()
        self.continue_work()
        merge, = self.merges()
        inputs = merge['request']['inputs']
        self.assertEqual(inputs['automation_parent'], {**note_revisions.binding(sent), 'purpose': 'merge-sent-revision'})
        self.assertEqual(inputs['note_sources'], [note_revisions.binding(old), note_revisions.binding(sent)])
        self.assertEqual(inputs['selected_inputs'], old['data']['result']['artifacts'] + sent['data']['result']['artifacts'])
        self.assertIn('Make no new claims', inputs['instructions'])
        self.assertEqual(self.tasks.get(stale['id'])['state'], 'held')
        self.assertIn(self.rid, self.tasks.get(stale['id'])['data']['reason'])
        self.assertIn(merge['id'], correspondence.linked_contacts(self.tasks.all(), self.root, self.cfg))
        automation.parent_gate(merge['request'], self.tasks)

        with patch('managed.daily_local.status', return_value={}):
            view = automation.work(self.tasks, self.root, self.cfg)
        self.assertEqual(view['diverged_short_notes'], [{
            'result_id': self.rid, 'head_task_id': old['id'], 'reply_task_id': sent['id'],
            'head_sha256': [a['sha256'] for a in old['data']['result']['artifacts']],
            'reply_sha256': [sent['data']['result']['artifacts'][0]['sha256']], 'merge_task_id': merge['id']}])

        files = [artifact(merge['id'], 'note-v5.pdf', 'note-pdf', b'merged'),
                 artifact(merge['id'], 'note-v5.tex', 'note-tex', b'merged source')]
        approved = self.ready(merge, *files, state='approved')
        self.derive()
        self.assertEqual(len([r for r in self.tasks.all() if r['kind'] == 'mathesis']), 1)
        merged = self.tasks.update(approved['id'], approved['revision'], 'handoff-ready', approved['data'])
        self.derive()
        handoff, = [r for r in self.tasks.all() if r['kind'] == 'mathesis' and r['id'] != stale['id']]
        self.assertEqual(handoff['request']['inputs']['note_sources'], [note_revisions.binding(merged)])
        index = note_revisions.Revisions(self.tasks)
        self.assertEqual(index.work(), [])
        self.assertIsNone(index.reason(handoff))
        self.assertIn('Stale note', index.reason(stale))
        self.assertIn(handoff['id'], correspondence.linked_contacts(self.tasks.all(), self.root, self.cfg))

    def test_results_sharing_one_note_get_one_merge_and_duplicates_are_superseded(self):
        self.cfg['automation']['max_new_tasks_per_tick'] = 4
        old = self.head(problem_ids=[self.rid, 'sibling-conjecture'])
        sent = self.reply()
        packet = dict(sent['data']['result']['packet'], problem_ids=[self.rid, 'sibling-conjecture'])
        data = {**sent['data'], 'result': {**sent['data']['result'], 'packet': packet},
                'sent_receipt': {**sent['data']['sent_receipt'], 'content_hash': digest(packet)}}
        sent = self.tasks.update(sent['id'], sent['revision'], 'verified', data)
        index = note_revisions.Revisions(self.tasks)
        self.assertEqual({rid for rid, _, _ in index.divergences()}, {self.rid, 'sibling-conjecture'})
        self.continue_work()
        merge, = self.merges()
        # A duplicate staged before deduplication existed is superseded, not run.
        duplicate = self.tasks.add({**request('legacy-duplicate', merge['owner'], 'shortnote'),
                                    'inputs': {**merge['request']['inputs'],
                                               'sent_revision_result_id': 'sibling-conjecture'}})
        self.continue_work()
        self.assertEqual(self.tasks.get(duplicate['id'])['state'], 'superseded')
        self.assertEqual(self.tasks.get(duplicate['id'])['data']['successor'], merge['id'])
        self.assertEqual([m['id'] for m in self.merges() if m['state'] != 'superseded'], [merge['id']])
        with patch('managed.daily_local.status', return_value={}):
            view = automation.work(self.tasks, self.root, self.cfg)
        self.assertEqual({row['merge_task_id'] for row in view['diverged_short_notes']}, {merge['id']})

    def test_merge_superseded_by_coverage_is_restaged_once(self):
        self.head()
        self.reply()
        self.continue_work()
        merge, = self.merges()
        self.tasks.update(merge['id'], merge['revision'], 'superseded', {
            **merge['data'], 'reason': 'Resolved result already covered by a PASSed completed short note'})
        self.continue_work()
        live = [m for m in self.merges() if m['state'] != 'superseded']
        self.assertEqual(len(live), 1)
        self.assertNotEqual(live[0]['id'], merge['id'])
        self.continue_work()
        self.assertEqual(len([m for m in self.merges() if m['state'] != 'superseded']), 1)

    def test_verified_bytes_and_recorded_inputs_avoid_false_divergence(self):
        self.head(content=b'same PDF')
        same = self.reply('same', content=b'same PDF')
        self.reply('draft', state='owner-draft', at='2026-11-20T00:00:00Z')
        self.reply('unverified', receipt=False)
        self.reply('other-result', rid='another-result')
        changed_packet = {**same['data']['result']['packet'], 'problem_ids': ['wrong-result']}
        self.tasks.update(same['id'], same['revision'], same['state'],
                          {**same['data'], 'packet_override': changed_packet})
        self.assertEqual(note_revisions.Revisions(self.tasks).work(), [])
        sent = self.reply()
        adopted = self.head('already-included', note_sources=[note_revisions.binding(sent)])
        self.assertIsNone(note_revisions.Revisions(self.tasks).reason(adopted))
        self.continue_work()
        self.assertEqual(self.merges(), [])

    def test_latest_sent_timestamp_controls_lineage_and_tick_budget_is_bounded(self):
        self.head()
        self.head('other-head', rid='second')
        newest = self.reply('newest', at='2026-11-19T02:00:00+01:00')
        older = self.reply('older', at='2026-11-19T05:00:00+08:00', content=b'older correction')
        self.reply('second-reply', rid='second')
        self.continue_work()
        merge, = self.merges()
        self.assertEqual(merge['request']['inputs']['automation_parent']['task_id'], newest['id'])
        self.assertIn(note_revisions.binding(older), merge['request']['inputs']['note_sources'])
        self.continue_work()
        self.continue_work()
        self.assertEqual(len(self.merges()), 2)

    def test_tick_stages_bounded_automation_after_advance_overruns_and_resumes_advance(self):
        self.head()
        self.reply()
        for task_id in ('first-bounce', 'second-bounce'):
            row = self.add(task_id, 'research', task_id)
            self.tasks.update(task_id, row['revision'], 'bounced', {
                'result': {'outcome': 'completed'}, 'review': {'verdict': 'BOUNCE'}})
        for task_id in ('slow', 'deferred'):
            row = self.add(task_id, 'research', task_id)
            self.tasks.update(task_id, row['revision'], 'running', {
                'agent_id': 'agt_' + task_id, 'response_id': 'resp_' + task_id})
        self.cfg.pop('sandbox_pool')
        self.cfg['max_active'] = 2
        self.cfg['automation']['max_new_tasks_per_tick'] = 2
        clock = Clock()
        worker = Worker(clock)
        worker.cost = 757
        mocks = {
            'managed.control.CMA': {'return_value': worker},
            'managed.control.refresh_oracle_capacity': {'return_value': None},
            'managed.control.subprocess.check_output': {'return_value': 'a' * 40},
            'managed.mailbox.configured': {'return_value': None},
            'managed.mailbox.intake': {},
            'managed.pipeline.reconcile': {},
            'managed.delivery.advance': {},
            'managed.sent_events.advance': {'return_value': {}},
            'managed.publication.advance': {},
            'managed.mathesis_delivery.advance': {},
            'managed.formalization_requests.advance': {},
        }
        with ExitStack() as stack:
            stack.enter_context(redirect_stderr(io.StringIO()))
            stack.enter_context(patch.object(self.remote, 'prune_snapshots', create=True))
            for name, kwargs in mocks.items():
                stack.enter_context(patch(name, **kwargs))
            tick = Tick(600, clock)
            tick.bind(self.ledger)
            control.run_tick(self.ledger, self.cfg, self.root, tick)
            tick.finish()
            self.ledger.save(force=True)

            self.assertGreater(clock(), tick.deadline)
            self.assertEqual(worker.reads, ['slow'])
            self.assertIn('deferred', tick.pending['advance'])
            merges = self.merges()
            self.assertEqual(len(merges), 1)
            self.assertEqual(self.tasks.get('first-bounce')['state'], 'revising')
            self.assertEqual(self.tasks.get('second-bounce')['state'], 'bounced')

            worker.cost = 0
            with tempfile.TemporaryDirectory() as folder, ExitStack() as cleanup:
                restored = control.Ledger(self.remote, folder)
                cleanup.callback(restored.tasks.close)
                tick = Tick(600, clock)
                tick.bind(restored)
                self.assertIn('deferred', tick.previous['advance'])
                control.run_tick(restored, self.cfg, Path(folder), tick)
                self.assertEqual(worker.reads[1], 'deferred')
                self.assertNotIn('deferred', tick.pending.get('advance', []))
                self.assertEqual(restored.tasks.get('second-bounce')['state'], 'revising')
                self.assertEqual([row['id'] for row in restored.tasks.all()
                                  if note_revisions.Revisions.is_merge(row)], [merges[0]['id']])

    def test_selected_base_infers_result_and_preserves_packet_only_revision(self):
        head = self.head()
        sent = self.reply(rid='', provide_artifacts=False, selected_inputs=head['data']['result']['artifacts'])
        files = {}
        self.remote.repo = 'fixture/repo'

        def preserve(path, tag):
            files[tag, path.name] = path.read_bytes()

        def download(*args):
            tag, name, dest = args[2], args[args.index('--pattern') + 1], args[args.index('--dir') + 1]
            (Path(dest) / name).write_bytes(files[tag, name])

        self.remote.preserve_task = preserve
        self.remote.run = download
        self.continue_work()
        merge, = self.merges()
        other = merge['request']['inputs']['selected_inputs'][-1]
        self.assertEqual(other['release_tag'], 'outreach-task-' + sent['id'])
        self.assertEqual(files[other['release_tag'], other['name']], b'comments 20 to 26')

    def test_publication_and_signed_delivery_ingest_reject_stale_note(self):
        head = self.head()
        handoff = self.ready(self.add('stale', 'mathesis', 'result-' + self.rid, note_sources=[
            note_revisions.binding(head)]))
        self.reply()
        (self.root / 'tables' / 'decisions.json').write_text(json.dumps({'short_note_publication': {'enabled': True}}))
        self.cfg['mathesis'] = {'mode': 'deliver'}
        gh = Mock()
        with patch('managed.publication.standing', return_value=({}, {})), patch('managed.publication.package') as package:
            publication.advance(self.ledger, self.cfg, self.root, self.root, gh=gh, secret='test')
        package.assert_not_called()
        gh.register_note.assert_not_called()
        with patch('managed.mathesis_delivery.validate'), patch('managed.mathesis_delivery.check_row'):
            with self.assertRaisesRegex(GateError, 'handoff-ready merged'):
                mathesis_delivery.ingest(self.tasks, {'task_id': handoff['id'], 'metadata': {}}, 'test')

    def test_previously_staged_delivery_cannot_post_stale_files(self):
        head = self.head()
        handoff = self.ready(self.add('stale', 'mathesis', 'result-' + self.rid, note_sources=[
            note_revisions.binding(head)]))
        self.tasks.update(handoff['id'], handoff['revision'], handoff['state'], {
            **handoff['data'], 'mathesis_delivery': {'phase': 'ready', 'envelope': {'metadata': {}}}})
        self.reply()
        self.cfg['mathesis'] = {'mode': 'deliver'}
        client = Mock()
        mathesis_delivery.advance(self.ledger, self.cfg, self.root, self.root, client=client, secret='test')
        self.assertEqual(client.mock_calls, [])

    def test_round_complete_merge_stays_blocked_until_its_successor_is_ready(self):
        head = self.head()
        self.reply()
        self.continue_work()
        merge, = self.merges()
        completed = self.ready(merge, artifact(merge['id'], 'note-v5.pdf', 'note-pdf', b'merged'),
                               state='round-complete')
        successor = self.add('complete-closure', 'shortnote', merge['owner'],
                             **merge['request']['inputs'])
        self.tasks.update(completed['id'], completed['revision'], completed['state'],
                          {**completed['data'], 'continuation_id': successor['id']})
        index = note_revisions.Revisions(self.tasks)
        self.assertIn('handoff-ready merged short note', index.reason(head))
        self.assertEqual(index.work()[0]['merge_task_id'], successor['id'])
