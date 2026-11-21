"""Acceptance tests for the shortnote draft -> uplift review rounds."""
import hashlib
import hmac
from pathlib import Path
import tempfile
import unittest

from managed import automation, note_closure
from managed.control import Ledger, task_prompt, validate_result, workflow
from managed.queue import canonical, digest
from test_tasks import Releases, use_fixture_workflow_revisions


class NoteUpliftTests(unittest.TestCase):
    def setUp(self):
        use_fixture_workflow_revisions(self)
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        (self.root / 'ledger').mkdir()
        self.ledger = Ledger(Releases(), self.root / 'ledger')
        self.config = {'paused': False, 'automation': {
            'enabled': True, 'max_new_tasks_per_tick': 8, 'max_revisions': 3}}

    def tearDown(self):
        self.ledger.tasks.close()
        self.temp.cleanup()

    def request(self, task_id, note_round=None):
        inputs = {'source': 'fixture-result'}
        if note_round is not None:
            inputs['note_round'] = note_round
        return {'schema_version': 1, 'id': task_id, 'kind': 'shortnote',
                'owner': 'result-fixture', 'source_revision': 'a' * 40,
                'inputs': inputs, 'workflow_hash': workflow('shortnote')}

    def result(self, task_id, *, outcome='completed', routing=None):
        def artifact(name, role):
            return {'release_tag': 'outreach-task-' + task_id, 'name': name,
                    'sha256': hashlib.sha256(name.encode()).hexdigest(),
                    'bytes': len(name), 'role': role}
        value = {'schema_version': 1, 'task_id': task_id, 'outcome': outcome,
                 'report': 'Checked the exact claim and proof.',
                 'artifacts': [artifact('note.tex', 'note-tex'),
                               artifact('note.pdf', 'note-pdf')],
                 'packet': None}
        if outcome == 'completed':
            value.update(formalization_gaps=[], value_assessment={
                'verdict': 'incremental', 'reasons': 'The proof extends the known positive-index bound to zero.'})
        if routing is not None:
            value['routing'] = routing
        return value

    def awaiting(self, task_id, note_round=None, *, outcome='completed', routing=None):
        row = self.ledger.tasks.add(self.request(task_id, note_round))
        return self.ledger.tasks.update(
            task_id, row['revision'], 'awaiting-review',
            {'result': self.result(task_id, outcome=outcome, routing=routing)})

    def reconcile(self):
        automation.reconcile(self.ledger, self.config, self.root, revision='b' * 40)

    def sign(self, row, verdict):
        return self.ledger.tasks.verdict(self.signed(self.ledger.tasks.review_binding(row), verdict), 'secret')

    def signed(self, binding, verdict):
        envelope = {'binding': binding,
                    'reviewer': 'local-claude-loop', 'verdict': verdict,
                    'reason': 'Checked the exact fixture result.',
                    'evidence_ref': 'private:note-uplift-test'}
        envelope['signature'] = hmac.new(
            b'secret', canonical(envelope).encode(), hashlib.sha256).hexdigest()
        return envelope

    def set_trivial(self, row):
        result = self.result(row['id'])
        result['value_assessment'] = {'verdict': 'trivial', 'reasons': 'Immediate from the cited lemma.'}
        return self.ledger.tasks.update(row['id'], row['revision'], 'awaiting-review', {'result': result})

    def test_trivial_pass_rejected_and_two_rounds_end_in_archive(self):
        tasks = self.ledger.tasks
        row = self.set_trivial(tasks.add(self.request('note-trivial', 'uplift')))
        binding = {'task_id': row['id'], 'task_revision': row['revision'],
                   'result_hash': digest(row['data']['result']), 'workflow_hash': workflow('shortnote')}
        with self.assertRaisesRegex(ValueError, 'trivial short note cannot PASS'):
            tasks.verdict(self.signed(binding, 'PASS'), 'secret')
        for count in (1, 2):
            self.reconcile()
            self.reconcile()
            previous = tasks.get(row['id'])
            row = tasks.get(previous['data']['continuation_id'])
            automation.parent_gate(row['request'], tasks)
            self.assertEqual(row['request']['inputs']['note_value_uplifts'], count)
            self.assertIn('Make the contribution non-trivial', row['request']['inputs']['instructions'])
            self.assertNotIn('one required Oracle GPT Pro prior-work check', task_prompt(row))
            row = self.set_trivial(row)
        self.reconcile()
        self.assertEqual(len(tasks.all()), 3)
        self.assertEqual(tasks.reviews(), [])
        work = automation.work(tasks, self.root)
        self.assertEqual(work['shortnote_archive_only'][0]['reason'], 'trivial after uplift')
        self.assertEqual(work['short_note_deliveries'], [])

    def test_old_unreviewed_note_gets_a_closure_round(self):
        row = self.awaiting('old-note', 'uplift')
        result = dict(row['data']['result'])
        del result['formalization_gaps'], result['value_assessment']
        tasks = self.ledger.tasks
        row = tasks.update(row['id'], row['revision'], row['state'], {'result': result})
        self.assertEqual(tasks.reviews(), [])
        self.reconcile()
        successor = tasks.get(tasks.get(row['id'])['data']['continuation_id'])
        automation.parent_gate(successor['request'], tasks)
        self.assertEqual(successor['request']['inputs']['automation_parent']['purpose'], 'shortnote-closure')
        self.assertEqual(successor['request']['inputs']['previous_result'], result)

    def test_value_uplift_can_earn_pass_and_limit_can_be_disabled(self):
        tasks = self.ledger.tasks
        row = self.set_trivial(tasks.add(self.request('note-to-improve', 'uplift')))
        self.reconcile()
        row = tasks.get(tasks.get(row['id'])['data']['continuation_id'])
        row = tasks.update(row['id'], row['revision'], 'awaiting-review', {'result': self.result(row['id'])})
        self.assertEqual(self.sign(row, 'PASS')['state'], 'approved')

        self.config['automation']['shortnote_extra_uplift_rounds'] = 0
        row = self.set_trivial(tasks.add(self.request('note-without-extra-rounds', 'uplift')))
        self.reconcile()
        self.assertEqual(tasks.get(row['id'])['data']['reason'], 'trivial after uplift')
        self.assertEqual(len(tasks.all()), 3)

    def test_current_result_contract_requires_both_closure_fields(self):
        row = self.awaiting('note-contract', 'uplift')
        result = {**row['data']['result'], 'author_contacts': [],
                  'author_contacts_reason': 'No posing author is credited at the source.'}
        validate_result(result, row)
        for field in ('formalization_gaps', 'value_assessment'):
            with self.subTest(field=field), self.assertRaises(ValueError):
                validate_result({k: v for k, v in result.items() if k != field}, row)

    def test_mail_and_mathesis_require_exact_eligible_source(self):
        tasks = self.ledger.tasks
        trivial = self.set_trivial(tasks.add(self.request('note-trivial', 'uplift')))
        approved = self.sign(self.awaiting('note-approved', 'uplift'), 'PASS')
        for kind in ('firstcontact', 'reply', 'mathesis'):
            with self.subTest(kind=kind):
                request = {'kind': kind, 'inputs': {}}
                ref = {'task_id': approved['id'], 'result_hash': digest(approved['data']['result'])}
                note_closure.source_gate(request, tasks, {'note_sources': [ref]})
                with self.assertRaisesRegex(ValueError, 'Source short note'):
                    note_closure.source_gate(request, tasks, {'note_sources': [{**ref, 'result_hash': '0' * 64}]})
                request['inputs']['note_sources'] = [{'task_id': trivial['id'],
                                                      'result_hash': digest(trivial['data']['result'])}]
                with self.assertRaisesRegex(ValueError, 'Source short note'):
                    note_closure.source_gate(request, tasks)

    def test_draft_completion_creates_one_uplift_and_leaves_draft_out_of_reviews(self):
        draft = self.awaiting('shortnote-draft', 'draft')
        exact = draft['data']['result']
        self.reconcile()
        self.reconcile()
        rows = self.ledger.tasks.all()
        children = [r for r in rows if r['request']['inputs'].get('automation_parent', {}).get('task_id') == draft['id']]
        self.assertEqual(len(children), 1)
        child = children[0]
        self.assertEqual(self.ledger.tasks.get(draft['id'])['state'], 'round-complete')
        self.assertEqual(child['request']['inputs']['note_round'], 'uplift')
        self.assertEqual(child['request']['inputs']['previous_result'], exact)
        self.assertNotIn(draft['id'], {item['binding']['task_id'] for item in self.ledger.tasks.reviews()})

    def test_uplift_result_is_reviewable(self):
        draft = self.awaiting('shortnote-draft', 'draft')
        self.reconcile()
        child = next(r for r in self.ledger.tasks.all() if r['id'] != draft['id'])
        result = self.result(child['id'])
        row = self.ledger.tasks.update(child['id'], child['revision'], 'awaiting-review', {'result': result})
        self.assertEqual([item['binding']['task_id'] for item in self.ledger.tasks.reviews()], [child['id']])
        self.assertEqual(row['request']['inputs']['note_round'], 'uplift')

    def test_uplift_bounce_revises_within_uplift(self):
        draft = self.awaiting('shortnote-draft', 'draft')
        self.reconcile()
        child = next(r for r in self.ledger.tasks.all() if r['id'] != draft['id'])
        child = self.ledger.tasks.update(child['id'], child['revision'], 'awaiting-review',
                                         {'result': self.result(child['id'])})
        self.sign(child, 'BOUNCE')
        self.reconcile()
        revision = next(r for r in self.ledger.tasks.all()
                        if r['request']['inputs'].get('automation_parent', {}).get('task_id') == child['id'])
        self.assertEqual(revision['request']['inputs']['note_round'], 'uplift')
        self.assertEqual(revision['request']['inputs']['previous_result'], child['data']['result'])

    def test_closed_roundless_note_is_reviewable_and_pass_can_derive(self):
        paper = {'id': 'fixture-paper', 'title': 'Fixture paper', 'authors': ['Blaire Mercer'],
                 'target_journal': 'Fixture Journal', 'source_quote': 'source quote',
                 'message_id': '<fixture@example.invalid>', 'artifact_names': ['note.tex']}
        routing = {'mail': 'none', 'questions': [], 'independent_submission': paper}
        legacy = self.awaiting('shortnote-legacy', None, routing=routing)
        self.assertEqual(self.ledger.tasks.reviews()[0]['binding']['task_id'], legacy['id'])
        self.sign(legacy, 'PASS')
        self.reconcile()
        self.assertTrue(any(r['kind'] == 'submission' for r in self.ledger.tasks.all()))

    def test_uplift_pass_can_derive_downstream_task(self):
        paper = {'id': 'fixture-paper', 'title': 'Fixture paper', 'authors': ['Blaire Mercer'],
                 'target_journal': 'Fixture Journal', 'source_quote': 'source quote',
                 'message_id': '<fixture@example.invalid>', 'artifact_names': ['note.tex']}
        routing = {'mail': 'none', 'questions': [], 'independent_submission': paper}
        draft = self.awaiting('shortnote-draft', 'draft')
        self.reconcile()
        uplift = next(r for r in self.ledger.tasks.all() if r['id'] != draft['id'])
        uplift = self.ledger.tasks.update(uplift['id'], uplift['revision'], 'awaiting-review',
                                          {'result': self.result(uplift['id'], routing=routing)})
        self.sign(uplift, 'PASS')
        self.reconcile()
        self.assertTrue(any(r['kind'] == 'submission' and
                            r['request']['inputs']['automation_parent']['task_id'] == uplift['id']
                            for r in self.ledger.tasks.all()))
        deliveries = automation.work(self.ledger.tasks, self.root)['short_note_deliveries']
        self.assertEqual([item['task_id'] for item in deliveries], [uplift['id']])

    def test_prompts_contain_uplift_and_reply_hard_checks(self):
        uplift = self.awaiting('shortnote-uplift', 'uplift')
        prompt = task_prompt(uplift)
        for phrase in ('one required Oracle GPT Pro prior-work check', 'sibling analysis', 'coverage table',
                       'exact posing locator', 'pinned .lean', 'self-audit checklist'):
            self.assertIn(phrase, prompt)
        self.assertNotIn('one-question prompt', prompt)
        reply = {'schema_version': 1, 'id': 'reply-fixture', 'kind': 'reply',
                 'owner': 'result-fixture', 'source_revision': 'a' * 40,
                 'inputs': {}, 'workflow_hash': workflow('reply')}
        reply_prompt = task_prompt({'kind': 'reply', 'request': reply, 'data': {}})
        for phrase in ('Use the newest latest_sent entry', 'for its salutation and closing',
                       'Hold if drafts_complete is false or missing', 'an absent historical ID',
                       'They need no separate short-note PASS', 'the reply attaches or cites'):
            self.assertIn(phrase, reply_prompt)


if __name__ == '__main__':
    unittest.main()
