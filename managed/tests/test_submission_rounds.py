"""Sequential submission rounds with isolated ledger and staging fixtures."""
import copy
from contextlib import redirect_stderr, redirect_stdout
import hashlib
import hmac
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from fixture_config import use_example_config
from managed import automation, control
from managed.queue import canonical, digest
from test_tasks import CMA, Releases, request, use_fixture_workflow_revisions


class SubmissionRoundTests(unittest.TestCase):
    def setUp(self):
        use_fixture_workflow_revisions(self)
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        (self.root/'tables').mkdir()
        self.folder = self.root/'ledger'; self.folder.mkdir()
        self.remote = Releases()
        self.ledger = control.Ledger(self.remote, self.folder)
        self.cfg = {'paused':False, 'kinds':{'submission':True}, 'max_active':4,
                    'automation':{'enabled':True, 'max_new_tasks_per_tick':4, 'max_revisions':3}}
        self.paper = {'id':'example', 'title':'A theorem', 'authors':['Avery Reed'],
                      'workspace':'../mail/person/submission/example', 'version_date':'2026-11-01',
                      'target_journal':'Journal', 'stage':'needs_work', 'blocker':'Review'}
        self.board()

    def tearDown(self):
        self.ledger.tasks.close()
        self.temp.cleanup()

    def board(self, papers=None):
        (self.root/'tables/submissions.json').write_text(json.dumps({'papers':
            [self.paper] if papers is None else papers}))

    def add(self, name='paper-improve', round_name='improve'):
        value = request(name, 'contact-a', 'submission')
        value['inputs'] = {'automation_contract':1, 'paper':self.paper,
                           'submission_source':{'paper_id':self.paper['id'],
                                                'identity':automation.paper_identity(self.paper)},
                           'owner_ids':['contact-a'], 'revision_number':2, 'review_revisions':2}
        if round_name is not None:
            value['inputs']['submission_round'] = round_name
        return self.ledger.tasks.add(value)

    def complete(self, row, extra_data=None, outcome='completed'):
        result = {'schema_version':1, 'task_id':row['id'], 'outcome':outcome, 'report':'Checked proofs.',
                  'packet':None, 'routing':{'mail':'none', 'questions':[], 'independent_submission':None},
                  'artifacts':[{'release_tag':'outreach-task-'+row['id'], 'name':role,
                                'bytes':10, 'sha256':'a'*64, 'role':role} for role in
                               ['manuscript-pdf', 'manuscript-tex', 'source-archive', 'evidence', 'handoff']]}
        control.validate_result(result, row)
        return self.ledger.tasks.update(row['id'], row['revision'],
                                       'held' if outcome == 'hold' else 'awaiting-review',
                                       {**row['data'], **(extra_data or {}), 'result':result})

    def review(self, row, verdict='PASS', extra_data=None):
        """Import a review recorded under the previous per-round policy."""
        row = self.complete(row, extra_data)
        with patch('managed.tasks.reviewable', return_value=True):
            envelope = {'binding':self.ledger.tasks.review_binding(row), 'reviewer':'local-claude-loop',
                        'verdict':verdict, 'reason':'Verified exact package; confirmed uplift direction.',
                        'evidence_ref':'fixture:review'}
            envelope['signature'] = hmac.new(b'secret', canonical(envelope).encode(), hashlib.sha256).hexdigest()
            return self.ledger.tasks.verdict(envelope, 'secret')

    def reconcile(self, revision='b'*40):
        automation.reconcile(self.ledger, self.cfg, self.root, revision=revision)

    def route(self):
        with patch('managed.control.ROOT', self.root):
            control.advance(self.ledger, CMA(), {**self.cfg, 'paused':True}, self.folder)

    def inventory(self):
        with patch('managed.daily_local.status', return_value={}):
            return automation.work(self.ledger.tasks, self.root)

    def test_next_round_preserves_existing_review_and_uses_exact_artifacts_once(self):
        parent = self.review(self.add(), extra_data={'thread_inputs':{'owner_ids':['contact-a', 'contact-b']}})
        self.route(); self.reconcile(); self.reconcile(revision='c'*40)
        self.assertEqual(len(self.ledger.tasks.all()), 2)
        child = self.ledger.tasks.all()[-1]
        inputs = child['request']['inputs']
        self.assertEqual(inputs['submission_round'], 'sprint')
        self.assertEqual(inputs['automation_parent'], {'task_id':parent['id'],
                         'result_hash':digest(parent['data']['result']), 'purpose':'submission-round'})
        for key in ['automation_contract', 'paper', 'submission_source']:
            self.assertEqual(inputs[key], parent['request']['inputs'][key])
        self.assertEqual(inputs['owner_ids'], ['contact-a', 'contact-b'])
        self.assertEqual(inputs['previous_result'], parent['data']['result'])
        self.assertIsNone(inputs['review_feedback'])
        self.assertTrue(inputs['auto_advanced'])
        self.assertEqual(inputs['selected_inputs'], parent['data']['result']['artifacts'][:4])
        self.assertEqual((inputs['revision_number'], inputs['review_revisions']), (0, 0))
        self.assertIn('sprint', inputs['instructions'])
        self.assertEqual(child['owner'], parent['owner'])
        self.assertEqual(child['request']['source_revision'], 'b'*40)
        self.assertEqual(child['request']['workflow_hash'], control.workflow('submission'))
        automation.parent_gate(child['request'], self.ledger.tasks)
        self.assertEqual(self.ledger.tasks.get(parent['id'])['state'], 'round-complete')
        self.assertEqual(self.ledger.tasks.get(parent['id'])['data']['review'], parent['data']['review'])
        self.assertFalse(self.inventory()['submission_ready'])
        self.assertEqual(self.inventory()['submission_rounds'], [
            {'paper_id':'example', 'round':'sprint', 'task_id':child['id'], 'state':'queued'}])

    def test_completed_rounds_advance_without_reviews_and_finalize_waits_separately(self):
        row = self.add()
        for name in ['improve', 'sprint', 'consult', 'uplift', 'finalize']:
            self.assertEqual(row['request']['inputs']['submission_round'], name)
            self.complete(row)
            self.assertFalse(self.inventory()['reviews'])
            self.reconcile(); self.reconcile()
            row = self.ledger.tasks.get(row['id'])
            self.assertEqual(row['state'], 'submission-ready' if name == 'finalize' else 'round-complete')
            if name != 'finalize':
                self.assertTrue(row['data']['auto_advanced'])
                self.assertFalse(self.inventory()['submission_ready'])
                self.assertFalse(self.inventory()['submission_ready_unreviewed'])
                row = self.ledger.tasks.all()[-1]
                self.assertTrue(row['request']['inputs']['auto_advanced'])
                self.assertIsNone(row['request']['inputs']['review_feedback'])
                automation.parent_gate(row['request'], self.ledger.tasks)
        self.assertEqual(len(self.ledger.tasks.all()), 5)
        self.assertTrue(row['data']['awaiting_operator_review'])
        self.assertFalse(self.inventory()['reviews'])
        self.assertFalse(control.review_digest(self.inventory(), self.root)['items'])
        self.assertEqual(self.inventory()['submission_ready_unreviewed'], [
            {'paper_id':'example', 'title':'A theorem', 'target_journal':'Journal', 'task_id':row['id']}])
        self.assertEqual([r['task_id'] for r in self.inventory()['submission_ready']], [row['id']])
        self.assertEqual(self.inventory()['submission_rounds'], [
            {'paper_id':'example', 'round':'finalize', 'task_id':row['id'], 'state':'submission-ready'}])

    def test_existing_awaiting_review_round_advances_even_with_an_old_review(self):
        parent = self.review(self.add(round_name=None))
        parent = self.ledger.tasks.update(parent['id'], parent['revision'], 'awaiting-review', parent['data'])
        # Old saved inventories also disappear from the digest.
        old = {'reviews':[{'request':parent['request'], 'result':parent['data']['result']}]}
        self.assertFalse(control.review_digest(old, self.root)['items'])
        self.reconcile(); self.reconcile()
        self.assertEqual(len(self.ledger.tasks.all()), 2)
        self.assertEqual(self.ledger.tasks.get(parent['id'])['state'], 'round-complete')
        self.assertFalse(self.inventory()['reviews'])
        child = self.ledger.tasks.all()[-1]
        self.assertEqual(child['request']['inputs']['submission_round'], 'sprint')
        self.assertIsNone(child['request']['inputs']['review_feedback'])
        automation.parent_gate(child['request'], self.ledger.tasks)

    def test_existing_reviewed_successor_keeps_its_immutable_inputs(self):
        parent = self.review(self.add())
        link = {'task_id':parent['id'], 'result_hash':digest(parent['data']['result']), 'purpose':'submission-round'}
        value = request('submission-'+digest(link)[:32], parent['owner'], 'submission')
        value['inputs'] = {'automation_parent':link, 'submission_round':'sprint',
                           'previous_result':parent['data']['result'], 'review_feedback':parent['data']['review']}
        child = self.ledger.tasks.add(value)
        self.reconcile(); self.reconcile(revision='c'*40)
        self.assertEqual(len(self.ledger.tasks.all()), 2)
        self.assertEqual(self.ledger.tasks.get(child['id'])['request'], value)
        self.assertTrue(self.ledger.tasks.get(parent['id'])['data']['auto_advanced'])
        automation.parent_gate(value, self.ledger.tasks)

    def test_request_submission_review_cli_stages_newest_finalize_and_ingest_exposes_review(self):
        self.complete(self.add('old-finalize', 'finalize')); self.reconcile()
        row = self.complete(self.add('new-finalize', 'finalize')); self.reconcile()
        self.paper.update(title='A stronger theorem', target_journal={'name':'Another journal'})
        self.board()
        self.assertEqual(self.inventory()['submission_ready_unreviewed'][-1]['title'], 'A stronger theorem')
        self.assertEqual(self.inventory()['submission_ready_unreviewed'][-1]['target_journal'], 'Another journal')
        (self.root/'managed').mkdir(); (self.root/'config.json').write_text('{}')
        self.remote.ensure = lambda:None
        self.ledger.save(force=True)
        with patch('managed.control.ROOT', self.root), patch('managed.control.Releases', return_value=self.remote), \
             patch('managed.control.review_key', return_value='secret'):
            output = io.StringIO()
            with redirect_stdout(output):
                control.main(['request-submission-review', '--paper', 'example'])
        receipt = json.loads(output.getvalue())
        envelope = json.loads(self.remote.assets[receipt['asset']])
        self.assertEqual(envelope['task_id'], row['id'])
        self.assertEqual(envelope['result_hash'], digest(row['data']['result']))
        self.assertEqual(self.ledger.tasks.get(row['id'])['state'], 'submission-ready')
        with self.assertRaisesRegex(ValueError, 'signature failed'):
            self.ledger.tasks.request_submission_review(envelope, 'wrong-key')
        self.assertEqual(self.ledger.ingest('secret'), [])
        self.reconcile()
        reviews = self.inventory()['reviews']
        self.assertEqual([r['binding']['task_id'] for r in reviews], [row['id']])
        self.assertEqual([r['task_id'] for r in self.inventory()['submission_ready_unreviewed']], ['old-finalize'])
        self.assertEqual(control.review_digest(self.inventory(), self.root)['items'][0]['task_id'], row['id'])
        self.ledger.tasks.request_submission_review(envelope, 'secret')
        self.assertEqual(self.ledger.tasks.review_binding(self.ledger.tasks.get(row['id'])), reviews[0]['binding'])
        review = {'binding':reviews[0]['binding'], 'reviewer':'local-claude-loop', 'verdict':'PASS',
                  'reason':'Checked exact submission package.', 'evidence_ref':'fixture:review'}
        self.ledger.tasks.verdict(control.sign_review(review, 'secret'), 'secret')
        self.route(); self.reconcile()
        self.assertEqual(self.ledger.tasks.get(row['id'])['state'], 'submission-ready')
        self.assertFalse(self.ledger.tasks.get(row['id'])['data']['awaiting_operator_review'])

    def test_hold_still_requires_review_and_never_auto_advances(self):
        row = self.complete(self.add(), outcome='hold')
        self.reconcile()
        reviews = self.inventory()['reviews']
        self.assertEqual([r['binding']['task_id'] for r in reviews], [row['id']])
        self.assertEqual(control.review_digest(self.inventory(), self.root)['items'][0]['outcome'], 'hold')
        self.assertEqual(len(self.ledger.tasks.all()), 1)
        review = {'binding':reviews[0]['binding'], 'reviewer':'local-claude-loop', 'verdict':'HOLD',
                  'reason':'Concur with contribution gate stop.', 'evidence_ref':'fixture:review'}
        self.ledger.tasks.verdict(control.sign_review(review, 'secret'), 'secret')
        self.reconcile()
        self.assertFalse(self.inventory()['reviews'])
        self.assertEqual(len(self.ledger.tasks.all()), 1)

    def test_bounce_keeps_round_and_feedback_then_pass_resets_revision_budget(self):
        parent = self.review(self.add(round_name='consult'), 'BOUNCE')
        self.reconcile(); self.reconcile()
        child = self.ledger.tasks.all()[-1]
        self.assertEqual(len(self.ledger.tasks.all()), 2)
        self.assertEqual(child['request']['inputs']['submission_round'], 'consult')
        self.assertEqual(child['request']['inputs']['previous_result'], parent['data']['result'])
        self.assertEqual(child['request']['inputs']['review_feedback'], parent['data']['review'])
        self.assertEqual(child['request']['inputs']['review_revisions'], 3)
        automation.parent_gate(child['request'], self.ledger.tasks)
        self.review(child); self.route(); self.reconcile()
        next_row = self.ledger.tasks.all()[-1]
        self.assertEqual(next_row['request']['inputs']['submission_round'], 'uplift')
        self.assertEqual(next_row['request']['inputs']['review_revisions'], 0)
        automation.parent_gate(next_row['request'], self.ledger.tasks)

    def test_admission_rejects_skipped_round_incomplete_or_wrong_kind_parent(self):
        parent = self.complete(self.add()); self.reconcile()
        parent = self.ledger.tasks.get(parent['id'])
        child = self.ledger.tasks.all()[-1]
        skipped = copy.deepcopy(child['request']); skipped['inputs']['submission_round'] = 'consult'
        with self.assertRaisesRegex(ValueError, 'exactly the next round'):
            automation.parent_gate(skipped, self.ledger.tasks)
        for changes in [{'state':'awaiting-review'}, {'data':{**parent['data'], 'auto_advanced':False}},
                        {'data':{**parent['data'], 'result':{**parent['data']['result'], 'outcome':'hold'}}},
                        {'kind':'reply'}]:
            with self.subTest(changes=changes), patch.object(self.ledger.tasks, 'get', return_value={**parent, **changes}):
                with self.assertRaisesRegex(ValueError, 'auto-completed parent|source version changed'):
                    automation.parent_gate(child['request'], self.ledger.tasks)
        changed = {**parent, 'data':{**parent['data'], 'result':{'report':'different'}}}
        with patch.object(self.ledger.tasks, 'get', return_value=changed):
            with self.assertRaisesRegex(ValueError, 'source version changed'):
                automation.parent_gate(child['request'], self.ledger.tasks)

    def test_legacy_roundless_ready_pass_derives_sprint_and_hides_old_ready(self):
        parent = self.review(self.add(round_name=None))
        self.ledger.tasks.update(parent['id'], parent['revision'], 'submission-ready', parent['data'])
        self.assertEqual(len(self.inventory()['submission_ready']), 1)
        self.reconcile(); self.reconcile()
        child = self.ledger.tasks.all()[-1]
        self.assertEqual(len(self.ledger.tasks.all()), 2)
        self.assertEqual(child['request']['inputs']['submission_round'], 'sprint')
        self.assertEqual(self.ledger.tasks.get(parent['id'])['state'], 'round-complete')
        self.assertFalse(self.inventory()['submission_ready'])
        automation.parent_gate(child['request'], self.ledger.tasks)
        # A workflow re-pin keeps the same lineage and cannot derive a duplicate.
        child = self.ledger.tasks.repin(child['id'], child['revision'], 'c'*64, 'c'*40)
        self.reconcile()
        self.assertEqual(len(self.ledger.tasks.all()), 3)
        self.assertFalse(self.inventory()['submission_ready'])
        self.assertEqual(self.inventory()['submission_rounds'][0]['task_id'], child['id'])
        automation.parent_gate(child['request'], self.ledger.tasks)

    def test_new_roundless_pass_is_improve_and_hold_never_advances(self):
        row = self.review(self.add(round_name=None)); self.route(); self.reconcile()
        self.assertEqual(self.ledger.tasks.get(row['id'])['state'], 'round-complete')
        self.assertEqual(self.ledger.tasks.all()[-1]['request']['inputs']['submission_round'], 'sprint')
        self.review(self.add('held', 'uplift'), 'HOLD'); self.reconcile()
        self.assertEqual(len(self.ledger.tasks.all()), 3)

    def test_round_derivation_respects_pause_enable_and_shared_per_tick_budget(self):
        for number in range(3):
            self.review(self.add('paper-'+str(number)))
        self.cfg['paused'] = True; self.reconcile()
        self.assertEqual(len(self.ledger.tasks.all()), 3)
        self.cfg['paused'] = False; self.cfg['automation']['enabled'] = False; self.reconcile()
        self.assertEqual(len(self.ledger.tasks.all()), 3)
        self.cfg['automation'].update(enabled=True, max_new_tasks_per_tick=1)
        for total in [4, 5, 6, 6]:
            self.reconcile(); self.assertEqual(len(self.ledger.tasks.all()), total)

    def test_board_gate_accepts_legacy_and_stable_identity_and_retains_boundaries(self):
        value = self.add()['request']
        legacy = copy.deepcopy(value)
        legacy['inputs']['submission_source']['identity'] = digest(automation.paper_summary(self.paper))
        automation.paper_gate(legacy, self.root); automation.paper_gate(value, self.root)
        self.paper.update(stage='blocked', blocker='Awaiting review', version_date='2026-11-02',
                          target_journal='Another journal', title='A stronger theorem')
        self.board(); automation.paper_gate(value, self.root)
        with self.assertRaisesRegex(ValueError, 'board changed'):
            automation.paper_gate(legacy, self.root)
        for key, changed in [('authors',['Blaire Mercer']), ('workspace','../mail/elsewhere/paper')]:
            paper = {**self.paper, key:changed}; self.board([paper])
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, 'board changed'):
                automation.paper_gate(value, self.root)
        for papers in [[], [{**self.paper, 'stage':'submitted'}]]:
            self.board(papers)
            with self.assertRaisesRegex(ValueError, 'removed or already submitted'):
                automation.paper_gate(value, self.root)

    def test_staging_round_cli_defaults_choices_and_saved_inputs(self):
        canonical_root = self.root/'outreach'; (canonical_root/'.git').mkdir(parents=True)
        person = self.root/'mail/person/submission/example'; person.mkdir(parents=True)
        (person/'paper.tex').write_text('Scientific source')
        (self.root/'tables/contacts.json').write_text('{"contacts":[]}')
        (self.root/'managed').mkdir(); (self.root/'config.json').write_text('{}')
        selected = {}
        self.remote.ensure = lambda:None
        self.remote.repo = 'fixture/repo'
        self.remote.preserve_task = lambda path,tag:selected.update({(tag,path.name):path.read_bytes()})
        def download(*args):
            tag = args[2]; name = args[args.index('--pattern')+1]
            (Path(args[args.index('--dir')+1])/name).write_bytes(selected[tag,name])
        self.remote.run = download
        def git(args, **kwargs):
            return str(canonical_root/'.git')+'\n' if '--git-common-dir' in args else 'b'*40+'\n'
        receipts = []
        with patch('managed.control.ROOT', self.root), patch('managed.control.workflow', return_value='a'*64), \
             patch('managed.control.Releases', return_value=self.remote), \
             patch('managed.automation.subprocess.check_output', side_effect=git):
            for name in [None, 'consult', 'consult']:
                output = io.StringIO()
                with redirect_stdout(output):
                    control.main(['stage-submission', '--paper','example', '--file','paper.tex'] +
                                 (['--round',name] if name else []))
                receipts.append(json.loads(output.getvalue()))
            with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                control.main(['stage-submission', '--paper','example', '--file','paper.tex', '--round','skip'])
        self.assertNotEqual(receipts[0]['asset'], receipts[1]['asset'])
        self.assertEqual(receipts[1], receipts[2])
        for receipt, name in zip(receipts, ['improve','consult','consult']):
            value = json.loads(self.remote.assets[receipt['asset']])
            self.assertEqual(value['inputs']['submission_round'], name)
            self.assertEqual(value['inputs']['submission_source']['identity'], automation.paper_identity(self.paper))
            self.assertIn('relative_path', value['inputs']['selected_inputs'][0])
            automation.paper_gate(value, self.root)

    def test_submission_legacy_hash_and_prompt_uses_round_and_author_policy(self):
        paths = ['submission/workflow.json', 'submission/review.md', 'research/review.md',
                 'replies/workflow.json', 'automation.json', 'submission/author-policy.json', 'runtime.py']
        expected = digest({p:(control.ROOT/'managed'/p).read_text() for p in paths})
        self.assertEqual(control.legacy_workflow('submission'), expected)
        prompt = control.task_prompt(self.add(round_name='finalize'))
        for wording in ['inputs.submission_round (missing = improve)', 'rounds_policy, quality_bar',
                        'per-round\nnew-content ledger and Oracle identifiers', 'hold with the redirect and board proposal',
                        'locked_statement_without_lean', 'locked_statement_with_lean', 'verbatim',
                        "'private preparation draft' banners", 'leave license unselected']:
            self.assertIn(wording, prompt)
        self.assertNotIn('truthful placeholder', prompt)
        self.assertNotIn('marked provisional', prompt)


if __name__ == '__main__':
    unittest.main()


def setUpModule():
    use_example_config()
