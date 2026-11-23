"""Workflow changes replace only work that has never reached CMA or review."""
import copy
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from managed import automation, control, pipeline
from managed.control import Ledger, advance, ROOT
from managed.queue import digest, GateError
from managed.tasks import ACTIVE, WORKFLOW_CHANGED, WORKFLOW_PIN_MISMATCH
from test_tasks import CMA, Releases, request


class RepinTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.folder=Path(self.temp.name)
        self.remote=Releases();self.ledger=Ledger(self.remote,self.folder);self.cma=CMA()
        self.cfg={'paused':False,'kinds':{'research':True},'max_active':1,'agent_template':{}}
        self.current='b'*64
        self.committed={'a'*40: request()['workflow_hash']}

    def tearDown(self):
        self.ledger.tasks.close();self.temp.cleanup()

    def tick(self,revision='b'*40):
        if revision is not None:
            self.committed.setdefault(revision, self.current)
        def workflow(kind, source=None):
            return self.committed.get(source, self.current)
        with patch('managed.control.workflow',side_effect=workflow):
            advance(self.ledger,self.cma,self.cfg,self.folder,revision=revision)
        self.ledger.save(force=True)  # Match the production tick's final flush.

    def successor(self,old):
        old=self.ledger.tasks.get(old['id'])
        return self.ledger.tasks.get(old['data']['successor'])

    def test_queued_stale_request_is_replaced_without_starting_cma(self):
        value=request();value['inputs']={'owner_ids':['coauthor'],'scientific_revision':'c'*40}
        old=self.ledger.tasks.add(value);self.tick()
        new=self.successor(old)
        expected={**value,'id':'research-'+digest({'supersedes':old['id'],'workflow_hash':self.current,
                                                  'source_revision':'b'*40})[:32],
                  'source_revision':'b'*40,'workflow_hash':self.current}
        self.assertEqual(new['request'],expected);self.assertEqual(new['state'],'queued')
        old=self.ledger.tasks.get(old['id'])
        self.assertEqual(old['state'],'superseded')
        self.assertEqual(old['data'],{'reason':WORKFLOW_CHANGED,'successor':new['id']})
        self.assertEqual(old['request'],value);self.assertEqual(old['revision'],2)
        self.assertFalse(self.cma.creates)

    def test_default_revision_uses_the_same_repository_head_as_request_producers(self):
        old=self.ledger.tasks.add(request())
        with patch('managed.control.subprocess.check_output',return_value='c'*40+'\n') as git:
            self.tick(revision=None)
        git.assert_called_once_with(['git','rev-parse','HEAD'],cwd=ROOT,text=True)
        self.assertEqual(self.successor(old)['request']['source_revision'],'c'*40)

    def test_only_the_exact_legacy_workflow_hold_is_recovered(self):
        for name,reason in [('legacy',WORKFLOW_CHANGED),('other','Author timing hold')]:
            row=self.ledger.tasks.add(request(name))
            self.ledger.tasks.update(row['id'],row['revision'],'held',{'reason':reason})
        self.tick()
        self.assertEqual(self.ledger.tasks.get('legacy')['state'],'superseded')
        self.assertEqual(self.ledger.tasks.get('other')['state'],'held')
        self.assertEqual(len(self.ledger.tasks.all()),3)

    def test_cached_prompt_from_a_restart_does_not_pin_a_workflow_hold(self):
        old = self.ledger.tasks.add(request('restart'))
        evidence = [{'agent_id': 'retired-fixture', 'cleanup': {'deleted': True}}]
        self.ledger.tasks.update(old['id'], old['revision'], 'held', {
            'reason': WORKFLOW_CHANGED, 'prompt': 'Synthetic old prompt',
            'execution_restart': 'retry-1', 'execution_history': evidence})
        self.tick()
        successor = self.successor(old)
        self.assertEqual(successor['state'], 'queued')
        self.assertNotIn('prompt', successor['data'])
        self.assertEqual(self.ledger.tasks.get(old['id'])['data']['execution_history'], evidence)
        self.assertFalse(self.cma.creates)

    def test_handles_results_reviews_and_uncertain_intents_prevent_repin(self):
        for state in ['queued','held']:
            for key,value in [('agent_id','agt_original'),('operation_id','op_original'),
                              ('response_id','resp_original'),('result',{}),('review',{}),
                              ('result',None),('review',None),('create_key','original-key'),
                              ('command_key','original-key'),('create_body',{})]:
                with self.subTest(state=state,key=key,value=value):
                    name='protected-'+str(len(self.ledger.tasks.all()))
                    row=self.ledger.tasks.add(request(name))
                    data={key:value,'reason':WORKFLOW_CHANGED}
                    row=self.ledger.tasks.update(name,row['revision'],state,data)
                    self.tick()
                    current=self.ledger.tasks.get(name)
                    self.assertEqual(current['state'],'held')
                    self.assertEqual({k:current['data'][k] for k in data},data)
                    self.assertNotIn('successor',current['data'])
        self.assertFalse(any(r['state']=='superseded' for r in self.ledger.tasks.all()))
        self.assertFalse(self.cma.creates)

    def test_admitted_task_retains_original_response_across_workflow_change(self):
        row=self.ledger.tasks.add(request())
        row=self.ledger.tasks.update(row['id'],row['revision'],'running',
                                     {'agent_id':'agt_original','response_id':'resp_original'})
        self.tick();self.tick()
        self.assertEqual(self.ledger.tasks.all(),[row]);self.assertFalse(self.cma.creates)

    def test_two_ticks_and_snapshot_restore_do_not_create_another_successor(self):
        blocker=self.ledger.tasks.add(request('busy','another-owner'))
        self.ledger.tasks.update(blocker['id'],blocker['revision'],'running',{'response_id':'resp_busy'})
        old=self.ledger.tasks.add(request());self.tick();new=self.successor(old)
        before=copy.deepcopy(self.ledger.tasks.all())
        self.ledger.tasks.close();self.folder=self.folder/'restored';self.folder.mkdir()
        self.ledger=Ledger(self.remote,self.folder)
        self.tick(revision='c'*40)
        self.assertEqual(self.ledger.tasks.all(),before)
        self.assertEqual(self.ledger.tasks.get(new['id'])['state'],'queued')
        self.assertFalse(self.cma.creates)

    def test_successor_can_be_repinned_again_and_only_latest_consumes_capacity(self):
        old=self.ledger.tasks.add(request());self.tick();first=self.successor(old)
        self.current='c'*64;self.tick(revision='c'*40);last=self.successor(first)
        self.assertEqual(last['request']['source_revision'],'c'*40)
        self.assertEqual(last['request']['inputs'],old['request']['inputs'])
        self.assertEqual(last['state'],'queued');self.assertNotEqual(first['id'],last['id'])
        self.tick(revision='d'*40)
        rows=self.ledger.tasks.all()
        self.assertEqual([r['state'] for r in rows],['superseded','superseded','running'])
        self.assertEqual(sum(r['state'] in ACTIVE for r in rows),1)
        self.assertEqual(len(self.cma.creates),1)

    def test_pause_and_disabled_kind_leave_stale_work_unchanged(self):
        row=self.ledger.tasks.add(request())
        for settings in [{'paused':True},{'kinds':{'research':False}}]:
            with self.subTest(settings=settings):
                original=self.cfg;self.cfg={**original,**settings}
                try:self.tick()
                finally:self.cfg=original
                self.assertEqual(self.ledger.tasks.all(),[row])

    def test_repinned_task_still_waits_for_shared_coauthor(self):
        self.cfg['max_active']=4
        blocker=request('busy','another-owner');blocker['inputs']['owner_ids']=['shared']
        row=self.ledger.tasks.add(blocker)
        self.ledger.tasks.update(row['id'],row['revision'],'running',{'response_id':'resp_busy'})
        value=request();value['inputs']['owner_ids']=['shared']
        old=self.ledger.tasks.add(value);self.tick();new=self.successor(old);self.tick()
        self.assertEqual(self.ledger.tasks.get(new['id'])['state'],'queued')
        self.assertFalse(self.cma.creates)

    def test_stale_writer_cannot_create_successor_or_change_source_link(self):
        row=self.ledger.tasks.add(request())
        self.ledger.tasks.db.execute('INSERT INTO pipeline_sources VALUES(?,?,?,?,?)',
                                     ('result','identity','{}',0,row['id']))
        self.ledger.tasks.update(row['id'],row['revision'],'held',{'reason':'Actual decision'})
        with self.assertRaisesRegex(GateError,'Stale task writer'):
            self.ledger.tasks.repin(row['id'],row['revision'],self.current,'b'*40)
        self.assertEqual(len(self.ledger.tasks.all()),1)
        self.assertEqual(self.ledger.tasks.db.execute('SELECT task_id FROM pipeline_sources').fetchone()[0],row['id'])

    def test_failure_inside_repin_rolls_back_tasks_events_and_source_link(self):
        row=self.ledger.tasks.add(request());tasks=self.ledger.tasks
        tasks.db.execute('INSERT INTO pipeline_sources VALUES(?,?,?,?,?)',('result','identity','{}',0,row['id']))
        tasks.db.execute("""CREATE TRIGGER reject_relink BEFORE UPDATE ON pipeline_sources
                            BEGIN SELECT RAISE(ABORT,'relink failed'); END""")
        with self.assertRaisesRegex(sqlite3.IntegrityError,'relink failed'):self.tick()
        self.assertEqual(tasks.all(),[row])
        self.assertEqual(tasks.db.execute('SELECT COUNT(*) FROM task_events').fetchone()[0],1)
        self.assertEqual(tasks.db.execute('SELECT task_id FROM pipeline_sources').fetchone()[0],row['id'])

    def test_failed_snapshot_commit_replays_repin_from_prior_snapshot(self):
        row=self.ledger.tasks.add(request());self.ledger.save();self.remote.fail_pointer=True
        with self.assertRaises(TimeoutError):self.tick()
        successor=self.successor(row)
        self.ledger.tasks.close();self.folder=self.folder/'restored';self.folder.mkdir()
        self.ledger=Ledger(self.remote,self.folder);self.remote.fail_pointer=False
        self.assertEqual(self.ledger.tasks.all(),[row]);self.tick()
        replay=self.successor(row)
        self.assertEqual(replay['request'],successor['request'])
        self.assertEqual(replay['request_hash'],successor['request_hash'])
        self.assertEqual(replay['state'],'queued')

    def test_repinned_registered_decision_continuation_restores_parent_registration(self):
        parent = request('decision-parent', 'coauthor', 'reply')
        parent['inputs'] = {'automation_contract': 1}
        parent = self.ledger.tasks.add(parent)
        result = {'outcome': 'needs-decision', 'packet': None}
        parent = self.ledger.tasks.update(parent['id'], parent['revision'], 'superseded',
                                          {'result': result, 'continuation_id': 'reply-old'})
        child = request('reply-old', 'coauthor', 'reply')
        child['workflow_hash'] = 'a' * 64
        child['inputs'] = {'automation_contract': 1,
                           'user_answers': [{'answers': {'coauthorship': '同意'},
                                             'evidence_ref': 'fixture:answer'},
                                            ],
                           'automation_parent': {'task_id': parent['id'],
                                                 'result_hash': digest(result),
                                                 'purpose': 'user-answer'}}
        child = self.ledger.tasks.add(child)
        successor = self.ledger.tasks.repin(child['id'], child['revision'], 'b' * 64, 'b' * 40)
        current_parent = self.ledger.tasks.get(parent['id'])
        self.assertEqual(current_parent['state'], 'revising')
        self.assertEqual(current_parent['data']['continuation_id'], successor['id'])
        automation.parent_gate(successor['request'], self.ledger.tasks)
        self.assertEqual(successor['request']['inputs']['user_answers'][0]['evidence_ref'], 'fixture:answer')


class WorkflowPinTests(unittest.TestCase):
    def setUp(self):
        self.folder = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.root = self.folder / 'repo'
        self.root.mkdir()
        self.git('init', '-q', '-b', 'main')
        for path in ('firstcontact/workflow.json', 'firstcontact/review.md', 'runtime.py',
                     'automation.json'):
            target = self.root / 'managed' / path
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / 'managed' / path, target)
        shutil.copyfile(ROOT / 'managed/config.example.json', self.root / 'config.json')
        self.old = self.commit('Old workflow')
        self.enterContext(patch.object(control, 'ROOT', self.root))
        self.enterContext(patch.object(control, 'ENGINE_ROOT', self.root))
        self.old_hash = control.workflow('firstcontact', self.old)
        voice = self.root / 'managed/replies/voice.md'
        voice.parent.mkdir()
        shutil.copyfile(ROOT / 'managed/replies/voice.md', voice)
        policy = self.root / 'managed/firstcontact/workflow.json'
        policy.write_text(policy.read_text() + '\n')
        self.main = self.commit('New workflow')
        self.current_hash = control.workflow('firstcontact')
        self.assertNotEqual(self.old_hash, self.current_hash)
        self.remote = Releases()
        self.remote.ensure = lambda: None
        self.ledger = Ledger(self.remote, self.folder)
        self.addCleanup(self.ledger.tasks.close)
        self.cma = CMA()
        self.config = {'paused': False, 'kinds': {'firstcontact': True}, 'max_active': 1}

    def git(self, *args):
        return subprocess.check_output(['git', *args], cwd=self.root, text=True).strip()

    def commit(self, message):
        self.git('add', 'managed')
        self.git('-c', 'user.email=test@example.invalid', '-c', 'user.name=Test',
                 '-c', 'commit.gpgsign=false', 'commit', '-qm', message)
        return self.git('rev-parse', 'HEAD')

    def request(self, identity, source, **overrides):
        value = pipeline.request('firstcontact', identity, 'contact-a', {'selection': identity}, source)
        return {**value, **overrides}

    def tick(self):
        advance(self.ledger, self.cma, self.config, self.folder, revision=self.main)

    def successor(self, row):
        old = self.ledger.tasks.get(row['id'])
        self.assertEqual(old['state'], 'superseded')
        self.assertEqual(old['data']['reason'], WORKFLOW_PIN_MISMATCH)
        successor = self.ledger.tasks.get(old['data']['successor'])
        self.assertEqual(successor['request']['source_revision'], self.main)
        self.assertEqual(successor['request']['workflow_hash'], self.current_hash)
        self.assertEqual(successor['request']['inputs'], row['request']['inputs'])
        return successor

    def test_request_builder_plan_and_enqueue_hash_committed_workflow(self):
        self.assertEqual(self.request('fresh', self.old)['workflow_hash'], self.old_hash)
        policy = self.root / 'managed/firstcontact/review.md'
        policy.write_text(policy.read_text() + '\nUncommitted review text\n')
        self.assertNotEqual(control.workflow('firstcontact'), self.current_hash)
        inputs = self.folder / 'inputs.json'
        inputs.write_text('{}')
        with patch.object(control, 'Releases', return_value=self.remote), patch.object(control, 'firstcontact_gate'):
            with redirect_stdout(io.StringIO()) as out:
                control.main(['plan', 'firstcontact', '--id', 'planned', '--owner', 'contact-a',
                              '--inputs', str(inputs)])
            value = json.loads(out.getvalue())
            self.assertEqual(value['source_revision'], self.main)
            self.assertEqual(value['workflow_hash'], self.current_hash)
            staged = self.folder / 'request.json'
            staged.write_text(json.dumps({**value, 'source_revision': self.old}))
            with self.assertRaisesRegex(GateError, 'does not match the pinned source revision'):
                control.main(['enqueue', str(staged)])
            self.assertFalse(self.remote.assets)
            staged.write_text(json.dumps({**value, 'source_revision': self.old, 'workflow_hash': self.old_hash}))
            with redirect_stdout(io.StringIO()):
                control.main(['enqueue', str(staged)])
            self.assertEqual(len(self.remote.assets), 1)

    def test_queued_and_agent_held_mismatch_repin_idempotently_with_cleanup(self):
        waiting = self.ledger.tasks.add(self.request('waiting', self.old, workflow_hash=self.current_hash))
        held = self.ledger.tasks.add(self.request('held', self.old, workflow_hash=self.current_hash))
        result = {'outcome': 'hold', 'report': 'Policy-identity mismatch: different digests',
                  'packet': None, 'artifacts': []}
        self.ledger.tasks.update(held['id'], held['revision'], 'held',
                                 {'agent_id': 'original-agent', 'response_id': 'original-response', 'result': result})
        ordinary = self.ledger.tasks.add(self.request('ordinary', self.old, workflow_hash=self.current_hash))
        self.ledger.tasks.update(ordinary['id'], ordinary['revision'], 'held', {'reason': 'Author timing hold'})
        protected = self.ledger.tasks.add(self.request('reviewed', self.old, workflow_hash=self.current_hash))
        protected = self.ledger.tasks.update(protected['id'], protected['revision'], 'held',
                                             {'result': result, 'review': {'verdict': 'HOLD'}})
        self.tick()
        self.assertEqual(self.successor(waiting)['state'], 'queued')
        self.assertEqual(self.successor(held)['state'], 'queued')
        self.assertEqual(self.successor(ordinary)['data']['reason'], 'Author timing hold')
        self.assertEqual(self.successor(ordinary)['state'], 'held')
        self.assertEqual(self.ledger.tasks.get(protected['id']), protected)
        self.assertEqual(self.ledger.tasks.get(held['id'])['data']['result'], result)
        self.assertFalse(self.cma.creates)
        task_ids = [row['id'] for row in self.ledger.tasks.all()]
        blocker = self.ledger.tasks.add(self.request('blocker', self.main))
        self.ledger.tasks.update(blocker['id'], blocker['revision'], 'running', {'response_id': 'blocker'})
        self.tick()
        self.assertEqual([row['id'] for row in self.ledger.tasks.all()], [*task_ids, blocker['id']])
        self.assertEqual(self.cma.retired, ['original-agent'])

    def test_shallow_checkout_fetches_old_pin_and_repairs_at_most_eight(self):
        shallow = self.folder / 'checkout'
        self.git('clone', '-q', '--depth=1', self.root.as_uri(), str(shallow))
        rows = [self.ledger.tasks.add(self.request(i, self.old, workflow_hash=self.current_hash))
                for i in range(control.WORKFLOW_REPIN_LIMIT + 1)]
        self.remote.fetch_workflow_revision = control.Releases().fetch_workflow_revision
        with patch.object(control, 'ENGINE_ROOT', shallow), patch.object(control, 'ROOT', shallow), \
                patch.object(self.remote, 'fetch_workflow_revision', wraps=self.remote.fetch_workflow_revision) as fetch:
            self.tick()
        fetch.assert_called_once_with(self.old)
        for row in rows[:-1]:
            self.assertEqual(self.successor(row)['state'], 'queued')
        self.assertEqual(self.ledger.tasks.get(rows[-1]['id'])['state'], 'queued')
        self.assertFalse(self.cma.creates)


    def test_unreadable_old_pin_is_repinned_instead_of_starving(self):
        shallow = self.folder / 'checkout-unreadable'
        self.git('clone', '-q', '--depth=1', self.root.as_uri(), str(shallow))
        rows = [self.ledger.tasks.add(self.request('old-%d' % i, self.old, workflow_hash=self.current_hash))
                for i in range(3)]
        self.remote.fetch_workflow_revision = lambda revision: False
        with patch.object(control, 'ENGINE_ROOT', shallow), patch.object(control, 'ROOT', shallow):
            self.tick()
        for row in rows:
            successor = self.successor(row)
            self.assertEqual(successor['state'], 'queued')
            self.assertEqual(successor['request']['source_revision'], self.main)
            self.assertEqual(self.ledger.tasks.get(row['id'])['state'], 'superseded')

class SplitWorkflowPinTests(unittest.TestCase):
    def setUp(self):
        self.folder = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.engine = self.folder / 'engine'
        self.data = self.folder / 'data'
        self.paths = ('automation.json', 'runtime.py', 'research/workflow.json', 'research/review.md')
        for root in (self.engine, self.data):
            root.mkdir()
            self.git(root, 'init', '-q')
            for path in self.paths:
                self.write(root / 'managed' / path, (ROOT / 'managed' / path).read_bytes())
        self.engine_pin = self.commit(self.engine)
        self.historical_pin = self.commit(self.data)
        for path in self.paths:
            if path.endswith('.py'):
                continue
            raw = (self.data / 'managed' / path).read_bytes()
            self.write(self.data / 'policy/managed' / path, raw + b'\n')
        self.policy_pin = self.commit(self.data)
        self.enterContext(patch.object(control, 'ENGINE_ROOT', self.engine))
        self.enterContext(patch.object(control, 'ROOT', self.data))
        self.enterContext(patch.dict('os.environ', {'OUTREACH_DATA_ROOT': str(self.data), 'GITHUB_ACTIONS': 'false'}))
        self.cma = CMA()
        self.ledger = Ledger(Releases(), self.folder)
        self.addCleanup(self.ledger.tasks.close)
        self.config = {'paused': False, 'kinds': {'research': True}, 'max_active': 1}

    @staticmethod
    def git(root, *args):
        return subprocess.check_output(['git', *args], cwd=root, text=True).strip()

    @staticmethod
    def write(path, raw):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)

    def commit(self, root):
        self.git(root, 'add', '-A')
        self.git(root, '-c', 'user.email=test@example.invalid', '-c', 'user.name=Test',
                 '-c', 'commit.gpgsign=false', 'commit', '-qm', f'Synthetic {root.name} workflow fixture')
        return self.git(root, 'rev-parse', 'HEAD')

    def request(self):
        return pipeline.request('research', 'split-fixture', 'fixture-owner', {}, self.engine_pin)

    def test_binds_resolved_policy_and_engine_bytes(self):
        value = self.request()
        self.assertEqual(value['policy_revision'], self.policy_pin)
        control.verify_request_workflow(value)
        self.assertEqual(value['workflow_hash'], control.workflow('research'))
        historical = control.workflow('research', self.historical_pin)
        self.assertNotEqual(value['workflow_hash'], historical)
        with self.assertRaises(ValueError):
            control.workflow('research', self.engine_pin)
        with self.assertRaisesRegex(ValueError, 'Workflow commit unavailable'):
            control.workflow('daily', self.historical_pin, self.policy_pin)

        overlay = self.data / 'policy/managed/research/review.md'
        self.assertEqual(control.workflow_text('research/review.md').encode(), overlay.read_bytes())
        self.write(self.engine / 'managed/research/review.md', b'Unused public reference change')
        self.assertEqual(control.workflow('research'), value['workflow_hash'])
        self.write(overlay, overlay.read_bytes() + b'\r\nNew private review rule.\r\n')
        self.assertNotEqual(control.workflow('research'), value['workflow_hash'])
        self.assertNotEqual(control.workflow_text('research/review.md'),
                            control.workflow_text('research/review.md', self.engine_pin, self.policy_pin))
        control.verify_request_workflow(value)

    def test_reference_fallback_is_for_offline_runs(self):
        overlay = self.data / 'policy/managed/research/review.md'
        overlay.unlink()
        incomplete = self.commit(self.data)
        expected = (self.engine / 'managed/research/review.md').read_text()
        self.assertEqual(control.workflow_text('research/review.md'), expected)
        self.assertEqual(control.workflow_text('research/review.md', self.engine_pin, incomplete), expected)
        historical = control.workflow('research', self.historical_pin)
        with patch.dict('os.environ', {'GITHUB_ACTIONS': 'true', 'OUTREACH_PUBLIC_LOG': '0'}):
            from managed.policy import MissingPolicy
            from managed.run_log import public_run
            for pins in ((), (self.engine_pin, incomplete)):
                with self.assertRaises(MissingPolicy):
                    control.workflow('research', *pins)
            from contextlib import redirect_stderr
            with redirect_stderr(io.StringIO()) as public:
                self.assertEqual(public_run(lambda: control.workflow('research'), 'control', persist=False), 1)
            self.assertIn('control failed', public.getvalue())
            self.assertNotIn('research/review.md', public.getvalue())
            self.assertEqual(control.workflow('research', self.historical_pin),
                             historical)

    def assert_repin(self, change_engine):
        value = self.request()
        old = self.ledger.tasks.add(value)
        row = self.ledger.tasks.add({**value, 'id': 'already-reviewed'})
        result = {'outcome': 'completed', 'report': 'Checked fixture'}
        review = {'verdict': 'PASS', 'binding': {'task_id': row['id'],
                  'workflow_hash': value['workflow_hash'], 'result_hash': digest(result)}}
        row = self.ledger.tasks.update(row['id'], row['revision'], 'approved',
                                       {'result': result, 'review': review})
        self.assertTrue(control.matches_workflow(row, control.workflow('research')))
        if change_engine:
            self.write(self.engine / 'managed/runtime.py', b'# Changed bound engine source\n')
            self.engine_pin = self.commit(self.engine)
        else:
            self.write(self.data / 'policy/managed/research/review.md', b'Changed private review policy\n')
            self.policy_pin = self.commit(self.data)
        self.assertFalse(control.matches_workflow(row, control.workflow('research')))
        advance(self.ledger, self.cma, self.config, self.folder, revision=self.engine_pin)
        prior = self.ledger.tasks.get(old['id'])
        successor = self.ledger.tasks.get(prior['data']['successor'])['request']
        self.assertEqual(prior['state'], 'superseded')
        self.assertEqual(successor['source_revision'], self.engine_pin)
        self.assertEqual(successor['policy_revision'], self.policy_pin)
        self.assertNotEqual(successor['workflow_hash'], value['workflow_hash'])
        control.verify_request_workflow(successor)
        self.assertEqual(self.ledger.tasks.get(row['id'])['request'], row['request'])
        self.assertFalse(self.cma.creates)

    def test_engine_change_repins_queued_work_and_stales_review(self):
        self.assert_repin(change_engine=True)

    def test_policy_change_repins_queued_work_and_stales_review(self):
        self.assert_repin(change_engine=False)


if __name__=='__main__':unittest.main()
