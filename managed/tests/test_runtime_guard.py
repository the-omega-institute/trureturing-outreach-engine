import hashlib
import json
import unittest
from io import BytesIO
from urllib.error import HTTPError
from managed import automation, control
from managed.cma import CMA, CMARequestError
from unittest.mock import patch
from test_packet_artifact import PacketReleases
from test_sandbox_pool import PoolFixture
from test_tasks import use_fixture_workflow_revisions


class RobustnessTests(PoolFixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.cfg.pop('sandbox_pool')
        self.calls=[]
        self.cma.controls=lambda a,r: {'agent_revision':len(self.calls)+1,
            'target':{'response_id':r,'turn_sequence':1}}
        self.cma.control=lambda a,r,b,k: self.calls.append((b.copy(),k)) or {'command_id':k}

    def running(self):
        row=self.queue('guard')
        return self.ledger.tasks.update(row['id'],row['revision'],'running',{
            'agent_id':'agt_test','response_id':'resp_test','prompt':'original',
            'response_started_at':self.now-9*3600})

    def guard(self,row,tokens=0):
        return control.runtime_guard(self.ledger,self.cma,row,
            {'status':'in_progress','usage':{'input_tokens':tokens}},self.cfg)

    def test_steer_then_cancel_and_operator_failure(self):
        row=self.guard(self.running())
        command=self.calls[0][0]['command']
        self.assertEqual(command['type'],'steer')
        for requirement in ('deliverables/', 'agent_path', 'bytes', 'SHA-256', 'role "checkpoint"',
                            'outcome=hold', 'one archive', 'manifest of member paths',
                            'Loose checkpoint files are not accepted'):
            self.assertIn(requirement,command['text'])
        row=self.guard(row)
        self.assertEqual(len(self.calls),1)
        self.now+=7200
        row=self.guard(row)
        self.assertEqual(self.calls[-1][0]['command']['type'],'cancel')
        row=control.recover_execution_failure(self.ledger,row,
            {'status':'cancelled','error':{'code':'rate_limit_exceeded'}},self.cfg)
        self.assertEqual(row['state'],'failed')
        self.assertEqual(row['data']['recovery']['error_class'],'runtime-guard')

    def artifact_result(self, row, outcome):
        remote=PacketReleases()
        self.ledger.releases=remote
        raw=b'Checked partial proof and exact certificate.'
        reference={'release_tag':'outreach-task-'+row['id'], 'name':'certificate.txt',
                   'bytes':len(raw), 'sha256':hashlib.sha256(raw).hexdigest(),
                   'role':'checkpoint' if outcome=='hold' else 'evidence'}
        agent_path='deliverables/certificate.txt'
        def read(agent_id,path,size,sha256):
            self.assertEqual((agent_id,path,size,sha256),(row['data']['agent_id'],agent_path,len(raw),reference['sha256']))
            remote.calls.append('CMA read')
            return raw
        self.cma.artifact=read
        result={'schema_version':1,'task_id':row['id'],'outcome':outcome,'report':'Checked certificate.',
                'artifacts':[{**reference,'agent_path':agent_path}],'packet':None}
        self.cma.response=lambda _: {'status':'completed','output':[{'type':'message','content':[
            {'type':'output_text','text':json.dumps(result)}]}]}
        retire=self.cma.retire
        def retire_after_readback(*args,**kwargs):
            remote.calls.append('retire')
            return retire(*args,**kwargs)
        self.cma.retire=retire_after_readback
        return remote, reference, raw

    def test_guard_checkpoint_is_read_back_before_retirement_and_passed_to_continuation(self):
        use_fixture_workflow_revisions(self)
        row=self.guard(self.running())
        remote,reference,raw=self.artifact_result(row,'hold')
        self.tick()
        parent=self.ledger.tasks.get(row['id'])
        self.assertEqual(parent['state'],'held')
        self.assertEqual(parent['data']['result']['artifacts'],[reference])
        self.assertEqual(remote.files,{(reference['release_tag'],reference['name']):raw})
        self.assertEqual(remote.calls,['CMA read','preserve','readback','retire'])
        self.assertTrue(parent['data']['cleanup']['deleted'])

        self.cfg['automation']={'enabled':True}
        automation.reconcile(self.ledger,self.cfg,self.folder,revision='b'*40)
        parent=self.ledger.tasks.get(row['id'])
        child=self.ledger.tasks.get(parent['data']['continuation_id'])
        inputs=child['request']['inputs']
        self.assertEqual(inputs['checkpoint_artifacts'],[reference])
        self.assertEqual(inputs['previous_result'],parent['data']['result'])
        self.assertEqual(inputs['review_feedback']['verdict'],'CONTINUE')
        reason=inputs['review_feedback']['reason']
        self.assertIn('Restore inputs.checkpoint_artifacts into this worktree',reason)
        self.assertIn('GitHub Releases API',reason)
        self.assertIn('SHA-256',reason)
        self.assertIn('Rebuild any missing or unverifiable files',reason)
        automation.parent_gate(child['request'],self.ledger.tasks)
        self.assertEqual(automation.work(self.ledger.tasks,self.folder)['guard_continuations_without_checkpoint'],[])

    def test_failed_checkpoint_readback_is_reported_without_retirement(self):
        row=self.guard(self.running())
        row=self.ledger.tasks.update(row['id'],row['revision'],'running',
            {**row['data'],'repair_attempts':control.RESULT_REPAIR_LIMIT})
        remote,_,_=self.artifact_result(row,'hold')
        remote.corrupt=True
        self.tick()
        row=self.ledger.tasks.get(row['id'])
        self.assertEqual(row['state'],'held')
        self.assertEqual(row['data']['repair_error'],'Artifact readback failed')
        self.assertNotIn('result',row['data'])
        self.assertEqual(remote.calls,['CMA read','preserve','readback'])
        self.assertFalse(self.cma.retire_calls)
        self.cfg['automation']={'enabled':True}
        automation.reconcile(self.ledger,self.cfg,self.folder,revision='b'*40)
        self.assertEqual(len(self.ledger.tasks.all()),1)
        follow_up=automation.work(self.ledger.tasks,self.folder)['follow_up']
        self.assertIn('Artifact readback failed',follow_up[0]['reason'])

    def test_completed_result_is_still_preserved_and_awaits_review(self):
        row=self.running()
        remote,reference,raw=self.artifact_result(row,'completed')
        self.tick()
        row=self.ledger.tasks.get(row['id'])
        self.assertEqual(row['state'],'awaiting-review')
        self.assertEqual(row['data']['result']['artifacts'],[reference])
        self.assertEqual(remote.files,{(reference['release_tag'],reference['name']):raw})
        self.assertEqual(remote.calls,['CMA read','preserve','readback','retire'])
        self.assertTrue(row['data']['cleanup']['deleted'])

    def test_token_limits(self):
        row=self.running();row['data']['response_started_at']=self.now
        row=self.guard(row,5000000)
        row=self.guard(row,7000000)
        self.assertEqual([b['command']['type'] for b,k in self.calls],['steer','cancel'])

    def test_conflict_reobserves_with_new_keys(self):
        def post(a,r,b,k):
            self.calls.append((b.copy(),k))
            if len(self.calls)<5:raise CMARequestError(409,'response_control_conflict')
            return {'command_id':'ok'}
        self.cma.control=post
        row=self.guard(self.running())
        self.assertEqual(len(self.calls),5)
        self.assertEqual(len({k for b,k in self.calls}),5)
        self.assertEqual(self.calls[-1][0]['expected_agent_revision'],5)
        self.assertIn('receipt',row['data']['runtime_guard']['steer'])

    def test_uncertain_post_reuses_exact_intent_after_restore(self):
        def post(a,r,b,k):
            self.calls.append((b.copy(),k));raise TimeoutError('lost')
        self.cma.control=post
        row=self.guard(self.running())
        self.restore();row=self.ledger.tasks.get(row['id'])
        self.cma.controls=lambda *a:self.fail('Must replay original observation')
        self.guard(row)
        self.assertEqual(self.calls[0],self.calls[1])

    def test_admission_rejections_bounded_and_retired(self):
        row=self.running()
        row=self.ledger.tasks.update(row['id'],row['revision'],'admitting',row['data'])
        for generation in range(3):
            for tick in range(3):
                exc=HTTPError('https://example.test',422,'refused',{},BytesIO(b'rejection body'))
                row=control.admission_rejected(self.ledger,self.cma,row,exc)
                if tick<2:self.assertEqual(row['state'],'admission-unknown')
            if generation<2:
                self.assertEqual(row['state'],'queued')
                self.assertNotIn('create_key',row['data'])
                row=self.ledger.tasks.update(row['id'],row['revision'],'admitting',{
                    **row['data'],'agent_id':'agt_'+str(generation),'prompt':'original'})
        self.assertEqual(row['state'],'failed')
        self.assertEqual(row['data']['recovery']['error_class'],'admission-rejected')
        self.assertEqual(len(self.cma.retire_calls),3)
        self.assertEqual(row['data']['admission_errors'][-1],{'status':422,'body':'rejection body'})

    def test_legacy_oversize_rejection_gets_offload_requeue_after_old_budget(self):
        row=self.running()
        row=self.ledger.tasks.update(row['id'],row['revision'],'admitting',{
            **row['data'],'prompt':'x'*130000,'admission_rejections':2,
            'admission_rejection_ticks':3})
        row=control.admission_rejected(self.ledger,self.cma,row,None)
        self.assertEqual(row['state'],'queued')
        self.assertEqual(row['data']['admission_rejections'],3)

    def test_operator_large_requeue_is_idempotent(self):
        row=self.running()
        row=self.ledger.tasks.update(row['id'],row['revision'],'failed',{
            **row['data'],'recovery':{'error_class':'request-too-large'}})
        value={'task_id':row['id'],'request_hash':row['request_hash']}
        control.requeue_large_task(self.ledger,value)
        queued=self.ledger.tasks.get(row['id'])
        self.assertEqual(queued['state'],'queued')
        self.assertEqual(queued['request'],row['request'])
        self.assertEqual(queued['data']['execution_history'][-1],row['data'])
        control.requeue_large_task(self.ledger,value)
        self.assertEqual(self.ledger.tasks.get(row['id'])['revision'],queued['revision'])

    def test_operator_requeue_restarts_retry_exhausted_transient_failure(self):
        row=self.running()
        row=self.ledger.tasks.update(row['id'],row['revision'],'failed',{
            **row['data'],'recovery':{'error_class':'rate-limit','status':'retry-scheduled'}})
        with self.assertRaises(ValueError):
            control.requeue_large_task(self.ledger,{'task_id':row['id'],'request_hash':row['request_hash'],'try':1})
        row=self.ledger.tasks.update(row['id'],row['revision'],'failed',{
            **row['data'],'recovery':{'error_class':'rate-limit','status':'needs-operator','attempts':5}})
        control.requeue_large_task(self.ledger,{'task_id':row['id'],'request_hash':row['request_hash']})
        queued=self.ledger.tasks.get(row['id'])
        self.assertEqual(queued['state'],'queued')
        self.assertTrue(queued['data']['execution_restart'].startswith('operator-'))
        self.assertNotIn('agent_id',queued['data'])

    def test_operator_requeue_accepts_exhausted_sandbox_failure(self):
        row=self.running()
        row=self.ledger.tasks.update(row['id'],row['revision'],'failed',{
            **row['data'],'recovery':{'error_class':'sandbox-failed','status':'needs-operator','attempts':2}})
        control.requeue_large_task(self.ledger,{'task_id':row['id'],'request_hash':row['request_hash']})
        queued=self.ledger.tasks.get(row['id'])
        self.assertEqual(queued['state'],'queued')
        self.assertTrue(queued['data']['execution_restart'].startswith('operator-'))

    def test_operator_requeue_repeats_after_a_new_failure(self):
        row=self.running()
        for attempt in range(2):
            row=self.ledger.tasks.get(row['id'])
            row=self.ledger.tasks.update(row['id'],row['revision'],'failed',{
                **row['data'],'recovery':{'error_class':'sandbox-failed','status':'needs-operator','attempts':2}})
            control.requeue_large_task(self.ledger,{'task_id':row['id'],'request_hash':row['request_hash'],
                                                   'failed_revision':row['revision']})
            self.assertEqual(self.ledger.tasks.get(row['id'])['state'],'queued')
        restarts={h.get('execution_restart') for h in self.ledger.tasks.get(row['id'])['data']['execution_history']}
        self.assertEqual(len(restarts-{None}),1)

    def test_control_paths_and_body(self):
        cma=CMA(token='test-token')
        body={'turn_sequence':2,'expected_agent_revision':4,'command':{'type':'cancel'}}
        with patch.object(cma,'request',return_value={}) as request:
            cma.controls('agt_test','resp_test')
            request.assert_called_with('/api/v2/agents/agt_test/responses/resp_test/controls')
            cma.control('agt_test','resp_test',body,'original-key')
            request.assert_called_with('/api/v2/agents/agt_test/responses/resp_test/controls','POST',body,'original-key')

    def test_transient_admission_stays_uncertain(self):
        for status in (409,429,503):
            row=self.queue('transient'+str(status))
            self.ledger.tasks.update(row['id'],row['revision'],'admitting',{
                'agent_id':'agt_transient','prompt':'original','command_key':str(status)})
        self.cma.admit=lambda *a: (_ for _ in ()).throw(CMARequestError(int(a[2]),'unavailable'))
        self.tick();self.tick()
        for row in self.ledger.tasks.all():
            self.assertEqual(row['state'],'admission-unknown')
            self.assertNotIn('admission_rejection_ticks',row['data'])
        self.assertFalse(self.cma.retire_calls)

class SandboxFailureTests(PoolFixture, unittest.TestCase):
    def failed_task(self, state='running', count=0, legacy=False):
        entry=self.seed_pool()[0]
        row=self.queue('broken')
        data={'agent_id':'agt_broken','response_id':'resp_broken','create_key':'old-key',
              'sandbox_failures':count,'workflow_binding':{'test':'binding'},'credit_checked':True,
              ('owned_sandbox_id' if legacy else 'pool_sandbox_id'):entry['sandbox_id']}
        self.cma.statuses[entry['sandbox_id']]={'status':'failed','failure':{'class':'terminal_validation'}}
        if legacy:
            self.ledger.tasks.db.execute("DELETE FROM pipeline_state WHERE key='sandbox_pool'")
        return self.ledger.tasks.update(row['id'],row['revision'],state,data),entry

    def test_requeue_new_key_and_preserve_request_binding_credit(self):
        row,entry=self.failed_task()
        original=row['request']
        self.tick()
        row=self.ledger.tasks.get(row['id'])
        self.assertEqual(row['state'],'queued')
        self.assertEqual(row['request'],original)
        self.assertEqual(row['data']['sandbox_failures'],1)
        self.assertEqual(row['data']['workflow_binding'],{'test':'binding'})
        self.assertTrue(row['data']['credit_checked'])
        self.assertTrue(self.cma.retire_calls)
        self.assertEqual(self.state(),[])
        self.tick()
        row=self.ledger.tasks.get(row['id'])
        self.assertIn('-sandbox1',row['data']['create_key'])
        self.assertNotEqual(row['data']['create_key'],'old-key')
        self.assertTrue(self.state('sandbox_pool_cleanup')[0]['cleanup_intent'])

    def test_bound_reached(self):
        row,_=self.failed_task(count=2)
        self.tick();row=self.ledger.tasks.get(row['id'])
        self.assertEqual(row['state'],'failed')
        self.assertEqual(row['data']['error_class'],'sandbox-failed')
        self.assertEqual(row['data']['sandbox_failures'],2)

    def test_admission_provisioning_and_legacy(self):
        for state in ('admitting','admission-unknown','provisioning','provision-unknown'):
            with self.subTest(state=state):
                row,entry=self.failed_task(state=state,legacy=True)
                self.tick();row=self.ledger.tasks.get(row['id'])
                self.assertEqual(row['state'],'queued')
                self.assertEqual(self.state('sandbox_pool_cleanup')[0]['sandbox_id'],entry['sandbox_id'])
                self.ledger.tasks.db.execute('DELETE FROM tasks')

    def test_verified_result_never_requeued(self):
        row,_=self.failed_task()
        row=self.ledger.tasks.update(row['id'],row['revision'],'running',{**row['data'],'result':{'verified':True}})
        self.assertFalse(control.sandbox_failure(self.ledger,self.cma,row))
        self.assertFalse(self.cma.retire_calls)

    def test_agent_failed_even_in_healthy_sandbox(self):
        row,entry=self.failed_task()
        self.cma.statuses[entry['sandbox_id']]={'status':'active'}
        base=self.cma.request
        self.cma.request=lambda path,*args: ({'status':'failed'} if path=='/api/v1/agents/agt_broken' else base(path,*args))
        self.tick();self.assertEqual(self.ledger.tasks.get(row['id'])['state'],'queued')
        self.assertEqual(len(self.state()),1)

    def test_retire_records_refusals_and_attempts_delete(self):
        cma=CMA(token='fake');calls=[]
        def request(path,method='GET',body=None,key=None):
            calls.append((path,method))
            if method!='GET':raise CMARequestError(409,'runtime rejected lifecycle')
            return {'status':'failed'}
        cma.request=request
        result=cma.retire('agt_broken','key')
        self.assertEqual([e['action'] for e in result['errors']],['stop','delete'])
        self.assertFalse(result['deleted'])

    def test_sandbox_cleanup_preserves_any_live_agent_and_extra_workspaces(self):
        cma=CMA(token='fake');calls=[]
        for agents,workspaces in [([{'id':'agt_foreign','status':'active'}],[{}]),
                                  ([{'id':'agt_ours','status':'failed'}],[{}]),([], [{},{}]),([], [{}])]:
            def request(path,method='GET',body=None,key=None):
                calls.append((path,method))
                if '/agents?' in path:return {'items':agents}
                if path.endswith('/workspaces'):return {'items':workspaces}
                return {'status':'failed'}
            cma.request=request;calls.clear()
            result=cma.retire_sandbox(None,'sbx_broken','key',{})
            self.assertEqual(any(m=='DELETE' for p,m in calls),not agents and len(workspaces)==1)

    def test_failed_pool_sandbox_cleanup_ignores_only_settled_agents(self):
        cma=CMA(token='fake');calls=[]
        for agents,deleted in [([{'id':'agt_dead','status':'failed'},{'id':'agt_off','status':'stopped'}],False),
                               ([{'id':'agt_dead','status':'failed'},{'id':'agt_live','status':'ready'}],False)]:
            def request(path,method='GET',body=None,key=None):
                calls.append((path,method))
                if '/agents?' in path:return {'items':agents}
                if path.endswith('/workspaces'):return {'items':[{}]}
                return {'status':'active'}
            cma.request=request;calls.clear()
            cma.retire_sandbox(None,'sbx_corrupt','key',{},dead_ok=True)
            self.assertEqual(any(m=='DELETE' for p,m in calls),deleted)

    def test_corrupt_runtime_agent_drains_its_active_sandbox(self):
        row,entry=self.failed_task()
        self.cma.statuses[entry['sandbox_id']]={'status':'active'}
        corrupt={'status':'failed','failure':{'class':'agent_runtime_rejected',
                 'detail':'mutation_journal_corrupt: runtime mutation journal contains too many entries'}}
        base=self.cma.request
        self.cma.request=lambda path,*args: (corrupt if path=='/api/v1/agents/agt_broken' else base(path,*args))
        live=self.occupy(entry,9,state='running')
        self.tick()
        self.assertEqual(self.ledger.tasks.get(row['id'])['state'],'queued')
        self.assertEqual(self.state(),[])
        cleanup=self.state('sandbox_pool_cleanup')
        self.assertIn('mutation_journal_corrupt',cleanup[0]['corrupt'])
        self.assertEqual(self.ledger.tasks.get(row['id'])['data']['sandbox_failure_history'][-1]['failure']['class'],
                         'agent_runtime_rejected')
        # The live Agent keeps the corrupt Sandbox; it is not reused or deleted yet.
        self.assertFalse(any(m=='DELETE' for _,m,_ in self.cma.calls))
        live=self.ledger.tasks.get(live['id'])
        self.ledger.tasks.update(live['id'],live['revision'],'failed',{**live['data'],'cleanup':{'deleted':True}})
        self.now+=1;self.tick()
        self.assertTrue(any(m=='DELETE' and entry['sandbox_id'] in p for p,m,_ in self.cma.calls))
        self.assertEqual(self.state('sandbox_pool_cleanup')[0]['sandbox_id'],entry['sandbox_id'])

    def test_dead_sandbox_ignores_idle_agents_of_finished_ledger_executions(self):
        cma=CMA(token='fake');calls=[]
        pages={None:{'items':[{'id':'agt_dead','status':'deleted'}],'next':'agt_dead'},
               'agt_dead':{'items':[{'id':'agt_ours','status':'stopped'}]}}
        for finished,deleted in [(frozenset({'agt_ours'}),True),(frozenset(),False)]:
            pages['agt_dead']['items'][0]['status']='stopped' if finished else 'ready'
            def request(path,method='GET',body=None,key=None):
                calls.append((path,method))
                if '/agents?' in path:
                    return pages['agt_dead' if 'cursor=agt_dead' in path else None]
                if path.endswith('/workspaces'):return {'items':[{}]}
                return {'status':'active'}
            cma.request=request;calls.clear()
            cma.retire_sandbox(None,'sbx_corrupt','key',{},dead_ok=True,finished=finished)
            self.assertEqual(any(m=='DELETE' for p,m in calls),deleted)
        # Without dead_ok a second page still retains the Sandbox.
        calls.clear();cma.retire_sandbox(None,'sbx_corrupt','key',{},finished=frozenset({'agt_ours'}))
        self.assertFalse(any(m=='DELETE' for p,m in calls))

    def test_ledger_agents_collects_current_and_history(self):
        from managed.sandbox_pool import ledger_agents
        rows=[{'data':{'agent_id':'a1','execution_history':[{'agent_id':'a2','cleanup':{'agent_id':'a3'}}],
                       'sandbox_failure_history':[{'agent_id':'a4'}],
                       'admission_history':[{'agent_id':'a5'}],
                       'credit_recovery':{'agent_id':'a6'}}}]
        self.assertEqual(ledger_agents(rows),{'a1','a2','a3','a4','a5','a6'})

    def test_operator_requeue_accepts_inactive_sandbox_provisioning_failure(self):
        row=self.queue('inactive')
        row=self.ledger.tasks.update(row['id'],row['revision'],'failed',{
            'operation_id':'op_x','operation_error':'Agent lifecycle request conflicts with existing state: sandbox is not active',
            'capacity_checked':True,'runtime_checked':True})
        control.requeue_large_task(self.ledger,{'task_id':row['id'],'request_hash':row['request_hash'],'failed_revision':row['revision']})
        self.assertEqual(self.ledger.tasks.get(row['id'])['state'],'queued')
