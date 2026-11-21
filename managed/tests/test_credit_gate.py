"""Billing outages pause admission, retain capacity, and recover legacy failures."""
import json
import unittest
from unittest.mock import patch

from managed.automation import work
from managed.control import CREDIT_RETRIES, CreditGate, credit_failure
from managed.queue import GateError
from managed.sandbox_pool import SandboxPool, live_reference
from managed.tasks import ACTIVE
from test_sandbox_pool import PoolFixture


CREDIT_ERROR={'code':'agent_turn_failed','message':
    'unexpected status 402 Payment Required: {"error":"insufficient_credits",'
    '"error_code":11300,"message":"Insufficient billing credits"}, url: https://example.invalid'}


class CreditGateTests(PoolFixture,unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.cma.response=lambda _: {'status':'failed','error':CREDIT_ERROR}

    def running(self,name,entry=None,**extra):
        row=self.queue(name)
        data={'agent_id':'agt_'+name,'response_id':'resp_'+name,
              'prompt':'original prompt '+name,'command_key':'original-key-'+name,**extra}
        if entry:data.update(pool_sandbox_id=entry['sandbox_id'],workspace_id=entry['workspace_id'])
        return self.ledger.tasks.update(name,row['revision'],'running',data)

    def legacy(self,name,deleted=False,cached=True,**extra):
        row=self.running(name,**extra)
        data={**row['data'],'reason':'Original CMA response ended failed','cleanup':{'deleted':deleted}}
        if cached:data['response_error']=CREDIT_ERROR
        return self.ledger.tasks.update(name,row['revision'],'failed',data)

    def completed(self,name):
        result={'schema_version':1,'task_id':name,'outcome':'completed',
                'report':'Checked result.','artifacts':[],'packet':None}
        return {'status':'completed','output':[{'type':'message',
            'content':[{'type':'output_text','text':json.dumps(result)}]}]}

    def test_detector_requires_agent_turn_code_and_either_billing_marker(self):
        for message in [CREDIT_ERROR['message'],'insufficient_credits','402 Payment Required']:
            self.assertTrue(credit_failure({'error':{'code':'agent_turn_failed','message':message}}))
        for error in [None,'insufficient_credits',{'code':'permission_denied','message':'402 Payment Required'},
                      {'code':'agent_turn_failed','message':'tool failed'}]:
            self.assertFalse(credit_failure({'error':error}))

    def test_credit_failure_waits_retains_agent_and_stops_all_later_admissions(self):
        row=self.running('outage',transport_retries=2,capacity_retries=3)
        queued=self.queue('new')
        pending=self.running('pending')
        self.ledger.tasks.update(pending['id'],pending['revision'],'admitting',pending['data'])
        self.tick();waiting=self.ledger.tasks.get(row['id'])
        self.assertEqual(waiting['state'],'credit-wait');self.assertIn(waiting['state'],ACTIVE)
        self.assertEqual(waiting['data']['agent_id'],row['data']['agent_id'])
        self.assertEqual(waiting['data']['credit_retries'],1)
        self.assertEqual(waiting['data']['transport_retries'],2)
        self.assertEqual(waiting['data']['capacity_retries'],3)
        self.assertEqual(waiting['data']['response_error'],CREDIT_ERROR)
        self.assertEqual(self.ledger.tasks.get(queued['id']),queued)
        self.assertFalse(self.cma.creates);self.assertFalse(self.cma.admissions);self.assertFalse(self.cma.retired)
        self.assertIsNone(self.state('credit_block')['probe_task'])

    def test_only_oldest_waiting_is_probed_once_each_tick_across_restore(self):
        for name in ['oldest','second','third']:self.running(name)
        self.tick();since=self.state('credit_block')['since'];self.restore()
        for attempt in range(1,5):
            before=len(self.cma.admissions);self.tick()
            self.assertEqual(len(self.cma.admissions),before+1)
            agent,prompt,key=self.cma.admissions[-1]
            self.assertEqual((agent,prompt),('agt_oldest','original prompt oldest'))
            self.assertTrue(key.endswith('-credit-'+str(attempt)))
            self.assertEqual(self.state('credit_block')['since'],since)
            self.assertIsNotNone(self.state('credit_block')['last_probe_at'])
            self.assertEqual(self.ledger.tasks.get('second')['data']['credit_retries'],1)
        self.assertFalse(self.cma.creates);self.assertFalse(self.cma.retired)

    def test_in_progress_probe_prevents_further_probes_until_terminal_response(self):
        self.running('oldest');self.running('second');self.tick()
        self.cma.response=lambda _: {'status':'in_progress'}
        self.tick();self.assertEqual(self.state('credit_block')['probe_task'],'oldest')
        self.restore();self.tick();self.tick()
        self.assertEqual(len(self.cma.admissions),1)
        self.assertEqual(self.ledger.tasks.get('second')['state'],'credit-wait')
        self.assertEqual(self.ledger.tasks.get('oldest')['state'],'running')

    def test_long_in_progress_probe_opens_the_gate(self):
        self.running('oldest');self.running('second');self.tick()
        self.cma.response=lambda _: {'status':'in_progress'}
        self.tick();self.assertEqual(self.state('credit_block')['probe_task'],'oldest')
        self.now+=10*60;self.tick()
        self.assertTrue(self.state('credit_block'))  # still within the acceptance window
        self.now+=6*60;self.tick()
        self.assertFalse(self.state('credit_block'))
        self.assertEqual(self.ledger.tasks.get('oldest')['state'],'running')

    def test_successful_probe_clears_block_and_resumes_waiting_and_fresh_work(self):
        self.running('oldest');self.running('second');queued=self.queue('fresh');self.tick()
        first_key=self.ledger.tasks.get('oldest')['data']['credit_commands'][-1]['command_key']
        self.cma.response=lambda rid: self.completed('oldest') if rid=='resp_'+first_key else {'status':'in_progress'}
        self.tick()
        self.assertFalse(self.state('credit_block'))
        self.assertEqual(self.ledger.tasks.get('oldest')['state'],'awaiting-review')
        self.assertEqual(self.ledger.tasks.get('second')['state'],'running')
        self.assertEqual(self.ledger.tasks.get(queued['id'])['state'],'running')
        self.assertEqual(len(self.cma.admissions),3)
        self.assertEqual(self.cma.retired,['agt_oldest'])

    def test_blocked_gate_with_only_queued_tasks_starts_one_probe_then_resumes_after_success(self):
        queued=[self.queue(name) for name in ['first','second','third']]
        CreditGate(self.ledger,self.cfg).observe('deleted-agent')
        self.assertTrue(all(row['state']=='queued' for row in self.ledger.tasks.all()))
        self.cma.response=lambda _: {'status':'in_progress'}
        self.tick()
        probe=self.ledger.tasks.get('first')
        self.assertEqual(probe['state'],'running')
        self.assertEqual(self.state('credit_block')['probe_task'],probe['id'])
        self.assertEqual(len(self.cma.creates),1);self.assertEqual(len(self.cma.admissions),1)
        self.assertEqual(self.cma.admissions[0][0],probe['data']['agent_id'])
        for row in queued[1:]:self.assertEqual(self.ledger.tasks.get(row['id']),row)

        self.restore()
        self.cma.response=lambda rid: self.completed('first') if rid==probe['data']['response_id'] else {'status':'in_progress'}
        self.tick()
        self.assertFalse(self.state('credit_block'))
        self.assertEqual(self.ledger.tasks.get('first')['state'],'awaiting-review')
        for row in queued[1:]:self.assertEqual(self.ledger.tasks.get(row['id'])['state'],'running')
        self.assertEqual(len(self.cma.creates),3);self.assertEqual(len(self.cma.admissions),3)

    def test_queued_probe_credit_failure_retries_its_agent_before_other_queued_tasks(self):
        queued=[self.queue(name) for name in ['first','second','third']]
        CreditGate(self.ledger,self.cfg).observe('deleted-agent')
        since=self.state('credit_block')['since']
        self.assertTrue(all(row['state']=='queued' for row in self.ledger.tasks.all()))
        self.tick()
        waiting=self.ledger.tasks.get('first')
        self.assertEqual(waiting['state'],'credit-wait')
        self.assertEqual(waiting['data']['credit_retries'],1)
        self.assertEqual(waiting['data']['response_error'],CREDIT_ERROR)
        self.assertEqual(self.state('credit_block')['since'],since)
        self.assertIsNone(self.state('credit_block')['probe_task'])
        self.assertEqual(len(self.cma.creates),1);self.assertEqual(len(self.cma.admissions),1)
        self.assertEqual(self.cma.admissions[0][0],waiting['data']['agent_id'])
        for row in queued[1:]:self.assertEqual(self.ledger.tasks.get(row['id']),row)
        self.assertFalse(self.cma.retired)

        self.restore();self.cma.response=lambda _: {'status':'in_progress'};self.tick()
        self.assertEqual(self.ledger.tasks.get('first')['state'],'running')
        self.assertEqual(self.state('credit_block')['probe_task'],'first')
        self.assertEqual(self.state('credit_block')['since'],since)
        self.assertEqual(len(self.cma.creates),1);self.assertEqual(len(self.cma.admissions),2)
        self.assertEqual(self.cma.admissions[-1][:2],self.cma.admissions[0][:2])
        self.assertEqual(self.cma.admissions[-1][2],waiting['data']['credit_commands'][-1]['command_key'])
        self.assertTrue(self.cma.admissions[-1][2].endswith('-credit-1'))
        for row in queued[1:]:self.assertEqual(self.ledger.tasks.get(row['id']),row)
        self.assertFalse(self.cma.retired)

    def test_non_credit_probe_failure_opens_gate_and_keeps_transport_budget_separate(self):
        self.running('oldest');self.running('second');self.tick()
        error={'code':'agent_turn_failed','message':'stream disconnected'}
        self.cma.response=lambda _: {'status':'failed','error':error}
        self.tick()
        self.assertFalse(self.state('credit_block'))
        row=self.ledger.tasks.get('oldest')
        self.assertEqual(row['state'],'admitting');self.assertEqual(row['data']['transport_retries'],1)
        self.assertEqual(row['data']['credit_retries'],1)
        self.assertEqual(row['data']['transport_commands'][-1]['prompt'],'original prompt oldest')

    def test_later_credit_failure_keeps_tick_closed_after_probe_success(self):
        self.running('probe');self.running('other');self.queue('fresh');self.tick()
        self.cma.response=lambda _: {'status':'in_progress'}
        self.tick()  # Leave the original probe running across ticks.
        other=self.ledger.tasks.get('other')
        self.ledger.tasks.update('other',other['revision'],'running',other['data'])
        self.cma.response=lambda rid: self.completed('probe') if '-credit-' in rid else {'status':'failed','error':CREDIT_ERROR}
        self.tick()
        self.assertTrue(self.state('credit_block'))
        self.assertEqual(self.ledger.tasks.get('fresh')['state'],'queued')

    def test_recovery_uses_cached_error_and_does_not_repeat_after_restore(self):
        row=self.legacy('legacy',transport_retries=2,capacity_retries=4)
        calls=[]
        self.cma.response=lambda rid: calls.append(rid) or {'status':'in_progress'}
        self.tick();recovered=self.ledger.tasks.get(row['id'])
        self.assertEqual(recovered['state'],'credit-wait');self.assertTrue(recovered['data']['credit_recovered'])
        self.assertFalse(calls);self.assertFalse(self.cma.retired)
        self.restore();self.tick();self.tick()
        recovered=self.ledger.tasks.get(row['id'])
        self.assertEqual(recovered['state'],'running');self.assertEqual(recovered['data']['credit_retries'],1)
        self.assertEqual(recovered['data']['transport_retries'],2)
        self.assertEqual(recovered['data']['capacity_retries'],4)
        self.assertEqual(len(self.cma.admissions),1)

    def test_recovery_fetches_missing_error_once_and_caches_it(self):
        self.legacy('legacy',cached=False)
        calls=[]
        self.cma.response=lambda rid: calls.append(rid) or {'status':'failed','error':CREDIT_ERROR}
        self.tick();self.assertEqual(calls,['resp_legacy'])
        self.assertEqual(self.ledger.tasks.get('legacy')['data']['response_error'],CREDIT_ERROR)
        self.restore();self.cma.response=lambda rid: calls.append(rid) or {'status':'in_progress'}
        self.tick();self.tick();self.assertEqual(calls.count('resp_legacy'),1)

    def test_retired_legacy_agent_requeues_inputs_and_restarts_under_gate_with_new_keys(self):
        row=self.legacy('retired',deleted=True,capacity_retries=4,transport_retries=2,
                        create_key='old-create',owned_sandbox_id='sbx_old')
        fresh=self.queue('fresh');self.tick();recovered=self.ledger.tasks.get(row['id'])
        self.assertEqual(recovered['state'],'queued');self.assertEqual(recovered['request'],row['request'])
        self.assertNotIn('agent_id',recovered['data']);self.assertTrue(recovered['data']['credit_waiting'])
        self.assertEqual(recovered['data']['credit_recovery']['agent_id'],'agt_retired')
        self.assertFalse(self.cma.creates)
        self.restore();self.cma.response=lambda _: {'status':'in_progress'};self.tick()
        restarted=self.ledger.tasks.get(row['id'])
        self.assertEqual(restarted['state'],'running')
        self.assertEqual(restarted['request'],row['request'])
        self.assertEqual(restarted['data']['credit_recovery'],recovered['data']['credit_recovery'])
        self.assertNotEqual(restarted['data']['agent_id'],'agt_retired')
        self.assertNotEqual(restarted['data']['create_key'],'old-create')
        self.assertTrue(restarted['data']['create_key'].endswith('-credit-recovery'))
        self.assertTrue(restarted['data']['command_key'].endswith('-credit-recovery'))
        self.assertEqual(restarted['data']['capacity_retries'],4)
        self.assertEqual(restarted['data']['transport_retries'],2)
        self.assertEqual(self.state('credit_block')['probe_task'],row['id'])
        self.assertEqual(self.ledger.tasks.get(fresh['id']),fresh)
        self.assertEqual(len(self.cma.creates),1);self.assertEqual(len(self.cma.admissions),1)

        self.cma.response=lambda rid: self.completed(row['id']) if rid==restarted['data']['response_id'] else {'status':'in_progress'}
        self.tick()
        self.assertFalse(self.state('credit_block'))
        self.assertEqual(self.ledger.tasks.get(row['id'])['state'],'awaiting-review')
        self.assertEqual(self.ledger.tasks.get(fresh['id'])['state'],'running')
        self.assertEqual(len(self.cma.creates),2);self.assertEqual(len(self.cma.admissions),2)

    def test_non_credit_legacy_failure_is_checked_once_and_cleans_up(self):
        self.legacy('other',cached=False)
        calls=[]
        self.cma.response=lambda rid: calls.append(rid) or {'status':'failed','error':{'code':'tool_error'}}
        self.tick();self.restore();self.tick()
        self.assertEqual(calls,['resp_other']);self.assertFalse(self.state('credit_block'))
        self.assertEqual(self.ledger.tasks.get('other')['state'],'failed')
        self.assertEqual(self.cma.retired,['agt_other'])

    def test_retired_recovery_waiting_for_full_pool_does_not_starve_existing_agent_probe(self):
        entry=self.seed_pool()[0]
        self.cfg['sandbox_pool']={'max_sandboxes':1,'agents_per_sandbox':1}
        self.legacy('retired',deleted=True)
        self.running('alive',entry)
        self.tick();self.cma.response=lambda _: {'status':'in_progress'};self.tick()
        self.assertEqual(self.cma.admissions[0][0],'agt_alive')
        self.assertEqual(self.ledger.tasks.get('retired')['state'],'queued')
        self.assertEqual(self.state('credit_block')['probe_task'],'alive')
        self.assertFalse(self.cma.creates)

    def test_retired_recovery_retains_waiting_marker_and_resumes_after_repin_and_live_probe(self):
        row=self.legacy('retired',deleted=True,transport_retries=2)
        self.running('survivor')
        self.tick();self.cma.response=lambda _: {'status':'in_progress'}
        with patch('managed.control.workflow',return_value='f'*64):
            self.tick()
            old=self.ledger.tasks.get(row['id']);self.assertEqual(old['state'],'superseded')
            successor=self.ledger.tasks.get(old['data']['successor'])
            self.assertTrue(successor['data']['credit_waiting'])
            self.assertEqual(successor['request']['inputs'],row['request']['inputs'])
            self.cma.response=lambda rid: self.completed('survivor') if rid.endswith('-credit-1') else {'status':'in_progress'}
            self.tick()
        self.assertEqual(self.ledger.tasks.get(successor['id'])['state'],'running')
        self.assertFalse(self.state('credit_block'))
        self.assertEqual(self.ledger.tasks.get(successor['id'])['data']['transport_retries'],2)

    def test_recovery_read_failure_preserves_agent_and_retries_lookup(self):
        self.legacy('unknown',cached=False)
        def unavailable(_):raise TimeoutError('CMA unavailable')
        self.cma.response=unavailable;self.tick()
        self.assertFalse(self.cma.retired);self.assertNotIn('credit_checked',self.ledger.tasks.get('unknown')['data'])
        self.cma.response=lambda _: {'status':'failed','error':CREDIT_ERROR}
        self.tick();self.assertEqual(self.ledger.tasks.get('unknown')['state'],'credit-wait')

    def test_waiting_reserves_active_owner_and_pool_slot_until_completion(self):
        entry=self.seed_pool()[0]
        self.cfg['sandbox_pool']={'max_sandboxes':1,'agents_per_sandbox':1}
        self.running('waiting',entry);self.tick()
        waiting=self.ledger.tasks.get('waiting')
        self.assertTrue(live_reference(waiting,entry))
        pool=SandboxPool(self.ledger,self.cfg['sandbox_pool']);pool.available.add(entry['sandbox_id'])
        self.assertEqual(pool.slot(),(False,None))
        other=self.queue('other')
        with self.assertRaises(GateError):self.ledger.tasks.claim(other['id'],1)
        same=self.ledger.tasks.add({**other['request'],'id':'same-owner','owner':waiting['owner']})
        with self.assertRaises(GateError):self.ledger.tasks.claim(same['id'],24)
        for row in [other,same]:
            self.ledger.tasks.update(row['id'],row['revision'],'held',{'reason':'Fixture: leave released slot free'})
        self.cma.response=lambda _: self.completed('waiting');self.tick()
        self.assertFalse(live_reference(self.ledger.tasks.get('waiting'),entry))
        self.assertEqual(pool.slot(),(True,{**entry,'agents_placed':1}))

    def test_paused_or_disabled_kind_preserves_waiting_without_probing(self):
        self.running('waiting');self.tick()
        self.cfg['paused']=True;self.tick();self.assertFalse(self.cma.admissions)
        self.cfg['paused']=False;self.cfg['kinds']['research']=False
        self.tick();self.assertFalse(self.cma.admissions)
        self.assertEqual(self.ledger.tasks.get('waiting')['state'],'credit-wait')

    def test_uncertain_probe_checkpoint_replays_same_key_after_restore(self):
        self.running('waiting');self.tick()
        admit=self.cma.admit
        def lost(agent,prompt,key):
            result=admit(agent,prompt,key)
            self.assertEqual(self.state('credit_block')['probe_task'],'waiting')
            raise TimeoutError('Admission receipt lost')
        self.cma.admit=lost;self.tick()
        self.assertEqual(self.ledger.tasks.get('waiting')['state'],'admission-unknown')
        self.now=self.ledger.tasks.get('waiting')['data']['recovery']['next_at']
        self.restore();self.cma.admit=admit;self.cma.response=lambda _: {'status':'in_progress'};self.tick()
        self.assertEqual(self.cma.admissions[-1],self.cma.admissions[-2])
        self.assertEqual(len(self.cma.commands),1)
        self.assertEqual(self.ledger.tasks.get('waiting')['data']['credit_retries'],1)

    def test_credit_retry_preserves_correction_prompt_and_transport_prompt(self):
        for name,commands in [('repair','repairs'),('transport','transport_commands')]:
            self.running(name,**{commands:[{'response_id':'resp_'+name,'prompt':'exact '+name+' prompt'}]})
        self.tick();self.cma.response=lambda _: {'status':'in_progress'};self.tick()
        self.assertEqual(self.cma.admissions[0][:2],('agt_repair','exact repair prompt'))
        self.cma.response=lambda _: self.completed('repair');self.tick()
        self.assertEqual(self.cma.admissions[1][:2],('agt_transport','exact transport prompt'))

    def test_credit_limit_keeps_task_waiting_and_does_not_retire_agent(self):
        self.running('exhausted',credit_retries=CREDIT_RETRIES);self.tick();self.tick()
        row=self.ledger.tasks.get('exhausted')
        self.assertEqual(row['state'],'credit-wait');self.assertTrue(row['data']['credit_retry_exhausted'])
        self.assertEqual(row['data']['credit_retries'],CREDIT_RETRIES)
        self.assertFalse(self.cma.admissions);self.assertFalse(self.cma.retired)

    def test_work_surfaces_block_since_and_waiting_count_including_retired_recovery(self):
        self.running('waiting');self.legacy('retired',deleted=True);self.tick()
        with patch('managed.daily_local.status',return_value={}):snapshot=work(self.ledger.tasks,self.folder)
        self.assertEqual(snapshot['credit_block']['since'],self.state('credit_block')['since'])
        self.assertEqual(snapshot['credit_block']['waiting'],2)
        self.assertEqual(snapshot['reviews'],[])


if __name__=='__main__':unittest.main()
