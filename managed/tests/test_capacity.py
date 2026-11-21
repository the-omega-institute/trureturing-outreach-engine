"""CMA sandbox-capacity failures create no Agent and are retried, not terminal."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from managed.automation import work

from managed.control import Ledger, advance, CAPACITY_RETRIES, RUNTIME_RETRIES, ROOT
from test_tasks import CMA, Releases, request


class FullCMA(CMA):
    def __init__(self):super().__init__();self.full=True;self.ops={};self.polled=[]
    def create(self,body,key):
        self.creates.setdefault(key,body);op='op-%d'%len(self.creates);self.ops[op]=self.full;return {'id':op}
    def operation(self,operation_id):
        self.polled.append(operation_id)
        if self.ops.get(operation_id):
            return {'status':'failed','error':'Agent lifecycle request conflicts with existing state: sandbox limit reached: 10 of 10'}
        return {'status':'succeeded','agent_id':'agt_test'}


class RuntimeCMA(FullCMA):
    def operation(self,operation_id):
        self.polled.append(operation_id)
        return {'status':'failed','error':
                'Agent lifecycle request conflicts with existing state: Agent Runtime Preparation Failed'}


class CapacityTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.folder=Path(self.temp.name)
        self.ledger=Ledger(Releases(),self.folder);self.cma=FullCMA()
        self.cfg={'paused':False,'kinds':{'research':True},'max_active':2,'agent_template':{}}
    def tearDown(self):self.ledger.tasks.close();self.temp.cleanup()
    def tick(self):advance(self.ledger,self.cma,self.cfg,self.folder,revision='b'*40)

    def test_capacity_failure_requeues_with_a_new_key_then_starts(self):
        self.ledger.tasks.add(request());self.tick()
        row=self.ledger.tasks.get('job-1')
        self.assertEqual(row['state'],'queued');self.assertEqual(row['data']['capacity_retries'],1)
        self.cma.full=False;self.tick();self.tick()
        row=self.ledger.tasks.get('job-1')
        self.assertIn(row['state'],{'admitting','running'})
        keys=list(self.cma.creates)
        self.assertEqual(len(keys),2);self.assertNotEqual(keys[0],keys[1]);self.assertTrue(keys[1].endswith('-r1'))
        self.assertEqual(row['data']['previous_operations'][0]['create_key'],keys[0])

    def test_retries_are_bounded(self):
        self.ledger.tasks.add(request())
        for _ in range(CAPACITY_RETRIES+2):self.tick()
        self.assertEqual(self.ledger.tasks.get('job-1')['state'],'failed')

    def test_capacity_failure_stops_new_provisioning_but_advances_running(self):
        queued=[]
        for i,retries in enumerate([0,3,0],1):
            row=self.ledger.tasks.add(request('job-%d'%i,'scholar-%d'%i))
            queued.append(self.ledger.tasks.update(row['id'],row['revision'],'queued',{'capacity_retries':retries}))
        row=self.ledger.tasks.add(request('running','other-scholar'))
        self.ledger.tasks.update(row['id'],row['revision'],'running',
            {'agent_id':'agt_running','response_id':'resp_running','owned_sandbox_id':'sbx_running'})
        result={'schema_version':1,'task_id':'running','outcome':'completed',
                'report':'checked','artifacts':[],'packet':None}
        self.cma.response=lambda _: {'status':'completed','output':[{'type':'message',
            'content':[{'type':'output_text','text':json.dumps(result)}]}]}
        self.tick()
        first=self.ledger.tasks.get('job-1')
        self.assertEqual(first['state'],'queued');self.assertEqual(first['data']['capacity_retries'],1)
        self.assertEqual([body['name'] for body in self.cma.creates.values()],['outreach-job-1'])
        for row in queued[1:]:self.assertEqual(self.ledger.tasks.get(row['id']),row)
        running=self.ledger.tasks.get('running')
        self.assertEqual(running['state'],'awaiting-review');self.assertEqual(running['data']['result'],result)
        self.assertTrue(running['data']['cleanup']['sandbox_retired']);self.assertEqual(self.cma.retired,['agt_running'])

    def test_failed_capacity_recheck_stops_new_work_but_polls_existing_work(self):
        failed=[]
        for i in range(2):
            row=self.ledger.tasks.add(request('failed-%d'%i,'scholar-%d'%i))
            self.cma.ops[row['id']]=True
            failed.append(self.ledger.tasks.update(row['id'],row['revision'],'failed',
                {'create_key':row['id'],'operation_id':row['id'],
                 'reason':'CMA provisioning failed; retained original operation'}))
        row=self.ledger.tasks.add(request('existing','other-scholar'))
        self.cma.ops['existing-op']=True
        self.ledger.tasks.update(row['id'],row['revision'],'provisioning',
            {'create_key':'existing-key','operation_id':'existing-op'})
        queued=self.ledger.tasks.add(request('queued','queued-scholar'))
        self.tick()
        self.assertEqual(self.cma.polled,['failed-0','existing-op']);self.assertFalse(self.cma.creates)
        self.assertEqual(self.ledger.tasks.get('failed-0')['data']['capacity_retries'],1)
        self.assertEqual(self.ledger.tasks.get('failed-1'),failed[1])
        existing=self.ledger.tasks.get('existing')
        self.assertEqual(existing['state'],'queued');self.assertEqual(existing['data']['capacity_retries'],1)
        self.assertEqual(self.ledger.tasks.get('queued'),queued)

    def test_capacity_failure_at_retry_limit_still_stops_new_provisioning(self):
        row=self.ledger.tasks.add(request())
        self.ledger.tasks.update(row['id'],row['revision'],'queued',{'capacity_retries':CAPACITY_RETRIES})
        queued=self.ledger.tasks.add(request('job-2','scholar-b'))
        self.tick()
        self.assertEqual(self.ledger.tasks.get('job-1')['state'],'failed')
        self.assertEqual(len(self.cma.creates),1)
        self.assertEqual(self.ledger.tasks.get('job-2'),queued)

    def test_existing_capacity_failure_is_recovered_once(self):
        row=self.ledger.tasks.add(request());self.cma.ops['old-op']=True
        self.ledger.tasks.update(row['id'],row['revision'],'failed',
            {'create_key':'k','operation_id':'old-op','reason':'CMA provisioning failed; retained original operation'})
        self.tick()
        self.assertEqual(self.ledger.tasks.get('job-1')['state'] in {'queued','provisioning'},True)

    def test_runtime_retries_are_separate_bounded_and_use_new_keys(self):
        self.cma=RuntimeCMA()
        row=self.ledger.tasks.add(request())
        self.ledger.tasks.update(row['id'],row['revision'],'queued',{'capacity_retries':2})
        for attempt in range(1,RUNTIME_RETRIES+1):
            self.tick()
            row=self.ledger.tasks.get('job-1')
            self.assertEqual(row['state'],'queued')
            self.assertEqual(row['data']['runtime_retries'],attempt)
            self.assertEqual(row['data']['capacity_retries'],2)
            self.assertEqual(len(row['data']['previous_operations']),attempt)
        self.tick();self.tick()
        row=self.ledger.tasks.get('job-1')
        self.assertEqual(row['state'],'failed')
        self.assertTrue(row['data']['capacity_checked'])
        self.assertEqual(len(self.cma.creates),RUNTIME_RETRIES+1)
        self.assertEqual(len(self.cma.polled),RUNTIME_RETRIES+1)
        self.assertTrue(list(self.cma.creates)[-1].endswith('-runtime3'))

    def test_runtime_failure_does_not_stop_other_provisioning(self):
        self.cma=RuntimeCMA()
        self.ledger.tasks.add(request())
        self.ledger.tasks.add(request('job-2','other-scholar'))
        self.tick()
        self.assertEqual(len(self.cma.creates),2)
        self.assertEqual(self.ledger.tasks.get('job-2')['data']['runtime_retries'],1)

    def test_checked_runtime_failure_is_read_and_requeued_once(self):
        self.cma=RuntimeCMA();row=self.ledger.tasks.add(request())
        self.ledger.tasks.update(row['id'],row['revision'],'failed',
            {'create_key':'k','operation_id':'old-op','capacity_checked':True,
             'reason':'CMA provisioning failed; retained original operation'})
        self.tick()
        row=self.ledger.tasks.get('job-1')
        self.assertEqual(row['state'],'queued')
        self.assertEqual(row['data']['runtime_retries'],1)
        self.assertEqual(self.cma.polled,['old-op'])
        self.tick()
        self.assertEqual(self.cma.polled.count('old-op'),1)

    def test_runtime_failure_inventory_exit_depends_on_remaining_retries(self):
        row=self.ledger.tasks.add(request())
        for attempts,exit in [(1,'retry-later'),(RUNTIME_RETRIES,'rewrite-task')]:
            row=self.ledger.tasks.update(row['id'],row['revision'],'failed',
                {'operation_error':'Agent runtime preparation failed','runtime_retries':attempts})
            with patch('managed.daily_local.status',return_value={}):
                item=work(self.ledger.tasks,ROOT)['failed_unrecovered'][0]
            self.assertEqual(item['error_class'],'runtime-preparation')
            self.assertEqual(item['attempts'],attempts)
            self.assertEqual(item['suggested_exit'],exit)

    def test_other_provisioning_failures_stay_failed_and_are_checked_once(self):
        class Broken(CMA):
            def __init__(self):super().__init__();self.calls=0
            def operation(self,operation_id):self.calls+=1;return {'status':'failed','error':'image pull failed'}
        self.cma=Broken();row=self.ledger.tasks.add(request())
        self.ledger.tasks.update(row['id'],row['revision'],'failed',
            {'create_key':'k','operation_id':'old-op','reason':'CMA provisioning failed; retained original operation'})
        self.tick();self.tick()
        self.assertEqual(self.ledger.tasks.get('job-1')['state'],'failed');self.assertEqual(self.cma.calls,1)


if __name__=='__main__':unittest.main()


class TransientPlatformFailureTests(unittest.TestCase):
    def test_security_readiness_503_is_retryable(self):
        from managed.control import execution_failure_class, reclassified_retry
        error={'code':'agent_turn_failed','message':'unexpected status 503 Service Unavailable: '
               '{"error":"security_readiness_unavailable","error_code":12900}'}
        self.assertEqual(execution_failure_class(error),'rate-limit')
        parked={'response_error':error,'recovery':{'status':'needs-operator','error_class':'unknown'}}
        self.assertTrue(reclassified_retry(parked))

    def test_agent_command_queue_timeout_is_retryable(self):
        from managed.control import execution_failure_class, reclassified_retry
        error={'code':'agent_command_queue_timeout','message':'The admitted Agent command could not complete.'}
        self.assertEqual(execution_failure_class(error),'rate-limit')
        parked={'response_error':error,'recovery':{'status':'needs-operator','error_class':'unknown'}}
        self.assertTrue(reclassified_retry(parked))

    def test_inactive_sandbox_create_is_a_runtime_retry(self):
        from managed.control import runtime_failure
        self.assertTrue(runtime_failure({'error':'Agent lifecycle request conflicts with existing state: sandbox is not active'}))
        self.assertFalse(runtime_failure({'error':'sandbox limit reached: 10 of 10'}))
