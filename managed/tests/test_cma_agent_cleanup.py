"""Synthetic CMA ownership cleanup and bounded Sandbox retirement."""
import copy
import unittest
from urllib.parse import parse_qs, urlsplit

from managed.cma import AGENT_CLEANUP_WAIT_SECONDS, CMA, CMARequestError


class LifecycleCMA(CMA):
    """Synthetic CMA with independently controlled inventory and GET views."""
    def __init__(self, agents, *, accepted=False):
        super().__init__(token='synthetic')
        self.agents={aid:{'id':aid,'name':'outreach-'+aid[4:],'status':status}
                     for aid,status in agents.items()}
        self.accepted=accepted
        self.calls=[]
        self.stop_error=CMARequestError(409,{'code':'agent_not_ready'})
        self.delete_error=None
        self.get_error=None
        self.omit_inventory=False
        self.sandbox_status='retiring'
        self.sandbox_statuses={}
        self.inventory_by_sandbox={}

    def request(self,path,method='GET',body=None,key=None):
        self.calls.append((path,method,key))
        if path.startswith('/api/v1/agents?'):
            sandbox=parse_qs(urlsplit(path).query)['sandbox'][0]
            agents=[item for aid,item in self.agents.items()
                    if sandbox not in self.inventory_by_sandbox or aid in self.inventory_by_sandbox[sandbox]]
            return {'items':[] if self.omit_inventory else copy.deepcopy(agents)}
        if path.endswith('/workspaces'):return {'items':[{'id':'wks_synthetic'}]}
        if path.startswith('/api/v1/sandboxes/'):
            sandbox=path.split('/')[-1]
            if method=='DELETE':self.sandbox_statuses[sandbox]='retired'
            return {'status':self.sandbox_statuses.get(sandbox,self.sandbox_status)}
        aid=path.split('/')[-2] if path.endswith('/stop') else path.split('/')[-1]
        if method=='GET':
            if self.get_error:raise self.get_error
            if aid not in self.agents:raise CMARequestError(404,{'code':'agent_not_found'})
            return copy.deepcopy(self.agents[aid])
        if path.endswith('/stop'):
            if self.stop_error:raise self.stop_error
            self.agents[aid]['status']='stopping'
            return {'status':'stopping'}
        if method=='DELETE':
            if self.delete_error:raise self.delete_error
            if not self.accepted:self.agents[aid]['status']='deleted'
            return {'status':'accepted' if self.accepted else 'deleted'}
        raise AssertionError((path,method))


class CMAAgentCleanupTests(unittest.TestCase):
    def test_task_retire_does_not_sweep_shared_sandbox_agents(self):
        calls=[]
        agents={
            'agt_B':{'id':'agt_B','name':'outreach-task-B','status':'failed'},
            'agt_A':{'id':'agt_A','name':'outreach-task-A','status':'working'},
        }
        def request(path,method='GET',body=None,key=None):
            calls.append((path,method,key))
            if path=='/api/v1/agents/agt_B' and method=='GET':return dict(agents['agt_B'])
            if path.endswith('/stop'):
                aid=path.split('/')[-2]
                if aid=='agt_B':raise CMARequestError(409,'failed Agent')
                raise AssertionError('task retirement swept '+aid)
            if path=='/api/v1/agents/agt_B' and method=='DELETE':
                agents['agt_B']['status']='deleted';return {'status':'deleted'}
            if path=='/api/v1/sandboxes/sbx_shared':return {'status':'active'}
            if path.startswith('/api/v1/agents?'):
                return {'items':[dict(item) for item in agents.values()],'next':None}
            raise AssertionError((path,method))
        cma=CMA(token='fake');cma.request=request
        result=cma.retire('agt_B','outreach-retire-task-B','sbx_shared')
        self.assertFalse(any(path.endswith('/agt_A/stop') for path,_,_ in calls))
        self.assertNotIn('operator_check',result)
        self.assertNotIn('_agent_deletions',result)
        self.assertEqual(result['foreign_agents'],['agt_A'])
        self.assertFalse(any(path.endswith('/sbx_shared') and method=='DELETE'
                             for path,method,_ in calls))

    def test_delete_gone_is_distinct_from_accepted_delete(self):
        for resource in ('agent','sandbox'):
            for gone in (False,True):
                with self.subTest(resource=resource,gone=gone):
                    def request(path,method='GET',body=None,key=None):
                        if method=='DELETE':
                            if gone:raise CMARequestError(404,{'code':resource+'_not_found'})
                            return {'status':'deleted'}
                        if '/agents?' in path:return {'items':[]}
                        if path.endswith('/workspaces'):return {'items':[{'id':'wks_test'}]}
                        return {'status':'stopped' if resource=='agent' else 'active'}
                    cma=CMA(token='fake');cma.request=request
                    if resource=='agent':
                        result=cma.retire('agt_test','stable-key')
                        field='delete_outcome'
                        self.assertTrue(result['deleted'])
                    else:
                        result=cma.retire_sandbox(None,'sbx_test','stable-key',{})
                        field='sandbox_delete_outcome'
                        if gone:self.assertTrue(result['sandbox_retired'])
                    self.assertEqual(result[field],'gone' if gone else 'accepted')

    def test_title_prefix_is_not_ownership_without_task_name(self):
        calls=[]
        def request(path,method='GET',body=None,key=None):
            calls.append((path,method,key))
            if path=='/api/v1/sandboxes/sbx_title':return {'status':'active'}
            if path.startswith('/api/v1/agents?'):
                return {'items':[{'id':'agt_foreign','title':'outreach-task','status':'ready'}], 'next':None}
            raise AssertionError((path,method))
        cma=CMA(token='fake');cma.request=request
        result=cma.retire_sandbox(None,'sbx_title','pool-key',{},dead_ok=True,
                                  task_ids={'different-task'})
        self.assertFalse(result['sandbox_retired'])
        self.assertEqual(result['operator_check']['agents'],['agt_foreign'])
        self.assertFalse(any(method=='DELETE' for _,method,_ in calls))

    def test_inflight_observations_do_not_consume_cleanup_attempts(self):
        statuses=['working','stopping','stopped','deleting','deleted']
        calls=[]
        def request(path,method='GET',body=None,key=None):
            calls.append((path,method,key))
            if path.endswith('/stop'):return {}
            if method=='DELETE':return {}
            if path=='/api/v1/agents/agt_progress':
                return {'id':'agt_progress','status':statuses.pop(0)}
            raise AssertionError((path,method))
        cma=CMA(token='fake');cma.request=request
        results=[cma.retire('agt_progress','stable-key') for _ in range(3)]
        self.assertEqual([r.get('cleanup_attempts',0) for r in results],[0]*3)
        self.assertEqual([r['status'] for r in results],['stopping','deleting','deleted'])
        self.assertEqual([key for path,method,key in calls if method=='DELETE'],['stable-key-delete'])

    def test_failed_agent_is_stopped_and_deleted(self):
        calls=[]
        def request(path, method='GET', body=None, key=None):
            calls.append((path, method, key))
            if path == '/api/v1/agents/agt_failed' and method == 'GET':
                return {'id':'agt_failed','status':'failed','sandbox':{'id':'sbx_test'}}
            if path.endswith('/stop'):
                raise CMARequestError(409, 'failed Agent cannot be stopped')
            if path == '/api/v1/agents/agt_failed' and method == 'DELETE':
                return {'status':'deleted'}
            if path == '/api/v1/agents/agt_failed':
                return {'id':'agt_failed','status':'failed','sandbox':{'id':'sbx_test'}}
            raise AssertionError(path)

        cma=CMA(token='fake');cma.request=request
        result=cma.retire('agt_failed','outreach-retire-failed')
        self.assertTrue(result['deleted'])
        self.assertEqual([method for _,method,_ in calls],['GET','POST','DELETE'])
        self.assertEqual(calls[1][2],'outreach-retire-failed-stop')
        self.assertEqual(calls[2][2],'outreach-retire-failed-delete')

    def test_ready_agent_is_deleted_after_task_end(self):
        cma=LifecycleCMA({'agt_ready':'ready'})
        result=cma.retire('agt_ready','stable-key',now=1000)
        self.assertTrue(result['deleted'])
        self.assertEqual([method for _,method,_ in cma.calls],['GET','DELETE'])

    def test_foreign_agent_is_left_alone_and_keeps_sandbox(self):
        calls=[]
        def request(path, method='GET', body=None, key=None):
            calls.append((path, method, key))
            if path == '/api/v1/sandboxes/sbx_foreign':return {'status':'active'}
            if path.startswith('/api/v1/agents?'):
                return {'items':[{'id':'agt_foreign','title':'human-debug','status':'ready'}],
                        'next':None}
            raise AssertionError(path)

        cma=CMA(token='fake');cma.request=request
        result=cma.retire_sandbox(None,'sbx_foreign','outreach-pool-retire-sbx_foreign',{},dead_ok=True)
        self.assertFalse(result['sandbox_retired'])
        self.assertEqual(result['reason'],'Other Agent work found; retained Sandbox')
        self.assertFalse(any(method in {'POST','DELETE'} for _,method,_ in calls))

    def test_gone_sandbox_surfaces_operator_check(self):
        calls=[]
        def request(path, method='GET', body=None, key=None):
            calls.append((path, method, key))
            if path == '/api/v1/sandboxes/sbx_gone':return {'status':'active'}
            if path.startswith('/api/v1/agents?'):
                return {'items':[{'id':'agt_ours','name':'outreach-old','status':'working'}],
                        'next':None}
            if path.endswith('/stop'):
                raise CMARequestError(409, 'sandbox_gone: Sandbox is physically gone')
            raise AssertionError(path)

        cma=CMA(token='fake');cma.request=request
        result=cma.retire_sandbox(None,'sbx_gone','outreach-pool-retire-sbx_gone',{},dead_ok=True,
                                  task_ids={'old'})
        self.assertFalse(result['sandbox_retired'])
        self.assertEqual(result['operator_check']['agents'],['agt_ours'])
        self.assertFalse(any(method == 'DELETE' for _,method,_ in calls))

    def test_owned_agent_sweep_is_bounded_and_idempotent_across_ticks(self):
        calls=[]
        agents={
            'agt_one':{'id':'agt_one','name':'outreach-one','status':'failed'},
            'agt_two':{'id':'agt_two','name':'outreach-two','status':'ready'},
        }
        def request(path, method='GET', body=None, key=None):
            calls.append((path, method, key))
            if path == '/api/v1/sandboxes/sbx_sweep':
                return {'status':'retired' if method == 'GET' and
                        agents['agt_two']['status'] == 'deleted' and
                        any(call[1] == 'DELETE' and call[0] == path for call in calls)
                        else 'active'}
            if path.startswith('/api/v1/agents?'):
                return {'items':[copy.deepcopy(value) for value in agents.values()], 'next':None}
            if path.endswith('/stop'):
                aid=path.split('/')[-2]
                if aid == 'agt_one':raise CMARequestError(409,'failed Agent')
                agents[aid]['status']='stopped'
                return {}
            if path.startswith('/api/v1/agents/'):
                aid=path.split('/')[-1]
                if method == 'DELETE':
                    agents[aid]['status']='deleted';return {}
                return copy.deepcopy(agents[aid])
            if path.endswith('/workspaces'):
                return {'items':[{'id':'wks_sweep'}]}
            raise AssertionError((path,method))

        cma=CMA(token='fake');cma.request=request
        key='outreach-pool-retire-sbx_sweep'
        first=cma.retire_sandbox(None,'sbx_sweep',key,{},dead_ok=True,max_agent_deletions=1,
                                 task_ids={'one','two'})
        self.assertEqual(first['owned_agents_pending'],['agt_two'])
        self.assertEqual(sum(call[1] == 'DELETE' and 'agt_one' in call[0] for call in calls),1)
        second=cma.retire_sandbox(None,'sbx_sweep',key,first,dead_ok=True,max_agent_deletions=1,
                                  task_ids={'one','two'})
        self.assertTrue(second['sandbox_retired'])
        self.assertEqual(sum(call[1] == 'DELETE' and 'agt_one' in call[0] for call in calls),1)
        self.assertEqual(sum(call[1] == 'DELETE' and 'agt_two' in call[0] for call in calls),1)
    def test_accepted_delete_observes_stale_failed_status_until_gone(self):
        cma=LifecycleCMA({'agt_failed':'failed'},accepted=True)
        result={}
        for now in range(1000,1006):
            result=cma.retire_sandbox(None,'sbx_synthetic','original-key',result,
                dead_ok=True,task_ids={'failed'},now=now)
            [agent]=result['agent_cleanup']
            self.assertEqual(agent['cleanup_attempts'],0)
            self.assertEqual(agent['errors'],[])
            self.assertNotIn('operator_check',result)
        self.assertEqual([(method,key) for _,method,key in cma.calls if method!='GET'],
            [('POST','original-key-agent-agt_failed-stop'),('DELETE','original-key-agent-agt_failed-delete')])
        cma.agents.clear()
        result=cma.retire_sandbox(None,'sbx_synthetic','original-key',result,
            dead_ok=True,task_ids={'failed'},now=1006)
        self.assertTrue(result['sandbox_retired'])
        self.assertEqual(result['agent_cleanup'][0]['status'],'gone')

    def test_accepted_delete_has_persistent_observation_deadline(self):
        cma=LifecycleCMA({'agt_idle':'ready'},accepted=True)
        first=cma.retire('agt_idle','original-key',now=1000)
        before=cma.retire('agt_idle','original-key',previous=copy.deepcopy(first),
                          now=1000+AGENT_CLEANUP_WAIT_SECONDS-1)
        self.assertNotIn('operator_check',before)
        expired=cma.retire('agt_idle','original-key',previous=before,
                           now=1000+AGENT_CLEANUP_WAIT_SECONDS)
        self.assertIn('24 hours',expired['operator_check']['reason'])
        self.assertEqual(expired['cleanup_started_at'],1000)
        cma.agents.clear()
        gone=cma.retire('agt_idle','original-key',previous=expired,now=1001+AGENT_CLEANUP_WAIT_SECONDS)
        self.assertTrue(gone['deleted'])
        self.assertNotIn('operator_check',gone)
        self.assertEqual(sum(method=='DELETE' for _,method,_ in cma.calls),1)

    def test_accepted_delete_read_failures_keep_acceptance_and_bound_observation(self):
        cma=LifecycleCMA({'agt_idle':'ready'},accepted=True)
        cma.get_error=TimeoutError('lost readback')
        first=cma._retire_agent('agt_idle','key',agent=cma.agents['agt_idle'],now=1000)
        self.assertEqual(first['delete_outcome'],'accepted')
        for error in (TimeoutError('lost read'),CMARequestError(503,{'code':'unavailable'})):
            cma.get_error=error
            result=cma.retire('agt_idle','key',previous=first,now=2000)
            self.assertEqual(result['errors'],[])
            self.assertNotIn('operator_check',result)
        result=cma.retire('agt_idle','key',previous=result,now=1000+AGENT_CLEANUP_WAIT_SECONDS)
        self.assertIn('operator_check',result)
        self.assertEqual(sum(method=='DELETE' for _,method,_ in cma.calls),1)

    def test_accepted_delete_terminal_get_refusal_is_not_deletion(self):
        for error in (CMARequestError(403,{'code':'forbidden'}),CMARequestError(404,{'status':404}),
                      CMARequestError(405,{'code':'method_not_allowed'})):
            with self.subTest(error=error):
                cma=LifecycleCMA({'agt_idle':'ready'},accepted=True)
                result=cma.retire('agt_idle','key',now=1000)
                cma.get_error=error
                result=cma.retire('agt_idle','key',previous=result,now=1001)
                self.assertFalse(result['deleted'])
                self.assertIn('operator_check',result)
                self.assertEqual(sum(method=='DELETE' for _,method,_ in cma.calls),1)

    def test_terminal_agent_not_ready_stop_is_benign(self):
        for status in ('failed','stopped','terminal'):
            with self.subTest(status=status):
                cma=LifecycleCMA({'agt_terminal':status},accepted=True)
                result=cma.retire('agt_terminal','key',now=1000)
                self.assertEqual(result['errors'],[])
                self.assertEqual(result['delete_outcome'],'accepted')
                self.assertNotIn('operator_check',result)
                self.assertEqual(sum(method=='DELETE' for _,method,_ in cma.calls),1)

    def test_working_agent_keeps_stop_then_delete_order(self):
        cma=LifecycleCMA({'agt_working':'working'},accepted=True)
        cma.stop_error=None
        first=cma.retire('agt_working','key',now=1000)
        self.assertEqual(first['status'],'stopping')
        self.assertFalse(any(method=='DELETE' for _,method,_ in cma.calls))
        cma.agents['agt_working']['status']='stopped'
        second=cma.retire('agt_working','key',previous=first,now=1001)
        cma.retire('agt_working','key',previous=second,now=1002)
        self.assertEqual([method for _,method,_ in cma.calls if method!='GET'],['POST','DELETE'])

    def test_four_ready_agents_retire_sandbox_within_budget(self):
        for budget in (2,4):
            with self.subTest(budget=budget):
                cma=LifecycleCMA({'agt_'+str(n):'ready' for n in range(4)})
                result={}
                for tick in range(4//budget):
                    start=len(cma.calls)
                    result=cma.retire_sandbox(None,'sbx_synthetic','key',result,dead_ok=True,
                        task_ids={str(n) for n in range(4)},max_agent_deletions=budget,now=1000+tick)
                    self.assertLessEqual(sum(method=='DELETE' and '/agents/' in path
                        for path,method,_ in cma.calls[start:]),budget)
                self.assertTrue(result['sandbox_retired'])
                self.assertFalse(any(method=='POST' for _,method,_ in cma.calls))



if __name__ == '__main__':
    unittest.main()
