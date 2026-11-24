"""Synthetic CMA ownership cleanup and bounded Sandbox retirement."""
import copy
import unittest

from managed.cma import CMA, CMARequestError


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
        calls=[];statuses=['ready','stopping','stopped','deleted']
        def request(path, method='GET', body=None, key=None):
            calls.append((path, method, key))
            if path.endswith('/stop'):
                return {}
            if path == '/api/v1/agents/agt_ready' and method == 'DELETE':
                return {}
            if path == '/api/v1/agents/agt_ready':
                return {'id':'agt_ready','status':statuses.pop(0),
                        'sandbox':{'id':'sbx_test'}}
            raise AssertionError(path)

        cma=CMA(token='fake');cma.request=request
        first=cma.retire('agt_ready','outreach-retire-ready')
        self.assertFalse(first['deleted'])
        result=cma.retire('agt_ready','outreach-retire-ready')
        self.assertTrue(result['deleted'])
        self.assertEqual([method for _,method,_ in calls],['GET','POST','GET','GET','DELETE','GET'])

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


if __name__ == '__main__':
    unittest.main()
