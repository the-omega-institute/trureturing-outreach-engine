"""Synthetic CMA ownership cleanup and bounded Sandbox retirement."""
import copy
import unittest

from managed.cma import CMA, CMARequestError


class CMAAgentCleanupTests(unittest.TestCase):
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
        calls=[];statuses=['ready','ready','deleted']
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
        result=cma.retire('agt_ready','outreach-retire-ready')
        self.assertTrue(result['deleted'])
        self.assertEqual([method for _,method,_ in calls],['GET','POST','GET','DELETE','GET'])

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
        result=cma.retire_sandbox(None,'sbx_gone','outreach-pool-retire-sbx_gone',{},dead_ok=True)
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
        first=cma.retire_sandbox(None,'sbx_sweep',key,{},dead_ok=True,max_agent_deletions=1)
        self.assertEqual(first['owned_agents_pending'],['agt_two'])
        self.assertEqual(sum(call[1] == 'DELETE' and 'agt_one' in call[0] for call in calls),1)
        second=cma.retire_sandbox(None,'sbx_sweep',key,first,dead_ok=True,max_agent_deletions=1)
        self.assertTrue(second['sandbox_retired'])
        self.assertEqual(sum(call[1] == 'DELETE' and 'agt_one' in call[0] for call in calls),1)
        self.assertEqual(sum(call[1] == 'DELETE' and 'agt_two' in call[0] for call in calls),1)


if __name__ == '__main__':
    unittest.main()
