"""Pool ordering, asynchronous reservations, lifecycle cleanup and turn retries."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from managed.cma import CMA as RESTCMA, CMARequestError
from managed.control import Ledger, advance
from managed.queue import canonical
from managed.sandbox_pool import CAPACITY_BLOCK_SECONDS, POOL_WAIT_SECONDS, SandboxPool, summary
from managed.tick import Tick
from test_tasks import CMA, Releases, request


class PoolCMA(CMA):
    def __init__(self):
        super().__init__()
        self.calls=[];self.operations={};self.agents={};self.statuses={}
        self.create_status='succeeded';self.create_error=None
        self.admissions=[];self.retire_calls=[];self.before_admit=None
        self.lose_transport=False
        self.before_retire=None;self.retire_errors=[];self.lose_retire=False;self.foreign_agents=[]
        self.lifecycle_errors={};self.lose_lifecycle=set();self.before_lifecycle=None
        self.lifecycle_status={'pause':'pausing','revive':'reviving'}

    retire_sandbox=RESTCMA.retire_sandbox

    def request(self,path,method='GET',body=None,key=None):
        if method=='GET' and path.startswith('/api/v1/agents/'):
            aid=path.split('/')[-1]
            return {'id':aid,'status':'active','sandbox':{'id':self.agents.get(aid)}}
        self.calls.append((path,method,key))
        if method=='GET' and path.startswith('/api/v1/agents?'):
            return {'items':self.foreign_agents}
        if method=='GET' and path.endswith('/workspaces'):
            return {'items':[{'id':'wks_'+path.split('/')[-2]}]}
        if method=='GET' and path.startswith('/api/v1/sandboxes/'):
            result=self.statuses.get(path.split('/')[-1],{'status':'active'})
            if isinstance(result,Exception):raise result
            return result
        if method=='DELETE' and path.startswith('/api/v1/sandboxes/'):
            if self.before_retire:self.before_retire(path.split('/')[-1],key)
            if self.retire_errors:raise self.retire_errors.pop(0)
            self.statuses[path.split('/')[-1]]={'status':'retiring'}
            if self.lose_retire:
                self.lose_retire=False;raise TimeoutError('Retirement scheduled; response lost')
            return {}
        if method=='POST' and path.startswith('/api/v1/sandboxes/'):
            sandbox_id,action=path.split('/')[-2:]
            if self.before_lifecycle:self.before_lifecycle(sandbox_id,action,key)
            if action in self.lifecycle_errors:raise self.lifecycle_errors.pop(action)
            result={'status':self.lifecycle_status[action]}
            self.statuses[sandbox_id]=result
            if action in self.lose_lifecycle:
                self.lose_lifecycle.remove(action);raise TimeoutError(action+' scheduled; response lost')
            return result
        raise AssertionError('Unexpected CMA request: '+path)

    def create(self,body,key):
        if self.create_error:
            error=self.create_error;self.create_error=None;raise error
        if key not in self.creates:
            self.creates[key]=copy.deepcopy(body)
            number=len(self.creates);agent='agt_'+str(number);operation='op_'+str(number)
            self.agents[agent]='sbx_'+str(number)
            self.operations[operation]={'status':self.create_status,'agent_id':agent}
        return {'id':'op_'+str(list(self.creates).index(key)+1)}

    def operation(self,operation_id):return self.operations[operation_id]
    def sandbox_of(self,agent_id):return self.agents[agent_id]

    def admit(self,agent_id,prompt,key):
        self.admissions.append((agent_id,prompt,key))
        if self.before_admit:self.before_admit(agent_id,prompt,key)
        self.commands.setdefault(key,{'agent':agent_id,'prompt':prompt,'response_id':'resp_'+key})
        if self.lose_transport and '-transport-' in key:
            self.lose_transport=False;raise TimeoutError('Retry accepted; observation lost')
        return {'response_id':self.commands[key]['response_id']}

    def retire(self,agent_id,key,owned_sandbox_id=None):
        self.retire_calls.append((agent_id,key,owned_sandbox_id))
        return super().retire(agent_id,key,owned_sandbox_id)


class PoolFixture:
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.folder=Path(self.temp.name)
        self.remote=Releases();self.ledger=Ledger(self.remote,self.folder);self.cma=PoolCMA()
        self.now=1000;self.ledger.tasks.clock=lambda:self.now
        self.cfg={'paused':False,'kinds':{'research':True},'max_active':24,'agent_template':{},
                  'sandbox_pool':{'max_sandboxes':4,'agents_per_sandbox':6}}

    def tearDown(self):self.ledger.tasks.close();self.temp.cleanup()
    def tick(self):
        advance(self.ledger,self.cma,self.cfg,self.folder,revision='b'*40)
        self.ledger.save(force=True)  # Match the production tick's final flush.

    def state(self,key='sandbox_pool'):
        record=self.ledger.tasks.db.execute('SELECT payload FROM pipeline_state WHERE key=?',(key,)).fetchone()
        return json.loads(record[0]) if record else []

    def seed_pool(self,count=1):
        entries=[{'sandbox_id':'sbx_existing'+str(i),'workspace_id':'wks_existing'+str(i),
                  'created_by_task':'seed-'+str(i),'state':'active','idle_since':self.now} for i in range(count)]
        self.ledger.tasks.db.execute('INSERT OR REPLACE INTO pipeline_state VALUES(?,?)',
                                     ('sandbox_pool',canonical(entries)))
        return entries

    def queue(self,name):return self.ledger.tasks.add(request(name,'owner-'+name))

    def occupy(self,entry,number,state='held',deleted=False):
        name=entry['sandbox_id']+'-'+str(number);row=self.queue(name)
        return self.ledger.tasks.update(name,row['revision'],state,
            {'agent_id':'agt_'+name,'response_id':'resp_'+name,
             'sandbox_id':entry['sandbox_id'],'workspace_id':entry['workspace_id'],
             'cleanup':{'deleted':deleted}})

    def restore(self):
        self.ledger.tasks.close();self.folder=self.folder/'restore';self.folder.mkdir()
        self.ledger=Ledger(self.remote,self.folder)
        self.ledger.tasks.clock=lambda:self.now


class PoolTests(PoolFixture,unittest.TestCase):
    def test_retiring_failed_entry_reconciles_agents_and_clears_stale_error(self):
        from managed.sandbox_pool import SandboxPool
        sid='sbx_stuck';wid='wks_stuck'
        row=self.queue('stuck-agent')
        self.ledger.tasks.update(row['id'],row['revision'],'failed',{
            'agent_id':'agt_stuck','sandbox_id':sid,'workspace_id':wid,
            'cleanup':{'deleted':True}})
        entry={'sandbox_id':sid,'workspace_id':wid,'state':'failed',
               'agents_placed':1,'cleanup_error':'TimeoutError: old read',
               'cleanup':{'sandbox_id':sid,'sandbox_retired':False},
               'cleanup_intent':{'key':'outreach-pool-retire-'+sid},
               'cleanup_checked_at':0}
        self.ledger.tasks.db.execute('INSERT OR REPLACE INTO pipeline_state VALUES(?,?)',
                                     ('sandbox_pool_cleanup',canonical([entry])))
        self.ledger.tasks.db.execute('INSERT OR REPLACE INTO pipeline_state VALUES(?,?)',
                                     ('sandbox_pool',canonical([])))
        state={'status':'retiring'};agent={'id':'agt_stuck','name':'outreach-stuck-agent','status':'failed'};calls=[]
        class SweepCMA:
            retire_sandbox=RESTCMA.retire_sandbox

            def request(_,path,method='GET',body=None,key=None):
                calls.append((path,method,key))
                if path==f'/api/v1/sandboxes/{sid}' and method=='DELETE':
                    state['status']='retiring';return {}
                if path==f'/api/v1/sandboxes/{sid}':return dict(state)
                if path.startswith('/api/v1/agents?'):
                    return {'items':[dict(agent)],'next':None}
                if path==f'/api/v1/agents/agt_stuck/stop':
                    agent['status']='failed';raise CMARequestError(409,'failed Agent')
                if path==f'/api/v1/agents/agt_stuck' and method=='DELETE':
                    agent['status']='deleted';return {'status':'deleted'}
                if path==f'/api/v1/agents/agt_stuck':return dict(agent)
                if path==f'/api/v1/sandboxes/{sid}/workspaces':return {'items':[{'id':wid}]}
                raise AssertionError((path,method))
        pool=SandboxPool(self.ledger,self.cfg['sandbox_pool'],self.cfg['kinds'],paused=True)
        pool.health(SweepCMA(),rows=self.ledger.tasks.all())
        self.assertTrue(any(path.startswith('/api/v1/agents?') for path,_,_ in calls))
        self.assertTrue(any(path.endswith('/agt_stuck') and method=='DELETE' for path,method,_ in calls))
        saved=self.state('sandbox_pool_cleanup')[0]
        self.assertNotIn('cleanup_error',saved)

    def test_first_sandbox_fills_to_six_before_second_opens(self):
        for i in range(13):self.queue('new-'+str(i))
        self.tick()
        bodies=list(self.cma.creates.values())
        self.assertEqual([b.get('workspace_id') for b in bodies],
                         [None]+['wks_sbx_1']*5+[None]+['wks_sbx_7']*5+[None])
        self.assertEqual([e['created_by_task'] for e in self.state()],['new-0','new-6','new-12'])
        for i in range(13):
            data=self.ledger.tasks.get('new-'+str(i))['data']
            self.assertIn(data['pool_sandbox_id'],{'sbx_1','sbx_7','sbx_13'})
            self.assertEqual(data['workspace_id'],'wks_'+data['pool_sandbox_id'])
            self.assertNotIn('owned_sandbox_id',data)
            if i%6:self.assertIn(data['workspace_id'],data['create_key'])
        self.assertFalse(any(path.startswith('/api/v1/agents?') for path,_,_ in self.cma.calls))

    def test_least_loaded_existing_entry_receives_agents(self):
        entries=self.seed_pool(2)
        for i in range(5):self.occupy(entries[0],i)
        self.queue('first');self.queue('second');self.tick()
        self.assertEqual([b['workspace_id'] for b in self.cma.creates.values()],
                         [entries[1]['workspace_id'],entries[1]['workspace_id']])
        self.assertEqual(self.state(),[{**entries[0],'agents_placed':5},{**entries[1],'agents_placed':2}])
        self.assertEqual(self.cma.calls,[(f"/api/v1/sandboxes/{e['sandbox_id']}",'GET',None) for e in entries])

    def test_equal_load_uses_existing_order_then_spreads_evenly(self):
        entries=self.seed_pool(4)
        for i in range(8):self.queue('balanced-'+str(i))
        self.tick()
        self.assertEqual([b['workspace_id'] for b in self.cma.creates.values()],
                         [e['workspace_id'] for e in entries]*2)

    def test_full_entry_opens_new_sandbox_and_pool_survives_restore(self):
        entry=self.seed_pool()[0]
        for i in range(6):self.occupy(entry,i)
        self.queue('next');self.tick()
        self.assertNotIn('workspace_id',next(iter(self.cma.creates.values())))
        self.assertEqual(self.state()[1],{'sandbox_id':'sbx_1','workspace_id':'wks_sbx_1','created_by_task':'next',
                                         'state':'active','idle_since':self.now,'agents_placed':1})
        self.restore();self.queue('after-restore');self.tick()
        self.assertEqual(list(self.cma.creates.values())[-1]['workspace_id'],'wks_sbx_1')
        self.assertEqual(len(self.state()),2)

    def test_live_legacy_owned_sandboxes_count_against_pool_quota(self):
        entry=self.seed_pool()[0]
        for i in range(6):self.occupy(entry,i)
        for i in range(3):
            name='legacy-'+str(i);row=self.queue(name)
            self.ledger.tasks.update(name,row['revision'],'running',
                {'agent_id':'agt_'+name,'response_id':'resp_'+name,'owned_sandbox_id':'sbx_legacy'+str(i)})
        self.queue('waiting');self.tick()
        self.assertEqual(self.cma.creates,{})
        self.assertEqual(self.ledger.tasks.get('waiting')['state'],'queued')

    def test_full_four_by_six_pool_leaves_queue_untouched_without_task_calls(self):
        entries=self.seed_pool(4)
        for entry in entries:
            for i in range(6):self.occupy(entry,i)
        row=self.queue('waiting')
        row=self.ledger.tasks.update(row['id'],row['revision'],'queued',{'capacity_retries':3})
        self.tick()
        self.assertEqual(self.ledger.tasks.get(row['id']),row)
        self.assertFalse(self.cma.creates);self.assertFalse(self.cma.admissions);self.assertFalse(self.cma.retire_calls)
        self.assertEqual(self.cma.calls,[(f"/api/v1/sandboxes/{e['sandbox_id']}",'GET',None) for e in entries])

    def test_pending_workspace_creations_reserve_slots(self):
        entry=self.seed_pool()[0];self.cma.create_status='queued'
        for i in range(9):self.queue('pending-'+str(i))
        self.tick()
        bodies=list(self.cma.creates.values())
        self.assertEqual([b.get('workspace_id') for b in bodies],[entry['workspace_id']]*6+[None])
        self.assertEqual(self.ledger.tasks.get('pending-7')['state'],'queued')
        self.assertEqual(self.ledger.tasks.get('pending-8')['state'],'queued')

    def test_pending_first_sandbox_waits_for_workspace_before_opening_more(self):
        self.cma.create_status='queued'
        for i in range(5):self.queue('pending-'+str(i))
        self.tick();self.restore();self.tick()
        self.assertEqual(len(self.cma.creates),1)
        self.cma.operations['op_1']['status']='succeeded';self.tick()
        self.assertEqual(len(self.cma.creates),5)
        self.assertEqual(len(self.state()),1)
        self.assertEqual([b.get('workspace_id') for b in list(self.cma.creates.values())[1:]],['wks_sbx_1']*4)

    def test_deleted_agents_release_slots_but_held_agents_count(self):
        entry=self.seed_pool()[0]
        for i in range(6):self.occupy(entry,i,deleted=i==5)
        self.queue('reuse');self.tick()
        self.assertEqual(next(iter(self.cma.creates.values()))['workspace_id'],entry['workspace_id'])
        self.assertEqual(len(self.state()),1)

    def test_stopped_agents_awaiting_deletion_release_slots(self):
        entry=self.seed_pool()[0]
        for i in range(6):
            row=self.occupy(entry,i)
            if i==5:
                self.ledger.tasks.update(row['id'],row['revision'],row['state'],
                    {**row['data'],'cleanup':{'deleted':False,'stopped':True,'status':'stopped'}})
        self.assertEqual(summary(self.ledger.tasks)[0]['live_agents'],5)
        self.queue('reuse');self.tick()
        self.assertEqual(next(iter(self.cma.creates.values()))['workspace_id'],entry['workspace_id'])
        self.assertEqual(len(self.state()),1)

    def test_failed_pool_sandbox_dropped_and_deleted_with_stable_key(self):
        entry=self.seed_pool()[0];self.cfg['paused']=True
        self.cma.statuses[entry['sandbox_id']]={'status':'failed'}
        self.tick()
        self.assertEqual(self.state(),[])
        deletion=(f"/api/v1/sandboxes/{entry['sandbox_id']}",'DELETE','outreach-pool-retire-'+entry['sandbox_id']+'-sandbox-delete')
        self.assertIn(deletion,self.cma.calls)
        self.restore();self.cma.statuses[entry['sandbox_id']]={'status':'failed'};self.tick()
        self.assertEqual(self.cma.calls.count(deletion),2)
        self.cma.statuses[entry['sandbox_id']]={'status':'retired'};self.tick()
        self.assertEqual(self.state('sandbox_pool_cleanup'),[])

    def test_long_stalled_empty_sandbox_is_retired_but_occupied_one_waits(self):
        from managed.sandbox_pool import RETIRE_STALLED_SECONDS
        empty,busy=self.seed_pool(2);self.cfg['paused']=True;self.occupy(busy,1)
        for entry in (empty,busy):self.cma.statuses[entry['sandbox_id']]={'status':'credential_rotating'}
        self.tick()
        self.assertEqual(len(self.state()),2)  # Not yet stalled long enough.
        self.now+=RETIRE_STALLED_SECONDS;self.tick()
        self.assertEqual([e['sandbox_id'] for e in self.state()],[busy['sandbox_id']])
        deletion=(f"/api/v1/sandboxes/{empty['sandbox_id']}",'DELETE',
                  'outreach-pool-retire-'+empty['sandbox_id']+'-sandbox-delete')
        self.assertIn(deletion,self.cma.calls)
        self.assertFalse(any(path.endswith(busy['sandbox_id']) and method=='DELETE'
                             for path,method,_ in self.cma.calls))
        self.cma.statuses[empty['sandbox_id']]={'status':'retired'};self.tick()
        self.assertEqual(self.state('sandbox_pool_cleanup'),[])

    def test_failed_sandbox_waits_for_last_live_agent_before_delete(self):
        entry=self.seed_pool()[0];row=self.occupy(entry,1);self.cfg['paused']=True
        self.cma.statuses[entry['sandbox_id']]={'status':'failed'};self.tick()
        self.assertEqual(self.state(),[])
        self.assertFalse(any(method=='DELETE' for _,method,_ in self.cma.calls))
        row=self.ledger.tasks.get(row['id'])
        self.ledger.tasks.update(row['id'],row['revision'],'failed',{**row['data'],'cleanup':{'deleted':True}})
        self.tick()
        self.assertEqual(sum(method=='DELETE' for _,method,_ in self.cma.calls),1)

    def test_finished_slot_holder_retires_before_queued_admission(self):
        entry=self.seed_pool()[0];self.cfg['sandbox_pool']['max_sandboxes']=1
        for i in range(6):
            held=self.occupy(entry,i)
            self.ledger.tasks.update(held['id'],held['revision'],'held',
                {**held['data'],'result':{'outcome':'hold'},'reason':'Checked hold'})
        row=self.queue('waiting');self.tick()
        self.assertTrue(self.cma.retire_calls)
        self.assertIn(self.ledger.tasks.get(row['id'])['state'],{'provisioning','admitting','running'})
        self.assertEqual(next(iter(self.cma.creates.values()))['workspace_id'],entry['workspace_id'])

    def test_agent_whose_stop_was_accepted_releases_its_slot(self):
        entry=self.seed_pool()[0]
        for i,status in enumerate(['stopping','deleting','ready']):
            row=self.occupy(entry,i)
            self.ledger.tasks.update(row['id'],row['revision'],row['state'],
                {**row['data'],'cleanup':{'deleted':False,'stopped':False,'status':status}})
        self.assertEqual(summary(self.ledger.tasks)[0]['live_agents'],1)
        # A retire call that requested stop records the Agent; a refused one records errors.
        for i,extra in enumerate([{},{'errors':[{'action':'stop','status':409}]}],start=3):
            row=self.occupy(entry,i)
            self.ledger.tasks.update(row['id'],row['revision'],row['state'],
                {**row['data'],'cleanup':{'agent_id':row['data']['agent_id'],'deleted':False,
                                          'stopped':False,'status':'ready',**extra}})
        self.assertEqual(summary(self.ledger.tasks)[0]['live_agents'],2)

    def test_retained_failed_sandbox_is_rechecked_with_backoff(self):
        from managed import sandbox_pool
        entry=self.seed_pool()[0];self.cfg['paused']=True
        self.cma.statuses[entry['sandbox_id']]={'status':'failed'}
        retained={'sandbox_id':entry['sandbox_id'],'sandbox_retired':False,
                  'reason':'Other Agent work found; retained Sandbox'}
        calls=[]
        self.cma.retire_sandbox=lambda *args,**kw:calls.append(args) or dict(retained)
        self.tick()
        self.assertEqual(len(calls),1)
        reads=len(self.cma.calls)
        self.now+=sandbox_pool.RETAINED_RECHECK_SECONDS-1;self.tick()
        self.assertEqual(len(calls),1)
        self.assertFalse([c for c in self.cma.calls[reads:] if entry['sandbox_id'] in c[0]])
        self.now+=1;self.tick()
        self.assertEqual(len(calls),2)

    def test_retired_and_interrupted_entries_drop_without_deletion(self):
        entries=self.seed_pool(3);self.cfg['paused']=True
        self.cma.statuses[entries[0]['sandbox_id']]={'status':'retired'}
        self.cma.statuses[entries[1]['sandbox_id']]={'status':'active','lifecycle_interruption':{'reason':'runtime lost'}}
        self.tick()
        self.assertEqual(self.state(),[{**entries[2],'agents_placed':0}])
        self.assertEqual(len(self.cma.calls),3)

    def test_health_read_failure_preserves_entry_and_waits(self):
        entry=self.seed_pool()[0];self.cma.statuses[entry['sandbox_id']]=TimeoutError('CMA unavailable')
        row=self.queue('waiting');self.tick()
        self.assertEqual(self.state(),[{**entry,'agents_placed':0}]);self.assertEqual(self.ledger.tasks.get(row['id']),row)
        self.assertFalse(self.cma.creates)
        self.assertIn('CMA unavailable',self.state('sandbox_pool_errors')[0]['error'])

    def test_workspace_4xx_drops_target_without_capacity_retry_and_changes_key(self):
        entries=self.seed_pool(2);row=self.queue('rejected')
        row=self.ledger.tasks.update(row['id'],row['revision'],'queued',{'capacity_retries':3})
        error=HTTPError('https://cma/api/v1/agents',409,'Workspace not ready',{},None)
        self.cma.create_error=error
        self.tick();error.close();row=self.ledger.tasks.get(row['id'])
        self.assertEqual(row['state'],'queued');self.assertEqual(row['data']['capacity_retries'],3)
        old_key=row['data']['previous_operations'][0]['create_key']
        self.assertIn(entries[0]['workspace_id'],old_key);self.assertEqual(self.state(),[{**entries[1],'agents_placed':0}])
        self.restore();self.tick();row=self.ledger.tasks.get(row['id'])
        self.assertEqual(row['state'],'running');self.assertEqual(row['data']['capacity_retries'],3)
        self.assertIn(entries[1]['workspace_id'],row['data']['create_key'])
        self.assertNotEqual(row['data']['create_key'],old_key)

    def test_uncertain_workspace_create_retains_original_body_and_key(self):
        entry=self.seed_pool()[0];self.queue('uncertain')
        error=HTTPError('https://cma/api/v1/agents',503,'Unavailable',{},None)
        self.cma.create_error=error
        self.tick();error.close();row=self.ledger.tasks.get('uncertain')
        self.assertEqual(row['state'],'provision-unknown');self.assertEqual(self.state(),[{**entry,'agents_placed':1}])
        self.restore();self.tick();restored=self.ledger.tasks.get('uncertain')
        self.assertEqual(restored['data']['create_key'],row['data']['create_key'])
        self.assertEqual(restored['data']['create_body'],row['data']['create_body'])

    def test_workspace_reuse_omits_environment_creation_options(self):
        entry=self.seed_pool()[0]
        self.cfg['agent_template'].update(image='image',repository={'url':'repo'},machine_included=True)
        self.queue('reuse');self.tick()
        body=next(iter(self.cma.creates.values()))
        self.assertEqual(body['workspace_id'],entry['workspace_id'])
        self.assertFalse({'image','repository','machine_included','sandbox_id'}&body.keys())

    def test_pooled_retire_deletes_only_agent_even_with_legacy_owned_field(self):
        entry=self.seed_pool()[0];row=self.queue('finished')
        self.ledger.tasks.update(row['id'],row['revision'],'failed',
            {'agent_id':'agt_done','response_id':'resp_done','owned_sandbox_id':entry['sandbox_id'],
             'pool_sandbox_id':entry['sandbox_id'],'workspace_id':entry['workspace_id']})
        self.tick();self.tick()
        self.assertEqual(len(self.cma.retire_calls),1)
        self.assertIsNone(self.cma.retire_calls[0][2]);self.assertEqual(self.state(),[{**entry,'agents_placed':1}])
        cma=RESTCMA(token='fixture')
        with patch.object(cma,'request',side_effect=[{'status':'running'},{},{'status':'stopped'},
                                                    {},{'status':'deleted'}]) as calls:
            self.assertTrue(cma.retire('agt_done','stable-key')['deleted'])
        self.assertEqual([(call.args[0],call.args[1]) for call in calls.call_args_list if len(call.args)>1],
                         [('/api/v1/agents/agt_done/stop','POST'),('/api/v1/agents/agt_done','DELETE')])
        self.assertFalse(any('/sandboxes/' in call.args[0] for call in calls.call_args_list))

    def test_legacy_owned_sandbox_retire_unchanged_with_pool_enabled(self):
        self.seed_pool();row=self.queue('legacy')
        self.ledger.tasks.update(row['id'],row['revision'],'failed',
            {'agent_id':'agt_legacy','response_id':'resp_legacy','owned_sandbox_id':'sbx_legacy'})
        self.tick()
        self.assertEqual(self.cma.retire_calls[0][2],'sbx_legacy')
        self.assertTrue(self.ledger.tasks.get(row['id'])['data']['cleanup']['sandbox_retired'])

    def test_completed_pooled_agent_retires_without_sandbox(self):
        entry=self.seed_pool()[0];row=self.queue('complete')
        result={'schema_version':1,'task_id':row['id'],'outcome':'completed',
                'report':'Checked result.','artifacts':[],'packet':None}
        self.cma.response=lambda _: {'status':'completed','output':[{'type':'message',
            'content':[{'type':'output_text','text':json.dumps(result)}]}]}
        self.tick();row=self.ledger.tasks.get(row['id'])
        self.assertEqual(row['state'],'awaiting-review');self.assertEqual(row['data']['result'],result)
        self.assertTrue(row['data']['cleanup']['deleted']);self.assertIsNone(self.cma.retire_calls[0][2])
        self.assertEqual(self.state(),[{**entry,'agents_placed':1}])

    def test_capacity_failure_stops_new_pool_provisioning_in_tick(self):
        self.queue('first');second=self.queue('second')
        original=self.cma.operation
        self.cma.operation=lambda _: {'status':'failed','error':'sandbox limit reached: 10 of 10'}
        self.tick()
        self.assertEqual(len(self.cma.creates),1)
        self.assertEqual(self.ledger.tasks.get('first')['data']['capacity_retries'],1)
        self.assertEqual(self.ledger.tasks.get('second'),second)
        self.assertEqual(self.state('sandbox_pool_capacity'),{'capacity_blocked_at':self.now})
        self.cma.operation=original
        self.restore();self.tick()
        self.assertEqual(self.state('sandbox_pool_capacity'),{})

    def test_capacity_failure_still_admits_into_existing_free_slots(self):
        # The account Sandbox limit blocks only new Sandboxes (2026-11-08).
        self.seed_pool()
        self.queue('first');self.queue('second')
        original=self.cma.operation
        self.cma.operation=lambda _: {'status':'failed','error':'sandbox limit reached: 10 of 10'}
        self.tick()
        self.assertEqual(len(self.cma.creates),2)
        self.assertTrue(all(body.get('workspace_id')=='wks_existing0' for body in self.cma.creates.values()))
        self.assertFalse(self.state('sandbox_pool_capacity'))
        self.cma.operation=original


class PoolRotationTests(PoolFixture,unittest.TestCase):
    def draining_pool(self,agents_placed=24):
        entries=self.seed_pool(2)
        for entry in entries:
            entry['agents_placed']=agents_placed
            for number in range(2):self.occupy(entry,number)
        self.ledger.tasks.db.execute('UPDATE pipeline_state SET payload=? WHERE key=?',
                                     (canonical(entries),'sandbox_pool'))
        pool=SandboxPool(self.ledger,self.cfg['sandbox_pool']);pool.health(self.cma)
        return pool

    def test_draining_slots_remain_available_after_capacity_failure_without_inflight_creation(self):
        pool=self.draining_pool();pool.record_capacity_failure()
        self.ledger.save(force=True);self.restore();self.now+=60
        pool=SandboxPool(self.ledger,self.cfg['sandbox_pool']);pool.health(self.cma)
        available,chosen=pool.slot()
        self.assertTrue(available)
        self.assertEqual(chosen['sandbox_id'],pool.entries[0]['sandbox_id'])
        self.assertTrue(chosen['draining'])

    def test_draining_slots_without_capacity_failure_request_new_sandbox(self):
        pool=self.draining_pool()
        self.assertEqual(pool.slot(),(True,None))

    def test_capacity_fallback_uses_default_96_placement_ceiling(self):
        pool=self.draining_pool(agents_placed=96);pool.record_capacity_failure()
        self.assertEqual(pool.slot(),(True,None))
        # The same hard cap also binds the original in-flight retry fallback.
        row=self.queue('replacement')
        self.ledger.tasks.update(row['id'],row['revision'],'provisioning',
            {'pool_create':True,'operation_id':'op_x','capacity_retries':2})
        self.assertEqual(pool.slot(),(False,None))
        pool.entries[0]['agents_placed']=95
        available,chosen=pool.slot()
        self.assertTrue(available);self.assertIs(chosen,pool.entries[0])
        pool.assigned(chosen)
        self.assertEqual(pool.slot(),(False,None))

    def test_capacity_fallback_ceiling_is_configurable_and_validated(self):
        self.cfg['sandbox_pool']['quota_blocked_max_agents_placed']=60
        pool=self.draining_pool(agents_placed=59);pool.record_capacity_failure()
        self.assertIsNotNone(pool.slot()[1])
        for entry in pool.entries:pool.assigned(entry)
        self.assertEqual(pool.slot(),(True,None))
        for value in (0,-1,True,96.5,'96'):
            with self.subTest(value=value):
                self.cfg['sandbox_pool']['quota_blocked_max_agents_placed']=value
                with self.assertRaisesRegex(ValueError,'quota_blocked_max_agents_placed'):
                    SandboxPool(self.ledger,self.cfg['sandbox_pool'])

    def test_quota_block_at_48_keeps_one_live_agent_and_admits_five_more(self):
        entry=self.seed_pool()[0];entry['agents_placed']=48
        self.ledger.tasks.db.execute('UPDATE pipeline_state SET payload=? WHERE key=?',
                                     (canonical([entry]),'sandbox_pool'))
        self.occupy(entry,0,state='running')
        SandboxPool(self.ledger,self.cfg['sandbox_pool']).record_capacity_failure()
        row=self.queue('replacement')
        self.ledger.tasks.update(row['id'],row['revision'],'provisioning',
            {'pool_create':True,'operation_id':'op_quota','capacity_retries':24})
        self.cma.operations['op_quota']={'status':'failed','error':'sandbox limit reached: 10 of 10'}
        for i in range(6):self.queue('waiting-'+str(i))
        self.tick()
        self.assertEqual(len(self.cma.creates),5)
        self.assertTrue(all(b.get('workspace_id')==entry['workspace_id'] for b in self.cma.creates.values()))
        self.assertEqual(self.state()[0]['agents_placed'],53)
        self.assertEqual(self.state()[0]['state'],'active')
        self.assertEqual(self.ledger.tasks.get('waiting-5')['state'],'queued')
        self.assertFalse(self.state('sandbox_pool_cleanup'))
        create=self.cma.create
        self.cma.create=lambda body,key: (create(body,key) if body.get('workspace_id') else {'id':'op_quota'})
        self.restore();self.tick()
        self.assertEqual(len(self.cma.creates),5)

    def test_empty_healthy_draining_sandbox_survives_quota_block_and_idle_grace(self):
        entry=self.seed_pool()[0];entry['agents_placed']=24
        self.ledger.tasks.db.execute('UPDATE pipeline_state SET payload=? WHERE key=?',
                                     (canonical([entry]),'sandbox_pool'))
        pool=SandboxPool(self.ledger,self.cfg['sandbox_pool']);pool.record_capacity_failure()
        self.now+=601;self.tick();self.restore()
        self.assertEqual(self.state()[0]['state'],'active')
        self.assertFalse(self.state('sandbox_pool_cleanup'))
        self.queue('reuse');self.tick()
        self.assertEqual(self.ledger.tasks.get('reuse')['state'],'running')
        self.assertEqual(next(iter(self.cma.creates.values()))['workspace_id'],entry['workspace_id'])
        self.assertFalse(any(method=='DELETE' for _,method,_ in self.cma.calls))

    def test_empty_sandbox_at_quota_ceiling_retires_without_reporting_failure(self):
        entry=self.seed_pool()[0];entry['agents_placed']=96
        self.ledger.tasks.db.execute('UPDATE pipeline_state SET payload=? WHERE key=?',
                                     (canonical([entry]),'sandbox_pool'))
        SandboxPool(self.ledger,self.cfg['sandbox_pool']).record_capacity_failure()
        self.tick()
        self.assertEqual(self.state(),[])
        self.assertEqual(self.state('sandbox_pool_cleanup')[0]['state'],'retiring')
        self.assertEqual(self.state('sandbox_pool_cleanup')[0]['rotated'],'draining')

    def test_soft_rotation_without_quota_block_is_retirement_not_failure(self):
        self.cfg['sandbox_pool']['rotate_after_agents']=1
        self.seed_pool();self.tick()
        self.assertEqual(self.state()[0]['agents_placed'],0)
        entry=self.state()[0];entry['agents_placed']=1
        self.ledger.tasks.db.execute('UPDATE pipeline_state SET payload=? WHERE key=?',
                                     (canonical([entry]),'sandbox_pool'))
        self.tick()
        self.assertEqual(self.state(),[])
        self.assertEqual(self.state('sandbox_pool_cleanup')[0]['state'],'retiring')

    def test_pending_creation_timeout_requeues_with_evidence_and_a_fresh_key(self):
        self.cma.create_status='queued';self.queue('pending');self.tick()
        pending=self.ledger.tasks.get('pending')
        self.assertEqual(pending['data']['pool_create_started_at'],self.now)
        self.now+=POOL_WAIT_SECONDS-1;self.tick()
        self.assertEqual(len(self.cma.creates),1)
        self.restore();self.now+=1;self.tick()
        self.assertEqual(len(self.cma.creates),1)
        recovered=self.ledger.tasks.get('pending')
        self.assertEqual(recovered['state'],'queued')
        history=recovered['data']['sandbox_failure_history'][0]
        self.assertEqual(history['operation_id'],pending['data']['operation_id'])
        self.assertEqual(history['create_key'],pending['data']['create_key'])
        self.assertEqual(history['sandbox_id'],'sbx_1')
        self.assertEqual(history['failure'],{'class':'sandbox_creation_stalled','started_at':1000})
        self.assertEqual(self.state('sandbox_pool_capacity'),{'capacity_blocked_at':self.now})
        self.cma.create_status='succeeded'
        self.tick()
        retried=self.ledger.tasks.get('pending')
        self.assertEqual(retried['state'],'running')
        self.assertNotEqual(retried['data']['create_key'],pending['data']['create_key'])
        self.assertIn('-sandbox1',retried['data']['create_key'])
        self.assertEqual(retried['data']['pool_create_started_at'],self.now)
        self.assertEqual(self.state('sandbox_pool_capacity'),{})

    def test_legacy_creation_wait_timestamp_survives_repeated_observations(self):
        row=self.queue('pending')
        self.ledger.tasks.update(row['id'],row['revision'],'provision-unknown',
            {'pool_create':True,'operation_id':'op_pending','last_error':'HTTPError: HTTP Error 409: Conflict'})
        self.cma.operations['op_pending']={'status':'queued'}
        self.now+=POOL_WAIT_SECONDS-1;self.tick()
        self.assertEqual(self.ledger.tasks.get('pending')['data']['pool_create_started_at'],1000)
        self.restore();self.now+=1
        pool=SandboxPool(self.ledger,self.cfg['sandbox_pool']);pool.health(self.cma)
        self.assertEqual(pool.slot(),(True,None))
        self.tick()
        self.assertEqual(self.ledger.tasks.get('pending')['state'],'queued')
        self.assertEqual(self.ledger.tasks.get('pending')['data']['sandbox_failure_history'][0]['failure']['started_at'],1000)

    def test_stalled_creation_is_capacity_evidence_before_reconciliation(self):
        pool=self.draining_pool()
        row=self.queue('pending')
        self.ledger.tasks.update(row['id'],row['revision'],'provisioning',
            {'pool_create':True,'pool_create_started_at':self.now})
        self.now+=POOL_WAIT_SECONDS-1
        self.assertEqual(pool.slot(),(False,None))
        self.now+=1
        self.assertIsNotNone(pool.slot()[1])
        # An unreconciled creation remains evidence beyond the cached quota TTL.
        self.now+=CAPACITY_BLOCK_SECONDS
        self.assertIsNotNone(pool.slot()[1])

    def test_stuck_provisioning_sandbox_requeues_and_fills_draining_free_slots(self):
        entry=self.seed_pool()[0];entry['agents_placed']=24
        self.ledger.tasks.db.execute('UPDATE pipeline_state SET payload=? WHERE key=?',
                                     (canonical([entry]),'sandbox_pool'))
        self.occupy(entry,0,state='running')
        self.cma.create_status='queued';self.queue('pending');self.tick()
        pending=self.ledger.tasks.get('pending')
        self.cma.statuses['sbx_1']={'status':'provisioning'}
        self.cma.foreign_agents=[{'id':'agt_foreign','status':'active'}]
        for i in range(6):self.queue('waiting-'+str(i))
        self.ledger.save(force=True)
        self.restore();self.now+=POOL_WAIT_SECONDS
        create=self.cma.create
        self.cma.operations['op_quota']={'status':'failed','error':'sandbox limit reached: 10 of 10'}
        self.cma.create=lambda body,key: create(body,key) if body.get('workspace_id') else {'id':'op_quota'}
        self.cma.create_status='succeeded';self.tick()
        self.assertEqual(self.ledger.tasks.get('pending')['state'],'queued')
        self.assertEqual(self.state()[0]['agents_placed'],29)
        self.assertEqual(len(self.cma.creates),6)  # Original creation and five free slots.
        self.assertTrue(all(body.get('workspace_id')==entry['workspace_id']
                            for body in list(self.cma.creates.values())[1:]))
        self.assertEqual(self.ledger.tasks.get('waiting-5')['state'],'queued')
        failed=self.state('sandbox_pool_cleanup')[0]
        self.assertEqual(failed['sandbox_id'],'sbx_1')
        self.assertEqual(failed['stalled'],'provisioning')
        self.tick()  # Cleanup uses the ordinary foreign-work guard.
        failed=self.state('sandbox_pool_cleanup')[0]
        self.assertEqual(failed['cleanup']['reason'],'Other Agent work found; retained Sandbox')
        self.assertFalse(any(method=='DELETE' for _,method,_ in self.cma.calls))
        self.assertEqual(self.cma.retire_calls[0][0],pending['data']['agent_id'])
        self.assertIsNone(self.cma.retire_calls[0][2])

    def test_successful_lifecycle_without_workspace_keeps_original_deadline_and_capacity_evidence(self):
        self.queue('pending')
        SandboxPool(self.ledger,self.cfg['sandbox_pool']).record_capacity_failure()
        original_request=self.cma.request
        def no_workspace(path,*args,**kwargs):
            if path.endswith('/workspaces'):return {'items':[]}
            return original_request(path,*args,**kwargs)
        self.cma.request=no_workspace
        self.tick();pending=self.ledger.tasks.get('pending')
        self.assertEqual(pending['state'],'provisioning')
        self.assertEqual(self.state('sandbox_pool_capacity'),{'capacity_blocked_at':1000})
        self.now+=POOL_WAIT_SECONDS-1;self.tick();self.restore()
        self.assertEqual(self.ledger.tasks.get('pending')['data']['pool_create_started_at'],1000)
        self.now+=1;self.tick()
        row=self.ledger.tasks.get('pending')
        self.assertEqual(row['state'],'queued')
        self.assertEqual(row['data']['sandbox_failure_history'][0]['sandbox_id'],'sbx_1')
        self.assertEqual(len(self.cma.creates),1)

    def test_reprovisioning_preserves_existing_creation_start(self):
        row=self.queue('pending')
        self.ledger.tasks.update(row['id'],row['revision'],'queued',
            {'pool_create':True,'pool_create_started_at':self.now})
        self.now+=POOL_WAIT_SECONDS-1;self.cma.create_status='queued';self.tick()
        self.assertEqual(self.ledger.tasks.get('pending')['data']['pool_create_started_at'],1000)
        self.now+=1;self.tick()
        self.assertEqual(self.ledger.tasks.get('pending')['state'],'queued')
        self.assertEqual(len(self.cma.creates),1)

    def test_stalled_creation_without_known_agent_preserves_operation_for_recovery(self):
        row=self.queue('pending')
        self.ledger.tasks.update(row['id'],row['revision'],'provisioning',{
            'pool_create':True,'pool_create_started_at':self.now,'operation_id':'op_pending',
            'create_key':'original-key','create_body':{}})
        self.cma.operations['op_pending']={'status':'queued'}
        self.now+=POOL_WAIT_SECONDS;self.tick()
        row=self.ledger.tasks.get('pending')
        self.assertEqual(row['state'],'queued')
        self.assertEqual(row['data']['sandbox_failure_history'][0]['operation_id'],'op_pending')
        self.assertEqual(row['data']['previous_operations'],[
            {'operation_id':'op_pending','create_key':'original-key'}])
        self.assertFalse(self.cma.retire_calls)

    def test_admission_unknown_blocks_new_sandbox_only_without_workspace_and_before_timeout(self):
        pool=self.draining_pool(agents_placed=48)
        row=self.queue('conflict')
        data={'pool_create':True,'agent_id':'agt_conflict',
              'last_error':'HTTPError: HTTP Error 409: Conflict'}
        row=self.ledger.tasks.update(row['id'],row['revision'],'admission-unknown',data)
        self.assertEqual(pool.slot(),(False,None))
        row=self.ledger.tasks.update(row['id'],row['revision'],row['state'],{**data,'workspace_id':'wks_known'})
        self.assertEqual(pool.slot(),(True,None))
        self.ledger.tasks.update(row['id'],row['revision'],row['state'],data)
        pool.record_capacity_failure()
        # An unknown creation never blocks healthy existing slots in a quota block.
        self.assertIsNotNone(pool.slot()[1])
        pool.clear_capacity_failure();self.now+=POOL_WAIT_SECONDS
        self.assertIsNotNone(pool.slot()[1])

    def test_admission_409_wait_is_bounded_then_requeues_only_after_deletion(self):
        self.queue('conflict')
        def conflict(*args):raise CMARequestError(409,'Conflict')
        self.cma.admit=conflict
        self.tick();original=self.ledger.tasks.get('conflict')['data']
        self.assertEqual(self.ledger.tasks.get('conflict')['state'],'admission-unknown')
        self.assertIn('workspace_id',original)  # This row cannot trigger the Workspace gate.
        self.restore();self.now+=POOL_WAIT_SECONDS-1;self.tick()
        self.assertFalse(self.cma.retire_calls)
        retire=self.cma.retire
        self.cma.retire=lambda *args: {'deleted':False,'status':'stopping'}
        self.now+=1;self.tick()
        self.assertEqual(self.ledger.tasks.get('conflict')['state'],'admission-unknown')
        self.assertEqual(len(self.cma.creates),1)
        self.cma.retire=retire;self.tick()
        recovered=self.ledger.tasks.get('conflict')
        self.assertEqual(recovered['state'],'queued')
        self.assertEqual(recovered['data']['admission_history'][0]['agent_id'],original['agent_id'])
        self.assertEqual(recovered['data']['admission_history'][0]['command_key'],original['command_key'])
        self.assertEqual(len(self.cma.creates),1)
        self.cma.admit=PoolCMA.admit.__get__(self.cma);self.tick()
        self.assertEqual(self.ledger.tasks.get('conflict')['state'],'running')
        self.assertEqual(len(self.cma.creates),2)

    def test_legacy_admission_409_uses_saved_age_for_recovery(self):
        entry=self.seed_pool()[0];row=self.queue('legacy-conflict')
        self.ledger.tasks.update(row['id'],row['revision'],'admission-unknown',{
            'pool_create':True,'pool_sandbox_id':entry['sandbox_id'],'workspace_id':entry['workspace_id'],
            'agent_id':'agt_legacy','prompt':'Original prompt','command_key':'original-command',
            'last_error':'HTTPError: HTTP Error 409: Conflict'})
        self.now+=POOL_WAIT_SECONDS
        def conflict(*args):raise CMARequestError(409,'Conflict')
        self.cma.admit=conflict;self.tick()
        row=self.ledger.tasks.get(row['id'])
        self.assertEqual(row['state'],'queued')
        self.assertEqual(row['data']['admission_history'][0]['admission_conflict_since'],1000)
        self.assertFalse(self.cma.creates)

    def test_capacity_failure_fallback_expires_after_two_hours(self):
        pool=self.draining_pool();pool.record_capacity_failure()
        self.now+=CAPACITY_BLOCK_SECONDS-1
        self.assertIsNotNone(pool.slot()[1])
        self.now+=1
        self.assertEqual(pool.slot(),(True,None))

    def test_capacity_fallback_requires_observed_active_status_and_no_intent(self):
        pool=self.draining_pool();pool.record_capacity_failure()
        for status,intent in [(None,None),('paused',None),('active',{'action':'pause'})]:
            with self.subTest(status=status,intent=intent):
                for entry in pool.entries:
                    pool.observed[entry['sandbox_id']]={'status':status}
                    entry['intent']=intent
                self.assertIsNone(pool.slot()[1])

    def test_quota_failure_at_retry_limit_keeps_draining_capacity_across_ticks(self):
        from managed.control import CAPACITY_RETRIES
        self.draining_pool()
        row=self.queue('replacement')
        self.ledger.tasks.update(row['id'],row['revision'],'provisioning',
            {'pool_create':True,'operation_id':'op_quota','capacity_retries':CAPACITY_RETRIES})
        self.cma.operations['op_quota']={'status':'failed','error':'sandbox limit reached: 10 of 10'}
        self.queue('waiting');self.tick()
        self.assertEqual(self.ledger.tasks.get('replacement')['state'],'failed')
        self.assertEqual(self.ledger.tasks.get('waiting')['state'],'running')
        self.assertEqual(self.state('sandbox_pool_capacity'),{'capacity_blocked_at':self.now})
        self.restore();self.now+=60;self.queue('next-tick');self.tick()
        self.assertEqual(self.ledger.tasks.get('next-tick')['state'],'running')
        self.assertTrue(all(body.get('workspace_id','').startswith('wks_existing')
                            for body in self.cma.creates.values()))
        # Reusing a Workspace succeeds without clearing the Sandbox quota block.
        self.assertEqual(self.state('sandbox_pool_capacity'),{'capacity_blocked_at':1000})

    def test_failed_pool_creation_recheck_records_capacity_failure(self):
        self.draining_pool()
        row=self.queue('replacement')
        self.ledger.tasks.update(row['id'],row['revision'],'failed',
            {'pool_create':True,'operation_id':'op_quota',
             'reason':'CMA provisioning failed; retained original operation'})
        self.cma.operations['op_quota']={'status':'failed','error':'sandbox limit reached: 10 of 10'}
        self.queue('waiting');self.tick()
        self.assertEqual(self.state('sandbox_pool_capacity'),{'capacity_blocked_at':self.now})
        self.assertEqual(self.ledger.tasks.get('waiting')['state'],'running')

    def test_counter_checkpoint_and_restore_include_first_and_reused_agents(self):
        self.queue('first');self.tick()
        self.assertEqual(self.state()[0]['agents_placed'],1)
        self.restore();self.queue('second')
        create=self.cma.create
        def checked_create(body,key):
            with tempfile.TemporaryDirectory() as folder:
                saved=Ledger(self.remote,folder)
                try:
                    entry=json.loads(saved.tasks.db.execute(
                        "SELECT payload FROM pipeline_state WHERE key='sandbox_pool'").fetchone()[0])[0]
                    self.assertEqual(entry['agents_placed'],2)
                    self.assertEqual(saved.tasks.get('second')['data']['workspace_id'],entry['workspace_id'])
                finally:saved.tasks.close()
            return create(body,key)
        self.cma.create=checked_create;self.tick();self.restore();self.tick()
        self.assertEqual(self.state()[0]['agents_placed'],2)
        self.assertEqual(len(self.cma.creates),2)

    def test_uncertain_creation_does_not_count_twice_after_restore(self):
        self.seed_pool();self.queue('uncertain')
        self.cma.create_error=TimeoutError('Lost creation response');self.tick()
        before=self.ledger.tasks.get('uncertain')
        self.assertEqual(before['state'],'provision-unknown')
        self.assertEqual(self.state()[0]['agents_placed'],1)
        self.restore();self.tick()
        self.assertEqual(self.ledger.tasks.get('uncertain')['data']['create_key'],before['data']['create_key'])
        self.assertEqual(self.state()[0]['agents_placed'],1)

    def test_threshold_stops_same_tick_admission_and_opens_fresh_under_limit(self):
        self.cfg['sandbox_pool'].update(rotate_after_agents=1,max_sandboxes=2)
        for name in ('first','second','waiting'):self.queue(name)
        self.tick()
        self.assertEqual([b.get('workspace_id') for b in self.cma.creates.values()],[None,None])
        self.assertEqual([e['agents_placed'] for e in self.state()],[1,1])
        self.assertTrue(all(e['draining'] for e in self.state()))
        self.assertEqual(self.ledger.tasks.get('waiting')['state'],'queued')
        self.restore();self.tick();self.assertEqual(len(self.cma.creates),2)

    def test_draining_existing_active_or_paused_capacity_is_never_reused(self):
        self.cfg['sandbox_pool']['rotate_after_agents']=1
        entry=self.seed_pool()[0];self.occupy(entry,0)
        self.cma.statuses[entry['sandbox_id']]={'status':'paused'}
        self.queue('fresh');self.tick()
        self.assertTrue(self.state()[0]['draining'])
        self.assertNotIn('workspace_id',next(iter(self.cma.creates.values())))
        self.assertFalse(any(method=='POST' for _,method,_ in self.cma.calls))

    def test_existing_entry_reaches_default_threshold_and_next_task_opens_fresh(self):
        entry=self.seed_pool()[0];entry['agents_placed']=23
        self.ledger.tasks.db.execute('UPDATE pipeline_state SET payload=? WHERE key=?',
                                     (canonical([entry]),'sandbox_pool'))
        self.queue('last-placement');self.queue('fresh');self.tick()
        self.assertEqual([b.get('workspace_id') for b in self.cma.creates.values()],
                         [entry['workspace_id'],None])
        self.assertEqual(self.state()[0]['agents_placed'],24)
        self.assertTrue(self.state()[0]['draining'])
        self.restore();self.tick()
        self.assertEqual(self.state()[0]['agents_placed'],24)
        self.assertTrue(self.state()[0]['draining'])

    def test_draining_entry_waits_for_live_agent_then_retires_with_stable_key(self):
        self.cfg['sandbox_pool']['rotate_after_agents']=1;self.cfg['paused']=True
        entry=self.seed_pool()[0];row=self.occupy(entry,0);self.tick()
        self.assertTrue(self.state()[0]['draining'])
        self.assertFalse(self.state('sandbox_pool_cleanup'))
        self.assertFalse(any(method=='DELETE' for _,method,_ in self.cma.calls))
        self.restore();row=self.ledger.tasks.get(row['id'])
        self.ledger.tasks.update(row['id'],row['revision'],'held',{**row['data'],'cleanup':{'deleted':True}})
        self.cma.retire_errors=[TimeoutError('No response')];self.tick()
        self.assertEqual(self.state(),[])
        self.assertEqual(self.state('sandbox_pool_cleanup')[0]['rotated'],'draining')
        self.restore();self.tick()
        deletes=[c for c in self.cma.calls if c[1]=='DELETE']
        self.assertEqual([c[2] for c in deletes],['outreach-pool-retire-'+entry['sandbox_id']+'-sandbox-delete']*2)
        self.cma.statuses[entry['sandbox_id']]={'status':'retired'};self.tick()
        self.assertEqual(self.state('sandbox_pool_cleanup'),[])

    def test_last_agent_completed_this_tick_rotates_without_idle_grace(self):
        self.cfg['sandbox_pool']['rotate_after_agents']=1
        row=self.queue('complete')
        result={'schema_version':1,'task_id':row['id'],'outcome':'completed',
                'report':'Checked result.','artifacts':[],'packet':None}
        self.cma.response=lambda _: {'status':'completed','output':[{'type':'message',
            'content':[{'type':'output_text','text':json.dumps(result)}]}]}
        self.tick()
        self.assertTrue(self.ledger.tasks.get(row['id'])['data']['cleanup']['deleted'])
        self.assertEqual(self.state(),[])
        self.assertEqual(self.state('sandbox_pool_cleanup')[0]['rotated'],'draining')
        self.assertTrue(any(method=='DELETE' for _,method,_ in self.cma.calls))

    def test_legacy_usage_includes_nested_execution_and_sandbox_failure_history(self):
        entry=self.seed_pool()[0]
        old={'sandbox_id':entry['sandbox_id'],'agent_id':'agt_old','create_key':'create-old'}
        row=self.queue('restarted')
        self.ledger.tasks.update(row['id'],row['revision'],'held',{
            'sandbox_id':'sbx_elsewhere','agent_id':'agt_elsewhere',
            'execution_history':[old,{'workspace_id':entry['workspace_id'],'agent_id':'agt_second',
                'sandbox_failure_history':[old]}],
            'sandbox_failure_history':[{'sandbox_id':entry['sandbox_id'],'agent_id':'agt_old'}]})
        row=self.queue('current')
        self.ledger.tasks.update(row['id'],row['revision'],'held',{
            'owned_sandbox_id':entry['sandbox_id'],'agent_id':'agt_current',
            'sandbox_failure_history':[{'sandbox_id':entry['sandbox_id'],'agent_id':'agt_current'}]})
        row=self.queue('pending')
        self.ledger.tasks.update(row['id'],row['revision'],'provision-unknown',{
            'workspace_id':entry['workspace_id'],'create_key':'pending-key'})
        row=self.queue('reserved')
        self.ledger.tasks.update(row['id'],row['revision'],'provisioning',{'pool_sandbox_id':entry['sandbox_id']})
        self.cfg['sandbox_pool']['rotate_after_agents']=5
        pool=SandboxPool(self.ledger,self.cfg['sandbox_pool']);pool.health(self.cma)
        self.ledger.save(force=True)
        self.assertEqual(self.state()[0]['agents_placed'],5)
        self.assertTrue(self.state()[0]['draining'])
        self.restore()
        # The persisted count survives task/history removal and is never reset.
        self.ledger.tasks.db.execute('DELETE FROM tasks')
        pool=SandboxPool(self.ledger,self.cfg['sandbox_pool'])
        self.assertEqual(pool.entries[0]['agents_placed'],5)
        self.assertTrue(pool.entries[0]['draining'])

    def test_draining_sandbox_accepts_work_while_replacement_is_capacity_blocked(self):
        entry=self.seed_pool()[0];entry['agents_placed']=24
        self.ledger.tasks.db.execute('UPDATE pipeline_state SET payload=? WHERE key=?',
                                     (canonical([entry]),'sandbox_pool'))
        self.occupy(entry,0)  # A live Agent keeps the draining Sandbox in the pool.
        row=self.queue('replacement')
        self.ledger.tasks.update(row['id'],row['revision'],'provisioning',
                                 {'pool_create':True,'operation_id':'op_x'})
        pool=SandboxPool(self.ledger,self.cfg['sandbox_pool']);pool.health(self.cma)
        self.assertTrue(pool.entries[0]['draining'])
        # A first creation attempt in flight still waits for its Workspace.
        self.assertEqual(pool.slot(),(False,None))
        row=self.ledger.tasks.get('replacement')
        self.ledger.tasks.update(row['id'],row['revision'],'provisioning',
                                 {'pool_create':True,'operation_id':'op_x','capacity_retries':2})
        available,chosen=pool.slot()
        self.assertTrue(available)
        self.assertEqual(chosen['sandbox_id'],entry['sandbox_id'])

    def test_foreign_agents_retain_rotated_sandbox_and_cleanup_backoff(self):
        from managed.sandbox_pool import RETAINED_RECHECK_SECONDS
        entry=self.seed_pool()[0];self.cfg['paused']=True
        self.cma.foreign_agents=[{'id':'agt_foreign','status':'active'}]
        self.now+=600;self.tick()
        self.assertEqual(self.state(),[])
        cleanup=self.state('sandbox_pool_cleanup')[0]
        self.assertEqual(cleanup['rotated'],'idle')
        self.assertIn('Other Agent work',cleanup['cleanup']['reason'])
        self.assertFalse(any(method=='DELETE' for _,method,_ in self.cma.calls))
        reads=len(self.cma.calls);self.restore();self.now+=RETAINED_RECHECK_SECONDS-1;self.tick()
        self.assertEqual(len(self.cma.calls),reads)
        self.now+=1;self.cma.foreign_agents=[];self.tick()
        self.assertIn((f"/api/v1/sandboxes/{entry['sandbox_id']}",'DELETE',
                       'outreach-pool-retire-'+entry['sandbox_id']+'-sandbox-delete'),self.cma.calls)
        self.cma.statuses[entry['sandbox_id']]={'status':'retired'};self.tick()
        self.assertEqual(self.state('sandbox_pool_cleanup'),[])

    def test_rotation_config_requires_positive_integer_and_defaults_to_24(self):
        config=self.cfg['sandbox_pool']
        self.assertEqual(SandboxPool(self.ledger,config).rotate_after_agents,24)
        for value in (None,True,False,0,-1,1.5,'24',[],{}):
            with self.subTest(value=value),self.assertRaisesRegex(ValueError,'rotate_after_agents'):
                SandboxPool(self.ledger,{**config,'rotate_after_agents':value})
        for value in (1,24,100):
            self.assertEqual(SandboxPool(self.ledger,{**config,'rotate_after_agents':value}).rotate_after_agents,value)


class PoolLifecycleTests(PoolFixture,unittest.TestCase):
    def setUp(self):
        super().setUp()
        environment=patch.dict('os.environ',{'OUTREACH_POOL_IDLE_PAUSE_SECONDS':'600'})
        environment.start();self.addCleanup(environment.stop)

    def lifecycle_calls(self,action=None):
        return [call for call in self.cma.calls if call[1]=='POST' and
                (action is None or call[0].endswith('/'+action))]

    def check_checkpoint(self,sandbox_id,action,key):
        with tempfile.TemporaryDirectory() as folder:
            saved=Ledger(self.remote,folder)
            try:
                entries=json.loads(saved.tasks.db.execute(
                    "SELECT payload FROM pipeline_state WHERE key='sandbox_pool'").fetchone()[0])
                entry=next(e for e in entries if e['sandbox_id']==sandbox_id)
                self.assertEqual(entry['intent'],{'action':action,'key':key})
                self.assertEqual(entry['state'],'pausing' if action=='pause' else 'reviving')
            finally:saved.tasks.close()

    def check_retirement_checkpoint(self,sandbox_id,key):
        with tempfile.TemporaryDirectory() as folder:
            saved=Ledger(self.remote,folder)
            try:
                entries=json.loads(saved.tasks.db.execute(
                    "SELECT payload FROM pipeline_state WHERE key='sandbox_pool_cleanup'").fetchone()[0])
                entry=next(e for e in entries if e['sandbox_id']==sandbox_id)
                self.assertEqual(entry['cleanup_intent'],{'key':'outreach-pool-retire-'+sandbox_id})
                self.assertEqual(key,entry['cleanup_intent']['key']+'-sandbox-delete')
                self.assertTrue(entry['rotated'])
                self.assertEqual(entry['state'],'failed')
            finally:saved.tasks.close()

    def retirement_calls(self):
        return [call for call in self.cma.calls if call[1]=='DELETE']

    def test_idle_retirement_threshold_and_forced_checkpoint(self):
        entry=self.seed_pool()[0];self.tick()
        self.now+=599;self.tick();self.assertFalse(self.retirement_calls())
        self.cma.before_retire=self.check_retirement_checkpoint
        self.now+=1;self.tick()
        self.assertEqual(self.retirement_calls(),[(f"/api/v1/sandboxes/{entry['sandbox_id']}",
                         'DELETE','outreach-pool-retire-'+entry['sandbox_id']+'-sandbox-delete')])
        self.assertFalse(self.lifecycle_calls());self.assertEqual(self.state(),[])
        self.assertEqual(self.state('sandbox_pool_cleanup')[0]['rotated'],'idle')
        self.assertEqual(self.state('sandbox_pool_cleanup')[0]['idle_since'],1000)
        self.restore();self.cma.statuses[entry['sandbox_id']]={'status':'retired'};self.tick()
        self.assertEqual(self.state('sandbox_pool_cleanup'),[])

    def test_unread_result_agent_stays_live_until_deleted_then_waits_full_idle_period(self):
        entry=self.seed_pool()[0];row=self.occupy(entry,0)
        self.now+=6000;self.tick();self.assertFalse(self.lifecycle_calls())
        self.assertEqual(self.state()[0]['idle_since'],self.now)
        self.assertFalse(self.ledger.tasks.get(row['id'])['data']['cleanup']['deleted'])
        row=self.ledger.tasks.get(row['id'])
        self.ledger.tasks.update(row['id'],row['revision'],'held',{**row['data'],'cleanup':{'deleted':True}})
        self.now+=599;self.tick();self.assertFalse(self.lifecycle_calls())
        self.now+=1;self.tick();self.assertEqual(len(self.retirement_calls()),1)

    def test_active_reservations_without_agent_are_live_by_every_sandbox_identity(self):
        entry=self.seed_pool()[0]
        for field in ('sandbox_id','pool_sandbox_id','owned_sandbox_id','workspace_id'):
            with self.subTest(field=field):
                row=self.queue(field)
                data={field:entry['workspace_id'] if field=='workspace_id' else entry['sandbox_id']}
                self.ledger.tasks.update(row['id'],row['revision'],'provision-unknown',data)
        self.now+=10000
        pool=SandboxPool(self.ledger,self.cfg['sandbox_pool']);pool.health(self.cma);pool.idle(self.cma)
        self.assertFalse(self.lifecycle_calls());self.assertEqual(summary(self.ledger.tasks)[0]['live_agents'],4)

    def test_demand_revives_instead_of_creating_and_waits_for_get_active(self):
        entry=self.seed_pool()[0];self.occupy(entry,0);self.cma.statuses[entry['sandbox_id']]={'status':'paused'}
        queued=self.queue('wake');self.cma.before_lifecycle=self.check_checkpoint
        self.tick()
        self.assertEqual(self.ledger.tasks.get('wake'),queued)
        self.assertFalse(self.cma.creates);self.assertEqual(len(self.lifecycle_calls('revive')),1)
        self.assertEqual(self.state()[0]['state'],'reviving')
        self.restore();self.tick();self.tick()
        self.assertEqual(len(self.lifecycle_calls('revive')),1);self.assertFalse(self.cma.creates)
        self.cma.statuses[entry['sandbox_id']]={'status':'active'};self.tick()
        self.assertEqual(self.ledger.tasks.get('wake')['state'],'running')
        self.assertEqual(next(iter(self.cma.creates.values()))['workspace_id'],entry['workspace_id'])
        self.assertEqual(len(self.state()),1)

    def test_revive_post_active_view_still_requires_next_get(self):
        entry=self.seed_pool()[0];self.occupy(entry,0);self.cma.statuses[entry['sandbox_id']]={'status':'paused'}
        self.cma.lifecycle_status['revive']='active'
        for i in range(3):self.queue('wake-'+str(i))
        self.tick();self.assertFalse(self.cma.creates)
        self.assertEqual(len(self.lifecycle_calls('revive')),1)
        self.tick();self.assertEqual(len(self.cma.creates),3)
        self.assertTrue(all(b.get('workspace_id')==entry['workspace_id'] for b in self.cma.creates.values()))

    def test_pausing_capacity_waits_for_pause_before_demand_can_revive(self):
        entry=self.seed_pool()[0];self.occupy(entry,0);self.cma.statuses[entry['sandbox_id']]={'status':'pausing'}
        self.queue('waiting');self.tick();self.tick()
        self.assertFalse(self.cma.creates);self.assertFalse(self.lifecycle_calls())
        self.cma.statuses[entry['sandbox_id']]={'status':'paused'};self.tick()
        self.assertEqual(len(self.lifecycle_calls('revive')),1);self.assertFalse(self.cma.creates)

    def test_legacy_paused_empty_entry_retires_instead_of_reviving(self):
        entry=self.seed_pool()[0];self.cma.statuses[entry['sandbox_id']]={'status':'paused'}
        self.queue('new-work');self.tick()
        self.assertFalse(self.lifecycle_calls())
        self.assertEqual(self.state('sandbox_pool_cleanup')[0]['rotated'],'paused')
        self.assertEqual(len(self.retirement_calls()),1)
        self.assertNotIn('workspace_id',next(iter(self.cma.creates.values())))
        self.assertNotEqual(self.state()[0]['sandbox_id'],entry['sandbox_id'])

    def test_available_active_capacity_precedes_paused_capacity(self):
        entries=self.seed_pool(2);self.cma.statuses[entries[0]['sandbox_id']]={'status':'paused'}
        self.queue('reuse');self.tick()
        self.assertFalse(self.lifecycle_calls())
        self.assertEqual(next(iter(self.cma.creates.values()))['workspace_id'],entries[1]['workspace_id'])

    def test_paused_entries_count_toward_cap_but_full_entries_allow_creation_below_cap(self):
        entries=self.seed_pool(4)
        for entry in entries:
            self.cma.statuses[entry['sandbox_id']]={'status':'paused'}
            for i in range(6):self.occupy(entry,i)
        self.queue('waiting');self.tick();self.assertFalse(self.cma.creates)
        self.cfg['sandbox_pool']['max_sandboxes']=5;self.tick()
        self.assertEqual(len(self.cma.creates),1)
        self.assertNotIn('workspace_id',next(iter(self.cma.creates.values())))

    def test_scheduled_states_are_healthy_even_with_interruption_metadata(self):
        entries=self.seed_pool(3)
        for entry,status in zip(entries,('paused','pausing','reviving')):
            self.occupy(entry,0)
            self.cma.statuses[entry['sandbox_id']]={'status':status,'lifecycle_interruption':{'reason':'suspended'}}
        self.tick()
        self.assertEqual([e['state'] for e in self.state()],['paused','pausing','reviving'])
        self.assertFalse(self.state('sandbox_pool_cleanup'));self.assertFalse(self.lifecycle_calls())
        self.assertFalse(any(method=='DELETE' for _,method,_ in self.cma.calls))

    def test_revive_409_refusals_retry_only_next_tick_after_get(self):
        for reason in ('sandbox_lifecycle_busy','sandbox_not_paused'):
            with self.subTest(reason=reason):
                self.cma.calls.clear();entry=self.seed_pool()[0]
                self.occupy(entry,reason)
                self.cma.statuses[entry['sandbox_id']]={'status':'paused'}
                self.cma.lifecycle_errors['revive']=CMARequestError(409,reason)
                self.queue(reason);self.tick()
                self.assertEqual(len(self.lifecycle_calls('revive')),1)
                self.assertEqual(self.state()[0]['state'],'paused');self.assertNotIn('intent',self.state()[0])
                self.assertFalse(self.cma.creates)
                self.restore();self.tick()
                calls=self.lifecycle_calls('revive')
                self.assertEqual(len(calls),2);self.assertNotEqual(calls[0][2],calls[1][2])
                for row in self.ledger.tasks.all():
                    self.ledger.tasks.update(row['id'],row['revision'],'superseded',{})

    def test_http_409_reason_body_and_revive_allowed_gate(self):
        import io
        entry=self.seed_pool()[0];self.occupy(entry,0);self.queue('waiting')
        self.cma.statuses[entry['sandbox_id']]={'status':'paused'}
        error=HTTPError('https://cma',409,'Conflict',{},
            io.BytesIO(b'{"code":"sandbox_lifecycle_busy"}'))
        self.cma.lifecycle_errors['revive']=error
        try:self.tick()
        finally:error.close()
        self.assertNotIn('intent',self.state()[0])
        self.cma.statuses[entry['sandbox_id']]={'status':'paused','lifecycle':{'revive':{'allowed':False,'reason':'busy'}}}
        self.tick();self.assertEqual(len(self.lifecycle_calls()),1)
        self.assertFalse(self.cma.creates)

    def test_lost_retirement_response_recovers_by_get_without_second_delete(self):
        entry=self.seed_pool()[0];self.now+=600
        self.cma.lose_retire=True;self.tick()
        self.assertEqual(self.state(),[])
        self.assertIn('response lost',self.state('sandbox_pool_errors')[0]['error'])
        self.restore();self.tick()  # GET retiring: wait rather than delete again.
        self.assertEqual(len(self.retirement_calls()),1)
        self.cma.statuses[entry['sandbox_id']]={'status':'retired'};self.tick()
        self.assertEqual(self.state('sandbox_pool_cleanup'),[])
        self.assertEqual(len(self.retirement_calls()),1)

    def test_uncertain_retirement_replays_same_key_after_restore(self):
        self.seed_pool();self.now+=600
        self.cma.retire_errors=[TimeoutError('No response')];self.tick()
        key=self.state('sandbox_pool_cleanup')[0]['cleanup_intent']['key']
        self.restore();self.tick()
        self.assertEqual([c[2] for c in self.retirement_calls()],[key+'-sandbox-delete']*2)
        self.assertFalse(self.lifecycle_calls())

    def test_lost_revive_response_recovers_without_extra_sandbox(self):
        entry=self.seed_pool()[0];self.occupy(entry,0);self.cma.statuses[entry['sandbox_id']]={'status':'paused'}
        self.cma.lose_lifecycle.add('revive');self.queue('waiting');self.tick();self.restore();self.tick()
        self.assertEqual(len(self.lifecycle_calls('revive')),1);self.assertFalse(self.cma.creates)
        self.cma.statuses[entry['sandbox_id']]={'status':'active'};self.tick()
        self.assertEqual(next(iter(self.cma.creates.values()))['workspace_id'],entry['workspace_id'])

    def test_retirement_503_422_and_idempotency_conflicts_retain_original_intent(self):
        entry=self.seed_pool()[0];self.now+=600
        for code,reason in ((503,'unavailable'),(422,'idempotency_key_reused'),(409,'idempotency_conflict')):
            self.cma.retire_errors=[CMARequestError(code,reason)];self.tick()
            self.assertEqual(self.state('sandbox_pool_cleanup')[0]['cleanup_intent']['key'],
                             'outreach-pool-retire-'+entry['sandbox_id'])
            self.restore()
        self.assertEqual(len(self.retirement_calls()),3)
        keys=[c[2] for c in self.retirement_calls()]
        self.assertEqual(keys[0],keys[1])
        self.assertNotEqual(keys[1],keys[2])

    def test_failed_forced_checkpoint_prevents_retirement(self):
        self.seed_pool();self.tick();self.now+=600;self.remote.fail_pointer=True
        with self.assertRaises(TimeoutError):self.tick()
        self.assertFalse(self.retirement_calls());self.assertFalse(self.lifecycle_calls())

    def test_budget_exhausted_after_health_defers_retirement(self):
        entry=self.seed_pool()[0]
        clock=[0];tick=Tick(1,lambda:clock[0]);tick.bind(self.ledger)
        pool=SandboxPool(self.ledger,self.cfg['sandbox_pool']);pool.health(self.cma)
        self.now+=600;clock[0]=1;pool.idle(self.cma)
        self.assertFalse(self.retirement_calls());self.assertFalse(self.lifecycle_calls())
        self.assertIn(entry['sandbox_id'],tick.pending['pool-health'])
        self.assertTrue(self.state('sandbox_pool_cleanup')[0]['rotated'])

    def test_budget_exhausted_after_health_defers_revive(self):
        entry=self.seed_pool()[0];self.occupy(entry,0)
        self.cma.statuses[entry['sandbox_id']]={'status':'paused'}
        clock=[0];tick=Tick(1,lambda:clock[0]);tick.bind(self.ledger)
        pool=SandboxPool(self.ledger,self.cfg['sandbox_pool']);pool.health(self.cma);clock[0]=1
        self.assertEqual(pool.slot(cma=self.cma),(False,None))
        self.assertFalse(self.lifecycle_calls());self.assertNotIn('intent',self.state()[0])
        self.assertIn(entry['sandbox_id'],tick.pending['pool-health'])

    def test_new_live_reference_prevents_idle_retirement_after_health(self):
        entry=self.seed_pool()[0]
        pool=SandboxPool(self.ledger,self.cfg['sandbox_pool']);pool.health(self.cma)
        self.now+=600;self.occupy(entry,0);pool.idle(self.cma)
        self.assertFalse(self.retirement_calls());self.assertFalse(self.lifecycle_calls())
        self.assertEqual(self.state()[0]['idle_since'],self.now)

    def test_legacy_uncertain_pause_retires_when_empty_and_idle(self):
        entry=self.seed_pool()[0]
        entry.update(state='pausing',intent={'action':'pause','key':'old-pause-key'})
        self.ledger.tasks.db.execute('UPDATE pipeline_state SET payload=? WHERE key=?',
                                     (canonical([entry]),'sandbox_pool'))
        self.now+=600;self.tick()
        self.assertFalse(self.lifecycle_calls());self.assertEqual(len(self.retirement_calls()),1)
        self.assertEqual(self.state('sandbox_pool_cleanup')[0]['rotated'],'idle')

    def test_long_result_readback_refreshes_last_live_time_before_cleanup(self):
        self.seed_pool();row=self.queue('long-result')
        result={'schema_version':1,'task_id':row['id'],'outcome':'completed',
                'report':'Checked result.','artifacts':[],'packet':None}
        def response(_):
            self.now+=700
            return {'status':'completed','output':[{'type':'message',
                'content':[{'type':'output_text','text':canonical(result)}]}]}
        self.cma.response=response;self.tick()
        self.assertTrue(self.ledger.tasks.get(row['id'])['data']['cleanup']['deleted'])
        self.assertEqual(self.state()[0]['idle_since'],self.now);self.assertFalse(self.lifecycle_calls())
        self.now+=599;self.tick();self.assertFalse(self.lifecycle_calls())
        self.now+=1;self.tick();self.assertEqual(len(self.retirement_calls()),1)

    def test_work_exposes_ledger_pool_only_without_cma_calls(self):
        from managed.automation import work
        entries=self.seed_pool(2);self.occupy(entries[0],0);self.occupy(entries[0],1,deleted=True);self.occupy(entries[1],0)
        self.cma.statuses[entries[1]['sandbox_id']]={'status':'paused'};self.tick()
        self.cma.statuses['sbx_not_ours']={'status':'active'}
        with patch('managed.daily_local.status',return_value={}):inventory=work(self.ledger.tasks,self.folder)
        self.assertEqual(inventory['sandbox_pool'],[
            {'sandbox_id':entries[0]['sandbox_id'],'state':'active','live_agents':1,'idle_since':1000},
            {'sandbox_id':entries[1]['sandbox_id'],'state':'paused','live_agents':1,'idle_since':1000}])
        self.assertFalse(any('sbx_not_ours' in path for path,_,_ in self.cma.calls))

    def test_production_pause_allows_idle_retirement_but_never_demand_revival(self):
        entry=self.seed_pool()[0];self.cfg['paused']=True;self.now+=600
        self.queue('waiting');self.tick();self.assertEqual(len(self.retirement_calls()),1)
        self.cma.statuses[entry['sandbox_id']]={'status':'retired'};self.tick()
        self.assertFalse(self.lifecycle_calls('revive'));self.assertFalse(self.cma.creates)
        self.assertEqual(self.state('sandbox_pool_cleanup'),[])

    def test_legacy_entry_gets_full_grace_and_environment_override(self):
        entry=self.seed_pool()[0];entry.pop('idle_since');entry.pop('state')
        self.ledger.tasks.db.execute('UPDATE pipeline_state SET payload=? WHERE key=?',
                                     (canonical([entry]),'sandbox_pool'))
        self.now+=10000;self.tick();self.assertFalse(self.lifecycle_calls())
        self.assertEqual(self.state()[0]['idle_since'],self.now)
        with patch.dict('os.environ',{'OUTREACH_POOL_IDLE_PAUSE_SECONDS':'10'}):
            self.now+=9;self.tick();self.assertFalse(self.lifecycle_calls())
            self.now+=1;self.tick();self.assertEqual(len(self.retirement_calls()),1)
        for value in ('-1','nan','inf','bad'):
            with self.subTest(value=value),patch.dict('os.environ',{'OUTREACH_POOL_IDLE_PAUSE_SECONDS':value}):
                with self.assertRaises(ValueError):SandboxPool(self.ledger,self.cfg['sandbox_pool'])


TRANSPORT_ERROR={'code':'agent_turn_failed',
                 'message':'stream disconnected before completion: Transport error: network error during read'}


class TransportTests(PoolFixture,unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.cfg.pop('sandbox_pool')
        self.row=self.queue('transport')
        self.cma.response=lambda _: {'status':'failed','error':TRANSPORT_ERROR}

    def test_transport_re_admission_is_bounded_at_two_then_retires(self):
        self.tick();first=self.ledger.tasks.get(self.row['id'])
        self.assertEqual(first['state'],'admitting');self.assertEqual(first['data']['transport_retries'],1)
        self.assertFalse(self.cma.retired)
        self.tick();second=self.ledger.tasks.get(self.row['id'])
        self.assertEqual(second['data']['transport_retries'],2);self.assertFalse(self.cma.retired)
        self.tick();row=self.ledger.tasks.get(self.row['id'])
        self.assertEqual(row['state'],'failed');self.assertEqual(row['data']['response_error'],TRANSPORT_ERROR)
        self.assertTrue(row['data']['cleanup']['deleted']);self.assertTrue(row['data']['cleanup']['sandbox_retired'])
        self.assertEqual(len(self.cma.creates),1);self.assertEqual(len(self.cma.commands),3)
        self.assertEqual(len({a for a,_,_ in self.cma.admissions}),1)
        self.assertEqual(len({p for _,p,_ in self.cma.admissions}),1)
        self.assertEqual(len({k for _,_,k in self.cma.admissions}),3)
        self.restore();self.tick();self.tick()
        self.assertEqual(len(self.cma.admissions),3);self.assertEqual(self.cma.retired,['agt_1'])

    def test_retry_intent_checkpoint_and_lost_admission_replays_same_key(self):
        self.tick()
        def checkpoint(agent,prompt,key):
            if '-transport-' not in key:return
            with tempfile.TemporaryDirectory() as temp:
                saved=Ledger(self.remote,temp)
                try:
                    row=saved.tasks.get(self.row['id'])
                    self.assertIn(row['state'],{'admitting','admission-unknown'})
                    expected=2 if row['data'].get('recovery',{}).get('scope')=='observation' else 1
                    self.assertEqual(row['data']['transport_retries'],expected)
                    command=row['data']['transport_commands'][-1]
                    self.assertEqual(command['command_key'],key);self.assertEqual(command['prompt'],prompt)
                finally:saved.tasks.close()
        self.cma.before_admit=checkpoint;self.cma.lose_transport=True
        self.tick();row=self.ledger.tasks.get(self.row['id'])
        self.assertEqual(row['state'],'admission-unknown')
        self.now=row['data']['recovery']['next_at']
        self.restore();self.cma.response=lambda _: {'status':'in_progress'};self.tick()
        row=self.ledger.tasks.get(self.row['id'])
        self.assertEqual(row['state'],'running');self.assertEqual(row['data']['transport_retries'],2)
        self.assertEqual(self.cma.admissions[-1],self.cma.admissions[-2])
        self.assertEqual(len(self.cma.commands),2)

    def test_retry_of_correction_uses_that_same_prompt(self):
        data={'agent_id':'agt_repair','response_id':'resp_repair','prompt':'original prompt',
              'command_key':'original-key','repair_attempts':1,
              'repairs':[{'response_id':'resp_repair','command_key':'repair-key','prompt':'correct exact JSON'}]}
        self.ledger.tasks.update(self.row['id'],self.row['revision'],'running',data)
        self.tick();self.cma.response=lambda _: {'status':'in_progress'};self.tick()
        self.assertEqual(self.cma.admissions[0][:2],('agt_repair','correct exact JSON'))
        self.assertFalse(self.cma.creates)

    def test_transport_retry_can_complete_on_same_pooled_agent(self):
        self.cfg['sandbox_pool']={'max_sandboxes':4,'agents_per_sandbox':6}
        entry=self.seed_pool()[0];self.tick()
        result={'schema_version':1,'task_id':self.row['id'],'outcome':'completed',
                'report':'Recovered transport.','artifacts':[],'packet':None}
        self.cma.response=lambda _: {'status':'completed','output':[{'type':'message',
            'content':[{'type':'output_text','text':json.dumps(result)}]}]}
        self.tick();row=self.ledger.tasks.get(self.row['id'])
        self.assertEqual(row['state'],'awaiting-review');self.assertEqual(row['data']['transport_retries'],1)
        self.assertEqual(len(self.cma.creates),1);self.assertEqual(len(self.cma.commands),2)
        self.assertEqual(self.cma.admissions[0][:2],self.cma.admissions[1][:2])
        self.assertTrue(row['data']['cleanup']['deleted']);self.assertIsNone(self.cma.retire_calls[0][2])
        self.assertEqual(self.state(),[{**entry,'agents_placed':1}])

    def test_other_failures_retire_immediately_without_transport_retry(self):
        for code,message in [('agent_turn_failed','tool execution failed'),
                             ('permission_denied',TRANSPORT_ERROR['message'])]:
            with self.subTest(code=code):
                row=self.queue(code);self.cma.response=lambda _: {'status':'failed','error':{'code':code,'message':message}}
                self.tick();failed=self.ledger.tasks.get(row['id'])
                self.assertEqual(failed['state'],'failed');self.assertNotIn('transport_retries',failed['data'])
                self.assertTrue(failed['data']['cleanup']['deleted'])

    def test_existing_terminal_failure_retries_cleanup_until_agent_deleted(self):
        self.ledger.tasks.update(self.row['id'],self.row['revision'],'failed',
            {'agent_id':'agt_leaked','response_id':'resp_failed','owned_sandbox_id':'sbx_leaked',
             'reason':'Original CMA response ended failed'})
        retire=self.cma.retire
        self.cma.retire=lambda *args: {'deleted':False,'status':'stopping'}
        self.tick();self.assertFalse(self.ledger.tasks.get(self.row['id'])['data']['cleanup']['deleted'])
        self.restore();self.cma.retire=retire;self.tick()
        row=self.ledger.tasks.get(self.row['id'])
        self.assertTrue(row['data']['cleanup']['deleted']);self.assertTrue(row['data']['cleanup']['sandbox_retired'])
        self.assertEqual(self.cma.retire_calls[0][2],'sbx_leaked')


if __name__=='__main__':unittest.main()


class UnhealthySandboxOrderTests(PoolFixture,unittest.TestCase):
    def test_stalled_sandboxes_stop_blocking_a_new_one_within_the_limit(self):
        entries=self.seed_pool(3)
        for entry in entries:self.cma.statuses[entry['sandbox_id']]={'status':'credential_rotating'}
        self.queue('waiting');self.tick()
        self.assertFalse(self.cma.creates)  # A brief rotation does not open another Sandbox.
        self.assertEqual({e.get('unhealthy_since') for e in self.state()},{1000})
        self.now+=2*3600;self.tick()
        self.assertEqual(len(self.cma.creates),1)
        self.assertNotIn('workspace_id',next(iter(self.cma.creates.values())))

    def test_stalled_sandboxes_still_count_against_the_limit(self):
        entries=self.seed_pool(4)
        for entry in entries:self.cma.statuses[entry['sandbox_id']]={'status':'credential_rotating'}
        self.queue('waiting');self.tick();self.now+=3*3600;self.tick()
        self.assertFalse(self.cma.creates)

    def test_recovered_sandbox_clears_its_stall_mark(self):
        entry=self.seed_pool()[0]
        self.cma.statuses[entry['sandbox_id']]={'status':'credential_rotating'};self.tick()
        self.assertIn('unhealthy_since',self.state()[0])
        self.cma.statuses[entry['sandbox_id']]={'status':'active'};self.tick()
        self.assertNotIn('unhealthy_since',self.state()[0])

    def test_rows_on_unhealthy_sandbox_wait_behind_admissions(self):
        entries=self.seed_pool(2)
        self.cma.statuses[entries[1]['sandbox_id']]={'status':'credential_rotating'}
        stuck=self.occupy(entries[1],0,state='running')
        waiting=self.queue('waiting')
        seen=[]
        import managed.control as control
        original=control.units
        def spy(ledger,lane,items,*args,**kwargs):
            items=list(items)
            if lane=='advance':seen.extend(r['id'] for r in items)
            return original(ledger,lane,items,*args,**kwargs)
        with patch.object(control,'units',spy):
            self.tick()
        self.assertLess(seen.index(waiting['id']),seen.index(stuck['id']))
