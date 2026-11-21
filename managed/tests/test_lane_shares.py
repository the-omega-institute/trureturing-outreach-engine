"""Per-kind guarantees prevent continuation-heavy pipelines starving intake."""
import copy
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from fixture_config import test_config
from fixture_config import use_cli_config
from managed import control, lanes, maintenance, thread_staging
from managed.tasks import ACTIVE
from managed.tick import Tick
from test_tasks import CMA, Releases, request, use_fixture_workflow_revisions


CONFIG=test_config()


class LaneSharesTests(unittest.TestCase):
    def setUp(self):
        use_fixture_workflow_revisions(self)
        self.temp=tempfile.TemporaryDirectory();self.folder=Path(self.temp.name)
        self.remote=Releases();self.ledger=control.Ledger(self.remote,self.folder);self.cma=CMA()
        self.cfg={'paused':False,'max_active':4,'kinds':{'shortnote':True,'research':True},
                  'agent_template':{},'kind_quota':'equal'}

    def tearDown(self):
        self.ledger.tasks.close();self.temp.cleanup()

    def queue(self,name,kind='shortnote',owner=None,inputs=None):
        value=request(name,owner or 'owner-'+name,kind)
        value['inputs']=inputs or {}
        return self.ledger.tasks.add(value)

    def running(self,name,kind='shortnote'):
        row=self.queue(name,kind)
        return self.ledger.tasks.update(name,row['revision'],'running',
            {'agent_id':'agt_'+name,'response_id':'resp_'+name,'prompt':'original',
             'command_key':'original-'+name})

    def tick(self):
        control.advance(self.ledger,self.cma,self.cfg,self.folder,revision='b'*40)

    def state(self,name):return self.ledger.tasks.get(name)['state']

    def test_cap_enforced_in_both_directions_when_both_lanes_have_queued_work(self):
        for kind,other in [('shortnote','research'),('research','shortnote')]:
            with self.subTest(kind=kind):
                # Independent ledger per direction, preserving the same arrival order.
                self.ledger.tasks.db.execute('DELETE FROM tasks')
                self.running('active-1',kind);self.running('active-2',kind)
                self.queue('at-cap',kind);self.queue('other-lane',other);self.queue('other-second',other)
                self.tick()
                self.assertEqual(self.state('at-cap'),'queued')
                self.assertEqual(self.state('other-lane'),'running')

    def test_borrow_all_free_capacity_when_other_lane_has_no_queue(self):
        for i in range(6):self.queue('outreach-'+str(i))
        self.tick()
        self.assertEqual(sum(r['state'] in ACTIVE for r in self.ledger.tasks.all()),4)
        self.assertEqual(len(self.cma.creates),4)

    def test_held_disabled_and_policy_blocked_work_does_not_reserve_capacity(self):
        self.running('one');self.running('two')
        held=self.queue('held','research')
        self.ledger.tasks.update(held['id'],held['revision'],'held',{'reason':'Owner hold'})
        self.cfg['kinds']['reply']=False
        self.queue('disabled','reply')
        self.queue('blocked','research',inputs={'pipeline':{'scenario':0}})
        self.queue('borrow')
        self.tick()
        self.assertEqual(self.state('borrow'),'running')
        self.assertEqual(self.state('held'),'held')
        self.assertEqual(self.state('disabled'),'queued')
        self.assertEqual(self.state('blocked'),'held')

    def test_owner_blocked_queue_does_not_reserve_capacity(self):
        active=self.running('one');self.running('two')
        self.queue('owner-blocked','research',owner=active['owner'])
        self.queue('borrow')
        self.tick()
        self.assertEqual(self.state('borrow'),'running')
        self.assertEqual(self.state('owner-blocked'),'queued')

    def test_delayed_or_stale_queue_does_not_reserve_capacity(self):
        self.running('one');self.running('two')
        delayed=self.queue('delayed','research')
        self.ledger.tasks.update(delayed['id'],delayed['revision'],'queued',
            {'recovery':{'status':'retry-scheduled','next_at':self.ledger.tasks.clock()+3600}})
        stale=request('stale','stale-owner','research');stale['workflow_hash']='0'*64
        self.ledger.tasks.add(stale)
        self.queue('borrow')
        self.tick()
        self.assertEqual(self.state('borrow'),'running')
        self.assertEqual(self.state('delayed'),'queued')

    def test_running_tasks_above_new_cap_keep_exact_handles_and_state(self):
        before=[self.running('old-'+str(i)) for i in range(3)]
        self.queue('new-outreach');self.queue('reply-work','research')
        self.tick()
        for row in before:self.assertEqual(self.ledger.tasks.get(row['id']),row)
        self.assertEqual(self.state('new-outreach'),'queued')
        self.assertEqual(self.state('reply-work'),'running')
        self.assertEqual(self.cma.retired,[])

    def test_equal_quotas_remainder_and_disabled_kinds(self):
        self.cfg['max_active']=5
        self.assertEqual(lanes.quotas(self.cfg),{'research':3,'shortnote':2})
        self.cfg['kinds']['daily']=False
        self.assertNotIn('daily',lanes.quotas(self.cfg))
        for index,state in enumerate(sorted(ACTIVE)):
            row=self.queue('active-'+str(index),'research')
            self.ledger.tasks.update(row['id'],row['revision'],state,{})
        self.assertEqual(lanes.summary(self.ledger.tasks.all(),self.cfg)['research'],
                         {'active':len(ACTIVE),'queued':0,'quota':3,'borrowing':max(0,len(ACTIVE)-3)})

    def test_firstcontact_not_starved_by_shortnote_continuations(self):
        self.cfg=copy.deepcopy(CONFIG);self.cfg.pop('sandbox_pool');self.cfg['agent_template']={}
        for i in range(30):self.queue('revision-'+str(i),inputs={'automation_parent':'parent'})
        self.queue('new-contact','firstcontact',inputs={'recipient_emails':['author@example.invalid'],'to':['author@example.invalid'],'cc':[],'excluded_recipients':[]})
        # Under the old global continuation sort all thirty revisions precede intake.
        rows=self.ledger.tasks.all()
        old=sorted(rows,key=lambda r:0 if r['request']['inputs'].get('automation_parent') else 1)
        self.assertEqual(old[-1]['id'],'new-contact')
        Tick(seconds=600).bind(self.ledger)
        # Mock external/policy gates only: exercise actual firstcontact admission.
        with patch.object(control,'queued_gates'), \
                patch('managed.firstcontact_history.resolve'), \
                patch('managed.mailbox.configured',return_value=Mock()):
            self.tick()
        self.assertEqual(self.state('new-contact'),'running',self.ledger.tasks.get('new-contact')['data'])
        self.assertEqual(len(self.cma.creates),24)
        counts=lanes.summary(self.ledger.tasks.all(),self.cfg)
        self.assertEqual(counts['firstcontact']['active'],1)
        self.assertEqual(counts['shortnote']['active'],23)
        self.assertEqual(counts['shortnote']['borrowing'],23-lanes.quotas(self.cfg)['shortnote'])

    def test_followup_lane_separates_correspondent_notes_from_backlog(self):
        self.cfg['followup_lane']=True
        self.assertEqual(lanes.quotas(self.cfg),{'followup':2,'research':1,'shortnote':1})
        backlog=self.queue('backlog');owned=self.queue('owned',inputs={'owner_ids':['contact-1']})
        reply=self.queue('reply-work','research',inputs={'owner_ids':['contact-2']})
        self.assertEqual(lanes.lane(backlog,self.cfg),'shortnote')
        self.assertEqual(lanes.lane(owned,self.cfg),'followup')
        self.assertEqual(lanes.lane(reply,self.cfg),'research')
        self.cfg['followup_lane']=False
        self.assertEqual(lanes.lane(owned,self.cfg),'shortnote')
        with self.assertRaises(ValueError):lanes.validate({**self.cfg,'followup_lane':'yes'})

    def test_followup_shares_are_independent_across_groups(self):
        self.cfg.update(max_active=9, followup_lane=True, sandbox_pool={
            'max_sandboxes': 3, 'agents_per_sandbox': 3, 'groups': [
                {'name': 'notes', 'kinds': ['shortnote'], 'max_sandboxes': 1},
                {'name': 'other', 'kinds': ['mathesis', 'research', 'daily'], 'max_sandboxes': 2},
            ],
        })
        self.cfg['kinds'].update(mathesis=True, daily=False)
        self.running('active-shortnote')
        for index in range(3):
            active = self.queue('active-mathesis-'+str(index), 'mathesis', inputs={'owner_ids': ['contact']})
            self.ledger.tasks.update(active['id'], active['revision'], 'running', {})
        backlog = self.queue('backlog')
        followup = self.queue('waiting-followup', inputs={'owner_ids': ['another-contact']})
        rows = self.ledger.tasks.all()

        self.assertEqual(lanes.quotas(self.cfg), {'followup': 4, 'shortnote': 1, 'mathesis': 2, 'research': 2})
        self.assertTrue(lanes.at_share(backlog, rows, self.cfg, lambda row: row['id'] == followup['id']))
        self.assertEqual(lanes.summary(rows, self.cfg)['followup'],
                         {'active': 3, 'queued': 1, 'quota': 4, 'borrowing': 1})

    def test_followup_note_admitted_while_backlog_lane_is_full(self):
        self.cfg['followup_lane']=True;self.cfg['max_active']=3
        self.running('busy-1');self.running('busy-2')
        self.queue('backlog');self.queue('owned',inputs={'owner_ids':['contact-1']})
        self.tick()
        self.assertEqual(self.state('owned'),'running')
        self.assertEqual(self.state('backlog'),'queued')

    def test_owner_gets_next_free_slot_then_borrowing_resumes(self):
        before=[self.running('old-'+str(i)) for i in range(3)]
        self.queue('borrower');self.queue('owner','research')
        self.tick()
        self.assertEqual(self.state('owner'),'running')
        self.assertEqual(self.state('borrower'),'queued')
        for row in before:self.assertEqual(self.ledger.tasks.get(row['id']),row)

    def test_skipped_borrower_is_revisited_after_owner_demand_is_satisfied(self):
        self.running('one');self.running('two')
        self.queue('borrower');self.queue('owner','research')
        self.tick()
        self.assertEqual(self.state('owner'),'running')
        # A borrower skipped for owner demand is admitted on the next tick; the
        # advance phase no longer re-sorts the whole queue after every admission.
        self.tick()
        self.assertEqual(self.state('borrower'),'running')

    def test_lane_skip_happens_before_pool_slot_allocation(self):
        self.running('one');self.running('two')
        self.queue('at-cap');self.queue('other','research');self.queue('other-second','research')
        self.cfg['sandbox_pool']={'max_sandboxes':1,'agents_per_sandbox':4}
        pool=Mock();pool.slot.return_value=(True,{'sandbox_id':'sbx_shared','workspace_id':'wks_shared'})
        with patch('managed.sandbox_pool.SandboxPool',return_value=pool):self.tick()
        self.assertEqual(pool.slot.call_count,2)
        self.assertEqual(self.state('at-cap'),'queued')
        self.assertEqual(self.state('other'),'running')

    def test_skipped_lane_does_not_capture_the_only_credit_probe(self):
        self.running('one');self.running('two')
        self.queue('at-cap');self.queue('other','research');self.queue('other-second','research')
        self.ledger.tasks.db.execute('INSERT INTO pipeline_state VALUES(?,?)',
            ('credit_block',json.dumps({'since':0,'last_probe_at':None,'probe_task':None})))
        self.tick()
        self.assertEqual(self.state('at-cap'),'queued')
        self.assertEqual(self.state('other'),'running')

    def test_interleave_preserves_internal_continuation_then_arrival_order(self):
        rows=[]
        for name,kind,parent in [('s1','shortnote',False),('s2','shortnote',False),
                                 ('rev','shortnote',True),('r1','research',False)]:
            rows.append({'id':name,'kind':kind,'state':'queued',
                         'request':{'inputs':{'automation_parent':parent}}})
        ordered=control.dispatch_order(rows,self.cfg)
        self.assertEqual([r['id'] for r in ordered if r['kind']=='shortnote'],['rev','s1','s2'])

    def test_long_queue_does_not_fill_the_tick_before_other_lane(self):
        for i in range(8):self.queue('outreach-'+str(i))
        self.queue('correspondence','research')
        self.tick()
        created=[body['name'] for body in self.cma.creates.values()]
        self.assertEqual(created[:2],['outreach-outreach-0','outreach-correspondence'])
        self.assertEqual(len(created),4)  # Borrow after the only correspondence task starts.

    def test_status_and_work_cli_expose_lane_counts_and_caps(self):
        (self.folder / 'tables').mkdir()
        (self.folder / 'tables/contacts.json').write_text('{"contacts": []}')
        (self.folder / 'tables/refusals.json').write_text('{"refusals": []}')
        self.running('active');self.queue('queued','research')
        self.ledger.save(force=True)
        for action in ('status','work'):
            with self.subTest(action=action), patch.object(control,'Releases',return_value=self.remote), \
                    patch.object(control, 'ROOT', self.folder), \
                    patch('managed.daily_local.status',return_value={}), redirect_stdout(io.StringIO()) as output:
                control.main([action])
            counts=json.loads(output.getvalue())['lanes']
            quota=lanes.quotas(CONFIG)
            self.assertEqual(counts['shortnote'],{'active':1,'queued':0,'quota':quota['shortnote'],'borrowing':0})
            self.assertEqual(counts['research'],{'active':0,'queued':1,'quota':quota['research'],'borrowing':0})

    def test_config_validation(self):
        lanes.validate(CONFIG)
        for value in (None,True,{},'weighted'):
            config=copy.deepcopy(CONFIG);config['kind_quota']=value
            with self.assertRaises(ValueError):lanes.validate(config)

    def test_maintenance_checks_lane_config(self):
        with patch('managed.lanes.validate',side_effect=ValueError('Invalid lane configuration')):
            with self.assertRaisesRegex(ValueError,'Invalid lane configuration'):maintenance.check_config(CONFIG)


if __name__=='__main__':unittest.main()


def setUpModule():
    use_cli_config()
