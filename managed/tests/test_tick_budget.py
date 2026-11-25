"""Offline fake-clock ticks, including restoration of deferred work."""
from contextlib import ExitStack, redirect_stderr
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from fixture_config import use_cli_config
from managed import control, delivery, mathesis_delivery
from managed.queue import canonical, digest
from managed.tick import Tick, units
from test_tasks import CMA, Ledger, Releases, request


class Clock:
    def __init__(self):self.now=100
    def __call__(self):return self.now
    def advance(self,seconds):self.now+=seconds


class Remote(Releases):
    repo='fixture/repo'
    upload_retained=control.Releases.upload_retained
    def __init__(self):super().__init__();self.files={};self.downloaded=[]
    def ensure(self):pass
    def prune_snapshots(self,*args):pass
    def prune_logs(self,*args,**kwargs):pass
    def evict_oldest_log(self,*args):return 0
    def prune_ingested(self,receipts=None):pass
    def download(self,name,folder):
        self.downloaded.append(name)
        return super().download(name,folder)
    def preserve_task(self,path,tag):self.files[tag,path.name]=path.read_bytes()
    def run(self,*args):
        folder=Path(args[args.index('--dir')+1]);name=args[args.index('--pattern')+1]
        (folder/name).write_bytes(self.files[args[2],name])


class Worker(CMA):
    def __init__(self,clock):
        super().__init__();self.clock=clock;self.reads=[];self.cost=0
        self.completed=False;self.artifacts=False;self.artifact_reads=[]
    def response(self,response_id):
        name=response_id.removeprefix('resp_');self.reads.append(name);self.clock.advance(self.cost)
        if not self.completed:return {'status':'in_progress'}
        artifacts=[]
        if self.artifacts:
            raw=b'PRIVATE ATTACHMENT'
            artifacts=[{'name':'proof.txt','agent_path':'deliverables/proof.txt','role':'evidence',
                        'release_tag':'outreach-task-'+name,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}]
        result={'schema_version':1,'task_id':name,'outcome':'completed','report':'PRIVATE REPORT',
                'packet':None,'artifacts':artifacts}
        return {'status':'completed','output':[{'type':'message','content':[{'type':'output_text','text':canonical(result)}]}]}
    def artifact(self,*args):
        self.artifact_reads.append(args[0]);self.clock.advance(22)
        return b'PRIVATE ATTACHMENT'


class TickBudgetTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.folder=Path(self.temp.name)
        self.clock=Clock();self.remote=Remote();self.cma=Worker(self.clock)
        (self.folder / 'tables').mkdir()
        (self.folder / 'tables/contacts.json').write_text('{"contacts": []}')
        (self.folder / 'tables/refusals.json').write_text('{"refusals": []}')

    def tearDown(self):self.temp.cleanup()

    def seed(self,names,state='running'):
        with tempfile.TemporaryDirectory() as folder:
            ledger=Ledger(self.remote,folder)
            try:
                for name in names:
                    row=ledger.tasks.add(request(name,name))
                    if state!='queued':
                        ledger.tasks.update(name,row['revision'],state,{'agent_id':'agt_'+name,'response_id':'resp_'+name})
                ledger.save(force=True)
            finally:ledger.tasks.close()

    def rows(self):
        with tempfile.TemporaryDirectory() as folder:
            ledger=Ledger(self.remote,folder)
            try:return {r['id']:r for r in ledger.tasks.all()}
            finally:ledger.tasks.close()

    def run_tick(self,seconds=600,mail=None,mailbox=None):
        log=io.StringIO();forced=[];save=Ledger.save
        def record_save(ledger,force=False):
            forced.append(force)
            return save(ledger,force)
        output=self.folder/'status.json'
        with ExitStack() as stack:
            stack.enter_context(redirect_stderr(log))
            stack.enter_context(patch.dict('os.environ',{'GITHUB_ACTIONS':'true','OUTREACH_TICK_BUDGET_SECONDS':str(seconds)}))
            stack.enter_context(patch.object(Ledger,'save',record_save))
            for name,kwargs in {
                'managed.control.ROOT':{'new':self.folder},
                'managed.control.Releases':{'return_value':self.remote},
                'managed.control.CMA':{'return_value':self.cma},
                'managed.control.Tick':{'side_effect':lambda:Tick(clock=self.clock)},
                'managed.mailbox.configured':{'return_value':mailbox},
                'managed.pipeline.reconcile':{},'managed.pipeline.status':{'return_value':{}},
            }.items():stack.enter_context(patch(name,**kwargs))
            if mail:stack.enter_context(patch('managed.delivery.advance',side_effect=mail))
            self.assertIsNone(control.main(['--output',str(output),'tick']))
        self.assertTrue(forced[-1], 'The final save must be forced')
        return log.getvalue(),json.loads(output.read_text())

    def approve(self,ledger,name,kind='reply',route='auto'):
        row=ledger.tasks.add(request(name,name,kind))
        result={'outcome':'completed','report':'Checked result','routing':{'mail':route}}
        if kind == 'shortnote':
            result.update(formalization_gaps=[], value_assessment={
                'verdict': 'substantial', 'reasons': 'Resolves the general case.'})
        row=ledger.tasks.update(name,row['revision'],'awaiting-review',{
            'agent_id':'agt_'+name,'response_id':'resp_'+name,'result':result})
        envelope={'binding':ledger.tasks.review_binding(row),'reviewer':'local-claude-loop',
                  'verdict':'PASS','reason':'Checked exact result','evidence_ref':'private:fixture'}
        envelope['signature']=hmac.new(b'fixture-key',canonical(envelope).encode(),hashlib.sha256).hexdigest()
        return ledger.tasks.verdict(envelope,'fixture-key')

    def test_approved_rows_route_before_expensive_work_even_with_exhausted_budget(self):
        for exhausted in (False,True):
            with self.subTest(exhausted=exhausted),tempfile.TemporaryDirectory() as folder:
                ledger=Ledger(Remote(),folder);self.addCleanup(ledger.tasks.close)
                tick=Tick(5,self.clock);tick.bind(ledger)
                worker=Worker(self.clock);worker.cost=6
                for i in range(40):
                    row=ledger.tasks.add(request('live-'+str(i),'live-'+str(i)))
                    ledger.tasks.update(row['id'],row['revision'],'running',{
                        'agent_id':'agt_'+row['id'],'response_id':'resp_'+row['id']})
                approved={}
                for i in range(20):
                    kind,route,expected=[('reply','auto','awaiting-mailbox'),
                        ('reply','owner','owner-draft'),('firstcontact','none','no-reply'),
                        ('shortnote','none','handoff-ready'),('mathesis','none','handoff-ready')][i%5]
                    name='approved-'+str(i)
                    approved[name]=(self.approve(ledger,name,kind,route),expected)
                if exhausted:self.clock.advance(5)
                config={'paused':True,'kinds':{},'max_active':24,'agent_template':{}}
                control.advance(ledger,worker,config,folder)
                for name,(before,expected) in approved.items():
                    after=ledger.tasks.get(name)
                    self.assertEqual(after['state'],expected,name)
                    self.assertEqual(after['data']['result'],before['data']['result'])
                    self.assertEqual(after['data']['review'],before['data']['review'])
                self.assertGreaterEqual(self.clock(),tick.deadline)
                self.assertEqual(worker.reads,[] if exhausted else ['live-0'])
                self.assertEqual(worker.retired,[])

    def test_approved_gates_and_unexpected_exception_handling_survive_exhaustion(self):
        gates=['managed.pipeline.source_gate','managed.automation.parent_gate',
               'managed.automation.paper_gate','managed.automation.owner_gate',
               'managed.thread_staging.gate','managed.control.firstcontact_gate']
        for failure in [*gates,'unexpected']:
            with self.subTest(failure=failure),tempfile.TemporaryDirectory() as folder,ExitStack() as stack:
                ledger=Ledger(Remote(),folder);stack.callback(ledger.tasks.close)
                self.approve(ledger,'approved','firstcontact')
                tick=Tick(0,self.clock);tick.bind(ledger)
                for gate in gates:
                    stack.enter_context(patch(gate,side_effect=ValueError('Policy changed') if gate==failure else None))
                if failure=='unexpected':
                    stack.enter_context(patch('managed.automation.approved_route',side_effect=RuntimeError('Routing failed')))
                control.advance(ledger,Worker(self.clock),{'paused':True,'kinds':{}},folder)
                row=ledger.tasks.get('approved')
                if failure=='unexpected':
                    self.assertEqual(row['state'],'approved')
                    self.assertEqual(row['data']['last_error'],'RuntimeError: Routing failed')
                else:
                    self.assertEqual(row['state'],'held')
                    self.assertEqual(row['data']['reason'],'Policy changed')

    def test_sent_events_keeps_one_page_when_advance_and_delivery_use_their_slices(self):
        self.seed([])
        box=Mock(settings={'reconciled':True,'live_since':'2026-11-01T00:00:00Z'})
        box.verify_owner.return_value='owner@example.invalid'
        def page(*args):
            self.clock.advance(30)
            return [],'next-page'
        box.sent_events_page.side_effect=page
        def drain(ledger,*args,**kwargs):
            self.clock.advance(max(0,ledger.tick.limit()-self.clock()))
        with patch('managed.control.advance',side_effect=drain), \
                patch('managed.mailbox.intake'), \
                patch('managed.mathesis_delivery.advance',side_effect=drain), \
                patch('managed.sent_events.record_page',return_value=0), \
                patch('managed.sent_events.registry.render'), \
                patch('managed.sent_events.tables_need_push',return_value=True), \
                patch('managed.sent_events.commit_tables') as commit:
            for i in range(2):
                log,_=self.run_tick(600,mail=drain,mailbox=box)
                self.assertEqual(box.sent_events_page.call_count,i+1)
                self.assertIn('"pages": 1, "status": "deferred"',log)
                self.assertEqual(commit.call_count,i+1)
        self.assertIsNone(box.sent_events_page.call_args_list[0].args[2])
        self.assertEqual(box.sent_events_page.call_args_list[1].args[2],'next-page')

    def test_deferred_cleanup_gets_a_turn_before_live_polling_after_restore(self):
        self.seed(['live']);self.cma.cost=6
        with tempfile.TemporaryDirectory() as folder:
            ledger=Ledger(self.remote,folder)
            try:
                row=ledger.tasks.add(request('cleanup','cleanup'))
                ledger.tasks.update(row['id'],row['revision'],'awaiting-review',{
                    'agent_id':'agt_cleanup','result':{'report':'Checked'},
                    'cleanup':{'stopped':True,'status':'deleting'}})
                ledger.save(force=True)
            finally:ledger.tasks.close()
        self.run_tick(5)
        self.assertEqual(self.cma.reads,['live'])
        self.assertEqual(self.cma.retired,[])
        self.run_tick(5)
        self.assertIn('agt_cleanup',self.cma.retired)
        self.assertTrue(self.rows()['cleanup']['data']['cleanup']['deleted'])

    def test_deferred_historical_cleanup_joins_the_same_fair_queue(self):
        self.seed(['live']);self.cma.cost=6
        with tempfile.TemporaryDirectory() as folder:
            ledger=Ledger(self.remote,folder)
            try:
                row=ledger.tasks.add(request('finished','finished'))
                ledger.tasks.update(row['id'],row['revision'],'verified',{
                    'execution_history':[{'agent_id':'agt_previous'}]})
                ledger.save(force=True)
            finally:ledger.tasks.close()
        self.run_tick(5)
        self.assertEqual(self.cma.retired,[])
        self.run_tick(5)
        self.assertIn('agt_previous',self.cma.retired)
        self.assertTrue(self.rows()['finished']['data']['execution_history'][0]['cleanup']['deleted'])

    def test_main_logs_phases_and_only_slow_task_steps(self):
        self.seed(['slow','fast']);self.cma.cost=21
        original=self.cma.response
        def response(identity):
            self.cma.cost=21 if identity=='resp_slow' else 20
            return original(identity)
        self.cma.response=response
        log,_=self.run_tick()
        phases=[line.split()[1] for line in log.splitlines() if line.startswith('tick-phase ')]
        for name in ['restore','ingest','mailbox-intake','pipeline','advance','credit-recovery',
                     'pool-health','firstcontact-history','automation','delivery','mathesis','maintenance','final-save']:
            self.assertEqual(phases.count(name),1,name)
        self.assertIn('tick-slow slow result 21.000s',log)
        self.assertNotIn('tick-slow fast',log)

    def test_sent_events_follow_delivery_and_failure_does_not_stop_tick(self):
        self.seed([])
        calls=[]
        def fail(*args,**kwargs):
            calls.append('sent-events')
            raise RuntimeError('Synthetic mailbox failure')
        with patch('managed.sent_events.reconcile',side_effect=fail), \
                patch('managed.mathesis_delivery.advance',side_effect=lambda *args,again=False,**kwargs:
                      calls.append('mathesis-again' if again else 'mathesis')):
            log,status=self.run_tick(mail=lambda *args:calls.append('delivery'))
        self.assertEqual(calls,['delivery','sent-events','mathesis','mathesis-again'])
        self.assertIn('"status": "error"',log)
        self.assertIn('tick-phase final-save',log)
        self.assertIn({'connector':'sent-events','error':'RuntimeError'},status['connector_errors'])

    def test_sent_forced_save_failure_prevents_commit_and_later_tick_stages_run(self):
        self.seed([])
        calls=[];save=Ledger.save;failed=[]
        box=Mock(settings={'reconciled':True,'live_since':'2026-11-01T00:00:00Z'})
        box.verify_owner.return_value='owner@example.invalid'
        box.sent_events_page.return_value=([],None)
        def fail_intent(ledger,force=False):
            if force and not failed and ledger.tasks.db.execute(
                    "SELECT 1 FROM pipeline_state WHERE key='sent-events-pending'").fetchone():
                failed.append(True)
                raise TimeoutError('Synthetic checkpoint failure')
            return save(ledger,force)
        with patch.object(Ledger,'save',fail_intent), \
                patch('managed.mailbox.intake'), \
                patch('managed.sent_events.record_page',return_value=1), \
                patch('managed.sent_events.registry.render'), \
                patch('managed.sent_events.commit_tables') as commit, \
                patch('managed.mathesis_delivery.advance',side_effect=lambda *args,again=False,**kwargs:
                      calls.append('mathesis-again' if again else 'mathesis')):
            log,status=self.run_tick(mail=lambda *args:calls.append('delivery'),mailbox=box)
        self.assertEqual(failed,[True])
        commit.assert_not_called()
        self.assertEqual(calls,['delivery','mathesis','mathesis-again'])
        self.assertIn('tick-phase final-save',log)
        self.assertIn({'connector':'sent-events','error':'TimeoutError'},status['connector_errors'])

    def test_exhaustion_finishes_result_preservation_then_saves_and_succeeds(self):
        self.seed(['a','b']);self.cma.cost=2;self.cma.completed=True;self.cma.artifacts=True
        log,status=self.run_tick(5)
        self.assertEqual(self.cma.reads,['a'])
        self.assertEqual(self.cma.artifact_reads,['agt_a'])
        self.assertEqual(self.remote.files['outreach-task-a','proof.txt'],b'PRIVATE ATTACHMENT')
        rows=self.rows()
        self.assertEqual(rows['a']['state'],'awaiting-review')
        self.assertEqual(rows['a']['data']['result']['artifacts'][0]['name'],'proof.txt')
        self.assertEqual(rows['b']['state'],'running')
        self.assertFalse(self.cma.retired, 'Retirement is a separate unit after result preservation')
        self.assertIn('tick-budget exhausted after 24.000s; deferred ',log)
        self.assertIn('tick-phase final-save ',log)
        self.assertEqual(status['counts'],{'awaiting-review':1,'running':1})
        for private in ['PRIVATE ATTACHMENT','PRIVATE REPORT','deliverables/proof.txt']:
            self.assertNotIn(private,log)

    def test_result_window_defers_later_completions_to_next_tick(self):
        self.seed(['a','b','c']);self.cma.cost=2;self.cma.completed=True;self.cma.artifacts=True
        with patch.dict('os.environ',{'OUTREACH_RESULT_BUDGET_SECONDS':'10'}):
            self.run_tick(600)
        rows=self.rows()
        self.assertEqual(rows['a']['state'],'awaiting-review')
        self.assertEqual(rows['b']['state'],'running')
        self.assertEqual(rows['c']['state'],'running')
        self.assertEqual(self.cma.artifact_reads,['agt_a'])

    def test_deferred_tasks_precede_previously_polled_tasks_after_restore(self):
        self.seed(['a','b','c']);self.cma.cost=6
        for expected in ['a','b','c']:
            self.run_tick(5)
            self.assertEqual(self.cma.reads[-1],expected)
        self.assertEqual(self.cma.reads,['a','b','c'])
        self.assertTrue(all(r['state']=='running' for r in self.rows().values()))

    def test_later_phases_get_priority_over_another_result_batch(self):
        self.seed(['a','b']);self.cma.cost=6;delivered=[]
        def mail(ledger,*args):
            for row in units(ledger,'delivery',[ledger.tasks.get('b')]):
                delivered.append(row['id'])
        self.run_tick(5,mail)
        self.assertEqual(delivered,[])
        log,_=self.run_tick(5,mail)
        self.assertEqual(delivered,['b'])
        self.assertLess(log.index('tick-phase delivery '),log.index('tick-phase advance '))

    def test_create_finishes_but_admission_waits_for_next_tick(self):
        self.seed(['a'],state='queued');original=self.cma.create
        def create(*args):
            self.clock.advance(6)
            return original(*args)
        self.cma.create=create
        with patch('managed.sandbox_pool.SandboxPool',return_value=None):self.run_tick(5)
        row=self.rows()['a']
        self.assertEqual(len(self.cma.creates),1)
        self.assertEqual(row['state'],'admitting')
        self.assertEqual(row['data']['agent_id'],'agt_test')
        self.assertFalse(self.cma.commands)
        self.assertFalse(self.cma.reads)

    def test_ingest_drains_deferred_assets_without_redownloading_accepted_bytes(self):
        for name in ['a','b','c']:
            self.remote.assets['request-'+name+'.json']=canonical(request(name,name)).encode()
        download=self.remote.download
        def slow_download(name,folder):
            if name.startswith('request-'):self.clock.advance(6)
            return download(name,folder)
        self.remote.download=slow_download
        for count in [1,2,3]:
            self.run_tick(5)
            self.assertEqual(len(self.rows()),count)
        self.assertEqual([n for n in self.remote.downloaded if n.startswith('request-')],
                         ['request-a.json','request-b.json','request-c.json'])

    def test_mailbox_intake_retains_work_after_its_slice_when_ingest_exhausts_setup(self):
        self.remote.assets['request-bad.json']=b'{invalid json'
        download=self.remote.download
        def slow_download(name,folder):
            if name=='request-bad.json':self.clock.advance(85)
            return download(name,folder)
        self.remote.download=slow_download
        box=Mock(settings={'reconciled':True,'live_since':'2020-01-01T00:00:00Z'})
        box.verify_owner.return_value='owner@example.invalid'
        received = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        box.messages.return_value=[{'id':name,'conversationId':name,'internetMessageId':f'<{name}@example.invalid>',
                                    'receivedDateTime':received} for name in ('first','second')]
        def slow_thread(conversation):
            self.clock.advance(60)
            return []
        box.research_thread.side_effect=slow_thread
        with patch.dict('os.environ',{'OUTREACH_INTAKE_RESERVE_SECONDS':'60'}), \
                patch('managed.control.refresh_oracle_capacity') as oracle, \
                patch('managed.sent_events.advance',return_value={}):
            log,status=self.run_tick(mailbox=box)
        oracle.assert_not_called()
        box.research_thread.assert_called_once_with('first')
        self.assertIn('tick-phase mailbox-intake 60.000s units=2',log)
        self.assertEqual(status['connector_errors'],[])
        with tempfile.TemporaryDirectory() as folder:
            ledger=Ledger(self.remote,folder)
            try:
                mailbox=json.loads(ledger.tasks.db.execute(
                    "SELECT payload FROM pipeline_state WHERE key='mailbox'").fetchone()[0])
                self.assertEqual([(m['id'],m['intake_reason']) for m in mailbox['unmapped']],
                                 [('first','Inbox message missing from conversation readback; retained'),
                                  ('second','tick budget; retained')])
            finally:ledger.tasks.close()

    def test_empty_inbox_releases_intake_reserve_before_advance(self):
        box=Mock(settings={'reconciled':True,'live_since':'2020-01-01T00:00:00Z'})
        box.verify_owner.return_value='owner@example.invalid'
        box.messages.return_value=[]
        advance_limits=[]
        def observe_advance(ledger,*args,**kwargs):
            tick=ledger.tick
            self.assertIn('mailbox-intake',tick.finished)
            held=sum(tick.reserves.get(stage,0) for stage in ('delivery','mathesis','sent-events','automation'))
            advance_limits.append((tick.limit(),tick.deadline-held))
        with patch('managed.control.advance',side_effect=observe_advance), \
                patch('managed.control.refresh_oracle_capacity'), \
                patch('managed.sent_events.advance',return_value={}):
            _,status=self.run_tick(mailbox=box)
        box.messages.assert_called_once()
        self.assertEqual(status['connector_errors'],[])
        self.assertEqual(len(advance_limits),1)
        self.assertEqual(*advance_limits[0])

    def test_expired_budget_never_starts_mail_or_mathesis_operations(self):
        with tempfile.TemporaryDirectory() as folder:
            ledger=Ledger(self.remote,folder);tick=Tick(0,self.clock);tick.bind(ledger)
            try:
                row=ledger.tasks.add(request('mail','mail','reply'))
                ledger.tasks.update(row['id'],row['revision'],'awaiting-mailbox',{'result':{'packet':{'body':'PRIVATE'}}})
                row=ledger.tasks.add(request('math','math','mathesis'))
                ledger.tasks.update(row['id'],row['revision'],'handoff-ready',
                                    {'mathesis_delivery':{'phase':'ready','envelope':{}}})
                config={'paused':False,'kinds':{'reply':True,'mathesis':True},'mathesis':{'mode':'deliver'}}
                from unittest.mock import Mock
                box=Mock();client=Mock()
                delivery.advance(ledger,config,control.ROOT,box)
                mathesis_delivery.advance(ledger,config,control.ROOT,folder,client=client)
                self.assertFalse(box.mock_calls);self.assertFalse(client.mock_calls)
                self.assertIn('mail',tick.pending['delivery'])
                self.assertIn('math',tick.pending['mathesis'])
            finally:ledger.tasks.close()

    def test_mail_preflight_can_finish_without_starting_a_write_after_deadline(self):
        from unittest.mock import Mock
        with tempfile.TemporaryDirectory() as folder:
            ledger=Ledger(self.remote,folder);tick=Tick(5,self.clock);tick.bind(ledger)
            try:
                packet=json.loads((Path(__file__).parent/'fixtures/letter.json').read_text())
                row=ledger.tasks.add(request('mail','mail','reply'))
                result={'packet':packet}
                ledger.tasks.update(row['id'],row['revision'],'awaiting-mailbox',{
                    'result':result,'review':{'verdict':'PASS','binding':{
                        'task_id':row['id'],'result_hash':digest(result),
                        'workflow_hash':row['request']['workflow_hash']}}})
                box=Mock()
                def preflight(*args):self.clock.advance(6);return {}
                box.preflight.side_effect=preflight
                delivery.advance(ledger,{'paused':False,'kinds':{'reply':True}},control.ROOT,box)
                self.assertEqual(box.preflight.call_count,1)
                self.assertFalse(box.submit.called);self.assertFalse(box.save_draft.called)
                self.assertIn('mail',tick.pending['delivery'])
                self.assertNotIn('draft_intent',ledger.tasks.get('mail')['data'])
            finally:ledger.tasks.close()

    def test_reserved_stage_keeps_its_slice_until_it_runs(self):
        clock=Clock();tick=Tick(100,clock);tick.stages=['delivery','advance']
        tick.reserve('advance',30)
        tick.current='delivery';clock.advance(69)
        self.assertTrue(tick.allow('delivery','a'))
        clock.advance(1)
        self.assertFalse(tick.allow('delivery','b'))
        self.assertEqual(tick.pending['delivery'],['b'])
        tick.finished.add('delivery');tick.current='advance'
        self.assertTrue(tick.allow('advance','c'))
        clock.advance(29)
        self.assertTrue(tick.allow('advance','d'))
        clock.advance(1)
        self.assertFalse(tick.allow('advance','e'))

    def test_two_reserves_keep_delivery_time_after_advance(self):
        clock=Clock();tick=Tick(100,clock);tick.stages=['advance','delivery']
        tick.reserve('advance',30);tick.reserve('delivery',15)
        tick.current='intake';clock.advance(55)
        self.assertFalse(tick.allow('intake','x'))
        tick.current='advance';self.assertTrue(tick.allow('advance','a'))
        clock.advance(30);self.assertFalse(tick.allow('advance','b'))
        tick.finished.add('advance');tick.current='delivery'
        self.assertTrue(tick.allow('delivery','send'))

    def test_publication_stage_has_its_own_reserve(self):
        with patch.dict('os.environ',{},clear=True):
            self.assertEqual(control.stage_reserves(600),{'advance':270,'delivery':90,'mathesis':90,
                                                          'sent-events':30,'mailbox-intake':60,'pipeline':30})
            self.assertEqual(control.stage_reserves(100)['pipeline'],5)
            self.assertEqual(control.stage_reserves(100)['mathesis'],15)
            self.assertEqual(control.stage_reserves(100)['sent-events'],5)
            self.assertEqual(control.stage_reserves(100)['mailbox-intake'],10)
        with patch.dict('os.environ',{'OUTREACH_MATHESIS_RESERVE_SECONDS':'40','OUTREACH_INTAKE_RESERVE_SECONDS':'25'}):
            self.assertEqual(control.stage_reserves(600)['mathesis'],40)
            self.assertEqual(control.stage_reserves(600)['mailbox-intake'],25)
            self.assertEqual(control.stage_reserves(100)['mailbox-intake'],10)

    def test_pipeline_slice_runs_after_setup_is_exhausted_without_shortening_other_stages(self):
        clock=Clock();tick=Tick(100,clock);tick.reserve('advance',45)
        tick.current='setup';shared=tick.limit()
        tick.reserve('pipeline',5,dedicated=True,hold=False)
        self.assertEqual(tick.limit(),shared)  # the slice takes no shared time
        clock.advance(200);self.assertFalse(tick.allow('setup','late'))
        tick.current='pipeline'
        self.assertTrue(tick.allow('pipeline','reconcile'))  # its own deadline starts now
        clock.advance(6);self.assertFalse(tick.allow('pipeline','more'))

    def test_reserve_released_after_reserved_stage_finishes(self):
        clock=Clock();tick=Tick(100,clock);tick.reserve('advance',30)
        tick.finished.add('advance');tick.current='delivery';clock.advance(90)
        self.assertTrue(tick.allow('delivery','late'))
        with self.assertRaises(ValueError):tick.reserve('advance',float('nan'))

    def test_budget_env_default_and_validation(self):
        with patch.dict('os.environ',{},clear=True):
            tick=Tick(clock=self.clock)
            self.assertEqual(tick.deadline-self.clock(),540)
        for value in ['-1','nan','inf','bad']:
            with self.subTest(value=value),patch.dict('os.environ',{'OUTREACH_TICK_BUDGET_SECONDS':value}):
                with self.assertRaises(ValueError):Tick(clock=self.clock)

    def test_last_unit_overrun_is_reported_even_without_deferred_work(self):
        with tempfile.TemporaryDirectory() as folder:
            ledger=Ledger(self.remote,folder);tick=Tick(5,self.clock);tick.bind(ledger)
            log=io.StringIO()
            try:
                with redirect_stderr(log):
                    with tick.unit('advance','only','result') as ready:
                        self.assertTrue(ready);self.clock.advance(6)
                    tick.finish();ledger.save(force=True)
                self.assertIn('tick-budget exhausted after 6.000s; deferred 0 tasks',log.getvalue())
            finally:ledger.tasks.close()

    def test_exhausted_final_save_can_prune_at_release_asset_limit(self):
        self.seed(['a']);self.cma.cost=6;self.cma.completed=True
        upload=self.remote.upload;limited=True;pruned=[]
        def upload_at_limit(path):
            nonlocal limited
            if limited and self.clock.now>=106:
                limited=False
                raise RuntimeError('file_count limited')
            return upload(path)
        def prune(*args):
            self.assertIsNone(getattr(self.remote,'tick',None), 'Required checkpoint pruning cannot be deferred')
            pruned.append(args)
        self.remote.upload=upload_at_limit;self.remote.prune_snapshots=prune
        self.run_tick(5)
        self.assertFalse(limited);self.assertEqual(len(pruned),3)
        self.assertEqual(self.rows()['a']['state'],'awaiting-review')


    def test_review_and_author_registration_failures_are_isolated_and_tick_continues(self):
        self.seed([])
        from managed.location import example_config
        config = example_config()
        config['independent_review']['enabled'] = True
        config['automation']['enabled'] = True
        config['paused'] = False
        from managed.independent_review import advance as actual_review
        for failure in ('missing-key', 'row-exception'):
            calls = []
            def register(*args):
                calls.append('register')
                raise RuntimeError('Synthetic author stage error')
            review_failure = actual_review if failure == 'missing-key' else KeyError('synthetic malformed row')
            with patch.dict('os.environ', {'OUTREACH_REVIEW_KEY':''}), \
                 patch('managed.control.load_config', return_value=config), \
                 patch('managed.independent_review.advance', side_effect=review_failure), \
                 patch('managed.author_contacts.advance', side_effect=register), \
                 patch('managed.prior_work_issues.advance', side_effect=lambda *args:calls.append('later')):
                _, status = self.run_tick()
            self.assertIn('register', calls)
            self.assertIn('later', calls)
            errors = {e['connector']:e for e in status['connector_errors']}
            self.assertEqual(errors['independent-review']['phase'], 'needs_attention')
            self.assertEqual(errors['author-registration']['phase'], 'needs_attention')

    def test_tick_observes_existing_review_when_activation_is_disabled(self):
        self.seed([])
        with tempfile.TemporaryDirectory() as folder:
            ledger = Ledger(self.remote, folder)
            try:
                row = ledger.tasks.add(request('review-only','owner'))
                ledger.tasks.update(row['id'], row['revision'], 'awaiting-review', {
                    'hosted_review':{'phase':'reviewing','create_key':'original','response_id':'response'}})
                ledger.save(force=True)
            finally: ledger.tasks.close()
        with patch('managed.independent_review.advance') as observe:
            self.run_tick()
        observe.assert_called_once()


if __name__=='__main__':unittest.main()


def setUpModule():
    use_cli_config()
