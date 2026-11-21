import copy
from datetime import datetime
import hashlib
import hmac
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from fixture_config import use_example_config
from managed.control import Ledger, advance
from managed.pipeline import reconcile, source_gate, status, note_coverage, ZONE
from managed.queue import canonical, digest
from test_tasks import Releases, CMA, use_fixture_workflow_revisions


class PipelineTests(unittest.TestCase):
    def setUp(self):
        use_fixture_workflow_revisions(self)
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        (self.root/'tables').mkdir()
        self.catalog={'schema_version':1,'source_commit':'a'*40,'results':[self.result('historical')]}
        self.contacts=[];self.refusals=[];self.write_tables()
        self.remote=Releases();self.folder=self.root/'ledger';self.folder.mkdir()
        self.ledger=Ledger(self.remote,self.folder)
        self.config={'paused':True,'kinds':{'shortnote':True,'firstcontact':True,'mathesis':True,'daily':True},
                     'max_active':4,'agent_template':{},
                     'pipeline':{'enabled':True,'max_new_tasks_per_tick':4,'backfill_result_ids':[],'daily':False}}
        self.time=datetime(2026,10,31,12,tzinfo=ZONE)

    def tearDown(self):
        self.ledger.tasks.close();self.temp.cleanup()

    def result(self,name):
        return {'id':name,'note_status':'missing','upstream_status':'resolved-at-current-snapshot',
                'upstream':{'repository':'the-omega-institute/trureturing','commit':'a'*40,
                            'resolution_kind':'proved','declaration_gids':[name+'.result']}}

    def contact(self,name='author',sent=False):
        row={'id':'contact-'+name,'name':name,'email':name+'@sample.example.invalid','problem_ids':['new-result']}
        if sent:row['relationship']={'last_sent':'2026-10-01T00:00:00Z','last_sent_message_id':'actual-message'}
        return row

    def write_tables(self):
        for name,value in [('results',self.catalog),('contacts',{'contacts':self.contacts}),('refusals',{'refusals':self.refusals})]:
            (self.root/'tables'/(name+'.json')).write_text(json.dumps(value))

    def tick(self):
        reconcile(self.ledger,self.config,self.root,now=self.time,revision='b'*40)

    def new_note(self):
        self.tick();self.catalog['results'].append(self.result('new-result'));self.write_tables()
        self.config['paused']=False;self.tick()
        note=next(r for r in self.ledger.tasks.all() if r['kind']=='shortnote')
        # These older pipeline scenarios intentionally retain a roundless
        # request so they continue to cover legacy review compatibility. New
        # production pipeline requests use the explicit draft round.
        if note['request']['inputs'].get('note_round') == 'draft':
            request=copy.deepcopy(note['request']);request['inputs'].pop('note_round')
            self.ledger.tasks.db.execute('UPDATE tasks SET request=?,request_hash=? WHERE id=?',
                                         (canonical(request),digest(request),note['id']))
            note=self.ledger.tasks.get(note['id'])
        return note

    def review_note(self,note,verdict='PASS',authors=None,metadata=None,manifest='metadata.json'):
        artifact=lambda name,role:{'release_tag':'outreach-task-'+note['id'],'name':name,
                                    'bytes':10,'sha256':'a'*64,'role':role}
        result={'schema_version':1,'task_id':note['id'],'outcome':'completed','report':'Checked source and complete argument.',
                'formalization_gaps':[], 'value_assessment':{'verdict':'incremental','reasons':'Extends the previously checked range.'},
                'artifacts':[artifact('note.tex','note-tex'),artifact('note.pdf','note-pdf')],'packet':None,
                'author_contacts':authors or [], 'author_contacts_reason':'Routing fixture: no researched author evidence.'}
        if metadata is not None:
            payload = ({'source_note':metadata} if manifest == 'handoff.json' else
                       {'metadata':metadata} if manifest != 'metadata.json' else metadata)
            raw = canonical(payload).encode()
            result['artifacts'].append({**artifact(manifest,'evidence'),
                'bytes':len(raw), 'sha256':hashlib.sha256(raw).hexdigest()})
            self.remote.repo = 'fixture/repo'
            self.remote.run = lambda *args: (Path(args[args.index('--dir')+1])/manifest).write_bytes(raw)
        row=self.ledger.tasks.update(note['id'],note['revision'],'awaiting-review',{'result':result})
        if verdict is None:return row
        envelope={'binding':self.ledger.tasks.review_binding(row),'reviewer':'local-claude-loop',
                  'verdict':verdict,'reason':'Reviewed exact source/PDF and argument','evidence_ref':'private:review'}
        envelope['signature']=hmac.new(b'secret',canonical(envelope).encode(),hashlib.sha256).hexdigest()
        return self.ledger.tasks.verdict(envelope,'secret')

    def family_note(self):
        return self.review_note(self.new_note(),metadata={
            'id':'family-note', 'problem_ids':['new-result','sibling','other-sibling']})

    def sibling(self, state='queued', **data):
        self.catalog['results'].append(self.result('sibling'));self.write_tables();self.tick()
        row=next(r for r in self.ledger.tasks.all() if r['owner']=='result-sibling')
        return self.ledger.tasks.update(row['id'],row['revision'],state,data) if state!='queued' or data else row

    def test_covered_result_reuses_note_without_second_delivery_and_survives_restart(self):
        note=self.family_note()
        before=self.ledger.tasks.get(note['id'])
        self.catalog['results'].append(self.result('sibling'));self.write_tables();self.tick()
        self.ledger.save(force=True)  # The coordinator's end-of-tick durability barrier.
        self.ledger.tasks.close();self.folder=self.root/'restored';self.folder.mkdir()
        self.ledger=Ledger(self.remote,self.folder)
        self.tick()
        rows=self.ledger.tasks.all()
        self.assertEqual([r['id'] for r in rows if r['kind']=='shortnote'],[note['id']])
        self.assertEqual(sum(r['kind']=='mathesis' for r in rows),1)
        self.assertEqual(self.ledger.tasks.db.execute(
            "SELECT task_id FROM pipeline_sources WHERE result_id='sibling'").fetchone()[0],note['id'])
        self.assertEqual(self.ledger.tasks.get(note['id']),before)

    def test_queued_sibling_superseded_atomically_with_covering_pointer(self):
        original=self.new_note();sibling=self.sibling()
        note=self.review_note(original,metadata={'id':'family-note','problem_ids':['new-result','sibling']})
        self.tick();self.tick()
        stopped=self.ledger.tasks.get(sibling['id'])
        self.assertEqual(stopped['state'],'superseded')
        self.assertEqual(stopped['revision'],sibling['revision']+1)
        self.assertEqual(stopped['data']['covering_note_task_id'],note['id'])
        self.assertEqual(self.ledger.tasks.db.execute(
            "SELECT task_id FROM pipeline_sources WHERE result_id='sibling'").fetchone()[0],note['id'])

    def test_started_siblings_are_only_reported_in_work(self):
        from managed.automation import work
        original=self.new_note();sibling=self.sibling()
        note=self.review_note(original,metadata={'id':'family-note','problem_ids':['sibling']})
        for state in ['running','revising','awaiting-review','handoff-ready','verified','queued']:
            with self.subTest(state=state):
                current=self.ledger.tasks.get(sibling['id'])
                row=self.ledger.tasks.update(current['id'],current['revision'],state,{'agent_id':'agt-existing'})
                self.tick()
                self.assertEqual(self.ledger.tasks.get(row['id']),row)
                overlaps=work(self.ledger.tasks,self.root)['note_coverage_overlap']
                self.assertIn({'task_id':row['id'],'state':state,'result_id':'sibling',
                               'covering_note_task_id':note['id']},overlaps)

    def test_own_note_uplift_is_not_superseded_or_reported(self):
        from managed.automation import work
        draft=self.new_note()
        note=self.review_note(draft,metadata={'id':'own-note','problem_ids':['new-result']})
        value=copy.deepcopy(draft['request']);value['id']='own-uplift'
        value['inputs'].update(note_round='uplift',automation_parent={
            'task_id':note['id'],'purpose':'shortnote-uplift','result_hash':digest(note['data']['result'])})
        uplift=self.ledger.tasks.add(value)
        self.tick();self.tick()
        self.assertEqual(self.ledger.tasks.get(uplift['id']),uplift)
        self.assertEqual(work(self.ledger.tasks,self.root)['note_coverage_overlap'],[])

    def test_covering_note_ancestor_is_not_reported(self):
        from managed.automation import work
        draft=self.new_note()
        value=copy.deepcopy(draft['request']);value['id']='child-revision'
        value['inputs'].update(note_round='uplift',automation_parent={
            'task_id':draft['id'],'purpose':'revision','result_hash':'0'*64})
        child=self.ledger.tasks.add(value)
        note=self.review_note(child,metadata={'id':'own-note','problem_ids':['new-result']})
        current=self.ledger.tasks.get(draft['id'])
        self.ledger.tasks.update(current['id'],current['revision'],'revising',{'agent_id':'agt-old'})
        self.tick()
        self.assertIn('new-result',__import__('managed.pipeline',fromlist=['x']).note_coverage(
            self.ledger.tasks.all(),self.ledger.tasks))
        self.assertEqual(work(self.ledger.tasks,self.root)['note_coverage_overlap'],[])

    def test_non_pass_non_completed_or_stale_metadata_does_not_cover(self):
        note=self.family_note();self.tick()
        self.config['kinds']['mathesis']=False
        current=self.ledger.tasks.get(note['id'])
        for verdict,outcome,changed in [('HOLD','completed',False),('BOUNCE','completed',False),
                                        ('PASS','hold',False),('PASS','needs-decision',False),
                                        ('PASS','completed',True)]:
            with self.subTest(verdict=verdict,outcome=outcome,changed=changed):
                data=copy.deepcopy(current['data']);data['review']['verdict']=verdict
                data['result']['outcome']=outcome
                if changed:data['result']['report']='Changed after PASS'
                row=self.ledger.tasks.update(note['id'],self.ledger.tasks.get(note['id'])['revision'],'approved',data)
                self.assertEqual(note_coverage([row],self.ledger.tasks),{})
        self.catalog['results'].append(self.result('sibling'));self.write_tables();self.tick()
        self.assertTrue(any(r['owner']=='result-sibling' for r in self.ledger.tasks.all()))

    def test_reviewed_manifest_formats(self):
        for manifest in ['metadata.json','handoff.json','note-v2.json']:
            with self.subTest(manifest=manifest):
                note=self.review_note(self.new_note(),metadata={'id':'family-note','problem_ids':['sibling']},manifest=manifest)
                self.tick()
                self.assertEqual(note_coverage(self.ledger.tasks.all(),self.ledger.tasks)['sibling'][0]['id'],note['id'])
                # Reset the candidate's cached coverage to exercise each format.
                self.ledger.tasks.db.execute("DELETE FROM pipeline_state WHERE key LIKE 'note-coverage:%'")

    def test_existing_publication_metadata_is_reused_without_download(self):
        note=self.review_note(self.new_note())
        row=self.ledger.tasks.update(note['id'],note['revision'],'handoff-ready',{
            **note['data'], 'publication_loop':{'result_hash':digest(note['data']['result']),
                'note_id':'family-note','problem_ids':['new-result','sibling']}})
        self.catalog['results'].append(self.result('sibling'));self.write_tables();self.tick()
        self.assertEqual(self.ledger.tasks.get(row['id']),row)
        self.assertFalse(any(r['owner']=='result-sibling' for r in self.ledger.tasks.all()))

    def test_coverage_metadata_reads_obey_cap_and_tick_deadline(self):
        from managed.tick import Tick
        note=self.family_note()
        value=copy.deepcopy(note['request']);value['id']='second-family-note'
        self.review_note(self.ledger.tasks.add(value),metadata={
            'id':'family-note','problem_ids':['new-result','sibling','other-sibling']})
        self.catalog['results'].append(self.result('uncovered'));self.write_tables()
        self.config['pipeline']['max_new_tasks_per_tick']=1
        tick=Tick(seconds=0);tick.bind(self.ledger);self.tick()
        self.assertEqual(note_coverage(self.ledger.tasks.all(),self.ledger.tasks),{})
        del self.ledger.tick
        self.tick()
        self.assertEqual(len(note_coverage(self.ledger.tasks.all(),self.ledger.tasks)['sibling']),1)
        self.assertFalse(any(r['owner']=='result-uncovered' for r in self.ledger.tasks.all()))
        self.tick()
        self.assertEqual(len(note_coverage(self.ledger.tasks.all(),self.ledger.tasks)['sibling']),2)
        self.assertTrue(any(r['owner']=='result-uncovered' for r in self.ledger.tasks.all()))

    def test_metadata_hash_mismatch_does_not_establish_coverage(self):
        self.family_note();self.remote.run=lambda *args: (Path(args[args.index('--dir')+1])/'metadata.json').write_text('{}')
        self.catalog['results'].append(self.result('sibling'));self.write_tables();self.tick()
        self.assertTrue(any(r['owner']=='result-sibling' for r in self.ledger.tasks.all()))

    def test_first_snapshot_is_baseline_and_paused_changes_survive_restart(self):
        self.tick();self.assertEqual(self.ledger.tasks.all(),[])
        self.catalog['results'].append(self.result('new-result'));self.write_tables();self.tick()
        self.assertEqual(status(self.ledger.tasks,self.root)['new_results_waiting'],1)
        self.assertEqual(self.ledger.tasks.all(),[])
        self.ledger.tasks.close()
        # Ledger download names are immutable assets, so restore to a fresh directory.
        self.folder=self.root/'restored';self.folder.mkdir();self.ledger=Ledger(self.remote,self.folder)
        self.config['paused']=False;self.tick();self.tick()
        self.assertEqual(len(self.ledger.tasks.all()),1)
        self.assertEqual(self.ledger.tasks.all()[0]['request']['inputs']['pipeline']['result_id'],'new-result')

    def test_upstream_tip_and_workflow_updates_do_not_repeat_a_result(self):
        note=self.new_note()
        self.catalog['source_commit']='c'*40
        for row in self.catalog['results']:row['upstream']['commit']='c'*40
        self.write_tables()
        reconcile(self.ledger,self.config,self.root,self.time,revision='d'*40)
        self.assertEqual([r['id'] for r in self.ledger.tasks.all()],[note['id']])
        self.assertEqual(self.ledger.tasks.get(note['id'])['request']['inputs']['result']['upstream']['commit'],'a'*40)

    def test_repin_relinks_source_and_approved_successor_derives_downstream(self):
        self.contacts=[self.contact()];self.write_tables();note=self.new_note()
        for current in ['b'*64,'c'*64]:
            with patch('managed.control.workflow',return_value=current):
                advance(self.ledger,CMA(),self.config,self.folder,revision='d'*40)
            old=self.ledger.tasks.get(note['id'])
            self.assertEqual(old['state'],'superseded')
            note=self.ledger.tasks.get(old['data']['successor'])
            link=self.ledger.tasks.db.execute('SELECT task_id FROM pipeline_sources WHERE result_id=?',
                                               ('new-result',)).fetchone()[0]
            self.assertEqual(link,note['id'])
            self.assertEqual(status(self.ledger.tasks,self.root)['source_notes'],1)
        note=self.review_note(note);self.tick();self.tick()
        children=[r for r in self.ledger.tasks.all() if r['kind']!='shortnote']
        self.assertEqual(sorted(r['kind'] for r in children),['firstcontact','mathesis'])
        for child in children:
            self.assertEqual(child['request']['inputs']['pipeline']['note_task_id'],note['id'])
            source_gate(child['request'],self.ledger.tasks,self.root)

    def test_disabled_shortnote_retains_observations_across_restart_for_later_derivation(self):
        self.tick();self.config['paused']=False;self.config['kinds']['shortnote']=False
        self.catalog['results'].append(self.result('new-result'));self.write_tables();self.tick()
        self.catalog['results'][-1]['upstream']['resolution_kind']='refuted';self.write_tables();self.tick()
        self.assertEqual(self.ledger.tasks.all(),[])
        self.assertEqual(status(self.ledger.tasks,self.root)['new_results_waiting'],1)
        self.ledger.tasks.close();self.folder=self.root/'restored';self.folder.mkdir()
        self.ledger=Ledger(self.remote,self.folder)
        self.config['kinds']['shortnote']=True;self.tick();self.tick()
        rows=self.ledger.tasks.all();self.assertEqual(len(rows),1)
        self.assertEqual(rows[0]['kind'],'shortnote')
        self.assertEqual(rows[0]['request']['inputs']['result']['upstream']['resolution_kind'],'refuted')

    def test_downstream_kind_flags_are_independent_and_resume_once(self):
        self.contacts=[self.contact()];self.write_tables();self.review_note(self.new_note())
        self.config['pipeline']['max_new_tasks_per_tick']=1
        self.config['kinds']['firstcontact']=False;self.config['kinds']['mathesis']=False
        self.tick();self.assertEqual([r['kind'] for r in self.ledger.tasks.all()],['shortnote'])
        self.config['kinds']['firstcontact']=True;self.tick();self.tick()
        self.assertEqual(sorted(r['kind'] for r in self.ledger.tasks.all()),['firstcontact','shortnote'])
        self.config['kinds']['firstcontact']=False;self.config['kinds']['mathesis']=True
        self.tick();self.tick()
        self.assertEqual(sorted(r['kind'] for r in self.ledger.tasks.all()),['firstcontact','mathesis','shortnote'])
        self.config['kinds']['firstcontact']=True;self.tick()
        self.assertEqual(len(self.ledger.tasks.all()),3)

    def test_changed_resolution_creates_new_source_and_blocks_old_note(self):
        note=self.new_note();self.catalog['results'][-1]['upstream']['resolution_kind']='refuted';self.write_tables();self.tick()
        self.assertEqual(len(self.ledger.tasks.all()),2)
        with self.assertRaisesRegex(ValueError,'identity changed'):source_gate(note['request'],self.ledger.tasks,self.root)

    def test_approved_note_derives_mail_and_mathesis_once_with_exact_parent(self):
        self.contacts=[self.contact()];self.write_tables()
        note=self.review_note(self.new_note());self.tick();self.tick()
        rows=self.ledger.tasks.all()
        self.assertEqual(sorted(r['kind'] for r in rows),['firstcontact','mathesis','shortnote'])
        for child in rows:
            if child['kind']=='shortnote':continue
            self.assertEqual(child['request']['inputs']['pipeline']['note_task_id'],note['id'])
            source_gate(child['request'],self.ledger.tasks,self.root)
        self.assertEqual(next(r for r in rows if r['kind']=='firstcontact')['request']['inputs']['recipient_emails'],['author@sample.example.invalid'])
        mail=next(r for r in rows if r['kind']=='firstcontact')
        changed=copy.deepcopy(note['data']);changed['result']['report']='Revised result'
        self.ledger.tasks.update(note['id'],note['revision'],'approved',changed)
        with self.assertRaisesRegex(ValueError,'version changed'):source_gate(mail['request'],self.ledger.tasks,self.root)

    def test_pass_on_a_needs_decision_note_derives_nothing(self):
        self.contacts=[self.contact()];self.write_tables()
        note=self.new_note()
        row=self.review_note(note,None)
        data=copy.deepcopy(row['data']);data['result']['outcome']='needs-decision'
        row=self.ledger.tasks.update(row['id'],row['revision'],'awaiting-review',data)
        envelope={'binding':self.ledger.tasks.review_binding(row),'reviewer':'local-claude-loop',
                  'verdict':'PASS','reason':'Question is warranted','evidence_ref':'private:review'}
        envelope['signature']=hmac.new(b'secret',canonical(envelope).encode(),hashlib.sha256).hexdigest()
        self.ledger.tasks.verdict(envelope,'secret');self.tick();self.tick()
        self.assertEqual([r['kind'] for r in self.ledger.tasks.all()],['shortnote'])

    def test_unreviewed_or_bounced_note_produces_no_downstream_task(self):
        note=self.review_note(self.new_note(),None);self.tick()
        self.assertEqual(len(self.ledger.tasks.all()),1)
        self.ledger.tasks.update(note['id'],note['revision'],'bounced',note['data']);self.tick()
        self.assertEqual(len(self.ledger.tasks.all()),1)

    def test_refusal_and_prior_sent_block_cold_outreach_but_not_mathesis(self):
        self.contacts=[self.contact('refused'),self.contact('existing',sent=True)]
        self.refusals=[{'id':'boundary','active':True,'name':'refused','emails':['refused@sample.example.invalid']}]
        self.write_tables();self.review_note(self.new_note());self.tick()
        self.assertEqual(sorted(r['kind'] for r in self.ledger.tasks.all()),['mathesis','shortnote'])
        self.assertEqual(len(status(self.ledger.tasks,self.root)['outreach_waits']),2)

    def test_missing_contacts_are_visible_and_registration_resumes_outreach(self):
        self.review_note(self.new_note());self.tick()
        self.assertIn('No verified author',status(self.ledger.tasks,self.root)['outreach_waits'][0]['reason'])
        self.contacts=[self.contact()];self.write_tables();self.tick()
        self.assertEqual(sum(r['kind']=='firstcontact' for r in self.ledger.tasks.all()),1)

    def test_creation_budget_and_explicit_backfill(self):
        self.tick();self.config['paused']=False;self.config['pipeline']['max_new_tasks_per_tick']=1
        self.config['pipeline']['backfill_result_ids']=['historical']
        self.catalog['results'] += [self.result('second'),self.result('third')];self.write_tables()
        self.tick();self.assertEqual(len(self.ledger.tasks.all()),1)
        self.tick();self.assertEqual(len(self.ledger.tasks.all()),2)
        self.tick();self.assertEqual(len(self.ledger.tasks.all()),3)

    def test_daily_depends_on_upstream_calendar_window_without_mail_or_submission(self):
        self.tick();self.config['paused']=False;self.config['pipeline']['daily']=True
        self.time=datetime(2026,11,1,0,10,tzinfo=ZONE);self.tick()
        rows=self.ledger.tasks.all();self.assertEqual(len(rows),1)
        daily=rows[0];self.assertEqual(daily['kind'],'daily')
        self.assertEqual(daily['request']['inputs']['covered_days'],['2026-10-31'])
        self.assertEqual(daily['request']['inputs']['pipeline']['upstream_commit'],'a'*40)
        self.tick();self.assertEqual(len(self.ledger.tasks.all()),1)

    def test_source_removed_before_cma_admission_is_held_without_a_call(self):
        note=self.new_note();self.catalog['results'][-1]['upstream_status']='absent-at-current-snapshot';self.write_tables()
        cma=CMA()
        with patch('managed.control.ROOT',self.root), patch('managed.control.workflow',return_value=note['request']['workflow_hash']):
            advance(self.ledger,cma,self.config,self.folder)
        self.assertEqual(self.ledger.tasks.get(note['id'])['state'],'held')
        self.assertFalse(cma.creates)

    def test_malformed_provenance_fails_before_lookup_or_cma(self):
        note=self.new_note()
        for value in ['not-an-object',{}, {'scenario':2}, {'scenario':3}]:
            request=copy.deepcopy(note['request']);request['inputs']['pipeline']=value
            with self.assertRaises(ValueError):source_gate(request,self.ledger.tasks,self.root)

    def test_routed_note_runs_through_coordinator_and_waits_for_signed_review(self):
        self.contacts=[self.contact()];self.write_tables();note=self.new_note();cma=CMA()
        data={'note.tex':b'fixture source','note.pdf':b'%PDF-1.4 fixture'}
        result={'schema_version':1,'task_id':note['id'],'outcome':'completed','report':'Fixture for routing, not research.',
                'formalization_gaps':[], 'value_assessment':{'verdict':'substantial','reasons':'Proves the previously open general case.'},
                'author_contacts':[], 'author_contacts_reason':'Routing fixture: no researched author evidence.',
                'artifacts':[{'release_tag':'outreach-task-'+note['id'],'name':name,'bytes':len(raw),
                              'sha256':hashlib.sha256(raw).hexdigest(),'role':'note-pdf' if name.endswith('.pdf') else 'note-tex'}
                             for name,raw in data.items()],'packet':None}
        cma.response=lambda _: {'status':'completed','output':[{'type':'message','content':[{'type':'output_text','text':json.dumps(result)}]}]}
        def download(*args):
            name=args[args.index('--pattern')+1];folder=Path(args[args.index('--dir')+1]);(folder/name).write_bytes(data[name])
        self.remote.run=download;self.remote.repo='fixture/repo'
        with patch('managed.control.ROOT',self.root), patch('managed.control.workflow',return_value=note['request']['workflow_hash']):
            advance(self.ledger,cma,self.config,self.folder)
        row=self.ledger.tasks.get(note['id']);self.assertEqual(row['state'],'awaiting-review')
        self.assertEqual(len(cma.creates),1);self.assertEqual(len(cma.commands),1)
        self.assertEqual(cma.retired,['agt_test']);self.tick()
        self.assertEqual(len(self.ledger.tasks.all()),1)
        envelope={'binding':self.ledger.tasks.review_binding(row),'reviewer':'local-claude-loop','verdict':'PASS',
                  'reason':'Routing fixture reviewed','evidence_ref':'fixture:review'}
        envelope['signature']=hmac.new(b'secret',canonical(envelope).encode(),hashlib.sha256).hexdigest()
        self.ledger.tasks.verdict(envelope,'secret');self.tick()
        self.assertEqual(sorted(r['kind'] for r in self.ledger.tasks.all()),['firstcontact','mathesis','shortnote'])

    def test_reviewed_author_registration_then_next_tick_derives_one_firstcontact(self):
        from managed import author_contacts, automation, registry
        from test_author_contacts import proposal
        # Needed by the same production view renderer used by apply-author-contacts.
        registry.write(self.root,'positive',{'entries':[]})
        registry.write(self.root,'submissions',{'schema_version':1,'updated_on':'2026-11-01',
            'scope':'independent-team','verification':{'mailbox_check':'saved-evidence-only'},'papers':[]})
        self.contacts=[self.contact('existing',sent=True),
                       {'id':'contact-author','name':'Original Author','email':'author@example.invalid','problem_ids':[]}]
        self.refusals=[{'id':'boundary','active':True,'name':'Refused Author',
                        'emails':['refused@example.invalid'],'date':'2026-10-31',
                        'reason':'No further contact','quote':'Stop','source':'fixture'}]
        self.write_tables()
        note=self.new_note()
        authors=[proposal(),proposal('Refused Author','refused@example.invalid'),
                 proposal('existing','existing@sample.example.invalid')]
        # This alias must not create a second cold contact to the same scholar.
        authors[0]['emails'].append({'email':'alias@example.invalid','evidence_url':'https://example.invalid/author'})
        note=self.review_note(note,authors=authors)
        with patch('managed.daily_local.status',return_value={}):
            item=automation.work(self.ledger.tasks,self.root)['author_contacts_needed'][0]
        self.assertEqual(len(item['proposals']),1)  # The existing sent author is fully linked; the refused author is blocked.
        self.assertEqual(len(automation.work(self.ledger.tasks,self.root)['author_contacts_blocked']),1)
        outcome=author_contacts.apply(item['reviewed_proposal'],self.root,'secret')
        self.assertEqual(len(outcome['registered_or_linked']),2)
        self.assertEqual(len(outcome['linked_existing']),1)
        self.assertEqual(len(outcome['skipped']),1)
        linked=next(r for r in registry.read(self.root,'contacts')['contacts'] if r['id']=='contact-author')
        self.assertEqual(linked['problem_ids'],['new-result'])
        self.assertEqual(linked['problem_links'][0]['evidence'],authors[0]['source_url'])
        self.assertEqual(len(self.ledger.tasks.all()),1)  # Applying never enqueues.
        self.tick();self.tick()
        mail=[r for r in self.ledger.tasks.all() if r['kind']=='firstcontact']
        self.assertEqual(len(mail),1)
        self.assertEqual(mail[0]['request']['inputs']['recipient_emails'],['author@example.invalid'])
        self.assertEqual(mail[0]['request']['inputs']['pipeline']['note_task_id'],note['id'])
        source_gate(mail[0]['request'],self.ledger.tasks,self.root)

    def grouped_note(self):
        from test_author_contacts import proposal
        # Table order deliberately differs from the posing paper's author order.
        self.contacts = [self.contact('third'), self.contact('first'), self.contact('second')]
        self.contacts[1]['relationship'] = {'aliases': ['first-alias@sample.example.invalid']}
        self.write_tables()
        authors = [proposal(n, n+'@sample.example.invalid') for n in ('first', 'second', 'third')]
        authors[0]['emails'].append({'email':'first-alias@sample.example.invalid', 'evidence_url':'https://sample.example.invalid/first'})
        authors.insert(1, {**proposal('unreachable'), 'emails': [], 'email_omission_reason': 'No verified address'})
        return self.review_note(self.new_note(), authors=authors)

    def test_coauthors_get_one_letter_in_reviewed_author_order_without_alias_duplicates(self):
        note = self.grouped_note(); self.tick(); self.tick()
        mail = [r for r in self.ledger.tasks.all() if r['kind']=='firstcontact']
        self.assertEqual(len(mail), 1)
        inputs = mail[0]['request']['inputs']
        self.assertEqual(inputs['to'], ['first@sample.example.invalid'])
        self.assertEqual(inputs['cc'], ['second@sample.example.invalid', 'third@sample.example.invalid'])
        self.assertEqual(inputs['owner_ids'], ['contact-first', 'contact-second', 'contact-third'])
        self.assertEqual(inputs['recipient_emails'], inputs['to']+inputs['cc'])
        source_gate(mail[0]['request'], self.ledger.tasks, self.root)
        # Later registration/table reorder cannot create a second letter for the same note.
        self.contacts.reverse(); self.contacts.append(self.contact('later')); self.write_tables(); self.tick()
        self.assertEqual(len([r for r in self.ledger.tasks.all() if r['kind']=='firstcontact']), 1)

    def test_active_coauthor_thread_is_excluded_and_reported(self):
        self.grouped_note()
        (self.root/'tables/threads.json').write_text(json.dumps({'lines':[
            {'id':'active-first', 'stage':'exchange', 'correspondent':{'email':'first-alias@sample.example.invalid'}}]}))
        self.tick(); self.tick()
        mail = next(r for r in self.ledger.tasks.all() if r['kind']=='firstcontact')
        self.assertEqual(mail['request']['inputs']['to'], ['second@sample.example.invalid'])
        self.assertEqual(mail['request']['inputs']['cc'], ['third@sample.example.invalid'])
        excluded = mail['request']['inputs']['excluded_recipients']
        self.assertEqual(excluded[0]['email'], 'first@sample.example.invalid')
        self.assertEqual(excluded[0]['thread_ids'], ['active-first'])

    def legacy_outreach(self, note, name, state='queued'):
        from managed.pipeline import request
        link = {**note['request']['inputs']['pipeline'], 'scenario':2, 'note_task_id':note['id'],
                'note_result_hash':digest(note['data']['result']), 'contact_id':'contact-'+name}
        value = request('firstcontact',link,'contact-'+name,{'pipeline':link,
                        'recipient_emails':[name+'@sample.example.invalid']},'b'*40)
        row = self.ledger.tasks.add(value)
        if state!='queued':
            row = self.ledger.tasks.update(row['id'],row['revision'],state,{'reason':'Original agent hold',
                'result':{'outcome':'hold','report':'Keep this original review history'}})
        return row

    def test_group_supersedes_queued_per_contact_tasks_but_retains_held_history(self):
        note = self.grouped_note()
        old = self.legacy_outreach(note, 'first')
        held = self.legacy_outreach(note, 'second', 'held')
        self.tick(); self.tick()
        rows = [r for r in self.ledger.tasks.all() if r['kind']=='firstcontact']
        grouped = next(r for r in rows if r['request']['inputs']['pipeline'].get('recipient_group'))
        self.assertEqual(len(rows), 3)
        self.assertEqual(self.ledger.tasks.get(old['id'])['state'], 'superseded')
        self.assertEqual(self.ledger.tasks.get(old['id'])['data']['successor'], grouped['id'])
        self.assertEqual(self.ledger.tasks.get(held['id']), held)
        # A late legacy replay is superseded too, with no second group.
        late = self.legacy_outreach(note, 'third'); self.tick(); self.tick()
        self.assertEqual(self.ledger.tasks.get(late['id'])['state'], 'superseded')
        self.assertEqual(sum(r['kind']=='firstcontact' and r['state']=='queued' for r in self.ledger.tasks.all()), 1)

    def test_started_legacy_letter_prevents_duplicate_group_and_budget_preserves_queue(self):
        note = self.grouped_note()
        old = self.legacy_outreach(note, 'first')
        old = self.ledger.tasks.update(old['id'],old['revision'],'running',{'agent_id':'existing-agent'})
        self.tick(); self.tick()
        self.assertEqual(len([r for r in self.ledger.tasks.all() if r['kind']=='firstcontact']), 1)
        self.assertEqual(self.ledger.tasks.get(old['id']), old)

    def test_group_replaces_queued_slot_even_when_new_task_budget_is_used(self):
        note = self.grouped_note(); old = self.legacy_outreach(note, 'first')
        self.config['pipeline']['max_new_tasks_per_tick'] = 1
        self.tick()  # Mathesis uses the new-task budget; grouping reuses the old queued slot.
        self.assertEqual(self.ledger.tasks.get(old['id'])['state'], 'superseded')
        self.tick(); self.tick()
        self.assertEqual(self.ledger.tasks.get(old['id'])['state'], 'superseded')
        self.assertEqual(sum(r['kind']=='firstcontact' and r['state']=='queued' for r in self.ledger.tasks.all()), 1)

    def test_group_workflow_repin_keeps_one_live_lineage_on_rerun(self):
        self.grouped_note(); self.tick()
        group = next(r for r in self.ledger.tasks.all() if r['kind']=='firstcontact')
        successor = self.ledger.tasks.repin(group['id'],group['revision'],'c'*64,'d'*40)
        self.tick(); self.tick()
        groups = [r for r in self.ledger.tasks.all() if r['kind']=='firstcontact']
        self.assertEqual(len(groups), 2)
        self.assertEqual([r['id'] for r in groups if r['state']!='superseded'], [successor['id']])


def setUpModule():
    use_example_config()


if __name__=='__main__':unittest.main()
