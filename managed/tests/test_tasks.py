import copy
import hashlib
import hmac
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from managed.control import ROOT, Ledger, advance, committed_workflow_text, workflow, validate_result
from managed.queue import GateError, canonical, digest
from managed.tasks import Tasks


def request(name='job-1',owner='scholar-a',kind='research'):
    return {'schema_version':1,'id':name,'kind':kind,'owner':owner,
            'source_revision':'a'*40,'inputs':{},'workflow_hash':workflow(kind)}


def use_fixture_workflow_revisions(test):
    """Let tests that use synthetic commit IDs share the repository policy fixtures."""
    def read(root, revision, path):
        if revision in {c * 40 for c in 'abcd'}:
            return (ROOT / path).read_text(encoding='utf-8')
        return committed_workflow_text(root, revision, path)
    test.enterContext(patch('managed.control.committed_workflow_text', side_effect=read))


class Releases:
    def __init__(self):self.assets={};self.body={'schema_version':1,'snapshot':None};self.fail_pointer=False
    def release(self):
        return {'body':canonical(self.body),'assets':[{'name':n,'digest':'sha256:'+hashlib.sha256(v).hexdigest()} for n,v in self.assets.items()]}
    def upload(self,path):
        if path.name in self.assets and self.assets[path.name]!=path.read_bytes():raise ValueError('Immutable conflict')
        self.assets[path.name]=path.read_bytes()
    def download(self,name,folder):
        path=Path(folder)/name;path.write_bytes(self.assets[name]);return path
    def pointer(self,pointer,folder):
        if self.fail_pointer:raise TimeoutError('Lost pointer write')
        self.body=copy.deepcopy(pointer)


class CMA:
    def __init__(self):self.creates={};self.commands={};self.lose_admission=False;self.retired=[]
    def request(self,path,method='GET',body=None,key=None):return {'status':'active'}
    def create(self,body,key):self.creates.setdefault(key,body);return {'id':'operation-1'}
    def operation(self,operation_id):return {'status':'succeeded','agent_id':'agt_test'}
    def admit(self,agent_id,prompt,key):
        self.commands.setdefault(key,{'agent':agent_id,'prompt':prompt})
        if self.lose_admission:self.lose_admission=False;raise TimeoutError('Admission accepted; observation lost')
        return {'response_id':'resp_original'}
    def response(self,response_id):return {'status':'in_progress'}
    def sandbox_of(self,agent_id):return 'sbx_test'
    def retire(self,agent_id,key,owned_sandbox_id=None):self.retired.append(agent_id);return {'deleted':True,'sandbox_retired':bool(owned_sandbox_id)}


class TaskTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        self.tasks=Tasks(self.root/'test.sqlite3')
    def tearDown(self):self.tasks.close();self.temp.cleanup()
    def test_bulk_reads_use_one_query_and_return_independent_current_rows(self):
        for name, kind in [('first', 'research'), ('second', 'mathesis'), ('third', 'reply')]:
            self.tasks.add(request(name, kind=kind))
        statements = []
        self.tasks.db.set_trace_callback(statements.append)
        rows = self.tasks.all()
        self.assertEqual([row['id'] for row in rows], ['first', 'second', 'third'])
        self.assertEqual(len(statements), 1)
        self.assertEqual(rows, [self.tasks.get(row['id']) for row in rows])
        rows[0]['data']['uncommitted'] = True
        current = self.tasks.get('first')
        self.tasks.update('first', current['revision'], 'held', {'reason': 'new state'})
        self.assertEqual(self.tasks.all()[0]['data'], {'reason': 'new state'})
        statements.clear()
        self.assertEqual([row['id'] for row in self.tasks.of_kinds('reply', 'mathesis')], ['second', 'third'])
        self.assertEqual(len(statements), 1)
        self.assertEqual(self.tasks.of_kinds(), [])

    def test_bulk_reads_observe_other_connections_and_transaction_rollback(self):
        self.tasks.add(request())
        other = Tasks(self.tasks.path)
        try:
            row = other.get('job-1')
            other.update(row['id'], row['revision'], 'held', {'reason': 'other connection'})
        finally:
            other.close()
        self.assertEqual(self.tasks.all()[0]['data']['reason'], 'other connection')
        with self.assertRaises(RuntimeError):
            with self.tasks.tx():
                row = self.tasks.get('job-1')
                self.tasks._update(row['id'], row['revision'], 'queued', {})
                self.assertEqual(self.tasks.all()[0]['state'], 'queued')
                raise RuntimeError('rollback')
        self.assertEqual(self.tasks.all()[0]['state'], 'held')

    def test_row_version_tracks_raw_writes_rollback_and_other_connections(self):
        row = self.tasks.add(request())
        original = self.tasks.row_version()
        self.tasks.db.execute("INSERT INTO pipeline_state VALUES('bookkeeping','{}')")
        self.assertEqual(self.tasks.row_version(), original)
        self.tasks.db.execute("UPDATE tasks SET data='{}' WHERE id=?", (row['id'],))
        raw = self.tasks.row_version()
        self.assertNotEqual(raw, original)
        with self.assertRaises(RuntimeError):
            with self.tasks.tx():
                self.tasks.db.execute('DELETE FROM tasks')
                self.assertNotEqual(self.tasks.row_version(), raw)
                raise RuntimeError('rollback')
        self.assertNotEqual(self.tasks.row_version(), raw)
        other = Tasks(self.tasks.path)
        try:
            other.update(row['id'], row['revision'], 'held', {'reason': 'external'})
        finally:
            other.close()
        self.assertNotEqual(self.tasks.row_version(), raw)
        self.tasks.snapshot(self.root / 'snapshot.sqlite3')
        import sqlite3
        snapshot = sqlite3.connect(self.root / 'snapshot.sqlite3')
        try:
            self.assertEqual(snapshot.execute(
                "SELECT name FROM sqlite_master WHERE name LIKE 'task_rows_%'").fetchall(), [])
        finally:
            snapshot.close()

    def test_row_version_invalidates_on_failed_writes_and_all_rollback_interfaces(self):
        import sqlite3
        self.tasks.add(request())
        for execute in (self.tasks.db.execute, self.tasks.db.cursor().execute,
                        lambda sql: self.tasks.db.executemany(sql, [()])):
            with self.subTest(execute=execute):
                version = self.tasks.row_version()
                with self.assertRaises(sqlite3.IntegrityError):
                    execute("UPDATE tasks SET state=NULL")
                self.assertNotEqual(self.tasks.row_version(), version)
        for rollback in (lambda: self.tasks.db.execute('ROLLBACK'),
                         lambda: self.tasks.db.cursor().execute('/* comment */ ROLLBACK'),
                         self.tasks.db.rollback,
                         lambda: self.tasks.db.executescript('BEGIN; SELECT 1; ROLLBACK;')):
            self.tasks.db.execute('BEGIN')
            version = self.tasks.row_version()
            rollback()
            self.assertNotEqual(self.tasks.row_version(), version)
        version = self.tasks.row_version()
        with self.assertRaises(RuntimeError):
            with self.tasks.db:
                self.tasks.db.execute('BEGIN')
                raise RuntimeError('context rollback')
        self.assertNotEqual(self.tasks.row_version(), version)
    def test_restart_replay_and_conflicting_inputs(self):
        r=request();self.tasks.add(r);self.tasks.close();self.tasks=Tasks(self.root/'test.sqlite3')
        self.assertEqual(self.tasks.add(r)['state'],'queued')
        with self.assertRaises(GateError):self.tasks.add({**r,'inputs':{'different':True}})
    def test_claim_serializes_scholar_and_bounds_independent_work(self):
        for r in [request(),request('job-2'),request('job-3','scholar-b')]:self.tasks.add(r)
        self.tasks.claim('job-1',2)
        other=Tasks(self.root/'test.sqlite3')
        try:
            with self.assertRaises(GateError):other.claim('job-2',2)
            other.claim('job-3',2)
        finally:other.close()
        self.tasks.add(request('job-4','scholar-c'))
        with self.assertRaises(GateError):self.tasks.claim('job-4',2)
    def test_stale_writer_cannot_replace_result(self):
        r=self.tasks.add(request());self.tasks.update(r['id'],r['revision'],'held',{'reason':'decision'})
        with self.assertRaises(GateError):self.tasks.update(r['id'],r['revision'],'queued',{})
    def test_shared_coauthor_serializes_different_primary_owners(self):
        first=request();first['inputs']['owner_ids']=['shared-scholar']
        second=request('job-2','scholar-b');second['inputs']['owner_ids']=['shared-scholar']
        self.tasks.add(first);self.tasks.add(second);self.tasks.claim('job-1',4)
        with self.assertRaises(GateError):self.tasks.claim('job-2',4)
    def test_review_is_signed_bound_and_replay_safe(self):
        r=self.tasks.add(request());r=self.tasks.update(r['id'],r['revision'],'awaiting-review',{'result':{'report':'proof'}})
        envelope={'binding':self.tasks.review_binding(r),'reviewer':'local-claude-loop','verdict':'PASS','reason':'proof checked','evidence_ref':'private:review'}
        envelope['signature']=hmac.new(b'reviewer-secret',canonical(envelope).encode(),hashlib.sha256).hexdigest()
        with self.assertRaises(GateError):self.tasks.verdict(envelope,'wrong-secret')
        self.assertEqual(self.tasks.verdict(envelope,'reviewer-secret')['state'],'approved')
        self.assertEqual(self.tasks.verdict(envelope,'reviewer-secret')['state'],'approved')
        changed={**envelope,'verdict':'HOLD'};unsigned={k:v for k,v in changed.items() if k!='signature'}
        changed['signature']=hmac.new(b'reviewer-secret',canonical(unsigned).encode(),hashlib.sha256).hexdigest()
        with self.assertRaises(GateError):self.tasks.verdict(changed,'reviewer-secret')
    def test_short_note_cannot_complete_without_tex_and_pdf(self):
        row={'id':'job-1','kind':'shortnote'}
        result={'schema_version':1,'task_id':'job-1','outcome':'completed','report':'checked','artifacts':[],'packet':None}
        with self.assertRaises(GateError):validate_result(result,row)


class CoordinatorTests(unittest.TestCase):
    def test_snapshot_pointer_failure_restores_prior_committed_state(self):
        remote=Releases()
        with tempfile.TemporaryDirectory() as temp:
            ledger=Ledger(remote,temp);ledger.tasks.add(request());ledger.save()
            ledger.tasks.add(request('job-2','scholar-b'));remote.fail_pointer=True
            with self.assertRaises(TimeoutError):ledger.save(force=True)
            ledger.tasks.close()
        with tempfile.TemporaryDirectory() as temp:
            restored=Ledger(remote,temp)
            self.assertEqual([r['id'] for r in restored.tasks.all()],['job-1']);restored.tasks.close()
    def test_uncertain_admission_recovers_original_task_without_new_agent(self):
        remote=Releases();cma=CMA();cma.lose_admission=True
        cfg={'paused':False,'kinds':{'research':True},'max_active':2,'agent_template':{'profile':{}}}
        with tempfile.TemporaryDirectory() as temp:
            ledger=Ledger(remote,temp);ledger.tasks.add(request());ledger.save()
            advance(ledger,cma,cfg,temp)
            self.assertEqual(ledger.tasks.get('job-1')['state'],'admission-unknown')
            ledger.save(force=True)  # Match the coordinator tick's final flush.
            ledger.tasks.close()
        with tempfile.TemporaryDirectory() as temp:
            ledger=Ledger(remote,temp)
            retry_at=ledger.tasks.get('job-1')['data']['recovery']['next_at']
            ledger.tasks.clock=lambda:retry_at
            advance(ledger,cma,cfg,temp)
            self.assertEqual(ledger.tasks.get('job-1')['state'],'running')
            self.assertEqual(len(cma.commands),1);self.assertEqual(len(cma.creates),1)
            ledger.tasks.close()
    def test_pause_admits_no_new_work_and_keeps_existing_response_handle(self):
        remote=Releases();cma=CMA()
        cfg={'paused':True,'kinds':{'research':True},'max_active':2,'agent_template':{}}
        with tempfile.TemporaryDirectory() as temp:
            ledger=Ledger(remote,temp);ledger.tasks.add(request());ledger.save()
            advance(ledger,cma,cfg,temp)
            self.assertFalse(cma.creates);self.assertEqual(ledger.tasks.get('job-1')['state'],'queued')
            ledger.tasks.close()
    def test_approved_reply_waits_for_mailbox_or_user_and_does_not_send(self):
        remote=Releases();cma=CMA()
        cfg={'paused':True,'kinds':{},'max_active':1,'agent_template':{}}
        with tempfile.TemporaryDirectory() as temp:
            ledger=Ledger(remote,temp)
            for name,decision,route,expected in [('reply-1',False,'auto','awaiting-mailbox'),
                                                ('reply-2',True,'auto','awaiting-mailbox'),
                                                ('reply-3',True,'owner','owner-draft')]:
                req=request(name,name,'reply');req['inputs']['user_decision_ever']=decision
                row=ledger.tasks.add(req);ledger.tasks.update(name,row['revision'],'approved',
                    {'result':{'packet':{'user_decision_ever':decision},'routing':{'mail':route}}})
                advance(ledger,cma,cfg,temp);self.assertEqual(ledger.tasks.get(name)['state'],expected)
            self.assertFalse(cma.commands);ledger.tasks.close()
    def test_bad_request_does_not_prevent_an_independent_valid_request(self):
        remote=Releases();remote.assets['request-bad.json']=b'{invalid json'
        remote.assets['request-good.json']=canonical(request()).encode()
        with tempfile.TemporaryDirectory() as temp:
            ledger=Ledger(remote,temp);errors=ledger.ingest()
            self.assertEqual(len(errors),1);self.assertEqual(ledger.tasks.get('job-1')['state'],'queued')
            ledger.tasks.close()
    def test_completed_invalid_result_is_held_with_original_agent_for_repair(self):
        remote=Releases();cma=CMA()
        cma.response=lambda response_id:{'status':'completed','output':[{'type':'message','content':[{'type':'output_text','text':'invalid result'}]}]}
        cfg={'paused':False,'kinds':{'research':True},'max_active':1,'agent_template':{}}
        with tempfile.TemporaryDirectory() as temp:
            ledger=Ledger(remote,temp);ledger.tasks.add(request());ledger.save()
            advance(ledger,cma,cfg,temp)
            row=ledger.tasks.get('job-1')
            self.assertEqual(row['state'],'held');self.assertEqual(row['data']['response_text'],'invalid result')
            self.assertEqual(cma.retired,[]);ledger.tasks.close()

    def test_failed_artifact_delivery_keeps_original_agent_for_recovery(self):
        remote=Releases();cma=CMA();data=b'useful result'
        artifact={'release_tag':'outreach-task-job-1','agent_path':'deliverables/evidence.txt',
                  'name':'evidence.txt','role':'evidence','bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()}
        result={'schema_version':1,'task_id':'job-1','outcome':'completed','report':'checked',
                'artifacts':[artifact],'packet':None}
        cma.response=lambda _: {'status':'completed','output':[{'type':'message','content':[{'type':'output_text','text':json.dumps(result)}]}]}
        cma.artifact=lambda *args:data
        def unavailable(path,tag):raise TimeoutError('Task artifact upload unavailable')
        remote.preserve_task=unavailable
        cfg={'paused':False,'kinds':{'research':True},'max_active':1,'agent_template':{}}
        with tempfile.TemporaryDirectory() as temp:
            ledger=Ledger(remote,temp)
            try:
                ledger.tasks.add(request());advance(ledger,cma,cfg,temp)
                row=ledger.tasks.get('job-1')
                self.assertEqual(row['state'],'running')
                self.assertEqual(row['data']['response_id'],'resp_original')
                self.assertIn('upload unavailable',row['data']['last_error'])
                self.assertFalse(cma.retired)
                self.assertEqual(len(cma.creates),1)
            finally:ledger.tasks.close()


if __name__=='__main__':unittest.main()


class ParkedHoldTests(unittest.TestCase):
    def test_reviewer_hold_parks_exact_agent_hold_until_result_changes(self):
        from managed.tasks import parked, reviewable
        from managed.queue import digest
        result={'outcome':'hold','report':'r'}
        row={'state':'held','kind':'reply','request':{'inputs':{}},
             'data':{'result':result}}
        self.assertTrue(reviewable(row))
        row['data']['review']={'verdict':'HOLD','binding':{'result_hash':digest(result)}}
        self.assertTrue(parked(row));self.assertFalse(reviewable(row))
        row['data']['result']={'outcome':'hold','report':'new'}
        self.assertFalse(parked(row));self.assertTrue(reviewable(row))
