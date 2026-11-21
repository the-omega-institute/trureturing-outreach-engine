"""Compact audit history and remote checkpoints under crash/replay and load."""
import hashlib
import json
from contextlib import ExitStack
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from fixture_config import use_cli_config
from managed.control import Ledger, advance
from managed import control
from managed.queue import canonical, digest
from managed.tasks import EVENT_COMPACTION, Tasks
from test_review_binding import sign
from test_tasks import CMA, Releases, request


class CompactionTests(unittest.TestCase):
    def test_legacy_migration_preserves_bindings_markers_and_current_payload(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'state.sqlite3';tasks=Tasks(path)
            row=tasks.add(request())
            payload={'result':{'outcome':'hold','report':'evidence'*20000},
                     'operation_id':'op-original','reason':'a'*1000}
            row=tasks.update(row['id'],row['revision'],'held',payload)
            original_binding=tasks.review_binding(row)
            envelope=sign(original_binding,verdict='HOLD')
            row=tasks.verdict(envelope,'secret')
            hold_binding=tasks.review_binding(row)
            row=tasks.update(row['id'],row['revision'],'held',{**row['data'],'cleanup':{'deleted':True}})
            # Reconstruct legacy full-data update events, retaining the exact
            # small verdict/decision markers that legacy code already emitted.
            tasks.db.execute('UPDATE task_events SET data=? WHERE revision IN (2,4)',(canonical(row['data']),))
            tasks.db.execute('INSERT INTO task_events(id,state,revision,at,data) VALUES(?,?,?,?,?)',
                             ('decision-a','decision-recorded',2,1,canonical({'decision':'original-decision'})))
            tasks.db.execute('DELETE FROM task_migrations')
            before=path.stat().st_size
            metadata=[tuple(r) for r in tasks.db.execute('SELECT seq,id,state,revision,at FROM task_events')]
            current=tasks.get(row['id'])
            control=[tuple(r) for r in tasks.db.execute('SELECT * FROM control_events')]
            tasks.close();tasks=Tasks(path)
            try:
                self.assertLess(path.stat().st_size,before/2)
                self.assertEqual(tasks.get(row['id']),current)
                self.assertEqual(tasks.review_binding(current),hold_binding)
                self.assertGreater(hold_binding['task_revision'],original_binding['task_revision'])
                # A reviewer-HOLDed agent hold is parked, not re-listed for review.
                self.assertEqual(tasks.reviews(),[])
                self.assertEqual([tuple(r) for r in tasks.db.execute('SELECT * FROM control_events')],control)
                self.assertEqual([tuple(r) for r in tasks.db.execute('SELECT seq,id,state,revision,at FROM task_events')],metadata)
                summaries=[json.loads(r[0]) for r in tasks.db.execute('SELECT data FROM task_events')]
                self.assertIn({'review':digest(envelope)},summaries)
                self.assertIn({'decision':'original-decision'},summaries)
                self.assertTrue(all('result' not in s for s in summaries))
                self.assertEqual(summaries[1]['operation_id'],'op-original')
                self.assertEqual(len(summaries[1]['reason']),300)
                self.assertEqual(tasks.db.execute('PRAGMA freelist_count').fetchone()[0],0)
            finally:tasks.close()
            sha=hashlib.sha256(path.read_bytes()).hexdigest()
            tasks=Tasks(path);tasks.close()
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(),sha)

    def test_new_updates_store_small_summaries(self):
        with tempfile.TemporaryDirectory() as folder:
            tasks=Tasks(Path(folder)/'state.sqlite3')
            try:
                row=tasks.add(request())
                data={'result':{'packet':{'attachments':['x'*1000000]}},'response_id':'resp-original',
                      'reason':'checked','review':{'verdict':'PASS'}}
                tasks.update(row['id'],row['revision'],'awaiting-review',data)
                summary=tasks.db.execute('SELECT data FROM task_events ORDER BY seq DESC LIMIT 1').fetchone()[0]
                self.assertEqual(json.loads(summary),{'response_id':'resp-original','reason':'checked','verdict':'PASS'})
                self.assertEqual(tasks.get(row['id'])['data'],data)
            finally:tasks.close()

    def test_interrupted_compaction_rolls_back_and_pending_vacuum_resumes(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'state.sqlite3';tasks=Tasks(path)
            row=tasks.add(request());tasks.update(row['id'],row['revision'],'held',{'reason':'r'})
            tasks.db.execute('UPDATE task_events SET data=? WHERE revision=1',(canonical({'result':'legacy'}),))
            tasks.db.execute('DELETE FROM task_migrations')
            before=[tuple(r) for r in tasks.db.execute('SELECT * FROM task_events')]
            with patch('managed.tasks.event_summary',side_effect=[{},RuntimeError('killed')]):
                with self.assertRaises(RuntimeError):tasks.compact_events()
            self.assertEqual([tuple(r) for r in tasks.db.execute('SELECT * FROM task_events')],before)
            self.assertFalse(tasks.db.execute('SELECT * FROM task_migrations').fetchall())
            # Simulate a kill after the rewrite transaction, before VACUUM.
            tasks.db.execute('INSERT INTO task_migrations VALUES(?,?)',(EVENT_COMPACTION,'vacuum'))
            tasks.db.execute('CREATE TABLE padding(data BLOB)')
            tasks.db.execute('INSERT INTO padding VALUES(zeroblob(1000000))')
            tasks.db.execute('DELETE FROM padding')
            self.assertGreater(tasks.db.execute('PRAGMA freelist_count').fetchone()[0],0)
            tasks.close();tasks=Tasks(path)
            try:
                self.assertEqual(tasks.db.execute('PRAGMA freelist_count').fetchone()[0],0)
                self.assertEqual(tasks.db.execute('SELECT phase FROM task_migrations').fetchone()[0],'done')
            finally:tasks.close()


class CountingReleases(Releases):
    def __init__(self):super().__init__();self.uploads=0
    def upload(self,path):self.uploads+=1;super().upload(path)


class CheckpointTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.folder=Path(self.temp.name)
        self.remote=CountingReleases();self.now=0
        self.ledger=Ledger(self.remote,self.folder,clock=lambda:self.now)
        self.cfg={'paused':False,'kinds':{'research':True},'max_active':24,'agent_template':{}}
    def tearDown(self):self.ledger.tasks.close();self.temp.cleanup()
    def restore(self):
        self.ledger.tasks.close();self.folder=self.folder/'restored';self.folder.mkdir()
        self.ledger=Ledger(self.remote,self.folder,clock=lambda:self.now)
    def observed(self):
        with tempfile.TemporaryDirectory() as folder:
            saved=Ledger(self.remote,folder)
            try:return saved.tasks.get('job-1')
            finally:saved.tasks.close()

    def test_24_tasks_and_144_transitions_need_only_two_uploads(self):
        for i in range(24):self.ledger.tasks.add(request('job-'+str(i),'owner-'+str(i)))
        self.ledger.save()
        for i in range(24):
            for step in range(6):
                row=self.ledger.tasks.get('job-'+str(i))
                self.ledger.tasks.update(row['id'],row['revision'],'held',{'reason':str(step)})
                self.assertFalse(self.ledger.save())
        self.assertEqual(self.remote.uploads,1)
        self.assertTrue(self.ledger.save(force=True))
        self.assertEqual(self.remote.uploads,2)
        expected=self.ledger.tasks.all();self.restore()
        self.assertEqual(self.ledger.tasks.all(),expected)

    def test_interval_force_and_unchanged_save_do_not_snapshot_unnecessarily(self):
        row=self.ledger.tasks.add(request());self.ledger.save()
        row=self.ledger.tasks.update(row['id'],row['revision'],'held',{'reason':'one'})
        with patch.object(self.ledger.tasks,'snapshot',wraps=self.ledger.tasks.snapshot) as snapshot:
            self.now=44.99;self.assertFalse(self.ledger.save());snapshot.assert_not_called()
            self.now=45;self.assertTrue(self.ledger.save())
            row=self.ledger.tasks.update(row['id'],row['revision'],'held',{'reason':'two'})
            self.now=46;self.assertTrue(self.ledger.save(force=True))
            self.assertFalse(self.ledger.save(force=True))
            self.assertEqual(snapshot.call_count,2)
        self.assertEqual(self.remote.uploads,3)

    def test_failed_pointer_does_not_mark_changes_saved_or_advance_generation(self):
        row=self.ledger.tasks.add(request());self.ledger.save()
        self.ledger.tasks.update(row['id'],row['revision'],'held',{'reason':'new'})
        self.remote.fail_pointer=True
        with self.assertRaises(TimeoutError):self.ledger.save(force=True)
        self.assertEqual(self.ledger.generation,1)
        self.assertEqual(self.observed()['state'],'queued')
        self.remote.fail_pointer=False;self.ledger.save(force=True)
        self.assertEqual(self.observed()['state'],'held')
        self.assertEqual(self.ledger.generation,2)

    def test_cma_create_and_admit_intents_are_remote_and_lost_receipt_replays(self):
        self.ledger.tasks.add(request());self.ledger.save(force=True)
        cma=CMA();create=cma.create;admit=cma.admit;calls=[]
        def creating(body,key):
            saved=self.observed()
            self.assertEqual(saved['data']['create_key'],key)
            self.assertEqual(saved['data']['create_body'],body)
            calls.append(key);result=create(body,key)
            if len(calls)==1:raise KeyboardInterrupt('Create accepted; process killed')
            return result
        def admitting(agent,prompt,key):
            saved=self.observed()
            self.assertEqual(saved['state'],'admitting')
            self.assertEqual(saved['data']['agent_id'],agent)
            self.assertEqual(saved['data']['command_key'],key)
            self.assertEqual(saved['data']['prompt'],prompt)
            return admit(agent,prompt,key)
        cma.create=creating;cma.admit=admitting
        with self.assertRaises(KeyboardInterrupt):advance(self.ledger,cma,self.cfg,self.folder)
        self.restore();advance(self.ledger,cma,self.cfg,self.folder)
        self.assertEqual(calls,[calls[0],calls[0]])
        self.assertEqual(self.ledger.tasks.get('job-1')['state'],'running')
        # Kill before the debounced running receipt is uploaded. Restoring the
        # admitting intent repeats the same command and recovers its response.
        self.restore();advance(self.ledger,cma,self.cfg,self.folder)
        self.assertEqual(len(cma.creates),1);self.assertEqual(len(cma.commands),1)
        self.assertEqual(self.ledger.tasks.get('job-1')['data']['response_id'],'resp_original')

    def test_failed_checkpoint_prevents_cma_create_and_admit(self):
        self.ledger.tasks.add(request());self.ledger.save(force=True)
        cma=CMA();self.remote.fail_pointer=True
        advance(self.ledger,cma,self.cfg,self.folder)
        self.assertFalse(cma.creates);self.assertFalse(cma.commands)
        self.remote.fail_pointer=False
        retry_at=self.ledger.tasks.get('job-1')['data']['recovery']['next_at']
        self.ledger.tasks.clock=lambda:retry_at
        operation=cma.operation
        def fail_before_admit(operation_id):
            self.remote.fail_pointer=True
            return operation(operation_id)
        cma.operation=fail_before_admit
        advance(self.ledger,cma,self.cfg,self.folder)
        self.assertEqual(len(cma.creates),1);self.assertFalse(cma.commands)

    def test_lost_noncritical_route_is_recomputed_next_tick(self):
        row=self.ledger.tasks.add(request())
        self.ledger.tasks.update(row['id'],row['revision'],'approved',{'result':{'outcome':'completed','packet':None}})
        self.ledger.save(force=True)
        advance(self.ledger,CMA(),self.cfg,self.folder)
        self.assertEqual(self.ledger.tasks.get('job-1')['state'],'handoff-ready')
        self.assertEqual(self.observed()['state'],'approved')
        self.restore();advance(self.ledger,CMA(),self.cfg,self.folder)
        self.assertEqual(self.ledger.tasks.get('job-1')['state'],'handoff-ready')
        self.ledger.save(force=True)
        self.assertEqual(self.observed()['state'],'handoff-ready')

    def run_main_tick(self,fail=False):
        self.ledger.tasks.add(request());self.ledger.save(force=True)
        self.remote.ensure=lambda:None
        pruned=[]
        def prune(keep,protect):
            self.assertEqual(self.observed()['state'],'held')
            pruned.append((keep,protect))
        self.remote.prune_snapshots=prune
        def progress(*args,**kwargs):
            row=self.ledger.tasks.get('job-1')
            self.ledger.tasks.update(row['id'],row['revision'],'held',{'reason':'replayable'})
            self.assertFalse(self.ledger.save())
        with ExitStack() as stack:
            stack.enter_context(patch.dict('os.environ',{'GITHUB_ACTIONS':'true'}))
            for name,kwargs in {
                'managed.control.Releases':{'return_value':self.remote},
                'managed.control.Ledger':{'return_value':self.ledger},
                'managed.control.CMA':{'return_value':CMA()},
                'managed.control.advance':{'side_effect':progress},
                'managed.mailbox.configured':{'return_value':None},
                'managed.mailbox.intake':{},'managed.pipeline.reconcile':{},
                'managed.pipeline.status':{'return_value':{}},
                'managed.automation.reconcile':{},'managed.delivery.advance':{},
                'managed.mathesis_delivery.advance':{'side_effect':RuntimeError('tick interrupted')} if fail else {},
            }.items():stack.enter_context(patch(name,**kwargs))
            args=['--output',str(self.folder/'status.json'),'tick']
            if fail:
                with self.assertRaisesRegex(RuntimeError,'tick interrupted'):control.main(args)
            else:control.main(args)
        self.assertEqual(self.observed()['state'],'held')
        self.assertEqual(len(pruned),0 if fail else 1)
        self.assertEqual(self.remote.uploads,3)  # Includes the one-time workflow migration checkpoint.

    def test_main_forces_final_snapshot_before_pruning(self):self.run_main_tick()
    def test_main_forces_progress_on_exceptional_exit(self):self.run_main_tick(fail=True)


if __name__=='__main__':unittest.main()


def setUpModule():
    use_cli_config()
