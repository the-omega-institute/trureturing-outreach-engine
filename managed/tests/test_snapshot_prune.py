"""Ledger snapshots are pruned so the control release stays below 1000 assets."""
import unittest
import tempfile
from pathlib import Path
from unittest.mock import patch
from managed.control import Ledger, Releases
from test_tasks import Releases as MemoryReleases, request


class Fake(Releases):
    def __init__(self,rows):
        super().__init__();self.rows=rows;self.deleted=[]
    def release(self):return {'id':1}
    def run(self,*args,missing=False):
        if args[0]=='api' and '-X' in args:
            self.deleted.append(args[-1].rsplit('/',1)[1]);return b''
        return '\n'.join('\t'.join(r[:2]) for r in self.rows).encode()


class RetentionReleases(MemoryReleases):
    """Exercise real pruning against a synthetic immutable asset service."""
    prune_snapshots=Releases.prune_snapshots
    snapshot_rows=Releases.snapshot_rows
    prune_ingested=Releases.prune_ingested
    snapshot_receipts=Releases.snapshot_receipts
    upload_retained=Releases.upload_retained
    prune_logs=Releases.prune_logs
    repo='synthetic/repo'
    def __init__(self):
        super().__init__();self.ids={};self.deleted=[]
    def release(self):
        value=super().release();value['id']=1
        for asset in value['assets']:
            asset['id']=self.ids.setdefault(asset['name'],len(self.ids)+1)
            asset['created_at']='2026-11-23T00:00:00Z'
        return value
    def run(self,*args,**kwargs):
        assets=self.release()['assets']
        if args[:3]==('api','-X','DELETE'):
            asset=next(a for a in assets if str(a['id'])==args[-1].rsplit('/',1)[1])
            assert asset['name']!=(self.body.get('snapshot') or {}).get('name')
            self.deleted.append(asset['name']);del self.assets[asset['name']];return b''
        assert args[:2]==('api','--paginate'),args
        query=args[-1]
        for prefix in ('state-','tick-log-','run-log-'):
            if f'startswith("{prefix}")' in query:assets=[a for a in assets if a['name'].startswith(prefix)]
        keys=['id','name']
        if '.created_at' in query:keys.append('created_at')
        if '.digest' in query:keys.append('digest')
        return '\n'.join('\t'.join(str(a.get(k,'')) for k in keys) for a in assets).encode()


class PruneTests(unittest.TestCase):
    def test_no_valid_pointer_never_prunes_even_with_more_than_three_snapshots(self):
        rows=[(str(i),f'state-{i}.sqlite3','same-time') for i in range(10)]
        for body in (None, '{}', '{"schema_version":1,"snapshot":null}', 'invalid-json', '[]'):
            with self.subTest(body=body):
                f=Fake(rows)
                with patch.object(f, 'release', return_value={'id':1,'body':body}):
                    self.assertEqual(f.prune_snapshots(3), 0)
                self.assertEqual(f.deleted, [])

    def test_keeps_committed_history_and_explicitly_protected(self):
        rows=[(str(i),'state-%02d.sqlite3'%i,'2026-11-01T00:%02d:00Z'%i) for i in range(10)]
        f=Fake(rows)
        from managed.queue import canonical
        pointer={'schema_version':1,'snapshot':{'name':'state-09.sqlite3'},
                 'run_history':[{'name':f'state-{i:02}.sqlite3'} for i in (8,7,6)]}
        with patch.object(f,'release',return_value={'id':1,'body':canonical(pointer)}):
            self.assertEqual(f.prune_snapshots(4,{'state-01.sqlite3'}),5)
        self.assertEqual(f.deleted,['0','2','3','4','5'])

    def test_nothing_to_prune(self):
        f=Fake([('1','state-a.sqlite3','2026-11-01T00:00:00Z')])
        self.assertEqual(f.prune_snapshots(4),0);self.assertEqual(f.deleted,[])

    def test_forced_save_prunes_on_asset_limit_and_protects_current_pointer(self):
        class Limited(MemoryReleases):
            upload_retained=Releases.upload_retained
            limited=False
            def upload(self,path):
                if self.limited:
                    self.limited=False
                    raise RuntimeError('file_count limited')
                super().upload(path)
            def prune_snapshots(self,keep,protect):
                self.pruned=getattr(self,'pruned',[])+[(keep,protect)]
            def prune_logs(self,*args,**kwargs):pass
            def evict_oldest_log(self,*args):return 0
            def prune_ingested(self):pass
        remote=Limited()
        with tempfile.TemporaryDirectory() as folder:
            ledger=Ledger(remote,folder)
            try:
                row=ledger.tasks.add(request());ledger.save()
                current=ledger.current;remote.limited=True
                ledger.tasks.update(row['id'],row['revision'],'held',{'reason':'new'})
                self.assertFalse(ledger.save())
                self.assertTrue(ledger.save(force=True))
                self.assertEqual(remote.pruned[-2],(3,{ledger.current}))
                self.assertEqual(remote.pruned[-1],(3,{ledger.current}))
                self.assertNotEqual(ledger.current,current)
                self.assertEqual(remote.body['snapshot']['name'],ledger.current)
            finally:ledger.tasks.close()

    def test_each_run_retains_live_pointer_and_two_committed_run_predecessors(self):
        remote=RetentionReleases();committed=[]
        with tempfile.TemporaryDirectory() as folder:
            ledger=Ledger(remote,folder)
            try:
                row=ledger.tasks.add(request())
                for i in range(7):
                    ledger.run_id=f'synthetic-run-{i}'
                    row=ledger.tasks.update(row['id'],row['revision'],'held',{'reason':str(i)})
                    self.assertTrue(ledger.save(force=True));committed.append(ledger.current)
                    self.assertEqual(set(remote.assets),set(committed[-3:]))
                    self.assertEqual([item['name'] for item in remote.body['run_history']],list(reversed(committed[:-1]))[:2])
                # A failed/uncommitted upload never becomes a predecessor.
                orphan=Path(folder)/'state-orphan.sqlite3';orphan.write_bytes(b'orphan');remote.upload(orphan)
                remote.fail_pointer=True
                ledger.run_id='retry-run'
                row=ledger.tasks.update(row['id'],row['revision'],'held',{'reason':'pending'})
                with self.assertRaises(TimeoutError):ledger.save(force=True)
                self.assertIn(committed[-1],remote.assets)
                remote.fail_pointer=False;ledger.save(force=True);committed.append(ledger.current)
                self.assertEqual(set(remote.assets),set(committed[-3:]))
                self.assertNotIn(orphan.name,remote.assets)
            finally:ledger.tasks.close()

    def test_failed_upload_and_lost_pointer_receipt_preserve_live_target(self):
        remote=RetentionReleases()
        with tempfile.TemporaryDirectory() as folder:
            ledger=Ledger(remote,folder)
            try:
                row=ledger.tasks.add(request());ledger.save(force=True);current=ledger.current
                row=ledger.tasks.update(row['id'],row['revision'],'held',{'reason':'new'})
                with patch.object(remote,'upload',side_effect=RuntimeError('upload failed')):
                    with self.assertRaisesRegex(RuntimeError,'upload failed'):ledger.save(force=True)
                self.assertEqual(remote.body['snapshot']['name'],current);self.assertIn(current,remote.assets)
                pointer=remote.pointer
                def lost(value,folder):pointer(value,folder);raise TimeoutError('lost receipt')
                with patch.object(remote,'pointer',side_effect=lost):
                    with self.assertRaises(TimeoutError):ledger.save(force=True)
                applied=remote.body['snapshot']['name']
                self.assertNotEqual(applied,current)
                # Even a stale caller's headroom pruning reads and protects the
                # actual live pointer, including after an uncertain write.
                remote.prune_snapshots(3,{current,applied})
                self.assertIn(applied,remote.assets);self.assertIn(current,remote.assets)
                ledger.save(force=True)
                with tempfile.TemporaryDirectory() as restored:
                    check=Ledger(remote,restored)
                    try:self.assertEqual(check.tasks.get(row['id'])['state'],'held')
                    finally:check.tasks.close()
            finally:ledger.tasks.close()

    def test_live_pointer_overrides_stale_protect_and_timestamp_ties(self):
        remote=RetentionReleases()
        for i in range(6):
            remote.assets[f'state-{i}.sqlite3']=b'synthetic'
        remote.release()
        remote.body={'schema_version':1,'generation':3,'snapshot':{'name':'state-2.sqlite3'},
                     'run_history':[{'name':'state-1.sqlite3'},{'name':'state-0.sqlite3'}]}
        remote.prune_snapshots(3)
        self.assertEqual(set(remote.assets),{'state-0.sqlite3','state-1.sqlite3','state-2.sqlite3'})

    def test_committed_history_survives_truncated_embedded_asset_listing(self):
        remote=RetentionReleases();committed=[]
        with tempfile.TemporaryDirectory() as folder:
            ledger=Ledger(remote,folder)
            try:
                row=ledger.tasks.add(request())
                for i in range(3):
                    ledger.run_id=f'synthetic-run-{i}'
                    row=ledger.tasks.update(row['id'],row['revision'],'held',{'reason':str(i)})
                    ledger.save(force=True);committed.append(ledger.current)
            finally:ledger.tasks.close()
            release=remote.release
            def partial():
                value=release();value['assets']=value['assets'][:1];return value
            # A truncated embedded listing cannot change committed history.
            with tempfile.TemporaryDirectory() as restored:
                with patch.object(remote,'release',side_effect=partial):
                    # The fake API itself uses release(); retain its full API
                    # listing while truncating only the constructor response.
                    run=remote.run
                    def full_api(*args,**kwargs):
                        with patch.object(remote,'release',side_effect=release):return run(*args,**kwargs)
                    with patch.object(remote,'run',side_effect=full_api):check=Ledger(remote,restored)
                try:
                    self.assertEqual([item['name'] for item in check.run_history],list(reversed(committed[:-1])))
                    row=check.tasks.get(row['id'])
                    check.run_id='after-migration'
                    check.tasks.update(row['id'],row['revision'],'held',{'reason':'after migration'})
                    check.save(force=True)
                    self.assertEqual(set(remote.assets),{check.current,*committed[-2:]})
                finally:check.tasks.close()

    def test_restored_unchanged_snapshot_does_not_age_coverage_or_repeat_predecessor(self):
        remote=RetentionReleases()
        with tempfile.TemporaryDirectory() as folder:
            ledger=Ledger(remote,folder)
            try:
                row=ledger.tasks.add(request())
                for i in range(3):
                    row=ledger.tasks.update(row['id'],row['revision'],'held',{'reason':str(i)})
                    ledger.save(force=True)
            finally:ledger.tasks.close()
            original=remote.body.copy();snapshots=set(remote.assets)
            with tempfile.TemporaryDirectory() as restored:
                ledger=Ledger(remote,restored)
                try:
                    self.assertFalse(ledger.save(force=True))
                    self.assertEqual(remote.body,original)
                    self.assertEqual(set(remote.assets),snapshots)
                    self.assertEqual(len(snapshots),1)
                finally:ledger.tasks.close()


if __name__=='__main__':unittest.main()
