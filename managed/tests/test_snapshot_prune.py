"""Ledger snapshots are pruned so the control release stays below 1000 assets."""
import unittest
import tempfile
from managed.control import Ledger, Releases
from test_tasks import Releases as MemoryReleases, request


class Fake(Releases):
    def __init__(self,rows):
        super().__init__();self.rows=rows;self.deleted=[]
    def release(self):return {'id':1}
    def run(self,*args,missing=False):
        if args[0]=='api' and '-X' in args:
            self.deleted.append(args[-1].rsplit('/',1)[1]);return b''
        return '\n'.join('\t'.join(r) for r in self.rows).encode()


class PruneTests(unittest.TestCase):
    def test_keeps_newest_and_protected(self):
        rows=[(str(i),'state-%02d.sqlite3'%i,'2026-11-01T00:%02d:00Z'%i) for i in range(10)]
        f=Fake(rows)
        self.assertEqual(f.prune_snapshots(4,{'state-01.sqlite3'}),5)
        self.assertEqual(f.deleted,['0','2','3','4','5'])

    def test_nothing_to_prune(self):
        f=Fake([('1','state-a.sqlite3','2026-11-01T00:00:00Z')])
        self.assertEqual(f.prune_snapshots(4),0);self.assertEqual(f.deleted,[])

    def test_forced_save_prunes_on_asset_limit_and_protects_current_pointer(self):
        class Limited(MemoryReleases):
            limited=False
            def upload(self,path):
                if self.limited:
                    self.limited=False
                    raise RuntimeError('file_count limited')
                super().upload(path)
            def prune_snapshots(self,keep,protect):self.pruned=(keep,protect)
        remote=Limited()
        with tempfile.TemporaryDirectory() as folder:
            ledger=Ledger(remote,folder)
            try:
                row=ledger.tasks.add(request());ledger.save()
                current=ledger.current;remote.limited=True
                ledger.tasks.update(row['id'],row['revision'],'held',{'reason':'new'})
                self.assertFalse(ledger.save())
                self.assertTrue(ledger.save(force=True))
                self.assertEqual(remote.pruned,(150,{current}))
                self.assertNotEqual(ledger.current,current)
                self.assertEqual(remote.body['snapshot']['name'],ledger.current)
            finally:ledger.tasks.close()


if __name__=='__main__':unittest.main()
