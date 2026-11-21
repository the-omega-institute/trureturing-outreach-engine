"""The mailbox owner's own table-1 row must not serialize every reply."""
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from managed.control import Ledger, advance
from managed.tasks import Tasks, owners
from test_tasks import CMA, Releases, request


def reply(name,owner,ids):
    r=request(name,owner,'reply');r['inputs']={'owner_ids':ids};return r


class SelfOwnerTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.folder=Path(self.temp.name)
        self.ledger=Ledger(Releases(),self.folder);self.cma=CMA()
        self.cfg={'paused':False,'kinds':{'reply':True},'max_active':5,'agent_template':{},
                  'mailbox':{'account':'me@alumni.example.invalid'}}
    def tearDown(self):self.ledger.tasks.close();self.temp.cleanup()
    def tick(self,ids):
        with patch('managed.control.self_contact_ids',return_value=frozenset(ids)),\
             patch('managed.control.workflow',side_effect=lambda k, revision=None:request('x','o',k)['workflow_hash']):
            advance(self.ledger,self.cma,self.cfg,self.folder,revision='b'*40)

    def test_shared_self_contact_does_not_serialize(self):
        for i in range(3):self.ledger.tasks.add(reply('reply-%d'%i,'scholar-%d'%i,['self','scholar-%d'%i]))
        self.tick({'self'})
        started=[r for r in self.ledger.tasks.all() if r['state']!='queued']
        self.assertEqual(len(started),3)

    def test_real_shared_scholar_still_serializes(self):
        for i in range(2):self.ledger.tasks.add(reply('reply-%d'%i,'scholar-a',['self','scholar-a']))
        self.tick({'self'})
        self.assertEqual(sum(r['state']!='queued' for r in self.ledger.tasks.all()),1)

    def test_owners_ignore(self):
        row={'owner':'a','request':{'inputs':{'owner_ids':['self','b']}}}
        self.assertEqual(owners(row,{'self'}),{'a','b'})


if __name__=='__main__':unittest.main()
