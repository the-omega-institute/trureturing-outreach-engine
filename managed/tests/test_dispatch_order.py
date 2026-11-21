import unittest
from managed.control import dispatch_order


def row(i,parent=None,state='queued',owners=None):
    inputs={'automation_parent':parent} if parent else {}
    if owners is not None:inputs['owner_ids']=owners
    return {'id':i,'state':state,'request':{'inputs':inputs}}


class DispatchOrderTests(unittest.TestCase):
    def test_continuations_first_then_arrival_order(self):
        rows=[row('a'),row('b'),row('rev-1','x'),row('c'),row('rev-2','y')]
        self.assertEqual([r['id'] for r in dispatch_order(rows)],['rev-1','rev-2','a','b','c'])

    def test_running_and_finished_work_before_queued(self):
        rows=[row('q1'),row('done',state='awaiting-review'),row('rev','x'),row('run',state='running')]
        self.assertEqual([r['id'] for r in dispatch_order(rows)],['done','run','rev','q1'])

    def test_recorded_correspondent_before_ownerless_backlog(self):
        rows=[row('a',owners=[]),row('b'),row('owned',owners=['contact-1']),row('rev','x'),row('c')]
        self.assertEqual([r['id'] for r in dispatch_order(rows)],['rev','owned','a','b','c'])


if __name__=='__main__':unittest.main()
