import hashlib, hmac, io, json, tempfile, unittest
from pathlib import Path
from unittest.mock import patch

from fixture_config import use_cli_config
from managed import control
from managed.queue import canonical, digest
from managed.tasks import Tasks


def request(task_id='reply-1'):
    return {'schema_version':1,'id':task_id,'kind':'reply','owner':'contact-a','source_revision':'a'*40,
            'inputs':{'x':1},'workflow_hash':'b'*64}


def sign(binding, secret='secret', verdict='PASS', **optional):
    env={'binding':binding,'reviewer':'local-claude-loop','verdict':verdict,'reason':'checked','evidence_ref':'ref',**optional}
    env['signature']=hmac.new(secret.encode(),canonical(env).encode(),hashlib.sha256).hexdigest()
    return env


class ReviewBindingTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.tasks=Tasks(Path(self.temp.name)/'state.sqlite3')
        row=self.tasks.add(request())
        row=self.tasks.update(row['id'],row['revision'],'running',{'agent_id':'agt'})
        self.result={'schema_version':1,'task_id':'reply-1','outcome':'completed','report':'r','artifacts':[],'packet':None}
        self.row=self.tasks.update(row['id'],row['revision'],'awaiting-review',{'agent_id':'agt','result':self.result})

    def tearDown(self):self.tasks.close();self.temp.cleanup()

    def test_cleanup_after_result_keeps_signed_binding_valid(self):
        binding=self.tasks.review_binding(self.row)
        self.assertEqual(binding['task_revision'],self.row['revision'])
        row=self.row
        for step in ['stopped','deleted','retired']:
            row=self.tasks.update(row['id'],row['revision'],'awaiting-review',{**row['data'],'cleanup':step})
        self.assertEqual(self.tasks.review_binding(row),binding)
        self.assertEqual(self.tasks.verdict(sign(binding),'secret')['state'],'approved')

    def test_changed_result_still_invalidates(self):
        binding=self.tasks.review_binding(self.row)
        changed={**self.result,'report':'different'}
        row=self.tasks.update(self.row['id'],self.row['revision'],'awaiting-review',{**self.row['data'],'result':changed})
        self.assertNotEqual(self.tasks.review_binding(row)['result_hash'],binding['result_hash'])
        with self.assertRaises(ValueError):self.tasks.verdict(sign(binding),'secret')

    def test_rebind_is_signed_and_ingested_without_changing_binding(self):
        binding=self.tasks.review_binding(self.row)
        unsigned=sign(binding,rebind_workflow_hash='c'*64);unsigned.pop('signature')
        with patch('managed.control.workflow',return_value='c'*64) as workflow:
            envelope=control.sign_review(unsigned,'secret',kind='reply')
        workflow.assert_called_once_with('reply')
        tampered={**envelope,'rebind_workflow_hash':'d'*64}
        with self.assertRaisesRegex(ValueError,'signature'):
            self.tasks.verdict(tampered,'secret')
        row=self.tasks.verdict(envelope,'secret')
        self.assertEqual(row['data']['review'],envelope)
        self.assertEqual(row['data']['review']['binding'],binding)
        self.assertEqual(row['request'],request())
        self.assertEqual(self.tasks.verdict(envelope,'secret'),row)

    def test_signing_rejects_wrong_stale_or_missing_rebind_kind(self):
        unsigned=sign(self.tasks.review_binding(self.row));unsigned.pop('signature')
        with patch('managed.control.workflow',side_effect=lambda kind:'c'*64 if kind=='reply' else 'd'*64):
            for value in ('b'*64,'d'*64):
                with self.subTest(hash=value),self.assertRaisesRegex(ValueError,'current workflow'):
                    control.sign_review({**unsigned,'rebind_workflow_hash':value},'secret',kind='reply')
            with self.assertRaisesRegex(ValueError,'--kind'):
                control.sign_review({**unsigned,'rebind_workflow_hash':'c'*64},'secret')
            with self.assertRaisesRegex(ValueError,'current workflow'):
                control.sign_review({**unsigned,'rebind_workflow_hash':'c'*64},'secret',kind='firstcontact')

    def test_sign_review_auto_rebinds_only_stale_pass(self):
        binding = self.tasks.review_binding(self.row)
        for verdict, current, added in [('PASS','c'*64,True), ('PASS','b'*64,False),
                                        ('BOUNCE','c'*64,False), ('HOLD','c'*64,False)]:
            with self.subTest(verdict=verdict, current=current):
                unsigned = sign(binding, verdict=verdict)
                unsigned.pop('signature')
                stderr = io.StringIO()
                with patch.object(control, 'workflow', return_value=current), patch('sys.stderr', stderr):
                    envelope = control.sign_review(unsigned, 'secret', kind='reply')
                self.assertEqual('rebind_workflow_hash' in envelope, added)
                self.assertNotIn('rebind_workflow_hash', unsigned)
                self.assertEqual(len(stderr.getvalue().splitlines()), int(added))
                if added:self.assertEqual(envelope['rebind_workflow_hash'], current)
                payload = {k:v for k,v in envelope.items() if k != 'signature'}
                self.assertEqual(envelope['signature'], hmac.new(b'secret', canonical(payload).encode(), hashlib.sha256).hexdigest())

    def test_sign_review_without_kind_retains_original_pass(self):
        unsigned = sign(self.tasks.review_binding(self.row))
        unsigned.pop('signature')
        with patch.object(control, 'workflow') as workflow:
            envelope = control.sign_review(unsigned, 'secret')
        workflow.assert_not_called()
        self.assertNotIn('rebind_workflow_hash', envelope)

    def test_bounce_and_hold_cannot_rebind_even_with_a_valid_signature(self):
        binding=self.tasks.review_binding(self.row)
        for verdict in ('BOUNCE','HOLD'):
            with self.subTest(verdict=verdict):
                envelope=sign(binding,verdict=verdict,rebind_workflow_hash='c'*64)
                unsigned={k:v for k,v in envelope.items() if k!='signature'}
                with self.assertRaisesRegex(ValueError,'Only PASS'):
                    control.sign_review(unsigned,'secret',kind='reply')
                with self.assertRaisesRegex(ValueError,'Only PASS'):
                    self.tasks.verdict(envelope,'secret')

    def test_sign_review_cli_requires_and_uses_kind_for_rebind(self):
        unsigned=sign(self.tasks.review_binding(self.row),rebind_workflow_hash='c'*64)
        unsigned.pop('signature')
        source=Path(self.temp.name)/'review.json';source.write_text(canonical(unsigned))
        output=Path(self.temp.name)/'signed.json'
        args=['--output',str(output),'sign-review',str(source)]
        with patch('managed.control.review_key',return_value='secret'),patch('managed.control.workflow',return_value='c'*64):
            with self.assertRaisesRegex(ValueError,'--kind'):control.main(args)
            self.assertFalse(output.exists())
            control.main(args+['--kind','reply'])
        envelope=json.loads(output.read_text())
        self.assertEqual(self.tasks.verdict(envelope,'secret')['data']['review'],envelope)

    def test_sign_review_cli_auto_rebinds_stale_pass(self):
        binding = self.tasks.review_binding(self.row)
        unsigned = sign(binding)
        unsigned.pop('signature')
        source = Path(self.temp.name)/'review.json'
        source.write_text(canonical(unsigned))
        stdout, stderr = io.StringIO(), io.StringIO()
        with patch.object(control, 'review_key', return_value='secret'), \
                patch.object(control, 'workflow', return_value='c'*64), \
                patch('sys.stdout', stdout), patch('sys.stderr', stderr):
            control.main(['sign-review',str(source),'--kind','reply'])
        envelope = json.loads(stdout.getvalue())
        self.assertEqual(envelope['binding'], binding)
        self.assertEqual(envelope['rebind_workflow_hash'], 'c'*64)
        self.assertEqual(len(stderr.getvalue().splitlines()), 1)
        self.assertEqual(self.tasks.verdict(envelope, 'secret')['state'], 'approved')

    def test_new_review_round_gets_new_revision(self):
        first=self.tasks.review_binding(self.row)
        row=self.tasks.update(self.row['id'],self.row['revision'],'running',{'agent_id':'agt'})
        row=self.tasks.update(row['id'],row['revision'],'awaiting-review',{'agent_id':'agt','result':self.result})
        self.assertGreater(self.tasks.review_binding(row)['task_revision'],first['task_revision'])

    def test_agent_hold_cleanup_keeps_binding_and_pass_is_forbidden(self):
        result={**self.result,'outcome':'hold','report':'Oracle route needs repair'}
        row=self.tasks.update(self.row['id'],self.row['revision'],'held',{'result':result,'agent_id':'agt'})
        binding=self.tasks.review_binding(row)
        row=self.tasks.update(row['id'],row['revision'],'held',{**row['data'],'cleanup':{'deleted':True}})
        self.assertEqual(self.tasks.review_binding(row),binding)
        self.assertEqual(self.tasks.reviews()[0]['binding'],binding)
        with self.assertRaisesRegex(ValueError,'never PASS'):
            self.tasks.verdict(sign(binding),'secret')
        self.assertEqual(self.tasks.get(row['id'])['state'],'held')
        envelope=sign(binding,verdict='HOLD')
        held=self.tasks.verdict(envelope,'secret')
        self.assertEqual(held['state'],'held')
        self.assertEqual(self.tasks.verdict(envelope,'secret'),held)
        # A reviewer HOLD needs a fresh binding to release by BOUNCE later.
        new_binding=self.tasks.review_binding(held)
        self.assertGreater(new_binding['task_revision'],binding['task_revision'])
        cleaned=self.tasks.update(held['id'],held['revision'],'held',{**held['data'],'cleanup':{'retired':True}})
        self.assertEqual(self.tasks.review_binding(cleaned),new_binding)
        with self.assertRaises(ValueError):
            self.tasks.verdict(sign(binding,verdict='BOUNCE'),'secret')
        bounced=self.tasks.verdict(sign(new_binding,verdict='BOUNCE'),'secret')
        self.assertEqual(bounced['state'],'bounced')

    def test_coordinator_hold_is_not_reviewable(self):
        for data in [{'reason':'Connector failed'}, {'reason':'Workflow changed'},
                     {'result':self.result,'reason':'Delivery gate failed'}]:
            row=self.tasks.get(self.row['id'])
            row=self.tasks.update(row['id'],row['revision'],'held',data)
            self.assertEqual(self.tasks.reviews(),[])
            with self.assertRaises(ValueError):self.tasks.review_binding(row)


if __name__=='__main__':unittest.main()


def setUpModule():
    use_cli_config()
