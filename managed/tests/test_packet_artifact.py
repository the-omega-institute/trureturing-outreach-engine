"""Exact mail packets delivered through private task artifacts, without live I/O."""
import base64
import copy
import hashlib
import hmac
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from managed.automation import work
from managed.control import Ledger, MAIL_PACKET_MAX_BYTES, advance, validate_result
from managed.queue import canonical, digest
from test_tasks import CMA, Releases, request


class PacketReleases(Releases):
    repo='fixture/repo'

    def __init__(self):
        super().__init__();self.files={};self.calls=[];self.corrupt=False

    def preserve_task(self,path,tag):
        self.calls.append('preserve')
        self.files[tag,path.name]=path.read_bytes()

    def run(self,*args):
        self.calls.append('readback')
        tag=args[2];name=args[args.index('--pattern')+1]
        raw=self.files[tag,name]
        if self.corrupt:raw=b'X'*len(raw)
        (Path(args[args.index('--dir')+1])/name).write_bytes(raw)


class PacketArtifactTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        self.remote=PacketReleases();self.ledger=Ledger(self.remote,self.root)
        self.cma=CMA()
        self.cfg={'paused':False,'kinds':{'reply':True},'max_active':1,'agent_template':{}}
        req=request('reply-1',kind='reply')
        req['inputs']={'automation_contract':1,'thread_id':'thread-1',
                       'latest_inbound_message_id':'<inbound-1>','user_decision_ever':True}
        self.row=self.ledger.tasks.add(req)
        self.packet=json.loads((Path(__file__).parent/'fixtures/letter.json').read_text())
        self.packet.update(kind='reply',thread_id='thread-1',latest_inbound_message_id='<inbound-1>',
                           user_decision_ever=True)
        self.packet['attachments']=[{'name':'note.pdf','media_type':'application/pdf',
                                    'content_b64':base64.b64encode(b'%PDF-'+b'x'*(207*1024)).decode()}]
        self.result={'schema_version':1,'task_id':'reply-1','outcome':'completed','report':'Checked result.',
                     'packet':{'artifact':'mail-packet.json'},'artifacts':[],
                     'routing':{'mail':'owner','questions':[],'independent_submission':None},
                     'owner_brief':{'summary_zh':'这封信处理了当前请求。','decisions_zh':['决定是否发送。'],
                                    'cautions_zh':['发送前检查收件人。']}}
        self.set_payload(canonical(self.packet).encode())

    def tearDown(self):
        self.ledger.tasks.close();self.temp.cleanup()

    def set_payload(self,raw):
        self.result['artifacts']=[{'release_tag':'outreach-task-reply-1','name':'mail-packet.json',
                                  'agent_path':'deliverables/mail-packet.json','role':'mail-packet',
                                  'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}]
        self.cma.artifact=lambda *args:raw

    def run_result(self):
        self.text=canonical(self.result)
        self.cma.response=lambda _: {'status':'completed','output':[{'type':'message','content':[
            {'type':'output_text','text':self.text}]}]}
        advance(self.ledger,self.cma,self.cfg,self.root)
        return self.ledger.tasks.get(self.row['id'])

    def test_expanded_readback_is_preserved_before_review_and_bound(self):
        row=self.run_result()
        self.assertEqual(row['state'],'awaiting-review')
        self.assertEqual(self.remote.calls,['preserve','readback'])
        self.assertEqual(row['data']['result']['packet'],self.packet)
        self.assertNotIn('agent_path',row['data']['result']['artifacts'][0])
        self.assertEqual(self.cma.retired,['agt_test'])
        binding=self.ledger.tasks.review_binding(row)
        self.assertEqual(binding['result_hash'],digest(row['data']['result']))
        unexpanded={**row['data']['result'],'packet':self.result['packet']}
        self.assertNotEqual(binding['result_hash'],digest(unexpanded))
        with patch('managed.daily_local.status',return_value={}):
            self.assertEqual(work(self.ledger.tasks,self.root)['reviews'][0]['result']['packet'],self.packet)
        # The original signed verdict cannot approve a changed attachment.
        env={'binding':binding,'reviewer':'local-claude-loop','verdict':'PASS',
             'reason':'Exact packet checked','evidence_ref':'fixture:review'}
        env['signature']=hmac.new(b'secret',canonical(env).encode(),hashlib.sha256).hexdigest()
        changed=copy.deepcopy(row['data']);changed['result']['packet']['attachments'][0]['content_b64']='YQ=='
        self.ledger.tasks.update(row['id'],row['revision'],'awaiting-review',changed)
        with self.assertRaisesRegex(ValueError,'obsolete'):
            self.ledger.tasks.verdict(env,'secret')

    def assert_repair(self,row):
        self.assertEqual(row['state'],'held')
        self.assertIn('needs repair',row['data']['reason'])
        self.assertEqual(row['data']['response_text'],self.text)
        self.assertEqual(row['data']['agent_id'],'agt_test')
        self.assertNotIn('result',row['data'])
        self.assertFalse(self.cma.retired)
        with patch('managed.daily_local.status',return_value={}):
            self.assertFalse(work(self.ledger.tasks,self.root)['reviews'])

    def test_mismatched_readback_retains_agent_for_repair(self):
        self.remote.corrupt=True
        row=self.run_result();self.assert_repair(row)
        self.assertIn('readback failed',row['data']['reason'])

    def test_invalid_json_retains_agent_for_repair(self):
        self.set_payload(b'{invalid JSON')
        self.assert_repair(self.run_result())

    def test_invalid_packet_retains_agent_for_repair(self):
        del self.packet['body'];self.set_payload(canonical(self.packet).encode())
        row=self.run_result();self.assert_repair(row)
        self.assertIn('letter fields',row['data']['reason'])

    def test_artifact_packets_keep_reply_identity_and_owner_checks(self):
        for field,value in [('thread_id','wrong-thread'),('latest_inbound_message_id','<wrong>'),
                            ('user_decision_ever',False)]:
            with self.subTest(field=field):
                packet={**self.packet,field:value}
                # Both transports use exactly the same post-expansion validator.
                result={**self.result,'packet':packet}
                with self.assertRaises(ValueError):validate_result(result,self.row,allow_packet_artifact=False)
        self.packet['thread_id']='wrong-thread';self.set_payload(canonical(self.packet).encode())
        self.assert_repair(self.run_result())

    def test_reference_must_select_same_result_mail_packet_and_fit_cap(self):
        for change in [lambda r:r['packet'].update(artifact='missing.json'),
                       lambda r:r['packet'].update(extra=True),
                       lambda r:r['artifacts'][0].update(role='evidence'),
                       lambda r:r['artifacts'][0].update(bytes=MAIL_PACKET_MAX_BYTES+1),
                       lambda r:r['artifacts'].append(copy.deepcopy(r['artifacts'][0])),
                       lambda r:r['artifacts'][0].update(release_tag='outreach-task-other'),
                       lambda r:r.update(packet=None)]:
            result=copy.deepcopy(self.result);change(result)
            with self.subTest(result=result['packet']),self.assertRaises(ValueError):
                validate_result(result,self.row)
        self.assertFalse(self.remote.calls)

    def test_existing_release_reference_and_auto_route_also_expand(self):
        artifact=self.result['artifacts'][0];artifact.pop('agent_path')
        self.remote.files[artifact['release_tag'],artifact['name']]=canonical(self.packet).encode()
        self.result['routing']['mail']='auto'
        row=self.run_result()
        self.assertEqual(row['state'],'awaiting-review')
        self.assertEqual(row['data']['result']['packet'],self.packet)
        self.assertEqual(self.remote.calls,['readback'])

    def firstcontact(self):
        req=request('firstcontact-1',kind='firstcontact')
        req['inputs']={'automation_contract':1,'recipient_emails':['author@example.invalid']}
        self.row=self.ledger.tasks.add(req)
        # These tests exercise packet readback after admission. Acquisition of
        # fresh history is covered by test_firstcontact_history.
        self.row=self.ledger.tasks.update(self.row['id'],self.row['revision'],'running',{
            'agent_id':'agt_test', 'response_id':'resp_original',
            'mailbox_history':{'fetched_at':'2026-11-02T00:00:00Z', 'complete':True,
                'addresses':['author@example.invalid'], 'messages':[], 'attempts':[],
                'recipient_addresses':{'author@example.invalid':['author@example.invalid']}}})
        self.cfg['kinds']={'firstcontact':True}
        self.result.update(task_id=self.row['id'])
        self.result['routing']['mail']='auto'
        for field in ['thread_id','latest_inbound_message_id','user_decision_ever']:
            self.packet.pop(field)
        self.packet['kind']='firstcontact'
        self.set_payload(canonical(self.packet).encode())
        self.result['artifacts'][0]['release_tag']='outreach-task-'+self.row['id']

    def test_firstcontact_artifact_uses_same_recipient_validation(self):
        self.firstcontact()
        with patch('managed.control.firstcontact_gate') as gate:
            row=self.run_result()
        self.assertTrue(gate.called)
        self.assertEqual(row['state'],'awaiting-review')
        self.assertEqual(row['data']['result']['packet'],self.packet)

    def test_firstcontact_recipient_mismatch_retains_agent_for_repair(self):
        self.packet['to']=['other@example.invalid'];self.firstcontact()
        with patch('managed.control.firstcontact_gate'):
            row=self.run_result()
        self.assert_repair(row)
        self.assertIn('preflighted recipients',row['data']['reason'])


if __name__=='__main__':unittest.main()
