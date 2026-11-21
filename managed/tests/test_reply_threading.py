"""Exchange reply threading and draft ownership, entirely offline."""
import base64
import copy
from email import policy
from email.parser import BytesParser
import json
from pathlib import Path
import unittest
from urllib.parse import parse_qs, unquote, urlsplit

from managed import mailbox
from managed.queue import GateError
from managed.transports import NotSent


def participant(value):
    return {'emailAddress':{'address':value}}


class DraftGraph:
    """Model Exchange choosing a conversation from Thread-Index, not Message-ID."""
    def __init__(self, packet):
        self.packet=packet
        self.index=base64.b64encode(b'\x01'+bytes.fromhex('01dC4a0000')+bytes(range(16))).decode()
        self.inbound={'id':'inbound','internetMessageId':packet['latest_inbound_message_id'],
                      'conversationId':packet['thread_id'],'isDraft':False,
                      'from':participant(packet['to'][0]),'toRecipients':[participant(packet['sender'])],
                      'ccRecipients':[],'subject':'Original topic','receivedDateTime':'2026-11-01T05:00:00Z',
                      'internetMessageHeaders':[{'name':'Thread-Index','value':self.index},
                                                {'name':'Thread-Topic','value':'Original topic'}]}
        self.drafts={};self.sent={};self.attachments={};self.mimes=[];self.raw_mimes=[];self.calls=[]
        self.force_wrong_conversation=False;self.send_failure=None;self.delete_failure=None

    def message(self, identity, key, wrong=False):
        p=self.packet
        return {'id':identity,'internetMessageId':'<'+identity+'>','conversationId':'wrong-thread' if wrong else p['thread_id'],
                'isDraft':True,'from':participant(p['sender']),
                'toRecipients':[participant(a) for a in p['to']],
                'ccRecipients':[participant(a) for a in p['cc']],
                'subject':p['subject'],'body':{'contentType':'text','content':p['body']},
                'internetMessageHeaders':[{'name':'X-Outreach-Attempt','value':key}]}

    def strand(self, key, wrong=True):
        draft=self.message('stranded',key,wrong)
        self.drafts[draft['id']]=draft
        self.attachments[draft['id']]=self.packet_attachments()
        return draft

    def packet_attachments(self):
        return [{'@odata.type':'#microsoft.graph.fileAttachment','name':a['name'],
                 'contentType':a['media_type'],'contentBytes':a['content_b64']} for a in self.packet['attachments']]

    def mime(self, path, content):
        assert path=='/me/messages'
        raw=base64.b64decode(content);self.raw_mimes.append(raw)
        mime=BytesParser(policy=policy.default).parsebytes(raw)
        self.mimes.append(mime)
        draft=self.message('draft-'+str(len(self.mimes)),str(mime['X-Outreach-Attempt']),
                           self.force_wrong_conversation or mime['Thread-Index'] is None)
        # Populate the actual content from MIME so assertions exercise matches().
        draft['subject']=str(mime['Subject'])
        draft['body']['content']=mime.get_body(preferencelist=('plain',)).get_content()
        self.attachments[draft['id']]=[
            {'@odata.type':'#microsoft.graph.fileAttachment','name':a.get_filename(),
             'contentType':a.get_content_type(),'contentBytes':base64.b64encode(a.get_payload(decode=True)).decode()}
            for a in mime.iter_attachments()]
        self.drafts[draft['id']]=draft
        return {'id':draft['id']}

    def request(self, path, method='GET', body=None):
        self.calls.append((path,method,body))
        if path.startswith('/me?'):return {'mail':self.packet['sender']}
        identity=unquote(urlsplit(path).path.split('/')[3])
        if method=='DELETE':
            if self.delete_failure:raise self.delete_failure
            del self.drafts[identity]
            return {}
        if path.endswith('/send'):
            if self.send_failure:raise self.send_failure
            message=self.drafts.pop(identity)
            self.sent[identity]={**message,'isDraft':False,'sentDateTime':'2026-11-01T06:00:00Z'}
            return {}
        if identity=='inbound':return copy.deepcopy(self.inbound)
        return copy.deepcopy(self.drafts.get(identity) or self.sent[identity])

    def pages(self, path):
        if '/attachments?' in path:
            return copy.deepcopy(self.attachments[unquote(urlsplit(path).path.split('/')[3])])
        if '/mailFolders/drafts/' in path:return copy.deepcopy(list(self.drafts.values()))
        if '/mailFolders/sentitems/' in path:return copy.deepcopy(list(self.sent.values()))
        query=parse_qs(urlsplit(path).query)
        assert query['$filter']==["conversationId eq '"+self.packet['thread_id']+"'"]
        return copy.deepcopy([self.inbound]+[m for m in [*self.drafts.values(),*self.sent.values()]
                                            if m['conversationId']==self.packet['thread_id']])


class ReplyThreadingTests(unittest.TestCase):
    def setUp(self):
        self.packet=json.loads((Path(__file__).parent/'fixtures/letter.json').read_text())
        self.packet.update(kind='reply',thread_id='thread',latest_inbound_message_id='<in>',user_decision_ever=False)
        self.graph=DraftGraph(self.packet)
        self.box=mailbox.GraphMailbox(self.graph,{'account':self.packet['sender'],'reconciled':True})

    def test_parent_headers_create_exact_reply_in_original_exchange_conversation(self):
        draft=self.box.create_draft(self.packet,'attempt')
        self.assertEqual(draft['conversationId'],self.packet['thread_id'])
        self.assertTrue(self.box.matches(draft,self.packet))
        mime=self.graph.mimes[0]
        parent=base64.b64decode(self.graph.index)
        child=base64.b64decode(str(mime['Thread-Index']))
        self.assertEqual(child[:-5],parent)
        self.assertEqual(len(child),len(parent)+5)
        self.assertEqual(mime['Thread-Topic'],'Original topic')
        self.assertEqual(mime['In-Reply-To'],'<in>')
        self.assertEqual(mime['References'],'<in>')
        self.assertFalse(any(c[0].endswith('/send') for c in self.graph.calls))

    def test_child_index_encodes_filetime_delta_both_scales_and_response_level(self):
        root_time=0x01dc4a0000 << 24
        parent=b'\x01'+(root_time >> 24).to_bytes(5,'big')+bytes(range(16))
        for seconds,shift,code in [(3600,18,0),(3*365*86400,23,1)]:
            with self.subTest(shift=shift):
                now=(root_time-116444736000000000)/10_000_000+seconds
                encoded=mailbox.child_thread_index(base64.b64encode(parent).decode(),now)
                child=base64.b64decode(encoded)
                delta=int(now*10_000_000)+116444736000000000-root_time
                self.assertEqual(int.from_bytes(child[-5:-1],'big'),(code << 31) | (delta >> shift))
                self.assertEqual(child[-1] & 15,0)
                grandchild=base64.b64decode(mailbox.child_thread_index(encoded,now+60))
                self.assertEqual(grandchild[:-5],child)
                self.assertEqual(grandchild[-1] & 15,1)

    def test_long_parent_index_stays_raw_base64_on_the_wire(self):
        parent=base64.b64decode(self.graph.index)+b'\x00'*50
        self.graph.inbound['internetMessageHeaders'][0]['value']=base64.b64encode(parent).decode()
        self.box.create_draft(self.packet,'long-thread')
        child=base64.b64decode(str(self.graph.mimes[0]['Thread-Index']))
        self.assertEqual(child[:-5],parent)
        self.assertIn(b'Thread-Index: '+base64.b64encode(child)+b'\r\n',self.graph.raw_mimes[0])

    def test_foreign_parent_index_is_omitted_and_missing_source_cannot_create_or_send(self):
        # Zimbra sends its own 22-byte Thread-Index (first byte not 0x01). The
        # reply then carries no Thread-Index and threads by In-Reply-To alone.
        zimbra='zRQNDRQHg2BE3SJF5EXcZdacVBVQ7A=='
        for index in ['invalid!',base64.b64encode(b'\x01'*23).decode(),zimbra]:
            with self.subTest(index=index):
                self.graph.mimes.clear();self.graph.raw_mimes.clear();self.graph.drafts.clear()
                self.graph.inbound['internetMessageHeaders'][0]['value']=index
                try:self.box.submit(self.packet,'attempt-'+str(len(index)))
                except NotSent as exc:self.assertNotIn('Thread-Index',str(exc))
                self.assertTrue(self.graph.mimes)
                mime=self.graph.mimes[0]
                self.assertIsNone(mime['Thread-Index']);self.assertIsNone(mime['Thread-Topic'])
                self.assertEqual(str(mime['In-Reply-To']),self.packet['latest_inbound_message_id'])
        self.graph.mimes.clear();self.graph.drafts.clear()
        self.graph.inbound['internetMessageId']='<other>'
        with self.assertRaisesRegex(NotSent,'missing or ambiguous'):self.box.submit(self.packet,'attempt')

    def test_cleanup_preserves_edited_unowned_or_sent_drafts(self):
        original=self.graph.strand('attempt')
        for change in [{'isDraft':False},{'body':{'contentType':'text','content':'owner edit'}},
                       {'from':participant('other@example.invalid')},
                       {'internetMessageHeaders':[{'name':'X-Outreach-Attempt','value':'attempt'}]*2}]:
            with self.subTest(change=change):
                self.graph.drafts['stranded']={**original,**change}
                with self.assertRaisesRegex(GateError,'preserve'):self.box.discard_draft(self.packet,'attempt')
                self.assertIn('stranded',self.graph.drafts)
        self.graph.drafts['stranded']={**original,'internetMessageHeaders':[]}
        self.box.discard_draft(self.packet,'attempt')
        self.assertIn('stranded',self.graph.drafts)
        self.assertFalse(any(c[1]=='DELETE' for c in self.graph.calls))

    def test_cleanup_refuses_multiple_correlated_copies_and_changed_attachments(self):
        self.graph.strand('attempt')
        self.graph.attachments['stranded'][0]['contentBytes']='Y2hhbmdlZA=='
        with self.assertRaisesRegex(GateError,'preserve'):self.box.discard_draft(self.packet,'attempt')
        self.graph.drafts['duplicate']=self.graph.message('duplicate','attempt',True)
        with self.assertRaisesRegex(GateError,'Multiple'):self.box.discard_draft(self.packet,'attempt')
        self.assertFalse(any(c[1]=='DELETE' for c in self.graph.calls))


if __name__=='__main__':unittest.main()
