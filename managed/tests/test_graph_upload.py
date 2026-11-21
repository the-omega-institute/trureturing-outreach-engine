import base64
from email import policy
from email.parser import BytesParser
import io
import json
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from managed import mailbox
from managed.queue import GateError, digest
from managed.transports import NotSent


MIB = 1024 * 1024
UPLOAD_URL = 'https://region.outlook.office.com/upload?authtoken=private-session'
STAMP = '2026-11-20T08:00:00Z'


def attachment(name, raw):
    return {'name': name, 'media_type': 'application/zip', 'content_b64': base64.b64encode(raw).decode()}


class FakeGraph(mailbox.Graph):
    def __init__(self):
        super().__init__('graph-bearer')
        self.url = UPLOAD_URL
        self.session = None
        self.calls = []
        self.http = []
        self.files = []
        self.data = bytearray()
        self.draft = None
        self.upload_fault = None
        self.inbound = {'id': 'source', 'internetMessageId': '<source@example.invalid>',
                        'isDraft': False, 'conversationId': 'thread', 'receivedDateTime': STAMP,
                        'from': {'emailAddress': {'address': 'author@example.invalid'}}}

    def request(self, path, method='GET', body=None, *, raw_response=False):
        path = path.split('?', 1)[0]
        self.calls.append((method, path))
        if method == 'POST' and path == '/me/messages':
            self.files = [{**a, 'id': 'small', 'size': len(base64.b64decode(a['contentBytes'])) + 161}
                          for a in body['attachments']]
            self.draft = {k: v for k, v in body.items() if k != 'attachments'}
            self.draft.update(id='new-draft', isDraft=True, createdDateTime=STAMP)
            return {'id': 'new-draft'}
        if method == 'POST' and path.endswith('/attachments/createUploadSession'):
            self.session = body
            return {'uploadUrl': self.url}
        if method == 'DELETE' and path == '/me/messages/new-draft':
            self.draft = None
            return {}
        if method == 'POST' and path == '/me/messages/new-draft/send':
            self.draft.update(isDraft=False, internetMessageId='<sent@example.invalid>', sentDateTime=STAMP)
            return {}
        if path == '/me':
            return {'mail': 'sender@example.invalid'}
        if path == '/me/messages/source':
            return self.inbound
        if path == '/me/messages/new-draft':
            return self.draft
        if path == '/me/messages/new-draft/attachments/large/$value' and raw_response:
            return bytes(self.data)
        if path == '/me/messages/new-draft/attachments/small':
            return self.files[0]
        raise AssertionError((method, path))

    def mime(self, path, content):
        parsed = BytesParser(policy=policy.default).parsebytes(base64.b64decode(content))
        body = {
            'subject': parsed['Subject'], 'body': {'contentType': 'text', 'content': parsed.get_body().get_content()},
            'from': {'emailAddress': {'address': str(parsed['From'])}},
            'toRecipients': [{'emailAddress': {'address': str(parsed['To'])}}], 'ccRecipients': [],
            'internetMessageHeaders': [{'name': 'In-Reply-To', 'value': str(parsed['In-Reply-To'])},
                                       {'name': 'X-Outreach-Attempt', 'value': str(parsed['X-Outreach-Attempt'])}],
            'conversationId': 'thread',
            'attachments': [{'@odata.type': mailbox.FILE_ATTACHMENT, 'name': a.get_filename(),
                             'contentType': a.get_content_type(),
                             'contentBytes': base64.b64encode(a.get_payload(decode=True)).decode()}
                            for a in parsed.iter_attachments()],
        }
        return self.request(path, 'POST', body)

    def pages(self, path):
        self.calls.append(('LIST', path))
        if path.startswith('/me/messages?'):
            return [self.inbound]
        if path.startswith('/me/mailFolders/sentitems/') and self.draft and not self.draft['isDraft']:
            return [self.draft]
        if '/attachments?' in path:
            if '$select=' in path:
                return [{k: v for k, v in a.items() if k != 'contentBytes'} for a in self.files]
            return self.files
        return []

    def open(self, request, *, timeout):
        self.http.append(request)
        method = request.get_method()
        if self.upload_fault and self.upload_fault(request):
            raise HTTPError(self.url, 503, self.url, None, None)
        if method == 'GET':
            return self.response(200, {'nextExpectedRanges': [f'{len(self.data)}-']})
        return self.accept_chunk(request)

    def accept_chunk(self, request):
        self.data.extend(request.data)
        item = self.session['AttachmentItem']
        if len(self.data) < item['size']:
            return self.response(200, {'nextExpectedRanges': [str(len(self.data))]})
        self.files.append({'@odata.type': mailbox.FILE_ATTACHMENT, 'id': 'large', 'name': item['name'],
                           'contentType': item['contentType'], 'size': len(self.data) + 161})  # Graph adds overhead
        return self.response(201)

    @staticmethod
    def response(status, body=None):
        response = io.BytesIO(json.dumps(body).encode())
        response.status = status
        return response


class GraphUploadTests(unittest.TestCase):
    def setUp(self):
        self.graph = FakeGraph()
        self.box = mailbox.GraphMailbox(self.graph, {'account': 'sender@example.invalid', 'reconciled': True})
        self.packet = {'kind': 'firstcontact', 'sender': 'sender@example.invalid',
                       'to': ['author@example.invalid'], 'cc': [], 'subject': 'Verification',
                       'body': 'Here are the verification files.', 'attachments': []}
        self.opener = self.enterContext(patch('managed.mailbox.build_opener'))
        self.opener.return_value.open.side_effect = self.graph.open
        self.enterContext(patch('managed.mailbox.time.sleep'))

    def large(self, raw=None):
        self.packet['attachments'].append(attachment('verification.zip', raw if raw is not None else b'L' * (5 * MIB)))

    def small(self):
        self.packet['attachments'].append(attachment('small.zip', b'small attachment'))

    def test_five_mib_upload_and_draft_and_sent_receipts(self):
        self.large(b'a' * (4 * MIB) + b'b' * MIB)
        self.box.preflight(self.packet, {})
        draft_receipt = self.box.save_draft(self.packet, 'key')
        self.assertEqual(draft_receipt['content_hash'], digest(self.packet))
        self.assertEqual(draft_receipt['remote_id'], 'new-draft')
        self.assertEqual(self.graph.session, {'AttachmentItem': {
            'attachmentType': 'file', 'name': 'verification.zip', 'size': 5 * MIB,
            'contentType': 'application/zip'}})
        self.assertEqual([r.get_header('Content-range') for r in self.graph.http],
                         ['bytes 0-3932159/5242880', 'bytes 3932160-5242879/5242880'])
        self.assertEqual(bytes(self.graph.data), base64.b64decode(self.packet['attachments'][0]['content_b64']))
        for request, opener in zip(self.graph.http, self.opener.call_args_list, strict=True):
            self.assertEqual(request.full_url, UPLOAD_URL)
            self.assertEqual(request.get_method(), 'PUT')
            self.assertIsNone(request.get_header('Authorization'))
            self.assertEqual(request.get_header('Content-length'), str(len(request.data)))
            self.assertIsInstance(opener.args[0], mailbox.NoRedirect)
        self.graph.request('/me/messages/new-draft/send', 'POST')
        sent_receipt = self.box.lookup(self.packet, 'key')
        self.assertEqual(sent_receipt.content_hash, draft_receipt['content_hash'])
        self.assertEqual(sent_receipt.remote_id, 'new-draft')
        self.graph.data[0] ^= 1
        with self.assertRaisesRegex(GateError, 'Actual Sent differs'):
            self.box.lookup(self.packet, 'key')

    def test_mixed_reply_uses_mime_and_session_before_send(self):
        self.packet.update(kind='reply', thread_id='thread', latest_inbound_message_id=self.graph.inbound['internetMessageId'])
        self.small()
        self.large(b'L' * (3 * MIB))
        self.box.submit(self.packet, 'key')
        self.assertEqual(self.graph.files[0]['contentBytes'], self.packet['attachments'][0]['content_b64'])
        self.assertEqual(self.graph.files[1]['size'], 3 * MIB + 161)
        self.assertEqual(self.graph.draft['internetMessageHeaders'][0]['value'], self.packet['latest_inbound_message_id'])
        self.assertEqual(self.graph.calls[-2:], [('GET', '/me/messages/new-draft/attachments/small'),
                                                 ('POST', '/me/messages/new-draft/send')])

    def test_lost_chunk_response_resumes_from_server_offset(self):
        self.large()

        def lose_first_response(request):
            if len(self.graph.http) == 1:
                self.graph.accept_chunk(request).close()
                return True
            return False

        self.graph.upload_fault = lose_first_response
        self.box.create_draft(self.packet, 'key')
        self.assertEqual([r.get_method() for r in self.graph.http], ['PUT', 'GET', 'PUT'])
        self.assertIsNone(self.graph.http[1].get_header('Authorization'))
        self.assertEqual(self.graph.http[2].get_header('Content-range'), 'bytes 3932160-5242879/5242880')

    def test_lost_final_response_with_closed_session_uses_draft_readback(self):
        self.large(b'L' * (3 * MIB))

        def lose_final_response(request):
            if request.get_method() == 'PUT':
                self.graph.accept_chunk(request).close()
                return True
            raise HTTPError(self.graph.url, 404, 'Session closed', None, None)

        self.graph.upload_fault = lose_final_response
        draft = self.box.create_draft(self.packet, 'key')
        self.assertEqual(draft['id'], 'new-draft')
        self.assertEqual([r.get_method() for r in self.graph.http], ['PUT', 'GET'])

    def test_permanent_failure_retries_and_deletes_only_new_draft(self):
        self.large()
        self.graph.upload_fault = lambda r: r.get_method() == 'PUT'
        with self.assertRaisesRegex(NotSent, r'Graph attachment upload PUT failed \(HTTP 503\)') as error:
            self.box.submit(self.packet, 'key')
        self.assertNotIn(UPLOAD_URL, str(error.exception.__cause__))
        self.assertEqual([r.get_method() for r in self.graph.http], ['PUT', 'GET', 'PUT', 'GET', 'PUT'])
        self.assertEqual([c for c in self.graph.calls if c[0] == 'DELETE'], [('DELETE', '/me/messages/new-draft')])
        self.assertIsNone(self.graph.draft)

    def test_untrusted_url_is_rejected_without_a_network_request(self):
        self.large()
        for url in ('http://outlook.office.com/session', 'https://outlook.office.com.sample.example.invalid/session',
                    'https://fakeoutlook.office.com/session'):
            with self.subTest(url=url):
                self.graph.url = url
                with self.assertRaisesRegex(GateError, 'HTTPS on an allowed Microsoft host'):
                    self.box.create_draft(self.packet, 'key')
                self.opener.assert_not_called()
                self.assertIsNone(self.graph.draft)

    def test_limit_counts_combined_decoded_bytes(self):
        self.small()
        self.large()
        self.box.settings['max_total_attachment_bytes'] = 5 * MIB
        with self.assertRaisesRegex(GateError, '5242896 bytes exceeds the mailbox limit of 5242880 bytes'):
            self.box.preflight(self.packet, {})
        self.assertEqual(self.graph.calls, [('GET', '/me')])
        self.box.settings['max_total_attachment_bytes'] += len(b'small attachment')
        self.box.preflight(self.packet, {})

    def test_small_attachment_stays_inline_and_is_checked_byte_for_byte(self):
        self.small()
        draft = self.box.create_draft(self.packet, 'key')
        self.opener.assert_not_called()
        self.assertEqual(self.graph.files[0]['contentBytes'], self.packet['attachments'][0]['content_b64'])
        self.graph.files[0]['contentBytes'] = base64.b64encode(b'X' * len(b'small attachment')).decode()
        self.assertFalse(self.box.matches(draft, self.packet))

    def test_large_readback_size_mismatch_prevents_send_and_removes_draft(self):
        self.large()
        pages = self.graph.pages

        def wrong_size(path):
            rows = pages(path)
            if '/attachments?' in path:
                rows[0]['size'] -= 1000
            return rows

        with patch.object(self.graph, 'pages', side_effect=wrong_size), self.assertRaisesRegex(NotSent, 'differs'):
            self.box.submit(self.packet, 'key')
        self.assertIsNone(self.graph.draft)
        self.assertNotIn(('POST', '/me/messages/new-draft/send'), self.graph.calls)
