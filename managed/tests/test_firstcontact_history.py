"""Fresh coordinator evidence, private snapshots and cold-contact routing."""
import copy
import hashlib
from html import escape
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlencode, urlsplit

from fixture_config import use_example_config
from managed import automation, control, firstcontact_history, mailbox
from managed.queue import Queue, GateError
from managed.thread_staging import effective_inputs
from test_tasks import CMA, Releases, request


def addr(value):
    return {'emailAddress': {'address': value}}


class HistoryGraph:
    def __init__(self):
        self.rows = {f: [] for f in ('inbox', 'sentitems', 'drafts')}
        self.search_rows = {}
        self.details = {}
        self.paths = []
        self.error = None
        self.scan_error = None

    def request(self, path):
        if path.startswith('/me/messages/'):
            return copy.deepcopy(self.details[urlsplit(path).path.rsplit('/', 1)[-1]])
        return {'mail': 'sender@example.invalid'}

    def pages(self, path):
        self.paths.append(path)
        if self.error:
            raise self.error
        folder = path.split('/')[3]
        query = parse_qs(urlsplit(path).query)
        if '$search' in query:
            return copy.deepcopy(self.search_rows.get(folder, self.rows[folder]))
        if self.scan_error:
            raise self.scan_error
        return copy.deepcopy(self.rows[folder])


class FirstcontactHistoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.root = Path(self.tmp.name)
        (self.root/'tables').mkdir(); self.folder = self.root/'ledger'; self.folder.mkdir()
        contacts = [{'id': 'contact-a', 'name': 'Author A', 'email': 'a@example.invalid',
                     'policy_fresh': False, 'relationship': {'aliases': ['alias@example.invalid']}},
                    {'id': 'contact-b', 'name': 'Author B', 'email': 'b@example.invalid', 'policy_fresh': False}]
        for table, value in [('contacts', {'contacts': contacts}), ('refusals', {'refusals': []})]:
            (self.root/'tables'/(table+'.json')).write_text(json.dumps(value))
        self.ledger = control.Ledger(Releases(), self.folder); self.cma = CMA()
        self.graph = HistoryGraph()
        self.box = mailbox.GraphMailbox(self.graph, {'account': 'sender@example.invalid', 'reconciled': True})
        self.req = request('first-history', 'contact-a', 'firstcontact')
        self.req['inputs'] = {'automation_contract': 1, 'recipient_emails': ['a@example.invalid'],
                              'to': ['a@example.invalid'], 'cc': []}
        self.cfg = {'paused': False, 'kinds': {'firstcontact': True}, 'max_active': 2, 'agent_template': {}}

    def tearDown(self):
        self.ledger.tasks.close(); self.tmp.cleanup()

    def tick(self, add=False):
        if add:
            self.ledger.tasks.add(self.req)
        with patch.object(control, 'ROOT', self.root), patch.object(control, 'workflow', return_value=self.req['workflow_hash']):
            control.advance(self.ledger, self.cma, self.cfg, self.folder, mailbox=self.box)
        return self.ledger.tasks.get(self.req['id'])

    def retry_when_due(self):
        next_at = self.ledger.tasks.get(self.req['id'])['data']['mailbox_retry']['next_at']
        self.ledger.tasks.clock = lambda: next_at
        return self.tick()

    def message(self, folder='inbox', recipient='alias@example.invalid'):
        return {'id': 'graph-1', 'internetMessageId': '<prior@example.invalid>',
                'conversationId': 'existing-thread', 'subject': 'Prior research',
                'from': addr(recipient if folder=='inbox' else 'sender@example.invalid'),
                'toRecipients': [addr('sender@example.invalid' if folder=='inbox' else recipient)],
                'ccRecipients': [], 'isDraft': folder=='drafts',
                'receivedDateTime': '2026-11-01T01:00:00Z', 'bodyPreview': 'Preview '*100,
                'body': {'contentType': 'text', 'content': 'PRIVATE FULL BODY'}}

    def result(self, recipients=None):
        packet = json.loads((Path(__file__).parent/'fixtures/letter.json').read_text())
        packet.update(to=recipients or self.req['inputs']['to'], cc=[])
        return {'schema_version': 1, 'task_id': self.req['id'], 'outcome': 'completed',
                'report': 'Checked mathematics and recipients', 'packet': packet, 'artifacts': [],
                'routing': {'mail': 'auto', 'questions': [], 'independent_submission': None}}

    def folder_requests(self, folder, search=False):
        return [path for path in self.graph.paths if path.split('/')[3] == folder
                and ('$search' in parse_qs(urlsplit(path).query)) == search]

    def add_second_task(self):
        second = copy.deepcopy(self.req)
        second.update(id='second-history', owner='contact-b')
        second['inputs'].update(recipient_emails=['b@example.invalid'], to=['b@example.invalid'])
        self.ledger.tasks.add(second)
        return second['id']

    def test_complete_history_checkpointed_before_create_and_admission(self):
        original_create = self.cma.create
        def create(body, key):
            stored = self.ledger.tasks.get(self.req['id'])
            self.assertTrue(stored['data']['mailbox_history']['complete'])
            self.assertEqual(stored['request'], self.req)
            self.assertEqual(len(self.graph.paths), 6)  # primary and verified alias, all three folders
            return original_create(body, key)
        self.cma.create = create
        row = self.tick(add=True)
        self.assertEqual(row['state'], 'running')
        history = row['data']['mailbox_history']
        self.assertEqual(history['addresses'], ['a@example.invalid', 'alias@example.invalid'])
        self.assertTrue(history['fetched_at']); self.assertEqual(history['messages'], [])
        self.assertEqual(history['attempts'], [])
        prompt = next(iter(self.cma.commands.values()))['prompt']
        self.assertIn('IS the fresh recipient-specific mailbox evidence', prompt)
        self.assertIn('policy_fresh=false', prompt)
        self.tick(); self.assertEqual(len(self.graph.paths), 6)  # no re-read after admission
        row = self.ledger.tasks.update(row['id'], row['revision'], 'awaiting-review',
                                      {**row['data'], 'result': self.result()})
        self.assertEqual(self.ledger.tasks.reviews()[0]['mailbox_history'], history)

    def test_reply_history_is_attached_before_admission_across_all_threads(self):
        req = request('reply-history', 'contact-a', 'reply')
        counterpart = 'a@example.invalid'
        req['inputs'] = {'automation_contract': 1, 'thread': [{
            'id': 'in-current', 'internetMessageId': '<in-current@example.invalid>',
            'conversationId': 'current-thread', 'subject': 'Current topic',
            'from': addr(counterpart), 'toRecipients': [addr('sender@example.invalid')],
            'ccRecipients': [], 'isDraft': False,
            'receivedDateTime': '2026-11-02T01:00:00Z'}]}
        self.ledger.tasks.add(req)
        def message(identity, conversation, subject, sender, recipient, stamp, draft=False, body=''):
            return {'id': identity, 'internetMessageId': '<'+identity+'@example.invalid>',
                    'conversationId': conversation, 'subject': subject, 'from': addr(sender),
                    'toRecipients': [addr(recipient)], 'ccRecipients': [], 'isDraft': draft,
                    'sentDateTime': stamp if sender == 'sender@example.invalid' else None,
                    'receivedDateTime': None if sender == 'sender@example.invalid' else stamp,
                    'body': {'contentType': 'text', 'content': body},
                    'bodyPreview': body[:300]}
        self.graph.rows['inbox'] = [message('in-1', 'current-thread', 'Current topic', counterpart,
                                             'sender@example.invalid', '2026-11-02T01:00:00Z', body='Please check this.'),
                                     message('in-2', 'other-thread', 'Another topic', counterpart,
                                             'sender@example.invalid', '2026-11-02T04:00:00Z', body='A new question after your answer.')]
        self.graph.rows['sentitems'] = [
            message('sent-old', 'current-thread', 'Current topic', 'sender@example.invalid', counterpart,
                    '2026-10-30T01:00:00Z', body='Dear Professor A,\n\nOlder answer.\n\nBest,\nAvery'),
            message('sent-new', 'other-thread', 'Other topic', 'sender@example.invalid', counterpart,
                    '2026-11-02T02:00:00Z',
                    body='Dear Professor A,\n\nNewest answer. ' + 'Details. ' * 180
                    + '\n\nSincerely,\n\nAvery and Blaire\n\nOn Friday, A wrote:\n> quoted'),
            message('sent-alias', 'alias-thread', 'Alias topic', 'sender@example.invalid',
                    'alias@example.invalid', '2026-11-01T02:00:00Z',
                    body='Dear Dr. A,\n\nAn answer.\n\nBest,\nAvery')]
        sent = self.graph.rows['sentitems'][1]
        self.graph.details['sent-new'] = {'id': sent['id'], 'body': sent.pop('body')}
        self.graph.rows['drafts'] = [message('draft-1', 'old-thread', 'Old topic',
                                              'sender@example.invalid', counterpart,
                                              '2026-11-02T03:00:00Z', draft=True,
                                              body='Dear Professor A,\n\nSaved owner draft.')]
        draft = self.graph.rows['drafts'][0]
        draft_body = draft.pop('body')
        self.graph.details['draft-1'] = {'id': 'draft-1', 'body': draft_body,
                                         'lastModifiedDateTime': '2026-11-02T03:05:00Z'}
        self.graph.search_rows['drafts'] = []  # Search has not indexed this draft.
        self.req = req
        self.cfg['kinds']['reply'] = True
        with patch.object(control, 'ROOT', self.root), patch.object(control, 'workflow', return_value=req['workflow_hash']):
            with patch.object(self.cma, 'create', wraps=self.cma.create) as create:
                row = self.tick()
                self.assertEqual(row['state'], 'running')
                self.assertTrue(row['data']['mailbox_history']['complete'])
                create.assert_called_once()
        history = row['data']['mailbox_history']
        self.assertEqual(history['addresses'], [counterpart, 'alias@example.invalid'])
        self.assertEqual([m['id'] for m in history['messages']],
                         ['in-2', 'draft-1', 'sent-new', 'in-1', 'sent-alias', 'sent-old'])
        # Incoming after our latest Sent carries its text; earlier incoming does not.
        self.assertIn('new question', next(m for m in history['messages'] if m['id'] == 'in-2')['body_excerpt'])
        self.assertNotIn('body_excerpt', next(m for m in history['messages'] if m['id'] == 'in-1'))
        self.assertLessEqual(len(history['messages']), 12)
        sent_excerpt = next(m for m in history['messages'] if m['id'] == 'sent-new')['body_excerpt']
        self.assertIn('Newest answer', sent_excerpt)
        self.assertNotIn('Sincerely', sent_excerpt)
        self.assertIn('Saved owner draft', next(m for m in history['messages'] if m['id'] == 'draft-1')['body_excerpt'])
        self.assertNotIn('body_excerpt', next(m for m in history['messages'] if m['id'] == 'sent-old'))
        self.assertEqual(history['latest_sent'][counterpart], {
            'internetMessageId': '<sent-new@example.invalid>', 'date': '2026-11-02T02:00:00Z',
            'subject': 'Other topic', 'to': [counterpart], 'cc': [],
            'salutation': 'Dear Professor A,', 'closing': 'Sincerely,\n\nAvery and Blaire',
            'has_affiliation_lines': False})
        self.assertEqual(history['latest_sent']['alias@example.invalid']['internetMessageId'],
                         '<sent-alias@example.invalid>')
        self.assertTrue(history['drafts_complete'])
        self.assertEqual(history['drafts'], [{
            'id': 'draft-1', 'internetMessageId': '<draft-1@example.invalid>',
            'lastModifiedDateTime': '2026-11-02T03:05:00Z', 'subject': 'Old topic',
            'to': [counterpart], 'body_sha256': hashlib.sha256(draft_body['content'].encode()).hexdigest(),
            'body_excerpt': draft_body['content']}])
        self.assertEqual(len(self.folder_requests('drafts')), 1)
        self.assertEqual(self.folder_requests('drafts', search=True), [])
        prompt = next(iter(self.cma.commands.values()))['prompt']
        self.assertIn('Read inputs/data mailbox_history across all threads with this correspondent', prompt)
        self.assertIn('COORDINATOR MAILBOX HISTORY ACROSS CORRESPONDENT THREADS', prompt)

    def test_reply_draft_listing_is_complete_beyond_message_limit_and_can_confirm_empty(self):
        counterpart = 'a@example.invalid'
        req = request('reply-history', 'contact-a', 'reply')
        req['inputs'] = {'thread': [self.message(recipient=counterpart)]}
        row = {'kind': 'reply', 'request': req, 'data': {}}
        self.graph.rows['inbox'] = [
            {**self.message(recipient=counterpart), 'id': f'in-{i}',
             'receivedDateTime': '2026-11-02T01:00:00Z'} for i in range(12)]
        body = '新' * 201
        old = {**self.message('drafts', 'unrelated@example.invalid'), 'id': 'cc-draft',
               'from': None, 'ccRecipients': [addr(counterpart)],
               'body': {'contentType': 'text', 'content': body}}
        bcc = {**old, 'id': 'bcc-draft', 'ccRecipients': [], 'bccRecipients': [addr(counterpart)]}
        self.graph.rows['drafts'] = [
            old, bcc, {**old, 'id': 'sent-copy', 'isDraft': False},
            {**old, 'id': 'unrelated-draft', 'ccRecipients': []},
            {**old, 'id': 'from-only', 'from': addr(counterpart), 'ccRecipients': []}]

        def resolve():
            history = firstcontact_history.snapshot(self.ledger.tasks.clock)
            firstcontact_history.resolve_reply(row, self.box, self.ledger, self.root, history)
            return history

        history = resolve()
        self.assertTrue(history['drafts_complete'])
        self.assertEqual(len(history['messages']), 12)
        self.assertTrue(all(message['id'].startswith('in-') for message in history['messages']))
        self.assertEqual({draft['id'] for draft in history['drafts']}, {'cc-draft', 'bcc-draft'})
        self.assertTrue(all(draft['body_excerpt'] == body[:200] for draft in history['drafts']))
        self.assertEqual(history['latest_sent'], {})
        resolve()
        self.assertEqual(len(self.folder_requests('drafts')), 1)
        self.graph.rows['drafts'] = []
        self.box.begin_history_tick()
        history = resolve()
        self.assertTrue(history['drafts_complete'])
        self.assertEqual(history['drafts'], [])
        self.assertEqual(len(self.folder_requests('drafts')), 2)
        fresh = {**old, 'from': addr('sender@example.invalid'), 'id': 'fresh-draft'}
        self.graph.rows['drafts'] = [fresh]
        self.assertEqual([draft['id'] for draft in self.box.drafts_for(counterpart)], ['fresh-draft'])

    def test_failed_drafts_listing_does_not_confirm_empty(self):
        req = request('reply-history', 'contact-a', 'reply')
        req['inputs'] = {'thread': [self.message()]}
        row = {'kind': 'reply', 'request': req, 'data': {}}
        self.graph.scan_error = TimeoutError('Drafts page unavailable')
        history = firstcontact_history.snapshot(self.ledger.tasks.clock)
        with self.assertRaisesRegex(TimeoutError, 'Drafts page unavailable'):
            firstcontact_history.resolve_reply(row, self.box, self.ledger, self.root, history)
        self.assertFalse(history['drafts_complete'])
        self.assertFalse(history['complete'])

    def test_all_folder_searches_check_from_to_cc_locally_without_bodies(self):
        for folder in self.graph.rows:
            matched = self.message(folder)
            if folder == 'drafts':
                matched.update(toRecipients=[], ccRecipients=[addr('alias@example.invalid')])
            self.graph.rows[folder] = [matched, self.message(folder, 'unrelated@example.invalid')]
        rows = self.box.recipient_history('  ALIAS@EXAMPLE.INVALID  ')
        self.assertEqual([row['folder'] for row in rows], ['inbox', 'sentitems', 'drafts'])
        self.assertEqual(len(self.graph.paths), 3)
        for path in self.graph.paths:
            query = parse_qs(urlsplit(path).query)
            self.assertEqual(query['$search'], ['"participants:alias@example.invalid"'])
            self.assertEqual(query['$top'], ['100'])
            self.assertNotIn('$filter', query)
            fields = query['$select'][0].split(',')
            self.assertIn('bodyPreview', fields); self.assertNotIn('body', fields)
            self.assertTrue({'from', 'toRecipients', 'ccRecipients'} <= set(fields))

    def test_searches_below_cap_are_fresh_for_each_call(self):
        self.assertEqual(self.box.recipient_history('alias@example.invalid'), [])
        self.graph.rows['inbox'] = [self.message()]
        self.assertEqual(len(self.box.recipient_history('alias@example.invalid')), 1)
        self.assertEqual(len(self.graph.paths), 6)
        self.assertFalse(self.folder_requests('inbox'))

    def test_search_cap_counts_all_rows_before_local_matching(self):
        unrelated = self.message(recipient='unrelated@example.invalid')
        self.graph.rows['inbox'] = [unrelated, self.message()]
        for count in (999, 1000, 1001):
            with self.subTest(count=count):
                self.box.begin_history_tick(); self.graph.paths.clear()
                self.graph.search_rows['inbox'] = [unrelated] * count
                rows = self.box.recipient_history('alias@example.invalid')
                self.assertEqual(len(rows), int(count >= 1000))
                self.assertEqual(len(self.folder_requests('inbox')), int(count >= 1000))

    def test_capped_folder_scans_are_shared_across_recipients_and_coordinator_tasks(self):
        second_id = self.add_second_task()
        for folder in self.graph.rows:
            unrelated = self.message(folder, 'unrelated@example.invalid')
            self.graph.search_rows[folder] = [unrelated] * 1000
            self.graph.rows[folder] = [unrelated, self.message(folder),
                                      {**self.message(folder, 'b@example.invalid'), 'id': 'graph-b'}]
        # Delivery's caches must not substitute for fresh history metadata.
        self.box.inbox = []; self.box.sent = []
        row = self.tick(add=True)
        second = self.ledger.tasks.get(second_id)
        for current, recipient in ((row, 'alias@example.invalid'), (second, 'b@example.invalid')):
            self.assertEqual(current['state'], 'running')
            history = current['data']['mailbox_history']
            self.assertTrue(history['complete'])
            self.assertEqual([m['addresses'] for m in history['messages']], [[recipient]])
        for folder in self.graph.rows:
            self.assertEqual(len(self.folder_requests(folder)), 1)
            self.assertEqual(len(self.folder_requests(folder, search=True)), 1)

    def test_failed_capped_scan_is_shared_then_retried_fresh_next_coordinator_tick(self):
        second_id = self.add_second_task()
        self.graph.search_rows['inbox'] = [self.message()] * 1000
        self.graph.scan_error = HTTPError('https://graph.microsoft.com', 503, 'Unavailable', {}, None)
        self.addCleanup(self.graph.scan_error.close)
        self.tick(add=True)
        for task_id in (self.req['id'], second_id):
            row = self.ledger.tasks.get(task_id)
            self.assertEqual(row['state'], 'queued')
            self.assertFalse(row['data']['mailbox_history']['complete'])
            self.assertEqual(row['data']['mailbox_history_retries'], 1)
        self.assertEqual(len(self.graph.paths), 2)  # one search and one failed full scan
        self.assertFalse(self.cma.creates)
        self.graph.scan_error = None
        self.graph.rows['inbox'] = [self.message()]
        self.retry_when_due()
        for task_id in (self.req['id'], second_id):
            row = self.ledger.tasks.get(task_id)
            self.assertEqual(row['state'], 'running')
            self.assertTrue(row['data']['mailbox_history']['complete'])
        self.assertEqual(len(self.folder_requests('inbox')), 2)  # once in each tick
        self.assertEqual(len(self.folder_requests('inbox', search=True)), 2)
        self.assertEqual(len(self.ledger.tasks.get(self.req['id'])['data']['mailbox_history']['messages']), 1)

    def test_successful_capped_scan_expires_on_next_coordinator_tick(self):
        self.graph.search_rows['inbox'] = [self.message()] * 1000
        self.tick(add=True)
        self.add_second_task()
        self.graph.rows['inbox'] = [self.message(recipient='b@example.invalid')]
        self.tick()
        history = self.ledger.tasks.get('second-history')['data']['mailbox_history']
        self.assertTrue(history['complete'])
        self.assertEqual(history['messages'][0]['addresses'], ['b@example.invalid'])
        self.assertEqual(len(self.folder_requests('inbox')), 2)

    def test_prior_mail_on_alias_routes_to_existing_thread_and_forbids_cold_packet(self):
        self.graph.rows['inbox'] = [self.message()]
        row = self.tick(add=True)
        inputs = effective_inputs(row)
        self.assertEqual(inputs['recipient_emails'], [])
        self.assertEqual(inputs['excluded_recipients'][0]['thread_ids'], ['existing-thread'])
        history = row['data']['mailbox_history']
        self.assertEqual(len(history['messages']), 1)  # shared-address query results deduplicate
        self.assertEqual(history['messages'][0]['direction'], 'incoming')
        self.assertEqual(len(history['messages'][0]['preview']), 300)
        self.assertNotIn('PRIVATE FULL BODY', json.dumps(history))
        with patch.object(control, 'ROOT', self.root), self.assertRaises(GateError):
            control.validate_result(self.result(), row)
        result = self.result(); result.update(outcome='hold', packet=None,
            report='Continue as a reply on existing-thread; stage its complete conversation.')
        result['routing']['mail'] = 'none'
        self.cma.response = lambda _: {'status': 'completed', 'output': [{'type': 'message', 'content': [
            {'type': 'output_text', 'text': json.dumps(result)}]}]}
        row = self.tick()
        self.assertEqual(row['state'], 'held')
        self.assertEqual(row['data']['result']['routing']['mail'], 'none')
        self.assertIn('existing-thread', row['data']['result']['report'])

    def test_prior_mail_excludes_one_author_and_recomputes_to_cc(self):
        self.req['inputs'].update(recipient_emails=['a@example.invalid', 'b@example.invalid'],
                                  cc=['b@example.invalid'], owner_ids=['contact-b'])
        self.graph.rows['drafts'] = [self.message('drafts')]
        row = self.tick(add=True)
        inputs = effective_inputs(row)
        self.assertEqual(inputs['to'], ['b@example.invalid']); self.assertEqual(inputs['cc'], [])
        self.assertTrue(row['data']['mailbox_history']['messages'][0]['isDraft'])
        with patch.object(control, 'ROOT', self.root):
            control.validate_result(self.result(['b@example.invalid']), row)
            with self.assertRaises(GateError):
                control.validate_result(self.result(), row)

    def test_delivery_queue_uncertain_attempt_is_evidence_even_without_graph_mail(self):
        queue = Queue(self.ledger.tasks.path, {'firstcontact': self.req['workflow_hash']})
        try:
            packet = self.result(['alias@example.invalid'])['packet']
            queue.add('prior-letter', packet)
            queue.db.execute("INSERT INTO attempts VALUES(?,?,?,?,?,?,?,?,?)",
                             ('prior-letter', 'prior-attempt', 'microsoft-graph', 'send-unknown', 1, 1, 'authority', '', ''))
            queue._event('prior-letter', 'send-intent', {})
        finally:
            queue.close()
        row = self.tick(add=True)
        attempt = row['data']['mailbox_history']['attempts'][0]
        self.assertEqual(attempt['status'], 'send-unknown')
        self.assertEqual(attempt['addresses'], ['alias@example.invalid'])
        self.assertEqual(effective_inputs(row)['excluded_recipients'][0]['attempt_keys'], ['prior-attempt'])
        self.assertFalse(effective_inputs(row)['recipient_emails'])

    def test_transient_errors_retry_then_recover_before_any_cma_claim(self):
        self.graph.error = HTTPError('https://graph.microsoft.com', 503, 'Unavailable', {}, None)
        self.addCleanup(self.graph.error.close)
        row = self.tick(add=True)
        self.assertEqual(row['state'], 'queued'); self.assertFalse(self.cma.creates)
        self.assertFalse(row['data']['mailbox_history']['complete'])
        self.assertEqual(row['data']['mailbox_history_retries'], 1)
        self.assertEqual(len(self.graph.paths), 1)  # search failure does not trigger a full scan
        self.assertIn('$search', parse_qs(urlsplit(self.graph.paths[0]).query))
        self.graph.error = None
        row = self.retry_when_due()
        self.assertEqual(row['state'], 'running'); self.assertEqual(len(self.cma.creates), 1)
        self.assertTrue(row['data']['mailbox_history']['complete'])

    def test_legacy_reply_hold_gets_one_bounded_backoff_window(self):
        self.cfg['kinds']['reply'] = True
        self.req = request('legacy-reply', 'contact-a', 'reply')
        self.req['inputs'] = {'thread': [self.message()]}
        row = self.ledger.tasks.add(self.req)
        self.ledger.tasks.update(row['id'], row['revision'], 'held', {
            'reason': 'Reply mailbox history failed: HTTPError: HTTP Error 502: Synthetic outage',
            'mailbox_history_retries': control.RESOLUTION_RETRIES})
        with patch('managed.firstcontact_history.resolve_reply',
                   side_effect=ConnectionResetError('Synthetic reset')) as failed:
            row = self.tick()
            self.assertEqual(row['state'], 'queued')
            self.assertEqual(row['data']['mailbox_history_retries'], 1)
            self.tick()
            failed.assert_called_once()
            for _ in range(control.RESOLUTION_RETRIES):
                row = self.retry_when_due()
            self.assertEqual(row['state'], 'held')
            self.assertEqual(row['data']['mailbox_retry']['status'], 'exhausted')
            self.tick()
            self.assertEqual(failed.call_count, control.RESOLUTION_RETRIES + 1)
        work = automation.work(self.ledger.tasks, self.root)
        self.assertEqual(work['mailbox_retry_exhausted'][0]['task_id'], row['id'])
        self.assertFalse(self.cma.creates)

    def test_transient_exhaustion_and_permanent_error_hold_with_reason(self):
        for code in (429, 403):
            with self.subTest(code=code):
                self.req['id'] = 'failure-'+str(code)
                self.graph.error = HTTPError('https://graph.microsoft.com', code, 'Graph failure', {}, None)
                self.addCleanup(self.graph.error.close)
                row = self.tick(add=True)
                for _ in range(control.RESOLUTION_RETRIES):
                    if row['state'] == 'queued':
                        row = self.retry_when_due()
                self.assertEqual(row['state'], 'held'); self.assertFalse(self.cma.creates)
                self.assertIn('HTTPError:', row['data']['reason'])
                if code == 429:
                    self.assertEqual(row['data']['mailbox_retry']['status'], 'exhausted')
                else:
                    self.assertNotIn('mailbox_retry', row['data'])
                self.assertFalse(row['data']['mailbox_history']['complete'])

    def test_configured_adapter_failure_records_incomplete_snapshot(self):
        self.ledger.tasks.add(self.req)
        with patch.object(control, 'ROOT', self.root), patch.object(control, 'workflow', return_value=self.req['workflow_hash']), \
                patch('managed.mailbox.configured', side_effect=GateError('No configured mailbox')):
            control.advance(self.ledger, self.cma, self.cfg, self.folder)
        row = self.ledger.tasks.get(self.req['id'])
        self.assertEqual(row['state'], 'held'); self.assertFalse(row['data']['mailbox_history']['complete'])
        self.assertFalse(self.cma.creates)

    def test_partial_alias_scan_retries_without_admitting_partial_evidence(self):
        primary = self.message('sentitems', 'a@example.invalid')
        with patch.object(self.box, 'recipient_history', side_effect=[[primary], TimeoutError('Alias scan failed')]):
            row = self.tick(add=True)
        self.assertEqual(row['state'], 'queued'); self.assertFalse(self.cma.creates)
        self.assertFalse(row['data']['mailbox_history']['complete'])
        self.assertEqual(len(row['data']['mailbox_history']['messages']), 1)
        with patch.object(control, 'ROOT', self.root), self.assertRaisesRegex(GateError, 'incomplete'):
            control.validate_result(self.result(), row)
        # A retry replaces partial material with a fresh complete scan.
        row = self.retry_when_due()
        self.assertEqual(row['state'], 'running')
        self.assertTrue(row['data']['mailbox_history']['complete'])
        self.assertEqual(row['data']['mailbox_history']['messages'], [])

    def test_paused_or_disabled_tasks_do_not_query_mailbox(self):
        self.cfg['paused'] = True; self.tick(add=True)
        self.cfg['paused'] = False; self.cfg['kinds']['firstcontact'] = False; self.tick()
        self.assertFalse(self.graph.paths); self.assertFalse(self.cma.creates)

    def test_graph_folder_pagination_is_exhausted_and_errors_are_not_truncated_success(self):
        graph = mailbox.Graph('fixture-token')
        first = self.message('sentitems'); second = {**first, 'id': 'graph-2'}
        pages = [{'value': [first], '@odata.nextLink': graph.BASE+'/next-page'}, {'value': [second]},
                 {'value': []}, {'value': []}]
        with patch.object(graph, 'request', side_effect=pages) as fetch:
            box = mailbox.GraphMailbox(graph, self.box.settings)
            self.assertEqual(len(box.recipient_history('alias@example.invalid')), 2)
            self.assertEqual(fetch.call_args_list[1].args[0], '/next-page')
        with patch.object(graph, 'request', side_effect=[pages[0], TimeoutError('page two failed')]):
            with self.assertRaises(TimeoutError):
                box.recipient_history('alias@example.invalid')

    def test_cap_across_search_pages_falls_back_to_fully_paginated_metadata(self):
        graph = mailbox.Graph('fixture-token')
        box = mailbox.GraphMailbox(graph, self.box.settings)
        unrelated = self.message(recipient='unrelated@example.invalid')
        pages = [{'value': [unrelated] * 999, '@odata.nextLink': graph.BASE+'/search-next'},
                 {'value': [unrelated]},
                 {'value': [unrelated], '@odata.nextLink': graph.BASE+'/scan-next'},
                 {'value': [self.message()]}, {'value': []}, {'value': []}]
        with patch.object(graph, 'request', side_effect=pages) as fetch:
            rows = box.recipient_history('alias@example.invalid')
        self.assertEqual([m['id'] for m in rows], ['graph-1'])
        self.assertEqual(fetch.call_args_list[1].args[0], '/search-next')
        fallback = parse_qs(urlsplit(fetch.call_args_list[2].args[0]).query)
        self.assertNotIn('$search', fallback); self.assertNotIn('$filter', fallback)
        self.assertIn('bodyPreview', fallback['$select'][0].split(','))
        self.assertEqual(fetch.call_args_list[3].args[0], '/scan-next')

    def test_failed_fallback_page_never_caches_partial_results_or_rescans_same_tick(self):
        graph = mailbox.Graph('fixture-token')
        box = mailbox.GraphMailbox(graph, self.box.settings)
        pages = [{'value': [self.message()] * 1000},
                 {'value': [self.message()], '@odata.nextLink': graph.BASE+'/scan-next'},
                 TimeoutError('Full scan page two failed')]
        with patch.object(graph, 'request', side_effect=pages) as fetch:
            for recipient in ('alias@example.invalid', 'b@example.invalid'):
                with self.assertRaisesRegex(TimeoutError, 'page two failed'):
                    box.recipient_history(recipient)
            self.assertEqual(fetch.call_count, 3)

    def test_search_transport_sets_eventual_consistency_header(self):
        graph = mailbox.Graph('fixture-token')
        path = '/me/mailFolders/inbox/messages?' + urlencode({
            '$search': '"participants:alias@example.invalid"', '$top': '100'})
        with patch('managed.mailbox.build_opener') as opener:
            opener.return_value.open.return_value.__enter__.return_value.read.return_value = b'{"value":[]}'
            graph.request(path)
        sent_request = opener.return_value.open.call_args.args[0]
        self.assertEqual(sent_request.get_header('Consistencylevel'), 'eventual')
        self.assertIn('IdType="ImmutableId"', sent_request.get_header('Prefer'))


if __name__ == '__main__':
    unittest.main()


class MessageBodyTests(unittest.TestCase):
    def test_preview_only_row_fetches_the_full_message(self):
        full = 'Dear Avery, ' + 'details ' * 80 + 'end of the full message.'
        class Box:
            calls = []
            def message(self, identity):
                self.calls.append(identity)
                return {'id': identity, 'body': {'contentType': 'text', 'content': full}}
        box = Box()
        row = {'id': 'm1', 'bodyPreview': full[:255]}
        details = firstcontact_history._message_details(box, row)
        self.assertEqual(firstcontact_history._message_body(details), full)
        self.assertEqual(box.calls, ['m1'])

    def test_full_body_is_used_without_a_fetch(self):
        class Box:
            def message(self, identity):
                raise AssertionError('no fetch expected')
        row = {'id': 'm1', 'body': {'contentType': 'text', 'content': 'Complete text.'}, 'bodyPreview': 'Complete'}
        details = firstcontact_history._message_details(Box(), row)
        self.assertEqual(firstcontact_history._message_body(details), 'Complete text.')

    def test_sent_signature_stops_at_plain_or_html_quote_boundaries(self):
        own = '\nDear Professor A,\n\nOn the mathematical side, the calculation checks out.'
        own += '\n\nBest regards,\n\nAvery and Blaire\nExample University\nExample Research Group'
        quotes = [
            '> An incoming message with no attribution header',
            'On Fri, Nov 6, 2026 at 2:20 PM Author A <a@example.invalid> wrote:',
            'From: Author A <a@example.invalid>\nSent: Friday, November 6, 2026 2:20 PM',
            '2026年11月6日(金) 14:20 Author A <a@example.invalid>:',
            '-----Original Message-----',
            '________________________________',
        ]
        for quote in quotes:
            text_body = own + '\n\n' + quote + '\n\nOther author,\nPrevious signature'
            for html in (False, True):
                with self.subTest(quote=quote, html=html):
                    body = '<html><body>' + escape(text_body).replace('\n', '<br>') + '</body></html>'
                    actual = firstcontact_history._sent_signature(body if html else text_body, html)
                    self.assertEqual(actual, {
                        'salutation': 'Dear Professor A,',
                        'closing': 'Best regards,\n\nAvery and Blaire\nExample University\nExample Research Group',
                        'has_affiliation_lines': True})

    def test_html_blocks_and_structural_quote_preserve_the_sign_off(self):
        body = ('<head><style>p { color: red }</style></head><body>'
                '<p>Dear Professor A,</p><p>On the mathematical side, our proof works.</p>'
                '<div>Best regards,</div><div><br></div><div>Avery &amp; Blaire</div>'
                '<blockquote><p>Earlier author</p></blockquote></body>')
        self.assertEqual(firstcontact_history._sent_signature(body, True), {
            'salutation': 'Dear Professor A,', 'closing': 'Best regards,\n\nAvery & Blaire',
            'has_affiliation_lines': False})

    def test_collapsed_outlook_header_line_is_a_quote_boundary(self):
        # Graph's text conversion can put the rule and the header on one line.
        body = ('Dear Dr. Example,\n\nThe bound holds.\n\nBest regards,\nAvery and Blaire\n\n'
                '________________________________ From: A Person <a@example.invalid> Sent: Sunday, '
                '15 November 2026 09:56:54 To: Reed Avery Subject: Re: x\n\n'
                'Earlier message text that must not become the closing. This message and its attachment '
                'are confidential.\n')
        self.assertEqual(firstcontact_history._sent_signature(body, False), {
            'salutation': 'Dear Dr. Example,', 'closing': 'Best regards,\nAvery and Blaire',
            'has_affiliation_lines': False})

    def test_sent_closing_is_bounded(self):
        signature = firstcontact_history._sent_signature(
            'Dear Professor A,\n\nAnswer.\n\nBest,\nAvery\n' + 'x'*500 + '\na\nb\nc\nd\ne\nf', False)
        self.assertLessEqual(len(signature['closing'].splitlines()), 6)
        self.assertLessEqual(len(signature['closing']), 400)
        signature = firstcontact_history._sent_signature(
            'Dear Professor A,\n\nAnswer.\n\nBest,\nAvery\n' + 'x'*500, False)
        self.assertEqual(len(signature['closing']), 400)


def setUpModule():
    use_example_config()
