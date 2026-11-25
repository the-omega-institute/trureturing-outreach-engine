"""Business-scenario acceptance with isolated CMA, Releases and mailbox fixtures."""
import copy
from datetime import datetime, timezone
import hashlib
import hmac
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from fixture_config import use_example_config
from managed import automation, control, delivery, mailbox
from managed.control import Ledger, advance, sign_review, validate_result, workflow
from managed.owner_todo import owner_todo, render_markdown
from managed.queue import GateError, canonical, digest
from managed.tick import Tick
from managed.transports import NotSent, Readback, Submission
from test_tasks import CMA, Releases, request, use_fixture_workflow_revisions


class Mailbox:
    def __init__(self):self.calls=[];self.saved=[];self.lose_send=False;self.before_send=lambda:None
    def preflight(self,packet,request):
        now=time.time()
        return {'checked_at':now,'expires_at':now+60,'complete':True,'paused':False,
                'contacts':{a:'clear' for a in packet['to']+packet['cc']}}
    def submit(self,packet,key):
        self.before_send();self.calls.append((copy.deepcopy(packet),key))
        if self.lose_send:raise TimeoutError('Accepted, response lost')
        return Submission('server-accepted')
    def lookup(self,packet,key):
        return Readback(key,digest(packet),'sent-id','fixture:actual-sent') if self.calls else None
    def save_draft(self,packet,key,recovery=False):
        self.saved.append((copy.deepcopy(packet),key,recovery))
        return {'remote_id':'draft-id','content_hash':digest(packet),'evidence_ref':'fixture:actual-draft'}


class GuardMailbox(Mailbox):
    def __init__(self, drafts=(), keep_saved=True):
        super().__init__()
        self.drafts=list(drafts);self.keep_saved=keep_saved
    def save_draft(self,packet,key,recovery=False):
        receipt=super().save_draft(packet,key,recovery)
        if self.keep_saved:  # an unsent owner draft stays in Drafts
            self.drafts.append({'id':receipt['remote_id'],'isDraft':True,
                                'from':{'emailAddress':{'address':packet['sender']}},
                                'toRecipients':[{'emailAddress':{'address':a}} for a in packet['to']],
                                'ccRecipients':[{'emailAddress':{'address':a}} for a in packet['cc']]})
        return receipt
    def drafts_for(self, recipient):
        return [draft for draft in self.drafts
                if recipient in {draft.get('from', {}).get('emailAddress', {}).get('address'),
                                 *[value.get('emailAddress', {}).get('address')
                                   for value in draft.get('toRecipients', [])],
                                 *[value.get('emailAddress', {}).get('address')
                                   for value in draft.get('ccRecipients', [])]}]


class AutomationTests(unittest.TestCase):
    def setUp(self):
        use_fixture_workflow_revisions(self)
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        (self.root/'tables').mkdir();self.folder=self.root/'ledger';self.folder.mkdir()
        self.remote=Releases();self.ledger=Ledger(self.remote,self.folder)
        self.cfg={'paused':False,'kinds':{'reply':True,'submission':True},'max_active':4,'agent_template':{},
                  'automation':{'enabled':True,'automatic_mail':True,'max_new_tasks_per_tick':4,'max_revisions':3}}
        self.tables('contacts',{'contacts':[{'id':'contact-a','name':'Professor Example','email':'author@example.invalid',
                                           'problem_ids':[],'hub':{'workspace_paths':['../mail/person']}}]})
        self.tables('refusals',{'refusals':[]});self.tables('submissions',{'papers':[]})
    def tearDown(self):self.ledger.tasks.close();self.temp.cleanup()
    def tables(self,name,value):(self.root/'tables'/(name+'.json')).write_text(json.dumps(value))
    def result(self,row,outcome='completed',route='auto',questions=None,paper=None):
        packet=json.loads((Path(__file__).parent/'fixtures/letter.json').read_text())
        packet.update(kind='reply',thread_id='thread-1',latest_inbound_message_id='<inbound-1>',
                      user_decision_ever=bool(row.get('request',{}).get('inputs',{}).get('user_decision_ever')))
        result={'schema_version':1,'task_id':row['id'],'outcome':outcome,'report':'Checked the actual request.',
                'packet':packet if outcome=='completed' and route!='none' else None,'artifacts':[],
                'routing':{'mail':route,'questions':questions or [],'independent_submission':paper}}
        if route == 'owner' or questions:
            result['owner_brief']={'summary_zh':'这封信处理了当前请求。','decisions_zh':['决定是否发送。'],
                                   'cautions_zh':['发送前检查收件人。']}
        return result
    def add(self,name='reply-1',kind='reply',user_decision_ever=False):
        req=request(name,'contact-a',kind);req['inputs']={'automation_contract':1}
        if user_decision_ever:req['inputs']['user_decision_ever']=True
        return self.ledger.tasks.add(req)
    def review(self,row,result,verdict='PASS',rebind_workflow_hash=None,reviewer='local-claude-loop'):
        validate_result(result,row)
        row=self.ledger.tasks.update(row['id'],row['revision'],'awaiting-review',{'result':result})
        env={'binding':self.ledger.tasks.review_binding(row),'reviewer':reviewer,'verdict':verdict,
             'reason':'Read complete thread and exact current files.','evidence_ref':'fixture:independent-review'}
        if rebind_workflow_hash is not None:env['rebind_workflow_hash']=rebind_workflow_hash
        env=sign_review(env,'secret',kind=row['kind'])
        return self.ledger.tasks.verdict(env,'secret')
    def rebind(self,row,current_hash):
        env={k:v for k,v in row['data']['review'].items() if k!='signature'}
        env['rebind_workflow_hash']=current_hash
        env=sign_review(env,'secret',kind=row['kind'])
        self.remote.assets['review-'+digest(env)+'.json']=canonical(env).encode()
        self.assertEqual(self.ledger.ingest('secret'),[])
        return self.ledger.tasks.get(row['id'])
    def route(self):
        with patch('managed.control.ROOT',self.root):advance(self.ledger,CMA(),{**self.cfg,'paused':True},self.folder)
    def continue_work(self):automation.reconcile(self.ledger,self.cfg,self.root,revision='b'*40)

    def register_recipient(self, address):
        contacts = self.root / 'tables/contacts.json'
        data = json.loads(contacts.read_text())
        data['contacts'].append({'id': 'contact-' + address, 'name': 'Recipient Fixture',
                                 'email': address, 'problem_ids': []})
        self.tables('contacts', data)

    def test_hosted_scientific_reviewer_identity_survives_mail_queue(self):
        row = self.add('hosted-mail')
        self.review(row, self.result(row), reviewer='hosted-cma-review')
        self.route()
        box = Mailbox()
        delivery.advance(self.ledger, self.cfg, self.root, box)
        self.assertEqual(len(box.calls), 1)
        envelope = json.loads(self.ledger.tasks.db.execute(
            'SELECT envelope FROM reviews WHERE item=?', (row['id'],)).fetchone()[0])
        self.assertEqual(envelope['reviewer'], 'hosted-cma-review')
        self.assertEqual(envelope['evidence_ref'], 'fixture:independent-review')

    def test_home_to_cc_and_subdomains_force_an_auto_routed_reply_to_drafts(self):
        self.cfg['home_domains'] = ['campus.example.invalid', 'alumni.example.invalid']
        for field, address in [('to', 'faculty@campus.example.invalid'), ('cc', 'faculty@alumni.example.invalid'),
                               ('to', 'faculty@math.campus.example.invalid')]:
            with self.subTest(field=field, address=address):
                self.register_recipient(address)
                row = self.add('home-' + address.split('@')[1])
                result = self.result(row)
                result['packet'][field] = [address]
                reviewed = self.review(row, result)
                self.route()
                box = Mailbox()
                delivery.advance(self.ledger, self.cfg, self.root, box)
                final = self.ledger.tasks.get(row['id'])
                self.assertEqual(final['state'], 'owner-draft')
                self.assertEqual(final['data']['routing_override'], {
                    'reason': 'home-institution recipient', 'addresses': [address]})
                self.assertEqual(final['data']['result'], result)
                self.assertEqual(final['data']['review'], reviewed['data']['review'])
                self.assertEqual([entry[0] for entry in box.saved], [result['packet']])
                self.assertFalse(box.calls)

    def test_home_cc_on_first_contact_is_visible_in_work_and_owner_todo(self):
        self.cfg['kinds']['firstcontact'] = True
        self.cfg['mailbox'] = {'home_domains': ['CAMPUS.EXAMPLE.INVALID']}
        address = 'faculty@campus.example.invalid'
        self.register_recipient(address)
        req = request('home-firstcontact', 'contact-a', 'firstcontact')
        req['inputs'] = {'automation_contract': 1, 'recipient_emails': ['author@example.invalid', address]}
        row = self.ledger.tasks.add(req)
        result = self.result(row)
        result['packet'] = json.loads((Path(__file__).parent/'fixtures/letter.json').read_text())
        result['packet']['cc'] = [address]
        with patch.object(control, 'ROOT', self.root), \
                patch.object(control, 'workflow', return_value=req['workflow_hash']):
            self.review(row, result)
        self.route()
        box = Mailbox()
        delivery.advance(self.ledger, self.cfg, self.root, box)
        self.assertEqual([entry[0] for entry in box.saved], [result['packet']])
        self.assertFalse(box.calls)
        observations = {row['id']: {'remote_id': 'draft-id', 'draft_status': 'present'}}
        with patch('managed.draft_status.inventory', return_value=observations), \
                patch('managed.daily_local.status', return_value={}):
            work = automation.work(self.ledger.tasks, self.root, self.cfg)
        override = {'reason': 'home-institution recipient', 'addresses': [address]}
        self.assertEqual(work['owner_drafts'][0]['routing_override'], override)
        self.assertEqual(work['affairs_tasks'][0]['routing_override'], override)
        self.assertIn('收件人在本校，需你过目后发送', render_markdown(owner_todo(work, self.root)))

    def test_non_home_domain_with_similar_suffix_still_auto_sends(self):
        self.cfg['home_domains'] = ['campus.example.invalid', 'alumni.example.invalid']
        for address in ('faculty@notcampus.example.invalid', 'faculty@campus.example.invalid.other.example.invalid'):
            self.register_recipient(address)
            row = self.add('external-' + address.split('@')[1])
            result = self.result(row)
            result['packet']['to'] = [address]
            result['packet']['latest_inbound_message_id'] = f'<{row["id"]}>'
            self.review(row, result)
            self.route()
            box = Mailbox()
            delivery.advance(self.ledger, self.cfg, self.root, box)
            delivery.advance(self.ledger, self.cfg, self.root, box)
            final = self.ledger.tasks.get(row['id'])
            self.assertEqual(final['state'], 'verified')
            self.assertNotIn('routing_override', final['data'])
            self.assertEqual([entry[0] for entry in box.calls], [result['packet']])
            self.assertFalse(box.saved)

    def legacy_home_reply(self, task_id='legacy-home'):
        row = self.add(task_id)
        result = self.result(row)
        result['packet']['to'] = ['faculty@campus.example.invalid']
        reviewed = self.review(row, result)
        return self.ledger.tasks.update(row['id'], reviewed['revision'], 'awaiting-mailbox', reviewed['data'])

    def test_coordinator_tick_migrates_legacy_home_mail_before_delivery(self):
        row = self.legacy_home_reply()
        self.register_recipient('faculty@campus.example.invalid')
        self.cfg['home_domains'] = ['campus.example.invalid', 'alumni.example.invalid']
        box = Mailbox()
        tick = Tick(600)
        tick.bind(self.ledger)
        deliver = delivery.advance
        with patch('managed.mailbox.configured', return_value=box), patch('managed.mailbox.intake'), \
                patch('managed.control.refresh_oracle_capacity'), patch('managed.control.advance'), \
                patch('managed.control.CMA', return_value=CMA()), patch('managed.pipeline.reconcile'), \
                patch('managed.sent_events.advance', return_value={}), \
                patch('managed.draft_status.refresh', return_value=None), \
                patch('managed.publication.advance'), patch('managed.mathesis_delivery.advance'), \
                patch('managed.formalization_requests.advance'), patch('managed.prior_work_issues.advance'), \
                patch.object(self.remote, 'prune_snapshots', create=True), \
                patch('managed.delivery.advance', side_effect=lambda ledger, config, root, mailbox:
                      deliver(ledger, config, self.root, mailbox)):
            control.run_tick(self.ledger, self.cfg, self.folder, tick)
        final = self.ledger.tasks.get(row['id'])
        self.assertEqual(final['state'], 'owner-draft')
        self.assertEqual(final['data']['review'], row['data']['review'])
        self.assertEqual([entry[0] for entry in box.saved], [row['data']['result']['packet']])
        self.assertFalse(box.calls)

    def test_home_mail_migration_is_bounded_and_idempotent(self):
        self.cfg['home_domains'] = ['campus.example.invalid']
        limit = automation.HOME_RECIPIENT_MIGRATION_LIMIT
        rows = [self.legacy_home_reply(f'legacy-home-{i}') for i in range(limit + 1)]
        automation.migrate_home_recipients(self.ledger.tasks, self.cfg)
        migrated = [self.ledger.tasks.get(row['id']) for row in rows]
        self.assertEqual([row['state'] for row in migrated], ['owner-draft'] * limit + ['awaiting-mailbox'])
        automation.migrate_home_recipients(self.ledger.tasks, self.cfg)
        final = [self.ledger.tasks.get(row['id']) for row in rows]
        self.assertEqual(final[:-1], migrated[:-1])
        self.assertEqual(final[-1]['state'], 'owner-draft')
        automation.migrate_home_recipients(self.ledger.tasks, self.cfg)
        self.assertEqual([self.ledger.tasks.get(row['id']) for row in rows], final)

    def test_send_guard_refuses_legacy_home_mail_even_without_migration(self):
        row = self.legacy_home_reply()
        self.register_recipient('faculty@campus.example.invalid')
        self.cfg['home_domains'] = ['campus.example.invalid']
        box = Mailbox()
        delivery.advance(self.ledger, self.cfg, self.root, box)
        guarded = self.ledger.tasks.get(row['id'])
        self.assertEqual(guarded['state'], 'owner-draft')
        self.assertEqual(guarded['data']['routing_override'], {
            'reason': 'home-institution recipient', 'addresses': ['faculty@campus.example.invalid']})
        self.assertFalse(box.calls)
        delivery.advance(self.ledger, self.cfg, self.root, box)
        self.assertEqual([entry[0] for entry in box.saved], [row['data']['result']['packet']])
        self.assertFalse(box.calls)

    def test_migrated_home_mail_with_proven_unsent_attempt_saves_an_owner_draft(self):
        from test_reply_threading import DraftGraph

        row = self.legacy_home_reply()
        self.register_recipient('faculty@campus.example.invalid')
        packet = row['data']['result']['packet']
        graph = DraftGraph(packet)
        box = mailbox.GraphMailbox(graph, {'account': packet['sender'], 'reconciled': True})
        # Model a legacy automatic attempt that failed before /send.
        graph.force_wrong_conversation = True
        delivery.advance(self.ledger, self.cfg, self.root, box)
        self.assertEqual(self.ledger.tasks.db.execute('SELECT status FROM attempts').fetchone()[0], 'not-sent')
        self.cfg['home_domains'] = ['campus.example.invalid']
        automation.migrate_home_recipients(self.ledger.tasks, self.cfg)
        graph.force_wrong_conversation = False
        for _ in range(2):
            delivery.advance(self.ledger, self.cfg, self.root, box)
        final = self.ledger.tasks.get(row['id'])
        self.assertEqual(final['state'], 'owner-draft')
        self.assertEqual(set(graph.drafts), {final['data']['draft_receipt']['remote_id']})
        self.assertFalse(any(path.endswith('/send') for path, _, _ in graph.calls))

    def test_signed_pass_dry_run_reports_the_home_recipient_override(self):
        row = self.add()
        result = self.result(row)
        result['packet']['cc'] = ['faculty@math.campus.example.invalid', 'faculty@alumni.example.invalid']
        validate_result(result, row)
        pending = self.ledger.tasks.update(row['id'], row['revision'], 'awaiting-review', {'result': result})
        envelope = sign_review({'binding': self.ledger.tasks.review_binding(pending), 'verdict': 'PASS',
            'reviewer': 'local-claude-loop', 'reason': 'Checked the letter', 'evidence_ref': 'fixture:review'},
            'secret', kind='reply')
        self.ledger.save(force=True)
        with patch.object(control, 'review_key', return_value='secret'), \
                patch.object(control, 'load_config', return_value=self.cfg | {'home_domains': ['campus.example.invalid', 'alumni.example.invalid']}):
            report = control.review_dry_run(envelope, self.remote)
        self.assertEqual(report, {'routing_override': {
            'reason': 'home-institution recipient', 'addresses': result['packet']['cc']}})
        self.assertEqual(self.ledger.tasks.get(row['id']), pending)

    def reviewed_delivery_graph(self, name, route='auto'):
        from test_reply_threading import DraftGraph
        row=self.add(name);result=self.result(row,route=route);self.review(row,result);self.route()
        graph=DraftGraph(result['packet'])
        box=mailbox.GraphMailbox(graph,{'account':result['packet']['sender'],'reconciled':True})
        return self.ledger.tasks.get(row['id']),graph,box

    def assert_terminal_delivery(self, row, graph, box, blocker):
        original=copy.deepcopy(row['data'])
        delivery.advance(self.ledger,self.cfg,self.root,box)
        held=self.ledger.tasks.get(row['id'])
        self.assertEqual(held['state'],'held')
        self.assertEqual(held['data']['hold_kind'],'delivery-terminal')
        self.assertEqual(held['data']['hold_reason'],blocker)
        self.assertEqual(held['data']['result'],original['result'])
        self.assertEqual(held['data']['review'],original['review'])
        calls=copy.deepcopy(graph.calls)
        delivery.advance(self.ledger,self.cfg,self.root,box)
        self.continue_work()
        self.assertEqual(self.ledger.tasks.get(row['id']),held)
        self.assertEqual(graph.calls,calls)
        self.assertFalse(graph.mimes)
        self.assertTrue(all(method=='GET' for _,method,_ in graph.calls))
        work=automation.work(self.ledger.tasks,self.root)
        self.assertEqual([item for item in work['delivery_terminal'] if item['task_id']==row['id']],
                         [{'task_id':row['id'],'owner':row['owner'],'blocker':blocker}])
        self.assertNotIn(row['id'],[item['binding']['task_id'] for item in work['reviews']])
        self.assertNotIn(row['id'],[item['task_id'] for item in work['owner_draft_exceptions']])
        if row['state']=='owner-draft':
            self.assertNotIn('draft_intent',held['data'])
            self.assertNotIn('draft_receipt',held['data'])

    def test_terminal_delivery_blockers_hold_exact_result_without_mailbox_writes(self):
        cases=[('refusal','Current conversation has an active table-3 refusal: author@example.invalid'),
               ('incoming','A newer incoming message requires a new reply/review'),
               ('other-incoming','A newer incoming message from author@example.invalid in another conversation requires a new reply/review'),
               ('recipients','Reply-all recipients differ from the latest incoming message'),
               ('draft','A server draft already exists; preserve the user draft'),
               ('sent','Actual Sent changed or its source identity was not captured')]
        for route,name,blocker in [(route,name,blocker) for route in ('auto','owner') for name,blocker in cases]:
            with self.subTest(route=route,blocker=name):
                row,graph,box=self.reviewed_delivery_graph('terminal-'+route+'-'+name,route=route)
                if name=='refusal':
                    self.tables('refusals',{'refusals':[{'id':'refusal-a','name':'Professor Example',
                                'emails':['author@example.invalid'],'active':True}]})
                    # Historical replies carry coordinator-hydrated thread data.
                    row=self.ledger.tasks.update(row['id'],row['revision'],row['state'],
                        {**row['data'],'thread_inputs':{'thread':[graph.inbound]}})
                elif name=='incoming':graph.inbound['internetMessageId']='<newer>'
                elif name=='other-incoming':
                    box.settings['cross_conversation_guard']=True
                    box.recipient_history=lambda recipient:[{**graph.inbound,'folder':'inbox',
                        'conversationId':'other-thread','receivedDateTime':'2026-11-02T05:00:00Z'}]
                elif name=='recipients':
                    graph.inbound['ccRecipients']=[{'emailAddress':{'address':'colleague@example.invalid'}}]
                elif name=='draft':graph.strand('user-draft',wrong=False)
                elif name=='sent':
                    graph.sent['new-sent']={**graph.message('new-sent','unrelated'),
                        'isDraft':False,'sentDateTime':'2026-11-01T04:00:00Z'}
                drafts=copy.deepcopy(graph.drafts)
                self.assert_terminal_delivery(row,graph,box,blocker)
                self.assertEqual(graph.drafts,drafts)
                self.tables('refusals',{'refusals':[]})

    def test_unsaved_owner_draft_transient_and_unknown_errors_keep_backoff(self):
        errors=[TimeoutError('Connector timeout'),GateError('Unrecognized mailbox blocker')]
        for index,error in enumerate(errors):
            with self.subTest(error=str(error)):
                row,graph,box=self.reviewed_delivery_graph('owner-retry-'+str(index),route='owner')
                with patch.object(box,'preflight',side_effect=error) as check:
                    for _ in range(2):delivery.advance(self.ledger,self.cfg,self.root,box)
                current=self.ledger.tasks.get(row['id'])
                self.assertEqual(current['state'],'owner-draft')
                self.assertEqual(current['data']['last_error'],str(error))
                self.assertNotIn('hold_kind',current['data'])
                self.assertNotIn('draft_intent',current['data'])
                self.assertEqual(check.call_count,1)
                work=automation.work(self.ledger.tasks,self.root)
                self.assertEqual([item['blocker'] for item in work['owner_draft_exceptions']
                                  if item['task_id']==row['id']],['save-failed'])
                self.assertFalse(graph.mimes)
                self.assertTrue(all(method=='GET' for _,method,_ in graph.calls))
                self.ledger.tasks.update(row['id'],current['revision'],'superseded',current['data'])

    def test_owner_draft_receipts_intents_and_stale_conflicts_are_not_terminal(self):
        blocker='Current conversation has an active table-3 refusal: author@example.invalid'
        protected=[{'draft_receipt':{'remote_id':'stranded'}},
                   {'draft_intent':{'key':'pending','at':time.time()}},
                   {'draft_revision_intent':{'key':'pending-revision'}},
                   {'stale_draft_conflict':{'task_ids':['older'],'remote_id':'stranded'}}]
        for index,fields in enumerate(protected):
            with self.subTest(fields=fields):
                row,graph,box=self.reviewed_delivery_graph('owner-protected-'+str(index),route='owner')
                graph.strand('own-draft',wrong=False)
                row=self.ledger.tasks.update(row['id'],row['revision'],row['state'],
                    {**row['data'],**fields,'thread_inputs':{'thread':[graph.inbound]}})
                self.tables('refusals',{'refusals':[{'id':'refusal-a','name':'Professor Example',
                            'emails':['author@example.invalid'],'active':True}]})
                drafts=copy.deepcopy(graph.drafts)
                # Isolate the delivery gates from the separate revision-recovery path.
                with patch('managed.delivery._advance_draft_revision',return_value=row):
                    delivery.advance(self.ledger,self.cfg,self.root,box)
                current=self.ledger.tasks.get(row['id'])
                self.assertEqual(current['state'],'owner-draft')
                self.assertEqual(current['data']['last_error'],blocker)
                self.assertNotIn('hold_kind',current['data'])
                for key,value in fields.items():self.assertEqual(current['data'][key],value)
                self.assertEqual(graph.drafts,drafts)
                self.assertFalse(graph.mimes)
                self.assertTrue(all(method=='GET' for _,method,_ in graph.calls))
                self.ledger.tasks.update(row['id'],current['revision'],'superseded',current['data'])
                self.tables('refusals',{'refusals':[]})

    def test_unsaved_owner_draft_uncertain_ownership_read_keeps_retry(self):
        row,graph,box=self.reviewed_delivery_graph('owner-uncertain-draft',route='owner')
        draft=graph.strand('user-draft',wrong=False)
        rows=box.thread(row['data']['result']['packet']['thread_id'])
        # Stale-draft check and preflight succeed in reading; classification cannot.
        with patch.object(box,'thread',side_effect=[rows,rows,TimeoutError('Draft read unavailable')]):
            delivery.advance(self.ledger,self.cfg,self.root,box)
        current=self.ledger.tasks.get(row['id'])
        self.assertEqual(current['state'],'owner-draft')
        self.assertNotIn('hold_kind',current['data'])
        self.assertEqual(graph.drafts[draft['id']],draft)
        self.assertFalse(graph.mimes)

    def test_transient_and_unknown_delivery_errors_keep_retrying(self):
        from urllib.error import HTTPError
        errors=[HTTPError('https://graph',503,'Unavailable',{},None),
                HTTPError('https://graph',429,'Rate limited',{},None),
                TimeoutError('Connector timeout'),GateError('Mailbox credentials unavailable'),
                GateError('Unrecognized mailbox blocker')]
        for index,error in enumerate(errors):
            with self.subTest(error=str(error)):
                if isinstance(error,HTTPError):self.addCleanup(error.close)
                row,graph,box=self.reviewed_delivery_graph('transient-'+str(index))
                with patch.object(box,'preflight',side_effect=error) as check:
                    for _ in range(2):delivery.advance(self.ledger,self.cfg,self.root,box)
                current=self.ledger.tasks.get(row['id'])
                self.assertEqual(current['state'],'awaiting-mailbox')
                self.assertEqual(current['data']['last_error'],str(error))
                self.assertNotIn('hold_kind',current['data'])
                self.assertEqual(check.call_count,2)
                self.assertFalse(graph.mimes)
                # Keep other subcases out of subsequent delivery passes.
                self.ledger.tasks.update(row['id'],current['revision'],'superseded',current['data'])

    def test_own_saved_draft_and_uncertain_draft_read_stay_transient(self):
        for own in (True,False):
            with self.subTest(own=own):
                row,graph,box=self.reviewed_delivery_graph('draft-retry-'+str(own))
                draft=graph.strand('saved-attempt',wrong=False)
                if own:
                    row=self.ledger.tasks.update(row['id'],row['revision'],row['state'],
                        {**row['data'],'draft_receipt':{'remote_id':draft['id']}})
                # An unavailable ownership read must not turn a draft into a hold.
                reads=[box.thread(row['data']['result']['packet']['thread_id'])]
                reads.append(reads[0] if own else TimeoutError('Draft read unavailable'))
                with patch.object(box,'thread',side_effect=reads):
                    delivery.advance(self.ledger,self.cfg,self.root,box)
                current=self.ledger.tasks.get(row['id'])
                self.assertEqual(current['state'],'awaiting-mailbox')
                self.assertNotIn('hold_kind',current['data'])
                self.assertEqual(graph.drafts[draft['id']],draft)
                self.assertFalse(graph.mimes)
                self.ledger.tasks.update(row['id'],current['revision'],'superseded',current['data'])

    def test_terminal_old_reply_keeps_newer_intake_task_before_or_after_hold(self):
        for intake_first in (True,False):
            with self.subTest(intake_first=intake_first):
                row,graph,box=self.reviewed_delivery_graph('old-reply-'+str(intake_first))
                graph.inbound['internetMessageId']='<newer-'+str(intake_first)+'>'
                box.settings.update(live_since='2026-10-30T00:00:00Z')
                graph.sent['previous']={**graph.message('previous','earlier'),
                    'isDraft':False,'sentDateTime':'2026-10-31T04:00:00Z'}
                when=datetime(2026,11,2,tzinfo=timezone.utc)
                def intake():
                    with patch.object(box,'messages',return_value=[graph.inbound]):
                        mailbox.intake(self.ledger,self.cfg,self.root,box,revision='b'*40,now=when)
                if intake_first:intake()
                self.assert_terminal_delivery(row,graph,box,
                    'A newer incoming message requires a new reply/review')
                intake();intake()
                newer=[r for r in self.ledger.tasks.all() if r['request']['inputs'].get(
                    'latest_inbound_message_id')==graph.inbound['internetMessageId']]
                self.assertEqual(len(newer),1)
                self.assertEqual(newer[0]['state'],'queued')

    def test_routine_reply_is_sent_once_after_pass_and_intent_is_remotely_preserved(self):
        row=self.add();result=self.result(row);self.review(row,result);self.route()
        self.ledger.save(force=True)  # Start delivery inside the debounce window.
        box=Mailbox()
        def preserved():
            with tempfile.TemporaryDirectory() as temp:
                recovered=Ledger(self.remote,temp)
                try:self.assertEqual(recovered.tasks.db.execute('SELECT status FROM attempts').fetchone()[0],'sending')
                finally:recovered.tasks.close()
        box.before_send=preserved
        delivery.advance(self.ledger,self.cfg,self.root,box)
        self.assertEqual(len(box.calls),1)
        delivery.advance(self.ledger,self.cfg,self.root,box)
        self.assertEqual(self.ledger.tasks.get(row['id'])['state'],'verified')
        delivery.advance(self.ledger,self.cfg,self.root,box)
        self.assertEqual(len(box.calls),1)

    def test_pass_rebind_delivers_reply_with_stale_request_hash(self):
        row=self.add();original=copy.deepcopy(row['request']);result=self.result(row);box=Mailbox()
        with patch('managed.control.workflow',return_value='c'*64):
            reviewed=self.review(row,result,rebind_workflow_hash='c'*64)
            self.route()
            delivery.advance(self.ledger,self.cfg,self.root,box)
            self.assertEqual(len(box.calls),1)
            delivery.advance(self.ledger,self.cfg,self.root,box)
        row=self.ledger.tasks.get(row['id'])
        self.assertEqual(row['state'],'verified')
        self.assertEqual(row['request'],original)
        self.assertEqual(row['data']['review'],reviewed['data']['review'])

    def test_pass_rebind_delivers_firstcontact_with_stale_request_hash(self):
        self.cfg['kinds']['firstcontact']=True
        req=request('first-rebind','contact-a','firstcontact')
        req['inputs']={'automation_contract':1,'recipient_emails':['author@example.invalid']}
        row=self.ledger.tasks.add(req);result=self.result(row)
        result['packet']=json.loads((Path(__file__).parent/'fixtures/letter.json').read_text())
        box=Mailbox()
        with patch('managed.control.workflow',return_value='c'*64),patch('managed.control.ROOT',self.root):
            self.review(row,result,rebind_workflow_hash='c'*64);self.route()
            delivery.advance(self.ledger,self.cfg,self.root,box)
            self.assertEqual(len(box.calls),1)
            delivery.advance(self.ledger,self.cfg,self.root,box)
        self.assertEqual(self.ledger.tasks.get(row['id'])['state'],'verified')

    def test_stale_pass_without_rebind_blocks_automatic_mail_and_owner_draft(self):
        box=Mailbox()
        for route in ('auto','owner'):
            row=self.add('stale-'+route);self.review(row,self.result(row,route=route));self.route()
        with patch('managed.control.workflow',return_value='c'*64):
            delivery.advance(self.ledger,self.cfg,self.root,box)
        self.assertFalse(box.calls);self.assertFalse(box.saved)
        for route in ('auto','owner'):
            self.assertIn('Mail workflow changed',self.ledger.tasks.get('stale-'+route)['data']['last_error'])

    def test_legacy_deploy_preserves_owner_draft_save_and_signed_review_after_restore(self):
        req=request('legacy-owner','contact-a','reply')
        req['workflow_hash']=control.legacy_workflow('reply')
        row=self.ledger.tasks.add(req)
        reviewed=self.review(row,self.result(row,route='owner'));self.route()
        self.assertEqual(control.migrate_workflows(self.ledger.tasks),1)
        self.ledger.save(force=True)
        box=Mailbox()
        with tempfile.TemporaryDirectory() as folder:
            restored=Ledger(self.remote,folder)
            try:
                delivery.advance(restored,self.cfg,self.root,box)
                delivery.advance(restored,self.cfg,self.root,box)
                row=restored.tasks.get(row['id'])
                self.assertEqual(row['data']['review'],reviewed['data']['review'])
                self.assertEqual(row['request'],req)
                self.assertTrue(row['data'].get('draft_receipt'))
            finally:restored.tasks.close()
        self.assertEqual(len(box.saved),1)
        self.assertFalse(box.calls)

    def test_migration_preserves_existing_packet_queue_binding(self):
        from managed.queue import Queue
        req=request('legacy-queued-mail','contact-a','reply')
        legacy=control.legacy_workflow('reply');req['workflow_hash']=legacy
        row=self.ledger.tasks.add(req);result=self.result(row)
        self.review(row,result);self.route()
        queue=Queue(self.ledger.tasks.path,{'reply':legacy},reviewer='local-claude-loop')
        try:
            queue.add(row['id'],result['packet']);binding=queue.binding(row['id'])
            queue.research_checked(binding,'fixture:research');queue.enqueue(binding)
            queue.verdict({'schema_version':1,**binding,'reviewer':'local-claude-loop',
                           'verdict':'PASS','reason':'Checked','evidence_ref':'fixture:review'})
        finally:queue.close()
        control.migrate_workflows(self.ledger.tasks)
        box=Mailbox()
        delivery.advance(self.ledger,self.cfg,self.root,box)
        self.assertEqual(len(box.calls),1)
        delivery.advance(self.ledger,self.cfg,self.root,box)
        self.assertEqual(self.ledger.tasks.get(row['id'])['state'],'verified')

    def test_changed_result_cannot_reuse_pass_to_save_owner_draft(self):
        row=self.add();self.review(row,self.result(row,route='owner'));self.route()
        row=self.ledger.tasks.get(row['id'])
        data=copy.deepcopy(row['data']);data['result']['report']='Changed after review'
        self.ledger.tasks.update(row['id'],row['revision'],row['state'],data)
        box=Mailbox();delivery.advance(self.ledger,self.cfg,self.root,box)
        self.assertFalse(box.saved)
        self.assertIn('review no longer binds',self.ledger.tasks.get(row['id'])['data']['last_error'])

    def test_pending_pass_can_be_rebound_without_rerunning_cma(self):
        row=self.add();result=self.result(row);self.review(row,result);self.route();box=Mailbox()
        row=self.ledger.tasks.get(row['id']);original=copy.deepcopy(row)
        with patch('managed.control.workflow',return_value='c'*64):
            delivery.advance(self.ledger,self.cfg,self.root,box)
            self.assertFalse(box.calls)
            row=self.rebind(self.ledger.tasks.get(row['id']),'c'*64)
            self.assertEqual(row['state'],original['state'])
            self.assertEqual(row['request'],original['request'])
            self.assertEqual(row['data']['review']['binding'],original['data']['review']['binding'])
            delivery.advance(self.ledger,self.cfg,self.root,box)
            self.assertEqual(len(box.calls),1)
        self.assertEqual(len(self.ledger.tasks.all()),1)

    def test_second_workflow_change_blocks_until_a_fresh_rebind(self):
        row=self.add();result=self.result(row);box=Mailbox()
        with patch('managed.control.workflow',return_value='c'*64):
            self.review(row,result,rebind_workflow_hash='c'*64);self.route()
        with patch('managed.control.workflow',return_value='d'*64):
            delivery.advance(self.ledger,self.cfg,self.root,box)
            row=self.ledger.tasks.get(row['id'])
            self.assertFalse(box.calls);self.assertIn('Mail workflow changed',row['data']['last_error'])
            self.rebind(row,'d'*64)
            delivery.advance(self.ledger,self.cfg,self.root,box)
            self.assertEqual(len(box.calls),1)

    def test_pass_rebind_preserves_owner_draft_route(self):
        row=self.add();result=self.result(row,route='owner');self.review(row,result);self.route();box=Mailbox()
        with patch('managed.control.workflow',return_value='c'*64):
            row=self.rebind(self.ledger.tasks.get(row['id']),'c'*64)
            self.assertEqual(row['state'],'owner-draft')
            delivery.advance(self.ledger,self.cfg,self.root,box)
            delivery.advance(self.ledger,self.cfg,self.root,box)
        self.assertEqual(len(box.saved),1);self.assertFalse(box.calls)

    def test_owner_inventory_rebind_survives_failed_delivery_ticks_and_saves(self):
        row=self.add();result=self.result(row,route='owner');self.review(row,result);self.route()
        original=self.ledger.tasks.get(row['id']);box=Mailbox()
        with patch('managed.control.workflow',return_value='c'*64):
            work=automation.work(self.ledger.tasks,self.root)
            self.assertEqual(work['owner_drafts'][0]['blocker'],'workflow-changed')
            item=work['owner_draft_rebind'][0]
            env=sign_review({'binding':item['binding'],'rebind_workflow_hash':item['rebind_workflow_hash'],
                             'reviewer':'local-claude-loop','verdict':'PASS',
                             'reason':'Checked exact owner draft against the current workflow.',
                             'evidence_ref':'fixture:owner-rebind'},'secret',kind='reply')
            for _ in range(3):
                delivery.advance(self.ledger,self.cfg,self.root,box)
                row=self.ledger.tasks.get(row['id'])
                self.assertIn('Mail workflow changed',row['data']['last_error'])
                self.assertEqual(self.ledger.tasks.review_binding(row),item['binding'])
            self.assertGreater(row['revision'],original['revision'])
            self.assertFalse(box.saved)
            self.remote.assets['review-'+digest(env)+'.json']=canonical(env).encode()
            self.assertEqual(self.ledger.ingest('secret'),[])
            row=self.ledger.tasks.get(row['id'])
            self.assertEqual(row['state'],'owner-draft')
            self.assertEqual(row['request'],original['request'])
            self.assertTrue(automation.work(self.ledger.tasks,self.root)['owner_drafts'][0]['review_current'])
            self.assertEqual(automation.work(self.ledger.tasks,self.root)['owner_draft_rebind'],[])
            delivery.advance(self.ledger,self.cfg,self.root,box)
            work=automation.work(self.ledger.tasks,self.root)
            self.assertEqual(work['owner_draft_exceptions'],[])
            self.assertIsNone(work['owner_drafts'][0]['blocker'])
            delivery.advance(self.ledger,self.cfg,self.root,box)
        row=self.ledger.tasks.get(row['id'])
        self.assertEqual(row['state'],'owner-draft')
        self.assertEqual(row['data']['review'],env)
        self.assertTrue(row['data']['draft_receipt'])
        self.assertEqual(len(box.saved),1);self.assertFalse(box.calls)

    def test_mail_inventory_rebind_survives_delivery_errors_and_resumes_delivery(self):
        row=self.add();result=self.result(row);reviewed=self.review(row,result);self.route()
        original=self.ledger.tasks.get(row['id']);box=Mailbox()
        with patch('managed.control.workflow',return_value='c'*64):
            item=automation.work(self.ledger.tasks,self.root)['mail_rebind'][0]
            self.assertEqual({key:item[key] for key in ('task_id','owner','kind','reason')},
                             {'task_id':row['id'],'owner':row['owner'],'kind':'reply',
                              'reason':'Mail workflow changed; re-review the current version'})
            self.assertEqual(item['binding']['task_revision'],reviewed['revision'])
            for _ in range(3):
                delivery.advance(self.ledger,self.cfg,self.root,box)
                row=self.ledger.tasks.get(row['id'])
                self.assertEqual(self.ledger.tasks.review_binding(row),item['binding'])
            self.assertFalse(box.calls);self.assertFalse(box.saved)
            env=sign_review({'binding':item['binding'],'reviewer':'local-claude-loop','verdict':'PASS',
                             'reason':'Checked unchanged mail against the current workflow.',
                             'evidence_ref':'fixture:mail-rebind'},'secret',kind='reply')
            self.assertEqual(env['rebind_workflow_hash'],'c'*64)
            self.remote.assets['review-'+digest(env)+'.json']=canonical(env).encode()
            self.assertEqual(self.ledger.ingest('secret'),[])
            row=self.ledger.tasks.get(row['id'])
            self.assertEqual(row['state'],'awaiting-mailbox')
            self.assertEqual(row['request'],original['request'])
            self.assertEqual(row['data']['result'],result)
            self.assertEqual(automation.work(self.ledger.tasks,self.root)['mail_rebind'],[])
            delivery.advance(self.ledger,self.cfg,self.root,box)
            self.assertEqual(len(box.calls),1)
            delivery.advance(self.ledger,self.cfg,self.root,box)
        self.assertEqual(self.ledger.tasks.get(row['id'])['state'],'verified')

    def test_mail_rebind_excludes_current_changed_review_and_attempted_rows(self):
        from managed.queue import Queue
        row=self.add();self.review(row,self.result(row));self.route()
        original=self.ledger.tasks.get(row['id'])
        self.assertFalse(self.ledger.tasks.mail_rebindable(original,workflow('reply')))
        cases=[{'sent_receipt':{'remote_id':'sent'}}, {'draft_receipt':{'remote_id':'draft'}},
               {'stale_draft_conflict':{'remote_id':'other'}},
               {'result':{**original['data']['result'],'report':'Changed after PASS'}},
               {'review':{**original['data']['review'],'verdict':'HOLD'}},
               {'review':{**original['data']['review'],'verdict':'BOUNCE'}}]
        with patch('managed.control.workflow',return_value='c'*64):
            for change in cases:
                with self.subTest(change=change):
                    row=self.ledger.tasks.get(row['id'])
                    row=self.ledger.tasks.update(row['id'],row['revision'],row['state'],
                                                 {**original['data'],**change})
                    self.assertFalse(self.ledger.tasks.mail_rebindable(row,'c'*64))
                    self.assertEqual(automation.work(self.ledger.tasks,self.root)['mail_rebind'],[])
                    with self.assertRaises(ValueError):self.ledger.tasks.review_binding(row)
            row=self.ledger.tasks.update(row['id'],row['revision'],row['state'],original['data'])
            queue=Queue(self.ledger.tasks.path,{'reply':workflow('reply')})
            try:
                # Even a proven not-sent attempt keeps its original recovery path.
                queue.add(row['id'],original['data']['result']['packet'])
                queue.db.execute('INSERT INTO attempts(key,item,transport,status,calls,policy_revision,authority) '
                                 'VALUES(?,?,?,?,?,?,?)',
                                 ('attempt',row['id'],'graph','not-sent',0,1,'fixture'))
                self.assertFalse(self.ledger.tasks.mail_rebindable(row,'c'*64))
                self.assertEqual(automation.work(self.ledger.tasks,self.root)['mail_rebind'],[])
                with self.assertRaises(ValueError):self.ledger.tasks.review_binding(row)
                env={k:v for k,v in original['data']['review'].items() if k!='signature'}
                with self.assertRaisesRegex(ValueError,'delivery attempt or receipt'):
                    self.ledger.tasks.verdict(sign_review(env,'secret',kind='reply'),'secret')
            finally:queue.close()

    def test_stale_pending_mail_accepts_hold_and_bounce(self):
        for verdict,state in [('HOLD','held'),('BOUNCE','bounced')]:
            with self.subTest(verdict=verdict):
                row=self.add('mail-'+verdict);self.review(row,self.result(row));self.route()
                with patch('managed.control.workflow',return_value='c'*64):
                    row=self.ledger.tasks.get(row['id'])
                    env=sign_review({'binding':self.ledger.tasks.review_binding(row),
                                     'reviewer':'local-claude-loop','verdict':verdict,
                                     'reason':'Exact current result requires correction.',
                                     'evidence_ref':'fixture:mail-verdict'},'secret',kind='reply')
                    row=self.ledger.tasks.verdict(env,'secret')
                    self.assertEqual(row['state'],state)
                    self.assertEqual(self.ledger.tasks.verdict(env,'secret'),row)

    def test_rebound_pending_mail_still_holds_terminal_delivery_blocker(self):
        row=self.add();self.review(row,self.result(row));self.route();box=Mailbox()
        with patch('managed.control.workflow',return_value='c'*64):
            row=self.ledger.tasks.get(row['id'])
            env=sign_review({'binding':self.ledger.tasks.review_binding(row),
                             'reviewer':'local-claude-loop','verdict':'PASS',
                             'reason':'Checked exact unchanged mail.',
                             'evidence_ref':'fixture:mail-rebind'},'secret',kind='reply')
            self.ledger.tasks.verdict(env,'secret')
            blocker='A newer incoming message requires a new reply/review'
            with patch.object(box,'preflight',side_effect=GateError(blocker)):
                delivery.advance(self.ledger,self.cfg,self.root,box)
            row=self.ledger.tasks.get(row['id'])
            self.assertEqual(row['state'],'held')
            self.assertEqual(row['data']['hold_kind'],'delivery-terminal')
            self.assertEqual(row['data']['hold_reason'],blocker)
        self.assertFalse(box.calls);self.assertFalse(box.saved)

    def test_saved_owner_inventory_rebind_preserves_receipt_and_clears_blocker(self):
        row=self.add();self.review(row,self.result(row,route='owner'));self.route();box=Mailbox()
        delivery.advance(self.ledger,self.cfg,self.root,box)
        original=self.ledger.tasks.get(row['id'])
        with patch('managed.control.workflow',return_value='c'*64):
            delivery.advance(self.ledger,self.cfg,self.root,box)
            work=automation.work(self.ledger.tasks,self.root)
            self.assertEqual(work['owner_drafts'][0]['blocker'],'workflow-changed')
            item=work['owner_draft_rebind'][0]
            env=sign_review({'binding':item['binding'],'rebind_workflow_hash':item['rebind_workflow_hash'],
                             'reviewer':'local-claude-loop','verdict':'PASS',
                             'reason':'Checked saved owner draft against the current workflow.',
                             'evidence_ref':'fixture:saved-owner-rebind'},'secret',kind='reply')
            self.ledger.tasks.verdict(env,'secret')
            delivery.advance(self.ledger,self.cfg,self.root,box)
            work=automation.work(self.ledger.tasks,self.root)
        self.assertTrue(work['owner_drafts'][0]['review_current'])
        self.assertEqual(work['owner_draft_exceptions'],[])
        self.assertEqual(work['owner_draft_rebind'],[])
        row=self.ledger.tasks.get(row['id'])
        self.assertEqual(row['state'],'owner-draft')
        self.assertEqual(row['data']['draft_receipt'],original['data']['draft_receipt'])
        self.assertEqual(len(box.saved),1);self.assertFalse(box.calls)

    def test_pending_rebind_cannot_approve_a_changed_result(self):
        row=self.add();self.review(row,self.result(row));self.route()
        row=self.ledger.tasks.get(row['id'])
        row=self.ledger.tasks.update(row['id'],row['revision'],row['state'],
            {**row['data'],'result':{**row['data']['result'],'report':'Changed after review'}})
        env={k:v for k,v in row['data']['review'].items() if k!='signature'}
        with patch('managed.control.workflow',return_value='c'*64):
            env=sign_review({**env,'rebind_workflow_hash':'c'*64},'secret',kind='reply')
        with self.assertRaisesRegex(ValueError,'obsolete result'):
            self.ledger.tasks.verdict(env,'secret')

    def graph_reply(self):
        from test_reply_threading import DraftGraph
        row=self.add();result=self.result(row);self.review(row,result);self.route()
        graph=DraftGraph(result['packet'])
        box=mailbox.GraphMailbox(graph,{'account':result['packet']['sender'],'reconciled':True})
        return row,result,graph,box

    def mail_queue(self):
        from managed.queue import Queue
        return Queue(self.ledger.tasks.path,{k:workflow(k) for k in ('firstcontact','reply')},
                     reviewer='local-claude-loop',checkpoint=lambda:self.ledger.save(force=True))

    def strand_legacy_reply(self, row, result, graph):
        # Reproduce the old submit implementation: it created the MIME draft,
        # failed its conversation gate and only persisted the exception type.
        class LegacyChannel:
            name='microsoft-graph';kind='reply'
            def submit(inner,packet,key):
                graph.strand(key)
                from managed.queue import GateError
                raise GateError('Created reply is not in the original conversation')
        q=self.mail_queue()
        try:
            packet=result['packet'];q.add(row['id'],packet);binding=q.binding(row['id'])
            q.research_checked(binding,'fixture:research');q.enqueue(binding)
            q.verdict({'schema_version':1,**binding,'reviewer':'local-claude-loop','verdict':'PASS',
                       'reason':'Exact packet checked','evidence_ref':'fixture:independent-review'})
            q.set_policy('reply',1,Mailbox().preflight(packet,{}))
            from managed.queue import Authority
            q.dispatch(row['id'],LegacyChannel(),Authority('reply',packet['sender'],time.time()+60,'fixture:authority'))
            q.db.execute("UPDATE attempts SET locator='GateError' WHERE item=?",(row['id'],))
            q.db.execute("UPDATE items SET reason='GateError' WHERE id=?",(row['id'],))
            # Remove the new tracking flag to model an actual legacy snapshot.
            for event in q.events(row['id']):
                data=json.loads(event['data'])
                if event['action']=='send-intent':data.pop('tracks_send_call',None)
                if event['action']=='send-unknown':data['evidence']='GateError'
                q.db.execute('UPDATE events SET data=? WHERE seq=?',(canonical(data),event['seq']))
            self.ledger.save()
        finally:q.close()

    def test_failed_draft_is_not_sent_cleaned_and_retried_next_tick_under_same_pass(self):
        row,result,graph,box=self.graph_reply();graph.force_wrong_conversation=True
        review=self.ledger.tasks.get(row['id'])['data']['review']
        delivery.advance(self.ledger,self.cfg,self.root,box)
        q=self.mail_queue()
        try:
            attempt=dict(q.db.execute('SELECT * FROM attempts').fetchone());key=attempt['key']
            self.assertEqual(attempt['status'],'not-sent')
            self.assertEqual(q.get(row['id'])['state'],'not-sent')
            self.assertEqual(attempt['locator'],'Created reply is not in the original conversation')
            self.assertFalse(any(e['action']=='send-call' for e in q.events(row['id'])))
        finally:q.close()
        self.assertFalse(graph.drafts)
        self.assertFalse(any(c[0].endswith('/send') for c in graph.calls))
        self.assertEqual(self.ledger.tasks.get(row['id'])['data']['last_error'],
                         'Created reply is not in the original conversation')
        graph.force_wrong_conversation=False
        delivery.advance(self.ledger,self.cfg,self.root,box)
        delivery.advance(self.ledger,self.cfg,self.root,box)
        actual=self.ledger.tasks.get(row['id'])
        self.assertEqual(actual['state'],'verified');self.assertEqual(actual['data']['review'],review)
        self.assertEqual([str(m['X-Outreach-Attempt']) for m in graph.mimes],[key,key])
        self.assertEqual(sum(c[0].endswith('/send') for c in graph.calls),1)

    def test_legacy_wrong_conversation_draft_is_reclassified_deleted_and_retried(self):
        row,result,graph,box=self.graph_reply();self.strand_legacy_reply(row,result,graph)
        delivery.advance(self.ledger,self.cfg,self.root,box)
        self.assertNotIn('stranded',graph.drafts)
        q=self.mail_queue()
        try:
            events=q.events(row['id']);actions=[e['action'] for e in events]
            self.assertLess(actions.index('not-sent'),actions.index('draft-cleaned'))
            self.assertLess(actions.index('draft-cleaned'),actions.index('send-call'))
            evidence=json.loads(next(e['data'] for e in events if e['action']=='not-sent'))['evidence']
            self.assertIn('original conversation',evidence)
        finally:q.close()
        self.assertEqual(sum(c[1]=='DELETE' for c in graph.calls),1)
        self.assertEqual(sum(c[0].endswith('/send') for c in graph.calls),1)
        delivery.advance(self.ledger,self.cfg,self.root,box)
        self.assertEqual(self.ledger.tasks.get(row['id'])['state'],'verified')

    def test_send_call_timeout_never_retries_or_deletes_even_if_draft_remains(self):
        row,result,graph,box=self.graph_reply();graph.send_failure=TimeoutError('Lost /send response')
        self.ledger.save(force=True)  # Send markers must bypass this fresh checkpoint.
        original_request=graph.request
        def request_with_checkpoint(path,method='GET',body=None):
            if path.endswith('/send'):
                with tempfile.TemporaryDirectory() as temp:
                    recovered=Ledger(self.remote,temp)
                    try:
                        self.assertEqual(recovered.tasks.db.execute(
                            "SELECT count(*) FROM events WHERE action='send-call'").fetchone()[0],1)
                    finally:recovered.tasks.close()
            return original_request(path,method,body)
        graph.request=request_with_checkpoint
        delivery.advance(self.ledger,self.cfg,self.root,box)
        for _ in range(2):delivery.advance(self.ledger,self.cfg,self.root,box)
        q=self.mail_queue()
        try:
            self.assertEqual(q.get(row['id'])['state'],'send-unknown')
            self.assertEqual(q.db.execute('SELECT status FROM attempts').fetchone()[0],'unknown')
            self.assertEqual(sum(e['action']=='send-call' for e in q.events(row['id'])),1)
        finally:q.close()
        self.assertTrue(graph.drafts)
        self.assertEqual(sum(c[0].endswith('/send') for c in graph.calls),1)
        self.assertFalse(any(c[1]=='DELETE' for c in graph.calls))
        self.assertEqual(self.ledger.tasks.get(row['id'])['data']['last_error'],'Lost /send response')

    def test_full_gate_message_is_bounded_in_attempt_event_and_task(self):
        from managed.queue import GateError
        row,result,graph,box=self.graph_reply()
        reason='A specific failing draft gate: '+('detail '*60)
        with patch.object(box,'create_draft',side_effect=GateError(reason)):
            delivery.advance(self.ledger,self.cfg,self.root,box)
        self.assertEqual(self.ledger.tasks.get(row['id'])['data']['last_error'],reason[:300])
        q=self.mail_queue()
        try:
            attempt=q.db.execute('SELECT * FROM attempts').fetchone()
            self.assertEqual(attempt['status'],'not-sent');self.assertEqual(attempt['locator'],reason[:300])
            event=next(e for e in q.events(row['id']) if e['action']=='not-sent')
            self.assertEqual(json.loads(event['data'])['evidence'],reason[:300])
        finally:q.close()
        self.assertFalse(graph.mimes)

    def test_cleanup_failure_prevents_retry_until_deletion_is_confirmed(self):
        row,result,graph,box=self.graph_reply();graph.force_wrong_conversation=True
        graph.delete_failure=TimeoutError('Draft deletion response lost')
        for _ in range(2):delivery.advance(self.ledger,self.cfg,self.root,box)
        self.assertEqual(len(graph.mimes),1)
        self.assertTrue(graph.drafts)
        self.assertFalse(any(c[0].endswith('/send') for c in graph.calls))
        graph.delete_failure=None;graph.force_wrong_conversation=False
        delivery.advance(self.ledger,self.cfg,self.root,box)
        self.assertEqual(len(graph.mimes),2)
        self.assertEqual(sum(c[0].endswith('/send') for c in graph.calls),1)

    def test_retry_registers_missing_recipient_from_fresh_thread_evidence(self):
        row,result,graph,box=self.graph_reply();graph.force_wrong_conversation=True
        delivery.advance(self.ledger,self.cfg,self.root,box)
        self.tables('positive',{'entries':[]})
        self.tables('submissions',{'schema_version':1,'updated_on':'2026-11-02',
                    'scope':'independent-team','verification':{'mailbox_check':'saved-evidence-only'},'papers':[]})
        self.tables('contacts',{'contacts':[]});graph.force_wrong_conversation=False
        delivery.advance(self.ledger,self.cfg,self.root,box)
        self.assertEqual(len(graph.mimes),2)
        self.assertEqual(sum(c[0].endswith('/send') for c in graph.calls),1)
        contact=json.loads((self.root/'tables/contacts.json').read_text())['contacts'][0]
        self.assertEqual(contact['identity_evidence'],'mail-thread')

    def test_crash_before_send_marker_recovers_after_restoring_original_intent(self):
        class ProcessDeath(BaseException):pass
        row,result,graph,box=self.graph_reply();create=box.create_draft
        def crash_after_draft(packet,key):
            create(packet,key)
            raise ProcessDeath()
        with patch.object(box,'create_draft',side_effect=crash_after_draft),self.assertRaises(ProcessDeath):
            delivery.advance(self.ledger,self.cfg,self.root,box)
        self.ledger.tasks.close();self.folder=self.root/'restored';self.folder.mkdir()
        self.ledger=Ledger(self.remote,self.folder)
        delivery.advance(self.ledger,self.cfg,self.root,box)
        self.assertEqual(sum(c[1]=='DELETE' for c in graph.calls),1)
        self.assertEqual(sum(c[0].endswith('/send') for c in graph.calls),1)
        delivery.advance(self.ledger,self.cfg,self.root,box)
        self.assertEqual(self.ledger.tasks.get(row['id'])['state'],'verified')

    def test_crash_after_send_marker_never_retries_after_restoring_snapshot(self):
        class ProcessDeath(BaseException):pass
        row,result,graph,box=self.graph_reply();graph.send_failure=ProcessDeath()
        with self.assertRaises(ProcessDeath):delivery.advance(self.ledger,self.cfg,self.root,box)
        self.ledger.tasks.close();self.folder=self.root/'restored';self.folder.mkdir()
        self.ledger=Ledger(self.remote,self.folder)
        delivery.advance(self.ledger,self.cfg,self.root,box)
        q=self.mail_queue()
        try:self.assertEqual(q.get(row['id'])['state'],'send-unknown')
        finally:q.close()
        self.assertEqual(sum(c[0].endswith('/send') for c in graph.calls),1)
        self.assertFalse(any(c[1]=='DELETE' for c in graph.calls))

    def test_legacy_recovery_preserves_edited_draft_and_same_conversation_unknown(self):
        row,result,graph,box=self.graph_reply();self.strand_legacy_reply(row,result,graph)
        original=copy.deepcopy(graph.drafts['stranded'])
        for draft in [{**original,'body':{'contentType':'text','content':'Avery edited this'}},
                      {**original,'conversationId':result['packet']['thread_id']},
                      {**original,'isDraft':False}]:
            graph.drafts['stranded']=draft
            delivery.advance(self.ledger,self.cfg,self.root,box)
            q=self.mail_queue()
            try:self.assertEqual(q.get(row['id'])['state'],'send-unknown')
            finally:q.close()
        self.assertFalse(any(c[1]=='DELETE' or c[0].endswith('/send') for c in graph.calls))

    def test_legacy_recovery_waits_for_unpause_before_cleanup_or_retry(self):
        row,result,graph,box=self.graph_reply();self.strand_legacy_reply(row,result,graph)
        delivery.advance(self.ledger,{**self.cfg,'paused':True},self.root,box)
        self.assertIn('stranded',graph.drafts)
        q=self.mail_queue()
        try:self.assertEqual(q.get(row['id'])['state'],'not-sent')
        finally:q.close()
        self.assertFalse(any(c[1]=='DELETE' or c[0].endswith('/send') for c in graph.calls))
        delivery.advance(self.ledger,self.cfg,self.root,box)
        self.assertNotIn('stranded',graph.drafts)
        self.assertEqual(sum(c[0].endswith('/send') for c in graph.calls),1)

    def test_sent_copy_appearing_after_not_sent_proof_prevents_cleanup_and_retry(self):
        row,result,graph,box=self.graph_reply();self.strand_legacy_reply(row,result,graph)
        delivery.advance(self.ledger,{**self.cfg,'paused':True},self.root,box)
        # The owner sends the stranded copy while automatic writes are paused.
        sent=graph.drafts.pop('stranded')
        graph.sent['stranded']={**sent,'isDraft':False,'sentDateTime':'2026-11-01T06:00:00Z'}
        delivery.advance(self.ledger,self.cfg,self.root,box)
        self.assertEqual(self.ledger.tasks.get(row['id'])['state'],'verified')
        self.assertFalse(graph.mimes)
        self.assertFalse(any(c[1]=='DELETE' or c[0].endswith('/send') for c in graph.calls))

    def test_timeout_and_restart_reconcile_original_send_even_when_paused(self):
        row=self.add();self.review(row,self.result(row));self.route();box=Mailbox();box.lose_send=True
        delivery.advance(self.ledger,self.cfg,self.root,box)
        self.assertEqual(self.ledger.tasks.db.execute('SELECT status FROM attempts').fetchone()[0],'unknown')
        self.ledger.tasks.close();self.folder=self.root/'restored';self.folder.mkdir();self.ledger=Ledger(self.remote,self.folder)
        delivery.advance(self.ledger,{**self.cfg,'paused':True},self.root,box)
        self.assertEqual(self.ledger.tasks.get(row['id'])['state'],'verified');self.assertEqual(len(box.calls),1)

    def test_checkpoint_failure_prevents_any_mail_write(self):
        row=self.add();self.review(row,self.result(row));self.route();box=Mailbox()
        self.ledger.save()
        def fail(**kwargs):raise TimeoutError('Snapshot unavailable')
        with patch.object(self.ledger,'save',side_effect=fail),self.assertRaises(TimeoutError):
            delivery.advance(self.ledger,self.cfg,self.root,box)
        self.assertFalse(box.calls)

    def test_owner_draft_intent_is_forced_before_save_and_recovers_same_key(self):
        row=self.add();result=self.result(row,route='owner');self.review(row,result);self.route()
        self.ledger.save(force=True);box=Mailbox();save=box.save_draft
        def interrupted(packet,key,recovery=False):
            with tempfile.TemporaryDirectory() as folder:
                saved=Ledger(self.remote,folder)
                try:self.assertEqual(saved.tasks.get(row['id'])['data']['draft_intent']['key'],key)
                finally:saved.tasks.close()
            save(packet,key,recovery)
            raise KeyboardInterrupt('Draft saved; process killed before receipt checkpoint')
        box.save_draft=interrupted
        with self.assertRaises(KeyboardInterrupt):delivery.advance(self.ledger,self.cfg,self.root,box)
        self.ledger.tasks.close();self.folder=self.root/'draft-restored';self.folder.mkdir()
        self.ledger=Ledger(self.remote,self.folder);box.save_draft=save
        delivery.advance(self.ledger,self.cfg,self.root,box)
        self.assertEqual(box.saved[0][1],box.saved[1][1]);self.assertTrue(box.saved[1][2])
        self.assertEqual(self.ledger.tasks.get(row['id'])['data']['draft_receipt']['remote_id'],'draft-id')

    def uncertain_owner_draft(self, age):
        from test_reply_threading import DraftGraph
        row=self.add();result=self.result(row,route='owner');self.review(row,result);self.route()
        self.now=time.time();self.ledger.tasks.clock=lambda:self.now
        intent={'key':'outage-attempt','at':self.now-age}
        row=self.ledger.tasks.get(row['id'])
        row=self.ledger.tasks.update(row['id'],row['revision'],'owner-draft',
                                     {**row['data'],'draft_intent':intent})
        graph=DraftGraph(result['packet'])
        box=mailbox.GraphMailbox(graph,{'account':result['packet']['sender'],'reconciled':True},
                                 clock=lambda:self.now)
        return row,result,graph,box,intent

    def test_uncertain_owner_draft_refuses_through_settle_period(self):
        row,result,graph,box,intent=self.uncertain_owner_draft(0)
        for age in (0,delivery.DRAFT_INTENT_SETTLE_SECONDS-1,delivery.DRAFT_INTENT_SETTLE_SECONDS):
            with self.subTest(age=age):
                self.now=intent['at']+age
                delivery.advance(self.ledger,self.cfg,self.root,box)
                data=self.ledger.tasks.get(row['id'])['data']
                self.assertIn('Draft outcome unknown',data['last_error'])
                self.assertEqual(data['draft_intent'],intent)
                self.assertNotIn('draft_intent_expired',data)
                self.assertNotIn('draft_receipt',data)
                self.assertFalse(graph.mimes)

    def test_uncertain_owner_draft_expires_with_new_key_and_durable_absence_evidence(self):
        row,result,graph,box,intent=self.uncertain_owner_draft(delivery.DRAFT_INTENT_SETTLE_SECONDS+1)
        create=box.create_draft
        def preserved(packet,key):
            self.assertNotEqual(key,intent['key'])
            with tempfile.TemporaryDirectory() as folder:
                saved=Ledger(self.remote,folder)
                try:
                    data=saved.tasks.get(row['id'])['data']
                    self.assertEqual(data['draft_intent'],{'key':key,'at':self.now})
                    self.assertEqual(data['draft_intent_expired'],[
                        {**intent,'confirmed_at':self.now,'replacement_key':key}])
                finally:saved.tasks.close()
            return create(packet,key)
        with patch.object(box,'find',wraps=box.find) as find, \
                patch.object(box,'create_draft',side_effect=preserved):
            delivery.advance(self.ledger,self.cfg,self.root,box)
        self.assertIn(unittest.mock.call('drafts',intent['key']),find.call_args_list)
        self.assertIn(unittest.mock.call('sentitems',intent['key']),find.call_args_list)
        data=self.ledger.tasks.get(row['id'])['data'];key=data['draft_intent']['key']
        self.assertEqual(str(graph.mimes[0]['X-Outreach-Attempt']),key)
        self.assertEqual(data['draft_receipt']['attempt_key'],key)
        self.assertNotIn('last_error',data)
        self.assertFalse(any(c[1]=='DELETE' or c[0].endswith('/send') for c in graph.calls))

    def test_uncertain_owner_draft_lookup_errors_never_establish_absence(self):
        row,result,graph,box,intent=self.uncertain_owner_draft(delivery.DRAFT_INTENT_SETTLE_SECONDS+1)
        find=box.find
        for folder in ('drafts','sentitems'):
            with self.subTest(folder=folder):
                def fail(selected,key):
                    if selected==folder:raise TimeoutError('Folder lookup unavailable')
                    return find(selected,key)
                with patch.object(box,'find',side_effect=fail):
                    delivery.advance(self.ledger,self.cfg,self.root,box)
                data=self.ledger.tasks.get(row['id'])['data']
                self.assertEqual(data['draft_intent'],intent)
                self.assertNotIn('draft_intent_expired',data)
                self.assertNotIn('draft_receipt',data)
                self.assertIn('Folder lookup unavailable',data['last_error'])
                self.assertFalse(graph.mimes)

    def test_expired_owner_draft_recovery_after_lost_receipt_uses_replacement_key(self):
        row,result,graph,box,intent=self.uncertain_owner_draft(delivery.DRAFT_INTENT_SETTLE_SECONDS+1)
        save=box.save_draft
        def interrupted(packet,key,recovery=False):
            save(packet,key,recovery)
            raise KeyboardInterrupt('Replacement saved; receipt checkpoint lost')
        with patch.object(box,'save_draft',side_effect=interrupted):
            with self.assertRaises(KeyboardInterrupt):
                delivery.advance(self.ledger,self.cfg,self.root,box)
        replacement=self.ledger.tasks.get(row['id'])['data']['draft_intent']
        self.ledger.tasks.close();self.folder=self.root/'expiry-restored';self.folder.mkdir()
        self.ledger=Ledger(self.remote,self.folder);self.ledger.tasks.clock=lambda:self.now
        delivery.advance(self.ledger,self.cfg,self.root,box)
        data=self.ledger.tasks.get(row['id'])['data']
        self.assertEqual(data['draft_intent'],replacement)
        self.assertEqual(data['draft_receipt']['attempt_key'],replacement['key'])
        self.assertEqual(len(graph.mimes),1)

    def test_expired_owner_draft_checkpoint_failure_prevents_replacement_create(self):
        row,result,graph,box,intent=self.uncertain_owner_draft(delivery.DRAFT_INTENT_SETTLE_SECONDS+1)
        save=self.ledger.save
        def fail(**kwargs):
            if kwargs.get('force') and self.ledger.tasks.get(row['id'])['data'].get('draft_intent_expired'):
                raise TimeoutError('Expiry checkpoint unavailable')
            return save(**kwargs)
        with patch.object(self.ledger,'save',side_effect=fail):
            delivery.advance(self.ledger,self.cfg,self.root,box)
        data=self.ledger.tasks.get(row['id'])['data']
        self.assertIn('Expiry checkpoint unavailable',data['last_error'])
        self.assertNotIn('draft_receipt',data)
        self.assertFalse(graph.mimes)

    def test_uncertain_owner_draft_recovers_old_copy_even_after_settle_period(self):
        row,result,graph,box,intent=self.uncertain_owner_draft(delivery.DRAFT_INTENT_SETTLE_SECONDS+1)
        draft=graph.strand(intent['key'],wrong=False)
        delivery.advance(self.ledger,self.cfg,self.root,box)
        data=self.ledger.tasks.get(row['id'])['data']
        self.assertEqual(data['draft_receipt']['remote_id'],draft['id'])
        self.assertEqual(data['draft_intent'],intent)
        self.assertNotIn('draft_intent_expired',data)
        self.assertFalse(graph.mimes)

    def test_uncertain_owner_draft_recognises_old_key_sent_without_creating_again(self):
        row,result,graph,box,intent=self.uncertain_owner_draft(delivery.DRAFT_INTENT_SETTLE_SECONDS+1)
        sent=graph.strand(intent['key'],wrong=False);graph.drafts.clear()
        graph.sent[sent['id']]={**sent,'isDraft':False,'sentDateTime':'2026-11-14T01:00:00Z'}
        delivery.advance(self.ledger,self.cfg,self.root,box)
        current=self.ledger.tasks.get(row['id']);data=current['data']
        self.assertEqual(current['state'],'verified')
        self.assertEqual(data['sent_receipt']['idempotency_key'],intent['key'])
        self.assertEqual(data['sent_receipt']['remote_id'],sent['id'])
        self.assertNotIn('draft_intent_expired',data)
        self.assertFalse(graph.mimes)

    def test_expired_owner_draft_late_copy_is_visible_and_neither_copy_is_deleted(self):
        row,result,graph,box,intent=self.uncertain_owner_draft(delivery.DRAFT_INTENT_SETTLE_SECONDS+1)
        delivery.advance(self.ledger,self.cfg,self.root,box)
        receipt=self.ledger.tasks.get(row['id'])['data']['draft_receipt']
        late=graph.strand(intent['key'],wrong=False)
        late['body']['content']='Owner edited the delayed draft'
        delivery.advance(self.ledger,self.cfg,self.root,box)
        data=self.ledger.tasks.get(row['id'])['data']
        self.assertIn('Expired draft attempt reappeared',data['last_error'])
        self.assertIn(late['id'],data['last_error'])
        self.assertEqual(data['draft_receipt'],receipt)
        self.assertEqual(set(graph.drafts),{late['id'],receipt['remote_id']})
        self.assertFalse(any(c[1]=='DELETE' or c[0].endswith('/send') for c in graph.calls))
        with patch('managed.daily_local.status',return_value={}):
            inventory=automation.work(self.ledger.tasks,self.root)
        draft=next(item for item in inventory['owner_drafts'] if item['task_id']==row['id'])
        self.assertEqual(draft['blocker'],'draft-intent-reconciliation-failed')
        self.assertEqual(draft['last_error'],data['last_error'])
        # Once the owner resolves the duplicate, a fresh check clears the error.
        del graph.drafts[late['id']]
        delivery.advance(self.ledger,self.cfg,self.root,box)
        self.assertNotIn('last_error',self.ledger.tasks.get(row['id'])['data'])

    def test_pause_and_missing_activation_never_send(self):
        row=self.add();self.review(row,self.result(row));self.route();box=Mailbox()
        delivery.advance(self.ledger,{**self.cfg,'paused':True},self.root,box)
        self.cfg['automation']['automatic_mail']=False
        delivery.advance(self.ledger,self.cfg,self.root,box)
        self.assertFalse(box.calls)

    def test_automatic_send_holds_for_another_owner_draft_task(self):
        owner=self.add('owner-draft-conflict');self.review(owner,self.result(owner,route='owner'))
        current=self.add('automatic-conflict',user_decision_ever=True);self.review(current,self.result(current));self.route()
        box=GuardMailbox()
        delivery.advance(self.ledger,self.cfg,self.root,box)
        row=self.ledger.tasks.get(current['id'])
        self.assertEqual(row['state'],'held')
        self.assertEqual(row['data']['reason'],delivery.SEND_GUARD_REASON)
        self.assertEqual(row['data']['conflicting_task_ids'],[owner['id']])
        self.assertEqual(row['data']['conflicting_draft_ids'],['draft-id'])  # still unsent in Drafts
        self.assertFalse(box.calls)

    def test_sent_or_deleted_owner_draft_no_longer_blocks(self):
        owner=self.add('owner-draft-sent');self.review(owner,self.result(owner,route='owner'))
        current=self.add('automatic-after-sent');self.review(current,self.result(current));self.route()
        box=GuardMailbox(keep_saved=False)  # Avery already sent (or deleted) the saved draft
        delivery.advance(self.ledger,self.cfg,self.root,box)
        self.assertEqual(self.ledger.tasks.get(owner['id'])['state'],'owner-draft')
        self.assertEqual(len(box.calls),1)

    def test_automatic_send_holds_for_another_mailbox_draft(self):
        row=self.add('mailbox-draft-conflict',user_decision_ever=True);self.review(row,self.result(row));self.route()
        row=self.ledger.tasks.get(row['id'])
        packet=row['data']['result']['packet']
        box=GuardMailbox([{'id':'web-draft-1','isDraft':True,
                           'from':{'emailAddress':{'address':packet['sender']}},
                           'toRecipients':[{'emailAddress':{'address':packet['to'][0]}}],
                           'ccRecipients':[]}])
        delivery.advance(self.ledger,self.cfg,self.root,box)
        current=self.ledger.tasks.get(row['id'])
        self.assertEqual(current['state'],'held')
        self.assertEqual(current['data']['conflicting_task_ids'],[])
        self.assertEqual(current['data']['conflicting_draft_ids'],['web-draft-1'])
        self.assertFalse(box.calls)

    def test_automatic_guard_ignores_this_task_attempt_draft(self):
        row=self.add('own-attempt-draft');self.review(row,self.result(row));self.route()
        row=self.ledger.tasks.get(row['id'])
        packet=row['data']['result']['packet']
        box=GuardMailbox([{'id':'own-draft','isDraft':True,
                           'internetMessageHeaders':[{'name':'X-Outreach-Attempt','value':'own-key'}],
                           'from':{'emailAddress':{'address':packet['sender']}},
                           'toRecipients':[{'emailAddress':{'address':packet['to'][0]}}],
                           'ccRecipients':[]}])
        class Database:
            def execute(self,*args):
                class Result:
                    def fetchone(self):
                        class Attempt:
                            def __getitem__(self,key):return 'own-key'
                        return Attempt()
                return Result()
        queue=type('QueueProxy',(),{'db':Database()})()
        self.assertEqual(delivery._send_conflicts(self.ledger.tasks,box,
                         self.ledger.tasks.get(row['id']),packet,queue),
                         {'task_ids':[],'draft_ids':[]})

    def test_automatic_guard_allows_a_clear_recipient(self):
        row=self.add('clear-recipient');self.review(row,self.result(row));self.route()
        box=GuardMailbox()
        delivery.advance(self.ledger,self.cfg,self.root,box)
        self.assertEqual(len(box.calls),1)

    def test_duplicate_letter_in_another_task_cannot_send_twice(self):
        for name in ['one','two']:
            row=self.add(name);self.review(row,self.result(row))
        self.route();box=Mailbox();delivery.advance(self.ledger,self.cfg,self.root,box)
        self.assertEqual(len(box.calls),1)

    def test_different_results_for_one_scholar_do_not_send_two_first_contacts(self):
        self.cfg['kinds']['firstcontact']=True
        for name,problem in [('first-a','problem-a'),('first-b','problem-b')]:
            req=request(name,'contact-a','firstcontact')
            req['inputs']={'automation_contract':1,'recipient_emails':['author@example.invalid']}
            row=self.ledger.tasks.add(req)
            result=self.result(row)
            result['packet']=json.loads((Path(__file__).parent/'fixtures/letter.json').read_text())
            result['packet']['problem_ids']=[problem]
            with patch('managed.control.ROOT',self.root), \
                    patch('managed.control.workflow',return_value=req['workflow_hash']):
                self.review(row,result)
        self.route();box=Mailbox();delivery.advance(self.ledger,self.cfg,self.root,box)
        self.assertEqual(len(box.calls),1)

    def test_packet_decision_history_does_not_force_owner_route(self):
        row=self.add();result=self.result(row);result['packet']['user_decision_ever']=True
        self.review(row,result);self.route();box=Mailbox()
        self.assertEqual(self.ledger.tasks.get(row['id'])['state'],'awaiting-mailbox')
        delivery.advance(self.ledger,self.cfg,self.root,box)
        delivery.advance(self.ledger,self.cfg,self.root,box)
        self.assertEqual(self.ledger.tasks.get(row['id'])['state'],'verified')
        self.assertEqual(len(box.calls),1);self.assertFalse(box.saved)

    def test_decision_history_thread_uses_this_letters_reviewed_route(self):
        for route,expected in [('auto','awaiting-mailbox'),('owner','owner-draft')]:
            with self.subTest(route=route):
                row=self.add('history-'+route,user_decision_ever=True)
                result=self.result(row,route=route)
                self.assertTrue(result['packet']['user_decision_ever'])
                self.review(row,result);self.route();box=Mailbox()
                self.assertEqual(self.ledger.tasks.get(row['id'])['state'],expected)
                delivery.advance(self.ledger,self.cfg,self.root,box)
                delivery.advance(self.ledger,self.cfg,self.root,box)
                final=self.ledger.tasks.get(row['id'])
                self.assertTrue(final['request']['inputs']['user_decision_ever'])
                self.assertTrue(final['data']['result']['packet']['user_decision_ever'])
                self.assertEqual(len(box.calls),int(route=='auto'))
                self.assertEqual(len(box.saved),int(route=='owner'))
                self.assertEqual(final['state'],'verified' if route=='auto' else 'owner-draft')

    def test_actual_user_answer_produces_new_reviewed_academic_letter(self):
        row=self.add();questions=[{'id':'meeting','question':'是否接受会面？','source_quote':'Could we meet?',
                                  'evidence_ref':'fixture:complete-latest-message'}]
        result=self.result(row,'needs-decision','owner',questions)
        self.review(row,result);self.route()
        pending=automation.work(self.ledger.tasks,self.root)['user_questions'][0]
        self.assertEqual(pending['questions'],questions)
        env={'schema_version':1,'task_id':row['id'],'result_hash':pending['result_hash'],
             'answers':{'meeting':'本次婉拒会议，继续邮件交流。'},'evidence_ref':'fixture:actual-user-reply',
             'reviewer':'local-claude-loop'}
        env['signature']=hmac.new(b'secret',canonical(env).encode(),hashlib.sha256).hexdigest()
        with self.assertRaisesRegex(ValueError,'signature'):self.ledger.tasks.decision(env,'wrong')
        self.ledger.tasks.decision(env,'secret');self.continue_work();self.continue_work()
        self.ledger.tasks.decision(env,'secret')
        child=next(r for r in self.ledger.tasks.all() if r['id']!=row['id'])
        self.assertTrue(child['request']['inputs']['user_decision_ever'])
        self.assertEqual(child['request']['inputs']['user_answers'],[env])
        automation.parent_gate(child['request'],self.ledger.tasks)
        result=self.result(child);result['packet']['user_decision_ever']=True
        self.assertEqual(result['routing']['mail'],'auto')  # Defense against a mistaken CMA route.
        self.review(child,result);self.route();box=Mailbox();delivery.advance(self.ledger,self.cfg,self.root,box)
        self.assertEqual(self.ledger.tasks.get(child['id'])['state'],'awaiting-mailbox')
        delivery.advance(self.ledger,self.cfg,self.root,box)
        self.assertEqual(len(box.calls),1);self.assertFalse(box.saved)

    def test_decision_answers_allow_academic_bounced_revision_with_auto_route(self):
        req=request('answered-reply','contact-a','reply')
        req['inputs']={'automation_contract':1,'user_decision_ever':True,
                       'user_answers':[{'answers':{'meeting':'Decline this meeting.'}}]}
        row=self.ledger.tasks.add(req)
        self.review(row,self.result(row),'BOUNCE');self.continue_work()
        child=self.ledger.tasks.all()[-1]
        self.assertEqual(child['request']['inputs']['automation_parent']['purpose'],'revision')
        self.assertEqual(child['request']['inputs']['user_answers'],req['inputs']['user_answers'])
        self.review(child,self.result(child));self.route();box=Mailbox()
        self.assertEqual(self.ledger.tasks.get(child['id'])['state'],'awaiting-mailbox')
        delivery.advance(self.ledger,self.cfg,self.root,box)
        self.assertEqual(len(box.calls),1);self.assertFalse(box.saved)

    def test_delivery_rechecks_decision_continuation_before_enqueue(self):
        req=request('answered-reply','contact-a','reply')
        req['inputs']={'automation_contract':1,'user_answers':[{'answers':{'meeting':'Decline.'}}]}
        row=self.ledger.tasks.add(req)
        self.review(row,self.result(row))
        # Simulate an incorrectly staged state; delivery must inspect task inputs.
        row=self.ledger.tasks.get(row['id'])
        self.ledger.tasks.update(row['id'],row['revision'],'awaiting-mailbox',row['data'])
        box=Mailbox();delivery.advance(self.ledger,self.cfg,self.root,box)
        self.assertEqual(len(box.calls),1);self.assertFalse(box.saved)
        row=self.ledger.tasks.get(row['id'])
        self.assertNotIn('last_error',row['data'])

    def test_bounce_revises_once_with_feedback_and_preserves_owner(self):
        row=self.add();result=self.result(row,route='owner')
        self.review(row,result,'BOUNCE');self.continue_work();self.continue_work()
        self.assertEqual(len(self.ledger.tasks.all()),2)
        child=self.ledger.tasks.all()[-1]
        self.assertTrue(child['request']['inputs']['user_decision_ever'])
        self.assertEqual(child['request']['inputs']['review_feedback']['verdict'],'BOUNCE')
        self.cfg['automation']['max_revisions']=1
        self.review(child,self.result(child),'BOUNCE');self.continue_work()
        self.assertEqual(len(self.ledger.tasks.all()),2)
        self.assertTrue(automation.work(self.ledger.tasks,self.root)['follow_up'])

    def test_agent_hold_review_inventory_and_bounce_continue_exactly_once(self):
        row=self.add();result=self.result(row,outcome='hold',route='none')
        result['report']='Oracle route is wrong; use the repaired broker route.'
        validate_result(result,row)
        row=self.ledger.tasks.update(row['id'],row['revision'],'held',{'result':result})
        self.continue_work()  # Holds never release themselves.
        self.assertEqual(len(self.ledger.tasks.all()),1)
        reviews=automation.work(self.ledger.tasks,self.root)['reviews']
        self.assertEqual(reviews,self.ledger.tasks.reviews())
        self.assertEqual(reviews[0]['result'],result)
        env={'binding':reviews[0]['binding'],'reviewer':'local-claude-loop','verdict':'BOUNCE',
             'reason':'Route repaired; repeat the required Oracle review.', 'evidence_ref':'fixture:route-fix'}
        env['signature']=hmac.new(b'secret',canonical(env).encode(),hashlib.sha256).hexdigest()
        self.ledger.tasks.verdict(env,'secret')
        self.continue_work();self.continue_work()
        rows=self.ledger.tasks.all();self.assertEqual(len(rows),2)
        parent=self.ledger.tasks.get(row['id']);child=rows[-1]
        self.assertEqual(parent['state'],'revising')
        self.assertEqual(parent['data']['continuation_id'],child['id'])
        self.assertEqual(child['request']['inputs']['previous_result'],result)
        self.assertEqual(child['request']['inputs']['review_feedback'],env)
        self.assertEqual(child['request']['inputs']['review_revisions'],1)
        self.assertEqual(child['request']['workflow_hash'],workflow('reply'))
        self.assertEqual(child['request']['source_revision'],'b'*40)
        automation.parent_gate(child['request'],self.ledger.tasks)
        # Replayed signed BOUNCE and reconciliation still cannot duplicate work.
        self.ledger.tasks.verdict(env,'secret');self.continue_work()
        self.assertEqual(len(self.ledger.tasks.all()),2)

    def guard_hold(self, row, response_id='response-1', controls=None, state='held'):
        result=self.result(row,outcome='hold',route='none')
        result['report']='Research complete; validate the amended PDF package only.'
        return self.ledger.tasks.update(row['id'],row['revision'],state,{
            'result':result,'response_id':'response-1',
            'runtime_guard':{'response_id':response_id,'controls':controls if controls is not None else
                             [{'kind':'steer','status':'admitted'}]}})

    def test_guard_hold_without_checkpoint_continues_with_redo_and_preserves_inputs(self):
        req=request('guard-parent','contact-a','reply')
        req['inputs']={'automation_contract':1,'review_revisions':3,'revision_number':4,
                       'user_decision_ever':True}
        row=self.ledger.tasks.add(req)
        row=self.guard_hold(row)
        self.continue_work()
        parent=self.ledger.tasks.get(row['id'])
        child=next(r for r in self.ledger.tasks.all() if r['id']!=row['id'])
        inputs=child['request']['inputs']
        self.assertEqual(parent['state'],'revising')
        self.assertEqual(parent['data']['continuation_id'],child['id'])
        self.assertEqual(inputs['guard_continuations'],1)
        self.assertEqual(inputs['review_revisions'],3)
        self.assertEqual(inputs['revision_number'],5)
        self.assertTrue(inputs['user_decision_ever'])
        self.assertEqual(inputs['previous_result'],row['data']['result'])
        self.assertEqual(inputs['checkpoint_artifacts'],[])
        self.assertEqual(inputs['automation_parent'],{
            'task_id':row['id'],'result_hash':digest(row['data']['result']),'purpose':'guard-continuation'})
        self.assertEqual(inputs['review_feedback'],{
            'verdict':'CONTINUE','reviewer':'runtime-guard',
            'reason':'The runtime guard stopped the previous turn. The parent has no preserved checkpoint artifacts. Redo the missing work from the primary source in this worktree. Use previous_result as context. Missing files from the parent are not a reason to return hold. Return the final task JSON.'})
        self.assertEqual(automation.work(self.ledger.tasks,self.root)['guard_continuations_without_checkpoint'],
                         [{'task_id':child['id'],'parent_id':row['id']}])
        legacy=copy.deepcopy(child)
        del legacy['request']['inputs']['checkpoint_artifacts']
        self.assertIn('Redo any work you cannot restore from those references using the primary source',
                      control.task_prompt(legacy))
        self.assertEqual(child['request']['source_revision'],'b'*40)
        automation.parent_gate(child['request'],self.ledger.tasks)
        self.continue_work()
        self.assertEqual(len(self.ledger.tasks.all()),2)
        # The allowance travels with each continuation: a second stop continues
        # again until GUARD_CONTINUATIONS is spent, then stays held for review.
        current=child
        for count in range(2,automation.GUARD_CONTINUATIONS+1):
            self.guard_hold(current)
            self.continue_work()
            current=next(r for r in self.ledger.tasks.all()
                         if r['request']['inputs'].get('guard_continuations')==count)
            automation.parent_gate(current['request'],self.ledger.tasks)
        total=len(self.ledger.tasks.all())
        self.guard_hold(current)
        self.continue_work()
        self.assertEqual(len(self.ledger.tasks.all()),total)
        self.assertEqual(self.ledger.tasks.get(current['id'])['state'],'held')

    def test_guard_hold_requires_matching_admitted_steer_and_eligible_state_kind(self):
        cases=[({},'response-1',[], 'held','reply'),
               ({},'response-1',[{'kind':'steer','status':'pending'}],'held','reply'),
               ({},'response-1',[{'kind':'cancel','status':'admitted'}],'held','reply'),
               ({},'other-response',[{'kind':'steer','status':'admitted'}],'held','reply'),
               ({'guard_continuations':3},'response-1',None,'held','reply'),
               ({},'response-1',None,'failed','reply'),
               ({},'response-1',None,'held','daily')]
        for index,(inputs,response,controls,state,kind) in enumerate(cases):
            req=request('ineligible-guard-'+str(index),'contact-a',kind)
            req['inputs']=inputs
            row=self.ledger.tasks.add(req)
            self.guard_hold(row,response,controls,state)
        self.continue_work()
        self.assertEqual(len(self.ledger.tasks.all()),len(cases))
        self.assertFalse(any(r['data'].get('continuation_id') for r in self.ledger.tasks.all()))

    def test_guard_hold_in_a_later_response_of_the_steered_agent_continues(self):
        req=request('guard-later-response','contact-a','shortnote')
        row=self.ledger.tasks.add(req)
        receipt={'control':{'receipt':{'agent_id':'agt-1'}}}
        row=self.guard_hold(row,'earlier-response',[{'kind':'steer','status':'admitted','receipt':receipt}])
        row=self.ledger.tasks.update(row['id'],row['revision'],'held',{**row['data'],'agent_id':'agt-1'})
        self.continue_work()
        self.assertTrue(self.ledger.tasks.get(row['id'])['data'].get('continuation_id'))
        other=self.ledger.tasks.add(request('guard-other-agent','contact-a','shortnote'))
        other=self.guard_hold(other,'earlier-response',[{'kind':'steer','status':'admitted','receipt':receipt}])
        other=self.ledger.tasks.update(other['id'],other['revision'],'held',{**other['data'],'agent_id':'agt-2'})
        self.continue_work()
        self.assertFalse(self.ledger.tasks.get(other['id'])['data'].get('continuation_id'))

    def test_work_omits_guard_continuable_holds_from_reviews(self):
        fresh=self.ledger.tasks.add(request('guard-fresh','contact-a','shortnote'))
        self.guard_hold(fresh)
        spent=request('guard-spent','contact-a','shortnote');spent['inputs']={'guard_continuations':3}
        spent=self.ledger.tasks.add(spent)
        self.guard_hold(spent)
        listed={item['binding']['task_id'] for item in automation.work(self.ledger.tasks,self.root)['reviews']}
        self.assertNotIn(fresh['id'],listed)
        self.assertIn(spent['id'],listed)

    def test_work_excludes_coordinator_holds_from_reviews(self):
        for name,data in [('connector',{'reason':'Connector failure'}),
                          ('workflow',{'reason':'Workflow changed; stage a new request revision'}),
                          ('repair',{'reason':'Completed CMA result needs repair','response_text':'invalid'})]:
            row=self.add(name)
            self.ledger.tasks.update(row['id'],row['revision'],'held',data)
        work=automation.work(self.ledger.tasks,self.root)
        self.assertFalse(work['reviews'])
        self.assertEqual(len(work['follow_up']),3)

    def test_repin_review_continuation_keeps_parent_gate_and_owner_constraints(self):
        row=self.add();self.review(row,self.result(row,route='owner'),'BOUNCE');self.continue_work()
        child=self.ledger.tasks.all()[-1]
        for current in ['b'*64,'c'*64]:
            with patch('managed.control.workflow',return_value=current):
                advance(self.ledger,CMA(),self.cfg,self.folder,revision='c'*40)
            old=self.ledger.tasks.get(child['id'])
            child=self.ledger.tasks.get(old['data']['successor'])
            self.assertEqual(child['request']['inputs'],old['request']['inputs'])
            self.assertTrue(child['request']['inputs']['user_decision_ever'])
            self.assertEqual(self.ledger.tasks.get(row['id'])['data']['continuation_id'],child['id'])
            automation.parent_gate(child['request'],self.ledger.tasks)
            self.continue_work()
        self.assertEqual(len(self.ledger.tasks.all()),4)

    def test_decline_needs_no_reply_but_independent_manuscript_starts_after_review(self):
        row=self.add();paper={'id':'independent-result','title':'A checked result','authors':['Blaire Mercer','Avery Reed'],
                             'target_journal':'Oracle positioning pending','source_quote':'Please publish independently.',
                             'message_id':'<inbound-1>','artifact_names':['note.tex']}
        result=self.result(row,'no-reply','none',paper=paper)
        result['artifacts']=[{'release_tag':'outreach-task-'+row['id'],'name':'note.tex','bytes':10,'sha256':'a'*64,'role':'note-tex'}]
        staged=self.ledger.tasks.update(row['id'],row['revision'],'awaiting-review',{'result':result})
        self.continue_work();self.assertEqual(len(self.ledger.tasks.all()),1)
        # Reset only this fixture's staging, then use the actual signed review path.
        self.review(staged,result);self.route();self.continue_work();self.continue_work()
        self.assertEqual(self.ledger.tasks.get(row['id'])['state'],'no-reply')
        child=self.ledger.tasks.all()[-1]
        self.assertEqual(child['kind'],'submission');self.assertEqual(len(self.ledger.tasks.all()),2)
        automation.parent_gate(child['request'],self.ledger.tasks)
        box=Mailbox();delivery.advance(self.ledger,self.cfg,self.root,box);self.assertFalse(box.calls)

    def test_submission_cannot_advance_without_actual_package_roles(self):
        row=self.add(kind='submission');result=self.result(row,route='none')
        with self.assertRaisesRegex(ValueError,'needs source'):validate_result(result,row)
        for role in ['manuscript-pdf','manuscript-tex','source-archive','evidence']:
            result['artifacts'].append({'release_tag':'outreach-task-'+row['id'],'name':role,'bytes':10,'sha256':'a'*64,'role':role})
        validate_result(result,row)
        self.ledger.tasks.update(row['id'],row['revision'],'awaiting-review',{'result':result})
        self.continue_work()
        self.assertEqual(self.ledger.tasks.get(row['id'])['state'],'round-complete')

    def test_questions_are_not_silently_hidden_in_a_hold_or_auto_letter(self):
        row=self.add();q=[{'id':'author','question':'作者顺序？','source_quote':'Author order?', 'evidence_ref':'fixture:source'}]
        for outcome,route in [('completed','auto'),('hold','owner')]:
            with self.assertRaises(ValueError):validate_result(self.result(row,outcome,route,q),row)

    def test_unregistered_sender_thread_is_not_reread_every_tick(self):
        box=unittest.mock.Mock()
        box.settings={'reconciled':True,'live_since':'2026-10-30T00:00:00Z'}
        box.verify_owner.return_value='sender@example.invalid'
        def sender(value):return {'emailAddress':{'address':value}}
        unknown={'id':'other','internetMessageId':'<other>','conversationId':'other-thread',
                 'receivedDateTime':'2026-10-31T09:00:00Z','from':sender('new@example.invalid'),
                 'toRecipients':[sender('sender@example.invalid')],'subject':'Hello','isDraft':False}
        box.messages.side_effect=[[unknown],[],[]]
        box.research_thread.return_value=[unknown]
        box.match_inbound_sent=None
        when=datetime(2026,10,31,10,tzinfo=timezone.utc)
        mailbox.intake(self.ledger,self.cfg,self.root,box,revision='b'*40,now=when)
        mailbox.intake(self.ledger,self.cfg,self.root,box,revision='b'*40,now=when)
        self.assertEqual(box.research_thread.call_count,1)
        unmapped=automation.work(self.ledger.tasks,self.root)['mailbox_needs_classification']
        self.assertEqual([m['internetMessageId'] for m in unmapped],['<other>'])
        clock=self.ledger.tasks.clock
        self.ledger.tasks.clock=lambda:clock()+mailbox.UNMAPPED_RECHECK_SECONDS+1
        mailbox.intake(self.ledger,self.cfg,self.root,box,revision='b'*40,now=when)
        self.assertEqual(box.research_thread.call_count,2)

    def test_inbox_thread_enters_once_and_unknown_or_owner_mail_remains_visible(self):
        box=unittest.mock.Mock()
        box.settings={'reconciled':True,'live_since':'2026-10-30T00:00:00Z'}
        box.verify_owner.return_value='sender@example.invalid'
        def sender(value):return {'emailAddress':{'address':value}}
        latest={'id':'in','internetMessageId':'<new-in>','conversationId':'new-thread','receivedDateTime':'2026-10-31T09:00:00Z',
                'from':sender('author@example.invalid'),'toRecipients':[sender('sender@example.invalid')],
                'subject':'A real question','body':{'contentType':'text','content':'Can you prove this?'},'isDraft':False}
        sent={**latest,'id':'out','internetMessageId':'<out>','sentDateTime':'2026-10-30T09:00:00Z',
              'from':sender('sender@example.invalid'),'toRecipients':[sender('author@example.invalid')]}
        unknown={**latest,'id':'other','conversationId':'other-thread','from':sender('new@example.invalid')}
        box.messages.return_value=[latest,unknown]
        box.research_thread.side_effect=lambda thread:[unknown] if thread=='other-thread' else [sent,latest]
        when=datetime(2026,10,31,10,tzinfo=timezone.utc)
        mailbox.intake(self.ledger,self.cfg,self.root,box,revision='b'*40,now=when)
        mailbox.intake(self.ledger,self.cfg,self.root,box,revision='b'*40,now=when)
        self.assertEqual(len(self.ledger.tasks.all()),1)
        row=self.ledger.tasks.all()[0]
        self.assertEqual(row['request']['inputs']['latest_sent_message_id'],'<out>')
        self.assertEqual(len(row['request']['inputs']['thread']),2)
        unmapped=automation.work(self.ledger.tasks,self.root)['mailbox_needs_classification']
        self.assertEqual(len(unmapped),1)
        self.assertEqual(unmapped[0]['intake_reason'],'unregistered sender, no link to our Sent')
        # Intake still finds the original source-derived ID after a workflow
        # replacement, even when a later tick uses a different repository HEAD.
        with patch('managed.control.workflow',return_value='c'*64):
            advance(self.ledger,CMA(),self.cfg,self.folder,revision='c'*40)
        old=self.ledger.tasks.get(row['id']);self.assertEqual(old['state'],'superseded')
        mailbox.intake(self.ledger,self.cfg,self.root,box,revision='d'*40,now=when)
        self.assertEqual(len(self.ledger.tasks.all()),2)
        inventory=automation.work(self.ledger.tasks,self.root)
        self.assertEqual([r['task_id'] for r in inventory['registry_updates']],[old['data']['successor']])
        self.assertNotIn(row['id'],[r['id'] for r in inventory['follow_up']])
        self.tables('decisions',{'owner_controlled_threads':['Professor Example']})
        box.messages.return_value=[{**latest,'internetMessageId':'<later>'}]
        box.research_thread.side_effect=lambda thread:[sent,{**latest,'internetMessageId':'<later>'}]
        mailbox.intake(self.ledger,self.cfg,self.root,box,revision='b'*40,now=when)
        self.assertEqual(len(self.ledger.tasks.all()),2)

    def test_known_correspondent_without_current_sent_is_requeued_with_registry_context(self):
        contacts=self.root/'tables'/'contacts.json'
        value=json.loads(contacts.read_text())
        value['contacts'][0]['hub']={'events':[{'id':'sent-event','kind':'sent',
            'at':'2026-10-20T09:00:00Z','summary':'First contact','evidence':'private:sent'}]}
        contacts.write_text(json.dumps(value))
        box=unittest.mock.Mock();box.settings={'reconciled':True,'live_since':'2026-10-30T00:00:00Z'}
        box.verify_owner.return_value='sender@example.invalid'
        sender=lambda value:{'emailAddress':{'address':value}}
        inbound={'id':'known-in','internetMessageId':'<known-in@example.invalid>',
                 'conversationId':'known-new','receivedDateTime':'2026-10-31T09:00:00Z',
                 'from':sender('author@example.invalid'),'toRecipients':[sender('sender@example.invalid')],
                 'ccRecipients':[],'subject':'A new question','isDraft':False}
        box.messages.return_value=[inbound];box.research_thread.return_value=[inbound]
        mailbox.intake(self.ledger,self.cfg,self.root,box,revision='b'*40,
                       now=datetime(2026,10,31,10,tzinfo=timezone.utc))
        row=self.ledger.tasks.all()[0]
        self.assertEqual(row['request']['inputs']['prior_context'],'registry-only')
        self.assertNotIn('latest_sent_message_id',row['request']['inputs'])
        self.assertIn('new conversation from a known correspondent',row['request']['inputs']['instructions'])

    def test_previous_unmapped_message_is_re_evaluated_under_new_history_rule(self):
        contacts=self.root/'tables'/'contacts.json'
        value=json.loads(contacts.read_text())
        value['contacts'][0]['hub']={'threads':[{'id':'prior-thread','last_out':
            {'date':'2026-10-20T09:00:00Z','ROWID':1},'last_in':{},
            'correspondent_email':'author@example.invalid'}]}
        contacts.write_text(json.dumps(value))
        box=unittest.mock.Mock();box.settings={'reconciled':True}
        box.verify_owner.return_value='sender@example.invalid';box.messages.return_value=[]
        inbound={'id':'old-in','internetMessageId':'<old-in@example.invalid>',
                 'conversationId':'old-new','receivedDateTime':'2026-10-30T09:00:00Z',
                 'from':{'emailAddress':{'address':'author@example.invalid'}},
                 'toRecipients':[{'emailAddress':{'address':'sender@example.invalid'}}],
                 'ccRecipients':[],'subject':'Old question','isDraft':False}
        box.research_thread.return_value=[inbound]
        prior={'through':'2026-10-30T00:00:00Z','unmapped':[{
            k:inbound[k] for k in ('id','internetMessageId','conversationId','from','toRecipients',
                                   'ccRecipients','subject','receivedDateTime')}]}
        self.ledger.tasks.db.execute('INSERT INTO pipeline_state VALUES(?,?)',('mailbox',canonical(prior)))
        mailbox.intake(self.ledger,self.cfg,self.root,box,revision='b'*40,
                       now=datetime(2026,10,31,10,tzinfo=timezone.utc))
        self.assertEqual(len(self.ledger.tasks.all()),1)

    def test_unregistered_alias_linked_to_sent_is_reply_task_with_registry_hint(self):
        box=unittest.mock.Mock();box.settings={'reconciled':True,'live_since':'2026-10-30T00:00:00Z'}
        box.verify_owner.return_value='sender@example.invalid'
        sender=lambda value:{'emailAddress':{'address':value}}
        inbound={'id':'alias-in','internetMessageId':'<alias-in@example.invalid>',
                 'conversationId':'alias-thread','receivedDateTime':'2026-10-31T09:00:00Z',
                 'from':sender('alias@example.invalid'),'toRecipients':[sender('sender@example.invalid')],
                 'ccRecipients':[],'subject':'Re: A proof','isDraft':False}
        linked_sent={'id':'sent-link','internetMessageId':'<sent-link@example.invalid>',
                     'conversationId':'old-thread','from':sender('sender@example.invalid'),
                     'toRecipients':[sender('author@example.invalid')],'ccRecipients':[],
                     'subject':'A proof','sentDateTime':'2026-10-30T09:00:00Z','isDraft':False}
        box.messages.return_value=[inbound];box.research_thread.return_value=[inbound]
        def adapter(_,name):
            if name=='match_inbound_sent':
                return lambda message,persons,now=None: {'match':'internet-message-id',
                    'sent':linked_sent,'person':next(p for p in persons if p['id']=='contact-a')}
            return None
        with patch('managed.mailbox._adapter_method',side_effect=adapter):
            mailbox.intake(self.ledger,self.cfg,self.root,box,revision='b'*40,
                           now=datetime(2026,10,31,10,tzinfo=timezone.utc))
        row=self.ledger.tasks.all()[0];inputs=row['request']['inputs']
        self.assertEqual(row['owner'],'contact-a')
        self.assertEqual(inputs['registry_proposal_hint']['email'],'alias@example.invalid')
        self.assertEqual(inputs['linked_sent_message_id'],'<sent-link@example.invalid>')
        result=self.result(row)
        result['packet'].update(thread_id='alias-thread',latest_inbound_message_id='<alias-in@example.invalid>')
        result['routing']['registry_proposal']={**inputs['registry_proposal_hint']}
        validate_result(result,row)

    def intake_grouped_alias(self, sender_name='', *, shared_domain=False, sender_email=None):
        def recipient(email, name=''):
            return {'emailAddress':{'address':email,'name':name}}
        primary='mira@' + 'fictional.co.uk' if shared_domain else 'mira@sample.example.invalid'
        self.tables('contacts',{'contacts':[
            {'id':'contact-synthetic-primary','name':'Mira Stroud','email':primary},
            {'id':'contact-synthetic-coauthor','name':'Roderick Langford','email':'nlangford@' + 'fictional.co.uk'},
            # Matching a name/domain elsewhere in the registry is insufficient.
            {'id':'unrelated','name':sender_name,'email':'elsewhere@' + 'fictional.co.uk'}]})
        inbound={'id':'redirect','internetMessageId':'<redirect@' + 'connect.fictional.co.uk>',
                 'conversationId':'redirect-thread','receivedDateTime':'2026-10-31T09:00:00Z',
                 'from':recipient(sender_email or 'nlangf@' + 'connect.fictional.co.uk',sender_name),
                 'toRecipients':[recipient('sender@example.invalid')],'ccRecipients':[],
                 'subject':'Redirect notice','isDraft':False,
                 'internetMessageHeaders':[{'name':'References','value':'<firstcontact@example.invalid>'}]}
        sent={'id':'firstcontact','internetMessageId':'<firstcontact@example.invalid>',
              'conversationId':'original-thread','from':recipient('sender@example.invalid'),
              'toRecipients':[recipient(primary,'Mira Stroud')],
              # Use the registered name, not this potentially unreliable label.
              'ccRecipients':[recipient('nlangford@' + 'fictional.co.uk','Different label')],
              'subject':'A proof','sentDateTime':'2026-10-30T09:00:00Z','isDraft':False}
        box=mailbox.GraphMailbox(unittest.mock.Mock(),{
            'account':'sender@example.invalid','reconciled':True,'live_since':'2026-10-30T00:00:00Z'})
        with patch.object(box,'verify_owner',return_value='sender@example.invalid'), \
             patch.object(box,'messages',return_value=[inbound]), \
             patch.object(box,'research_thread',return_value=[inbound]), \
             patch.object(box,'sent_message_by_internet_id',return_value=sent) as lookup:
            mailbox.intake(self.ledger,self.cfg,self.root,box,revision='b'*40,
                           now=datetime(2026,10,31,10,tzinfo=timezone.utc))
        lookup.assert_called_once_with('<firstcontact@example.invalid>')
        row,=self.ledger.tasks.all()
        self.assertEqual(row['owner'],'contact-synthetic-primary')
        self.assertEqual(row['request']['inputs']['linked_sent_message_id'],'<firstcontact@example.invalid>')
        return row

    def test_alias_hint_matches_cc_registered_name_even_when_domain_is_ambiguous(self):
        row=self.intake_grouped_alias('RODERICK J. LANGFORD',shared_domain=True)
        hint=row['request']['inputs']['registry_proposal_hint']
        self.assertEqual(hint['contact_id'],'contact-synthetic-coauthor')
        self.assertEqual(hint['name'],'Roderick Langford')
        self.assertEqual(hint['email'],'nlangf@' + 'connect.fictional.co.uk')
        self.assertEqual(hint['evidence_ref'],'graph:inbox:redirect')
        result=self.result(row,outcome='no-reply',route='none')
        result['routing']['registry_proposal']=hint.copy()
        validate_result(result,row)
        result['routing']['registry_proposal']['contact_id']=row['owner']
        with self.assertRaisesRegex(ValueError,'linked alias evidence'):
            validate_result(result,row)

    def test_alias_hint_matches_unique_sent_recipient_domain(self):
        row=self.intake_grouped_alias('Automated redirect')
        hint=row['request']['inputs']['registry_proposal_hint']
        self.assertEqual(hint['contact_id'],'contact-synthetic-coauthor')
        self.assertEqual(hint['name'],'Roderick Langford')

    def test_ambiguous_alias_has_no_hint_but_keeps_thread_owner_task(self):
        row=self.intake_grouped_alias('Automated redirect',shared_domain=True)
        inputs=row['request']['inputs']
        self.assertNotIn('registry_proposal_hint',inputs)
        self.assertNotIn('return routing.registry_proposal',inputs['instructions'])
        validate_result(self.result(row,outcome='no-reply',route='none'),row)

    def test_unmatched_alias_has_no_hint_but_keeps_thread_owner_task(self):
        row=self.intake_grouped_alias('Unknown sender',sender_email='unknown@' + 'unrelated.co.uk')
        self.assertNotIn('registry_proposal_hint',row['request']['inputs'])
        validate_result(self.result(row,outcome='no-reply',route='none'),row)

    def test_machine_bounce_is_separate_from_unmapped_inventory(self):
        box=unittest.mock.Mock();box.settings={'reconciled':True,'live_since':'2026-10-30T00:00:00Z'}
        box.verify_owner.return_value='sender@example.invalid'
        sender=lambda value:{'emailAddress':{'address':value}}
        bounce={'id':'bounce','internetMessageId':'<bounce@example.invalid>',
                'conversationId':'bounce-thread','receivedDateTime':'2026-10-31T09:00:00Z',
                'from':sender('no-reply@example.invalid'),'toRecipients':[sender('sender@example.invalid')],
                'ccRecipients':[],'subject':'Delivery failed','isDraft':False,
                'body':{'contentType':'text','content':'Final-Recipient: rfc822; alias@example.invalid'}}
        box.messages.return_value=[bounce]
        mailbox.intake(self.ledger,self.cfg,self.root,box,revision='b'*40,
                       now=datetime(2026,10,31,10,tzinfo=timezone.utc))
        payload=json.loads(self.ledger.tasks.db.execute(
            "SELECT payload FROM pipeline_state WHERE key='mailbox'").fetchone()[0])
        self.assertEqual(payload['unmapped'],[])
        self.assertEqual(payload['bounce_count'],1)
        self.assertEqual(payload['bounces'][0]['intake_reason'],'bounce')
        self.assertEqual(payload['bounces'][0]['original_recipient'],'alias@example.invalid')
        inventory=automation.work(self.ledger.tasks,self.root)
        self.assertEqual(inventory['mailbox_bounce_count'],1)

    def test_incomplete_inbox_read_never_advances_cursor(self):
        box=unittest.mock.Mock();box.settings={'reconciled':True,'live_since':'2026-10-30T00:00:00Z'}
        box.verify_owner.return_value='sender@example.invalid';box.messages.side_effect=TimeoutError('Lost page')
        with self.assertRaises(TimeoutError):mailbox.intake(self.ledger,self.cfg,self.root,box,revision='b'*40)
        self.assertIsNone(self.ledger.tasks.db.execute("SELECT * FROM pipeline_state WHERE key='mailbox'").fetchone())

    def test_selected_local_submission_inputs_are_verified_and_staged_without_environment(self):
        canonical=self.root/'outreach';(canonical/'.git').mkdir(parents=True)
        person=self.root/'mail/person/submission/result';person.mkdir(parents=True)
        for name,content in [('paper.tex',b'current source'),('paper.pdf',b'current pdf'),('.credential',b'private')]:
            (person/name).write_bytes(content)
        board={'id':'result','title':'A theorem','authors':['Avery Reed'],'version_date':'2026-10-31',
               'target_journal':'Journal','workspace':'../mail/person/submission/result','stage':'needs_work','blocker':'Review needed'}
        self.tables('submissions',{'papers':[board]})
        self.remote.repo='fixture/repo';self.remote.ensure=lambda:None;selected={}
        self.remote.preserve_task=lambda path,tag:selected.update({(tag,path.name):path.read_bytes()})
        def download(*args):
            name=args[args.index('--pattern')+1];target=Path(args[args.index('--dir')+1])
            (target/name).write_bytes(next(v for (tag,n),v in selected.items() if n==name))
        self.remote.run=download
        def git(args,**kwargs):return str(canonical/'.git')+'\n' if '--git-common-dir' in args else 'b'*40+'\n'
        with patch('managed.automation.subprocess.check_output',side_effect=git):
            receipt=automation.stage_submission('result',['paper.tex','paper.pdf'],self.root,self.remote,self.folder)
            with self.assertRaises(ValueError):automation.stage_submission('result',['.credential'],self.root,self.remote,self.folder)
        self.assertEqual(len(selected),2)
        req=json.loads(self.remote.assets[receipt['asset']])
        self.assertEqual(req['kind'],'submission');self.assertEqual(req['owner'],'contact-a')
        self.assertEqual(req['inputs']['submission_round'],'improve')
        self.assertEqual(req['inputs']['submission_source']['identity'],automation.paper_identity(board))
        automation.paper_gate(req,self.root)
        board['version_date']='2026-11-01';self.tables('submissions',{'papers':[board]})
        automation.paper_gate(req,self.root)


class LiveMailboxRulesTests(unittest.TestCase):
    def setUp(self):
        self.graph=unittest.mock.Mock()
        self.box=mailbox.GraphMailbox(self.graph,{'account':'sender@example.invalid','reconciled':True})
        self.graph.request.return_value={'id':'owner','mail':'sender@example.invalid'}
        self.packet=json.loads((Path(__file__).parent/'fixtures/letter.json').read_text())
        self.packet.update(kind='reply',thread_id='thread',latest_inbound_message_id='<in>',user_decision_ever=False)
        self.latest={'id':'in','internetMessageId':'<in>','conversationId':'thread','isDraft':False,
                     'receivedDateTime':'2026-10-31T10:00:00Z','from':{'emailAddress':{'address':'author@example.invalid'}},
                     'toRecipients':[{'emailAddress':{'address':'sender@example.invalid'}}],'ccRecipients':[]}
        self.sent={**self.latest,'id':'out','internetMessageId':'<out>','sentDateTime':'2026-10-30T10:00:00Z',
                   'from':{'emailAddress':{'address':'sender@example.invalid'}}}
    def test_new_incoming_sent_or_reply_all_changes_block_delivery(self):
        self.graph.pages.return_value=[self.latest,self.sent]
        self.box.preflight(self.packet,{'inputs':{'latest_sent_message_id':'<out>'}})
        self.graph.pages.return_value=[{**self.latest,'internetMessageId':'<new>'},self.sent]
        with self.assertRaisesRegex(ValueError,'newer incoming'):self.box.preflight(self.packet,{'inputs':{'latest_sent_message_id':'<out>'}})
        self.graph.pages.return_value=[self.latest,{**self.sent,'internetMessageId':'<new-sent>'}]
        with self.assertRaisesRegex(ValueError,'Sent changed'):self.box.preflight(self.packet,{'inputs':{'latest_sent_message_id':'<out>'}})
        self.graph.pages.return_value=[{**self.latest,'ccRecipients':[{'emailAddress':{'address':'colleague@example.invalid'}}]},self.sent]
        with self.assertRaisesRegex(ValueError,'Reply-all'):self.box.preflight(self.packet,{'inputs':{'latest_sent_message_id':'<out>'}})
    def test_user_web_draft_is_never_overwritten(self):
        self.graph.pages.return_value=[self.latest,{**self.sent,'isDraft':True}]
        with self.assertRaisesRegex(ValueError,'server draft'):self.box.create_draft(self.packet,'attempt')
        self.assertFalse(any(c.args[1:] for c in self.graph.request.call_args_list))
    def test_wrong_account_and_unreconciled_boundary_fail_before_write(self):
        self.graph.request.return_value={'mail':'somebody@example.invalid'}
        with self.assertRaisesRegex(ValueError,'owner'):self.box.preflight(self.packet,{'inputs':{}})
        self.box.identity=None;self.graph.request.return_value={'mail':'sender@example.invalid'};self.box.settings['reconciled']=False
        with self.assertRaisesRegex(ValueError,'reconciliation'):self.box.preflight(self.packet,{'inputs':{}})

    def test_first_contact_checks_actual_inbox_as_well_as_sent(self):
        # Per-recipient participant search, one folder per call.
        self.graph.pages.side_effect=lambda path:[self.latest] if '/inbox/' in path else []
        with self.assertRaisesRegex(ValueError,'Actual Inbox'):
            self.box.preflight({**self.packet,'kind':'firstcontact'},{'inputs':{}})
        self.box.begin_history_tick()
        sent={**self.sent,'toRecipients':[{'emailAddress':{'address':self.packet['to'][0]}}]}
        self.graph.pages.side_effect=lambda path:[sent] if '/sentitems/' in path else []
        with self.assertRaisesRegex(ValueError,'Actual Sent'):
            self.box.preflight({**self.packet,'kind':'firstcontact'},{'inputs':{}})
        self.box.begin_history_tick()
        self.graph.pages.side_effect=lambda path:[]
        self.box.preflight({**self.packet,'kind':'firstcontact'},{'inputs':{}})
        self.assertTrue(all('%24search' in c.args[0] or '$search' in c.args[0]
                            for c in self.graph.pages.call_args_list[-3:]))

    def test_correlation_header_alone_cannot_verify_wrong_sent_body_or_attachment(self):
        message={**self.sent,'subject':self.packet['subject'],
                 'toRecipients':[{'emailAddress':{'address':a}} for a in self.packet['to']],
                 'ccRecipients':[],'body':{'contentType':'text','content':self.packet['body']}}
        expected=self.packet['attachments'][0]
        attachment={'@odata.type':'#microsoft.graph.fileAttachment','name':expected['name'],
                    'contentType':expected['media_type'],'contentBytes':expected['content_b64']}
        self.graph.pages.return_value=[attachment]
        self.assertTrue(self.box.matches(message,self.packet))
        self.assertFalse(self.box.matches({**message,'body':{'contentType':'text','content':'changed'}},self.packet))
        self.graph.pages.return_value=[{**attachment,'contentBytes':'Y2hhbmdlZA=='}]
        self.assertFalse(self.box.matches(message,self.packet))

    def test_reply_mime_keeps_source_thread_and_sends_only_after_exact_draft_readback(self):
        import base64
        from email import policy
        from email.parser import BytesParser
        observed={**self.sent,'id':'draft','conversationId':'thread','isDraft':True,'subject':self.packet['subject'],
                  'toRecipients':[{'emailAddress':{'address':a}} for a in self.packet['to']],
                  'ccRecipients':[],'body':{'contentType':'text','content':self.packet['body']}}
        source=self.packet['attachments'][0]
        attachment={'@odata.type':'#microsoft.graph.fileAttachment','name':source['name'],
                    'contentType':source['media_type'],'contentBytes':source['content_b64']}
        self.graph.mime.return_value={'id':'draft'}
        self.graph.request.side_effect=[self.latest,observed,{}]
        self.graph.pages.side_effect=[[self.latest,self.sent],[attachment]]
        result=self.box.submit(self.packet,'exact-attempt')
        mime=BytesParser(policy=policy.default).parsebytes(base64.b64decode(self.graph.mime.call_args.args[1]))
        self.assertEqual(mime['In-Reply-To'],'<in>')
        self.assertEqual(mime['X-Outreach-Attempt'],'exact-attempt')
        self.assertIsNone(mime['Thread-Index'])
        self.assertIsNone(mime['Thread-Topic'])
        self.assertEqual(result.locator,'draft')
        self.assertEqual(self.graph.request.call_args.args,('/me/messages/draft/send','POST',{}))
        self.graph.reset_mock();self.graph.mime.return_value={'id':'draft'}
        self.graph.request.side_effect=[self.latest,{**observed,'body':{'contentType':'text','content':'server changed body'}}]
        self.graph.pages.side_effect=[[self.latest,self.sent]]
        with self.assertRaisesRegex(NotSent,'differs'):self.box.submit(self.packet,'different-attempt')
        self.assertFalse(any(len(c.args)>1 and c.args[1]=='POST' for c in self.graph.request.call_args_list))


if __name__=='__main__':unittest.main()


def setUpModule():
    use_example_config()
