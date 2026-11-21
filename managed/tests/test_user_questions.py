"""Coordinator-only question closure from dated, observed Sent evidence."""
import hashlib
import hmac
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from managed import automation, control, owner_todo, registry, sent_events
from managed.mailbox import instant
from managed.queue import canonical, digest
from managed.tick import Tick
from test_tasks import Releases, request, use_fixture_workflow_revisions


ACCOUNT = 'owner@example.invalid'
CONTACT = 'scholar@example.invalid'
INBOUND = '2026-11-13T17:14:00Z'
SENT = '2026-11-14T18:30:00Z'


class UserQuestionTests(unittest.TestCase):
    def setUp(self):
        use_fixture_workflow_revisions(self)
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        (self.root / 'tables').mkdir()
        folder = self.root / 'ledger'
        folder.mkdir()
        self.ledger = control.Ledger(Releases(), folder)
        self.addCleanup(self.ledger.tasks.close)
        registry.write(self.root, 'contacts', {'contacts': [
            {'id': 'contact-a', 'name': 'Professor Scholar', 'email': CONTACT, 'problem_ids': []},
            {'id': 'contact-b', 'name': 'Professor Other', 'email': 'other@example.invalid', 'problem_ids': []}]})
        registry.write(self.root, 'refusals', {'refusals': []})
        registry.write(self.root, 'submissions', {'papers': []})
        self.config = {'paused': False, 'mailbox': {'account': ACCOUNT},
                       'automation': {'enabled': True, 'max_new_tasks_per_tick': 4}}

    def question(self, name='reply-old', at=INBOUND, thread='conversation', state='awaiting-user', owner='contact-a'):
        req = request(name, owner, 'reply')
        req['inputs'] = {'latest_inbound_message_id': '<' + name + '>',
                         'mailbox_observation': {'message_id': '<' + name + '>', 'at': at, 'email': CONTACT}}
        message = {'internetMessageId': '<' + name + '>', 'subject': 'Topic', 'receivedDateTime': at}
        if thread:
            req['inputs']['thread_id'] = thread
            message['conversationId'] = thread
        req['inputs']['thread'] = [message]
        row = self.ledger.tasks.add(req)
        result = {'outcome': 'needs-decision', 'packet': None, 'routing': {'mail': 'owner', 'questions': [
            {'id': 'meeting', 'question': '是否会面？'}]}}
        return self.ledger.tasks.update(name, row['revision'], state, {'result': result})

    def sent(self, at=SENT, conversation='conversation', recipient=CONTACT, **extra):
        message = {'id': 'graph-sent', 'internetMessageId': '<sent@example.invalid>', 'sentDateTime': at,
                   'isDraft': False, 'subject': 'RE: Topic', 'from': {'emailAddress': {'address': ACCOUNT}},
                   'toRecipients': [{'emailAddress': {'address': recipient}}], **extra}
        if conversation:
            message['conversationId'] = conversation
        sent_events.record_page(self.root, [message], self.ledger.tasks.all(), set(),
                                instant('2026-11-01T00:00:00Z'), instant('2026-11-16T00:00:00Z'), ACCOUNT)

    def tick(self):
        automation.reconcile(self.ledger, self.config, self.root, revision='b' * 40)

    def clear_events(self):
        contacts = registry.read(self.root, 'contacts')
        for record in contacts['contacts']:
            record.setdefault('hub', {})['events'] = []
        registry.write(self.root, 'contacts', contacts)

    def test_observed_sent_closes_once_without_continuation_or_todo(self):
        row = self.question()
        self.sent()
        self.tick()
        closed = self.ledger.tasks.get(row['id'])
        self.assertEqual(closed['state'], 'answered-by-mail')
        evidence = closed['data']['user_question_closed']['evidence']
        self.assertEqual((evidence['message_id'], evidence['at'], evidence['match']),
                         ('<sent@example.invalid>', SENT, 'conversation'))
        self.assertEqual(closed['data']['result'], row['data']['result'])
        self.assertFalse({'continuation_id', 'draft_receipt', 'sent_receipt'} & closed['data'].keys())
        self.tick()
        self.assertEqual(self.ledger.tasks.get(row['id']), closed)
        self.assertEqual(len(self.ledger.tasks.all()), 1)
        work = automation.work(self.ledger.tasks, self.root)
        self.assertEqual(work['user_questions'], [])
        self.assertEqual(work['user_questions_closed'], [
            {'task_id': row['id'], 'owner': row['owner'], 'reason': 'answered-by-mail', 'evidence': evidence}])
        self.assertEqual(owner_todo.owner_todo(work, self.root)['questions'], [])

    def test_prior_same_time_different_conversation_sender_or_recipient_stay_open(self):
        cases = [dict(at='2026-11-12T18:30:00Z'), dict(at=INBOUND),
                 dict(conversation='different'), dict(recipient='other@example.invalid'),
                 {'from': {'emailAddress': {'address': 'other@example.invalid'}}}, dict(isDraft=True)]
        for index, values in enumerate(cases):
            with self.subTest(values=values):
                registry.write(self.root, 'contacts', {'contacts': [
                    {'id': 'contact-a', 'name': 'Scholar', 'email': CONTACT, 'problem_ids': []}]})
                row = self.question('negative-' + str(index))
                self.sent(**values)
                self.tick()
                self.assertEqual(self.ledger.tasks.get(row['id'])['state'], 'awaiting-user')

    def test_missing_conversation_uses_only_verified_owner_events(self):
        for task_thread, sent_thread in [(None, 'conversation'), ('conversation', None)]:
            with self.subTest(task_thread=task_thread):
                self.clear_events()
                row = self.question('fallback-' + str(task_thread), thread=task_thread)
                self.sent(conversation=sent_thread)
                self.tick()
                evidence = self.ledger.tasks.get(row['id'])['data']['user_question_closed']['evidence']
                self.assertEqual(evidence['match'], 'owner-registry-fallback')
                self.assertEqual(evidence['recipient'], CONTACT)

    def test_fallback_requires_the_same_subject(self):
        self.clear_events()
        row = self.question('other-subject', thread=None)
        self.sent(conversation=None, subject='A different letter')
        self.tick()
        self.assertEqual(self.ledger.tasks.get(row['id'])['state'], 'awaiting-user')

    def test_unstructured_or_incomplete_evidence_leaves_question_open(self):
        self.question()
        registry.record_event(CONTACT, 'sent', SENT, 'Manual assertion', 'Sent mail', self.root)
        self.tick()
        self.assertEqual(self.ledger.tasks.get('reply-old')['state'], 'awaiting-user')
        self.sent()
        row = self.ledger.tasks.get('reply-old')
        req = request('undated', 'contact-a', 'reply')
        req['inputs'] = {'thread_id': 'conversation'}
        undated = self.ledger.tasks.add(req)
        self.ledger.tasks.update('undated', undated['revision'], 'awaiting-user', row['data'])
        self.tick()
        self.assertEqual(self.ledger.tasks.get('undated')['state'], 'awaiting-user')

    def test_legacy_coordinator_event_and_validated_receipt_fallback(self):
        for index, evidence in enumerate([
                {'source': 'sent-events', 'internetMessageId': '<legacy@example.invalid>'},
                {'remote_id': 'graph-id', 'content_hash': 'a' * 64,
                 'evidence_ref': 'graph:sent:<receipt@example.invalid>'}]):
            with self.subTest(evidence=evidence):
                self.clear_events()
                evidence['recipient'] = CONTACT
                registry.record_event(CONTACT, 'sent', SENT, 'RE: Topic', json.dumps(evidence), self.root)
                row = self.question('legacy-' + str(index))
                self.tick()
                self.assertEqual(self.ledger.tasks.get(row['id'])['state'], 'answered-by-mail')

    def test_supersession_requires_newer_same_owner_conversation_and_mature_state(self):
        for index, state in enumerate(['queued', 'running', 'awaiting-user', 'awaiting-review', 'owner-draft', 'verified']):
            with self.subTest(state=state):
                thread = 'thread-' + str(index)
                old = self.question('old-' + str(index), thread=thread)
                new = self.question('new-' + str(index), at=SENT, thread=thread, state=state)
                # Inspect only closure here; unrelated delivery/continuation paths are irrelevant.
                automation.close_user_questions(self.ledger, self.config, self.root)
                closed = self.ledger.tasks.get(old['id'])
                self.assertEqual(closed['state'], 'awaiting-user' if state in {'queued', 'running'} else 'superseded')
                if closed['state'] == 'superseded':
                    self.assertEqual(closed['data']['superseded_by'], new['id'])
                    work = automation.work(self.ledger.tasks, self.root)
                    self.assertEqual([c['task_id'] for c in work['user_questions_closed']].count(old['id']), 1)

    def test_other_owner_conversation_or_older_inbound_cannot_supersede(self):
        for index, values in enumerate([dict(owner='contact-b'), dict(thread='different'),
                                        dict(at='2026-11-12T00:00:00Z'), dict(at=INBOUND)]):
            thread = 'isolated-' + str(index)
            old = self.question('open-' + str(index), thread=thread)
            self.question('sibling-' + str(index), **{'thread': thread, 'at': SENT, **values})
            self.tick()
            self.assertEqual(self.ledger.tasks.get(old['id'])['state'], 'awaiting-user')

    def test_bounded_pass_rotates_past_open_questions_and_honors_tick_budget(self):
        self.question('a-open', thread='other')
        self.question('z-answered')
        self.sent()
        with patch.object(automation, 'QUESTION_CLOSE_LIMIT', 1):
            self.tick()
            self.assertEqual(self.ledger.tasks.get('z-answered')['state'], 'awaiting-user')
            self.tick()
            self.assertEqual(self.ledger.tasks.get('z-answered')['state'], 'answered-by-mail')
        self.question('zz-deferred')
        Tick(seconds=0).bind(self.ledger)
        self.tick()
        self.assertEqual(self.ledger.tasks.get('zz-deferred')['state'], 'awaiting-user')

    def test_decision_on_open_question_still_records_actual_answer(self):
        row = self.question()
        self.sent(at='2026-11-12T18:30:00Z')
        self.tick()
        envelope = {'schema_version': 1, 'task_id': row['id'], 'result_hash': digest(row['data']['result']),
                    'answers': {'meeting': 'Decline the meeting.'}, 'evidence_ref': 'fixture:user-answer',
                    'reviewer': 'local-claude-loop'}
        envelope['signature'] = hmac.new(b'secret', canonical(envelope).encode(), hashlib.sha256).hexdigest()
        self.assertEqual(self.ledger.tasks.decision(envelope, 'secret')['state'], 'decision-recorded')
        self.tick()
        self.assertEqual(self.ledger.tasks.get(row['id'])['state'], 'revising')
        self.assertEqual(len(self.ledger.tasks.all()), 2)
