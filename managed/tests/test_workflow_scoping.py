"""Workflow changes preserve only the exact reviews whose inputs still bind."""
import copy
import hashlib
import json
from pathlib import Path
import re
import tempfile
import unittest
from unittest.mock import patch

from managed import automation, control
from managed.tasks import Tasks, matches_workflow
from test_review_binding import sign


class WorkflowScopingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        paths = ['automation.json', 'runtime.py', 'submission/author-policy.json',
                 'daily/DESIGN-notes.md', 'replies/voice.md']
        paths += [str(p.relative_to(control.ROOT / 'managed'))
                  for pattern in ('*/workflow.json', '*/review.md')
                  for p in (control.ROOT / 'managed').glob(pattern)]
        for name in paths:
            target = self.root / 'managed' / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes((control.ROOT / 'managed' / name).read_bytes())
        self.tasks = Tasks(self.root / 'state.sqlite3')
        self.root_patch = patch.object(control, 'ENGINE_ROOT', self.root)
        self.root_patch.start()

    def tearDown(self):
        self.root_patch.stop()
        self.tasks.close()
        self.temp.cleanup()

    def edit(self, name):
        path = self.root / 'managed' / name
        path.write_text(path.read_text() + '\nchanged input\n')

    def owner_draft(self, name='reply-1', workflow_hash=None, **data):
        request = {'schema_version': 1, 'id': name, 'kind': 'reply', 'owner': 'contact-a',
                   'source_revision': 'a' * 40, 'inputs': {},
                   'workflow_hash': workflow_hash or control.workflow('reply')}
        row = self.tasks.add(request)
        result = {'schema_version': 1, 'task_id': name, 'outcome': 'completed',
                  'report': 'Synthetic checked result', 'artifacts': [], 'packet': None}
        row = self.tasks.update(name, row['revision'], 'awaiting-review', {'result': result})
        review = control.sign_review({'binding': self.tasks.review_binding(row),
                    'reviewer': 'local-claude-loop', 'verdict': 'PASS',
                    'reason': 'Checked synthetic result', 'evidence_ref': 'fixture:review'}, 'secret')
        row = self.tasks.verdict(review, 'secret')
        return self.tasks.update(name, row['revision'], 'owner-draft', {**row['data'], **data})

    def inventory(self):
        with patch('managed.daily_local.status', return_value={}):
            return automation.work(self.tasks, self.root)

    def test_submission_policy_edit_preserves_reply_firstcontact_and_shortnote(self):
        row = self.owner_draft()
        before = {kind: control.workflow(kind) for kind in control.KINDS}
        self.edit('submission/author-policy.json')
        for kind in ('reply', 'firstcontact', 'shortnote', 'research', 'mathesis', 'daily'):
            self.assertEqual(before[kind], control.workflow(kind), kind)
        self.assertNotEqual(before['submission'], control.workflow('submission'))
        self.assertTrue(matches_workflow(row, control.workflow('reply')))
        self.assertTrue(self.inventory()['owner_drafts'][0]['review_current'])

    def test_voice_guide_is_pinned_only_for_mail(self):
        before = {kind: control.workflow(kind) for kind in control.KINDS}
        self.edit('replies/voice.md')
        for kind in control.KINDS:
            self.assertEqual(before[kind] != control.workflow(kind), kind in {'reply', 'firstcontact'}, kind)
            prompt = control.task_prompt({'kind': kind, 'request': {'inputs': {}}})
            self.assertEqual('managed/replies/voice.md' in prompt, kind in {'reply', 'firstcontact'}, kind)

    def test_voice_examples_do_not_include_corpus_recipient_names_or_addresses(self):
        # SHA-256 of the lowercase recipient surnames, including compound surname parts.
        deny = {
            '5cd260a34007729adb2ccee3223f318583d00f3e1649ca30f9c84ffbd38d032a',
            '15fa35b0d2855a3a04a994908a0884ea682f0ef5064bc09d477b2c247789336b',
            '2e8bc75ff4978cf73fbfed89e2864749b005b360921e33ff35b7a3cfcdd794de',
            'd94c0f0d181e477c084bff5cd882dba9e92fca5d10df798e7a43a2c77b84066b',
            '2b5c3dc48d8d474aec091357087f3ec97623b10f1a9a197a57a20eacc52cd421',
            '2716952ccb18cef79a778b2c4c5c964d159d93b23e611ee90e37507404132877',
            'cc0d4c4f3a05291e5da69d261a053756c8965d8ca3c95e9f7ae364296f2b80e3',
            '0a1ecea24825f1b10370a951df7535b101a3339964e43f11a80942cd1d1b6251',
        }
        voice = (self.root / 'managed/replies/voice.md').read_text()
        self.assertEqual(1, len(re.findall(r'^Dear ', voice, re.M)))
        self.assertNotIn('@', voice)
        self.assertNotRegex(voice, r'https?://')
        words = re.findall(r'[^\W_]+(?:-[^\W_]+)*', voice.lower())
        hashes = {hashlib.sha256(part.encode()).hexdigest()
                  for word in words for part in (word, *word.split('-'))}
        self.assertFalse(deny & hashes)

    def test_mail_workflows_and_review_contracts_require_final_voice_pass(self):
        managed = self.root / 'managed'
        reply = (managed / 'replies/workflow.json').read_text()
        firstcontact = (managed / 'firstcontact/workflow.json').read_text()
        self.assertNotIn('polish.sh', reply + firstcontact)
        compose = next(s for s in json.loads(firstcontact)['steps'] if s['id'] == 'compose')
        for rule in (json.loads(reply)['reply_pre_return_checks']['voice_pass'], compose['voice_pass']):
            for phrase in ("rewrite the body once as a whole", "voice.md", "self_audit",
                           "'voice pass'", "first prose sentence", "each subsequent prose paragraph",
                           "project scale or method", "owner has not decided", "You may submit"):
                self.assertIn(phrase, rule)
        for name in ('replies/review.md', 'firstcontact/review.md'):
            self.assertRegex((managed / name).read_text(), r'BOUNCE if the\s+`?self_audit`?')

    def test_reply_workflow_edit_invalidates_reply_only(self):
        row = self.owner_draft()
        before = {kind: control.workflow(kind) for kind in control.KINDS}
        policy = self.root / 'managed/replies/workflow.json'
        workflow = json.loads(policy.read_text())
        checks = workflow['reply_pre_return_checks']
        self.assertIn('mailbox_history.latest_sent', checks['salutation'])
        self.assertIn('the latest_sent entry is missing', checks['salutation'])
        self.assertIn('Hold when drafts_complete is false or missing', checks['saved_owner_draft'])
        self.assertIn('an empty list confirms there is no current unsent draft', checks['saved_owner_draft'])
        self.assertIn('it needs no separate short-note PASS', checks['note_sources'])
        self.assertIn("bind that note's exact PASS", checks['note_sources'])
        checks['note_sources'] += ' Changed note policy.'
        policy.write_text(json.dumps(workflow))
        self.assertFalse(matches_workflow(row, control.workflow('reply')))
        for kind in control.KINDS - {'reply'}:
            self.assertEqual(before[kind], control.workflow(kind), kind)
        item = self.inventory()['owner_drafts'][0]
        self.assertFalse(item['review_current'])
        self.assertEqual(item['blocker'], 'workflow-changed')

    def test_owner_rebind_binding_survives_cleanup_delivery_errors_and_restore(self):
        row = self.owner_draft()
        original = copy.deepcopy(row)
        self.edit('replies/workflow.json')
        item = self.inventory()['owner_draft_rebind'][0]
        binding = item['binding']
        self.assertEqual(binding, self.tasks.review_binding(row))
        self.assertEqual(binding['task_revision'], row['revision'] - 1)  # Current PASS event.
        self.assertEqual(item['request'], row['request'])
        self.assertEqual(item['result'], row['data']['result'])
        unsigned = sign(binding, rebind_workflow_hash=item['rebind_workflow_hash'])
        unsigned.pop('signature')
        envelope = control.sign_review(unsigned, 'secret', kind='reply')
        for data in ({'cleanup': 'retired'}, *[{'last_error': 'Mail workflow changed; re-review the current version'}] * 3):
            row = self.tasks.update(row['id'], row['revision'], row['state'], {**row['data'], **data})
            self.assertEqual(self.tasks.review_binding(row), binding)
            self.assertEqual(self.inventory()['owner_draft_rebind'][0]['binding'], binding)
        self.tasks.close()
        self.tasks = Tasks(self.root / 'state.sqlite3')
        row = self.tasks.verdict(envelope, 'secret')
        self.assertEqual(row['state'], 'owner-draft')
        self.assertEqual(row['request'], original['request'])
        self.assertEqual(row['data']['result'], original['data']['result'])
        self.assertEqual(row['data']['review'], envelope)
        self.assertEqual(self.tasks.verdict(envelope, 'secret'), row)
        self.assertTrue(matches_workflow(row, control.workflow('reply')))
        work = self.inventory()
        self.assertTrue(work['owner_drafts'][0]['review_current'])
        self.assertEqual(work['owner_drafts'][0]['blocker'], 'save-failed')
        self.assertEqual(work['owner_draft_rebind'], [])
        self.edit('replies/workflow.json')
        fresh = self.inventory()['owner_draft_rebind'][0]
        self.assertEqual(fresh['binding']['task_revision'], row['revision'])
        self.assertGreater(fresh['binding']['task_revision'], binding['task_revision'])
        row = self.tasks.verdict(sign(fresh['binding'], rebind_workflow_hash=fresh['rebind_workflow_hash']), 'secret')
        self.assertTrue(self.inventory()['owner_drafts'][0]['review_current'])

    def test_owner_rebind_rejects_wrong_binding_hash_and_non_pass(self):
        row = self.owner_draft()
        self.edit('replies/workflow.json')
        binding = self.tasks.review_binding(row)
        current = control.workflow('reply')
        invalid = [sign({**binding, field: value}, rebind_workflow_hash=current)
                   for field, value in [('result_hash', '0' * 64), ('workflow_hash', '0' * 64),
                                        ('task_revision', row['revision'] + 1)]]
        invalid += [sign(binding, rebind_workflow_hash=row['request']['workflow_hash']),
                    sign(binding)]
        invalid += [sign(binding, verdict=verdict, **optional)
                    for verdict in ('BOUNCE', 'HOLD')
                    for optional in ({}, {'rebind_workflow_hash': current})]
        for envelope in invalid:
            with self.subTest(envelope=envelope), self.assertRaises(ValueError):
                self.tasks.verdict(envelope, 'secret')
            self.assertEqual(self.tasks.get(row['id']), row)

    def test_owner_rebind_requires_a_workflow_stale_exact_pass(self):
        row = self.owner_draft()
        self.assertEqual(self.inventory()['owner_draft_rebind'], [])
        with self.assertRaises(ValueError):
            self.tasks.review_binding(row)
        self.edit('replies/workflow.json')
        binding = self.tasks.review_binding(row)
        envelope = sign(binding, rebind_workflow_hash=control.workflow('reply'))
        self.edit('replies/workflow.json')
        with self.assertRaisesRegex(ValueError, 'current workflow'):
            self.tasks.verdict(envelope, 'secret')
        for verdict in ('BOUNCE', 'HOLD'):
            row = self.tasks.update(row['id'], row['revision'], row['state'],
                                    {**row['data'], 'review': {**row['data']['review'], 'verdict': verdict}})
            self.assertEqual(self.inventory()['owner_draft_rebind'], [])
            with self.assertRaises(ValueError):
                self.tasks.review_binding(row)
            with self.assertRaises(ValueError):
                self.tasks.verdict(sign(binding, rebind_workflow_hash=control.workflow('reply')), 'secret')

    def test_owner_rebind_rejects_sent_conflicted_and_changed_results(self):
        for name, extra in [('sent', {'sent_receipt': {'remote_id': 'fixture:sent'}}),
                            ('conflict', {'stale_draft_conflict': {'remote_id': 'fixture:edited'}}),
                            ('changed', {'result': {'report': 'Changed after PASS', 'packet': None}})]:
            with self.subTest(case=name):
                row = self.owner_draft(name)
                self.edit('replies/workflow.json')
                binding = self.tasks.review_binding(row)
                row = self.tasks.update(row['id'], row['revision'], row['state'], {**row['data'], **extra})
                with self.assertRaises(ValueError):
                    self.tasks.review_binding(row)
                # Reject both the fresh inventory binding and the legacy PASS binding.
                for target in (binding, row['data']['review']['binding']):
                    with self.assertRaises(ValueError):
                        self.tasks.verdict(sign(target, rebind_workflow_hash=control.workflow('reply')), 'secret')
                self.assertEqual(self.tasks.get(row['id']), row)
        self.assertEqual(self.inventory()['owner_draft_rebind'], [])

    def test_automation_mail_and_daily_policies_are_scoped(self):
        before = {kind: control.workflow(kind) for kind in control.KINDS}
        path = self.root / 'managed/automation.json'
        value = json.loads(path.read_text())
        value['publication']['daily'] += ' Changed daily policy.'
        path.write_text(json.dumps(value))
        self.assertNotEqual(before['daily'], control.workflow('daily'))
        for kind in control.KINDS - {'daily'}:
            self.assertEqual(before[kind], control.workflow(kind), kind)
        before = {kind: control.workflow(kind) for kind in control.KINDS}
        value['mail']['owner_routing'] += ' Changed mail policy.'
        path.write_text(json.dumps(value))
        for kind in control.KINDS:
            self.assertEqual(before[kind] != control.workflow(kind), kind in {'reply', 'firstcontact'}, kind)

    def test_legacy_migration_preserves_exact_request_review_and_revision(self):
        legacy = control.legacy_workflow('reply')
        row = self.owner_draft(workflow_hash=legacy)
        original = copy.deepcopy(row)
        self.assertNotEqual(legacy, control.workflow('reply'))
        self.assertEqual(control.migrate_workflows(self.tasks), 1)
        row = self.tasks.get(row['id'])
        for key in ('request', 'request_hash', 'revision', 'updated', 'state'):
            self.assertEqual(row[key], original[key], key)
        self.assertEqual(row['data']['review'], original['data']['review'])
        self.assertTrue(matches_workflow(row, control.workflow('reply')))
        self.assertTrue(self.inventory()['owner_drafts'][0]['review_current'])
        self.edit('submission/author-policy.json')
        self.assertTrue(matches_workflow(row, control.workflow('reply')))
        self.edit('replies/workflow.json')
        self.assertEqual(control.migrate_workflows(self.tasks), 0)
        self.assertFalse(matches_workflow(self.tasks.get(row['id']), control.workflow('reply')))
        # A restart preserves the one-time marker; it cannot bless the later edit.
        self.tasks.close()
        self.tasks = Tasks(self.root / 'state.sqlite3')
        self.assertEqual(control.migrate_workflows(self.tasks), 0)
        self.assertFalse(self.inventory()['owner_drafts'][0]['review_current'])

    def test_migration_accepts_current_legacy_rebind_and_leaves_stale_pass_stale(self):
        legacy = control.legacy_workflow('reply')
        row = self.owner_draft(workflow_hash='b' * 64)
        review = {k: v for k, v in row['data']['review'].items() if k != 'signature'}
        with patch.object(control, 'workflow', return_value=legacy):
            review = control.sign_review({**review, 'rebind_workflow_hash': legacy}, 'secret', kind='reply')
        self.tasks.verdict(review, 'secret')
        stale = self.owner_draft('stale', workflow_hash='c' * 64)
        self.assertEqual(control.migrate_workflows(self.tasks), 1)
        self.assertTrue(matches_workflow(self.tasks.get(row['id']), control.workflow('reply')))
        self.assertFalse(matches_workflow(self.tasks.get(stale['id']), control.workflow('reply')))
        exceptions = {r['task_id']: r for r in self.inventory()['owner_draft_exceptions']}
        self.assertEqual(exceptions['stale']['blocker'], 'workflow-changed')

    def test_migration_preserves_pending_review_and_queued_work_for_every_kind(self):
        for kind in control.KINDS:
            self.tasks.add({'schema_version': 1, 'id': kind, 'kind': kind, 'owner': 'contact-a',
                            'source_revision': 'a' * 40, 'inputs': {},
                            'workflow_hash': control.legacy_workflow(kind)})
        row = self.tasks.get('reply')
        row = self.tasks.update(row['id'], row['revision'], 'awaiting-review',
                                {'result': {'outcome': 'completed', 'report': 'Synthetic result'}})
        binding = self.tasks.review_binding(row)
        envelope = control.sign_review({'binding': binding, 'reviewer': 'local-claude-loop',
                                        'verdict': 'PASS', 'reason': 'Checked',
                                        'evidence_ref': 'fixture:pending-review'}, 'secret')
        self.assertEqual(control.migrate_workflows(self.tasks), len(control.KINDS))
        for row in self.tasks.all():
            self.assertTrue(matches_workflow(row, control.workflow(row['kind'])), row['kind'])
        reviews = self.tasks.reviews()
        self.assertEqual(reviews[0]['binding'], binding)
        self.assertFalse(control.review_digest({'reviews': reviews}, self.root)['items'][0]['workflow_changed'])
        self.assertEqual(self.tasks.verdict(envelope, 'secret')['state'], 'approved')
        # Provisioning retries must carry compatibility metadata forward too.
        data = self.tasks.get('firstcontact')['data']
        for retry in (control.requeue, control.workspace_requeue):
            self.assertEqual(retry(data)['workflow_binding'], data['workflow_binding'])

    def test_exception_list_includes_missing_receipt_and_save_failure(self):
        self.owner_draft()
        self.owner_draft('failed', last_error='TimeoutError: synthetic failure')
        saved = self.owner_draft('saved', draft_receipt={'remote_id': 'fixture:draft'})
        self.owner_draft('sent', sent_receipt={'remote_id': 'fixture:sent'})
        work = self.inventory()
        items = {r['task_id']: r for r in work['owner_drafts']}
        self.assertEqual(len(items), 4)
        self.assertTrue(items['reply-1']['review_current'])
        self.assertIsNone(items['saved']['blocker'])
        exceptions = {r['task_id']: r for r in work['owner_draft_exceptions']}
        self.assertEqual(set(exceptions), {'reply-1', 'failed'})
        self.assertEqual(exceptions['reply-1'], {'task_id': 'reply-1', 'kind': 'reply',
                         'owner': 'contact-a', 'blocker': 'no-draft-receipt'})
        self.assertEqual(exceptions['failed']['blocker'], 'save-failed:TimeoutError')
        self.edit('replies/workflow.json')
        exceptions = {r['task_id']: r for r in self.inventory()['owner_draft_exceptions']}
        self.assertIn(saved['id'], exceptions)
        self.assertEqual(exceptions[saved['id']]['blocker'], 'workflow-changed')

    def test_changed_result_never_reports_a_current_pass(self):
        row = self.owner_draft(draft_receipt={'remote_id': 'fixture:draft'})
        row['data']['result']['report'] = 'Changed since independent review'
        self.tasks.update(row['id'], row['revision'], row['state'], row['data'])
        work = self.inventory()
        self.assertFalse(work['owner_drafts'][0]['review_current'])
        self.assertEqual(work['owner_draft_exceptions'][0]['blocker'], 'review-stale')


if __name__ == '__main__':
    unittest.main()
