import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from managed import affairs, author_contacts, control, registry


class OwnerReportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        (self.root / 'tables').mkdir()
        self.write('contacts', {'contacts': [
            {'id': 'contact-a', 'name': 'Professor A', 'email': 'a@example.invalid', 'hub': {'threads': []}},
            {'id': 'contact-b', 'name': 'Professor B', 'email': 'b@example.invalid', 'hub': {'threads': []}},
            {'id': 'contact-c', 'name': 'Casey Claremont', 'email': 'c@example.invalid', 'hub': {'threads': []}},
        ]})
        self.write('refusals', {'refusals': []})
        self.write('decisions', {'owner_controlled_threads': ['Casey Claremont']})
        self.write('threads', {'lines': []})

    def tearDown(self):
        self.temp.cleanup()

    def write(self, name, value):
        (self.root / 'tables' / (name + '.json')).write_text(json.dumps(value))

    def test_owner_brief_and_self_audit_contract(self):
        row = {'id': 'reply', 'kind': 'reply', 'request': {'inputs': {}}}
        result = {'schema_version': 1, 'task_id': 'reply', 'outcome': 'hold', 'report': 'Checked.',
                  'artifacts': [], 'packet': None,
                  'routing': {'mail': 'owner', 'questions': [], 'independent_submission': None},
                  'owner_brief': {'summary_zh': '这封信回答了问题。', 'decisions_zh': ['决定是否合作'],
                                  'cautions_zh': ['发送前检查附件。']},
                  'self_audit': [{'check': 'salutation', 'status': 'pass', 'note': 'Matched latest Sent.'}]}
        control.validate_result(result, row)
        missing = dict(result); missing.pop('owner_brief')
        with self.assertRaisesRegex(ValueError, 'require owner_brief'):
            control.validate_result(missing, row)
        malformed = dict(result); malformed['self_audit'] = [{'check': 'x', 'status': 'bad', 'note': 'x'}]
        with self.assertRaisesRegex(ValueError, 'self_audit'):
            control.validate_result(malformed, row)

        legacy = {'schema_version': 1, 'task_id': 'research', 'outcome': 'hold', 'report': 'Checked.',
                  'artifacts': [], 'packet': None}
        control.validate_result(legacy, {'id': 'research', 'kind': 'research', 'request': {'inputs': {}}})

    def test_owner_report_filters_contacts_controlled_threads_and_sent_drafts(self):
        work = {
            'owner_drafts': [
                {'task_id': 'draft-a', 'packet': {'to': ['a@example.invalid'], 'cc': [], 'subject': 'A subject'},
                 'result': {'report': 'Fallback first sentence. More.', 'routing': {'mail': 'owner'},
                            'owner_brief': {'summary_zh': '这是给A的摘要。', 'decisions_zh': ['选择期刊'],
                                            'cautions_zh': ['检查正文。']}},
                 'draft_receipt': {'saved_at': '2026-11-02T01:00:00Z'}},
                {'task_id': 'sent-b', 'packet': {'to': ['b@example.invalid'], 'cc': [], 'subject': 'Sent'},
                 'result': {'report': 'Sent.', 'routing': {'mail': 'owner'}},
                 'draft_receipt': {'saved_at': '2026-11-02T01:00:00Z'}, 'sent_receipt': {'id': 'sent-1'}},
                {'task_id': 'controlled', 'packet': {'to': ['c@example.invalid'], 'cc': [], 'subject': 'Owned'},
                 'result': {'report': 'Owned.', 'routing': {'mail': 'owner'}},
                 'draft_receipt': {'saved_at': '2026-11-02T01:00:00Z'}},
            ],
            'user_questions': [{'task_id': 'question-a',
                               'request': {'inputs': {'recipient_emails': ['a@example.invalid']}},
                               'questions': [{'question': '请选择作者顺序', 'source_quote': 'Please choose.'}],
                               'result': {'report': '决定。', 'routing': {'mail': 'owner', 'questions': []}}}],
            'affairs_tasks': [{'id': 'reviewed-a', 'state': 'approved',
                               'request': {'inputs': {'recipient_emails': ['a@example.invalid'], 'subject': 'Unsaved'}},
                               'result': {'report': 'Reviewed.', 'routing': {'mail': 'owner'}}}],
        }
        result = affairs.owner_report(self.root, work)
        self.assertEqual({row['task_id'] for row in result['rows']}, {'draft-a', 'question-a', 'reviewed-a'})
        draft = next(row for row in result['rows'] if row['task_id'] == 'draft-a')
        self.assertEqual(draft['brief'], '这是给A的摘要。')
        self.assertIn('选择期刊', draft['notes'])
        self.assertIn('草稿保存时间', draft['notes'])
        self.assertNotIn('Casey Claremont', result['markdown'])
        self.assertTrue(result['markdown'].startswith('| 人 | 邮箱 | 内容 | 注意事项 |'))

    def test_owner_report_groups_packet_recipients_and_uses_report_fallback(self):
        work = {'owner_drafts': [
            {'task_id': 'grouped',
             'packet': {'to': ['a@example.invalid'], 'cc': ['b@example.invalid'],
                        'subject': 'Grouped subject', 'body': {'content': 'Letter body must not be used.'}},
             'result': {'report': 'Dr. Sawyer checked e.g. examples. A second sentence.',
                        'routing': {'mail': 'owner'}},
             'draft_receipt': {'saved_at': '2026-11-02T01:00:00Z'}}]}

        result = affairs.owner_report(self.root, work)

        self.assertEqual(result['count'], 1)
        row = result['rows'][0]
        self.assertEqual(row['person'], 'Professor A、Professor B')
        self.assertEqual(row['email'], 'a@example.invalid, b@example.invalid')
        self.assertEqual(row['brief'], 'Grouped subject：Dr. Sawyer checked e.g. examples.')
        self.assertIn('Grouped subject：Dr. Sawyer checked e.g. examples.', result['markdown'])
        self.assertNotIn('Letter body must not be used.', result['markdown'])

    def test_owner_report_mailbox_check_handles_draft_sent_missing_and_error(self):
        class FakeMailbox:
            def __init__(self, values):
                self.values = values
                self.calls = []

            def message(self, identity):
                self.calls.append(identity)
                value = self.values[identity]
                if isinstance(value, BaseException):
                    raise value
                return value

        def draft(task_id, recipient, remote_id):
            return {'task_id': task_id,
                    'packet': {'to': [recipient], 'cc': [], 'subject': task_id},
                    'result': {'report': 'Checked.', 'routing': {'mail': 'owner'}},
                    'draft_receipt': {'remote_id': remote_id}}

        fake = FakeMailbox({'exists': {'id': 'exists', 'isDraft': True},
                            'sent': {'id': 'sent', 'isDraft': False},
                            'missing': None,
                            'error': RuntimeError('temporary network failure')})
        work = {'owner_drafts': [draft('exists-task', 'a@example.invalid', 'exists'),
                                 draft('sent-task', 'b@example.invalid', 'sent'),
                                 draft('missing-task', 'a@example.invalid', 'missing'),
                                 draft('error-task', 'b@example.invalid', 'error')]}

        result = affairs.owner_report(self.root, work, check_mailbox=True, mailbox=fake)

        self.assertEqual(fake.calls, ['exists', 'sent', 'missing', 'error'])
        self.assertEqual({row['task_id'] for row in result['rows']}, {'exists-task', 'error-task'})
        self.assertEqual(result['sent_or_deleted'], 2)
        error = next(row for row in result['rows'] if row['task_id'] == 'error-task')
        self.assertEqual(error['mailbox_status'], '未核实')
        self.assertIn('未核实', error['notes'])
        self.assertIn('已发送或已删除: 2', result['markdown'])

    def test_review_digest_risky_items_first(self):
        work = {'reviews': [
            {'binding': {'task_id': 'safe', 'task_revision': 2},
             'request': {'id': 'safe', 'kind': 'reply', 'owner': 'contact-a', 'workflow_hash': control.workflow('reply'),
                         'inputs': {}},
             'result': {'outcome': 'completed', 'report': 'ok', 'artifacts': [],
                        'routing': {'mail': 'auto', 'questions': [], 'independent_submission': None}}},
            {'binding': {'task_id': 'risky', 'task_revision': 4},
             'request': {'id': 'risky', 'kind': 'reply', 'owner': 'contact-a', 'workflow_hash': '0' * 64,
                         'inputs': {'result': {'id': 'result-1'}}},
             'result': {'outcome': 'hold', 'report': 'hold reason', 'artifacts': [{'name': 'x'}],
                        'routing': {'mail': 'owner', 'questions': [], 'independent_submission': None},
                        'self_audit': [{'check': 'attachments', 'status': 'fail', 'note': 'Mismatch'}]}},
        ]}
        digest = control.review_digest(work, self.root)
        self.assertEqual([item['task_id'] for item in digest['items']], ['risky', 'safe'])
        self.assertIn('Professor A', digest['lines'][0])
        self.assertIn('result-1', digest['lines'][0])
        self.assertIn('self_audit fail', digest['lines'][0])
        self.assertIn('workflow changed', digest['lines'][0])
        self.assertIn('attachments: 1', digest['lines'][0])

    def test_blocked_author_proposal_is_separate_and_registry_skips_it(self):
        author = {'full_name': 'Professor A', 'title': 'Professor',
                  'title_evidence_url': 'https://example.invalid/a', 'affiliation': 'Example',
                  'source_url': 'https://doi.org/10.1/source',
                  'emails': [{'email': 'a@example.invalid', 'evidence_url': 'https://example.invalid/a'}],
                  'problem_ids': ['result-a'], 'in_refusals': False, 'refusals_checked_revision': 'a' * 40}
        row = {'id': 'note-a', 'request': {'id': 'note-a', 'source_revision': 'a' * 40,
                                           'inputs': {}}, 'data': {'result': {}, 'review': {}}}
        self.write('refusals', {'refusals': [{'id': 'stop', 'active': True, 'name': 'Professor A',
                                              'emails': ['a@example.invalid'], 'date': '2026-11-02',
                                              'reason': 'stop', 'quote': 'stop', 'source': 'fixture',
                                              'evidence': 'fixture'}]})
        with patch.object(author_contacts, 'reviewed', return_value=[author]):
            pending = author_contacts.pending(row, self.root)
        self.assertEqual(pending['proposals'], [])
        self.assertEqual(pending['blocked'][0]['full_name'], 'Professor A')
        stale = {'author_contacts_needed': [{'task_id': 'note-a', 'reviewed_proposal': {
            'request': row['request'], 'result': {'author_contacts': [author]}, 'review': {}}}],
                 'author_contacts_blocked': [{'task_id': 'note-a', 'full_name': 'Professor A', 'reason': 'stop'}],
                 'registry_updates': []}
        result = registry.reconcile(stale, root=self.root)
        self.assertEqual(result['exceptions'], [])


if __name__ == '__main__':
    unittest.main()
