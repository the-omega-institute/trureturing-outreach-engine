"""Offline owner to-do projection; all correspondence here is synthetic."""
from contextlib import redirect_stderr, redirect_stdout
import copy
from datetime import datetime, timedelta, timezone
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from fixture_config import use_example_config
from managed import control
from managed.owner_todo import owner_todo, render_markdown


NOW = datetime(2026, 11, 3, 12, tzinfo=timezone.utc)


def draft(identity, at='2026-11-02T12:00:00Z', **changes):
    return {'id': identity, 'kind': 'reply', 'owner': 'contact-a', 'state': 'owner-draft',
            'request': {'owner': 'contact-a', 'inputs': {'mailbox_observation': {
                'email': 'a@example.invalid', 'at': at}}},
            'result': {'headline': '已备好回复。', 'packet': {'to': ['a@example.invalid'], 'cc': [],
                'subject': '合成测试主题'}, 'owner_brief': {'summary_zh': '回复中的结论已核对。',
                'cautions_zh': ['发送前确认附件。'], 'decisions_zh': []}},
            'draft_receipt': {'remote_id': 'draft-' + identity, 'at': at}, 'sent_receipt': None,
            'draft_status': 'present',
            'packet_override': None, **changes}


class OwnerTodoTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        (self.root / 'tables').mkdir()
        (self.root / 'managed').mkdir()
        (self.root / 'config.json').write_text('{}')
        self.contacts = [
            {'id': 'contact-a', 'name': '甲教授', 'email': 'a@example.invalid'},
            {'id': 'contact-a-alias', 'name': '甲教授', 'email': 'alias@example.invalid',
             'relationship': {'alias_of': 'a@example.invalid'}},
            {'id': 'contact-b', 'name': '乙博士', 'email': 'b@example.invalid'},
        ]
        self.write_contacts()
        (self.root / 'tables' / 'refusals.json').write_text('{"refusals": []}')

    def write_contacts(self):
        (self.root / 'tables' / 'contacts.json').write_text(json.dumps({'contacts': self.contacts}))

    def view(self, work):
        return owner_todo(work, self.root, now=NOW)

    def test_questions_show_original_quote_and_age_oldest_first_across_threads(self):
        newer = {'task_id': 'choice-new', 'opened_at': '2026-11-03T00:00:00Z',
                 'request': {'owner': 'contact-a-alias'},
                 'questions': [{'id': 'author-order', 'question': '是否采用这个署名顺序？',
                                'source_quote': 'Our suggested author order is Avery and Ellis.'}]}
        older = {'task_id': 'choice-old', 'opened_at': '2026-11-01T00:00:00Z',
                 'request': {'owner': 'contact-a'},
                 'questions': [{'id': 'meeting', 'question': '是否会面？', 'source_quote': 'Can we meet?'}]}
        work = {'user_questions': [newer, older], 'mailbox_account': 'owner@example.invalid'}
        original = copy.deepcopy(work)
        rows = self.view(work)['questions']
        self.assertEqual([r['task_id'] for r in rows], ['choice-old', 'choice-new'])
        self.assertEqual([r['person'] for r in rows], ['甲教授', '甲教授'])
        self.assertEqual([r['age_seconds'] for r in rows], [216000, 43200])
        self.assertIn('“Can we meet?”', rows[0]['content'])
        self.assertIn('已等待 2 天 12 小时', rows[0]['notes'])
        self.assertEqual(work, original)

    def test_question_falls_back_to_registered_address_and_recent_inbound(self):
        self.contacts[2]['hub'] = {'events': [{'kind': 'reply', 'at': '2026-11-01T09:30:00Z'}]}
        self.write_contacts()
        row, = self.view({'user_questions': [{'task_id': 'q', 'request': {'owner': 'contact-b'},
            'questions': [{'id': 'choice', 'question': '请选择。'}]}]})['questions']
        self.assertEqual(row['email'], 'b@example.invalid')
        self.assertEqual(row['incoming_date'], '2026-11-01')
        self.assertIn('登记的最近来信日期：2026-11-01', row['notes'])

    def test_reason_can_come_from_id_or_chinese_question_text(self):
        questions = [
            {'id': 'journal-choice', 'question': '请选择一个。'},
            {'id': 'choice', 'question': '是否接受合作承诺？'},
            {'id': 'invitation', 'question': '你愿意参加吗？'},
            {'id': 'choice', 'question': '如何介绍机构身份？'},
            {'id': 'choice', 'question': '是否提及个人健康情况？'},
        ]
        rows = self.view({'user_questions': [{'task_id': 'q', 'result': {
            'routing': {'questions': questions}}}]})['questions']
        self.assertEqual([q['reason'] for q in rows], [
            '涉及投稿或发表安排', '涉及合作或个人承诺', '涉及邀请或时间安排', '涉及个人身份或项目情况',
            '涉及个人事项'])

    def test_question_projection_fallback_and_email_alias_lookup(self):
        task = draft('q', owner='not-registered')
        del task['request']['inputs']['mailbox_observation']['email']
        task['result']['packet']['to'] = ['ALIAS@example.invalid']
        rows = self.view({'affairs_tasks': [task], 'user_questions': [
            {'task_id': 'q', 'questions': [{'id': 'q1', 'question': '请选择。'}]}]})['questions']
        self.assertEqual(rows[0]['person'], '甲教授')
        self.assertEqual(rows[0]['email'], 'ALIAS@example.invalid')

    def test_drafts_sorted_by_save_date_and_brief_notes(self):
        first = draft('first', '2026-10-30T12:00:00Z')
        newest = draft('newest', '2026-11-03T10:00:00Z')
        newest['result']['owner_brief'].update(cautions_zh=['检' * 61, '不应出现的第二条注意事项'],
                                              decisions_zh=['确定作者顺序', '确定发表时间'])
        headline = draft('headline', '2026-11-02T22:00:00+08:00')
        headline['result'].pop('owner_brief')
        work = {'affairs_tasks': [first, newest, headline]}
        original = copy.deepcopy(work)
        rows = self.view(work)['drafts']
        self.assertEqual([r['task_id'] for r in rows], ['newest', 'headline', 'first'])
        self.assertEqual(rows[0]['content'], '回复中的结论已核对。')
        self.assertEqual(rows[1]['content'], '已备好回复。')
        self.assertEqual(rows[0]['notes'].split('；')[:2], ['0 个附件', '检' * 59 + '…'])
        self.assertIn('确定作者顺序；确定发表时间；来信日期：2026-11-03', rows[0]['notes'])
        self.assertNotIn('第二条', rows[0]['notes'])
        self.assertEqual(work, original)

    def test_sent_drafts_and_tasks_without_saved_owner_draft_are_excluded(self):
        sent = {'remote_id': 'sent-id'}
        work = {'affairs_tasks': [draft('ready'), draft('stale-state', sent_receipt=sent),
            draft('verified', state='verified', sent_receipt=sent), draft('no-receipt', draft_receipt=None),
            draft('empty-receipt', draft_receipt={}), draft('approved', state='approved'),
            draft('unchecked', draft_status=None)]}
        view = self.view(work)
        self.assertEqual([r['task_id'] for r in view['drafts']], ['ready'])
        self.assertEqual(view['old_drafts'], [])

    def test_older_than_21_days_moves_to_low_priority_with_exact_boundary(self):
        tasks = [draft('oldest', '2026-09-01'), draft('boundary', '2026-10-13T12:00:00Z'),
                 draft('just-old', '2026-10-13T19:59:59+08:00'), draft('newest')]
        view = self.view({'affairs_tasks': tasks})
        self.assertEqual([r['task_id'] for r in view['drafts']], ['newest', 'boundary'])
        self.assertEqual([r['task_id'] for r in view['old_drafts']], ['just-old', 'oldest'])
        later = owner_todo({'affairs_tasks': tasks}, self.root, now=NOW + timedelta(seconds=1))
        self.assertIn('boundary', [r['task_id'] for r in later['old_drafts']])

    def test_missing_and_invalid_dates_stay_in_drafts_and_sort_last(self):
        view = self.view({'affairs_tasks': [draft('missing', None), draft('invalid', 'not-a-date'),
                                          draft('dated')]})
        self.assertEqual([r['task_id'] for r in view['drafts']], ['dated', 'missing', 'invalid'])
        self.assertIsNone(view['drafts'][1]['incoming_date'])
        self.assertIn('来信日期：未知', view['drafts'][1]['notes'])
        self.assertEqual(view['old_drafts'], [])

    def test_packet_override_recipient_and_observation_fallback(self):
        override = draft('override', owner='unregistered', packet_override={'to': ['b@example.invalid']})
        fallback = draft('fallback', owner='unregistered')
        fallback['result']['packet']['to'] = []
        rows = self.view({'affairs_tasks': [override, fallback]})['drafts']
        self.assertEqual([(r['person'], r['email']) for r in rows], [
            ('乙博士', 'b@example.invalid'), ('甲教授', 'a@example.invalid')])

    def test_event_dates_precede_newer_saves_and_live_flags_lead_notes(self):
        later = draft('later')
        later['result']['packet']['body'] = 'Our call is on November 18, 2026.'
        earlier = draft('earlier', '2026-10-29T12:00:00Z', owner_edited=True,
                        has_placeholder=True, attachment_count=2, draft_stale=True)
        earlier['result']['owner_brief']['summary_zh'] = '11月12日线上会议。'
        newest = draft('newest', '2026-11-03T10:00:00Z')
        view = self.view({'owner_drafts': [{**row, 'task_id': row['id']} for row in [newest, later, earlier]]})
        self.assertEqual([r['task_id'] for r in view['drafts']], ['earlier', 'later', 'newest'])
        self.assertEqual([r['event_date'] for r in view['drafts']], ['2026-11-12', '2026-11-18', None])
        self.assertTrue(view['drafts'][0]['notes'].startswith('需要你补内容；你已改过；2 个附件；草稿箱状态待复查'))

    def test_confirmation_reminders_collapse_and_owner_controlled_counts_messages(self):
        confirmations = [
            {'category': 'editorial-action', 'subject': 'Confirm co-authorship: ABC-2026-1',
             'received': '2026-10-31T07:00:00Z', 'name': 'Journal One', 'from': 'editor@example.invalid'},
            {'category': 'editorial-action', 'subject': 'Reminder - Confirm co-authorship: abc-2026-1',
             'received': '2026-11-02T07:00:00Z', 'name': 'Journal One', 'from': 'editor@example.invalid'},
            {'category': 'editorial-action', 'subject': 'Verify your authorship. Manuscript ID: 98765',
             'received': '2026-11-03T07:00:00Z', 'name': 'Journal Two', 'from': 'editor@example.invalid'},
            {'category': 'editorial-action', 'subject': 'Decision on manuscript. Manuscript ID: 77777',
             'received': '2026-11-03T08:00:00Z', 'name': 'Journal Two', 'from': 'editor@example.invalid'},
        ]
        def inbound(identity, email, at):
            return {'internetMessageId': identity, 'intake_reason': 'owner-controlled',
                    'receivedDateTime': at, 'from': {'emailAddress': {'address': email}}}
        earlier = inbound('<1>', 'a@example.invalid', '2026-11-01T07:00:00Z')
        later = inbound('<2>', 'alias@example.invalid', '2026-11-03T07:00:00Z')
        view = self.view({'mailbox_attention': confirmations,
                          'mailbox_needs_classification': [earlier, later, earlier]})
        self.assertEqual([row['manuscript_id'] for row in view['confirmations']], ['98765', 'abc-2026-1'])
        self.assertEqual(view['confirmations'][1]['incoming_date'], '2026-11-02')
        self.assertIn('abc-2026-1：点开邮件中的作者身份确认链接', view['confirmations'][1]['content'])
        row, = view['owner_controlled']
        self.assertEqual((row['person'], row['email'], row['unanswered_count'], row['incoming_date']),
                         ('甲教授', 'alias@example.invalid', 2, '2026-11-03'))

    def test_markdown_escapes_all_cells_and_retains_exact_columns(self):
        self.contacts[0].update(name='甲|乙\r\n丙', email='a|b@example.invalid')
        self.write_contacts()
        task = draft('escape')
        task['result']['packet']['to'] = ['a|b@example.invalid']
        task['result']['owner_brief'].update(summary_zh='结论 A|B\n及路径\\|项',
                                             cautions_zh=[], decisions_zh=['先确认 A|B\r再发送'])
        view = self.view({'affairs_tasks': [task]})
        text = render_markdown(view)
        self.assertEqual(text.count('| 人 | 邮箱 | 内容 | 注意事项 |'), 1)
        self.assertIn('甲\\|乙<br>丙', text)
        self.assertIn('a\\|b@example.invalid', text)
        self.assertIn('结论 A\\|B<br>及路径' + '\\' * 3 + '|项', text)
        self.assertIn('先确认 A\\|B<br>再发送', text)
        self.assertNotIn('\r', text)
        self.assertIn('A|B\n', json.dumps(view, ensure_ascii=False).replace('\\n', '\n'))

    def test_empty_inventory_omits_sections_in_markdown(self):
        view = self.view({})
        self.assertEqual((view['questions'], view['drafts'], view['old_drafts']), ([], [], []))
        self.assertEqual(render_markdown(view), '')

    def run_cli(self, *args):
        output = io.StringIO()
        with patch.object(control, 'ROOT', self.root), redirect_stdout(output), \
                patch.object(control, 'Ledger', side_effect=AssertionError('must not open ledger')), \
                patch.object(control.Releases, 'run', side_effect=AssertionError('must stay offline')):
            control.main(list(args))
        return output.getvalue()

    def test_cli_json_and_default_markdown_are_offline(self):
        task = draft('cli', datetime.now(timezone.utc).isoformat())
        task['result']['owner_brief']['summary_zh'] = '合成内容 A|B\n下一行。'
        sent = draft('sent', '2026-08-01', draft_status='sent')
        missing = draft('missing', '2026-08-01', draft_status='missing')
        sent['result']['owner_brief']['summary_zh'] = '合成已发'
        missing['result']['owner_brief']['summary_zh'] = '合成消失'
        work = self.root / 'work.json'
        work.write_text(json.dumps({'affairs_tasks': [task, sent, missing]}))
        before = {p: p.read_bytes() for p in self.root.rglob('*') if p.is_file()}
        value = json.loads(self.run_cli('todo', '--work', str(work), '--format', 'json'))
        self.assertEqual(value['drafts'][0]['content'], '合成内容 A|B\n下一行。')
        self.assertEqual([r['draft_status'] for r in value['operator_check']['rows']], ['sent', 'missing'])
        markdown = self.run_cli('todo', '--work', str(work))
        self.assertIn('合成内容 A\\|B<br>下一行。', markdown)
        for hidden in ('供操作员核对', '合成已发', '合成消失', '很久未回'):
            self.assertNotIn(hidden, markdown)
        self.assertEqual(markdown, self.run_cli('todo', '--work', str(work), '--format', 'md'))
        self.assertEqual(before, {p: p.read_bytes() for p in self.root.rglob('*') if p.is_file()})

    def test_cli_requires_work_path(self):
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
            self.run_cli('todo')
        self.assertEqual(error.exception.code, 2)


if __name__ == '__main__':
    unittest.main()


class ProcessNoiseTests(unittest.TestCase):
    def test_process_cautions_are_skipped(self):
        from managed.owner_todo import PROCESS_NOISE
        import re
        self.assertTrue(re.search(PROCESS_NOISE, '这只是待独立Claude核对的提案，未保存邮箱草稿'))
        self.assertFalse(re.search(PROCESS_NOISE, '涉及署名，需你确认作者顺序'))


def setUpModule():
    use_example_config()
