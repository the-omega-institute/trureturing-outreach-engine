import unittest
from unittest.mock import patch

from managed import control
from managed.tasks import KINDS


class ReviewHeadlineTests(unittest.TestCase):
    def row(self, kind='research', inputs=None):
        return {'id': 'task', 'kind': kind, 'request': {
            'id': 'task', 'kind': kind, 'owner': 'scholar',
            'workflow_hash': 'a'*64, 'inputs': inputs or {}}}

    def result(self, **values):
        return {'schema_version': 1, 'task_id': 'task', 'outcome': 'hold',
                'report': 'The theorem holds for all positive integers. Details follow.',
                'artifacts': [], 'packet': None, **values}

    def digest(self, result, inputs=None):
        row = self.row(inputs=inputs)
        with patch('managed.registry.people', return_value=[]), patch.object(control, 'workflow', return_value='a'*64):
            return control.review_digest({'reviews': [{'request': row['request'], 'result': result}]})

    def test_headline_and_self_audit_are_optional_for_every_kind(self):
        for kind in sorted(KINDS):
            with self.subTest(kind=kind):
                control.validate_result(self.result(), self.row(kind))
                control.validate_result(self.result(headline='The theorem holds for all positive integers.',
                    self_audit=[{'check': 'feedback 1', 'status': 'pass', 'note': 'note.tex:12'}]), self.row(kind))

    def test_headline_accepts_one_to_300_characters(self):
        for headline in ('A', 'A'*300, 'The letter answers the question about finite rank.'):
            with self.subTest(headline=headline):
                control.validate_result(self.result(headline=headline), self.row())

    def test_headline_rejects_non_strings_with_specific_error(self):
        for headline in (None, 1, 1.5, False, [], {}):
            with self.subTest(headline=headline), self.assertRaisesRegex(ValueError, '^headline must be a string$'):
                control.validate_result(self.result(headline=headline), self.row())

    def test_headline_rejects_empty_non_english_multiline_and_long_strings(self):
        for headline in ('', ' ', '这是结果。', 'First\nSecond', 'First\n', 'First\rSecond',
                         'First\u2028Second', 'A'*301):
            with self.subTest(headline=headline), self.assertRaisesRegex(ValueError, 'headline must be one English line'):
                control.validate_result(self.result(headline=headline), self.row())

    def test_prompts_require_headline_and_numbered_feedback_audit_for_every_kind(self):
        for kind in sorted(KINDS):
            with self.subTest(kind=kind):
                prompt = ' '.join(control.task_prompt(self.row(kind)).split())
                for phrase in (
                    'Return a top-level headline: one English sentence (<=300 chars)',
                    'stating the main result or what the letter does, precise enough for a reviewer to judge',
                    'For every kind, self_audit is allowed as an optional top-level list of',
                    '{check, status: pass|fail|n/a, note}',
                    'When inputs.review_feedback is present, self_audit must contain one entry per numbered point of review_feedback.reason',
                    "check exactly 'feedback N' (N the point number)",
                    'status pass if resolved, fail otherwise',
                    'a note citing where it is resolved (file and line or a short verbatim quote)',
                ):
                    self.assertIn(phrase, prompt)

    def test_digest_headline_fallback_order_and_truncation(self):
        cases = (
            ({'headline': 'Explicit result.', 'owner_brief': {'summary_zh': '中文摘要。'}}, 'Explicit result.'),
            ({'owner_brief': {'summary_zh': '中文摘要。'}}, '中文摘要。'),
            ({}, 'The theorem holds for all positive integers'),
            ({'headline': 'A'*250}, 'A'*200),
            ({'owner_brief': {'summary_zh': '中'*250}}, '中'*200),
            ({'report': 'A'*250 + '. More.'}, 'A'*200),
            ({'report': 'First line\ncontinues. More.'}, 'First line continues'),
        )
        for values, expected in cases:
            with self.subTest(values=values):
                digest = self.digest(self.result(**values))
                self.assertEqual(digest['items'][0]['headline'], expected)
                self.assertEqual(digest['items'][0]['feedback'], 'feedback —')
                self.assertIn(' | feedback — | risks: ', digest['lines'][0])
                self.assertEqual(digest['lines'][1], '  ↳ ' + expected)
                self.assertEqual(len(digest['lines']), 2)

    def test_digest_feedback_counts_distinct_requested_points_and_unresolved(self):
        inputs = {'review_feedback': {'reason': '1. Fix the proof.\n2. Fix the citation. 7. Fix the scope.\n2. Same citation.'}}
        audits = [{'check': check, 'status': status, 'note': 'note.tex:12'} for check, status in (
            ('feedback 1', 'pass'), ('feedback 1', 'pass'), ('feedback 2', 'pass'),
            ('feedback 7', 'fail'), ('feedback 8', 'pass'), ('feedback 7 extra', 'pass'),
            ('Feedback 7', 'pass'), ('feedback  7', 'pass'), ('scope', 'pass'))]
        digest = self.digest(self.result(self_audit=audits), inputs)
        self.assertEqual(digest['items'][0]['feedback'], 'feedback 2/3')
        self.assertIn('feedback unresolved: 1/3', digest['items'][0]['risk_flags'])
        self.assertIn(' | feedback 2/3 | risks: ', digest['lines'][0])
        audits[3]['status'] = 'pass'
        item = self.digest(self.result(self_audit=audits), inputs)['items'][0]
        self.assertEqual(item['feedback'], 'feedback 3/3')
        self.assertFalse(any(flag.startswith('feedback unresolved:') for flag in item['risk_flags']))

    def test_digest_missing_failed_or_na_feedback_stays_unresolved(self):
        inputs = {'review_feedback': {'reason': '1. Supply the proof. 2. Cite the scope.'}}
        for status in (None, 'fail', 'n/a'):
            audits = [] if status is None else [{'check': 'feedback 1', 'status': status, 'note': 'Unresolved.'}]
            with self.subTest(status=status):
                item = self.digest(self.result(self_audit=audits), inputs)['items'][0]
                self.assertEqual(item['feedback'], 'feedback 0/2')
                self.assertIn('feedback unresolved: 2/2', item['risk_flags'])
        item = self.digest(self.result(), {'review_feedback': {'reason': 'No numbered points.'}})['items'][0]
        self.assertEqual(item['feedback'], 'feedback 0/0')

    def test_digest_has_no_lean_text_heuristic(self):
        item = self.digest(self.result(report='No Lean was run; the Lean declaration covers clause (ii).'))['items'][0]
        self.assertNotIn('lean claim without link', item['risk_flags'])


if __name__ == '__main__':
    unittest.main()


class DigestCoverageFlagTests(unittest.TestCase):
    def test_review_covered_by_another_passed_note_is_flagged(self):
        request = {'id': 'shortnote-dup', 'kind': 'shortnote', 'owner': 'scholar',
                   'workflow_hash': 'a'*64, 'inputs': {}}
        result = {'schema_version': 1, 'task_id': 'shortnote-dup', 'outcome': 'completed',
                  'report': 'Proves the sibling recurrence.', 'artifacts': [], 'packet': None}
        snapshot = {'reviews': [{'request': request, 'result': result,
                                 'binding': {'task_id': 'shortnote-dup'}}],
                    'note_coverage_overlap': [{'task_id': 'shortnote-dup', 'state': 'awaiting-review',
                                               'result_id': 'sibling', 'covering_note_task_id': 'shortnote-family'}]}
        with patch('managed.registry.people', return_value=[]), patch.object(control, 'workflow', return_value='a'*64):
            digest = control.review_digest(snapshot)
        self.assertIn('covered by PASSed note shortnote-family', digest['items'][0]['risk_flags'])
