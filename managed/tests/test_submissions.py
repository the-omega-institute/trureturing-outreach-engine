"""The independent-submission board stays small and portable."""
import copy
import json
from pathlib import Path
import tempfile
import unittest

from fixture_config import use_example_config
from managed import submissions as s
from managed.queue import GateError


def board():
    paper = {
        'id': 'example-paper',
        'title': 'A | B < C',
        'authors': ['Blaire Mercer', 'Avery Reed'],
        'version_date': '2026-10-29',
        'stage': 'ready_to_submit',
        'target_journal': 'Journal A',
        'blocker': '',
        'next_step_owner': 'Avery Reed',
        'workspace': '../mail/person/submission/example-paper',
    }
    return {
        'schema_version': 1,
        'updated_on': '2026-10-31',
        'scope': 'independent-team',
        'papers': [paper],
    }


class SubmissionsTests(unittest.TestCase):
    def test_view_counts_escape_cells_and_reject_stale_reading_view(self):
        data = board()
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / 'tables').mkdir()
            (root / 'tables/submissions.json').write_text(
                json.dumps(data, ensure_ascii=False, indent=2) + '\n')
            self.assertEqual(s.render(root), {
                'total': 1, 'ready_to_submit': 1, 'blocked': 0, 'submitted': 0,
            })
            s.validate(root)
            view = (root / 'tables/SUBMISSIONS.md').read_text()
            self.assertIn('A &#124; B &lt; C', view)
            self.assertIn('| 论文 | 阶段 | 目标期刊 | arXiv | 卡在哪 | 下一步在谁 | 目录 |', view)
            (root / 'tables/SUBMISSIONS.md').write_text(view + 'stale\n')
            with self.assertRaisesRegex(GateError, 'Regenerate'):
                s.validate(root)

    def test_rows_have_only_the_compact_schema(self):
        data = board()
        data['papers'][0]['evidence'] = []
        with self.assertRaisesRegex(GateError, 'compact schema'):
            s.validate_data(data)

    def test_duplicate_identity_and_invalid_stage_are_rejected(self):
        data = board()
        data['papers'].append(copy.deepcopy(data['papers'][0]))
        with self.assertRaisesRegex(GateError, 'Duplicate'):
            s.validate_data(data)

    def test_verified_arxiv_fields_are_allowed_and_rendered(self):
        data = board()
        data['papers'][0].update({
            'arxiv_id': '2611.00992',
            'arxiv_url': 'https://arxiv.org/abs/2611.00992',
            'arxiv_version': 'v1',
            'arxiv_status': 'public',
            'arxiv_title': 'A verified public preprint',
            'arxiv_date': '2026-10-31',
            'arxiv_evidence': 'https://arxiv.org/abs/2611.00992',
        })
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); (root / 'tables').mkdir()
            (root / 'tables/submissions.json').write_text(json.dumps(data))
            s.render(root)
            s.validate(root)
            self.assertIn('2611.00992v1', (root / 'tables/SUBMISSIONS.md').read_text())

    def test_public_arxiv_requires_complete_identity(self):
        data = board()
        data['papers'][0].update({key: '' for key in s.ARXIV_KEYS})
        data['papers'][0]['arxiv_status'] = 'public'
        with self.assertRaisesRegex(GateError, 'verified identity'):
            s.validate_data(data)
        data = board()
        data['papers'][0]['stage'] = 'unknown'
        with self.assertRaisesRegex(GateError, 'stage'):
            s.validate_data(data)

    def test_blocker_is_one_short_line_and_workspace_is_relative(self):
        for blocker in ['line one\nline two', 'x' * 161]:
            data = board(); data['papers'][0]['blocker'] = blocker
            with self.assertRaises(GateError):
                s.validate_data(data)
        for workspace in ['/home/example/paper', '../mail/person/../../elsewhere', '../mail/person\\escape']:
            data = board(); data['papers'][0]['workspace'] = workspace
            with self.assertRaises(GateError):
                s.validate_data(data)


if __name__ == '__main__':
    unittest.main()


def setUpModule():
    use_example_config()
