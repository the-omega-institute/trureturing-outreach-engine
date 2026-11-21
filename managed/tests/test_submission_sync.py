import json
from pathlib import Path
import tempfile
import unittest
from urllib.error import HTTPError

from fixture_config import use_example_config
from managed import submission_sync


class Mailbox:
    def __init__(self, message):
        self.message = message
        self.settings = {'live_since': '2026-10-30T00:00:00Z'}

    def messages(self, folder, query):
        self.query = query
        return [{k: v for k, v in self.message.items() if k != 'body'}]

    def thread(self, conversation):
        return [self.message]


def board(root):
    value = {'schema_version': 1, 'updated_on': '2026-11-04', 'scope': 'independent-team',
             'papers': [{
                 'id': 'paper', 'title': 'Extremal input entropy for deletion and insertion channels',
                 'authors': ['Blaire Mercer', 'Avery Reed'], 'version_date': '2026-11-01',
                 'stage': 'submitted', 'target_journal': 'Journal A', 'blocker': 'ID pending',
                 'next_step_owner': 'Blaire Mercer', 'workspace': '../mail/person/submission/paper'}]}
    path = Path(root) / 'tables'; path.mkdir()
    (path / 'submissions.json').write_text(json.dumps(value))


class SubmissionSyncTests(unittest.TestCase):
    def test_reconcile_reads_arxiv_system_thread_and_writes_only_board_files(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); board(root)
            message = {
                'from': {'emailAddress': {'address': ('@'.join(('no-reply', 'arxiv.org')))}},
                'subject': 'arXiv announcement 2611.00992v1',
                'body': {'content': 'Title: Maximal input entropy for binary two-deletion and two-insertion channels\n'
                                   'Authors: Blaire Mercer, Avery Reed\n'
                                   'https://arxiv.org/abs/2611.00992v1'},
                'conversationId': 'c1', 'receivedDateTime': '2026-11-01T02:00:00Z',
                'internetMessageId': '<receipt@example>',
            }
            mailbox = Mailbox(message)
            record = {'arxiv_id': '2611.00992', 'arxiv_version': 'v1',
                      'arxiv_url': 'https://arxiv.org/abs/2611.00992',
                      'title': 'Maximal input entropy for binary two-deletion and two-insertion channels',
                      'authors': ['Blaire Mercer', 'Avery Reed'], 'date': '2026-10-31'}
            report, updated = submission_sync.reconcile(root, mailbox, verify=lambda receipt: record)
            self.assertEqual(report['status'], 'updated')
            self.assertEqual(updated['papers'][0]['arxiv_id'], '2611.00992')
            submission_sync.write_board(root, updated)
            self.assertIn('2611.00992v1', (root / 'tables/SUBMISSIONS.md').read_text())
            self.assertNotIn('receipt@example', (root / 'tables/submissions.json').read_text())

    def test_arxiv_rate_limit_is_recorded_without_mutating_board(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); board(root)
            message = {
                'from': {'emailAddress': {'address': ('@'.join(('no-reply', 'arxiv.org')))}},
                'subject': 'arXiv announcement 2611.00992v1',
                'body': {'content': 'https://arxiv.org/abs/2611.00992v1'},
                'conversationId': 'c1', 'receivedDateTime': '2026-11-01T02:00:00Z',
            }
            error = HTTPError('https://export.arxiv.org/api/query', 429, 'Too Many Requests', {}, None)
            report, updated = submission_sync.reconcile(
                root, Mailbox(message), verify=lambda receipt: (_ for _ in ()).throw(error))
            self.assertEqual(report['status'], 'arxiv-unavailable')
            self.assertIn('429', report['error'])
            self.assertEqual(updated, json.loads((root / 'tables/submissions.json').read_text()))


if __name__ == '__main__':
    unittest.main()


def setUpModule():
    use_example_config()
