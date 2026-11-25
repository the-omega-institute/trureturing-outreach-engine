import json
from pathlib import Path
import tempfile
import unittest
from urllib.error import HTTPError
from unittest.mock import patch

from fixture_config import use_example_config
from managed import submission_sync


class Mailbox:
    def __init__(self, message):
        self.message = message
        self.settings = {'live_since': '2026-10-30T00:00:00Z'}

    def messages(self, folder, query, **kwargs):
        self.query = query
        self.options = kwargs
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
    def test_new_coauthored_public_paper_does_not_require_a_private_board_row(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); board(root)
            catalog = {'schema_version': 'research-publication.v1', 'publications': [],
                       'collaborations': [], 'notes': []}
            path = root / 'tables/publications.json'; path.write_text(json.dumps(catalog))
            record = {'arxiv_id': '2611.99115', 'arxiv_version': 'v1',
                      'arxiv_url': 'https://arxiv.org/abs/2611.99115v1',
                      'title': 'Recursive sequence families',
                      'authors': ['Casey Lin', 'Blaire Mercer', 'Avery Reed'],
                      'date': '2026-11-11', 'abstract': 'We characterize nested recurrences. Exact formulas follow.'}
            message = {'from': {'emailAddress': {'address': 'no-reply@arxiv.org'}},
                       'subject': '[arXiv] 2611.99115v1 announcement',
                       'body': {'content': 'https://arxiv.org/abs/2611.99115v1'},
                       'conversationId': 'private-conversation', 'internetMessageId': '<private-receipt>',
                       'receivedDateTime': '2026-11-12T00:00:00Z'}
            report, updated = submission_sync.reconcile(root, Mailbox(message), verify=lambda receipt: record)
            self.assertEqual(report['status'], 'updated')
            self.assertEqual(report['updates'], [])
            self.assertEqual(updated, json.loads((root/'tables/submissions.json').read_text()))
            projected, added = submission_sync.publication_catalog(root, report['publications'] * 2)
            self.assertEqual(len(added), 1)
            self.assertEqual(added[0]['authors'], ', '.join(record['authors']))
            self.assertEqual(added[0]['summary'], record['abstract'])
            self.assertNotIn('private', json.dumps(projected))
            path.write_text(json.dumps(projected))
            self.assertEqual(submission_sync.publication_catalog(root, report['publications']), (None, []))

    def test_public_papers_by_other_authors_cannot_enter_the_catalog(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); board(root)
            path = root / 'tables/publications.json'
            path.write_text(json.dumps({'schema_version': 'research-publication.v1',
                'publications': [], 'collaborations': [], 'notes': []}))
            self.assertEqual(submission_sync.publication_catalog(root, [
                {'authors': ['Another Author'], 'title': 'Not our paper'}]), (None, []))

    def test_public_link_in_correspondence_is_discovered_from_its_own_body(self):
        message = {'from': {'emailAddress': {'address': 'coauthor@example.edu'}},
                   'subject': 'Our paper is public', 'conversationId': 'c1',
                   'uniqueBody': {'content': 'https://arxiv.org/abs/2611.99114v1'},
                   'body': {'content': 'https://arxiv.org/abs/2611.99114v1'}}
        mailbox = Mailbox(message)
        with patch.object(mailbox, 'thread', side_effect=AssertionError('Whole threads are outside the incoming window')):
            collected = submission_sync.collect(mailbox, '2026-11-01T00:00:00Z', '2026-11-02T00:00:00Z')
        self.assertEqual(collected, [{k:v for k,v in message.items() if k!='body'}])
        self.assertTrue(mailbox.options['include_unique_body'])
        self.assertIn('receivedDateTime ge 2026-11-01T00:00:00Z', mailbox.query['$filter'])
        self.assertIn('receivedDateTime le 2026-11-02T00:00:00Z', mailbox.query['$filter'])

    def test_verified_papers_survive_a_partial_failure_and_the_failed_paper_is_retried(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); board(root)
            path = root/'tables/publications.json'
            path.write_text(json.dumps({'schema_version': 'research-publication.v1',
                'publications': [], 'collaborations': [], 'notes': []}))
            message = {'from': {'emailAddress': {'address': 'coauthor@example.edu'}},
                       'subject': 'Two public papers', 'internetMessageId': '<private-receipt>',
                       'uniqueBody': {'content': 'https://arxiv.org/abs/2611.99101v1\n'
                                                 'https://arxiv.org/abs/2611.99102v1'}}
            mailbox = Mailbox(message)
            def record(receipt):
                return {'arxiv_id': receipt['arxiv_id'], 'arxiv_version': 'v1',
                        'arxiv_url': receipt['arxiv_url'], 'title': 'Recurrences '+receipt['arxiv_id'],
                        'authors': ['Casey Lin', 'Blaire Mercer', 'Avery Reed'],
                        'date': '2026-11-01', 'abstract': 'We derive exact recurrence formulas.'}
            def partial(receipt):
                if receipt['arxiv_id']=='2611.99101':
                    raise HTTPError('https://export.arxiv.org/api/query', 429, 'Rate limit', {}, None)
                return record(receipt)
            report, updated = submission_sync.reconcile(root, mailbox, verify=partial)
            self.assertEqual(report['status'], 'updated')
            self.assertEqual([p['url'] for p in report['new_publications']], ['https://arxiv.org/abs/2611.99102'])
            self.assertEqual([p['arxiv_id'] for p in report['deferred']], ['2611.99101'])
            self.assertEqual(updated, json.loads((root/'tables/submissions.json').read_text()))
            catalog, _ = submission_sync.publication_catalog(root, report['publications'])
            path.write_text(json.dumps(catalog))
            retry, _ = submission_sync.reconcile(root, mailbox, verify=record)
            self.assertEqual([p['url'] for p in retry['new_publications']], ['https://arxiv.org/abs/2611.99101'])
            self.assertEqual(retry['deferred'], [])
            catalog, _ = submission_sync.publication_catalog(root, retry['publications'])
            self.assertEqual(len(catalog['publications']), 2)
            self.assertNotIn('private-receipt', json.dumps(catalog))

    def test_incoming_drafts_do_not_supply_publication_candidates(self):
        message = {'subject': '[arXiv] 2611.99101v1', 'isDraft': True,
                   'uniqueBody': {'content': 'https://arxiv.org/abs/2611.99101v1'}}
        self.assertEqual(submission_sync.collect(Mailbox(message), '2026-11-01T00:00:00Z'), [])


    def test_reconcile_reads_arxiv_notice_and_writes_only_board_files(self):
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
