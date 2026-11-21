import json
from pathlib import Path
import tempfile
import unittest

from fixture_config import use_example_config
from managed import arxiv_watch


FIXTURE = Path(__file__).parent / 'fixtures/arxiv_author_watch.xml'


def tables(root, publications=None, papers=None):
    tables_dir = Path(root) / 'tables'
    tables_dir.mkdir()
    catalog = {'schema_version': 'research-publication.v1',
               'publications': publications or [], 'collaborations': [], 'notes': []}
    (tables_dir / 'publications.json').write_text(json.dumps(catalog))
    board = {'schema_version': 1, 'updated_on': '2026-11-04',
             'scope': 'independent-team', 'papers': papers or []}
    (tables_dir / 'submissions.json').write_text(json.dumps(board))


class ArxivWatchTests(unittest.TestCase):
    def setUp(self):
        self.xml = FIXTURE.read_bytes()

    def test_exact_authors_summary_and_replacement_version(self):
        with tempfile.TemporaryDirectory() as folder:
            tables(folder)
            report, catalog = arxiv_watch.watch(
                folder, api_fetch=lambda url: self.xml,
                served_index={'schema_version': 'publication-index.v1', 'publications': []})
            self.assertEqual(report['status'], 'updated')
            self.assertEqual({row['arxiv_id'] for row in report['new_papers_added']},
                             {'2612.00001', '2612.00003', '2612.00004', '2610.00991'})
            added = {row['url'].rsplit('/', 1)[-1]: row for row in catalog['publications']}
            paper = added['2612.00001']
            self.assertEqual(paper['authors'], 'Blaire Mercer, Avery Reed, Rui Example')
            self.assertNotIn('$', paper['summary'])
            self.assertEqual(len(paper['summary']), 500)
            self.assertEqual(added['2610.00991']['venue'], 'arXiv:2610.00991v2')
            self.assertEqual(added['2610.00991']['evidence_url'],
                             'https://arxiv.org/abs/2610.00991v2')
            self.assertEqual(added['2610.00991']['date'], '2026-11-03')

    def test_catalog_board_and_served_index_identities_are_covered(self):
        catalog_paper = {
            'id': 'catalog-paper', 'problem_ids': [], 'topic': 'catalog paper',
            'title': 'Catalog paper', 'authors': 'Blaire Mercer, Avery Reed',
            'date': '2026-10-30', 'url': 'https://arxiv.org/abs/2612.00001',
            'venue': 'arXiv:2612.00001', 'status': 'preprint', 'summary': 'Catalog.',
            'evidence_url': 'https://arxiv.org/abs/2612.00001v1'}
        board_row = {'id': 'board-paper', 'title': 'Board paper',
                     'authors': ['Blaire Mercer', 'Avery Reed'], 'version_date': '2026-10-30',
                     'stage': 'submitted', 'target_journal': 'Journal', 'blocker': '',
                     'next_step_owner': 'Avery Reed', 'workspace': '../mail/person/board',
                     'arxiv_id': '2612.00003', 'arxiv_version': 'v1',
                     'arxiv_url': 'https://arxiv.org/abs/2612.00003',
                     'arxiv_status': 'public'}
        with tempfile.TemporaryDirectory() as folder:
            tables(folder, publications=[catalog_paper], papers=[board_row])
            served = {'schema_version': 'publication-index.v1', 'publications': [{
                'id': 'paper-served', 'problem_ids': [], 'topic': 'served paper',
                'title': 'Served paper', 'authors': 'Blaire Mercer, Avery Reed',
                'date': '2026-10-30', 'url': 'https://arxiv.org/abs/2612.00004',
                'venue': 'arXiv:2612.00004', 'status': 'preprint', 'summary': 'Served.',
                'evidence_url': 'https://arxiv.org/abs/2612.00004v1'}]}
            report, catalog = arxiv_watch.watch(folder, api_fetch=lambda url: self.xml,
                                                served_index=served)
            self.assertEqual({row['arxiv_id'] for row in report['new_papers_added']},
                             {'2610.00991'})
            self.assertEqual(len(catalog['publications']), 2)

    def test_api_failure_is_reported_without_mutating_catalog(self):
        with tempfile.TemporaryDirectory() as folder:
            tables(folder)
            report, catalog = arxiv_watch.watch(
                folder, api_fetch=lambda url: (_ for _ in ()).throw(OSError('offline')),
                served_index={'schema_version': 'publication-index.v1', 'publications': []})
            self.assertEqual(report['status'], 'arxiv-unavailable')
            self.assertEqual(report['new_papers_added'], [])
            self.assertIsNone(catalog)
            self.assertEqual(json.loads((Path(folder) / 'tables/publications.json').read_text())['publications'], [])


if __name__ == '__main__':
    unittest.main()


def setUpModule():
    use_example_config()
