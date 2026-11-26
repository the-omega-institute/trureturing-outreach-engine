from copy import deepcopy
import hashlib
import io
import json
from contextlib import redirect_stdout
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from fixture_config import use_cli_config
from managed import automation, control, prior_work_evidence as evidence, prior_work_issues as filing
from managed.control import Ledger, main
from managed.formalization_requests import REPO
from managed.queue import digest
from test_tasks import Releases, request


RESULT = 'example-parity'
SOURCE = 'https://sample.example.invalid/posing.pdf'
LEAN = 'https://github.com/the-omega-institute/trureturing/blob/' + 'a' * 40 + '/Proof.lean'
ISSUE = 'https://github.com/' + REPO + '/issues/42'
ITEM = {'authors': ['A. Example', 'B. Example'], 'date': 'Sept 2026',
        'locator': 'Theorem 2', 'url': 'https://sample.example.invalid/earlier.pdf',
        'proves': 'Proves that a(n) is even for every nonnegative integer n.'}
PRIVATE = {'private': True, 'date': '2026-09-09', 'proves': 'QUOTED PRIVATE TEXT',
           'locator': 'UNPUBLISHED TALK TITLE', 'url': 'https://sample.example.invalid/private-id'}
CHECK = {'verdict': 'prior-exists', 'items': [ITEM], 'statement': 'For every n ≥ 0, a(n) is even.',
         'posing_locator': 'Problem 2, Example et al., arXiv:2608.01234v1',
         'posing_url': SOURCE, 'lean_url': LEAN}
TEXT = b"""# Evidence

## Prior-work check
**Verdict:** prior-exists
**Posed statement:** For every nonnegative n, a(n) is even.
**Posing locator:** Problem 2, Section 4

| Authors | Year/date | Locator | URL | What it proves |
| --- | --- | --- | --- | --- |
| A. Example | September 2026 | Theorem 2 | [PDF](https://sample.example.invalid/earlier.pdf) | Gives the parity proof for every n. |

### Private report
- Kind: private correspondence
- Date: 2026-09-09
- Locator: UNPUBLISHED TALK TITLE
- What it proves: QUOTED PRIVATE TEXT

## Unrelated sources
Authors: Outside
URL: https://sample.example.invalid/unrelated.pdf
What it proves: Something else.
"""


class FilingTests(unittest.TestCase):
    def setUp(self):
        folder = self.enterContext(tempfile.TemporaryDirectory())
        self.root = Path(folder)
        self.remote = Releases()
        self.ledger = Ledger(self.remote, folder)
        self.addCleanup(self.ledger.tasks.close)
        self.tasks = self.ledger.tasks
        self.config = {'paused': False}

    def note(self, name='checked-note', *, rid=RESULT, check=CHECK, resolution='2026-11-16',
             artifact=None, artifact_name='evidence.md', pipeline=None, verdict='PASS', **input_flags):
        inputs = {'pipeline': {'result_id': rid, **(pipeline or {})},
                  'result': {'id': rid, 'upstream': {'resolved_at': resolution, 'declaration_gids': ['Parity.result']}},
                  **input_flags}
        row = self.tasks.add({**request(name, kind='shortnote'), 'inputs': inputs})
        result = {'task_id': name, 'outcome': 'completed', 'artifacts': []}
        if check is not None:
            result['prior_work_check'] = check
        if artifact is not None:
            result['artifacts'].append({'role': 'evidence', 'name': artifact_name,
                                        'release_tag': 'outreach-task-' + name,
                                        'bytes': len(artifact), 'sha256': hashlib.sha256(artifact).hexdigest()})
        review = {'reviewer': 'local-claude-loop', 'verdict': verdict,
                  'binding': {'task_id': name, 'result_hash': digest(result),
                              'workflow_hash': row['request']['workflow_hash']}}
        return self.tasks.update(name, row['revision'], 'handoff-ready', {'result': result, 'review': review})

    def snapshot(self):
        return {'tasks': self.tasks.all(), **filing.inventory_view(self.tasks)}

    def select(self, row):
        references = [{'task_id': row['id'], 'result_hash': digest(row['data']['result'])}]
        filing.ingest(self.tasks, filing.envelope(references, 'secret'), 'secret')

    def test_cli_filters_dates_and_projects_only_safe_issue_fields(self):
        later = {**ITEM, 'date': '2026-11-17', 'url': 'https://sample.example.invalid/later'}
        same = {**ITEM, 'date': '2026-11-16', 'url': 'https://sample.example.invalid/same'}
        overlap = {**ITEM, 'date': '2026', 'url': 'https://sample.example.invalid/overlap'}
        undated = {**ITEM, 'date': None, 'url': 'https://sample.example.invalid/undated'}
        unverified = {**ITEM, 'verified': False, 'url': 'https://sample.example.invalid/unverified'}
        background = {**ITEM, 'proves': 'Does not prove the posed claim.', 'url': 'https://sample.example.invalid/background'}
        self.note(check={**CHECK, 'items': [same, PRIVATE, undated, later, ITEM, overlap, unverified, background],
                         'statement': CHECK['statement'] * 20})
        self.note('first-known', rid='different', check={'verdict': 'first-known', 'items': [ITEM]})
        self.note('not-reviewed', rid='different', verdict='BOUNCE')
        stale = self.note('stale', rid='stale')
        result = {**stale['data']['result'], 'changed': True}
        self.tasks.update(stale['id'], stale['revision'], stale['state'], {**stale['data'], 'result': result})
        path = self.root / 'work.json'
        path.write_text(json.dumps(automation.work(self.tasks, self.root)))
        stdout = io.StringIO()
        with redirect_stdout(stdout), patch('managed.control.stage') as stage:
            main(['file-prior-work-issues', '--work', str(path), '--dry-run'])
            stage.assert_not_called()
        value = json.loads(stdout.getvalue())
        self.assertEqual(value['count'], 1)
        self.assertEqual(value['needs_evidence'], [])
        note, = value['notes']
        self.assertEqual(note['key'], digest([RESULT, [ITEM['url']]]))
        title, body = (note['issue'][k] for k in ('title', 'body'))
        self.assertLessEqual(len(title), 256)
        self.assertTrue(title.endswith(' | ' + CHECK['posing_locator']))
        for expected in (LEAN, SOURCE, ITEM['proves'], 'Atlas'):
            self.assertIn(expected, body)
        for excluded in (same['url'], overlap['url'], later['url'], unverified['url'], background['url'],
                         undated['url'], PRIVATE['url'], PRIVATE['locator'], PRIVATE['proves'],
                         PRIVATE['date'], 'correspondence', 'date unknown'):
            self.assertNotIn(excluded, body)

    def test_legacy_reads_exact_evidence_and_caches_projection(self):
        note = self.note(check=None, artifact=TEXT)
        initial = filing.plan(self.snapshot(), self.root)
        self.assertEqual(initial['count'], 0)
        self.assertEqual(initial['needs_evidence'][0]['task_id'], note['id'])
        self.assertEqual(initial['needs_evidence'][0]['reason'], filing.BACKFILL_REQUIRED)
        github = Mock()
        with patch('managed.publication.download') as download:
            filing.advance(self.ledger, self.config, self.root, github)
            download.assert_not_called()
        github.assert_not_called()
        directory = self.root / note['id']
        directory.mkdir()
        path = directory / 'evidence.md'
        path.write_bytes(TEXT + b'tampered')
        invalid = filing.plan(self.snapshot(), self.root, self.root)
        self.assertEqual(invalid['count'], 0)
        self.assertIn('differs from the exact PASS', invalid['notes'][0]['reason'])
        path.write_bytes(TEXT)
        offline = filing.plan(self.snapshot(), self.root, self.root)
        self.assertEqual(offline['count'], 1)
        self.assertTrue(offline['notes'][0]['requires_backfill'])
        self.select(note)
        calls = []

        def github(_, method='GET', body=None):
            calls.append(method)
            if method == 'GET':
                return []
            self.assertEqual(body, offline['notes'][0]['issue'])
            self.assertNotIn('https://sample.example.invalid/unrelated.pdf', body['body'])
            return {'html_url': ISSUE}

        with patch('managed.publication.download', return_value=path) as download:
            filing.advance(self.ledger, self.config, self.root, github)
            filing.advance(self.ledger, self.config, self.root, github)
            download.assert_called_once()
        self.assertEqual(calls, ['GET', 'POST'])
        work = automation.work(self.tasks, self.root)
        self.assertEqual(work['prior_work_issues'][0]['issue_url'], ISSUE)
        self.assertNotIn('QUOTED PRIVATE TEXT', json.dumps(work['prior_work_checks']))
        self.assertEqual(filing.plan(work, self.root)['count'], 0)

    def test_headed_private_report_stays_one_dated_item(self):
        text = TEXT.decode().replace('- Date: 2026-09-09', '- Date: 2027-01-05')
        check = evidence.parse_text(text)
        self.assertEqual([item for item in check['items'] if item.get('private')], [{'private': True}])
        later_only = evidence.parse_text(text.split('| Authors')[0] + text[text.index('### Private report'):])
        self.assertEqual(later_only['items'], [{'private': True}])
        self.note(check=later_only)
        github = Mock()
        filing.advance(self.ledger, self.config, self.root, github)
        github.assert_not_called()
        pending, = automation.work(self.tasks, self.root)['prior_work_issues']
        self.assertEqual(pending['reason'], 'Only private prior-work evidence; needs a public source')

    def test_publication_exclusions_are_pending_in_preview_and_work(self):
        (self.root / 'tables').mkdir()
        (self.root / 'tables/decisions.json').write_text(json.dumps({'holds': [
            {'id': 'held-result', 'decision': 'HOLD'},
            {'problem_ids': ['checked-id-only'], 'decision': 'JOINT-WORK'},
            {'task_id': 'held-sibling', 'decision': 'HOLD'},
        ]}))
        self.note('held', rid='held-result')
        self.note('held-from-check', rid=None, check={**CHECK, 'result_id': 'checked-id-only'})
        self.note('joint', rid='joint-result', joint_work=True)
        self.note('sair', rid='sair-result')
        self.note('cpp', rid='cpp-result', track='CPP')
        self.note('held-sibling', rid='shared-result', check=None)
        self.note('public-looking-sibling', rid='shared-result')
        preview = filing.plan(self.snapshot(), self.root)
        self.assertEqual(preview['count'], 0)
        self.assertEqual(len(preview['notes']), 6)
        github = Mock()
        filing.advance(self.ledger, self.config, self.root, github)
        github.assert_not_called()
        pending = automation.work(self.tasks, self.root)['prior_work_issues']
        self.assertEqual(len(pending), 6)
        self.assertTrue(all(item['reason'].startswith('Excluded from public filing') for item in pending))

    def test_privacy_phrases_block_context_but_mathematical_correspondence_is_public(self):
        self.note('statement', rid='private-statement',
                  check={**CHECK, 'statement': 'Conjecture from a personal communication'})
        self.note('locator', rid='private-locator',
                  check={**CHECK, 'posing_locator': 'E-Mail from the author'})
        self.note('public', check={**CHECK, 'items': [{**ITEM, 'locator': 'Galois correspondence',
                                                      'proves': 'Uses the Galois correspondence.'}]})
        preview = filing.plan(self.snapshot(), self.root)
        self.assertEqual(preview['count'], 1)
        public = next(item for item in preview['notes'] if 'issue' in item)
        self.assertIn('Galois correspondence', public['issue']['body'])
        github = Mock(side_effect=[[], {'html_url': ISSUE}])
        filing.advance(self.ledger, self.config, self.root, github)
        get, post = github.call_args_list
        self.assertIn('state=all', get.args[0])
        self.assertEqual(post.args[1], 'POST')
        pending = [item for item in self.snapshot()['prior_work_issues'] if item['state'] == 'pending']
        self.assertEqual(len(pending), 2)
        self.assertTrue(all('private-source wording' in item['reason'] for item in pending))

    def test_source_wording_variants_and_addresses_in_urls_stay_out(self):
        for text in ('communicated privately by the poser', 'The authors emailed a proof.', 'two e-mails',
                     'pers. comm., 2026', 'personal correspondence', 'stated in correspondence',
                     'correspondence with the authors, Sept 2026', 'our correspondence', 'confidential note'):
            with self.subTest(private=text):
                self.assertTrue(evidence.PRIVATE.search(text))
                check = evidence.project({**CHECK, 'items': [{**ITEM, 'locator': text}]})
                self.assertEqual(check['items'], [{'private': True}])
        for text in ('Galois correspondence', 'in one-to-one correspondence with the partitions of n',
                     'The orbits are in correspondence with the vertices.', 'by the RSK correspondence'):
            with self.subTest(public=text):
                self.assertFalse(evidence.PRIVATE.search(text))
        self.assertEqual(evidence.project({**CHECK, 'items': [{**ITEM, 'private': 'true'}]})['items'],
                         [{'private': True}])
        self.assertEqual(evidence.project({**CHECK, 'items': [{**ITEM, 'private': 0}]})['items'],
                         evidence.project({**CHECK, 'items': [ITEM]})['items'])
        self.assertEqual(evidence.public_url('https://sample.example.invalid/e.pdf?contact=a.example@example.invalid'), '')
        self.note(check={**CHECK, 'items': [{**ITEM, 'url': 'https://sample.example.invalid/e.pdf?contact=a.example@example.invalid'}]})
        note, = filing.plan(self.snapshot(), self.root)['notes']
        self.assertNotIn('issue', note)

    def test_missing_and_overlapping_dates_and_missing_id_are_pending(self):
        cases = [
            ('unknown-date', RESULT, None, '2026-11-16', 'missing its release date'),
            ('missing-both', 'other', None, None, 'Missing trureturing resolution date'),
            ('year-overlap', 'other-year', '2026', '2026-11-16', 'date range overlaps'),
            ('month-overlap', 'other-month', '2026-11', '2026-11-16', 'date range overlaps'),
            ('missing-id', None, ITEM['date'], '2026-11-16', 'Missing or invalid upstream result ID'),
            ('invalid-date', 'invalid-date', 'last year', '2026-11-16', 'could not be read'),
        ]
        for name, rid, when, resolution, _ in cases:
            self.note(name, rid=rid, check={**CHECK, 'items': [{**ITEM, 'date': when}]}, resolution=resolution)
        github = Mock()
        filing.advance(self.ledger, self.config, self.root, github)
        github.assert_not_called()
        pending = {item['task_id']: item for item in automation.work(self.tasks, self.root)['prior_work_issues']}
        self.assertEqual(len(pending), len(cases))
        for name, rid, _, _, reason in cases:
            self.assertEqual(pending[name]['result_id'], rid)
            self.assertIn(reason, pending[name]['reason'])

    def test_legacy_skips_private_filenames_and_old_cached_projections(self):
        for index, filename in enumerate(('Private-evidence.MD', 'author-correspondence.txt',
                                          'notes-private.md', 'Emails.md')):
            row = self.note(f'excluded-{index}', rid=f'excluded-{index}', check=None,
                            artifact=TEXT, artifact_name=filename)
            self.select(row)
        row = self.note('old-cache', check=None, artifact=TEXT)
        filing.save(self.tasks, filing.CHECK_PREFIX, row['id'],
                    {'result_hash': digest(row['data']['result']), 'check': evidence.project(CHECK)})
        github = Mock()
        with patch('managed.publication.download') as download:
            filing.advance(self.ledger, self.config, self.root, github)
            download.assert_not_called()
        github.assert_not_called()
        self.select(row)
        self.assertEqual(filing.plan(self.snapshot(), self.root)['needs_evidence'][0]['reason'],
                         filing.NEEDS_EVIDENCE)

    def test_legacy_heading_required_and_unparseable_evidence_stays_pending(self):
        malformed = [
            TEXT.replace(b'### Private report\n', b''),
            b'## Prior-work check\nVerdict: prior-exists\nAuthors: Example\nKind: private\n',
            b'## Prior-work check\nVerdict: prior-exists\n### Citation\nAuthors: Example\nAuthors: Other\n',
            b'## Prior-work check\nVerdict: prior-exists\n### Citation\nAuthors: Example\nKind: private\n',
        ]
        for data in malformed:
            with self.assertRaisesRegex(evidence.UnparseableEvidence, 'one heading per entry'):
                evidence.parse_text(data.decode())
        row = self.note(check=None, artifact=malformed[0])
        self.select(row)
        path = self.root / 'evidence.md'
        path.write_bytes(malformed[0])
        github = Mock()
        with patch('managed.publication.download', return_value=path) as download:
            filing.advance(self.ledger, self.config, self.root, github)
            filing.advance(self.ledger, self.config, self.root, github)
            download.assert_called_once()
        github.assert_not_called()
        pending, = self.snapshot()['prior_work_issues']
        self.assertIn('one heading per entry', pending['reason'])

    def test_read_failures_are_visible_and_back_off_until_recovery(self):
        row = self.note(check=None, artifact=TEXT)
        self.select(row)
        path = self.root / 'evidence.md'
        path.write_bytes(b'tampered')
        now = self.tasks.clock()
        delay = filing.recheck.RECHECK_SECONDS
        github = Mock(side_effect=[[], {'html_url': ISSUE}])
        with patch('managed.publication.download', side_effect=[
                RuntimeError('download failure (HTTP 404)'), path, path]) as download:
            for offset, attempts in ((0, 1), (delay - 1, 1), (delay, 2), (2 * delay, 2)):
                with patch.object(self.tasks, 'clock', return_value=now + offset):
                    filing.advance(self.ledger, self.config, self.root, github)
                self.assertEqual(download.call_count, attempts)
                work = automation.work(self.tasks, self.root)
                self.assertEqual(work['prior_work_evidence_failure_count'], 1)
                progress = work['prior_work_checks'][row['id']]
                self.assertEqual(progress['failures'], attempts)
                self.assertIn('HTTP 404' if attempts == 1 else 'exact PASS', progress['last_error'])
            github.assert_not_called()
            path.write_bytes(TEXT)
            with patch.object(self.tasks, 'clock', return_value=now + 3 * delay):
                filing.advance(self.ledger, self.config, self.root, github)
        self.assertEqual(self.snapshot()['prior_work_evidence_failure_count'], 0)
        self.assertEqual(self.snapshot()['prior_work_issues'][0]['issue_url'], ISSUE)

    def test_public_fields_never_carry_an_address(self):
        item = {**ITEM, 'authors': 'A. Example <a.example@example.invalid>',
                'proves': ITEM['proves'] + ' Contact b.example+notes@sample.example.invalid.'}
        self.note(check={**CHECK, 'items': [item],
                         'posing_locator': CHECK['posing_locator'] + ' (c.example@example.invalid)'})
        note, = filing.plan(self.snapshot(), self.root)['notes']
        text = note['issue']['title'] + note['issue']['body']
        self.assertNotIn('@', text)
        self.assertIn('A. Example', text)
        self.assertIn(ITEM['proves'], text)

    def test_phase3d_installation_data_repository_is_not_public_evidence(self):
        repository = 'example/synthetic-private-data'
        with patch.object(evidence, 'DATA_REPO', repository), patch('managed.location.DATA_REPO', repository):
            private_url = 'https://github.com/' + repository + '/blob/' + 'a' * 40 + '/evidence.md'
            check = {**CHECK, 'items': [{**ITEM, 'url': private_url}]}
            self.assertEqual(evidence.project(check)['items'], [])
            self.note(check=check)
            preview = filing.plan(self.snapshot(), self.root)
        self.assertEqual(preview['count'], 0)
        self.assertEqual(preview['notes'][0]['reason'], 'Prior-exists evidence has no usable public citation')

    def test_result_contract_and_policy_cover_the_structured_check(self):
        policy = json.loads((Path(control.__file__).parent / 'research/workflow.json').read_text())
        self.assertEqual(set(policy['short_note']['prior_work_check']),
                         {'result_field', 'resolution_date', 'items', 'evidence_section'})
        for malformed in (None, [], {'verdict': 'earlier', 'items': []}, {'verdict': 'prior-exists'},
                          {'verdict': 'prior-exists', 'items': ['text']}):
            with self.subTest(check=malformed), self.assertRaises(ValueError):
                evidence.validate(malformed)
        # Private mail results cannot carry the field at all.
        reply = {'schema_version': 1, 'task_id': 'mail', 'outcome': 'completed', 'report': 'r',
                 'artifacts': [], 'packet': None, 'prior_work_check': CHECK}
        with self.assertRaisesRegex(ValueError, "unexpected keys \\['prior_work_check'\\]"):
            control.validate_result(reply, {'id': 'mail', 'kind': 'reply', 'request': {'inputs': {}}})

    def test_lost_post_recovers_by_result_even_after_sources_change(self):
        note = self.note()
        issues = []

        def github(path, method='GET', body=None):
            if method == 'GET':
                self.assertIn('state=all', path)
                return issues
            self.assertEqual(path, filing.ISSUES)
            self.assertIsNotNone(self.remote.body['snapshot'])
            issues.append({**body, 'title': 'Edited title', 'state': 'closed', 'html_url': ISSUE})
            raise TimeoutError('response lost')

        filing.advance(self.ledger, self.config, self.root, github)
        previous, = self.snapshot()['prior_work_issues']
        self.assertEqual(previous['state'], 'pending')
        self.assertEqual(previous['last_error'], 'TimeoutError')
        with tempfile.TemporaryDirectory() as temp:
            restored = Ledger(self.remote, temp)
            try:
                row = restored.tasks.get(note['id'])
                changed = deepcopy(row['data'])
                changed['result']['prior_work_check']['items'][0]['url'] = 'https://sample.example.invalid/revised'
                changed['review']['binding']['result_hash'] = digest(changed['result'])
                restored.tasks.update(row['id'], row['revision'], row['state'], changed)
                filing.advance(restored, self.config, self.root, github)
                current, = filing.inventory_view(restored.tasks)['prior_work_issues']
                self.assertEqual(current['issue_url'], ISSUE)
                self.assertNotEqual(current['key'], previous['key'])
            finally:
                restored.tasks.close()
        self.assertEqual(len(issues), 1)

    def test_pending_until_resolution_date_from_result_table(self):
        self.note(resolution=None)
        with patch.dict('os.environ', {'CMA_ACCESS_TOKEN': ''}):
            filing.advance(self.ledger, self.config, self.root)
        entry, = self.snapshot()['prior_work_issues']
        self.assertEqual(entry['reason'], 'Missing trureturing resolution date')
        (self.root / 'tables').mkdir()
        (self.root / 'tables/results.json').write_text(json.dumps({'results': [
            {'id': RESULT, 'upstream': {'resolution_date': '2026-10'}}]}))
        filing.advance(self.ledger, self.config, self.root,
                       lambda path, method='GET', body=None: [] if method == 'GET' else {'html_url': ISSUE})
        self.assertEqual(self.snapshot()['prior_work_issues'][0]['issue_url'], ISSUE)

    def test_tick_is_paused_and_bounded_with_one_issue_per_result(self):
        for n in range(filing.PER_TICK + 2):
            self.note(f'note-{n}', rid=f'result-{n}')
        self.note('sibling', rid='result-0', pipeline={'resolution_date': '2026-11-11'})
        self.assertEqual(filing.plan(self.snapshot(), self.root)['count'], filing.PER_TICK + 2)
        posted = []

        def github(path, method='GET', body=None):
            if method == 'GET':
                return posted[:]
            issue = {**body, 'html_url': ISSUE + str(len(posted))}
            posted.append(issue)
            return issue

        filing.advance(self.ledger, {'paused': True}, self.root, github)
        self.assertEqual(posted, [])
        filing.advance(self.ledger, self.config, self.root, github)
        self.assertEqual(len(posted), filing.PER_TICK)
        filing.advance(self.ledger, self.config, self.root, github)
        self.assertEqual(len(posted), filing.PER_TICK + 2)

    def test_backfill_stages_only_signed_exact_result_references(self):
        row = self.note(check=None, artifact=TEXT)
        path = self.root / 'work.json'
        path.write_text(json.dumps(self.snapshot()))
        with redirect_stdout(io.StringIO()), patch('managed.control.review_key', return_value='secret'), \
                patch('managed.control.stage', return_value='staged') as stage:
            main(['file-prior-work-issues', '--work', str(path)])
        value, prefix, *_ = stage.call_args.args
        self.assertEqual(prefix, filing.BACKFILL_PREFIX)
        self.assertEqual(value['notes'], [{'task_id': row['id'], 'result_hash': digest(row['data']['result'])}])
        with self.assertRaisesRegex(ValueError, 'signature failed'):
            filing.ingest(self.tasks, value, 'wrong')
        path = self.root / (prefix + '-' + digest(value) + '.json')
        path.write_text(json.dumps(value))
        self.remote.upload(path)
        self.assertEqual(self.ledger.ingest('secret'), [])
        self.assertEqual(self.ledger.ingest('secret'), [])
        self.assertEqual(filing.state(self.tasks, filing.BACKFILL_KEY), {row['id']: digest(row['data']['result'])})


if __name__ == '__main__':
    unittest.main()


def setUpModule():
    use_cli_config()
