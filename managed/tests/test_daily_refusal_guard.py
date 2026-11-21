"""Daily feature boundaries use both registered problem IDs and attribution."""
from contextlib import ExitStack, nullcontext
from datetime import datetime
import hashlib
import hmac
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

from fixture_config import use_example_config
from managed import daily_local as d
from managed.queue import canonical, digest


DAY = '2026-11-14'
NAME = 'Gordon M. Bellamy'
REFUSAL_ID = 'refusal-bellamy'


class DailyRefusalGuardTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.daily = self.make_daily()

    def make_daily(self):
        root = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        self.stack.enter_context(patch.object(d, 'DATA_ROOT', root))
        self.stack.enter_context(patch.object(d, 'workflow', lambda kind: 'synthetic-workflow'))
        self.stack.enter_context(patch.object(d, 'review_key', lambda: 'secret'))
        config = {'timezone': 'Asia/Singapore', 'daily_state': str(root / 'state'),
                  'historical_state': str(root / 'history'), 'enabled': True,
                  'merged_days': {}, 'account': {'id': 'synthetic-account'}}
        d.save(root / 'config.json', config)
        d.save(root / 'managed/daily/workflow.json', {'enable_live_publication': True})
        d.save(root / 'tables/contacts.json', {'contacts': [
            {'id': 'contact-bellamy', 'name': NAME, 'problem_ids': ['bellamy-question15']}]})
        refusal = {'id': REFUSAL_ID, 'contact_id': 'contact-bellamy', 'name': NAME, 'active': True}
        d.save(root / 'tables/refusals.json', {'refusals': [refusal]})
        packet = root / 'state' / DAY
        for filename in ['candidate.json', 'stats.json', 'digest.json', 'catalogue.json',
                         'source-evidence.json', 'research-focus.json', 'history.json',
                         'cumulative.json', 'facts.json', 'sshx-transcript.json']:
            d.save(packet / filename, {'day': DAY})
        (packet / 'body.txt').write_text('Synthetic daily body\n')
        (packet / 'card.png').write_bytes(b'synthetic-card')
        producer = SimpleNamespace(q=SimpleNamespace(locked=lambda path: nullcontext()),
                                   publish=Mock(return_value={'status': 'fixture-published'}))
        return SimpleNamespace(root=root, config=config, packet=packet, refusal=refusal,
                               producer=producer)

    def fresh(self):
        """A new isolated fixture for each parameterized case."""
        self.stack.close()
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.daily = self.make_daily()
        return self.daily

    def feature(self, **fields):
        entry = {'problem_id': 'question23', 'text': "Gordon Bellamy's 2012 Question 23",
                 'card_title': 'A question about rings', **fields}
        d.save(self.daily.packet / 'digest.json', {'featured': [entry]})

    def envelope(self, verdict='PASS'):
        result = {'binding': d.binding(self.daily.config, DAY), 'reviewer': 'local-claude-loop',
                  'verdict': verdict, 'reason': 'Synthetic independent review',
                  'evidence_ref': 'test:daily-refusal-guard'}
        result['signature'] = hmac.new(b'secret', canonical(result).encode(), hashlib.sha256).hexdigest()
        return result

    def set_active(self, active):
        self.daily.refusal['active'] = active
        d.save(self.daily.root / 'tables/refusals.json', {'refusals': [self.daily.refusal]})

    def assert_blocked(self):
        daily = self.daily
        review = self.envelope()
        with self.assertRaises(ValueError) as error:
            d.record_review(daily.config, DAY, review, 'secret')
        self.assertIn(NAME, str(error.exception)); self.assertIn(REFUSAL_ID, str(error.exception))
        self.assertFalse((daily.packet / ('claude-review-' + digest(review['binding']) + '.json')).exists())
        with self.assertRaises(ValueError) as error:
            d.operate('publish', daily.config, DAY, daily.producer)
        self.assertIn(NAME, str(error.exception)); self.assertIn(REFUSAL_ID, str(error.exception))
        daily.producer.publish.assert_not_called()

    def test_surname_match_blocks_pass_and_publish(self):
        for fields in [{},
                       {'text': 'An algebra question', 'card_title': "BELLAMY's ring question"},
                       {'text': 'An algebra question', 'problem_id': 'new-bellamy-question23'},
                       {'text': 'An algebra question', 'problem_id': 'new_BELLAMY_question23'}]:
            with self.subTest(fields=fields):
                self.fresh(); self.feature(**fields)
                conflicts = d.featured_refusal_conflicts(self.daily.packet, self.daily.root)
                self.assertEqual(len(conflicts), 1)
                self.assertEqual(conflicts[0]['name'], NAME)
                self.assertEqual(conflicts[0]['refusal_id'], REFUSAL_ID)
                self.assertEqual(conflicts[0]['matched_by'], ['surname'])
                self.assert_blocked()

    def test_problem_id_match_blocks_without_surname(self):
        d.save(self.daily.root / 'tables/contacts.json', {'contacts': [
            {'id': 'contact-bellamy', 'problem_ids': ['neutral-question15']}]})
        self.feature(problem_id='neutral-question15', text='A question about rings')
        self.assertEqual(d.featured_refusal_conflicts(self.daily.packet, self.daily.root)[0]['matched_by'],
                         ['problem_id'])
        self.assert_blocked()

    def test_publish_rechecks_boundary_activated_after_pass(self):
        self.feature(); self.set_active(False)
        d.record_review(self.daily.config, DAY, self.envelope(), 'secret')
        self.set_active(True)
        with self.assertRaisesRegex(ValueError, REFUSAL_ID):
            d.operate('publish', self.daily.config, DAY, self.daily.producer)
        self.daily.producer.publish.assert_not_called()

    def test_unrelated_featured_passes_review_and_publish(self):
        for text in ['An unrelated ring question', 'Bellamyy and Superbellamy questions']:
            with self.subTest(text=text):
                daily = self.fresh(); self.feature(text=text)
                self.assertEqual(d.featured_refusal_conflicts(daily.packet, daily.root), [])
                self.assertEqual(d.record_review(daily.config, DAY, self.envelope(), 'secret')['verdict'], 'PASS')
                self.assertEqual(d.operate('publish', daily.config, DAY, daily.producer)['status'],
                                 'fixture-published')
                daily.producer.publish.assert_called_once()

    def test_inactive_refusal_ignored(self):
        self.feature(problem_id='bellamy-question15'); self.set_active(False)
        self.assertEqual(d.featured_refusal_conflicts(self.daily.packet, self.daily.root), [])
        d.record_review(self.daily.config, DAY, self.envelope(), 'secret')
        d.operate('publish', self.daily.config, DAY, self.daily.producer)
        self.daily.producer.publish.assert_called_once()

    def test_non_pass_verdict_recordable_despite_conflict(self):
        for verdict in ['BOUNCE', 'HOLD']:
            with self.subTest(verdict=verdict):
                daily = self.fresh(); self.feature()
                result = d.record_review(daily.config, DAY, self.envelope(verdict), 'secret')
                self.assertEqual(d.read(result['review'])['verdict'], verdict)
                daily.producer.publish.assert_not_called()

    def test_status_includes_conflicts(self):
        self.feature()
        view = d.status(self.daily.root / 'config.json', DAY,
                        datetime(2026, 11, 15, tzinfo=ZoneInfo('Asia/Singapore')))
        self.assertEqual(view['featured_refusal_conflicts'],
                         d.featured_refusal_conflicts(self.daily.packet, self.daily.root))
        self.assertEqual(view['featured_refusal_conflicts'][0]['refusal_id'], REFUSAL_ID)
        self.feature(text='An unrelated question')
        self.assertEqual(d.status(self.daily.root / 'config.json', DAY)['featured_refusal_conflicts'], [])

    def test_surname_accents_folded_on_both_sides(self):
        for name, text in [('Gordon M. Béllamy', "BELLAMY's question"),
                           (NAME, "Béllamy's question")]:
            with self.subTest(name=name, text=text):
                self.fresh()
                self.daily.refusal['name'] = name; self.set_active(True)
                self.feature(text=text)
                self.assertEqual(d.featured_refusal_conflicts(self.daily.packet, self.daily.root)[0]['matched_by'],
                                 ['surname'])

    def test_short_surname_does_not_match_text(self):
        self.daily.refusal['name'] = 'Example Li'; self.set_active(True)
        self.feature(text="Li's question")
        self.assertEqual(d.featured_refusal_conflicts(self.daily.packet, self.daily.root), [])

    def test_no_featured_entries_have_no_conflicts(self):
        for featured in [[], None]:
            with self.subTest(featured=featured):
                d.save(self.daily.packet / 'digest.json', {'featured': featured})
                self.assertEqual(d.featured_refusal_conflicts(self.daily.packet, self.daily.root), [])

    def test_surname_match_without_registered_contact(self):
        d.save(self.daily.root / 'tables/contacts.json', {'contacts': []})
        self.feature()
        self.assert_blocked()


if __name__ == '__main__':
    unittest.main()


def setUpModule():
    use_example_config()
