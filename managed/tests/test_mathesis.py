import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from managed import mathesis as m


class Client:
    def __init__(self): self.calls = 0
    def deliver(self, metadata, source):
        self.calls += 1
        return {'submission': 'same-submission', 'source_sha256': hashlib.sha256(Path(source).read_bytes()).hexdigest()}


class MathesisTests(unittest.TestCase):
    def metadata(self):
        return {'producer':'outreach-infrastructure-test', 'work_id':'test-fixture', 'version':'1',
                'paper':{'purpose':'review_only', 'make_public_after_acceptance':False,
                         'open_to_contributors':False}}

    def test_test_command_cannot_publish(self):
        value = self.metadata()
        value['paper']['make_public_after_acceptance'] = True
        with self.assertRaisesRegex(ValueError, 'private synthetic'):
            m.live_delivery(None, value, None, None, test_only=True)

    def test_test_command_cannot_submit_a_real_producer(self):
        value = self.metadata()
        value['producer'] = 'trureturing-outreach'
        with self.assertRaises(ValueError):
            m.live_delivery(None, value, None, None, test_only=True)

    def test_uncertain_delivery_keeps_identity_and_source(self):
        client = Client()
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            source = root/'fixture.tex'
            source.write_text('synthetic source')
            state = root/'state.json'
            first = m.live_delivery(client, self.metadata(), source, state, True)
            second = m.live_delivery(client, self.metadata(), source, state, True)
            self.assertEqual(first, second)
            source.write_text('changed source')
            with self.assertRaisesRegex(ValueError, 'previously attempted'):
                m.live_delivery(client, self.metadata(), source, state, True)
        self.assertEqual(client.calls, 2)

    def test_historical_receipt_cannot_confirm(self):
        with self.assertRaisesRegex(ValueError, 'historical'):
            m.Mathesis('test-token').confirm({'current':False}, [{'id':'C1'}])

    def test_mapping_preserves_author_order_problem_and_version(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            (root/'source-v2.zip').write_bytes(b'selected source')
            handoff = {'schema_version':'outreach-mathesis-handoff.v1', 'private_correspondence_included':False,
                       'source_note':{'id':'original-result', 'revision':2, 'title':'Result', 'abstract':'Scope',
                                      'creators':[{'name':'B'},{'name':'A'}], 'problem_ids':['original-problem'],
                                      'evidence_url':'https://sample.example.invalid/source', 'license':'CC-BY-4.0'},
                       'files':{'source-v2.zip':hashlib.sha256(b'selected source').hexdigest()}}
            (root/'handoff.json').write_text(json.dumps(handoff))
            decisions = {'decision_ref':'actual-author-decision', 'processing_consent':'ai-review-2026-10-11',
                         'ai_disclosure':{'level':'assisted','statement':'Actual assistance'},
                         'purpose':'review_only', 'source_file':'source-v2.zip', 'make_public_after_acceptance':True}
            value = m.plan(root, decisions)['metadata']
            self.assertEqual([a['name'] for a in value['paper']['authors']], ['B','A'])
            self.assertEqual(value['origin']['problem_ids'], ['original-problem'])
            self.assertEqual(value['version'], '2')
            self.assertFalse(value['paper']['make_public_after_acceptance'])
            decisions.pop('processing_consent')
            with self.assertRaisesRegex(ValueError, 'actual author choices'):
                m.plan(root, decisions)


if __name__ == '__main__':
    unittest.main()
