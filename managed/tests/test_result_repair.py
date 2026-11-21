"""Bounded same-Agent corrections with checkpointed, replay-safe admission."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from managed.control import Ledger, advance, validate_result
from test_tasks import CMA, Releases, request
from test_packet_artifact import PacketReleases


class RepairCMA(CMA):
    def __init__(self, result):
        super().__init__()
        self.result = result
        self.corrected = {k: v for k, v in result.items() if k != 'refusals_checked_revision'}
        self.admit_calls = []
        self.lose_repair = False
        self.before_repair = None

    def admit(self, agent_id, prompt, key):
        self.admit_calls.append(key)
        repair = key.startswith('outreach-repair-')
        if repair and self.before_repair:
            self.before_repair(key, prompt)
        self.commands.setdefault(key, {'agent': agent_id, 'prompt': prompt,
                                      'response_id': 'resp_' + key if repair else 'resp_original'})
        if repair and self.lose_repair:
            self.lose_repair = False
            raise TimeoutError('Repair accepted; observation lost')
        return {'response_id': self.commands[key]['response_id']}

    def response(self, response_id):
        result = self.result if response_id == 'resp_original' else self.corrected
        return {'status': 'completed', 'output': [{'type': 'message', 'content': [
            {'type': 'output_text', 'text': json.dumps(result)}]}]}


class ResultRepairTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.root = Path(self.temp.name)
        self.remote = Releases(); self.ledger = Ledger(self.remote, self.root)
        self.req = request()
        self.row = self.ledger.tasks.add(self.req)
        self.result = {'schema_version': 1, 'task_id': self.row['id'], 'outcome': 'completed',
                       'report': 'Checked result.', 'artifacts': [], 'packet': None,
                       'refusals_checked_revision': 'a'*40}
        self.cma = RepairCMA(self.result)
        self.cfg = {'paused': False, 'kinds': {'research': True}, 'max_active': 1, 'agent_template': {}}

    def tearDown(self):
        self.ledger.tasks.close(); self.temp.cleanup()

    def tick(self):
        advance(self.ledger, self.cma, self.cfg, self.root)
        self.ledger.save(force=True)  # Match the production tick's final flush.
        return self.ledger.tasks.get(self.row['id'])

    def restore(self):
        self.ledger.tasks.close()
        self.root = self.root / 'restored'; self.root.mkdir()
        self.ledger = Ledger(self.remote, self.root)

    def test_specific_contract_errors(self):
        with self.assertRaisesRegex(ValueError, "unexpected keys \\['refusals_checked_revision'\\] / missing keys \\[\\]"):
            validate_result(self.result, self.row)
        broken = copy.deepcopy(self.result); broken.pop('packet')
        with self.assertRaisesRegex(ValueError, "missing keys \\['packet'\\]"):
            validate_result(broken, self.row)
        with self.assertRaisesRegex(ValueError, 'expected a JSON object'):
            validate_result([], self.row)

    def test_extra_key_gets_one_correction_on_same_agent_and_validates(self):
        initial = self.tick()
        self.assertEqual(initial['state'], 'held')
        self.assertIn('refusals_checked_revision', initial['data']['repair_error'])
        row = self.tick()
        self.assertEqual(row['state'], 'awaiting-review')
        self.assertEqual(row['data']['repair_attempts'], 1)
        self.assertEqual(row['data']['result'], self.cma.corrected)
        self.assertNotIn('reason', row['data'])
        repair = row['data']['repairs'][0]
        self.assertIn(initial['data']['repair_error'], repair['prompt'])
        self.assertIn('Return only the corrected final JSON object for the same task', repair['prompt'])
        self.assertIn('Do not redo the research', repair['prompt'])
        self.assertIn('Keep artifacts unchanged unless the error concerns them', repair['prompt'])
        self.assertEqual(repair['source_response_id'], 'resp_original')
        self.assertEqual(row['data']['response_text'], initial['data']['response_text'])
        self.assertEqual(len(self.cma.creates), 1)
        self.assertEqual({c['agent'] for c in self.cma.commands.values()}, {'agt_test'})
        self.assertEqual(len(self.cma.commands), 2)
        self.assertEqual(self.cma.retired, ['agt_test'])

    def test_repeated_failure_stops_after_two_and_retires_sandbox(self):
        self.cma.corrected = self.result
        self.tick()
        row = self.tick()
        self.assertEqual(row['data']['repair_attempts'], 1)
        self.assertFalse(self.cma.retired)
        row = self.tick()
        self.assertEqual(row['state'], 'held')
        self.assertEqual(row['data']['repair_attempts'], 2)
        self.assertIn('automatic repair limit reached', row['data']['reason'])
        self.assertEqual(json.loads(row['data']['response_text']), self.result)
        self.assertTrue(row['data']['cleanup']['deleted'])
        self.assertTrue(row['data']['cleanup']['sandbox_retired'])
        self.assertEqual(len(self.cma.commands), 3)
        self.restore(); self.tick(); self.tick()
        self.assertEqual(len(self.cma.admit_calls), 3)
        self.assertEqual(self.cma.retired, ['agt_test'])

    def test_repair_intent_is_saved_before_call_and_lost_admission_replays_same_key(self):
        self.tick()
        def checkpoint(key, prompt):
            with tempfile.TemporaryDirectory() as temp:
                saved = Ledger(self.remote, temp)
                try:
                    row = saved.tasks.get(self.row['id'])
                    self.assertEqual(row['state'], 'admitting')
                    self.assertEqual(row['data']['repair_attempts'], 1)
                    self.assertEqual(row['data']['repairs'][0]['command_key'], key)
                    self.assertEqual(row['data']['repairs'][0]['prompt'], prompt)
                finally:
                    saved.tasks.close()
        self.cma.before_repair = checkpoint
        self.cma.lose_repair = True
        row = self.tick()
        self.assertEqual(row['state'], 'admission-unknown')
        self.cma.before_repair = None
        self.restore()
        retry_at = row['data']['recovery']['next_at']
        self.ledger.tasks.clock = lambda: retry_at
        row = self.tick()
        self.assertEqual(row['state'], 'awaiting-review')
        self.assertEqual(row['data']['repair_attempts'], 1)
        self.assertEqual(self.cma.admit_calls[-1], self.cma.admit_calls[-2])
        self.assertEqual(len(self.cma.commands), 2)  # Original plus one admitted correction.
        calls = list(self.cma.admit_calls)
        self.restore(); self.tick()
        self.assertEqual(self.cma.admit_calls, calls)

    def test_legacy_malformed_hold_is_repaired_without_restarting_task(self):
        data = {'agent_id': 'agt_test', 'response_id': 'resp_original',
                'response_text': json.dumps(self.result),
                'reason': 'Completed CMA result needs repair: Invalid CMA result contract'}
        self.ledger.tasks.update(self.row['id'], self.row['revision'], 'held', data)
        row = self.tick()
        self.assertEqual(row['state'], 'awaiting-review')
        self.assertEqual(row['data']['repair_attempts'], 1)
        self.assertEqual(len(self.cma.commands), 1)
        self.assertFalse(self.cma.creates)
        self.assertIn('refusals_checked_revision', row['data']['repairs'][0]['error'])

    def test_pause_or_disabled_kind_does_not_start_a_repair(self):
        self.tick()
        self.cfg['paused'] = True
        self.tick()
        self.cfg.update(paused=False, kinds={'research': False})
        self.tick()
        self.assertEqual(len(self.cma.commands), 1)
        self.assertFalse(self.cma.retired)

    def test_deleted_agent_is_not_given_a_correction_turn(self):
        row = self.tick()
        self.ledger.tasks.update(row['id'], row['revision'], 'held',
                                 {**row['data'], 'cleanup': {'deleted': True, 'sandbox_retired': True}})
        self.tick()
        self.assertEqual(len(self.cma.commands), 1)

    def test_repairs_observe_concurrency_and_owner_serialization(self):
        self.tick()
        other = self.ledger.tasks.add(request('other', owner=self.req['owner']))
        other = self.ledger.tasks.update(other['id'], other['revision'], 'running', {'response_id': 'other-response'})
        response = self.cma.response
        self.cma.response = lambda i: {'status': 'in_progress'} if i == 'other-response' else response(i)
        self.cfg['max_active'] = 2
        self.tick()
        self.assertEqual(len(self.cma.commands), 1)  # Same owner, even with a free slot.
        self.ledger.tasks.update(other['id'], other['revision'], 'failed', {'reason': 'fixture finished'})
        self.assertEqual(self.tick()['state'], 'awaiting-review')

    def test_shortnote_extra_key_repair_preserves_original_artifact_bytes(self):
        self.remote = PacketReleases(); self.ledger.releases = self.remote
        self.req = request('note-1', kind='shortnote')
        self.row = self.ledger.tasks.add(self.req)
        self.cfg['kinds'] = {'shortnote': True}
        payloads = {'note.tex': b'fixture source', 'note.pdf': b'%PDF-fixture'}
        artifacts = [{'release_tag': 'outreach-task-note-1', 'name': name,
                      'agent_path': 'deliverables/'+name, 'role': role,
                      'bytes': len(payloads[name]), 'sha256': hashlib.sha256(payloads[name]).hexdigest()}
                     for name, role in [('note.tex', 'note-tex'), ('note.pdf', 'note-pdf')]]
        self.result.update(task_id='note-1', artifacts=artifacts, formalization_gaps=[],
                           value_assessment={'verdict': 'incremental', 'reasons': 'Extends the known bound.'})
        self.cma = RepairCMA(self.result)
        self.cma.artifact = lambda agent, path, size, sha: payloads[path.split('/')[-1]]
        self.tick()
        self.assertFalse(self.remote.files)
        row = self.tick()
        self.assertEqual(row['state'], 'awaiting-review')
        self.assertEqual(row['data']['repair_attempts'], 1)
        self.assertEqual(self.remote.files, {('outreach-task-note-1', name): raw for name, raw in payloads.items()})
        self.assertEqual(row['data']['result']['artifacts'],
                         [{k: v for k, v in a.items() if k != 'agent_path'} for a in artifacts])
        self.assertEqual(self.cma.retired, ['agt_test'])

    def test_policy_gate_failure_uses_same_bounded_repair_and_rechecks_gate(self):
        self.cma.result = self.cma.corrected
        response = self.cma.response
        self.cma.response = lambda _: {'status': 'in_progress'}
        self.tick()
        self.cma.response = response
        error = ValueError('Current thread is owner-controlled')
        with patch('managed.automation.owner_gate', side_effect=error) as gate:
            first = self.tick()
            second = self.tick()
            row = self.tick()
        self.assertEqual(gate.call_count, 3)
        self.assertEqual(first['data']['repair_error'], str(error))
        self.assertEqual(second['data']['repair_attempts'], 1)
        self.assertEqual(row['state'], 'held')
        self.assertEqual(row['data']['repair_attempts'], 2)
        self.assertEqual(row['data']['repairs'][-1]['error'], str(error))
        self.assertNotIn('result', row['data'])
        self.assertTrue(row['data']['cleanup']['deleted'])

    def test_artifact_validation_exceptions_include_exact_error_in_repair(self):
        self.cma.result = self.cma.corrected
        for error in (ValueError('artifact mismatch'), KeyError('artifact-field'), TypeError('artifact type')):
            with self.subTest(error=error):
                with patch('managed.control.verify_artifacts', side_effect=error):
                    row = self.tick()
                self.assertEqual(row['state'], 'held')
                self.assertEqual(row['data']['repair_error'], str(error))
                # Each subcase reuses the same task/Agent, within its two-turn budget.
        self.assertEqual(row['data']['repair_attempts'], 2)
        self.assertTrue(row['data']['cleanup']['deleted'])


if __name__ == '__main__':
    unittest.main()
