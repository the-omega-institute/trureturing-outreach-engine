"""Terminal CMA failures must retry within a budget or name an operator exit."""
import copy
import json
import unittest
from unittest.mock import patch

from managed import control
from managed.automation import work
from managed.tasks import ACTIVE
from test_sandbox_pool import PoolFixture, TRANSPORT_ERROR


RATE_ERROR = {'code': 'agent_turn_failed', 'message':
              'exceeded retry limit, last status: 429 Too Many Requests, request id: synthetic-request'}
SIZE_ERROR = {'code': 'agent_turn_failed', 'message':
              'unexpected status 413 Payload Too Large: proxy request body is too large, '
              'url: http://127.0.0.1:9091/responses'}

INPUT_NOT_WRITTEN = {'code': 'input_not_written', 'message':
                     'the retired connection refused the original prompt before it was written; '
                     'the original message is preserved and was not resent'}
BILLING_BUSY = {'code': 'agent_turn_failed', 'message':
                'unexpected status 409 Conflict: {"error":"conflict","error_code":1004,'
                '"message":"Conflict: Billing settlement is busy; retry"}, url: http://127.0.0.1:9091/responses'}


class ExecutionRecoveryTests(PoolFixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.cfg.pop('sandbox_pool')

    def running(self, name, error, **extra):
        row = self.queue(name)
        data = {'agent_id': 'agt_'+name, 'response_id': 'resp_'+name,
                'command_key': 'original-'+name, 'prompt': 'Synthetic task context', **extra}
        self.cma.response = lambda _: {'status': 'failed', 'error': error}
        return self.ledger.tasks.update(name, row['revision'], 'running', data)

    def legacy(self, name, error, deleted=True, cached=True, **extra):
        row = self.running(name, error, **extra)
        data = {**row['data'], 'cleanup': {'deleted': deleted, 'sandbox_retired': deleted},
                'reason': 'Original CMA response ended failed', 'credit_checked': True}
        if cached:
            data['response_error'] = error
        return self.ledger.tasks.update(name, row['revision'], 'failed', data)

    def inventory(self):
        with patch('managed.daily_local.status', return_value={}):
            return work(self.ledger.tasks, control.ROOT)

    def test_rate_limit_backoff_survives_restore_and_retains_same_agent(self):
        row = self.running('limited', RATE_ERROR)
        self.tick()
        waiting = self.ledger.tasks.get(row['id'])
        recovery = waiting['data']['recovery']
        self.assertIn(waiting['state'], ACTIVE)
        self.assertEqual(recovery['error_class'], 'rate-limit')
        self.assertEqual(recovery['attempts'], 1)
        self.assertEqual(recovery['next_at'], self.now+60)
        self.assertFalse(self.cma.retired)
        self.restore()
        self.now += 59
        self.tick()
        self.assertFalse(self.cma.admissions)
        self.now += 1
        self.tick()
        second = self.ledger.tasks.get(row['id'])
        self.assertEqual(second['data']['recovery']['attempts'], 2)
        self.assertEqual(second['data']['recovery']['next_at'], self.now+120)
        self.assertEqual(self.cma.admissions[0][:2],
                         (row['data']['agent_id'], row['data']['prompt']))
        self.assertFalse(self.cma.creates)

    def test_rate_limit_configured_cap_requires_operator_and_cleans_up(self):
        self.cfg['recovery'] = {'rate_limit_max_attempts': 1}
        row = self.running('cap', RATE_ERROR)
        self.tick()
        self.now += 60
        self.tick()
        failed = self.ledger.tasks.get(row['id'])
        self.assertEqual(failed['state'], 'failed')
        self.assertEqual(failed['data']['recovery']['status'], 'needs-operator')
        self.assertEqual(failed['data']['recovery']['attempts'], 1)
        self.assertIn('limit', failed['data']['recovery']['reason'])
        self.assertTrue(failed['data']['cleanup']['deleted'])
        self.restore()
        self.tick()
        self.assertEqual(len(self.cma.admissions), 1)

    def test_default_rate_limit_cap_is_five_and_delay_is_bounded(self):
        row = self.running('default-cap', RATE_ERROR)
        self.tick()
        for attempt in range(1, 6):
            row = self.ledger.tasks.get(row['id'])
            self.assertEqual(row['data']['recovery']['attempts'], attempt)
            self.assertLessEqual(row['data']['recovery']['next_at']-self.now, 3600)
            self.now = row['data']['recovery']['next_at']
            self.tick()
        self.assertEqual(self.ledger.tasks.get(row['id'])['data']['recovery']['status'], 'needs-operator')
        self.assertEqual(len(self.cma.admissions), 5)

    def test_pause_and_disabled_kind_defer_rate_retry(self):
        row = self.running('pause', RATE_ERROR)
        self.tick()
        self.now += 60
        self.cfg['paused'] = True
        self.tick()
        self.cfg['paused'] = False
        self.cfg['kinds']['research'] = False
        self.tick()
        self.assertFalse(self.cma.admissions)
        self.assertEqual(self.ledger.tasks.get(row['id'])['data']['recovery']['attempts'], 1)

    def test_network_uses_transport_budget_and_marks_exhaustion(self):
        row = self.running('network', TRANSPORT_ERROR)
        self.tick()
        self.assertEqual(self.ledger.tasks.get(row['id'])['data']['transport_retries'], 1)
        self.tick()
        self.tick()
        failed = self.ledger.tasks.get(row['id'])
        self.assertEqual(failed['data']['recovery']['error_class'], 'network')
        self.assertEqual(failed['data']['recovery']['status'], 'needs-operator')
        self.assertEqual(failed['data']['recovery']['attempts'], control.TRANSPORT_RETRIES)
        self.assertEqual(len(self.cma.admissions), control.TRANSPORT_RETRIES)
        self.assertTrue(all('-transport-' in key for _, _, key in self.cma.admissions))

    def test_413_keeps_complete_context_and_requests_local_takeover(self):
        row = self.running('large', SIZE_ERROR)
        original = copy.deepcopy(row['request'])
        self.tick()
        failed = self.ledger.tasks.get(row['id'])
        self.assertEqual(failed['state'], 'failed')
        self.assertEqual(failed['request'], original)
        self.assertEqual(failed['data']['prompt'], row['data']['prompt'])
        self.assertEqual(failed['data']['recovery']['error_class'], 'request-too-large')
        self.assertEqual(failed['data']['recovery']['status'], 'needs-operator')
        self.assertEqual(failed['data']['recovery']['suggested_exit'], 'local-codex')
        self.assertFalse(self.cma.admissions)

    def test_unknown_error_has_rewrite_exit_without_automatic_retry(self):
        row = self.running('unknown', {'code': 'permission_denied', 'message': 'Forbidden'})
        self.tick()
        failed = self.ledger.tasks.get(row['id'])
        self.assertEqual(failed['data']['recovery']['status'], 'needs-operator')
        self.assertEqual(failed['data']['recovery']['suggested_exit'], 'rewrite-task')
        self.assertFalse(self.cma.admissions)

    def test_existing_17_failures_are_classified_once_including_cleaned_agents(self):
        for i in range(14):
            self.legacy('rate-'+str(i), RATE_ERROR)
        for i in range(2):
            self.legacy('size-'+str(i), SIZE_ERROR)
        self.legacy('network', TRANSPORT_ERROR)
        self.cfg['paused'] = True  # Classify while paused; dispatch waits for activation.
        self.cma.response = lambda _: self.fail('Cached errors must avoid a new CMA lookup')
        self.tick()
        rows = self.ledger.tasks.all()
        self.assertEqual(sum(r['state'] == 'queued' for r in rows), 15)
        self.assertEqual(sum(r['data']['recovery']['status'] == 'needs-operator' for r in rows), 2)
        self.assertEqual(self.cma.creates, {})
        revisions = [r['revision'] for r in rows]
        self.restore()
        self.cfg['paused'] = True
        self.tick()
        self.assertEqual([r['revision'] for r in self.ledger.tasks.all()], revisions)
        self.assertEqual(len(self.inventory()['failed_unrecovered']), 2)

    def test_legacy_live_network_uses_original_transport_command(self):
        row = self.legacy('live-network', TRANSPORT_ERROR, deleted=False)
        self.tick()
        self.assertEqual(self.cma.admissions[0][:2],
                         (row['data']['agent_id'], row['data']['prompt']))
        self.assertIn('-transport-', self.cma.admissions[0][2])
        self.assertFalse(self.cma.creates)

    def test_deleted_agent_recovery_uses_new_lifecycle_keys_and_normal_admission(self):
        row = self.legacy('retired', RATE_ERROR, create_key='old-create-key',
                          create_body={'name': 'old'})
        self.tick()
        queued = self.ledger.tasks.get(row['id'])
        self.assertEqual(queued['state'], 'queued')
        self.assertNotIn('agent_id', queued['data'])
        self.assertEqual(queued['data']['execution_history'][0]['response_id'], row['data']['response_id'])
        self.cma.response = lambda _: {'status': 'in_progress'}
        self.now += 60
        self.tick()
        restarted = self.ledger.tasks.get(row['id'])
        self.assertEqual(restarted['state'], 'running')
        self.assertNotEqual(restarted['data']['create_key'], row['data']['create_key'])
        self.assertNotEqual(restarted['data']['command_key'], row['data']['command_key'])
        self.assertEqual(self.cma.admissions[0][1], control.task_prompt(restarted))
        self.assertEqual(restarted['request'], row['request'])
        self.cma.response = lambda _: {'status': 'failed', 'error': SIZE_ERROR}
        self.tick()
        self.assertEqual(self.cma.retire_calls[-1][1],
                         'outreach-retire-'+row['request_hash']+'-execution-rate-limit-1')

    def test_stopped_agent_recovery_restarts_with_new_agent(self):
        row = self.legacy('stopped', RATE_ERROR, deleted=False)
        row = self.ledger.tasks.update(row['id'], row['revision'], 'failed', {
            **row['data'], 'cleanup': {'agent_id': 'agt_stopped', 'stopped': True, 'deleted': False,
                                       'status': 'stopped'}})
        self.tick()
        queued = self.ledger.tasks.get(row['id'])
        self.assertEqual(queued['state'], 'queued')
        self.assertNotIn('agent_id', queued['data'])
        self.assertNotIn('cleanup', queued['data'])
        self.assertEqual(queued['data']['execution_restart'], 'rate-limit-1')
        self.assertFalse(self.cma.admissions)

    def test_scheduled_retry_on_stopped_agent_restarts_after_conflict(self):
        from urllib.error import HTTPError
        row = self.running('conflict', RATE_ERROR)
        retry = {'command_key': 'original-conflict-rate-limit-1', 'source_response_id': 'resp_conflict',
                 'error': RATE_ERROR, 'prompt': row['data']['prompt']}
        data = {**row['data'], 'rate_limit_retries': 1, 'rate_limit_commands': [retry],
                'cleanup': {'agent_id': 'agt_conflict', 'stopped': True, 'deleted': False, 'status': 'stopped'},
                'recovery': {'status': 'retry-scheduled', 'error_class': 'rate-limit', 'attempts': 1,
                             'next_at': self.now-1}}
        row = self.ledger.tasks.update(row['id'], row['revision'], 'admission-unknown', data)

        def conflict(agent_id, prompt, key):
            if agent_id == 'agt_conflict':
                raise HTTPError('fixture', 409, 'Conflict', {}, None)
        self.cma.before_admit = conflict
        self.tick()
        queued = self.ledger.tasks.get(row['id'])
        self.assertEqual(queued['state'], 'queued')
        self.assertEqual(queued['data']['execution_restart'], 'rate-limit-1')
        self.assertEqual(queued['data']['prompt'], row['data']['prompt'])
        self.assertEqual(queued['data']['execution_history'][-1]['agent_id'], 'agt_conflict')
        self.assertNotIn('rate_limit_commands', queued['data'])
        self.cma.response = lambda _: {'status': 'in_progress'}
        self.tick()
        restarted = self.ledger.tasks.get(row['id'])
        self.assertEqual(restarted['state'], 'running')
        self.assertNotEqual(restarted['data']['agent_id'], 'agt_conflict')

    def test_legacy_missing_error_reads_original_response_once(self):
        row = self.legacy('lookup', RATE_ERROR, cached=False)
        calls = []
        self.cma.response = lambda rid: calls.append(rid) or {'status': 'failed', 'error': RATE_ERROR}
        self.tick()
        self.tick()
        self.assertEqual(calls, [row['data']['response_id']])
        self.assertEqual(self.ledger.tasks.get(row['id'])['state'], 'queued')

    def test_legacy_lookup_failure_never_replays_or_cleans_up_original(self):
        row = self.legacy('lookup-wait', RATE_ERROR, deleted=False, cached=False)
        self.cma.response = lambda _: (_ for _ in ()).throw(TimeoutError('Unavailable'))
        self.tick()
        self.assertFalse(self.cma.admissions)
        self.assertFalse(self.cma.retired)
        failed = self.ledger.tasks.get(row['id'])
        self.assertEqual(failed['data']['recovery']['status'], 'lookup-pending')
        self.cma.response = lambda _: {'status': 'failed', 'error': RATE_ERROR}
        self.tick()
        self.assertEqual(self.ledger.tasks.get(row['id'])['data']['recovery']['status'], 'retry-scheduled')

    def test_existing_cap_is_not_reset_by_reclassification(self):
        row = self.legacy('exhausted', RATE_ERROR, rate_limit_retries=5)
        self.tick()
        self.assertEqual(self.ledger.tasks.get(row['id'])['data']['recovery']['status'], 'needs-operator')
        self.assertFalse(self.cma.creates)

    def test_missing_stored_prompt_requires_local_takeover(self):
        row = self.legacy('missing-prompt', RATE_ERROR)
        data = dict(row['data'])
        data.pop('prompt')
        self.ledger.tasks.update(row['id'], row['revision'], 'failed', data)
        self.tick()
        recovery = self.ledger.tasks.get(row['id'])['data']['recovery']
        self.assertEqual(recovery['status'], 'needs-operator')
        self.assertEqual(recovery['suggested_exit'], 'local-codex')
        self.assertFalse(self.cma.creates)

    def test_work_exposes_compact_failed_exit_even_before_migration(self):
        row = self.legacy('inventory', SIZE_ERROR)
        item = self.inventory()['failed_unrecovered'][0]
        self.assertEqual(item, {'task_id': row['id'], 'kind': row['kind'], 'owner': row['owner'],
                               'error_class': 'request-too-large', 'attempts': 0,
                               'suggested_exit': 'local-codex'})
        self.assertNotIn('prompt', json.dumps(item))

    def test_rate_retry_checkpoint_and_unknown_admission_reuse_exact_key(self):
        row = self.running('lost', RATE_ERROR)
        self.tick()
        self.now += 60
        calls = []
        admit = self.cma.admit

        def lose_receipt(agent, prompt, key):
            saved = control.Ledger(self.remote, self.folder/'checkpoint')
            try:
                command = saved.tasks.get(row['id'])['data']['rate_limit_commands'][-1]
                self.assertEqual((command['prompt'], command['command_key']), (prompt, key))
            finally:
                saved.tasks.close()
            calls.append((agent, prompt, key))
            admit(agent, prompt, key)
            raise TimeoutError('Synthetic lost receipt')

        (self.folder/'checkpoint').mkdir()
        self.cma.admit = lose_receipt
        self.tick()
        self.assertEqual(self.ledger.tasks.get(row['id'])['state'], 'admission-unknown')
        self.restore()
        self.cma.admit = admit
        self.cma.response = lambda _: {'status': 'in_progress'}
        self.tick()
        self.assertEqual(self.cma.admissions[-1], calls[0])
        self.assertEqual(len(self.cma.commands), 1)
        self.assertEqual(self.ledger.tasks.get(row['id'])['data']['recovery']['attempts'], 1)

    def test_completed_retry_leaves_unrecovered_inventory(self):
        row = self.running('success', RATE_ERROR)
        self.tick()
        result = {'schema_version': 1, 'task_id': row['id'], 'outcome': 'completed',
                  'report': 'Synthetic checked result', 'artifacts': [], 'packet': None}
        self.cma.response = lambda _: {'status': 'completed', 'output': [{'type': 'message',
            'content': [{'type': 'output_text', 'text': json.dumps(result)}]}]}
        self.now += 60
        self.tick()
        recovered = self.ledger.tasks.get(row['id'])
        self.assertEqual(recovered['state'], 'awaiting-review')
        self.assertEqual(recovered['data']['recovery']['status'], 'recovered')
        self.assertEqual(self.inventory()['failed_unrecovered'], [])

    def test_zero_rate_budget_immediately_names_operator_exit(self):
        self.cfg['recovery'] = {'rate_limit_max_attempts': 0}
        row = self.running('no-budget', RATE_ERROR)
        self.tick()
        failed = self.ledger.tasks.get(row['id'])
        self.assertEqual(failed['data']['recovery']['status'], 'needs-operator')
        self.assertEqual(failed['data']['recovery']['attempts'], 0)
        self.assertFalse(self.cma.admissions)

    def test_saved_correction_prompt_is_retained_for_rate_retry(self):
        row = self.running('correction', RATE_ERROR, repairs=[{
            'response_id': 'resp_correction', 'command_key': 'repair-key', 'prompt': 'Exact correction'}])
        self.tick()
        self.cma.response = lambda _: {'status': 'in_progress'}
        self.now += 60
        self.tick()
        self.assertEqual(self.cma.admissions[0][:2], (row['data']['agent_id'], 'Exact correction'))

    def test_deleted_correction_agent_requires_local_takeover(self):
        row = self.legacy('lost-correction', RATE_ERROR, repairs=[{
            'response_id': 'resp_lost-correction', 'command_key': 'repair-key', 'prompt': 'Exact correction'}])
        self.tick()
        recovery = self.ledger.tasks.get(row['id'])['data']['recovery']
        self.assertEqual(recovery['status'], 'needs-operator')
        self.assertEqual(recovery['suggested_exit'], 'local-codex')
        self.assertIn('correction context', recovery['reason'])
        self.assertFalse(self.cma.creates)

    def test_legacy_http_observation_reconciles_original_running_response(self):
        row = self.legacy('observation', None, deleted=False, cached=False)
        row = self.ledger.tasks.update(row['id'], row['revision'], 'failed',
                                      {**row['data'], 'reason': 'HTTPError: HTTP Error 503: Service Unavailable'})
        self.cma.response = lambda _: {'status': 'in_progress'}
        self.tick()
        self.assertEqual(self.ledger.tasks.get(row['id'])['state'], 'running')
        self.assertFalse(self.cma.admissions)
        self.assertFalse(self.cma.retired)

    def test_parked_unknown_transient_failure_is_reclassified_once(self):
        row = self.legacy('parked', INPUT_NOT_WRITTEN, deleted=False)
        data = {**row['data'], 'recovery': {'status': 'needs-operator', 'error_class': 'unknown',
                                            'attempts': 0, 'suggested_exit': 'rewrite-task'}}
        row = self.ledger.tasks.update(row['id'], row['revision'], 'failed', data)
        self.tick()
        retried = self.ledger.tasks.get(row['id'])
        self.assertIn(retried['state'], ACTIVE)
        self.assertEqual(retried['data']['recovery']['error_class'], 'rate-limit')
        self.assertEqual(retried['data']['recovery']['status'], 'retry-scheduled')
        other = self.legacy('unrelated', {'code': 'permission_denied', 'message': 'no'}, deleted=False)
        other = self.ledger.tasks.update(other['id'], other['revision'], 'failed',
                                         {**other['data'], 'recovery': data['recovery']})
        self.tick()
        self.assertEqual(self.ledger.tasks.get(other['id'])['state'], 'failed')

    def test_classifier_keeps_billing_and_unrelated_permissions_separate(self):
        for error, expected in [
            (RATE_ERROR, 'rate-limit'), (SIZE_ERROR, 'request-too-large'),
            (TRANSPORT_ERROR, 'network'),
            ({'code': 'agent_turn_failed', 'message': 'stream disconnected before completion: '
              'Transport error: network error: error decoding response body'}, 'network'),
            ({'code': 'agent_turn_failed', 'message': '402 Payment Required: insufficient_credits'}, 'credit'),
            ({'code': 'permission_denied', 'message': RATE_ERROR['message']}, 'unknown'),
            (INPUT_NOT_WRITTEN, 'rate-limit'), (BILLING_BUSY, 'rate-limit'),
        ]:
            with self.subTest(error=error):
                self.assertEqual(control.execution_failure_class(error), expected)


if __name__ == '__main__':
    unittest.main()
