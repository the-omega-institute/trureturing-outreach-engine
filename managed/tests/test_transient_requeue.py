"""Offline regression tests for HTTP observations and signed operator recovery."""
import copy
from contextlib import redirect_stdout
import hashlib
import hmac
import io
import json
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError, URLError

from fixture_config import use_cli_config
from managed import control
from managed.automation import work
from managed.cma import CMARequestError
from managed.queue import canonical, digest
from test_sandbox_pool import PoolFixture


SECRET = 'offline-recovery-key'
HTTP_503 = 'HTTPError: HTTP Error 503: Service Unavailable'


class TransientRequeueTests(PoolFixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.cfg.pop('sandbox_pool')

    def running(self, name='http', **extra):
        row = self.queue(name)
        return self.ledger.tasks.update(name, row['revision'], 'running', {
            'agent_id': 'agt_'+name, 'response_id': 'resp_'+name,
            'command_key': 'original-'+name, 'prompt': 'Exact original prompt', **extra})

    def parked(self, name='legacy', **extra):
        row = self.running(name)
        return self.ledger.tasks.update(name, row['revision'], 'failed', {
            **row['data'], 'reason': HTTP_503,
            'recovery': {'error_class': 'unknown', 'status': 'needs-operator',
                         'attempts': 0, 'suggested_exit': 'rewrite-task'}, **extra})

    def inventory(self):
        with patch('managed.daily_local.status', return_value={}):
            return work(self.ledger.tasks, control.ROOT)['failed_unrecovered']

    def test_response_503_backs_off_survives_restore_and_only_rereads_original(self):
        row = self.running()
        self.cma.response = Mock(side_effect=HTTPError('fixture', 503, 'Unavailable', {}, None))
        self.tick()
        waiting = self.ledger.tasks.get(row['id'])
        self.assertEqual(waiting['state'], 'running')
        self.assertEqual(waiting['data']['rate_limit_retries'], 1)
        self.assertEqual(waiting['data']['recovery']['next_at'], self.now+60)
        self.assertEqual(waiting['data']['recovery']['scope'], 'observation')
        self.assertEqual(waiting['data']['last_error'], HTTP_503.replace('Service Unavailable', 'Unavailable'))
        self.restore()
        self.now += 59
        self.tick()
        self.assertEqual(self.cma.response.call_count, 1)
        self.now += 1
        self.cma.response.return_value = {'status': 'in_progress'}
        self.cma.response.side_effect = None
        self.tick()
        self.cma.response.assert_called_with(row['data']['response_id'])
        recovered = self.ledger.tasks.get(row['id'])
        self.assertEqual(recovered['data']['recovery']['status'], 'recovered')
        self.assertEqual(recovered['request'], row['request'])
        self.assertEqual(recovered['data']['command_key'], row['data']['command_key'])
        self.assertFalse(self.cma.admissions or self.cma.creates or self.cma.retire_calls)

    def test_http_exhaustion_preserves_uncertain_agent_and_does_not_reset_budget(self):
        self.cfg['recovery'] = {'rate_limit_max_attempts': 1}
        row = self.running()
        self.cma.response = Mock(side_effect=CMARequestError(503, 'Unavailable'))
        self.tick()
        self.now += 60
        self.tick()
        failed = self.ledger.tasks.get(row['id'])
        self.assertEqual(failed['state'], 'failed')
        self.assertEqual(failed['data']['recovery']['status'], 'needs-operator')
        self.assertEqual(failed['data']['recovery']['attempts'], 1)
        self.assertEqual(failed['data']['response_id'], row['data']['response_id'])
        self.restore()
        self.now += 3600
        self.tick()
        self.assertEqual(self.cma.response.call_count, 2)
        self.assertFalse(self.cma.admissions or self.cma.creates or self.cma.retire_calls)

    def test_transient_statuses_and_network_exceptions_use_existing_budgets(self):
        for index, error in enumerate([
            *[HTTPError('fixture', code, 'Transient', {}, None) for code in sorted(control.TRANSIENT_HTTP)],
            URLError('connection lost'), TimeoutError('timed out'), ConnectionError('reset')]):
            with self.subTest(error=error):
                row = self.running('case-'+str(index))
                self.assertTrue(control.recover_transport_exception(self.ledger, row, error, self.cfg))
                row = self.ledger.tasks.get(row['id'])
                self.assertEqual(row['data']['recovery']['next_at'], self.now+60)
                network = not isinstance(error, HTTPError)
                counter = 'transport_retries' if network else 'rate_limit_retries'
                self.assertEqual(row['data'][counter], 1)
                if network:
                    self.assertTrue(control.recover_transport_exception(self.ledger, row, error, self.cfg))
                    row = self.ledger.tasks.get(row['id'])
                    self.assertEqual(row['data']['recovery']['next_at'], self.now+120)
                    control.recover_transport_exception(self.ledger, row, error, self.cfg)
                    self.assertEqual(self.ledger.tasks.get(row['id'])['state'], 'failed')

    def test_uncertain_create_reuses_original_key_after_backoff(self):
        row = self.queue('create')
        create = self.cma.create
        calls = []
        def lose_once(body, key):
            calls.append((copy.deepcopy(body), key))
            receipt = create(body, key)
            if len(calls) == 1:
                raise HTTPError('fixture', 503, 'Lost create receipt', {}, None)
            return receipt
        self.cma.create = lose_once
        self.tick()
        waiting = self.ledger.tasks.get(row['id'])
        self.assertEqual(waiting['state'], 'provision-unknown')
        self.restore()
        self.tick()
        self.assertEqual(len(calls), 1)
        self.now += 60
        self.tick()
        self.assertEqual(calls[0], calls[1])
        self.assertEqual(len(self.cma.creates), 1)
        self.assertEqual(self.ledger.tasks.get(row['id'])['state'], 'running')

    def test_uncertain_admission_reuses_original_key_after_backoff(self):
        row = self.running()
        self.ledger.tasks.update(row['id'], row['revision'], 'admitting', row['data'])
        admit = self.cma.admit
        calls = []
        def lose_once(agent, prompt, key):
            calls.append((agent, prompt, key))
            receipt = admit(agent, prompt, key)
            if len(calls) == 1:
                raise HTTPError('fixture', 408, 'Lost admission receipt', {}, None)
            return receipt
        self.cma.admit = lose_once
        self.tick()
        self.assertEqual(self.ledger.tasks.get(row['id'])['state'], 'admission-unknown')
        self.restore()
        self.now += 60
        self.tick()
        self.assertEqual(calls, [calls[0], calls[0]])
        self.assertEqual(len(self.cma.commands), 1)
        self.assertFalse(self.cma.creates or self.cma.retire_calls)

    def test_pause_and_disabled_kind_defer_observation_retry(self):
        row = self.running()
        self.cma.response = Mock(side_effect=HTTPError('fixture', 503, 'Unavailable', {}, None))
        self.tick()
        self.now += 60
        self.cfg['paused'] = True
        self.tick()
        self.cfg['paused'] = False
        self.cfg['kinds']['research'] = False
        self.tick()
        self.assertEqual(self.cma.response.call_count, 1)
        self.assertEqual(self.ledger.tasks.get(row['id'])['data']['recovery']['attempts'], 1)
        self.assertFalse(self.cma.admissions or self.cma.retire_calls)

    def test_nontransient_http_and_other_errors_keep_existing_behavior(self):
        row = self.running()
        for error in (HTTPError('fixture', 403, 'Forbidden', {}, None), ValueError('invalid result')):
            with self.subTest(error=error):
                self.cma.response = Mock(side_effect=error)
                self.tick()
                current = self.ledger.tasks.get(row['id'])
                self.assertEqual(current['state'], 'running')
                self.assertNotIn('recovery', current['data'])
                self.assertIn(type(error).__name__, current['data']['last_error'])
        self.assertEqual(control.execution_failure_class({'code': 'agent_turn_failed',
                         'message': 'HTTP Error 403: Forbidden'}), 'unknown')

    def test_http_text_classifies_legacy_inventory_and_reconciles_running_response(self):
        row = self.parked()
        self.assertEqual(self.inventory()[0]['error_class'], 'rate-limit')
        self.assertEqual(self.inventory()[0]['suggested_exit'], 'retry-later')
        self.cma.response = Mock(return_value={'status': 'in_progress'})
        self.tick()
        recovered = self.ledger.tasks.get(row['id'])
        self.assertEqual(recovered['state'], 'running')
        self.assertEqual(recovered['data']['http_observation_failure'], HTTP_503)
        self.assertFalse(self.cma.admissions or self.cma.creates or self.cma.retire_calls)

    def test_legacy_last_error_and_confirmed_terminal_http_use_existing_retry(self):
        for code in (408, 429, 500, 502, 503, 504):
            with self.subTest(code=code):
                error = {'code': 'agent_turn_failed', 'message': 'HTTP Error '+str(code)+': Unavailable'}
                row = self.parked('legacy-'+str(code), reason='Task step failed',
                                  last_error='HTTPError: '+error['message'])
                self.cma.response = Mock(return_value={'status': 'failed', 'error': error})
                control.recover_execution_failures(self.ledger, self.cma, self.cfg)
                waiting = self.ledger.tasks.get(row['id'])
                self.assertEqual(waiting['state'], 'admitting')
                self.assertEqual(waiting['data']['recovery']['next_at'], self.now+60)
                self.assertEqual(waiting['data']['recovery']['attempts'], 1)
                self.assertTrue(waiting['data']['rate_limit_commands'][-1]['command_key'].endswith('-rate-limit-1'))
        self.assertFalse(self.cma.admissions or self.cma.creates)

    def test_failed_lookup_503_is_bounded_and_does_not_retire_original(self):
        self.cfg['recovery'] = {'rate_limit_max_attempts': 1}
        row = self.parked()
        self.cma.response = Mock(side_effect=HTTPError('fixture', 503, 'Unavailable', {}, None))
        self.tick()
        self.assertEqual(self.ledger.tasks.get(row['id'])['state'], 'running')
        self.now += 60
        self.tick()
        self.assertEqual(self.ledger.tasks.get(row['id'])['state'], 'failed')
        self.tick()
        self.assertEqual(self.cma.response.call_count, 2)
        self.assertFalse(self.cma.retire_calls or self.cma.admissions)

    def test_legacy_transient_with_exhausted_budget_is_not_reset(self):
        row = self.parked(rate_limit_retries=5)
        self.cma.response = Mock(return_value={'status': 'failed', 'error': {
            'code': 'agent_turn_failed', 'message': HTTP_503}})
        self.tick()
        failed = self.ledger.tasks.get(row['id'])
        self.assertEqual(failed['state'], 'failed')
        self.assertEqual(failed['data']['recovery']['attempts'], 5)
        self.assertEqual(failed['data']['recovery']['status'], 'needs-operator')
        self.assertFalse(self.cma.admissions)

    def test_legacy_http_lookup_billing_failure_returns_to_existing_credit_gate(self):
        row = self.parked()
        self.cfg['paused'] = True
        self.cma.response = Mock(return_value={'status': 'failed', 'error': {
            'code': 'agent_turn_failed', 'message': '402 Payment Required: insufficient_credits'}})
        self.tick()
        self.tick()
        waiting = self.ledger.tasks.get(row['id'])
        self.assertEqual(waiting['state'], 'credit-wait')
        self.assertEqual(waiting['data']['credit_retries'], 1)
        self.assertFalse(self.cma.admissions or self.cma.creates or self.cma.retire_calls)

    def test_legacy_uncertain_admission_waits_then_reuses_original_key(self):
        row = self.parked()
        data = {k: v for k, v in row['data'].items() if k != 'response_id'}
        row = self.ledger.tasks.update(row['id'], row['revision'], 'failed', data)
        self.tick()
        self.assertEqual(self.ledger.tasks.get(row['id'])['state'], 'admission-unknown')
        self.assertFalse(self.cma.admissions)
        self.now += 60
        self.tick()
        self.assertEqual(self.cma.admissions, [(data['agent_id'], data['prompt'], data['command_key'])])
        self.assertFalse(self.cma.creates)

    def test_external_effect_evidence_and_missing_handles_never_auto_restart(self):
        for key in ('result', 'draft_intent', 'draft_receipt', 'sent_receipt', 'mathesis_delivery'):
            row = self.parked(key, **{key: {'phase': 'uncertain'}})
            self.cma.response = Mock(side_effect=AssertionError('Do not replay delivery work'))
            control.recover_execution_failures(self.ledger, self.cma, self.cfg)
            self.assertEqual(self.ledger.tasks.get(row['id']), row)
            self.cma.response.assert_not_called()
        row = self.queue('no-handles')
        self.ledger.tasks.update(row['id'], row['revision'], 'failed', {'reason': HTTP_503})
        control.recover_execution_failures(self.ledger, self.cma, self.cfg)
        data = self.ledger.tasks.get(row['id'])['data']
        self.assertEqual(data['recovery']['suggested_exit'], 'local-codex')
        self.assertFalse(self.cma.admissions or self.cma.creates)


class OperatorRequeueTests(PoolFixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.cfg.pop('sandbox_pool')
        self.remote.ensure = lambda: None
        row = self.queue('guard-failed')
        self.failed = self.ledger.tasks.update(row['id'], row['revision'], 'failed', {
            'agent_id': 'agt_old', 'response_id': 'resp_old',
            'create_key': 'old-create', 'command_key': 'old-command', 'prompt': 'Original',
            'reason': 'CMA runtime-guard retry limit reached; operator recovery required',
            'runtime_guard': {'controls': [{'kind': 'cancel'}]},
            'recovery': {'status': 'needs-operator', 'error_class': 'runtime-guard', 'attempts': 0},
            'cleanup': {'deleted': True}})

    def envelope(self, **changes):
        value = {'schema_version': 1, 'task_id': self.failed['id'],
                 'request_hash': self.failed['request_hash'], 'failed_revision': self.failed['revision'],
                 'reason': 'Reconciled the cancelled execution; restart the task.', **changes}
        value['signature'] = hmac.new(SECRET.encode(), canonical(value).encode(), hashlib.sha256).hexdigest()
        return value

    def stage(self, value):
        return control.stage(value, 'failed-requeue', self.remote, self.folder)

    def test_cli_stages_signed_immutable_request_without_mutating_snapshot(self):
        self.ledger.save(force=True)
        pointer = copy.deepcopy(self.remote.body)
        with patch.object(control, 'Releases', return_value=self.remote), \
             patch.object(control, 'review_key', return_value=SECRET), redirect_stdout(io.StringIO()) as output:
            control.main(['requeue-failed', '--task', self.failed['id'], '--reason', 'Runtime guard reconciled.'])
        receipt = json.loads(output.getvalue())
        raw = self.remote.assets[receipt['asset']]
        self.assertEqual(hashlib.sha256(raw).hexdigest(), receipt['sha256'])
        value = json.loads(raw)
        unsigned = {k: v for k, v in value.items() if k != 'signature'}
        self.assertEqual(value['signature'], hmac.new(SECRET.encode(), canonical(unsigned).encode(), hashlib.sha256).hexdigest())
        self.assertEqual(value['failed_revision'], self.failed['revision'])
        self.assertEqual(self.remote.body, pointer)
        self.assertEqual(self.ledger.tasks.get(self.failed['id']), self.failed)
        self.assertFalse(self.cma.creates or self.cma.admissions)
        self.assertEqual(self.ledger.ingest(SECRET), [])
        queued = self.ledger.tasks.get(self.failed['id'])
        self.assertEqual(queued['state'], 'queued')
        self.assertEqual(queued['request'], self.failed['request'])
        self.assertEqual(queued['data']['execution_history'][-1], self.failed['data'])
        event = self.ledger.tasks.db.execute('SELECT payload FROM control_events WHERE id=?', (digest(value),)).fetchone()
        self.assertEqual(json.loads(event[0]), value)
        self.restore()
        revision = self.ledger.tasks.get(self.failed['id'])['revision']
        self.assertEqual(self.ledger.ingest(SECRET), [])
        control.requeue_failed_task(self.ledger, value, SECRET)
        self.assertEqual(self.ledger.tasks.get(self.failed['id'])['revision'], revision)

    def test_coordinator_restarts_any_failed_class_with_new_execution_keys(self):
        for error_class in ('runtime-guard', 'unknown', 'request-too-large', 'admission-rejected', 'network'):
            with self.subTest(error_class=error_class):
                row = self.ledger.tasks.get(self.failed['id'])
                row = self.ledger.tasks.update(row['id'], row['revision'], 'failed', {
                    **self.failed['data'], 'recovery': {'error_class': error_class, 'status': 'needs-operator'}})
                self.failed = row
                value = self.envelope()
                self.stage(value)
                self.assertEqual(self.ledger.ingest(SECRET), [])
                self.tick()
                restarted = self.ledger.tasks.get(row['id'])
                self.assertEqual(restarted['state'], 'running')
                self.assertNotEqual(restarted['data']['create_key'], row['data']['create_key'])
                self.assertNotEqual(restarted['data']['command_key'], row['data']['command_key'])
                self.assertEqual(restarted['data']['execution_history'][-1], row['data'])
                self.assertEqual(restarted['request_hash'], row['request_hash'])

    def test_ingest_rejects_bad_or_missing_signatures_and_preserves_failure(self):
        for value, secret in [(self.envelope(), None), (self.envelope(), 'wrong-key'),
                              ({**self.envelope(), 'signature': '0'*64}, SECRET),
                              ({k: v for k, v in self.envelope().items() if k != 'signature'}, SECRET),
                              ({**self.envelope(), 'reason': 'Tampered after signing'}, SECRET)]:
            with self.subTest(secret=secret, fields=set(value)):
                self.remote.assets = {name: raw for name, raw in self.remote.assets.items()
                                      if not name.startswith('failed-requeue-')}
                self.stage(value)
                errors = self.ledger.ingest(secret)
                self.assertEqual(len(errors), 1)
                self.assertEqual(self.ledger.tasks.get(self.failed['id']), self.failed)
        self.assertEqual(self.ledger.tasks.db.execute('SELECT COUNT(*) FROM control_events').fetchone()[0], 0)

    def test_coordinator_rejects_stale_binding_nonfailed_state_and_empty_reason(self):
        for changes, expected in [({'request_hash': 'f'*64}, 'hash changed'),
                                  ({'failed_revision': self.failed['revision']-1}, 'revision changed'),
                                  ({'reason': '  '}, 'needs a reason')]:
            with self.subTest(changes=changes), self.assertRaisesRegex(ValueError, expected):
                control.requeue_failed_task(self.ledger, self.envelope(**changes), SECRET)
        value = self.envelope()
        self.ledger.tasks.update(self.failed['id'], self.failed['revision'], 'running', self.failed['data'])
        with self.assertRaisesRegex(ValueError, 'Only failed'):
            control.requeue_failed_task(self.ledger, value, SECRET)

    def test_later_failure_needs_fresh_request_and_preserves_both_execution_keys(self):
        first = self.envelope()
        control.requeue_failed_task(self.ledger, first, SECRET)
        queued = self.ledger.tasks.get(self.failed['id'])
        self.failed = self.ledger.tasks.update(queued['id'], queued['revision'], 'failed', {
            **queued['data'], 'reason': 'Later execution failed'})
        control.requeue_failed_task(self.ledger, first, SECRET)
        self.assertEqual(self.ledger.tasks.get(self.failed['id']), self.failed)
        second = self.envelope()
        control.requeue_failed_task(self.ledger, second, SECRET)
        restarted = self.ledger.tasks.get(self.failed['id'])
        self.assertNotEqual(restarted['data']['execution_restart'], queued['data']['execution_restart'])
        self.assertEqual(restarted['data']['execution_history'][-1], self.failed['data'])
        self.assertEqual(self.ledger.tasks.db.execute('SELECT COUNT(*) FROM control_events').fetchone()[0], 2)

    def test_cli_rejects_nonfailed_or_blank_reason_before_upload(self):
        self.ledger.save(force=True)
        with patch.object(control, 'Releases', return_value=self.remote), \
             patch.object(control, 'review_key', return_value=SECRET):
            with self.assertRaisesRegex(ValueError, 'needs a reason'):
                control.main(['requeue-failed', '--task', self.failed['id'], '--reason', ' '])
            self.ledger.tasks.update(self.failed['id'], self.failed['revision'], 'running', self.failed['data'])
            self.ledger.save(force=True)
            with self.assertRaisesRegex(ValueError, 'Only failed'):
                control.main(['requeue-failed', '--task', self.failed['id'], '--reason', 'Restart'])
        self.assertFalse(any(name.startswith('failed-requeue-') for name in self.remote.assets))

    def test_refuses_task_with_external_effects(self):
        for key in ('draft_intent', 'draft_receipt', 'sent_receipt', 'mathesis_delivery'):
            with self.subTest(key=key):
                row = self.ledger.tasks.get(self.failed['id'])
                self.failed = self.ledger.tasks.update(row['id'], row['revision'], 'failed',
                                                       {**row['data'], key: {'id': 'x'}})
                with self.assertRaisesRegex(ValueError, 'external effects'):
                    control.requeue_failed_task(self.ledger, self.envelope(), SECRET)
                self.assertEqual(self.ledger.tasks.get(self.failed['id'])['state'], 'failed')
                row = self.ledger.tasks.get(self.failed['id'])
                self.failed = self.ledger.tasks.update(row['id'], row['revision'], 'failed',
                                                       {k: v for k, v in row['data'].items() if k != key})


if __name__ == '__main__':
    unittest.main()


def setUpModule():
    use_cli_config()
