"""Retire replaced attempts without touching current or active execution."""
import unittest

from managed import control
from managed.tasks import ACTIVE
from managed.tick import Tick
from test_sandbox_pool import PoolFixture
from test_cma_agent_cleanup import LifecycleCMA


class HistoryRetirementTests(PoolFixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.cfg['paused'] = True

    def history_row(self, name, entries, state='superseded', **data):
        row = self.queue(name)
        return self.ledger.tasks.update(name, row['revision'], state,
                                        {**data, 'execution_history': entries})

    def test_incomplete_cleanup_is_retired_and_persisted(self):
        row = self.history_row('old', [{'agent_id': 'agt_old',
            'owned_sandbox_id': 'sbx_old', 'cleanup': {'deleted': False}}])
        self.tick()
        self.assertEqual(self.cma.retire_calls, [('agt_old',
            'outreach-retire-history-'+row['request_hash']+'-0', None)])
        self.restore()
        entry = self.ledger.tasks.get(row['id'])['data']['execution_history'][0]
        self.assertTrue(entry['cleanup']['deleted'])
        self.assertEqual(entry['owned_sandbox_id'], 'sbx_old')

    def test_accepted_historical_delete_is_only_observed_on_later_ticks(self):
        self.cma=LifecycleCMA({'agt_old':'ready'},accepted=True)
        row=self.history_row('old', [{'agent_id':'agt_old'}])
        self.tick();self.restore()
        for _ in range(4):self.now+=60;self.tick()
        self.assertEqual(sum(method=='DELETE' for _,method,_ in self.cma.calls),1)
        self.cma.agents.clear();self.tick()
        entry=self.ledger.tasks.get(row['id'])['data']['execution_history'][0]
        self.assertTrue(entry['cleanup']['deleted'])

    def test_current_agent_and_all_active_states_are_skipped(self):
        self.history_row('current', [{'agent_id': 'agt_current'}], agent_id='agt_current')
        for state in ACTIVE:
            self.history_row('active-'+state, [{'agent_id': 'agt_'+state}], state=state)
        self.tick()
        self.assertEqual(self.cma.retire_calls, [])

    def test_deleted_and_missing_agent_entries_are_skipped(self):
        self.history_row('deleted', [{'agent_id': 'agt_deleted', 'cleanup': {'deleted': True}}, {}])
        self.tick()
        self.assertEqual(self.cma.retire_calls, [])

    def test_exception_is_bounded_and_does_not_stop_later_entries_or_rows(self):
        row = self.history_row('failure', [{'agent_id': 'agt_bad'}, {'agent_id': 'agt_good'}])
        other = self.history_row('other', [{'agent_id': 'agt_other'}])
        retire = self.cma.retire
        def fail(agent, key, sandbox, **kwargs):
            if agent == 'agt_bad':
                self.cma.retire_calls.append((agent, key, sandbox))
                raise RuntimeError('x'*400)
            return retire(agent, key, sandbox, **kwargs)
        self.cma.retire = fail
        self.tick()
        history = self.ledger.tasks.get(row['id'])['data']['execution_history']
        self.assertEqual(history[0]['cleanup_error'], 'RuntimeError: '+'x'*300)
        self.assertTrue(history[1]['cleanup']['deleted'])
        self.assertTrue(self.ledger.tasks.get(other['id'])['data']['execution_history'][0]['cleanup']['deleted'])
        self.cma.retire = retire
        self.tick()
        self.assertNotIn('cleanup_error', self.ledger.tasks.get(row['id'])['data']['execution_history'][0])

    def test_persistent_retire_error_stops_after_three_attempts(self):
        row = self.history_row('stuck', [{'agent_id': 'agt_stuck'}])
        def fail(agent, key, sandbox, **kwargs):
            self.cma.retire_calls.append((agent, key, sandbox))
            raise RuntimeError('unavailable')
        self.cma.retire = fail
        for _ in range(5):
            self.tick()
        self.assertEqual(len(self.cma.retire_calls), 3)
        entry = self.ledger.tasks.get(row['id'])['data']['execution_history'][0]
        self.assertEqual(entry['cleanup_errors'], 3)

    def test_limit_is_shared_across_rows_and_remaining_entries_run_next_tick(self):
        for name in ('one', 'two'):
            self.history_row(name, [{'agent_id': 'agt_'+name+str(i)} for i in range(4)])
        self.tick()
        self.assertEqual(len(self.cma.retire_calls), control.RETIRE_HISTORY_PER_TICK)
        self.tick()
        self.assertEqual(len(self.cma.retire_calls), 8)

    def test_tick_budget_defers_history_retirement(self):
        row = self.history_row('budget', [{'agent_id': 'agt_old'}])
        clock = [0]
        tick = Tick(1, lambda: clock[0])
        tick.bind(self.ledger)
        clock[0] = 1
        self.tick()
        self.assertEqual(self.cma.retire_calls, [])
        self.assertIn(row['id']+'-history-0', tick.pending['advance'])
