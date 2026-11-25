"""Strict Sandbox reservations through admission, restart and recovery."""
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from fixture_config import test_config
from managed.automation import work
from managed.lanes import lane, summary as lane_summary
from managed.queue import canonical
from managed.sandbox_pool import CAPACITY_BLOCK_SECONDS, POOL_WAIT_SECONDS, SandboxPool, group_config
from test_sandbox_pool import PoolFixture
from test_tasks import request


class GroupFixture(PoolFixture):
    def setUp(self):
        super().setUp()
        self.cfg['kinds']['shortnote'] = True
        self.cfg['sandbox_pool']['groups'] = [
            {'name': 'notes', 'kinds': ['shortnote', 'mathesis'], 'max_sandboxes': 1},
            {'name': 'mail', 'kinds': ['research', 'reply'], 'max_sandboxes': 1},
        ]

    def queue(self, name, kind='research', **inputs):
        return self.ledger.tasks.add({**request(name, 'owner-'+name, kind), 'inputs': inputs})

    def tagged_entries(self, placements=(1, 1)):
        entries = self.seed_pool(2)
        for entry, group, count in zip(entries, ('notes', 'mail'), placements):
            entry.update(group=group, agents_placed=count)
        self.ledger.tasks.db.execute('UPDATE pipeline_state SET payload=? WHERE key=?',
                                     (canonical(entries), 'sandbox_pool'))
        return entries


class SandboxGroupTests(GroupFixture, unittest.TestCase):
    def test_validation_rejects_duplicate_missing_and_unknown_kinds_and_excess_limits(self):
        config = self.cfg['sandbox_pool']
        groups = config['groups']
        for bad_groups in [
            [groups[0], {**groups[1], 'kinds': ['research', 'shortnote']}],
            [groups[0], {**groups[1], 'kinds': ['reply']}],
            [groups[0], {**groups[1], 'kinds': ['research', 'unknown']}],
            [groups[0], {**groups[1], 'max_sandboxes': 4}],
            [groups[0], {**groups[1], 'max_sandboxes': True}],
            [groups[0], {**groups[1], 'name': 'notes'}],
            [],
        ]:
            with self.subTest(groups=bad_groups), self.assertRaises(ValueError):
                group_config({**config, 'groups': bad_groups}, self.cfg['kinds'])
        self.assertEqual(group_config(config, self.cfg['kinds'])[0], {'notes': 1, 'mail': 1})

    def test_migration_persists_least_used_assignment_and_keeps_existing_agents(self):
        self.cfg['sandbox_pool']['quota_blocked_max_agents_placed'] = 192
        older, newer = self.seed_pool(2)
        older['agents_placed'] = 100
        newer['agents_placed'] = 7
        self.ledger.tasks.db.execute('UPDATE pipeline_state SET payload=? WHERE key=?',
                                     (canonical([older, newer]), 'sandbox_pool'))
        legacy_mail = self.occupy(newer, 0, state='running')
        self.ledger.tasks.db.execute('INSERT INTO pipeline_state VALUES(?,?)',
            ('sandbox_pool_capacity', canonical({'capacity_blocked_at': self.now - 10})))
        self.tick()
        self.restore()
        self.tick()
        self.assertEqual(self.ledger.tasks.get(legacy_mail['id']), legacy_mail)
        self.assertEqual(self.cma.retire_calls, [])
        with patch('managed.daily_local.status', return_value={}):
            inventory = work(self.ledger.tasks, self.folder)
        self.assertEqual([(e['sandbox_id'], e['group'], e['live_agents']) for e in inventory['sandbox_pool']],
                         [(older['sandbox_id'], 'mail', 0), (newer['sandbox_id'], 'notes', 1)])
        self.assertEqual(self.state('sandbox_pool_capacity'), {'groups': {'mail': 990, 'notes': 990}})

    def test_first_contact_shares_notes_and_previous_sandbox_drains(self):
        production = test_config()
        self.cfg.update(kinds=production['kinds'], sandbox_pool=production['sandbox_pool'])
        notes, mail, previous = self.seed_pool(3)
        for entry, group in zip((notes, mail, previous), ('notes', 'mail', 'firstcontact')):
            entry.update(group=group, agents_placed=1)
        self.ledger.tasks.db.execute('UPDATE pipeline_state SET payload=? WHERE key=?',
                                     (canonical([notes, mail, previous]), 'sandbox_pool'))
        live = self.queue('existing-first-contact', 'firstcontact')
        live = self.ledger.tasks.update(live['id'], live['revision'], 'running', {
            'agent_id': 'agt_existing_firstcontact', 'pool_group': 'firstcontact',
            'workspace_id': previous['workspace_id'], 'pool_sandbox_id': previous['sandbox_id'],
        })
        subjects = [self.queue(kind, kind) for kind in ('shortnote', 'mathesis', 'firstcontact', 'reply')]

        pool = SandboxPool(self.ledger, self.cfg['sandbox_pool'], self.cfg['kinds'])
        pool.health(self.cma)
        for subject, entry in zip(subjects, (notes, notes, notes, mail)):
            self.assertEqual(pool.slot(subject), (True, entry))
        self.assertEqual(self.ledger.tasks.get(live['id']), live)
        self.assertFalse(any(method == 'DELETE' for _, method, _ in self.cma.calls))

        self.ledger.tasks.update(live['id'], live['revision'], 'held',
                                 {**live['data'], 'cleanup': {'deleted': True}})
        pool.health(self.cma)
        self.assertEqual([entry['sandbox_id'] for entry in pool.entries],
                         [notes['sandbox_id'], mail['sandbox_id']])
        self.assertIn(('/api/v1/sandboxes/' + previous['sandbox_id'], 'DELETE',
                       'outreach-pool-retire-' + previous['sandbox_id'] + '-sandbox-delete'), self.cma.calls)

    def test_full_group_cannot_borrow_idle_slots_in_either_direction(self):
        self.cfg['kinds']['mathesis'] = True
        self.cfg['followup_lane'] = True
        self.cfg['sandbox_pool']['agents_per_sandbox'] = 1
        notes, mail = self.tagged_entries()
        legacy_note = self.occupy(notes, 0, state='running')
        self.queue('note', 'shortnote')
        followup = self.queue('handoff', 'mathesis', owner_ids=['correspondent'])
        self.queue('correspondence')
        self.assertEqual(lane(followup, self.cfg), 'followup')
        self.tick()
        self.assertEqual([b['workspace_id'] for b in self.cma.creates.values()], [mail['workspace_id']])
        for name in ('note', 'handoff'):
            self.assertEqual(self.ledger.tasks.get(name)['state'], 'queued')

        self.ledger.tasks.update(legacy_note['id'], legacy_note['revision'], 'held',
                                 {**legacy_note['data'], 'cleanup': {'deleted': True}})
        more_mail = self.queue('more-mail')
        pool = SandboxPool(self.ledger, self.cfg['sandbox_pool'], self.cfg['kinds'])
        pool.health(self.cma)
        self.assertEqual(pool.slot(more_mail), (False, None))
        self.assertEqual(pool.slot(followup)[1]['sandbox_id'], notes['sandbox_id'])
        self.tick()
        self.assertEqual([b['workspace_id'] for b in self.cma.creates.values()],
                         [mail['workspace_id'], notes['workspace_id']])
        self.assertEqual(self.ledger.tasks.get('handoff')['state'], 'running')
        self.assertEqual(self.ledger.tasks.get('more-mail')['state'], 'queued')

    def test_mail_can_borrow_within_its_group_while_note_lane_waits(self):
        self.cfg.update(max_active=3, kind_quota='equal')
        self.cfg['kinds']['reply'] = True  # equal shares of one slot each
        self.cfg['sandbox_pool']['agents_per_sandbox'] = 2
        notes, mail = self.tagged_entries()
        for index in range(2):
            self.occupy(notes, index)  # The retained Agents fill the Workspace.
        self.occupy(mail, 0, state='running')
        self.queue('waiting-note', 'shortnote')
        self.queue('more-mail')
        self.tick()
        self.assertEqual(self.ledger.tasks.get('waiting-note')['state'], 'queued')
        self.assertEqual(self.ledger.tasks.get('more-mail')['data']['workspace_id'], mail['workspace_id'])

        self.queue('waiting-reply', 'reply')
        self.queue('research-at-share')
        self.tick()
        self.assertEqual(self.ledger.tasks.get('waiting-reply')['state'], 'queued')  # mail group full
        finished = self.ledger.tasks.get('more-mail')
        self.ledger.tasks.update(finished['id'], finished['revision'], 'held',
                                 {**finished['data'], 'cleanup': {'deleted': True}})
        self.tick()
        self.assertEqual(self.ledger.tasks.get('waiting-reply')['state'], 'running')
        self.assertEqual(self.ledger.tasks.get('research-at-share')['state'], 'queued')

    def running_mathesis(self, entry, index):
        name = 'mathesis-active-'+str(index)
        row = self.queue(name, 'mathesis')
        self.ledger.tasks.update(name, row['revision'], 'running',
            {'agent_id': 'agt_'+name, 'response_id': 'resp_'+name,
             'sandbox_id': entry['sandbox_id'], 'workspace_id': entry['workspace_id']})

    def test_shortnote_backlog_fills_idle_notes_slots_while_mail_is_full(self):
        from test_tasks import use_fixture_workflow_revisions
        use_fixture_workflow_revisions(self)
        self.cfg.update(kind_quota='equal', followup_lane=True)
        self.cfg['kinds'].update(mathesis=True, reply=True, firstcontact=True, submission=True, daily=False)
        self.cfg['sandbox_pool']['groups'][1]['kinds'].extend(['firstcontact', 'submission', 'daily'])
        notes, mail = self.tagged_entries()
        self.running_mathesis(notes, 0)
        for index in range(6):
            self.occupy(mail, index, state='running')
        self.queue('waiting-mail', 'reply')
        for index in range(200):
            self.queue('shortnote-'+str(index), 'shortnote')

        self.tick()

        self.assertEqual([body['workspace_id'] for body in self.cma.creates.values()],
                         [notes['workspace_id']]*5)
        counts = lane_summary(self.ledger.tasks.all(), self.cfg)
        self.assertEqual(counts['shortnote'], {'active': 5, 'queued': 195, 'quota': 2, 'borrowing': 3})
        self.assertEqual(counts['mathesis'], {'active': 1, 'queued': 0, 'quota': 2, 'borrowing': 0})
        self.assertEqual(self.ledger.tasks.get('waiting-mail')['state'], 'queued')

    def test_notes_lane_at_group_share_yields_to_waiting_shortnotes(self):
        self.cfg.update(kind_quota='equal', followup_lane=True)
        self.cfg['kinds']['mathesis'] = True
        notes, _ = self.tagged_entries()
        for index in range(3):
            self.running_mathesis(notes, index)
        self.queue('waiting-mathesis', 'mathesis')
        for index in range(3):
            self.queue('shortnote-'+str(index), 'shortnote')

        self.tick()

        self.assertEqual(self.ledger.tasks.get('waiting-mathesis')['state'], 'queued')
        self.assertEqual([body['name'] for body in self.cma.creates.values()],
                         ['outreach-shortnote-'+str(index) for index in range(3)])

    def test_parallel_creation_reservations_survive_restart_and_enforce_group_caps(self):
        self.cfg['sandbox_pool']['agents_per_sandbox'] = 1
        self.cma.create_status = 'queued'
        self.queue('first-note', 'shortnote')
        self.queue('second-note', 'shortnote')
        self.queue('first-mail')
        self.queue('second-mail')
        self.tick()
        self.assertEqual([body['name'] for body in self.cma.creates.values()],
                         ['outreach-first-note', 'outreach-first-mail'])
        for name, group in [('first-note', 'notes'), ('first-mail', 'mail')]:
            self.assertEqual(self.ledger.tasks.get(name)['data']['pool_group'], group)
        self.restore()
        for operation in self.cma.operations.values():
            operation['status'] = 'succeeded'
        self.tick()
        self.assertEqual([(e['group'], e['created_by_task']) for e in self.state()],
                         [('notes', 'first-note'), ('mail', 'first-mail')])
        self.assertEqual(len(self.cma.creates), 2)

    def test_capacity_failure_does_not_block_other_group_creation_or_clear_its_evidence(self):
        self.queue('note', 'shortnote')
        self.queue('mail')
        operation = self.cma.operation

        def reject_first(operation_id):
            if operation_id == 'op_1':
                return {'status': 'failed', 'error': 'sandbox limit reached: 10 of 10'}
            return operation(operation_id)

        self.cma.operation = reject_first
        self.tick()
        self.assertEqual(self.ledger.tasks.get('note')['state'], 'queued')
        self.assertEqual(self.ledger.tasks.get('mail')['state'], 'running')
        self.assertEqual(self.state('sandbox_pool_capacity'), {'groups': {'notes': self.now}})

    def test_expired_notes_capacity_window_fills_existing_slots_without_creation(self):
        self.cfg.update(kind_quota='equal', followup_lane=True)
        self.cfg['sandbox_pool'].update(rotate_after_agents=24, quota_blocked_max_agents_placed=192)
        blocked_at = self.now
        self.now += 3 * 3600 + 1800
        notes, _ = self.tagged_entries(placements=(37, 1))
        notes['sandbox_id'] = 'sbx_6e01'
        notes['draining'] = True
        self.ledger.tasks.db.execute('UPDATE pipeline_state SET payload=? WHERE key=?',
                                     (canonical([notes]), 'sandbox_pool'))
        live = self.queue('live-note', 'shortnote')
        self.ledger.tasks.update(live['id'], live['revision'], 'running',
            {'agent_id': 'agt_live_note', 'response_id': 'resp_live_note',
             'sandbox_id': notes['sandbox_id'], 'workspace_id': notes['workspace_id']})
        self.ledger.tasks.db.execute('INSERT INTO pipeline_state VALUES(?,?)',
            ('sandbox_pool_capacity', canonical({'groups': {'notes': blocked_at}})))
        for index in range(211):
            self.queue(f'waiting-note-{index:03}', 'shortnote')

        with patch.object(self.cma, 'create', wraps=self.cma.create) as attempts:
            self.tick()

        self.assertEqual([call.args[0].get('workspace_id') for call in attempts.call_args_list],
                         [notes['workspace_id']] * 5)
        rows = self.ledger.tasks.all()
        self.assertEqual(sum(row['state'] == 'queued' for row in rows), 206)
        self.assertEqual(sum(row['state'] == 'running' and row['data']['workspace_id'] == notes['workspace_id']
                             for row in rows), 6)
        self.assertEqual(self.state()[0]['agents_placed'], 42)
        self.assertNotIn('draining', self.state()[0])
        self.assertEqual(self.state('sandbox_pool_capacity'), {'groups': {'notes': blocked_at}})

    def test_group_sandbox_keeps_admitting_while_old_agent_finishes(self):
        notes, mail = self.tagged_entries(placements=(37, 1))
        self.occupy(notes, 0, state='running')
        for index in range(7):
            self.queue(f'waiting-note-{index}', 'shortnote')
        self.queue('mail')

        self.tick()

        self.assertEqual(len(self.state()), 2)
        self.assertEqual([body.get('workspace_id') for body in self.cma.creates.values()].count(None), 0)
        self.assertEqual(self.state()[0]['agents_placed'], 42)
        for index in (5, 6):  # The max-1 group waits for a free slot.
            self.assertEqual(self.ledger.tasks.get(f'waiting-note-{index}')['state'], 'queued')
        self.assertEqual(self.ledger.tasks.get('mail')['data']['workspace_id'], mail['workspace_id'])

    def test_full_global_pool_keeps_admitting_into_notes_past_former_ceiling(self):
        self.cfg['sandbox_pool'].update(max_sandboxes=2, quota_blocked_max_agents_placed=192)
        blocked_at = self.now
        self.now += CAPACITY_BLOCK_SECONDS + 1
        notes, _ = self.tagged_entries(placements=(191, 1))
        self.occupy(notes, 0, state='running')
        self.ledger.tasks.db.execute('INSERT INTO pipeline_state VALUES(?,?)',
            ('sandbox_pool_capacity', canonical({'groups': {'notes': blocked_at}})))
        self.queue('last-placement', 'shortnote')
        self.queue('waiting-note', 'shortnote')

        self.tick()

        self.assertEqual([body.get('workspace_id') for body in self.cma.creates.values()],
                         [notes['workspace_id']] * 2)
        self.assertEqual(self.ledger.tasks.get('last-placement')['state'], 'running')
        self.assertEqual(self.ledger.tasks.get('waiting-note')['state'], 'running')
        self.assertEqual(self.state()[0]['agents_placed'], 193)
        self.assertEqual(self.state('sandbox_pool_capacity'), {'groups': {'notes': blocked_at}})

    def test_used_group_sandbox_admits_without_waiting_for_a_replacement(self):
        notes, _ = self.tagged_entries(placements=(37, 1))
        self.cma.create_status = 'queued'
        self.queue('replacement', 'shortnote')
        self.queue('waiting-note', 'shortnote')

        self.tick()

        for name in ('replacement', 'waiting-note'):
            row = self.ledger.tasks.get(name)
            self.assertEqual(row['data']['workspace_id'], notes['workspace_id'])
            self.assertNotIn('pool_create', row['data'])
        self.assertEqual([body.get('workspace_id') for body in self.cma.creates.values()],
                         [notes['workspace_id']] * 2)
        self.assertEqual(self.state()[0]['sandbox_id'], notes['sandbox_id'])
        self.assertEqual(self.state()[0]['agents_placed'], 39)

    def test_stalled_creation_recovers_only_its_group_and_leaves_mail_in_place(self):
        notes, mail = self.tagged_entries(placements=(24, 24))
        pending = self.queue('legacy-creation', 'shortnote')
        self.ledger.tasks.update(pending['id'], pending['revision'], 'provisioning',
            {'pool_create': True, 'pool_create_started_at': self.now,
             'operation_id': 'op_lost', 'create_key': 'original-key', 'sandbox_id': 'sbx_lost'})
        self.now += POOL_WAIT_SECONDS
        self.queue('note', 'shortnote')
        self.queue('mail')
        self.tick()
        recovered = self.ledger.tasks.get(pending['id'])
        self.assertEqual(recovered['state'], 'queued')
        self.assertEqual(recovered['data']['sandbox_failure_history'][0]['operation_id'], 'op_lost')
        self.assertEqual(recovered['data']['sandbox_failure_history'][0]['create_key'], 'original-key')
        self.assertEqual(self.ledger.tasks.get('note')['data']['workspace_id'], notes['workspace_id'])
        self.assertEqual(self.ledger.tasks.get('mail')['data']['workspace_id'], mail['workspace_id'])
        self.assertEqual(self.state('sandbox_pool_capacity'), {'groups': {'notes': self.now}})
        self.assertEqual({e['sandbox_id']: e['group'] for e in self.state('sandbox_pool_cleanup')},
                         {'sbx_lost': 'notes'})

    def test_notes_health_error_does_not_prevent_mail_creation(self):
        notes = {**self.seed_pool()[0], 'group': 'notes'}
        self.ledger.tasks.db.execute('UPDATE pipeline_state SET payload=? WHERE key=?',
                                     (canonical([notes]), 'sandbox_pool'))
        self.cma.statuses[notes['sandbox_id']] = TimeoutError('notes health timed out')
        self.queue('mail')
        self.tick()
        self.assertEqual([e['group'] for e in self.state()], ['notes', 'mail'])
        self.assertEqual(self.ledger.tasks.get('mail')['state'], 'running')

    def test_full_notes_group_does_not_claim_the_credit_probe(self):
        self.cfg['sandbox_pool']['agents_per_sandbox'] = 1
        notes, mail = self.tagged_entries()
        self.occupy(notes, 0, state='running')
        self.ledger.tasks.db.execute('INSERT INTO pipeline_state VALUES(?,?)',
            ('credit_block', canonical({'since': self.now, 'last_probe_at': None, 'probe_task': None})))
        self.queue('note', 'shortnote')
        self.queue('mail')
        self.tick()
        self.assertEqual(self.state('credit_block')['probe_task'], 'mail')
        self.assertEqual(self.ledger.tasks.get('mail')['data']['workspace_id'], mail['workspace_id'])

    def test_global_max_active_keeps_a_free_group_slot_queued(self):
        self.cfg['max_active'] = 1
        notes, mail = self.tagged_entries()
        self.occupy(notes, 0, state='running')
        row = self.queue('mail')
        pool = SandboxPool(self.ledger, self.cfg['sandbox_pool'], self.cfg['kinds'])
        pool.health(self.cma)
        self.assertEqual(pool.slot(row), (True, mail))
        self.tick()
        self.assertEqual(self.ledger.tasks.get(row['id']), row)

    def idle_past_quota_evidence(self, paused=False, extra_notes=False, spare_status='active'):
        """Health and idle passes after the quota evidence and idle grace expired."""
        entries = self.tagged_entries()
        if extra_notes:
            entries.append({**entries[0], 'sandbox_id': 'sbx_spare', 'workspace_id': 'wks_spare'})
            self.ledger.tasks.db.execute('UPDATE pipeline_state SET payload=? WHERE key=?',
                                         (canonical(entries), 'sandbox_pool'))
            self.cma.statuses['sbx_spare'] = {'status': spare_status}
        self.ledger.tasks.db.execute('INSERT INTO pipeline_state VALUES(?,?)', (
            'sandbox_pool_capacity', canonical({'groups': {'notes': self.now, 'mail': self.now}})))
        # Placement fell behind (advance ran out of tick budget); only health runs.
        self.now += CAPACITY_BLOCK_SECONDS + 600
        pool = SandboxPool(self.ledger, self.cfg['sandbox_pool'], self.cfg['kinds'], paused=paused)
        pool.health(self.cma)
        pool.idle(self.cma)
        rotated = {e['sandbox_id']: e.get('rotated') for e in self.state('sandbox_pool_cleanup')}
        return pool, entries, rotated

    def test_group_keeps_its_last_sandbox_while_its_queued_work_waits(self):
        waiting = self.queue('waiting', 'shortnote')
        pool, (notes, mail), rotated = self.idle_past_quota_evidence()
        # The mail group has no queued work and still rotates its idle Sandbox.
        self.assertEqual(rotated, {mail['sandbox_id']: 'idle'})
        self.assertEqual([e['sandbox_id'] for e in self.state()], [notes['sandbox_id']])
        self.assertIn(notes['sandbox_id'], pool.available)
        self.assertEqual(pool.slot(waiting), (True, self.state()[0]))
        # Without waiting work the same Sandbox rotates as idle after the idle period.
        self.ledger.tasks.update('waiting', waiting['revision'], 'failed', {})
        pool = SandboxPool(self.ledger, self.cfg['sandbox_pool'], self.cfg['kinds'])
        pool.health(self.cma)
        self.assertEqual([e['sandbox_id'] for e in self.state()], [notes['sandbox_id']])
        self.now += pool.idle_seconds
        pool.health(self.cma)
        self.assertEqual(self.state(), [])
        self.assertEqual(self.state('sandbox_pool_cleanup')[-1]['rotated'], 'idle')

    def test_idle_rotation_is_unchanged_when_paused(self):
        self.queue('waiting', 'shortnote')
        _, (notes, mail), rotated = self.idle_past_quota_evidence(paused=True)
        self.assertEqual(rotated, {notes['sandbox_id']: 'idle', mail['sandbox_id']: 'idle'})

    def test_idle_rotation_is_unchanged_for_disabled_kinds(self):
        self.queue('waiting', 'mathesis')  # The notes group's mathesis kind is not enabled.
        _, (notes, mail), rotated = self.idle_past_quota_evidence()
        self.assertEqual(rotated, {notes['sandbox_id']: 'idle', mail['sandbox_id']: 'idle'})

    def test_every_sandbox_of_a_group_with_queued_work_is_kept(self):
        self.queue('waiting', 'shortnote')
        _, (notes, mail, spare), rotated = self.idle_past_quota_evidence(extra_notes=True)
        self.assertEqual([e['sandbox_id'] for e in self.state()], [notes['sandbox_id'], spare['sandbox_id']])
        self.assertEqual(rotated, {mail['sandbox_id']: 'idle'})

    def test_peer_that_failed_since_the_last_pass_does_not_count_as_usable(self):
        waiting = self.queue('waiting', 'shortnote')
        pool, (notes, mail, spare), rotated = self.idle_past_quota_evidence(extra_notes=True,
                                                                            spare_status='failed')
        # The healthy Sandbox is kept; its failed peer follows failure cleanup.
        self.assertEqual(rotated, {mail['sandbox_id']: 'idle', spare['sandbox_id']: None})
        self.assertEqual(self.state('sandbox_pool_cleanup')[-1]['state'], 'failed')
        self.assertEqual([e['sandbox_id'] for e in self.state()], [notes['sandbox_id']])
        self.assertEqual(pool.slot(waiting), (True, self.state()[0]))


class IdleOnlyRetirementTests(GroupFixture, unittest.TestCase):
    """Owner decision 2026-11-26: retire a pool Sandbox only when its group is idle."""

    def admits_into_used_sandbox(self, count):
        # The notes group (max 1) has one active, observed Sandbox with two live
        # Agents, no capacity evidence and room for another Sandbox in the pool.
        notes, _ = self.tagged_entries(placements=(count, 1))
        for index in range(2):
            self.occupy(notes, index, state='running')
        self.queue('note', 'shortnote')
        self.tick()
        self.assertEqual([body.get('workspace_id') for body in self.cma.creates.values()],
                         [notes['workspace_id']])
        self.assertEqual(self.ledger.tasks.get('note')['data']['workspace_id'], notes['workspace_id'])
        self.assertEqual(self.ledger.tasks.get('note')['data']['pool_sandbox_id'], notes['sandbox_id'])
        self.assertEqual(self.state()[0]['agents_placed'], count + 1)
        self.assertEqual(self.state('sandbox_pool_cleanup'), [])

    def test_sandbox_with_24_placements_admits_group_work_without_creating_a_sandbox(self):
        self.admits_into_used_sandbox(24)

    def test_sandbox_with_60_placements_admits_group_work_without_creating_a_sandbox(self):
        self.admits_into_used_sandbox(60)

    def test_sandbox_with_200_placements_admits_group_work_without_creating_a_sandbox(self):
        self.admits_into_used_sandbox(200)

    def test_idle_group_retires_its_used_sandbox_through_cleanup_with_a_stable_key(self):
        notes, _ = self.tagged_entries(placements=(200, 1))
        self.occupy(notes, 0, deleted=True)  # Finished; its Agent is deleted.
        self.queue('disabled-kind', 'mathesis')  # Queued work of a disabled kind does not count.
        self.queue('mail-work')  # Keeps the other group's Sandbox out of this test.
        lost = []

        def lose_first_notes_delete(sandbox_id, key):
            if sandbox_id == notes['sandbox_id'] and not lost:
                lost.append(key)
                raise TimeoutError('No response')

        self.cma.before_retire = lose_first_notes_delete
        self.now += 600
        pool = SandboxPool(self.ledger, self.cfg['sandbox_pool'], self.cfg['kinds'], reserve_review_slots=1)
        pool.health(self.cma)
        self.ledger.save(force=True)
        [retired] = self.state('sandbox_pool_cleanup')
        self.assertEqual((retired['sandbox_id'], retired['state'], retired['rotated'], retired['agents_placed']),
                         (notes['sandbox_id'], 'retiring', 'idle', 200))
        self.assertEqual(retired['cleanup_intent'], {'key': 'outreach-pool-retire-' + notes['sandbox_id']})
        self.restore()
        SandboxPool(self.ledger, self.cfg['sandbox_pool'], self.cfg['kinds'], reserve_review_slots=1).health(self.cma)
        deletes = [key for path, method, key in self.cma.calls if method == 'DELETE']
        self.assertEqual(deletes, ['outreach-pool-retire-' + notes['sandbox_id'] + '-sandbox-delete'] * 2)
        self.assertEqual(lost, deletes[:1])

    def kept_at_high_count(self, prepare=lambda notes: None, elapsed=600, **pool_args):
        notes, _ = self.tagged_entries(placements=(200, 1))
        prepare(notes)
        self.now += elapsed
        pool = SandboxPool(self.ledger, self.cfg['sandbox_pool'], self.cfg['kinds'], **pool_args)
        pool.health(self.cma)
        pool.idle(self.cma)
        self.assertIn(notes['sandbox_id'], [e['sandbox_id'] for e in self.state()])
        self.assertNotIn(notes['sandbox_id'], [e['sandbox_id'] for e in self.state('sandbox_pool_cleanup')])
        self.assertFalse([path for path, method, _ in self.cma.calls
                          if method == 'DELETE' and notes['sandbox_id'] in path])
        return notes

    def test_live_agent_keeps_the_sandbox(self):
        self.kept_at_high_count(lambda notes: self.occupy(notes, 0, state='running'))

    def test_queued_group_task_keeps_the_sandbox(self):
        self.kept_at_high_count(lambda notes: self.queue('waiting', 'shortnote'))

    def test_pending_review_keeps_the_sandbox(self):
        self.cfg['kinds']['mathesis'] = True

        def pending_review(notes):
            row = self.queue('reviewable', 'mathesis')
            self.ledger.tasks.update(row['id'], row['revision'], 'awaiting-review',
                                     {'result': {'outcome': 'completed'}})

        self.kept_at_high_count(pending_review, reserve_review_slots=1)

    def test_provisioning_row_reserved_on_it_keeps_the_sandbox(self):
        def reserved(notes):
            row = self.queue('reserved', 'shortnote')
            self.ledger.tasks.update(row['id'], row['revision'], 'provisioning',
                                     {'pool_sandbox_id': notes['sandbox_id'], 'pool_group': 'notes'})

        self.kept_at_high_count(reserved)

    def test_creation_in_flight_in_the_group_keeps_the_sandbox(self):
        def creating(notes):
            row = self.queue('creating', 'shortnote')
            self.ledger.tasks.update(row['id'], row['revision'], 'provisioning',
                                     {'pool_create': True, 'pool_group': 'notes', 'operation_id': 'op_new',
                                      'pool_create_started_at': self.now + 600})

        self.kept_at_high_count(creating)

    def test_idle_period_not_yet_elapsed_keeps_the_sandbox(self):
        self.kept_at_high_count(elapsed=599)

    def test_capacity_block_keeps_the_groups_last_usable_sandbox(self):
        self.kept_at_high_count(lambda notes: self.ledger.tasks.db.execute(
            'INSERT INTO pipeline_state VALUES(?,?)',
            ('sandbox_pool_capacity', canonical({'groups': {'notes': self.now}}))))

    def test_paused_sandbox_whose_group_has_work_is_kept(self):
        def paused_with_work(notes):
            self.cma.statuses[notes['sandbox_id']] = {'status': 'paused'}
            self.queue('waiting', 'shortnote')

        self.kept_at_high_count(paused_with_work)
        self.assertEqual(self.state()[0]['state'], 'paused')

    def test_group_work_restarts_the_idle_period(self):
        notes = self.kept_at_high_count(lambda notes: self.queue('waiting', 'shortnote'))
        waiting = self.ledger.tasks.get('waiting')
        self.ledger.tasks.update('waiting', waiting['revision'], 'failed', {})
        pool = SandboxPool(self.ledger, self.cfg['sandbox_pool'], self.cfg['kinds'])
        self.now += 599
        pool.health(self.cma)
        self.assertEqual(self.state()[0]['sandbox_id'], notes['sandbox_id'])
        self.now += 1
        pool.health(self.cma)
        self.assertEqual(self.state('sandbox_pool_cleanup')[0]['rotated'], 'idle')

    def test_full_sandbox_in_a_max_one_group_makes_work_wait_without_a_second_sandbox(self):
        notes, _ = self.tagged_entries(placements=(200, 1))
        occupants = [self.occupy(notes, index, state='running') for index in range(6)]
        self.queue('waiting-a', 'shortnote')
        self.queue('waiting-b', 'shortnote')
        self.tick()
        self.assertFalse(self.cma.creates)
        self.assertEqual([self.ledger.tasks.get(n)['state'] for n in ('waiting-a', 'waiting-b')], ['queued'] * 2)
        self.assertEqual(len(self.state()), 2)
        finished = self.ledger.tasks.get(occupants[0]['id'])
        self.ledger.tasks.update(finished['id'], finished['revision'], 'held',
                                 {**finished['data'], 'cleanup': {'deleted': True}})
        self.tick()
        self.assertEqual([body.get('workspace_id') for body in self.cma.creates.values()], [notes['workspace_id']])
        self.assertEqual(sorted(self.ledger.tasks.get(n)['state'] for n in ('waiting-a', 'waiting-b')),
                         ['queued', 'running'])

    def test_review_slot_reservation_is_unchanged_at_a_high_placement_count(self):
        notes, _ = self.tagged_entries(placements=(200, 1))
        for index in range(5):
            self.occupy(notes, index, state='running')
        row = self.queue('note', 'shortnote')
        pool = SandboxPool(self.ledger, self.cfg['sandbox_pool'], self.cfg['kinds'], reserve_review_slots=1)
        pool.health(self.cma)
        self.assertEqual(pool.admission_limit(pool.entries[0], self.ledger.tasks.all()), 5)
        self.assertEqual(pool.slot(row), (False, None))
        finished = self.ledger.tasks.get(notes['sandbox_id'] + '-0')
        self.ledger.tasks.update(finished['id'], finished['revision'], 'held',
                                 {**finished['data'], 'cleanup': {'deleted': True}})
        self.assertEqual(pool.slot(row), (True, pool.entries[0]))
