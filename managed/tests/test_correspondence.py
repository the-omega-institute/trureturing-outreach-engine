"""Registry evidence and reply lineage drive both placement and queue reporting."""
import unittest
from unittest.mock import patch

from managed import automation, control, correspondence, lanes, mathesis_delivery
from managed.queue import canonical
from managed.registry import write
from managed.tick import Tick
from test_sandbox_pool import PoolFixture
from test_tasks import request


class CorrespondenceTests(PoolFixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.root = self.folder / 'registry'
        (self.root / 'tables').mkdir(parents=True)
        self.cfg.update(kind_quota='equal', followup_lane=True)
        self.cfg['kinds'].update(shortnote=True, mathesis=True, reply=True)
        self.cfg['sandbox_pool'].update(prioritize_correspondence_notes=True, groups=[
            {'name': 'notes', 'kinds': ['shortnote', 'mathesis'], 'max_sandboxes': 1},
            {'name': 'mail', 'kinds': ['reply', 'research'], 'max_sandboxes': 1},
        ])
        write(self.root, 'refusals', {'refusals': []})
        write(self.root, 'positive', {'entries': [{'contact_ids': ['positive-alias']}]})
        contacts = [
            self.contact('waiting', 'bulk-problem', status='sent_waiting_reply'),
            self.contact('conversation', 'discussion', status='in_conversation'),
            self.contact('reply-event', 'event', hub={'events': [{'kind': 'reply', 'at': '2026-11-18'}]}),
            self.contact('positive', 'praise'),
            self.contact('positive-alias', alias_of='positive@example.invalid'),
            self.contact('open', 'open-problem'),
            self.contact('finished', 'finished-problem'),
        ]
        write(self.root, 'contacts', {'contacts': contacts})

    def contact(self, name, problem=None, hub=None, **relationship):
        return {'id': name, 'name': name.title(), 'email': name + '@example.invalid',
                'problem_ids': [problem] if problem else [], 'relationship': relationship, 'hub': hub or {}}

    def queue(self, name, kind='shortnote', owner=None, **inputs):
        return self.ledger.tasks.add({**request(name, owner or 'owner-' + name, kind), 'inputs': inputs})

    def set_state(self, row, state, **data):
        return self.ledger.tasks.update(row['id'], row['revision'], state, data)

    def linked(self):
        return correspondence.linked_contacts(self.ledger.tasks.all(), self.root, self.cfg)

    def test_classifies_table_evidence_and_open_reply_by_exact_problem_id(self):
        self.queue('a-conversation', problem_ids=['discussion'])
        self.queue('an-event', 'mathesis', result={'id': 'event'})
        self.queue('some-praise', owner='result-praise')
        self.queue('open-reply', 'reply', owner='open')
        self.queue('open-note', result_id='open-problem')
        self.queue('bulk', result_id='bulk-problem', owner_ids=['waiting'])
        self.queue('different-problem', result_id='unrelated', owner_ids=['conversation'])
        self.set_state(self.queue('finished-reply', 'reply', owner='finished'), 'verified')
        self.queue('finished-note', problem_id='finished-problem')
        # Praise, an old reply or a conversation status alone do not link a
        # result; only a live reply task does.
        self.assertEqual({key: [p['id'] for p in contacts] for key, contacts in self.linked().items()}, {
            'open-note': ['open'],
        })

    def test_origin_survives_a_finished_decision_reply_and_downstream_inheritance(self):
        parent = self.set_state(self.queue('reply-parent', 'reply', owner='open'), 'revising')
        decision = self.set_state(self.queue('decision', 'reply', owner='open',
                                             automation_parent={'task_id': parent['id'], 'purpose': 'user-answer'}),
                                  'verified')
        self.queue('note', reply_origin_task_id=decision['id'])
        self.queue('revision', automation_parent={'task_id': 'note', 'purpose': 'revision'})
        self.queue('handoff', 'mathesis', pipeline={'note_task_id': 'revision'})
        self.queue('selected-handoff', 'mathesis', note_sources=[{'task_id': 'revision', 'result_hash': 'a' * 64}])
        self.queue('direct-continuation', automation_parent={'task_id': decision['id']})
        self.queue('unknown-origin', reply_origin_task_id='missing')
        self.queue('nonreply-origin', reply_origin_task_id='note')
        self.assertEqual({key: contacts[0]['id'] for key, contacts in self.linked().items()},
                         {key: 'open' for key in ('note', 'revision', 'handoff', 'selected-handoff',
                                                 'direct-continuation')})

    def test_reply_line_placement_and_order_with_deferred_bulk(self):
        notes, mail = self.seed_pool(2)
        notes['group'], mail['group'] = 'notes', 'mail'
        self.ledger.tasks.db.execute('UPDATE pipeline_state SET payload=? WHERE key=?',
                                     (canonical([notes, mail]), 'sandbox_pool'))
        bulk = self.queue('bulk', result_id='bulk-problem', owner_ids=['waiting'])
        legacy = self.set_state(self.queue('legacy', result_id='event'), 'running',
                                pool_group='notes', sandbox_id=notes['sandbox_id'],
                                agent_id='agt_legacy', response_id='resp_legacy')
        self.set_state(self.queue('open-reply', 'reply', owner='open'), 'awaiting-user')
        self.queue('linked-note', result_id='open-problem')
        self.queue('linked-mathesis', 'mathesis', result_id='open-problem')
        self.queue('ordinary-mathesis', 'mathesis', result_id='bulk-problem')
        deferred = {'units': {'advance': ['bulk']}}
        self.ledger.tasks.db.execute('INSERT INTO pipeline_state VALUES (?, ?)',
                                     ('tick_deferred', canonical(deferred)))
        Tick(600).bind(self.ledger)
        with patch('managed.control.linked_contacts',
                   side_effect=lambda rows, root, config: correspondence.linked_contacts(rows, self.root, config)):
            self.tick()
        creates = [(body['name'], body['workspace_id']) for body in self.cma.creates.values()]
        self.assertEqual(creates, [
            ('outreach-linked-note', mail['workspace_id']),
            ('outreach-linked-mathesis', mail['workspace_id']),
            ('outreach-bulk', notes['workspace_id']),
            ('outreach-ordinary-mathesis', notes['workspace_id']),
        ])
        self.assertEqual(self.ledger.tasks.get(legacy['id'])['data']['sandbox_id'], notes['sandbox_id'])
        self.assertEqual(lanes.lane(self.ledger.tasks.get(bulk['id']), self.cfg), 'followup')
        self.assertEqual(lanes.summary(self.ledger.tasks.all(), self.cfg)['correspondence']['active'], 2)

    def test_work_reports_contact_and_position_in_reply_lane(self):
        self.queue('bulk', result_id='bulk-problem', owner_ids=['waiting'])
        self.queue('reply-a', 'reply', owner='open')
        self.queue('note', result_id='open-problem')
        self.queue('mathesis', 'mathesis', result_id='open-problem')
        with patch('managed.daily_local.status', return_value={}):
            view = automation.work(self.ledger.tasks, self.root, self.cfg)
        self.assertEqual(view['correspondence_note_queue'], [
            {'task_id': 'note', 'kind': 'shortnote', 'position': 2, 'contacts': [
                {'id': 'open', 'name': 'Open', 'email': 'open@example.invalid'}]},
            {'task_id': 'mathesis', 'kind': 'mathesis', 'position': 3, 'contacts': [
                {'id': 'open', 'name': 'Open', 'email': 'open@example.invalid'}]},
        ])
        self.assertEqual(view['lanes']['reply']['queued'], 1)
        self.assertEqual(view['lanes']['correspondence']['queued'], 2)
        self.assertEqual(view['lanes']['followup']['queued'], 1)

    def test_disabled_policy_keeps_the_previous_lane_and_order(self):
        self.cfg['sandbox_pool']['prioritize_correspondence_notes'] = False
        first = self.queue('first', result_id='bulk-problem')
        second = self.queue('second', result_id='discussion')
        self.assertEqual(self.linked(), {})
        self.assertEqual(control.dispatch_order([first, second], self.cfg), [first, second])
        self.assertEqual(lanes.lane(second, self.cfg), 'shortnote')

    def test_linked_handoff_is_selected_before_older_pending_deliveries(self):
        for index in range(4):
            row = self.queue(f'bulk-{index}', 'mathesis', result_id='bulk-problem')
            self.set_state(row, 'handoff-ready', mathesis_delivery={'phase': 'ready'})
        self.now += 1
        self.set_state(self.queue('open-reply', 'reply', owner='open'), 'awaiting-user')
        row = self.queue('linked', 'mathesis')
        self.set_state(row, 'handoff-ready', mathesis_delivery={
            'phase': 'ready', 'envelope': {'metadata': {'origin': {'problem_ids': ['open-problem']}}},
        })
        with patch('managed.mathesis_delivery.units', return_value=[]) as units:
            mathesis_delivery.advance(self.ledger, self.cfg, self.root, self.folder)
        self.assertEqual([r['id'] for r in units.call_args.args[2]], ['linked', 'bulk-0', 'bulk-1', 'bulk-2'])
