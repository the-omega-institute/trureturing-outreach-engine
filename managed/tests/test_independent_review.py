"""Production review survives writer restarts without a local supervisor."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from managed import independent_review as review
from managed.queue import canonical
from managed.sandbox_pool import live_reference, placements, ledger_agents, SandboxPool
from managed.tasks import Tasks
from test_review_binding import request


class Releases:
    repo = 'example/private-data'

    def __init__(self):
        self.files = {}

    def preserve_task(self, path, tag):
        key = tag, path.name
        raw = path.read_bytes()
        if key in self.files:
            assert raw == self.files[key]
        self.files[key] = raw


class Ledger:
    def __init__(self, tasks):
        self.tasks = tasks
        self.releases = Releases()
        self.saves = 0

    def save(self, **kwargs):
        self.saves += 1


class CMA:
    def __init__(self, ledger):
        self.ledger = ledger
        self.creates = {}
        self.commands = {}
        self.retirements = []
        self.lose_admission = False
        self.status = 'completed'
        self.change_verdict = lambda value: value

    def create(self, body, key):
        # The original create intent must already be durable before the write.
        run = next(row['data']['hosted_review'] for row in self.ledger.tasks.all()
                   if row['data'].get('hosted_review', {}).get('create_key') == key)
        assert run['create_body'] == body and run['create_key'] == key
        self.creates.setdefault(key, copy.deepcopy(body))
        return {'id': 'operation-' + key}

    def operation(self, identity):
        number = list(self.creates).index(identity.removeprefix('operation-')) + 1
        return {'status': 'succeeded', 'agent_id': 'agt_review' + (str(number) if number > 1 else '')}

    def admit(self, agent, prompt, key):
        run = next(row['data']['hosted_review'] for row in self.ledger.tasks.all()
                   if row['data'].get('hosted_review', {}).get('command_key') == key)
        assert run['agent_id'] == agent and run['prompt'] == prompt and run['command_key'] == key
        self.commands.setdefault(key, (agent, prompt))
        if self.lose_admission:
            self.lose_admission = False
            raise TimeoutError('accepted, acknowledgement lost')
        return {'response_id': 'response-' + key}

    def response(self, identity):
        row = next(row for row in self.ledger.tasks.all()
                   if row['data'].get('hosted_review', {}).get('response_id') == identity)
        run = row['data']['hosted_review']
        value = self.change_verdict({'binding': run['binding'],
            'current_workflow_hash': run['current_workflow_hash'], 'verdict': 'PASS',
            'reason': 'Checked the complete exact source and evidence.',
            'checked_artifacts': [{k: a[k] for k in ('name', 'bytes', 'sha256')}
                                  for a in row['data']['result']['artifacts']]})
        return {'status': self.status, 'output': [{'type': 'message',
            'content': [{'type': 'output_text', 'text': canonical(value)}]}]}

    def retire(self, agent, key, **kwargs):
        self.retirements.append((agent, key))
        return {'deleted': True}


class HostedReviewTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.tasks = Tasks(self.root / 'state.sqlite3')
        row = self.tasks.add(request())
        self.result = {'schema_version': 1, 'task_id': row['id'], 'outcome': 'completed',
                       'report': 'A checked academic result.', 'artifacts': [], 'packet': None}
        self.tasks.update(row['id'], row['revision'], 'awaiting-review', {
            'agent_id': 'agt_author', 'pool_group': 'mail', 'result': self.result,
            'cleanup': {'stopped': True}})
        self.entry = {'state': 'active', 'group': 'mail', 'sandbox_id': 'sbx_mail', 'workspace_id': 'wks_mail'}
        self.tasks.db.execute('INSERT INTO pipeline_state VALUES(?,?)', ('sandbox_pool', canonical([self.entry])))
        self.ledger = Ledger(self.tasks)
        self.cma = CMA(self.ledger)
        self.config = {'paused': False, 'automation': {'enabled': True},
            'independent_review': {'enabled': True, 'max_active': 1},
            'agent_template': {'profile': {'model': 'gpt-6.2-sol'}},
            'sandbox_pool': {'agents_per_sandbox': 6}}
        self.policy = patch('managed.independent_review.contracts', return_value={'review.md': 'Check the complete proof.'})
        self.workflow = patch('managed.control.workflow', return_value='b' * 64)
        self.policy.start()
        self.workflow.start()

    def tearDown(self):
        self.policy.stop()
        self.workflow.stop()
        self.tasks.close()
        self.tmp.cleanup()

    def advance(self):
        review.advance(self.ledger, self.cma, self.config, self.root, 'signing-key')

    def row(self):
        return self.tasks.get('reply-1')

    def test_exact_review_is_signed_and_routes_without_local_loop(self):
        self.config['identity'] = {'firstcontact_signature':'Verified configured signature'}
        self.advance()
        row = self.row()
        self.assertEqual(row['state'], 'approved')
        self.assertEqual(row['data']['review']['reviewer'], review.REVIEWER)
        self.assertEqual(row['data']['hosted_review']['phase'], 'completed')
        self.assertNotIn('signing-key', canonical(self.cma.creates) + canonical(self.cma.commands))
        self.assertTrue(any('independent-review-' in name for tag, name in self.ledger.releases.files))
        self.advance()
        self.assertTrue(self.row()['data']['hosted_review']['cleanup_complete'])
        self.assertEqual(len(self.cma.creates), 1)
        payload = next(json.loads(raw) for (tag, name), raw in self.ledger.releases.files.items()
                       if name.startswith('review-input-'))
        self.assertEqual(payload['identity'], self.config['identity'])

    def test_review_snapshot_contains_workflow_routing_and_operative_policy(self):
        automation = {'routing_contract':{}, 'mail':{'automatic':'exact signed PASS'}, 'continuation':{}}
        def read(path):
            return canonical(automation) if path == 'automation.json' else 'operative ' + path
        self.policy.stop()
        with patch('managed.control.workflow_text', side_effect=read), \
             patch('managed.policy.policy_text', return_value='operative AGENTS'):
            rules = review.contracts('reply')
        self.assertIn('replies/workflow.json', rules)
        self.assertIn('firstcontact/review.md', rules)
        self.assertEqual(rules['automation.json'], automation)
        self.assertEqual(rules['AGENTS.md'], 'operative AGENTS')
        self.policy.start()

    def test_lost_admission_reuses_original_agent_prompt_and_key(self):
        self.cma.lose_admission = True
        self.advance()
        row = self.row()
        run = row['data']['hosted_review']
        self.assertEqual(row['state'], 'awaiting-review')
        run['next_retry_at'] = 0
        review.write(self.ledger, row['id'], run)
        self.advance()
        self.assertEqual(self.row()['state'], 'approved')
        self.assertEqual(len(self.cma.creates), 1)
        self.assertEqual(len(self.cma.commands), 1)

    def test_changed_result_cannot_receive_prior_review(self):
        self.cma.status = 'in_progress'
        self.advance()
        row = self.row()
        self.tasks.update(row['id'], row['revision'], row['state'], {
            **row['data'], 'result': {**self.result, 'report': 'A revised result'}})
        self.cma.status = 'completed'
        self.advance()
        self.assertEqual(self.row()['state'], 'awaiting-review')
        self.assertEqual(self.row()['data']['hosted_review']['phase'], 'obsolete')
        self.assertNotIn('review', self.row()['data'])

    def test_pass_requires_every_exact_artifact(self):
        row = self.row()
        result = {**self.result, 'artifacts': [{'name': 'note.pdf', 'bytes': 42, 'sha256': 'a' * 64}]}
        self.tasks.update(row['id'], row['revision'], row['state'], {**row['data'], 'result': result})
        self.cma.change_verdict = lambda v: {**v, 'checked_artifacts': []}
        self.advance()
        self.assertEqual(self.row()['state'], 'awaiting-review')
        self.assertEqual(self.row()['data']['hosted_review']['phase'], 'needs_attention')
        self.assertNotIn('review', self.row()['data'])

    def test_obsolete_provisioning_resolves_original_create_before_cleanup(self):
        self.cma.operation = lambda identity: {'status':'pending'}
        self.advance()
        row = self.row()
        self.tasks.update(row['id'], row['revision'], row['state'], {
            **row['data'], 'result':{**self.result, 'report':'Changed during provisioning'}})
        self.advance()
        self.assertEqual(self.row()['data']['hosted_review']['phase'], 'obsolete')
        self.cma.operation = lambda identity: {'status':'succeeded', 'agent_id':'agt_review'}
        self.advance()
        self.assertTrue(self.row()['data']['hosted_review']['cleanup_complete'])
        self.assertEqual(len(self.cma.creates), 1)
        self.assertEqual(self.cma.commands, {})

    def test_committed_verdict_is_recovered_after_lost_checkpoint(self):
        original = self.tasks.verdict
        def lose_ack(*args, **kwargs):
            original(*args, **kwargs)
            raise TimeoutError('verdict committed before writer interruption')
        with patch.object(self.tasks, 'verdict', side_effect=lose_ack):
            self.advance()
        self.assertEqual(self.row()['state'], 'approved')
        run = self.row()['data']['hosted_review']; run['next_retry_at'] = 0
        review.write(self.ledger, 'reply-1', run)
        self.advance()
        self.assertEqual(self.row()['data']['hosted_review']['phase'], 'completed')
        self.assertEqual(self.tasks.db.execute('SELECT count(*) FROM control_events').fetchone()[0], 1)
        self.assertEqual(len(self.cma.commands), 1)

    def test_pause_and_live_author_do_not_admit_reviewer(self):
        self.config['paused'] = True
        self.advance()
        self.assertFalse(self.cma.creates)
        self.config['paused'] = False
        row = self.row()
        self.tasks.update(row['id'], row['revision'], row['state'], {**row['data'], 'cleanup': {}})
        self.advance()
        self.assertFalse(self.cma.creates)

    def test_disabled_kind_and_unavailable_input_do_not_block_other_stages(self):
        self.config['kinds'] = {'reply':False}
        self.advance()
        self.assertFalse(self.cma.creates)
        self.config['kinds']['reply'] = True
        with patch('managed.independent_review.contracts', side_effect=FileNotFoundError):
            self.advance()
        self.assertFalse(self.cma.creates)
        self.assertEqual(self.row()['data']['hosted_review_preparation']['phase'], 'preparation_failed')
        self.assertEqual(self.row()['state'], 'awaiting-review')

    def test_review_reserves_sandbox_until_cleanup_and_counts_usage(self):
        self.cma.status = 'in_progress'
        self.advance()
        row = self.row()
        self.assertTrue(live_reference(row, self.entry))
        self.assertEqual(placements([row], self.entry), 1)
        self.assertIn('agt_review', ledger_agents([row]))
        pool = SandboxPool(self.ledger, {'max_sandboxes': 1, 'agents_per_sandbox': 6}, reserve_review_slots=1)
        self.assertEqual(pool.admission_limit(self.entry, [row]), 6)
        self.assertEqual(pool.admission_limit(self.entry, []), 5)

    def test_stale_unsigned_delivery_gets_a_fresh_policy_review_without_local_loop(self):
        self.advance(); self.advance()
        original = self.row()['data']['review']
        with patch('managed.control.workflow', return_value='c' * 64):
            self.advance()
        row = self.row()
        self.assertEqual(row['state'], 'approved')
        self.assertEqual(row['data']['review']['binding'], original['binding'])
        self.assertEqual(row['data']['review']['rebind_workflow_hash'], 'c' * 64)
        self.assertEqual(len(self.cma.creates), 2)

    def test_stale_mail_with_a_receipt_is_not_admitted_for_review(self):
        self.advance(); self.advance()
        row = self.row()
        self.tasks.update(row['id'], row['revision'], 'awaiting-mailbox', {
            **row['data'], 'sent_receipt':{'verified':True}})
        with patch('managed.control.workflow', return_value='c' * 64):
            self.advance()
        self.assertEqual(len(self.cma.creates), 1)

    def test_notes_review_progresses_while_mail_review_is_occupied(self):
        self.config['independent_review'].update(max_active=2, max_active_per_group=1)
        self.config['sandbox_pool']['groups'] = [{'name':'mail','kinds':['reply']},
                                                {'name':'notes','kinds':['research']}]
        notes = {**self.entry, 'group':'notes', 'sandbox_id':'sbx_notes', 'workspace_id':'wks_notes'}
        self.tasks.db.execute('UPDATE pipeline_state SET payload=? WHERE key=?',
                              (canonical([self.entry, notes]), 'sandbox_pool'))
        for identity, kind, group in [('reply-2','reply','mail'), ('research-1','research','notes')]:
            req = request(identity); req['kind'] = kind
            row = self.tasks.add(req)
            self.tasks.update(identity, row['revision'], 'awaiting-review', {
                'agent_id':'author-' + identity, 'pool_group':group, 'cleanup':{'stopped':True},
                'result':{**self.result, 'task_id':identity}})
        self.cma.status = 'in_progress'
        self.advance()
        self.assertEqual(len(self.cma.creates), 2)
        self.assertNotIn('hosted_review', self.tasks.get('reply-2')['data'])
        self.assertEqual(self.tasks.get('research-1')['data']['hosted_review']['phase'], 'reviewing')
