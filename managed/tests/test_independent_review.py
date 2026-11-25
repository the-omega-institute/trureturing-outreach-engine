"""Production review survives writer restarts without a local supervisor."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from managed import independent_review as review
from managed.queue import canonical, digest
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

    def request(self, path):
        return {'status': 'active'}

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
            'kinds': {'reply':True, 'research':True},
            'independent_review': {'enabled': True, 'max_active': 1,
                'profile': {'model':'gpt-6.2-sol'},
                'github': {'service':'github-review-read', 'user_service_id':'review-reader', 'read_only':True}},
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
        automation = {'schema_version':1, 'routing_contract':{}, 'mail':{'automatic':'exact signed PASS'}, 'continuation':{}}
        def read(path, *args):
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

    def test_completed_review_retires_its_agent_and_preserves_other_agents(self):
        from managed.cma import CMA as Adapter
        self.advance()
        run = self.row()['data']['hosted_review']
        self.assertEqual(run['phase'], 'completed')
        self.assertIn('agt_review', ledger_agents([self.row()]))
        calls = []
        agents = {
            'agt_review': {'id':'agt_review', 'status':'ready', 'sandbox':{'id':'sbx_mail'}},
            'agt_other': {'id':'agt_other', 'name':'human-debug', 'status':'working'},
        }

        def request(path, method='GET', body=None, key=None):
            calls.append((path, method, key))
            if path == '/api/v1/agents/agt_review/stop' and method == 'POST':
                agents['agt_review']['status'] = 'stopped'
                return dict(agents['agt_review'])
            if path == '/api/v1/agents/agt_review':
                if method == 'DELETE':
                    agents['agt_review']['status'] = 'deleted'
                return dict(agents['agt_review'])
            if path == '/api/v1/sandboxes/sbx_mail' and method == 'GET':
                return {'status':'active'}
            if path.startswith('/api/v1/agents?') and method == 'GET':
                return {'items':[dict(agent) for agent in agents.values()], 'next':None}
            raise AssertionError((path, method))

        adapter = Adapter(token='fake')
        adapter.request = request
        self.cma.retire = adapter.retire
        self.advance()
        completed = self.row()['data']['hosted_review']
        self.assertTrue(completed['cleanup_complete'])
        self.assertNotIn('foreign_agents', completed['cleanup'])
        self.assertNotIn('sandbox_retired', completed['cleanup'])
        # Engine #22: an idle (ready) Agent is deleted directly after a fresh read.
        self.assertEqual([(path, method, key) for path, method, key in calls if method != 'GET'], [
            ('/api/v1/agents/agt_review', 'DELETE', run['create_key'] + '-cleanup-delete'),
        ])
        self.assertEqual(agents['agt_other']['status'], 'working')
        before = len(calls)
        self.advance()
        self.assertEqual(len(calls), before)

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
        self.assertNotIn('hosted_review', self.tasks.get('reply-1')['data'])
        self.assertEqual(self.tasks.get('research-1')['data']['hosted_review']['phase'], 'reviewing')


    def test_last_reviewer_agent_never_retires_the_groups_only_sandbox(self):
        from managed.cma import CMA as Adapter
        self.advance()
        calls = []
        agent = {'id':'agt_review', 'status':'ready', 'sandbox':{'id':'sbx_mail'}}
        def request(path, method='GET', body=None, key=None):
            calls.append((path, method))
            if path == '/api/v1/agents/agt_review/stop':
                agent['status'] = 'stopped'
            elif path == '/api/v1/agents/agt_review' and method == 'DELETE':
                agent['status'] = 'deleted'
            elif path != '/api/v1/agents/agt_review':
                self.fail('Reviewer cleanup touched shared Sandbox inventory: ' + path)
            return dict(agent)
        adapter = Adapter(token='synthetic'); adapter.request = request; adapter.retire_wait = 0
        self.cma.retire = adapter.retire
        self.advance()
        self.assertTrue(self.row()['data']['hosted_review']['cleanup_complete'])
        self.assertEqual(json.loads(self.tasks.db.execute(
            "SELECT payload FROM pipeline_state WHERE key='sandbox_pool'").fetchone()[0]), [self.entry])
        self.assertEqual([method for _, method in calls if method != 'GET'], ['DELETE'])

    def test_accepted_reviewer_delete_is_observed_not_resent(self):
        from managed.cma import CMA as Adapter, AGENT_CLEANUP_WAIT_SECONDS
        self.advance()
        calls = []
        agent = {'id': 'agt_review', 'status': 'ready', 'sandbox': {'id': 'sbx_mail'}}
        def request(path, method='GET', body=None, key=None):
            calls.append((path, method))
            if path == '/api/v1/agents/agt_review' and method == 'DELETE':
                return {'status': 'accepted'}
            if path != '/api/v1/agents/agt_review':
                self.fail('Reviewer cleanup touched shared Sandbox inventory: ' + path)
            return dict(agent)
        adapter = Adapter(token='synthetic'); adapter.request = request; adapter.retire_wait = 0
        self.cma.retire = adapter.retire
        for _ in range(3):
            self.advance()
        self.assertEqual(sum(method == 'DELETE' for _, method in calls), 1)
        cleanup = self.row()['data']['hosted_review']['cleanup']
        self.assertEqual(cleanup.get('delete_outcome'), 'accepted')
        started = cleanup['cleanup_started_at']
        self.tasks.clock = lambda: started + AGENT_CLEANUP_WAIT_SECONDS + 1
        self.advance()
        self.assertIn('operator_check', self.row()['data']['hosted_review']['cleanup'])
        self.assertEqual(sum(method == 'DELETE' for _, method in calls), 1)

    def test_delivered_and_immutable_intent_rows_keep_their_signed_review(self):
        self.advance(); self.advance()
        base = self.row()
        cases = [('verified', {}), ('handoff-ready', {'publication_receipt':{'id':'pub'}}),
                 ('handoff-ready', {'publication_loop':{'phase':'queued', 'request':'publish-immutable.json'}}),
                 ('awaiting-review', {'sent_receipt':{'verified':True}}),
                 ('handoff-ready', {'mathesis_delivery':{'phase':'ready','envelope':{'exact':'PASS'}}}),
                 ('submission-ready', {'submission_receipt':{'id':'sent'}}),
                 ('owner-draft', {'draft_receipt':{'remote_id':'saved'}}),
                 ('owner-draft', {'draft_intent':{'key':'uncertain'}}),
                 ('approved', {'sent_receipt':{'verified':True}}),
                 ('awaiting-mailbox', {'delivery_attempt':{'key':'uncertain'}})]
        with patch('managed.control.workflow', return_value='c'*64):
            for state, effects in cases:
                row = self.row()
                self.tasks.update(row['id'], row['revision'], state, {**base['data'], **effects})
                self.advance()
                self.assertEqual(self.row()['data']['review'], base['data']['review'])
                self.assertIsNone(review.candidate_binding(self.row(), self.tasks))
        self.assertEqual(len(self.cma.creates), 1)

    def test_queue_attempt_prevents_stale_review_even_without_task_receipt(self):
        self.advance(); self.advance()
        self.tasks.db.execute('CREATE TABLE attempts(item TEXT)')
        self.tasks.db.execute('INSERT INTO attempts VALUES(?)', ('reply-1',))
        with patch('managed.control.workflow', return_value='c'*64):
            self.advance()
        self.assertEqual(len(self.cma.creates), 1)

    def test_fresh_mail_precedes_historical_policy_rebinds(self):
        self.advance(); self.advance()
        req = request('new-mail')
        row = self.tasks.add(req)
        self.tasks.update(row['id'], row['revision'], 'awaiting-review', {
            'result':{**self.result, 'task_id':row['id']}, 'pool_group':'mail'})
        with patch('managed.control.workflow', return_value='c'*64):
            self.advance()
        self.assertEqual(self.tasks.get('new-mail')['state'], 'approved')
        self.assertEqual(len(self.cma.creates), 2)
        self.assertNotIn('rebind_workflow_hash', self.row()['data']['review'])

    def test_complete_contract_snapshot_contains_every_workflow_input(self):
        from managed import control
        self.policy.stop()
        root = Path(control.__file__).parent
        def read(path, *args):
            return (root / path).read_text()
        with patch.object(control, 'workflow_text', side_effect=read), \
             patch('managed.policy.policy_text', return_value='synthetic owner policy'):
            for kind in ('reply','firstcontact','daily','research','shortnote','mathesis','submission'):
                rules = review.contracts(kind)
                for path, content in control.workflow_inputs(kind).items():
                    self.assertEqual(rules[path], content)
            self.assertIn('replies/voice.md', review.contracts('reply'))
            self.assertIn('replies/voice.md', review.contracts('firstcontact'))
        self.policy.start()

    def test_contract_and_identity_edits_cancel_inflight_reviews(self):
        for changed in ('AGENTS.md', 'WORKSPACE.md', 'identity'):
            with self.subTest(changed=changed):
                self.cma.status = 'in_progress'
                self.advance()
                if changed == 'identity':
                    self.config['identity'] = {'signature':'Changed owner identity'}
                    self.advance()
                else:
                    with patch('managed.independent_review.contracts', return_value={changed:'Edited owner policy'}):
                        self.advance()
                self.assertEqual(self.row()['data']['hosted_review']['phase'], 'obsolete')
                self.assertNotIn('review', self.row()['data'])
                self.advance()  # retire the obsolete Agent
                # Start the next subcase with the same exact task result.
                row = self.row(); data = dict(row['data']); data.pop('hosted_review')
                self.tasks.update(row['id'], row['revision'], row['state'], data)

    def test_edit_during_response_read_cannot_be_signed_under_new_policy(self):
        response = self.cma.response
        def change(identity):
            answer = response(identity)
            self.config['identity'] = {'signature':'Changed during response read'}
            return answer
        self.cma.response = change
        self.advance()
        self.assertEqual(self.row()['data']['hosted_review']['phase'], 'obsolete')
        self.assertNotIn('review', self.row()['data'])

    def test_pause_and_disable_continue_observing_and_retiring(self):
        for mode in ('pause', 'kind', 'activation', 'automation'):
            with self.subTest(mode=mode):
                self.cma.status = 'in_progress'; self.advance()
                if mode == 'pause': self.config['paused'] = True
                elif mode == 'kind': self.config['kinds']['reply'] = False
                elif mode == 'activation': self.config['independent_review']['enabled'] = False
                else: self.config['automation']['enabled'] = False
                self.cma.status = 'completed'
                self.advance(); self.advance()
                self.assertTrue(self.row()['data']['hosted_review']['cleanup_complete'])
                self.assertFalse(live_reference(self.row(), self.entry))
                self.config['paused'] = False; self.config['kinds']['reply'] = True
                self.config['independent_review']['enabled'] = True
                self.config['automation']['enabled'] = True
                row = self.row(); data = dict(row['data']); data.pop('hosted_review'); data.pop('review')
                self.tasks.update(row['id'], row['revision'], 'awaiting-review', data)

    def test_paused_provisioning_resolves_and_retires_without_admitting(self):
        self.cma.operation = lambda identity: {'status':'pending'}
        self.advance()
        self.config['paused'] = True
        self.cma.operation = lambda identity: {'status':'succeeded', 'agent_id':'agt_review'}
        self.advance()
        self.assertTrue(self.row()['data']['hosted_review']['cleanup_complete'])
        self.assertFalse(self.cma.commands)

    def test_transient_errors_back_off_and_reuse_original_intent(self):
        from managed.cma import CMARequestError
        for error in (CMARequestError(409, 'sandbox_lifecycle_busy'), CMARequestError(503, 'gateway'), TimeoutError()):
            with self.subTest(error=type(error).__name__, code=getattr(error, 'code', None)):
                with patch.object(self.cma, 'create', side_effect=error) as create:
                    self.advance()
                    run = self.row()['data']['hosted_review']
                    self.assertEqual(run['phase'], 'provisioning')
                    self.assertGreater(run['next_retry_at'], self.tasks.clock())
                    self.advance()
                    self.assertEqual(create.call_count, 1)
                original = run['create_key']; run['next_retry_at'] = 0
                review.write(self.ledger, 'reply-1', run)
                self.advance(); self.advance()
                self.assertEqual(self.row()['state'], 'approved')
                self.assertIn(original, self.cma.creates)
                row = self.row(); data = dict(row['data']); data.pop('hosted_review'); data.pop('review')
                # Reset nonce gives independent synthetic cases distinct provider handles.
                data['hosted_review_reset'] = original
                self.tasks.update(row['id'], row['revision'], 'awaiting-review', data)

    def test_nonactive_saved_sandbox_is_never_used_for_placement(self):
        for status in ('paused','retiring','credential_rotating','failed'):
            self.cma.request = lambda path: {'status':status}
            self.advance()
        self.assertFalse(self.cma.creates)
        self.assertNotIn('hosted_review', self.row()['data'])

    def test_idle_pool_retains_last_sandbox_for_pending_review(self):
        self.tasks.clock = lambda: 10000
        config = {'max_sandboxes':1, 'agents_per_sandbox':6,
                  'groups':[{'name':'mail','kinds':['reply'],'max_sandboxes':1}]}
        pool = SandboxPool(self.ledger, config, self.config['kinds'] | {'research':False}, reserve_review_slots=1)
        entry = pool.entries[0]; entry['idle_since'] = 0
        self.assertFalse(pool.rotate_empty(entry, [self.row()], 10000))
        self.assertEqual(len(pool.entries), 1)

    def test_missing_kind_is_default_off(self):
        self.config['kinds'] = {}
        self.advance()
        self.assertFalse(self.cma.creates)

    def test_reviewer_access_is_explicit_read_only_and_does_not_inherit_author_services(self):
        self.config['agent_template']['profile']['permissions'] = {'services':['api-github-pat','oracle','mail']}
        self.config['github'] = {'service':'api-github-pat', 'user_service_id':'writer'}
        self.config['independent_review']['profile']['permissions'] = {'services':['mail','oracle']}
        self.advance()
        run = self.row()['data']['hosted_review']
        self.assertEqual(run['create_body']['profile']['permissions'], {'services':['github-review-read']})
        self.assertNotIn('writer', run['prompt'])
        self.assertNotIn('2026-11-23', run['prompt'])
        self.assertEqual(run['reviewer_model'], 'gpt-6.2-sol')
        evidence = json.loads(next(raw for (_, name), raw in self.ledger.releases.files.items()
                                  if name.startswith('independent-review-')))
        self.assertEqual(evidence['reviewer'], review.REVIEWER)
        self.assertEqual(evidence['reviewer_model'], 'gpt-6.2-sol')

    def test_missing_read_only_connection_parks_preparation(self):
        self.config['independent_review'].pop('github')
        self.advance()
        self.assertFalse(self.cma.creates)
        self.assertEqual(self.row()['data']['hosted_review_preparation']['phase'], 'preparation_failed')

    def test_one_bad_row_does_not_prevent_another_review(self):
        req = request('new-bad-row'); req['kind'] = 'reply'
        row = self.tasks.add(req)
        self.tasks.update(row['id'], row['revision'], 'awaiting-review', {'pool_group':'mail'})
        self.advance()
        self.assertEqual(self.tasks.get('new-bad-row')['data']['hosted_review_preparation']['phase'], 'preparation_failed')
        self.assertEqual(self.row()['state'], 'approved')

    def test_scan_checks_budget_before_binding_and_reads_ledger_once(self):
        from managed.tick import Tick
        now = [0]
        tick = Tick(1, clock=lambda: now[0]); tick.bind(self.ledger)
        binding = review.candidate_binding
        def slow(*args, **kwargs):
            now[0] += 2
            return binding(*args, **kwargs)
        for i in range(20):
            req = request('queued-' + str(i)); self.tasks.add(req)
        with patch.object(self.tasks, 'all', wraps=self.tasks.all) as all_rows, \
             patch('managed.independent_review.candidate_binding', side_effect=slow) as scan:
            self.advance()
        self.assertEqual(all_rows.call_count, 1)  # No provider call after the exhausted scan.
        self.assertLessEqual(scan.call_count, 1)

    def test_operator_reset_is_signed_exact_replayable_and_uses_new_handles(self):
        from managed import control
        from managed.cma import CMARequestError
        with patch.object(self.cma, 'create', side_effect=CMARequestError(400, 'bad profile')):
            self.advance()
        old = self.row()['data']['hosted_review']
        self.assertEqual(old['phase'], 'needs_attention')
        self.advance()
        self.assertFalse(self.cma.creates)
        value = {'schema_version':1, 'task_id':'reply-1', 'reason':'Operator corrected profile',
                 **review.reset_binding(self.tasks, 'reply-1')}
        import hashlib, hmac
        def signed(value):
            return {**value, 'signature':hmac.new(b'signing-key', canonical(value).encode(), hashlib.sha256).hexdigest()}
        for mutation in ({'review_digest':'0'*64}, {'row_revision':value['row_revision']+1}, {'row_revision':True}):
            with self.assertRaises(ValueError): review.reset(self.ledger, signed({**value, **mutation}), 'signing-key')
        with self.assertRaisesRegex(ValueError, 'signature'):
            review.reset(self.ledger, {**signed(value), 'reason':'forged'}, 'signing-key')
        review.reset(self.ledger, signed(value), 'signing-key')
        review.reset(self.ledger, signed(value), 'signing-key')
        self.assertEqual(self.row()['data']['hosted_review_history'], [old])
        self.advance()
        self.assertEqual(self.row()['state'], 'approved')
        self.assertNotEqual(self.row()['data']['hosted_review']['create_key'], old['create_key'])


    def test_forged_binding_or_policy_hash_is_parked_without_a_signature(self):
        for field, value in [('binding', {'task_id':'another-task'}), ('current_workflow_hash', 'f'*64)]:
            with self.subTest(field=field):
                self.cma.change_verdict = lambda v: {**v, field:value}
                self.advance(); self.advance()
                self.assertEqual(self.row()['data']['hosted_review']['phase'], 'needs_attention')
                self.assertNotIn('review', self.row()['data'])
                row = self.row(); data = dict(row['data']); data['hosted_review_reset'] = field
                data.pop('hosted_review')
                self.tasks.update(row['id'], row['revision'], row['state'], data)

    def test_cleanup_can_finish_without_coordinator_signing_key(self):
        self.advance()
        self.config['paused'] = True
        with patch.dict('os.environ', {'OUTREACH_REVIEW_KEY':''}):
            review.advance(self.ledger, self.cma, self.config, self.root)
        self.assertTrue(self.row()['data']['hosted_review']['cleanup_complete'])

    def test_reset_cli_stages_exact_signed_intent_and_coordinator_ingests_it(self):
        from managed import control
        from managed.cma import CMARequestError
        with patch.object(self.cma, 'create', side_effect=CMARequestError(400, 'bad profile')):
            self.advance()
        binding = review.reset_binding(self.tasks, 'reply-1')
        staged = []
        with patch.object(control, 'load_config', return_value=self.config), \
             patch.object(control, 'Releases', return_value=self.ledger.releases), \
             patch.object(control, 'Ledger', return_value=self.ledger), \
             patch.object(control, 'review_key', return_value='signing-key'), \
             patch.object(control, 'stage', side_effect=lambda value, prefix, *args:staged.append((prefix,value)) or {}), \
             patch.object(self.tasks, 'close'):
            control.main(['independent-review-reset','--task','reply-1','--reason','Corrected profile'])
        prefix, envelope = staged[0]
        self.assertEqual(prefix, 'independent-review-reset')
        self.assertEqual({key:envelope[key] for key in binding}, binding)
        review.reset(self.ledger, envelope, 'signing-key')
        self.advance()
        self.assertEqual(self.row()['state'], 'approved')

    def test_doctor_identifies_configured_reviewer_and_model(self):
        from managed import control
        for activated in (False, True):
            self.config['independent_review']['enabled'] = activated
            target = self.root / 'doctor.json'
            from unittest.mock import Mock
            remote = Mock(); remote.release.return_value = None
            with patch.object(control, 'load_config', return_value=self.config), \
                 patch.object(control, 'Releases', return_value=remote), \
                 patch.object(control, 'CMA') as adapter, \
                 patch.object(control, 'review_key', return_value='signing-key'):
                adapter.return_value.discover.return_value = {}
                control.main(['--output', str(target), 'doctor'])
            report = json.loads(target.read_text())['reviewer']
            self.assertEqual(report['identity'], review.REVIEWER if activated else 'local-claude-loop')
            self.assertEqual(report['model'], 'gpt-6.2-sol' if activated else None)

    def test_negative_stale_rebind_parks_for_operator_and_preserves_delivery_gate(self):
        from managed.tasks import review_current
        self.advance(); self.advance()
        self.cma.change_verdict = lambda v: {**v, 'verdict':'BOUNCE', 'reason':'New policy requires a substantive correction'}
        with patch('managed.control.workflow', return_value='c'*64):
            self.advance(); self.advance()
            self.assertFalse(review_current(self.row(), 'c'*64))
        self.assertEqual(self.row()['data']['hosted_review']['phase'], 'needs_attention')
        self.assertEqual(self.row()['data']['hosted_review']['last_error'], 'Review rejected by task gates')


    def test_reader_cannot_select_the_coordinator_service_or_connection(self):
        self.config['github'] = {'service':'api-github-pat', 'user_service_id':'writer'}
        for change in ({'service':'api-github-pat'}, {'user_service_id':'writer'}, {'read_only':False}):
            options = copy.deepcopy(self.config)
            options['independent_review']['github'].update(change)
            with self.subTest(change=change), self.assertRaises(ValueError):
                review.reviewer_access(options)
        self.config['independent_review']['profile'].pop('model')
        with self.assertRaisesRegex(ValueError, 'explicit.*model'):
            review.reviewer_access(self.config)

    def test_transient_backoff_has_a_one_hour_ceiling(self):
        self.cma.status = 'in_progress'; self.advance()
        run = self.row()['data']['hosted_review']
        run.update(attempts=100, next_retry_at=0)
        review.write(self.ledger, 'reply-1', run)
        now = 10000; self.tasks.clock = lambda: now
        with patch.object(self.cma, 'response', side_effect=TimeoutError):
            self.advance()
        self.assertEqual(self.row()['data']['hosted_review']['next_retry_at'], now + 3600)
        self.assertEqual(self.row()['data']['hosted_review']['phase'], 'reviewing')


    def test_idle_retention_distinguishes_stale_review_from_completed_handoff(self):
        self.advance(); self.advance()
        config = {'max_sandboxes':1, 'agents_per_sandbox':6,
                  'groups':[{'name':'mail','kinds':['reply'],'max_sandboxes':1}]}
        pool = SandboxPool(self.ledger, config, {'reply':True}, reserve_review_slots=1)
        entry = pool.entries[0]
        self.assertFalse(pool.awaited(entry, [self.row()]))
        with patch('managed.control.workflow', return_value='c'*64):
            self.assertTrue(pool.awaited(entry, [self.row()]))
        row = self.row()
        self.tasks.update(row['id'], row['revision'], 'handoff-ready', {
            **row['data'], 'publication_loop':{'phase':'queued','request':'immutable-publication.json'}})
        with patch('managed.control.workflow', return_value='c'*64):
            self.assertFalse(pool.awaited(entry, [self.row()]))
