import copy
import hashlib
import json
import tempfile
from managed import control


class Releases:
    def __init__(self):
        self.uploads=[]
    def preserve_task(self,path,tag):
        self.uploads.append((tag,path.name,path.read_bytes()))


def row(inputs):
    return {'id':'large','kind':'submission','request_hash':'unchanged',
            'request':{'id':'large','inputs':inputs},'data':{}}


def test_large_previous_result(tmp_path):
    task=row({'previous_result':{'report':'é'*300000},'review':{'verdict':'BOUNCE'}})
    original=copy.deepcopy(task['request'])
    releases=Releases()
    config={'github':{'service':'api-github-pat','user_service_id':'selected'}}
    prompt=control.prepare_task_prompt(task,releases,tmp_path,config)
    assert len(prompt)<120000
    assert task['request']==original and task['request_hash']=='unchanged'
    tag,name,payload=releases.uploads[0]
    meta=task['data']['prompt_offload']
    assert tag=='outreach-task-large' and name==meta['asset']
    assert meta['bytes']==len(payload)
    assert meta['sha256']==hashlib.sha256(payload).hexdigest()
    assert json.loads(payload)['fields']['request.inputs.previous_result']==original['inputs']['previous_result']
    for key in ('offloaded','release_tag','asset','sha256','bytes'):
        assert json.dumps(key) in prompt
    assert 'Verify its exact byte size and SHA-256' in prompt
    assert 'Releases API' in prompt and 'api-github-pat' in prompt
    assert control.prepare_task_prompt(task,releases,tmp_path,config)==prompt
    assert len(releases.uploads)==1


def test_small_unchanged(tmp_path):
    task=row({'previous_result':{'report':'short'}})
    original=copy.deepcopy(task)
    releases=Releases()
    assert control.prepare_task_prompt(task,releases,tmp_path,{})==control.task_prompt(task)
    assert task==original and releases.uploads==[]


def test_multiple_inputs(tmp_path):
    task=row({'previous_result':'x'*150000,'prior_context':'y'*150000})
    releases=Releases()
    assert len(control.prepare_task_prompt(task,releases,tmp_path,{}))<=120000
    assert len(releases.uploads)==1
    assert len(task['data']['prompt_offload']['fields'])==2


import unittest
from test_sandbox_pool import PoolFixture
from test_tasks import request


class AdmissionOffloadTests(PoolFixture, unittest.TestCase):
    def test_verified_offload_checkpointed_before_cma_admission(self):
        value=request('offload','owner-offload')
        value['inputs']['previous_result']={'report':'x'*300000}
        task=self.ledger.tasks.add(value)
        uploads=[]
        self.remote.preserve_task=lambda path,tag:uploads.append((tag,path.read_bytes()))
        def before_admit(agent,prompt,key):
            current=self.ledger.tasks.get(task['id'])
            self.assertTrue(current['data']['prompt_offload'])
            self.assertEqual(current['request'],value)
            self.assertEqual(len(uploads),1)
            self.assertLessEqual(len(prompt),120000)
        self.cma.before_admit=before_admit
        self.tick()
        self.assertEqual(len(self.cma.admissions),1)


class ReplyHistoryOffloadTests(unittest.TestCase):
    def test_mailbox_history_offload_accounts_for_sent_and_drafts(self):
        task = row({})
        task['kind'] = 'reply'
        history = {
            'latest_sent': {'author@example.invalid': {'salutation': 'Dear Professor A,',
                                                        'closing': 'Best regards,\n\nAvery'}},
            'drafts': [{'id': f'draft-{i}', 'body_excerpt': '新' * 200} for i in range(30)],
            'drafts_complete': True,
            'messages': [],
        }
        task['data']['mailbox_history'] = history
        original = control.task_prompt(task)
        limit = len(original) - 3000
        releases = Releases()
        with tempfile.TemporaryDirectory() as folder:
            prompt = control.prepare_task_prompt(task, releases, folder, {'prompt_max_characters': limit})
        self.assertLessEqual(len(prompt), limit)
        self.assertIs(task['data']['mailbox_history'], history)
        meta = task['data']['prompt_offload']
        self.assertEqual(meta['fields'], ['data.mailbox_history'])
        _, name, payload = releases.uploads[0]
        self.assertEqual(meta['asset'], name)
        self.assertEqual(meta['bytes'], len(payload))
        self.assertGreater(meta['bytes'], len(payload.decode()))
        self.assertEqual(meta['sha256'], hashlib.sha256(payload).hexdigest())
        self.assertEqual(json.loads(payload)['fields']['data.mailbox_history'], history)
        self.assertIn(name, prompt)
        self.assertNotIn('新' * 200, prompt)
