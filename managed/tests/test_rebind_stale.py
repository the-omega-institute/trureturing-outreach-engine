import copy
import hashlib
import hmac
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from managed import control
from managed.queue import canonical, digest


CHANGED = 'Mail workflow changed; re-review the current version'


def row(task_id, state='awaiting-mailbox', kind='reply', reason=CHANGED):
    result = {'task_id':task_id, 'report':'Exact reviewed result', 'packet':None}
    binding = {'task_id':task_id, 'task_revision':3, 'workflow_hash':'b'*64,
               'result_hash':digest(result)}
    return {'id':task_id, 'kind':kind, 'state':state, 'reason':reason, 'result':result,
            'request':{'id':task_id, 'kind':kind, 'workflow_hash':'b'*64},
            'review':{'binding':binding, 'reviewer':'local-claude-loop', 'verdict':'PASS',
                      'reason':'Original review', 'evidence_ref':'private/exact-review.json'}}


class RebindStaleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)/'control'
        self.root.mkdir()
        owner = row('owner', 'owner-draft', reason=None)
        # work() supplies a fresh owner binding at the current PASS event.
        owner_binding = {**owner['review']['binding'], 'task_revision':5}
        changed = row('changed')
        changed['result']['report'] = 'Edited after PASS'
        bounce = row('bounce')
        bounce['review']['verdict'] = 'BOUNCE'
        hold = row('hold')
        hold['review']['verdict'] = 'HOLD'
        current = row('current')
        current['review']['rebind_workflow_hash'] = 'c'*64
        sent = row('sent')
        sent['sent_receipt'] = {'id':'already-sent'}
        self.snapshot = {'affairs_tasks':[
            owner, row('approved', 'approved'), row('first', kind='firstcontact'),
            row('owner-error', 'owner-draft'), changed, bounce, hold, current, sent,
            row('other-error', reason='Mailbox unavailable'), row('held', 'held'),
            row('science', kind='research')],
            'owner_draft_rebind':[{'binding':owner_binding, 'request':owner['request'],
                                    'result':owner['result'], 'rebind_workflow_hash':'c'*64}]}

    def test_cli_signs_selected_rows_and_preserves_evidence_and_binding(self):
        work = self.root/'work.json'
        work.write_text(json.dumps(self.snapshot))
        output = self.root/'summary.json'
        before = copy.deepcopy(self.snapshot)
        with patch.object(control, 'ROOT', self.root), \
                patch.object(control, 'workflow', return_value='c'*64), \
                patch.object(control, 'review_key', return_value='secret'), \
                patch.object(control, 'submit_review', return_value={'asset':'review.json'}) as submit:
            # Keep the actual config read in main(), while placing outputs in a temp workspace.
            (self.root/'managed').mkdir()
            (self.root/'config.json').write_bytes((Path(control.__file__).parent/'config.example.json').read_bytes())
            control.main(['--output',str(output),'rebind-stale','--work',str(work),
                          '--reason','Checked against the current mail workflow'])
        summary = json.loads(output.read_text())
        self.assertEqual([item['task_id'] for item in summary['signed']],
                         ['owner','approved','first','owner-error'])
        self.assertEqual({item['task_id'] for item in summary['skipped']},
                         {'changed','bounce','hold','current','sent'})
        self.assertEqual(submit.call_count, 4)
        for item, call in zip(summary['signed'], submit.call_args_list):
            path = Path(item['path'])
            self.assertEqual(path.parent.parent, self.root.parent/'mail/outreach-maintenance/reviews')
            self.assertTrue(path.parent.name.startswith('rebind-'))
            envelope = json.loads(path.read_text())
            self.assertEqual(envelope, call.args[0])
            self.assertEqual(envelope['rebind_workflow_hash'], 'c'*64)
            self.assertEqual(envelope['evidence_ref'], 'private/exact-review.json')
            self.assertEqual(envelope['reason'], 'Checked against the current mail workflow')
            payload = {k:v for k,v in envelope.items() if k != 'signature'}
            self.assertEqual(envelope['signature'], hmac.new(b'secret', canonical(payload).encode(), hashlib.sha256).hexdigest())
        self.assertEqual(submit.call_args_list[0].args[0]['binding'],
                         self.snapshot['owner_draft_rebind'][0]['binding'])
        self.assertEqual(self.snapshot, before)

    def test_coordinator_rejection_is_skipped_and_others_continue(self):
        def submit(envelope, releases, folder):
            if envelope['binding']['task_id'] == 'approved':
                raise ValueError('Review would be rejected by the coordinator: stale draft conflict')
            return {'asset':'review.json'}
        with patch.object(control, 'ROOT', self.root), \
                patch.object(control, 'workflow', return_value='c'*64), \
                patch.object(control, 'review_key', return_value='secret'), \
                patch.object(control, 'submit_review', side_effect=submit):
            summary = control.rebind_stale(self.snapshot, 'Rechecked', object(), self.root)
        self.assertEqual([item['task_id'] for item in summary['signed']], ['owner','first','owner-error'])
        rejected = [item for item in summary['skipped'] if item['task_id'] == 'approved']
        self.assertEqual(len(rejected), 1)
        self.assertIn('stale draft conflict', rejected[0]['reason'])

    def test_dry_run_lists_candidates_without_key_files_or_submission(self):
        with patch.object(control, 'ROOT', self.root), \
                patch.object(control, 'workflow', return_value='c'*64), \
                patch.object(control, 'review_key') as key, \
                patch.object(control, 'sign_review') as sign, \
                patch.object(control, 'submit_review') as submit:
            summary = control.rebind_stale(self.snapshot, 'Rechecked', object(), self.root, dry_run=True)
        self.assertEqual(summary['signed'], [])
        self.assertEqual([item['binding']['task_id'] for item in summary['planned']],
                         ['owner','approved','first','owner-error'])
        key.assert_not_called()
        sign.assert_not_called()
        submit.assert_not_called()
        self.assertFalse((self.root.parent/'mail').exists())

    def test_owner_entries_require_stored_pass_and_unchanged_result(self):
        for change in ('missing-row','bounce','result','binding'):
            with self.subTest(change=change):
                snapshot = copy.deepcopy(self.snapshot)
                snapshot['affairs_tasks'] = snapshot['affairs_tasks'][:1]
                owner = snapshot['affairs_tasks'][0]
                if change == 'missing-row':snapshot['affairs_tasks'] = []
                elif change == 'bounce':owner['review']['verdict'] = 'BOUNCE'
                elif change == 'result':owner['result']['report'] = 'Changed'
                else:snapshot['owner_draft_rebind'][0]['binding']['result_hash'] = '0'*64
                with patch.object(control, 'submit_review') as submit:
                    summary = control.rebind_stale(snapshot, 'Rechecked', object(), self.root, dry_run=True)
                self.assertEqual(summary['planned'], [])
                self.assertEqual([item['task_id'] for item in summary['skipped']], ['owner'])
                submit.assert_not_called()

    def test_owner_also_in_mail_errors_uses_fresh_binding_once(self):
        snapshot = copy.deepcopy(self.snapshot)
        snapshot['affairs_tasks'][0]['reason'] = CHANGED
        with patch.object(control, 'workflow', return_value='c'*64):
            summary = control.rebind_stale(snapshot, 'Rechecked', object(), self.root, dry_run=True)
        owners = [item for item in summary['planned'] if item['binding']['task_id'] == 'owner']
        self.assertEqual(len(owners), 1)
        self.assertEqual(owners[0]['binding'], snapshot['owner_draft_rebind'][0]['binding'])

    def test_submit_uses_the_review_commands_preflight_and_stage(self):
        envelope, releases, folder = {}, object(), self.root
        with patch.object(control, 'review_dry_run') as preflight, \
                patch.object(control, 'stage', return_value={'asset':'review.json'}) as stage:
            self.assertEqual(control.submit_review(envelope, releases, folder), {'asset':'review.json'})
        preflight.assert_called_once_with(envelope, releases)
        stage.assert_called_once_with(envelope, 'review', releases, folder)

    def test_mail_inventory_uses_pass_event_binding_and_skips_rejected_row(self):
        snapshot=copy.deepcopy(self.snapshot)
        mail=row('mail',reason=None)
        fresh={**mail['review']['binding'],'task_revision':5}
        snapshot['affairs_tasks'].append(mail)
        snapshot['mail_rebind']=[{'task_id':'mail','owner':'contact-a','kind':'reply',
                                 'reason':CHANGED,'binding':fresh,'rebind_workflow_hash':'c'*64}]
        with patch.object(control,'ROOT',self.root), \
                patch.object(control,'workflow',return_value='c'*64), \
                patch.object(control,'review_key',return_value='secret'), \
                patch.object(control,'submit_review') as submit:
            planned=control.rebind_stale(snapshot,'Rechecked',object(),self.root,dry_run=True)
            self.assertEqual(next(item for item in planned['planned']
                                  if item['binding']['task_id']=='mail')['binding'],fresh)
            def stage(envelope,releases,folder):
                if envelope['binding']['task_id']=='mail':
                    raise ValueError('Review would be rejected by the coordinator: delivery attempt')
                return {'asset':'review.json'}
            submit.side_effect=stage
            summary=control.rebind_stale(snapshot,'Rechecked',object(),self.root)
        self.assertIn({'task_id':'mail',
                       'reason':'Review would be rejected by the coordinator: delivery attempt'},summary['skipped'])
        self.assertEqual([item['task_id'] for item in summary['signed']],
                         ['owner','approved','first','owner-error'])


if __name__ == '__main__':
    unittest.main()
