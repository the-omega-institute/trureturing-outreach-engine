"""Offline regressions for cleanup isolation and exact operator acknowledgements."""
import copy
import hashlib
import hmac
import unittest
from unittest.mock import patch

from fixture_config import use_cli_config
from managed import control, recheck
from managed.cma import CMARequestError
from managed.queue import canonical
from managed.sandbox_pool import SandboxPool
from test_sandbox_pool import PoolFixture


SECRET='synthetic-cleanup-key'


def signed(value):
    value=dict(value)
    value['signature']=hmac.new(SECRET.encode(),canonical(value).encode(),hashlib.sha256).hexdigest()
    return value


class CleanupRecoveryTests(PoolFixture,unittest.TestCase):
    def seed_cleanup(self,entries):
        self.ledger.tasks.db.execute('INSERT OR REPLACE INTO pipeline_state VALUES(?,?)',
                                    ('sandbox_pool_cleanup',canonical(entries)))

    def failed_entry(self,sid='sbx_failed',**changes):
        return {'sandbox_id':sid,'workspace_id':'wks_failed','state':'failed','agents_placed':1,
                'cleanup':{'sandbox_id':sid,'sandbox_retired':False},'cleanup_checked_at':0,**changes}

    def finished(self,name='finished',state='awaiting-review',**changes):
        row=self.queue(name)
        return self.ledger.tasks.update(row['id'],row['revision'],state,{
            'agent_id':'agt_'+name,'response_id':'resp_'+name,
            'result':{'outcome':'completed','artifacts':[]},**changes})

    def task_clear(self,task_id,**changes):
        return signed({'schema_version':1,'task_id':task_id,'reason':'reconciled',
                       **control.cleanup_check_binding(self.ledger.tasks,task_id=task_id),**changes})

    def pool_clear(self,sandbox_id,**changes):
        return signed({'schema_version':1,'sandbox_id':sandbox_id,'reason':'reconciled',
                       **control.cleanup_check_binding(self.ledger.tasks,sandbox_id=sandbox_id),**changes})

    def test_first_processed_cleanup_get_failure_is_isolated_with_earlier_skips(self):
        for skipped in (False,True):
            for exc in (TimeoutError('unavailable'),CMARequestError(404,{'status':404})):
                with self.subTest(skipped=skipped,exc=type(exc).__name__):
                    entries=[self.failed_entry()]
                    if skipped:
                        entries.insert(0,self.failed_entry('sbx_skipped',
                            operator_check={'kind':'manual'},cleanup_checked_at=self.now))
                    self.seed_cleanup(entries)
                    self.cma.statuses['sbx_failed']=exc
                    pool=SandboxPool(self.ledger,self.cfg['sandbox_pool'],self.cfg['kinds'],paused=True)
                    pool.health(self.cma,rows=[])
                    self.assertEqual([e['sandbox_id'] for e in pool.errors],['sbx_failed'])
                    saved=next(e for e in self.state('sandbox_pool_cleanup') if e['sandbox_id']=='sbx_failed')
                    self.assertIn('cleanup_error',saved)
                    self.assertNotIn('sandbox_delete_requested',saved['cleanup'])
                    self.assertFalse(pool.available)
                    self.assertEqual(self.state(),[])

    def test_previous_retiring_status_cannot_mark_failed_get_as_delete_requested(self):
        self.seed_cleanup([self.failed_entry('sbx_previous'),self.failed_entry()])
        self.cma.statuses['sbx_previous']={'status':'retiring'}
        self.cma.statuses['sbx_failed']=TimeoutError('unavailable')
        pool=SandboxPool(self.ledger,self.cfg['sandbox_pool'],self.cfg['kinds'],paused=True)
        pool.health(self.cma,rows=[])
        failed=next(e for e in self.state('sandbox_pool_cleanup') if e['sandbox_id']=='sbx_failed')
        self.assertNotIn('sandbox_delete_requested',failed['cleanup'])
        self.assertEqual([e['sandbox_id'] for e in pool.errors],['sbx_failed'])

    def test_lasting_cleanup_404_does_not_abort_advance_or_approved_routing(self):
        self.seed_cleanup([self.failed_entry()])
        self.cma.statuses['sbx_failed']=CMARequestError(404,{'status':404})
        row=self.finished(state='approved')
        self.tick();self.tick()
        self.assertNotEqual(self.ledger.tasks.get(row['id'])['state'],'approved')
        self.assertEqual(len(self.state('sandbox_pool_errors')),1)

    def test_cleanup_transport_exhaustion_does_not_block_approved_routing(self):
        blocked=self.finished('blocked',reason='original result reason')
        control_row=self.finished('control')
        calls=[];original=self.cma.retire
        def retire(agent_id,*args):
            if agent_id=='agt_blocked':
                calls.append(args);raise TimeoutError('lost transport')
            return original(agent_id,*args)
        self.cma.retire=retire
        for _ in range(4):
            self.tick();self.now+=4000
        row=self.ledger.tasks.get(blocked['id'])
        self.assertEqual(len(calls),3)
        self.assertEqual(len(set(calls)),1,'transport retries retain their original lifecycle key')
        self.assertEqual({k:v for k,v in row['data'].items() if k!='cleanup'},blocked['data'])
        self.assertEqual(row['data']['cleanup']['transport_retries'],3)
        self.assertIn('operator_check',row['data']['cleanup'])
        for original_row in (blocked,control_row):
            row=self.ledger.tasks.get(original_row['id'])
            self.ledger.tasks.update(row['id'],row['revision'],'approved',row['data'])
        self.tick()
        self.assertEqual(self.ledger.tasks.get(blocked['id'])['state'],
                         self.ledger.tasks.get(control_row['id'])['state'])
        self.assertNotEqual(self.ledger.tasks.get(blocked['id'])['state'],'approved')

    def test_cleanup_backoff_and_success_are_independent_of_pause_and_kind(self):
        for disabled in ('paused','kind'):
            with self.subTest(disabled=disabled):
                row=self.finished(disabled,reason='original reason',
                    recovery={'scope':'execution','status':'recovered'},error_class='request-too-large')
                if disabled=='paused':self.cfg['paused']=True
                else:self.cfg['paused']=False;self.cfg['kinds']['research']=False
                calls=[]
                def retire(*args):
                    calls.append(args)
                    if len(calls)==1:raise TimeoutError('lost transport')
                    return {'deleted':True}
                self.cma.retire=retire
                self.tick()
                failed=self.ledger.tasks.get(row['id'])
                self.assertEqual(failed['data']['cleanup']['transport_retries'],1)
                retry_at=failed['data']['cleanup']['next_at']
                self.restore();self.tick()
                self.assertEqual(len(calls),1)
                self.now=retry_at;self.tick()
                recovered=self.ledger.tasks.get(row['id'])
                self.assertEqual(len(calls),2)
                self.assertEqual(calls[0],calls[1])
                self.assertEqual({k:v for k,v in recovered['data'].items() if k!='cleanup'},row['data'])
                self.assertEqual(recovered['data']['cleanup'],{'deleted':True,'cleanup_attempts':0})

    def test_cleanup_rate_limit_uses_its_own_budget(self):
        row=self.finished(reason='original reason',rate_limit_retries=4)
        self.cfg['recovery']={'rate_limit_max_attempts':1}
        def retire(*args):raise CMARequestError(429,{'code':'rate_limit_exceeded'})
        self.cma.retire=retire
        self.tick();self.now+=4000;self.tick()
        failed=self.ledger.tasks.get(row['id'])
        self.assertEqual({k:v for k,v in failed['data'].items() if k!='cleanup'},row['data'])
        self.assertEqual(failed['data']['cleanup']['transport_retries'],2)
        self.assertIn('operator_check',failed['data']['cleanup'])

    def test_cleanup_exhaustion_preserves_failed_task_requeue_classification(self):
        row=self.finished(state='failed',reason='prompt too large',error_class='request-too-large')
        def retire(*args):raise TimeoutError('lost transport')
        self.cma.retire=retire
        for _ in range(3):self.tick();self.now+=4000
        current=self.ledger.tasks.get(row['id'])
        self.assertEqual({k:v for k,v in current['data'].items() if k!='cleanup'},row['data'])
        control.requeue_large_task(self.ledger,{'task_id':row['id'],'request_hash':row['request_hash']})
        self.assertEqual(self.ledger.tasks.get(row['id'])['state'],'queued')

    def test_task_clear_resets_budgets_and_timer_then_retirement_resumes(self):
        row=self.finished(state='failed',cleanup={
            'deleted':False,'cleanup_attempts':3,'transport_retries':3,'next_at':self.now+9000,
            'transport_error':'TimeoutError','transport_error_class':'network',
            'operator_check':{'kind':'cma-agent-cleanup'}})
        recheck.defer(self.ledger.tasks,'retire',row['id'],row['data']['agent_id'])
        value=self.task_clear(row['id'])
        control.clear_task_operator_check(self.ledger,value,SECRET)
        control.clear_task_operator_check(self.ledger,value,SECRET)
        cleared=self.ledger.tasks.get(row['id'])
        self.assertEqual(cleared['data']['cleanup'],{'deleted':False,'cleanup_attempts':0})
        self.assertEqual(cleared['revision'],row['revision']+1)
        self.tick()
        self.assertEqual(len(self.cma.retire_calls),1)
        self.assertTrue(self.ledger.tasks.get(row['id'])['data']['cleanup']['deleted'])

    def test_task_clear_rejects_check_revision_and_signature_mismatches(self):
        row=self.finished(cleanup={'operator_check':{'kind':'initial'}})
        value=self.task_clear(row['id'])
        for changed in ({'check_digest':'0'*64},{'row_revision':row['revision']+1},
                        {'row_revision':True}):
            with self.subTest(changed=changed),self.assertRaisesRegex(ValueError,'changed'):
                control.clear_task_operator_check(self.ledger,self.task_clear(row['id'],**changed),SECRET)
        with self.assertRaisesRegex(ValueError,'signature failed'):
            control.clear_task_operator_check(self.ledger,{**value,'check_digest':'0'*64},SECRET)
        current=self.ledger.tasks.update(row['id'],row['revision'],row['state'],row['data'])
        with self.assertRaisesRegex(ValueError,'row revision changed'):
            control.clear_task_operator_check(self.ledger,value,SECRET)
        changed=copy.deepcopy(current['data']);changed['cleanup']['operator_check']={'kind':'later'}
        self.ledger.tasks.update(row['id'],current['revision'],current['state'],changed)
        with self.assertRaisesRegex(ValueError,'operator check changed'):
            control.clear_task_operator_check(self.ledger,value,SECRET)
        control.clear_task_operator_check(self.ledger,self.task_clear(row['id']),SECRET)

    def test_reissued_task_clear_for_later_check_is_a_new_event(self):
        row=self.finished(cleanup={'operator_check':{'kind':'same'}})
        value=self.task_clear(row['id'])
        control.clear_task_operator_check(self.ledger,value,SECRET)
        row=self.ledger.tasks.get(row['id'])
        data=copy.deepcopy(row['data']);data['cleanup']['operator_check']={'kind':'same'}
        self.ledger.tasks.update(row['id'],row['revision'],row['state'],data)
        control.clear_task_operator_check(self.ledger,value,SECRET)
        self.assertIn('operator_check',self.ledger.tasks.get(row['id'])['data']['cleanup'])
        new_value=self.task_clear(row['id'])
        self.assertNotEqual(value,new_value)
        control.clear_task_operator_check(self.ledger,new_value,SECRET)
        self.assertNotIn('operator_check',self.ledger.tasks.get(row['id'])['data']['cleanup'])

    def test_pool_clear_rejects_check_and_row_mismatches_then_accepts_reissue(self):
        check={'kind':'foreign','agents':['agt_foreign']}
        entry=self.failed_entry(operator_check=check)
        entry['cleanup']['operator_check']=check
        self.seed_cleanup([entry])
        value=self.pool_clear(entry['sandbox_id'])
        for changes in ({'check_digest':'0'*64},{'row_revision':'0'*64}):
            with self.subTest(changes=changes),self.assertRaisesRegex(ValueError,'changed'):
                control.clear_sandbox_operator_check(self.ledger,self.pool_clear(entry['sandbox_id'],**changes),SECRET)
        entry['cleanup_checked_at']+=1;self.seed_cleanup([entry])
        with self.assertRaisesRegex(ValueError,'row revision changed'):
            control.clear_sandbox_operator_check(self.ledger,value,SECRET)
        value=self.pool_clear(entry['sandbox_id'])
        entry['operator_check']=entry['cleanup']['operator_check']={'kind':'later'}
        self.seed_cleanup([entry])
        with self.assertRaisesRegex(ValueError,'operator check changed'):
            control.clear_sandbox_operator_check(self.ledger,value,SECRET)
        value=self.pool_clear(entry['sandbox_id'])
        control.clear_sandbox_operator_check(self.ledger,value,SECRET)
        # Replaying the receipt cannot clear a later check. Reissuing at its
        # new row revision can, even with identical check, reason and clock.
        entry=self.state('sandbox_pool_cleanup')[0]
        entry['operator_check']=entry['cleanup']['operator_check']={'kind':'later'}
        self.seed_cleanup([entry])
        control.clear_sandbox_operator_check(self.ledger,value,SECRET)
        self.assertIn('operator_check',self.state('sandbox_pool_cleanup')[0])
        new_value=self.pool_clear(entry['sandbox_id'])
        self.assertNotEqual(value,new_value)
        control.clear_sandbox_operator_check(self.ledger,new_value,SECRET)
        self.assertNotIn('operator_check',self.state('sandbox_pool_cleanup')[0])

    def test_clear_commands_require_bound_envelopes(self):
        row=self.finished(cleanup={'operator_check':{'kind':'manual'}})
        self.seed_cleanup([self.failed_entry(operator_check={'kind':'manual'})])
        for clear,identity in ((control.clear_task_operator_check,{'task_id':row['id']}),
                               (control.clear_sandbox_operator_check,{'sandbox_id':'sbx_failed'})):
            with self.subTest(identity=identity),self.assertRaisesRegex(ValueError,'Invalid .* acknowledgement'):
                clear(self.ledger,signed({'schema_version':1,'reason':'reconciled',**identity}),SECRET)

    def test_cli_binds_and_signs_current_task_and_pool_checks(self):
        row=self.finished(cleanup={'operator_check':{'kind':'task'}})
        self.seed_cleanup([self.failed_entry(operator_check={'kind':'pool'})])
        self.ledger.save(force=True)
        staged=[]
        with patch.object(control,'Releases',return_value=self.remote), \
                patch.object(control,'review_key',return_value=SECRET), \
                patch.object(control,'stage',side_effect=lambda value,prefix,*a:staged.append((prefix,value)) or {}):
            control.main(['task-clear-check','--task',row['id'],'--reason','reconciled'])
            control.main(['pool-clear-check','--sandbox','sbx_failed','--reason','reconciled'])
        self.assertEqual(staged,[('task-clear-check',self.task_clear(row['id'])),
                                 ('pool-clear-check',self.pool_clear('sbx_failed'))])


def setUpModule():
    use_cli_config()
