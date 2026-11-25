"""Offline author discovery contract and operator-only registry reconciliation."""
import copy
import hashlib
import hmac
import io
import json
from pathlib import Path
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from fixture_config import use_example_config
from managed import author_contacts as authors, automation, control, registry
from managed.queue import canonical
from managed.tasks import Tasks
from test_tasks import request


def proposal(name='Original Author', address='author@example.invalid'):
    return {'full_name':name, 'title':'Professor',
            'title_evidence_url':'https://example.invalid/author', 'affiliation':'Example University',
            'source_url':'https://doi.org/10.0000/posing-source',
            'emails':[{'email':address,'evidence_url':'https://example.invalid/author'}],
            'problem_ids':['new-result'], 'in_refusals':False, 'refusals_checked_revision':'b'*40}


class AuthorContactsTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        (self.root/'tables').mkdir()
        registry.write(self.root,'contacts',{'contacts':[]})
        registry.write(self.root,'refusals',{'refusals':[]})
        registry.write(self.root,'positive',{'entries':[]})
        registry.write(self.root,'submissions',{'schema_version':1,'updated_on':'2026-11-01',
            'scope':'independent-team','verification':{'mailbox_check':'saved-evidence-only'},'papers':[]})
        self.tasks=Tasks(self.root/'tasks.sqlite3')
        req=request('note-author',kind='shortnote');req['source_revision']='b'*40
        req['inputs']={'result':{'id':'new-result'}}
        self.row=self.tasks.add(req)

    def tearDown(self):
        self.tasks.close();self.temp.cleanup()

    def result(self, contacts=None):
        return {'schema_version':1,'task_id':self.row['id'],'outcome':'completed',
                'formalization_gaps':[], 'value_assessment':{'verdict':'incremental','reasons':'Extends the checked range.'},
                'report':'Source and argument checked', 'packet':None,
                'artifacts':[{'release_tag':'outreach-task-'+self.row['id'],
                              'name':role,'bytes':10,'sha256':'a'*64,'role':role}
                             for role in ['note-pdf','note-tex']],
                'author_contacts':contacts if contacts is not None else [proposal()]}

    def reviewed(self, contacts=None, verdict='PASS'):
        result=self.result(contacts)
        row=self.tasks.update(self.row['id'],self.tasks.get(self.row['id'])['revision'],
                              'awaiting-review',{'result':result})
        env={'binding':self.tasks.review_binding(row),'reviewer':'local-claude-loop',
             'verdict':verdict,'reason':'Verified exact primary author/title/address sources',
             'evidence_ref':'private:independent-review'}
        env['signature']=hmac.new(b'secret',canonical(env).encode(),hashlib.sha256).hexdigest()
        row=self.tasks.verdict(env,'secret')
        return {'request':row['request'],'result':result,'review':env}

    def inventory(self):
        with patch('managed.daily_local.status',return_value={}):
            return automation.work(self.tasks,self.root)['author_contacts_needed']

    def refuse(self, address='author@example.invalid', name='Original Author'):
        registry.write(self.root,'refusals',{'refusals':[{'id':'stop','active':True,'name':name,
            'emails':[address],'date':'2026-10-31','reason':'No more invitations',
            'quote':'Stop','source':'fixture','evidence':'private:message'}]})

    def test_complete_contract_and_research_optional_proposal(self):
        control.validate_result(self.result(),self.row)
        result=self.result();result['author_contacts'][0]['title']='none'
        control.validate_result(result,self.row)
        row=copy.deepcopy(self.row);row['kind']='research'
        del result['formalization_gaps'], result['value_assessment']
        control.validate_result(result,row)
        result.pop('author_contacts');control.validate_result(result,row)

    def test_production_registers_reviewed_authors_without_operator_polling(self):
        from types import SimpleNamespace
        self.reviewed()
        ledger = SimpleNamespace(tasks=self.tasks, save=lambda **kwargs:None)
        config = {'paused':False, 'independent_review':{'enabled':True, 'register_author_contacts':True}}
        with patch('managed.data_writes.commit_paths') as commit, patch('managed.data_writes.push') as push:
            authors.advance(ledger, config, self.root, 'secret')
            authors.advance(ledger, config, self.root, 'secret')
        self.assertEqual(len(registry.people(self.root)), 1)
        self.assertEqual(commit.call_count, 1)
        self.assertEqual(push.call_count, 1)
        self.assertEqual(self.tasks.get(self.row['id'])['data']['author_contacts_reconciled']['phase'], 'reconciled')

    def test_production_registration_preserves_new_refusal(self):
        from types import SimpleNamespace
        self.reviewed(); self.refuse()
        ledger = SimpleNamespace(tasks=self.tasks, save=lambda **kwargs:None)
        config = {'paused':False, 'independent_review':{'enabled':True, 'register_author_contacts':True}}
        with patch('managed.data_writes.commit_paths'), patch('managed.data_writes.push'):
            authors.advance(ledger, config, self.root, 'secret')
        self.assertEqual(registry.people(self.root), [])
        row = self.tasks.get(self.row['id'])
        self.assertTrue(row['data']['author_contacts_reconciled']['skipped'])

    def test_production_rejects_a_forged_pass_before_registry_writes(self):
        from types import SimpleNamespace
        self.reviewed()
        row = self.tasks.get(self.row['id'])
        data = copy.deepcopy(row['data']); data['review']['signature'] = '0' * 64
        self.tasks.update(row['id'], row['revision'], row['state'], data)
        ledger = SimpleNamespace(tasks=self.tasks, save=lambda **kwargs:None)
        config = {'paused':False, 'independent_review':{'enabled':True, 'register_author_contacts':True}}
        with patch('managed.data_writes.commit_paths') as commit, patch('managed.data_writes.push') as push:
            authors.advance(ledger, config, self.root, 'secret')
            authors.advance(ledger, config, self.root, 'secret')
        commit.assert_not_called(); push.assert_not_called()
        self.assertEqual(registry.people(self.root), [])
        self.assertEqual(self.tasks.get(row['id'])['data']['author_contacts_reconciled']['attempts'], 1)

    def test_explicit_empty_and_unreachable_author_are_valid_with_reasons(self):
        result=self.result([])
        with self.assertRaisesRegex(ValueError,'author_contacts_reason'):control.validate_result(result,self.row)
        result['author_contacts_reason']='Read the anonymous primary posing source; no identifiable authors or verified addresses.'
        control.validate_result(result,self.row)
        result['outcome']='hold';control.validate_result(result,self.row)
        result.pop('author_contacts')
        with self.assertRaises(ValueError):control.validate_result(result,self.row)
        result=self.result();result['author_contacts'][0]['emails']=[]
        with self.assertRaisesRegex(ValueError,'email_omission_reason'):control.validate_result(result,self.row)
        result['author_contacts'][0]['email_omission_reason']='Institutional page and ORCID provide no verifiable address.'
        control.validate_result(result,self.row)

    def test_missing_malformed_unpinned_and_guessed_fields_are_rejected(self):
        for field in proposal():
            result=self.result();del result['author_contacts'][0][field]
            with self.subTest(field=field),self.assertRaises(ValueError):control.validate_result(result,self.row)
        mutations=[('title','PhD'),('title_evidence_url','unverified'),('in_refusals','false'),
                   ('refusals_checked_revision','a'*40),('problem_ids',['other-result']),
                   ('emails',[{'email':'guess@example.invalid'}]),('emails',[{'email':'bad','evidence_url':'https://example.invalid'}])]
        for field,value in mutations:
            result=self.result();result['author_contacts'][0][field]=value
            with self.subTest(field=field),self.assertRaises(ValueError):control.validate_result(result,self.row)
        # A note admitted before the contract may omit the field; it stays reviewable.
        result=self.result();del result['author_contacts']
        control.validate_result(result,self.row)
        result=self.result();del result['author_contacts'];result['author_contacts_reason']='orphan reason'
        with self.assertRaises(ValueError):control.validate_result(result,self.row)

    def test_duplicate_author_and_address_and_unexpected_top_field_rejected(self):
        for contacts in [[proposal(),proposal()], [proposal(),proposal('Different Person')]]:
            with self.assertRaises(ValueError):control.validate_result(self.result(contacts),self.row)
        result=self.result();result['author_contacts_extra']=[]
        with self.assertRaises(ValueError):control.validate_result(result,self.row)

    def test_prompts_require_primary_author_evidence_pinned_table_and_no_guesses(self):
        prompt=control.task_prompt(self.row)
        for phrase in ['author_contacts','primary source','never guess','tables/refusals.json','source_revision',
                       'refusals_checked_revision field inside EACH author_contacts entry',
                       'refusals_checked_revision must never appear at the top level of the result']:
            self.assertIn(phrase,prompt)

    def test_prompt_states_first_contact_standard_about_us_routes_auto(self):
        prompt=' '.join(control.task_prompt(self.row).split())
        self.assertIn('a first contact whose only about-us content is the approved standard opening '
                      'and tie-back closing',prompt)
        self.assertLess(prompt.index('Exception (the owner 2026-11-04)'),prompt.index('When unsure, choose owner'))

    def test_register_link_title_provenance_and_idempotence(self):
        row=registry.register('author@example.invalid','Original Author',root=self.root,
            problem_ids=['new-result'],evidence='https://example.invalid/paper',
            title='Professor',title_evidence='https://example.invalid/author')
        self.assertEqual(row['problem_ids'],['new-result'])
        registry.link_problem(row['email'],['other-result'],root=self.root,evidence='https://example.invalid/other')
        before=(self.root/'tables/contacts.json').read_bytes()
        registry.link_problem(row['email'],['other-result'],root=self.root,evidence='https://example.invalid/other')
        self.assertEqual((self.root/'tables/contacts.json').read_bytes(),before)
        registry.render(self.root);registry.validate(self.root)
        row=registry.read(self.root,'contacts')['contacts'][0]
        self.assertEqual(row['salutation']['title'],'Professor')
        self.assertEqual(len(row['problem_links']),2)

    def test_invalid_new_links_title_or_unknown_contact_do_not_write(self):
        before=(self.root/'tables/contacts.json').read_bytes()
        for kwargs in [{'problem_ids':['new-result']},{'title':'Professor'},{'title_evidence':'https://example.invalid'}]:
            with self.assertRaises(ValueError):
                registry.register('author@example.invalid','Original Author',root=self.root,**kwargs)
            self.assertEqual((self.root/'tables/contacts.json').read_bytes(),before)
        with self.assertRaises(ValueError):registry.link_problem('missing@example.invalid',['new-result'],root=self.root,evidence='source')

    def test_refusal_runs_before_registration_or_link_validation(self):
        registry.register('author@example.invalid','Original Author',root=self.root)
        self.refuse();before=(self.root/'tables/contacts.json').read_bytes()
        with self.assertRaisesRegex(ValueError,'Table 3'):
            registry.register('author@example.invalid','Original Author',root=self.root,problem_ids=['new-result'])
        with self.assertRaisesRegex(ValueError,'Table 3'):
            registry.link_problem('author@example.invalid',['new-result'],root=self.root)
        self.assertEqual((self.root/'tables/contacts.json').read_bytes(),before)

    def test_work_only_surfaces_exact_pass_pending_links_and_clears_after_apply(self):
        self.assertEqual(self.inventory(),[])
        value=self.reviewed();items=self.inventory()
        self.assertEqual(items[0]['reviewed_proposal'],value)
        self.assertIn('--problem-id new-result',items[0]['proposals'][0]['addresses'][0]['command'])
        self.assertIn('apply-author-contacts',items[0]['next_action'])
        authors.apply(value,self.root,'secret')
        self.assertEqual(self.inventory(),[])
        self.assertEqual(len(self.tasks.all()),1)

    def test_work_reports_existing_contact_needing_link_and_legacy_pass(self):
        registry.register('author@example.invalid','Original Author',root=self.root)
        self.reviewed()
        self.assertIn('link-problem',self.inventory()[0]['proposals'][0]['addresses'][0]['command'])
        row=self.tasks.get(self.row['id']);data=copy.deepcopy(row['data']);del data['result']['author_contacts']
        self.tasks.update(row['id'],row['revision'],'approved',data)
        self.assertIn('Legacy PASS',self.inventory()[0]['reason'])

    def test_bounce_hold_and_unreviewed_proposals_are_not_registration_work(self):
        for verdict in ['BOUNCE','HOLD']:
            self.reviewed(verdict=verdict);self.assertEqual(self.inventory(),[])
        row=self.tasks.get(self.row['id'])
        self.tasks.update(row['id'],row['revision'],'awaiting-review',{'result':self.result()})
        self.assertEqual(self.inventory(),[])

    def test_apply_signed_exact_result_and_replay_preserves_registry_bytes(self):
        value=self.reviewed()
        authors.apply(value,self.root,'secret');registry.validate(self.root)
        before=(self.root/'tables/contacts.json').read_bytes()
        outcome=authors.apply(value,self.root,'secret')
        self.assertEqual((self.root/'tables/contacts.json').read_bytes(),before)
        self.assertFalse(outcome['enqueued']);self.assertFalse(outcome['sent'])
        with self.assertRaisesRegex(ValueError,'signature'):authors.apply(value,self.root,'wrong')
        changed=copy.deepcopy(value);changed['result']['author_contacts'][0]['emails'][0]['email']='tampered@example.invalid'
        with self.assertRaisesRegex(ValueError,'exact independently'):authors.apply(changed,self.root,'secret')

    def test_apply_preflights_all_addresses_and_propagates_alias_refusal(self):
        author=proposal();author['emails'].append({'email':'alternate@example.invalid','evidence_url':'https://example.invalid/author'})
        value=self.reviewed([author]);self.refuse('alternate@example.invalid','Different recorded spelling')
        before=(self.root/'tables/contacts.json').read_bytes()
        with patch('managed.registry.preflight',wraps=registry.preflight) as check:
            outcome=authors.apply(value,self.root,'secret')
        self.assertEqual(check.call_count,2)
        self.assertEqual(len(outcome['skipped']),2)
        self.assertEqual(outcome['skipped'][0]['refusal_ids'],['stop'])
        self.assertEqual((self.root/'tables/contacts.json').read_bytes(),before)

    def test_apply_skips_pinned_refusal_links_existing_sent_and_registers_eligible(self):
        registry.register('old@example.invalid','Existing Author',root=self.root)
        registry.record_event('old@example.invalid','sent','2026-10-31','Actual Sent','private:sent-message',root=self.root)
        pinned=proposal('Refused Author','refused@example.invalid');pinned['in_refusals']=True
        value=self.reviewed([proposal(),pinned,proposal('Existing Author','old@example.invalid')])
        outcome=authors.apply(value,self.root,'secret')
        self.assertEqual(len(outcome['registered_or_linked']),1)
        self.assertEqual(len(outcome['linked_existing']),1)
        self.assertEqual(len(outcome['skipped']),1)
        rows=registry.read(self.root,'contacts')['contacts']
        self.assertEqual(len(rows),2)
        self.assertEqual(next(r for r in rows if r['email']=='old@example.invalid')['problem_ids'],['new-result'])

    def test_contacted_author_links_existing_row_merges_alias_and_replays_identically(self):
        row=registry.register('author@example.invalid','Original Author',root=self.root,
                              problem_ids=['old-result'],evidence='https://example.invalid/old')
        registry.record_event(row['email'],'sent','2026-10-31','Actual Sent','private:sent',root=self.root)
        registry.mark_undeliverable(row['email'],root=self.root,evidence='private:ndr')
        # The unregistered alias comes first, as in the Kingsley proposal.
        author=proposal(address='new@example.invalid')
        author['emails'].append({'email':row['email'],'evidence_url':'https://example.invalid/author'})
        value=self.reviewed([author])
        self.assertEqual(len(self.inventory()),1)
        self.assertEqual(authors.blocked(value,self.root),[])
        outcome=authors.apply(value,self.root,'secret')
        self.assertEqual(outcome['linked_existing'],[{'email':row['email'],'contact_id':row['id'],
                                                    'problem_ids':['new-result','old-result']}])
        self.assertEqual(outcome['registered_or_linked'],[])
        self.assertEqual(outcome['skipped'],[])
        self.assertFalse(outcome['enqueued']);self.assertFalse(outcome['sent'])
        rows=registry.read(self.root,'contacts')['contacts']
        self.assertEqual(len(rows),1)
        linked=rows[0]
        self.assertEqual(linked['id'],row['id'])
        self.assertEqual(linked['relationship']['aliases'],['author@example.invalid','new@example.invalid'])
        self.assertIn({'problem_id':'new-result','evidence':author['source_url']},linked['problem_links'])
        self.assertEqual(linked['salutation'],{'title':'Professor','verified':True,
                                              'evidence_url':author['title_evidence_url']})
        self.assertEqual(linked['affiliation'],author['affiliation'])
        self.assertEqual(linked['author_contact_sources'],[author])
        self.assertEqual(self.inventory(),[])
        self.assertEqual(len(self.tasks.all()),1)
        registry.validate(self.root)
        before={p.name:p.read_bytes() for p in (self.root/'tables').iterdir()}
        authors.apply(value,self.root,'secret')
        self.assertEqual({p.name:p.read_bytes() for p in (self.root/'tables').iterdir()},before)

    def test_reconcile_links_contacted_author_from_saved_work_and_replays(self):
        registry.register('author@example.invalid','Original Author',root=self.root)
        registry.record_event('author@example.invalid','reply','2026-10-31','Actual reply','private:reply',root=self.root)
        self.reviewed()
        work={'author_contacts_needed':self.inventory()}
        with patch('managed.control.review_key',return_value='secret'):
            result=registry.reconcile(work,apply=True,root=self.root)
        self.assertEqual(result['applied']['author_contacts'],1)
        self.assertEqual(result['exceptions'],[])
        self.assertEqual(self.inventory(),[])
        before=(self.root/'tables/contacts.json').read_bytes()
        with patch('managed.control.review_key',return_value='secret'):
            result=registry.reconcile(work,apply=True,root=self.root)
        self.assertEqual(result['applied']['author_contacts'],0)
        self.assertEqual(result['exceptions'],[])
        self.assertEqual((self.root/'tables/contacts.json').read_bytes(),before)

    def test_refused_author_not_written_and_only_reported_as_blocked(self):
        # A fully linked address must still propagate its refusal to a new alias.
        registry.register('author@example.invalid','Original Author',root=self.root,
                          problem_ids=['new-result'],evidence='https://example.invalid/old')
        author=proposal(address='new@example.invalid')
        author['emails'].append({'email':'author@example.invalid','evidence_url':'https://example.invalid/author'})
        for pinned in [True,False]:
            with self.subTest(pinned=pinned):
                author['in_refusals']=pinned
                value=self.reviewed([author])
                stale={'author_contacts_needed':[{'task_id':self.row['id'],'reviewed_proposal':value}]}
                if not pinned:self.refuse(name='Different recorded spelling')
                before=(self.root/'tables/contacts.json').read_bytes()
                outcome=authors.apply(value,self.root,'secret')
                self.assertEqual(len(outcome['skipped']),2)
                self.assertEqual(outcome['linked_existing'],[])
                self.assertEqual(outcome['registered_or_linked'],[])
                self.assertEqual((self.root/'tables/contacts.json').read_bytes(),before)
                with patch('managed.daily_local.status',return_value={}):
                    work=automation.work(self.tasks,self.root)
                self.assertEqual(work['author_contacts_needed'],[])
                self.assertEqual(len(work['author_contacts_blocked']),1)
                self.assertEqual(len(authors.blocked(value,self.root)),1)
                result=registry.reconcile(stale,apply=True,root=self.root)
                self.assertEqual(result['exceptions'],[])
                self.assertEqual((self.root/'tables/contacts.json').read_bytes(),before)

    def test_region_excluded_author_not_written_and_only_reported_as_blocked(self):
        registry.write(self.root,'decisions',{'firstcontact_excluded_regions':{'domain_suffixes':['foreign.example.invalid']}})
        registry.register('author@example.invalid','Original Author',root=self.root)
        registry.record_event('author@example.invalid','sent','2026-10-31','Actual Sent','private:sent',root=self.root)
        author=proposal()
        author['emails'].append({'email':'author@foreign.example.invalid','evidence_url':'https://example.invalid/author'})
        value=self.reviewed([author])
        before=(self.root/'tables/contacts.json').read_bytes()
        outcome=authors.apply(value,self.root,'secret')
        self.assertEqual(len(outcome['skipped']),2)
        self.assertIn('excluded-region-mainland-china',outcome['skipped'][0]['reasons'])
        self.assertEqual(outcome['linked_existing'],[])
        self.assertEqual(outcome['registered_or_linked'],[])
        self.assertEqual((self.root/'tables/contacts.json').read_bytes(),before)
        with patch('managed.daily_local.status',return_value={}):
            work=automation.work(self.tasks,self.root)
        self.assertEqual(work['author_contacts_needed'],[])
        self.assertEqual(len(work['author_contacts_blocked']),1)
        self.assertEqual(len(authors.blocked(value,self.root)),1)
        self.assertIn('excluded-region-mainland-china',work['author_contacts_blocked'][0]['reason'])
        self.assertEqual(len(self.tasks.all()),1)

    def test_verified_aliases_merge_and_historical_sources_remain_valid(self):
        author=proposal();author['emails'].append({'email':'second@example.invalid','evidence_url':'https://example.invalid/author'})
        authors.apply(self.reviewed([author]),self.root,'secret')
        self.assertEqual(len(registry.people(self.root)),1)
        self.assertEqual(len(registry.read(self.root,'contacts')['contacts']),1)
        author['source_url']='https://example.invalid/new-source'
        authors.apply(self.reviewed([author]),self.root,'secret')
        registry.validate(self.root)
        self.assertEqual(len(registry.read(self.root,'contacts')['contacts'][0]['author_contact_sources']),2)

    def test_apply_links_existing_contact_with_null_relationship(self):
        registry.register('author@example.invalid','Original Author',root=self.root)
        data=registry.read(self.root,'contacts');data['contacts'][0]['relationship']=None
        registry.write(self.root,'contacts',data)
        outcome=authors.apply(self.reviewed(),self.root,'secret')
        self.assertEqual(len(outcome['registered_or_linked']),1)
        row=registry.read(self.root,'contacts')['contacts'][0]
        self.assertEqual(row['relationship']['aliases'],['author@example.invalid'])

    def test_cli_register_link_and_apply_run_locally_and_render(self):
        with patch('managed.registry.ROOT',self.root),redirect_stdout(io.StringIO()):
            registry.main(['register','--email','author@example.invalid','--name','Original Author',
                           '--problem-id','old-result','--title','Dr.','--title-evidence','https://example.invalid/author',
                           '--evidence','https://example.invalid/paper'])
            registry.main(['link-problem','--email','author@example.invalid','--problem-id','new-result',
                           '--problem-id','extra-result','--evidence','https://example.invalid/paper'])
            path=self.root/'reviewed.json';path.write_text(json.dumps(self.reviewed()))
            with patch('managed.control.review_key',return_value='secret'):
                registry.main(['apply-author-contacts',str(path)])
        registry.validate(self.root)
        self.assertEqual(len(registry.read(self.root,'contacts')['contacts'][0]['problem_ids']),3)

    def test_contact_schema_rejects_missing_link_evidence_and_title_sources(self):
        authors.apply(self.reviewed(),self.root,'secret')
        original=registry.read(self.root,'contacts')
        for field,value in [('problem_links',[{'problem_id':'new-result','evidence':''}]),
                            ('salutation',{'title':'Professor','verified':True,'evidence_url':'missing'}),
                            ('author_contact_sources',[{'full_name':'Unverified Author'}])]:
            data=copy.deepcopy(original);data['contacts'][0][field]=value
            registry.write(self.root,'contacts',data)
            with self.subTest(field=field),self.assertRaises(ValueError):registry.validate(self.root)


def setUpModule():
    use_example_config()


if __name__=='__main__':unittest.main()
