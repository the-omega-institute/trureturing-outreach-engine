#!/usr/bin/env python3
"""Extract intake, inbound hydration and issue stages with original assertions intact."""
import copy
from contextlib import ExitStack
from datetime import datetime, timezone
import importlib
import json
from pathlib import Path
import sys
import tempfile
from types import CodeType
import unittest
from unittest.mock import patch

from runner import ROOT, NOW, dump, logical, files, writes, encode
from extract_pool_delivery import StageCapture, TransparentProxy, error_value as base_error_value

def error_value(error):
    value=base_error_value(error)
    for name in ('attachments','intake_reason'):
        if hasattr(error,name):value[name]=getattr(error,name)
    return value
sys.path[:0] = [str(ROOT / 'managed/tests'), str(ROOT / 'scripts/short_notes')]
from managed import control, mailbox, thread_staging, formalization_requests, prior_work_issues, publication, recording
from managed.tasks import Tasks

def workflows():
    result={}
    for kind in ('reply','research','shortnote'):
        try: result[kind]=control.workflow(kind)
        except KeyError: pass
    return result

FEATURES = ('sent_snapshot', 'match_inbound_sent', 'sent_messages_to', 'attachment_content')
MODULES = ('test_mailbox_triage', 'test_bounces', 'test_automation',
           'test_mailbox_attachments', 'test_thread_staging', 'test_formalization_requests', 'test_prior_work_issues')

def flatten(suite):
    for item in suite:
        if isinstance(item, unittest.TestSuite): yield from flatten(item)
        else: yield item

class Proxy(TransparentProxy):
    def __getattr__(self,name):
        if isinstance(self.target,mailbox.GraphMailbox) and name in {'match_inbound_sent','_refresh_sent_subjects'} and getattr(getattr(self.target,name),'__func__',None) is getattr(mailbox.GraphMailbox,name): return getattr(self.target,name)
        if name in FEATURES and type(self.target).__module__.startswith("unittest.mock"): raise AttributeError(name)
        return super().__getattr__(name)
    @property
    def __class__(self): return self.target.__class__

class Capture(StageCapture):
    def proxy(self,name,target): return Proxy(self,name,target)
    def call(self, name, method, function, args, kwargs):
        if name == 'releases' and method == 'upload_task_batch':
            # Paths are local handles. The port transmits ordered file/ref pairs.
            pairs = [[encode(path), ref] for path, ref in args[0].items()]
            def upload(*_a,**_kw):
                function(*args,**kwargs); return {'listings':encode(args[2])}
            super().call(name, method, upload, (pairs, *args[1:]), kwargs)
            return None
        try:
            result=super().call(name, method, function, args, kwargs)
            if name=='releases' and method=='run' and '--dir' in args:
                path=Path(args[args.index('--dir')+1])/args[args.index('--pattern')+1]
                self.entries[-1]['download']={'name':path.name,'base64':encode(path)['base64']}
            return result
        except BaseException as error:
            if self.entries and 'error' in self.entries[-1]:
                self.entries[-1]['error']={**error_value(error), 'bases':[base.__name__ for base in type(error).__mro__[1:] if base is not object]}
            raise

def synthetic_domains(code):
    # Original alias fixtures use invented .co.uk institutions. Use the reserved
    # .invalid namespace while preserving their distinct registrable domains.
    return code.replace(co_consts=tuple(synthetic_domains(v) if isinstance(v,CodeType) else v.replace('fictional.co.uk','fictional.invalid').replace('unrelated.co.uk','unrelated.invalid') if isinstance(v,str) else v for v in code.co_consts))


def main():
    out = Path(__file__).parent/'cases'; mapping_path = out.parent/'test-map.json'
    mapping = json.loads(mapping_path.read_text())
    for path in out.glob('*.json'):
        if json.loads(path.read_text())['stage'] in {'intake','inbound','formalization','prior-work'}: path.unlink()
    for key in list(mapping):
        mapping[key] = [v for v in mapping[key] if not v.startswith(('intake-','inbound-','formalization-','prior-work-'))]
    total = 0
    for module in MODULES:
        mod = importlib.import_module(module)
        if module=='test_automation':
            for value in vars(mod.AutomationTests).values():
                if callable(value) and hasattr(value,'__code__'):value.__code__=synthetic_domains(value.__code__)
        if hasattr(mod, 'setUpModule'): mod.setUpModule()
        for test in flatten(unittest.defaultTestLoader.loadTestsFromModule(mod)):
            identity = test.id()
            if not identity.startswith(module+'.'): continue
            if module=='test_automation' and not any(word in identity for word in ('unregistered','inbox_thread','known_correspondent','unmapped','alias','machine_bounce','incomplete_inbox')): continue
            mapping.setdefault(identity, [])
            depth = 0
            def capture(stage, operation, ledger, cfg, root, box, invoke, kwargs):
                nonlocal depth, total
                root=Path(root); cap=Capture(); caught=None
                case={'schema_version':1,'id':stage+'-'+identity.replace('.','-')+'-'+str(len(mapping[identity])+1),
                      'data_repo':__import__('managed.location',fromlist=['DATA_REPO']).DATA_REPO,'source_test':identity,'stage':stage,'operation':operation,'now':ledger.tasks.clock(),
                      'workflow':workflows().get('reply','e'*64),'workflows':workflows(),
                      'installation':__import__('managed.location',fromlist=['example_config']).example_config(),
                      'config':copy.deepcopy(cfg),'initial_rows':dump(ledger.tasks),'initial_files':files(root),
                      'adapter_attributes':{'releases':{k:getattr(ledger.releases,k,'') for k in ('repo','tag')}},
                      'previous_units':copy.deepcopy(getattr(getattr(ledger,'tick',None),'previous',{})),
                      'options':{'mailbox':box is not None,'github':box is not None,
                                 'previous':list(getattr(getattr(ledger,'tick',None),'previous',{}).get('mailbox-intake',[]))}}
                if stage in {'intake','inbound'}:
                    case['adapter_attributes']['mailbox']={'settings':copy.deepcopy(getattr(box,'settings',{})), 'graph_mailbox':isinstance(box,mailbox.GraphMailbox),'native_matching':getattr(getattr(box,'match_inbound_sent',None),'__func__',None) is mailbox.GraphMailbox.match_inbound_sent, **{k:mailbox._adapter_method(box,k) is not None for k in FEATURES}}
                if stage=='intake':
                    case.update(revision=kwargs.get('revision') or 'b'*40,datetime=(kwargs.get('now') or datetime.fromtimestamp(NOW,timezone.utc)).isoformat())
                if stage=='inbound': case['row']=copy.deepcopy(cfg)
                frame={'admissions':[],'_errors':{}}; token=recording._STAGE.set(frame); depth+=1
                try:
                    with ExitStack() as hooks:
                        original_adapter=mailbox._adapter_method
                        def method(target,name):
                            target=target.target if isinstance(target,Proxy) else target
                            function=original_adapter(target,name)
                            if function is None:return None
                            if name=='match_inbound_sent' and getattr(function,'__func__',None) is mailbox.GraphMailbox.match_inbound_sent:return function
                            return lambda *a,**kw:cap.call('mailbox',name,function,a,kw)
                        hooks.enter_context(patch.object(mailbox,'_adapter_method',method))
                        if isinstance(box,mailbox.GraphMailbox):
                            for name in ('_read_sent_subjects','sent_message_by_internet_id'):
                                original=getattr(box,name)
                                hooks.enter_context(patch.object(box,name,lambda *a,_name=name,_original=original,**kw:cap.call('mailbox',_name,_original,a,kw)))
                        original_save, original_releases, original_update=ledger.save,ledger.releases,ledger.tasks.update
                        def save(*a,**kw):
                            with patch.object(ledger,'releases',original_releases): result=cap.call('ledger','save',original_save,a,kw)
                            if getattr(ledger,'snapshot',None): cap.entries[-1]['saved_snapshot']={'generation':ledger.generation,'digest':ledger.snapshot['sha256']}
                            return result
                        times=[]
                        def update(*a,**kw):
                            row=original_update(*a,**kw); at=ledger.tasks.db.execute('SELECT at FROM task_events ORDER BY seq DESC LIMIT 1').fetchone()[0]
                            times.append([row['updated'],at]); return row
                        hooks.enter_context(patch.object(ledger,'save',save)); hooks.enter_context(patch.object(ledger,'releases',cap.proxy('releases',original_releases)))
                        hooks.enter_context(patch.object(ledger.tasks,'update',update))
                        from managed import pipeline
                        request=pipeline.request
                        def bound(*a,**kw):
                            value=request(*a,**kw); case['binding']={k:v for k,v in value.items() if k in {'source_revision','workflow_hash','policy_revision'}}; return value
                        hooks.enter_context(patch.object(pipeline,'request',bound))
                        download=publication.download
                        def read(row,name,directory,releases):
                            return cap.call('evidence','download',lambda *_a: download(row,name,directory,original_releases),
                                            (row['id'],name,Path(directory)),{})
                        hooks.enter_context(patch.object(publication,'download',read))
                        wrapped=cap.proxy('mailbox',box) if box is not None and stage in {'intake','inbound'} else ((lambda *a,**kw:cap.call('github','request',box,a,kw)) if box is not None else None)
                        try: result=invoke(wrapped)
                        except BaseException as error: case['error']=error_value(error);caught=error;result=None
                finally: depth-=1;recording._STAGE.reset(token)
                case.update(expected_result=encode(result),script=cap.entries,admissions=frame['admissions'],update_times=times,
                            expected_final_rows=logical(dump(ledger.tasks)),expected_final_files=files(root),expected_writes=writes(cap.entries))
                (out/(case['id']+'.json')).write_text(json.dumps(case,ensure_ascii=False,separators=(',',':'))+'\n')
                mapping[identity].append(case['id']);total+=1
                if caught:raise caught
                return result
            def pure(stage,operation,function):
                def invoke(*args,**kwargs):
                    nonlocal depth,total
                    if depth:return function(*args,**kwargs)
                    if operation=='plan':root=Path(args[1])
                    else:root=Path(test.root) if hasattr(test,'root') else Path(test.folder.name) if hasattr(test,'folder') else ROOT
                    # Only policy/data files of the test's synthetic fixture.
                    if root==ROOT: initial_files={}
                    else:initial_files=files(root)
                    arguments=encode(args[:1] if operation in {'plan','coverage'} else args[:3] if operation=='build_inbound_request' else args)
                    if operation=='plan':arguments[0]={k:v for k,v in arguments[0].items() if k in {'tasks','affairs_tasks','prior_work_issues','prior_work_checks','prior_work_backfill'}}
                    local=None
                    if operation=='plan' and len(args)>2 and args[2] is not None:
                        directory=Path(args[2]);local={str(path.relative_to(directory)):encode(path)['base64'] for path in directory.rglob('*') if path.is_file() and path.suffix in {'.md','.txt'}}
                    if operation=='coverage':initial=dump(args[1])
                    else:
                        with tempfile.TemporaryDirectory() as temporary:
                            tasks=Tasks(Path(temporary)/'state.sqlite3');initial=dump(tasks);tasks.close()
                    case={'schema_version':1,'id':stage+'-'+identity.replace('.','-')+'-'+str(len(mapping[identity])+1),
                          'data_repo':__import__('managed.location',fromlist=['DATA_REPO']).DATA_REPO,'source_test':identity,'stage':stage,'operation':operation,'now':NOW,'workflow':'e'*64,
                          'config':{},'initial_rows':initial,'expected_final_rows':logical(initial),'initial_files':initial_files,'expected_final_files':initial_files,
                          'arguments':arguments,'local_files':local,'options':{},'adapter_attributes':{},'script':[],'expected_writes':[]}
                    depth+=1;caught=None
                    try:
                        result=function(*args,**kwargs);case['expected_result']=encode(result)
                        if operation=='build_inbound_request':case['binding']={k:v for k,v in result.items() if k in {'source_revision','workflow_hash','policy_revision'}}
                    except BaseException as error:case['error']=error_value(error);caught=error;result=None
                    finally:depth-=1
                    (out/(case['id']+'.json')).write_text(json.dumps(case,ensure_ascii=False,separators=(',',':'))+'\n')
                    mapping[identity].append(case['id']);total+=1
                    if caught:raise caught
                    return result
                return invoke
            original_intake,original_resolve=mailbox.intake,thread_staging.resolve
            original_formal,original_prior=formalization_requests.advance,prior_work_issues.advance
            def intake(ledger,cfg,root,box,revision=None,now=None):
                if depth:return original_intake(ledger,cfg,root,box,revision,now)
                kw={'revision':revision,'now':now}
                return capture('intake','advance',ledger,cfg,root,box,lambda b:original_intake(ledger,cfg,root,b,revision or 'b'*40,now or datetime.fromtimestamp(NOW,timezone.utc)),kw)
            def resolve(row,box,ledger,root):
                if depth:return original_resolve(row,box,ledger,root)
                return capture('inbound','resolve',ledger,row,root,box,lambda b:original_resolve(row,b,ledger,root),{})
            def formal(ledger,cfg,github=None):
                if depth:return original_formal(ledger,cfg,github)
                return capture('formalization','advance',ledger,cfg,Path(ledger.tasks.path).parent,github,lambda b:original_formal(ledger,cfg,b),{})
            def prior(ledger,cfg,root,github=None):
                if depth:return original_prior(ledger,cfg,root,github)
                return capture('prior-work','advance',ledger,cfg,root,github,lambda b:original_prior(ledger,cfg,root,b),{})
            with ExitStack() as hooks:
                init=Tasks.__init__
                hooks.enter_context(patch.object(Tasks,'__init__',lambda self,path,clock=None:init(self,path,clock=clock or (lambda:NOW-100))))
                hooks.enter_context(patch('time.time',return_value=NOW))
                if module=='test_thread_staging':
                    hooks.enter_context(patch.object(thread_staging,'build_inbound_request',pure('inbound','build_inbound_request',thread_staging.build_inbound_request)))
                    hooks.enter_context(patch.object(thread_staging,'require_uncovered_inbound',pure('inbound','coverage',thread_staging.require_uncovered_inbound)))
                if module=='test_formalization_requests':
                    for name in ('pending_requests','issue_payload','same_issue'):
                        hooks.enter_context(patch.object(formalization_requests,name,pure('formalization',name,getattr(formalization_requests,name))))
                if module=='test_prior_work_issues':
                    from managed import prior_work_evidence
                    for target,name in ((prior_work_issues,'plan'),(prior_work_evidence,'project'),(prior_work_evidence,'parse_text')):
                        hooks.enter_context(patch.object(target,name,pure('prior-work',name,getattr(target,name))))
                hooks.enter_context(patch.object(mailbox,'intake',intake));hooks.enter_context(patch.object(thread_staging,'resolve',resolve))
                hooks.enter_context(patch.object(formalization_requests,'advance',formal));hooks.enter_context(patch.object(prior_work_issues,'advance',prior))
                result=unittest.TestResult();test.run(result)
                if result.errors or result.failures:raise AssertionError(identity+'\n'+str(result.errors+result.failures))
    mapping_path.write_text(json.dumps(mapping,indent=2)+'\n')
    from test_review_regressions import extract as review_regressions
    review_regressions()
    print(json.dumps({'new_scenarios':total,'converted_tests':sum(any(v.startswith(('intake-','inbound-','formalization-','prior-work-')) for v in values) for values in mapping.values())}))

if __name__=='__main__':main()
