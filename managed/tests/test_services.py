import json
import base64
import hashlib
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from fixture_config import use_example_config
from fixture_config import use_cli_config
from managed.cma import CMA, output_text
from managed.handoff import prepare
from managed.control import validate_result, verify_artifacts


class CMAContractTests(unittest.TestCase):
    def test_doctor_resolves_review_key_without_disclosing_it(self):
        from managed.control import main
        with tempfile.TemporaryDirectory() as temp:
            home=Path(temp);default=home/'.config/trureturing-outreach/review.key'
            default.parent.mkdir(parents=True);default.write_text('default-review-secret\n')
            custom=home/'custom.key';custom.write_text('custom-review-secret\n')
            empty=home/'empty.key';empty.write_text(' \n')
            cases=[({},True),({'OUTREACH_REVIEW_KEY_FILE':str(custom)},True),
                   ({'OUTREACH_REVIEW_KEY':'env-review-secret','OUTREACH_REVIEW_KEY_FILE':str(empty)},True),
                   ({'OUTREACH_REVIEW_KEY_FILE':str(empty)},False),
                   ({'OUTREACH_REVIEW_KEY_FILE':str(home/'missing')},False)]
            for env,expected in cases:
                with self.subTest(env=env), patch.dict('os.environ',env,clear=True), \
                        patch('managed.control.Path.home',return_value=home), \
                        patch('managed.control.shutil.which',return_value=None), \
                        patch('managed.control.Releases') as releases, \
                        patch.object(CMA,'discover') as discover:
                    releases.return_value.release.return_value=None
                    report=home/'doctor.json'
                    with self.assertRaises(SystemExit):main(['--output',str(report),'doctor'])
                    result=json.loads(report.read_text())
                    self.assertIs(result.pop('review_key_configured'),expected)
                    if not env:baseline=result
                    self.assertEqual(result,baseline)
                    self.assertNotIn('review-secret',report.read_text())
                    discover.assert_not_called()

    def test_doctor_unreadable_review_key_is_unconfigured(self):
        from managed.control import main
        with tempfile.TemporaryDirectory() as temp:
            report=Path(temp)/'doctor.json'
            with patch.dict('os.environ',{},clear=True), \
                    patch('managed.control.review_key',side_effect=PermissionError('unreadable')), \
                    patch('managed.control.shutil.which',return_value=None), \
                    patch('managed.control.Releases') as releases:
                releases.return_value.release.return_value=None
                with self.assertRaises(SystemExit):main(['--output',str(report),'doctor'])
            self.assertFalse(json.loads(report.read_text())['review_key_configured'])

    def test_failed_readiness_is_nonzero_and_preserves_diagnostic_report(self):
        from managed.control import main
        for error in (None, RuntimeError('Provider refused this request')):
            with self.subTest(error=error), tempfile.TemporaryDirectory() as temp:
                report=Path(temp)/'status.json'
                with patch.dict('os.environ',{'CMA_ACCESS_TOKEN':'fixture'} if error else {},clear=True), \
                        patch('managed.control.shutil.which',return_value=None), \
                        patch('managed.control.Releases') as releases, \
                        patch.object(CMA,'discover',side_effect=error) as discover:
                    releases.return_value.release.return_value=None
                    with self.assertRaises(SystemExit) as stopped:
                        main(['--output',str(report),'doctor'])
                    self.assertEqual(stopped.exception.code,1)
                    result=json.loads(report.read_text())
                    self.assertEqual(result['cma']['status'],'unavailable' if error else 'unconfigured')
                    self.assertEqual(discover.call_count,1 if error else 0)
                    self.assertEqual(result['mailbox'], 'microsoft-graph')

    def test_task_owned_sandbox_cleanup_preserves_unexpected_user_work(self):
        cma=CMA()
        with patch.object(cma,'request',side_effect=[{'status':'deleted'},{'status':'active'},
                {'items':[{'id':'agt_other','status':'stopped'}],'next':None}]) as request:
            result=cma.retire('agt_test','original-key','sbx_owned')
            self.assertTrue(result['deleted']);self.assertFalse(result['sandbox_retired'])
            self.assertFalse(any(call.args[1:2]==('DELETE',) for call in request.call_args_list))
        with patch.object(cma,'request',side_effect=[{'status':'deleted'},{'status':'active'},
                {'items':[{'id':'agt_test','status':'deleted'}],'next':None},
                {'items':[{'id':'wks_original'}]},{},{'status':'retired'}]) as request:
            result=cma.retire('agt_test','original-key','sbx_owned')
            self.assertTrue(result['sandbox_retired'])
            self.assertIn((('/api/v1/sandboxes/sbx_owned','DELETE',None,'original-key-sandbox-delete'),{}),
                          [(call.args,call.kwargs) for call in request.call_args_list])
        with patch.object(cma,'request',side_effect=[{'status':'deleted'},{'status':'active'},
                {'items':[{'id':'agt_test','status':'deleted'}],'next':None},
                {'items':[{'id':'wks_original'},{'id':'wks_user'}]}]) as request:
            result=cma.retire('agt_test','original-key','sbx_owned')
            self.assertFalse(result['sandbox_retired'])
            self.assertFalse(any(call.args[1:2]==('DELETE',) for call in request.call_args_list))

    def test_agent_file_delivery_binds_worktree_bytes_and_size(self):
        raw=b'checked result';sha=hashlib.sha256(raw).hexdigest();cma=CMA()
        record={'path':'cma/workspace/.worktrees/wt-1/deliverables/result.txt',
                'size_bytes':len(raw),'sha256':sha,'content_base64':base64.b64encode(raw).decode()}
        detail={'worktree':{'path':'/home/cma/workspace/.worktrees/wt-1'}}
        with patch.object(cma,'request',side_effect=[detail,record]) as request:
            self.assertEqual(cma.artifact('agt_test','deliverables/result.txt',len(raw),sha),raw)
            self.assertIn('path=cma%2Fworkspace%2F.worktrees%2Fwt-1%2Fdeliverables%2Fresult.txt',request.call_args.args[0])
        with patch.object(cma,'request',side_effect=[detail,{**record,'sha256':'0'*64}]):
            with self.assertRaisesRegex(ValueError,'mismatch'):cma.artifact('agt_test','deliverables/result.txt',len(raw),sha)
        with patch.object(cma,'request',side_effect=[detail,{**record,'size_bytes':len(raw)+1}]):
            with self.assertRaisesRegex(ValueError,'mismatch'):cma.artifact('agt_test','deliverables/result.txt',len(raw),sha)
        with patch.object(cma,'request') as request:
            for path in ['../secret','/home/cma/secret','deliverables/../../secret']:
                with self.assertRaises(ValueError):cma.artifact('agt_test',path,len(raw),sha)
            request.assert_not_called()

    def test_result_excludes_durable_progress_messages_and_tool_activity(self):
        result={'schema_version':1,'task_id':'example','outcome':'completed'}
        response={'output':[
            {'type':'message','content':[{'type':'output_text','text':'I will check the source.'}]},
            {'type':'mcp_call','status':'completed','name':'exact computation'},
            {'type':'message','content':[{'type':'output_text','text':json.dumps(result)}]},
            {'type':'mcp_call','status':'completed','name':'retained activity'}]}
        self.assertEqual(json.loads(output_text(response)),result)
        self.assertEqual(output_text({'output':[]}), '')

    def test_json_content_type_and_http_problem_even_on_cli_success(self):
        with patch('managed.cma.subprocess.run',return_value=subprocess.CompletedProcess([],0,'{"status":415,"code":"unsupported_media_type"}','')) as run:
            with self.assertRaisesRegex(RuntimeError,'unsupported_media_type'):
                CMA().create({'name':'example'},'original-key')
        args=run.call_args.args[0]
        self.assertIn('Content-Type:application/json',args)
        self.assertIn('Idempotency-Key:original-key',args)

    def test_retire_waits_for_stop_and_delete_within_one_call(self):
        cma=CMA();cma.retire_wait=10
        seq=[{'status':'running'},{},{'status':'stopping'},{'status':'stopped'},{},{'status':'deleting'},{'status':'deleted'}]
        with patch('managed.cma.time.sleep'),patch.object(cma,'request',side_effect=seq) as request:
            result=cma.retire('agt_example','key')
        self.assertTrue(result['deleted'])
        self.assertIn(('/api/v1/agents/agt_example','DELETE',None,'key-delete'),[c.args for c in request.call_args_list])

    def test_async_stop_is_observed_before_delete(self):
        cma=CMA()
        with patch.object(cma,'request',side_effect=[{'status':'running'},{},{'status':'stopping'}]) as request:
            self.assertFalse(cma.retire('agt_example','key')['deleted'])
            self.assertFalse(any(call.args[1:2]==('DELETE',) for call in request.call_args_list))
        with patch.object(cma,'request',side_effect=[{'status':'stopped'},{},{'status':'deleting'}]) as request:
            self.assertFalse(cma.retire('agt_example','key')['deleted'])
            self.assertEqual(request.call_args_list[1].args,('/api/v1/agents/agt_example','DELETE',None,'key-delete'))
        with patch.object(cma,'request',return_value={'status':'deleted'}) as request:
            self.assertTrue(cma.retire('agt_example','key')['deleted'])
            self.assertEqual(request.call_count,1)
        with patch.object(cma,'request',return_value={'status':'deleting'}) as request:
            self.assertFalse(cma.retire('agt_example','key')['deleted'])
            self.assertEqual(request.call_count,1)


class HandoffTests(unittest.TestCase):
    def test_handoff_has_exact_selected_science_and_requires_platform_confirmation(self):
        from managed import handoff
        import sys
        sys.path.insert(0,str(Path(handoff.__file__).resolve().parents[1]/'scripts/short_notes'))
        import workflow as w
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            meta={'id':'example-result','revision':1,'problem_ids':['example-problem'],
                  'topic':'Example','title':'An example theorem','creators':[{'name':'Example, A.'}],
                  'abstract':'An exact result.','summary':'An exact result.',
                  'license':'cc-by-4.0','evidence_url':'https://sample.example.invalid/proof.lean'}
            (root/'metadata.json').write_text(json.dumps(meta))
            (root/'note.pdf').write_bytes(b'%PDF-1.4\nexample')
            with zipfile.ZipFile(root/'source.zip','w') as z:z.writestr('note.tex',r'\documentclass{article}\begin{document}Example\end{document}')
            w.prepare(root/'metadata.json',root/'note.pdf',root/'source.zip',root/'package')
            (root/'package'/'private-thread.eml').write_text('private correspondence')
            result=prepare(root/'package',root/'handoff')
            self.assertEqual(set(p.name for p in (root/'handoff').iterdir()),{'note-v1.pdf','source-v1.zip','handoff.json'})
            self.assertEqual(result['status'],'prepared-not-submitted')
            self.assertEqual(result['third_party_processing_consent'],'not-asserted')
            (root/'package'/'note-v1.pdf').write_bytes(b'changed')
            with self.assertRaisesRegex(ValueError,'artifact changed'):prepare(root/'package',root/'changed')


class ArtifactTests(unittest.TestCase):
    def test_agent_files_are_preserved_and_read_back_before_review(self):
        data=b'private task result'
        artifact={'release_tag':'outreach-task-job-1','agent_path':'deliverables/evidence.txt',
                  'name':'evidence.txt','role':'evidence','bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()}
        result={'schema_version':1,'task_id':'job-1','outcome':'completed','report':'checked','artifacts':[artifact],'packet':None}
        validate_result(result,{'id':'job-1','kind':'research'})
        class Releases:
            repo='test/repo'
            saved=None
            def preserve_task(self,path,tag):self.saved=(tag,path.name,path.read_bytes())
            def run(self,*args):
                folder=Path(args[args.index('--dir')+1]);(folder/self.saved[1]).write_bytes(self.saved[2])
        remote=Releases()
        with tempfile.TemporaryDirectory() as temp, patch.object(CMA,'artifact',return_value=data):
            verify_artifacts(result,remote,temp,CMA(),'agt_test')
        self.assertEqual(remote.saved,('outreach-task-job-1','evidence.txt',data))
        self.assertNotIn('agent_path',result['artifacts'][0])
        # A rejected runtime path must not get as far as any file read or upload.
        for bad in ['../credentials','deliverables/../credentials','/home/cma/secret']:
            artifact['agent_path']=bad
            with self.assertRaisesRegex(ValueError,'artifact path'):validate_result(result,{'id':'job-1','kind':'research'})

    def test_actual_asset_readback_and_task_release_boundary(self):
        data=b'checked private evidence'
        artifact={'release_tag':'outreach-task-job-1','name':'evidence.txt','role':'evidence','bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()}
        result={'schema_version':1,'task_id':'job-1','outcome':'completed','report':'Verified result','artifacts':[artifact],'packet':None}
        row={'id':'job-1','kind':'research'}
        validate_result(result,row)
        class Releases:
            repo='test/repo'
            def run(self,*args):
                folder=Path(args[args.index('--dir')+1])
                with (folder/'evidence.txt').open('xb') as output:output.write(data)
        with tempfile.TemporaryDirectory() as temp:
            verify_artifacts(result,Releases(),temp)
            artifact['sha256']='0'*64
            with self.assertRaisesRegex(ValueError,'readback'):verify_artifacts(result,Releases(),temp)
            artifact['sha256']=hashlib.sha256(data).hexdigest()
            verify_artifacts(result,Releases(),temp)  # A failed readback leaves no collision for the next task.
        artifact['release_tag']='workspace-migration-20261031'
        with self.assertRaisesRegex(ValueError,'task release'):validate_result(result,row)


if __name__=='__main__':unittest.main()


def setUpModule():
    use_cli_config()
    use_example_config()
