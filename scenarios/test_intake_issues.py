"""Recording boundaries and local replay for the new offline stages."""
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT),str(ROOT/'scenarios'),str(ROOT/'rust')]
import runner
from managed import recording
from managed.tasks import Tasks
from replay_mathesis import replay


class IntakeIssuesReplayTests(unittest.TestCase):
    def roundtrip(self,case,expected):
        binary=ROOT/'rust/target/debug/examples/scenarios'
        self.assertTrue(binary.is_file())
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);records=root/'records';records.mkdir();snapshot=root/'snapshot.sqlite3'
            tasks=Tasks(snapshot);runner.load(tasks,case['initial_rows']);tasks.close()
            before=hashlib.sha256(snapshot.read_bytes()).hexdigest()
            with patch.dict(os.environ,{'OUTREACH_RECORD_DIR':str(records)}):
                recording.pointer('start',{'schema_version':1,'snapshot':{'sha256':before}})
                runner.execute(case)
            with contextlib.redirect_stdout(io.StringIO()) as out,contextlib.redirect_stderr(io.StringIO()) as err:
                actual=replay(records,snapshot,binary,None)
            self.assertEqual(actual,expected)
            self.assertEqual(out.getvalue()+err.getvalue(),'')
            self.assertEqual(hashlib.sha256(snapshot.read_bytes()).hexdigest(),before)
            return ''.join(p.read_text() for p in records.glob('calls-*.jsonl'))

    def test_new_stages_replay_without_key_and_keep_snapshot_immutable(self):
        patterns=[
            'intake-test_mailbox_triage-TriageTests-test_departmental_reply_links_to_a_sent_copy-*.json',
            'inbound-test_thread_staging-StagingTests-test_split_conversations_choose_incoming_and_keep_read_only_sent_context-*.json',
            'formalization-test_formalization_requests-FilingTests-test_lost_post_is_reconciled_after_restoring_snapshot-*.json',
            'prior-work-test_prior_work_issues-FilingTests-test_lost_post_recovers_by_result_even_after_sources_change-*.json',
            'prior-work-test_prior_work_issues-FilingTests-test_legacy_reads_exact_evidence_and_caches_projection-*.json',
        ]
        for pattern in patterns:
            cases=[json.loads(p.read_text()) for p in sorted((ROOT/'scenarios/cases').glob(pattern))]
            case=next(c for c in cases if c.get('operation') in {'advance','resolve'} and c['script'] and not c.get('error'))
            with self.subTest(stage=case['stage']):
                # Existing hydration fixtures contain mail bodies. This stage
                # intentionally emits only the content-free unavailable frame.
                body=case['stage']=='inbound' and recording.mail_bodies(case)
                text=self.roundtrip(case,{'equal':int(not body),'different':0,'skipped':int(body)})
                if body:
                    self.assertNotIn('Exact mathematical request',text)
                    def metadata(value):
                        if isinstance(value,dict):
                            if 'from' in value or 'internetMessageId' in value:value.pop('body',None)
                            for part in value.values():metadata(part)
                        elif isinstance(value,list):
                            for part in value:metadata(part)
                    metadata(case)
                    self.roundtrip(case,{'equal':1,'different':0,'skipped':0})

    def test_issue_subclass_errors_replay_with_recorded_ancestry(self):
        for stage in ('formalization','prior-work'):
            for kind in ('URLError','JSONDecodeError','ConnectionResetError','SyntheticValueError'):
                case=json.loads((ROOT/'scenarios/cases'/f'{stage}-review-round2-post-{kind}.json').read_text())
                with self.subTest(stage=stage,kind=kind):
                    text=self.roundtrip(case,{'equal':1,'different':0,'skipped':0})
                    self.assertIn('bases',text)

    def test_evidence_subclass_errors_replay_with_recorded_ancestry(self):
        paths=sorted((ROOT/'scenarios/cases').glob('prior-work-review-round3-*.json'))
        self.assertEqual(len(paths),4)
        for path in paths:
            with self.subTest(case=path.stem):
                text=self.roundtrip(json.loads(path.read_text()),{'equal':1,'different':0,'skipped':0})
                self.assertIn('bases',text)

    def test_new_intake_frames_omit_mail_bodies(self):
        case=json.loads(next((ROOT/'scenarios/cases').glob('intake-test_mailbox_triage-TriageTests-test_hello_stranger_stays_in_attention-*.json')).read_text())
        marker='SYNTHETIC MAIL CONTENT THAT MUST NOT BE RECORDED'
        for entry in case['script']:
            if entry['adapter']=='mailbox' and entry['method'] in {'messages','research_thread'}:
                for message in entry['result']:message['body']={'contentType':'text','content':marker}
        # Intake retains only metadata for this unmapped message.
        text=self.roundtrip(case,{'equal':0,'different':0,'skipped':1})
        self.assertNotIn(marker,text)
        self.assertIn('intake_stage_unavailable',text)

    def test_graph_content_fields_emit_only_unavailable_frames(self):
        import copy
        original=json.loads(next((ROOT/'scenarios/cases').glob('intake-test_mailbox_triage-TriageTests-test_hello_stranger_stays_in_attention-*.json')).read_text())
        marker='SYNTHETIC GRAPH CONTENT MUST NOT BE RECORDED'
        for field in ('body','uniqueBody','bodyPreview','contentBytes','content_b64'):
            for nested in (False, True):
                with self.subTest(field=field,nested=nested):
                    case=copy.deepcopy(original)
                    for entry in case['script']:
                        if entry['adapter']=='mailbox' and entry['method'] in {'messages','research_thread'}:
                            for message in entry['result']:
                                value={field:({'contentType':'text','content':marker} if field in {'body','uniqueBody'} else marker)}
                                if nested:message['attachments']=[value]
                                else:message.update(value)
                    text=self.roundtrip(case,{'equal':0,'different':0,'skipped':1})
                    self.assertNotIn(marker,text)
                    self.assertIn('intake_stage_unavailable',text)
        self.assertTrue(recording.mail_bodies({'contentType':'text','content':marker}))

    def test_standalone_attachment_content_is_unavailable_in_both_read_stages(self):
        marker='SYNTHETIC BASE64 ATTACHMENT'
        for stage in ('intake','inbound'):
            frame={'stage':stage,'script':[]}
            token=recording._STAGE.set(frame)
            try:
                self.assertIs(recording._capture('mailbox','attachment_content',lambda:marker,(),{}),marker)
            finally:recording._STAGE.reset(token)
            with self.subTest(stage=stage):
                self.assertTrue(frame.get('incomplete'))
                self.assertNotIn(marker,json.dumps(frame))

    def test_recording_disabled_preserves_handles_and_performs_no_capture(self):
        from managed import formalization_requests,thread_staging,mailbox
        ledger=object();config=object();box=object();row=object();root=object()
        for decorator,args in [(recording.issue_stage,(ledger,config,box)),(recording.inbound_stage,(row,box,ledger,root)),(recording.stage_frame('intake'),(ledger,config,root,box))]:
            seen=[]
            @decorator
            def stage(*a):seen.append(a);return box
            with patch.dict(os.environ,{},clear=True),patch.object(recording,'capture_tables',create=True,side_effect=AssertionError('capture forbidden')),patch.object(recording,'emit',side_effect=AssertionError('emit forbidden')):
                self.assertIs(stage(*args),box)
            self.assertEqual(seen,[args])
        self.assertTrue(callable(formalization_requests.advance) and callable(thread_staging.resolve) and callable(mailbox.intake))

    def test_capture_failure_does_not_prevent_or_repeat_immutable_upload(self):
        from types import SimpleNamespace
        calls=[]
        target=SimpleNamespace(upload_task_batch=lambda *args:calls.append(args) or 'synthetic-receipt')
        frame={'stage':'intake','script':[],'options':{}}
        token=recording._STAGE.set(frame)
        try:
            with patch.dict(os.environ,{'OUTREACH_RECORD_DIR':'synthetic-unused'}),patch('managed.scenario_contract.encode',side_effect=RuntimeError('Synthetic capture failure')),contextlib.redirect_stderr(io.StringIO()):
                result=recording.adapter('releases',target).upload_task_batch({Path('synthetic.bin'):{}},'synthetic-tag',{})
        finally:recording._STAGE.reset(token)
        self.assertEqual(result,'synthetic-receipt')
        self.assertEqual(len(calls),1)
        self.assertTrue(frame['incomplete'])

    def test_json_string_pipeline_payload_roundtrips(self):
        with tempfile.TemporaryDirectory() as directory:
            tasks=Tasks(Path(directory)/'state.sqlite3')
            try:
                tasks.db.execute('INSERT INTO pipeline_state VALUES(?,?)',('prior-work-backfill:synthetic',json.dumps('a'*64)))
                before=runner.dump(tasks);runner.load(tasks,before)
                self.assertEqual(runner.dump(tasks),before)
            finally:tasks.close()

if __name__=='__main__':unittest.main()
