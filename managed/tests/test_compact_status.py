"""Compact status matches aggregates and does not restore task payloads."""
from contextlib import redirect_stdout
import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from managed import control, pipeline
from managed.queue import canonical
from managed.tasks import Tasks
from test_tasks import Releases, request


class CompactStatusTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);(self.root/'tables').mkdir()
        (self.root/'tables'/'contacts.json').write_text('{"contacts":[]}')
        self.remote=Releases();self.ledger=control.Ledger(self.remote,self.root)
        self.addCleanup(self.ledger.tasks.close)
        self.config={'paused':True,'kinds':{'research':True,'shortnote':True},
                     'max_active':4,'kind_quota':'equal','followup_lane':True}

    def seed(self):
        for i,(kind,state) in enumerate((('research','running'),('shortnote','queued'),('research','failed'))):
            value=request('synthetic-'+str(i),kind=kind)
            value['inputs']={'pipeline':{'scenario':3 if kind=='shortnote' else 2},
                             'private_payload':'synthetic large prompt'}
            row=self.ledger.tasks.add(value)
            if state!='queued':self.ledger.tasks.update(row['id'],row['revision'],state,
                {'execution_history':[{'prompt':'synthetic execution payload'}]})

    def test_summary_uses_aggregates_and_pipeline_without_full_task_reads(self):
        self.seed();tasks=self.ledger.tasks
        tasks.db.execute('INSERT INTO pipeline_sources VALUES(?,?,?,?,?)',('result-1','identity','{}',0,'synthetic-0'))
        tasks.db.execute('INSERT INTO pipeline_state VALUES(?,?)',('connector_errors','[{"connector":"synthetic","error":"unavailable"}]'))
        expected=pipeline.status(tasks,self.root,self.config)
        with patch.object(control,'ROOT',self.root), \
                patch.object(tasks,'all',side_effect=AssertionError('full task list')), \
                patch.object(tasks,'get',side_effect=AssertionError('full payload')):
            result=control.status_summary(tasks,self.config)
        self.assertEqual(result['counts'],{'failed':1,'queued':1,'running':1})
        self.assertEqual(result['counts_by_kind'],{'research':2,'shortnote':1})
        self.assertEqual(result['lanes']['research']['active'],1)
        self.assertEqual(result['lanes']['shortnote']['queued'],1)
        self.assertEqual(result['errors']['failed'],1)
        self.assertEqual(result['connector_errors'],[{'connector':'synthetic','error':'unavailable'}])
        for key in ('source_notes','derived_tasks','observed_results','new_results_waiting'):
            self.assertEqual(result['pipeline'][key],expected[key])
        self.assertNotIn('tasks',result)
        self.assertNotIn('synthetic execution payload',canonical(result))
        self.assertNotIn('synthetic large prompt',canonical(result))

    def test_cli_compact_is_available_and_full_status_remains_compatible(self):
        self.seed();self.ledger.save(force=True)
        for compact in (False,True):
            output=io.StringIO()
            with patch.object(control,'ROOT',self.root),patch.object(control,'Releases',return_value=self.remote), \
                    patch.object(control,'load_config',return_value=self.config),redirect_stdout(output):
                control._main(['status']+(['--compact'] if compact else []))
            result=json.loads(output.getvalue())
            self.assertEqual(result['counts'],{'failed':1,'queued':1,'running':1})
            if compact:
                self.assertNotIn('tasks',result)
                self.assertIn('counts_by_kind',result)
            else:
                self.assertEqual(result['tasks'],self.ledger.tasks.all())

    def test_large_ledger_restore_and_summary_use_bounded_memory(self):
        # 6,400 synthetic task payloads occupy about 424 MiB. A child process
        # measures the complete restore/hash/summary path with a fresh RSS peak.
        path=self.root/'large.sqlite3';tasks=Tasks(path)
        try:
            value=canonical(request())
            data=canonical({'execution_history':[{'prompt':'x'*65536}]})
            with tasks.tx():
                tasks.db.executemany('INSERT INTO tasks VALUES(?,?,?,?,?,?,?,?,?)',
                    ((f'job-{i}',value,str(i),'synthetic-owner','research','queued',1,data,0) for i in range(6400)))
        finally:tasks.close()
        self.assertGreater(path.stat().st_size,400*1024*1024)
        with path.open('rb') as stream:sha=hashlib.file_digest(stream,'sha256').hexdigest()
        script='''
import json, resource, shutil, sys, tempfile, tracemalloc
from pathlib import Path
from unittest.mock import patch
from managed import control
class LocalRelease:
    def release(self):
        return {'body':json.dumps({'schema_version':1,'generation':1,
            'snapshot':{'name':'large.sqlite3','sha256':sys.argv[3]}}), 'assets':[]}
    def download(self,name,folder):
        path=Path(folder)/name;shutil.copyfile(sys.argv[1],path);return path
tracemalloc.start()
with tempfile.TemporaryDirectory() as folder, patch.object(Path,'read_bytes',side_effect=AssertionError('whole file read')):
    ledger=control.Ledger(LocalRelease(),folder)
    try:
        with patch.object(control,'ROOT',Path(sys.argv[2])), \\
                patch.object(ledger.tasks,'all',side_effect=AssertionError('all')), \\
                patch.object(ledger.tasks,'get',side_effect=AssertionError('get')):
            result=control.status_summary(ledger.tasks,{'paused':True,'kinds':{'research':True},'max_active':4,'kind_quota':'equal'})
        assert result['counts']=={'queued':6400}
        assert result['lanes']['research']['queued']==6400
        print(json.dumps({'python_peak':tracemalloc.get_traced_memory()[1],
                          'rss':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                          'output_bytes':len(json.dumps(result))}))
    finally:ledger.tasks.close()
'''
        run=subprocess.run([sys.executable,'-c',script,str(path),str(self.root),sha],capture_output=True,text=True,timeout=60)
        self.assertEqual(run.returncode,0,run.stderr)
        measured=json.loads(run.stdout)
        print('compact-status measurement '+json.dumps({'ledger_bytes':path.stat().st_size,**measured}),file=sys.stderr)
        self.assertLess(measured['python_peak'],48*1024*1024,measured)
        rss=measured['rss'] if sys.platform=='darwin' else measured['rss']*1024
        self.assertLess(rss,128*1024*1024,measured)
        self.assertLess(measured['output_bytes'],4096,measured)


if __name__=='__main__':unittest.main()
