"""Private log limits retain diagnostics without changing public output."""
from contextlib import redirect_stderr, redirect_stdout
import io
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from managed import control, run_log


class LogRetentionTests(unittest.TestCase):
    def test_upload_reserves_three_tick_logs_and_three_runs_before_upload(self):
        with tempfile.TemporaryDirectory() as folder,patch.object(control,'Releases') as releases:
            path=Path(folder)/'log';path.write_text('synthetic')
            for tick,keep in ((True,3),(False,3)):
                run_log._upload(path,tick)
                releases.return_value.upload_retained.assert_called_with(path)
                releases.return_value.prune_logs.assert_called_with(keep,tick=tick,incoming=path.name)

    def test_actual_pruning_deletes_only_old_logs_of_the_requested_type(self):
        class Logs(control.Releases):
            def __init__(self):super().__init__();self.deleted=[]
            def release(self):return {'id':1}
            def run(self,*args,**kwargs):
                if args[:3]==('api','-X','DELETE'):
                    self.deleted.append(int(args[-1].rsplit('/',1)[1]));return b''
                prefix='tick-log' if 'tick-log' in args[-1] else 'run-log'
                return '\n'.join(f'{i}\t{prefix}-{i:03}.log' for i in reversed(range(30))).encode()
        for tick,keep in ((True,3),(False,3)):
            releases=Logs();releases.prune_logs(keep,tick=tick)
            self.assertEqual(releases.deleted,list(range(30-keep)))

    def test_large_python_and_native_capture_retains_head_tail_and_caps_bytes(self):
        public=io.StringIO();captured=[];secret='synthetic-secret-token'
        def entry():
            print('BEGIN '+secret,flush=True)
            for _ in range(80):os.write(1,b'x'*65536)
            print('END '+secret)
        with patch.dict(os.environ,{'OUTREACH_PUBLIC_LOG':'1','GH_TOKEN':secret}), \
                patch.object(run_log,'_upload',side_effect=lambda path,tick:captured.append(path.read_bytes())), \
                redirect_stdout(public),redirect_stderr(public):
            self.assertEqual(run_log.public_run(entry,'synthetic',tick=True),0)
        value=captured[0]
        self.assertLessEqual(len(value),2*1024*1024)
        self.assertIn(b'BEGIN ',value);self.assertTrue(value.endswith(b'\n'))
        self.assertIn(b'END ',value);self.assertIn(run_log.TRUNCATED,value)
        self.assertNotIn(secret.encode(),value)
        self.assertRegex(public.getvalue(),r'^synthetic complete [\d.]+s\n$')

    def test_secret_crossing_either_excerpt_boundary_is_masked_before_cut(self):
        limit=1024;half=(limit-len(run_log.TRUNCATED))//2
        secret='unique-boundary-secret'
        with tempfile.TemporaryDirectory() as folder:
            raw=Path(folder)/'raw'
            value=b'A'*(half-8)+secret.encode()+b'x'*3000+secret.encode()+b'Z'*(half-8)
            raw.write_bytes(value)
            excerpt=run_log._excerpt(raw,limit,{secret})
        self.assertLessEqual(len(excerpt),limit)
        self.assertNotIn(b'unique-b',excerpt)
        self.assertNotIn(b'y-secret',excerpt)

    def test_utf8_capture_stays_valid_and_failure_keeps_final_traceback(self):
        public=io.StringIO();captured=[]
        def entry():
            print('BEGIN '+'\u03bb'*1200000)
            raise ValueError('synthetic final failure')
        with patch.dict(os.environ,{'OUTREACH_PUBLIC_LOG':'1'}), \
                patch.object(run_log,'_upload',side_effect=lambda path,tick:captured.append(path.read_bytes())), \
                redirect_stdout(public),redirect_stderr(public):
            self.assertEqual(run_log.public_run(entry,'synthetic'),1)
        text=captured[0].decode('utf-8')
        self.assertLessEqual(len(captured[0]),2*1024*1024)
        self.assertIn('BEGIN ',text);self.assertIn('Traceback',text)
        self.assertIn('ValueError: synthetic final failure',text)
        self.assertNotIn('synthetic final failure',public.getvalue())


if __name__=='__main__':unittest.main()
