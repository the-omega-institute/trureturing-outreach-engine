"""Release-backed, single-writer coordinator for CMA work and Claude reviews.

Producers only upload immutable requests. The GitHub Actions concurrency group
is the live writer; local commands inspect, stage or sign, never race that writer.
Every external-call intent is checkpointed before the call. Mail requires an
activated, reconciled adapter; public publication/Mathesis submission is separate.
"""
import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import hmac
import json
import math
import os
import re
from pathlib import Path
import subprocess
import sys
import shutil
import sqlite3
import tempfile
import time
import zipfile
from urllib.error import HTTPError, URLError

from .cma import CMA, output_text
from .correspondence import enabled as correspondence_priority, linked_contacts
from .github_diagnostics import stderr_failure as github_failure
from .queue import canonical, digest, require
from .mail_domains import home_domains
from .tasks import (ACTIVE, KINDS, Tasks, POLICY_IDENTITY_MISMATCH, WORKFLOW_CHANGED, WORKFLOW_PIN_MISMATCH, matches_workflow,
                    owners, policy_mismatch_hold, unstarted, validate_request, validate_review_edit, validate_review_rebind)
from .registry import email, firstcontact_gate
from .thread_staging import effective_inputs, LEGACY_RESOLUTION_REASON
from .tick import Tick, artifact_io_budget, artifact_io_timeout, measure, phase, unit, units
from .lanes import at_share, interleave, lane as lane_of, summary as lane_summary, validate as validate_lanes
from . import note_closure, note_revisions, recheck
from .formalization_requests import BACKFILL_PREFIX
from .prior_work_issues import BACKFILL_PREFIX as PRIOR_WORK_BACKFILL

from .location import DATA_ROOT as ROOT, DATA_REPO as REPO, DATA_BRANCH, ENGINE_ROOT, ENGINE_REPO, load_config, settings, source_head
from .policy import committed_text, policy_text, verify_commit

TAG='outreach-control'
MAIL_PACKET_MAX_BYTES=25*1024*1024
RESULT_REPAIR_LIMIT=2
WORKFLOW_REPIN_LIMIT=8
WORKFLOW_UNAVAILABLE_REPIN_LIMIT=64
TRANSIENT_HTTP={401,408,429,500,502,503,504}
RESOLUTION_RETRIES=8
RESULT_REPAIR_REASON='Completed CMA result needs repair: '
ORACLE_CAPACITY_MAX_AGE=15*60
ORACLE_CAPACITY_STATUS_KEY='oracle_capacity'
ORACLE_CAPACITY_UNSET=object()


def _oracle_time(value):
    if isinstance(value,datetime):
        result=value
    elif isinstance(value,(int,float)):
        result=datetime.fromtimestamp(value,timezone.utc)
    elif isinstance(value,str):
        result=datetime.fromisoformat(value.replace('Z','+00:00'))
    else:
        return None
    return result.astimezone(timezone.utc) if result.tzinfo else result.replace(tzinfo=timezone.utc)


def _oracle_iso(value):
    return _oracle_time(value).isoformat(timespec='seconds').replace('+00:00','Z')


def _oracle_number(value):
    return value if isinstance(value,int) and not isinstance(value,bool) and value>=0 else None


def _oracle_workers(payload):
    value=payload.get('workers') if isinstance(payload,dict) else None
    if isinstance(value,list):return value
    if isinstance(value,dict):
        for key in ('items','workers'):
            if isinstance(value.get(key),list):return value[key]
        if all(isinstance(item,dict) for item in value.values()):return list(value.values())
    for key in ('active_workers','items','worker_statuses','worker_list'):
        value=payload.get(key) if isinstance(payload,dict) else None
        if isinstance(value,list):return value
        if isinstance(value,dict) and all(isinstance(item,dict) for item in value.values()):return list(value.values())
    nested=payload.get('data') if isinstance(payload,dict) else None
    if isinstance(nested,dict) and nested is not payload:return _oracle_workers(nested)
    return []


def _oracle_field(worker,keys):
    for key in keys:
        if key in worker:return worker[key]
    return None


def summarize_oracle_capacity(payload,now=None):
    """Reduce one pool response to the safe, prompt-facing capacity snapshot."""
    if not isinstance(payload,dict):raise ValueError('Oracle pool status is not an object')
    now=_oracle_time(datetime.now(timezone.utc) if now is None else now)
    workers=_oracle_workers(payload)
    available=cooling=0;recoveries=[]
    for worker in workers:
        if not isinstance(worker,dict):continue
        cooldown=_oracle_time(_oracle_field(worker,('cooldown_until','cooldownUntil','cooldown_end','cooldown_expires_at')))
        cooling_now=bool(cooldown and cooldown>now)
        if cooling_now:
            cooling+=1;recoveries.append(cooldown)
        seen_flag=_oracle_field(worker,('seen_recently','recently_seen'))
        seen_ago=_oracle_field(worker,('last_seen_secs_ago',))  # measured broker field (2026-11-02)
        seen=_oracle_time(_oracle_field(worker,('seen_at','last_seen_at','last_seen','updated_at','heartbeat_at')))
        recent=(seen_flag if isinstance(seen_flag,bool) else
                0 <= seen_ago <= ORACLE_CAPACITY_MAX_AGE if isinstance(seen_ago,(int,float)) else
                seen is None or -60 <= (now-seen).total_seconds() <= ORACLE_CAPACITY_MAX_AGE)
        if not cooling_now and recent:available+=1
    total=len(workers)
    if not workers:
        source=payload.get('data') if isinstance(payload.get('data'),dict) else payload
        summary=source.get('workers') if isinstance(source.get('workers'),dict) else source
        total=next((value for key in ('workers','worker_count','total_workers','total')
                    if (value:=_oracle_number(summary.get(key))) is not None),0)
        available=next((value for key in ('available','available_workers')
                        if (value:=_oracle_number(summary.get(key))) is not None),0)
        cooling=next((value for key in ('cooling','cooling_workers','cooldown_workers')
                      if (value:=_oracle_number(summary.get(key))) is not None),0)
        recovery=summary.get('earliest_recovery') or source.get('earliest_recovery')
        if recovery:
            parsed=_oracle_time(recovery)
            if parsed and parsed>now:recoveries.append(parsed)
    source=payload.get('data') if isinstance(payload.get('data'),dict) else payload
    queued=next((value for key in ('queued','queue_depth','queue_length','queue_count','queued_tasks','pending','pending_count')
                 if (value:=_oracle_number(source.get(key))) is not None),None)
    queue=source.get('queue')
    if queued is None:queued=_oracle_number(queue)
    if queued is None and isinstance(queue,dict):
        queued=next((value for key in ('queued','depth','length','count','pending')
                     if (value:=_oracle_number(queue.get(key))) is not None),None)
    if queued is None:queued=0
    total=max(total,available+cooling)
    return {'checked_at':_oracle_iso(now),'workers':total,'available':available,
            'cooling':cooling,'earliest_recovery':_oracle_iso(min(recoveries)) if recoveries else None,
            'queued':queued}


def oracle_capacity_constrained(snapshot):
    return bool(snapshot and (snapshot.get('available',0)<3 or
                              (snapshot.get('workers',0)>0 and snapshot.get('cooling',0)*2>snapshot['workers'])))


def oracle_capacity_line(snapshot):
    if not snapshot:return ''
    available=snapshot.get('available',0);workers=snapshot.get('workers',0)
    cooling=snapshot.get('cooling',0);queued=snapshot.get('queued',0)
    line=('COORDINATOR ORACLE CAPACITY: {}/{} workers available; {} cooling; {} queued.'
          .format(available,workers,cooling,queued))
    if oracle_capacity_constrained(snapshot):
        recovery=snapshot.get('earliest_recovery') or 'the next capacity check'
        line+=(' Oracle capacity is constrained until {}: make at most one short Oracle attempt per required step; '
               'for note/reply gap checks use one independent Codex referee pass instead and record it; '
               'for a required journal-editor assessment, do not hold the round — complete the round, '
               'mark editor_assessment pending in the evidence and self_audit, and the coordinator will request it in a later round.'
               .format(recovery))
    return line


def oracle_capacity_snapshot(tasks):
    row=tasks.db.execute('SELECT payload FROM pipeline_state WHERE key=?',(ORACLE_CAPACITY_STATUS_KEY,)).fetchone()
    if not row:return None
    try:value=json.loads(row[0])
    except (TypeError,ValueError):return None
    return value if isinstance(value,dict) else None


def refresh_oracle_capacity(ledger,fetcher=None,now=None):
    """Best-effort status read; an error preserves the last safe snapshot."""
    try:
        from .credentials import oracle_pool_status
        payload=oracle_pool_status(fetcher)
        if payload is None:return None
        snapshot=summarize_oracle_capacity(payload,now=now)
    except Exception:
        return None
    ledger.tasks.db.execute('INSERT OR REPLACE INTO pipeline_state VALUES(?,?)',
                            (ORACLE_CAPACITY_STATUS_KEY,canonical(snapshot)))
    return snapshot


class Releases:
    def __init__(self,repo=REPO,tag=TAG):self.repo=repo;self.tag=tag
    # Read-only release calls are retried on transient GitHub 5xx errors: one
    # asset download HTTP 500 aborted a whole tick on 2026-11-07. Writes are
    # never retried here, so an upload can never be duplicated.
    READ_ONLY=(('release','download'),('release','view'))
    RETRY_DELAYS=(2,5)

    def run(self,*args,missing=False):
        done=subprocess.run(['gh',*args],capture_output=True,timeout=artifact_io_timeout())
        for delay in (self.RETRY_DELAYS if tuple(args[:2]) in self.READ_ONLY else ()):
            if not done.returncode or not re.search(rb'HTTP 5\d\d',done.stderr):break
            time.sleep(artifact_io_timeout(delay))
            done=subprocess.run(['gh',*args],capture_output=True,timeout=artifact_io_timeout())
        if done.returncode:
            if missing and b'(HTTP 404)' in done.stderr:return None
            stderr = done.stderr.decode(errors='replace')
            error = RuntimeError('GitHub release operation failed: ' + stderr[-500:])
            error.github_diagnostic = github_failure(stderr, args)
            raise error
        return done.stdout
    def release(self):
        raw=self.run('api',f'repos/{self.repo}/releases/tags/{self.tag}',missing=True)
        return json.loads(raw) if raw else None
    def ensure(self):
        if self.release() is None:
            self.run('release','create',self.tag,'--repo',self.repo,'--title','Private Outreach task ledger',
                     '--notes',canonical({'schema_version':1,'snapshot':None}),'--target',DATA_BRANCH,'--prerelease','--latest=false')
    def require_private(self):
        raw = self.run('api', f'repos/{self.repo}', '--jq', '.private')
        require(raw.strip() == b'true', 'Private data operations require a private repository')

    def prune_logs(self, keep, *, tick, incoming=None):
        from .run_log import log_run_id, log_workflow_id
        release = self.release()
        if release is None:return 0
        prefix = 'tick-log-' if tick else 'run-log-'
        query = f'.[]|select(.name|startswith("{prefix}"))|[.id,.name]|@tsv'
        raw = self.run('api', '--paginate', f'repos/{self.repo}/releases/{release["id"]}/assets',
                       '--jq', query)
        assets = sorted((line.split('	') for line in raw.decode().splitlines() if line),
                        key=lambda row: row[1])
        names=[row[1] for row in assets]
        if incoming:names.append(incoming)
        # Tick files have one window; step logs have a run window per workflow.
        retained={}
        for name in sorted(names,reverse=True):
            workflow=None if tick else log_workflow_id(name)
            group=name if tick else log_run_id(name)
            # Bound names also advance the old shared window, so historical
            # logs without workflow metadata age out rather than live forever.
            for scope in ((workflow,None) if workflow is not None else (None,)):
                groups=retained.setdefault(scope,set())
                if len(groups)<keep:groups.add(group)
        victims=[row for row in assets if (row[1] if tick else log_run_id(row[1])) not in
                 retained[None if tick else log_workflow_id(row[1])]]
        for asset_id, _ in victims:
            self.run('api', '-X', 'DELETE', f'repos/{self.repo}/releases/assets/{asset_id}')
        return len(victims)

    def evict_oldest_log(self,protect=()):
        """At capacity, sacrifice one diagnostic before durable input assets."""
        rel=self.release()
        if rel is None:return 0
        raw=self.run('api','--paginate',f'repos/{self.repo}/releases/{rel["id"]}/assets',
                     '--jq','.[]|[.id,.name,.created_at]|@tsv')
        rows=(line.split('\t') for line in raw.decode().splitlines() if line)
        oldest=min((row for row in rows if row[1].startswith(('tick-log-','run-log-')) and row[1] not in protect),
                   key=lambda row:(row[2],row[1]),default=None)
        if oldest is None:return 0
        self.run('api','-X','DELETE',f'repos/{self.repo}/releases/assets/{oldest[0]}')
        return 1

    def fetch_workflow_revision(self, revision, *, engine=False):
        """Restore a historical pin into the checkout that owns it."""
        try:
            result = subprocess.run(
                ['git', 'fetch', '--depth=1', '--no-tags', '--no-write-fetch-head', 'origin', revision],
                cwd=ENGINE_ROOT if engine else ROOT,
                env={**os.environ, 'GIT_TERMINAL_PROMPT': '0'},
                capture_output=True, timeout=5)
            return result.returncode == 0
        except (OSError, subprocess.TimeoutExpired):
            return False

    def upload(self,path):
        # Duplicate content is a replay, never a clobber of a prior request.
        rel=self.release();old=next((a for a in rel['assets'] if a['name']==path.name),None)
        if old:
            with path.open('rb') as stream:sha=hashlib.file_digest(stream,'sha256').hexdigest()
            require(old.get('digest')=='sha256:'+sha,'Existing immutable asset differs')
            return
        self.run('release','upload',self.tag,str(path),'--repo',self.repo)

    def upload_retained(self,path,protect=()):
        """Retry only an explicit capacity rejection, preserving pending work."""
        from .run_log import TICK_LOGS_KEPT, RUN_LOGS_KEPT, log_name
        try:self.upload(path)
        except RuntimeError as exc:
            if 'file_count limited' not in str(exc):raise
            # Stop once a cleanup frees space; an optional later cleanup must
            # not prevent a now-possible checkpoint or diagnostics upload.
            incoming=path.name if path.name.startswith('run-log-') else log_name()
            for cleanup in (lambda:self.prune_logs(TICK_LOGS_KEPT,tick=True,
                                                  incoming=path.name if path.name.startswith('tick-log-') else None),
                            lambda:self.prune_logs(RUN_LOGS_KEPT,tick=False,incoming=incoming),
                            lambda:self.prune_snapshots(SNAPSHOTS_KEPT,{path.name,*protect}),
                            lambda:self.evict_oldest_log({path.name,*protect}),
                            self.prune_ingested):
                if cleanup():break
            self.upload(path)
    def snapshot_rows(self,rel):
        raw=self.run('api','--paginate',f'repos/{self.repo}/releases/{rel["id"]}/assets',
                     '--jq','.[]|select(.name|startswith("state-"))|[.id,.name]|@tsv')
        return [line.split('\t') for line in raw.decode().splitlines() if line.strip()]

    def prune_snapshots(self,keep,protect=()):
        """Delete superseded snapshots while preserving the committed chain."""
        rel=self.release()
        if rel is None:return 0
        try:pointer=json.loads(rel.get('body') or '{}')
        except (TypeError,ValueError):return 0
        if not isinstance(pointer,dict) or pointer.get('schema_version')!=1:return 0
        if not isinstance(pointer.get('snapshot'),dict):return 0
        current=(pointer.get('snapshot') or {}).get('name')
        if not current:return 0
        rows=self.snapshot_rows(rel)
        protected=set(protect)
        protected.add(current)
        protected.update(item['name'] for item in pointer.get('run_history',[])[:max(keep-1,0)])
        victims=[r for r in rows if r[1] not in protected]
        deleted=0
        for asset_id,_ in units(self,'snapshot-prune',victims,key=lambda r:r[0]):
            self.run('api','-X','DELETE',f'repos/{self.repo}/releases/assets/{asset_id}');deleted+=1
        return deleted
    def snapshot_receipts(self,snapshot):
        """Read receipts from verified immutable bytes, without migrating them."""
        with tempfile.TemporaryDirectory() as folder:
            path=self.download(snapshot['name'],folder)
            with path.open('rb') as stream:
                require(hashlib.file_digest(stream,'sha256').hexdigest()==snapshot['sha256'],
                        'Control snapshot mismatch')
            db=sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)
            try:
                if not db.execute("SELECT 1 FROM sqlite_master WHERE name='input_asset_receipts'").fetchone():return {}
                return dict(db.execute('SELECT identity,generation FROM input_asset_receipts'))
            finally:db.close()

    def prune_ingested(self,receipts=None):
        """Delete only identities receipted in the oldest retained run final."""
        rel=self.release()
        if rel is None:return 0
        pointer=json.loads(rel.get('body') or '{}')
        if not pointer.get('snapshot'):return 0
        history=pointer.get('run_history',[])
        if len(history)<SNAPSHOTS_KEPT-1:return 0
        if receipts is None:receipts=self.snapshot_receipts(pointer['snapshot'])
        if not receipts:return 0
        oldest_generation=history[-1].get('generation')
        if oldest_generation is not None:
            receipts={identity:first for identity,first in receipts.items() if first<=oldest_generation}
            if not receipts:return 0
        oldest=self.snapshot_receipts(history[-1])
        covered={identity for identity,first in receipts.items() if oldest.get(identity)==first}
        if not covered:return 0
        raw=self.run('api','--paginate',f'repos/{self.repo}/releases/{rel["id"]}/assets',
                     '--jq','.[]|[.id,.name,.created_at,(.digest // "")]|@tsv')
        victims=[]
        for line in raw.decode().splitlines():
            if not line.strip():continue
            asset_id,name,created,asset_digest=(line.split('\t')+['',''])[:4]
            identity=input_identity({'id':asset_id,'name':name,'digest':asset_digest})
            if name.startswith(INPUT_PREFIXES) and name.endswith('.json') and identity in covered:
                victims.append((asset_id,name))
        deleted=0
        for asset_id,_ in units(self,'input-prune',victims,key=lambda r:r[0]):
            self.run('api','-X','DELETE',f'repos/{self.repo}/releases/assets/{asset_id}');deleted+=1
        return deleted
    def download(self,name,folder):
        self.run('release','download',self.tag,'--repo',self.repo,'--pattern',name,'--dir',str(folder))
        return Path(folder)/name
    def create_task_release(self,tag):
        self.run('release','create',tag,'--repo',self.repo,'--title',tag,
                 '--notes','Private task deliverables; no scientific publication.',
                 '--target',DATA_BRANCH,'--prerelease','--latest=false')
    def task_assets(self,tag):
        raw=self.run('api',f'repos/{self.repo}/releases/tags/{tag}','--jq','.assets',missing=True)
        return {asset['name']:asset for asset in json.loads(raw)} if raw is not None else None
    def upload_task_batch(self,paths,tag,listings):
        if tag not in listings:
            exists=self.run('api',f'repos/{self.repo}/releases/tags/{tag}','--jq','.id',missing=True)
            listings[tag]=self.task_assets(tag) if exists is not None else None
        old=listings[tag]
        if old is None:
            self.create_task_release(tag)
        # A previous attempt may have uploaded some files before failing.
        # Leave those immutable and let the caller verify every asset.
        missing=[str(path) for path in paths if path.name not in (old or {})]
        if missing:
            del listings[tag]
            self.run('release','upload',tag,*missing,'--repo',self.repo)
            listings[tag]=self.task_assets(tag)
    def preserve_task(self,path,tag):
        rel=self.run('api',f'repos/{self.repo}/releases/tags/{tag}',missing=True)
        if rel is None:
            self.create_task_release(tag)
            rel=self.run('api',f'repos/{self.repo}/releases/tags/{tag}')
        old=next((a for a in json.loads(rel)['assets'] if a['name']==path.name),None)
        if old:
            require(old.get('digest')=='sha256:'+hashlib.sha256(path.read_bytes()).hexdigest(),
                    'Existing immutable task artifact differs')
            return
        self.run('release','upload',tag,str(path),'--repo',self.repo)
    def pointer(self,pointer,folder):
        path=Path(folder)/'pointer.json';path.write_text(canonical(pointer))
        self.run('release','edit',self.tag,'--repo',self.repo,'--notes-file',str(path))


SNAPSHOTS_KEPT=3
INPUT_PREFIXES=('request-','local-result-','submission-review-','review-','decision-','draft-revision-','mathesis-','prompt-requeue-','failed-requeue-','pool-adopt-',BACKFILL_PREFIX+'-',PRIOR_WORK_BACKFILL+'-')
SAVE_INTERVAL=45
INGEST_REJECTION_LIMIT=3


def ingest_rejections(tasks):
    row=tasks.db.execute("SELECT payload FROM pipeline_state WHERE key='ingest_rejected'").fetchone()
    return json.loads(row[0]) if row else {}


def given_up_digests(rejections):
    return {sha for sha,entry in rejections.items() if entry['attempts']>=INGEST_REJECTION_LIMIT}


def input_identity(asset):
    # A new asset (even with equal bytes or a reused name) needs its own receipt.
    return canonical([str(asset.get('id','')),asset['name'],asset.get('digest')])


class Ledger:
    def __init__(self,releases,folder,clock=time.monotonic):
        from .run_log import coordinator_run_id
        self.releases=releases;self.folder=Path(folder)
        self.clock=clock;self.last_saved_at=None;self.saved_version=None
        self.run_id=coordinator_run_id();self.published_run=None;self.run_history=[]
        self.connector_errors=[];self.snapshot=None
        rel=releases.release();self.generation=0;self.current=None
        if rel:
            pointer=json.loads(rel.get('body') or '{}')
            require(pointer.get('schema_version')==1,'Unknown control ledger')
            snap=pointer.get('snapshot')
            if snap:
                path=releases.download(snap['name'],self.folder)
                with path.open('rb') as stream:
                    require(hashlib.file_digest(stream,'sha256').hexdigest()==snap['sha256'],'Control snapshot mismatch')
                path.rename(self.folder/'state.sqlite3')
                self.generation=pointer['generation'];self.current=snap['name']
                self.snapshot={**snap,'generation':self.generation,'run_id':pointer.get('run_id')}
                self.published_run=pointer.get('run_id')
                self.run_history=pointer.get('run_history',[])[:SNAPSHOTS_KEPT-1]
        self.tasks=Tasks(self.folder/'state.sqlite3')
    def version(self):
        # Queue writes use a second connection to this same SQLite file.
        # total_changes alone would miss its send-intent/send-call markers.
        return (self.tasks.db.total_changes,self.tasks.db.execute('PRAGMA data_version').fetchone()[0])
    def save(self,force=False):
        """Publish at most once per 45s for replayable local progress.

        force=True is mandatory at the end of a tick and BEFORE an external
        side effect: persist its intent, original handles and idempotency keys
        first. A forced failure must prevent the call. Replaying observations
        and deterministic derivations may use ordinary debounced saves.
        """
        version=self.version()
        if version==self.saved_version and self.published_run==self.run_id:return False
        if not force and self.last_saved_at is not None and self.clock()-self.last_saved_at<SAVE_INTERVAL:
            return False
        generation=self.generation+1
        path=self.folder/'next.sqlite3';self.tasks.snapshot(path)
        with path.open('rb') as stream:sha=hashlib.file_digest(stream,'sha256').hexdigest()
        target=path.with_name('state-'+sha+'.sqlite3')
        if target.name==self.current and self.published_run==self.run_id:
            # A fresh process has no saved_version yet. Repointing at the same
            # bytes within the same run adds no new checkpoint or coverage.
            path.unlink();self.saved_version=version;self.last_saved_at=self.clock()
            return False
        path.rename(target)
        upload=getattr(self.releases,'upload_retained',self.releases.upload)
        upload(target)
        history=self.run_history
        if self.snapshot and self.published_run!=self.run_id:
            # The previous live pointer is the last committed snapshot of that
            # run, including a run interrupted before its final-save phase.
            history=[self.snapshot,*history][:SNAPSHOTS_KEPT-1]
        self.releases.pointer({'schema_version':1,'generation':generation,
            'snapshot':{'name':target.name,'sha256':sha},
            'run_id':self.run_id,'run_history':history},self.folder)
        self.generation=generation;self.current=target.name
        self.snapshot={'name':target.name,'sha256':sha,'generation':generation,'run_id':self.run_id}
        self.published_run=self.run_id;self.run_history=history
        self.saved_version=version;self.last_saved_at=self.clock()
        try:target.unlink()
        except OSError:pass  # Local cleanup cannot invalidate a committed save.
        if hasattr(self.releases,'prune_snapshots'):
            # Part of publication, outside the optional maintenance deadline.
            try:self.releases.prune_snapshots(SNAPSHOTS_KEPT,{self.current})
            except Exception as exc:
                # The pointer is already live. Maintenance cannot turn a
                # durable intent into a failed save and skip the following call.
                try:
                    self.connector_errors.append({'connector':'snapshot-prune','error':str(exc)[:300]})
                    self.tasks.db.execute('INSERT OR REPLACE INTO pipeline_state VALUES(?,?)',
                                          ('connector_errors',canonical(self.connector_errors)))
                except Exception:pass
                try:print('connector-error snapshot-prune '+type(exc).__name__,file=sys.stderr)
                except Exception:pass
        return True

    def ingestion_receipts(self):
        return dict(self.tasks.db.execute('SELECT identity,generation FROM input_asset_receipts'))

    def receipt_input(self,asset):
        self.tasks.db.execute('INSERT OR IGNORE INTO input_asset_receipts VALUES(?,?)',
                              (input_identity(asset),self.generation+1))

    def save_ingest_rejections(self,rejections):
        if rejections:
            self.tasks.db.execute('INSERT OR REPLACE INTO pipeline_state VALUES(?,?)',
                                  ('ingest_rejected',canonical(rejections)))
        else:
            self.tasks.db.execute("DELETE FROM pipeline_state WHERE key='ingest_rejected'")
    def preserve_review_packet(self, artifact, raw):
        with tempfile.TemporaryDirectory(prefix='review-packet-',dir=self.folder) as temp:
            path=Path(temp)/artifact['name']
            path.write_bytes(raw)
            self.releases.preserve_task(path,artifact['release_tag'])
            verify_artifacts({'packet':{'artifact':artifact['name']},'artifacts':[artifact]},self.releases,temp)

    def ingest(self,review_secret=None):
        with unit(self,'setup','ingest-list') as ready:
            if not ready:
                self.save(force=True);return []
            rel=self.releases.release()
        errors=[]
        prefixes=INPUT_PREFIXES
        assets=[a for a in rel['assets'] if a['name'].startswith(prefixes) and a['name'].endswith('.json')]
        rejections=ingest_rejections(self.tasks)
        present={a.get('digest') for a in assets}
        retained={sha:entry for sha,entry in rejections.items() if sha in present}
        if retained!=rejections:self.save_ingest_rejections(retained)
        present_identities={input_identity(a) for a in assets}
        known=self.ingestion_receipts()
        self.tasks.db.executemany('DELETE FROM input_asset_receipts WHERE identity=?',
                                  ((identity,) for identity in known if identity not in present_identities))
        # Legacy snapshots have no receipts. Give-ups remain authoritative;
        # seed fresh identity coverage without retrying or changing counters.
        given_up=given_up_digests(retained)
        for asset in assets:
            identity=input_identity(asset)
            if identity not in known and asset.get('digest') in given_up:
                self.receipt_input(asset);known[identity]=self.generation+1
        assets=[a for a in assets if input_identity(a) not in known]
        ordered=sorted(assets,key=lambda a:(next(i for i,p in enumerate(prefixes) if a['name'].startswith(p)),a['name']))
        failed_this_tick=set()
        for asset in units(self,'ingest',ordered,key=lambda a:digest(a['name'])):
            name=asset['name']
            asset_digest=asset.get('digest')
            if asset_digest in failed_this_tick:continue
            path=self.folder/name
            try:
                self.releases.download(name,self.folder)
                require(asset_digest=='sha256:'+hashlib.sha256(path.read_bytes()).hexdigest(),'Queue asset mismatch')
                value=json.loads(path.read_text());path.unlink()
                if name.startswith('request-'):
                    from .thread_staging import require_uncovered_inbound
                    validate_request(value)
                    firstcontact_gate(value,ROOT)
                    require_uncovered_inbound(value,self.tasks)
                    self.tasks.add(value)
                elif name.startswith('prompt-requeue-'):
                    requeue_large_task(self,value)
                elif name.startswith('failed-requeue-'):
                    requeue_failed_task(self,value,review_secret)
                elif name.startswith('pool-adopt-'):
                    adopt_pool_sandbox(self,value,review_secret)
                elif name.startswith('local-result-'):
                    from .local_run import ingest
                    ingest(self,value,review_secret)
                elif review_secret:
                    if name.startswith('submission-review-'):self.tasks.request_submission_review(value,review_secret)
                    elif name.startswith('decision-'):self.tasks.decision(value,review_secret)
                    elif name.startswith('draft-revision-'):self.tasks.draft_revision(value,review_secret)
                    elif name.startswith(BACKFILL_PREFIX+'-'):
                        from .formalization_requests import ingest
                        ingest(self.tasks,value,review_secret)
                    elif name.startswith(PRIOR_WORK_BACKFILL+'-'):
                        from .prior_work_issues import ingest
                        ingest(self.tasks,value,review_secret)
                    elif name.startswith('mathesis-'):
                        from .mathesis_delivery import ingest, TEST_TAG
                        ingest(self.tasks,value,review_secret,test=getattr(self.releases,'tag',None)==TEST_TAG)
                    else:self.tasks.verdict(value,review_secret,preserve_packet=self.preserve_review_packet)
                else:continue  # No review key: this asset has not been consumed.
                self.receipt_input(asset)
            except (ValueError,KeyError,TypeError) as exc:
                error=str(exc)[:300]
                if asset_digest:
                    failed_this_tick.add(asset_digest)
                    attempts=retained.get(asset_digest,{}).get('attempts',0)+1
                    retained[asset_digest]={'attempts':attempts,'error':error}
                    self.save_ingest_rejections(retained)
                    if attempts>=INGEST_REJECTION_LIMIT:
                        self.receipt_input(asset)
                    if attempts==INGEST_REJECTION_LIMIT:
                        error+=f'; giving up after {INGEST_REJECTION_LIMIT} attempts'
                errors.append({'asset':name,'error':error})
        # Review/decision ingest must be durable before downstream execution or
        # cleanup; immutable input assets make a partially ingested batch replayable.
        self.save(force=True)
        return errors


def committed_workflow_text(root, revision, path):
    return committed_text(root, revision, path)


def workflow_text(path, revision=None, policy_revision=None):
    return policy_text(f'managed/{path}', data_root=ROOT, engine_root=ENGINE_ROOT,
                       source_revision=revision, policy_revision=policy_revision,
                       read_commit=committed_workflow_text)


def current_policy_revision():
    """Single-checkout installations retain the historical request format."""
    if not os.environ.get('OUTREACH_DATA_ROOT') or ROOT == ENGINE_ROOT:
        return None
    if not (ROOT / '.git').exists():
        raise ValueError('Task creation requires a separate Git checkout for private data')
    return source_head(ROOT)


def request_binding(kind, revision, policy_revision):
    if policy_revision is None:
        return {'source_revision': revision, 'workflow_hash': workflow(kind, revision)}
    return {'source_revision': revision, 'policy_revision': policy_revision,
            'workflow_hash': workflow(kind, revision, policy_revision)}


def legacy_workflow(kind, revision=None):
    """The pre-scoping formula, used only at the coordinator migration boundary."""
    if kind=='reply':paths=['replies/workflow.json','replies/review.md','firstcontact/review.md']
    elif kind=='daily':paths=['daily/workflow.json','daily/DESIGN-notes.md','firstcontact/review.md']
    elif kind=='firstcontact':paths=['firstcontact/workflow.json','firstcontact/review.md']
    elif kind=='submission':paths=['submission/workflow.json','submission/review.md','research/review.md','replies/workflow.json']
    else:paths=['research/workflow.json','research/review.md','replies/workflow.json']
    paths+=['automation.json','submission/author-policy.json','runtime.py']
    return digest({p:workflow_text(p, revision) for p in paths})


def workflow(kind, revision=None, policy_revision=None):
    # task_prompt() and the per-kind review contracts select these inputs.
    # Mail uses its own workflow/rubric plus the shared firstcontact rubric;
    # scientific tasks use research's rubric, and only submissions use author
    # policy. replies/workflow.json does not govern scientific-only output.
    # runtime.py builds scientific PDFs (including mail attachments); daily
    # uses its existing image producer. Operator-loop prose/activation guidance
    # is not an output policy. Bind the routing/continuation contract for all
    # CMA kinds, mail ownership/delivery only for mail, and publication guidance
    # only for the scientific packages or daily output that consumes it.
    if kind=='reply':paths=['replies/workflow.json','replies/review.md','firstcontact/review.md']
    elif kind=='daily':paths=['daily/workflow.json','daily/DESIGN-notes.md','firstcontact/review.md']
    elif kind=='firstcontact':paths=['firstcontact/workflow.json','firstcontact/review.md']
    elif kind=='submission':paths=['submission/workflow.json','submission/review.md','research/review.md',
                                   'submission/author-policy.json']
    else:paths=['research/workflow.json','research/review.md']
    if kind!='daily':paths.append('runtime.py')
    if policy_revision is not None:
        verify_commit(ENGINE_ROOT, revision)

    def read(path):
        if policy_revision is None:
            return workflow_text(path, revision)
        return workflow_text(path, revision, policy_revision)

    inputs = {p: read(p) for p in paths}
    if kind in {'reply', 'firstcontact'}:
        path = 'replies/voice.md'
        try:
            inputs[path] = read(path)
        except FileNotFoundError:
            # Historical mail pins can predate the voice guide.
            if revision is None or policy_revision is not None:
                raise
    automation = json.loads(read('automation.json'))
    selected={key:automation[key] for key in ('schema_version','routing_contract','continuation')}
    if kind in {'reply','firstcontact'}:selected['mail']=automation['mail']
    if kind=='daily':selected['publication']={'daily':automation['publication']['daily']}
    elif kind in {'research','shortnote','mathesis'}:
        selected['publication']={key:automation['publication'][key] for key in ('coverage','public_updates')}
    return digest({**inputs,'automation.json':selected})


def verify_request_workflow(request):
    pinned = request_binding(request['kind'], request['source_revision'], request.get('policy_revision'))
    require(request['workflow_hash'] == pinned['workflow_hash'],
            'Workflow hash does not match the pinned source revision; regenerate the request')


def pinned_workflow_mismatch(row, read_workflow):
    request = row['request']
    kind, revision = request['kind'], request['source_revision']
    recorded = request['workflow_hash']
    policy_revision = request.get('policy_revision')
    pinned = read_workflow(kind, revision, policy_revision) if policy_revision else read_workflow(kind, revision)
    if pinned is None:
        return None
    if recorded == pinned:
        return False
    # Old, migrated requests were issued under the earlier digest formula.
    binding = row['data'].get('workflow_binding', {})
    if policy_revision is None and binding.get('legacy_hash') == recorded:
        try:
            if recorded == legacy_workflow(kind, revision):
                return False
        except ValueError:
            pass
    return True


def migrate_workflows(tasks):
    """Pin compatible legacy rows once, before any coordinator effects.

    Store the old->scoped pair per row, rather than rewriting immutable requests
    or signed PASSes. A later edit changes the scoped hash, so this compatibility
    cannot bless a stale review. The existing migration table makes the deploy
    boundary durable, including when no rows match. Inspect commands never run it.
    """
    marker='scoped-workflows-v1'
    if tasks.db.execute('SELECT 1 FROM task_migrations WHERE name=?',(marker,)).fetchone():return 0
    rows=tasks.all()
    bindings={kind:{'legacy_hash':legacy_workflow(kind),'workflow_hash':workflow(kind)}
              for kind in {row['kind'] for row in rows}}
    count=0
    with tasks.tx():
        for row in rows:
            binding=bindings[row['kind']]
            if matches_workflow(row,binding['legacy_hash']):
                # Metadata only: preserve revision, timestamps and review-event
                # history so already signed pending verdicts still ingest.
                data={**row['data'],'workflow_binding':binding}
                tasks.db.execute('UPDATE tasks SET data=? WHERE id=?',(canonical(data),row['id']))
                count+=1
        tasks.db.execute('INSERT INTO task_migrations VALUES(?,?)',(marker,'done'))
    return count


def review_dry_run(envelope, releases):
    """Apply a signed verdict to a throwaway copy of the current ledger first.

    The coordinator ingests staged verdicts later and a rejected one used to
    disappear silently (2026-11-13: owner-draft rebinds and a PASS on an
    agent-held result). Nothing here is saved or uploaded.
    """
    from .automation import mail_routing_override
    with tempfile.TemporaryDirectory() as temp:
        ledger=Ledger(releases,temp)
        try:
            try:
                report = {}
                if 'edited_body' in envelope:
                    row = ledger.tasks.verdict(envelope,review_key(),preserve_packet=lambda artifact, raw: None)
                    report['reviewer_edit_valid'] = True
                else:
                    row = ledger.tasks.verdict(envelope,review_key())
                if envelope['verdict'] == 'PASS' and row['kind'] in {'reply', 'firstcontact'}:
                    config = load_config(ROOT)
                    override = mail_routing_override(row, home_domains(config))
                    if override is not None:
                        report['routing_override'] = override
                return report or None
            except ValueError as exc:
                raise ValueError('Review would be rejected by the coordinator: '+str(exc)) from exc
        finally:ledger.tasks.close()


def sign_review(envelope, secret, kind=None):
    require('signature' not in envelope, 'Envelope is already signed')
    validate_review_edit(envelope, kind)
    current = workflow(kind) if kind in KINDS else None
    if (current is not None and envelope.get('verdict') == 'PASS'
            and 'rebind_workflow_hash' not in envelope
            and envelope['binding']['workflow_hash'] != current):
        envelope = {**envelope, 'rebind_workflow_hash':current}
        print('sign-review: added rebind_workflow_hash for the current '+kind+' workflow', file=sys.stderr)
    validate_review_rebind(envelope)
    if 'rebind_workflow_hash' in envelope:
        require(kind in KINDS, 'Workflow rebind requires --kind from the bound task request')
        require(envelope['rebind_workflow_hash'] == current,
                'Workflow rebind must match the current workflow for the task kind')
    return {**envelope, 'signature': hmac.new(secret.encode(), canonical(envelope).encode(), hashlib.sha256).hexdigest()}


def submit_review(envelope, releases, folder):
    """Use the same live-ledger preflight for single and batch reviews."""
    preflight=review_dry_run(envelope, releases)
    return {**stage(envelope, 'review', releases, folder),**(preflight or {})}


def rebind_stale(snapshot, reason, releases, folder, dry_run=False):
    """Rebind unchanged PASSes selected from a complete work() inventory."""
    require(reason.strip(), 'Workflow rebind needs a reason')
    rows = {row['id']:row for row in snapshot.get('affairs_tasks', [])}
    candidates = {item['binding']['task_id']:item['binding']
                  for item in snapshot.get('owner_draft_rebind', []) + snapshot.get('mail_rebind', [])}
    for task_id, row in rows.items():
        if (row['kind'] in {'firstcontact','reply'}
                and row['state'] in {'approved','awaiting-mailbox','owner-draft'}
                and 'Mail workflow changed' in (row.get('reason') or '')):
            candidates.setdefault(task_id, (row.get('review') or {}).get('binding'))
    summary = {'signed':[], 'skipped':[]}
    if dry_run:summary['planned'] = []
    secret = None
    for task_id, binding in candidates.items():
        row = rows.get(task_id)
        review = (row.get('review') or {}) if row else {}
        skip = None
        if row is None:skip = 'Task missing from affairs_tasks'
        elif review.get('verdict') != 'PASS':skip = 'Stored review verdict is not PASS'
        elif (row.get('result') is None
              or (review.get('binding') or {}).get('result_hash') != digest(row['result'])
              or not binding or binding.get('result_hash') != digest(row['result'])):
            skip = 'Result hash changed or review binding is missing'
        elif (binding.get('task_id') != task_id
              or binding.get('workflow_hash') != row['request']['workflow_hash']):
            skip = 'Review binding does not match the task request'
        elif row.get('sent_receipt'):skip = 'Task already has a Sent receipt'
        elif not review.get('evidence_ref'):skip = 'Stored review evidence reference is missing'
        if skip:
            summary['skipped'].append({'task_id':task_id, 'reason':skip})
            continue
        current = workflow(row['kind'])
        if current in {review.get('rebind_workflow_hash'), binding['workflow_hash']}:
            summary['skipped'].append({'task_id':task_id, 'reason':'Review already binds the current workflow'})
            continue
        envelope = {'binding':binding, 'reviewer':'local-claude-loop', 'verdict':'PASS',
                    'reason':reason, 'evidence_ref':review['evidence_ref'],
                    'rebind_workflow_hash':current}
        if dry_run:
            summary['planned'].append(envelope)
            continue
        if secret is None:secret = review_key()
        signed = sign_review(envelope, secret, row['kind'])
        directory = ROOT.parent/'mail/outreach-maintenance/reviews'/('rebind-'+datetime.now(timezone.utc).date().isoformat())
        directory.mkdir(parents=True, exist_ok=True)
        path = directory/('review-'+digest(signed)+'.json')
        try:
            submitted = submit_review(signed, releases, folder)
        except ValueError as exc:
            # A row the coordinator would reject (e.g. a stale draft conflict)
            # is reported and skipped; the remaining rebinds still proceed.
            summary['skipped'].append({'task_id':task_id, 'reason':str(exc)[:300]})
            continue
        path.write_text(canonical(signed)+'\n')
        summary['signed'].append({'task_id':task_id, 'path':str(path), 'submission':submitted})
    return summary


PROMPT_MAX_CHARACTERS=120000


def prompt_budget(config):
    value=config.get('prompt_max_characters',PROMPT_MAX_CHARACTERS)
    require(type(value) is int and value>0,'Invalid prompt_max_characters')
    return value


def prepare_task_prompt(row,releases,folder,config,oracle_capacity=None):
    """Project large inputs into verified private assets, never changing the request."""
    import copy
    projected=copy.deepcopy(row)
    github=config.get('github')
    budget=prompt_budget(config)
    original=task_prompt(row,github,oracle_capacity)
    if len(original)<=budget:return original
    candidates=[]
    for root in [('request','inputs'),('data',)]:
        obj=projected
        for key in root:obj=obj.get(key,{})
        for key,value in obj.items():
            if key in {'previous_result','thread','thread_inputs','mailbox_history','prior_context',
                       'previous_review','review','review_feedback','review_artifacts','artifacts','feedback',
                       'checkpoint_artifacts'}:
                # Size the entire inline field, including latest_sent and drafts
                # in mailbox_history. The asset stub below measures UTF-8 bytes.
                inline_characters=len(canonical(value))
                candidates.append((0 if key=='previous_result' else 1,-inline_characters,root+(key,)))
    selected={}
    tag='outreach-task-'+row['id']
    instruction=('\nOFFLOADED INPUTS: Download the JSON asset from the private task Release using '
                 'the already selected api-github-pat connection (Releases API, including the selected '
                 'user_service_id). Verify its exact byte size and SHA-256 against each stub before '
                 'reading it. Its fields map dotted paths to original values; replace each offloaded '
                 'field with its original value and treat it exactly as the original input field. '
                 'Never read or include raw tokens.\n')
    for _,__,path in sorted(candidates):
        obj=projected
        for key in path[:-1]:obj=obj[key]
        selected['.'.join(path)]=obj[path[-1]]
        payload=canonical({'fields':selected}).encode()
        sha=hashlib.sha256(payload).hexdigest()
        meta={'offloaded':True,'release_tag':tag,'asset':'prompt-inputs-'+sha+'.json',
              'sha256':sha,'bytes':len(payload)}
        for moved in selected:
            target=projected
            parts=moved.split('.')
            for key in parts[:-1]:target=target[key]
            target[parts[-1]]=dict(meta)
        prompt=task_prompt(projected,github,oracle_capacity,context=row)+instruction
        if len(prompt)<=budget:
            asset=Path(folder)/meta['asset'];asset.write_bytes(payload)
            if row['data'].get('prompt_offload')!={**meta,'fields':list(selected)}:
                releases.preserve_task(asset,tag)
            row['data']['prompt_offload']={**meta,'fields':list(selected)}
            return prompt
    raise ValueError('Task prompt exceeds prompt_max_characters after input offload')


def task_prompt(row,github=None,oracle_capacity=None,*,context=None):
    context=row if context is None else context
    from .author_contacts import INSTRUCTIONS as author_instructions
    connection=('Use this existing NyxID GitHub connection for repository API calls, '
                'including the explicit --via-service user_service_id; never read a raw token:\n'
                +canonical(github)+'\n') if github else ''
    capacity_line=oracle_capacity_line(oracle_capacity)
    policy_revision = row['request'].get('policy_revision')
    if policy_revision:
        staging = f"""Stage engine source from https://github.com/{ENGINE_REPO} at source_revision.
Then stage policy/ from https://github.com/{REPO} at policy_revision in a separate data
directory and overlay its policy files at their original paths in the engine staging
directory. Use the explicitly selected api-github-pat connection for both repositories.
Verify both commits and every staged blob against their respective Git trees. Keep
executable files from the engine tree. Set OUTREACH_DATA_ROOT to the staged data directory
when recomputing the workflow digest so the resolver uses that exact policy tree.
Use policy_revision for pinned reads of private tables."""
    else:
        staging = f"""Prepare https://github.com/{REPO} at the exact source_revision in TASK
through the explicitly selected api-github-pat connection. Verify the commit and
staged blobs against its Git tree. Use source_revision for pinned reads of private tables."""
    identity = settings('identity')
    private_inputs = canonical({'repository': REPO, 'branch': DATA_BRANCH, 'identity': identity,
                                'mathesis_url': settings('mathesis')['base_url'].rstrip('/') + '/'})
    return '''Execute this single Outreach task at the pinned repository revision.
Use the repository's managed workflow for its kind, AGENTS.md and managed/WORKSPACE.md.
''' + staging + '''
Use the following private repository for task Releases. Use the configured branch
when a contract specifically calls for a fresh registry check. Apply the configured
identity, exact first-contact signature and Mathesis URL where policy uses placeholders.
PRIVATE DATA LOCATIONS AND IDENTITY:
''' + private_inputs + '''

Read the commits, Git trees and contents APIs. Stage the verified files in an isolated
task directory and preserve pre-existing files and .git. Recompute the workflow digest
with managed.control.workflow(kind) over the staged tree and its operative policy.
If it computes a different workflow_hash, return outcome=hold
with report beginning "''' + POLICY_IDENTITY_MISMATCH + ''':" and both digests. Return no
packet or artifacts for this preflight hold. Do not use the default
api-github OAuth checkout or request a replacement connection when the pinned
existing connection suffices. Never modify upstream
trureturing, send email, post to X, submit to Mathesis, grant yourself approval, or
publish a scientific work. Read all supplied thread/source material before judgment.
Write only inside your own worktree (including deliverables and any person workspace
below it), use no shared cache or directory outside it for task material, and delete
any temporary files you created outside the worktree before finishing.
Keep every computational project focused, in a person-owned workspace. Preserve
deliverables under deliverables/ in your actual Agent worktree. Return agent_path
relative to that worktree on each artifact; the coordinator retrieves exact bytes,
checks hashes and saves them as private Release assets. Do not upload with native
GitHub credentials or search for tokens. Use a task-specific
release tag outreach-task-<task_id>, never a control, migration or publication
release. Exclude credentials, environments and raw logs.
If the runtime guard asks you to stop, pack the unfinished working files needed
to resume into one archive below deliverables/ with role "checkpoint". Include
an internal manifest of member paths, sizes in bytes and SHA-256 values. List
that archive and any final deliverables in artifacts with their exact agent_path,
name, bytes and SHA-256, even on a hold. Loose checkpoint files are not accepted.
In a hold, use packet=null and report what was verified and what remains.
When resuming from a checkpoint archive, restore the files using its manifest.
For a guard continuation staged without inputs.checkpoint_artifacts, use the
immutable Release references in inputs.previous_result.artifacts, if available.
Redo any work you cannot restore from those references using the primary source
in this worktree, even if the old CONTINUE reason asks you to finish only the gap.
Short-note standing default: use the configured shortnote_creators in order and
license CC BY 4.0 (cc-by-4.0) in metadata.json and the PDF.
Treat new research questions from correspondents as research backlog and do
substantive work before drafting. Ignore recurring promotional email footers.
For submission tasks (user 2026-11-01): final byline order, license, AI-declaration wording,
CRediT roles, phone and postal addresses do not need a new user decision during preparation.
Keep the provisional alphabetical byline and leave license unselected. Use the
user-approved locked AI statement from managed/submission/author-policy.json verbatim:
locked_statement_without_lean unless Lean verification of this paper's results actually
applies, then locked_statement_with_lean with its exact verification scope. Add no placeholder
preface or internal-process wording. Finalize-round deliverables must omit 'provisional' and
'private preparation draft' banners. Ask only genuine research questions.
Reader-facing files (PDF, TeX, README, metadata, checker output) never link or cite internal
trureturing or Outreach issues, pull requests or provenance pages and never describe how the
work was produced; cite public scientific sources and pinned .lean files only. For a Mathesis
handoff whose selected approved note or archive contains such wording, prepare the next
revision changing only those sentences or fields to neutral coverage statements, rebuild,
confirm identical mathematics and checker output, bind the handoff to the new hashes and
report the exact changes, instead of holding.
Oracle reviews go through the NyxID oracle broker only: nyxid proxy request oracle
api/v1/oracle/openai/v1/chat/completions --method POST --data @ask.json with
{"model":"oracle/chrono-chatgpt-pro-pool","stream":true,"messages":[...]} (one question per
call; for long jobs POST api/v1/oracle/pools/chrono-chatgpt-pro-pool/tasks then GET
api/v1/oracle/tasks/ID?wait=60). Never use `nyxid oracle ...`: that older service answers
410 oracle_retired, which means a wrong route, not that Oracle review is unavailable.
Short notes (kind shortnote) make exactly one Oracle call: the GPT Pro prior-work check
of the uplift round in research/workflow.json (the owner 2026-11-04, replacing the earlier
no-Oracle rule); no other Oracle use. They are Codex-written summaries of resolved
conjectures for correspondence and the results record.
Keep every Oracle prompt short and single-question, attach the relevant note PDF as
a file part, and never paste an archive into the prompt. Preserve stream=true. On
no_assistant_output, retry the same short question with bounded backoff instead of
expanding it into a long multi-question request.
For newly authored short notes or manuscript revisions, follow short_note.layout and
short_note.manuscript_template when present in this task's pinned research/workflow.json.
Fetch and verify that exact template; record its provenance in private evidence.
Use research-paper-writing-rigor for a note. If it is not preinstalled, read the published skill
(guid a22344ff-4cf3-4ea7-a22e-2f0f98792c10) and its reference files through the CMA ornn-api
service, e.g. GET /api/v1/skills/research-paper-writing-rigor; a missing local install alone
is never a reason to HOLD. Unselected author order/license is a needs-decision question,
not a HOLD. A shortnote task must deliver rebuildable
LaTeX, a readable PDF, exact claim/source/verification evidence and checked references.
For scientific PDFs, fetch managed/runtime.py at the same pinned workflow revision
through the configured GitHub API connection. Save it in this task workspace and
run python3 runtime.py --runtime .runtime/tex --source deliverables/note.tex
--output deliverables (substitute the actual main source). This installs the
checksum-pinned official Tectonic engine in the task only. Never retain the
runtime/cache as an artifact and never put runtime.py into a public source archive;
its README tells readers to build with Tectonic 0.17.0 or pdflatex/bibtex directly. Rebuild and visually inspect the final PDF.
Everything a reader or correspondent receives (PDF, README, verify scripts and their output,
JSON metadata) states coverage in neutral scientific terms: which Lean declaration proves which
statement, which parts are written proofs, which are finite checks. It never contains process
wording such as 'no Lean run', 'not executed here', 'inspected, not run', 'kernel build',
'axiom audit', 'receipt', 'private input', 'runtime installer', 'this task', 'sending approval'
or 'review'; keep those facts in the private evidence report only.
A reply must answer the actual latest message and preserve user-owned decisions.
Never draft a letter about our own unfinished work (the owner 2026-11-01): if the reply depends on
a deliverable we owe (verification package, revised file, proof, certificate), produce that
deliverable in this task and attach or link it; do not write 'we will', 'please wait until we'
or plans. If it cannot be finished here, return the research result with packet=null and say
what remains, so the letter is drafted only when the work is done.
Research-only work still requires a useful checked result and an evidence report.
Do not run Lean without an execution machine selected for this task by the user.
Finish with one JSON object, no Markdown fences:
{"schema_version":1,"task_id":"...","outcome":"completed|hold|no-reply|needs-decision",
 "headline":"one English sentence stating the main result or the letter's answer",
 "report":"a substantive readable account with exact claim, citations, proof scope,
 outstanding questions and user decisions",
 "artifacts":[{"release_tag":"...","agent_path":"deliverables/file","name":"...","sha256":"...","bytes":1,"role":"note-tex|note-pdf|manuscript-tex|manuscript-pdf|source-archive|evidence|mail-packet|handoff|checkpoint"}],
 "packet":null}
Return a top-level headline: one English sentence (<=300 chars) stating the main result
or what the letter does, precise enough for a reviewer to judge (e.g. the theorem proved
and its scope, or the letter's answer).
For every kind, self_audit is allowed as an optional top-level list of
{check, status: pass|fail|n/a, note}.
When inputs.review_feedback is present, self_audit must contain one entry per numbered
point of review_feedback.reason, with check exactly 'feedback N' (N the point number),
status pass if resolved, fail otherwise, and a note citing where it is resolved
(file and line or a short verbatim quote).
Read managed/automation.json for the routing contract. Add its exact routing object
when the request has automation_contract=1 or a user decision, no-reply outcome or
independent-submission classification is appropriate. Questions need actual source
quotes, not guessed personal choices. A packet is only a proposal for Claude review.
the owner's 2026-11-02 per-letter rule supersedes older thread-ownership wording:
decide routing.mail from THIS latest incoming message and the needed reply;
thread history alone does not make a letter owner-routed. user_decision_ever is
history only and must remain recorded. Route owner whenever the latest incoming
message or needed reply touches anything personal or about us/the project (who
we are, affiliation, how results are produced, tools/AI, funding, plans, motives,
meetings, invitations, collaboration terms, authorship or publication), or relays
a decision the owner made. Only a purely academic exchange about the mathematics
uses auto, after the operator's exact independent PASS and fresh mailbox checks.
Exception (the owner 2026-11-04): a first contact whose only about-us content is the
approved standard opening and tie-back closing in managed/firstcontact/workflow.json
routes auto; route a first contact owner only for
anything beyond them.
When unsure, choose owner. Academic includes: technical progress, checks and corrections on a joint manuscript; corrections to a correspondents paper, note or OEIS entry; adding requested references; welcoming a correspondents further mathematical questions or ideas without new commitments. Reserve owner for the listed personal/about-us/meeting/byline/submission-timing topics; do not route an academic letter owner merely because it concerns joint work or the correspondent offered more problems. (User 2026-11-19.)
A decision continuation carrying inputs.user_answers
must treat those questions as decided and must not ask them again. If its needed
reply is purely academic, it may use routing.mail=auto after exact review and
fresh mailbox checks; meetings/calls, personal facts, questions about us/the
project/AI, byline order and submission timing remain owner drafts.
For either letter type, always use routing.mail=owner if any To or Cc address
is at the mailbox owner's institution (configured home domains or their subdomains).
For a completed reply/firstcontact with mail=auto or owner, return the exact packet
inline. When attachments make it large, save the packet JSON itself (including
attachment content_b64) under deliverables/, list it in artifacts with role
mail-packet, exact name/bytes/sha256 and agent_path, and return
packet={"artifact":"<that artifact name>"}. The packet JSON must be at most 25 MiB;
do not return packet=null for a completed outgoing letter or wrap it in a result.
Use outcome needs-decision with packet=null for missing user decisions; use no-reply
for a reply that needs no response. Selected manuscript inputs can still accompany
a no-reply result. A submission task uses managed/submission/workflow.json and must
deliver manuscript-pdf, manuscript-tex, source-archive and evidence artifacts.
A completed task is never a sending approval.
'''+('''For the initial shortnote uplift or a standalone shortnote, read
inputs.previous_result and the exact release-referenced draft PDF/source if supplied. Make
the one required Oracle GPT Pro prior-work check of research/workflow.json (the owner
2026-11-04) and record its 'Prior-work check' section; make no other Oracle call. Perform
the sibling analysis of research/workflow.json yourself (which other conjectures, comments or open questions in the posing source and
its cross-referenced entries the argument settles or nearly settles; the strongest
corollary or generalization; whether the posing locator is exact; whether the Lean
coverage is stated precisely).
Coverage labels: use "Refuted" or "Proved" only for statements the source actually
asserts or conjectures, quoted at its locator. A converse, a universal reading or a
generalization the source did not assert is labelled "not asserted in the source;
fails for <example>" or "not posed; proved/obstruction proved", never "Refuted".
A question or problem the source poses (not an assertion) is labelled "Answered: yes" or
"Answered: no", never "Proved" or "Refuted".
Search for proofs of the posed statement and its siblings that appeared after the posing date (arXiv, Zenodo, figshare, OEIS edit history and linked papers, MathOverflow answers, the posing authors' pages and citing papers); credit the earliest proof found by date and later independent proofs in order; never claim priority; record the search scope and dates in the evidence.
Add every
few-line sibling result as a proved corollary. Explain relevant source-item coverage
(proved, refuted-and-corrected, prior proof cited, or not covered) in prose or a coverage table
as useful, and retain the complete mapping in private evidence; add a sibling's problem ID
to metadata only when an upstream trureturing record with that ID exists, never invent one. Name the resolved problem and its posers in the title, with no page, revision, identifier or date locator there (research/workflow.json); put the exact posing locator in the first abstract sentence. Link every
pinned .lean file in the PDF and state each declaration's scope, distinguishing
written proofs and finite checks. Finish with and report a self-audit checklist for
those requirements and for absence of process wording in public files, and return it
in self_audit.
''' if row['kind']=='shortnote' and row['request']['inputs'].get('note_round')!='draft'
       and not note_revisions.Revisions.is_merge(row)
       and row['request']['inputs'].get('automation_parent', {}).get('purpose') not in
       {'shortnote-value-uplift', 'shortnote-closure'}
       and not row['request']['inputs'].get('previous_result', {}).get('value_assessment') else '')+('''For
a completed shortnote final round, including a roundless task, return both fields
defined by short_note.closure in managed/research/workflow.json. Return every exact
uncovered statement in formalization_gaps or an explicit empty list. Give a concrete
editorial decision in value_assessment. Reuse prior-work evidence in closure and value
uplift continuations, including later revisions of those continuations. For a value
uplift read the previous assessment and try to prove a stronger statement, general
case, sharp bound or structural explanation. If that cannot be done, preserve the
note's exact deliverables and return outcome=completed with verdict=trivial and
explain the obstruction. Never file GitHub issues from an Agent.
Include the top-level prior_work_check described in research/workflow.json when
available. Use the exact posed statement and verified upstream resolution date.
Include public citations with a URL, release date and locator. Keep private
reports out of prior_work_check.
''' if row['kind']=='shortnote' and row['request']['inputs'].get('note_round')!='draft' else '')+('''If
this result uses any Outreach short note in the letter or handoff, identify its
exact reviewed version in top-level note_sources as a list of {task_id, result_hash}
using the supplied task/result bindings. Use only a completed note with an exact
independent PASS and eligible final round. If the source cannot be verified, hold.
''' if row['kind'] in {'firstcontact', 'reply', 'mathesis'} else '')+('''Execute exactly inputs.submission_round (missing = improve) from
managed/submission/workflow.json, applying rounds_policy, quality_bar and the stages;
stop at that round's deliverable. The evidence artifact must contain the per-round
new-content ledger and Oracle identifiers. A failed contribution gate returns outcome
hold with the redirect and board proposal.
the owner 2026-11-02 supersedes the workflow's operator checkpoints: Codex + Oracle
advance completed rounds autonomously; operator review waits until the owner prepares
to submit. For consult/uplift, choose the direction and fallback from the previous
result and Oracle evidence when review_feedback is null. Return hold or
needs-decision for a real stop or user choice; never invent an operator approval.
''' if row['kind']=='submission' else '')+(author_instructions+'\n' if row['kind']=='shortnote' else '')+('''For a reply or first contact with an outgoing letter, read managed/replies/voice.md
at the pinned revision. Apply the workflow's final voice_pass and include its
required 'voice pass' entry in self_audit for the exact body.
''' if row['kind'] in {'reply', 'firstcontact'} else '')+('''For a reply, read the complete thread and apply reply_pre_return_checks in
the pinned workflow. Read inputs/data mailbox_history across all threads with this correspondent.
Use the newest latest_sent entry across the correspondent's verified addresses
for its salutation and closing. Cite its internetMessageId. Hold if Sent evidence
exists for the correspondent and that entry is missing. Use drafts to check
saved-draft supersession. Hold if drafts_complete is false or missing. When it is
true, an absent historical ID or an empty list establishes absence from current
Drafts. When a current owner draft exists, route owner and consolidate its
substance, naming it in owner_brief.cautions_zh. Attachments built in this reply
task are reviewed with the exact packet. They need no separate short-note PASS
or note_sources entry. Bind an exact PASS for any existing Outreach short note
the reply attaches or cites.
When routing.mail is owner, or routing.questions is non-empty, include the required
top-level owner_brief object: summary_zh is one Chinese sentence describing what
the letter/result does, decisions_zh lists only concrete decisions the owner must make,
and cautions_zh lists checks before sending. Legacy results may omit owner_brief
when neither condition applies. Use self_audit for the salutation/latest-Sent,
saved-draft supersession, registry_patch, and attachment/claim checks.
''' if row['kind']=='reply' else '')+('''For every reply task, always return a top-level registry_patch describing the
latest incoming message, even when no praise, refusal, or new contact applies. The
patch is {incoming_summary: one factual English sentence, praise: [{quote: an exact
verbatim substring, context, story}], refusal: null or {quote: an exact verbatim
substring, scope}, new_contact: null or {name, title: Professor|Dr.|none,
title_evidence_url: HTTP(S), affiliation, evidence_url: HTTP(S)}, alias_of: null or {contact_id, evidence_ref, reason}}.
alias_of is optional for backward compatibility. Use alias_of, not new_contact,
when the sender is evidently an already-registered person writing from another
address: cite their existing contact_id from tables/contacts.json, the incoming
Message-ID or graph: reference in evidence_ref, and one factual English sentence
of at most 300 characters in reason explaining the identity evidence (for example,
the same display name and institutional domain continuing the same thread).
alias_of and new_contact must not both be non-null. Never guess: when identity is
uncertain leave both null and explain the uncertainty in self_audit. A refusal is only
an explicit decline of contact/collaboration or a request to stop; “busy, keep us
updated” is not a refusal. Set new_contact only when the sender is absent from
tables/contacts.json. Quotes must be copied verbatim from the latest incoming
message so the local operator can verify them offline. Research tasks with a
mailbox observation follow the same registry_patch contract.
''' if row['kind']=='reply' or (row['kind']=='research' and effective_inputs(context).get('mailbox_observation')) else '')+(
        '\n'+capacity_line+'\n' if capacity_line else '')+'''TASK:
'''+connection+canonical(row['request'])+(
        '\nCOORDINATOR-RESOLVED THREAD INPUTS (verified Graph read; request remains immutable):\n'
        +canonical(row['data']['thread_inputs']) if row.get('data',{}).get('thread_inputs') else '')+(
        ('''\nCOORDINATOR MAILBOX HISTORY (fresh Graph snapshot; request remains immutable):
This complete coordinator snapshot IS the fresh recipient-specific mailbox evidence
required by the firstcontact workflow, even when table policy_fresh=false. CMA has
no mailbox access and must use this evidence. It covers verified aliases, Inbox,
Sent Items, Drafts and the delivery queue's attempts. Do not demand another mailbox
read. Use the effective To/Cc below for ONE letter to the remaining clear authors.
Report every excluded author. If current evidence shows that an effective recipient's
address is stale or unreachable, or the coordinator's feedback narrows the
recipients, omit that address from the packet and list it in result
recipient_exclusions as {"email","reason"}; never add a recipient. To is then the
first remaining planned To (else the first remaining author) and Cc the rest in order.
Prior correspondence/active threads must continue as
replies on the listed conversation IDs, never as cold contact. Preserve owner drafts
and reconcile uncertain sends. If nobody remains eligible, return outcome=hold,
packet=null, routing={mail:none,questions:[],independent_submission:null}, and name
the reply thread(s) or draft/attempt release condition in report. A metadata preview
does not suffice to compose a reply: a separately staged reply resolves the complete
thread through the coordinator. No firstcontact packet may target an excluded author.
'''+canonical(row['data']['mailbox_history'])+'\nEFFECTIVE FIRSTCONTACT RECIPIENTS:\n'+canonical({
            k:effective_inputs(context)[k] for k in ('recipient_emails','to','cc','excluded_recipients')})
        if row['kind']=='firstcontact' and row.get('data',{}).get('mailbox_history') else
        '''\nCOORDINATOR MAILBOX HISTORY ACROSS CORRESPONDENT THREADS (fresh Graph snapshot; request remains immutable):
This coordinator snapshot covers the correspondent's verified addresses across
Inbox, Sent Items and Drafts. CMA has no mailbox access and must use this
evidence. latest_sent is keyed by address, and drafts lists current unsent
matches across all covered addresses when drafts_complete is true. Use the
dedicated fields even when the corresponding message is outside the bounded
messages list. Reply sign-offs end with the names only (the owner 2026-11-04).
'''+canonical(row['data']['mailbox_history'])
        if row['kind']=='reply' and row.get('data',{}).get('mailbox_history') else ''))


def reply_signoff_gate(body):
    """Replies sign with the names only; affiliations belong to first contacts (the owner 2026-11-04, 11-09)."""
    lines=[line.strip() for line in str(body).strip().splitlines() if line.strip()]
    closing=next((i for i in range(len(lines)-1,-1,-1)
                  if re.match(r'(best|kind|warm)( regards| wishes)?,?$|regards,?$|sincerely,?$|cheers,?$',lines[i],re.I)),None)
    tail=lines[closing+1:] if closing is not None else lines[-3:]
    require(not any(marker in line.casefold() for line in tail for marker in settings('identity')['reply_affiliation_markers']),
            'Reply sign-off must end with the names only; remove the affiliation line')


def recipient_exclusions(result,intended):
    """Reviewed narrowing of a grouped first contact: only removals, each with a reason."""
    entries=result.get('recipient_exclusions') or []
    require(isinstance(entries,list),'recipient_exclusions must be a list')
    dropped=set()
    for entry in entries:
        require(isinstance(entry,dict) and set(entry)=={'email','reason'} and
                all(isinstance(entry[k],str) and entry[k].strip() for k in entry),
                'Invalid recipient_exclusions entry')
        address=email(entry['email'])
        require(address in intended and address not in dropped,
                'recipient_exclusions may only remove distinct preflighted recipients')
        dropped.add(address)
    require(not dropped or intended-dropped,'recipient_exclusions cannot remove every recipient')
    return dropped


def packet_artifact(result):
    packet=result.get('packet')
    if not isinstance(packet,dict) or 'artifact' not in packet:return None
    require(set(packet)=={'artifact'} and isinstance(packet['artifact'],str),
            'Invalid mail packet artifact reference')
    matches=[a for a in result['artifacts'] if a['name']==packet['artifact']]
    require(len(matches)==1 and matches[0]['role']=='mail-packet',
            'Packet must name one mail-packet artifact of this result')
    require(matches[0]['bytes']<=MAIL_PACKET_MAX_BYTES,'Mail packet artifact exceeds 25 MiB')
    return matches[0]


def firstcontact_review_body_gate(body):
    policy=json.loads(workflow_text('firstcontact/workflow.json',None))
    _, _, prose=body.strip().partition('\n')  # Skip the salutation.
    opening=re.split(r'\n\s*\n',prose.lstrip(),maxsplit=1)[0].casefold()
    identity = settings('identity')
    require(all(term in opening for term in (*identity['firstcontact_opening_terms'],
        'https://github.com/the-omega-institute/trureturing','https://www.trureturing.org/',
        'ai orchestration','connect','lean')),'First-contact opening is missing required content')
    compose=next(step['instructions'] for step in policy['steps'] if step['id']=='compose')
    ending=next((rule for rule in compose if rule.startswith('Ending:')), '')
    paragraphs=re.search(r'Closing paragraph 1: (.+) Closing paragraph 2: (.+) No work-plan paragraph',ending)
    require(paragraphs is not None,'First-contact closing requirements are missing from the workflow')
    mathesis_url = settings('mathesis')['base_url'].rstrip('/') + '/'
    closing='\n\n'.join((policy['closing_suffix_exact'],
                         *(paragraph.replace('[configured Mathesis URL]', mathesis_url)
                           for paragraph in paragraphs.groups()), policy['signature_exact'].replace('[configured firstcontact_signature]',
                                                           identity['firstcontact_signature'])))
    require(body.rstrip().endswith(closing),'First-contact closing must use the required invitation, paragraphs and signature')


def edited_review_result(row, body):
    """Build the exact replacement result and its immutable packet bytes."""
    original=row['data']['result']
    require(original.get('outcome')=='completed' and isinstance(original.get('packet'),dict)
            and 'body' in original['packet'],'edited_body requires a completed result with a mail packet')
    body=body.replace('\r\n','\n').replace('\r','\n')
    packet={**original['packet'],'body':body}
    raw=canonical(packet).encode('utf-8')
    require(len(raw)<=MAIL_PACKET_MAX_BYTES,'Edited mail packet exceeds 25 MiB')
    require(not re.search(r'—|(?<!\S)–|–(?!\S)',body),
            'Edited body must not use em or en dashes as sentence punctuation')
    if row['kind']=='firstcontact':firstcontact_review_body_gate(body)
    sha=hashlib.sha256(raw).hexdigest()
    artifact={'release_tag':'outreach-task-'+row['id'],'name':'reviewed-packet-'+sha+'.json',
              'role':'mail-packet','bytes':len(raw),'sha256':sha}
    artifacts=[a for a in original['artifacts'] if a['role']!='mail-packet']+[artifact]
    result={**original,'packet':packet,'artifacts':artifacts}
    validate_result(result,row,allow_packet_artifact=False)
    return result,artifact,raw


def validate_result(result,row,*,allow_packet_artifact=True):
    fields={'schema_version','task_id','outcome','report','artifacts','packet'}
    registry_allowed = row['kind'] == 'reply' or (
        row['kind'] == 'research' and bool(effective_inputs(row).get('mailbox_observation')))
    optional={'routing', 'self_audit', 'headline'} | ({'author_contacts','author_contacts_reason'} if row['kind'] in {'shortnote','research'} else set())
    if row['kind'] == 'shortnote':
        optional |= {'formalization_gaps', 'value_assessment', 'prior_work_check'}
    if row['kind'] in {'firstcontact', 'reply', 'mathesis'}:
        optional.add('note_sources')
    if row['kind'] in {'reply','firstcontact','research'}:
        optional.add('owner_brief')
    if row['kind'] == 'firstcontact':
        optional.add('recipient_exclusions')
    if registry_allowed: optional.add('registry_patch')
    require(isinstance(result,dict),'Invalid CMA result contract: expected a JSON object')
    unexpected=sorted(set(result)-fields-optional)
    missing=sorted(fields-set(result))
    require(not unexpected and not missing,
            'Invalid CMA result contract: unexpected keys '+repr(unexpected)+' / missing keys '+repr(missing))
    require(result['schema_version']==1 and result['task_id']==row['id'],'Wrong CMA result identity')
    require(result['outcome'] in {'completed','hold','no-reply','needs-decision'} and isinstance(result['report'],str) and result['report'].strip(),'Missing research judgment')
    if 'note_sources' in result:
        note_closure.validate_sources(result['note_sources'])
    if row['kind'] == 'shortnote':
        from .prior_work_evidence import validate as validate_prior_work
        request = row.get('request', {})
        final = result['outcome'] == 'completed' and request.get('inputs', {}).get('note_round') != 'draft'
        note_closure.validate_result(result, required=final and request.get('workflow_hash') == workflow('shortnote'))
        if 'prior_work_check' in result:
            validate_prior_work(result['prior_work_check'])
        require(not note_closure.trivial(result) or result['outcome'] == 'completed',
                'A trivial note must return its completed artifacts and value assessment for uplift')
    if 'headline' in result:
        headline = result['headline']
        require(isinstance(headline, str), 'headline must be a string')
        require(headline.strip() and len(headline) <= 300 and headline.splitlines() == [headline]
                and re.search(r'[A-Za-z]', headline),
                'headline must be one English line of 1..300 characters')
    if 'self_audit' in result:
        audit = result['self_audit']
        require(isinstance(audit, list), 'self_audit must be a list')
        for item in audit:
            require(isinstance(item, dict) and set(item) == {'check','status','note'} and
                    all(isinstance(item[k], str) and item[k].strip() for k in item) and
                    item['status'] in {'pass','fail','n/a'},
                    'Invalid self_audit entry')
    if 'registry_patch' in result:
        patch = result['registry_patch']
        require(isinstance(patch, dict) and set(patch) in (
                {'incoming_summary','praise','refusal','new_contact'},
                {'incoming_summary','praise','refusal','new_contact','alias_of'}),
                'Invalid registry_patch shape')
        summary = patch['incoming_summary']
        require(isinstance(summary, str) and summary.strip() and '\n' not in summary and
                re.search(r'[A-Za-z]', summary) and len(summary) <= 400,
                'incoming_summary must be one short factual English line')
        praise = patch['praise']
        require(isinstance(praise, list), 'registry_patch praise must be a list')
        for item in praise:
            require(isinstance(item, dict) and set(item) == {'quote','context','story'} and
                    all(isinstance(item[k], str) and item[k].strip() for k in item),
                    'Invalid registry_patch praise entry')
        refusal = patch['refusal']
        require(refusal is None or (isinstance(refusal, dict) and set(refusal) == {'quote','scope'} and
                                    all(isinstance(refusal[k], str) and refusal[k].strip() for k in refusal)),
                'Invalid registry_patch refusal')
        from .registry import _validate_alias, people, read as read_registry
        alias = _validate_alias(patch)
        if alias is not None:
            require(any(c['id'] == alias['contact_id'] for c in read_registry(ROOT, 'contacts')['contacts']),
                    'registry_patch alias_of contact_id does not exist')
            observation = effective_inputs(row).get('mailbox_observation', {})
            sender = observation.get('email') if isinstance(observation, dict) else None
            if sender:
                require(not any(sender.casefold() in p['emails'] for p in people(ROOT)),
                        'registry_patch alias_of is only for an unregistered sender')
        contact = patch['new_contact']
        if contact is not None:
            from .author_contacts import url
            require(isinstance(contact, dict) and set(contact) ==
                    {'name','title','title_evidence_url','affiliation','evidence_url'} and
                    all(isinstance(contact[k], str) and contact[k].strip() for k in
                        ('name','title_evidence_url','affiliation','evidence_url')) and
                    contact['title'] in {'Professor','Dr.','none'},
                    'Invalid registry_patch new_contact')
            url(contact['title_evidence_url']); url(contact['evidence_url'])
            observation = effective_inputs(row).get('mailbox_observation', {})
            sender = observation.get('email') if isinstance(observation, dict) else None
            if sender:
                from .registry import people
                require(not any(sender.casefold() in p['emails'] for p in people(ROOT)),
                        'registry_patch new_contact is only for an unregistered sender')
    from .automation import validate_routing
    validate_routing(result,row)
    if 'owner_brief' in result:
        require(row['kind'] in {'reply','firstcontact','research'},
                'owner_brief is only valid for correspondence or research results')
        brief = result['owner_brief']
        require(isinstance(brief, dict) and set(brief) == {'summary_zh','decisions_zh','cautions_zh'},
                'Invalid owner_brief shape')
        require(isinstance(brief['summary_zh'], str) and brief['summary_zh'].strip() and
                '\n' not in brief['summary_zh'] and re.search(r'[\u3400-\u9fff]', brief['summary_zh']),
                'owner_brief.summary_zh must be one Chinese sentence')
        require(sum(brief['summary_zh'].count(mark) for mark in '。！？') <= 1,
                'owner_brief.summary_zh must be one Chinese sentence')
        for key in ('decisions_zh','cautions_zh'):
            values = brief[key]
            require(isinstance(values, list), 'owner_brief.' + key + ' must be a list')
            for value in values:
                require(isinstance(value, str) and value.strip() and '\n' not in value and
                        re.search(r'[\u3400-\u9fff]', value),
                        'owner_brief.' + key + ' entries must be Chinese strings')
    route = result.get('routing') or {}
    if route.get('mail') == 'owner' or route.get('questions'):
        require('owner_brief' in result, 'Owner routing or user questions require owner_brief')
    # Strict when present. Notes admitted before the author-contact contract may omit it;
    # the reviewer bounces new notes without it and work() lists PASS notes still lacking it.
    if 'author_contacts' in result or 'author_contacts_reason' in result:
        from .author_contacts import validate as validate_authors
        validate_authors(result, row.get('request'))
    if result['outcome']=='no-reply':
        require(row['kind']=='reply' and result.get('routing',{}).get('mail')=='none'
                and result['packet'] is None,'No-reply must be an explicit correspondence judgment')
    if result['outcome']=='needs-decision':
        require(result.get('routing',{}).get('questions') and result['packet'] is None,
                'A decision request must expose its questions for independent review')
    require(isinstance(result['artifacts'],list),'Missing artifact list')
    require(len(result['artifacts'])<=100,'Too many task artifacts')
    names=[]
    for a in result['artifacts']:
        fields={'release_tag','name','sha256','bytes','role'}
        require(set(a) in (fields,fields|{'agent_path'}),'Invalid artifact reference')
        require(all(isinstance(a[k],str) and a[k].strip() for k in ['release_tag','name','role']),'Invalid artifact identity')
        require(a['release_tag']=='outreach-task-'+row['id'],'Artifacts must belong to this task release')
        require(re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,199}',a['name']) is not None,'Invalid artifact filename')
        require(isinstance(a['sha256'],str) and len(a['sha256'])==64 and all(c in '0123456789abcdef' for c in a['sha256']),'Invalid artifact checksum')
        require(type(a['bytes']) is int and a['bytes']>0,'Invalid artifact size')
        names.append(a['name'])
        if 'agent_path' in a:
            require(isinstance(a['agent_path'],str) and re.fullmatch(r'deliverables/[A-Za-z0-9][A-Za-z0-9._/-]{0,499}',a['agent_path'])
                    and all(p not in {'','.', '..'} for p in a['agent_path'].split('/'))
                    and a['bytes']<=30*1024*1024,'Invalid Agent artifact path or size')
    require(len(names)==len(set(names)),'Duplicate task artifact name')
    reference=packet_artifact(result)
    if reference is not None:
        require(allow_packet_artifact and row['kind'] in {'reply','firstcontact'}
                and result['outcome']=='completed','Mail packet artifact is not allowed here')
    if row['kind']=='shortnote' and result['outcome']=='completed':
        require({'note-tex','note-pdf'}<=set(a['role'] for a in result['artifacts']),'Short note needs source and PDF')
    if row['kind']=='submission' and result['outcome']=='completed':
        require({'manuscript-pdf','manuscript-tex','source-archive','evidence'} <=
                {a['role'] for a in result['artifacts']},'Submission preparation needs source, PDF, archive and reviews')
    if (row['kind'] in {'firstcontact','reply','daily'} and result['outcome']=='completed'
            and result.get('routing',{}).get('mail')!='none'):
        from .queue import validate_packet
        if reference is not None:return  # Validate the exact packet after verified readback.
        require(isinstance(result['packet'],dict) and result['packet'].get('kind')==row['kind'],'Missing exact outgoing packet')
        validate_packet(result['packet'])
        if row['kind']=='reply':
            reply_signoff_gate(result['packet'].get('body',''))
            inputs=effective_inputs(row)
            for key in ['thread_id','latest_inbound_message_id']:
                if key in inputs:require(result['packet'][key]==inputs[key],'Reply packet changed its source '+key)
            # Preserve historical provenance; approved_route decides this letter's ownership.
            require(not inputs.get('user_decision_ever') or result['packet']['user_decision_ever'],
                    'A revision cannot clear user-decision history')
        if row['kind']=='firstcontact':
            history=row.get('data',{}).get('mailbox_history')
            if history is not None:
                require(history.get('complete') is True, 'Fresh first-contact mailbox history is incomplete')
            firstcontact_gate({**row['request'], 'inputs':effective_inputs(row)},ROOT)
            actual=set(result['packet']['to']+result['packet']['cc'])
            inputs=effective_inputs(row)
            intended={email(a) for a in inputs['recipient_emails']}
            dropped=recipient_exclusions(result,intended)
            require(actual==intended-dropped,'First-contact packet To/Cc differ from the preflighted recipients; stage a revised request')
            if 'to' in inputs and not dropped:
                require(result['packet']['to']==inputs['to'] and result['packet']['cc']==inputs['cc'],
                        'First-contact packet changed the grouped To/Cc order')
            elif dropped:
                order=[a for a in inputs['recipient_emails'] if email(a) not in dropped]
                preferred=[a for a in inputs.get('to',[]) if email(a) not in dropped]
                to=preferred[:1] or order[:1]
                require([email(a) for a in result['packet']['to']]==[email(a) for a in to] and
                        [email(a) for a in result['packet']['cc']]==[email(a) for a in order if a not in to],
                        'First-contact packet changed the grouped To/Cc order')


ARTIFACT_WORKERS=4
ARTIFACT_READ_DELAYS=(3,10)
ARTIFACT_SAVE_INTERVAL=30


def read_agent_artifact(cma,agent_id,a,lock,can_read):
    """One CMA file read at a time: concurrent reads of an Agent's files are
    refused with file_capacity (HTTP 409/429), which stalled every collection
    for hours on 2026-11-08. Brief refusals are retried."""
    for delay in ARTIFACT_READ_DELAYS+(None,):
        try:
            with lock:
                if not can_read():return None
                return cma.artifact(agent_id,a['agent_path'],a['bytes'],a['sha256'])
        except Exception as exc:
            if 'file_runtime_not_ready' in str(exc):
                # CMA reclaims the file runtime of an Agent idle for hours after
                # its turn completed (11 rows on 2026-11-08). One repair turn on
                # the original Agent restarts it; the files stay in its worktree.
                raise ValueError('Artifact '+a['agent_path']+' could not be read: the Agent file runtime '
                                 'was not ready (file_runtime_not_ready). Confirm every listed deliverable '
                                 'still exists unchanged in your worktree and return the same final JSON.') from exc
            if delay is None or 'file_capacity' not in str(exc):raise
            if not can_read(delay):return None
            time.sleep(delay)


def artifact_key(releases,a):
    return digest([getattr(releases,'repo',None),a['release_tag'],a['name'],a['bytes'],a['sha256']])


def verify_artifacts(result,releases,folder,cma=None,agent_id=None,*,progress=None,checkpoint=None,
                     tick=None,unit_deadline=None):
    from concurrent.futures import ThreadPoolExecutor, FIRST_COMPLETED, wait
    reference=packet_artifact(result)
    import threading
    from queue import SimpleQueue
    lock=threading.Lock()
    progress={} if progress is None else progress
    observations=SimpleQueue()
    deferred=object()
    stage_deadline=tick.limit() if tick else float('inf')
    capped=tick is not None and unit_deadline is not None
    start_deadline=min(stage_deadline,unit_deadline) if capped else stage_deadline
    def within_unit():
        return not capped or tick.clock()<unit_deadline
    def can_start(delay=0):
        return not tick or tick.clock()+delay<start_deadline

    def flush():
        # Worker threads only report observations. SQLite and snapshot writes
        # belong to the coordinator thread, including when another file failed.
        changed=False
        while not observations.empty():
            key,status=observations.get()
            progress[key]=status;changed=True
        if checkpoint and (changed or tick):checkpoint()
    def preserve(a):
        if not capped:return collect(a)
        try:
            with artifact_io_budget(tick.clock,unit_deadline):
                return collect(a)
        except (TimeoutError, subprocess.TimeoutExpired, URLError):
            if within_unit():raise
            return deferred

    def collect(a):
        if not can_start():return deferred
        key=artifact_key(releases,a)
        status=progress.get(key)
        with tempfile.TemporaryDirectory(prefix='artifact-',dir=folder) as temp:
            dest=Path(temp);path=dest/a['name']
            if 'agent_path' in a and status not in {'preserved','verified'}:
                require(cma is not None and bool(agent_id),'Agent file delivery needs its original Agent')
                raw=read_agent_artifact(cma,agent_id,a,lock,can_start)
                if raw is None or not within_unit():return deferred
                path.write_bytes(raw)
                releases.preserve_task(path,a['release_tag'])
                observations.put((key,'preserved'))
                path.unlink()
            if not within_unit():return deferred
            releases.run('release','download',a['release_tag'],'--repo',releases.repo,'--pattern',a['name'],'--dir',str(dest))
            require(path.stat().st_size==a['bytes'] and hashlib.sha256(path.read_bytes()).hexdigest()==a['sha256'],'Artifact readback failed')
            observations.put((key,'verified'))
            if a is reference:
                # Parse only the preserved, checksum-verified Release readback.
                return json.loads(path.read_text(encoding='utf-8'))
        return None
    artifacts=result['artifacts']
    remaining=[a for a in artifacts if a is reference or progress.get(artifact_key(releases,a))!='verified']
    if tick and reference is not None:
        # Read the packet on the final pass so its parsed contents stay local.
        remaining=[a for a in remaining if a is not reference]+[reference]
    packet=None
    offset=0
    batch_size=ARTIFACT_WORKERS if tick else len(remaining)
    # The first upload creates the task Release. Later uploads/readbacks can
    # overlap, while a worker waiting for CMA gets a fresh budget check.
    with ThreadPoolExecutor(max_workers=ARTIFACT_WORKERS) as pool:
        while offset<len(remaining):
            if not can_start():return False
            width=1 if offset==0 else batch_size
            if tick and reference is not None and offset<len(remaining)-1:
                width=min(width,len(remaining)-offset-1)
            batch=remaining[offset:offset+width]
            futures=[pool.submit(preserve,a) for a in batch]
            pending=set(futures)
            while pending:
                _,pending=wait(pending,timeout=1 if tick else None,return_when=FIRST_COMPLETED)
                flush()
            deferred_batch=False
            for a,future in zip(batch,futures):
                value=future.result()
                deferred_batch |= value is deferred
                if a is reference:packet=value
            if deferred_batch:return False
            offset+=len(batch)
    # Review binds portable Release identities; runtime paths are no longer needed.
    for a in artifacts:a.pop('agent_path',None)
    if reference is not None:
        result['packet']=packet
    return True


CAPACITY_RETRIES=24
RUNTIME_RETRIES=3
TRANSPORT_RETRIES=2
RETIRE_HISTORY_PER_TICK=6
CREDIT_RETRIES=50
CREDIT_PROBE_ACCEPT=15*60
RATE_LIMIT_RETRIES=5


def capacity_failure(op):
    return 'sandbox limit reached' in str(op.get('error') or '').lower()


def runtime_failure(op):
    # A pool Sandbox briefly leaves 'active' (e.g. credential_rotating on
    # 2026-11-08); creating an Agent there is refused and a fresh slot works.
    error=str(op.get('error') or '').lower()
    return 'runtime preparation failed' in error or 'sandbox is not active' in error


def requeue_large_task(ledger,value):
    event=digest(value)
    if ledger.tasks.db.execute('SELECT 1 FROM control_events WHERE id=?',(event,)).fetchone():return
    row=ledger.tasks.get(value['task_id'])
    require(row['request_hash']==value['request_hash'],'Requeue request hash changed')
    recovery=row['data'].get('recovery',{})
    error_class=recovery.get('error_class',row['data'].get('error_class'))
    # Oversized prompts restart with offloaded inputs; transient CMA failures
    # whose bounded retries on the same Agent are exhausted restart on a new one.
    # sandbox-failed: both automatic attempts landed in a failed Sandbox; the
    # operator requeues once that Sandbox has been retired from the pool.
    exhausted=error_class in {'rate-limit','network','sandbox-failed'} and recovery.get('status')=='needs-operator'
    # Provisioning refused before this was retryable leaves no Agent work.
    inactive=(not row['data'].get('response_id') and
              'sandbox is not active' in str(row['data'].get('operation_error') or '').lower())
    exhausted=exhausted or inactive
    require(row['state']=='failed' and (error_class=='request-too-large' or exhausted),
            'Only failed request-too-large or retry-exhausted transient tasks may be requeued')
    data=row['data']
    queued={'execution_history':[*data.get('execution_history',[]),data],
            'execution_restart':('operator-' if exhausted else 'prompt-offload-')+event[:16]}
    with ledger.tasks.tx():
        ledger.tasks._update(row['id'],row['revision'],'queued',queued)
        ledger.tasks.db.execute('INSERT INTO control_events VALUES(?,?)',(event,canonical(value)))


def adopt_pool_sandbox(ledger,value,secret):
    """Apply an operator-signed adoption of an existing CMA Sandbox into a pool group.

    Used when the account Sandbox quota is held by Sandboxes CMA cannot retire
    and a healthy Sandbox of ours exists outside the pool (Wenlin 2026-11-22:
    use the 2026-11-20 Sandbox for replies; replies and first contacts keep
    separate Sandboxes). Pool health verifies the Sandbox on the next tick.
    """
    require(bool(secret),'Recovery verification key is not configured')
    require(set(value)=={'schema_version','sandbox_id','workspace_id','group','reason','signature'}
            and value['schema_version']==1,'Invalid pool adoption request')
    signed={k:v for k,v in value.items() if k!='signature'}
    signature=hmac.new(secret.encode(),canonical(signed).encode(),hashlib.sha256).hexdigest()
    require(isinstance(value['signature'],str) and hmac.compare_digest(signature,value['signature']),
            'Pool adoption signature failed')
    require(all(isinstance(value[k],str) and value[k].strip() for k in ('sandbox_id','workspace_id','group','reason')),
            'Pool adoption needs sandbox, workspace, group and reason')
    require(value['sandbox_id'].startswith('sbx_') and value['workspace_id'].startswith('wks_'),'Invalid CMA identifiers')
    from .location import settings
    groups={g['name'] for g in (settings('sandbox_pool') or {}).get('groups',[])}
    require(value['group'] in groups,'Unknown pool group '+value['group'])
    event=digest(value)
    with ledger.tasks.tx():
        db=ledger.tasks.db
        if db.execute('SELECT 1 FROM control_events WHERE id=?',(event,)).fetchone():return
        row=db.execute("SELECT payload FROM pipeline_state WHERE key='sandbox_pool'").fetchone()
        entries=json.loads(row[0]) if row else []
        require(not any(e.get('sandbox_id')==value['sandbox_id'] for e in entries),'Sandbox is already in the pool')
        cleanup=db.execute("SELECT payload FROM pipeline_state WHERE key='sandbox_pool_cleanup'").fetchone()
        require(not any(e.get('sandbox_id')==value['sandbox_id'] for e in (json.loads(cleanup[0]) if cleanup else [])),
                'Sandbox is on the cleanup list')
        entries.append({'sandbox_id':value['sandbox_id'],'workspace_id':value['workspace_id'],
                        'created_by_task':'operator-adopt-'+event[:16],'state':'active',
                        'idle_since':ledger.tasks.clock(),'agents_placed':0,'group':value['group']})
        db.execute('INSERT OR REPLACE INTO pipeline_state VALUES(?,?)',('sandbox_pool',canonical(entries)))
        db.execute('INSERT INTO control_events VALUES(?,?)',(event,canonical(value)))


def requeue_failed_task(ledger,value,secret):
    """Apply an exact, operator-signed restart only in the serialized writer."""
    require(bool(secret),'Recovery verification key is not configured')
    require(set(value)=={'schema_version','task_id','request_hash','failed_revision','reason','signature'}
            and value['schema_version']==1,'Invalid failed-task recovery request')
    signed={k:v for k,v in value.items() if k!='signature'}
    signature=hmac.new(secret.encode(),canonical(signed).encode(),hashlib.sha256).hexdigest()
    require(isinstance(value['signature'],str) and hmac.compare_digest(signature,value['signature']),
            'Recovery request signature failed')
    require(isinstance(value['reason'],str) and value['reason'].strip(),'Recovery request needs a reason')
    event=digest(value)
    with ledger.tasks.tx():
        if ledger.tasks.db.execute('SELECT 1 FROM control_events WHERE id=?',(event,)).fetchone():return
        row=ledger.tasks.get(value['task_id'])
        require(row['request_hash']==value['request_hash'],'Requeue request hash changed')
        require(row['state']=='failed','Only failed tasks may be requeued')
        require(type(value['failed_revision']) is int and row['revision']==value['failed_revision'],
                'Failed task revision changed')
        data=row['data']
        effects=[k for k in ('draft_intent','draft_receipt','sent_receipt','mathesis_delivery','publication_receipt') if data.get(k)]
        require(not effects,'Failed task already has external effects; reconcile them instead: '+', '.join(effects))
        queued={'execution_history':[*data.get('execution_history',[]),data],
                'execution_restart':'operator-'+event[:16]}
        ledger.tasks._update(row['id'],row['revision'],'queued',queued)
        ledger.tasks.db.execute('INSERT INTO control_events VALUES(?,?)',(event,canonical(value)))


def runtime_requeue(data):
    queued=requeue(data)
    queued['capacity_retries']=data.get('capacity_retries',0)
    queued['runtime_retries']=data.get('runtime_retries',0)+1
    return queued


def requeue(data):
    return {**{k:v for k,v in data.items() if k.startswith('credit_') or k in {'sandbox_failures','sandbox_failure_history','workflow_binding','runtime_retries','admission_rejections','admission_errors','admission_history'} or
               (data.get('execution_restart') and k in EXECUTION_RESTART_FIELDS)},
            'capacity_retries':data.get('capacity_retries',0)+1,
            'previous_operations':[*data.get('previous_operations',[]),
                                   {'operation_id':data.get('operation_id'),'create_key':data.get('create_key')}]}


def workspace_requeue(data):
    """A refused Workspace needs another target, not a capacity retry."""
    return {**{k:v for k,v in data.items() if k in
               {'sandbox_failures','sandbox_failure_history','capacity_retries','runtime_retries','thread_inputs','resolution_retries','workflow_binding',
                'admission_rejections','admission_errors','admission_history'} or k.startswith('credit_') or
               (data.get('execution_restart') and k in EXECUTION_RESTART_FIELDS)},
            'previous_operations':[*data.get('previous_operations',[]),
                                   {'operation_id':data.get('operation_id'),'create_key':data.get('create_key')}]}


def sandbox_failure(ledger,cma,row,pool=None):
    """Retain the failed execution evidence, never replay a collected result."""
    data=dict(row['data'])
    if data.get('result') is not None:return False
    stalled=(pool and data.get('pool_create') and not data.get('workspace_id') and
             row['state'] in {'provisioning','provision-unknown'} and pool.creation_stalled(row))
    if stalled:pool.record_capacity_failure(row)
    sid=data.get('sandbox_id') or data.get('pool_sandbox_id') or data.get('owned_sandbox_id')
    agent={}
    if data.get('agent_id'):
        agent=cma.request('/api/v1/agents/'+data['agent_id'])
        sid=sid or (agent.get('sandbox') or {}).get('id')
    known_failed=next((e for e in pool.failed if e['sandbox_id']==sid),None) if pool and isinstance(pool.failed,list) else None
    sandbox=({'status':'failed'} if known_failed else pool.observed[sid]
             if pool and isinstance(pool.observed,dict) and sid in pool.observed else cma.request('/api/v1/sandboxes/'+sid) if sid else {})
    if not stalled and sandbox.get('status')!='failed' and agent.get('status')!='failed':return False
    from .sandbox_pool import runtime_corrupt
    corrupt=runtime_corrupt(agent)
    if sid and pool and (sandbox.get('status')=='failed' or corrupt or stalled):
        pool.failed_entry(sid,data.get('workspace_id'),corrupt,
                          stalled='provisioning' if stalled else None, group=pool.group(row))
    key='outreach-sandbox-failure-'+row['request_hash']+'-'+str(data.get('sandbox_failures',0))
    data['sandbox_failure_intent']={'sandbox_id':sid,'agent_id':data.get('agent_id'),'key':key}
    row=ledger.tasks.update(row['id'],row['revision'],row['state'],data);ledger.save(force=True)
    cleanup=cma.retire(data['agent_id'],key,sid if sandbox.get('status')=='failed' else None) if data.get('agent_id') else {}
    history=[*data.get('sandbox_failure_history',[]),{'sandbox_id':sid,'agent_id':data.get('agent_id'),
             'create_key':data.get('create_key'),'operation_id':data.get('operation_id'),'cleanup':cleanup,
             'failure':({'class':'sandbox_creation_stalled',
                         'started_at':data.get('pool_create_started_at',row['updated'])}
                        if stalled else sandbox.get('failure') or agent.get('failure'))}]
    count=data.get('sandbox_failures',0)
    if count<2:
        queued=workspace_requeue(data)
        queued.update(sandbox_failures=count+1,sandbox_failure_history=history)
        ledger.tasks.update(row['id'],row['revision'],'queued',queued)
    else:
        data.update(cleanup=cleanup,sandbox_failure_history=history,error_class='sandbox-failed',
                    reason='CMA Sandbox/Agent failed; recovery limit reached',
                    recovery={'status':'needs-operator','error_class':'sandbox-failed','attempts':count})
        ledger.tasks.update(row['id'],row['revision'],'failed',data)
    ledger.save()
    return True


def rejection_detail(exc):
    body=getattr(exc,'body',None)
    if body is None and hasattr(exc,'read'):
        body=exc.read().decode('utf-8',errors='replace')
    return {'status':getattr(exc,'code',None),'body':body if body is not None else str(exc)}


def admission_rejected(ledger,cma,row,exc):
    data=dict(row['data'])
    history=data.setdefault('admission_errors',[])
    if exc is not None:
        history.append(rejection_detail(exc))
        data['admission_rejection_ticks']=data.get('admission_rejection_ticks',0)+1
    row=ledger.tasks.update(row['id'],row['revision'],'admission-unknown',data)
    ledger.save(force=True)
    if data['admission_rejection_ticks']<3:return row
    data['cleanup']=cma.retire(data['agent_id'],
        'outreach-admission-retire-'+row['request_hash']+'-'+str(data.get('admission_rejections',0)),
        data.get('owned_sandbox_id'))
    if not data['cleanup'].get('deleted'):
        return ledger.tasks.update(row['id'],row['revision'],row['state'],data)
    count=data.get('admission_rejections',0)
    if count<2 or (len(data.get('prompt',''))>PROMPT_MAX_CHARACTERS and not data.get('prompt_offload')):
        queued=workspace_requeue(data)
        queued.update(admission_rejections=count+1,admission_errors=history,
                      admission_history=[*data.get('admission_history',[]),data])
        state='queued';data=queued
    else:
        state='failed'
        data.update(error_class='admission-rejected',reason='CMA admission rejected; operator recovery required',
                    recovery={'status':'needs-operator','error_class':'admission-rejected',
                              'attempts':count,'suggested_exit':'rewrite-task'})
    row=ledger.tasks.update(row['id'],row['revision'],state,data)
    ledger.save(force=True)
    return row


def runtime_guard(ledger,cma,row,response,config):
    data=json.loads(json.dumps(row['data']));now=ledger.tasks.clock()
    guard=data.setdefault('runtime_guard',{})
    if guard.get('response_id')!=data['response_id']:
        guard={ 'response_id':data['response_id'],'started_at':response.get('created_at') or data.get('response_started_at',row.get('updated',now))}
        data['runtime_guard']=guard
    tokens=(response.get('usage') or {}).get('input_tokens',0) or 0
    limits=config.get('runtime_guard',{})
    steer=guard.get('steer')
    kind=None
    if steer and 'receipt' in steer:
        if now-steer['at']>=7200 or tokens-steer['input_tokens']>=2000000:kind='cancel'
    elif now-guard['started_at']>=limits.get('max_hours',8)*3600 or tokens>=limits.get('max_input_tokens',5000000):
        kind='steer'
    if kind:
        attempts=guard.setdefault('controls',[])
        for _ in range(5):
            pending=next((a for a in reversed(attempts) if a['kind']==kind and a['status']=='pending'),None)
            if pending is None:
                if kind in guard and 'receipt' in guard[kind]:break
                observed=cma.controls(data['agent_id'],data['response_id'])
                target=observed.get('target')
                if not target or target.get('response_id')!=data['response_id']:break
                command={'type':kind}
                if kind=='steer':
                    command['text']=(
                        'Stop exploring now. Before returning the task JSON, pack the unfinished working '
                        'files needed to resume into one archive below deliverables/ with role "checkpoint". '
                        'Include an internal manifest of member paths, sizes in bytes and SHA-256 values. '
                        'Loose checkpoint files are not accepted. List the archive and any final deliverables in artifacts with '
                        'release_tag outreach-task-<task_id> and exact name, agent_path, bytes and SHA-256 '
                        'as for a completed result. '
                        'If the task is unfinished, return outcome=hold with packet=null and report '
                        'what was verified and the precise remaining gap. The next Agent needs these '
                        'Release artifacts to restore the work.')
                pending={'kind':kind,'key':'outreach-guard-'+row['request_hash']+'-'+data['response_id']+'-'+kind+'-'+str(len(attempts)),
                         'body':{'turn_sequence':target['turn_sequence'],'expected_agent_revision':observed['agent_revision'],'command':command},
                         'status':'pending','at':now,'input_tokens':tokens}
                attempts.append(pending)
            row=ledger.tasks.update(row['id'],row['revision'],row['state'],data)
            ledger.save(force=True)
            try:receipt=cma.control(data['agent_id'],data['response_id'],pending['body'],pending['key'])
            except Exception as exc:
                detail=rejection_detail(exc);pending['error']=detail
                if detail['status']==409 and any(code in str(detail['body']) for code in ('response_control_conflict','response_control_not_admitted')):
                    pending['status']='conflict'
                    row=ledger.tasks.update(row['id'],row['revision'],row['state'],data);ledger.save(force=True)
                    continue
                row=ledger.tasks.update(row['id'],row['revision'],row['state'],data);ledger.save(force=True)
                return row
            pending.update(status='admitted',receipt=receipt)
            guard[kind]=dict(pending)
            break
    if not kind:return row
    if data!=row['data']:
        row=ledger.tasks.update(row['id'],row['revision'],row['state'],data);ledger.save(force=True)
    return row


def transport_failure(response):
    error=response.get('error') or {}
    return (response['status']=='failed' and isinstance(error,dict) and
            error.get('code')=='agent_turn_failed' and
            any(s in str(error.get('message') or '').lower() for s in
                ('stream disconnected','transport error','network error','connection reset',
                 'connection aborted','connection lost','broken pipe','unexpected eof',
                 'error sending request for url','error trying to connect','dns error')))


def credit_failure(response):
    error=response.get('error') or {}
    return (isinstance(error,dict) and error.get('code')=='agent_turn_failed' and
            any(s in str(error.get('message') or '').lower() for s in
                ('insufficient_credits','402 payment required')))


def execution_failure_class(error):
    """Classify terminal execution errors, never uncertain API observations."""
    if not isinstance(error,dict):return 'rate-limit' if transient_http_status(error) else 'unknown'
    code=str(error.get('code') or '').lower()
    message=str(error.get('message') or '').lower()
    if credit_failure({'error':error}):return 'credit'
    if code in {'rate_limit_exceeded','rate_limit_error'} or (code=='agent_turn_failed' and
            any(s in message for s in ('429 too many requests','rate limit','rate_limit'))):
        return 'rate-limit'
    # Transient refusals that retry safely with backoff: CMA reports the prompt
    # was never written to the retired connection, or billing asks for a retry.
    if code=='input_not_written' or (code=='agent_turn_failed' and
            'billing settlement is busy' in message):
        return 'rate-limit'
    # CMA platform outages before the agent did any work (observed 2026-11-06:
    # security_readiness_unavailable behind an upstream 503) retry with backoff.
    if code=='agent_turn_failed' and ('security_readiness_unavailable' in message or
            '503 service unavailable' in message):
        return 'rate-limit'
    # CMA could not finish an admitted command within its queue window
    # (TUM reply 2026-11-08); the same Agent retries the turn with backoff.
    if code=='agent_command_queue_timeout':
        return 'rate-limit'
    if code in {'','http_error','agent_turn_failed'} and transient_http_status(error):
        return 'rate-limit'
    if code in {'request_too_large','payload_too_large'} or (code=='agent_turn_failed' and
            re.search(r'\b413\b',message)):
        return 'request-too-large'
    if transport_failure({'status':'failed','error':error}):return 'network'
    return 'unknown'


def failure_exit(error_class):
    return {'rate-limit':'retry-later','credit':'retry-later',
            'network':'local-codex','request-too-large':'local-codex'}.get(error_class,'rewrite-task')


def transient_http_status(error):
    """Recognize saved urllib errors as well as structured HTTP failures."""
    status=getattr(error,'code',None)
    if isinstance(error,dict):status=error.get('http_status',error.get('status'))
    if type(status) is int:return status if status in TRANSIENT_HTTP else None
    message=error.get('message','') if isinstance(error,dict) else str(error or '')
    match=re.search(r'\bHTTP(?:\s+Error)?\s*:?\s*(\d{3})\b',message,re.IGNORECASE)
    return int(match[1]) if match and int(match[1]) in TRANSIENT_HTTP else None


def saved_http_failure(data):
    return next((message for key in ('last_error','reason')
                 if transient_http_status(message:=data.get(key))),None)


def rate_limit_budget(config):
    limit=config.get('recovery',{}).get('rate_limit_max_attempts',RATE_LIMIT_RETRIES)
    return limit if type(limit) is int and limit>=0 else RATE_LIMIT_RETRIES


def retry_delay(attempt):
    return min(3600,60*2**min(attempt-1,6))


def recover_transport_exception(ledger,row,exc,config,retry_state=None):
    """Retry an observation with its original handles/keys, never a new turn.

    An HTTP refusal or lost response does not prove that a create/admit failed.
    Exhaustion retains those handles for reconciliation and prevents cleanup.
    This helper is confined to CMA execution; delivery adapters own their retries.
    """
    status=getattr(exc,'code',None)
    error_class=('rate-limit' if type(status) is int and status in TRANSIENT_HTTP else
                 'network' if not isinstance(exc,HTTPError) and status is None and
                 isinstance(exc,(URLError,TimeoutError,ConnectionError)) else None)
    state=retry_state or row['state']
    if error_class is None or state not in {'provisioning','provision-unknown','admitting','admission-unknown','running'}:
        return False
    data=dict(row['data'])
    counter='rate_limit_retries' if error_class=='rate-limit' else 'transport_retries'
    attempts=data.get(counter,0)
    limit=rate_limit_budget(config) if error_class=='rate-limit' else TRANSPORT_RETRIES
    message=type(exc).__name__+': '+str(exc)[:300]
    recovery={'scope':'observation','error_class':error_class,'attempts':attempts,
              'suggested_exit':'retry-later','retry_state':state}
    data.update(last_error=message,recovery=recovery)
    if attempts<limit:
        attempts+=1;data[counter]=attempts
        recovery.update(status='retry-scheduled',attempts=attempts,
                        next_at=ledger.tasks.clock()+retry_delay(attempts),
                        reason='Reconcile the original CMA operation/command/response')
        state={'provisioning':'provision-unknown','admitting':'admission-unknown'}.get(state,state)
    else:
        state='failed'
        recovery.update(status='needs-operator',suggested_exit='local-codex',
                        reason='CMA transport retry limit reached; reconcile original handles before recovery')
        data['reason']=recovery['reason']
    # No external effect follows this update. The tick flushes it; the next
    # create/admit still requires its usual forced intent checkpoint.
    ledger.tasks.update(row['id'],row['revision'],state,data);ledger.save()
    return True


def observation_recovered(data):
    recovery=data.get('recovery',{})
    if recovery.get('scope')!='observation' or recovery.get('status')!='retry-scheduled':return False
    data['recovery']={**recovery,'status':'recovered'}
    data.pop('last_error',None)
    return True


EXECUTION_RESTART_FIELDS={'recovery','execution_history','execution_restart','prompt',
                          'rate_limit_retries','transport_retries','thread_inputs',
                          'mailbox_history','resolution_retries','workflow_binding'}


def agent_retired(data):
    """A stopped or deleted Agent accepts no new turn; retries need a new Agent."""
    cleanup=data.get('cleanup') or {}
    return bool(cleanup.get('deleted') or cleanup.get('stopped'))


def execution_restart(row,data,prompt,label):
    """Preserve the retired Agent's exact evidence and restart under new lifecycle keys."""
    history=[*data.get('execution_history',[]),
             {k:v for k,v in row['data'].items() if k!='execution_history'}]
    data={k:v for k,v in data.items() if k in EXECUTION_RESTART_FIELDS or
          k.startswith('credit_') or k in {'capacity_retries','previous_operations'}}
    data.update(prompt=prompt,execution_history=history,execution_restart=label)
    return data


def recover_execution_failure(ledger,row,response,config):
    """Checkpoint a bounded retry, or make terminal failure actionable."""
    data=dict(row['data']);error=response.get('error')
    error_class=('runtime-guard' if any(c['kind']=='cancel' for c in
                 data.get('runtime_guard',{}).get('controls',[])) else execution_failure_class(error))
    counter='rate_limit_retries' if error_class=='rate-limit' else 'transport_retries'
    attempts=data.get(counter,0) if error_class in {'rate-limit','network'} else 0
    limit=rate_limit_budget(config)
    if error_class=='network':limit=TRANSPORT_RETRIES
    recovery={'status':'needs-operator','error_class':error_class,'attempts':attempts,
              'suggested_exit':failure_exit(error_class)}
    data.update(response_status=response.get('status','failed'),response_error=error,recovery=recovery)
    # No generic context reduction has been verified: full correspondence and
    # exact prior results can be required evidence. Never truncate them blindly.
    reason={'request-too-large':'CMA HTTP 413; no verified safe context reduction; local takeover required',
            'unknown':'CMA execution failed; inspect the original error and rewrite the task'}.get(
                error_class,'CMA '+error_class+' retry limit reached; operator recovery required')
    command=response_command(data) if data.get('response_id') else data
    prompt=command.get('prompt')
    lost_context=(agent_retired(data) and prompt!=data.get('prompt'))
    retry=(error_class in {'rate-limit','network'} and attempts<limit and bool(prompt)
           and bool(data.get('response_id')) and bool(data.get('agent_id')) and not lost_context)
    if retry:
        attempts+=1
        data.pop('last_error',None);data.pop('reason',None)
        recovery.update(status='retry-scheduled',attempts=attempts,
                        next_at=ledger.tasks.clock()+(retry_delay(attempts)
                                                     if error_class=='rate-limit' else 0),
                        reason='Retry the terminal CMA '+error_class+' failure')
        data[counter]=attempts
        if agent_retired(data):
            # The Agent is stopped or gone. Preserve its exact evidence, then use a
            # distinct lifecycle key; a retired Agent's create key cannot recover it.
            data=execution_restart(row,data,prompt,error_class+'-'+str(attempts))
            state='queued'
        elif error_class=='network':
            # Keep the established transport command/key and prompt semantics.
            row=ledger.tasks.update(row['id'],row['revision'],row['state'],
                                    {**data,'transport_retries':attempts-1})
            return queue_transport_retry(ledger,row,response)
        else:
            retry_command={'command_key':'outreach-command-'+row['request_hash']+'-rate-limit-'+str(attempts),
                           'source_response_id':data['response_id'],'error':error,'prompt':prompt}
            data['rate_limit_commands']=[*data.get('rate_limit_commands',[]),retry_command]
            state='admitting'
    else:
        if error_class in {'rate-limit','network'} and not (prompt and data.get('agent_id') and data.get('response_id')):
            reason='CMA retry lacks its original prompt or handles; reconcile the task locally'
            recovery['suggested_exit']='local-codex'
        elif lost_context:
            reason='Original Agent retired; correction context requires local takeover'
            recovery['suggested_exit']='local-codex'
        recovery['reason']=reason
        data['reason']=reason;state='failed'
    row=ledger.tasks.update(row['id'],row['revision'],state,data)
    ledger.save(force=True)
    return row


def reclassified_retry(data):
    """A failure parked as unknown whose saved error is now a known transient."""
    recovery=data.get('recovery') or {}
    return (recovery.get('status')=='needs-operator' and recovery.get('error_class')=='unknown'
            and (execution_failure_class(data.get('response_error'))=='rate-limit' or saved_http_failure(data)))


def recover_execution_failures(ledger,cma,config):
    """One pass per legacy row, before cleanup; scheduled retries keep their keys."""
    rows=[r for r in ledger.tasks.all() if r['state']=='failed' and
          (not r['data'].get('recovery') or r['data']['recovery']['status']=='lookup-pending' or
           reclassified_retry(r['data'])) and
          (r['data'].get('response_error') is not None or
           saved_http_failure(r['data']) or
           (r['data'].get('response_id') and
            (r['data'].get('reason')=='Original CMA response ended failed' or
             str(r['data'].get('last_error') or r['data'].get('reason','')).startswith('HTTPError:'))))]
    for row in units(ledger,'execution-recovery',rows):
        data=dict(row['data']);error=data.get('response_error')
        http_error=saved_http_failure(data)
        if http_error:
            # Mail/Mathesis effects belong to their own receipt/reconcile paths.
            if any(key in data for key in ('result','draft_intent','draft_receipt','sent_receipt','mathesis_delivery')):continue
            data['http_observation_failure']=http_error
            data.pop('last_error',None);data.pop('reason',None)
            if execution_failure_class(error)=='unknown':
                error=None  # Re-read the original response, not the HTTP observation.
            if not data.get('response_id'):
                state=('admission-unknown' if data.get('agent_id') and data.get('prompt') and
                       data.get('command_key') else 'provision-unknown' if data.get('operation_id') or
                       (data.get('create_key') and data.get('create_body')) else None)
                row=ledger.tasks.update(row['id'],row['revision'],'failed',data)
                if state:
                    exc=HTTPError('saved-observation',transient_http_status(http_error),http_error,{},None)
                    recover_transport_exception(ledger,row,exc,config,retry_state=state)
                else:
                    data.update(reason='Transient HTTP failure lacks original CMA handles; reconcile locally',
                                recovery={'scope':'observation','status':'needs-operator','error_class':'rate-limit','attempts':0,
                                          'suggested_exit':'local-codex'})
                    ledger.tasks.update(row['id'],row['revision'],'failed',data);ledger.save(force=True)
                continue
            row=ledger.tasks.update(row['id'],row['revision'],'failed',data)
        if (data.get('reason')=='Original CMA response ended failed' and data.get('agent_id') and
                data.get('response_id') and not data.get('credit_checked')):continue
        if error is None:
            try:response=cma.response(data['response_id'])
            except Exception as exc:
                if http_error and recover_transport_exception(ledger,row,exc,config,retry_state='running'):continue
                data.update(recovery={'status':'lookup-pending','error_class':'unknown','attempts':0,
                                     'suggested_exit':'retry-later','reason':'Read the original CMA response'},
                            last_error='Execution recovery lookup: '+type(exc).__name__+': '+str(exc)[:300])
                ledger.tasks.update(row['id'],row['revision'],'failed',data);ledger.save();continue
            if response.get('status')!='failed':
                # A read/admission observation failed, not necessarily the turn.
                # Reconcile through the original response, without another POST.
                data.pop('recovery',None)
                data.pop('last_error',None);data.pop('reason',None)
                ledger.tasks.update(row['id'],row['revision'],'running',data);ledger.save();continue
            error=response.get('error')
        if credit_failure({'error':error}):
            if http_error:
                data.update(reason='Original CMA response ended failed',response_error=error)
                data.pop('credit_checked',None);data.pop('recovery',None)
                ledger.tasks.update(row['id'],row['revision'],'failed',data);ledger.save()
            continue  # Billing has its own probe gate.
        recover_execution_failure(ledger,row,{'status':'failed','error':error},config)


class CreditGate:
    """A durable single probe; uncertain admissions keep their original intent."""
    def __init__(self,ledger,config):
        self.ledger=ledger;self.config=config;self.stopped=False
        record=ledger.tasks.db.execute("SELECT payload FROM pipeline_state WHERE key='credit_block'").fetchone()
        self.block=json.loads(record[0]) if record else None

    def save(self):
        if self.block:
            self.ledger.tasks.db.execute('INSERT OR REPLACE INTO pipeline_state VALUES(?,?)',
                                         ('credit_block',canonical(self.block)))
        else:self.ledger.tasks.db.execute("DELETE FROM pipeline_state WHERE key='credit_block'")

    def observe(self,task_id):
        if not self.block:
            self.block={'since':self.ledger.tasks.clock(),'last_probe_at':None,'probe_task':None}
        if self.block['probe_task']==task_id:self.block['probe_task']=None
        self.stopped=True  # Even a later successful probe cannot reopen this tick.
        self.save()

    def settled(self,task_id):
        if self.block and self.block['probe_task']==task_id:
            self.block=None;self.save()

    def accepted(self,row,response):
        """A billing refusal arrives as a prompt failure. A probe turn still in
        progress well after admission has passed the billing check, so the
        gate opens without waiting for the whole turn (2026-11-03: one long
        Mathesis turn held every admission for hours)."""
        if (self.block and self.block.get('probe_task')==row['id'] and response.get('status')=='in_progress'
                and self.block.get('last_probe_at') is not None
                and self.ledger.tasks.clock()-self.block['last_probe_at']>=CREDIT_PROBE_ACCEPT):
            self.block=None;self.save()

    def allow(self,row,*,reserve=True):
        if self.stopped:return False
        if not self.block:return True
        if self.block['probe_task']:return self.block['probe_task']==row['id']
        waiting=[r for r in self.ledger.tasks.all() if r['state']=='credit-wait'
                 and not r['data'].get('credit_retry_exhausted')
                 and self.config['kinds'].get(r['kind'],False)]
        oldest=min(waiting,key=lambda r:r['data'].get('credit_wait_since',r['updated']),default=None)
        if oldest is None and row['state']=='queued':
            # No retained Agent can probe (e.g. recovered tasks whose Agents were
            # already deleted): the first queued task in dispatch order probes.
            if reserve:self.probe(row)
            return True
        return bool(oldest and oldest['id']==row['id'])

    def probe(self,row):
        if self.block and not self.block['probe_task']:
            self.block.update(last_probe_at=self.ledger.tasks.clock(),probe_task=row['id'])
            self.save()  # The caller force-saves before create/admit.


def admission_command(data):
    for key in ('credit_commands','transport_commands','rate_limit_commands','repairs'):
        if data.get(key) and not data[key][-1].get('response_id'):return data[key][-1]
    return data


def response_command(data):
    return next((c for c in [*data.get('credit_commands',[]),*data.get('transport_commands',[]),
                            *data.get('rate_limit_commands',[]),*data.get('repairs',[])]
                 if c.get('response_id')==data['response_id']),data)


def queue_credit_retry(ledger,row,response):
    data=dict(row['data']);attempt=data.get('credit_retries',0)+1
    data.pop('last_error',None)
    data.update(response_status='failed',response_error=response['error'],
                reason='CMA billing credits unavailable; waiting for credit probe',
                credit_wait_since=data.get('credit_wait_since',ledger.tasks.clock()))
    if attempt<=CREDIT_RETRIES:
        command=response_command(data)
        retry={'command_key':'outreach-command-'+row['request_hash']+'-credit-'+str(attempt),
               'source_response_id':data['response_id'],'error':response['error'],'prompt':command['prompt']}
        data.update(credit_retries=attempt,credit_commands=[*data.get('credit_commands',[]),retry])
    else:
        data.update(credit_retry_exhausted=True,reason='CMA credit retry limit reached; billing recovery needs attention')
    row=ledger.tasks.update(row['id'],row['revision'],'credit-wait',data)
    ledger.save(force=True)
    return row


def recover_credit_failures(ledger,cma,gate):
    """Classify each legacy failure once, before cleanup or any admission."""
    rows=[r for r in ledger.tasks.all() if r['state']=='failed'
          and r['data'].get('reason')=='Original CMA response ended failed'
          and r['data'].get('agent_id') and r['data'].get('response_id') and not r['data'].get('credit_checked')]
    for row in units(ledger,'credit-recovery',rows):
        data=dict(row['data'])
        try:
            error=data.get('response_error')
            if error is None:error=cma.response(data['response_id']).get('error')
        except Exception as exc:
            ledger.tasks.update(row['id'],row['revision'],'failed',
                {**data,'last_error':'Credit recovery lookup: '+type(exc).__name__+': '+str(exc)[:300]})
            ledger.save();continue  # No cleanup until the original failure is known.
        data.update(response_error=error,credit_checked=True)
        if not credit_failure({'error':error}):
            ledger.tasks.update(row['id'],row['revision'],'failed',data);ledger.save();continue
        gate.observe(row['id'])
        data['credit_recovered']=True
        if data.get('cleanup',{}).get('deleted'):
            # A new lifecycle needs a new key, while the original evidence stays intact.
            restart={k:v for k,v in data.items() if k in
                     {'capacity_retries','transport_retries','credit_retries','previous_operations',
                      'thread_inputs','resolution_retries'} or k.startswith('credit_')}
            restart.update(credit_recovery=data,credit_waiting=True,credit_wait_since=ledger.tasks.clock())
            ledger.tasks.update(row['id'],row['revision'],'queued',restart);ledger.save(force=True)
        else:
            row=ledger.tasks.update(row['id'],row['revision'],'failed',data)
            queue_credit_retry(ledger,row,{'error':error})


def queue_transport_retry(ledger, row, response):
    data=dict(row['data'])
    attempt=data.get('transport_retries',0)+1
    command=response_command(data)
    retry={'command_key':'outreach-command-'+row['request_hash']+'-transport-'+str(attempt),
           'source_response_id':data['response_id'],'error':response['error'],'prompt':command['prompt']}
    data.update(transport_retries=attempt,transport_commands=[*data.get('transport_commands',[]),retry])
    row=ledger.tasks.update(row['id'],row['revision'],'admitting',data)
    ledger.save(force=True)  # Preserve the retry key/prompt, including across ticks.
    return row


def self_contact_ids(config,root):
    account=(config.get('mailbox') or {}).get('account')
    if not account:return frozenset()
    try:
        from .registry import people
        return frozenset(p['id'] for p in people(root) if email(account) in p['emails'])
    except (OSError,ValueError,KeyError):
        return frozenset()


def stage_reserves(budget):
    """Per-tick slices kept for stages that otherwise starve.

    Advance: mailbox intake alone took ~300 s on 2026-11-08 and advance never
    reached repairs or queued work. Delivery: it got 0 units for consecutive
    ticks after intake and advance, so reviewed first contacts never left.
    Mathesis (publication loop and Mathesis delivery): on 2026-11-14 it ran
    0 units per tick after pipeline (~140 s) and advance (~310 s), so PASSed
    notes waited many hours for their DOI and Mathesis collection. Sent Items
    has its own slice so delivery cannot consume all of its recording time.
    Inbox intake keeps a dedicated slice when ingest exhausts setup.
    Pipeline reconciliation (new short-note tasks, Mathesis tasks from PASSed
    notes) shared the setup lane after ingest and intake and ran 0 units on
    seven of eight sampled ticks on 2026-11-20; it needs about 20 s.
    """
    env=os.environ.get
    return {'advance':min(float(env('OUTREACH_ADVANCE_RESERVE_SECONDS','270')),0.45*budget),
            'delivery':min(float(env('OUTREACH_DELIVERY_RESERVE_SECONDS','90')),0.15*budget),
            'mathesis':min(float(env('OUTREACH_MATHESIS_RESERVE_SECONDS','90')),0.15*budget),
            'sent-events':min(float(env('OUTREACH_SENT_EVENTS_RESERVE_SECONDS','30')),0.05*budget),
            'mailbox-intake':min(float(env('OUTREACH_INTAKE_RESERVE_SECONDS','60')),0.10*budget),
            'pipeline':min(float(env('OUTREACH_PIPELINE_RESERVE_SECONDS','30')),0.05*budget)}


def dispatch_order(rows, config=None, correspondence=(), deferred=None):
    """Order CMA work, giving the reply lane the first queued admission pass."""
    # Advance running/finished work first: collecting results and retiring their
    # Sandboxes frees CMA capacity before any queued task tries to start.
    ordered=sorted(rows,key=lambda r:(r['state']=='queued',
                                     0 if r['request']['inputs'].get('automation_parent') else 1,
                                     0 if r['request']['inputs'].get('owner_ids') else 1))
    config = config or {}
    ordered = interleave(ordered, config, correspondence)
    if correspondence_priority(config):
        deferred = deferred or {}
        ordered.sort(key=lambda r: (
            r['state'] == 'queued',
            r['state'] == 'queued' and lane_of(r, config, correspondence) != 'reply',
            r['state'] == 'queued' and lane_of(r, config, correspondence) != 'correspondence',
            deferred.get(r['id'], len(deferred)) if r['state'] == 'queued' else 0))
    return ordered


def queued_gates(row,tasks,revisions=None):
    """Read-only policy checks shared by admission and kind demand detection."""
    from .pipeline import source_gate
    from .automation import parent_gate, paper_gate, owner_gate
    from .thread_staging import gate as thread_gate
    if row['kind'] == 'mathesis':
        reason = (revisions or note_revisions.Revisions(tasks)).reason(row)
        require(reason is None, reason)
    source_gate(row['request'],tasks,ROOT)
    note_closure.source_gate(row['request'],tasks)
    parent_gate(row['request'],tasks)
    paper_gate(row['request'],ROOT)
    owner_gate(row['request'],ROOT)
    firstcontact_gate(row['request'],ROOT)
    thread_gate(row['request'],ROOT)


def lane_demand(row,tasks,config,active,ignore,revisions=None):
    """Held, disabled, delayed or policy/owner-blocked work reserves no share.

    This uses local evidence only; mailbox hydration still runs at admission.
    The caller scopes competing demand to lanes that share Sandbox slots.
    """
    if row['state']!='queued' or not config['kinds'].get(row['kind'],False):return False
    recovery=row['data'].get('recovery',{})
    if recovery.get('status')=='lookup-pending':return False
    if recovery.get('status')=='retry-scheduled' and tasks.clock()<recovery['next_at']:return False
    if not matches_workflow(row,workflow(row['kind'])):return False
    if any(owners(r,ignore)&owners(row,ignore) for r in active):return False
    try:queued_gates(row,tasks,revisions)
    except ValueError:return False
    return True


def malformed_result(row):
    return (row['state']=='held' and 'result' not in row['data']
            and row['data'].get('reason','').startswith(RESULT_REPAIR_REASON))


def queue_result_repair(ledger,row,config,ignore=frozenset()):
    """Checkpoint one correction turn on the original Agent before admission."""
    data=dict(row['data'])
    if (not malformed_result(row) or config['paused'] or not config['kinds'].get(row['kind'],False)
            or not data.get('agent_id') or not data.get('response_id') or data.get('cleanup',{}).get('deleted')
            or data.get('repair_attempts',0)>=RESULT_REPAIR_LIMIT):
        return row
    active=[r for r in ledger.tasks.all() if r['state'] in ACTIVE]
    if len(active)>=config['max_active'] or any(owners(r,ignore)&owners(row,ignore) for r in active):
        return row
    attempt=data.get('repair_attempts',0)+1
    error=data.get('repair_error')
    if error is None:
        # Old holds predate specific contract errors; diagnose their saved bytes
        # before asking the original Agent to correct them.
        try:validate_result(json.loads(data.get('response_text','')),row)
        except (ValueError,KeyError,TypeError) as exc:error=str(exc)
        if error is None:error=data['reason'][len(RESULT_REPAIR_REASON):]
    repair={'command_key':'outreach-repair-'+row['request_hash']+'-'+str(attempt),
            'source_response_id':data['response_id'], 'response_text':data.get('response_text',''),
            'error':error,
            'prompt':('Correct the final result for the same Outreach task '+row['id']+'.\n'
                      'Return only the corrected final JSON object for the same task. Do not redo the research. '
                      'Keep artifacts unchanged unless the error concerns them. All original task and policy gates still apply.\n'
                      'Exact validation error:\n'+error)}
    data.update(repair_attempts=attempt,repairs=[*data.get('repairs',[]),repair])
    row=ledger.tasks.update(row['id'],row['revision'],'admitting',data)
    ledger.save(force=True)  # Corrections replay this exact command, never a new turn.
    return row


def advance(ledger,cma,config,folder,revision=None,mailbox=None,oracle_capacity=ORACLE_CAPACITY_UNSET):
    validate_lanes(config)
    from .sandbox_pool import POOL_WAIT_SECONDS, SandboxPool
    from .pipeline import source_gate
    from .automation import approved_route, mail_routing_override, parent_gate, owner_gate
    from .automation import paper_gate
    tasks=ledger.tasks;updates=[]
    suffixes=home_domains(config)
    tick=getattr(ledger,'tick',None)
    advance_clock=tick.clock if tick else ledger.clock
    advance_started=advance_clock()
    last_artifact_save=advance_started
    unit_seconds=None
    if tick:
        unit_seconds=float(os.environ.get('OUTREACH_RESULT_UNIT_BUDGET_SECONDS','240'))
        require(math.isfinite(unit_seconds) and unit_seconds>0,'Invalid result unit budget seconds')
    if oracle_capacity is ORACLE_CAPACITY_UNSET:oracle_capacity=oracle_capacity_snapshot(tasks)
    history_tick_started=False
    # The mailbox owner appears in every thread; a legacy table-1 row for that
    # address must not serialize all scholars' work behind one owner.
    mailbox_self=self_contact_ids(config,ROOT)
    initial_rows = tasks.all()
    correspondence = linked_contacts(initial_rows, ROOT, config)
    revisions = note_revisions.Revisions(tasks, initial_rows) if config['kinds'].get('mathesis') else None
    pool=SandboxPool(ledger,config['sandbox_pool'],config['kinds'],paused=config['paused'],
                     correspondence=correspondence) if config.get('sandbox_pool') else None
    grouped=bool(pool and config['sandbox_pool'].get('groups'))
    failure_pool=pool or SandboxPool(ledger,{'max_sandboxes':1,'agents_per_sandbox':1})
    credit=CreditGate(ledger,config)
    with phase(ledger,'credit-recovery'):recover_credit_failures(ledger,cma,credit)
    with phase(ledger,'execution-recovery'):recover_execution_failures(ledger,cma,config)
    with phase(ledger,'pool-health'):
        if failure_pool:failure_pool.health(cma)
    # Limit fresh Sandbox creates after capacity failures in the same group.
    capacity_limited=set()
    # A repinned Agent hold still owns its original cleanup handles.
    rows=[r for r in tasks.all() if (r['state']!='superseded' or
          r['data'].get('reason')==WORKFLOW_PIN_MISMATCH) and
          (r['state'] in ACTIVE|{'queued','approved'} or r['data'].get('agent_id') or
           r['data'].get('operation_id') or (r['state']=='held' and unstarted(r)) or policy_mismatch_hold(r))]
    fetch_revision = getattr(ledger.releases, 'fetch_workflow_revision', None)
    fetched_revisions = set()
    pin_checks = {}
    repins = 0
    unavailable_repins = 0

    current_policy = current_policy_revision()

    def read_pinned_workflow(kind, source, policy=None):
        key = (kind, source, policy)
        if key not in pin_checks:
            try:
                pin_checks[key] = request_binding(kind, source, policy)['workflow_hash']
            except (ValueError, FileNotFoundError):
                pin_checks[key] = None
                pins = ((source, True), (policy, False)) if policy else ((source, False),)
                for commit, engine in pins:
                    pin = (commit, engine)
                    if not fetch_revision or pin in fetched_revisions or len(fetched_revisions) >= WORKFLOW_REPIN_LIMIT:
                        continue
                    fetched_revisions.add(pin)
                    with unit(ledger, 'advance', commit, 'workflow-source') as ready:
                        if ready:
                            if engine:
                                fetch_revision(commit, engine=True)
                            else:
                                fetch_revision(commit)
                try:
                    pin_checks[key] = request_binding(kind, source, policy)['workflow_hash']
                except (ValueError, FileNotFoundError):
                    pass
        return pin_checks[key]

    collections=[];collecting=set()
    defer_collections=any(r['state']=='queued' for r in rows)
    def advance_order():
        # Observe live work and capacity/credit failures first. Completed
        # responses are checkpointed here; their expensive file collection
        # follows an admission pass, using the ordinary owner/lane/pool gates.
        # Deferred queued work keeps its priority within each admission pass.
        # Refresh rows routed in the cheap approved pass so their ordinary
        # cleanup can still run, budget permitting. Do not retry routing errors.
        tick=getattr(ledger,'tick',None)
        priority={key:i for i,key in enumerate(tick.previous.get('advance',[]))} if tick else {}
        ordered=dispatch_order([tasks.get(r['id']) if r['state']=='approved' else r for r in rows],
                               config, correspondence, priority)
        ordered=[r for r in ordered if r['state']!='approved']
        deferred=set()
        work=ordered+history_rows
        prioritize_queued = correspondence_priority(config)
        if tick:
            work=sorted(work,key=lambda r:priority.get(r['id'],len(priority)))
            if not prioritize_queued:
                ordered=[r for r in work if 'history_task' not in r]
            deferred=set(priority)
        # 1) observe live CMA work/recover failures, 2) admit queued work,
        # 3) collect completed results and retry newly available capacity,
        # 4) cleanup-only rows. Slot-releasing stops still precede admissions;
        # deletion confirmations stay at the end.
        def first(r):
            # Live work plus failed provisioning rechecks (no Agent yet), which
            # decide capacity for this tick's admissions.
            return r['state']!='queued' and (r['state'] in ACTIVE or
                (r['state']=='failed' and not r['data'].get('agent_id')))
        def holds_slot(r):
            # Finished work whose Agent stop is not yet accepted still reserves a
            # pool slot. Retirement is one or two requests, so free those slots
            # before admissions; deletion confirmations stay at the end.
            cleanup=r['data'].get('cleanup') or {}
            return (r['state']!='queued' and not first(r) and bool(r['data'].get('agent_id'))
                    and not (cleanup.get('deleted') or cleanup.get('stopped') or
                             cleanup.get('status') in {'stopping','stopped','deleting','deleted'}))
        # Rows on a pool Sandbox CMA reports outside HEALTHY (credential_rotating
        # for hours on 2026-11-08) only make slow, failing calls; they wait
        # behind admissions so a healthy Sandbox does not sit empty.
        from .sandbox_pool import HEALTHY
        entries=getattr(failure_pool,'entries',None)
        unhealthy={e['sandbox_id'] for e in (entries if isinstance(entries,list) else [])
                   if e.get('state') not in HEALTHY}
        def stalled(r):
            d=r['data']
            return r['state']!='queued' and bool(unhealthy) and (
                d.get('sandbox_id') in unhealthy or d.get('pool_sandbox_id') in unhealthy)
        # Saved deferrals precede fresh work across all tiers. Sorting within
        # tiers alone left cleanup/unhealthy rows behind live polling forever.
        for r in work:
            if r['id'] not in deferred:continue
            if prioritize_queued and r.get('state') == 'queued':continue
            # A saved completion already had its admission pass on an earlier
            # tick; collect it now rather than putting it behind that pass again.
            saved=r.get('data',{}).get('completed_response') or {}
            if saved and saved.get('response_id')==r['data'].get('response_id'):
                collecting.add(r['id'])
            yield r
        ordered_live=[r for r in ordered if not stalled(r)]
        queued=[r for r in ordered_live if r['state']=='queued']
        ordered_live=[r for r in ordered_live if r['id'] not in deferred]
        yield from [r for r in ordered_live if first(r)]
        yield from [r for r in ordered_live if holds_slot(r)]
        yield from [r for r in queued if prioritize_queued or r['id'] not in deferred]
        # Response handles/text are already durable. No second CMA response
        # read is needed, and a free/new Sandbox has had an admission chance.
        # The collection window starts here: with ~190 queued rows the admission
        # pass alone outlasted a window counted from the stage start, so every
        # completed result was deferred on each tick while its Agent kept a
        # pool slot (2026-11-12..13: all six slots of the only healthy Sandbox).
        if collections:result_window['start']=advance_clock()
        for r in collections:
            collecting.add(r['id'])
            yield r
        # Collection/stop may have released capacity that was full above.
        # Retry only still-queued rows, never an admitted task's command.
        if collections:
            yield from [r for r in queued if tasks.get(r['id'])['state']=='queued']
        yield from [r for r in ordered_live if r['state']!='queued' and not first(r) and not holds_slot(r)]
        yield from [r for r in ordered if stalled(r) and r['id'] not in deferred]
        yield from [r for r in history_rows if r['id'] not in deferred]
    # Completed-result collection (artifact readback and preservation) can take
    # 20-200 s per task. Count recovery/health time too and check BEFORE polling
    # a response or beginning collection, rather than after the response read.
    # Collection also checks the tick and per-result deadlines between files.
    result_window={'start':advance_started,'limit':float(os.environ.get('OUTREACH_RESULT_BUDGET_SECONDS','120'))}
    def result_ready(row):
        ready=(advance_clock()-result_window['start']<result_window['limit'] and
               recheck.due(tasks,'result',row['id'],row['data'].get('response_id')))
        if not ready and tick:
            pending=tick.pending.setdefault('advance',[])
            if row['id'] not in pending:pending.append(row['id'])
        return ready
    snapshot={'version':None,'rows':None}
    from collections import Counter
    skips=Counter()  # content-free admission decisions, logged once per tick
    def current_rows():
        # One full-ledger read per state change instead of two per queued row.
        version=ledger.version()
        if snapshot['version']!=version:
            snapshot.update(version=version,rows=tasks.all())
        return snapshot['rows']
    def active_count():
        marks=','.join('?'*len(ACTIVE))
        return tasks.db.execute('SELECT COUNT(*) FROM tasks WHERE state IN ('+marks+')',tuple(ACTIVE)).fetchone()[0]
    # Historical Agent cleanup joins the same fair work queue. Keeping a
    # separate trailing pass left it permanently behind live work as well.
    history_rows=[];history_retired=0
    for row in tasks.all():
        if row['state'] in ACTIVE:continue
        data=row['data']
        for index,entry in enumerate(data.get('execution_history',[])):
            if (not entry.get('agent_id') or entry['agent_id']==data.get('agent_id') or
                    (entry.get('cleanup') or {}).get('deleted') or
                    entry.get('cleanup_errors',0)>=3):continue
            history_rows.append({'id':row['id']+'-history-'+str(index),
                                 'history_task':row['id'],'history_index':index})
    def advance_rows():
        # Local policy checks and one state update must not wait for CMA reads,
        # artifact collection or retirement, even if an earlier phase overran.
        with phase(ledger,'approved'):
            yield from [r for r in rows if r['state']=='approved']
        yield from units(ledger,'advance',advance_order(),timed=False,preserve_order=True)
    for original in advance_rows():
        if 'history_task' in original:
            if history_retired>=RETIRE_HISTORY_PER_TICK:
                if tick:tick.pending.setdefault('advance',[]).append(original['id'])
                continue
            row=tasks.get(original['history_task']);data=dict(row['data'])
            if row['state'] in ACTIVE:continue
            history=list(data.get('execution_history',[]));index=original['history_index'];entry=dict(history[index])
            history_retired+=1
            try:
                entry['cleanup']=cma.retire(entry['agent_id'],
                    'outreach-retire-history-'+row['request_hash']+'-'+str(index),None)
                entry.pop('cleanup_error',None)
            except Exception as exc:
                entry['cleanup_error']=type(exc).__name__+': '+str(exc)[:300]
                entry['cleanup_errors']=entry.get('cleanup_errors',0)+1
            history[index]=entry;data['execution_history']=history
            tasks.update(row['id'],row['revision'],row['state'],data);ledger.save()
            continue
        row=tasks.get(original['id']);data=dict(row['data'])
        if row['state']=='superseded' and data.get('reason') != WORKFLOW_PIN_MISMATCH:continue
        group=pool.group(row) if grouped else None
        if (pool and row['state'] in ACTIVE and data.get('pool_create') and
                not data.get('workspace_id') and 'pool_create_started_at' not in data):
            # Backfill once before observations update the row's timestamp.
            data['pool_create_started_at']=row['updated']
            row=tasks.update(row['id'],row['revision'],row['state'],data);ledger.save()
        if row['state']=='running' and not result_ready(row):continue
        recovery=data.get('recovery',{})
        if recovery.get('scope')=='observation' and recovery.get('status')=='needs-operator':continue
        if recovery.get('status')=='lookup-pending':continue
        if recovery.get('status')=='retry-scheduled' and (config['paused'] or
                not config['kinds'].get(row['kind'],False) or tasks.clock()<recovery['next_at']):continue
        if row['state'] in {'running','admitting','admission-unknown','provisioning','provision-unknown'}:
            try:
                if sandbox_failure(ledger,cma,row,failure_pool):continue
            except Exception as exc:
                current=tasks.get(row['id']);data=dict(current['data'])
                if recover_transport_exception(ledger,current,exc,config):continue
                data['last_error']=type(exc).__name__+': '+str(exc)[:300]
                tasks.update(current['id'],current['revision'],current['state'],data);ledger.save();continue
        if (row['state']=='failed' and data.get('reason')=='Original CMA response ended failed'
                and data.get('agent_id') and data.get('response_id') and not data.get('credit_checked')):
            continue  # A failed recovery read must not destroy a potentially reusable Agent.
        if (row['state']=='held' and (data.get('reason')==LEGACY_RESOLUTION_REASON or
                data.get('reason','').startswith('Thread resolution failed: HTTPError: HTTP Error 401'))
                and unstarted(row)
                and row['kind'] in {'reply','research'} and row['request']['inputs'].get('thread_ref')
                and not config['paused'] and config['kinds'].get(row['kind'],False)):
            data.pop('reason')
            row=tasks.update(row['id'],row['revision'],'queued',data);ledger.save()
        if (group not in capacity_limited and row['state']=='failed' and data.get('reason')=='CMA provisioning failed; retained original operation'
                and data.get('operation_id') and not data.get('agent_id') and (not data.get('capacity_checked') or
                    (not data.get('runtime_checked') and data.get('runtime_retries',0)<RUNTIME_RETRIES))
                and not config['paused'] and config['kinds'].get(row['kind'],False)):
            with unit(ledger,'advance',row['id'],'capacity-read') as ready:
                if not ready:continue
                try:op=cma.operation(data['operation_id'])
                except Exception:continue
            data.update(operation_error=op.get('error'),runtime_checked=True)
            at_capacity=capacity_failure(op)
            if at_capacity:
                capacity_limited.add(group)
                if pool and data.get('pool_create'):pool.record_capacity_failure(row)
            if at_capacity and data.get('capacity_retries',0)<CAPACITY_RETRIES:
                tasks.update(row['id'],row['revision'],'queued',requeue(data));ledger.save()
            elif runtime_failure(op) and data.get('runtime_retries',0)<RUNTIME_RETRIES:
                tasks.update(row['id'],row['revision'],'queued',runtime_requeue(data));ledger.save()
            else:
                tasks.update(row['id'],row['revision'],'failed',{**data,'capacity_checked':True});ledger.save()
            continue
        waiting = row['state'] in {'queued', 'held'} and unstarted(row)
        identity_hold = policy_mismatch_hold(row)
        if waiting or identity_hold or row['state'] == 'queued':
            if config['paused'] or not config['kinds'].get(row['kind'],False):continue
            mismatch = pinned_workflow_mismatch(row, read_pinned_workflow) if waiting or identity_hold else False
            stale_hold = waiting and row['state'] == 'held' and data.get('reason') == WORKFLOW_CHANGED
            stale_queue = row['state'] == 'queued' and not matches_workflow(row, workflow(row['kind']))
            if mismatch or stale_hold or stale_queue:
                if mismatch and repins >= WORKFLOW_REPIN_LIMIT:
                    continue
                if waiting or identity_hold:
                    revision = revision or source_head(ENGINE_ROOT)
                    current_hash = request_binding(row['kind'], revision, current_policy)['workflow_hash']
                    if mismatch or stale_hold or not matches_workflow(row, current_hash):
                        reason = WORKFLOW_PIN_MISMATCH if mismatch else WORKFLOW_CHANGED
                        tasks.repin(row['id'], row['revision'], current_hash, revision, reason=reason,
                                    policy_revision=current_policy)
                        repins += int(bool(mismatch))
                        ledger.save();updates.append(row['id']);continue
                else:
                    tasks.update(row['id'],row['revision'],'held',{**data,'reason':WORKFLOW_CHANGED})
                    ledger.save();updates.append(row['id']);continue
            if mismatch is None and row['state'] == 'queued' and (fetch_revision or row['request'].get('policy_revision')):
                # The pinned commit cannot be read in this checkout, so the
                # pair cannot be verified. Skipping starved every older queued
                # task (2026-11-20: 224 short notes, 0 active). Repin unstarted
                # work to the tick's own commit, where the pair is consistent.
                revision = revision or source_head(ENGINE_ROOT)
                old_request = row['request']
                old_pins = (old_request['source_revision'], old_request.get('policy_revision'))
                if (unstarted(row) and old_pins != (revision, current_policy)
                        and unavailable_repins < WORKFLOW_UNAVAILABLE_REPIN_LIMIT):
                    binding = request_binding(row['kind'], revision, current_policy)
                    tasks.repin(row['id'], row['revision'], binding['workflow_hash'], revision,
                                reason=WORKFLOW_PIN_MISMATCH, policy_revision=current_policy)
                    unavailable_repins += 1
                    ledger.save();updates.append(row['id']);continue
                skips['workflow-source-unavailable'] += 1
                if tick and ((old_pins[0], bool(old_pins[1])) not in fetched_revisions
                             or (old_pins[1] and (old_pins[1], False) not in fetched_revisions)):
                    tick.pending.setdefault('advance', []).append(row['id'])
                continue
        if row['state']=='queued':
            # The account Sandbox limit only blocks creating a Sandbox. With a
            # pool, free slots in existing Sandboxes still admit work (checked
            # after pool.slot below); 2026-11-08 one failed 4th-Sandbox create
            # per tick had idled five free slots for hours.
            if group in capacity_limited and not pool:skips['capacity-limited']+=1;continue
            # Cheap global capacity check before gates and full-ledger scans.
            if active_count()>=config['max_active']:skips['max-active']+=1;continue
            try:
                queued_gates(row,tasks,revisions)
            except ValueError as exc:
                skips['gate-held']+=1
                tasks.update(row['id'],row['revision'],'held',{**data,'reason':str(exc)});ledger.save();continue
            current=current_rows();active=[r for r in current if r['state'] in ACTIVE]
            if any(owners(r,mailbox_self)&owners(row,mailbox_self) for r in active):skips['owner-busy']+=1;continue
            if len(active)>=config['max_active']:skips['max-active']+=1;continue
            if at_share(row,current,config,
                        lambda candidate:lane_demand(candidate,tasks,config,active,mailbox_self,revisions),
                        correspondence):
                skips['lane-share:'+lane_of(row,config,correspondence)]+=1;continue
            if not credit.allow(row,reserve=not grouped):skips['credit']+=1;continue
            pool_entry=None
            if pool:
                available,pool_entry=pool.slot(row,cma=cma,rows=current_rows())
                if not available:skips['pool-slot']+=1;continue
                if group in capacity_limited and pool_entry is None:skips['capacity-limited']+=1;continue
            skips['passed-gates:'+row['kind']]+=1
            inputs=row['request']['inputs']
            resolve_thread=row['kind'] in {'reply','research'} and inputs.get('thread_ref') and not inputs.get('thread')
            if resolve_thread or row['kind']=='firstcontact':
                label='Thread resolution' if resolve_thread else 'First-contact mailbox history'
                retries_key='resolution_retries' if resolve_thread else 'mailbox_history_retries'
                if not resolve_thread:
                    from . import firstcontact_history
                    data['mailbox_history']=firstcontact_history.snapshot(tasks.clock)
                with unit(ledger,'advance',row['id'],'thread-resolution' if resolve_thread else 'firstcontact-history') as ready:
                    if not ready:continue
                    try:
                        if mailbox is None:
                            from .mailbox import configured
                            mailbox=configured(config,ROOT)
                        if resolve_thread:
                            from .thread_staging import resolve
                            data['thread_inputs']=resolve(row,mailbox,ledger,ROOT)
                        else:
                            if not history_tick_started:
                                mailbox.begin_history_tick()
                                history_tick_started=True
                            with measure(ledger,'firstcontact-history'):
                                firstcontact_history.resolve(row,mailbox,ledger,ROOT,data['mailbox_history'])
                        data.pop('last_error',None)
                        row=tasks.update(row['id'],row['revision'],'queued',data)
                        if resolve_thread:row=tasks.supersede_reply_drafts(row['id'])
                        ledger.save()
                    except Exception as exc:
                        from urllib.error import HTTPError, URLError
                        transient=(isinstance(exc,HTTPError) and exc.code in TRANSIENT_HTTP) or (
                            isinstance(exc,(URLError,TimeoutError,ConnectionError)) and not isinstance(exc,HTTPError))
                        retries=data.get(retries_key,0)
                        if transient and retries<RESOLUTION_RETRIES:
                            # A token rotation or Graph outage is not a thread defect;
                            # stay queued and resolve again on a later tick.
                            tasks.update(row['id'],row['revision'],'queued',{
                                **data,retries_key:retries+1,
                                'last_error':label+' retry: '+type(exc).__name__+': '+str(exc)[:300]})
                        else:
                            tasks.update(row['id'],row['revision'],'held',{
                                **data,'reason':label+' failed: '+type(exc).__name__+': '+str(exc)[:300]})
                        ledger.save();continue
            # Replies need the same fresh, coordinator-owned mailbox evidence as
            # first contacts, but their counterpart addresses come from every
            # hydrated current/prior conversation rather than recipient_emails.
            if row['kind']=='reply' and (data.get('thread_inputs', {}).get('thread') or
                                         row['request']['inputs'].get('thread')):
                from . import firstcontact_history
                data['mailbox_history']=firstcontact_history.snapshot(tasks.clock)
                with unit(ledger,'advance',row['id'],'reply-mailbox-history') as ready:
                    if not ready:continue
                    try:
                        if mailbox is None:
                            from .mailbox import configured
                            mailbox=configured(config,ROOT)
                        if not history_tick_started:
                            mailbox.begin_history_tick()
                            history_tick_started=True
                        with measure(ledger,'reply-mailbox-history'):
                            firstcontact_history.resolve_reply(row,mailbox,ledger,ROOT,data['mailbox_history'])
                        data.pop('last_error',None)
                        row=tasks.update(row['id'],row['revision'],'queued',data);ledger.save()
                    except Exception as exc:
                        from urllib.error import HTTPError, URLError
                        transient=(isinstance(exc,HTTPError) and exc.code in TRANSIENT_HTTP) or (
                            isinstance(exc,(URLError,TimeoutError,ConnectionError)) and not isinstance(exc,HTTPError))
                        retries=data.get('mailbox_history_retries',0)
                        label='Reply mailbox history'
                        if transient and retries<RESOLUTION_RETRIES:
                            tasks.update(row['id'],row['revision'],'queued',{
                                **data, 'mailbox_history_retries':retries+1,
                                'last_error':label+' retry: '+type(exc).__name__+': '+str(exc)[:300]})
                        else:
                            tasks.update(row['id'],row['revision'],'held',{
                                **data,'reason':label+' failed: '+type(exc).__name__+': '+str(exc)[:300]})
                        ledger.save();continue
            with unit(ledger,'advance',row['id'],'claim') as ready:
                if not ready:continue
                if grouped:credit.probe(row)
                row=tasks.claim(row['id'],config['max_active'],mailbox_self)
                profile=dict(config['agent_template'].get('profile',{}))
                profile['system_prompt']='You execute one private Outreach task. Read AGENTS.md and managed/WORKSPACE.md. Never send or publicly publish. All judgments require evidence.'
                body={**config['agent_template'],'name':'outreach-'+row['id'],'profile':profile}
                if pool_entry:
                    body={k:v for k,v in body.items() if k not in {'image','repository','sandbox_id','machine_included'}}
                    body['workspace_id']=pool_entry['workspace_id']
                # repository auto-setup pins CMA's default OAuth connection. Its
                # measured 404 for our private repo creates an irrelevant approval
                # wait; the task prompt stages the pinned source through the actual
                # existing PAT connection instead.
                retries=row['data'].get('capacity_retries',0)
                data={**row['data'],'create_body':body,'create_key':'outreach-create-'+row['request_hash']+
                      ('-'+pool_entry['workspace_id'] if pool_entry else '')+('-r%d'%retries if retries else '')+
                      ('-admission%d'%row['data']['admission_rejections'] if row['data'].get('admission_rejections') else '')+
                      ('-sandbox%d'%row['data']['sandbox_failures'] if row['data'].get('sandbox_failures') else '')+
                      ('-runtime%d'%row['data']['runtime_retries'] if row['data'].get('runtime_retries') else '')+
                      ('-credit-recovery' if row['data'].get('credit_recovered') else '')+
                      ('-execution-'+row['data']['execution_restart'] if row['data'].get('execution_restart') else '')}
                if pool_entry:
                    data.update(pool_sandbox_id=pool_entry['sandbox_id'],sandbox_id=pool_entry['sandbox_id'],
                                workspace_id=pool_entry['workspace_id'])
                elif pool:
                    data['pool_create']=True
                    data.setdefault('pool_create_started_at',tasks.clock())
                if grouped:
                    data['pool_group']=group
                    if row['id'] in correspondence:
                        data['pool_lane']='correspondence'
                    else:
                        data.pop('pool_lane',None)
                if retries:data.update(capacity_retries=retries,previous_operations=row['data'].get('previous_operations',[]))
                row=tasks.update(row['id'],row['revision'],'provisioning',data)
                if pool_entry:pool.assigned(pool_entry)
                if pool:pool.observe_live(tasks.all())
                ledger.save()
        try:
            if row['state']=='approved':
                try:
                    if row['kind'] == 'mathesis':
                        note_revisions.gate(row, tasks)
                    source_gate(row['request'],tasks,ROOT)
                    note_closure.source_gate(row['request'],tasks,data.get('result'))
                    parent_gate(row['request'],tasks)
                    paper_gate(row['request'],ROOT)
                    owner_gate(row['request'],ROOT)
                    from .thread_staging import gate as thread_gate
                    thread_gate(row['request'],ROOT,data.get('thread_inputs'))
                except ValueError as exc:
                    tasks.update(row['id'],row['revision'],'held',{**data,'reason':str(exc)});ledger.save();continue
                if row['kind']=='firstcontact':
                    try:
                        if data.get('result',{}).get('routing',{}).get('mail')!='none':
                            firstcontact_gate({**row['request'], 'inputs':effective_inputs(row)},ROOT)
                    except ValueError as exc:
                        tasks.update(row['id'],row['revision'],'held',{**data,'reason':str(exc)});ledger.save();continue
                # These adapters are deliberately deferred. Review is a quality
                # decision; no mailbox/publication permission is synthesized.
                override=mail_routing_override(row,suffixes)
                state=approved_route(row,override)
                data.pop('routing_override',None)
                if override is not None:data['routing_override']=override
                tasks.update(row['id'],row['revision'],state,data);ledger.save()
                updates.append(row['id']);continue
            if not credit.block and not credit.stopped and malformed_result(row):
                with unit(ledger,'advance',row['id'],'repair') as ready:
                    if not ready:continue
                    row=queue_result_repair(ledger,row,config,mailbox_self)
            data=dict(row['data'])
            if row['state'] in {'provisioning','provision-unknown'}:
                with unit(ledger,'advance',row['id'],'provision') as ready:
                    if not ready:continue
                    if not data.get('operation_id'):
                        if not credit.allow(row):continue
                        ledger.save(force=True)  # Original create body/key precede CMA creation.
                        try:op=cma.create(data['create_body'],data['create_key'])
                        except Exception as exc:
                            status=getattr(exc,'code',None)
                            if (pool and data.get('pool_sandbox_id') and isinstance(status,int) and
                                    400<=status<500 and status not in TRANSIENT_HTTP):
                                pool.drop(data['pool_sandbox_id'])
                                tasks.update(row['id'],row['revision'],'queued',workspace_requeue(data));ledger.save();continue
                            raise
                        data['operation_id']=op['id']
                        row=tasks.update(row['id'],row['revision'],'provisioning',data);ledger.save()
                    op=cma.operation(data['operation_id'])
                    if observation_recovered(data):
                        row=tasks.update(row['id'],row['revision'],row['state'],data);ledger.save()
                    if op.get('agent_id'):
                        data['agent_id']=op['agent_id']
                        row=tasks.update(row['id'],row['revision'],'provisioning',data)
                        if sandbox_failure(ledger,cma,row,failure_pool):continue
                    if op['status']=='failed':
                        at_capacity=capacity_failure(op)
                        if at_capacity:
                            capacity_limited.add(group)
                            if pool and data.get('pool_create'):pool.record_capacity_failure(row)
                        if at_capacity and data.get('capacity_retries',0)<CAPACITY_RETRIES:
                            # No Agent was created; retry later under a new idempotency key.
                            tasks.update(row['id'],row['revision'],'queued',requeue(data));ledger.save();continue
                        if runtime_failure(op) and data.get('runtime_retries',0)<RUNTIME_RETRIES:
                            tasks.update(row['id'],row['revision'],'queued',runtime_requeue(data));ledger.save();continue
                        row=tasks.update(row['id'],row['revision'],'failed',{**data,
                            'operation_error':op.get('error'),'capacity_checked':True,'runtime_checked':True,
                            'reason':'CMA provisioning failed; retained original operation'});ledger.save()
                        data=dict(row['data'])
                    elif op['status']!='succeeded':continue
                    else:
                        require(bool(op.get('agent_id')),'Lifecycle succeeded without Agent ID')
                        data['agent_id']=op['agent_id']
                        row=tasks.update(row['id'],row['revision'],'provisioning',data);ledger.save()
                        if data.get('pool_create'):
                            data['sandbox_id']=data.get('sandbox_id') or cma.sandbox_of(data['agent_id'])
                            row=tasks.update(row['id'],row['revision'],'provisioning',data)
                            if sandbox_failure(ledger,cma,row,failure_pool):continue
                            row=tasks.update(row['id'],row['revision'],'provisioning',data);ledger.save()
                            workspaces=cma.request('/api/v1/sandboxes/'+data['sandbox_id']+'/workspaces')
                            if not workspaces['items']:continue
                            data.update(pool_sandbox_id=data['sandbox_id'],workspace_id=workspaces['items'][0]['id'])
                            pool.remember(data['sandbox_id'],data['workspace_id'],row)
                            pool.clear_capacity_failure(row)
                        elif not data['create_body'].get('workspace_id') and not data['create_body'].get('sandbox_id'):
                            data['owned_sandbox_id']=cma.sandbox_of(data['agent_id'])
                        prompt=prepare_task_prompt(row,ledger.releases,folder,config,oracle_capacity)
                        if row['data'].get('prompt_offload'):data['prompt_offload']=row['data']['prompt_offload']
                        data.update(prompt=prompt,command_key='outreach-command-'+row['request_hash'])
                        if data.get('admission_rejections'):data['command_key']+='-admission'+str(data['admission_rejections'])
                        if data.get('credit_recovered'):data['command_key']+='-credit-recovery'
                        if data.get('execution_restart'):data['command_key']+='-execution-'+data['execution_restart']
                        row=tasks.update(row['id'],row['revision'],'admitting',data);ledger.save()
            if row['state']=='admitting' and sandbox_failure(ledger,cma,row,failure_pool):continue
            if row['state']=='credit-wait':
                if (config['paused'] or not config['kinds'].get(row['kind'],False)
                        or data.get('credit_retry_exhausted') or not credit.allow(row)):continue
                credit.probe(row)
                row=tasks.update(row['id'],row['revision'],'admitting',data);ledger.save()
            if row['state'] in {'admitting','admission-unknown'}:
                with unit(ledger,'advance',row['id'],'admit') as ready:
                    if not ready:continue
                    if not credit.allow(row):continue
                    credit.probe(row)
                    if data.get('admission_rejection_ticks',0)>=3:
                        admission_rejected(ledger,cma,row,None);continue
                    command=admission_command(data)
                    ledger.save(force=True)  # Agent, pool entry and exact command precede admission.
                    try:receipt=cma.admit(data['agent_id'],command['prompt'],command['command_key'])
                    except Exception as exc:
                        status=getattr(exc,'code',None)
                        if (status==409 and command is not data and agent_retired(data)
                                and data.get('recovery',{}).get('status')=='retry-scheduled'):
                            # A scheduled retry targeted an Agent that cleanup already
                            # stopped; CMA rejects every new turn. Restart it once.
                            if command['prompt']!=data.get('prompt'):
                                data['reason']='Original Agent retired; correction context requires local takeover'
                                data['recovery']={**data['recovery'],'status':'needs-operator',
                                                  'reason':data['reason'],'suggested_exit':'local-codex'}
                                tasks.update(row['id'],row['revision'],'failed',data);ledger.save(force=True);continue
                            label=data['recovery'].get('error_class','retry')+'-'+str(data['recovery'].get('attempts',0))
                            data.pop('last_error',None)
                            tasks.update(row['id'],row['revision'],'queued',
                                         execution_restart(row,data,command['prompt'],label))
                            ledger.save(force=True);continue
                        if status==409 and pool:
                            # Replay the exact admission during a bounded wait.
                            # Afterwards use normal rejection recovery, which
                            # confirms deletion before creating another Agent.
                            legacy=(row['state']=='admission-unknown' and
                                    '409' in str(data.get('last_error','')))
                            data.setdefault('admission_conflict_since',row['updated'] if legacy else tasks.clock())
                            if tasks.clock()-data['admission_conflict_since']>=POOL_WAIT_SECONDS:
                                data['admission_rejection_ticks']=max(2,data.get('admission_rejection_ticks',0))
                                row=tasks.update(row['id'],row['revision'],row['state'],data)
                                admission_rejected(ledger,cma,row,exc);continue
                            row=tasks.update(row['id'],row['revision'],row['state'],data)
                        if isinstance(status,int) and 400<=status<500 and status not in TRANSIENT_HTTP|{409}:
                            admission_rejected(ledger,cma,row,exc);continue
                        raise
                    if command is not data:command.update(receipt)
                    observation_recovered(data)
                    if data.get('recovery',{}).get('status')=='retry-scheduled':
                        data['recovery']={**data['recovery'],'status':'retrying'}
                    data.update(receipt);data['response_started_at']=tasks.clock();row=tasks.update(row['id'],row['revision'],'running',data);ledger.save()
            if row['state']=='running':
                if not result_ready(row):continue
                with unit(ledger,'advance',row['id'],'result') as ready, \
                        recheck.attempt(ledger,'result',row['id'],data.get('response_id')) as skip_backoff:
                    if not ready:continue
                    result_started=advance_clock()
                    saved=data.get('completed_response',{})
                    response=({'status':'completed'} if saved.get('response_id')==data['response_id']
                              else cma.response(data['response_id']))
                    if observation_recovered(data):
                        row=tasks.update(row['id'],row['revision'],'running',data);ledger.save()
                    if response['status'] in {'queued','in_progress'}:
                        credit.accepted(row,response)
                        runtime_guard(ledger,cma,row,response,config)
                        continue
                    data['response_status']=response['status']
                    if credit_failure(response):
                        credit.observe(row['id'])
                        queue_credit_retry(ledger,row,response);continue
                    credit.settled(row['id'])
                    data['response_error']=response.get('error')
                    if response['status']!='completed':
                        row=recover_execution_failure(ledger,row,response,config)
                        data=dict(row['data'])
                        if row['state']!='failed':continue
                    else:
                        if data.get('recovery'):data['recovery']={**data['recovery'],'status':'recovered'}
                        text=saved['text'] if saved.get('response_id')==data['response_id'] else output_text(response)
                        if saved.get('response_id')!=data['response_id']:
                            data['completed_response']={'response_id':data['response_id'],'text':text}
                            row=tasks.update(row['id'],row['revision'],'running',data);ledger.save()
                        if defer_collections and row['id'] not in collecting:
                            collections.append(row)
                            continue
                        preserving_hold_artifacts=False
                        try:
                            result=json.loads(text);validate_result(result,row)
                            source_gate(row['request'],tasks,ROOT)
                            note_closure.source_gate(row['request'],tasks,result)
                            parent_gate(row['request'],tasks)
                            paper_gate(row['request'],ROOT)
                            owner_gate(row['request'],ROOT)
                            from .thread_staging import gate as thread_gate
                            thread_gate(row['request'],ROOT,data.get('thread_inputs'))
                            identities={artifact_key(ledger.releases,a) for a in result['artifacts']}
                            progress={key:status for key,status in data.get('artifact_progress',{}).items()
                                      if key in identities}
                            data['artifact_progress']=progress
                            previous_seconds=data.get('artifact_collection',{}).get('seconds_spent',0)
                            progress_dirty=False

                            def checkpoint_artifacts(stopped=False):
                                nonlocal row, progress_dirty, last_artifact_save
                                now=advance_clock()
                                collection={'total':len(result['artifacts']),
                                            'seconds_spent':round(previous_seconds+now-result_started,3)}
                                if (progress!=row['data'].get('artifact_progress',{}) or
                                        stopped and collection!=row['data'].get('artifact_collection')):
                                    data['artifact_collection']=collection
                                    row=tasks.update(row['id'],row['revision'],row['state'],data)
                                    progress_dirty=True
                                if stopped or (tick and progress_dirty and
                                               now-last_artifact_save>=ARTIFACT_SAVE_INTERVAL):
                                    ledger.save(force=True)
                                    last_artifact_save=advance_clock()
                                    progress_dirty=False
                                elif not tick:
                                    ledger.save()

                            preserving_hold_artifacts=result['outcome']=='hold' and bool(result['artifacts'])
                            complete=verify_artifacts(result,ledger.releases,folder,cma,data['agent_id'],
                                progress=progress,checkpoint=checkpoint_artifacts,tick=tick,
                                unit_deadline=result_started+unit_seconds if tick else None)
                            if not complete:
                                if tick.clock()>=tick.limit():
                                    tick.allow('advance',row['id'])
                                else:
                                    pending=tick.pending.setdefault('advance',[])
                                    if row['id'] not in pending:pending.append(row['id'])
                                checkpoint_artifacts(stopped=True)
                                skip_backoff()
                                continue
                            validate_result(result,row,allow_packet_artifact=False)
                        except (ValueError,KeyError,TypeError) as exc:
                            data.pop('last_error',None)
                            data.update(response_text=text,repair_error=str(exc),repair_attempts=data.get('repair_attempts',0),
                                        reason=RESULT_REPAIR_REASON+str(exc)[:300])
                            if data['repair_attempts']>=RESULT_REPAIR_LIMIT:
                                data['reason']+=' (automatic repair limit reached: 2 attempts)'
                            row=tasks.update(row['id'],row['revision'],'held',data);ledger.save()
                            result=None
                            if preserving_hold_artifacts:
                                # Even on the last repair, report the failed readback before a later cleanup pass.
                                continue
                        if result is not None:
                            data.pop('reason',None);data.pop('repair_error',None);data.pop('last_error',None)
                            data.pop('artifact_progress',None)
                            data.pop('artifact_collection',None)
                            data.pop('completed_response',None)
                            data['result']=result
                            row=tasks.update(row['id'],row['revision'],'held' if result['outcome']=='hold' else 'awaiting-review',data);ledger.save()
            cleanup=data.get('cleanup',{})
            # Retain malformed deliveries through two correction turns, then
            # free their Agent/Sandbox while keeping the final response visible.
            exhausted=malformed_result(row) and data.get('repair_attempts',0)>=RESULT_REPAIR_LIMIT
            repair_pending=(row['state']!='failed' and bool(data.get('agent_id') and data.get('response_id'))
                            and 'result' not in data and not exhausted)
            owned_sandbox=pool.owned_sandbox(data) if pool else data.get('owned_sandbox_id')
            if data.get('pool_sandbox_id'):owned_sandbox=None
            if row['state'] not in ACTIVE and not repair_pending and data.get('agent_id') and (not cleanup.get('deleted') or
                    (owned_sandbox and not cleanup.get('sandbox_retired'))) and \
                    recheck.due(tasks,'retire',row['id'],data['agent_id']):
                # Deleting an Agent can destroy the only remaining artifact copy.
                # Preserve verified result/readback (or exhausted repair evidence) first.
                with unit(ledger,'advance',row['id'],'retire') as ready, \
                        recheck.attempt(ledger,'retire',row['id'],data['agent_id']):
                    if not ready:continue
                    if pool:pool.observe_live(tasks.all())
                    ledger.save(force=True)
                    retire_key='outreach-retire-'+row['request_hash']
                    if data.get('execution_restart'):retire_key+='-execution-'+data['execution_restart']
                    data['cleanup']=cma.retire(data['agent_id'],retire_key,owned_sandbox)
                    row=tasks.update(row['id'],row['revision'],row['state'],data);ledger.save()
        except Exception as exc:
            # Keep the original operation/response/key on every uncertain call.
            current=tasks.get(row['id'])
            if recover_transport_exception(ledger,current,exc,config):continue
            data={**current['data'],'last_error':type(exc).__name__+': '+str(exc)[:300]}
            state={'provisioning':'provision-unknown','admitting':'admission-unknown'}.get(current['state'],current['state'])
            tasks.update(row['id'],current['revision'],state,data);ledger.save()
        updates.append(row['id'])
    if skips:print('admission-skips '+json.dumps(dict(sorted(skips.items()))),file=sys.stderr,flush=True)
    with phase(ledger,'pool-idle'):
        if pool:pool.idle(cma)
    return updates


def stage(value,prefix,releases,folder):
    releases.ensure()
    path=Path(folder)/(prefix+'-'+digest(value)+'.json');path.write_text(canonical(value))
    releases.upload(path)
    path.unlink()
    return {'release':getattr(releases,'tag',TAG),'asset':path.name,'sha256':digest(value)}


def review_key():
    secret=os.environ.get('OUTREACH_REVIEW_KEY')
    key_path=Path(os.environ.get('OUTREACH_REVIEW_KEY_FILE',str(Path.home()/'.config/trureturing-outreach/review.key')))
    if not secret and key_path.is_file():secret=key_path.read_text().strip()
    require(bool(secret),'Configure the local reviewer-only key file or OUTREACH_REVIEW_KEY')
    return secret


def review_digest(work, root=ROOT):
    """Summarize the review inventory without opening or mutating the ledger."""
    from .registry import people
    snapshot = work if isinstance(work, dict) else {}
    try:
        persons = people(root)
    except (OSError, ValueError, KeyError, TypeError):
        try:
            rows = json.loads((Path(root) / 'tables' / 'contacts.json').read_text()).get('contacts', [])
        except (OSError, ValueError, TypeError):
            rows = []
        persons = [{**row, 'emails': sorted(set([row.get('email')] +
                    list((row.get('relationship') or {}).get('aliases') or [])))}
                    for row in rows if isinstance(row, dict) and row.get('email')]
    by_id = {p['id']: p['name'] for p in persons}
    by_email = {address: p['name'] for p in persons for address in p['emails']}

    def names(request, result=None):
        inputs = request.get('inputs', {}) if isinstance(request, dict) else {}
        values = [request.get('owner'), *inputs.get('owner_ids', [])]
        labels = []
        for value in values:
            if not value:
                continue
            if value in by_id:
                labels.append(by_id[value])
            elif isinstance(value, str) and '@' in value and value.casefold() in by_email:
                labels.append(by_email[value.casefold()])
            else:
                labels.append(str(value))
        result_id = inputs.get('result', {}).get('id') if isinstance(inputs.get('result'), dict) else inputs.get('result_id')
        if not result_id and isinstance(result, dict):
            result_id = result.get('id') or result.get('result_id')
        if result_id:
            labels.append(str(result_id))
        return ', '.join(dict.fromkeys(labels)) or '—'

    covered_by = {row.get('task_id'): row.get('covering_note_task_id')
                  for row in snapshot.get('note_coverage_overlap', []) or [] if isinstance(row, dict)}
    items = []
    for item in snapshot.get('reviews', []) or []:
        request = item.get('request') or {}
        result = item.get('result') or {}
        inputs = request.get('inputs') or {}
        kind = request.get('kind', item.get('kind', 'unknown'))
        if (kind == 'submission' and result.get('outcome') == 'completed'
                and not item.get('submission_review_requested')):
            continue
        binding = item.get('binding') or {}
        flags = []
        report = result.get('report', '')
        brief = result.get('owner_brief') or {}
        headline = result.get('headline') or brief.get('summary_zh') or report.split('. ', 1)[0]
        headline = ' '.join(headline.split())[:200]
        audits = result.get('self_audit') or []
        if not isinstance(audits, list):audits = []
        reason = (inputs.get('review_feedback') or {}).get('reason')
        feedback = 'feedback —'
        if isinstance(reason, str):
            points = set(re.findall(r'(?:^|\s)(\d+)\.\s', reason))
            passed = {audit['check'].split()[1] for audit in audits if isinstance(audit, dict)
                      and re.fullmatch(r'feedback \d+', audit.get('check', '')) and audit.get('status') == 'pass'}
            resolved, total = len(points & passed), len(points)
            feedback = f'feedback {resolved}/{total}'
            if resolved < total:flags.append(f'feedback unresolved: {total-resolved}/{total}')
        hold_reason = item.get('reason') or result.get('report', '')
        if result.get('outcome') == 'hold' or item.get('state') == 'held':
            flags.append('agent hold: ' + ' '.join(str(hold_reason).split())[:80])
        if any(isinstance(audit, dict) and audit.get('status') == 'fail' for audit in audits):
            flags.append('self_audit fail')
        task_key = binding.get('task_id') or request.get('id') or item.get('task_id', '')
        if covered_by.get(task_key):
            flags.append('covered by PASSed note ' + str(covered_by[task_key]))
        routing = result.get('routing') or {}
        if routing.get('mail') == 'owner':
            flags.append('routing owner')
        try:
            current_hash = workflow(kind)
        except (KeyError, OSError, ValueError):
            current_hash = None
        request_hash = request.get('workflow_hash')
        workflow_changed = bool(current_hash and request_hash and not matches_workflow(
            {'request':request,'data':{'workflow_binding':item.get('workflow_binding',{})}},current_hash))
        if workflow_changed:
            flags.append('workflow changed: rebind or rerun')
        round_parts = []
        if request.get('inputs', {}).get('note_round') is not None:
            round_parts.append('note_round=' + str(request['inputs']['note_round']))
        if request.get('inputs', {}).get('submission_round') is not None:
            round_parts.append('submission_round=' + str(request['inputs']['submission_round']))
        packet = result.get('packet') if isinstance(result.get('packet'), dict) else {}
        attachment_value = packet.get('attachments') if isinstance(packet.get('attachments'), list) else result.get('artifacts', [])
        item_result = {
            'task_id': binding.get('task_id') or request.get('id') or item.get('task_id', ''),
            'kind': kind,
            'counterpart': names(request, result),
            'round': ', '.join(round_parts) or '—',
            'outcome': result.get('outcome', '—'),
            'headline': headline,
            'feedback': feedback,
            'risk_flags': flags,
            'attachments': len(attachment_value or []),
            'workflow_changed': workflow_changed,
            'revision': binding.get('task_revision', item.get('revision', '—')),
        }
        item_result['_risk'] = len(flags)
        items.append(item_result)
    items.sort(key=lambda value: (-value.pop('_risk'), value['task_id']))
    lines = []
    for item in items:
        flags = '; '.join(item['risk_flags']) or 'none'
        lines.append('{} | {} | {} | {} | {} | {} | risks: {} | attachments: {} | revision: {}'.format(
            item['task_id'], item['kind'], item['counterpart'], item['round'], item['outcome'],
            item['feedback'], flags, item['attachments'], item['revision']))
        lines.append('  ↳ ' + item['headline'])
    return {'items': items, 'lines': lines}


@contextmanager
def finished(tick,name):
    previous=tick.current
    tick.current=name
    try:yield
    finally:
        tick.current=previous
        tick.finished.add(name)


def run_tick(ledger,config,folder,tick,oracle_fetcher=None):
    from .draft_status import refresh as refresh_draft_status
    from .mailbox import configured, intake
    from .pipeline import reconcile
    from .automation import migrate_home_recipients, reconcile as continue_work
    from .delivery import advance as deliver
    from .sent_events import advance as record_sent
    from .mathesis_delivery import advance as deliver_mathesis
    from .publication import advance as publish_notes
    from .formalization_requests import advance as file_formalization
    from .prior_work_issues import advance as file_prior_work
    connector_errors=ledger.connector_errors;mailbox=None;oracle_capacity=None;sent_messages=[]
    def record_errors():
        db=ledger.tasks.db;value=canonical(connector_errors)
        old=db.execute("SELECT payload FROM pipeline_state WHERE key='connector_errors'").fetchone()
        if not old or old[0]!=value:
            db.execute('INSERT OR REPLACE INTO pipeline_state VALUES(?,?)',('connector_errors',value))
    # Ingest remains first so fresh reviews/decisions precede every dependent
    # effect. Rotate the remaining passes, without changing their own gates.
    stages=tick.order(['advance','automation','delivery','sent-events','mathesis',
                       'formalization-requests','prior-work-issues','maintenance'])
    # Pool health, result collection, repair turns and admissions live in
    # advance. Mailbox intake alone took ~300 s on 2026-11-08 and advance then
    # never reached repairs or queued work, so its slice is reserved on every
    # tick, whatever the rotation. Deferred intake units resume next tick.
    budget=tick.deadline-tick.started
    for stage,seconds in stage_reserves(budget).items():
        # Sent Items recording is cheap but ran 0 pages per tick once setup and
        # the other reserves exceeded the budget; give it a floor (2026-11-15).
        tick.reserve(stage,seconds,floor=stage=='sent-events',dedicated=stage in {'mailbox-intake','pipeline'},
                     hold=stage!='pipeline')
    if not config['paused'] and ledger.tasks.db.execute("""
            SELECT 1 FROM tasks WHERE state='owner-draft'
            AND json_extract(data, '$.draft_receipt.remote_id') IS NOT NULL LIMIT 1
            """).fetchone():
        tick.reserve('owner-drafts', min(10, 0.02*budget), floor=True)
    if not config['paused'] and config.get('automation',{}).get('enabled'):
        # In-flight CMA work can finish well past the shared deadline.
        tick.reserve('automation',min(15,0.05*budget),dedicated=True)
    with phase(ledger,'ingest'):
        ingest_errors=ledger.ingest(os.environ.get('OUTREACH_REVIEW_KEY'))
        migrate_workflows(ledger.tasks)
        migrate_home_recipients(ledger.tasks,config)
        ledger.save(force=True)  # Compatibility must survive before dependent effects.
    with phase(ledger,'oracle-capacity'):
        with unit(ledger,'setup','oracle-capacity') as ready:
            if ready:oracle_capacity=refresh_oracle_capacity(ledger,oracle_fetcher)
    revision=source_head(ENGINE_ROOT)
    # Clear mailbox deferrals recorded before intake had its own lane.
    setup_pending=tick.pending.get('setup',[])
    if 'mailbox' in setup_pending:setup_pending.remove('mailbox')
    with phase(ledger,'mailbox-intake'),finished(tick,'mailbox-intake'):
        # Build the adapter outside the setup budget: delivery, owner drafts and
        # Sent recording need it even when a long review ingest leaves no setup
        # time for intake (2026-11-20: three ticks sent and saved nothing).
        try:
            mailbox=configured(config,ROOT)
        except Exception as exc:
            connector_errors.append({'connector':'mailbox','error':type(exc).__name__+': '+str(exc)[:300]})
        if mailbox is not None:
            with unit(ledger,'mailbox-intake','mailbox') as ready:
                if ready:
                    try:
                        intake(ledger,config,ROOT,mailbox,revision=revision)
                    except Exception as exc:
                        connector_errors.append({'connector':'mailbox','error':type(exc).__name__+': '+str(exc)[:300]})
    # Clear a pipeline deferral recorded before reconciliation had its own slice.
    setup_pending=tick.pending.get('setup',[])
    if 'pipeline' in setup_pending:setup_pending.remove('pipeline')
    with phase(ledger,'pipeline'),finished(tick,'pipeline'):
        with unit(ledger,'pipeline','reconcile') as ready:
            if ready:reconcile(ledger,config,ROOT,revision=revision)
    record_errors()
    # Request stop/delete without waiting (~25 s per retirement at 5 s);
    # later ticks observe the transition and confirm deletion.
    cma=CMA();cma.retire_wait=0
    for name in stages:
        with phase(ledger,name),finished(tick,name):
            if name=='advance':
                try:advance(ledger,cma,config,folder,revision=revision,mailbox=mailbox,
                            oracle_capacity=oracle_capacity)
                finally:tick.report('firstcontact-history')
            elif name=='automation':continue_work(ledger,config,ROOT,revision=revision)
            elif name=='delivery':
                deliver(ledger,config,ROOT,mailbox)
            elif name=='sent-events':
                sent_status=record_sent(ledger,config,ROOT,mailbox,observe=sent_messages.extend)
                if sent_status.get('status')=='error':
                    connector_errors.append({'connector':'sent-events','error':sent_status['error']})
            elif name=='mathesis':
                publish_notes(ledger,config,ROOT,folder)
                deliver_mathesis(ledger,config,ROOT,folder)
            elif name=='formalization-requests':
                file_formalization(ledger,config)
            elif name=='prior-work-issues':
                file_prior_work(ledger,config,ROOT)
            else:
                with unit(ledger,'maintenance','snapshot-prune') as ready:
                    if ready:
                        ledger.save(force=True)  # Preserve the current snapshot before pruning.
                        # Only optional maintenance observes the deadline. Pruning
                        # required by save() at the asset limit belongs to that
                        # checkpoint and must finish even after exhaustion.
                        ledger.releases.tick=tick
                        try:ledger.releases.prune_snapshots(SNAPSHOTS_KEPT,{ledger.current})
                        except Exception as exc:
                            connector_errors.append({'connector':'snapshot-prune','error':str(exc)[:300]})
                        finally:ledger.releases.tick=None
                with unit(ledger,'maintenance','input-prune') as ready:
                    # Ingested control inputs otherwise accumulate until the
                    # 1000-asset release limit blocks new reviews and requests.
                    if ready and hasattr(ledger.releases,'prune_ingested'):
                        ledger.releases.tick=tick
                        try:ledger.releases.prune_ingested(ledger.ingestion_receipts())
                        except Exception as exc:
                            connector_errors.append({'connector':'input-prune','error':str(exc)[:300]})
                        finally:ledger.releases.tick=None
    with phase(ledger,'owner-drafts'),finished(tick,'owner-drafts'):
        draft_error=refresh_draft_status(ledger,config,mailbox,sent_messages)
        if draft_error:
            connector_errors.append({'connector':'owner-drafts','error':draft_error})
    record_errors()
    return ingest_errors,connector_errors


def status_summary(tasks,config,ingest_errors=None,connector_errors=None):
    """Bounded operator view shared by tick and status --compact."""
    from .pipeline import status as pipeline_status
    rows=tasks.lane_rows()
    counts=tasks.counts()
    stored=tasks.db.execute("SELECT payload FROM pipeline_state WHERE key='connector_errors'").fetchone()
    rejected=ingest_rejections(tasks)
    return {'schema_version':1,'paused':config['paused'],
            'counts':counts,'counts_by_kind':tasks.counts('kind'),
            'lanes':lane_summary(rows,config,linked_contacts(rows,ROOT,config)),
            'pipeline':pipeline_status(tasks,ROOT,config,compact=True),
            'errors':{'failed':counts.get('failed',0),'held':counts.get('held',0),
                      'ingest_rejected':len(given_up_digests(rejected))},
            'admission_errors':ingest_errors if ingest_errors is not None else [
                {'digest':sha,**entry} for sha,entry in rejected.items()],
            'connector_errors':connector_errors if connector_errors is not None else
                (json.loads(stored[0]) if stored else [])}


def _main(argv=None):
    from .automation import submission_rounds
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path)
    commands=parser.add_subparsers(dest='action',required=True)
    commands.add_parser('doctor')
    p=commands.add_parser('status');p.add_argument('--compact',action='store_true',
                                                help='Aggregate status without loading task payloads')
    commands.add_parser('reviews');commands.add_parser('work');commands.add_parser('tick')
    p=commands.add_parser('review-digest',
                          description='Review inventory. A PASS on an awaiting reply or first contact may include a signed edited_body.')
    p.add_argument('--work',type=Path,required=True);p.add_argument('--format',choices=['text','json'],default='text')
    p=commands.add_parser('file-formalization-requests');p.add_argument('--work',type=Path,required=True)
    p.add_argument('--dry-run',action='store_true')
    p=commands.add_parser('file-prior-work-issues');p.add_argument('--work',type=Path,required=True)
    p.add_argument('--dry-run',action='store_true')
    p.add_argument('--evidence-dir',type=Path)
    p=commands.add_parser('exceptions');p.add_argument('--work',type=Path);p.add_argument('--format',choices=['json','text'],default='json')
    p=commands.add_parser('todo');p.add_argument('--work',type=Path,required=True);p.add_argument('--format',choices=['md','json'],default='md')
    commands.add_parser('mathesis-test')
    p=commands.add_parser('requeue-large');p.add_argument('--task',required=True)
    p=commands.add_parser('requeue-failed');p.add_argument('--task',required=True);p.add_argument('--reason',required=True)
    p=commands.add_parser('pool-adopt');p.add_argument('--sandbox',required=True);p.add_argument('--workspace',required=True)
    p.add_argument('--group',required=True);p.add_argument('--reason',required=True)
    p=commands.add_parser('enqueue');p.add_argument('request',type=Path)
    p=commands.add_parser('review');p.add_argument('envelope',type=Path)
    p=commands.add_parser('decision');p.add_argument('envelope',type=Path)
    p=commands.add_parser('sign-review');p.add_argument('envelope',type=Path);p.add_argument('--kind',choices=sorted(KINDS))
    p=commands.add_parser('rebind-stale');p.add_argument('--work',type=Path,required=True)
    p.add_argument('--reason',required=True);p.add_argument('--dry-run',action='store_true')
    p=commands.add_parser('stage-draft-revision');p.add_argument('--task',required=True)
    p.add_argument('--packet',type=Path,required=True);p.add_argument('--reason',required=True)
    p=commands.add_parser('plan');p.add_argument('kind',choices=sorted(KINDS));p.add_argument('--id',required=True);p.add_argument('--owner',required=True);p.add_argument('--inputs',type=Path,required=True)
    p=commands.add_parser('stage-submission');p.add_argument('--paper',required=True);p.add_argument('--file',action='append',required=True)
    p.add_argument('--round',choices=submission_rounds(),default='improve')
    p=commands.add_parser('request-submission-review');p.add_argument('--paper',required=True)
    p=commands.add_parser('stage-thread');p.add_argument('--thread',required=True);p.add_argument('--kind',choices=['reply','research'],default='reply');p.add_argument('--instructions',default='')
    p=commands.add_parser('stage-inbound');p.add_argument('--message-id',required=True);p.add_argument('--owner');p.add_argument('--instructions',default='')
    p=commands.add_parser('stage-mathesis');p.add_argument('--task',required=True);p.add_argument('--directory',type=Path,required=True);p.add_argument('--decisions',type=Path,required=True)
    args=parser.parse_args(argv);tick=Tick() if args.action=='tick' else None
    config=load_config(ROOT)
    validate_lanes(config)
    releases=Releases();result=None
    with tempfile.TemporaryDirectory() as folder:
        if args.action=='requeue-large':
            ledger=Ledger(releases,folder)
            row=ledger.tasks.get(args.task)
            # The failed revision makes a later requeue of the same task a new event.
            result=stage({'task_id':row['id'],'request_hash':row['request_hash'],
                          'failed_revision':row['revision']},'prompt-requeue',releases,folder)
        elif args.action=='requeue-failed':
            require(args.reason.strip(),'Recovery request needs a reason')
            ledger=Ledger(releases,folder)
            try:
                row=ledger.tasks.get(args.task)
                require(row['state']=='failed','Only failed tasks may be requeued')
                envelope={'schema_version':1,'task_id':row['id'],'request_hash':row['request_hash'],
                          'failed_revision':row['revision'],'reason':args.reason}
                secret=review_key()
                envelope['signature']=hmac.new(secret.encode(),canonical(envelope).encode(),hashlib.sha256).hexdigest()
                result=stage(envelope,'failed-requeue',releases,folder)
            finally:ledger.tasks.close()
        elif args.action=='pool-adopt':
            require(args.reason.strip(),'Pool adoption needs a reason')
            envelope={'schema_version':1,'sandbox_id':args.sandbox,'workspace_id':args.workspace,
                      'group':args.group,'reason':args.reason}
            secret=review_key()
            envelope['signature']=hmac.new(secret.encode(),canonical(envelope).encode(),hashlib.sha256).hexdigest()
            result=stage(envelope,'pool-adopt',releases,folder)
        elif args.action=='todo':
            from .owner_todo import owner_todo, render_markdown
            value=owner_todo(json.loads(args.work.read_text()),ROOT)
            result=value if args.format=='json' else render_markdown(value)
        elif args.action=='exceptions':
            from .exceptions import inventory, render
            if args.work:
                snapshot=json.loads(args.work.read_text())
            else:
                ledger=Ledger(releases,folder)
                try:snapshot={'tasks':ledger.tasks.all()}
                finally:ledger.tasks.close()
            result=render(inventory(snapshot,ROOT),args.format)
        elif args.action=='review-digest':
            value=review_digest(json.loads(args.work.read_text()), ROOT)
            result=value if args.format=='json' else '\n'.join(value['lines']) + ('\n' if value['lines'] else '')
        elif args.action=='file-formalization-requests':
            from .formalization_requests import backfill_plan, backfill_envelope
            plan=backfill_plan(json.loads(args.work.read_text()))
            result={'notes':plan,'dry_run':args.dry_run}
            if plan and not args.dry_run:
                result['staged']=stage(backfill_envelope(plan,review_key()),BACKFILL_PREFIX,releases,folder)
        elif args.action=='file-prior-work-issues':
            from .prior_work_issues import BACKFILL_REQUIRED, envelope, plan
            result=plan(json.loads(args.work.read_text()),ROOT,args.evidence_dir)
            result['dry_run']=args.dry_run
            selection=[note for note in result['needs_evidence'] if note['reason']==BACKFILL_REQUIRED]
            selection.extend(note for note in result['notes'] if note.get('requires_backfill'))
            if selection and not args.dry_run:
                result['staged']=stage(envelope(selection,review_key()),PRIOR_WORK_BACKFILL,releases,folder)
        elif args.action=='mathesis-test':
            from .mathesis_acceptance import run
            result=run(config,folder)
        elif args.action=='stage-mathesis':
            from .mathesis_delivery import select
            from .correspondence import delivery_problem_ids
            ledger=Ledger(releases,folder)
            try:
                row=ledger.tasks.get(args.task)
                note_revisions.gate(row, ledger.tasks)
                note_closure.source_gate(row['request'],ledger.tasks,row['data'].get('result'))
                value=select(row,args.directory,json.loads(args.decisions.read_text()),review_key())
                note_revisions.gate(row, ledger.tasks, delivery_problem_ids(value['metadata']))
                result=stage(value,'mathesis',releases,folder)
            finally:ledger.tasks.close()
        elif args.action=='stage-submission':
            from .automation import stage_submission
            result=stage_submission(args.paper,args.file,ROOT,releases,folder,submission_round=args.round)
        elif args.action=='request-submission-review':
            ledger=Ledger(releases,folder)
            try:
                row=ledger.tasks.submission_review_task(args.paper)
                envelope={'schema_version':1,'paper_id':args.paper,'task_id':row['id'],
                          'result_hash':digest(row['data']['result'])}
                secret=review_key()
                envelope['signature']=hmac.new(secret.encode(),canonical(envelope).encode(),hashlib.sha256).hexdigest()
                result=stage(envelope,'submission-review',releases,folder)
            finally:ledger.tasks.close()
        elif args.action=='stage-draft-revision':
            from .queue import validate_packet
            ledger=Ledger(releases,folder)
            try:
                row=ledger.tasks.get(args.task)
                require(row['state']=='owner-draft','Draft revision requires an owner-draft task')
                data=row['data'];old_packet=data.get('packet_override') or data.get('result',{}).get('packet')
                receipt=data.get('draft_receipt') or {}
                require(isinstance(old_packet,dict) and isinstance(receipt,dict),
                        'Owner draft has no reviewed packet or receipt')
                old_hash=receipt.get('content_hash')
                require(isinstance(old_hash,str) and old_hash==digest(old_packet),
                        'Owner draft receipt does not match its reviewed packet')
                require(isinstance(receipt.get('remote_id'), str) and receipt['remote_id'].strip(),
                        'Owner draft receipt has no remote ID')
                require(args.reason.strip(), 'Draft revision needs a reason')
                new_packet=json.loads(args.packet.read_text())
                validate_packet(new_packet)
                immutable=set(old_packet)-{'body','attachments'}
                require(all(new_packet.get(key)==old_packet.get(key) for key in immutable),
                        'Draft revision may change only body and attachments')
                require(set(new_packet)==set(old_packet),
                        'Draft revision may change only body and attachments')
                envelope={'schema_version':1,'task_id':args.task,
                          'old_packet_hash':old_hash,'new_packet_hash':digest(new_packet),
                          'reason':args.reason,'packet':new_packet}
                secret=review_key()
                envelope['signature']=hmac.new(secret.encode(),canonical(envelope).encode(),hashlib.sha256).hexdigest()
                result=stage(envelope,'draft-revision',releases,folder)
            finally:ledger.tasks.close()
        elif args.action=='stage-thread':
            from .thread_staging import stage_thread
            result=stage_thread(args.thread,args.kind,args.instructions,ROOT,releases,folder)
        elif args.action=='stage-inbound':
            from .thread_staging import stage_inbound
            result=stage_inbound(args.message_id,args.owner,args.instructions,ROOT,releases,folder)
        elif args.action=='plan':
            commit=source_head(ENGINE_ROOT)
            result={'schema_version':1,'id':args.id,'kind':args.kind,'owner':args.owner,
                    'inputs':json.loads(args.inputs.read_text()),
                    **request_binding(args.kind, commit, current_policy_revision())}
            validate_request(result)
            firstcontact_gate(result,ROOT)
        elif args.action=='sign-review':
            secret=review_key()
            result=sign_review(json.loads(args.envelope.read_text()),secret,args.kind)
        elif args.action=='rebind-stale':
            result=rebind_stale(json.loads(args.work.read_text()),args.reason,releases,folder,args.dry_run)
        elif args.action in {'enqueue','review','decision'}:
            value=json.loads((args.request if args.action=='enqueue' else args.envelope).read_text())
            if args.action=='enqueue':
                validate_request(value);verify_request_workflow(value);firstcontact_gate(value,ROOT)
            if args.action=='review':
                result=submit_review(value,releases,folder)
            else:
                result=stage(value,'request' if args.action=='enqueue' else args.action,releases,folder)
        elif args.action=='doctor':
            try:review_key_configured=bool(review_key())
            except (OSError, ValueError):review_key_configured=False
            cma={'status':'unconfigured','reason':'Set a supported CMA_ACCESS_TOKEN in Actions; local discovery can use NyxID CLI'}
            if os.environ.get('CMA_ACCESS_TOKEN') or shutil.which('nyxid'):
                try:cma={'status':'reachable','capabilities':CMA().discover()}
                except Exception as exc:cma={'status':'unavailable','reason':type(exc).__name__+': '+str(exc)[:300]}
            result={'schema_version':1,'paused':config['paused'],'cma':cma,
                'mailbox':config.get('mailbox_adapter','unconfigured'),'reviewer':'local Claude loop, not started by this service',
                'review_key_configured':review_key_configured,
                'publication':'scripts/short_notes/service.py','control_release_exists':releases.release() is not None}
        else:
            if args.action=='tick':
                require(os.environ.get('GITHUB_ACTIONS')=='true','The live writer is the serialized GitHub Actions coordinator')
            with tick.phase('restore') if tick else phase(None,'restore'):
                if tick:releases.ensure()
                ledger=Ledger(releases,folder)
            try:
                if tick:
                    tick.bind(ledger)
                ingest_errors=[];connector_errors=[]
                if args.action=='tick':
                    ingest_errors,connector_errors=run_tick(ledger,config,folder,tick)
                if args.action=='reviews':
                    result=ledger.tasks.reviews()
                elif args.action=='work':
                    from .automation import work
                    result={'paused':config['paused'],'mailbox_adapter':config.get('mailbox_adapter'),
                            'automatic_mail_enabled':config.get('automation',{}).get('automatic_mail',False),
                            **work(ledger.tasks,ROOT,config)}
                elif args.action=='tick' or getattr(args,'compact',False):
                    result=status_summary(ledger.tasks,config,
                                          ingest_errors if tick else None,connector_errors if tick else None)
                else:
                    from .prior_work_issues import inventory_view as prior_work_view
                    rows=ledger.tasks.all()
                    result={'schema_version':1,'paused':config['paused'],'tasks':rows,
                            'admission_errors':ingest_errors,
                            'connector_errors':connector_errors,
                            **prior_work_view(ledger.tasks),
                            'counts':ledger.tasks.counts()}
                    from .pipeline import status as pipeline_status
                    result['pipeline']=pipeline_status(ledger.tasks,ROOT,config)
                    migration=ledger.tasks.db.execute(
                        "SELECT payload FROM pipeline_state WHERE key='mathesis-legacy-migration'").fetchone()
                    result['mathesis_legacy_migration']=(json.loads(migration[0]) if migration else {
                        'pending':0, 'withdrawn':0, 'redelivered':0, 'skipped':0,
                        'skipped_reasons':{}})
                    result['lanes']=lane_summary(result['tasks'],config,linked_contacts(result['tasks'],ROOT,config))
            finally:
                try:
                    # Also flush progress on an exceptional tick exit. Hard kills
                    # recover from the last forced intent and replay observations.
                    if args.action=='tick':
                        with tick.phase('final-save'):
                            tick.finish()
                            ledger.save(force=True)
                finally:ledger.tasks.close()
    output=(result if isinstance(result, str) else json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    if args.output:args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(output)
    else:print(output,end='')
    if args.action=='doctor' and result['cma']['status']!='reachable':
        raise SystemExit(1)


def main(argv=None):
    from .run_log import public_run

    tick = 'tick' in (argv if argv is not None else sys.argv[1:])
    return public_run(lambda: _main(argv), 'tick' if tick else 'control', tick=tick)


if __name__ == '__main__':
    raise SystemExit(main())
