"""CMA's deployed REST/lifecycle and Responses v2 contracts, via NyxID.

Tokens stay in the environment or NyxID credential store. A disconnected SSE
stream never implies that a server-owned turn stopped.
"""
from . import recording

import json
import base64
import hashlib
import os
from pathlib import Path, PurePosixPath
import selectors
import subprocess
import tempfile
import time
from urllib.request import Request, build_opener, HTTPRedirectHandler
from urllib.error import HTTPError
from urllib.parse import urlencode

from .location import settings
from .tick import artifact_io_timeout


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise RuntimeError('CMA credentials cannot follow redirects')


class CMARequestError(RuntimeError):
    def __init__(self, status, reason):
        self.code=status
        self.body=reason
        super().__init__('CMA refused request: '+str(reason))


class CMA:
    def __init__(self, base=None, token=None, service='cma', via_service=None):
        self.token=token or os.environ.get('CMA_ACCESS_TOKEN')
        # The direct, documented origin retains admission headers and stable
        # owner custody. The measured NyxID proxy lost those headers and an
        # equivalent keyed reconnect admitted another command. Keep NyxID for
        # credential issuance and the CLI path, but use direct API token custody.
        self.base=(base or os.environ.get('CMA_BASE_URL') or
                   settings('cma')['token_base_url' if self.token else 'proxy_base_url']).rstrip('/')
        if not self.base.startswith('https://'):
            raise ValueError('CMA requires HTTPS')
        self.service=service
        self.via_service=via_service

    def request(self, path, method='GET', body=None, key=None, stream=False):
        if not path.startswith('/api/') or '..' in path:
            raise ValueError('Use the documented CMA API path')
        if method!='GET' and not key:
            raise ValueError('Every CMA write needs its original idempotency key')
        if self.token:
            headers={'Authorization':'Bearer '+self.token,
                     'User-Agent':'trureturing-outreach/managed',
                     'Accept':'text/event-stream' if stream else 'application/json'}
            if key: headers['Idempotency-Key']=key
            data=None
            if body is not None:
                data=json.dumps(body,ensure_ascii=False).encode();headers['Content-Type']='application/json'
            with recording.open('cma', build_opener(NoRedirect()),
                Request(self.base+path,data=data,headers=headers,method=method),
                timeout=artifact_io_timeout(30),
            ) as response:
                if stream:
                    rid=response.headers.get('X-CMA-Response-ID')
                    cid=response.headers.get('X-CMA-Command-ID')
                    if rid:return {'response_id':rid,'command_id':cid}
                    # The deployed proxy may omit the admission headers while
                    # returning response.created on SSE. Observe that durable
                    # event before closing; never submit under a replacement key.
                    deadline=time.monotonic()+25
                    observed=0
                    for line in response:
                        observed+=len(line)
                        if time.monotonic()>deadline or observed>1024*1024:break
                        if not line.startswith(b'data:'):continue
                        try:event=json.loads(line[5:].strip())
                        except ValueError:continue
                        record=event.get('response',{})
                        if event.get('type')=='response.created' and record.get('id'):
                            return {'response_id':record['id'],'command_id':event.get('command_id')}
                    raise RuntimeError('Admission outcome unknown; retain key and original body')
                raw=response.read()
                return json.loads(raw) if raw.strip() else {}
        args=['nyxid','proxy','request',self.service,path.lstrip('/'),'--method',method,'--output','json']
        if self.via_service:args+=['--via-service',self.via_service]
        if key:args+=['--header','Idempotency-Key:'+key]
        with tempfile.TemporaryDirectory() as temp:
            if body is not None:
                p=Path(temp)/'request.json';p.write_text(json.dumps(body,ensure_ascii=False))
                args+=['--data','@'+str(p),'--header','Content-Type:application/json']
            if not stream:
                done=recording.run(args,capture_output=True,text=True,timeout=artifact_io_timeout(35))
                try:result=json.loads(done.stdout) if done.stdout.strip() else {}
                except ValueError:
                    if not done.returncode:raise
                    result={}
                if isinstance(result,dict) and isinstance(result.get('status'),int) and result['status']>=400:
                    raise CMARequestError(result['status'],result)
                if done.returncode:raise RuntimeError('NyxID CMA request failed; inspect the saved task and original key')
                return result
            process=subprocess.Popen(args+['--stream'],stdout=subprocess.PIPE,stderr=subprocess.DEVNULL)
            # Read just the durable admission event. Closing this observer leaves
            # the server-owned turn running; the next tick retrieves its ID.
            captured=b''
            buffer=b'';selector=selectors.DefaultSelector();selector.register(process.stdout,selectors.EVENT_READ)
            deadline=time.monotonic()+25
            try:
                while time.monotonic()<deadline:
                    if not selector.select(timeout=1):
                        if process.poll() is not None:break
                        continue
                    chunk=os.read(process.stdout.fileno(),65536)
                    if not chunk:break
                    captured+=chunk
                    buffer+=chunk
                    while b'\n' in buffer:
                        line,buffer=buffer.split(b'\n',1)
                        if not line.startswith(b'data:'):continue
                        try:event=json.loads(line[5:].strip())
                        except ValueError:continue
                        response=event.get('response',{})
                        if event.get('type')=='response.created' and response.get('id'):
                            return {'response_id':response['id'],'command_id':event.get('command_id')}
                try:refusal=json.loads(captured)
                except ValueError:refusal={}
                if isinstance(refusal,dict) and isinstance(refusal.get('status'),int) and refusal['status']>=400:
                    raise CMARequestError(refusal['status'],refusal)
                raise RuntimeError('Admission outcome unknown; reconnect with the saved request and key')
            finally:
                if recording.enabled():
                    recording.emit({**recording.command_call(args + ['--stream']),
                        'status': process.poll(), 'response_body': captured})
                selector.close()
                process.terminate()
                try:process.wait(timeout=3)
                except subprocess.TimeoutExpired:process.kill();process.wait()

    def discover(self):return self.request('/api/v2')
    def create(self, body, key):return self.request('/api/v1/agents','POST',body,key)
    def operation(self, operation_id):return self.request('/api/v1/agent-lifecycle/'+operation_id)
    def admit(self, agent_id, prompt, key):
        return self.request('/api/v2/responses','POST',
                            {'conversation':agent_id,'input':prompt,'stream':True},key,True)
    def response(self, response_id):return self.request('/api/v2/responses/'+response_id)
    def controls(self, agent_id, response_id):
        return self.request('/api/v2/agents/'+agent_id+'/responses/'+response_id+'/controls')
    def control(self, agent_id, response_id, body, key):
        return self.request('/api/v2/agents/'+agent_id+'/responses/'+response_id+'/controls','POST',body,key)
    def sandbox_of(self,agent_id):
        agent=self.request('/api/v1/agents/'+agent_id)
        sandbox=(agent.get('sandbox') or {}).get('id')
        if not isinstance(sandbox,str) or not sandbox.startswith('sbx_'):
            raise ValueError('Missing original Agent Sandbox identity')
        return sandbox
    def artifact(self, agent_id, relative, size, sha256):
        """Read one selected file below this Agent's actual worktree, with receipts."""
        path=PurePosixPath(relative)
        if (not relative or path.is_absolute() or any(p in {'','.', '..'} for p in relative.split('/'))
                or not 0<size<=30*1024*1024):
            raise ValueError('Invalid Agent artifact path or size')
        agent=self.request('/api/v1/agents/'+agent_id)
        worktree=PurePosixPath(agent['worktree']['path'])
        # The deployed file API exposes paths relative to /home, not the cwd.
        if not worktree.is_absolute() or '..' in worktree.parts:
            raise ValueError('Invalid Agent worktree')
        root=worktree.relative_to('/home')
        query=urlencode({'path':str(root/path),'max_bytes':size})
        try:
            record=self.request('/api/v1/agents/'+agent_id+'/files/content?'+query)
        except HTTPError as exc:
            # Token mode raises urllib errors whose text omits the CMA problem
            # code (file_capacity, file_runtime_not_ready); keep it visible.
            raw=exc.read().decode('utf-8',errors='replace')
            try:problem=json.loads(raw)
            except ValueError:problem={'status':exc.code,'detail':raw[:500]}
            raise CMARequestError(exc.code,problem) from exc
        raw=base64.b64decode(record['content_base64'],validate=True)
        if (record['path']!=str(root/path) or len(raw)!=size or record['size_bytes']!=size
                or hashlib.sha256(raw).hexdigest()!=sha256 or record['sha256']!=sha256):
            raise ValueError('Agent artifact readback mismatch')
        return raw
    retire_wait=0  # seconds to observe stop/delete within one call; the coordinator sets it

    def _await_status(self, agent_id, done, agent=None):
        deadline=time.time()+getattr(self,'retire_wait',0)
        agent=agent or self.request('/api/v1/agents/'+agent_id)
        while agent.get('status') not in done and time.time()<deadline:
            time.sleep(2)
            agent=self.request('/api/v1/agents/'+agent_id)
        return agent

    @staticmethod
    def _gone(exc):
        """Whether CMA has already torn down the object we are reconciling."""
        code=getattr(exc,'code',None)
        if code==410:return True
        body=getattr(exc,'body',None)
        if body is None and hasattr(exc,'read'):
            try:
                raw=exc.read()
                if raw:
                    body=raw.decode('utf-8',errors='replace') if isinstance(raw,bytes) else raw
                    try:exc.body=json.loads(body)
                    except (TypeError,ValueError):exc.body=body
            except Exception:
                body=None
        # A NyxID routing 404 is commonly just {"status": 404}; treating that
        # as a deleted Agent would skip the owned-Sandbox reconciliation. CMA's
        # resource responses carry an explicit resource/not-found or gone
        # marker, which is the only safe 404 shape to accept here.
        if code==404:
            body=getattr(exc,'body',body)
            if isinstance(body,dict):
                values=[]
                def visit(value):
                    if isinstance(value,dict):
                        for key,item in value.items():
                            if key not in {'status','http_status'}:values.append(item)
                            visit(item)
                    elif isinstance(value,(list,tuple)):
                        for item in value:visit(item)
                    elif isinstance(value,str):values.append(value)
                visit(body)
                text=' '.join(str(value) for value in values).casefold()
            else:
                text=str(body or '').casefold()
            return any(mark in text for mark in
                       ('agent_not_found','agent not found','agent gone','agent_gone','agent retired',
                        'sandbox_not_found','sandbox not found','sandbox_gone',
                        'sandbox gone','sandbox_retired','sandbox retired',
                        'resource_not_found'))
        text=' '.join(str(value) for value in
                      (getattr(exc,'body',''),exc)).casefold()
        return any(mark in text for mark in
                   ('sandbox_not_found','sandbox not found','sandbox_gone',
                    'sandbox gone','sandbox_retired','sandbox retired',
                    'agent_not_found','agent not found','agent gone','agent_gone','agent retired'))

    @staticmethod
    def _error(action, exc):
        body=getattr(exc,'body',None)
        if body is None and hasattr(exc,'read'):
            try:
                raw=exc.read()
                body=raw.decode('utf-8',errors='replace') if isinstance(raw,bytes) else raw
            except Exception:
                body=None
        if body is None:body=str(exc)
        if not isinstance(body,str):
            try:body=json.dumps(body,ensure_ascii=False,sort_keys=True)
            except (TypeError,ValueError):body=str(body)
        return {'action':action,'status':getattr(exc,'code',None),
                'error':body[:500]}

    @staticmethod
    def _operator_check(agent_id, reason, sandbox_id=None):
        value={'kind':'cma-agent-cleanup','agents':[agent_id],
               'reason':reason[:300]}
        if sandbox_id:value['sandbox_id']=sandbox_id
        return value

    def _retire_agent(self, agent_id, key, agent=None, sandbox_id=None):
        """Stop and delete one Agent, retaining the original keys on replay.

        Failed Agents are terminal enough to submit the delete after a stop
        refusal. Ready and working Agents still need an observed stopped state.
        A missing Sandbox is a durable operator check: repeating the same stop
        forever cannot repair a CMA object that no longer exists.
        """
        try:
            agent=agent or self.request('/api/v1/agents/'+agent_id)
        except Exception as exc:
            if CMA._gone(exc):
                return {'agent_id':agent_id,'stopped':True,'deleted':True,
                        'status':'gone','gone':True,'errors':[],
                        'operator_check':CMA._operator_check(
                            agent_id,'Sandbox or Agent is already gone; verify CMA cleanup manually',sandbox_id)}
            raise
        status=agent.get('status')
        result={'agent_id':agent_id,'stopped':status in {'stopped','stopping','deleting','deleted'},
                'deleted':status=='deleted','status':status,'errors':[]}
        if status=='deleted':return result
        if status=='deleting':
            if getattr(self,'retire_wait',0):
                try:agent=CMA._await_status(self,agent_id,{'deleted'},agent)
                except Exception as exc:
                    if CMA._gone(exc):return {**result,'deleted':True,'status':'gone','gone':True}
                    raise
                result.update(status=agent.get('status'),deleted=agent.get('status')=='deleted')
            return result

        original_status=status
        stop_refused=False
        if status not in {'stopped','stopping'}:
            try:
                stopped=self.request('/api/v1/agents/'+agent_id+'/stop','POST',{},key+'-stop')
                # Some CMA deployments return the transition in the POST body;
                # use it when present and reconcile with a GET otherwise.
                if isinstance(stopped,dict) and stopped.get('status'):
                    agent=stopped
                else:
                    agent=None
            except Exception as exc:
                if CMA._gone(exc):
                    return {**result,'operator_check':CMA._operator_check(
                        agent_id,'Sandbox disappeared while stopping the Agent',sandbox_id)}
                if getattr(exc,'code',None) not in {400,409,422}:raise
                result['errors'].append(CMA._error('stop',exc))
                stop_refused=True
                agent=None
            if stop_refused and original_status in {'failed','ready'}:
                agent={'status':original_status}
            else:
                try:
                    agent=CMA._await_status(self,agent_id,{'stopped','deleted'},agent)
                except Exception as exc:
                    if CMA._gone(exc):
                        return {**result,'operator_check':CMA._operator_check(
                            agent_id,'Sandbox disappeared while observing the stop',sandbox_id)}
                    raise
        elif status=='stopping' and getattr(self,'retire_wait',0):
            try:agent=CMA._await_status(self,agent_id,{'stopped','deleted'},agent)
            except Exception as exc:
                if CMA._gone(exc):
                    return {**result,'operator_check':CMA._operator_check(
                        agent_id,'Sandbox disappeared while observing the stop',sandbox_id)}
                raise

        status=agent.get('status')
        result.update(status=status,stopped=status in {'stopped','stopping','deleting','deleted'},
                      deleted=status=='deleted')
        # A stop refusal for a failed Agent is terminal and CMA permits its
        # deletion. A ready Agent still needs an observed stopped state: a
        # successful stop request can be asynchronous and a cached ready view
        # is not proof that DELETE is safe.
        can_delete=status in {'stopped','deleted'} or original_status=='failed'
        if not can_delete:
            return result
        if status=='deleted':return result
        try:
            deleted=self.request('/api/v1/agents/'+agent_id,'DELETE',None,key+'-delete')
            if isinstance(deleted,dict) and deleted.get('status'):
                agent=deleted
            else:
                agent=None
        except Exception as exc:
            if CMA._gone(exc):
                return {**result,'stopped':True,'deleted':True,'status':'gone','gone':True,
                        'delete_outcome':'gone'}
            if getattr(exc,'code',None) not in {400,409,422}:raise
            result['errors'].append(CMA._error('delete',exc));return result
        result['delete_outcome']='accepted'
        try:
            agent=CMA._await_status(self,agent_id,{'deleted'},agent)
        except Exception as exc:
            if CMA._gone(exc):
                return {**result,'stopped':True,'deleted':True,'status':'gone','gone':True}
            raise
        result.update(status=agent.get('status'),stopped=True,deleted=agent.get('status')=='deleted')
        return result

    def retire(self, agent_id, key, owned_sandbox_id=None, *, sweep_owned=False,
                       task_ids=frozenset()):
        try:agent=self.request('/api/v1/agents/'+agent_id)
        except Exception as exc:
            if CMA._gone(exc):
                result={'agent_id':agent_id,'stopped':True,'deleted':True,
                        'status':'gone','gone':True,'errors':[]}
            elif getattr(exc,'code',None)==404 and owned_sandbox_id:
                # A routing 404 is not proof that the Agent is gone, but the
                # caller still supplied the original Sandbox identity. Reconcile
                # that Sandbox and retain the routing check in the result.
                result={'agent_id':agent_id,'stopped':False,'deleted':False,
                        'status':'routing-404','errors':[],
                        'operator_check':CMA._operator_check(
                            agent_id,'Agent GET returned an unclassified 404; verify CMA routing/resource shape',
                            owned_sandbox_id)}
            else:
                raise
            if owned_sandbox_id:
                result=self.retire_sandbox(agent_id,owned_sandbox_id,key,result,
                                          sweep_owned=sweep_owned,task_ids=task_ids)
        else:
            sid=owned_sandbox_id or (agent.get('sandbox') or {}).get('id')
            result=self._retire_agent(agent_id,key,agent,sid)
            if owned_sandbox_id and result.get('deleted') and not result.get('operator_check'):
                result=self.retire_sandbox(agent_id,owned_sandbox_id,key,result,
                                          sweep_owned=sweep_owned,task_ids=task_ids)
        # The pool consumes this per-call budget counter directly. Task cleanup
        # records describe durable outcomes and never store the internal counter.
        result.pop('_agent_deletions',None)
        return result

    @staticmethod
    def _ours(agent, owned_ids, task_ids=None):
        if agent.get('id') in owned_ids:return True
        name=agent.get('name')
        return (isinstance(name,str) and name in {'outreach-'+task_id for task_id in (task_ids or ())})

    def retire_sandbox(self,agent_id,sandbox_id,key,result,dead_ok=False,finished=frozenset(),
                       max_agent_deletions=None, sweep_owned=True, task_ids=None):
        result=dict(result or {})
        try:sandbox=self.request('/api/v1/sandboxes/'+sandbox_id)
        except Exception as exc:
            if CMA._gone(exc):
                result.update(sandbox_id=sandbox_id,sandbox_retired=True,
                              operator_check={'kind':'cma-sandbox-cleanup','sandbox_id':sandbox_id,
                                              'reason':'Sandbox is already gone; verify its Agents manually'})
                return result
            raise
        if sandbox.get('status')=='retired':return {**result,'sandbox_id':sandbox_id,'sandbox_retired':True}
        query={'sandbox':sandbox_id,'limit':100}
        agents=self.request('/api/v1/agents?'+urlencode(query))
        items=list(agents['items'])
        # A dead pool Sandbox accumulates hundreds of Agents; read every page
        # (bounded) instead of retaining it forever on the first cursor.
        for _ in range(20 if dead_ok else 0):
            if not agents.get('next'):break
            agents=self.request('/api/v1/agents?'+urlencode({**query,'cursor':agents['next']}))
            items+=agents['items']
        owned_ids=set(finished)
        if agent_id:owned_ids.add(agent_id)
        previous_checks=result.get('operator_check') or {}
        checked=set(previous_checks.get('agents',[])) if isinstance(previous_checks,dict) else set()
        preserve_check=result.get('status')=='routing-404'
        present_ids={a.get('id') for a in items if a.get('id') and a.get('status')!='deleted'}
        checked &= present_ids
        if not checked and not preserve_check:
            result.pop('operator_check',None)
        if sweep_owned:
            owned=[a for a in items if CMA._ours(a,owned_ids,task_ids)]
        else:
            owned=[a for a in items if a.get('id') in owned_ids]
        outcomes_by_id={item.get('agent_id'):item for item in result.get('agent_cleanup',[])
                        if isinstance(item,dict) and item.get('agent_id')}
        pending=[];checks=list(checked);processed=0;processed_ids=set()
        for item in owned:
            aid=item.get('id')
            if not aid or item.get('status')=='deleted':continue
            if aid == result.get('agent_id') and result.get('deleted'):continue
            if aid in checked:
                checks.append(aid);continue
            if max_agent_deletions is not None and processed>=max_agent_deletions:
                pending.append(aid);continue
            processed+=1
            processed_ids.add(aid)
            prior_attempts=outcomes_by_id.get(aid,{}).get('cleanup_attempts',0)
            agent_key=key+'-agent-'+aid
            if prior_attempts:agent_key+='-retry-'+str(prior_attempts)
            try:outcome=CMA._retire_agent(self,aid,agent_key,item,sandbox_id)
            except Exception as exc:
                outcome={'agent_id':aid,'stopped':False,'deleted':False,'status':item.get('status'),
                         'errors':[CMA._error('retire',exc)]}
            prior=outcomes_by_id.get(aid,{})
            failed=(not outcome.get('deleted') and
                    bool(outcome.get('errors') or outcome.get('cleanup_error')))
            attempts=prior.get('cleanup_attempts',0)+(1 if failed else 0)
            outcome['cleanup_attempts']=attempts
            if (not outcome.get('deleted') and attempts>=3 and
                    not outcome.get('operator_check')):
                outcome['operator_check']=CMA._operator_check(
                    aid,'Agent cleanup did not complete after three bounded attempts',sandbox_id)
            outcomes_by_id[aid]=outcome
            if outcome.get('operator_check'):
                checks.append(aid)
            elif not outcome.get('deleted'):
                pending.append(aid)
        pending.extend(a.get('id') for a in owned if a.get('id') and
                       a.get('status')!='deleted' and a.get('id') not in processed_ids and
                       a.get('id') not in checked and a.get('id') not in checks)
        pending=list(dict.fromkeys(pending))
        outcomes=list(outcomes_by_id.values())
        if checks:
            operator={'kind':'cma-agent-cleanup','sandbox_id':sandbox_id,'agents':sorted(set(checks)),
                      'reason':'Engine-owned Agent cleanup requires an operator check; see agent outcomes'}
            result.update(sandbox_id=sandbox_id,sandbox_retired=False,operator_check=operator,
                          agent_cleanup=outcomes,_agent_deletions=processed)
            if pending:result['owned_agents_pending']=pending
            return result
        if pending or agents.get('next'):
            result.update(sandbox_id=sandbox_id,sandbox_retired=False,agent_cleanup=outcomes,
                          owned_agents_pending=pending,_agent_deletions=processed,
                          reason='Engine Agent cleanup pending')
            if agents.get('next'):result['agent_inventory_pending']=True
            return result
        # Preserve other work in the Sandbox. The pool can classify ownership
        # across tasks; a task retirement knows only its own Agent identity.
        result.pop('owned_agents_pending',None);result.pop('agent_inventory_pending',None)
        if sweep_owned:
            foreign=[a for a in items if not CMA._ours(a,owned_ids,task_ids) and
                     a.get('status')!='deleted']
        else:
            foreign=[a for a in items if a.get('id') not in owned_ids and
                     a.get('status')!='deleted']
        if foreign:
            foreign_ids=[a.get('id') for a in foreign if a.get('id')]
            retained={**result,'sandbox_id':sandbox_id,'sandbox_retired':False,
                      'reason':'Other Agent work found; retained Sandbox',
                      'foreign_agents':foreign_ids,'_agent_deletions':processed}
            # A task can share a pool Sandbox with other live tasks. Only the
            # pool's inventory sweep can decide that remaining work is foreign.
            if sweep_owned:
                retained['operator_check']={'kind':'cma-sandbox-cleanup','sandbox_id':sandbox_id,
                    'agents':foreign_ids,
                    'reason':'Foreign Agents remain; operator must reconcile them before Sandbox DELETE'}
            return retained
        workspaces=self.request('/api/v1/sandboxes/'+sandbox_id+'/workspaces')
        if workspaces.get('next') or len(workspaces['items'])>1:
            return {**result,'sandbox_id':sandbox_id,'sandbox_retired':False,'reason':'Additional workspaces found; retained Sandbox',
                    '_agent_deletions':processed}
        result.pop('reason',None);result.pop('foreign_agents',None)
        if (sandbox.get('status')=='retiring' and result.get('sandbox_delete_requested') and
                not result.get('sandbox_delete_error')):
            # A prior DELETE was admitted but its response was lost. Reconcile
            # the GET state and keep the original key; do not issue a second
            # DELETE while CMA is already retiring the Sandbox.
            return {**result,'sandbox_id':sandbox_id,'sandbox_retired':False,
                    '_agent_deletions':processed}
        sandbox_attempts=int(result.get('sandbox_delete_attempts',0) or 0)
        sandbox_key=key+'-sandbox-delete'
        if sandbox_attempts:sandbox_key+='-retry-'+str(sandbox_attempts)
        try:
            self.request('/api/v1/sandboxes/'+sandbox_id,'DELETE',None,sandbox_key)
        except Exception as exc:
            if CMA._gone(exc):
                return {**result,'sandbox_id':sandbox_id,'sandbox_retired':True,
                        'sandbox_status':'gone','sandbox_gone':True,
                        'sandbox_delete_outcome':'gone',
                        'sandbox_delete_requested':True,'_agent_deletions':processed}
            if getattr(exc,'code',None) is None:
                # The DELETE may have been admitted before the transport
                # failed. Preserve that uncertainty and the original key; a
                # later GET skips replay while CMA reports `retiring`, but
                # retries the same key if the Sandbox is still active.
                return {**result,'sandbox_id':sandbox_id,'sandbox_retired':False,
                        'sandbox_delete_requested':True,
                        'sandbox_transport_error':CMA._error('delete',exc),
                        '_agent_deletions':processed}
            if getattr(exc,'code',None) not in {400,409,422}:
                raise
            updated={**result,'sandbox_id':sandbox_id,'sandbox_retired':False,
                     'sandbox_delete_attempts':sandbox_attempts+1,
                     'sandbox_delete_error':CMA._error('delete',exc),
                     'sandbox_delete_requested':False,'_agent_deletions':processed}
            if updated['sandbox_delete_attempts']>=3:
                updated['operator_check']={'kind':'cma-sandbox-cleanup','sandbox_id':sandbox_id,
                    'reason':'Sandbox DELETE was refused after three bounded attempts'}
            return updated
        result['sandbox_delete_outcome']='accepted'
        try:
            sandbox=self.request('/api/v1/sandboxes/'+sandbox_id)
        except Exception as exc:
            if CMA._gone(exc):
                return {**result,'sandbox_id':sandbox_id,'sandbox_retired':True,
                        'sandbox_status':'gone','sandbox_gone':True,
                        'sandbox_delete_requested':True,'_agent_deletions':processed}
            raise
        return {**result,'sandbox_id':sandbox_id,'sandbox_retired':sandbox.get('status')=='retired',
                'sandbox_delete_requested':True,'_agent_deletions':processed}


def output_text(response):
    """Read the final message; progress messages are separate durable items."""
    for item in reversed(response.get('output',[])):
        if item.get('type')!='message':continue
        chunks=[p.get('text','') for p in item.get('content',[]) if p.get('type')=='output_text']
        if any(chunks):return '\n'.join(chunks)
    return ''
