"""CMA's deployed REST/lifecycle and Responses v2 contracts, via NyxID.

Tokens stay in the environment or NyxID credential store. A disconnected SSE
stream never implies that a server-owned turn stopped.
"""
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
            with build_opener(NoRedirect()).open(
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
                done=subprocess.run(args,capture_output=True,text=True,timeout=artifact_io_timeout(35))
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
        deadline=time.time()+self.retire_wait
        agent=agent or self.request('/api/v1/agents/'+agent_id)
        while agent.get('status') not in done and time.time()<deadline:
            time.sleep(2)
            agent=self.request('/api/v1/agents/'+agent_id)
        return agent

    def retire(self, agent_id, key, owned_sandbox_id=None):
        agent=self.request('/api/v1/agents/'+agent_id)
        sid=owned_sandbox_id or (agent.get('sandbox') or {}).get('id')
        failed=agent.get('status')=='failed'
        if sid and not failed and agent.get('status') not in {'deleted','deleting'}:
            failed=self.request('/api/v1/sandboxes/'+sid).get('status')=='failed'
        if failed:
            result={'agent_id':agent_id,'stopped':False,'deleted':False,'errors':[]}
            for action,method,body in [('stop','POST',{}),('delete','DELETE',None)]:
                try:
                    self.request('/api/v1/agents/'+agent_id+('/stop' if action=='stop' else ''),
                                 method,body,key+'-'+action)
                    observed=self.request('/api/v1/agents/'+agent_id)
                    result['stopped']=observed.get('status') in {'stopped','deleted'}
                    result['deleted']=observed.get('status')=='deleted'
                except Exception as exc:
                    if getattr(exc,'code',None) not in {400,409,422}:raise
                    result['errors'].append({'action':action,'status':exc.code,
                                            'error':getattr(exc,'body',str(exc))})
            return result
        if agent.get('status')=='deleting' and self.retire_wait:
            agent=self._await_status(agent_id,{'deleted'},agent)
        if agent.get('status')=='deleted':
            result={'agent_id':agent_id,'stopped':True,'deleted':True}
            return self.retire_sandbox(agent_id,owned_sandbox_id,key,result) if owned_sandbox_id else result
        if agent.get('status')=='deleting':return {'agent_id':agent_id,'stopped':True,'deleted':False,'status':'deleting'}
        if agent.get('status') not in {'stopped','stopping'}:
            self.request('/api/v1/agents/'+agent_id+'/stop','POST',{},key+'-stop')
            agent=self._await_status(agent_id,{'stopped'})
        if agent.get('status')!='stopped':
            return {'agent_id':agent_id,'stopped':False,'deleted':False,'status':agent.get('status')}
        self.request('/api/v1/agents/'+agent_id,'DELETE',None,key+'-delete')
        agent=self._await_status(agent_id,{'deleted'})
        result={'agent_id':agent_id,'stopped':True,'deleted':agent.get('status')=='deleted','status':agent.get('status')}
        return self.retire_sandbox(agent_id,owned_sandbox_id,key,result) if owned_sandbox_id and result['deleted'] else result

    def retire_sandbox(self,agent_id,sandbox_id,key,result,dead_ok=False,finished=frozenset()):
        sandbox=self.request('/api/v1/sandboxes/'+sandbox_id)
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
        # A task-owned context can still acquire unexpected user work. Preserve it.
        # A failed or corrupt pool Sandbox may hold failed/stopped Agents that CMA
        # refuses to stop or delete (409); they run nothing, so they do not block.
        # Neither do idle Agents of our own finished executions (`finished`, from
        # the ledger), which a corrupt runtime also refuses to stop (sbx_54df).
        settled={'deleted','failed','stopped'} if dead_ok else {'deleted'}
        if agents.get('next') or any(a['status'] not in settled and not (dead_ok and a['id'] in finished)
                                     for a in items):
            return {**result,'sandbox_id':sandbox_id,'sandbox_retired':False,'reason':'Other Agent work found; retained Sandbox'}
        workspaces=self.request('/api/v1/sandboxes/'+sandbox_id+'/workspaces')
        if workspaces.get('next') or len(workspaces['items'])>1:
            return {**result,'sandbox_id':sandbox_id,'sandbox_retired':False,'reason':'Additional workspaces found; retained Sandbox'}
        self.request('/api/v1/sandboxes/'+sandbox_id,'DELETE',None,key+'-sandbox-delete')
        sandbox=self.request('/api/v1/sandboxes/'+sandbox_id)
        return {**result,'sandbox_id':sandbox_id,'sandbox_retired':sandbox.get('status')=='retired'}


def output_text(response):
    """Read the final message; progress messages are separate durable items."""
    for item in reversed(response.get('output',[])):
        if item.get('type')!='message':continue
        chunks=[p.get('text','') for p in item.get('content',[]) if p.get('type')=='output_text']
        if any(chunks):return '\n'.join(chunks)
    return ''
