"""Validate the compact control repository or refresh upstream result coverage."""
import argparse
from collections import Counter
import json
from pathlib import Path
import re
import subprocess
import sys

from .location import DATA_ROOT as ROOT, ENGINE_ROOT, load_config, source_head
TABLES={'contacts','positive','refusals','threads','projects','results','publications','decisions','workspaces','operations','submissions'}


def read(name):return json.loads((ROOT/'tables'/name).with_suffix('.json').read_text())


def check():
    if {p.stem for p in (ROOT/'tables').glob('*.json')}!=TABLES:
        raise ValueError('Core table inventory differs')
    for name in TABLES:read(name)
    for name,key,identity in [('contacts','contacts','email'),('threads','lines','id'),
                              ('results','results','id'),('projects','projects','id'),
                              ('workspaces','workspaces','id')]:
        rows=read(name)[key];ids=[r[identity] for r in rows]
        if len(ids)!=len(set(ids)):raise ValueError('Duplicate '+name+' identity')
    source=read('results')['source_commit']
    if not re.fullmatch('[0-9a-f]{40}',source):raise ValueError('Missing upstream commit')
    for r in read('results')['results']:
        if r.get('upstream',{}).get('commit',source)!=source:raise ValueError('Mixed upstream coverage revisions')
    config=load_config(ROOT)
    check_config(config)
    from .registry import validate
    registry=validate(ROOT)
    from .submissions import counts as submission_counts
    submissions=submission_counts(read('submissions'))
    return {'schema_version':1,'contacts':len(read('contacts')['contacts']),
        'contact_map':registry,
        'submissions':submissions,
        'threads':len(read('threads')['lines']),'results':len(read('results')['results']),
        'notes':dict(Counter(r['note_status'] for r in read('results')['results'])),
        'paused':config['paused'],'source_commit':source}


def check_config(config):
    from .lanes import validate as validate_lanes
    validate_lanes(config)
    from .tasks import KINDS
    if set(config['kinds'])!=KINDS or any(type(v) is not bool for v in config['kinds'].values()):
        raise ValueError('Declare every supported task kind with an explicit boolean')
    automation=config.get('automation',{})
    for field,maximum in [('max_new_tasks_per_tick',16),('max_revisions',10)]:
        if type(automation.get(field)) is not int or not 1<=automation[field]<=maximum:
            raise ValueError('Bound automation '+field)
    pool=config.get('sandbox_pool')
    if pool:
        from .sandbox_pool import group_config
        group_config(pool, config['kinds'])
    review=config.get('independent_review', {})
    if review:
        if (type(review.get('enabled')) is not bool
                or type(review.get('max_active', 1)) is not int
                or not 1 <= review.get('max_active', 1) <= 4
                or type(review.get('max_active_per_group', 1)) is not int
                or not 1 <= review.get('max_active_per_group', 1) <= review.get('max_active', 1)
                or type(review.get('register_author_contacts', False)) is not bool):
            raise ValueError('Declare a bounded independent review configuration')
        if review['enabled'] and (not pool or pool['agents_per_sandbox'] < 2):
            raise ValueError('Independent review needs an existing pool with a reserved Agent slot')
    # Without a pool each task owns a Sandbox; with one, Agents share at most
    # max_sandboxes x agents_per_sandbox slots (CMA allows 16 Agents per Workspace).
    ceiling=16 if not pool else pool['max_sandboxes']*pool['agents_per_sandbox']
    if type(config['max_active']) is not int or not 1<=config['max_active']<=min(ceiling,64):
        raise ValueError('Explicit bounded CMA concurrency required')
    from .control import workflow
    for kind in config['kinds']:workflow(kind)
    return {'status': 'validated'}


def upstream(repo,ref):
    commit=subprocess.check_output(['git','rev-parse',ref+'^{commit}'],cwd=repo,text=True).strip()
    raw=subprocess.check_output(['git','grep','-h','scribe-open-problem-resolution-v1',commit,'--','Blueprint'],cwd=repo,text=True)
    markers={}
    for line in raw.splitlines():
        match=re.search(r'<!--\s*scribe-open-problem-resolution-v1\s+(\{.*\})\s*-->',line)
        if not match:continue
        m=json.loads(match[1]);slug=m['problem_slug']
        row=markers.setdefault(slug,{'repository':'the-omega-institute/trureturing','commit':commit,
            'resolution_kind':m['resolution_kind'],'declaration_gids':[]})
        if row['resolution_kind']!=m['resolution_kind']:raise ValueError('Conflicting upstream resolution kinds')
        if m['declaration_gid'] not in row['declaration_gids']:row['declaration_gids'].append(m['declaration_gid'])
    if not markers:raise ValueError('Ref has no existing upstream resolution markers')
    data=read('results');rows={r['id']:r for r in data['results']}
    for row in rows.values():
        if row['id'] not in markers and 'upstream' in row:
            row['prior_upstream']=row.pop('upstream');row['upstream_status']='absent-at-current-snapshot'
    for slug,marker in markers.items():
        row=rows.setdefault(slug,{'id':slug,'note_required':True,'note_status':'missing',
            'publication_status':'unselected','mail_status':'unprepared'})
        row['upstream']=marker;row['upstream_status']='resolved-at-current-snapshot'
    data.update(source_commit=commit,results=sorted(rows.values(),key=lambda r:r['id']))
    (ROOT/'tables/results.json').write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n')
    return {'source_commit':commit,'resolved':len(markers),'total_records':len(rows)}


def plan_notes(output):
    """Materialize immutable requests; only an explicit enqueue reaches the ledger."""
    from .control import current_policy_revision, request_binding
    from .queue import digest, canonical
    from .author_contacts import INSTRUCTIONS as author_instructions
    contacts=read('contacts')['contacts'];rows=read('results')['results']
    commit=source_head()
    binding=request_binding('shortnote', commit, current_policy_revision())
    output=Path(output).resolve()
    if any(output == root or root in output.parents for root in (ROOT, ENGINE_ROOT)):raise ValueError('Task inputs belong under mail, outside the control checkout')
    output.mkdir(parents=True,exist_ok=True);count=0
    for row in rows:
        if row.get('upstream_status')!='resolved-at-current-snapshot' or row['note_status']=='reviewed':continue
        matches=[p for p in contacts if row['id'] in p.get('problem_ids',[])]
        owner=matches[0]['id'] if matches else 'result-'+row['id']
        inputs={'result':row,'recorded_contacts':matches,'note_round':'draft','owner_ids':[p['id'] for p in matches],
                'decision_table_at_workflow_revision':'tables/decisions.json',
                'migration_release':read('operations')['migration_release'],
                'instructions':'Reuse existing note assets first; verify current holds and ownership. Produce a complete private short note. This request grants no outreach or publication authority. '+author_instructions}
        identity = {'inputs': inputs, 'workflow_revision': commit}
        if 'policy_revision' in binding:
            identity['policy_revision'] = binding['policy_revision']
        request={'schema_version':1,'id':'note-'+digest(identity)[:32],'kind':'shortnote',
                 'owner':owner,'inputs':inputs,**binding}
        text=canonical(request)+'\n';path=output/(request['id']+'.json')
        if path.exists() and path.read_text()!=text:raise ValueError('Existing plan differs; use a fresh output directory')
        path.write_text(text);count+=1
    return {'prepared_requests':count,'directory':str(output),'enqueued':False}


def main():
    p=argparse.ArgumentParser(description=__doc__);s=p.add_subparsers(dest='action',required=True)
    s.add_parser('check')
    s.add_parser('check-engine')
    u=s.add_parser('sync-results');u.add_argument('--repo',type=Path,required=True);u.add_argument('--ref',default='origin/dev')
    n=s.add_parser('plan-notes');n.add_argument('--output',type=Path,required=True)
    a=p.parse_args()
    if a.action == 'check-engine':
        from .location import example_config
        result = check_config(example_config())
    elif a.action == 'check':
        result = check()
    elif a.action == 'plan-notes':
        result = plan_notes(a.output)
    else:
        result = upstream(a.repo, a.ref)
    print(json.dumps(result,ensure_ascii=False,indent=2))

if __name__ == '__main__':
    from .run_log import public_run
    raise SystemExit(public_run(main, 'coverage', persist='check-engine' not in sys.argv[1:]))
