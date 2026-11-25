"""The finite Outreach flow: upstream -> note -> outreach/Mathesis, plus daily.

This runs inside the existing single writer. It creates ordinary CMA requests;
it neither reviews nor sends/submits/publishes. The mailbox feedback entry is
deferred until a live mailbox adapter can establish actual thread identity.
"""
from datetime import datetime, timedelta
import json
from pathlib import Path
import re
import subprocess
import tempfile
from zoneinfo import ZoneInfo

from .location import source_head
from .queue import canonical, digest, require
from .registry import activity, email, people, preflight
from .author_contacts import INSTRUCTIONS as AUTHOR_CONTACT_INSTRUCTIONS
from .tasks import unstarted
from . import note_closure, note_revisions

ZONE = ZoneInfo('Asia/Singapore')


def read(root, name):
    return json.loads((Path(root)/'tables'/(name+'.json')).read_text())


def source_identity(row):
    upstream = row['upstream']
    # The coverage commit changes for every row on every hourly sync. It does
    # not mean that every result is new. Freeze the first observed commit below.
    return digest({'id':row['id'], 'repository':upstream['repository'],
                   'kind':upstream['resolution_kind'],
                   'declarations':sorted(upstream['declaration_gids'])})


def note_coverage(rows, tasks):
    """Index explicit metadata coverage of exact PASSed, completed notes only."""
    from .tasks import review_current
    covered = {}
    for row in rows:
        if (not note_closure.eligible(row) or row['state'] not in {'approved','handoff-ready','verified'}
                or row['data'].get('result', {}).get('outcome') != 'completed'
                or not review_current(row, row['request']['workflow_hash'])):
            continue
        cached = tasks.db.execute('SELECT payload FROM pipeline_state WHERE key=?',
                                  ('note-coverage:'+row['id'],)).fetchone()
        for metadata in (json.loads(cached[0]) if cached else {}, row['data'].get('publication_loop', {})):
            if metadata.get('result_hash') != digest(row['data']['result']):
                continue
            ids = metadata.get('problem_ids')
            if isinstance(ids, list) and ids and all(isinstance(p, str) and p.strip() for p in ids):
                for rid in dict.fromkeys(ids):
                    covered.setdefault(rid, []).append(row)
                break
    return covered


def covering_note(row, covered, by_id):
    """A note's own uplift/revision lineage is not duplicate sibling work."""
    inputs = row['request']['inputs']
    rid = inputs.get('pipeline', {}).get('result_id') or inputs.get('result_id') or inputs.get('result', {}).get('id')
    def ancestors(start):
        seen, pending = set(), [start]
        while pending:
            current = pending.pop()
            if current and current['id'] not in seen:
                seen.add(current['id'])
                pending.extend(by_id.get(link['task_id']) for link in note_revisions.source_links(current))
        return seen
    # Same lineage in either direction (a revision's ancestors and descendants)
    # is the same note, not another one.
    mine = ancestors(row)
    return rid, next((note for note in covered.get(rid, [])
                      if note['id'] not in mine and row['id'] not in ancestors(note)), None)


def cache_note_coverage(ledger, limit):
    """Read existing reviewed metadata artifacts, bounded by pipeline units/cap."""
    from .publication import download
    from .tasks import review_current
    from .tick import units
    from . import recheck
    candidates = [r for r in ledger.tasks.all() if note_closure.eligible(r)
                  and r['state'] in {'approved','handoff-ready','verified'}
                  and r['data'].get('result', {}).get('outcome') == 'completed'
                  and review_current(r, r['request']['workflow_hash'])]
    inspected = set()
    for row in units(ledger, 'pipeline-note-coverage', candidates):
        inspected.add(row['id'])
        if note_coverage([row], ledger.tasks):
            continue
        binding = digest(row['data']['result'])
        if not recheck.due(ledger.tasks, 'note-coverage', row['id'], binding):
            continue
        names = {a['name'] for a in row['data']['result'].get('artifacts', [])}
        manifests = sorted(n for n in names if re.fullmatch(r'note-v[1-9][0-9]*\.json', n))
        name = ('handoff.json' if 'handoff.json' in names else
                manifests[0] if len(manifests) == 1 else
                'metadata.json' if 'metadata.json' in names else None)
        if not name:
            continue
        if not limit:
            return False  # Finish the bounded index scan before deriving new notes.
        limit -= 1
        try:
            with tempfile.TemporaryDirectory(prefix='note-coverage-') as folder:
                manifest = json.loads(download(row, name, folder, ledger.releases).read_text())
            require(isinstance(manifest, dict), 'Invalid note coverage manifest')
            metadata = (manifest['source_note'] if name == 'handoff.json' else
                        manifest['metadata'] if name != 'metadata.json' else manifest)
            require(isinstance(metadata, dict) and isinstance(metadata.get('id'), str)
                    and metadata['id'].strip(), 'Missing stable note identity')
            ids = metadata['problem_ids']
            require(isinstance(ids, list) and ids and all(isinstance(p, str) and p.strip() for p in ids),
                    'Missing explicit note coverage')
            ledger.tasks.db.execute('INSERT OR REPLACE INTO pipeline_state VALUES(?,?)',
                ('note-coverage:'+row['id'], canonical({
                'result_hash':digest(row['data']['result']), 'note_id':metadata['id'],
                'problem_ids':ids, 'artifact':name})))
            ledger.save()
        except (ValueError, OSError, KeyError, subprocess.SubprocessError):
            # Missing/unavailable/mismatched metadata establishes no coverage.
            recheck.defer(ledger.tasks, 'note-coverage', row['id'], binding)
            ledger.save()
            continue
    return len(inspected) == len(candidates)


def note_coverage_overlap(rows, tasks):
    covered = note_coverage(rows, tasks)
    by_id = {r['id']:r for r in rows}
    overlaps = []
    for row in rows:
        if row['kind'] != 'shortnote' or row['state'] in {'superseded','round-complete'}:
            continue
        rid, note = covering_note(row, covered, by_id)
        if note:
            overlaps.append({'task_id':row['id'], 'state':row['state'], 'result_id':rid,
                             'covering_note_task_id':note['id']})
    return overlaps


def request(kind, identity, owner, inputs, revision):
    from .control import current_policy_revision, request_binding
    return {'schema_version':1, 'id':kind+'-'+digest(identity)[:32],
            'kind':kind, 'owner':owner, 'inputs':inputs,
            **request_binding(kind, revision, current_policy_revision())}


def outreach_recipients(note, contacts, root):
    """Use the reviewed posing-author order and one verified address per person."""
    rid = note['request']['inputs']['pipeline']['result_id']
    persons = people(root)
    linked = {p['id'] for p in persons if any(rid in r.get('problem_ids', []) for r in p['records'])}
    proposals = note['data']['result'].get('author_contacts') or []
    candidates = ([email(a['email']) for author in proposals for a in author['emails']]
                  if proposals else [email(p['email']) for p in contacts if rid in p.get('problem_ids', [])])
    path = Path(root)/'tables/threads.json'
    threads = read(root, 'threads')['lines'] if path.exists() else []
    selected, excluded, seen = [], [], set()
    for candidate in candidates:
        person = next((p for p in persons if candidate in p['emails']), None)
        if person is None or person['id'] not in linked or person['id'] in seen:
            continue
        check = preflight(candidate, person['name'], root)
        active = []
        for thread in [*threads, *activity(person)['threads']]:
            participants = {email(p['email']) for p in [thread.get('correspondent', {}),
                            *thread.get('co_correspondents', [])] if p.get('email')}
            participants.update(email(a) for a in thread.get('mail_match', {}).get('emails', []))
            if thread.get('correspondent_email'):
                participants.add(email(thread['correspondent_email']))
            if (participants & set(person['emails']) and
                    thread.get('stage', '').lower() not in {'closed', 'declined', 'stopped', 'cancelled', 'done', 'superseded'}):
                active.append(thread['id'])
        if not check['allowed'] or active:
            excluded.append({'contact_id': person['id'], 'email': candidate,
                             'reason': ', '.join(check['reasons']) if not active else 'active thread; continue as a reply',
                             'thread_ids': list(dict.fromkeys(active))})
            # An address failure does not rule out another verified address for
            # the same author. Refusals/history/active threads apply to the person.
            if active or set(check['reasons']) != {'undeliverable-address'}:
                seen.add(person['id'])
            continue
        seen.add(person['id'])
        selected.append({'contact_id': person['id'], 'email': candidate})
    return selected, excluded


def source_gate(task, tasks, root):
    """Recheck source and reviewed parent before admission and result acceptance."""
    link = task['inputs'].get('pipeline')
    if link is None:
        return  # Existing manually staged and historical tasks stay compatible.
    require(isinstance(link,dict), 'Pipeline provenance must be an object')
    require(type(link.get('scenario')) is int and link['scenario'] in {2,3,4,5}, 'Unknown pipeline scenario')
    if link['scenario'] == 5:
        require(task['kind'] == 'daily', 'A daily source window requires a daily task')
        return
    require(all(isinstance(link.get(k),str) and link[k] for k in ['result_id','source_identity']), 'Missing pipeline source identity')
    row = next((r for r in read(root,'results')['results'] if r['id']==link['result_id']), None)
    require(row is not None and row.get('upstream_status')=='resolved-at-current-snapshot',
            'Upstream result is no longer resolved at the current snapshot')
    require(source_identity(row)==link['source_identity'], 'Upstream result identity changed; review the new source')
    if link['scenario'] == 3:
        require(task['kind']=='shortnote', 'An upstream result first requires a short note')
        return
    require(all(isinstance(link.get(k),str) and link[k] for k in ['note_task_id','note_result_hash']), 'Missing reviewed note parent')
    require(task['kind']=={2:'firstcontact',4:'mathesis'}[link['scenario']], 'Wrong downstream task kind')
    parent = tasks.get(link['note_task_id'])
    require(parent['kind']=='shortnote' and parent['state'] in {'approved','handoff-ready','verified'},
            'The source note is not approved')
    require(note_closure.eligible(parent), 'The source note needs a non-trivial final round')
    require(parent['data'].get('review',{}).get('verdict')=='PASS', 'The source note needs independent review')
    require(digest(parent['data']['result'])==link['note_result_hash'], 'The approved note version changed')
    parent_link=parent['request']['inputs'].get('pipeline',{})
    require(parent_link.get('scenario')==3 and parent_link.get('result_id')==link['result_id']
            and parent_link.get('source_identity')==link['source_identity'], 'Wrong upstream parent')


def reconcile(ledger, config, root, now=None, revision=None):
    """Observe sources while paused; enqueue bounded, restart-safe work when active."""
    settings = config.get('pipeline',{})
    if not settings.get('enabled'):
        return
    now = now or datetime.now(ZONE)
    require(now.tzinfo is not None, 'Pipeline time needs a timezone')
    today = now.astimezone(ZONE).date()
    tasks = ledger.tasks; db = tasks.db
    sources = read(root,'results')
    resolved = {r['id']:r for r in sources['results'] if r.get('upstream_status')=='resolved-at-current-snapshot'}
    initialized = db.execute("SELECT payload FROM pipeline_state WHERE key='baseline'").fetchone()
    if not initialized:
        with tasks.tx():
            for rid, row in resolved.items():
                db.execute('INSERT INTO pipeline_sources VALUES(?,?,?,?,NULL)',
                           (rid,source_identity(row),canonical(row),1))
            db.execute('INSERT INTO pipeline_state VALUES(?,?)', ('baseline',canonical({
                'upstream_commit':sources['source_commit'], 'date':today.isoformat(),
                'historical_results':len(resolved)})))
        ledger.save(force=True)  # Freeze the initial historical/new-source boundary.
        return
    with tasks.tx():
        for rid, row in resolved.items():
            identity = source_identity(row)
            old = db.execute('SELECT identity FROM pipeline_sources WHERE result_id=?',(rid,)).fetchone()
            if not old:
                db.execute('INSERT INTO pipeline_sources VALUES(?,?,?,?,NULL)',(rid,identity,canonical(row),0))
            elif old['identity'] != identity:
                db.execute('UPDATE pipeline_sources SET identity=?,payload=?,baseline=0,task_id=NULL WHERE result_id=?',
                           (identity,canonical(row),rid))
    # Observations/derivations can be replayed; the tick's final forced save
    # preserves pending sources while paused. CMA creation forces its own intent.
    ledger.save()
    if config['paused']:
        return
    revision = revision or source_head()
    contacts = read(root,'contacts')['contacts']
    backfill = set(settings.get('backfill_result_ids',[]))
    auto_backfill = settings.get('auto_backfill', False)
    historical_limit = settings.get('backfill_per_tick', 1)
    pending_limit = settings.get('backfill_pending_limit', 16)
    require(type(auto_backfill) is bool and type(historical_limit) is int and 1 <= historical_limit <= 4
            and type(pending_limit) is int and 1 <= pending_limit <= 256,
            'Bound automatic historical coverage and its pending queue')
    historical_started = 0
    budget = settings.get('max_new_tasks_per_tick',4)
    require(type(budget) is int and 1<=budget<=16, 'Bound pipeline task creation to 1..16 per tick')
    coverage_ready = cache_note_coverage(ledger, budget)
    rows = tasks.all()
    pending_notes = sum(r['kind'] == 'shortnote' and r['state'] in
                        {'queued','provisioning','provision-unknown','admitting','admission-unknown',
                         'running','credit-wait','revising','awaiting-review'} for r in rows)
    covered = note_coverage(rows, tasks)
    by_id = {r['id']:r for r in rows}
    revisions = note_revisions.Revisions(tasks, rows)

    from .tick import units
    queued_notes = [r for r in rows if r['kind'] == 'shortnote' and r['state'] == 'queued']
    for old in units(ledger, 'pipeline-covered-tasks', queued_notes):
        # A sent-revision merge exists precisely because the covering note
        # omits what a correspondent saw; coverage never supersedes it.
        if not unstarted(old) or note_revisions.Revisions.is_merge(old):
            continue
        rid, note = covering_note(old, covered, by_id)
        if note:
            with tasks.tx():
                tasks._update(old['id'], old['revision'], 'superseded', {
                    **old['data'], 'covering_note_task_id':note['id'],
                    'reason':'Resolved result already covered by a PASSed completed short note'})
                db.execute('UPDATE pipeline_sources SET task_id=? WHERE result_id=? AND (task_id IS NULL OR task_id=?)',
                           (note['id'], rid, old['id']))
            ledger.save()

    def enqueue(value):
        nonlocal budget
        if db.execute('SELECT 1 FROM tasks WHERE id=?',(value['id'],)).fetchone():
            return True
        if not budget:
            return False
        tasks.add(value); budget-=1; ledger.save()
        return True

    if config['kinds'].get('mathesis'):
        from .publication import reviewed
        merged = {r['id']: r for r in revisions.heads.values()
                  if revisions.is_merge(r) and r['state'] in note_revisions.READY_STATES
                  and reviewed(r) and not revisions.reason(r)}
        for note in units(ledger, 'pipeline-merged-notes', merged.values()):
            inputs = note['request']['inputs']
            rid = inputs['sent_revision_result_id']
            if rid not in resolved:
                continue
            note_link = note_revisions.binding(note)
            downstream = {'scenario':4, 'result_id':rid, 'source_identity':source_identity(resolved[rid]),
                          'note_task_id':note['id'], 'note_result_hash':note_link['result_hash']}
            science = {'result':resolved[rid], 'note':note['data']['result'],
                       'note_sources':[note_link], 'owner_ids':inputs.get('owner_ids', [])}
            if inputs.get('pipeline', {}).get('result_id') == rid:
                science['pipeline'] = downstream
            enqueue(request('mathesis', downstream, note['owner'], {
                **science, 'instructions':'Use only the exact approved merged scientific note and its evidence '
                'to prepare the Mathesis handoff. Exclude private correspondence and the private difference list.'
            }, revision))

    for record in units(ledger,'pipeline-sources',db.execute('SELECT * FROM pipeline_sources ORDER BY baseline,rowid').fetchall(),
                        key=lambda r:r['result_id']):
        rid = record['result_id']
        if (not config['kinds'].get('shortnote') or rid not in resolved or record['task_id']
                or (record['baseline'] and rid not in backfill and not auto_backfill)):
            continue
        if rid in covered:
            db.execute('UPDATE pipeline_sources SET task_id=? WHERE result_id=?', (covered[rid][0]['id'], rid))
            ledger.save()
            continue
        if not coverage_ready:
            continue
        automatic_historical = record['baseline'] and rid not in backfill
        if automatic_historical and (historical_started >= historical_limit or pending_notes >= pending_limit):
            continue
        row = json.loads(record['payload'])
        matches = [p for p in contacts if rid in p.get('problem_ids',[])]
        link = {'scenario':3, 'result_id':rid, 'source_identity':record['identity']}
        value = request('shortnote',link,'result-'+rid,{
            'pipeline':link, 'result':row, 'recorded_contacts':matches,
            'note_round':'draft',
            'owner_ids':[p['id'] for p in matches],
            'instructions':'Read current holds and ownership. Reuse an existing note first. Produce a checked, rebuildable note and PDF. This is the source for author outreach and Mathesis; no sending or publication authority. '+AUTHOR_CONTACT_INSTRUCTIONS},revision)
        if enqueue(value):
            db.execute('UPDATE pipeline_sources SET task_id=? WHERE result_id=?',(value['id'],rid));ledger.save()
            pending_notes += 1
            if automatic_historical:
                historical_started += 1

    for record in units(ledger,'pipeline-notes',db.execute('SELECT DISTINCT task_id FROM pipeline_sources WHERE task_id IS NOT NULL').fetchall(),
                        key=lambda r:r['task_id']):
        note = tasks.get(record['task_id'])
        if note['state'] not in {'approved','handoff-ready','verified'} or note['data'].get('review',{}).get('verdict')!='PASS':
            continue
        if not note_closure.eligible(note):
            continue
        # A PASS on a needs-decision/hold result confirms a question, not a finished note.
        if note['data'].get('result',{}).get('outcome')!='completed':
            continue
        if note['request']['inputs'].get('pipeline', {}).get('scenario') != 3:
            continue
        link = {**note['request']['inputs']['pipeline'], 'note_task_id':note['id'],
                'note_result_hash':digest(note['data']['result'])}
        try:
            source_gate(note['request'],tasks,root)
        except ValueError:
            continue
        science = {'result':note['request']['inputs']['result'], 'note':note['data']['result']}
        downstream = {**link,'scenario':4}
        if config['kinds'].get('mathesis') and not revisions.reason(note):
            enqueue(request('mathesis',downstream,note['owner'],{
                **science, 'pipeline':downstream,
                'owner_ids':note['request']['inputs'].get('owner_ids',[]),
                'instructions':'Adapt only the exact approved scientific note and evidence to the existing Mathesis contract. Exclude private correspondence. Prepare a handoff; submission consent, author confirmation and platform receipts remain separate.'},revision))
        if not config['kinds'].get('firstcontact'):
            continue
        related = [r for r in tasks.all() if r['kind']=='firstcontact' and
                   r['request']['inputs'].get('pipeline', {}).get('note_task_id')==note['id']]
        grouped = next((r for r in related if r['request']['inputs']['pipeline'].get('recipient_group')), None)
        legacy = [r for r in related if not r['request']['inputs']['pipeline'].get('recipient_group')]
        # Held results remain review history. Started or delivered legacy work
        # must be reconciled before creating another potentially overlapping letter.
        if grouped is None and any(r['state'] not in {'held', 'superseded'} and
               (r['state']!='queued' or not unstarted(r)) for r in legacy):
            continue
        if grouped is None:
            recipients, excluded = outreach_recipients(note, contacts, root)
            if not recipients:
                continue
            outreach = {**link, 'scenario':2, 'recipient_group':'posing-authors'}
            addresses = [p['email'] for p in recipients]
            value = request('firstcontact',outreach,recipients[0]['contact_id'],{
                **science, 'automation_contract':1, 'pipeline':outreach, 'recipient_emails':addresses,
                'to':addresses[:1], 'cc':addresses[1:],
                'owner_ids':[p['contact_id'] for p in recipients], 'excluded_recipients':excluded,
                'instructions':'Research the posing authors and source problem. Draft one useful letter per approved note, with the first reachable author in To and the other reachable authors in Cc, preserving author_contacts order. Report excluded authors and their existing threads. The coordinator supplies fresh mailbox_history before admission; use it to continue existing relationships as replies, preserve owner drafts and hold uncertain sends. No sending authority is granted here.'},revision)
            queued_legacy = [r for r in legacy if r['state']=='queued' and unstarted(r)]
            if not budget and not queued_legacy:
                continue
            # Group creation and legacy supersession are one durable transition;
            # no queued component may dispatch between these operations.
            with tasks.tx():
                tasks._add(value)
                for old in queued_legacy:
                    tasks._update(old['id'],old['revision'],'superseded',{
                        **old['data'], 'successor':value['id'],
                        'reason':'Superseded by one letter to the posing authors of the approved note'})
            # Replacement reuses an already queued slot; it must finish before
            # dispatch even when unrelated derivations consumed this tick's budget.
            if not queued_legacy:
                budget-=1
            ledger.save()
        else:
            # Also catch a late replay of an old queued per-contact request.
            for old in legacy:
                if old['state']=='queued' and unstarted(old):
                    tasks.update(old['id'],old['revision'],'superseded',{
                        **old['data'], 'successor':grouped['id'],
                        'reason':'Superseded by the existing grouped first-contact task'})
                    ledger.save()

    if settings.get('daily') and config['kinds'].get('daily'):
        # Calendar windows derive from upstream history, never discovery time or
        # successful email/submission counts. The Agent must establish cutoffs.
        first = datetime.fromisoformat(json.loads(initialized['payload'])['date']).date()
        day = first
        while day < today and budget:
            identity = {'scenario':5,'day':day.isoformat()}
            if not db.execute('SELECT 1 FROM tasks WHERE id=?',('daily-'+digest(identity)[:32],)).fetchone():
                enqueue(request('daily',identity,'research-daily',{
                    'pipeline':{**identity,'upstream_repository':'the-omega-institute/trureturing',
                                'upstream_commit':sources['source_commit']},
                    'covered_days':[day.isoformat()], 'timezone':'Asia/Singapore',
                    'instructions':'Audit this complete calendar day from pinned upstream history. Establish exact baseline/cutoff commits and existing publication/date reservations. HOLD if evidence is missing. Count upstream results, not email sends or Mathesis receipts. Prepare text and image for independent review; do not post.'},revision))
            day += timedelta(days=1)


def status(tasks, root, config=None, *, compact=False):
    """Compact status for the operator; no credential or mailbox access."""
    baseline = tasks.db.execute("SELECT payload FROM pipeline_state WHERE key='baseline'").fetchone()
    sources = tasks.db.execute('SELECT result_id,baseline,task_id FROM pipeline_sources')
    counts=tasks.db.execute("""SELECT
        COALESCE(SUM(kind='shortnote' AND json_extract(request,'$.inputs.pipeline.scenario')=3
                     AND COALESCE(json_extract(request,'$.inputs.note_round'),'')!='draft'),0),
        COALESCE(SUM(json_extract(request,'$.inputs.pipeline.scenario') IN (2,4,5)),0)
        FROM tasks WHERE state!='superseded'""").fetchone()
    contacts = read(root,'contacts')['contacts']
    waits = [];wait_counts={};observed=waiting=0
    def wait(value):
        if compact:
            reason=value['reason'];wait_counts[reason]=wait_counts.get(reason,0)+1
        else:waits.append(value)
    for source in sources:
        observed+=1;waiting+=not source['baseline'] and not source['task_id']
        if not source['task_id']:
            continue
        note = tasks.db.execute('SELECT state FROM tasks WHERE id=?',(source['task_id'],)).fetchone()
        require(note is not None,'Unknown task')
        if note['state'] not in {'approved','handoff-ready','verified'}:
            continue
        matches=[p for p in contacts if source['result_id'] in p.get('problem_ids',[])]
        if not matches:
            wait({'result_id':source['result_id'],'reason':'No verified author contact registered for this result'})
        for person in matches:
            gate=preflight(person['email'],person['name'],root)
            if not gate['allowed']:
                wait({'result_id':source['result_id'],'contact_id':gate['contact_id'],'reason':', '.join(gate['reasons'])})
    return {'baseline':json.loads(baseline['payload']) if baseline else None,
            'observed_results':observed,
            'new_results_waiting':waiting,
            'source_notes':counts[0],
            'derived_tasks':counts[1],
            **({'outreach_wait_count':sum(wait_counts.values()),'outreach_waits_by_reason':wait_counts}
               if compact else {'outreach_waits':waits}),
            'mailbox_feedback': ('microsoft-graph configured; ' +
                ('production paused' if config.get('paused') else 'per-message live receipts required')
                if config and config.get('mailbox_adapter') == 'microsoft-graph'
                else 'unconfigured; requires actual sent/thread receipts')}
