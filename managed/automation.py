"""Continue reviewed work in the existing single-writer ledger.

Classification is a proposal until Claude reviews the exact result. Questions
and independent-paper work stay separate from the permission to send mail.
"""
import copy
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import subprocess

from .location import settings, source_head
from .policy import policy_text
from .tick import units
from .queue import digest, require
from .mail_domains import home_domains, home_recipient_override
from .tasks import (ACTIVE, matches_workflow, owner_draft_rebindable, parked,
                    review_current, submission_ready_unreviewed, validate_request)
from .thread_staging import answer_continuation, effective_inputs, inbound_tasks
from . import note_closure, note_revisions

MAIL = {'firstcontact', 'reply'}
HOME_RECIPIENT_MIGRATION_LIMIT = 32
MAILBOX_NO_TASK_LIMIT = 100
REVIEWED = {'approved', 'awaiting-mailbox', 'owner-draft', 'handoff-ready',
            'verified', 'no-reply', 'round-complete', 'submission-ready'}
def submission_rounds():
    return tuple(json.loads(policy_text('managed/submission/workflow.json'))['rounds'])


def submission_round(inputs):
    name = inputs.get('submission_round', 'improve')
    require(name in submission_rounds(), 'Unknown submission round')
    return name


def next_submission_round(inputs):
    rounds = submission_rounds()
    index = rounds.index(submission_round(inputs)) + 1
    return rounds[index] if index < len(rounds) else None


def reviewed_shortnote(row):
    """Whether a shortnote PASS is eligible for downstream derivation."""
    return note_closure.eligible(row)


def _plain_message_body(message):
    """Return the preserved plain-text body when Graph supplied one."""
    body = message.get('body') if isinstance(message, dict) else None
    if isinstance(body, dict):
        content = body.get('content')
        content_type = str(body.get('contentType', '')).casefold()
        if isinstance(content, str) and content_type in {'text', 'text/plain', 'plain'}:
            return content
        for key in ('plain_text', 'plainText', 'text'):
            if isinstance(body.get(key), str):
                return body[key]
    for key in ('plain_text', 'plainText', 'text'):
        if isinstance(message, dict) and isinstance(message.get(key), str):
            return message[key]
    return None


def _incoming_details(row):
    """Extract the latest inbound identity, From name and plain text from task inputs."""
    inputs = effective_inputs(row)
    observation = inputs.get('mailbox_observation') or {}
    thread = inputs.get('thread') or []
    wanted = observation.get('message_id') or inputs.get('latest_inbound_message_id')
    latest = next((m for m in thread if m.get('internetMessageId') == wanted), None)
    incoming = [m for m in thread if not m.get('isDraft')]
    if latest is None and incoming:
        latest = max(incoming, key=lambda m: str(m.get('receivedDateTime', '')))
    def address(value):
        if isinstance(value, str): return value
        if isinstance(value, dict):
            return value.get('emailAddress', {}).get('address') or value.get('address')
        return None
    message_id = observation.get('message_id') or (latest or {}).get('internetMessageId')
    sender = observation.get('email') or address((latest or {}).get('from'))
    name = None
    header = (latest or {}).get('from') or {}
    # Do not borrow a name from an unrelated fallback message in the thread.
    if (latest and isinstance(header, dict) and latest.get('internetMessageId') == message_id and
            str(address(header) or '').strip().casefold() == str(sender or '').strip().casefold()):
        name = (header.get('emailAddress') or header).get('name')
    at = observation.get('at') or (latest or {}).get('receivedDateTime')
    evidence_ref = observation.get('evidence_ref')
    if not evidence_ref and latest and latest.get('id'):
        evidence_ref = 'graph:inbox:' + str(latest['id'])
    fallback_text = next((inputs.get(key) for key in
                          ('incoming_text', 'latest_incoming_plain_text', 'latest_incoming_text', 'plain_text')
                          if isinstance(inputs.get(key), str)), None)
    return {'email': sender, 'name': name, 'at': at, 'message_id': message_id,
            'evidence_ref': evidence_ref, 'subject': (latest or {}).get('subject') or observation.get('subject'),
            'text': _plain_message_body(latest) if latest else fallback_text}


def _recorded_message(root, detail):
    """Match a registry event by either durable inbound identity."""
    from . import registry
    needles = [detail.get('message_id'), detail.get('evidence_ref')]
    needles = [str(value) for value in needles if isinstance(value, str) and value]
    if not needles:
        return False
    for person in registry.people(root):
        for row in person['records']:
            for event in row.get('hub', {}).get('events', []):
                evidence = event.get('evidence', '')
                if isinstance(evidence, (dict, list)):
                    evidence = json.dumps(evidence, ensure_ascii=False, sort_keys=True)
                if any(needle in str(evidence) for needle in needles):
                    return True
    return False


def validate_routing(result, row):
    routing = result.get('routing')
    if routing is None:
        require(not row.get('request', {}).get('inputs', {}).get('automation_contract'),
                'This task requires explicit mail/decision/submission routing')
        return
    require(isinstance(routing, dict) and
            {'mail', 'questions', 'independent_submission'} <= set(routing) <=
            {'mail', 'questions', 'independent_submission', 'registry_proposal'},
            'Invalid result routing')
    proposal = routing.get('registry_proposal')
    hint = row.get('request', {}).get('inputs', {}).get('registry_proposal_hint')
    if hint is not None:
        require(isinstance(proposal, dict),
                'A linked alias needs routing.registry_proposal for operator registration')
        require({'email', 'contact_id', 'evidence_ref'} <= set(proposal) <=
                {'email', 'name', 'contact_id', 'evidence_ref', 'reason'} and
                all(isinstance(proposal[k], str) and proposal[k].strip()
                    for k in ['email', 'contact_id', 'evidence_ref']),
                'Invalid registry proposal')
        require(proposal['email'].casefold() == hint['email'].casefold() and
                proposal['contact_id'] == hint['contact_id'] and
                proposal['evidence_ref'] == hint['evidence_ref'],
                'Registry proposal does not preserve the linked alias evidence')
    elif proposal is not None:
        require(isinstance(proposal, dict) and
                {'email', 'contact_id', 'evidence_ref'} <= set(proposal) <=
                {'email', 'name', 'contact_id', 'evidence_ref', 'reason'} and
                all(isinstance(proposal[k], str) and proposal[k].strip()
                    for k in ['email', 'contact_id', 'evidence_ref']),
                'Invalid registry proposal')
    require(routing['mail'] in {'auto', 'owner', 'none'}, 'Unknown mail route')
    require(row['kind'] in MAIL or routing['mail'] == 'none', 'A scientific task cannot send mail')
    questions = routing['questions']
    require(isinstance(questions, list), 'Questions must be a list')
    for question in questions:
        require(isinstance(question, dict) and set(question) == {'id', 'question', 'source_quote', 'evidence_ref'}
                and all(isinstance(v, str) and v.strip() for v in question.values()),
                'A user question needs its actual quote and source')
    require(len({q['id'] for q in questions}) == len(questions), 'Duplicate user question')
    if questions:
        require(routing['mail'] != 'auto', 'Unresolved user decisions cannot auto-send')
        require(result['outcome'] == 'needs-decision', 'User questions need the reviewed decision-request outcome')
    if result['outcome'] == 'needs-decision':
        require(questions and result['packet'] is None, 'A decision request is not a finished letter')
    if routing['mail'] == 'none':
        require(result['packet'] is None, 'No-mail result must not contain a sendable packet')
    paper = routing['independent_submission']
    if paper is not None:
        require(isinstance(paper, dict) and set(paper) ==
                {'id', 'title', 'authors', 'target_journal', 'source_quote', 'message_id', 'artifact_names'},
                'Independent-paper proposal needs identity, authors, source and selected inputs')
        require(all(isinstance(paper[k], str) and paper[k].strip()
                    for k in ['id', 'title', 'target_journal', 'source_quote', 'message_id']),
                'Missing independent-paper provenance')
        require(re.fullmatch(r'[a-z0-9]+(?:-[a-z0-9]+)*',paper['id']), 'Invalid independent-paper ID')
        require(isinstance(paper['authors'], list) and paper['authors']
                and len(set(paper['authors'])) == len(paper['authors'])
                and set(paper['authors']) <= set(settings('identity')['team_authors']),
                'Joint and conditional coauthorship belong to the collaboration track')
        names = paper['artifact_names']
        require(isinstance(names, list) and names and len(names) == len(set(names))
                and set(names) <= {a['name'] for a in result['artifacts']},
                'Select actual preserved manuscript inputs')
        require(row['kind'] == 'reply', 'Independent classification starts from correspondence')


def mail_routing_override(row, suffixes):
    if row['kind'] not in MAIL:
        return None
    data = row['data']
    packet = data.get('packet_override') or (data.get('result') or {}).get('packet')
    return home_recipient_override(packet, suffixes)


def approved_route(row, routing_override=None):
    result = row['data']['result']
    route = result.get('routing', {})
    if route.get('questions'):
        return 'awaiting-user'
    if result.get('outcome') == 'no-reply' or (row['kind'] in MAIL and route.get('mail') == 'none'):
        return 'no-reply'
    if row['kind'] in MAIL:
        # Thread decision history does not own later letters. Route each letter
        # from its reviewed content; an academic answer may use the automatic
        # queue, while owner-routed decisions remain the owner's draft.
        owner = route.get('mail') == 'owner' or routing_override is not None
        return 'owner-draft' if owner else 'awaiting-mailbox'
    if row['kind'] == 'submission':
        return 'submission-ready' if submission_round(effective_inputs(row)) == 'finalize' else 'round-complete'
    return 'handoff-ready'


def migrate_home_recipients(tasks, config):
    """Move pending home-institution mail to the owner path before delivery."""
    suffixes = home_domains(config)
    if not suffixes:
        return
    candidates = tasks.db.execute("""
        SELECT id FROM tasks WHERE state='awaiting-mailbox'
        AND kind IN ('reply', 'firstcontact') ORDER BY rowid
        """).fetchall()
    moved = 0
    for (task_id,) in candidates:
        row = tasks.get(task_id)
        override = mail_routing_override(row, suffixes)
        if override is None or row['data'].get('review', {}).get('verdict') != 'PASS':
            continue
        tasks.update(task_id, row['revision'], 'owner-draft', {**row['data'], 'routing_override': override})
        moved += 1
        if moved == HOME_RECIPIENT_MIGRATION_LIMIT:
            break


def parent_gate(request, tasks):
    seen=set()
    while (link := request['inputs'].get('automation_parent')) is not None:
        require(request['id'] not in seen, 'Continuation parent cycle')
        seen.add(request['id'])
        require(isinstance(link, dict) and set(link) == {'task_id', 'result_hash', 'purpose'},
                'Invalid continuation parent')
        parent = tasks.get(link['task_id'])
        require(digest(parent['data'].get('result')) == link['result_hash'], 'Continuation source version changed')
        if link['purpose'] == note_revisions.PURPOSE:
            note_revisions.parent_gate(request, tasks, parent)
            return  # A sent reply is durable evidence, including after a workflow change.
        if link['purpose'] == 'independent-submission':
            require(request['kind'] == 'submission' and parent['state'] in REVIEWED
                    and parent['data'].get('review', {}).get('verdict') == 'PASS',
                    'Independent classification needs the exact Claude PASS')
        elif link['purpose'] == 'submission-round':
            require(request['kind'] == 'submission' and parent['kind'] == 'submission'
                    and ((parent['state'] == 'round-complete' and parent['data'].get('auto_advanced') is True
                          and parent['data']['result'].get('outcome') == 'completed')
                         or (parent['state'] in REVIEWED
                             and parent['data'].get('review', {}).get('verdict') == 'PASS')),
                    'Submission round needs an auto-completed parent or the exact Claude PASS')
            require(submission_round(request['inputs']) == next_submission_round(effective_inputs(parent)),
                    'Submission continuation must execute exactly the next round')
        elif link['purpose'] in {'shortnote-uplift', 'shortnote-closure', 'shortnote-value-uplift'}:
            require(request['kind'] == 'shortnote' and parent['kind'] == 'shortnote'
                    and parent['state'] == 'round-complete',
                    'Shortnote uplift needs the completed previous round')
            reason = note_closure.retry_reason({**parent, 'state': 'awaiting-review'})
            require(link['purpose'] == {'draft': 'shortnote-uplift', 'missing-closure': 'shortnote-closure',
                                        'trivial': 'shortnote-value-uplift'}.get(reason),
                    'Shortnote uplift parent does not match the continuation')
            require(request['inputs'].get('note_round') == 'uplift',
                    'Shortnote continuation must execute the uplift round')
            count = parent['request']['inputs'].get('note_value_uplifts', 0) + int(reason == 'trivial')
            require(request['inputs'].get('note_value_uplifts', 0) == count,
                    'Shortnote continuation must preserve the value uplift count')
            require(request['inputs'].get('previous_result') == parent['data'].get('result'),
                    'Shortnote uplift must carry the exact previous result')
        else:
            require(link['purpose'] in {'revision', 'user-answer', 'guard-continuation'}
                    and parent['data'].get('continuation_id') == request['id']
                    and parent['state'] == 'revising', 'Revision is not the registered continuation')
        request=parent['request']


def owner_gate(request, root):
    from .registry import people
    path=Path(root)/'tables/decisions.json'
    if not path.exists():return
    controlled={n.casefold() for n in json.loads(path.read_text()).get('owner_controlled_threads',[])}
    owners={request['owner'],*request['inputs'].get('owner_ids',[])}
    for person in people(root):
        if owners & {person['id'],*(r['id'] for r in person['records'])}:
            require(person['name'].casefold() not in controlled and
                    not any(r.get('owner_controlled') or r.get('hub',{}).get('owner_controlled') for r in person['records']),
                    'Owner-controlled line: register incoming facts only; preserve user work')


def owner_participant_gate(addresses, root):
    """An owner-controlled person anywhere in the thread controls the whole thread.

    The task owner alone is not enough: a co-recipient may write the latest
    message."""
    from .registry import people
    path=Path(root)/'tables/decisions.json'
    if not path.exists() or not addresses:return
    controlled={n.casefold() for n in json.loads(path.read_text()).get('owner_controlled_threads',[])}
    for person in people(root):
        if addresses & set(person['emails']):
            require(person['name'].casefold() not in controlled and
                    not any(r.get('owner_controlled') or r.get('hub',{}).get('owner_controlled') for r in person['records']),
                    'Owner-controlled participant in thread: register incoming facts only; preserve user work')

def paper_summary(row):
    """Read both the migrated compact index and the PR #162 recovery schema."""
    journal = row['target_journal']
    return {'id':row['id'], 'title':row['title'], 'authors':row['authors'],
            'version_date':row.get('version_date') or row['version']['date'],
            'target_journal':journal if isinstance(journal,str) else journal['name'],
            'workspace':row.get('workspace') or row['local_package'],
            'stage':row['stage'],
            'blocker':row.get('blocker', '') or '; '.join(b['reason'] for b in row.get('blocking_items', []))}


def paper_identity(paper):
    return digest({key:paper[key] for key in ('id', 'authors', 'workspace')})


def paper_gate(request, root):
    link = request['inputs'].get('submission_source')
    if link is None:
        return
    require(request['kind'] == 'submission' and isinstance(link,dict)
            and set(link) == {'paper_id','identity'}, 'Invalid submission board source')
    rows = json.loads((Path(root)/'tables/submissions.json').read_text())['papers']
    paper = next((paper_summary(r) for r in rows if r['id'] == link['paper_id']), None)
    require(paper is not None and paper['stage'] != 'submitted', 'Paper removed or already submitted')
    require(link['identity'] in {digest(paper), paper_identity(paper)},
            'Submission board changed; select the current package')


def stage_submission(paper_id, files, root, releases, folder, submission_round='improve'):
    """Explicit local bridge: upload selected inputs, never mirror a mail tree."""
    import hashlib
    from .pipeline import request
    from .control import stage
    require(submission_round in submission_rounds(), 'Unknown submission round')
    root = Path(root)
    rows = json.loads((root/'tables/submissions.json').read_text())['papers']
    source = next((r for r in rows if r['id'] == paper_id), None)
    require(source is not None, 'Unknown independent paper')
    paper = paper_summary(source)
    require(paper['stage'] != 'submitted', 'Paper is already recorded as submitted')
    # Resolve the two roots through the common Git directory, including when this
    # command is tested in a disposable workflow-maintenance worktree.
    common = subprocess.check_output(['git','rev-parse','--path-format=absolute','--git-common-dir'],
                                     cwd=root,text=True).strip()
    canonical = Path(common).parent.resolve()
    workspace = (canonical/paper['workspace']).resolve()
    mail = (canonical.parent/'mail').resolve()
    require(workspace.is_dir() and mail in workspace.parents and
            'outreach-maintenance' not in workspace.relative_to(mail).parts,
            'Finish the person-owned package migration before staging it')
    require(0 < len(files) <= 100, 'Select 1..100 task inputs explicitly')
    selected = []
    for file in files:
        path = (workspace/file).resolve()
        require(workspace in path.parents and path.is_file()
                and not any(p.startswith('.') for p in path.relative_to(workspace).parts),
                'Select a scientific file inside this person-owned package')
        raw = path.read_bytes()
        require(0 < len(raw) <= 30*1024*1024, 'Selected input exceeds the task asset limit')
        selected.append((str(path.relative_to(workspace)),path.name,raw,hashlib.sha256(raw).hexdigest()))
    require(len({p[0] for p in selected}) == len(selected), 'Duplicate selected input')
    identity = digest({'paper':paper,'files':[(p[0],p[3]) for p in selected]})
    tag = 'outreach-input-'+identity[:32]
    artifacts = []
    for number,(relative,name,raw,sha) in enumerate(selected):
        suffix=Path(name).suffix
        name = 'input-'+str(number)+'-'+sha[:20]+(suffix if re.fullmatch(r'\.[A-Za-z0-9]{1,10}',suffix) else '')
        path = Path(folder)/name;path.write_bytes(raw)
        releases.preserve_task(path,tag)
        # Read back input bytes too. An upload receipt is not proof of preservation.
        path.unlink()
        releases.run('release','download',tag,'--repo',releases.repo,'--pattern',name,'--dir',str(folder))
        require(path.read_bytes() == raw, 'Selected input readback differs')
        path.unlink()
        artifacts.append({'release_tag':tag,'name':name,'relative_path':relative,'sha256':sha,'bytes':len(raw)})
    revision = source_head()
    owner = 'workspace-'+workspace.relative_to(mail).parts[0]
    contacts = json.loads((root/'tables/contacts.json').read_text())['contacts']
    workspace_name = workspace.relative_to(mail).parts[0]
    owners = sorted({r['id'] for r in contacts if '../mail/'+workspace_name in
                     r.get('hub',{}).get('workspace_paths',[])})
    link = {'paper_id':paper_id,'identity':paper_identity(paper)}
    value = request('submission',{'source':link,'selected_inputs':identity,'submission_round':submission_round},owners[0] if owners else owner,
                    {'automation_contract':1,'paper':paper,'submission_source':link,
                     'submission_round':submission_round,
                     'selected_inputs':artifacts,'owner_ids':owners,
                     'instructions':'Execute the '+submission_round+' round. Reuse effective existing reviews and exact manuscript versions. '
                     'Audit literature, proof, journal policy and the saved Oracle reports, repair and '
                     're-review gaps, compile and inspect the exact final PDF and source archive. '
                     'Do not invent author facts or override publication holds. Preparation only.'},revision)
    return stage(value,'request',releases,folder)


def reconcile(ledger, config, root, revision=None):
    """A bounded continuation pass; never retry an uncertain external operation."""
    settings = config.get('automation', {})
    if config['paused'] or not settings.get('enabled', False):
        return
    from .pipeline import request
    tasks = ledger.tasks
    revision = revision or source_head()
    budget = settings.get('max_new_tasks_per_tick', 4)
    limit = settings.get('max_revisions', 3)
    uplift_limit = settings.get('shortnote_extra_uplift_rounds', note_closure.UPLIFT_LIMIT)
    require(type(budget) is int and 1 <= budget <= 16, 'Bound continuation creation to 1..16')
    require(type(limit) is int and 1 <= limit <= 10, 'Bound review revisions to 1..10')
    require(type(uplift_limit) is int and uplift_limit >= 0, 'Invalid short-note extra uplift round limit')
    revisions = note_revisions.Revisions(tasks)
    # Stage sent revisions while the automation slice is still available.
    if config.get('kinds', {}).get('shortnote', True):
        budget = note_revisions.stage(ledger, revisions, revision, budget)
    close_user_questions(ledger, config, root)
    note_revisions.hold_stale(ledger, revisions)
    for row in units(ledger,'automation',tasks.all()):
        if not budget:
            break
        if (row['kind'] == 'submission' and row['data'].get('result', {}).get('outcome') == 'completed'
                and ((row['state'] == 'awaiting-review' and not row['data'].get('submission_review_requested'))
                     or (row['state'] in REVIEWED and row['data'].get('review', {}).get('verdict') == 'PASS'
                         and next_submission_round(effective_inputs(row)) is not None))):
            # Accepted results already passed validation and artifact readback.
            # Codex + Oracle continue preparation; Claude reviews only on request.
            result = row['data']['result']
            parent_inputs = effective_inputs(row)
            next_round = next_submission_round(parent_inputs)
            if next_round is None:
                tasks.update(row['id'], row['revision'], 'submission-ready',
                             {**row['data'], 'awaiting_operator_review':True})
                ledger.save()
                continue
            link = {'task_id':row['id'], 'result_hash':digest(result), 'purpose':'submission-round'}
            inputs = {key:copy.deepcopy(parent_inputs[key]) for key in
                      ('automation_contract', 'paper', 'submission_source', 'owner_ids') if key in parent_inputs}
            paper = inputs.get('paper') or {}
            if 'submission_source' in inputs and {'id', 'authors', 'workspace'} <= set(paper):
                inputs['submission_source'] = {'paper_id':inputs['submission_source']['paper_id'],
                                               'identity':paper_identity(paper)}
            inputs.update(automation_parent=link, submission_round=next_round,
                          previous_result=result, review_feedback=None, auto_advanced=True,
                          selected_inputs=[a for a in result['artifacts'] if a['role'] in
                                           {'manuscript-pdf', 'manuscript-tex', 'source-archive', 'evidence'}],
                          revision_number=0, review_revisions=0,
                          instructions='Execute the '+next_round+' round with the exact previous manuscript and evidence. '
                          'Use Codex + Oracle to choose and check the direction without waiting for operator review. '
                          'Stop at its deliverable and propose the compact board update. Never submit to a journal.')
            new = request('submission', link, row['owner'], inputs, revision)
            with tasks.tx():
                # Older reviewed successors and workflow re-pins retain this ID;
                # keep their immutable inputs and never derive a second child.
                if not tasks.db.execute('SELECT 1 FROM tasks WHERE id=?', (new['id'],)).fetchone():
                    tasks._add(new); budget -= 1
                if row['state'] != 'round-complete' or not row['data'].get('auto_advanced'):
                    tasks._update(row['id'], row['revision'], 'round-complete', {**row['data'], 'auto_advanced':True})
            ledger.save()
            continue
        if reason := note_closure.retry_reason(row):
            inputs = copy.deepcopy(effective_inputs(row))
            count = inputs.get('note_value_uplifts', 0)
            if reason == 'trivial' and (count >= uplift_limit or note_revisions.Revisions.is_merge(row)):
                tasks.update(row['id'], row['revision'], 'held', {
                    **row['data'], 'hold_kind': 'archive-only', 'reason': note_closure.ARCHIVE_REASON})
                ledger.save()
                continue
            if not config.get('kinds', {}).get('shortnote', True):
                continue
            purpose, instructions = {
                'draft': ('shortnote-uplift', 'Execute the initial uplift with the exact draft and its saved artifacts.'),
                'missing-closure': ('shortnote-closure', 'Complete closure for the exact previous note. Reuse its prior-work evidence.'),
                'trivial': ('shortnote-value-uplift', 'Make the contribution non-trivial. Prove a stronger statement, '
                            'general case, sharp bound or structural explanation. If that is impossible, explain '
                            'the obstruction in the value assessment and return the complete preserved note as trivial.'),
            }[reason]
            if note_revisions.Revisions.is_merge(row):
                instructions += ' ' + note_revisions.INSTRUCTIONS
            link = {'task_id': row['id'], 'result_hash': digest(row['data']['result']),
                    'purpose': purpose}
            existing = next((candidate for candidate in tasks.all()
                             if candidate['request']['inputs'].get('automation_parent') == link), None)
            inputs.update(note_round='uplift', automation_parent=link,
                          previous_result=row['data']['result'],
                          note_value_uplifts=count + int(reason == 'trivial'),
                          revision_number=0, review_revisions=0, instructions=instructions)
            new = request('shortnote', {'parent': link, 'note_round': 'uplift'}, row['owner'], inputs, revision)
            validate_request(new)
            with tasks.tx():
                if existing is None:
                    tasks._add(new)
                    budget -= 1
                target = existing['id'] if existing else new['id']
                tasks._update(row['id'], row['revision'], 'round-complete',
                              {**row['data'], 'continuation_id': target})
                tasks.db.execute('UPDATE pipeline_sources SET task_id=? WHERE task_id=?',
                                 (target, row['id']))
            ledger.save()
            continue
        data = row['data']
        if guard_continuable(row):
            inputs = copy.deepcopy(effective_inputs(row))
            checkpoint_artifacts = data['result']['artifacts']
            if checkpoint_artifacts:
                resume = ('Restore inputs.checkpoint_artifacts into this worktree. Use the selected '
                          'api-github-pat connection and the GitHub Releases API to download each asset '
                          'by release_tag and name. Verify its exact bytes and SHA-256 before using it. '
                          'Reuse the verified files and finish the remaining work described in previous_result. '
                          'Rebuild any missing or unverifiable files from the primary source.')
            else:
                resume = ('The parent has no preserved checkpoint artifacts. Redo the missing work '
                          'from the primary source in this worktree. Use previous_result as context. '
                          'Missing files from the parent are not a reason to return hold.')
            inputs.update(revision_number=inputs.get('revision_number', 0) + 1,
                          guard_continuations=inputs.get('guard_continuations', 0) + 1,
                          checkpoint_artifacts=checkpoint_artifacts,
                          previous_result=data['result'],
                          review_feedback={
                              'verdict':'CONTINUE', 'reviewer':'runtime-guard',
                              'reason':f'The runtime guard stopped the previous turn. {resume} Return the final task JSON.'})
            if (inputs.get('user_decision_ever') or
                    (data['result'].get('packet') or {}).get('user_decision_ever') or
                    data['result'].get('routing', {}).get('mail') == 'owner'):
                inputs['user_decision_ever'] = True
            link = {'task_id':row['id'], 'result_hash':digest(data['result']),
                    'purpose':'guard-continuation'}
            inputs['automation_parent'] = link
            new = request(row['kind'], {'parent':link, 'guard':True}, row['owner'], inputs, revision)
            tasks.add(new)
            tasks.update(row['id'], row['revision'], 'revising', {**data, 'continuation_id':new['id']})
            tasks.db.execute('UPDATE pipeline_sources SET task_id=? WHERE task_id=?', (new['id'], row['id']))
            ledger.save(); budget -= 1
        elif row['state'] in {'bounced', 'decision-recorded'} and not row['data'].get('continuation_id'):
            inputs = copy.deepcopy(effective_inputs(row))
            attempt = inputs.get('revision_number', 0) + 1
            bounces = inputs.get('review_revisions',0) + int(row['state']=='bounced')
            if bounces > limit:
                continue  # Exposed in work(), with the full review reason.
            data = row['data']
            inputs.update(revision_number=attempt, review_revisions=bounces, previous_result=data['result'],
                          review_feedback=data.get('review'))
            if (inputs.get('user_decision_ever') or
                    (data['result'].get('packet') or {}).get('user_decision_ever') or
                    data['result'].get('routing',{}).get('mail')=='owner'):
                inputs['user_decision_ever']=True
            if row['state'] == 'decision-recorded':
                inputs.update(user_decision_ever=True,
                              user_answers=[*inputs.get('user_answers', []), data['user_decision']])
            purpose = 'user-answer' if row['state'] == 'decision-recorded' else 'revision'
            link = {'task_id':row['id'], 'result_hash':digest(data['result']), 'purpose':purpose}
            inputs['automation_parent'] = link
            new = request(row['kind'], {'parent':link, 'review':data.get('review'),
                                       'decision':data.get('user_decision')}, row['owner'], inputs, revision)
            # Register the parent and the child atomically. A replay cannot
            # create a second continuation if a snapshot write was interrupted.
            tasks.add(new)
            tasks.update(row['id'], row['revision'], 'revising', {**data, 'continuation_id':new['id']})
            tasks.db.execute('UPDATE pipeline_sources SET task_id=? WHERE task_id=?', (new['id'], row['id']))
            ledger.save(); budget -= 1
        elif (row['state'] in REVIEWED and row['data'].get('review', {}).get('verdict') == 'PASS'
              and (row['kind'] != 'shortnote' or reviewed_shortnote(row))):
            result = row['data']['result']
            if row['kind'] == 'submission':
                continue
            paper = result.get('routing', {}).get('independent_submission')
            if not paper:
                continue
            link = {'task_id':row['id'], 'result_hash':digest(result), 'purpose':'independent-submission'}
            artifacts = [a for a in result['artifacts'] if a['name'] in paper['artifact_names']]
            inputs = {'automation_contract':1, 'automation_parent':link, 'paper':paper, 'submission_round':'improve',
                      'selected_inputs':artifacts, 'owner_ids':row['request']['inputs'].get('owner_ids', []),
                      'instructions':'Execute the improve round for the confirmed independent submission: audit existing manuscript/reviews first, '
                      'complete literature/proof and Oracle journal-editor review, fix and re-review, '
                      'deliver final source/PDF/package. Propose the compact board update. Never submit to a journal.'}
            new = request('submission', link, row['owner'], inputs, revision)
            if not tasks.db.execute('SELECT 1 FROM tasks WHERE id=?', (new['id'],)).fetchone():
                tasks.add(new); ledger.save(); budget -= 1


QUESTION_CLOSE_LIMIT = 32


def _question_context(row):
    """Only an identified, dated latest inbound can establish the cutoff."""
    from .mailbox import instant
    inputs = effective_inputs(row)
    packet = row['data'].get('packet_override') or (row['data'].get('result') or {}).get('packet') or {}
    observation = inputs.get('mailbox_observation') or {}
    identity = inputs.get('latest_inbound_message_id') or packet.get('latest_inbound_message_id')
    identity = identity or observation.get('message_id')
    conversation = inputs.get('thread_id') or packet.get('thread_id')
    if not identity or observation.get('message_id', identity) != identity:
        return None
    dates = []
    try:
        for message in inputs.get('thread', []):
            if message.get('internetMessageId') != identity or message.get('isDraft'):
                continue
            if conversation and message.get('conversationId') != conversation:
                return None
            conversation = conversation or message.get('conversationId')
            dates.append(instant(message['receivedDateTime']))
        if observation.get('message_id') == identity and observation.get('at'):
            dates.append(instant(observation['at']))
    except (KeyError, ValueError, TypeError, AttributeError):
        return None
    return (conversation, identity, max(dates)) if dates else None


def _subject_key(subject):
    text = ' '.join(str(subject or '').split()).casefold()
    while True:
        stripped = re.sub(r'^(re|fw|fwd|aw|sv)\s*:\s*', '', text)
        if stripped == text:
            return text
        text = stripped


def _question_sent_evidence(row, person, account):
    """Use coordinator Sent intake or registry-validated Graph Sent receipts only."""
    from .mailbox import instant
    context = _question_context(row)
    if context is None or person is None:
        return None
    conversation, identity, inbound_at = context
    inputs = effective_inputs(row)
    packet = row['data'].get('packet_override') or (row['data'].get('result') or {}).get('packet') or {}
    subjects = {_subject_key(m.get('subject')) for m in inputs.get('thread', [])
                if m.get('internetMessageId') == identity}
    subjects |= {_subject_key(packet.get('subject'))}
    subjects.discard('')
    matches = []
    for record in person['records']:
        for event in record.get('hub', {}).get('events', []):
            if event.get('kind') != 'sent':
                continue
            try:
                evidence = event.get('evidence')
                evidence = json.loads(evidence) if isinstance(evidence, str) else evidence
                if not isinstance(evidence, dict) or evidence.get('recipient') not in person['emails']:
                    continue
                message_id = evidence.get('internetMessageId')
                if evidence.get('source') != 'sent-events':
                    ref = evidence.get('evidence_ref', '')
                    if not (isinstance(ref, str) and ref.startswith('graph:sent:') and
                            evidence.get('remote_id') and evidence.get('content_hash')):
                        continue
                    message_id = ref.removeprefix('graph:sent:')
                if not isinstance(message_id, str) or not message_id.strip():
                    continue
                # Older verified intake/receipts predate account/From metadata.
                if 'account' in evidence and (not evidence.get('account') or
                        evidence.get('from') != evidence['account'] or
                        (account and evidence['account'] != account)):
                    continue
                sent_at = instant(event['at'])
                if sent_at <= inbound_at:
                    continue
                sent_conversation = evidence.get('conversationId')
                if conversation and sent_conversation and conversation != sent_conversation:
                    continue
                match = 'conversation' if conversation and sent_conversation else 'owner-registry-fallback'
                # Without conversation identity, only the same subject line can
                # show that this Sent answered this thread (not another letter).
                if match != 'conversation' and _subject_key(str(event.get('summary', '')).split(' — ')[0]) not in subjects:
                    continue
                matches.append((sent_at, {'source': 'registry-sent-event', 'event_id': event.get('id'),
                    'message_id': message_id, 'at': event['at'], 'recipient': evidence['recipient'],
                    'conversation_id': sent_conversation, 'task_conversation_id': conversation,
                    'latest_inbound_message_id': identity, 'match': match}))
            except (KeyError, ValueError, TypeError, AttributeError):
                continue
    # Prefer exact conversation evidence even if an older legacy event exists.
    matches.sort(key=lambda item: (item[1]['match'] != 'conversation', item[0], item[1]['message_id']))
    return matches[0][1] if matches else None


def close_user_questions(ledger, config, root):
    """Coordinator-only, replay-safe closure; rotate at most 32 questions per tick."""
    from .registry import people
    tasks = ledger.tasks
    db = tasks.db
    saved = db.execute("SELECT payload FROM pipeline_state WHERE key='user-question-cursor'").fetchone()
    cursor = json.loads(saved[0]) if saved else ''
    query = "SELECT id FROM tasks WHERE state='awaiting-user' AND kind='reply' AND id>? ORDER BY id LIMIT ?"
    ids = [r[0] for r in db.execute(query, (cursor, QUESTION_CLOSE_LIMIT))]
    if not ids and cursor:
        ids = [r[0] for r in db.execute(query, ('', QUESTION_CLOSE_LIMIT))]
    if not ids:
        return
    persons = people(root)
    by_owner = {owner: p for p in persons for owner in
                [*[r['id'] for r in p['records']], *p['emails']]}
    account = str(config.get('mailbox', {}).get('account') or '').strip().casefold()
    for row in units(ledger, 'user-question-closure', [tasks.get(i) for i in ids], preserve_order=True):
        evidence = _question_sent_evidence(row, by_owner.get(row['owner']), account)
        with tasks.tx():
            if evidence:
                data = {**row['data'], 'user_question_closed': {'reason': 'answered-by-mail', 'evidence': evidence}}
                tasks._update(row['id'], row['revision'], 'answered-by-mail', data)
            db.execute('INSERT OR REPLACE INTO pipeline_state VALUES(?,?)',
                       ('user-question-cursor', json.dumps(row['id'])))
        ledger.save()


# Long manuscript/research tasks hit the runtime guard repeatedly; every second
# stop reached the operator for a manual "continue" BOUNCE (five on 2026-11-18).
GUARD_CONTINUATIONS = 3


def guard_continuable(row):
    """A hold returned after the runtime guard's steer is continued automatically
    up to GUARD_CONTINUATIONS times."""
    data = row['data']
    guard = data.get('runtime_guard', {})
    return (row['state'] == 'held' and row['kind'] in
            {'firstcontact', 'reply', 'shortnote', 'mathesis', 'submission', 'research'}
            and data.get('result', {}).get('outcome') == 'hold'
            and bool(data.get('response_id'))
            and any(control.get('kind') == 'steer' and control.get('status') == 'admitted'
                    and (guard.get('response_id') == data['response_id'] or
                         # After a steer the Agent often returns its hold in a
                         # later response; the admitted receipt still binds the
                         # steer to this task's Agent (2026-11-16, two holds).
                         (bool(data.get('agent_id')) and
                          (((control.get('receipt') or {}).get('control') or {}).get('receipt') or {})
                          .get('agent_id') == data['agent_id']))
                    for control in guard.get('controls', []))
            and not data.get('continuation_id')
            and effective_inputs(row).get('guard_continuations', 0) < GUARD_CONTINUATIONS)


def work(tasks, root, config=None):
    """The user-started Claude loop consumes one complete actionable inventory."""
    from .daily_local import status as daily_status
    from .author_contacts import pending as pending_authors
    from .sandbox_pool import summary as pool_summary
    from .pipeline import note_coverage_overlap
    from .prior_work_issues import inventory_view as prior_work_view
    from .control import (dispatch_order, execution_failure_class, failure_exit, workflow,
                          runtime_failure, RUNTIME_RETRIES, saved_http_failure,
                          given_up_digests, ingest_rejections)
    from .correspondence import linked_contacts
    from .lanes import lane, summary as lane_summary
    from .mailbox import recent_no_task
    from .mailbox_triage import inventory as mailbox_inventory
    from .draft_status import inventory as draft_inventory, recent_owner_edits
    from .registry import reply_mail_thread_contacts, bounce_updates
    try:
        daily = daily_status()
    except Exception as error:
        daily = {'status': 'needs-attention', 'error': type(error).__name__,
                 'next_action': 'python3 -m managed.daily_local status'}
    all_rows = tasks.all()
    revisions = note_revisions.Revisions(tasks, all_rows)
    rows = [r for r in all_rows if r['state'] != 'superseded']
    artifact_collections = []
    for row in rows:
        data = row['data']
        collection = data.get('artifact_collection')
        progress = data.get('artifact_progress', {})
        if row['state'] != 'running' or (collection is None and not progress):
            continue
        if collection is None:
            # Snapshots from before collection totals were recorded.
            try:
                artifacts = json.loads(data['completed_response']['text'])['artifacts']
            except (ValueError, KeyError, TypeError):
                continue
            if not isinstance(artifacts, list):
                continue
            collection = {'total': len(artifacts)}
        statuses = list(progress.values())
        artifact_collections.append({'task_id': row['id'], **collection,
                                     'verified': statuses.count('verified'),
                                     'preserved': statuses.count('preserved')})
    config = config or {}
    correspondence = linked_contacts(all_rows, root, config)
    queued_notes = []
    if correspondence:
        saved = tasks.db.execute("SELECT payload FROM pipeline_state WHERE key='tick_deferred'").fetchone()
        deferred = json.loads(saved[0]).get('units', {}).get('advance', []) if saved else []
        reply_queue = [r for r in rows if r['state'] == 'queued' and lane(r, config, correspondence) in ('reply', 'correspondence')]
        ordered = dispatch_order(reply_queue, config, correspondence, {key: i for i, key in enumerate(deferred)})
        queued_notes = [{'task_id': r['id'], 'kind': r['kind'], 'position': position,
                         'contacts': correspondence[r['id']]}
                        for position, r in enumerate(ordered, 1) if r['id'] in correspondence]
    from . import pre_reviewed, publication, recheck
    prior_work_needed = []
    for row in rows:
        if (row['kind'] not in {'shortnote', 'mathesis'} or not publication.reviewed(row)
                or revisions.reason(row)):
            continue
        delivery = row['data'].get('mathesis_delivery', {})
        cached = row['data'].get('mathesis_prior_work', {})
        current = cached.get('result_hash') == digest(row['data']['result'])
        if not current or cached.get('check') is None:
            prior_work_needed.append({'task_id': row['id'],
                'phase': (cached.get('phase') if current else 'needs-evidence-scan'),
                'verdict': (cached.get('check') or {}).get('verdict') if current else None,
                **({'delivery': delivery.get('phase')} if delivery.get('phase') else {})})
    round_successors = {link['task_id'] for r in all_rows
                        if (link := r['request']['inputs'].get('automation_parent', {})).get('purpose') == 'submission-round'}
    submission_rounds = {}
    for row in rows:
        if row['kind'] == 'submission' and (paper_id := effective_inputs(row).get('paper', {}).get('id')):
            submission_rounds[paper_id] = {'paper_id':paper_id, 'round':submission_round(effective_inputs(row)),
                                           'task_id':row['id'], 'state':row['state']}
    failed_unrecovered = []
    for row in rows:
        if row['state'] != 'failed':
            continue
        data = row['data']
        recovery = data.get('recovery', {})
        error_class = recovery.get('error_class', execution_failure_class(data.get('response_error')))
        if error_class=='unknown' and saved_http_failure(data):
            error_class='rate-limit'
            recovery={**recovery,'suggested_exit':'retry-later'}
        if runtime_failure({'error':data.get('operation_error')}):
            error_class='runtime-preparation'
            recovery={'attempts':data.get('runtime_retries',0),
                      'suggested_exit':'retry-later' if data.get('runtime_retries',0)<RUNTIME_RETRIES else 'rewrite-task'}
        failed_unrecovered.append({'task_id':row['id'], 'kind':row['kind'], 'owner':row['owner'],
            'error_class':error_class,
            'attempts':recovery.get('attempts', data.get('rate_limit_retries', 0)+data.get('transport_retries', 0)),
            'suggested_exit':recovery.get('suggested_exit', failure_exit(error_class))})
    # Guard-stopped holds are continued by the next tick without a review;
    # listing them would only invite a redundant BOUNCE (2026-11-14).
    reviews = [item for item in tasks.reviews()
               if not ((row := tasks.get(item['binding']['task_id'])) and guard_continuable(row))]
    guard_without_checkpoint = []
    for row in rows:
        inputs = row['request']['inputs']
        parent = inputs.get('automation_parent') or {}
        if parent.get('purpose') != 'guard-continuation':
            continue
        # Older continuations carried the parent result but no checkpoint field.
        if not inputs.get('checkpoint_artifacts', inputs.get('previous_result', {}).get('artifacts')):
            guard_without_checkpoint.append({'task_id':row['id'], 'parent_id':parent['task_id']})
    owner_drafts = []
    draft_observations = draft_inventory(tasks)
    owner_draft_rebind = []
    mail_rebind = []
    current_hashes = {kind:workflow(kind) for kind in {r['kind'] for r in rows
                      if r['state'] in {'owner-draft','awaiting-mailbox'}}}
    for row in rows:
        if tasks.mail_rebindable(row,current_hashes.get(row['kind'])):
            mail_rebind.append({'task_id':row['id'], 'owner':row['owner'], 'kind':row['kind'],
                                'reason':'Mail workflow changed; re-review the current version',
                                'binding':tasks.review_binding(row),
                                'rebind_workflow_hash':current_hashes[row['kind']]})
        if row['state']!='owner-draft':continue
        data=row['data'];current_hash=current_hashes[row['kind']]
        current=review_current(row,current_hash)
        if owner_draft_rebindable(row,current_hash):
            owner_draft_rebind.append({'binding':tasks.review_binding(row),
                                      'rebind_workflow_hash':current_hash,
                                      'request':row['request'], 'result':data['result']})
        blocker=None
        if data.get('stale_draft_conflict'):blocker='stale-user-draft'
        elif not matches_workflow(row,current_hash):blocker='workflow-changed'
        elif not current:blocker='review-stale'
        elif data.get('draft_intent_expired') and data.get('last_error'):
            blocker='draft-intent-reconciliation-failed'
        elif not data.get('draft_receipt') and not data.get('sent_receipt'):
            blocker='no-draft-receipt'
            if data.get('last_error'):
                # Older errors retain only prose; never put mail/error text in
                # the machine reason or invent an exception type for those rows.
                error_type=re.match(r'^([A-Za-z_][A-Za-z0-9_]*(?:Error|Exception))(?::|$)',str(data['last_error']))
                blocker='save-failed'+(':'+error_type[1] if error_type else '')
        receipt = data.get('draft_receipt') or {}
        draft_observation = draft_observations.get(row['id'], {})
        if draft_observation.get('remote_id') != receipt.get('remote_id'):
            draft_observation = {}
        owner_drafts.append({'task_id':row['id'], 'kind':row['kind'], 'state':row['state'],
                             'request':row['request'], 'result':data.get('result'),
                             'mailbox_observation':effective_inputs(row).get('mailbox_observation'),
                             'packet':data.get('packet_override') or data['result']['packet'],
                             'draft_receipt':data.get('draft_receipt'),
                             'draft_status':draft_observation.get('draft_status'),
                             'draft_checked_at':draft_observation.get('draft_checked_at'),
                             'draft_stale':draft_observation.get('draft_stale', True),
                             **{key:draft_observation[key] for key in
                                ('draft_saved_at', 'later_sent_at', 'lastModifiedDateTime', 'owner_edited',
                                 'has_placeholder', 'attachment_count', 'event_date', 'draft_body_checked')
                                if key in draft_observation},
                             'draft_intent_expired':data.get('draft_intent_expired', []),
                             'draft_revisions':data.get('draft_revisions', []),
                             'recipient_exception':data.get('recipient_exception'),
                             'routing_override':data.get('routing_override'),
                             'sent_receipt':data.get('sent_receipt'),
                             'review_current':current, 'blocker':blocker,
                             'stale_draft_conflict':data.get('stale_draft_conflict'),
                             'last_error':data.get('last_error')})
    questions = []
    open_questions = {task_id: opened for task_id, opened, _ in tasks.owner_questions(correspondence_only=False)}
    for task_id, opened_at in open_questions.items():
        row = tasks.get(task_id)
        result = row['data']['result']
        inputs = effective_inputs(row)
        questions.append({'task_id': task_id, 'owner': row['owner'], 'opened_at': opened_at,
                          'result_hash': digest(result), 'request': {**row['request'], 'inputs': inputs},
                          'result': result, 'mailbox_observation': inputs.get('mailbox_observation'),
                          'questions': (result.get('routing') or {}).get('questions', [])})
    follow = [{'id':r['id'], 'kind':r['kind'], 'state':r['state'],
               'reason':r['data'].get('last_error') or r['data'].get('reason') or
                        r['data'].get('review', {}).get('reason') or r['data'].get('result',{}).get('report',''),
               **({'owner_question_blockers':r['data']['owner_question_blockers']}
                  if r['data'].get('owner_question_blockers') else {}),
               **({'delivery_conflicts':r['data']['delivery_conflicts']}
                  if r['data'].get('delivery_conflicts') else {}),
               'continuation_id':r['data'].get('continuation_id'),
               'request':r['request'],'result':r['data'].get('result'),
               'handles':{k:r['data'][k] for k in ['agent_id','operation_id','response_id','create_key','command_key'] if k in r['data']},
               'response_text':r['data'].get('response_text')}
              for r in rows if r['state'] in ACTIVE | {'held', 'failed', 'bounced', 'awaiting-mailbox'}]
    papers_path = Path(root)/'tables/submissions.json'
    papers = json.loads(papers_path.read_text())['papers'] if papers_path.exists() else []
    active_papers = {r['request']['inputs'].get('paper', {}).get('id') for r in rows if r['kind'] == 'submission'}
    mailbox=tasks.db.execute("SELECT payload FROM pipeline_state WHERE key='mailbox'").fetchone()
    mailbox=json.loads(mailbox[0]) if mailbox else {}
    no_task = mailbox.get('no_task', {})
    if no_task:
        no_task = recent_no_task(no_task, datetime.fromtimestamp(tasks.clock(), timezone.utc))
    errors=tasks.db.execute("SELECT payload FROM pipeline_state WHERE key='connector_errors'").fetchone()
    oracle_capacity=tasks.db.execute("SELECT payload FROM pipeline_state WHERE key='oracle_capacity'").fetchone()
    oracle_capacity=json.loads(oracle_capacity[0]) if oracle_capacity else None
    credit_block=tasks.db.execute("SELECT payload FROM pipeline_state WHERE key='credit_block'").fetchone()
    if credit_block:
        credit_block={**json.loads(credit_block[0]),
                      'waiting':sum(r['state']=='credit-wait' or
                                    (r['state']=='queued' and bool(r['data'].get('credit_waiting'))) for r in rows)}
    legacy_migration=tasks.db.execute("SELECT payload FROM pipeline_state WHERE key='mathesis-legacy-migration'").fetchone()
    legacy_migration=json.loads(legacy_migration[0]) if legacy_migration else {
        'pending': 0, 'withdrawn': 0, 'redelivered': 0, 'skipped': 0,
        'skipped_reasons': {}}
    author_contacts_needed = []
    author_contacts_blocked = []
    for row in rows:
        if (row['kind']=='shortnote' and reviewed_shortnote(row) and row['state'] in REVIEWED
                and row['data'].get('review',{}).get('verdict')=='PASS'
                and row['data'].get('result',{}).get('outcome')=='completed'):
            if 'author_contacts' not in row['data']['result']:
                author_contacts_needed.append({'task_id':row['id'],
                    'reason':'Legacy PASS note has no author_contacts; obtain a new evidence-bearing result and review'})
                continue
            try:
                pending = pending_authors(row, root)
                if pending:
                    blocked = pending.pop('blocked', [])
                    for item in blocked:
                        author_contacts_blocked.append({'task_id': row['id'], **item})
                    if pending.get('proposals'):
                        author_contacts_needed.append(pending)
            except ValueError as error:
                author_contacts_needed.append({'task_id':row['id'], 'reason':str(error)})
    publication_deliveries = [{'task_id': r['id'], **r['data']['publication_loop']}
                              for r in rows if r['data'].get('publication_loop')]
    publication_failures = []
    for delivery in publication_deliveries:
        if delivery.get('phase') != 'pending' or not delivery.get('last_error'):
            continue
        task_id = delivery['task_id']
        retry_at = delivery.get('next_retry_at')
        if retry_at is None:
            binding = [delivery.get('result_hash'), delivery.get('decision_hash')]
            retry_at = recheck.next_at(tasks, 'publication', task_id, binding)
        publication_failures.append({
            'task_id': task_id, 'note_id': delivery.get('note_id'), 'revision': delivery.get('revision'),
            'step': delivery.get('step'), 'diagnostic': delivery.get('diagnostic') or delivery['last_error'],
            'consecutive_failures': delivery.get('consecutive_failures'), 'next_retry_at': retry_at,
        })
    return {'lanes': lane_summary(rows, config, correspondence),
            'mailbox_account': mailbox.get('account') or config.get('mailbox', {}).get('account'),
            'correspondence_note_queue': queued_notes,
            'shortnote_archive_only': [{'task_id':r['id'], 'owner':r['owner'],
                                       'reason':r['data']['reason'],
                                       'value_assessment':r['data']['result']['value_assessment']}
                                      for r in rows if r['kind'] == 'shortnote'
                                      and r['data'].get('hold_kind') == 'archive-only'],
            'formalization_requests': [{'task_id':r['id'], **item} for r in rows
                                       for item in r['data'].get('formalization_requests', [])],
            **prior_work_view(tasks),
            'affairs_tasks':[{'id':r['id'], 'kind':r['kind'], 'owner':r['owner'],
                             'state':r['state'], 'updated':r['updated'], 'request':r['request'],
                             'reason':r['data'].get('last_error') or r['data'].get('reason'),
                             'result':r['data'].get('result'), 'review':r['data'].get('review'),
                             **({'reviewer_edit':{key:r['data']['reviewer_edit'][key] for key in
                                 ('from_result_hash','body_sha256_before','body_sha256_after')}}
                                if 'reviewer_edit' in r['data'] else {}),
                             'packet_override':r['data'].get('packet_override'),
                             'routing_override':r['data'].get('routing_override'),
                             'draft_receipt':r['data'].get('draft_receipt'),
                             'sent_receipt':r['data'].get('sent_receipt'),
                             **({'cleanup_operator_check':r['data']['cleanup']['operator_check']}
                                if isinstance(r['data'].get('cleanup'),dict) and
                                r['data']['cleanup'].get('operator_check') else {})}
                            for r in rows],
            'user_questions':questions,
            'mailbox_retry_exhausted': [
                {'task_id': r['id'], 'owner': r['owner'], 'request': r['request'],
                 'mailbox_observation': effective_inputs(r).get('mailbox_observation'),
                 'reason': r['data'].get('reason'), **r['data']['mailbox_retry']}
                for r in rows if r['state'] == 'held'
                and r['data'].get('mailbox_retry', {}).get('status') == 'exhausted'],
            'mailbox_no_task':list(no_task.values())[:MAILBOX_NO_TASK_LIMIT],
            'user_questions_closed':[{'task_id':r['id'], 'owner':r['owner'], **r['data']['user_question_closed']}
                                     for r in all_rows if r['id'] not in open_questions and r['data'].get('user_question_closed')],
            'superseded_answer_continuations':[
                {'task_id': r['id'], 'owner': r['owner'], 'kind': r['kind'],
                 'superseded_by': r['data']['superseded_by'], 'reason': r['data'].get('reason'),
                 'answers': [*(r['request']['inputs'].get('user_answers') or []),
                             *r['data'].get('carried_user_answers', [])]}
                for r in all_rows if r['state'] == 'superseded' and r['data'].get('superseded_by')
                and (answer_continuation(r) or r['data'].get('carried_user_answers'))],
            'reviews':reviews,
            'artifact_collections_in_progress':artifact_collections,
            'guard_continuations_without_checkpoint':guard_without_checkpoint,
            'delivery_terminal':[{'task_id':r['id'], 'owner':r['owner'],
                                  'blocker':r['data']['hold_reason']}
                                 for r in rows if r['state']=='held'
                                 and r['data'].get('hold_kind')=='delivery-terminal'],
            'failed_unrecovered':failed_unrecovered,
            'parked_holds':[{'task_id':r['id'],'reason':str(r['data']['review'].get('reason',''))[:200]} for r in rows if parked(r)],
            'daily_local':daily,
            'author_contacts_needed':author_contacts_needed,
            'author_contacts_blocked':author_contacts_blocked,
            'short_note_deliveries':[{'task_id':r['id'], 'binding':r['data']['review']['binding'],
                                      'request':r['request'], 'result':r['data']['result']}
                                     for r in rows if reviewed_shortnote(r) and r['state'] in REVIEWED
                                     and r['data'].get('review',{}).get('verdict')=='PASS'
                                     and not revisions.reason(r)
                                     and r['data'].get('result',{}).get('outcome')=='completed'],
            'diverged_short_notes':revisions.work(),
            'note_coverage_overlap':note_coverage_overlap(all_rows, tasks),
            'mathesis_needs_prior_work_check': prior_work_needed,
            'mathesis_legacy_migration': legacy_migration,
            'mathesis_deliveries_needed':[{'task_id':r['id'], 'binding':r['data']['review']['binding'],
                                           'request':r['request'], 'result':r['data']['result']}
                                          for r in rows if r['kind']=='mathesis' and r['state'] in REVIEWED
                                          and r['data'].get('review',{}).get('verdict')=='PASS'
                                          and r['data'].get('result',{}).get('outcome')=='completed'
                                          and not revisions.reason(r)
                                          and not r['data'].get('mathesis_delivery')],
            'mathesis_deliveries':[{'task_id':r['id'], 'state':r['state'], **r['data']['mathesis_delivery']}
                                    for r in rows if r['kind']=='mathesis' and r['data'].get('mathesis_delivery')],
            'publication_deliveries':publication_deliveries,
            'publication_failures':publication_failures,
            'connector_errors':json.loads(errors[0]) if errors else [],
            'ingest_rejected_count':len(given_up_digests(ingest_rejections(tasks))),
            'sandbox_pool':pool_summary(tasks,all_rows),
            'failed_sandboxes':json.loads((tasks.db.execute(
                "SELECT payload FROM pipeline_state WHERE key='sandbox_pool_cleanup'").fetchone() or ['[]'])[0]),
            'sandbox_pool_operator_checks':json.loads((tasks.db.execute(
                "SELECT payload FROM pipeline_state WHERE key='sandbox_pool_operator_checks'").fetchone() or ['[]'])[0]),
            'task_cleanup_operator_checks':[{'task_id':r['id'],
                'check':r['data']['cleanup']['operator_check']}
                for r in all_rows if isinstance(r['data'].get('cleanup'),dict) and
                r['data']['cleanup'].get('operator_check')],
            'sandbox_requeued_tasks':[{'task_id':r['id'],'state':r['state'],
                'sandbox_failures':r['data'].get('sandbox_failures',0),
                'history':r['data']['sandbox_failure_history']} for r in all_rows
                if r['data'].get('sandbox_failure_history')],
            'oracle_capacity':oracle_capacity,
            'credit_block':credit_block,
            'owner_drafts':owner_drafts,
            'owner_edits':recent_owner_edits(tasks, rows, root),
            'owner_draft_rebind':owner_draft_rebind,
            'mail_rebind':mail_rebind,
            'owner_draft_exceptions':[{'task_id':r['task_id'], 'kind':r['kind'],
                                       'owner':r['request']['owner'], 'blocker':r['blocker'],
                                       **(r['stale_draft_conflict'] or {})}
                                      for r in owner_drafts if r['blocker']],
            'submission_inputs_needed':[p for p in papers if p['stage'] != 'submitted' and p['id'] not in active_papers],
            'submission_rounds':list(submission_rounds.values()),
            'submission_ready_unreviewed':[
                {'paper_id':paper.get('id'), 'title':paper.get('title'),
                 'target_journal':(paper.get('target_journal', {}).get('name')
                                   if isinstance(paper.get('target_journal'), dict) else paper.get('target_journal')),
                 'task_id':r['id']}
                for r in rows if submission_ready_unreviewed(r)
                for paper in [next((p for p in papers if p['id'] == effective_inputs(r).get('paper', {}).get('id')),
                                   effective_inputs(r).get('paper', {}))]],
            'submission_ready':[{'task_id':r['id'], 'result':r['data']['result']}
                                for r in rows if r['state'] == 'submission-ready' and r['id'] not in round_successors
                                and r['request']['inputs'].get('submission_round', 'finalize') == 'finalize'],
            **mailbox_inventory(mailbox,all_rows,root,config),
            'duplicate_inbound_tasks':[{'message_id':identity, 'tasks':group}
                                       for identity,group in inbound_tasks(r for r in rows if r['kind']=='reply').items()
                                       if len(group)>1],
            'mailbox_covered_rows':mailbox.get('covered',[]),
            'mailbox_bounces':mailbox.get('bounces',[]),
            'mailbox_bounce_count':mailbox.get('bounce_count',len(mailbox.get('bounces',[]))),
            'mailbox_machine_notices':mailbox.get('machine_notice_count',0),
            'mailbox_machine_notice_rows':mailbox.get('machine_notices',[])[-20:],
            'registry_updates':[
                (lambda detail: {
                    'task_id': r['id'],
                    'incoming': detail,
                    'incoming_text': detail.get('text'),
                    'subject': detail.get('subject'),
                    'message_id': detail.get('message_id'),
                    'evidence_ref': detail.get('evidence_ref'),
                    'sent_receipt': r['data'].get('sent_receipt'),
                    'classification': r['data'].get('result',{}).get('routing'),
                    'registry_patch': r['data'].get('result',{}).get('registry_patch'),
                    'mail_thread_contacts': reply_mail_thread_contacts(r),
                    'recorded': _recorded_message(root, detail),
                })(_incoming_details(r))
                for r in rows if r['request']['inputs'].get('mailbox_observation')
                or r.get('data',{}).get('thread_inputs',{}).get('mailbox_observation')
                or r['data'].get('sent_receipt')] + bounce_updates(mailbox.get('bounces',[]), root),
            'follow_up':follow}
