"""Transactional work ledger. Only the serialized coordinator writes this DB."""
from contextlib import contextmanager
import copy
import hashlib
import hmac
import json
import re
import sqlite3
import time

from .queue import canonical, digest, require, validate_packet
from . import note_closure

KINDS={'firstcontact','reply','research','shortnote','daily','mathesis','submission'}
INDEPENDENT_REVIEWERS=frozenset({'local-claude-loop','hosted-cma-review'})
NOTE_KINDS={'shortnote','mathesis'}
CORRESPONDENCE_KINDS={'reply','research'}
ACTIVE={'provisioning','admitting','running','admission-unknown','provision-unknown','credit-wait'}
STATES=ACTIVE|{'queued','awaiting-review','approved','bounced','held','failed',
              'awaiting-mailbox','owner-draft','owner-discarded','handoff-ready','verified',
              'awaiting-user','answered-by-mail','question-closed','decision-recorded','revising','no-reply','round-complete','submission-ready','superseded'}
WORKFLOW_CHANGED='Workflow changed; stage a new request revision'
WORKFLOW_PIN_MISMATCH='Workflow hash does not match the pinned source revision'
POLICY_IDENTITY_MISMATCH='Policy identity mismatch'
EVENT_COMPACTION='compact-task-events-v1'


def event_summary(data):
    """History is an audit trail, not another copy of the current task payload.

    The only history reader, review_binding(), needs state/revision and the
    exact standalone review marker. Never emit that marker for an ordinary
    update carrying a review forward: a reviewer HOLD starts a new binding,
    whereas cleanup does not. Full results/packets remain in tasks.data and
    signed reviews/decisions remain in control_events.
    """
    if set(data) in ({'review'}, {'decision'}) and isinstance(next(iter(data.values())),str):
        return data
    summary={}
    for key in ('reason','last_error','operation_id','agent_id','response_id',
                'create_key','command_key','successor','continuation_id'):
        if isinstance(data.get(key),str):summary[key]=data[key][:300]
    for key in ('repair_attempts','transport_retries','capacity_retries','credit_retries'):
        if type(data.get(key)) is int:summary[key]=data[key]
    review=data.get('review')
    if isinstance(review,dict) and review.get('verdict') in {'PASS','BOUNCE','HOLD'}:
        summary['verdict']=review['verdict']
    # Retain summaries on interrupted/replayed migrations too.
    elif data.get('verdict') in ('PASS','BOUNCE','HOLD'):summary['verdict']=data['verdict']
    return summary


def validate_review_rebind(envelope):
    if 'rebind_workflow_hash' not in envelope:
        return
    require(envelope.get('verdict') == 'PASS', 'Only PASS may rebind a workflow')
    value = envelope['rebind_workflow_hash']
    require(isinstance(value, str) and len(value) == 64 and
            all(c in '0123456789abcdef' for c in value), 'Invalid workflow rebind digest')


def validate_review_edit(envelope, kind=None):
    if 'edited_body' not in envelope:return
    require(envelope.get('verdict')=='PASS','Only PASS may include edited_body')
    require(kind is None or kind in {'reply','firstcontact'},'edited_body is only allowed for reply and firstcontact')
    require(isinstance(envelope['edited_body'],str) and envelope['edited_body'].strip(),
            'edited_body must be a nonempty string')


def matches_workflow(row, current_hash):
    review = row['data'].get('review') or {}
    hashes = {row['request']['workflow_hash']}
    if review.get('verdict') == 'PASS':hashes.add(review.get('rebind_workflow_hash'))
    migrated = row['data'].get('workflow_binding') or {}
    return (current_hash in hashes or
            (migrated.get('workflow_hash') == current_hash and migrated.get('legacy_hash') in hashes))


def review_current(row, current_hash):
    """An accepted PASS must still bind both the exact result and its workflow."""
    review = row['data'].get('review') or {}
    binding = review.get('binding') or {}
    return (review.get('verdict') == 'PASS' and binding.get('task_id') == row['id']
            and binding.get('workflow_hash') == row['request']['workflow_hash']
            and binding.get('result_hash') == digest(row['data'].get('result'))
            and matches_workflow(row, current_hash))


def owner_draft_rebindable(row, current_hash):
    """Only an unchanged, unsent PASS may be re-reviewed for a new workflow."""
    return (row['state'] == 'owner-draft' and not row['data'].get('sent_receipt')
            and not row['data'].get('stale_draft_conflict')
            and review_current(row, row['request']['workflow_hash'])
            and not matches_workflow(row, current_hash))


def unstarted(row):
    # Even an uncertain creation/admission intent must retain its original keys.
    data = row['data']
    handles = {'agent_id','operation_id','response_id','create_body','create_key',
               'command_key','owned_sandbox_id','response_text','result','review'}
    # A restart caches the previous prompt before creating its next Agent.
    if row['kind'] not in CORRESPONDENCE_KINDS or not data.get('execution_restart'):
        handles.add('prompt')
    return not handles & data.keys()


def policy_mismatch_hold(row):
    """Recognize a preflight hold with no scientific or delivery output."""
    if row['state'] != 'held':
        return False
    data = row['data']
    if any(key in data for key in ('review', 'delivery_attempt', 'sent_receipt', 'draft_receipt')):
        return False
    result = data.get('result')
    if unstarted(row):
        reason = data.get('reason', '')
    elif (isinstance(result, dict) and result.get('outcome') == 'hold'
          and result.get('packet') is None and result.get('artifacts') == []):
        reason = result.get('report', '')
    else:
        return False
    if not isinstance(reason, str):
        return False
    label = re.split(r'[:.]', reason, maxsplit=1)[0]
    return label.strip().replace('-', ' ').casefold() == POLICY_IDENTITY_MISMATCH.casefold()


def agent_hold(row):
    return (row['state']=='held' and isinstance(row['data'].get('result'),dict)
            and row['data']['result'].get('outcome')=='hold')


def parked(row):
    """An agent hold the reviewer already answered with HOLD for this exact result."""
    review=row['data'].get('review')
    return (agent_hold(row) and isinstance(review,dict) and review.get('verdict')=='HOLD'
            and review.get('binding',{}).get('result_hash')==digest(row['data']['result']))


def reviewable(row):
    # Drafts and incomplete or trivial final rounds go to automatic uplift.
    if agent_hold(row):
        # A reviewer HOLD on this exact result parks it; a new result is reviewable again.
        return not parked(row)
    if row['state'] != 'awaiting-review':
        return False
    if row['kind'] == 'submission' and row['data'].get('result', {}).get('outcome') == 'completed':
        return bool(row['data'].get('submission_review_requested'))
    if row['kind'] != 'shortnote':
        return True
    return (row['request']['inputs'].get('note_round') != 'draft'
            and note_closure.retry_reason(row) is None)


def submission_ready_unreviewed(row):
    return (row['kind'] == 'submission' and row['state'] == 'submission-ready'
            and row['request']['inputs'].get('submission_round', 'improve') == 'finalize'
            and row['data'].get('result', {}).get('outcome') == 'completed'
            and row['data'].get('awaiting_operator_review') is True)


def validate_request(request):
    require(isinstance(request,dict) and request.get('schema_version')==1,'Unsupported task request')
    fields = {'schema_version','id','kind','owner','source_revision','inputs','workflow_hash'}
    require(fields <= set(request) <= fields | {'policy_revision'}, 'Unexpected task request fields')
    require(request['kind'] in KINDS,'Unknown task kind')
    for key in ['id','owner','source_revision','workflow_hash']:
        require(isinstance(request[key],str) and bool(request[key].strip()),'Missing '+key)
    require(len(request['id'])<=120 and all(c.isalnum() or c in '-_.' for c in request['id']),'Unsafe task ID')
    require(isinstance(request['inputs'],dict),'Task inputs must be an object')
    if 'supersedes_owner_questions' in request['inputs']:
        supersedes = request['inputs']['supersedes_owner_questions']
        require(request['kind'] == 'reply' and isinstance(supersedes, dict) and supersedes
                and all(isinstance(key, str) and key.strip() and isinstance(reason, str) and reason.strip()
                        for key, reason in supersedes.items()),
                'Question supersession requires reply task IDs and operator reasons')
    if 'reply_origin_task_id' in request['inputs']:
        origin = request['inputs']['reply_origin_task_id']
        require(request['kind'] in NOTE_KINDS and isinstance(origin, str) and origin.strip(),
                'Reply origin needs a reply task ID on shortnote or Mathesis work')
    owners=request['inputs'].get('owner_ids',[])
    require(isinstance(owners,list) and all(isinstance(o,str) and o.strip() for o in owners),'Invalid additional scholar ownership')
    require(len(request['source_revision'])==40 and all(c in '0123456789abcdef' for c in request['source_revision']),'Pin the workflow source commit')
    if 'policy_revision' in request:
        revision = request['policy_revision']
        require(isinstance(revision, str) and len(revision) == 40
                and all(c in '0123456789abcdef' for c in revision), 'Pin the policy commit')
    require(len(request['workflow_hash'])==64 and all(c in '0123456789abcdef' for c in request['workflow_hash']),'Missing workflow digest')
    if request['kind'] == 'shortnote':
        require(request['inputs'].get('note_round') in {None, 'draft', 'uplift'},
                'Unknown shortnote round')
        count = request['inputs'].get('note_value_uplifts', 0)
        require(type(count) is int and count >= 0, 'Invalid short-note uplift count')
    if 'note_sources' in request['inputs']:
        note_closure.validate_sources(request['inputs']['note_sources'])
    canonical(request)


def owners(row,ignore=frozenset()):
    return {row['owner'],*row['request']['inputs'].get('owner_ids',[])}-set(ignore)


def newer_reply(new, old):
    """Compare dated inbound evidence within one correspondence kind."""
    from .mailbox import instant
    from .thread_staging import effective_inputs
    if new['kind'] != old['kind'] or old['kind'] not in CORRESPONDENCE_KINDS:return False
    def target(row):
        data=row['data'];inputs=effective_inputs(row)
        packet=data.get('packet_override') or (data.get('result') or {}).get('packet') or {}
        return {**packet, **inputs}
    latest, previous=target(new), target(old)
    if (not latest.get('thread_id') or latest['thread_id'] != previous.get('thread_id')
            or not latest.get('latest_inbound_message_id') or not previous.get('latest_inbound_message_id')
            or latest['latest_inbound_message_id'] == previous['latest_inbound_message_id']):return False
    def received(inputs):
        identity=inputs['latest_inbound_message_id']
        for message in [*latest.get('thread', []), *inputs.get('thread', [])]:
            if (message.get('internetMessageId') == identity and not message.get('isDraft')
                    and message.get('conversationId') == inputs['thread_id']):
                return instant(message['receivedDateTime'])
        observation=inputs.get('mailbox_observation') or {}
        if observation.get('message_id') == identity:return instant(observation['at'])
        return None
    try:
        at, before=received(latest), received(previous)
        return at is not None and before is not None and at > before
    except (KeyError, TypeError, ValueError, AttributeError):
        return False


def question_thread(row):
    from .thread_staging import effective_inputs
    inputs = effective_inputs(row)
    data = row['data']
    packet = data.get('packet_override') or (data.get('result') or {}).get('packet') or {}
    if conversation := inputs.get('thread_id') or packet.get('thread_id'):
        return ('graph', conversation)
    if record := (inputs.get('thread_ref') or {}).get('thread_id'):
        return ('registry', record)


def _starts_with_rollback(sql):
    """True when the first statement keyword, after whitespace and comments, is ROLLBACK.

    A linear scan: a regular expression with nested repetition can backtrack
    exponentially on crafted comment sequences.
    """
    i, n = 0, len(sql)
    while i < n:
        if sql[i].isspace():
            i += 1
        elif sql.startswith('--', i):
            j = sql.find('\n', i)
            i = n if j < 0 else j + 1
        elif sql.startswith('/*', i):
            j = sql.find('*/', i + 2)
            i = n if j < 0 else j + 2
        else:
            break
    word = sql[i:i + 9]
    return word[:8].upper() == 'ROLLBACK' and not (len(word) > 8 and (word[8].isalnum() or word[8] == '_'))


class _TaskCursor(sqlite3.Cursor):
    def _call(self, operation, sql, *args, **kwargs):
        # TEMP trigger updates are transactional. A rollback can reuse an old
        # generation after another write, so keep a separate Python epoch.
        if _starts_with_rollback(sql):
            self.connection.row_cache_epoch += 1
        try:
            return operation(sql, *args, **kwargs)
        except sqlite3.Error:
            self.connection.row_cache_epoch += 1
            raise

    def execute(self, sql, parameters=()):
        return self._call(super().execute, sql, parameters)

    def executemany(self, sql, parameters):
        return self._call(super().executemany, sql, parameters)

    def executescript(self, sql):
        # Scripts can contain transaction control after their first statement.
        self.connection.row_cache_epoch += 1
        return self._call(super().executescript, sql)


class _TaskConnection(sqlite3.Connection):
    row_cache_epoch = 0

    def cursor(self, factory=_TaskCursor):
        return super().cursor(factory)

    def execute(self, sql, parameters=()):
        return self.cursor().execute(sql, parameters)

    def executemany(self, sql, parameters):
        return self.cursor().executemany(sql, parameters)

    def executescript(self, sql):
        return self.cursor().executescript(sql)

    def rollback(self):
        self.row_cache_epoch += 1
        return super().rollback()

    def commit(self):
        try:
            return super().commit()
        except sqlite3.Error:
            self.row_cache_epoch += 1
            raise

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            self.row_cache_epoch += 1
        try:
            return super().__exit__(exc_type, exc_value, traceback)
        except sqlite3.Error:
            self.row_cache_epoch += 1
            raise


class Tasks:
    def __init__(self,path,clock=time.time):
        self.path=str(path);self.clock=clock
        self.db=sqlite3.connect(self.path,isolation_level=None,timeout=10,factory=_TaskConnection)
        self.db.row_factory=sqlite3.Row
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS tasks(id TEXT PRIMARY KEY, request TEXT NOT NULL,
          request_hash TEXT NOT NULL, owner TEXT NOT NULL, kind TEXT NOT NULL,
          state TEXT NOT NULL, revision INTEGER NOT NULL, data TEXT NOT NULL,
          updated REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS task_events(seq INTEGER PRIMARY KEY AUTOINCREMENT,
          id TEXT NOT NULL, state TEXT NOT NULL, revision INTEGER NOT NULL,
          at REAL NOT NULL, data TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS control_events(id TEXT PRIMARY KEY, payload TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS input_asset_receipts(identity TEXT PRIMARY KEY, generation INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS pipeline_sources(result_id TEXT PRIMARY KEY,
          identity TEXT NOT NULL, payload TEXT NOT NULL, baseline INTEGER NOT NULL,
          task_id TEXT);
        CREATE TABLE IF NOT EXISTS pipeline_state(key TEXT PRIMARY KEY, payload TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS task_migrations(name TEXT PRIMARY KEY, phase TEXT NOT NULL);
        ''')
        # TEMP triggers track task-table writes, including direct SQL. Their
        # generation is transactional; the connection epoch invalidates on
        # rollbacks/failed statements. Neither enters a Release snapshot.
        # data_version below also notices writes through another connection.
        self.db.executescript('''
        CREATE TEMP TABLE task_rows_version(generation INTEGER NOT NULL);
        INSERT INTO task_rows_version VALUES(0);
        CREATE TEMP TRIGGER task_rows_insert AFTER INSERT ON main.tasks BEGIN
          UPDATE task_rows_version SET generation=generation+1; END;
        CREATE TEMP TRIGGER task_rows_update AFTER UPDATE ON main.tasks BEGIN
          UPDATE task_rows_version SET generation=generation+1; END;
        CREATE TEMP TRIGGER task_rows_delete AFTER DELETE ON main.tasks BEGIN
          UPDATE task_rows_version SET generation=generation+1; END;
        ''')
        self.compact_events()

    def compact_events(self):
        """One-time, resumable compaction; VACUUM actually shrinks snapshots.

        Keep a pending marker in the same transaction as the rewritten rows.
        A kill before VACUUM finishes retries it at the next restore; a kill
        before COMMIT rolls back the compaction. Never VACUUM each snapshot.
        """
        marker=self.db.execute('SELECT phase FROM task_migrations WHERE name=?',(EVENT_COMPACTION,)).fetchone()
        if marker and marker[0]=='done':return
        if not marker:
            with self.tx():
                for row in self.db.execute('SELECT seq,data FROM task_events'):
                    compact=canonical(event_summary(json.loads(row['data'])))
                    if compact!=row['data']:
                        self.db.execute('UPDATE task_events SET data=? WHERE seq=?',(compact,row['seq']))
                self.db.execute('INSERT INTO task_migrations VALUES(?,?)',(EVENT_COMPACTION,'vacuum'))
        self.db.execute('VACUUM')
        self.db.execute("UPDATE task_migrations SET phase='done' WHERE name=?",(EVENT_COMPACTION,))

    @contextmanager
    def tx(self):
        self.db.execute('BEGIN IMMEDIATE')
        try:yield;self.db.execute('COMMIT')
        except BaseException:self.db.execute('ROLLBACK');raise

    def close(self):self.db.close()
    def row_version(self):
        return (self.db.row_cache_epoch,
                self.db.execute('SELECT generation FROM temp.task_rows_version').fetchone()[0],
                self.db.execute('PRAGMA data_version').fetchone()[0])
    def get(self,task_id):
        row=self.db.execute('SELECT * FROM tasks WHERE id=?',(task_id,)).fetchone()
        require(row is not None,'Unknown task')
        return self._decode(row)
    @staticmethod
    def _decode(row):
        result=dict(row)
        for key in ['request','data']:result[key]=json.loads(result[key])
        return result
    def all(self):
        return [self._decode(row) for row in self.db.execute('SELECT * FROM tasks ORDER BY rowid')]

    def of_kinds(self,*kinds):
        marks=','.join('?' for _ in kinds)
        return [self._decode(row) for row in self.db.execute(
            f'SELECT * FROM tasks WHERE kind IN ({marks}) ORDER BY rowid',kinds)]

    def counts(self,column='state'):
        require(column in {'state','kind'},'Unknown task count column')
        return dict(self.db.execute(f'SELECT {column},COUNT(*) FROM tasks GROUP BY {column} ORDER BY {column}'))

    def owner_questions(self, *, correspondence_only=True):
        """Reviewed questions awaiting a user decision and their original wait times."""
        return self.db.execute("""
            SELECT id, COALESCE(json_extract(data,'$.owner_question_opened_at'),
                (SELECT MIN(e.at) FROM task_events e WHERE e.id=tasks.id AND e.state='awaiting-user'),
                (SELECT MIN(e.at) FROM task_events e WHERE e.id=tasks.id))
                AS opened_at, json_array_length(data,'$.result.routing.questions') AS question_count
            FROM tasks
            WHERE state='awaiting-user' AND (
                NOT ? OR (kind IN ('reply','research')
                          AND json_array_length(data,'$.result.routing.questions') > 0))
            ORDER BY opened_at NULLS LAST,id
        """, (correspondence_only,)).fetchall()

    def route_no_reply(self, row, data):
        """Apply a reviewed close and any explicit supersessions atomically."""
        thread = question_thread(row)
        supersedes = row['request']['inputs'].get('supersedes_owner_questions', {})
        with self.tx():
            blockers, replacements = [], []
            if row['kind'] == 'reply' and thread is not None:
                for task_id, _, _ in self.owner_questions():
                    old = self.get(task_id)
                    if question_thread(old) != thread or newer_reply(old, row):
                        continue
                    if task_id in supersedes:
                        replacements.append(old)
                    else:
                        blockers.append(task_id)
            if blockers:
                data['owner_question_blockers'] = blockers
                if row['state'] != 'held' or row['data'] != data:
                    self._update(row['id'], row['revision'], 'held', data)
                return False
            for old in replacements:
                evidence = {'superseded_by': row['id'], 'reason': supersedes[old['id']].strip()}
                self._update(old['id'], old['revision'], 'superseded',
                             {**old['data'], 'superseded_by': row['id'],
                              'user_question_closed': {'reason': 'superseded', 'evidence': evidence}})
            data.pop('owner_question_blockers', None)
            self._update(row['id'], row['revision'], 'no-reply', data)
        return True

    def lane_rows(self):
        """Project only lane/lineage fields in SQLite, excluding bulky payloads.

        Keep the shape consumed by correspondence.linked_contacts and lanes;
        neither prompts, thread bodies, results nor execution history cross
        the Python boundary. SQLite's page cache stays bounded as it scans.
        """
        def field(column,path):return f"json_extract({column},'$.{path}')"
        def obj(fields):
            return 'json_object('+','.join(f"'{key}',{value}" for key,value in fields.items())+')'
        def ids(column,path):
            return {key:field(column,f'{path}.{key}') for key in
                    ('result_id','problem_id','id','result_ids','problem_ids')}
        inputs=ids('request','inputs')
        inputs.update({key:field('request','inputs.'+key) for key in ('owner_ids','reply_origin_task_id')})
        inputs['mailbox_observation']=obj({'email':"COALESCE("+field('request','inputs.mailbox_observation.email')+",'')"})
        inputs['pipeline']=obj({**ids('request','inputs.pipeline'),
                                'note_task_id':field('request','inputs.pipeline.note_task_id')})
        inputs['result']=obj(ids('request','inputs.result'))
        inputs['automation_parent']=obj({'task_id':field('request','inputs.automation_parent.task_id')})
        inputs['note_sources']=("json((SELECT json_group_array(json_object('task_id',json_extract(value,'$.task_id'))) "
                                "FROM json_each(request,'$.inputs.note_sources')))")
        delivery={}
        for envelope in ('collection_envelope','envelope'):
            path='mathesis_delivery.'+envelope+'.metadata'
            delivery[envelope]=obj({'metadata':obj({
                'origin':obj(ids('data',path+'.origin')),
                'review':obj({'record':obj(ids('data',path+'.review.record'))})})})
        # problem_ids prefers collection_envelope when present; preserve its
        # absence so the projected envelope has the same fallback semantics.
        for envelope in delivery:
            delivery[envelope]=("CASE WHEN json_extract(data,'$.mathesis_delivery."+envelope+
                                "') IS NOT NULL AND json_extract(data,'$.mathesis_delivery."+envelope+
                                "') != '{}' THEN "+delivery[envelope]+" ELSE NULL END")
        data=obj({'pool_group':field('data','pool_group'),'pool_lane':field('data','pool_lane'),
                  'publication_loop':obj(ids('data','publication_loop')),
                  'mathesis_delivery':obj(delivery)})
        request=obj({'inputs':obj(inputs)})
        return [{**dict(row),'request':json.loads(row['request']),'data':json.loads(row['data'])}
                for row in self.db.execute(f'SELECT id,owner,kind,state,{request} AS request,{data} AS data FROM tasks ORDER BY rowid')]
    def add(self,request):
        validate_request(request)
        with self.tx():
            self._add(request)
        return self.get(request['id'])
    def _add(self,request):
        old=self.db.execute('SELECT request_hash FROM tasks WHERE id=?',(request['id'],)).fetchone()
        if old:
            require(old[0]==digest(request),'Task ID already has different inputs; use a new revision ID')
            return
        self.db.execute('INSERT INTO tasks VALUES(?,?,?,?,?,?,1,?,?)',
            (request['id'],canonical(request),digest(request),request['owner'],request['kind'],'queued','{}',self.clock()))
        self.db.execute('INSERT INTO task_events(id,state,revision,at,data) VALUES(?,?,1,?,?)',
            (request['id'],'queued',self.clock(),'{}'))
        self._supersede_reply_drafts(self.get(request['id']))
    @staticmethod
    def _answer_key(answer):
        if isinstance(answer, dict) and answer.get('evidence_ref'):
            return ('evidence_ref', answer['evidence_ref'])
        return ('answer', canonical(answer))
    def _carry_user_answers(self, row, answers):
        """Preserve signed answers without changing an already bound request."""
        if row['kind'] not in CORRESPONDENCE_KINDS or not answers:
            return row
        inputs = row['request']['inputs']
        existing = inputs.get('user_answers') or []
        recorded = row['data'].get('carried_user_answers', [])
        keys = {self._answer_key(answer) for answer in [*existing, *recorded]}
        inherited = []
        for answer in answers:
            key = self._answer_key(answer)
            if key not in keys:
                inherited.append(answer)
                keys.add(key)
        if row['state'] not in {'queued', 'held', 'credit-wait'} or not unstarted(row):
            if inherited:
                self._update(row['id'], row['revision'], row['state'],
                             {**row['data'], 'carried_user_answers': [*recorded, *inherited]})
                return self.get(row['id'])
            return row
        if not inherited and inputs.get('user_decision_ever') is True:
            return row
        request = copy.deepcopy(row['request'])
        inputs = request['inputs']
        inputs['user_answers'] = [*existing, *inherited]
        inputs['user_decision_ever'] = True
        outcome = 'reply' if row['kind'] == 'reply' else 'research task'
        instruction = ('Recorded user answers are decided. Treat those questions as settled; '
                       f'do not ask the owner again, and use the answers to complete the {outcome}.')
        if instruction not in str(inputs.get('instructions', '')):
            inputs['instructions'] = (str(inputs.get('instructions', '')).rstrip() + '\n' + instruction).strip()
        validate_request(request)
        revision = row['revision'] + 1
        self.db.execute('UPDATE tasks SET request=?,request_hash=?,revision=?,updated=? WHERE id=?',
                        (canonical(request), digest(request), revision, self.clock(), row['id']))
        self.db.execute('INSERT INTO task_events(id,state,revision,at,data) VALUES(?,?,?,?,?)',
                        (row['id'], row['state'], revision, self.clock(),
                         canonical({'decision_answers_carried': True})))
        return self.get(row['id'])
    def _supersede_reply_drafts(self,row):
        if row['kind'] != 'reply':return row
        inherited = []
        for old in self.all():
            old_inputs = old['request'].get('inputs', {})
            answers = old_inputs.get('user_answers') or []
            decision_continuation = bool(answers) or (
                old_inputs.get('automation_parent', {}).get('purpose') == 'user-answer')
            eligible_states = {'owner-draft','revising'}
            if decision_continuation:
                eligible_states |= {'queued','held','awaiting-review','approved',
                                    'awaiting-mailbox','decision-recorded','bounced'}
            # A registered continuation parent is still needed by its child.
            # New inbound work may supersede the child later, but must not erase
            # this registration before repinning.
            if (old['state'] in eligible_states
                    and not old['data'].get('sent_receipt')
                    and newer_reply(row,old)):
                inherited.extend(answers)
                if not old['data'].get('continuation_id'):
                    self._update(old['id'],old['revision'],'superseded',
                                 {**old['data'],'superseded_by':row['id']})
        if inherited:
            row = self._carry_user_answers(row, inherited)
        return row
    def supersede_reply_drafts(self,task_id):
        """Hydrated staged requests learn their actual conversation later."""
        with self.tx():row=self._supersede_reply_drafts(self.get(task_id))
        return self.get(row['id'])

    def supersede_inbound(self, row, successor_id, reason):
        """Supersede covered work and transfer any held answers in one transaction."""
        with self.tx():
            if row['state'] == 'held':
                answers = [*(row['request']['inputs'].get('user_answers') or []),
                           *row['data'].get('carried_user_answers', [])]
                if answers:
                    self._carry_user_answers(self.get(successor_id), answers)
            self._update(row['id'], row['revision'], 'superseded',
                         {**row['data'], 'superseded_by': successor_id, 'reason': reason})

    def update(self,task_id,expected,state,data):
        with self.tx():
            self._update(task_id,expected,state,data)
        return self.get(task_id)
    def _update(self,task_id,expected,state,data):
        require(state in STATES,'Unknown task state')
        old=self.get(task_id)
        require(old['revision']==expected,'Stale task writer')
        self.db.execute('UPDATE tasks SET state=?,revision=revision+1,data=?,updated=? WHERE id=?',
            (state,canonical(data),self.clock(),task_id))
        self.db.execute('INSERT INTO task_events(id,state,revision,at,data) VALUES(?,?,?,?,?)',
            (task_id,state,expected+1,self.clock(),canonical(event_summary(data))))
    def repin(self,task_id,expected,workflow_hash,source_revision,reason=WORKFLOW_CHANGED,*,policy_revision=None):
        """Atomically replace a stale request, retaining its intake identity."""
        with self.tx():
            old=self.get(task_id)
            require(old['revision']==expected,'Stale task writer')
            request = old['request']
            identity_hold = policy_mismatch_hold(old)
            pin = {'source_revision': source_revision, 'workflow_hash': workflow_hash}
            if policy_revision is not None:
                pin['policy_revision'] = policy_revision
            mismatched = (reason == WORKFLOW_PIN_MISMATCH and old['state'] in {'queued', 'held'}
                          and (unstarted(old) or identity_hold)
                          and (request['source_revision'], request.get('policy_revision'), request['workflow_hash'])
                          != (source_revision, policy_revision, workflow_hash))
            stale = (reason == WORKFLOW_CHANGED and unstarted(old)
                     and ((old['state'] == 'queued' and request['workflow_hash'] != workflow_hash)
                          or (old['kind'] in CORRESPONDENCE_KINDS and old['state'] == 'held'
                              and old['data'].get('reason') == WORKFLOW_CHANGED)))
            require(mismatched or stale, 'Only workflow-stale or pin-mismatched work can be re-pinned')
            successor = {**request, **pin, 'id': old['kind']+'-'+digest({'supersedes': task_id, **pin})[:32]}
            if policy_revision is None:
                successor.pop('policy_revision', None)
            validate_request(successor)
            self._add(successor)
            if old['data'].get('credit_waiting'):
                queued=self.get(successor['id'])
                if queued['state']=='queued' and unstarted(queued):
                    self._update(queued['id'],queued['revision'],'queued',
                                 {**queued['data'],**{k:v for k,v in old['data'].items() if k.startswith('credit_')
                                    or k in {'capacity_retries','transport_retries'}}})
            if old['state'] == 'held' and not identity_hold and old['data'].get('reason') != WORKFLOW_CHANGED:
                queued = self.get(successor['id'])
                self._update(queued['id'], queued['revision'], 'held', {**old['data'], **queued['data']})
            self._update(task_id,expected,'superseded',
                         {**(old['data'] if identity_hold or (old['kind'] in CORRESPONDENCE_KINDS
                                                           and old['data'].get('execution_history')) else {}),
                          'reason':reason,'successor':successor['id']})
            self.db.execute('UPDATE pipeline_sources SET task_id=? WHERE task_id=?',
                            (successor['id'],task_id))
            # Reviewed revision/user-answer parents register the executable child.
            for parent in self.all():
                if parent['data'].get('continuation_id')==task_id:
                    # A newer inbound may have superseded the registration
                    # while this child waited unstarted.  Repinning restores
                    # the registered continuation state before admission.
                    self._update(parent['id'],parent['revision'],'revising',
                                 {**parent['data'],'continuation_id':successor['id']})
        return self.get(successor['id'])
    def claim(self,task_id,limit,ignore=frozenset()):
        with self.tx():
            row=self.get(task_id);require(row['state']=='queued','Task is not queued')
            active=[r for r in self.all() if r['state'] in ACTIVE]
            require(len(active)<limit,'CMA concurrency limit reached')
            require(not any(owners(r,ignore)&owners(row,ignore) for r in active),'This scholar/thread already has active work')
            self.db.execute("UPDATE tasks SET state='provisioning',revision=revision+1,updated=? WHERE id=?",(self.clock(),task_id))
            self.db.execute('INSERT INTO task_events(id,state,revision,at,data) VALUES(?,?,?,?,?)',
                (task_id,'provisioning',row['revision']+1,self.clock(),'{}'))
        return self.get(task_id)
    def snapshot(self,path):
        target=sqlite3.connect(str(path))
        try:self.db.backup(target)
        finally:target.close()

    def mail_rebindable(self,row,current_hash):
        """Only an unchanged PASS with no delivery attempt can rebind pending mail."""
        if not (row['state'] == 'awaiting-mailbox' and row['kind'] in {'firstcontact','reply'}
                and not row['data'].get('sent_receipt') and not row['data'].get('draft_receipt')
                and not row['data'].get('stale_draft_conflict')
                and review_current(row, row['request']['workflow_hash'])
                and not matches_workflow(row, current_hash)):
            return False
        # The packet queue shares this database, but need not have been opened yet.
        return (not self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='attempts'").fetchone()
                or not self.db.execute('SELECT 1 FROM attempts WHERE item=?',(row['id'],)).fetchone())

    def review_binding(self,row):
        owner_rebind = False
        mail_rebind = False
        if row['state'] in {'owner-draft','awaiting-mailbox'}:
            from .control import workflow
            current_hash = workflow(row['kind'])
            owner_rebind = owner_draft_rebindable(row, current_hash)
            mail_rebind = self.mail_rebindable(row, current_hash)
        require(reviewable(row) or parked(row) or owner_rebind or mail_rebind,
                'Task is not awaiting review, an agent-held result or unsent workflow-stale mail')
        # Bind the revision at which the current result entered review. Later
        # Agent/Sandbox cleanup bumps the row revision without changing the
        # result, and must not make an already signed verdict obsolete. For
        # pending mail, walk through routing back to its current PASS event too;
        # delivery errors carrying that same reviewed result are not new work.
        revision=expected=row['revision']
        binding_states = {'owner-draft','approved'} if owner_rebind else {row['state']}
        if mail_rebind:binding_states = {'awaiting-mailbox','approved'}
        review=row['data'].get('review')
        review_event=canonical({'review':review.get('signed_review_hash') or digest(review)}) if review else None
        # Compare the small verdict event in SQLite; do not load packet-bearing
        # historical payloads just to find the review revision.
        for state,number,reviewed in self.db.execute('SELECT state,revision,data=? FROM task_events WHERE id=? AND revision<=? '
                                            'ORDER BY revision DESC',(review_event,row['id'],row['revision'])).fetchall():
            if number!=expected or state not in binding_states:break
            revision=number;expected=number-1
            # A reviewer verdict starts a new binding; cleanup alone does not.
            if reviewed:break
        return {'task_id':row['id'],'task_revision':revision,
                'result_hash':digest(row['data']['result']),
                'workflow_hash':row['request']['workflow_hash']}

    def reviews(self):
        return [{'binding':self.review_binding(r),'request':r['request'],'result':r['data']['result'],
                 **({'workflow_binding':r['data']['workflow_binding']} if r['data'].get('workflow_binding') else {}),
                 **({'submission_review_requested':True} if r['data'].get('submission_review_requested') else {}),
                 **({'resolved_inputs':r['data']['thread_inputs']} if r['data'].get('thread_inputs') else {}),
                 **({'mailbox_history':r['data']['mailbox_history']} if r['data'].get('mailbox_history') else {})}
                for r in self.all() if reviewable(r)]

    def submission_review_task(self, paper_id):
        row = next((r for r in reversed(self.all()) if submission_ready_unreviewed(r)
                    and r['request']['inputs'].get('paper', {}).get('id') == paper_id), None)
        require(row is not None, 'No unreviewed submission-ready finalize task for paper')
        return row

    def request_submission_review(self, envelope, secret):
        require(bool(secret), 'Reviewer verification key is not configured')
        require(set(envelope) == {'schema_version','paper_id','task_id','result_hash','signature'}
                and envelope['schema_version'] == 1, 'Invalid submission review request')
        signed = {k:v for k,v in envelope.items() if k != 'signature'}
        expected = hmac.new(secret.encode(), canonical(signed).encode(), hashlib.sha256).hexdigest()
        require(hmac.compare_digest(expected, envelope['signature']), 'Submission review request signature failed')
        event_id = digest(envelope)
        with self.tx():
            if self.db.execute('SELECT 1 FROM control_events WHERE id=?', (event_id,)).fetchone():
                return self.get(envelope['task_id'])
            row = self.submission_review_task(envelope['paper_id'])
            require(row['id'] == envelope['task_id'] and digest(row['data']['result']) == envelope['result_hash'],
                    'Submission review request targets an obsolete result')
            self._update(row['id'], row['revision'], 'awaiting-review',
                         {**row['data'], 'awaiting_operator_review':False, 'submission_review_requested':True})
            self.db.execute('INSERT INTO control_events VALUES(?,?)', (event_id, canonical(envelope)))
        return self.get(row['id'])

    def verdict(self,envelope,secret,*,preserve_packet=None):
        require(bool(secret),'Reviewer verification key is not configured')
        fields={'binding','reviewer','verdict','reason','evidence_ref','signature'}
        require(set(envelope)-{'rebind_workflow_hash','edited_body'}==fields,'Invalid signed review')
        validate_review_rebind(envelope)
        signed={k:v for k,v in envelope.items() if k!='signature'}
        signature=hmac.new(secret.encode(),canonical(signed).encode(),hashlib.sha256).hexdigest()
        require(isinstance(envelope['signature'],str) and re.fullmatch(r'[0-9a-f]{64}',envelope['signature'])
                and hmac.compare_digest(signature,envelope['signature']),'Invalid signed review: review signature failed')
        require(envelope['reviewer'] in INDEPENDENT_REVIEWERS and envelope['verdict'] in {'PASS','BOUNCE','HOLD'},'Invalid reviewer/verdict')
        require(all(isinstance(envelope[k],str) and envelope[k].strip() for k in ['reason','evidence_ref']),'Review needs reasoning and evidence')
        event_id=digest(envelope)
        with self.tx():
            previous=self.db.execute('SELECT payload FROM control_events WHERE id=?',(event_id,)).fetchone()
            if previous:return self.get(envelope['binding']['task_id'])
            row=self.get(envelope['binding']['task_id'])
            validate_review_edit(envelope,row['kind'])
            if 'edited_body' in envelope:
                require(row['state']=='awaiting-review','edited_body requires a task awaiting review')
            if envelope['verdict'] == 'PASS':
                result = row['data'].get('result', {})
                note_closure.source_gate(row['request'], self, result)
                if row['kind'] == 'shortnote' and result.get('outcome') == 'completed':
                    require(not note_closure.trivial(result), 'A trivial short note cannot PASS')
                    require(note_closure.eligible(row), 'Short note requires a closed final round before PASS')
                    if row['data'].get('review', {}).get('verdict') != 'PASS':
                        note_closure.validate_result(result, required=True)
            if row['state'] == 'awaiting-mailbox':
                from .control import workflow
                require(self.mail_rebindable(row, workflow(row['kind'])),
                        'Review targets an obsolete result or workflow, or mail has a delivery attempt or receipt')
            if row['state'] == 'owner-draft':
                require(envelope['verdict'] == 'PASS' and 'rebind_workflow_hash' in envelope,
                        'Owner drafts accept only PASS workflow rebinds')
                require(not row['data'].get('sent_receipt'), 'Cannot rebind a sent owner draft')
                require(not row['data'].get('stale_draft_conflict'), 'Cannot rebind an owner draft with a stale draft conflict')
            # Re-review an already approved, pending delivery without rerunning
            # CMA or changing the original result/request binding.
            rebind = ('rebind_workflow_hash' in envelope and
                      row['state'] in {'approved','awaiting-mailbox','owner-draft','handoff-ready','submission-ready'} and
                      row['data'].get('review',{}).get('verdict') == 'PASS')
            if rebind:
                binding = row['data']['review']['binding']
                if row['state'] in {'owner-draft','awaiting-mailbox'} and binding != envelope['binding']:
                    from .control import workflow
                    require(envelope['rebind_workflow_hash'] == workflow(row['kind']),
                            'Workflow rebind must match the current workflow for the task kind')
                    binding = self.review_binding(row)
                require(binding == envelope['binding'] and
                        digest(row['data']['result']) == envelope['binding']['result_hash'] and
                        row['request']['workflow_hash'] == envelope['binding']['workflow_hash'],
                        'Review targets an obsolete result or workflow')
            else:
                require(self.review_binding(row)==envelope['binding'],'Review targets an obsolete result or workflow')
            require(not agent_hold(row) or envelope['verdict'] in {'BOUNCE','HOLD'},
                    'An agent-held result accepts only BOUNCE or HOLD, never PASS')
            data={**row['data'],'review':envelope}
            if 'edited_body' in envelope:
                from .control import edited_review_result
                result,artifact,raw=edited_review_result(row,envelope['edited_body'])
                require(preserve_packet is not None,'Edited review requires packet preservation')
                preserve_packet(artifact,raw)
                previous_result=row['data']['result']
                previous_hash=envelope['binding']['result_hash']
                data['result']=result
                data['reviewer_edit']={'from_result_hash':previous_hash,
                    'body_sha256_before':hashlib.sha256(previous_result['packet']['body'].encode('utf-8')).hexdigest(),
                    'body_sha256_after':hashlib.sha256(result['packet']['body'].encode('utf-8')).hexdigest(),
                    'reviewer':envelope['reviewer'],'at':self.clock()}
                # Keep only the replaced text: the pre-edit packet (with its attachment
                # bytes) stays in the task Release as the original mail-packet artifact.
                data['review_history']={**data.get('review_history',{}),
                    previous_hash:{'body':previous_result['packet']['body'],
                                   'artifacts':[a for a in previous_result['artifacts'] if a['role']=='mail-packet']}}
                # The original HMAC belongs to the submitted envelope in control_events.
                data['review']={**{k:v for k,v in signed.items() if k!='edited_body'},
                    'binding':{**envelope['binding'],'result_hash':digest(result),'task_revision':row['revision']+1},
                    'signed_review_hash':event_id}
            if (row['kind'] == 'shortnote' and envelope['verdict'] == 'PASS' and not rebind
                    and row['data']['result'].get('outcome') == 'completed'):
                from .formalization_requests import pending_requests
                requests = pending_requests(row['data']['result'])
                if requests:
                    data['formalization_requests'] = requests
            state=row['state'] if rebind else {'PASS':'approved','BOUNCE':'bounced','HOLD':'held'}[envelope['verdict']]
            self.db.execute('UPDATE tasks SET state=?,revision=revision+1,data=?,updated=? WHERE id=?',
                            (state,canonical(data),self.clock(),row['id']))
            self.db.execute('INSERT INTO control_events VALUES(?,?)',(event_id,canonical(envelope)))
            self.db.execute('INSERT INTO task_events(id,state,revision,at,data) VALUES(?,?,?,?,?)',
                            (row['id'],state,row['revision']+1,self.clock(),canonical({'review':event_id})))
        return self.get(row['id'])

    def decision(self,envelope,secret):
        """Record actual user answers or close the reviewed questions."""
        require(bool(secret),'Reviewer verification key is not configured')
        fields = {'schema_version','task_id','result_hash','evidence_ref','reviewer','signature'}
        require(isinstance(envelope, dict) and set(envelope) in
                (fields | {'answers'}, fields | {'action', 'reason'}), 'Invalid user decision envelope')
        closing = 'action' in envelope
        if closing:
            require(envelope['action'] == 'close' and isinstance(envelope['reason'], str)
                    and envelope['reason'].strip(), 'Question closure needs a reason')
        signed={k:v for k,v in envelope.items() if k!='signature'}
        expected=hmac.new(secret.encode(),canonical(signed).encode(),hashlib.sha256).hexdigest()
        require(hmac.compare_digest(expected,envelope['signature']),'Decision signature failed')
        require(envelope['schema_version']==1 and envelope['reviewer']=='local-claude-loop',
                'Only the local operator records user decisions')
        require(isinstance(envelope['evidence_ref'],str) and envelope['evidence_ref'].strip(),
                'Preserve the evidence source for the decision')
        event_id=digest(envelope)
        with self.tx():
            if self.db.execute('SELECT 1 FROM control_events WHERE id=?',(event_id,)).fetchone():
                return self.get(envelope['task_id'])
            row=self.get(envelope['task_id'])
            require(row['state']=='awaiting-user', 'Task is not awaiting a user decision')
            result = row['data']['result']
            require(digest(result)==envelope['result_hash'],'Decision targets an obsolete result')
            questions = result.get('routing', {}).get('questions', [])
            data = dict(row['data'])
            if closing:
                require(row['kind'] in CORRESPONDENCE_KINDS and questions,
                        'Question closure requires open reply or research questions')
                data['user_question_closed'] = {'reason': 'question closed', 'evidence': {
                    'reason': envelope['reason'].strip(), 'evidence_ref': envelope['evidence_ref']}}
                state = 'question-closed'
            else:
                answers = envelope['answers']
                require(isinstance(answers,dict) and set(answers)=={q['id'] for q in questions}
                        and answers and all(isinstance(v,str) and v.strip() for v in answers.values()),
                        'Answer every current question with the actual user response')
                data['user_decision'] = envelope
                state = 'decision-recorded'
            self.db.execute('UPDATE tasks SET state=?,revision=revision+1,data=?,updated=? WHERE id=?',
                            (state,canonical(data),self.clock(),row['id']))
            self.db.execute('INSERT INTO control_events VALUES(?,?)',(event_id,canonical(envelope)))
            self.db.execute('INSERT INTO task_events(id,state,revision,at,data) VALUES(?,?,?,?,?)',
                            (row['id'],state,row['revision']+1,self.clock(),canonical({'decision':event_id})))
        return self.get(row['id'])

    def draft_revision(self, envelope, secret):
        """Admit one immutable, operator-signed owner-draft replacement.

        The request is retained on the task until delivery has observed both
        the replacement and the deletion of the original server draft.  The
        control-event key makes Release replays harmless while keeping the
        signed bytes available for audit.
        """
        require(bool(secret), 'Reviewer verification key is not configured')
        require(isinstance(envelope, dict) and
                set(envelope) in ({'schema_version', 'task_id', 'old_packet_hash',
                                   'new_packet_hash', 'reason', 'packet', 'signature'},
                                  {'schema_version', 'task_id', 'old_packet_hash',
                                   'new_packet_hash', 'reason', 'packet', 'reviewer', 'signature'}),
                'Invalid draft revision envelope')
        signed = {k: v for k, v in envelope.items() if k != 'signature'}
        expected = hmac.new(secret.encode(), canonical(signed).encode(), hashlib.sha256).hexdigest()
        require(hmac.compare_digest(expected, envelope['signature']), 'Draft revision signature failed')
        require(envelope['schema_version'] == 1 and isinstance(envelope['task_id'], str)
                and envelope['task_id'].strip(), 'Invalid draft revision identity')
        if 'reviewer' in envelope:
            require(envelope['reviewer'] == 'local-claude-loop',
                    'Only the local operator stages draft revisions')
        require(isinstance(envelope['old_packet_hash'], str)
                and re.fullmatch(r'[0-9a-f]{64}', envelope['old_packet_hash']),
                'Invalid old packet hash')
        require(isinstance(envelope['new_packet_hash'], str)
                and re.fullmatch(r'[0-9a-f]{64}', envelope['new_packet_hash']),
                'Invalid new packet hash')
        require(isinstance(envelope['reason'], str) and envelope['reason'].strip(),
                'Draft revision needs a reason')
        validate_packet(envelope['packet'])
        require(digest(envelope['packet']) == envelope['new_packet_hash'],
                'New packet hash does not match the staged packet')
        event_id = digest(envelope)
        with self.tx():
            if self.db.execute('SELECT 1 FROM control_events WHERE id=?', (event_id,)).fetchone():
                return self.get(envelope['task_id'])
            row = self.get(envelope['task_id'])
            require(row['state'] == 'owner-draft', 'Draft revision requires an owner draft')
            data = row['data']
            current_packet = data.get('packet_override') or data.get('result', {}).get('packet')
            receipt = data.get('draft_receipt') or {}
            require(isinstance(current_packet, dict) and isinstance(receipt, dict),
                    'Owner draft has no reviewed packet or receipt')
            require(isinstance(receipt.get('remote_id'), str) and receipt['remote_id'].strip(),
                    'Owner draft receipt has no remote ID')
            require(receipt.get('content_hash') == envelope['old_packet_hash'],
                    'Draft revision targets an obsolete draft receipt')
            require(digest(current_packet) == envelope['old_packet_hash'],
                    'Draft revision targets an obsolete packet')
            require(envelope['packet'].get('kind') == current_packet.get('kind'),
                    'Draft revision cannot change kind')
            immutable = set(current_packet) - {'body', 'attachments'}
            require(all(envelope['packet'].get(key) == current_packet.get(key) for key in immutable),
                    'Draft revision may change only body and attachments')
            pending = list(data.get('draft_revision_requests', []))
            if not any(item.get('event_id') == event_id for item in pending):
                pending.append({**envelope, 'event_id': event_id})
            data = {**data, 'draft_revision_requests': pending}
            self.db.execute('UPDATE tasks SET revision=revision+1,data=?,updated=? WHERE id=?',
                            (canonical(data), self.clock(), row['id']))
            self.db.execute('INSERT INTO control_events VALUES(?,?)',
                            (event_id, canonical(envelope)))
            self.db.execute('INSERT INTO task_events(id,state,revision,at,data) VALUES(?,?,?,?,?)',
                            (row['id'], row['state'], row['revision'] + 1, self.clock(),
                             canonical({'draft_revision': event_id})))
        return self.get(envelope['task_id'])
