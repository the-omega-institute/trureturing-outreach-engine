"""Reconcile short-note revisions with attachments on verified reply tasks."""
import base64
from collections import defaultdict
import hashlib
import json
from pathlib import Path
import re
import sys
import tempfile

from .correspondence import problem_ids
from .mailbox import instant
from .queue import canonical, digest, require
from .tick import units

PURPOSE = 'merge-sent-revision'
DUPLICATE_REASON = 'Duplicate sent-revision merge of the same note'
READY_STATES = {'handoff-ready', 'verified'}
HEAD_STATES = READY_STATES | {'round-complete'}
NOTE_ROLES = {'note-pdf', 'note-tex', 'source-archive'}
HOLD_STATES = {'queued', 'credit-wait', 'awaiting-review', 'approved', 'handoff-ready', 'bounced'}
INSTRUCTIONS = (
    'Merge the short-note head with the exact sent note revisions in selected_inputs. '
    'Keep every change sent to the correspondent and every addition on the short-note line, '
    'including prior-work findings and attribution. Make no new claims beyond the supplied versions. '
    'Keep the stable note ID and use a revision greater than both inputs and any existing public revision. '
    'Return the complete rebuilt PDF and source, with a list in the private evidence of differences '
    'resolved and where each change appears. Carry forward the prior-work evidence and keep private '
    'correspondence and the difference list out of the public files. If the base has no prior-work '
    'check, perform the required check for the existing claims and their attribution.'
)


def result(row):
    return row['data'].get('result') or {}


def binding(row):
    return {'task_id': row['id'], 'result_hash': digest(result(row))}


def source_links(row):
    inputs = row['request']['inputs']
    links = [*inputs.get('note_sources', []), *result(row).get('note_sources', [])]
    pipeline = inputs.get('pipeline', {})
    if pipeline.get('note_task_id'):
        links.append({'task_id': pipeline['note_task_id'], 'result_hash': pipeline.get('note_result_hash')})
    if parent := inputs.get('automation_parent'):
        if parent.get('result_hash'):
            links.append(parent)
    return links


def note_files(row):
    return [a for a in result(row).get('artifacts', []) if a.get('role') in NOTE_ROLES]


def attachment_role(name):
    if re.match(r'^note(?:[._ -]|[0-9])', name, re.I):
        return {'.pdf': 'note-pdf', '.tex': 'note-tex', '.zip': 'source-archive'}.get(Path(name).suffix.lower())
    if re.fullmatch(r'source-v[1-9][0-9]*\.zip', name, re.I):
        return 'source-archive'
    return None


def merge_identity(result_id, head, replies):
    return {'purpose': PURPOSE, 'result_id': result_id,
            'sources': [binding(head), *(binding(r) for r in replies)]}


def merge_task_id(result_id, head, replies):
    return 'shortnote-' + digest(merge_identity(result_id, head, replies))[:32]


def sources_key(head, replies):
    """One merge per note version pair: results sharing a note share it."""
    return canonical([binding(head), *(binding(r) for r in replies)])


def merges_by_sources(rows):
    found = {}
    for row in rows.values():  # Ledger creation order: the earliest merge wins.
        if (row['kind'] == 'shortnote' and Revisions.is_merge(row) and row['state'] != 'superseded'):
            found.setdefault(canonical(row['request']['inputs'].get('note_sources', [])), row['id'])
    return found


def parent_gate(request, tasks, reply):
    links = request['inputs'].get('note_sources', [])
    require(request['kind'] == 'shortnote' and len(links) >= 2 and links[1] == binding(reply),
            'Sent note merge needs exact source note and reply bindings')
    for index, link in enumerate(links):
        source = tasks.get(link['task_id'])
        require(link == binding(source), 'Sent note merge source version changed')
        require((index == 0 and source['kind'] == 'shortnote')
                or (index > 0 and source['kind'] == 'reply' and source['state'] == 'verified'
                    and source['data'].get('sent_receipt')),
                'Sent note merge requires a short-note base and verified replies')


class Revisions:
    def __init__(self, tasks, rows=None):
        if rows is None:
            rows = [tasks.get(task_id) for (task_id,) in tasks.db.execute(
                "SELECT id FROM tasks WHERE kind IN ('reply', 'shortnote', 'mathesis') ORDER BY rowid")]
        self.rows = {r['id']: r for r in rows}
        self.ids = {r['id']: problem_ids(r) for r in rows if r['kind'] != 'reply'}
        self.files = {}
        self.shortnote_ids_by_sha = defaultdict(set)
        self.heads = {}
        self.sent = defaultdict(list)
        self.replies = {}
        self.attachments = {}
        for row in rows:
            if row['kind'] != 'shortnote':
                continue
            cached = tasks.db.execute('SELECT payload FROM pipeline_state WHERE key=?',
                                      ('note-coverage:' + row['id'],)).fetchone()
            if cached:
                coverage = json.loads(cached[0])
                if coverage.get('result_hash') == digest(result(row)):
                    self.ids[row['id']].update(coverage.get('problem_ids', []))
            self.files[row['id']] = note_files(row)
            for a in self.files[row['id']]:
                self.shortnote_ids_by_sha[a['sha256']].update(self.ids[row['id']])
            if (row['state'] in HEAD_STATES and result(row).get('outcome') == 'completed'
                    and self.files[row['id']]):
                for result_id in self.ids[row['id']]:
                    self.heads[result_id] = row  # Tasks are in ledger creation order.
        for row in rows:
            if row['kind'] == 'reply' and row['state'] == 'verified':
                self.index_reply(row)
        for entries in self.sent.values():
            entries.sort(reverse=True)

    def sources(self, row):
        for link in source_links(row):
            parent = self.rows.get(link['task_id'])
            if (parent and parent['kind'] == 'shortnote'
                    and binding(parent)['result_hash'] == link.get('result_hash')):
                yield parent

    def index_reply(self, row):
        data = row['data']
        receipt = data.get('sent_receipt') or {}
        packet = data.get('packet_override') or result(row).get('packet') or {}
        if not receipt.get('remote_id') or not receipt.get('evidence_ref') or not packet.get('attachments'):
            return
        if receipt.get('content_hash') != digest(packet):
            return
        try:
            at = instant(receipt['at']).timestamp()
        except (KeyError, ValueError, TypeError, AttributeError):
            return
        inputs = row['request']['inputs']
        ids = problem_ids(row) | set(packet.get('problem_ids', []))
        selected = [*inputs.get('selected_inputs', []),
                    *data.get('thread_inputs', {}).get('selected_inputs', [])]
        known = {a['sha256']: a for a in selected if a.get('sha256') and a.get('release_tag')}
        known.update((a['sha256'], a) for a in note_files(row))
        source_ids = defaultdict(set)
        for parent in self.sources(row):
            for a in self.files[parent['id']]:
                known.setdefault(a['sha256'], a)
                source_ids[a['sha256']].update(self.ids[parent['id']])
        # A selected note input can establish the result even in a legacy packet
        # with no explicit problem IDs.
        for a in selected:
            tag = a.get('release_tag', '')
            if a.get('role') in NOTE_ROLES or attachment_role(a.get('name', '')):
                ids.update(self.ids.get(tag.removeprefix('outreach-task-'), set()))
                ids.update(self.shortnote_ids_by_sha[a.get('sha256')])
        attached = {}
        for item in packet['attachments']:
            raw = base64.b64decode(item['content_b64'])
            sha = hashlib.sha256(raw).hexdigest()
            ref = known.get(sha, {})
            role = ref.get('role')
            if role not in NOTE_ROLES:
                role = attachment_role(item['name']) or attachment_role(ref.get('name', ''))
            if not role:
                continue
            if sha in known:
                source_ids[sha].update(self.shortnote_ids_by_sha[sha])
            attached[sha] = {**ref, 'role': role, 'sha256': sha, 'bytes': len(raw)}
            self.attachments[row['id'], sha] = item
        if not attached:
            return
        self.replies[row['id']] = row
        for result_id in ids.union(*(source_ids[sha] for sha in attached)):
            hashes = tuple(sha for sha in attached
                           if not source_ids[sha] or result_id in source_ids[sha])
            if hashes:
                self.sent[result_id].append((at, row['id'], hashes))
        self.files[row['id']] = list(attached.values())

    def incorporated(self, row):
        """Follow pinned short-note inputs, including earlier merged rounds."""
        hashes, seen, pending = set(), set(), [row]
        while pending:
            current = pending.pop()
            if current['id'] in seen:
                continue
            seen.add(current['id'])
            hashes.update(a['sha256'] for a in note_files(current))
            if current['kind'] == 'shortnote':
                hashes.update(a['sha256'] for a in current['request']['inputs'].get('selected_inputs', [])
                              if a.get('sha256') and a.get('release_tag'))
                for link in current['request']['inputs'].get('note_sources', []):
                    reply = self.replies.get(link['task_id'])
                    if reply and binding(reply) == link:
                        hashes.update(a['sha256'] for a in self.files[reply['id']])
            pending.extend(self.sources(current))
        return hashes

    def missing(self, result_id, row):
        sent = self.sent.get(result_id)
        if not sent:
            return []
        hashes = self.incorporated(row)
        missing = []
        for _, task_id, attachments in sent:
            if not hashes.issuperset(attachments):
                missing.append(self.replies[task_id])
                hashes.update(attachments)  # Distinct earlier sent versions only.
        return missing

    def divergences(self):
        for result_id, head in sorted(self.heads.items()):
            if replies := self.missing(result_id, head):
                yield result_id, head, replies

    def reason(self, row, extra_ids=()):
        ids = problem_ids(row) | set(extra_ids)
        for source in self.sources(row):
            ids.update(self.ids[source['id']])
        for result_id in sorted(ids & self.sent.keys()):
            head = self.heads.get(result_id)
            if head and (self.missing(result_id, head)
                         or (self.is_merge(head) and head['state'] not in READY_STATES)):
                return f'Sent note revision for {result_id} requires a handoff-ready merged short note'
            if missing := self.missing(result_id, row):
                return f'Stale note for {result_id} omits the verified sent revision from {missing[0]["id"]}'
        return None

    @staticmethod
    def is_merge(row):
        return bool(row['request']['inputs'].get('sent_revision_result_id'))

    def work(self):
        rows = []
        for result_id, head in sorted(self.heads.items()):
            replies = self.missing(result_id, head)
            if replies:
                base, reply = head, replies[0]
                task_id = (merges_by_sources(self.rows).get(sources_key(base, replies))
                           or merge_task_id(result_id, base, replies))
            elif self.is_merge(head) and head['state'] == 'round-complete':
                sources = head['request']['inputs']['note_sources']
                base, reply = self.rows[sources[0]['task_id']], self.replies[sources[1]['task_id']]
                task_id = head['id']
            else:
                continue
            merge_id = None
            seen = set()
            while task_id in self.rows and task_id not in seen:
                seen.add(task_id)
                merge_id = task_id
                row = self.rows[merge_id]
                successor = row['data'].get('continuation_id') or row['data'].get('successor')
                if not successor:
                    break
                task_id = successor
            rows.append({'result_id': result_id, 'head_task_id': base['id'],
                         'head_sha256': [a['sha256'] for a in self.files[base['id']]],
                         'reply_task_id': reply['id'],
                         'reply_sha256': [a['sha256'] for a in self.files[reply['id']]],
                         'merge_task_id': merge_id})
        return rows

    def selected_inputs(self, ledger, head, replies):
        selected = [a for a in result(head)['artifacts'] if a['role'] in NOTE_ROLES | {'evidence'}]
        for reply in replies:
            sent = self.files[reply['id']]
            for ref in sent:
                if not ref.get('release_tag'):
                    ref = self.preserve_attachment(ledger, reply, ref)
                selected.append(ref)
            if len([a for a in sent if a['role'] == 'note-pdf']) == 1:
                sources = [a for a in note_files(reply) if a['role'] != 'note-pdf']
                for role in ('note-tex', 'source-archive'):
                    matching = [a for a in sources if a['role'] == role]
                    if len(matching) == 1:
                        selected.extend(matching)
        return list({(a['release_tag'], a['name'], a['sha256']): a for a in selected}.values())

    def preserve_attachment(self, ledger, reply, ref):
        """Promote exact packet bytes when the reply did not return a separate asset."""
        from .control import verify_artifacts

        item = self.attachments[reply['id'], ref['sha256']]
        suffix = Path(item['name']).suffix.lower()
        ref = {**ref, 'release_tag': 'outreach-task-' + reply['id'],
               'name': 'sent-note-' + ref['sha256'] + suffix}
        with tempfile.TemporaryDirectory(prefix='sent-note-') as temp:
            path = Path(temp) / ref['name']
            path.write_bytes(base64.b64decode(item['content_b64']))
            ledger.releases.preserve_task(path, ref['release_tag'])
            verify_artifacts({'artifacts': [ref]}, ledger.releases, temp)
        return ref


def gate(row, tasks, extra_ids=()):
    if row['kind'] in {'mathesis', 'shortnote'}:
        reason = Revisions(tasks).reason(row, extra_ids)
        require(reason is None, reason)


def hold_stale(ledger, revisions):
    rows = [r for r in revisions.rows.values() if r['kind'] == 'mathesis' and r['state'] in HOLD_STATES]
    for row in units(ledger, 'sent-revision-holds', rows):
        if reason := revisions.reason(row):
            ledger.tasks.update(row['id'], row['revision'], 'held',
                                {**row['data'], 'reason': reason, 'hold_kind': 'sent-note-revision'})
            ledger.save()


def dedupe(ledger, revisions):
    """Supersede unstarted duplicate merges of the same note version pair."""
    from .tasks import unstarted
    keep = merges_by_sources(revisions.rows)
    for row in list(revisions.rows.values()):
        if (row['kind'] == 'shortnote' and Revisions.is_merge(row) and row['state'] == 'queued'
                and unstarted(row)):
            key = canonical(row['request']['inputs'].get('note_sources', []))
            if keep.get(key) not in (None, row['id']):
                updated = ledger.tasks.update(row['id'], row['revision'], 'superseded',
                                              {**row['data'], 'reason': DUPLICATE_REASON,
                                               'successor': keep[key]})
                revisions.rows[row['id']] = updated or ledger.tasks.get(row['id'])
                ledger.save()


def stage(ledger, revisions, revision, budget):
    from .pipeline import request

    dedupe(ledger, revisions)
    staged = merges_by_sources(revisions.rows)
    for result_id, head, replies in units(ledger, 'sent-revision-merges', list(revisions.divergences()),
                                          key=lambda item: item[0]):
        task_id = merge_task_id(result_id, head, replies)
        if sources_key(head, replies) in staged:
            continue
        identity = merge_identity(result_id, head, replies)
        if task_id in revisions.rows:
            old = revisions.rows[task_id]
            if old['state'] != 'superseded' or old['data'].get('reason') == DUPLICATE_REASON:
                continue
            # Superseded by an older rule (e.g. note coverage): stage afresh.
            identity = {**identity, 'restaged_from': task_id}
            task_id = 'shortnote-' + digest(identity)[:32]
            if task_id in revisions.rows:
                continue
        if not budget:
            break
        budget -= 1
        inputs = head['request']['inputs']
        try:
            selected = revisions.selected_inputs(ledger, head, replies)
        except (OSError, RuntimeError, ValueError) as error:
            print(f'Sent note revision {result_id}: artifact preservation failed ({type(error).__name__})',
                  file=sys.stderr)
            continue
        merge = {key: inputs[key] for key in ('pipeline', 'result', 'owner_ids', 'recorded_contacts',
                                              'joint_work', 'authorship') if key in inputs}
        merge.update(sent_revision_result_id=result_id, problem_ids=sorted(revisions.ids[head['id']]),
                     automation_parent={**binding(replies[0]), 'purpose': PURPOSE},
                     note_sources=identity['sources'],
                     selected_inputs=selected, previous_result=result(head), note_round='uplift',
                     instructions=INSTRUCTIONS)
        ledger.tasks.add(request('shortnote', identity, head['owner'], merge, revision))
        staged[sources_key(head, replies)] = task_id
        ledger.save()
    return budget
