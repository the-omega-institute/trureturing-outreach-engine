"""Transactional offline queue, immutable versions and version-bound approvals.

Trusted in-process reference API, not an authenticated production service. Callers
supply evidence references; this library does not perform scientific review or
validate external evidence. Production must authenticate roles, validate reports,
resolve contact aliases and implement storage/transport contracts in DESIGN.md.
"""
import base64
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
import hashlib
import json
import re
import sqlite3
import time
from typing import Callable, Union
from pathlib import Path

from .policy import policy_text
from .transports import DefiniteRejection, NotSent, Readback, Submission, Transport


class GateError(ValueError):
    """An invalid transition, stale approval or policy refusal."""


def canonical(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


def digest(value) -> str:
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def workflow_digest(workflow: dict, review_text: Union[str, dict]) -> str:
    return digest({"workflow": workflow, "review": review_text})


def load_workflow_digests(root=None) -> dict:
    """Bind all transitive reviewer instructions, including the shared contract."""
    engine = {} if root is None else {'engine_root': Path(root).parent}
    prefix = 'managed' if root is None else Path(root).name

    def read(path):
        return policy_text(f'managed/{path}', **engine,
                           engine_path=f'{prefix}/{path}')

    shared = read("firstcontact/review.md")
    result = {}
    for kind in ("firstcontact", "daily", "reply"):
        directory = 'replies' if kind == 'reply' else kind
        workflow = json.loads(read(f"{directory}/workflow.json"))
        reviews = {"shared_review_contract": shared}
        if kind == "daily":
            reviews["daily_review"] = read("daily/DESIGN-notes.md")
        if kind == "reply":
            reviews["reply_review"] = read("replies/review.md")
        result[kind] = workflow_digest(workflow, reviews)
    return result


def require(condition, reason):
    if not condition:
        raise GateError(reason)


def _strings(values, label):
    require(isinstance(values, list) and all(isinstance(v, str) and v.strip() for v in values),
            "Invalid " + label)
    require(len(set(values)) == len(values), "Duplicate " + label)


def _media(values):
    require(isinstance(values, list), "Media must be a list")
    names = []
    for media in values:
        require(isinstance(media, dict) and set(media) == {"name", "media_type", "content_b64"},
                "Media must include exact name, type and bytes")
        name = media["name"]
        require(isinstance(name, str) and name not in {"", ".", ".."}
                and not any(c in name for c in "/\\\r\n\x00"), "Invalid media name")
        require(isinstance(media["media_type"], str) and "/" in media["media_type"], "Invalid MIME type")
        try:
            raw = base64.b64decode(media["content_b64"], validate=True)
            require(base64.b64encode(raw).decode("ascii") == media["content_b64"], "Noncanonical base64")
        except (ValueError, TypeError) as exc:
            raise GateError("Invalid media bytes") from exc
        names.append(name)
    require(len(names) == len(set(names)), "Duplicate media name")


def validate_packet(packet: dict):
    require(isinstance(packet, dict) and type(packet.get("schema_version")) is int
            and packet["schema_version"] == 1, "Unsupported packet schema")
    kind = packet.get("kind")
    if kind in {"firstcontact", "reply"}:
        fields = {"schema_version", "kind", "sender", "to", "cc", "subject",
                  "body", "problem_ids", "attachments"}
        if kind == 'reply': fields |= {'thread_id', 'latest_inbound_message_id', 'user_decision_ever'}
        require(set(packet) == fields, "Unknown/missing letter fields")
        for field in ("to", "cc", "problem_ids"):
            _strings(packet[field], field)
        require(packet["to"] and (packet["problem_ids"] or kind == 'reply'), "Missing recipient or problem")
        if kind == 'reply':
            require(all(isinstance(packet[k], str) and packet[k].strip() for k in
                        ['thread_id','latest_inbound_message_id']) and type(packet['user_decision_ever']) is bool,
                    'Reply requires the exact latest inbound identity and user-decision history')
        addresses = [packet["sender"], *packet["to"], *packet["cc"]]
        require(all(isinstance(a, str) and a == a.lower() and
                    re.fullmatch(r"[^\s@<>]+@[^\s@<>]+\.[^\s@<>]+", a) for a in addresses),
                "Use explicit normalized mailbox addresses")
        require(len(packet["to"] + packet["cc"]) == len(set(packet["to"] + packet["cc"])),
                "To/Cc must not overlap")
        require(isinstance(packet["subject"], str) and packet["subject"].strip()
                and not any(c in packet["subject"] for c in "\r\n\x00"), "Invalid subject")
        require(isinstance(packet["body"], str) and packet["body"].strip(), "Missing body")
        _media(packet["attachments"])
    elif kind == "daily":
        require(set(packet) == {"schema_version", "kind", "account", "covered_days", "text",
                               "media", "evidence_fingerprint"}, "Unknown/missing daily fields")
        require(isinstance(packet["account"], str) and packet["account"].strip(), "Missing account")
        _strings(packet["covered_days"], "covered_days")
        require(bool(packet["covered_days"]), "Missing daily coverage")
        try:
            days = [date.fromisoformat(d) for d in packet["covered_days"]]
        except ValueError as exc:
            raise GateError("Invalid covered date") from exc
        require(all(d.isoformat() == s for d, s in zip(days, packet["covered_days"]))
                and days == sorted(days)
                and (days[-1] - days[0]).days + 1 == len(days), "Daily dates must be ordered contiguous ISO dates")
        require(isinstance(packet["text"], str) and packet["text"].strip(), "Missing daily text")
        require(isinstance(packet["evidence_fingerprint"], str)
                and re.fullmatch(r"[0-9a-f]{64}", packet["evidence_fingerprint"]), "Invalid evidence fingerprint")
        _media(packet["media"])
    else:
        raise GateError("Unknown workflow kind")
    canonical(packet)  # Reject non-JSON values and nonfinite numbers.


def delivery_tokens(packet):
    if packet['kind'] == 'reply':
        return [digest(['reply', packet['thread_id'], packet['latest_inbound_message_id']])]
    if packet["kind"] == "firstcontact":
        return [digest(["firstcontact", recipient, problem])
                for recipient in packet["to"] + packet["cc"] for problem in packet["problem_ids"]]
    return [digest(["daily", packet["account"], day]) for day in packet["covered_days"]]


@dataclass(frozen=True)
class Authority:
    kind: str
    account: str
    expires_at: float
    evidence_ref: str


class Queue:
    MAX_SUBMISSIONS = 3

    def __init__(self, path, workflows: dict, reviewer="claude-loop", clock: Callable = time.time,
                 checkpoint: Callable = lambda: None):
        require(workflows and all(k in {"firstcontact", "daily", "reply"} and isinstance(v, str)
                and re.fullmatch(r"[0-9a-f]{64}", v) for k, v in workflows.items()), "Invalid workflow digest registry")
        self.workflows = dict(workflows)
        self.reviewer = reviewer
        self.clock = clock
        # The coordinator supplies a FORCED remote save here. send-intent,
        # send-call and draft deletion proofs must never be debounced.
        self.checkpoint = checkpoint
        self.db = sqlite3.connect(str(path), isolation_level=None, timeout=5)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
          CREATE TABLE IF NOT EXISTS items (
            id TEXT PRIMARY KEY, state TEXT NOT NULL, version INTEGER NOT NULL,
            kind TEXT NOT NULL, reason TEXT NOT NULL DEFAULT '');
          CREATE TABLE IF NOT EXISTS versions (
            item TEXT NOT NULL REFERENCES items(id), version INTEGER NOT NULL,
            hash TEXT NOT NULL, workflow TEXT NOT NULL, packet TEXT NOT NULL,
            PRIMARY KEY(item,version));
          CREATE TABLE IF NOT EXISTS research (
            item TEXT NOT NULL, version INTEGER NOT NULL, report TEXT NOT NULL,
            PRIMARY KEY(item,version), FOREIGN KEY(item,version) REFERENCES versions(item,version));
          CREATE TABLE IF NOT EXISTS reviews (
            item TEXT NOT NULL, version INTEGER NOT NULL, envelope TEXT NOT NULL,
            PRIMARY KEY(item,version), FOREIGN KEY(item,version) REFERENCES versions(item,version));
          CREATE TABLE IF NOT EXISTS policies (kind TEXT PRIMARY KEY, revision INTEGER NOT NULL, snapshot TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS attempts (
            item TEXT PRIMARY KEY REFERENCES items(id), key TEXT UNIQUE NOT NULL,
            transport TEXT NOT NULL, status TEXT NOT NULL, calls INTEGER NOT NULL,
            policy_revision INTEGER NOT NULL, authority TEXT NOT NULL,
            locator TEXT NOT NULL DEFAULT '', readback TEXT NOT NULL DEFAULT '');
          CREATE TABLE IF NOT EXISTS reservations (
            token TEXT PRIMARY KEY, item TEXT NOT NULL REFERENCES items(id));
          CREATE TABLE IF NOT EXISTS events (
            seq INTEGER PRIMARY KEY AUTOINCREMENT, at REAL NOT NULL, item TEXT NOT NULL,
            action TEXT NOT NULL, data TEXT NOT NULL);
        """)

    def close(self):
        self.db.close()

    @contextmanager
    def _tx(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def _event(self, item, action, data):
        self.db.execute("INSERT INTO events(at,item,action,data) VALUES(?,?,?,?)",
                        (self.clock(), item, action, canonical(data)))

    def get(self, item):
        row = self.db.execute("""SELECT i.*,v.hash AS content_hash,v.workflow AS workflow_hash,v.packet
          FROM items i JOIN versions v ON i.id=v.item AND i.version=v.version WHERE i.id=?""", (item,)).fetchone()
        require(row is not None, "Unknown item")
        result = dict(row)
        result["packet"] = json.loads(result["packet"])
        return result

    def _bound(self, binding):
        require(isinstance(binding, dict) and type(binding.get("version")) is int
                and binding["version"] > 0 and isinstance(binding.get("item_id"), str)
                and all(isinstance(binding.get(k), str) and re.fullmatch(r"[0-9a-f]{64}", binding[k])
                        for k in ("content_hash", "workflow_hash")), "Invalid version binding")
        row = self.get(binding["item_id"])
        require((row["version"], row["content_hash"], row["workflow_hash"]) ==
                (binding["version"], binding["content_hash"], binding["workflow_hash"]), "Stale version/content/policy")
        require(row["workflow_hash"] == self.workflows.get(row["kind"]), "Workflow policy changed")
        require(digest(row["packet"]) == row["content_hash"], "Stored packet corruption")
        return row

    def binding(self, item):
        row = self.get(item)
        return {"item_id": item, "version": row["version"], "content_hash": row["content_hash"],
                "workflow_hash": row["workflow_hash"]}

    def add(self, item, packet):
        validate_packet(packet)
        require(isinstance(item, str) and item.strip(), "Missing stable item ID")
        require(packet["kind"] in self.workflows, "Workflow not configured")
        with self._tx():
            exists = self.db.execute("SELECT 1 FROM items WHERE id=?", (item,)).fetchone()
            if exists:
                row = self.get(item)
                require(row["content_hash"] == digest(packet) and
                        row["workflow_hash"] == self.workflows[packet["kind"]], "Conflicting intake; create explicit revision")
                return row
            self.db.execute("INSERT INTO items(id,state,version,kind) VALUES(?,'drafted',1,?)", (item, packet["kind"]))
            self.db.execute("INSERT INTO versions VALUES(?,?,?,?,?)",
                            (item, 1, digest(packet), self.workflows[packet["kind"]], canonical(packet)))
            self._event(item, "drafted", self.binding(item))
        return self.get(item)

    def revise(self, item, packet, release_evidence=""):
        validate_packet(packet)
        with self._tx():
            row = self.get(item)
            require(not self.db.execute("SELECT 1 FROM attempts WHERE item=?", (item,)).fetchone(), "Attempted delivery is immutable")
            require(packet["kind"] == row["kind"], "Cannot change workflow kind")
            require(row["state"] != "held" or release_evidence.strip(), "Held item requires explicit release evidence")
            version = row["version"] + 1
            self.db.execute("INSERT INTO versions VALUES(?,?,?,?,?)",
                            (item, version, digest(packet), self.workflows[row["kind"]], canonical(packet)))
            self.db.execute("UPDATE items SET state='drafted',version=?,reason='' WHERE id=?", (version, item))
            self._event(item, "revised", {**self.binding(item), "release_evidence": release_evidence})
        return self.get(item)

    def research_checked(self, binding, report_ref):
        require(isinstance(report_ref, str) and report_ref.strip(), "Missing research evidence")
        with self._tx():
            row = self._bound(binding)
            require(row["state"] == "drafted", "Research gate requires drafted state")
            self.db.execute("INSERT INTO research VALUES(?,?,?)", (row["id"], row["version"], report_ref))
            self.db.execute("UPDATE items SET state='research-checked' WHERE id=?", (row["id"],))
            self._event(row["id"], "research-checked", {**binding, "report_ref": report_ref})

    def pending_reviews(self):
        # Full inventory, including already claimed items. No receipt/checkpoint filter.
        return [self.get(r["id"]) for r in self.db.execute(
            "SELECT id FROM items WHERE state IN ('research-checked','in-review') ORDER BY rowid")]

    def inventory(self):
        """All current items, including holds and uncertain deliveries, for patrol."""
        return [self.get(r["id"]) for r in self.db.execute("SELECT id FROM items ORDER BY rowid")]

    def enqueue(self, binding):
        with self._tx():
            row = self._bound(binding)
            require(row["state"] in {"research-checked", "in-review"}, "Research must precede review")
            self.db.execute("UPDATE items SET state='in-review' WHERE id=?", (row["id"],))
            self._event(row["id"], "in-review", binding)

    def verdict(self, envelope):
        fields = {"schema_version", "item_id", "version", "content_hash", "workflow_hash",
                  "reviewer", "verdict", "reason", "evidence_ref"}
        require(isinstance(envelope, dict) and set(envelope) == fields
                and type(envelope["schema_version"]) is int and envelope["schema_version"] == 1,
                "Invalid verdict envelope")
        require(envelope["reviewer"] == self.reviewer, "Only configured Claude reviewer may approve")
        require(envelope["verdict"] in {"PASS", "BOUNCE", "HOLD"}, "Unknown verdict")
        require(all(isinstance(envelope[k], str) and envelope[k].strip() for k in ("reason", "evidence_ref")),
                "Verdict requires reason and evidence")
        with self._tx():
            row = self._bound(envelope)
            prior = self.db.execute("SELECT envelope FROM reviews WHERE item=? AND version=?",
                                    (row["id"], row["version"])).fetchone()
            if prior:
                require(prior["envelope"] == canonical(envelope), "Conflicting verdict replay")
                return
            require(row["state"] == "in-review", "Review must be queued")
            require(self.db.execute("SELECT 1 FROM research WHERE item=? AND version=?",
                                   (row["id"], row["version"])).fetchone(), "Missing research report")
            self.db.execute("INSERT INTO reviews VALUES(?,?,?)", (row["id"], row["version"], canonical(envelope)))
            state = {"PASS": "approved", "BOUNCE": "bounced", "HOLD": "held"}[envelope["verdict"]]
            self.db.execute("UPDATE items SET state=?,reason=? WHERE id=?", (state, envelope["reason"], row["id"]))
            self._event(row["id"], state, envelope)

    def hold(self, item, reason):
        require(isinstance(reason, str) and reason.strip(), "Missing hold reason")
        with self._tx():
            self.get(item)
            require(not self.db.execute("SELECT 1 FROM attempts WHERE item=?", (item,)).fetchone(),
                    "Reconcile attempted delivery; do not relabel it held")
            self.db.execute("UPDATE items SET state='held',reason=? WHERE id=?", (reason, item))
            self._event(item, "held", {"reason": reason})

    def set_policy(self, kind, revision, snapshot):
        require(kind in self.workflows and type(revision) is int and revision > 0, "Invalid policy revision")
        require(set(snapshot) == {"checked_at", "expires_at", "complete", "paused", "contacts"}, "Invalid policy fields")
        require(type(snapshot["complete"]) is bool and type(snapshot["paused"]) is bool
                and isinstance(snapshot["contacts"], dict), "Invalid policy snapshot")
        require(snapshot["checked_at"] <= self.clock() < snapshot["expires_at"], "Policy must be fresh")
        with self._tx():
            old = self.db.execute("SELECT revision FROM policies WHERE kind=?", (kind,)).fetchone()
            require(not old or revision > old["revision"], "Stale policy revision")
            self.db.execute("INSERT OR REPLACE INTO policies VALUES(?,?,?)", (kind, revision, canonical(snapshot)))
            self._event("policy:" + kind, "policy-updated", {"revision": revision, "snapshot": snapshot})

    def _policy(self, packet, authority):
        now = self.clock()
        account = packet["sender"] if packet["kind"] in {"firstcontact", "reply"} else packet["account"]
        require(isinstance(authority, Authority) and authority.kind == packet["kind"]
                and authority.account == account and authority.expires_at > now
                and authority.evidence_ref.strip(), "Missing, expired or mis-scoped send authority")
        policy = self.db.execute("SELECT * FROM policies WHERE kind=?", (packet["kind"],)).fetchone()
        require(policy is not None, "Missing contact/account policy")
        snapshot = json.loads(policy["snapshot"])
        require(snapshot["complete"] and not snapshot["paused"]
                and snapshot["checked_at"] <= now < snapshot["expires_at"], "Policy incomplete, stale or paused")
        recipients = packet["to"] + packet["cc"] if packet["kind"] in {"firstcontact", "reply"} else [account]
        require(all(snapshot["contacts"].get(r) == "clear" for r in recipients), "Recipient/account blocked or unknown")
        return policy["revision"]

    def dispatch(self, item, transport: Transport, authority: Authority, recipient_keys=()):
        # Persist intent/reservations BEFORE any possible external effect.
        with self._tx():
            row = self._bound(self.binding(item))
            require(row["state"] in {"approved", "not-sent"}, "Exact current version is not approved or already attempted")
            require(transport.kind == row["kind"] and bool(transport.name), "Wrong transport channel")
            review = self.db.execute("SELECT envelope FROM reviews WHERE item=? AND version=?",
                                     (item, row["version"])).fetchone()
            require(review is not None and json.loads(review["envelope"])["verdict"] == "PASS", "No exact Claude PASS")
            revision = self._policy(row["packet"], authority)
            key = digest([item, row["version"], row["content_hash"]])
            old = self.db.execute("SELECT * FROM attempts WHERE item=?", (item,)).fetchone()
            if old:
                require(old["status"] in {"rejected", "not-sent"} and old["key"] == key
                        and old["transport"] == transport.name, "Uncertain/accepted attempt cannot be repeated or moved")
                if old['status']=='not-sent':
                    require(self._cleaned(item,old['calls']), 'Remove the proven unsent attempt draft before retry')
                require(old["calls"] < self.MAX_SUBMISSIONS, "Submission budget exhausted")
                self.db.execute("UPDATE attempts SET status='sending',calls=calls+1,policy_revision=?,authority=? WHERE item=?",
                                (revision, canonical(authority.__dict__), item))
            else:
                require(all(isinstance(k,str) and re.fullmatch(r'[0-9a-f]{64}',k) for k in recipient_keys),
                        'Invalid resolved-recipient reservations')
                for token in {*delivery_tokens(row["packet"]),*recipient_keys}:
                    owner = self.db.execute("SELECT item FROM reservations WHERE token=?", (token,)).fetchone()
                    require(owner is None, "Duplicate recipient/problem or covered day already reserved")
                    self.db.execute("INSERT INTO reservations VALUES(?,?)", (token, item))
                self.db.execute("INSERT INTO attempts(item,key,transport,status,calls,policy_revision,authority) VALUES(?,?,?,'sending',1,?,?)",
                                (item, key, transport.name, revision, canonical(authority.__dict__)))
            self.db.execute("UPDATE items SET state='sending',reason='' WHERE id=?", (item,))
            self._event(item, "send-intent", {"key": key, "transport": transport.name, "policy_revision": revision,
                                            "authority": authority.__dict__,
                                            "tracks_send_call":bool(getattr(transport,'tracks_send_call',False))})
        self.checkpoint()  # The remote ledger must preserve intent before submit.
        def before_send(draft_id):
            with self._tx():
                require(self.get(item)['state']=='sending', 'Attempt changed before send')
                self._event(item,'send-call',{'key':key,'draft_id':draft_id})
            self.checkpoint()  # No /send can precede durable send-call evidence.
        try:
            if getattr(transport,'tracks_send_call',False):
                accepted = transport.submit_tracked(row['packet'],key,before_send)
            else:
                accepted = transport.submit(row["packet"], key)
            require(isinstance(accepted, Submission) and accepted.locator.strip(), "Invalid acceptance; outcome unknown")
        except NotSent as exc:
            # A misbehaving adapter can never undo a persisted send boundary.
            crossed=any(e['action']=='send-call' for e in self.events(item))
            self._outcome(item, 'send-unknown' if crossed else 'not-sent',
                          'unknown' if crossed else 'not-sent', (str(exc) or type(exc).__name__)[:300])
        except DefiniteRejection as exc:
            crossed=any(e['action']=='send-call' for e in self.events(item))
            self._outcome(item, 'send-unknown' if crossed else 'approved',
                          'unknown' if crossed else 'rejected', (str(exc) or type(exc).__name__)[:300])
        except Exception as exc:
            pre_send=(getattr(transport,'tracks_send_call',False)
                      and not any(e['action']=='send-call' for e in self.events(item)))
            self._outcome(item, 'not-sent' if pre_send else 'send-unknown',
                          'not-sent' if pre_send else 'unknown', (str(exc) or type(exc).__name__)[:300])
        else:
            self._outcome(item, "sent", "accepted", accepted.locator)
        return self.get(item)

    def _outcome(self, item, state, status, evidence):
        with self._tx():
            # A concurrent reconciler may already have verified the original attempt.
            current = self.get(item)["state"]
            if current == "verified":
                return
            require(current in {"sending", "send-unknown", "sent"}, "Invalid attempt outcome")
            self.db.execute("UPDATE items SET state=?,reason=? WHERE id=?", (state, evidence, item))
            self.db.execute("UPDATE attempts SET status=?,locator=? WHERE item=?", (status, evidence, item))
            self._event(item, state, {"status": status, "evidence": evidence})

    def reconcile(self, item, transport: Transport):
        # Read-only transport methods only; cleanup and dispatch are separate.
        row = self.get(item)
        if row["state"] == "verified":
            return row
        require(row["state"] in {"sending", "send-unknown", "sent", "not-sent"}, "No original attempt to reconcile")
        attempt = self.db.execute("SELECT * FROM attempts WHERE item=?", (item,)).fetchone()
        require(attempt and attempt["transport"] == transport.name and transport.kind == row["kind"], "Wrong original transport")
        observed = transport.lookup(row["packet"], attempt["key"])
        if observed is None:
            events=self.events(item)
            intent=next(e for e in reversed(events) if e['action']=='send-intent')
            crossed=any(e['action']=='send-call' for e in events)
            tracked=json.loads(intent['data']).get('tracks_send_call') is True
            evidence=None
            if not crossed and row['state'] in {'sending','send-unknown'}:
                if tracked:
                    evidence='Draft preparation interrupted before /send; no send-call boundary recorded'
                elif row['state']=='send-unknown' and attempt['locator']=='GateError':
                    # Legacy Graph submit checked exact content/conversation
                    # before /send. Only its unchanged draft in a DIFFERENT
                    # conversation proves that that gate prevented the call.
                    check=getattr(transport,'legacy_not_sent',None)
                    if callable(check):evidence=check(row['packet'],attempt['key'])
            if evidence:
                self._outcome(item,'not-sent','not-sent',evidence[:300])
                self.checkpoint()
                return self.get(item)
            # A crash may have left sending. Absence does NOT prove no effect.
            if row["state"] == "sending":
                self._outcome(item, "send-unknown", "unknown", "No readback; original outcome unknown")
            return self.get(item)
        require(isinstance(observed, Readback) and observed.idempotency_key == attempt["key"]
                and observed.content_hash == row["content_hash"]
                and observed.remote_id.strip() and observed.evidence_ref.strip(), "Readback does not match approved content")
        with self._tx():
            current = self.get(item)
            require(current["state"] in {"sending", "send-unknown", "sent", "not-sent", "verified"}, "Attempt state changed")
            self.db.execute("UPDATE items SET state='verified',reason='' WHERE id=?", (item,))
            self.db.execute("UPDATE attempts SET status='verified',readback=? WHERE item=?", (canonical(observed.__dict__), item))
            self._event(item, "verified", observed.__dict__)
        return self.get(item)

    def _cleaned(self, item, calls):
        return any(e['action']=='draft-cleaned' and json.loads(e['data']).get('calls')==calls
                   for e in self.events(item))

    def cleanup_not_sent(self, item, transport):
        row=self.get(item)
        attempt=self.db.execute('SELECT * FROM attempts WHERE item=?',(item,)).fetchone()
        require(row['state']=='not-sent' and attempt and attempt['status']=='not-sent'
                and attempt['transport']==transport.name and transport.kind==row['kind'],
                'Only a proven not-sent attempt permits draft cleanup')
        require(not any(e['action']=='send-call' for e in self.events(item)),
                'An attempt that reached send cannot be cleaned up for retry')
        if self._cleaned(item,attempt['calls']):return
        self.checkpoint()  # Preserve the not-sent proof before any deletion.
        evidence=transport.discard_draft(row['packet'],attempt['key'])
        with self._tx():
            self._event(item,'draft-cleaned',{'calls':attempt['calls'],'evidence':evidence})
        self.checkpoint()

    def events(self, item):
        return [dict(r) for r in self.db.execute("SELECT * FROM events WHERE item=? ORDER BY seq", (item,))]
