"""Offline failure/regression tests. No live integrations or third-party packages."""
import base64
import copy
import json
from pathlib import Path
import tempfile
import threading
import unittest

from fixture_config import use_example_config
from managed.queue import (Authority, GateError, Queue, digest, load_workflow_digests,
                           validate_packet, workflow_digest)
from managed.transports import (AppleMailAdapter, DefiniteRejection, NyxIDGraphAdapter,
                                Readback, Submission)

FIXTURES = Path(__file__).parent / "fixtures"
POLICIES = {"firstcontact": "1" * 64, "daily": "2" * 64, "reply": "3" * 64}


class FakeTransport:
    """Captures exact wire intent, can simulate late success or certain rejection."""
    name = "fake-mail"
    kind = "firstcontact"

    def __init__(self, failure=None):
        self.failure = failure
        self.submissions = []
        self.lookups = []
        self.readback = None

    def submit(self, packet, key):
        self.submissions.append((copy.deepcopy(packet), key))
        if self.failure:
            raise self.failure
        return Submission("synthetic-acceptance")

    def lookup(self, packet, key):
        self.lookups.append((packet, key))
        return self.readback

    def observe(self, index=-1):
        packet, key = self.submissions[index]
        self.readback = Readback(key, digest(packet), "synthetic-message-id", "fixtures/observed-mime")


class QueueTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix=".test-", dir=FIXTURES.parents[1])
        self.path = Path(self.tmp.name) / "queue.sqlite3"
        self.now = 1000.0
        self.q = Queue(self.path, POLICIES, clock=lambda: self.now)
        self.letter = json.loads((FIXTURES / "letter.json").read_text())
        self.daily = json.loads((FIXTURES / "daily.json").read_text())
        self.transport = FakeTransport()
        self.policy()

    def tearDown(self):
        self.q.close()
        self.tmp.cleanup()

    def policy(self, contacts=None, revision=1, kind="firstcontact", **changes):
        snapshot = {"checked_at": self.now, "expires_at": self.now + 300,
                    "complete": True, "paused": False,
                    "contacts": contacts if contacts is not None else {"author@example.invalid": "clear"}}
        snapshot.update(changes)
        self.q.set_policy(kind, revision, snapshot)

    def authority(self, kind="firstcontact"):
        account = self.letter["sender"] if kind in {"firstcontact", "reply"} else self.daily["account"]
        return Authority(kind, account, self.now + 300, "fixtures/explicit-user-authority")

    def draft(self, item="a", packet=None):
        return self.q.add(item, packet or self.letter)

    def checked(self, item="a", packet=None):
        self.draft(item, packet)
        self.q.research_checked(self.q.binding(item), "fixtures/research-report")
        return self.q.binding(item)

    def envelope(self, item="a", verdict="PASS"):
        return {"schema_version": 1, **self.q.binding(item), "reviewer": "claude-loop",
                "verdict": verdict, "reason": "Synthetic independent review", "evidence_ref": "fixtures/claude-report"}

    def approve(self, item="a", packet=None):
        self.checked(item, packet)
        self.q.enqueue(self.q.binding(item))
        verdict = self.envelope(item)
        self.q.verdict(verdict)
        return verdict

    def test_complete_lifecycle_and_verification_is_idempotent(self):
        self.approve()
        self.assertEqual(self.q.dispatch("a", self.transport, self.authority())["state"], "sent")
        self.assertEqual(self.transport.submissions[0][0], self.letter)
        self.assertEqual(self.q.reconcile("a", self.transport)["state"], "sent")
        self.transport.observe()
        self.assertEqual(self.q.reconcile("a", self.transport)["state"], "verified")
        self.q.reconcile("a", self.transport)
        with self.assertRaises(GateError):
            self.q.dispatch("a", self.transport, self.authority())
        self.assertEqual(len(self.transport.submissions), 1)
        actions = [e["action"] for e in self.q.events("a")]
        self.assertEqual(actions, ["drafted", "research-checked", "in-review", "approved", "send-intent", "sent", "verified"])

    def test_reply_with_user_decision_history_can_send_after_exact_pass(self):
        packet = {**self.letter, 'kind': 'reply', 'thread_id': 'thread',
                  'latest_inbound_message_id': '<latest>', 'user_decision_ever': True}
        self.transport.kind = 'reply'
        self.policy(kind='reply')
        self.approve(packet=packet)
        self.assertEqual(self.q.dispatch('a', self.transport, self.authority('reply'))['state'], 'sent')
        self.assertEqual(self.transport.submissions[0][0], packet)
        self.transport.observe()
        self.assertEqual(self.q.reconcile('a', self.transport)['state'], 'verified')

    def test_reply_history_still_requires_pass_fresh_clear_policy_and_authority(self):
        packet = {**self.letter, 'kind': 'reply', 'thread_id': 'thread',
                  'latest_inbound_message_id': '<latest>', 'user_decision_ever': True,
                  'cc': ['colleague@example.invalid']}
        contacts = {a: 'clear' for a in packet['to'] + packet['cc']}
        self.transport.kind = 'reply'
        self.policy(contacts, kind='reply')
        self.checked(packet=packet)
        with self.assertRaisesRegex(GateError, 'not approved'):
            self.q.dispatch('a', self.transport, self.authority('reply'))
        self.q.enqueue(self.q.binding('a'))
        with self.assertRaisesRegex(GateError, 'not approved'):
            self.q.dispatch('a', self.transport, self.authority('reply'))
        self.q.verdict(self.envelope())
        for revision, changes in enumerate([
                {'paused': True}, {'complete': False},
                {'contacts': {**contacts, packet['to'][0]: 'declined'}},
                {'contacts': {**contacts, packet['cc'][0]: 'unknown'}}], 2):
            with self.subTest(changes=changes):
                self.policy(kind='reply', revision=revision, **{'contacts': contacts, **changes})
                with self.assertRaises(GateError):
                    self.q.dispatch('a', self.transport, self.authority('reply'))
        self.policy(contacts, kind='reply', revision=6)
        with self.assertRaisesRegex(GateError, 'authority'):
            self.q.dispatch('a', self.transport, self.authority('firstcontact'))
        self.now += 301
        with self.assertRaisesRegex(GateError, 'stale'):
            self.q.dispatch('a', self.transport, self.authority('reply'))
        self.assertFalse(self.transport.submissions)

    def test_unreviewed_reconciliation_never_rebuilds_or_sends(self):
        self.draft()
        with self.assertRaises(GateError):
            self.q.reconcile("a", self.transport)
        with self.assertRaises(GateError):
            self.q.dispatch("a", self.transport, self.authority())
        self.assertEqual(self.transport.submissions, [])

    def test_research_cannot_be_skipped_or_claimed_without_evidence(self):
        self.draft()
        with self.assertRaises(GateError):
            self.q.enqueue(self.q.binding("a"))
        with self.assertRaises(GateError):
            self.q.research_checked(self.q.binding("a"), "")
        with self.assertRaises(GateError):
            self.q.verdict(self.envelope())

    def test_every_changed_wire_field_invalidates_old_approval(self):
        mutations = {
            "body": lambda p: p.update(body=p["body"] + "Changed.\n"),
            "subject": lambda p: p.update(subject="Changed subject"),
            "to": lambda p: p.update(to=["other@example.invalid"]),
            "cc": lambda p: p.update(cc=["other@example.invalid"]),
            "sender": lambda p: p.update(sender="other-sender@example.invalid"),
            "attachment_bytes": lambda p: p["attachments"][0].update(content_b64=base64.b64encode(b"changed").decode()),
            "attachment_name": lambda p: p["attachments"][0].update(name="different.txt"),
            "attachment_type": lambda p: p["attachments"][0].update(media_type="application/octet-stream")}
        for field, mutate in mutations.items():
            with self.subTest(field=field):
                item = "change-" + field
                old_verdict = self.approve(item)
                changed = copy.deepcopy(self.letter)
                mutate(changed)
                self.q.revise(item, changed)
                with self.assertRaises(GateError):
                    self.q.verdict(old_verdict)
                with self.assertRaises(GateError):
                    self.q.dispatch(item, self.transport, self.authority())
                self.assertEqual(self.q.get(item)["state"], "drafted")
        self.assertEqual(self.transport.submissions, [])

    def test_policy_change_rejects_unchanged_previously_approved_packet(self):
        self.approve()
        self.q.close()
        self.q = Queue(self.path, {**POLICIES, "firstcontact": "3" * 64}, clock=lambda: self.now)
        with self.assertRaises(GateError):
            self.q.dispatch("a", self.transport, self.authority())
        self.q.revise("a", self.letter)
        self.q.research_checked(self.q.binding("a"), "new-policy-research")
        self.q.enqueue(self.q.binding("a"))
        self.q.verdict(self.envelope())
        self.assertEqual(self.q.dispatch("a", self.transport, self.authority())["state"], "sent")

    def test_bounce_and_hold_never_send_and_release_requires_evidence(self):
        for decision, state in [("BOUNCE", "bounced"), ("HOLD", "held")]:
            self.checked(decision)
            self.q.enqueue(self.q.binding(decision))
            self.q.verdict(self.envelope(decision, decision))
            self.assertEqual(self.q.get(decision)["state"], state)
            with self.assertRaises(GateError):
                self.q.dispatch(decision, self.transport, self.authority())
        with self.assertRaises(GateError):
            self.q.revise("HOLD", self.letter)
        self.q.revise("HOLD", self.letter, "fixtures/user-release-and-corrected-evidence")
        self.q.revise("BOUNCE", self.letter)
        self.assertEqual(self.q.get("HOLD")["state"], "drafted")
        self.assertEqual(self.transport.submissions, [])

    def test_post_approval_user_hold_cannot_be_overridden_by_pass_replay(self):
        verdict = self.approve()
        self.q.hold("a", "User decision pending")
        self.q.verdict(verdict)  # Idempotent replay must not undo explicit hold.
        self.assertEqual(self.q.get("a")["state"], "held")
        with self.assertRaises(GateError):
            self.q.dispatch("a", self.transport, self.authority())

    def test_verdict_replay_is_idempotent_but_conflict_and_wrong_reviewer_refused(self):
        verdict = self.approve()
        self.q.verdict(verdict)
        self.assertEqual(sum(e["action"] == "approved" for e in self.q.events("a")), 1)
        for change in ({"verdict": "HOLD"}, {"reviewer": "codex"}, {"body": "rewritten"}, {"evidence_ref": ""}):
            with self.subTest(change=change), self.assertRaises(GateError):
                self.q.verdict({**verdict, **change})

    def test_complete_queue_survives_restart_and_includes_old_in_review(self):
        self.checked("older")
        self.q.enqueue(self.q.binding("older"))
        self.checked("newer")
        self.draft("not-researched")
        self.draft("held")
        self.q.hold("held", "Waiting for user decision")
        self.q.close()
        self.q = Queue(self.path, POLICIES, clock=lambda: self.now)
        self.assertEqual([r["id"] for r in self.q.pending_reviews()], ["older", "newer"])
        self.assertEqual([r["id"] for r in self.q.inventory()], ["older", "newer", "not-researched", "held"])
        self.q.verdict(self.envelope("older"))
        self.assertEqual([r["id"] for r in self.q.pending_reviews()], ["newer"])

    def test_intake_replay_cannot_silently_overwrite_content(self):
        self.draft()
        self.draft()
        self.assertEqual(len(self.q.events("a")), 1)
        changed = {**self.letter, "body": "Unreviewed replacement"}
        with self.assertRaises(GateError):
            self.q.add("a", changed)
        self.assertEqual(self.q.get("a")["packet"], self.letter)

    def test_declines_active_threads_unknown_cc_and_pause_are_send_time_gates(self):
        self.approve()
        for i, status in enumerate(["declined", "active-reply", "held", "unknown", "bounced-address"], 2):
            self.policy({"author@example.invalid": status}, i)
            with self.subTest(status=status), self.assertRaises(GateError):
                self.q.dispatch("a", self.transport, self.authority())
        self.policy({}, 8)
        with self.assertRaises(GateError):
            self.q.dispatch("a", self.transport, self.authority())
        self.policy(revision=9, paused=True)
        with self.assertRaises(GateError):
            self.q.dispatch("a", self.transport, self.authority())
        self.policy(revision=10, complete=False)
        with self.assertRaises(GateError):
            self.q.dispatch("a", self.transport, self.authority())
        cc_packet = {**self.letter, "cc": ["unresolved@example.invalid"]}
        self.approve("with-cc", cc_packet)
        self.policy(revision=11)
        with self.assertRaises(GateError):
            self.q.dispatch("with-cc", self.transport, self.authority())
        self.assertEqual(self.transport.submissions, [])

    def test_expired_policy_and_wrong_authority_cannot_dispatch(self):
        self.approve()
        for authority in [Authority("daily", self.letter["sender"], 2000, "ref"),
                          Authority("firstcontact", "wrong@example.invalid", 2000, "ref"),
                          Authority("firstcontact", self.letter["sender"], 999, "ref"),
                          Authority("firstcontact", self.letter["sender"], 2000, "")]:
            with self.subTest(authority=authority), self.assertRaises(GateError):
                self.q.dispatch("a", self.transport, authority)
        self.now += 301
        with self.assertRaises(GateError):
            self.q.dispatch("a", self.transport, self.authority())
        self.assertEqual(self.transport.submissions, [])

    def test_policy_revision_is_monotonic(self):
        with self.assertRaises(GateError):
            self.policy(revision=1)

    def test_overlap_between_bundle_and_single_letter_is_reserved_atomically(self):
        bundle = {**self.letter, "problem_ids": ["synthetic-question-a", "synthetic-question-b"]}
        self.approve("bundle", bundle)
        self.approve("single")
        self.q.dispatch("bundle", self.transport, self.authority())
        with self.assertRaises(GateError):
            self.q.dispatch("single", self.transport, self.authority())
        self.assertEqual(self.q.get("single")["state"], "approved")
        self.assertEqual(len(self.transport.submissions), 1)

    def test_two_connections_cannot_dispatch_overlapping_results(self):
        self.approve("a")
        self.approve("b", {**self.letter, "subject": "Different subject"})
        barrier = threading.Barrier(2)
        outcomes = []

        def attempt(item):
            q = Queue(self.path, POLICIES, clock=lambda: self.now)
            try:
                barrier.wait(timeout=5)
                outcomes.append(q.dispatch(item, self.transport, self.authority())["state"])
            except GateError:
                outcomes.append("refused")
            finally:
                q.close()

        threads = [threading.Thread(target=attempt, args=(item,)) for item in ("a", "b")]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)
            self.assertFalse(thread.is_alive())
        self.assertCountEqual(outcomes, ["sent", "refused"])
        self.assertEqual(len(self.transport.submissions), 1)

    def test_timeout_never_retries_or_fails_over_and_can_reconcile_late_success(self):
        self.approve()
        self.transport.failure = TimeoutError("lost response after request")
        self.assertEqual(self.q.dispatch("a", self.transport, self.authority())["state"], "send-unknown")
        self.assertEqual(self.q.reconcile("a", self.transport)["state"], "send-unknown")
        for adapter in [self.transport, AppleMailAdapter(), NyxIDGraphAdapter()]:
            with self.assertRaises(GateError):
                self.q.dispatch("a", adapter, self.authority())
        self.transport.observe()
        self.assertEqual(self.q.reconcile("a", self.transport)["state"], "verified")
        self.assertEqual(len(self.transport.submissions), 1)

    def test_process_crash_leaves_durable_attempt_and_recovery_never_submits(self):
        class SimulatedProcessDeath(BaseException):
            pass
        self.approve()
        self.transport.failure = SimulatedProcessDeath()
        with self.assertRaises(SimulatedProcessDeath):
            self.q.dispatch("a", self.transport, self.authority())
        self.q.close()
        self.q = Queue(self.path, POLICIES, clock=lambda: self.now)
        self.assertEqual(self.q.get("a")["state"], "sending")
        self.assertEqual(self.q.reconcile("a", self.transport)["state"], "send-unknown")
        self.transport.observe()
        self.assertEqual(self.q.reconcile("a", self.transport)["state"], "verified")
        self.assertEqual(len(self.transport.submissions), 1)

    def test_wrong_body_key_or_attachment_evidence_cannot_verify(self):
        self.approve()
        self.q.dispatch("a", self.transport, self.authority())
        _, key = self.transport.submissions[0]
        for observed in [Readback("wrong", digest(self.letter), "id", "ref"),
                         Readback(key, "0" * 64, "id", "ref"),
                         Readback(key, digest(self.letter), "", "ref"),
                         Readback(key, digest(self.letter), "id", "")]:
            self.transport.readback = observed
            with self.subTest(observed=observed), self.assertRaises(GateError):
                self.q.reconcile("a", self.transport)
            self.assertEqual(self.q.get("a")["state"], "sent")
        self.assertEqual(len(self.transport.submissions), 1)

    def test_definite_no_effect_retry_is_bounded_and_uses_original_key(self):
        self.approve()
        self.transport.failure = DefiniteRejection("fixture confirms no effect")
        for _ in range(3):
            self.assertEqual(self.q.dispatch("a", self.transport, self.authority())["state"], "approved")
        with self.assertRaises(GateError):
            self.q.dispatch("a", self.transport, self.authority())
        self.assertEqual(len({key for _, key in self.transport.submissions}), 1)
        with self.assertRaises(GateError):
            self.q.revise("a", self.letter)

    def test_daily_coverage_prevents_merged_and_standalone_double_post(self):
        self.policy({self.daily["account"]: "clear"}, kind="daily")
        transport = FakeTransport()
        transport.name, transport.kind = "fake-x", "daily"
        merged = {**self.daily, "covered_days": ["2026-10-28", "2026-10-29"]}
        self.approve("merged", merged)
        self.approve("standalone", self.daily)
        with self.assertRaises(GateError):
            self.q.dispatch("merged", transport, self.authority())  # Mail authority is not daily authority.
        self.q.dispatch("merged", transport, self.authority("daily"))
        with self.assertRaises(GateError):
            self.q.dispatch("standalone", transport, self.authority("daily"))
        transport.observe()
        self.assertEqual(self.q.reconcile("merged", transport)["state"], "verified")
        self.assertEqual(len(transport.submissions), 1)

    def test_daily_text_media_evidence_and_date_edits_invalidate_approval(self):
        for field, value in [("text", "Edited headline"), ("evidence_fingerprint", "b" * 64),
                             ("covered_days", ["2026-10-29"]), ("account", "other-account"),
                             ("media", [])]:
            item = "daily-" + field
            old = self.approve(item, self.daily)
            self.q.revise(item, {**self.daily, field: value})
            with self.subTest(field=field), self.assertRaises(GateError):
                self.q.verdict(old)

    def test_malformed_packets_are_rejected(self):
        for changed in [{**self.letter, "bcc": ["hidden@example.invalid"]},
                        {**self.letter, "subject": "Header\r\nBcc: x"},
                        {**self.letter, "to": ["Author@example.invalid"]},
                        {**self.letter, "cc": self.letter["to"]},
                        {**self.letter, "attachments": [{"name": "../note", "media_type": "text/plain", "content_b64": "AA=="}]},
                        {**self.letter, "attachments": [{"name": "note", "media_type": "text/plain", "content_b64": "invalid!!"}]},
                        {**self.daily, "covered_days": ["2026-10-28", "2026-10-30"]},
                        {**self.daily, "covered_days": ["2026-10-29", "2026-10-28"]}]:
            with self.subTest(changed=changed), self.assertRaises(GateError):
                validate_packet(changed)

    def test_transport_stubs_make_no_live_calls(self):
        for adapter in [AppleMailAdapter(), NyxIDGraphAdapter()]:
            with self.subTest(adapter=adapter.name):
                with self.assertRaises(NotImplementedError):
                    adapter.submit(self.letter, "synthetic-key")
                with self.assertRaises(NotImplementedError):
                    adapter.lookup(self.letter, "synthetic-key")


class WorkflowArtifactTests(unittest.TestCase):
    def test_shared_review_edit_invalidates_both_workflow_digests(self):
        root = Path(__file__).resolve().parents[1]
        original = load_workflow_digests(root)
        with tempfile.TemporaryDirectory(prefix=".test-", dir=root) as directory:
            copy_root = Path(directory)
            for relative in ["firstcontact/workflow.json", "firstcontact/review.md",
                             "daily/workflow.json", "daily/DESIGN-notes.md",
                             "replies/workflow.json", "replies/review.md"]:
                destination = copy_root / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes((root / relative).read_bytes())
            self.assertEqual(load_workflow_digests(copy_root), original)
            review = copy_root / "firstcontact/review.md"
            review.write_text(review.read_text() + "\nNew review requirement.\n")
            changed = load_workflow_digests(copy_root)
            for kind in ["firstcontact", "daily"]:
                self.assertNotEqual(changed[kind], original[kind])

    def test_versioned_workflows_and_policy_hash_include_review(self):
        root = Path(__file__).resolve().parents[1]
        for kind, review in [("firstcontact", "review.md"), ("daily", "DESIGN-notes.md")]:
            workflow = json.loads((root / kind / "workflow.json").read_text())
            review_text = (root / kind / review).read_text()
            self.assertEqual(workflow["status"], "foundation_paused")
            self.assertRegex(workflow["version"], r"^\d+\.\d+\.\d+$")
            self.assertNotEqual(workflow_digest(workflow, review_text), workflow_digest(workflow, review_text + "changed rule"))
        first = json.loads((root / "firstcontact/workflow.json").read_text())
        self.assertEqual(first["signature_exact"], "[configured firstcontact_signature]")


if __name__ == "__main__":
    unittest.main()


def setUpModule():
    use_example_config()
