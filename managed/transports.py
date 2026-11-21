"""Transport boundary. No network, subprocess, AppleEvents or credentials here."""
from dataclasses import dataclass
from typing import Optional, Protocol


class DefiniteRejection(Exception):
    """Adapter has positive evidence the request produced NO external effect.

    Never use this for a timeout, missing Sent item, ambiguous status or 5xx.
    """


class NotSent(Exception):
    """Draft preparation failed before the adapter could call send.

    A draft may exist. It must be reconciled and safely removed before retry.
    Never raise this once the send-call boundary has been crossed.
    """


@dataclass(frozen=True)
class Submission:
    locator: str  # Acceptance/correlation locator, not verified delivery.


@dataclass(frozen=True)
class Readback:
    idempotency_key: str
    content_hash: str  # Reconstructed from actual observed fields/attachment bytes.
    remote_id: str
    evidence_ref: str
    at: str = ""  # Actual server timestamp when the adapter can establish it.


class Transport(Protocol):
    name: str
    kind: str

    def submit(self, packet: dict, idempotency_key: str) -> Submission: ...

    def lookup(self, packet: dict, idempotency_key: str) -> Optional[Readback]: ...


class AppleMailAdapter:
    """Future guarded local fallback; do not invoke legacy batch/reconcile scripts.

    Requires shared ownership lock, exact composer readback and Sent MIME evidence.
    Mutable Mail IDs and subject-only matches cannot implement this contract.
    """
    name = "apple-mail-stub"
    kind = "firstcontact"

    def submit(self, packet: dict, idempotency_key: str) -> Submission:
        raise NotImplementedError("Design only: Apple Mail access is disabled")

    def lookup(self, packet: dict, idempotency_key: str) -> Optional[Readback]:
        raise NotImplementedError("Design only: Apple Mail access is disabled")


class NyxIDGraphAdapter:
    """Future Exchange adapter. No mailbox service has been provisioned.

    Graph vs EWS access, consent and correlation/readback semantics are unresolved.
    A 202 must not be reported as a verified Sent copy.
    """
    name = "nyxid-graph-stub"
    kind = "firstcontact"

    def submit(self, packet: dict, idempotency_key: str) -> Submission:
        raise NotImplementedError("Design only: NyxID mailbox transport is unconfigured")

    def lookup(self, packet: dict, idempotency_key: str) -> Optional[Readback]:
        raise NotImplementedError("Design only: NyxID mailbox transport is unconfigured")
