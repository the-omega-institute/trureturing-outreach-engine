"""Read-only arXiv receipt parsing and conservative board reconciliation.

The mailbox is the source of submission evidence.  This module deliberately
does not send mail, submit a paper, or treat an arXiv ``submit/`` number as a
public paper identifier.  Callers may use :func:`reconcile` to produce a
small, reviewable board patch and :func:`apply_updates` after the public
record has been independently verified.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import date
from html import unescape
import re
from urllib.parse import urlsplit
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET

from .queue import require
from .location import settings

ARXIV_ID = re.compile(r"(?<![\w.])(?:arXiv\s*:\s*)?(\d{4}\.\d{4,5})(?:v([1-9]\d*))?", re.I)
ARXIV_URL = re.compile(r"https?://(?:www\.)?arxiv\.org/(?:abs|pdf|html)/(\d{4}\.\d{4,5})(?:v([1-9]\d*))?(?:\.pdf)?", re.I)
SUBMIT_ID = re.compile(r"\bsubmit/(\d{6,})\b", re.I)
EMAIL = re.compile(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}")
WORD = re.compile(r"[a-z0-9]+")
STOPWORDS = frozenset("a an and by for from in insertion input of on the to two with binary".split())


def _text(value):
    return " ".join(unescape(str(value or "")).replace("\r", "\n").split())


def _version(value):
    return "v" + value if value else None


def _public(arxiv_id, version=None):
    require(re.fullmatch(r"\d{4}\.\d{4,5}", arxiv_id), "Invalid arXiv identifier")
    return "https://arxiv.org/abs/" + arxiv_id + (version or "")


def _title_words(value):
    return {word for word in WORD.findall(_text(value).casefold()) if word not in STOPWORDS}


def _surname(value):
    words = WORD.findall(_text(value).casefold())
    return words[-1] if words else ""


def is_system_message(message):
    sender = str((message.get("from") or {}).get("emailAddress", {}).get("address", "")).casefold()
    subject = _text(message.get("subject"))
    body = _text(message.get("body", {}).get("content") if isinstance(message.get("body"), dict) else message.get("body"))
    host = sender.rsplit("@", 1)[-1] if "@" in sender else ""
    return (host in {"arxiv.org", "export.arxiv.org"} or
            ("arxiv" in subject.casefold() and bool(ARXIV_ID.search(subject + " " + body))))


def parse_message(message):
    """Extract a public arXiv identity and optional metadata from one message."""
    subject = _text(message.get("subject"))
    body_field = message.get("body") or message.get("uniqueBody")
    body_value = body_field.get("content") if isinstance(body_field, dict) else body_field
    body = _text(body_value)
    combined = subject + "\n" + body
    if not is_system_message({**message, "body": {"content": body}}) and not ARXIV_URL.search(combined):
        return None
    match = ARXIV_URL.search(combined)
    if match is None:
        match = ARXIV_ID.search(combined)
    result = {"submit_id": (SUBMIT_ID.search(combined).group(1) if SUBMIT_ID.search(combined) else None),
              "arxiv_id": match.group(1) if match else None,
              "arxiv_version": _version(match.group(2)) if match and match.group(2) else None,
              "title": None, "authors": [], "received_at": message.get("receivedDateTime")}
    title = re.search(r"(?im)^\s*title\s*:\s*(.+?)\s*$", body)
    authors = re.search(r"(?im)^\s*authors?\s*:\s*(.+?)\s*$", body)
    if title:
        result["title"] = _text(title.group(1))
    if authors:
        result["authors"] = [_text(value) for value in re.split(r",\s*(?=[A-Z][^,;]+(?:,|$))|;", authors.group(1)) if _text(value)]
    if result["arxiv_id"]:
        result["arxiv_url"] = _public(result["arxiv_id"], result["arxiv_version"])
    return result if result["arxiv_id"] or result["submit_id"] else None


def parse_api_response(raw):
    """Parse the small Atom response returned by export.arxiv.org."""
    root = ET.fromstring(raw)
    ns = {"a": "http://www.w3.org/2005/Atom"}
    entries = root.findall("a:entry", ns)
    require(len(entries) == 1, "arXiv API did not return exactly one record")
    entry = entries[0]
    ident = _text(entry.findtext("a:id", "", ns)).rstrip("/")
    match = re.search(r"/abs/(\d{4}\.\d{4,5})(v[1-9]\d*)?$", ident)
    require(match is not None, "arXiv API returned an invalid public identifier")
    authors = [_text(node.findtext("a:name", "", ns)) for node in entry.findall("a:author", ns)]
    published = _text(entry.findtext("a:published", "", ns))[:10]
    result = {"arxiv_id": match.group(1), "arxiv_version": match.group(2) or "v1",
              "arxiv_url": _public(match.group(1), match.group(2)),
              "title": _text(entry.findtext("a:title", "", ns)), "authors": authors,
              "abstract": _text(entry.findtext("a:summary", "", ns)),
              "date": date.fromisoformat(published).isoformat()}
    return result


def verify_public(receipt, fetch=None):
    """Verify a receipt against the public arXiv API; return metadata or ``None``."""
    if not receipt.get("arxiv_id"):
        return None
    fetch = fetch or (lambda url: urlopen(Request(url, headers={"User-Agent": "trureturing-outreach/1.0"}), timeout=20).read())
    raw = fetch("https://export.arxiv.org/api/query?id_list=" + receipt["arxiv_id"])
    record = parse_api_response(raw)
    if record["arxiv_id"] != receipt["arxiv_id"]:
        raise ValueError("arXiv API identity differs from the mailbox receipt")
    if receipt.get("arxiv_version") and int(record["arxiv_version"][1:]) < int(receipt["arxiv_version"][1:]):
        raise ValueError("arXiv API returned an older version than the receipt")
    return record


def _candidate(receipt, row):
    if receipt.get("arxiv_id") and row.get("arxiv_id") == receipt["arxiv_id"]:
        return 1.0
    if not receipt.get("title"):
        return 0.0
    authors = {_surname(value) for value in receipt.get("authors", []) if _surname(value)}
    expected = {_surname(value) for value in row.get("authors", []) if _surname(value)}
    if authors and not expected <= authors:
        return 0.0
    left, right = _title_words(receipt["title"]), _title_words(row["title"])
    if not left or not right:
        return 0.0
    return len(left & right) / max(1, min(len(left), len(right)))


def match(receipt, papers):
    """Return one exact candidate, or ``None``/a list when ambiguous."""
    scored = sorted(((score, row) for row in papers if (score := _candidate(receipt, row)) >= 0.55),
                    key=lambda item: (-item[0], item[1]["id"]))
    if not scored or (len(scored) > 1 and scored[0][0] == scored[1][0]):
        return None if not scored else [row["id"] for _, row in scored]
    return scored[0][1]


def reconcile(papers, messages, verify=None):
    """Build deterministic updates from arXiv messages without mutating input."""
    updates, ambiguous, ignored, publications = [], [], [], []
    for message in messages:
        receipt = parse_message(message)
        if receipt is None or not receipt.get("arxiv_id"):
            if receipt and receipt.get("submit_id"):
                ignored.append({"submit_id": receipt["submit_id"], "reason": "submission request is not public"})
            continue
        known = next((row for row in papers if row.get("arxiv_id") == receipt["arxiv_id"]), None)
        if known and known.get("arxiv_status") == "public":
            if not receipt.get("arxiv_version") or int(known.get("arxiv_version", "v0")[1:]) >= int(receipt["arxiv_version"][1:]):
                continue
        record = (verify or verify_public)(receipt)
        # A public coauthored paper is independently identifiable even when no
        # private submission-board row exists. Never infer a journal, workspace
        # or authorship agreement merely to make the paper discoverable.
        team = set(settings('identity')['team_authors'])
        if team and team <= set(record.get('authors', [])):
            publications.append(record)
        candidate = match({**receipt, **record}, papers)
        if isinstance(candidate, list):
            ambiguous.append({"receipt": receipt, "candidates": candidate})
        elif candidate is not None:
            updates.append({"paper_id": candidate["id"], "record": record, "message_ref": message.get("internetMessageId")})
        else:
            ignored.append({"arxiv_id": receipt["arxiv_id"], "reason": "no unique board match"})
    return {"updates": updates, "ambiguous": ambiguous, "ignored": ignored,
            "publications": publications}


def apply_updates(data, report):
    """Apply only verified, uniquely matched public records to a board copy."""
    result = deepcopy(data)
    by_id = {row["id"]: row for row in result["papers"]}
    for item in report.get("updates", []):
        row = by_id[item["paper_id"]]
        record = item["record"]
        row.update({"arxiv_id": record["arxiv_id"], "arxiv_url": record["arxiv_url"],
                    "arxiv_version": record["arxiv_version"], "arxiv_status": "public",
                    "arxiv_title": record["title"], "arxiv_date": record["date"],
                    "arxiv_evidence": record["arxiv_url"]})
        if 'blocker' in row:
            row["blocker"] = re.sub(r"arXiv v\d+ submitted[^.]*ID pending\.?\s*", "", row["blocker"], flags=re.I).strip()
    result["papers"] = [dict(sorted(row.items())) for row in result["papers"]]
    return result
