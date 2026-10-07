"""Turn a raw Kafka dump into the demo's recorded capture and the TS/Python parity vectors.

Usage: python scripts/export_capture.py RAW.jsonl
  RAW.jsonl comes from `make capture` (kafka-console-consumer over wiki.edits.raw).
Writes web/public/sample/enwiki-capture.jsonl (contract fields only) and
web/tests/parity.fixture.json (expected parse/score results from wikipulse.events).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from wikipulse.events import InvalidEvent, is_anonymous, is_revert_comment, parse_edit, risk_score

ROOT = Path(__file__).resolve().parents[1]
CAPTURE = ROOT / "web/public/sample/enwiki-capture.jsonl"
PARITY = ROOT / "web/tests/parity.fixture.json"
FIXTURE = ROOT / "tests/fixtures/recentchange.jsonl"


def contract_fields(event: dict) -> dict:
    """Keep only what the contract reads, plus server_url for diff links."""
    keep = {
        key: event[key]
        for key in (
            "$schema",
            "id",
            "type",
            "wiki",
            "title",
            "title_url",
            "server_url",
            "timestamp",
            "user",
            "bot",
            "comment",
            "length",
        )
        if key in event
    }
    keep["meta"] = {k: event["meta"][k] for k in ("id", "domain") if k in event.get("meta", {})}
    keep["revision"] = {"new": event["revision"]["new"]}
    return keep


def expected(payload: dict) -> dict | str | None:
    try:
        edit = parse_edit(payload)
    except InvalidEvent:
        return "invalid"
    if edit is None:
        return None
    return {
        "eventId": edit.event_id,
        "wiki": edit.wiki,
        "pageId": edit.page_id,
        "revisionId": edit.revision_id,
        "timestamp": edit.timestamp,
        "user": edit.user,
        "bot": edit.bot,
        "oldLength": edit.old_length,
        "newLength": edit.new_length,
        "score": risk_score(edit),
        "revertHint": is_revert_comment(edit.comment),
        "anonymous": is_anonymous(edit.user),
    }


def edge_cases() -> list[dict]:
    base = json.loads(FIXTURE.read_text().splitlines()[0])

    def variant(**changes):
        event = json.loads(json.dumps(base))
        for key, value in changes.items():
            if value is None:
                event.pop(key, None)
            else:
                event[key] = value
        return event

    return [
        *(json.loads(line) for line in FIXTURE.read_text().splitlines()),
        variant(type="log"),
        variant(meta={"id": "canary-1", "domain": "canary"}),
        variant(revision=None),
        variant(meta={"id": "x-1", "domain": "en.wikipedia.org"}, id=0),
        variant(meta={"id": "x-2", "domain": "en.wikipedia.org"}, user="203.0.113.9"),
        variant(meta={"id": "x-3", "domain": "en.wikipedia.org"}, user="2001:db8::7"),
        variant(meta={"id": "x-4", "domain": "en.wikipedia.org"}, user="~2026-12345-67"),
        variant(meta={"id": "x-5", "domain": "en.wikipedia.org"}, user="Cafe"),
        variant(meta={"id": "x-6", "domain": "de.wikipedia.org"}, comment="Änderung rückgängig gemacht"),
        variant(meta={"id": "x-7", "domain": "en.wikipedia.org"}, comment="Reverted edits by X (rvv)"),
        variant(meta={"id": "x-8", "domain": "en.wikipedia.org"}, comment="prevert unrevertable"),
        variant(meta={"id": "x-9", "domain": "en.wikipedia.org"}, bot=True, length={"old": 0, "new": 90000}),
        variant(meta={"id": "x-10", "domain": "en.wikipedia.org"}, length=None, comment=None),
    ]


def main(raw_path: str) -> None:
    events = []
    for line in Path(raw_path).read_text().splitlines():
        try:
            event = json.loads(line)
            if parse_edit(event) is not None:
                events.append(contract_fields(event))
        except (ValueError, InvalidEvent):
            continue  # truncated final line of a dump, or a DLQ-worthy record
    events.sort(key=lambda event: event["timestamp"])
    CAPTURE.parent.mkdir(parents=True, exist_ok=True)
    CAPTURE.write_text("".join(json.dumps(e, ensure_ascii=False) + "\n" for e in events))

    vectors = [{"input": e, "expected": expected(e)} for e in edge_cases() + events[:1500]]
    PARITY.write_text(json.dumps(vectors, ensure_ascii=False))
    span = (events[-1]["timestamp"] - events[0]["timestamp"]) / 60 if events else 0
    print(f"capture: {len(events)} edits over {span:.1f} min -> {CAPTURE.relative_to(ROOT)}")
    print(f"parity: {len(vectors)} vectors -> {PARITY.relative_to(ROOT)}")


if __name__ == "__main__":
    main(sys.argv[1])
