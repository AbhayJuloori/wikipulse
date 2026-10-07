import json
from pathlib import Path

import pytest

from wikipulse.events import (
    InvalidEvent,
    is_anonymous,
    is_revert_comment,
    parse_edit,
    risk_score,
)
from wikipulse.ingest import sse_messages

FIXTURE = Path(__file__).parent / "fixtures/recentchange.jsonl"


def test_fixture_contract_and_schema_addition():
    edits = [parse_edit(line) for line in FIXTURE.read_text().splitlines()]
    assert [edit.event_id for edit in edits] == [f"fixture-{n}" for n in range(1, 5)]
    assert edits[0].kafka_key == "enwiki:42"
    assert edits[3].schema_uri.endswith("1.0.1")
    assert edits[3].event_time.timestamp() == 1791400030


def test_missing_identity_goes_to_dlq():
    payload = json.loads(FIXTURE.read_text().splitlines()[0])
    del payload["revision"]
    with pytest.raises(InvalidEvent):
        parse_edit(payload)


def test_non_edits_and_canaries_are_ignored():
    payload = json.loads(FIXTURE.read_text().splitlines()[0])
    payload["type"] = "log"
    assert parse_edit(payload) is None
    payload["type"] = "edit"
    payload["meta"]["domain"] = "canary"
    assert parse_edit(payload) is None


def test_revert_hint_and_review_score_are_explainable():
    edits = [parse_edit(line) for line in FIXTURE.read_text().splitlines()]
    assert not is_revert_comment(edits[0].comment)
    assert is_revert_comment(edits[1].comment)
    assert risk_score(edits[1]) > risk_score(edits[0])


@pytest.mark.parametrize(
    "user, expected",
    [
        ("203.0.113.7", True),
        ("2001:db8::1", True),
        ("~2026-54044-77", True),  # temporary account, as enwiki now emits
        ("Ada", False),  # hex-only names are not IPv6
        ("Cafe", False),
        ("Smasongarrison", False),
    ],
)
def test_anonymous_editor_forms(user, expected):
    assert is_anonymous(user) is expected


def test_sse_cursor_and_multiline_payload():
    lines = [
        ":ok",
        "",
        "id: cursor-1",
        'data: {"x":',
        "data: 1}",
        "",
        "id: cursor-2",
        "data: two",
        "",
    ]
    assert list(sse_messages(lines)) == [("cursor-1", '{"x":\n1}'), ("cursor-2", "two")]
