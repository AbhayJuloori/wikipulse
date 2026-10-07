"""Small, stable contract at the edge of the evolving Wikimedia event schema."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime


class InvalidEvent(ValueError):
    pass


@dataclass(frozen=True)
class Edit:
    event_id: str
    wiki: str
    page_id: int
    revision_id: int
    timestamp: int
    user: str
    title: str
    bot: bool
    comment: str
    old_length: int | None
    new_length: int | None
    schema_uri: str

    @property
    def kafka_key(self) -> str:
        return f"{self.wiki}:{self.page_id}"

    @property
    def event_time(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp, tz=UTC)


def parse_edit(payload: str | bytes | dict) -> Edit | None:
    """Return None for valid non-edit/canary events; raise for malformed edit records."""
    try:
        obj = json.loads(payload) if isinstance(payload, (str, bytes)) else payload
        if not isinstance(obj, dict):
            raise InvalidEvent("expected a JSON object")
        if obj.get("type") != "edit" or obj.get("meta", {}).get("domain") == "canary":
            return None
        meta = obj["meta"]
        event_id = str(meta["id"])
        wiki = str(obj["wiki"])
        page_id = int(obj["id"])
        revision_id = int(obj["revision"]["new"])
        timestamp = int(obj["timestamp"])
        user = str(obj["user"])
        title = str(obj["title"])
        if not event_id or not wiki or page_id <= 0 or revision_id <= 0 or not user:
            raise InvalidEvent("empty or invalid identity field")
        lengths = obj.get("length") or {}
        return Edit(
            event_id=event_id,
            wiki=wiki,
            page_id=page_id,
            revision_id=revision_id,
            timestamp=timestamp,
            user=user,
            title=title,
            bot=bool(obj.get("bot", False)),
            comment=str(obj.get("comment") or ""),
            old_length=int(lengths["old"]) if lengths.get("old") is not None else None,
            new_length=int(lengths["new"]) if lengths.get("new") is not None else None,
            schema_uri=str(obj.get("$schema") or "unknown"),
        )
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        if isinstance(exc, InvalidEvent):
            raise
        raise InvalidEvent(str(exc)) from exc


def is_revert_comment(comment: str) -> bool:
    """Conservative, language-limited operational proxy; never a ground-truth label."""
    return bool(
        re.search(r"\b(revert(?:ed|ing)?|undid|undo|rvv?|revertido|rückgängig)\b", comment, re.I)
    )


# Logged-out editors appear as IPv4, IPv6 (always contains ":"), or, on wikis with
# temporary accounts enabled (enwiki since 2025), names like "~2026-54044-77".
ANONYMOUS_USER = r"(?:\d{1,3}\.){3}\d{1,3}|[0-9a-fA-F]*:[0-9a-fA-F:]+|~\d{4}-\d+-\d+"


def is_anonymous(user: str) -> bool:
    return bool(re.fullmatch(ANONYMOUS_USER, user))


def risk_score(edit: Edit) -> int:
    """Transparent review ordering score, not a vandalism probability."""
    size_delta = abs((edit.new_length or 0) - (edit.old_length or 0))
    anonymous = is_anonymous(edit.user)
    return min(
        100,
        20
        + 25 * anonymous
        + 20 * is_revert_comment(edit.comment)
        + min(size_delta // 100, 25)
        - 10 * edit.bot,
    )
