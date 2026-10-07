import json
from pathlib import Path

import wikipulse.ingest as ingest
from wikipulse.ingest import DLQ_TOPIC, RAW_TOPIC, publish_payload


class FakeProducer:
    def __init__(self):
        self.messages = []

    def produce(self, topic, key, value, callback=None):
        self.messages.append((topic, key, value))
        if callback:
            callback(None, None)

    def flush(self, timeout):
        return 0


def test_routing_and_dlq():
    producer = FakeProducer()
    event = {
        "meta": {"id": "a", "domain": "en.wikipedia.org"},
        "id": 42,
        "revision": {"new": 7},
        "type": "edit",
        "timestamp": 1791400000,
        "user": "Alice",
        "title": "Example",
        "wiki": "enwiki",
    }
    assert publish_payload(producer, json.dumps(event), "sse-1", "enwiki") == "published"
    assert producer.messages[0][:2] == (RAW_TOPIC, b"enwiki:42")
    assert publish_payload(producer, "{broken", "sse-2", "enwiki") == "dlq"
    assert producer.messages[1][0] == DLQ_TOPIC
    event["wiki"] = "dewiki"
    assert publish_payload(producer, json.dumps(event), "sse-3", "enwiki") == "skipped"


def test_replay_retimes_fixture_without_changing_event_id(monkeypatch, tmp_path: Path):
    producer = FakeProducer()
    monkeypatch.setattr(ingest, "Producer", lambda _config: producer)
    monkeypatch.setattr(ingest.time, "time", lambda: 2_000_000_000)
    fixture = Path(__file__).parent / "fixtures/recentchange.jsonl"
    target = tmp_path / "events.jsonl"
    target.write_text(fixture.read_text())
    ingest.replay(str(target), retime=True)
    first = json.loads(producer.messages[0][2])
    last = json.loads(producer.messages[-1][2])
    assert first["timestamp"] == 2_000_000_000
    assert last["timestamp"] == 2_000_000_030
    assert first["meta"]["id"] == "fixture-1"
