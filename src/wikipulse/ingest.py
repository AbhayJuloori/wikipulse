"""SSE ingestion with acknowledged Kafka writes and a persisted resume cursor."""

from __future__ import annotations

import json
import logging
import os
import random
import time
from pathlib import Path

import httpx
from confluent_kafka import Producer

from wikipulse.events import InvalidEvent, parse_edit

LOG = logging.getLogger(__name__)
RAW_TOPIC = "wiki.edits.raw"
DLQ_TOPIC = "wiki.edits.dlq"


def sse_messages(lines):
    """Yield (SSE id, data) and ignore heartbeats; no SSE library dependency."""
    event_id = None
    data = []
    for line in lines:
        line = line.rstrip("\r\n")
        if not line:
            if data:
                yield event_id, "\n".join(data)
            event_id, data = None, []
        elif line.startswith("id:"):
            event_id = line[3:].strip()
        elif line.startswith("data:"):
            data.append(line[5:].lstrip())


def publish_payload(producer: Producer, payload: str, event_id: str | None, wiki_filter: str = ""):
    """Wait for a broker acknowledgement before advancing the SSE cursor."""
    try:
        edit = parse_edit(payload)
    except (InvalidEvent, json.JSONDecodeError) as exc:
        dlq = json.dumps({"reason": str(exc), "sse_id": event_id, "payload": payload})
        delivery_error = []

        def dlq_delivered(err, _msg):
            if err:
                delivery_error.append(err)

        producer.produce(
            DLQ_TOPIC,
            key=(event_id or "unknown").encode(),
            value=dlq.encode(),
            callback=dlq_delivered,
        )
        outstanding = producer.flush(10)
        if outstanding or delivery_error:
            raise RuntimeError(
                f"DLQ acknowledgement failed: {delivery_error or outstanding}"
            ) from exc
        return "dlq"
    if edit is None or (wiki_filter and edit.wiki != wiki_filter):
        return "skipped"
    delivery_error = []

    def delivered(err, _msg):
        if err:
            delivery_error.append(err)

    producer.produce(
        RAW_TOPIC, key=edit.kafka_key.encode(), value=payload.encode(), callback=delivered
    )
    outstanding = producer.flush(10)
    if outstanding or delivery_error:
        raise RuntimeError(f"Kafka acknowledgement failed: {delivery_error or outstanding}")
    return "published"


def ingest_forever():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    data_dir = Path(os.getenv("DATA_DIR", "data"))
    data_dir.mkdir(parents=True, exist_ok=True)
    cursor_file = data_dir / "last_event_id"
    producer = Producer(
        {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
            "acks": "all",
            "enable.idempotence": True,
        }
    )
    url = os.getenv("SSE_URL", "https://stream.wikimedia.org/v2/stream/recentchange")
    user_agent = os.getenv(
        "WIKIPULSE_USER_AGENT", "WikiPulse/0.1 (portfolio demo; contact: wikipulse@example.com)"
    )
    wiki_filter = os.getenv("WIKI_FILTER", "enwiki")
    delay = 1.0
    while True:
        headers = {"Accept": "text/event-stream", "User-Agent": user_agent}
        if cursor_file.exists():
            headers["Last-Event-ID"] = cursor_file.read_text().strip()
        try:
            with httpx.Client(
                timeout=httpx.Timeout(15, read=None), follow_redirects=True
            ) as client:
                with client.stream("GET", url, headers=headers) as response:
                    response.raise_for_status()
                    LOG.info("connected to %s, cursor=%s", url, bool(headers.get("Last-Event-ID")))
                    delay = 1.0
                    for event_id, payload in sse_messages(response.iter_lines()):
                        result = publish_payload(producer, payload, event_id, wiki_filter)
                        if event_id:
                            temp = cursor_file.with_suffix(".tmp")
                            temp.write_text(event_id)
                            temp.replace(cursor_file)
                        if result == "dlq":
                            LOG.warning("sent malformed event to DLQ")
        except (httpx.HTTPError, RuntimeError) as exc:
            LOG.warning("stream interrupted: %s; reconnecting", exc)
            time.sleep(delay + random.random())
            delay = min(delay * 2, 60)


def replay(path: str, retime: bool = False):
    producer = Producer(
        {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
            "acks": "all",
            "enable.idempotence": True,
        }
    )
    counts = {"published": 0, "skipped": 0, "dlq": 0}
    base_timestamp = None
    replay_start = int(time.time())
    with Path(path).open() as source:
        for line in source:
            payload = line.strip()
            if retime:
                try:
                    event = json.loads(payload)
                    if base_timestamp is None:
                        base_timestamp = int(event["timestamp"])
                    event["timestamp"] = replay_start + int(event["timestamp"]) - base_timestamp
                    payload = json.dumps(event)
                except (KeyError, TypeError, ValueError):
                    pass  # Malformed input still goes through the normal DLQ route.
            result = publish_payload(producer, payload, None, os.getenv("WIKI_FILTER", "enwiki"))
            counts[result] += 1
    print(json.dumps(counts))
