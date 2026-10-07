# Verification record — 2026-10-07

Environment: Docker Desktop 29.2.1 on Apple Silicon; Spark 4.0.4 container with Java 17; Kafka 4.0.0; Iceberg 1.10.2. The project was started with `docker compose up -d --build` and connected to Wikimedia EventStreams. The dashboard returned HTTP 200, `/api/health` reported `live`, and Iceberg bronze/silver files and Spark checkpoints appeared on disk.

Offline checks: `uv run --extra dev pytest -q` reported **6 passed**; `uv run --extra dev ruff check .` reported **all checks passed**.

The controlled replay used four fixture-shaped, current-time edits on one synthetic page, sent twice to Kafka. The read-only Iceberg verifier reported:

```text
silver=4 bronze=8 alerts=1
smoke-20261007-1 through -4: each count=1 in silver
alert: edit_war, edits=4, editors=2, revert_hints=3
```

This demonstrates the intended distinction: bronze audits eight broker deliveries, while silver and the alert table converge on keyed unique records. It does not prove a distributed exactly-once transaction across tables.

A separate record one hour older than the stream watermark produced `bronze=1`, `silver=0`, and `late_rows_dropped=1` in the relevant Spark progress batch. The metric is per batch; it returned to zero in later batches.

`make schema-demo` paused Spark, added nullable `analyst_note` to `lake.wiki.alerts`, read the existing alert with `analyst_note=NULL`, and restarted Spark. `/api/health` again reported live with new committed batches. The synthetic replay data was then reset, leaving a clean live capture for the dashboard.

The dashboard was opened and inspected in a browser at a narrow viewport. It showed live status, current batch volume, ranked edits, the injected edit-war alert, and a throughput chart. A timezone display bug found during this check was fixed before the final clean restart.

## Addendum — browser demo and scorer fix (2026-10-07, later)

- Live capture showed enwiki logged-out editors as temporary accounts (`~2026-…`), all scoring 20 (no anonymous points). After the fix and a Spark restart from the existing checkpoints, batch 310's snapshot scored those edits 45–49. `uv run --extra dev pytest -q`: **13 passed**; ruff clean.
- `make capture` exported 4,060 enwiki edits spanning 42.5 minutes from `wiki.edits.raw`. `npm --prefix web test`: **11 passed**, including 1,517 Python-generated parity vectors with zero mismatches.
- In a browser against live EventStreams: the queue populated within one 10-second commit; "Stage an edit war" produced one simulated `edit_war` alert (4 edits, 2 editors, 3 revert hints); "Re-deliver the last batch" counted 20 duplicates with no new queue rows; "Send a late edit" counted 1 late drop. Recorded-capture and Local-pipeline modes rendered; no console errors; no horizontal overflow at 375 px.
