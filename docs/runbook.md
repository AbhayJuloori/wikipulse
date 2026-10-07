# Runbook

## Start and inspect

Copy `.env.example` to `.env`, set a real contact in `WIKIPULSE_USER_AGENT`, then run `make up`. `docker compose ps` should show Kafka, Spark, producer and API; `kafka-init` should exit 0. Open `http://localhost:8000/api/health`. The dashboard becomes live when silver commits a nonempty batch. Use `make logs` for stream and ingestion errors.

## Deterministic replay and duplicate check

For an isolated run, stop the producer and clear the Kafka, checkpoint and warehouse volumes only if you intend to reset all prior data. Run `make replay` twice from the host, with `KAFKA_BOOTSTRAP=localhost:9092`. The command shifts fixture timestamps into the current minute. A query through Spark should report four fixture event IDs in silver, each once:

```sql
SELECT event_id, COUNT(*) FROM lake.wiki.silver
WHERE event_id LIKE 'fixture-%' GROUP BY event_id ORDER BY event_id;
```

Bronze may have eight fixture deliveries, because its contract is Kafka delivery audit. The fourth fixture has an unfamiliar `future_optional_field`; verify it remains in bronze payload while silver still has `schema_uri=/mediawiki/recentchange/1.0.1`. Replay the fixture with records out of order to inspect watermark behavior. A strongly late record can be dropped from silver, visible through `/api/metrics` and still in bronze.

## Recovery

- Producer disconnect: it reconnects with backoff and the persisted `data/last_event_id`. The cursor is written after Kafka acknowledgement. Inspect `wiki.edits.dlq` for malformed events.
- Spark crash: `docker compose restart spark`. Keep `checkpoints/` and `warehouse/` together. Microbatches may replay; Iceberg MERGEs are keyed.
- Dashboard stale: compare `/api/health`, `/api/metrics`, `docker compose logs spark`, and the modification time of `data/dashboard.json`. A stale snapshot means no new successful silver export; do not infer zero edits.
- Breaking source schema: preserve bronze, inspect DLQ samples, extend the parser, deploy a versioned checkpoint if state schema changes, then backfill from bronze. Do not delete checkpoints to “fix” a stalled stream without a replay plan.

## Schema and maintenance

`make schema-demo` stops the Spark writer, adds a nullable `analyst_note` to the Iceberg alert table, prints a reader query, and restarts Spark. If the migration process fails, restart with `docker compose up -d spark`; the DDL is idempotent.

Iceberg compaction after a capture can be run from a Spark shell while the streaming writer is paused:

```sql
CALL lake.system.rewrite_data_files(table => 'wiki.silver');
CALL lake.system.rewrite_data_files(table => 'wiki.alerts');
```

Expire old snapshots only after choosing a retention period and confirming no reader needs them. Keep bronze and checkpoints for an audit or backfill.
