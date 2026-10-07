# Detection and correctness notes

## Decision and scoring

The score is `20 + 25 anonymous + 20 revert hint + min(abs(byte delta)/100, 25) - 10 bot`, capped at 100. It is intentionally interpretable.

"Anonymous" means a logged-out editor: an IPv4 address, an IPv6 address (which always contains `:`), or a temporary account such as `~2026-54044-77`. English Wikipedia replaced IP display with temporary accounts, and the live capture on 2026-10-07 showed them throughout the queue. An earlier pattern matched only IPs (and treated hex-only usernames such as "Cafe" as IPv6), so the anonymous signal never fired on enwiki. The fix is applied identically in `events.py`, the Spark expression and the TypeScript port, and covered by parity tests. No ground-truth vandalism labels or model calibration are supplied, so precision, recall and claims about improving patrol outcomes would be unjustified. The queue surfaces a decision candidate and links to the original page for human review.

Revert hints are regex matches on edit summaries. Summaries can be empty, translated, misleading or legitimately mention the word “revert”. Therefore alert names describe *patterns* rather than confirmed bad edits. A future evaluation should derive confirmed revert links from revision events, wait for the label horizon, use a time split, and compare against Wikimedia's [revertrisk](https://wikitech.wikimedia.org/wiki/Machine_Learning/LiftWing) at equal review volume. Do not use later reverts as features when scoring earlier edits.

## Delivery semantics

The SSE source does not participate in Kafka transactions. `Last-Event-ID` is stored after Kafka acknowledgement, so a crash can cause replay. Broker producer idempotence prevents retries within a producer session from creating broker duplicates, but it cannot dedupe a new session after an SSE replay. The Spark silver watermark dedupes within its bounded state, while the Iceberg `MERGE` on immutable `event_id` makes a replay idempotent even after the watermark state is gone. Bronze is keyed by Kafka coordinates, so SSE replay can appear as a second raw Kafka record by design. The raw table is an audit of deliveries, not unique edits.

Each Iceberg `MERGE` is atomic at the table level. A silver batch can commit, then the process can fail before its alert MERGE or Spark checkpoint advances. On restart, silver's keyed MERGE is a no-op and alert computation is retried. This is *effectively once per keyed row* under a single writer and stable keys. There is no atomic commit spanning bronze, silver, alerts and the serving snapshot. Consumers should tolerate short-lived layer skew. The local Hadoop catalog is not a production multi-writer commit coordinator.

## Event time and late records

The 10-minute watermark bounds Spark's duplicate state. It follows the maximum observed event time, not wall clock. A record older than the current watermark can be discarded by the stateful operator, so it will remain only in bronze. The dashboard's `late_rows_dropped` is the latest microbatch counter, not an all-time total. A stricter 2-minute watermark shrinks state but loses more delayed edits; a 1-hour watermark retains more edits at higher state and recovery cost. Because the producer can reconnect after an outage, replayed records may exceed the 10-minute horizon. A separate offline backfill from bronze is needed to restore historical silver completeness without rewriting the streaming checkpoint.

Alerts use a five-minute event-time lookback and include only touched pages, reducing scans. Arrivals inside the watermark can change the alert count or create an alert later than its event timestamp. Alert IDs include the page, type and minute bucket. They are stable for retries of the same bucket, while successive buckets intentionally create separate observations. The dashboard shows the latest observations; it does not claim a globally consistent exactly-once alert count.

## Schema evolution

The raw payload is the compatibility boundary. Optional source additions are retained in bronze and ignored by an older silver parser. Required-field disappearance is a breaking change: validation sends malformed edits to `wiki.edits.dlq`, with the original payload and reason. The fixture includes a later `$schema` URI and an extra field to exercise the additive case. The Iceberg migration adds a nullable `analyst_note` to alerts; explicit named columns let existing writes continue. State schema changes in `dropDuplicatesWithinWatermark` may require a new checkpoint directory and controlled backfill. Reusing an incompatible checkpoint is unsafe.

## Operational tradeoffs

- Ten-second triggers reduce visible latency but create small Iceberg files and frequent MERGEs. Periodic Iceberg `rewrite_data_files` compaction is appropriate after a longer capture; do not compact concurrently with the local Hadoop writer.
- `maxOffsetsPerTrigger=2000` bounds per-batch load. During a high-volume backlog, this increases catch-up time. Compare Kafka end offsets with Spark progress before tuning it.
- The dashboard snapshot uses Spark driver `collect()` only for a bounded 60 edits and 40 alerts. Larger serving workloads need a query engine or materialized service, not an unbounded driver export.
- `WIKI_FILTER=enwiki` keeps the laptop capture manageable. It can be removed, but the queue and thresholds then mix languages and very different traffic baselines.

## Browser demo engine

`web/src/core/engine.ts` reproduces the silver and alert semantics at browser scale so the public demo needs no backend: arrivals are buffered and committed every 10 seconds; the dedupe set is bounded by a 10-minute event-time watermark computed from *previous* batches (as in Spark); records below it are counted as late and dropped; alerts are recomputed for touched pages over a five-minute lookback and keyed by page, minute and kind. Differences from the pipeline: no bronze audit table, no durability (state is in memory and covers the last 15 minutes), and the dedupe state is not checkpointed, so a page reload starts fresh. Recorded-capture mode replays on a virtual event clock, so faster playback does not compress the five-minute windows and inflate burst alerts. Simulated hard-case events are flagged, labelled in the UI, and never linked to Wikipedia.
