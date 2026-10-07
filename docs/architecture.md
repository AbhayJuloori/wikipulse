# Architecture

```text
Wikimedia EventStreams (SSE)
     │ Last-Event-ID cursor; stable-field validation; DLQ
     ▼
Kafka: wiki.edits.raw ───────────────────────────────┐
     │                                               │
     ├─ Spark bronze query → Iceberg wiki.bronze     │ raw payload + Kafka coordinates
     │                                               │
     └─ Spark silver query → watermark + dedupe      │
                      → Iceberg wiki.silver           │ typed edits and review score
                      → five-minute touched-page scan│
                      → Iceberg wiki.alerts           │ page anomaly alerts
                      → atomic serving snapshot ─────┘
                                               │
                                           FastAPI / dashboard
```

The broker has three partitions per topic. The producer keys each message by `wiki:page_id`, retaining per-page order inside one Kafka partition. Both Spark queries read `wiki.edits.raw` independently. They checkpoint separately because bronze and silver have different recovery state. Spark uses a local Hadoop Iceberg catalog at `/app/warehouse` and Iceberg format v2 tables. The files are bind-mounted for inspection. `data/dashboard.json` is a cache, not the data store.

Bronze is a faithful raw record with Kafka topic, partition, offset and timestamp. Silver parses the known fields and carries source schema URI plus Kafka coordinates. Gold is an Iceberg alert table. Each affected page is rescanned over the previous five event-time minutes after a silver microbatch, so alerts can update before a window formally closes. That choice gives low latency at the cost of more read and MERGE work than a final-only window aggregate.

The dashboard is intentionally decoupled from Spark: the stream writes a complete JSON file then atomically renames it. An interrupted export leaves the previous snapshot, and the UI marks stale snapshots after 90 seconds. `/api/metrics` exposes Spark's own progress counters.

**Local constraints:** A single broker and a local Hadoop catalog are a laptop topology. They do not provide broker or catalog high availability. The REST/S3 upgrade path would replace the catalog and warehouse settings while preserving the table schemas and event contract. Multi-writer schema changes should use a catalog with proper concurrency controls; the runbook pauses the local writer for migrations.
