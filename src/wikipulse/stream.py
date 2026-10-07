"""Kafka → Iceberg bronze/silver/gold with restart-safe, idempotent microbatches."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

WAREHOUSE = "file:///app/warehouse"
CHECKPOINTS = "/app/checkpoints"


def spark_session():
    return (
        SparkSession.builder.appName("WikiPulse")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "hadoop")
        .config("spark.sql.catalog.lake.warehouse", WAREHOUSE)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider",
        )
        .getOrCreate()
    )


def initialize(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.wiki")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lake.wiki.bronze (
          topic STRING, partition INT, offset BIGINT, kafka_ts TIMESTAMP,
          payload STRING, ingested_at TIMESTAMP
        ) USING iceberg PARTITIONED BY (topic) TBLPROPERTIES ('format-version'='2')
    """)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lake.wiki.silver (
          event_id STRING, wiki STRING, page_id BIGINT, revision_id BIGINT,
          event_ts TIMESTAMP, username STRING, title STRING, page_url STRING,
          bot BOOLEAN, comment STRING, old_length BIGINT, new_length BIGINT,
          schema_uri STRING, risk_score INT, revert_hint BOOLEAN,
          topic STRING, partition INT, offset BIGINT
        ) USING iceberg PARTITIONED BY (days(event_ts), wiki) TBLPROPERTIES ('format-version'='2')
    """)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lake.wiki.alerts (
          alert_id STRING, bucket_ts TIMESTAMP, last_event_ts TIMESTAMP,
          kind STRING, severity STRING, wiki STRING, page_id BIGINT,
          title STRING, edits BIGINT, editors BIGINT, revert_hints BIGINT,
          bot_edits BIGINT, explanation STRING
        ) USING iceberg PARTITIONED BY (days(bucket_ts), wiki) TBLPROPERTIES ('format-version'='2')
    """)


def edit_schema():
    return T.StructType(
        [
            T.StructField("$schema", T.StringType()),
            T.StructField(
                "meta",
                T.StructType(
                    [
                        T.StructField("id", T.StringType()),
                        T.StructField("domain", T.StringType()),
                    ]
                ),
            ),
            T.StructField("id", T.LongType()),
            T.StructField("type", T.StringType()),
            T.StructField("wiki", T.StringType()),
            T.StructField("title", T.StringType()),
            T.StructField("title_url", T.StringType()),
            T.StructField("timestamp", T.LongType()),
            T.StructField("user", T.StringType()),
            T.StructField("bot", T.BooleanType()),
            T.StructField("comment", T.StringType()),
            T.StructField(
                "length",
                T.StructType(
                    [
                        T.StructField("old", T.LongType()),
                        T.StructField("new", T.LongType()),
                    ]
                ),
            ),
            T.StructField("revision", T.StructType([T.StructField("new", T.LongType())])),
        ]
    )


def parse_silver(kafka):
    event = kafka.select(
        F.from_json(F.col("value").cast("string"), edit_schema()).alias("e"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
    )
    typed = event.where(
        (F.col("e.type") == "edit")
        & (F.col("e.meta.domain") != "canary")
        & F.col("e.meta.id").isNotNull()
        & F.col("e.id").isNotNull()
        & F.col("e.revision.new").isNotNull()
        & F.col("e.timestamp").isNotNull()
    ).select(
        F.col("e.meta.id").alias("event_id"),
        F.col("e.wiki").alias("wiki"),
        F.col("e.id").alias("page_id"),
        F.col("e.revision.new").alias("revision_id"),
        F.to_timestamp(F.from_unixtime(F.col("e.timestamp"))).alias("event_ts"),
        F.col("e.user").alias("username"),
        F.col("e.title").alias("title"),
        F.col("e.title_url").alias("page_url"),
        F.coalesce(F.col("e.bot"), F.lit(False)).alias("bot"),
        F.coalesce(F.col("e.comment"), F.lit("")).alias("comment"),
        F.col("e.length.old").alias("old_length"),
        F.col("e.length.new").alias("new_length"),
        F.col("e.`$schema`").alias("schema_uri"),
        "topic",
        "partition",
        "offset",
    )
    revert = F.col("comment").rlike(
        "(?i)\\b(revert(ed|ing)?|undid|undo|rvv?|revertido|rückgängig)\\b"
    )
    # Keep in sync with wikipulse.events.ANONYMOUS_USER (stream.py ships without the package).
    anonymous = F.col("username").rlike(
        r"^(?:(?:\d{1,3}\.){3}\d{1,3}|[0-9a-fA-F]*:[0-9a-fA-F:]+|~\d{4}-\d+-\d+)$"
    )
    delta = F.abs(
        F.coalesce(F.col("new_length"), F.lit(0)) - F.coalesce(F.col("old_length"), F.lit(0))
    )
    scored = typed.withColumn("revert_hint", revert).withColumn(
        "risk_score",
        F.least(
            F.lit(100),
            F.lit(20)
            + F.when(anonymous, 25).otherwise(0)
            + F.when(revert, 20).otherwise(0)
            + F.least(F.floor(delta / 100), F.lit(25))
            - F.when(F.col("bot"), 10).otherwise(0),
        ).cast("int"),
    )
    return scored.withWatermark("event_ts", "10 minutes").dropDuplicatesWithinWatermark(
        ["event_id"]
    )


def merge_bronze(batch, batch_id):
    if batch.isEmpty():
        return
    batch.select(
        "topic",
        "partition",
        "offset",
        F.col("timestamp").alias("kafka_ts"),
        F.col("value").cast("string").alias("payload"),
        F.current_timestamp().alias("ingested_at"),
    ).createOrReplaceTempView("bronze_batch")
    batch.sparkSession.sql("""
      MERGE INTO lake.wiki.bronze t USING bronze_batch s
      ON t.topic = s.topic AND t.partition = s.partition AND t.offset = s.offset
      WHEN NOT MATCHED THEN INSERT (topic, partition, offset, kafka_ts, payload, ingested_at)
      VALUES (s.topic, s.partition, s.offset, s.kafka_ts, s.payload, s.ingested_at)
    """)


def merge_silver(batch, batch_id):
    if batch.isEmpty():
        return
    batch.persist()
    spark = batch.sparkSession
    try:
        batch.createOrReplaceTempView("silver_batch")
        columns = [
            "event_id",
            "wiki",
            "page_id",
            "revision_id",
            "event_ts",
            "username",
            "title",
            "page_url",
            "bot",
            "comment",
            "old_length",
            "new_length",
            "schema_uri",
            "risk_score",
            "revert_hint",
            "topic",
            "partition",
            "offset",
        ]
        names = ", ".join(columns)
        values = ", ".join(f"s.{column}" for column in columns)
        spark.sql(f"""
          MERGE INTO lake.wiki.silver t USING silver_batch s ON t.event_id = s.event_id
          WHEN NOT MATCHED THEN INSERT ({names}) VALUES ({values})
        """)
        refresh_alerts(spark, batch)
        export_snapshot(spark, batch_id, batch.count())
    finally:
        batch.unpersist()


def refresh_alerts(spark, batch):
    latest = batch.agg(F.max("event_ts")).first()[0]
    if latest is None:
        return
    lower = latest.timestamp() - 300
    touched = batch.select("wiki", "page_id").distinct()
    history = spark.table("lake.wiki.silver").join(F.broadcast(touched), ["wiki", "page_id"])
    history = history.where(
        (F.col("event_ts").cast("long") >= lower) & (F.col("event_ts") <= F.lit(latest))
    )
    stats = history.groupBy("wiki", "page_id").agg(
        F.max_by("title", "event_ts").alias("title"),
        F.count("*").alias("edits"),
        F.countDistinct("username").alias("editors"),
        F.sum(F.col("revert_hint").cast("bigint")).alias("revert_hints"),
        F.sum(F.col("bot").cast("bigint")).alias("bot_edits"),
    )
    alerts = stats.withColumn(
        "kind",
        F.when((F.col("revert_hints") >= 3) & (F.col("editors") >= 2), "edit_war")
        .when((F.col("bot_edits") >= 12) & (F.col("editors") <= 2), "bot_burst")
        .when((F.col("edits") >= 8) & (F.col("editors") >= 3), "edit_burst"),
    ).where(F.col("kind").isNotNull())
    bucket = latest.replace(second=0, microsecond=0)
    alerts = alerts.withColumn("bucket_ts", F.lit(bucket).cast("timestamp"))
    alerts = alerts.withColumn("last_event_ts", F.lit(latest).cast("timestamp"))
    alerts = alerts.withColumn(
        "severity", F.when(F.col("kind") == "edit_war", "high").otherwise("medium")
    )
    alerts = alerts.withColumn(
        "explanation",
        F.concat_ws(
            " ",
            F.col("edits"),
            F.lit("edits from"),
            F.col("editors"),
            F.lit("editors in 5 minutes;"),
            F.col("revert_hints"),
            F.lit("revert hints."),
        ),
    )
    alerts = alerts.withColumn(
        "alert_id",
        F.sha2(
            F.concat_ws(
                ":",
                "wiki",
                F.col("page_id").cast("string"),
                F.col("bucket_ts").cast("string"),
                "kind",
            ),
            256,
        ),
    )
    if alerts.isEmpty():
        return
    alerts.createOrReplaceTempView("alert_batch")
    spark.sql("""
      MERGE INTO lake.wiki.alerts t USING alert_batch s ON t.alert_id = s.alert_id
      WHEN MATCHED THEN UPDATE SET t.last_event_ts = s.last_event_ts,
        t.edits = s.edits, t.editors = s.editors, t.revert_hints = s.revert_hints,
        t.bot_edits = s.bot_edits, t.explanation = s.explanation
      WHEN NOT MATCHED THEN INSERT (alert_id, bucket_ts, last_event_ts, kind, severity,
        wiki, page_id, title, edits, editors, revert_hints, bot_edits, explanation)
      VALUES (s.alert_id, s.bucket_ts, s.last_event_ts, s.kind, s.severity,
        s.wiki, s.page_id, s.title, s.edits, s.editors, s.revert_hints, s.bot_edits, s.explanation)
    """)


def export_snapshot(spark, batch_id, batch_count):
    output = Path(os.getenv("DATA_DIR", "/app/data")) / "dashboard.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    queue = (
        spark.table("lake.wiki.silver").orderBy(F.desc("event_ts"), F.desc("risk_score")).limit(60)
    )
    alerts = spark.table("lake.wiki.alerts").orderBy(F.desc("last_event_ts")).limit(40)
    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "batch_id": batch_id,
        "batch_rows": batch_count,
        "queue": [row.asDict() for row in queue.collect()],
        "alerts": [row.asDict() for row in alerts.collect()],
    }
    previous = {}
    if output.exists():
        try:
            previous = json.loads(output.read_text())
        except (ValueError, OSError):
            pass
    history = previous.get("throughput", [])[-29:]
    history.append({"at": result["generated_at"], "rows": batch_count})
    result["throughput"] = history
    # Atomic replacement means API readers never observe a partial JSON snapshot.
    temp = output.with_suffix(".tmp")
    # Spark returns UTC timestamps as naive Python datetimes. Label them UTC for browsers.
    temp.write_text(
        json.dumps(
            result,
            default=lambda value: value.replace(tzinfo=timezone.utc).isoformat()
            if isinstance(value, datetime)
            else str(value),
        )
    )
    temp.replace(output)


def export_progress(query):
    progress = query.lastProgress
    if not progress:
        return
    state = progress.get("stateOperators") or []
    result = {
        "batch_id": progress.get("batchId"),
        "watermark": (progress.get("eventTime") or {}).get("watermark"),
        "input_rows": progress.get("numInputRows", 0),
        "input_rows_per_second": progress.get("inputRowsPerSecond", 0),
        "processed_rows_per_second": progress.get("processedRowsPerSecond", 0),
        "batch_duration_ms": (progress.get("durationMs") or {}).get("triggerExecution", 0),
        "late_rows_dropped": sum(item.get("numRowsDroppedByWatermark", 0) for item in state),
    }
    output = Path(os.getenv("DATA_DIR", "/app/data")) / "stream_metrics.json"
    temp = output.with_suffix(".tmp")
    temp.write_text(json.dumps(result))
    temp.replace(output)


def main():
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")
    initialize(spark)
    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP", "kafka:19092"))
        .option("subscribe", "wiki.edits.raw")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", "2000")
        .load()
    )
    bronze = (
        source.writeStream.queryName("wikipulse-bronze")
        .foreachBatch(merge_bronze)
        .option("checkpointLocation", f"{CHECKPOINTS}/bronze-v1")
        .trigger(processingTime="10 seconds")
        .start()
    )
    silver = (
        parse_silver(source)
        .writeStream.queryName("wikipulse-silver-gold")
        .foreachBatch(merge_silver)
        .option("checkpointLocation", f"{CHECKPOINTS}/silver-v1")
        .trigger(processingTime="10 seconds")
        .start()
    )
    while bronze.isActive and silver.isActive:
        export_progress(silver)
        time.sleep(5)
    bronze.stop()
    silver.stop()
    raise RuntimeError("A streaming query stopped; inspect Spark logs before restarting")


if __name__ == "__main__":
    main()
