"""Read-only Iceberg assertion for a replay marker; run via spark-submit."""

import sys

from pyspark.sql import functions as F

from wikipulse.stream import spark_session

marker = sys.argv[1]
spark = spark_session()
silver = spark.table("lake.wiki.silver").where(F.col("event_id").startswith(marker))
bronze = spark.table("lake.wiki.bronze").where(F.col("payload").contains(marker))
alerts = spark.table("lake.wiki.alerts").where(F.col("page_id") == 987654321)
print(
    f"VERIFY marker={marker} silver={silver.count()} bronze={bronze.count()} alerts={alerts.count()}"
)
for row in silver.groupBy("event_id").count().orderBy("event_id").collect():
    print(f"VERIFY event={row.event_id} count={row['count']}")
for row in alerts.select("kind", "edits", "editors", "revert_hints").collect():
    print(f"VERIFY alert={row.asDict()}")
spark.stop()
