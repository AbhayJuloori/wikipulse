"""Run while Spark writer is stopped: additive Iceberg schema evolution demo."""

from wikipulse.stream import initialize, spark_session

spark = spark_session()
initialize(spark)
columns = {row.col_name for row in spark.sql("DESCRIBE lake.wiki.alerts").collect()}
if "analyst_note" not in columns:
    spark.sql("ALTER TABLE lake.wiki.alerts ADD COLUMN analyst_note STRING")
print("Added nullable analyst_note; existing alert rows and named-column readers remain valid.")
spark.table("lake.wiki.alerts").select("alert_id", "kind", "analyst_note").show(5, truncate=False)
spark.stop()
