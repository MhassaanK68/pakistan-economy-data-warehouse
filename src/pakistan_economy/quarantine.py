"""Quarantine invalid records without failing unaffected rows."""

from pyspark.sql import DataFrame, functions as F


def append_quarantine(
    frame: DataFrame,
    *,
    table_name: str,
    reason: str,
    rule: str,
) -> int:
    count = frame.count()
    if count:
        (
            frame.withColumn("quarantine_reason", F.lit(reason))
            .withColumn("failed_rule", F.lit(rule))
            .withColumn("quarantined_at", F.current_timestamp())
            .write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(table_name)
        )
    return count
