"""Read successful incremental checkpoints from Databricks Delta tables."""

from __future__ import annotations

from datetime import date
from typing import Any


def table_exists(spark: Any, table_name: str) -> bool:
    """Return False before setup has created an operational table."""

    return bool(spark.catalog.tableExists(table_name))


def read_successful_watermark(spark: Any, table_name: str, dataset: str) -> date | None:
    """Return the most recent committed Silver watermark for a dataset.

    This function is intentionally read-only. Step 7 advances a watermark only
    after every page in the acquisition run has merged successfully.
    """

    if not table_exists(spark, table_name):
        return None

    from pyspark.sql import functions as F

    rows = (
        spark.table(table_name)
        .filter(F.col("dataset") == F.lit(dataset))
        .orderBy(F.col("load_timestamp").desc())
        .select("watermark_value")
        .limit(1)
        .collect()
    )
    if not rows or rows[0]["watermark_value"] is None:
        return None

    value = rows[0]["watermark_value"]
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])
