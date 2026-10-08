"""Deterministic Spark hashing helpers."""

from pyspark.sql import Column, DataFrame, functions as F


NULL_SENTINEL = "∅"


def stable_hash(*columns: Column) -> Column:
    normalized = [F.coalesce(col.cast("string"), F.lit(NULL_SENTINEL)) for col in columns]
    return F.sha2(F.concat_ws("\u001f", *normalized), 256)


def with_record_hash(frame: DataFrame, column_names: list[str], name: str = "record_hash") -> DataFrame:
    return frame.withColumn(name, stable_hash(*(F.col(column) for column in column_names)))
