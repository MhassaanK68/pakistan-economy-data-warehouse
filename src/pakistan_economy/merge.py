"""Reusable, idempotent Delta merge operations."""

from __future__ import annotations

from dataclasses import dataclass

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


@dataclass(frozen=True)
class MergeMetrics:
    inserted: int = 0
    updated: int = 0
    deleted: int = 0


def _metrics(spark: SparkSession, target_table: str) -> MergeMetrics:
    raw = DeltaTable.forName(spark, target_table).history(1).select("operationMetrics").first()[0] or {}
    return MergeMetrics(
        inserted=int(raw.get("numTargetRowsInserted", 0)),
        updated=int(raw.get("numTargetRowsUpdated", 0)),
        deleted=int(raw.get("numTargetRowsDeleted", 0)),
    )


def merge_bronze_insert_only(spark: SparkSession, incoming: DataFrame, target_table: str) -> MergeMetrics:
    (
        DeltaTable.forName(spark, target_table)
        .alias("t")
        .merge(incoming.alias("s"), "t.source_file_hash = s.source_file_hash AND t.source_record_id = s.source_record_id")
        .whenNotMatchedInsertAll()
        .execute()
    )
    return _metrics(spark, target_table)


def merge_silver_current(
    spark: SparkSession,
    incoming: DataFrame,
    target_table: str,
    business_key: list[str],
) -> MergeMetrics:
    condition = " AND ".join(f"t.`{column}` <=> s.`{column}`" for column in business_key)
    (
        DeltaTable.forName(spark, target_table)
        .alias("t")
        .merge(incoming.alias("s"), condition)
        .whenMatchedUpdateAll(
            condition="s.source_snapshot_at > t.source_snapshot_at AND s.record_hash <> t.record_hash"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    return _metrics(spark, target_table)
