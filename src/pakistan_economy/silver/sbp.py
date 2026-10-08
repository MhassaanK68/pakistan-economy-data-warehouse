"""Canonical SBP Silver transformation shared by all five structured feeds."""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

from ..config import SourceConfig
from ..hashing import stable_hash


def _parse_date(column: str, formats: tuple[str, ...]):
    return F.coalesce(*(F.to_date(F.col(column), date_format) for date_format in formats))


def to_silver(bronze: DataFrame, config: SourceConfig) -> tuple[DataFrame, DataFrame]:
    typed = (
        bronze.withColumn("observation_date", _parse_date("observation_date_raw", config.date_formats))
        .withColumn("reporting_month", F.trunc(F.col("observation_date"), "month"))
        .withColumn(
            "observation_value",
            F.regexp_replace(F.trim(F.col("observation_value_raw")), ",", "").cast("decimal(24,6)"),
        )
        .withColumn("series_key", F.trim(F.col("series_key_raw")))
        .withColumn("series_display_name", F.trim(F.col("series_display_name_raw")))
        .withColumn("series_name", F.trim(F.col("series_name_raw")))
        .withColumn("unit", F.trim(F.col("unit_raw")))
        .withColumn("observation_status", F.trim(F.col("observation_status_raw")))
        .withColumn("observation_status_comment", F.trim(F.col("observation_status_comment_raw")))
        .withColumn("sequence_no", F.col("sequence_no_raw").cast("long"))
    )
    invalid_condition = (
        F.col("series_key").isNull()
        | F.col("observation_date").isNull()
        | (F.col("observation_value_raw").isNotNull() & F.col("observation_value").isNull())
        | F.col("_corrupt_record").isNotNull()
    )
    invalid = typed.filter(invalid_condition)
    valid = typed.filter(~invalid_condition)
    date_key = "observation_date" if config.frequency == "daily" else "reporting_month"
    window = Window.partitionBy("series_key", date_key).orderBy(
        F.col("source_snapshot_at").desc(),
        F.col("retrieved_at_utc").desc(),
        F.col("source_file_hash").desc(),
    )
    selected = (
        valid.withColumn("_candidate_rank", F.row_number().over(window))
        .filter(F.col("_candidate_rank") == 1)
        .drop("_candidate_rank")
        .withColumn("mapping_status", F.lit("UNMAPPED"))
        .withColumn("load_timestamp", F.current_timestamp())
    )
    hash_columns = [
        F.col("series_key"),
        F.col(date_key),
        F.col("observation_value"),
        F.col("unit"),
        F.col("observation_status"),
        F.col("observation_status_comment"),
    ]
    selected = selected.withColumn("record_hash", stable_hash(*hash_columns))
    output_columns = [
        "source_id", "dataset_code", "series_key", "series_display_name", "series_name",
        "observation_date", "reporting_month", "observation_value", "unit",
        "observation_status", "observation_status_comment", "sequence_no", "mapping_status",
        "batch_id", "manifest_id", "source_file_path", "source_file_hash",
        "source_snapshot_at", "retrieved_at_utc", "record_hash", "load_timestamp",
    ]
    return selected.select(*output_columns), invalid


def business_key(config: SourceConfig) -> list[str]:
    return ["series_key", "observation_date" if config.frequency == "daily" else "reporting_month"]
