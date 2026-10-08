"""Explicit-schema SBP CSV reader and immutable Bronze projection."""

from __future__ import annotations

import csv
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StringType, StructField, StructType

from ..config import FullLoadParameters, SBP_SOURCES
from ..hashing import stable_hash
from ..schemas import SBP_CSV_SCHEMA, SBP_RAW_COLUMNS


class SchemaDriftError(ValueError):
    pass


def _read_header(spark: SparkSession, path: str) -> list[str]:
    first_line = spark.read.text(path).limit(1).first()
    if first_line is None:
        raise ValueError(f"Empty input file: {path}")
    return next(csv.reader([first_line["value"]]))


def validate_header(spark: SparkSession, path: str) -> list[str]:
    observed = _read_header(spark, path)
    expected = list(SBP_RAW_COLUMNS)
    missing = [name for name in expected if name not in observed]
    extras = [name for name in observed if name not in expected]
    if missing:
        raise SchemaDriftError(f"Missing required SBP columns={missing}; extra columns={extras}")
    return extras


def read_sbp_bronze(spark: SparkSession, params: FullLoadParameters) -> DataFrame:
    extras = validate_header(spark, params.input_path)
    observed_header = _read_header(spark, params.input_path)
    # This schema is still explicit: every observed source field is declared as
    # a nullable string and no value-based type inference is performed.
    read_schema = StructType(
        [StructField(name, StringType(), True) for name in observed_header]
        + [StructField("_corrupt_record", StringType(), True)]
    )
    raw = (
        spark.read.format("csv")
        .schema(read_schema)
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("multiLine", "false")
        .option("escape", '"')
        .load(params.input_path)
    )
    raw_values = [F.col(f"`{column}`") for column in SBP_RAW_COLUMNS]
    rescued = F.to_json(F.struct(*(F.col(f"`{column}`") for column in extras))) if extras else F.lit(None)
    config = SBP_SOURCES[params.source_id]
    return (
        raw.withColumn("source_id", F.lit(params.source_id))
        .withColumn("dataset_code", F.lit(config.dataset_code))
        .withColumn("source_record_id", stable_hash(*raw_values))
        .withColumn("batch_id", F.lit(params.batch_id))
        .withColumn("manifest_id", F.lit(params.manifest_id))
        .withColumn("source_file_path", F.input_file_name())
        .withColumn("source_file_hash", F.lit(params.source_file_hash))
        .withColumn("source_snapshot_at", F.lit(params.source_snapshot_at))
        .withColumn("retrieved_at_utc", F.lit(params.retrieved_at_utc))
        .withColumn(
            "_rescued_data",
            rescued.cast("string"),
        )
        .withColumn("load_timestamp", F.current_timestamp())
        .select(
            F.col("`Dataset Name`").alias("dataset_name_raw"),
            F.col("`Observation Date`").alias("observation_date_raw"),
            F.col("`Series Key`").alias("series_key_raw"),
            F.col("`Series Display Name`").alias("series_display_name_raw"),
            F.col("`Observation Value`").alias("observation_value_raw"),
            F.col("`Unit`").alias("unit_raw"),
            F.col("`Observation Status`").alias("observation_status_raw"),
            F.col("`Observation Status Comment`").alias("observation_status_comment_raw"),
            F.col("`Sequence No.`").alias("sequence_no_raw"),
            F.col("`Series name`").alias("series_name_raw"),
            "source_id", "dataset_code", "source_record_id", "batch_id", "manifest_id",
            "source_file_path", "source_file_hash", "source_snapshot_at", "retrieved_at_utc",
            "_corrupt_record", "_rescued_data", "load_timestamp",
        )
    )
