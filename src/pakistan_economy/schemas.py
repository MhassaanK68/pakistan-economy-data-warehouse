"""All source schemas are explicit; schema inference is intentionally forbidden."""

from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


SBP_RAW_COLUMNS = (
    "Dataset Name",
    "Observation Date",
    "Series Key",
    "Series Display Name",
    "Observation Value",
    "Unit",
    "Observation Status",
    "Observation Status Comment",
    "Sequence No.",
    "Series name",
)

SBP_CSV_SCHEMA = StructType(
    [StructField(name, StringType(), True) for name in SBP_RAW_COLUMNS]
    + [StructField("_corrupt_record", StringType(), True)]
)

MANIFEST_SCHEMA = StructType(
    [
        StructField("manifest_id", StringType(), False),
        StructField("batch_id", StringType(), False),
        StructField("source_id", StringType(), False),
        StructField("dataset_code", StringType(), True),
        StructField("source_file_name", StringType(), False),
        StructField("staging_path", StringType(), False),
        StructField("file_format", StringType(), False),
        StructField("file_size_bytes", LongType(), False),
        StructField("sha256_hash", StringType(), False),
        StructField("retrieved_at_utc", TimestampType(), False),
        StructField("ingestion_status", StringType(), False),
        StructField("created_at_utc", TimestampType(), False),
    ]
)

EXECUTION_LOG_SCHEMA = StructType(
    [
        StructField("execution_log_id", StringType(), False),
        StructField("run_id", StringType(), False),
        StructField("parent_execution_log_id", StringType(), True),
        StructField("databricks_job_id", StringType(), True),
        StructField("databricks_run_id", StringType(), True),
        StructField("source_id", StringType(), False),
        StructField("batch_id", StringType(), False),
        StructField("load_type", StringType(), False),
        StructField("layer_from", StringType(), False),
        StructField("layer_to", StringType(), False),
        StructField("operation_name", StringType(), False),
        StructField("input_parameter_json", StringType(), False),
        StructField("input_file_or_table", StringType(), False),
        StructField("target_table", StringType(), False),
        StructField("execution_start_utc", TimestampType(), False),
        StructField("execution_end_utc", TimestampType(), True),
        StructField("status", StringType(), False),
        StructField("rows_read", LongType(), False),
        StructField("rows_inserted", LongType(), False),
        StructField("rows_updated", LongType(), False),
        StructField("rows_deleted", LongType(), False),
        StructField("rows_rejected", LongType(), False),
        StructField("rows_quarantined", LongType(), False),
        StructField("files_processed", IntegerType(), False),
        StructField("error_class", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("notebook_or_module", StringType(), False),
        StructField("code_version", StringType(), False),
        StructField("load_timestamp", TimestampType(), False),
    ]
)
