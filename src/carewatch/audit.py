"""Typed Delta control tables and failure-safe pipeline execution logging."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


MAX_ERROR_LENGTH = 2000
VALID_LAYERS = {"CMS-to-Landing", "Raw-to-Bronze", "Bronze-to-Silver"}
VALID_LOAD_TYPES = {"full", "incremental"}
VALID_STATUSES = {
    "SUCCESS",
    "SKIPPED_ALREADY_ACQUIRED",
    "FAILURE",
    "QUARANTINED_PARTIAL",
}


LOG_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("batch_id", StringType(), False),
        StructField("pipeline_layer", StringType(), False),
        StructField("dataset", StringType(), False),
        StructField("load_type", StringType(), False),
        StructField("parameter_processed", StringType(), False),
        StructField("start_time", TimestampType(), False),
        StructField("end_time", TimestampType(), False),
        StructField("status", StringType(), False),
        StructField("rows_read", LongType(), False),
        StructField("rows_inserted", LongType(), False),
        StructField("rows_updated", LongType(), False),
        StructField("rows_deleted", LongType(), False),
        StructField("rows_quarantined", LongType(), False),
        StructField("error_message", StringType(), True),
        StructField("load_timestamp", TimestampType(), False),
    ]
)

MANIFEST_SCHEMA = StructType(
    [
        StructField("acquisition_run_id", StringType(), False),
        StructField("dataset", StringType(), False),
        StructField("dataset_id", StringType(), False),
        StructField("load_type", StringType(), False),
        StructField("acquisition_strategy", StringType(), False),
        StructField("source_url", StringType(), False),
        StructField("source_catalog_modified", DateType(), True),
        StructField("window_start", DateType(), True),
        StructField("window_end", DateType(), True),
        StructField("page_offset", LongType(), True),
        StructField("landing_path", StringType(), False),
        StructField("batch_id", StringType(), False),
        StructField("source_content_sha256", StringType(), False),
        StructField("source_file_sha256", StringType(), False),
        StructField("source_bytes", LongType(), False),
        StructField("expected_run_rows", LongType(), True),
        StructField("source_rows", LongType(), True),
        StructField("row_count_validated", BooleanType(), False),
        StructField("status", StringType(), False),
        StructField("error_message", StringType(), True),
        StructField("load_timestamp", TimestampType(), False),
    ]
)

WATERMARK_SCHEMA = StructType(
    [
        StructField("dataset", StringType(), False),
        StructField("watermark_column", StringType(), False),
        StructField("watermark_value", DateType(), False),
        StructField("last_successful_acquisition_run_id", StringType(), False),
        StructField("last_successful_silver_run_id", StringType(), False),
        StructField("load_timestamp", TimestampType(), False),
    ]
)

SCHEMA_DRIFT_LOG_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("dataset", StringType(), False),
        StructField("source_file", StringType(), False),
        StructField("drift_type", StringType(), False),
        StructField("column_name", StringType(), True),
        StructField("detail", StringType(), False),
        StructField("load_timestamp", TimestampType(), False),
    ]
)

SILVER_QUARANTINE_SCHEMA = StructType(
    [
        StructField("dataset", StringType(), False),
        StructField("batch_id", StringType(), False),
        StructField("raw_record", StringType(), False),
        StructField(
            "failed_rules", ArrayType(StringType(), containsNull=False), False
        ),
        StructField("load_timestamp", TimestampType(), False),
    ]
)

CONTROL_TABLE_SCHEMAS: dict[str, StructType] = {
    "pipeline_execution_logs": LOG_SCHEMA,
    "source_file_manifest": MANIFEST_SCHEMA,
    "ingestion_watermarks": WATERMARK_SCHEMA,
    "schema_drift_log": SCHEMA_DRIFT_LOG_SCHEMA,
    "silver_quarantine": SILVER_QUARANTINE_SCHEMA,
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def truncate_error(error: object | None) -> str | None:
    if error is None:
        return None
    return str(error)[:MAX_ERROR_LENGTH]


def _validate_log_record(record: Mapping[str, Any]) -> None:
    if record["pipeline_layer"] not in VALID_LAYERS:
        raise ValueError(f"Unknown pipeline layer: {record['pipeline_layer']!r}")
    if record["load_type"] not in VALID_LOAD_TYPES:
        raise ValueError(f"Unknown load type: {record['load_type']!r}")
    if record["status"] not in VALID_STATUSES:
        raise ValueError(f"Unknown execution status: {record['status']!r}")
    for metric in (
        "rows_read",
        "rows_inserted",
        "rows_updated",
        "rows_deleted",
        "rows_quarantined",
    ):
        value = record[metric]
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise ValueError(f"{metric} must be a non-negative integer")


def append_typed_records(
    spark: Any,
    table_name: str,
    records: Iterable[Mapping[str, Any]],
    schema: StructType,
) -> None:
    """Append records with an explicit schema; never infer operational tables."""

    rows = [dict(record) for record in records]
    if not rows:
        return
    spark.createDataFrame(rows, schema=schema).write.format("delta").mode(
        "append"
    ).saveAsTable(table_name)


def write_execution_log(spark: Any, log_table: str, record: Mapping[str, Any]) -> None:
    """Validate and append one completed execution attempt."""

    normalized = dict(record)
    normalized["error_message"] = truncate_error(normalized.get("error_message"))
    _validate_log_record(normalized)
    append_typed_records(spark, log_table, [normalized], LOG_SCHEMA)


def completed_log_record(
    *,
    run_id: str,
    batch_id: str,
    pipeline_layer: str,
    dataset: str,
    load_type: str,
    parameter_processed: str,
    start_time: datetime,
    status: str = "SUCCESS",
    rows_read: int = 0,
    rows_inserted: int = 0,
    rows_updated: int = 0,
    rows_deleted: int = 0,
    rows_quarantined: int = 0,
    error_message: object | None = None,
    end_time: datetime | None = None,
) -> dict[str, Any]:
    """Build a complete, manifest-friendly execution log record."""

    finished_at = end_time or utc_now()
    record = {
        "run_id": run_id,
        "batch_id": batch_id,
        "pipeline_layer": pipeline_layer,
        "dataset": dataset,
        "load_type": load_type,
        "parameter_processed": parameter_processed,
        "start_time": start_time,
        "end_time": finished_at,
        "status": status,
        "rows_read": rows_read,
        "rows_inserted": rows_inserted,
        "rows_updated": rows_updated,
        "rows_deleted": rows_deleted,
        "rows_quarantined": rows_quarantined,
        "error_message": truncate_error(error_message),
        "load_timestamp": finished_at,
    }
    _validate_log_record(record)
    return record


@contextmanager
def audited(
    spark: Any,
    log_table: str,
    run_id: str,
    layer: str,
    dataset: str,
    load_type: str,
    parameter: str,
    batch_id: str = "",
) -> Iterator[dict[str, Any]]:
    """Log one attempt on success or failure and re-raise processing errors."""

    record: dict[str, Any] = {
        "run_id": run_id,
        "batch_id": batch_id,
        "pipeline_layer": layer,
        "dataset": dataset,
        "load_type": load_type,
        "parameter_processed": parameter,
        "start_time": utc_now(),
        "status": "SUCCESS",
        "rows_read": 0,
        "rows_inserted": 0,
        "rows_updated": 0,
        "rows_deleted": 0,
        "rows_quarantined": 0,
        "error_message": None,
    }
    processing_error: Exception | None = None
    try:
        yield record
        if record["rows_quarantined"] > 0 and record["status"] == "SUCCESS":
            record["status"] = "QUARANTINED_PARTIAL"
    except Exception as exc:
        processing_error = exc
        record["status"] = "FAILURE"
        record["error_message"] = truncate_error(exc)
        raise
    finally:
        end_time = utc_now()
        record["end_time"] = end_time
        record["load_timestamp"] = end_time
        try:
            write_execution_log(spark, log_table, record)
        except Exception as log_error:
            if processing_error is None:
                raise
            # Preserve the processing exception. Databricks driver output still
            # exposes the independent audit-write problem for diagnosis.
            print(f"WARNING: failed to persist execution log: {log_error}")


def _schema_signature(schema: StructType) -> list[tuple[str, str]]:
    return [(field.name, field.dataType.simpleString()) for field in schema.fields]


def ensure_delta_table(spark: Any, table_name: str, schema: StructType) -> str:
    """Create an empty managed Delta table, or verify an existing contract."""

    if spark.catalog.tableExists(table_name):
        actual = spark.table(table_name).schema
        if _schema_signature(actual) != _schema_signature(schema):
            raise ValueError(
                f"Existing table {table_name} does not match its locked schema. "
                f"Expected {_schema_signature(schema)}, got {_schema_signature(actual)}"
            )
        return "VERIFIED"

    spark.createDataFrame([], schema=schema).write.format("delta").mode(
        "errorifexists"
    ).saveAsTable(table_name)
    return "CREATED"


def ensure_control_tables(
    spark: Any, catalog: str, schema_name: str
) -> list[dict[str, str]]:
    """Idempotently create or verify every Step 4 control table."""

    from carewatch.config import qualified_name

    results = []
    for object_name, table_schema in CONTROL_TABLE_SCHEMAS.items():
        table_name = qualified_name(catalog, schema_name, object_name)
        action = ensure_delta_table(spark, table_name, table_schema)
        results.append({"table_name": table_name, "action": action})
    return results
