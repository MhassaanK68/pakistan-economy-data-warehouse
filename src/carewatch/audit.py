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
_SUPPRESS_AUDIT_LOG_KEY = "_suppress_audit_log"
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
        StructField("as_of_date", DateType(), False),
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
        StructField("last_successful_bronze_run_id", StringType(), False),
        # Legacy Step 4 field. It remains nullable for a safe in-place migration
        # and is not used as the source-extraction checkpoint.
        StructField("last_successful_silver_run_id", StringType(), True),
        StructField("load_timestamp", TimestampType(), False),
    ]
)

SCHEMA_DRIFT_LOG_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("batch_id", StringType(), True),
        StructField("dataset", StringType(), False),
        StructField("source_file", StringType(), False),
        StructField("source_file_sha256", StringType(), True),
        StructField("drift_type", StringType(), False),
        StructField("column_name", StringType(), True),
        StructField("detail", StringType(), False),
        StructField("load_timestamp", TimestampType(), False),
    ]
)

BRONZE_QUARANTINE_SCHEMA = StructType(
    [
        StructField("dataset", StringType(), False),
        StructField("dataset_id", StringType(), False),
        StructField("source_file", StringType(), False),
        StructField("source_file_sha256", StringType(), False),
        StructField("batch_id", StringType(), False),
        StructField("load_type", StringType(), False),
        StructField("raw_record", StringType(), True),
        StructField("corrupt_record", StringType(), True),
        StructField(
            "failed_rules", ArrayType(StringType(), containsNull=False), False
        ),
        StructField("load_timestamp", TimestampType(), False),
    ]
)

SILVER_QUARANTINE_SCHEMA = StructType(
    [
        StructField("dataset", StringType(), False),
        StructField("source_batch_id", StringType(), False),
        StructField("source_file", StringType(), False),
        StructField("source_file_sha256", StringType(), False),
        StructField("candidate_entity_key", StringType(), True),
        StructField("raw_record", StringType(), False),
        StructField(
            "failed_rules", ArrayType(StringType(), containsNull=False), False
        ),
        StructField("load_timestamp", TimestampType(), False),
    ]
)

# Step 4 originally created a smaller placeholder before the locked Silver
# contract was implemented.  Accepting that exact legacy shape during setup
# keeps Raw-to-Bronze operational, but no migration is performed implicitly.
# Step 7 must require the current schema before it writes Silver quarantine rows.
LEGACY_SILVER_QUARANTINE_SCHEMA = StructType(
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
    "bronze_quarantine": BRONZE_QUARANTINE_SCHEMA,
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


def suppress_audit_log(record: dict[str, Any]) -> None:
    """Cancel a successful audit row when a scoped item intentionally does no work.

    This is used for a batch that exists in Bronze but has no rows inside an
    explicitly requested ingestion-date range. Exceptions still produce a
    failure log; callers cannot use this marker to hide an error.
    """

    record[_SUPPRESS_AUDIT_LOG_KEY] = True


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
        suppress_success = bool(record.pop(_SUPPRESS_AUDIT_LOG_KEY, False))
        if suppress_success and processing_error is None:
            return
        try:
            write_execution_log(spark, log_table, record)
        except Exception as log_error:
            if processing_error is None:
                raise
            # Preserve the processing exception. Databricks driver output still
            # exposes the independent audit-write problem for diagnosis.
            print(f"WARNING: failed to persist execution log: {log_error}")


def _schema_signature(schema: StructType) -> dict[str, str]:
    return {field.name: field.dataType.simpleString() for field in schema.fields}


def silver_quarantine_schema_state(spark: Any, table_name: str) -> str:
    """Classify the deployed quarantine table without changing it.

    ``CURRENT`` is the only state from which Step 7 may write.  Setup continues
    to accept ``LEGACY`` so Step 5 remains usable until the separately approved
    migration notebook is run.
    """

    if not spark.catalog.tableExists(table_name):
        return "MISSING"
    def ordered_signature(schema: StructType) -> tuple[tuple[str, str], ...]:
        return tuple(
            (field.name, field.dataType.simpleString())
            for field in schema.fields
        )

    actual = ordered_signature(spark.table(table_name).schema)
    if actual == ordered_signature(SILVER_QUARANTINE_SCHEMA):
        return "CURRENT"
    if actual == ordered_signature(LEGACY_SILVER_QUARANTINE_SCHEMA):
        return "LEGACY"
    return "MISMATCH"


def ensure_delta_table(spark: Any, table_name: str, schema: StructType) -> str:
    """Create an empty managed Delta table, or verify an existing contract."""

    if spark.catalog.tableExists(table_name):
        actual = spark.table(table_name).schema
        actual_signature = _schema_signature(actual)
        expected_signature = _schema_signature(schema)
        mismatches = {
            name: (expected_type, actual_signature.get(name))
            for name, expected_type in expected_signature.items()
            if actual_signature.get(name) != expected_type
        }
        unexpected = sorted(set(actual_signature) - set(expected_signature))
        if mismatches or unexpected:
            raise ValueError(
                f"Existing table {table_name} does not match its locked schema. "
                f"Missing/type-mismatched fields: {mismatches}; "
                f"unexpected fields: {unexpected}. Actual: {actual_signature}"
            )
        return "VERIFIED"

    # Databricks Free/serverless does not support the ``errorifexists`` writer
    # mode. An empty typed append creates a missing managed table without
    # inserting rows; a concurrent creator is still protected by Delta's
    # schema checks.
    spark.createDataFrame([], schema=schema).write.format("delta").mode(
        "append"
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
        if object_name == "silver_quarantine" and spark.catalog.tableExists(
            table_name
        ):
            actual_signature = _schema_signature(spark.table(table_name).schema)
            legacy_signature = _schema_signature(LEGACY_SILVER_QUARANTINE_SCHEMA)
            if actual_signature == legacy_signature:
                # Do not alter, rename, or recreate a cloud table from setup.
                # This compatibility path exists solely so the Step 5 notebook,
                # which verifies all control tables, continues to run unchanged.
                results.append(
                    {
                        "table_name": table_name,
                        "action": "LEGACY_SCHEMA_ACCEPTED_NO_MIGRATION",
                    }
                )
                continue
        if object_name == "source_file_manifest" and spark.catalog.tableExists(
            table_name
        ):
            actual_names = {field.name for field in spark.table(table_name).schema.fields}
            if "as_of_date" not in actual_names:
                # Existing Step 2 tables predate the explicit acquisition date.
                # The log timestamp is the UTC acquisition time for bulk rows;
                # API rows also have the authoritative inclusive window end.
                spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS (as_of_date DATE)")
                spark.sql(
                    f"UPDATE {table_name} "
                    "SET as_of_date = COALESCE(window_end, CAST(load_timestamp AS DATE)) "
                    "WHERE as_of_date IS NULL"
                )
        if object_name == "ingestion_watermarks" and spark.catalog.tableExists(
            table_name
        ):
            actual_names = {field.name for field in spark.table(table_name).schema.fields}
            if "last_successful_bronze_run_id" not in actual_names:
                spark.sql(
                    f"ALTER TABLE {table_name} ADD COLUMNS "
                    "(last_successful_bronze_run_id STRING)"
                )
                if "last_successful_silver_run_id" in actual_names:
                    spark.sql(
                        f"UPDATE {table_name} "
                        "SET last_successful_bronze_run_id = "
                        "last_successful_silver_run_id "
                        "WHERE last_successful_bronze_run_id IS NULL"
                    )
        if object_name == "schema_drift_log" and spark.catalog.tableExists(
            table_name
        ):
            actual_names = {field.name for field in spark.table(table_name).schema.fields}
            additions = []
            if "batch_id" not in actual_names:
                additions.append("batch_id STRING")
            if "source_file_sha256" not in actual_names:
                additions.append("source_file_sha256 STRING")
            if additions:
                spark.sql(
                    f"ALTER TABLE {table_name} ADD COLUMNS ({', '.join(additions)})"
                )
        action = ensure_delta_table(spark, table_name, table_schema)
        results.append({"table_name": table_name, "action": action})
    return results
