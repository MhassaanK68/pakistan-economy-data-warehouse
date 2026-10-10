"""Manifest-driven Raw-to-Bronze ingestion for CareWatch.

Landing files are trusted only through ``source_file_manifest``. Each file or
API page is processed independently, written to managed Delta by deterministic
batch id, and reconciled at acquisition-run scope before a source watermark can
advance.
"""

from __future__ import annotations

import csv
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from carewatch.config import DatasetConfig, get_dataset, qualified_name
from carewatch.drift import (
    DriftAssessment,
    classify_csv_header,
    classify_json_keys,
)


_BATCH_ID = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
_ACQUISITION_STATUSES = ("SUCCESS", "SKIPPED_ALREADY_ACQUIRED")


class BronzeIngestionError(RuntimeError):
    """Base exception for a rejected landing artifact."""


class IntegrityError(BronzeIngestionError):
    """Raised when landed bytes no longer match their manifest record."""


class SchemaDriftError(BronzeIngestionError):
    """Raised after breaking drift has been recorded."""


class EnvelopeValidationError(BronzeIngestionError):
    """Raised when a CMS API page envelope contradicts its manifest."""


@dataclass(frozen=True)
class ManifestItem:
    acquisition_run_id: str
    dataset: str
    dataset_id: str
    load_type: str
    as_of_date: date
    acquisition_strategy: str
    source_url: str
    window_start: date | None
    window_end: date | None
    page_offset: int | None
    landing_path: str
    batch_id: str
    source_file_sha256: str
    source_bytes: int
    expected_run_rows: int | None
    source_rows: int | None
    row_count_validated: bool
    status: str
    load_timestamp: datetime

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | Any) -> "ManifestItem":
        data = value.asDict(recursive=True) if hasattr(value, "asDict") else dict(value)
        return cls(
            acquisition_run_id=str(data["acquisition_run_id"]),
            dataset=str(data["dataset"]),
            dataset_id=str(data["dataset_id"]),
            load_type=str(data["load_type"]),
            as_of_date=data["as_of_date"],
            acquisition_strategy=str(data["acquisition_strategy"]),
            source_url=str(data["source_url"]),
            window_start=data.get("window_start"),
            window_end=data.get("window_end"),
            page_offset=(
                int(data["page_offset"])
                if data.get("page_offset") is not None
                else None
            ),
            landing_path=str(data["landing_path"]),
            batch_id=str(data["batch_id"]),
            source_file_sha256=str(data["source_file_sha256"]),
            source_bytes=int(data["source_bytes"]),
            expected_run_rows=(
                int(data["expected_run_rows"])
                if data.get("expected_run_rows") is not None
                else None
            ),
            source_rows=(
                int(data["source_rows"])
                if data.get("source_rows") is not None
                else None
            ),
            row_count_validated=bool(data["row_count_validated"]),
            status=str(data["status"]),
            load_timestamp=data["load_timestamp"],
        )


@dataclass(frozen=True)
class ManifestSelection:
    """Manifest scope requiring work and the subset requiring a data write."""

    scope_items: tuple[ManifestItem, ...]
    process_items: tuple[ManifestItem, ...]


@dataclass(frozen=True)
class BronzeFileResult:
    acquisition_run_id: str
    dataset: str
    batch_id: str
    rows_read: int
    rows_inserted: int
    rows_quarantined: int
    added_columns: tuple[str, ...]


@dataclass(frozen=True)
class ReconciliationResult:
    acquisition_run_id: str
    dataset: str
    complete: bool
    expected_rows: int | None
    parsed_rows: int | None
    watermark_value: date | None
    detail: str


def _validate_batch_id(batch_id: str) -> None:
    if not _BATCH_ID.fullmatch(batch_id):
        raise ValueError(f"Unsafe batch id: {batch_id!r}")


def _sha256_file(path: str, chunk_size: int = 8 * 1024 * 1024) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with Path(path).open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def verify_landing_file(item: ManifestItem) -> None:
    """Fail before Spark reads a missing or mutated landing artifact."""

    path = Path(item.landing_path)
    if not path.is_file():
        raise IntegrityError(f"Landing file does not exist: {item.landing_path}")
    digest, size = _sha256_file(item.landing_path)
    if size != item.source_bytes:
        raise IntegrityError(
            f"Landing byte count changed for {item.landing_path}: "
            f"manifest={item.source_bytes}, actual={size}"
        )
    if digest.lower() != item.source_file_sha256.lower():
        raise IntegrityError(
            f"Landing SHA-256 changed for {item.landing_path}: "
            f"manifest={item.source_file_sha256}, actual={digest}"
        )


def read_csv_header(path: str) -> tuple[str, ...]:
    """Read only the physical CSV header, removing a possible UTF-8 BOM."""

    with Path(path).open("r", encoding="utf-8-sig", newline="") as handle:
        try:
            return tuple(next(csv.reader(handle)))
        except StopIteration as exc:
            raise BronzeIngestionError(f"CSV file is empty: {path}") from exc


def inspect_json_envelope(path: str) -> tuple[dict[str, Any], tuple[str, ...], int]:
    """Inspect one bounded CMS API page before the explicit Spark read."""

    with Path(path).open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise EnvelopeValidationError("API landing page must be a JSON object")
    meta = payload.get("_meta")
    results = payload.get("results")
    if not isinstance(meta, dict):
        raise EnvelopeValidationError("API landing page has no _meta object")
    if not isinstance(results, list):
        raise EnvelopeValidationError("API landing page has no results array")

    keys: list[str] = []
    seen: set[str] = set()
    for index, record in enumerate(results):
        if not isinstance(record, dict):
            raise EnvelopeValidationError(
                f"API result at index {index} is not an object"
            )
        for key in record:
            if key not in seen:
                seen.add(key)
                keys.append(str(key))
    return dict(meta), tuple(keys), len(results)


def _expected_filter(config: DatasetConfig, item: ManifestItem) -> str:
    if config.watermark_col is None or item.window_start is None or item.window_end is None:
        raise EnvelopeValidationError(
            "API page is missing its configured watermark column or date window"
        )
    return (
        f"{config.watermark_col} >= {item.window_start.isoformat()} AND "
        f"{config.watermark_col} <= {item.window_end.isoformat()}"
    )


def validate_json_envelope(
    item: ManifestItem,
    config: DatasetConfig,
    meta: Mapping[str, Any],
    result_count: int,
) -> None:
    """Validate acquisition metadata before accepting any page rows."""

    checks = {
        "dataset_id": (str(meta.get("dataset_id", "")), item.dataset_id),
        "load_type": (str(meta.get("load_type", "")), item.load_type),
        "filter": (str(meta.get("filter", "")), _expected_filter(config, item)),
    }
    for field, (actual, expected) in checks.items():
        if actual != expected:
            raise EnvelopeValidationError(
                f"API envelope {field} mismatch: expected {expected!r}, got {actual!r}"
            )

    try:
        declared_page_rows = int(meta["rows_in_file"])
    except (KeyError, TypeError, ValueError) as exc:
        raise EnvelopeValidationError("API envelope rows_in_file is invalid") from exc
    if declared_page_rows != result_count:
        raise EnvelopeValidationError(
            f"API envelope row count mismatch: metadata={declared_page_rows}, "
            f"results={result_count}"
        )

    rows_available = meta.get("rows_available")
    if item.expected_run_rows is not None and rows_available is not None:
        if int(rows_available) != item.expected_run_rows:
            raise EnvelopeValidationError(
                "API envelope rows_available does not match the manifest"
            )


def _successful_bronze_batch_ids(
    spark: Any,
    log_table: str,
    datasets: Iterable[str],
    batch_ids: Iterable[str],
    *,
    run_id: str | None = None,
) -> set[tuple[str, str]]:
    dataset_values = tuple(dict.fromkeys(datasets))
    batch_values = tuple(dict.fromkeys(batch_ids))
    if not dataset_values or not batch_values or not spark.catalog.tableExists(log_table):
        return set()

    from pyspark.sql import functions as F

    logs = (
        spark.table(log_table)
        .filter(F.col("pipeline_layer") == F.lit("Raw-to-Bronze"))
        .filter(F.col("status") == F.lit("SUCCESS"))
        .filter(F.col("dataset").isin(list(dataset_values)))
        .filter(F.col("batch_id").isin(list(batch_values)))
    )
    if run_id:
        logs = logs.filter(F.col("run_id") == F.lit(run_id))

    return {
        (str(row["dataset"]), str(row["batch_id"]))
        for row in (
            logs
            .select("dataset", "batch_id")
            .distinct()
            .collect()
        )
    }


def select_manifest_items(
    spark: Any,
    manifest_table: str,
    log_table: str,
    *,
    dataset: str | None = None,
    load_type: str | None = None,
    acquisition_run_id: str | None = None,
    source_path: str | None = None,
    reprocess: bool = False,
) -> ManifestSelection:
    """Select trusted work without scanning a landing directory."""

    if not spark.catalog.tableExists(manifest_table):
        raise RuntimeError(f"Manifest table does not exist: {manifest_table}")

    from pyspark.sql import Window, functions as F

    df = spark.table(manifest_table).filter(F.col("status").isin(*_ACQUISITION_STATUSES))
    if dataset:
        get_dataset(dataset)
        df = df.filter(F.col("dataset") == F.lit(dataset))
    if load_type:
        if load_type not in {"full", "incremental"}:
            raise ValueError("load_type must be full, incremental, or omitted")
        df = df.filter(F.col("load_type") == F.lit(load_type))
    if acquisition_run_id:
        df = df.filter(F.col("acquisition_run_id") == F.lit(acquisition_run_id))
    if source_path:
        df = df.filter(F.col("landing_path") == F.lit(source_path))

    latest = Window.partitionBy(
        "acquisition_run_id", "dataset", "batch_id", "landing_path"
    ).orderBy(F.col("load_timestamp").desc())
    rows = (
        df.withColumn("_manifest_rn", F.row_number().over(latest))
        .filter(F.col("_manifest_rn") == 1)
        .drop("_manifest_rn")
        .orderBy("dataset", "acquisition_run_id", F.col("page_offset").asc_nulls_first())
        .collect()
    )
    items = tuple(ManifestItem.from_mapping(row) for row in rows)
    if source_path and not items:
        raise ValueError(
            "source_path is not registered by a successful acquisition manifest row: "
            f"{source_path}"
        )

    successful = _successful_bronze_batch_ids(
        spark,
        log_table,
        (item.dataset for item in items),
        (item.batch_id for item in items),
    )
    if reprocess:
        scope = items
    else:
        scope = tuple(
            item
            for item in items
            if not item.row_count_validated
            or (item.dataset, item.batch_id) not in successful
        )

    process: list[ManifestItem] = []
    seen_batches: set[tuple[str, str]] = set()
    for item in sorted(scope, key=lambda value: value.load_timestamp, reverse=True):
        identity = (item.dataset, item.batch_id)
        if identity in seen_batches:
            continue
        seen_batches.add(identity)
        if reprocess or identity not in successful:
            process.append(item)

    return ManifestSelection(scope_items=scope, process_items=tuple(process))


def _drift_records(
    run_id: str,
    item: ManifestItem,
    assessment: DriftAssessment,
    timestamp: datetime,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    possible_rename = bool(assessment.missing_columns)
    for added in assessment.added_columns:
        drift_type = "RENAMED_OR_UNMAPPED_COLUMN" if possible_rename else "ADDED_COLUMN"
        detail = (
            f"Source column {added.source_name!r} resolved to provisional Bronze "
            f"column {added.bronze_name!r}."
        )
        if possible_rename:
            detail += " Required columns are also missing; file is rejected as a possible rename."
        records.append(
            {
                "run_id": run_id,
                "batch_id": item.batch_id,
                "dataset": item.dataset,
                "source_file": item.landing_path,
                "source_file_sha256": item.source_file_sha256,
                "drift_type": drift_type,
                "column_name": added.bronze_name,
                "detail": detail,
                "load_timestamp": timestamp,
            }
        )
    for column in assessment.missing_columns:
        records.append(
            {
                "run_id": run_id,
                "batch_id": item.batch_id,
                "dataset": item.dataset,
                "source_file": item.landing_path,
                "source_file_sha256": item.source_file_sha256,
                "drift_type": "MISSING_COLUMN",
                "column_name": column,
                "detail": "Required source column is absent; file rejected before Bronze write.",
                "load_timestamp": timestamp,
            }
        )
    for column in (*assessment.invalid_columns, *assessment.duplicate_columns):
        records.append(
            {
                "run_id": run_id,
                "batch_id": item.batch_id,
                "dataset": item.dataset,
                "source_file": item.landing_path,
                "source_file_sha256": item.source_file_sha256,
                "drift_type": "RENAMED_OR_UNMAPPED_COLUMN",
                "column_name": column or None,
                "detail": "Source column is invalid, reserved, duplicated, or maps ambiguously.",
                "load_timestamp": timestamp,
            }
        )
    return records


def _write_drift(
    spark: Any,
    drift_table: str,
    run_id: str,
    item: ManifestItem,
    assessment: DriftAssessment,
    timestamp: datetime,
) -> None:
    from carewatch.audit import (
        SCHEMA_DRIFT_LOG_SCHEMA,
        append_typed_records,
    )

    append_typed_records(
        spark,
        drift_table,
        _drift_records(run_id, item, assessment, timestamp),
        SCHEMA_DRIFT_LOG_SCHEMA,
    )


def _csv_dataframe(spark: Any, item: ManifestItem, assessment: DriftAssessment) -> Any:
    from pyspark.sql.types import StringType, StructField, StructType

    schema = StructType(
        [StructField(column, StringType(), True) for column in assessment.ordered_columns]
        + [StructField("_corrupt_record", StringType(), True)]
    )
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("enforceSchema", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .schema(schema)
        .load(item.landing_path)
    )


def _json_dataframe(spark: Any, item: ManifestItem, assessment: DriftAssessment) -> Any:
    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType, StringType, StructField, StructType
    from carewatch.schemas import INCREMENTAL_META

    result_schema = StructType(
        [StructField(column, StringType(), True) for column in assessment.ordered_columns]
    )
    envelope = StructType(
        [
            StructField("_meta", INCREMENTAL_META, False),
            StructField("results", ArrayType(result_schema, containsNull=False), False),
        ]
    )
    return (
        spark.read.option("multiLine", "true")
        .schema(envelope)
        .json(item.landing_path)
        .select(F.explode("results").alias("record"))
        .select("record.*")
    )


def _add_bronze_metadata(
    df: Any, item: ManifestItem, timestamp: datetime, source_columns: Iterable[str]
) -> Any:
    from pyspark.sql import functions as F

    return (
        df.select(*source_columns)
        .withColumn("_dataset_id", F.lit(item.dataset_id))
        .withColumn("_source_file", F.lit(item.landing_path))
        .withColumn("_source_file_sha256", F.lit(item.source_file_sha256.lower()))
        .withColumn("_batch_id", F.lit(item.batch_id))
        .withColumn("_load_type", F.lit(item.load_type))
        .withColumn("_ingest_date", F.lit(timestamp.date()).cast("date"))
        .withColumn("load_timestamp", F.lit(timestamp).cast("timestamp"))
    )


def _quarantine_dataframe(
    corrupt_df: Any,
    item: ManifestItem,
    timestamp: datetime,
    source_columns: Iterable[str],
) -> Any:
    from pyspark.sql import functions as F

    columns = tuple(source_columns)
    return corrupt_df.select(
        F.lit(item.dataset).alias("dataset"),
        F.lit(item.dataset_id).alias("dataset_id"),
        F.lit(item.landing_path).alias("source_file"),
        F.lit(item.source_file_sha256.lower()).alias("source_file_sha256"),
        F.lit(item.batch_id).alias("batch_id"),
        F.lit(item.load_type).alias("load_type"),
        F.to_json(
            F.struct(*[F.col(column).alias(column) for column in columns]),
            options={"ignoreNullFields": "false"},
        ).alias("raw_record"),
        F.col("_corrupt_record").alias("corrupt_record"),
        F.array(F.lit("CORRUPT_CSV_RECORD")).alias("failed_rules"),
        F.lit(timestamp).cast("timestamp").alias("load_timestamp"),
    )


def _write_batch(
    spark: Any,
    df: Any,
    table_name: str,
    batch_column: str,
    batch_id: str,
    row_count: int,
    *,
    merge_schema: bool,
) -> None:
    """Replace one batch without touching any other table rows."""

    _validate_batch_id(batch_id)
    if not spark.catalog.tableExists(table_name):
        (
            df.write.format("delta")
            .mode("append")
            .option("mergeSchema", str(merge_schema).lower())
            .saveAsTable(table_name)
        )
        return

    if row_count == 0:
        from delta.tables import DeltaTable
        from pyspark.sql import functions as F

        DeltaTable.forName(spark, table_name).delete(
            F.col(batch_column) == F.lit(batch_id)
        )
        return

    writer = (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"{batch_column} = '{batch_id}'")
    )
    if merge_schema:
        writer = writer.option("mergeSchema", "true")
    writer.saveAsTable(table_name)


def _update_manifest_source_rows(
    spark: Any, manifest_table: str, item: ManifestItem, source_rows: int
) -> None:
    """Propagate parsed counts to every manifest reference to the same batch.

    Acquisition may reuse identical landed content in a later run. Because the
    deterministic batch id is the stable content identity, parsing it once is
    sufficient for every successful manifest row that references that batch.
    """

    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    condition = (
        (F.col("dataset") == F.lit(item.dataset))
        & (F.col("batch_id") == F.lit(item.batch_id))
        & (F.col("status").isin(*_ACQUISITION_STATUSES))
    )
    DeltaTable.forName(spark, manifest_table).update(
        condition=condition,
        set={"source_rows": F.lit(source_rows).cast("long")},
    )


def process_manifest_item(
    spark: Any,
    item: ManifestItem,
    *,
    run_id: str,
    catalog: str,
    schema_name: str,
    manifest_table: str,
    drift_table: str,
    quarantine_table: str,
) -> BronzeFileResult:
    """Validate and commit one manifest-backed file or API page."""

    from pyspark.sql import functions as F
    from carewatch.audit import BRONZE_QUARANTINE_SCHEMA, utc_now
    from carewatch.schemas import HEADER_OVERRIDES, source_columns

    verify_landing_file(item)
    config = get_dataset(item.dataset)
    expected = source_columns(item.dataset)
    event_time = utc_now()

    if item.acquisition_strategy == "api_date_window":
        meta, observed_keys, inspected_rows = inspect_json_envelope(item.landing_path)
        validate_json_envelope(item, config, meta, inspected_rows)
        # A valid empty result page has no record keys to inspect.
        assessment = classify_json_keys(observed_keys or expected, expected)
        source_df = _json_dataframe(spark, item, assessment)
        has_corrupt_field = False
    elif item.acquisition_strategy in {"bulk_snapshot", "snapshot_diff"}:
        header = read_csv_header(item.landing_path)
        assessment = classify_csv_header(
            header, expected, HEADER_OVERRIDES.get(item.dataset, {})
        )
        source_df = _csv_dataframe(spark, item, assessment)
        has_corrupt_field = True
    else:
        raise BronzeIngestionError(
            f"Unsupported acquisition strategy: {item.acquisition_strategy!r}"
        )

    _write_drift(spark, drift_table, run_id, item, assessment, event_time)
    if assessment.has_breaking_drift:
        raise SchemaDriftError(
            f"Breaking schema drift for {item.landing_path}: "
            f"missing={list(assessment.missing_columns)}, "
            f"invalid={list(assessment.invalid_columns)}, "
            f"duplicates={list(assessment.duplicate_columns)}"
        )

    if has_corrupt_field:
        statistics = source_df.agg(
            F.count(F.lit(1)).alias("rows_read"),
            F.sum(F.when(F.col("_corrupt_record").isNotNull(), 1).otherwise(0)).alias(
                "rows_quarantined"
            ),
        ).first()
        rows_read = int(statistics["rows_read"] or 0)
        rows_quarantined = int(statistics["rows_quarantined"] or 0)
        good_df = source_df.filter(F.col("_corrupt_record").isNull()).drop(
            "_corrupt_record"
        )
        bad_df = source_df.filter(F.col("_corrupt_record").isNotNull())
    else:
        rows_read = int(source_df.count())
        rows_quarantined = 0
        good_df = source_df
        bad_df = None

    rows_inserted = rows_read - rows_quarantined
    if item.acquisition_strategy == "api_date_window" and rows_read != inspected_rows:
        raise EnvelopeValidationError(
            f"Spark parsed {rows_read} API rows but the envelope contains {inspected_rows}"
        )

    quarantine_df = (
        _quarantine_dataframe(
            bad_df,
            item,
            event_time,
            assessment.ordered_columns,
        )
        if bad_df is not None
        else spark.createDataFrame([], schema=BRONZE_QUARANTINE_SCHEMA)
    )
    _write_batch(
        spark,
        quarantine_df,
        quarantine_table,
        "batch_id",
        item.batch_id,
        rows_quarantined,
        merge_schema=False,
    )

    bronze_df = _add_bronze_metadata(
        good_df,
        item,
        event_time,
        assessment.ordered_columns,
    )
    bronze_table = qualified_name(catalog, schema_name, config.bronze_table)
    _write_batch(
        spark,
        bronze_df,
        bronze_table,
        "_batch_id",
        item.batch_id,
        rows_inserted,
        merge_schema=True,
    )
    _update_manifest_source_rows(spark, manifest_table, item, rows_read)

    return BronzeFileResult(
        acquisition_run_id=item.acquisition_run_id,
        dataset=item.dataset,
        batch_id=item.batch_id,
        rows_read=rows_read,
        rows_inserted=rows_inserted,
        rows_quarantined=rows_quarantined,
        added_columns=tuple(
            added.bronze_name for added in assessment.added_columns
        ),
    )


def _mark_manifest_run_validated(
    spark: Any, manifest_table: str, acquisition_run_id: str, dataset: str
) -> None:
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    condition = (
        (F.col("acquisition_run_id") == F.lit(acquisition_run_id))
        & (F.col("dataset") == F.lit(dataset))
    )
    DeltaTable.forName(spark, manifest_table).update(
        condition=condition,
        set={"row_count_validated": F.lit(True)},
    )


def reconcile_acquisition_run(
    spark: Any,
    *,
    acquisition_run_id: str,
    dataset: str,
    bronze_run_id: str,
    manifest_table: str,
    log_table: str,
    watermark_table: str,
    require_bronze_run_id: str | None = None,
) -> ReconciliationResult:
    """Validate a complete run and advance its event watermark at most once."""

    from pyspark.sql import functions as F
    from carewatch.watermarks import commit_bronze_watermark

    rows = (
        spark.table(manifest_table)
        .filter(F.col("acquisition_run_id") == F.lit(acquisition_run_id))
        .filter(F.col("dataset") == F.lit(dataset))
        .filter(F.col("status").isin(*_ACQUISITION_STATUSES))
        .orderBy(F.col("page_offset").asc_nulls_first())
        .collect()
    )
    items = tuple(ManifestItem.from_mapping(row) for row in rows)
    if not items:
        return ReconciliationResult(
            acquisition_run_id, dataset, False, None, None, None,
            "No successful acquisition manifest rows found.",
        )

    expected_values = {
        item.expected_run_rows
        for item in items
        if item.expected_run_rows is not None
    }
    if len(expected_values) != 1:
        return ReconciliationResult(
            acquisition_run_id, dataset, False, None, None, None,
            "Manifest does not contain one authoritative expected row count.",
        )
    expected_rows = next(iter(expected_values))
    if any(item.source_rows is None for item in items):
        return ReconciliationResult(
            acquisition_run_id, dataset, False, expected_rows, None, None,
            "At least one manifest file has not been parsed by Bronze.",
        )
    parsed_rows = sum(int(item.source_rows or 0) for item in items)
    if parsed_rows != expected_rows:
        return ReconciliationResult(
            acquisition_run_id, dataset, False, expected_rows, parsed_rows, None,
            "Parsed rows do not reconcile with expected_run_rows.",
        )

    if items[0].acquisition_strategy == "api_date_window":
        offsets = [item.page_offset for item in items]
        if any(offset is None for offset in offsets):
            return ReconciliationResult(
                acquisition_run_id, dataset, False, expected_rows, parsed_rows, None,
                "API acquisition contains a null page offset.",
            )
        expected_offsets = [index * 500 for index in range(len(offsets))]
        if [int(offset) for offset in offsets] != expected_offsets:
            return ReconciliationResult(
                acquisition_run_id, dataset, False, expected_rows, parsed_rows, None,
                "API page offsets are not contiguous in 500-row increments.",
            )

    success = _successful_bronze_batch_ids(
        spark,
        log_table,
        [dataset],
        [item.batch_id for item in items],
        run_id=require_bronze_run_id,
    )
    missing_success = [
        item.batch_id
        for item in items
        if (dataset, item.batch_id) not in success
    ]
    if missing_success:
        return ReconciliationResult(
            acquisition_run_id, dataset, False, expected_rows, parsed_rows, None,
            f"Batches without a successful Raw-to-Bronze log: {missing_success}",
        )

    config = get_dataset(dataset)
    watermark_value: date | None = None
    if config.watermark_col is not None:
        window_ends = {item.window_end for item in items if item.window_end is not None}
        if window_ends:
            if len(window_ends) != 1:
                return ReconciliationResult(
                    acquisition_run_id, dataset, False, expected_rows, parsed_rows, None,
                    "Manifest contains conflicting API window end dates.",
                )
            watermark_value = next(iter(window_ends))
        else:
            as_of_dates = {item.as_of_date for item in items}
            if len(as_of_dates) != 1:
                return ReconciliationResult(
                    acquisition_run_id, dataset, False, expected_rows, parsed_rows, None,
                    "Manifest contains conflicting full-load as_of_date values.",
                )
            watermark_value = next(iter(as_of_dates))

        commit_bronze_watermark(
            spark,
            watermark_table,
            dataset=dataset,
            watermark_column=config.watermark_col,
            watermark_value=watermark_value,
            acquisition_run_id=acquisition_run_id,
            bronze_run_id=bronze_run_id,
        )

    # Mark validation only after the event watermark commit succeeds. If the
    # process stops between these operations, the next run safely repeats the
    # monotonic watermark MERGE and then repairs this manifest flag.
    _mark_manifest_run_validated(spark, manifest_table, acquisition_run_id, dataset)

    return ReconciliationResult(
        acquisition_run_id=acquisition_run_id,
        dataset=dataset,
        complete=True,
        expected_rows=expected_rows,
        parsed_rows=parsed_rows,
        watermark_value=watermark_value,
        detail="All expected files/pages reached Bronze and row counts reconciled.",
    )
