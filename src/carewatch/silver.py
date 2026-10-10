"""Manifest-driven Bronze-to-Silver processing for CareWatch.

The functions in this module are deliberately independent of ``dbutils``.  A
Databricks notebook supplies parameters and an address-secret reference, while
this module owns deterministic projection, validation, quarantine, and Delta
MERGE behavior. Nothing here writes to Bronze or ingestion watermarks.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Iterable, Mapping, Sequence

from carewatch.config import get_dataset, qualified_name


SILVER_TERMINAL_STATUSES = frozenset({"SUCCESS", "QUARANTINED_PARTIAL"})
ACQUISITION_SUCCESS_STATUSES = frozenset({"SUCCESS", "SKIPPED_ALREADY_ACQUIRED"})
CORRECTION_DATE_PRESENT_STATUS = "Deficient, Provider has date of correction"


class SilverProcessingError(RuntimeError):
    """Base exception for a rejected Silver work item."""


class SilverSchemaError(SilverProcessingError):
    """Raised when a required Bronze or Silver contract is not deployed."""


@dataclass(frozen=True)
class SilverWorkItem:
    """One successful Bronze batch and its latest trusted manifest context."""

    acquisition_run_id: str
    dataset: str
    load_type: str
    acquisition_strategy: str
    batch_id: str
    source_file: str
    source_file_sha256: str
    expected_run_rows: int | None
    source_rows: int | None
    row_count_validated: bool
    manifest_status: str
    manifest_load_timestamp: datetime
    acquisition_file_count: int

    @classmethod
    def from_mapping(
        cls, value: Mapping[str, Any] | Any, *, acquisition_file_count: int = 1
    ) -> "SilverWorkItem":
        data = value.asDict(recursive=True) if hasattr(value, "asDict") else dict(value)
        return cls(
            acquisition_run_id=str(data["acquisition_run_id"]),
            dataset=str(data["dataset"]),
            load_type=str(data["load_type"]),
            acquisition_strategy=str(data["acquisition_strategy"]),
            batch_id=str(data["batch_id"]),
            source_file=str(data["landing_path"]),
            source_file_sha256=str(data["source_file_sha256"]).lower(),
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
            manifest_status=str(data["status"]),
            manifest_load_timestamp=data["load_timestamp"],
            acquisition_file_count=acquisition_file_count,
        )


@dataclass(frozen=True)
class SilverTransformResult:
    accepted: Any
    quarantine: Any
    rows_read: int
    rows_accepted: int
    rows_rejected: int
    invalid_input_rows: int
    conflicting_input_rows: int
    unique_quarantine_records: int
    exact_valid_duplicates_collapsed: int
    max_source_processing_date: date | None


@dataclass(frozen=True)
class MergeMetrics:
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_deleted: int = 0


@dataclass(frozen=True)
class SoftDeleteDecision:
    allowed: bool
    reasons: tuple[str, ...]


def is_terminal_silver_status(status: str) -> bool:
    return status in SILVER_TERMINAL_STATUSES


def pending_work_keys(
    bronze_successes: Iterable[tuple[str, str]],
    silver_attempts: Iterable[tuple[str, str, str]],
    *,
    reprocess: bool = False,
) -> tuple[tuple[str, str], ...]:
    """Pure checkpoint rule used by selection and local tests."""

    successful = set(bronze_successes)
    if reprocess:
        return tuple(sorted(successful))
    complete = {
        (dataset, batch_id)
        for dataset, batch_id, status in silver_attempts
        if is_terminal_silver_status(status)
    }
    return tuple(sorted(successful - complete))


def missing_required_columns(
    actual_columns: Iterable[str], required_columns: Iterable[str]
) -> tuple[str, ...]:
    """Return breaking missing-column drift while allowing added Bronze fields."""

    return tuple(sorted(set(required_columns) - set(actual_columns)))


def canonical_json_sha256(columns: Sequence[str], values: Mapping[str, Any]) -> str:
    """Reference implementation of the ordered, null-preserving hash contract.

    Spark production rows use :func:`sha256_json_columns`.  This small pure
    helper makes the exact ordering/null semantics independently testable.
    """

    payload = {column: values.get(column) for column in columns}
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def classify_duplicate_records(
    records: Iterable[Mapping[str, Any]],
    *,
    key_field: str,
    hash_field: str = "row_hash",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Pure mirror of the Spark exact-duplicate/conflict policy for tests."""

    rows = [dict(record) for record in records]
    hashes_by_key: dict[Any, set[Any]] = {}
    for row in rows:
        hashes_by_key.setdefault(row.get(key_field), set()).add(row.get(hash_field))
    conflict_keys = {key for key, hashes in hashes_by_key.items() if len(hashes) > 1}
    conflicts = [row for row in rows if row.get(key_field) in conflict_keys]
    accepted: list[dict[str, Any]] = []
    seen: set[tuple[Any, Any]] = set()
    for row in rows:
        identity = (row.get(key_field), row.get(hash_field))
        if row.get(key_field) in conflict_keys or identity in seen:
            continue
        seen.add(identity)
        accepted.append(row)
    return accepted, conflicts


def calculate_transform_metrics(
    *,
    rows_read: int,
    invalid_input_rows: int,
    conflicting_input_rows: int,
    valid_nonconflicting_rows: int,
    rows_accepted: int,
    unique_quarantine_records: int,
) -> dict[str, int]:
    """Calculate non-overlapping Step 7 counts and enforce conservation."""

    values = (
        rows_read,
        invalid_input_rows,
        conflicting_input_rows,
        valid_nonconflicting_rows,
        rows_accepted,
        unique_quarantine_records,
    )
    if any(value < 0 for value in values):
        raise ValueError("Transform metrics cannot be negative")
    if rows_read != (
        invalid_input_rows + conflicting_input_rows + valid_nonconflicting_rows
    ):
        raise ValueError("Transform input categories do not conserve rows_read")
    if rows_accepted > valid_nonconflicting_rows:
        raise ValueError("Accepted rows cannot exceed valid non-conflicting rows")
    return {
        "rows_rejected": invalid_input_rows + conflicting_input_rows,
        "exact_valid_duplicates_collapsed": (
            valid_nonconflicting_rows - rows_accepted
        ),
        "unique_quarantine_records": unique_quarantine_records,
    }


def penalty_subtype_failures(values: Mapping[str, Any]) -> tuple[str, ...]:
    """Pure reference for the locked mutually exclusive penalty subtype rules."""

    penalty_type = values.get("penalty_type")
    if penalty_type == "Fine":
        invalid = (
            values.get("fine_id") is None
            or values.get("fine_amount") is None
            or values.get("payment_denial_start_date") is not None
            or values.get("payment_denial_length_in_days") is not None
        )
        return ("CONDITIONAL:fine_subtype",) if invalid else ()
    if penalty_type == "Payment Denial":
        invalid = (
            values.get("payment_denial_start_date") is None
            or values.get("payment_denial_length_in_days") is None
            or values.get("fine_id") is not None
            or values.get("fine_amount") is not None
        )
        return ("CONDITIONAL:payment_denial_subtype",) if invalid else ()
    return ("DOMAIN:penalty_type",)


def map_legacy_quarantine_record(
    record: Mapping[str, Any], lineage: Mapping[str, Any]
) -> dict[str, Any]:
    """Pure representation of the migration's legacy-to-current mapping."""

    source_file = lineage.get("source_file")
    source_hash = lineage.get("source_file_sha256")
    if not source_file or not source_hash:
        raise ValueError("Legacy quarantine lineage is incomplete")
    return {
        "dataset": record["dataset"],
        "source_batch_id": record["batch_id"],
        "source_file": source_file,
        "source_file_sha256": str(source_hash).lower(),
        "candidate_entity_key": None,
        "raw_record": record["raw_record"],
        "failed_rules": list(record["failed_rules"]),
        "load_timestamp": record["load_timestamp"],
    }


def should_apply_change(
    *,
    target_hash: str,
    source_hash: str,
    target_is_deleted: bool,
    target_processing_date: date | None,
    source_processing_date: date | None,
) -> bool:
    """Return the conservative SCD1 update decision used by the MERGE.

    A changed row with no source processing date cannot prove it is at least as
    recent as an existing row, so it is not allowed to overwrite that row.
    """

    changed = target_hash != source_hash or target_is_deleted
    if not changed:
        return False
    if source_processing_date is None:
        return False
    return target_processing_date is None or source_processing_date >= target_processing_date


def evaluate_soft_delete(
    item: SilverWorkItem,
    *,
    explicitly_enabled: bool,
    subset_or_reprocess: bool,
    rows_quarantined: int,
    is_latest_validated_snapshot: bool,
    snapshot_max_processing_date: date | None,
) -> SoftDeleteDecision:
    """Evaluate all destructive snapshot gates without performing a write."""

    reasons: list[str] = []
    if not explicitly_enabled:
        reasons.append("soft deletion was not explicitly enabled")
    if subset_or_reprocess:
        reasons.append("subset, backfill, or reprocess scopes cannot soft-delete")
    if item.acquisition_strategy not in {"bulk_snapshot", "snapshot_diff"}:
        reasons.append("API date windows are not complete snapshots")
    if item.manifest_status not in ACQUISITION_SUCCESS_STATUSES:
        reasons.append("manifest status is not successful")
    if not item.row_count_validated:
        reasons.append("manifest row-count validation is incomplete")
    if item.acquisition_file_count != 1:
        reasons.append("snapshot is not represented by exactly one bulk artifact")
    if item.expected_run_rows is None or item.source_rows is None:
        reasons.append("snapshot row counts are unavailable")
    elif item.expected_run_rows != item.source_rows:
        reasons.append("snapshot row counts do not reconcile")
    elif item.source_rows <= 0:
        reasons.append("an empty snapshot cannot soft-delete existing rows")
    if rows_quarantined:
        reasons.append("snapshot contains quarantined or conflicting records")
    if not is_latest_validated_snapshot:
        reasons.append("snapshot is not the newest validated Bronze snapshot")
    if snapshot_max_processing_date is None:
        reasons.append("snapshot has no processing-date bound for deletion scope")
    return SoftDeleteDecision(not reasons, tuple(reasons))


def _schema_signature(schema: Any) -> tuple[tuple[str, str], ...]:
    return tuple(
        (field.name, field.dataType.simpleString())
        for field in schema.fields
    )


def _nullability_signature(schema: Any) -> dict[str, bool]:
    return {field.name: bool(field.nullable) for field in schema.fields}


def _information_schema_nullability(
    spark: Any, table_name: str
) -> dict[str, bool] | None:
    """Read Unity Catalog nullability metadata when the runtime exposes it."""

    parts = table_name.split(".")
    if len(parts) != 3:
        return None
    catalog, schema_name, object_name = parts
    try:
        rows = spark.sql(
            f"SELECT column_name, is_nullable FROM {catalog}.information_schema.columns "
            f"WHERE table_schema = '{schema_name}' AND table_name = '{object_name}' "
            "ORDER BY ordinal_position"
        ).collect()
    except Exception:
        return None
    if not rows:
        return None
    return {
        str(row["column_name"]): str(row["is_nullable"]).upper() == "YES"
        for row in rows
    }


def require_exact_table_schema(spark: Any, table_name: str, expected_schema: Any) -> None:
    """Require exact columns, types, order, and declared nullability.

    Unity Catalog ``information_schema.columns`` is preferred for nullability.
    If that metadata view is unavailable, the Spark table schema is used. A
    runtime that normalizes ``NOT NULL`` to nullable is not silently accepted;
    the raised error identifies the metadata source so the operator can inspect
    ``DESCRIBE TABLE EXTENDED`` and run explicit null scans before correcting
    the table definition.
    """

    if not spark.catalog.tableExists(table_name):
        raise SilverSchemaError(f"Required Delta table does not exist: {table_name}")
    actual = spark.table(table_name).schema
    if _schema_signature(actual) != _schema_signature(expected_schema):
        raise SilverSchemaError(
            f"Table {table_name} does not match the locked ordered schema. "
            f"Expected {_schema_signature(expected_schema)}, got {_schema_signature(actual)}"
        )
    expected_nullability = _nullability_signature(expected_schema)
    catalog_nullability = _information_schema_nullability(spark, table_name)
    actual_nullability = catalog_nullability or _nullability_signature(actual)
    nullability_source = (
        "Unity Catalog information_schema.columns"
        if catalog_nullability is not None
        else "Spark table schema fallback"
    )
    mismatches = {
        column: (expected_nullable, actual_nullability.get(column))
        for column, expected_nullable in expected_nullability.items()
        if actual_nullability.get(column) != expected_nullable
    }
    if mismatches:
        raise SilverSchemaError(
            f"Table {table_name} nullability does not match the locked schema "
            f"according to {nullability_source}: {mismatches}. Do not bypass this "
            "check; inspect DESCRIBE TABLE EXTENDED and verify non-null columns "
            "with explicit null-count queries before correcting the table contract."
        )


def select_silver_work_items(
    spark: Any,
    manifest_table: str,
    log_table: str,
    *,
    dataset: str | None = None,
    load_type: str | None = None,
    acquisition_run_id: str | None = None,
    batch_ids: Sequence[str] = (),
    reprocess: bool = False,
) -> tuple[SilverWorkItem, ...]:
    """Select successful Bronze batches using log state as the Silver checkpoint."""

    if not spark.catalog.tableExists(manifest_table):
        raise SilverProcessingError(f"Manifest table does not exist: {manifest_table}")
    if not spark.catalog.tableExists(log_table):
        raise SilverProcessingError(f"Execution log table does not exist: {log_table}")
    if dataset:
        get_dataset(dataset)
    if load_type and load_type not in {"full", "incremental"}:
        raise ValueError("load_type must be full, incremental, or omitted")

    from pyspark.sql import Window, functions as F

    manifest = spark.table(manifest_table).filter(
        F.col("status").isin(*sorted(ACQUISITION_SUCCESS_STATUSES))
    )
    if dataset:
        manifest = manifest.filter(F.col("dataset") == F.lit(dataset))
    if load_type:
        manifest = manifest.filter(F.col("load_type") == F.lit(load_type))
    if acquisition_run_id:
        manifest = manifest.filter(
            F.col("acquisition_run_id") == F.lit(acquisition_run_id)
        )
    if batch_ids:
        manifest = manifest.filter(F.col("batch_id").isin(list(batch_ids)))

    run_counts = manifest.groupBy("dataset", "acquisition_run_id").agg(
        F.countDistinct("batch_id").alias("_acquisition_file_count")
    )
    latest = Window.partitionBy("dataset", "batch_id").orderBy(
        F.col("load_timestamp").desc()
    )
    candidates = (
        manifest.withColumn("_manifest_rn", F.row_number().over(latest))
        .filter(F.col("_manifest_rn") == 1)
        .drop("_manifest_rn")
        .join(run_counts, ["dataset", "acquisition_run_id"], "left")
    )

    bronze_success = (
        spark.table(log_table)
        .filter(F.col("pipeline_layer") == F.lit("Raw-to-Bronze"))
        .filter(F.col("status") == F.lit("SUCCESS"))
        .select("dataset", "batch_id")
        .distinct()
    )
    candidates = candidates.join(bronze_success, ["dataset", "batch_id"], "inner")

    if not reprocess:
        silver_complete = (
            spark.table(log_table)
            .filter(F.col("pipeline_layer") == F.lit("Bronze-to-Silver"))
            .filter(F.col("status").isin(*sorted(SILVER_TERMINAL_STATUSES)))
            .select("dataset", "batch_id")
            .distinct()
        )
        candidates = candidates.join(
            silver_complete, ["dataset", "batch_id"], "left_anti"
        )

    rows = candidates.orderBy(
        F.col("load_timestamp").asc(),
        "dataset",
        F.col("page_offset").asc_nulls_first(),
        "batch_id",
    ).collect()
    return tuple(
        SilverWorkItem.from_mapping(
            row, acquisition_file_count=int(row["_acquisition_file_count"] or 0)
        )
        for row in rows
    )


def select_bronze_batch(
    spark: Any,
    table_name: str,
    batch_id: str,
    *,
    ingest_from: date | None = None,
    ingest_to: date | None = None,
) -> Any:
    """Read one durable Bronze batch, with optional ingestion-date backfill bounds."""

    from pyspark.sql import functions as F

    df = spark.table(table_name).filter(F.col("_batch_id") == F.lit(batch_id))
    if ingest_from:
        df = df.filter(F.col("_ingest_date") >= F.lit(ingest_from).cast("date"))
    if ingest_to:
        df = df.filter(F.col("_ingest_date") <= F.lit(ingest_to).cast("date"))
    return df


def is_latest_validated_snapshot(
    spark: Any,
    manifest_table: str,
    log_table: str,
    item: SilverWorkItem,
) -> bool:
    """Confirm that a deletion candidate is the newest validated Bronze snapshot."""

    from pyspark.sql import Window, functions as F

    raw_success = (
        spark.table(log_table)
        .filter(F.col("pipeline_layer") == F.lit("Raw-to-Bronze"))
        .filter(F.col("status") == F.lit("SUCCESS"))
        .filter(F.col("dataset") == F.lit(item.dataset))
        .select("dataset", "batch_id")
        .distinct()
    )
    snapshots = (
        spark.table(manifest_table)
        .filter(F.col("dataset") == F.lit(item.dataset))
        .filter(F.col("status").isin(*sorted(ACQUISITION_SUCCESS_STATUSES)))
        .filter(F.col("acquisition_strategy").isin("bulk_snapshot", "snapshot_diff"))
        .filter(F.col("row_count_validated") == F.lit(True))
        .join(raw_success, ["dataset", "batch_id"], "inner")
    )
    newest = Window.partitionBy("dataset").orderBy(
        F.col("as_of_date").desc(), F.col("load_timestamp").desc()
    )
    rows = (
        snapshots.withColumn("_rn", F.row_number().over(newest))
        .filter(F.col("_rn") == 1)
        .select("batch_id")
        .collect()
    )
    return bool(rows and str(rows[0]["batch_id"]) == item.batch_id)


def sha256_json_columns(columns: Sequence[str]) -> Any:
    """Build a Spark SHA-256 expression over ordered JSON with explicit nulls."""

    from pyspark.sql import functions as F

    payload = F.to_json(
        F.struct(*[F.col(column).alias(column) for column in columns]),
        options={"ignoreNullFields": "false"},
    )
    return F.sha2(payload, 256)


def _try_cast(column: str, spark_type: str) -> Any:
    from pyspark.sql import functions as F

    return F.expr(f"try_cast(`{column}` as {spark_type})")


def _sql_string_literal(value: str) -> str:
    """Quote a non-secret widget value for a Databricks SQL expression."""

    return "'" + value.replace("'", "''") + "'"


def databricks_secret_expression(scope: str, key: str) -> Any:
    """Resolve a secret in the Spark engine without materializing it in Python.

    Only the non-secret scope and key names enter the submitted expression.
    Databricks applies its normal best-effort secret redaction to the resolved
    value; callers must still avoid selecting or logging that expression.
    """

    if not scope or not key:
        raise SilverProcessingError(
            "Address secret scope and key are required and must be non-empty"
        )
    from pyspark.sql import functions as F

    return F.expr(
        f"secret({_sql_string_literal(scope)}, {_sql_string_literal(key)})"
    )


def _typed_allowed_values(field: Any, values: Sequence[str]) -> tuple[Any, ...]:
    """Convert declarative domain literals to the actual Spark target type."""

    kind = field.dataType.simpleString()
    if kind in {"int", "bigint"}:
        return tuple(int(value) for value in values)
    if kind in {"double", "float"} or kind.startswith("decimal("):
        return tuple(float(value) for value in values)
    return tuple(values)


def _validation_rule_expressions(dataset: str, typed: Any) -> list[tuple[Any, str]]:
    from pyspark.sql import functions as F
    from carewatch.schemas import SILVER_SCHEMAS, silver_validation_spec

    spec = silver_validation_spec(dataset)
    schema_fields = {field.name: field for field in SILVER_SCHEMAS[dataset].fields}
    source_for_target = {target: source for source, target in spec.source_renames}
    rules: list[tuple[Any, str]] = []

    for field_name in spec.required_source_fields:
        rules.append((typed[field_name].isNull(), f"REQUIRED:{field_name}"))

    for field_name in spec.safe_cast_fields:
        source_name = source_for_target.get(field_name, field_name)
        rules.append(
            (
                typed[f"_raw__{source_name}"].isNotNull()
                & typed[field_name].isNull(),
                f"SAFE_CAST:{source_name}->{field_name}",
            )
        )

    for field_name in spec.boolean_yn_fields:
        rules.append(
            (
                typed[f"_raw__{field_name}"].isNotNull()
                & typed[field_name].isNull(),
                f"DOMAIN_YN:{field_name}",
            )
        )

    for field_name, pattern in spec.regex_rules:
        rules.append(
            (
                typed[field_name].isNotNull() & ~typed[field_name].rlike(pattern),
                f"REGEX:{field_name}",
            )
        )

    for field_name, values in spec.allowed_values:
        allowed = _typed_allowed_values(schema_fields[field_name], values)
        rules.append(
            (
                typed[field_name].isNotNull()
                & ~typed[field_name].isin(list(allowed)),
                f"DOMAIN:{field_name}",
            )
        )

    for field_name, minimum, maximum in spec.numeric_ranges:
        condition = F.lit(False)
        if minimum is not None:
            condition = condition | (typed[field_name] < F.lit(minimum))
        if maximum is not None:
            condition = condition | (typed[field_name] > F.lit(maximum))
        rules.append(
            (typed[field_name].isNotNull() & condition, f"RANGE:{field_name}")
        )

    for field_name in spec.non_negative_fields:
        rules.append(
            (
                typed[field_name].isNotNull() & (typed[field_name] < F.lit(0)),
                f"NON_NEGATIVE:{field_name}",
            )
        )

    if dataset == "health_deficiencies":
        rules.append(
            (
                (typed["deficiency_corrected"] == F.lit(CORRECTION_DATE_PRESENT_STATUS))
                & typed["correction_date"].isNull(),
                "CONDITIONAL:correction_date_required",
            )
        )
    elif dataset == "penalties":
        is_fine = typed["penalty_type"] == F.lit("Fine")
        is_denial = typed["penalty_type"] == F.lit("Payment Denial")
        rules.extend(
            [
                (
                    is_fine
                    & (
                        typed["fine_id"].isNull()
                        | typed["fine_amount"].isNull()
                        | typed["payment_denial_start_date"].isNotNull()
                        | typed["payment_denial_length_in_days"].isNotNull()
                    ),
                    "CONDITIONAL:fine_subtype",
                ),
                (
                    is_denial
                    & (
                        typed["payment_denial_start_date"].isNull()
                        | typed["payment_denial_length_in_days"].isNull()
                        | typed["fine_id"].isNotNull()
                        | typed["fine_amount"].isNotNull()
                    ),
                    "CONDITIONAL:payment_denial_subtype",
                ),
            ]
        )
    return rules


def transform_bronze_batch(
    df: Any,
    dataset: str,
    *,
    address_secret_scope: str,
    address_secret_key: str,
) -> SilverTransformResult:
    """Normalize, validate, hash, deduplicate, and split one Bronze batch."""

    from pyspark.sql import functions as F
    from carewatch.schemas import (
        SILVER_BUSINESS_KEYS,
        SILVER_ENTITY_KEYS,
        SILVER_SCHEMAS,
        SOURCE_COLUMNS,
        silver_business_columns,
        silver_validation_spec,
    )

    spec = silver_validation_spec(dataset)
    address_salt_expression = databricks_secret_expression(
        address_secret_scope, address_secret_key
    )
    source_columns = SOURCE_COLUMNS[dataset]
    required_bronze = set(source_columns) | {
        "_source_file",
        "_source_file_sha256",
        "_batch_id",
        "_ingest_date",
    }
    missing = missing_required_columns(df.columns, required_bronze)
    if missing:
        raise SilverSchemaError(
            f"Bronze input for {dataset} is missing required columns: {missing}"
        )

    original_raw_record = F.to_json(
        F.struct(*[F.col(column).alias(column) for column in source_columns]),
        options={"ignoreNullFields": "false"},
    )
    raw = df.select(
        *[
            F.when(F.trim(F.col(column)) == "", F.lit(None))
            .otherwise(F.trim(F.col(column)))
            .alias(column)
            for column in source_columns
        ],
        "_source_file",
        "_source_file_sha256",
        "_batch_id",
        "_ingest_date",
        original_raw_record.alias("_raw_record_original"),
    )

    target_schema = SILVER_SCHEMAS[dataset]
    target_fields = {field.name: field for field in target_schema.fields}
    source_for_target = {target: source for source, target in spec.source_renames}
    business_columns = silver_business_columns(dataset)
    expressions: list[Any] = []
    for column in business_columns:
        if column == "provider_address_hash":
            expressions.append(
                F.when(raw["provider_address"].isNull(), F.lit(None))
                .otherwise(
                    F.sha2(
                        F.concat(
                            F.upper(raw["provider_address"]),
                            address_salt_expression,
                        ),
                        256,
                    )
                )
                .alias(column)
            )
            continue
        if column == "severity_group":
            normalized_severity = F.upper(raw["scope_severity_code"])
            expressions.append(
                F.when(normalized_severity.isin("A", "B", "C"), "A-C")
                .when(normalized_severity.isin("D", "E", "F"), "D-F")
                .when(normalized_severity.isin("G", "H", "I"), "G-I")
                .when(normalized_severity.isin("J", "K", "L"), "J-L")
                .otherwise(F.lit(None))
                .alias(column)
            )
            continue
        source_name = source_for_target.get(column, column)
        value = raw[source_name]
        if column in spec.uppercase_fields:
            value = F.upper(value)
        if column in spec.boolean_yn_fields:
            value = (
                F.when(F.upper(raw[source_name]) == F.lit("Y"), F.lit(True))
                .when(F.upper(raw[source_name]) == F.lit("N"), F.lit(False))
                .otherwise(F.lit(None).cast("boolean"))
            )
        elif target_fields[column].dataType.simpleString() != "string":
            value = _try_cast(source_name, target_fields[column].dataType.simpleString())
        expressions.append(value.alias(column))

    typed = raw.select(
        *expressions,
        _try_cast("processing_date", "date").alias("source_processing_date"),
        raw["_raw_record_original"].alias("_raw_record"),
        *[
            raw[column].alias(f"_raw__{column}")
            for column in source_columns
        ],
        F.col("_source_file").alias("source_file"),
        F.lower(F.col("_source_file_sha256")).alias("source_file_sha256"),
        F.col("_batch_id").alias("source_batch_id"),
    )
    rule_expressions = _validation_rule_expressions(dataset, typed)
    failed_rules = F.filter(
        F.array(*[F.when(condition, F.lit(name)) for condition, name in rule_expressions]),
        lambda value: value.isNotNull(),
    )
    typed = typed.withColumn("_failed_rules", failed_rules)

    key_name = SILVER_ENTITY_KEYS[dataset]
    nonnull_key_fields = tuple(
        name
        for name in SILVER_BUSINESS_KEYS[dataset]
        if not target_fields[name].nullable
    )
    key_ready = F.lit(True)
    for column in nonnull_key_fields:
        key_ready = key_ready & F.col(column).isNotNull()
    typed = typed.withColumn(
        key_name,
        F.when(key_ready, sha256_json_columns(SILVER_BUSINESS_KEYS[dataset])).otherwise(
            F.lit(None).cast("string")
        ),
    ).withColumn("row_hash", sha256_json_columns(business_columns))

    invalid = typed.filter(F.size("_failed_rules") > 0)
    valid = typed.filter(F.size("_failed_rules") == 0)
    conflicts = (
        valid.groupBy(key_name)
        .agg(F.countDistinct("row_hash").alias("_distinct_hashes"))
        .filter(F.col("_distinct_hashes") > 1)
        .select(key_name)
    )
    conflicting = valid.join(conflicts, key_name, "inner").withColumn(
        "_failed_rules", F.array(F.lit("CONFLICTING_BUSINESS_KEY"))
    )
    accepted = (
        valid.join(conflicts, key_name, "left_anti")
        .dropDuplicates([key_name, "row_hash"])
        .withColumn("is_deleted", F.lit(False))
        .withColumn("load_timestamp", F.current_timestamp())
    )

    quarantine_rows = invalid.unionByName(conflicting, allowMissingColumns=True)
    quarantine = (
        quarantine_rows.select(
            F.lit(dataset).alias("dataset"),
            "source_batch_id",
            "source_file",
            "source_file_sha256",
            F.col(key_name).alias("candidate_entity_key"),
            F.col("_raw_record").alias("raw_record"),
            F.array_sort("_failed_rules").alias("failed_rules"),
            F.current_timestamp().alias("load_timestamp"),
        )
        .dropDuplicates(
            [
                "dataset",
                "source_batch_id",
                "source_file",
                "source_file_sha256",
                "candidate_entity_key",
                "raw_record",
                "failed_rules",
            ]
        )
    )
    accepted = accepted.select(*[field.name for field in target_schema.fields])

    rows_read = int(typed.count())
    invalid_count = int(invalid.count())
    conflicting_count = int(conflicting.count())
    valid_nonconflicting_count = int(
        valid.join(conflicts, key_name, "left_anti").count()
    )
    accepted_count = int(accepted.count())
    unique_quarantine_count = int(quarantine.count())
    max_processing_row = accepted.agg(F.max("source_processing_date").alias("value")).first()
    max_processing_date = max_processing_row["value"] if max_processing_row else None
    metric_counts = calculate_transform_metrics(
        rows_read=rows_read,
        invalid_input_rows=invalid_count,
        conflicting_input_rows=conflicting_count,
        valid_nonconflicting_rows=valid_nonconflicting_count,
        rows_accepted=accepted_count,
        unique_quarantine_records=unique_quarantine_count,
    )
    return SilverTransformResult(
        accepted=accepted,
        quarantine=quarantine,
        rows_read=rows_read,
        rows_accepted=accepted_count,
        rows_rejected=metric_counts["rows_rejected"],
        invalid_input_rows=invalid_count,
        conflicting_input_rows=conflicting_count,
        unique_quarantine_records=metric_counts["unique_quarantine_records"],
        exact_valid_duplicates_collapsed=metric_counts[
            "exact_valid_duplicates_collapsed"
        ],
        max_source_processing_date=max_processing_date,
    )


def append_quarantine_idempotently(
    spark: Any, quarantine: Any, table_name: str
) -> int:
    """Append only quarantine rows not already present from an earlier retry."""

    from pyspark.sql import functions as F

    if quarantine.limit(1).count() == 0:
        return 0
    existing = spark.table(table_name).select(
        "dataset",
        "source_batch_id",
        "source_file",
        "source_file_sha256",
        "candidate_entity_key",
        "raw_record",
        F.array_sort("failed_rules").alias("failed_rules"),
    )
    source = quarantine.alias("source")
    target = existing.alias("target")
    identity_columns = (
        "dataset",
        "source_batch_id",
        "source_file",
        "source_file_sha256",
        "candidate_entity_key",
        "raw_record",
        "failed_rules",
    )
    condition = F.lit(True)
    for column in identity_columns:
        condition = condition & F.col(f"source.{column}").eqNullSafe(
            F.col(f"target.{column}")
        )
    new_rows = source.join(target, condition, "left_anti")
    count = int(new_rows.count())
    if count:
        new_rows.write.format("delta").mode("append").saveAsTable(table_name)
    return count


def _latest_delta_metrics(delta_table: Any) -> Mapping[str, str]:
    row = delta_table.history(1).select("operationMetrics").first()
    return dict(row["operationMetrics"] or {}) if row else {}


def merge_silver_batch(
    spark: Any,
    source: Any,
    table_name: str,
    entity_key: str,
) -> MergeMetrics:
    """Idempotently insert or update current rows, rejecting stale backfills."""

    if source.limit(1).count() == 0:
        return MergeMetrics()

    from delta.tables import DeltaTable

    target = DeltaTable.forName(spark, table_name)
    source_columns = source.columns
    update_map = {column: f"source.`{column}`" for column in source_columns}
    insert_map = dict(update_map)
    change_condition = (
        "(target.row_hash <> source.row_hash OR target.is_deleted = true) "
        "AND source.source_processing_date IS NOT NULL "
        "AND (target.source_processing_date IS NULL OR "
        "source.source_processing_date >= target.source_processing_date)"
    )
    (
        target.alias("target")
        .merge(source.alias("source"), f"target.`{entity_key}` = source.`{entity_key}`")
        .whenMatchedUpdate(condition=change_condition, set=update_map)
        .whenNotMatchedInsert(values=insert_map)
        .execute()
    )
    metrics = _latest_delta_metrics(target)
    return MergeMetrics(
        rows_inserted=int(metrics.get("numTargetRowsInserted", 0)),
        rows_updated=int(metrics.get("numTargetRowsUpdated", 0)),
    )


def soft_delete_missing_rows(
    spark: Any,
    snapshot: Any,
    table_name: str,
    entity_key: str,
    snapshot_max_processing_date: date,
) -> int:
    """Soft-delete target keys absent from one separately approved snapshot."""

    from delta.tables import DeltaTable

    target = DeltaTable.forName(spark, table_name)
    keys = snapshot.select(entity_key).distinct()
    (
        target.alias("target")
        .merge(keys.alias("source"), f"target.`{entity_key}` = source.`{entity_key}`")
        .whenNotMatchedBySourceUpdate(
            condition=(
                "target.is_deleted = false AND "
                "target.source_processing_date IS NOT NULL AND "
                f"target.source_processing_date <= DATE '{snapshot_max_processing_date.isoformat()}'"
            ),
            set={"is_deleted": "true", "load_timestamp": "current_timestamp()"},
        )
        .execute()
    )
    metrics = _latest_delta_metrics(target)
    return int(metrics.get("numTargetRowsUpdated", 0))


def table_names(catalog: str, schema_name: str, dataset: str) -> dict[str, str]:
    """Return all qualified objects used by one Silver work item."""

    config = get_dataset(dataset)
    return {
        "bronze": qualified_name(catalog, schema_name, config.bronze_table),
        "silver": qualified_name(catalog, schema_name, config.silver_table),
        "manifest": qualified_name(catalog, schema_name, "source_file_manifest"),
        "logs": qualified_name(catalog, schema_name, "pipeline_execution_logs"),
        "quarantine": qualified_name(catalog, schema_name, "silver_quarantine"),
    }
