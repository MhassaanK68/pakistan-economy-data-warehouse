"""Read successful Bronze extraction checkpoints from Databricks Delta tables."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class AcquisitionWindowState:
    """A complete, reusable API acquisition window from the manifest."""

    acquisition_run_id: str
    dataset: str
    window_start: date
    window_end: date
    batch_ids: tuple[str, ...]
    landing_paths: tuple[str, ...]
    expected_rows: int
    acquired_rows: int
    bronze_complete: bool


def table_exists(spark: Any, table_name: str) -> bool:
    """Return False before setup has created an operational table."""

    return bool(spark.catalog.tableExists(table_name))


def read_successful_watermark(spark: Any, table_name: str, dataset: str) -> date | None:
    """Return the most recent committed Bronze extraction watermark for a dataset.

    This function is intentionally read-only. Raw-to-Bronze advances a
    watermark once, only after every expected page in the acquisition run has
    been validated, reconciled, and committed successfully.
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


def _bronze_success_batch_ids(
    spark: Any, log_table: str, dataset: str, batch_ids: tuple[str, ...]
) -> set[str]:
    if not batch_ids or not table_exists(spark, log_table):
        return set()

    from pyspark.sql import functions as F

    return {
        row["batch_id"]
        for row in (
            spark.table(log_table)
            .filter(F.col("dataset") == F.lit(dataset))
            .filter(F.col("pipeline_layer") == F.lit("Raw-to-Bronze"))
            .filter(F.col("status") == F.lit("SUCCESS"))
            .filter(F.col("batch_id").isin(list(batch_ids)))
            .select("batch_id")
            .distinct()
            .collect()
        )
    }


def _complete_window_state(
    spark: Any,
    log_table: str,
    rows: list[Any],
) -> AcquisitionWindowState | None:
    """Validate one manifest run without trusting a partial page list."""

    if not rows:
        return None

    expected_values = {
        int(row["expected_run_rows"])
        for row in rows
        if row["expected_run_rows"] is not None
    }
    if len(expected_values) != 1:
        return None
    expected_rows = next(iter(expected_values))
    if any(row["source_rows"] is None for row in rows):
        return None
    acquired_rows = sum(int(row["source_rows"]) for row in rows)
    if acquired_rows != expected_rows:
        return None
    if any(
        row["status"] not in {"SUCCESS", "SKIPPED_ALREADY_ACQUIRED"}
        for row in rows
    ):
        return None

    offsets = sorted(int(row["page_offset"]) for row in rows)
    expected_offsets = [index * 500 for index in range(len(rows))]
    if offsets != expected_offsets:
        return None

    paths = tuple(dict.fromkeys(str(row["landing_path"]) for row in rows))
    if not paths or any(not Path(path).is_file() for path in paths):
        return None

    batch_ids = tuple(dict.fromkeys(str(row["batch_id"]) for row in rows))
    bronze_success = _bronze_success_batch_ids(
        spark, log_table, str(rows[0]["dataset"]), batch_ids
    )
    return AcquisitionWindowState(
        acquisition_run_id=str(rows[0]["acquisition_run_id"]),
        dataset=str(rows[0]["dataset"]),
        window_start=rows[0]["window_start"],
        window_end=rows[0]["window_end"],
        batch_ids=batch_ids,
        landing_paths=paths,
        expected_rows=expected_rows,
        acquired_rows=acquired_rows,
        bronze_complete=set(batch_ids).issubset(bronze_success),
    )


def find_complete_api_window(
    spark: Any,
    manifest_table: str,
    log_table: str,
    dataset: str,
    window_start: date,
    window_end: date,
) -> AcquisitionWindowState | None:
    """Return the newest complete exact API window, if its files still exist."""

    if not table_exists(spark, manifest_table):
        return None

    from pyspark.sql import functions as F

    candidates = (
        spark.table(manifest_table)
        .filter(F.col("dataset") == F.lit(dataset))
        .filter(F.col("load_type") == F.lit("incremental"))
        .filter(F.col("acquisition_strategy") == F.lit("api_date_window"))
        .filter(F.col("window_start") == F.lit(window_start))
        .filter(F.col("window_end") == F.lit(window_end))
        .select(
            "acquisition_run_id",
            "dataset",
            "window_start",
            "window_end",
            "page_offset",
            "landing_path",
            "batch_id",
            "expected_run_rows",
            "source_rows",
            "status",
            "load_timestamp",
        )
        .orderBy(F.col("load_timestamp").desc())
        .collect()
    )
    run_ids = tuple(
        dict.fromkeys(str(row["acquisition_run_id"]) for row in candidates)
    )
    for run_id in run_ids:
        state = _complete_window_state(
            spark,
            log_table,
            [row for row in candidates if str(row["acquisition_run_id"]) == run_id],
        )
        if state is not None:
            return state
    return None


def find_pending_api_window(
    spark: Any,
    manifest_table: str,
    log_table: str,
    dataset: str,
    committed_watermark: date,
) -> AcquisitionWindowState | None:
    """Find a complete newer acquisition that has not fully reached Bronze."""

    if not table_exists(spark, manifest_table):
        return None

    from pyspark.sql import functions as F

    candidates = (
        spark.table(manifest_table)
        .filter(F.col("dataset") == F.lit(dataset))
        .filter(F.col("load_type") == F.lit("incremental"))
        .filter(F.col("acquisition_strategy") == F.lit("api_date_window"))
        .filter(F.col("window_end") > F.lit(committed_watermark))
        .select(
            "acquisition_run_id",
            "dataset",
            "window_start",
            "window_end",
            "page_offset",
            "landing_path",
            "batch_id",
            "expected_run_rows",
            "source_rows",
            "status",
            "load_timestamp",
        )
        .orderBy(F.col("load_timestamp").desc())
        .collect()
    )
    run_ids = tuple(
        dict.fromkeys(str(row["acquisition_run_id"]) for row in candidates)
    )
    for run_id in run_ids:
        state = _complete_window_state(
            spark,
            log_table,
            [row for row in candidates if str(row["acquisition_run_id"]) == run_id],
        )
        if state is not None and not state.bronze_complete:
            return state
    return None


def commit_bronze_watermark(
    spark: Any,
    table_name: str,
    *,
    dataset: str,
    watermark_column: str,
    watermark_value: date,
    acquisition_run_id: str,
    bronze_run_id: str,
) -> None:
    """Atomically advance a source watermark after complete Bronze success.

    The Raw-to-Bronze orchestrator must perform its run-level completeness and
    row-count checks before calling this function. A stale retry cannot move the
    watermark backwards.
    """

    if not table_exists(spark, table_name):
        raise RuntimeError(f"Watermark table does not exist: {table_name}")

    from delta.tables import DeltaTable
    from pyspark.sql import functions as F
    from carewatch.audit import WATERMARK_SCHEMA

    now = spark.sql("SELECT current_timestamp() AS now").first()["now"]
    record = {
        "dataset": dataset,
        "watermark_column": watermark_column,
        "watermark_value": watermark_value,
        "last_successful_acquisition_run_id": acquisition_run_id,
        "last_successful_bronze_run_id": bronze_run_id,
        # Retained temporarily so existing Step 4 tables can migrate safely.
        "last_successful_silver_run_id": None,
        "load_timestamp": now,
    }
    source = spark.createDataFrame([record], schema=WATERMARK_SCHEMA).alias("source")
    target = DeltaTable.forName(spark, table_name).alias("target")
    update_condition = (
        F.col("source.watermark_value") >= F.col("target.watermark_value")
    )
    (
        target.merge(source, "target.dataset = source.dataset")
        .whenMatchedUpdateAll(condition=update_condition)
        .whenNotMatchedInsertAll()
        .execute()
    )
