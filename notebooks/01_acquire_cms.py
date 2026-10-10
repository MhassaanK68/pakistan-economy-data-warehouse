
"""Acquire CMS full snapshots or incrementals into a Unity Catalog Volume."""

import sys
from datetime import datetime, timezone
from pathlib import Path


def _add_project_src() -> None:
    """Support both repo-root and notebooks-directory working directories."""

    cwd = Path.cwd()
    candidates = (cwd / "src", cwd.parent / "src")
    for candidate in candidates:
        if (candidate / "carewatch").is_dir():
            value = str(candidate)
            if value not in sys.path:
                sys.path.insert(0, value)
            return
    raise RuntimeError("Could not locate src/carewatch from the Databricks Git folder")


_add_project_src()

from carewatch.acquire import (  # noqa: E402
    AcquisitionRequest,
    acquire_dataset,
    parse_bool,
)
from carewatch.audit import (  # noqa: E402
    LOG_SCHEMA,
    MANIFEST_SCHEMA,
    append_typed_records,
    completed_log_record,
    utc_now,
    write_execution_log,
)
from carewatch.config import DATASETS, qualified_name  # noqa: E402
from carewatch.watermarks import read_successful_watermark, table_exists  # noqa: E402

# COMMAND ----------

dbutils.widgets.dropdown("load_type", "incremental", ["full", "incremental"])
dbutils.widgets.text("overlap_days", "60")
dbutils.widgets.dropdown("force_refresh", "false", ["false", "true"])
dbutils.widgets.text("catalog", "carewatch")
dbutils.widgets.text("schema", "pipeline")
dbutils.widgets.text("landing_root", "/Volumes/carewatch/pipeline/landing")

PARAMETERS = {
    name: dbutils.widgets.get(name)
    for name in (
        "load_type",
        "overlap_days",
        "force_refresh",
        "catalog",
        "schema",
        "landing_root",
    )
}

# COMMAND ----------

as_of_date = datetime.now(timezone.utc).date()
force_refresh = parse_bool(PARAMETERS["force_refresh"], "force_refresh")

try:
    overlap_days = int(PARAMETERS["overlap_days"])
except ValueError as exc:
    raise ValueError("overlap_days must be a whole number") from exc

manifest_table = qualified_name(
    PARAMETERS["catalog"], PARAMETERS["schema"], "source_file_manifest"
)
watermark_table = qualified_name(
    PARAMETERS["catalog"], PARAMETERS["schema"], "ingestion_watermarks"
)
execution_log_table = qualified_name(
    PARAMETERS["catalog"], PARAMETERS["schema"], "pipeline_execution_logs"
)

from pyspark.sql import functions as F


def existing_content_files(dataset_name: str) -> dict[str, str]:
    """Map stable source-content hashes to already completed landing files."""

    if not table_exists(spark, manifest_table):
        return {}
    rows = (
        spark.table(manifest_table)
        .filter(F.col("dataset") == F.lit(dataset_name))
        .filter(F.col("status").isin("SUCCESS", "SKIPPED_ALREADY_ACQUIRED"))
        .select("source_content_sha256", "landing_path", "load_timestamp")
        .orderBy(F.col("load_timestamp").desc())
        .collect()
    )
    # Ordered newest-first so the first path wins if historical manifests repeat a hash.
    existing: dict[str, str] = {}
    for row in rows:
        path = row["landing_path"]
        if path and Path(path).is_file():
            existing.setdefault(row["source_content_sha256"], path)
    return existing

# COMMAND ----------

manifest_records = []
failures = []

for dataset_config in DATASETS.values():
    attempt_started = utc_now()
    last_watermark = None
    start_date = None
    if (
        PARAMETERS["load_type"] == "incremental"
        and dataset_config.incremental_strategy == "api_date_window"
    ):
        last_watermark = read_successful_watermark(
            spark, watermark_table, dataset_config.name
        )
        if last_watermark is None:
            start_date = dataset_config.history_start_date

    request = AcquisitionRequest(
        dataset=dataset_config.name,
        load_type=PARAMETERS["load_type"],
        as_of_date=as_of_date,
        start_date=start_date,
        overlap_days=overlap_days,
        force_refresh=force_refresh,
        landing_root=PARAMETERS["landing_root"],
    )

    try:
        result = acquire_dataset(
            request,
            last_successful_watermark=last_watermark,
            existing_files_by_content_hash=existing_content_files(
                dataset_config.name
            ),
        )
        records = [item.as_manifest_record() for item in result.files]
        if not records:
            raise RuntimeError("Acquisition completed without a manifest record")

        append_typed_records(spark, manifest_table, records, MANIFEST_SCHEMA)
        manifest_records.extend(records)

        attempt_finished = utc_now()
        execution_records = []
        for item in result.files:
            source_rows = (
                item.source_rows
                if item.source_rows is not None
                else item.expected_run_rows or 0
            )
            execution_records.append(
                completed_log_record(
                    run_id=item.acquisition_run_id,
                    batch_id=item.batch_id,
                    pipeline_layer="CMS-to-Landing",
                    dataset=item.dataset,
                    load_type=item.load_type,
                    parameter_processed=item.source_url,
                    start_time=attempt_started,
                    end_time=attempt_finished,
                    status=item.status,
                    rows_read=source_rows,
                    rows_inserted=(
                        source_rows if item.status == "SUCCESS" else 0
                    ),
                    error_message=item.error_message,
                )
            )
        append_typed_records(
            spark, execution_log_table, execution_records, LOG_SCHEMA
        )
    except Exception as exc:
        failure_message = f"{type(exc).__name__}: {exc}"[:2000]
        failures.append(
            {
                "dataset": dataset_config.name,
                "load_type": PARAMETERS["load_type"],
                "as_of_date": as_of_date.isoformat(),
                "error": failure_message,
            }
        )
        try:
            write_execution_log(
                spark,
                execution_log_table,
                completed_log_record(
                    run_id=request.acquisition_run_id,
                    batch_id="",
                    pipeline_layer="CMS-to-Landing",
                    dataset=dataset_config.name,
                    load_type=PARAMETERS["load_type"],
                    parameter_processed=(
                        f"dataset={dataset_config.name};as_of_date={as_of_date.isoformat()}"
                    ),
                    start_time=attempt_started,
                    status="FAILURE",
                    error_message=failure_message,
                ),
            )
        except Exception as log_error:
            failures[-1]["error"] = (
                f"{failure_message}; audit write also failed: {log_error}"
            )[:2000]

# COMMAND ----------

if manifest_records:
    summary = spark.createDataFrame(manifest_records, schema=MANIFEST_SCHEMA).select(
        "acquisition_run_id",
        "dataset",
        "load_type",
        "acquisition_strategy",
        "status",
        "page_offset",
        "source_rows",
        "expected_run_rows",
        "source_bytes",
        "landing_path",
    )
    display(summary.orderBy("dataset", F.col("page_offset").asc_nulls_first()))

if failures:
    display(spark.createDataFrame(failures).orderBy("dataset"))
    failed_names = ", ".join(item["dataset"] for item in failures)
    raise RuntimeError(f"Acquisition failed for: {failed_names}")