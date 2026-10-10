# Databricks notebook source
"""Remove the accidental history-wide API bootstrap from Landing and controls.

This notebook is intentionally narrow. It targets only Health Deficiencies and
Penalties API acquisitions whose window started at the registry history date and
ended on the explicitly supplied incident date. It never recursively deletes a
directory and refuses to clean any batch that may have reached Bronze or Silver.
"""

# COMMAND ----------

import sys
from datetime import date
from pathlib import Path


def _add_project_src() -> None:
    cwd = Path.cwd()
    for candidate in (cwd / "src", cwd.parent / "src"):
        if (candidate / "carewatch").is_dir():
            value = str(candidate)
            if value not in sys.path:
                sys.path.insert(0, value)
            return
    raise RuntimeError("Could not locate src/carewatch from the Databricks Git folder")


_add_project_src()

from carewatch.config import DATASETS, qualified_name  # noqa: E402
from carewatch.watermarks import table_exists  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("catalog", "carewatch")
dbutils.widgets.text("schema", "pipeline")
dbutils.widgets.text("landing_root", "/Volumes/carewatch/pipeline/landing")
dbutils.widgets.text("mistaken_window_end", "2026-10-10")
dbutils.widgets.text("confirmation", "")

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema")
landing_root = dbutils.widgets.get("landing_root").rstrip("/")
window_end = date.fromisoformat(dbutils.widgets.get("mistaken_window_end"))
confirmation = dbutils.widgets.get("confirmation").strip()
required_confirmation = f"DELETE_MISTAKEN_API_BOOTSTRAP_{window_end:%Y_%m_%d}"

manifest_table = qualified_name(catalog, schema_name, "source_file_manifest")
log_table = qualified_name(catalog, schema_name, "pipeline_execution_logs")
watermark_table = qualified_name(catalog, schema_name, "ingestion_watermarks")

for required_table in (manifest_table, log_table, watermark_table):
    if not table_exists(spark, required_table):
        raise RuntimeError(f"Required control table does not exist: {required_table}")

# COMMAND ----------

from functools import reduce

from pyspark.sql import functions as F


event_datasets = tuple(
    config
    for config in DATASETS.values()
    if config.incremental_strategy == "api_date_window"
    and config.history_start_date is not None
)

history_window_conditions = [
    (F.col("dataset") == F.lit(config.name))
    & (F.col("window_start") == F.lit(config.history_start_date))
    for config in event_datasets
]
history_window_filter = reduce(lambda left, right: left | right, history_window_conditions)

targets = (
    spark.table(manifest_table)
    .filter(F.col("load_type") == F.lit("incremental"))
    .filter(F.col("acquisition_strategy") == F.lit("api_date_window"))
    .filter(F.col("window_end") == F.lit(window_end))
    .filter(history_window_filter)
    .cache()
)

target_rows = targets.count()
target_runs = targets.select("acquisition_run_id").distinct().cache()
target_batches = targets.select("batch_id").distinct().cache()
target_paths = targets.select("dataset", "landing_path").distinct().cache()

preview = (
    targets.groupBy(
        "dataset", "acquisition_run_id", "window_start", "window_end"
    )
    .agg(
        F.count("*").alias("manifest_rows"),
        F.countDistinct("landing_path").alias("physical_files"),
        F.sum("source_rows").alias("source_rows"),
        F.sum("source_bytes").alias("source_bytes"),
        F.min("load_timestamp").alias("first_manifest_time"),
        F.max("load_timestamp").alias("last_manifest_time"),
    )
    .orderBy("dataset", "first_manifest_time")
)

print("Cleanup preview — no files or table rows have been deleted.")
display(preview)
print(
    f"Matched {target_rows:,} manifest rows, {target_runs.count():,} acquisition runs, "
    f"and {target_paths.count():,} distinct landing files."
)
display(target_paths.orderBy("dataset", "landing_path").limit(100))

if target_rows == 0:
    dbutils.notebook.exit("Nothing matched the guarded cleanup scope.")

# COMMAND ----------

# Even a failed downstream attempt may have committed data before logging.
downstream_attempts = (
    spark.table(log_table)
    .filter(F.col("pipeline_layer").isin("Raw-to-Bronze", "Bronze-to-Silver"))
    .join(F.broadcast(target_batches), on="batch_id", how="inner")
)
if downstream_attempts.limit(1).count():
    display(
        downstream_attempts.select(
            "run_id", "batch_id", "pipeline_layer", "dataset", "status", "load_timestamp"
        ).orderBy("load_timestamp")
    )
    raise RuntimeError(
        "Cleanup blocked: at least one target batch has a downstream processing log."
    )

# Verify physical Bronze tables as well as logs.
for config in event_datasets:
    bronze_table = qualified_name(catalog, schema_name, config.bronze_table)
    if table_exists(spark, bronze_table) and "_batch_id" in spark.table(bronze_table).columns:
        present = (
            spark.table(bronze_table)
            .select(F.col("_batch_id").alias("batch_id"))
            .join(F.broadcast(target_batches), on="batch_id", how="inner")
            .limit(1)
            .count()
        )
        if present:
            raise RuntimeError(
                f"Cleanup blocked: a target batch exists in Bronze table {bronze_table}."
            )

# No source watermark may depend on a target acquisition run.
watermarks = spark.table(watermark_table).alias("watermarks")
runs_for_watermark = target_runs.alias("cleanup_runs")
watermark_references = watermarks.join(
    F.broadcast(runs_for_watermark),
    F.col("watermarks.last_successful_acquisition_run_id")
    == F.col("cleanup_runs.acquisition_run_id"),
    "inner",
)
if watermark_references.limit(1).count():
    display(watermark_references)
    raise RuntimeError(
        "Cleanup blocked: ingestion_watermarks references a target acquisition run."
    )

# Target files cannot be referenced by retained manifest rows.
retained_manifest = spark.table(manifest_table).join(
    F.broadcast(target_runs), on="acquisition_run_id", how="left_anti"
)
shared_paths = retained_manifest.join(
    F.broadcast(target_paths.select("landing_path")), on="landing_path", how="inner"
)
if shared_paths.limit(1).count():
    display(shared_paths.select("acquisition_run_id", "dataset", "landing_path"))
    raise RuntimeError(
        "Cleanup blocked: at least one landing file is referenced by a retained manifest."
    )

# Every exact file must remain inside its expected dataset folder.
root = Path(landing_root).resolve()
validated_paths: list[Path] = []
for row in target_paths.collect():
    dataset = str(row["dataset"])
    candidate = Path(str(row["landing_path"])).resolve()
    expected_parent = (root / "incremental" / dataset).resolve()
    try:
        candidate.relative_to(expected_parent)
    except ValueError as exc:
        raise RuntimeError(
            f"Cleanup blocked: path escapes expected dataset directory: {candidate}"
        ) from exc
    if candidate.exists() and not candidate.is_file():
        raise RuntimeError(f"Cleanup blocked: target is not a regular file: {candidate}")
    validated_paths.append(candidate)

print("All cleanup safety checks passed.")
print(f"To delete these files and rows, set confirmation to: {required_confirmation}")
if confirmation != required_confirmation:
    dbutils.notebook.exit("PREVIEW_ONLY: confirmation token was not supplied.")

# COMMAND ----------

# Delete exact manifest-resolved files only. Directories are deliberately left
# in place; this notebook never performs recursive deletion.
deleted_files = 0
already_missing = 0
for candidate in dict.fromkeys(validated_paths):
    if candidate.exists():
        candidate.unlink()
        deleted_files += 1
    else:
        already_missing += 1

# Only after every file operation succeeds do we remove acquisition rows. If a
# file deletion fails, manifests/logs remain so the operation can be recovered.
from delta.tables import DeltaTable

(
    DeltaTable.forName(spark, manifest_table)
    .alias("target")
    .merge(
        target_runs.alias("cleanup"),
        "target.acquisition_run_id = cleanup.acquisition_run_id",
    )
    .whenMatchedDelete()
    .execute()
)
(
    DeltaTable.forName(spark, log_table)
    .alias("target")
    .merge(target_runs.alias("cleanup"), "target.run_id = cleanup.acquisition_run_id")
    .whenMatchedDelete()
    .execute()
)

remaining_manifest_rows = (
    spark.table(manifest_table)
    .join(F.broadcast(target_runs), on="acquisition_run_id", how="inner")
    .count()
)
logs_after = spark.table(log_table).alias("logs_after")
runs_after = target_runs.alias("runs_after")
remaining_log_rows = (
    logs_after.join(
        F.broadcast(runs_after),
        F.col("logs_after.run_id") == F.col("runs_after.acquisition_run_id"),
        "inner",
    ).count()
)
if remaining_manifest_rows or remaining_log_rows:
    raise RuntimeError(
        "Cleanup verification failed: matching control rows remain. "
        f"manifest={remaining_manifest_rows}, logs={remaining_log_rows}"
    )

print(
    f"Cleanup complete: deleted {deleted_files:,} files; "
    f"{already_missing:,} paths were already absent; removed {target_rows:,} manifest rows "
    f"and their CMS-to-Landing execution logs."
)
