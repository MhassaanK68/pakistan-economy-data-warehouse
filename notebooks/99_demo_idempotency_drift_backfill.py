"""Evidence views for Raw-to-Bronze idempotency, drift, and backfill scope."""

# COMMAND ----------

import sys
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

from carewatch.config import DATASETS, get_dataset, qualified_name  # noqa: E402

# COMMAND ----------

dbutils.widgets.dropdown(
    "dataset", "health_deficiencies", sorted(DATASETS)
)
dbutils.widgets.text("acquisition_run_id", "")
dbutils.widgets.text("batch_id", "")
dbutils.widgets.text("catalog", "carewatch")
dbutils.widgets.text("schema", "pipeline")

PARAMETERS = {
    name: dbutils.widgets.get(name).strip()
    for name in ("dataset", "acquisition_run_id", "batch_id", "catalog", "schema")
}

spark.conf.set("spark.sql.session.timeZone", "UTC")

dataset = PARAMETERS["dataset"]
config = get_dataset(dataset)
catalog = PARAMETERS["catalog"]
schema_name = PARAMETERS["schema"]
acquisition_run_id = PARAMETERS["acquisition_run_id"] or None
batch_id = PARAMETERS["batch_id"] or None

manifest_table = qualified_name(catalog, schema_name, "source_file_manifest")
log_table = qualified_name(catalog, schema_name, "pipeline_execution_logs")
drift_table = qualified_name(catalog, schema_name, "schema_drift_log")
quarantine_table = qualified_name(catalog, schema_name, "bronze_quarantine")
watermark_table = qualified_name(catalog, schema_name, "ingestion_watermarks")
bronze_table = qualified_name(catalog, schema_name, config.bronze_table)

required_tables = (manifest_table, log_table, drift_table, quarantine_table)
missing_tables = [
    table for table in required_tables if not spark.catalog.tableExists(table)
]
if missing_tables:
    raise RuntimeError(
        "Run 00_setup_tables.py and 02_raw_to_bronze.py first. Missing: "
        + ", ".join(missing_tables)
    )

# COMMAND ----------

from pyspark.sql import functions as F  # noqa: E402


manifest = spark.table(manifest_table).filter(F.col("dataset") == F.lit(dataset))
if acquisition_run_id:
    manifest = manifest.filter(
        F.col("acquisition_run_id") == F.lit(acquisition_run_id)
    )
if batch_id:
    manifest = manifest.filter(F.col("batch_id") == F.lit(batch_id))

if manifest.limit(1).count() == 0:
    raise ValueError("No manifest rows match the selected evidence scope")

scope_batches = manifest.select("batch_id").distinct()

display(
    manifest.select(
        "acquisition_run_id",
        "dataset",
        "load_type",
        "acquisition_strategy",
        "page_offset",
        "batch_id",
        "expected_run_rows",
        "source_rows",
        "row_count_validated",
        "status",
        "landing_path",
        "load_timestamp",
    ).orderBy("acquisition_run_id", F.col("page_offset").asc_nulls_first())
)

# COMMAND ----------

if spark.catalog.tableExists(bronze_table):
    bronze_counts = (
        spark.table(bronze_table)
        .select(F.col("_batch_id").alias("batch_id"))
        .join(scope_batches, "batch_id", "inner")
        .groupBy("batch_id")
        .agg(F.count(F.lit(1)).alias("bronze_rows"))
    )
else:
    bronze_counts = scope_batches.withColumn("bronze_rows", F.lit(0).cast("long"))

quarantine_counts = (
    spark.table(quarantine_table)
    .filter(F.col("dataset") == F.lit(dataset))
    .join(scope_batches, "batch_id", "inner")
    .groupBy("batch_id")
    .agg(F.count(F.lit(1)).alias("quarantine_rows"))
)

batch_evidence = (
    manifest.select(
        "acquisition_run_id",
        "batch_id",
        "source_rows",
        "row_count_validated",
    )
    .dropDuplicates(["acquisition_run_id", "batch_id"])
    .join(bronze_counts, "batch_id", "left")
    .join(quarantine_counts, "batch_id", "left")
    .fillna({"bronze_rows": 0, "quarantine_rows": 0})
    .withColumn(
        "persisted_rows",
        F.col("bronze_rows") + F.col("quarantine_rows"),
    )
    .withColumn(
        "count_conserved",
        F.when(F.col("source_rows").isNull(), F.lit(False)).otherwise(
            F.col("source_rows") == F.col("persisted_rows")
        ),
    )
)

display(batch_evidence.orderBy("acquisition_run_id", "batch_id"))

# COMMAND ----------

raw_logs = (
    spark.table(log_table)
    .filter(F.col("pipeline_layer") == F.lit("Raw-to-Bronze"))
    .filter(F.col("dataset") == F.lit(dataset))
    .join(scope_batches, "batch_id", "inner")
)

display(
    raw_logs.select(
        "run_id",
        "batch_id",
        "status",
        "rows_read",
        "rows_inserted",
        "rows_quarantined",
        "start_time",
        "end_time",
        "error_message",
    ).orderBy("batch_id", "start_time")
)

idempotency_evidence = (
    raw_logs.filter(F.col("status") == F.lit("SUCCESS"))
    .groupBy("batch_id")
    .agg(
        F.count(F.lit(1)).alias("successful_attempts"),
        F.max("rows_inserted").alias("expected_current_bronze_rows"),
    )
    .join(bronze_counts, "batch_id", "left")
    .withColumn(
        "idempotent_count_ok",
        F.col("bronze_rows") == F.col("expected_current_bronze_rows"),
    )
)

display(idempotency_evidence.orderBy("batch_id"))

# COMMAND ----------

drift = (
    spark.table(drift_table)
    .filter(F.col("dataset") == F.lit(dataset))
    .join(scope_batches, "batch_id", "inner")
)
display(
    drift.select(
        "run_id",
        "batch_id",
        "drift_type",
        "column_name",
        "detail",
        "source_file",
        "load_timestamp",
    ).orderBy("load_timestamp", "column_name")
)

if config.watermark_col and spark.catalog.tableExists(watermark_table):
    display(
        spark.table(watermark_table)
        .filter(F.col("dataset") == F.lit(dataset))
        .select(
            "dataset",
            "watermark_column",
            "watermark_value",
            "last_successful_acquisition_run_id",
            "last_successful_bronze_run_id",
            "load_timestamp",
        )
    )

print(
    "Evidence ready. For the idempotency screenshot, run 02_raw_to_bronze.py "
    "again with the same source_path and reprocess=true, then rerun this notebook. "
    "successful_attempts should increase while bronze_rows stays equal to the "
    "single-batch expected row count. acquisition_run_id and batch_id provide the "
    "same targeted scope used for historical backfill/reprocessing evidence."
)
