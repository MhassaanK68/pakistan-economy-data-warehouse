# Databricks notebook source
"""Acquire CMS full snapshots or incrementals into a Unity Catalog Volume."""

# COMMAND ----------

import sys
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
    parse_iso_date,
)
from carewatch.config import get_dataset, qualified_name  # noqa: E402
from carewatch.watermarks import read_successful_watermark, table_exists  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("dataset", "health_deficiencies")
dbutils.widgets.dropdown("load_type", "full", ["full", "incremental"])
dbutils.widgets.text("as_of_date", "")
dbutils.widgets.text("start_date", "")
dbutils.widgets.text("overlap_days", "60")
dbutils.widgets.dropdown("force_refresh", "false", ["false", "true"])
dbutils.widgets.text("catalog", "carewatch")
dbutils.widgets.text("schema", "pipeline")
dbutils.widgets.text("landing_root", "/Volumes/carewatch/pipeline/landing")

PARAMETERS = {
    name: dbutils.widgets.get(name)
    for name in (
        "dataset",
        "load_type",
        "as_of_date",
        "start_date",
        "overlap_days",
        "force_refresh",
        "catalog",
        "schema",
        "landing_root",
    )
}

# COMMAND ----------

dataset_config = get_dataset(PARAMETERS["dataset"])
as_of_date = parse_iso_date(PARAMETERS["as_of_date"], "as_of_date", required=True)
start_date = parse_iso_date(PARAMETERS["start_date"], "start_date")
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

request = AcquisitionRequest(
    dataset=dataset_config.name,
    load_type=PARAMETERS["load_type"],
    as_of_date=as_of_date,
    start_date=start_date,
    overlap_days=overlap_days,
    force_refresh=force_refresh,
    landing_root=PARAMETERS["landing_root"],
)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
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


def existing_content_files() -> dict[str, str]:
    """Map stable source-content hashes to already completed landing files."""

    if not table_exists(spark, manifest_table):
        return {}
    rows = (
        spark.table(manifest_table)
        .filter(F.col("dataset") == F.lit(dataset_config.name))
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


last_watermark = None
if (
    request.load_type == "incremental"
    and dataset_config.incremental_strategy == "api_date_window"
    and request.start_date is None
):
    last_watermark = read_successful_watermark(
        spark, watermark_table, dataset_config.name
    )

# COMMAND ----------

result = acquire_dataset(
    request,
    last_successful_watermark=last_watermark,
    existing_files_by_content_hash=existing_content_files(),
)

manifest_records = [item.as_manifest_record() for item in result.files]
if not manifest_records:
    raise RuntimeError("Acquisition completed without producing a manifest record")

(
    spark.createDataFrame(manifest_records, schema=MANIFEST_SCHEMA)
    .write.format("delta")
    .mode("append")
    .saveAsTable(manifest_table)
)

# COMMAND ----------

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
display(summary.orderBy(F.col("page_offset").asc_nulls_first()))
