"""Databricks orchestration for manifest-driven Raw-to-Bronze ingestion."""

# COMMAND ----------

import sys
import uuid
from pathlib import Path


def _add_project_src() -> None:
    """Support both repo-root and notebooks-directory working directories."""

    cwd = Path.cwd()
    for candidate in (cwd / "src", cwd.parent / "src"):
        if (candidate / "carewatch").is_dir():
            value = str(candidate)
            if value not in sys.path:
                sys.path.insert(0, value)
            return
    raise RuntimeError("Could not locate src/carewatch from the Databricks Git folder")


_add_project_src()

from carewatch.acquire import parse_bool  # noqa: E402
from carewatch.audit import audited, ensure_control_tables  # noqa: E402
from carewatch.bronze import (  # noqa: E402
    process_manifest_item,
    reconcile_acquisition_run,
    select_manifest_items,
)
from carewatch.config import DATASETS, qualified_name  # noqa: E402

# COMMAND ----------

dbutils.widgets.dropdown("dataset", "all", ["all", *sorted(DATASETS)])
dbutils.widgets.dropdown("load_type", "all", ["all", "full", "incremental"])
dbutils.widgets.text("acquisition_run_id", "")
dbutils.widgets.text("source_path", "")
dbutils.widgets.dropdown("reprocess", "false", ["false", "true"])
dbutils.widgets.text("catalog", "carewatch")
dbutils.widgets.text("schema", "pipeline")

PARAMETERS = {
    name: dbutils.widgets.get(name).strip()
    for name in (
        "dataset",
        "load_type",
        "acquisition_run_id",
        "source_path",
        "reprocess",
        "catalog",
        "schema",
    )
}

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", "UTC")

catalog = PARAMETERS["catalog"]
schema_name = PARAMETERS["schema"]
dataset = None if PARAMETERS["dataset"] == "all" else PARAMETERS["dataset"]
load_type = None if PARAMETERS["load_type"] == "all" else PARAMETERS["load_type"]
acquisition_run_id = PARAMETERS["acquisition_run_id"] or None
source_path = PARAMETERS["source_path"] or None
reprocess = parse_bool(PARAMETERS["reprocess"], "reprocess")
run_id = str(uuid.uuid4())

# Re-running setup here is cheap and applies additive control-table migrations
# before any file is processed. Bronze data tables themselves are created lazily.
ensure_control_tables(spark, catalog, schema_name)

manifest_table = qualified_name(catalog, schema_name, "source_file_manifest")
log_table = qualified_name(catalog, schema_name, "pipeline_execution_logs")
drift_table = qualified_name(catalog, schema_name, "schema_drift_log")
quarantine_table = qualified_name(catalog, schema_name, "bronze_quarantine")
watermark_table = qualified_name(catalog, schema_name, "ingestion_watermarks")

selection = select_manifest_items(
    spark,
    manifest_table,
    log_table,
    dataset=dataset,
    load_type=load_type,
    acquisition_run_id=acquisition_run_id,
    source_path=source_path,
    reprocess=reprocess,
)

print(
    f"Raw-to-Bronze run_id={run_id}; "
    f"files_to_process={len(selection.process_items)}; "
    f"acquisition_scope_files={len(selection.scope_items)}"
)

# COMMAND ----------

file_results = []
file_failures = []

for item in selection.process_items:
    parameter = (
        f"acquisition_run_id={item.acquisition_run_id};"
        f"strategy={item.acquisition_strategy};path={item.landing_path}"
    )
    try:
        result = None
        with audited(
            spark,
            log_table,
            run_id,
            "Raw-to-Bronze",
            item.dataset,
            item.load_type,
            parameter,
            item.batch_id,
        ) as record:
            result = process_manifest_item(
                spark,
                item,
                run_id=run_id,
                catalog=catalog,
                schema_name=schema_name,
                manifest_table=manifest_table,
                drift_table=drift_table,
                quarantine_table=quarantine_table,
            )
            record["rows_read"] = result.rows_read
            record["rows_inserted"] = result.rows_inserted
            record["rows_quarantined"] = result.rows_quarantined
        file_results.append(
            {
                "acquisition_run_id": result.acquisition_run_id,
                "dataset": result.dataset,
                "batch_id": result.batch_id,
                "status": (
                    "QUARANTINED_PARTIAL"
                    if result.rows_quarantined
                    else "SUCCESS"
                ),
                "rows_read": result.rows_read,
                "rows_inserted": result.rows_inserted,
                "rows_quarantined": result.rows_quarantined,
                "added_columns": list(result.added_columns),
            }
        )
    except Exception as exc:
        # The audited context has already persisted the failure. Continue so a
        # bad file/page cannot prevent independent files from reaching Bronze.
        file_failures.append(
            {
                "acquisition_run_id": item.acquisition_run_id,
                "dataset": item.dataset,
                "batch_id": item.batch_id,
                "source_file": item.landing_path,
                "error": f"{type(exc).__name__}: {exc}"[:2000],
            }
        )

# COMMAND ----------

# Reconcile every acquisition run in scope, including the recovery case where
# its data/logs committed previously but row_count_validated or the watermark did not.
run_keys = tuple(
    dict.fromkeys(
        (item.acquisition_run_id, item.dataset) for item in selection.scope_items
    )
)
reconciliations = [
    reconcile_acquisition_run(
        spark,
        acquisition_run_id=scope_run_id,
        dataset=scope_dataset,
        bronze_run_id=run_id,
        manifest_table=manifest_table,
        log_table=log_table,
        watermark_table=watermark_table,
        require_bronze_run_id=run_id if reprocess else None,
    )
    for scope_run_id, scope_dataset in run_keys
]

# COMMAND ----------

from pyspark.sql.types import (  # noqa: E402
    ArrayType,
    BooleanType,
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
)


FILE_RESULT_SCHEMA = StructType(
    [
        StructField("acquisition_run_id", StringType(), False),
        StructField("dataset", StringType(), False),
        StructField("batch_id", StringType(), False),
        StructField("status", StringType(), False),
        StructField("rows_read", LongType(), False),
        StructField("rows_inserted", LongType(), False),
        StructField("rows_quarantined", LongType(), False),
        StructField("added_columns", ArrayType(StringType(), False), False),
    ]
)

FAILURE_SCHEMA = StructType(
    [
        StructField("acquisition_run_id", StringType(), False),
        StructField("dataset", StringType(), False),
        StructField("batch_id", StringType(), False),
        StructField("source_file", StringType(), False),
        StructField("error", StringType(), False),
    ]
)

RECONCILIATION_SCHEMA = StructType(
    [
        StructField("acquisition_run_id", StringType(), False),
        StructField("dataset", StringType(), False),
        StructField("complete", BooleanType(), False),
        StructField("expected_rows", LongType(), True),
        StructField("parsed_rows", LongType(), True),
        StructField("watermark_value", DateType(), True),
        StructField("detail", StringType(), False),
    ]
)

if file_results:
    display(
        spark.createDataFrame(file_results, FILE_RESULT_SCHEMA).orderBy(
            "dataset", "acquisition_run_id", "batch_id"
        )
    )
if file_failures:
    display(
        spark.createDataFrame(file_failures, FAILURE_SCHEMA).orderBy(
            "dataset", "acquisition_run_id", "batch_id"
        )
    )
if reconciliations:
    display(
        spark.createDataFrame(
            [result.__dict__ for result in reconciliations],
            RECONCILIATION_SCHEMA,
        ).orderBy("dataset", "acquisition_run_id")
    )

partial_runs = [result for result in reconciliations if not result.complete]
if file_failures or partial_runs:
    print(
        "Raw-to-Bronze completed with partial/failed inputs. "
        "Successful independent files were committed, but affected acquisition "
        "runs were not checkpointed. Inspect the displayed results and Delta logs."
    )
elif not selection.scope_items:
    print("No pending or unvalidated manifest records matched the parameters.")
else:
    print("Raw-to-Bronze completed successfully; eligible watermarks were advanced.")
