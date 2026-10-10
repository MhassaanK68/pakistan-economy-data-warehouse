"""Databricks orchestration for manifest-driven Bronze-to-Silver processing."""

# COMMAND ----------

import sys
import uuid
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

from carewatch.acquire import parse_bool, parse_iso_date  # noqa: E402
from carewatch.audit import (  # noqa: E402
    SILVER_QUARANTINE_SCHEMA,
    audited,
    silver_quarantine_schema_state,
    suppress_audit_log,
)
from carewatch.config import DATASETS, qualified_name  # noqa: E402
from carewatch.schemas import SILVER_ENTITY_KEYS, SILVER_SCHEMAS  # noqa: E402
from carewatch.silver import (  # noqa: E402
    append_quarantine_idempotently,
    evaluate_soft_delete,
    is_latest_validated_snapshot,
    merge_silver_batch,
    require_exact_table_schema,
    SilverProcessingError,
    select_bronze_batch,
    select_silver_work_items,
    soft_delete_missing_rows,
    table_names,
    transform_bronze_batch,
)

# COMMAND ----------

dbutils.widgets.dropdown("dataset", "all", ["all", *sorted(DATASETS)])
dbutils.widgets.dropdown("load_type", "all", ["all", "full", "incremental"])
dbutils.widgets.text("acquisition_run_id", "")
dbutils.widgets.text("batch_ids", "")
dbutils.widgets.text("ingest_from", "")
dbutils.widgets.text("ingest_to", "")
dbutils.widgets.dropdown("reprocess", "false", ["false", "true"])
dbutils.widgets.dropdown("allow_soft_deletes", "false", ["false", "true"])
dbutils.widgets.text("address_secret_scope", "")
dbutils.widgets.text("address_secret_key", "")
dbutils.widgets.text("catalog", "carewatch")
dbutils.widgets.text("schema", "pipeline")

PARAMETERS = {
    name: dbutils.widgets.get(name).strip()
    for name in (
        "dataset",
        "load_type",
        "acquisition_run_id",
        "batch_ids",
        "ingest_from",
        "ingest_to",
        "reprocess",
        "allow_soft_deletes",
        "address_secret_scope",
        "address_secret_key",
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
batch_ids = tuple(
    value.strip() for value in PARAMETERS["batch_ids"].split(",") if value.strip()
)
ingest_from = parse_iso_date(PARAMETERS["ingest_from"], "ingest_from")
ingest_to = parse_iso_date(PARAMETERS["ingest_to"], "ingest_to")
reprocess = parse_bool(PARAMETERS["reprocess"], "reprocess")
allow_soft_deletes = parse_bool(
    PARAMETERS["allow_soft_deletes"], "allow_soft_deletes"
)
if ingest_from and ingest_to and ingest_from > ingest_to:
    raise ValueError("ingest_from cannot be later than ingest_to")

manifest_table = qualified_name(catalog, schema_name, "source_file_manifest")
log_table = qualified_name(catalog, schema_name, "pipeline_execution_logs")
quarantine_table = qualified_name(catalog, schema_name, "silver_quarantine")

quarantine_state = silver_quarantine_schema_state(spark, quarantine_table)
if quarantine_state != "CURRENT":
    raise RuntimeError(
        f"Step 7 requires the current eight-column silver_quarantine contract; "
        f"{quarantine_table} is {quarantine_state}. Run the separate migration "
        "notebook in preview mode and obtain approval before preparing or activating it."
    )
require_exact_table_schema(spark, quarantine_table, SILVER_QUARANTINE_SCHEMA)

datasets_to_verify = (dataset,) if dataset else tuple(sorted(DATASETS))
for dataset_name in datasets_to_verify:
    names = table_names(catalog, schema_name, dataset_name)
    require_exact_table_schema(spark, names["silver"], SILVER_SCHEMAS[dataset_name])
    if not spark.catalog.tableExists(names["bronze"]):
        raise RuntimeError(f"Required Bronze table does not exist: {names['bronze']}")

secret_scope = PARAMETERS["address_secret_scope"]
secret_key = PARAMETERS["address_secret_key"]
if not secret_scope or not secret_key:
    raise RuntimeError(
        "address_secret_scope and address_secret_key are required; the salt is "
        "retrieved only from Databricks Secrets"
    )
# The secret value is intentionally not retrieved into Python. The reusable
# transformation resolves it inside Spark with Databricks SQL secret(scope,key),
# where Databricks applies its normal best-effort secret redaction. Do not
# select or log the secret expression itself.

run_id = str(uuid.uuid4())
work_items = select_silver_work_items(
    spark,
    manifest_table,
    log_table,
    dataset=dataset,
    load_type=load_type,
    acquisition_run_id=acquisition_run_id,
    batch_ids=batch_ids,
    reprocess=reprocess,
)
subset_or_reprocess = bool(
    acquisition_run_id or batch_ids or ingest_from or ingest_to or reprocess
)

print(f"Bronze-to-Silver run_id={run_id}; batches_selected={len(work_items)}")

# COMMAND ----------

results = []
failures = []
date_filtered_batches = []

for item in work_items:
    names = table_names(catalog, schema_name, item.dataset)
    parameter = (
        f"acquisition_run_id={item.acquisition_run_id};"
        f"strategy={item.acquisition_strategy};batch_id={item.batch_id};"
        f"ingest_from={ingest_from or ''};ingest_to={ingest_to or ''};"
        f"reprocess={str(reprocess).lower()};"
        f"allow_soft_deletes={str(allow_soft_deletes).lower()}"
    )
    try:
        transformed = None
        decision = None
        with audited(
            spark,
            log_table,
            run_id,
            "Bronze-to-Silver",
            item.dataset,
            item.load_type,
            parameter,
            item.batch_id,
        ) as record:
            # Materialize the unfiltered batch inside the audited boundary so
            # missing/corrupt table reads produce a failure log where possible.
            unfiltered_bronze = select_bronze_batch(
                spark, names["bronze"], item.batch_id
            )
            unfiltered_rows = int(unfiltered_bronze.count())
            if unfiltered_rows == 0:
                raise SilverProcessingError(
                    f"Successful Bronze batch {item.batch_id} has no rows in "
                    f"{names['bronze']}"
                )

            bronze = select_bronze_batch(
                spark,
                names["bronze"],
                item.batch_id,
                ingest_from=ingest_from,
                ingest_to=ingest_to,
            )
            scoped_rows = (
                unfiltered_rows
                if not ingest_from and not ingest_to
                else int(bronze.count())
            )
            if scoped_rows == 0:
                # This batch exists and is healthy, but it is outside an
                # explicit date filter. Do not emit a terminal Silver log.
                suppress_audit_log(record)
                date_filtered_batches.append(
                    {"dataset": item.dataset, "batch_id": item.batch_id}
                )
                continue
            if scoped_rows != unfiltered_rows:
                raise SilverProcessingError(
                    f"Ingestion-date filters selected only {scoped_rows} of "
                    f"{unfiltered_rows} rows for Bronze batch {item.batch_id}. "
                    "A partial batch cannot receive a terminal Silver checkpoint."
                )

            # Preserve the materialized read count in a failure log even if a
            # later schema, secret, validation, or write operation fails.
            record["rows_read"] = scoped_rows

            transformed = transform_bronze_batch(
                bronze,
                item.dataset,
                address_secret_scope=secret_scope,
                address_secret_key=secret_key,
            )
            record["rows_read"] = transformed.rows_read
            record["rows_quarantined"] = transformed.rows_rejected

            quarantine_records_written = append_quarantine_idempotently(
                spark, transformed.quarantine, quarantine_table
            )
            metrics = merge_silver_batch(
                spark,
                transformed.accepted,
                names["silver"],
                SILVER_ENTITY_KEYS[item.dataset],
            )
            record["rows_inserted"] = metrics.rows_inserted
            record["rows_updated"] = metrics.rows_updated

            decision = evaluate_soft_delete(
                item,
                explicitly_enabled=allow_soft_deletes,
                subset_or_reprocess=subset_or_reprocess,
                rows_quarantined=transformed.rows_rejected,
                is_latest_validated_snapshot=is_latest_validated_snapshot(
                    spark, manifest_table, log_table, item
                ),
                snapshot_max_processing_date=(
                    transformed.max_source_processing_date
                ),
            )
            if decision.allowed:
                record["rows_deleted"] = soft_delete_missing_rows(
                    spark,
                    transformed.accepted,
                    names["silver"],
                    SILVER_ENTITY_KEYS[item.dataset],
                    transformed.max_source_processing_date,
                )

        results.append(
            {
                "dataset": item.dataset,
                "batch_id": item.batch_id,
                "load_type": item.load_type,
                "rows_read": transformed.rows_read,
                "rows_accepted": transformed.rows_accepted,
                "input_rows_rejected": transformed.rows_rejected,
                "invalid_input_rows": transformed.invalid_input_rows,
                "conflicting_input_rows": transformed.conflicting_input_rows,
                "unique_quarantine_records": (
                    transformed.unique_quarantine_records
                ),
                "quarantine_records_written": quarantine_records_written,
                "exact_valid_duplicates_collapsed": (
                    transformed.exact_valid_duplicates_collapsed
                ),
                "soft_delete_applied": decision.allowed,
                "soft_delete_reasons": list(decision.reasons),
            }
        )
    except Exception as exc:
        failures.append(
            {
                "dataset": item.dataset,
                "batch_id": item.batch_id,
                "error": f"{type(exc).__name__}: {exc}"[:2000],
            }
        )

# COMMAND ----------

if results:
    display(spark.createDataFrame(results).orderBy("dataset", "batch_id"))
if failures:
    display(spark.createDataFrame(failures).orderBy("dataset", "batch_id"))
if date_filtered_batches:
    display(spark.createDataFrame(date_filtered_batches).orderBy("dataset", "batch_id"))
    print(
        "Batches outside the requested Bronze ingestion-date range were not "
        f"processed or checkpointed: {len(date_filtered_batches)}"
    )

if failures:
    print(
        "Bronze-to-Silver completed with failures. Successful independent batches "
        "were committed; failed batches remain pending for an idempotent retry."
    )
    raise RuntimeError(
        f"Bronze-to-Silver run {run_id} failed for {len(failures)} batch(es); "
        "see the displayed failure summary and pipeline_execution_logs"
    )
elif not work_items:
    print("No successful Bronze batches require Silver processing.")
else:
    print("Bronze-to-Silver processing completed for all matching batches.")
