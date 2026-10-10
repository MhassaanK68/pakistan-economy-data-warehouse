# Databricks notebook source
"""Preview, prepare, verify, or activate the Silver quarantine migration.

The default ``preview`` mode is read-only.  ``prepare`` creates a separate
candidate table; ``activate`` only renames verified tables and retains the
legacy table as a backup.  This notebook is never called by pipeline setup or
Bronze-to-Silver processing.
"""

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

from carewatch.audit import (  # noqa: E402
    LEGACY_SILVER_QUARANTINE_SCHEMA,
    SILVER_QUARANTINE_SCHEMA,
    silver_quarantine_schema_state,
)
from carewatch.config import qualified_name  # noqa: E402
from carewatch.silver import require_exact_table_schema  # noqa: E402

# COMMAND ----------

dbutils.widgets.dropdown(
    "mode", "preview", ["preview", "prepare", "verify", "activate"]
)
dbutils.widgets.text("confirmation", "")
dbutils.widgets.text("catalog", "carewatch")
dbutils.widgets.text("schema", "pipeline")
dbutils.widgets.text("candidate_table", "silver_quarantine_v2_candidate")
dbutils.widgets.text("backup_table", "silver_quarantine_legacy_backup")

mode = dbutils.widgets.get("mode").strip().lower()
confirmation = dbutils.widgets.get("confirmation").strip()
catalog = dbutils.widgets.get("catalog").strip()
schema_name = dbutils.widgets.get("schema").strip()
candidate_object = dbutils.widgets.get("candidate_table").strip()
backup_object = dbutils.widgets.get("backup_table").strip()

legacy_table = qualified_name(catalog, schema_name, "silver_quarantine")
candidate_table = qualified_name(catalog, schema_name, candidate_object)
backup_table = qualified_name(catalog, schema_name, backup_object)
manifest_table = qualified_name(catalog, schema_name, "source_file_manifest")

WRITE_CONFIRMATION = "I_APPROVE_SILVER_QUARANTINE_MIGRATION"
ACTIVATE_CONFIRMATION = "I_APPROVE_SILVER_QUARANTINE_ACTIVATION"

spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

from pyspark.sql import Window, functions as F  # noqa: E402


def _ordered_signature(schema):
    return tuple(
        (field.name, field.dataType.simpleString()) for field in schema.fields
    )


def _require_valid_legacy_table(source_table=legacy_table) -> int:
    """Accept legacy metadata normalization only after structural/data checks."""

    if not spark.catalog.tableExists(source_table):
        raise RuntimeError(f"Required legacy table does not exist: {source_table}")
    source = spark.table(source_table)
    actual = _ordered_signature(source.schema)
    expected = _ordered_signature(LEGACY_SILVER_QUARANTINE_SCHEMA)
    if actual != expected:
        raise RuntimeError(
            f"Legacy table {source_table} does not match the exact ordered "
            f"five-column structure. Expected {expected}, got {actual}"
        )

    required_columns = tuple(
        field.name
        for field in LEGACY_SILVER_QUARANTINE_SCHEMA.fields
        if not field.nullable
    )
    invalid_condition = F.lit(False)
    for column in required_columns:
        invalid_condition = invalid_condition | F.col(column).isNull()
    # The legacy contract also forbids null elements inside failed_rules.
    invalid_condition = invalid_condition | F.exists(
        F.col("failed_rules"), lambda value: value.isNull()
    )
    invalid_rows = int(source.filter(invalid_condition).count())
    if invalid_rows:
        raise RuntimeError(
            f"Legacy table {source_table} contains {invalid_rows} row(s) with "
            "NULL required fields or NULL failed_rules elements; migration is blocked"
        )
    return int(source.count())


def _create_candidate_with_constraints() -> None:
    """Create the new empty Delta table with explicit column constraints."""

    columns = []
    for field in SILVER_QUARANTINE_SCHEMA.fields:
        nullability = "" if field.nullable else " NOT NULL"
        columns.append(
            f"`{field.name}` {field.dataType.simpleString()}{nullability}"
        )
    spark.sql(
        f"CREATE TABLE {candidate_table} ({', '.join(columns)}) USING DELTA"
    )
    # Fail before writing any migrated rows if the runtime did not preserve the
    # requested constraints in Unity Catalog metadata.
    require_exact_table_schema(spark, candidate_table, SILVER_QUARANTINE_SCHEMA)


def _legacy_with_lineage(source_table=legacy_table):
    """Build the candidate rows without writing them."""

    legacy_rows = _require_valid_legacy_table(source_table)
    source = spark.table(source_table)
    if legacy_rows == 0:
        return source.select(
            "dataset",
            F.col("batch_id").alias("source_batch_id"),
            F.lit(None).cast("string").alias("source_file"),
            F.lit(None).cast("string").alias("source_file_sha256"),
            F.lit(None).cast("string").alias("candidate_entity_key"),
            "raw_record",
            "failed_rules",
            "load_timestamp",
        )

    if not spark.catalog.tableExists(manifest_table):
        raise RuntimeError(f"Required manifest table does not exist: {manifest_table}")

    manifest = spark.table(manifest_table).filter(
        F.col("status").isin("SUCCESS", "SKIPPED_ALREADY_ACQUIRED")
    )
    ambiguous_lineage = (
        manifest.groupBy("dataset", "batch_id")
        .agg(
            F.countDistinct(
                F.struct(
                    F.col("landing_path"), F.lower("source_file_sha256")
                )
            ).alias("lineage_count")
        )
        .filter(F.col("lineage_count") > 1)
    )
    conflict_count = ambiguous_lineage.count()
    if conflict_count:
        raise RuntimeError(
            f"Migration cannot resolve {conflict_count} dataset/batch manifest "
            "identities with more than one source-file path/hash pair"
        )

    latest = Window.partitionBy("dataset", "batch_id").orderBy(
        F.col("load_timestamp").desc()
    )
    lineage = (
        manifest.withColumn("_rn", F.row_number().over(latest))
        .filter(F.col("_rn") == 1)
        .select(
            "dataset",
            "batch_id",
            F.col("landing_path").alias("source_file"),
            F.lower("source_file_sha256").alias("source_file_sha256"),
        )
    )
    joined = source.alias("legacy").join(
        lineage.alias("manifest"), ["dataset", "batch_id"], "left"
    )
    unresolved = joined.filter(
        F.col("source_file").isNull() | F.col("source_file_sha256").isNull()
    ).count()
    if unresolved:
        raise RuntimeError(
            f"Migration cannot safely reconstruct lineage for {unresolved} legacy "
            "rows; no placeholder values will be invented"
        )
    return joined.select(
        "dataset",
        F.col("batch_id").alias("source_batch_id"),
        "source_file",
        "source_file_sha256",
        F.lit(None).cast("string").alias("candidate_entity_key"),
        "raw_record",
        "failed_rules",
        "load_timestamp",
    )


def _verify_pair(
    legacy_source=legacy_table, current_target=candidate_table
) -> dict[str, object]:
    require_exact_table_schema(spark, current_target, SILVER_QUARANTINE_SCHEMA)
    _require_valid_legacy_table(legacy_source)
    expected = _legacy_with_lineage(legacy_source)
    candidate = spark.table(current_target)
    legacy_count = int(expected.count())
    candidate_count = int(candidate.count())
    missing = int(expected.exceptAll(candidate).count())
    unexpected = int(candidate.exceptAll(expected).count())
    verified = (
        legacy_count == candidate_count and missing == 0 and unexpected == 0
    )
    return {
        "legacy_table": legacy_source,
        "current_table": current_target,
        "legacy_rows": legacy_count,
        "candidate_rows": candidate_count,
        "missing_rows": missing,
        "unexpected_rows": unexpected,
        "verified": verified,
    }


def _recover_failed_activation() -> str:
    """Best-effort rename rollback that never drops or overwrites a table."""

    try:
        if (
            spark.catalog.tableExists(legacy_table)
            and not spark.catalog.tableExists(candidate_table)
        ):
            spark.sql(f"ALTER TABLE {legacy_table} RENAME TO {candidate_table}")
        if (
            spark.catalog.tableExists(backup_table)
            and not spark.catalog.tableExists(legacy_table)
        ):
            spark.sql(f"ALTER TABLE {backup_table} RENAME TO {legacy_table}")
    except Exception as recovery_error:
        return f"automatic rename recovery failed: {type(recovery_error).__name__}"
    if (
        spark.catalog.tableExists(legacy_table)
        and spark.catalog.tableExists(candidate_table)
        and not spark.catalog.tableExists(backup_table)
    ):
        return "legacy canonical name restored; candidate retained"
    return "manual recovery required; no table was dropped or overwritten"


state = silver_quarantine_schema_state(spark, legacy_table)
preview = {
    "mode": mode,
    "canonical_table": legacy_table,
    "canonical_schema_state": state,
    "canonical_rows": (
        int(spark.table(legacy_table).count())
        if spark.catalog.tableExists(legacy_table)
        else None
    ),
    "candidate_exists": bool(spark.catalog.tableExists(candidate_table)),
    "backup_exists": bool(spark.catalog.tableExists(backup_table)),
}
display(spark.createDataFrame([preview]))

# COMMAND ----------

if mode == "preview":
    if state == "LEGACY":
        # This builds and validates lineage in memory only; it performs no table write.
        preview_rows = int(_legacy_with_lineage().count())
        print(
            f"Read-only preview succeeded for {preview_rows} legacy rows. "
            "No table was created, changed, renamed, or dropped."
        )
    elif state == "CURRENT":
        print("The canonical table already has the current eight-column contract.")
    else:
        print(f"Migration preview stopped: canonical schema state is {state}.")

elif mode == "prepare":
    if confirmation != WRITE_CONFIRMATION:
        raise RuntimeError(
            "Prepare is blocked. Obtain approval, then supply the exact documented "
            "migration confirmation token."
        )
    if state != "LEGACY":
        raise RuntimeError(f"Prepare requires LEGACY state, got {state}")
    if spark.catalog.tableExists(candidate_table):
        raise RuntimeError(
            f"Candidate table already exists and will not be overwritten: {candidate_table}"
        )
    migrated = _legacy_with_lineage()
    _create_candidate_with_constraints()
    if migrated.limit(1).count() > 0:
        migrated.write.format("delta").mode("append").saveAsTable(candidate_table)
    verification = _verify_pair()
    display(spark.createDataFrame([verification]))
    if not verification["verified"]:
        raise RuntimeError(
            "Candidate verification failed. The legacy table is unchanged; do not activate."
        )
    print("Candidate prepared and verified. The canonical legacy table is unchanged.")

elif mode == "verify":
    verification = _verify_pair()
    display(spark.createDataFrame([verification]))
    if not verification["verified"]:
        raise RuntimeError("Candidate verification failed; activation is blocked")
    print("Candidate verification passed. No tables were changed.")

elif mode == "activate":
    if confirmation != ACTIVATE_CONFIRMATION:
        raise RuntimeError(
            "Activation is blocked. Stop pipeline writers, obtain separate approval, "
            "then supply the exact activation confirmation token."
        )
    if state != "LEGACY":
        raise RuntimeError(f"Activation requires LEGACY state, got {state}")
    if spark.catalog.tableExists(backup_table):
        raise RuntimeError(
            f"Backup target already exists and will not be overwritten: {backup_table}"
        )
    verification = _verify_pair()
    if not verification["verified"]:
        raise RuntimeError("Candidate verification failed; activation is blocked")

    spark.sql(f"ALTER TABLE {legacy_table} RENAME TO {backup_table}")
    try:
        spark.sql(f"ALTER TABLE {candidate_table} RENAME TO {legacy_table}")
        post_activation = _verify_pair(backup_table, legacy_table)
        if not post_activation["verified"]:
            raise RuntimeError("post-activation record verification failed")
    except Exception as activation_error:
        recovery = _recover_failed_activation()
        raise RuntimeError(
            f"Activation did not verify ({type(activation_error).__name__}); {recovery}"
        ) from activation_error

    display(spark.createDataFrame([post_activation]))
    print(
        f"Activation verified. Historical backup retained as {backup_table}. "
        "No table was dropped or overwritten."
    )
else:
    raise ValueError(f"Unknown migration mode: {mode!r}")
