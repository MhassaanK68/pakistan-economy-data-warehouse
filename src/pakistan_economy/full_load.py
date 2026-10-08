"""End-to-end full-load orchestration for structured SBP sources."""

from __future__ import annotations

from dataclasses import asdict

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window

from .bronze.sbp import read_sbp_bronze
from .config import FullLoadParameters, SBP_SOURCES, qualify
from .logging import ExecutionLogger, ExecutionResult
from .merge import merge_bronze_insert_only, merge_silver_current
from .quarantine import append_quarantine
from .silver.sbp import business_key, to_silver


def _ensure_delta_table(spark: SparkSession, table_name: str, template: DataFrame) -> None:
    if not spark.catalog.tableExists(table_name.replace("`", "")):
        template.limit(0).write.format("delta").mode("errorifexists").saveAsTable(table_name)


def run_sbp_full_load(spark: SparkSession, params: FullLoadParameters) -> dict[str, int | str]:
    """Load one staged SBP CSV snapshot into immutable Bronze and current Silver."""
    params.validate()
    config = SBP_SOURCES[params.source_id]
    bronze_table = qualify(params.catalog, config.bronze_table)
    silver_table = qualify(params.catalog, config.silver_table)
    quarantine_table = qualify(params.catalog, "ops.quarantined_records")
    public_params = {**asdict(params), "source_file_hash": params.source_file_hash[:12] + "…"}

    bronze_log = ExecutionLogger(
        spark, params.catalog, run_id=params.run_id, source_id=params.source_id,
        batch_id=params.batch_id, layer_from="STAGING", layer_to="BRONZE",
        operation_name="sbp_full_load_to_bronze", input_name=params.input_path,
        target_table=bronze_table, parameters=public_params, code_version=params.code_version,
    )
    bronze_result = ExecutionResult(files_processed=1)
    try:
        bronze = read_sbp_bronze(spark, params).cache()
        bronze_result.rows_read = bronze.count()
        corrupt = bronze.filter("_corrupt_record IS NOT NULL")
        parseable = bronze.filter("_corrupt_record IS NULL")
        bronze_result.rows_quarantined = append_quarantine(
            corrupt, table_name=quarantine_table, reason="CSV_PARSE_ERROR", rule="_corrupt_record IS NULL"
        )
        bronze_result.rows_rejected = bronze_result.rows_quarantined
        duplicate_window = Window.partitionBy("source_file_hash", "source_record_id").orderBy(
            F.col("source_record_id")
        )
        ranked = parseable.withColumn("_duplicate_rank", F.row_number().over(duplicate_window))
        duplicates = ranked.filter(F.col("_duplicate_rank") > 1).drop("_duplicate_rank")
        clean_bronze = ranked.filter(F.col("_duplicate_rank") == 1).drop("_duplicate_rank")
        duplicate_count = append_quarantine(
            duplicates,
            table_name=quarantine_table,
            reason="DUPLICATE_SOURCE_RECORD",
            rule="source_file_hash + source_record_id must be unique",
        )
        bronze_result.rows_quarantined += duplicate_count
        bronze_result.rows_rejected += duplicate_count
        _ensure_delta_table(spark, bronze_table, clean_bronze)
        metrics = merge_bronze_insert_only(spark, clean_bronze, bronze_table)
        bronze_result.rows_inserted = metrics.inserted
        bronze_log.finish("PARTIAL" if bronze_result.rows_rejected else "SUCCESS", bronze_result)
    except Exception as error:
        bronze_log.finish("FAILED", bronze_result, error)
        raise

    silver_log = ExecutionLogger(
        spark, params.catalog, run_id=params.run_id, source_id=params.source_id,
        batch_id=params.batch_id, layer_from="BRONZE", layer_to="SILVER",
        operation_name="sbp_full_load_to_silver", input_name=bronze_table,
        target_table=silver_table, parameters=public_params, code_version=params.code_version,
    )
    silver_result = ExecutionResult(files_processed=1)
    try:
        all_bronze = spark.table(bronze_table)
        batch_bronze = all_bronze.filter(
            (all_bronze.batch_id == params.batch_id)
            & (all_bronze.source_file_hash == params.source_file_hash)
        )
        silver_result.rows_read = batch_bronze.count()
        silver, invalid = to_silver(batch_bronze, config)
        silver_result.rows_quarantined = append_quarantine(
            invalid,
            table_name=quarantine_table,
            reason="SILVER_CAST_OR_KEY_FAILURE",
            rule="series_key/date/value must satisfy the explicit source contract",
        )
        silver_result.rows_rejected = silver_result.rows_quarantined
        _ensure_delta_table(spark, silver_table, silver)
        metrics = merge_silver_current(spark, silver, silver_table, business_key(config))
        silver_result.rows_inserted = metrics.inserted
        silver_result.rows_updated = metrics.updated
        silver_log.finish("PARTIAL" if silver_result.rows_rejected else "SUCCESS", silver_result)
    except Exception as error:
        silver_log.finish("FAILED", silver_result, error)
        raise

    return {
        "source_id": params.source_id,
        "rows_read": bronze_result.rows_read,
        "bronze_inserted": bronze_result.rows_inserted,
        "silver_inserted": silver_result.rows_inserted,
        "silver_updated": silver_result.rows_updated,
        "quarantined": bronze_result.rows_quarantined + silver_result.rows_quarantined,
    }
