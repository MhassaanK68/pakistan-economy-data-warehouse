"""Operational logging with one mutable row per task attempt."""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from .config import qualify
from .schemas import EXECUTION_LOG_SCHEMA


@dataclass
class ExecutionResult:
    rows_read: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_deleted: int = 0
    rows_rejected: int = 0
    rows_quarantined: int = 0
    files_processed: int = 0


class ExecutionLogger:
    def __init__(
        self,
        spark: SparkSession,
        catalog: str,
        *,
        run_id: str,
        source_id: str,
        batch_id: str,
        layer_from: str,
        layer_to: str,
        operation_name: str,
        input_name: str,
        target_table: str,
        parameters: dict[str, Any],
        code_version: str,
    ) -> None:
        self.spark = spark
        self.table = qualify(catalog, "ops.pipeline_execution_logs")
        self.id = str(uuid.uuid4())
        self.base = {
            "execution_log_id": self.id,
            "run_id": run_id,
            "parent_execution_log_id": None,
            "databricks_job_id": None,
            "databricks_run_id": None,
            "source_id": source_id,
            "batch_id": batch_id,
            "load_type": "full",
            "layer_from": layer_from,
            "layer_to": layer_to,
            "operation_name": operation_name,
            "input_parameter_json": json.dumps(parameters, default=str, sort_keys=True),
            "input_file_or_table": input_name,
            "target_table": target_table,
            "execution_start_utc": datetime.now(timezone.utc).replace(tzinfo=None),
            "execution_end_utc": None,
            "status": "RUNNING",
            **asdict(ExecutionResult()),
            "error_class": None,
            "error_message": None,
            "notebook_or_module": "pakistan_economy.full_load",
            "code_version": code_version,
            "load_timestamp": datetime.now(timezone.utc).replace(tzinfo=None),
        }
        spark.createDataFrame([self.base], EXECUTION_LOG_SCHEMA).write.mode("append").saveAsTable(self.table)

    def finish(self, status: str, result: ExecutionResult, error: Exception | None = None) -> None:
        completed = dict(self.base)
        completed.update(asdict(result))
        completed.update(
            execution_end_utc=datetime.now(timezone.utc).replace(tzinfo=None),
            status=status,
            error_class=type(error).__name__ if error else None,
            error_message=str(error)[:4000] if error else None,
            load_timestamp=datetime.now(timezone.utc).replace(tzinfo=None),
        )
        update = self.spark.createDataFrame([completed], EXECUTION_LOG_SCHEMA)
        (
            DeltaTable.forName(self.spark, self.table)
            .alias("t")
            .merge(update.alias("s"), "t.execution_log_id = s.execution_log_id")
            .whenMatchedUpdateAll()
            .execute()
        )
