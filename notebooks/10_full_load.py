# Databricks notebook source
# MAGIC %md
# MAGIC # Parameterized full load
# MAGIC Upload the partitioned `data/staging` folders to the configured Volume, run
# MAGIC `sql/create_catalog_objects.sql`, then invoke this notebook once per SBP source file.

# COMMAND ----------

from datetime import datetime
import json
import os
import sys
import uuid

sys.path.append(os.path.abspath("../src"))

from pakistan_economy.config import FullLoadParameters
from pakistan_economy.full_load import run_sbp_full_load


widgets = {
    "catalog": "workspace",
    "source_id": "SBP_REMITTANCES",
    "input_path": "/Volumes/workspace/ops/economy_lake/staging/source=SBP_REMITTANCES/ingest_date=2026-10-08/batch_id=full_20261008_001/*.csv",
    "batch_id": "full_20261008_001",
    "run_id": "",
    "manifest_id": "",
    "source_file_hash": "",
    "source_snapshot_at": "2026-10-08T00:00:00",
    "retrieved_at_utc": "2026-10-08T00:00:00",
    "code_version": "local",
}
for name, default in widgets.items():
    dbutils.widgets.text(name, default)

values = {name: dbutils.widgets.get(name).strip() for name in widgets}
values["run_id"] = values["run_id"] or str(uuid.uuid4())
required = ["catalog", "source_id", "input_path", "batch_id", "manifest_id", "source_file_hash"]
missing = [name for name in required if not values[name]]
if missing:
    raise ValueError(f"Required widget(s) missing: {missing}")

params = FullLoadParameters(
    catalog=values["catalog"],
    source_id=values["source_id"],
    input_path=values["input_path"],
    batch_id=values["batch_id"],
    run_id=values["run_id"],
    manifest_id=values["manifest_id"],
    source_file_hash=values["source_file_hash"],
    source_snapshot_at=datetime.fromisoformat(values["source_snapshot_at"].replace("Z", "+00:00")),
    retrieved_at_utc=datetime.fromisoformat(values["retrieved_at_utc"].replace("Z", "+00:00")),
    code_version=values["code_version"],
)

result = run_sbp_full_load(spark, params)
print(json.dumps(result, sort_keys=True))
