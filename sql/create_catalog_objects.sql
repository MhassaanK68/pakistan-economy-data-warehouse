-- Replace `workspace` below if the Free Edition workspace exposes a different writable catalog.
CREATE SCHEMA IF NOT EXISTS workspace.ops;
CREATE SCHEMA IF NOT EXISTS workspace.bronze;
CREATE SCHEMA IF NOT EXISTS workspace.silver;
CREATE SCHEMA IF NOT EXISTS workspace.gold;

CREATE VOLUME IF NOT EXISTS workspace.ops.economy_lake;

CREATE TABLE IF NOT EXISTS workspace.ops.pipeline_execution_logs (
  execution_log_id STRING NOT NULL,
  run_id STRING NOT NULL,
  parent_execution_log_id STRING,
  databricks_job_id STRING,
  databricks_run_id STRING,
  source_id STRING NOT NULL,
  batch_id STRING NOT NULL,
  load_type STRING NOT NULL,
  layer_from STRING NOT NULL,
  layer_to STRING NOT NULL,
  operation_name STRING NOT NULL,
  input_parameter_json STRING NOT NULL,
  input_file_or_table STRING NOT NULL,
  target_table STRING NOT NULL,
  execution_start_utc TIMESTAMP NOT NULL,
  execution_end_utc TIMESTAMP,
  status STRING NOT NULL,
  rows_read BIGINT NOT NULL,
  rows_inserted BIGINT NOT NULL,
  rows_updated BIGINT NOT NULL,
  rows_deleted BIGINT NOT NULL,
  rows_rejected BIGINT NOT NULL,
  rows_quarantined BIGINT NOT NULL,
  files_processed INT NOT NULL,
  error_class STRING,
  error_message STRING,
  notebook_or_module STRING NOT NULL,
  code_version STRING NOT NULL,
  load_timestamp TIMESTAMP NOT NULL
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.ops.quarantined_records (
  source_id STRING,
  dataset_code STRING,
  source_record_id STRING,
  batch_id STRING,
  manifest_id STRING,
  source_file_path STRING,
  source_file_hash STRING,
  source_snapshot_at TIMESTAMP,
  retrieved_at_utc TIMESTAMP,
  quarantine_reason STRING,
  failed_rule STRING,
  quarantined_at TIMESTAMP
) USING DELTA;
