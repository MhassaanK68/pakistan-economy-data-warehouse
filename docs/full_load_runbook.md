# Full-load runbook

## 1. Prepare the immutable local staging layout

From the repository root:

```powershell
python scripts/prepare_full_load.py `
  --input-root data/raw/full `
  --staging-root data/staging `
  --batch-id full_20261008_001 `
  --ingest-date 2026-10-08
```

The command copies the supplied files without changing their bytes, verifies
each copy with SHA-256, and writes
`data/staging/_manifests/full_20261008_001.jsonl`. Re-running it is safe: a
matching destination becomes `SKIPPED_IDENTICAL`; different bytes at the same
immutable path cause an error.

## 2. Upload to the Databricks Free Edition Volume

Run `sql/create_catalog_objects.sql` in Databricks SQL, then upload the contents
of `data/staging/` below:

```text
/Volumes/workspace/ops/economy_lake/staging/
```

Keep the generated `source=.../ingest_date=.../batch_id=...` folders intact.
The local staging folder is ignored by Git because it contains duplicate source
bytes; the scripts, configuration, tests, and documentation are versioned.

## 3. Run the five structured full loads

Open `notebooks/10_full_load.py` from the Databricks Git folder. For each SBP
manifest row, supply the catalog, source ID, staged CSV path, batch/run IDs,
manifest ID, complete SHA-256, source snapshot timestamp, retrieval timestamp,
and Git commit SHA. Run once for each of:

- `SBP_REMITTANCES`
- `SBP_FDI`
- `SBP_FX`
- `SBP_EXPORTS`
- `SBP_IMPORTS`

The notebook uses `StructType`/`StructField` rather than `inferSchema`, inserts
immutable Bronze rows with a deterministic record ID, casts dates and values in
Silver, quarantines bad rows, and uses Delta `MERGE` for repeat-safe loads.

## 4. Document-source boundary

The PBS SPI workbooks, PBS CPI PDF, and scanned OGRA PDF are staged and hashed by
the same command. They are not passed through the SBP CSV loader. Their bytes and
lineage are ready for the layout-specific Excel, PDF-table, and OCR adapters.
This prevents guessed or visually misread document values from entering Bronze.

## 5. Verify

Run these queries after each source:

```sql
SELECT source_id, batch_id, status, rows_read, rows_inserted, rows_updated,
       rows_quarantined, execution_start_utc, execution_end_utc
FROM workspace.ops.pipeline_execution_logs
WHERE batch_id = 'full_20261008_001'
ORDER BY execution_start_utc;

SELECT source_id, batch_id, quarantine_reason, count(*) AS rejected_rows
FROM workspace.ops.quarantined_records
WHERE batch_id = 'full_20261008_001'
GROUP BY source_id, batch_id, quarantine_reason;
```

Re-run the same notebook parameters. Expected result: Bronze and Silver insert
counts are zero and table row counts do not increase.
