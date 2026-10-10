# Phase 2 Guide: Build the Bronze and Silver Pipeline (PySpark + Delta)

This guide tells you exactly what to build, in what order, and what evidence to collect for the submission. It assumes the Phase 1 design (CareWatch: CMS nursing-home data) and that your Phase 1 repo and sample files exist.

**Estimated effort:** 4 to 6 working days for two people. The riskiest parts are Step 5 (Bronze with drift handling) and Step 7 (Silver MERGE). Start with **one dataset end to end** (Health Deficiencies), then repeat for the others.

---

## 0. What the instructor wants, mapped to the steps

| # | Instructor requirement | Where you satisfy it | Evidence to keep |
|---|---|---|---|
| 1 | Databricks (Community/Free) or Azure workspace | Step 1 | Screenshot of workspace + repo linked |
| 2 | All code committed to GitHub, continuously | Step 1, Step 12 | Commit history (both partners) |
| 3 | Data dictionary for Bronze and Silver (names, types, primary keys) | Step 6, Step 11 | `docs/data_dictionary.md`, README section |
| 4 | **No `inferSchema`**; explicit `StructType`/`StructField` before reading | Step 3, Step 5 | `schemas.py`; grep shows no `inferSchema` |
| 5 | Strict types and casting into Silver | Step 7 | Silver `DESCRIBE` output |
| 6 | `load_timestamp` on **every record of every table** | Step 3, 5, 7 | `SELECT load_timestamp` on each table |
| 7 | Idempotent: rerun on same data gives no duplicates; **MERGE INTO** | Step 7 | Before/after counts + log rows with 0 inserted/updated |
| 8 | Parameterized backfills (date / batch id / path), no hard-coded "today" | Step 2, Step 9 | Execution guide in README + a backfill demo run |
| 9 | Schema drift: evolve (`mergeSchema`) or quarantine, never crash the batch | Step 5, Step 7, Step 8 | Drift demo: new column + changed type |
| 10 | Logging tables with layer, parameter/file, start/end, status, rows inserted/updated | Step 4 | `pipeline_execution_logs` query output |
| 11 | README with Bronze + Silver models and execution guide | Step 11 | README |
| 12 | Databricks performs full and incremental source loading; no manual full-load upload | Step 1, Step 2, Step 10 | Empty-landing full run, source manifest, incremental watermark evidence |

Keep this table in your README as a "requirements traceability" section. Graders love it.

---

## 1. Locked architectural decisions

These decisions were reviewed and locked on **2026-10-10**. Implementations must follow them unless a later architecture decision record explicitly supersedes one. The exact column contracts are in [`docs/bronze_silver_schema_contract.md`](bronze_silver_schema_contract.md).

1. **Platform and namespace.** Use Databricks Free Edition with Unity Catalog, managed Delta tables, and a managed Volume. The default namespace is `carewatch.pipeline`; the default landing root is `/Volumes/carewatch/pipeline/landing`. Catalog, schema, and paths remain parameters so the same code can run against a different Unity Catalog namespace or a legacy `hive_metastore`/DBFS profile. Confirm with the instructor that Free Edition satisfies the stated “Community Edition or Azure” requirement.
2. **Datasets in scope.** Phase 2 covers Health Deficiencies, Provider Information, Penalties, and MDS Quality Measures. Build Health Deficiencies end to end first, then reuse one registry-driven pipeline for the other three.
3. **Snapshot-validated business keys.** Profiling of the September 2026 full extracts found no null key components and no duplicate keys:
   - Deficiencies: CCN + survey date + survey type + deficiency prefix + tag number + inspection cycle (`418,947` rows and distinct keys).
   - Penalties: CCN + penalty date + penalty type + fine ID + payment-denial start date (`15,419` rows and distinct keys). The two event types populate different subtype fields; blanks are retained in the hash with an explicit null sentinel.
   - Provider Information: CCN (`14,690` rows and distinct keys).
   - MDS Quality Measures: CCN + measure code + resident type + measure period (`249,730` rows and distinct keys).
   These are contracts for the profiled snapshot, not assumptions about all future releases. Re-run the uniqueness and null-key checks for every new full snapshot. Quarantine a release if the contract fails until the key is deliberately revised.
4. **Facility history.** Implement `silver_facility` as SCD Type 1 keyed by CCN for Phase 2. SCD Type 2 is explicitly deferred until the required Bronze/Silver pipeline and evidence suite are complete. Confirm with the instructor whether SCD2 is mandatory before final submission.
5. **Code structure.** Put reusable logic in `src/carewatch`; keep notebooks thin and limited to widgets, orchestration, and evidence display. Do not duplicate transformation logic between dataset notebooks.
6. **Storage and acquisition.** Use managed, initially unpartitioned Delta tables. At this data volume, partitioning would create avoidable small files. Source files live in the landing Volume and raw downloads remain outside Git. **Do not manually upload full-load files to Databricks.** A Databricks acquisition notebook must discover the current official CMS distribution URL and stream the source into the Volume itself.
7. **Bronze semantics.** Bronze uses canonical API names and stores every source field as nullable `STRING`, plus required non-null ingestion metadata. New batches append logically. A rerun of the same deterministic batch may replace only that batch, making ingestion idempotent while preserving all other source batches.
8. **Identity and time.** `run_id` is a UUID per execution. Default `batch_id` is a deterministic SHA-256-derived identifier from dataset, load type, stable source identity (resolved bulk URL or API URL/filter/page), and source-content SHA-256; it must not include the acquisition run directory. A user-supplied batch ID overrides it. `source_file_sha256` hashes the exact landed bytes, while `source_content_sha256` is the stable idempotency identity (for API pages it excludes volatile envelope fields such as retrieval time). All operational timestamps are UTC. Source dates remain `DATE` values.
9. **Drift policy.** Added columns evolve Bronze and are logged; Silver ignores them until its explicit contract is deliberately updated. Missing/renamed expected columns reject that file without stopping other files. Corrupt CSV rows go to Bronze quarantine. Values that fail Silver casting or business rules go to Silver quarantine.
10. **Silver and privacy.** Silver uses strict types, deterministic entity keys, business-only row hashes, and Delta `MERGE`. CCN, ZIP, deficiency tag number, measure code, fine ID, chain ID, and other identifiers stay strings even when CMS labels them numeric. Drop `telephone_number`. Replace `provider_address` with a salted SHA-256 `provider_address_hash`, and drop `location` because it repeats the street address. Keep city, state, and ZIP for geography.
11. **Full and incremental acquisition.** Initial and reconciliation full loads discover and download CMS bulk CSV snapshots inside Databricks. Health Deficiencies and Penalties use paginated, date-filtered CMS datastore queries for incrementals with a default 60-day safety overlap. Provider Information and MDS Quality Measures have no reliable event-date feed, so their refresh strategy is a newly published full CMS snapshot followed by hash-aware Silver `MERGE`; source extraction is full, but only inserted/changed target rows are written. No full-load file is manually uploaded.
12. **Incremental processing and backfills.** With no explicit filters, Silver processes successful Bronze batches that do not yet have a successful Silver log entry. API watermarks advance only after the corresponding Silver run succeeds. Backfills select an explicit source date window, file, or batch. Every acquisition run has an explicit `as_of_date`; dates, paths, and table names must not be hard-coded inside pipeline functions.
13. **Deletion policy.** Infer soft deletes only from a complete bulk snapshot that passed acquisition and row-count validation, and only within that snapshot’s explicitly supplied scope. Never infer deletions from API-window incrementals, partial files, or failed downloads.
14. **Audit model.** Write execution logs for `CMS-to-Landing`, `Raw-to-Bronze`, and `Bronze-to-Silver`. Use `SUCCESS`, `SKIPPED_ALREADY_ACQUIRED`, `QUARANTINED_PARTIAL`, and `FAILURE`; record parameters, source URL, UTC start/end times, row metrics, and truncated errors. Every data, manifest, watermark, quarantine, drift, and log table includes `load_timestamp`.
15. **Team ownership.** Hassan owns CMS acquisition, configuration, explicit schemas, Raw-to-Bronze, and drift handling. Hanan owns the audit framework, watermarks, Bronze-to-Silver, Delta merges, generated data dictionary, and evidence. Agree on and implement the logging and watermark interfaces before parallel pipeline work.

Two decisions require external confirmation but do not block initial development: instructor acceptance of Databricks Free Edition, and whether SCD Type 2 is mandatory.

---

## 2. Step 1: Workspace and repo setup (1 to 2 hours)

1. Open your Databricks workspace. Create a catalog/schema (or use `default`) such as `carewatch.pipeline`.
2. Create three storage areas (UC Volumes, or DBFS folders on Community Edition):
   ```
   /Volumes/<catalog>/<schema>/landing/full_load/
   /Volumes/<catalog>/<schema>/landing/incremental/
   /Volumes/<catalog>/<schema>/landing/archive/
   ```
3. Test outbound HTTPS from Databricks to both CMS endpoints used by the pipeline:
   - `https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/<dataset-id>`
   - `https://data.cms.gov/provider-data/api/1/datastore/query/<dataset-id>/0`
   A request for one row must succeed before pipeline development continues. Save the response/status as setup evidence.
4. **Do not upload the Phase 1 full-load CSV parts.** They are local profiling evidence only. The initial full load must be downloaded by Databricks from the bulk CSV URL returned by the CMS metastore. Incremental landing files must likewise be created by the acquisition notebook from the CMS datastore API or a newly published bulk snapshot.
5. Link GitHub: Workspace > Create > Git folder, paste your repo URL (needs a GitHub personal access token). If Git folders are not available on your plan, develop in notebooks and export/commit them manually after every session; the instructor wants a visible history either way.
6. Repo layout to create now:

```
carewatch-medallion/
├── notebooks/
│   ├── 00_setup_tables.py          # creates schemas + log tables (run once)
│   ├── 01_acquire_cms.py            # CMS-to-Landing: full + incremental
│   ├── 02_raw_to_bronze.py          # parameterized
│   ├── 03_bronze_to_silver.py       # parameterized
│   └── 99_demo_idempotency_drift_backfill.py
├── src/carewatch/
│   ├── config.py                   # dataset registry + table names
│   ├── schemas.py                  # explicit StructTypes (Bronze) + Silver typing rules
│   ├── audit.py                    # logging helper
│   ├── acquire.py                  # metastore discovery, downloads, API pagination
│   ├── watermarks.py               # successful incremental checkpoints
│   ├── drift.py                    # header/schema drift detection
│   ├── bronze.py
│   └── silver.py
├── docs/ (data_dictionary.md, design_decisions.md, evidence/)
├── tests/
└── README.md
```

Notebooks can import `src/carewatch` after `sys.path.append("/Workspace/.../carewatch-medallion/src")`. If imports are awkward on your plan, use `%run ./helpers` notebooks instead; the requirement is explicit schemas and parameters, not a package.

---

## 3. Step 2: Automated CMS acquisition, parameters, and config (half a day)

**Rule: no full-load file is uploaded manually.** `01_acquire_cms.py` must create the landing files from official CMS endpoints. No path, date, dataset ID, table name, or watermark is hard-coded inside an acquisition or transformation function.

### Acquisition widgets

```python
# notebooks/01_acquire_cms.py
dbutils.widgets.text("dataset", "health_deficiencies")
dbutils.widgets.dropdown("load_type", "full", ["full", "incremental"])
dbutils.widgets.text("as_of_date", "")          # required YYYY-MM-DD; never derive silently from today
dbutils.widgets.text("start_date", "")          # optional explicit incremental/backfill start
dbutils.widgets.text("overlap_days", "60")      # used when start_date is empty
dbutils.widgets.dropdown("force_refresh", "false", ["false", "true"])
dbutils.widgets.text("catalog", "carewatch")
dbutils.widgets.text("schema", "pipeline")
dbutils.widgets.text("landing_root", "/Volumes/carewatch/pipeline/landing")
```

`as_of_date` is mandatory so the same run can be reproduced later. For a normal incremental, an empty `start_date` means “read the last successful Silver watermark and subtract `overlap_days`.” `force_refresh=false` skips content already registered successfully by SHA-256.

For a bulk snapshot, `as_of_date` labels the acquisition run; it does not make the mutable CMS “current download” URL historical. Reproducibility comes from retaining the downloaded bytes, resolved URL, CMS catalog-modified date, and SHA-256 in the manifest.

Raw-to-Bronze and Bronze-to-Silver keep their own widgets. `source_path` is an optional backfill/test override in `02_raw_to_bronze.py`; an empty value consumes only the landing files registered by a successful acquisition run.

### Dataset registry

`config.py` holds one registry used by acquisition, Bronze, and Silver:

```python
# src/carewatch/config.py
DATASETS = {
    "health_deficiencies": {
        "dataset_id": "r5ix-sfxw",
        "incremental_strategy": "api_date_window",
        "watermark_col": "survey_date",
        "bronze_table": "bronze_nh_health_deficiencies",
        "silver_table": "silver_deficiency",
        "key_cols": ["cms_certification_number_ccn", "survey_date", "survey_type",
                     "deficiency_prefix", "deficiency_tag_number", "inspection_cycle"],
    },
    "penalties": {
        "dataset_id": "g6vv-u9sr",
        "incremental_strategy": "api_date_window",
        "watermark_col": "penalty_date",
        "bronze_table": "bronze_nh_penalties",
        "silver_table": "silver_penalty",
        "key_cols": ["cms_certification_number_ccn", "penalty_date", "penalty_type",
                     "fine_id", "payment_denial_start_date"],
    },
    "provider_info": {
        "dataset_id": "4pq5-n9py",
        "incremental_strategy": "snapshot_diff",
        "watermark_col": None,
        "bronze_table": "bronze_nh_provider_info",
        "silver_table": "silver_facility",
        "key_cols": ["cms_certification_number_ccn"],
    },
    "mds_quality": {
        "dataset_id": "djen-97ju",
        "incremental_strategy": "snapshot_diff",
        "watermark_col": None,
        "bronze_table": "bronze_nh_mds_quality",
        "silver_table": "silver_mds_quality",
        "key_cols": ["cms_certification_number_ccn", "measure_code", "resident_type",
                     "measure_period"],
    },
}

def fq(p, name):
    return f"{p['catalog']}.{p['schema']}.{name}"
```

### Full-load acquisition

For `load_type=full`, Databricks must:

1. Request the dataset’s metastore item using its configured `dataset_id`.
2. Select the official CSV distribution; do not hard-code the release-specific `downloadURL`.
3. Record the catalog modified date, resolved URL, and retrieval time.
4. Stream the response in chunks to `<landing_root>/full_load/<dataset>/<as_of_date>/...partial`; do not call `response.content` for a 100+ MB file.
5. Compute SHA-256 and byte count while streaming.
6. Atomically rename the completed `.partial` file to `.csv` only after HTTP, byte-count, and hash checks pass.
7. Register it in `source_file_manifest` with the CMS datastore count as `expected_run_rows`. Raw-to-Bronze later fills `source_rows` and sets `row_count_validated=true` only when the acquisition-run sum reconciles.
8. If the same dataset/hash already succeeded and `force_refresh=false`, log `SKIPPED_ALREADY_ACQUIRED` and do not download/process it again.
9. Pass the registered landing file to Raw-to-Bronze. No local sample or human upload participates in the run.

The download helper should follow this shape:

```python
import hashlib, os, requests

def stream_download(url, final_path):
    partial = final_path + ".partial"
    digest = hashlib.sha256()
    size = 0
    with requests.get(url, stream=True, timeout=(30, 300)) as response:
        response.raise_for_status()
        with open(partial, "wb") as out:
            for chunk in response.iter_content(1024 * 1024):
                if chunk:
                    out.write(chunk)
                    digest.update(chunk)
                    size += len(chunk)
    os.replace(partial, final_path)
    return digest.hexdigest(), size
```

On failure, remove only that run’s `.partial` file, write a `CMS-to-Landing` failure log, and leave any previously completed landing file untouched.

### Incremental acquisition

**Health Deficiencies and Penalties — API date window**

1. Determine `window_start` from explicit `start_date`, otherwise from the last successful Silver watermark minus 60 days. If neither exists, fail with an instruction to run the initial full load; never guess an unbounded incremental start.
2. Use `as_of_date` as the inclusive upper bound.
3. Query the datastore API with conditions on the configured watermark column, `limit=500`, and increasing `offset`.
4. Write each returned page immediately as a separate JSON landing part; do not accumulate all pages in driver memory.
5. Record the exact filter, page offset, result count, URL parameters, and content SHA-256 in `source_file_manifest`.
6. Stop when a page contains fewer than 500 results. Treat a repeated page, non-advancing offset, malformed response, or reported-count mismatch as failure.
7. Mark the overall `CMS-to-Landing` acquisition run `SUCCESS` only after every expected page is present and reconciled. Raw-to-Bronze may consume page manifests only when their parent acquisition run succeeded; successful early pages from a later failed pagination run must not leak downstream.
8. Do not advance the watermark here. Advance it only after Bronze-to-Silver succeeds for every page in the acquisition run.

The 60-day overlap deliberately re-reads recent keys so CMS corrections become Silver updates rather than duplicate rows.

The overlap cannot detect a correction older than the window or a source-side deletion. Run a periodic bulk reconciliation for Deficiencies and Penalties as well; only a validated complete snapshot may drive scoped soft deletes.

**Provider Information and MDS Quality Measures — snapshot diff**

These datasets do not provide a trustworthy event-date column for row-level incremental extraction. For `load_type=incremental`, check the metastore for a newly published bulk snapshot:

1. If its downloaded SHA-256 already exists as a successful manifest entry, log `SKIPPED_ALREADY_ACQUIRED`.
2. Otherwise stream the new complete snapshot into `landing/incremental/<dataset>/<as_of_date>/`.
3. Process it through Bronze and compare deterministic Silver `row_hash` values.
4. Insert new keys, update changed keys, and leave unchanged keys untouched.

This is a full source snapshot with incremental target processing. It is intentionally preferred over pretending `processing_date` is a row-change timestamp.

---

## 4. Step 3: Explicit schemas for Bronze (2 to 3 hours)

**Bronze stores everything as strings** (raw, unmodified), plus metadata. Typing happens in Silver.

1. Get the exact column list for each dataset from the API (one row is enough):
   ```
   https://data.cms.gov/provider-data/api/1/datastore/query/r5ix-sfxw/0?limit=1&schema=false
   ```
   The JSON keys are your canonical (snake_case) column names. Do the same for `4pq5-n9py` (about 100 columns), `g6vv-u9sr`, `djen-97ju`.
2. Put them in `schemas.py` and build `StructType` objects. **Never call `inferSchema`.**

```python
# src/carewatch/schemas.py
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DateType

DEFICIENCY_COLS = [
    "cms_certification_number_ccn", "provider_name", "provider_address", "citytown", "state",
    "zip_code", "survey_date", "survey_footnote", "survey_type", "deficiency_prefix", "deficiency_category",
    "deficiency_tag_number", "deficiency_description", "scope_severity_code",
    "deficiency_corrected", "correction_date", "inspection_cycle", "standard_deficiency",
    "complaint_deficiency", "infection_control_inspection_deficiency", "citation_under_idr",
    "citation_under_iidr", "location", "processing_date",
]

def string_struct(cols):
    return StructType([StructField(c, StringType(), True) for c in cols])

# Metadata columns added by YOU in Bronze (every Bronze table has all of them)
BRONZE_META = StructType([
    StructField("_dataset_id",   StringType(),    False),
    StructField("_source_file",  StringType(),    False),
    StructField("_source_file_sha256", StringType(), False),
    StructField("_batch_id",     StringType(),    False),
    StructField("_load_type",    StringType(),    False),
    StructField("_ingest_date",  DateType(),      False),   # date part, handy for backfill filters
    StructField("load_timestamp", TimestampType(), False),  # REQUIRED by the instructor
])
```

3. **Important: the bulk CSV headers are human-readable** ("Survey Date", "City/Town"), while the API JSON uses snake_case (`survey_date`, `citytown`). The easy way to match both to your canonical names is a normalising key that strips everything except letters and digits:

```python
import re
def key(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())
# key("City/Town") == key("citytown") ; key("CMS Certification Number (CCN)") == key("cms_certification_number_ccn")
```
   A few Provider Information headers will not match this way. The verified September 2026 API name for the bulk header `Total number of nurse staff hours per resident per day on the weekend` is the CMS-truncated `total_number_of_nurse_staff_hours_per_resident_per_day_on_t_4a14`. Keep that exact API name as the canonical Bronze field and declare the mapping in an explicit override dictionary. Fail the contract check for any other unresolved mismatch.

The live CMS datastore exposes every field as text, which supports the all-string Bronze decision but is not a Silver typing contract. Use the consolidated CMS Nursing Home Data Dictionary plus the profiled local values to define Silver types. The locked contract is [`docs/bronze_silver_schema_contract.md`](bronze_silver_schema_contract.md).

---

## 5. Step 4: Logging framework (2 hours) **build this before the pipelines**

Create these tables in `00_setup_tables.py` (Delta):

**`pipeline_execution_logs`** (one row per file/table processed, per layer, per attempt)

| Column | Type | Notes |
|---|---|---|
| run_id | string | uuid per notebook run |
| batch_id | string | ties Bronze batch to Silver processing |
| pipeline_layer | string | `CMS-to-Landing`, `Raw-to-Bronze`, `Bronze-to-Silver` |
| dataset | string | e.g. `health_deficiencies` |
| load_type | string | `full` / `incremental` |
| parameter_processed | string | source URL/filter, file path, batch id, or date range |
| start_time, end_time | timestamp | UTC |
| status | string | `SUCCESS` / `SKIPPED_ALREADY_ACQUIRED` / `FAILURE` / `QUARANTINED_PARTIAL` |
| rows_read | long | |
| rows_inserted | long | |
| rows_updated | long | 0 for Bronze appends |
| rows_deleted | long | soft-deleted count |
| rows_quarantined | long | |
| error_message | string | first 2000 chars |
| load_timestamp | timestamp | when the log row was written (keeps the "every table" rule true) |

Also create:

**`source_file_manifest`** — one row per acquired bulk file or API page:

| Column | Type | Notes |
|---|---|---|
| `acquisition_run_id` | string | UUID for one acquisition invocation |
| `dataset`, `dataset_id` | string | registry key and CMS identifier |
| `load_type` | string | `full` / `incremental` |
| `acquisition_strategy` | string | `bulk_snapshot` / `api_date_window` / `snapshot_diff` |
| `source_url` | string | resolved official CMS URL without secrets |
| `source_catalog_modified` | date | metastore modified date when available |
| `window_start`, `window_end` | date | API bounds; null for bulk snapshot |
| `page_offset` | long | API offset; null for bulk snapshot |
| `landing_path` | string | completed Volume path |
| `batch_id` | string | deterministic batch passed into Bronze |
| `source_content_sha256` | string | stable idempotency identity; excludes volatile API envelope fields |
| `source_file_sha256` | string | digest of the exact landed file bytes |
| `source_bytes` | long | completed file size |
| `expected_run_rows` | long | CMS count for the whole snapshot/filter; repeated per page when applicable |
| `source_rows` | long | rows parsed from this file/page; nullable until Bronze validation |
| `row_count_validated` | boolean | true only after count reconciliation |
| `status` | string | `SUCCESS`, `SKIPPED_ALREADY_ACQUIRED`, or `FAILURE` |
| `error_message` | string | first 2000 characters |
| `load_timestamp` | timestamp | UTC manifest-write time |

**`ingestion_watermarks`** — one current row per API-incremental dataset:

| Column | Type | Notes |
|---|---|---|
| `dataset` | string | primary key |
| `watermark_column` | string | `survey_date` or `penalty_date` |
| `watermark_value` | date | maximum successfully merged source date |
| `last_successful_acquisition_run_id` | string | run whose every page reached Silver |
| `last_successful_silver_run_id` | string | corresponding merge run |
| `load_timestamp` | timestamp | UTC update time |

Update this table with `MERGE` only after all acquired pages for the run have successful Bronze-to-Silver log rows. A failed or partially quarantined run does not advance it.

Also create **`schema_drift_log`** (run_id, dataset, source_file, drift_type, column_name, detail, load_timestamp) and **`silver_quarantine`** (dataset, batch_id, raw_record string, failed_rules array<string>, load_timestamp).

The helper everything calls:

```python
# src/carewatch/audit.py
from contextlib import contextmanager
from datetime import datetime, timezone
from pyspark.sql.types import (StructType, StructField, StringType, LongType, TimestampType)

LOG_SCHEMA = StructType([
    StructField("run_id", StringType()), StructField("batch_id", StringType()),
    StructField("pipeline_layer", StringType()), StructField("dataset", StringType()),
    StructField("load_type", StringType()), StructField("parameter_processed", StringType()),
    StructField("start_time", TimestampType()), StructField("end_time", TimestampType()),
    StructField("status", StringType()), StructField("rows_read", LongType()),
    StructField("rows_inserted", LongType()), StructField("rows_updated", LongType()),
    StructField("rows_deleted", LongType()), StructField("rows_quarantined", LongType()),
    StructField("error_message", StringType()), StructField("load_timestamp", TimestampType()),
])

@contextmanager
def audited(spark, log_table, run_id, layer, dataset, load_type, parameter, batch_id=""):
    rec = {"run_id": run_id, "batch_id": batch_id, "pipeline_layer": layer, "dataset": dataset,
           "load_type": load_type, "parameter_processed": parameter,
           "start_time": datetime.now(timezone.utc), "status": "SUCCESS",
           "rows_read": 0, "rows_inserted": 0, "rows_updated": 0, "rows_deleted": 0,
           "rows_quarantined": 0, "error_message": None}
    try:
        yield rec                      # caller fills in the row counts
        if rec["rows_quarantined"] > 0 and rec["status"] == "SUCCESS":
            rec["status"] = "QUARANTINED_PARTIAL"
    except Exception as exc:           # log the failure, then let the caller decide
        rec["status"] = "FAILURE"
        rec["error_message"] = str(exc)[:2000]
        raise
    finally:                           # runs on success AND failure
        rec["end_time"] = datetime.now(timezone.utc)
        rec["load_timestamp"] = rec["end_time"]
        spark.createDataFrame([rec], schema=LOG_SCHEMA).write.mode("append").saveAsTable(log_table)
```

Usage pattern (one log row per file):

```python
for f in files:
    try:
        with audited(spark, fq(P, "pipeline_execution_logs"), run_id,
                     "Raw-to-Bronze", P["dataset"], P["load_type"], f) as rec:
            n = load_one_file(f)            # returns rows written
            rec["rows_read"] = rec["rows_inserted"] = n
    except Exception:
        continue                            # one bad file must not kill the batch
```

---

## 6. Step 5: Raw to Bronze (1 to 1.5 days)

Select files from successful `source_file_manifest` rows for the requested acquisition run. Do not scan an upload folder and assume every file is trusted. For **each registered file or API page** (loop per file so every file gets its own log row):

1. **Read the header** and compare with the expected columns (drift check):

```python
import csv

def read_header(spark, path):
    first = spark.read.text(path).limit(1).collect()[0][0]
    return next(csv.reader([first.lstrip("﻿")]))

def check_drift(actual_header, expected_cols):
    exp = {key(c): c for c in expected_cols}
    act = {key(h): h for h in actual_header}
    missing = [exp[k] for k in exp if k not in act]
    extra   = [act[k] for k in act if k not in exp]
    return missing, extra
```
2. **Decide what to do**:
   - `missing` columns: the file is non-conforming. Log a `schema_drift_log` row, mark the file `FAILURE` (or `QUARANTINED_PARTIAL`), **move on to the next file**. Do not crash the run.
   - `extra` columns (source added a column): build the read schema with the extra columns appended as `StringType` in header order, and write with `mergeSchema=true` so the Bronze table evolves. Log the drift.
3. **Read with the explicit schema** (header row skipped, columns matched by position, so build the schema in *header order*):

```python
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

def read_csv_file(spark, path, ordered_cols):
    schema = StructType([StructField(c, StringType(), True) for c in ordered_cols] +
                        [StructField("_corrupt_record", StringType(), True)])
    return (spark.read.format("csv")
            .option("header", "true")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .option("multiLine", "true")
            .option("quote", '"').option("escape", '"')
            .schema(schema)                      # explicit, NOT inferred
            .load(path))
```
   For incremental **JSON page files**, the acquisition notebook wraps each CMS page as `{"_meta": {...}, "results": [ {...}, ... ]}`. Read it with the explicit envelope in the schema contract using `StructType([StructField("_meta", ...), StructField("results", ArrayType(string_struct(cols)))])` and `multiLine=true`, validate the metadata/page count, then `F.explode("results")`.
4. **Add the metadata columns**, including `load_timestamp`:

```python
def add_bronze_meta(df, dataset_id, path, source_file_sha256, batch_id, load_type):
    return (df.withColumn("_source_file", F.lit(path))
              .withColumn("_dataset_id", F.lit(dataset_id))
              .withColumn("_source_file_sha256", F.lit(source_file_sha256))
              .withColumn("_batch_id", F.lit(batch_id))
              .withColumn("_load_type", F.lit(load_type))
              .withColumn("_ingest_date", F.current_date())
              .withColumn("load_timestamp", F.current_timestamp()))
```
   (Use `F.lit(path)` from the path you looped over; do not use `input_file_name()` under Unity Catalog.)
5. **Quarantine bad CSV rows**: rows where `_corrupt_record IS NOT NULL` go to a Bronze quarantine table (`bronze_quarantine`), the rest continue.
6. **Write idempotently**. Compute the source-file SHA-256 while registering the file. Make `batch_id` **deterministic** from dataset + load type + stable source identity + source-content SHA-256 unless the user passes one. The stable source identity is the resolved bulk URL, or the API URL/filter/page; never use the run-specific landing directory. Including the content hash prevents changed content at the same source from being mistaken for the original batch, while an identical rerun resolves to the same batch:

```python
import hashlib
def derive_batch_id(dataset, load_type, source_identity, source_content_sha256):
    identity = f"{dataset}|{load_type}|{source_identity}|{source_content_sha256}"
    return hashlib.sha256(identity.encode()).hexdigest()[:24]

(df.write.format("delta")
   .mode("overwrite")
   .option("replaceWhere", f"_batch_id = '{batch_id}'")
   .option("mergeSchema", "true")
   .saveAsTable(bronze_table))
```
   If `replaceWhere` misbehaves on your runtime, use `DELETE FROM <bronze_table> WHERE _batch_id = '<id>'` followed by an append.
7. Count rows once (`n = df.count()`) and put it in the log record. Avoid `collect()`/`toPandas()` on full data (free-tier compute).
8. After success, update the manifest/Bronze log relationship. Archiving is optional because the content-addressed manifest and dated landing path already make completed acquisitions immutable; never archive or delete a file before Silver succeeds.

---

## 7. Step 6: Silver design and data dictionary (half a day)

**Profile first.** Before you trust a primary key, prove it:

```sql
SELECT COUNT(*) AS total_rows,
       COUNT(DISTINCT concat_ws('|', cms_certification_number_ccn, survey_date, survey_type,
                                 deficiency_prefix, deficiency_tag_number, inspection_cycle)) AS distinct_keys
FROM bronze_nh_health_deficiencies;
```
If `total_rows > distinct_keys`, inspect the duplicates. Either add a column to the key, or the source has true duplicate rows (then de-duplicate on the full row hash). **Write down the result and the final key in `design_decisions.md`.** Never describe a key as unique without having tested it.

**Silver rules** (apply to every dataset):

| Rule | Detail |
|---|---|
| Names | snake_case, same as Bronze canonical names |
| Dates | `survey_date`, `correction_date`, `penalty_date`, etc. become `DATE` |
| Integers | `inspection_cycle`, bed counts, star ratings (1 to 5), `payment_denial_length_in_days` become `INT` |
| Decimals | `fine_amount` and facility fine totals become `DECIMAL(14,2)`; staffing hours, turnover, and quality scores become `DOUBLE` |
| Flags | `Y`/`N` columns become `BOOLEAN` |
| **Keep as STRING** | `cms_certification_number_ccn`, `zip_code`, `provider_ssa_county_code`, `deficiency_tag_number`, `measure_code`, `fine_id`, and `chain_id`: leading zeros or identifier formatting matter, so casting these to numbers is a bug |
| Empty strings | become `NULL` |
| PII (from Phase 1) | drop `telephone_number`; replace `provider_address` with `provider_address_hash` (salted SHA-256 with the salt kept in a Databricks secret, not in code); drop `location` because it repeats the street address |
| Added columns | `severity_group` (A to C, D to F, G to I, J to L as in Phase 1), `row_hash`, `is_deleted` (BOOLEAN), `source_processing_date`, `load_timestamp` |
| Primary key | a `<entity>_key` column = SHA-256 of the key columns (this is what you MERGE on) |

Silver tables:

| Table | Primary key | Notes |
|---|---|---|
| `silver_deficiency` | `deficiency_key` | one row per citation |
| `silver_penalty` | `penalty_key` | fines and payment denials |
| `silver_facility` | `facility_key` derived from `cms_certification_number_ccn` (SCD1) | 102 source columns are projected into the locked typed/privacy-safe contract |
| `silver_mds_quality` | `mds_key` | scores `q1` to `q4` and four-quarter average as `DOUBLE` |
| `silver_quarantine` | none (append-only) | rejected records with reason |

The exact source-to-Silver types, nullability, derived fields, and dropped fields are locked in [`docs/bronze_silver_schema_contract.md`](bronze_silver_schema_contract.md). Generate the final deployed data dictionary from code (Step 11) and compare it to that contract; do not independently hand-type a second definition of the 102-column facility source.

---

## 8. Step 7: Bronze to Silver with MERGE (1.5 to 2 days)

Pipeline inside `03_bronze_to_silver.py` for one dataset and one set of batches:

**a. Select the Bronze slice from parameters** (this enables backfills, Step 9):

```python
from pyspark.sql import functions as F

def select_bronze(spark, bronze_table, batch_ids=None, ingest_from=None, ingest_to=None, load_type=None):
    df = spark.table(bronze_table)
    if batch_ids:
        df = df.filter(F.col("_batch_id").isin(batch_ids))
    if ingest_from:
        df = df.filter(F.col("_ingest_date") >= F.lit(ingest_from).cast("date"))
    if ingest_to:
        df = df.filter(F.col("_ingest_date") <= F.lit(ingest_to).cast("date"))
    if load_type:
        df = df.filter(F.col("_load_type") == load_type)
    return df
```
If the user passes none of these, process **batches that reached Bronze successfully but have no successful Bronze-to-Silver log row**. That is your "standard incremental" run, driven by `pipeline_execution_logs`.

**b. Clean and cast** with `try_cast` so a bad value becomes `NULL` instead of throwing (works whether or not ANSI mode is on):

```python
def clean_empty(df):
    return df.select([F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c))).alias(c)
                      if t == "string" else F.col(c)
                      for c, t in df.dtypes])

def tcast(col, to):          # safe cast
    return F.expr(f"try_cast({col} as {to})")
```

**c. Quarantine non-conforming rows** instead of failing: for each required typed column, if the raw value is not null but `try_cast` is null, the row is bad. Collect the failing column names into an array, split the DataFrame into `good` and `bad`, write `bad` (with the raw record as JSON via `F.to_json(F.struct(*raw_cols))`) to `silver_quarantine`. Also reject rows that break the locked rules (CCN not six alphanumeric characters, rating outside 1 to 5, negative fine, or an invalid penalty subtype combination). **Do not reject a deficiency merely because `correction_date < survey_date`.** The September 2026 extract contains 5,339 such rows, including 5,167 valid `Past Non-Compliance` records; CMS can document a condition that was corrected before the survey. Validate correction-date presence against correction status instead.

**d. De-duplicate inside the batch.** MERGE fails ("multiple source rows matched") if the source has two rows with the same key:

```python
from pyspark.sql import Window
w = Window.partitionBy("deficiency_key").orderBy(F.col("load_timestamp").desc())
good = good.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
```

**e. Add keys and the change-detection hash:**

```python
def sha_cols(cols):
    # Ordered JSON with explicit null fields avoids delimiter and null/empty collisions.
    payload = F.to_json(F.struct(*[F.col(c).alias(c) for c in cols]),
                        options={"ignoreNullFields": "false"})
    return F.sha2(payload, 256)

good = (good.withColumn("deficiency_key", sha_cols(KEY_COLS))
            .withColumn("row_hash", sha_cols(BUSINESS_COLS))   # business columns only, NOT load_timestamp
            .withColumn("is_deleted", F.lit(False))
            .withColumn("load_timestamp", F.current_timestamp()))
```

**f. The MERGE (this is your idempotency proof):**

```python
good.createOrReplaceTempView("stage_deficiency")
spark.sql(f"""
MERGE INTO {silver_table} AS t
USING stage_deficiency AS s
ON t.deficiency_key = s.deficiency_key
WHEN MATCHED AND (t.row_hash <> s.row_hash OR t.is_deleted = true) THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
```
Because unchanged rows have the same `row_hash`, running this twice on the same data inserts **0** and updates **0** rows, and `load_timestamp` is untouched on unchanged rows.

**g. Soft deletes (validated complete snapshots only).** Add this clause only when the source manifest proves that the input is a complete CMS bulk snapshot, every expected file/page succeeded, and the row-count reconciliation passed. Scope it to the exact snapshot coverage. Never run it for an API date window, an incomplete download, a test subset, or a manually supplied file:

```sql
WHEN NOT MATCHED BY SOURCE AND t.is_deleted = false
  AND t.survey_date <= CAST(:snapshot_max_date AS DATE)
THEN UPDATE SET t.is_deleted = true, t.load_timestamp = current_timestamp()
```
The bound comes from validated snapshot metadata, not a hard-coded date. For Provider Information and MDS snapshot refreshes, soft deletion is allowed only if that refresh is explicitly marked `complete_snapshot=true`. Skip deletion for every `api_date_window` run.

**h. Get row counts for the log** from the Delta history (do not recount the table):

```python
from delta.tables import DeltaTable
m = DeltaTable.forName(spark, silver_table).history(1).select("operationMetrics").collect()[0][0]
rec["rows_inserted"] = int(m.get("numTargetRowsInserted", 0))
rec["rows_updated"]  = int(m.get("numTargetRowsUpdated", 0))
rec["rows_deleted"]  = int(m.get("numTargetRowsDeleted", 0))
```
(Soft deletes show up under updated; count `is_deleted` changes separately if you want them in `rows_deleted`.)

**i. Facility table.** Use the same MERGE pattern with `facility_key = SHA-256(CCN)` and SCD Type 1 semantics. SCD Type 2 is outside the locked Phase 2 implementation; do not add `valid_from`, `valid_to`, or `is_current` unless the instructor makes SCD2 mandatory and the architecture record is updated first.

---

## 9. Step 8: Schema drift: what you must be able to demonstrate

| Drift event | Where it is caught | What happens |
|---|---|---|
| Source **adds a column** | Header check in Raw-to-Bronze | Column appended to the read schema, Bronze evolves via `mergeSchema`, row written to `schema_drift_log`. Silver ignores it (explicit column list) until you decide to add it; the log shows it was noticed. |
| Source **drops/renames** an expected column | Header check | File marked `FAILURE`/quarantined in the log, other files continue. |
| A column **changes type** (integer becomes text like `"N/A"`) | `try_cast` in Bronze-to-Silver | Those rows go to `silver_quarantine` with the column name as reason; clean rows continue. Bronze is unaffected because it is all strings. |
| Corrupt/short CSV row | `PERMISSIVE` mode | Row goes to `bronze_quarantine`. |

Create **test files** for the demo (Step 10): copy a sample CSV, (1) add a column `new_col`, (2) put `"abc"` in `inspection_cycle` on two rows, (3) delete a required column in another copy.

---

## 10. Step 9: Backfill versus standard incremental (document this in the README)

| Scenario | Notebook | Parameters |
|---|---|---|
| **Initial full load** | `01_acquire_cms` → `02_raw_to_bronze` → `03_bronze_to_silver` | `dataset=health_deficiencies`, `load_type=full`, explicit `as_of_date`; Databricks discovers and downloads the CMS bulk CSV |
| **Event incremental** | Same three notebooks | Health Deficiencies/Penalties with `load_type=incremental`; empty `start_date` uses successful watermark minus overlap; explicit `as_of_date` is upper bound |
| **Snapshot-diff incremental** | Same three notebooks | Provider/MDS with `load_type=incremental`; Databricks checks for a new CMS snapshot, downloads it if new, and MERGEs only new/changed rows |
| **Source-date backfill** | `01_acquire_cms` → downstream notebooks | API dataset with explicit `start_date`, `as_of_date`, and `force_refresh=false` |
| **Reprocess one acquired file** | `02_raw_to_bronze` | `source_path=<landing_path from source_file_manifest>`; no local upload |
| **Reprocess by batch** | `03_bronze_to_silver` | `batch_id=<id from pipeline_execution_logs>` |
| **Reprocess an ingestion window** | `03_bronze_to_silver` | `ingest_date_from=2026-08-01`, `ingest_date_to=2026-08-31`, `reprocess=true` |

Three checks that you really are parameterized: (1) grep the repo for `"2026-` and release-specific CMS download URLs and make sure they appear only in docs/tests; (2) `/Volumes/` appears only in widget defaults/config, not transformation functions; (3) the same notebooks run for a different dataset by changing only the `dataset` widget.

If Jobs are not available on your plan, running notebooks manually with widgets is fine; say so in the README.

---

## 11. Step 10: Evidence run (half a day, save screenshots into `docs/evidence/`)

Run `99_demo_idempotency_drift_backfill.py` and keep the outputs:

1. **Automated full acquisition:** start with an empty landing path, run `01_acquire_cms` with `load_type=full`, and show that Databricks resolved the CMS URL, created the file, computed its hash, and wrote a successful manifest/log row. The evidence must not rely on a manually uploaded full-load file.
2. **Acquisition idempotency:** rerun the same full acquisition. Show `SKIPPED_ALREADY_ACQUIRED`, the same content hash, and no second physical copy or Bronze duplication.
3. **Pipeline idempotency:** run Raw-to-Bronze then Bronze-to-Silver. Record `COUNT(*)` and `COUNT(DISTINCT key)` of Bronze and Silver. Run both again. Counts remain identical, and the second Silver log row shows `rows_inserted = 0, rows_updated = 0`.
4. **API incremental:** run Health Deficiencies or Penalties with an explicit `as_of_date`; show paginated landing JSON, the 60-day overlap filter, manifest pages, and only new/changed Silver keys.
5. **Watermark safety:** show the watermark before and after successful Silver completion. Demonstrate that a deliberately failed Silver run does not advance it.
6. **Snapshot-diff incremental:** acquire a new Provider or MDS bulk snapshot and show that unchanged row hashes are not updated while changed/new keys are merged.
7. **Update:** alter one value only in a controlled test copy after acquisition; one Silver row is updated and only that row's `load_timestamp` changes.
8. **Backfill:** acquire/reprocess an explicit old source-date window or batch; no duplicates.
9. **Drift:** run the three controlled test files; show `schema_drift_log`, quarantine, and log rows with the batch still completing for good files.
10. **Failure logging:** use a controlled invalid CMS URL or mock acquisition failure and show a `CMS-to-Landing` `FAILURE`; also show downstream failure logging without modifying a successful manifest row.
11. Query `source_file_manifest`, `ingestion_watermarks`, and `pipeline_execution_logs` to show all three layers and both load types.

---

## 12. Step 11: README and data dictionary

The README must contain: (a) project overview and architecture, (b) automated CMS acquisition design with an explicit statement that full loads are not uploaded manually, (c) **Bronze data model**, (d) **Silver data model**, (e) **execution guide** for initial full, API incremental, snapshot-diff incremental, and backfill runs, (f) the requirements traceability table from Section 0, and (g) known limitations.

Generate the dictionary from code so it cannot drift from reality:

```python
# docs/generate_data_dictionary.py  (run in a notebook where the tables exist)
def dictionary_md(spark, table, pk_cols, descriptions=None):
    descriptions = descriptions or {}
    rows = ["| Column | Type | Nullable | PK | Description |", "|---|---|---|---|---|"]
    for f in spark.table(table).schema.fields:
        rows.append(f"| `{f.name}` | {f.dataType.simpleString()} | {f.nullable} | "
                    f"{'Yes' if f.name in pk_cols else ''} | {descriptions.get(f.name, '')} |")
    return "\n".join(rows)

print(dictionary_md(spark, "carewatch.pipeline.silver_deficiency", ["deficiency_key"]))
```
Paste the output into `docs/data_dictionary.md` and link it from the README. Add descriptions by hand only for columns that need explanation (severity codes, hashes, flags). For the 102-column facility source, group repetitive descriptions rather than manually maintaining another competing schema definition.

---

## 13. Step 12: Commit discipline and submission checklist

- Commit **after every working step** (small commits: "Add Bronze schema for deficiencies", "Add MERGE for silver_deficiency"). Both partners commit. Use feature branches and short pull requests if you can.
- Never commit tokens, salts or the raw data folder.

Final checklist (tick all before submitting the repo link):

- [ ] `grep -ri inferSchema notebooks src` returns nothing (mentions in docs/README are fine)
- [ ] An empty Databricks landing area can perform the initial full load directly from CMS; no full-load file is manually uploaded
- [ ] `source_file_manifest` records URL, strategy, hash, bytes, expected/parsed rows, count validation, landing path, and status for every bulk file/API page
- [ ] API incrementals paginate, use an explicit upper bound and safety overlap, and advance watermarks only after Silver succeeds
- [ ] Provider/MDS refreshes use new CMS snapshots plus row-hash MERGE, not a fabricated event-date incremental
- [ ] Every Bronze and Silver table, plus the log tables, has `load_timestamp`
- [ ] Silver written only via `MERGE INTO`; rerun shows 0 inserted / 0 updated
- [ ] All notebooks use widgets/parameters; no hard-coded dates or paths
- [ ] Drift demo and quarantine tables populated, batch does not crash
- [ ] `pipeline_execution_logs` has `CMS-to-Landing`, `Raw-to-Bronze`, and `Bronze-to-Silver` rows for full and incremental processing
- [ ] README has Bronze model, Silver model, backfill-vs-incremental guide
- [ ] Repo opens in a private browser window for your instructor
- [ ] `docs/evidence/` has screenshots/query outputs

---

## 14. Suggested timeline (adjust to your deadline)

| Day | Work |
|---|---|
| 1 | Workspace, outbound CMS connectivity, registry, setup tables, manifest/watermark design |
| 2 | Databricks CMS acquisition: streaming bulk full load plus paginated API incremental |
| 3 | Raw-to-Bronze and drift handling for Health Deficiencies |
| 4 | Bronze-to-Silver for Health Deficiencies; then replicate for the other datasets |
| 5 | Snapshot-diff refreshes, watermark tests, drift, idempotency, and failure evidence |
| 6 | README, generated data dictionary, screenshots, and final checklist |

---

## Things I could not verify (check them early)

- **Databricks Free Edition specifics:** serverless compute may block RDD/caching APIs, some Spark configs, Jobs, or outbound access to CMS. Test HTTPS requests and writing a streamed response to a UC Volume on day 1. If outbound CMS access is blocked, stop and ask the instructor for an approved workspace/network alternative; manual full-load upload is not an acceptable fallback under the locked requirement.
- **Your exact runtime version:** `try_cast`, `replaceWhere` on a data column and `WHEN NOT MATCHED BY SOURCE` need reasonably recent Databricks/Delta versions. If a statement errors, use the fallback noted next to it.
- **Primary keys:** the proposed keys passed uniqueness and null-component profiling on the September 2026 full extracts. They must still be revalidated for every new full CMS snapshot.
- **Provider Information column mapping:** about 100 columns; some bulk-file headers will not match API names automatically (Step 3).
- **Instructor expectations:** whether Free Edition is accepted in place of Community Edition, and whether SCD2 is mandatory. Ask in one message.
