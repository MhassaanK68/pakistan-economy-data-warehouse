# CareWatch Project Status

Last updated: 2026-10-10 (Asia/Karachi)  
Repository state reviewed: `main` at `a28bac2` (`origin/main`) plus the current Step 5 working-tree implementation
Current review scope: Phase 2 Steps 1–5 from `docs/phase2_guidelines.md`

Latest implementation update: Raw-to-Bronze is implemented locally and passes focused regression verification. A Databricks Delta/Unity Catalog smoke run is still required.

## Overall state

Steps 1–5 are substantially implemented. Raw-to-Bronze now consumes only trusted manifest records, verifies landed bytes, handles compatible and breaking drift, writes idempotent managed Delta batches, quarantines corrupt CSV rows, and advances event watermarks only after complete run reconciliation.

| Step | State | Summary |
|---|---|---|
| 1. Workspace and repo setup | Partially verified | Repository structure, setup notebook, and focused local regression tests exist. Databricks workspace linkage, outbound CMS connectivity, landing subdirectories, and screenshots cannot be verified from the local repository. |
| 2. Automated CMS acquisition | Implemented; Databricks verification pending | Registry-driven full and incremental acquisition, streaming, hashing, API pagination, snapshot refresh, and watermark prechecks exist. Manifest dates, failed-download cleanup, and reuse auditing are fixed. The acquisition widget still defaults to incremental instead of full. |
| 3. Explicit Bronze schemas | Implemented | Four explicit all-string source schemas, typed Bronze metadata, incremental envelopes, canonical header normalization, and the Provider header override are present. No `inferSchema` usage was found under `src/` or `notebooks/`. |
| 4. Logging framework | Implemented; Databricks verification pending | Typed Delta schemas and helpers exist for execution logs, manifests, watermarks, drift logs, and Silver quarantine. Exact-window early returns now produce typed skip log rows. |
| 5. Raw-to-Bronze | Implemented; Databricks verification pending | Registry-driven CSV/API-page ingestion, integrity validation, schema drift, Bronze quarantine, per-file audit, deterministic batch replacement, manifest reconciliation, and Bronze watermark commits are implemented. |

## Review findings and resolutions

### 1. Resolved — `as_of_date` is stored in every manifest row

- Requirement: Step 2 says the standard acquisition run must derive the current UTC `as_of_date` and record it in every manifest row.
- Resolution: `AcquiredFile` and `MANIFEST_SCHEMA` now include non-null `as_of_date DATE`, and all acquisition strategies populate it from the UTC-derived request date.
- Migration: setup adds the field to an existing manifest and backfills API rows from `window_end`, otherwise from the UTC `load_timestamp` date.
- Documentation: the Step 4 manifest contract table now lists `as_of_date` explicitly.

### 2. Resolved — bulk acquisition removes `.partial` files after post-download failures

- Requirement: Step 2 requires removal of only that run's `.partial` file on failure.
- Resolution: all post-download validation and finalization now run inside `try/finally`; the `finally` clause unlinks only the exact run-scoped temporary path if it still exists.

### 3. Resolved — exact-window incremental reuse writes execution logs

- Requirement: Step 4 defines one execution-log row per processed file/table per attempt, and Step 2 requires reusable acquisition behavior to remain auditable.
- Resolution: before returning early, the notebook now writes one typed `CMS-to-Landing` `SKIPPED_ALREADY_ACQUIRED` row per reused batch/page, including the prior acquisition run, exact window, and landing path.

### 4. Low — acquisition defaults to `incremental`, contrary to the Step 2 guide

- Requirement: the documented acquisition widget defaults `load_type` to `full`, supporting the initial run.
- Evidence: `notebooks/01_acquire_cms.ipynb` defaults the widget to `incremental`.
- Impact: a fresh workspace immediately fails for missing Bronze watermarks unless the operator changes the widget, making the default path inconsistent with the implementation guide.
- Proposed fix: change the widget and saved notebook widget metadata defaults to `full`.

### 5. Verification limitation — Step 1 and Databricks integration remain unverified

- The repository now has focused `unittest` coverage for the approved fixes, but not yet a complete pipeline test suite.
- `docs/evidence/` contains only `.gitkeep`; there is no checked-in evidence for workspace linkage, catalog/schema/Volume creation, outbound CMS requests, or Delta table creation.
- Local checks cannot execute PySpark/Delta or Databricks `dbutils`, so the setup notebook and table DDL still require a Databricks smoke run.

## Checks performed

| Check | Result | Notes |
|---|---|---|
| Bugbot branch-diff review | No diff available | `main` equals `origin/main`; Bugbot had no branch changes to compare. Current files were reviewed directly against the guidelines instead. |
| Python syntax compilation | Pass | All Python source and notebook Python cells compiled with bundled Python 3.12.14. |
| Notebook JSON validation | Pass | `notebooks/01_acquire_cms.ipynb` is valid JSON. |
| Config smoke checks | Pass | Four datasets load; qualified-name validation accepts safe identifiers and rejects an unsafe identifier. |
| Acquisition helper smoke checks | Pass | Boolean/date parsing, 60-day watermark overlap, request validation, CSV distribution selection, and deterministic batch ID behavior passed with a stubbed HTTP module. |
| Explicit-schema scan | Pass | No `inferSchema` references found under `src/` or `notebooks/`. |
| Hard-coded Volume-path scan | Pass | `/Volumes/` appears only in setup/configuration and widget defaults, not transformation functions. |
| Release-specific CMS URL scan | Pass | No release-specific bulk CSV URL is hard-coded. |
| PySpark/Delta/Databricks execution | Not run locally | Bundled runtime does not include `pyspark`; no connected Databricks execution environment was available in this review. |
| Focused Raw-to-Bronze regression suite | Pass | Thirteen tests cover drift classification, header overrides, file integrity, API-envelope validation, and all checked-in full/incremental sample source shapes. |

## Current implementation inventory

- `notebooks/00_setup_tables.py`: creates/verifies catalog, schema, Volume, and Step 4 control tables.
- `notebooks/01_acquire_cms.ipynb`: parameterized all-dataset CMS-to-Landing orchestration.
- `src/carewatch/config.py`: central four-dataset registry and safe qualified-name helper.
- `src/carewatch/acquire.py`: bulk discovery/download, incremental API pagination, hashing, idempotency identities, and landing writes.
- `src/carewatch/schemas.py`: explicit source, CSV-read, Bronze, and API-envelope schemas.
- `src/carewatch/audit.py`: typed control-table contracts, table setup, and execution logging.
- `src/carewatch/watermarks.py`: Bronze checkpoint and pending/exact-window lookup support.
- `src/carewatch/drift.py`: pure compatible/breaking source-shape classification.
- `src/carewatch/bronze.py`: manifest selection, explicit readers, integrity checks, quarantine, Delta batch writes, and run reconciliation.
- `notebooks/02_raw_to_bronze.py`: parameterized per-file orchestration and evidence summaries.
- `notebooks/99_demo_idempotency_drift_backfill.py`: scoped manifest/Bronze/quarantine, idempotency, drift, and watermark evidence views.
- `tests/`: local drift, integrity, envelope, and checked-in sample-contract regression tests.

## Remaining work and next update

The next required verification is a Databricks smoke run of `00_setup_tables.py`, `01_acquire_cms.ipynb`, and `02_raw_to_bronze.py`. Verify managed Delta creation, `replaceWhere` plus `mergeSchema`, corrupt-record capture, manifest updates, full-load watermark seeding, multi-page incremental reconciliation, and the no-watermark-on-partial rule. After that evidence is saved, Step 7 Silver work can begin.
