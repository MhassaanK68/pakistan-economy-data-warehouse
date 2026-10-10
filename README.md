# CareWatch: U.S. Nursing Home Quality Warehouse

CareWatch is a data engineering and business intelligence project for analysing the quality and regulatory performance of Medicare- and Medicaid-certified nursing homes in the United States.

The project uses public datasets from the Centers for Medicare & Medicaid Services (CMS), processes them through a Bronze–Silver–Gold lakehouse architecture with PySpark and Delta Lake, and prepares business-ready data for Power BI.

CareWatch is an analytical decision-support system. It does not provide medical advice or replace official CMS records.

## Project goals

CareWatch is designed to help families, healthcare regulators, nursing-home operators, investors, and analysts answer questions such as:

- Which facilities have the most serious or repeated deficiencies?
- Which states, ownership types, or facility chains have weaker quality and compliance outcomes?
- How do staffing levels and staff turnover relate to quality ratings?
- Which facilities receive the largest fines or payment denials?
- How do deficiencies, penalties, and quality measures change over time?



## Data sources

The source is the [CMS Provider Data Catalog](https://data.cms.gov/provider-data/), accessed through its bulk CSV downloads and public APIs.

The project currently uses four datasets:

- **Provider Information** (`4pq5-n9py`): facility identity, location, ownership type, beds, ratings, staffing, turnover, and chain information.
- **Health Deficiencies** (`r5ix-sfxw`): inspection citations, deficiency categories, severity, survey dates, and correction status.
- **Penalties** (`g6vv-u9sr`): fines and payment-denial events.
- **MDS Quality Measures** (`djen-97ju`): quarterly and four-quarter resident quality measures.

The common facility join key is `cms_certification_number_ccn` (CCN).

## Architecture

```text
CMS API and bulk CSV files
            |
            v
Bronze: raw source-shaped records and ingestion metadata
            |
            v
Silver: typed, validated, privacy-treated, deduplicated records
            |
            v
Gold: facts, dimensions, scorecards, and BI aggregates
            |
            v
Power BI dashboards
```



### Bronze

Bronze preserves source values as strings and adds operational metadata such as the source file, batch ID, load type, ingestion date, and `load_timestamp`. Distinct batches append logically; an explicit rerun atomically replaces only the same deterministic batch so ingestion remains auditable and idempotent.

### Silver

Silver applies explicit data types, standardises names and categories, validates business keys, removes duplicates, and uses Delta `MERGE` for idempotent inserts and updates. Invalid records are routed to quarantine rather than silently discarded.

Sensitive contact fields are excluded from analytical outputs. Street addresses are intended to be hashed with a securely supplied salt before publication to Silver.

### Gold

The planned Gold model contains:

- `dim_facility`
- `dim_geography`
- `dim_date`
- `dim_deficiency`
- `fact_deficiency`
- `fact_penalty`
- `fact_facility_snapshot`
- state-quality, ownership-comparison, facility-risk, and deficiency-trend aggregates

Power BI should connect to Gold tables or approved aggregate views, not directly to raw source files.

## Repository layout

```text
us-healthcare-warehouse/
├── data/
│   ├── raw/                         # Original downloads; ignored by Git
│   └── samples/
│       ├── full_load/               # Historical CSV parts
│       ├── incremental/             # Incremental API response samples
│       └── samples_manifest.json    # Source, row-count, and file metadata
├── docs/
│   ├── data_analysis_project_proposal.md
│   ├── phase2_guidelines.md
│   └── bronze_silver_schema_contract.md
├── notebooks/
│   ├── 00_setup_tables.py
│   ├── 01_acquire_cms.py
│   ├── 02_raw_to_bronze.py
│   ├── 03_bronze_to_silver.py
│   ├── 04_migrate_silver_quarantine.py
│   ├── 98_cleanup_mistaken_incremental.py
│   └── 99_demo_idempotency_drift_backfill.py
├── src/carewatch/
├── initial_ingestion_samples/
│   └── initial_fetch_for_samples.py
├── .gitignore
└── README.md
```

The Phase 2 implementation will add Databricks notebooks, a reusable `src/carewatch` package, tests, operational SQL, and generated data-dictionary documentation.

## Preparing the sample data

The files under `data/samples` are local profiling and schema-development evidence only. **They are not the Phase 2 full-load input and must not be uploaded to Databricks.** The production Phase 2 flow starts with an empty landing area; Databricks discovers and streams the official CMS bulk files itself and creates incremental landing pages through the CMS API.

### Prerequisites

- Python 3.10 or later
- `requests`
- Internet access to `data.cms.gov`

Install the ingestion dependency:

```bash
python -m pip install requests
```

Download the CMS bulk files, split them into GitHub-safe parts, and create incremental samples:

```bash
python ingestion/initial_fetch_for_samples.py
```

The script defaults to:

- storing original downloads in `data/raw/`;
- storing version-controlled samples in `data/samples/`;
- keeping each CSV part below 45 MiB;
- requiring more than 210 MiB of historical sample data;
- using `2026-07-01` as the deficiency cutoff;
- using `2026-06-01` as the penalty cutoff;
- limiting each incremental API sample to 5,000 rows.

To rebuild the samples from files already present in `data/raw/` without downloading them again:

```bash
python ingestion/initial_fetch_for_samples.py --reuse
```

Useful overrides include:

```bash
python ingestion/initial_fetch_for_samples.py \
  --cutoff 2026-07-01 \
  --penalty-cutoff 2026-06-01 \
  --part-mb 45 \
  --min-mb 210 \
  --incr-rows 5000
```

Do not commit `data/raw/`. The generated manifest records the CMS dataset IDs, source URLs, catalog dates, split cutoffs, row counts, part sizes, and incremental filters.

## Phase 2 engineering requirements

The Bronze and Silver implementation must satisfy the following requirements:

1. Acquire initial/full loads directly inside Databricks from CMS-discovered bulk URLs; do not upload full-load files manually.
2. Acquire Health Deficiency and Penalty incrementals through paginated, date-windowed CMS API calls with a safety overlap and success-controlled watermarks.
3. Refresh Provider and MDS data through newly published CMS snapshots and merge only changed/new target rows.
4. Define every input with explicit PySpark `StructType` and `StructField` schemas; do not use `inferSchema`.
5. Add `load_timestamp` to every record in every table.
6. Run acquisition for all registered datasets automatically, using today UTC as the upper bound, a successful Bronze extraction watermark for normal API incrementals, and parameterized load type, paths, batch ID, catalog, schema, and landing root. Registry history starts are reserved for explicit backfills.
7. Support both standard incremental runs and reproducible historical backfills.
8. Use append-only Bronze tables with source lineage.
9. Use deterministic business keys, row hashes, and Delta `MERGE` in Silver.
10. Make reruns idempotent: processing the same input twice must not create duplicates.
11. Detect schema drift and either evolve compatible schemas deliberately or quarantine incompatible data.
12. Record acquisition manifests, watermarks, run status, timing, parameters, and row metrics.
13. Demonstrate automated full-load, incremental, idempotency, schema-drift, quarantine, and backfill scenarios.

See [Phase 2 guidelines](docs/phase2_guidelines.md) for the implementation sequence and evidence checklist.

### Running Raw-to-Bronze

Run `00_setup_tables.py` and `01_acquire_cms.ipynb` before `02_raw_to_bronze.py`. Raw-to-Bronze reads only successful records from `source_file_manifest`; it never scans the landing Volume for unregistered files.

The Raw-to-Bronze notebook accepts:

- `dataset`: one registry dataset or `all`;
- `load_type`: `full`, `incremental`, or `all`;
- `acquisition_run_id`: optional exact acquisition run;
- `source_path`: optional exact path that must already exist in the manifest;
- `reprocess`: defaults to `false`; set it to `true` only for deliberate batch replacement;
- `catalog` and `schema`: Unity Catalog namespace.

Each file or API page receives an independent execution-log row. Compatible added columns evolve the managed Delta Bronze table and are recorded in `schema_drift_log`. Missing required columns reject only that file. Corrupt CSV records enter `bronze_quarantine`, while valid records from the file continue to Bronze. A failed or partially quarantined acquisition run never advances its source watermark.

### Running Bronze-to-Silver

Before the first Step 7 run, `silver_quarantine` must have the current eight-column schema. If setup reports `LEGACY_SCHEMA_ACCEPTED_NO_MIGRATION`, run `04_migrate_silver_quarantine.py` in its default `preview` mode. Preview is read-only. Preparing a candidate table and activating it are separate, approval-gated operations; activation retains the old table as `silver_quarantine_legacy_backup`.

`03_bronze_to_silver.py` accepts:

- `dataset` and `load_type`, each defaulting to `all`;
- optional `acquisition_run_id` and comma-separated `batch_ids`;
- optional inclusive Bronze `ingest_from` and `ingest_to` dates for backfill;
- `reprocess`, which defaults to `false` and deliberately retries completed batches when enabled;
- `allow_soft_deletes`, which defaults to `false`;
- `address_secret_scope` and `address_secret_key`, which identify the Databricks Secret containing the stable address-hashing salt;
- `catalog` and `schema`.

With no scope filters, the notebook processes successful Raw-to-Bronze batches that have no terminal Silver log. `SUCCESS` and `QUARANTINED_PARTIAL` are terminal; `FAILURE` remains retryable. It trims strings, converts empty values to null, safely casts values, applies the locked validation rules, removes clear-text contact fields, hashes addresses, generates deterministic keys and business hashes, collapses exact duplicates, quarantines invalid/conflicting rows, and performs an idempotent Delta `MERGE`.

The salt value is never returned to the Python driver or embedded with `F.lit`. The transformation invokes Databricks SQL `secret(scope, key)` inside Spark, so only the non-secret scope and key names appear in the submitted expression. Databricks secret redaction is best-effort, so the code never selects or logs the secret expression. A lookup or permission failure is captured by the per-batch audit boundary.

Batch reads and their materializing counts run inside that same audit boundary. An existing batch excluded by explicit ingestion dates is reported but receives no terminal Silver log. A date filter must include either all or none of a Bronze batch; a partial match fails instead of incorrectly checkpointing only part of a batch. A manifest/log batch with no Bronze rows is a failure. The notebook displays every failure after processing independent batches, then raises `RuntimeError` so a Databricks Job reports failure while already successful batches remain committed and are skipped on retry.

Identical matches are not updated, so their `load_timestamp` stays unchanged. Changed rows update only when the incoming `source_processing_date` is present and is not older than the current row; this prevents an old or undated backfill from replacing current SCD1 values. Backfill reads durable Bronze and never reacquires CMS data or advances `ingestion_watermarks`.

Soft deletion is disabled unless `allow_soft_deletes=true`. Even then, it is blocked for API windows, filtered/reprocessed scopes, empty or unreconciled snapshots, multi-artifact snapshots, snapshots older than the newest validated Bronze snapshot, snapshots without a processing-date scope bound, and any snapshot with quarantined or conflicting rows. Eligible deletion is limited to target rows whose processing date is not newer than the snapshot bound. The current acquisition design produces one bulk artifact for Provider and MDS snapshots; a future multi-artifact snapshot requires a deliberate run-level deletion design rather than weakening this guard.

The source values for some CMS descriptive domains, especially deficiency correction status, are not formally enumerated in the locked contract. Step 7 enforces the documented conditional correction-date rule but does not invent a closed domain that could reject a new valid CMS label.

### Cleaning the accidental 2026-10-10 API bootstrap

Run `notebooks/98_cleanup_mistaken_incremental.py` in Databricks twice:

1. Leave `confirmation` empty. Review the preview counts and paths. The notebook must report that all safety checks passed.
2. Set `confirmation` to `DELETE_MISTAKEN_API_BOOTSTRAP_2026_10_10` and run it again.

The cleanup is limited to Health Deficiencies and Penalties API windows that began at their registry history dates and ended on `2026-10-10`. It refuses batches referenced by Bronze, Silver, another manifest, or a watermark. It deletes only exact manifest-listed files and their matching acquisition manifest/execution-log rows; it never recursively deletes a Volume directory.

## Planned implementation order

1. Create the Databricks catalog/schema, empty landing areas, manifest, watermark, and operational log tables.
2. Implement CMS metastore discovery, streaming bulk downloads, and paginated API acquisition.
3. Add a central dataset registry and explicit Bronze schemas.
4. Implement audit, watermark, and schema-drift helpers.
5. Build Health Deficiencies from CMS acquisition through Bronze and Silver.
6. Repeat the config-driven flow for penalties, provider information, and MDS quality measures.
7. Add quarantine handling, backfill controls, and evidence notebooks.
8. Create Gold facts, dimensions, aggregates, and the Power BI semantic model.

Provider Information can initially use a Type 1 upsert. Slowly changing facility history (SCD Type 2) should only be added after the required Bronze and Silver pipeline is working reliably.

## Data quality and security

- Telephone numbers must not be published to Silver or Gold.
- Street addresses should be hashed with SHA-256 and a salt supplied through a secret, never committed to Git.
- Invalid CCNs, dates, numeric values, and required keys must be quarantined with an actionable reason.
- CMS schema changes must not silently alter downstream tables.
- Raw source values and ingestion lineage must remain available for auditing.
- Missing numeric values must remain null rather than being converted to zero.



## Current status

Completed:

- Project proposal and target analytical questions
- CMS dataset selection
- Historical and incremental sample-generation utility
- Sample manifest with source and volume metadata
- Detailed Phase 2 implementation guide
- Locked, API-verified Bronze and Silver schema contract
- Step 2 CMS acquisition: registry, metastore-driven bulk downloads, paginated API loads, snapshot refreshes, landing manifests, and watermark lookup
- Step 3 explicit Bronze schemas: all-string source contracts, typed lineage metadata, incremental JSON envelopes, and strict bulk-header mapping
- Step 4 operational controls: idempotent Delta table setup, typed execution logs, acquisition audit integration, watermarks, drift logs, and Silver quarantine contracts
- Bronze-checkpoint acquisition safeguards: no history fallback, exact-window reuse, pending-Bronze blocking, unchanged-snapshot preflight, and page progress
- Step 5 Raw-to-Bronze: manifest-only selection, landed-file hash verification, explicit CSV/JSON schemas, compatible schema evolution, corrupt-row quarantine, deterministic Delta batch replacement, run-level count reconciliation, and Bronze-controlled watermarks
- Local Step 5 regression tests for drift classification, file integrity, API-envelope validation, and all checked-in full/incremental sample schemas
- Step 5 evidence views for manifest/Bronze/quarantine count conservation, repeated-batch idempotency, drift events, and source watermarks
- Guarded cleanup notebook for the accidental 2026-10-10 history-wide API acquisition
- Step 6 explicit Silver schemas, keys, validation specifications, quarantine contract, and data dictionary
- Step 7 local implementation: manifest-driven Bronze selection, validation, privacy projection, deterministic hashes, duplicate/conflict handling, idempotent Delta MERGE, retry/backfill safeguards, execution logging, and evidence views
- Separate preview-first, non-destructive legacy Silver quarantine migration notebook
- Focused local Step 7 policy and contract tests

Still to be verified or implemented:

- Databricks execution of the Silver quarantine migration after separate approval
- Databricks Spark/Delta integration and evidence runs for Bronze-to-Silver
- Databricks integration/evidence runs for the implemented Bronze pipeline
- Controlled drift fixture runs and saved screenshots/query outputs
- Gold analytical model
- Power BI dashboard



## Documentation

- [Project proposal](docs/data_analysis_project_proposal.md)
- [Phase 2 implementation guidelines](docs/phase2_guidelines.md)
- [Bronze and Silver schema contract](docs/bronze_silver_schema_contract.md)



## Team

- Hassan Mehmood — 24L2559, BDS-5B
- Hanan Ishaq — 24L2537, BDS-5B

