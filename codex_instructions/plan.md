# Pakistan Economy Data Warehouse — Databricks Implementation Plan

## 1. Purpose and required outcome

This plan converts the proposed Pakistan Economy Data Warehouse into an implementable Databricks project. It is based on:

- `project_overview.MD` for the source, medallion, ingestion, validation, and revision requirements;
- `projected_models.md` for the proposed operational, Bronze, Silver, and Gold models;
- `target_market.md` for the intended users, business questions, and product boundaries; and
- the assignment requirements for explicit PySpark schemas, GitHub version control, `load_timestamp`, idempotent `MERGE INTO`, parameterized backfills, schema-drift handling, and execution logging.

The final submission must contain working PySpark code in Databricks and a GitHub repository that can recreate and run the pipeline. The repository must not contain only screenshots or exported notebooks.

### 1.1 MVP definition of done

Stop adding features when these items work:

1. Databricks Free Edition can run the five SBP CSV sources.
2. All implementation files are committed to GitHub.
3. Original files are preserved unchanged in the managed Volume.
4. Bronze and Silver use explicit schemas; `inferSchema=True` is never used.
5. Every Bronze and Silver row has `load_timestamp` and source lineage.
6. Re-running the same file does not increase Bronze or Silver row counts.
7. Silver writes use Delta `MERGE` with a documented business key.
8. Bad rows go to quarantine without blocking valid rows.
9. Every Bronze and Silver attempt writes status and row counts to the execution log.
10. A historical file can be replayed using notebook parameters without editing code.
11. One sequential Databricks Job runs all five SBP sources successfully.
12. One Gold SQL view returns clean data for an internal analyst.

Incremental source discovery, PBS/OGRA document extraction, advanced revision
auditing, a full Gold star schema, Power BI automation, and CI/CD are completion
criteria for later phases—not for the first internal MVP.

### 1.2 Internal MVP scope — keep the first version small

This is an internal learning MVP, not a production platform. Build the smallest
version that demonstrates the required engineering ideas clearly.

**Build now:**

- Databricks Free Edition only;
- the five structured SBP CSV datasets;
- manual upload of the supplied historical files;
- one managed Volume for staged files;
- Bronze and Silver Delta tables;
- explicit PySpark schemas and casts;
- one quarantine table and one execution-log table;
- parameterized full-load/backfill execution;
- idempotent Bronze and Silver `MERGE` operations;
- one simple Gold SQL view for demonstration; and
- one manually triggered Databricks Job that runs the five SBP sources in sequence.

**Do not build in the first MVP:**

- automated web scraping or API discovery;
- Auto Loader, Delta Live Tables, or streaming;
- Azure infrastructure;
- separate development and production workspaces;
- Databricks Asset Bundles;
- automated CI/CD deployment;
- parallel job execution;
- complex dimensional models or seven Gold facts;
- Power BI refresh automation; or
- Excel/PDF/OCR extraction for PBS and OGRA.

The PBS SPI workbooks, PBS CPI PDF, and OGRA PDF are still staged, hashed, and
documented. Their extraction is Phase 2 because document parsing adds layout,
OCR, and validation risks that are not needed to prove the MVP pipeline.

The core assignment techniques can be demonstrated by the SBP vertical slice:
real files, explicit schemas, Bronze/Silver modeling, timestamps, quarantine,
logging, parameters, and `MERGE`. Add more sources only after this path works.

## 2. Platform decision

### 2.1 Required zero-cost default: Databricks Free Edition

Use Databricks Free Edition for the final graded implementation. It is the only route in this plan that is no-cost by product design rather than merely being paid from promotional credits. It supplies serverless notebooks, default managed storage, a workspace metastore, Lakeflow jobs/pipelines, and one small SQL warehouse, subject to fair-usage quotas.

This route does not require:

- an Azure subscription;
- a credit/debit card;
- an Azure resource group;
- ADLS Gen2;
- classic compute;
- a paid SQL warehouse; or
- a paid GitHub plan.

Use the Free Edition workspace catalog, managed volumes/default storage, serverless PySpark compute, Lakeflow Jobs, and the included single SQL warehouse. If the fair-usage quota is exhausted, work stops until the quota resets; it does not create a bill.

Free Edition limitations that affect this design:

- serverless compute only;
- limited compute size and daily/monthly fair-usage quotas;
- one workspace and one metastore;
- one SQL warehouse, limited to `2X-Small`;
- no custom workspace storage location;
- at most five concurrent job tasks per account;
- restricted outbound internet access; and
- no production SLA or enterprise administration features.

The plan therefore uses sequential source processing, managed volumes, small fixtures, and manual source upload when an official site is blocked by outbound-network restrictions.

### 2.2 Optional Azure for Students path — zero out-of-pocket, but not zero resource consumption

Azure for Students provides USD 100 of credit for 12 months and does not require a credit card. Azure Databricks usage consumes that credit. This is not the default because it is possible to spend the promotional credit rapidly, and Azure spending limits do not cover every Marketplace/external-service charge.

Use Azure only if the instructor explicitly requires Azure-specific evidence. To preserve zero out-of-pocket cost, all of the following are mandatory:

1. Use an **Azure for Students** subscription, not Pay-As-You-Go.
2. Confirm the Azure spending limit is active.
3. Do not remove the spending limit.
4. Do not add a payment method or upgrade to Pay-As-You-Go.
5. Do not purchase Marketplace items, paid support, reserved instances, savings plans, or third-party services.
6. Create only the Databricks workspace and minimum storage required for the demonstration.
7. Use the smallest job compute, auto-termination, and no idle all-purpose cluster.
8. Set Azure budget alerts at 25%, 50%, and 75% of the remaining student credit; alerts are monitoring, not a hard cap.
9. Check remaining student credit before and after every full-history run.
10. Delete the Azure Databricks workspace, managed resource group resources, storage, and access connector after exporting the submission evidence.

If the Azure portal asks to remove the spending limit, add a card, or upgrade the subscription, stop and use Free Edition instead.

### 2.3 Decision gate

Complete this gate before writing pipeline code:

- [ ] Databricks Free Edition workspace can be created and opened.
- [ ] A catalog and schema can be created.
- [ ] A volume can store and read an uploaded test file.
- [ ] GitHub can be connected through a Databricks Git folder.
- [ ] A PySpark job can create and query a Delta table.
- [ ] A workflow/job can be scheduled.
- [ ] The workflow is configured to stay below the five-concurrent-task Free Edition limit.
- [ ] No paid Azure, Databricks, GitHub, Power BI, Marketplace, or support subscription is required.
- [ ] If direct source download is blocked, the file is uploaded manually to the managed volume and still registered in the manifest.

## 3. Target architecture

```text
Five SBP CSV files
        |
        | manual upload for the MVP
        v
Databricks Volume (original files, unchanged)
        |
        v
Bronze Delta tables (raw strings + lineage + load_timestamp)
        |
        v
Silver Delta tables (dates/numbers cast + MERGE + load_timestamp)
        |
        +----> quarantine table
        +----> pipeline execution log
        |
        v
One simple Gold SQL view for the demo
```

Think of the layers this way:

- **Staging:** the original file exactly as received.
- **Bronze:** the file converted into rows, but values are still mostly strings.
- **Silver:** clean dates and numbers, one row per business key, safe to query.
- **Gold:** a small user-facing query/view, not another large engineering project.

### 3.1 Databricks responsibilities

- **Git folder:** opens the GitHub repository inside Databricks.
- **Volume:** stores the original files without changing them.
- **Serverless notebook compute:** runs the PySpark code.
- **Delta Lake:** stores Bronze/Silver tables and supports `MERGE`.
- **Databricks Job:** runs five notebook tasks in a simple sequence.
- **SQL Editor:** creates objects and validates results.

Asset Bundles, CI/CD, alerts, streaming, and complex scheduling are Phase 2.

## 4. Repository and branching design

Use Python modules for reusable logic and thin notebooks only as task entry points. This prevents important business rules from being scattered across notebooks and makes unit testing possible.

```text
pakistan-economy-data-warehouse/
├── databricks.yml
├── resources/
│   ├── jobs.yml
│   ├── permissions.yml
│   └── sql_warehouse.yml                 # optional if managed outside the bundle
├── conf/
│   ├── sources.yml
│   ├── environments/
│   │   ├── dev.yml
│   │   └── prod.yml
│   └── mappings/
│       ├── sbp_series_mapping.csv
│       ├── cpi_item_mapping.csv
│       └── trade_commodity_mapping.csv
├── src/pakistan_economy/
│   ├── __init__.py
│   ├── config.py
│   ├── schemas.py
│   ├── logging.py
│   ├── quality.py
│   ├── hashing.py
│   ├── merge.py
│   ├── quarantine.py
│   ├── source_adapters/
│   │   ├── base.py
│   │   ├── sbp.py
│   │   ├── pbs_spi.py
│   │   ├── pbs_cpi.py
│   │   └── ogra.py
│   ├── bronze/
│   │   ├── sbp.py
│   │   ├── spi.py
│   │   ├── cpi.py
│   │   └── fuel.py
│   ├── silver/
│   │   ├── remittances.py
│   │   ├── fdi.py
│   │   ├── fx.py
│   │   ├── trade.py
│   │   ├── spi.py
│   │   ├── cpi.py
│   │   └── fuel.py
│   └── gold/
│       ├── dimensions.py
│       └── facts.py
├── notebooks/
│   ├── 00_setup.py
│   ├── 10_ingest_source.py
│   ├── 20_raw_to_bronze.py
│   ├── 30_bronze_to_silver.py
│   ├── 40_build_gold.py
│   ├── 50_validate_batch.py
│   └── 90_finalize_run.py
├── sql/
│   ├── create_catalog_objects.sql
│   ├── create_ops_tables.sql
│   ├── create_bronze_tables.sql
│   ├── create_silver_tables.sql
│   └── create_gold_tables.sql
├── tests/
│   ├── unit/
│   ├── integration/
│   ├── fixtures/
│   └── expected/
├── .github/workflows/
│   ├── validate.yml
│   └── deploy.yml
├── docs/
│   ├── data_dictionary.md
│   ├── runbook.md
│   ├── test_evidence.md
│   └── architecture.md
├── requirements.txt
├── pyproject.toml
├── README.md
└── plan.md
```

### 4.1 Git workflow

1. Create or use the existing GitHub repository.
2. Protect `main` if repository permissions allow it.
3. Use short feature branches such as `feature/sbp-bronze` or `feature/execution-logging`.
4. Commit after each testable milestone, not only at the end.
5. Open a pull request, run automated validation, review the diff, and merge to `main`.
6. Include the Git commit SHA in `ops.pipeline_execution_logs.code_version` so every run is traceable to code.
7. Never commit Databricks tokens, Azure secrets, credentials, private connection strings, or `.databrickscfg`.

Suggested milestone commits:

```text
chore: initialize Databricks bundle and project structure
feat: create Unity Catalog objects and operational tables
feat: add explicit Bronze source schemas
feat: ingest SBP FX staging snapshots and manifest
feat: implement idempotent SBP FX Bronze and Silver merges
test: prove rerun and backfill behavior for FX
feat: generalize SBP ingestion to remittances FDI and trade
feat: add SPI Excel extraction and validation
feat: add CPI PDF extraction and lineage
feat: add OGRA OCR quarantine workflow
feat: build Gold dimensions and facts
feat: add Lakeflow Job orchestration and audit finalizer
docs: add runbook data dictionary and submission evidence
```

## 5. Beginner step-by-step implementation — follow this exact order

Do not attempt the later reference sections all at once. Complete each checkpoint
below before moving to the next one. When a checkpoint fails, fix it while the
problem is still small.

### MVP file map

Only these files are needed for the first working version:

| File | What it does | When you use it |
| --- | --- | --- |
| `scripts/prepare_full_load.py` | copies and hashes local raw files | once before upload |
| `conf/sources.yml` | lists source IDs and target tables | reference/configuration |
| `sql/create_catalog_objects.sql` | creates schemas, Volume, logs, and quarantine | once in Databricks SQL |
| `notebooks/10_full_load.py` | Databricks entry notebook | once per SBP source |
| `src/pakistan_economy/bronze/sbp.py` | reads CSV with an explicit schema | called by the notebook |
| `src/pakistan_economy/silver/sbp.py` | casts and validates columns | called by the notebook |
| `src/pakistan_economy/merge.py` | performs idempotent Delta merges | called by the notebook |
| `src/pakistan_economy/logging.py` | records start/end status and row metrics | called automatically |
| `docs/full_load_runbook.md` | short operator instructions | keep open while working |

### Before opening Databricks — prepare the files locally

1. Open PowerShell in the repository folder.
2. Confirm the raw files exist:

   ```powershell
   Get-ChildItem data/raw/full -Recurse -File
   ```

3. Run the staging script. Use a batch ID that never contains spaces:

   ```powershell
   python scripts/prepare_full_load.py `
     --input-root data/raw/full `
     --staging-root data/staging `
     --batch-id full_20261008_001 `
     --ingest-date 2026-10-08
   ```

4. Expected output: `"files": 9` and `"LANDED": 9`. If the same command is
   run again, expected output is `"SKIPPED_IDENTICAL": 9`.
5. Open `data/staging/_manifests/full_20261008_001.jsonl`. Each line contains
   the `source_id`, `manifest_id`, `sha256_hash`, and staged filename needed by
   the notebook.
6. Do not edit anything under `data/staging`. If a source file changes, use a
   new batch ID instead of overwriting the old staged file.

**Checkpoint A:** nine staged files exist and the second run reports nine
identical files.

### First Databricks session — create only the required objects

1. Sign in to Databricks Free Edition.
2. In the left sidebar, open **SQL Editor**. If the SQL warehouse is stopped,
   start the included warehouse and wait until its status is Running.
3. Copy the contents of `sql/create_catalog_objects.sql` into a new query.
4. Leave `workspace` as the catalog unless your Free Edition workspace shows a
   different writable catalog. If it does, replace every `workspace` in the SQL
   file and in later notebook parameters with that catalog name.
5. Click **Run all**.
6. Open **Catalog** in the sidebar and check that these objects exist:

   ```text
   workspace
     ops
       economy_lake       (Volume)
       pipeline_execution_logs
       quarantined_records
     bronze
     silver
     gold
   ```

7. Stop the SQL warehouse when this check is complete. Starting it later for a
   short validation query is fine and remains within Free Edition.

**Checkpoint B:** the four schemas, Volume, and two operational tables are
visible in Catalog Explorer.

### Connect the repository

1. In Databricks, open **Workspace**.
2. Click **Create** and choose **Git folder**.
3. Select GitHub and enter:
   `https://github.com/MhassaanK68/pakistan-economy-data-warehouse.git`.
4. Choose the branch containing the implementation. During development this is
   `feature/full-load-pipeline`; after merging, use `main`.
5. Open the created Git folder and confirm that `notebooks`, `src`, `sql`, and
   `conf` are visible.
6. Do not configure Asset Bundles for the MVP. The Git folder is sufficient for
   version control and keeps the first deployment understandable.

**Checkpoint C:** `notebooks/10_full_load.py` opens inside Databricks.

### Upload the staged files to the Volume

1. Open **Catalog** → `workspace` → `ops` → `economy_lake`.
2. Create a `staging` directory if it does not already exist.
3. Recreate the local partition folders under the Volume and upload the files.
   The SBP remittance file, for example, must end at:

   ```text
   /Volumes/workspace/ops/economy_lake/staging/
     source=SBP_REMITTANCES/
       ingest_date=2026-10-08/
         batch_id=full_20261008_001/
           dataset.csv
   ```

4. Repeat this for all nine files. Only the five `SBP_*` folders are processed
   in the MVP; the PBS/OGRA files are preserved for Phase 2.
5. In a small notebook cell, verify one upload:

   ```python
   display(dbutils.fs.ls(
       "/Volumes/workspace/ops/economy_lake/staging/source=SBP_REMITTANCES/"
       "ingest_date=2026-10-08/batch_id=full_20261008_001/"
   ))
   ```

**Checkpoint D:** Databricks lists the uploaded CSV and its file size is the
same as the local manifest.

### Run one source manually before creating a Job

Start with `SBP_REMITTANCES`; do not run all sources yet.

1. Open `notebooks/10_full_load.py` from the Git folder.
2. Choose the default serverless Python compute when prompted.
3. Click **Run all** once so the text widgets appear at the top.
4. Fill the widgets as follows. Copy `manifest_id` and `source_file_hash` from
   the matching JSONL manifest line—do not shorten the hash.

   | Widget | First-run value |
   | --- | --- |
   | `catalog` | `workspace` |
   | `source_id` | `SBP_REMITTANCES` |
   | `input_path` | `/Volumes/workspace/ops/economy_lake/staging/source=SBP_REMITTANCES/ingest_date=2026-10-08/batch_id=full_20261008_001/dataset.csv` |
   | `batch_id` | `full_20261008_001` |
   | `run_id` | leave blank; the notebook generates one |
   | `manifest_id` | value from the manifest |
   | `source_file_hash` | full SHA-256 value from the manifest |
   | `source_snapshot_at` | `2026-10-08T00:00:00` |
   | `retrieved_at_utc` | value from the manifest |
   | `code_version` | current Git commit from `git rev-parse --short HEAD` |

5. Click **Run all** again.
6. Read the final JSON output. It should show a positive `rows_read` and
   `bronze_inserted`/`silver_inserted` counts. `quarantined` should normally be
   zero, but a non-zero count is acceptable only after inspecting the reason.
7. Open Catalog Explorer and preview:

   ```text
   workspace.bronze.sbp_remittances_raw
   workspace.silver.sbp_remittances
   workspace.ops.pipeline_execution_logs
   ```

8. Confirm that Bronze and Silver both contain a non-null `load_timestamp`.

**Checkpoint E:** one file has travelled from Volume → Bronze → Silver, and two
successful log entries exist for the batch.

### Run the other four SBP files

Use the same notebook and change only the source-specific values. Filenames are
shown here so that spaces and parentheses are not guessed.

| `source_id` | Volume filename | Silver table |
| --- | --- | --- |
| `SBP_FDI` | `dataset (2).csv` | `silver.sbp_fdi` |
| `SBP_FX` | `dataset (4).csv` | `silver.sbp_exchange_rates` |
| `SBP_EXPORTS` | `dataset (6).csv` | `silver.sbp_export_receipts` |
| `SBP_IMPORTS` | `dataset (8).csv` | `silver.sbp_import_payments` |

For each source:

1. Copy its manifest ID and full hash from the JSONL file.
2. Update `source_id` and `input_path`.
3. Keep the same `batch_id` for this historical load.
4. Run the notebook.
5. Do not continue to the next source until the current source has Bronze,
   Silver, and successful execution-log rows.

**Checkpoint F:** all five SBP Silver tables exist and each source has two log
rows: `STAGING → BRONZE` and `BRONZE → SILVER`.

### Prove idempotency

1. Record the row count of one Silver table:

   ```sql
   SELECT count(*) FROM workspace.silver.sbp_remittances;
   ```

2. Run the remittance notebook again with exactly the same parameters.
3. Run the count query again. It must be unchanged.
4. Query the latest logs. The second run must show zero new Bronze and Silver
   inserts; updates must also be zero.
5. Repeat this proof for at least one daily source (`SBP_FX`).

**Checkpoint G:** repeated input produces no duplicate rows.

### Prove backfill and schema handling

For the MVP, “backfill” means replaying an explicitly selected historical file
or batch; it does not require a second code path.

1. Choose a historical staged path and pass it through `input_path` with its
   original manifest/hash metadata.
2. Confirm that no source-code date needs to be edited.
3. For an extra-column test, create a small CSV fixture under `tests/fixtures`
   with the normal header plus `Unexpected Column`. The known fields should load
   and `_rescued_data` should contain the extra value.
4. For a missing-column test, remove `Observation Value` from a fixture header.
   The source branch should fail, and `pipeline_execution_logs` must contain a
   `FAILED` row with the error class/message.
5. For a bad-type test, place `not-a-number` in `Observation Value`. Bronze must
   retain the string, while Silver sends the row to `quarantined_records`.

**Checkpoint H:** backfill uses parameters, additive drift is rescued, missing
required fields are logged as failure, and bad types are quarantined.

### Create one simple Databricks Job

Do this only after all five sources work manually.

1. Open **Workflows** → **Jobs & Pipelines** → **Create job**.
2. Name it `pakistan_economy_mvp_full_load`.
3. Create task `01_sbp_remittances` using `notebooks/10_full_load.py`, serverless
   compute, and the tested remittance parameters.
4. Add `02_sbp_fdi` and set **Depends on** to `01_sbp_remittances`.
5. Add `03_sbp_fx`, depending on `02_sbp_fdi`.
6. Add `04_sbp_exports`, depending on `03_sbp_fx`.
7. Add `05_sbp_imports`, depending on `04_sbp_exports`.
8. Set maximum concurrent runs to `1`. Do not add a schedule yet.
9. Click **Run now** and inspect each task result.
10. Save screenshots of the green task chain and the execution-log query for
    project evidence.

This deliberately simple five-task chain is easier to understand than a loop,
fan-out graph, or dynamic bundle. Add scheduling only after the manual MVP demo
is stable.

**Checkpoint I:** one manual Job run processes all five sources sequentially.

### Add one minimal Gold view

Do not build a star schema for the MVP. Create one view that proves Silver can
serve an internal analyst:

```sql
CREATE OR REPLACE VIEW workspace.gold.v_latest_exchange_rates AS
SELECT
  observation_date,
  series_key,
  series_display_name,
  observation_value,
  unit,
  source_snapshot_at,
  load_timestamp
FROM workspace.silver.sbp_exchange_rates
WHERE observation_date = (
  SELECT max(observation_date)
  FROM workspace.silver.sbp_exchange_rates
);
```

Run `SELECT * FROM workspace.gold.v_latest_exchange_rates`. If it returns clean
rows, the MVP is complete. Power BI, more Gold models, scheduling, documents,
and automated deployment are separate follow-up phases.

### Beginner troubleshooting guide

- **`TABLE_OR_VIEW_NOT_FOUND`:** run `sql/create_catalog_objects.sql` and check
  that the notebook `catalog` widget matches the catalog used in SQL.
- **`PATH_NOT_FOUND`:** copy the exact path from Catalog Explorer; spaces and
  parentheses in filenames matter.
- **Import error for `pakistan_economy`:** confirm the notebook is opened from
  the Git folder and that the repository contains the `src` directory.
- **All rows quarantined:** preview Bronze and compare the date/value strings
  with the formats in `src/pakistan_economy/config.py`.
- **Schema error:** compare the CSV header with `SBP_RAW_COLUMNS` in
  `src/pakistan_economy/schemas.py`.
- **Second run inserts rows:** confirm that the exact same full SHA-256 and
  staged file were used; then inspect the business key and `record_hash`.
- **Free Edition quota message:** stop and wait for quota reset. Do not create a
  paid workspace as a workaround.

The remaining sections are the detailed technical reference and Phase 2
backlog. They explain how to expand the MVP but are not all required before the
first successful demonstration.

## 6. Phase 2 technical reference — not the beginner execution checklist

Everything below this heading is reference material for hardening or expanding
the project after Checkpoints A–I work. Do not treat it as another list that
must be completed before running the MVP.

## Reference Step 1 — Create the cloud workspace

### Zero-cost Free Edition path

1. Create a Databricks Free Edition workspace.
2. Confirm serverless notebook and job compute are available.
3. Confirm the account shows Free Edition and does not request a cloud subscription or payment method.
4. Use the workspace catalog/metastore and managed storage supplied by Free Edition.
5. Create a managed Unity Catalog volume for source, checkpoint, schema, quarantine, and extraction files.
6. Confirm the included SQL warehouse exists; keep it stopped/idle except during SQL validation and Power BI testing.
7. Create a tiny notebook and scheduled job to verify PySpark and Lakeflow Jobs.
8. Record the serverless, quota, storage, network, concurrency, and SLA limitations in `docs/architecture.md`.
9. Do not start a Databricks free trial, Azure subscription, or paid upgrade for this project.

### Optional Azure for Students path

Use this only when the instructor requires Azure-specific evidence and only while the student spending limit remains active.

1. Verify the subscription offer is `Azure for Students`, remaining credit is positive, and no payment method is required.
2. Verify the spending limit is active; take a screenshot for private evidence but do not commit billing identifiers.
3. Create one resource group such as `rg-pak-economy-student`.
4. Create one minimal Azure Databricks workspace and one minimal ADLS Gen2 storage account only if required.
5. Use serverless/job compute or the smallest available auto-terminating configuration.
6. Never run an idle all-purpose cluster.
7. Add budget alerts and review the Azure Sponsorships balance before each large run.
8. Do not deploy any Marketplace or third-party paid offer.
9. Export code, logs, screenshots, and required data before the credit expires.
10. Delete every resource in the dedicated resource group when the demonstration is complete.

### Acceptance evidence

- Screenshot or SQL output showing the workspace, catalog, schema, and volume.
- A notebook cell that writes and reads a small test file from the volume.
- A query that creates and reads a Delta table.
- Free Edition account/workspace evidence, or Azure for Students evidence showing the spending limit remains active.
- The evidence must not expose credentials.

## Reference Step 2 — Add deployment automation later

1. Create a Databricks Git folder by cloning the GitHub repository.
2. Configure GitHub authentication through the supported Databricks Git integration.
3. Confirm pull, branch, commit, and push from the Git folder.
4. Initialize a Declarative Automation Bundle in the repository root.
5. Define `dev` and `prod` bundle targets, but deploy only to the same Free Edition workspace with separate catalog/schema prefixes; do not create a second paid workspace.
6. Add `resources/jobs.yml` for the workflow definition.
7. Run bundle validation before every deployment.
8. Keep the job/workflow definition in Git; do not create an undocumented production job only through the UI.
9. Use GitHub Free. Keep GitHub Actions within the included free allowance or run tests locally if the allowance is unavailable.
10. Do not purchase GitHub-hosted runner minutes, Codespaces, storage add-ons, Marketplace Actions, or a paid GitHub plan.

Example environment naming:

```text
dev catalog:  pakistan_economy_dev
prod catalog: pakistan_economy

schemas per catalog:
  ops
  bronze
  silver
  gold
```

## Reference Step 3 — Expand governed storage and namespaces

Create the following volume structure. Do not use a developer home folder or a rigid date-specific DBFS path as the production landing zone.

```text
/Volumes/<catalog>/ops/economy_lake/
  staging/
    source=<source_id>/ingest_date=YYYY-MM-DD/batch_id=<batch_id>/<original_file>
  checkpoints/
    <source_id>/
  schemas/
    <source_id>/
  quarantine/
    layer=<bronze|silver>/source=<source_id>/reason=<reason>/
  artifacts/
    extraction_debug/
```

Rules:

- A staged source object is immutable.
- The original filename and byte content are preserved.
- A corrected publisher file is a new snapshot, never an overwrite.
- Every staged file has a SHA-256 hash and manifest record.
- Checkpoint and schema-location paths are unique per ingestion workload.
- Paths are constructed by configuration functions; notebook code must not embed a fixed processing date.

## Reference Step 4 — Expand the source registry

Create `conf/sources.yml` as the single configuration point for dataset codes, formats, schedules, lookbacks, and paths.

Required source entries:

| Source ID | Publisher dataset | Format | Frequency | Normal acquisition | Revision lookback |
| --- | --- | --- | --- | --- | --- |
| `SBP_REMITTANCES` | `TS_GP_BOP_WR_M` | CSV | monthly | verified long-format export | 12 months |
| `SBP_FDI` | `TS_GP_BOP_FDIISIC4_M` | CSV | monthly | verified long-format export | 12 months |
| `SBP_FX` | `TS_GP_ES_FADERPKR_M` | CSV | daily | verified long-format export | 90 calendar days |
| `SBP_EXPORTS` | `TS_GP_BOP_XRECCOM_M` | CSV | monthly | verified long-format export | 12 months |
| `SBP_IMPORTS` | `TS_GP_BOP_MRECCOM_M` | CSV | monthly | verified long-format export | 12 months |
| `PBS_SPI` | weekly SPI Report and Annexure | Excel | weekly | discover release page and workbook links | overlap recent releases |
| `PBS_CPI` | Monthly Review on Price Indices | PDF | monthly | discover PDF from listing | recheck replaced PDFs |
| `OGRA_FUEL` | petroleum-price notification | scanned PDF | ad hoc | discover notification link | recheck recent links |

Each entry must also declare:

- expected filename pattern;
- expected header or document extraction version;
- Bronze and Silver target table;
- expected units;
- business key columns;
- active/inactive flag;
- base landing path;
- quarantine policy;
- schedule identifier; and
- secret/config keys, never secret values.

Do not treat portal session IDs or checksums captured in old SBP links as reusable automation endpoints. Until a stable endpoint is verified, support a controlled manual upload mode that still registers the official landing-page URL and the acquisition metadata.

## Reference Step 5 — Generalize parameter handling

Every task entry notebook/script must accept the same core parameter contract:

| Parameter | Type | Required | Purpose |
| --- | --- | --- | --- |
| `source_id` | string | yes for source tasks | selects the configured source |
| `load_type` | enum | yes | `FULL`, `INCREMENTAL`, or `BACKFILL` |
| `start_date` | ISO date | for full/backfill | first requested source/observation date |
| `end_date` | ISO date | for full/backfill | final requested source/observation date |
| `process_date` | ISO date | no | orchestration date, not assumed from system date |
| `batch_id` | string | no | supplied by parent job or generated once |
| `input_path` | string | no | explicit staged path for replay/testing |
| `reprocess` | boolean | no | allow controlled re-evaluation without duplicate writes |
| `contract_version` | string | no | schema/mapping version to apply |
| `triggered_by` | string | no | manual, scheduled, CI, or backfill |

Parameter rules:

1. Validate all enum and date inputs before reading data.
2. Reject `start_date > end_date`.
3. Resolve the input set from the manifest when `input_path` is absent.
4. Never build a path using only `current_date()`.
5. Record the resolved parameter JSON in the execution log.
6. A historical file can be replayed by `batch_id`, date range, source file hash, or explicit path.
7. Generate one `run_id` for the overall workflow and one `execution_log_id` for each task/file/layer operation.

## Reference Step 6 — Add the complete operational model

Operational tables are required before the first source is processed.

### 6.1 `ops.raw_file_manifest`

**Grain:** one row per acquired source file snapshot.  
**Logical key:** `source_id + sha256_hash`. A repeated hash is recorded as `SKIPPED_IDENTICAL` and is not parsed again unless `reprocess=true`.

Required columns:

| Column | Delta type | Required use |
| --- | --- | --- |
| `manifest_id` | STRING | unique manifest identifier |
| `batch_id` | STRING | acquisition batch |
| `source_id` | STRING | source registry identifier |
| `dataset_code` | STRING | SBP dataset code when applicable |
| `landing_page_url` | STRING | official discovery page |
| `download_url` | STRING | exact retrieval URL |
| `requested_period_start` | DATE | requested source window |
| `requested_period_end` | DATE | requested source window |
| `source_file_name` | STRING | original publisher filename |
| `staging_path` | STRING | immutable volume path |
| `file_format` | STRING | `csv`, `xlsx`, or `pdf` |
| `file_size_bytes` | BIGINT | file-level control total |
| `sha256_hash` | STRING | duplicate/replacement detection |
| `retrieved_at_utc` | TIMESTAMP | source snapshot ordering time |
| `release_date` | DATE | publication date where available |
| `reference_period_start` | DATE | first represented period |
| `reference_period_end` | DATE | last represented period |
| `ingestion_status` | STRING | `LANDED`, `SKIPPED_IDENTICAL`, `PARSED`, `FAILED`, `QUARANTINED` |
| `error_message` | STRING | diagnostic text |
| `created_at_utc` | TIMESTAMP | manifest insertion time |

### 6.2 `ops.pipeline_execution_logs`

**Grain:** one row per file/table/layer task attempt. This is the mandatory execution log.

| Column | Delta type | Required use |
| --- | --- | --- |
| `execution_log_id` | STRING | unique task attempt |
| `run_id` | STRING | parent workflow run |
| `parent_execution_log_id` | STRING | optional parent task attempt |
| `databricks_job_id` | STRING | job identity when available |
| `databricks_run_id` | STRING | run identity when available |
| `source_id` | STRING | processed source |
| `batch_id` | STRING | batch being processed |
| `load_type` | STRING | full, incremental, or backfill |
| `layer_from` | STRING | `SOURCE`, `STAGING`, `BRONZE`, `SILVER`, or `GOLD` |
| `layer_to` | STRING | destination layer |
| `operation_name` | STRING | stable code/task name |
| `input_parameter_json` | STRING | exact serialized parameters |
| `input_file_or_table` | STRING | path/table processed |
| `target_table` | STRING | output table |
| `execution_start_utc` | TIMESTAMP | task start |
| `execution_end_utc` | TIMESTAMP | task end |
| `status` | STRING | `RUNNING`, `SUCCESS`, `FAILED`, `PARTIAL`, `SKIPPED` |
| `rows_read` | BIGINT | source rows read |
| `rows_inserted` | BIGINT | target inserts |
| `rows_updated` | BIGINT | target updates |
| `rows_deleted` | BIGINT | normally zero; retained for completeness |
| `rows_rejected` | BIGINT | validation failures |
| `rows_quarantined` | BIGINT | rows routed to quarantine |
| `files_processed` | INT | file count |
| `error_class` | STRING | exception class/category |
| `error_message` | STRING | sanitized failure detail |
| `notebook_or_module` | STRING | code entry point |
| `code_version` | STRING | Git commit SHA or release tag |
| `load_timestamp` | TIMESTAMP | log write time |

Logging sequence:

1. Append a `RUNNING` entry before the task reads its input.
2. Run the task inside `try/except/finally`.
3. Read Delta operation metrics after a write/merge.
4. Update the same log row to `SUCCESS`, `PARTIAL`, `SKIPPED`, or `FAILED`.
5. Always populate the end time.
6. Rethrow failures after logging so the Databricks job reports the correct result.

### 6.3 `ops.data_quality_results`

**Grain:** one result per check, dataset, batch, and table.

Required fields: `quality_result_id`, `run_id`, `batch_id`, `source_id`, `layer`, `table_name`, `check_name`, `check_type`, `severity`, `expected_value`, `actual_value`, `failed_row_count`, `status`, `sample_failure_json`, `checked_at_utc`, and `load_timestamp`.

### 6.4 `ops.schema_drift_events`

**Grain:** one detected source/table contract change.

Required fields: `drift_event_id`, `run_id`, `batch_id`, `source_id`, `source_file_hash`, `expected_schema_json`, `observed_schema_json`, `drift_type`, `affected_columns_json`, `policy_action`, `status`, `detected_at_utc`, `reviewed_by`, `reviewed_at_utc`, and `load_timestamp`.

### 6.5 `ops.observation_revision_audit`

**Grain:** one accepted change to a current Silver observation.

Required fields: `revision_audit_id`, `run_id`, `target_table`, `business_key_json`, `old_record_hash`, `new_record_hash`, `old_values_json`, `new_values_json`, `old_source_file_hash`, `new_source_file_hash`, `old_source_snapshot_at`, `new_source_snapshot_at`, `revision_detected_at_utc`, and `load_timestamp`.

## Reference Step 7 — Automate source acquisition later

Create a common source-adapter interface:

```python
class SourceAdapter(Protocol):
    def discover(self, params: LoadParameters) -> list[SourceObject]: ...
    def download(self, obj: SourceObject) -> bytes: ...
    def derive_metadata(self, obj: SourceObject, content: bytes) -> FileMetadata: ...
```

For each discovered object:

1. Generate or receive `batch_id`.
2. Download the original bytes.
3. Compute SHA-256 before parsing.
4. Check `ops.raw_file_manifest` for the same `source_id + sha256_hash`.
5. If already known and `reprocess=false`, log `SKIPPED_IDENTICAL` and stop for that object.
6. Write the bytes once to the deterministic staging path.
7. Verify written byte count and hash.
8. Append the manifest record as `LANDED`.
9. Pass `manifest_id`, not an untracked raw path, to the Bronze task.

Source-specific acquisition rules:

- **SBP:** export all selected series in long format. Verify a reusable official endpoint before automation. For incremental loads, request new observations plus the revision lookback.
- **PBS SPI:** discover the official weekly release page and linked Report/Annexure workbooks. Never manufacture filenames.
- **PBS CPI:** discover the official PDF link; preserve the PDF unchanged before extracting Tables 1–3 and Annexures A–B.
- **OGRA:** discover notification links; preserve the scanned PDF; distinguish notification date from effective date.

## Reference Step 8 — Full schema dictionaries

All business data schemas must be declared in `src/pakistan_economy/schemas.py` with `StructType` and `StructField`. Do not use `inferSchema=True`.

Example for SBP CSV input:

```python
from pyspark.sql.types import StructField, StructType, StringType

SBP_SOURCE_SCHEMA = StructType([
    StructField("Dataset Name", StringType(), True),
    StructField("Observation Date", StringType(), True),
    StructField("Series Key", StringType(), True),
    StructField("Series Display Name", StringType(), True),
    StructField("Observation Value", StringType(), True),
    StructField("Unit", StringType(), True),
    StructField("Observation Status", StringType(), True),
    StructField("Observation Status Comment", StringType(), True),
    StructField("Sequence No.", StringType(), True),
    StructField("Series name", StringType(), True),
    StructField("_corrupt_record", StringType(), True),
])

df = (
    spark.read
    .schema(SBP_SOURCE_SCHEMA)
    .option("header", True)
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .csv(input_paths)
)
```

Schema rules:

- Read source business values as strings in Bronze when publishers can change formatting.
- Cast to `DATE`, `TIMESTAMP`, `DECIMAL`, `BOOLEAN`, or controlled `STRING` fields in Silver.
- Use `DECIMAL(24,6)` for published numeric measures unless the source demonstrably needs more precision.
- Add `load_timestamp = current_timestamp()` after the source read and before the Bronze/Silver write.
- Use one captured batch timestamp per DataFrame when exact per-run equality is required.
- Store all timestamps in UTC.
- Use `createDataFrame(records, schema=EXPLICIT_SCHEMA)` for records extracted from Excel/PDF/OCR. Never allow Spark to infer the document-extraction result.

### 8.1 Bronze data dictionary

Delta does not enforce traditional primary keys. The following are logical primary keys and must be validated for uniqueness before a write.

#### 8.1.1 `bronze.sbp_economic_series_raw`

**Sources:** all five SBP CSV datasets.  
**Logical primary key:** `source_file_hash + source_record_number`.

| Column | Delta type | Nullable | Definition |
| --- | --- | --- | --- |
| `source_id` | STRING | no | source registry ID |
| `dataset_code` | STRING | no | SBP dataset code |
| `dataset_name_raw` | STRING | yes | source `Dataset Name` |
| `observation_date_raw` | STRING | yes | unparsed source date |
| `series_key_raw` | STRING | yes | source `Series Key` |
| `series_display_name_raw` | STRING | yes | source display label |
| `observation_value_raw` | STRING | yes | uncast published value |
| `unit_raw` | STRING | yes | published unit |
| `observation_status_raw` | STRING | yes | source status |
| `observation_status_comment_raw` | STRING | yes | source status comment |
| `sequence_no_raw` | STRING | yes | source sequence text |
| `series_name_raw` | STRING | yes | source series name |
| `source_record_number` | BIGINT | no | deterministic row number within file |
| `batch_id` | STRING | no | processing batch |
| `manifest_id` | STRING | no | manifest foreign key |
| `source_file_path` | STRING | no | immutable staged path |
| `source_file_hash` | STRING | no | staged SHA-256 |
| `source_url` | STRING | yes | exact official URL |
| `retrieved_at_utc` | TIMESTAMP | no | source snapshot time |
| `source_snapshot_at` | TIMESTAMP | no | ordering timestamp for revisions |
| `_corrupt_record` | STRING | yes | malformed CSV row |
| `_rescued_data` | STRING | yes | unexpected fields/type rescue payload |
| `load_timestamp` | TIMESTAMP | no | Bronze ingestion time |

#### 8.1.2 `bronze.pbs_spi_raw`

**Logical primary key:** `source_file_hash + source_sheet_name + source_row_number + extracted_field_group`.

| Column | Delta type | Nullable | Definition |
| --- | --- | --- | --- |
| `source_id` | STRING | no | `PBS_SPI` |
| `reference_date_raw` | STRING | yes | workbook reference date text |
| `item_code_raw` | STRING | yes | source item identifier |
| `item_name_raw` | STRING | yes | source item label |
| `location_name_raw` | STRING | yes | city/national/coverage label |
| `unit_raw` | STRING | yes | published unit |
| `price_type_raw` | STRING | yes | min/average/max/other |
| `price_value_raw` | STRING | yes | uncast source price |
| `workbook_kind_raw` | STRING | no | `REPORT` or `ANNEXURE` |
| `source_sheet_name` | STRING | no | workbook sheet lineage |
| `source_row_number` | BIGINT | no | workbook row lineage |
| `source_cell_range` | STRING | yes | optional cell/range lineage |
| `extracted_field_group` | STRING | no | logical extracted table/section |
| `batch_id` | STRING | no | processing batch |
| `manifest_id` | STRING | no | manifest foreign key |
| `source_file_path` | STRING | no | immutable staged workbook |
| `source_file_hash` | STRING | no | staged SHA-256 |
| `source_url` | STRING | yes | official workbook URL |
| `retrieved_at_utc` | TIMESTAMP | no | source snapshot time |
| `source_snapshot_at` | TIMESTAMP | no | revision ordering timestamp |
| `_corrupt_record` | STRING | yes | extraction error payload |
| `_rescued_data` | STRING | yes | unexpected fields |
| `load_timestamp` | TIMESTAMP | no | Bronze ingestion time |

#### 8.1.3 `bronze.pbs_cpi_raw`

**Logical primary key:** `source_file_hash + source_page_number + source_table_name + source_row_number`.

| Column | Delta type | Nullable | Definition |
| --- | --- | --- | --- |
| `source_id` | STRING | no | `PBS_CPI` |
| `index_family_raw` | STRING | yes | CPI/index family label |
| `observation_month_raw` | STRING | yes | reported month text |
| `geographic_coverage_raw` | STRING | yes | national/urban/rural text |
| `base_year_raw` | STRING | yes | published base year |
| `series_level_raw` | STRING | yes | overall/group/item text |
| `item_or_group_code_raw` | STRING | yes | source identifier |
| `item_or_group_name_raw` | STRING | yes | source label |
| `cpi_index_raw` | STRING | yes | uncast index value |
| `weight_raw` | STRING | yes | uncast published weight |
| `mom_change_pct_raw` | STRING | yes | uncast monthly change |
| `yoy_change_pct_raw` | STRING | yes | uncast annual change |
| `contribution_pp_raw` | STRING | yes | uncast contribution |
| `source_page_number` | INT | no | PDF page lineage |
| `source_table_name` | STRING | no | Table 1–3 or Annexure A–B |
| `source_row_number` | BIGINT | no | row within extracted table |
| `batch_id` | STRING | no | processing batch |
| `manifest_id` | STRING | no | manifest foreign key |
| `source_file_path` | STRING | no | immutable staged PDF |
| `source_file_hash` | STRING | no | staged SHA-256 |
| `source_url` | STRING | yes | official PDF URL |
| `retrieved_at_utc` | TIMESTAMP | no | source snapshot time |
| `source_snapshot_at` | TIMESTAMP | no | revision ordering timestamp |
| `_corrupt_record` | STRING | yes | extraction error payload |
| `_rescued_data` | STRING | yes | unexpected fields |
| `load_timestamp` | TIMESTAMP | no | Bronze ingestion time |

#### 8.1.4 `bronze.ogra_fuel_raw`

**Logical primary key:** `source_file_hash + source_page_number + source_row_number`.

| Column | Delta type | Nullable | Definition |
| --- | --- | --- | --- |
| `source_id` | STRING | no | `OGRA_FUEL` |
| `notification_date_raw` | STRING | yes | source notification date text |
| `effective_date_raw` | STRING | yes | source effective date text |
| `product_name_raw` | STRING | yes | OCR product label |
| `geographic_applicability_raw` | STRING | yes | applicability text |
| `price_basis_raw` | STRING | yes | prescribed/ex-depot/etc. |
| `prescribed_price_raw` | STRING | yes | uncast component |
| `petroleum_levy_raw` | STRING | yes | uncast component |
| `ifem_raw` | STRING | yes | uncast IFEM component |
| `dealer_commission_raw` | STRING | yes | uncast component |
| `distributor_margin_raw` | STRING | yes | uncast component |
| `gst_raw` | STRING | yes | uncast component |
| `maximum_ex_depot_price_raw` | STRING | yes | uncast published value |
| `unit_raw` | STRING | yes | published unit |
| `ocr_confidence_raw` | STRING | yes | uncast OCR confidence |
| `source_page_number` | INT | no | PDF page lineage |
| `source_row_number` | BIGINT | no | OCR table row lineage |
| `batch_id` | STRING | no | processing batch |
| `manifest_id` | STRING | no | manifest foreign key |
| `source_file_path` | STRING | no | immutable staged PDF |
| `source_file_hash` | STRING | no | staged SHA-256 |
| `source_url` | STRING | yes | official notification URL |
| `retrieved_at_utc` | TIMESTAMP | no | source snapshot time |
| `source_snapshot_at` | TIMESTAMP | no | revision ordering timestamp |
| `_corrupt_record` | STRING | yes | OCR/extraction failure payload |
| `_rescued_data` | STRING | yes | unexpected fields |
| `load_timestamp` | TIMESTAMP | no | Bronze ingestion time |

### 8.2 Silver data dictionary

Common Silver rules:

- `record_hash` is SHA-256 over normalized business fields and relevant source-status fields.
- `source_file_hash`, `batch_id`, `manifest_id`, and `source_snapshot_at` are mandatory lineage.
- `load_timestamp` is mandatory and represents the Silver processing time.
- `updated_at_utc` changes only when an accepted newer revision updates the current record.
- Silver does not replace missing values with zero.
- Validation failures are written to quarantine with the business key, raw fields, rule, and reason.

#### 8.2.1 `silver.sbp_remittances`

**Logical primary key:** `series_key + reporting_month`.

| Column | Delta type | Nullable | Definition |
| --- | --- | --- | --- |
| `reporting_month` | DATE | no | parsed month end |
| `series_key` | STRING | no | SBP series key |
| `series_display_name` | STRING | yes | published label |
| `series_name` | STRING | yes | published series description |
| `geography_name` | STRING | yes | reviewed country/region/total label |
| `geography_level` | STRING | yes | country, region, or total |
| `remittance_value_million_usd` | DECIMAL(24,6) | yes | parsed measure |
| `unit` | STRING | no | expected `Million USD` |
| `observation_status` | STRING | yes | source status |
| `observation_status_comment` | STRING | yes | source comment |
| `is_published_total` | BOOLEAN | no | total/component safeguard |
| `record_hash` | STRING | no | normalized fingerprint |
| `batch_id` | STRING | no | accepted batch |
| `manifest_id` | STRING | no | source manifest |
| `source_file_hash` | STRING | no | source snapshot hash |
| `source_snapshot_at` | TIMESTAMP | no | source ordering timestamp |
| `load_timestamp` | TIMESTAMP | no | Silver processing time |
| `updated_at_utc` | TIMESTAMP | no | last accepted current-state change |

#### 8.2.2 `silver.sbp_fdi`

**Logical primary key:** `series_key + reporting_month`.

| Column | Delta type | Nullable | Definition |
| --- | --- | --- | --- |
| `reporting_month` | DATE | no | parsed month end |
| `series_key` | STRING | no | SBP series key |
| `series_display_name` | STRING | yes | published label |
| `series_name` | STRING | yes | published description |
| `sector_name` | STRING | yes | reviewed sector mapping |
| `flow_type` | STRING | yes | `NET_FLOW`, `INFLOW`, or `OUTFLOW` |
| `aggregation_level` | STRING | no | total, parent, component, or detail |
| `fdi_value_million_usd` | DECIMAL(24,6) | yes | parsed measure |
| `unit` | STRING | no | expected `Million USD` |
| `observation_status` | STRING | yes | source status |
| `observation_status_comment` | STRING | yes | source comment |
| `record_hash` | STRING | no | normalized fingerprint |
| `batch_id` | STRING | no | accepted batch |
| `manifest_id` | STRING | no | source manifest |
| `source_file_hash` | STRING | no | source snapshot hash |
| `source_snapshot_at` | TIMESTAMP | no | source ordering timestamp |
| `load_timestamp` | TIMESTAMP | no | Silver processing time |
| `updated_at_utc` | TIMESTAMP | no | last accepted change |

#### 8.2.3 `silver.sbp_exchange_rates`

**Logical primary key:** `series_key + observation_date`.

| Column | Delta type | Nullable | Definition |
| --- | --- | --- | --- |
| `observation_date` | DATE | no | parsed daily date |
| `series_key` | STRING | no | SBP series key |
| `series_display_name` | STRING | yes | published label |
| `currency_code` | STRING | yes | reviewed currency mapping |
| `currency_name` | STRING | yes | reviewed name |
| `rate_type` | STRING | yes | reviewed rate type |
| `quote_currency_code` | STRING | no | constant `PKR` |
| `exchange_rate_pkr_per_unit` | DECIMAL(24,6) | yes | parsed rate |
| `unit` | STRING | no | published unit |
| `observation_status` | STRING | yes | source status |
| `observation_status_comment` | STRING | yes | source comment |
| `record_hash` | STRING | no | normalized fingerprint |
| `batch_id` | STRING | no | accepted batch |
| `manifest_id` | STRING | no | source manifest |
| `source_file_hash` | STRING | no | source snapshot hash |
| `source_snapshot_at` | TIMESTAMP | no | source ordering timestamp |
| `load_timestamp` | TIMESTAMP | no | Silver processing time |
| `updated_at_utc` | TIMESTAMP | no | last accepted change |

#### 8.2.4 `silver.sbp_export_receipts`

**Logical primary key:** `series_key + reporting_month`.

| Column | Delta type | Nullable | Definition |
| --- | --- | --- | --- |
| `reporting_month` | DATE | no | parsed month end |
| `series_key` | STRING | no | SBP series key |
| `series_display_name` | STRING | yes | published label |
| `commodity_name` | STRING | no | reviewed commodity label |
| `commodity_code` | STRING | yes | validated code only |
| `aggregation_level` | STRING | no | total/parent/component/detail |
| `export_receipts_thousand_usd` | DECIMAL(24,6) | yes | parsed measure |
| `unit` | STRING | no | expected `Thousand USD` |
| `observation_status` | STRING | yes | source status |
| `record_hash` | STRING | no | normalized fingerprint |
| `batch_id` | STRING | no | accepted batch |
| `manifest_id` | STRING | no | source manifest |
| `source_file_hash` | STRING | no | source snapshot hash |
| `source_snapshot_at` | TIMESTAMP | no | source ordering timestamp |
| `load_timestamp` | TIMESTAMP | no | Silver processing time |
| `updated_at_utc` | TIMESTAMP | no | last accepted change |

#### 8.2.5 `silver.sbp_import_payments`

**Logical primary key:** `series_key + reporting_month`.

| Column | Delta type | Nullable | Definition |
| --- | --- | --- | --- |
| `reporting_month` | DATE | no | parsed month end |
| `series_key` | STRING | no | SBP series key |
| `series_display_name` | STRING | yes | published label |
| `commodity_name` | STRING | no | reviewed commodity label |
| `commodity_code` | STRING | yes | validated code only |
| `aggregation_level` | STRING | no | total/parent/component/detail |
| `import_payments_thousand_usd` | DECIMAL(24,6) | yes | parsed measure |
| `unit` | STRING | no | expected `Thousand USD` |
| `observation_status` | STRING | yes | source status |
| `record_hash` | STRING | no | normalized fingerprint |
| `batch_id` | STRING | no | accepted batch |
| `manifest_id` | STRING | no | source manifest |
| `source_file_hash` | STRING | no | source snapshot hash |
| `source_snapshot_at` | TIMESTAMP | no | source ordering timestamp |
| `load_timestamp` | TIMESTAMP | no | Silver processing time |
| `updated_at_utc` | TIMESTAMP | no | last accepted change |

#### 8.2.6 `silver.pbs_spi_prices`

**Logical primary key:** `reference_week_end_date + item_key + location_name + unit + price_type`.

| Column | Delta type | Nullable | Definition |
| --- | --- | --- | --- |
| `reference_week_end_date` | DATE | no | parsed reference week |
| `item_key` | STRING | no | item code or deterministic normalized-name key |
| `item_code` | STRING | yes | published item identifier |
| `item_name` | STRING | no | standardized published label |
| `location_name` | STRING | no | city/national/coverage label |
| `unit` | STRING | no | published unit |
| `price_type` | STRING | no | min/average/max/other |
| `price_value` | DECIMAL(24,6) | yes | parsed source price |
| `workbook_kind` | STRING | no | Report or Annexure |
| `source_sheet_name` | STRING | no | sheet lineage |
| `source_row_number` | BIGINT | no | row lineage |
| `record_hash` | STRING | no | normalized fingerprint |
| `batch_id` | STRING | no | accepted batch |
| `manifest_id` | STRING | no | source manifest |
| `source_file_hash` | STRING | no | source snapshot hash |
| `source_snapshot_at` | TIMESTAMP | no | source ordering timestamp |
| `load_timestamp` | TIMESTAMP | no | Silver processing time |
| `updated_at_utc` | TIMESTAMP | no | last accepted change |

#### 8.2.7 `silver.pbs_cpi`

**Logical primary key:** `index_family + base_year + geographic_coverage + item_or_group_key + aggregation_level + observation_month`.

| Column | Delta type | Nullable | Definition |
| --- | --- | --- | --- |
| `index_family` | STRING | no | CPI/index family |
| `observation_month` | DATE | no | parsed month |
| `geographic_coverage` | STRING | no | national/urban/rural |
| `base_year` | STRING | no | published base year |
| `aggregation_level` | STRING | no | overall/group/item |
| `item_or_group_key` | STRING | no | code or deterministic normalized-name key |
| `item_or_group_code` | STRING | yes | published identifier |
| `item_or_group_name` | STRING | no | standardized label |
| `cpi_index` | DECIMAL(24,6) | yes | parsed index |
| `weight` | DECIMAL(24,6) | yes | parsed published weight |
| `mom_change_pct` | DECIMAL(12,6) | yes | parsed monthly change |
| `yoy_change_pct` | DECIMAL(12,6) | yes | parsed annual change |
| `contribution_pp` | DECIMAL(12,6) | yes | parsed contribution |
| `source_page_number` | INT | no | PDF page lineage |
| `source_table_name` | STRING | no | source table lineage |
| `record_hash` | STRING | no | normalized fingerprint |
| `batch_id` | STRING | no | accepted batch |
| `manifest_id` | STRING | no | source manifest |
| `source_file_hash` | STRING | no | source snapshot hash |
| `source_snapshot_at` | TIMESTAMP | no | source ordering timestamp |
| `load_timestamp` | TIMESTAMP | no | Silver processing time |
| `updated_at_utc` | TIMESTAMP | no | last accepted change |

#### 8.2.8 `silver.ogra_fuel_prices`

**Logical primary key:** `product_name + effective_date + price_basis + geographic_applicability`.

| Column | Delta type | Nullable | Definition |
| --- | --- | --- | --- |
| `effective_date` | DATE | no | parsed effective date |
| `notification_date` | DATE | yes | parsed publication/notification date |
| `product_name` | STRING | no | standardized product |
| `geographic_applicability` | STRING | no | source applicability |
| `price_basis` | STRING | no | prescribed/ex-depot/etc. |
| `price_pkr_per_litre` | DECIMAL(24,6) | yes | primary value for stated basis |
| `prescribed_price_pkr_per_litre` | DECIMAL(24,6) | yes | parsed component |
| `petroleum_levy_pkr_per_litre` | DECIMAL(24,6) | yes | parsed component |
| `ifem_pkr_per_litre` | DECIMAL(24,6) | yes | parsed component |
| `dealer_commission_pkr_per_litre` | DECIMAL(24,6) | yes | parsed component |
| `distributor_margin_pkr_per_litre` | DECIMAL(24,6) | yes | parsed component |
| `gst_pkr_per_litre` | DECIMAL(24,6) | yes | parsed component |
| `maximum_ex_depot_price_pkr_per_litre` | DECIMAL(24,6) | yes | published component |
| `unit` | STRING | no | expected `PKR per litre` |
| `ocr_confidence` | DECIMAL(5,4) | yes | extraction confidence |
| `source_page_number` | INT | no | PDF page lineage |
| `record_hash` | STRING | no | normalized fingerprint |
| `batch_id` | STRING | no | accepted batch |
| `manifest_id` | STRING | no | source manifest |
| `source_file_hash` | STRING | no | source snapshot hash |
| `source_snapshot_at` | TIMESTAMP | no | source ordering timestamp |
| `load_timestamp` | TIMESTAMP | no | Silver processing time |
| `updated_at_utc` | TIMESTAMP | no | last accepted change |

## Reference Step 9 — Harden idempotent Bronze writes

Bronze is append-oriented but must still avoid duplicate rows from a repeated task.

1. Derive a deterministic `source_record_number` or source row locator.
2. Add the manifest metadata and a single captured `load_timestamp`.
3. Split rows with `_corrupt_record IS NOT NULL` or non-null `_rescued_data` to quarantine.
4. Deduplicate the incoming DataFrame on the Bronze logical key.
5. `MERGE` into Bronze on the logical key and insert only unmatched rows.
6. Do not update an existing Bronze row; the original parsed snapshot is immutable.
7. Update the manifest to `PARSED` only after count reconciliation succeeds.

Bronze `MERGE` concept:

```python
target.alias("t").merge(
    incoming.alias("s"),
    "t.source_file_hash = s.source_file_hash "
    "AND t.source_record_number = s.source_record_number"
).whenNotMatchedInsertAll().execute()
```

For document tables, replace `source_record_number` with the documented page/sheet/row composite.

## Reference Step 10 — Extend Bronze-to-Silver transformations

For each Silver dataset:

1. Resolve Bronze rows by `batch_id`, manifest hash, or requested observation window.
2. Rename raw columns to canonical names.
3. Normalize whitespace but retain the published label separately.
4. Parse dates with source-specific format lists.
5. Strip thousands separators and cast numeric text to `DECIMAL`, not binary floating-point, unless a source-specific exception is justified.
6. Preserve nulls. Do not fill missing official values with zero.
7. Join only to reviewed mapping tables.
8. Flag totals, parents, components, and detail rows.
9. Derive the business key and test it for nulls and duplicates.
10. Create `record_hash` from ordered normalized business/status fields.
11. Route invalid rows to quarantine with the failing rule.
12. Deduplicate valid incoming rows so the `MERGE` has at most one row per key.
13. Compare against current Silver and write revision-audit rows before updating.
14. Execute the Silver `MERGE`.
15. Read Delta operation metrics and update the execution log.

Recommended cast-failure pattern:

```python
from pyspark.sql import functions as F

typed = (
    bronze
    .withColumn("parsed_date", F.to_date("observation_date_raw", "dd-MMM-yyyy"))
    .withColumn(
        "parsed_value",
        F.regexp_replace("observation_value_raw", ",", "").cast("decimal(24,6)")
    )
)

invalid = typed.filter(
    F.col("parsed_date").isNull()
    | (F.col("observation_value_raw").isNotNull() & F.col("parsed_value").isNull())
)
```

## Reference Step 11 — Add revision-aware Silver `MERGE INTO`

Before the merge, incoming data must be unique on the business key. Use a deterministic window ordered by `source_snapshot_at DESC`, then `retrieved_at_utc DESC`, then `source_file_hash DESC` to select the latest candidate.

Merge behavior:

- **Not matched:** insert the observation.
- **Matched, same hash:** no operation.
- **Matched, incoming snapshot older:** no operation and optionally log `IGNORED_OLDER_SNAPSHOT`.
- **Matched, newer snapshot and different hash:** insert before/after evidence into `ops.observation_revision_audit`, then update the current Silver row.

PySpark/Delta pattern:

```python
from delta.tables import DeltaTable

target = DeltaTable.forName(spark, target_table)

(
    target.alias("t")
    .merge(incoming.alias("s"), merge_condition)
    .whenMatchedUpdateAll(
        condition=(
            "s.source_snapshot_at > t.source_snapshot_at "
            "AND s.record_hash <> t.record_hash"
        )
    )
    .whenNotMatchedInsertAll()
    .execute()
)
```

Do not use an unconditional `whenMatchedUpdateAll()` because an older downloaded file must not replace a newer accepted observation.

After the merge:

```python
metrics = (
    DeltaTable.forName(spark, target_table)
    .history(1)
    .select("operationMetrics")
    .first()["operationMetrics"]
)
```

Map `numTargetRowsInserted` and `numTargetRowsUpdated` to the logging table. If a runtime returns different metric keys, normalize them in one shared helper and unit-test the helper.

## Reference Step 12 — Expand schema-drift handling

The default policy for this project is **rescue/quarantine, not automatic silent evolution of curated tables**.

### 12.1 Before parsing

1. Read the CSV header or document extraction signature.
2. Compare it with the versioned expected contract.
3. Classify the drift:
   - new optional column;
   - missing required column;
   - renamed column;
   - type/format change;
   - document layout/table change.
4. Write an `ops.schema_drift_events` row.

### 12.2 Policy

- **Extra source column:** retain it in `_rescued_data` or the extraction payload; continue processing known fields; mark the run `PARTIAL` if review is required.
- **Missing required column:** quarantine the entire file and fail that source branch.
- **Type change:** keep the Bronze raw string, quarantine the Silver cast failure, and continue unaffected rows.
- **Renamed required column:** treat as missing + extra until a reviewed contract version maps it.
- **Document layout change:** preserve the file, save extraction diagnostics, quarantine the file, and continue other sources.
- **Approved additive change:** update `schemas.py`, the data dictionary, table DDL, tests, and contract version in Git before using `ALTER TABLE ADD COLUMNS` or controlled `mergeSchema`.

If Auto Loader is used, provide the explicit schema and configure a rescued-data column. Do not enable broad automatic Silver schema evolution. Each independent Auto Loader source must have its own checkpoint/schema location.

## Reference Step 13 — Expand data-quality checks

Run the following checks for every source/batch and write each result to `ops.data_quality_results`:

### Staging and manifest

- file exists and byte count matches manifest;
- calculated SHA-256 equals manifest hash;
- source URL and retrieval timestamp are present;
- duplicate file hash is classified correctly;
- reference period is not after an impossible future boundary.

### Bronze

- expected columns exist;
- `load_timestamp`, `batch_id`, `manifest_id`, path, and hash are non-null;
- Bronze logical key is unique;
- `_corrupt_record` count is zero or fully reconciled to quarantine;
- source row count equals Bronze accepted + quarantined count;
- extracted page/sheet/row lineage is populated for documents.

### Silver

- business key fields are non-null;
- business key is unique;
- dates parse and fit the expected frequency;
- numeric values cast successfully or are quarantined;
- units match the source contract;
- source-file and snapshot lineage are non-null;
- values are not silently changed or filled with zero;
- FDI/trade aggregation levels are known before aggregation;
- FX rates are not aggregated by sum;
- OCR confidence below threshold is quarantined/reviewed;
- incoming snapshot ordering is respected.

### Batch reconciliation

For each file/table transition, enforce:

```text
rows_read = rows_inserted + rows_updated + rows_unchanged
            + rows_rejected/quarantined
```

Account for deduplicated duplicates explicitly; never let them disappear from the reconciliation.

Define blocking severity:

- `ERROR`: stop the source branch and prevent Gold refresh.
- `WARNING`: allow the branch but mark it partial and expose the issue.
- `INFO`: operational observation only.

## Reference Step 14 — Build a full Gold model later

Do not start Gold until each source-specific Silver table passes uniqueness, completeness, unit, lineage, and revision tests.

Create:

```text
gold.dim_date
gold.dim_series
gold.fact_exchange_rate_daily
gold.fact_remittance_monthly
gold.fact_fdi_monthly
gold.fact_trade_monthly
gold.fact_spi_item_price_weekly
gold.fact_cpi_monthly
gold.fact_fuel_price
```

Gold rules:

1. Generate `dim_date` for the entire supported historical range plus a controlled future horizon.
2. Build `dim_series` only from reviewed mapping data; do not infer economic hierarchy from `Sequence No.`.
3. Filter or label a single aggregation level in default FDI/trade views.
4. Never sum FX rates.
5. Keep published CPI/SPI coverage and units in the grain.
6. Expose source freshness and revision indicators.
7. Preserve `record_hash`, `source_file_hash`, and `source_snapshot_at` in facts or drill-through views.
8. Use incremental Gold merges based on affected Silver business keys rather than rebuilding all history for each small load, unless the table size makes a full rebuild simpler and it is documented.

## Reference Step 15 — Advanced Databricks orchestration

Implement two job entry modes using the same task code:

- `pakistan_economy_full_or_backfill`: manually triggered with source/date parameters.
- `pakistan_economy_incremental`: scheduled discovery and revision-lookback processing.

### 15.1 Workflow DAG

```text
00_start_run
      |
10_process_sources_sequentially (For each, concurrency = 1)
      |
      +-- source 1: ingest -> Bronze -> Silver -> quality -> log
      +-- source 2: ingest -> Bronze -> Silver -> quality -> log
      +-- source 3: ingest -> Bronze -> Silver -> quality -> log
      +-- source 4: ingest -> Bronze -> Silver -> quality -> log
      +-- source 5: ingest -> Bronze -> Silver -> quality -> log
      +-- source 6: ingest -> Bronze -> Silver -> quality -> log
      +-- source 7: ingest -> Bronze -> Silver -> quality -> log
      +-- source 8: ingest -> Bronze -> Silver -> quality -> log
      |
40_build_gold
      |
45_gold_quality
      |
50_publish_ready

Failure/always-run branches:
  90_finalize_run (ALL_DONE)
  91_notify_failure (AT_LEAST_ONE_FAILED)
```

The sequential `For each` design is intentional: Free Edition permits at most five concurrent job tasks, and sequential source processing minimizes quota consumption and avoids competing merges. If `For each` is unavailable, define the eight source tasks as an explicit chain. Do not fan out all eight sources in Free Edition. An optional Azure deployment may use a maximum source concurrency of four, but the zero-cost default remains one.

### 15.2 Task behavior

- `00_start_run`: validate parameters, create `run_id`, record workflow start, and resolve source list.
- `10_process_sources_sequentially`: invokes the parameterized source runner once per configured source with concurrency one.
- `ingest`: discover/download or resolve a manually uploaded file, hash, stage, and register manifest.
- `Bronze`: read staged objects with explicit schema and perform insert-only idempotent merge.
- `Silver`: cast, validate, quarantine, audit revisions, and merge current state.
- `quality`: execute blocking and warning checks for the source.
- `40_build_gold`: run only after all required source quality tasks succeed.
- `45_gold_quality`: check fact grain, relationships, and semantic safeguards.
- `50_publish_ready`: update a publication-state table/view so BI sees only a successful batch.
- `90_finalize_run`: aggregate task metrics and set overall run status even when upstream tasks fail.
- `91_notify_failure`: send a sanitized alert with job/run/source and a link to logs; do not include credentials or raw sensitive payloads.

### 15.3 Suggested schedules

Schedules must be configuration-driven and adjusted to observed publication timing.

- SBP FX: daily discovery with a 90-day revision lookback.
- SBP remittances, FDI, exports, imports: monthly discovery with a 12-month lookback.
- PBS SPI: weekly discovery plus recheck of recent releases.
- PBS CPI: daily or weekly lightweight discovery; process when a new/replaced monthly PDF appears.
- OGRA: daily lightweight discovery because notifications are ad hoc.

Do not schedule all full-history sources concurrently during initial loading. Run and reconcile one source at a time.

### 15.4 Retry and timeout policy

- Retry transient download/storage errors with bounded exponential backoff.
- Do not retry deterministic schema/quality failures indefinitely.
- Set per-source task timeouts.
- Set job-level maximum concurrent runs to one.
- Set `For each` source concurrency to one in Free Edition.
- Prevent concurrent merges to the same target table or serialize them by source/table.
- Run full-history sources one at a time and stop if the workspace reports a fair-usage limit.

## Reference Step 16 — Expand unit, integration, and acceptance tests

### 16.1 Unit tests

Test pure functions and small DataFrames for:

- every explicit schema;
- date parsing;
- decimal parsing with commas, blanks, and invalid values;
- label normalization;
- business-key construction;
- record hashing;
- source-specific aggregation-level mapping;
- quarantine reason generation;
- log metric normalization; and
- parameter validation.

### 16.2 Integration tests

Use small fixture files copied from the repository samples:

1. valid SBP CSV;
2. duplicate file rerun;
3. later snapshot with one changed value;
4. older snapshot attempting to overwrite a newer record;
5. new unexpected source column;
6. missing required column;
7. numeric value changed to invalid text;
8. duplicate business key within one input;
9. SPI workbook layout fixture;
10. CPI/OGRA extraction fixture.

### 16.3 Mandatory acceptance demonstrations

#### Idempotency test

1. Run a batch once.
2. Capture Silver row count and hashes.
3. Run the exact same batch again.
4. Prove `rows_inserted=0`, `rows_updated=0`, unchanged Silver count, and unchanged hashes.

#### Revision test

1. Load an original observation.
2. Load a newer snapshot with the same business key and changed value.
3. Prove one Silver update and one revision-audit record.
4. Attempt to load an older snapshot and prove no overwrite.

#### Backfill test

1. Trigger a historical `start_date` and `end_date`.
2. Prove the input files were resolved from parameters/manifest.
3. Prove no source-code path/date change was required.

#### Schema-drift test

1. Add an unexpected source column or incompatible value to a fixture.
2. Prove known rows continue or the source branch fails according to policy.
3. Prove an `ops.schema_drift_events` record exists.
4. Prove affected rows/files are in quarantine with a reason.

#### Failure-log test

1. Trigger a controlled parsing failure.
2. Prove the execution log has start and end time, `FAILED` status, input identifier, and sanitized error.
3. Prove the overall workflow finalizer still runs.

## Reference Step 17 — Configure CI/CD later

### Pull-request validation

GitHub Actions should:

1. install pinned Python dependencies;
2. run formatting/lint checks;
3. run unit tests;
4. scan for accidentally committed secrets;
5. validate YAML/configuration;
6. run offline/static bundle and YAML checks; run `databricks bundle validate` against the Free Edition dev target only when supported authentication is already available; and
7. block merge on failure.

Use GitHub Actions only within the GitHub Free included allowance. If the repository is private and included minutes are exhausted, run the same commands locally and attach the output to the submission rather than purchasing minutes.

### Deployment

On merge to `main`:

1. push and merge the tested Git commit;
2. pull `main` in the Databricks Free Edition Git folder;
3. deploy/validate the bundle from the workspace when supported, or update the Git-backed job definition manually from versioned YAML;
4. run a smoke-test workflow against tiny test fixtures;
5. deploy the same commit to the separate production schema in the same free workspace; and
6. store the Git SHA in job tags and execution logs.

Free Edition does not require a paid CI service or a production service principal for this coursework workflow. If automated Databricks deployment authentication is unavailable, use the documented manual Git-folder deployment rather than opening a paid Azure environment. If optional Azure automation is used, store credentials only in GitHub encrypted secrets or use federated/OAuth authentication. Never place a token in workflow YAML or the repository.

## Reference Step 18 — Connect Power BI later

1. Create approved Gold views in the `gold` schema.
2. Use the single SQL warehouse included with Free Edition; do not create another warehouse or start a paid trial.
3. Grant the BI identity `USE CATALOG`, `USE SCHEMA`, and `SELECT` only on approved views/tables.
4. Use free Power BI Desktop locally. Do not buy Power BI Pro/Premium or publish to a paid Fabric capacity for this submission.
5. First attempt the Databricks connector against the Free Edition SQL warehouse. If the workspace restricts external BI connectivity, export only approved Gold views to CSV/Parquet and import those snapshots into Power BI Desktop; document this Free Edition limitation.
6. Use Import mode to avoid a continuously running connection and unnecessary query consumption.
7. Build relationships from all facts to `gold.dim_date`; use a role-playing/inactive relationship for fuel notification date where required.
8. Relate SBP facts to `gold.dim_series`.
9. Implement measures that enforce aggregation-level and additivity rules.
10. Add a data-quality/freshness page showing source, retrieval time, latest observation, last successful run, quarantine count, and revision count.
11. Do not expose Bronze or unapproved Silver tables directly to Power BI.
12. Store the `.pbix` in the project/submission location; no paid Power BI Service refresh is required.

## Reference Step 19 — Production-style security and governance

- Use least-privilege catalog/schema/table permissions.
- In Free Edition, use the student workspace identity; document that enterprise service-principal separation is an optional production enhancement.
- Keep secrets in Databricks secret scopes or supported environment authentication.
- Do not hardcode URLs throughout notebooks; store public endpoints and dataset codes in configuration.
- Treat any unexpected personal field as schema drift and prevent it from reaching Silver.
- Keep raw public files immutable, but restrict write access to the ingestion identity.
- Use only Free Edition serverless compute and the included SQL warehouse.
- Keep job concurrency at one and never exceed five concurrent tasks.
- Run the full historical loads one source at a time.
- Use the repository samples for development and acceptance tests; do not repeatedly process the entire history.
- Pause work when Free Edition reports a fair-usage limit; wait for the free quota to reset.
- Do not activate a Databricks free trial, add a payment method, or upgrade the account.
- Use GitHub Free and Power BI Desktop only.
- Process incremental/revision windows rather than full history on every schedule.
- Use small fixtures in CI and development.
- Optimize Delta tables only after measuring file-count/query issues; do not add complexity without evidence.
- Do not use external paid OCR, proxy, scraping, notification, storage, or monitoring services. Use local/open-source extraction libraries and Databricks job notifications available in the free workspace.
- If Azure for Students is used, keep the spending limit active, avoid Marketplace items, check remaining credit before every run, and delete the dedicated resource group at project completion.

### 19.1 Zero-cost stop conditions

Stop the setup and return to Free Edition if any screen or workflow asks for:

- a credit/debit card;
- Pay-As-You-Go conversion;
- removal of the Azure spending limit;
- a paid Databricks trial/upgrade;
- a paid Power BI/Fabric license;
- paid GitHub runner minutes or Codespaces;
- a Marketplace subscription; or
- a third-party paid API/service.

The project may consume Free Edition quota or Azure student promotional credit, but it must not create an out-of-pocket charge.

## Reference Step 20 — Phase 2 source implementation order

Implement sources in this order so the reusable framework is proven on the simplest reliable format before document extraction:

1. **SBP FX proof of concept**
   - explicit CSV schema;
   - staging, manifest, Bronze, Silver, quality, logging, and merge;
   - full, incremental, rerun, revision, and backfill tests.
2. **Generalize SBP framework**
   - remittances;
   - FDI;
   - export receipts;
   - import payments.
3. **PBS SPI Excel**
   - Report and Annexure extraction;
   - explicit extracted-row schema;
   - city-column reshape;
   - workbook lineage tests.
4. **PBS CPI PDF**
   - representative PDF table-quality tests first;
   - Tables 1–3 and Annexures A–B;
   - page/table lineage and extraction diagnostics.
5. **OGRA scanned PDF/OCR**
   - OCR confidence;
   - human-review quarantine;
   - effective vs notification date;
   - ex-depot basis labeling.
6. **Gold and Power BI**
   - only after all Silver sources reconcile successfully.

## Reference Step 21 — Full product delivery milestones

### Milestone 1 — Foundation

Deliverables:

- workspace, repository, bundle, catalog/schemas, volume;
- source registry;
- operational tables;
- logging and parameter libraries.

Exit criteria:

- clean bundle validation;
- one workflow writes a successful execution-log record;
- one test file is staged and manifested.

### Milestone 2 — End-to-end SBP FX vertical slice

Deliverables:

- explicit schema;
- immutable staging;
- Bronze table;
- validated Silver merge;
- quality results;
- rerun, revision, drift, and backfill evidence.

Exit criteria:

- all mandatory technical requirements demonstrated on FX.

### Milestone 3 — Remaining SBP sources

Deliverables:

- four additional SBP Silver tables;
- governed mappings for geography, sector, flow, commodity, and aggregation level.

Exit criteria:

- source-to-Silver reconciliation for each full and incremental sample.

### Milestone 4 — PBS and OGRA documents

Deliverables:

- SPI Excel extraction;
- CPI PDF extraction;
- OGRA OCR and review path;
- document lineage and extraction tests.

Exit criteria:

- unreadable/uncertain content is quarantined, not silently accepted.

### Milestone 5 — Gold, BI, and production orchestration

Deliverables:

- conformed dimensions and seven Gold facts;
- SQL warehouse and Power BI connection;
- scheduled incremental job and backfill job;
- monitoring/freshness query.

Exit criteria:

- a successful source run publishes approved Gold data;
- a failed blocking check prevents publication.

### Milestone 6 — Submission package

Deliverables:

- final GitHub URL and commit/tag;
- architecture and data dictionary;
- runbook;
- test evidence;
- screenshots/query results for mandatory demonstrations;
- limitations and future work.

Exit criteria:

- another person can follow the README and deploy/run the project without undocumented manual code edits.

## Reference Step 22 — Expanded submission checklist

### Infrastructure and version control

- [ ] Databricks Free Edition is the default workspace and the platform choice is documented.
- [ ] No payment method, paid trial, Pay-As-You-Go upgrade, or Marketplace purchase is attached to the default implementation.
- [ ] If Azure for Students was used, the spending limit remained active and all project resources were deleted after evidence export.
- [ ] Free Edition source-task concurrency is one and total concurrent tasks remain below five.
- [ ] Only GitHub Free and Power BI Desktop are required.
- [ ] Repository contains all PySpark code and job configuration.
- [ ] Git history shows continuous milestone commits.
- [ ] No credentials are committed.

### Bronze and Silver

- [ ] Full data dictionary is present and matches DDL/code.
- [ ] Every source read uses an explicit `StructType`/`StructField` schema.
- [ ] No `inferSchema=True` exists in the repository.
- [ ] Cast logic is tested.
- [ ] Every Bronze/Silver row has non-null `load_timestamp`.
- [ ] Logical primary keys are documented and tested for uniqueness.

### Robustness

- [ ] Bronze repeated-file processing is idempotent.
- [ ] Silver uses `MERGE INTO` and does not duplicate rows.
- [ ] Newer revisions update; older snapshots do not overwrite.
- [ ] Backfills accept date/batch/path parameters.
- [ ] Schema drift creates rescue/quarantine and drift evidence.
- [ ] One source failure does not erase or corrupt successful source outputs.

### Logging

- [ ] Every file/table/layer task creates an execution-log row.
- [ ] Logs contain layer, input parameter/file, start/end, status, inserted, updated, rejected, and quarantined counts.
- [ ] Failures are logged before being rethrown.
- [ ] Delta operation metrics support inserted/updated values.

### Quality and delivery

- [ ] Full and incremental counts reconcile.
- [ ] Idempotency demonstration is captured.
- [ ] Revision demonstration is captured.
- [ ] Backfill demonstration is captured.
- [ ] Drift/quarantine demonstration is captured.
- [ ] Gold tables follow safe aggregation rules.
- [ ] Power BI reads approved Gold objects only.

## 23. Official implementation references

- [Databricks Free Edition and the retirement of Community Edition](https://docs.databricks.com/aws/en/getting-started/free-edition)
- [Databricks Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)
- [Azure for Students — USD 100 credit and no credit card](https://azure.microsoft.com/en-in/free/students/)
- [Azure spending-limit behavior and exclusions](https://learn.microsoft.com/en-us/azure/cost-management-billing/manage/spending-limit)
- [Azure for Students credit exhaustion and subscription disablement](https://learn.microsoft.com/en-us/azure/cost-management-billing/manage/azurestudents-subscription-disabled)
- [Databricks Git folders](https://docs.databricks.com/aws/en/repos/git-operations-with-repos)
- [Declarative Automation Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles)
- [CI/CD workflows with bundles](https://docs.databricks.com/aws/en/dev-tools/ci-cd/flows)
- [Azure Databricks authorization and unified authentication](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/)
- [Unity Catalog volumes](https://learn.microsoft.com/azure/databricks/connect/unity-catalog/volumes)
- [Unity Catalog storage credentials and external locations](https://learn.microsoft.com/en-us/azure/databricks/connect/unity-catalog/storage-credentials)
- [Auto Loader schema inference, evolution, and rescued data](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/schema)
- [Auto Loader production and schema best practices](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/best-practices)
- [Delta `MERGE INTO`](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-merge-into)

