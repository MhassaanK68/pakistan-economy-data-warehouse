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

Bronze preserves source values as strings and adds operational metadata such as the source file, batch ID, load type, ingestion date, and `load_timestamp`. Bronze data is append-only so every ingestion event remains auditable.

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
- Guarded cleanup notebook for the accidental 2026-10-10 history-wide API acquisition

Still to be implemented: 

- Bronze ingestion
- Silver validation and Delta merges
- Bronze/Silver quarantine processing and transformation-layer audit integration
- Automated tests and evidence notebooks
- Gold analytical model
- Power BI dashboard



## Documentation

- [Project proposal](docs/data_analysis_project_proposal.md)
- [Phase 2 implementation guidelines](docs/phase2_guidelines.md)
- [Bronze and Silver schema contract](docs/bronze_silver_schema_contract.md)



## Team

- Hassan Mehmood — 24L2559, BDS-5B
- Hanan Ishaq — 24L2537, BDS-5B

