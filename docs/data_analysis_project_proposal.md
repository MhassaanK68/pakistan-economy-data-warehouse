[Page 1]

# Data Analysis Project Proposal

## Team Members
1. Hassan Mehmood 24L2559-BDS5B
2. Hanan Ishaq - 24L2537 - BDS5B

## Project Purpose & Target Audience
**Target Market:** Families, healthcare regulators, investors, and nursing home chain operators.

**Key Questions Answered:** This project provides a data-driven tool to uncover insights into nursing home quality. It specifically answers:
* Which facilities are unsafe for residents?
* Which states or facility owners have the worst performance and compliance records?
* How do staffing levels and ownership types (e.g., for-profit vs. non-profit) impact overall care quality and regulatory outcomes?

## Executive Summary & Architecture
We are building an automated data engineering pipeline to analyze U.S. healthcare quality in Medicare/Medicaid-certified nursing homes. The pipeline will ingest data from CMS, process it through the Bronze, Silver, and Gold layers of the Medallion Architecture using Apache Spark, and serve a Power BI dashboard.

## Data Sources & Volumes
*   **Source:** CMS Provider Data Catalog API (Metastore and Datastore)
*   **API Documentation:** data.cms.gov/provider-data/about/api
*   **Metastore (catalog):** data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items
*   **Datastore (query):** data.cms.gov/provider-data/api/1/datastore/query/(datasetId}/0

All datasets share the `cms_certification_number_ccn` (a 6-character ID) as a clean join key.

| Dataset Name | Resource ID | Row Count | Attributes | Est. Volume | Load Strategy & Behavior |
| :--- | :--- | :--- | :--- | :--- | :--- |
| Provider Information | 4pq5-n9py | 14,690 | ~100 cols | 20-25 MB | Full snapshot monthly. Tracks facility dimensions (ratings, ownership, beds). |

[Page 2]

```text
┌────────────────┐    ┌────────────────┐    ┌──────────────────┐
│  BRONZE LAYER  │───>│  SILVER LAYER  │───>│    GOLD LAYER    │
│   (Raw Data)   │    │   (Cleansed)   │    │ (Business-Ready) │
└────────────────┘    └────────────────┘    └──────────────────┘
```

| Dataset Name | Resource ID | Row Count | Attributes | Est. Volume | Load Strategy & Behavior |
| :--- | :--- | :--- | :--- | :--- | :--- |
| Health Deficiencies | r5ix-sfxw | 419,479 | 23 cols | 150-200 MB | Incremental: 2k-3k rows (1-2 MB)/month. Main fact table for inspection citations. |
| Penalties | g6vv-u9sr | 15,419 | 14 cols | 3-5 MB | Incremental: 100-150 rows (<0.1 MB)/month. Second fact table for fines/denials. |

**Total Estimated Volume:** ~450,000 rows across initial loads (175-230 MB baseline, <5 MB incremental/month).
Compressed storage size will remain under 1 GB.

## Ingestion & Processing Strategy
*   **Full Load:** Initial baseline download executed via bulk CSV extraction.
*   **Incremental Load:** Employs date-based API filtering (survey_date/penalty_date), a 60-day safety overlap window, and row hashing (Delta MERGE) to capture updates and soft deletes.
*   **Security & PII Handling:** Excludes the CMS Ownership dataset entirely to mitigate PII exposure. Drops telephone_number attributes and hashes street addresses using SHA-256 combined with a secure salt in the Silver layer.

## Medallion Data Modeling

```text
| BRONZE LAYER |  > | SILVER LAYER |  > | GOLD LAYER |
| (Raw Data)   |    | (Cleansed)   |    | (Business-Ready) |
```

### Bronze Layer (Raw)
Append-only unmodified copy of each API payload. Every column is ingested strictly as a string to eliminate ingestion failures, augmented with operational metadata (`_ingest_ts`, `_load_type`).

### Silver Layer (Cleansed)
Standardizes naming conventions and data types, normalizes categorical indicators (e.g., categorizing severity codes A-L into standardized risk tiers), applies PII masking, merges records, and routes non-conforming rows to a dedicated `silver_quarantine` table.

[Page 3]

### Gold Layer (Business-Ready)
Star schema optimized for analytical query workloads and BI engine consumption.
*   **Dimensions:** `dim_facility`, `dim_geography`, `dim_date`, `dim_deficiency`
*   **Facts:** `fact_deficiency`, `fact_penalty`, `fact_facility_snapshot`
*   **Aggregates:** `agg_state_quality`, `agg_ownership_compare`, `agg_facility_risk_scorecard`, `agg_deficiency_trend`

## Business Intelligence & Dashboards
**Primary Tool:** Power BI (Fallback: Tableau Public)

**Core Dashboard Visualizations**
*   **Geographic Risk Map:** Filled map showing serious citation rates aggregated by state.
*   **Ownership Performance:** Clustered bar chart and KPI metrics comparing average star ratings, staffing hours, and fine amounts per bed across ownership models.
*   **Compliance Correlation:** Scatter plot mapping direct staffing hours against severity-weighted compliance scores.
*   **Trend Analysis:** Multi-line timeline charting historical compliance fluctuations and citation category mix over time.

## Infrastructure & FinOps Setup
*   **Version Control:** GitHub repository. Capped sample subsets for testing are stored in `data/samples`.
*   **Compute Environment:** Databricks Free Edition.
*   **Cost & Performance Controls:**
    *   Execute tests against capped local sample files to prevent compute quota exhaustion.
    *   Avoid high-cost Spark driver actions (e.g., explicit `.collect()` calls).
    *   Optimize shuffle partitions relative to cluster core counts.
    *   Leverage Parquet/Delta Snappy compression for minimal storage footprint.
    *   Direct BI tools exclusively to pre-aggregated Gold layer tables.

## Technical Review & Recommendations

**1. Databricks Free Tier Constraints**
Free tier environments enforce compute restrictions and lack native job scheduling. Managing an automated pipeline without dedicated instance pools may introduce operational overhead or require external orchestration (e.g., GitHub Actions).

**2. Omission of Ownership Data**
Fully excluding the Ownership dataset to bypass PII risks eliminates systemic analysis capabilities.

[Page 4]

Multi-facility operators often standardise practices across subsidiary entities; retaining anonymized ownership IDs would preserve analytical depth.

**3. Schema Drift Vulnerability**
CMS endpoints frequently modify attribute labels or alter API responses without advance notice. The ingestion stage should incorporate explicit schema validation contracts prior to Silver processing to prevent silent downstream failures.

**4. Change Data Capture Efficiency**
Full-row hashing across non-key attributes introduces noticeable compute overhead as state size increases. Restricting row-hash checks to stateful columns or leveraging native Delta change data feeds will better optimize execution times within restricted compute boundaries.