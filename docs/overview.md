# Pakistan Economy Data Warehouse

## 1. Purpose

The Pakistan Economy Data Warehouse is an academic lakehouse that turns eight official, differently structured economic-data feeds into reproducible analytical datasets. It is implemented with PySpark and Delta Lake on Databricks Free Edition (the successor to the legacy Community Edition experience) and follows a Staging → Bronze → Silver → Gold medallion architecture.

The project is designed to answer one central question: **how can official macroeconomic releases be made consistent, traceable, revision-aware, and ready for analysis without losing the source evidence?**

The warehouse preserves each downloaded file, applies explicit PySpark schemas, captures record-level lineage, quarantines invalid records, and uses Delta `MERGE` operations so that a rerun or historical backfill does not create duplicates. Curated Gold tables are shaped for Power BI and for notebook-based exploratory analysis.

## 2. Target stakeholders

| Stakeholder | Primary need | Warehouse outcome |
|---|---|---|
| Financial analysts | Compare exchange rates, external flows, inflation, and fuel prices over time | Consistent dates, units, dimensions, and Power BI-ready facts |
| Macroeconomic researchers | Reproduce results and distinguish original releases from later revisions | Immutable source snapshots, version hashes, lineage, and revision history |
| Policy and public-sector analysts | Monitor high-frequency price pressure and external-sector movements | Weekly/daily indicators linked to monthly macro series |
| Data engineering students and instructors | Assess schema enforcement, data quality, idempotency, and orchestration | Explicit contracts, Delta `MERGE`, quarantine, and operational logs |
| BI developers | Build drill-down dashboards without parsing source publications | Star-schema Gold layer with conformed date, geography, commodity, sector, and series dimensions |
| Data stewards | Explain where every number came from and whether it passed validation | Row-level source metadata, validation status, and run-level audit metrics |

## 3. Business problems solved

1. **Fragmented publication formats.** The sources mix long/wide CSV, multi-sheet XLSX, born-digital PDF, and scanned PDF. The warehouse supplies a common ingestion and validation contract.
2. **Inconsistent time grains.** Daily exchange rates, weekly SPI, monthly external-sector and CPI data, and ad-hoc fuel notifications are represented with explicit observation/effective dates and a conformed calendar.
3. **Revisions and silent replacement.** Government publishers can revise a period or replace a file at the same URL. The warehouse retains source bytes, SHA-256 hashes, ingestion batches, and effective record versions.
4. **Duplicate results after reruns.** Deterministic business keys and Delta `MERGE` make normal runs and backfills idempotent.
5. **Unclear units and aggregation traps.** Units are attached to each series and converted only under governed rules. Hierarchical SBP series are flagged so totals are not recomputed by summing totals and children together.
6. **Weak auditability.** Every row carries its source and run lineage; every pipeline execution records parameters, timing, status, and insert/update/reject counts.
7. **Schema drift.** Header, sheet, series-catalog, and PDF-layout changes are detected before publication. Unexpected structures are routed to Quarantine with the original payload and reason.
8. **Slow analytics preparation.** Silver contracts standardize names, types, dates, units, null semantics, and keys; Gold facts and dimensions can therefore be consumed directly by Power BI.

## 4. Source inventory

Official landing pages are used below instead of release-specific download links because PBS and OGRA file URLs change by release. A discovery step must resolve and record the exact file URL for every batch.

| # | Source family / configured identifier | Publisher | Publication frequency | Input format | Official source URL | Key business grain |
|---:|---|---|---|---|---|---|
| 1 | Country-wise Workers' Remittances (`TS_GP_BOP_WR_M`) | State Bank of Pakistan (SBP) | Monthly | CSV from EasyData | [SBP EasyData dataset catalogue](https://easydata.sbp.org.pk/apex/f?p=10:210) | One remittance series/country-or-aggregate × month, normally in USD millions |
| 2 | FDI by sector, ISIC Rev. 4 (`TS_GP_BOP_FDIISIC4_M`) | SBP | Monthly | CSV from EasyData | [SBP EasyData](https://easydata.sbp.org.pk/) and [SBP economic data](https://www.sbp.org.pk/economic-data) | One ISIC sector × flow component (inflow, outflow, or net) × month |
| 3 | Bank Floating Daily Average Exchange Rates (`TS_GP_ES_FADERPKR_M`) | SBP | Daily observations; release page commonly refreshed monthly | CSV from EasyData | [SBP EasyData dataset catalogue](https://easydata.sbp.org.pk/apex/f?p=10:210) and [SBP economic data](https://www.sbp.org.pk/economic-data) | One foreign currency/unit × business date, expressed as PKR per foreign-currency unit |
| 4 | Export Receipts by all Commodities—HS2 level (`TS_GP_BOP_XRECCOM_M`) | SBP | Monthly | CSV from EasyData | [SBP EasyData dataset catalogue](https://easydata.sbp.org.pk/apex/f?p=10:210) | One published HS2 commodity/group series × month, in thousand USD |
| 5 | Import Payments by all Commodities—HS2 level (`TS_GP_BOP_MRECCOM_M`) | SBP | Monthly | CSV from EasyData | [SBP EasyData dataset catalogue](https://easydata.sbp.org.pk/apex/f?p=10:210) | One published HS2 commodity/group series × month, in thousand USD |
| 6 | Sensitive Price Indicator (SPI) | Pakistan Bureau of Statistics (PBS) | Weekly | XLSX; report and annexure workbooks | [PBS Price Statistics](https://www.pbs.gov.pk/price-statistics/) | One week-ending date × SPI record type × expenditure group/item/geography |
| 7 | Consumer Price Index (CPI) / Monthly Review on Price Indices | PBS | Monthly | PDF, usually text-based but layout-driven | [PBS Price Statistics](https://www.pbs.gov.pk/price-statistics/) | One month × national/urban/rural domain × CPI group or headline measure |
| 8 | Notified petroleum/fuel prices, including kerosene | Oil and Gas Regulatory Authority (OGRA) | Ad hoc/effective-date driven | PDF notification; some historical files are scanned | [OGRA Notified Petroleum Prices](https://price.ogra.org.pk/?section=oil-notified&ui=en) | One notification revision × effective date × product × price/component |

### 4.1 Source-contract note

The identifiers in this document are confirmed by the project's inspected long-format CSV samples. Treat every SBP dataset code as an opaque identifier: the daily FX dataset legitimately uses `TS_GP_ES_FADERPKR_M`, so its `_M` suffix must not be used to infer frequency. SBP also publishes similarly named but distinct monthly average FX datasets. Likewise, `TS_GP_BOP_MRECCOM_M` (all commodities—HS2 level) is distinct from commodity-group datasets such as `MRECCG`; they must not be substituted merely because their titles look similar.

The pipeline must therefore not infer identity from a filename. Before ingestion it must validate:

- configured dataset code and expected dataset title;
- publisher, frequency, unit, and available date range;
- series catalogue checksum and column/sheet signature;
- resolved download URL and HTTP retrieval timestamp.

A mismatch is a `SOURCE_CONTRACT_MISMATCH` quarantine event and requires a deliberate configuration change. It must never be auto-corrected in a production/backfill run.

## 5. Scope and analytical outcomes

### In scope for Phase 2

- parameterized acquisition or loading of already-downloaded source files;
- immutable file registration and SHA-256 deduplication;
- strict schema-on-read using `StructType` / `StructField` and `inferSchema=False`;
- Bronze persistence with technical metadata;
- Silver parsing, standardization, validation, deduplication, and Delta `MERGE`;
- quarantine of bad files and bad rows;
- revision-aware history for corrected official observations;
- operational execution logging;
- conformed Gold facts/dimensions suitable for Power BI;
- documented, repeatable backfills by `batch_id`, observation date/range, or folder path.

### Out of scope for Phase 2

- forecasting, causal inference, or automated policy recommendations;
- treating OCR output as authoritative without validation;
- scraping private, paywalled, or non-official substitutes when an official release is unavailable;
- real-time streaming guarantees;
- enterprise-scale identity, networking, disaster recovery, or paid-workspace service-level objectives.

## 6. Proposed Gold model

The final physical names may be refined during implementation, but the analytical contract is:

- `fact_external_flow_monthly`: remittances, FDI, export receipts, and import payments at their lowest safe published grain;
- `fact_exchange_rate_daily`: PKR rates by currency and date;
- `fact_price_index`: weekly SPI and monthly CPI observations with explicit index family, domain, and base year;
- `fact_fuel_price`: OGRA product prices/components by effective period and notification revision;
- `dim_date`, `dim_series`, `dim_country`, `dim_currency`, `dim_sector`, `dim_commodity`, `dim_price_group`, and `dim_product`;
- optional aggregate views such as `vw_macro_monthly`, generated from facts rather than stored as untraceable extracts.

Gold never hides source units, provisional/revised status, or hierarchy level. Measures that cannot safely be added are marked non-additive in the semantic model.

## 7. Success criteria

Phase 2 is complete when all of the following are demonstrable:

- every source family has an explicit, version-controlled schema;
- every accepted record has `load_timestamp`, `batch_id`, source-file lineage, and a deterministic hash;
- running the same batch twice produces zero additional current Silver records;
- an official revision updates or versions the affected key without destroying the prior evidence;
- malformed rows and unexpected schema versions appear in Quarantine with actionable reason codes;
- `pipeline_execution_logs` reconciles read, accepted, inserted, updated, unchanged, and quarantined counts;
- a clean environment can run one incremental load and one parameterized historical backfill;
- Power BI can query documented Gold tables without parsing source-specific columns.

## 8. Authoritative references

- [SBP EasyData](https://easydata.sbp.org.pk/)
- [SBP Economic Data](https://www.sbp.org.pk/economic-data)
- [PBS Price Statistics](https://www.pbs.gov.pk/price-statistics/)
- [OGRA Fuel Pricing & Price Publication Portal](https://price.ogra.org.pk/?ui=en)
- [Databricks guidance for DBFS and Unity Catalog](https://docs.databricks.com/aws/en/dbfs/unity-catalog)

Source locations and catalogue details were checked on **2026-10-09**. Exact release URLs, hashes, and retrieval timestamps belong in run metadata, not hard-coded documentation.
