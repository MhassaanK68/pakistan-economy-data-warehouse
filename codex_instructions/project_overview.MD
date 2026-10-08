# Pakistan Economy Data Warehouse — Project Overview

## 1. Purpose

This project will build a Pakistan macroeconomic intelligence platform for investment research. It combines public economic data into a governed, reproducible warehouse that supports analysis of inflation, exchange rates, trade, remittances, foreign direct investment (FDI), and fuel prices.

The end product is intended to supply curated Gold tables to Power BI. It supports research and decision support; it must not generate direct buy or sell recommendations.

The architecture follows the medallion pattern:

```text
Public source -> Staging (immutable source snapshots) -> Bronze (parsed source records)
              -> Silver (validated, standardized, deduplicated records) -> Gold / Power BI
```

The scope in this repository currently covers eight source areas. Raw full-load and incremental samples are already stored under `data/raw/`.

## 2. Data sources and extraction method

| Source area | Publisher / URL | Frequency and format | Extraction approach |
| --- | --- | --- | --- |
| Workers' remittances | [SBP EasyData](https://easydata.sbp.org.pk/), dataset `TS_GP_BOP_WR_M` | Monthly CSV, Long Format, Million USD | Export all series in Long Format. Initial load spans available history; each subsequent run requests new months plus a revision lookback. The reusable download endpoint must be verified before automation. |
| FDI by sector | [SBP EasyData](https://easydata.sbp.org.pk/), dataset `TS_GP_BOP_FDIISIC4_M` | Monthly CSV, Long Format, Million USD | Export all series in Long Format. Preserve net flow, inflow, outflow, sector, and aggregate labels. Re-fetch the previous 12 months each monthly run. |
| Bank floating exchange rates | [SBP EasyData](https://easydata.sbp.org.pk/), dataset `TS_GP_ES_FADERPKR_M` | Daily CSV, Long Format, PKR per foreign-currency unit | Export all currency series in Long Format. Incrementally request new dates and re-fetch the previous 90 calendar days for corrections. Do not infer missing business-day observations. |
| Export receipts by commodity | [SBP EasyData](https://easydata.sbp.org.pk/), dataset `TS_GP_BOP_XRECCOM_M` | Monthly CSV, Long Format, Thousand USD | Export all series in Long Format. Preserve the published hierarchy; parent categories and components must not be summed together. Re-fetch the previous 12 months. |
| Import payments by commodity | [SBP EasyData](https://easydata.sbp.org.pk/), dataset `TS_GP_BOP_MRECCOM_M` | Monthly CSV, Long Format, Thousand USD | Same approach as export receipts. Validate any HS2 mapping before labelling a series as an HS2 code. Re-fetch the previous 12 months. |
| Sensitive Price Indicator (SPI) | [PBS Price Statistics](https://www.pbs.gov.pk/price-statistics/) | Weekly Excel workbooks | Discover release pages from the official listing and follow the actual workbook links—never manufacture filenames. Read the Report and Annexure sheets with explicit schemas; reshape city price columns to records. |
| Consumer Price Index (CPI) | [PBS Price Statistics](https://www.pbs.gov.pk/price-statistics/) | Monthly PDF reports | Discover published PDF links from the listing. Preserve PDFs unchanged, then extract Tables 1–3 and Annexures A–B using a tested table-extraction process. Store source page/table provenance. |
| Kerosene notifications | [OGRA petroleum-price notifications](https://www.ogra.org.pk/index.php/notified-petroleum-prices) | Ad hoc scanned PDF | Discover new notification download links from the listing. Apply OCR before extracting the product-price table; quarantine uncertain OCR output for human review. Keep notification date separate from effective date. |

### Source-specific cautions

- SBP links recorded in earlier documentation include portal session IDs/checksums. Treat them as manual-export evidence, not stable automation endpoints. Validate an official reusable download mechanism before scheduling jobs.
- The FDI, trade, and remittance series contain totals and components. Never sum all series.
- FX values are PKR per unit of foreign currency; rates must not be summed across currencies or dates.
- CPI/SPI reports often contain comparison-period values. Release date and observation date are separate fields.
- OGRA samples are scanned PDFs and initially cover Kerosene Oil only. A maximum ex-depot price is not necessarily a retail pump price.

## 3. Storage and naming conventions

Use Unity Catalog volumes (or an equivalent governed object-storage location) and retain the raw repository files only as local development samples.

```text
/Volumes/<catalog>/<schema>/economy_lake/
  staging/
    source=<source_name>/ingest_date=YYYY-MM-DD/batch_id=<batch_id>/original_file
  checkpoints/
    <source_name>/
  quarantine/
    layer=<bronze|silver>/source=<source_name>/reason=<reason>/
```

Each acquired file must retain its original name and bytes. Its companion manifest record must include at least:

- `batch_id`, `source_name`, `dataset_code` where relevant
- source landing-page URL and direct download URL
- requested date range / export parameters
- retrieval timestamp in UTC
- release date and reference period where available
- original filename, storage path, byte size, and SHA-256 hash
- ingestion status, error details, and source snapshot ordering timestamp

## 4. Full-load technique

1. **Discover and download:** obtain all available historical releases/periods from the authoritative source. For PBS and OGRA, discover current links from the listing page; for SBP, use a verified repeatable export interface.
2. **Land immutable snapshots:** write each original object once into `staging/` with a deterministic batch identifier. Do not overwrite an existing object.
3. **Register the manifest:** calculate SHA-256 and insert a `raw_file_manifest` record. A duplicate hash for the same source is recorded as a no-change acquisition and need not be parsed again.
4. **Parse to Bronze:** read the source snapshot with an explicit source schema, retaining raw values and file-level lineage.
5. **Standardize to Silver:** validate and normalize records, then upsert only accepted current observations into source-specific Silver Delta tables.
6. **Reconcile:** compare counts, coverage dates, duplicate counts, null counts, and control totals against the original source before marking the batch successful.

Full loads should be run one source at a time. This keeps retries isolated and makes the lineage auditable.

## 5. Incremental-load and revision technique

Incremental processing is not append-only because official statistical data may be revised.

### Acquisition window

| Source | Normal incremental window | Revision lookback |
| --- | --- | --- |
| SBP FX | Newly available daily observations | Previous 90 calendar days |
| SBP FDI, remittances, exports, imports | Newly available monthly observations | Previous 12 months |
| PBS SPI | Newly discovered weekly release | Recheck recent release links and overlapping reference periods |
| PBS CPI | Newly discovered monthly release | Recheck recent release links / replaced PDFs |
| OGRA | Newly discovered notification | Recheck recent links for replacement PDFs |

### Record-level merge rules

1. Compare the downloaded file hash with prior snapshots. Identical files can skip parsing.
2. Parse a changed/new snapshot and compute a normalized record hash from business fields and source-status fields.
3. Insert a record when its business key does not exist.
4. Update the current Silver record only when the incoming source snapshot is newer and its normalized values differ.
5. Preserve the prior version in an audit/history table; never destroy the raw snapshot.
6. Do not let an older downloaded or released file overwrite a newer accepted observation.

Proposed business keys:

- SBP CSVs: `series_key + observation_date`
- SPI: `reference_date + item + location + unit + price_type` (exact key depends on the extracted table)
- CPI: `index_family + base_year + geographic_coverage + item_or_group + aggregation_level + observation_month`
- OGRA: `product + effective_date + price_basis + geographic_applicability`

## 6. Databricks implementation plan

### Phase A — Foundation

1. Create a Unity Catalog catalog/schema and external volume(s) for landing, checkpoints, and quarantine data.
2. Create Delta tables: `ops.raw_file_manifest`, `ops.ingestion_runs`, `ops.data_quality_results`, and `ops.observation_revision_audit`.
3. Store secrets and source credentials/configuration in Databricks secret scopes or job parameters; public URLs must not be hard-coded throughout notebooks.
4. Build a shared Python package/notebook module for batch IDs, hashes, metadata, schemas, quality checks, and Delta MERGE helpers.
5. Configure a Databricks Workflow with one task per source and source-specific schedules.

### Phase B — Staging

1. Implement a source adapter for each publisher that returns the original bytes plus acquisition metadata.
2. Land bytes to the staging volume using a unique path such as `source=sbp_fx/ingest_date=2026-10-08/batch_id=<uuid>/dataset.csv`.
3. Calculate SHA-256 from the landed bytes and insert a manifest row transactionally.
4. Mark an already-known hash as `SKIPPED_IDENTICAL`; otherwise mark it `LANDED`.
5. Keep staging immutable. Any corrected upstream file is a new snapshot, not an overwrite.

### Phase C — Bronze Delta tables

Create one Bronze table per source family, for example:

```text
bronze.sbp_economic_series_raw
bronze.pbs_spi_raw
bronze.pbs_cpi_raw
bronze.ogra_fuel_raw
```

For CSV sources, parse the ten exported fields exactly as delivered and add metadata columns:

```text
_batch_id, _source_file_path, _source_file_hash, _retrieved_at_utc,
_source_url, _ingested_at_utc, _record_number, _rescued_data
```

For Excel/PDF sources, Bronze should retain the extracted cell/text/table values and source page/sheet/row references. Do not apply business transformations in Bronze.

Use explicit schemas for controlled parsing. Databricks Auto Loader (`cloudFiles`) is appropriate when files land continuously in cloud object storage; for scheduled API downloads, invoke the parser directly after staging and still use a checkpoint for idempotent retries. Route malformed rows or unreadable documents to quarantine with a failure reason.

### Phase D — Silver Delta tables

Build normalized source-specific tables such as:

```text
silver.sbp_remittances
silver.sbp_fdi
silver.sbp_exchange_rates
silver.sbp_export_receipts
silver.sbp_import_payments
silver.pbs_spi_prices
silver.pbs_cpi
silver.ogra_fuel_prices
```

Silver transformations must:

- parse dates and derive reporting month where appropriate;
- cast measures to fixed-precision decimals and retain original units/statuses;
- normalize labels and map series keys only through reviewed reference mappings;
- preserve nulls for missing values instead of replacing them with zero;
- validate units, keys, row uniqueness, accepted value ranges, and source coverage;
- distinguish totals from components and published aggregate rows from detailed rows;
- retain all provenance required to trace a Silver record back to a raw file;
- write failed validation records to quarantine rather than silently dropping them.

Use Delta `MERGE INTO` for current-state Silver tables. The merge condition is the source business key; the update condition requires a different normalized record hash and a newer source snapshot timestamp. Write before/after details to `ops.observation_revision_audit`.

Illustrative pattern:

```python
from delta.tables import DeltaTable

target = DeltaTable.forName(spark, "silver.sbp_exchange_rates")
(target.alias("t")
 .merge(incoming.alias("s"), "t.series_key = s.series_key AND t.observation_date = s.observation_date")
 .whenMatchedUpdate(
     condition="s.source_snapshot_at > t.source_snapshot_at AND s.record_hash <> t.record_hash",
     set={"observation_value": "s.observation_value", "record_hash": "s.record_hash",
          "source_snapshot_at": "s.source_snapshot_at", "updated_at_utc": "current_timestamp()"})
 .whenNotMatchedInsertAll()
 .execute())
```

## 7. Validation, operations, and delivery sequence

### Minimum data-quality checks

- required column and schema validation;
- file hash and manifest uniqueness;
- duplicate business-key detection;
- valid parseable dates and expected frequency coverage;
- numeric parsing and unit checks;
- null/missing-status reconciliation;
- non-positive or unusual FX-rate flags;
- source-to-Bronze and Bronze-to-Silver row-count reconciliation;
- aggregate/component safeguards to prevent double counting.

### Recommended implementation order

1. Establish governance, storage, manifest, and reusable framework.
2. Implement SBP CSV ingestion for FX first—it has a clear schema and large historical sample.
3. Extend the shared CSV framework to remittances, FDI, exports, and imports.
4. Implement PBS SPI Excel extraction and schema tests.
5. Implement PBS CPI PDF extraction only after verifying table quality on representative reports.
6. Implement OGRA OCR extraction with an explicit human-review quarantine path.
7. Add workflows, alerts, quality dashboards, and runbook documentation.
8. Only after Silver reconciliation is stable, create Gold facts/dimensions and connect Power BI.

## 8. Current status

The repository currently supplies raw samples and detailed design/source documentation. It does not yet contain Databricks notebooks, PySpark jobs, Delta tables, automated download code, or Power BI artifacts. The next practical milestone is a reusable SBP CSV Staging → Bronze → Silver implementation, proven with the exchange-rate dataset, then generalized to the other SBP datasets.
