# Projected Data Models

## 1. Modelling principles

This specification defines the objects required to build the Pakistan economic dashboard. Databricks Delta tables are the system of record; Power BI connects only to approved Gold views/tables.

- Monetary values retain their published unit. Unit conversion, if needed for a visual, happens in an explicitly labelled Gold view.
- A source snapshot is immutable. A revised value updates the current Silver record only when its source snapshot is newer; the prior value remains in the revision audit.
- `source_file_hash`, `batch_id`, and `source_snapshot_at` are mandatory lineage fields on all Bronze/Silver facts.
- `decimal(24,6)` is the default numeric measure type unless a source needs greater precision.
- Timestamps are stored in UTC. Dates are stored as `date`.

## 2. Source catalogue

| Source ID | Dataset source name | Official link | Source type | Required acquisition method |
| --- | --- | --- | --- | --- |
| `SBP_REMITTANCES` | SBP Country-wise Workers' Remittances (`TS_GP_BOP_WR_M`) | [SBP EasyData](https://easydata.sbp.org.pk/) | CSV; future API/download endpoint to be verified | Export all series in Long Format; monthly lookback of 12 months. |
| `SBP_FDI` | SBP Foreign Direct Investment by Sector (`TS_GP_BOP_FDIISIC4_M`) | [SBP EasyData](https://easydata.sbp.org.pk/) | CSV; future API/download endpoint to be verified | Export all series in Long Format; monthly lookback of 12 months. |
| `SBP_FX` | SBP Bank Floating Daily Average Exchange Rates (`TS_GP_ES_FADERPKR_M`) | [SBP EasyData](https://easydata.sbp.org.pk/) | CSV; future API/download endpoint to be verified | Export all series in Long Format; daily lookback of 90 calendar days. |
| `SBP_EXPORTS` | SBP Export Receipts by all Commodities—HS2 level (`TS_GP_BOP_XRECCOM_M`) | [SBP EasyData](https://easydata.sbp.org.pk/) | CSV; future API/download endpoint to be verified | Export all series in Long Format; monthly lookback of 12 months. |
| `SBP_IMPORTS` | SBP Import Payments by all Commodities—HS2 level (`TS_GP_BOP_MRECCOM_M`) | [SBP EasyData](https://easydata.sbp.org.pk/) | CSV; future API/download endpoint to be verified | Export all series in Long Format; monthly lookback of 12 months. |
| `PBS_SPI` | PBS Weekly Sensitive Price Indicator | [PBS Price Statistics](https://www.pbs.gov.pk/price-statistics/) | Excel (`.xlsx`) | Discover weekly release pages and download the linked Report and Annexure workbooks. |
| `PBS_CPI` | PBS Monthly Review on Price Indices / CPI | [PBS Price Statistics](https://www.pbs.gov.pk/price-statistics/) | PDF | Discover each monthly report and extract Tables 1–3 plus Annexures A–B. |
| `OGRA_FUEL` | OGRA notified petroleum prices | [OGRA Petroleum Price Notifications](https://www.ogra.org.pk/index.php/notified-petroleum-prices) | Scanned PDF | Discover notification links, retain the PDF, apply OCR, and route uncertain rows to review. |

`API` is deliberately not listed as an active source type: no stable SBP or PBS API/download endpoint has yet been verified. Once verified, the source catalogue may add an endpoint and authentication/parameter configuration without changing the business model.

## 3. Cross-source operational objects

### 3.1 `ops.raw_file_manifest`

One row per acquired source file; this is the authoritative record of staging objects.

| Column | Delta type | Dataset source name / link | Why it is required |
| --- | --- | --- | --- |
| `manifest_id` | `string` | All sources in Section 2 | Unique manifest identifier. |
| `batch_id` | `string` | All sources | Groups files processed together. |
| `source_id` | `string` | All sources | Joins to the source catalogue. |
| `dataset_code` | `string` | SBP datasets / [SBP EasyData](https://easydata.sbp.org.pk/) | Retains an SBP dataset identifier; null for sources without one. |
| `landing_page_url` | `string` | All sources | Official page from which the file was discovered. |
| `download_url` | `string` | All sources | Exact URL used to retrieve the snapshot. |
| `requested_period_start` | `date` | SBP and scheduled sources | Start date supplied to the extraction request, when relevant. |
| `requested_period_end` | `date` | SBP and scheduled sources | End date supplied to the extraction request, when relevant. |
| `source_file_name` | `string` | All sources | Original publisher filename. |
| `staging_path` | `string` | All sources | Immutable Databricks Volume/object-storage path. |
| `file_format` | `string` | CSV, Excel, PDF | `csv`, `xlsx`, or `pdf`. |
| `file_size_bytes` | `bigint` | All sources | Source-file volume and acquisition check. |
| `sha256_hash` | `string` | All sources | Identifies identical or replaced files. |
| `retrieved_at_utc` | `timestamp` | All sources | Actual acquisition time. |
| `release_date` | `date` | PBS and OGRA; null when unavailable | Publication/notification date. |
| `reference_period_start` | `date` | All sources where published | Beginning of period represented by the file. |
| `reference_period_end` | `date` | All sources where published | End of period represented by the file. |
| `ingestion_status` | `string` | All sources | `LANDED`, `SKIPPED_IDENTICAL`, `PARSED`, `FAILED`, or `QUARANTINED`. |
| `error_message` | `string` | All sources | Failure diagnostic, if any. |
| `created_at_utc` | `timestamp` | All sources | Manifest audit timestamp. |

### 3.2 `ops.observation_revision_audit`

One row for each changed current observation, enabling an analyst to explain historical restatements.

| Column | Delta type | Dataset source name / link | Why it is required |
| --- | --- | --- | --- |
| `revision_audit_id` | `string` | All sources | Unique revision event. |
| `target_table` | `string` | All Silver tables | Table whose current record changed. |
| `business_key_json` | `string` | All sources | Serialized natural key of the revised observation. |
| `old_record_hash` | `string` | All sources | Fingerprint before revision. |
| `new_record_hash` | `string` | All sources | Fingerprint after revision. |
| `old_values_json` | `string` | All sources | Previous values for audit. |
| `new_values_json` | `string` | All sources | Accepted new values. |
| `old_source_file_hash` | `string` | All sources | Previous raw snapshot. |
| `new_source_file_hash` | `string` | All sources | Revising raw snapshot. |
| `changed_at_utc` | `timestamp` | All sources | Audit timestamp. |

## 4. Bronze source models

### 4.1 `bronze.sbp_economic_series_raw`

**Source type:** CSV. **Applies to:** `SBP_REMITTANCES`, `SBP_FDI`, `SBP_FX`, `SBP_EXPORTS`, and `SBP_IMPORTS` from [SBP EasyData](https://easydata.sbp.org.pk/).

| Column | Delta type | Source column / derivation |
| --- | --- | --- |
| `source_id` | `string` | Pipeline-assigned source ID. |
| `dataset_name_raw` | `string` | `Dataset Name`. |
| `observation_date_raw` | `string` | `Observation Date`; preserve before parsing. |
| `series_key` | `string` | `Series Key`. |
| `series_display_name_raw` | `string` | `Series Display Name`. |
| `observation_value_raw` | `string` | `Observation Value`; preserve blank/missing representation. |
| `unit_raw` | `string` | `Unit`. |
| `observation_status_raw` | `string` | `Observation Status`. |
| `observation_status_comment_raw` | `string` | `Observation Status Comment`. |
| `sequence_no_raw` | `string` | `Sequence No.`; not assumed to be an HS2 code. |
| `series_name_raw` | `string` | `Series name`. |
| `record_number` | `bigint` | CSV row number within the source file. |
| `batch_id` | `string` | Pipeline metadata. |
| `source_file_path` | `string` | Manifest staging path. |
| `source_file_hash` | `string` | Manifest SHA-256. |
| `retrieved_at_utc` | `timestamp` | Manifest retrieval timestamp. |
| `ingested_at_utc` | `timestamp` | Bronze write timestamp. |
| `rescued_data` | `string` | Parsing rescue payload, if present. |

### 4.2 `bronze.pbs_spi_raw`

**Source type:** Excel. **Source:** `PBS_SPI` from [PBS Price Statistics](https://www.pbs.gov.pk/price-statistics/). One row represents an extracted source row/cell group; source layouts must be versioned by workbook and sheet.

| Column | Delta type | Source column / derivation |
| --- | --- | --- |
| `workbook_kind` | `string` | `REPORT` or `ANNEXURE`. |
| `sheet_name` | `string` | Excel worksheet name. |
| `source_row_number` | `int` | Original worksheet row number. |
| `source_table_name` | `string` | Identified table/appendix name. |
| `item_code_raw` | `string` | Published item identifier, when supplied. |
| `item_name_raw` | `string` | Published item description. |
| `location_raw` | `string` | City, national, urban, rural, or aggregate label. |
| `unit_raw` | `string` | Published unit. |
| `price_type_raw` | `string` | Minimum, average, maximum, index, or comparison measure. |
| `value_raw` | `string` | Unparsed numeric cell value. |
| `reference_date_raw` | `string` | Reported week/date before parsing. |
| `batch_id` | `string` | Pipeline metadata. |
| `source_file_hash` | `string` | Manifest SHA-256. |
| `source_file_path` | `string` | Staging path. |
| `retrieved_at_utc` | `timestamp` | Manifest retrieval timestamp. |

### 4.3 `bronze.pbs_cpi_raw`

**Source type:** PDF. **Source:** `PBS_CPI` from [PBS Price Statistics](https://www.pbs.gov.pk/price-statistics/).

| Column | Delta type | Source column / derivation |
| --- | --- | --- |
| `source_page_number` | `int` | PDF page number. |
| `source_table_name` | `string` | Table 1–3, Annexure A, or Annexure B. |
| `source_row_number` | `int` | Extracted table row. |
| `coverage_raw` | `string` | National, urban, or rural. |
| `item_or_group_raw` | `string` | Published CPI group/item label. |
| `item_code_raw` | `string` | Published identifier where available. |
| `base_year_raw` | `string` | Published base-year text. |
| `observation_month_raw` | `string` | Month text before parsing. |
| `index_value_raw` | `string` | Extracted CPI index text. |
| `weight_raw` | `string` | Extracted weight text. |
| `mom_change_pct_raw` | `string` | Month-on-month percentage text. |
| `yoy_change_pct_raw` | `string` | Year-on-year percentage text. |
| `contribution_pp_raw` | `string` | Percentage-point contribution text, if published. |
| `extraction_confidence` | `decimal(5,4)` | Table/OCR extraction confidence where available. |
| `batch_id` | `string` | Pipeline metadata. |
| `source_file_hash` | `string` | Manifest SHA-256. |
| `source_file_path` | `string` | Staging path. |

### 4.4 `bronze.ogra_fuel_raw`

**Source type:** scanned PDF + OCR. **Source:** `OGRA_FUEL` from [OGRA Petroleum Price Notifications](https://www.ogra.org.pk/index.php/notified-petroleum-prices).

| Column | Delta type | Source column / derivation |
| --- | --- | --- |
| `source_page_number` | `int` | PDF page number. |
| `source_row_number` | `int` | Extracted/OCR table row. |
| `product_name_raw` | `string` | Published product label. |
| `notification_date_raw` | `string` | Notification date text. |
| `effective_date_raw` | `string` | Effective date text. |
| `geographic_applicability_raw` | `string` | Applicability note/area. |
| `price_basis_raw` | `string` | Prescribed price, maximum ex-depot price, etc. |
| `prescribed_price_raw` | `string` | Extracted source value. |
| `petroleum_levy_raw` | `string` | Extracted source value. |
| `ifem_raw` | `string` | Inland freight equalization margin. |
| `dealer_commission_raw` | `string` | Extracted source value. |
| `distributor_margin_raw` | `string` | Extracted source value. |
| `gst_raw` | `string` | General sales tax source value. |
| `maximum_ex_depot_price_raw` | `string` | Extracted source value. |
| `unit_raw` | `string` | Published price unit. |
| `ocr_confidence` | `decimal(5,4)` | OCR confidence, when available. |
| `batch_id` | `string` | Pipeline metadata. |
| `source_file_hash` | `string` | Manifest SHA-256. |

## 5. Shared dimensions

### 5.1 `gold.dim_date`

This conformed date dimension is generated by the warehouse, not downloaded from a source.

| Column | Delta type | Dataset source name / link |
| --- | --- | --- |
| `date_key` | `int` | Derived as `yyyyMMdd`. |
| `calendar_date` | `date` | Derived calendar date. |
| `year` | `smallint` | Derived. |
| `quarter_number` | `tinyint` | Derived. |
| `month_number` | `tinyint` | Derived. |
| `month_name` | `string` | Derived. |
| `month_start_date` | `date` | Derived. |
| `month_end_date` | `date` | Derived. |
| `week_start_date` | `date` | Derived. |
| `week_end_date` | `date` | Derived. |

### 5.2 `gold.dim_series`

Maps published SBP/PBS series to dashboard-safe labels and aggregation rules. Mappings are governed reference data, not inferred from `Sequence No.`.

| Column | Delta type | Dataset source name / link |
| --- | --- | --- |
| `series_sk` | `bigint` | Warehouse-generated surrogate key. |
| `source_id` | `string` | Source catalogue in Section 2. |
| `series_key` | `string` | SBP `Series Key`; nullable for non-SBP items. |
| `published_series_name` | `string` | SBP/PBS published label. |
| `dashboard_series_name` | `string` | Reviewed reporting label. |
| `series_category` | `string` | Currency, geography, FDI sector, trade commodity, CPI group, or SPI item. |
| `flow_type` | `string` | `NET_FLOW`, `INFLOW`, `OUTFLOW`, or null. |
| `aggregation_level` | `string` | `TOTAL`, `PARENT`, `COMPONENT`, `DETAIL`, or `UNKNOWN`. |
| `is_additive` | `boolean` | Prevents unsafe summation in dashboard measures. |
| `valid_from_date` | `date` | Mapping validity start. |
| `valid_to_date` | `date` | Mapping validity end; null if current. |

## 6. Dashboard fact models

### 6.1 `gold.fact_exchange_rate_daily`

**Source:** `SBP_FX` — [SBP EasyData](https://easydata.sbp.org.pk/). **Type:** CSV. **Grain:** one row per published observation date and currency/rate series.

| Column | Delta type | Source field / derivation |
| --- | --- | --- |
| `observation_date` | `date` | Parsed `Observation Date`. |
| `date_key` | `int` | Join to `dim_date`. |
| `series_sk` | `bigint` | Join through `series_key`. |
| `currency_code` | `string` | Reviewed `Series Key` mapping. |
| `currency_name` | `string` | Reviewed `Series Display Name` mapping. |
| `rate_type` | `string` | Reviewed series mapping. |
| `quote_currency_code` | `string` | Constant `PKR`. |
| `exchange_rate_pkr_per_unit` | `decimal(24,6)` | Parsed `Observation Value`. |
| `unit` | `string` | `Unit`. |
| `observation_status` | `string` | `Observation Status`. |
| `observation_status_comment` | `string` | `Observation Status Comment`. |
| `record_hash` | `string` | Normalized record fingerprint. |
| `source_file_hash` | `string` | Manifest lineage. |
| `source_snapshot_at` | `timestamp` | Source retrieval timestamp. |

### 6.2 `gold.fact_remittance_monthly`

**Source:** `SBP_REMITTANCES` — [SBP EasyData](https://easydata.sbp.org.pk/). **Type:** CSV. **Grain:** one row per reporting month and published remittance series.

| Column | Delta type | Source field / derivation |
| --- | --- | --- |
| `reporting_month` | `date` | Month-end parsed from `Observation Date`. |
| `date_key` | `int` | Join to `dim_date`. |
| `series_sk` | `bigint` | Join to `dim_series`. |
| `geography_name` | `string` | Reviewed series mapping. |
| `geography_level` | `string` | Country, region, or total from mapping. |
| `remittance_value_million_usd` | `decimal(24,6)` | Parsed `Observation Value`. |
| `unit` | `string` | Published unit; expected Million USD. |
| `observation_status` | `string` | Source status. |
| `is_published_total` | `boolean` | Mapping safeguard against double counting. |
| `record_hash` | `string` | Normalized record fingerprint. |
| `source_file_hash` | `string` | Manifest lineage. |
| `source_snapshot_at` | `timestamp` | Source retrieval timestamp. |

### 6.3 `gold.fact_fdi_monthly`

**Source:** `SBP_FDI` — [SBP EasyData](https://easydata.sbp.org.pk/). **Type:** CSV. **Grain:** one row per reporting month, published sector series, and flow type.

| Column | Delta type | Source field / derivation |
| --- | --- | --- |
| `reporting_month` | `date` | Month-end parsed from `Observation Date`. |
| `date_key` | `int` | Join to `dim_date`. |
| `series_sk` | `bigint` | Join to `dim_series`. |
| `sector_name` | `string` | Reviewed series mapping. |
| `flow_type` | `string` | `NET_FLOW`, `INFLOW`, or `OUTFLOW` from mapping. |
| `aggregation_level` | `string` | Total, sector, or component from mapping. |
| `fdi_value_million_usd` | `decimal(24,6)` | Parsed `Observation Value`. |
| `unit` | `string` | Published unit; expected Million USD. |
| `observation_status` | `string` | Source status. |
| `record_hash` | `string` | Normalized record fingerprint. |
| `source_file_hash` | `string` | Manifest lineage. |
| `source_snapshot_at` | `timestamp` | Source retrieval timestamp. |

### 6.4 `gold.fact_trade_monthly`

**Sources:** `SBP_EXPORTS` and `SBP_IMPORTS` — [SBP EasyData](https://easydata.sbp.org.pk/). **Type:** CSV. **Grain:** one row per reporting month, flow direction, and published commodity series.

| Column | Delta type | Source field / derivation |
| --- | --- | --- |
| `reporting_month` | `date` | Month-end parsed from `Observation Date`. |
| `date_key` | `int` | Join to `dim_date`. |
| `series_sk` | `bigint` | Join to `dim_series`. |
| `trade_flow` | `string` | `EXPORT_RECEIPTS` or `IMPORT_PAYMENTS` based on source ID. |
| `commodity_name` | `string` | Reviewed `Series Display Name` mapping. |
| `commodity_code` | `string` | Validated mapping only; null until confirmed. |
| `aggregation_level` | `string` | Parent/component/total mapping. |
| `trade_value_thousand_usd` | `decimal(24,6)` | Parsed `Observation Value`. |
| `unit` | `string` | Published unit; expected Thousand USD. |
| `observation_status` | `string` | Source status. |
| `record_hash` | `string` | Normalized record fingerprint. |
| `source_file_hash` | `string` | Manifest lineage. |
| `source_snapshot_at` | `timestamp` | Source retrieval timestamp. |

### 6.5 `gold.fact_spi_item_price_weekly`

**Source:** `PBS_SPI` — [PBS Price Statistics](https://www.pbs.gov.pk/price-statistics/). **Type:** Excel. **Grain:** one row per reference week, item, location, unit, and price type.

| Column | Delta type | Source field / derivation |
| --- | --- | --- |
| `reference_week_end_date` | `date` | Parsed workbook reference date. |
| `date_key` | `int` | Join to `dim_date`. |
| `item_code` | `string` | Published item identifier, if present. |
| `item_name` | `string` | Standardized published item label. |
| `location_name` | `string` | City/national/urban/rural label. |
| `unit` | `string` | Published unit. |
| `price_type` | `string` | Minimum, average, maximum, or other published measure. |
| `price_value` | `decimal(24,6)` | Parsed value. |
| `workbook_kind` | `string` | Report or Annexure. |
| `source_sheet_name` | `string` | Workbook sheet lineage. |
| `record_hash` | `string` | Normalized record fingerprint. |
| `source_file_hash` | `string` | Manifest lineage. |
| `source_snapshot_at` | `timestamp` | Source retrieval timestamp. |

### 6.6 `gold.fact_cpi_monthly`

**Source:** `PBS_CPI` — [PBS Price Statistics](https://www.pbs.gov.pk/price-statistics/). **Type:** PDF. **Grain:** one row per observation month, coverage, base year, series level, and CPI item/group.

| Column | Delta type | Source field / derivation |
| --- | --- | --- |
| `observation_month` | `date` | Parsed reported month. |
| `date_key` | `int` | Join to `dim_date`. |
| `geographic_coverage` | `string` | National, urban, or rural. |
| `base_year` | `string` | Published base year. |
| `series_level` | `string` | Overall, group, or item. |
| `item_or_group_code` | `string` | Published identifier where available. |
| `item_or_group_name` | `string` | Standardized published label. |
| `cpi_index` | `decimal(24,6)` | Parsed index value. |
| `weight` | `decimal(24,6)` | Published weight, if applicable. |
| `mom_change_pct` | `decimal(12,6)` | Parsed month-on-month change. |
| `yoy_change_pct` | `decimal(12,6)` | Parsed year-on-year change. |
| `contribution_pp` | `decimal(12,6)` | Parsed contribution, if published. |
| `source_page_number` | `int` | PDF lineage. |
| `source_table_name` | `string` | PDF table lineage. |
| `record_hash` | `string` | Normalized record fingerprint. |
| `source_file_hash` | `string` | Manifest lineage. |
| `source_snapshot_at` | `timestamp` | Source retrieval timestamp. |

### 6.7 `gold.fact_fuel_price`

**Source:** `OGRA_FUEL` — [OGRA Petroleum Price Notifications](https://www.ogra.org.pk/index.php/notified-petroleum-prices). **Type:** scanned PDF/OCR. **Grain:** one row per product, effective date, price basis, and geographic applicability.

| Column | Delta type | Source field / derivation |
| --- | --- | --- |
| `effective_date` | `date` | Parsed notification effective date. |
| `date_key` | `int` | Join to `dim_date`. |
| `notification_date` | `date` | Parsed notification date. |
| `product_name` | `string` | Standardized product name. |
| `geographic_applicability` | `string` | Published applicability note. |
| `price_basis` | `string` | Prescribed, ex-depot, etc. |
| `price_pkr_per_litre` | `decimal(24,6)` | Primary price for the stated basis. |
| `prescribed_price_pkr_per_litre` | `decimal(24,6)` | Parsed source component. |
| `petroleum_levy_pkr_per_litre` | `decimal(24,6)` | Parsed source component. |
| `ifem_pkr_per_litre` | `decimal(24,6)` | Parsed source component. |
| `dealer_commission_pkr_per_litre` | `decimal(24,6)` | Parsed source component. |
| `distributor_margin_pkr_per_litre` | `decimal(24,6)` | Parsed source component. |
| `gst_pkr_per_litre` | `decimal(24,6)` | Parsed source component. |
| `maximum_ex_depot_price_pkr_per_litre` | `decimal(24,6)` | Parsed source component. |
| `unit` | `string` | Expected `PKR per litre`. |
| `ocr_confidence` | `decimal(5,4)` | Extraction confidence. |
| `record_hash` | `string` | Normalized record fingerprint. |
| `source_file_hash` | `string` | Manifest lineage. |
| `source_snapshot_at` | `timestamp` | Source retrieval timestamp. |

## 7. Power BI semantic-model rules

1. Relate each fact to `gold.dim_date` through `date_key`; use a separate inactive relationship or role-playing date for `notification_date` when needed.
2. Relate SBP facts to `gold.dim_series` through `series_sk`.
3. Default trade and FDI measures must filter to a single validated aggregation level; never sum totals and components together.
4. Default FX measures are averages or last-available values over a selected period, never sums.
5. Default CPI/SPI measures retain their coverage, item/group, and unit filters. Do not add indices across items.
6. Display the source/retrieval timestamp in drill-through or a data-quality page so users can identify data freshness and revisions.
7. Dashboard KPI measures should be built only after each source model passes completeness, uniqueness, unit, and lineage checks.

## 8. Recommended physical implementation

```text
Staging volume: immutable original source files
Bronze tables:  bronze.sbp_economic_series_raw
                bronze.pbs_spi_raw
                bronze.pbs_cpi_raw
                bronze.ogra_fuel_raw
Silver tables:  silver.sbp_remittances, silver.sbp_fdi, silver.sbp_exchange_rates
                silver.sbp_export_receipts, silver.sbp_import_payments
                silver.pbs_spi_prices, silver.pbs_cpi, silver.ogra_fuel_prices
Gold tables:    gold.dim_date, gold.dim_series
                gold.fact_exchange_rate_daily, gold.fact_remittance_monthly
                gold.fact_fdi_monthly, gold.fact_trade_monthly
                gold.fact_spi_item_price_weekly, gold.fact_cpi_monthly
                gold.fact_fuel_price
Operations:     ops.raw_file_manifest, ops.observation_revision_audit
```
