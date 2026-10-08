# Bronze and Silver Data Dictionary

## 1. Conventions

This dictionary is the Phase 2 physical contract for Bronze and Silver. All Spark types are uppercase for readability; implementations use the equivalent `pyspark.sql.types` class (`StringType`, `DateType`, `DecimalType`, and so on).

### 1.1 Key and nullability notation

| Mark | Meaning |
|---|---|
| `PK` | Physical primary-key contract. Delta Lake does not enforce it; the pipeline does. |
| `BK` | Natural/business-key component used to compute `business_key_hash`. |
| `FK` | Reference to a governed dimension/mapping or upstream record. |
| `—` | Not a key. |
| `No` | Value must be non-null before the record crosses that layer boundary. |
| `Yes` | Null is valid under the stated source/business rule. Empty strings are normalized to null. |
| `Conditional` | Nullability depends on `record_type`, status, or another documented field. |

No numeric source blank, em dash, `..`, `N/A`, or suppressed value is converted to zero. It becomes null with a compatible observation status or is quarantined.

### 1.2 Canonical date and amount rules

- Daily observations use their calendar `observation_date`.
- Weekly SPI uses the publisher's `week_ended_date`.
- Monthly series use the calendar month-end as `period_end_date`; `period_start_date` is the first day of that month.
- Currency amounts use `DECIMAL(24,6)` to avoid binary floating-point changes in hashes and revisions.
- Percentage rates and index values use `DECIMAL(18,6)`.
- Timestamps are UTC. Publisher effective dates remain `DATE`.
- `source_row_number` is 1-based for CSV; XLSX/PDF coordinates are retained in `source_location`.

### 1.3 Common Bronze lineage columns

These columns are physically present in **every Bronze table** and are not repeated in each source-specific table below.

| Column name | Source data type | Target Spark type | Nullable | Key | Description |
|---|---|---|---:|---|---|
| `bronze_record_id` | Derived | `STRING` | No | PK | SHA-256 of dataset code, source file hash, source location, and parser version. |
| `batch_id` | Run parameter | `STRING` | No | — | Stable logical ingestion/backfill batch identifier. |
| `run_id` | Runtime | `STRING` | No | — | Execution identifier in `ops.pipeline_execution_logs`. |
| `source_system` | Configuration | `STRING` | No | — | `SBP`, `PBS`, or `OGRA`. |
| `dataset_code` | Configuration/catalogue | `STRING` | No | — | Governed source code/alias. |
| `source_file_name` | File metadata | `STRING` | No | — | Original filename as obtained. |
| `source_file_path` | Landing metadata | `STRING` | No | — | Immutable Volume/DBFS path to exact source bytes. |
| `source_file_sha256` | Derived from bytes | `STRING` | No | — | Lowercase 64-character SHA-256 digest. |
| `source_url` | Discovery metadata | `STRING` | Yes | — | Exact resolved official URL; null for a controlled manual upload. |
| `source_published_date` | Landing/discovery metadata | `DATE` | Yes | — | Publisher release date when reliably known. |
| `source_location` | Parser metadata | `STRING` | No | — | CSV row/column, XLSX sheet/cell/range, or PDF page/table/line/bounding box. |
| `source_row_number` | Parser metadata | `LONG` | Yes | — | 1-based logical row number where meaningful. |
| `schema_version` | Contract registry | `STRING` | No | — | Explicit source/Bronze schema version used. |
| `parser_version` | Code/configuration | `STRING` | No | — | Version of the extraction/unpivot logic. |
| `raw_payload` | Source row/cells/text | `STRING` | Yes | — | Canonical JSON or bounded raw text retained for diagnosis. |
| `load_timestamp` | Runtime | `TIMESTAMP` | No | — | UTC time the Bronze record was written. |

### 1.4 Common Silver lineage and revision columns

These columns are physically present in **every Silver table** and are not repeated in each source-specific table below.

| Column name | Source data type | Target Spark type | Nullable | Key | Description |
|---|---|---|---:|---|---|
| `business_key_hash` | Derived | `STRING` | No | BK | SHA-256 of the table's canonical business-key fields. Unique among current records. |
| `revision_number` | Derived | `INT` | No | PK | 1-based version number for a business key. Physical key is `(business_key_hash, revision_number)`. |
| `record_hash` | Derived | `STRING` | No | — | SHA-256 of canonical non-key business values; detects no-op versus revision. |
| `is_current` | Derived | `BOOLEAN` | No | — | True for the active version of a business key. |
| `valid_from_timestamp` | Runtime | `TIMESTAMP` | No | — | UTC time this record version became current. |
| `valid_to_timestamp` | Runtime | `TIMESTAMP` | Yes | — | UTC close time; null only for the current version. |
| `bronze_record_id` | Bronze | `STRING` | No | FK | Winning source Bronze record for this version. |
| `batch_id` | Bronze/run | `STRING` | No | — | Logical batch that produced this version. |
| `run_id` | Runtime | `STRING` | No | — | Silver merge execution identifier. |
| `source_system` | Bronze | `STRING` | No | — | `SBP`, `PBS`, or `OGRA`. |
| `dataset_code` | Bronze | `STRING` | No | — | Governed source code/alias. |
| `source_file_path` | Bronze | `STRING` | No | — | Immutable source-file location. |
| `source_file_sha256` | Bronze | `STRING` | No | — | Hash of exact source bytes. |
| `source_location` | Bronze | `STRING` | No | — | Original row/cell/page location. |
| `schema_version` | Bronze/contract | `STRING` | No | — | Source schema version accepted. |
| `parser_version` | Bronze | `STRING` | No | — | Extractor version. |
| `observation_status` | Source/derived | `STRING` | No | — | Canonical `FINAL`, `PROVISIONAL`, `REVISED`, `MISSING`, or `RETRACTED`. |
| `quality_warning_codes` | Quality engine | `ARRAY<STRING>` | No | — | Non-blocking rule codes; empty array when none. |
| `load_timestamp` | Runtime | `TIMESTAMP` | No | — | UTC time this Silver version was written. |

## 2. SBP EasyData Bronze contract

The five SBP CSV families are unpivoted in Staging to one series/period/value row, then written with an explicit common schema. This contract applies to:

- `bronze.sbp_workers_remittances_raw` (`TS_GP_BOP_WR_M`)
- `bronze.sbp_fdi_sector_raw` (`TS_GP_BOP_FDIISIC4_M`)
- `bronze.sbp_fx_daily_raw` (configured `TS_GP_ES_FADERPKR_M`)
- `bronze.sbp_export_receipts_raw` (`TS_GP_BOP_XRECCOM_M`)
- `bronze.sbp_import_payments_raw` (`TS_GP_BOP_MRECCOM_M`)

The common Bronze lineage columns in §1.3 are included in each table.

| Column name | Source data type | Target Spark type | Nullable | Key | Description |
|---|---|---|---:|---|---|
| `source_dataset_code_raw` | Catalogue/configuration | `STRING` | No | — | Dataset identifier resolved for the export; checked against the configured alias. |
| `dataset_name_raw` | CSV `Dataset Name` | `STRING` | No | — | Publisher dataset title. |
| `observation_date_raw` | CSV `Observation Date` | `STRING` | No | — | Original daily/month-end date token before parsing. |
| `series_key_raw` | CSV `Series Key` | `STRING` | No | — | Full SBP series key. |
| `series_display_name_raw` | CSV `Series Display Name` | `STRING` | No | — | Display label, including hierarchy numbering/indentation. |
| `observation_value_raw` | CSV `Observation Value` | `STRING` | Yes | — | Original numeric token. Blank is allowed only with a compatible observation status. |
| `unit_raw` | CSV `Unit` | `STRING` | No | — | Publisher unit exactly as supplied. |
| `observation_status_raw` | CSV `Observation Status` | `STRING` | No | — | Publisher status, such as `Normal` or `Missing value`. |
| `observation_status_comment_raw` | CSV `Observation Status Comment` | `STRING` | Yes | — | Publisher comment; tokens such as `NA` are normalized only in Silver. |
| `sequence_no_raw` | CSV `Sequence No.` | `STRING` | No | — | Publisher display sequence. It is not treated as an HS/ISIC code without a mapping. |
| `series_name_raw` | CSV `Series name` | `STRING` | No | — | Descriptive publisher series name. |
| `frequency_raw` | Catalogue snapshot | `STRING` | No | — | Publisher frequency (`Monthly` or `Daily`) joined during Staging. |
| `catalogue_snapshot_hash` | Derived | `STRING` | No | — | Hash of series metadata used to interpret this row. |

The Spark CSV reader may include a staging-only `_corrupt_record` field. Any populated corrupt record is written to Quarantine and is **not** an accepted Bronze row.

## 3. Workers' remittances

### 3.1 Bronze table

`bronze.sbp_workers_remittances_raw` uses the complete SBP EasyData Bronze contract in §2 plus the common Bronze lineage columns in §1.3. Its physical key is `bronze_record_id`.

### 3.2 Silver table: `silver.sbp_remittance_monthly`

Common Silver columns in §1.4 are included.

**Business grain:** one SBP series/country-or-aggregate × calendar month.  
**Business key:** `(series_key, period_end_date)`.

| Column name | Source data type | Target Spark type | Nullable | Key | Description |
|---|---|---|---:|---|---|
| `series_key` | CSV text | `STRING` | No | BK | Canonical SBP series key. |
| `period_start_date` | Parsed CSV period | `DATE` | No | — | First calendar day of observation month. |
| `period_end_date` | Parsed CSV period | `DATE` | No | BK | Last calendar day of observation month. |
| `country_code` | Reference mapping | `STRING` | Yes | FK | ISO 3166-1 alpha-3 for a single country; null for publisher aggregates. |
| `country_name` | CSV series + mapping | `STRING` | No | — | Canonical country/corridor/aggregate name. |
| `publisher_series_name` | CSV text | `STRING` | No | — | Original SBP series label. |
| `hierarchy_level` | Mapping | `INT` | No | — | `0` total, `1` additive corridor group, `2+` child detail as governed. |
| `parent_series_key` | Mapping | `STRING` | Yes | FK | Parent aggregate series where the catalogue is hierarchical. |
| `is_total` | Derived | `BOOLEAN` | No | — | True for SBP total series. |
| `is_additive_member` | Mapping | `BOOLEAN` | No | — | True only when safe to include in the approved total partition. |
| `amount_usd_million` | CSV numeric text | `DECIMAL(24,6)` | Conditional | — | Remittance inflow in USD millions; null only for valid missing/retracted status. |
| `source_unit` | CSV text | `STRING` | No | — | Original unit label. Must map to million USD. |
| `source_comment` | CSV text | `STRING` | Yes | — | Observation comment/footnote. |

## 4. FDI by sector (ISIC Rev. 4)

### 4.1 Bronze table

`bronze.sbp_fdi_sector_raw` uses the complete SBP EasyData Bronze contract in §2 plus §1.3.

### 4.2 Silver table: `silver.sbp_fdi_sector_monthly`

Common Silver columns in §1.4 are included.

**Business grain:** one SBP series/ISIC sector × flow component × calendar month.  
**Business key:** `(series_key, period_end_date, flow_component)`.

| Column name | Source data type | Target Spark type | Nullable | Key | Description |
|---|---|---|---:|---|---|
| `series_key` | CSV text | `STRING` | No | BK | Canonical SBP series key. |
| `period_start_date` | Parsed CSV period | `DATE` | No | — | First day of month. |
| `period_end_date` | Parsed CSV period | `DATE` | No | BK | Last day of month. |
| `isic_revision` | Catalogue/mapping | `STRING` | No | — | `ISIC4`. |
| `sector_code` | Catalogue/mapping | `STRING` | No | FK | Governed ISIC division/section or publisher aggregate code. |
| `sector_name` | CSV series + mapping | `STRING` | No | — | Canonical sector label. |
| `publisher_series_name` | CSV text | `STRING` | No | — | Original SBP series label. |
| `hierarchy_level` | Mapping | `INT` | No | — | Sector hierarchy depth; total is level 0. |
| `parent_sector_code` | Mapping | `STRING` | Yes | FK | Parent sector/group, if applicable. |
| `flow_component` | Parsed series label | `STRING` | No | BK | Controlled value: `INFLOW`, `OUTFLOW`, or `NET`. |
| `amount_usd_million` | CSV numeric text | `DECIMAL(24,6)` | Conditional | — | FDI component value in USD millions. Net may be negative. |
| `source_unit` | CSV text | `STRING` | No | — | Original unit; must map to million USD. |
| `is_publisher_total` | Derived/mapping | `BOOLEAN` | No | — | Identifies the publisher total row. |
| `source_comment` | CSV text | `STRING` | Yes | — | Observation footnote/comment. |

When inflow, outflow, and net are all present, the validation rule is `net = inflow - outflow` within the configured rounding tolerance.

## 5. Bank Floating Daily Average FX rates

### 5.1 Bronze table

`bronze.sbp_fx_daily_raw` uses the complete SBP EasyData Bronze contract in §2 plus §1.3. The preflight contract must confirm that the resolved dataset is daily even if a release/archive page is refreshed monthly.

### 5.2 Silver table: `silver.sbp_fx_daily`

Common Silver columns in §1.4 are included.

**Business grain:** one currency series × observation date.  
**Business key:** `(series_key, observation_date)`.

| Column name | Source data type | Target Spark type | Nullable | Key | Description |
|---|---|---|---:|---|---|
| `series_key` | CSV text | `STRING` | No | BK | Canonical SBP FX series key. |
| `observation_date` | Parsed CSV date | `DATE` | No | BK | Publisher business date. |
| `currency_code` | Reference mapping | `STRING` | No | FK | ISO 4217 code, for example `USD`. |
| `currency_name` | CSV series + mapping | `STRING` | No | — | Canonical currency name. |
| `publisher_series_name` | CSV text | `STRING` | No | — | Original SBP label. |
| `currency_unit` | Series mapping | `INT` | No | — | Number of foreign-currency units used in quote; normally 1, but explicitly retained. |
| `pkr_per_currency_unit` | CSV numeric text | `DECIMAL(24,8)` | Conditional | — | PKR per `currency_unit`; must be positive when present. |
| `normalized_pkr_per_one_unit` | Derived | `DECIMAL(24,8)` | Conditional | — | `pkr_per_currency_unit / currency_unit`. |
| `source_unit` | CSV text | `STRING` | No | — | Original `PKR per National Currency`-style unit. |
| `source_comment` | CSV text | `STRING` | Yes | — | Observation footnote/comment. |

## 6. Export receipts by commodity

### 6.1 Bronze table

`bronze.sbp_export_receipts_raw` uses the complete SBP EasyData Bronze contract in §2 plus §1.3.

### 6.2 Silver table: `silver.sbp_export_receipts_monthly`

Common Silver columns in §1.4 are included.

**Business grain:** one SBP export commodity/group series × calendar month.  
**Business key:** `(series_key, period_end_date)`.

| Column name | Source data type | Target Spark type | Nullable | Key | Description |
|---|---|---|---:|---|---|
| `series_key` | CSV text | `STRING` | No | BK | Canonical SBP series key. |
| `period_start_date` | Parsed CSV period | `DATE` | No | — | First day of month. |
| `period_end_date` | Parsed CSV period | `DATE` | No | BK | Last day of month. |
| `commodity_code` | Catalogue/mapping | `STRING` | No | FK | Governed publisher commodity code; not assumed to be HS unless explicitly mapped. |
| `commodity_name` | CSV series + mapping | `STRING` | No | — | Canonical commodity label. |
| `commodity_group_code` | Mapping | `STRING` | Yes | FK | Parent commodity-group code. |
| `commodity_group_name` | Mapping | `STRING` | Yes | — | Parent commodity-group label. |
| `publisher_series_name` | CSV text | `STRING` | No | — | Original SBP series label. |
| `hierarchy_level` | Mapping | `INT` | No | — | Commodity hierarchy depth; total is level 0. |
| `is_total` | Derived/mapping | `BOOLEAN` | No | — | True for publisher total/group total as classified. |
| `is_additive_member` | Mapping | `BOOLEAN` | No | — | True only for a non-overlapping approved aggregation set. |
| `amount_usd_thousand` | CSV numeric text | `DECIMAL(24,6)` | Conditional | — | Export receipts in thousand USD. |
| `source_unit` | CSV text | `STRING` | No | — | Original unit; must map to thousand USD. |
| `source_comment` | CSV text | `STRING` | Yes | — | Observation comment/footnote. |

## 7. Import payments by commodity

### 7.1 Bronze table

`bronze.sbp_import_payments_raw` uses the complete SBP EasyData Bronze contract in §2 plus §1.3.

### 7.2 Silver table: `silver.sbp_import_payments_monthly`

Common Silver columns in §1.4 are included.

**Business grain:** one SBP import commodity/group series × calendar month.  
**Business key:** `(series_key, period_end_date)`.

| Column name | Source data type | Target Spark type | Nullable | Key | Description |
|---|---|---|---:|---|---|
| `series_key` | CSV text | `STRING` | No | BK | Canonical SBP series key. |
| `period_start_date` | Parsed CSV period | `DATE` | No | — | First day of month. |
| `period_end_date` | Parsed CSV period | `DATE` | No | BK | Last day of month. |
| `commodity_code` | Catalogue/mapping | `STRING` | No | FK | Governed publisher commodity code; not assumed to be HS unless explicitly mapped. |
| `commodity_name` | CSV series + mapping | `STRING` | No | — | Canonical commodity label. |
| `commodity_group_code` | Mapping | `STRING` | Yes | FK | Parent commodity-group code. |
| `commodity_group_name` | Mapping | `STRING` | Yes | — | Parent commodity-group label. |
| `publisher_series_name` | CSV text | `STRING` | No | — | Original SBP series label. |
| `hierarchy_level` | Mapping | `INT` | No | — | Commodity hierarchy depth; total is level 0. |
| `is_total` | Derived/mapping | `BOOLEAN` | No | — | True for publisher total/group total as classified. |
| `is_additive_member` | Mapping | `BOOLEAN` | No | — | True only for a non-overlapping approved aggregation set. |
| `amount_usd_thousand` | CSV numeric text | `DECIMAL(24,6)` | Conditional | — | Import payments in thousand USD. |
| `source_unit` | CSV text | `STRING` | No | — | Original unit; must map to thousand USD. |
| `source_comment` | CSV text | `STRING` | Yes | — | Observation comment/footnote. |

## 8. PBS weekly Sensitive Price Indicator (SPI)

The weekly release contains a Report workbook and an Annexure workbook. Inspected releases include Report pages for summary, item comparisons, and historical SPI plus Annexure appendices for city-level minimum/average/maximum prices and other tables. Because these structures have different measures, Bronze and Silver use a governed **long measure** contract. Item order is never a key.

### 8.1 Bronze table: `bronze.pbs_spi_raw`

Common Bronze columns in §1.3 are included.

| Column name | Source data type | Target Spark type | Nullable | Key | Description |
|---|---|---|---:|---|---|
| `workbook_kind` | File classification | `STRING` | No | — | `REPORT` or `ANNEXURE`. |
| `sheet_name` | XLSX metadata | `STRING` | No | — | Exact worksheet name. |
| `source_table_name` | Parser/layout contract | `STRING` | No | — | Stable table identifier such as report page/table or Appendix A/B. |
| `record_type_raw` | Parser classification | `STRING` | No | — | `SUMMARY_INDEX`, `ITEM_PRICE`, `HISTORICAL_INDEX`, or reviewed appendix type. |
| `reference_date_raw` | XLSX date/text cell | `STRING` | No | — | Official week-ending/comparison date associated with the measure. |
| `base_year_raw` | XLSX text cell | `STRING` | Conditional | — | Printed index base; required for index/rate records. |
| `location_raw` | XLSX text cell/header | `STRING` | Conditional | — | City, national, combined, or other geography; required for location-specific price rows. |
| `expenditure_group_raw` | XLSX text cell/header | `STRING` | Yes | — | Quintile/income group or combined group. |
| `item_code_raw` | XLSX cell | `STRING` | Yes | — | Published item identifier when supplied. |
| `item_name_raw` | XLSX text cell | `STRING` | Conditional | — | Item description; required for item records. |
| `item_unit_raw` | XLSX text cell | `STRING` | Conditional | — | Published quantity/unit; required for price records. |
| `measure_name_raw` | XLSX header | `STRING` | No | — | Published measure label (price, index, weight, percentage change, etc.). |
| `price_type_raw` | XLSX header | `STRING` | Yes | — | Minimum, average, maximum, or another published price type. |
| `comparison_period_raw` | XLSX header/date | `STRING` | No | — | `CURRENT_WEEK`, `PREVIOUS_WEEK`, `YEAR_AGO`, or a parsed historical period. |
| `value_raw` | XLSX numeric/text cell | `STRING` | Yes | — | Original measure token; null only for a published missing value. |
| `value_unit_raw` | XLSX header/derived contract | `STRING` | No | — | Printed/contract unit such as `PKR`, `INDEX`, `PERCENT`, or `WEIGHT`. |
| `footnote_raw` | XLSX text cell | `STRING` | Yes | — | Associated note/provisional marker. |
| `workbook_formula_mode` | Parser metadata | `STRING` | No | — | `CACHED_VALUE`, `FORMULA_TEXT`, or `NOT_APPLICABLE`. |

### 8.2 Silver table: `silver.pbs_spi_weekly`

Common Silver columns in §1.4 are included.

**Business grain:** one reference week/date × record type × geography × expenditure group × item × measure × price type × comparison period.  
**Business key:** `(reference_date, record_type, geography_code, expenditure_group_code, item_code, measure_code, price_type, comparison_period)` using `ALL` only when a dimension is structurally absent.

| Column name | Source data type | Target Spark type | Nullable | Key | Description |
|---|---|---|---:|---|---|
| `reference_date` | Parsed XLSX date/text | `DATE` | No | BK | Week-ending or historical reference date represented by this measure. |
| `record_type` | Bronze classification | `STRING` | No | BK | Governed record type. |
| `base_year` | Parsed XLSX text | `STRING` | Conditional | — | Canonical base year; required for index/percentage records. |
| `geography_code` | Mapping | `STRING` | No | BK/FK | Governed city/area code or `ALL`. |
| `geography_name` | XLSX + mapping | `STRING` | No | — | Canonical geography label. |
| `expenditure_group_code` | Mapping | `STRING` | No | BK/FK | Governed quintile/group code or `ALL`. |
| `expenditure_group_name` | XLSX + mapping | `STRING` | No | — | Canonical expenditure group. |
| `item_code` | Mapping/source | `STRING` | No | BK/FK | Governed essential-item code or `ALL`. |
| `item_name` | XLSX + mapping | `STRING` | Conditional | — | Canonical item name; required for item records. |
| `item_unit` | XLSX + mapping | `STRING` | Conditional | — | Canonical quantity/unit for item prices. |
| `measure_code` | Header mapping | `STRING` | No | BK/FK | Governed code such as `PRICE`, `SPI_INDEX`, `WOW_CHANGE_PCT`, `YOY_CHANGE_PCT`, or `WEIGHT`. |
| `measure_name` | XLSX + mapping | `STRING` | No | — | Canonical measure label. |
| `price_type` | Header mapping | `STRING` | No | BK | `MINIMUM`, `AVERAGE`, `MAXIMUM`, or `NOT_APPLICABLE`. |
| `comparison_period` | Header/date mapping | `STRING` | No | BK | `CURRENT_WEEK`, `PREVIOUS_WEEK`, `YEAR_AGO`, or `HISTORICAL`. |
| `measure_value` | XLSX numeric text | `DECIMAL(24,8)` | Conditional | — | Parsed published measure; null only for valid missing status. |
| `measure_unit` | XLSX/contract mapping | `STRING` | No | — | `PKR`, `INDEX`, `PERCENT`, or `WEIGHT`. |
| `source_footnote` | XLSX text | `STRING` | Yes | — | Footnote/provisional note. |

Gold may pivot current/previous/year-ago measures into convenient columns, but Silver preserves the publisher's complete long-form measure set. Published percentage changes are checked against comparable rows within rounding tolerance and are not silently replaced by derived values.

## 9. PBS monthly Consumer Price Index (CPI)

The monthly PDF contains multiple tables and domains. The parser emits one row per headline/group observation. It does not use prose statements as substitutes for table values; prose may be captured as a reconciliation check.

### 9.1 Bronze table: `bronze.pbs_cpi_raw`

Common Bronze columns in §1.3 are included.

| Column name | Source data type | Target Spark type | Nullable | Key | Description |
|---|---|---|---:|---|---|
| `record_type_raw` | Parser classification | `STRING` | No | — | `HEADLINE`, `GROUP_INDEX`, `CORE_INFLATION`, or another reviewed contract value. |
| `period_raw` | PDF text token/header | `STRING` | No | — | Month/year exactly as printed. |
| `base_year_raw` | PDF text | `STRING` | No | — | Printed index base, normally `2015-16=100`. |
| `domain_raw` | PDF table/header text | `STRING` | No | — | `National`, `Urban`, or `Rural` publisher label. |
| `group_code_raw` | PDF table cell | `STRING` | Yes | — | Publisher/COICOP group code when printed. |
| `group_name_raw` | PDF table cell | `STRING` | No | — | Headline/group/component label. |
| `weight_raw` | PDF numeric token | `STRING` | Yes | — | Published CPI weight. |
| `current_index_raw` | PDF numeric token | `STRING` | Conditional | — | Current-month index level. |
| `previous_month_index_raw` | PDF numeric token | `STRING` | Yes | — | Previous-month index level. |
| `previous_year_index_raw` | PDF numeric token | `STRING` | Yes | — | Same month of previous year index. |
| `mom_change_pct_raw` | PDF numeric token | `STRING` | Yes | — | Published month-over-month percentage change. |
| `yoy_change_pct_raw` | PDF numeric token | `STRING` | Yes | — | Published year-over-year percentage change. |
| `fiscal_ytd_change_pct_raw` | PDF numeric token | `STRING` | Yes | — | Published fiscal-year-to-date/average change where present. |
| `contribution_pp_raw` | PDF numeric token | `STRING` | Yes | — | Published contribution in percentage points where present. |
| `provisional_marker_raw` | PDF text | `STRING` | Yes | — | `P`, `Provisional`, or equivalent marker. |
| `footnote_raw` | PDF text | `STRING` | Yes | — | Relevant table footnote. |
| `page_number` | PDF metadata | `INT` | No | — | 1-based page number. |
| `table_identifier` | Parser metadata | `STRING` | No | — | Stable configured table/layout identifier. |
| `extraction_method` | Parser metadata | `STRING` | No | — | `PDF_TEXT` or `OCR`. |
| `extraction_confidence` | Extractor metadata | `DECIMAL(7,6)` | Yes | — | OCR confidence from 0 to 1; null for deterministic text extraction. |

`current_index_raw` is required for index/headline records. A rate-only table may omit it only when `record_type_raw` explicitly defines a rate observation.

### 9.2 Silver table: `silver.pbs_cpi_monthly`

Common Silver columns in §1.4 are included.

**Business grain:** one month × domain × CPI measure/group × base year.  
**Business key:** `(period_end_date, domain_code, group_code, measure_type, base_year)`.

| Column name | Source data type | Target Spark type | Nullable | Key | Description |
|---|---|---|---:|---|---|
| `period_start_date` | Parsed PDF period | `DATE` | No | — | First day of observation month. |
| `period_end_date` | Parsed PDF period | `DATE` | No | BK | Last day of observation month. |
| `base_year` | Parsed PDF text | `STRING` | No | BK | Canonical index base, e.g. `2015-16`. |
| `domain_code` | Mapping | `STRING` | No | BK/FK | `NATIONAL`, `URBAN`, or `RURAL`. |
| `domain_name` | PDF + mapping | `STRING` | No | — | Canonical domain label. |
| `group_code` | Mapping/source | `STRING` | No | BK/FK | Governed CPI/COICOP group code; `GENERAL` for headline. |
| `group_name` | PDF + mapping | `STRING` | No | — | Canonical group/measure name. |
| `measure_type` | Parser/mapping | `STRING` | No | BK | `INDEX`, `CORE_INFLATION`, or other reviewed measure. |
| `weight_pct` | PDF numeric text | `DECIMAL(18,8)` | Yes | — | Published group weight; may be null for rate-only measures. |
| `current_index` | PDF numeric text | `DECIMAL(18,6)` | Conditional | — | Current-month index level for index records. |
| `previous_month_index` | PDF numeric text | `DECIMAL(18,6)` | Yes | — | Previous-month index. |
| `previous_year_index` | PDF numeric text | `DECIMAL(18,6)` | Yes | — | Same month of previous year index. |
| `mom_change_pct` | PDF numeric text | `DECIMAL(18,6)` | Yes | — | Publisher month-over-month change. |
| `yoy_change_pct` | PDF numeric text | `DECIMAL(18,6)` | Yes | — | Publisher year-over-year change. |
| `fiscal_ytd_change_pct` | PDF numeric text | `DECIMAL(18,6)` | Yes | — | Publisher fiscal-year-to-date/average change. |
| `contribution_pp` | PDF numeric text | `DECIMAL(18,6)` | Yes | — | Published contribution in percentage points. |
| `source_footnote` | PDF text | `STRING` | Yes | — | Relevant footnote. |
| `extraction_method` | Bronze | `STRING` | No | — | `PDF_TEXT` or `OCR`. |
| `extraction_confidence` | Bronze | `DECIMAL(7,6)` | Yes | — | OCR confidence; required for OCR-derived records. |

Where comparison indices exist, month-over-month and year-over-year changes are recalculated for validation but the publisher values remain the stored published measures. A discrepancy outside tolerance is quarantined or warned according to the rule catalogue.

## 10. OGRA fuel prices / kerosene notifications

OGRA notifications may show a final product price and a component buildup. Each component is a separate record. A product-price row uses `component_code = 'FINAL_PRICE'`, avoiding a wide schema that changes when the price formula changes.

### 10.1 Bronze table: `bronze.ogra_fuel_price_raw`

Common Bronze columns in §1.3 are included.

| Column name | Source data type | Target Spark type | Nullable | Key | Description |
|---|---|---|---:|---|---|
| `notification_number_raw` | PDF text/OCR | `STRING` | Yes | — | Official notification/reference number; may be absent in some historical scans. |
| `notification_title_raw` | PDF text/OCR | `STRING` | No | — | Publication title. |
| `issue_date_raw` | PDF text/OCR | `STRING` | Yes | — | Issue/publication date token. |
| `effective_from_raw` | PDF text/OCR | `STRING` | No | — | Required effective start date token. |
| `effective_to_raw` | PDF text/OCR | `STRING` | Yes | — | Printed end date, if any. |
| `revision_marker_raw` | PDF text/OCR | `STRING` | Yes | — | Revised/corrigendum/supersession wording. |
| `product_code_raw` | PDF table text/OCR | `STRING` | Yes | — | Publisher abbreviation such as `SKO`, if printed. |
| `product_name_raw` | PDF table text/OCR | `STRING` | No | — | Product label, including kerosene aliases. |
| `component_name_raw` | PDF table text/OCR | `STRING` | No | — | Final price or buildup component label. |
| `price_basis_raw` | PDF table/header text | `STRING` | Yes | — | Ex-refinery, ex-depot, direct, rail/defence, maximum, etc. |
| `geography_raw` | PDF text/OCR | `STRING` | Yes | — | National, district, depot, or other location when applicable. |
| `unit_raw` | PDF text/OCR | `STRING` | No | — | Printed unit, normally PKR/litre. |
| `amount_raw` | PDF numeric token/OCR | `STRING` | No | — | Component or final-price numeric token. |
| `previous_amount_raw` | PDF numeric token/OCR | `STRING` | Yes | — | Prior notified price when printed. |
| `change_amount_raw` | PDF numeric token/OCR | `STRING` | Yes | — | Printed absolute change. |
| `tax_rate_raw` | PDF numeric token/OCR | `STRING` | Yes | — | Percentage rate when component is a tax. |
| `footnote_raw` | PDF text/OCR | `STRING` | Yes | — | Applicable note. |
| `page_number` | PDF metadata | `INT` | No | — | 1-based source page. |
| `table_identifier` | Parser metadata | `STRING` | No | — | Stable table/layout identifier. |
| `extraction_method` | Parser metadata | `STRING` | No | — | `PDF_TEXT` or `OCR`. |
| `ocr_confidence` | OCR engine | `DECIMAL(7,6)` | Conditional | — | 0–1 confidence; required when extraction method is OCR. |
| `evidence_text` | PDF text/OCR | `STRING` | No | — | Bounded source snippet used to verify the extracted row. |

### 10.2 Silver table: `silver.ogra_fuel_price`

Common Silver columns in §1.4 are included.

**Business grain:** one notification revision × effective start × product × price basis × geography × component.  
**Business key:** `(notification_id, effective_from_date, product_code, price_basis_code, geography_code, component_code)`.

| Column name | Source data type | Target Spark type | Nullable | Key | Description |
|---|---|---|---:|---|---|
| `notification_id` | PDF number or derived | `STRING` | No | BK | Normalized official number; when absent, deterministic ID from title/date/file hash. |
| `notification_title` | PDF text/OCR | `STRING` | No | — | Normalized publication title. |
| `notification_issue_date` | Parsed PDF text | `DATE` | Yes | — | Date issued; null only when not recoverable with acceptable confidence. |
| `effective_from_date` | Parsed PDF text | `DATE` | No | BK | Date price/component becomes effective. |
| `effective_to_date` | Parsed PDF text/derived | `DATE` | Yes | — | Printed or later derived supersession end date; null while open-ended. |
| `product_code` | Mapping | `STRING` | No | BK/FK | Governed code such as `SKO`, `MS`, `HSD`, or `LDO`. |
| `product_name` | PDF + mapping | `STRING` | No | — | Canonical product name. |
| `price_basis_code` | Mapping | `STRING` | No | BK/FK | Governed basis such as `MAX_EX_DEPOT`, `DIRECT`, or `RAIL_DEFENCE`. |
| `geography_code` | Mapping | `STRING` | No | BK/FK | `PAK` for national or governed district/depot code. |
| `geography_name` | PDF + mapping | `STRING` | No | — | Canonical geography. |
| `component_code` | Mapping | `STRING` | No | BK/FK | Governed component; `FINAL_PRICE` for the product price itself. |
| `component_name` | PDF + mapping | `STRING` | No | — | Canonical component label. |
| `amount_pkr_per_litre` | PDF numeric/OCR | `DECIMAL(24,6)` | No | — | Published component/final amount in PKR per litre after approved unit mapping. |
| `previous_amount_pkr_per_litre` | PDF numeric/OCR | `DECIMAL(24,6)` | Yes | — | Previous amount when printed. |
| `change_pkr_per_litre` | PDF numeric/OCR | `DECIMAL(24,6)` | Yes | — | Published absolute change; recalculated for validation where possible. |
| `tax_rate_pct` | PDF numeric/OCR | `DECIMAL(18,6)` | Yes | — | Tax rate for rate-based components. |
| `source_unit` | PDF text/OCR | `STRING` | No | — | Original printed unit. |
| `is_revised_notification` | PDF text/derived | `BOOLEAN` | No | — | True when marked revised/corrigendum or superseding same effective period. |
| `supersedes_notification_id` | PDF text/derived | `STRING` | Yes | FK | Prior notification explicitly or deterministically superseded. |
| `source_footnote` | PDF text/OCR | `STRING` | Yes | — | Relevant note. |
| `extraction_method` | Bronze | `STRING` | No | — | `PDF_TEXT` or `OCR`. |
| `ocr_confidence` | Bronze | `DECIMAL(7,6)` | Conditional | — | Required for OCR-derived rows; must meet field threshold. |

For a price-buildup table, the sum of components is checked against the corresponding `FINAL_PRICE` using the document's formula and rounding tolerance. A component can be negative; the final price must be non-negative.

## 11. Cross-source field matrix

| Source family | Bronze table | Silver table | Business date | Business-key summary | Canonical measure unit |
|---|---|---|---|---|---|
| SBP remittances | `bronze.sbp_workers_remittances_raw` | `silver.sbp_remittance_monthly` | `period_end_date` | series × month | USD million |
| SBP FDI sector | `bronze.sbp_fdi_sector_raw` | `silver.sbp_fdi_sector_monthly` | `period_end_date` | series × component × month | USD million |
| SBP FX daily | `bronze.sbp_fx_daily_raw` | `silver.sbp_fx_daily` | `observation_date` | series × day | PKR per foreign-currency unit |
| SBP exports | `bronze.sbp_export_receipts_raw` | `silver.sbp_export_receipts_monthly` | `period_end_date` | series × month | USD thousand |
| SBP imports | `bronze.sbp_import_payments_raw` | `silver.sbp_import_payments_monthly` | `period_end_date` | series × month | USD thousand |
| PBS SPI | `bronze.pbs_spi_raw` | `silver.pbs_spi_weekly` | `week_ended_date` | week × type × geo × group × item | index / PKR per item unit |
| PBS CPI | `bronze.pbs_cpi_raw` | `silver.pbs_cpi_monthly` | `period_end_date` | month × domain × group × measure × base | index / percent |
| OGRA fuel | `bronze.ogra_fuel_price_raw` | `silver.ogra_fuel_price` | `effective_from_date` | notification × product × basis × geo × component | PKR/litre |

## 12. Required validation rules by source

| Rule code | Applies to | Blocking condition |
|---|---|---|
| `SOURCE_CONTRACT_MISMATCH` | All | Dataset identity/title/frequency/unit does not match active contract. |
| `SCHEMA_SIGNATURE_MISMATCH` | All | Required header, sheet, or PDF layout signature missing/changed. |
| `REQUIRED_VALUE_MISSING` | All | Non-null contract field absent after normalization. |
| `TYPE_PARSE_FAILED` | All | Date/numeric/boolean cannot be parsed under explicit rules. |
| `DUPLICATE_BUSINESS_KEY_IN_BATCH` | All Silver | More than one unresolved candidate for a key. |
| `UNKNOWN_REFERENCE_VALUE` | All Silver | Series/country/currency/sector/commodity/group/product alias not governed. |
| `INVALID_OBSERVATION_STATUS` | SBP | Null/non-null amount conflicts with publisher status. |
| `UNSAFE_HIERARCHY` | Remittance/trade/FDI | Parent/child relationship missing or a row is incorrectly marked additive. |
| `FDI_COMPONENT_RECONCILIATION_FAILED` | FDI | Net differs from inflow minus outflow beyond tolerance. |
| `FX_RATE_NON_POSITIVE` | FX | Present normalized rate is less than or equal to zero. |
| `SPI_CHANGE_RECONCILIATION_FAILED` | SPI | Published percentage differs from comparable values beyond rounding tolerance. |
| `CPI_CHANGE_RECONCILIATION_FAILED` | CPI | Published percentage differs from index-derived change beyond tolerance. |
| `OCR_CONFIDENCE_LOW` | CPI/OGRA OCR | Required extracted field is below configured confidence. |
| `FUEL_BUILDUP_RECONCILIATION_FAILED` | OGRA | Components do not reconcile to final price under the documented formula. |
| `DATE_OUTSIDE_REQUESTED_RANGE` | All backfills | Observation/effective date lies outside requested inclusive range. |

## 13. Delta constraints and table properties

Where supported, declare check constraints for stable invariants, while keeping pipeline validation as the primary control:

```sql
-- Examples; apply to the relevant table rather than as one combined statement.
CHECK (revision_number >= 1)
CHECK ((is_current AND valid_to_timestamp IS NULL)
    OR (NOT is_current AND valid_to_timestamp IS NOT NULL))
CHECK (extraction_confidence IS NULL
    OR (extraction_confidence >= 0 AND extraction_confidence <= 1))
CHECK (normalized_pkr_per_one_unit IS NULL OR normalized_pkr_per_one_unit > 0)
```

Recommended table properties:

```text
delta.enableChangeDataFeed = true       # if supported by the workspace/runtime
delta.columnMapping.mode = name         # only after confirming runtime support
project.schema_version = <version>
project.business_grain = <documented grain>
project.owner = <team/owner>
```

Do not enable a feature merely because it appears in this dictionary; the environment capability probe in `plan.md` determines supported properties.

## 14. Schema evolution procedure

1. Quarantine the drifted file/rows under the existing contract.
2. Preserve the exact source file and observed signature.
3. Profile the change against at least one prior and one new release.
4. Update this dictionary and the explicit `StructType` together.
5. Add a new `schema_version`; never mutate the meaning of an existing version.
6. Add/adjust parser fixtures and negative tests.
7. Reprocess with a new run ID and link original quarantine records through `resolution_run_id`.
8. Review downstream hashes, merge behavior, and Gold compatibility before activation.

## 15. Implementation notes

- CSV columns are read as explicit strings in Bronze when publisher formatting/missing tokens must be preserved; typed numeric/date fields are produced only in Silver.
- XLSX/PDF libraries may first produce Python values, but the DataFrame is constructed with the declared Bronze `StructType`; pandas inference is not an accepted schema contract.
- Field lists are intentionally stable and source-aware. Additional arbitrary PDF text belongs in `raw_payload`/evidence, not in dynamically created columns.
- Business-key sentinels such as `ALL` are allowed only where a dimension is structurally absent for that record type; source nulls are not silently converted to sentinels.
- Natural keys use stable codes/series keys, never display labels alone.
- Every table described here also includes the common lineage fields for its layer, satisfying record-level lineage and `load_timestamp` requirements.
