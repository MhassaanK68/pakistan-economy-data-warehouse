# CareWatch Bronze and Silver Schema Contract

**Status:** Locked for Phase 2  
**Decision date:** 2026-10-10  
**Source snapshot:** CMS September 2026 nursing-home release  
**Change policy:** Any column, type, key, or validation change requires an explicit update to this document and the matching code/tests in the same pull request.

## 1. Research basis

This contract was checked against the live CMS Provider Data Catalog, the live datastore schemas, the CMS consolidated nursing-home data dictionary, and the full local September 2026 CSV extracts.

Official sources:

- [Provider Information (`4pq5-n9py`)](https://data.cms.gov/provider-data/dataset/4pq5-n9py)
- [Health Deficiencies (`r5ix-sfxw`)](https://data.cms.gov/provider-data/dataset/r5ix-sfxw)
- [Penalties (`g6vv-u9sr`)](https://data.cms.gov/provider-data/dataset/g6vv-u9sr)
- [MDS Quality Measures (`djen-97ju`)](https://data.cms.gov/provider-data/dataset/djen-97ju)
- [CMS Nursing Home Care Compare and Provider Data Catalog Consolidated Data Dictionary](https://data.cms.gov/provider-data/sites/default/files/data_dictionaries/nursing_home/NH_Data_Dictionary.pdf)
- Datastore schema endpoint pattern: `https://data.cms.gov/provider-data/api/1/datastore/query/<dataset-id>/0?limit=1&schema=true`

The live datastore reports every API field as `text`. Therefore, its schema determines canonical names and source shape, while the CMS dictionary and observed values determine Silver types.

Profile results from the complete local extracts:

| Dataset | Rows | Candidate-key distinct count | Null key rows | Result |
|---|---:|---:|---:|---|
| Health Deficiencies | 418,947 | 418,947 | 0 | Key accepted for this snapshot |
| Penalties | 15,419 | 15,419 | 0 | Key accepted for this snapshot |
| Provider Information | 14,690 | 14,690 | 0 | CCN accepted as key |
| MDS Quality Measures | 249,730 | 249,730 | 0 | Key accepted for this snapshot |

These checks must run against every new full snapshot. A future violation is a contract failure, not permission to silently drop a row.

## 2. Global conventions

### 2.1 Names and source matching

- Bronze uses the exact snake-case names returned by the CMS API.
- Bulk CSV display headers are mapped to API names by a normalized comparison that removes non-alphanumeric characters.
- Every mismatch must have an explicit, version-controlled override. The known September 2026 override is:

```text
Total number of nurse staff hours per resident per day on the weekend
  -> total_number_of_nurse_staff_hours_per_resident_per_day_on_t_4a14
```

- Silver retains the canonical source name unless this document explicitly renames it.

### 2.2 Identifier types

The following remain `STRING` even if the CMS dictionary calls them numeric:

- `cms_certification_number_ccn`
- `zip_code`
- `provider_ssa_county_code`
- `deficiency_tag_number`
- `measure_code`
- `fine_id`
- `chain_id`

Leading zeros and identifier formatting are meaningful. A CCN may be alphanumeric; use `^[0-9A-Z]{6}$`, not a digits-only rule.

### 2.3 Nulls, dates, and timestamps

- Bronze preserves source strings, including empty strings.
- Silver trims strings and converts empty strings to `NULL` before casting.
- Source calendar values use `DATE`.
- Operational timestamps use UTC `TIMESTAMP`.
- A failed `try_cast` on a non-empty source value quarantines the row.

### 2.4 Deterministic hashes

- Entity keys use SHA-256 over the normalized business-key columns in the documented order.
- `row_hash` uses SHA-256 over an ordered JSON struct of business columns only. Generate JSON with null fields retained; do not include ingestion metadata, `load_timestamp`, or the entity key.
- `provider_address_hash` uses SHA-256 over `upper(trim(provider_address))` plus a secret salt from Databricks Secrets.
- Never commit the address salt.

### 2.5 Standard Bronze metadata

Every accepted Bronze row has these non-null columns:

| Column | Type | Purpose |
|---|---|---|
| `_dataset_id` | `STRING` | CMS dataset identifier |
| `_source_file` | `STRING` | Exact landing path processed |
| `_source_file_sha256` | `STRING` | Lowercase 64-character source-file digest |
| `_batch_id` | `STRING` | Deterministic 24-character batch identifier unless explicitly overridden |
| `_load_type` | `STRING` | `full` or `incremental` |
| `_ingest_date` | `DATE` | UTC date of ingestion |
| `load_timestamp` | `TIMESTAMP` | UTC row ingestion timestamp |

`_corrupt_record` is a read-stage field only. Corrupt rows go to `bronze_quarantine`; it is not stored in accepted Bronze tables.

### 2.6 Standard Silver lineage

Every accepted Silver row has:

| Column | Type | Nullability | Purpose |
|---|---|---|---|
| `row_hash` | `STRING` | NOT NULL | Change detection across ordered business columns |
| `is_deleted` | `BOOLEAN` | NOT NULL | Soft-delete marker; defaults to `false` |
| `source_file` | `STRING` | NOT NULL | Source file that produced the current version |
| `source_file_sha256` | `STRING` | NOT NULL | Source content digest |
| `source_batch_id` | `STRING` | NOT NULL | Bronze batch lineage |
| `source_processing_date` | `DATE` | Nullable | CMS publication/retrieval date from `processing_date` |
| `load_timestamp` | `TIMESTAMP` | NOT NULL | Last inserted or materially updated time in UTC |

Unchanged `MERGE` matches do not update `load_timestamp`.

### 2.7 Privacy projection

For all four Silver datasets:

- Drop `telephone_number` where present.
- Drop raw `provider_address`.
- Drop `location`, which repeats the street address.
- Add nullable `provider_address_hash STRING`.
- Retain `citytown`, `state`, and `zip_code` for geographic analysis.

## 3. Bronze schemas

All source columns below are nullable `STRING`. Append the standard non-null Bronze metadata from Section 2.5.

Full loads use header-bearing CSV files. Incremental deficiency and penalty samples use this explicitly typed JSON envelope:

| JSON field | Spark type | Purpose |
|---|---|---|
| `_meta.dataset_id` | `STRING` | Must equal the configured dataset ID |
| `_meta.filter` | `STRING` | API filter recorded by acquisition |
| `_meta.rows_available` | `LONG` | Rows CMS reported for the filter |
| `_meta.rows_in_file` | `LONG` | Rows stored in this sample |
| `_meta.fetched_at_utc` | `STRING` | ISO-8601 acquisition timestamp; validate, then cast only if retained in audit data |
| `_meta.load_type` | `STRING` | Must equal `incremental` |
| `results` | `ARRAY<STRUCT<...>>` | Dataset-specific source columns below, each as `STRING` |

Validate the envelope before exploding `results`: dataset ID and load type must match the run parameters, `rows_in_file` must equal the exploded count, and `rows_in_file` must not exceed `rows_available`. Envelope metadata belongs in audit logs, not as duplicated columns on every accepted Bronze row.

### 3.1 `bronze_nh_health_deficiencies`

Dataset ID: `r5ix-sfxw`; 24 source columns.

```text
cms_certification_number_ccn
provider_name
provider_address
citytown
state
zip_code
survey_date
survey_footnote
survey_type
deficiency_prefix
deficiency_category
deficiency_tag_number
deficiency_description
scope_severity_code
deficiency_corrected
correction_date
inspection_cycle
standard_deficiency
complaint_deficiency
infection_control_inspection_deficiency
citation_under_idr
citation_under_iidr
location
processing_date
```

### 3.2 `bronze_nh_penalties`

Dataset ID: `g6vv-u9sr`; 14 source columns.

```text
cms_certification_number_ccn
provider_name
provider_address
citytown
state
zip_code
penalty_date
penalty_type
fine_id
fine_amount
payment_denial_start_date
payment_denial_length_in_days
location
processing_date
```

### 3.3 `bronze_nh_mds_quality`

Dataset ID: `djen-97ju`; 23 source columns.

```text
cms_certification_number_ccn
provider_name
provider_address
citytown
state
zip_code
measure_code
measure_description
resident_type
q1_measure_score
footnote_for_q1_measure_score
q2_measure_score
footnote_for_q2_measure_score
q3_measure_score
footnote_for_q3_measure_score
q4_measure_score
footnote_for_q4_measure_score
four_quarter_average_score
footnote_for_four_quarter_average_score
used_in_quality_measure_five_star_rating
measure_period
location
processing_date
```

### 3.4 `bronze_nh_provider_info`

Dataset ID: `4pq5-n9py`; 102 source columns in the September 2026 live API and local extract. Older catalog descriptions may still state 99 columns, so the live API contract takes precedence and the difference must be captured as schema evolution.

```text
cms_certification_number_ccn
provider_name
provider_address
citytown
state
zip_code
telephone_number
provider_ssa_county_code
countyparish
urban
ownership_type
number_of_certified_beds
average_number_of_residents_per_day
average_number_of_residents_per_day_footnote
provider_type
provider_resides_in_hospital
legal_business_name
date_first_approved_to_provide_medicare_and_medicaid_services
chain_name
chain_id
number_of_facilities_in_chain
chain_average_overall_5star_rating
chain_average_health_inspection_rating
chain_average_staffing_rating
chain_average_qm_rating
continuing_care_retirement_community
special_focus_status
abuse_icon
high_performing_icon
most_recent_health_inspection_more_than_2_years_ago
provider_changed_ownership_in_last_12_months
with_a_resident_and_family_council
automatic_sprinkler_systems_in_all_required_areas
overall_rating
overall_rating_footnote
health_inspection_rating
health_inspection_rating_footnote
qm_rating
qm_rating_footnote
longstay_qm_rating
longstay_qm_rating_footnote
shortstay_qm_rating
shortstay_qm_rating_footnote
staffing_rating
staffing_rating_footnote
reported_staffing_footnote
physical_therapist_staffing_footnote
reported_nurse_aide_staffing_hours_per_resident_per_day
reported_lpn_staffing_hours_per_resident_per_day
reported_rn_staffing_hours_per_resident_per_day
reported_licensed_staffing_hours_per_resident_per_day
reported_total_nurse_staffing_hours_per_resident_per_day
total_number_of_nurse_staff_hours_per_resident_per_day_on_t_4a14
registered_nurse_hours_per_resident_per_day_on_the_weekend
reported_physical_therapist_staffing_hours_per_resident_per_day
total_nursing_staff_turnover
total_nursing_staff_turnover_footnote
registered_nurse_turnover
registered_nurse_turnover_footnote
number_of_administrators_who_have_left_the_nursing_home
administrator_turnover_footnote
nursing_casemix_index
nursing_casemix_index_ratio
casemix_nurse_aide_staffing_hours_per_resident_per_day
casemix_lpn_staffing_hours_per_resident_per_day
casemix_rn_staffing_hours_per_resident_per_day
casemix_total_nurse_staffing_hours_per_resident_per_day
casemix_weekend_total_nurse_staffing_hours_per_resident_per_day
adjusted_nurse_aide_staffing_hours_per_resident_per_day
adjusted_lpn_staffing_hours_per_resident_per_day
adjusted_rn_staffing_hours_per_resident_per_day
adjusted_total_nurse_staffing_hours_per_resident_per_day
adjusted_weekend_total_nurse_staffing_hours_per_resident_per_day
rating_cycle_1_standard_survey_health_date
rating_cycle_1_standard_survey_footnote
rating_cycle_1_total_number_of_health_deficiencies
rating_cycle_1_number_of_standard_health_deficiencies
rating_cycle_1_number_of_complaint_health_deficiencies
rating_cycle_1_health_deficiency_score
rating_cycle_1_number_of_health_revisits
rating_cycle_1_health_revisit_score
rating_cycle_1_total_health_score
rating_cycle_2_standard_health_survey_date
rating_cycle_2_standard_survey_footnote
rating_cycle_23_total_number_of_health_deficiencies
rating_cycle_2_number_of_standard_health_deficiencies
rating_cycle_23_number_of_complaint_health_deficiencies
rating_cycle_23_health_deficiency_score
rating_cycle_23_number_of_health_revisits
rating_cycle_23_health_revisit_score
rating_cycle_23_total_health_score
total_weighted_health_survey_score
number_of_citations_from_infection_control_inspections
number_of_fines
total_amount_of_fines_in_dollars
number_of_payment_denials
total_number_of_penalties
location
latitude
longitude
geocoding_footnote
processing_date
```

## 4. Silver schemas

`Nullable` below describes the stored Silver column. Business-key fields and technical keys are non-null. Other source fields are nullable unless a validation rule explicitly requires them for a subtype.

### 4.1 `silver_deficiency`

Business key, in order: `cms_certification_number_ccn`, `survey_date`, `survey_type`, `deficiency_prefix`, `deficiency_tag_number`, `inspection_cycle`.

| Column | Type | Nullable | Transformation or rule |
|---|---|---|---|
| `deficiency_key` | `STRING` | No | SHA-256 of ordered business key |
| `cms_certification_number_ccn` | `STRING` | No | Uppercase; regex `^[0-9A-Z]{6}$` |
| `provider_name` | `STRING` | Yes | Trimmed |
| `provider_address_hash` | `STRING` | Yes | Salted SHA-256 of normalized address |
| `citytown` | `STRING` | Yes | Trimmed |
| `state` | `STRING` | Yes | Uppercase two-character postal code |
| `zip_code` | `STRING` | Yes | Preserve leading zeros; expected five digits |
| `survey_date` | `DATE` | No | `try_cast` |
| `survey_footnote` | `INT` | Yes | CMS footnote code; do not treat as a measure |
| `survey_type` | `STRING` | No | Current health dataset value is `Health` |
| `deficiency_prefix` | `STRING` | No | Current health dataset value is `F` |
| `deficiency_category` | `STRING` | Yes | Trimmed |
| `deficiency_tag_number` | `STRING` | No | Four digits; preserve leading zero |
| `deficiency_description` | `STRING` | Yes | Trimmed |
| `scope_severity_code` | `STRING` | Yes | Uppercase `A` through `L` |
| `severity_group` | `STRING` | Yes | `A-C`, `D-F`, `G-I`, or `J-L` |
| `deficiency_corrected` | `STRING` | Yes | Controlled CMS status text |
| `correction_date` | `DATE` | Yes | Required when status states that a correction date exists; may precede survey date for past non-compliance |
| `inspection_cycle` | `INT` | No | Allowed `1`, `2`, or `3` |
| `standard_deficiency` | `BOOLEAN` | Yes | Strict `Y`/`N` mapping |
| `complaint_deficiency` | `BOOLEAN` | Yes | Strict `Y`/`N` mapping |
| `infection_control_inspection_deficiency` | `BOOLEAN` | Yes | Strict `Y`/`N` mapping |
| `citation_under_idr` | `BOOLEAN` | Yes | Strict `Y`/`N` mapping |
| `citation_under_iidr` | `BOOLEAN` | Yes | Strict `Y`/`N` mapping |
| `row_hash` | `STRING` | No | Standard change hash |
| `is_deleted` | `BOOLEAN` | No | Defaults to `false` |
| `source_file` | `STRING` | No | Bronze lineage |
| `source_file_sha256` | `STRING` | No | Bronze lineage |
| `source_batch_id` | `STRING` | No | Bronze `_batch_id` |
| `source_processing_date` | `DATE` | Yes | Cast from `processing_date` |
| `load_timestamp` | `TIMESTAMP` | No | Insert/material-change time |

Observed profile: survey date range `2017-03-23` through `2026-08-26`; all 418,947 rows were `Health`/`F`; inspection cycles were 1–3; all five indicator fields were `Y`/`N`; severity values were within `B`–`L`. Validation still permits the documented `A`–`L` domain. There were 5,339 rows with a correction date before the survey date, 5,167 of them marked `Past Non-Compliance`; this date ordering is not itself an error.

### 4.2 `silver_penalty`

Business key, in order: `cms_certification_number_ccn`, `penalty_date`, `penalty_type`, `fine_id`, `payment_denial_start_date`. Null subtype components are encoded explicitly in the key serialization.

| Column | Type | Nullable | Transformation or rule |
|---|---|---|---|
| `penalty_key` | `STRING` | No | SHA-256 of ordered business key |
| `cms_certification_number_ccn` | `STRING` | No | Uppercase; six-character CCN |
| `provider_name` | `STRING` | Yes | Trimmed |
| `provider_address_hash` | `STRING` | Yes | Salted SHA-256 |
| `citytown` | `STRING` | Yes | Trimmed |
| `state` | `STRING` | Yes | Uppercase postal code |
| `zip_code` | `STRING` | Yes | Preserve leading zeros |
| `penalty_date` | `DATE` | No | Inspection date that triggered event |
| `penalty_type` | `STRING` | No | Exactly `Fine` or `Payment Denial` |
| `fine_id` | `STRING` | Yes | Required for `Fine`; null for denial |
| `fine_amount` | `DECIMAL(14,2)` | Yes | Required and non-negative for `Fine`; null for denial |
| `payment_denial_start_date` | `DATE` | Yes | Required for `Payment Denial`; null for fine |
| `payment_denial_length_in_days` | `INT` | Yes | Required and non-negative for denial; null for fine |
| `row_hash` | `STRING` | No | Standard change hash |
| `is_deleted` | `BOOLEAN` | No | Defaults to `false` |
| `source_file` | `STRING` | No | Bronze lineage |
| `source_file_sha256` | `STRING` | No | Bronze lineage |
| `source_batch_id` | `STRING` | No | Bronze `_batch_id` |
| `source_processing_date` | `DATE` | Yes | Cast from `processing_date` |
| `load_timestamp` | `TIMESTAMP` | No | Insert/material-change time |

Observed profile: 12,989 fine rows and 2,430 payment-denial rows. The subtype fields were mutually exclusive. Observed fines ranged from `$344` to `$713,795`.

### 4.3 `silver_mds_quality`

Business key, in order: `cms_certification_number_ccn`, `measure_code`, `resident_type`, `measure_period`.

| Column | Type | Nullable | Transformation or rule |
|---|---|---|---|
| `mds_key` | `STRING` | No | SHA-256 of ordered business key |
| `cms_certification_number_ccn` | `STRING` | No | Uppercase; six-character CCN |
| `provider_name` | `STRING` | Yes | Trimmed |
| `provider_address_hash` | `STRING` | Yes | Salted SHA-256 |
| `citytown` | `STRING` | Yes | Trimmed |
| `state` | `STRING` | Yes | Uppercase postal code |
| `zip_code` | `STRING` | Yes | Preserve leading zeros |
| `measure_code` | `STRING` | No | Three-character identifier |
| `measure_description` | `STRING` | Yes | Trimmed |
| `resident_type` | `STRING` | No | `Long Stay` or `Short Stay` |
| `q1_measure_score` | `DOUBLE` | Yes | Observed range 0–100 |
| `footnote_for_q1_measure_score` | `INT` | Yes | CMS footnote code |
| `q2_measure_score` | `DOUBLE` | Yes | Observed range 0–100 |
| `footnote_for_q2_measure_score` | `INT` | Yes | CMS footnote code |
| `q3_measure_score` | `DOUBLE` | Yes | Observed range 0–100 |
| `footnote_for_q3_measure_score` | `INT` | Yes | CMS footnote code |
| `q4_measure_score` | `DOUBLE` | Yes | Observed range 0–100 |
| `footnote_for_q4_measure_score` | `INT` | Yes | CMS footnote code |
| `four_quarter_average_score` | `DOUBLE` | Yes | Observed range 0–100 |
| `footnote_for_four_quarter_average_score` | `INT` | Yes | CMS footnote code |
| `used_in_quality_measure_five_star_rating` | `BOOLEAN` | No | Strict `Y`/`N` mapping |
| `measure_period` | `STRING` | No | Pattern `YYYYQn-YYYYQn` |
| `row_hash` | `STRING` | No | Standard change hash |
| `is_deleted` | `BOOLEAN` | No | Defaults to `false` |
| `source_file` | `STRING` | No | Bronze lineage |
| `source_file_sha256` | `STRING` | No | Bronze lineage |
| `source_batch_id` | `STRING` | No | Bronze `_batch_id` |
| `source_processing_date` | `DATE` | Yes | Cast from `processing_date` |
| `load_timestamp` | `TIMESTAMP` | No | Insert/material-change time |

Observed profile: 205,660 long-stay and 44,070 short-stay rows; 17 measure codes; all non-empty score values parsed as numeric and fell between 0 and 100. Quarterly scores are legitimately nullable and must not be imputed from the four-quarter average.

### 4.4 `silver_facility`

Business key: `cms_certification_number_ccn`. Phase 2 uses SCD Type 1. `facility_key` is deterministic and stable; there are no `valid_from`, `valid_to`, or `is_current` columns in the Phase 2 contract.

#### Identity, organization, and geography

| Column | Type | Nullable | Transformation or rule |
|---|---|---|---|
| `facility_key` | `STRING` | No | SHA-256 of CCN |
| `cms_certification_number_ccn` | `STRING` | No | Uppercase; six-character CCN |
| `provider_name` | `STRING` | Yes | Trimmed |
| `provider_address_hash` | `STRING` | Yes | Salted SHA-256; raw address dropped |
| `citytown` | `STRING` | Yes | Trimmed |
| `state` | `STRING` | Yes | Uppercase postal code |
| `zip_code` | `STRING` | Yes | Preserve leading zeros |
| `provider_ssa_county_code` | `STRING` | Yes | Identifier; preserve formatting |
| `countyparish` | `STRING` | Yes | Trimmed |
| `urban` | `BOOLEAN` | Yes | Strict `Y`/`N` mapping |
| `ownership_type` | `STRING` | Yes | Controlled CMS category |
| `provider_type` | `STRING` | Yes | Controlled CMS category |
| `provider_resides_in_hospital` | `BOOLEAN` | Yes | Strict `Y`/`N` mapping |
| `legal_business_name` | `STRING` | Yes | Trimmed |
| `date_first_approved_to_provide_medicare_and_medicaid_services` | `DATE` | Yes | `try_cast` |
| `chain_name` | `STRING` | Yes | Trimmed |
| `chain_id` | `STRING` | Yes | Identifier; do not cast to number |
| `special_focus_status` | `STRING` | Yes | Null, `SFF`, or `SFF Candidate` |
| `with_a_resident_and_family_council` | `STRING` | Yes | `Resident`, `Family`, `Both`, or `None` |
| `automatic_sprinkler_systems_in_all_required_areas` | `STRING` | Yes | `Yes`, `Partial`, `No`, or `Data Not Available` |
| `latitude` | `DOUBLE` | Yes | Range -90 to 90 |
| `longitude` | `DOUBLE` | Yes | Range -180 to 180 |
| `geocoding_footnote` | `INT` | Yes | CMS footnote code |

`telephone_number` is dropped. `provider_address` and `location` are dropped after `provider_address_hash` is created.

#### Capacity, chain, ratings, and indicators

| Column | Type | Nullable |
|---|---|---|
| `number_of_certified_beds` | `INT` | Yes |
| `average_number_of_residents_per_day` | `DOUBLE` | Yes |
| `average_number_of_residents_per_day_footnote` | `INT` | Yes |
| `number_of_facilities_in_chain` | `INT` | Yes |
| `chain_average_overall_5star_rating` | `DOUBLE` | Yes |
| `chain_average_health_inspection_rating` | `DOUBLE` | Yes |
| `chain_average_staffing_rating` | `DOUBLE` | Yes |
| `chain_average_qm_rating` | `DOUBLE` | Yes |
| `continuing_care_retirement_community` | `BOOLEAN` | Yes |
| `abuse_icon` | `BOOLEAN` | Yes |
| `high_performing_icon` | `BOOLEAN` | Yes |
| `most_recent_health_inspection_more_than_2_years_ago` | `BOOLEAN` | Yes |
| `provider_changed_ownership_in_last_12_months` | `BOOLEAN` | Yes |
| `overall_rating` | `INT` | Yes |
| `overall_rating_footnote` | `INT` | Yes |
| `health_inspection_rating` | `INT` | Yes |
| `health_inspection_rating_footnote` | `INT` | Yes |
| `qm_rating` | `INT` | Yes |
| `qm_rating_footnote` | `INT` | Yes |
| `longstay_qm_rating` | `INT` | Yes |
| `longstay_qm_rating_footnote` | `INT` | Yes |
| `shortstay_qm_rating` | `INT` | Yes |
| `shortstay_qm_rating_footnote` | `INT` | Yes |
| `staffing_rating` | `INT` | Yes |
| `staffing_rating_footnote` | `INT` | Yes |

All non-null rating values must be between 1 and 5. Observed `overall_rating` values met this rule; 125 rows were null.

#### Staffing, turnover, and case mix

All measure columns in this group are nullable `DOUBLE`, except footnotes and administrator count.

| Column | Type |
|---|---|
| `reported_staffing_footnote` | `INT` |
| `physical_therapist_staffing_footnote` | `INT` |
| `reported_nurse_aide_staffing_hours_per_resident_per_day` | `DOUBLE` |
| `reported_lpn_staffing_hours_per_resident_per_day` | `DOUBLE` |
| `reported_rn_staffing_hours_per_resident_per_day` | `DOUBLE` |
| `reported_licensed_staffing_hours_per_resident_per_day` | `DOUBLE` |
| `reported_total_nurse_staffing_hours_per_resident_per_day` | `DOUBLE` |
| `total_number_of_nurse_staff_hours_per_resident_per_day_on_t_4a14` | `DOUBLE` |
| `registered_nurse_hours_per_resident_per_day_on_the_weekend` | `DOUBLE` |
| `reported_physical_therapist_staffing_hours_per_resident_per_day` | `DOUBLE` |
| `total_nursing_staff_turnover` | `DOUBLE` |
| `total_nursing_staff_turnover_footnote` | `INT` |
| `registered_nurse_turnover` | `DOUBLE` |
| `registered_nurse_turnover_footnote` | `INT` |
| `number_of_administrators_who_have_left_the_nursing_home` | `INT` |
| `administrator_turnover_footnote` | `INT` |
| `nursing_casemix_index` | `DOUBLE` |
| `nursing_casemix_index_ratio` | `DOUBLE` |
| `casemix_nurse_aide_staffing_hours_per_resident_per_day` | `DOUBLE` |
| `casemix_lpn_staffing_hours_per_resident_per_day` | `DOUBLE` |
| `casemix_rn_staffing_hours_per_resident_per_day` | `DOUBLE` |
| `casemix_total_nurse_staffing_hours_per_resident_per_day` | `DOUBLE` |
| `casemix_weekend_total_nurse_staffing_hours_per_resident_per_day` | `DOUBLE` |
| `adjusted_nurse_aide_staffing_hours_per_resident_per_day` | `DOUBLE` |
| `adjusted_lpn_staffing_hours_per_resident_per_day` | `DOUBLE` |
| `adjusted_rn_staffing_hours_per_resident_per_day` | `DOUBLE` |
| `adjusted_total_nurse_staffing_hours_per_resident_per_day` | `DOUBLE` |
| `adjusted_weekend_total_nurse_staffing_hours_per_resident_per_day` | `DOUBLE` |

Non-null hours, turnover, counts, and case-mix values must be non-negative. Footnotes explain suppressed or unavailable measures; missing measures must not be converted to zero.

#### Health-inspection cycles and penalty summaries

| Column | Type | Nullable |
|---|---|---|
| `rating_cycle_1_standard_survey_health_date` | `DATE` | Yes |
| `rating_cycle_1_standard_survey_footnote` | `INT` | Yes |
| `rating_cycle_1_total_number_of_health_deficiencies` | `INT` | Yes |
| `rating_cycle_1_number_of_standard_health_deficiencies` | `INT` | Yes |
| `rating_cycle_1_number_of_complaint_health_deficiencies` | `INT` | Yes |
| `rating_cycle_1_health_deficiency_score` | `INT` | Yes |
| `rating_cycle_1_number_of_health_revisits` | `INT` | Yes |
| `rating_cycle_1_health_revisit_score` | `INT` | Yes |
| `rating_cycle_1_total_health_score` | `INT` | Yes |
| `rating_cycle_2_standard_health_survey_date` | `DATE` | Yes |
| `rating_cycle_2_standard_survey_footnote` | `INT` | Yes |
| `rating_cycle_23_total_number_of_health_deficiencies` | `INT` | Yes |
| `rating_cycle_2_number_of_standard_health_deficiencies` | `INT` | Yes |
| `rating_cycle_23_number_of_complaint_health_deficiencies` | `INT` | Yes |
| `rating_cycle_23_health_deficiency_score` | `INT` | Yes |
| `rating_cycle_23_number_of_health_revisits` | `INT` | Yes |
| `rating_cycle_23_health_revisit_score` | `INT` | Yes |
| `rating_cycle_23_total_health_score` | `INT` | Yes |
| `total_weighted_health_survey_score` | `DOUBLE` | Yes |
| `number_of_citations_from_infection_control_inspections` | `INT` | Yes |
| `number_of_fines` | `INT` | Yes |
| `total_amount_of_fines_in_dollars` | `DECIMAL(14,2)` | Yes |
| `number_of_payment_denials` | `INT` | Yes |
| `total_number_of_penalties` | `INT` | Yes |

All counts and monetary totals must be non-negative. Do not force a missing CMS value to zero.

#### Facility lineage

Append the standard Silver lineage columns from Section 2.6. The `row_hash` business projection contains every retained facility field above, but excludes the key and lineage fields.

## 5. Quarantine and drift schemas

### 5.1 `bronze_quarantine`

| Column | Type | Nullability |
|---|---|---|
| `dataset` | `STRING` | NOT NULL |
| `dataset_id` | `STRING` | NOT NULL |
| `source_file` | `STRING` | NOT NULL |
| `source_file_sha256` | `STRING` | NOT NULL |
| `batch_id` | `STRING` | NOT NULL |
| `load_type` | `STRING` | NOT NULL |
| `raw_record` | `STRING` | Nullable |
| `corrupt_record` | `STRING` | Nullable |
| `failed_rules` | `ARRAY<STRING>` | NOT NULL |
| `load_timestamp` | `TIMESTAMP` | NOT NULL |

### 5.2 `silver_quarantine`

| Column | Type | Nullability |
|---|---|---|
| `dataset` | `STRING` | NOT NULL |
| `source_batch_id` | `STRING` | NOT NULL |
| `source_file` | `STRING` | NOT NULL |
| `source_file_sha256` | `STRING` | NOT NULL |
| `candidate_entity_key` | `STRING` | Nullable |
| `raw_record` | `STRING` | NOT NULL |
| `failed_rules` | `ARRAY<STRING>` | NOT NULL |
| `load_timestamp` | `TIMESTAMP` | NOT NULL |

### 5.3 `schema_drift_log`

| Column | Type | Nullability |
|---|---|---|
| `run_id` | `STRING` | NOT NULL |
| `batch_id` | `STRING` | Nullable |
| `dataset` | `STRING` | NOT NULL |
| `source_file` | `STRING` | NOT NULL |
| `source_file_sha256` | `STRING` | Nullable |
| `drift_type` | `STRING` | NOT NULL |
| `column_name` | `STRING` | Nullable |
| `detail` | `STRING` | NOT NULL |
| `load_timestamp` | `TIMESTAMP` | NOT NULL |

Allowed `drift_type` values are `ADDED_COLUMN`, `MISSING_COLUMN`, `RENAMED_OR_UNMAPPED_COLUMN`, and `TYPE_VALUE_FAILURE`.

## 6. Required contract tests

Before publishing a batch to Silver, test:

1. Exact required Bronze columns are present after canonical header mapping.
2. Added columns are logged and accepted only into Bronze.
3. Business-key components are non-null after cleaning and casting.
4. Candidate keys are unique within the source slice.
5. CCN, state, ZIP, tag, measure-period, rating, flag, and subtype domains meet the rules above.
6. Non-empty typed values cast successfully.
7. Counts, hours, scores, days, and monetary values are within their documented domains.
8. Address and telephone source values do not survive into Silver in clear text.
9. `row_hash` excludes lineage and operational timestamps.
10. A second run of identical content produces zero inserts and zero updates.

## 7. Known source risks

- CMS changes columns over time. The September 2026 release added three Provider Information fields (`High Performing Icon` and the two rating-cycle survey-footnote fields) and added `Survey Footnote` to Health Deficiencies. Header contracts must therefore be release-aware.
- The API may truncate long canonical names, as demonstrated by `..._on_t_4a14`; never derive a replacement name without an explicit override.
- CMS calls several identifiers “Numeric” in the PDF even though leading zeros are significant. The Silver contract intentionally overrides those labels with `STRING`.
- MDS quarter scores can be absent while a four-quarter average or footnote is present. Null is meaningful.
- Penalty rows represent two event subtypes with mutually exclusive fields. A single unconditional “all fields required” rule would reject valid data.
- Provider Information is a current snapshot. Under SCD1, prior facility attribute values are not retained in Silver; Bronze remains the audit history.
