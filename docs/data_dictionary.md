# CareWatch Phase 2 Data Dictionary

This dictionary reflects the explicit schemas in `src/carewatch/schemas.py`.
It documents schema design only; it does not claim that Step 7 processing or
Databricks integration has run.

## Conventions

- `PK` is the deterministic SHA-256 entity key used by the future Silver merge.
- `BK1`, `BK2`, and so on show ordered business-key components.
- Every source string is trimmed and an empty trimmed string becomes null before
  validation or casting.
- Identifiers such as CCN, ZIP, deficiency tag, measure code, fine ID, county
  code, and chain ID remain strings.
- Non-empty source values that fail a required safe cast are invalid.
- `provider_address`, `telephone_number`, and `location` never appear in Silver.
  A salted `provider_address_hash` replaces the street address.

## Bronze model retained by Step 6

Step 6 does not change the existing Bronze schemas. The four Bronze tables are
`bronze_nh_health_deficiencies` (24 source columns), `bronze_nh_penalties` (14),
`bronze_nh_provider_info` (102), and `bronze_nh_mds_quality` (23). Every source
column is nullable `STRING`. Each table appends the following non-null metadata:

| Column | Type | Purpose |
|---|---|---|
| `_dataset_id` | `STRING` | CMS dataset identifier |
| `_source_file` | `STRING` | Exact manifest-backed landing path |
| `_source_file_sha256` | `STRING` | Digest of the landed bytes |
| `_batch_id` | `STRING` | Deterministic Bronze batch identifier |
| `_load_type` | `STRING` | `full` or `incremental` |
| `_ingest_date` | `DATE` | UTC ingestion date |
| `load_timestamp` | `TIMESTAMP` | UTC row-ingestion time |

## Silver tables

### `silver_deficiency`

Business key: CCN, survey date, survey type, deficiency prefix, deficiency tag,
and inspection cycle.

| Column | Type | Nullable | Key | Notes |
|---|---|---:|---|---|
| `deficiency_key` | `STRING` | No | PK | SHA-256 of the ordered business key |
| `cms_certification_number_ccn` | `STRING` | No | BK1 | Uppercase; `^[0-9A-Z]{6}$` |
| `provider_name` | `STRING` | Yes |  | Trimmed |
| `provider_address_hash` | `STRING` | Yes |  | Salted SHA-256; no clear-text address |
| `citytown` | `STRING` | Yes |  | Trimmed |
| `state` | `STRING` | Yes |  | Uppercase two-character code |
| `zip_code` | `STRING` | Yes |  | Five digits; preserve leading zero |
| `survey_date` | `DATE` | No | BK2 | Safe cast |
| `survey_footnote` | `INT` | Yes |  | CMS footnote code |
| `survey_type` | `STRING` | No | BK3 | Trimmed CMS value |
| `deficiency_prefix` | `STRING` | No | BK4 | Uppercase |
| `deficiency_category` | `STRING` | Yes |  | Trimmed |
| `deficiency_tag_number` | `STRING` | No | BK5 | Four digits; preserve leading zero |
| `deficiency_description` | `STRING` | Yes |  | Trimmed |
| `scope_severity_code` | `STRING` | Yes |  | Uppercase A through L |
| `severity_group` | `STRING` | Yes |  | Derived `A-C`, `D-F`, `G-I`, or `J-L` |
| `deficiency_corrected` | `STRING` | Yes |  | Controlled CMS status text |
| `correction_date` | `DATE` | Yes |  | Required when correction status requires it |
| `inspection_cycle` | `INT` | No | BK6 | 1, 2, or 3 |
| `standard_deficiency` | `BOOLEAN` | Yes |  | Strict `Y`/`N` source mapping |
| `complaint_deficiency` | `BOOLEAN` | Yes |  | Strict `Y`/`N` source mapping |
| `infection_control_inspection_deficiency` | `BOOLEAN` | Yes |  | Strict `Y`/`N` source mapping |
| `citation_under_idr` | `BOOLEAN` | Yes |  | Strict `Y`/`N` source mapping |
| `citation_under_iidr` | `BOOLEAN` | Yes |  | Strict `Y`/`N` source mapping |
| `row_hash` | `STRING` | No |  | Hash of ordered retained business columns |
| `is_deleted` | `BOOLEAN` | No |  | Soft-delete marker; default false |
| `source_file` | `STRING` | No |  | Bronze source path |
| `source_file_sha256` | `STRING` | No |  | Bronze source digest |
| `source_batch_id` | `STRING` | No |  | Bronze `_batch_id` |
| `source_processing_date` | `DATE` | Yes |  | Safe cast from `processing_date` |
| `load_timestamp` | `TIMESTAMP` | No |  | UTC insert or material-change time |

Validation notes: a correction date before the survey date is allowed. Required
key fields must be non-null after normalization and safe casting.

### `silver_penalty`

Business key: CCN, penalty date, penalty type, fine ID, and payment-denial start
date. Nullable subtype fields are explicitly represented in key serialization.

| Column | Type | Nullable | Key | Notes |
|---|---|---:|---|---|
| `penalty_key` | `STRING` | No | PK | SHA-256 of the ordered business key |
| `cms_certification_number_ccn` | `STRING` | No | BK1 | Uppercase; `^[0-9A-Z]{6}$` |
| `provider_name` | `STRING` | Yes |  | Trimmed |
| `provider_address_hash` | `STRING` | Yes |  | Salted SHA-256 |
| `citytown` | `STRING` | Yes |  | Trimmed |
| `state` | `STRING` | Yes |  | Uppercase two-character code |
| `zip_code` | `STRING` | Yes |  | Five digits; preserve leading zero |
| `penalty_date` | `DATE` | No | BK2 | Safe cast |
| `penalty_type` | `STRING` | No | BK3 | `Fine` or `Payment Denial` |
| `fine_id` | `STRING` | Yes | BK4 | Required only for Fine |
| `fine_amount` | `DECIMAL(14,2)` | Yes |  | Required and non-negative only for Fine |
| `payment_denial_start_date` | `DATE` | Yes | BK5 | Required only for Payment Denial |
| `payment_denial_length_in_days` | `INT` | Yes |  | Required and non-negative only for denial |
| `row_hash` | `STRING` | No |  | Hash of ordered retained business columns |
| `is_deleted` | `BOOLEAN` | No |  | Soft-delete marker; default false |
| `source_file` | `STRING` | No |  | Bronze source path |
| `source_file_sha256` | `STRING` | No |  | Bronze source digest |
| `source_batch_id` | `STRING` | No |  | Bronze `_batch_id` |
| `source_processing_date` | `DATE` | Yes |  | Safe cast from `processing_date` |
| `load_timestamp` | `TIMESTAMP` | No |  | UTC insert or material-change time |

Subtype validation is mutually exclusive: Fine rows must not carry denial
values, and Payment Denial rows must not carry fine values.

### `silver_mds_quality`

Business key: CCN, measure code, resident type, and measure period.

| Column | Type | Nullable | Key | Notes |
|---|---|---:|---|---|
| `mds_key` | `STRING` | No | PK | SHA-256 of the ordered business key |
| `cms_certification_number_ccn` | `STRING` | No | BK1 | Uppercase; `^[0-9A-Z]{6}$` |
| `provider_name` | `STRING` | Yes |  | Trimmed |
| `provider_address_hash` | `STRING` | Yes |  | Salted SHA-256 |
| `citytown` | `STRING` | Yes |  | Trimmed |
| `state` | `STRING` | Yes |  | Uppercase two-character code |
| `zip_code` | `STRING` | Yes |  | Five digits; preserve leading zero |
| `measure_code` | `STRING` | No | BK2 | Three-character identifier |
| `measure_description` | `STRING` | Yes |  | Trimmed |
| `resident_type` | `STRING` | No | BK3 | `Long Stay` or `Short Stay` |
| `q1_measure_score` | `DOUBLE` | Yes |  | 0 through 100 when present |
| `footnote_for_q1_measure_score` | `INT` | Yes |  | CMS footnote code |
| `q2_measure_score` | `DOUBLE` | Yes |  | 0 through 100 when present |
| `footnote_for_q2_measure_score` | `INT` | Yes |  | CMS footnote code |
| `q3_measure_score` | `DOUBLE` | Yes |  | 0 through 100 when present |
| `footnote_for_q3_measure_score` | `INT` | Yes |  | CMS footnote code |
| `q4_measure_score` | `DOUBLE` | Yes |  | 0 through 100 when present |
| `footnote_for_q4_measure_score` | `INT` | Yes |  | CMS footnote code |
| `four_quarter_average_score` | `DOUBLE` | Yes |  | 0 through 100 when present |
| `footnote_for_four_quarter_average_score` | `INT` | Yes |  | CMS footnote code |
| `used_in_quality_measure_five_star_rating` | `BOOLEAN` | No |  | Strict `Y`/`N` source mapping |
| `measure_period` | `STRING` | No | BK4 | Pattern `YYYYQn-YYYYQn` |
| `row_hash` | `STRING` | No |  | Hash of ordered retained business columns |
| `is_deleted` | `BOOLEAN` | No |  | Soft-delete marker; default false |
| `source_file` | `STRING` | No |  | Bronze source path |
| `source_file_sha256` | `STRING` | No |  | Bronze source digest |
| `source_batch_id` | `STRING` | No |  | Bronze `_batch_id` |
| `source_processing_date` | `DATE` | Yes |  | Safe cast from `processing_date` |
| `load_timestamp` | `TIMESTAMP` | No |  | UTC insert or material-change time |

Quarterly score nulls are meaningful and are not filled from the four-quarter
average.

### `silver_facility`

Business key: CCN. Phase 2 uses SCD Type 1 and therefore has no `valid_from`,
`valid_to`, or `is_current` fields.

| Column | Type | Nullable | Key | Notes |
|---|---|---:|---|---|
| `facility_key` | `STRING` | No | PK | SHA-256 of CCN |
| `cms_certification_number_ccn` | `STRING` | No | BK1 | Uppercase; `^[0-9A-Z]{6}$` |
| `provider_name` | `STRING` | Yes |  | Trimmed |
| `provider_address_hash` | `STRING` | Yes |  | Salted SHA-256 |
| `citytown` | `STRING` | Yes |  | Trimmed |
| `state` | `STRING` | Yes |  | Uppercase two-character code |
| `zip_code` | `STRING` | Yes |  | Five digits; preserve leading zero |
| `provider_ssa_county_code` | `STRING` | Yes |  | Identifier; preserve formatting |
| `countyparish` | `STRING` | Yes |  | Trimmed |
| `urban` | `BOOLEAN` | Yes |  | Strict `Y`/`N` source mapping |
| `ownership_type` | `STRING` | Yes |  | Trimmed CMS category |
| `provider_type` | `STRING` | Yes |  | Trimmed CMS category |
| `provider_resides_in_hospital` | `BOOLEAN` | Yes |  | Strict `Y`/`N` source mapping |
| `legal_business_name` | `STRING` | Yes |  | Trimmed |
| `date_first_approved_to_provide_medicare_and_medicaid_services` | `DATE` | Yes |  | Safe cast |
| `chain_name` | `STRING` | Yes |  | Trimmed |
| `chain_id` | `STRING` | Yes |  | Identifier; preserve formatting |
| `special_focus_status` | `STRING` | Yes |  | Null, `SFF`, or `SFF Candidate` |
| `with_a_resident_and_family_council` | `STRING` | Yes |  | Resident, Family, Both, or None |
| `automatic_sprinkler_systems_in_all_required_areas` | `STRING` | Yes |  | Yes, Partial, No, or Data Not Available |
| `latitude` | `DOUBLE` | Yes |  | -90 through 90 |
| `longitude` | `DOUBLE` | Yes |  | -180 through 180 |
| `geocoding_footnote` | `INT` | Yes |  | CMS footnote code |
| `number_of_certified_beds` | `INT` | Yes |  | Non-negative |
| `average_number_of_residents_per_day` | `DOUBLE` | Yes |  | Non-negative |
| `average_number_of_residents_per_day_footnote` | `INT` | Yes |  | CMS footnote code |
| `number_of_facilities_in_chain` | `INT` | Yes |  | Non-negative |
| `chain_average_overall_5star_rating` | `DOUBLE` | Yes |  | 1 through 5 |
| `chain_average_health_inspection_rating` | `DOUBLE` | Yes |  | 1 through 5 |
| `chain_average_staffing_rating` | `DOUBLE` | Yes |  | 1 through 5 |
| `chain_average_qm_rating` | `DOUBLE` | Yes |  | 1 through 5 |
| `continuing_care_retirement_community` | `BOOLEAN` | Yes |  | Strict `Y`/`N` source mapping |
| `abuse_icon` | `BOOLEAN` | Yes |  | Strict `Y`/`N` source mapping |
| `high_performing_icon` | `BOOLEAN` | Yes |  | Strict `Y`/`N` source mapping |
| `most_recent_health_inspection_more_than_2_years_ago` | `BOOLEAN` | Yes |  | Strict `Y`/`N` source mapping |
| `provider_changed_ownership_in_last_12_months` | `BOOLEAN` | Yes |  | Strict `Y`/`N` source mapping |
| `overall_rating` | `INT` | Yes |  | 1 through 5 |
| `overall_rating_footnote` | `INT` | Yes |  | CMS footnote code |
| `health_inspection_rating` | `INT` | Yes |  | 1 through 5 |
| `health_inspection_rating_footnote` | `INT` | Yes |  | CMS footnote code |
| `qm_rating` | `INT` | Yes |  | 1 through 5 |
| `qm_rating_footnote` | `INT` | Yes |  | CMS footnote code |
| `longstay_qm_rating` | `INT` | Yes |  | 1 through 5 |
| `longstay_qm_rating_footnote` | `INT` | Yes |  | CMS footnote code |
| `shortstay_qm_rating` | `INT` | Yes |  | 1 through 5 |
| `shortstay_qm_rating_footnote` | `INT` | Yes |  | CMS footnote code |
| `staffing_rating` | `INT` | Yes |  | 1 through 5 |
| `staffing_rating_footnote` | `INT` | Yes |  | CMS footnote code |
| `reported_staffing_footnote` | `INT` | Yes |  | CMS footnote code |
| `physical_therapist_staffing_footnote` | `INT` | Yes |  | CMS footnote code |
| `reported_nurse_aide_staffing_hours_per_resident_per_day` | `DOUBLE` | Yes |  | Non-negative |
| `reported_lpn_staffing_hours_per_resident_per_day` | `DOUBLE` | Yes |  | Non-negative |
| `reported_rn_staffing_hours_per_resident_per_day` | `DOUBLE` | Yes |  | Non-negative |
| `reported_licensed_staffing_hours_per_resident_per_day` | `DOUBLE` | Yes |  | Non-negative |
| `reported_total_nurse_staffing_hours_per_resident_per_day` | `DOUBLE` | Yes |  | Non-negative |
| `total_number_of_nurse_staff_hours_per_resident_per_day_on_t_4a14` | `DOUBLE` | Yes |  | Non-negative |
| `registered_nurse_hours_per_resident_per_day_on_the_weekend` | `DOUBLE` | Yes |  | Non-negative |
| `reported_physical_therapist_staffing_hours_per_resident_per_day` | `DOUBLE` | Yes |  | Non-negative |
| `total_nursing_staff_turnover` | `DOUBLE` | Yes |  | Non-negative |
| `total_nursing_staff_turnover_footnote` | `INT` | Yes |  | CMS footnote code |
| `registered_nurse_turnover` | `DOUBLE` | Yes |  | Non-negative |
| `registered_nurse_turnover_footnote` | `INT` | Yes |  | CMS footnote code |
| `number_of_administrators_who_have_left_the_nursing_home` | `INT` | Yes |  | Non-negative |
| `administrator_turnover_footnote` | `INT` | Yes |  | CMS footnote code |
| `nursing_casemix_index` | `DOUBLE` | Yes |  | Non-negative |
| `nursing_casemix_index_ratio` | `DOUBLE` | Yes |  | Non-negative |
| `casemix_nurse_aide_staffing_hours_per_resident_per_day` | `DOUBLE` | Yes |  | Non-negative |
| `casemix_lpn_staffing_hours_per_resident_per_day` | `DOUBLE` | Yes |  | Non-negative |
| `casemix_rn_staffing_hours_per_resident_per_day` | `DOUBLE` | Yes |  | Non-negative |
| `casemix_total_nurse_staffing_hours_per_resident_per_day` | `DOUBLE` | Yes |  | Non-negative |
| `casemix_weekend_total_nurse_staffing_hours_per_resident_per_day` | `DOUBLE` | Yes |  | Non-negative |
| `adjusted_nurse_aide_staffing_hours_per_resident_per_day` | `DOUBLE` | Yes |  | Non-negative |
| `adjusted_lpn_staffing_hours_per_resident_per_day` | `DOUBLE` | Yes |  | Non-negative |
| `adjusted_rn_staffing_hours_per_resident_per_day` | `DOUBLE` | Yes |  | Non-negative |
| `adjusted_total_nurse_staffing_hours_per_resident_per_day` | `DOUBLE` | Yes |  | Non-negative |
| `adjusted_weekend_total_nurse_staffing_hours_per_resident_per_day` | `DOUBLE` | Yes |  | Non-negative |
| `rating_cycle_1_standard_survey_health_date` | `DATE` | Yes |  | Safe cast |
| `rating_cycle_1_standard_survey_footnote` | `INT` | Yes |  | CMS footnote code |
| `rating_cycle_1_total_number_of_health_deficiencies` | `INT` | Yes |  | Non-negative |
| `rating_cycle_1_number_of_standard_health_deficiencies` | `INT` | Yes |  | Non-negative |
| `rating_cycle_1_number_of_complaint_health_deficiencies` | `INT` | Yes |  | Non-negative |
| `rating_cycle_1_health_deficiency_score` | `INT` | Yes |  | Non-negative |
| `rating_cycle_1_number_of_health_revisits` | `INT` | Yes |  | Non-negative |
| `rating_cycle_1_health_revisit_score` | `INT` | Yes |  | Non-negative |
| `rating_cycle_1_total_health_score` | `INT` | Yes |  | Non-negative |
| `rating_cycle_2_standard_health_survey_date` | `DATE` | Yes |  | Safe cast |
| `rating_cycle_2_standard_survey_footnote` | `INT` | Yes |  | CMS footnote code |
| `rating_cycle_23_total_number_of_health_deficiencies` | `INT` | Yes |  | Non-negative |
| `rating_cycle_2_number_of_standard_health_deficiencies` | `INT` | Yes |  | Non-negative |
| `rating_cycle_23_number_of_complaint_health_deficiencies` | `INT` | Yes |  | Non-negative |
| `rating_cycle_23_health_deficiency_score` | `INT` | Yes |  | Non-negative |
| `rating_cycle_23_number_of_health_revisits` | `INT` | Yes |  | Non-negative |
| `rating_cycle_23_health_revisit_score` | `INT` | Yes |  | Non-negative |
| `rating_cycle_23_total_health_score` | `INT` | Yes |  | Non-negative |
| `total_weighted_health_survey_score` | `DOUBLE` | Yes |  | Non-negative |
| `number_of_citations_from_infection_control_inspections` | `INT` | Yes |  | Non-negative |
| `number_of_fines` | `INT` | Yes |  | Non-negative |
| `total_amount_of_fines_in_dollars` | `DECIMAL(14,2)` | Yes |  | Non-negative |
| `number_of_payment_denials` | `INT` | Yes |  | Non-negative |
| `total_number_of_penalties` | `INT` | Yes |  | Non-negative |
| `row_hash` | `STRING` | No |  | Hash of every retained facility business field |
| `is_deleted` | `BOOLEAN` | No |  | Soft-delete marker; default false |
| `source_file` | `STRING` | No |  | Bronze source path |
| `source_file_sha256` | `STRING` | No |  | Bronze source digest |
| `source_batch_id` | `STRING` | No |  | Bronze `_batch_id` |
| `source_processing_date` | `DATE` | Yes |  | Safe cast from `processing_date` |
| `load_timestamp` | `TIMESTAMP` | No |  | UTC insert or material-change time |

## Silver quarantine

| Column | Type | Nullable | Purpose |
|---|---|---:|---|
| `dataset` | `STRING` | No | Registry dataset name |
| `source_batch_id` | `STRING` | No | Rejected Bronze batch |
| `source_file` | `STRING` | No | Source lineage |
| `source_file_sha256` | `STRING` | No | Source digest |
| `candidate_entity_key` | `STRING` | Yes | Key when it could be computed safely |
| `raw_record` | `STRING` | No | Raw source columns serialized as JSON |
| `failed_rules` | `ARRAY<STRING>` | No | All actionable failure identifiers |
| `load_timestamp` | `TIMESTAMP` | No | UTC quarantine time |

## Cross-table validation and privacy rules

- CCN is uppercase and matches `^[0-9A-Z]{6}$`.
- Non-null state is an uppercase two-character code and non-null ZIP is five
  digits.
- All business-key components required by the dataset must be non-null after
  normalization and casting.
- Exact duplicate business rows may collapse to one. Different business hashes
  for one entity key are a conflict and must not be silently selected.
- Entity keys and `row_hash` use ordered JSON with explicit null fields.
- `row_hash` excludes entity key, lineage, and operational timestamps.
- Clear-text address, telephone, location, and the address salt never survive
  into Silver.
