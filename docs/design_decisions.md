# CareWatch Silver Design Decisions

This file records the Phase 2 Step 6 decisions implemented in
`src/carewatch/schemas.py`. It defines the contract that the future Step 7
pipeline must follow; it does not claim that Bronze-to-Silver processing has
already been implemented or run.

## 1. Contract ownership

- `docs/bronze_silver_schema_contract.md` is the locked human-readable source
  contract.
- `src/carewatch/schemas.py` contains the matching executable PySpark
  `StructType` definitions and reusable `SilverValidationSpec` rule metadata.
- `docs/data_dictionary.md` is the review-friendly dictionary for the same
  implemented schemas.
- A change to a column, type, nullability rule, business key, or validation
  rule must update all three artifacts together.

## 2. Silver tables and keys

| Dataset | Silver table | Entity key | Ordered business key |
|---|---|---|---|
| Health Deficiencies | `silver_deficiency` | `deficiency_key` | `cms_certification_number_ccn`, `survey_date`, `survey_type`, `deficiency_prefix`, `deficiency_tag_number`, `inspection_cycle` |
| Penalties | `silver_penalty` | `penalty_key` | `cms_certification_number_ccn`, `penalty_date`, `penalty_type`, `fine_id`, `payment_denial_start_date` |
| Provider Information | `silver_facility` | `facility_key` | `cms_certification_number_ccn` |
| MDS Quality Measures | `silver_mds_quality` | `mds_key` | `cms_certification_number_ccn`, `measure_code`, `resident_type`, `measure_period` |

The September 2026 complete local extracts were profiled before these keys were
accepted:

| Dataset | Rows | Distinct candidate keys | Null-key rows |
|---|---:|---:|---:|
| Health Deficiencies | 418,947 | 418,947 | 0 |
| Penalties | 15,419 | 15,419 | 0 |
| Provider Information | 14,690 | 14,690 | 0 |
| MDS Quality Measures | 249,730 | 249,730 | 0 |

These results are evidence for that snapshot, not a permanent assumption. Each
new complete snapshot must test required key components and key uniqueness
before publication to Silver.

## 3. Normalization and nulls

- Trim every source string before validation.
- Convert a trimmed empty string to `NULL`.
- Uppercase CCN, state, deficiency prefix, and severity code where applicable.
- Preserve CCN, ZIP, county code, deficiency tag, measure code, fine ID, and
  chain ID as strings so leading zeros and identifier formatting survive.
- Use safe casting for dates, integers, decimals, and doubles. A non-empty
  value that cannot be cast is invalid; it must not silently become a valid
  null.
- Convert only strict `Y` and `N` source values to booleans. Other non-empty
  values are invalid.
- Missing numeric values remain null and are never imputed as zero.
- Operational timestamps are UTC. Source calendar values use `DATE`.

## 4. Deterministic hashes

Entity keys use SHA-256 over ordered JSON of normalized business-key columns.
The serialization must retain explicit null fields so null and empty values do
not collide. Required business-key fields are validated before the key is
accepted.

`row_hash` uses SHA-256 over ordered JSON of retained business columns. It
excludes the entity key, all Silver lineage columns, and operational
timestamps. This makes an unchanged business record keep the same hash even
when it is read in another batch.

## 5. Duplicate policy

- Exact duplicates have the same normalized entity key and the same business
  `row_hash`; a future pipeline may collapse them deterministically to one row.
- If the same entity key has more than one business hash in one source slice,
  the rows conflict. The pipeline must not silently choose the newest or first
  row. It must report a contract conflict and keep the ambiguous records out of
  the accepted merge input.
- A new full snapshot that violates a documented candidate key is a contract
  failure requiring investigation, not permission to weaken the key silently.

## 6. Dataset-specific validation

### Health Deficiencies

- Required: CCN, survey date, survey type, deficiency prefix, deficiency tag,
  and inspection cycle.
- CCN must match `^[0-9A-Z]{6}$`; state must be two uppercase characters; ZIP
  must be five digits; the deficiency tag must be four digits.
- Inspection cycle is 1, 2, or 3. Severity is A through L and derives one of
  `A-C`, `D-F`, `G-I`, or `J-L`.
- Five indicator columns use strict `Y`/`N` mapping.
- Correction-date presence must agree with the correction status. A correction
  date before the survey date is allowed, including Past Non-Compliance.

### Penalties

- Required for every row: CCN, penalty date, and penalty type.
- `Fine` requires `fine_id` and a non-negative `fine_amount`; payment-denial
  fields must be null.
- `Payment Denial` requires a start date and non-negative denial length; fine
  fields must be null.
- Nullable subtype components remain explicit nulls in key serialization.

### Provider Information

- CCN is required and is the SCD Type 1 business key.
- Phase 2 intentionally does not add `valid_from`, `valid_to`, or `is_current`.
- Ratings must be 1 through 5. Latitude is -90 through 90 and longitude is
  -180 through 180.
- Counts, hours, turnover, case-mix values, scores, days, and monetary totals
  identified by the contract must be non-negative.
- Special-focus, resident/family-council, and sprinkler fields use the domains
  listed in the schema contract. Ownership and provider categories remain
  trimmed CMS text because the locked contract does not define a closed list.

### MDS Quality Measures

- Required: CCN, measure code, resident type, measure period, and the five-star
  usage flag.
- Resident type is `Long Stay` or `Short Stay`.
- Measure period matches `YYYYQn-YYYYQn` and measure code is a three-character
  identifier.
- Non-null quarterly and four-quarter scores must be between 0 and 100.
- Quarterly nulls are meaningful and are not filled from the average.

## 7. Privacy

- `telephone_number` is not present in Silver.
- Clear-text `provider_address` is not present in Silver.
- `location` is not present because it repeats the street address.
- `provider_address_hash` is nullable and is SHA-256 over the normalized
  address plus a salt supplied through Databricks Secrets.
- The salt must never be stored in the repository, logs, table properties, or
  notebook widgets as a clear-text value.
- City, state, and ZIP remain available for geographic analysis.

## 8. Lineage and quarantine

Every accepted Silver table includes `row_hash`, `is_deleted`, `source_file`,
`source_file_sha256`, `source_batch_id`, `source_processing_date`, and
`load_timestamp`. All except `source_processing_date` are non-null.

The locked `silver_quarantine` schema records dataset, source batch, source
file, source digest, optional candidate entity key, raw JSON, all failed rules,
and load timestamp. The earlier five-column placeholder is recognized by setup
only to avoid breaking Step 5; setup does not migrate or write to it.

## 9. Snapshot deletion boundary

The schemas contain `is_deleted`, but Step 6 performs no deletion processing.
A future Step 7 implementation may infer soft deletion only from a complete,
validated bulk snapshot and only within its explicit scope. API windows,
partial files, test subsets, and manually supplied files may never infer
deletions.

## 10. Deliberate Step 6 boundary

This step defines schemas and rules only. It does not select Bronze batches,
cast DataFrames, write quarantine records, run Delta `MERGE`, update rows,
advance watermarks, or execute any Databricks notebook.
