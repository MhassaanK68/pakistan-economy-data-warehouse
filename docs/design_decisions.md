# CareWatch Silver Design Decisions

This file records the Phase 2 Step 6 contract and the Step 7 implementation
decisions in `src/carewatch/schemas.py` and `src/carewatch/silver.py`. The code
is implemented locally; Spark/Delta behavior still requires Databricks
verification.

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
  `row_hash`; Step 7 collapses them deterministically to one row.
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
- Step 7 passes only secret scope/key names into a Databricks SQL
  `secret(scope, key)` expression. It does not materialize the secret in Python
  or use `F.lit(secret_value)`, reducing exposure through driver state and Spark
  plan rendering. Secret redaction remains best-effort, so users with secret
  access must still avoid selecting the expression directly.
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

Soft deletion is disabled by default. It can run only when the operator sets
the explicit option and the input is a successful, non-empty, row-count-
validated single-artifact bulk snapshot with no quarantine/conflict rows. It
must also be the newest validated Bronze snapshot and provide a non-null maximum
`source_processing_date`; only target rows at or before that bound are eligible.
API windows, filtered backfills, reprocessing, partial files, and multi-artifact
snapshots cannot infer deletions. This conservative boundary compensates for
the current manifest not having an explicit `complete_snapshot` Boolean.

## 10. Merge, retry, and backfill

- Standard runs select Raw-to-Bronze `SUCCESS` batches without a terminal
  Bronze-to-Silver log. `SUCCESS` and `QUARANTINED_PARTIAL` are terminal;
  `FAILURE` remains pending.
- Ingestion-date filters may include all or none of a durable Bronze batch. A
  zero-row date match is reported without a terminal checkpoint; a partial
  batch match fails rather than marking an incompletely processed batch done.
- Delta `MERGE` inserts new entity keys and updates only changed/reactivated
  rows. Identical hashes are no-ops and preserve `load_timestamp`.
- A changed row can update an existing SCD1 row only when its non-null
  `source_processing_date` is at least as recent as the target date. An older
  or undated backfill cannot overwrite a current row. There is deliberately no
  historical-overwrite option.
- Quarantine append is idempotent using batch, file, candidate key, raw JSON,
  and failed rules; `load_timestamp` is excluded from that identity.
- Audit `rows_quarantined` counts input rows rejected. The run summary separately
  reports invalid input rows, conflicting input rows, unique quarantine records,
  newly written quarantine records, and exact valid duplicates collapsed.
- Step 7 never imports or calls the Bronze watermark commit function. If a
  merge commits but its log append fails, retrying is safe because the same
  business hash is a no-op.

## 11. Legacy quarantine migration

`04_migrate_silver_quarantine.py` defaults to a read-only preview. It resolves
legacy `dataset`/`batch_id` lineage against successful manifest records and
refuses missing lineage or more than one distinct source-path/hash pair. Prepare
creates a separate eight-column candidate without overwriting the five-column
table. Verify uses row counts and bidirectional `exceptAll`, so duplicate legacy
records are preserved. Activation requires a second explicit confirmation,
renames the legacy table to a retained backup, and renames the verified
candidate to the canonical name. If promotion or post-promotion verification
fails, a best-effort rename rollback restores the legacy canonical name and
retains the candidate. No path issues `DROP TABLE` or overwrite writes.

## 12. Deployed nullability verification

Step 7 compares ordered column names and Spark types, then checks nullability
through Unity Catalog `information_schema.columns`. If that metadata view is
unavailable, it uses the Spark table schema. A mismatch fails closed rather
than weakening the Step 6 contract. Some Delta/runtime combinations normalize
nullable metadata; in that case the operator must inspect `DESCRIBE TABLE
EXTENDED`, run explicit null-count queries for every non-null contract column,
and correct or recreate the table definition before Step 7 is allowed to write.

## 13. Domain uncertainty

The contract does not publish a closed list for every CMS descriptive field.
In particular, deficiency correction status is not treated as a closed domain.
The known phrase `Deficient, Provider has date of correction` requires a
correction date, while a date before the survey remains valid. New non-empty CMS
labels are retained unless they violate an explicit locked rule.

`inspection_cycle` is stored as `INT`; executable validation converts the
declarative values `1`, `2`, and `3` to the target Spark type before comparison.

## 14. Verification boundary

Local tests cover pure checkpoint, hash, duplicate/conflict, stale-update,
migration-guard, and deletion-gate decisions. They do not prove Databricks
Secrets, Unity Catalog permissions, Delta MERGE metrics, transaction history,
or concurrent writer behavior.
