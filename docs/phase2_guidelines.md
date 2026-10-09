# Phase 2 Guide: Build the Bronze and Silver Pipeline (PySpark + Delta)

This guide tells you exactly what to build, in what order, and what evidence to collect for the submission. It assumes the Phase 1 design (CareWatch: CMS nursing-home data) and that your Phase 1 repo and sample files exist.

**Estimated effort:** 4 to 6 working days for two people. The riskiest parts are Step 5 (Bronze with drift handling) and Step 7 (Silver MERGE). Start with **one dataset end to end** (Health Deficiencies), then repeat for the others.

---

## 0. What the instructor wants, mapped to the steps

| # | Instructor requirement | Where you satisfy it | Evidence to keep |
|---|---|---|---|
| 1 | Databricks (Community/Free) or Azure workspace | Step 1 | Screenshot of workspace + repo linked |
| 2 | All code committed to GitHub, continuously | Step 1, Step 12 | Commit history (both partners) |
| 3 | Data dictionary for Bronze and Silver (names, types, primary keys) | Step 6, Step 11 | `docs/data_dictionary.md`, README section |
| 4 | **No `inferSchema`**; explicit `StructType`/`StructField` before reading | Step 3, Step 5 | `schemas.py`; grep shows no `inferSchema` |
| 5 | Strict types and casting into Silver | Step 7 | Silver `DESCRIBE` output |
| 6 | `load_timestamp` on **every record of every table** | Step 3, 5, 7 | `SELECT load_timestamp` on each table |
| 7 | Idempotent: rerun on same data gives no duplicates; **MERGE INTO** | Step 7 | Before/after counts + log rows with 0 inserted/updated |
| 8 | Parameterized backfills (date / batch id / path), no hard-coded "today" | Step 2, Step 9 | Execution guide in README + a backfill demo run |
| 9 | Schema drift: evolve (`mergeSchema`) or quarantine, never crash the batch | Step 5, Step 7, Step 8 | Drift demo: new column + changed type |
| 10 | Logging tables with layer, parameter/file, start/end, status, rows inserted/updated | Step 4 | `pipeline_execution_logs` query output |
| 11 | README with Bronze + Silver models and execution guide | Step 11 | README |

Keep this table in your README as a "requirements traceability" section. Graders love it.

---

## 1. Decisions to lock before writing code (30 min)

Decide these with your partner and write them in `docs/design_decisions.md`:

1. **Platform.** Your Phase 1 proposal says Databricks Free Edition. The instructor wrote "Community Edition". Free Edition replaced Community Edition, but **send your instructor one line to confirm it is acceptable** (or use Azure). Differences that matter: Free Edition uses Unity Catalog (`catalog.schema.table`, files in `/Volumes/...`) and serverless compute; Community Edition used `hive_metastore` and DBFS paths. Your code must take catalog/schema/paths as parameters so it works on either.
2. **Datasets in scope for Phase 2.** Recommended: Health Deficiencies, Provider Information, Penalties, MDS Quality Measures (the four from the Phase 1 sample plan). Build them with one config-driven pipeline (Step 3) so each extra dataset costs about an hour, not a day.
3. **Primary keys** (to be validated in Step 6):
   - Deficiencies: CCN + survey date + survey type + deficiency prefix + tag number + inspection cycle (candidate, must be proven unique by profiling)
   - Penalties: CCN + penalty date + penalty type + fine id + payment-denial start date
   - Provider Information: CCN
   - MDS Quality Measures: CCN + measure code + resident type + measure period (candidate)
4. **Silver facility history.** Phase 1 promised SCD Type 2 for facilities. It is more work. Safe path: first implement a plain upsert (SCD Type 1) so every requirement is met, then add SCD2 only if time permits. If you do not do SCD2, **update the README and tell the instructor** rather than leaving the Phase 1 promise silently broken.
5. **Who does what** (suggested): Partner A = config, schemas, Raw-to-Bronze, drift handling. Partner B = logging framework, Bronze-to-Silver, MERGE, README/data dictionary. Do the logging helper (Step 4) first, because both pipelines call it.

---

## 2. Step 1: Workspace and repo setup (1 to 2 hours)

1. Open your Databricks workspace. Create a catalog/schema (or use `default`) such as `carewatch.pipeline`.
2. Create three storage areas (UC Volumes, or DBFS folders on Community Edition):
   ```
   /Volumes/<catalog>/<schema>/landing/full_load/
   /Volumes/<catalog>/<schema>/landing/incremental/
   /Volumes/<catalog>/<schema>/landing/archive/
   ```
3. Upload your Phase 1 sample part files into `landing/full_load/` and the JSON files into `landing/incremental/`. **For development, upload only 1 or 2 part files**, not all 200+ MB. Save compute quota.
4. Link GitHub: Workspace > Create > Git folder, paste your repo URL (needs a GitHub personal access token). If Git folders are not available on your plan, develop in notebooks and export/commit them manually after every session; the instructor wants a visible history either way.
5. Repo layout to create now:

```
carewatch-medallion/
├── notebooks/
│   ├── 00_setup_tables.py          # creates schemas + log tables (run once)
│   ├── 01_raw_to_bronze.py         # parameterized
│   ├── 02_bronze_to_silver.py      # parameterized
│   └── 99_demo_idempotency_drift_backfill.py
├── src/carewatch/
│   ├── config.py                   # dataset registry + table names
│   ├── schemas.py                  # explicit StructTypes (Bronze) + Silver typing rules
│   ├── audit.py                    # logging helper
│   ├── drift.py                    # header/schema drift detection
│   ├── bronze.py
│   └── silver.py
├── docs/ (data_dictionary.md, design_decisions.md, evidence/)
├── tests/
└── README.md
```

Notebooks can import `src/carewatch` after `sys.path.append("/Workspace/.../carewatch-medallion/src")`. If imports are awkward on your plan, use `%run ./helpers` notebooks instead; the requirement is explicit schemas and parameters, not a package.

---

## 3. Step 2: Parameters and config (1 hour)

**Rule: no path, date or table name is hard-coded inside a function.** Every notebook starts with widgets:

```python
# notebooks/01_raw_to_bronze.py  (top cell)
dbutils.widgets.text("dataset", "health_deficiencies")        # key in DATASETS registry
dbutils.widgets.dropdown("load_type", "full", ["full", "incremental"])
dbutils.widgets.text("source_path", "")                         # folder or glob; empty = default folder for load_type
dbutils.widgets.text("batch_id", "")                            # optional override; empty = derived per file
dbutils.widgets.text("catalog", "carewatch")
dbutils.widgets.text("schema", "pipeline")
dbutils.widgets.text("landing_root", "/Volumes/carewatch/pipeline/landing")

P = {k: dbutils.widgets.get(k) for k in
     ["dataset", "load_type", "source_path", "batch_id", "catalog", "schema", "landing_root"]}
```

`config.py` holds a **dataset registry** so the same code serves every dataset:

```python
# src/carewatch/config.py
DATASETS = {
    "health_deficiencies": {
        "bronze_table": "bronze_nh_health_deficiencies",
        "silver_table": "silver_deficiency",
        "key_cols": ["cms_certification_number_ccn", "survey_date", "survey_type",
                     "deficiency_prefix", "deficiency_tag_number", "inspection_cycle"],
        "date_col": "survey_date",
    },
    "penalties": {
        "bronze_table": "bronze_nh_penalties",
        "silver_table": "silver_penalty",
        "key_cols": ["cms_certification_number_ccn", "penalty_date", "penalty_type",
                     "fine_id", "payment_denial_start_date"],
        "date_col": "penalty_date",
    },
    # provider_info, mds_quality: same shape
}

def fq(p, name):
    """fully qualified table name from parameters"""
    return f"{p['catalog']}.{p['schema']}.{name}"
```

Default folder when `source_path` is empty: `<landing_root>/full_load` or `<landing_root>/incremental` based on `load_type`. That one rule is the difference between a standard run and a backfill (Step 9).

---

## 4. Step 3: Explicit schemas for Bronze (2 to 3 hours)

**Bronze stores everything as strings** (raw, unmodified), plus metadata. Typing happens in Silver.

1. Get the exact column list for each dataset from the API (one row is enough):
   ```
   https://data.cms.gov/provider-data/api/1/datastore/query/r5ix-sfxw/0?limit=1&schema=false
   ```
   The JSON keys are your canonical (snake_case) column names. Do the same for `4pq5-n9py` (about 100 columns), `g6vv-u9sr`, `djen-97ju`.
2. Put them in `schemas.py` and build `StructType` objects. **Never call `inferSchema`.**

```python
# src/carewatch/schemas.py
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DateType

DEFICIENCY_COLS = [
    "cms_certification_number_ccn", "provider_name", "provider_address", "citytown", "state",
    "zip_code", "survey_date", "survey_type", "deficiency_prefix", "deficiency_category",
    "deficiency_tag_number", "deficiency_description", "scope_severity_code",
    "deficiency_corrected", "correction_date", "inspection_cycle", "standard_deficiency",
    "complaint_deficiency", "infection_control_inspection_deficiency", "citation_under_idr",
    "citation_under_iidr", "location", "processing_date",
]

def string_struct(cols):
    return StructType([StructField(c, StringType(), True) for c in cols])

# Metadata columns added by YOU in Bronze (every Bronze table has all of them)
BRONZE_META = StructType([
    StructField("_source_file",  StringType(),    False),
    StructField("_batch_id",     StringType(),    False),
    StructField("_load_type",    StringType(),    False),
    StructField("_ingest_date",  DateType(),      False),   # date part, handy for backfill filters
    StructField("load_timestamp", TimestampType(), False),  # REQUIRED by the instructor
])
```

3. **Important: the bulk CSV headers are human-readable** ("Survey Date", "City/Town"), while the API JSON uses snake_case (`survey_date`, `citytown`). The easy way to match both to your canonical names is a normalising key that strips everything except letters and digits:

```python
import re
def key(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())
# key("City/Town") == key("citytown") ; key("CMS Certification Number (CCN)") == key("cms_certification_number_ccn")
```
   A few Provider Information headers will not match this way (CMS truncated one API name to `..._on_t_4a14`). Print the mismatches once and add an explicit override dictionary for them. Do not skip this; the bulk files are your full load.

---

## 5. Step 4: Logging framework (2 hours) **build this before the pipelines**

Create these tables in `00_setup_tables.py` (Delta):

**`pipeline_execution_logs`** (one row per file/table processed, per layer, per attempt)

| Column | Type | Notes |
|---|---|---|
| run_id | string | uuid per notebook run |
| batch_id | string | ties Bronze batch to Silver processing |
| pipeline_layer | string | `Raw-to-Bronze`, `Bronze-to-Silver` |
| dataset | string | e.g. `health_deficiencies` |
| load_type | string | `full` / `incremental` |
| parameter_processed | string | file path (Raw-to-Bronze) or batch id/date range (Bronze-to-Silver) |
| start_time, end_time | timestamp | UTC |
| status | string | `SUCCESS` / `FAILURE` / `QUARANTINED_PARTIAL` |
| rows_read | long | |
| rows_inserted | long | |
| rows_updated | long | 0 for Bronze appends |
| rows_deleted | long | soft-deleted count |
| rows_quarantined | long | |
| error_message | string | first 2000 chars |
| load_timestamp | timestamp | when the log row was written (keeps the "every table" rule true) |

Also create **`schema_drift_log`** (run_id, dataset, source_file, drift_type, column_name, detail, load_timestamp) and **`silver_quarantine`** (dataset, batch_id, raw_record string, failed_rules array<string>, load_timestamp).

The helper everything calls:

```python
# src/carewatch/audit.py
from contextlib import contextmanager
from datetime import datetime, timezone
from pyspark.sql.types import (StructType, StructField, StringType, LongType, TimestampType)

LOG_SCHEMA = StructType([
    StructField("run_id", StringType()), StructField("batch_id", StringType()),
    StructField("pipeline_layer", StringType()), StructField("dataset", StringType()),
    StructField("load_type", StringType()), StructField("parameter_processed", StringType()),
    StructField("start_time", TimestampType()), StructField("end_time", TimestampType()),
    StructField("status", StringType()), StructField("rows_read", LongType()),
    StructField("rows_inserted", LongType()), StructField("rows_updated", LongType()),
    StructField("rows_deleted", LongType()), StructField("rows_quarantined", LongType()),
    StructField("error_message", StringType()), StructField("load_timestamp", TimestampType()),
])

@contextmanager
def audited(spark, log_table, run_id, layer, dataset, load_type, parameter, batch_id=""):
    rec = {"run_id": run_id, "batch_id": batch_id, "pipeline_layer": layer, "dataset": dataset,
           "load_type": load_type, "parameter_processed": parameter,
           "start_time": datetime.now(timezone.utc), "status": "SUCCESS",
           "rows_read": 0, "rows_inserted": 0, "rows_updated": 0, "rows_deleted": 0,
           "rows_quarantined": 0, "error_message": None}
    try:
        yield rec                      # caller fills in the row counts
        if rec["rows_quarantined"] > 0 and rec["status"] == "SUCCESS":
            rec["status"] = "QUARANTINED_PARTIAL"
    except Exception as exc:           # log the failure, then let the caller decide
        rec["status"] = "FAILURE"
        rec["error_message"] = str(exc)[:2000]
        raise
    finally:                           # runs on success AND failure
        rec["end_time"] = datetime.now(timezone.utc)
        rec["load_timestamp"] = rec["end_time"]
        spark.createDataFrame([rec], schema=LOG_SCHEMA).write.mode("append").saveAsTable(log_table)
```

Usage pattern (one log row per file):

```python
for f in files:
    try:
        with audited(spark, fq(P, "pipeline_execution_logs"), run_id,
                     "Raw-to-Bronze", P["dataset"], P["load_type"], f) as rec:
            n = load_one_file(f)            # returns rows written
            rec["rows_read"] = rec["rows_inserted"] = n
    except Exception:
        continue                            # one bad file must not kill the batch
```

---

## 6. Step 5: Raw to Bronze (1 to 1.5 days)

For **each file** in the folder (loop per file so every file gets its own log row):

1. **Read the header** and compare with the expected columns (drift check):

```python
import csv

def read_header(spark, path):
    first = spark.read.text(path).limit(1).collect()[0][0]
    return next(csv.reader([first.lstrip("﻿")]))

def check_drift(actual_header, expected_cols):
    exp = {key(c): c for c in expected_cols}
    act = {key(h): h for h in actual_header}
    missing = [exp[k] for k in exp if k not in act]
    extra   = [act[k] for k in act if k not in exp]
    return missing, extra
```
2. **Decide what to do**:
   - `missing` columns: the file is non-conforming. Log a `schema_drift_log` row, mark the file `FAILURE` (or `QUARANTINED_PARTIAL`), **move on to the next file**. Do not crash the run.
   - `extra` columns (source added a column): build the read schema with the extra columns appended as `StringType` in header order, and write with `mergeSchema=true` so the Bronze table evolves. Log the drift.
3. **Read with the explicit schema** (header row skipped, columns matched by position, so build the schema in *header order*):

```python
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

def read_csv_file(spark, path, ordered_cols):
    schema = StructType([StructField(c, StringType(), True) for c in ordered_cols] +
                        [StructField("_corrupt_record", StringType(), True)])
    return (spark.read.format("csv")
            .option("header", "true")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .option("multiLine", "true")
            .option("quote", '"').option("escape", '"')
            .schema(schema)                      # explicit, NOT inferred
            .load(path))
```
   For the incremental **JSON** files (structure `{"_meta": {...}, "results": [ {...}, ... ]}`), read with an explicit schema `StructType([StructField("_meta", ...), StructField("results", ArrayType(string_struct(cols)))])` and `multiLine=true`, then `F.explode("results")`.
4. **Add the metadata columns**, including `load_timestamp`:

```python
def add_bronze_meta(df, path, batch_id, load_type):
    return (df.withColumn("_source_file", F.lit(path))
              .withColumn("_batch_id", F.lit(batch_id))
              .withColumn("_load_type", F.lit(load_type))
              .withColumn("_ingest_date", F.current_date())
              .withColumn("load_timestamp", F.current_timestamp()))
```
   (Use `F.lit(path)` from the path you looped over; do not use `input_file_name()` under Unity Catalog.)
5. **Quarantine bad CSV rows**: rows where `_corrupt_record IS NOT NULL` go to a Bronze quarantine table (`bronze_quarantine`), the rest continue.
6. **Write idempotently**. Make `batch_id` **deterministic** (hash of dataset + load type + file name) unless the user passes one. Then rerunning the same file overwrites exactly that batch instead of duplicating it:

```python
import hashlib
def derive_batch_id(dataset, load_type, file_name):
    return hashlib.sha1(f"{dataset}|{load_type}|{file_name}".encode()).hexdigest()[:16]

(df.write.format("delta")
   .mode("overwrite")
   .option("replaceWhere", f"_batch_id = '{batch_id}'")
   .option("mergeSchema", "true")
   .saveAsTable(bronze_table))
```
   If `replaceWhere` misbehaves on your runtime, use `DELETE FROM <bronze_table> WHERE _batch_id = '<id>'` followed by an append.
7. Count rows once (`n = df.count()`) and put it in the log record. Avoid `collect()`/`toPandas()` on full data (free-tier compute).
8. After success, you may copy the file to `landing/archive/` (optional; not required).

---

## 7. Step 6: Silver design and data dictionary (half a day)

**Profile first.** Before you trust a primary key, prove it:

```sql
SELECT COUNT(*) AS total_rows,
       COUNT(DISTINCT concat_ws('|', cms_certification_number_ccn, survey_date, survey_type,
                                 deficiency_prefix, deficiency_tag_number, inspection_cycle)) AS distinct_keys
FROM bronze_nh_health_deficiencies;
```
If `total_rows > distinct_keys`, inspect the duplicates. Either add a column to the key, or the source has true duplicate rows (then de-duplicate on the full row hash). **Write down the result and the final key in `design_decisions.md`.** Never describe a key as unique without having tested it.

**Silver rules** (apply to every dataset):

| Rule | Detail |
|---|---|
| Names | snake_case, same as Bronze canonical names |
| Dates | `survey_date`, `correction_date`, `penalty_date`, etc. become `DATE` |
| Integers | `inspection_cycle`, bed counts, star ratings (1 to 5), `payment_denial_length_in_days` become `INT` |
| Decimals | `fine_amount` becomes `DECIMAL(12,2)`; staffing hours, turnover, scores become `DOUBLE` |
| Flags | `Y`/`N` columns become `BOOLEAN` |
| **Keep as STRING** | `cms_certification_number_ccn`, `zip_code`, `deficiency_tag_number`, `telephone` (dropped anyway): leading zeros matter, so casting these to numbers is a bug |
| Empty strings | become `NULL` |
| PII (from Phase 1) | drop `telephone_number`; replace `provider_address` with `provider_address_hash` (SHA-256 with a salt kept in a Databricks secret, not in code) |
| Added columns | `severity_group` (A to C, D to F, G to I, J to L as in Phase 1), `row_hash`, `is_deleted` (BOOLEAN), `source_processing_date`, `load_timestamp` |
| Primary key | a `<entity>_key` column = SHA-256 of the key columns (this is what you MERGE on) |

Silver tables:

| Table | Primary key | Notes |
|---|---|---|
| `silver_deficiency` | `deficiency_key` | one row per citation |
| `silver_penalty` | `penalty_key` | fines and payment denials |
| `silver_facility` | `cms_certification_number_ccn` (SCD1) or `facility_sk` + `valid_from` (SCD2) | about 100 typed columns |
| `silver_mds_quality` | `mds_key` | scores `q1` to `q4` and four-quarter average as `DOUBLE` |
| `silver_quarantine` | none (append-only) | rejected records with reason |

**Data dictionary: do not hand-type 100 columns.** Generate it from your code (Step 11).

---

## 8. Step 7: Bronze to Silver with MERGE (1.5 to 2 days)

Pipeline inside `02_bronze_to_silver.py` for one dataset and one set of batches:

**a. Select the Bronze slice from parameters** (this enables backfills, Step 9):

```python
from pyspark.sql import functions as F

def select_bronze(spark, bronze_table, batch_ids=None, ingest_from=None, ingest_to=None, load_type=None):
    df = spark.table(bronze_table)
    if batch_ids:
        df = df.filter(F.col("_batch_id").isin(batch_ids))
    if ingest_from:
        df = df.filter(F.col("_ingest_date") >= F.lit(ingest_from).cast("date"))
    if ingest_to:
        df = df.filter(F.col("_ingest_date") <= F.lit(ingest_to).cast("date"))
    if load_type:
        df = df.filter(F.col("_load_type") == load_type)
    return df
```
If the user passes none of these, process **batches that reached Bronze successfully but have no successful Bronze-to-Silver log row**. That is your "standard incremental" run, driven by `pipeline_execution_logs`.

**b. Clean and cast** with `try_cast` so a bad value becomes `NULL` instead of throwing (works whether or not ANSI mode is on):

```python
def clean_empty(df):
    return df.select([F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c))).alias(c)
                      if t == "string" else F.col(c)
                      for c, t in df.dtypes])

def tcast(col, to):          # safe cast
    return F.expr(f"try_cast({col} as {to})")
```

**c. Quarantine non-conforming rows** instead of failing: for each required typed column, if the raw value is not null but `try_cast` is null, the row is bad. Collect the failing column names into an array, split the DataFrame into `good` and `bad`, write `bad` (with the raw record as JSON via `F.to_json(F.struct(*raw_cols))`) to `silver_quarantine`. Also reject rows that break your rules from Phase 1 (CCN not 6 characters, rating outside 1 to 5, negative fine, correction before survey date).

**d. De-duplicate inside the batch.** MERGE fails ("multiple source rows matched") if the source has two rows with the same key:

```python
from pyspark.sql import Window
w = Window.partitionBy("deficiency_key").orderBy(F.col("load_timestamp").desc())
good = good.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
```

**e. Add keys and the change-detection hash:**

```python
def sha_cols(cols):
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]), 256)

good = (good.withColumn("deficiency_key", sha_cols(KEY_COLS))
            .withColumn("row_hash", sha_cols(BUSINESS_COLS))   # business columns only, NOT load_timestamp
            .withColumn("is_deleted", F.lit(False))
            .withColumn("load_timestamp", F.current_timestamp()))
```

**f. The MERGE (this is your idempotency proof):**

```python
good.createOrReplaceTempView("stage_deficiency")
spark.sql(f"""
MERGE INTO {silver_table} AS t
USING stage_deficiency AS s
ON t.deficiency_key = s.deficiency_key
WHEN MATCHED AND (t.row_hash <> s.row_hash OR t.is_deleted = true) THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
```
Because unchanged rows have the same `row_hash`, running this twice on the same data inserts **0** and updates **0** rows, and `load_timestamp` is untouched on unchanged rows.

**g. Soft deletes (full loads only).** Add this clause **only when `load_type = 'full'`** and **scope it to the same slice the snapshot covers**. Your full-load deficiency sample stops at the cutoff date, so without the date condition you would wrongly delete every incremental row loaded later:

```sql
WHEN NOT MATCHED BY SOURCE AND t.is_deleted = false AND t.survey_date < DATE'2026-07-01'
THEN UPDATE SET t.is_deleted = true, t.load_timestamp = current_timestamp()
```
The cutoff must come from a parameter, not be typed into the code. Skip this clause for incremental loads entirely.

**h. Get row counts for the log** from the Delta history (do not recount the table):

```python
from delta.tables import DeltaTable
m = DeltaTable.forName(spark, silver_table).history(1).select("operationMetrics").collect()[0][0]
rec["rows_inserted"] = int(m.get("numTargetRowsInserted", 0))
rec["rows_updated"]  = int(m.get("numTargetRowsUpdated", 0))
rec["rows_deleted"]  = int(m.get("numTargetRowsDeleted", 0))
```
(Soft deletes show up under updated; count `is_deleted` changes separately if you want them in `rows_deleted`.)

**i. Facility table.** First do the same MERGE keyed on CCN (SCD1). SCD2 later: use the standard Delta pattern: stage rows whose `row_hash` changed twice (once with `merge_key = ccn` to expire the old row, once with `merge_key = NULL` to insert the new version), with `valid_from`, `valid_to`, `is_current`.

---

## 9. Step 8: Schema drift: what you must be able to demonstrate

| Drift event | Where it is caught | What happens |
|---|---|---|
| Source **adds a column** | Header check in Raw-to-Bronze | Column appended to the read schema, Bronze evolves via `mergeSchema`, row written to `schema_drift_log`. Silver ignores it (explicit column list) until you decide to add it; the log shows it was noticed. |
| Source **drops/renames** an expected column | Header check | File marked `FAILURE`/quarantined in the log, other files continue. |
| A column **changes type** (integer becomes text like `"N/A"`) | `try_cast` in Bronze-to-Silver | Those rows go to `silver_quarantine` with the column name as reason; clean rows continue. Bronze is unaffected because it is all strings. |
| Corrupt/short CSV row | `PERMISSIVE` mode | Row goes to `bronze_quarantine`. |

Create **test files** for the demo (Step 10): copy a sample CSV, (1) add a column `new_col`, (2) put `"abc"` in `inspection_cycle` on two rows, (3) delete a required column in another copy.

---

## 10. Step 9: Backfill versus standard incremental (document this in the README)

| Scenario | Notebook | Parameters |
|---|---|---|
| **Initial full load** | `01_raw_to_bronze` | `dataset=health_deficiencies`, `load_type=full`, `source_path=` (empty = default full folder) |
| **Standard incremental** | `01_raw_to_bronze` then `02_bronze_to_silver` | `load_type=incremental`, `source_path=` empty (default incremental folder). Silver run with no batch filters processes only unprocessed batches. |
| **Backfill one file** | `01_raw_to_bronze` | `source_path=/Volumes/.../landing/incremental/nh_penalties_incr_2026-06.json` |
| **Backfill by batch** | `02_bronze_to_silver` | `batch_id=<id from pipeline_execution_logs>` |
| **Backfill a historical window** | `02_bronze_to_silver` | `ingest_date_from=2026-08-01`, `ingest_date_to=2026-08-31`, `reprocess=true` |

Two checks that you really are parameterized: (1) grep the repo for `"2026-` and `/Volumes/` and make sure they only appear in widget defaults and docs; (2) the same notebook runs for a different dataset by changing only the `dataset` widget.

If Jobs are not available on your plan, running notebooks manually with widgets is fine; say so in the README.

---

## 11. Step 10: Evidence run (half a day, save screenshots into `docs/evidence/`)

Run `99_demo_idempotency_drift_backfill.py` and keep the outputs:

1. **Idempotency:** run Raw-to-Bronze then Bronze-to-Silver. Record `COUNT(*)` and `COUNT(DISTINCT key)` of Bronze and Silver. Run **both again**. Counts identical, and the second Silver log row shows `rows_inserted = 0, rows_updated = 0`.
2. **Incremental:** load the incremental JSON; Silver inserts only new keys.
3. **Update:** edit one value in a copy of a file, reload; one row is updated (and only that row's `load_timestamp` changes).
4. **Backfill:** rerun with explicit dates/batch id for an old batch; no duplicates.
5. **Drift:** run the three test files; show `schema_drift_log`, `silver_quarantine` and the log rows (status `SUCCESS`, `QUARANTINED_PARTIAL`, `FAILURE`), with the batch still completing for the good files.
6. **Failure logging:** point `source_path` at a missing file; a `FAILURE` row with an `error_message` appears.
7. `SELECT * FROM pipeline_execution_logs ORDER BY start_time DESC` screenshot showing both layers and both load types.

---

## 12. Step 11: README and data dictionary

The README must contain: (a) project overview and architecture, (b) **Bronze data model**, (c) **Silver data model**, (d) **execution guide** (the Step 9 table plus how to set up the workspace), (e) the requirements traceability table from Section 0, (f) known limitations.

Generate the dictionary from code so it cannot drift from reality:

```python
# docs/generate_data_dictionary.py  (run in a notebook where the tables exist)
def dictionary_md(spark, table, pk_cols, descriptions=None):
    descriptions = descriptions or {}
    rows = ["| Column | Type | Nullable | PK | Description |", "|---|---|---|---|---|"]
    for f in spark.table(table).schema.fields:
        rows.append(f"| `{f.name}` | {f.dataType.simpleString()} | {f.nullable} | "
                    f"{'Yes' if f.name in pk_cols else ''} | {descriptions.get(f.name, '')} |")
    return "\n".join(rows)

print(dictionary_md(spark, "carewatch.pipeline.silver_deficiency", ["deficiency_key"]))
```
Paste the output into `docs/data_dictionary.md` and link it from the README. Add descriptions by hand only for columns that need explanation (severity codes, hashes, flags). For the 100-column facility table, group the descriptions in one paragraph instead of 100 lines.

---

## 13. Step 12: Commit discipline and submission checklist

- Commit **after every working step** (small commits: "Add Bronze schema for deficiencies", "Add MERGE for silver_deficiency"). Both partners commit. Use feature branches and short pull requests if you can.
- Never commit tokens, salts or the raw data folder.

Final checklist (tick all before submitting the repo link):

- [ ] `grep -ri inferSchema notebooks src` returns nothing (mentions in docs/README are fine)
- [ ] Every Bronze and Silver table, plus the log tables, has `load_timestamp`
- [ ] Silver written only via `MERGE INTO`; rerun shows 0 inserted / 0 updated
- [ ] All notebooks use widgets/parameters; no hard-coded dates or paths
- [ ] Drift demo and quarantine tables populated, batch does not crash
- [ ] `pipeline_execution_logs` has rows for Raw-to-Bronze and Bronze-to-Silver, full and incremental, with all required fields
- [ ] README has Bronze model, Silver model, backfill-vs-incremental guide
- [ ] Repo opens in a private browser window for your instructor
- [ ] `docs/evidence/` has screenshots/query outputs

---

## 14. Suggested timeline (adjust to your deadline)

| Day | Work |
|---|---|
| 1 | Steps 1 to 4: workspace, repo, widgets/config, schemas, logging tables and helper |
| 2 | Step 5: Raw-to-Bronze for Health Deficiencies (full + incremental JSON), drift check |
| 3 | Step 6 profiling and Step 7 for Health Deficiencies (clean, cast, quarantine, MERGE) |
| 4 | Replicate for Penalties, Provider Information, MDS Quality (registry makes this fast); soft deletes |
| 5 | Steps 8 and 10: drift and idempotency demos, evidence screenshots |
| 6 | Step 11: README, data dictionary, final checklist, buffer |

---

## Things I could not verify (check them early)

- **Databricks Free Edition specifics:** serverless compute may block RDD/caching APIs and some Spark configs, and Git folders or Jobs may be limited. Test a 5-line notebook on day 1.
- **Your exact runtime version:** `try_cast`, `replaceWhere` on a data column and `WHEN NOT MATCHED BY SOURCE` need reasonably recent Databricks/Delta versions. If a statement errors, use the fallback noted next to it.
- **Primary keys:** the keys in this guide are candidates until profiled (Step 6).
- **Provider Information column mapping:** about 100 columns; some bulk-file headers will not match API names automatically (Step 3).
- **Instructor expectations:** whether Free Edition is accepted in place of Community Edition, and whether SCD2 is mandatory. Ask in one message.
