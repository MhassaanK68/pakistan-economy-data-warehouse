"""Focused local checks for the declarative Phase 2 Step 6 contracts.

The repository's lightweight local Python runtime does not include PySpark.
When PySpark is absent, this test installs a minimal in-memory stand-in for
``pyspark.sql.types`` so schema structure can still be checked. No Spark or
Databricks behavior is simulated.
"""

from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


def _install_pyspark_type_stub() -> None:
    if importlib.util.find_spec("pyspark") is not None:
        return

    class DataType:
        type_name = "unknown"

        def simpleString(self):  # noqa: N802 - mirrors PySpark API
            return self.type_name

    class StringType(DataType):
        type_name = "string"

    class BooleanType(DataType):
        type_name = "boolean"

    class DateType(DataType):
        type_name = "date"

    class DoubleType(DataType):
        type_name = "double"

    class IntegerType(DataType):
        type_name = "int"

    class LongType(DataType):
        type_name = "bigint"

    class TimestampType(DataType):
        type_name = "timestamp"

    class DecimalType(DataType):
        def __init__(self, precision, scale):
            self.precision = precision
            self.scale = scale

        def simpleString(self):  # noqa: N802 - mirrors PySpark API
            return f"decimal({self.precision},{self.scale})"

    class ArrayType(DataType):
        def __init__(self, elementType, containsNull=True):  # noqa: N803
            self.elementType = elementType
            self.containsNull = containsNull

        def simpleString(self):  # noqa: N802 - mirrors PySpark API
            return f"array<{self.elementType.simpleString()}>"

    class StructField:
        def __init__(self, name, dataType, nullable=True):  # noqa: N803
            self.name = name
            self.dataType = dataType
            self.nullable = nullable

    class StructType(DataType):
        def __init__(self, fields=None):
            self.fields = list(fields or [])

        def simpleString(self):  # noqa: N802 - mirrors PySpark API
            inner = ",".join(
                f"{field.name}:{field.dataType.simpleString()}"
                for field in self.fields
            )
            return f"struct<{inner}>"

    pyspark_module = types.ModuleType("pyspark")
    sql_module = types.ModuleType("pyspark.sql")
    types_module = types.ModuleType("pyspark.sql.types")
    for value in (
        ArrayType,
        BooleanType,
        DateType,
        DecimalType,
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    ):
        setattr(types_module, value.__name__, value)
    pyspark_module.sql = sql_module
    sql_module.types = types_module
    sys.modules["pyspark"] = pyspark_module
    sys.modules["pyspark.sql"] = sql_module
    sys.modules["pyspark.sql.types"] = types_module


_install_pyspark_type_stub()

from carewatch import audit, schemas  # noqa: E402
from carewatch.config import DATASETS  # noqa: E402


def _field_map(schema):
    return {field.name: field for field in schema.fields}


def _contract_table_fields(text, start_marker, end_marker):
    section = text.split(start_marker, 1)[1].split(end_marker, 1)[0]
    fields = []
    for line in section.splitlines():
        if not line.startswith("| `"):
            continue
        cells = [cell.strip() for cell in line.split("|")[1:-1]]
        fields.append((cells[0].strip("`"), cells[1].strip("`").lower()))
    return fields


class SilverSchemaContractTests(unittest.TestCase):
    def test_four_silver_schemas_match_registry(self):
        expected = {
            "health_deficiencies",
            "penalties",
            "provider_info",
            "mds_quality",
        }
        self.assertEqual(set(schemas.SILVER_SCHEMAS), expected)
        self.assertEqual(set(schemas.SILVER_VALIDATION_SPECS), expected)
        for dataset in expected:
            self.assertEqual(
                schemas.SILVER_TABLE_NAMES[dataset], DATASETS[dataset].silver_table
            )
            self.assertEqual(
                schemas.SILVER_VALIDATION_SPECS[dataset].table_name,
                DATASETS[dataset].silver_table,
            )
            self.assertEqual(
                schemas.SILVER_BUSINESS_KEYS[dataset], DATASETS[dataset].key_cols
            )

    def test_silver_schema_sizes_are_locked(self):
        self.assertEqual(len(schemas.SILVER_DEFICIENCY_SCHEMA.fields), 31)
        self.assertEqual(len(schemas.SILVER_PENALTY_SCHEMA.fields), 20)
        self.assertEqual(len(schemas.SILVER_MDS_QUALITY_SCHEMA.fields), 29)
        self.assertEqual(len(schemas.SILVER_FACILITY_SCHEMA.fields), 107)

    def test_required_nullability_matches_contract(self):
        expected_non_null = {
            "health_deficiencies": {
                "deficiency_key",
                "cms_certification_number_ccn",
                "survey_date",
                "survey_type",
                "deficiency_prefix",
                "deficiency_tag_number",
                "inspection_cycle",
            },
            "penalties": {
                "penalty_key",
                "cms_certification_number_ccn",
                "penalty_date",
                "penalty_type",
            },
            "provider_info": {
                "facility_key",
                "cms_certification_number_ccn",
            },
            "mds_quality": {
                "mds_key",
                "cms_certification_number_ccn",
                "measure_code",
                "resident_type",
                "used_in_quality_measure_five_star_rating",
                "measure_period",
            },
        }
        lineage_non_null = set(schemas.SILVER_LINEAGE_COLUMNS) - {
            "source_processing_date"
        }
        for dataset, expected in expected_non_null.items():
            actual = {
                field.name
                for field in schemas.SILVER_SCHEMAS[dataset].fields
                if not field.nullable
            }
            self.assertEqual(actual, expected | lineage_non_null)

    def test_privacy_fields_are_not_in_silver(self):
        for dataset, schema in schemas.SILVER_SCHEMAS.items():
            names = {field.name for field in schema.fields}
            self.assertTrue(
                set(schemas.SILVER_PRIVACY_DROPPED_FIELDS).isdisjoint(names), dataset
            )
            self.assertIn("provider_address_hash", names)

    def test_facility_projects_every_allowed_provider_source_column(self):
        expected = set(schemas.PROVIDER_INFO_COLS)
        expected.difference_update(schemas.SILVER_PRIVACY_DROPPED_FIELDS)
        expected.remove("processing_date")
        expected.add("provider_address_hash")
        actual = set(schemas.silver_business_columns("provider_info"))
        self.assertEqual(actual, expected)

    def test_row_hash_column_contract_excludes_keys_and_lineage(self):
        for dataset, schema in schemas.SILVER_SCHEMAS.items():
            business_columns = schemas.silver_business_columns(dataset)
            self.assertNotIn(schemas.SILVER_ENTITY_KEYS[dataset], business_columns)
            self.assertTrue(
                set(schemas.SILVER_LINEAGE_COLUMNS).isdisjoint(business_columns)
            )
            expected_count = (
                len(schema.fields) - 1 - len(schemas.SILVER_LINEAGE_COLUMNS)
            )
            self.assertEqual(len(business_columns), expected_count)

    def test_bronze_contracts_are_unchanged(self):
        for dataset, source_columns in schemas.SOURCE_COLUMNS.items():
            bronze_fields = schemas.BRONZE_SCHEMAS[dataset].fields
            self.assertEqual(
                [field.name for field in bronze_fields[: len(source_columns)]],
                list(source_columns),
            )
            self.assertEqual(
                [field.name for field in bronze_fields[len(source_columns) :]],
                [field.name for field in schemas.BRONZE_META.fields],
            )
            self.assertEqual(
                len(bronze_fields), len(source_columns) + len(schemas.BRONZE_META.fields)
            )

    def test_critical_types_match_contract(self):
        deficiency = _field_map(schemas.SILVER_DEFICIENCY_SCHEMA)
        penalty = _field_map(schemas.SILVER_PENALTY_SCHEMA)
        mds = _field_map(schemas.SILVER_MDS_QUALITY_SCHEMA)
        facility = _field_map(schemas.SILVER_FACILITY_SCHEMA)
        self.assertEqual(deficiency["survey_date"].dataType.simpleString(), "date")
        self.assertEqual(deficiency["inspection_cycle"].dataType.simpleString(), "int")
        self.assertEqual(
            deficiency["standard_deficiency"].dataType.simpleString(), "boolean"
        )
        self.assertEqual(
            penalty["fine_amount"].dataType.simpleString(), "decimal(14,2)"
        )
        self.assertEqual(mds["q1_measure_score"].dataType.simpleString(), "double")
        self.assertEqual(facility["overall_rating"].dataType.simpleString(), "int")
        self.assertEqual(
            facility["total_amount_of_fines_in_dollars"].dataType.simpleString(),
            "decimal(14,2)",
        )

    def test_all_columns_and_types_match_locked_markdown_contract(self):
        text = (ROOT / "docs" / "bronze_silver_schema_contract.md").read_text(
            encoding="utf-8"
        )
        contract_fields = {
            "health_deficiencies": _contract_table_fields(
                text, "### 4.1 `silver_deficiency`", "### 4.2 `silver_penalty`"
            ),
            "penalties": _contract_table_fields(
                text, "### 4.2 `silver_penalty`", "### 4.3 `silver_mds_quality`"
            ),
            "mds_quality": _contract_table_fields(
                text, "### 4.3 `silver_mds_quality`", "### 4.4 `silver_facility`"
            ),
            "provider_info": _contract_table_fields(
                text, "### 4.4 `silver_facility`", "## 5. Quarantine and drift schemas"
            ),
        }
        # Facility lineage is specified once in the global lineage table rather
        # than repeated in the 102-column facility section.
        contract_fields["provider_info"].extend(
            _contract_table_fields(
                text, "### 2.6 Standard Silver lineage", "### 2.7 Privacy projection"
            )
        )
        for dataset, expected in contract_fields.items():
            actual = [
                (field.name, field.dataType.simpleString().lower())
                for field in schemas.SILVER_SCHEMAS[dataset].fields
            ]
            self.assertEqual(actual, expected, dataset)

    def test_data_dictionary_matches_implemented_schemas(self):
        text = (ROOT / "docs" / "data_dictionary.md").read_text(encoding="utf-8")
        documented = {
            "health_deficiencies": _contract_table_fields(
                text, "### `silver_deficiency`", "### `silver_penalty`"
            ),
            "penalties": _contract_table_fields(
                text, "### `silver_penalty`", "### `silver_mds_quality`"
            ),
            "mds_quality": _contract_table_fields(
                text, "### `silver_mds_quality`", "### `silver_facility`"
            ),
            "provider_info": _contract_table_fields(
                text, "### `silver_facility`", "## Silver quarantine"
            ),
        }
        for dataset, expected in documented.items():
            actual = [
                (field.name, field.dataType.simpleString().lower())
                for field in schemas.SILVER_SCHEMAS[dataset].fields
            ]
            self.assertEqual(actual, expected, dataset)

    def test_validation_specs_cover_required_rule_categories(self):
        for dataset, spec in schemas.SILVER_VALIDATION_SPECS.items():
            self.assertEqual(spec.dataset, dataset)
            self.assertTrue(spec.required_source_fields)
            self.assertTrue(spec.safe_cast_fields)
            self.assertNotIn("load_timestamp", spec.safe_cast_fields)
            self.assertIn(
                ("processing_date", "source_processing_date"), spec.source_renames
            )
            self.assertTrue(spec.regex_rules)
            self.assertIn("provider_address", spec.privacy_dropped_fields)
            self.assertIn("telephone_number", spec.privacy_dropped_fields)
            self.assertIn("location", spec.privacy_dropped_fields)
            self.assertIn("SHA-256", spec.entity_key_specification)
            self.assertIn("SHA-256", spec.row_hash_specification)
            self.assertIn("contract conflict", spec.conflicting_key_policy)

            schema_names = {field.name for field in schemas.SILVER_SCHEMAS[dataset].fields}
            referenced = set(spec.required_source_fields)
            referenced.update(spec.safe_cast_fields)
            referenced.update(spec.uppercase_fields)
            referenced.update(spec.boolean_yn_fields)
            referenced.update(field for field, _ in spec.regex_rules)
            referenced.update(field for field, _ in spec.allowed_values)
            referenced.update(field for field, _, _ in spec.numeric_ranges)
            referenced.update(spec.non_negative_fields)
            self.assertTrue(referenced.issubset(schema_names), dataset)

    def test_silver_quarantine_schema_matches_locked_contract(self):
        fields = audit.SILVER_QUARANTINE_SCHEMA.fields
        self.assertEqual(
            [field.name for field in fields],
            [
                "dataset",
                "source_batch_id",
                "source_file",
                "source_file_sha256",
                "candidate_entity_key",
                "raw_record",
                "failed_rules",
                "load_timestamp",
            ],
        )
        self.assertEqual(
            [field.dataType.simpleString() for field in fields],
            [
                "string",
                "string",
                "string",
                "string",
                "string",
                "string",
                "array<string>",
                "timestamp",
            ],
        )
        self.assertTrue(_field_map(audit.SILVER_QUARANTINE_SCHEMA)["candidate_entity_key"].nullable)
        self.assertFalse(_field_map(audit.SILVER_QUARANTINE_SCHEMA)["source_batch_id"].nullable)

    def test_step7_files_remain_empty(self):
        self.assertEqual((ROOT / "src" / "carewatch" / "silver.py").stat().st_size, 0)
        self.assertEqual(
            (ROOT / "notebooks" / "03_bronze_to_silver.py").stat().st_size,
            0,
        )


if __name__ == "__main__":
    unittest.main()
