"""Explicit source and Bronze schemas for the four CareWatch CMS datasets.

Bronze deliberately preserves every CMS source value as a nullable string.  The
only typed Bronze fields are ingestion metadata owned by this pipeline.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass

from pyspark.sql.types import (
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
)


DEFICIENCY_COLS = (
    "cms_certification_number_ccn",
    "provider_name",
    "provider_address",
    "citytown",
    "state",
    "zip_code",
    "survey_date",
    "survey_footnote",
    "survey_type",
    "deficiency_prefix",
    "deficiency_category",
    "deficiency_tag_number",
    "deficiency_description",
    "scope_severity_code",
    "deficiency_corrected",
    "correction_date",
    "inspection_cycle",
    "standard_deficiency",
    "complaint_deficiency",
    "infection_control_inspection_deficiency",
    "citation_under_idr",
    "citation_under_iidr",
    "location",
    "processing_date",
)

PENALTY_COLS = (
    "cms_certification_number_ccn",
    "provider_name",
    "provider_address",
    "citytown",
    "state",
    "zip_code",
    "penalty_date",
    "penalty_type",
    "fine_id",
    "fine_amount",
    "payment_denial_start_date",
    "payment_denial_length_in_days",
    "location",
    "processing_date",
)

MDS_QUALITY_COLS = (
    "cms_certification_number_ccn",
    "provider_name",
    "provider_address",
    "citytown",
    "state",
    "zip_code",
    "measure_code",
    "measure_description",
    "resident_type",
    "q1_measure_score",
    "footnote_for_q1_measure_score",
    "q2_measure_score",
    "footnote_for_q2_measure_score",
    "q3_measure_score",
    "footnote_for_q3_measure_score",
    "q4_measure_score",
    "footnote_for_q4_measure_score",
    "four_quarter_average_score",
    "footnote_for_four_quarter_average_score",
    "used_in_quality_measure_five_star_rating",
    "measure_period",
    "location",
    "processing_date",
)

PROVIDER_INFO_COLS = (
    "cms_certification_number_ccn",
    "provider_name",
    "provider_address",
    "citytown",
    "state",
    "zip_code",
    "telephone_number",
    "provider_ssa_county_code",
    "countyparish",
    "urban",
    "ownership_type",
    "number_of_certified_beds",
    "average_number_of_residents_per_day",
    "average_number_of_residents_per_day_footnote",
    "provider_type",
    "provider_resides_in_hospital",
    "legal_business_name",
    "date_first_approved_to_provide_medicare_and_medicaid_services",
    "chain_name",
    "chain_id",
    "number_of_facilities_in_chain",
    "chain_average_overall_5star_rating",
    "chain_average_health_inspection_rating",
    "chain_average_staffing_rating",
    "chain_average_qm_rating",
    "continuing_care_retirement_community",
    "special_focus_status",
    "abuse_icon",
    "high_performing_icon",
    "most_recent_health_inspection_more_than_2_years_ago",
    "provider_changed_ownership_in_last_12_months",
    "with_a_resident_and_family_council",
    "automatic_sprinkler_systems_in_all_required_areas",
    "overall_rating",
    "overall_rating_footnote",
    "health_inspection_rating",
    "health_inspection_rating_footnote",
    "qm_rating",
    "qm_rating_footnote",
    "longstay_qm_rating",
    "longstay_qm_rating_footnote",
    "shortstay_qm_rating",
    "shortstay_qm_rating_footnote",
    "staffing_rating",
    "staffing_rating_footnote",
    "reported_staffing_footnote",
    "physical_therapist_staffing_footnote",
    "reported_nurse_aide_staffing_hours_per_resident_per_day",
    "reported_lpn_staffing_hours_per_resident_per_day",
    "reported_rn_staffing_hours_per_resident_per_day",
    "reported_licensed_staffing_hours_per_resident_per_day",
    "reported_total_nurse_staffing_hours_per_resident_per_day",
    "total_number_of_nurse_staff_hours_per_resident_per_day_on_t_4a14",
    "registered_nurse_hours_per_resident_per_day_on_the_weekend",
    "reported_physical_therapist_staffing_hours_per_resident_per_day",
    "total_nursing_staff_turnover",
    "total_nursing_staff_turnover_footnote",
    "registered_nurse_turnover",
    "registered_nurse_turnover_footnote",
    "number_of_administrators_who_have_left_the_nursing_home",
    "administrator_turnover_footnote",
    "nursing_casemix_index",
    "nursing_casemix_index_ratio",
    "casemix_nurse_aide_staffing_hours_per_resident_per_day",
    "casemix_lpn_staffing_hours_per_resident_per_day",
    "casemix_rn_staffing_hours_per_resident_per_day",
    "casemix_total_nurse_staffing_hours_per_resident_per_day",
    "casemix_weekend_total_nurse_staffing_hours_per_resident_per_day",
    "adjusted_nurse_aide_staffing_hours_per_resident_per_day",
    "adjusted_lpn_staffing_hours_per_resident_per_day",
    "adjusted_rn_staffing_hours_per_resident_per_day",
    "adjusted_total_nurse_staffing_hours_per_resident_per_day",
    "adjusted_weekend_total_nurse_staffing_hours_per_resident_per_day",
    "rating_cycle_1_standard_survey_health_date",
    "rating_cycle_1_standard_survey_footnote",
    "rating_cycle_1_total_number_of_health_deficiencies",
    "rating_cycle_1_number_of_standard_health_deficiencies",
    "rating_cycle_1_number_of_complaint_health_deficiencies",
    "rating_cycle_1_health_deficiency_score",
    "rating_cycle_1_number_of_health_revisits",
    "rating_cycle_1_health_revisit_score",
    "rating_cycle_1_total_health_score",
    "rating_cycle_2_standard_health_survey_date",
    "rating_cycle_2_standard_survey_footnote",
    "rating_cycle_23_total_number_of_health_deficiencies",
    "rating_cycle_2_number_of_standard_health_deficiencies",
    "rating_cycle_23_number_of_complaint_health_deficiencies",
    "rating_cycle_23_health_deficiency_score",
    "rating_cycle_23_number_of_health_revisits",
    "rating_cycle_23_health_revisit_score",
    "rating_cycle_23_total_health_score",
    "total_weighted_health_survey_score",
    "number_of_citations_from_infection_control_inspections",
    "number_of_fines",
    "total_amount_of_fines_in_dollars",
    "number_of_payment_denials",
    "total_number_of_penalties",
    "location",
    "latitude",
    "longitude",
    "geocoding_footnote",
    "processing_date",
)


SOURCE_COLUMNS: dict[str, tuple[str, ...]] = {
    "health_deficiencies": DEFICIENCY_COLS,
    "penalties": PENALTY_COLS,
    "provider_info": PROVIDER_INFO_COLS,
    "mds_quality": MDS_QUALITY_COLS,
}

EXPECTED_SOURCE_COLUMN_COUNTS = {
    "health_deficiencies": 24,
    "penalties": 14,
    "provider_info": 102,
    "mds_quality": 23,
}

HEADER_OVERRIDES: dict[str, dict[str, str]] = {
    "provider_info": {
        "Total number of nurse staff hours per resident per day on the weekend": (
            "total_number_of_nurse_staff_hours_per_resident_per_day_on_t_4a14"
        )
    }
}


def string_struct(columns: Iterable[str]) -> StructType:
    """Build an explicit nullable-string Spark schema without inference."""

    return StructType(
        [StructField(column, StringType(), nullable=True) for column in columns]
    )


BRONZE_META = StructType(
    [
        StructField("_dataset_id", StringType(), nullable=False),
        StructField("_source_file", StringType(), nullable=False),
        StructField("_source_file_sha256", StringType(), nullable=False),
        StructField("_batch_id", StringType(), nullable=False),
        StructField("_load_type", StringType(), nullable=False),
        StructField("_ingest_date", DateType(), nullable=False),
        StructField("load_timestamp", TimestampType(), nullable=False),
    ]
)

INCREMENTAL_META = StructType(
    [
        StructField("dataset_id", StringType(), nullable=False),
        StructField("filter", StringType(), nullable=False),
        StructField("rows_available", LongType(), nullable=True),
        StructField("rows_in_file", LongType(), nullable=False),
        StructField("fetched_at_utc", StringType(), nullable=False),
        StructField("load_type", StringType(), nullable=False),
    ]
)

SOURCE_SCHEMAS = {
    dataset: string_struct(columns) for dataset, columns in SOURCE_COLUMNS.items()
}

CSV_READ_SCHEMAS = {
    dataset: StructType(
        [*schema.fields, StructField("_corrupt_record", StringType(), nullable=True)]
    )
    for dataset, schema in SOURCE_SCHEMAS.items()
}

BRONZE_SCHEMAS = {
    dataset: StructType([*schema.fields, *BRONZE_META.fields])
    for dataset, schema in SOURCE_SCHEMAS.items()
}


# ---------------------------------------------------------------------------
# Silver contracts
# ---------------------------------------------------------------------------
# These definitions are deliberately declarative.  Step 7 will consume them,
# but this module does not clean, cast, quarantine, or write any data.

SILVER_LINEAGE = StructType(
    [
        StructField("row_hash", StringType(), nullable=False),
        StructField("is_deleted", BooleanType(), nullable=False),
        StructField("source_file", StringType(), nullable=False),
        StructField("source_file_sha256", StringType(), nullable=False),
        StructField("source_batch_id", StringType(), nullable=False),
        StructField("source_processing_date", DateType(), nullable=True),
        StructField("load_timestamp", TimestampType(), nullable=False),
    ]
)

SILVER_DEFICIENCY_SCHEMA = StructType(
    [
        StructField("deficiency_key", StringType(), nullable=False),
        StructField("cms_certification_number_ccn", StringType(), nullable=False),
        StructField("provider_name", StringType(), nullable=True),
        StructField("provider_address_hash", StringType(), nullable=True),
        StructField("citytown", StringType(), nullable=True),
        StructField("state", StringType(), nullable=True),
        StructField("zip_code", StringType(), nullable=True),
        StructField("survey_date", DateType(), nullable=False),
        StructField("survey_footnote", IntegerType(), nullable=True),
        StructField("survey_type", StringType(), nullable=False),
        StructField("deficiency_prefix", StringType(), nullable=False),
        StructField("deficiency_category", StringType(), nullable=True),
        StructField("deficiency_tag_number", StringType(), nullable=False),
        StructField("deficiency_description", StringType(), nullable=True),
        StructField("scope_severity_code", StringType(), nullable=True),
        StructField("severity_group", StringType(), nullable=True),
        StructField("deficiency_corrected", StringType(), nullable=True),
        StructField("correction_date", DateType(), nullable=True),
        StructField("inspection_cycle", IntegerType(), nullable=False),
        StructField("standard_deficiency", BooleanType(), nullable=True),
        StructField("complaint_deficiency", BooleanType(), nullable=True),
        StructField(
            "infection_control_inspection_deficiency", BooleanType(), nullable=True
        ),
        StructField("citation_under_idr", BooleanType(), nullable=True),
        StructField("citation_under_iidr", BooleanType(), nullable=True),
        *SILVER_LINEAGE.fields,
    ]
)

SILVER_PENALTY_SCHEMA = StructType(
    [
        StructField("penalty_key", StringType(), nullable=False),
        StructField("cms_certification_number_ccn", StringType(), nullable=False),
        StructField("provider_name", StringType(), nullable=True),
        StructField("provider_address_hash", StringType(), nullable=True),
        StructField("citytown", StringType(), nullable=True),
        StructField("state", StringType(), nullable=True),
        StructField("zip_code", StringType(), nullable=True),
        StructField("penalty_date", DateType(), nullable=False),
        StructField("penalty_type", StringType(), nullable=False),
        StructField("fine_id", StringType(), nullable=True),
        StructField("fine_amount", DecimalType(14, 2), nullable=True),
        StructField("payment_denial_start_date", DateType(), nullable=True),
        StructField("payment_denial_length_in_days", IntegerType(), nullable=True),
        *SILVER_LINEAGE.fields,
    ]
)

SILVER_MDS_QUALITY_SCHEMA = StructType(
    [
        StructField("mds_key", StringType(), nullable=False),
        StructField("cms_certification_number_ccn", StringType(), nullable=False),
        StructField("provider_name", StringType(), nullable=True),
        StructField("provider_address_hash", StringType(), nullable=True),
        StructField("citytown", StringType(), nullable=True),
        StructField("state", StringType(), nullable=True),
        StructField("zip_code", StringType(), nullable=True),
        StructField("measure_code", StringType(), nullable=False),
        StructField("measure_description", StringType(), nullable=True),
        StructField("resident_type", StringType(), nullable=False),
        StructField("q1_measure_score", DoubleType(), nullable=True),
        StructField("footnote_for_q1_measure_score", IntegerType(), nullable=True),
        StructField("q2_measure_score", DoubleType(), nullable=True),
        StructField("footnote_for_q2_measure_score", IntegerType(), nullable=True),
        StructField("q3_measure_score", DoubleType(), nullable=True),
        StructField("footnote_for_q3_measure_score", IntegerType(), nullable=True),
        StructField("q4_measure_score", DoubleType(), nullable=True),
        StructField("footnote_for_q4_measure_score", IntegerType(), nullable=True),
        StructField("four_quarter_average_score", DoubleType(), nullable=True),
        StructField(
            "footnote_for_four_quarter_average_score", IntegerType(), nullable=True
        ),
        StructField(
            "used_in_quality_measure_five_star_rating",
            BooleanType(),
            nullable=False,
        ),
        StructField("measure_period", StringType(), nullable=False),
        *SILVER_LINEAGE.fields,
    ]
)

SILVER_FACILITY_SCHEMA = StructType(
    [
        StructField("facility_key", StringType(), nullable=False),
        StructField("cms_certification_number_ccn", StringType(), nullable=False),
        StructField("provider_name", StringType(), nullable=True),
        StructField("provider_address_hash", StringType(), nullable=True),
        StructField("citytown", StringType(), nullable=True),
        StructField("state", StringType(), nullable=True),
        StructField("zip_code", StringType(), nullable=True),
        StructField("provider_ssa_county_code", StringType(), nullable=True),
        StructField("countyparish", StringType(), nullable=True),
        StructField("urban", BooleanType(), nullable=True),
        StructField("ownership_type", StringType(), nullable=True),
        StructField("provider_type", StringType(), nullable=True),
        StructField("provider_resides_in_hospital", BooleanType(), nullable=True),
        StructField("legal_business_name", StringType(), nullable=True),
        StructField(
            "date_first_approved_to_provide_medicare_and_medicaid_services",
            DateType(),
            nullable=True,
        ),
        StructField("chain_name", StringType(), nullable=True),
        StructField("chain_id", StringType(), nullable=True),
        StructField("special_focus_status", StringType(), nullable=True),
        StructField(
            "with_a_resident_and_family_council", StringType(), nullable=True
        ),
        StructField(
            "automatic_sprinkler_systems_in_all_required_areas",
            StringType(),
            nullable=True,
        ),
        StructField("latitude", DoubleType(), nullable=True),
        StructField("longitude", DoubleType(), nullable=True),
        StructField("geocoding_footnote", IntegerType(), nullable=True),
        StructField("number_of_certified_beds", IntegerType(), nullable=True),
        StructField("average_number_of_residents_per_day", DoubleType(), nullable=True),
        StructField(
            "average_number_of_residents_per_day_footnote",
            IntegerType(),
            nullable=True,
        ),
        StructField("number_of_facilities_in_chain", IntegerType(), nullable=True),
        StructField("chain_average_overall_5star_rating", DoubleType(), nullable=True),
        StructField(
            "chain_average_health_inspection_rating", DoubleType(), nullable=True
        ),
        StructField("chain_average_staffing_rating", DoubleType(), nullable=True),
        StructField("chain_average_qm_rating", DoubleType(), nullable=True),
        StructField(
            "continuing_care_retirement_community", BooleanType(), nullable=True
        ),
        StructField("abuse_icon", BooleanType(), nullable=True),
        StructField("high_performing_icon", BooleanType(), nullable=True),
        StructField(
            "most_recent_health_inspection_more_than_2_years_ago",
            BooleanType(),
            nullable=True,
        ),
        StructField(
            "provider_changed_ownership_in_last_12_months",
            BooleanType(),
            nullable=True,
        ),
        StructField("overall_rating", IntegerType(), nullable=True),
        StructField("overall_rating_footnote", IntegerType(), nullable=True),
        StructField("health_inspection_rating", IntegerType(), nullable=True),
        StructField(
            "health_inspection_rating_footnote", IntegerType(), nullable=True
        ),
        StructField("qm_rating", IntegerType(), nullable=True),
        StructField("qm_rating_footnote", IntegerType(), nullable=True),
        StructField("longstay_qm_rating", IntegerType(), nullable=True),
        StructField("longstay_qm_rating_footnote", IntegerType(), nullable=True),
        StructField("shortstay_qm_rating", IntegerType(), nullable=True),
        StructField("shortstay_qm_rating_footnote", IntegerType(), nullable=True),
        StructField("staffing_rating", IntegerType(), nullable=True),
        StructField("staffing_rating_footnote", IntegerType(), nullable=True),
        StructField("reported_staffing_footnote", IntegerType(), nullable=True),
        StructField(
            "physical_therapist_staffing_footnote", IntegerType(), nullable=True
        ),
        StructField(
            "reported_nurse_aide_staffing_hours_per_resident_per_day",
            DoubleType(),
            nullable=True,
        ),
        StructField(
            "reported_lpn_staffing_hours_per_resident_per_day",
            DoubleType(),
            nullable=True,
        ),
        StructField(
            "reported_rn_staffing_hours_per_resident_per_day",
            DoubleType(),
            nullable=True,
        ),
        StructField(
            "reported_licensed_staffing_hours_per_resident_per_day",
            DoubleType(),
            nullable=True,
        ),
        StructField(
            "reported_total_nurse_staffing_hours_per_resident_per_day",
            DoubleType(),
            nullable=True,
        ),
        StructField(
            "total_number_of_nurse_staff_hours_per_resident_per_day_on_t_4a14",
            DoubleType(),
            nullable=True,
        ),
        StructField(
            "registered_nurse_hours_per_resident_per_day_on_the_weekend",
            DoubleType(),
            nullable=True,
        ),
        StructField(
            "reported_physical_therapist_staffing_hours_per_resident_per_day",
            DoubleType(),
            nullable=True,
        ),
        StructField("total_nursing_staff_turnover", DoubleType(), nullable=True),
        StructField(
            "total_nursing_staff_turnover_footnote", IntegerType(), nullable=True
        ),
        StructField("registered_nurse_turnover", DoubleType(), nullable=True),
        StructField(
            "registered_nurse_turnover_footnote", IntegerType(), nullable=True
        ),
        StructField(
            "number_of_administrators_who_have_left_the_nursing_home",
            IntegerType(),
            nullable=True,
        ),
        StructField("administrator_turnover_footnote", IntegerType(), nullable=True),
        StructField("nursing_casemix_index", DoubleType(), nullable=True),
        StructField("nursing_casemix_index_ratio", DoubleType(), nullable=True),
        StructField(
            "casemix_nurse_aide_staffing_hours_per_resident_per_day",
            DoubleType(),
            nullable=True,
        ),
        StructField(
            "casemix_lpn_staffing_hours_per_resident_per_day",
            DoubleType(),
            nullable=True,
        ),
        StructField(
            "casemix_rn_staffing_hours_per_resident_per_day",
            DoubleType(),
            nullable=True,
        ),
        StructField(
            "casemix_total_nurse_staffing_hours_per_resident_per_day",
            DoubleType(),
            nullable=True,
        ),
        StructField(
            "casemix_weekend_total_nurse_staffing_hours_per_resident_per_day",
            DoubleType(),
            nullable=True,
        ),
        StructField(
            "adjusted_nurse_aide_staffing_hours_per_resident_per_day",
            DoubleType(),
            nullable=True,
        ),
        StructField(
            "adjusted_lpn_staffing_hours_per_resident_per_day",
            DoubleType(),
            nullable=True,
        ),
        StructField(
            "adjusted_rn_staffing_hours_per_resident_per_day",
            DoubleType(),
            nullable=True,
        ),
        StructField(
            "adjusted_total_nurse_staffing_hours_per_resident_per_day",
            DoubleType(),
            nullable=True,
        ),
        StructField(
            "adjusted_weekend_total_nurse_staffing_hours_per_resident_per_day",
            DoubleType(),
            nullable=True,
        ),
        StructField(
            "rating_cycle_1_standard_survey_health_date", DateType(), nullable=True
        ),
        StructField(
            "rating_cycle_1_standard_survey_footnote", IntegerType(), nullable=True
        ),
        StructField(
            "rating_cycle_1_total_number_of_health_deficiencies",
            IntegerType(),
            nullable=True,
        ),
        StructField(
            "rating_cycle_1_number_of_standard_health_deficiencies",
            IntegerType(),
            nullable=True,
        ),
        StructField(
            "rating_cycle_1_number_of_complaint_health_deficiencies",
            IntegerType(),
            nullable=True,
        ),
        StructField(
            "rating_cycle_1_health_deficiency_score", IntegerType(), nullable=True
        ),
        StructField(
            "rating_cycle_1_number_of_health_revisits",
            IntegerType(),
            nullable=True,
        ),
        StructField(
            "rating_cycle_1_health_revisit_score", IntegerType(), nullable=True
        ),
        StructField("rating_cycle_1_total_health_score", IntegerType(), nullable=True),
        StructField(
            "rating_cycle_2_standard_health_survey_date", DateType(), nullable=True
        ),
        StructField(
            "rating_cycle_2_standard_survey_footnote", IntegerType(), nullable=True
        ),
        StructField(
            "rating_cycle_23_total_number_of_health_deficiencies",
            IntegerType(),
            nullable=True,
        ),
        StructField(
            "rating_cycle_2_number_of_standard_health_deficiencies",
            IntegerType(),
            nullable=True,
        ),
        StructField(
            "rating_cycle_23_number_of_complaint_health_deficiencies",
            IntegerType(),
            nullable=True,
        ),
        StructField(
            "rating_cycle_23_health_deficiency_score", IntegerType(), nullable=True
        ),
        StructField(
            "rating_cycle_23_number_of_health_revisits",
            IntegerType(),
            nullable=True,
        ),
        StructField(
            "rating_cycle_23_health_revisit_score", IntegerType(), nullable=True
        ),
        StructField("rating_cycle_23_total_health_score", IntegerType(), nullable=True),
        StructField("total_weighted_health_survey_score", DoubleType(), nullable=True),
        StructField(
            "number_of_citations_from_infection_control_inspections",
            IntegerType(),
            nullable=True,
        ),
        StructField("number_of_fines", IntegerType(), nullable=True),
        StructField(
            "total_amount_of_fines_in_dollars",
            DecimalType(14, 2),
            nullable=True,
        ),
        StructField("number_of_payment_denials", IntegerType(), nullable=True),
        StructField("total_number_of_penalties", IntegerType(), nullable=True),
        *SILVER_LINEAGE.fields,
    ]
)

SILVER_SCHEMAS: dict[str, StructType] = {
    "health_deficiencies": SILVER_DEFICIENCY_SCHEMA,
    "penalties": SILVER_PENALTY_SCHEMA,
    "provider_info": SILVER_FACILITY_SCHEMA,
    "mds_quality": SILVER_MDS_QUALITY_SCHEMA,
}

SILVER_TABLE_NAMES = {
    "health_deficiencies": "silver_deficiency",
    "penalties": "silver_penalty",
    "provider_info": "silver_facility",
    "mds_quality": "silver_mds_quality",
}

SILVER_ENTITY_KEYS = {
    "health_deficiencies": "deficiency_key",
    "penalties": "penalty_key",
    "provider_info": "facility_key",
    "mds_quality": "mds_key",
}

SILVER_BUSINESS_KEYS: dict[str, tuple[str, ...]] = {
    "health_deficiencies": (
        "cms_certification_number_ccn",
        "survey_date",
        "survey_type",
        "deficiency_prefix",
        "deficiency_tag_number",
        "inspection_cycle",
    ),
    "penalties": (
        "cms_certification_number_ccn",
        "penalty_date",
        "penalty_type",
        "fine_id",
        "payment_denial_start_date",
    ),
    "provider_info": ("cms_certification_number_ccn",),
    "mds_quality": (
        "cms_certification_number_ccn",
        "measure_code",
        "resident_type",
        "measure_period",
    ),
}

SILVER_LINEAGE_COLUMNS = tuple(field.name for field in SILVER_LINEAGE.fields)
SILVER_PRIVACY_DROPPED_FIELDS = (
    "telephone_number",
    "provider_address",
    "location",
)
ENTITY_KEY_SERIALIZATION = (
    "SHA-256 over ordered JSON of normalized business-key columns with explicit "
    "null fields; business-key fields that are required must be non-null first."
)
ROW_HASH_SERIALIZATION = (
    "SHA-256 over ordered JSON of retained business columns with explicit null "
    "fields; exclude entity key, lineage fields, and operational timestamps."
)
ADDRESS_HASH_SPECIFICATION = (
    "SHA-256 over upper(trim(provider_address)) plus a Databricks-secret salt; "
    "never store the salt or clear-text address in Silver."
)
EXACT_DUPLICATE_POLICY = (
    "Collapse rows only when the normalized entity key and business row hash are "
    "identical within the source slice."
)
CONFLICTING_KEY_POLICY = (
    "If one normalized entity key has multiple business row hashes in the same "
    "source slice, treat it as a contract conflict; do not silently choose a row."
)


@dataclass(frozen=True)
class SilverValidationSpec:
    """Declarative Step 6 rules consumed by future Step 7 transformations."""

    dataset: str
    table_name: str
    entity_key: str
    business_key: tuple[str, ...]
    required_source_fields: tuple[str, ...]
    safe_cast_fields: tuple[str, ...]
    source_renames: tuple[tuple[str, str], ...] = (
        ("processing_date", "source_processing_date"),
    )
    string_normalization: str = (
        "Trim every source string and convert a trimmed empty string to null "
        "before casting or domain validation."
    )
    uppercase_fields: tuple[str, ...] = ()
    boolean_yn_fields: tuple[str, ...] = ()
    regex_rules: tuple[tuple[str, str], ...] = ()
    allowed_values: tuple[tuple[str, tuple[str, ...]], ...] = ()
    numeric_ranges: tuple[tuple[str, float | None, float | None], ...] = ()
    non_negative_fields: tuple[str, ...] = ()
    conditional_rules: tuple[str, ...] = ()
    privacy_dropped_fields: tuple[str, ...] = SILVER_PRIVACY_DROPPED_FIELDS
    exact_duplicate_policy: str = EXACT_DUPLICATE_POLICY
    conflicting_key_policy: str = CONFLICTING_KEY_POLICY
    entity_key_specification: str = ENTITY_KEY_SERIALIZATION
    row_hash_specification: str = ROW_HASH_SERIALIZATION
    address_hash_specification: str = ADDRESS_HASH_SPECIFICATION


def _safe_cast_fields_for(dataset: str) -> tuple[str, ...]:
    """List source-projected fields that require non-throwing typed casts."""

    cast_types = (DateType, DecimalType, DoubleType, IntegerType)
    return tuple(
        field.name
        for field in SILVER_SCHEMAS[dataset].fields
        if isinstance(field.dataType, cast_types)
    )


_FACILITY_BOOLEAN_FIELDS = (
    "urban",
    "provider_resides_in_hospital",
    "continuing_care_retirement_community",
    "abuse_icon",
    "high_performing_icon",
    "most_recent_health_inspection_more_than_2_years_ago",
    "provider_changed_ownership_in_last_12_months",
)

_FACILITY_RATING_FIELDS = (
    "chain_average_overall_5star_rating",
    "chain_average_health_inspection_rating",
    "chain_average_staffing_rating",
    "chain_average_qm_rating",
    "overall_rating",
    "health_inspection_rating",
    "qm_rating",
    "longstay_qm_rating",
    "shortstay_qm_rating",
    "staffing_rating",
)

_FACILITY_NON_NEGATIVE_FIELDS = (
    "number_of_certified_beds",
    "average_number_of_residents_per_day",
    "number_of_facilities_in_chain",
    "chain_average_overall_5star_rating",
    "chain_average_health_inspection_rating",
    "chain_average_staffing_rating",
    "chain_average_qm_rating",
    "reported_nurse_aide_staffing_hours_per_resident_per_day",
    "reported_lpn_staffing_hours_per_resident_per_day",
    "reported_rn_staffing_hours_per_resident_per_day",
    "reported_licensed_staffing_hours_per_resident_per_day",
    "reported_total_nurse_staffing_hours_per_resident_per_day",
    "total_number_of_nurse_staff_hours_per_resident_per_day_on_t_4a14",
    "registered_nurse_hours_per_resident_per_day_on_the_weekend",
    "reported_physical_therapist_staffing_hours_per_resident_per_day",
    "total_nursing_staff_turnover",
    "registered_nurse_turnover",
    "number_of_administrators_who_have_left_the_nursing_home",
    "nursing_casemix_index",
    "nursing_casemix_index_ratio",
    "casemix_nurse_aide_staffing_hours_per_resident_per_day",
    "casemix_lpn_staffing_hours_per_resident_per_day",
    "casemix_rn_staffing_hours_per_resident_per_day",
    "casemix_total_nurse_staffing_hours_per_resident_per_day",
    "casemix_weekend_total_nurse_staffing_hours_per_resident_per_day",
    "adjusted_nurse_aide_staffing_hours_per_resident_per_day",
    "adjusted_lpn_staffing_hours_per_resident_per_day",
    "adjusted_rn_staffing_hours_per_resident_per_day",
    "adjusted_total_nurse_staffing_hours_per_resident_per_day",
    "adjusted_weekend_total_nurse_staffing_hours_per_resident_per_day",
    "rating_cycle_1_total_number_of_health_deficiencies",
    "rating_cycle_1_number_of_standard_health_deficiencies",
    "rating_cycle_1_number_of_complaint_health_deficiencies",
    "rating_cycle_1_health_deficiency_score",
    "rating_cycle_1_number_of_health_revisits",
    "rating_cycle_1_health_revisit_score",
    "rating_cycle_1_total_health_score",
    "rating_cycle_23_total_number_of_health_deficiencies",
    "rating_cycle_2_number_of_standard_health_deficiencies",
    "rating_cycle_23_number_of_complaint_health_deficiencies",
    "rating_cycle_23_health_deficiency_score",
    "rating_cycle_23_number_of_health_revisits",
    "rating_cycle_23_health_revisit_score",
    "rating_cycle_23_total_health_score",
    "total_weighted_health_survey_score",
    "number_of_citations_from_infection_control_inspections",
    "number_of_fines",
    "total_amount_of_fines_in_dollars",
    "number_of_payment_denials",
    "total_number_of_penalties",
)

SILVER_VALIDATION_SPECS: dict[str, SilverValidationSpec] = {
    "health_deficiencies": SilverValidationSpec(
        dataset="health_deficiencies",
        table_name="silver_deficiency",
        entity_key="deficiency_key",
        business_key=SILVER_BUSINESS_KEYS["health_deficiencies"],
        required_source_fields=SILVER_BUSINESS_KEYS["health_deficiencies"],
        safe_cast_fields=_safe_cast_fields_for("health_deficiencies"),
        uppercase_fields=(
            "cms_certification_number_ccn",
            "state",
            "deficiency_prefix",
            "scope_severity_code",
        ),
        boolean_yn_fields=(
            "standard_deficiency",
            "complaint_deficiency",
            "infection_control_inspection_deficiency",
            "citation_under_idr",
            "citation_under_iidr",
        ),
        regex_rules=(
            ("cms_certification_number_ccn", r"^[0-9A-Z]{6}$"),
            ("state", r"^[A-Z]{2}$"),
            ("zip_code", r"^[0-9]{5}$"),
            ("deficiency_tag_number", r"^[0-9]{4}$"),
        ),
        allowed_values=(
            ("scope_severity_code", tuple("ABCDEFGHIJKL")),
            ("inspection_cycle", ("1", "2", "3")),
        ),
        conditional_rules=(
            "correction_date is required when deficiency_corrected states that a "
            "correction date exists",
            "correction_date may precede survey_date, including Past Non-Compliance",
            "severity_group is derived as A-C, D-F, G-I, or J-L",
        ),
    ),
    "penalties": SilverValidationSpec(
        dataset="penalties",
        table_name="silver_penalty",
        entity_key="penalty_key",
        business_key=SILVER_BUSINESS_KEYS["penalties"],
        required_source_fields=(
            "cms_certification_number_ccn",
            "penalty_date",
            "penalty_type",
        ),
        safe_cast_fields=_safe_cast_fields_for("penalties"),
        uppercase_fields=("cms_certification_number_ccn", "state"),
        regex_rules=(
            ("cms_certification_number_ccn", r"^[0-9A-Z]{6}$"),
            ("state", r"^[A-Z]{2}$"),
            ("zip_code", r"^[0-9]{5}$"),
        ),
        allowed_values=(("penalty_type", ("Fine", "Payment Denial")),),
        non_negative_fields=("fine_amount", "payment_denial_length_in_days"),
        conditional_rules=(
            "Fine requires fine_id and fine_amount, and requires both payment-denial "
            "fields to be null",
            "Payment Denial requires payment_denial_start_date and "
            "payment_denial_length_in_days, and requires both fine fields to be null",
            "Null subtype components are represented explicitly in entity-key JSON",
        ),
    ),
    "provider_info": SilverValidationSpec(
        dataset="provider_info",
        table_name="silver_facility",
        entity_key="facility_key",
        business_key=SILVER_BUSINESS_KEYS["provider_info"],
        required_source_fields=SILVER_BUSINESS_KEYS["provider_info"],
        safe_cast_fields=_safe_cast_fields_for("provider_info"),
        uppercase_fields=("cms_certification_number_ccn", "state"),
        boolean_yn_fields=_FACILITY_BOOLEAN_FIELDS,
        regex_rules=(
            ("cms_certification_number_ccn", r"^[0-9A-Z]{6}$"),
            ("state", r"^[A-Z]{2}$"),
            ("zip_code", r"^[0-9]{5}$"),
        ),
        allowed_values=(
            ("special_focus_status", ("SFF", "SFF Candidate")),
            (
                "with_a_resident_and_family_council",
                ("Resident", "Family", "Both", "None"),
            ),
            (
                "automatic_sprinkler_systems_in_all_required_areas",
                ("Yes", "Partial", "No", "Data Not Available"),
            ),
        ),
        numeric_ranges=(
            *((field, 1, 5) for field in _FACILITY_RATING_FIELDS),
            ("latitude", -90, 90),
            ("longitude", -180, 180),
        ),
        non_negative_fields=_FACILITY_NON_NEGATIVE_FIELDS,
        conditional_rules=(
            "Phase 2 facility behavior is SCD Type 1; no valid_from, valid_to, or "
            "is_current fields are allowed",
            "Missing measures remain null and are never imputed as zero",
        ),
    ),
    "mds_quality": SilverValidationSpec(
        dataset="mds_quality",
        table_name="silver_mds_quality",
        entity_key="mds_key",
        business_key=SILVER_BUSINESS_KEYS["mds_quality"],
        required_source_fields=(
            *SILVER_BUSINESS_KEYS["mds_quality"],
            "used_in_quality_measure_five_star_rating",
        ),
        safe_cast_fields=_safe_cast_fields_for("mds_quality"),
        uppercase_fields=("cms_certification_number_ccn", "state"),
        boolean_yn_fields=("used_in_quality_measure_five_star_rating",),
        regex_rules=(
            ("cms_certification_number_ccn", r"^[0-9A-Z]{6}$"),
            ("state", r"^[A-Z]{2}$"),
            ("zip_code", r"^[0-9]{5}$"),
            ("measure_code", r"^[0-9A-Z]{3}$"),
            ("measure_period", r"^[0-9]{4}Q[1-4]-[0-9]{4}Q[1-4]$"),
        ),
        allowed_values=(
            ("resident_type", ("Long Stay", "Short Stay")),
        ),
        numeric_ranges=tuple(
            (field, 0, 100)
            for field in (
                "q1_measure_score",
                "q2_measure_score",
                "q3_measure_score",
                "q4_measure_score",
                "four_quarter_average_score",
            )
        ),
        conditional_rules=(
            "Quarterly scores may be null and must not be imputed from the "
            "four-quarter average",
        ),
    ),
}


def silver_schema(dataset: str) -> StructType:
    """Return the locked explicit Silver schema for a configured dataset."""

    try:
        return SILVER_SCHEMAS[dataset]
    except KeyError as exc:
        allowed = ", ".join(sorted(SILVER_SCHEMAS))
        raise ValueError(f"Unknown dataset {dataset!r}. Expected one of: {allowed}") from exc


def silver_validation_spec(dataset: str) -> SilverValidationSpec:
    """Return declarative normalization and validation rules for a dataset."""

    try:
        return SILVER_VALIDATION_SPECS[dataset]
    except KeyError as exc:
        allowed = ", ".join(sorted(SILVER_VALIDATION_SPECS))
        raise ValueError(f"Unknown dataset {dataset!r}. Expected one of: {allowed}") from exc


def silver_business_columns(dataset: str) -> tuple[str, ...]:
    """Return the ordered columns included in the deterministic row hash."""

    schema = silver_schema(dataset)
    excluded = {SILVER_ENTITY_KEYS[dataset], *SILVER_LINEAGE_COLUMNS}
    return tuple(field.name for field in schema.fields if field.name not in excluded)

INCREMENTAL_ENVELOPE_SCHEMAS = {
    dataset: StructType(
        [
            StructField("_meta", INCREMENTAL_META, nullable=False),
            StructField(
                "results",
                ArrayType(SOURCE_SCHEMAS[dataset], containsNull=False),
                nullable=False,
            ),
        ]
    )
    for dataset in ("health_deficiencies", "penalties")
}


def normalize_header_key(name: str) -> str:
    """Normalize display and API headers to a punctuation-insensitive key."""

    return re.sub(r"[^a-z0-9]", "", name.lower())


def source_columns(dataset: str) -> tuple[str, ...]:
    """Return canonical CMS API columns for a configured dataset."""

    try:
        return SOURCE_COLUMNS[dataset]
    except KeyError as exc:
        allowed = ", ".join(sorted(SOURCE_COLUMNS))
        raise ValueError(f"Unknown dataset {dataset!r}. Expected one of: {allowed}") from exc


def source_schema(dataset: str) -> StructType:
    """Return the explicit all-string source schema."""

    source_columns(dataset)
    return SOURCE_SCHEMAS[dataset]


def csv_read_schema(dataset: str) -> StructType:
    """Return the explicit CSV read schema including the quarantine-only field."""

    source_columns(dataset)
    return CSV_READ_SCHEMAS[dataset]


def bronze_schema(dataset: str) -> StructType:
    """Return accepted Bronze fields: source strings plus typed lineage metadata."""

    source_columns(dataset)
    return BRONZE_SCHEMAS[dataset]


def incremental_envelope_schema(dataset: str) -> StructType:
    """Return the explicit API-page envelope schema for an event dataset."""

    try:
        return INCREMENTAL_ENVELOPE_SCHEMAS[dataset]
    except KeyError as exc:
        allowed = ", ".join(sorted(INCREMENTAL_ENVELOPE_SCHEMAS))
        raise ValueError(
            f"Dataset {dataset!r} has no API incremental envelope. Expected: {allowed}"
        ) from exc


def bulk_header_mapping(dataset: str, headers: Iterable[str]) -> dict[str, str]:
    """Resolve bulk display headers to canonical API names, or fail closed.

    The returned mapping is ``{bulk_header: canonical_name}``. Missing, added,
    duplicated, or ambiguously normalized headers are contract failures here;
    the drift layer decides later how a pipeline run records/quarantines them.
    """

    expected_columns = source_columns(dataset)
    normalized_expected: dict[str, str] = {}
    for column in expected_columns:
        normalized = normalize_header_key(column)
        if normalized in normalized_expected:
            raise RuntimeError(
                f"Canonical columns normalize ambiguously: "
                f"{normalized_expected[normalized]!r}, {column!r}"
            )
        normalized_expected[normalized] = column

    overrides = HEADER_OVERRIDES.get(dataset, {})
    mapping: dict[str, str] = {}
    unresolved: list[str] = []
    mapped_columns: set[str] = set()

    for header in headers:
        canonical = overrides.get(header)
        if canonical is None:
            canonical = normalized_expected.get(normalize_header_key(header))
        if canonical is None:
            unresolved.append(header)
            continue
        if canonical in mapped_columns:
            raise ValueError(
                f"Multiple bulk headers resolve to canonical column {canonical!r}"
            )
        mapping[header] = canonical
        mapped_columns.add(canonical)

    missing = [column for column in expected_columns if column not in mapped_columns]
    if unresolved or missing:
        details = []
        if unresolved:
            details.append(f"unresolved headers={unresolved}")
        if missing:
            details.append(f"missing canonical columns={missing}")
        raise ValueError(f"{dataset} bulk header contract failed: {'; '.join(details)}")
    return mapping


def _validate_contract_constants() -> None:
    for dataset, columns in SOURCE_COLUMNS.items():
        expected_count = EXPECTED_SOURCE_COLUMN_COUNTS[dataset]
        if len(columns) != expected_count:
            raise RuntimeError(
                f"{dataset} declares {len(columns)} source columns; "
                f"contract requires {expected_count}"
            )
        if len(set(columns)) != len(columns):
            raise RuntimeError(f"{dataset} contains duplicate source columns")

    for dataset, overrides in HEADER_OVERRIDES.items():
        unknown_targets = set(overrides.values()) - set(SOURCE_COLUMNS[dataset])
        if unknown_targets:
            raise RuntimeError(
                f"{dataset} header overrides target unknown columns: "
                f"{sorted(unknown_targets)}"
            )


_validate_contract_constants()
