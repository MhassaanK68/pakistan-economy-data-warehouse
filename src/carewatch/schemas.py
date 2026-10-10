"""Explicit source and Bronze schemas for the four CareWatch CMS datasets.

Bronze deliberately preserves every CMS source value as a nullable string.  The
only typed Bronze fields are ingestion metadata owned by this pipeline.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

from pyspark.sql.types import (
    ArrayType,
    DateType,
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
