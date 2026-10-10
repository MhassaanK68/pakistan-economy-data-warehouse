"""Static dataset registry and namespace helpers for CareWatch."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal


IncrementalStrategy = Literal["api_date_window", "snapshot_diff"]


@dataclass(frozen=True)
class DatasetConfig:
    """Configuration shared by acquisition, Bronze, and Silver pipelines."""

    name: str
    dataset_id: str
    incremental_strategy: IncrementalStrategy
    watermark_col: str | None
    bronze_table: str
    silver_table: str
    key_cols: tuple[str, ...]


DATASETS: dict[str, DatasetConfig] = {
    "health_deficiencies": DatasetConfig(
        name="health_deficiencies",
        dataset_id="r5ix-sfxw",
        incremental_strategy="api_date_window",
        watermark_col="survey_date",
        bronze_table="bronze_nh_health_deficiencies",
        silver_table="silver_deficiency",
        key_cols=(
            "cms_certification_number_ccn",
            "survey_date",
            "survey_type",
            "deficiency_prefix",
            "deficiency_tag_number",
            "inspection_cycle",
        ),
    ),
    "penalties": DatasetConfig(
        name="penalties",
        dataset_id="g6vv-u9sr",
        incremental_strategy="api_date_window",
        watermark_col="penalty_date",
        bronze_table="bronze_nh_penalties",
        silver_table="silver_penalty",
        key_cols=(
            "cms_certification_number_ccn",
            "penalty_date",
            "penalty_type",
            "fine_id",
            "payment_denial_start_date",
        ),
    ),
    "provider_info": DatasetConfig(
        name="provider_info",
        dataset_id="4pq5-n9py",
        incremental_strategy="snapshot_diff",
        watermark_col=None,
        bronze_table="bronze_nh_provider_info",
        silver_table="silver_facility",
        key_cols=("cms_certification_number_ccn",),
    ),
    "mds_quality": DatasetConfig(
        name="mds_quality",
        dataset_id="djen-97ju",
        incremental_strategy="snapshot_diff",
        watermark_col=None,
        bronze_table="bronze_nh_mds_quality",
        silver_table="silver_mds_quality",
        key_cols=(
            "cms_certification_number_ccn",
            "measure_code",
            "resident_type",
            "measure_period",
        ),
    ),
}


_SQL_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def get_dataset(name: str) -> DatasetConfig:
    """Return a dataset configuration with a useful error for invalid widget input."""

    try:
        return DATASETS[name]
    except KeyError as exc:
        allowed = ", ".join(sorted(DATASETS))
        raise ValueError(f"Unknown dataset {name!r}. Expected one of: {allowed}") from exc


def qualified_name(catalog: str, schema: str, object_name: str) -> str:
    """Build a safe three-part Databricks object name."""

    parts = (catalog, schema, object_name)
    invalid = [part for part in parts if not _SQL_IDENTIFIER.fullmatch(part)]
    if invalid:
        raise ValueError(f"Unsafe catalog/schema/object identifier: {invalid[0]!r}")
    return ".".join(parts)
