"""Versioned source contracts and parameter validation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class SourceConfig:
    source_id: str
    dataset_code: str
    bronze_table: str
    silver_table: str
    frequency: str
    expected_unit: str
    date_formats: tuple[str, ...]


@dataclass(frozen=True)
class FullLoadParameters:
    catalog: str
    source_id: str
    input_path: str
    batch_id: str
    run_id: str
    manifest_id: str
    source_file_hash: str
    source_snapshot_at: datetime
    retrieved_at_utc: datetime
    code_version: str = "local"

    def validate(self) -> None:
        if self.source_id not in SBP_SOURCES:
            raise ValueError(f"Unsupported structured full-load source: {self.source_id}")
        for name in ("catalog", "input_path", "batch_id", "run_id", "manifest_id", "source_file_hash"):
            if not getattr(self, name):
                raise ValueError(f"{name} must not be empty")
        if ".." in self.catalog or any(ch in self.catalog for ch in " /\\;'"):
            raise ValueError("catalog contains unsafe characters")


SBP_SOURCES: dict[str, SourceConfig] = {
    "SBP_REMITTANCES": SourceConfig("SBP_REMITTANCES", "TS_GP_BOP_WR_M", "bronze.sbp_remittances_raw", "silver.sbp_remittances", "monthly", "Million USD", ("dd-MMM-yyyy", "yyyy-MM-dd", "MMM-yyyy")),
    "SBP_FDI": SourceConfig("SBP_FDI", "TS_GP_BOP_FDIISIC4_M", "bronze.sbp_fdi_raw", "silver.sbp_fdi", "monthly", "Million USD", ("dd-MMM-yyyy", "yyyy-MM-dd", "MMM-yyyy")),
    "SBP_FX": SourceConfig("SBP_FX", "TS_GP_ES_FADERPKR_M", "bronze.sbp_fx_raw", "silver.sbp_exchange_rates", "daily", "PKR", ("dd-MMM-yyyy", "yyyy-MM-dd", "MM/dd/yyyy")),
    "SBP_EXPORTS": SourceConfig("SBP_EXPORTS", "TS_GP_BOP_XRECCOM_M", "bronze.sbp_exports_raw", "silver.sbp_export_receipts", "monthly", "Thousand USD", ("dd-MMM-yyyy", "yyyy-MM-dd", "MMM-yyyy")),
    "SBP_IMPORTS": SourceConfig("SBP_IMPORTS", "TS_GP_BOP_MRECCOM_M", "bronze.sbp_imports_raw", "silver.sbp_import_payments", "monthly", "Thousand USD", ("dd-MMM-yyyy", "yyyy-MM-dd", "MMM-yyyy")),
}


def qualify(catalog: str, table_name: str) -> str:
    schema, table = table_name.split(".", maxsplit=1)
    return f"`{catalog}`.`{schema}`.`{table}`"
