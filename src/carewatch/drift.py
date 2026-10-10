"""Pure schema-drift classification for CareWatch landing artifacts.

The module deliberately has no Spark dependency so header behaviour can be
tested locally. Spark-specific reads and Delta writes live in ``bronze.py``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, Mapping


_NON_ALNUM = re.compile(r"[^a-z0-9]+")
_NON_IDENTIFIER = re.compile(r"[^a-z0-9_]+")
_MULTIPLE_UNDERSCORES = re.compile(r"_+")
_SQL_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")

BRONZE_RESERVED_COLUMNS = {
    "_dataset_id",
    "_source_file",
    "_source_file_sha256",
    "_batch_id",
    "_load_type",
    "_ingest_date",
    "_corrupt_record",
    "load_timestamp",
}


def normalize_header_key(name: str) -> str:
    """Return the punctuation-insensitive key used by the CMS bulk mapping."""

    return _NON_ALNUM.sub("", name.strip().lower())


def provisional_column_name(name: str) -> str:
    """Create a safe, deterministic Bronze name for a newly added bulk field.

    The source header remains in the drift log. The generated name is only a
    provisional Bronze name until the explicit contract adopts the field.
    """

    value = _NON_IDENTIFIER.sub("_", name.strip().lower())
    value = _MULTIPLE_UNDERSCORES.sub("_", value).strip("_")
    if not value:
        return ""
    if value[0].isdigit():
        value = f"source_{value}"
    return value


@dataclass(frozen=True)
class AddedColumn:
    source_name: str
    bronze_name: str


@dataclass(frozen=True)
class DriftAssessment:
    """Result of resolving one physical source shape to Bronze columns."""

    ordered_columns: tuple[str, ...]
    source_to_bronze: Mapping[str, str]
    missing_columns: tuple[str, ...]
    added_columns: tuple[AddedColumn, ...]
    invalid_columns: tuple[str, ...]
    duplicate_columns: tuple[str, ...]

    @property
    def has_breaking_drift(self) -> bool:
        return bool(
            self.missing_columns
            or self.invalid_columns
            or self.duplicate_columns
        )


def _expected_by_key(expected_columns: Iterable[str]) -> dict[str, str]:
    expected_by_key: dict[str, str] = {}
    for column in expected_columns:
        key = normalize_header_key(column)
        if not key or key in expected_by_key:
            raise ValueError(
                f"Expected source columns normalize ambiguously at {column!r}"
            )
        expected_by_key[key] = column
    return expected_by_key


def classify_csv_header(
    actual_header: Iterable[str],
    expected_columns: Iterable[str],
    overrides: Mapping[str, str] | None = None,
) -> DriftAssessment:
    """Resolve a bulk CSV header while retaining compatible added fields."""

    expected = tuple(expected_columns)
    expected_by_key = _expected_by_key(expected)
    override_map = dict(overrides or {})
    source_to_bronze: dict[str, str] = {}
    ordered: list[str] = []
    added: list[AddedColumn] = []
    invalid: list[str] = []
    duplicate: list[str] = []
    used: set[str] = set()

    for raw_header in actual_header:
        source_name = str(raw_header).lstrip("\ufeff").strip()
        canonical = override_map.get(source_name)
        if canonical is None:
            canonical = expected_by_key.get(normalize_header_key(source_name))

        if canonical is None:
            canonical = provisional_column_name(source_name)
            if (
                not canonical
                or not _SQL_IDENTIFIER.fullmatch(canonical)
                or canonical in BRONZE_RESERVED_COLUMNS
            ):
                invalid.append(source_name)
                continue

        if canonical in used:
            duplicate.append(source_name)
            continue
        if canonical in BRONZE_RESERVED_COLUMNS:
            invalid.append(source_name)
            continue

        if canonical not in expected:
            added.append(AddedColumn(source_name, canonical))

        used.add(canonical)
        source_to_bronze[source_name] = canonical
        ordered.append(canonical)

    missing = tuple(column for column in expected if column not in used)
    return DriftAssessment(
        ordered_columns=tuple(ordered),
        source_to_bronze=source_to_bronze,
        missing_columns=missing,
        added_columns=tuple(added),
        invalid_columns=tuple(invalid),
        duplicate_columns=tuple(duplicate),
    )


def classify_json_keys(
    actual_keys: Iterable[str], expected_columns: Iterable[str]
) -> DriftAssessment:
    """Classify canonical API JSON keys without renaming valid added keys."""

    expected = tuple(expected_columns)
    actual = tuple(dict.fromkeys(str(key) for key in actual_keys))
    actual_set = set(actual)
    missing = tuple(column for column in expected if column not in actual_set)
    added: list[AddedColumn] = []
    invalid: list[str] = []

    for key in actual:
        if key in expected:
            continue
        if not _SQL_IDENTIFIER.fullmatch(key) or key in BRONZE_RESERVED_COLUMNS:
            invalid.append(key)
        else:
            added.append(AddedColumn(key, key))

    ordered = [*expected]
    ordered.extend(item.bronze_name for item in added)
    return DriftAssessment(
        ordered_columns=tuple(ordered),
        source_to_bronze={key: key for key in ordered},
        missing_columns=missing,
        added_columns=tuple(added),
        invalid_columns=tuple(invalid),
        duplicate_columns=(),
    )
