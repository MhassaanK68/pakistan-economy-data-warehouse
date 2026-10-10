"""CMS-to-Landing acquisition for full snapshots and API incrementals.

This module intentionally has no Spark dependency. It downloads immutable landing
artifacts and returns manifest-ready records; the Databricks notebook persists them.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import unquote, urlparse

import requests

from carewatch.config import DatasetConfig, get_dataset


CMS_API_BASE = "https://data.cms.gov/provider-data/api/1"
DEFAULT_PAGE_SIZE = 500
DEFAULT_OVERLAP_DAYS = 60
USER_AGENT = "carewatch-medallion/1.0 (academic data engineering project)"


@dataclass(frozen=True)
class AcquisitionRequest:
    dataset: str
    load_type: str
    as_of_date: date
    landing_root: str
    start_date: date | None = None
    overlap_days: int = DEFAULT_OVERLAP_DAYS
    force_refresh: bool = False
    acquisition_run_id: str = ""

    def __post_init__(self) -> None:
        get_dataset(self.dataset)
        if self.load_type not in {"full", "incremental"}:
            raise ValueError("load_type must be 'full' or 'incremental'")
        if self.overlap_days < 0:
            raise ValueError("overlap_days cannot be negative")
        if not self.landing_root.strip():
            raise ValueError("landing_root is required")
        if not self.acquisition_run_id:
            object.__setattr__(self, "acquisition_run_id", str(uuid.uuid4()))


@dataclass(frozen=True)
class AcquiredFile:
    acquisition_run_id: str
    dataset: str
    dataset_id: str
    load_type: str
    acquisition_strategy: str
    source_url: str
    source_catalog_modified: date | None
    window_start: date | None
    window_end: date | None
    page_offset: int | None
    landing_path: str
    batch_id: str
    source_content_sha256: str
    source_file_sha256: str
    source_bytes: int
    expected_run_rows: int | None
    source_rows: int | None
    row_count_validated: bool
    status: str
    error_message: str | None
    load_timestamp: datetime

    def as_manifest_record(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AcquisitionResult:
    acquisition_run_id: str
    dataset: str
    load_type: str
    strategy: str
    files: tuple[AcquiredFile, ...]
    expected_rows: int | None
    acquired_rows: int | None


def parse_iso_date(value: str, field_name: str, *, required: bool = False) -> date | None:
    """Parse a YYYY-MM-DD widget value."""

    text = value.strip()
    if not text:
        if required:
            raise ValueError(f"{field_name} is required and must use YYYY-MM-DD")
        return None
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{field_name} must use YYYY-MM-DD, got {value!r}") from exc


def parse_bool(value: str, field_name: str) -> bool:
    normalized = value.strip().lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    raise ValueError(f"{field_name} must be 'true' or 'false', got {value!r}")


def resolve_window_start(
    explicit_start: date | None,
    last_successful_watermark: date | None,
    overlap_days: int,
) -> date:
    """Resolve an incremental lower bound without inventing an initial watermark."""

    if explicit_start is not None:
        return explicit_start
    if last_successful_watermark is None:
        raise ValueError(
            "No successful Silver watermark exists. Run the initial full load or provide start_date."
        )
    return last_successful_watermark - timedelta(days=overlap_days)


def create_http_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json, text/csv, */*"})
    return session


def _get_json(
    session: requests.Session,
    url: str,
    *,
    params: Mapping[str, Any] | None = None,
    retries: int = 4,
) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            response = session.get(url, params=params, timeout=(30, 120))
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError(f"CMS returned non-object JSON for {url}")
            return payload
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            if attempt + 1 < retries:
                time.sleep(2**attempt)
    raise RuntimeError(f"CMS request failed after {retries} attempts: {url}") from last_error


def get_catalog_item(
    session: requests.Session, dataset_id: str
) -> tuple[dict[str, Any], str]:
    url = f"{CMS_API_BASE}/metastore/schemas/dataset/items/{dataset_id}"
    return _get_json(session, url), url


def get_dataset_count(session: requests.Session, dataset_id: str) -> int:
    """Read the current datastore row count for full-snapshot reconciliation."""

    url = f"{CMS_API_BASE}/datastore/query/{dataset_id}/0"
    payload = _get_json(
        session,
        url,
        params={"limit": 1, "offset": 0, "count": "true", "schema": "false"},
    )
    if payload.get("count") is None:
        raise ValueError(f"CMS did not return a row count for dataset {dataset_id}")
    return int(payload["count"])


def select_csv_distribution(item: Mapping[str, Any]) -> str:
    """Select the official bulk CSV URL from a CMS metastore item."""

    distributions = item.get("distribution")
    if not isinstance(distributions, list) or not distributions:
        raise ValueError("CMS catalog item has no distributions")

    for distribution in distributions:
        if not isinstance(distribution, Mapping):
            continue
        url = distribution.get("downloadURL")
        if not isinstance(url, str) or not url.startswith("https://"):
            continue
        media = str(distribution.get("mediaType", "")).lower()
        fmt = str(distribution.get("format", "")).lower()
        path = urlparse(url).path.lower()
        if "csv" in media or fmt == "csv" or path.endswith(".csv"):
            return url

    raise ValueError("CMS catalog item does not expose an official CSV distribution")


def _safe_filename_from_url(url: str, dataset: str) -> str:
    name = unquote(Path(urlparse(url).path).name) or f"{dataset}.csv"
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", name)
    if not name.lower().endswith(".csv"):
        name += ".csv"
    return name


def _catalog_modified(item: Mapping[str, Any]) -> date | None:
    value = item.get("modified")
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _batch_id(dataset: str, load_type: str, source_identity: str, digest: str) -> str:
    """Derive a batch ID that is stable across acquisition-run directories."""

    identity = f"{dataset}|{load_type}|{source_identity}|{digest}"
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()[:24]


def _sha256_file(path: str) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with open(path, "rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _stream_to_temporary_file(
    session: requests.Session,
    url: str,
    temporary_path: Path,
    *,
    retries: int = 3,
) -> tuple[str, int]:
    """Stream a remote object and return its SHA-256 and byte count."""

    last_error: Exception | None = None
    temporary_path.parent.mkdir(parents=True, exist_ok=True)
    for attempt in range(retries):
        digest = hashlib.sha256()
        size = 0
        try:
            with session.get(url, stream=True, timeout=(30, 300)) as response:
                response.raise_for_status()
                declared_length = response.headers.get("Content-Length")
                with temporary_path.open("wb") as output:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            output.write(chunk)
                            digest.update(chunk)
                            size += len(chunk)
            if size == 0:
                raise ValueError("CMS returned an empty bulk file")
            if declared_length is not None and size != int(declared_length):
                raise ValueError(
                    f"Incomplete bulk download: expected {declared_length} bytes, received {size}"
                )
            return digest.hexdigest(), size
        except (requests.RequestException, OSError, ValueError) as exc:
            last_error = exc
            temporary_path.unlink(missing_ok=True)
            if attempt + 1 < retries:
                time.sleep(2**attempt)
    raise RuntimeError(f"Bulk download failed after {retries} attempts: {url}") from last_error


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> tuple[str, int]:
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()
    temporary_path = path.with_name(f".{path.name}.{uuid.uuid4().hex}.partial")
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        temporary_path.write_bytes(encoded)
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)
    return digest, len(encoded)


def _landing_directory(request: AcquisitionRequest, strategy: str) -> Path:
    load_folder = "full_load" if request.load_type == "full" else "incremental"
    return (
        Path(request.landing_root)
        / load_folder
        / request.dataset
        / request.as_of_date.isoformat()
        / request.acquisition_run_id
    )


def _acquire_bulk_snapshot(
    request: AcquisitionRequest,
    config: DatasetConfig,
    session: requests.Session,
    existing_files_by_content_hash: Mapping[str, str],
    *,
    strategy: str,
) -> AcquisitionResult:
    item, _ = get_catalog_item(session, config.dataset_id)
    source_url = select_csv_distribution(item)
    landing_dir = _landing_directory(request, strategy)
    source_name = _safe_filename_from_url(source_url, config.name)
    temporary_path = landing_dir / f".{source_name}.{uuid.uuid4().hex}.partial"
    digest, size = _stream_to_temporary_file(session, source_url, temporary_path)
    expected_rows = get_dataset_count(session, config.dataset_id)
    now = datetime.now(timezone.utc)

    existing_path = existing_files_by_content_hash.get(digest)
    if existing_path and not request.force_refresh:
        temporary_path.unlink(missing_ok=True)
        final_path = existing_path
        status = "SKIPPED_ALREADY_ACQUIRED"
    else:
        stem = Path(source_name).stem
        final_path_obj = landing_dir / f"{stem}_{digest[:12]}.csv"
        final_path_obj.parent.mkdir(parents=True, exist_ok=True)
        os.replace(temporary_path, final_path_obj)
        final_path = str(final_path_obj)
        status = "SUCCESS"

    acquired = AcquiredFile(
        acquisition_run_id=request.acquisition_run_id,
        dataset=config.name,
        dataset_id=config.dataset_id,
        load_type=request.load_type,
        acquisition_strategy=strategy,
        source_url=source_url,
        source_catalog_modified=_catalog_modified(item),
        window_start=None,
        window_end=None,
        page_offset=None,
        landing_path=final_path,
        batch_id=_batch_id(config.name, request.load_type, source_url, digest),
        source_content_sha256=digest,
        source_file_sha256=digest,
        source_bytes=size,
        expected_run_rows=expected_rows,
        source_rows=None,
        row_count_validated=False,
        status=status,
        error_message=None,
        load_timestamp=now,
    )
    return AcquisitionResult(
        acquisition_run_id=request.acquisition_run_id,
        dataset=config.name,
        load_type=request.load_type,
        strategy=strategy,
        files=(acquired,),
        expected_rows=expected_rows,
        acquired_rows=None,
    )


def _query_api_page(
    session: requests.Session,
    config: DatasetConfig,
    window_start: date,
    window_end: date,
    offset: int,
    page_size: int,
) -> tuple[dict[str, Any], str, dict[str, Any]]:
    if config.watermark_col is None:
        raise ValueError(f"{config.name} has no API watermark column")
    url = f"{CMS_API_BASE}/datastore/query/{config.dataset_id}/0"
    params: dict[str, Any] = {
        "limit": page_size,
        "offset": offset,
        "count": "true",
        "schema": "false",
        "conditions[0][property]": config.watermark_col,
        "conditions[0][operator]": ">=",
        "conditions[0][value]": window_start.isoformat(),
        "conditions[1][property]": config.watermark_col,
        "conditions[1][operator]": "<=",
        "conditions[1][value]": window_end.isoformat(),
    }
    prepared_url = requests.Request("GET", url, params=params).prepare().url or url
    return _get_json(session, url, params=params), prepared_url, params


def _acquire_api_window(
    request: AcquisitionRequest,
    config: DatasetConfig,
    session: requests.Session,
    existing_files_by_content_hash: Mapping[str, str],
    last_successful_watermark: date | None,
    *,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> AcquisitionResult:
    window_start = resolve_window_start(
        request.start_date, last_successful_watermark, request.overlap_days
    )
    if window_start > request.as_of_date:
        raise ValueError("Incremental start_date cannot be after as_of_date")

    landing_dir = _landing_directory(request, "api_date_window")
    acquired_files: list[AcquiredFile] = []
    created_paths: list[Path] = []
    seen_result_hashes: set[str] = set()
    offset = 0
    expected_rows: int | None = None
    acquired_rows = 0
    filter_text = (
        f"{config.watermark_col} >= {window_start.isoformat()} AND "
        f"{config.watermark_col} <= {request.as_of_date.isoformat()}"
    )

    try:
        while True:
            payload, source_url, _ = _query_api_page(
                session,
                config,
                window_start,
                request.as_of_date,
                offset,
                page_size,
            )
            results = payload.get("results")
            if not isinstance(results, list):
                raise ValueError(f"CMS page at offset {offset} has no results array")

            reported_count = payload.get("count")
            if expected_rows is None:
                expected_rows = int(reported_count) if reported_count is not None else None
            elif reported_count is not None and int(reported_count) != expected_rows:
                raise ValueError("CMS result count changed while paging the same date window")

            results_fingerprint = hashlib.sha256(
                json.dumps(results, sort_keys=True, separators=(",", ":")).encode("utf-8")
            ).hexdigest()
            if results and results_fingerprint in seen_result_hashes:
                raise ValueError(f"CMS repeated a result page at offset {offset}")
            seen_result_hashes.add(results_fingerprint)

            fetched_at = datetime.now(timezone.utc)
            envelope = {
                "_meta": {
                    "dataset_id": config.dataset_id,
                    "filter": filter_text,
                    "rows_available": expected_rows,
                    "rows_in_file": len(results),
                    "fetched_at_utc": fetched_at.isoformat(timespec="seconds"),
                    "load_type": "incremental",
                },
                "results": results,
            }
            content_identity = {
                "dataset_id": config.dataset_id,
                "filter": filter_text,
                "page_offset": offset,
                "results": results,
            }
            content_digest = hashlib.sha256(
                json.dumps(content_identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
            ).hexdigest()
            encoded = json.dumps(
                envelope, ensure_ascii=False, separators=(",", ":")
            ).encode("utf-8")
            file_digest = hashlib.sha256(encoded).hexdigest()
            existing_path = existing_files_by_content_hash.get(content_digest)

            if existing_path and not request.force_refresh:
                final_path = existing_path
                file_digest, size = _sha256_file(existing_path)
                status = "SKIPPED_ALREADY_ACQUIRED"
            else:
                final_path_obj = landing_dir / f"part-{offset:09d}-{content_digest[:12]}.json"
                written_digest, size = _write_json_atomic(final_path_obj, envelope)
                if written_digest != file_digest:
                    raise RuntimeError("JSON content hash changed during atomic write")
                final_path = str(final_path_obj)
                created_paths.append(final_path_obj)
                status = "SUCCESS"

            acquired_files.append(
                AcquiredFile(
                    acquisition_run_id=request.acquisition_run_id,
                    dataset=config.name,
                    dataset_id=config.dataset_id,
                    load_type=request.load_type,
                    acquisition_strategy="api_date_window",
                    source_url=source_url,
                    source_catalog_modified=None,
                    window_start=window_start,
                    window_end=request.as_of_date,
                    page_offset=offset,
                    landing_path=final_path,
                    batch_id=_batch_id(
                        config.name, request.load_type, source_url, content_digest
                    ),
                    source_content_sha256=content_digest,
                    source_file_sha256=file_digest,
                    source_bytes=size,
                    expected_run_rows=expected_rows,
                    source_rows=len(results),
                    row_count_validated=False,
                    status=status,
                    error_message=None,
                    load_timestamp=fetched_at,
                )
            )
            acquired_rows += len(results)

            if len(results) < page_size:
                break
            offset += page_size

        if expected_rows is not None and acquired_rows != expected_rows:
            raise ValueError(
                f"CMS count mismatch: expected {expected_rows:,}, acquired {acquired_rows:,}"
            )
    except Exception:
        for path in created_paths:
            path.unlink(missing_ok=True)
        raise

    return AcquisitionResult(
        acquisition_run_id=request.acquisition_run_id,
        dataset=config.name,
        load_type=request.load_type,
        strategy="api_date_window",
        files=tuple(acquired_files),
        expected_rows=expected_rows,
        acquired_rows=acquired_rows,
    )


def acquire_dataset(
    request: AcquisitionRequest,
    *,
    last_successful_watermark: date | None = None,
    existing_files_by_content_hash: Mapping[str, str] | None = None,
    session: requests.Session | None = None,
) -> AcquisitionResult:
    """Acquire one configured dataset according to the locked Phase 2 strategy."""

    config = get_dataset(request.dataset)
    http = session or create_http_session()
    existing = existing_files_by_content_hash or {}

    if request.load_type == "full":
        return _acquire_bulk_snapshot(
            request, config, http, existing, strategy="bulk_snapshot"
        )
    if config.incremental_strategy == "snapshot_diff":
        return _acquire_bulk_snapshot(
            request, config, http, existing, strategy="snapshot_diff"
        )
    return _acquire_api_window(
        request,
        config,
        http,
        existing,
        last_successful_watermark,
    )
