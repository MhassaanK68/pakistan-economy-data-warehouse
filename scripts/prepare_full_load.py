"""Stage the supplied historical files without modifying their bytes.

This script deliberately has no Spark or third-party dependency, so it can be
run before uploading the resulting partitioned folders to a Databricks Volume.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
import uuid
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path


SOURCE_FOLDERS = {
    "sbp_remittances": ("SBP_REMITTANCES", "TS_GP_BOP_WR_M"),
    "sbp_fdi": ("SBP_FDI", "TS_GP_BOP_FDIISIC4_M"),
    "sbp_fx": ("SBP_FX", "TS_GP_ES_FADERPKR_M"),
    "sbp_exports": ("SBP_EXPORTS", "TS_GP_BOP_XRECCOM_M"),
    "sbp_imports": ("SBP_IMPORTS", "TS_GP_BOP_MRECCOM_M"),
    "pbs_spi": ("PBS_SPI", "PBS_SPI_WEEKLY"),
    "pbs_cpi": ("PBS_CPI", "PBS_CPI_MONTHLY"),
    "ogra_fuel": ("OGRA_FUEL", "OGRA_FUEL_PRICES"),
}


@dataclass(frozen=True)
class ManifestRecord:
    manifest_id: str
    batch_id: str
    source_id: str
    dataset_code: str
    source_file_name: str
    source_path: str
    staging_path: str
    file_format: str
    file_size_bytes: int
    sha256_hash: str
    retrieved_at_utc: str
    ingestion_status: str
    created_at_utc: str


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def discover(input_root: Path) -> list[tuple[str, str, Path]]:
    objects: list[tuple[str, str, Path]] = []
    for folder, (source_id, dataset_code) in SOURCE_FOLDERS.items():
        source_dir = input_root / folder
        if not source_dir.is_dir():
            raise FileNotFoundError(f"Missing source folder: {source_dir}")
        files = sorted(path for path in source_dir.iterdir() if path.is_file())
        if not files:
            raise FileNotFoundError(f"No files found in source folder: {source_dir}")
        objects.extend((source_id, dataset_code, path) for path in files)
    return objects


def stage_file(source: Path, destination: Path, expected_hash: str) -> str:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        if sha256_file(destination) != expected_hash:
            raise FileExistsError(
                f"Immutable staging collision: {destination} already exists with different bytes"
            )
        return "SKIPPED_IDENTICAL"
    shutil.copyfile(source, destination)
    if destination.stat().st_size != source.stat().st_size:
        destination.unlink(missing_ok=True)
        raise IOError(f"Byte-count verification failed for {destination}")
    if sha256_file(destination) != expected_hash:
        destination.unlink(missing_ok=True)
        raise IOError(f"SHA-256 verification failed for {destination}")
    return "LANDED"


def prepare(args: argparse.Namespace) -> list[ManifestRecord]:
    input_root = args.input_root.resolve()
    staging_root = args.staging_root.resolve()
    retrieved_at = args.retrieved_at_utc or datetime.now(timezone.utc).isoformat()
    records: list[ManifestRecord] = []

    for source_id, dataset_code, source_path in discover(input_root):
        digest = sha256_file(source_path)
        destination = (
            staging_root
            / f"source={source_id}"
            / f"ingest_date={args.ingest_date}"
            / f"batch_id={args.batch_id}"
            / source_path.name
        )
        status = "DRY_RUN" if args.dry_run else stage_file(source_path, destination, digest)
        manifest_id = str(
            uuid.uuid5(uuid.NAMESPACE_URL, f"{source_id}:{digest}:{args.batch_id}")
        )
        records.append(
            ManifestRecord(
                manifest_id=manifest_id,
                batch_id=args.batch_id,
                source_id=source_id,
                dataset_code=dataset_code,
                source_file_name=source_path.name,
                source_path=str(source_path),
                staging_path=str(destination),
                file_format=source_path.suffix.lower().lstrip("."),
                file_size_bytes=source_path.stat().st_size,
                sha256_hash=digest,
                retrieved_at_utc=retrieved_at,
                ingestion_status=status,
                created_at_utc=datetime.now(timezone.utc).isoformat(),
            )
        )

    if not args.dry_run:
        manifest_path = staging_root / "_manifests" / f"{args.batch_id}.jsonl"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        payload = "".join(json.dumps(asdict(row), sort_keys=True) + "\n" for row in records)
        temp_path = manifest_path.with_suffix(".jsonl.tmp")
        temp_path.write_text(payload, encoding="utf-8")
        temp_path.replace(manifest_path)
    return records


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-root", type=Path, default=Path("data/raw/full"))
    parser.add_argument("--staging-root", type=Path, default=Path("data/staging"))
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--ingest-date", default=date.today().isoformat())
    parser.add_argument("--retrieved-at-utc")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    records = prepare(args)
    counts: dict[str, int] = {}
    for record in records:
        counts[record.ingestion_status] = counts.get(record.ingestion_status, 0) + 1
    print(json.dumps({"batch_id": args.batch_id, "files": len(records), "status_counts": counts}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
