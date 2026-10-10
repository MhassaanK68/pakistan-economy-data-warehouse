import argparse
import csv
import io
import json
import re
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

BASE = "https://data.cms.gov/provider-data/api/1"
PAGE = 500
MIB = 1024 * 1024
GITHUB_HARD_LIMIT = 100 * MIB


REQUIRED = [
    ("health_deficiencies", "r5ix-sfxw", "survey_date", "nh_health_deficiencies_full"),
    ("provider_info",       "4pq5-n9py", None,          "nh_provider_info_full"),
    ("penalties",           "g6vv-u9sr", "penalty_date", "nh_penalties_full"),
    ("mds_quality",         "djen-97ju", None,          "nh_mds_quality_measures_full"),
]

FALLBACK = [
    ("fire_safety_deficiencies", "ifjz-ge4w", None, "nh_fire_safety_deficiencies_full"),
    ("claims_quality",           "ijh5-nb2v", None, "nh_claims_quality_measures_full"),
]

INCREMENTAL = {"health_deficiencies": "survey_date", "penalties": "penalty_date"}

session = requests.Session()
session.headers["User-Agent"] = "course-project-phase1/1.0 (student data engineering project)"


def get_json(url, params=None, retries=4):
    for attempt in range(retries):
        try:
            r = session.get(url, params=params, timeout=60)
            r.raise_for_status()
            return r.json()
        except requests.RequestException as exc:
            wait = 2 ** attempt
            print(f"  request failed ({exc}); retrying in {wait}s", file=sys.stderr)
            time.sleep(wait)
    raise RuntimeError(f"Giving up on {url}")


def catalog_item(dataset_id):
    return get_json(f"{BASE}/metastore/schemas/dataset/items/{dataset_id}")


def query(dataset_id, conditions=None, max_rows=None):
    """Page through the datastore. conditions = [(property, operator, value), ...]"""
    params = {"limit": PAGE, "offset": 0, "count": "true", "schema": "false"}
    for i, (prop, op, val) in enumerate(conditions or []):
        params[f"conditions[{i}][property]"] = prop
        params[f"conditions[{i}][operator]"] = op
        params[f"conditions[{i}][value]"] = val
    rows, total = [], None
    while True:
        data = get_json(f"{BASE}/datastore/query/{dataset_id}/0", params)
        total = data.get("count", total)
        batch = data.get("results", [])
        rows.extend(batch)
        if not batch or len(batch) < PAGE:
            break
        if max_rows and len(rows) >= max_rows:
            break
        params["offset"] += PAGE
        time.sleep(0.5)
    return (rows[:max_rows] if max_rows else rows), total


    # ----------------------------------------------------------------------------- bulk download
def download(url, dest, retries=3):
    """Stream the bulk CSV to disk (no memory blow-up)."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    for attempt in range(retries):
        try:
            with session.get(url, stream=True, timeout=120) as r:
                r.raise_for_status()
                with dest.open("wb") as f:
                    for chunk in r.iter_content(chunk_size=MIB):
                        f.write(chunk)
            return
        except requests.RequestException as exc:
            wait = 2 ** attempt
            print(f"  download failed ({exc}); retrying in {wait}s", file=sys.stderr)
            time.sleep(wait)
    raise RuntimeError(f"Giving up on {url}")


class PartWriter:
    """Writes CSV rows into numbered part files, each kept below part_bytes (header repeated)."""

    def __init__(self, out_dir, stem, header, part_bytes):
        self.out_dir, self.stem, self.header, self.part_bytes = out_dir, stem, header, part_bytes
        self.parts = []          # [{"file", "bytes", "rows"}]
        self._f = None
        self._buf = io.StringIO()
        self._w = csv.writer(self._buf, lineterminator="\n")
        self._open_new()

    def _encode(self, row):
        self._buf.seek(0)
        self._buf.truncate()
        self._w.writerow(row)
        return self._buf.getvalue()

    def _open_new(self):
        if self._f:
            self._f.close()
        name = f"{self.stem}_part{len(self.parts) + 1:03d}.csv"
        path = self.out_dir / name
        self._f = path.open("w", encoding="utf-8", newline="")
        head = self._encode(self.header)
        self._f.write(head)
        self.parts.append({"file": name, "bytes": len(head.encode("utf-8")), "rows": 0})

    def write(self, row):
        line = self._encode(row)
        size = len(line.encode("utf-8"))
        cur = self.parts[-1]
        if cur["rows"] > 0 and cur["bytes"] + size > self.part_bytes:
            self._open_new()
            cur = self.parts[-1]
        self._f.write(line)
        cur["bytes"] += size
        cur["rows"] += 1

    def close(self):
        if self._f:
            self._f.close()
            self._f = None
        return self.parts


def norm(name):
    """'Survey Date' / 'survey_date' / 'Survey-Date' -> 'survey_date'"""
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def find_col(header, wanted):
    """Bulk CSVs use readable headers ('Survey Date'); the API uses snake_case ('survey_date')."""
    for i, h in enumerate(header):
        if norm(h) == norm(wanted):
            return i
    raise SystemExit(f"Column '{wanted}' not found. Headers in this file are:\n  " + "\n  ".join(header))


def to_iso(value):
    """Return YYYY-MM-DD for ISO or M/D/YYYY dates, else None."""
    v = value.strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}", v):
        return v[:10]
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})", v)
    if m:
        return f"{m.group(3)}-{int(m.group(1)):02d}-{int(m.group(2)):02d}"
    return None


def split_csv(raw_path, out_dir, stem, part_bytes, date_col=None, before=None):
    """Split a bulk CSV into parts. If date_col/before are given, keep rows with date < before
    (rows with an empty or unparseable date are kept) so that later records form the incremental sample."""
    out_dir.mkdir(parents=True, exist_ok=True)
    with raw_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        idx = find_col(header, date_col) if date_col else None
        writer = PartWriter(out_dir, stem, header, part_bytes)
        kept = dropped = unparsed = 0
        for row in reader:
            if not row:
                continue
            if idx is not None and before:
                iso = to_iso(row[idx]) if idx < len(row) else None
                if iso is None:
                    unparsed += 1
                elif iso >= before:
                    dropped += 1
                    continue
            writer.write(row)
            kept += 1
    if unparsed:
        print(f"  note: {unparsed:,} rows had an empty/unrecognised date and were kept in the full load")
    return writer.close(), kept, dropped


def write_json(path, rows, meta):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump({"_meta": meta, "results": rows}, f, ensure_ascii=False, indent=1)


# ----------------------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/samples")
    ap.add_argument("--raw-dir", default="data/raw", help="bulk downloads (do not commit)")
    ap.add_argument("--cutoff", default="2026-07-01", help="deficiency survey_date split: full < cutoff <= incremental")
    ap.add_argument("--penalty-cutoff", default="2026-06-01", help="penalty_date split")
    ap.add_argument("--min-mb", type=float, default=210.0, help="required total size of full-load sample (MiB)")
    ap.add_argument("--part-mb", type=float, default=45.0, help="max size of one part file (MiB); must be < 100")
    ap.add_argument("--incr-rows", type=int, default=5000, help="cap for each incremental JSON sample")
    ap.add_argument("--reuse", action="store_true", help="reuse bulk CSVs already in --raw-dir")
    args = ap.parse_args()

    if args.part_mb * MIB >= GITHUB_HARD_LIMIT:
        sys.exit("--part-mb must be below 100 (GitHub rejects files of 100 MiB or more).")

    out, raw = Path(args.out), Path(args.raw_dir)
    full_dir, inc_dir = out / "full_load", out / "incremental"
    part_bytes, min_bytes = int(args.part_mb * MIB), int(args.min_mb * MIB)
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    cutoffs = {"survey_date": args.cutoff, "penalty_date": args.penalty_cutoff}

    # remove stale files from earlier runs / earlier versions of this script
    for pattern in ("*_part*.csv", "*_full_sample.csv"):
        for old in full_dir.glob(pattern) if full_dir.exists() else []:
            old.unlink()

    manifest = {"fetched_at_utc": now, "min_required_mib": args.min_mb, "full_load": {}, "incremental": {}}
    total_bytes = 0

    def process(name, ds, date_col, stem):
        nonlocal total_bytes
        item = catalog_item(ds)
        url = item["distribution"][0]["downloadURL"]
        raw_path = raw / f"{ds}.csv"
        if args.reuse and raw_path.exists():
            print(f"[{name}] reusing {raw_path}")
        else:
            print(f"[{name}] downloading {item.get('title')} (modified {item.get('modified')})")
            download(url, raw_path)
        before = cutoffs.get(date_col) if date_col else None
        parts, kept, dropped = split_csv(raw_path, full_dir, stem, part_bytes, date_col, before)
        size = sum(p["bytes"] for p in parts)
        total_bytes += size
        print(f"  -> {len(parts)} part file(s), {kept:,} rows, {size / MIB:.1f} MiB"
              + (f" ({dropped:,} rows on/after {before} left for the incremental load)" if dropped else ""))
        manifest["full_load"][name] = {
            "dataset_id": ds, "title": item.get("title"), "catalog_modified": item.get("modified"),
            "next_update": item.get("nextUpdateDate"), "bulk_csv_url": url, "split_column": date_col,
            "full_cutoff": before, "rows": kept, "rows_held_for_incremental": dropped,
            "bytes": size, "parts": parts}

    for spec in REQUIRED:
        process(*spec)

    fallback = list(FALLBACK)
    while total_bytes <= min_bytes and fallback:
        print(f"Total {total_bytes / MIB:.1f} MiB is below {args.min_mb:g} MiB; adding another dataset.")
        process(*fallback.pop(0))

    if total_bytes <= min_bytes:
        manifest["full_load_total_bytes"] = total_bytes
        (out / "samples_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        sys.exit(f"ERROR: full-load sample is only {total_bytes / MIB:.1f} MiB, below the required "
                 f"{args.min_mb:g} MiB, and no fallback datasets are left. Do NOT submit this. "
                 f"Add another CMS dataset to FALLBACK in this script and re-run with --reuse.")

    # incremental samples (small JSON, straight from the datastore API)
    for name, col in INCREMENTAL.items():
        ds = manifest["full_load"][name]["dataset_id"]
        cutoff = cutoffs[col]
        print(f"[{name}] incremental sample: {col} >= {cutoff}")
        rows, available = query(ds, [(col, ">=", cutoff)], max_rows=args.incr_rows)
        path = inc_dir / f"nh_{name}_incr_{cutoff[:7]}.json"
        write_json(path, rows, {
            "dataset_id": ds, "filter": f"{col} >= {cutoff}", "rows_available": available,
            "rows_in_file": len(rows), "fetched_at_utc": now, "load_type": "incremental"})
        manifest["incremental"][name] = {"file": path.name, "filter": f"{col} >= {cutoff}",
                                         "rows_available": available, "rows_in_file": len(rows)}

    manifest["full_load_total_bytes"] = total_bytes
    manifest["full_load_total_mib"] = round(total_bytes / MIB, 1)
    (out / "samples_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    # final independent verification, measured from the files actually on disk
    on_disk = sum(p.stat().st_size for p in full_dir.glob("*.csv"))
    biggest = max(p.stat().st_size for p in full_dir.glob("*.csv"))
    assert on_disk > min_bytes, "verification failed: files on disk are below the required size"
    assert biggest < GITHUB_HARD_LIMIT, "verification failed: a part file is over GitHub's 100 MiB limit"

    print("\nFiles written:")
    for p in sorted(out.rglob("*")):
        if p.is_file():
            print(f"  {p}  {p.stat().st_size / MIB:.2f} MiB")
    print(f"\nFull-load sample total on disk: {on_disk / MIB:.1f} MiB "
          f"({on_disk / 1_000_000:.1f} MB)  -> requirement of >200 MB: MET")
    print("Largest single file: %.1f MiB (GitHub limit 100 MiB)." % (biggest / MIB))
    print("Remember: commit the files in data/samples/ only. Do NOT commit data/raw/.")


if __name__ == "__main__":
    main()
