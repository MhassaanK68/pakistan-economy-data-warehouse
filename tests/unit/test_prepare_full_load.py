from argparse import Namespace
from pathlib import Path
import tempfile
import unittest

from scripts.prepare_full_load import SOURCE_FOLDERS, prepare, sha256_file


class PrepareFullLoadTests(unittest.TestCase):
    def test_stage_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw = root / "raw"
            for folder in SOURCE_FOLDERS:
                source = raw / folder
                source.mkdir(parents=True)
                (source / f"{folder}.csv").write_bytes(folder.encode("utf-8"))
            args = Namespace(
                input_root=raw,
                staging_root=root / "staging",
                batch_id="full_test",
                ingest_date="2026-10-08",
                retrieved_at_utc="2026-10-08T00:00:00+00:00",
                dry_run=False,
            )

            first = prepare(args)
            second = prepare(args)

            self.assertEqual({row.ingestion_status for row in first}, {"LANDED"})
            self.assertEqual({row.ingestion_status for row in second}, {"SKIPPED_IDENTICAL"})
            for row in second:
                self.assertEqual(sha256_file(Path(row.source_path)), sha256_file(Path(row.staging_path)))


if __name__ == "__main__":
    unittest.main()
