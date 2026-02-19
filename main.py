from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from config import Config
from reader import read_json_array
from transformer import transform_record
from utils import get_logger
from validator import validate_record
from writer import write_jsonl


@dataclass
class RunStats:
    files_found: int = 0
    files_processed: int = 0
    files_failed: int = 0

    records_seen: int = 0
    records_clean: int = 0
    records_bad: int = 0


def iter_raw_files(raw_dir: Path) -> list[Path]:
    """Return raw input files in deterministic order."""
    return sorted(raw_dir.glob("*.json"))


def main() -> None:
    cfg = Config()
    log = get_logger()

    # Ensure directories exist
    cfg.out_dir.mkdir(parents=True, exist_ok=True)
    cfg.bad_dir.mkdir(parents=True, exist_ok=True)

    stats = RunStats()

    files = iter_raw_files(cfg.raw_dir)
    stats.files_found = len(files)
    log.info(f"Found {stats.files_found} raw files under {cfg.raw_dir}")

    # --- Streaming approach: create generators and let writer stream them ---
    def clean_records() -> Any:
        """Yield transformed valid records as they are processed."""
        for fp in files:
            nonlocal stats
            log.info(f"Processing {fp.name}")

            try:
                records = read_json_array(fp)
                stats.files_processed += 1
            except Exception as e:
                stats.files_failed += 1
                log.error(f"Failed reading {fp.name}: {e}")
                continue

            for rec in records:
                stats.records_seen += 1
                result = validate_record(rec)

                if result.is_valid:
                    stats.records_clean += 1
                    yield transform_record(rec)

    def bad_records() -> Any:
        """Yield invalid records (with reasons) as they are processed."""
        for fp in files:
            nonlocal stats
            # NOTE: We read again here, which is not ideal.
            # We'll fix this by doing a single pass in the next iteration.
            try:
                records = read_json_array(fp)
            except Exception:
                continue

            for rec in records:
                result = validate_record(rec)
                if not result.is_valid:
                    stats.records_bad += 1
                    yield {
                        "source_file": fp.name,
                        "errors": result.errors,
                        "record": rec,
                    }

    # Write outputs
    out_path = cfg.out_dir / cfg.out_file
    bad_path = cfg.bad_dir / cfg.bad_file

    write_jsonl(out_path, clean_records())
    write_jsonl(bad_path, bad_records())

    # Summary logs
    log.info(
        "Run Summary | "
        f"files_found={stats.files_found} "
        f"files_processed={stats.files_processed} "
        f"files_failed={stats.files_failed} | "
        f"records_seen={stats.records_seen} "
        f"records_clean={stats.records_clean} "
        f"records_bad={stats.records_bad}"
    )


if __name__ == "__main__":
    main()
