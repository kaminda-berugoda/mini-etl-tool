from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from pathlib import Path

from config import Config
from reader import read_json_array
from transformer import transform_record
from utils import get_logger
from validator import validate_record


@dataclass
class RunStats:
    files_found: int = 0
    files_processed: int = 0
    files_failed: int = 0
    records_seen: int = 0
    records_clean: int = 0
    records_bad: int = 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Mini ETL: validate + transform JSON files into JSONL outputs.")
    p.add_argument("--input", default="data/raw", help="Input directory containing *.json files")
    p.add_argument("--out", default="data/out", help="Output directory for cleaned data")
    p.add_argument("--bad", default="data/bad", help="Output directory for bad records")
    p.add_argument("--out-file", default="cleaned.jsonl", help="Cleaned output filename")
    p.add_argument("--bad-file", default="bad_records.jsonl", help="Bad records output filename")
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    return p.parse_args()


def main() -> None:
    args = parse_args()

    log_level = getattr(logging, args.log_level)
    log = get_logger(level=log_level)

    cfg = Config(
        raw_dir=Path(args.input),
        out_dir=Path(args.out),
        bad_dir=Path(args.bad),
        out_file=args.out_file,
        bad_file=args.bad_file,
    )

    cfg.out_dir.mkdir(parents=True, exist_ok=True)
    cfg.bad_dir.mkdir(parents=True, exist_ok=True)

    stats = RunStats()

    files = sorted(cfg.raw_dir.glob("*.json"))
    stats.files_found = len(files)
    log.info(f"Found {stats.files_found} raw files under {cfg.raw_dir}")

    out_path = cfg.out_dir / cfg.out_file
    bad_path = cfg.bad_dir / cfg.bad_file

    with out_path.open("w", encoding="utf-8") as clean_f, bad_path.open("w", encoding="utf-8") as bad_f:
        for fp in files:
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
                    transformed = transform_record(rec)
                    clean_f.write(json.dumps(transformed, ensure_ascii=False) + "\n")
                    stats.records_clean += 1
                else:
                    bad_record = {"source_file": fp.name, "errors": result.errors, "record": rec}
                    bad_f.write(json.dumps(bad_record, ensure_ascii=False) + "\n")
                    stats.records_bad += 1

    log.info(
        "Run Summary | "
        f"files_found={stats.files_found} files_processed={stats.files_processed} files_failed={stats.files_failed} | "
        f"records_seen={stats.records_seen} records_clean={stats.records_clean} records_bad={stats.records_bad}"
    )


if __name__ == "__main__":
    main()
