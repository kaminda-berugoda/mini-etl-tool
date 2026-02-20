from __future__ import annotations

import argparse
import json
import logging
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Any

from config import Config
from reader import read_json_array, read_csv_rows
from schema_tracker import SchemaTracker
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
    p = argparse.ArgumentParser(description="Mini ETL: validate + transform JSON/CSV into JSONL outputs.")
    p.add_argument("--input", default="data/raw", help="Input directory")
    p.add_argument("--out", default="data/out", help="Output directory for cleaned data")
    p.add_argument("--bad", default="data/bad", help="Output directory for bad records")
    p.add_argument("--out-file", default="cleaned.jsonl", help="Cleaned output filename")
    p.add_argument("--bad-file", default="bad_records.jsonl", help="Bad records output filename")
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    p.add_argument("--report", default=None, help="Optional JSON report path")
    p.add_argument("--format", default="auto", choices=["auto", "json", "csv"], help="Input format")
    p.add_argument("--schema-baseline", default=None, help="Optional baseline schema JSON path to diff against")
    p.add_argument("--schema-out", default=None, help="Optional path to write current observed schema JSON")
    return p.parse_args()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def pick_reader(fmt: str, fp: Path) -> Callable[[Path], list[dict[str, Any]]]:
    if fmt == "json":
        return read_json_array
    if fmt == "csv":
        return read_csv_rows
    # auto
    if fp.suffix.lower() == ".csv":
        return read_csv_rows
    return read_json_array


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
    processed_files: list[str] = []
    failed_files: list[str] = []

    schema = SchemaTracker()

    start_ts = utc_now_iso()
    t0 = time.perf_counter()

    # Pick files based on format
    if args.format == "csv":
        files = sorted(cfg.raw_dir.glob("*.csv"))
    elif args.format == "json":
        files = sorted(cfg.raw_dir.glob("*.json"))
    else:
        # auto: both
        files = sorted(list(cfg.raw_dir.glob("*.json")) + list(cfg.raw_dir.glob("*.csv")))

    stats.files_found = len(files)
    log.info(f"Found {stats.files_found} raw files under {cfg.raw_dir} (format={args.format})")

    out_path = cfg.out_dir / cfg.out_file
    bad_path = cfg.bad_dir / cfg.bad_file

    with out_path.open("w", encoding="utf-8") as clean_f, bad_path.open("w", encoding="utf-8") as bad_f:
        for fp in files:
            reader = pick_reader(args.format, fp)
            log.info(f"Processing {fp.name}")

            try:
                records = reader(fp)
                stats.files_processed += 1
                processed_files.append(fp.name)
            except Exception as e:
                stats.files_failed += 1
                failed_files.append(fp.name)
                log.error(f"Failed reading {fp.name}: {e}")
                continue

            for rec in records:
                stats.records_seen += 1

                # observe schema on RAW record (pre-transform)
                if isinstance(rec, dict):
                    schema.observe(rec, source_file=fp.name)

                result = validate_record(rec)

                if result.is_valid:
                    transformed = transform_record(rec)
                    clean_f.write(json.dumps(transformed, ensure_ascii=False) + "\n")
                    stats.records_clean += 1
                else:
                    bad_record = {"source_file": fp.name, "errors": result.errors, "record": rec}
                    bad_f.write(json.dumps(bad_record, ensure_ascii=False) + "\n")
                    stats.records_bad += 1

    duration_ms = round((time.perf_counter() - t0) * 1000, 2)
    end_ts = utc_now_iso()

    # schema snapshot + optional baseline diff
    current_schema = schema.snapshot()
    schema_diff = None
    if args.schema_baseline:
        baseline_path = Path(args.schema_baseline)
        baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
        schema_diff = SchemaTracker.diff(baseline=baseline, current=current_schema)

    if args.schema_out:
        Path(args.schema_out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.schema_out).write_text(json.dumps(current_schema, indent=2), encoding="utf-8")
        log.info(f"Wrote current schema snapshot to {args.schema_out}")

    log.info(
        "Run Summary | "
        f"files_found={stats.files_found} files_processed={stats.files_processed} files_failed={stats.files_failed} | "
        f"records_seen={stats.records_seen} records_clean={stats.records_clean} records_bad={stats.records_bad} | "
        f"duration_ms={duration_ms}"
    )

    if args.report:
        report_path = Path(args.report)
        report_path.parent.mkdir(parents=True, exist_ok=True)

        report = {
            "start_time_utc": start_ts,
            "end_time_utc": end_ts,
            "duration_ms": duration_ms,
            "config": {
                "raw_dir": str(cfg.raw_dir),
                "out_dir": str(cfg.out_dir),
                "bad_dir": str(cfg.bad_dir),
                "out_file": cfg.out_file,
                "bad_file": cfg.bad_file,
                "format": args.format,
            },
            "stats": asdict(stats),
            "processed_files": processed_files,
            "failed_files": failed_files,
            "schema": {
                "current": current_schema,
                "baseline_path": args.schema_baseline,
                "diff": schema_diff,
            },
        }

        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        log.info(f"Wrote run report to {report_path}")


if __name__ == "__main__":
    main()
