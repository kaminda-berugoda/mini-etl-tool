from __future__ import annotations

import argparse
import json
import logging
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from config import Config
from reader import read_json_array, read_csv_rows
from schema_registry import SchemaRegistry, SchemaDef
from transformer_schema import transform_with_schema
from utils import get_logger
from validator import ValidationResult  # keep your existing dataclass


# ----------------------------
# Metrics
# ----------------------------
@dataclass
class RunStats:
    files_found: int = 0
    files_processed: int = 0
    files_failed: int = 0
    records_seen: int = 0
    records_clean: int = 0
    records_bad: int = 0


# ----------------------------
# CLI
# ----------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Mini ETL: multi-schema ingestion (JSON/CSV) -> JSONL outputs.")

    p.add_argument("--input", default="data/raw", help="Input directory")
    p.add_argument("--out", default="data/out", help="Output directory for cleaned data")
    p.add_argument("--bad", default="data/bad", help="Output directory for bad records")
    p.add_argument("--out-file", default="cleaned.jsonl", help="Cleaned output filename")
    p.add_argument("--bad-file", default="bad_records.jsonl", help="Bad records output filename")

    p.add_argument("--format", default="auto", choices=["auto", "json", "csv"], help="Input format")
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])

    p.add_argument("--report", default=None, help="Optional JSON report path")

    # ---- multi-schema settings
    p.add_argument("--schemas-dir", default="schemas", help="Directory containing schema JSON files")
    p.add_argument("--schema-mode", default="filename", choices=["filename", "fixed"],
                   help="How to choose schema: filename prefix or fixed schema for all files")
    p.add_argument("--schema-name", default=None,
                   help="Schema name when using --schema-mode fixed (e.g. crm)")

    return p.parse_args()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ----------------------------
# Helpers
# ----------------------------
def pick_reader(fmt: str, fp: Path) -> Callable[[Path], list[dict[str, Any]]]:
    if fmt == "json":
        return read_json_array
    if fmt == "csv":
        return read_csv_rows
    # auto
    if fp.suffix.lower() == ".csv":
        return read_csv_rows
    return read_json_array


def list_input_files(raw_dir: Path, fmt: str) -> list[Path]:
    if fmt == "json":
        return sorted(raw_dir.glob("*.json"))
    if fmt == "csv":
        return sorted(raw_dir.glob("*.csv"))
    return sorted(list(raw_dir.glob("*.json")) + list(raw_dir.glob("*.csv")))


def schema_for_file(fp: Path, mode: str, fixed_name: str | None, registry: SchemaRegistry) -> SchemaDef:
    if mode == "fixed":
        if not fixed_name:
            raise ValueError("schema-mode fixed requires --schema-name")
        return registry.get(fixed_name)

    # filename mode: prefix before first underscore, else full stem
    # e.g. crm_2026-02-23.json -> crm
    prefix = fp.name.split("_", 1)[0]
    return registry.get(prefix)


def validate_canonical_record(canon: dict[str, Any]) -> ValidationResult:
    """
    Canonical-level validation (simple & consistent across schemas).
    You can move this into validator.py if you prefer.
    """
    errors: list[str] = []

    user_id = canon.get("user_id")
    if user_id is None or (isinstance(user_id, str) and user_id.strip() == ""):
        errors.append("missing user_id")

    email = canon.get("email")
    if email is None or "@" not in str(email):
        errors.append("invalid email")

    signup_date = canon.get("signup_date")
    try:
        if signup_date is None:
            raise ValueError("missing signup_date")
        # canonical signup_date should be YYYY-MM-DD after transform
        datetime.fromisoformat(str(signup_date))
    except Exception:
        errors.append("invalid signup_date")

    # totals are optional; if present ensure numeric
    tov = canon.get("total_order_value")
    if tov is not None:
        try:
            float(tov)
        except Exception:
            errors.append("total_order_value not numeric")

    return ValidationResult(is_valid=(len(errors) == 0), errors=errors)


# ----------------------------
# Main
# ----------------------------
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

    # Load schema registry
    registry = SchemaRegistry(Path(args.schemas_dir))
    registry.load_all()

    stats = RunStats()
    processed_files: list[str] = []
    failed_files: list[str] = []

    start_ts = utc_now_iso()
    t0 = time.perf_counter()

    files = list_input_files(cfg.raw_dir, args.format)
    stats.files_found = len(files)
    log.info(f"Found {stats.files_found} raw files under {cfg.raw_dir} (format={args.format})")

    out_path = cfg.out_dir / cfg.out_file
    bad_path = cfg.bad_dir / cfg.bad_file

    with out_path.open("w", encoding="utf-8") as clean_f, bad_path.open("w", encoding="utf-8") as bad_f:
        for fp in files:
            reader = pick_reader(args.format, fp)

            try:
                schema = schema_for_file(fp, args.schema_mode, args.schema_name, registry)
            except Exception as e:
                stats.files_failed += 1
                failed_files.append(fp.name)
                log.error(f"Schema resolution failed for {fp.name}: {e}")
                continue

            log.info(f"Processing {fp.name} (schema={schema.schema_name} v{schema.version})")

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

                # Defensive: readers should return dict rows
                if not isinstance(rec, dict):
                    stats.records_bad += 1
                    bad_f.write(json.dumps({
                        "source_file": fp.name,
                        "schema": schema.schema_name,
                        "errors": ["record is not an object/dict"],
                        "record": rec,
                    }, ensure_ascii=False) + "\n")
                    continue

                # Transform to canonical using schema mapping
                canon = transform_with_schema(rec, schema)

                # Validate canonical consistently across all schemas
                v = validate_canonical_record(canon)

                if v.is_valid:
                    stats.records_clean += 1
                    clean_f.write(json.dumps(canon, ensure_ascii=False) + "\n")
                else:
                    stats.records_bad += 1
                    bad_f.write(json.dumps({
                        "source_file": fp.name,
                        "schema": schema.schema_name,
                        "errors": v.errors,
                        "record": rec,
                        "canonical_preview": canon,  # super useful for debugging mappings
                    }, ensure_ascii=False) + "\n")

    duration_ms = round((time.perf_counter() - t0) * 1000, 2)
    end_ts = utc_now_iso()

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
                "schemas_dir": args.schemas_dir,
                "schema_mode": args.schema_mode,
                "schema_name": args.schema_name,
            },
            "stats": asdict(stats),
            "processed_files": processed_files,
            "failed_files": failed_files,
        }

        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        log.info(f"Wrote run report to {report_path}")


if __name__ == "__main__":
    main()