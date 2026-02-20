# Mini ETL Framework

A lightweight, modular Python-based ETL pipeline supporting:

-   JSON input (array format)
-   CSV input
-   Validation and transformation
-   Streaming JSONL output
-   Bad record isolation (quarantine)
-   Schema drift tracking
-   Run reporting
-   CLI configuration

This project demonstrates clean Data Engineering principles including
modular design, defensive validation, schema observability, and
reproducible execution.

------------------------------------------------------------------------

# Project Structure

mini_etl/ │ ├── README.md ├── main.py ├── config.py ├── reader.py ├──
validator.py ├── transformer.py ├── writer.py ├── schema_tracker.py ├──
utils.py └── data/ ├── raw/ ├── out/ └── bad/

------------------------------------------------------------------------

# Architecture Overview

Pipeline Flow:

Raw Files\
→ Reader\
→ Validator\
→ Transformer\
→ Writer\
→ JSONL Output

Core Components:

-   main.py -- Orchestration + CLI
-   config.py -- Configuration model
-   reader.py -- JSON + CSV readers
-   validator.py -- Record validation logic
-   transformer.py -- Canonical transformation logic
-   writer.py -- JSONL writer
-   schema_tracker.py -- Schema observation + drift detection
-   utils.py -- Logging utilities

------------------------------------------------------------------------

# Supported Input Formats

## JSON (Array Format)

\[ { "user_id": "123", "name": "John", "email": "john@example.com",
"signup_date": "2026-01-10T12:33:00Z", "address": { "city": "Sydney",
"country": "AU" }, "orders": \[ {"order_id": "A1", "amount": "120.50"}
\] }\]

## CSV

user_id,name,email,signup_date,city,country
123,John,john@example.com,2026-01-10T12:33:00Z,Sydney,AU

CSV values are initially read as strings and validated/transformed
during processing.

------------------------------------------------------------------------

# Validation Rules

A record is valid if:

-   user_id exists
-   email contains "@"
-   signup_date is ISO formatted
-   orders\[\].amount is numeric and \> 0 (if present)

Invalid records:

-   Do NOT stop pipeline execution
-   Are written to bad_records.jsonl
-   Include detailed validation error reasons

------------------------------------------------------------------------

# Transformation Rules

Canonical output schema:

{ "user_id": "123", "name": "John", "email": "john@example.com",
"signup_date": "2026-01-10", "city": "Sydney", "country": "AU",
"total_order_value": 120.5, "order_count": 1, "extras": {} }

Unknown fields are preserved under "extras" to prevent silent data loss.

------------------------------------------------------------------------

# CLI Usage

Basic run:

python main.py

Specify format:

python main.py --format json python main.py --format csv

Generate run report:

python main.py --report data/out/run_report.json

Export observed schema:

python main.py --schema-out data/out/schema_current.json

Detect schema drift:

python main.py --schema-baseline data/out/schema_baseline.json --report
data/out/run_report.json

------------------------------------------------------------------------

# Output Files

Clean output: data/out/cleaned.jsonl

Bad records: data/bad/bad_records.jsonl

Run report: run_report.json

------------------------------------------------------------------------

# Design Principles

-   Single-pass streaming writes
-   No large in-memory buffers
-   Separation of concerns
-   Defensive programming
-   Schema observability
-   CLI configurability
-   Extensible architecture

------------------------------------------------------------------------

# Future Enhancements

-   JSONL streaming input
-   Decimal-based money handling
-   Config-driven validation rules
-   Parallel file processing
-   Unit testing (pytest)
-   Docker packaging

------------------------------------------------------------------------

# Requirements

-   Python 3.8+
-   No external runtime dependencies

------------------------------------------------------------------------

# Author

Mini ETL project built as part of progressive Data Engineering skill
sharpening.
