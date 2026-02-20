# Mini ETL Framework

A lightweight, modular Python-based ETL pipeline supporting:

- JSON (array) input
- CSV input
- Validation & transformation
- Streaming JSONL output
- Bad record isolation (quarantine)
- Schema drift tracking
- Run reporting
- CLI configuration

This project demonstrates clean data engineering practices including modular design, schema observation, defensive validation, and reproducible execution.

---

## 🏗 Architecture Overview

Pipeline flow:

Raw Files → Reader → Validator → Transformer → Writer → JSONL Output

Core components:

- `main.py` – Orchestration + CLI
- `config.py` – Configuration model
- `reader.py` – JSON + CSV readers
- `validator.py` – Record validation logic
- `transformer.py` – Canonical transformation logic
- `writer.py` – JSONL writer
- `schema_tracker.py` – Schema observation + drift detection
- `utils.py` – Logging utilities

---

## 📦 Supported Input Formats

### JSON (Array Format)

```json
[
  {
    "user_id": "123",
    "name": "John",
    "email": "john@example.com",
    "signup_date": "2026-01-10T12:33:00Z",
    "address": {
      "city": "Sydney",
      "country": "AU"
    },
    "orders": [
      {"order_id": "A1", "amount": "120.50"}
    ]
  }
]
