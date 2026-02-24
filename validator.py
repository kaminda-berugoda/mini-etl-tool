from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from schema_registry import SchemaDef, get_by_path


@dataclass
class ValidationResult:
    is_valid: bool
    errors: list[str]


def validate_required_fields_raw(rec: dict[str, Any], schema: SchemaDef) -> ValidationResult:
    """
    Validate required fields on RAW record, based on schema.required_fields.
    Supports dot paths like 'address.city'.

    Use this if you want to fail fast before transform.
    In many pipelines, it's cleaner to validate after transform (canonical validation),
    but this is still useful for quick schema-level checks.
    """
    errors: list[str] = []

    for field_path in schema.required_fields:
        val = get_by_path(rec, field_path) if "." in field_path else rec.get(field_path)

        if val is None:
            errors.append(f"missing required field: {field_path}")
            continue

        if isinstance(val, str) and val.strip() == "":
            errors.append(f"missing required field: {field_path}")

    return ValidationResult(is_valid=(len(errors) == 0), errors=errors)


def validate_canonical(canon: dict[str, Any]) -> ValidationResult:
    """
    Validate a CANONICAL record. This is recommended for multi-schema ingestion because
    all sources are mapped into the same output structure before validation.
    """
    errors: list[str] = []

    # user_id
    user_id = canon.get("user_id")
    if user_id is None or (isinstance(user_id, str) and user_id.strip() == ""):
        errors.append("missing user_id")

    # email: basic check
    email = canon.get("email")
    if email is None or "@" not in str(email):
        errors.append("invalid email")

    # signup_date: canonical format should be YYYY-MM-DD (after transformation)
    signup_date = canon.get("signup_date")
    try:
        if signup_date is None:
            raise ValueError("missing signup_date")
        # fromisoformat accepts YYYY-MM-DD
        datetime.fromisoformat(str(signup_date))
    except Exception:
        errors.append("invalid signup_date")

    # total_order_value: if present, must be numeric
    tov = canon.get("total_order_value")
    if tov is not None:
        try:
            float(tov)
        except Exception:
            errors.append("total_order_value not numeric")

    # order_count: if present, must be int-like and >= 0
    oc = canon.get("order_count")
    if oc is not None:
        try:
            oc_int = int(oc)
            if oc_int < 0:
                errors.append("order_count must be >= 0")
        except Exception:
            errors.append("order_count not an integer")

    return ValidationResult(is_valid=(len(errors) == 0), errors=errors)


def validate_record(rec: dict[str, Any]) -> ValidationResult:
    """
    Backwards-compatible single-schema validation for your earlier version.
    Kept here so older parts of the project still work.

    For multi-schema mode, prefer validate_canonical() after transform_with_schema().
    """
    errors: list[str] = []

    # required: user_id
    user_id = rec.get("user_id")
    if not user_id:
        errors.append("missing user_id")

    # email rule
    email = rec.get("email")
    if not email or "@" not in str(email):
        errors.append("invalid email")

    # signup_date ISO rule (raw)
    signup = rec.get("signup_date")
    try:
        if signup is None:
            raise ValueError("missing signup_date")
        s = str(signup).replace("Z", "+00:00")
        datetime.fromisoformat(s)
    except Exception:
        errors.append("invalid signup_date")

    # orders amounts (raw)
    orders = rec.get("orders", [])
    if orders is not None and not isinstance(orders, list):
        errors.append("orders must be a list")
    else:
        for idx, o in enumerate(orders or []):
            amt = None if not isinstance(o, dict) else o.get("amount")
            try:
                val = float(amt)
                if val <= 0:
                    errors.append(f"orders[{idx}].amount must be > 0")
            except Exception:
                errors.append(f"orders[{idx}].amount not numeric")

    return ValidationResult(is_valid=(len(errors) == 0), errors=errors)