from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Any

@dataclass
class ValidationResult:
    is_valid: bool
    errors: list[str]

def validate_record(rec: dict[str, Any]) -> ValidationResult:
    errors: list[str] = []

    # required: user_id
    user_id = rec.get("user_id")
    if not user_id:
        errors.append("missing user_id")

    # email rule
    email = rec.get("email")
    if not email or "@" not in str(email):
        errors.append("invalid email")

    # signup_date ISO rule
    signup = rec.get("signup_date")
    try:
        # accepts '2026-01-10T12:33:00Z' by replacing Z
        if signup is None:
            raise ValueError("missing signup_date")
        s = str(signup).replace("Z", "+00:00")
        datetime.fromisoformat(s)
    except Exception:
        errors.append("invalid signup_date")

    # orders amounts
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
