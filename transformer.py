from __future__ import annotations
from datetime import datetime
from typing import Any


CANONICAL_KEYS = {
    "user_id", "name", "email", "signup_date", "address", "orders"
}


def transform_record(rec: dict[str, Any]) -> dict[str, Any]:
    addr = rec.get("address") or {}
    orders = rec.get("orders") or []

    total = 0.0
    count = 0

    if isinstance(orders, list):
        for o in orders:
            if isinstance(o, dict) and o.get("amount") is not None:
                try:
                    total += float(o["amount"])
                    count += 1
                except Exception:
                    pass

    # signup_date normalization (validator should ensure valid)
    signup_raw = rec.get("signup_date")
    signup_date = None
    if signup_raw is not None:
        s = str(signup_raw).replace("Z", "+00:00")
        signup_dt = datetime.fromisoformat(s)
        signup_date = signup_dt.date().isoformat()

    extras = {k: v for k, v in rec.items() if k not in CANONICAL_KEYS}

    return {
        "user_id": rec.get("user_id"),
        "name": rec.get("name"),
        "email": rec.get("email"),
        "signup_date": signup_date,
        "city": (addr.get("city") if isinstance(addr, dict) else None),
        "country": (addr.get("country") if isinstance(addr, dict) else None),
        "total_order_value": round(total, 2),
        "order_count": count,
        "extras": extras,  # 👈 drift-safe
    }
