from __future__ import annotations

from datetime import datetime
from typing import Any
from schema_registry import SchemaDef, get_by_path


def normalize_iso_date(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).replace("Z", "+00:00")
    dt = datetime.fromisoformat(s)
    return dt.date().isoformat()


def transform_with_schema(rec: dict[str, Any], schema: SchemaDef) -> dict[str, Any]:
    # 1) Map input -> canonical using field_map
    out: dict[str, Any] = {}
    extras: dict[str, Any] = dict(rec)  # start with everything; remove mapped later

    for src_path, dest_key in schema.field_map.items():
        val = get_by_path(rec, src_path) if "." in src_path else rec.get(src_path)
        out[dest_key] = val

        # best-effort remove simple top-level keys from extras
        if "." not in src_path and src_path in extras:
            extras.pop(src_path, None)

    # 2) Normalize signup_date if present in canonical field
    if "signup_date" in out:
        try:
            out["signup_date"] = normalize_iso_date(out["signup_date"])
        except Exception:
            # keep raw; validator/quarantine can catch it
            pass

    # 3) Orders aggregation (schema-specific paths)
    total = 0.0
    count = 0

    if schema.orders_path:
        orders = get_by_path(rec, schema.orders_path) if "." in schema.orders_path else rec.get(schema.orders_path)
        if isinstance(orders, list):
            for o in orders:
                if not isinstance(o, dict):
                    continue
                amt = o.get(schema.order_amount_path or "amount")
                if amt is None:
                    continue
                try:
                    total += float(amt)
                    count += 1
                except Exception:
                    continue

    out["total_order_value"] = round(total, 2)
    out["order_count"] = count

    # 4) Keep drift-safe extras
    out["extras"] = extras

    return out