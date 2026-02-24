from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def get_by_path(obj: dict[str, Any], path: str) -> Any:
    """
    Supports dot paths like: "address.city"
    Returns None if any part missing.
    """
    cur: Any = obj
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
        if cur is None:
            return None
    return cur


@dataclass(frozen=True)
class SchemaDef:
    schema_name: str
    version: int
    required_fields: tuple[str, ...]
    field_map: dict[str, str]
    orders_path: str | None = None
    order_amount_path: str | None = None


class SchemaRegistry:
    def __init__(self, schemas_dir: Path):
        self.schemas_dir = schemas_dir
        self._schemas: dict[str, SchemaDef] = {}

    def load_all(self) -> None:
        for fp in sorted(self.schemas_dir.glob("*.json")):
            data = json.loads(fp.read_text(encoding="utf-8"))
            sd = SchemaDef(
                schema_name=data["schema_name"],
                version=int(data.get("version", 1)),
                required_fields=tuple(data.get("required_fields", [])),
                field_map=dict(data.get("field_map", {})),
                orders_path=data.get("orders_path"),
                order_amount_path=data.get("order_amount_path"),
            )
            self._schemas[sd.schema_name] = sd

    def get(self, schema_name: str) -> SchemaDef:
        if schema_name not in self._schemas:
            raise KeyError(f"Unknown schema_name={schema_name}. Available: {list(self._schemas.keys())}")
        return self._schemas[schema_name]