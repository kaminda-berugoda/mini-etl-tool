from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


def _type_name(v: Any) -> str:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, int) and not isinstance(v, bool):
        return "int"
    if isinstance(v, float):
        return "float"
    if isinstance(v, str):
        return "str"
    if isinstance(v, dict):
        return "object"
    if isinstance(v, list):
        return "array"
    return type(v).__name__


def _walk(obj: Any, prefix: str = "", max_array_samples: int = 1):
    """
    Yields (path, type_name) pairs for nested JSON-like objects.
    - Dict keys become dot paths: address.city
    - Arrays become path[] and (optionally) sample element types
    """
    t = _type_name(obj)

    # Record the current node as well
    yield (prefix or "$", t)

    if isinstance(obj, dict):
        for k, v in obj.items():
            path = f"{prefix}.{k}" if prefix else str(k)
            yield from _walk(v, path, max_array_samples=max_array_samples)

    elif isinstance(obj, list):
        arr_path = f"{prefix}[]" if prefix else "$[]"
        yield (arr_path, "array")

        # sample a couple of items to infer element structure/types
        for i, item in enumerate(obj[:max_array_samples]):
            item_path = f"{arr_path}"
            yield from _walk(item, item_path, max_array_samples=max_array_samples)


@dataclass
class SchemaTracker:
    """
    Tracks observed types per path.
    Reports drift like:
      - new paths (seen now, not in baseline)
      - missing paths (baseline expected, not seen now)
      - type changes (path seen with new type)
    """
    observed: dict[str, set[str]] = field(default_factory=dict)
    files_seen: set[str] = field(default_factory=set)

    def observe(self, record: dict[str, Any], source_file: str) -> None:
        self.files_seen.add(source_file)
        for path, t in _walk(record, prefix="", max_array_samples=1):
            self.observed.setdefault(path, set()).add(t)

    def snapshot(self) -> dict[str, list[str]]:
        # JSON-friendly
        return {k: sorted(list(v)) for k, v in sorted(self.observed.items())}

    @staticmethod
    def diff(baseline: dict[str, list[str]], current: dict[str, list[str]]) -> dict[str, Any]:
        base_keys = set(baseline.keys())
        curr_keys = set(current.keys())

        new_paths = sorted(list(curr_keys - base_keys))
        missing_paths = sorted(list(base_keys - curr_keys))

        type_changes = []
        for k in sorted(list(base_keys & curr_keys)):
            b = set(baseline.get(k, []))
            c = set(current.get(k, []))
            if b != c:
                type_changes.append({
                    "path": k,
                    "baseline_types": sorted(list(b)),
                    "current_types": sorted(list(c)),
                })

        return {
            "new_paths": new_paths,
            "missing_paths": missing_paths,
            "type_changes": type_changes,
        }
