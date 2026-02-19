import json
from pathlib import Path
from typing import Any

def read_json_array(path: Path) -> list[dict[str, Any]]:
    
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError(f"{path} must contain a JSON array at top level")

    out: list[dict[str, Any]] = []
    for i, item in enumerate(data):
        if isinstance(item, dict):
            out.append(item)
        else:
            raise ValueError(f"Item {i} in {path} is not an object")
    return out