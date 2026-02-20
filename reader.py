import csv
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


def read_csv_rows(path: Path) -> list[dict[str, Any]]:
    """
    Reads a CSV file and returns list[dict].
    All values start as strings (or None). We keep it simple; transform/validate can coerce.
    """
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError(f"{path} has no header row")
        rows = []
        for row in reader:
            # DictReader can return None keys in weird CSVs; guard it
            if None in row:
                raise ValueError(f"{path} has malformed rows (extra columns)")
            rows.append(row)
        return rows
