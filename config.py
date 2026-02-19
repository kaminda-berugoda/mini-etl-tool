from dataclasses import dataclass
from pathlib import Path 

@dataclass (frozen=True)

class Config:
    raw_dir: Path = Path("data/raw")
    out_dir: Path = Path("data/out")
    bad_dir: Path = Path("data/bad")

    out_file: str = "cleaned.jsonl"
    bad_file: str = "bad_records.jsonl"

    #required_fields: tuple[str, ...] = ("user_id", "email", "signup_date")

