from __future__ import annotations

import csv
import json
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any


def write_csv(path: str | Path, rows: list[Any]) -> Path:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)

    normalized_rows = [_normalize_row(row) for row in rows]
    fieldnames = _fieldnames(normalized_rows)

    with destination.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in normalized_rows:
            writer.writerow(row)
    return destination


def write_json(path: str | Path, rows: list[Any]) -> Path:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    normalized_rows = [_normalize_row(row) for row in rows]
    with destination.open("w", encoding="utf-8") as handle:
        json.dump(normalized_rows, handle, indent=2)
    return destination


def _normalize_row(row: Any) -> dict[str, Any]:
    if hasattr(row, "to_row"):
        return row.to_row()
    if is_dataclass(row):
        return asdict(row)
    if isinstance(row, dict):
        return dict(row)
    raise TypeError(f"unsupported row type: {type(row)!r}")


def _fieldnames(rows: list[dict[str, Any]]) -> list[str]:
    seen: list[str] = []
    for row in rows:
        for key in row:
            if key not in seen:
                seen.append(key)
    return seen
