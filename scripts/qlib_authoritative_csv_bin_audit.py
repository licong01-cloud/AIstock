from __future__ import annotations

import argparse
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Sequence

import numpy as np


DEFAULT_FIELDS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "factor",
    "up_limit_price",
    "down_limit_price",
    "prev_close",
    "limit_up",
    "limit_down",
]


def _count_csv_rows(path: Path) -> int:
    with path.open("rb") as f:
        line_count = sum(chunk.count(b"\n") for chunk in iter(lambda: f.read(1024 * 1024), b""))
    return max(0, line_count - 1)


def _audit_one(csv_path: Path, qlib_dir: Path, freq: str, fields: Sequence[str]) -> dict[str, Any]:
    code = csv_path.stem
    expected_rows = _count_csv_rows(csv_path)
    feature_dir = qlib_dir / "features" / code.lower()
    errors: list[dict[str, Any]] = []
    field_finite_counts: dict[str, int] = {}
    for field in fields:
        bin_path = feature_dir / f"{field}.{freq}.bin"
        if not bin_path.exists():
            errors.append({"field": field, "reason": "missing_bin_file", "path": str(bin_path)})
            continue
        arr = np.fromfile(bin_path, dtype="<f4")
        if arr.size == 0:
            errors.append({"field": field, "reason": "empty_bin_file", "path": str(bin_path)})
            continue
        finite_count = int(np.isfinite(arr[1:]).sum())
        field_finite_counts[field] = finite_count
        if finite_count != expected_rows:
            errors.append(
                {
                    "field": field,
                    "reason": "finite_count_mismatch",
                    "csv_rows": expected_rows,
                    "bin_finite_rows": finite_count,
                    "path": str(bin_path),
                }
            )
    return {
        "code": code,
        "csv_rows": expected_rows,
        "field_finite_counts": field_finite_counts,
        "ok": not errors,
        "errors": errors,
    }


def audit_csv_vs_bin(
    *,
    csv_dir: Path,
    qlib_dir: Path,
    freq: str,
    fields: Sequence[str],
    workers: int,
    max_errors: int,
) -> dict[str, Any]:
    csv_files = sorted(csv_dir.glob("*.csv"))
    if not csv_files:
        raise RuntimeError(f"no CSV files under {csv_dir}")
    calendar_path = qlib_dir / "calendars" / f"{freq}.txt"
    instruments_path = qlib_dir / "instruments" / "all.txt"
    if not calendar_path.exists():
        raise FileNotFoundError(calendar_path)
    if not instruments_path.exists():
        raise FileNotFoundError(instruments_path)

    errors: list[dict[str, Any]] = []
    checked_stocks = 0
    checked_csv_rows = 0
    checked_field_values = 0
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = [pool.submit(_audit_one, path, qlib_dir, freq, fields) for path in csv_files]
        for future in as_completed(futures):
            result = future.result()
            checked_stocks += 1
            checked_csv_rows += int(result["csv_rows"])
            checked_field_values += int(result["csv_rows"]) * len(fields)
            if not result["ok"]:
                errors.append({"code": result["code"], "errors": result["errors"]})
                if len(errors) >= max_errors:
                    break

    calendar_rows = sum(1 for _ in calendar_path.open("rb"))
    instrument_rows = sum(1 for _ in instruments_path.open("rb"))
    return {
        "ok": not errors,
        "csv_dir": str(csv_dir),
        "qlib_dir": str(qlib_dir),
        "freq": freq,
        "fields": list(fields),
        "csv_files": len(csv_files),
        "checked_stocks": checked_stocks,
        "checked_csv_rows": checked_csv_rows,
        "checked_field_values": checked_field_values,
        "calendar_rows": calendar_rows,
        "instrument_rows": instrument_rows,
        "error_count": len(errors),
        "errors": errors,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit authoritative per-stock CSV row counts against Qlib bin finite counts.")
    parser.add_argument("--csv-dir", required=True)
    parser.add_argument("--qlib-dir", required=True)
    parser.add_argument("--freq", default="1min")
    parser.add_argument("--fields", default=",".join(DEFAULT_FIELDS))
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--max-errors", type=int, default=50)
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    fields = [item.strip() for item in args.fields.split(",") if item.strip()]
    result = audit_csv_vs_bin(
        csv_dir=Path(args.csv_dir),
        qlib_dir=Path(args.qlib_dir),
        freq=args.freq,
        fields=fields,
        workers=args.workers,
        max_errors=args.max_errors,
    )
    text = json.dumps(result, ensure_ascii=False, indent=2)
    print(text)
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(text, encoding="utf-8")
    return 0 if result["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
