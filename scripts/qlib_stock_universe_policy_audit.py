from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Sequence

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.db.pg_pool import get_conn  # noqa: E402
from backend.qlib_exporter.config import IPO_FILTER_DAYS  # noqa: E402


def _feature_dir_to_ts_code(path: Path) -> str:
    name = path.name.strip()
    if "." in name:
        code, exchange = name.split(".", 1)
        return f"{code.upper()}.{exchange.upper()}"
    lower = name.lower()
    if len(lower) == 8 and lower[:2] in {"sh", "sz", "bj"}:
        return f"{lower[2:].upper()}.{lower[:2].upper()}"
    if len(lower) == 8 and lower[-2:] in {"sh", "sz", "bj"}:
        return f"{lower[:6].upper()}.{lower[-2:].upper()}"
    return name.upper()


def _split_instrument_line(raw: str) -> tuple[str, str, str]:
    text = raw.strip()
    if "\t" in text:
        parts = [part.strip() for part in text.split("\t") if part.strip()]
    elif "," in text:
        parts = [part.strip() for part in text.split(",") if part.strip()]
    else:
        parts = text.split()
        if len(parts) >= 5:
            parts = [parts[0], f"{parts[1]} {parts[2]}", f"{parts[3]} {parts[4]}"]
    if len(parts) < 3:
        raise ValueError(f"invalid instruments/all.txt line: {raw!r}")
    return parts[0], parts[1], parts[2]


def _instrument_to_ts_code(symbol: str) -> str:
    return _feature_dir_to_ts_code(Path(str(symbol).strip()))


def _date_prefix(value: str) -> date:
    return date.fromisoformat(str(value).strip()[:10])


def _load_stock_metadata(codes: Sequence[str], end: date) -> pd.DataFrame:
    if not codes:
        return pd.DataFrame()
    sql = """
        SELECT s.ts_code,
               s.exchange,
               s.list_status,
               s.list_date,
               EXISTS (
                   SELECT 1
                   FROM market.stock_st st
                   WHERE st.ts_code = s.ts_code
                     AND st.ann_date <= %(end)s
               ) AS has_st_record
        FROM market.stock_basic s
        WHERE s.ts_code = ANY(%(codes)s)
    """
    with get_conn() as conn:
        df = pd.read_sql(sql, conn, params={"codes": list(codes), "end": end})
    if df.empty:
        return df
    df["ts_code"] = df["ts_code"].astype(str).str.upper()
    df["list_date"] = pd.to_datetime(df["list_date"], errors="coerce").dt.date
    return df


def audit_stock_universe_policy(*, qlib_dir: Path, end: date, min_listed_days: int) -> dict[str, Any]:
    features_dir = qlib_dir / "features"
    all_txt = qlib_dir / "instruments" / "all.txt"
    if not features_dir.exists():
        raise FileNotFoundError(features_dir)
    if not all_txt.exists():
        raise FileNotFoundError(all_txt)

    feature_codes = sorted(
        {
            _feature_dir_to_ts_code(path)
            for path in features_dir.iterdir()
            if path.is_dir() and any(path.glob("*.bin"))
        }
    )
    instrument_rows: list[dict[str, Any]] = []
    for raw in all_txt.read_text(encoding="utf-8").splitlines():
        if not raw.strip():
            continue
        symbol, start_value, end_value = _split_instrument_line(raw)
        instrument_rows.append(
            {
                "symbol": symbol,
                "ts_code": _instrument_to_ts_code(symbol),
                "start": start_value,
                "end": end_value,
                "start_date": _date_prefix(start_value),
                "end_date": _date_prefix(end_value),
            }
        )
    instrument_codes = sorted({row["ts_code"] for row in instrument_rows})
    all_codes = sorted(set(feature_codes).union(instrument_codes))
    meta = _load_stock_metadata(all_codes, end)
    meta_by_code = {row["ts_code"]: row for row in meta.to_dict(orient="records")}

    errors: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    for code in feature_codes:
        row = meta_by_code.get(code)
        if row is None:
            errors.append({"code": code, "reason": "missing_stock_basic"})
            continue
        if code.endswith(".BJ") or row.get("exchange") == "BSE":
            errors.append({"code": code, "reason": "bj_bse_feature_exported", "exchange": row.get("exchange")})
        if row.get("list_status") != "L":
            errors.append({"code": code, "reason": "non_active_listing_feature_exported", "list_status": row.get("list_status")})
        if bool(row.get("has_st_record")):
            errors.append({"code": code, "reason": "st_record_feature_exported"})
        if row.get("list_date") is None or row.get("list_date") > end:
            errors.append({"code": code, "reason": "invalid_or_future_list_date", "list_date": str(row.get("list_date"))})

    for item in instrument_rows:
        code = item["ts_code"]
        row = meta_by_code.get(code)
        if row is None:
            errors.append({"code": code, "reason": "instrument_missing_stock_basic"})
            continue
        expected_start = row["list_date"] + timedelta(days=min_listed_days)
        if item["start_date"] < expected_start:
            errors.append(
                {
                    "code": code,
                    "reason": "all_txt_start_before_ipo_filter",
                    "all_txt_start": item["start_date"].isoformat(),
                    "expected_min_start": expected_start.isoformat(),
                    "list_date": row["list_date"].isoformat(),
                }
            )
        if item["end_date"] > end:
            warnings.append(
                {
                    "code": code,
                    "reason": "all_txt_end_after_requested_end",
                    "all_txt_end": item["end_date"].isoformat(),
                    "requested_end": end.isoformat(),
                }
            )

    ipo_young_feature_codes = []
    for code in feature_codes:
        row = meta_by_code.get(code)
        if not row or row.get("list_date") is None:
            continue
        if row["list_date"] + timedelta(days=min_listed_days) > end:
            ipo_young_feature_codes.append(code)
            if code in instrument_codes:
                errors.append({"code": code, "reason": "ipo_young_stock_present_in_all_txt"})

    summary = {
        "ok": not errors,
        "qlib_dir": str(qlib_dir),
        "end": end.isoformat(),
        "min_listed_days": min_listed_days,
        "feature_stock_count": len(feature_codes),
        "all_txt_stock_count": len(instrument_codes),
        "ipo_young_feature_count": len(ipo_young_feature_codes),
        "ipo_young_feature_sample": ipo_young_feature_codes[:20],
        "missing_from_all_txt_count": len(set(feature_codes) - set(instrument_codes)),
        "errors": errors[:100],
        "error_count": len(errors),
        "warnings": warnings[:100],
        "warning_count": len(warnings),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit AIstock authoritative Qlib stock universe policy.")
    parser.add_argument("--qlib-dir", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--min-listed-days", type=int, default=IPO_FILTER_DAYS)
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    result = audit_stock_universe_policy(
        qlib_dir=Path(args.qlib_dir),
        end=date.fromisoformat(args.end),
        min_listed_days=args.min_listed_days,
    )
    text = json.dumps(result, ensure_ascii=False, indent=2, default=str)
    print(text)
    if args.output:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(text, encoding="utf-8")
    return 0 if result["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
