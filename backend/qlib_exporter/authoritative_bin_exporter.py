from __future__ import annotations

import json
import shutil
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd

from ..db.pg_pool import get_conn
from .config import IPO_FILTER_DAYS


PRICE_UNIT_DIVISOR = 1000.0
MINUTE_FREQ_DB = "1m"
MINUTE_FREQ_QLIB = "1min"

DAILY_REQUIRED_COLUMNS = [
    "date",
    "symbol",
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

MINUTE_REQUIRED_COLUMNS = DAILY_REQUIRED_COLUMNS

MINUTE_REQUIRED_BIN_FIELDS = [
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

VALUE_COMPARE_ABS_TOL = 1e-4


@dataclass(frozen=True)
class StockUniverseConfig:
    start: date
    end: date
    exchanges: Sequence[str] | None = None
    exclude_st: bool = True
    exclude_delisted_or_paused: bool = True
    # IPO listing-age is intentionally not applied to exported feature bins.
    # It is applied only to instruments/all.txt so newly listed stocks keep
    # their full post-listing history while remaining ineligible before T+365.
    min_listed_days: int = IPO_FILTER_DAYS
    ts_codes: Sequence[str] | None = None
    universe_key: str | None = None


@dataclass(frozen=True)
class CsvExportSummary:
    dataset: str
    start: str
    end: str
    basis_start: str
    basis_end: str
    csv_dir: str
    csv_files: int
    csv_rows: int
    stocks_requested: int
    stocks_written: int
    skipped_no_price_rows: int
    suspended_prev_close_filled_rows: int
    previous_daily_prev_close_filled_rows: int
    strict_limit: bool
    generated_at: str


STOCK_EXPORT_EXCHANGES = ("sh", "sz")
EXCLUDED_STOCK_EXPORT_EXCHANGES = {"bj"}
DEFAULT_PIT_UNIVERSE_KEY = "shsz_st_pit_active_v1"


def normalize_stock_export_exchanges(exchanges: Sequence[str] | None) -> list[str]:
    """Normalize authoritative stock-export exchanges and reject BSE/BJ stocks."""

    if not exchanges:
        return list(STOCK_EXPORT_EXCHANGES)
    normalized = []
    for item in exchanges:
        value = str(item or "").strip().lower()
        if value:
            normalized.append(value)
    requested = sorted(set(normalized))
    if not requested:
        return list(STOCK_EXPORT_EXCHANGES)
    if EXCLUDED_STOCK_EXPORT_EXCHANGES.intersection(requested):
        raise ValueError("BJ/BSE stocks are excluded from AIstock QE/Qlib stock exports; use sh/sz only")
    unsupported = sorted(set(requested) - set(STOCK_EXPORT_EXCHANGES))
    if unsupported:
        raise ValueError(f"unsupported exchange(s) for stock export: {', '.join(unsupported)}")
    return requested


def _normalize_exchanges(exchanges: Sequence[str] | None) -> list[str]:
    return normalize_stock_export_exchanges(exchanges)


def _exchange_sql_values(exchanges: Sequence[str] | None) -> list[str]:
    mapping = {"sh": "SSE", "sz": "SZSE"}
    values = []
    for item in _normalize_exchanges(exchanges):
        values.append(mapping[item])
    return values


def _clean_ts_codes(ts_codes: Iterable[str]) -> list[str]:
    out = []
    seen = set()
    for code in ts_codes:
        value = str(code or "").strip().upper()
        if not value:
            continue
        if value not in seen:
            out.append(value)
            seen.add(value)
    return out


def _is_bj_ts_code(code: str) -> bool:
    value = str(code or "").strip().upper()
    return value.endswith(".BJ") or value.startswith("BJ")


def _reject_bj_ts_codes(ts_codes: Sequence[str]) -> None:
    bj_codes = [code for code in ts_codes if _is_bj_ts_code(code)]
    if bj_codes:
        sample = ", ".join(bj_codes[:5])
        raise ValueError(f"BJ/BSE stocks are excluded from AIstock QE/Qlib stock exports; invalid codes: {sample}")


def _enforce_stock_export_policy(config: StockUniverseConfig) -> None:
    if config.universe_key:
        return
    if not config.exclude_st:
        raise ValueError("authoritative QE/Qlib stock exports must exclude all stocks that have ST records")
    if not config.exclude_delisted_or_paused:
        raise ValueError("authoritative QE/Qlib stock exports must exclude delisted or paused listings")


def _resolve_explicit_stock_universe(config: StockUniverseConfig, codes: Sequence[str]) -> list[str]:
    exchange_values = _exchange_sql_values(config.exchanges)
    conditions = [
        "s.ts_code = ANY(%(codes)s)",
        "s.exchange = ANY(%(exchanges)s)",
        "s.list_date IS NOT NULL",
        "s.list_date <= %(end)s",
        "s.list_status = 'L'",
    ]
    params: dict[str, Any] = {
        "codes": list(codes),
        "exchanges": exchange_values,
        "end": config.end,
    }
    if config.exclude_st:
        conditions.append(
            """
            NOT EXISTS (
                SELECT 1
                FROM market.stock_st st
                WHERE st.ts_code = s.ts_code
                  AND st.ann_date <= %(end)s
            )
            """
        )
    sql = f"""
        SELECT s.ts_code
        FROM market.stock_basic s
        WHERE {' AND '.join(conditions)}
        ORDER BY s.ts_code
    """
    with get_conn() as conn:
        df = pd.read_sql(sql, conn, params=params)
    allowed = _clean_ts_codes(df["ts_code"].tolist()) if not df.empty else []
    missing = [code for code in codes if code not in set(allowed)]
    if missing:
        sample = ", ".join(missing[:5])
        raise ValueError(f"explicit ts_codes include stocks outside the eligible SH/SZ QE export universe: {sample}")
    return allowed


def resolve_stock_universe(config: StockUniverseConfig) -> list[str]:
    """Resolve the export universe from stock_basic using explicit, reproducible filters."""

    if config.min_listed_days < 0:
        raise ValueError("min_listed_days must be >= 0")
    _enforce_stock_export_policy(config)

    if config.universe_key:
        return resolve_stock_universe_from_pit_spans(
            start=config.start,
            end=config.end,
            universe_key=config.universe_key,
            exchanges=config.exchanges,
            ts_codes=config.ts_codes,
        )

    if config.ts_codes:
        codes = _clean_ts_codes(config.ts_codes)
        _reject_bj_ts_codes(codes)
        return _resolve_explicit_stock_universe(config, codes)

    exchange_values = _exchange_sql_values(config.exchanges)
    conditions = [
        "s.exchange = ANY(%(exchanges)s)",
        "s.list_date IS NOT NULL",
        "s.list_date <= %(end)s",
        "s.list_status = 'L'",
    ]
    params: dict[str, Any] = {
        "exchanges": exchange_values,
        "end": config.end,
    }

    if config.exclude_st:
        conditions.append(
            """
            NOT EXISTS (
                SELECT 1
                FROM market.stock_st st
                WHERE st.ts_code = s.ts_code
                  AND st.ann_date <= %(end)s
            )
            """
        )

    sql = f"""
        SELECT s.ts_code
        FROM market.stock_basic s
        WHERE {' AND '.join(conditions)}
        ORDER BY s.ts_code
    """
    with get_conn() as conn:
        df = pd.read_sql(sql, conn, params=params)
    return _clean_ts_codes(df["ts_code"].tolist())


def resolve_stock_universe_from_pit_spans(
    *,
    start: date,
    end: date,
    universe_key: str = DEFAULT_PIT_UNIVERSE_KEY,
    exchanges: Sequence[str] | None = None,
    ts_codes: Sequence[str] | None = None,
) -> list[str]:
    """Resolve stock export codes from PIT eligibility spans.

    The PIT universe controls buy/selection eligibility in instruments/all.txt.
    Feature bins still preserve each selected stock's factual price history in
    the requested export window.
    """

    if end < start:
        raise ValueError("end must be >= start")
    if not universe_key:
        raise ValueError("universe_key is required for PIT stock universe export")

    normalized_exchanges = _normalize_exchanges(exchanges)
    suffixes = tuple(f".{item.upper()}" for item in normalized_exchanges)
    codes_filter = _clean_ts_codes(ts_codes or [])
    if codes_filter:
        _reject_bj_ts_codes(codes_filter)

    conditions = [
        "p.universe_key = %(universe_key)s",
        "p.eligible_start <= %(end)s",
        "p.eligible_end >= %(start)s",
    ]
    params: dict[str, Any] = {"universe_key": universe_key, "start": start, "end": end}
    if suffixes:
        conditions.append("p.ts_code LIKE ANY(%(suffixes)s)")
        params["suffixes"] = [f"%{suffix}" for suffix in suffixes]
    if codes_filter:
        conditions.append("p.ts_code = ANY(%(codes)s)")
        params["codes"] = codes_filter

    sql = f"""
        SELECT DISTINCT p.ts_code
        FROM market.stock_universe_pit_spans p
        WHERE {' AND '.join(conditions)}
        ORDER BY p.ts_code
    """
    with get_conn() as conn:
        df = pd.read_sql(sql, conn, params=params)
    codes = _clean_ts_codes(df["ts_code"].tolist()) if not df.empty else []
    if codes_filter:
        missing = [code for code in codes_filter if code not in set(codes)]
        if missing:
            sample = ", ".join(missing[:5])
            raise ValueError(
                f"explicit ts_codes include stocks outside PIT universe {universe_key} in {start}~{end}: {sample}"
            )
    if not codes:
        raise RuntimeError(f"no stocks found in PIT universe {universe_key} for {start}~{end}")
    return codes


def _instrument_symbol_to_ts_code(symbol: str) -> str:
    value = str(symbol or "").strip()
    upper = value.upper()
    if "." in upper:
        return upper
    lower = value.lower()
    if len(lower) == 8 and lower[:2] in {"sh", "sz", "bj"}:
        return f"{lower[2:].upper()}.{lower[:2].upper()}"
    if len(lower) == 8 and lower[-2:] in {"sh", "sz", "bj"}:
        return f"{lower[:6].upper()}.{lower[-2:].upper()}"
    return upper


def _split_instrument_line(raw: str) -> tuple[str, str, str]:
    text = raw.strip()
    if not text:
        raise ValueError("empty instrument line")
    if "\t" in text:
        parts = [part.strip() for part in text.split("\t") if part.strip()]
    elif "," in text:
        parts = [part.strip() for part in text.split(",") if part.strip()]
    else:
        parts = text.split()
        if len(parts) >= 5:
            parts = [parts[0], f"{parts[1]} {parts[2]}", f"{parts[3]} {parts[4]}"]
    if len(parts) < 3:
        raise ValueError(f"invalid Qlib instrument line: {raw!r}")
    return parts[0], parts[1], parts[2]


def _date_prefix(value: str) -> date:
    try:
        return date.fromisoformat(str(value).strip()[:10])
    except ValueError as exc:
        raise ValueError(f"invalid instrument date value: {value!r}") from exc


def _format_instrument_start(original_start: str, effective_start: date) -> str:
    original_start = str(original_start).strip()
    if _date_prefix(original_start) == effective_start:
        return original_start
    suffix = original_start[10:] if len(original_start) > 10 else ""
    return f"{effective_start.isoformat()}{suffix}"


def _format_instrument_end(original_end: str, effective_end: date) -> str:
    original_end = str(original_end).strip()
    if _date_prefix(original_end) == effective_end:
        return original_end
    suffix = original_end[10:] if len(original_end) > 10 else ""
    return f"{effective_end.isoformat()}{suffix}"


def _load_existing_instrument_ranges(all_txt: Path) -> dict[str, list[tuple[str, str, str, str]]]:
    raw_lines = [line for line in all_txt.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not raw_lines:
        raise RuntimeError(f"empty instruments/all.txt: {all_txt}")
    parsed: dict[str, list[tuple[str, str, str, str]]] = {}
    for raw in raw_lines:
        symbol, start_value, end_value = _split_instrument_line(raw)
        ts_code = _instrument_symbol_to_ts_code(symbol)
        _reject_bj_ts_codes([ts_code])
        parsed.setdefault(ts_code, []).append((symbol, start_value, end_value, ts_code))
    return parsed


def _load_pit_spans_for_all_txt(
    *,
    universe_key: str,
    start: date,
    end: date,
    ts_codes: Sequence[str],
) -> pd.DataFrame:
    clean_codes = _clean_ts_codes(ts_codes)
    if not clean_codes:
        return pd.DataFrame(columns=["ts_code", "eligible_start", "eligible_end", "entry_reason", "exit_reason"])
    sql = """
        SELECT ts_code, eligible_start, eligible_end, entry_reason, exit_reason
        FROM market.stock_universe_pit_spans
        WHERE universe_key = %(universe_key)s
          AND ts_code = ANY(%(codes)s)
          AND eligible_start <= %(end)s
          AND eligible_end >= %(start)s
        ORDER BY ts_code, eligible_start, eligible_end
    """
    with get_conn() as conn:
        df = pd.read_sql(
            sql,
            conn,
            params={"universe_key": universe_key, "codes": clean_codes, "start": start, "end": end},
        )
    if df.empty:
        return df
    df["ts_code"] = df["ts_code"].astype(str).str.upper()
    df["eligible_start"] = pd.to_datetime(df["eligible_start"]).dt.date
    df["eligible_end"] = pd.to_datetime(df["eligible_end"]).dt.date
    return df


def _build_pit_all_txt_lines(
    *,
    existing_ranges: dict[str, list[tuple[str, str, str, str]]],
    pit_spans: pd.DataFrame,
    start: date,
    end: date,
) -> tuple[list[str], list[dict[str, str]]]:
    """Build Qlib instruments/all.txt lines from PIT spans and bin data ranges."""

    lines: list[str] = []
    skipped: list[dict[str, str]] = []
    for row in pit_spans.itertuples(index=False):
        ts_code = str(row.ts_code).upper()
        parsed_ranges_raw = existing_ranges.get(ts_code)
        if not parsed_ranges_raw:
            skipped.append({"ts_code": ts_code, "reason": "missing_feature_in_all_txt"})
            continue
        if isinstance(parsed_ranges_raw, tuple):
            # Backward-compatible path for existing unit tests and older callers.
            parsed_ranges = [parsed_ranges_raw]
        else:
            parsed_ranges = parsed_ranges_raw

        matched = False
        for symbol, data_start_value, data_end_value, _ in parsed_ranges:
            data_start = _date_prefix(data_start_value)
            data_end = _date_prefix(data_end_value)
            eligible_start = max(row.eligible_start, data_start, start)
            eligible_end = min(row.eligible_end, data_end, end)
            if eligible_start > eligible_end:
                continue
            matched = True
            lines.append(
                "\t".join(
                    [
                        symbol,
                        _format_instrument_start(data_start_value, eligible_start),
                        _format_instrument_end(data_end_value, eligible_end),
                    ]
                )
            )

        if not matched:
            skipped.append(
                {
                    "ts_code": ts_code,
                    "reason": "no_overlap_with_feature_range",
                    "eligible_start": row.eligible_start.isoformat(),
                    "eligible_end": row.eligible_end.isoformat(),
                    "data_ranges": ";".join(
                        f"{_date_prefix(start_value).isoformat()}~{_date_prefix(end_value).isoformat()}"
                        for _, start_value, end_value, _ in parsed_ranges[:5]
                    ),
                }
            )
    lines = sorted(set(lines), key=lambda line: (_instrument_symbol_to_ts_code(line.split("\t", 1)[0]), line))
    return lines, skipped


def rewrite_stock_all_txt_from_pit_spans(
    *,
    bin_dir: Path,
    universe_key: str = DEFAULT_PIT_UNIVERSE_KEY,
    start: date,
    end: date,
) -> dict[str, Any]:
    """Rewrite Qlib instruments/all.txt using PIT stock-universe spans."""

    if end < start:
        raise ValueError("end must be >= start")
    if not universe_key:
        raise ValueError("universe_key is required")

    all_txt = bin_dir / "instruments" / "all.txt"
    if not all_txt.exists():
        raise FileNotFoundError(all_txt)

    existing_ranges = _load_existing_instrument_ranges(all_txt)
    pit_spans = _load_pit_spans_for_all_txt(
        universe_key=universe_key,
        start=start,
        end=end,
        ts_codes=list(existing_ranges.keys()),
    )
    if pit_spans.empty:
        raise RuntimeError(f"no PIT spans found for {universe_key} within {start}~{end}")

    kept_lines, skipped = _build_pit_all_txt_lines(
        existing_ranges=existing_ranges,
        pit_spans=pit_spans,
        start=start,
        end=end,
    )
    if not kept_lines:
        raise RuntimeError("PIT all.txt rewrite would remove every stock; refusing to write empty universe")

    all_txt.write_text("\n".join(kept_lines) + "\n", encoding="utf-8")
    line_instruments = [_instrument_symbol_to_ts_code(line.split("\t", 1)[0]) for line in kept_lines]
    unique_instruments = sorted(set(line_instruments))
    counts = pd.Series(line_instruments).value_counts()
    input_range_count = sum(len(ranges) for ranges in existing_ranges.values())
    summary = {
        "path": str(all_txt),
        "mode": "pit_universe_spans",
        "universe_key": universe_key,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "input_feature_instruments": len(existing_ranges),
        "input_feature_ranges": input_range_count,
        "pit_span_rows": int(len(pit_spans)),
        "output_rows": len(kept_lines),
        "output_instruments": len(unique_instruments),
        "multi_span_instruments": int((counts > 1).sum()) if not counts.empty else 0,
        "skipped_rows": len(skipped),
        "sample_skipped": skipped[:20],
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }
    summary_path = all_txt.parent / "all_pit_universe_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def _load_stock_list_dates(ts_codes: Sequence[str]) -> dict[str, date]:
    clean_codes = _clean_ts_codes(ts_codes)
    if not clean_codes:
        return {}
    sql = """
        SELECT ts_code, list_date
        FROM market.stock_basic
        WHERE ts_code = ANY(%(codes)s)
    """
    with get_conn() as conn:
        df = pd.read_sql(sql, conn, params={"codes": clean_codes})
    if df.empty:
        raise RuntimeError("no stock_basic rows found for instruments/all.txt rewrite")
    df["ts_code"] = df["ts_code"].astype(str).str.upper()
    df["list_date"] = pd.to_datetime(df["list_date"], errors="coerce").dt.date
    missing_date = df.loc[df["list_date"].isna(), "ts_code"].tolist()
    if missing_date:
        sample = ", ".join(missing_date[:5])
        raise RuntimeError(f"stock_basic.list_date is missing for all.txt instruments: {sample}")
    mapping = dict(zip(df["ts_code"], df["list_date"]))
    missing = [code for code in clean_codes if code not in mapping]
    if missing:
        sample = ", ".join(missing[:5])
        raise RuntimeError(f"stock_basic rows are missing for all.txt instruments: {sample}")
    return mapping


def rewrite_stock_all_txt_for_ipo_filter(
    *,
    bin_dir: Path,
    min_listed_days: int = IPO_FILTER_DAYS,
) -> dict[str, Any]:
    """Apply IPO eligibility to instruments/all.txt without deleting feature bins.

    Exported bin files retain the full post-listing history for every eligible
    SH/SZ non-ST active stock. Qlib's tradable universe is controlled by
    instruments/all.txt, where each stock starts at max(data_start,
    list_date + min_listed_days). Stocks still younger than the threshold by
    the dataset end are omitted from all.txt, while their feature bins remain.
    """

    if min_listed_days < 0:
        raise ValueError("min_listed_days must be >= 0")
    all_txt = bin_dir / "instruments" / "all.txt"
    if not all_txt.exists():
        raise FileNotFoundError(all_txt)

    raw_lines = [line for line in all_txt.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not raw_lines:
        raise RuntimeError(f"empty instruments/all.txt: {all_txt}")

    parsed: list[tuple[str, str, str, str]] = []
    ts_codes: list[str] = []
    for raw in raw_lines:
        symbol, start_value, end_value = _split_instrument_line(raw)
        ts_code = _instrument_symbol_to_ts_code(symbol)
        _reject_bj_ts_codes([ts_code])
        parsed.append((symbol, start_value, end_value, ts_code))
        ts_codes.append(ts_code)

    list_dates = _load_stock_list_dates(ts_codes)
    kept_lines: list[str] = []
    skipped: list[dict[str, str]] = []
    for symbol, start_value, end_value, ts_code in parsed:
        data_start = _date_prefix(start_value)
        data_end = _date_prefix(end_value)
        ipo_start = list_dates[ts_code] + timedelta(days=min_listed_days)
        effective_start = max(data_start, ipo_start)
        if effective_start > data_end:
            skipped.append(
                {
                    "symbol": symbol,
                    "ts_code": ts_code,
                    "list_date": list_dates[ts_code].isoformat(),
                    "ipo_start": ipo_start.isoformat(),
                    "data_end": data_end.isoformat(),
                }
            )
            continue
        kept_lines.append(f"{symbol}\t{_format_instrument_start(start_value, effective_start)}\t{end_value}")

    if not kept_lines:
        raise RuntimeError("IPO all.txt rewrite would remove every stock; refusing to write empty universe")

    all_txt.write_text("\n".join(kept_lines) + "\n", encoding="utf-8")
    summary = {
        "path": str(all_txt),
        "mode": "instruments_all_txt",
        "min_listed_days": int(min_listed_days),
        "input_rows": len(raw_lines),
        "output_rows": len(kept_lines),
        "skipped_ipo_rows": len(skipped),
        "sample_skipped": skipped[:20],
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }
    summary_path = all_txt.parent / "all_ipo_filter_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def _load_adj_factors(code: str, basis_start: date, basis_end: date) -> pd.DataFrame:
    sql = """
        SELECT ts_code, trade_date, adj_factor
        FROM market.adj_factor
        WHERE ts_code = %(code)s
          AND trade_date >= %(basis_start)s
          AND trade_date <= %(basis_end)s
        ORDER BY trade_date
    """
    with get_conn() as conn:
        df = pd.read_sql(sql, conn, params={"code": code, "basis_start": basis_start, "basis_end": basis_end})
    if df.empty:
        raise RuntimeError(f"{code}: no adj_factor rows in basis window {basis_start}~{basis_end}")
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    df["adj_factor"] = pd.to_numeric(df["adj_factor"], errors="coerce")
    if df["adj_factor"].isna().any() or (df["adj_factor"] <= 0).any():
        bad = df.loc[df["adj_factor"].isna() | (df["adj_factor"] <= 0), ["trade_date", "adj_factor"]].head()
        raise RuntimeError(f"{code}: invalid adj_factor rows: {bad.to_dict(orient='records')}")
    denominator = float(df["adj_factor"].max())
    if not np.isfinite(denominator) or denominator <= 0:
        raise RuntimeError(f"{code}: invalid qfq denominator {denominator}")
    df["qfq_factor"] = (df["adj_factor"] / denominator).astype("float64")
    return df[["ts_code", "trade_date", "qfq_factor"]]


def _load_limits(code: str, start: date, end: date) -> pd.DataFrame:
    sql = """
        SELECT ts_code,
               trade_date,
               pre_close AS prev_close,
               up_limit AS up_limit_price,
               down_limit AS down_limit_price
        FROM market.stk_limit
        WHERE ts_code = %(code)s
          AND trade_date >= %(start)s
          AND trade_date <= %(end)s
        ORDER BY trade_date
    """
    with get_conn() as conn:
        df = pd.read_sql(sql, conn, params={"code": code, "start": start, "end": end})
    if df.empty:
        return pd.DataFrame(columns=["ts_code", "trade_date", "prev_close", "up_limit_price", "down_limit_price"])
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    for col in ["prev_close", "up_limit_price", "down_limit_price"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _load_suspend_dates(code: str, start: date, end: date) -> set[date]:
    sql = """
        SELECT trade_date
        FROM market.suspend_d
        WHERE ts_code = %(code)s
          AND trade_date >= %(start)s
          AND trade_date <= %(end)s
          AND suspend_type = 'S'
    """
    with get_conn() as conn:
        df = pd.read_sql(sql, conn, params={"code": code, "start": start, "end": end})
    if df.empty:
        return set()
    return set(pd.to_datetime(df["trade_date"]).dt.date.tolist())


def _load_minute_raw(code: str, start: date, end: date) -> pd.DataFrame:
    start_ts = f"{start.isoformat()} 00:00:00+08"
    end_exclusive = f"{(end + timedelta(days=1)).isoformat()} 00:00:00+08"
    sql = """
        SELECT m.trade_time AT TIME ZONE 'Asia/Shanghai' AS trade_time,
               m.ts_code,
               m.open_li,
               m.high_li,
               m.low_li,
               m.close_li,
               m.volume_hand,
               m.amount_li
        FROM market.kline_minute_raw m
        JOIN market.stock_basic s ON s.ts_code = m.ts_code
        WHERE m.ts_code = %(code)s
          AND m.freq = %(freq)s
          AND m.trade_time >= %(start_ts)s::timestamptz
          AND m.trade_time < %(end_exclusive)s::timestamptz
          AND (m.trade_time AT TIME ZONE 'Asia/Shanghai')::date >= s.list_date
        ORDER BY m.trade_time
    """
    with get_conn() as conn:
        return pd.read_sql(
            sql,
            conn,
            params={"code": code, "freq": MINUTE_FREQ_DB, "start_ts": start_ts, "end_exclusive": end_exclusive},
        )


def _load_daily_raw(code: str, start: date, end: date) -> pd.DataFrame:
    sql = """
        SELECT d.trade_date,
               d.ts_code,
               d.open_li,
               d.high_li,
               d.low_li,
               d.close_li,
               d.volume_hand,
               d.amount_li
        FROM market.kline_daily_raw d
        JOIN market.stock_basic s ON s.ts_code = d.ts_code
        WHERE d.ts_code = %(code)s
          AND d.trade_date >= %(start)s
          AND d.trade_date <= %(end)s
          AND d.trade_date >= s.list_date
        ORDER BY d.trade_date
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn, params={"code": code, "start": start, "end": end})


def _load_daily_close_history(code: str, start: date, end: date, lookback_days: int = 180) -> pd.DataFrame:
    sql = """
        SELECT d.trade_date, d.close_li
        FROM market.kline_daily_raw d
        JOIN market.stock_basic s ON s.ts_code = d.ts_code
        WHERE d.ts_code = %(code)s
          AND d.trade_date >= %(lookback_start)s
          AND d.trade_date <= %(end)s
          AND d.trade_date >= s.list_date
        ORDER BY d.trade_date
    """
    lookback_start = start - timedelta(days=lookback_days)
    with get_conn() as conn:
        df = pd.read_sql(sql, conn, params={"code": code, "lookback_start": lookback_start, "end": end})
    if df.empty:
        return pd.DataFrame(columns=["trade_date", "daily_close"])
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    df["daily_close"] = pd.to_numeric(df["close_li"], errors="coerce") / PRICE_UNIT_DIVISOR
    return df[["trade_date", "daily_close"]]


def _fill_prev_close_from_daily_history(
    df: pd.DataFrame,
    daily_history: pd.DataFrame,
    *,
    code: str | None = None,
) -> int:
    """Fill missing pre-close with the last actual daily close before the row date."""

    if df.empty or daily_history.empty or "prev_close" not in df.columns or not df["prev_close"].isna().any():
        return 0

    if code is not None:
        histories = {code: daily_history.sort_values("trade_date")}
        code_values = pd.Series(code, index=df.index)
    else:
        histories = {
            str(symbol): group.sort_values("trade_date")
            for symbol, group in daily_history.groupby("ts_code", sort=False)
        }
        code_values = df["ts_code"].astype(str)

    filled = 0
    for symbol, history in histories.items():
        mask = df["prev_close"].isna() & (code_values == symbol)
        if not mask.any():
            continue
        dates = pd.to_datetime(history["trade_date"]).to_numpy(dtype="datetime64[D]")
        closes = pd.to_numeric(history["daily_close"], errors="coerce").to_numpy(dtype="float64")
        row_idx = df.index[mask]
        row_dates = pd.to_datetime(df.loc[row_idx, "trade_date"]).to_numpy(dtype="datetime64[D]")
        positions = np.searchsorted(dates, row_dates, side="left") - 1
        valid = positions >= 0
        if valid.any():
            valid_idx = row_idx[valid]
            df.loc[valid_idx, "prev_close"] = closes[positions[valid]]
            filled += int(valid.sum())
    return filled


def _build_minute_expected_frame(
    code: str,
    raw_df: pd.DataFrame,
    *,
    start: date,
    end: date,
    basis_start: date,
    basis_end: date,
    strict_limit: bool,
) -> pd.DataFrame:
    df = raw_df.copy()
    df["trade_time"] = pd.to_datetime(df["trade_time"])
    df["trade_date"] = df["trade_time"].dt.date

    adj = _load_adj_factors(code, basis_start, basis_end)
    limits = _load_limits(code, start, end)
    df = df.merge(adj[["trade_date", "qfq_factor"]], on="trade_date", how="left")
    df = df.merge(limits[["trade_date", "prev_close", "up_limit_price", "down_limit_price"]], on="trade_date", how="left")
    raw_close_for_fill = pd.to_numeric(df["close_li"], errors="coerce") / PRICE_UNIT_DIVISOR
    filled_prev_close = 0
    filled_prev_close_from_daily = 0
    if df["prev_close"].isna().any():
        suspend_dates = _load_suspend_dates(code, start, end)
        if suspend_dates:
            minute_volume = pd.to_numeric(df["volume_hand"], errors="coerce").fillna(0)
            date_volume = minute_volume.groupby(df["trade_date"]).transform("sum")
            fill_dates = suspend_dates & set(df.loc[date_volume == 0, "trade_date"].tolist())
            fill_mask = df["prev_close"].isna() & df["trade_date"].isin(fill_dates)
            if fill_mask.any():
                df.loc[fill_mask, "prev_close"] = raw_close_for_fill[fill_mask]
                filled_prev_close = int(fill_mask.sum())
    if df["prev_close"].isna().any():
        daily_history = _load_daily_close_history(code, start, end)
        filled_prev_close_from_daily = _fill_prev_close_from_daily_history(df, daily_history, code=code)

    if df["qfq_factor"].isna().any():
        bad = df.loc[df["qfq_factor"].isna(), ["ts_code", "trade_date"]].drop_duplicates().head()
        raise RuntimeError(f"{code}: missing qfq_factor for minute rows: {bad.to_dict(orient='records')}")
    if strict_limit and df[["prev_close", "up_limit_price", "down_limit_price"]].isna().any().any():
        bad = df.loc[df[["prev_close", "up_limit_price", "down_limit_price"]].isna().any(axis=1), ["ts_code", "trade_date"]].drop_duplicates().head()
        raise RuntimeError(f"{code}: missing stk_limit rows for minute export: {bad.to_dict(orient='records')}")

    qfq = pd.to_numeric(df["qfq_factor"], errors="coerce")
    raw_open = pd.to_numeric(df["open_li"], errors="coerce") / PRICE_UNIT_DIVISOR
    raw_high = pd.to_numeric(df["high_li"], errors="coerce") / PRICE_UNIT_DIVISOR
    raw_low = pd.to_numeric(df["low_li"], errors="coerce") / PRICE_UNIT_DIVISOR
    raw_close = pd.to_numeric(df["close_li"], errors="coerce") / PRICE_UNIT_DIVISOR

    out = pd.DataFrame()
    out["date"] = df["trade_time"].dt.strftime("%Y-%m-%d %H:%M:%S")
    out["symbol"] = code
    out["open"] = (raw_open * qfq).astype("float32")
    out["high"] = (raw_high * qfq).astype("float32")
    out["low"] = (raw_low * qfq).astype("float32")
    out["close"] = (raw_close * qfq).astype("float32")
    out["volume"] = (pd.to_numeric(df["volume_hand"], errors="coerce") * 100.0 / qfq).astype("float32")
    out["amount"] = (pd.to_numeric(df["amount_li"], errors="coerce") / PRICE_UNIT_DIVISOR).astype("float32")
    out["factor"] = qfq.astype("float32")
    out["up_limit_price"] = pd.to_numeric(df["up_limit_price"], errors="coerce").astype("float32")
    out["down_limit_price"] = pd.to_numeric(df["down_limit_price"], errors="coerce").astype("float32")
    out["prev_close"] = pd.to_numeric(df["prev_close"], errors="coerce").astype("float32")
    have_limits = out["up_limit_price"].notna() & out["down_limit_price"].notna()
    out["limit_up"] = np.where(have_limits, (raw_close >= out["up_limit_price"] - VALUE_COMPARE_ABS_TOL).astype("float32"), np.nan)
    out["limit_down"] = np.where(have_limits, (raw_close <= out["down_limit_price"] + VALUE_COMPARE_ABS_TOL).astype("float32"), np.nan)
    out.attrs["suspended_prev_close_filled_rows"] = filled_prev_close
    out.attrs["previous_daily_prev_close_filled_rows"] = filled_prev_close_from_daily
    return out


def _build_daily_expected_frame(
    code: str,
    raw_df: pd.DataFrame,
    *,
    start: date,
    end: date,
    basis_start: date,
    basis_end: date,
    strict_limit: bool,
) -> pd.DataFrame:
    df = raw_df.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    adj = _load_adj_factors(code, basis_start, basis_end)
    limits = _load_limits(code, start, end)
    df = df.merge(adj[["trade_date", "qfq_factor"]], on="trade_date", how="left")
    df = df.merge(limits[["trade_date", "prev_close", "up_limit_price", "down_limit_price"]], on="trade_date", how="left")
    raw_close_for_fill = pd.to_numeric(df["close_li"], errors="coerce") / PRICE_UNIT_DIVISOR
    filled_prev_close = 0
    filled_prev_close_from_daily = 0
    if df["prev_close"].isna().any():
        suspend_dates = _load_suspend_dates(code, start, end)
        if suspend_dates:
            daily_volume = pd.to_numeric(df["volume_hand"], errors="coerce").fillna(0)
            fill_mask = df["prev_close"].isna() & df["trade_date"].isin(suspend_dates) & (daily_volume == 0)
            if fill_mask.any():
                df.loc[fill_mask, "prev_close"] = raw_close_for_fill[fill_mask]
                filled_prev_close = int(fill_mask.sum())
    if df["prev_close"].isna().any():
        daily_history = _load_daily_close_history(code, start, end)
        filled_prev_close_from_daily = _fill_prev_close_from_daily_history(df, daily_history, code=code)
    if df["qfq_factor"].isna().any():
        bad = df.loc[df["qfq_factor"].isna(), ["ts_code", "trade_date"]].drop_duplicates().head()
        raise RuntimeError(f"{code}: missing qfq_factor for daily rows: {bad.to_dict(orient='records')}")
    if strict_limit and df[["prev_close", "up_limit_price", "down_limit_price"]].isna().any().any():
        bad = df.loc[df[["prev_close", "up_limit_price", "down_limit_price"]].isna().any(axis=1), ["ts_code", "trade_date"]].drop_duplicates().head()
        raise RuntimeError(f"{code}: missing stk_limit rows for daily export: {bad.to_dict(orient='records')}")

    qfq = pd.to_numeric(df["qfq_factor"], errors="coerce")
    raw_open = pd.to_numeric(df["open_li"], errors="coerce") / PRICE_UNIT_DIVISOR
    raw_high = pd.to_numeric(df["high_li"], errors="coerce") / PRICE_UNIT_DIVISOR
    raw_low = pd.to_numeric(df["low_li"], errors="coerce") / PRICE_UNIT_DIVISOR
    raw_close = pd.to_numeric(df["close_li"], errors="coerce") / PRICE_UNIT_DIVISOR

    out = pd.DataFrame()
    out["date"] = pd.Series([d.isoformat() for d in df["trade_date"]], index=df.index)
    out["symbol"] = code
    out["open"] = (raw_open * qfq).astype("float32")
    out["high"] = (raw_high * qfq).astype("float32")
    out["low"] = (raw_low * qfq).astype("float32")
    out["close"] = (raw_close * qfq).astype("float32")
    out["volume"] = (pd.to_numeric(df["volume_hand"], errors="coerce") * 100.0 / qfq).astype("float32")
    out["amount"] = (pd.to_numeric(df["amount_li"], errors="coerce") / PRICE_UNIT_DIVISOR).astype("float32")
    out["factor"] = qfq.astype("float32")
    out["up_limit_price"] = pd.to_numeric(df["up_limit_price"], errors="coerce").astype("float32")
    out["down_limit_price"] = pd.to_numeric(df["down_limit_price"], errors="coerce").astype("float32")
    out["prev_close"] = pd.to_numeric(df["prev_close"], errors="coerce").astype("float32")
    have_limits = out["up_limit_price"].notna() & out["down_limit_price"].notna()
    out["limit_up"] = np.where(have_limits, (raw_close >= out["up_limit_price"] - VALUE_COMPARE_ABS_TOL).astype("float32"), np.nan)
    out["limit_down"] = np.where(have_limits, (raw_close <= out["down_limit_price"] + VALUE_COMPARE_ABS_TOL).astype("float32"), np.nan)
    out.attrs["suspended_prev_close_filled_rows"] = filled_prev_close
    out.attrs["previous_daily_prev_close_filled_rows"] = filled_prev_close_from_daily
    return out


def _check_required_non_null(df: pd.DataFrame, code: str, required: Sequence[str]) -> None:
    bad_cols = [col for col in required if col in df.columns and df[col].isna().any()]
    missing_cols = [col for col in required if col not in df.columns]
    if missing_cols or bad_cols:
        detail: dict[str, Any] = {"missing_columns": missing_cols, "nan_columns": bad_cols}
        if bad_cols:
            sample_cols = ["date", "symbol", *bad_cols[:4]]
            detail["sample"] = df.loc[df[bad_cols].isna().any(axis=1), [c for c in sample_cols if c in df.columns]].head(5).to_dict(orient="records")
        raise RuntimeError(f"{code}: required CSV fields are missing/null: {detail}")


def _write_csv_atomic(df: pd.DataFrame, path: Path, columns: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    try:
        df.loc[:, list(columns)].to_csv(tmp, index=False)
        tmp.replace(path)
    finally:
        if tmp.exists():
            tmp.unlink()


def _read_csv_last_date(path: Path) -> str | None:
    """Read the last data row timestamp from a per-stock CSV without loading it."""

    if not path.exists() or path.stat().st_size == 0:
        return None
    with path.open("rb") as f:
        try:
            f.seek(-min(path.stat().st_size, 8192), 2)
        except OSError:
            f.seek(0)
        tail = f.read().decode("utf-8", errors="ignore").splitlines()
    for line in reversed(tail):
        line = line.strip()
        if not line or line.startswith("date,"):
            continue
        return line.split(",", 1)[0]
    return None


def _count_csv_data_rows(path: Path) -> int:
    with path.open("rb") as f:
        line_count = sum(chunk.count(b"\n") for chunk in iter(lambda: f.read(1024 * 1024), b""))
    return max(0, line_count - 1)


def _count_csv_dir(csv_dir: Path) -> tuple[int, int]:
    files = list(csv_dir.glob("*.csv")) if csv_dir.exists() else []
    return len(files), sum(_count_csv_data_rows(path) for path in files)


def _finalize_summary(summary: CsvExportSummary, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(summary), ensure_ascii=False, indent=2), encoding="utf-8")


def export_stock_minute_csv(
    *,
    snapshot_id: str,
    start: date,
    end: date,
    csv_root: Path,
    exchanges: Sequence[str] | None = None,
    exclude_st: bool = True,
    exclude_delisted_or_paused: bool = True,
    ts_codes: Sequence[str] | None = None,
    universe_key: str | None = None,
    basis_start: date | None = None,
    basis_end: date | None = None,
    strict_limit: bool = True,
    overwrite_csv: bool = False,
) -> CsvExportSummary:
    """Export authoritative per-stock 1min CSV files for Qlib dump_bin.

    The formulas intentionally match the validated QE/V25 contract:
    Shanghai minute timestamp, qfq OHLCV/factor, raw limit/pre-close prices,
    and close-only limit flags. There is no default fill for missing factors or
    limit rows when strict mode is enabled.
    """

    if end < start:
        raise ValueError("end must be >= start")
    basis_start = basis_start or start
    basis_end = basis_end or end
    if basis_end < basis_start:
        raise ValueError("basis_end must be >= basis_start")

    codes = resolve_stock_universe(
        StockUniverseConfig(
            start=start,
            end=end,
            exchanges=exchanges,
            exclude_st=exclude_st,
            exclude_delisted_or_paused=exclude_delisted_or_paused,
            ts_codes=ts_codes,
            universe_key=universe_key,
        )
    )
    csv_dir = csv_root / snapshot_id / "stock_minute_1min"
    if overwrite_csv and csv_dir.exists():
        shutil.rmtree(csv_dir)
    csv_dir.mkdir(parents=True, exist_ok=True)

    csv_files = 0
    csv_rows = 0
    skipped = 0
    suspended_prev_close_filled_rows = 0
    previous_daily_prev_close_filled_rows = 0

    for code in codes:
        df = _load_minute_raw(code, start, end)
        if df.empty:
            skipped += 1
            continue
        out = _build_minute_expected_frame(
            code,
            df,
            start=start,
            end=end,
            basis_start=basis_start,
            basis_end=basis_end,
            strict_limit=strict_limit,
        )

        _check_required_non_null(out, code, MINUTE_REQUIRED_COLUMNS if strict_limit else ["date", "symbol", "open", "high", "low", "close", "volume", "amount", "factor"])
        _write_csv_atomic(out, csv_dir / f"{code}.csv", MINUTE_REQUIRED_COLUMNS)
        csv_files += 1
        csv_rows += len(out)
        suspended_prev_close_filled_rows += int(out.attrs.get("suspended_prev_close_filled_rows", 0))
        previous_daily_prev_close_filled_rows += int(out.attrs.get("previous_daily_prev_close_filled_rows", 0))

    summary = CsvExportSummary(
        dataset="stock_minute_1min",
        start=start.isoformat(),
        end=end.isoformat(),
        basis_start=basis_start.isoformat(),
        basis_end=basis_end.isoformat(),
        csv_dir=str(csv_dir),
        csv_files=csv_files,
        csv_rows=csv_rows,
        stocks_requested=len(codes),
        stocks_written=csv_files,
        skipped_no_price_rows=skipped,
        suspended_prev_close_filled_rows=suspended_prev_close_filled_rows,
        previous_daily_prev_close_filled_rows=previous_daily_prev_close_filled_rows,
        strict_limit=strict_limit,
        generated_at=datetime.now().isoformat(timespec="seconds"),
    )
    _finalize_summary(summary, csv_dir / "export_summary.json")
    return summary


def export_stock_minute_csv_chunked(
    *,
    snapshot_id: str,
    start: date,
    end: date,
    csv_root: Path,
    exchanges: Sequence[str] | None = None,
    exclude_st: bool = True,
    exclude_delisted_or_paused: bool = True,
    ts_codes: Sequence[str] | None = None,
    universe_key: str | None = None,
    basis_start: date | None = None,
    basis_end: date | None = None,
    strict_limit: bool = True,
    code_batch_size: int = 20,
    chunk_months: int = 3,
    overwrite_csv: bool = False,
    resume_csv: bool = False,
) -> CsvExportSummary:
    """Export large 1min datasets by date/code chunks without changing formulas."""

    if end < start:
        raise ValueError("end must be >= start")
    if code_batch_size <= 0:
        raise ValueError("code_batch_size must be > 0")
    if chunk_months <= 0:
        raise ValueError("chunk_months must be > 0")
    basis_start = basis_start or start
    basis_end = basis_end or end

    codes = resolve_stock_universe(
        StockUniverseConfig(
            start=start,
            end=end,
            exchanges=exchanges,
            exclude_st=exclude_st,
            exclude_delisted_or_paused=exclude_delisted_or_paused,
            ts_codes=ts_codes,
            universe_key=universe_key,
        )
    )
    csv_dir = csv_root / snapshot_id / "stock_minute_1min"
    if overwrite_csv and resume_csv:
        raise ValueError("overwrite_csv and resume_csv cannot both be true")
    if overwrite_csv and csv_dir.exists():
        shutil.rmtree(csv_dir)
    csv_dir.mkdir(parents=True, exist_ok=True)
    existing_last_dates: dict[str, str] = {}
    if resume_csv:
        for path in csv_dir.glob("*.csv"):
            last_date = _read_csv_last_date(path)
            if last_date:
                existing_last_dates[path.stem.upper()] = last_date

    with get_conn() as conn:
        adj = pd.read_sql(
            """
            SELECT ts_code, trade_date, adj_factor
            FROM market.adj_factor
            WHERE ts_code = ANY(%(codes)s)
              AND trade_date >= %(basis_start)s
              AND trade_date <= %(basis_end)s
            ORDER BY ts_code, trade_date
            """,
            conn,
            params={"codes": codes, "basis_start": basis_start, "basis_end": basis_end},
        )
        limits = pd.read_sql(
            """
            SELECT ts_code,
                   trade_date,
                   pre_close AS prev_close,
                   up_limit AS up_limit_price,
                   down_limit AS down_limit_price
            FROM market.stk_limit
            WHERE ts_code = ANY(%(codes)s)
              AND trade_date >= %(start)s
              AND trade_date <= %(end)s
            ORDER BY ts_code, trade_date
            """,
            conn,
            params={"codes": codes, "start": start, "end": end},
        )
        suspend = pd.read_sql(
            """
            SELECT ts_code, trade_date
            FROM market.suspend_d
            WHERE ts_code = ANY(%(codes)s)
              AND trade_date >= %(start)s
              AND trade_date <= %(end)s
              AND suspend_type = 'S'
            """,
            conn,
            params={"codes": codes, "start": start, "end": end},
        )
        daily_history = pd.read_sql(
            """
            SELECT d.ts_code, d.trade_date, d.close_li
            FROM market.kline_daily_raw d
            JOIN market.stock_basic s ON s.ts_code = d.ts_code
            WHERE d.ts_code = ANY(%(codes)s)
              AND d.trade_date >= %(lookback_start)s
              AND d.trade_date <= %(end)s
              AND d.trade_date >= s.list_date
            ORDER BY d.ts_code, d.trade_date
            """,
            conn,
            params={"codes": codes, "lookback_start": start - timedelta(days=180), "end": end},
        )

    if adj.empty:
        raise RuntimeError(f"no adj_factor rows in basis window {basis_start}~{basis_end}")
    adj["trade_date"] = pd.to_datetime(adj["trade_date"]).dt.date
    adj["adj_factor"] = pd.to_numeric(adj["adj_factor"], errors="coerce")
    if adj["adj_factor"].isna().any() or (adj["adj_factor"] <= 0).any():
        bad = adj.loc[adj["adj_factor"].isna() | (adj["adj_factor"] <= 0), ["ts_code", "trade_date", "adj_factor"]].head()
        raise RuntimeError(f"invalid adj_factor rows: {bad.to_dict(orient='records')}")
    adj["qfq_factor"] = (adj["adj_factor"] / adj.groupby("ts_code")["adj_factor"].transform("max")).astype("float64")
    adj = adj[["ts_code", "trade_date", "qfq_factor"]]

    if limits.empty and strict_limit:
        raise RuntimeError(f"no stk_limit rows in export window {start}~{end}")
    if not limits.empty:
        limits["trade_date"] = pd.to_datetime(limits["trade_date"]).dt.date
        for col in ["prev_close", "up_limit_price", "down_limit_price"]:
            limits[col] = pd.to_numeric(limits[col], errors="coerce")

    suspend_dates: set[tuple[str, date]] = set()
    if not suspend.empty:
        suspend["trade_date"] = pd.to_datetime(suspend["trade_date"]).dt.date
        suspend_dates = set(zip(suspend["ts_code"].astype(str), suspend["trade_date"]))

    if daily_history.empty:
        daily_history = pd.DataFrame(columns=["ts_code", "trade_date", "daily_close"])
    else:
        daily_history["trade_date"] = pd.to_datetime(daily_history["trade_date"]).dt.date
        daily_history["daily_close"] = pd.to_numeric(daily_history["close_li"], errors="coerce") / PRICE_UNIT_DIVISOR
        daily_history = daily_history[["ts_code", "trade_date", "daily_close"]]

    csv_files_seen: set[str] = set()
    csv_rows = 0
    suspended_prev_close_filled_rows = 0
    previous_daily_prev_close_filled_rows = 0
    current = pd.Timestamp(start)
    end_exclusive_all = pd.Timestamp(end) + pd.Timedelta(days=1)

    while current < end_exclusive_all:
        next_chunk = min(current + pd.DateOffset(months=chunk_months), end_exclusive_all)
        chunk_last = (next_chunk - pd.Timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
        start_ts = f"{current.date().isoformat()} 00:00:00+08"
        end_ts = f"{next_chunk.date().isoformat()} 00:00:00+08"
        for offset in range(0, len(codes), code_batch_size):
            batch_codes = codes[offset : offset + code_batch_size]
            if resume_csv and batch_codes and all(existing_last_dates.get(code, "") >= chunk_last for code in batch_codes):
                continue
            with get_conn() as conn:
                df = pd.read_sql(
                    """
                    SELECT m.trade_time AT TIME ZONE 'Asia/Shanghai' AS trade_time,
                           m.ts_code,
                           m.open_li,
                           m.high_li,
                           m.low_li,
                           m.close_li,
                           m.volume_hand,
                           m.amount_li
                    FROM market.kline_minute_raw m
                    JOIN market.stock_basic s ON s.ts_code = m.ts_code
                    WHERE m.ts_code = ANY(%(codes)s)
                      AND m.freq = %(freq)s
                      AND m.trade_time >= %(start_ts)s::timestamptz
                      AND m.trade_time < %(end_ts)s::timestamptz
                      AND (m.trade_time AT TIME ZONE 'Asia/Shanghai')::date >= s.list_date
                    ORDER BY m.ts_code, m.trade_time
                    """,
                    conn,
                    params={"codes": batch_codes, "freq": MINUTE_FREQ_DB, "start_ts": start_ts, "end_ts": end_ts},
                )
            if df.empty:
                continue
            df["trade_time"] = pd.to_datetime(df["trade_time"])
            df["trade_date"] = df["trade_time"].dt.date
            df = df.merge(adj, on=["ts_code", "trade_date"], how="left")
            df = df.merge(limits, on=["ts_code", "trade_date"], how="left")
            raw_close_for_fill = pd.to_numeric(df["close_li"], errors="coerce") / PRICE_UNIT_DIVISOR
            if df["prev_close"].isna().any() and suspend_dates:
                volume = pd.to_numeric(df["volume_hand"], errors="coerce").fillna(0)
                date_volume = volume.groupby([df["ts_code"], df["trade_date"]]).transform("sum")
                suspend_mask = pd.Series(list(zip(df["ts_code"], df["trade_date"])), index=df.index).isin(suspend_dates)
                fill_mask = df["prev_close"].isna() & suspend_mask & (date_volume == 0)
                if fill_mask.any():
                    df.loc[fill_mask, "prev_close"] = raw_close_for_fill[fill_mask]
                    suspended_prev_close_filled_rows += int(fill_mask.sum())
            if df["prev_close"].isna().any():
                previous_daily_prev_close_filled_rows += _fill_prev_close_from_daily_history(df, daily_history)

            if df["qfq_factor"].isna().any():
                bad = df.loc[df["qfq_factor"].isna(), ["ts_code", "trade_date"]].drop_duplicates().head()
                raise RuntimeError(f"missing qfq_factor for minute rows: {bad.to_dict(orient='records')}")
            limit_cols = ["prev_close", "up_limit_price", "down_limit_price"]
            if strict_limit and df[limit_cols].isna().any().any():
                bad = df.loc[df[limit_cols].isna().any(axis=1), ["ts_code", "trade_date"]].drop_duplicates().head()
                raise RuntimeError(f"missing stk_limit rows for minute export: {bad.to_dict(orient='records')}")

            qfq = pd.to_numeric(df["qfq_factor"], errors="coerce")
            raw_open = pd.to_numeric(df["open_li"], errors="coerce") / PRICE_UNIT_DIVISOR
            raw_high = pd.to_numeric(df["high_li"], errors="coerce") / PRICE_UNIT_DIVISOR
            raw_low = pd.to_numeric(df["low_li"], errors="coerce") / PRICE_UNIT_DIVISOR
            raw_close = pd.to_numeric(df["close_li"], errors="coerce") / PRICE_UNIT_DIVISOR
            out = pd.DataFrame()
            out["date"] = df["trade_time"].dt.strftime("%Y-%m-%d %H:%M:%S")
            out["symbol"] = df["ts_code"].astype(str)
            out["open"] = (raw_open * qfq).astype("float32")
            out["high"] = (raw_high * qfq).astype("float32")
            out["low"] = (raw_low * qfq).astype("float32")
            out["close"] = (raw_close * qfq).astype("float32")
            out["volume"] = (pd.to_numeric(df["volume_hand"], errors="coerce") * 100.0 / qfq).astype("float32")
            out["amount"] = (pd.to_numeric(df["amount_li"], errors="coerce") / PRICE_UNIT_DIVISOR).astype("float32")
            out["factor"] = qfq.astype("float32")
            out["up_limit_price"] = pd.to_numeric(df["up_limit_price"], errors="coerce").astype("float32")
            out["down_limit_price"] = pd.to_numeric(df["down_limit_price"], errors="coerce").astype("float32")
            out["prev_close"] = pd.to_numeric(df["prev_close"], errors="coerce").astype("float32")
            have_limits = out["up_limit_price"].notna() & out["down_limit_price"].notna()
            out["limit_up"] = np.where(have_limits, (raw_close >= out["up_limit_price"] - VALUE_COMPARE_ABS_TOL).astype("float32"), np.nan)
            out["limit_down"] = np.where(have_limits, (raw_close <= out["down_limit_price"] + VALUE_COMPARE_ABS_TOL).astype("float32"), np.nan)
            required = MINUTE_REQUIRED_COLUMNS if strict_limit else ["date", "symbol", "open", "high", "low", "close", "volume", "amount", "factor"]
            _check_required_non_null(out, ",".join(batch_codes[:3]), required)

            for symbol, group in out.loc[:, MINUTE_REQUIRED_COLUMNS].groupby("symbol", sort=True):
                if resume_csv:
                    last_date = existing_last_dates.get(str(symbol).upper())
                    if last_date:
                        group = group.loc[group["date"] > last_date]
                    if group.empty:
                        continue
                csv_path = csv_dir / f"{symbol}.csv"
                write_header = not csv_path.exists()
                group.to_csv(csv_path, index=False, mode="a", header=write_header)
                csv_files_seen.add(str(symbol))
                existing_last_dates[str(symbol).upper()] = str(group["date"].iloc[-1])
            csv_rows += len(out)
        current = next_chunk

    csv_files_final, csv_rows_final = _count_csv_dir(csv_dir) if resume_csv else (len(csv_files_seen), csv_rows)
    summary = CsvExportSummary(
        dataset="stock_minute_1min",
        start=start.isoformat(),
        end=end.isoformat(),
        basis_start=basis_start.isoformat(),
        basis_end=basis_end.isoformat(),
        csv_dir=str(csv_dir),
        csv_files=csv_files_final,
        csv_rows=csv_rows_final,
        stocks_requested=len(codes),
        stocks_written=csv_files_final,
        skipped_no_price_rows=len(set(codes) - {path.stem.upper() for path in csv_dir.glob("*.csv")}),
        suspended_prev_close_filled_rows=suspended_prev_close_filled_rows,
        previous_daily_prev_close_filled_rows=previous_daily_prev_close_filled_rows,
        strict_limit=strict_limit,
        generated_at=datetime.now().isoformat(timespec="seconds"),
    )
    _finalize_summary(summary, csv_dir / "export_summary.json")
    return summary


def export_stock_daily_csv(
    *,
    snapshot_id: str,
    start: date,
    end: date,
    csv_root: Path,
    exchanges: Sequence[str] | None = None,
    exclude_st: bool = True,
    exclude_delisted_or_paused: bool = True,
    ts_codes: Sequence[str] | None = None,
    universe_key: str | None = None,
    basis_start: date | None = None,
    basis_end: date | None = None,
    strict_limit: bool = False,
    overwrite_csv: bool = False,
) -> CsvExportSummary:
    """Export per-stock day CSV files with the same required QE limit fields."""

    if end < start:
        raise ValueError("end must be >= start")
    basis_start = basis_start or start
    basis_end = basis_end or end

    codes = resolve_stock_universe(
        StockUniverseConfig(
            start=start,
            end=end,
            exchanges=exchanges,
            exclude_st=exclude_st,
            exclude_delisted_or_paused=exclude_delisted_or_paused,
            ts_codes=ts_codes,
            universe_key=universe_key,
        )
    )
    csv_dir = csv_root / snapshot_id / "stock_daily"
    if overwrite_csv and csv_dir.exists():
        shutil.rmtree(csv_dir)
    csv_dir.mkdir(parents=True, exist_ok=True)

    csv_files = 0
    csv_rows = 0
    skipped = 0
    suspended_prev_close_filled_rows = 0
    previous_daily_prev_close_filled_rows = 0
    for code in codes:
        df = _load_daily_raw(code, start, end)
        if df.empty:
            skipped += 1
            continue
        out = _build_daily_expected_frame(
            code,
            df,
            start=start,
            end=end,
            basis_start=basis_start,
            basis_end=basis_end,
            strict_limit=strict_limit,
        )

        required = DAILY_REQUIRED_COLUMNS if strict_limit else ["date", "symbol", "open", "high", "low", "close", "volume", "amount", "factor"]
        _check_required_non_null(out, code, required)
        _write_csv_atomic(out, csv_dir / f"{code}.csv", DAILY_REQUIRED_COLUMNS)
        csv_files += 1
        csv_rows += len(out)
        suspended_prev_close_filled_rows += int(out.attrs.get("suspended_prev_close_filled_rows", 0))
        previous_daily_prev_close_filled_rows += int(out.attrs.get("previous_daily_prev_close_filled_rows", 0))

    summary = CsvExportSummary(
        dataset="stock_daily",
        start=start.isoformat(),
        end=end.isoformat(),
        basis_start=basis_start.isoformat(),
        basis_end=basis_end.isoformat(),
        csv_dir=str(csv_dir),
        csv_files=csv_files,
        csv_rows=csv_rows,
        stocks_requested=len(codes),
        stocks_written=csv_files,
        skipped_no_price_rows=skipped,
        suspended_prev_close_filled_rows=suspended_prev_close_filled_rows,
        previous_daily_prev_close_filled_rows=previous_daily_prev_close_filled_rows,
        strict_limit=strict_limit,
        generated_at=datetime.now().isoformat(timespec="seconds"),
    )
    _finalize_summary(summary, csv_dir / "export_summary.json")
    return summary


def write_bin_meta(
    *,
    bin_dir: Path,
    snapshot_id: str,
    start: date | None,
    end: date,
    exchanges: Sequence[str] | None,
    exclude_st: bool,
    exclude_delisted_or_paused: bool,
    freq_types: Sequence[str],
    last_end_dates: dict[str, str],
    extra: dict[str, Any] | None = None,
) -> None:
    meta = {
        "snapshot_id": snapshot_id,
        "start": start.isoformat() if start else None,
        "end": end.isoformat(),
        "exchanges": _normalize_exchanges(exchanges),
        "exclude_st": bool(exclude_st),
        "exclude_delisted_or_paused": bool(exclude_delisted_or_paused),
        "exclude_bj": True,
        "min_listed_days": IPO_FILTER_DAYS,
        "ipo_filter_mode": "instruments_all_txt",
        "bin_contains_pre_ipo_filter_data": True,
        "stock_universe_min_listed_days_prefilter": False,
        "freq_types": list(freq_types),
        "last_end_dates": last_end_dates,
        "export_mode": "authoritative_aistock_dump_bin",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "required_minute_fields": MINUTE_REQUIRED_BIN_FIELDS,
    }
    if extra:
        meta.update(extra)
    bin_dir.mkdir(parents=True, exist_ok=True)
    (bin_dir / "meta_export.json").write_text(
        json.dumps(meta, ensure_ascii=False, default=str, indent=2),
        encoding="utf-8",
    )


def load_calendar(root: Path, freq: str) -> list[str]:
    path = root / "calendars" / f"{freq}.txt"
    if not path.exists():
        raise FileNotFoundError(path)
    return path.read_text(encoding="utf-8").splitlines()


def read_qlib_bin(path: Path) -> tuple[int, np.ndarray]:
    if not path.exists():
        raise FileNotFoundError(path)
    arr = np.fromfile(path, dtype="<f4")
    if arr.size == 0:
        raise ValueError(f"empty bin file: {path}")
    return int(arr[0]), arr


def _compare_expected_to_bin(
    *,
    code: str,
    expected: pd.DataFrame,
    qlib_dir: Path,
    freq: str,
    calendar_index: dict[str, int],
    fields: Sequence[str],
    abs_tol: float,
    max_errors: int,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    feature_code = str(code).lower()
    feature_root = qlib_dir / "features" / feature_code
    errors: list[dict[str, Any]] = []
    positions_raw = [calendar_index.get(str(value)) for value in expected["date"].tolist()]
    missing_dates = sorted({str(expected.iloc[i]["date"]) for i, pos in enumerate(positions_raw) if pos is None})
    if missing_dates:
        errors.append(
            {
                "ts_code": code,
                "reason": "calendar_timestamp_missing",
                "missing_timestamps": missing_dates[:10],
                "missing_count": len(missing_dates),
            }
        )
        return {"checked_values": 0, "field_max_abs_diff": {}}, errors[:max_errors]

    positions = np.asarray([int(pos) for pos in positions_raw], dtype=np.int64)
    checked_values = 0
    field_max_abs_diff: dict[str, float] = {}

    for field in fields:
        start_idx, arr = read_qlib_bin(feature_root / f"{field}.{freq}.bin")
        arr_pos = positions - int(start_idx) + 1
        if arr_pos.size and (arr_pos.min() < 1 or arr_pos.max() >= len(arr)):
            errors.append(
                {
                    "ts_code": code,
                    "field": field,
                    "reason": "bin_offset_out_of_range",
                    "min_position": int(arr_pos.min()) if arr_pos.size else None,
                    "max_position": int(arr_pos.max()) if arr_pos.size else None,
                    "bin_len": int(len(arr)),
                }
            )
            if len(errors) >= max_errors:
                break
            continue

        qlib_values = arr[arr_pos]
        expected_values = pd.to_numeric(expected[field], errors="coerce").to_numpy(dtype=np.float32)
        finite_expected = np.isfinite(expected_values)
        finite_qlib = np.isfinite(qlib_values)
        missing = finite_expected & ~finite_qlib
        unexpected = ~finite_expected & finite_qlib
        checked_values += int(finite_expected.sum())
        if missing.any() or unexpected.any():
            errors.append(
                {
                    "ts_code": code,
                    "field": field,
                    "reason": "finite_mask_mismatch",
                    "expected_finite": int(finite_expected.sum()),
                    "qlib_finite": int(finite_qlib.sum()),
                    "missing_in_qlib": int(missing.sum()),
                    "unexpected_finite": int(unexpected.sum()),
                    "first_bad_date": str(expected.loc[missing | unexpected, "date"].iloc[0]),
                }
            )
            if len(errors) >= max_errors:
                break

        both = finite_expected & finite_qlib
        max_diff = 0.0
        if both.any():
            diffs = np.abs(qlib_values[both].astype("float64") - expected_values[both].astype("float64"))
            max_diff = float(diffs.max()) if diffs.size else 0.0
            if max_diff > abs_tol:
                worst_idx = int(np.argmax(diffs))
                original_idx = int(np.flatnonzero(both)[worst_idx])
                errors.append(
                    {
                        "ts_code": code,
                        "field": field,
                        "reason": "value_mismatch",
                        "max_abs_diff": max_diff,
                        "abs_tol": abs_tol,
                        "date": str(expected.iloc[original_idx]["date"]),
                        "expected": float(expected_values[original_idx]),
                        "qlib": float(qlib_values[original_idx]),
                    }
                )
                if len(errors) >= max_errors:
                    break
        field_max_abs_diff[field] = max_diff

    return {"checked_values": checked_values, "field_max_abs_diff": field_max_abs_diff}, errors[:max_errors]


def validate_minute_bin_against_db(
    *,
    qlib_dir: Path,
    start: date,
    end: date,
    exchanges: Sequence[str] | None = None,
    exclude_st: bool = True,
    exclude_delisted_or_paused: bool = True,
    ts_codes: Sequence[str] | None = None,
    universe_key: str | None = None,
    fields: Sequence[str] | None = None,
    basis_start: date | None = None,
    basis_end: date | None = None,
    strict_limit: bool = True,
    compare_values: bool = True,
    abs_tol: float = VALUE_COMPARE_ABS_TOL,
    max_errors: int = 50,
) -> dict[str, Any]:
    """Validate direct Qlib 1min bin coverage against DB minute stock-date counts."""

    fields = list(fields or MINUTE_REQUIRED_BIN_FIELDS)
    basis_start = basis_start or start
    basis_end = basis_end or end
    codes = resolve_stock_universe(
        StockUniverseConfig(
            start=start,
            end=end,
            exchanges=exchanges,
            exclude_st=exclude_st,
            exclude_delisted_or_paused=exclude_delisted_or_paused,
            ts_codes=ts_codes,
            universe_key=universe_key,
        )
    )
    if not codes:
        raise RuntimeError("no stock universe for validation")

    calendar = load_calendar(qlib_dir, MINUTE_FREQ_QLIB)
    calendar_index = {value: idx for idx, value in enumerate(calendar)}
    indices_by_date: dict[str, list[int]] = {}
    for idx, value in enumerate(calendar):
        day = value[:10]
        if start.isoformat() <= day <= end.isoformat():
            indices_by_date.setdefault(day, []).append(idx)

    if compare_values:
        errors: list[dict[str, Any]] = []
        checked_stock_dates = 0
        checked_field_values = 0
        db_rows_total = 0
        field_max_abs_diff: dict[str, float] = {field: 0.0 for field in fields}
        for code in codes:
            raw_df = _load_minute_raw(str(code), start, end)
            if raw_df.empty:
                continue
            raw_dates = pd.to_datetime(raw_df["trade_time"]).dt.date.astype(str)
            db_rows_total += int(len(raw_df))
            checked_stock_dates += int(raw_dates.nunique())
            expected = _build_minute_expected_frame(
                str(code),
                raw_df,
                start=start,
                end=end,
                basis_start=basis_start,
                basis_end=basis_end,
                strict_limit=strict_limit,
            )
            required = MINUTE_REQUIRED_COLUMNS if strict_limit else ["date", "symbol", "open", "high", "low", "close", "volume", "amount", "factor"]
            _check_required_non_null(expected, str(code), required)
            stats, compare_errors = _compare_expected_to_bin(
                code=str(code),
                expected=expected,
                qlib_dir=qlib_dir,
                freq=MINUTE_FREQ_QLIB,
                calendar_index=calendar_index,
                fields=fields,
                abs_tol=abs_tol,
                max_errors=max_errors - len(errors),
            )
            checked_field_values += int(stats["checked_values"])
            for field, diff in stats["field_max_abs_diff"].items():
                field_max_abs_diff[field] = max(field_max_abs_diff.get(field, 0.0), float(diff))
            errors.extend(compare_errors)
            if len(errors) >= max_errors:
                break

        return {
            "ok": not errors,
            "qlib_dir": str(qlib_dir),
            "start": start.isoformat(),
            "end": end.isoformat(),
            "stocks_in_universe": len(codes),
            "db_stock_dates": checked_stock_dates,
            "db_rows": db_rows_total,
            "checked_stock_dates": checked_stock_dates,
            "checked_field_values": checked_field_values,
            "fields": fields,
            "compare_values": True,
            "abs_tol": abs_tol,
            "basis_start": basis_start.isoformat(),
            "basis_end": basis_end.isoformat(),
            "field_max_abs_diff": field_max_abs_diff,
            "error_count": len(errors),
            "errors": errors,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
        }

    sql = """
        SELECT m.ts_code,
               (m.trade_time AT TIME ZONE 'Asia/Shanghai')::date AS trade_date,
               count(*)::int AS db_rows
        FROM market.kline_minute_raw m
        JOIN market.stock_basic s ON s.ts_code = m.ts_code
        WHERE m.ts_code = ANY(%(codes)s)
          AND m.freq = %(freq)s
          AND m.trade_time >= %(start_ts)s::timestamptz
          AND m.trade_time < %(end_exclusive)s::timestamptz
          AND (m.trade_time AT TIME ZONE 'Asia/Shanghai')::date >= s.list_date
        GROUP BY m.ts_code, (m.trade_time AT TIME ZONE 'Asia/Shanghai')::date
        ORDER BY m.ts_code, trade_date
    """
    start_ts = f"{start.isoformat()} 00:00:00+08"
    end_exclusive = f"{(end + timedelta(days=1)).isoformat()} 00:00:00+08"
    feature_root = qlib_dir / "features"
    errors: list[dict[str, Any]] = []
    checked_stock_dates = 0
    checked_field_values = 0
    db_stock_dates_total = 0
    db_rows_total = 0
    field_max_abs_diff: dict[str, float] = {field: 0.0 for field in fields}

    for offset in range(0, len(codes), 200):
        batch_codes = codes[offset : offset + 200]
        with get_conn() as conn:
            counts = pd.read_sql(
                sql,
                conn,
                params={"codes": batch_codes, "freq": MINUTE_FREQ_DB, "start_ts": start_ts, "end_exclusive": end_exclusive},
            )
        if counts.empty:
            continue
        counts["trade_date"] = pd.to_datetime(counts["trade_date"]).dt.date.astype(str)
        db_stock_dates_total += int(len(counts))
        db_rows_total += int(counts["db_rows"].sum())

        for code, group in counts.groupby("ts_code", sort=True):
            feature_code = str(code).lower()
            arrays: dict[str, tuple[int, np.ndarray]] = {}
            for field in fields:
                arrays[field] = read_qlib_bin(feature_root / feature_code / f"{field}.{MINUTE_FREQ_QLIB}.bin")
            for row in group.itertuples(index=False):
                day = str(row.trade_date)
                db_rows = int(row.db_rows)
                indices = indices_by_date.get(day)
                if not indices:
                    errors.append({"ts_code": code, "date": day, "reason": "calendar_date_missing", "db_rows": db_rows})
                    if len(errors) >= max_errors:
                        break
                    continue
                checked_stock_dates += 1
                for field, (start_idx, arr) in arrays.items():
                    positions = np.asarray(indices, dtype=np.int64) - int(start_idx) + 1
                    if positions.size and (positions.min() < 1 or positions.max() >= len(arr)):
                        errors.append({"ts_code": code, "date": day, "field": field, "reason": "bin_offset_out_of_range", "db_rows": db_rows})
                        continue
                    values = arr[positions]
                    finite = int(np.isfinite(values).sum())
                    checked_field_values += len(values)
                    if finite != db_rows:
                        errors.append(
                            {
                                "ts_code": code,
                                "date": day,
                                "field": field,
                                "reason": "finite_count_mismatch",
                                "db_rows": db_rows,
                                "qlib_finite": finite,
                                "calendar_rows": len(indices),
                            }
                        )
                        if len(errors) >= max_errors:
                            break
                if len(errors) >= max_errors:
                    break
            if len(errors) >= max_errors:
                break
        if len(errors) >= max_errors:
            break

    if db_stock_dates_total == 0:
        raise RuntimeError("no DB minute rows for validation range")

    return {
        "ok": not errors,
        "qlib_dir": str(qlib_dir),
        "start": start.isoformat(),
        "end": end.isoformat(),
        "stocks_in_universe": len(codes),
        "db_stock_dates": db_stock_dates_total,
        "db_rows": db_rows_total,
        "checked_stock_dates": checked_stock_dates,
        "checked_field_values": checked_field_values,
        "fields": fields,
        "compare_values": compare_values,
        "abs_tol": abs_tol,
        "basis_start": basis_start.isoformat(),
        "basis_end": basis_end.isoformat(),
        "field_max_abs_diff": field_max_abs_diff,
        "error_count": len(errors),
        "errors": errors,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }


def validate_daily_bin_against_db(
    *,
    qlib_dir: Path,
    start: date,
    end: date,
    exchanges: Sequence[str] | None = None,
    exclude_st: bool = True,
    exclude_delisted_or_paused: bool = True,
    ts_codes: Sequence[str] | None = None,
    universe_key: str | None = None,
    fields: Sequence[str] | None = None,
    basis_start: date | None = None,
    basis_end: date | None = None,
    strict_limit: bool = True,
    compare_values: bool = True,
    abs_tol: float = VALUE_COMPARE_ABS_TOL,
    max_errors: int = 50,
) -> dict[str, Any]:
    """Validate direct Qlib day bin coverage and values against DB daily rows."""

    fields = list(fields or MINUTE_REQUIRED_BIN_FIELDS)
    basis_start = basis_start or start
    basis_end = basis_end or end
    codes = resolve_stock_universe(
        StockUniverseConfig(
            start=start,
            end=end,
            exchanges=exchanges,
            exclude_st=exclude_st,
            exclude_delisted_or_paused=exclude_delisted_or_paused,
            ts_codes=ts_codes,
            universe_key=universe_key,
        )
    )
    if not codes:
        raise RuntimeError("no stock universe for validation")

    calendar = load_calendar(qlib_dir, "day")
    calendar_index = {value[:10]: idx for idx, value in enumerate(calendar)}
    feature_root = qlib_dir / "features"
    errors: list[dict[str, Any]] = []
    checked_rows = 0
    checked_values = 0
    field_max_abs_diff: dict[str, float] = {field: 0.0 for field in fields}

    for code in codes:
        raw_df = _load_daily_raw(code, start, end)
        if raw_df.empty:
            continue
        expected = _build_daily_expected_frame(
            code,
            raw_df,
            start=start,
            end=end,
            basis_start=basis_start,
            basis_end=basis_end,
            strict_limit=strict_limit,
        )
        required = DAILY_REQUIRED_COLUMNS if strict_limit else ["date", "symbol", "open", "high", "low", "close", "volume", "amount", "factor"]
        _check_required_non_null(expected, code, required)
        checked_rows += len(expected)

        for field in fields:
            read_qlib_bin(feature_root / code.lower() / f"{field}.day.bin")
        if compare_values:
            stats, compare_errors = _compare_expected_to_bin(
                code=code,
                expected=expected,
                qlib_dir=qlib_dir,
                freq="day",
                calendar_index=calendar_index,
                fields=fields,
                abs_tol=abs_tol,
                max_errors=max_errors - len(errors),
            )
            checked_values += int(stats["checked_values"])
            for field, diff in stats["field_max_abs_diff"].items():
                field_max_abs_diff[field] = max(field_max_abs_diff.get(field, 0.0), float(diff))
            errors.extend(compare_errors)
        if len(errors) >= max_errors:
            break

    return {
        "ok": not errors,
        "qlib_dir": str(qlib_dir),
        "start": start.isoformat(),
        "end": end.isoformat(),
        "stocks_in_universe": len(codes),
        "checked_rows": checked_rows,
        "checked_values": checked_values,
        "fields": fields,
        "compare_values": compare_values,
        "abs_tol": abs_tol,
        "basis_start": basis_start.isoformat(),
        "basis_end": basis_end.isoformat(),
        "field_max_abs_diff": field_max_abs_diff,
        "error_count": len(errors),
        "errors": errors,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }
