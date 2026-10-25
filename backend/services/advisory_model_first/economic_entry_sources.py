"""Advisory-owned read-only batch consumer for economic labels.

Restored price-coordinate parity is not a native historical capture receipt.
Nothing here regenerates Selection candidates, QE predictions or a data release.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import time
from typing import Any

import numpy as np
import pandas as pd

from backend.db.pg_pool import get_conn
from backend.services.advisory_model_first.economic_entry_contracts import POLICY_PRICE_RELATIVE_TOLERANCE
from backend.services.advisory_model_first.economic_entry_labels import KEY, _day, _fail, _frame, _positive
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.realtime_feature_source import _target_raw_price_multiplier
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


@dataclass(frozen=True)
class EconomicDailySnapshot:
    daily: pd.DataFrame
    suspend_rows: pd.DataFrame
    dividends: pd.DataFrame
    calendar: pd.DatetimeIndex
    source_sha256: str
    source_receipt: dict[str, Any]
    raw_suspend_rows: pd.DataFrame | None = None


def _records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    # JSON identity contains actual source values, not a synthetic capture date.
    def scalar(value):
        if value is None or pd.isna(value):
            return None
        if isinstance(value, (datetime, date, pd.Timestamp)):
            return value.isoformat()
        if isinstance(value, Decimal):
            return str(value)  # Preserve DB decimal text without encoder recursion or rounding.
        if isinstance(value, np.generic):
            return scalar(value.item())
        if isinstance(value, (str, bool, int, float)):
            return value
        _fail("economic source contains a non-scalar value")
    return [{key: scalar(value) for key, value in row.items()} for row in frame.to_dict("records")]


def _month_batches(start: date, end: date):
    while start <= end:
        next_month = date(start.year + (start.month == 12), start.month % 12 + 1, 1)
        stop = min(end, next_month - timedelta(days=1))
        yield start, stop
        start = stop + timedelta(days=1)


def _suspension_states(suspend: pd.DataFrame) -> pd.DataFrame:
    states = []
    for (day, symbol), group in suspend.groupby(["trade_date", "instrument"], sort=True):
        stopping = group.loc[group["suspend_type"].eq("S")]
        if stopping.empty:
            continue  # R-only is not a suspension record.
        timing = stopping["suspend_timing"].fillna("").astype(str).str.strip()
        unknown = group["suspend_type"].eq("R").any() or not timing.isin({"", "09:30-09:30"}).all()
        states.append({"trade_date": day, "instrument": symbol,
                       "suspended": not unknown, "tradability_unknown": bool(unknown)})
    return pd.DataFrame(states, columns=["trade_date", "instrument", "suspended", "tradability_unknown"])


class EconomicEntryReadonlyDailySource:
    def __init__(self, *, connection_context_factory: Callable[[], Any] | None = None):
        self._connection = connection_context_factory or (lambda: get_conn(autocommit=False, manage_transaction=False))

    def load(
        self, *, symbols: Sequence[str], start_date: date, end_date: date, maximum_rows: int
    ) -> EconomicDailySnapshot:
        unique = sorted(set(symbols))
        if not unique or len(unique) != len(symbols) or start_date > end_date or maximum_rows <= 0:
            _fail("economic DB read has invalid roster/window/budget")
        with self._connection() as conn:
            cursor = conn.cursor()
            query_receipts = []

            def read_frame(phase: str, sql: str, parameters: tuple) -> pd.DataFrame:
                began = time.monotonic()
                try:
                    cursor.execute(sql, parameters)
                    frame = pd.DataFrame(cursor.fetchall(), columns=[column.name for column in cursor.description])
                except Exception as exc:
                    raise AdvisoryModelFirstError(
                        "economic read-only source query failed",
                        reason_code="ADVISORY_ECONOMIC_SOURCE_QUERY_FAILED",
                        context={
                            "phase": phase,
                            "error_type": type(exc).__name__,
                            "completed_queries": query_receipts,
                            "statement_timeout_ms": 30000,
                        },
                    ) from exc
                query_receipts.append(
                    {"phase": phase, "rows": len(frame), "seconds": round(time.monotonic() - began, 6)}
                )
                return frame

            try:
                conn.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
                cursor.execute("SET LOCAL statement_timeout = %s", (30000,))
                parts, row_count = [], 0
                # Calendar-month batching is one consistent snapshot, not per-day tasks.
                # Explicit bounds on joined hypertables enable static chunk pruning.
                for batch_start, batch_end in _month_batches(start_date, end_date):
                    part = read_frame(
                        f"daily:{batch_start.isoformat()}:{batch_end.isoformat()}",
                        """SELECT p.trade_date, p.ts_code AS instrument,
                              p.open_li, p.high_li, p.low_li, p.close_li,
                              a.adj_factor, l.pre_close, l.up_limit, l.down_limit
                       FROM market.kline_daily_raw p
                       LEFT JOIN market.adj_factor a ON a.trade_date=p.trade_date AND a.ts_code=p.ts_code
                           AND a.trade_date BETWEEN %s AND %s
                       LEFT JOIN market.stk_limit l ON l.trade_date=p.trade_date AND l.ts_code=p.ts_code
                           AND l.trade_date BETWEEN %s AND %s
                       WHERE p.ts_code=ANY(%s) AND p.trade_date BETWEEN %s AND %s
                       ORDER BY p.trade_date,p.ts_code LIMIT %s""",
                        (
                            batch_start,
                            batch_end,
                            batch_start,
                            batch_end,
                            unique,
                            batch_start,
                            batch_end,
                            maximum_rows - row_count + 1,
                        ),
                    )
                    row_count += len(part)
                    if row_count > maximum_rows:
                        _fail("economic DB market read exceeds registered row budget")
                    parts.append(part)
                daily = pd.concat(parts, ignore_index=True)
                suspend = read_frame(
                    "suspend",
                    "SELECT trade_date,ts_code AS instrument,suspend_type,suspend_timing FROM market.suspend_d WHERE ts_code=ANY(%s) AND trade_date BETWEEN %s AND %s ORDER BY trade_date,ts_code,suspend_type,suspend_timing NULLS FIRST LIMIT %s",
                    (unique, start_date, end_date, maximum_rows + 1),
                )
                calendar_frame = read_frame(
                    "calendar",
                    "SELECT cal_date FROM market.trading_calendar WHERE is_trading=TRUE AND cal_date BETWEEN %s AND %s ORDER BY cal_date",
                    (start_date, end_date),
                )
                calendar = pd.DatetimeIndex(calendar_frame["cal_date"])
                dividends = read_frame(
                    "dividend",
                    """SELECT ts_code AS instrument,end_date,ann_date,div_proc,stk_div,stk_bo_rate,stk_co_rate,
                              cash_div,cash_div_tax,imp_ann_date,ex_date
                       FROM market.dividend WHERE ts_code=ANY(%s) AND ex_date BETWEEN %s AND %s AND div_proc='实施'
                       ORDER BY ts_code,ex_date,imp_ann_date DESC NULLS LAST,end_date DESC,ann_date DESC LIMIT %s""",
                    (unique, start_date, end_date, maximum_rows + 1),
                )
            finally:
                conn.rollback()
                cursor.close()
        daily = _frame(
            daily, ["trade_date", "instrument"], {"trade_date", "instrument", "open_li", "close_li", "adj_factor"}
        )
        if daily.empty or calendar.empty or not calendar.is_unique or not calendar.is_monotonic_increasing:
            _fail("economic DB daily/calendar source is unavailable")
        if not set(daily["instrument"]).issubset(unique):
            _fail("economic DB snapshot contains foreign symbols")
        if not daily["trade_date"].between(pd.Timestamp(start_date), pd.Timestamp(end_date)).all():
            _fail("economic DB source exceeds the authorized development window")
        if calendar.min() < pd.Timestamp(start_date) or calendar.max() > pd.Timestamp(end_date):
            _fail("economic DB calendar exceeds the authorized development window")
        for frame in (suspend, dividends):
            if len(frame) > maximum_rows or not set(frame["instrument"]).issubset(unique):
                _fail("economic auxiliary source exceeds its row budget or symbol roster")
        for column in ("open_li", "high_li", "low_li", "close_li"):
            daily[f"raw_{column[:-3]}_cny"] = pd.to_numeric(daily[column], errors="coerce") / 1000.0
        if not suspend.empty:
            suspend["trade_date"] = suspend["trade_date"].map(_day)
            if not suspend["suspend_type"].isin(["S", "R"]).all():
                _fail("economic suspension source contains unknown state")
        suspended = _suspension_states(suspend)
        source_content = {
            "source_policy": "readonly_db_daily_snapshot_v1",
            "unit_divisor": 1000,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "symbols": unique,
            "daily": _records(daily),
            "suspend": _records(suspend),
            "dividends": _records(dividends),
            "calendar": [day.date().isoformat() for day in calendar],
        }
        source_sha = canonical_json_sha256(source_content)
        receipt = {key: value for key, value in source_content.items() if key not in {"daily", "suspend", "dividends"}}
        receipt.update(
            {
                "source_sha256": source_sha,
                "daily_rows": len(daily),
                "suspend_rows": len(suspend),
                "ambiguous_tradability_rows": int(suspended["tradability_unknown"].sum()),
                "dividend_rows": len(dividends),
                "read_at": datetime.now(timezone.utc).isoformat(),
                "readonly": True,
                "isolation": "REPEATABLE READ",
                "statement_timeout_ms": 30000,
                "query_count": len(query_receipts) + 1,
                "queries": query_receipts,
                "batching": "calendar_month_single_snapshot",
                "database_written": False,
                "native_capture": False,
            }
        )
        return EconomicDailySnapshot(daily, suspended, dividends, calendar, source_sha, receipt, suspend)


def project_economic_price_inputs(
    *,
    snapshot: EconomicDailySnapshot,
    candidates: pd.DataFrame,
    episodes: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, str, dict[str, Any]]:
    """Recover one constant per symbol and verify every observable old endpoint.

    O(market rows + episode endpoints); exact one-to-one date/symbol joins only.
    Coefficients only translate units. They cannot be fitted to improve returns.
    """
    daily = snapshot.daily.copy()
    endpoints = []
    for date_column, price_column in (("entry_trade_date", "entry_price"), ("effective_exit_date", "exit_price")):
        part = episodes.loc[episodes["label_status"].eq("MATURED"), ["instrument", date_column, price_column]].copy()
        part = part.rename(columns={date_column: "trade_date", price_column: "policy_price"})
        part["trade_date"] = part["trade_date"].map(_day)
        endpoints.append(part)
    points = pd.concat(endpoints, ignore_index=True).merge(
        daily.loc[:, ["trade_date", "instrument", "raw_open_cny", "adj_factor"]],
        on=["trade_date", "instrument"],
        how="left",
        validate="many_to_one",
    )
    for name in ("policy_price", "raw_open_cny", "adj_factor"):
        points[name] = pd.to_numeric(points[name], errors="coerce")
    known = points.loc[
        np.isfinite(points[["policy_price", "raw_open_cny", "adj_factor"]]).all(axis=1)
        & points[["policy_price", "raw_open_cny", "adj_factor"]].gt(0).all(axis=1)
    ].copy()
    known["coefficient"] = known["policy_price"] / (known["raw_open_cny"] * known["adj_factor"])
    scales = known.groupby("instrument", sort=True)["coefficient"].first().to_dict()
    known["projected"] = known["raw_open_cny"] * known["adj_factor"] * known["instrument"].map(scales)
    parity = np.isclose(known["projected"], known["policy_price"], rtol=POLICY_PRICE_RELATIVE_TOLERANCE, atol=1e-8)
    if not parity.all():
        _fail(
            "DB adjustment coordinate cannot reproduce every frozen episode endpoint", "ADVISORY_ECONOMIC_PRICE_PARITY"
        )
    coordinate = {
        "schema_version": "economic_policy_price_coordinate_v1",
        "method": "constant_per_symbol_times_db_adj_factor_all_frozen_endpoints",
        "coefficients": scales,
        "source_sha256": snapshot.source_sha256,
    }
    coordinate_sha = canonical_json_sha256(coordinate)
    daily["policy_price_per_raw_cny"] = pd.to_numeric(daily["adj_factor"], errors="coerce") * daily["instrument"].map(
        scales
    )
    states = snapshot.suspend_rows.copy()
    if "suspended" not in states:
        states["suspended"] = True
    if "tradability_unknown" not in states:
        states["tradability_unknown"] = False
    suspended_set = set(map(tuple, states.loc[states["suspended"], ["trade_date", "instrument"]].to_numpy()))
    unknown_set = set(map(tuple, states.loc[states["tradability_unknown"], ["trade_date", "instrument"]].to_numpy()))
    daily["suspended"] = [
        (day, symbol) in suspended_set for day, symbol in zip(daily["trade_date"], daily["instrument"], strict=True)
    ]
    daily["tradability_unknown"] = [(day, symbol) in unknown_set for day, symbol in zip(daily["trade_date"], daily["instrument"], strict=True)]
    missing_suspended = states.merge(
        daily[["trade_date", "instrument"]], on=["trade_date", "instrument"], how="left", indicator=True
    )
    missing_suspended = missing_suspended.loc[
        missing_suspended["_merge"].eq("left_only"), ["trade_date", "instrument", "suspended", "tradability_unknown"]
    ].copy()
    if not missing_suspended.empty:
        daily = pd.concat([daily, missing_suspended], ignore_index=True)
    daily["price_coordinate_sha256"] = coordinate_sha
    daily["source_sha256"] = snapshot.source_sha256
    price_map = daily.set_index(["trade_date", "instrument"]).to_dict("index")
    dividend_map: dict[tuple[date, str], list[tuple[Any, ...]]] = {}
    for row in snapshot.dividends.itertuples(index=False, name=None):
        dividend_map.setdefault((_day(row[-1]).date(), row[0]), []).append(row[:-1])
    reference_rows, reference_unavailable = [], []
    for row in candidates.to_dict("records"):
        decision, target, symbol = (_day(row[KEY[0]]), _day(row[KEY[1]]), row["instrument"])
        observed = price_map.get((decision, symbol))
        close = _positive(observed.get("raw_close_cny")) if observed is not None else None
        if close is None:
            reference_unavailable.append(
                {"decision": decision.date().isoformat(), "instrument": symbol, "reason": "DECISION_RAW_CLOSE_MISSING"}
            )
            continue
        try:
            multiplier, action_source = _target_raw_price_multiplier(
                symbol=symbol,
                decision_raw_close=close,
                decision_adjustment_factor=observed["adj_factor"],
                rows=dividend_map.get((target.date(), symbol), []),
                decision_as_of_trade_date=decision.date(),
            )
        except AdvisoryModelFirstError as exc:
            reference_unavailable.append(
                {"decision": decision.date().isoformat(), "instrument": symbol, "reason": exc.reason_code}
            )
            continue
        reference_rows.append(
            {
                **{name: row[name] for name in KEY},
                "decision_raw_close_cny": close,
                "target_reference_raw_cny": close * multiplier,
                "reference_visible_through": decision,
                "source_sha256": snapshot.source_sha256,
                "corporate_action_source": action_source,
            }
        )
    reference_columns = KEY + [
        "decision_raw_close_cny",
        "target_reference_raw_cny",
        "reference_visible_through",
        "source_sha256",
        "corporate_action_source",
    ]
    references = pd.DataFrame(reference_rows, columns=reference_columns)
    receipt = {
        "coordinate": coordinate,
        "coordinate_sha256": coordinate_sha,
        "endpoint_count": len(points),
        "verified_endpoint_count": len(known),
        "unavailable_endpoint_count": len(points) - len(known),
        "native_capture": False,
        "reference_unavailable": reference_unavailable,
        "relative_tolerance": POLICY_PRICE_RELATIVE_TOLERANCE,
        "maximum_relative_error": float(np.max(np.abs(known["projected"] / known["policy_price"] - 1)))
        if len(known)
        else None,
    }
    return daily.sort_values(["trade_date", "instrument"]), references, coordinate_sha, receipt
