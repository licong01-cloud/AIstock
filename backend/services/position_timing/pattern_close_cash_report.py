"""PIT equal-account daily-return research indices, never a pooled account."""
from __future__ import annotations

from collections import Counter
from typing import Any, Iterable

import numpy as np
import pandas as pd

from .pattern_universe_benchmark import CandidatePoolMemberships, POOL_IDS

INDEX_CODES = {"stock_universe": "000300.SH", "csi300": "000300.SH", "csi500": "000905.SH",
               "csi1000": "000852.SH", "star50": "000688.SH", "star100": "000698.SH"}


def performance(returns: np.ndarray) -> dict[str, Any]:
    values = np.asarray(returns, dtype=float)
    if not len(values) or not np.isfinite(values).all():
        return {"status": "UNAVAILABLE", "sessions": len(values), "total_return": None,
                "annualized_return": None, "max_drawdown": None}
    curve = np.r_[1., np.cumprod(1 + values)]
    return {"status": "COMPLETE", "sessions": len(values), "total_return": float(curve[-1] - 1),
            "annualized_return": float(curve[-1] ** (252 / len(values)) - 1),
            "max_drawdown": float(np.min(curve / np.maximum.accumulate(curve) - 1))}


def comparison(timing: np.ndarray, hold: np.ndarray, index: np.ndarray) -> dict[str, Any]:
    result = {"timing": performance(timing), "hold": performance(hold), "index": performance(index)}
    for left, right in (("timing", "hold"), ("timing", "index"), ("hold", "index")):
        a, b = result[left]["total_return"], result[right]["total_return"]
        result[f"{left}_minus_{right}"] = a - b if a is not None and b is not None else None
    return result


def index_returns(frame: pd.DataFrame, calendar: pd.DatetimeIndex) -> dict[str, np.ndarray]:
    results = {}
    for code in set(INDEX_CODES.values()):
        selected = frame.loc[frame.ts_code.eq(code)].copy()
        selected["trade_date"] = pd.to_datetime(selected.trade_date)
        if selected.trade_date.duplicated().any():
            raise ValueError(f"duplicate index date: {code}")
        price = pd.to_numeric(selected.set_index("trade_date").close, errors="coerce").reindex(calendar)
        price = price.where(np.isfinite(price) & price.gt(0))
        results[code] = price.pct_change(fill_method=None).to_numpy()
    return results


def build_report(
    chunks: Iterable[tuple[pd.DataFrame, list[dict[str, Any]]]], *,
    memberships: CandidatePoolMemberships, calendar: pd.DatetimeIndex, index_frame: pd.DataFrame,
) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    n = len(calendar)
    indexes = index_returns(index_frame, calendar)
    years = sorted(set(calendar.year))
    year_start = {year: int(np.flatnonzero(calendar.year == year)[0]) for year in years}
    keys = [(pool, "dynamic", 0) for pool in POOL_IDS] + [(pool, "annual_fixed", y) for pool in POOL_IDS for y in years]
    totals = {key: np.zeros((n, 5)) for key in keys}  # timing sum, hold sum, expected, paired, unknown
    stock_rows, diagnostics, counters = [], [], Counter()
    for frame, details in chunks:
        diagnostics.extend(details)
        for detail in details:
            counters.update(detail.get("counts", {}))
        for symbol, data in (frame.groupby("symbol", sort=False) if not frame.empty else []):
            data = data.sort_values("ordinal")
            ordinals = data.ordinal.to_numpy(int)
            start = int(ordinals[0])
            active = np.zeros(n, dtype=bool)
            for row in memberships.intervals["stock_universe"].query("symbol == @symbol").itertuples():
                active |= (calendar >= row.start) & (calendar <= row.end)
            full = np.full((n, 2), np.nan)
            # Missing rows and valuation unknowns remain missing, never pct_change forward filled.
            full[ordinals, 0] = data.timing_nav.to_numpy(float)
            full[ordinals, 1] = data.hold_nav.to_numpy(float)
            returns = np.full_like(full, np.nan)
            returns[1:] = full[1:] / full[:-1] - 1
            valid = np.isfinite(returns).all(axis=1)
            result = comparison(returns[start+1:, 0], returns[start+1:, 1], indexes["000300.SH"][start+1:])
            exposure = data.timing_exposure.to_numpy(float)
            invested = np.isfinite(exposure) & (exposure > 0)
            stock_rows.append({"symbol": symbol, "start": str(calendar[start].date()),
                               "end": str(calendar[-1].date()), "benchmark": "000300.SH",
                               "paired_unknown_sessions": int((~valid[start+1:]).sum()),
                               "invested_session_fraction": float(invested.mean()),
                               "conditional_exposure": float(exposure[invested].mean()) if invested.any() else 0.,
                               "average_exposure": float(np.nanmean(exposure)),
                               "stale_mark_sessions": int(data.stale_mark.sum()),
                               **pd.json_normalize(result, sep="_").iloc[0].to_dict()})
            for pool in POOL_IDS:
                membership = memberships.effective_mask(pool_id=pool, symbol=symbol, calendar=calendar, stock_pit_mask=active)
                mask = np.r_[False, membership[:-1]]
                # Population denominators include not-yet-feature-ready members.
                scopes = [("dynamic", 0, mask)]
                for year in years:
                    anchor = max(0, year_start[year] - 1)
                    fixed = (calendar.year == year) & bool(membership[anchor])
                    scopes.append(("annual_fixed", year, np.asarray(fixed)))
                for mode, year, expected in scopes:
                    paired = expected & valid
                    a = totals[(pool, mode, year)]
                    a[:, 0] += np.where(paired, returns[:, 0], 0.)
                    a[:, 1] += np.where(paired, returns[:, 1], 0.)
                    a[:, 2] += expected
                    a[:, 3] += paired
                    a[:, 4] += expected & ~valid
    # Stocks with no feature-ready sessions have no path but still belong to PIT denominators.
    missing_symbols = [d["symbol"] for d in diagnostics if d["status"] != "REPLAYED"]
    for symbol in missing_symbols:
        stock_rows.append({"symbol": symbol, "status": "NO_FEATURE_READY_PIT_SESSION", "benchmark": "000300.SH"})
        for pool in POOL_IDS:
            membership = memberships.effective_mask(pool_id=pool, symbol=symbol, calendar=calendar, stock_pit_mask=np.ones(n, bool))
            scopes = [("dynamic", 0, np.r_[False, membership[:-1]])]
            scopes += [("annual_fixed", y, (calendar.year == y) & membership[max(0, year_start[y]-1)]) for y in years]
            for mode, year, mask in scopes:
                totals[(pool, mode, year)][:, 2] += mask
                totals[(pool, mode, year)][:, 4] += mask
    daily_rows, pool_results = [], []
    first_evaluation = min((int(d["start_ordinal"]) + 1 for d in diagnostics if d["status"] == "REPLAYED"), default=n)
    for (pool, mode, year), a in totals.items():
        eligible = (a[:, 2] > 0) & (np.arange(n) >= first_evaluation)
        selected = np.flatnonzero(eligible)
        if not len(selected):
            continue
        count = a[:, 3]
        t = np.divide(a[:, 0], count, out=np.full(n, np.nan), where=count > 0)
        h = np.divide(a[:, 1], count, out=np.full(n, np.nan), where=count > 0)
        complete = (a[:, 4] == 0) & (count > 0)
        idx = indexes[INDEX_CODES[pool]]
        official = comparison(np.where(complete, t, np.nan)[eligible], np.where(complete, h, np.nan)[eligible], idx[eligible])
        conditional = comparison(t[eligible], h[eligible], idx[eligible])
        observed_index = np.flatnonzero(eligible & np.isfinite(idx))
        # Common coverage is a date interval, not a way to drop missing index
        # returns in the middle of the interval and pretend they were zero.
        common = eligible & (np.arange(n) >= observed_index[0]) if len(observed_index) else np.zeros(n, bool)
        periods = {str(y): comparison(t[eligible & (calendar.year == y)], h[eligible & (calendar.year == y)], idx[eligible & (calendar.year == y)]) for y in years if (eligible & (calendar.year == y)).any()}
        pool_results.append({"pool": pool, "mode": mode, "year": year or None,
                             "index_code": INDEX_CODES[pool], "first_date": str(calendar[selected[0]].date()),
                             "last_date": str(calendar[selected[-1]].date()),
                             "population_unique_symbols": int(memberships.intervals[pool].symbol.nunique()),
                             "incomplete_sessions": int((eligible & ~complete).sum()),
                             "missing_index_sessions": int((eligible & ~np.isfinite(idx)).sum()),
                             "full_population": official, "paired_observed_only_not_full_population": conditional,
                             "annual_paired_observed_diagnostic": periods,
                             "index_common_observed_dates_diagnostic": comparison(t[common], h[common], idx[common])})
        for ordinal in selected:
            daily_rows.append({"pool": pool, "mode": mode, "year": year, "date": str(calendar[ordinal].date()),
                               "timing_return_observed": t[ordinal], "hold_return_observed": h[ordinal],
                               "index_return": idx[ordinal], "expected_accounts": int(a[ordinal, 2]),
                               "paired_accounts": int(count[ordinal]), "unknown_accounts": int(a[ordinal, 4])})
    stock_frame = pd.DataFrame(stock_rows)
    delta = pd.to_numeric(stock_frame.get("timing_minus_hold", pd.Series(dtype=float)), errors="coerce").dropna()
    distribution = {"paired_complete_symbols": len(delta), "win_fraction": float((delta > 0).mean()) if len(delta) else None,
                    "mean": float(delta.mean()) if len(delta) else None, "quantiles": {str(q): float(delta.quantile(q)) for q in (.05, .25, .5, .75, .95)} if len(delta) else {}}
    return {"schema_version": "position_timing_close_cash_report_v1", "result_class": "EXPLORATORY",
            "pools": pool_results, "timing_minus_hold_distribution": distribution,
            "fill_counters": dict(counters), "symbol_diagnostics": diagnostics,
            "index_basis": "OFFICIAL_PRICE_INDEX_NOT_TOTAL_RETURN", "shared_cash_portfolio": False}, stock_frame, pd.DataFrame(daily_rows)
