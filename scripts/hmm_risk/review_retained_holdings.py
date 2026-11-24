"""Select one industry for manual review per account day from decision-close data.

All input files are Parquet. Days give the original number of old holdings and
next-session target availability. Stocks give one row per old security, with
its original value and the portion expected to remain. Warnings are optional.
Output is CSV. Amounts and net asset value must use the same currency.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd


DAY = ["account", "date"]
DAYS = [*DAY, "effective", "nav", "has_next_target", "old_stock_count"]
STOCKS = [*DAY, "stock", "code", "old_value", "retained_value"]
PRED = ["date", "effective", "code", "M0", "M1", "p0", "p1"]
RESULT = [
    *DAY, "effective", "status", "nav", "code", "old_value", "retained_value",
    "nav_weight_pct", "stocks", "M0", "M1", "p0", "p1", "warning_status",
]


def checked(frame, columns, key):
    data = frame.loc[:, columns].copy()
    for col in ("date", "effective"):
        if col in data:
            data[col] = pd.to_datetime(data[col], errors="raise")
    for col in key:
        if data[col].isna().any() or data[col].astype(str).str.strip().eq("").any():
            raise ValueError(f"missing key in {col}")
    if data.duplicated(key).any():
        raise ValueError(f"duplicate rows for {key}")
    return data


def review(days, stocks, warnings=None):
    """Pick the largest expected retained amount; break ties by industry code."""
    days = checked(days, DAYS, DAY)
    stocks = checked(stocks, STOCKS, [*DAY, "stock"])
    if days.nav.isna().any() or not np.isfinite(days.nav).all() or days.nav.le(0).any():
        raise ValueError("NAV must be finite and positive")
    if not pd.api.types.is_bool_dtype(days.has_next_target) or days.has_next_target.isna().any():
        raise ValueError("has_next_target must be a known boolean")
    if not pd.api.types.is_integer_dtype(days.old_stock_count) or days.old_stock_count.isna().any() or days.old_stock_count.lt(0).any():
        raise ValueError("old_stock_count must be a nonnegative integer")
    if (days.has_next_target & ~(days.effective > days.date)).any():
        raise ValueError("known target must have a later effective date")
    amounts = stocks[["retained_value", "old_value"]]
    if amounts.isna().any().any() or not np.isfinite(amounts).all().all():
        raise ValueError("holding values must be finite")
    if (stocks.old_value.le(0) | stocks.retained_value.lt(0) | stocks.retained_value.gt(stocks.old_value)).any():
        raise ValueError("invalid original or retained value")

    by_day = {key: rows for key, rows in stocks.groupby(DAY, sort=False)}
    if by_day.keys() - set(days[DAY].itertuples(index=False, name=None)):
        raise ValueError("stocks refer to an unknown account day")
    predictions = {}
    if warnings is not None:
        w = checked(warnings, PRED, ["date", "code"])
        for flag, probability in (("M0", "p0"), ("M1", "p1")):
            values = pd.to_numeric(w[probability], errors="raise")
            if not w[flag].dropna().map(lambda v: isinstance(v, (bool, np.bool_))).all():
                raise ValueError(f"invalid {flag} flag")
            if not values.dropna().between(0, 1).all() or not values.isna().equals(w[flag].isna()):
                raise ValueError(f"invalid or incomplete {flag} probability/flag pair")
            w[probability] = values
        if not w.effective.gt(w.date).all():
            raise ValueError("warning must apply after the decision")
        predictions = {(row.date, row.code): row for row in w.itertuples()}

    output = []
    for day in days.sort_values(DAY).itertuples():
        result = dict(account=day.account, date=day.date, effective=day.effective,
                      nav=day.nav, warning_status="NO_SELECTION")
        book = by_day.get((day.account, day.date), stocks.iloc[:0])
        if not day.has_next_target:
            result["status"] = "NO_NEXT_TARGET"
        elif len(book) != day.old_stock_count:
            result["status"] = "INCOMPLETE_HOLDINGS"
        elif book.code.isna().any() or book.code.astype(str).str.strip().eq("").any():
            result["status"] = "UNKNOWN_INDUSTRY"
        elif not book.retained_value.gt(0).any():
            result["status"] = "NO_RETAINED_OLD_HOLDING"
        else:
            totals = book.groupby("code", sort=True).retained_value.sum()
            code = totals.idxmax()
            members = book[book.code.eq(code)]
            retained = float(totals.at[code])
            result.update(status="READY", code=code, old_value=members.old_value.sum(),
                          retained_value=retained, nav_weight_pct=100 * retained / day.nav,
                          stocks="|".join(sorted(members.loc[members.retained_value.gt(0), "stock"])),
                          warning_status="UNAVAILABLE")
            prediction = predictions.get((day.date, code))
            if prediction is not None:
                if prediction.effective != day.effective:
                    raise ValueError(f"warning effective date mismatch on {day.date} for {code}")
                present = int(pd.notna(prediction.p0)) + int(pd.notna(prediction.p1))
                result.update(M0=prediction.M0, M1=prediction.M1, p0=prediction.p0,
                              p1=prediction.p1, warning_status=("UNAVAILABLE", "PARTIAL", "AVAILABLE")[present])
        output.append(result)
    return pd.DataFrame(output, columns=RESULT)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=Path, required=True)
    parser.add_argument("--stocks", type=Path, required=True)
    parser.add_argument("--warnings", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    sheet = review(pd.read_parquet(args.days, columns=DAYS),
                   pd.read_parquet(args.stocks, columns=STOCKS),
                   pd.read_parquet(args.warnings, columns=PRED) if args.warnings else None)
    sheet.to_csv(args.output or sys.stdout.buffer, index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()
