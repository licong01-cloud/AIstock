"""Review positive target gaps in industries with retained old holdings, using decision-close inputs."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

if __package__:
    from . import review_retained_holdings as holdings
else:
    import review_retained_holdings as holdings


DAY = holdings.DAY
DAYS = [*holdings.DAYS, "target_count"]
TARGETS = [*DAY, "effective", "stock", "code", "target_weight", "held_value"]
COLUMNS = [
    *DAY, "effective", "focus", "status", "nav", "code", "retained_value",
    "planned_addition", "new_position_gap", "existing_position_gap", "addition_nav_pct",
    "positive_targets", "first_stock", "first_gap", "M0", "M1", "p0", "p1", "warning_status",
]


def planned_reviews(days, stocks, targets, warnings=None):
    """Return two independent one-slot choices per day. Only positive gaps count towards a purchase."""
    base = holdings.review(days, stocks)
    days = holdings.checked(days, DAYS, DAY)
    stocks = holdings.checked(stocks, holdings.STOCKS, [*DAY, "stock"])
    targets = holdings.checked(targets, TARGETS, [*DAY, "stock"])
    predictions = holdings.warning_map(warnings)
    counts = days.loc[days.has_next_target, "target_count"]
    if counts.isna().any() or not np.isfinite(counts).all() or (counts.lt(0) | counts.mod(1).ne(0)).any():
        raise ValueError("available target lists must declare a nonnegative integer count")
    if targets.target_weight.isna().any() or not np.isfinite(targets.target_weight).all() or targets.target_weight.lt(0).any():
        raise ValueError("target weights must be finite and nonnegative")
    values = targets.held_value.dropna()
    if not np.isfinite(values).all() or values.lt(0).any():
        raise ValueError("known holding values must be finite and nonnegative")
    targets = targets.merge(days[[*DAY, "nav", "effective", "has_next_target"]], on=DAY,
                            how="left", suffixes=("", "_calendar"), validate="many_to_one")
    if (targets.nav.isna() | ~targets.has_next_target.eq(True) | targets.effective.isna()
            | targets.effective.ne(targets.effective_calendar)).any():
        raise ValueError("target lacks a matching decision day and effective date")
    shared = targets.merge(stocks[[*DAY, "stock", "code", "old_value"]], on=[*DAY, "stock"],
                           suffixes=("", "_old"), validate="one_to_one")
    if ((shared.old_value - shared.held_value > .005) |
            (shared.code.notna() & shared.code_old.notna() & shared.code.ne(shared.code_old))).any():
        raise ValueError("target holding or industry conflicts with the old stock inventory")
    gap = (targets.target_weight * targets.nav - targets.held_value).clip(lower=0)
    targets["gap_cents"] = gap.mul(100).round().astype("Int64")
    books = {k: g for k, g in targets.groupby(DAY)}
    retained = stocks.groupby([*DAY, "code"]).retained_value.sum()
    old_sectors = {k: g.droplevel(DAY) for k, g in retained[retained.gt(0)].groupby(level=DAY)}
    calendar = days.set_index(DAY)
    rows = []
    for day in base.itertuples():
        key = day.account, day.date
        table = books.get(key, targets.iloc[:0])
        status = day.status
        if status == "READY":
            if len(table) != calendar.at[key, "target_count"]:
                status = "INCOMPLETE_TARGETS"
            elif table.held_value.isna().any():
                status = "UNKNOWN_TARGET_HOLDINGS"
            elif ((table.code.isna() | table.code.astype(str).str.strip().eq("")) & table.gap_cents.gt(0)).any():
                status = "UNKNOWN_TARGET_INDUSTRY"
        positive = table[table.gap_cents.gt(0)].sort_values(["gap_cents", "stock"], ascending=[False, True])
        for focus in ("retained", "incoming"):
            row = dict(account=day.account, date=day.date, effective=day.effective, nav=day.nav,
                       focus=focus, status=status, warning_status="NO_SELECTION")
            rows.append(row)
            if status != "READY":
                continue
            exposure = old_sectors[key]
            code = day.code
            if focus == "incoming":
                sums = positive.groupby("code").gap_cents.sum().reindex(exposure.index, fill_value=0)
                ranked = pd.DataFrame({"gap": sums, "retained": exposure}).reset_index()
                ranked = ranked.sort_values(["gap", "retained", "code"], ascending=[False, False, True])
                if ranked.iloc[0].gap <= 0:
                    row["status"] = "NO_PLANNED_ADDITION"
                    row.update(planned_addition=0., new_position_gap=0., existing_position_gap=0.,
                               addition_nav_pct=0., positive_targets=0)
                    continue
                code = ranked.iloc[0].code
            picks = positive[positive.code.eq(code)]
            gap = picks.gap_cents.sum() / 100
            row.update(code=code, retained_value=exposure.at[code], planned_addition=gap,
                       new_position_gap=picks.loc[picks.held_value.eq(0), "gap_cents"].sum() / 100,
                       existing_position_gap=picks.loc[picks.held_value.gt(0), "gap_cents"].sum() / 100,
                       addition_nav_pct=100 * gap / day.nav, positive_targets=len(picks))
            if len(picks):
                row.update(first_stock=picks.iloc[0].stock, first_gap=picks.iloc[0].gap_cents / 100)
            row.update(holdings.warning_fields(predictions, day.date, day.effective, code))
    return pd.DataFrame(rows, columns=COLUMNS)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("days", "stocks", "targets"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    parser.add_argument("--warnings", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = planned_reviews(pd.read_parquet(args.days, columns=DAYS),
                             pd.read_parquet(args.stocks, columns=holdings.STOCKS),
                             pd.read_parquet(args.targets, columns=TARGETS),
                             pd.read_parquet(args.warnings, columns=holdings.PRED) if args.warnings else None)
    result.to_csv(args.output or sys.stdout.buffer, index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()
