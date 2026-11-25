"""Audit marginal cash competition between simultaneous archived buy fills (integer CNY cents)."""
from __future__ import annotations

import argparse
from numbers import Integral
from pathlib import Path

import pandas as pd

from backend.execution_algos.board_lot import board_lot_rule

CLOCK = ["account", "tick"]
TICKETS = [*CLOCK, "date", "stock", "code", "sequence", "filled", "attempted", "exact_attempt",
           "cash_limited", "remaining_before", "price_cents", "prior_notional_cents", "cash_end_cents", "rank"]
SCORES = ["date", "effective", "code", "score"]
PAIR = [*CLOCK, "date", "funded", "limited", "funded_code", "limited_code", "original_funded_shares",
        "original_limited_shares", "status",
        "funded_shares", "limited_shares", "saved_cents", "extra_cents", "budget_cents",
        "whole_children_exclusive", "whole_status", "whole_budget_cents", "whole_funded_shares",
        "whole_limited_shares", "first_daily", "whole_first_daily", "hmm_choice", "rank_choice"]


def buy_fee(cents):
    """Original LocalSIM buy fee, 0.000095 on cumulative order notional, minimum CNY 5."""
    return max(500, (cents * 95 + 500_000) // 1_000_000) if cents else 0


def fill_cost(row, shares):
    notional = shares * int(row.price_cents)
    prior = int(row.prior_notional_cents)
    return notional + buy_fee(prior + notional) - buy_fee(prior)


def legal(quantity, stock):
    minimum, increment = board_lot_rule(stock)
    return quantity == 0 or (quantity >= minimum and quantity % increment == 0)


def affordable(row, cash, ceiling):
    minimum, step = board_lot_rule(row.stock)
    low, high = 0, ceiling // step
    while low < high:
        mid = (low + high + 1) // 2
        if fill_cost(row, mid * step) <= cash:
            low = mid
        else:
            high = mid - 1
    return low * step if low * step >= minimum else 0


def whole_children(winner, limited, pool):
    if not (limited.exact_attempt and winner.exact_attempt):
        return {"whole_status": "UNKNOWN_ATTEMPT_SIZE"}
    left, right = fill_cost(winner, winner.attempted), fill_cost(limited, limited.attempted)
    exclusive = max(left, right) <= pool < left + right
    result = dict(whole_children_exclusive=exclusive, whole_budget_cents=pool)
    if not exclusive:
        return dict(result, whole_status="NO_WHOLE_CHOICE")
    original = affordable(limited, pool - left, limited.attempted)
    if winner.filled != winner.attempted or limited.filled != original:
        return dict(result, whole_status="ORIGINAL_ALLOCATION_MISMATCH")
    revised = affordable(winner, pool - right, winner.filled)
    minimum, step = board_lot_rule(winner.stock)
    if not legal(winner.remaining_before - revised, winner.stock):
        revised = max(0, (winner.remaining_before - minimum) // step * step)
        if revised < minimum:
            revised = 0
    if not legal(limited.remaining_before - limited.attempted, limited.stock):
        return dict(result, whole_status="ILLEGAL_PARENT_REMAINDER")
    return dict(result, whole_status="EXCLUSIVE", whole_funded_shares=revised,
                whole_limited_shares=limited.attempted)


def compare_pair(winner, limited):
    """Keep all other same-clock buys funded, including already accepted challenger shares."""
    row = dict(account=limited.account, tick=limited.tick, date=limited.date,
               funded=winner.stock, limited=limited.stock, funded_code=winner.code, limited_code=limited.code,
               original_funded_shares=winner.filled, original_limited_shares=limited.filled)
    if pd.isna(winner.code) or pd.isna(limited.code):
        return dict(row, status="UNKNOWN_INDUSTRY")
    if winner.code == limited.code:
        return dict(row, status="SAME_INDUSTRY")
    old_cost = fill_cost(winner, winner.filled)
    used = fill_cost(limited, limited.filled)
    cash = int(limited.cash_end_cents)
    row.update(whole_children(winner, limited, cash + old_cost + used))
    give = board_lot_rule(winner.stock)[0]
    take = board_lot_rule(limited.stock)[0]
    if (winner.filled < give or limited.attempted - limited.filled < take
            or not legal(winner.filled - give, winner.stock)
            or not legal(limited.filled + take, limited.stock)
            or not legal(limited.remaining_before - limited.filled - take, limited.stock)):
        return dict(row, status="NO_INDEPENDENT_LOT")
    saved = old_cost - fill_cost(winner, winner.filled - give)
    extra = fill_cost(limited, limited.filled + take) - used
    row.update(funded_shares=give, limited_shares=take, saved_cents=saved, extra_cents=extra,
               budget_cents=cash + saved)
    if extra <= cash:
        return dict(row, status="BOTH_AFFORDABLE")
    if extra > cash + saved:
        return dict(row, status="ALTERNATIVE_TOO_EXPENSIVE")
    return dict(row, status="EXCLUSIVE")


def review(tickets, scores=None):
    t = tickets[TICKETS].copy()
    t["tick"] = pd.to_datetime(t.tick, format="ISO8601")
    t["date"] = pd.to_datetime(t.date, format="ISO8601")
    for col in ("account", "stock"):
        if t[col].isna().any() or t[col].astype(str).str.strip().eq("").any():
            raise ValueError(f"missing {col}")
    t["code"] = t.code.replace(r"^\s*$", None, regex=True)
    if (t.tick.isna().any() or not t.date.eq(t.date.dt.normalize()).all()
            or not t.tick.dt.normalize().gt(t.date).all()):
        raise ValueError("decision date must precede the execution day")
    for col in ("sequence", "filled", "attempted", "remaining_before", "price_cents", "prior_notional_cents", "cash_end_cents"):
        if not t[col].map(lambda n: isinstance(n, Integral) and not isinstance(n, bool) and n >= 0).all():
            raise ValueError(f"expected nonnegative integers in {col}")
    if any(not pd.api.types.is_bool_dtype(t[c]) or t[c].isna().any() for c in ("cash_limited", "exact_attempt")):
        raise ValueError("constraint and exactness flags must be known booleans")
    if (t.price_cents.eq(0) | t.attempted.lt(t.filled) | t.attempted.gt(t.remaining_before)
            | t.cash_limited.ne(t.attempted.gt(t.filled))).any():
        raise ValueError("inconsistent price, attempted quantity or cash constraint")
    if any(not legal(r.filled, r.stock) or not legal(r.attempted, r.stock) for r in t.itertuples()):
        raise ValueError("original child quantities violate board lots")
    if t.duplicated([*CLOCK, "stock"]).any() or t.duplicated([*CLOCK, "sequence"]).any():
        raise ValueError("duplicate buy or execution sequence at the same clock")
    if t.groupby(CLOCK)[["cash_end_cents", "date"]].nunique().gt(1).any().any():
        raise ValueError("cash or decision date mismatch within the clock")
    t["rank"] = pd.to_numeric(t["rank"], errors="raise")
    ranks = t["rank"].dropna()
    if not (ranks.gt(0) & ranks.mod(1).eq(0)).all():
        raise ValueError("known source ranks must be positive integers")
    lookup = {}
    if scores is not None:
        s = scores[SCORES].copy()
        for name in ("date", "effective"):
            s[name] = pd.to_datetime(s[name])
        if (s.code.isna().any() or s.code.astype(str).str.strip().eq("").any()
                or not s.effective.eq(s.effective.dt.normalize()).all() or not s.date.eq(s.date.dt.normalize()).all()
                or s.duplicated(SCORES[:3]).any() or not s.effective.gt(s.date).all()):
            raise ValueError("invalid score key or clock")
        values = pd.to_numeric(s.score, errors="raise")
        if values.isin([float("inf"), float("-inf")]).any():
            raise ValueError("infinite score")
        lookup = s.assign(score=values).set_index(SCORES[:3]).score.to_dict()
    rows, selected, selected_whole = [], set(), set()
    grouped = t.sort_values([*CLOCK, "sequence"]).groupby(CLOCK, sort=True)
    for _, group in grouped:
        stock_rows = list(group.itertuples())
        constrained = sorted((v for v in stock_rows if v.cash_limited), key=lambda v: v.stock)
        for winner in stock_rows:
            if not winner.filled:
                continue
            for lost in constrained:
                if winner.sequence >= lost.sequence:
                    continue
                row = compare_pair(winner, lost)
                row["first_daily"] = False
                day = winner.account, winner.tick.normalize()
                row["whole_first_daily"] = False
                if row.get("whole_status") == "EXCLUSIVE":
                    row["whole_first_daily"] = day not in selected_whole
                    selected_whole.add(day)
                if row["status"] == "EXCLUSIVE":
                    row["first_daily"] = day not in selected
                    selected.add(day)
                if row["status"] == "EXCLUSIVE" or row.get("whole_status") == "EXCLUSIVE":
                    key = winner.date, day[1]
                    a, b = (lookup.get((*key, code)) for code in (winner.code, lost.code))
                    row["hmm_choice"] = ("UNKNOWN" if pd.isna(a) or pd.isna(b)
                                         else "SWITCH" if b > a else "KEEP")
                    row["rank_choice"] = ("UNKNOWN" if pd.isna(winner.rank) or pd.isna(lost.rank)
                                          else "SWITCH" if lost.rank < winner.rank else "KEEP")
                rows.append(row)
        for limited in constrained:
            if not any(r.filled and r.sequence < limited.sequence for r in stock_rows):
                rows.append(dict(account=limited.account, tick=limited.tick, date=limited.date,
                                 limited=limited.stock, limited_code=limited.code,
                                 original_limited_shares=limited.filled,
                                 status="NO_EARLIER_FUNDED_BUY", first_daily=False, whole_first_daily=False))
    return pd.DataFrame(rows, columns=PAIR)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tickets", type=Path, required=True)
    parser.add_argument("--scores", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    scores = pd.read_parquet(args.scores, columns=SCORES) if args.scores else None
    review(pd.read_parquet(args.tickets, columns=TICKETS), scores).to_csv(args.output, index=False,
                                                                         encoding="utf-8-sig")


if __name__ == "__main__":
    main()
