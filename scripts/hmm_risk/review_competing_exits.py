"""Audit a frozen three-slice LocalSIM liquidation after a historical buy-priority change."""
from __future__ import annotations

import argparse
from numbers import Integral
from pathlib import Path

import pandas as pd

from backend.execution_algos.board_lot import round_to_board_lot


GROUP = ["account", "order"]
INPUT = [*GROUP, "tick", "stock", "target_quantity", "original_open", "revised_open",
         "split_count", "sellable_bar", "volume_shares", "original_sold"]
OUTPUT = [*INPUT, "status", "revised_sold"]


def _schedule(stock, total, bars):
    """Native three-slice timing and board rounding using the same original bar sequence."""
    sold = 0
    quantities = []
    for i, bar in enumerate(bars):
        remaining = total - sold
        qty = 0
        if remaining and bar.sellable_bar:
            target = remaining if i >= 2 else int(total / 3 * (i + 1) - sold)
            raw = min(max(0, target), remaining, bar.volume_shares)
            qty = round_to_board_lot(raw, stock, side="SELL", allow_sell_residual=i >= 2 and raw == remaining)
        quantities.append(qty)
        sold += qty
    return quantities


def review(bars):
    data = bars[INPUT].copy()
    data["tick"] = pd.to_datetime(data.tick)
    if data.tick.isna().any() or data.duplicated([*GROUP, "tick"]).any():
        raise ValueError("incomplete or duplicate order clock")
    for col in [*GROUP, "stock"]:
        if data[col].isna().any() or data[col].astype(str).str.strip().eq("").any():
            raise ValueError(f"missing {col}")
    for col in ("target_quantity", "original_open", "revised_open", "split_count", "volume_shares", "original_sold"):
        if not data[col].map(lambda n: isinstance(n, Integral) and not isinstance(n, bool) and n >= 0).all():
            raise ValueError(f"{col} must contain nonnegative integers")
    if not pd.api.types.is_bool_dtype(data.sellable_bar) or data.sellable_bar.isna().any():
        raise ValueError("sellability is required for every bar")
    sheets = []
    for _, g in data.sort_values([*GROUP, "tick"]).groupby(GROUP):
        first = g.iloc[0]
        if (g[["stock", "target_quantity", "original_open", "revised_open", "split_count"]].nunique().gt(1).any()
                or g.tick.dt.normalize().nunique() != 1):
            raise ValueError("order identity or opening balance changes within a liquidation")
        status = "VERIFIED"
        revised = None
        if first.target_quantity != 0 or first.split_count != 3:
            status = "OUTSIDE_ZERO_TARGET_TWAP"
        else:
            book = list(g.itertuples())
            original = _schedule(first.stock, int(first.original_open), book)
            if original != g.original_sold.tolist() or sum(original) != first.original_open:
                status = "ORIGINAL_NOT_REPRODUCED"
            else:
                proposal = _schedule(first.stock, int(first.revised_open), book)
                if sum(proposal) != first.revised_open:
                    status = "INSUFFICIENT_EXIT_BARS"
                elif any(new > old and (old == 0 or (new - old) * 100 > bar.volume_shares)
                         for new, old, bar in zip(proposal, original, book)):
                    status = "EXTRA_VOLUME_UNVERIFIED"
                else:
                    revised = proposal
        sheets.append(g.assign(status=status, revised_sold=revised if revised is not None else pd.NA))
    return pd.concat(sheets, ignore_index=True) if sheets else pd.DataFrame(columns=OUTPUT)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bars", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    review(pd.read_parquet(args.bars, columns=INPUT)).to_csv(args.output, index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()
