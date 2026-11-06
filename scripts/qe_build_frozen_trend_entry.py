"""Map signed factor research values to frozen prediction keys, without research."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

if __package__:
    from .qe_trend_entry_admission import panel_index_sha, require, sha256, symbol_key
else:
    from qe_trend_entry_admission import panel_index_sha, require, sha256, symbol_key


def build_panel(index, r20, r60, links, resolved, calendar, output):
    require(isinstance(index, pd.MultiIndex) and set(index.names) == {"datetime", "instrument"}
            and index.is_unique, "REFERENCE_INDEX")
    require(r20.index.equals(r60.index) and r20.columns.equals(r60.columns)
            and r20.index.is_unique and r20.columns.is_unique, "RESEARCH_MATRIX_KEYS")
    require(all(t == np.dtype("float32") for t in r20.dtypes)
            and all(t == np.dtype("float32") for t in r60.dtypes), "RESEARCH_FLOAT32")
    codes = np.array(sorted(symbol_key(s) for s in index.get_level_values("instrument").unique()), dtype="U9")
    require(len(codes) == len(set(codes)), "REFERENCE_SYMBOL_COLLISION")
    code_map = {s: i for i, s in enumerate(codes)}
    symbol_map = {s: code_map[symbol_key(s)] for s in index.get_level_values("instrument").unique()}
    days_raw = pd.DatetimeIndex(index.get_level_values("datetime"))
    require(days_raw.equals(days_raw.normalize()) and days_raw.tz is None, "REFERENCE_DATE")
    stock_ids = np.array(index.get_level_values("instrument").map(symbol_map), dtype=np.int32)
    order = np.lexsort((stock_ids, days_raw.asi8))
    sorted_days, stock_ids = days_raw[order], stock_ids[order]
    dates, counts = np.unique(sorted_days.to_numpy(), return_counts=True)
    days = pd.DatetimeIndex(dates)
    offsets = np.concatenate(([0], np.cumsum(counts))).astype(np.int64)
    require(days.isin(r20.index).all() and set(codes).issubset(r20.columns), "RESEARCH_COVERAGE")
    positions = calendar.get_indexer(days)
    require(np.all(positions >= 0) and np.all(positions + 1 < len(calendar)), "NEXT_SESSION_MISSING")
    # Finite values are taken verbatim. Endpoint evidence is consulted only for NA.
    proven = set()
    for row in resolved.itertuples(index=False):
        require(row.resolved_reason == "BOUND_PHYSICAL_NA_WITH_DECLARED_SUSPENSION"
                and bool(row.declared_suspension) and pd.isna(row.finite_physical_value), "NA_EVIDENCE_INVALID")
        proven.add((symbol_key(row.stock), str(row.endpoint_date)))
    references = {}
    for row in links.itertuples(index=False):
        key = (str(row.signal_date), symbol_key(row.stock))
        references.setdefault(key, []).append((symbol_key(row.stock), str(row.endpoint_date)))
    values20, values60 = np.empty(len(index), np.float32), np.empty(len(index), np.float32)
    reasons = np.zeros(len(index), dtype=np.uint8)
    unknown, na_count = [], 0
    for i, day in enumerate(days):
        lo, hi = offsets[i:i + 2]
        names = codes[stock_ids[lo:hi]]
        # These are paired one-dimensional arrays, never [:, None] or a Cartesian join.
        v20, v60 = r20.loc[day, names].to_numpy(), r60.loc[day, names].to_numpy()
        require(not np.isinf(v20).any() and not np.isinf(v60).any(), "RESEARCH_INF")
        values20[lo:hi], values60[lo:hi] = v20, v60
        for j in np.flatnonzero(~(np.isfinite(v20) & np.isfinite(v60))):
            key = (str(day.date()), str(names[j]))
            endpoints = references.get(key, [])
            if not endpoints or any(endpoint not in proven for endpoint in endpoints):
                if len(unknown) < 20:
                    unknown.append(key)
            else:
                reasons[lo + j] = 1
                na_count += 1
    finite = np.isfinite(values20) & np.isfinite(values60)
    require(np.all(reasons[~finite] == 1), "UNPROVEN_NA:" + json.dumps(unknown))
    arrays = {"dates": days.strftime("%Y-%m-%d").to_numpy(dtype="U10"),
              "execution_dates": calendar[positions + 1].strftime("%Y-%m-%d").to_numpy(dtype="U10"),
              "codes": codes, "offsets": offsets, "stock_indices": stock_ids,
              "return20": values20, "return60": values60, "reason": reasons}
    output = Path(output)
    with output.open("xb") as stream:
        np.savez_compressed(stream, **arrays)
    return {"path": output.name, "sha256": sha256(output), "rows": len(index), "days": len(days),
            "start": str(days[0].date()), "end": str(days[-1].date()),
            "index_sha256": panel_index_sha(arrays["dates"], codes, offsets, stock_ids),
            "legal_na_rows": na_count, "available_rows": int(finite.sum()),
            "allocated_array_bytes": sum(a.nbytes for a in arrays.values())}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request", required=True, help="Explicit signed input paths and output path")
    args = parser.parse_args()
    request = json.loads(Path(args.request).read_text(encoding="utf-8"))
    paths = {}
    for key in ("prediction", "return20", "return60", "missing_links", "resolved_endpoints", "calendar"):
        ref = request["inputs"][key]
        require(sha256(ref["path"]) == ref["sha256"], "SOURCE_HASH:" + key)
        paths[key] = ref["path"]
    require(not Path(request["output"]).exists() and not Path(request["receipt"]).exists(), "EXISTING_OUTPUT")
    # Only an explicitly SHA-pinned trusted QE prediction pickle may be deserialized.
    index = pd.read_pickle(paths["prediction"]).index
    receipt = build_panel(index, pd.read_parquet(paths["return20"]), pd.read_parquet(paths["return60"]),
                          pd.read_csv(paths["missing_links"]), pd.read_csv(paths["resolved_endpoints"]),
                          pd.DatetimeIndex(pd.to_datetime(Path(paths["calendar"]).read_text().splitlines())),
                          request["output"])
    receipt.update(source_receipts=request["inputs"], new_fits=0, new_predictions=0, new_backtests=0)
    with Path(request["receipt"]).open("x", encoding="utf-8") as stream:
        json.dump(receipt, stream, ensure_ascii=False, indent=2, allow_nan=False)
    print(json.dumps({k: receipt[k] for k in ("rows", "days", "legal_na_rows", "sha256")}))


if __name__ == "__main__":
    main()
