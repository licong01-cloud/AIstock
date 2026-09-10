"""Block-wise new-vs-library diagnostics through the existing correlation engine.

Reads old caches, never changes them. Only pairs touching a new factor are saved.
Blocks bound resident memory; no input frames survive the current old-factor block.
"""
from __future__ import annotations

import argparse
import gc
import json
import math
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class PanelLoader:
    def __init__(self, frame, dates):
        self.frame = frame
        self.dates = dates

    def get_trading_dates(self, start, end):
        return [d for d in self.dates if start <= d <= end]

    def load_factor_panel(self, names, start_date, end_date, **kwargs):
        return self.frame.loc[start_date:end_date, names]


def main() -> None:
    from dotenv import load_dotenv

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--evaluation-dir", type=Path, required=True)
    parser.add_argument("--existing-cache", type=Path, required=True)
    parser.add_argument("--calendar", type=Path, required=True)
    args = parser.parse_args()
    load_dotenv(args.env_file, override=True)
    os.environ["PGOPTIONS"] = "-c default_transaction_read_only=on"
    from backend.db.pg_pool import get_conn
    from backend.services.quantevolver.correlation_engine import CorrelationEngine
    from backend.services.quantevolver.factor_universe_mask_service import FactorUniverseMaskService
    from scripts.qe_alpha_candidates.run_rotation_ten_research import sources

    names = list(sources())
    metrics = json.loads((args.evaluation_dir / "metrics.json").read_text(encoding="utf-8"))
    cutoff = metrics["snapshot_date"]
    meta = json.loads((args.existing_cache / "_meta.json").read_text(encoding="utf-8"))
    if meta["data_end"] != cutoff or meta["universe_key"] != "aistock_equity_pit_canonical_v2":
        raise ValueError("old/new cache window or universe differs")
    dates = [d for d in args.calendar.read_text(encoding="utf-8").splitlines() if d <= cutoff][-252:]
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT factor_name FROM aistock_factor_catalog WHERE is_available ORDER BY factor_name")
            old_names = [row[0] for row in cur.fetchall() if row[0] not in names]
    eligible = FactorUniverseMaskService().build_eligible_index(
        start_date=dates[0], end_date=dates[-1], universe_key=meta["universe_key"], ensure=False)
    new_frames = []
    for name in names:
        data = pd.read_hdf(args.evaluation_dir / f"{name}.h5")
        new_frames.append(data.iloc[:, 0].rename(name).reindex(eligible))
    new = pd.concat(new_frames, axis=1)
    del data, new_frames
    unavailable = {}
    pairs = {}
    for offset in range(0, len(old_names), 32):
        frames = [new]
        data = series = None
        for name in old_names[offset:offset + 32]:
            path = args.existing_cache / "single" / f"{name}.parquet"
            if not path.is_file():
                unavailable[name] = "cache_file_missing"
                continue
            data = pd.read_parquet(path, filters=[("datetime", ">=", pd.Timestamp(dates[0])), ("datetime", "<=", pd.Timestamp(dates[-1]))])
            series = data.iloc[:, 0].rename(name).reindex(eligible)
            if not np.isfinite(series.to_numpy()).any():
                unavailable[name] = "degenerate_nan"
                continue
            frames.append(series)
        panel = pd.concat(frames, axis=1).sort_index()
        del frames, data, series
        engine = CorrelationEngine(PanelLoader(panel, dates))
        result = engine.compute_full_matrix(list(panel.columns), as_of_date=cutoff, save_hdf5=False)
        for i, name in enumerate(result.factor_names):
            for j in range(i + 1, len(result.factor_names)):
                other = result.factor_names[j]
                if name not in names and other not in names:
                    continue
                value = float(result.matrix[i, j])
                key = tuple(sorted((name, other)))
                record = {"factor_a": key[0], "factor_b": key[1],
                          "correlation": value if np.isfinite(value) else None,
                          "reason": None if np.isfinite(value) else "no_valid_pairs"}
                if key in pairs:
                    previous = pairs[key]["correlation"]
                    same = previous is None and record["correlation"] is None
                    if previous is not None and record["correlation"] is not None:
                        same = math.isclose(previous, value, abs_tol=1e-12, rel_tol=1e-9)
                    if not same:
                        raise ValueError("block-dependent pair result")
                else:
                    pairs[key] = record
        print(json.dumps({"old_factors_processed": min(offset + 32, len(old_names)), "old_total": len(old_names),
                          "pairs": len(pairs), "unavailable_factors": unavailable}), flush=True)
        del panel, engine, result
        gc.collect()
    output = {"method": "spearman_ewma", "window": 252, "half_life": 125, "snapshot_date": cutoff,
              "pairs": list(pairs.values()), "unavailable_factors": unavailable,
              "universe_metadata": metrics["universe_metadata"], "database_writes": 0}
    with (args.evaluation_dir / "correlations.json").open("x", encoding="utf-8") as handle:
        json.dump(output, handle, ensure_ascii=False, allow_nan=False)


if __name__ == "__main__":
    main()
