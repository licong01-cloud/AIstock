"""Real-file compatibility check through the official executor; no DB writes."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.quantevolver.backtest_base_data_memory_cache import BacktestBaseDataMemoryCache  # noqa: E402
from backend.services.quantevolver.offline_code_text_factor_executor import OfflineCodeTextFactorExecutor  # noqa: E402
from scripts.qe_alpha_candidates.rotation_liquidity_factors import FACTOR_NAMES  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--factor-data-dir", type=Path, required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    args = parser.parse_args()
    cache = BacktestBaseDataMemoryCache.load_once(
        args.factor_data_dir, args.start, args.end, allowed_files=("daily_pv.h5",),
    )
    executor = OfflineCodeTextFactorExecutor(cache)
    source = (Path(__file__).with_name("rotation_liquidity_factors.py")).read_text(encoding="utf-8")
    all_ok = True
    for name in FACTOR_NAMES:
        code = source + f"\nFACTOR_NAME = {name!r}\nif __name__ == '__main__':\n    compute_factor(FACTOR_NAME)\n"
        result = executor.compute_factor(name, code)
        summary = {
            "factor_name": name, "success": result.success,
            "elapsed_sec": result.elapsed_sec, "error": result.error,
        }
        frame = result.dataframe
        if result.success and frame is not None:
            finite = bool(np.isfinite(frame.to_numpy()).all())
            summary.update({
                "rows": len(frame), "stocks": frame.index.get_level_values("instrument").nunique(),
                "first": str(frame.index.get_level_values("datetime").min()),
                "last": str(frame.index.get_level_values("datetime").max()),
                "all_finite": finite,
            })
            all_ok = all_ok and not frame.empty and finite
        else:
            all_ok = False
        print(json.dumps(summary, ensure_ascii=False, allow_nan=False), flush=True)
        del result, frame
    print(json.dumps({"compatibility_only": True, "official_metrics": False, "database_writes": 0,
                      "source_reads": cache.read_counts, "all_ok": all_ok}), flush=True)
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
