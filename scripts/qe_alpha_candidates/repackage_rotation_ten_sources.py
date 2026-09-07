"""Prove full-file numerical equivalence before specializing catalog source."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> None:
    from backend.services.quantevolver.backtest_base_data_memory_cache import BacktestBaseDataMemoryCache
    from backend.services.quantevolver.offline_code_text_factor_executor import OfflineCodeTextFactorExecutor
    from scripts.qe_alpha_candidates.run_rotation_ten_research import sources

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--factor-data-dir", type=Path, required=True)
    parser.add_argument("--context-dir", type=Path, required=True)
    parser.add_argument("--evaluation-dir", type=Path, required=True)
    args = parser.parse_args()
    payload = json.loads((args.evaluation_dir / "metrics.json").read_text(encoding="utf-8"))
    full = next(row for row in payload["metrics"] if row["eval_window"] == "full")
    cache = BacktestBaseDataMemoryCache.load_once(args.factor_data_dir, full["data_start"], full["data_end"],
                                                allowed_files=("daily_pv.h5",), supplemental_data_dir=args.context_dir)
    executor = OfflineCodeTextFactorExecutor(cache)
    proof = []
    for name, source in sources().items():
        result = executor.compute_factor(name, source)
        if not result.success:
            raise RuntimeError(result.error)
        previous = pd.read_hdf(args.evaluation_dir / f"{name}.h5").rename(columns={name: "value"})
        pd.testing.assert_frame_equal(result.dataframe, previous, check_exact=True)
        path = args.evaluation_dir / f"{name}.py"
        old_source = path.read_text(encoding="utf-8")
        if old_source != source:
            with (args.evaluation_dir / f"{name}.reference.py").open("x", encoding="utf-8") as handle:
                handle.write(old_source)
            path.write_text(source, encoding="utf-8")
        row = {"factor_name": name, "equal_rows": len(previous), "exact_equal": True}
        proof.append(row)
        print(json.dumps(row), flush=True)
        del result, previous
    with (args.evaluation_dir / "source-specialization-proof.json").open("x", encoding="utf-8") as handle:
        json.dump(proof, handle)


if __name__ == "__main__":
    main()
