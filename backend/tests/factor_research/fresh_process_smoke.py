"""Built-in unittest runner for WSL environments without pytest; no database access."""
import sys
import tempfile
import unittest
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from backend.services.factor_research.runner import (  # noqa: E402
    CANONICAL_UNIVERSE, execute, validate_spec,
)
from backend.tests.factor_research.fixtures.trend_pullback import calculate  # noqa: E402


class FreshProcessTests(unittest.TestCase):
    def setUp(self):
        self.dates = pd.bdate_range("2025-01-01", periods=150)
        self.names = [f"{n:06d}.SZ" for n in range(1, 13)]
        random = np.random.default_rng(17)
        self.close = pd.DataFrame(np.exp(random.normal(.001, .01, (150, 12)).cumsum(0)),
                                  index=self.dates, columns=self.names)
        self.close.index.name = "datetime"
        self.close.columns.name = "instrument"

    def test_causality_and_stock_isolation(self):
        close = self.close.stack()
        whole = calculate(close)
        prefix = calculate(close.loc[(slice(None, self.dates[100]), slice(None))])
        pd.testing.assert_frame_equal(prefix, whole.loc[prefix.index])
        changed = self.close.copy()
        changed.loc[self.dates[101]:] *= 10
        pd.testing.assert_frame_equal(prefix, calculate(changed.stack()).loc[prefix.index])
        isolated = calculate(close.loc[(slice(None), [self.names[0]])])
        pd.testing.assert_frame_equal(isolated, whole.loc[isolated.index])

    def test_subprocess_shared_metric_oracle(self):
        from backend.services.quantevolver.qe_eval_v2_metric_engine import compute_single_factor_metrics
        with tempfile.TemporaryDirectory(prefix="factor-research-") as directory:
            root = Path(directory)
            data = root / "data"
            data.mkdir()
            self.close.stack().to_frame("close").to_hdf(data / "daily_pv.h5", key="data", format="table")
            raw = {"task_id": str(uuid4()), "record_id": str(uuid4()), "attempt_id": str(uuid4()),
                   "expected_revision": 1, "universe_key": CANONICAL_UNIVERSE, "method_version": "1.0",
                   "read_start": str(self.dates[0].date()), "signal_start": str(self.dates[70].date()),
                   "signal_end": str(self.dates[110].date()), "read_end": str(self.dates[-1].date()),
                   "cutoff": str(self.dates[-1].date()), "instruments": self.names, "data_dir": str(data),
                   "qlib_bin_path": str(data), "artifact_root": str(root / "output"),
                   "candidates": [{"factor_name": "m_p1_trend_pullback_case",
                                   "script": str(Path(__file__).parent / "fixtures" / "trend_pullback.py")}]}
            spec, output = validate_spec(raw)
            context = {"close_unstacked": self.close,
                       "fwd_ret_mats": {p: self.close.shift(-n) / self.close.shift(-1) - 1
                                        for p, n in (("1d", 2), ("5d", 6), ("10d", 11), ("20d", 21))},
                       "dates": self.dates, "st_pit_eligible_mask": self.close.notna(),
                       "data_start": raw["read_start"], "data_end": raw["read_end"],
                       "calc_batch_id": str(uuid4()), "suspended_pairs": set(), "universe_metadata": {},
                       "coverage_semantics": "test_only"}
            captured = []
            def oracle(name, frame, ctx):
                actual = compute_single_factor_metrics(name, frame, ctx)
                captured.append(actual)
                return actual
            result = execute(spec, output, prepare=lambda **kwargs: context, compute=oracle)
            self.assertEqual(result["status"], "computed")
            self.assertEqual(len(result["candidates"]), 1)
            self.assertEqual(result["candidates"][0]["metrics"], captured[0])
            self.assertEqual(result["candidates"][0]["signal_rows"], 41 * 12)
            self.assertEqual(result["candidates"][0]["nan_rows"], 60 * 12)


if __name__ == "__main__":
    unittest.main(verbosity=2)
