"""Built-in unittest runner for WSL environments without pytest; no database access."""
import sys
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from backend.services.factor_research.runner import (  # noqa: E402
    CANONICAL_UNIVERSE, evaluation_context, execute, validate_spec,
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
        from backend.services.quantevolver import qe_eval_v2_metric_engine as engine
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
            calls = []
            def prepare(**kwargs):
                calls.append(kwargs)
                with patch.object(engine, "read_close_prices", return_value=self.close.stack().to_frame("close")), patch.object(engine, "read_trading_calendar", return_value=self.dates):
                    return engine.prepare_shared_context(**{**kwargs, "load_suspend_d": False, "load_st_pit_mask": False})
            def oracle(name, frame, ctx, **kwargs):
                self.assertTrue(evaluation_context(ctx, raw)["label_calendar"].equals(self.dates))
                return engine.compute_single_factor_metrics(name, frame, ctx, **kwargs)
            result = execute(spec, output, prepare=prepare, compute=oracle)
            self.assertEqual(result["candidates"][0]["signal_rows"], 41 * 12)
            self.assertEqual(result["candidates"][0]["nan_rows"], 60 * 12)
            source = Path(result["candidates"][0]["values"])
            for suffix, periods in ((".h5", None), (".parquet", [1, 5, 10, 20, 40, 60, 120, 240]), (".h5", None)):
                if suffix == ".parquet":
                    source = root / "existing.parquet"
                    pd.read_hdf(result["candidates"][0]["values"], key="data").to_parquet(source)
                before = (source.read_bytes(), source.stat().st_mtime_ns)
                raw["attempt_id"] = str(uuid4())
                raw["holding_periods"] = periods
                raw["candidates"][0].update(values_artifact=str(source), reuse_basis="same fixture inputs/formula")
                spec, output = validate_spec(raw)
                with patch("backend.services.factor_research.runner.subprocess.run", side_effect=AssertionError("must not regenerate")):
                    reused = execute(spec, output, prepare=prepare, compute=oracle)["candidates"][0]
                if periods is None:
                    self.assertNotIn("holding_periods", calls[-1])
                    self.assertNotIn("prediction_evaluation", reused)
                    self.assertEqual(reused["metrics"]["metrics"], result["candidates"][0]["metrics"]["metrics"])
                else:
                    report = reused["prediction_evaluation"]
                    self.assertEqual(calls[-1]["holding_periods"], periods)
                    self.assertEqual(set(report["holding_periods"]), {f"{h}d" for h in periods})
                    support = report["windows"]["full"]["horizon_support"]
                    self.assertEqual([support[f"{h}d"]["n_mature_days"] for h in (1, 40, 60, 120, 240)], [41, 39, 19, 0, 0])
                    self.assertIsNone(report["windows"]["full"]["horizon_metrics"]["240d"]["rank_ic_mean"])
                self.assertEqual((source.read_bytes(), source.stat().st_mtime_ns), before)
                self.assertFalse((output / raw["candidates"][0]["factor_name"] / "values.h5").exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
