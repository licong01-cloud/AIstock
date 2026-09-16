"""Compact contracts for request, CLI, candidate identity and safe defaults."""

import os
import runpy
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pandas as pd
import pytest

from backend.services.factor_research.models import ResearchError
from backend.services.factor_research.runner import CANONICAL_UNIVERSE, evaluation_context, validate_spec
from scripts.factor_research import configure

ROOT = Path(__file__).resolve().parents[3]


def spec(tmp_path):
    data, script = tmp_path / "data", tmp_path / "factor.py"
    data.mkdir()
    script.write_text("# reviewed", encoding="utf-8")
    return {
        "task_id": str(uuid4()),
        "record_id": str(uuid4()),
        "attempt_id": str(uuid4()),
        "expected_revision": 1,
        "universe_key": CANONICAL_UNIVERSE,
        "method_version": "1.0",
        "data_dir": str(data),
        "qlib_bin_path": str(data),
        "artifact_root": str(tmp_path / "out"),
        "instruments": ["000001.SZ"],
        "read_start": "2026-01-01",
        "signal_start": "2026-01-02",
        "signal_end": "2026-01-03",
        "read_end": "2026-01-04",
        "cutoff": "2026-01-05",
        "candidates": [{"factor_name": "m_trial", "script": str(script)}],
    }


@pytest.mark.parametrize(
    "mutation,match",
    [
        (lambda value: value.update(universe_key="shsz_st_pit_active_v1"), "no PIT bootstrap"),
        (lambda value: value.update(activate_factor=True), "Unknown run fields"),
        (lambda value: value["candidates"].append(dict(value["candidates"][0])), "unique"),
    ],
)
def test_run_scope_and_candidate_identity_fail_closed(tmp_path, mutation, match):
    value = spec(tmp_path)
    mutation(value)
    with pytest.raises(ResearchError, match=match):
        validate_spec(value)


def test_cli_and_backend_plan_default_to_no_database(monkeypatch, tmp_path):
    result = subprocess.run(
        [sys.executable, "scripts/factor_research.py", "--help"],
        cwd=ROOT,
        capture_output=True,
        timeout=20,
        env={**os.environ, "TDX_DB_PORT": "invalid"},
    )
    assert result.returncode == 0 and b"attach" in result.stdout
    path = tmp_path / "dev.env"
    path.write_text("TDX_DB_NAME=production\n", encoding="utf-8")
    with pytest.raises(ResearchError, match="incomplete"):
        configure(path, "dev")
    monkeypatch.setenv("AISTOCK_DEV_DB_E2E", "1")
    calls = []

    class Session:
        def run(self, *args, **kwargs):
            calls.append((args, kwargs))

    runpy.run_path(str(ROOT / "noxfile.py"))["factor_research_backend"](Session())
    assert calls and all(call[1]["env"]["AISTOCK_DEV_DB_E2E"] == "0" for call in calls)


def test_evaluation_slice_reuses_engine_labels_without_recomputation():
    dates = pd.date_range("2026-01-01", periods=5)
    close = pd.DataFrame({"000001.SZ": [1.0, 2.0, 4.0, 8.0, 16.0]}, index=dates)
    labels = close.shift(-2) / close.shift(-1) - 1
    ctx = {
        "close_unstacked": close,
        "fwd_ret_mats": {"1d": labels},
        "dates": dates,
        "st_pit_eligible_mask": close.notna(),
    }
    view = evaluation_context(ctx, {"signal_start": "2026-01-02", "signal_end": "2026-01-03"})
    assert view["fwd_ret_mats"]["1d"].equals(labels.loc[dates[1:3]]) and len(ctx["dates"]) == 5
