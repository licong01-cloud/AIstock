import json
import os
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import numpy as np
import pandas as pd
import pytest

from backend.services.factor_research.models import ResearchError, encode, request
from backend.services.factor_research.runner import CANONICAL_UNIVERSE, evaluation_context, load_values, validate_spec
from scripts.factor_research import configure

ROOT = Path(__file__).resolve().parents[3]


def creation():
    return {"task_id": str(uuid4()), "record_id": str(uuid4()), "summary": "创建研究",
            "task": {"title": "回撤", "objective": "验证趋势内回撤是否有增量", "task_type": "new_factor",
                     "context_json": {"method_version": "1.0", "purpose": "research_feedback"}}}


def test_request_roundtrip_and_no_unknown_business_fields():
    req = request(creation(), create=True)
    assert request(json.loads(encode(req)), create=True) == req
    req["task"]["is_available"] = False
    with pytest.raises(ResearchError, match="Unknown task fields"):
        request(req, create=True)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf")])
def test_nonfinite_request_never_becomes_zero(value):
    req = creation()
    req["payload"] = {"ic": value}
    with pytest.raises(ResearchError):
        request(req, create=True)


def test_correction_must_link_history():
    req = {"task_id": str(uuid4()), "record_id": str(uuid4()), "expected_revision": 1,
           "record_type": "correction", "summary": "修正"}
    with pytest.raises(ResearchError, match="related_record_id"):
        request(req)


def test_help_has_no_db_or_business_imports():
    code = "import runpy,sys; sys.argv=['factor_research.py','--help']; runpy.run_path('scripts/factor_research.py',run_name='__main__')"
    result = subprocess.run([sys.executable, "-c", code], cwd=ROOT, capture_output=True,
                            env={**os.environ, "PYTHONIOENCODING": "utf-8", "TDX_DB_PORT": "invalid"}, timeout=20)
    assert result.returncode == 0
    assert b"attach" in result.stdout


def test_explicit_dev_configuration_no_implicit_production(tmp_path, monkeypatch):
    for key in ("HOST", "PORT", "NAME", "USER", "PASSWORD"):
        monkeypatch.setenv("TDX_DB_" + key, "old")
    path = tmp_path / "config.env"
    path.write_text("TDX_DB_NAME=production\n", encoding="utf-8")
    with pytest.raises(ResearchError, match="incomplete"):
        configure(path, "dev")
    assert os.environ["TDX_DB_NAME"] == "old"


def values():
    index = pd.MultiIndex.from_product([pd.date_range("2026-01-01", periods=3), ["000001.SZ", "000002.SZ"]],
                                      names=["datetime", "instrument"])
    return pd.DataFrame({"m_trial": [np.nan, 2, 3, 4, 5, 6]}, index=index)


def test_candidate_nan_preserved_not_dropna(tmp_path):
    path = tmp_path / "v.parquet"
    values().to_parquet(path)
    frame = load_values(path, "m_trial")
    assert len(frame) == 6 and frame.m_trial.isna().sum() == 1


@pytest.mark.parametrize("mode", ["duplicate", "multicolumn", "infinite", "intraday"])
def test_candidate_shape_never_silently_normalizes(tmp_path, mode):
    frame = values()
    if mode == "duplicate":
        frame = pd.concat([frame, frame.iloc[[0]]])
    elif mode == "multicolumn":
        frame["second"] = 1
    elif mode == "infinite":
        frame.iloc[0, 0] = np.inf
    else:
        frame.index = pd.MultiIndex.from_arrays([frame.index.get_level_values(0) + pd.Timedelta(hours=1),
                                                frame.index.get_level_values(1)], names=frame.index.names)
    path = tmp_path / "v.parquet"
    frame.to_parquet(path)
    with pytest.raises(ResearchError):
        load_values(path, "m_trial")


def spec(tmp_path):
    data = tmp_path / "data"
    data.mkdir()
    script = tmp_path / "factor.py"
    script.write_text("print('reviewed fixture')", encoding="utf-8")
    return {"task_id": str(uuid4()), "record_id": str(uuid4()), "attempt_id": str(uuid4()),
            "expected_revision": 1, "universe_key": CANONICAL_UNIVERSE, "method_version": "1.0",
            "data_dir": str(data), "qlib_bin_path": str(data), "artifact_root": str(tmp_path / "output"),
            "instruments": ["000001.SZ"], "read_start": "2026-01-01", "signal_start": "2026-01-02",
            "signal_end": "2026-01-03", "read_end": "2026-01-05", "cutoff": "2026-01-05",
            "candidates": [{"factor_name": "m_trial", "script": str(script)}]}


def test_scope_and_old_snapshot_no_bootstrap(tmp_path):
    value = spec(tmp_path)
    validate_spec(value)
    value["universe_key"] = "shsz_st_pit_active_v1"
    with pytest.raises(ResearchError, match="no PIT bootstrap"):
        validate_spec(value)


def test_output_cannot_be_in_canonical_repository(tmp_path):
    value = spec(tmp_path)
    value["artifact_root"] = str(ROOT / "tmp" / "output")
    with pytest.raises(ResearchError):
        validate_spec(value)


def test_unknown_run_fields_are_not_ignored(tmp_path):
    value = spec(tmp_path)
    value["activate_factor"] = True
    with pytest.raises(ResearchError):
        validate_spec(value)


def test_context_slice_preserves_future_labels_without_warmup_denominator():
    dates = pd.date_range("2026-01-01", periods=6)
    close = pd.DataFrame({"000001.SZ": [1., 2., 4., 8., 16., 32.]}, index=dates)
    forward = close.shift(-2) / close.shift(-1) - 1
    ctx = {"close_unstacked": close, "fwd_ret_mats": {"1d": forward}, "dates": dates,
           "st_pit_eligible_mask": close.notna(), "universe_metadata": {"source": "existing"}}
    view = evaluation_context(ctx, {"signal_start": "2026-01-02", "signal_end": "2026-01-03"})
    assert len(view["dates"]) == 2 and view["fwd_ret_mats"]["1d"].notna().all().all()
    assert view["fwd_ret_mats"]["1d"].iloc[-1, 0] == 1
    assert len(ctx["dates"]) == 6


@pytest.fixture(scope="module")
def nox_plans():
    import runpy

    return runpy.run_path(str(ROOT / "noxfile.py"))


def test_backend_plan_overrides_inherited_dev_authorization(monkeypatch, nox_plans):
    monkeypatch.setenv("AISTOCK_DEV_DB_E2E", "1")
    monkeypatch.setenv("FACTOR_RESEARCH_DEV_ENV_FILE", "must-not-be-read.env")
    calls = []

    class Session:
        def run(self, *args, **kwargs):
            calls.append((args, kwargs))

    nox_plans["factor_research_backend"](Session())
    assert "backend/tests/factor_research/test_repository_dev.py" in calls[0][0]
    for _, kwargs in calls:
        assert kwargs["env"]["AISTOCK_DEV_DB_E2E"] == "0"
        assert kwargs["env"]["FACTOR_RESEARCH_DEV_ENV_FILE"] == ""


def test_dev_plan_requires_explicit_env_and_enables_only_dev(monkeypatch, nox_plans):
    calls = []

    class Session:
        def error(self, message):
            raise RuntimeError(message)

        def run(self, *args, **kwargs):
            calls.append((args, kwargs))

    monkeypatch.delenv("FACTOR_RESEARCH_DEV_ENV_FILE", raising=False)
    with pytest.raises(RuntimeError, match="FACTOR_RESEARCH_DEV_ENV_FILE"):
        nox_plans["factor_research_dev_db"](Session())
    assert not calls
    monkeypatch.setenv("FACTOR_RESEARCH_DEV_ENV_FILE", "explicit-dev.env")
    nox_plans["factor_research_dev_db"](Session())
    assert calls[0][1]["env"]["AISTOCK_DEV_DB_E2E"] == "1"


def test_unauthorized_dev_tests_collect_and_skip_without_connecting():
    code = """
import pytest, psycopg2
def forbidden(*args, **kwargs):
    raise AssertionError('Source CI must not connect to any database')
psycopg2.connect = forbidden
raise SystemExit(pytest.main(['backend/tests/factor_research/test_repository_dev.py', '-q', '-p', 'no:cacheprovider']))
"""
    result = subprocess.run(
        [sys.executable, "-c", code], cwd=ROOT, capture_output=True, timeout=30,
        env={**os.environ, "AISTOCK_DEV_DB_E2E": "0", "FACTOR_RESEARCH_DEV_ENV_FILE": "must-not-be-read.env",
             "PYTHONIOENCODING": "utf-8"},
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert b"8 skipped" in result.stdout


def test_runtime_registration_is_exact_cli_only():
    from scripts.aistock_issue_workflow import _classify_runtime_impact, _load_runtime_target_catalog

    catalog = _load_runtime_target_catalog(ROOT)
    entries = catalog["non_runtime_source_paths"]
    assert entries.count("scripts/factor_research.py") == 1
    assert _classify_runtime_impact(["scripts/factor_research.py"], root=ROOT)["runtime_impact"] == "none"
    backend_files = [f"backend/services/factor_research/{name}.py"
                     for name in ("__init__", "models", "repository", "runner", "service")]
    assert all(path not in entries for path in backend_files)
    result = _classify_runtime_impact(backend_files, root=ROOT)
    assert result["runtime_impact"] == "backend"
    assert result["target_ids"] == ["backend-main"]
    assert set(result["runtime_files"]) == set(backend_files)
    assert _classify_runtime_impact(["scripts/factor_research_unregistered.py"], root=ROOT)["runtime_impact"] == "unknown"
