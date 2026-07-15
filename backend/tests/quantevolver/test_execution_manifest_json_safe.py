"""BUG-101 regression: execution manifest must survive json.dumps after YAML date parse.

PyYAML safe_load parses unquoted dates (2024-07-01) into datetime.date objects.
These must be normalized into ISO strings before they reach json.dumps / httpx.
"""

from __future__ import annotations

import json

import pytest

from backend.services.qe_archive.models import normalize_json
from backend.services.quantevolver.execution_manifest import (
    _artifact_manifest,
    _compare_manifest,
    _safe_conf_yaml_load,
)

YAML_WITH_DATES = """
task:
  model: {class: LSTM, kwargs: {}}
  dataset:
    class: DatasetH
    kwargs: {handler: {kwargs: {}}}
port_analysis_config:
  strategy: {class: ScoreWeightedTopkStrategyV2, kwargs: {}}
  backtest:
    start_time: 2024-07-01
    end_time: 2026-04-27
    account: 10000000
  executor: {class: SimulatorExecutor}
"""


def test_yaml_dates_parsed_as_date_objects():
    """Confirm that PyYAML parses unquoted dates as datetime.date (root cause)."""
    from datetime import date as date_type

    conf = _safe_conf_yaml_load(YAML_WITH_DATES)
    backtest = conf["port_analysis_config"]["backtest"]
    assert isinstance(backtest["start_time"], date_type)
    assert isinstance(backtest["end_time"], date_type)


def test_artifact_manifest_raw_fails_json_dumps():
    """Without normalization, json.dumps raises TypeError on date objects."""
    conf = _safe_conf_yaml_load(YAML_WITH_DATES)
    manifest = {"artifact": _artifact_manifest(conf, {"label_horizon": 10, "factor_list": ["x"]})}
    with pytest.raises(TypeError, match="date"):
        json.dumps(manifest)


def test_normalize_json_fixes_date_objects():
    """normalize_json converts datetime.date to .isoformat() strings."""
    conf = _safe_conf_yaml_load(YAML_WITH_DATES)
    manifest = {"artifact": _artifact_manifest(conf, {"label_horizon": 10, "factor_list": ["x"]})}
    normalized = normalize_json(manifest)
    payload = json.dumps(normalized)
    assert "2024-07-01" in payload
    assert "2026-04-27" in payload


def test_normalize_json_roundtrip_preserves_strings():
    """Normalize is idempotent — calling it again on already-normalized data is a no-op."""
    conf = _safe_conf_yaml_load(YAML_WITH_DATES)
    manifest = {"artifact": _artifact_manifest(conf, {"label_horizon": 10, "factor_list": ["x"]})}
    once = normalize_json(manifest)
    twice = normalize_json(once)
    assert json.dumps(once) == json.dumps(twice)


def test_right_tail_processor_is_audited_against_requested_identity():
    conf = _safe_conf_yaml_load(
        """
task:
  model: {class: LGBModel, kwargs: {}}
  dataset:
    class: DatasetH
    kwargs:
      handler:
        kwargs:
          learn_processors:
            - class: LongHorizonLabelMaturityPurge
            - class: CSRightTailBinaryLabel
              module_path: qe_custom_loaders
              kwargs: {quantile: 0.99}
port_analysis_config:
  strategy: {class: ScoreWeightedTopkStrategyV2, kwargs: {}}
  backtest: {start_time: 2024-07-01, end_time: 2026-06-29, account: 10000000}
  executor: {class: SimulatorExecutor}
"""
    )
    requested = {
        "factor_list": ["x"],
        "label_horizon": 40,
        "custom_params": {
            "label_objective": "cs_top_quantile_return",
            "right_tail_quantile": 0.99,
        },
    }
    artifact = _artifact_manifest(conf, requested)

    assert artifact["dataset"]["label_objective"] == {
        "name": "cs_top_quantile_return",
        "right_tail_quantile": 0.99,
    }
    assert _compare_manifest(requested, artifact) == []

    drifted = dict(artifact)
    drifted["dataset"] = dict(artifact["dataset"])
    drifted["dataset"]["label_objective"] = {
        "name": "cs_top_quantile_return",
        "right_tail_quantile": 0.95,
    }
    assert any("right_tail_quantile" in item for item in _compare_manifest(requested, drifted))
