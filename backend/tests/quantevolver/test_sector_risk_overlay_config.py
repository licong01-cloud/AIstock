from __future__ import annotations

import hashlib
import importlib
import json
from pathlib import Path
import sys
import types

import pandas as pd
import pytest

from backend.services.quantevolver.config_composer import (
    QE_MINUTE_RUNTIME_HELPER_FILES,
    QE_STRATEGY_RUNTIME_HELPER_FILES,
    SECTOR_RISK_OVERLAY_ACTION_LOG,
    SECTOR_RISK_OVERLAY_DATA_FILE,
    SECTOR_RISK_OVERLAY_MANIFEST_FILE,
    ConfigComposer,
)
from backend.services.quantevolver.sector_risk_overlay import canonical_json_sha256


DATA_SPLIT = {
    "train_start": "2020-01-02",
    "train_end": "2023-12-29",
    "valid_start": "2024-01-02",
    "valid_end": "2024-12-31",
    "test_start": "2026-01-05",
    "test_end": "2026-01-09",
    "backtest_end": "2026-01-09",
}
STRATEGY_INFO = {
    "strategy_id": "fixture-score-v2",
    "source_code": "class ScoreWeightedTopkStrategyV2:\n    pass\n",
    "portfolio_config": {"class": "ScoreWeightedTopkStrategyV2", "kwargs": {}},
}


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = PROJECT_ROOT / "scripts"


def _load_score_weighted_strategy(monkeypatch: pytest.MonkeyPatch):
    modules = {
        name: types.ModuleType(name)
        for name in (
            "qlib",
            "qlib.contrib",
            "qlib.contrib.strategy",
            "qlib.contrib.strategy.signal_strategy",
            "qlib.backtest",
            "qlib.backtest.decision",
        )
    }

    class TopkDropoutStrategy:
        pass

    class Order:
        pass

    class OrderDir:
        BUY = 1
        SELL = 0

    class TradeDecisionWO:
        pass

    modules["qlib.contrib.strategy.signal_strategy"].TopkDropoutStrategy = TopkDropoutStrategy
    modules["qlib.backtest.decision"].Order = Order
    modules["qlib.backtest.decision"].OrderDir = OrderDir
    modules["qlib.backtest.decision"].TradeDecisionWO = TradeDecisionWO
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)
    monkeypatch.syspath_prepend(str(SCRIPTS_DIR))
    sys.modules.pop("score_weighted_strategy", None)
    return importlib.import_module("score_weighted_strategy")


def _artifact(tmp_path):
    data_path = tmp_path / "overlay.parquet"
    pd.DataFrame({"value": [1]}).to_parquet(data_path, index=False)
    runtime_hash = hashlib.sha256(data_path.read_bytes()).hexdigest()
    manifest = {
        "schema_version": "qe_sector_risk_overlay_manifest_v1",
        "dataset_identity": "fixture-v1",
        "output_start": "2026-01-05",
        "output_end": "2026-01-09",
        "artifacts": {"runtime": {"sha256": runtime_hash}},
    }
    manifest["manifest_payload_sha256"] = canonical_json_sha256(manifest)
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return manifest_path, data_path


def _params(manifest_path, data_path, **overrides):
    params = {
        "sector_risk_overlay_enabled": True,
        "sector_risk_overlay_mode": "bounded_de_risk",
        "sector_risk_overlay_manifest_source": str(manifest_path),
        "sector_risk_overlay_data_source": str(data_path),
        "sector_risk_overlay_strict": True,
    }
    params.update(overrides)
    return params


def test_prepare_packages_verified_artifacts_and_runtime_names(tmp_path) -> None:
    manifest_path, data_path = _artifact(tmp_path)
    params, manifest_text, runtime_bytes = ConfigComposer()._prepare_sector_risk_overlay_runtime(
        custom_params=_params(manifest_path, data_path),
        data_split=DATA_SPLIT,
        strategy_info=STRATEGY_INFO,
    )

    assert json.loads(manifest_text)["dataset_identity"] == "fixture-v1"
    assert runtime_bytes == data_path.read_bytes()
    assert params["sector_risk_overlay_manifest_file"] == SECTOR_RISK_OVERLAY_MANIFEST_FILE
    assert params["sector_risk_overlay_data_file"] == SECTOR_RISK_OVERLAY_DATA_FILE
    assert params["sector_risk_overlay_action_log"] == SECTOR_RISK_OVERLAY_ACTION_LOG


def test_qe_materialization_includes_rebalance_hook_parent_modules() -> None:
    required = {
        "score_weighted_strategy_v2.py",
        "score_weighted_strategy_v2_capacity_v1.py",
        "qe_sector_risk_overlay_strategy.py",
    }
    assert required <= set(QE_STRATEGY_RUNTIME_HELPER_FILES)
    assert required <= set(QE_MINUTE_RUNTIME_HELPER_FILES)


def test_prepare_rejects_runtime_hash_drift(tmp_path) -> None:
    manifest_path, data_path = _artifact(tmp_path)
    data_path.write_bytes(data_path.read_bytes() + b"drift")
    with pytest.raises(ValueError, match="runtime hash mismatch"):
        ConfigComposer()._prepare_sector_risk_overlay_runtime(
            custom_params=_params(manifest_path, data_path),
            data_split=DATA_SPLIT,
            strategy_info=STRATEGY_INFO,
        )


def test_prepare_rejects_unsupported_strategy_and_unknown_mode(tmp_path) -> None:
    manifest_path, data_path = _artifact(tmp_path)
    with pytest.raises(ValueError, match="not supported by strategy"):
        ConfigComposer()._prepare_sector_risk_overlay_runtime(
            custom_params=_params(manifest_path, data_path),
            data_split=DATA_SPLIT,
            strategy_info={
                "source_code": "class TopkDropoutStrategy:\n    pass\n",
                "portfolio_config": {"class": "TopkDropoutStrategy"},
            },
        )
    with pytest.raises(ValueError, match="sector_risk_overlay_mode"):
        ConfigComposer()._prepare_sector_risk_overlay_runtime(
            custom_params=_params(manifest_path, data_path, sector_risk_overlay_mode="mystery"),
            data_split=DATA_SPLIT,
            strategy_info=STRATEGY_INFO,
        )


def test_composed_yaml_routes_to_qe_overlay_wrapper(tmp_path) -> None:
    manifest_path, data_path = _artifact(tmp_path)
    params, _, _ = ConfigComposer()._prepare_sector_risk_overlay_runtime(
        custom_params=_params(manifest_path, data_path),
        data_split=DATA_SPLIT,
        strategy_info=STRATEGY_INFO,
    )
    yaml_text = ConfigComposer()._compose_conf_yaml(
        factors_info=[],
        model_info=None,
        strategy_info=STRATEGY_INFO,
        data_split=DATA_SPLIT,
        custom_params=params,
        has_custom_factors=False,
        has_alpha158=False,
        backtest_freq="day",
        execution_algo="CLOSE_PRICE",
    )

    assert "class: QESectorRiskOverlayScoreWeightedTopkStrategyV2" in yaml_text
    assert "module_path: qe_sector_risk_overlay_strategy" in yaml_text
    assert f"sector_risk_overlay_manifest_file: {SECTOR_RISK_OVERLAY_MANIFEST_FILE}" in yaml_text
    assert f"sector_risk_overlay_data_file: {SECTOR_RISK_OVERLAY_DATA_FILE}" in yaml_text


def test_hmm_adjustment_uses_point_in_time_membership_spans(monkeypatch, tmp_path):
    module = _load_score_weighted_strategy(monkeypatch)
    artifact = tmp_path / "hmm.json"
    artifact.write_text(
        module.json.dumps(
            {
                "daily_coefficients": {
                    "2026-07-15": {"OLD.SI": 2.0, "NEW.SI": 3.0},
                    "2026-07-16": {"OLD.SI": 2.0, "NEW.SI": 3.0},
                },
                "stock_sector_membership_spans": {
                    "000001.SZ": [
                        {
                            "start_date": "2026-07-15",
                            "end_date": "2026-07-15",
                            "sector_code": "OLD.SI",
                        },
                        {
                            "start_date": "2026-07-16",
                            "end_date": "2026-07-16",
                            "sector_code": "NEW.SI",
                        },
                    ]
                },
            }
        ),
        encoding="utf-8",
    )
    strategy = object.__new__(module.ScoreWeightedTopkStrategy)
    strategy.enable_sector_hmm = True
    strategy.hmm_coefficients_file = str(artifact)
    strategy._hmm_config = None
    strategy._hmm_config_loaded = False
    scores = module.pd.Series({"000001.SZ": 1.0})

    old = strategy._apply_hmm_adjustment(scores, "2026-07-15")
    new = strategy._apply_hmm_adjustment(scores, "2026-07-16")

    assert old["000001.SZ"] == 2.0
    assert new["000001.SZ"] == 3.0


def test_hmm_adjustment_rejects_missing_point_in_time_membership(monkeypatch, tmp_path):
    module = _load_score_weighted_strategy(monkeypatch)
    artifact = tmp_path / "hmm.json"
    artifact.write_text(
        module.json.dumps(
            {
                "daily_coefficients": {"2026-07-16": {"A.SI": 1.1}},
                "stock_sector_map_by_date": {
                    "2026-07-16": {"000001.SZ": "A.SI"},
                },
            }
        ),
        encoding="utf-8",
    )
    strategy = object.__new__(module.ScoreWeightedTopkStrategy)
    strategy.enable_sector_hmm = True
    strategy.hmm_coefficients_file = str(artifact)
    strategy._hmm_config = None
    strategy._hmm_config_loaded = False

    with pytest.raises(RuntimeError, match="coverage is incomplete"):
        strategy._apply_hmm_adjustment(
            module.pd.Series({"000002.SZ": 1.0}),
            "2026-07-16",
        )


def test_hmm_adjustment_explicitly_skips_l2_overlay_when_quote_is_unavailable(monkeypatch, tmp_path):
    module = _load_score_weighted_strategy(monkeypatch)
    artifact = tmp_path / "hmm.json"
    artifact.write_text(
        module.json.dumps(
            {
                "daily_coefficients": {"2026-07-16": {"A.SI": 1.1}},
                "stock_sector_map_by_date": {
                    "2026-07-16": {
                        "000001.SZ": "A.SI",
                        "000002.SZ": "B.SI",
                    },
                },
                "quote_unavailable_sector_codes_by_date": {
                    "2026-07-16": ["B.SI"],
                },
                "l2_overlay_unavailable_policy": "explicit_no_l2_overlay_v1",
                "quote_availability_authority": {
                    "authority_id": "fixture",
                    "authority_sha256": "a" * 64,
                },
                "quote_availability_digest": "b" * 64,
            }
        ),
        encoding="utf-8",
    )
    strategy = object.__new__(module.ScoreWeightedTopkStrategy)
    strategy.enable_sector_hmm = True
    strategy.hmm_coefficients_file = str(artifact)
    strategy._hmm_config = None
    strategy._hmm_config_loaded = False

    adjusted = strategy._apply_hmm_adjustment(
        module.pd.Series({"000001.SZ": 2.0, "000002.SZ": 3.0}),
        "2026-07-16",
    )

    assert adjusted["000001.SZ"] == pytest.approx(2.2)
    assert adjusted["000002.SZ"] == 3.0
    assert strategy._last_hmm_adjustment_trace["no_l2_overlay_rows"] == [
        {
            "stock_id": "000002.SZ",
            "sector_code": "B.SI",
            "coefficient": None,
            "reason": "hmm_l2_quote_unavailable_no_overlay",
        }
    ]


def test_hmm_adjustment_rejects_missing_coefficient_without_quote_authority(monkeypatch, tmp_path):
    module = _load_score_weighted_strategy(monkeypatch)
    artifact = tmp_path / "hmm.json"
    artifact.write_text(
        module.json.dumps(
            {
                "daily_coefficients": {"2026-07-16": {"A.SI": 1.1}},
                "stock_sector_map_by_date": {
                    "2026-07-16": {"000002.SZ": "B.SI"},
                },
            }
        ),
        encoding="utf-8",
    )
    strategy = object.__new__(module.ScoreWeightedTopkStrategy)
    strategy.enable_sector_hmm = True
    strategy.hmm_coefficients_file = str(artifact)
    strategy._hmm_config = None
    strategy._hmm_config_loaded = False

    with pytest.raises(RuntimeError, match="coverage is incomplete"):
        strategy._apply_hmm_adjustment(
            module.pd.Series({"000002.SZ": 3.0}),
            "2026-07-16",
        )


def test_hmm_adjustment_rejects_sector_marked_available_and_unavailable(monkeypatch, tmp_path):
    module = _load_score_weighted_strategy(monkeypatch)
    artifact = tmp_path / "hmm.json"
    artifact.write_text(
        module.json.dumps(
            {
                "daily_coefficients": {"2026-07-16": {"A.SI": 1.1}},
                "stock_sector_map_by_date": {
                    "2026-07-16": {"000001.SZ": "A.SI"},
                },
                "quote_unavailable_sector_codes_by_date": {
                    "2026-07-16": ["A.SI"],
                },
                "l2_overlay_unavailable_policy": "explicit_no_l2_overlay_v1",
                "quote_availability_authority": {
                    "authority_id": "fixture",
                    "authority_sha256": "a" * 64,
                },
                "quote_availability_digest": "b" * 64,
            }
        ),
        encoding="utf-8",
    )
    strategy = object.__new__(module.ScoreWeightedTopkStrategy)
    strategy.enable_sector_hmm = True
    strategy.hmm_coefficients_file = str(artifact)
    strategy._hmm_config = None
    strategy._hmm_config_loaded = False

    with pytest.raises(RuntimeError, match="overlaps coefficients"):
        strategy._apply_hmm_adjustment(
            module.pd.Series({"000001.SZ": 2.0}),
            "2026-07-16",
        )


def test_hmm_adjustment_keeps_legacy_static_map_compatible(monkeypatch, tmp_path):
    module = _load_score_weighted_strategy(monkeypatch)
    artifact = tmp_path / "hmm.json"
    artifact.write_text(
        module.json.dumps(
            {
                "daily_coefficients": {"2026-07-16": {"A.SI": 1.1}},
                "stock_sector_map": {"000001.SZ": "A.SI"},
            }
        ),
        encoding="utf-8",
    )
    strategy = object.__new__(module.ScoreWeightedTopkStrategy)
    strategy.enable_sector_hmm = True
    strategy.hmm_coefficients_file = str(artifact)
    strategy._hmm_config = None
    strategy._hmm_config_loaded = False

    adjusted = strategy._apply_hmm_adjustment(
        module.pd.Series({"000001.SZ": 2.0, "legacy-unmapped": 3.0}),
        "2026-07-16",
    )

    assert adjusted["000001.SZ"] == pytest.approx(2.2)
    assert adjusted["legacy-unmapped"] == 3.0
