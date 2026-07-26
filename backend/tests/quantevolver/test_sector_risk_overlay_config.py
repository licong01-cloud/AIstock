from __future__ import annotations

import hashlib
import json

import pandas as pd
import pytest

from backend.services.quantevolver.config_composer import (
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
