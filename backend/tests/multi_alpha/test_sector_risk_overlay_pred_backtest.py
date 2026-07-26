from __future__ import annotations

import hashlib

from backend.services.multi_alpha.combine_backtest import apply_pred_backtest_overrides


def test_pred_backtest_arm_changes_policy_only_and_preserves_prediction(tmp_path) -> None:
    conf = """port_analysis_config:
  strategy:
    class: QESectorRiskOverlayScoreWeightedTopkStrategyV2
    module_path: qe_sector_risk_overlay_strategy
    kwargs:
      signal: <PRED>
      topk: 25
      sector_risk_overlay_enabled: true
      sector_risk_overlay_mode: none
      sector_risk_overlay_manifest_file: qe_sector_risk_overlay_manifest.json
      sector_risk_overlay_data_file: qe_sector_risk_overlay.parquet
  backtest:
    account: 100000000
"""
    conf_path = tmp_path / "conf.yaml"
    conf_path.write_text(conf, encoding="utf-8")
    pred_path = tmp_path / "combined_prediction.pkl"
    pred_path.write_bytes(b"immutable-prediction-payload")
    pred_hash = hashlib.sha256(pred_path.read_bytes()).hexdigest()

    apply_pred_backtest_overrides(
        workspace=tmp_path,
        backtest_config={
            "strategy_kwargs": {"sector_risk_overlay_mode": "exit_reentry"}
        },
    )

    updated = conf_path.read_text(encoding="utf-8")
    assert "class: QESectorRiskOverlayScoreWeightedTopkStrategyV2" in updated
    assert "sector_risk_overlay_mode: exit_reentry" in updated
    assert "sector_risk_overlay_manifest_file: qe_sector_risk_overlay_manifest.json" in updated
    assert "sector_risk_overlay_data_file: qe_sector_risk_overlay.parquet" in updated
    assert hashlib.sha256(pred_path.read_bytes()).hexdigest() == pred_hash
