from __future__ import annotations

import hashlib
import json

import pytest

from scripts.qe_sector_risk_overlay_artifacts import persist_sector_risk_overlay_artifacts


class Recorder:
    def __init__(self):
        self.saved = {}

    def save_objects(self, **kwargs):
        self.saved.update(kwargs)


def test_overlay_evidence_is_persisted_with_recorder_identity(tmp_path, monkeypatch) -> None:
    manifest_path = tmp_path / "manifest.json"
    data_path = tmp_path / "runtime.parquet"
    action_path = tmp_path / "actions.jsonl"
    data_path.write_bytes(b"runtime")
    manifest = {
        "dataset_identity": "fixture-v1",
        "manifest_payload_sha256": "manifest-hash",
    }
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    action = {
        "trade_date": "2026-01-05",
        "instrument": "000001.SZ",
        "action_type": "DE_RISK_SELL",
        "policy_hash": "manifest-hash",
    }
    action_path.write_text(json.dumps(action) + "\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    config = {
        "task": {
            "record": {
                "portfolio_analysis_config": {
                    "strategy": {
                        "class": "QESectorRiskOverlayScoreWeightedTopkStrategyV2",
                        "kwargs": {
                            "sector_risk_overlay_enabled": True,
                            "sector_risk_overlay_mode": "bounded_de_risk",
                            "sector_risk_overlay_manifest_file": manifest_path.name,
                            "sector_risk_overlay_data_file": data_path.name,
                            "sector_risk_overlay_action_log": action_path.name,
                        },
                    }
                }
            }
        }
    }
    recorder = Recorder()

    receipt = persist_sector_risk_overlay_artifacts(recorder, config)

    assert receipt["action_count"] == 1
    assert receipt["runtime_sha256"] == hashlib.sha256(b"runtime").hexdigest()
    assert recorder.saved["qe_sector_risk_overlay_actions.pkl"] == [action]
    assert recorder.saved["qe_sector_risk_overlay_manifest.pkl"] == manifest
    assert recorder.saved["qe_sector_risk_overlay_receipt.pkl"] == receipt


def test_recorder_persistence_is_noop_without_executable_overlay() -> None:
    assert persist_sector_risk_overlay_artifacts(Recorder(), {"items": [1, 2]}) is None


def test_recorder_persistence_rejects_multiple_overlay_strategies(tmp_path) -> None:
    strategy = {
        "class": "QESectorRiskOverlayScoreWeightedTopkStrategyV2",
        "kwargs": {"sector_risk_overlay_enabled": True},
    }
    with pytest.raises(RuntimeError, match="multiple executable"):
        persist_sector_risk_overlay_artifacts(
            Recorder(),
            {"strategies": [strategy, strategy]},
        )


def test_recorder_persistence_rejects_missing_and_invalid_action_assets(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    kwargs = {
        "sector_risk_overlay_enabled": True,
        "sector_risk_overlay_mode": "bounded_de_risk",
        "sector_risk_overlay_manifest_file": "manifest.json",
        "sector_risk_overlay_data_file": "runtime.parquet",
        "sector_risk_overlay_action_log": "actions.jsonl",
    }
    config = {
        "strategy": {
            "class": "QESectorRiskOverlayScoreWeightedTopkStrategyV2",
            "kwargs": kwargs,
        }
    }
    with pytest.raises(RuntimeError, match="missing files"):
        persist_sector_risk_overlay_artifacts(Recorder(), config)

    (tmp_path / "manifest.json").write_text("{}", encoding="utf-8")
    (tmp_path / "runtime.parquet").write_bytes(b"runtime")
    (tmp_path / "actions.jsonl").write_text("not-json\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="invalid JSON"):
        persist_sector_risk_overlay_artifacts(Recorder(), config)

    action = {
        "trade_date": "2026-01-05",
        "instrument": "000001.SZ",
        "action_type": "EXIT",
        "policy_hash": "p1",
    }
    duplicate = json.dumps(action) + "\n" + json.dumps(action) + "\n"
    (tmp_path / "actions.jsonl").write_text(duplicate, encoding="utf-8")
    with pytest.raises(RuntimeError, match="duplicate action identities"):
        persist_sector_risk_overlay_artifacts(Recorder(), config)
