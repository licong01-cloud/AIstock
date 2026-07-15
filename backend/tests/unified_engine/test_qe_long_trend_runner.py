from __future__ import annotations

import json
from collections import OrderedDict
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from scripts import qe_long_trend_evaluate_runs as runner
from scripts.qe_long_trend_evaluate_runs import (
    normalize_qlib_indicator_object,
    normalize_qlib_position_artifact,
    resolve_recorder_artifacts,
)


class _FakeIndex:
    def __init__(self, values):
        self.idx_list = np.asarray(values)


class _FakeSingleData:
    def __init__(self, values, instruments):
        self.data = np.asarray(values)
        self.indices = [_FakeIndex(instruments)]


class _FakeOrderIndicator:
    def __init__(self, data):
        self.data = data


class _FakeIndicator:
    def __init__(self, history):
        self.order_indicator_his = history


def test_position_resolver_preserves_complete_daily_absence_state():
    value = OrderedDict(
        [
            (
                pd.Timestamp("2026-01-05"),
                {
                    "position": {
                        "cash": 100.0,
                        "now_account_value": 100.0,
                        "000001.SZ": {"amount": 10.0},
                    }
                },
            ),
            (
                pd.Timestamp("2026-01-06"),
                {
                    "position": {
                        "cash": 100.0,
                        "now_account_value": 100.0,
                        "600000.SH": {"amount": 20.0},
                    }
                },
            ),
            (
                pd.Timestamp("2026-01-07"),
                {"position": {"cash": 100.0, "now_account_value": 100.0}},
            ),
        ]
    )

    frame = normalize_qlib_position_artifact(value)
    pivot = frame.pivot(index="datetime", columns="instrument", values="amount")
    assert pivot.loc[pd.Timestamp("2026-01-05"), "000001.SZ"] == 10.0
    assert pivot.loc[pd.Timestamp("2026-01-05"), "600000.SH"] == 0.0
    assert pivot.loc[pd.Timestamp("2026-01-06"), "000001.SZ"] == 0.0
    assert pivot.loc[pd.Timestamp("2026-01-06"), "600000.SH"] == 20.0
    assert pivot.loc[pd.Timestamp("2026-01-07"), "000001.SZ"] == 0.0
    assert pivot.loc[pd.Timestamp("2026-01-07"), "600000.SH"] == 0.0


def test_indicator_resolver_preserves_orders_and_derives_reconciled_trades():
    instruments = ["000001.SZ", "600000.SH"]
    raw = _FakeIndicator(
        OrderedDict(
            [
                (
                    pd.Timestamp("2026-01-06"),
                    _FakeOrderIndicator(
                        OrderedDict(
                            [
                                ("amount", _FakeSingleData([100.0, 50.0], instruments)),
                                ("deal_amount", _FakeSingleData([100.0, 0.0], instruments)),
                                ("trade_price", _FakeSingleData([10.0, np.nan], instruments)),
                                ("trade_cost", _FakeSingleData([1.0, 0.0], instruments)),
                                ("trade_dir", _FakeSingleData([1.0, 0.0], instruments)),
                                ("ffr", _FakeSingleData([1.0, 0.0], instruments)),
                            ]
                        )
                    ),
                )
            ]
        )
    )

    indicator, trades = normalize_qlib_indicator_object(raw)
    assert indicator["side"].tolist() == ["buy", "sell"]
    assert len(trades) == 1
    assert trades.iloc[0]["instrument"] == "000001.SZ"
    assert trades.iloc[0]["quantity"] == 100.0
    assert trades.iloc[0]["price"] == 10.0
    assert trades.iloc[0]["fees"] == 1.0


def test_recorder_resolver_uses_exact_manifest_identity(tmp_path: Path):
    workspace = (tmp_path / "qe_20260716_010203_abcd" / "Loop2").resolve()
    recorder_id = "a" * 32
    experiment_id = "123456789"
    artifact_root = workspace / "mlruns" / experiment_id / recorder_id / "artifacts"
    artifact_root.mkdir(parents=True)
    pd.DataFrame({"score": [1.0]}).to_pickle(artifact_root / "pred.pkl")
    (workspace / "qe_current_recorder.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "recorder_id": recorder_id,
                "experiment_id": experiment_id,
                "cwd": str(workspace),
                "target_mlruns_realpath": str((workspace / "mlruns").resolve()),
            }
        ),
        encoding="utf-8",
    )
    (workspace / "config.json").write_text(
        json.dumps(
            {
                "task_id": "qe_20260716_010203_abcd",
                "loop_index": 2,
                "label_horizon": 40,
                "data_split": {"test_start": "2024-07-01"},
                "execution_manifest": {
                    "artifact": {
                        "strategy": {
                            "audit_subset": {"topk": 50},
                            "kwargs": {"topk": 50},
                        }
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    resolved = resolve_recorder_artifacts(workspace)
    assert resolved.run_id == "qe_20260716_010203_abcd_L2"
    assert resolved.label_horizon == 40
    assert resolved.strategy_topk == 50
    assert resolved.prediction_path == artifact_root / "pred.pkl"
    assert resolved.input_hashes["prediction_sha256"]

    manifest = _read_json(workspace / "qe_current_recorder.json")
    manifest["cwd"] = str(tmp_path / "other")
    (workspace / "qe_current_recorder.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(RuntimeError, match="recorder cwd does not match workspace"):
        resolve_recorder_artifacts(workspace)


def test_evaluate_run_rejects_source_identity_change(monkeypatch: pytest.MonkeyPatch) -> None:
    artifacts = runner.RecorderArtifacts(
        workspace=Path("/tmp/qe_workspace/Loop1"),
        run_id="qe_test_L1",
        prediction_path=Path("/tmp/pred.pkl"),
        label_path=None,
        position_path=None,
        portfolio_report_path=None,
        indicator_object_path=None,
        recorder_manifest=Path("/tmp/qe_current_recorder.json"),
        config_path=Path("/tmp/conf.yaml"),
        artifact_root=Path("/tmp/artifacts"),
        input_hashes={},
        label_horizon=40,
        strategy_topk=50,
        test_start="2024-07-01",
    )
    monkeypatch.setattr(runner, "_evaluator_source_sha256", lambda: "changed")

    with pytest.raises(RuntimeError, match="source changed after process identity"):
        runner.evaluate_run(
            artifacts=artifacts,
            prices=pd.DataFrame(),
            sectors=None,
            snapshot_identity=object(),
            overlap_receipt=object(),
            evaluator_source_sha256="captured",
            output_root=Path("/tmp/out"),
        )


def _read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))
