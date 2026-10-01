"""Direct contract tests for the controlled Risk_L1 product executor."""

from __future__ import annotations

import argparse
from datetime import date

import pytest

from backend.services.hmm_risk import risk_l1_prediction
from scripts.hmm_risk import run_risk_l1_product as subject


def _rows() -> list[dict[str, object]]:
    rows = []
    for index in range(31):
        row: dict[str, object] = {
            "product_bundle_id": None,
            "trade_date": date(2026, 3, 31),
            "as_of_date": date(2026, 3, 30),
            "sector_level": "L1",
            "sector_code": f"801{index:03d}.SI",
            "sector_name": f"Sector {index}",
            "risk_score": index / 30.0,
            "risk_percentile": index / 30.0,
            "risk_level": "high" if index >= 24 else "watch" if index >= 18 else "normal",
            "predicted_warning": index >= 24,
            "feature_contributions": [float(index)] + [0.0] * 9,
            "availability": "available",
            "reason_code": None,
            "risk_l1_research_surface_status": "NOT_AVAILABLE",
            "risk_l1_capability_status": "NOT_AVAILABLE",
            "forward_power_status": "UNAVAILABLE",
            "forward_confirmation": "NOT_STARTED",
            "advisory_status": "NOT_AVAILABLE",
            "validation_basis": "development_causal_oof",
            "development_precision_lift": 0.01,
            "development_recall": 0.2,
            "model_hash": "a" * 64,
            "input_hash": "b" * 64,
            "mapping_snapshot_hash": "c" * 64,
            "tail_accessed": False,
            "revision": 1,
            "supersedes_prediction_id": None,
        }
        row["prediction_id"] = risk_l1_prediction._prediction_id(row)
        rows.append(row)
    return rows


def test_product_executor_dry_run_writes_receipt_without_database(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(subject, "_oof_rows", lambda _args: _rows())
    output = tmp_path / "receipt.json"
    args = argparse.Namespace(
        mode="oof",
        output_receipt=output,
        write_database=False,
        expected_database_name=None,
    )

    receipt = subject.execute(args)

    assert output.exists()
    assert receipt["row_count"] == 31
    assert receipt["database_write_performed"] is False
    assert receipt["model_fit_count"] == 0
    assert receipt["tail_accessed"] is False


def test_product_executor_requires_exact_database_target(tmp_path) -> None:
    with pytest.raises(subject.RiskL1ProductExecutorError, match="required"):
        subject.execute(
            argparse.Namespace(
                mode="oof",
                output_receipt=tmp_path / "receipt.json",
                write_database=True,
                expected_database_name=None,
            )
        )
    with pytest.raises(subject.RiskL1ProductExecutorError, match="forbidden"):
        subject.execute(
            argparse.Namespace(
                mode="oof",
                output_receipt=tmp_path / "receipt.json",
                write_database=False,
                expected_database_name="aistock_dev",
            )
        )
