from __future__ import annotations

import pytest

from backend.services.hmm_risk.contracts import canonical_sha256
from backend.services.hmm_risk.rotation_l2 import ACCEPTANCE_SCHEMA
from backend.services.hmm_risk.rotation_l2_prediction import (
    RotationL2PredictionError,
    _validate_row,
    rows_from_acceptance,
)


def _acceptance() -> dict:
    predictions = []
    for index in range(131):
        score = index / 130.0 - 0.5
        predictions.append(
            {
                "trade_date": "2026-03-31",
                "as_of_date": "2026-03-30",
                "sector_code": f"801{index:03d}.SI",
                "sector_name": f"801{index:03d}.SI",
                "rotation_score": score,
                "forecast_state": "fading" if index < 27 else "trending" if index >= 104 else "neutral",
                "feature_contributions": {"moneyflow_intensity_delta_5d_rank": score},
                "availability": "available",
                "reason_code": None,
                "structural_eligible": True,
                "feature_eligible": True,
                "outcome_status": "outcome_not_mature",
            }
        )
    acceptance = {
        "schema_version": ACCEPTANCE_SCHEMA,
        "run_id": "a" * 64,
        "model_hash": "b" * 64,
        "evaluation_contract_hash": "c" * 64,
        "input_hash": "d" * 64,
        "mapping_hash": "e" * 64,
        "quote_authority_hash": "f" * 64,
        "planned_fits": 0,
        "completed_fits": 0,
        "tail_accessed": False,
        "predictions": predictions,
        "metrics": {"overall": {"mean_daily_rank_ic": 0.03}},
        "execution_status": "COMPLETED",
        "effect_status": "DEVELOPMENT_EFFECT_QUALIFIED",
        "rotation_l2_capability_status": "RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED",
        "forward_power_status": "UNAVAILABLE",
        "forward_confirmation": "NOT_STARTED",
        "advisory_status": "NOT_AVAILABLE",
        "validation_basis": "HISTORICAL_CAUSAL_REPLAY_ZERO_FIT",
    }
    acceptance["acceptance_sha256"] = canonical_sha256(acceptance)
    return acceptance


def test_acceptance_maps_to_complete_immutable_l2_rows() -> None:
    rows = rows_from_acceptance(_acceptance())

    assert len(rows) == 131
    assert len({row["sector_code"] for row in rows}) == 131
    assert all(row["sector_level"] == "L2" and row["research_surface_status"] == "NOT_AVAILABLE" for row in rows)
    assert all(row["revision"] == 1 and row["supersedes_prediction_id"] is None for row in rows)


def test_acceptance_hash_drift_fails_closed() -> None:
    acceptance = _acceptance()
    acceptance["effect_status"] = "BELOW_BINDING_MBE"

    with pytest.raises(RotationL2PredictionError, match="acceptance receipt is invalid"):
        rows_from_acceptance(acceptance)


def test_missing_catalog_row_fails_closed() -> None:
    acceptance = _acceptance()
    acceptance["predictions"].pop()
    acceptance["acceptance_sha256"] = canonical_sha256(
        {key: value for key, value in acceptance.items() if key != "acceptance_sha256"}
    )

    with pytest.raises(RotationL2PredictionError, match="does not contain 131"):
        rows_from_acceptance(acceptance)


def test_revision_requires_a_direct_supersedes_identity() -> None:
    row = rows_from_acceptance(_acceptance())[0]
    row["revision"] = 2
    row["prediction_id"] = None

    with pytest.raises(RotationL2PredictionError, match="revision lineage differs"):
        _validate_row(row)


def test_writer_rejects_contribution_or_zero_fit_summary_drift() -> None:
    row = rows_from_acceptance(_acceptance())[0]
    row["feature_contributions"] = {"moneyflow_intensity_delta_5d_rank": 0.0}
    with pytest.raises(RotationL2PredictionError, match="contribution differs"):
        _validate_row(row)

    row = rows_from_acceptance(_acceptance())[0]
    row["run_summary"] = {**row["run_summary"], "tail_accessed": True}
    with pytest.raises(RotationL2PredictionError, match="zero-fit no-tail"):
        _validate_row(row)


def test_writer_rejects_effect_and_capability_drift() -> None:
    row = rows_from_acceptance(_acceptance())[0]
    row["rotation_l2_capability_status"] = "NOT_AVAILABLE"

    with pytest.raises(RotationL2PredictionError, match="effect and L2 capability"):
        _validate_row(row)
