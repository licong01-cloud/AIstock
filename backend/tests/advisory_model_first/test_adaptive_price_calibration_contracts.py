from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
from pydantic import ValidationError

from backend.services.advisory_model_first.adaptive_price_calibration_contracts import (
    ADAPTIVE_ARM_ID,
    AdaptivePriceDailyStateV1,
    AdaptivePriceNavigationGateV1,
    FrozenAdaptivePriceCalibrationRequestV1,
    build_adaptive_price_calibration_request,
)


def _request(tmp_path):
    output_root = tmp_path / "models"
    replay_id = "advprhist_" + "1" * 24
    return build_adaptive_price_calibration_request(
        created_at=datetime(2026, 9, 16, tzinfo=timezone.utc),
        output_root=str(output_root.resolve()),
        registry_path=str((tmp_path / "n0" / "registry.jsonl").resolve()),
        source_replay_root=str(
            (output_root / "price_range_historical_replays" / replay_id).resolve()
        ),
        source_replay_id=replay_id,
        source_request_file_sha256="1" * 64,
        source_prediction_file_sha256="2" * 64,
        source_outcome_file_sha256="3" * 64,
        source_manifest_file_sha256="4" * 64,
        source_receipt_file_sha256="5" * 64,
        source_prediction_snapshot_sha256="6" * 64,
        source_outcome_sha256="7" * 64,
        source_price_range_bundle_id="8" * 64,
        source_price_range_manifest_sha256="b" * 64,
        decision_start_trade_date=date(2025, 11, 7),
        decision_end_trade_date=date(2026, 3, 10),
        dataset_identity="9" * 64,
        repository_commit="a" * 40,
    )


def test_request_identity_is_stable_and_paths_are_explicit(tmp_path) -> None:
    first = _request(tmp_path)
    second = _request(tmp_path)
    assert first.request_id == second.request_id
    assert first.request_sha256 == second.request_sha256
    assert first.study_type == "EXPLORATORY_SCREEN"
    assert first.decision_use == "NAVIGATION_ONLY"
    assert first.binding_activated is False
    assert first.database_written is False


def test_request_rejects_protocol_drift(tmp_path) -> None:
    payload = _request(tmp_path).model_dump(mode="python")
    payload["lookback_target_dates"] = 19
    with pytest.raises(ValidationError):
        FrozenAdaptivePriceCalibrationRequestV1.model_validate(payload)


def test_request_rejects_source_outside_model_root(tmp_path) -> None:
    payload = _request(tmp_path).model_dump(mode="python")
    payload["source_replay_root"] = str((tmp_path / "foreign" / payload["source_replay_id"]).resolve())
    with pytest.raises(ValidationError, match="escapes model root"):
        FrozenAdaptivePriceCalibrationRequestV1.model_validate(payload)


def test_consumed_replay_cannot_be_activation_evidence(tmp_path) -> None:
    payload = _request(tmp_path).model_dump(mode="python")
    payload["decision_use"] = "ACTIVATION_EVIDENCE"
    payload["binding_activated"] = True
    with pytest.raises(ValidationError):
        FrozenAdaptivePriceCalibrationRequestV1.model_validate(payload)


def test_daily_state_enforces_maturity_and_warmup() -> None:
    with pytest.raises(ValidationError, match="PIT clock"):
        AdaptivePriceDailyStateV1(
            decision_as_of_trade_date=date(2026, 1, 5),
            target_trade_date=date(2026, 1, 6),
            mode="ADAPTIVE_ACTIVE",
            delta=0.001,
            fit_target_date_count=5,
            fit_row_count=100,
            fit_target_start=date(2025, 12, 29),
            fit_target_end=date(2026, 1, 6),
        )
    with pytest.raises(ValidationError, match="zero delta"):
        AdaptivePriceDailyStateV1(
            decision_as_of_trade_date=date(2026, 1, 5),
            target_trade_date=date(2026, 1, 6),
            mode="WARMUP_STATIC_FALLBACK",
            delta=0.001,
            fit_target_date_count=4,
            fit_row_count=80,
            fit_target_start=date(2025, 12, 29),
            fit_target_end=date(2026, 1, 5),
        )


def test_navigation_gate_cannot_advertise_selection_when_a_gate_fails() -> None:
    values = {
        "support_pass": True,
        "model_coverage_improvement_pass": True,
        "bootstrap_lower_bound_pass": True,
        "model_width_pass": True,
        "miss_rates_pass": True,
        "business_metrics_pass": False,
        "identity_pass": True,
        "all_pass": True,
        "selected_candidate": ADAPTIVE_ARM_ID,
    }
    with pytest.raises(ValidationError, match="summary is inconsistent"):
        AdaptivePriceNavigationGateV1(**values)
