from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.prospective_price_confirmation import (
    build_price_prospective_confirmation,
)
from backend.services.advisory_model_first.prospective_price_evaluation import (
    AdvisoryPriceProspectiveSettlementArtifact,
)
from backend.services.advisory_model_first.prospective_price_evaluation_contracts import (
    AdvisoryPriceOutcomeRefreshAuditV1,
    AdvisoryPriceProspectiveOutcomeCandidateV1,
    AdvisoryPriceProspectiveSettlementMetricsV1,
    build_settlement,
)


BUNDLE_ID = "8" * 64


def _artifact(index: int, *, package_id: str = "package") -> AdvisoryPriceProspectiveSettlementArtifact:
    target = date(2026, 1, 2) + timedelta(days=index)
    candidates = tuple(
        AdvisoryPriceProspectiveOutcomeCandidateV1(
            symbol=f"{number:06d}.SZ",
            model_prediction_status="AVAILABLE",
            market_outcome_status="AVAILABLE",
            market_outcome_reason="target_open_observed",
            actual_open=10.0 + (0.3 if number % 5 == 0 else 0.0),
            decision_reference_price=10.0,
            calibrated_low=9.8,
            calibrated_mid=10.0,
            calibrated_high=10.2,
            covered=number % 5 != 0,
            lower_miss=False,
            upper_miss=number % 5 == 0,
            interval_width_bps=400.0,
            absolute_mid_error_bps=300.0 if number % 5 == 0 else 0.0,
        )
        for number in range(15)
    )
    coverage = sum(bool(row.covered) for row in candidates) / len(candidates)
    metrics = AdvisoryPriceProspectiveSettlementMetricsV1(
        candidate_count=15,
        market_available_count=15,
        not_applicable_count=0,
        model_available_market_available_count=15,
        model_unavailable_count=0,
        calibrated_coverage=coverage,
        lower_miss_rate=0.0,
        upper_miss_rate=1.0 - coverage,
        mean_interval_width_bps=400.0,
        median_interval_width_bps=400.0,
        mean_absolute_mid_error_bps=60.0,
        median_absolute_mid_error_bps=0.0,
    )
    audits = tuple(
        AdvisoryPriceOutcomeRefreshAuditV1(
            dataset=dataset,
            trade_date=target,
            data_source="test",
            quality_status="ok" if dataset == "kline_daily_raw" else "empty_valid",
            row_count=100,
            refreshed_at=datetime.combine(target, datetime.min.time(), tzinfo=timezone.utc)
            + timedelta(hours=10),
        )
        for dataset in ("kline_daily_raw", "suspend_d")
    )
    settlement = build_settlement(
        request_id=f"advprpros_{index:024x}",
        request_sha256=f"{index + 1:064x}",
        prediction_bundle_id=f"{index + 101:064x}",
        prediction_sha256=f"{index + 201:064x}",
        price_range_bundle_id=BUNDLE_ID,
        package_id=package_id,
        review_policy_sha256="b" * 64,
        decision_as_of_trade_date=target - timedelta(days=1),
        target_trade_date=target,
        predicted_at=datetime.combine(target, datetime.min.time(), tzinfo=timezone.utc)
        - timedelta(hours=5),
        settled_at=datetime.combine(target, datetime.min.time(), tzinfo=timezone.utc)
        + timedelta(hours=10),
        refresh_audits=audits,
        candidates=candidates,
        metrics=metrics,
    )
    return AdvisoryPriceProspectiveSettlementArtifact(
        path=Path(str(index)),
        settlement=settlement,
        manifest={},
        receipt=None,  # type: ignore[arg-type]
    )


def test_confirmation_accumulates_without_exposing_metrics(monkeypatch) -> None:
    monkeypatch.setattr(
        "backend.services.advisory_model_first.prospective_price_confirmation._matching_settlements",
        lambda *_args, **_kwargs: [_artifact(index) for index in range(19)],
    )
    result = build_price_prospective_confirmation(model_root="unused", price_range_bundle_id=BUNDLE_ID)
    assert result.status == "ACCUMULATING"
    assert result.metrics is None
    assert result.cluster_bootstrap is None
    assert "target_dates:19/20" in result.support_gaps
    assert result.activation_recommended is False


def test_confirmation_uses_target_date_cluster_bootstrap(monkeypatch) -> None:
    artifacts = [_artifact(index) for index in range(20)]
    monkeypatch.setattr(
        "backend.services.advisory_model_first.prospective_price_confirmation._matching_settlements",
        lambda *_args, **_kwargs: artifacts,
    )
    first = build_price_prospective_confirmation(model_root="unused", price_range_bundle_id=BUNDLE_ID)
    second = build_price_prospective_confirmation(model_root="unused", price_range_bundle_id=BUNDLE_ID)
    assert first.status == "CONFIRMATION_EVIDENCE_READY"
    assert first.available_candidate_count == 300
    assert first.metrics is not None
    assert first.metrics["calibrated_coverage"] == pytest.approx(0.8)
    assert first.cluster_bootstrap == second.cluster_bootstrap
    assert first.cluster_bootstrap["method"] == "TARGET_TRADE_DATE_CLUSTER_BOOTSTRAP_V1"
    assert first.activation_recommended is False
    assert first.decision_use == "NAVIGATION_ONLY"


def test_confirmation_rejects_mixed_lineage(monkeypatch) -> None:
    artifacts = [_artifact(0), _artifact(1, package_id="other")]
    monkeypatch.setattr(
        "backend.services.advisory_model_first.prospective_price_confirmation._matching_settlements",
        lambda *_args, **_kwargs: artifacts,
    )
    with pytest.raises(AdvisoryModelFirstError) as captured:
        build_price_prospective_confirmation(model_root="unused", price_range_bundle_id=BUNDLE_ID)
    assert captured.value.reason_code == "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_IDENTITY_MISMATCH"
