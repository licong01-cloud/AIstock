from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
from pydantic import ValidationError

from backend.services.advisory_model_first.prospective_price_evaluation_contracts import (
    AdvisoryPriceOutcomeRefreshAuditV1,
    AdvisoryPriceProspectiveOutcomeCandidateV1,
    AdvisoryPriceProspectiveSettlementMetricsV1,
    AdvisoryPriceProspectiveSettlementV1,
    build_confirmation,
    build_settlement,
)


SHA = "a" * 64


def _audits() -> tuple[AdvisoryPriceOutcomeRefreshAuditV1, AdvisoryPriceOutcomeRefreshAuditV1]:
    values = []
    for dataset in ("kline_daily_raw", "suspend_d"):
        values.append(
            AdvisoryPriceOutcomeRefreshAuditV1(
                dataset=dataset,
                trade_date=date(2026, 9, 15),
                data_source="test",
                quality_status="ok" if dataset == "kline_daily_raw" else "empty_valid",
                row_count=100 if dataset == "kline_daily_raw" else 0,
                refreshed_at=datetime(2026, 9, 15, 10, tzinfo=timezone.utc),
            )
        )
    return tuple(values)  # type: ignore[return-value]


def _available() -> AdvisoryPriceProspectiveOutcomeCandidateV1:
    return AdvisoryPriceProspectiveOutcomeCandidateV1(
        symbol="000001.SZ",
        model_prediction_status="AVAILABLE",
        market_outcome_status="AVAILABLE",
        market_outcome_reason="target_open_observed",
        actual_open=10.0,
        decision_reference_price=10.0,
        calibrated_low=9.8,
        calibrated_mid=10.1,
        calibrated_high=10.2,
        covered=True,
        lower_miss=False,
        upper_miss=False,
        interval_width_bps=400.0,
        absolute_mid_error_bps=100.0,
    )


def test_settlement_identity_is_self_validating() -> None:
    candidate = _available()
    settlement = build_settlement(
        request_id="advprpros_" + "1" * 24,
        request_sha256="1" * 64,
        prediction_bundle_id="2" * 64,
        prediction_sha256="3" * 64,
        price_range_bundle_id="4" * 64,
        package_id="pkg",
        review_policy_sha256="5" * 64,
        decision_as_of_trade_date=date(2026, 9, 14),
        target_trade_date=date(2026, 9, 15),
        predicted_at=datetime(2026, 9, 14, 19, tzinfo=timezone.utc),
        settled_at=datetime(2026, 9, 15, 10, tzinfo=timezone.utc),
        refresh_audits=_audits(),
        candidates=(candidate,),
        metrics=AdvisoryPriceProspectiveSettlementMetricsV1(
            candidate_count=1,
            market_available_count=1,
            not_applicable_count=0,
            model_available_market_available_count=1,
            model_unavailable_count=0,
            calibrated_coverage=1.0,
            lower_miss_rate=0.0,
            upper_miss_rate=0.0,
            mean_interval_width_bps=400.0,
            median_interval_width_bps=400.0,
            mean_absolute_mid_error_bps=100.0,
            median_absolute_mid_error_bps=100.0,
        ),
    )
    assert settlement.settlement_id == f"advprsett_{settlement.settlement_sha256[:24]}"
    with pytest.raises(ValidationError, match="settlement identity mismatch"):
        AdvisoryPriceProspectiveSettlementV1.model_validate(
            {**settlement.model_dump(mode="json"), "package_id": "tampered"}
        )


def test_settlement_contract_enforces_target_maturity_clock() -> None:
    candidate = _available()
    with pytest.raises(ValidationError, match="maturity clock"):
        build_settlement(
            request_id="advprpros_" + "1" * 24,
            request_sha256="1" * 64,
            prediction_bundle_id="2" * 64,
            prediction_sha256="3" * 64,
            price_range_bundle_id="4" * 64,
            package_id="pkg",
            review_policy_sha256="5" * 64,
            decision_as_of_trade_date=date(2026, 9, 14),
            target_trade_date=date(2026, 9, 15),
            predicted_at=datetime(2026, 9, 14, 19, tzinfo=timezone.utc),
            settled_at=datetime(2026, 9, 15, 9, 59, tzinfo=timezone.utc),
            refresh_audits=_audits(),
            candidates=(candidate,),
            metrics=AdvisoryPriceProspectiveSettlementMetricsV1(
                candidate_count=1,
                market_available_count=1,
                not_applicable_count=0,
                model_available_market_available_count=1,
                model_unavailable_count=0,
                calibrated_coverage=1.0,
                lower_miss_rate=0.0,
                upper_miss_rate=0.0,
                mean_interval_width_bps=400.0,
                median_interval_width_bps=400.0,
                mean_absolute_mid_error_bps=100.0,
                median_absolute_mid_error_bps=100.0,
            ),
        )


def test_candidate_keeps_model_and_market_availability_orthogonal() -> None:
    unavailable_model = AdvisoryPriceProspectiveOutcomeCandidateV1(
        symbol="000002.SZ",
        model_prediction_status="UNAVAILABLE",
        model_prediction_reason="NORMAL_MISSING",
        market_outcome_status="AVAILABLE",
        market_outcome_reason="target_open_observed",
        actual_open=8.0,
    )
    suspended = AdvisoryPriceProspectiveOutcomeCandidateV1(
        symbol="000003.SZ",
        model_prediction_status="AVAILABLE",
        market_outcome_status="NOT_APPLICABLE",
        market_outcome_reason="target_authoritatively_suspended",
    )
    assert unavailable_model.actual_open == 8.0
    assert suspended.actual_open is None
    with pytest.raises(ValidationError, match="unavailable model prediction"):
        AdvisoryPriceProspectiveOutcomeCandidateV1(
            **{**unavailable_model.model_dump(), "calibrated_low": 7.0},
        )


def test_confirmation_does_not_expose_metrics_while_accumulating() -> None:
    confirmation = build_confirmation(
        status="ACCUMULATING",
        price_range_bundle_id=SHA,
        package_id=None,
        review_policy_sha256=None,
        target_trade_dates=(),
        target_date_count=0,
        available_candidate_count=0,
        not_applicable_count=0,
        support_date_count=0,
        support_date_ratio=0.0,
        support_gaps=("target_dates:0/20",),
        metrics=None,
        cluster_bootstrap=None,
    )
    assert confirmation.activation_recommended is False
    with pytest.raises(ValidationError, match="cannot expose result metrics"):
        build_confirmation(
            **confirmation.model_dump(exclude={"confirmation_sha256", "metrics"}),
            metrics={"calibrated_coverage": 1.0},
        )
