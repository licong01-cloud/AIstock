from __future__ import annotations

import pytest

from backend.services.advisory_quality import AdvisoryDiagnosticRecord, generate_quality_report
from backend.services.trading_core.errors import RuntimeConfigInvalidError
from backend.services.trading_core.price_guard import STOP_LOSS_TRIGGERED


def _record(idx: int, *, score_bucket: str = "top5", liquidity_bucket: str = "liq_hi") -> AdvisoryDiagnosticRecord:
    return AdvisoryDiagnosticRecord(
        code=f"000{idx:03d}.SZ",
        trade_date="2026-06-02",
        current_price=10.0 if idx % 2 == 0 else 10.4,
        day_low=9.9 if idx % 2 == 0 else (10.2 if idx == 1 else 10.3),
        entry_band_json={"max_buy_price": 10.2, "green": {"max_price": 10.1}, "yellow": {"max_price": 10.2}},
        action="HOLD" if idx % 3 else "SKIP",
        reason_code=STOP_LOSS_TRIGGERED if idx % 5 == 0 else "HOLD",
        score_bucket=score_bucket,
        gap_bucket="gap_0_200",
        regime="bull",
        liquidity_bucket=liquidity_bucket,
        board_type="MAIN",
        decision_input_json={"current_price": 10.0, "score_bucket": score_bucket},
        forward_alpha_bps=50.0 if idx % 2 == 0 else -20.0,
        stop_saved_loss_bps=30.0 if idx % 5 == 0 else None,
        stop_whipsaw_cost_bps=5.0 if idx % 7 == 0 else None,
        realized_reward_bps=60.0,
        realized_risk_bps=-30.0,
    )


def test_s1_11_quality_report_metrics_buckets_and_parent_shrink() -> None:
    records = [_record(1, liquidity_bucket="small_bucket"), _record(2, liquidity_bucket="large_bucket"), _record(3, liquidity_bucket="large_bucket")]

    report = generate_quality_report(records, min_bucket_size=2)

    assert report["report_type"] == "post_decision_diagnostics"
    assert report["validated_pnl"] is False
    assert report["pnl_label"] == "not_validated_pnl"
    metrics = report["metrics"]
    assert metrics["sample_count"] == 3
    assert metrics["entry_zone_hit_rate"] == pytest.approx(1 / 3)
    assert metrics["entry_zone_fillable_rate"] == pytest.approx(2 / 3)
    assert "stop_saved_loss_bps" in metrics
    assert "reward_risk_realized" in metrics
    assert any(bucket["shrunk_to_parent"] for bucket in report["buckets"])
    assert any("selection bias" in warning for warning in report["warnings"])


def test_s1_11_quality_report_rejects_future_fields_in_decision_inputs() -> None:
    bad = _record(1)
    bad = AdvisoryDiagnosticRecord(**{**bad.__dict__, "decision_input_json": {"forward_return_bps": 100}})

    with pytest.raises(RuntimeConfigInvalidError, match="future outcome fields"):
        generate_quality_report([bad])


def test_s1_11_quality_report_is_reproducible() -> None:
    records = [_record(idx) for idx in range(1, 5)]

    assert generate_quality_report(records, min_bucket_size=2) == generate_quality_report(records, min_bucket_size=2)
