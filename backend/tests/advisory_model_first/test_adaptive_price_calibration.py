from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

from backend.services.advisory_model_first.adaptive_price_calibration import (
    AdvisoryAdaptivePriceCalibrationService,
    _apply_delta,
    _cluster_bootstrap,
    _fit_delta,
    _mature_fit_rows,
    _navigation_gate,
    prepare_adaptive_price_calibration_request,
    read_adaptive_price_calibration_artifact,
)
from backend.services.advisory_model_first.adaptive_price_calibration_contracts import (
    ADAPTIVE_ARM_ID,
    AdaptivePriceArmMetricsV1,
    AdaptivePriceBootstrapV1,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.historical_price_replay import (
    AdvisoryHistoricalPriceReplayService,
    HistoricalPriceDaySnapshot,
    read_historical_price_replay_artifact,
)
from backend.services.advisory_model_first.historical_price_replay_contracts import (
    AdvisoryHistoricalPriceOutcomeRowV1,
    build_historical_price_replay_request,
)
from backend.services.advisory_model_first.prediction_source import sha256_file
from backend.services.advisory_model_first.realtime_feature_source import (
    PriceRangeRealtimeContext,
)


class _DailySource:
    def __init__(
        self,
        *,
        open_value: float = 10.06,
        decision_close: float = 10.0,
        suspended: tuple[date, str] | None = None,
        missing: tuple[date, str] | None = None,
        tick_size: float = 0.01,
    ) -> None:
        self.open_value = open_value
        self.decision_close = decision_close
        self.suspended = suspended
        self.missing = missing
        self.tick_size = tick_size
        self.calls = 0

    def load_day(self, *, symbols, decision_trade_date, target_trade_date, pit_universe_key):
        assert pit_universe_key == "canonical-pit-v1"
        self.calls += 1
        suspended = (
            frozenset({self.suspended[1]})
            if self.suspended is not None and self.suspended[0] == decision_trade_date
            else frozenset()
        )
        missing = (
            {self.missing[1]: "target_market_row_missing_unexplained"}
            if self.missing is not None and self.missing[0] == decision_trade_date
            else {}
        )
        excluded = suspended | frozenset(missing)
        contexts = {
            symbol: PriceRangeRealtimeContext(
                symbol=symbol,
                decision_raw_close=self.decision_close,
                decision_price_trade_date=decision_trade_date,
                decision_price_source="market.kline_daily_raw.close_li",
                price_unit_divisor=1000.0,
                target_raw_price_multiplier=1.0,
                corporate_action_source="none",
                board_type="MAIN",
                list_date=date(2020, 1, 1),
                listed_trading_days=1000,
                target_is_st=False,
                tick_size=self.tick_size,
            )
            for symbol in symbols
            if symbol not in excluded
        }
        return HistoricalPriceDaySnapshot(
            contexts=contexts,
            context_unavailable={},
            raw_open_by_symbol={symbol: self.open_value for symbol in symbols if symbol not in excluded},
            suspended_symbols=suspended,
            market_unavailable=missing,
        )


def _make_source_replay(
    tmp_path: Path,
    *,
    source: _DailySource | None = None,
) -> tuple[Path, Path, tuple[date, ...]]:
    model_root = tmp_path / "models"
    source_path = tmp_path / "predictions.parquet"
    decisions = tuple(value.date() for value in pd.bdate_range("2026-01-05", periods=66))
    targets = (*decisions[1:], (pd.Timestamp(decisions[-1]) + pd.offsets.BDay(1)).date())
    rows = []
    for decision_day, target_day in zip(decisions, targets, strict=True):
        for index in range(20):
            rows.append(
                {
                    "decision_as_of_trade_date": decision_day,
                    "target_trade_date": target_day,
                    "instrument": f"{index + 1:06d}.SZ",
                    "entry_gap_return": 99.0,
                    "entry_gap_calibrated_q10": -0.0051,
                    "entry_gap_calibrated_q50": 0.0,
                    "entry_gap_calibrated_q90": 0.0051,
                }
            )
    pd.DataFrame(rows).to_parquet(source_path, index=False)
    request = build_historical_price_replay_request(
        price_range_bundle_id="a" * 64,
        price_range_manifest_sha256="b" * 64,
        prediction_source_sha256=sha256_file(source_path),
        decision_start_trade_date=decisions[0],
        decision_end_trade_date=decisions[-1],
        replay_as_of_date=targets[-1] + timedelta(days=7),
        pit_universe_key="canonical-pit-v1",
    )
    AdvisoryHistoricalPriceReplayService(
        data_source=source or _DailySource(),
        now_provider=lambda: datetime(2026, 5, 1, tzinfo=timezone.utc),
    ).run(request=request, prediction_source_path=source_path, output_root=model_root)
    replay_root = model_root / "price_range_historical_replays" / request.replay_id
    return model_root, replay_root, decisions


def _request(model_root: Path, replay_root: Path, tmp_path: Path):
    return prepare_adaptive_price_calibration_request(
        source_replay_root=replay_root,
        output_root=model_root,
        registry_path=tmp_path / "n0" / "advisory_research_trial_registry_v1.jsonl",
        repository_commit="c" * 40,
    )


def test_adaptive_pipeline_uses_past_only_residuals_and_selects_one_candidate(tmp_path) -> None:
    model_root, replay_root, _ = _make_source_replay(tmp_path)
    data_source = _DailySource()
    delivery = AdvisoryAdaptivePriceCalibrationService(
        data_source=data_source,
        now_provider=lambda: datetime(2026, 9, 16, tzinfo=timezone.utc),
    ).run(request=_request(model_root, replay_root, tmp_path))

    result = delivery.artifact.result
    assert delivery.artifact.receipt.status == "PUBLISHED"
    assert data_source.calls == 66
    assert len(result.daily_states) == 66
    assert [item.mode for item in result.daily_states[:5]] == ["WARMUP_STATIC_FALLBACK"] * 5
    assert all(item.mode == "ADAPTIVE_ACTIVE" for item in result.daily_states[5:])
    assert result.daily_states[4].fit_target_date_count == 4
    assert result.daily_states[5].fit_target_date_count == 5
    assert result.daily_states[-1].fit_target_date_count == 20
    assert result.daily_states[5].delta == pytest.approx(0.0009)
    assert result.static_active.model_coverage == 0.0
    assert result.adaptive_active.model_coverage == 1.0
    assert result.static_active.business_coverage == 1.0
    assert result.adaptive_active.business_coverage == 1.0
    assert result.gate.all_pass is True
    assert result.gate.selected_candidate == ADAPTIVE_ARM_ID
    assert delivery.artifact.receipt.selected_trial_count == 1
    assert delivery.artifact.receipt.activation_recommended is False
    assert delivery.registry_delivery["appended_count"] == 1
    assert result.bootstrap.cluster_count == 61


def test_publish_is_immutable_and_exact_retry_is_noop(tmp_path) -> None:
    model_root, replay_root, _ = _make_source_replay(tmp_path)
    request = _request(model_root, replay_root, tmp_path)
    data_source = _DailySource()
    service = AdvisoryAdaptivePriceCalibrationService(data_source=data_source)
    first = service.run(request=request)
    calls_after_first = data_source.calls
    second = service.run(request=request)
    assert second.artifact.receipt.status == "ALREADY_MATERIALIZED"
    assert second.artifact.receipt.receipt_sha256 == first.artifact.receipt.receipt_sha256
    assert second.registry_delivery["appended_count"] == 0
    assert second.registry_delivery["duplicate_noop_count"] == 1
    assert data_source.calls == calls_after_first

    later_created_at = request.model_copy(
        update={"created_at": request.created_at + timedelta(minutes=5)}
    )
    third = service.run(request=later_created_at)
    assert third.artifact.receipt.status == "ALREADY_MATERIALIZED"
    assert third.registry_delivery["duplicate_noop_count"] == 1

    result_path = first.artifact.path / "result.json"
    payload = json.loads(result_path.read_text(encoding="utf-8"))
    payload["coverage_error_improvement"] = -99.0
    result_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError) as raised:
        read_adaptive_price_calibration_artifact(first.artifact.path)
    assert raised.value.reason_code == "ADVISORY_ADAPTIVE_PRICE_ARTIFACT_CONFLICT"


def test_future_outcome_poison_does_not_change_earlier_predictions(tmp_path) -> None:
    _, replay_root, decisions = _make_source_replay(tmp_path)
    artifact = read_historical_price_replay_artifact(replay_root)
    parsed = tuple(
        AdvisoryHistoricalPriceOutcomeRowV1.model_validate(item)
        for item in artifact.outcome["rows"]
    )
    rows = tuple(
        item
        for item in parsed
        if item.model_prediction_status == "AVAILABLE" and item.market_outcome_status == "AVAILABLE"
    )
    decision_day = decisions[10]
    baseline, baseline_dates = _mature_fit_rows(
        rows, decision_date=decision_day, lookback_target_dates=20
    )
    future_index = next(index for index, item in enumerate(rows) if item.target_trade_date > decision_day)
    poisoned = list(rows)
    poisoned[future_index] = poisoned[future_index].model_copy(
        update={"actual_entry_gap_return": 0.5}
    )
    candidate, candidate_dates = _mature_fit_rows(
        poisoned, decision_date=decision_day, lookback_target_dates=20
    )
    assert [item.actual_entry_gap_return for item in candidate] == [
        item.actual_entry_gap_return for item in baseline
    ]
    assert candidate_dates == baseline_dates
    assert _fit_delta(candidate, nominal_coverage=0.8) == _fit_delta(
        baseline, nominal_coverage=0.8
    )


def test_source_drift_fails_closed(tmp_path) -> None:
    model_root, replay_root, _ = _make_source_replay(tmp_path)
    with pytest.raises(AdvisoryModelFirstError) as raised:
        AdvisoryAdaptivePriceCalibrationService(data_source=_DailySource(open_value=10.07)).run(
            request=_request(model_root, replay_root, tmp_path)
        )
    assert raised.value.reason_code == "ADVISORY_ADAPTIVE_PRICE_SOURCE_DRIFT"


def test_price_context_drift_fails_closed(tmp_path) -> None:
    model_root, replay_root, _ = _make_source_replay(tmp_path)
    with pytest.raises(AdvisoryModelFirstError) as raised:
        AdvisoryAdaptivePriceCalibrationService(
            data_source=_DailySource(decision_close=10.01)
        ).run(request=_request(model_root, replay_root, tmp_path))
    assert raised.value.reason_code == "ADVISORY_ADAPTIVE_PRICE_SOURCE_DRIFT"


def test_static_business_projection_drift_fails_closed(tmp_path) -> None:
    model_root, replay_root, _ = _make_source_replay(tmp_path)
    with pytest.raises(AdvisoryModelFirstError) as raised:
        AdvisoryAdaptivePriceCalibrationService(
            data_source=_DailySource(tick_size=0.03)
        ).run(request=_request(model_root, replay_root, tmp_path))
    assert raised.value.reason_code == "ADVISORY_ADAPTIVE_PRICE_SOURCE_DRIFT"
    assert raised.value.context["field"] in {
        "calibrated_low",
        "calibrated_high",
        "interval_width_bps",
    }


def test_dataset_identity_drift_fails_closed(tmp_path) -> None:
    model_root, replay_root, _ = _make_source_replay(tmp_path)
    request = _request(model_root, replay_root, tmp_path).model_copy(
        update={"dataset_identity": "f" * 64}
    )
    with pytest.raises(AdvisoryModelFirstError) as raised:
        AdvisoryAdaptivePriceCalibrationService(data_source=_DailySource()).run(request=request)
    assert raised.value.reason_code == "ADVISORY_ADAPTIVE_PRICE_SOURCE_INVALID"
    assert raised.value.context == {"field": "dataset_identity"}


def test_suspend_and_missing_rows_are_preserved(tmp_path) -> None:
    decisions = tuple(value.date() for value in pd.bdate_range("2026-01-05", periods=66))
    suspended = (decisions[10], "000001.SZ")
    missing = (decisions[20], "000020.SZ")
    source = _DailySource(suspended=suspended, missing=missing)
    model_root, replay_root, _ = _make_source_replay(tmp_path, source=source)
    delivery = AdvisoryAdaptivePriceCalibrationService(
        data_source=_DailySource(suspended=suspended, missing=missing)
    ).run(request=_request(model_root, replay_root, tmp_path))
    result = delivery.artifact.result
    assert result.adaptive_all.supported_row_count == 66 * 20 - 2
    assert result.static_all.supported_row_count == 66 * 20 - 2
    assert result.adaptive_all.not_applicable_count == 1
    assert result.adaptive_all.market_unavailable_count == 1
    assert result.static_all.not_applicable_count == 1
    assert result.static_all.market_unavailable_count == 1


def test_adaptive_expansion_preserves_median_and_order() -> None:
    from backend.services.advisory_model_first.historical_price_replay_contracts import (
        AdvisoryHistoricalPricePredictionRowV1,
    )

    original = AdvisoryHistoricalPricePredictionRowV1(
        decision_as_of_trade_date=date(2026, 1, 5),
        target_trade_date=date(2026, 1, 6),
        symbol="000001.SZ",
        calibrated_gap_q10=-0.01,
        calibrated_gap_q50=0.001,
        calibrated_gap_q90=0.02,
    )
    adjusted = _apply_delta(original, delta=0.003)
    assert adjusted.calibrated_gap_q10 == pytest.approx(-0.013)
    assert adjusted.calibrated_gap_q50 == original.calibrated_gap_q50
    assert adjusted.calibrated_gap_q90 == pytest.approx(0.023)


def _gate_metrics(*, candidate: bool) -> AdaptivePriceArmMetricsV1:
    return AdaptivePriceArmMetricsV1(
        decision_date_count=60,
        candidate_count=1200,
        market_available_count=1200,
        not_applicable_count=0,
        market_unavailable_count=0,
        supported_row_count=1200,
        model_unavailable_count=0,
        model_coverage=0.8 if candidate else 0.7,
        model_lower_miss_rate=0.1 if candidate else 0.15,
        model_upper_miss_rate=0.1 if candidate else 0.15,
        model_mean_width_bps=110.0 if candidate else 100.0,
        model_median_width_bps=110.0 if candidate else 100.0,
        model_mean_absolute_mid_error_bps=40.0,
        model_median_absolute_mid_error_bps=30.0,
        business_coverage=0.82 if candidate else 0.8,
        business_lower_miss_rate=0.09 if candidate else 0.1,
        business_upper_miss_rate=0.09 if candidate else 0.1,
        business_mean_width_bps=110.0 if candidate else 100.0,
        business_median_width_bps=110.0 if candidate else 100.0,
        business_mean_absolute_mid_error_bps=40.0,
        business_median_absolute_mid_error_bps=30.0,
    )


@pytest.mark.parametrize(
    ("override", "bootstrap_cluster_count", "bootstrap_lower"),
    [
        ({"coverage_error_improvement": 0.019}, 60, 0.01),
        ({"model_width_ratio": 1.251}, 60, 0.01),
        ({"business_width_ratio": 1.251}, 60, 0.01),
        ({}, 59, 0.01),
        ({}, 60, 0.0),
    ],
)
def test_navigation_gate_rejects_each_numeric_boundary(
    override, bootstrap_cluster_count, bootstrap_lower
) -> None:
    values = {
        "static": _gate_metrics(candidate=False),
        "adaptive": _gate_metrics(candidate=True),
        "coverage_error_improvement": 0.1,
        "model_width_ratio": 1.1,
        "business_width_ratio": 1.1,
        "bootstrap": AdaptivePriceBootstrapV1(
            cluster_count=bootstrap_cluster_count,
            point=0.1,
            lower_95=bootstrap_lower,
            upper_95=0.2,
        ),
    }
    values.update(override)
    gate = _navigation_gate(**values)
    assert gate.all_pass is False
    assert gate.selected_candidate is None


def test_fit_delta_requires_one_hundred_rows(tmp_path) -> None:
    _, replay_root, _ = _make_source_replay(tmp_path)
    artifact = read_historical_price_replay_artifact(replay_root)
    rows = tuple(
        AdvisoryHistoricalPriceOutcomeRowV1.model_validate(item)
        for item in artifact.outcome["rows"][:99]
    )
    with pytest.raises(AdvisoryModelFirstError) as raised:
        _fit_delta(rows, nominal_coverage=0.8)
    assert raised.value.reason_code == "ADVISORY_ADAPTIVE_PRICE_EVALUATION_FAILED"


def test_cluster_bootstrap_is_deterministic(tmp_path) -> None:
    _, replay_root, _ = _make_source_replay(tmp_path)
    artifact = read_historical_price_replay_artifact(replay_root)
    rows = tuple(
        AdvisoryHistoricalPriceOutcomeRowV1.model_validate(item)
        for item in artifact.outcome["rows"]
    )
    first = _cluster_bootstrap(static_rows=rows, adaptive_rows=rows)
    second = _cluster_bootstrap(static_rows=rows, adaptive_rows=rows)
    assert first == second


@pytest.mark.parametrize(
    "adaptive_updates",
    [
        {"model_coverage": 0.8, "model_lower_miss_rate": 0.16, "model_upper_miss_rate": 0.04},
        {"business_coverage": 0.79, "business_lower_miss_rate": 0.11, "business_upper_miss_rate": 0.10},
    ],
)
def test_navigation_gate_rejects_miss_or_business_regression(adaptive_updates) -> None:
    candidate = _gate_metrics(candidate=True)
    adaptive = AdaptivePriceArmMetricsV1.model_validate(
        {**candidate.model_dump(mode="python"), **adaptive_updates}
    )
    gate = _navigation_gate(
        static=_gate_metrics(candidate=False),
        adaptive=adaptive,
        coverage_error_improvement=0.1,
        model_width_ratio=1.1,
        business_width_ratio=1.1,
        bootstrap=AdaptivePriceBootstrapV1(
            cluster_count=60,
            point=0.1,
            lower_95=0.01,
            upper_95=0.2,
        ),
    )
    assert gate.all_pass is False
