from __future__ import annotations

from datetime import UTC, date, datetime
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.advisory_forward.errors import AdvisoryForwardModelEvaluationError
from backend.services.advisory_forward.evaluation import (
    AdvisoryForwardEvaluationMarketData,
    REASON_MARKET_UNAVAILABLE,
    REASON_SEQUENCE_INCOMPLETE,
    _daily_frame,
    build_forward_model_evaluation,
)
from backend.services.advisory_forward.service import AdvisoryForwardService
from backend.services.advisory_model_first.model_binding_resolution import META_LABEL_MODEL_ROLE
from backend.services.advisory_model_first.policy_contracts import AdvisoryPolicyCostV1
from backend.services.selection_center.models import SelectionRunStatus
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


PROGRAM_ID = "advp_forward_eval"
DESCRIPTOR = "d" * 64
BUNDLE = "e" * 64
POLICY = {
    "target_count": 5,
    "rank_enter_threshold": 5,
    "rank_exit_threshold": 40,
    "rank_exit_confirm_days": 1,
    "daily_replacement_budget": 5,
    "stop_loss_bps": 100000,
    "take_profit_bps": 100000,
    "trailing_stop_bps": 100000,
    "time_stop_days": 1,
    "take_profit_mode": "trailing",
    "entry_price_basis": "next_open_executable",
    "exit_price_basis": "next_open_executable",
}
COST = AdvisoryPolicyCostV1(buy_cost_bps=3.0, sell_cost_bps=13.0)
POLICY_SHA = canonical_json_sha256(POLICY)


def _observations() -> list[dict]:
    decisions = [date(2026, 1, 2), date(2026, 1, 5), date(2026, 1, 6)]
    targets = [date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7)]
    maturities = [date(2026, 1, 6), date(2026, 1, 7), date(2026, 1, 8)]
    rows = []
    for index, (decision, target, maturity) in enumerate(zip(decisions, targets, maturities, strict=True)):
        candidates = [
            {
                "symbol": f"{rank:06d}.SZ",
                "selection_effective_rank": rank,
                "selection_exit_rank": rank,
                "entry_priority_rank": rank,
                "advisory_model_rank": rank,
                "is_top5": rank <= 5,
            }
            for rank in range(1, 21)
        ]
        prediction = {
            "status": "EXPERIMENTAL_SHADOW",
            "model_role": META_LABEL_MODEL_ROLE,
            "evaluation_contract_version": "advisory_forward_model_evaluation_v1",
            "shadow_policy": POLICY,
            "shadow_policy_sha256": POLICY_SHA,
            "cost_policy": COST.model_dump(mode="json"),
            "cost_policy_sha256": COST.policy_sha256,
            "candidates": candidates,
        }
        rows.append(
            {
                "observation_id": f"advobs_{index}",
                "forward_run_id": f"advfwd_{index}",
                "program_id": PROGRAM_ID,
                "status": "EXPERIMENTAL_SHADOW",
                "decision_as_of_trade_date": decision,
                "target_trade_date": target,
                "maturity_trade_date": maturity,
                "model_descriptor_sha256": DESCRIPTOR,
                "bundle_id": BUNDLE,
                "selection_run_id": f"selection_{index}",
                "prediction_payload_json": prediction,
                "payload_sha256": canonical_json_sha256({"index": index, "prediction": prediction}),
            }
        )
    return rows


def _selection_runs(*, missing_rank_40: bool = False) -> dict[str, SimpleNamespace]:
    runs = {}
    for index, target in enumerate((date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7))):
        stop = 40 if missing_rank_40 and index == 0 else 41
        candidates = [
            SimpleNamespace(symbol=f"{rank:06d}.SZ", rank=rank, score=1.0 - rank / 100.0)
            for rank in range(1, stop)
        ]
        runs[f"selection_{index}"] = SimpleNamespace(
            status=SelectionRunStatus.SUCCEEDED,
            trade_date=target,
            aggregate_results=candidates,
        )
    return runs


def _market(*, include_future_poison: bool = False) -> AdvisoryForwardEvaluationMarketData:
    calendar = pd.DatetimeIndex(pd.to_datetime(["2026-01-02", "2026-01-05", "2026-01-06", "2026-01-07"]))
    rows = []
    for day_index, day in enumerate(calendar[1:]):
        for rank in range(1, 41):
            rows.append(
                {
                    "datetime": day,
                    "instrument": f"{rank:06d}.SZ",
                    "factor": 1.0,
                    "open": 10.0 + day_index * 0.1 + rank / 1000.0,
                    "high": 10.2 + day_index * 0.1 + rank / 1000.0,
                    "low": 9.8 + day_index * 0.1 + rank / 1000.0,
                    "close": 10.1 + day_index * 0.1 + rank / 1000.0,
                    "up_limit_price": 20.0,
                    "down_limit_price": 1.0,
                    "limit_up": 0.0,
                    "limit_down": 0.0,
                }
            )
    if include_future_poison:
        for rank in range(1, 41):
            rows.append(
                {
                    "datetime": pd.Timestamp("2026-01-08"),
                    "instrument": f"{rank:06d}.SZ",
                    "factor": 99.0,
                    "open": 9999.0,
                    "high": 9999.0,
                    "low": 9999.0,
                    "close": 9999.0,
                    "up_limit_price": 9999.0,
                    "down_limit_price": 1.0,
                    "limit_up": 1.0,
                    "limit_down": 0.0,
                }
            )
    daily = pd.DataFrame(rows).set_index(["datetime", "instrument"]).sort_index()
    benchmark = pd.DataFrame(
        {
            "datetime": calendar[1:],
            "instrument": "000300.SH",
            "open": [100.0, 101.0, 102.0],
        }
    ).set_index(["datetime", "instrument"])
    return AdvisoryForwardEvaluationMarketData(
        daily=daily,
        benchmark_daily=benchmark,
        suspend_rows=pd.DataFrame(columns=["trade_date", "instrument", "suspend_type"]),
        trading_calendar=calendar,
        input_sha256="f" * 64,
    )


def test_forward_model_evaluation_replays_exact_top5_policy_and_builds_mature_outcomes() -> None:
    built = build_forward_model_evaluation(
        observations=_observations(),
        selection_runs=_selection_runs(),
        market=_market(),
        as_of_trade_date=date(2026, 1, 7),
    )

    assert built.evaluation.due_observation_count == 2
    assert built.evaluation.observation_count == 3
    assert built.evaluation.metrics_json["coverage"] == 1.0
    assert built.evaluation.metrics_json["completed_episode_hit_rate"] is not None
    assert {outcome.observation_id for outcome in built.new_outcomes} == {"advobs_0", "advobs_1"}
    assert {outcome.status for outcome in built.new_outcomes} == {"MATURED", "NO_ENTRY"}
    assert not built.unresolved_observation_ids


def test_forward_model_evaluation_ignores_market_rows_after_explicit_watermark() -> None:
    baseline = build_forward_model_evaluation(
        observations=_observations(),
        selection_runs=_selection_runs(),
        market=_market(),
        as_of_trade_date=date(2026, 1, 7),
    )
    poisoned = build_forward_model_evaluation(
        observations=_observations(),
        selection_runs=_selection_runs(),
        market=_market(include_future_poison=True),
        as_of_trade_date=date(2026, 1, 7),
    )

    assert poisoned.evaluation.metrics_json == baseline.evaluation.metrics_json
    assert poisoned.evaluation.result_payload_json == baseline.evaluation.result_payload_json
    assert poisoned.evaluation.payload_sha256() == baseline.evaluation.payload_sha256()


def test_forward_model_evaluation_identity_isolates_descriptor_switch_back_epochs() -> None:
    first_epoch = build_forward_model_evaluation(
        observations=_observations(),
        selection_runs=_selection_runs(),
        market=_market(),
        as_of_trade_date=date(2026, 1, 7),
    )
    switch_back_observations = _observations()
    switch_back_observations[0] = {
        **switch_back_observations[0],
        "observation_id": "advobs_switch_back_epoch",
    }
    switch_back_epoch = build_forward_model_evaluation(
        observations=switch_back_observations,
        selection_runs=_selection_runs(),
        market=_market(),
        as_of_trade_date=date(2026, 1, 7),
    )

    assert first_epoch.evaluation.model_descriptor_sha256 == switch_back_epoch.evaluation.model_descriptor_sha256
    assert first_epoch.evaluation.as_of_trade_date == switch_back_epoch.evaluation.as_of_trade_date
    assert first_epoch.evaluation.evaluation_id != switch_back_epoch.evaluation.evaluation_id


def test_forward_model_evaluation_closes_old_epoch_without_new_entries_after_descriptor_switch() -> None:
    old_epoch = _observations()[:2]
    built = build_forward_model_evaluation(
        observations=old_epoch,
        rank_contexts=_observations(),
        selection_runs=_selection_runs(),
        market=_market(),
        as_of_trade_date=date(2026, 1, 7),
    )

    entry_signal_dates = {
        episode["entry_signal_date"]
        for episode in built.evaluation.result_payload_json["episodes"]
    }
    assert entry_signal_dates <= {"2026-01-02", "2026-01-05"}


def test_forward_model_evaluation_rejects_incomplete_top40_exit_context() -> None:
    with pytest.raises(AdvisoryForwardModelEvaluationError) as excinfo:
        build_forward_model_evaluation(
            observations=_observations(),
            selection_runs=_selection_runs(missing_rank_40=True),
            market=_market(),
            as_of_trade_date=date(2026, 1, 7),
        )

    assert excinfo.value.reason_code == REASON_SEQUENCE_INCOMPLETE


def test_forward_model_evaluation_rejects_missing_top40_market_row() -> None:
    market = _market()
    incomplete_daily = market.daily.drop(index=(pd.Timestamp("2026-01-06"), "000040.SZ"))
    incomplete_market = AdvisoryForwardEvaluationMarketData(
        daily=incomplete_daily,
        benchmark_daily=market.benchmark_daily,
        suspend_rows=market.suspend_rows,
        trading_calendar=market.trading_calendar,
        input_sha256=market.input_sha256,
    )

    with pytest.raises(AdvisoryForwardModelEvaluationError) as excinfo:
        build_forward_model_evaluation(
            observations=_observations(),
            selection_runs=_selection_runs(),
            market=incomplete_market,
            as_of_trade_date=date(2026, 1, 7),
        )

    assert excinfo.value.reason_code == REASON_MARKET_UNAVAILABLE


def test_forward_model_market_rows_reject_missing_limit_evidence() -> None:
    row = {
        "trade_date": date(2026, 1, 5),
        "ts_code": "000001.SZ",
        "open_li": 100000,
        "high_li": 101000,
        "low_li": 99000,
        "close_li": 100500,
        "adj_factor": 1,
        "base_adj_factor": 1,
        "pre_close": 9.9,
        "up_limit": None,
        "down_limit": 8.9,
    }

    with pytest.raises(AdvisoryForwardModelEvaluationError) as excinfo:
        _daily_frame([row])

    assert excinfo.value.reason_code == REASON_MARKET_UNAVAILABLE


class _EvaluationRepository:
    def __init__(self) -> None:
        self.rows = _observations()
        self.committed = None
        self.failure = None

    def pending_settlements(self, **_kwargs):
        return []

    def pending_mature_model_observations(self, **_kwargs):
        return [self.rows[0]]

    def model_forward_timeline(self, **_kwargs):
        return self.rows

    def get_model_evaluation(self, **_kwargs):
        return None

    def model_outcome_observation_ids(self, **_kwargs):
        return set()

    def commit_model_evaluation(self, **kwargs):
        self.committed = kwargs
        return {"evaluation_id": kwargs["evaluation"].evaluation_id}

    def retryable_model_observations(self, **_kwargs):
        return []

    def mark_model_evaluation_failure(self, **kwargs):
        self.failure = kwargs


def test_forward_service_advances_mature_evaluation_without_publication_side_effects() -> None:
    repository = _EvaluationRepository()
    selection_runs = _selection_runs()
    program_service = SimpleNamespace(
        selection_service=SimpleNamespace(
            get_run=lambda run_id: selection_runs[run_id],
            refresh_audit=SimpleNamespace(require_success=lambda **_kwargs: {"status": "SUCCEEDED"}),
        )
    )
    service = AdvisoryForwardService(
        repository=repository,
        program_service=program_service,
        evaluation_market_source=SimpleNamespace(load=lambda **_kwargs: _market()),
        calendar=SimpleNamespace(is_trading_day=lambda _value: True),
        now_provider=lambda: datetime(2026, 1, 7, 2, 30, tzinfo=UTC),
    )

    result = service.run_once()

    assert result["publication_due"] is False
    assert result["results"][0]["status"] == "MODEL_EVALUATION_READY"
    assert repository.committed is not None
    assert repository.committed["evaluation"].due_observation_count == 2


def test_forward_service_keeps_missing_market_audit_visible_without_running_market_query() -> None:
    repository = _EvaluationRepository()
    selection_runs = _selection_runs()
    market_load_count = 0

    def _unexpected_market_load(**_kwargs):
        nonlocal market_load_count
        market_load_count += 1
        return _market()

    program_service = SimpleNamespace(
        selection_service=SimpleNamespace(
            get_run=lambda run_id: selection_runs[run_id],
            refresh_audit=SimpleNamespace(
                require_success=lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("audit missing"))
            ),
        )
    )
    service = AdvisoryForwardService(
        repository=repository,
        program_service=program_service,
        evaluation_market_source=SimpleNamespace(load=_unexpected_market_load),
        calendar=SimpleNamespace(is_trading_day=lambda _value: True),
        now_provider=lambda: datetime(2026, 1, 7, 2, 30, tzinfo=UTC),
    )

    result = service.run_once()

    assert result["results"][0]["status"] == "WAITING_DATA"
    assert result["results"][0]["reason_code"] == "ADVISORY_FORWARD_MODEL_EVALUATION_MARKET_DATA_UNAVAILABLE"
    assert repository.failure["waiting_data"] is True
    assert market_load_count == 0


def test_forward_service_isolates_evaluation_discovery_failure_from_run_once() -> None:
    repository = _EvaluationRepository()
    repository.pending_mature_model_observations = lambda **_kwargs: (_ for _ in ()).throw(
        RuntimeError("evaluation schema unavailable")
    )
    service = AdvisoryForwardService(
        repository=repository,
        program_service=SimpleNamespace(),
        calendar=SimpleNamespace(is_trading_day=lambda _value: True),
        now_provider=lambda: datetime(2026, 1, 7, 2, 30, tzinfo=UTC),
    )

    result = service.run_once()

    assert result["publication_due"] is False
    assert result["results"][0]["stage"] == "MODEL_EVALUATION_DISCOVERY"
    assert result["results"][0]["status"] == "FAILED"


def test_forward_service_isolates_evaluation_failure_persistence_error() -> None:
    repository = _EvaluationRepository()
    repository.mark_model_evaluation_failure = lambda **_kwargs: (_ for _ in ()).throw(
        RuntimeError("evaluation failure write unavailable")
    )
    selection_runs = _selection_runs()
    program_service = SimpleNamespace(
        selection_service=SimpleNamespace(
            get_run=lambda run_id: selection_runs[run_id],
            refresh_audit=SimpleNamespace(
                require_success=lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("audit missing"))
            ),
        )
    )
    service = AdvisoryForwardService(
        repository=repository,
        program_service=program_service,
        evaluation_market_source=SimpleNamespace(load=lambda **_kwargs: _market()),
        calendar=SimpleNamespace(is_trading_day=lambda _value: True),
        now_provider=lambda: datetime(2026, 1, 7, 2, 30, tzinfo=UTC),
    )

    result = service.run_once()

    assert result["publication_due"] is False
    assert result["results"][0]["stage"] == "MODEL_EVALUATION"
    assert result["results"][0]["failure_recorded"] is False
