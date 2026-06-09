from __future__ import annotations

from datetime import date, timedelta
import inspect

import pytest

from backend.services.advisory_program import (
    ACTION_ENTER,
    ACTION_HOLD,
    EXIT_ALPHA_RANK_DROP,
    EXIT_REPLACEMENT_BUDGET,
    EXIT_STOP_LOSS,
    EXIT_STOP_LOSS_DEFERRED_T1,
    EXIT_TIME_STOP,
    PACKAGE_MODE_FUSION,
    PACKAGE_MODE_SINGLE,
    PRICE_BASIS_NEXT_OPEN,
    AdvisoryProgramService,
    InMemoryAdvisoryProgramRepository,
)
from backend.services.selection_center.models import SelectionCandidate, SelectionMode, SelectionRun, SelectionRunStatus
from backend.services.trading_core.errors import RuntimeConfigInvalidError, UnsupportedFeatureError


class FakeTradingCalendar:
    def __init__(self, trading_days: list[date]) -> None:
        self.trading_days = trading_days
        self.requests: list[tuple[date, date]] = []

    def list_trading_days(self, start_date: date, end_date: date) -> list[date]:
        self.requests.append((start_date, end_date))
        return [day for day in self.trading_days if start_date <= day <= end_date]

    def next_trading_day(self, anchor_date: date, *, inclusive: bool = False) -> date:
        start = anchor_date if inclusive else anchor_date + timedelta(days=1)
        eligible = [day for day in self.trading_days if day >= start]
        return eligible[0] if eligible else start


def _calendar_days(start_date: date, end_date: date) -> list[date]:
    current = start_date
    days: list[date] = []
    while current <= end_date:
        days.append(current)
        current += timedelta(days=1)
    return days


def _service() -> tuple[AdvisoryProgramService, InMemoryAdvisoryProgramRepository]:
    repo = InMemoryAdvisoryProgramRepository()
    return AdvisoryProgramService(repository=repo, selection_service=None, calendar_provider=FakeTradingCalendar([])), repo


def _program(service: AdvisoryProgramService, *, target_count: int = 2, **policy_overrides):
    return service.create_program(
        program_name="Top advisory",
        package_mode=PACKAGE_MODE_SINGLE,
        package_ids=["pkg_a"],
        target_count=target_count,
        status="ENABLED",
        review_policy={
            "rank_enter_threshold": target_count,
            "rank_exit_threshold": target_count * 2,
            "rank_exit_confirm_days": 2,
            "daily_replacement_budget": 1,
            "stop_loss_bps": 800,
            "take_profit_bps": 1800,
            "trailing_stop_bps": 600,
            "time_stop_days": 20,
            **policy_overrides,
        },
    )


def _candidate(symbol: str, rank: int, price: float, *, score: float = 1.0) -> dict:
    return {
        "symbol": symbol,
        "rank": rank,
        "score": score,
        "reference_price": price,
        "next_open_executable": price,
        "component_scores": {"package_ranks": {"pkg_a": rank}},
    }


def test_advisory_program_create_validates_single_fusion_version_and_clone() -> None:
    service, _repo = _service()

    single = service.create_program(
        program_name="Single",
        package_mode=PACKAGE_MODE_SINGLE,
        package_ids=["pkg_a"],
    )
    fusion = service.create_program(
        program_name="Fusion",
        package_mode=PACKAGE_MODE_FUSION,
        package_ids=["pkg_a", "pkg_b"],
        package_weights={"pkg_a": 0.4, "pkg_b": 0.6},
    )

    assert single.fusion_policy_sha256 is None
    assert fusion.fusion_method == "weighted_rank_fusion"
    assert fusion.fusion_policy_sha256

    updated = service.update_program(single.program_id, {"target_count": 30})
    assert updated.version == single.version + 1

    cloned = service.clone_program(fusion.program_id, program_name="Fusion copy")
    assert cloned.program_id != fusion.program_id
    assert cloned.status == "DRAFT"
    assert cloned.package_ids == fusion.package_ids

    with pytest.raises(RuntimeConfigInvalidError):
        service.create_program(program_name="bad", package_mode=PACKAGE_MODE_SINGLE, package_ids=["pkg_a", "pkg_b"])
    with pytest.raises(UnsupportedFeatureError):
        service.create_program(program_name="future", package_mode="sleeve_mode_future", package_ids=["pkg_a", "pkg_b"])


def test_advisory_list_items_show_stock_name_and_effective_date_uses_candidate_data_day_next_trading_day() -> None:
    trading_days = [date(2026, 6, 5), date(2026, 6, 8), date(2026, 6, 9)]
    service = AdvisoryProgramService(
        repository=InMemoryAdvisoryProgramRepository(),
        selection_service=None,
        calendar_provider=FakeTradingCalendar(trading_days),
    )
    program = _program(service, target_count=1)

    result = service.run_review(
        program.program_id,
        trade_date=date(2026, 6, 8),
        candidates=[
            {
                "symbol": "000001.SZ",
                "stock_name": "平安银行",
                "rank": 1,
                "score": 0.9,
                "reference_price": 10,
                "next_open_executable": 10,
                "selection_entry_price_time": "2026-06-05",
            }
        ],
        preview=False,
    )

    assert result.active_pool[0].stock_name == "平安银行"
    assert result.active_pool[0].effective_entry_date == date(2026, 6, 8)
    assert result.list_items[0].stock_name == "平安银行"
    assert result.list_items[0].effective_trade_date == date(2026, 6, 8)
    detail = service.recommendation_list_version_detail(result.list_version_id or "")
    assert detail["items"][0]["stock_name"] == "平安银行"
    assert detail["items"][0]["symbol_name"] == "平安银行"


def test_top20_review_merges_active_pool_hysteresis_and_budget() -> None:
    service, _repo = _service()
    program = _program(service, target_count=2)

    first = service.run_review(
        program.program_id,
        trade_date=date(2026, 6, 1),
        candidates=[_candidate("000001.SZ", 1, 10), _candidate("000002.SZ", 2, 20)],
        preview=False,
    )
    assert [row.symbol for row in first.active_pool] == ["000001.SZ", "000002.SZ"]
    assert [row.action for row in first.decisions] == [ACTION_ENTER, ACTION_ENTER]

    second = service.run_review(
        program.program_id,
        trade_date=date(2026, 6, 2),
        candidates=[
            _candidate("000001.SZ", 5, 10),
            _candidate("000002.SZ", 5, 21),
            _candidate("000003.SZ", 1, 30),
        ],
        preview=False,
    )
    assert {row.symbol for row in second.active_pool if row.status == "ACTIVE"} == {"000001.SZ", "000002.SZ"}
    assert any(row.reason_code == ACTION_HOLD or row.reason_code == "NONE" for row in second.decisions)

    third = service.run_review(
        program.program_id,
        trade_date=date(2026, 6, 3),
        candidates=[
            _candidate("000001.SZ", 5, 10),
            _candidate("000002.SZ", 6, 21),
            _candidate("000003.SZ", 1, 30),
            _candidate("000004.SZ", 2, 40),
        ],
        preview=False,
    )
    exited = [row for row in third.active_pool if row.status == "EXITED"]
    assert len([row for row in exited if row.exit_reason == EXIT_ALPHA_RANK_DROP]) == 1
    assert len([row for row in third.active_pool if row.status == "ACTIVE" and row.symbol in {"000003.SZ", "000004.SZ"}]) == 1
    assert any(decision.reason_code == EXIT_REPLACEMENT_BUDGET for decision in third.decisions)


def test_stop_loss_take_profit_time_stop_and_metrics() -> None:
    service, _repo = _service()
    program = _program(service, target_count=1, take_profit_bps=1000, trailing_stop_bps=300, time_stop_days=5)

    service.run_review(program.program_id, trade_date=date(2026, 6, 1), candidates=[_candidate("000001.SZ", 1, 10)], preview=False)
    stopped = service.run_review(
        program.program_id,
        trade_date=date(2026, 6, 2),
        candidates=[_candidate("000001.SZ", 1, 9.0)],
        market_by_symbol={"000001.SZ": {"next_open_executable": 9.0, "mark_price": 9.0}},
        preview=False,
    )
    assert any(row.exit_reason == EXIT_STOP_LOSS for row in stopped.active_pool)
    assert stopped.metrics["stop_loss_count"] == 1

    second = _program(service, target_count=1, take_profit_bps=1000, trailing_stop_bps=300, time_stop_days=5)
    service.run_review(second.program_id, trade_date=date(2026, 6, 1), candidates=[_candidate("000002.SZ", 1, 10)], preview=False)
    service.run_review(
        second.program_id,
        trade_date=date(2026, 6, 2),
        candidates=[_candidate("000002.SZ", 1, 12.0)],
        market_by_symbol={"000002.SZ": {"next_open_executable": 12.0, "mark_price": 12.0}},
        preview=False,
    )
    taken = service.run_review(
        second.program_id,
        trade_date=date(2026, 6, 3),
        candidates=[_candidate("000002.SZ", 1, 11.4)],
        market_by_symbol={"000002.SZ": {"next_open_executable": 11.4, "mark_price": 11.4}},
        preview=False,
    )
    assert taken.metrics["take_profit_count"] == 1
    assert taken.metrics["win_rate"] == 1.0

    third = _program(service, target_count=1, time_stop_days=1)
    service.run_review(third.program_id, trade_date=date(2026, 6, 1), candidates=[_candidate("000003.SZ", 1, 10)], preview=False)
    timed = service.run_review(
        third.program_id,
        trade_date=date(2026, 6, 2),
        candidates=[_candidate("000003.SZ", 1, 10.2)],
        market_by_symbol={"000003.SZ": {"next_open_executable": 10.2, "mark_price": 10.2}},
        preview=False,
    )
    assert any(row.exit_reason == EXIT_TIME_STOP for row in timed.active_pool)


def test_next_open_basis_does_not_fallback_to_reference_or_mark_price() -> None:
    service, _repo = _service()
    program = _program(service, target_count=1)

    result = service.run_review(
        program.program_id,
        trade_date=date(2026, 6, 1),
        candidates=[{"symbol": "000001.SZ", "rank": 1, "reference_price": 10}],
        market_by_symbol={"000001.SZ": {"mark_price": 10}},
        preview=False,
    )

    assert result.review_status == "WAITING_DATA"
    assert result.active_pool == []
    assert result.decisions[0].reason_code == "MISSING_ENTRY_PRICE"


def test_same_day_stop_loss_is_deferred_until_effective_t1_entry() -> None:
    service, _repo = _service()
    program = _program(service, target_count=1, stop_loss_bps=500)

    service.run_review(program.program_id, trade_date=date(2026, 6, 1), candidates=[_candidate("000001.SZ", 1, 10)], preview=False)
    result = service.run_review(
        program.program_id,
        trade_date=date(2026, 6, 1),
        candidates=[_candidate("000001.SZ", 1, 9.0)],
        market_by_symbol={"000001.SZ": {"next_open_executable": 9.0}},
        preview=True,
    )

    episode = result.active_pool[0]
    assert episode.status == "ACTIVE"
    assert episode.exit_reason is None
    assert episode.return_bps is None
    assert result.decisions[0].reason_code == EXIT_STOP_LOSS_DEFERRED_T1


def test_multi_program_isolation_leaderboard_and_no_sample_fields() -> None:
    service, _repo = _service()
    strong = _program(service, target_count=1)
    weak = service.create_program(
        program_name="Fusion weak",
        package_mode=PACKAGE_MODE_FUSION,
        package_ids=["pkg_a", "pkg_b"],
        package_weights={"pkg_a": 0.7, "pkg_b": 0.3},
        target_count=1,
        status="ENABLED",
        review_policy={"take_profit_mode": "fixed", "take_profit_bps": 500, "daily_replacement_budget": 1},
    )

    service.run_review(strong.program_id, trade_date=date(2026, 6, 1), candidates=[_candidate("000001.SZ", 1, 10)], preview=False)
    service.run_review(strong.program_id, trade_date=date(2026, 6, 2), candidates=[_candidate("000001.SZ", 1, 11)], preview=False)
    service.run_review(weak.program_id, trade_date=date(2026, 6, 1), candidates=[_candidate("000001.SZ", 1, 20)], preview=False)

    assert {row["symbol"] for row in service.active_pool(strong.program_id)} == {"000001.SZ"}
    assert {row["symbol"] for row in service.active_pool(weak.program_id)} == {"000001.SZ"}

    board = service.leaderboard(sort_by="win_rate")
    assert {row["program_id"] for row in board} == {strong.program_id, weak.program_id}
    assert "last_review_status" in board[0]
    assert "eligible_episode_count" not in board[0]
    assert "data_excluded_count" not in board[0]


def test_replay_uses_default_next_open_entry_basis_and_records_win_rate() -> None:
    repo = InMemoryAdvisoryProgramRepository()
    service = AdvisoryProgramService(
        repository=repo,
        selection_service=None,
        calendar_provider=FakeTradingCalendar(_calendar_days(date(2026, 6, 1), date(2026, 6, 2))),
    )
    program = _program(service, target_count=1)

    replay = service.run_replay(
        program.program_id,
        start_date=date(2026, 6, 1),
        end_date=date(2026, 6, 2),
        candidates_by_date={
            "2026-06-01": [_candidate("000001.SZ", 1, 10)],
            "2026-06-02": [_candidate("000001.SZ", 1, 11)],
        },
        market_by_date={
            "2026-06-01": {"000001.SZ": {"next_open_executable": 10}},
            "2026-06-02": {"000001.SZ": {"next_open_executable": 11, "mark_price": 11}},
        },
    )

    episode = replay["episodes"][0]
    assert episode["entry_price_basis"] == PRICE_BASIS_NEXT_OPEN
    assert episode["effective_entry_date"] == "2026-06-02"
    assert replay["summary"]["win_rate"] == 1.0
    assert replay["summary"]["avg_return_bps"] == pytest.approx(1000.0)


def test_replay_can_run_real_selection_service_when_fixture_candidates_are_absent() -> None:
    class FakeSelectionService:
        def run_packages(self, *, package_ids, mode, trade_date, data_source, runtime_config):
            assert mode == SelectionMode.SINGLE_PACKAGE
            assert data_source == "DB_HISTORICAL"
            price = 10.0 if trade_date == date(2026, 6, 1) else 11.0
            return SelectionRun(
                mode=mode,
                trade_date=trade_date,
                data_source=data_source,
                package_ids=list(package_ids),
                runtime_config=dict(runtime_config),
                status=SelectionRunStatus.SUCCEEDED,
                aggregate_results=[
                    SelectionCandidate(
                        symbol="000001.SZ",
                        rank=1,
                        score=0.9,
                        selection_entry_price=price,
                        reference_price=price,
                    )
                ],
            )

    repo = InMemoryAdvisoryProgramRepository()
    service = AdvisoryProgramService(
        repository=repo,
        selection_service=FakeSelectionService(),
        calendar_provider=FakeTradingCalendar(_calendar_days(date(2026, 6, 1), date(2026, 6, 2))),
    )
    program = _program(service, target_count=1)

    replay = service.run_replay(
        program.program_id,
        start_date=date(2026, 6, 1),
        end_date=date(2026, 6, 2),
        candidates_by_date={},
        market_by_date={},
    )

    assert len(replay["daily_reviews"]) == 2
    assert replay["summary"]["win_rate"] == 1.0


def test_review_from_selection_builds_default_authoritative_runtime_config() -> None:
    class FakeSelectionService:
        def __init__(self) -> None:
            self.runtime_config = None

        def run_packages(self, *, package_ids, mode, trade_date, data_source, runtime_config):
            self.runtime_config = dict(runtime_config)
            return SelectionRun(
                mode=mode,
                trade_date=trade_date,
                data_source=data_source,
                package_ids=list(package_ids),
                runtime_config=dict(runtime_config),
                status=SelectionRunStatus.SUCCEEDED,
                aggregate_results=[
                    SelectionCandidate(
                        symbol="000001.SZ",
                        rank=1,
                        score=0.9,
                        selection_entry_price=10.0,
                        reference_price=10.0,
                    )
                ],
            )

    fake_selection = FakeSelectionService()
    service = AdvisoryProgramService(
        repository=InMemoryAdvisoryProgramRepository(),
        selection_service=fake_selection,
        calendar_provider=FakeTradingCalendar([]),
    )
    program = _program(service, target_count=20)

    result = service.run_review_from_selection(program.program_id, trade_date=date(2026, 6, 8), preview=True)

    assert result.review_status == "SUCCEEDED"
    assert fake_selection.runtime_config["top_k"] == 40
    assert fake_selection.runtime_config["display_top_n"] == 20
    assert fake_selection.runtime_config["st_pit_authoritative"] is True
    assert fake_selection.runtime_config["selection_artifact_config"]["auto_generate"] is True
    assert fake_selection.runtime_config["selection_artifact_config"]["pit_mode"] == "PREVIOUS_TRADING_DAY_CLOSE"
    assert fake_selection.runtime_config["runtime_profile"]["selection"]["top_k"] == 40


def test_review_marks_active_holding_not_in_current_topk_instead_of_waiting_evidence() -> None:
    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, _sql, _params):
            return None

        def fetchall(self):
            return [{"symbol": "000001.SZ", "open_li": 11000, "close_li": 11000}]

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def cursor(self, **_kwargs):
            return FakeCursor()

    service, repo = _service()
    repo._conn_factory = lambda: FakeConnection()
    program = _program(service, target_count=1, rank_exit_threshold=1, rank_exit_confirm_days=2)

    service.run_review(
        program.program_id,
        trade_date=date(2026, 6, 1),
        candidates=[_candidate("000001.SZ", 1, 10.0)],
        preview=False,
    )
    first_review = service.run_review(
        program.program_id,
        trade_date=date(2026, 6, 2),
        candidates=[_candidate("000002.SZ", 1, 20.0)],
        preview=False,
    )

    active = next(row for row in first_review.active_pool if row.symbol == "000001.SZ")
    decision = next(row for row in first_review.decisions if row.symbol == "000001.SZ")
    assert first_review.review_status == "SUCCEEDED"
    assert active.return_bps == pytest.approx(1000.0)
    assert active.current_rank == 2
    assert active.price_quality_status == "OK"
    assert decision.action == ACTION_HOLD
    assert decision.reason_code == "NOT_IN_CURRENT_TOPK"
    assert decision.review_status == "SUCCEEDED"
    assert decision.return_bps == pytest.approx(1000.0)
    assert decision.evidence_json["component_scores"]["active_holding_review"]["reason_code"] == "NOT_IN_CURRENT_TOPK"
    assert first_review.metrics["win_rate"] == 1.0

    second_review = service.run_review(
        program.program_id,
        trade_date=date(2026, 6, 3),
        candidates=[_candidate("000002.SZ", 1, 20.0)],
        market_by_symbol={"000001.SZ": {"next_open_executable": 11.5, "mark_price": 11.5}},
        preview=False,
    )

    assert any(row.symbol == "000001.SZ" and row.exit_reason == EXIT_ALPHA_RANK_DROP for row in second_review.active_pool)


def test_replay_uses_trading_calendar_and_skips_weekend_fixture_gaps() -> None:
    class RejectWeekendSelectionService:
        def run_packages(self, *, package_ids, mode, trade_date, data_source, runtime_config):
            raise AssertionError(f"selection service should not run for replay fixture gap on {trade_date}")

    trading_days = [
        date(2026, 5, 28),
        date(2026, 5, 29),
        date(2026, 6, 1),
        date(2026, 6, 2),
        date(2026, 6, 3),
    ]
    calendar = FakeTradingCalendar(trading_days)
    repo = InMemoryAdvisoryProgramRepository()
    service = AdvisoryProgramService(
        repository=repo,
        selection_service=RejectWeekendSelectionService(),
        calendar_provider=calendar,
    )
    program = _program(service, target_count=1)

    replay = service.run_replay(
        program.program_id,
        start_date=date(2026, 5, 28),
        end_date=date(2026, 6, 3),
        candidates_by_date={day.isoformat(): [_candidate("000001.SZ", 1, 10.0)] for day in trading_days},
        market_by_date={day.isoformat(): {"000001.SZ": {"next_open_executable": 10.0, "mark_price": 10.0}} for day in trading_days},
    )

    assert calendar.requests == [(date(2026, 5, 28), date(2026, 6, 3))]
    assert [row["trade_date"] for row in replay["daily_reviews"]] == [day.isoformat() for day in trading_days]
    assert "2026-05-30" not in {row["trade_date"] for row in replay["daily_reviews"]}
    assert "2026-05-31" not in {row["trade_date"] for row in replay["daily_reviews"]}


def test_advisory_program_has_no_order_broker_or_ledger_writes() -> None:
    import backend.routers.advisory as advisory_router
    import backend.services.advisory_program as advisory_program

    source = inspect.getsource(advisory_program) + inspect.getsource(advisory_router)

    for forbidden in ("create_order", "submit_order", "broker", "position_ledger", "paper_v2"):
        assert forbidden not in source


def test_program_package_binding_delete_preserves_historical_versions() -> None:
    import backend.services.advisory_program as advisory_program

    source = inspect.getsource(advisory_program.AdvisoryProgramPGRepository._replace_program_packages)

    assert "program_version = %s" in source
