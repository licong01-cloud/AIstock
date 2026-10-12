from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from types import SimpleNamespace

import pytest

from backend.services.advisory_program import (
    AdvisoryEpisode,
    AdvisoryProgram,
    AdvisoryCandidate,
    AdvisoryMarketMark,
    AdvisoryRecommendationListVersion,
    DEFAULT_REVIEW_POLICY,
    compute_program_metrics,
    AdvisoryProgramService,
    InMemoryAdvisoryProgramRepository,
    episode_to_dict,
)
from backend.services.advisory_historical_range.program_performance import (
    ProgramPerformanceMarketReader,
    compare_program_outcomes,
    cost_contract,
    fixed_five_day_outcomes,
    reporting_cutoff,
)
from backend.services.trading_core.errors import RuntimeConfigInvalidError


SHANGHAI = ZoneInfo("Asia/Shanghai")


def program() -> AdvisoryProgram:
    return AdvisoryProgram(
        program_id="p",
        program_name="P",
        status="ENABLED",
        target_count=20,
        package_mode="single_package",
        package_ids=["pkg"],
        package_weights={},
        fusion_method=None,
        package_set_hash="a" * 64,
        fusion_policy_sha256=None,
        review_policy=dict(DEFAULT_REVIEW_POLICY),
        review_policy_sha256="b" * 64,
        entry_price_basis="next_open_executable",
        exit_price_basis="next_open_executable",
        review_schedule={},
    )


def episode(**changes) -> AdvisoryEpisode:
    row = AdvisoryEpisode(
        episode_id="e",
        program_id="p",
        program_version=1,
        symbol="000001.SZ",
        status="EXITED",
        signal_date=date(2026, 10, 8),
        effective_entry_date=date(2026, 10, 9),
        entry_price=10,
        entry_price_basis="next_open_executable",
        entry_rank=1,
        exit_signal_date=date(2026, 10, 12),
        effective_exit_date=date(2026, 10, 13),
        exit_price=11,
        exit_price_basis="next_open_executable",
        exit_reason="TAKE_PROFIT",
        return_bps=1000,
        is_win=True,
        win_rate_inclusion_status="INCLUDED",
    )
    return replace(row, **changes)


def test_future_exit_and_open_marks_are_not_realized_wins() -> None:
    planned = episode(effective_exit_date=date(2099, 1, 5))
    active = episode(
        episode_id="a",
        status="ACTIVE",
        effective_exit_date=None,
        exit_reason=None,
        exit_price=None,
        win_rate_inclusion_status="OPEN_MARK_TO_MARKET",
    )
    metrics = compute_program_metrics(program(), [planned, active])
    assert metrics["win_rate"] is None
    assert metrics["metric_evaluable_count"] == 0
    assert metrics["take_profit_count"] == 0


@pytest.mark.parametrize("hour,expected", [(1, 0), (14, 0), (15, 1)])
def test_target_day_release_requires_close(hour: int, expected: int) -> None:
    metrics = compute_program_metrics(program(), [episode()], now=datetime(2026, 10, 13, hour, tzinfo=SHANGHAI))
    assert metrics["metric_evaluable_count"] == expected
    assert metrics["win_rate"] == (1 if expected else None)


def test_settled_distribution_and_unknown_fees_have_honest_denominators() -> None:
    rows = [
        episode(),
        episode(episode_id="loss", exit_price=9.5, return_bps=-500),
        episode(episode_id="flat", exit_price=10, return_bps=0),
        episode(episode_id="bad", return_bps=None, win_rate_inclusion_status="EXCLUDED"),
    ]
    metrics = compute_program_metrics(program(), rows, now=datetime(2026, 10, 14, tzinfo=SHANGHAI))
    assert metrics["settled"]["gross"]["win_count"] == 1
    assert metrics["settled"]["gross"]["loss_count"] == 1
    assert metrics["settled"]["gross"]["flat_count"] == 1
    assert metrics["settled"]["gross"]["profit_factor"] == pytest.approx(2)
    assert metrics["settled"]["net"]["avg_return_bps"] is None
    assert metrics["settled"]["net"]["reason_code"] == "COST_UNSPECIFIED"
    assert metrics["coverage"]["due_count"] == 4
    assert metrics["coverage"]["settled_count"] == 3
    assert metrics["coverage"]["settled_coverage"] == pytest.approx(0.75)
    costed = compute_program_metrics(
        program(), rows, now=datetime(2026, 10, 14, tzinfo=SHANGHAI), cost=cost_contract(10, 20)
    )
    assert costed["settled"]["net"]["sample_count"] == 3
    assert costed["settled"]["net"]["loss_count"] == 2  # gross flat becomes net loss


def market() -> dict:
    dates = [date(2026, 10, 9) + timedelta(days=i) for i in range(22)]
    trading = [d for d in dates if d.weekday() < 5]
    return {
        "calendar": [{"cal_date": d, "is_trading": d.weekday() < 5} for d in dates],
        "prices": [
            {
                "symbol": "000001.SZ",
                "trade_date": d,
                "open_li": 10000,
                "close_li": 11000,
                "high_li": 11000,
                "low_li": 10000,
                "adj_factor": 1,
                "up_limit": 12,
                "down_limit": 8,
            }
            for d in trading
        ],
        "benchmark": [{"trade_date": d, "open": 100, "close": 102} for d in trading],
        "suspended": set(),
    }


def test_five_sessions_do_not_shorten_for_suspensions_or_count_immature_returns() -> None:
    data = market()
    data["suspended"].add(("000001.SZ", date(2026, 10, 13)))
    first = episode_to_dict(episode())
    cost = cost_contract(10, 20)
    immature = fixed_five_day_outcomes([first], data, cutoff=date(2026, 10, 14), cost=cost)[0]
    mature = fixed_five_day_outcomes([first], data, cutoff=date(2026, 10, 15), cost=cost)[0]
    assert immature["status"] == "IMMATURE" and immature["gross_return_bps"] is None
    assert mature["maturity_trade_date"] == "2026-10-15"
    assert mature["status"] == "MATURE"
    assert mature["net_return_bps"] == pytest.approx((1.1 * 0.998 / 1.001 - 1) * 10000)
    assert mature["net_excess_return_bps"] == pytest.approx(mature["net_return_bps"] - 200)
    data["suspended"].add(("000001.SZ", date(2026, 10, 15)))
    stopped = fixed_five_day_outcomes([first], data, cutoff=date(2026, 10, 15), cost=cost)[0]
    assert stopped["status"] == "SUSPENDED" and stopped["net_return_bps"] is None


def test_five_day_adjustments_limits_missing_data_and_costs_are_explicit() -> None:
    data = market()
    rows = [episode_to_dict(episode())]
    terminal = next(row for row in data["prices"] if row["trade_date"] == date(2026, 10, 15))
    terminal.update(close_li=5500, adj_factor=2, down_limit=4)
    result = fixed_five_day_outcomes(rows, data, cutoff=date(2026, 10, 15), cost=None)[0]
    assert result["gross_return_bps"] == pytest.approx(1000)  # no fake loss at split
    assert result["net_return_bps"] is None
    terminal["adj_factor"] = None
    assert (
        fixed_five_day_outcomes(rows, data, cutoff=date(2026, 10, 15), cost=None)[0]["status"]
        == "MISSING_PRICE_ADJUSTMENT_OR_LIMIT"
    )
    terminal.update(adj_factor=2, down_limit=5.5)
    assert fixed_five_day_outcomes(rows, data, cutoff=date(2026, 10, 15), cost=None)[0]["status"] == "NON_EXECUTABLE"
    data["prices"].remove(terminal)
    assert fixed_five_day_outcomes(rows, data, cutoff=date(2026, 10, 15), cost=None)[0]["status"] == "MISSING_DATA"
    with pytest.raises(RuntimeConfigInvalidError):
        cost_contract(0, None)
    assert cost_contract(0, 0) is not None
    assert reporting_cutoff(datetime(2026, 10, 13, 6, 59, tzinfo=ZoneInfo("UTC"))) == date(2026, 10, 12)


def test_comparison_preserves_exclusions_and_equal_weights_common_dates() -> None:
    data = market()
    rows = [episode_to_dict(episode()), episode_to_dict(episode(episode_id="b", program_id="q"))]
    outcomes = fixed_five_day_outcomes(rows, data, cutoff=date(2026, 10, 15), cost=cost_contract(0, 0))
    outcomes += [
        {
            **outcomes[0],
            "episode_id": "extra",
            "entry_trade_date": "2026-10-12",
            "status": "IMMATURE",
            "net_return_bps": None,
        }
    ]
    response = compare_program_outcomes(
        outcomes,
        program_ids=["p", "q"],
        calendar=[r["cal_date"] for r in data["calendar"] if r["is_trading"]],
        start=date(2026, 10, 9),
        end=date(2026, 10, 15),
        cost=cost_contract(0, 0),
        benchmark="000300.SH",
    )
    assert response["common_valid_dates"] == ["2026-10-09"]
    assert response["comparable"] is True
    assert response["programs"][0]["total_count"] == 2
    assert response["programs"][0]["windows"]["ALL"]["entry_date_count"] == 1
    outcomes[1]["status"] = "MISSING_DATA"
    failed = compare_program_outcomes(
        outcomes,
        program_ids=["p", "q"],
        calendar=[],
        start=date(2026, 10, 9),
        end=date(2026, 10, 15),
        cost=None,
        benchmark="000300.SH",
    )
    assert not failed["comparable"] and failed["common_candidate_dates"] == ["2026-10-09"]
    assert failed["programs"][1]["status_counts"] == {"MISSING_DATA": 1}


class ReadConnection:
    def __init__(self, data: dict, settlement: list[dict] | None = None) -> None:
        self.data, self.settlement = data, settlement or []
        self.calls = []
        self.result = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def cursor(self, **_kwargs):
        return self

    def execute(self, sql, params):
        self.calls.append((sql, params))
        assert sql.lstrip().startswith(("SELECT", "WITH"))
        if "WITH due" in sql:
            self.result = self.settlement
        elif "FROM market.trading_calendar" in sql:
            self.result = self.data["calendar"]
        elif "FROM market.kline_daily_raw k" in sql:
            self.result = self.data["prices"]
        elif "FROM market.suspend_d" in sql:
            self.result = [{"symbol": s, "trade_date": d} for s, d in self.data["suspended"]]
        elif "FROM market.index_daily" in sql:
            self.result = self.data["benchmark"]
        else:
            self.result = []

    def fetchall(self):
        return self.result


def test_market_read_includes_maturity_tail_but_never_future_prices() -> None:
    conn = ReadConnection(market())
    ProgramPerformanceMarketReader(lambda: conn).load(
        symbols=["000001.SZ"],
        start=date(2026, 10, 9),
        end=date(2026, 10, 9),
        cutoff=date(2026, 10, 15),
        benchmark="000300.SH",
    )
    assert len(conn.calls) == 4
    assert all(params[-1] == date(2026, 10, 15) for _, params in conn.calls[1:])


def test_returns_api_uses_one_snapshot_and_keeps_next_open_price_after_close() -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from backend.routers.advisory import router, get_advisory_program_service

    repo = InMemoryAdvisoryProgramRepository()
    repo.programs["p"] = program()
    repo.insert_episode_snapshot(episode())
    repo.create_list_version(
        AdvisoryRecommendationListVersion(
            list_version_id="lv",
            program_id="p",
            binding_version_id="binding",
            review_run_id="review",
            trade_date=date(2026, 10, 12),
            previous_list_version_id=None,
            version_status="PUBLISHED",
            target_count=20,
            active_count=1,
            entered_count=1,
            held_count=0,
            exited_count=1,
            waiting_count=0,
            changed_count=2,
            turnover_rate=0.1,
            overlap_rate=0.9,
        ),
        [],
    )
    conn = ReadConnection(
        market(),
        [
            {
                "episode_id": "e",
                "entry_open": 10000,
                "entry_close": 9900,
                "exit_open": 12000,
                "exit_close": 12500,
                "suspended": False,
            }
        ],
    )
    repo._conn_factory = lambda: conn
    service = AdvisoryProgramService(
        repository=repo,
        selection_service=object(),
        calendar_provider=object(),
        symbol_name_resolver=SimpleNamespace(resolve=lambda _symbols: {}),
        now_provider=lambda: datetime(2026, 10, 13, 16, tzinfo=SHANGHAI),
    )
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    app.dependency_overrides[get_advisory_program_service] = lambda: service
    with TestClient(app) as client:
        response = client.get("/api/v1/advisory/programs/p/returns")
    assert response.status_code == 200
    payload = response.json()
    assert payload["returns"][0]["exit_price"] == 12  # published after close, executed at open
    assert payload["metrics"]["avg_return_bps"] == pytest.approx(2000)
    assert payload["returns"][0]["performance_state"] == "SETTLED"
    assert len(conn.calls) == 1
    assert repo.all_latest_episodes("p")[0].exit_price == 11  # no historical write
    with TestClient(app) as client:
        costed = client.get(
            "/api/v1/advisory/programs/p/returns",
            params={"buy_cost_bps": 10, "sell_cost_bps": 20},
        )
        comparison = client.get(
            "/api/v1/advisory/performance-comparison",
            params={
                "start_date": "2026-10-09",
                "end_date": "2026-10-12",
                "buy_cost_bps": 0,
                "sell_cost_bps": 0,
            },
        )
        invalid = client.get(
            "/api/v1/advisory/performance-comparison",
            params={
                "start_date": "2026-10-09",
                "end_date": "2026-10-12",
                "buy_cost_bps": 1,
            },
        )
    assert costed.status_code == 200
    net = (1.2 * 0.998 / 1.001 - 1) * 10000
    assert costed.json()["returns"][0]["realized_net_return_bps"] == pytest.approx(net)
    assert costed.json()["metrics"]["settled"]["net"]["avg_return_bps"] == pytest.approx(net)
    assert comparison.status_code == 200
    assert comparison.json()["comparison"]["outcomes"][0]["status"] == "IMMATURE"
    assert comparison.json()["comparison"]["programs"][0]["daily_list_changes"][0]["paired_list_rotation_count"] == 1
    assert invalid.status_code >= 400
    conn.settlement[0]["exit_close"] = None
    missing_close = service.return_snapshot("p")
    assert missing_close["metrics"]["metric_evaluable_count"] == 0
    assert missing_close["returns"][0]["performance_state"] == "UNKNOWN"


def test_forward_execution_does_not_shift_dates_or_use_missing_target_price() -> None:
    service = AdvisoryProgramService(
        repository=InMemoryAdvisoryProgramRepository(),
        selection_service=object(),
        calendar_provider=object(),
        symbol_name_resolver=object(),
        now_provider=lambda: datetime(2026, 10, 13, 10, tzinfo=SHANGHAI),
    )
    candidates = [
        AdvisoryCandidate(
            symbol="000001.SZ",
            rank=1,
            score=1,
            reference_price=10,
            next_open_executable=8,
            reference_price_trade_date=date(2026, 10, 12),
        )
    ]
    active = episode(
        status="ACTIVE",
        exit_price=None,
        exit_reason=None,
        effective_exit_date=None,
        return_bps=None,
        still_active_mark_price=10,
    )
    settled = service.evaluate_forward_settlement(
        program=program(),
        target_trade_date=date(2026, 10, 13),
        candidates=candidates,
        active_episodes=[active],
        market_by_symbol={"000001.SZ": AdvisoryMarketMark("000001.SZ", date(2026, 10, 13), next_open_executable=8)},
    )
    exited = settled.active_pool[0]
    assert exited.effective_exit_date == date(2026, 10, 13)
    assert exited.exit_price == 8
    assert settled.metrics["metric_evaluable_count"] == 0  # not yet released at close
    waiting = service.evaluate_forward_settlement(
        program=program(),
        target_trade_date=date(2026, 10, 13),
        candidates=candidates,
        active_episodes=[active],
        market_by_symbol={},
    )
    assert waiting.active_pool[0].status == "ACTIVE"  # no candidate-price fallback
