from __future__ import annotations

from datetime import UTC, date, datetime

from backend.routers import paper_trading_v2 as paper_v2_router
from backend.services.paper_trading_v2.execution.minqmt_execution_report import (
    REPORT_SCHEMA_VERSION,
    build_minqmt_execution_quality_report,
    list_minqmt_execution_quality_reports,
)
from backend.services.paper_trading_v2.market_data import MinuteDataSource
from backend.services.paper_trading_v2.models import PaperPortfolio, PaperRun
from backend.services.paper_trading_v2.repository import InMemoryPaperTradingV2Repository
from backend.services.trading_core.ledger import FeeModel
from backend.services.trading_core.models import AccountSnapshot, Fill, Order, OrderSide, OrderStatus, OrderType, RunStatus
from backend.tests.paper_trading_v2.test_day_runner import make_paper_enabled_manifest


def _order(
    *,
    status: OrderStatus = OrderStatus.FILLED,
    side: OrderSide = OrderSide.BUY,
    limit_price: float | None = 10.0,
    metadata: dict | None = None,
) -> Order:
    return Order(
        order_id="ord_unit",
        intent_id="intent_unit",
        package_id="pkg_unit",
        portfolio_id="paper_unit",
        symbol="000001.SZ",
        side=side,
        quantity=100,
        order_type=OrderType.LIMIT if limit_price is not None else OrderType.MARKET,
        limit_price=limit_price,
        status=status,
        filled_quantity=100 if status == OrderStatus.FILLED else 0,
        avg_fill_price=10.02 if status == OrderStatus.FILLED else None,
        metadata=metadata if metadata is not None else {"broker_status_msg": "reported", "broker_raw_status": 56},
    )


def _fill(*, side: OrderSide = OrderSide.BUY) -> Fill:
    return Fill(
        fill_id="fill_unit",
        order_id="ord_unit",
        symbol="000001.SZ",
        side=side,
        quantity=100,
        price=10.02,
        trade_time=datetime(2026, 6, 1, 9, 31, tzinfo=UTC),
        reason="unit",
        metadata={
            "trade_amount": 1002.0,
            "broker_reported_commission": 5.0,
            "broker_reported_fee_total": 5.0,
            "cost_precision_level": "broker_aggregate",
            "cost_breakdown_source": "broker_reported_aggregate",
        },
    )


def test_minqmt_execution_report_reconciles_broker_fee_and_slippage() -> None:
    report = build_minqmt_execution_quality_report(
        portfolio_id="paper_unit",
        run_id="prun_unit",
        trade_date=date(2026, 6, 1),
        orders=[_order()],
        fills=[_fill()],
        fee_model=FeeModel(open_cost=0.000095, close_cost=0.000595, min_cost=5.0),
    )

    assert report["schema_version"] == REPORT_SCHEMA_VERSION
    assert report["summary"]["broker_reported_fee_total"] == 5.0
    assert report["summary"]["estimated_fee_total"] == 5.0
    assert report["summary"]["cost_reconciliation_delta"] == 0.0
    assert report["summary"]["cost_precision_counts"] == {"broker_aggregate": 1}
    assert round(report["fills"][0]["slippage_bps"], 6) == 20.0
    assert report["fills"][0]["source_note"].startswith("broker fee is an aggregate MiniQMT field")


def test_minqmt_execution_report_marks_sell_tax_as_estimated_not_broker_confirmed() -> None:
    report = build_minqmt_execution_quality_report(
        portfolio_id="paper_unit",
        run_id="prun_unit",
        trade_date=date(2026, 6, 1),
        orders=[_order(side=OrderSide.SELL)],
        fills=[_fill(side=OrderSide.SELL)],
        fee_model=FeeModel(open_cost=0.000095, close_cost=0.000595, min_cost=5.0),
    )

    fill_item = report["fills"][0]
    assert fill_item["estimated_stamp_tax"] == 1002.0 * 0.0005
    assert fill_item["estimated_transfer_fee"] is None
    assert report["fee_model"]["breakdown_precision"] == "estimated_from_fee_model_not_broker_confirmed"


def test_minqmt_execution_report_requires_diagnostics_for_rejected_orders() -> None:
    rejected_without_diag = _order(status=OrderStatus.REJECTED, metadata={})

    report = build_minqmt_execution_quality_report(
        portfolio_id="paper_unit",
        run_id="prun_unit",
        trade_date=date(2026, 6, 1),
        orders=[rejected_without_diag],
        fills=[],
    )

    assert report["summary"]["diagnostic_coverage"]["orders_requiring_diagnostic"] == 1
    assert report["summary"]["diagnostic_coverage"]["orders_with_diagnostic"] == 0
    assert "rejected_order_missing_diagnostic" in report["summary"]["warning_flags"]
    assert report["orders_requiring_attention"][0]["status"] == "REJECTED"


def _repo_with_minqmt_portfolio() -> tuple[InMemoryPaperTradingV2Repository, PaperPortfolio, PaperRun]:
    repo = InMemoryPaperTradingV2Repository()
    manifest = make_paper_enabled_manifest()
    portfolio = PaperPortfolio(
        portfolio_id="paper_unit",
        portfolio_name="mini qmt report query",
        package_id=manifest.package_id,
        manifest_sha256=manifest.manifest_sha256,
        frozen_manifest=manifest,
        initial_cash=100_000,
        start_date=date(2026, 6, 1),
        data_source=MinuteDataSource.MINIQMT_REALTIME,
        broker_backend="minqmt_sim",
    )
    run = PaperRun(
        run_id="prun_unit",
        portfolio_id=portfolio.portfolio_id,
        trade_date=date(2026, 6, 1),
        status=RunStatus.SUCCEEDED,
        data_source=MinuteDataSource.MINIQMT_REALTIME,
    )
    repo.create_portfolio(portfolio)
    repo.create_run(run)
    return repo, portfolio, run


def test_minqmt_execution_quality_query_reads_snapshot_and_event_without_duplicate() -> None:
    repo, portfolio, run = _repo_with_minqmt_portfolio()
    report = build_minqmt_execution_quality_report(
        portfolio_id=portfolio.portfolio_id,
        run_id=run.run_id,
        trade_date=run.trade_date,
        orders=[_order()],
        fills=[_fill()],
    )
    repo.save_daily_snapshot(
        run_id=run.run_id,
        trade_date=run.trade_date,
        snapshot=AccountSnapshot(
            portfolio_id=portfolio.portfolio_id,
            cash=99_000,
            market_value=1_000,
            nav=100_000,
            snapshot_time=datetime(2026, 6, 1, 15, 0, tzinfo=UTC),
        ),
        metadata={"execution_quality_report": report},
    )
    repo.save_run_event(
        run_id=run.run_id,
        event_type="MINIQMT_EXECUTION_QUALITY_REPORTED",
        message="report",
        context=report,
    )

    result = list_minqmt_execution_quality_reports(
        repository=repo,
        portfolio_id=portfolio.portfolio_id,
        trade_date=run.trade_date,
    )

    assert result["report_count"] == 1
    assert result["source_counts"] == {"daily_snapshot_metadata": 1, "run_event": 1}
    assert result["latest_report"]["summary"]["broker_reported_fee_total"] == 5.0
    assert result["reports"][0]["source"]["source_type"] == "daily_snapshot_metadata"


def test_minqmt_execution_quality_query_warns_when_no_report_exists() -> None:
    repo, portfolio, _run = _repo_with_minqmt_portfolio()

    result = list_minqmt_execution_quality_reports(repository=repo, portfolio_id=portfolio.portfolio_id)

    assert result["report_count"] == 0
    assert result["latest_report"] is None
    assert result["warnings"][0]["code"] == "NO_EXECUTION_QUALITY_REPORT"


def test_minqmt_execution_quality_endpoint_returns_read_only_report(monkeypatch) -> None:
    repo, portfolio, run = _repo_with_minqmt_portfolio()
    report = build_minqmt_execution_quality_report(
        portfolio_id=portfolio.portfolio_id,
        run_id=run.run_id,
        trade_date=run.trade_date,
        orders=[_order()],
        fills=[_fill()],
    )
    repo.save_run_event(
        run_id=run.run_id,
        event_type="MINIQMT_EXECUTION_QUALITY_REPORTED",
        message="report",
        context=report,
    )
    monkeypatch.setattr(paper_v2_router, "PaperTradingV2Repository", lambda: repo)

    response = paper_v2_router.get_portfolio_execution_quality(
        portfolio_id=portfolio.portfolio_id,
        trade_date=run.trade_date,
        limit=10,
        scan_limit=100,
    )

    assert response["ok"] is True
    assert response["execution_quality"]["latest_report"]["run_id"] == run.run_id
    assert response["execution_quality"]["reports"][0]["source"]["source_type"] == "run_event"
