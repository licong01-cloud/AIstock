from __future__ import annotations

from datetime import UTC, date, datetime

from backend.services.paper_trading_v2.live_dashboard import PaperTradingLiveDashboardService
from backend.services.paper_trading_v2.market_data import MinuteDataSource
from backend.services.paper_trading_v2.models import (
    IntradaySnapshot,
    OrderExecutionState,
    PaperPortfolio,
    PaperRun,
    PaperSessionDay,
    PaperSessionMode,
    PaperSessionPhase,
    PaperSessionStatus,
    PaperTradingSession,
)
from backend.services.paper_trading_v2.repository import InMemoryPaperTradingV2Repository
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import PackageStatus
from backend.services.strategy_package.selection_artifact import (
    InMemorySelectionScoreArtifactRepository,
    SelectionScoreArtifact,
    selection_artifact_runtime_hash,
)
from backend.services.strategy_package.live_inference import AUTHORITATIVE_SELECTION_SCOPE, AUTHORITATIVE_SELECTION_SOURCE_TYPE
from backend.services.trading_core.models import Order, OrderEvent, OrderEventType, OrderSide, OrderStatus, OrderType, RunStatus
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


def _portfolio(repo: InMemoryPaperTradingV2Repository) -> tuple[PaperPortfolio, str]:
    manifest = freeze_manifest(
        make_manifest().model_copy(
            update={
                "package_id": "pkg_live_dash",
                "package_name": "qe_live_dash",
                "package_status": PackageStatus.PAPER_ENABLED,
            }
        )
    )
    portfolio = PaperPortfolio(
        portfolio_id="paper_live_dash",
        portfolio_name="实时详情测试盘",
        package_id=manifest.package_id,
        manifest_sha256=manifest.manifest_sha256 or "",
        frozen_manifest=manifest,
        initial_cash=1_000_000,
        start_date=date(2024, 1, 3),
        data_source=MinuteDataSource.TDX_REALTIME,
        fee_policy={},
        risk_policy={},
        execution_policy={},
    )
    repo.create_portfolio(portfolio)
    return portfolio, manifest.manifest_sha256 or ""


class StaticSymbolNameResolver:
    def resolve(self, symbols: list[str]) -> dict[str, str]:
        return {"000001.SZ": "平安银行"}


def test_live_dashboard_aggregates_signal_minute_execution_and_snapshots() -> None:
    repo = InMemoryPaperTradingV2Repository()
    portfolio, manifest_sha = _portfolio(repo)
    runtime_config = {
        "selection_artifact_config": {
            "auto_generate": True,
            "inference_backend": "local",
            "pit_mode": "PREVIOUS_TRADING_DAY_CLOSE",
            "cutoff_date": "2024-01-02",
        },
        "paper_v2_session": {"signal_data_source": "DB_HISTORICAL"},
    }
    artifact_repo = InMemorySelectionScoreArtifactRepository()
    artifact_repo.save(
        SelectionScoreArtifact(
            package_id=portfolio.package_id,
            manifest_sha256=manifest_sha,
            trade_date=date(2024, 1, 3),
            data_source="DB_HISTORICAL",
            runtime_config_hash=selection_artifact_runtime_hash(runtime_config),
            scores_json=[
                {"symbol": "000001.SZ", "score": 0.9, "rank": 1, "reference_price": 10.0, "target_weight": 0.05},
            ],
            score_count=1,
            universe_count=1,
            top_score_symbol="000001.SZ",
            metadata={
                "source_type": AUTHORITATIVE_SELECTION_SOURCE_TYPE,
                "authority_scope": AUTHORITATIVE_SELECTION_SCOPE,
                "cutoff_date": "2024-01-02",
                "score_trade_date": "2024-01-02",
                "reference_price_trade_date": "2024-01-02",
            },
        )
    )
    session = PaperTradingSession(
        session_id="psess_live_dash",
        portfolio_id=portfolio.portfolio_id,
        mode=PaperSessionMode.LIVE_ONLY,
        status=PaperSessionStatus.LIVE_WAITING_FOR_BAR,
        phase=PaperSessionPhase.LIVE_INTRADAY,
        start_date=date(2024, 1, 3),
        live_data_source=MinuteDataSource.TDX_REALTIME,
        runtime_config=runtime_config,
    )
    repo.create_session(session)
    run = PaperRun(
        run_id="prun_live_dash",
        portfolio_id=portfolio.portfolio_id,
        trade_date=date(2024, 1, 3),
        status=RunStatus.RUNNING,
        data_source=MinuteDataSource.TDX_REALTIME,
        runtime_config=runtime_config,
    )
    repo.create_run(run)
    repo.save_session_day(
        PaperSessionDay(
            session_id=session.session_id,
            portfolio_id=portfolio.portfolio_id,
            trade_date=run.trade_date,
            run_id=run.run_id,
            status=PaperSessionStatus.LIVE_WAITING_FOR_BAR,
            phase=PaperSessionPhase.LIVE_INTRADAY,
            data_source=MinuteDataSource.TDX_REALTIME,
            latest_available_bar_time=datetime(2024, 1, 3, 9, 31, tzinfo=UTC),
            last_processed_bar_time=datetime(2024, 1, 3, 9, 31, tzinfo=UTC),
        )
    )
    repo.save_run_event(
        run_id=run.run_id,
        event_type="TARGETS_GENERATED",
        message="targets",
        context={"target_count": 1, "targets": [{"symbol": "000001.SZ", "rank": 1, "target_quantity": 1000, "target_weight": 0.05}]},
    )
    repo.save_run_event(
        run_id=run.run_id,
        event_type="ORDER_INTENTS_GENERATED",
        message="intents",
        context={"order_intent_count": 1, "intents": [{"intent_id": "intent_1", "symbol": "000001.SZ", "side": "BUY", "quantity": 1000}]},
    )
    order = Order(
        order_id="ord_live_dash",
        intent_id="intent_1",
        package_id=portfolio.package_id,
        portfolio_id=portfolio.portfolio_id,
        symbol="000001.SZ",
        side=OrderSide.BUY,
        quantity=1000,
        order_type=OrderType.MARKET,
        status=OrderStatus.SUBMITTED,
    )
    repo.save_order(run.run_id, order)
    repo.save_order_event(
        run.run_id,
        OrderEvent(
            event_id="evt_live_dash",
            order_id=order.order_id,
            event_type=OrderEventType.NO_FILL,
            event_time=datetime(2024, 1, 3, 9, 31, tzinfo=UTC),
            reason="round_lot_zero",
            metadata={"remaining_quantity": 1000, "step": 1},
        ),
    )
    repo.save_order_execution_state(
        OrderExecutionState(
            session_id=session.session_id,
            run_id=run.run_id,
            order_id=order.order_id,
            symbol=order.symbol,
            trade_date=run.trade_date,
            algo_code="V25_TWO_STAGE",
            filled_quantity=0,
            remaining_quantity=1000,
            status=OrderStatus.SUBMITTED.value,
        )
    )
    repo.save_intraday_snapshot(
        IntradaySnapshot(
            session_id=session.session_id,
            run_id=run.run_id,
            portfolio_id=portfolio.portfolio_id,
            trade_date=run.trade_date,
            snapshot_time=datetime(2024, 1, 3, 9, 31, tzinfo=UTC),
            cash=1_000_000,
            market_value=0,
            nav=1_000_000,
            positions=[],
            source="TDX_REALTIME",
        )
    )

    dashboard = PaperTradingLiveDashboardService(
        repository=repo,
        artifact_repository=artifact_repo,
        symbol_name_resolver=StaticSymbolNameResolver(),
    ).get_dashboard(portfolio.portfolio_id)

    assert dashboard["daily_signal"]["status"] == "AVAILABLE"
    assert dashboard["daily_signal"]["cutoff_date"] == "2024-01-02"
    assert dashboard["daily_signal"]["top_candidates"][0]["stock_name"] == "平安银行"
    assert dashboard["target_rebalance"]["targets"][0]["symbol"] == "000001.SZ"
    assert dashboard["target_rebalance"]["targets"][0]["stock_name"] == "平安银行"
    assert dashboard["minute_execution"]["summary"]["no_fill_count"] == 1
    assert dashboard["minute_execution"]["timeline"][0]["stock_name"] == "平安银行"
    assert dashboard["minute_execution"]["timeline"][0]["reason_label"] == "本分钟计划量不足 A 股最小交易单位，不能成交"
    assert dashboard["intraday_nav"]["status"] == "AVAILABLE"
