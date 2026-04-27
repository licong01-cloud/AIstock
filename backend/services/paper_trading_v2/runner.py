"""Minimal Paper Trading v2 vertical runner.

The runner wires StrategyPackage -> OMS -> MinuteExecutionEngine -> Ledger.
It is intentionally explicit and in-memory for the first vertical slice; DB
persistence and scheduling are later phases.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict

from backend.services.paper_trading_v2.market_data import (
    MinuteDataSource,
    PaperV2MinuteMarketDataProvider,
)
from backend.services.strategy_package.models import StrategyPackageManifest
from backend.services.strategy_package.validators import StrategyPackageValidator
from backend.services.trading_core.errors import (
    DataUnavailableError,
    StrategyPackageValidationError,
)
from backend.services.trading_core.execution_algo_capabilities import required_minute_bars_for_policy
from backend.services.trading_core.ledger import FeeModel, InMemoryLedger
from backend.services.trading_core.minute_execution import MinuteExecutionEngine
from backend.services.trading_core.models import (
    AccountSnapshot,
    Fill,
    MinuteBar,
    Order,
    OrderEvent,
    OrderIntent,
)
from backend.services.trading_core.oms import OMS


class PaperRunResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    portfolio_id: str
    order: Order
    fills: list[Fill]
    events: list[OrderEvent]
    account_snapshot: AccountSnapshot


class PaperBatchRunResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    portfolio_id: str
    orders: list[Order]
    fills: list[Fill]
    events: list[OrderEvent]
    account_snapshot: AccountSnapshot


class PaperTradingV2Runner:
    """Run a single order through the authoritative minute execution path."""

    def __init__(
        self,
        *,
        validator: StrategyPackageValidator | None = None,
        oms: OMS | None = None,
        execution_engine: MinuteExecutionEngine | None = None,
        market_data_provider: PaperV2MinuteMarketDataProvider | None = None,
    ) -> None:
        self.validator = validator or StrategyPackageValidator()
        self.oms = oms or OMS()
        self.execution_engine = execution_engine or MinuteExecutionEngine(oms=self.oms)
        self.market_data_provider = market_data_provider or PaperV2MinuteMarketDataProvider()

    def run_single_order(
        self,
        *,
        manifest: StrategyPackageManifest,
        portfolio_id: str,
        initial_cash: float,
        order_intent: OrderIntent,
        minute_bars: list[MinuteBar],
        market_context: dict,
        snapshot_prices: dict[str, float],
        snapshot_time: datetime,
        fee_model: FeeModel | None = None,
    ) -> PaperRunResult:
        self.validator.validate_for_paper_trading(manifest)
        self._validate_order_intent(
            manifest=manifest,
            portfolio_id=portfolio_id,
            order_intent=order_intent,
        )

        order = self.oms.create_order(order_intent)
        final_order, fills, events = self.execution_engine.execute_order(
            order=order,
            minute_bars=minute_bars,
            algo_code=manifest.minute_execution_policy.algo_code,
            algo_config=manifest.minute_execution_policy.algo_config,
            market_context=market_context,
            allow_partial_fill=bool(
                manifest.minute_execution_policy.algo_config.get("allow_partial_fill", True)
            ),
        )

        ledger = InMemoryLedger(
            portfolio_id=portfolio_id,
            initial_cash=initial_cash,
            fee_model=fee_model,
        )
        for fill in fills:
            ledger.apply_fill(fill)
        snapshot = ledger.account_snapshot(
            prices=snapshot_prices,
            snapshot_time=snapshot_time,
        )

        return PaperRunResult(
            portfolio_id=portfolio_id,
            order=final_order,
            fills=fills,
            events=events,
            account_snapshot=snapshot,
        )

    def run_single_order_from_market_data(
        self,
        *,
        manifest: StrategyPackageManifest,
        portfolio_id: str,
        initial_cash: float,
        order_intent: OrderIntent,
        data_source: MinuteDataSource,
        min_bars: int | None = None,
        snapshot_time: datetime | None = None,
        fee_model: FeeModel | None = None,
    ) -> PaperRunResult:
        """Load explicit minute data source and run one order.

        No source fallback is performed. If TDX/DB data, limit prices, previous
        close, or required warmup bars are missing, the provider raises a
        ``DataUnavailableError`` before execution starts.
        """

        self.validator.validate_for_paper_trading(manifest)
        self._validate_order_intent(
            manifest=manifest,
            portfolio_id=portfolio_id,
            order_intent=order_intent,
        )
        required_bars = min_bars or self._required_minute_bars_for_manifest(manifest)
        market_input = self.market_data_provider.load_symbol_input(
            symbol=order_intent.symbol,
            trade_date=order_intent.target_trade_date,
            source=data_source,
            min_bars=required_bars,
            require_day_features=str(manifest.minute_execution_policy.algo_code).strip().upper() == "V25_TWO_STAGE",
        )
        if not market_input.minute_bars:
            raise DataUnavailableError(
                "market data provider returned no minute bars",
                context={
                    "symbol": order_intent.symbol,
                    "trade_date": order_intent.target_trade_date.isoformat(),
                    "source": data_source.value,
                },
            )

        last_bar = market_input.minute_bars[-1]
        return self.run_single_order(
            manifest=manifest,
            portfolio_id=portfolio_id,
            initial_cash=initial_cash,
            order_intent=order_intent,
            minute_bars=market_input.minute_bars,
            market_context=market_input.market_context,
            snapshot_prices={order_intent.symbol: last_bar.close},
            snapshot_time=snapshot_time or last_bar.bar_time,
            fee_model=fee_model,
        )

    def run_order_batch(
        self,
        *,
        manifest: StrategyPackageManifest,
        portfolio_id: str,
        initial_cash: float,
        order_intents: list[OrderIntent],
        minute_bars_by_symbol: dict[str, list[MinuteBar]],
        market_context_by_symbol: dict[str, dict[str, Any]],
        snapshot_prices: dict[str, float],
        snapshot_time: datetime,
        fee_model: FeeModel | None = None,
    ) -> PaperBatchRunResult:
        """Run multiple orders against one in-memory ledger.

        This is a strict in-memory batch skeleton for the next Paper v2 phase.
        Empty batches, missing symbol data, or any order failure abort the run.
        """

        self.validator.validate_for_paper_trading(manifest)
        if not order_intents:
            raise StrategyPackageValidationError(
                "order batch requires at least one order_intent",
                context={"package_id": manifest.package_id, "portfolio_id": portfolio_id},
            )
        for intent in order_intents:
            self._validate_order_intent(
                manifest=manifest,
                portfolio_id=portfolio_id,
                order_intent=intent,
            )
            if intent.symbol not in minute_bars_by_symbol:
                raise DataUnavailableError(
                    "missing minute bars for batch order symbol",
                    context={
                        "package_id": manifest.package_id,
                        "portfolio_id": portfolio_id,
                        "symbol": intent.symbol,
                    },
                )
            if intent.symbol not in market_context_by_symbol:
                raise DataUnavailableError(
                    "missing market context for batch order symbol",
                    context={
                        "package_id": manifest.package_id,
                        "portfolio_id": portfolio_id,
                        "symbol": intent.symbol,
                    },
                )

        ledger = InMemoryLedger(
            portfolio_id=portfolio_id,
            initial_cash=initial_cash,
            fee_model=fee_model,
        )
        orders: list[Order] = []
        fills: list[Fill] = []
        events: list[OrderEvent] = []
        for intent in order_intents:
            order = self.oms.create_order(intent)
            final_order, order_fills, order_events = self.execution_engine.execute_order(
                order=order,
                minute_bars=minute_bars_by_symbol[intent.symbol],
                algo_code=manifest.minute_execution_policy.algo_code,
                algo_config=manifest.minute_execution_policy.algo_config,
                market_context=market_context_by_symbol[intent.symbol],
                allow_partial_fill=bool(
                    manifest.minute_execution_policy.algo_config.get("allow_partial_fill", True)
                ),
            )
            for fill in order_fills:
                ledger.apply_fill(fill)
            orders.append(final_order)
            fills.extend(order_fills)
            events.extend(order_events)

        snapshot = ledger.account_snapshot(
            prices=snapshot_prices,
            snapshot_time=snapshot_time,
        )
        return PaperBatchRunResult(
            portfolio_id=portfolio_id,
            orders=orders,
            fills=fills,
            events=events,
            account_snapshot=snapshot,
        )

    def _validate_order_intent(
        self,
        *,
        manifest: StrategyPackageManifest,
        portfolio_id: str,
        order_intent: OrderIntent,
    ) -> None:
        if order_intent.package_id != manifest.package_id:
            raise StrategyPackageValidationError(
                "order_intent package_id does not match manifest",
                context={
                    "intent_package_id": order_intent.package_id,
                    "manifest_package_id": manifest.package_id,
                },
            )
        if order_intent.portfolio_id != portfolio_id:
            raise StrategyPackageValidationError(
                "order_intent portfolio_id does not match run portfolio",
                context={
                    "intent_portfolio_id": order_intent.portfolio_id,
                    "portfolio_id": portfolio_id,
                },
            )

    @staticmethod
    def _required_minute_bars_for_manifest(manifest: StrategyPackageManifest) -> int:
        return required_minute_bars_for_policy(
            manifest.minute_execution_policy.model_dump(mode="json"),
            package_id=manifest.package_id,
        )
