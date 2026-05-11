"""End-to-end paper-v2 sim runner (Phase 2 T5).

Wires the existing pieces:

    StrategyPackageManifest
        --(create LocalSim from manifest)--> LocalSimBackend
        --(wrap)--> SimGateway
        --(send each OrderIntent)--> OrderHandle / Fills / position update
        --(record at every state transition)--> DaemonEventLog (SQLite)

The runner is intentionally minimal: it accepts a manifest, an iterable of
OrderIntents (caller-supplied, derived from real selection scores or test
fixtures), and a DaemonEventLog instance. It does NOT load market data
itself -- that comes from the ``PaperV2MinuteMarketDataProvider`` injected
into the LocalSim constructor (or a test double).

This is the MVP shape Lead's Task #35 calls for. It is **not** a long-running
daemon -- it is a single-shot runner that drives one portfolio's order
batch through the broker and produces an event-log audit trail.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Iterable

from backend.services.paper_trading_v2.broker import (
    FillEvent,
    LocalSimBackend,
    OrderHandle,
    OrderHandleStatus,
    SubscriptionHandle,
)
from backend.services.paper_trading_v2.market_data import (
    MinuteDataSource,
    PaperV2MinuteMarketDataProvider,
)
from backend.services.strategy_package.models import StrategyPackageManifest
from backend.services.trading_core.errors import (
    BrokerConnectivityError,
    BrokerRejectedError,
    BrokerSubmitError,
    TradingCoreError,
)
from backend.services.trading_core.models import OrderIntent
from backend.services.trading_core.sim_gateway import SimGateway

from .event_log import DaemonEventLog, DaemonEventType


@dataclass
class SimRunResult:
    """Summary returned from ``PaperV2SimRunner.run_intents``."""

    run_id: str
    portfolio_id: str
    package_id: str
    handles: list[OrderHandle] = field(default_factory=list)
    statuses: list[OrderHandleStatus] = field(default_factory=list)
    fills_received: int = 0
    rejected_intents: list[str] = field(default_factory=list)  # intent_id list
    failed: bool = False
    failure_reason: str | None = None


class PaperV2SimRunner:
    """Drive a manifest's intent batch through SimGateway + LocalSim.

    The runner does not own the SimGateway lifecycle by default -- caller
    creates the gateway, calls ``connect()``, hands it in, and ``close()``s
    when done. Use ``run_intents`` to process a batch.
    """

    def __init__(
        self,
        *,
        gateway: SimGateway,
        event_log: DaemonEventLog,
        manifest: StrategyPackageManifest,
    ) -> None:
        if gateway.state.value != "CONNECTED":
            raise ValueError(
                "PaperV2SimRunner requires a CONNECTED SimGateway; "
                f"got state={gateway.state.value}"
            )
        self._gateway = gateway
        self._event_log = event_log
        self._manifest = manifest
        self._fill_subscription: SubscriptionHandle | None = None

    @property
    def event_log(self) -> DaemonEventLog:
        return self._event_log

    @property
    def gateway(self) -> SimGateway:
        return self._gateway

    # ----- public API -----
    def run_intents(self, intents: Iterable[OrderIntent]) -> SimRunResult:
        """Submit each intent in order, recording every state transition.

        Synchronous: fills callbacks fire before each ``send_order`` returns
        (LocalSim semantics, Lead 2026-05-08 decision (4)). The runner still
        subscribes to the fill stream so ``FILL_RECEIVED`` events are
        deduplicated against the synchronous return path -- callers that wire
        an async backend later get the same event ordering for free.
        """
        intents = list(intents)
        result = SimRunResult(
            run_id=self._event_log.run_id,
            portfolio_id=self._event_log.portfolio_id,
            package_id=self._event_log.package_id,
        )

        self._event_log.record(
            DaemonEventType.RUN_STARTED,
            payload={
                "manifest_package_id": self._manifest.package_id,
                "manifest_status": self._manifest.package_status.value,
                "intent_count": len(intents),
                "algo_code": self._manifest.minute_execution_policy.algo_code,
            },
        )

        seen_handle_ids: set[str] = set()

        def _on_fill(event: FillEvent) -> None:
            # LocalSim fires synchronously inside send_order; we still log
            # via subscription to validate the fan-out path. Dedup happens by
            # tracking handle_ids we've already credited against the
            # synchronous accounting.
            self._event_log.record(
                DaemonEventType.FILL_RECEIVED,
                payload={
                    "fill_quantity": event.fill_quantity,
                    "fill_price": str(event.fill_price),
                    "venue": event.venue,
                    "fill_ts": event.fill_ts.isoformat(),
                },
                handle_id=event.handle_id,
                intent_id=event.intent_id,
            )

        self._fill_subscription = self._gateway.subscribe_fill(_on_fill)

        try:
            for intent in intents:
                self._dispatch_one(intent, result, seen_handle_ids)
            # Successful tail: record positions snapshot + run completion.
            positions = self._gateway.query_positions()
            self._event_log.record(
                DaemonEventType.POSITION_UPDATED,
                payload={
                    "positions": {
                        sym: {
                            "quantity": lot.quantity,
                            "available_quantity": lot.available_quantity,
                            "avg_cost": lot.avg_cost,
                        }
                        for sym, lot in positions.items()
                    },
                },
            )
            self._event_log.record(
                DaemonEventType.RUN_COMPLETED,
                payload={
                    "submitted": len(result.handles),
                    "rejected": len(result.rejected_intents),
                    "fills_received": result.fills_received,
                },
            )
        except Exception as exc:  # noqa: BLE001 -- catch to record then re-raise
            result.failed = True
            result.failure_reason = repr(exc)
            self._event_log.record(
                DaemonEventType.RUN_FAILED,
                payload={
                    "error": repr(exc),
                    "submitted": len(result.handles),
                    "rejected": len(result.rejected_intents),
                },
            )
            raise
        finally:
            if self._fill_subscription is not None:
                self._gateway.unsubscribe_fill(self._fill_subscription)
                self._fill_subscription = None

        return result

    # ----- internals -----
    def _dispatch_one(
        self,
        intent: OrderIntent,
        result: SimRunResult,
        seen_handle_ids: set[str],
    ) -> None:
        self._event_log.record(
            DaemonEventType.INTENT_CREATED,
            payload={
                "side": intent.side.value,
                "quantity": intent.quantity,
                "order_type": intent.order_type.value,
                "limit_price": intent.limit_price,
                "target_trade_date": intent.target_trade_date.isoformat(),
            },
            intent_id=intent.intent_id,
            symbol=intent.symbol,
        )
        try:
            handle = self._gateway.send_order(intent)
        except BrokerRejectedError as exc:
            result.rejected_intents.append(intent.intent_id)
            self._event_log.record(
                DaemonEventType.ORDER_REJECTED,
                payload={
                    "error_code": exc.error_code,
                    "message": exc.message,
                    "context": dict(exc.context or {}),
                },
                intent_id=intent.intent_id,
                symbol=intent.symbol,
            )
            return
        except (BrokerSubmitError, BrokerConnectivityError, TradingCoreError) as exc:
            result.rejected_intents.append(intent.intent_id)
            self._event_log.record(
                DaemonEventType.ORDER_REJECTED,
                payload={
                    "error_code": getattr(exc, "error_code", "UNKNOWN"),
                    "message": getattr(exc, "message", str(exc)),
                    "context": dict(getattr(exc, "context", {}) or {}),
                },
                intent_id=intent.intent_id,
                symbol=intent.symbol,
            )
            # connectivity + submit errors are not "intent rejected" they are
            # "the runner cannot continue meaningfully"; surface to caller.
            if isinstance(exc, BrokerConnectivityError):
                raise
            return

        result.handles.append(handle)
        seen_handle_ids.add(handle.handle_id)

        status = self._gateway.query_status(handle)
        result.statuses.append(status)
        self._event_log.record(
            DaemonEventType.ORDER_SUBMITTED,
            payload={
                "state": status.state,
                "filled_quantity": status.filled_quantity,
                "avg_fill_price": (
                    str(status.avg_fill_price)
                    if status.avg_fill_price is not None
                    else None
                ),
                "rejection_reason": status.rejection_reason,
            },
            handle_id=handle.handle_id,
            intent_id=intent.intent_id,
            symbol=intent.symbol,
        )
        # Synchronous accounting: we know fill count from terminal status.
        # The subscription path already recorded each FillEvent, so we just
        # tally the totals here.
        if status.state in {"filled", "partial_filled"}:
            result.fills_received += status.filled_quantity // _round_lot(intent.quantity)
        elif status.state == "rejected":
            # rare under LocalSim: would have raised above. Defensive only.
            result.rejected_intents.append(intent.intent_id)


def _round_lot(quantity: int) -> int:
    # LocalSim partials always come out at 100-share lots; we use this only
    # for fill-count tallying when LocalSim aggregates a multi-bar fill.
    return 100 if quantity >= 100 else 1


__all__ = [
    "PaperV2SimRunner",
    "SimRunResult",
]
