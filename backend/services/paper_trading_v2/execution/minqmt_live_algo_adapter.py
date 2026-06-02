"""Paper v2 compatibility layer for shared MiniQMT vn.py-style execution."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable

from backend.services.paper_trading_v2.broker.base import BrokerBackend, OrderHandle, OrderHandleStatus
from backend.services.trading_core.miniqmt_vnpy_execution import (
    MiniQMTAlgoChildOrder,
    MiniQMTAlgoExecutionResult,
    MiniQMTCancelResult,
    MiniQMTChildOrderHandle,
    MiniQMTChildOrderRequest,
    MiniQMTChildOrderSubmitResult,
    MiniQMTChildOrderSubmitter,
    MiniQMTOrderStatus,
    UnifiedMiniQMTVnpyExecutionAdapter,
)

UTC = timezone.utc


class PaperV2MiniQMTSimSubmitter(MiniQMTChildOrderSubmitter):
    """Submit shared vn.py child requests through the Paper v2 broker boundary."""

    def __init__(self, *, broker: BrokerBackend) -> None:
        self.broker = broker
        self._paper_handles: dict[str, OrderHandle] = {}

    def submit_child(self, request: MiniQMTChildOrderRequest) -> MiniQMTChildOrderSubmitResult:
        handle = self.broker.submit_order_intent(request.child_intent)
        native = _safe_order_context(self.broker, handle)
        shared_handle = _shared_handle_from_paper(handle, native)
        self._paper_handles[shared_handle.handle_id] = handle
        status = _shared_status_from_paper(self.broker.query_status(handle))
        trades = self.broker.query_trades(handle)
        return MiniQMTChildOrderSubmitResult(
            handle=shared_handle,
            status=status,
            trades=[dict(row) for row in trades],
            native_context=native,
        )

    def cancel_child(self, handle: MiniQMTChildOrderHandle, *, action: Any, reason: str) -> MiniQMTCancelResult:  # noqa: ARG002
        ack = self.broker.cancel(self._paper_handle(handle))
        return MiniQMTCancelResult(accepted=bool(ack.accepted), reason=ack.reason, raw=ack.model_dump(mode="json"))

    def query_order(self, handle: MiniQMTChildOrderHandle) -> MiniQMTOrderStatus | None:
        return _shared_status_from_paper(self.broker.query_status(self._paper_handle(handle)))

    def query_trades(self, handle: MiniQMTChildOrderHandle) -> list[dict[str, Any]]:
        return [dict(row) for row in self.broker.query_trades(self._paper_handle(handle))]

    def _paper_handle(self, handle: MiniQMTChildOrderHandle) -> OrderHandle:
        existing = self._paper_handles.get(handle.handle_id)
        if existing is not None:
            return existing
        return OrderHandle(
            handle_id=handle.handle_id,
            backend_id="minqmt_sim",
            submitted_at=datetime.now(UTC),
            intent_id=handle.intent_id or str(handle.native_context.get("intent_id") or handle.handle_id),
        )


class MiniQMTLiveAlgoAdapter(UnifiedMiniQMTVnpyExecutionAdapter):
    """Backward-compatible Paper v2 adapter delegating to the shared adapter."""

    def __init__(
        self,
        *,
        broker: BrokerBackend,
        policy_context: dict[str, Any],
        quote_provider: Callable[[str], dict[str, Any] | None] | None = None,
        now_provider: Callable[[], datetime] | None = None,
        random_volume_provider: Callable[[int, int], float] | None = None,
    ) -> None:
        super().__init__(
            submitter=PaperV2MiniQMTSimSubmitter(broker=broker),
            policy_context=policy_context,
            quote_provider=quote_provider,
            now_provider=now_provider,
            random_volume_provider=random_volume_provider,
        )


def _safe_order_context(broker: BrokerBackend, handle: OrderHandle) -> dict[str, Any]:
    if hasattr(broker, "order_context"):
        try:
            return dict(broker.order_context(handle))  # type: ignore[attr-defined]
        except Exception as exc:  # noqa: BLE001 - preserve broker diagnostics
            return {"order_context_error": f"{type(exc).__name__}: {exc}"}
    return {"handle_id": handle.handle_id, "intent_id": handle.intent_id}


def _shared_handle_from_paper(handle: OrderHandle, native: dict[str, Any]) -> MiniQMTChildOrderHandle:
    return MiniQMTChildOrderHandle(
        handle_id=handle.handle_id,
        intent_id=handle.intent_id,
        native_order_id=str(native.get("miniqmt_order_id") or native.get("qmt_order_id") or "") or None,
        native_context={
            **dict(native),
            "paper_backend_id": handle.backend_id,
            "paper_submitted_at": handle.submitted_at.isoformat(),
        },
    )


def _shared_status_from_paper(status: OrderHandleStatus) -> MiniQMTOrderStatus:
    return MiniQMTOrderStatus(
        handle_id=status.handle_id,
        state=status.state,
        filled_quantity=int(status.filled_quantity),
        avg_fill_price=status.avg_fill_price,
        last_event_at=status.last_event_at,
        rejection_reason=status.rejection_reason,
        raw_status=status.raw_status,
        status_msg=status.status_msg,
        raw=dict(status.raw),
    )


__all__ = [
    "MiniQMTAlgoChildOrder",
    "MiniQMTAlgoExecutionResult",
    "MiniQMTLiveAlgoAdapter",
    "PaperV2MiniQMTSimSubmitter",
    "UnifiedMiniQMTVnpyExecutionAdapter",
]
