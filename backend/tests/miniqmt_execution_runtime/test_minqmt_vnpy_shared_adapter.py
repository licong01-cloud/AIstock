from __future__ import annotations

from datetime import date, timezone
from typing import Any


from backend.execution_algos.vnpy_style import VnpyAction
from backend.services.trading_core.miniqmt_vnpy_execution import (
    MiniQMTCancelResult,
    MiniQMTChildOrderHandle,
    MiniQMTChildOrderRequest,
    MiniQMTChildOrderSubmitResult,
    MiniQMTOrderStatus,
    UnifiedMiniQMTVnpyExecutionAdapter,
)
from backend.services.trading_core.models import OrderIntent, OrderSide, OrderType

UTC = timezone.utc
TRADE_DATE = date(2026, 5, 21)


class RecordingChildSubmitter:
    def __init__(self, *, status: MiniQMTOrderStatus | None = None) -> None:
        self.requests: list[MiniQMTChildOrderRequest] = []
        self.cancelled: list[tuple[str, str]] = []
        self.status = status

    def submit_child(self, request: MiniQMTChildOrderRequest) -> MiniQMTChildOrderSubmitResult:
        self.requests.append(request)
        handle = MiniQMTChildOrderHandle(
            handle_id=f"handle_{len(self.requests)}",
            intent_id=request.child_intent.intent_id,
            native_order_id=f"native_{len(self.requests)}",
            native_context={"raw_submit": {"status": 50}},
        )
        status = self.status or MiniQMTOrderStatus(
            handle_id=handle.handle_id,
            state="pending",
            raw_status=50,
            status_msg="reported pending",
            raw={"order_status": 50, "status_msg": "reported pending"},
        )
        return MiniQMTChildOrderSubmitResult(
            handle=handle,
            status=status,
            native_context={"miniqmt_order_id": handle.native_order_id, "raw_submit": {"status": 50}},
        )

    def cancel_child(self, handle: MiniQMTChildOrderHandle, *, action: VnpyAction, reason: str) -> MiniQMTCancelResult:
        self.cancelled.append((handle.handle_id, reason or action.reason))
        return MiniQMTCancelResult(accepted=True, reason=reason, raw={"handle_id": handle.handle_id})

    def query_order(self, handle: MiniQMTChildOrderHandle) -> MiniQMTOrderStatus | None:
        return MiniQMTOrderStatus(handle_id=handle.handle_id, state="cancelled", raw_status="cancelled")

    def query_trades(self, handle: MiniQMTChildOrderHandle) -> list[dict[str, Any]]:  # noqa: ARG002
        return []


def _policy_context(algo_code: str, algo_config: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "validated_execution_policy_id": "execpol_shared_vnpy",
        "policy_sha256": "sha_shared_vnpy",
        "policy_json": {"algo_code": algo_code, "algo_config": dict(algo_config or {})},
    }


def _parent_intent(*, side: OrderSide = OrderSide.BUY, limit_price: float = 10.0, quantity: int = 200) -> OrderIntent:
    return OrderIntent(
        intent_id="parent_intent_shared",
        package_id="pkg_shared",
        portfolio_id="portfolio_shared",
        symbol="000001.SZ",
        side=side,
        quantity=quantity,
        order_type=OrderType.LIMIT,
        limit_price=limit_price,
        target_trade_date=TRADE_DATE,
        metadata={"execution_plan_intent_id": "plan_intent_shared"},
    )


def test_unified_adapter_routes_best_limit_child_action_with_policy_lineage() -> None:
    submitter = RecordingChildSubmitter()
    adapter = UnifiedMiniQMTVnpyExecutionAdapter(
        submitter=submitter,
        policy_context=_policy_context("BEST_LIMIT_MINIQMT", {"min_volume": 100, "max_volume": 100}),
        quote_provider=lambda symbol: {
            "symbol": symbol,
            "bid_price_1": 9.95,
            "bid_volume_1": 500,
            "ask_price_1": 10.05,
            "ask_volume_1": 500,
            "time": "20260521093100",
        },
    )

    result = adapter.execute_intent(_parent_intent(limit_price=10.50), trade_date=TRADE_DATE)

    assert result.terminal_state == "PENDING"
    assert len(submitter.requests) == 1
    child = submitter.requests[0].child_intent
    assert child.limit_price == 9.95
    assert child.metadata["execution_algo_code"] == "BEST_LIMIT_MINIQMT"
    assert child.metadata["execution_policy_id"] == "execpol_shared_vnpy"
    assert child.metadata["source_attribution"]["upstream_source_file"].endswith("best_limit_algo.py")
    assert result.diagnostic["adapter"] == "UnifiedMiniQMTVnpyExecutionAdapter"
    assert result.diagnostic["child_orders"][0]["status"]["raw_status"] == 50


def test_unified_adapter_preserves_rejected_child_raw_status_msg() -> None:
    submitter = RecordingChildSubmitter(
        status=MiniQMTOrderStatus(
            handle_id="handle_1",
            state="rejected",
            raw_status=57,
            status_msg="[COUNTER][260200] insufficient buying power",
            rejection_reason="[COUNTER][260200] insufficient buying power",
            raw={"order_status": 57, "status_msg": "[COUNTER][260200] insufficient buying power"},
        )
    )
    adapter = UnifiedMiniQMTVnpyExecutionAdapter(
        submitter=submitter,
        policy_context=_policy_context("SNIPER_MINIQMT"),
    )

    result = adapter.execute_intent(_parent_intent(), trade_date=TRADE_DATE)

    assert result.terminal_state == "REJECTED"
    status = result.diagnostic["child_orders"][0]["status"]
    assert status["raw_status"] == 57
    assert status["status_msg"].startswith("[COUNTER][260200]")
