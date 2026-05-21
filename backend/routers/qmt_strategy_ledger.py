"""Read-only MiniQMT multi-strategy virtual ledger API."""

from __future__ import annotations

from datetime import date
import os
from decimal import Decimal, InvalidOperation
from typing import Any, Callable

from fastapi import APIRouter, HTTPException

from backend.infra.qmt_client import QMTNotAvailableError, get_qmt_client_singleton
from backend.services.selection_center.repository import SelectionCenterRepository
from backend.services.strategy_package.repository import StrategyPackageRepository
from backend.services.strategy_package.selection_artifact import StrategyPackageSelectionArtifactRepository
from backend.services.strategy_package.service import StrategyPackageService
from backend.services.qmt_strategy_ledger.lot_availability import DbTradingCalendarProvider
from backend.services.qmt_strategy_ledger.order_service import (
    QmtManagedOrderService,
    cancel_request_from_payload,
    request_from_payload,
)
from backend.services.qmt_strategy_ledger.package_binding import PackageBindingRequest, QmtStrategyPackageBindingService
from backend.services.qmt_strategy_ledger.reconciliation import QmtStrategyLedgerReconciliationService
from backend.services.qmt_strategy_ledger.repository import QmtStrategyLedgerRepository
from backend.services.qmt_strategy_ledger.selection_order_builder import SelectionOrderBuilder, SelectionOrderBuildConfig
from backend.services.qmt_strategy_ledger.sync_service import QmtStrategyLedgerSyncService
from backend.services.simulation_runtime import MiniQMTExecutionBridge, SimulationBrokerBackend, SimulationRuntimeRepository
from backend.services.trading_core.errors import (
    DataUnavailableError,
    InvalidStateTransitionError,
    StrategyPackageValidationError,
    TradingCoreError,
)

router = APIRouter(prefix="/qmt/virtual-strategies", tags=["qmt-virtual-strategies"])

_repository_factory: Callable[[], Any] = QmtStrategyLedgerRepository
_client_factory: Callable[[], Any] = get_qmt_client_singleton
_package_reader_factory: Callable[[], Any] = StrategyPackageRepository
_selection_reader_factory: Callable[[], Any] = SelectionCenterRepository
_calendar_provider_factory: Callable[[], Any] = DbTradingCalendarProvider
_artifact_repository_factory: Callable[[], Any] | None = None
_live_approval_service_factory: Callable[[], Any] = StrategyPackageService
_simulation_runtime_repository_factory: Callable[[], Any] = SimulationRuntimeRepository


def configure_dependencies(
    *,
    repository_factory: Callable[[], Any] | None = None,
    client_factory: Callable[[], Any] | None = None,
    package_reader_factory: Callable[[], Any] | None = None,
    selection_reader_factory: Callable[[], Any] | None = None,
    calendar_provider_factory: Callable[[], Any] | None = None,
    artifact_repository_factory: Callable[[], Any] | None = None,
    live_approval_service_factory: Callable[[], Any] | None = None,
    simulation_runtime_repository_factory: Callable[[], Any] | None = None,
) -> None:
    """Override dependencies for tests without touching the production singleton."""

    global _repository_factory, _client_factory, _package_reader_factory, _selection_reader_factory, _calendar_provider_factory
    global _artifact_repository_factory, _live_approval_service_factory, _simulation_runtime_repository_factory
    if repository_factory is not None:
        _repository_factory = repository_factory
    if client_factory is not None:
        _client_factory = client_factory
    if package_reader_factory is not None:
        _package_reader_factory = package_reader_factory
    if selection_reader_factory is not None:
        _selection_reader_factory = selection_reader_factory
    if calendar_provider_factory is not None:
        _calendar_provider_factory = calendar_provider_factory
    if live_approval_service_factory is not None:
        _live_approval_service_factory = live_approval_service_factory
    if simulation_runtime_repository_factory is not None:
        _simulation_runtime_repository_factory = simulation_runtime_repository_factory
    _artifact_repository_factory = artifact_repository_factory


@router.post("/package-bindings", summary="Bind StrategyPackage identity and optional daily Selection Run to a virtual strategy")
def bind_package(payload: dict[str, Any]) -> dict[str, Any]:
    repository = _repository_factory()
    artifact_repository = (
        _artifact_repository_factory()
        if _artifact_repository_factory is not None
        else StrategyPackageSelectionArtifactRepository()
        if isinstance(repository, QmtStrategyLedgerRepository)
        else None
    )
    request = PackageBindingRequest(
        strategy_id=str(payload.get("strategy_id") or "").strip(),
        package_id=str(payload.get("package_id") or "").strip(),
        selection_run_id=str(payload.get("selection_run_id") or "").strip(),
        trade_date=_parse_optional_trade_date(payload.get("trade_date")),
        target_weight=_optional_decimal(payload.get("target_weight")),
        top_k=int(payload["top_k"]) if payload.get("top_k") not in (None, "") else None,
        runtime_config=dict(payload.get("runtime_config") or {}),
        replace_active=_parse_bool(payload.get("replace_active")),
        replacement_reason=str(payload.get("replacement_reason") or "").strip() or None,
    )
    try:
        result = QmtStrategyPackageBindingService(
            repository=repository,
            package_reader=_package_reader_factory(),
            selection_reader=_selection_reader_factory(),
            artifact_repository=artifact_repository,
        ).bind_with_result(request)
    except TradingCoreError as exc:
        _raise_trading_core_http(exc)
    binding = result.binding
    return {
        "success": True,
        "action": result.action,
        "binding": {
            "binding_id": binding.binding_id,
            "strategy_id": binding.strategy_id,
            "package_id": binding.package_id,
            "manifest_sha256": binding.manifest_sha256,
            "selection_run_id": binding.selection_run_id,
            "trade_date": binding.trade_date.isoformat() if binding.trade_date else None,
            "target_weight": float(binding.target_weight) if binding.target_weight is not None else None,
            "top_k": binding.top_k,
            "binding_status": binding.binding_status.value,
        },
        "replaced_binding": _package_binding_to_dict(result.replaced_binding) if result.replaced_binding else None,
        "daily_selection_evidence": _selection_evidence_to_dict(result.selection_evidence) if result.selection_evidence else None,
    }


@router.get("/summary", summary="Summarize AIstock virtual strategy accounts and active ledger state")
def virtual_strategy_summary(account_id: str | None = None, trade_date: str | None = None) -> dict[str, Any]:
    repository = _repository_factory()
    account_filter = str(account_id or "").strip() or None
    summary_date = _parse_optional_trade_date(trade_date) or date.today()
    accounts = repository.list_virtual_accounts(account_id=account_filter)

    strategy_summaries: list[dict[str, Any]] = []
    symbol_to_strategy_count: dict[str, int] = {}
    for account in accounts:
        positions = _summarize_strategy_positions(repository.list_position_lots(account.strategy_id))
        for position in positions:
            if int(position["remaining_quantity"]) > 0:
                symbol = str(position["symbol"])
                symbol_to_strategy_count[symbol] = symbol_to_strategy_count.get(symbol, 0) + 1
        active_binding = repository.get_active_package_binding(account.strategy_id)
        cash_entries = repository.list_cash_entries(account.strategy_id)
        strategy_summaries.append(
            {
                "strategy_id": account.strategy_id,
                "strategy_name": account.strategy_name,
                "display_name": account.display_name,
                "account_id": account.account_id,
                "mode": account.mode,
                "status": account.status.value,
                "initial_cash": _decimal_to_float(account.initial_cash),
                "cash": _decimal_to_float(account.cash),
                "frozen_cash": _decimal_to_float(account.frozen_cash),
                "market_value": _decimal_to_float(account.market_value),
                "realized_pnl": _decimal_to_float(account.realized_pnl),
                "unrealized_pnl": _decimal_to_float(account.unrealized_pnl),
                "total_equity": _decimal_to_float(account.cash + account.frozen_cash + account.market_value),
                "risk_config": account.risk_config,
                "metadata": account.metadata,
                "created_at": account.created_at.isoformat(),
                "updated_at": account.updated_at.isoformat(),
                "positions": positions,
                "cash_entries_count": len(cash_entries),
                "active_binding": _package_binding_to_dict(active_binding) if active_binding else None,
            }
        )

    unattributed_orders = repository.list_unattributed_orders(account_id=account_filter, trade_date=summary_date)
    unattributed_trades = repository.list_unattributed_trades(account_id=account_filter, trade_date=summary_date)
    return {
        "success": True,
        "summary": {
            "account_id": account_filter,
            "trade_date": summary_date.isoformat(),
            "strategy_count": len(strategy_summaries),
            "strategies": strategy_summaries,
            "overlap_symbols": sorted(symbol for symbol, count in symbol_to_strategy_count.items() if count > 1),
            "unattributed_orders": len(unattributed_orders),
            "unattributed_trades": len(unattributed_trades),
        },
    }


@router.post("/package-bindings/{binding_id}/orders/preview", summary="Build managed order requests from bound Selection Run")
def preview_orders_from_binding(binding_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    repository = _repository_factory()
    binding = repository.get_package_binding(binding_id)
    config = _selection_order_build_config(payload)
    trade_date = _parse_trade_date(payload.get("trade_date"))
    try:
        result = SelectionOrderBuilder(
            repository=repository,
            selection_reader=_selection_reader_factory(),
            calendar_provider=_calendar_provider_factory(),
        ).build_for_binding(
            binding=binding,
            trade_date=trade_date,
            config=config,
        )
    except TradingCoreError as exc:
        _raise_trading_core_http(exc)
    preflights = [
        QmtManagedOrderService(repository=repository, calendar_provider=_calendar_provider_factory()).preview_order(request).to_dict()
        for request in result.requests
    ]
    return {"success": True, "order_build": result.to_dict(), "preflights": preflights}


@router.post("/execution-plans/{plan_id}/orders/preview", summary="Preview MiniQMT managed orders from shared ExecutionPlan")
def preview_orders_from_execution_plan(plan_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    simulation_repo = _simulation_runtime_repository_factory()
    qmt_repository = _repository_factory()
    plan = simulation_repo.get_execution_plan(plan_id)
    binding = simulation_repo.get_simulation_release_binding(plan.binding_id)
    if binding.broker_backend != SimulationBrokerBackend.MINIQMT_SIM:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "EXECUTION_PLAN_BACKEND_MISMATCH",
                "message": "MiniQMT order preview requires a minqmt_sim SimulationReleaseBinding",
                "context": {"plan_id": plan_id, "binding_id": binding.binding_id, "broker_backend": binding.broker_backend.value},
            },
        )
    try:
        preview = MiniQMTExecutionBridge(
            managed_order_service=QmtManagedOrderService(repository=qmt_repository, calendar_provider=_calendar_provider_factory())
        ).preview_plan(
            plan=plan,
            binding=binding,
            mode=str(payload.get("mode") or "SIM").strip().upper(),
            price_type=int(payload.get("price_type") or 5),
            price_by_symbol=payload.get("price_by_symbol") if isinstance(payload.get("price_by_symbol"), dict) else None,
        )
    except TradingCoreError as exc:
        _raise_trading_core_http(exc)
    return {
        "success": True,
        "execution_plan": {
            "plan_id": plan.plan_id,
            "plan_hash": plan.plan_hash,
            "binding_id": plan.binding_id,
            "intent_count": len(plan.intents),
        },
        "requests": [_managed_request_to_dict(request) for request in preview.requests],
        "preflights": [preflight.to_dict() for preflight in preview.preflights],
    }


@router.post("/orders/preview", summary="Preview managed MiniQMT order without broker submission")
def preview_order(payload: dict[str, Any]) -> dict[str, Any]:
    repository = _repository_factory()
    request = request_from_payload(payload)
    result = QmtManagedOrderService(repository=repository, calendar_provider=_calendar_provider_factory()).preview_order(request)
    return {"success": True, "preflight": result.to_dict()}


@router.post("/orders", summary="Submit managed MiniQMT order after AIstock virtual strategy preflight")
def submit_order(payload: dict[str, Any]) -> dict[str, Any]:
    _require_real_managed_orders_enabled()
    repository = _repository_factory()
    request = request_from_payload(payload)
    _require_request_mode_allowed(request)
    result = QmtManagedOrderService(
        repository=repository,
        broker=_client_factory(),
        calendar_provider=_calendar_provider_factory(),
    ).submit_order(request)
    return {"success": result.success, "result": result.to_dict()}


@router.post("/orders/batch", summary="Submit managed MiniQMT order batch with item-level results")
def submit_order_batch(payload: dict[str, Any]) -> dict[str, Any]:
    _require_real_managed_orders_enabled()
    orders = payload.get("orders")
    if not isinstance(orders, list) or not orders:
        raise HTTPException(status_code=400, detail="orders must be a non-empty list")
    repository = _repository_factory()
    service = QmtManagedOrderService(
        repository=repository,
        broker=_client_factory(),
        calendar_provider=_calendar_provider_factory(),
    )
    requests = [request_from_payload(item) for item in orders]
    for request in requests:
        _require_request_mode_allowed(request)
    result = service.submit_batch(requests)
    return {"success": result.success, "result": result.to_dict()}


@router.post("/orders/cancel", summary="Cancel managed MiniQMT order and release local frozen cash")
def cancel_order(payload: dict[str, Any]) -> dict[str, Any]:
    _require_real_managed_orders_enabled()
    repository = _repository_factory()
    request = cancel_request_from_payload(payload)
    _require_request_mode_allowed(request.mode)
    if not request.qmt_order_id:
        raise HTTPException(status_code=400, detail="qmt_order_id or order_id is required")
    result = QmtManagedOrderService(repository=repository, broker=_client_factory()).cancel_order(request)
    return {"success": result.success, "result": result.to_dict()}


@router.post("/sync-snapshot", summary="Read-only sync of MiniQMT orders/trades into virtual strategy ledger")
def sync_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
    account_id = str(payload.get("account_id") or "").strip()
    trade_date = _parse_trade_date(payload.get("trade_date"))
    if not account_id:
        raise HTTPException(status_code=400, detail="account_id is required")

    repository = _repository_factory()
    client = _client_factory()
    try:
        summary = QmtStrategyLedgerSyncService(
            repository=repository,
            qmt_client=client,
            account_id=account_id,
            trade_date=trade_date,
            calendar_provider=_calendar_provider_factory(),
        ).sync_snapshot()
    except QMTNotAvailableError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"success": True, "summary": summary.to_dict()}


@router.post("/reconciliation", summary="Read-only reconciliation between virtual strategy lots and MiniQMT positions")
def reconcile(payload: dict[str, Any]) -> dict[str, Any]:
    account_id = str(payload.get("account_id") or "").strip()
    trade_date = _parse_trade_date(payload.get("trade_date"))
    if not account_id:
        raise HTTPException(status_code=400, detail="account_id is required")

    repository = _repository_factory()
    client = _client_factory()
    try:
        broker_positions = client.get_positions()
    except QMTNotAvailableError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    report = QmtStrategyLedgerReconciliationService(repository=repository).reconcile_snapshot(
        account_id=account_id,
        trade_date=trade_date,
        broker_positions=broker_positions,
    )
    return {"success": True, "report": report.to_dict()}


def _parse_trade_date(value: Any) -> date:
    if value is None or str(value).strip() == "":
        return date.today()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="trade_date must be YYYY-MM-DD") from exc


def _parse_optional_trade_date(value: Any) -> date | None:
    if value is None or str(value).strip() == "":
        return None
    return _parse_trade_date(value)


def _raise_trading_core_http(exc: TradingCoreError) -> None:
    status_code = 400
    if isinstance(exc, DataUnavailableError):
        status_code = 404
    elif isinstance(exc, InvalidStateTransitionError):
        status_code = 409
    elif isinstance(exc, StrategyPackageValidationError):
        status_code = 422
    raise HTTPException(status_code=status_code, detail=exc.to_dict()) from exc


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _decimal_to_float(value: Decimal) -> float:
    return float(value)


def _package_binding_to_dict(binding: Any) -> dict[str, Any]:
    return {
        "binding_id": binding.binding_id,
        "strategy_id": binding.strategy_id,
        "package_id": binding.package_id,
        "manifest_sha256": binding.manifest_sha256,
        "selection_run_id": binding.selection_run_id,
        "trade_date": binding.trade_date.isoformat() if binding.trade_date else None,
        "target_weight": _decimal_to_float(binding.target_weight) if binding.target_weight is not None else None,
        "top_k": binding.top_k,
        "binding_status": binding.binding_status.value,
        "runtime_config": binding.runtime_config,
        "created_at": binding.created_at.isoformat(),
        "updated_at": binding.updated_at.isoformat(),
    }


def _selection_evidence_to_dict(evidence: Any) -> dict[str, Any]:
    return {
        "evidence_id": evidence.evidence_id,
        "binding_id": evidence.binding_id,
        "strategy_id": evidence.strategy_id,
        "package_id": evidence.package_id,
        "selection_run_id": evidence.selection_run_id,
        "trade_date": evidence.trade_date.isoformat(),
        "data_source": evidence.data_source,
        "manifest_sha256": evidence.manifest_sha256,
        "runtime_config_hash": evidence.runtime_config_hash,
        "artifact_id": evidence.artifact_id,
        "artifact_sha256": evidence.artifact_sha256,
        "source_type": evidence.source_type,
        "authority_scope": evidence.authority_scope,
        "score_count": evidence.score_count,
        "metadata": evidence.metadata,
        "created_at": evidence.created_at.isoformat(),
    }


def _summarize_strategy_positions(lots: list[Any]) -> list[dict[str, Any]]:
    by_symbol: dict[str, dict[str, Any]] = {}
    for lot in lots:
        if lot.remaining_quantity <= 0:
            continue
        row = by_symbol.setdefault(
            lot.symbol,
            {
                "symbol": lot.symbol,
                "quantity": 0,
                "available_quantity": 0,
                "remaining_quantity": 0,
                "cost_amount": Decimal("0"),
                "realized_pnl": Decimal("0"),
                "lot_count": 0,
            },
        )
        row["quantity"] += lot.quantity
        row["available_quantity"] += lot.available_quantity
        row["remaining_quantity"] += lot.remaining_quantity
        row["cost_amount"] += lot.cost_amount
        row["realized_pnl"] += lot.realized_pnl
        row["lot_count"] += 1

    positions: list[dict[str, Any]] = []
    for symbol, row in sorted(by_symbol.items()):
        remaining = int(row["remaining_quantity"])
        cost_amount = row["cost_amount"]
        avg_cost = cost_amount / Decimal(remaining) if remaining > 0 else Decimal("0")
        positions.append(
            {
                "symbol": symbol,
                "quantity": int(row["quantity"]),
                "available_quantity": int(row["available_quantity"]),
                "remaining_quantity": remaining,
                "cost_amount": _decimal_to_float(cost_amount),
                "avg_cost": _decimal_to_float(avg_cost),
                "realized_pnl": _decimal_to_float(row["realized_pnl"]),
                "lot_count": int(row["lot_count"]),
            }
        )
    return positions


def _optional_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise HTTPException(status_code=400, detail="decimal field is invalid") from exc


def _decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    if value is None or value == "":
        return default
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise HTTPException(status_code=400, detail="decimal field is invalid") from exc


def _managed_request_to_dict(request: Any) -> dict[str, Any]:
    return {
        "account_id": request.account_id,
        "strategy_name": request.strategy_name,
        "symbol": request.symbol,
        "side": request.side,
        "order_type": request.order_type,
        "quantity": request.quantity,
        "price_type": request.price_type,
        "price": float(request.price),
        "order_remark": request.order_remark,
        "trade_date": request.trade_date.isoformat(),
        "mode": request.mode,
        "package_id": request.package_id,
        "target_weight": float(request.target_weight) if request.target_weight is not None else None,
        "metadata": request.metadata,
    }


def _selection_order_build_config(payload: dict[str, Any]) -> SelectionOrderBuildConfig:
    return SelectionOrderBuildConfig(
        default_target_weight=_optional_decimal(payload.get("default_target_weight")),
        top_k=int(payload["top_k"]) if payload.get("top_k") not in (None, "") else None,
        price_type=int(payload.get("price_type") or 5),
        buy_price_slippage_bps=_decimal(payload.get("buy_price_slippage_bps"), Decimal("0")),
        sell_price_slippage_bps=_decimal(payload.get("sell_price_slippage_bps"), Decimal("0")),
        order_remark_prefix=str(payload.get("order_remark_prefix") or "qmtpkg").strip(),
        mode=str(payload.get("mode") or "SIM").strip().upper(),
    )


def _real_managed_orders_enabled() -> bool:
    values = {
        (os.getenv("AISTOCK_ALLOW_MINIQMT_MANAGED_ORDERS") or "").strip().lower(),
        (os.getenv("AISTOCK_ALLOW_MINIQMT_SUBMIT_TEST") or "").strip().lower(),
    }
    return bool(values & {"1", "true", "yes", "on"})


def _require_real_managed_orders_enabled() -> None:
    if not _real_managed_orders_enabled():
        raise HTTPException(
            status_code=403,
            detail=(
                "managed MiniQMT order submission is disabled by default; set "
                "AISTOCK_ALLOW_MINIQMT_MANAGED_ORDERS=1 or AISTOCK_ALLOW_MINIQMT_SUBMIT_TEST=1 explicitly"
            ),
        )


def _live_managed_orders_enabled() -> bool:
    return (os.getenv("AISTOCK_ALLOW_MINIQMT_LIVE_MANAGED_ORDERS") or "").strip().lower() in {"1", "true", "yes", "on"}


def _require_request_mode_allowed(request_or_mode: Any) -> None:
    if isinstance(request_or_mode, str):
        normalized = str(request_or_mode or "").strip().upper()
        request = None
    else:
        request = request_or_mode
        normalized = str(getattr(request, "mode", "") or "").strip().upper()
    if normalized == "SIM":
        return
    if normalized == "LIVE" and _live_managed_orders_enabled():
        if request is not None:
            _require_live_approval_for_managed_order(request)
        return
    raise HTTPException(
        status_code=403,
        detail="managed order submission currently allows SIM only unless AISTOCK_ALLOW_MINIQMT_LIVE_MANAGED_ORDERS=1",
    )


def _require_live_approval_for_managed_order(request: Any) -> None:
    metadata = dict(getattr(request, "metadata", {}) or {})
    package_id = str(getattr(request, "package_id", "") or "").strip()
    approval_id = str(metadata.get("live_approval_id") or "").strip()
    runtime_release_sha256 = str(metadata.get("runtime_release_sha256") or "").strip()
    if not package_id or not approval_id or not runtime_release_sha256:
        raise HTTPException(
            status_code=403,
            detail={
                "code": "MINIQMT_LIVE_APPROVAL_REQUIRED",
                "message": (
                    "LIVE managed MiniQMT order submission requires package_id, "
                    "metadata.live_approval_id, and metadata.runtime_release_sha256"
                ),
                "context": {
                    "package_id_present": bool(package_id),
                    "live_approval_id_present": bool(approval_id),
                    "runtime_release_sha256_present": bool(runtime_release_sha256),
                },
            },
        )
    try:
        _live_approval_service_factory().require_live_approval(
            package_id=package_id,
            approval_id=approval_id,
            runtime_release_sha256=runtime_release_sha256,
            target_broker_backend="minqmt_live",
        )
    except TradingCoreError as exc:
        raise HTTPException(status_code=403, detail=exc.to_dict()) from exc
