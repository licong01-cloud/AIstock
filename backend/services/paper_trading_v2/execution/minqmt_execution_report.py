"""MiniQMT execution quality and broker-cost reconciliation report.

The report is read-only: it summarizes Paper v2 orders/fills that were already
created from MiniQMT broker authority. It does not submit, cancel, or query
orders, and it does not mutate portfolio/account state.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

from backend.services.trading_core.ledger import FeeModel
from backend.services.trading_core.models import Fill, Order, OrderStatus

REPORT_SCHEMA_VERSION = "miniqmt_execution_quality_report_v1"
FEE_MODEL_VERSION = "paper_v2_fee_model_v1"
EXECUTION_QUALITY_EVENT_TYPE = "MINIQMT_EXECUTION_QUALITY_REPORTED"
EXECUTION_QUALITY_SNAPSHOT_METADATA_KEY = "execution_quality_report"

TERMINAL_OR_DIAGNOSTIC_STATUSES = {
    OrderStatus.REJECTED.value,
    OrderStatus.CANCELLED.value,
    OrderStatus.PARTIALLY_FILLED.value,
    OrderStatus.SUBMITTED.value,
    OrderStatus.PENDING.value,
}


def build_minqmt_execution_quality_report(
    *,
    portfolio_id: str,
    run_id: str,
    trade_date: date,
    orders: list[Order],
    fills: list[Fill],
    fee_model: FeeModel | None = None,
    fill_count_override: int | None = None,
    report_scope: str = "current_run_result",
) -> dict[str, Any]:
    """Build a JSON-safe cost and execution quality report for MiniQMT runs."""

    fee_model = fee_model or FeeModel()
    fill_items = [_fill_cost_item(fill, orders=orders, fee_model=fee_model) for fill in fills]
    order_items = [_order_diagnostic_item(order) for order in orders]
    persisted_fill_count = len(fills) if fill_count_override is None else int(fill_count_override)
    broker_fee_total = sum(_num(item.get("broker_reported_fee_total")) for item in fill_items)
    estimated_fee_total = sum(_num(item.get("estimated_fee_total")) for item in fill_items)
    traded_amount_total = sum(_num(item.get("trade_amount")) for item in fill_items)
    intended_amount_total = _intended_amount_total(orders)
    filled_quantity = sum(int(getattr(order, "filled_quantity", 0) or 0) for order in orders)
    ordered_quantity = sum(int(getattr(order, "quantity", 0) or 0) for order in orders)
    slippage_weighted_bps = _weighted_slippage_bps(fill_items)
    diagnostic_coverage = _diagnostic_coverage(order_items)
    precision_counts = _count_by(fill_items, "cost_precision_level")
    status_counts = _status_counts(order_items)
    reconciliation_delta = broker_fee_total - estimated_fee_total
    warning_flags = _warning_flags(
        fill_items=fill_items,
        order_items=order_items,
        persisted_fill_count=persisted_fill_count,
        materialized_fill_count=len(fills),
    )
    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "portfolio_id": portfolio_id,
        "run_id": run_id,
        "trade_date": trade_date.isoformat(),
        "generated_at": datetime.now(UTC).isoformat(),
        "report_scope": report_scope,
        "fee_model": {
            "version": FEE_MODEL_VERSION,
            "open_cost": fee_model.open_cost,
            "close_cost": fee_model.close_cost,
            "min_cost": fee_model.min_cost,
            "estimated_stamp_tax_rate": _estimated_stamp_tax_rate(fee_model),
            "breakdown_precision": "estimated_from_fee_model_not_broker_confirmed",
        },
        "summary": {
            "order_count": len(orders),
            "status_counts": status_counts,
            "persisted_fill_count": persisted_fill_count,
            "materialized_fill_count": len(fills),
            "fill_detail_scope": (
                "all_materialized_fills" if persisted_fill_count == len(fills) else "current_new_fills_only"
            ),
            "ordered_quantity": ordered_quantity,
            "filled_quantity": filled_quantity,
            "fill_rate_by_quantity": _ratio(filled_quantity, ordered_quantity),
            "intended_amount_total": intended_amount_total,
            "trade_amount_total": traded_amount_total,
            "broker_reported_fee_total": broker_fee_total,
            "estimated_fee_total": estimated_fee_total,
            "cost_reconciliation_delta": reconciliation_delta,
            "cost_reconciliation_delta_bps": _bps(reconciliation_delta, traded_amount_total),
            "weighted_slippage_bps": slippage_weighted_bps,
            "cost_precision_counts": precision_counts,
            "diagnostic_coverage": diagnostic_coverage,
            "warning_flags": warning_flags,
        },
        "fills": fill_items,
        "orders_requiring_attention": [
            item for item in order_items if item["status"] in TERMINAL_OR_DIAGNOSTIC_STATUSES or item["remaining_quantity"] > 0
        ],
    }


def list_minqmt_execution_quality_reports(
    *,
    repository: Any,
    portfolio_id: str,
    trade_date: date | None = None,
    run_id: str | None = None,
    limit: int = 20,
    scan_limit: int = 500,
) -> dict[str, Any]:
    """Read persisted MiniQMT execution reports from snapshots/events.

    This is intentionally read-only: it only consumes daily snapshot metadata and
    run events that were already persisted by the Paper v2 runner.
    """

    portfolio = repository.get_portfolio(portfolio_id)
    requested_limit = max(1, int(limit))
    requested_scan_limit = max(requested_limit, int(scan_limit))
    raw_records: list[dict[str, Any]] = []
    skipped = 0
    source_counts = {"daily_snapshot_metadata": 0, "run_event": 0}

    for snapshot in repository.list_daily_snapshots(portfolio_id, limit=requested_scan_limit):
        report = _report_from_snapshot(snapshot)
        if report is None:
            continue
        if not _report_matches(report, trade_date=trade_date, run_id=run_id):
            continue
        source_counts["daily_snapshot_metadata"] += 1
        raw_records.append(
            _report_record(
                report,
                source={
                    "source_type": "daily_snapshot_metadata",
                    "run_id": snapshot.get("run_id"),
                    "trade_date": _iso_date(snapshot.get("trade_date")),
                    "snapshot_time": _iso_datetime(snapshot.get("snapshot_time")),
                    "created_at": _iso_datetime(snapshot.get("created_at")),
                    "updated_at": _iso_datetime(snapshot.get("updated_at")),
                },
            )
        )

    for event in repository.list_run_events(portfolio_id, run_id=run_id, limit=requested_scan_limit):
        if event.get("event_type") != EXECUTION_QUALITY_EVENT_TYPE:
            continue
        context = event.get("context") if isinstance(event.get("context"), dict) else {}
        if not _is_execution_quality_report(context):
            skipped += 1
            continue
        if not _report_matches(context, trade_date=trade_date, run_id=run_id):
            continue
        source_counts["run_event"] += 1
        raw_records.append(
            _report_record(
                context,
                source={
                    "source_type": "run_event",
                    "run_id": event.get("run_id") or context.get("run_id"),
                    "trade_date": _iso_date(context.get("trade_date")),
                    "event_seq": event.get("event_seq"),
                    "created_at": _iso_datetime(event.get("created_at")),
                },
            )
        )

    reports = _dedupe_report_records(raw_records)
    reports.sort(key=_report_sort_key, reverse=True)
    limited = reports[:requested_limit]
    warnings: list[dict[str, Any]] = []
    broker_backend = getattr(portfolio, "broker_backend", None)
    if getattr(broker_backend, "value", broker_backend) != "minqmt_sim":
        warnings.append(
            {
                "code": "PORTFOLIO_NOT_MINIQMT_SIM",
                "message": "该组合不是 MiniQMT 模拟盘，执行质量报告通常不会产生",
            }
        )
    if not limited:
        warnings.append(
            {
                "code": "NO_EXECUTION_QUALITY_REPORT",
                "message": "尚未找到 MiniQMT 执行质量报告；需等待一次 MiniQMT run 完成后生成",
            }
        )
    if skipped:
        warnings.append(
            {
                "code": "MALFORMED_EXECUTION_QUALITY_EVENT_SKIPPED",
                "message": "发现事件类型匹配但内容不是执行质量报告，已跳过",
                "count": skipped,
            }
        )
    return {
        "schema_version": "miniqmt_execution_quality_query_v1",
        "portfolio_id": portfolio_id,
        "filters": {
            "trade_date": trade_date.isoformat() if trade_date else None,
            "run_id": run_id,
            "limit": requested_limit,
            "scan_limit": requested_scan_limit,
        },
        "source_counts": source_counts,
        "report_count": len(limited),
        "available_report_count": len(reports),
        "latest_record": limited[0] if limited else None,
        "latest_report": limited[0]["report"] if limited else None,
        "reports": limited,
        "warnings": warnings,
    }


def _fill_cost_item(fill: Fill, *, orders: list[Order], fee_model: FeeModel) -> dict[str, Any]:
    order = next((item for item in orders if item.order_id == fill.order_id), None)
    metadata = dict(fill.metadata or {})
    trade_amount = _positive_or_default(metadata.get("trade_amount"), fill.quantity * fill.price)
    broker_fee = _metadata_number(metadata, "broker_reported_fee_total")
    broker_commission = _metadata_number(metadata, "broker_reported_commission")
    precision = str(metadata.get("cost_precision_level") or _infer_cost_precision(metadata))
    intended_price = getattr(order, "limit_price", None) if order is not None else metadata.get("intended_price")
    estimated_fee_total = fee_model.calculate(fill)
    estimated_stamp_tax = trade_amount * _estimated_stamp_tax_rate(fee_model) if fill.side.value == "SELL" else 0.0
    estimated_broker_commission = max(0.0, estimated_fee_total - estimated_stamp_tax)
    return {
        "fill_id": fill.fill_id,
        "order_id": fill.order_id,
        "symbol": fill.symbol,
        "side": fill.side.value,
        "quantity": fill.quantity,
        "price": fill.price,
        "intended_price": intended_price,
        "slippage_bps": _slippage_bps(intended_price, fill.price),
        "trade_amount": trade_amount,
        "broker_reported_commission": broker_commission,
        "broker_reported_fee_total": broker_fee,
        "cost_precision_level": precision,
        "cost_breakdown_source": str(metadata.get("cost_breakdown_source") or "estimated_only"),
        "estimated_fee_total": estimated_fee_total,
        "estimated_broker_commission_or_fee": estimated_broker_commission,
        "estimated_stamp_tax": estimated_stamp_tax,
        "estimated_transfer_fee": None,
        "cost_reconciliation_delta": None if broker_fee is None else broker_fee - estimated_fee_total,
        "cost_reconciliation_delta_bps": None if broker_fee is None else _bps(broker_fee - estimated_fee_total, trade_amount),
        "source_note": (
            "broker fee is an aggregate MiniQMT field; tax/transfer breakdown is estimated and not broker-confirmed"
        ),
    }


def _order_diagnostic_item(order: Order) -> dict[str, Any]:
    metadata = dict(order.metadata or {})
    has_diag = any(
        metadata.get(key) is not None
        for key in (
            "broker_status_msg",
            "broker_rejection_reason",
            "broker_status_raw",
            "broker_diagnostic",
            "child_submit_error",
            "execution_diagnostic",
        )
    )
    return {
        "order_id": order.order_id,
        "intent_id": order.intent_id,
        "symbol": order.symbol,
        "side": order.side.value,
        "status": order.status.value,
        "quantity": order.quantity,
        "filled_quantity": order.filled_quantity,
        "remaining_quantity": order.remaining_quantity,
        "limit_price": order.limit_price,
        "broker_handle_id": metadata.get("broker_handle_id"),
        "miniqmt_order_id": metadata.get("miniqmt_order_id"),
        "broker_raw_status": metadata.get("broker_raw_status"),
        "broker_status_msg": metadata.get("broker_status_msg"),
        "broker_rejection_reason": metadata.get("broker_rejection_reason"),
        "execution_algo_code": metadata.get("execution_algo_code"),
        "has_diagnostic": has_diag,
    }


def _estimated_stamp_tax_rate(fee_model: FeeModel) -> float:
    return max(0.0, float(fee_model.close_cost) - float(fee_model.open_cost))


def _metadata_number(metadata: dict[str, Any], key: str) -> float | None:
    if key not in metadata or metadata.get(key) is None:
        return None
    return _num(metadata.get(key))


def _positive_or_default(value: Any, default: float) -> float:
    parsed = _num(value)
    return parsed if parsed > 0 else float(default)


def _num(value: Any) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return 0.0
    if parsed != parsed:
        return 0.0
    return parsed


def _ratio(numerator: float | int, denominator: float | int) -> float | None:
    denominator_f = float(denominator or 0)
    if denominator_f <= 0:
        return None
    return float(numerator) / denominator_f


def _bps(value: float, notional: float) -> float | None:
    if notional <= 0:
        return None
    return value / notional * 10000.0


def _slippage_bps(intended_price: Any, fill_price: Any) -> float | None:
    intended = _num(intended_price)
    filled = _num(fill_price)
    if intended <= 0 or filled <= 0:
        return None
    return (filled - intended) / intended * 10000.0


def _weighted_slippage_bps(items: list[dict[str, Any]]) -> float | None:
    weighted = 0.0
    weight = 0.0
    for item in items:
        bps = item.get("slippage_bps")
        amount = _num(item.get("trade_amount"))
        if bps is None or amount <= 0:
            continue
        weighted += float(bps) * amount
        weight += amount
    if weight <= 0:
        return None
    return weighted / weight


def _status_counts(items: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        status = str(item.get("status") or "UNKNOWN")
        counts[status] = counts.get(status, 0) + 1
    return counts


def _count_by(items: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        value = str(item.get(key) or "unknown")
        counts[value] = counts.get(value, 0) + 1
    return counts


def _diagnostic_coverage(order_items: list[dict[str, Any]]) -> dict[str, Any]:
    attention = [
        item
        for item in order_items
        if item["status"] in TERMINAL_OR_DIAGNOSTIC_STATUSES or int(item.get("remaining_quantity") or 0) > 0
    ]
    covered = [item for item in attention if item.get("has_diagnostic")]
    return {
        "orders_requiring_diagnostic": len(attention),
        "orders_with_diagnostic": len(covered),
        "coverage_ratio": _ratio(len(covered), len(attention)) if attention else 1.0,
    }


def _intended_amount_total(orders: list[Order]) -> float:
    total = 0.0
    for order in orders:
        if order.limit_price is not None:
            total += float(order.limit_price) * int(order.quantity)
    return total


def _infer_cost_precision(metadata: dict[str, Any]) -> str:
    if any(key in metadata for key in ("stamp_tax", "transfer_fee", "broker_reported_stamp_tax", "broker_reported_transfer_fee")):
        return "broker_breakdown"
    if metadata.get("broker_reported_fee_total") is not None or metadata.get("broker_reported_commission") is not None:
        return "broker_aggregate"
    return "estimated_only"


def _warning_flags(
    *,
    fill_items: list[dict[str, Any]],
    order_items: list[dict[str, Any]],
    persisted_fill_count: int,
    materialized_fill_count: int,
) -> list[str]:
    flags: list[str] = []
    if persisted_fill_count != materialized_fill_count:
        flags.append("fill_detail_scope_is_not_full_run")
    if any(item.get("broker_reported_fee_total") is None for item in fill_items):
        flags.append("missing_broker_reported_fee")
    if any(item["status"] == OrderStatus.REJECTED.value and not item.get("has_diagnostic") for item in order_items):
        flags.append("rejected_order_missing_diagnostic")
    if any(item["status"] in {OrderStatus.SUBMITTED.value, OrderStatus.PENDING.value} for item in order_items):
        flags.append("non_terminal_orders_remain")
    return flags


def _report_from_snapshot(snapshot: dict[str, Any]) -> dict[str, Any] | None:
    metadata = snapshot.get("metadata") if isinstance(snapshot.get("metadata"), dict) else {}
    report = metadata.get(EXECUTION_QUALITY_SNAPSHOT_METADATA_KEY)
    return report if _is_execution_quality_report(report) else None


def _is_execution_quality_report(value: Any) -> bool:
    return isinstance(value, dict) and value.get("schema_version") == REPORT_SCHEMA_VERSION


def _report_matches(report: dict[str, Any], *, trade_date: date | None, run_id: str | None) -> bool:
    if run_id is not None and str(report.get("run_id") or "") != run_id:
        return False
    if trade_date is not None and _iso_date(report.get("trade_date")) != trade_date.isoformat():
        return False
    return True


def _report_record(report: dict[str, Any], *, source: dict[str, Any]) -> dict[str, Any]:
    return {
        "record_key": _report_record_key(report),
        "run_id": report.get("run_id") or source.get("run_id"),
        "trade_date": _iso_date(report.get("trade_date") or source.get("trade_date")),
        "generated_at": _iso_datetime(report.get("generated_at")),
        "source": source,
        "summary": report.get("summary") or {},
        "report": report,
    }


def _report_record_key(report: dict[str, Any]) -> str:
    return "|".join(
        [
            str(report.get("portfolio_id") or ""),
            str(report.get("run_id") or ""),
            str(_iso_date(report.get("trade_date")) or ""),
            str(report.get("generated_at") or ""),
        ]
    )


def _dedupe_report_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_key: dict[str, dict[str, Any]] = {}
    source_order = {"daily_snapshot_metadata": 2, "run_event": 1}
    for record in records:
        key = str(record.get("record_key") or "")
        existing = by_key.get(key)
        if existing is None:
            by_key[key] = record
            continue
        existing_rank = source_order.get(str((existing.get("source") or {}).get("source_type")), 0)
        current_rank = source_order.get(str((record.get("source") or {}).get("source_type")), 0)
        if current_rank > existing_rank:
            by_key[key] = record
    return list(by_key.values())


def _report_sort_key(record: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(record.get("trade_date") or ""),
        str(record.get("generated_at") or ""),
        str(record.get("run_id") or ""),
    )


def _iso_date(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value)
    return text[:10] if text else None


def _iso_datetime(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)
