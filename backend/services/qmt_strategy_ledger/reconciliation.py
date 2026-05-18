"""Read-only reconciliation for MiniQMT merged positions and strategy lots."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any

from .models import ReconciliationIssueRecord, ReconciliationRunRecord, new_id
from .repository import InMemoryQmtStrategyLedgerRepository
from .sync_service import SyncSummary


@dataclass(frozen=True)
class ReconciliationReport:
    run: ReconciliationRunRecord
    issues: tuple[ReconciliationIssueRecord, ...]
    strategy_lot_quantities: dict[str, dict[str, int]]
    broker_quantities: dict[str, int]
    unattributed_orders: int
    unattributed_trades: int
    overlap_symbols: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "run": {
                "run_id": self.run.run_id,
                "account_id": self.run.account_id,
                "trade_date": self.run.trade_date.isoformat(),
                "status": self.run.status,
                "summary_json": self.run.summary_json,
            },
            "issues": [
                {
                    "issue_id": issue.issue_id,
                    "issue_type": issue.issue_type,
                    "severity": issue.severity,
                    "message": issue.message,
                    "strategy_id": issue.strategy_id,
                    "symbol": issue.symbol,
                    "qmt_order_id": issue.qmt_order_id,
                    "trade_id": issue.trade_id,
                    "context": issue.context,
                }
                for issue in self.issues
            ],
            "strategy_lot_quantities": self.strategy_lot_quantities,
            "broker_quantities": self.broker_quantities,
            "unattributed_orders": self.unattributed_orders,
            "unattributed_trades": self.unattributed_trades,
            "overlap_symbols": list(self.overlap_symbols),
        }


class QmtStrategyLedgerReconciliationService:
    def __init__(self, *, repository: InMemoryQmtStrategyLedgerRepository) -> None:
        self._repository = repository

    def reconcile_snapshot(
        self,
        *,
        account_id: str,
        trade_date: date,
        broker_positions: list[dict[str, Any]] | tuple[dict[str, Any], ...],
        sync_summary: SyncSummary | None = None,
    ) -> ReconciliationReport:
        run = ReconciliationRunRecord(
            run_id=new_id("qmtrec"),
            account_id=account_id,
            trade_date=trade_date,
            status="STARTED",
            started_at=datetime.now(UTC),
        )
        self._repository.create_reconciliation_run(run)

        accounts = self._repository.list_virtual_accounts(account_id=account_id)
        strategy_lot_quantities: dict[str, dict[str, int]] = {}
        strategy_ids_by_name = {account.strategy_name: account.strategy_id for account in accounts}
        for account in accounts:
            lots = self._repository.list_position_lots(account.strategy_id)
            by_symbol: dict[str, int] = {}
            for lot in lots:
                by_symbol[lot.symbol] = by_symbol.get(lot.symbol, 0) + lot.remaining_quantity
            strategy_lot_quantities[account.strategy_name] = dict(sorted(by_symbol.items()))

        broker_quantities = _broker_quantities(broker_positions)
        strategy_totals = _strategy_totals(strategy_lot_quantities)
        overlap_symbols = tuple(
            sorted(
                symbol
                for symbol in strategy_totals
                if sum(1 for quantities in strategy_lot_quantities.values() if quantities.get(symbol, 0) > 0) > 1
            )
        )

        issues: list[ReconciliationIssueRecord] = []
        for symbol in sorted(set(strategy_totals) | set(broker_quantities)):
            strategy_qty = strategy_totals.get(symbol, 0)
            broker_qty = broker_quantities.get(symbol, 0)
            if strategy_qty != broker_qty:
                issues.append(
                    self._append_issue(
                        run_id=run.run_id,
                        issue_type="POSITION_MISMATCH",
                        severity="ERROR",
                        message="strategy lot quantity does not match MiniQMT merged position quantity",
                        symbol=symbol,
                        context={"strategy_quantity": strategy_qty, "broker_quantity": broker_qty},
                    )
                )

        unattributed_orders = self._repository.list_unattributed_orders(account_id=account_id, trade_date=trade_date)
        unattributed_trades = self._repository.list_unattributed_trades(account_id=account_id, trade_date=trade_date)
        for record in unattributed_orders:
            issues.append(
                self._append_issue(
                    run_id=run.run_id,
                    issue_type="UNATTRIBUTED_ORDER",
                    severity="ERROR",
                    message="MiniQMT order cannot be safely attributed to a virtual strategy",
                    symbol=record.symbol,
                    qmt_order_id=record.qmt_order_id,
                    context={"reason": record.reason, "order_remark": record.order_remark},
                )
            )
        for record in unattributed_trades:
            issues.append(
                self._append_issue(
                    run_id=run.run_id,
                    issue_type="UNATTRIBUTED_TRADE",
                    severity="ERROR",
                    message="MiniQMT trade cannot be safely attributed to a virtual strategy lot",
                    symbol=record.symbol,
                    qmt_order_id=record.qmt_order_id,
                    trade_id=record.trade_id,
                    context={"reason": record.reason, "order_remark": record.order_remark},
                )
            )

        status = "SUCCEEDED" if not issues else "WARNING"
        summary_json = {
            "broker_symbol_count": len(broker_quantities),
            "strategy_symbol_count": len(strategy_totals),
            "issue_count": len(issues),
            "overlap_symbols": list(overlap_symbols),
            "strategy_ids_by_name": strategy_ids_by_name,
        }
        if sync_summary is not None:
            summary_json["sync_summary"] = sync_summary.to_dict()
        object.__setattr__(run, "status", status)
        object.__setattr__(run, "completed_at", datetime.now(UTC))
        object.__setattr__(run, "summary_json", summary_json)

        return ReconciliationReport(
            run=run,
            issues=tuple(issues),
            strategy_lot_quantities=strategy_lot_quantities,
            broker_quantities=broker_quantities,
            unattributed_orders=len(unattributed_orders),
            unattributed_trades=len(unattributed_trades),
            overlap_symbols=overlap_symbols,
        )

    def _append_issue(
        self,
        *,
        run_id: str,
        issue_type: str,
        severity: str,
        message: str,
        strategy_id: str | None = None,
        symbol: str | None = None,
        qmt_order_id: str | None = None,
        trade_id: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> ReconciliationIssueRecord:
        issue = ReconciliationIssueRecord(
            issue_id=new_id("qmtissue"),
            run_id=run_id,
            issue_type=issue_type,
            severity=severity,
            message=message,
            strategy_id=strategy_id,
            symbol=symbol,
            qmt_order_id=qmt_order_id,
            trade_id=trade_id,
            context=context or {},
        )
        self._repository.append_reconciliation_issue(issue)
        return issue


def _broker_quantities(positions: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> dict[str, int]:
    quantities: dict[str, int] = {}
    for position in positions:
        symbol = str(position.get("stock_code") or position.get("symbol") or "").strip()
        if not symbol:
            continue
        quantity = int(position.get("quantity") or position.get("volume") or 0)
        quantities[symbol] = quantities.get(symbol, 0) + quantity
    return dict(sorted(quantities.items()))


def _strategy_totals(strategy_lot_quantities: dict[str, dict[str, int]]) -> dict[str, int]:
    totals: dict[str, int] = {}
    for quantities in strategy_lot_quantities.values():
        for symbol, quantity in quantities.items():
            totals[symbol] = totals.get(symbol, 0) + quantity
    return dict(sorted(totals.items()))
