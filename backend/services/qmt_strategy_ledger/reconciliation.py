"""Read-only reconciliation for MiniQMT merged positions and strategy lots."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime
from typing import Any

from .models import MiniQmtStrategySlot, ReconciliationIssueRecord, ReconciliationRunRecord, new_id
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
    position_authority: str = "strategy_lot_quantities"
    raw_strategy_lot_quantities: dict[str, dict[str, int]] = field(default_factory=dict)
    position_authority_adjustments: tuple[dict[str, Any], ...] = field(default_factory=tuple)

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
            "position_authority": self.position_authority,
            "raw_strategy_lot_quantities": self.raw_strategy_lot_quantities,
            "position_authority_adjustments": list(self.position_authority_adjustments),
        }

    def strategy_scope(self, strategy_id: str | None = None, strategy_name: str | None = None) -> dict[str, Any]:
        """Project the account-level report to one simulation binding's virtual strategy."""

        scope = _resolve_strategy_scope(
            strategy_lot_quantities=self.strategy_lot_quantities,
            strategy_ids_by_name=dict(self.run.summary_json.get("strategy_ids_by_name") or {}),
            strategy_remark_prefixes_by_name=dict(self.run.summary_json.get("strategy_remark_prefixes_by_name") or {}),
            strategy_id=strategy_id,
            strategy_name=strategy_name,
        )
        scoped_strategy_name = scope.get("strategy_name")
        scoped_strategy_id = scope.get("strategy_id")
        scoped_remark_prefix = scope.get("order_remark_prefix")
        matched = bool(scoped_strategy_name and scoped_strategy_name in self.strategy_lot_quantities)
        scoped_quantities = (
            dict(self.strategy_lot_quantities.get(scoped_strategy_name, {})) if matched else {}
        )
        current_issues: list[ReconciliationIssueRecord] = []
        account_level_issues: list[ReconciliationIssueRecord] = []
        for issue in self.issues:
            if _issue_belongs_to_strategy_scope(
                issue=issue,
                scoped_strategy_id=scoped_strategy_id,
                scoped_strategy_name=scoped_strategy_name,
                scoped_remark_prefix=scoped_remark_prefix,
                scoped_quantities=scoped_quantities,
                broker_quantities=self.broker_quantities,
            ):
                current_issues.append(issue)
            else:
                account_level_issues.append(issue)
        issue_symbols = sorted({issue.symbol for issue in current_issues if issue.symbol})
        issue_types = sorted({issue.issue_type for issue in current_issues if issue.issue_type})
        account_level_issue_types = sorted({issue.issue_type for issue in account_level_issues if issue.issue_type})
        return {
            "schema_version": "miniqmt_reconciliation_strategy_scope_v1",
            "strategy_id": scoped_strategy_id or strategy_id,
            "strategy_name": scoped_strategy_name or strategy_name,
            "order_remark_prefix": scoped_remark_prefix,
            "matched": matched,
            "status": "SUCCEEDED" if matched and not current_issues else "WARNING",
            "issue_count": len(current_issues),
            "issue_types": issue_types,
            "issue_symbols": issue_symbols,
            "account_level_issue_count": len(account_level_issues),
            "account_level_issue_types": account_level_issue_types,
            "position_count": len(scoped_quantities),
            "symbols": sorted(scoped_quantities),
            "strategy_lot_quantities": scoped_quantities,
            "position_authority": self.position_authority,
            "raw_strategy_lot_quantities": dict(
                self.raw_strategy_lot_quantities.get(scoped_strategy_name, {})
            )
            if scoped_strategy_name
            else {},
            "position_authority_adjustments": [
                adjustment
                for adjustment in self.position_authority_adjustments
                if _authority_adjustment_belongs_to_strategy(
                    adjustment,
                    scoped_strategy_name=scoped_strategy_name,
                    scoped_strategy_id=scoped_strategy_id,
                )
            ],
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
        broker_authoritative: bool = False,
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
        strategy_remark_prefixes_by_name: dict[str, str] = {}
        for account in accounts:
            slot = MiniQmtStrategySlot.from_virtual_account(account)
            if slot is not None:
                strategy_remark_prefixes_by_name[account.strategy_name] = slot.order_remark_prefix
            lots = self._repository.list_position_lots(account.strategy_id)
            by_symbol: dict[str, int] = {}
            for lot in lots:
                by_symbol[lot.symbol] = by_symbol.get(lot.symbol, 0) + lot.remaining_quantity
            strategy_lot_quantities[account.strategy_name] = dict(sorted(by_symbol.items()))

        broker_quantities = _broker_quantities(broker_positions)
        raw_strategy_lot_quantities = {
            strategy_name: dict(quantities) for strategy_name, quantities in strategy_lot_quantities.items()
        }
        position_authority_adjustments: tuple[dict[str, Any], ...] = ()
        if broker_authoritative:
            projection = broker_authoritative_strategy_projection(
                strategy_lot_quantities=strategy_lot_quantities,
                broker_quantities=broker_quantities,
            )
            strategy_lot_quantities = projection.projected_quantities
            position_authority_adjustments = projection.adjustments
        strategy_totals = _strategy_totals(strategy_lot_quantities)
        overlap_symbols = tuple(
            sorted(
                symbol
                for symbol in strategy_totals
                if sum(1 for quantities in strategy_lot_quantities.values() if quantities.get(symbol, 0) > 0) > 1
            )
        )

        issues: list[ReconciliationIssueRecord] = []
        if broker_authoritative:
            for adjustment in position_authority_adjustments:
                issue_type = str(adjustment.get("issue_type") or "")
                if issue_type not in {"UNBACKED_STRATEGY_POSITION", "UNATTRIBUTED_BROKER_POSITION"}:
                    continue
                issues.append(
                    self._append_issue(
                        run_id=run.run_id,
                        issue_type=issue_type,
                        severity="WARNING",
                        message=str(
                            adjustment.get("message")
                            or "MiniQMT broker-authoritative position projection adjusted local strategy lots"
                        ),
                        symbol=str(adjustment.get("symbol") or "") or None,
                        context=dict(adjustment),
                    )
                )
        else:
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
            "strategy_remark_prefixes_by_name": strategy_remark_prefixes_by_name,
            "position_authority": "broker_positions" if broker_authoritative else "strategy_lot_quantities",
            "broker_authoritative": broker_authoritative,
        }
        if sync_summary is not None:
            summary_json["sync_summary"] = sync_summary.to_dict()
        completed_run = replace(
            run,
            status=status,
            completed_at=datetime.now(UTC),
            summary_json=summary_json,
        )
        complete_run = getattr(self._repository, "complete_reconciliation_run", None)
        run = complete_run(completed_run) if callable(complete_run) else completed_run

        return ReconciliationReport(
            run=run,
            issues=tuple(issues),
            strategy_lot_quantities=strategy_lot_quantities,
            broker_quantities=broker_quantities,
            unattributed_orders=len(unattributed_orders),
            unattributed_trades=len(unattributed_trades),
            overlap_symbols=overlap_symbols,
            position_authority="broker_positions" if broker_authoritative else "strategy_lot_quantities",
            raw_strategy_lot_quantities=raw_strategy_lot_quantities,
            position_authority_adjustments=position_authority_adjustments,
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


@dataclass(frozen=True)
class BrokerAuthoritativeStrategyProjection:
    projected_quantities: dict[str, dict[str, int]]
    raw_quantities: dict[str, dict[str, int]]
    broker_quantities: dict[str, int]
    adjustments: tuple[dict[str, Any], ...] = field(default_factory=tuple)


def broker_authoritative_strategy_projection(
    *,
    strategy_lot_quantities: Mapping[str, Mapping[str, int]],
    broker_quantities: Mapping[str, int],
) -> BrokerAuthoritativeStrategyProjection:
    """Project strategy-slot quantities from MiniQMT broker totals.

    Local strategy lots are attribution evidence only. They cannot create
    strategy-owned holdings beyond the broker account position.
    """

    strategy_names = [str(name) for name in strategy_lot_quantities]
    raw: dict[str, dict[str, int]] = {
        name: {
            str(symbol): max(int(quantity or 0), 0)
            for symbol, quantity in dict(strategy_lot_quantities.get(name, {})).items()
            if max(int(quantity or 0), 0) > 0
        }
        for name in strategy_names
    }
    broker = {
        str(symbol): max(int(quantity or 0), 0)
        for symbol, quantity in dict(broker_quantities).items()
        if max(int(quantity or 0), 0) > 0
    }
    projected: dict[str, dict[str, int]] = {name: {} for name in strategy_names}
    adjustments: list[dict[str, Any]] = []
    symbols = sorted(set(broker) | {symbol for quantities in raw.values() for symbol in quantities})
    for symbol in symbols:
        raw_by_strategy = {name: int(quantities.get(symbol, 0) or 0) for name, quantities in raw.items()}
        raw_by_strategy = {name: quantity for name, quantity in raw_by_strategy.items() if quantity > 0}
        strategy_total = sum(raw_by_strategy.values())
        broker_qty = int(broker.get(symbol, 0) or 0)
        if strategy_total <= broker_qty:
            for name, quantity in raw_by_strategy.items():
                projected[name][symbol] = quantity
            if broker_qty > strategy_total:
                adjustments.append(
                    {
                        "issue_type": "UNATTRIBUTED_BROKER_POSITION",
                        "symbol": symbol,
                        "message": "MiniQMT broker position quantity has no complete strategy-slot attribution",
                        "strategy_quantity": strategy_total,
                        "broker_quantity": broker_qty,
                        "unattributed_quantity": broker_qty - strategy_total,
                        "raw_strategy_quantities": dict(sorted(raw_by_strategy.items())),
                        "projected_strategy_quantities": dict(sorted(raw_by_strategy.items())),
                        "position_authority": "broker_positions",
                    }
                )
            continue

        allocations = _allocate_broker_quantity(raw_by_strategy, broker_qty)
        for name, quantity in allocations.items():
            if quantity > 0:
                projected[name][symbol] = quantity
        adjustments.append(
            {
                "issue_type": "UNBACKED_STRATEGY_POSITION",
                "symbol": symbol,
                "message": "local strategy lots exceed MiniQMT broker position and were capped by broker authority",
                "strategy_quantity": strategy_total,
                "broker_quantity": broker_qty,
                "unbacked_quantity": strategy_total - broker_qty,
                "raw_strategy_quantities": dict(sorted(raw_by_strategy.items())),
                "projected_strategy_quantities": dict(sorted(allocations.items())),
                "affected_strategies": {
                    name: {
                        "raw_quantity": quantity,
                        "projected_quantity": int(allocations.get(name, 0) or 0),
                        "unbacked_quantity": quantity - int(allocations.get(name, 0) or 0),
                    }
                    for name, quantity in sorted(raw_by_strategy.items())
                    if quantity != int(allocations.get(name, 0) or 0)
                },
                "position_authority": "broker_positions",
            }
        )
    return BrokerAuthoritativeStrategyProjection(
        projected_quantities={name: dict(sorted(quantities.items())) for name, quantities in projected.items()},
        raw_quantities={name: dict(sorted(quantities.items())) for name, quantities in raw.items()},
        broker_quantities=dict(sorted(broker.items())),
        adjustments=tuple(adjustments),
    )


def _allocate_broker_quantity(raw_by_strategy: Mapping[str, int], broker_quantity: int) -> dict[str, int]:
    if broker_quantity <= 0:
        return {str(name): 0 for name in raw_by_strategy}
    total = sum(max(int(quantity or 0), 0) for quantity in raw_by_strategy.values())
    if total <= 0:
        return {str(name): 0 for name in raw_by_strategy}
    if broker_quantity >= total:
        return {str(name): max(int(quantity or 0), 0) for name, quantity in raw_by_strategy.items()}
    allocations: dict[str, int] = {}
    lot_size = 100 if all(max(int(quantity or 0), 0) % 100 == 0 for quantity in raw_by_strategy.values()) else 1
    if lot_size > 1 and broker_quantity >= lot_size:
        lot_budget = broker_quantity // lot_size
        lot_quantities = {str(name): max(int(quantity or 0), 0) // lot_size for name, quantity in raw_by_strategy.items()}
        lot_allocations = _allocate_broker_lots(lot_quantities, lot_budget)
        allocations = {name: quantity * lot_size for name, quantity in lot_allocations.items()}
        remainder = broker_quantity - sum(allocations.values())
        if remainder <= 0:
            return allocations
        for name in sorted(allocations):
            raw_quantity = max(int(raw_by_strategy.get(name, 0) or 0), 0)
            if allocations[name] >= raw_quantity:
                continue
            add = min(remainder, raw_quantity - allocations[name])
            allocations[name] += add
            remainder -= add
            if remainder <= 0:
                break
        return allocations
    remainders: list[tuple[int, str]] = []
    allocated = 0
    for name, quantity in raw_by_strategy.items():
        numerator = max(int(quantity or 0), 0) * broker_quantity
        base = numerator // total
        allocations[str(name)] = base
        allocated += base
        remainders.append((numerator % total, str(name)))
    remaining = broker_quantity - allocated
    for _, name in sorted(remainders, key=lambda item: (-item[0], item[1]))[:remaining]:
        allocations[name] = allocations.get(name, 0) + 1
    return allocations


def _allocate_broker_lots(raw_lots_by_strategy: Mapping[str, int], broker_lots: int) -> dict[str, int]:
    total_lots = sum(max(int(quantity or 0), 0) for quantity in raw_lots_by_strategy.values())
    if broker_lots <= 0 or total_lots <= 0:
        return {str(name): 0 for name in raw_lots_by_strategy}
    allocations: dict[str, int] = {}
    remainders: list[tuple[int, str]] = []
    allocated = 0
    for name, quantity in raw_lots_by_strategy.items():
        numerator = max(int(quantity or 0), 0) * broker_lots
        base = min(max(int(quantity or 0), 0), numerator // total_lots)
        allocations[str(name)] = base
        allocated += base
        remainders.append((numerator % total_lots, str(name)))
    remaining = broker_lots - allocated
    for _, name in sorted(remainders, key=lambda item: (-item[0], item[1])):
        if remaining <= 0:
            break
        if allocations.get(name, 0) >= max(int(raw_lots_by_strategy.get(name, 0) or 0), 0):
            continue
        allocations[name] = allocations.get(name, 0) + 1
        remaining -= 1
    return allocations


def _resolve_strategy_scope(
    *,
    strategy_lot_quantities: dict[str, dict[str, int]],
    strategy_ids_by_name: dict[str, str],
    strategy_remark_prefixes_by_name: dict[str, str],
    strategy_id: str | None,
    strategy_name: str | None,
) -> dict[str, str | None]:
    wanted_id = str(strategy_id or "").strip() or None
    wanted_name = str(strategy_name or "").strip() or None
    if wanted_name and wanted_name in strategy_lot_quantities:
        return {
            "strategy_id": strategy_ids_by_name.get(wanted_name) or wanted_id,
            "strategy_name": wanted_name,
            "order_remark_prefix": strategy_remark_prefixes_by_name.get(wanted_name),
        }
    if wanted_id:
        for name, mapped_id in strategy_ids_by_name.items():
            if mapped_id == wanted_id:
                return {
                    "strategy_id": mapped_id,
                    "strategy_name": name,
                    "order_remark_prefix": strategy_remark_prefixes_by_name.get(name),
                }
    return {"strategy_id": wanted_id, "strategy_name": wanted_name, "order_remark_prefix": None}


def _issue_belongs_to_strategy_scope(
    *,
    issue: ReconciliationIssueRecord,
    scoped_strategy_id: str | None,
    scoped_strategy_name: str | None,
    scoped_remark_prefix: str | None,
    scoped_quantities: dict[str, int],
    broker_quantities: dict[str, int],
) -> bool:
    if scoped_strategy_id and issue.strategy_id == scoped_strategy_id:
        return True
    context = issue.context if isinstance(issue.context, dict) else {}
    if scoped_strategy_id and context.get("strategy_id") == scoped_strategy_id:
        return True
    if scoped_strategy_name and context.get("strategy_name") == scoped_strategy_name:
        return True
    if scoped_remark_prefix:
        order_remark = str(context.get("order_remark") or "").strip()
        if order_remark.startswith(f"{scoped_remark_prefix}-"):
            return True
    if issue.issue_type != "POSITION_MISMATCH" or not issue.symbol:
        return _authority_adjustment_belongs_to_strategy(
            context,
            scoped_strategy_name=scoped_strategy_name,
            scoped_strategy_id=scoped_strategy_id,
        )
    scoped_quantity = int(scoped_quantities.get(issue.symbol, 0) or 0)
    broker_quantity = int(broker_quantities.get(issue.symbol, 0) or 0)
    return scoped_quantity > broker_quantity


def _authority_adjustment_belongs_to_strategy(
    adjustment: Mapping[str, Any],
    *,
    scoped_strategy_name: str | None,
    scoped_strategy_id: str | None,
) -> bool:
    issue_type = str(adjustment.get("issue_type") or "").strip()
    if issue_type == "UNATTRIBUTED_BROKER_POSITION":
        return False
    if issue_type != "UNBACKED_STRATEGY_POSITION":
        return False
    affected = adjustment.get("affected_strategies")
    if isinstance(affected, Mapping):
        if scoped_strategy_name and scoped_strategy_name in affected:
            return True
        if scoped_strategy_id and scoped_strategy_id in affected:
            return True
    raw = adjustment.get("raw_strategy_quantities")
    projected = adjustment.get("projected_strategy_quantities")
    for values in (raw, projected):
        if not isinstance(values, Mapping):
            continue
        if scoped_strategy_name and scoped_strategy_name in values:
            return True
        if scoped_strategy_id and scoped_strategy_id in values:
            return True
    return False
