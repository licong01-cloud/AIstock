"""Persistence boundary for MiniQMT multi-strategy virtual ledgers.

The repository layer stores AIstock's strategy-level ledger state only. It does
not connect to MiniQMT, submit orders, cancel orders, or run schema DDL.
"""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import Enum
from hashlib import sha1
from typing import Any, Callable, Iterator

import psycopg2.extras

from backend.db.pg_pool import get_conn
from backend.services.trading_core.errors import DataUnavailableError, InvalidStateTransitionError

from .models import (
    BUY_ORDER_TYPE,
    MINIQMT_ACCOUNT_GROUP_ALLOCATION_MODE,
    MINIQMT_STRATEGY_SLOT_METADATA_KEY,
    SELL_ORDER_TYPE,
    BindingStatus,
    CashEntryType,
    CashLedgerEntry,
    DailySnapshotRecord,
    IntentPreflightStatus,
    IntentSubmitStatus,
    OrderBatchRecord,
    OrderBatchStatus,
    OrderIntentRecord,
    OrderLedgerRecord,
    OrderStatusEventRecord,
    PositionLotRecord,
    PositionLotStatus,
    ReconciliationIssueRecord,
    ReconciliationRunRecord,
    MiniQmtAccountGroup,
    MiniQmtStrategySlot,
    StrategyBindingSelectionEvidence,
    StrategyPackageBinding,
    TradeLedgerRecord,
    UnattributedOrderRecord,
    UnattributedTradeRecord,
    VirtualAccount,
    VirtualAccountStatus,
)

ConnFactory = Callable[[], Iterator[Any]]


class QmtStrategyLedgerRepository:
    """PostgreSQL repository for the qmt_strategy schema."""

    def __init__(self, conn_factory: ConnFactory | None = None) -> None:
        self._conn_factory = conn_factory or get_conn

    def create_virtual_account(self, account: VirtualAccount) -> VirtualAccount:
        _validate_virtual_account(account)
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO qmt_strategy.virtual_account (
                        strategy_id, strategy_name, display_name, account_id, mode,
                        initial_cash, cash, frozen_cash, market_value, realized_pnl,
                        unrealized_pnl, status, risk_config, metadata, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        account.strategy_id,
                        account.strategy_name,
                        account.display_name,
                        account.account_id,
                        account.mode,
                        account.initial_cash,
                        account.cash,
                        account.frozen_cash,
                        account.market_value,
                        account.realized_pnl,
                        account.unrealized_pnl,
                        _enum_value(account.status),
                        _json(account.risk_config),
                        _json(account.metadata),
                        account.created_at,
                        account.updated_at,
                    ),
                )
        return account

    def get_virtual_account(self, strategy_id: str) -> VirtualAccount:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM qmt_strategy.virtual_account WHERE strategy_id = %s", (strategy_id,))
                row = cur.fetchone()
        if not row:
            raise DataUnavailableError("qmt strategy virtual account does not exist", context={"strategy_id": strategy_id})
        return _row_to_virtual_account(row)

    def list_virtual_accounts(self, account_id: str | None = None) -> list[VirtualAccount]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                if account_id is None:
                    cur.execute("SELECT * FROM qmt_strategy.virtual_account ORDER BY created_at, strategy_id")
                else:
                    cur.execute(
                        """
                        SELECT *
                        FROM qmt_strategy.virtual_account
                        WHERE account_id = %s
                        ORDER BY created_at, strategy_id
                        """,
                        (account_id,),
                    )
                rows = cur.fetchall()
        return [_row_to_virtual_account(row) for row in rows]

    def update_virtual_account(self, account: VirtualAccount) -> VirtualAccount:
        _validate_virtual_account(account)
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                self._update_virtual_account_with_cursor(cur, account)
        return account

    def create_account_group_slots(self, group: MiniQmtAccountGroup) -> MiniQmtAccountGroup:
        existing_accounts = self.list_virtual_accounts(group.broker_account_id)
        _validate_account_group_slots(group, existing_accounts=existing_accounts)
        for slot in group.slots:
            self.create_virtual_account(slot.to_virtual_account(group))
        return group

    def list_account_group_slots(
        self,
        account_group_id: str,
        *,
        broker_account_id: str | None = None,
    ) -> list[MiniQmtStrategySlot]:
        accounts = self.list_virtual_accounts(broker_account_id)
        return _slots_from_accounts(accounts, account_group_id)

    def get_account_group(
        self,
        account_group_id: str,
        *,
        broker_account_id: str | None = None,
    ) -> MiniQmtAccountGroup:
        slots = self.list_account_group_slots(account_group_id, broker_account_id=broker_account_id)
        if not slots:
            raise DataUnavailableError(
                "MiniQMT account group does not exist",
                context={"account_group_id": account_group_id, "broker_account_id": broker_account_id},
            )
        return _group_from_slots(slots)

    def set_account_group_slot_status(
        self,
        *,
        account_group_id: str,
        strategy_slot_id: str,
        status: VirtualAccountStatus,
    ) -> MiniQmtStrategySlot:
        slots = self.list_account_group_slots(account_group_id)
        for slot in slots:
            if slot.strategy_slot_id != strategy_slot_id:
                continue
            account = self.get_virtual_account(slot.strategy_id)
            updated_metadata = dict(account.metadata)
            slot_meta = dict(updated_metadata.get(MINIQMT_STRATEGY_SLOT_METADATA_KEY) or {})
            slot_meta["status"] = status.value
            updated_metadata[MINIQMT_STRATEGY_SLOT_METADATA_KEY] = slot_meta
            self.update_virtual_account(replace(account, status=status, metadata=updated_metadata, updated_at=datetime.now(UTC)))
            updated = self.get_virtual_account(slot.strategy_id)
            mapped = MiniQmtStrategySlot.from_virtual_account(updated)
            if mapped is None:
                raise ValueError("updated slot metadata became invalid")
            return mapped
        raise DataUnavailableError(
            "MiniQMT strategy slot does not exist",
            context={"account_group_id": account_group_id, "strategy_slot_id": strategy_slot_id},
        )

    def _update_virtual_account_with_cursor(self, cur: Any, account: VirtualAccount) -> None:
        cur.execute(
            """
            UPDATE qmt_strategy.virtual_account
            SET display_name = %s,
                mode = %s,
                initial_cash = %s,
                cash = %s,
                frozen_cash = %s,
                market_value = %s,
                realized_pnl = %s,
                unrealized_pnl = %s,
                status = %s,
                risk_config = %s,
                metadata = %s,
                updated_at = %s
            WHERE strategy_id = %s
            """,
            (
                account.display_name,
                account.mode,
                account.initial_cash,
                account.cash,
                account.frozen_cash,
                account.market_value,
                account.realized_pnl,
                account.unrealized_pnl,
                _enum_value(account.status),
                _json(account.risk_config),
                _json(account.metadata),
                account.updated_at,
                account.strategy_id,
            ),
        )
        if cur.rowcount == 0:
            raise DataUnavailableError(
                "qmt strategy virtual account does not exist",
                context={"strategy_id": account.strategy_id},
            )

    def create_package_binding(self, binding: StrategyPackageBinding) -> StrategyPackageBinding:
        _validate_package_binding(binding)
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                self._insert_package_binding_with_cursor(cur, binding)
        return binding

    def replace_active_package_binding(
        self,
        binding: StrategyPackageBinding,
        *,
        replaced_binding_id: str,
        reason: str,
    ) -> StrategyPackageBinding:
        _validate_package_binding(binding)
        previous_autocommit = None
        now = datetime.now(UTC)
        with self._conn_factory() as conn:
            previous_autocommit = getattr(conn, "autocommit", None)
            if previous_autocommit is not None:
                conn.autocommit = False
            try:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT *
                        FROM qmt_strategy.strategy_package_binding
                        WHERE binding_id = %s AND strategy_id = %s AND binding_status = 'ACTIVE'
                        FOR UPDATE
                        """,
                        (replaced_binding_id, binding.strategy_id),
                    )
                    existing = cur.fetchone()
                    if existing is None:
                        raise DataUnavailableError(
                            "active package binding to replace does not exist",
                            context={"binding_id": replaced_binding_id, "strategy_id": binding.strategy_id},
                        )
                    cur.execute(
                        """
                        UPDATE qmt_strategy.strategy_package_binding
                        SET binding_status = %s,
                            runtime_config = jsonb_set(
                                COALESCE(runtime_config, '{}'::jsonb),
                                '{binding_lifecycle}',
                                (COALESCE(runtime_config->'binding_lifecycle', '{}'::jsonb)
                                    || jsonb_build_object(
                                        'replaced_by_binding_id', %s,
                                        'replace_reason', %s,
                                        'replaced_at', %s
                                    )),
                                true
                            ),
                            updated_at = %s
                        WHERE binding_id = %s
                        """,
                        (
                            BindingStatus.RETIRED.value,
                            binding.binding_id,
                            reason,
                            now.isoformat(),
                            now,
                            replaced_binding_id,
                        ),
                    )
                    if cur.rowcount != 1:
                        raise InvalidStateTransitionError(
                            "failed to retire active package binding",
                            context={"binding_id": replaced_binding_id, "strategy_id": binding.strategy_id},
                        )
                    self._insert_package_binding_with_cursor(cur, binding)
                if hasattr(conn, "commit"):
                    conn.commit()
            except Exception:
                if hasattr(conn, "rollback"):
                    conn.rollback()
                raise
            finally:
                if previous_autocommit is not None:
                    conn.autocommit = previous_autocommit
        return binding

    def _insert_package_binding_with_cursor(self, cur: Any, binding: StrategyPackageBinding) -> None:
        cur.execute(
            """
            INSERT INTO qmt_strategy.strategy_package_binding (
                binding_id, strategy_id, package_id, manifest_sha256,
                selection_run_id, trade_date, target_weight, top_k,
                binding_status, runtime_config, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                binding.binding_id,
                binding.strategy_id,
                binding.package_id,
                binding.manifest_sha256,
                binding.selection_run_id,
                binding.trade_date,
                binding.target_weight,
                binding.top_k,
                _enum_value(binding.binding_status),
                _json(binding.runtime_config),
                binding.created_at,
                binding.updated_at,
            ),
        )

    def get_active_package_binding(self, strategy_id: str) -> StrategyPackageBinding | None:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM qmt_strategy.strategy_package_binding
                    WHERE strategy_id = %s AND binding_status = 'ACTIVE'
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (strategy_id,),
                )
                row = cur.fetchone()
        return _row_to_package_binding(row) if row else None

    def list_package_bindings(self, strategy_id: str) -> list[StrategyPackageBinding]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM qmt_strategy.strategy_package_binding
                    WHERE strategy_id = %s
                    ORDER BY created_at, binding_id
                    """,
                    (strategy_id,),
                )
                rows = cur.fetchall()
        return [_row_to_package_binding(row) for row in rows]

    def get_package_binding(self, binding_id: str) -> StrategyPackageBinding:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM qmt_strategy.strategy_package_binding WHERE binding_id = %s",
                    (binding_id,),
                )
                row = cur.fetchone()
        if not row:
            raise DataUnavailableError("qmt strategy package binding does not exist", context={"binding_id": binding_id})
        return _row_to_package_binding(row)

    def record_binding_selection_evidence(
        self,
        evidence: StrategyBindingSelectionEvidence,
    ) -> StrategyBindingSelectionEvidence:
        _validate_binding_selection_evidence(evidence)
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM qmt_strategy.strategy_binding_selection_evidence
                    WHERE binding_id = %s AND trade_date = %s
                    FOR UPDATE
                    """,
                    (evidence.binding_id, evidence.trade_date),
                )
                existing = cur.fetchone()
                if existing is not None:
                    stored = _row_to_binding_selection_evidence(dict(existing))
                    if _same_daily_selection_evidence(stored, evidence):
                        return stored
                    raise InvalidStateTransitionError(
                        "daily selection evidence already exists for binding and trade_date",
                        context={
                            "binding_id": evidence.binding_id,
                            "trade_date": evidence.trade_date.isoformat(),
                            "existing_selection_run_id": stored.selection_run_id,
                            "requested_selection_run_id": evidence.selection_run_id,
                            "existing_runtime_config_hash": stored.runtime_config_hash,
                            "requested_runtime_config_hash": evidence.runtime_config_hash,
                        },
                    )
                cur.execute(
                    """
                    INSERT INTO qmt_strategy.strategy_binding_selection_evidence (
                        evidence_id, binding_id, strategy_id, package_id,
                        selection_run_id, trade_date, data_source, manifest_sha256,
                        runtime_config_hash, artifact_id, artifact_sha256,
                        source_type, authority_scope, score_count, metadata, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        evidence.evidence_id,
                        evidence.binding_id,
                        evidence.strategy_id,
                        evidence.package_id,
                        evidence.selection_run_id,
                        evidence.trade_date,
                        evidence.data_source,
                        evidence.manifest_sha256,
                        evidence.runtime_config_hash,
                        evidence.artifact_id,
                        evidence.artifact_sha256,
                        evidence.source_type,
                        evidence.authority_scope,
                        evidence.score_count,
                        _json(evidence.metadata),
                        evidence.created_at,
                    ),
                )
        return evidence

    def get_binding_selection_evidence(
        self,
        binding_id: str,
        trade_date: date,
    ) -> StrategyBindingSelectionEvidence:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM qmt_strategy.strategy_binding_selection_evidence
                    WHERE binding_id = %s AND trade_date = %s
                    """,
                    (binding_id, trade_date),
                )
                row = cur.fetchone()
        if not row:
            raise DataUnavailableError(
                "current-day MiniQMT selection evidence is missing; generate or resolve today's SelectionRun before order build",
                context={"binding_id": binding_id, "trade_date": trade_date.isoformat()},
            )
        return _row_to_binding_selection_evidence(dict(row))

    def list_binding_selection_evidence(self, binding_id: str) -> list[StrategyBindingSelectionEvidence]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM qmt_strategy.strategy_binding_selection_evidence
                    WHERE binding_id = %s
                    ORDER BY trade_date, created_at, evidence_id
                    """,
                    (binding_id,),
                )
                rows = cur.fetchall()
        return [_row_to_binding_selection_evidence(dict(row)) for row in rows]

    def upsert_order_batch(self, batch: OrderBatchRecord) -> OrderBatchRecord:
        _validate_order_batch(batch)
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO qmt_strategy.order_batch (
                        batch_id, strategy_id, account_id, mode, batch_status,
                        requested_by, request_json, result_json, metadata,
                        created_at, submitted_at, completed_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (batch_id) DO UPDATE SET
                        strategy_id = EXCLUDED.strategy_id,
                        account_id = EXCLUDED.account_id,
                        mode = EXCLUDED.mode,
                        batch_status = EXCLUDED.batch_status,
                        requested_by = EXCLUDED.requested_by,
                        request_json = EXCLUDED.request_json,
                        result_json = EXCLUDED.result_json,
                        metadata = EXCLUDED.metadata,
                        submitted_at = EXCLUDED.submitted_at,
                        completed_at = EXCLUDED.completed_at
                    """,
                    (
                        batch.batch_id,
                        batch.strategy_id,
                        batch.account_id,
                        batch.mode,
                        _enum_value(batch.batch_status),
                        batch.requested_by,
                        _json(batch.request_json),
                        _json(batch.result_json),
                        _json(batch.metadata),
                        batch.created_at,
                        batch.submitted_at,
                        batch.completed_at,
                    ),
                )
        return batch

    def get_order_batch(self, batch_id: str) -> OrderBatchRecord | None:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM qmt_strategy.order_batch WHERE batch_id = %s", (batch_id,))
                row = cur.fetchone()
        return _row_to_order_batch(row) if row else None

    def create_order_intent(self, intent: OrderIntentRecord) -> OrderIntentRecord:
        _validate_order_intent(intent)
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO qmt_strategy.order_intent (
                        intent_id, batch_id, strategy_id, strategy_name, symbol, side,
                        order_type, quantity, price_type, order_remark, account_id,
                        trade_date, package_id, selection_run_id, limit_price,
                        target_weight, estimated_notional, estimated_fee,
                        preflight_status, submit_status, metadata, created_at,
                        submitted_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """,
                    (
                        intent.intent_id,
                        intent.batch_id,
                        intent.strategy_id,
                        intent.strategy_name,
                        intent.symbol,
                        intent.side,
                        intent.order_type,
                        intent.quantity,
                        intent.price_type,
                        intent.order_remark,
                        intent.account_id,
                        intent.trade_date,
                        intent.package_id,
                        intent.selection_run_id,
                        intent.limit_price,
                        intent.target_weight,
                        intent.estimated_notional,
                        intent.estimated_fee,
                        _enum_value(intent.preflight_status),
                        _enum_value(intent.submit_status),
                        _json(intent.metadata),
                        intent.created_at,
                        intent.submitted_at,
                        intent.updated_at,
                    ),
                )
        return intent

    def get_order_intent(self, intent_id: str) -> OrderIntentRecord:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM qmt_strategy.order_intent WHERE intent_id = %s", (intent_id,))
                row = cur.fetchone()
        if not row:
            raise DataUnavailableError("qmt strategy order intent does not exist", context={"intent_id": intent_id})
        return _row_to_order_intent(row)

    def list_order_intents_by_batch(self, batch_id: str) -> list[OrderIntentRecord]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM qmt_strategy.order_intent
                    WHERE batch_id = %s
                    ORDER BY created_at, intent_id
                    """,
                    (batch_id,),
                )
                rows = cur.fetchall()
        return [_row_to_order_intent(row) for row in rows]

    def get_order_intent_by_remark(self, account_id: str, order_remark: str) -> OrderIntentRecord | None:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM qmt_strategy.order_intent
                    WHERE account_id = %s AND order_remark = %s
                    """,
                    (account_id, order_remark),
                )
                row = cur.fetchone()
        return _row_to_order_intent(row) if row else None

    def list_open_sell_intents(
        self,
        strategy_id: str,
        symbol: str | None = None,
        trade_date: date | None = None,
    ) -> list[OrderIntentRecord]:
        filters = [
            "strategy_id = %s",
            "side = 'SELL'",
            "submit_status IN ('CREATED', 'SUBMITTED', 'ACCEPTED')",
        ]
        params: list[Any] = [strategy_id]
        if symbol is not None:
            filters.append("symbol = %s")
            params.append(symbol)
        if trade_date is not None:
            filters.append("trade_date = %s")
            params.append(trade_date)
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    f"""
                    SELECT *
                    FROM qmt_strategy.order_intent
                    WHERE {' AND '.join(filters)}
                    ORDER BY created_at, intent_id
                    """,
                    tuple(params),
                )
                rows = cur.fetchall()
        return [_row_to_order_intent(row) for row in rows]

    def set_order_intent_submit_status(
        self,
        intent_id: str,
        status: IntentSubmitStatus,
        *,
        submitted_at: Any | None = None,
        updated_at: Any | None = None,
    ) -> OrderIntentRecord:
        updated_at = updated_at or datetime.now(UTC)
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE qmt_strategy.order_intent
                    SET submit_status = %s,
                        submitted_at = COALESCE(%s, submitted_at),
                        updated_at = %s
                    WHERE intent_id = %s
                    """,
                    (_enum_value(status), submitted_at, updated_at, intent_id),
                )
                if cur.rowcount == 0:
                    raise DataUnavailableError("qmt strategy order intent does not exist", context={"intent_id": intent_id})
        return self.get_order_intent(intent_id)

    def upsert_order_ledger(self, order: OrderLedgerRecord) -> OrderLedgerRecord:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO qmt_strategy.order_ledger (
                        ledger_id, intent_id, strategy_id, strategy_name, qmt_order_id,
                        qmt_order_sysid, symbol, order_type, order_volume, traded_volume,
                        order_status, account_id, trade_date, price_type, price,
                        traded_price, status_msg, order_remark, raw_json, last_synced_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (account_id, qmt_order_id) DO UPDATE SET
                        intent_id = EXCLUDED.intent_id,
                        strategy_id = EXCLUDED.strategy_id,
                        strategy_name = EXCLUDED.strategy_name,
                        qmt_order_sysid = EXCLUDED.qmt_order_sysid,
                        symbol = EXCLUDED.symbol,
                        order_type = EXCLUDED.order_type,
                        order_volume = EXCLUDED.order_volume,
                        traded_volume = EXCLUDED.traded_volume,
                        order_status = EXCLUDED.order_status,
                        trade_date = EXCLUDED.trade_date,
                        price_type = EXCLUDED.price_type,
                        price = EXCLUDED.price,
                        traded_price = EXCLUDED.traded_price,
                        status_msg = EXCLUDED.status_msg,
                        order_remark = EXCLUDED.order_remark,
                        raw_json = EXCLUDED.raw_json,
                        last_synced_at = EXCLUDED.last_synced_at
                    """,
                    (
                        _stable_id("ord", order.account_id, order.qmt_order_id),
                        order.intent_id,
                        order.strategy_id,
                        order.strategy_name,
                        order.qmt_order_id,
                        order.qmt_order_sysid,
                        order.symbol,
                        order.order_type,
                        order.order_volume,
                        order.traded_volume,
                        order.order_status,
                        order.account_id,
                        order.trade_date,
                        order.price_type,
                        order.price,
                        order.traded_price,
                        order.status_msg,
                        order.order_remark,
                        _json(order.raw_json),
                        order.last_synced_at,
                    ),
                )
        return order

    def append_order_status_event(self, event: OrderStatusEventRecord) -> OrderStatusEventRecord:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO qmt_strategy.order_status_event (
                        event_id, intent_id, qmt_order_id, qmt_order_sysid, event_type,
                        event_time, account_id, qmt_order_status, status_msg, raw_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (event_id) DO NOTHING
                    """,
                    (
                        event.event_id,
                        event.intent_id,
                        event.qmt_order_id,
                        event.qmt_order_sysid,
                        event.event_type,
                        event.event_time,
                        event.account_id,
                        event.qmt_order_status,
                        event.status_msg,
                        _json(event.raw_json),
                    ),
                )
        return event

    def upsert_trade_ledger(self, trade: TradeLedgerRecord) -> tuple[TradeLedgerRecord, bool]:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO qmt_strategy.trade_ledger (
                        trade_id, intent_id, strategy_id, qmt_order_id, qmt_order_sysid,
                        symbol, side, price, quantity, amount, commission, trade_date,
                        account_id, trade_time, order_remark, raw_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (account_id, trade_date, trade_id) DO NOTHING
                    RETURNING trade_id
                    """,
                    (
                        trade.trade_id,
                        trade.intent_id,
                        trade.strategy_id,
                        trade.qmt_order_id,
                        trade.qmt_order_sysid,
                        trade.symbol,
                        trade.side,
                        trade.price,
                        trade.quantity,
                        trade.amount,
                        trade.commission,
                        trade.trade_date,
                        trade.account_id,
                        trade.trade_time,
                        trade.order_remark,
                        _json(trade.raw_json),
                    ),
                )
                inserted = cur.fetchone() is not None
        return trade, inserted

    def create_position_lot(self, lot: PositionLotRecord) -> PositionLotRecord:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO qmt_strategy.position_lot (
                        lot_id, strategy_id, account_id, symbol, open_trade_id, open_date,
                        open_time, quantity, available_quantity, remaining_quantity,
                        avg_cost, cost_amount, realized_pnl, status, metadata
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        lot.lot_id,
                        lot.strategy_id,
                        lot.account_id,
                        lot.symbol,
                        lot.open_trade_id,
                        lot.open_date,
                        lot.open_time,
                        lot.quantity,
                        lot.available_quantity,
                        lot.remaining_quantity,
                        lot.avg_cost,
                        lot.cost_amount,
                        lot.realized_pnl,
                        _enum_value(lot.status),
                        _json(lot.metadata),
                    ),
                )
        return lot

    def list_position_lots(self, strategy_id: str, symbol: str | None = None) -> list[PositionLotRecord]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                if symbol is None:
                    cur.execute(
                        """
                        SELECT *
                        FROM qmt_strategy.position_lot
                        WHERE strategy_id = %s
                        ORDER BY open_date, lot_id
                        """,
                        (strategy_id,),
                    )
                else:
                    cur.execute(
                        """
                        SELECT *
                        FROM qmt_strategy.position_lot
                        WHERE strategy_id = %s AND symbol = %s
                        ORDER BY open_date, lot_id
                        """,
                        (strategy_id, symbol),
                    )
                rows = cur.fetchall()
        return [_row_to_position_lot(row) for row in rows]

    def update_position_lot(self, lot: PositionLotRecord) -> PositionLotRecord:
        if lot.available_quantity > lot.remaining_quantity:
            raise ValueError("position lot available_quantity cannot exceed remaining_quantity")
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                self._update_position_lot_with_cursor(cur, lot)
        return lot

    def _update_position_lot_with_cursor(self, cur: Any, lot: PositionLotRecord) -> None:
        cur.execute(
            """
            UPDATE qmt_strategy.position_lot
            SET available_quantity = %s,
                remaining_quantity = %s,
                avg_cost = %s,
                cost_amount = %s,
                realized_pnl = %s,
                status = %s,
                metadata = %s,
                updated_at = %s
            WHERE lot_id = %s
            """,
            (
                lot.available_quantity,
                lot.remaining_quantity,
                lot.avg_cost,
                lot.cost_amount,
                lot.realized_pnl,
                _enum_value(lot.status),
                _json(lot.metadata),
                datetime.now(UTC),
                lot.lot_id,
            ),
        )
        if cur.rowcount == 0:
            raise DataUnavailableError("qmt strategy position lot does not exist", context={"lot_id": lot.lot_id})

    def append_cash_entry(self, entry: CashLedgerEntry) -> CashLedgerEntry:
        self._insert_cash_entry(entry, ignore_conflict=False)
        return entry

    def append_cash_entry_once(self, entry: CashLedgerEntry) -> tuple[CashLedgerEntry, bool]:
        inserted = self._insert_cash_entry(entry, ignore_conflict=True)
        return entry, inserted

    def apply_cash_entry_once(self, entry: CashLedgerEntry, account: VirtualAccount) -> tuple[CashLedgerEntry, bool]:
        _validate_virtual_account(account)
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                inserted = self._insert_cash_entry_with_cursor(cur, entry, ignore_conflict=True)
                if inserted:
                    self._update_virtual_account_with_cursor(cur, account)
        return entry, inserted

    def apply_cash_entry_and_lots_once(
        self,
        entry: CashLedgerEntry,
        account: VirtualAccount,
        lots: list[PositionLotRecord],
    ) -> tuple[CashLedgerEntry, bool]:
        """Apply a cash event, account update, and lot updates as one DB transaction."""

        _validate_virtual_account(account)
        with self._conn_factory() as conn:
            previous_autocommit = getattr(conn, "autocommit", None)
            if previous_autocommit is not None:
                conn.autocommit = False
            try:
                with conn.cursor() as cur:
                    inserted = self._insert_cash_entry_with_cursor(cur, entry, ignore_conflict=True)
                    if inserted:
                        self._update_virtual_account_with_cursor(cur, account)
                        for lot in lots:
                            if lot.available_quantity > lot.remaining_quantity:
                                raise ValueError("position lot available_quantity cannot exceed remaining_quantity")
                            self._update_position_lot_with_cursor(cur, lot)
                if hasattr(conn, "commit"):
                    conn.commit()
            except Exception:
                if hasattr(conn, "rollback"):
                    conn.rollback()
                raise
            finally:
                if previous_autocommit is not None:
                    conn.autocommit = previous_autocommit
        return entry, inserted

    def _insert_cash_entry(self, entry: CashLedgerEntry, *, ignore_conflict: bool) -> bool:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                return self._insert_cash_entry_with_cursor(cur, entry, ignore_conflict=ignore_conflict)

    def _insert_cash_entry_with_cursor(self, cur: Any, entry: CashLedgerEntry, *, ignore_conflict: bool) -> bool:
        conflict_clause = "ON CONFLICT (cash_id) DO NOTHING RETURNING cash_id" if ignore_conflict else "RETURNING cash_id"
        cur.execute(
            f"""
            INSERT INTO qmt_strategy.cash_ledger (
                cash_id, strategy_id, account_id, trade_date, entry_type,
                cash_delta, cash_after, frozen_delta, frozen_after, intent_id,
                trade_id, symbol, reason, metadata, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            {conflict_clause}
            """,
            (
                entry.cash_id,
                entry.strategy_id,
                entry.account_id,
                entry.trade_date,
                _enum_value(entry.entry_type),
                entry.cash_delta,
                entry.cash_after,
                entry.frozen_delta,
                entry.frozen_after,
                entry.intent_id,
                entry.trade_id,
                entry.symbol,
                entry.reason,
                _json(entry.metadata),
                entry.created_at,
            ),
        )
        return cur.fetchone() is not None

    def list_cash_entries(self, strategy_id: str) -> list[CashLedgerEntry]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM qmt_strategy.cash_ledger
                    WHERE strategy_id = %s
                    ORDER BY cash_sequence
                    """,
                    (strategy_id,),
                )
                rows = cur.fetchall()
        return [_row_to_cash_entry(row) for row in rows]

    def get_cash_entry(self, cash_id: str) -> CashLedgerEntry | None:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM qmt_strategy.cash_ledger WHERE cash_id = %s", (cash_id,))
                row = cur.fetchone()
        return _row_to_cash_entry(row) if row else None

    def create_daily_snapshot(self, snapshot: DailySnapshotRecord) -> DailySnapshotRecord:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO qmt_strategy.daily_snapshot (
                        snapshot_id, strategy_id, account_id, trade_date, cash,
                        frozen_cash, market_value, realized_pnl, unrealized_pnl,
                        total_equity, positions_json, metadata, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        snapshot.snapshot_id,
                        snapshot.strategy_id,
                        snapshot.account_id,
                        snapshot.trade_date,
                        snapshot.cash,
                        snapshot.frozen_cash,
                        snapshot.market_value,
                        snapshot.realized_pnl,
                        snapshot.unrealized_pnl,
                        snapshot.total_equity,
                        _json(snapshot.positions_json),
                        _json(snapshot.metadata),
                        snapshot.created_at,
                    ),
                )
        return snapshot

    def create_reconciliation_run(self, run: ReconciliationRunRecord) -> ReconciliationRunRecord:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO qmt_strategy.reconciliation_run (
                        run_id, account_id, trade_date, status, started_at, completed_at, summary_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        run.run_id,
                        run.account_id,
                        run.trade_date,
                        run.status,
                        run.started_at,
                        run.completed_at,
                        _json(run.summary_json),
                    ),
                )
        return run

    def complete_reconciliation_run(self, run: ReconciliationRunRecord) -> ReconciliationRunRecord:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE qmt_strategy.reconciliation_run
                    SET status = %s,
                        completed_at = %s,
                        summary_json = %s
                    WHERE run_id = %s
                    """,
                    (
                        run.status,
                        run.completed_at,
                        _json(run.summary_json),
                        run.run_id,
                    ),
                )
        return run

    def append_reconciliation_issue(self, issue: ReconciliationIssueRecord) -> ReconciliationIssueRecord:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO qmt_strategy.reconciliation_issue (
                        issue_id, run_id, strategy_id, symbol, qmt_order_id, trade_id,
                        issue_type, severity, message, context, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        issue.issue_id,
                        issue.run_id,
                        issue.strategy_id,
                        issue.symbol,
                        issue.qmt_order_id,
                        issue.trade_id,
                        issue.issue_type,
                        issue.severity,
                        issue.message,
                        _json(issue.context),
                        issue.created_at,
                    ),
                )
        return issue

    def list_reconciliation_issues(self, run_id: str) -> list[ReconciliationIssueRecord]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM qmt_strategy.reconciliation_issue
                    WHERE run_id = %s
                    ORDER BY created_at, issue_id
                    """,
                    (run_id,),
                )
                rows = cur.fetchall()
        return [_row_to_reconciliation_issue(row) for row in rows]

    def upsert_unattributed_order(self, record: UnattributedOrderRecord) -> UnattributedOrderRecord:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO qmt_strategy.unattributed_order (
                        unattributed_id, account_id, trade_date, qmt_order_id, symbol,
                        reason, order_remark, raw_json, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (account_id, trade_date, qmt_order_id) DO UPDATE SET
                        symbol = EXCLUDED.symbol,
                        reason = EXCLUDED.reason,
                        order_remark = EXCLUDED.order_remark,
                        raw_json = EXCLUDED.raw_json
                    """,
                    (
                        record.unattributed_id,
                        record.account_id,
                        record.trade_date,
                        record.qmt_order_id,
                        record.symbol,
                        record.reason,
                        record.order_remark,
                        _json(record.raw_json),
                        record.created_at,
                    ),
                )
        return record

    def list_unattributed_orders(
        self,
        account_id: str | None = None,
        trade_date: date | None = None,
    ) -> list[UnattributedOrderRecord]:
        filters: list[str] = []
        params: list[Any] = []
        if account_id is not None:
            filters.append("account_id = %s")
            params.append(account_id)
        if trade_date is not None:
            filters.append("trade_date = %s")
            params.append(trade_date)
        where = f"WHERE {' AND '.join(filters)}" if filters else ""
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    f"""
                    SELECT *
                    FROM qmt_strategy.unattributed_order
                    {where}
                    ORDER BY trade_date, qmt_order_id
                    """,
                    tuple(params),
                )
                rows = cur.fetchall()
        return [_row_to_unattributed_order(row) for row in rows]

    def upsert_unattributed_trade(self, record: UnattributedTradeRecord) -> UnattributedTradeRecord:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO qmt_strategy.unattributed_trade (
                        unattributed_id, account_id, trade_date, trade_id, qmt_order_id,
                        symbol, reason, order_remark, raw_json, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (account_id, trade_date, trade_id) DO UPDATE SET
                        qmt_order_id = EXCLUDED.qmt_order_id,
                        symbol = EXCLUDED.symbol,
                        reason = EXCLUDED.reason,
                        order_remark = EXCLUDED.order_remark,
                        raw_json = EXCLUDED.raw_json
                    """,
                    (
                        record.unattributed_id,
                        record.account_id,
                        record.trade_date,
                        record.trade_id,
                        record.qmt_order_id,
                        record.symbol,
                        record.reason,
                        record.order_remark,
                        _json(record.raw_json),
                        record.created_at,
                    ),
                )
        return record

    def list_unattributed_trades(
        self,
        account_id: str | None = None,
        trade_date: date | None = None,
    ) -> list[UnattributedTradeRecord]:
        filters: list[str] = []
        params: list[Any] = []
        if account_id is not None:
            filters.append("account_id = %s")
            params.append(account_id)
        if trade_date is not None:
            filters.append("trade_date = %s")
            params.append(trade_date)
        where = f"WHERE {' AND '.join(filters)}" if filters else ""
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    f"""
                    SELECT *
                    FROM qmt_strategy.unattributed_trade
                    {where}
                    ORDER BY trade_date, trade_id
                    """,
                    tuple(params),
                )
                rows = cur.fetchall()
        return [_row_to_unattributed_trade(row) for row in rows]


class InMemoryQmtStrategyLedgerRepository:
    """Local repository with database-like uniqueness semantics for tests."""

    def __init__(self) -> None:
        self._virtual_accounts: dict[str, VirtualAccount] = {}
        self._virtual_account_names: dict[tuple[str, str], str] = {}
        self._bindings: dict[str, StrategyPackageBinding] = {}
        self._active_bindings: dict[str, str] = {}
        self._binding_selection_evidence: dict[tuple[str, date], StrategyBindingSelectionEvidence] = {}
        self._order_batches: dict[str, OrderBatchRecord] = {}
        self._order_intents: dict[str, OrderIntentRecord] = {}
        self._order_remark_index: dict[tuple[str, str], str] = {}
        self._order_ledgers: dict[tuple[str, str], OrderLedgerRecord] = {}
        self._order_status_events: dict[str, OrderStatusEventRecord] = {}
        self._trade_ledgers: dict[tuple[str, date, str], TradeLedgerRecord] = {}
        self._position_lots: dict[str, PositionLotRecord] = {}
        self._cash_entries: dict[str, CashLedgerEntry] = {}
        self._cash_entry_sequence: dict[str, int] = {}
        self._next_cash_entry_sequence = 0
        self._daily_snapshots: dict[tuple[str, date], DailySnapshotRecord] = {}
        self._reconciliation_runs: dict[str, ReconciliationRunRecord] = {}
        self._reconciliation_issues: dict[str, ReconciliationIssueRecord] = {}
        self._unattributed_orders: dict[tuple[str, date, str], UnattributedOrderRecord] = {}
        self._unattributed_trades: dict[tuple[str, date, str], UnattributedTradeRecord] = {}

    def create_virtual_account(self, account: VirtualAccount) -> VirtualAccount:
        _validate_virtual_account(account)
        if account.strategy_id in self._virtual_accounts:
            raise ValueError(f"virtual account already exists: {account.strategy_id}")
        name_key = (account.account_id, account.strategy_name)
        if name_key in self._virtual_account_names:
            raise ValueError(f"strategy_name already exists in account: {account.strategy_name}")
        self._virtual_accounts[account.strategy_id] = account
        self._virtual_account_names[name_key] = account.strategy_id
        return account

    def get_virtual_account(self, strategy_id: str) -> VirtualAccount:
        account = self._virtual_accounts.get(strategy_id)
        if account is None:
            raise DataUnavailableError("qmt strategy virtual account does not exist", context={"strategy_id": strategy_id})
        return account

    def list_virtual_accounts(self, account_id: str | None = None) -> list[VirtualAccount]:
        accounts = list(self._virtual_accounts.values())
        if account_id is not None:
            accounts = [account for account in accounts if account.account_id == account_id]
        return sorted(accounts, key=lambda account: (account.created_at, account.strategy_id))

    def update_virtual_account(self, account: VirtualAccount) -> VirtualAccount:
        _validate_virtual_account(account)
        if account.strategy_id not in self._virtual_accounts:
            raise DataUnavailableError("qmt strategy virtual account does not exist", context={"strategy_id": account.strategy_id})
        original = self._virtual_accounts[account.strategy_id]
        if (original.account_id, original.strategy_name) != (account.account_id, account.strategy_name):
            raise ValueError("account_id and strategy_name are immutable for virtual account updates")
        self._virtual_accounts[account.strategy_id] = account
        return account

    def create_account_group_slots(self, group: MiniQmtAccountGroup) -> MiniQmtAccountGroup:
        existing_accounts = self.list_virtual_accounts(group.broker_account_id)
        _validate_account_group_slots(group, existing_accounts=existing_accounts)
        for slot in group.slots:
            self.create_virtual_account(slot.to_virtual_account(group))
        return group

    def list_account_group_slots(
        self,
        account_group_id: str,
        *,
        broker_account_id: str | None = None,
    ) -> list[MiniQmtStrategySlot]:
        accounts = self.list_virtual_accounts(broker_account_id)
        return _slots_from_accounts(accounts, account_group_id)

    def get_account_group(
        self,
        account_group_id: str,
        *,
        broker_account_id: str | None = None,
    ) -> MiniQmtAccountGroup:
        slots = self.list_account_group_slots(account_group_id, broker_account_id=broker_account_id)
        if not slots:
            raise DataUnavailableError(
                "MiniQMT account group does not exist",
                context={"account_group_id": account_group_id, "broker_account_id": broker_account_id},
            )
        return _group_from_slots(slots)

    def set_account_group_slot_status(
        self,
        *,
        account_group_id: str,
        strategy_slot_id: str,
        status: VirtualAccountStatus,
    ) -> MiniQmtStrategySlot:
        slots = self.list_account_group_slots(account_group_id)
        for slot in slots:
            if slot.strategy_slot_id != strategy_slot_id:
                continue
            account = self.get_virtual_account(slot.strategy_id)
            updated_metadata = dict(account.metadata)
            slot_meta = dict(updated_metadata.get(MINIQMT_STRATEGY_SLOT_METADATA_KEY) or {})
            slot_meta["status"] = status.value
            updated_metadata[MINIQMT_STRATEGY_SLOT_METADATA_KEY] = slot_meta
            self.update_virtual_account(replace(account, status=status, metadata=updated_metadata, updated_at=datetime.now(UTC)))
            updated = self.get_virtual_account(slot.strategy_id)
            mapped = MiniQmtStrategySlot.from_virtual_account(updated)
            if mapped is None:
                raise ValueError("updated slot metadata became invalid")
            return mapped
        raise DataUnavailableError(
            "MiniQMT strategy slot does not exist",
            context={"account_group_id": account_group_id, "strategy_slot_id": strategy_slot_id},
        )

    def create_package_binding(self, binding: StrategyPackageBinding) -> StrategyPackageBinding:
        _validate_package_binding(binding)
        self.get_virtual_account(binding.strategy_id)
        if binding.binding_id in self._bindings:
            raise ValueError(f"package binding already exists: {binding.binding_id}")
        if binding.binding_status == BindingStatus.ACTIVE and binding.strategy_id in self._active_bindings:
            raise ValueError(f"active package binding already exists for strategy: {binding.strategy_id}")
        self._bindings[binding.binding_id] = binding
        if binding.binding_status == BindingStatus.ACTIVE:
            self._active_bindings[binding.strategy_id] = binding.binding_id
        return binding

    def replace_active_package_binding(
        self,
        binding: StrategyPackageBinding,
        *,
        replaced_binding_id: str,
        reason: str,
    ) -> StrategyPackageBinding:
        _validate_package_binding(binding)
        self.get_virtual_account(binding.strategy_id)
        if binding.binding_id in self._bindings:
            raise ValueError(f"package binding already exists: {binding.binding_id}")
        active_id = self._active_bindings.get(binding.strategy_id)
        if active_id != replaced_binding_id:
            raise DataUnavailableError(
                "active package binding to replace does not exist",
                context={"binding_id": replaced_binding_id, "strategy_id": binding.strategy_id},
            )
        active = self._bindings[replaced_binding_id]
        self._bindings[replaced_binding_id] = replace(
            active,
            binding_status=BindingStatus.RETIRED,
            runtime_config={
                **dict(active.runtime_config or {}),
                "binding_lifecycle": {
                    **dict((active.runtime_config or {}).get("binding_lifecycle") or {}),
                    "replaced_by_binding_id": binding.binding_id,
                    "replace_reason": reason,
                    "replaced_at": datetime.now(UTC).isoformat(),
                },
            },
            updated_at=datetime.now(UTC),
        )
        self._bindings[binding.binding_id] = binding
        self._active_bindings[binding.strategy_id] = binding.binding_id
        return binding

    def get_active_package_binding(self, strategy_id: str) -> StrategyPackageBinding | None:
        binding_id = self._active_bindings.get(strategy_id)
        return self._bindings[binding_id] if binding_id else None

    def list_package_bindings(self, strategy_id: str) -> list[StrategyPackageBinding]:
        return sorted(
            [binding for binding in self._bindings.values() if binding.strategy_id == strategy_id],
            key=lambda binding: (binding.created_at, binding.binding_id),
        )

    def get_package_binding(self, binding_id: str) -> StrategyPackageBinding:
        binding = self._bindings.get(binding_id)
        if binding is None:
            raise DataUnavailableError("qmt strategy package binding does not exist", context={"binding_id": binding_id})
        return binding

    def record_binding_selection_evidence(
        self,
        evidence: StrategyBindingSelectionEvidence,
    ) -> StrategyBindingSelectionEvidence:
        _validate_binding_selection_evidence(evidence)
        binding = self.get_package_binding(evidence.binding_id)
        if binding.strategy_id != evidence.strategy_id or binding.package_id != evidence.package_id:
            raise ValueError("daily selection evidence must match binding strategy_id and package_id")
        key = (evidence.binding_id, evidence.trade_date)
        existing = self._binding_selection_evidence.get(key)
        if existing is not None:
            if _same_daily_selection_evidence(existing, evidence):
                return existing
            raise InvalidStateTransitionError(
                "daily selection evidence already exists for binding and trade_date",
                context={
                    "binding_id": evidence.binding_id,
                    "trade_date": evidence.trade_date.isoformat(),
                    "existing_selection_run_id": existing.selection_run_id,
                    "requested_selection_run_id": evidence.selection_run_id,
                    "existing_runtime_config_hash": existing.runtime_config_hash,
                    "requested_runtime_config_hash": evidence.runtime_config_hash,
                },
            )
        self._binding_selection_evidence[key] = evidence
        return evidence

    def get_binding_selection_evidence(
        self,
        binding_id: str,
        trade_date: date,
    ) -> StrategyBindingSelectionEvidence:
        evidence = self._binding_selection_evidence.get((binding_id, trade_date))
        if evidence is None:
            raise DataUnavailableError(
                "current-day MiniQMT selection evidence is missing; generate or resolve today's SelectionRun before order build",
                context={"binding_id": binding_id, "trade_date": trade_date.isoformat()},
            )
        return evidence

    def list_binding_selection_evidence(self, binding_id: str) -> list[StrategyBindingSelectionEvidence]:
        return sorted(
            [item for (stored_binding_id, _), item in self._binding_selection_evidence.items() if stored_binding_id == binding_id],
            key=lambda item: (item.trade_date, item.created_at, item.evidence_id),
        )

    def upsert_order_batch(self, batch: OrderBatchRecord) -> OrderBatchRecord:
        _validate_order_batch(batch)
        self._order_batches[batch.batch_id] = batch
        return batch

    def get_order_batch(self, batch_id: str) -> OrderBatchRecord | None:
        return self._order_batches.get(batch_id)

    def create_order_intent(self, intent: OrderIntentRecord) -> OrderIntentRecord:
        _validate_order_intent(intent)
        self.get_virtual_account(intent.strategy_id)
        if intent.intent_id in self._order_intents:
            raise ValueError(f"order intent already exists: {intent.intent_id}")
        remark_key = (intent.account_id, intent.order_remark)
        if remark_key in self._order_remark_index:
            raise ValueError(f"order_remark already exists in account: {intent.order_remark}")
        self._order_intents[intent.intent_id] = intent
        self._order_remark_index[remark_key] = intent.intent_id
        return intent

    def get_order_intent(self, intent_id: str) -> OrderIntentRecord:
        intent = self._order_intents.get(intent_id)
        if intent is None:
            raise DataUnavailableError("qmt strategy order intent does not exist", context={"intent_id": intent_id})
        return intent

    def list_order_intents_by_batch(self, batch_id: str) -> list[OrderIntentRecord]:
        return sorted(
            [intent for intent in self._order_intents.values() if intent.batch_id == batch_id],
            key=lambda intent: (intent.created_at, intent.intent_id),
        )

    def get_order_intent_by_remark(self, account_id: str, order_remark: str) -> OrderIntentRecord | None:
        intent_id = self._order_remark_index.get((account_id, order_remark))
        return self._order_intents[intent_id] if intent_id else None

    def list_open_sell_intents(
        self,
        strategy_id: str,
        symbol: str | None = None,
        trade_date: date | None = None,
    ) -> list[OrderIntentRecord]:
        intents = [
            intent
            for intent in self._order_intents.values()
            if intent.strategy_id == strategy_id
            and intent.side == "SELL"
            and intent.submit_status in {IntentSubmitStatus.CREATED, IntentSubmitStatus.SUBMITTED, IntentSubmitStatus.ACCEPTED}
        ]
        if symbol is not None:
            intents = [intent for intent in intents if intent.symbol == symbol]
        if trade_date is not None:
            intents = [intent for intent in intents if intent.trade_date == trade_date]
        return sorted(intents, key=lambda intent: (intent.created_at, intent.intent_id))

    def set_order_intent_submit_status(
        self,
        intent_id: str,
        status: IntentSubmitStatus,
        *,
        submitted_at: Any | None = None,
        updated_at: Any | None = None,
    ) -> OrderIntentRecord:
        intent = self.get_order_intent(intent_id)
        updated = replace(
            intent,
            submit_status=status,
            submitted_at=submitted_at or intent.submitted_at,
            updated_at=updated_at or intent.updated_at,
        )
        self._order_intents[intent_id] = updated
        return updated

    def upsert_order_ledger(self, order: OrderLedgerRecord) -> OrderLedgerRecord:
        self._order_ledgers[(order.account_id, order.qmt_order_id)] = order
        return order

    def append_order_status_event(self, event: OrderStatusEventRecord) -> OrderStatusEventRecord:
        self._order_status_events.setdefault(event.event_id, event)
        return event

    def upsert_trade_ledger(self, trade: TradeLedgerRecord) -> tuple[TradeLedgerRecord, bool]:
        key = (trade.account_id, trade.trade_date, trade.trade_id)
        existing = self._trade_ledgers.get(key)
        if existing is not None:
            return existing, False
        self._trade_ledgers[key] = trade
        return trade, True

    def create_position_lot(self, lot: PositionLotRecord) -> PositionLotRecord:
        if lot.lot_id in self._position_lots:
            raise ValueError(f"position lot already exists: {lot.lot_id}")
        if lot.quantity < 0 or lot.available_quantity < 0 or lot.remaining_quantity < 0:
            raise ValueError("position lot quantities must be non-negative")
        if lot.available_quantity > lot.remaining_quantity:
            raise ValueError("position lot available_quantity cannot exceed remaining_quantity")
        self._position_lots[lot.lot_id] = lot
        return lot

    def list_position_lots(self, strategy_id: str, symbol: str | None = None) -> list[PositionLotRecord]:
        lots = [lot for lot in self._position_lots.values() if lot.strategy_id == strategy_id]
        if symbol is not None:
            lots = [lot for lot in lots if lot.symbol == symbol]
        return sorted(lots, key=lambda lot: (lot.open_date, lot.lot_id))

    def update_position_lot(self, lot: PositionLotRecord) -> PositionLotRecord:
        if lot.lot_id not in self._position_lots:
            raise DataUnavailableError("qmt strategy position lot does not exist", context={"lot_id": lot.lot_id})
        if lot.quantity < 0 or lot.available_quantity < 0 or lot.remaining_quantity < 0:
            raise ValueError("position lot quantities must be non-negative")
        if lot.available_quantity > lot.remaining_quantity:
            raise ValueError("position lot available_quantity cannot exceed remaining_quantity")
        self._position_lots[lot.lot_id] = lot
        return lot

    def append_cash_entry(self, entry: CashLedgerEntry) -> CashLedgerEntry:
        if entry.cash_id in self._cash_entries:
            raise ValueError(f"cash ledger entry already exists: {entry.cash_id}")
        self._append_cash_entry(entry)
        return entry

    def append_cash_entry_once(self, entry: CashLedgerEntry) -> tuple[CashLedgerEntry, bool]:
        if entry.cash_id in self._cash_entries:
            return self._cash_entries[entry.cash_id], False
        self._append_cash_entry(entry)
        return entry, True

    def apply_cash_entry_once(self, entry: CashLedgerEntry, account: VirtualAccount) -> tuple[CashLedgerEntry, bool]:
        if entry.cash_id in self._cash_entries:
            return self._cash_entries[entry.cash_id], False
        _validate_virtual_account(account)
        original = self._virtual_accounts.get(account.strategy_id)
        if original is None:
            raise DataUnavailableError("qmt strategy virtual account does not exist", context={"strategy_id": account.strategy_id})
        if (original.account_id, original.strategy_name) != (account.account_id, account.strategy_name):
            raise ValueError("account_id and strategy_name are immutable for virtual account updates")
        self._append_cash_entry(entry)
        self._virtual_accounts[account.strategy_id] = account
        return entry, True

    def apply_cash_entry_and_lots_once(
        self,
        entry: CashLedgerEntry,
        account: VirtualAccount,
        lots: list[PositionLotRecord],
    ) -> tuple[CashLedgerEntry, bool]:
        if entry.cash_id in self._cash_entries:
            return self._cash_entries[entry.cash_id], False
        _validate_virtual_account(account)
        original = self._virtual_accounts.get(account.strategy_id)
        if original is None:
            raise DataUnavailableError("qmt strategy virtual account does not exist", context={"strategy_id": account.strategy_id})
        if (original.account_id, original.strategy_name) != (account.account_id, account.strategy_name):
            raise ValueError("account_id and strategy_name are immutable for virtual account updates")
        for lot in lots:
            if lot.lot_id not in self._position_lots:
                raise DataUnavailableError("qmt strategy position lot does not exist", context={"lot_id": lot.lot_id})
            if lot.quantity < 0 or lot.available_quantity < 0 or lot.remaining_quantity < 0:
                raise ValueError("position lot quantities must be non-negative")
            if lot.available_quantity > lot.remaining_quantity:
                raise ValueError("position lot available_quantity cannot exceed remaining_quantity")
        self._append_cash_entry(entry)
        self._virtual_accounts[account.strategy_id] = account
        for lot in lots:
            self._position_lots[lot.lot_id] = lot
        return entry, True

    def _append_cash_entry(self, entry: CashLedgerEntry) -> None:
        self._cash_entries[entry.cash_id] = entry
        self._cash_entry_sequence[entry.cash_id] = self._next_cash_entry_sequence
        self._next_cash_entry_sequence += 1

    def list_cash_entries(self, strategy_id: str) -> list[CashLedgerEntry]:
        entries = [entry for entry in self._cash_entries.values() if entry.strategy_id == strategy_id]
        return sorted(entries, key=lambda entry: (self._cash_entry_sequence.get(entry.cash_id, 0), entry.created_at, entry.cash_id))

    def get_cash_entry(self, cash_id: str) -> CashLedgerEntry | None:
        return self._cash_entries.get(cash_id)

    def create_daily_snapshot(self, snapshot: DailySnapshotRecord) -> DailySnapshotRecord:
        key = (snapshot.strategy_id, snapshot.trade_date)
        if key in self._daily_snapshots:
            raise ValueError(f"daily snapshot already exists: {snapshot.strategy_id} {snapshot.trade_date}")
        self._daily_snapshots[key] = snapshot
        return snapshot

    def create_reconciliation_run(self, run: ReconciliationRunRecord) -> ReconciliationRunRecord:
        if run.run_id in self._reconciliation_runs:
            raise ValueError(f"reconciliation run already exists: {run.run_id}")
        self._reconciliation_runs[run.run_id] = run
        return run

    def complete_reconciliation_run(self, run: ReconciliationRunRecord) -> ReconciliationRunRecord:
        if run.run_id not in self._reconciliation_runs:
            raise ValueError(f"reconciliation run does not exist: {run.run_id}")
        self._reconciliation_runs[run.run_id] = run
        return run

    def append_reconciliation_issue(self, issue: ReconciliationIssueRecord) -> ReconciliationIssueRecord:
        if issue.issue_id in self._reconciliation_issues:
            raise ValueError(f"reconciliation issue already exists: {issue.issue_id}")
        self._reconciliation_issues[issue.issue_id] = issue
        return issue

    def list_reconciliation_issues(self, run_id: str) -> list[ReconciliationIssueRecord]:
        issues = [issue for issue in self._reconciliation_issues.values() if issue.run_id == run_id]
        return sorted(issues, key=lambda issue: (issue.created_at, issue.issue_id))

    def upsert_unattributed_order(self, record: UnattributedOrderRecord) -> UnattributedOrderRecord:
        key = (record.account_id, record.trade_date, record.qmt_order_id)
        self._unattributed_orders[key] = record
        return record

    def list_unattributed_orders(
        self,
        account_id: str | None = None,
        trade_date: date | None = None,
    ) -> list[UnattributedOrderRecord]:
        records = list(self._unattributed_orders.values())
        if account_id is not None:
            records = [record for record in records if record.account_id == account_id]
        if trade_date is not None:
            records = [record for record in records if record.trade_date == trade_date]
        return sorted(records, key=lambda record: (record.trade_date, record.qmt_order_id))

    def upsert_unattributed_trade(self, record: UnattributedTradeRecord) -> UnattributedTradeRecord:
        key = (record.account_id, record.trade_date, record.trade_id)
        self._unattributed_trades[key] = record
        return record

    def list_unattributed_trades(
        self,
        account_id: str | None = None,
        trade_date: date | None = None,
    ) -> list[UnattributedTradeRecord]:
        records = list(self._unattributed_trades.values())
        if account_id is not None:
            records = [record for record in records if record.account_id == account_id]
        if trade_date is not None:
            records = [record for record in records if record.trade_date == trade_date]
        return sorted(records, key=lambda record: (record.trade_date, record.trade_id))


def _validate_virtual_account(account: VirtualAccount) -> None:
    _require_text(account.strategy_id, "strategy_id")
    _require_text(account.strategy_name, "strategy_name")
    _require_text(account.display_name, "display_name")
    _require_text(account.account_id, "account_id")
    if account.mode not in {"SIM", "LIVE"}:
        raise ValueError("mode must be SIM or LIVE")
    if account.initial_cash <= Decimal("0"):
        raise ValueError("initial_cash must be positive")
    if account.cash < Decimal("0") or account.frozen_cash < Decimal("0"):
        raise ValueError("cash and frozen_cash must be non-negative")


def _validate_account_group_slots(
    group: MiniQmtAccountGroup,
    *,
    existing_accounts: list[VirtualAccount],
) -> None:
    _require_text(group.account_group_id, "account_group_id")
    _require_text(group.broker_account_id, "broker_account_id")
    _require_text(group.broker_backend, "broker_backend")
    _require_text(group.broker_mode, "broker_mode")
    if group.allocation_mode != MINIQMT_ACCOUNT_GROUP_ALLOCATION_MODE:
        raise ValueError("MiniQMT account group allocation_mode must be account_group_slots")
    if not group.slots:
        raise ValueError("MiniQMT account group must include at least one strategy slot")
    if group.cash_limit is not None and group.cash_limit <= Decimal("0"):
        raise ValueError("MiniQMT account group cash_limit must be positive")

    existing_slots = _slots_from_accounts(existing_accounts, group.account_group_id)
    existing_strategy_names = {
        account.strategy_name: account.strategy_id
        for account in existing_accounts
        if account.account_id == group.broker_account_id
    }
    existing_slot_ids = {slot.strategy_slot_id: slot.strategy_id for slot in existing_slots}
    existing_prefixes = {slot.order_remark_prefix: slot.strategy_id for slot in existing_slots}
    seen_slot_ids: set[str] = set()
    seen_strategy_names: set[str] = set()
    seen_prefixes: set[str] = set()
    active_cash_total = Decimal("0")

    for slot in group.slots:
        _require_text(slot.account_group_id, "slot.account_group_id")
        _require_text(slot.strategy_slot_id, "slot.strategy_slot_id")
        _require_text(slot.strategy_id, "slot.strategy_id")
        _require_text(slot.strategy_name, "slot.strategy_name")
        _require_text(slot.display_name, "slot.display_name")
        _require_text(slot.account_id, "slot.account_id")
        _require_text(slot.order_remark_prefix, "slot.order_remark_prefix")
        if slot.account_group_id != group.account_group_id:
            raise ValueError("strategy slot account_group_id must match group")
        if slot.account_id != group.broker_account_id:
            raise ValueError("strategy slot account_id must match MiniQMT broker_account_id")
        if slot.broker_backend != group.broker_backend or slot.broker_mode.upper() != group.broker_mode.upper():
            raise ValueError("strategy slot broker backend/mode must match group")
        if slot.allocated_cash <= Decimal("0"):
            raise ValueError("strategy slot allocated_cash must be positive")
        if slot.strategy_slot_id in seen_slot_ids or (
            slot.strategy_slot_id in existing_slot_ids and existing_slot_ids[slot.strategy_slot_id] != slot.strategy_id
        ):
            raise ValueError(f"strategy_slot_id already exists in account group: {slot.strategy_slot_id}")
        if slot.strategy_name in seen_strategy_names or (
            slot.strategy_name in existing_strategy_names and existing_strategy_names[slot.strategy_name] != slot.strategy_id
        ):
            raise ValueError(f"strategy_name already exists in MiniQMT account: {slot.strategy_name}")
        if slot.order_remark_prefix in seen_prefixes or (
            slot.order_remark_prefix in existing_prefixes and existing_prefixes[slot.order_remark_prefix] != slot.strategy_id
        ):
            raise ValueError(f"order_remark_prefix already exists in account group: {slot.order_remark_prefix}")
        seen_slot_ids.add(slot.strategy_slot_id)
        seen_strategy_names.add(slot.strategy_name)
        seen_prefixes.add(slot.order_remark_prefix)
        if slot.status != VirtualAccountStatus.DISABLED:
            active_cash_total += slot.allocated_cash

    for existing_slot in existing_slots:
        if existing_slot.status != VirtualAccountStatus.DISABLED:
            active_cash_total += existing_slot.allocated_cash
    if group.cash_limit is not None and active_cash_total > group.cash_limit:
        raise ValueError(
            f"MiniQMT account group allocated_cash_total exceeds cash_limit: {active_cash_total} > {group.cash_limit}"
        )


def _slots_from_accounts(accounts: list[VirtualAccount], account_group_id: str) -> list[MiniQmtStrategySlot]:
    slots = []
    for account in accounts:
        slot = MiniQmtStrategySlot.from_virtual_account(account)
        if slot is not None and slot.account_group_id == account_group_id:
            slots.append(slot)
    return sorted(slots, key=lambda slot: (slot.strategy_slot_id, slot.strategy_id))


def _group_from_slots(slots: list[MiniQmtStrategySlot]) -> MiniQmtAccountGroup:
    first = slots[0]
    for slot in slots:
        if slot.account_group_id != first.account_group_id:
            raise ValueError("all slots must belong to the same account group")
        if slot.account_id != first.account_id:
            raise ValueError("all slots must belong to the same MiniQMT account")
    return MiniQmtAccountGroup(
        account_group_id=first.account_group_id,
        broker_account_id=first.account_id,
        broker_backend=first.broker_backend,
        broker_mode=first.broker_mode,
        cash_limit=first.account_group_cash_limit,
        slots=tuple(slots),
    )


def _validate_package_binding(binding: StrategyPackageBinding) -> None:
    _require_text(binding.binding_id, "binding_id")
    _require_text(binding.strategy_id, "strategy_id")
    _require_text(binding.package_id, "package_id")
    _require_text(binding.manifest_sha256, "manifest_sha256")
    if binding.target_weight is not None and binding.target_weight < Decimal("0"):
        raise ValueError("target_weight must be non-negative")
    if binding.top_k is not None and binding.top_k <= 0:
        raise ValueError("top_k must be positive")


def _validate_binding_selection_evidence(evidence: StrategyBindingSelectionEvidence) -> None:
    _require_text(evidence.evidence_id, "evidence_id")
    _require_text(evidence.binding_id, "binding_id")
    _require_text(evidence.strategy_id, "strategy_id")
    _require_text(evidence.package_id, "package_id")
    _require_text(evidence.selection_run_id, "selection_run_id")
    _require_text(evidence.data_source, "data_source")
    _require_text(evidence.manifest_sha256, "manifest_sha256")
    _require_text(evidence.runtime_config_hash, "runtime_config_hash")
    if evidence.score_count is not None and evidence.score_count < 0:
        raise ValueError("score_count must be non-negative")


def _same_daily_selection_evidence(
    left: StrategyBindingSelectionEvidence,
    right: StrategyBindingSelectionEvidence,
) -> bool:
    return (
        left.binding_id == right.binding_id
        and left.strategy_id == right.strategy_id
        and left.package_id == right.package_id
        and left.selection_run_id == right.selection_run_id
        and left.trade_date == right.trade_date
        and left.data_source == right.data_source
        and left.manifest_sha256 == right.manifest_sha256
        and left.runtime_config_hash == right.runtime_config_hash
        and left.artifact_id == right.artifact_id
        and left.artifact_sha256 == right.artifact_sha256
        and left.source_type == right.source_type
        and left.authority_scope == right.authority_scope
        and left.score_count == right.score_count
    )


def _validate_order_batch(batch: OrderBatchRecord) -> None:
    _require_text(batch.batch_id, "batch_id")
    _require_text(batch.account_id, "account_id")
    _require_text(batch.mode, "mode")
    if batch.mode not in {"SIM", "LIVE"}:
        raise ValueError("mode must be SIM or LIVE")
    if not isinstance(batch.batch_status, OrderBatchStatus):
        OrderBatchStatus(str(batch.batch_status))


def _validate_order_intent(intent: OrderIntentRecord) -> None:
    _require_text(intent.intent_id, "intent_id")
    _require_text(intent.strategy_id, "strategy_id")
    _require_text(intent.strategy_name, "strategy_name")
    _require_text(intent.symbol, "symbol")
    _require_text(intent.side, "side")
    _require_text(intent.order_remark, "order_remark")
    _require_text(intent.account_id, "account_id")
    if intent.side not in {"BUY", "SELL"}:
        raise ValueError("side must be BUY or SELL")
    if intent.order_type not in {BUY_ORDER_TYPE, SELL_ORDER_TYPE}:
        raise ValueError("order_type must be 23 or 24")
    if intent.quantity <= 0:
        raise ValueError("quantity must be positive")


def _require_text(value: str, field_name: str) -> None:
    if not str(value or "").strip():
        raise ValueError(f"{field_name} must be non-empty")


def _stable_id(prefix: str, *parts: Any) -> str:
    source = "|".join(str(part) for part in parts)
    return f"{prefix}_{sha1(source.encode('utf-8')).hexdigest()[:24]}"


def _enum_value(value: Any) -> str:
    if isinstance(value, Enum):
        return str(value.value)
    return str(value)


def _json(value: Any) -> psycopg2.extras.Json:
    return psycopg2.extras.Json(value if value is not None else {}, dumps=_json_dumps)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _row_to_virtual_account(row: dict[str, Any]) -> VirtualAccount:
    return VirtualAccount(
        strategy_id=row["strategy_id"],
        strategy_name=row["strategy_name"],
        display_name=row["display_name"],
        account_id=row["account_id"],
        mode=row["mode"],
        initial_cash=row["initial_cash"],
        cash=row["cash"],
        frozen_cash=row["frozen_cash"],
        market_value=row["market_value"],
        realized_pnl=row["realized_pnl"],
        unrealized_pnl=row["unrealized_pnl"],
        status=VirtualAccountStatus(row["status"]),
        risk_config=row["risk_config"] or {},
        metadata=row["metadata"] or {},
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _row_to_package_binding(row: dict[str, Any]) -> StrategyPackageBinding:
    return StrategyPackageBinding(
        binding_id=row["binding_id"],
        strategy_id=row["strategy_id"],
        package_id=row["package_id"],
        manifest_sha256=row["manifest_sha256"],
        selection_run_id=row["selection_run_id"],
        trade_date=row["trade_date"],
        target_weight=row["target_weight"],
        top_k=row["top_k"],
        binding_status=BindingStatus(row["binding_status"]),
        runtime_config=row["runtime_config"] or {},
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _row_to_binding_selection_evidence(row: dict[str, Any]) -> StrategyBindingSelectionEvidence:
    return StrategyBindingSelectionEvidence(
        evidence_id=row["evidence_id"],
        binding_id=row["binding_id"],
        strategy_id=row["strategy_id"],
        package_id=row["package_id"],
        selection_run_id=row["selection_run_id"],
        trade_date=row["trade_date"],
        data_source=row["data_source"],
        manifest_sha256=row["manifest_sha256"],
        runtime_config_hash=row["runtime_config_hash"],
        artifact_id=row["artifact_id"],
        artifact_sha256=row["artifact_sha256"],
        source_type=row["source_type"],
        authority_scope=row["authority_scope"],
        score_count=row["score_count"],
        metadata=row["metadata"] or {},
        created_at=row["created_at"],
    )


def _row_to_order_batch(row: dict[str, Any]) -> OrderBatchRecord:
    return OrderBatchRecord(
        batch_id=row["batch_id"],
        strategy_id=row["strategy_id"],
        account_id=row["account_id"],
        mode=row["mode"],
        batch_status=OrderBatchStatus(row["batch_status"]),
        requested_by=row["requested_by"],
        request_json=row["request_json"] or {},
        result_json=row["result_json"] or {},
        metadata=row["metadata"] or {},
        created_at=row["created_at"],
        submitted_at=row["submitted_at"],
        completed_at=row["completed_at"],
    )


def _row_to_order_intent(row: dict[str, Any]) -> OrderIntentRecord:
    return OrderIntentRecord(
        intent_id=row["intent_id"],
        strategy_id=row["strategy_id"],
        strategy_name=row["strategy_name"],
        symbol=row["symbol"],
        side=row["side"],
        order_type=row["order_type"],
        quantity=row["quantity"],
        price_type=row["price_type"],
        order_remark=row["order_remark"],
        account_id=row["account_id"],
        trade_date=row["trade_date"],
        batch_id=row["batch_id"],
        package_id=row["package_id"],
        selection_run_id=row["selection_run_id"],
        limit_price=row["limit_price"],
        target_weight=row["target_weight"],
        estimated_notional=row["estimated_notional"],
        estimated_fee=row["estimated_fee"],
        preflight_status=IntentPreflightStatus(row["preflight_status"]),
        submit_status=IntentSubmitStatus(row["submit_status"]),
        metadata=row["metadata"] or {},
        created_at=row["created_at"],
        submitted_at=row["submitted_at"],
        updated_at=row["updated_at"],
    )


def _row_to_position_lot(row: dict[str, Any]) -> PositionLotRecord:
    return PositionLotRecord(
        lot_id=row["lot_id"],
        strategy_id=row["strategy_id"],
        symbol=row["symbol"],
        open_trade_id=row["open_trade_id"],
        open_date=row["open_date"],
        quantity=row["quantity"],
        available_quantity=row["available_quantity"],
        remaining_quantity=row["remaining_quantity"],
        avg_cost=row["avg_cost"],
        cost_amount=row["cost_amount"],
        account_id=row["account_id"],
        open_time=row["open_time"],
        realized_pnl=row["realized_pnl"],
        status=PositionLotStatus(row["status"]),
        metadata=row["metadata"] or {},
    )


def _row_to_cash_entry(row: dict[str, Any]) -> CashLedgerEntry:
    return CashLedgerEntry(
        cash_id=row["cash_id"],
        strategy_id=row["strategy_id"],
        entry_type=CashEntryType(row["entry_type"]),
        cash_delta=row["cash_delta"],
        cash_after=row["cash_after"],
        account_id=row["account_id"],
        trade_date=row["trade_date"],
        frozen_delta=row["frozen_delta"],
        frozen_after=row["frozen_after"],
        intent_id=row["intent_id"],
        trade_id=row["trade_id"],
        symbol=row["symbol"],
        reason=row["reason"],
        metadata=row["metadata"] or {},
        created_at=row["created_at"],
    )


def _row_to_reconciliation_issue(row: dict[str, Any]) -> ReconciliationIssueRecord:
    return ReconciliationIssueRecord(
        issue_id=row["issue_id"],
        run_id=row["run_id"],
        issue_type=row["issue_type"],
        severity=row["severity"],
        message=row["message"],
        strategy_id=row["strategy_id"],
        symbol=row["symbol"],
        qmt_order_id=row["qmt_order_id"],
        trade_id=row["trade_id"],
        context=row["context"] or {},
        created_at=row["created_at"],
    )


def _row_to_unattributed_order(row: dict[str, Any]) -> UnattributedOrderRecord:
    return UnattributedOrderRecord(
        unattributed_id=row["unattributed_id"],
        account_id=row["account_id"],
        trade_date=row["trade_date"],
        qmt_order_id=row["qmt_order_id"],
        symbol=row["symbol"],
        reason=row["reason"],
        order_remark=row["order_remark"],
        raw_json=row["raw_json"] or {},
        created_at=row["created_at"],
    )


def _row_to_unattributed_trade(row: dict[str, Any]) -> UnattributedTradeRecord:
    return UnattributedTradeRecord(
        unattributed_id=row["unattributed_id"],
        account_id=row["account_id"],
        trade_date=row["trade_date"],
        trade_id=row["trade_id"],
        qmt_order_id=row["qmt_order_id"],
        symbol=row["symbol"],
        reason=row["reason"],
        order_remark=row["order_remark"],
        raw_json=row["raw_json"] or {},
        created_at=row["created_at"],
    )
