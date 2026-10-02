"""Set-based daily facts frozen once before simulation planning."""

from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, time
import hashlib
import json
import math
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from backend.db.pg_pool import get_conn
from backend.execution_algos.board_lot import board_lot_rule
from backend.services.data_refresh_audit import DataRefreshAuditRepository
from backend.services.simulation_data.contracts import (
    DAILY_PRE_CLOSE_QUOTE_SOURCES,
    PRICE_UNIT_DIVISOR,
    TDX_REALTIME_QUOTE_MAX_AGE,
    ConnFactory,
    DailyStStatus,
    DailySuspendStatus,
    EquityInstrumentMetadata,
    PreTradeTradabilityStatus,
    RealtimeQuoteFetcher,
    StStatusProvider,
    SuspendStatusProvider,
    _canonical_json_sha256,
    _local_sim_snapshot_json_value,
)
from backend.services.simulation_data.daily_context import (
    DailyTradingContextV1,
    DailyTradingSymbolFactV1,
)
from backend.services.simulation_data.tdx_causal_minute import (
    _first_number,
    _normalize_symbol_list,
    _normalize_side_by_symbol,
    _quote_price_basis,
    _require_tdx_quote_timestamp,
    quote_tradability_evidence,
)
from backend.services.trading_core.errors import DataUnavailableError


class DbSuspendStatusProvider:
    """Read daily A-share suspension rows from ``market.suspend_d``."""

    def __init__(self, conn_factory: ConnFactory | None = None) -> None:
        self.conn_factory = conn_factory or get_conn

    def get_suspend_status(self, symbol: str, trade_date: date) -> DailySuspendStatus:
        try:
            with self.conn_factory() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT suspend_type, suspend_timing
                        FROM market.suspend_d
                        WHERE ts_code = %s
                          AND trade_date = %s
                          AND suspend_type = 'S'
                        ORDER BY suspend_timing NULLS FIRST
                        LIMIT 1
                        """,
                        (symbol, trade_date),
                    )
                    row = cur.fetchone()
        except Exception as exc:
            raise DataUnavailableError(
                "suspend status query failed",
                context={
                    "symbol": symbol,
                    "trade_date": trade_date.isoformat(),
                    "table": "market.suspend_d",
                },
            ) from exc
        if row is None:
            return DailySuspendStatus(symbol=symbol, trade_date=trade_date, is_suspended=False)
        return DailySuspendStatus(
            symbol=symbol,
            trade_date=trade_date,
            is_suspended=True,
            suspend_type=str(row[0]) if row[0] is not None else "S",
            suspend_timing=str(row[1]) if row[1] is not None else None,
        )


class DbEquityInstrumentMetadataProvider:
    """Read ``market.stock_basic`` once during scheduler-owned context preload."""

    def __init__(self, conn_factory: ConnFactory | None = None) -> None:
        self.conn_factory = conn_factory or get_conn

    def get_equity_metadata(self, symbol: str, trade_date: date) -> EquityInstrumentMetadata:
        normalized_symbol = str(symbol or "").strip().upper()
        if not normalized_symbol:
            raise DataUnavailableError(
                "symbol is required for stock-basic metadata lookup",
                context={"trade_date": trade_date.isoformat(), "table": "market.stock_basic"},
            )
        try:
            with self.conn_factory() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT ts_code, market, exchange, list_status, list_date, delist_date
                        FROM market.stock_basic
                        WHERE ts_code = %s
                        LIMIT 1
                        """,
                        (normalized_symbol,),
                    )
                    row = cur.fetchone()
        except Exception as exc:
            raise DataUnavailableError(
                "stock-basic metadata query failed",
                context={
                    "symbol": normalized_symbol,
                    "trade_date": trade_date.isoformat(),
                    "table": "market.stock_basic",
                },
            ) from exc
        if row is None:
            raise DataUnavailableError(
                "stock-basic metadata row is missing for exact symbol",
                context={
                    "symbol": normalized_symbol,
                    "trade_date": trade_date.isoformat(),
                    "table": "market.stock_basic",
                },
            )
        if isinstance(row, dict):
            values = (
                row.get("ts_code"),
                row.get("market"),
                row.get("exchange"),
                row.get("list_status"),
                row.get("list_date"),
                row.get("delist_date"),
            )
        else:
            values = tuple(row)
        if len(values) != 6 or values[0] is None or values[1] is None or values[2] is None or values[3] is None:
            raise DataUnavailableError(
                "stock-basic metadata row is incomplete",
                context={
                    "symbol": normalized_symbol,
                    "trade_date": trade_date.isoformat(),
                    "table": "market.stock_basic",
                },
            )
        resolved_symbol, market, exchange, list_status, list_date, delist_date = values
        source_payload = {
            "symbol": str(resolved_symbol).strip().upper(),
            "market": str(market).strip(),
            "exchange": str(exchange).strip().upper(),
            "list_status": str(list_status).strip().upper(),
            "list_date": list_date.isoformat() if isinstance(list_date, date) else None,
            "delist_date": delist_date.isoformat() if isinstance(delist_date, date) else None,
        }
        source_version = hashlib.sha256(
            json.dumps(source_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        return EquityInstrumentMetadata(
            symbol=source_payload["symbol"],
            market=source_payload["market"],
            exchange=source_payload["exchange"],
            list_status=source_payload["list_status"],
            list_date=list_date if isinstance(list_date, date) else None,
            delist_date=delist_date if isinstance(delist_date, date) else None,
            product_type="EQUITY",
            source="market.stock_basic",
            source_version=f"market.stock_basic:{source_version}",
        )


class DbStStatusProvider:
    """Read point-in-time ST/*ST rows from ``market.stock_st``."""

    def __init__(self, conn_factory: ConnFactory | None = None) -> None:
        self.conn_factory = conn_factory or get_conn

    def get_st_status(self, symbol: str, trade_date: date) -> DailyStStatus:
        normalized_symbol = str(symbol or "").strip()
        if not normalized_symbol:
            raise DataUnavailableError(
                "symbol is required for ST status lookup",
                context={"reason_code": "ST_STATUS_SYMBOL_MISSING", "trade_date": trade_date.isoformat()},
            )
        try:
            with self.conn_factory() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        WITH latest_stock_st_snapshot AS (
                            SELECT max(ann_date) AS latest_ann_date
                            FROM market.stock_st
                            WHERE ann_date <= %s
                        )
                        SELECT s.ts_code, s.start_date, s.end_date, latest.latest_ann_date
                        FROM latest_stock_st_snapshot latest
                        LEFT JOIN LATERAL (
                            SELECT ts_code, start_date, end_date, ann_date
                            FROM market.stock_st
                            WHERE ts_code = %s
                              AND (
                                (start_date IS NULL AND end_date IS NULL AND ann_date = latest.latest_ann_date)
                                OR (
                                  (start_date IS NOT NULL OR end_date IS NOT NULL)
                                  AND COALESCE(start_date, ann_date) <= %s
                                  AND (end_date IS NULL OR end_date >= %s)
                                )
                              )
                            ORDER BY
                              CASE
                                WHEN start_date IS NULL AND end_date IS NULL THEN ann_date
                                ELSE COALESCE(start_date, ann_date)
                              END DESC,
                              ann_date DESC
                            LIMIT 1
                        ) s ON TRUE
                        """,
                        (trade_date, normalized_symbol, trade_date, trade_date),
                    )
                    row = cur.fetchone()
                    if row is not None and row[3] is None:
                        raise DataUnavailableError(
                            "ST status source has no snapshot on or before trade_date",
                            context={
                                "reason_code": "ST_STATUS_SOURCE_EMPTY",
                                "symbol": normalized_symbol,
                                "trade_date": trade_date.isoformat(),
                                "table": "market.stock_st",
                            },
                        )
        except Exception as exc:
            if isinstance(exc, DataUnavailableError):
                raise
            raise DataUnavailableError(
                "ST status query failed",
                context={
                    "reason_code": "ST_STATUS_QUERY_FAILED",
                    "symbol": normalized_symbol,
                    "trade_date": trade_date.isoformat(),
                    "table": "market.stock_st",
                },
            ) from exc
        if row is None or row[0] is None:
            return DailyStStatus(symbol=normalized_symbol, trade_date=trade_date, is_st=False)
        return DailyStStatus(
            symbol=normalized_symbol,
            trade_date=trade_date,
            is_st=True,
            source=f"market.stock_st.latest_ann_date:{row[3].isoformat()}",
            start_date=row[1],
            end_date=row[2],
        )


class DailyTradingContextProvider:
    """Materialize the exact plan symbol set once from audited daily facts."""

    ready_after = time(9, 10)

    def __init__(
        self,
        *,
        conn_factory: ConnFactory | None = None,
        audit_repository: DataRefreshAuditRepository | None = None,
    ) -> None:
        self.conn_factory = conn_factory or get_conn
        self.audit_repository = audit_repository or DataRefreshAuditRepository(conn_factory=self.conn_factory)

    def load_supporting_facts(
        self,
        *,
        symbols: list[str],
        trade_date: date,
    ) -> dict[str, Any]:
        """Read set-based ST/suspension facts without touching daily limit authority."""

        raw_symbols = [str(symbol or "").strip() for symbol in symbols if str(symbol or "").strip()]
        normalized_aliases = [symbol.upper() for symbol in raw_symbols]
        if len(raw_symbols) != len(set(normalized_aliases)) or any(
            symbol != normalized for symbol, normalized in zip(raw_symbols, normalized_aliases)
        ):
            raise DataUnavailableError(
                "daily supporting fact symbol set contains an alias collision",
                context={"reason_code": "DAILY_TRADING_CONTEXT_SYMBOL_ALIAS_COLLISION"},
            )
        normalized = tuple(sorted(set(normalized_aliases)))
        if not normalized:
            raise DataUnavailableError(
                "daily supporting facts require a non-empty exact symbol set",
                context={"reason_code": "DAILY_TRADING_CONTEXT_SYMBOL_SET_EMPTY"},
            )
        suspend_audit = self._require_refresh("suspend_d", trade_date)
        suspend_rows, st_rows = self._read_supporting_batches(normalized, trade_date)
        suspend_facts = self._validate_suspend_rows(
            rows=suspend_rows,
            requested=normalized,
            trade_date=trade_date,
        )
        st_facts, st_source_version = self._validate_st_rows(
            rows=st_rows,
            requested=normalized,
            trade_date=trade_date,
        )
        return {
            "schema_version": "daily_trading_supporting_facts_v1",
            "trade_date": trade_date.isoformat(),
            "symbol_set": list(normalized),
            "stock_st": {
                "source": "market.stock_st",
                "source_version": st_source_version,
                "batch_hash": _canonical_json_sha256(
                    {symbol: st_facts[symbol]["evidence_hash"] for symbol in normalized}
                ),
            },
            "suspend_d": {
                "source": "market.suspend_d",
                "dataset": "suspend_d",
                "trade_date": trade_date.isoformat(),
                "refresh_identity": self._refresh_identity(suspend_audit),
                "available_at": suspend_audit.refreshed_at.isoformat(),
                "batch_hash": _canonical_json_sha256(suspend_facts),
            },
            "stock_st_facts": st_facts,
            "suspend_facts": suspend_facts,
        }

    def load_stk_limit_authority_attempt(
        self,
        *,
        symbols: list[str],
        trade_date: date,
    ) -> dict[str, Any]:
        """Read one exact-symbol stk_limit attempt without converting availability gaps into corruption."""

        raw_symbols = [str(symbol or "").strip() for symbol in symbols if str(symbol or "").strip()]
        aliases = [symbol.upper() for symbol in raw_symbols]
        if len(raw_symbols) != len(set(aliases)) or any(symbol != alias for symbol, alias in zip(raw_symbols, aliases)):
            raise DataUnavailableError(
                "stk_limit authority symbol set contains an alias collision",
                context={"reason_code": "DAILY_TRADING_CONTEXT_SYMBOL_ALIAS_COLLISION"},
            )
        requested = tuple(sorted(aliases))
        if not requested:
            raise DataUnavailableError(
                "stk_limit authority requires a non-empty exact symbol set",
                context={"reason_code": "DAILY_TRADING_CONTEXT_SYMBOL_SET_EMPTY"},
            )
        try:
            audit = self.audit_repository.require_success(dataset="stk_limit", trade_date=trade_date)
        except DataUnavailableError as exc:
            return {
                "schema_version": "stk_limit_authority_attempt_v1",
                "trade_date": trade_date.isoformat(),
                "symbol_set": list(requested),
                "availability": "UNAVAILABLE",
                "unavailable_reason": exc.message,
                "refresh_identity": None,
                "rows": [],
            }
        refresh_identity = self._refresh_identity(audit)
        if int(audit.row_count or 0) == 0:
            return {
                "schema_version": "stk_limit_authority_attempt_v1",
                "trade_date": trade_date.isoformat(),
                "symbol_set": list(requested),
                "availability": "ZERO_ROWS",
                "unavailable_reason": None,
                "refresh_identity": refresh_identity,
                "rows": [],
            }
        try:
            with self.conn_factory() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT ts_code, trade_date, pre_close, up_limit, down_limit
                        FROM market.stk_limit
                        WHERE ts_code = ANY(%s) AND trade_date = %s
                        ORDER BY ts_code
                        """,
                        (list(requested), trade_date),
                    )
                    rows = list(cur.fetchall())
        except Exception as exc:
            return {
                "schema_version": "stk_limit_authority_attempt_v1",
                "trade_date": trade_date.isoformat(),
                "symbol_set": list(requested),
                "availability": "UNAVAILABLE",
                "unavailable_reason": type(exc).__name__,
                "refresh_identity": refresh_identity,
                "rows": [],
            }
        return {
            "schema_version": "stk_limit_authority_attempt_v1",
            "trade_date": trade_date.isoformat(),
            "symbol_set": list(requested),
            "availability": "AVAILABLE",
            "unavailable_reason": None,
            "refresh_identity": refresh_identity,
            "rows": [
                {
                    "symbol": row[0],
                    "trade_date": row[1].isoformat() if isinstance(row[1], date) else row[1],
                    "pre_close": float(row[2]) if row[2] is not None else None,
                    "up_limit": float(row[3]) if row[3] is not None else None,
                    "down_limit": float(row[4]) if row[4] is not None else None,
                }
                for row in rows
            ],
        }

    def load(
        self,
        *,
        symbols: list[str],
        trade_date: date,
        as_of_time: datetime,
        calendar_service_snapshot: Mapping[str, Any],
        binding_identity: str,
        package_identity: str,
        release_identity: str,
        pre_close_quote_fetcher: RealtimeQuoteFetcher | None = None,
        pre_close_quote_source: str | None = None,
    ) -> DailyTradingContextV1:
        from backend.services.simulation_data.daily_context import (
            DailyTradingContextV1,
            DailyTradingSymbolFactV1,
        )

        captured_at = (
            as_of_time.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
            if as_of_time.tzinfo is None
            else as_of_time.astimezone(ZoneInfo("Asia/Shanghai"))
        )
        raw_symbols = [str(symbol or "").strip() for symbol in symbols if str(symbol or "").strip()]
        normalized_aliases = [symbol.upper() for symbol in raw_symbols]
        if len(set(raw_symbols)) != len(set(normalized_aliases)):
            raise DataUnavailableError(
                "daily trading context symbol set contains an alias collision",
                context={"reason_code": "DAILY_TRADING_CONTEXT_SYMBOL_ALIAS_COLLISION"},
            )
        normalized = tuple(sorted(set(normalized_aliases)))
        if not normalized:
            raise DataUnavailableError(
                "daily trading context requires a non-empty exact symbol set",
                context={"reason_code": "DAILY_TRADING_CONTEXT_SYMBOL_SET_EMPTY"},
            )
        if captured_at.date() != trade_date:
            raise DataUnavailableError(
                "daily trading context time must match trade_date",
                context={
                    "reason_code": "DAILY_TRADING_CONTEXT_TRADE_DATE_MISMATCH",
                    "trade_date": trade_date.isoformat(),
                    "as_of_time": as_of_time.isoformat(),
                },
            )
        if captured_at.time().replace(tzinfo=None) < self.ready_after:
            raise DataUnavailableError(
                "daily trading context is waiting for the 09:10 stk_limit window",
                context={
                    "reason_code": "DAILY_TRADING_CONTEXT_WAITING_STK_LIMIT_WINDOW",
                    "trade_date": trade_date.isoformat(),
                    "ready_after": "09:10:00",
                    "retryable": True,
                },
            )
        if calendar_service_snapshot.get("is_trading_day") is not True:
            raise DataUnavailableError(
                "daily trading context requires an authoritative trading-day snapshot",
                context={
                    "reason_code": "DAILY_TRADING_CONTEXT_CALENDAR_SNAPSHOT_INVALID",
                    "trade_date": trade_date.isoformat(),
                },
            )

        stk_audit = self._require_refresh("stk_limit", trade_date)
        suspend_audit = self._require_refresh("suspend_d", trade_date)
        stk_rows, suspend_rows, st_rows = self._read_exact_batches(normalized, trade_date)
        symbol_hash = _canonical_json_sha256(list(normalized))
        stk_facts = self._validate_stk_limit_rows(
            rows=stk_rows,
            requested=normalized,
            trade_date=trade_date,
            refresh_identity=self._refresh_identity(stk_audit),
        )
        self._resolve_missing_pre_close(
            facts=stk_facts,
            requested=normalized,
            trade_date=trade_date,
            as_of_time=captured_at,
            quote_fetcher=pre_close_quote_fetcher,
            quote_source=pre_close_quote_source,
        )
        suspend_facts = self._validate_suspend_rows(
            rows=suspend_rows,
            requested=normalized,
            trade_date=trade_date,
        )
        st_facts, st_source_version = self._validate_st_rows(
            rows=st_rows,
            requested=normalized,
            trade_date=trade_date,
        )
        facts: dict[str, DailyTradingSymbolFactV1] = {}
        for symbol in normalized:
            min_quantity, increment = board_lot_rule(symbol)
            code = symbol.split(".", 1)[0]
            board = (
                "STAR"
                if code.startswith(("688", "689"))
                else "CHINEXT"
                if code.startswith(("300", "301", "302"))
                else "MAIN"
            )
            limit = stk_facts[symbol]
            suspend = suspend_facts[symbol]
            st = st_facts[symbol]
            facts[symbol] = DailyTradingSymbolFactV1(
                symbol=symbol,
                trade_date=trade_date,
                pre_close=limit["pre_close"],
                pre_close_source=limit["pre_close_source"],
                pre_close_evidence_hash=limit["pre_close_evidence_hash"],
                up_limit=limit["up_limit"],
                down_limit=limit["down_limit"],
                stk_limit_row_hash=limit["row_hash"],
                is_st=st["is_st"],
                st_source=st["source"],
                st_evidence_hash=st["evidence_hash"],
                is_suspended=suspend["is_suspended"],
                suspend_type=suspend["suspend_type"],
                suspend_timing=suspend["suspend_timing"],
                suspend_source="market.suspend_d",
                board=board,
                lot_rule={"min_quantity": min_quantity, "increment": increment},
            )

        calendar_payload = _local_sim_snapshot_json_value(dict(calendar_service_snapshot))
        calendar_snapshot_id = f"tcal_{_canonical_json_sha256(calendar_payload)[:16]}"
        plan_identity = _canonical_json_sha256(
            {
                "binding_identity": binding_identity,
                "package_identity": package_identity,
                "release_identity": release_identity,
                "trade_date": trade_date.isoformat(),
                "symbol_set_hash": symbol_hash,
            }
        )
        sources = {
            "stk_limit": {
                "source": "market.stk_limit",
                "dataset": "stk_limit",
                "trade_date": trade_date.isoformat(),
                "refresh_identity": self._refresh_identity(stk_audit),
                "available_at": stk_audit.refreshed_at.isoformat(),
                "batch_hash": _canonical_json_sha256({symbol: stk_facts[symbol]["row_hash"] for symbol in normalized}),
                "pre_close_authority": {
                    "policy": "raw_stk_limit_else_broker_bound_plan_quote",
                    "sources": sorted({stk_facts[symbol]["pre_close_source"] for symbol in normalized}),
                },
            },
            "stock_st": {
                "source": "market.stock_st",
                "source_version": st_source_version,
                "batch_hash": _canonical_json_sha256(
                    {symbol: st_facts[symbol]["evidence_hash"] for symbol in normalized}
                ),
            },
            "suspend_d": {
                "source": "market.suspend_d",
                "dataset": "suspend_d",
                "trade_date": trade_date.isoformat(),
                "refresh_identity": self._refresh_identity(suspend_audit),
                "available_at": suspend_audit.refreshed_at.isoformat(),
                "batch_hash": _canonical_json_sha256(suspend_facts),
            },
        }
        seed = {
            "schema_version": "daily_trading_context_v1",
            "trade_date": trade_date.isoformat(),
            "timezone": "Asia/Shanghai",
            "plan_identity": plan_identity,
            "binding_identity": binding_identity,
            "package_identity": package_identity,
            "symbol_set": list(normalized),
            "symbol_set_hash": symbol_hash,
            "calendar_service_snapshot_id": calendar_snapshot_id,
            "captured_at": captured_at.isoformat(),
            "sources": sources,
            "symbols": {symbol: fact.canonical_payload() for symbol, fact in sorted(facts.items())},
        }
        digest = _canonical_json_sha256(seed)
        return DailyTradingContextV1(
            context_id=f"dtc_{digest[:16]}",
            context_hash=digest,
            trade_date=trade_date,
            plan_identity=plan_identity,
            binding_identity=binding_identity,
            package_identity=package_identity,
            symbol_set=normalized,
            symbol_set_hash=symbol_hash,
            calendar_service_snapshot_id=calendar_snapshot_id,
            captured_at=captured_at,
            sources=sources,
            symbols=facts,
        )

    @staticmethod
    def to_pre_trade_statuses(context: DailyTradingContextV1) -> dict[str, dict[str, Any]]:
        statuses: dict[str, dict[str, Any]] = {}
        for symbol, fact in context.symbols.items():
            statuses[symbol] = {
                "schema_version": "pre_trade_tradability_status_v1",
                "symbol": symbol,
                "trade_date": context.trade_date.isoformat(),
                "is_tradable": not fact.is_suspended,
                "reason_code": "SUSPENDED_BY_SUSPEND_D" if fact.is_suspended else "PRE_TRADE_TRADABLE",
                "source": "daily_trading_context_v1",
                "suspend_status": {
                    "is_suspended": fact.is_suspended,
                    "suspend_type": fact.suspend_type,
                    "suspend_timing": fact.suspend_timing,
                    "source": fact.suspend_source,
                },
                "quote_evidence": None,
                "daily_trading_context": {
                    "schema_version": "daily_trading_context_reference_v1",
                    "context_id": context.context_id,
                    "context_hash": context.context_hash,
                    "trade_date": context.trade_date.isoformat(),
                    "symbol_set_hash": context.symbol_set_hash,
                    "stk_limit_row_hash": fact.stk_limit_row_hash,
                    "source": "market.stk_limit",
                    "symbol_fact": fact.canonical_payload(),
                    "context": context.carrier_payload(),
                },
            }
        return statuses

    def _require_refresh(self, dataset: str, trade_date: date) -> Any:
        try:
            return self.audit_repository.require_success(dataset=dataset, trade_date=trade_date)
        except DataUnavailableError as exc:
            raise DataUnavailableError(
                "daily trading context is waiting for dataset refresh",
                context={
                    "reason_code": (
                        "DAILY_TRADING_CONTEXT_WAITING_STK_LIMIT_REFRESH"
                        if dataset == "stk_limit"
                        else "DAILY_TRADING_CONTEXT_WAITING_SUSPEND_D_REFRESH"
                    ),
                    "dataset": dataset,
                    "trade_date": trade_date.isoformat(),
                    "retryable": True,
                    "cause": exc.message,
                },
            ) from exc

    def _read_exact_batches(
        self,
        symbols: tuple[str, ...],
        trade_date: date,
    ) -> tuple[list[Any], list[Any], list[Any]]:
        try:
            with self.conn_factory() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT ts_code, trade_date, pre_close, up_limit, down_limit
                        FROM market.stk_limit
                        WHERE ts_code = ANY(%s) AND trade_date = %s
                        ORDER BY ts_code
                        """,
                        (list(symbols), trade_date),
                    )
                    stk_rows = list(cur.fetchall())
                    cur.execute(
                        """
                        SELECT ts_code, trade_date, suspend_type, suspend_timing
                        FROM market.suspend_d
                        WHERE ts_code = ANY(%s) AND trade_date = %s AND suspend_type = 'S'
                        ORDER BY ts_code, suspend_timing NULLS FIRST
                        """,
                        (list(symbols), trade_date),
                    )
                    suspend_rows = list(cur.fetchall())
                    cur.execute(
                        """
                        WITH requested AS (
                            SELECT unnest(%s::text[]) AS ts_code
                        ), latest AS (
                            SELECT max(ann_date) AS latest_ann_date
                            FROM market.stock_st
                            WHERE ann_date <= %s
                        )
                        SELECT r.ts_code, s.ts_code IS NOT NULL, s.start_date, s.end_date,
                               latest.latest_ann_date
                        FROM requested r
                        CROSS JOIN latest
                        LEFT JOIN LATERAL (
                            SELECT ts_code, start_date, end_date, ann_date
                            FROM market.stock_st
                            WHERE ts_code = r.ts_code
                              AND (
                                (start_date IS NULL AND end_date IS NULL AND ann_date = latest.latest_ann_date)
                                OR ((start_date IS NOT NULL OR end_date IS NOT NULL)
                                    AND COALESCE(start_date, ann_date) <= %s
                                    AND (end_date IS NULL OR end_date >= %s))
                              )
                            ORDER BY COALESCE(start_date, ann_date) DESC, ann_date DESC
                            LIMIT 1
                        ) s ON TRUE
                        ORDER BY r.ts_code
                        """,
                        (list(symbols), trade_date, trade_date, trade_date),
                    )
                    st_rows = list(cur.fetchall())
        except DataUnavailableError:
            raise
        except Exception as exc:
            raise DataUnavailableError(
                "daily trading context batch query failed",
                context={
                    "reason_code": "DAILY_TRADING_CONTEXT_BATCH_QUERY_FAILED",
                    "trade_date": trade_date.isoformat(),
                    "symbol_count": len(symbols),
                },
            ) from exc
        return stk_rows, suspend_rows, st_rows

    def _read_supporting_batches(
        self,
        symbols: tuple[str, ...],
        trade_date: date,
    ) -> tuple[list[Any], list[Any]]:
        try:
            with self.conn_factory() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT ts_code, trade_date, suspend_type, suspend_timing
                        FROM market.suspend_d
                        WHERE ts_code = ANY(%s) AND trade_date = %s AND suspend_type = 'S'
                        ORDER BY ts_code, suspend_timing NULLS FIRST
                        """,
                        (list(symbols), trade_date),
                    )
                    suspend_rows = list(cur.fetchall())
                    cur.execute(
                        """
                        WITH requested AS (
                            SELECT unnest(%s::text[]) AS ts_code
                        ), latest AS (
                            SELECT max(ann_date) AS latest_ann_date
                            FROM market.stock_st
                            WHERE ann_date <= %s
                        )
                        SELECT r.ts_code, s.ts_code IS NOT NULL, s.start_date, s.end_date,
                               latest.latest_ann_date
                        FROM requested r
                        CROSS JOIN latest
                        LEFT JOIN LATERAL (
                            SELECT ts_code, start_date, end_date, ann_date
                            FROM market.stock_st
                            WHERE ts_code = r.ts_code
                              AND (
                                (start_date IS NULL AND end_date IS NULL AND ann_date = latest.latest_ann_date)
                                OR ((start_date IS NOT NULL OR end_date IS NOT NULL)
                                    AND COALESCE(start_date, ann_date) <= %s
                                    AND (end_date IS NULL OR end_date >= %s))
                              )
                            ORDER BY COALESCE(start_date, ann_date) DESC, ann_date DESC
                            LIMIT 1
                        ) s ON TRUE
                        ORDER BY r.ts_code
                        """,
                        (list(symbols), trade_date, trade_date, trade_date),
                    )
                    st_rows = list(cur.fetchall())
        except DataUnavailableError:
            raise
        except Exception as exc:
            raise DataUnavailableError(
                "daily supporting fact batch query failed",
                context={
                    "reason_code": "DAILY_TRADING_CONTEXT_SUPPORTING_FACT_QUERY_FAILED",
                    "trade_date": trade_date.isoformat(),
                    "symbol_count": len(symbols),
                },
            ) from exc
        return suspend_rows, st_rows

    @staticmethod
    def _refresh_identity(status: Any) -> str:
        payload = {
            "dataset": status.dataset,
            "trade_date": status.trade_date.isoformat(),
            "data_source": status.data_source,
            "status": status.status,
            "row_count": status.row_count,
            "refreshed_at": status.refreshed_at.isoformat(),
            "job_id": status.job_id,
            "quality_status": status.quality_status,
        }
        return f"refresh_{_canonical_json_sha256(payload)[:24]}"

    @staticmethod
    def _validate_stk_limit_rows(
        *,
        rows: list[Any],
        requested: tuple[str, ...],
        trade_date: date,
        refresh_identity: str,
    ) -> dict[str, dict[str, Any]]:
        parsed: dict[str, dict[str, Any]] = {}
        duplicates: list[str] = []
        extras: list[str] = []
        for row in rows:
            symbol = str(row[0] or "").strip()
            if symbol not in requested:
                extras.append(symbol)
                continue
            if symbol in parsed:
                duplicates.append(symbol)
                continue
            if row[1] != trade_date:
                raise DataUnavailableError(
                    "stk_limit row trade_date conflicts with the plan",
                    context={"reason_code": "DAILY_TRADING_CONTEXT_STK_LIMIT_CROSS_DATE", "symbol": symbol},
                )
            try:
                pre_close = float(row[2]) if row[2] is not None else None
                up_limit, down_limit = (float(row[3]), float(row[4]))
            except (TypeError, ValueError) as exc:
                raise DataUnavailableError(
                    "stk_limit row contains invalid prices",
                    context={"reason_code": "DAILY_TRADING_CONTEXT_STK_LIMIT_INVALID", "symbol": symbol},
                ) from exc
            if (
                not all(math.isfinite(value) and value > 0 for value in (up_limit, down_limit))
                or not down_limit < up_limit
                or (
                    pre_close is not None
                    and (not math.isfinite(pre_close) or pre_close <= 0 or not down_limit < pre_close < up_limit)
                )
            ):
                raise DataUnavailableError(
                    "stk_limit row violates the raw price contract",
                    context={"reason_code": "DAILY_TRADING_CONTEXT_STK_LIMIT_INVALID", "symbol": symbol},
                )
            row_payload: dict[str, Any] = {
                "source": "market.stk_limit",
                "symbol": symbol,
                "trade_date": trade_date.isoformat(),
                "pre_close": pre_close,
                "up_limit": up_limit,
                "down_limit": down_limit,
                "price_basis": "raw",
            }
            parsed[symbol] = {
                **row_payload,
                "pre_close_source": "market.stk_limit.pre_close" if pre_close is not None else None,
                "pre_close_evidence_hash": None,
                "row_hash": _canonical_json_sha256(row_payload) if pre_close is not None else None,
            }
        missing = sorted(set(requested) - set(parsed))
        if missing or duplicates or extras or len(rows) != len(requested):
            raise DataUnavailableError(
                "stk_limit batch does not exactly cover the plan symbol set",
                context={
                    "reason_code": "DAILY_TRADING_CONTEXT_STK_LIMIT_COVERAGE_INVALID",
                    "trade_date": trade_date.isoformat(),
                    "missing": missing[:20],
                    "duplicates": sorted(set(duplicates))[:20],
                    "extras": sorted(set(extras))[:20],
                    "refresh_identity": refresh_identity,
                },
            )
        return parsed

    @staticmethod
    def _resolve_missing_pre_close(
        *,
        facts: dict[str, dict[str, Any]],
        requested: tuple[str, ...],
        trade_date: date,
        as_of_time: datetime,
        quote_fetcher: RealtimeQuoteFetcher | None,
        quote_source: str | None,
    ) -> None:
        missing = [symbol for symbol in requested if facts[symbol]["pre_close"] is None]
        if not missing:
            return
        source = str(quote_source or "").strip()
        if quote_fetcher is None or not source:
            raise DataUnavailableError(
                "stk_limit pre_close is absent and broker-bound quote authority is unavailable",
                context={
                    "reason_code": "DAILY_TRADING_CONTEXT_PRE_CLOSE_QUOTE_REQUIRED",
                    "trade_date": trade_date.isoformat(),
                    "missing": missing[:20],
                },
            )
        if source not in DAILY_PRE_CLOSE_QUOTE_SOURCES:
            raise DataUnavailableError(
                "broker-bound pre_close quote source is not approved",
                context={
                    "reason_code": "DAILY_TRADING_CONTEXT_PRE_CLOSE_QUOTE_SOURCE_INVALID",
                    "trade_date": trade_date.isoformat(),
                    "quote_source": source,
                },
            )
        try:
            quote_rows = quote_fetcher(list(missing))
        except DataUnavailableError:
            raise
        except Exception as exc:
            raise DataUnavailableError(
                "broker-bound pre_close quote fetch failed",
                context={
                    "reason_code": "DAILY_TRADING_CONTEXT_PRE_CLOSE_QUOTE_FETCH_FAILED",
                    "trade_date": trade_date.isoformat(),
                    "quote_source": source,
                    "symbols": missing[:20],
                    "error_type": type(exc).__name__,
                },
            ) from exc
        if not isinstance(quote_rows, dict):
            raise DataUnavailableError(
                "broker-bound pre_close quote payload must be keyed by symbol",
                context={
                    "reason_code": "DAILY_TRADING_CONTEXT_PRE_CLOSE_QUOTE_INVALID",
                    "trade_date": trade_date.isoformat(),
                    "quote_source": source,
                },
            )
        normalized_quotes = {str(symbol or "").strip().upper(): row for symbol, row in quote_rows.items()}
        alias_collision = len(quote_rows) != len(normalized_quotes)
        missing_quotes = sorted(set(missing) - set(normalized_quotes))
        extra_quotes = sorted(set(normalized_quotes) - set(missing))
        if alias_collision or missing_quotes or extra_quotes or len(normalized_quotes) != len(missing):
            raise DataUnavailableError(
                "broker-bound pre_close quotes do not exactly cover the missing symbol set",
                context={
                    "reason_code": "DAILY_TRADING_CONTEXT_PRE_CLOSE_QUOTE_COVERAGE_INVALID",
                    "trade_date": trade_date.isoformat(),
                    "quote_source": source,
                    "missing": missing_quotes[:20],
                    "extras": extra_quotes[:20],
                    "alias_collision": alias_collision,
                },
            )
        for symbol in missing:
            quote = normalized_quotes[symbol]
            if not isinstance(quote, dict):
                raise DataUnavailableError(
                    "broker-bound pre_close quote row is invalid",
                    context={
                        "reason_code": "DAILY_TRADING_CONTEXT_PRE_CLOSE_QUOTE_INVALID",
                        "trade_date": trade_date.isoformat(),
                        "quote_source": source,
                        "symbol": symbol,
                    },
                )
            kline = quote.get("K") if isinstance(quote.get("K"), dict) else {}
            source_pre_close = _first_number(
                kline,
                ("Last", "pre_close", "PreClose", "preClose", "preclose"),
            )
            if source_pre_close is None:
                source_pre_close = _first_number(
                    quote,
                    ("pre_close", "preClose", "preclose", "lastClose", "last_close"),
                )
            source_price_basis = _quote_price_basis(quote, source=source)
            pre_close = (
                source_pre_close / PRICE_UNIT_DIVISOR
                if source_pre_close is not None and source_price_basis == "raw_li"
                else source_pre_close
            )
            quote_timestamp = _require_tdx_quote_timestamp(
                symbol=symbol,
                quote=quote,
                trade_date=trade_date,
                as_of_time=as_of_time,
                source=source,
                max_quote_age=TDX_REALTIME_QUOTE_MAX_AGE,
            )
            limit = facts[symbol]
            if (
                pre_close is None
                or not math.isfinite(pre_close)
                or pre_close <= 0
                or not limit["down_limit"] < pre_close < limit["up_limit"]
            ):
                raise DataUnavailableError(
                    "broker-bound pre_close quote violates authoritative stk_limit bounds",
                    context={
                        "reason_code": "DAILY_TRADING_CONTEXT_PRE_CLOSE_QUOTE_INVALID",
                        "trade_date": trade_date.isoformat(),
                        "quote_source": source,
                        "symbol": symbol,
                    },
                )
            evidence_payload = {
                "schema_version": "daily_pre_close_quote_evidence_v1",
                "symbol": symbol,
                "trade_date": trade_date.isoformat(),
                "source": source,
                "quote_timestamp": quote_timestamp.isoformat(),
                "pre_close": pre_close,
                "source_pre_close": source_pre_close,
                "source_price_basis": source_price_basis,
            }
            evidence_hash = _canonical_json_sha256(evidence_payload)
            row_payload = {
                "source": "market.stk_limit",
                "symbol": symbol,
                "trade_date": trade_date.isoformat(),
                "pre_close": pre_close,
                "up_limit": limit["up_limit"],
                "down_limit": limit["down_limit"],
                "price_basis": "raw",
                "pre_close_source": source,
                "pre_close_evidence_hash": evidence_hash,
            }
            limit.update(
                pre_close=pre_close,
                pre_close_source=source,
                pre_close_evidence_hash=evidence_hash,
                row_hash=_canonical_json_sha256(row_payload),
            )

    @staticmethod
    def _validate_suspend_rows(
        *,
        rows: list[Any],
        requested: tuple[str, ...],
        trade_date: date,
    ) -> dict[str, dict[str, Any]]:
        result = {symbol: {"is_suspended": False, "suspend_type": None, "suspend_timing": None} for symbol in requested}
        seen: set[str] = set()
        for row in rows:
            symbol = str(row[0] or "").strip()
            if symbol not in result or row[1] != trade_date or symbol in seen:
                raise DataUnavailableError(
                    "suspend_d batch identity is invalid",
                    context={"reason_code": "DAILY_TRADING_CONTEXT_SUSPEND_D_INVALID", "symbol": symbol},
                )
            seen.add(symbol)
            result[symbol] = {
                "is_suspended": True,
                "suspend_type": str(row[2] or "S"),
                "suspend_timing": str(row[3]) if row[3] is not None else None,
            }
        return result

    @staticmethod
    def _validate_st_rows(
        *,
        rows: list[Any],
        requested: tuple[str, ...],
        trade_date: date,
    ) -> tuple[dict[str, dict[str, Any]], str]:
        if len(rows) != len(requested):
            raise DataUnavailableError(
                "stock_st batch does not exactly cover the plan symbol set",
                context={"reason_code": "DAILY_TRADING_CONTEXT_STOCK_ST_INVALID"},
            )
        result: dict[str, dict[str, Any]] = {}
        source_versions: set[str] = set()
        for row in rows:
            symbol = str(row[0] or "").strip()
            if symbol not in requested or symbol in result or row[4] is None:
                raise DataUnavailableError(
                    "stock_st PIT batch identity is invalid",
                    context={"reason_code": "DAILY_TRADING_CONTEXT_STOCK_ST_INVALID", "symbol": symbol},
                )
            latest = row[4].isoformat()
            source = f"market.stock_st.latest_ann_date:{latest}"
            payload = {
                "symbol": symbol,
                "trade_date": trade_date.isoformat(),
                "is_st": row[1] is True,
                "start_date": row[2].isoformat() if isinstance(row[2], date) else None,
                "end_date": row[3].isoformat() if isinstance(row[3], date) else None,
                "source": source,
            }
            result[symbol] = {
                "is_st": payload["is_st"],
                "source": source,
                "evidence_hash": _canonical_json_sha256(payload),
            }
            source_versions.add(source)
        if len(source_versions) != 1:
            raise DataUnavailableError(
                "stock_st PIT batch has conflicting source versions",
                context={"reason_code": "DAILY_TRADING_CONTEXT_STOCK_ST_INVALID"},
            )
        return result, next(iter(source_versions))


class PreTradeTradabilityProvider:
    """Combine suspend_d and realtime quote evidence before order creation.

    The provider is intentionally read-only. If a realtime quote fetcher is
    configured and fails, callers get DataUnavailableError instead of silently
    falling back to stale close prices.
    """

    def __init__(
        self,
        *,
        suspend_status_provider: SuspendStatusProvider | None = None,
        realtime_quote_fetcher: RealtimeQuoteFetcher | None = None,
        realtime_quote_source: str | None = None,
        st_status_provider: StStatusProvider | None = None,
        require_realtime_quote: bool = False,
    ) -> None:
        self.suspend_status_provider = suspend_status_provider or DbSuspendStatusProvider()
        self.realtime_quote_fetcher = realtime_quote_fetcher
        self.realtime_quote_source = realtime_quote_source or "not_configured"
        self.st_status_provider = st_status_provider or DbStStatusProvider()
        self.require_realtime_quote = bool(require_realtime_quote)

    def get_statuses(
        self,
        symbols: list[str],
        trade_date: date,
        *,
        require_realtime_quote: bool | None = None,
        as_of_time: datetime | None = None,
        side_by_symbol: dict[str, Any] | None = None,
        frozen_daily_statuses: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> dict[str, dict[str, Any]]:
        normalized_symbols = _normalize_symbol_list(symbols)
        if not normalized_symbols:
            return {}
        require_quote = self.require_realtime_quote if require_realtime_quote is None else bool(require_realtime_quote)
        effective_as_of_time = as_of_time or datetime.now()
        normalized_sides = _normalize_side_by_symbol(side_by_symbol, normalized_symbols)
        quotes: dict[str, dict[str, Any]] = {}
        if require_quote:
            if self.realtime_quote_fetcher is None:
                raise DataUnavailableError(
                    "pre-trade realtime quote fetcher is required",
                    context={
                        "reason_code": "REALTIME_QUOTE_FETCHER_MISSING",
                        "trade_date": trade_date.isoformat(),
                        "symbols": normalized_symbols,
                        "quote_source": self.realtime_quote_source,
                    },
                )
            try:
                quotes = self.realtime_quote_fetcher(normalized_symbols)
            except DataUnavailableError:
                raise
            except Exception as exc:
                raise DataUnavailableError(
                    "pre-trade realtime quote fetch failed",
                    context={
                        "reason_code": "REALTIME_QUOTE_FETCH_FAILED",
                        "trade_date": trade_date.isoformat(),
                        "symbols": normalized_symbols,
                        "quote_source": self.realtime_quote_source,
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                    },
                ) from exc

        statuses: dict[str, dict[str, Any]] = {}
        for symbol in normalized_symbols:
            frozen_status = frozen_daily_statuses.get(symbol) if isinstance(frozen_daily_statuses, Mapping) else None
            daily_reference = frozen_status.get("daily_trading_context") if isinstance(frozen_status, Mapping) else None
            raw_frozen_fact = daily_reference.get("symbol_fact") if isinstance(daily_reference, Mapping) else None
            if frozen_daily_statuses is not None and not isinstance(raw_frozen_fact, Mapping):
                raise DataUnavailableError(
                    "pre-trade quote is missing its frozen daily trading fact",
                    context={
                        "reason_code": "DAILY_TRADING_CONTEXT_QUOTE_FACT_MISSING",
                        "symbol": symbol,
                        "trade_date": trade_date.isoformat(),
                    },
                )
            if isinstance(raw_frozen_fact, Mapping):
                try:
                    from backend.services.simulation_data.daily_context import (
                        DailyTradingAuthorityStateV2,
                        DailyTradingSymbolFactV1,
                        DailyTradingSymbolFactV2,
                    )

                    frozen_fact = (
                        DailyTradingSymbolFactV2.model_validate(dict(raw_frozen_fact))
                        if "authority_state" in raw_frozen_fact
                        else DailyTradingSymbolFactV1.model_validate(dict(raw_frozen_fact))
                    )
                except Exception as exc:
                    raise DataUnavailableError(
                        "pre-trade frozen daily trading fact is invalid",
                        context={"reason_code": "DAILY_TRADING_CONTEXT_QUOTE_FACT_INVALID", "symbol": symbol},
                    ) from exc
                if (
                    isinstance(frozen_fact, DailyTradingSymbolFactV2)
                    and frozen_fact.authority_state is DailyTradingAuthorityStateV2.SYMBOL_FAILED
                ):
                    status_payload = PreTradeTradabilityStatus(
                        symbol=symbol,
                        trade_date=trade_date,
                        is_tradable=False,
                        reason_code=str(frozen_fact.authority_reason_code),
                        source="daily_trading_context_v2",
                        suspend_status={
                            "is_suspended": frozen_fact.is_suspended,
                            "suspend_type": frozen_fact.suspend_type,
                            "suspend_timing": frozen_fact.suspend_timing,
                            "source": frozen_fact.suspend_source,
                        },
                    ).to_payload()
                    statuses[symbol] = self._attach_daily_context_reference(status_payload, daily_reference)
                    continue
                suspend = DailySuspendStatus(
                    symbol=symbol,
                    trade_date=trade_date,
                    is_suspended=frozen_fact.is_suspended,
                    suspend_type=frozen_fact.suspend_type,
                    suspend_timing=frozen_fact.suspend_timing,
                    source=frozen_fact.suspend_source,
                )
            else:
                suspend = self.suspend_status_provider.get_suspend_status(symbol, trade_date)
            suspend_payload = {
                "is_suspended": bool(suspend.is_suspended),
                "suspend_type": suspend.suspend_type,
                "suspend_timing": suspend.suspend_timing,
                "source": suspend.source,
            }
            if suspend.is_suspended:
                status_payload = PreTradeTradabilityStatus(
                    symbol=symbol,
                    trade_date=trade_date,
                    is_tradable=False,
                    reason_code="SUSPENDED_BY_SUSPEND_D",
                    source="market.suspend_d",
                    suspend_status=suspend_payload,
                ).to_payload()
                statuses[symbol] = self._attach_daily_context_reference(status_payload, daily_reference)
                continue

            quote_payload = None
            if require_quote:
                quote = quotes.get(symbol)
                if not isinstance(quote, dict):
                    status_payload = PreTradeTradabilityStatus(
                        symbol=symbol,
                        trade_date=trade_date,
                        is_tradable=False,
                        reason_code="REALTIME_QUOTE_MISSING",
                        source=self.realtime_quote_source,
                        suspend_status=suspend_payload,
                        quote_evidence={"quote_source": self.realtime_quote_source, "quote_present": False},
                    ).to_payload()
                    statuses[symbol] = self._attach_daily_context_reference(status_payload, daily_reference)
                    continue
                quote_payload = quote_tradability_evidence(
                    symbol=symbol,
                    quote=quote,
                    source=self.realtime_quote_source,
                    trade_date=trade_date,
                    as_of_time=effective_as_of_time,
                    st_status_provider=self.st_status_provider,
                    frozen_daily_fact=raw_frozen_fact,
                    side=normalized_sides.get(symbol),
                )
                if quote_payload["no_tradable_market"]:
                    status_payload = PreTradeTradabilityStatus(
                        symbol=symbol,
                        trade_date=trade_date,
                        is_tradable=False,
                        reason_code="NO_TRADABLE_REALTIME_QUOTE",
                        source=self.realtime_quote_source,
                        suspend_status=suspend_payload,
                        quote_evidence=quote_payload,
                    ).to_payload()
                    statuses[symbol] = self._attach_daily_context_reference(status_payload, daily_reference)
                    continue
                # A limit state blocks one order side, not the security itself.
                # When planning has not derived BUY/SELL yet, preserve the
                # blocked_sides evidence and let TradingRuleService apply it
                # after the target delta determines the actual side.
                blocked_reason_code = quote_payload.get("side_block_reason_code")
                if blocked_reason_code:
                    status_payload = PreTradeTradabilityStatus(
                        symbol=symbol,
                        trade_date=trade_date,
                        is_tradable=False,
                        reason_code=str(blocked_reason_code),
                        source=self.realtime_quote_source,
                        suspend_status=suspend_payload,
                        quote_evidence=quote_payload,
                    ).to_payload()
                    statuses[symbol] = self._attach_daily_context_reference(status_payload, daily_reference)
                    continue

            status_payload = PreTradeTradabilityStatus(
                symbol=symbol,
                trade_date=trade_date,
                is_tradable=True,
                reason_code="OK",
                source=self.realtime_quote_source if require_quote else "market.suspend_d",
                suspend_status=suspend_payload,
                quote_evidence=quote_payload,
            ).to_payload()
            statuses[symbol] = self._attach_daily_context_reference(status_payload, daily_reference)
        return statuses

    @staticmethod
    def _attach_daily_context_reference(
        status_payload: dict[str, Any],
        daily_reference: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        if isinstance(daily_reference, Mapping):
            status_payload["daily_trading_context"] = deepcopy(dict(daily_reference))
        return status_payload
