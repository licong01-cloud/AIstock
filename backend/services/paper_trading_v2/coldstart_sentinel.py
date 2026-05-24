"""Cold-start sanity sentinel for Paper Trading v2.

This module is intentionally narrow: it records one LocalSim-only sentinel
order/fill chain for the production go/no-go sanity script without starting a
scheduler, calling a broker, or activating live trading.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import subprocess
from contextlib import suppress
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from typing import Any, Callable, Iterator, Mapping
from zoneinfo import ZoneInfo

import psycopg2.extras

from backend.db.pg_pool import get_conn
from backend.services.paper_trading_v2.market_data import MinuteDataSource
from backend.services.qe_archive.models import canonical_json_dumps, normalize_json, sha256_json
from backend.services.trading_calendar_status import TradingCalendarStatusService
from backend.services.trading_core.errors import (
    InvalidStateTransitionError,
    StrategyPackageValidationError,
    TradingCoreError,
)
from backend.services.trading_core.models import Fill, OrderIntent, OrderSide, OrderType
from backend.services.trading_core.oms import OMS


SENTINEL_SYMBOL = "000001.SZ"
SENTINEL_SIDE = "BUY"
SENTINEL_QUANTITY = 100
SENTINEL_INTENDED_PRICE = Decimal("10.00")
SENTINEL_SOURCE = "paper_v2_coldstart_sanity"
LOCAL_SIM_BACKEND = "local_sim"
PAPER_ENABLED_STATUSES = ("PAPER_ENABLED", "PAPER_RUNNING", "PAPER_PASSED")
SENTINEL_PORTFOLIO_PREFIX = "paper_v2_coldstart_sanity_"
SENTINEL_TZ = ZoneInfo("Asia/Shanghai")

ConnFactory = Callable[[], Iterator[Any]]
DaemonChecker = Callable[[str], bool]
NowFactory = Callable[[], dt.datetime]


class PaperV2DaemonUnavailableError(TradingCoreError):
    error_code = "PAPER_V2_DAEMON_UNAVAILABLE"


class ColdstartSentinelService:
    def __init__(
        self,
        *,
        conn_factory: ConnFactory | None = None,
        daemon_checker: DaemonChecker | None = None,
        now_factory: NowFactory | None = None,
        daemon_process_name: str = "paper_v2",
    ) -> None:
        self._conn_factory = conn_factory or get_conn
        self._daemon_checker = daemon_checker or _is_daemon_running
        self._now_factory = now_factory or _now_cst
        self._daemon_process_name = daemon_process_name
        self._oms = OMS()

    def record_sentinel_order(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        req = _validate_payload(payload)
        now = self._now_factory().astimezone(SENTINEL_TZ)
        if _is_a_share_trading_window(now):
            raise InvalidStateTransitionError(
                "paper v2 coldstart sentinel is blocked during A-share trading hours",
                context={
                    "as_of_time": now.isoformat(),
                    "timezone": "Asia/Shanghai",
                    "blocked_windows": ["09:30-11:30", "13:00-15:00"],
                },
            )
        if not self._daemon_checker(self._daemon_process_name):
            raise PaperV2DaemonUnavailableError(
                "paper v2 daemon process is not running",
                context={"process_name": self._daemon_process_name},
            )

        with self._conn_factory() as conn:
            try:
                package = self._select_enabled_package(conn, req["package_id"])
                self._require_capture_fields(conn)
                result = self._record_rows(conn, req=req, package=package, now=now)
                _commit(conn)
                return result
            except Exception:
                _rollback(conn)
                raise

    def _select_enabled_package(self, conn: Any, package_id: str) -> dict[str, Any]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT package_id, package_name, package_version, package_status,
                       manifest_sha256, manifest_json
                FROM strategy_pkg.package
                WHERE package_id = %s
                LIMIT 1
                """,
                (package_id,),
            )
            row = cur.fetchone()
        if not row:
            raise InvalidStateTransitionError(
                "paper v2 coldstart sentinel package was not found",
                context={"package_id": package_id},
            )
        package = dict(row)
        if package["package_status"] not in PAPER_ENABLED_STATUSES:
            raise InvalidStateTransitionError(
                "paper v2 coldstart sentinel requires an enable_paper StrategyPackage",
                context={
                    "package_id": package_id,
                    "package_status": package["package_status"],
                    "allowed_statuses": list(PAPER_ENABLED_STATUSES),
                },
            )
        return package

    def _require_capture_fields(self, conn: Any) -> None:
        required = {
            "created_at",
            "updated_at",
            "intended_price",
            "fill_market_context",
        }
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'paper_v2'
                  AND table_name = 'fills'
                  AND column_name = ANY(%s)
                """,
                (list(required),),
            )
            rows = cur.fetchall() or []
        present = {_row_value(row, "column_name", 0) for row in rows}
        missing = sorted(required - present)
        if missing:
            raise InvalidStateTransitionError(
                "paper v2 coldstart sentinel requires capture-field DDL on paper_v2.fills",
                context={
                    "table": "paper_v2.fills",
                    "missing_columns": missing,
                    "required_columns": sorted(required),
                    "ddl_file": "backend/db/add_paper_v2_capture_fields_20260510.sql",
                },
            )

    def _record_rows(
        self,
        conn: Any,
        *,
        req: dict[str, Any],
        package: dict[str, Any],
        now: dt.datetime,
    ) -> dict[str, Any]:
        run_id = req["run_id"]
        portfolio_id = sentinel_portfolio_id(run_id)
        order_id = f"ord_{_stable_suffix(run_id)}"
        fill_id = f"fill_{_stable_suffix(run_id)}"
        order_event_id = f"evt_{_stable_suffix(run_id)}"
        outbox_event_id = f"qear_evt_{sha256_json({'event_type': 'paper_v2.coldstart_sentinel', 'source_system': 'paper_v2', 'source_id': run_id})[:24]}"
        validation_run_id = f"vr_coldstart_{_stable_suffix(run_id, length=20)}"
        asset_ref = f"governance/coldstart_sanity/{run_id}/sentinel.json"
        trade_date = now.date()
        manifest_json = _coerce_json_map(package.get("manifest_json"))
        market_context = _fill_market_context(req=req, now=now)

        intent = OrderIntent(
            intent_id=f"intent_{_stable_suffix(run_id)}",
            package_id=str(package["package_id"]),
            portfolio_id=portfolio_id,
            symbol=req["symbol"],
            side=OrderSide(req["side"]),
            quantity=req["quantity"],
            order_type=OrderType.LIMIT,
            limit_price=float(req["intended_price"]),
            target_trade_date=trade_date,
            metadata={
                "source": SENTINEL_SOURCE,
                "run_id": run_id,
                "broker_backend": LOCAL_SIM_BACKEND,
                "local_sim_only": True,
            },
        )
        submitted_order = self._oms.create_order(intent).model_copy(
            update={"order_id": order_id, "created_at": now, "updated_at": now}
        )
        fill = Fill(
            fill_id=fill_id,
            order_id=order_id,
            symbol=req["symbol"],
            side=OrderSide(req["side"]),
            quantity=req["quantity"],
            price=float(req["intended_price"]),
            trade_time=now,
            bar_time=now,
            reason="paper_v2_coldstart_sanity_local_sim",
            metadata={
                "source": SENTINEL_SOURCE,
                "run_id": run_id,
                "broker_backend": LOCAL_SIM_BACKEND,
                "local_sim_only": True,
                "intended_price": str(req["intended_price"]),
                "fill_market_context": market_context,
                "slippage_bps": "0",
            },
        )
        final_order, order_event = self._oms.apply_fill(submitted_order, fill)
        final_order = final_order.model_copy(update={"updated_at": now})
        order_event = order_event.model_copy(update={"event_id": order_event_id, "event_time": now})

        evidence_payload = {
            "run_id": run_id,
            "package_id": str(package["package_id"]),
            "portfolio_id": portfolio_id,
            "order_id": order_id,
            "fill_id": fill_id,
            "source": SENTINEL_SOURCE,
            "broker_backend": LOCAL_SIM_BACKEND,
            "local_sim_only": True,
            "sentinel_order": {
                "symbol": req["symbol"],
                "side": req["side"],
                "quantity": req["quantity"],
                "intended_price": str(req["intended_price"]),
            },
        }
        artifact_payload = {"coldstart_sanity": evidence_payload, "routing_class": "telemetry"}
        outbox_payload = {
            **evidence_payload,
            "routing_class": "telemetry",
            "event_name": "paper_v2_coldstart_sentinel_order",
        }

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO paper_v2.portfolio (
                    portfolio_id, portfolio_name, package_id, manifest_sha256,
                    frozen_manifest_json, initial_cash, start_date, data_source,
                    fee_policy, risk_policy, execution_policy, status, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (portfolio_id) DO UPDATE SET
                    updated_at = EXCLUDED.updated_at,
                    status = EXCLUDED.status
                """,
                (
                    portfolio_id,
                    f"Coldstart sanity {run_id}",
                    package["package_id"],
                    package["manifest_sha256"],
                    _jsonb(manifest_json),
                    Decimal("100000.00"),
                    trade_date,
                    MinuteDataSource.DB_HISTORICAL.value,
                    _jsonb({"commission_rate": 0, "min_commission": 0}),
                    _jsonb({"sentinel_only": True}),
                    _jsonb({"broker_backend": LOCAL_SIM_BACKEND, "local_sim_only": True}),
                    "COMPLETED",
                    now,
                    now,
                ),
            )
            cur.execute(
                """
                INSERT INTO paper_v2.run (
                    run_id, portfolio_id, trade_date, status, data_source,
                    runtime_config, started_at, completed_at, error_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NULL)
                ON CONFLICT (run_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    runtime_config = EXCLUDED.runtime_config,
                    completed_at = EXCLUDED.completed_at
                """,
                (
                    run_id,
                    portfolio_id,
                    trade_date,
                    "SUCCEEDED",
                    MinuteDataSource.DB_HISTORICAL.value,
                    _jsonb({"source": SENTINEL_SOURCE, "broker_backend": LOCAL_SIM_BACKEND}),
                    now,
                    now,
                ),
            )
            cur.execute(
                """
                INSERT INTO paper_v2.orders (
                    order_id, run_id, portfolio_id, package_id, intent_id, symbol,
                    side, quantity, order_type, limit_price, status, filled_quantity,
                    avg_fill_price, metadata, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (order_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    filled_quantity = EXCLUDED.filled_quantity,
                    avg_fill_price = EXCLUDED.avg_fill_price,
                    metadata = EXCLUDED.metadata,
                    updated_at = EXCLUDED.updated_at
                """,
                (
                    final_order.order_id,
                    run_id,
                    final_order.portfolio_id,
                    final_order.package_id,
                    final_order.intent_id,
                    final_order.symbol,
                    final_order.side.value,
                    final_order.quantity,
                    final_order.order_type.value,
                    final_order.limit_price,
                    final_order.status.value,
                    final_order.filled_quantity,
                    final_order.avg_fill_price,
                    _jsonb(final_order.metadata),
                    final_order.created_at,
                    final_order.updated_at,
                ),
            )
            cur.execute(
                """
                INSERT INTO paper_v2.fills (
                    fill_id, run_id, order_id, symbol, side, quantity, price,
                    trade_time, bar_time, reason, metadata, created_at, updated_at,
                    intended_price, fill_market_context
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (fill_id) DO UPDATE SET
                    metadata = EXCLUDED.metadata,
                    updated_at = EXCLUDED.updated_at,
                    intended_price = EXCLUDED.intended_price,
                    fill_market_context = EXCLUDED.fill_market_context
                """,
                (
                    fill.fill_id,
                    run_id,
                    fill.order_id,
                    fill.symbol,
                    fill.side.value,
                    fill.quantity,
                    fill.price,
                    fill.trade_time,
                    fill.bar_time,
                    fill.reason,
                    _jsonb(fill.metadata),
                    now,
                    now,
                    req["intended_price"],
                    _jsonb(market_context),
                ),
            )
            cur.execute(
                """
                INSERT INTO paper_v2.order_events (
                    event_id, run_id, order_id, event_type, event_time, reason, metadata, fill_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING
                """,
                (
                    order_event.event_id,
                    run_id,
                    order_event.order_id,
                    order_event.event_type.value,
                    order_event.event_time,
                    order_event.reason,
                    _jsonb(order_event.metadata),
                    _jsonb(order_event.fill.model_dump(mode="json") if order_event.fill else None),
                ),
            )
            cur.execute(
                """
                INSERT INTO paper_v2.run_events (run_id, event_type, message, context)
                VALUES (%s, %s, %s, %s)
                """,
                (run_id, "COLDSTART_SENTINEL_ACCEPTED", "paper v2 coldstart sentinel accepted", _jsonb(evidence_payload)),
            )
            cur.execute(
                """
                INSERT INTO qe_archive.outbox_event (
                    event_id, event_type, source_system, source_id, source_sub_id, payload, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO UPDATE SET
                    payload = EXCLUDED.payload,
                    status = EXCLUDED.status,
                    updated_at = NOW()
                """,
                (
                    outbox_event_id,
                    "paper_v2.coldstart_sentinel",
                    "paper_v2",
                    run_id,
                    fill_id,
                    _jsonb(outbox_payload),
                    "pending",
                ),
            )
            cur.execute(
                """
                INSERT INTO strategy_pkg.package_validation_run (
                    validation_run_id, package_id, manifest_sha256, validation_type,
                    retrain_mode, status, metrics_json, artifact_manifest_json,
                    evidence_json, reproducibility_level, created_by, created_at, completed_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (validation_run_id) DO UPDATE SET
                    metrics_json = EXCLUDED.metrics_json,
                    artifact_manifest_json = EXCLUDED.artifact_manifest_json,
                    evidence_json = EXCLUDED.evidence_json,
                    completed_at = EXCLUDED.completed_at
                """,
                (
                    validation_run_id,
                    package["package_id"],
                    package["manifest_sha256"],
                    "original_fixed_weight",
                    "no_retrain",
                    "PASSED",
                    _jsonb({"sentinel_fill_count": 1, "local_sim_only": True}),
                    _jsonb(artifact_payload),
                    _jsonb(evidence_payload),
                    "NOT_APPLICABLE",
                    SENTINEL_SOURCE,
                    now,
                    now,
                ),
            )
            cur.execute(
                """
                INSERT INTO strategy_pkg.package_asset (
                    package_id, asset_type, asset_ref, asset_sha256, metadata,
                    created_at, asset_role, asset_size_bytes, protected_asset, source_uri
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (package_id, asset_type, asset_ref) DO UPDATE SET
                    asset_sha256 = EXCLUDED.asset_sha256,
                    metadata = EXCLUDED.metadata,
                    protected_asset = EXCLUDED.protected_asset
                """,
                (
                    package["package_id"],
                    "protected_asset_ledger_evidence",
                    asset_ref,
                    f"sha256:{sha256(canonical_json_dumps(artifact_payload).encode('utf-8')).hexdigest()}",
                    _jsonb({**evidence_payload, "validation_run_id": validation_run_id, "outbox_event_id": outbox_event_id}),
                    now,
                    "coldstart_sanity_evidence",
                    len(canonical_json_dumps(artifact_payload).encode("utf-8")),
                    True,
                    asset_ref,
                ),
            )

        return {
            "ok": True,
            "status": "accepted",
            "run_id": run_id,
            "package_id": str(package["package_id"]),
            "portfolio_id": portfolio_id,
            "order_id": order_id,
            "fill_id": fill_id,
            "symbol": req["symbol"],
            "side": req["side"],
            "quantity": req["quantity"],
            "intended_price": str(req["intended_price"]),
            "fill_market_context": market_context,
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
            "routing_class": "telemetry",
            "outbox_event_id": outbox_event_id,
            "validation_run_id": validation_run_id,
            "asset_ref": asset_ref,
            "broker_backend": LOCAL_SIM_BACKEND,
            "local_sim_only": True,
        }


def sentinel_portfolio_id(run_id: str) -> str:
    return f"{SENTINEL_PORTFOLIO_PREFIX}{_stable_suffix(run_id, length=24)}"


def _validate_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    run_id = str(payload.get("run_id") or "").strip()
    package_id = str(payload.get("package_id") or "").strip()
    symbol = str(payload.get("symbol") or "").strip().upper()
    side = str(payload.get("side") or "").strip().upper()
    quantity_raw = payload.get("quantity")
    if quantity_raw is None:
        quantity_raw = payload.get("qty")
    intended_price = _decimal(payload.get("intended_price"))
    source = str(payload.get("source") or SENTINEL_SOURCE).strip()
    broker_backend = str(payload.get("broker_backend") or LOCAL_SIM_BACKEND).strip().lower()

    failures: list[str] = []
    if not run_id.startswith("sanity-"):
        failures.append("run_id must start with sanity-")
    if not package_id:
        failures.append("package_id is required")
    if symbol != SENTINEL_SYMBOL:
        failures.append(f"symbol must be {SENTINEL_SYMBOL}")
    if side != SENTINEL_SIDE:
        failures.append(f"side must be {SENTINEL_SIDE}")
    try:
        quantity = int(quantity_raw)
    except (TypeError, ValueError):
        quantity = 0
    if quantity != SENTINEL_QUANTITY:
        failures.append(f"quantity must be {SENTINEL_QUANTITY}")
    if intended_price != SENTINEL_INTENDED_PRICE:
        failures.append(f"intended_price must be {SENTINEL_INTENDED_PRICE}")
    if source != SENTINEL_SOURCE:
        failures.append(f"source must be {SENTINEL_SOURCE}")
    if broker_backend != LOCAL_SIM_BACKEND:
        failures.append("broker_backend must be local_sim")
    if failures:
        raise StrategyPackageValidationError(
            "invalid paper v2 coldstart sentinel payload",
            context={"failures": failures},
        )
    return {
        "run_id": run_id,
        "package_id": package_id,
        "symbol": symbol,
        "side": side,
        "quantity": quantity,
        "intended_price": intended_price,
        "source": source,
        "broker_backend": broker_backend,
    }


def _decimal(value: Any) -> Decimal | None:
    try:
        return Decimal(str(value)).quantize(Decimal("0.01"))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _fill_market_context(*, req: dict[str, Any], now: dt.datetime) -> dict[str, Any]:
    price = str(req["intended_price"])
    return {
        "stock_id": req["symbol"],
        "symbol": req["symbol"],
        "broker_backend": LOCAL_SIM_BACKEND,
        "source": SENTINEL_SOURCE,
        "local_sim_only": True,
        "bar_time": now.isoformat(),
        "intended_price": price,
        "fill_price": price,
        "bid": price,
        "ask": price,
        "best_volume": req["quantity"],
        "spread": "0",
        "bar_open": price,
        "bar_high": price,
        "bar_low": price,
        "bar_close": price,
        "bar_vwap": price,
        "suspend_d": False,
        "limit_state": "normal",
        "limit_up": "11.00",
        "limit_down": "9.00",
    }


def _is_a_share_trading_window(now: dt.datetime) -> bool:
    local_dt = now.astimezone(SENTINEL_TZ)
    try:
        TradingCalendarStatusService().ensure_trading_day(local_dt.date())
    except TradingCoreError:
        return False
    local = local_dt.time()
    return dt.time(9, 30) <= local <= dt.time(11, 30) or dt.time(13, 0) <= local <= dt.time(15, 0)


def _now_cst() -> dt.datetime:
    return dt.datetime.now(SENTINEL_TZ)


def _is_daemon_running(process_name: str) -> bool:
    needle = process_name.lower().strip()
    if not needle:
        return True
    with suppress(Exception):
        import psutil  # type: ignore

        for proc in psutil.process_iter(["name", "cmdline"]):
            info = proc.info
            haystack = " ".join([str(info.get("name") or ""), *[str(part) for part in (info.get("cmdline") or [])]]).lower()
            if needle in haystack:
                return True
    with suppress(Exception):
        cmd = ["tasklist"] if os.name == "nt" else ["ps", "-eo", "pid,args"]
        output = subprocess.check_output(cmd, text=True, encoding="utf-8", errors="replace", stderr=subprocess.DEVNULL)
        return any(needle in line.lower() for line in output.splitlines())
    return False


def _stable_suffix(value: str, *, length: int = 28) -> str:
    return sha256(value.encode("utf-8")).hexdigest()[:length]


def _jsonb(value: Any) -> psycopg2.extras.Json:
    return psycopg2.extras.Json(normalize_json(value), dumps=canonical_json_dumps)


def _row_value(row: Any, key: str, index: int) -> Any:
    if isinstance(row, Mapping):
        return row.get(key)
    return row[index]


def _coerce_json_map(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        with suppress(json.JSONDecodeError):
            parsed = json.loads(value)
            if isinstance(parsed, Mapping):
                return dict(parsed)
    return {}


def _commit(conn: Any) -> None:
    commit = getattr(conn, "commit", None)
    if callable(commit):
        commit()


def _rollback(conn: Any) -> None:
    rollback = getattr(conn, "rollback", None)
    if callable(rollback):
        rollback()
