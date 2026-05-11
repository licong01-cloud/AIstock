"""Paper v2 cold-start sanity gate for the R6 production cutover.

Default dry-run mode is an offline preview: it emits the exact phases, guard
state, and sentinel payload without opening a DB connection or touching backend
services. Production mode is intentionally hard-gated and only then performs a
read-only preflight, one sentinel runtime round-trip, audit verification, and
scoped cleanup for the sentinel run.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable

try:  # Python 3.9+
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover - operator host fallback
    ZoneInfo = None  # type: ignore


SCHEMA_VERSION = "aistock_paper_v2_coldstart_sanity_v1"
CONFIRM_PROD = "RUN_PAPER_V2_COLDSTART_SANITY_PROD"
ENV_PROD_ENABLED = "AISTOCK_PAPER_V2_COLDSTART_SANITY_PROD_ENABLED"
ENV_MUTEX_HELD = "AISTOCK_PAPER_V2_COLDSTART_SANITY_MUTEX_HELD"
DEFAULT_API_BASE = "http://127.0.0.1:8001/api/v1"
DEFAULT_HEALTH_PATH = "/health"
DEFAULT_SENTINEL_ENDPOINT = "/paper-v2/coldstart-sanity/sentinel-order"
DEFAULT_DAEMON_PROCESS_NAME = "paper_v2"
SENTINEL_SYMBOL = "000001.SZ"
SENTINEL_SIDE = "BUY"
SENTINEL_QUANTITY = 100
SENTINEL_INTENDED_PRICE = "10.00"
PAPER_ENABLED_STATUSES = {"PAPER_ENABLED", "PAPER_RUNNING", "PAPER_PASSED"}
ALLOWED_OUTBOX_STATUSES = {"pending", "sent"}
REQUIRED_TABLES = (
    "strategy_pkg.package",
    "strategy_pkg.package_validation_run",
    "strategy_pkg.package_runtime_variant",
    "strategy_pkg.seed_fragility_score",
    "strategy_pkg.package_asset",
    "paper_v2.orders",
    "paper_v2.fills",
    "paper_v2.run_events",
    "qe_archive.outbox_event",
)
CLEANUP_TABLES = (
    "qe_archive.outbox_event",
    "strategy_pkg.package_validation_run",
    "strategy_pkg.package_asset",
    "paper_v2.fills",
    "paper_v2.order_events",
    "paper_v2.orders",
    "paper_v2.run_events",
)


class ColdStartSanityError(RuntimeError):
    """Raised when the cold-start sanity gate refuses to proceed."""


@dataclass(frozen=True)
class DbTarget:
    target_db: str
    host: str
    port: int
    dbname: str
    user: str
    password: str = ""

    @property
    def label(self) -> str:
        return f"{self.target_db}:{self.user}@{self.host}:{self.port}/{self.dbname}"

    def as_psycopg2_kwargs(self, *, readonly: bool) -> dict[str, Any]:
        readonly_option = " -c default_transaction_read_only=on" if readonly else ""
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
            "application_name": "AIstock-paper-v2-coldstart-sanity",
            "options": f"-c client_encoding=utf8{readonly_option}",
        }


@dataclass(frozen=True)
class SentinelOrder:
    run_id: str
    symbol: str = SENTINEL_SYMBOL
    side: str = SENTINEL_SIDE
    quantity: int = SENTINEL_QUANTITY
    intended_price: str = SENTINEL_INTENDED_PRICE

    def payload(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "symbol": self.symbol,
            "side": self.side,
            "quantity": self.quantity,
            "intended_price": self.intended_price,
            "source": "paper_v2_coldstart_sanity",
        }


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_local() -> datetime:
    if ZoneInfo is None:  # pragma: no cover - old Python fallback
        return datetime.now()
    return datetime.now(ZoneInfo("Asia/Shanghai"))


def _default_run_id(now: datetime | None = None) -> str:
    stamp = (now or _now_local()).strftime("%Y%m%d-%H%M%S")
    return f"sanity-{stamp}"


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ColdStartSanityError(message)


def _env_truthy(name: str) -> bool:
    value = (os.getenv(name) or "").strip().lower()
    return value in {"1", "true", "yes", "y", "on"}


def _target_from_args(args: argparse.Namespace) -> DbTarget:
    password = args.db_password or os.getenv(args.db_password_env, "")
    return DbTarget(
        target_db=args.target_db,
        host=args.db_host,
        port=args.db_port,
        dbname=args.db_name,
        user=args.db_user,
        password=password,
    )


def _connect(target: DbTarget, *, readonly: bool = True) -> Any:
    try:
        import psycopg2  # type: ignore
    except ImportError as exc:  # pragma: no cover - depends on operator host
        raise ColdStartSanityError("psycopg2 is required for production cold-start sanity") from exc
    try:
        conn = psycopg2.connect(**target.as_psycopg2_kwargs(readonly=readonly))
        set_session = getattr(conn, "set_session", None)
        if callable(set_session):
            set_session(readonly=readonly, autocommit=False)
        return conn
    except Exception as exc:  # pragma: no cover - depends on operator host
        raise ColdStartSanityError(f"failed to connect to DB target {target.label}: {exc}") from exc


def _join_url(api_base: str, path: str) -> str:
    return api_base.rstrip("/") + "/" + path.lstrip("/")


def _http_json(method: str, api_base: str, path: str, *, payload: dict[str, Any] | None, timeout: float) -> dict[str, Any]:
    data = None if payload is None else json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        _join_url(api_base, path),
        data=data,
        method=method,
        headers={"Accept": "application/json", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
            parsed = json.loads(raw.decode("utf-8")) if raw else {}
            _require(200 <= response.status < 300, f"{method} {path} returned HTTP {response.status}")
            return parsed if isinstance(parsed, dict) else {"payload": parsed}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise ColdStartSanityError(f"{method} {path} returned HTTP {exc.code}: {body[:500]}") from exc
    except urllib.error.URLError as exc:
        raise ColdStartSanityError(f"{method} {path} connection failed: {exc.reason}") from exc
    except TimeoutError as exc:
        raise ColdStartSanityError(f"{method} {path} timed out") from exc


def _http_get_json(api_base: str, path: str, *, timeout: float) -> dict[str, Any]:
    return _http_json("GET", api_base, path, payload=None, timeout=timeout)


def _http_post_json(api_base: str, path: str, payload: dict[str, Any], *, timeout: float) -> dict[str, Any]:
    return _http_json("POST", api_base, path, payload=payload, timeout=timeout)


def _find_daemon_process(process_name: str) -> dict[str, Any] | None:
    needle = process_name.lower().strip()
    if not needle:
        return None
    psutil_error: str | None = None
    try:
        import psutil  # type: ignore

        for proc in psutil.process_iter(["pid", "name", "cmdline"]):
            info = proc.info
            haystack = " ".join([str(info.get("name") or ""), *[str(part) for part in (info.get("cmdline") or [])]]).lower()
            if needle in haystack:
                return {"pid": info.get("pid"), "name": info.get("name"), "cmdline": info.get("cmdline") or []}
    except (ImportError, OSError, RuntimeError) as exc:
        psutil_error = str(exc)
    try:
        cmd = ["tasklist"] if os.name == "nt" else ["ps", "-eo", "pid,args"]
        output = subprocess.check_output(cmd, text=True, encoding="utf-8", errors="replace", stderr=subprocess.DEVNULL)
        for line in output.splitlines():
            if needle in line.lower():
                return {"pid": None, "name": process_name, "cmdline": [line.strip()]}
    except (OSError, subprocess.SubprocessError):
        if psutil_error:
            return {"pid": None, "name": process_name, "cmdline": [], "probe_error": psutil_error, "running": False}
    return None


def _is_a_share_trading_window(now: datetime) -> bool:
    if now.weekday() >= 5:
        return False
    local_time = now.time()
    # Conservative final-gate rule: reject the whole 09:30-15:00 weekday span,
    # including lunch break, to avoid running sentinel cleanup near live trading.
    return dt_time(9, 30) <= local_time <= dt_time(15, 0)


def _require_prod_guards(args: argparse.Namespace, target: DbTarget, *, now: datetime) -> None:
    _require(args.mode == "prod", "internal error: prod guards called outside --mode=prod")
    _require(args.confirm_prod == CONFIRM_PROD, f"--mode=prod requires exact --confirm-prod {CONFIRM_PROD}")
    _require(_env_truthy(ENV_PROD_ENABLED), f"--mode=prod requires {ENV_PROD_ENABLED}=true")
    _require(_env_truthy(ENV_MUTEX_HELD), f"--mode=prod requires mutex guard {ENV_MUTEX_HELD}=true")
    _require(not _is_a_share_trading_window(now), "cold-start sanity refuses A-share trading hours 09:30-15:00 CST on weekdays")
    confirmation = str(args.operator_confirmation or "").strip()
    _require(confirmation, "operator confirmation is required for production cold-start sanity")
    _require(CONFIRM_PROD in confirmation, "operator confirmation must include the exact production token")
    _require(target.target_db == "prod", "production sanity requires --target-db prod")
    _require(target.port == 5432, "production sanity requires DB port 5432")
    _require(target.dbname not in {"aistock_dev", "dev", "test"}, "production sanity refuses dev/test DB names")
    _require(target.host not in {"", "127.0.0.1-dev"}, "production sanity requires an explicit DB host")
    _require(target.label in confirmation or target.dbname in confirmation or "target=prod" in confirmation, "operator confirmation must include the target DB label, DB name, or target=prod")


def _row_get(row: Any, key: str, index: int, default: Any = None) -> Any:
    if row is None:
        return default
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        return row[index]
    except (IndexError, TypeError):
        return default


def _coerce_json(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _as_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _phase(check: str, status: str, message: str, data: dict[str, Any] | None = None) -> dict[str, Any]:
    return {"check": check, "status": status, "message": message, "data": data or {}}


def _execute_scalar(cur: Any, sql: str, params: object | None = None) -> Any:
    cur.execute(sql, params)
    row = cur.fetchone()
    if row is None:
        return None
    if isinstance(row, dict):
        return next(iter(row.values()), None)
    return row[0] if row else None


def _preflight_db_checks(conn: Any, package_ids: list[str]) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    with conn.cursor() as cur:
        try:
            cur.execute("SELECT 1")
            checks.append(_phase("db_readonly_ping", "PASS", "DB read-only ping succeeded"))
        except Exception as exc:
            checks.append(_phase("db_readonly_ping", "FAIL", f"DB read-only ping failed: {exc}"))
            return checks

        missing_tables: list[str] = []
        for table in REQUIRED_TABLES:
            try:
                exists = _execute_scalar(cur, "SELECT to_regclass(%s)", (table,))
            except Exception as exc:
                checks.append(_phase("required_tables", "FAIL", f"required table check failed for {table}: {exc}"))
                return checks
            if not exists:
                missing_tables.append(table)
        checks.append(_phase(
            "required_tables",
            "PASS" if not missing_tables else "FAIL",
            "required governance/Paper v2 tables exist" if not missing_tables else f"missing required table(s): {', '.join(missing_tables)}",
            {"missing_tables": missing_tables},
        ))

        if not package_ids:
            checks.append(_phase("package_ids", "FAIL", "production sanity requires approved package ids"))
            return checks

        cur.execute(
            """
            SELECT
                p.package_id,
                p.package_status,
                EXISTS (
                    SELECT 1 FROM strategy_pkg.package_validation_run vr
                    WHERE vr.package_id = p.package_id
                      AND UPPER(vr.status) IN ('PASSED', 'VALIDATION_PASSED')
                ) AS validation_evidence,
                EXISTS (
                    SELECT 1 FROM strategy_pkg.package_runtime_variant rv
                    WHERE rv.package_id = p.package_id
                      AND rv.validation_status = 'VALIDATION_PASSED'
                      AND rv.paper_candidate = TRUE
                ) AS runtime_variant,
                EXISTS (
                    SELECT 1 FROM strategy_pkg.seed_fragility_score sf
                    WHERE sf.package_id = p.package_id
                      AND COALESCE(sf.seed_fragile, FALSE) = FALSE
                ) AS stability_evidence,
                EXISTS (
                    SELECT 1 FROM strategy_pkg.package_asset pa
                    WHERE pa.package_id = p.package_id
                      AND (
                          pa.asset_type = 'protected_asset_ledger_evidence'
                          OR pa.asset_ref = 'governance/protected_asset_ledger_backfill'
                          OR pa.metadata::text LIKE '%protected_asset_ledger%'
                      )
                      AND COALESCE(pa.protected_asset, TRUE) = TRUE
                ) AS protected_asset_ledger
            FROM strategy_pkg.package p
            WHERE p.package_id = ANY(%s)
            ORDER BY p.package_id
            """,
            (package_ids,),
        )
        rows = list(cur.fetchall() or [])
        found = {_row_get(row, "package_id", 0) for row in rows}
        missing_packages = [pkg for pkg in package_ids if pkg not in found]
        package_failures: list[str] = []
        for row in rows:
            package_id = _row_get(row, "package_id", 0)
            status = _row_get(row, "package_status", 1)
            gates = {
                "validation_evidence": bool(_row_get(row, "validation_evidence", 2)),
                "runtime_variant": bool(_row_get(row, "runtime_variant", 3)),
                "stability_evidence": bool(_row_get(row, "stability_evidence", 4)),
                "protected_asset_ledger": bool(_row_get(row, "protected_asset_ledger", 5)),
                "paper_enabled": status in PAPER_ENABLED_STATUSES,
            }
            failed = [name for name, ok in gates.items() if not ok]
            if failed:
                package_failures.append(f"{package_id}:{','.join(failed)}")
        if missing_packages:
            package_failures.extend(f"{pkg}:missing_package" for pkg in missing_packages)
        checks.append(_phase(
            "governance_evidence_and_enable_paper",
            "PASS" if not package_failures else "FAIL",
            "governance evidence, protected ledger, and enable_paper gate are ready" if not package_failures else "governance/Paper gate failures detected",
            {"package_failures": package_failures, "checked_packages": package_ids},
        ))
    return checks


def _check_backend_health(args: argparse.Namespace) -> dict[str, Any]:
    payload = _http_get_json(args.api_base, args.health_path, timeout=args.http_timeout)
    status = str(payload.get("status") or payload.get("ok") or "").lower()
    ok = status in {"ok", "true", "1"} or payload.get("ok") is True
    return _phase("backend_health", "PASS" if ok else "FAIL", "backend health endpoint returned OK" if ok else "backend health endpoint did not return OK", {"payload": payload})


def _check_daemon(args: argparse.Namespace) -> dict[str, Any]:
    proc = _find_daemon_process(args.daemon_process_name)
    return _phase(
        "paper_v2_daemon_process",
        "PASS" if proc else "FAIL",
        "paper-v2 daemon process is running" if proc else f"paper-v2 daemon process not found: {args.daemon_process_name}",
        {"process": proc, "process_name": args.daemon_process_name},
    )


def _trigger_sentinel_order(args: argparse.Namespace, sentinel: SentinelOrder) -> dict[str, Any]:
    payload = sentinel.payload()
    result = _http_post_json(args.api_base, args.sentinel_endpoint, payload, timeout=args.http_timeout)
    accepted = result.get("ok") is True or str(result.get("status") or "").lower() in {"accepted", "ok", "submitted"}
    return _phase(
        "sentinel_order_trigger",
        "PASS" if accepted else "FAIL",
        "sentinel order accepted by Paper v2 runtime" if accepted else "sentinel order trigger did not report accepted/submitted",
        {"request": payload, "response": result, "endpoint": args.sentinel_endpoint},
    )


def _poll_fill(conn: Any, sentinel: SentinelOrder, *, timeout_seconds: int, poll_seconds: float) -> tuple[dict[str, Any], dict[str, Any] | None]:
    deadline = time.monotonic() + timeout_seconds
    latest: dict[str, Any] | None = None
    while time.monotonic() <= deadline:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT fill_id, run_id, order_id, symbol, side, quantity, price,
                       intended_price, fill_market_context, created_at, updated_at, trade_time
                FROM paper_v2.fills
                WHERE run_id = %s AND symbol = %s AND side = %s
                ORDER BY created_at DESC NULLS LAST, trade_time DESC NULLS LAST
                LIMIT 1
                """,
                (sentinel.run_id, sentinel.symbol, sentinel.side),
            )
            row = cur.fetchone()
        if row:
            latest = {
                "fill_id": _row_get(row, "fill_id", 0),
                "run_id": _row_get(row, "run_id", 1),
                "order_id": _row_get(row, "order_id", 2),
                "symbol": _row_get(row, "symbol", 3),
                "side": _row_get(row, "side", 4),
                "quantity": _row_get(row, "quantity", 5),
                "price": _row_get(row, "price", 6),
                "intended_price": _row_get(row, "intended_price", 7),
                "fill_market_context": _coerce_json(_row_get(row, "fill_market_context", 8)),
                "created_at": _row_get(row, "created_at", 9),
                "updated_at": _row_get(row, "updated_at", 10),
                "trade_time": _row_get(row, "trade_time", 11),
            }
            break
        time.sleep(poll_seconds)
    if latest is None:
        return _phase("sentinel_fill_poll", "FAIL", "timed out waiting for sentinel fill", {"run_id": sentinel.run_id}), None
    failures: list[str] = []
    if latest["run_id"] != sentinel.run_id:
        failures.append("run_id")
    if latest["symbol"] != sentinel.symbol:
        failures.append("symbol")
    if latest["side"] != sentinel.side:
        failures.append("side")
    if int(latest["quantity"] or 0) != sentinel.quantity:
        failures.append("quantity")
    if _as_decimal(latest["intended_price"]) != Decimal(sentinel.intended_price):
        failures.append("intended_price")
    if not isinstance(latest["fill_market_context"], dict) or not latest["fill_market_context"]:
        failures.append("fill_market_context")
    if latest["created_at"] is None:
        failures.append("created_at")
    if latest["updated_at"] is None:
        failures.append("updated_at")
    return _phase(
        "sentinel_fill_poll",
        "PASS" if not failures else "FAIL",
        "sentinel fill row is complete" if not failures else f"sentinel fill row failed fields: {', '.join(failures)}",
        {"fill": _json_safe(latest), "failed_fields": failures},
    ), latest


def _check_outbox(conn: Any, sentinel: SentinelOrder) -> tuple[dict[str, Any], dict[str, Any] | None]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT event_id, event_type, source_id, status, payload, created_at, updated_at
            FROM qe_archive.outbox_event
            WHERE source_id = %s OR payload->>'run_id' = %s
            ORDER BY created_at DESC NULLS LAST
            LIMIT 20
            """,
            (sentinel.run_id, sentinel.run_id),
        )
        rows = list(cur.fetchall() or [])
    events: list[dict[str, Any]] = []
    for row in rows:
        payload = _coerce_json(_row_get(row, "payload", 4, {})) or {}
        event = {
            "event_id": _row_get(row, "event_id", 0),
            "event_type": _row_get(row, "event_type", 1),
            "source_id": _row_get(row, "source_id", 2),
            "status": _row_get(row, "status", 3),
            "payload": payload,
            "created_at": _row_get(row, "created_at", 5),
            "updated_at": _row_get(row, "updated_at", 6),
        }
        events.append(event)
    matching = [
        event
        for event in events
        if isinstance(event.get("payload"), dict)
        and event["payload"].get("routing_class") == "telemetry"
        and str(event.get("status") or "").lower() in ALLOWED_OUTBOX_STATUSES
    ]
    event = matching[0] if matching else None
    return _phase(
        "outbox_routing",
        "PASS" if event else "FAIL",
        "sentinel outbox telemetry event is pending/sent" if event else "missing sentinel outbox event with routing_class=telemetry and pending/sent status",
        {"events": _json_safe(events[:5])},
    ), event


def _check_audit_chain(conn: Any, sentinel: SentinelOrder, fill: dict[str, Any] | None, *, require_ledger: bool) -> dict[str, Any]:
    with conn.cursor() as cur:
        like_run = f"%{sentinel.run_id}%"
        cur.execute(
            """
            SELECT validation_run_id, created_at
            FROM strategy_pkg.package_validation_run
            WHERE evidence_json::text LIKE %s
            ORDER BY created_at ASC
            LIMIT 1
            """,
            (like_run,),
        )
        evidence = cur.fetchone()
        cur.execute(
            """
            SELECT asset_id, created_at
            FROM strategy_pkg.package_asset
            WHERE metadata::text LIKE %s
               OR asset_ref LIKE %s
            ORDER BY created_at ASC
            LIMIT 1
            """,
            (like_run, f"governance/coldstart_sanity/{sentinel.run_id}%"),
        )
        ledger = cur.fetchone()
    failures: list[str] = []
    if evidence is None:
        failures.append("missing_governance_evidence")
    if ledger is None and require_ledger:
        failures.append("missing_protected_ledger")
    fill_ts = fill.get("created_at") if fill else None
    evidence_ts = _row_get(evidence, "created_at", 1) if evidence else None
    ledger_ts = _row_get(ledger, "created_at", 1) if ledger else None
    if fill_ts is not None and evidence_ts is not None and not (fill_ts <= evidence_ts):
        failures.append("fill_after_evidence")
    if evidence_ts is not None and ledger_ts is not None and not (evidence_ts <= ledger_ts):
        failures.append("evidence_after_ledger")
    return _phase(
        "audit_chain",
        "PASS" if not failures else "FAIL",
        "sentinel audit chain is ordered" if not failures else f"audit chain failures: {', '.join(failures)}",
        {
            "evidence": _json_safe({"validation_run_id": _row_get(evidence, "validation_run_id", 0), "created_at": evidence_ts}) if evidence else None,
            "ledger": _json_safe({"asset_id": _row_get(ledger, "asset_id", 0), "created_at": ledger_ts}) if ledger else None,
            "ledger_required": require_ledger,
            "failed_fields": failures,
        },
    )


def _cleanup_sentinel(conn: Any, sentinel: SentinelOrder) -> dict[str, Any]:
    deletes = [
        (
            "qe_archive.outbox_event",
            "DELETE FROM qe_archive.outbox_event WHERE source_id = %s OR payload->>'run_id' = %s",
            (sentinel.run_id, sentinel.run_id),
        ),
        (
            "strategy_pkg.package_validation_run",
            "DELETE FROM strategy_pkg.package_validation_run WHERE evidence_json::text LIKE %s",
            (f"%{sentinel.run_id}%",),
        ),
        (
            "strategy_pkg.package_asset",
            "DELETE FROM strategy_pkg.package_asset WHERE metadata::text LIKE %s OR asset_ref LIKE %s",
            (f"%{sentinel.run_id}%", f"governance/coldstart_sanity/{sentinel.run_id}%"),
        ),
        ("paper_v2.fills", "DELETE FROM paper_v2.fills WHERE run_id = %s", (sentinel.run_id,)),
        ("paper_v2.order_events", "DELETE FROM paper_v2.order_events WHERE run_id = %s", (sentinel.run_id,)),
        ("paper_v2.orders", "DELETE FROM paper_v2.orders WHERE run_id = %s", (sentinel.run_id,)),
        ("paper_v2.run_events", "DELETE FROM paper_v2.run_events WHERE run_id = %s", (sentinel.run_id,)),
    ]
    results: list[dict[str, Any]] = []
    for table, sql, params in deletes:
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rowcount = int(getattr(cur, "rowcount", 0) or 0)
            conn.commit()
            results.append({"table": table, "status": "committed", "deleted_rows": rowcount})
        except Exception as exc:
            rollback = getattr(conn, "rollback", None)
            if callable(rollback):
                rollback()
            results.append({"table": table, "status": "rolled_back", "error": str(exc), "deleted_rows": 0})
            return _phase("sentinel_cleanup", "FAIL", f"cleanup failed for {table}: {exc}", {"tables": results})
    return _phase("sentinel_cleanup", "PASS", "sentinel cleanup committed per table", {"tables": results})


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value


def _failed_checks(checks: Iterable[dict[str, Any]]) -> list[str]:
    return [str(item["check"]) for item in checks if item.get("status") == "FAIL"]


def _remedial_action(failed: list[str]) -> list[str]:
    mapping = {
        "backend_health": "Confirm backend health on the approved R6 release and /health returns OK.",
        "paper_v2_daemon_process": "Start or fix the approved Paper v2/R6 daemon before re-running the gate.",
        "db_readonly_ping": "Confirm production DB credentials and read-only connectivity.",
        "required_tables": "Apply or verify the approved R6 migrations before runtime activation.",
        "governance_evidence_and_enable_paper": "Verify evidence backfill, protected asset ledger, runtime variants, and enable_paper status for approved packages.",
        "sentinel_order_trigger": "Verify the configured Paper v2 sentinel endpoint or daemon entry point.",
        "sentinel_fill_poll": "Inspect daemon logs, market data, and paper_v2.fills for the sentinel run_id.",
        "outbox_routing": "Verify T13 routing_class telemetry outbox emission for daemon events.",
        "audit_chain": "Verify governance evidence and protected ledger rows reference the sentinel run_id with ordered timestamps.",
        "sentinel_cleanup": "Stop before 9:30 and manually review sentinel residue using the run_id; do not improvise ad hoc deletes.",
    }
    return [mapping.get(check, f"Investigate failed check: {check}") for check in failed]


def _base_report(args: argparse.Namespace, target: DbTarget, sentinel: SentinelOrder) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "mode": args.mode,
        "dry_run": args.mode == "dry-run",
        "target_db": target.target_db,
        "db_target": target.label,
        "run_id": sentinel.run_id,
        "sentinel_order": sentinel.payload(),
        "db_connection_opened": False,
        "db_writes_executed": False,
        "production_services_touched": False,
        "prod_backend_http_touched": False,
        "sentinel_order_requested": False,
        "phases": [],
        "failed_checks": [],
        "remedial_action": [],
        "real_trading_ready": False,
        "verdict": "NO-GO",
    }


def run_dry_run(args: argparse.Namespace, target: DbTarget, sentinel: SentinelOrder) -> dict[str, Any]:
    report = _base_report(args, target, sentinel)
    report["status"] = "dry_run_preview"
    report["phases"] = [
        _phase("prod_guards", "SKIPPED", "dry-run does not evaluate live prod guards", {
            "required_token": CONFIRM_PROD,
            "required_env": ENV_PROD_ENABLED,
            "required_mutex_env": ENV_MUTEX_HELD,
            "non_trading_hours_required": True,
        }),
        _phase("preflight", "SKIPPED", "dry-run opens no DB connection and makes no HTTP requests"),
        _phase("sentinel_round_trip", "SKIPPED", "dry-run does not trigger sentinel order"),
        _phase("audit_chain", "SKIPPED", "dry-run does not inspect production audit rows"),
        _phase("cleanup", "SKIPPED", "dry-run performs no cleanup writes"),
    ]
    report["safety_notes"] = [
        "No production DB connection opened.",
        "No backend HTTP request sent.",
        "No dev DB insert or production write attempted.",
        "Run --mode=prod only in the approved non-trading window with the exact token, env flag, mutex, and operator confirmation.",
    ]
    return report


def run_prod(args: argparse.Namespace, target: DbTarget, sentinel: SentinelOrder) -> dict[str, Any]:
    report = _base_report(args, target, sentinel)
    conn: Any | None = None
    triggered = False
    cleanup_done = False
    checks: list[dict[str, Any]] = []
    try:
        _require_prod_guards(args, target, now=_now_local())
        checks.append(_phase("prod_guards", "PASS", "all production guards passed"))

        health = _check_backend_health(args)
        report["prod_backend_http_touched"] = True
        checks.append(health)
        daemon = _check_daemon(args)
        checks.append(daemon)
        if health["status"] == "FAIL" or daemon["status"] == "FAIL":
            report["phases"] = checks
            failed = _failed_checks(checks)
            report["failed_checks"] = failed
            report["remedial_action"] = _remedial_action(failed)
            report["status"] = "failed"
            return report

        conn = _connect(target, readonly=True)
        report["db_connection_opened"] = True
        checks.extend(_preflight_db_checks(conn, list(args.package_id or [])))
        if any(item["status"] == "FAIL" for item in checks):
            report["phases"] = checks
            failed = _failed_checks(checks)
            report["failed_checks"] = failed
            report["remedial_action"] = _remedial_action(failed)
            report["status"] = "failed"
            return report

        trigger = _trigger_sentinel_order(args, sentinel)
        report["sentinel_order_requested"] = True
        report["production_services_touched"] = True
        checks.append(trigger)
        triggered = trigger["status"] == "PASS"
        if triggered:
            fill_check, fill = _poll_fill(conn, sentinel, timeout_seconds=args.timeout_seconds, poll_seconds=args.poll_seconds)
            checks.append(fill_check)
            if fill is not None:
                outbox_check, _ = _check_outbox(conn, sentinel)
                checks.append(outbox_check)
                checks.append(_check_audit_chain(conn, sentinel, fill, require_ledger=args.require_ledger_audit))
    finally:
        if conn is not None:
            close = getattr(conn, "close", None)
            if callable(close):
                close()
            conn = None
        if triggered:
            cleanup_conn = _connect(target, readonly=False)
            try:
                cleanup = _cleanup_sentinel(cleanup_conn, sentinel)
                cleanup_done = True
                checks.append(cleanup)
                report["db_writes_executed"] = True
            finally:
                close = getattr(cleanup_conn, "close", None)
                if callable(close):
                    close()
    report["cleanup_attempted"] = cleanup_done
    report["phases"] = checks
    failed = _failed_checks(checks)
    report["failed_checks"] = failed
    report["remedial_action"] = _remedial_action(failed)
    report["status"] = "passed" if not failed else "failed"
    report["verdict"] = "GO" if not failed else "NO-GO"
    report["real_trading_ready"] = not failed
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the R6 Paper v2 cold-start sanity go/no-go gate.")
    parser.add_argument("--mode", choices=("dry-run", "prod"), default="dry-run")
    parser.add_argument("--confirm-prod", default="", help="Exact production confirmation token required with --mode=prod.")
    parser.add_argument("--operator-confirmation", default="", help="Typed operator confirmation text for --mode=prod.")
    parser.add_argument("--api-base", default=os.environ.get("PAPER_V2_API_BASE", DEFAULT_API_BASE))
    parser.add_argument("--health-path", default=DEFAULT_HEALTH_PATH)
    parser.add_argument("--sentinel-endpoint", default=DEFAULT_SENTINEL_ENDPOINT)
    parser.add_argument("--daemon-process-name", default=DEFAULT_DAEMON_PROCESS_NAME)
    parser.add_argument("--package-id", action="append", default=[], help="Approved R6 package id; repeat for all production packages.")
    parser.add_argument("--run-id", help="Optional sentinel run_id. Defaults to sanity-<timestamp>.")
    parser.add_argument("--timeout-seconds", type=int, default=30)
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    parser.add_argument("--http-timeout", type=float, default=10.0)
    parser.add_argument("--require-ledger-audit", action="store_true", help="Require a protected ledger row for the sentinel audit chain.")
    parser.add_argument("--target-db", choices=("dev", "prod"), default="prod")
    parser.add_argument("--db-host", default="prod-db.invalid")
    parser.add_argument("--db-port", type=int, default=5432)
    parser.add_argument("--db-name", default="aistock")
    parser.add_argument("--db-user", default="aistock_operator")
    parser.add_argument("--db-password", default="", help="Optional DB password; prefer --db-password-env for operator use.")
    parser.add_argument("--db-password-env", default="AISTOCK_PROD_DB_PASSWORD", help="Environment variable containing DB password.")
    parser.add_argument("--json", action="store_true", help="Emit JSON to stdout.")
    parser.add_argument("--output", help="Optional path to write JSON report.")
    return parser


def _emit(report: dict[str, Any], *, json_output: bool, output: str | None) -> None:
    text = json.dumps(_json_safe(report), ensure_ascii=False, indent=2 if json_output else None, sort_keys=True) + "\n"
    if output:
        path = Path(output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    if json_output or not output:
        print(text, end="")


def _failure_payload(error: Exception, *, args: argparse.Namespace, target: DbTarget, sentinel: SentinelOrder) -> dict[str, Any]:
    report = _base_report(args, target, sentinel)
    report.update(
        {
            "status": "failed",
            "error": str(error),
            "failed_checks": ["unhandled_error"],
            "remedial_action": [str(error)],
        }
    )
    return report


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    target = _target_from_args(args)
    run_id = args.run_id or _default_run_id()
    sentinel = SentinelOrder(run_id=run_id)
    try:
        _require(args.timeout_seconds > 0, "--timeout-seconds must be positive")
        _require(args.poll_seconds > 0, "--poll-seconds must be positive")
        if args.mode == "dry-run":
            report = run_dry_run(args, target, sentinel)
        else:
            report = run_prod(args, target, sentinel)
        _emit(report, json_output=args.json, output=args.output)
        return 0 if report.get("verdict") == "GO" else (0 if args.mode == "dry-run" else 2)
    except ColdStartSanityError as exc:
        _emit(_failure_payload(exc, args=args, target=target, sentinel=sentinel), json_output=True, output=args.output)
        return 2
    except Exception as exc:
        _emit(_failure_payload(exc, args=args, target=target, sentinel=sentinel), json_output=True, output=args.output)
        return 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
