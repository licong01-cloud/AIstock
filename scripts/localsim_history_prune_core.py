"""Governed physical pruning of failed historical LocalSIM run data.

Only historical ``local_sim`` runs in explicit terminal/failure states are
eligible.  StrategyPackage, runtime releases, bindings, selection evidence,
successful runs, MiniQMT rows, and the latest healthy LocalSIM economic state
are protected and verified byte-for-byte across the deletion transaction.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date
from typing import Any, Mapping, Sequence

from psycopg2 import sql


ADVISORY_LOCK_KEY = 1_223_202_608_28
DELETABLE_STATUSES = ("CANCELLED", "FAILED_RETRYABLE", "FAILED_TERMINAL")
ANCHOR_STATUS = "SUCCEEDED"

RUN_CHILD_TABLES: tuple[tuple[str, str], ...] = (
    ("paper_v2.order_execution_state", "run_id"),
    ("paper_v2.intraday_snapshots", "run_id"),
    ("paper_v2.session_events", "run_id"),
    ("paper_v2.session_day", "run_id"),
    ("paper_v2.order_events", "run_id"),
    ("paper_v2.fills", "run_id"),
    ("paper_v2.cash_ledger", "run_id"),
    ("paper_v2.positions", "run_id"),
    ("paper_v2.daily_snapshots", "run_id"),
    ("paper_v2.run_events", "run_id"),
    ("paper_v2.errors", "run_id"),
    ("paper_v2.orders", "run_id"),
)

LOCK_TABLES = tuple(
    sorted(
        {
            *(table for table, _column in RUN_CHILD_TABLES),
            "paper_v2.run",
            "paper_v2.execution_plan",
            "paper_v2.simulation_daily_run",
        }
    )
)


class LocalSimPruneSafetyError(RuntimeError):
    """Raised when physical pruning cannot prove its complete safety contract."""


@dataclass(frozen=True)
class LocalSimPruneRequest:
    simulation_account_id: str
    package_id: str
    anchor_run_id: str

    @classmethod
    def build(
        cls, simulation_account_id: str, package_id: str, anchor_run_id: str
    ) -> "LocalSimPruneRequest":
        account = str(simulation_account_id or "").strip()
        package = str(package_id or "").strip()
        anchor = str(anchor_run_id or "").strip()
        if not account.startswith("simacct_"):
            raise LocalSimPruneSafetyError("simulation_account_id must be one explicit simacct_ identity")
        if not package.startswith("pkg_"):
            raise LocalSimPruneSafetyError("package_id must be one explicit pkg_ identity")
        if not anchor.startswith("simrun_"):
            raise LocalSimPruneSafetyError("anchor_run_id must be one explicit simrun_ identity")
        return cls(simulation_account_id=account, package_id=package, anchor_run_id=anchor)


def canonical_sha256(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _table_exists(cur: Any, qualified_name: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (qualified_name,))
    return cur.fetchone()[0] is not None


def _database_identity(cur: Any) -> dict[str, Any]:
    cur.execute(
        "SELECT current_database(),current_user,COALESCE(inet_server_addr()::text,'local'),"
        "inet_server_port(),current_setting('server_version_num')"
    )
    database, user, server, port, version = cur.fetchone()
    return {
        "database": database,
        "user": user,
        "server": server,
        "port": port,
        "server_version_num": version,
    }


def _fetch_dicts(cur: Any, query: str, params: tuple[Any, ...], columns: Sequence[str]) -> list[dict[str, Any]]:
    cur.execute(query, params)
    return [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]


def _count_any(cur: Any, table: str, column: str, ids: Sequence[str]) -> int:
    if not ids or not _table_exists(cur, table):
        return 0
    schema, name = table.split(".", 1)
    cur.execute(
        sql.SQL("SELECT count(*) FROM {}.{} WHERE {}=ANY(%s)").format(
            sql.Identifier(schema), sql.Identifier(name), sql.Identifier(column)
        ),
        (list(ids),),
    )
    return int(cur.fetchone()[0])


def _account_scope(cur: Any, request: LocalSimPruneRequest) -> dict[str, Any]:
    columns = (
        "account_id",
        "package_id",
        "status",
        "ledger_scope_id",
        "scope_kind",
        "ledger_scope_hash",
    )
    rows = _fetch_dicts(
        cur,
        """
        SELECT account.account_id, account.package_id, account.status,
               scope.ledger_scope_id, scope.scope_kind, scope.ledger_scope_hash
        FROM paper_v2.simulation_account_v1 AS account
        LEFT JOIN paper_v2.legacy_localsim_account_lineage_v1 AS lineage
          ON lineage.account_id = account.account_id
        JOIN paper_v2.simulation_ledger_scope_v1 AS scope
          ON scope.ledger_scope_id = COALESCE(lineage.ledger_scope_id, account.account_id)
        WHERE account.account_id = %s
        """,
        (request.simulation_account_id,),
        columns,
    )
    if len(rows) != 1:
        raise LocalSimPruneSafetyError("simulation account is missing or has ambiguous ledger lineage")
    account = rows[0]
    if account["package_id"] != request.package_id or account["status"] == "RETIRED":
        raise LocalSimPruneSafetyError("simulation account does not match the retained package or is retired")
    return account


def _anchor(cur: Any, request: LocalSimPruneRequest, account_scope: Mapping[str, Any]) -> dict[str, Any]:
    columns = (
        "run_id",
        "trade_date",
        "strategy_id",
        "broker_backend",
        "package_id",
        "release_id",
        "binding_id",
        "execution_plan_id",
        "status",
        "run_payload_json",
        "created_at",
        "updated_at",
    )
    rows = _fetch_dicts(
        cur,
        """
        SELECT run_id,trade_date,strategy_id,broker_backend,package_id,release_id,binding_id,
               execution_plan_id,status,run_payload_json,created_at,updated_at
        FROM paper_v2.simulation_daily_run WHERE run_id=%s
        """,
        (request.anchor_run_id,),
        columns,
    )
    if len(rows) != 1:
        raise LocalSimPruneSafetyError(f"anchor run is missing: {request.anchor_run_id}")
    anchor = rows[0]
    if anchor["broker_backend"] != "local_sim" or anchor["package_id"] != request.package_id:
        raise LocalSimPruneSafetyError("anchor run does not match the explicit LocalSIM package")
    if str(anchor["strategy_id"]) != str(account_scope["ledger_scope_id"]):
        raise LocalSimPruneSafetyError("anchor run does not belong to the explicit simulation account ledger scope")
    if anchor["status"] != ANCHOR_STATUS:
        raise LocalSimPruneSafetyError(f"anchor run must be SUCCEEDED: observed={anchor['status']}")
    cur.execute(
        """
        SELECT run_id,trade_date,status FROM paper_v2.simulation_daily_run
        WHERE broker_backend='local_sim' AND package_id=%s AND strategy_id=%s
        ORDER BY trade_date DESC,created_at DESC,run_id DESC LIMIT 1
        """,
        (request.package_id, account_scope["ledger_scope_id"]),
    )
    latest = cur.fetchone()
    if latest is None or str(latest[0]) != request.anchor_run_id:
        raise LocalSimPruneSafetyError(
            f"anchor is not the latest LocalSIM package run: latest={latest[0] if latest else None}"
        )
    payload = anchor["run_payload_json"]
    if not isinstance(payload, Mapping) or payload.get("last_stage") != ANCHOR_STATUS:
        raise LocalSimPruneSafetyError("anchor run payload does not carry a SUCCEEDED last_stage")
    return anchor


def _candidate_runs(
    cur: Any,
    request: LocalSimPruneRequest,
    *,
    ledger_scope_id: str,
    anchor_date: date,
) -> list[dict[str, Any]]:
    columns = (
        "run_id",
        "trade_date",
        "strategy_id",
        "package_id",
        "release_id",
        "binding_id",
        "execution_plan_id",
        "status",
        "run_payload_json",
        "created_at",
        "updated_at",
    )
    rows = _fetch_dicts(
        cur,
        """
        SELECT run_id,trade_date,strategy_id,package_id,release_id,binding_id,
               execution_plan_id,status,run_payload_json,created_at,updated_at
        FROM paper_v2.simulation_daily_run
        WHERE broker_backend='local_sim' AND package_id=%s AND strategy_id=%s AND trade_date < %s
          AND status=ANY(%s)
        ORDER BY trade_date,run_id
        """,
        (request.package_id, ledger_scope_id, anchor_date, list(DELETABLE_STATUSES)),
        columns,
    )
    for row in rows:
        if row["run_id"] == request.anchor_run_id:
            raise LocalSimPruneSafetyError("anchor run was selected as a prune candidate")
        if row["status"] not in DELETABLE_STATUSES:
            raise LocalSimPruneSafetyError(f"candidate status drift: run_id={row['run_id']}")
        if not isinstance(row["run_payload_json"], Mapping):
            raise LocalSimPruneSafetyError(f"candidate payload is not an object: run_id={row['run_id']}")
    return rows


def _anchor_economic_snapshot(cur: Any, anchor: Mapping[str, Any]) -> dict[str, Any]:
    strategy_id = str(anchor["strategy_id"])
    trade_date = anchor["trade_date"]
    cur.execute(
        "SELECT run_id,status,data_source,error_json FROM paper_v2.run WHERE run_id=%s",
        (anchor["run_id"],),
    )
    paper_run = cur.fetchone()
    if paper_run is None or str(paper_run[1]).upper() != "SUCCEEDED":
        raise LocalSimPruneSafetyError("anchor Paper v2 run is missing or not SUCCEEDED")
    cur.execute(
        """
        SELECT c.run_id,c.trade_date,c.cash_after,c.cash_id
        FROM paper_v2.cash_ledger c JOIN paper_v2.run r ON r.run_id=c.run_id
        WHERE c.portfolio_id=%s AND c.trade_date<=%s
        ORDER BY c.trade_date DESC,c.created_at DESC,c.cash_id DESC LIMIT 1
        """,
        (strategy_id, trade_date),
    )
    cash_row = cur.fetchone()
    if cash_row is None or str(cash_row[0]) != str(anchor["run_id"]):
        raise LocalSimPruneSafetyError("anchor run is not the latest cash authority")
    cur.execute(
        """
        SELECT run_id,trade_date,cash,market_value,nav,position_count,snapshot_time
        FROM paper_v2.daily_snapshots
        WHERE portfolio_id=%s AND trade_date<=%s
        ORDER BY trade_date DESC,updated_at DESC,snapshot_id DESC LIMIT 1
        """,
        (strategy_id, trade_date),
    )
    daily_snapshot = cur.fetchone()
    if daily_snapshot is None or str(daily_snapshot[0]) != str(anchor["run_id"]):
        raise LocalSimPruneSafetyError("anchor run is not the latest complete position snapshot authority")
    cur.execute(
        """
        SELECT symbol,quantity,available_quantity,avg_cost,trade_date,run_id
        FROM paper_v2.positions
        WHERE portfolio_id=%s AND run_id=%s
        ORDER BY symbol
        """,
        (strategy_id, anchor["run_id"]),
    )
    positions = [
        {
            "symbol": str(row[0]),
            "quantity": int(row[1]),
            "available_quantity": int(row[2]),
            "avg_cost": str(row[3]),
            "trade_date": str(row[4]),
            "run_id": str(row[5]),
        }
        for row in cur.fetchall()
    ]
    snapshot = {
        "paper_run": {
            "run_id": str(paper_run[0]),
            "status": str(paper_run[1]),
            "data_source": str(paper_run[2]),
            "error_json": paper_run[3],
        },
        "latest_cash": {
            "run_id": str(cash_row[0]),
            "trade_date": str(cash_row[1]),
            "cash_after": str(cash_row[2]),
            "cash_id": int(cash_row[3]),
        },
        "latest_daily_snapshot": {
            "run_id": str(daily_snapshot[0]),
            "trade_date": str(daily_snapshot[1]),
            "cash": str(daily_snapshot[2]),
            "market_value": str(daily_snapshot[3]),
            "nav": str(daily_snapshot[4]),
            "position_count": int(daily_snapshot[5]),
            "snapshot_time": str(daily_snapshot[6]),
        },
        "latest_positions": positions,
        "positive_position_count": sum(1 for item in positions if item["quantity"] > 0),
    }
    return {**snapshot, "snapshot_sha256": canonical_sha256(snapshot)}


def _projected_latest_position_snapshot(
    cur: Any, anchor: Mapping[str, Any], excluded_run_ids: Sequence[str]
) -> dict[str, Any]:
    cur.execute(
        """
        WITH authority AS (
            SELECT run_id FROM paper_v2.daily_snapshots
            WHERE portfolio_id=%s AND trade_date<=%s AND NOT (run_id=ANY(%s))
            ORDER BY trade_date DESC,updated_at DESC,snapshot_id DESC LIMIT 1
        )
        SELECT p.symbol,p.quantity,p.available_quantity,p.avg_cost,p.trade_date,p.run_id
        FROM paper_v2.positions p JOIN authority a ON a.run_id=p.run_id
        ORDER BY p.symbol
        """,
        (str(anchor["strategy_id"]), anchor["trade_date"], list(excluded_run_ids)),
    )
    positions = [
        {
            "symbol": str(row[0]),
            "quantity": int(row[1]),
            "available_quantity": int(row[2]),
            "avg_cost": str(row[3]),
            "trade_date": str(row[4]),
            "run_id": str(row[5]),
        }
        for row in cur.fetchall()
    ]
    payload = {
        "latest_positions": positions,
        "positive_position_count": sum(1 for item in positions if item["quantity"] > 0),
    }
    return {**payload, "snapshot_sha256": canonical_sha256(payload)}


def _protected_snapshot(cur: Any, request: LocalSimPruneRequest, anchor: Mapping[str, Any]) -> dict[str, Any]:
    cur.execute("SELECT count(*) FROM strategy_pkg.package WHERE package_id=%s", (request.package_id,))
    package_count = int(cur.fetchone()[0])
    if package_count != 1:
        raise LocalSimPruneSafetyError("explicit retained StrategyPackage is missing")
    cur.execute(
        """
        SELECT account.*, lineage.lineage_id, lineage.lineage_hash, lineage.legacy_account_id,
               lineage.ledger_scope_id, lineage.economic_facts_sha256, lineage.status AS lineage_status,
               scope.ledger_scope_hash, scope.scope_kind, scope.source_identity, scope.native_account_id
        FROM paper_v2.simulation_account_v1 AS account
        LEFT JOIN paper_v2.legacy_localsim_account_lineage_v1 AS lineage
          ON lineage.account_id = account.account_id
        JOIN paper_v2.simulation_ledger_scope_v1 AS scope
          ON scope.ledger_scope_id = COALESCE(lineage.ledger_scope_id, account.account_id)
        WHERE account.account_id=%s
        """,
        (request.simulation_account_id,),
    )
    account_scope = cur.fetchone()
    if account_scope is None:
        raise LocalSimPruneSafetyError("successor account and ledger scope protection readback is missing")
    cur.execute("SELECT count(*) FROM selection.daily_selection_evidence")
    selection_count = int(cur.fetchone()[0])
    cur.execute("SELECT count(*) FROM paper_v2.simulation_daily_run WHERE broker_backend='minqmt_sim'")
    miniqmt_run_count = int(cur.fetchone()[0])
    cur.execute("SELECT * FROM paper_v2.simulation_release_binding WHERE binding_id=%s", (anchor["binding_id"],))
    anchor_binding = cur.fetchone()
    if anchor_binding is None:
        raise LocalSimPruneSafetyError("anchor binding is missing")
    economic = _anchor_economic_snapshot(cur, anchor)
    protected = {
        "simulation_account_scope_sha256": canonical_sha256(list(account_scope)),
        "strategy_package_count": package_count,
        "selection_evidence_count": selection_count,
        "miniqmt_simulation_run_count": miniqmt_run_count,
        "anchor_run_sha256": canonical_sha256(dict(anchor)),
        "anchor_binding_sha256": canonical_sha256(list(anchor_binding)),
        "anchor_economic_snapshot": economic,
    }
    return {**protected, "protected_sha256": canonical_sha256(protected)}


def _deletable_plan_ids(cur: Any, run_ids: Sequence[str], candidate_plan_ids: Sequence[str]) -> list[str]:
    if not candidate_plan_ids:
        return []
    cur.execute(
        """
        SELECT p.plan_id FROM paper_v2.execution_plan p
        WHERE p.plan_id=ANY(%s)
          AND NOT EXISTS (
            SELECT 1 FROM paper_v2.simulation_daily_run r
            WHERE r.execution_plan_id=p.plan_id AND NOT (r.run_id=ANY(%s))
          )
        ORDER BY p.plan_id
        """,
        (list(candidate_plan_ids), list(run_ids)),
    )
    return [str(row[0]) for row in cur.fetchall()]


def build_prune_plan(cur: Any, request: LocalSimPruneRequest) -> dict[str, Any]:
    account_scope = _account_scope(cur, request)
    anchor = _anchor(cur, request, account_scope)
    candidates = _candidate_runs(
        cur,
        request,
        ledger_scope_id=str(account_scope["ledger_scope_id"]),
        anchor_date=anchor["trade_date"],
    )
    run_ids = [str(row["run_id"]) for row in candidates]
    candidate_plan_ids = sorted({str(row["execution_plan_id"]) for row in candidates if row["execution_plan_id"]})
    plan_ids = _deletable_plan_ids(cur, run_ids, candidate_plan_ids)
    shared_plan_ids = sorted(set(candidate_plan_ids) - set(plan_ids))
    delete_counts = {table: _count_any(cur, table, column, run_ids) for table, column in RUN_CHILD_TABLES}
    delete_counts["paper_v2.run"] = _count_any(cur, "paper_v2.run", "run_id", run_ids)
    delete_counts["paper_v2.simulation_daily_run"] = len(run_ids)
    delete_counts["paper_v2.execution_plan"] = len(plan_ids)
    status_counts = {status: sum(1 for row in candidates if row["status"] == status) for status in DELETABLE_STATUSES}
    plan: dict[str, Any] = {
        "schema_version": "aistock_localsim_history_prune_plan_v2",
        "database_identity": _database_identity(cur),
        "request": {
            "simulation_account_id": request.simulation_account_id,
            "package_id": request.package_id,
            "anchor_run_id": request.anchor_run_id,
        },
        "ledger_scope": account_scope,
        "anchor": {
            "run_id": anchor["run_id"],
            "trade_date": str(anchor["trade_date"]),
            "status": anchor["status"],
            "ledger_scope_id": anchor["strategy_id"],
            "binding_id": anchor["binding_id"],
            "execution_plan_id": anchor["execution_plan_id"],
        },
        "candidate_count": len(candidates),
        "candidate_status_counts": status_counts,
        "candidate_run_ids": run_ids,
        "candidate_rows_sha256": canonical_sha256(candidates),
        "deletable_execution_plan_ids": plan_ids,
        "protected_shared_execution_plan_ids": shared_plan_ids,
        "delete_counts": dict(sorted(delete_counts.items())),
        "projected_position_snapshot_after_prune": _projected_latest_position_snapshot(cur, anchor, run_ids),
        "protected": _protected_snapshot(cur, request, anchor),
        "preserved": {
            "strategy_packages": "all",
            "strategy_runtime_releases": "all",
            "simulation_release_bindings": "all",
            "selection_evidence": "all",
            "successful_localsim_runs": "all",
            "miniqmt_data": "all",
        },
    }
    return {**plan, "plan_sha256": canonical_sha256(plan)}


def _lock_tables(cur: Any) -> None:
    present = [table for table in LOCK_TABLES if _table_exists(cur, table)]
    identifiers = sql.SQL(", ").join(sql.Identifier(*table.split(".", 1)) for table in present)
    cur.execute(sql.SQL("LOCK TABLE {} IN SHARE ROW EXCLUSIVE MODE").format(identifiers))


def _delete_any(cur: Any, table: str, column: str, ids: Sequence[str]) -> int:
    if not ids or not _table_exists(cur, table):
        return 0
    schema, name = table.split(".", 1)
    cur.execute(
        sql.SQL("DELETE FROM {}.{} WHERE {}=ANY(%s)").format(
            sql.Identifier(schema), sql.Identifier(name), sql.Identifier(column)
        ),
        (list(ids),),
    )
    return int(cur.rowcount)


def apply_prune_plan(
    cur: Any,
    request: LocalSimPruneRequest,
    expected_plan_sha256: str,
) -> tuple[dict[str, Any], dict[str, int], dict[str, Any]]:
    cur.execute("SET LOCAL lock_timeout='5s'")
    cur.execute("SET LOCAL statement_timeout='5min'")
    cur.execute("SELECT pg_advisory_xact_lock(%s)", (ADVISORY_LOCK_KEY,))
    _lock_tables(cur)
    plan = build_prune_plan(cur, request)
    if not plan["candidate_count"]:
        raise LocalSimPruneSafetyError("prune plan has no eligible historical LocalSIM runs")
    if plan["plan_sha256"] != expected_plan_sha256:
        raise LocalSimPruneSafetyError(
            f"prune plan digest changed: expected={expected_plan_sha256} observed={plan['plan_sha256']}"
        )
    run_ids = list(plan["candidate_run_ids"])
    counts: dict[str, int] = {}
    for table, column in RUN_CHILD_TABLES:
        counts[table] = _delete_any(cur, table, column, run_ids)
    counts["paper_v2.run"] = _delete_any(cur, "paper_v2.run", "run_id", run_ids)
    counts["paper_v2.simulation_daily_run"] = _delete_any(cur, "paper_v2.simulation_daily_run", "run_id", run_ids)
    counts["paper_v2.execution_plan"] = _delete_any(
        cur,
        "paper_v2.execution_plan",
        "plan_id",
        list(plan["deletable_execution_plan_ids"]),
    )
    expected_counts = plan["delete_counts"]
    if dict(sorted(counts.items())) != dict(sorted(expected_counts.items())):
        raise LocalSimPruneSafetyError(f"deleted row counts drifted: expected={expected_counts} observed={counts}")
    cur.execute(
        "SELECT count(*) FROM paper_v2.simulation_daily_run WHERE run_id=ANY(%s)",
        (run_ids,),
    )
    remaining_simulation_runs = int(cur.fetchone()[0])
    cur.execute("SELECT count(*) FROM paper_v2.run WHERE run_id=ANY(%s)", (run_ids,))
    remaining_paper_runs = int(cur.fetchone()[0])
    anchor = _anchor(cur, request, _account_scope(cur, request))
    protected_after = _protected_snapshot(cur, request, anchor)
    if protected_after["protected_sha256"] != plan["protected"]["protected_sha256"]:
        raise LocalSimPruneSafetyError("protected package/evidence/MiniQMT/anchor economic state changed")
    readback = {
        "remaining_simulation_run_count": remaining_simulation_runs,
        "remaining_paper_run_count": remaining_paper_runs,
        "protected_sha256": protected_after["protected_sha256"],
        "anchor_economic_snapshot_sha256": protected_after["anchor_economic_snapshot"]["snapshot_sha256"],
    }
    if remaining_simulation_runs or remaining_paper_runs:
        raise LocalSimPruneSafetyError(f"candidate run rows remain after prune: {readback}")
    return plan, counts, readback
