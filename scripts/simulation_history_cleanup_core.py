"""Governed cleanup core for obsolete LocalSIM and MiniQMT history.

The module is deliberately independent from the normal portfolio delete API:
historical cleanup must cover the unified simulation/runtime graph in one
transaction, while preserving StrategyPackage and selection evidence records.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable, Mapping, Sequence

from psycopg2 import sql


ADVISORY_LOCK_KEY = 1_099_202_608_15
LEGACY_K2_TABLES = (
    "execution_child_order",
    "execution_algo_instance",
    "execution_runtime_event",
    "execution_runtime",
)
LEGACY_QMT_STRATEGY_TABLES = (
    "cash_ledger",
    "daily_snapshot",
    "order_batch",
    "order_intent",
    "order_ledger",
    "position_lot",
    "reconciliation_issue",
    "strategy_package_binding",
    "trade_ledger",
    "virtual_account",
)
CORE_PRODUCTION_TABLES = (
    "strategy_pkg.package",
    "strategy_pkg.strategy_runtime_release",
    "selection.daily_selection_evidence",
    "paper_v2.portfolio",
    "paper_v2.run",
    "paper_v2.trade_session",
    "paper_v2.orders",
    "paper_v2.simulation_release_binding",
    "paper_v2.simulation_daily_run",
    "paper_v2.execution_plan",
    "paper_v2.broker_account_binding",
    "qmt_strategy.strategy_package_binding",
    "qmt_strategy.execution_runtime",
    "qmt_strategy.execution_runtime_event",
    "qmt_strategy.execution_algo_instance",
    "qmt_strategy.execution_child_order",
)
MUTATION_TABLES = (
    "paper_v2.broker_account_binding",
    "paper_v2.cash_ledger",
    "paper_v2.config_change_audit",
    "paper_v2.daily_snapshots",
    "paper_v2.errors",
    "paper_v2.execution_plan",
    "paper_v2.execution_policy_activation",
    "paper_v2.fills",
    "paper_v2.intraday_snapshots",
    "paper_v2.order_events",
    "paper_v2.order_execution_state",
    "paper_v2.orders",
    "paper_v2.portfolio",
    "paper_v2.positions",
    "paper_v2.reset_audit",
    "paper_v2.run",
    "paper_v2.run_events",
    "paper_v2.runtime_config_activation",
    "paper_v2.runtime_profile",
    "paper_v2.runtime_profile_version",
    "paper_v2.session_day",
    "paper_v2.session_events",
    "paper_v2.simulation_daily_run",
    "paper_v2.simulation_release_binding",
    "paper_v2.trade_session",
    "qmt_strategy.execution_algo_instance",
    "qmt_strategy.execution_child_order",
    "qmt_strategy.execution_parent_benchmark",
    "qmt_strategy.execution_planning_subject",
    "qmt_strategy.execution_runtime",
    "qmt_strategy.execution_runtime_event",
    "qmt_strategy.cash_ledger",
    "qmt_strategy.daily_snapshot",
    "qmt_strategy.order_batch",
    "qmt_strategy.order_intent",
    "qmt_strategy.order_ledger",
    "qmt_strategy.order_status_event",
    "qmt_strategy.position_lot",
    "qmt_strategy.reconciliation_issue",
    "qmt_strategy.strategy_package_binding",
    "qmt_strategy.trade_ledger",
    "qmt_strategy.virtual_account",
    "selection.paper_portfolio_link",
    "strategy_pkg.strategy_runtime_release",
)


class CleanupSafetyError(RuntimeError):
    """Raised when the observed database cannot be cleaned safely."""


@dataclass(frozen=True)
class CleanupRequest:
    keep_package_ids: tuple[str, ...]
    miniqmt_cutoff: date

    @classmethod
    def build(cls, keep_package_ids: Iterable[str], miniqmt_cutoff: date) -> "CleanupRequest":
        normalized = tuple(sorted({item.strip() for item in keep_package_ids if item.strip()}))
        if len(normalized) < 2:
            raise CleanupSafetyError("at least two explicit keep package IDs are required")
        return cls(keep_package_ids=normalized, miniqmt_cutoff=miniqmt_cutoff)


def canonical_plan_sha256(plan: Mapping[str, Any]) -> str:
    payload = dict(plan)
    payload.pop("plan_sha256", None)
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def classify_miniqmt_runtime(
    *,
    mode: str,
    trade_date: date,
    binding_id: str | None,
    binding_package_id: str | None,
    keep_package_ids: Sequence[str],
    cutoff: date,
) -> str:
    """Classify a K2 runtime; unknown binding metadata always fails closed."""
    if trade_date >= cutoff:
        return "keep_cutoff"
    if mode != "SIM":
        raise CleanupSafetyError(f"refusing to purge non-SIM MiniQMT runtime mode={mode}")
    if binding_id:
        if not binding_package_id:
            raise CleanupSafetyError(f"runtime references unknown binding_id={binding_id}")
        return "keep_package" if binding_package_id in keep_package_ids else "purge_obsolete_package"
    return "purge_unbound_legacy"


def _table_exists(cur: Any, qualified_name: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (qualified_name,))
    return cur.fetchone()[0] is not None


def _fetch_ids(cur: Any, query: str, params: tuple[Any, ...] = ()) -> list[str]:
    cur.execute(query, params)
    return sorted(str(row[0]) for row in cur.fetchall())


def _count(cur: Any, query: str, params: tuple[Any, ...] = ()) -> int:
    cur.execute(query, params)
    return int(cur.fetchone()[0])


def _count_any(cur: Any, table: str, column: str, ids: Sequence[str]) -> int:
    if not ids or not _table_exists(cur, table):
        return 0
    schema, name = table.split(".", 1)
    cur.execute(
        sql.SQL("SELECT count(*) FROM {}.{} WHERE {} = ANY(%s)").format(
            sql.Identifier(schema), sql.Identifier(name), sql.Identifier(column)
        ),
        (list(ids),),
    )
    return int(cur.fetchone()[0])


def _database_identity(cur: Any) -> dict[str, Any]:
    cur.execute(
        "SELECT current_database(), current_user, "
        "COALESCE(inet_server_addr()::text, 'local'), inet_server_port(), current_setting('server_version_num')"
    )
    dbname, user, host, port, version = cur.fetchone()
    return {"database": dbname, "user": user, "server": host, "port": port, "server_version_num": version}


def _paper_candidates(cur: Any, keep: Sequence[str]) -> dict[str, list[str]]:
    portfolios = _fetch_ids(
        cur, "SELECT portfolio_id FROM paper_v2.portfolio WHERE NOT (package_id = ANY(%s))", (list(keep),)
    )
    runs = (
        _fetch_ids(cur, "SELECT run_id FROM paper_v2.run WHERE portfolio_id = ANY(%s)", (portfolios,))
        if portfolios
        else []
    )
    sessions = (
        _fetch_ids(cur, "SELECT session_id FROM paper_v2.trade_session WHERE portfolio_id = ANY(%s)", (portfolios,))
        if portfolios
        else []
    )
    profiles = (
        _fetch_ids(cur, "SELECT profile_id FROM paper_v2.runtime_profile WHERE portfolio_id = ANY(%s)", (portfolios,))
        if portfolios
        else []
    )
    result = {"portfolio_ids": portfolios, "paper_run_ids": runs, "session_ids": sessions, "profile_ids": profiles}
    for key, table, column in (
        ("binding_ids", "paper_v2.simulation_release_binding", "binding_id"),
        ("simulation_run_ids", "paper_v2.simulation_daily_run", "run_id"),
        ("execution_plan_ids", "paper_v2.execution_plan", "plan_id"),
    ):
        result[key] = _fetch_ids(cur, f"SELECT {column} FROM {table} WHERE NOT (package_id = ANY(%s))", (list(keep),))
    result["qmt_binding_ids"] = (
        _fetch_ids(
            cur,
            "SELECT binding_id FROM qmt_strategy.strategy_package_binding WHERE NOT (package_id = ANY(%s))",
            (list(keep),),
        )
        if _table_exists(cur, "qmt_strategy.strategy_package_binding")
        else []
    )
    return result


def _miniqmt_candidates(cur: Any, request: CleanupRequest) -> tuple[list[str], dict[str, int], list[str]]:
    if not _table_exists(cur, "qmt_strategy.execution_runtime"):
        return [], {}, []
    binding_table = _table_exists(cur, "paper_v2.simulation_release_binding")
    join = (
        "LEFT JOIN paper_v2.simulation_release_binding b ON b.binding_id = NULLIF(r.metadata->>'binding_id', '')"
        if binding_table
        else "LEFT JOIN (SELECT NULL::text binding_id, NULL::text package_id) b ON false"
    )
    cur.execute(
        f"SELECT r.runtime_id,r.mode,r.trade_date,NULLIF(r.metadata->>'binding_id',''),b.package_id "
        f"FROM qmt_strategy.execution_runtime r {join} ORDER BY r.runtime_id"
    )
    candidates: list[str] = []
    protected: list[str] = []
    for runtime_id, mode, trade_date, binding_id, package_id in cur.fetchall():
        classification = classify_miniqmt_runtime(
            mode=mode,
            trade_date=trade_date,
            binding_id=binding_id,
            binding_package_id=package_id,
            keep_package_ids=request.keep_package_ids,
            cutoff=request.miniqmt_cutoff,
        )
        (candidates if classification.startswith("purge_") else protected).append(str(runtime_id))

    counts: dict[str, int] = {}
    cur.execute(
        "SELECT table_name FROM information_schema.columns "
        "WHERE table_schema='qmt_strategy' AND column_name='runtime_id' ORDER BY table_name"
    )
    for (table_name,) in cur.fetchall():
        count = _count_any(cur, f"qmt_strategy.{table_name}", "runtime_id", candidates)
        if count:
            counts[f"qmt_strategy.{table_name}"] = count
            if table_name not in LEGACY_K2_TABLES:
                raise CleanupSafetyError(
                    f"legacy MiniQMT candidate graph contains unsupported rows in qmt_strategy.{table_name}: {count}"
                )

    # Refuse any non-legacy row that directly references a candidate legacy row.
    # This is driven from the live FK catalog so later K2 migrations cannot be
    # silently missed by this maintenance tool.
    if candidates:
        cur.execute(
            """
            SELECT con.conname, child_ns.nspname, child.relname, parent.relname,
                   array_agg(child_att.attname ORDER BY keys.ordinality),
                   array_agg(parent_att.attname ORDER BY keys.ordinality)
            FROM pg_constraint con
            JOIN pg_class child ON child.oid=con.conrelid
            JOIN pg_namespace child_ns ON child_ns.oid=child.relnamespace
            JOIN pg_class parent ON parent.oid=con.confrelid
            JOIN pg_namespace parent_ns ON parent_ns.oid=parent.relnamespace
            JOIN unnest(con.conkey,con.confkey) WITH ORDINALITY keys(child_num,parent_num,ordinality) ON true
            JOIN pg_attribute child_att ON child_att.attrelid=child.oid AND child_att.attnum=keys.child_num
            JOIN pg_attribute parent_att ON parent_att.attrelid=parent.oid AND parent_att.attnum=keys.parent_num
            WHERE con.contype='f' AND parent_ns.nspname='qmt_strategy'
              AND parent.relname=ANY(%s)
            GROUP BY con.conname,child_ns.nspname,child.relname,parent.relname
            ORDER BY child_ns.nspname,child.relname,parent.relname,con.conname
            """,
            (list(LEGACY_K2_TABLES),),
        )
        for _constraint, child_schema, child_table, parent_table, child_keys, parent_keys in cur.fetchall():
            if child_schema == "qmt_strategy" and child_table in LEGACY_K2_TABLES:
                continue
            joins = sql.SQL(" AND ").join(
                sql.SQL("c.{}=p.{}").format(sql.Identifier(child_key), sql.Identifier(parent_key))
                for child_key, parent_key in zip(child_keys, parent_keys, strict=True)
            )
            cur.execute(
                sql.SQL("SELECT count(*) FROM {}.{} c JOIN {}.{} p ON {} WHERE p.runtime_id=ANY(%s)").format(
                    sql.Identifier(child_schema),
                    sql.Identifier(child_table),
                    sql.Identifier("qmt_strategy"),
                    sql.Identifier(parent_table),
                    joins,
                ),
                (candidates,),
            )
            count = int(cur.fetchone()[0])
            if count:
                raise CleanupSafetyError(
                    "legacy MiniQMT candidate graph has unsupported dependent rows in "
                    f"{child_schema}.{child_table}: {count}"
                )
    return sorted(candidates), counts, sorted(protected)


def _release_candidates(cur: Any, keep: Sequence[str]) -> tuple[list[str], int]:
    """Keep package releases, evidence releases, and their complete base ancestry."""
    cur.execute(
        """
        WITH RECURSIVE protected(release_id) AS (
          SELECT release_id FROM strategy_pkg.strategy_runtime_release WHERE package_id=ANY(%s)
          UNION
          SELECT release_id FROM selection.daily_selection_evidence
          UNION
          SELECT parent.base_release_id
          FROM strategy_pkg.strategy_runtime_release parent
          JOIN protected p ON p.release_id=parent.release_id
          WHERE parent.base_release_id IS NOT NULL
        )
        SELECT release_id FROM strategy_pkg.strategy_runtime_release
        WHERE release_id NOT IN (SELECT release_id FROM protected)
        ORDER BY release_id
        """,
        (list(keep),),
    )
    candidates = [str(row[0]) for row in cur.fetchall()]
    retained = _count(
        cur,
        "SELECT count(*) FROM strategy_pkg.strategy_runtime_release WHERE NOT(package_id=ANY(%s))",
        (list(keep),),
    ) - len(candidates)
    return candidates, retained


def _legacy_qmt_strategy_candidates(cur: Any, keep: Sequence[str]) -> tuple[list[str], list[str], dict[str, int]]:
    if not _table_exists(cur, "qmt_strategy.strategy_package_binding"):
        return [], [], {}
    strategy_ids = _fetch_ids(
        cur,
        "SELECT DISTINCT strategy_id FROM qmt_strategy.strategy_package_binding WHERE NOT(package_id=ANY(%s))",
        (list(keep),),
    )
    if not strategy_ids:
        return [], [], {}
    intent_ids = (
        _fetch_ids(
            cur,
            "SELECT intent_id FROM qmt_strategy.order_intent WHERE strategy_id=ANY(%s)",
            (strategy_ids,),
        )
        if _table_exists(cur, "qmt_strategy.order_intent")
        else []
    )
    counts: dict[str, int] = {}
    cur.execute(
        "SELECT table_name FROM information_schema.columns "
        "WHERE table_schema='qmt_strategy' AND column_name='strategy_id' ORDER BY table_name"
    )
    for (table_name,) in cur.fetchall():
        count = _count_any(cur, f"qmt_strategy.{table_name}", "strategy_id", strategy_ids)
        if count:
            counts[f"qmt_strategy.{table_name}"] = count
            if table_name not in LEGACY_QMT_STRATEGY_TABLES:
                raise CleanupSafetyError(
                    f"legacy QMT strategy graph contains unsupported rows in qmt_strategy.{table_name}: {count}"
                )
    counts["qmt_strategy.order_status_event"] = _count_any(
        cur, "qmt_strategy.order_status_event", "intent_id", intent_ids
    )
    for child, query in (
        (
            "qmt_strategy.execution_parent_benchmark",
            "SELECT count(*) FROM qmt_strategy.execution_parent_benchmark e "
            "JOIN qmt_strategy.order_intent i ON i.intent_id=e.qmt_order_intent_id "
            "WHERE i.strategy_id=ANY(%s)",
        ),
        (
            "qmt_strategy.execution_tca_mark",
            "SELECT count(*) FROM qmt_strategy.execution_tca_mark m "
            "JOIN qmt_strategy.trade_ledger t ON (t.account_id,t.trade_date,t.trade_id)="
            "(m.trade_account_id,m.trade_date,m.trade_id) WHERE t.strategy_id=ANY(%s)",
        ),
        (
            "qmt_strategy.execution_tca_trade_observation",
            "SELECT count(*) FROM qmt_strategy.execution_tca_trade_observation o "
            "JOIN qmt_strategy.trade_ledger t ON (t.account_id,t.trade_date,t.trade_id)="
            "(o.account_id,o.trade_date,o.trade_id) WHERE t.strategy_id=ANY(%s)",
        ),
    ):
        if not _table_exists(cur, child):
            continue
        count = _count(cur, query, (strategy_ids,))
        if count:
            raise CleanupSafetyError(f"legacy QMT strategy graph has unsupported dependent rows in {child}: {count}")
    return strategy_ids, intent_ids, counts


PAPER_COUNT_SPECS: tuple[tuple[str, str, str], ...] = (
    ("paper_v2.order_execution_state_by_run", "paper_v2.order_execution_state", "run_id"),
    ("paper_v2.intraday_snapshots_by_run", "paper_v2.intraday_snapshots", "run_id"),
    ("paper_v2.order_events", "paper_v2.order_events", "run_id"),
    ("paper_v2.fills", "paper_v2.fills", "run_id"),
    ("paper_v2.cash_ledger", "paper_v2.cash_ledger", "run_id"),
    ("paper_v2.positions", "paper_v2.positions", "run_id"),
    ("paper_v2.daily_snapshots", "paper_v2.daily_snapshots", "run_id"),
    ("paper_v2.run_events", "paper_v2.run_events", "run_id"),
    ("paper_v2.orders", "paper_v2.orders", "run_id"),
)


def build_cleanup_plan(cur: Any, request: CleanupRequest, *, require_keep_packages: bool = True) -> dict[str, Any]:
    identity = _database_identity(cur)
    schema_presence = {table: _table_exists(cur, table) for table in CORE_PRODUCTION_TABLES}
    missing_core = sorted(table for table, present in schema_presence.items() if not present)
    if missing_core and require_keep_packages:
        raise CleanupSafetyError(f"production cleanup core tables are missing: {missing_core}")
    cur.execute(
        "SELECT package_id FROM strategy_pkg.package WHERE package_id=ANY(%s) ORDER BY package_id",
        (list(request.keep_package_ids),),
    )
    found = tuple(str(row[0]) for row in cur.fetchall())
    missing = sorted(set(request.keep_package_ids) - set(found))
    if missing and require_keep_packages:
        raise CleanupSafetyError(f"keep package IDs do not all exist: {missing}")
    candidates = _paper_candidates(cur, request.keep_package_ids)
    k2_ids, k2_counts, k2_protected = _miniqmt_candidates(cur, request)
    qmt_strategy_ids, qmt_intent_ids, qmt_strategy_counts = _legacy_qmt_strategy_candidates(
        cur, request.keep_package_ids
    )
    release_ids, evidence_retained = _release_candidates(cur, request.keep_package_ids)
    counts: dict[str, int] = {
        "paper_v2.portfolio": len(candidates["portfolio_ids"]),
        "paper_v2.run": len(candidates["paper_run_ids"]),
        "paper_v2.trade_session": len(candidates["session_ids"]),
        "paper_v2.runtime_profile": len(candidates["profile_ids"]),
        "paper_v2.simulation_release_binding": len(candidates["binding_ids"]),
        "paper_v2.simulation_daily_run": len(candidates["simulation_run_ids"]),
        "paper_v2.execution_plan": len(candidates["execution_plan_ids"]),
        "qmt_strategy.strategy_package_binding": len(candidates["qmt_binding_ids"]),
        "strategy_pkg.strategy_runtime_release": len(release_ids),
        **k2_counts,
        **qmt_strategy_counts,
    }
    for label, table, column in PAPER_COUNT_SPECS:
        counts[label] = _count_any(cur, table, column, candidates["paper_run_ids"])
    for label, table, column, ids_key in (
        ("paper_v2.session_events", "paper_v2.session_events", "session_id", "session_ids"),
        ("paper_v2.session_day", "paper_v2.session_day", "portfolio_id", "portfolio_ids"),
        (
            "paper_v2.execution_policy_activation",
            "paper_v2.execution_policy_activation",
            "portfolio_id",
            "portfolio_ids",
        ),
        ("paper_v2.runtime_config_activation", "paper_v2.runtime_config_activation", "portfolio_id", "portfolio_ids"),
        ("paper_v2.config_change_audit", "paper_v2.config_change_audit", "portfolio_id", "portfolio_ids"),
        ("paper_v2.reset_audit", "paper_v2.reset_audit", "portfolio_id", "portfolio_ids"),
        ("paper_v2.errors", "paper_v2.errors", "portfolio_id", "portfolio_ids"),
        ("paper_v2.broker_account_binding", "paper_v2.broker_account_binding", "portfolio_id", "portfolio_ids"),
    ):
        counts[label] = _count_any(cur, table, column, candidates[ids_key])
    counts["paper_v2.runtime_profile_version"] = _count_any(
        cur, "paper_v2.runtime_profile_version", "profile_id", candidates["profile_ids"]
    )
    counts["selection.paper_portfolio_link"] = _count_any(
        cur, "selection.paper_portfolio_link", "portfolio_id", candidates["portfolio_ids"]
    )
    counts["qmt_strategy.execution_parent_benchmark"] = (
        _count_any(
            cur,
            "qmt_strategy.execution_parent_benchmark",
            "package_id",
            _obsolete_package_ids(cur, request.keep_package_ids),
        )
        if _table_exists(cur, "qmt_strategy.execution_parent_benchmark")
        else 0
    )
    counts["qmt_strategy.execution_planning_subject"] = (
        _count_any(
            cur,
            "qmt_strategy.execution_planning_subject",
            "package_id",
            _obsolete_package_ids(cur, request.keep_package_ids),
        )
        if _table_exists(cur, "qmt_strategy.execution_planning_subject")
        else 0
    )
    plan: dict[str, Any] = {
        "schema_version": "aistock_simulation_history_cleanup_plan_v1",
        "database_identity": identity,
        "core_schema_presence": schema_presence,
        "keep_package_ids": list(request.keep_package_ids),
        "keep_package_presence": {"found": list(found), "missing": missing},
        "miniqmt_cutoff_exclusive": request.miniqmt_cutoff.isoformat(),
        "candidate_ids": {
            **candidates,
            "miniqmt_runtime_ids": k2_ids,
            "qmt_strategy_ids": qmt_strategy_ids,
            "qmt_intent_ids": qmt_intent_ids,
            "runtime_release_ids": release_ids,
        },
        "protected": {
            "miniqmt_runtime_ids": k2_protected,
            "obsolete_runtime_releases_retained_for_evidence_or_ancestry": evidence_retained,
            "strategy_package_definitions": "all",
            "selection_evidence": "all",
            "external_miniqmt_broker_history": "out_of_scope",
        },
        "protected_counts": _protected_counts(cur, request.keep_package_ids),
        "delete_counts": dict(sorted(counts.items())),
    }
    plan["plan_sha256"] = canonical_plan_sha256(plan)
    return plan


def _obsolete_package_ids(cur: Any, keep: Sequence[str]) -> list[str]:
    return _fetch_ids(cur, "SELECT package_id FROM strategy_pkg.package WHERE NOT(package_id=ANY(%s))", (list(keep),))


def _protected_counts(cur: Any, keep: Sequence[str]) -> dict[str, int]:
    result = {
        "strategy_pkg.package_all": _count(cur, "SELECT count(*) FROM strategy_pkg.package"),
        "selection.daily_selection_evidence_all": _count(
            cur, "SELECT count(*) FROM selection.daily_selection_evidence"
        ),
    }
    for table in (
        "paper_v2.portfolio",
        "paper_v2.simulation_release_binding",
        "paper_v2.simulation_daily_run",
        "paper_v2.execution_plan",
        "strategy_pkg.strategy_runtime_release",
        "qmt_strategy.strategy_package_binding",
    ):
        result[f"{table}_keep_packages"] = (
            _count(cur, f"SELECT count(*) FROM {table} WHERE package_id=ANY(%s)", (list(keep),))
            if _table_exists(cur, table)
            else 0
        )
    return dict(sorted(result.items()))


def _lock_cleanup_tables(cur: Any) -> None:
    present = [table for table in sorted(MUTATION_TABLES) if _table_exists(cur, table)]
    if not present:
        raise CleanupSafetyError("no cleanup mutation tables are present")
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


def apply_cleanup_plan(
    cur: Any,
    request: CleanupRequest,
    expected_plan_sha256: str,
    *,
    require_keep_packages: bool = True,
) -> tuple[dict[str, Any], dict[str, int]]:
    cur.execute("SET LOCAL lock_timeout='5s'")
    cur.execute("SET LOCAL statement_timeout='5min'")
    cur.execute("SELECT pg_advisory_xact_lock(%s)", (ADVISORY_LOCK_KEY,))
    _lock_cleanup_tables(cur)
    plan = build_cleanup_plan(cur, request, require_keep_packages=require_keep_packages)
    if plan["plan_sha256"] != expected_plan_sha256:
        raise CleanupSafetyError(f"plan digest changed: expected={expected_plan_sha256} observed={plan['plan_sha256']}")
    ids = plan["candidate_ids"]
    counts: dict[str, int] = {}

    # K2 early-history subset. Preflight rejected every later descendant table.
    for table in LEGACY_K2_TABLES:
        qualified = f"qmt_strategy.{table}"
        if _table_exists(cur, qualified):
            counts[qualified] = _delete_any(cur, qualified, "runtime_id", ids["miniqmt_runtime_ids"])

    obsolete_packages = _obsolete_package_ids(cur, request.keep_package_ids)
    for table in (
        "qmt_strategy.execution_tca_result_mark",
        "qmt_strategy.execution_tca_result_trade_observation",
        "qmt_strategy.execution_tca_receipt_result",
        "qmt_strategy.execution_parent_tca",
        "qmt_strategy.execution_tca_mark",
        "qmt_strategy.execution_tca_trade_conflict",
        "qmt_strategy.execution_tca_trade_observation",
    ):
        # Current cleanup candidates have no TCA descendants; their presence was
        # rejected during planning. These tables are intentionally untouched.
        counts.setdefault(table, 0)
    for table in ("qmt_strategy.execution_parent_benchmark", "qmt_strategy.execution_planning_subject"):
        if _table_exists(cur, table):
            counts[table] = _delete_any(cur, table, "package_id", obsolete_packages)

    counts["paper_v2.simulation_daily_run"] = _delete_any(
        cur, "paper_v2.simulation_daily_run", "run_id", ids["simulation_run_ids"]
    )
    counts["paper_v2.execution_plan"] = _delete_any(
        cur, "paper_v2.execution_plan", "plan_id", ids["execution_plan_ids"]
    )
    counts["paper_v2.simulation_release_binding"] = _delete_any(
        cur, "paper_v2.simulation_release_binding", "binding_id", ids["binding_ids"]
    )
    counts["qmt_strategy.order_status_event"] = _delete_any(
        cur, "qmt_strategy.order_status_event", "intent_id", ids["qmt_intent_ids"]
    )
    for table in (
        "cash_ledger",
        "daily_snapshot",
        "order_ledger",
        "position_lot",
        "reconciliation_issue",
        "trade_ledger",
    ):
        counts[f"qmt_strategy.{table}"] = _delete_any(
            cur, f"qmt_strategy.{table}", "strategy_id", ids["qmt_strategy_ids"]
        )
    counts["qmt_strategy.strategy_package_binding"] = _delete_any(
        cur, "qmt_strategy.strategy_package_binding", "binding_id", ids["qmt_binding_ids"]
    )
    counts["qmt_strategy.order_intent"] = _delete_any(
        cur, "qmt_strategy.order_intent", "strategy_id", ids["qmt_strategy_ids"]
    )
    counts["qmt_strategy.order_batch"] = _delete_any(
        cur, "qmt_strategy.order_batch", "strategy_id", ids["qmt_strategy_ids"]
    )
    counts["qmt_strategy.virtual_account"] = _delete_any(
        cur, "qmt_strategy.virtual_account", "strategy_id", ids["qmt_strategy_ids"]
    )

    run_ids, session_ids, portfolio_ids, profile_ids = (
        ids["paper_run_ids"],
        ids["session_ids"],
        ids["portfolio_ids"],
        ids["profile_ids"],
    )
    counts["paper_v2.order_execution_state_by_run"] = _delete_any(
        cur, "paper_v2.order_execution_state", "run_id", run_ids
    )
    counts["paper_v2.order_execution_state_by_session"] = _delete_any(
        cur, "paper_v2.order_execution_state", "session_id", session_ids
    )
    counts["paper_v2.session_events"] = _delete_any(cur, "paper_v2.session_events", "session_id", session_ids)
    counts["paper_v2.intraday_snapshots_by_run"] = _delete_any(cur, "paper_v2.intraday_snapshots", "run_id", run_ids)
    counts["paper_v2.intraday_snapshots_by_portfolio"] = _delete_any(
        cur, "paper_v2.intraday_snapshots", "portfolio_id", portfolio_ids
    )
    counts["paper_v2.session_day"] = _delete_any(cur, "paper_v2.session_day", "portfolio_id", portfolio_ids)
    counts["paper_v2.trade_session"] = _delete_any(cur, "paper_v2.trade_session", "session_id", session_ids)
    for label, table, _ in PAPER_COUNT_SPECS[2:]:
        counts[label] = _delete_any(cur, table, "run_id", run_ids)
    counts["paper_v2.run"] = _delete_any(cur, "paper_v2.run", "run_id", run_ids)
    counts["selection.paper_portfolio_link"] = _delete_any(
        cur, "selection.paper_portfolio_link", "portfolio_id", portfolio_ids
    )
    for table in (
        "execution_policy_activation",
        "runtime_config_activation",
        "config_change_audit",
        "reset_audit",
        "errors",
        "broker_account_binding",
    ):
        counts[f"paper_v2.{table}"] = _delete_any(cur, f"paper_v2.{table}", "portfolio_id", portfolio_ids)
    counts["paper_v2.runtime_profile_version"] = _delete_any(
        cur, "paper_v2.runtime_profile_version", "profile_id", profile_ids
    )
    counts["paper_v2.runtime_profile"] = _delete_any(cur, "paper_v2.runtime_profile", "profile_id", profile_ids)
    counts["paper_v2.portfolio"] = _delete_any(cur, "paper_v2.portfolio", "portfolio_id", portfolio_ids)
    counts["strategy_pkg.strategy_runtime_release"] = _delete_any(
        cur, "strategy_pkg.strategy_runtime_release", "release_id", ids["runtime_release_ids"]
    )

    for key, expected in plan["delete_counts"].items():
        if key in counts and counts[key] != expected:
            raise CleanupSafetyError(f"delete count mismatch for {key}: expected={expected} actual={counts[key]}")
    readback = build_cleanup_plan(cur, request, require_keep_packages=require_keep_packages)
    remaining = {key: value for key, value in readback["delete_counts"].items() if value}
    if remaining:
        raise CleanupSafetyError(f"cleanup readback still has candidates: {remaining}")
    if readback["protected_counts"] != plan["protected_counts"]:
        raise CleanupSafetyError(
            f"protected counts changed: before={plan['protected_counts']} after={readback['protected_counts']}"
        )
    if readback["protected"]["miniqmt_runtime_ids"] != plan["protected"]["miniqmt_runtime_ids"]:
        raise CleanupSafetyError("protected MiniQMT runtime IDs changed during cleanup")
    return plan, dict(sorted(counts.items()))
