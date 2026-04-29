from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@dataclass
class CheckResult:
    name: str
    status: str
    message: str
    context: dict[str, Any] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return self.status in {"PASS", "WARN"}


class SmokeFailure(RuntimeError):
    pass


def _load_dotenv() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _json_safe(value: Any) -> Any:
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


class DataQualitySmoke:
    def __init__(
        self,
        *,
        max_recent_runs: int = 80,
        since_hours: int | None = None,
        portfolio_name_prefix: str | None = None,
        portfolio_ids: list[str] | None = None,
        strict_history: bool = False,
    ) -> None:
        _load_dotenv()
        from backend.db.pg_pool import get_conn

        self._get_conn = get_conn
        self.max_recent_runs = max_recent_runs
        self.since_hours = since_hours
        self.portfolio_name_prefix = portfolio_name_prefix
        self.portfolio_ids = portfolio_ids or []
        self.strict_history = strict_history
        self.results: list[CheckResult] = []

    def run(self) -> list[CheckResult]:
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                self._check_required_tables(cur)
                latest_trading_day = self._latest_trading_day(cur)
                previous_trading_day = self._previous_trading_day(cur, latest_trading_day)
                self._check_dataset_audit(cur, latest_trading_day, previous_trading_day)
                self._check_strategy_packages(cur)
                self._check_selection_runs(cur)
                self._check_paper_v2_runs(cur)
                self._check_paper_v2_ledger_consistency(cur)
        return self.results

    def _pass(self, name: str, message: str, context: dict[str, Any] | None = None) -> None:
        self.results.append(CheckResult(name=name, status="PASS", message=message, context=context or {}))

    def _fail(self, name: str, message: str, context: dict[str, Any] | None = None) -> None:
        self.results.append(CheckResult(name=name, status="FAIL", message=message, context=context or {}))

    def _warn(self, name: str, message: str, context: dict[str, Any] | None = None) -> None:
        self.results.append(CheckResult(name=name, status="WARN", message=message, context=context or {}))

    def _query_one(self, cur: Any, sql: str, params: tuple[Any, ...] = ()) -> Any:
        cur.execute(sql, params)
        row = cur.fetchone()
        return row[0] if row else None

    def _paper_run_filter_sql(self, run_alias: str = "r", portfolio_alias: str = "p") -> tuple[str, list[Any]]:
        filters = [f"{run_alias}.status = 'SUCCEEDED'"]
        params: list[Any] = []
        if self.since_hours is not None:
            filters.append(f"{run_alias}.started_at >= now() - %s::interval")
            params.append(f"{self.since_hours} hours")
        if self.portfolio_name_prefix:
            filters.append(f"{portfolio_alias}.portfolio_name LIKE %s")
            params.append(f"{self.portfolio_name_prefix}%")
        if self.portfolio_ids:
            filters.append(f"{portfolio_alias}.portfolio_id = ANY(%s)")
            params.append(self.portfolio_ids)
        return " AND ".join(filters), params

    def _run_scope_context(self) -> dict[str, Any]:
        return {
            "max_recent_runs": self.max_recent_runs,
            "since_hours": self.since_hours,
            "portfolio_name_prefix": self.portfolio_name_prefix,
            "portfolio_ids": self.portfolio_ids,
            "strict_history": self.strict_history,
        }

    def _is_strict_run_scope(self) -> bool:
        return self.strict_history or bool(self.portfolio_name_prefix) or bool(self.portfolio_ids)

    def _check_required_tables(self, cur: Any) -> None:
        required = [
            ("market", "dataset_date_refresh_audit"),
            ("market", "trading_calendar"),
            ("strategy_pkg", "package"),
            ("strategy_pkg", "selection_score_artifact"),
            ("strategy_pkg", "validated_execution_policy"),
            ("selection", "run"),
            ("selection", "package_result"),
            ("selection", "aggregate_result"),
            ("selection", "excluded_result"),
            ("paper_v2", "portfolio"),
            ("paper_v2", "run"),
            ("paper_v2", "orders"),
            ("paper_v2", "fills"),
            ("paper_v2", "cash_ledger"),
            ("paper_v2", "positions"),
            ("paper_v2", "daily_snapshots"),
            ("paper_v2", "errors"),
            ("paper_v2", "trade_session"),
            ("paper_v2", "session_day"),
            ("paper_v2", "intraday_snapshots"),
            ("paper_v2", "order_execution_state"),
        ]
        cur.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema = ANY(%s)
            """,
            (sorted({schema for schema, _ in required}),),
        )
        present = {(schema, table) for schema, table in cur.fetchall()}
        missing = [f"{schema}.{table}" for schema, table in required if (schema, table) not in present]
        if missing:
            self._fail("schema_required_tables", "required Paper v2/Selection tables are missing", {"missing": missing})
        else:
            self._pass("schema_required_tables", "required Paper v2/Selection tables exist", {"table_count": len(required)})

    def _latest_trading_day(self, cur: Any) -> date:
        latest = self._query_one(
            cur,
            "SELECT max(cal_date) FROM market.trading_calendar WHERE is_trading = true AND cal_date <= current_date",
        )
        if latest is None:
            self._fail("trading_calendar_latest", "no completed trading day found in market.trading_calendar")
            raise SmokeFailure("no completed trading day found")
        self._pass("trading_calendar_latest", "latest completed trading day resolved", {"latest_trading_day": latest})
        return latest

    def _previous_trading_day(self, cur: Any, latest_trading_day: date) -> date:
        previous = self._query_one(
            cur,
            "SELECT max(cal_date) FROM market.trading_calendar WHERE is_trading = true AND cal_date < %s",
            (latest_trading_day,),
        )
        if previous is None:
            self._fail("trading_calendar_previous", "no previous trading day found", {"latest_trading_day": latest_trading_day})
            raise SmokeFailure("no previous trading day found")
        self._pass("trading_calendar_previous", "previous trading day resolved", {"previous_trading_day": previous})
        return previous

    def _check_dataset_audit(self, cur: Any, latest_trading_day: date, previous_trading_day: date) -> None:
        requirements = {
            "suspend_d": latest_trading_day,
            "stk_limit": latest_trading_day,
            "kline_daily_raw": previous_trading_day,
            "daily_basic": previous_trading_day,
            "stock_moneyflow_ts": previous_trading_day,
            "sector_data": previous_trading_day,
            "index_daily": previous_trading_day,
        }
        failures: list[dict[str, Any]] = []
        rows: dict[str, dict[str, Any]] = {}
        for dataset, min_date in requirements.items():
            cur.execute(
                """
                SELECT trade_date, refreshed_at, row_count
                FROM market.dataset_date_refresh_audit
                WHERE dataset = %s AND status = 'success'
                ORDER BY trade_date DESC, refreshed_at DESC
                LIMIT 1
                """,
                (dataset,),
            )
            row = cur.fetchone()
            latest_success, refreshed_at, row_count = row if row else (None, None, None)
            rows[dataset] = {
                "latest_success": latest_success,
                "min_required_date": min_date,
                "refreshed_at": refreshed_at.isoformat() if refreshed_at else None,
                "row_count_at_latest": row_count,
            }
            if latest_success is None or latest_success < min_date:
                failures.append({"dataset": dataset, **rows[dataset]})
        if failures:
            self._fail("dataset_refresh_audit", "required datasets are not fresh enough for Paper v2/Selection smoke", {"failures": failures, "rows": rows})
        else:
            self._pass("dataset_refresh_audit", "required dataset audit rows are fresh enough", {"rows": rows})

    def _check_strategy_packages(self, cur: Any) -> None:
        cur.execute(
            """
            SELECT
              count(*) FILTER (WHERE package_status IN ('SELECTION_ENABLED', 'PAPER_ENABLED', 'PAPER_RUNNING', 'PAPER_PASSED')) AS usable_packages,
              count(*) FILTER (WHERE manifest_sha256 IS NULL OR manifest_json IS NULL) AS missing_manifest,
              count(*) AS total_packages
            FROM strategy_pkg.package
            """
        )
        usable, missing_manifest, total = cur.fetchone()
        cur.execute(
            """
            SELECT count(*)
            FROM strategy_pkg.validated_execution_policy
            WHERE paper_enabled = true AND validation_status = 'BACKTEST_VALIDATED'
            """
        )
        policies = cur.fetchone()[0]
        context = {"usable_packages": usable, "missing_manifest": missing_manifest, "total_packages": total, "paper_enabled_validated_policies": policies}
        if usable <= 0 or missing_manifest > 0 or policies <= 0:
            self._fail("strategy_package_readiness", "StrategyPackage catalog is not ready for Paper v2 validation", context)
        else:
            self._pass("strategy_package_readiness", "StrategyPackage catalog has usable packages and validated paper policies", context)

    def _check_selection_runs(self, cur: Any) -> None:
        cur.execute(
            """
            WITH recent AS (
              SELECT run_id, mode, valid_no_candidate
              FROM selection.run
              WHERE status = 'SUCCEEDED'
              ORDER BY completed_at DESC NULLS LAST, created_at DESC
              LIMIT 100
            )
            SELECT
              count(*) AS succeeded_runs,
              count(*) FILTER (
                WHERE NOT valid_no_candidate
                  AND NOT EXISTS (SELECT 1 FROM selection.package_result pr WHERE pr.run_id = recent.run_id)
                  AND NOT EXISTS (SELECT 1 FROM selection.aggregate_result ar WHERE ar.run_id = recent.run_id)
              ) AS missing_results
            FROM recent
            """
        )
        succeeded, missing_results = cur.fetchone()
        if succeeded <= 0 or missing_results > 0:
            self._fail("selection_result_traceability", "successful selection runs must have persisted results or explicit valid_no_candidate", {"succeeded_runs": succeeded, "missing_results": missing_results})
        else:
            self._pass("selection_result_traceability", "successful selection runs have persisted result trace", {"sampled_succeeded_runs": succeeded})

    def _check_paper_v2_runs(self, cur: Any) -> None:
        filter_sql, params = self._paper_run_filter_sql("r", "p")
        cur.execute(
            f"""
            WITH recent AS (
              SELECT r.run_id
              FROM paper_v2.run r
              JOIN paper_v2.portfolio p ON p.portfolio_id = r.portfolio_id
              WHERE {filter_sql}
              ORDER BY r.completed_at DESC NULLS LAST, r.started_at DESC
              LIMIT %s
            )
            SELECT
              count(*) AS succeeded_runs,
              count(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM paper_v2.daily_snapshots ds WHERE ds.run_id = recent.run_id)) AS missing_snapshot,
              count(*) FILTER (
                WHERE NOT EXISTS (
                  SELECT 1 FROM paper_v2.run_events ev
                  WHERE ev.run_id = recent.run_id
                    AND ev.event_type IN ('RUN_SUCCEEDED', 'NO_REBALANCE_REQUIRED')
                )
                AND NOT EXISTS (
                  SELECT 1 FROM paper_v2.session_events sev
                  WHERE sev.run_id = recent.run_id
                    AND sev.event_type IN (
                      'SESSION_REPLAY_SUCCEEDED',
                      'SESSION_CATCHUP_REPLAY_SUCCEEDED',
                      'LIVE_DAY_FINALIZED',
                      'NO_REBALANCE_REQUIRED'
                    )
                )
              ) AS missing_success_event
            FROM recent
            """,
            (*params, self.max_recent_runs),
        )
        succeeded, missing_snapshot, missing_success_event = cur.fetchone()
        if succeeded <= 0 or missing_snapshot > 0 or missing_success_event > 0:
            self._fail("paper_v2_run_traceability", "successful Paper v2 runs must have snapshots and success/no-rebalance events", {
                "sampled_succeeded_runs": succeeded,
                "missing_snapshot": missing_snapshot,
                "missing_success_event": missing_success_event,
                **self._run_scope_context(),
            })
        else:
            self._pass("paper_v2_run_traceability", "successful Paper v2 runs have snapshot and event trace", {"sampled_succeeded_runs": succeeded, **self._run_scope_context()})

    def _check_paper_v2_ledger_consistency(self, cur: Any) -> None:
        filter_sql, params = self._paper_run_filter_sql("r", "p")
        checks: dict[str, Any] = {}
        cur.execute(
            f"""
            WITH recent AS (
              SELECT r.run_id FROM paper_v2.run r
              JOIN paper_v2.portfolio p ON p.portfolio_id = r.portfolio_id
              WHERE {filter_sql}
              ORDER BY r.completed_at DESC NULLS LAST, r.started_at DESC
              LIMIT %s
            ),
            fill_sum AS (
              SELECT order_id, sum(quantity)::int AS fill_qty
              FROM paper_v2.fills
              WHERE run_id IN (SELECT run_id FROM recent)
              GROUP BY order_id
            )
            SELECT count(*)
            FROM paper_v2.orders o
            LEFT JOIN fill_sum f ON f.order_id = o.order_id
            WHERE o.run_id IN (SELECT run_id FROM recent)
              AND o.filled_quantity <> COALESCE(f.fill_qty, 0)
            """,
            (*params, self.max_recent_runs),
        )
        checks["order_fill_quantity_mismatches"] = cur.fetchone()[0]
        cur.execute(
            f"""
            WITH recent AS (
              SELECT r.run_id FROM paper_v2.run r
              JOIN paper_v2.portfolio p ON p.portfolio_id = r.portfolio_id
              WHERE {filter_sql}
              ORDER BY r.completed_at DESC NULLS LAST, r.started_at DESC
              LIMIT %s
            )
            SELECT count(*)
            FROM paper_v2.fills f
            LEFT JOIN paper_v2.cash_ledger c ON c.fill_id = f.fill_id
            WHERE f.run_id IN (SELECT run_id FROM recent) AND c.fill_id IS NULL
            """,
            (*params, self.max_recent_runs),
        )
        checks["fills_without_cash_ledger"] = cur.fetchone()[0]
        cur.execute(
            f"""
            WITH recent AS (
              SELECT r.run_id FROM paper_v2.run r
              JOIN paper_v2.portfolio p ON p.portfolio_id = r.portfolio_id
              WHERE {filter_sql}
              ORDER BY r.completed_at DESC NULLS LAST, r.started_at DESC
              LIMIT %s
            )
            SELECT count(*)
            FROM paper_v2.daily_snapshots
            WHERE run_id IN (SELECT run_id FROM recent)
              AND (nav < 0 OR cash < 0 OR market_value < 0 OR abs(nav - cash - market_value) > 0.01)
            """,
            (*params, self.max_recent_runs),
        )
        checks["invalid_daily_snapshots"] = cur.fetchone()[0]
        cur.execute(
            f"""
            WITH recent AS (
              SELECT r.run_id FROM paper_v2.run r
              JOIN paper_v2.portfolio p ON p.portfolio_id = r.portfolio_id
              WHERE {filter_sql}
              ORDER BY r.completed_at DESC NULLS LAST, r.started_at DESC
              LIMIT %s
            )
            SELECT count(*)
            FROM paper_v2.positions
            WHERE run_id IN (SELECT run_id FROM recent)
              AND (
                quantity < 0 OR available_quantity < 0 OR market_price < 0 OR market_value < 0
                OR abs(market_value - quantity * market_price) > 0.05
              )
            """,
            (*params, self.max_recent_runs),
        )
        checks["invalid_positions"] = cur.fetchone()[0]
        has_violations = any(value > 0 for value in checks.values())
        checks.update(self._run_scope_context())
        if has_violations:
            if self._is_strict_run_scope():
                self._fail("paper_v2_ledger_consistency", "Paper v2 ledger persistence has consistency violations in strict run scope", checks)
            else:
                self._warn("paper_v2_ledger_consistency", "legacy Paper v2 ledger consistency violations exist; use --portfolio-name-prefix/--portfolio-id or --strict-history to fail this gate", checks)
        else:
            self._pass("paper_v2_ledger_consistency", "Paper v2 orders/fills/cash/positions/snapshots are internally consistent", checks)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Read-only AIstock data quality smoke checks.")
    parser.add_argument("--scope", default="paper_v2_selection_center", choices=["paper_v2_selection_center"])
    parser.add_argument("--max-recent-runs", type=int, default=80)
    parser.add_argument("--since-hours", type=int, help="Only inspect Paper v2 runs started within this many hours.")
    parser.add_argument("--portfolio-name-prefix", help="Only inspect Paper v2 runs for portfolios whose name starts with this prefix. Ledger violations are fatal in this scoped mode.")
    parser.add_argument("--portfolio-id", action="append", dest="portfolio_ids", help="Only inspect Paper v2 runs for this portfolio_id; repeatable. Ledger violations are fatal in this scoped mode.")
    parser.add_argument("--strict-history", action="store_true", help="Treat historical ledger consistency warnings as failures.")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    parser.add_argument("--output", help="Optional JSON output path for validation evidence.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    smoke = DataQualitySmoke(
        max_recent_runs=args.max_recent_runs,
        since_hours=args.since_hours,
        portfolio_name_prefix=args.portfolio_name_prefix,
        portfolio_ids=args.portfolio_ids,
        strict_history=args.strict_history,
    )
    results = smoke.run()
    payload = {
        "ok": all(item.passed for item in results),
        "scope": args.scope,
        "warning_count": sum(1 for item in results if item.status == "WARN"),
        "failure_count": sum(1 for item in results if item.status == "FAIL"),
        "results": [asdict(item) | {"context": _json_safe(item.context)} for item in results],
    }
    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        for item in results:
            print(f"{item.status} {item.name}: {item.message} {json.dumps(_json_safe(item.context), ensure_ascii=False)}")
    return 0 if payload["ok"] else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SmokeFailure as exc:
        print(f"FAIL data_quality_smoke: {exc}", file=sys.stderr)
        raise SystemExit(1)
