#!/usr/bin/env python3
"""Dry-run, DEV rollback-validation, and governed production cleanup CLI."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import Any

import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.simulation_history_cleanup_core import (  # noqa: E402
    CleanupRequest,
    CleanupSafetyError,
    apply_cleanup_plan,
    build_cleanup_plan,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", choices=("dev", "production"), required=True)
    parser.add_argument("--keep-package-id", action="append", required=True)
    parser.add_argument("--miniqmt-cutoff", type=date.fromisoformat, required=True, help="exclusive YYYY-MM-DD cutoff")
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true")
    mode.add_argument("--validate-rollback", action="store_true")
    parser.add_argument("--expected-plan-sha256")
    parser.add_argument("--expected-source-commit")
    parser.add_argument("--authorization")
    parser.add_argument("--confirm-production", action="store_true")
    parser.add_argument("--receipt", type=Path)
    parser.add_argument(
        "--dev-k2-fixture",
        action="store_true",
        help="transactionally create the missing minimal K2 graph in DEV; always rolled back",
    )
    return parser


def _connect(target: str, env_file: Path) -> Any:
    if not env_file.is_file():
        raise CleanupSafetyError(f"environment file does not exist: {env_file}")
    load_dotenv(env_file, override=False)
    prefix = "TDX_DB_DEV_" if target == "dev" else "TDX_DB_"
    values = {
        name: os.getenv(prefix + env)
        for name, env in (
            ("host", "HOST"),
            ("port", "PORT"),
            ("dbname", "NAME"),
            ("user", "USER"),
            ("password", "PASSWORD"),
        )
    }
    missing = [name for name, value in values.items() if not value]
    if missing:
        raise CleanupSafetyError(f"missing {target} database settings: {missing}")
    return psycopg2.connect(**values)


def _write_receipt(payload: dict[str, Any], path: Path | None) -> None:
    rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str)
    print(rendered)
    if path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(rendered + "\n", encoding="utf-8")


def _install_dev_k2_fixture(cur: Any, cutoff: date) -> None:
    tables = (
        "qmt_strategy.strategy_package_binding",
        "qmt_strategy.execution_runtime",
        "qmt_strategy.execution_runtime_event",
        "qmt_strategy.execution_algo_instance",
        "qmt_strategy.execution_child_order",
        "qmt_strategy.virtual_account",
        "qmt_strategy.order_batch",
        "qmt_strategy.order_intent",
        "qmt_strategy.order_status_event",
        "qmt_strategy.cash_ledger",
        "qmt_strategy.order_ledger",
        "qmt_strategy.trade_ledger",
        "qmt_strategy.position_lot",
        "qmt_strategy.daily_snapshot",
        "qmt_strategy.reconciliation_issue",
    )
    cur.execute("SELECT to_regclass(%s)", (tables[0],))
    if any(_table_is_present(cur, table) for table in tables):
        raise CleanupSafetyError("--dev-k2-fixture requires all fixture target tables to be absent")
    cur.execute(
        """
        CREATE SCHEMA IF NOT EXISTS qmt_strategy;
        CREATE TABLE qmt_strategy.strategy_package_binding (
          binding_id text PRIMARY KEY, strategy_id text NOT NULL, package_id text NOT NULL
        );
        CREATE TABLE qmt_strategy.virtual_account (strategy_id text PRIMARY KEY);
        CREATE TABLE qmt_strategy.order_batch (
          batch_id text PRIMARY KEY,
          strategy_id text NOT NULL REFERENCES qmt_strategy.virtual_account(strategy_id)
        );
        CREATE TABLE qmt_strategy.order_intent (
          intent_id text PRIMARY KEY,
          batch_id text NOT NULL REFERENCES qmt_strategy.order_batch(batch_id),
          strategy_id text NOT NULL REFERENCES qmt_strategy.virtual_account(strategy_id)
        );
        CREATE TABLE qmt_strategy.order_status_event (
          event_id text PRIMARY KEY,
          intent_id text NOT NULL REFERENCES qmt_strategy.order_intent(intent_id)
        );
        CREATE TABLE qmt_strategy.cash_ledger (
          cash_id text PRIMARY KEY,
          strategy_id text NOT NULL REFERENCES qmt_strategy.virtual_account(strategy_id),
          intent_id text REFERENCES qmt_strategy.order_intent(intent_id)
        );
        CREATE TABLE qmt_strategy.order_ledger (
          order_id text PRIMARY KEY,
          strategy_id text NOT NULL REFERENCES qmt_strategy.virtual_account(strategy_id),
          intent_id text NOT NULL REFERENCES qmt_strategy.order_intent(intent_id)
        );
        CREATE TABLE qmt_strategy.trade_ledger (
          trade_id text PRIMARY KEY,
          strategy_id text NOT NULL REFERENCES qmt_strategy.virtual_account(strategy_id),
          intent_id text NOT NULL REFERENCES qmt_strategy.order_intent(intent_id)
        );
        CREATE TABLE qmt_strategy.position_lot (
          lot_id text PRIMARY KEY,
          strategy_id text NOT NULL REFERENCES qmt_strategy.virtual_account(strategy_id)
        );
        CREATE TABLE qmt_strategy.daily_snapshot (
          snapshot_id text PRIMARY KEY,
          strategy_id text NOT NULL REFERENCES qmt_strategy.virtual_account(strategy_id)
        );
        CREATE TABLE qmt_strategy.reconciliation_issue (
          issue_id text PRIMARY KEY,
          strategy_id text NOT NULL REFERENCES qmt_strategy.virtual_account(strategy_id)
        );
        CREATE TABLE qmt_strategy.execution_runtime (
          runtime_id text PRIMARY KEY, mode text NOT NULL, trade_date date NOT NULL,
          metadata jsonb NOT NULL DEFAULT '{}'::jsonb
        );
        CREATE TABLE qmt_strategy.execution_runtime_event (
          event_id text PRIMARY KEY,
          runtime_id text NOT NULL REFERENCES qmt_strategy.execution_runtime(runtime_id)
        );
        CREATE TABLE qmt_strategy.execution_algo_instance (
          algo_instance_id text PRIMARY KEY,
          runtime_id text NOT NULL REFERENCES qmt_strategy.execution_runtime(runtime_id)
        );
        CREATE TABLE qmt_strategy.execution_child_order (
          child_order_id text PRIMARY KEY,
          runtime_id text NOT NULL REFERENCES qmt_strategy.execution_runtime(runtime_id),
          algo_instance_id text NOT NULL REFERENCES qmt_strategy.execution_algo_instance(algo_instance_id)
        );
        """
    )
    old_day = cutoff.fromordinal(cutoff.toordinal() - 1)
    cur.execute(
        "INSERT INTO qmt_strategy.virtual_account(strategy_id) VALUES ('bug1099_old_strategy');"
        "INSERT INTO qmt_strategy.strategy_package_binding(binding_id,strategy_id,package_id) VALUES "
        "('bug1099_old_binding','bug1099_old_strategy','bug1099_old_package');"
        "INSERT INTO qmt_strategy.order_batch(batch_id,strategy_id) VALUES "
        "('bug1099_old_batch','bug1099_old_strategy');"
        "INSERT INTO qmt_strategy.order_intent(intent_id,batch_id,strategy_id) VALUES "
        "('bug1099_old_intent','bug1099_old_batch','bug1099_old_strategy');"
        "INSERT INTO qmt_strategy.order_status_event(event_id,intent_id) VALUES "
        "('bug1099_old_status','bug1099_old_intent');"
        "INSERT INTO qmt_strategy.cash_ledger(cash_id,strategy_id,intent_id) VALUES "
        "('bug1099_old_cash','bug1099_old_strategy','bug1099_old_intent');"
        "INSERT INTO qmt_strategy.order_ledger(order_id,strategy_id,intent_id) VALUES "
        "('bug1099_old_order','bug1099_old_strategy','bug1099_old_intent');"
        "INSERT INTO qmt_strategy.trade_ledger(trade_id,strategy_id,intent_id) VALUES "
        "('bug1099_old_trade','bug1099_old_strategy','bug1099_old_intent');"
        "INSERT INTO qmt_strategy.position_lot(lot_id,strategy_id) VALUES "
        "('bug1099_old_lot','bug1099_old_strategy');"
        "INSERT INTO qmt_strategy.daily_snapshot(snapshot_id,strategy_id) VALUES "
        "('bug1099_old_snapshot','bug1099_old_strategy');"
        "INSERT INTO qmt_strategy.reconciliation_issue(issue_id,strategy_id) VALUES "
        "('bug1099_old_issue','bug1099_old_strategy')"
    )
    cur.execute(
        "INSERT INTO qmt_strategy.execution_runtime(runtime_id,mode,trade_date,metadata) VALUES "
        "('bug1099_old_runtime','SIM',%s,'{}'),('bug1099_cutoff_runtime','SIM',%s,'{}')",
        (old_day, cutoff),
    )
    cur.execute(
        "INSERT INTO qmt_strategy.execution_runtime_event(event_id,runtime_id) VALUES "
        "('bug1099_old_event','bug1099_old_runtime'),('bug1099_keep_event','bug1099_cutoff_runtime')"
    )
    cur.execute(
        "INSERT INTO qmt_strategy.execution_algo_instance(algo_instance_id,runtime_id) VALUES "
        "('bug1099_old_algo','bug1099_old_runtime'),('bug1099_keep_algo','bug1099_cutoff_runtime')"
    )
    cur.execute(
        "INSERT INTO qmt_strategy.execution_child_order(child_order_id,runtime_id,algo_instance_id) VALUES "
        "('bug1099_old_child','bug1099_old_runtime','bug1099_old_algo'),"
        "('bug1099_keep_child','bug1099_cutoff_runtime','bug1099_keep_algo')"
    )


def _table_is_present(cur: Any, table: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (table,))
    return cur.fetchone()[0] is not None


def _verify_immutable_source(expected_commit: str | None) -> str:
    if not expected_commit:
        raise CleanupSafetyError("production apply requires --expected-source-commit")
    current = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, check=True, capture_output=True, text=True
    ).stdout.strip()
    if current != expected_commit:
        raise CleanupSafetyError(f"source commit mismatch: expected={expected_commit} observed={current}")
    tracked_paths = (
        "scripts/simulation_history_cleanup_core.py",
        "scripts/cleanup_simulation_history.py",
    )
    dirty = subprocess.run(["git", "diff", "--quiet", "HEAD", "--", *tracked_paths], cwd=ROOT, check=False)
    if dirty.returncode != 0:
        raise CleanupSafetyError("production cleanup source files differ from the immutable commit")
    merged = subprocess.run(["git", "merge-base", "--is-ancestor", current, "origin/main"], cwd=ROOT, check=False)
    if merged.returncode != 0:
        raise CleanupSafetyError(f"source commit is not merged into origin/main: {current}")
    return current


def _post_commit_readback(
    args: argparse.Namespace, request: CleanupRequest, before_plan: dict[str, Any]
) -> dict[str, Any]:
    conn = _connect("production", args.env_file)
    try:
        conn.set_session(readonly=True)
        with conn.cursor() as cur:
            after = build_cleanup_plan(cur, request)
        conn.rollback()
    finally:
        conn.close()
    remaining = {key: value for key, value in after["delete_counts"].items() if value}
    protected_counts_match = after["protected_counts"] == before_plan["protected_counts"]
    protected_runtimes_match = (
        after["protected"]["miniqmt_runtime_ids"] == before_plan["protected"]["miniqmt_runtime_ids"]
    )
    return {
        "status": "passed" if not remaining and protected_counts_match and protected_runtimes_match else "failed",
        "remaining_delete_counts": remaining,
        "protected_counts_match": protected_counts_match,
        "protected_miniqmt_runtime_ids_match": protected_runtimes_match,
        "database_identity": after["database_identity"],
    }


def main() -> int:
    args = _parser().parse_args()
    request = CleanupRequest.build(args.keep_package_id, args.miniqmt_cutoff)
    if args.apply:
        if args.target != "production":
            raise CleanupSafetyError("--apply is reserved for the production target; use --validate-rollback for DEV")
        if not args.confirm_production or args.authorization != "BUG-1099":
            raise CleanupSafetyError("production apply requires --confirm-production --authorization BUG-1099")
        if not args.expected_plan_sha256:
            raise CleanupSafetyError("production apply requires --expected-plan-sha256 from the merged-code dry-run")
        source_commit = _verify_immutable_source(args.expected_source_commit)
    else:
        source_commit = None
    if args.validate_rollback and args.target != "dev":
        raise CleanupSafetyError("--validate-rollback is only permitted for DEV")
    if args.dev_k2_fixture and not (args.target == "dev" and args.validate_rollback):
        raise CleanupSafetyError("--dev-k2-fixture is only valid with DEV --validate-rollback")

    conn = _connect(args.target, args.env_file)
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            if args.apply or args.validate_rollback:
                if args.dev_k2_fixture:
                    _install_dev_k2_fixture(cur, request.miniqmt_cutoff)
                require_keep_packages = args.target == "production"
                expected = (
                    args.expected_plan_sha256
                    or build_cleanup_plan(cur, request, require_keep_packages=require_keep_packages)["plan_sha256"]
                )
                plan, deleted = apply_cleanup_plan(
                    cur,
                    request,
                    expected,
                    require_keep_packages=require_keep_packages,
                )
                payload = {
                    "schema_version": "aistock_simulation_history_cleanup_receipt_v1",
                    "target": args.target,
                    "mode": "production_apply" if args.apply else "dev_validate_rollback",
                    "plan": plan,
                    "deleted_counts": deleted,
                    "transaction": "committed" if args.apply else "rolled_back",
                    "dev_k2_fixture": bool(args.dev_k2_fixture),
                    "source_commit": source_commit,
                }
                if args.apply:
                    conn.commit()
                    payload["post_commit_readback"] = _post_commit_readback(args, request, plan)
                else:
                    conn.rollback()
            else:
                conn.set_session(readonly=True)
                with conn.cursor() as readonly_cur:
                    plan = build_cleanup_plan(readonly_cur, request)
                conn.rollback()
                payload = {
                    "schema_version": "aistock_simulation_history_cleanup_receipt_v1",
                    "target": args.target,
                    "mode": "dry_run",
                    "plan": plan,
                    "transaction": "read_only_rolled_back",
                }
        _write_receipt(payload, args.receipt)
        if args.apply and payload["post_commit_readback"]["status"] != "passed":
            return 3
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except CleanupSafetyError as exc:
        print(json.dumps({"status": "blocked", "reason": str(exc)}, ensure_ascii=False), file=sys.stderr)
        raise SystemExit(2)
