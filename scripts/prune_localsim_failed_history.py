"""Dry-run, DEV rollback-validate, or apply governed LocalSIM history pruning."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.localsim_failed_history_prune_core import (  # noqa: E402
    LocalSimPruneRequest,
    LocalSimPruneSafetyError,
    apply_prune_plan,
    build_prune_plan,
)


SOURCE_PATHS = (
    "scripts/localsim_failed_history_prune_core.py",
    "scripts/prune_localsim_failed_history.py",
)
FIXTURE_PACKAGE_ID = "pkg_bug1223_localsim_fixture"
FIXTURE_ANCHOR_RUN_ID = "simrun_bug1223_anchor"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", choices=("dev", "production"), required=True)
    parser.add_argument("--package-id", required=True)
    parser.add_argument("--anchor-run-id", required=True)
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--validate-rollback", action="store_true")
    mode.add_argument("--apply", action="store_true")
    parser.add_argument("--dev-fixture", action="store_true")
    parser.add_argument("--expected-plan-sha256")
    parser.add_argument("--expected-source-commit")
    parser.add_argument("--authorization")
    parser.add_argument("--confirm-production", action="store_true")
    parser.add_argument("--receipt", type=Path)
    return parser


def _connect(target: str, env_file: Path) -> Any:
    if not env_file.is_file():
        raise LocalSimPruneSafetyError(f"environment file does not exist: {env_file}")
    load_dotenv(env_file, override=False)
    prefix = "TDX_DB_DEV_" if target == "dev" else "TDX_DB_"
    values = {
        name: os.getenv(prefix + suffix)
        for name, suffix in (
            ("host", "HOST"),
            ("port", "PORT"),
            ("dbname", "NAME"),
            ("user", "USER"),
            ("password", "PASSWORD"),
        )
    }
    missing = [name for name, value in values.items() if not value]
    if missing:
        raise LocalSimPruneSafetyError(f"missing {target} database settings: {missing}")
    return psycopg2.connect(**values)


def _install_dev_fixture(cur: Any, request: LocalSimPruneRequest) -> None:
    if request != LocalSimPruneRequest.build(FIXTURE_PACKAGE_ID, FIXTURE_ANCHOR_RUN_ID):
        raise LocalSimPruneSafetyError(
            f"--dev-fixture requires --package-id {FIXTURE_PACKAGE_ID} --anchor-run-id {FIXTURE_ANCHOR_RUN_ID}"
        )
    cur.execute("SELECT count(*) FROM strategy_pkg.package WHERE package_id=%s", (FIXTURE_PACKAGE_ID,))
    if int(cur.fetchone()[0]):
        raise LocalSimPruneSafetyError("DEV fixture package already exists")

    manifest_sha = "1" * 64
    release_id = "srr_bug1223_fixture"
    release_hash = "2" * 64
    binding_id = "simbind_bug1223_fixture"
    binding_hash = "3" * 64
    portfolio_id = "paper_bug1223_fixture"
    anchor_day = date(2026, 8, 28)
    cur.execute(
        """
        INSERT INTO strategy_pkg.package(
            package_id,package_name,package_version,source_type,source_id,package_status,
            manifest_json,manifest_sha256
        ) VALUES (%s,'BUG-1223 DEV fixture','1','candidate_strategy_package','bug1223','ACTIVE',%s,%s)
        """,
        (
            FIXTURE_PACKAGE_ID,
            psycopg2.extras.Json({"schema_version": "bug1223_fixture_manifest_v1"}),
            manifest_sha,
        ),
    )
    cur.execute(
        """
        INSERT INTO strategy_pkg.strategy_runtime_release(
            release_id,package_id,manifest_sha256,runtime_profile_id,runtime_profile_version_id,
            runtime_profile_sha256,daily_strategy_profile_version_id,execution_policy_version_id,
            execution_policy_sha256,tail_policy_version_id,tail_policy_sha256,release_config_json,
            release_hash,validation_state
        ) VALUES (%s,%s,%s,'bug1223_profile','bug1223_profile_v1',%s,'bug1223_daily_v1',
                  'localsim_twap_only_v1',%s,'bug1223_tail_v1',%s,%s,%s,'SIM_PASSED')
        """,
        (
            release_id,
            FIXTURE_PACKAGE_ID,
            manifest_sha,
            "4" * 64,
            "5" * 64,
            "6" * 64,
            psycopg2.extras.Json({"schema_version": "bug1223_fixture_release_v1"}),
            release_hash,
        ),
    )
    cur.execute(
        """
        INSERT INTO paper_v2.portfolio(
            portfolio_id,portfolio_name,package_id,manifest_sha256,frozen_manifest_json,
            initial_cash,start_date,data_source,broker_backend,fee_policy,risk_policy,execution_policy,status
        ) VALUES (%s,'BUG-1223 DEV fixture',%s,%s,%s,500000,%s,'DB_HISTORICAL','local_sim',%s,%s,%s,'READY')
        """,
        (
            portfolio_id,
            FIXTURE_PACKAGE_ID,
            manifest_sha,
            psycopg2.extras.Json({"package_id": FIXTURE_PACKAGE_ID}),
            anchor_day - timedelta(days=5),
            psycopg2.extras.Json({}),
            psycopg2.extras.Json({}),
            psycopg2.extras.Json({"policy_json": {"algo_code": "TWAP"}}),
        ),
    )
    cur.execute(
        """
        INSERT INTO paper_v2.simulation_release_binding(
            binding_id,strategy_id,release_id,release_hash,package_id,manifest_sha256,
            broker_backend,capital_allocation,approval_state,binding_config_json,binding_hash
        ) VALUES (%s,%s,%s,%s,%s,%s,'local_sim',500000,'SIM_PASSED',%s,%s)
        """,
        (
            binding_id,
            portfolio_id,
            release_id,
            release_hash,
            FIXTURE_PACKAGE_ID,
            manifest_sha,
            psycopg2.extras.Json({"schema_version": "bug1223_fixture_binding_v1"}),
            binding_hash,
        ),
    )

    rows = (
        ("simrun_bug1223_cancelled", anchor_day - timedelta(days=4), "CANCELLED", "plan_bug1223_cancelled"),
        (
            "simrun_bug1223_retryable",
            anchor_day - timedelta(days=3),
            "FAILED_RETRYABLE",
            "plan_bug1223_retryable",
        ),
        (
            "simrun_bug1223_terminal",
            anchor_day - timedelta(days=2),
            "FAILED_TERMINAL",
            "plan_bug1223_terminal",
        ),
        ("simrun_bug1223_success_old", anchor_day - timedelta(days=1), "SUCCEEDED", "plan_bug1223_success_old"),
        (FIXTURE_ANCHOR_RUN_ID, anchor_day, "SUCCEEDED", "plan_bug1223_anchor"),
    )
    for index, (run_id, trade_day, status, plan_id) in enumerate(rows, start=1):
        evidence_id = f"dse_bug1223_{index}"
        evidence_hash = f"{index + 6:x}" * 64
        evidence_hash = evidence_hash[:64]
        plan_hash = f"{index + 10:x}" * 64
        plan_hash = plan_hash[:64]
        cur.execute(
            """
            INSERT INTO selection.daily_selection_evidence(
                evidence_id,target_trade_date,package_id,manifest_sha256,release_id,release_hash,
                runtime_profile_version_id,runtime_profile_hash,source_type,data_source,candidate_count,
                excluded_count,artifact_hash,evidence_payload_json
            ) VALUES (%s,%s,%s,%s,%s,%s,'bug1223_profile_v1',%s,'fixture','DB_HISTORICAL',1,0,%s,%s)
            """,
            (
                evidence_id,
                trade_day,
                FIXTURE_PACKAGE_ID,
                manifest_sha,
                release_id,
                release_hash,
                "4" * 64,
                evidence_hash,
                psycopg2.extras.Json({"run_id": run_id}),
            ),
        )
        cur.execute(
            """
            INSERT INTO paper_v2.execution_plan(
                plan_id,strategy_id,portfolio_id,package_id,release_id,release_hash,binding_id,binding_hash,
                selection_evidence_id,selection_evidence_hash,target_trade_date,execution_policy_version_id,
                execution_policy_sha256,tail_policy_version_id,tail_policy_sha256,intent_count,
                trading_rule_decision_count,plan_payload_json,plan_hash
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'localsim_twap_only_v1',%s,
                      'bug1223_tail_v1',%s,1,1,%s,%s)
            """,
            (
                plan_id,
                portfolio_id,
                portfolio_id,
                FIXTURE_PACKAGE_ID,
                release_id,
                release_hash,
                binding_id,
                binding_hash,
                evidence_id,
                evidence_hash,
                trade_day,
                "5" * 64,
                "6" * 64,
                psycopg2.extras.Json({"run_id": run_id}),
                plan_hash,
            ),
        )
        cur.execute(
            """
            INSERT INTO paper_v2.simulation_daily_run(
                run_id,trade_date,strategy_id,broker_backend,package_id,manifest_sha256,release_id,
                release_hash,binding_id,binding_hash,selection_evidence_id,selection_artifact_hash,
                execution_plan_id,execution_plan_hash,status,run_payload_json
            ) VALUES (%s,%s,%s,'local_sim',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                run_id,
                trade_day,
                portfolio_id,
                FIXTURE_PACKAGE_ID,
                manifest_sha,
                release_id,
                release_hash,
                binding_id,
                binding_hash,
                evidence_id,
                evidence_hash,
                plan_id,
                plan_hash,
                status,
                psycopg2.extras.Json({"last_stage": status, "broker_called": status != "CANCELLED"}),
            ),
        )
        paper_status = "SUCCEEDED" if status == "SUCCEEDED" else "FAILED"
        cur.execute(
            """
            INSERT INTO paper_v2.run(run_id,portfolio_id,trade_date,status,data_source,runtime_config,completed_at)
            VALUES (%s,%s,%s,%s,'DB_HISTORICAL','{}'::jsonb,NOW())
            """,
            (run_id, portfolio_id, trade_day, paper_status),
        )
        cur.execute(
            """
            INSERT INTO paper_v2.positions(
                run_id,portfolio_id,trade_date,symbol,quantity,available_quantity,avg_cost,
                market_price,market_value,metadata
            ) VALUES (%s,%s,%s,%s,100,100,10,10,1000,'{}'::jsonb)
            """,
            (run_id, portfolio_id, trade_day, f"00000{index}.SZ"),
        )
        cur.execute(
            """
            INSERT INTO paper_v2.daily_snapshots(
                run_id,portfolio_id,trade_date,cash,market_value,nav,position_count,snapshot_time,metadata
            ) VALUES (%s,%s,%s,%s,1000,%s,1,%s,'{}'::jsonb)
            """,
            (
                run_id,
                portfolio_id,
                trade_day,
                500000 - index * 1000,
                501000 - index * 1000,
                datetime.combine(trade_day, datetime.min.time(), tzinfo=UTC),
            ),
        )
        cur.execute(
            """
            INSERT INTO paper_v2.orders(
                order_id,run_id,portfolio_id,package_id,intent_id,symbol,side,quantity,order_type,
                status,filled_quantity,metadata,created_at,updated_at
            ) VALUES (%s,%s,%s,%s,%s,%s,'BUY',100,'LIMIT','FILLED',100,'{}'::jsonb,NOW(),NOW())
            """,
            (
                f"order_bug1223_{index}",
                run_id,
                portfolio_id,
                FIXTURE_PACKAGE_ID,
                f"intent_bug1223_{index}",
                f"00000{index}.SZ",
            ),
        )
        cur.execute(
            """
            INSERT INTO paper_v2.fills(
                fill_id,run_id,order_id,symbol,side,quantity,price,trade_time,reason,metadata
            ) VALUES (%s,%s,%s,%s,'BUY',100,10,NOW(),'fixture','{}'::jsonb)
            """,
            (f"fill_bug1223_{index}", run_id, f"order_bug1223_{index}", f"00000{index}.SZ"),
        )
        cur.execute(
            """
            INSERT INTO paper_v2.cash_ledger(
                run_id,portfolio_id,fill_id,trade_date,symbol,side,notional,fee,cash_delta,cash_after
            ) VALUES (%s,%s,%s,%s,%s,'BUY',1000,0,-1000,%s)
            """,
            (run_id, portfolio_id, f"fill_bug1223_{index}", trade_day, f"00000{index}.SZ", 500000 - index * 1000),
        )
        cur.execute(
            "INSERT INTO paper_v2.run_events(run_id,event_type,message) VALUES (%s,'FIXTURE','fixture')",
            (run_id,),
        )
        if status != "SUCCEEDED":
            cur.execute(
                "INSERT INTO paper_v2.errors(run_id,portfolio_id,error_code,message) VALUES (%s,%s,'FIXTURE','fixture')",
                (run_id, portfolio_id),
            )


def _verify_immutable_source(expected_commit: str | None) -> str:
    if not expected_commit:
        raise LocalSimPruneSafetyError("production apply requires --expected-source-commit")
    current = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, check=True, capture_output=True, text=True
    ).stdout.strip()
    if current != expected_commit:
        raise LocalSimPruneSafetyError(f"source commit mismatch: expected={expected_commit} observed={current}")
    dirty = subprocess.run(["git", "diff", "--quiet", "HEAD", "--", *SOURCE_PATHS], cwd=ROOT, check=False)
    if dirty.returncode:
        raise LocalSimPruneSafetyError("production prune source files differ from the immutable commit")
    merged = subprocess.run(["git", "merge-base", "--is-ancestor", current, "origin/main"], cwd=ROOT, check=False)
    if merged.returncode:
        raise LocalSimPruneSafetyError(f"source commit is not merged into origin/main: {current}")
    return current


def _write_receipt(payload: dict[str, Any], path: Path | None) -> None:
    rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str)
    print(rendered)
    if path is not None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(rendered + "\n", encoding="utf-8")


def _verify_dev_position_authority(conn: Any, request: LocalSimPruneRequest) -> dict[str, Any]:
    from backend.services.paper_trading_v2.repository import PaperTradingV2Repository

    repository = PaperTradingV2Repository()
    repository._set_transaction_conn(conn)  # noqa: SLF001 - same-transaction DEV fixture verification
    try:
        positions = repository.load_latest_positions("paper_bug1223_fixture", date(2026, 8, 28))
    finally:
        repository._clear_transaction_conn()  # noqa: SLF001
    observed = {symbol: position.quantity for symbol, position in positions.items()}
    expected = {"000005.SZ": 100}
    if request.anchor_run_id != FIXTURE_ANCHOR_RUN_ID or observed != expected:
        raise LocalSimPruneSafetyError(
            f"DEV latest-snapshot position authority failed: expected={expected} observed={observed}"
        )
    return {"status": "passed", "positions": observed, "authority_run_id": request.anchor_run_id}


def _post_commit_readback(
    args: argparse.Namespace, request: LocalSimPruneRequest, before: dict[str, Any]
) -> dict[str, Any]:
    conn = _connect("production", args.env_file)
    try:
        conn.set_session(readonly=True)
        with conn.cursor() as cur:
            after = build_prune_plan(cur, request)
        conn.rollback()
    finally:
        conn.close()
    passed = (
        after["candidate_count"] == 0
        and after["protected"]["protected_sha256"] == before["protected"]["protected_sha256"]
    )
    return {
        "status": "passed" if passed else "failed",
        "remaining_candidate_count": after["candidate_count"],
        "protected_sha256_match": after["protected"]["protected_sha256"] == before["protected"]["protected_sha256"],
        "database_identity": after["database_identity"],
    }


def main() -> int:
    args = _parser().parse_args()
    request = LocalSimPruneRequest.build(args.package_id, args.anchor_run_id)
    if args.dev_fixture and not (args.target == "dev" and args.validate_rollback):
        raise LocalSimPruneSafetyError("--dev-fixture is only valid with DEV --validate-rollback")
    if args.validate_rollback and args.target != "dev":
        raise LocalSimPruneSafetyError("--validate-rollback is only permitted for DEV")
    if args.apply:
        if args.target != "production":
            raise LocalSimPruneSafetyError("--apply is reserved for production")
        if not args.confirm_production or args.authorization != "BUG-1223":
            raise LocalSimPruneSafetyError("production apply requires --confirm-production --authorization BUG-1223")
        if not args.expected_plan_sha256:
            raise LocalSimPruneSafetyError("production apply requires --expected-plan-sha256")
        source_commit = _verify_immutable_source(args.expected_source_commit)
    else:
        source_commit = None

    conn = _connect(args.target, args.env_file)
    try:
        conn.autocommit = False
        if args.validate_rollback:
            with conn.cursor() as cur:
                if args.dev_fixture:
                    _install_dev_fixture(cur, request)
                    position_authority = _verify_dev_position_authority(conn, request)
                else:
                    position_authority = {"status": "not_run", "reason": "dev_fixture_disabled"}
                plan = build_prune_plan(cur, request)
                applied_plan, deleted_counts, readback = apply_prune_plan(cur, request, plan["plan_sha256"])
                payload = {
                    "schema_version": "aistock_localsim_failed_history_prune_receipt_v1",
                    "target": "dev",
                    "mode": "validate_rollback",
                    "plan": applied_plan,
                    "deleted_counts": deleted_counts,
                    "readback": readback,
                    "transaction": "rolled_back",
                    "dev_fixture": bool(args.dev_fixture),
                    "position_authority": position_authority,
                }
            conn.rollback()
        elif args.apply:
            with conn.cursor() as cur:
                plan, deleted_counts, readback = apply_prune_plan(cur, request, args.expected_plan_sha256)
                payload = {
                    "schema_version": "aistock_localsim_failed_history_prune_receipt_v1",
                    "target": "production",
                    "mode": "apply",
                    "plan": plan,
                    "deleted_counts": deleted_counts,
                    "readback": readback,
                    "transaction": "committed",
                    "source_commit": source_commit,
                }
            conn.commit()
            payload["post_commit_readback"] = _post_commit_readback(args, request, plan)
        else:
            conn.set_session(readonly=True)
            with conn.cursor() as cur:
                plan = build_prune_plan(cur, request)
            conn.rollback()
            payload = {
                "schema_version": "aistock_localsim_failed_history_prune_receipt_v1",
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
    except LocalSimPruneSafetyError as exc:
        print(json.dumps({"status": "blocked", "reason": str(exc)}, ensure_ascii=False), file=sys.stderr)
        raise SystemExit(2)
