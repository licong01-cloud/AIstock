#!/usr/bin/env python3
"""Dry-run, DEV rollback-validate, or explicitly apply stale-run terminalization."""

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

from scripts.stale_simulation_run_terminalization_core import (  # noqa: E402
    FailedRunTerminalizationRequest,
    TerminalizationSafetyError,
    apply_terminalization_plan,
    build_terminalization_plan,
)


FIXTURE_PACKAGE_IDS = ("bug1165_fixture_pkg_local", "bug1165_fixture_pkg_mini")
SOURCE_PATHS = (
    "scripts/stale_simulation_run_terminalization_core.py",
    "scripts/terminalize_stale_simulation_runs.py",
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", choices=("dev", "production"), required=True)
    parser.add_argument("--package-id", action="append", required=True)
    parser.add_argument("--cutoff", type=date.fromisoformat, required=True, help="exclusive YYYY-MM-DD cutoff")
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
        raise TerminalizationSafetyError(f"environment file does not exist: {env_file}")
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
        raise TerminalizationSafetyError(f"missing {target} database settings: {missing}")
    return psycopg2.connect(**values)


def _verify_immutable_source(expected_commit: str | None) -> str:
    if not expected_commit:
        raise TerminalizationSafetyError("production apply requires --expected-source-commit")
    current = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, check=True, capture_output=True, text=True
    ).stdout.strip()
    if current != expected_commit:
        raise TerminalizationSafetyError(f"source commit mismatch: expected={expected_commit} observed={current}")
    if subprocess.run(["git", "diff", "--quiet", "HEAD", "--", *SOURCE_PATHS], cwd=ROOT, check=False).returncode:
        raise TerminalizationSafetyError("terminalization source files differ from the immutable commit")
    if subprocess.run(["git", "merge-base", "--is-ancestor", current, "origin/main"], cwd=ROOT, check=False).returncode:
        raise TerminalizationSafetyError(f"source commit is not merged into origin/main: {current}")
    return current


def _install_dev_fixture(cur: Any, request: FailedRunTerminalizationRequest) -> None:
    if request.package_ids != tuple(sorted(FIXTURE_PACKAGE_IDS)):
        raise TerminalizationSafetyError(f"--dev-fixture requires exact package IDs: {sorted(FIXTURE_PACKAGE_IDS)}")
    cur.execute(
        "SELECT count(*) FROM strategy_pkg.package WHERE package_id=ANY(%s)",
        (list(request.package_ids),),
    )
    if int(cur.fetchone()[0]):
        raise TerminalizationSafetyError("DEV fixture package IDs already exist")

    old_local_day = request.cutoff - timedelta(days=2)
    old_mini_day = request.cutoff - timedelta(days=1)
    packages = (
        (FIXTURE_PACKAGE_IDS[0], "BUG-1165 Local fixture", "bug1165-local"),
        (FIXTURE_PACKAGE_IDS[1], "BUG-1165 Mini fixture", "bug1165-mini"),
    )
    for package_id, package_name, source_id in packages:
        manifest = {"schema_version": "bug1165_fixture_manifest_v1", "package_id": package_id}
        cur.execute(
            """
            INSERT INTO strategy_pkg.package(
                package_id,package_name,package_version,source_type,source_id,package_status,
                manifest_json,manifest_sha256
            ) VALUES (%s,%s,'1','candidate_strategy_package',%s,'ACTIVE',%s,%s)
            """,
            (package_id, package_name, source_id, psycopg2.extras.Json(manifest), (package_id + "0" * 64)[:64]),
        )

    fixture_rows = (
        (
            "local",
            FIXTURE_PACKAGE_IDS[0],
            "local_sim",
            old_local_day,
            {"last_stage": "FAILED_RETRYABLE", "broker_called": False, "submitted_intents": 0},
        ),
        (
            "mini",
            FIXTURE_PACKAGE_IDS[1],
            "minqmt_sim",
            old_mini_day,
            {
                "last_stage": "FAILED_RETRYABLE",
                "broker_called": False,
                "submitted_intents": 0,
                "qmt_batch_id": "bug1165_unknown_batch",
                "simulation_scheduler_retry_control_v1": {"entries": {"BINDING_FAILED_RETRYABLE": {"attempt": 3}}},
            },
        ),
    )
    for suffix, package_id, backend, trade_date, payload in fixture_rows:
        release_id = f"bug1165_fixture_release_{suffix}"
        release_hash = (f"bug1165-release-{suffix}" + "0" * 64)[:64]
        manifest_sha = (package_id + "0" * 64)[:64]
        cur.execute(
            """
            INSERT INTO strategy_pkg.strategy_runtime_release(
                release_id,package_id,manifest_sha256,runtime_profile_id,runtime_profile_version_id,
                runtime_profile_sha256,daily_strategy_profile_version_id,execution_policy_version_id,
                execution_policy_sha256,tail_policy_version_id,tail_policy_sha256,release_config_json,
                release_hash,validation_state
            ) VALUES (%s,%s,%s,'bug1165_profile','bug1165_profile_v1',%s,'bug1165_daily_v1',
                      'bug1165_exec_v1',%s,'bug1165_tail_v1',%s,%s,%s,'SIM_PASSED')
            """,
            (
                release_id,
                package_id,
                manifest_sha,
                "1" * 64,
                "2" * 64,
                "3" * 64,
                psycopg2.extras.Json({"schema_version": "bug1165_fixture_release_v1"}),
                release_hash,
            ),
        )
        binding_id = f"bug1165_fixture_binding_{suffix}"
        binding_hash = (f"bug1165-binding-{suffix}" + "0" * 64)[:64]
        strategy_id = f"bug1165_fixture_strategy_{suffix}"
        cur.execute(
            """
            INSERT INTO paper_v2.simulation_release_binding(
                binding_id,strategy_id,release_id,release_hash,package_id,manifest_sha256,
                broker_backend,capital_allocation,approval_state,binding_config_json,binding_hash
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,1000000,'SIM_PASSED',%s,%s)
            """,
            (
                binding_id,
                strategy_id,
                release_id,
                release_hash,
                package_id,
                manifest_sha,
                backend,
                psycopg2.extras.Json({"schema_version": "bug1165_fixture_binding_v1"}),
                binding_hash,
            ),
        )
        cur.execute(
            """
            INSERT INTO paper_v2.simulation_daily_run(
                run_id,trade_date,strategy_id,broker_backend,package_id,manifest_sha256,
                release_id,release_hash,binding_id,binding_hash,status,run_payload_json
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'FAILED_RETRYABLE',%s)
            """,
            (
                f"bug1165_fixture_run_{suffix}",
                trade_date,
                strategy_id,
                backend,
                package_id,
                manifest_sha,
                release_id,
                release_hash,
                binding_id,
                binding_hash,
                psycopg2.extras.Json(payload),
            ),
        )


def _write_receipt(payload: dict[str, Any], path: Path | None) -> None:
    rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str)
    print(rendered)
    if path is not None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(rendered + "\n", encoding="utf-8")


def _post_commit_readback(
    *, args: argparse.Namespace, request: FailedRunTerminalizationRequest, plan: dict[str, Any]
) -> dict[str, Any]:
    conn = _connect("production", args.env_file)
    try:
        conn.set_session(readonly=True)
        with conn.cursor() as cur:
            after = build_terminalization_plan(cur, request)
            cur.execute(
                """
                SELECT count(*) FROM paper_v2.simulation_daily_run
                WHERE trade_date < %s AND package_id=ANY(%s)
                  AND run_payload_json#>>'{historical_failed_retryable_terminalization_v1,plan_sha256}'=%s
                """,
                (request.cutoff, list(request.package_ids), plan["plan_sha256"]),
            )
            carrier_count = int(cur.fetchone()[0])
        conn.rollback()
    finally:
        conn.close()
    passed = after["candidate_count"] == 0 and carrier_count == plan["candidate_count"]
    return {
        "status": "passed" if passed else "failed",
        "remaining_candidate_count": after["candidate_count"],
        "carrier_count": carrier_count,
        "expected_carrier_count": plan["candidate_count"],
        "database_identity": after["database_identity"],
    }


def main() -> int:
    args = _parser().parse_args()
    request = FailedRunTerminalizationRequest.build(args.package_id, args.cutoff)
    if args.dev_fixture and not (args.target == "dev" and args.validate_rollback):
        raise TerminalizationSafetyError("--dev-fixture is only valid with DEV --validate-rollback")
    if args.validate_rollback and args.target != "dev":
        raise TerminalizationSafetyError("--validate-rollback is only permitted for DEV")
    if args.apply:
        if args.target != "production":
            raise TerminalizationSafetyError("--apply is reserved for production")
        if not args.confirm_production or args.authorization != "BUG-1165":
            raise TerminalizationSafetyError("production apply requires --confirm-production --authorization BUG-1165")
        if not args.expected_plan_sha256:
            raise TerminalizationSafetyError("production apply requires --expected-plan-sha256")
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
                plan = build_terminalization_plan(cur, request)
                applied_at = datetime.now(UTC)
                applied_plan, updated_counts, readback = apply_terminalization_plan(
                    cur,
                    request,
                    plan["plan_sha256"],
                    applied_at=applied_at,
                )
                payload = {
                    "schema_version": "aistock_stale_simulation_run_terminalization_receipt_v1",
                    "target": "dev",
                    "mode": "validate_rollback",
                    "plan": applied_plan,
                    "updated_counts": updated_counts,
                    "readback": readback,
                    "transaction": "rolled_back",
                    "dev_fixture": bool(args.dev_fixture),
                }
            conn.rollback()
        elif args.apply:
            with conn.cursor() as cur:
                plan, updated_counts, readback = apply_terminalization_plan(
                    cur,
                    request,
                    args.expected_plan_sha256,
                    applied_at=datetime.now(UTC),
                )
                payload = {
                    "schema_version": "aistock_stale_simulation_run_terminalization_receipt_v1",
                    "target": "production",
                    "mode": "apply",
                    "plan": plan,
                    "updated_counts": updated_counts,
                    "readback": readback,
                    "transaction": "committed",
                    "source_commit": source_commit,
                }
            conn.commit()
            payload["post_commit_readback"] = _post_commit_readback(args=args, request=request, plan=plan)
        else:
            conn.set_session(readonly=True)
            with conn.cursor() as cur:
                plan = build_terminalization_plan(cur, request)
            conn.rollback()
            payload = {
                "schema_version": "aistock_stale_simulation_run_terminalization_receipt_v1",
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
    except TerminalizationSafetyError as exc:
        print(json.dumps({"status": "blocked", "reason": str(exc)}, ensure_ascii=False), file=sys.stderr)
        raise SystemExit(2)
