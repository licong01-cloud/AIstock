"""Prepare a QE governance production migration rollout plan.

This script is deliberately prep-only. It validates the existing governance
migration stack and emits an operator plan for a later strategy/user-authorized
production DDL window; it has no mode that executes production DDL.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import scripts.governance_migration_smoke as migration_smoke


CONFIRM_PRODUCTION_PLAN = "PREPARE_QE_GOVERNANCE_PROD_APPLY_PLAN"
ENV_PRODUCTION_PLAN = "AISTOCK_QE_GOVERNANCE_PROD_APPLY_PLAN"


class GovernanceProductionApplyPlanError(RuntimeError):
    """Raised when the production rollout plan guard fails."""


def _env_truthy(key: str) -> bool:
    value = (os.getenv(key) or "").strip().lower()
    return value in {"1", "true", "yes", "y", "on"}


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise GovernanceProductionApplyPlanError(message)


def _require_production_plan_safety(args: argparse.Namespace) -> None:
    _require(
        args.confirm_production_plan == CONFIRM_PRODUCTION_PLAN,
        f"--prepare-production-plan requires --confirm-production-plan {CONFIRM_PRODUCTION_PLAN}",
    )
    _require(
        _env_truthy(ENV_PRODUCTION_PLAN),
        f"--prepare-production-plan requires {ENV_PRODUCTION_PLAN}=true",
    )


def build_plan(*, prepared_for_production: bool = False) -> dict[str, Any]:
    """Build a static rollout plan without opening a DB connection."""

    static_report = migration_smoke.run_static_smoke()
    apply_specs = migration_smoke._specs_in_apply_order()
    generated_at = datetime.now(timezone.utc).isoformat()
    migration_steps: list[dict[str, Any]] = []
    for index, spec in enumerate(apply_specs, start=1):
        migration_steps.append(
            {
                "step": index,
                "filename": spec.filename,
                "path": str(spec.path),
                "schema": spec.schema,
                "phase": spec.phase,
                "transaction": "operator runs one explicit BEGIN/COMMIT window per migration",
                "post_apply_verify": {
                    "tables": list(spec.tables),
                    "views": list(spec.views),
                    "indexes": list(spec.indexes),
                    "constraints": list(spec.constraints),
                    "alter_columns": [f"{table}.{column}" for table, column in spec.alter_columns],
                },
            }
        )

    return {
        "schema_version": "aistock_qe_governance_production_apply_plan_v1",
        "status": "passed",
        "mode": "production_plan_prepared" if prepared_for_production else "static_preview",
        "generated_at": generated_at,
        "prepared_for_production": prepared_for_production,
        "ddl_executed": False,
        "db_writes_executed": False,
        "operator_must_reconfirm_before_apply": True,
        "required_user_gate": "strategy session plus explicit user authorization immediately before production DDL",
        "required_confirmation_token_for_this_plan": CONFIRM_PRODUCTION_PLAN,
        "migration_apply_order": [spec.filename for spec in apply_specs],
        "migration_steps": migration_steps,
        "static_smoke": asdict(static_report),
        "recommended_readonly_preflight": {
            "script": "scripts/governance_migration_smoke.py",
            "mode": "production_readonly_preflight",
            "confirm_token": migration_smoke.CONFIRM_PRODUCTION_PREFLIGHT,
            "env_guard": migration_smoke.ENV_PRODUCTION_PREFLIGHT_ENABLED,
        },
        "safety_notes": [
            "This script is prep-only and has no production DDL execution mode.",
            "Run governance_migration_smoke.py --production-readonly-preflight for SELECT-only catalog checks.",
            "Do not run production DDL until R6 timing is confirmed by strategy session and the user.",
            "Keep model_registry_phase5_20260509.sql last, after strategy_pkg dependencies exist.",
        ],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="Emit JSON report.")
    parser.add_argument(
        "--prepare-production-plan",
        action="store_true",
        help="Require the production-plan token/env guard and mark the output as prepared.",
    )
    parser.add_argument("--confirm-production-plan", default="", help=f"Required token: {CONFIRM_PRODUCTION_PLAN}")
    parser.add_argument("--output", help="Optional path to write the JSON plan.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.prepare_production_plan:
            _require_production_plan_safety(args)
        plan = build_plan(prepared_for_production=bool(args.prepare_production_plan))
        if args.output:
            with open(args.output, "w", encoding="utf-8") as handle:
                json.dump(plan, handle, ensure_ascii=False, indent=2, sort_keys=True)
                handle.write("\n")
        if args.json:
            print(json.dumps(plan, ensure_ascii=False, indent=2, sort_keys=True))
        else:
            print(f"status={plan['status']} mode={plan['mode']} ddl_executed=false")
        return 0 if plan["status"] == "passed" else 2
    except migration_smoke.GovernanceMigrationSmokeError as exc:
        payload = {"status": "failed", "mode": "production_apply_plan", "error": str(exc)}
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        else:
            print(f"status=failed error={exc}", file=sys.stderr)
        return 2
    except GovernanceProductionApplyPlanError as exc:
        payload = {"status": "failed", "mode": "production_apply_plan", "error": str(exc)}
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        else:
            print(f"status=failed error={exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
