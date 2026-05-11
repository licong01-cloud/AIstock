"""Hard-gated R6 production cutover orchestration wrapper.

Default dry-run mode is offline only: it emits the ordered 9:30 cutover plan
without opening DB connections, sending HTTP requests, running service commands,
or invoking production executors. Production mode is explicit and operator-driven.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timezone
from pathlib import Path
from typing import Any

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

SCHEMA_VERSION = "aistock_r6_prod_cutover_e2e_wrapper_v1"
CONFIRM_PROD = "RUN_R6_PROD_CUTOVER_E2E"
CONFIRM_NON_CUTOVER_HOURS = "R6_PROD_CUTOVER_E2E_NON_CUTOVER_HOURS_OK"
FINAL_INTENT = "EXECUTE_R6_PROD_CUTOVER_E2E_GO_NO_GO"
ENV_PROD_ENABLED = "AISTOCK_R6_PROD_CUTOVER_E2E_PROD_ENABLED"
ENV_MUTEX_HELD = "AISTOCK_R6_PROD_CUTOVER_E2E_MUTEX_HELD"
DEFAULT_API_BASE = os.environ.get("PAPER_V2_API_BASE", "<PROD_API_BASE>")
DEFAULT_HEALTH_PATH = "/health"
DEFAULT_DAEMON_PROCESS_NAME = "paper_v2"
DEFAULT_COLDSTART_ENDPOINT = "/paper-v2/coldstart-sanity/sentinel-order"
STRATEGY_EXECUTOR_CONFIRM = (
    "APPLY_QE_GOVERNANCE_EVIDENCE_"
    "BACKFILL_PROD"
)
LEDGER_EXECUTOR_CONFIRM = (
    "APPLY_PROTECTED_ASSET_LEDGER_"
    "BACKFILL_PROD"
)
COLDSTART_CONFIRM = (
    "RUN_PAPER_V2_COLDSTART_"
    "SANITY_PROD"
)

MIGRATION_FILES = (
    "backend/migrations/strategy_pkg_package_asset_20260509.sql",
    "backend/migrations/qe_phase4_master_seed_contract_20260509.sql",
    "backend/migrations/strategy_pkg_runtime_variant_20260509.sql",
    "backend/migrations/strategy_pkg_validation_run_20260509.sql",
    "backend/migrations/strategy_pkg_promotion_review_20260509.sql",
    "backend/migrations/model_registry_phase5_20260509.sql",
)

REQUIRED_PROD_PATH_FIELDS = (
    "release_worktree", "prod_repo", "secure_evidence_dir", "evidence_bundle",
    "evidence_plan", "ledger_plan", "dr_snapshot",
)
READY_BAD_MARKERS = ("NO-GO", "NOT READY", "BLOCKED", "READY-WITH-CAVEATS", "GO-WITH-CAUTION")
READY_GO_MARKERS = ("READY", "GO=YES", "FULL R6 GO")


class R6CutoverE2EError(RuntimeError):
    """Raised when the R6 production cutover wrapper refuses to proceed."""


@dataclass(frozen=True)
class DbTarget:
    target_db: str
    host: str
    port: int
    dbname: str
    user: str
    password_env: str

    @property
    def label(self) -> str:
        return f"{self.target_db}:{self.user}@{self.host}:{self.port}/{self.dbname}"


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: str = ""
    stderr: str = ""
    duration_seconds: float = 0.0


@dataclass(frozen=True)
class PlannedCommand:
    step: str
    command: list[str]
    cwd: str | None = None
    output_path: str | None = None
    env_keys: tuple[str, ...] = ()
    production_touch: bool = False
    db_write: bool = False
    ddl: bool = False
    service_touch: bool = False


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_local() -> datetime:
    if ZoneInfo is None:  # pragma: no cover
        return datetime.now()
    return datetime.now(ZoneInfo("Asia/Shanghai"))


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise R6CutoverE2EError(message)


def _env_truthy(name: str) -> bool:
    value = (os.getenv(name) or "").strip().lower()
    return value in {"1", "true", "yes", "y", "on"}


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _sha256_file_or_placeholder(path: str, label: str) -> str:
    file_path = Path(path)
    if not file_path.exists():
        return f"<{label}_SHA256>"
    return hashlib.sha256(file_path.read_bytes()).hexdigest()


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return value


def _load_json(path: str | None, *, label: str, required: bool = True) -> dict[str, Any] | None:
    if not path:
        _require(not required, f"{label} path is required")
        return None
    file_path = Path(path)
    _require(file_path.exists(), f"{label} file does not exist: {path}")
    try:
        payload = json.loads(file_path.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError as exc:
        raise R6CutoverE2EError(f"{label} file is invalid JSON: {exc}") from exc
    _require(isinstance(payload, dict), f"{label} file must contain a JSON object")
    return payload


def _status_ok(payload: dict[str, Any]) -> bool:
    status = str(payload.get("status") or "").strip().lower()
    verdict = str(payload.get("verdict") or "").strip().upper()
    if verdict == "GO":
        return True
    return status in {"passed", "ready", "verified", "completed", "applied"}


def _target_from_args(args: argparse.Namespace) -> DbTarget:
    return DbTarget(args.target_db, args.db_host, args.db_port, args.db_name, args.db_user, args.db_password_env)


def _is_cutover_window(now: datetime) -> bool:
    if now.weekday() >= 5:
        return False
    return dt_time(8, 45) <= now.time() <= dt_time(9, 30)


def _path_text_in_confirmation(confirmation: str, value: str) -> bool:
    raw = str(value).strip()
    variants = {raw, raw.replace("\\", "/"), raw.replace("/", "\\")}
    return any(item and item in confirmation for item in variants)


def _required_confirmation_values(args: argparse.Namespace) -> list[tuple[str, str]]:
    values = [(field, str(getattr(args, field, "") or "").strip()) for field in REQUIRED_PROD_PATH_FIELDS]
    values.extend((f"ready_doc_{index}", str(doc)) for index, doc in enumerate(args.ready_doc or [], start=1))
    values.append(("dr_snapshot_ref", str(args.dr_snapshot_ref or "")))
    values.append(("final_intent", FINAL_INTENT))
    return values


def _require_prod_guards(args: argparse.Namespace, target: DbTarget, *, now: datetime) -> None:
    _require(args.mode == "prod", "internal error: prod guards called outside --mode=prod")
    _require(args.confirm_prod == CONFIRM_PROD, f"--mode=prod requires exact --confirm-prod {CONFIRM_PROD}")
    _require(_env_truthy(ENV_PROD_ENABLED), f"--mode=prod requires {ENV_PROD_ENABLED}=true")
    _require(_env_truthy(ENV_MUTEX_HELD), f"--mode=prod requires mutex guard {ENV_MUTEX_HELD}=true")
    if not _is_cutover_window(now):
        _require(args.non_cutover_hours_ok == CONFIRM_NON_CUTOVER_HOURS, f"outside the 08:45-09:30 CST cutover window requires --non-cutover-hours-ok {CONFIRM_NON_CUTOVER_HOURS}")
    _require(target.target_db == "prod", "production cutover requires --target-db prod")
    _require(target.port == 5432, "production cutover requires DB port 5432")
    dbname_lower = target.dbname.lower()
    _require("dev" not in dbname_lower and "test" not in dbname_lower, "production cutover refuses dev/test DB names")
    _require(target.host not in {"", "127.0.0.1-dev"}, "production cutover requires an explicit DB host")
    _require(len(args.ready_doc or []) >= args.min_ready_docs, f"production cutover requires at least {args.min_ready_docs} READY verification docs")
    confirmation = str(args.operator_confirmation or "").strip()
    _require(confirmation, "operator confirmation is required for production cutover")
    _require(CONFIRM_PROD in confirmation, "operator confirmation must include the exact production token")
    missing = [label for label, value in _required_confirmation_values(args) if not value or not _path_text_in_confirmation(confirmation, value)]
    _require(not missing, "operator confirmation must include all prerequisite paths, DR ref, and final intent: " + ", ".join(missing))


def _validate_ready_doc(path: str) -> dict[str, Any]:
    file_path = Path(path)
    _require(file_path.exists(), f"READY verification doc does not exist: {path}")
    text = file_path.read_text(encoding="utf-8", errors="ignore")
    upper = text.upper()
    bad = [marker for marker in READY_BAD_MARKERS if marker in upper]
    ready = any(marker in upper for marker in READY_GO_MARKERS)
    _require(not bad and ready, f"READY verification doc is not clean READY: {path}")
    return {"path": str(file_path), "status": "READY", "sha256": _sha256_text(text)}


def _validate_dr_snapshot(path: str, ref: str) -> dict[str, Any]:
    payload = _load_json(path, label="DR snapshot", required=True)
    assert payload is not None
    status = str(payload.get("status") or "").strip().lower()
    _require(status in {"verified", "passed", "completed"}, "DR snapshot must have status verified/passed/completed")
    observed_ref = str(payload.get("snapshot_id") or payload.get("snapshot_ref") or payload.get("checksum") or Path(path).name)
    _require(bool(ref), "DR snapshot ref is required")
    return {"path": path, "status": status, "dr_snapshot_ref": ref, "observed_ref": observed_ref}


def _command_from_json(value: str | None, *, label: str, required: bool) -> list[str] | None:
    if not value:
        _require(not required, f"{label} command JSON is required")
        return None
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise R6CutoverE2EError(f"{label} command must be a JSON array of strings: {exc}") from exc
    _require(isinstance(parsed, list) and parsed, f"{label} command must be a non-empty JSON array")
    command = [str(item) for item in parsed]
    _require(all(item.strip() for item in command), f"{label} command contains an empty token")
    return command


def _limit_output(text: str, limit: int = 4000) -> str:
    return text if len(text) <= limit else text[:limit] + "...<truncated>"


def _run_command(command: list[str], *, cwd: str | None, env: dict[str, str] | None, timeout: int) -> CommandResult:
    started = time.monotonic()
    completed = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
        check=False,
        shell=False,
    )
    return CommandResult(completed.returncode, completed.stdout or "", completed.stderr or "", round(time.monotonic() - started, 3))


def _redacted_command(command: list[str]) -> list[str]:
    redacted: list[str] = []
    skip_next = False
    for item in command:
        if skip_next:
            redacted.append("<REDACTED>")
            skip_next = False
            continue
        redacted.append(item)
        if item in {"--db-password", "--password", "--token", "--secret"}:
            skip_next = True
    return redacted


def _step_payload(
    *,
    step: str,
    status: str,
    message: str,
    command: PlannedCommand | None = None,
    result: CommandResult | None = None,
    data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {"step": step, "status": status, "message": message, "data": data or {}}
    if command is not None:
        payload.update({
            "command": _redacted_command(command.command),
            "cwd": command.cwd,
            "output_path": command.output_path,
            "env_keys": list(command.env_keys),
            "production_touch": command.production_touch,
            "db_write": command.db_write,
            "ddl": command.ddl,
            "service_touch": command.service_touch,
        })
    if result is not None:
        payload.update({
            "returncode": result.returncode,
            "duration_seconds": result.duration_seconds,
            "stdout_preview": _limit_output(result.stdout),
            "stderr_preview": _limit_output(result.stderr),
        })
    return payload


def _execute_command_step(command: PlannedCommand, *, timeout: int, extra_env: dict[str, str] | None = None) -> dict[str, Any]:
    env = None
    if extra_env:
        env = os.environ.copy()
        env.update(extra_env)
    result = _run_command(command.command, cwd=command.cwd, env=env, timeout=timeout)
    if command.output_path and result.stdout and result.returncode == 0:
        output_path = Path(command.output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(result.stdout, encoding="utf-8")
    return _step_payload(
        step=command.step,
        status="PASS" if result.returncode == 0 else "FAIL",
        message="command completed" if result.returncode == 0 else "command failed",
        command=command,
        result=result,
    )


def _secure_path(args: argparse.Namespace, filename: str) -> str:
    return str(Path(args.secure_evidence_dir) / filename)


def _python_exe() -> str:
    return sys.executable or "python"


def _build_planned_commands(args: argparse.Namespace) -> list[PlannedCommand]:
    release = str(args.release_worktree)
    prod = str(args.prod_repo)
    python = _python_exe()
    target_label = _target_from_args(args).label
    package_ids = " ".join(args.package_id or [])
    strategy_operator_confirmation = (
        f"{STRATEGY_EXECUTOR_CONFIRM} target={target_label} db={args.db_name} "
        f"plan={_sha256_file_or_placeholder(args.evidence_plan, 'EVIDENCE_PLAN')} "
        f"dr={args.dr_snapshot_ref} packages={package_ids} wrapper={CONFIRM_PROD}"
    )
    ledger_operator_confirmation = (
        f"{LEDGER_EXECUTOR_CONFIRM} target={target_label} db={args.db_name} "
        f"plan={_sha256_file_or_placeholder(args.ledger_plan, 'LEDGER_PLAN')} "
        f"dr={args.dr_snapshot_ref} packages={package_ids} wrapper={CONFIRM_PROD}"
    )
    commands: list[PlannedCommand] = [
        PlannedCommand(
            "preflight_static_plan",
            [python, "scripts/governance_production_apply_plan.py", "--json", "--output", _secure_path(args, "r6_governance_production_apply_plan.json")],
            cwd=release,
            output_path=_secure_path(args, "r6_governance_production_apply_plan.json"),
        ),
        PlannedCommand(
            "preflight_static_migration_smoke",
            [python, "scripts/governance_migration_smoke.py", "--json"],
            cwd=release,
            output_path=_secure_path(args, "r6_governance_migration_static_smoke.json"),
        ),
    ]
    for index, migration in enumerate(MIGRATION_FILES, start=1):
        commands.append(
            PlannedCommand(
                f"migration_{index:02d}",
                [
                    "psql", "--host", args.db_host, "--port", str(args.db_port),
                    "--username", args.db_user, "--dbname", args.db_name,
                    "--set", "ON_ERROR_STOP=1", "--single-transaction",
                    "--file", str(Path(args.release_worktree) / migration),
                ],
                cwd=release,
                output_path=_secure_path(args, f"r6_migration_{index:02d}_{Path(migration).name}.log"),
                env_keys=("PGOPTIONS", "PGPASSWORD"),
                production_touch=True,
                db_write=True,
                ddl=True,
            )
        )
    commands.extend([
        PlannedCommand(
            "strategy_package_evidence_apply",
            [
                python, "scripts/strategy_package_governance_evidence_backfill_prod_executor.py",
                "--apply", "--confirm-apply", STRATEGY_EXECUTOR_CONFIRM,
                "--evidence-bundle", args.evidence_bundle,
                "--plan-preview", args.evidence_plan,
                "--dr-snapshot", args.dr_snapshot,
                "--dr-snapshot-ref", args.dr_snapshot_ref,
                "--operator-confirmation", strategy_operator_confirmation,
                "--target-db", "prod", "--db-host", args.db_host,
                "--db-port", str(args.db_port), "--db-name", args.db_name,
                "--db-user", args.db_user, "--db-password-env", args.db_password_env,
                "--json", "--output", _secure_path(args, "r6_strategy_package_evidence_backfill_apply.json"),
            ],
            cwd=release,
            output_path=_secure_path(args, "r6_strategy_package_evidence_backfill_apply.json"),
            env_keys=(args.db_password_env,),
            production_touch=True,
            db_write=True,
        ),
        PlannedCommand(
            "protected_asset_ledger_apply",
            [
                python, "scripts/protected_asset_ledger_backfill_prod_executor.py",
                "--apply", "--confirm-apply", LEDGER_EXECUTOR_CONFIRM,
                "--evidence-bundle", args.ledger_plan,
                "--plan-preview", args.ledger_plan,
                "--dr-snapshot", args.dr_snapshot,
                "--dr-snapshot-ref", args.dr_snapshot_ref,
                "--operator-confirmation", ledger_operator_confirmation,
                "--target-db", "prod", "--db-host", args.db_host,
                "--db-port", str(args.db_port), "--db-name", args.db_name,
                "--db-user", args.db_user, "--db-password-env", args.db_password_env,
                "--json", "--output", _secure_path(args, "r6_protected_asset_ledger_backfill_apply.json"),
            ],
            cwd=release,
            output_path=_secure_path(args, "r6_protected_asset_ledger_backfill_apply.json"),
            env_keys=(args.db_password_env,),
            production_touch=True,
            db_write=True,
        ),
    ])
    backend_command = _command_from_json(args.backend_restart_command, label="backend restart", required=args.mode == "prod")
    daemon_command = _command_from_json(args.daemon_restart_command, label="daemon restart", required=args.mode == "prod")
    if args.mode == "dry-run" and backend_command is None:
        backend_command = ["<APPROVED_BACKEND_RESTART_COMMAND_JSON_REQUIRED_IN_PROD>"]
    if args.mode == "dry-run" and daemon_command is None:
        daemon_command = ["<APPROVED_PAPER_V2_DAEMON_RESTART_COMMAND_JSON_REQUIRED_IN_PROD>"]
    if backend_command:
        commands.append(PlannedCommand("backend_restart", backend_command, cwd=prod, output_path=_secure_path(args, "r6_backend_restart.log"), production_touch=True, service_touch=True))
    if daemon_command:
        commands.append(PlannedCommand("paper_v2_daemon_enable_restart", daemon_command, cwd=prod, output_path=_secure_path(args, "r6_paper_v2_daemon_restart.log"), production_touch=True, service_touch=True))
    coldstart_command = [
        python, "scripts/paper_v2_coldstart_sanity.py", "--mode", "prod",
        "--confirm-prod", COLDSTART_CONFIRM,
        "--operator-confirmation", f"{COLDSTART_CONFIRM} target=prod packages={','.join(args.package_id or [])} approved_by=R6-cutover-wrapper",
        "--api-base", args.api_base, "--health-path", args.health_path,
        "--sentinel-endpoint", args.sentinel_endpoint,
        "--daemon-process-name", args.daemon_process_name,
        "--target-db", "prod", "--db-host", args.db_host, "--db-port", str(args.db_port),
        "--db-name", args.db_name, "--db-user", args.db_user,
        "--db-password-env", args.db_password_env,
        "--json", "--output", _secure_path(args, "paper_v2_coldstart_sanity_prod.json"),
    ]
    for package_id in args.package_id or []:
        coldstart_command.extend(["--package-id", package_id])
    commands.append(PlannedCommand(
        "paper_v2_coldstart_sanity_prod",
        coldstart_command,
        cwd=prod,
        output_path=_secure_path(args, "paper_v2_coldstart_sanity_prod.json"),
        env_keys=("AISTOCK_PAPER_V2_COLDSTART_SANITY_PROD_ENABLED", "AISTOCK_PAPER_V2_COLDSTART_SANITY_MUTEX_HELD", args.db_password_env),
        production_touch=True,
        db_write=True,
        service_touch=True,
    ))
    return commands


def _base_report(args: argparse.Namespace, target: DbTarget) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "mode": args.mode,
        "dry_run": args.mode == "dry-run",
        "target_db": target.target_db,
        "db_target": target.label,
        "release_worktree": args.release_worktree,
        "prod_repo": args.prod_repo,
        "secure_evidence_dir": args.secure_evidence_dir,
        "production_services_touched": False,
        "prod_backend_port_touched": False,
        "prod_db_touched": False,
        "db_connection_opened_by_wrapper": False,
        "db_writes_executed": False,
        "ddl_executed": False,
        "main_merge_executed": False,
        "steps": [],
        "failed_steps": [],
        "remedial_action": [],
        "real_trading_ready": False,
        "verdict": "NO-GO",
    }


def run_dry_run(args: argparse.Namespace, target: DbTarget) -> dict[str, Any]:
    report = _base_report(args, target)
    report["status"] = "dry_run_preview"
    report["operator_guards"] = {
        "required_token": CONFIRM_PROD,
        "required_env": ENV_PROD_ENABLED,
        "required_mutex_env": ENV_MUTEX_HELD,
        "non_cutover_hours_token": CONFIRM_NON_CUTOVER_HOURS,
        "final_intent": FINAL_INTENT,
    }
    report["steps"] = [
        _step_payload(step=command.step, status="SKIPPED", message="dry-run preview only; command not executed", command=command)
        for command in _build_planned_commands(args)
    ]
    report["safety_notes"] = [
        "No production DB connection opened by the wrapper.",
        "No subprocess command executed in dry-run mode.",
        "No backend restart, daemon restart, git merge, psql, or production executor call was run.",
        "Run --mode=prod only inside the approved R6 cutover window with exact token/env/mutex/DR ref/prerequisites/final intent.",
    ]
    return report


def _validate_prerequisites(args: argparse.Namespace) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for field in REQUIRED_PROD_PATH_FIELDS:
        value = str(getattr(args, field) or "")
        path = Path(value)
        _require(path.exists(), f"required prerequisite path does not exist for --{field.replace('_', '-')}: {value}")
        results.append({"field": field, "path": value, "exists": True})
    for doc in args.ready_doc or []:
        results.append({"field": "ready_doc", **_validate_ready_doc(str(doc))})
    results.append({"field": "dr_snapshot", **_validate_dr_snapshot(args.dr_snapshot, args.dr_snapshot_ref)})
    return results


def _remedial_action(failed_steps: list[str]) -> list[str]:
    mapping = {
        "prod_guards": "Stop before production touch; correct token/env/mutex/window/operator confirmation and re-review the target scope.",
        "prerequisite_validation": "Stop before production touch; provide existing prerequisite paths, clean READY docs, and verified DR snapshot artifact.",
        "preflight_static_plan": "Regenerate the static governance plan and resolve any release-worktree mismatch before cutover.",
        "preflight_static_migration_smoke": "Resolve static migration smoke failures before DB snapshot or DDL.",
        "migration": "Stop runtime activation; use DR/DB operator rollback policy for any committed migration uncertainty.",
        "strategy_package_evidence_apply": "Stop DDL/runtime progression and review the StrategyPackage evidence executor report.",
        "protected_asset_ledger_apply": "Stop runtime activation and review protected asset ledger executor report.",
        "backend_restart": "Do not start daemon/coldstart; rollback code or restart old backend per release commander.",
        "paper_v2_daemon_enable_restart": "Disable/stop daemon and do not run coldstart until runtime operator resolves daemon state.",
        "paper_v2_coldstart_sanity_prod": "Keep R6 disabled or rolled back; inspect coldstart sanity JSON and sentinel cleanup status.",
        "final_readiness": "Default to NO-GO for 9:30 until all required evidence artifacts are clean GO/READY.",
    }
    actions: list[str] = []
    for step in failed_steps:
        key = "migration" if step.startswith("migration_") else step
        actions.append(mapping.get(key, f"Investigate failed cutover step: {step}"))
    return actions


def _assert_artifact_ok(path: str, *, label: str, require_go: bool = False) -> dict[str, Any]:
    payload = _load_json(path, label=label, required=True)
    assert payload is not None
    if require_go:
        _require(payload.get("verdict") == "GO" and payload.get("real_trading_ready") is True, f"{label} must report verdict=GO and real_trading_ready=true")
    else:
        _require(_status_ok(payload), f"{label} did not report passed/applied/verified/GO status")
    return {"path": path, "status": payload.get("status"), "verdict": payload.get("verdict")}


def _final_evidence_checks(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "dr_snapshot": _validate_dr_snapshot(args.dr_snapshot, args.dr_snapshot_ref),
        "strategy_package_apply": _assert_artifact_ok(_secure_path(args, "r6_strategy_package_evidence_backfill_apply.json"), label="StrategyPackage evidence apply report"),
        "protected_asset_ledger_apply": _assert_artifact_ok(_secure_path(args, "r6_protected_asset_ledger_backfill_apply.json"), label="protected asset ledger apply report"),
        "coldstart_sanity": _assert_artifact_ok(_secure_path(args, "paper_v2_coldstart_sanity_prod.json"), label="Paper v2 coldstart sanity report", require_go=True),
    }


def run_prod(args: argparse.Namespace, target: DbTarget) -> dict[str, Any]:
    report = _base_report(args, target)
    steps: list[dict[str, Any]] = []
    try:
        _require_prod_guards(args, target, now=_now_local())
        steps.append(_step_payload(step="prod_guards", status="PASS", message="all production cutover guards passed", data={"operator_confirmation_sha256": _sha256_text(args.operator_confirmation)}))
        prereqs = _validate_prerequisites(args)
        steps.append(_step_payload(step="prerequisite_validation", status="PASS", message="all prerequisite paths, READY docs, and DR snapshot validated", data={"items": prereqs}))
        env_for_db = {"PGOPTIONS": "-c lock_timeout=3s -c statement_timeout=120s", "PGPASSWORD": os.getenv(args.db_password_env, "")}
        for command in _build_planned_commands(args):
            step = _execute_command_step(command, timeout=args.command_timeout_seconds, extra_env=env_for_db if command.ddl else None)
            steps.append(step)
            if step["status"] == "FAIL":
                break
    except R6CutoverE2EError as exc:
        failed_step = "prod_guards" if not steps else "prerequisite_validation"
        steps.append(_step_payload(step=failed_step, status="FAIL", message=str(exc)))
    except Exception as exc:
        steps.append(_step_payload(step="unhandled_error", status="FAIL", message=str(exc)))

    failed_steps = [str(item["step"]) for item in steps if item.get("status") == "FAIL"]
    if not failed_steps:
        try:
            evidence = _final_evidence_checks(args)
            steps.append(_step_payload(step="final_readiness", status="PASS", message="all required R6 cutover evidence is GO/READY", data=evidence))
        except R6CutoverE2EError as exc:
            steps.append(_step_payload(step="final_readiness", status="FAIL", message=str(exc)))
    failed_steps = [str(item["step"]) for item in steps if item.get("status") == "FAIL"]
    report["steps"] = steps
    report["failed_steps"] = failed_steps
    report["remedial_action"] = _remedial_action(failed_steps)
    report["status"] = "passed" if not failed_steps else "failed"
    report["verdict"] = "GO" if not failed_steps else "NO-GO"
    report["real_trading_ready"] = not failed_steps
    report["production_services_touched"] = any(item.get("production_touch") for item in steps)
    report["prod_backend_port_touched"] = any(item.get("step") == "backend_restart" and item.get("status") == "PASS" for item in steps)
    report["prod_db_touched"] = any(item.get("db_write") or item.get("ddl") for item in steps)
    report["db_writes_executed"] = any(item.get("db_write") and item.get("status") == "PASS" for item in steps)
    report["ddl_executed"] = any(item.get("ddl") and item.get("status") == "PASS" for item in steps)
    report["main_merge_executed"] = False
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run or preview the hard-gated R6 production cutover E2E orchestration wrapper.")
    parser.add_argument("--mode", choices=("dry-run", "prod"), default="dry-run")
    parser.add_argument("--confirm-prod", default="", help="Exact production confirmation token required with --mode=prod.")
    parser.add_argument("--non-cutover-hours-ok", default="", help="Exact acknowledgement token required when --mode=prod is outside 08:45-09:30 CST.")
    parser.add_argument("--operator-confirmation", default="", help="Typed operator confirmation containing token, prerequisite paths, DR ref, and final intent.")
    parser.add_argument("--release-worktree", default=str(Path.cwd()))
    parser.add_argument("--prod-repo", default=os.environ.get("AISTOCK_PROD_REPO", "<PROD_REPO>"))
    parser.add_argument("--secure-evidence-dir", default="tests/aistock_validation/dry_runs/20260511_evidence_backfill_prod_executor_dry_run")
    parser.add_argument("--ready-doc", action="append", default=[], help="Verification doc that must contain clean READY/GO wording; repeat for all required docs.")
    parser.add_argument("--min-ready-docs", type=int, default=2)
    parser.add_argument("--evidence-bundle", default="<R6_EVIDENCE_BUNDLE>")
    parser.add_argument("--evidence-plan", default="<SECURE_EVIDENCE_DIR>/r6_evidence_backfill_plan.json")
    parser.add_argument("--ledger-plan", default="<SECURE_EVIDENCE_DIR>/r6_protected_asset_ledger_plan.json")
    parser.add_argument("--dr-snapshot", default="<SECURE_EVIDENCE_DIR>/r6_dr_snapshot_verified.json")
    parser.add_argument("--dr-snapshot-ref", default="")
    parser.add_argument("--backend-restart-command", help="JSON array command for approved backend restart/status capture.")
    parser.add_argument("--daemon-restart-command", help="JSON array command for approved Paper/R6 daemon enable/restart/status capture.")
    parser.add_argument("--api-base", default=DEFAULT_API_BASE)
    parser.add_argument("--health-path", default=DEFAULT_HEALTH_PATH)
    parser.add_argument("--sentinel-endpoint", default=DEFAULT_COLDSTART_ENDPOINT)
    parser.add_argument("--daemon-process-name", default=DEFAULT_DAEMON_PROCESS_NAME)
    parser.add_argument("--package-id", action="append", default=[])
    parser.add_argument("--target-db", choices=("dev", "prod"), default="prod")
    parser.add_argument("--db-host", default="prod-db.invalid")
    parser.add_argument("--db-port", type=int, default=5432)
    parser.add_argument("--db-name", default="aistock")
    parser.add_argument("--db-user", default="aistock_operator")
    parser.add_argument("--db-password-env", default="AISTOCK_PROD_DB_PASSWORD")
    parser.add_argument("--command-timeout-seconds", type=int, default=300)
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


def _failure_payload(error: Exception, *, args: argparse.Namespace, target: DbTarget) -> dict[str, Any]:
    report = _base_report(args, target)
    report.update({"status": "failed", "error": str(error), "failed_steps": ["unhandled_error"], "remedial_action": [str(error)]})
    return report


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    target = _target_from_args(args)
    try:
        _require(args.command_timeout_seconds > 0, "--command-timeout-seconds must be positive")
        report = run_dry_run(args, target) if args.mode == "dry-run" else run_prod(args, target)
        _emit(report, json_output=args.json, output=args.output)
        return 0 if report.get("verdict") == "GO" else (0 if args.mode == "dry-run" else 2)
    except R6CutoverE2EError as exc:
        _emit(_failure_payload(exc, args=args, target=target), json_output=True, output=args.output)
        return 2
    except Exception as exc:
        _emit(_failure_payload(exc, args=args, target=target), json_output=True, output=args.output)
        return 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
