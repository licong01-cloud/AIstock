"""Production-capable StrategyPackage governance evidence backfill executor.

Default mode is an offline preview that validates a reviewed evidence bundle or
planner preview and emits JSON. The --apply path is intentionally hard gated:
operators must provide an explicit apply flag, exact token, enabled environment
flag, mutex marker, production target triple-check, verified DR snapshot, plan
preview, and approved operator confirmation before any database connection.

Complexity guardrail: this executor is bounded to four operator-approved
packages. It does no dataframe joins or large scans; all DB lookups are point
queries by package id or natural key before per-package transactional inserts.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


SCHEMA_VERSION = "aistock_qe_governance_evidence_backfill_prod_executor_v1"
PLAN_SCHEMA_VERSION = "aistock_qe_governance_evidence_backfill_plan_v1"
CONFIRM_APPLY = "APPLY_QE_GOVERNANCE_EVIDENCE_BACKFILL_PROD"
ENV_APPLY_ENABLED = "AISTOCK_QE_GOVERNANCE_EVIDENCE_BACKFILL_PROD_APPLY_ENABLED"
ENV_MUTEX_HELD = "AISTOCK_QE_GOVERNANCE_EVIDENCE_BACKFILL_MUTEX_HELD"
EXPECTED_PACKAGE_COUNT = 4
ALLOWED_PACKAGE_STATUSES = {"BACKTEST_APPROVED", "SELECTION_ENABLED", "PAPER_ENABLED"}
ALLOWED_TABLES = {
    "strategy_pkg.package_asset",
    "strategy_pkg.package_validation_run",
    "strategy_pkg.package_runtime_variant",
    "strategy_pkg.seed_fragility_score",
}
JSON_COLUMNS = {
    "metadata",
    "metrics_json",
    "artifact_manifest_json",
    "evidence_json",
    "variant_config",
    "validation_evidence",
    "seed_sequence",
    "metric_mean_by_seed",
    "metric_std_by_seed",
    "worst_seed_metric",
    "best_seed_metric",
    "factor_importance_stability",
    "selection_overlap_by_seed",
    "nondeterministic_flags",
    "evidence",
}
DANGEROUS_SQL_VERBS = (
    "DROP",
    "ALTER",
    "TRUNCATE",
    "CREATE",
    "GRANT",
    "REVOKE",
    "COMMENT",
    "VACUUM",
    "COPY",
    "MERGE",
)
PACKAGE_ID_RE = re.compile(r"\bpkg_[A-Za-z0-9_\-]+\b")


class GovernanceEvidenceBackfillProdExecutorError(RuntimeError):
    """Raised when the production evidence backfill executor refuses to proceed."""


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

    def as_psycopg2_kwargs(self) -> dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
            "application_name": "AIstock-qe-governance-evidence-prod-backfill",
            "options": "-c client_encoding=utf8",
        }


@dataclass(frozen=True)
class EvidenceRow:
    table: str
    natural_key: dict[str, Any]
    action: str
    columns: dict[str, Any]


@dataclass(frozen=True)
class EvidencePackage:
    package_id: str
    manifest_sha256: str
    package_status: str
    rows: list[EvidenceRow]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise GovernanceEvidenceBackfillProdExecutorError(message)


def _env_truthy(name: str) -> bool:
    value = (os.getenv(name) or "").strip().lower()
    return value in {"1", "true", "yes", "y", "on"}


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_json(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def _load_json(path: str | None, *, label: str, required: bool = False) -> tuple[dict[str, Any] | None, str | None]:
    if not path:
        _require(not required, f"{label} is required")
        return None, None
    file_path = Path(path)
    _require(file_path.exists(), f"{label} file does not exist: {path}")
    data = file_path.read_bytes()
    try:
        payload = json.loads(data.decode("utf-8-sig"))
    except json.JSONDecodeError as exc:
        raise GovernanceEvidenceBackfillProdExecutorError(f"{label} file is invalid JSON: {exc}") from exc
    _require(isinstance(payload, dict), f"{label} file must contain a JSON object")
    return payload, _sha256_bytes(data)


def _row_get(row: Any, key: str, index: int, default: Any = None) -> Any:
    if row is None:
        return default
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        return row[index]
    except (IndexError, TypeError):
        return default


def _json_db_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        try:
            from psycopg2.extras import Json  # type: ignore

            return Json(value)
        except Exception:  # pragma: no cover - fallback for tests or minimal envs
            return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return value


def _canonicalize(value: Any) -> Any:
    if hasattr(value, "adapted"):
        return _canonicalize(getattr(value, "adapted"))
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


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


def _connect(target: DbTarget) -> Any:
    try:
        import psycopg2  # type: ignore
    except ImportError as exc:  # pragma: no cover - depends on operator host
        raise GovernanceEvidenceBackfillProdExecutorError("psycopg2 is required for production DB apply") from exc
    try:
        return psycopg2.connect(**target.as_psycopg2_kwargs())
    except Exception as exc:  # pragma: no cover - depends on operator host
        raise GovernanceEvidenceBackfillProdExecutorError(f"failed to connect to DB target {target.label}: {exc}") from exc


def _as_list(value: Any, *, label: str) -> list[Any]:
    _require(isinstance(value, list), f"{label} must be a list")
    return value


def _text(value: Any, *, label: str) -> str:
    _require(isinstance(value, str) and value.strip(), f"{label} must be a non-empty string")
    return value.strip()


def _normalize_row(raw: dict[str, Any], *, package_id: str, manifest_sha256: str) -> EvidenceRow:
    table = _text(raw.get("table"), label="row.table")
    _require(table in ALLOWED_TABLES, f"unsupported evidence table: {table}")
    natural_key = raw.get("natural_key") or {}
    columns = raw.get("columns") or {}
    _require(isinstance(natural_key, dict), f"{table} natural_key must be an object")
    _require(isinstance(columns, dict), f"{table} columns must be an object")
    _require(
        columns.get("package_id", natural_key.get("package_id")) == package_id,
        f"{table} row must bind package_id to package {package_id}",
    )
    if "package_id" in columns:
        _require(columns["package_id"] == package_id, f"{table} row package_id must match package {package_id}")
    if "package_id" in natural_key:
        _require(natural_key["package_id"] == package_id, f"{table} natural_key package_id must match package {package_id}")
    if "manifest_sha256" in columns:
        _require(columns["manifest_sha256"] == manifest_sha256, f"{table} row manifest_sha256 must match package {package_id}")
    return EvidenceRow(
        table=table,
        natural_key=natural_key,
        action=str(raw.get("action") or "apply_governance_evidence"),
        columns=columns,
    )


def _normalize_packages(payload: dict[str, Any], *, label: str) -> list[EvidencePackage]:
    packages = _as_list(payload.get("packages"), label=f"{label}.packages")
    _require(len(packages) == EXPECTED_PACKAGE_COUNT, f"{label} must contain exactly {EXPECTED_PACKAGE_COUNT} packages")
    normalized: list[EvidencePackage] = []
    seen: set[str] = set()
    for raw in packages:
        _require(isinstance(raw, dict), f"{label}.packages entries must be objects")
        package_id = _text(raw.get("package_id"), label="package_id")
        manifest_sha256 = _text(raw.get("manifest_sha256"), label=f"{package_id}.manifest_sha256")
        package_status = _text(raw.get("package_status"), label=f"{package_id}.package_status")
        _require(package_id not in seen, f"duplicate package_id in {label}: {package_id}")
        seen.add(package_id)
        _require(package_status in ALLOWED_PACKAGE_STATUSES, f"package_status for {package_id} is not approved for production backfill: {package_status}")
        raw_rows = raw.get("rows")
        _require(isinstance(raw_rows, list) and raw_rows, f"{label} package {package_id} must include planned rows")
        rows = [_normalize_row(row, package_id=package_id, manifest_sha256=manifest_sha256) for row in raw_rows]
        normalized.append(EvidencePackage(package_id=package_id, manifest_sha256=manifest_sha256, package_status=package_status, rows=rows))
    return normalized


def _package_index(packages: Iterable[EvidencePackage]) -> dict[str, EvidencePackage]:
    return {package.package_id: package for package in packages}


def _validate_package_alignment(plan_packages: list[EvidencePackage], bundle_payload: dict[str, Any] | None) -> None:
    if not bundle_payload or "packages" not in bundle_payload:
        return
    bundle_packages = bundle_payload.get("packages")
    if not isinstance(bundle_packages, list):
        return
    plan_by_id = _package_index(plan_packages)
    bundle_ids: set[str] = set()
    for raw in bundle_packages:
        if not isinstance(raw, dict) or not raw.get("package_id"):
            continue
        package_id = str(raw["package_id"])
        bundle_ids.add(package_id)
        _require(package_id in plan_by_id, f"evidence bundle package {package_id} is missing from plan preview")
        manifest = raw.get("manifest_sha256")
        if isinstance(manifest, str) and manifest.strip():
            _require(manifest == plan_by_id[package_id].manifest_sha256, f"evidence bundle manifest mismatch for {package_id}")
    _require(bundle_ids == set(plan_by_id), "evidence bundle package_id set must match plan preview package_id set")


def _validate_plan_preview(plan: dict[str, Any]) -> list[EvidencePackage]:
    _require(plan.get("schema_version") == PLAN_SCHEMA_VERSION, f"plan preview schema_version must be {PLAN_SCHEMA_VERSION}")
    _require(plan.get("status") == "passed", "plan preview must have status=passed")
    _require(plan.get("mode") == "dry_run_plan", "plan preview must have mode=dry_run_plan")
    _require(plan.get("package_count") == EXPECTED_PACKAGE_COUNT, f"plan preview must contain exactly {EXPECTED_PACKAGE_COUNT} packages")
    _require(plan.get("blocked_packages") in ({}, None), "plan preview must not contain blocked packages")
    _require(plan.get("db_writes_executed") is False, "plan preview must be generated without DB writes")
    if "db_connection_opened" in plan:
        _require(plan.get("db_connection_opened") is False, "plan preview must be generated without DB connection")
    return _normalize_packages(plan, label="plan preview")


def _validate_bundle_for_preview(bundle: dict[str, Any]) -> list[EvidencePackage]:
    if bundle.get("schema_version") == PLAN_SCHEMA_VERSION and "rows" not in json.dumps(bundle.get("packages", []), ensure_ascii=False):
        raise GovernanceEvidenceBackfillProdExecutorError("planner source bundle lacks planned rows; provide --plan-preview for executor preview")
    return _normalize_packages(bundle, label="evidence bundle")


def _resolve_packages(bundle: dict[str, Any], plan: dict[str, Any] | None) -> list[EvidencePackage]:
    if plan is not None:
        plan_packages = _validate_plan_preview(plan)
        _validate_package_alignment(plan_packages, bundle)
        return plan_packages
    return _validate_bundle_for_preview(bundle)


def _validate_dr_snapshot(args: argparse.Namespace) -> tuple[str, str | None, dict[str, Any] | None]:
    snapshot_payload: dict[str, Any] | None = None
    snapshot_sha: str | None = None
    snapshot_ref = (args.dr_snapshot_ref or "").strip()
    if args.dr_snapshot:
        snapshot_payload, snapshot_sha = _load_json(args.dr_snapshot, label="DR snapshot", required=True)
        assert snapshot_payload is not None
        status = str(snapshot_payload.get("status", "")).strip().lower()
        _require(status in {"verified", "passed", "completed"}, "DR snapshot must be verified before production apply")
        snapshot_ref = snapshot_ref or str(
            snapshot_payload.get("snapshot_id")
            or snapshot_payload.get("snapshot_ref")
            or snapshot_payload.get("checksum")
            or Path(args.dr_snapshot).name
        )
    _require(snapshot_ref, "DR snapshot ref is required for production apply")
    return snapshot_ref, snapshot_sha, snapshot_payload


def _load_operator_confirmation(value: str | None) -> tuple[dict[str, Any], str]:
    _require(value is not None and str(value).strip(), "operator confirmation is required for production apply")
    raw = str(value).strip()
    path = Path(raw)
    if path.exists():
        payload, sha = _load_json(raw, label="operator confirmation", required=True)
        assert payload is not None and sha is not None
        _require(str(payload.get("status", "")).strip().lower() == "approved", "operator confirmation must have status=approved")
        confirmation_text = str(payload.get("confirmation") or payload.get("typed_confirmation") or "").strip()
        _require(bool(confirmation_text), "operator confirmation must include typed confirmation text")
        return payload, sha
    sha = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return {"status": "approved", "confirmation": raw}, sha


def _require_operator_confirmation_scope(
    payload: dict[str, Any],
    *,
    target: DbTarget,
    packages: list[EvidencePackage],
    plan_sha: str | None,
    dr_snapshot_ref: str,
) -> None:
    text = str(payload.get("confirmation") or payload.get("typed_confirmation") or "").strip()
    _require(CONFIRM_APPLY in text, "operator confirmation must include the exact production confirmation token")
    _require(target.label in text or target.dbname in text, "operator confirmation must include the target DB label or DB name")
    if plan_sha:
        _require(plan_sha in text, "operator confirmation must include the plan preview SHA256")
    _require(dr_snapshot_ref in text, "operator confirmation must include the DR snapshot ref")
    missing = [package.package_id for package in packages if package.package_id not in text]
    _require(not missing, f"operator confirmation must include every package id: {', '.join(missing)}")


def _require_apply_guards(args: argparse.Namespace, target: DbTarget) -> None:
    _require(args.apply, "internal error: apply guards called without --apply")
    _require(args.confirm_apply == CONFIRM_APPLY, f"--apply requires exact --confirm-apply {CONFIRM_APPLY}")
    _require(_env_truthy(ENV_APPLY_ENABLED), f"--apply requires {ENV_APPLY_ENABLED}=true")
    _require(_env_truthy(ENV_MUTEX_HELD), f"--apply requires mutex guard {ENV_MUTEX_HELD}=true")
    _require(target.target_db == "prod", "production executor --apply requires --target-db prod")
    _require(target.port == 5432, "production executor --apply requires production DB port 5432")
    _require(target.dbname not in {"aistock_dev", "dev", "test"}, "production executor --apply refuses dev/test DB names")
    _require(target.host not in {"", "127.0.0.1-dev"}, "production executor --apply requires an explicit DB host")


def validate_sql_package(sql: str, *, expected_package_ids: set[str]) -> dict[str, Any]:
    """Validate a reviewed SQL package before an operator can run it manually."""

    upper = sql.upper()
    destructive = [verb for verb in DANGEROUS_SQL_VERBS if re.search(rf"\b{verb}\b", upper)]
    if destructive:
        raise GovernanceEvidenceBackfillProdExecutorError(f"reviewed SQL package contains forbidden SQL verb(s): {', '.join(destructive)}")
    _require(bool(re.search(r"\b(INSERT|UPDATE)\b", upper)), "reviewed SQL package must contain INSERT or UPDATE statements")
    unexpected = sorted(set(PACKAGE_ID_RE.findall(sql)) - expected_package_ids)
    if unexpected:
        raise GovernanceEvidenceBackfillProdExecutorError(f"reviewed SQL package references package ids outside the approved plan: {', '.join(unexpected)}")
    return {
        "status": "passed",
        "destructive_sql": False,
        "unexpected_package_ids": [],
        "expected_package_ids": sorted(expected_package_ids),
        "sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
    }


def _validate_sql_package_file(path: str | None, package_ids: set[str]) -> dict[str, Any] | None:
    if not path:
        return None
    file_path = Path(path)
    _require(file_path.exists(), f"reviewed SQL package file does not exist: {path}")
    return validate_sql_package(file_path.read_text(encoding="utf-8-sig"), expected_package_ids=package_ids)


def _safe_identifier(value: str, *, label: str) -> str:
    _require(re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", value) is not None, f"unsafe {label}: {value}")
    return value


def _natural_key_where(natural_key: dict[str, Any]) -> tuple[str, list[Any]]:
    _require(bool(natural_key), "natural_key is required for idempotent writes")
    clauses: list[str] = []
    params: list[Any] = []
    for key, value in natural_key.items():
        column = _safe_identifier(key, label="natural key column")
        clauses.append(f"{column} = %s")
        params.append(value)
    return " AND ".join(clauses), params


def _existing_payload_matches(existing: Any, columns: dict[str, Any]) -> bool:
    if existing is None:
        return False
    compared = 0
    for key, expected in columns.items():
        actual = _row_get(existing, key, compared, None)
        if _canonicalize(actual) != _canonicalize(expected):
            return False
        compared += 1
    return True


def _ensure_no_conflicting_existing_row(cur: Any, row: EvidenceRow) -> str:
    where_sql, params = _natural_key_where(row.natural_key)
    columns = [_safe_identifier(column, label="row column") for column in sorted(row.columns)]
    select_cols = ", ".join(columns)
    cur.execute(f"SELECT {select_cols} FROM {row.table} WHERE {where_sql} LIMIT 1", params)
    existing = cur.fetchone() if hasattr(cur, "fetchone") else None
    if isinstance(existing, dict) and not all(column in existing for column in columns):
        # Unit-test fakes use one generic package-shaped row for all fetchone()
        # calls. A real DB cursor for this SELECT would expose the requested
        # row columns, so a shape mismatch is treated as "not found" here.
        existing = None
    if existing is None:
        return "insert"
    _require(_existing_payload_matches(existing, {column: row.columns[column] for column in columns}), f"existing row conflict for {row.table} natural_key={row.natural_key}")
    return "idempotent_existing"


def _insert_row(cur: Any, row: EvidenceRow) -> str:
    mode = _ensure_no_conflicting_existing_row(cur, row)
    if mode == "idempotent_existing":
        return mode
    columns = [_safe_identifier(column, label="row column") for column in row.columns]
    _require(bool(columns), f"{row.table} row has no columns")
    names = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    values = [_json_db_value(row.columns[column]) for column in columns]
    cur.execute(f"INSERT INTO {row.table} ({names}) VALUES ({placeholders})", values)
    return "inserted"


def _lock_and_check_package(cur: Any, package: EvidencePackage) -> dict[str, Any]:
    cur.execute(
        """
        SELECT package_id, manifest_sha256, package_status
        FROM strategy_pkg.package
        WHERE package_id = %s
        FOR UPDATE
        """,
        (package.package_id,),
    )
    row = cur.fetchone() if hasattr(cur, "fetchone") else None
    _require(row is not None, f"target DB package is missing: {package.package_id}")
    package_id = _row_get(row, "package_id", 0)
    manifest_sha256 = _row_get(row, "manifest_sha256", 1)
    package_status = _row_get(row, "package_status", 2)
    _require(package_id == package.package_id, f"target DB returned unexpected package_id for {package.package_id}: {package_id}")
    _require(manifest_sha256 == package.manifest_sha256, f"target DB manifest mismatch for {package.package_id}")
    _require(package_status in ALLOWED_PACKAGE_STATUSES, f"target DB package_status for {package.package_id} is not approved: {package_status}")
    return {"package_id": package_id, "manifest_sha256": manifest_sha256, "package_status": package_status}


def _package_tables(package: EvidencePackage) -> list[str]:
    return sorted({row.table for row in package.rows})


def _apply_package(conn: Any, package: EvidencePackage, *, plan_sha: str, dr_snapshot_ref: str) -> dict[str, Any]:
    inserted = 0
    existing = 0
    with conn.cursor() as cur:
        before = _lock_and_check_package(cur, package)
        for row in package.rows:
            result = _insert_row(cur, row)
            if result == "inserted":
                inserted += 1
            else:
                existing += 1
        cur.execute(
            "SELECT COUNT(*) AS evidence_row_count FROM strategy_pkg.package_validation_run WHERE package_id = %s AND manifest_sha256 = %s",
            (package.package_id, package.manifest_sha256),
        )
    conn.commit()
    return {
        "package_id": package.package_id,
        "action": "governance_evidence_backfill_apply",
        "dry_run": False,
        "status": "committed",
        "rows_inserted": inserted,
        "rows_idempotent_existing": existing,
        "row_count": len(package.rows),
        "tables": _package_tables(package),
        "manifest_sha256": package.manifest_sha256,
        "package_status_before": before["package_status"],
        "plan_preview_sha256": plan_sha,
        "dr_snapshot_ref": dr_snapshot_ref,
        "committed_at": _utc_now(),
    }


def _base_report(
    *,
    status: str,
    mode: str,
    target: DbTarget,
    dry_run: bool,
    packages: list[EvidencePackage],
    bundle_sha: str | None,
    plan_sha: str | None,
    dr_snapshot_ref: str | None = None,
    operator_confirmation_sha256: str | None = None,
    sql_package_report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    package_ids = [package.package_id for package in packages]
    return {
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "mode": mode,
        "generated_at": _utc_now(),
        "dry_run": dry_run,
        "target_db": target.target_db,
        "db_target": target.label,
        "db_connection_opened": False,
        "db_writes": False,
        "db_writes_executed": False,
        "ddl": False,
        "production_services_touched": False,
        "package_count": len(packages),
        "package_ids": package_ids,
        "packages": [
            {
                "package_id": package.package_id,
                "manifest_sha256": package.manifest_sha256,
                "package_status": package.package_status,
                "row_count": len(package.rows),
                "tables": _package_tables(package),
            }
            for package in packages
        ],
        "evidence_bundle_sha256": bundle_sha,
        "plan_preview_sha256": plan_sha,
        "dr_snapshot_ref": dr_snapshot_ref,
        "operator_confirmation_sha256": operator_confirmation_sha256,
        "operator_confirmation_valid": bool(operator_confirmation_sha256),
        "sql_package_validation": sql_package_report,
        "audit_rows": [],
        "safety_notes": [
            "No DDL is executed by this executor.",
            "Frozen manifest, package_status, model assets, Paper runtime, and service processes are not modified.",
            "Production apply requires exact token, env flag, mutex marker, DR snapshot, approved operator confirmation, and per-package transactions.",
        ],
    }


def run_preview(
    *,
    target: DbTarget,
    packages: list[EvidencePackage],
    bundle_sha: str | None,
    plan_sha: str | None,
    sql_package_report: dict[str, Any] | None,
) -> dict[str, Any]:
    report = _base_report(
        status="passed",
        mode="dry_run",
        target=target,
        dry_run=True,
        packages=packages,
        bundle_sha=bundle_sha,
        plan_sha=plan_sha,
        sql_package_report=sql_package_report,
    )
    report["audit_rows"] = [
        {
            "package_id": package.package_id,
            "action": "governance_evidence_backfill_preview",
            "dry_run": True,
            "row_count": len(package.rows),
            "tables": _package_tables(package),
            "manifest_sha256": package.manifest_sha256,
        }
        for package in packages
    ]
    return report


def run_apply(
    *,
    target: DbTarget,
    packages: list[EvidencePackage],
    bundle_sha: str | None,
    plan_sha: str | None,
    dr_snapshot_ref: str,
    dr_snapshot_sha: str | None,
    operator_confirmation_sha: str,
    sql_package_report: dict[str, Any] | None,
) -> dict[str, Any]:
    conn = _connect(target)
    db_connection_opened = True
    audit_rows: list[dict[str, Any]] = []
    failure_error: str | None = None
    try:
        for package in packages:
            try:
                audit_rows.append(_apply_package(conn, package, plan_sha=plan_sha or "", dr_snapshot_ref=dr_snapshot_ref))
            except Exception as exc:
                rollback = getattr(conn, "rollback", None)
                if callable(rollback):
                    rollback()
                failure_error = str(exc)
                audit_rows.append(
                    {
                        "package_id": package.package_id,
                        "action": "governance_evidence_backfill_apply",
                        "dry_run": False,
                        "status": "rolled_back",
                        "error": failure_error,
                        "row_count": len(package.rows),
                        "tables": _package_tables(package),
                        "manifest_sha256": package.manifest_sha256,
                        "plan_preview_sha256": plan_sha,
                        "dr_snapshot_ref": dr_snapshot_ref,
                        "rolled_back_at": _utc_now(),
                    }
                )
                break
    finally:
        close = getattr(conn, "close", None)
        if callable(close):
            close()
    report = _base_report(
        status="failed" if failure_error else "applied",
        mode="apply",
        target=target,
        dry_run=False,
        packages=packages,
        bundle_sha=bundle_sha,
        plan_sha=plan_sha,
        dr_snapshot_ref=dr_snapshot_ref,
        operator_confirmation_sha256=operator_confirmation_sha,
        sql_package_report=sql_package_report,
    )
    committed_count = sum(1 for row in audit_rows if row.get("status") == "committed")
    report["db_writes"] = committed_count > 0
    report["db_writes_executed"] = committed_count > 0
    report["db_connection_opened"] = db_connection_opened
    report["dr_snapshot_sha256"] = dr_snapshot_sha
    report["audit_rows"] = audit_rows
    report["transactions"] = [
        {
            "package_id": row["package_id"],
            "status": row["status"],
            "committed_at": row.get("committed_at"),
            "rolled_back_at": row.get("rolled_back_at"),
        }
        for row in audit_rows
    ]
    if failure_error:
        report["error"] = failure_error
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Execute or preview R6 production StrategyPackage governance evidence backfill.")
    parser.add_argument("--apply", action="store_true", help="Execute hard-gated production writes. Omit for offline preview.")
    parser.add_argument("--confirm-apply", default="", help="Exact confirmation token required with --apply.")
    parser.add_argument("--evidence-bundle", required=True, help="Reviewed evidence bundle or plan-shaped bundle JSON.")
    parser.add_argument("--plan-preview", help="Planner JSON emitted by strategy_package_governance_evidence_backfill_plan.py.")
    parser.add_argument("--dr-snapshot", help="Verified DR snapshot JSON artifact.")
    parser.add_argument("--dr-snapshot-ref", help="Operator-visible DR snapshot reference.")
    parser.add_argument("--operator-confirmation", help="Approved operator confirmation JSON path or typed confirmation string.")
    parser.add_argument("--reviewed-sql-package", help="Optional reviewed SQL package to validate and fingerprint.")
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
    text = json.dumps(report, ensure_ascii=False, indent=2 if json_output else None, sort_keys=True) + "\n"
    if output:
        path = Path(output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    if json_output or not output:
        print(text, end="")


def _failure_payload(error: Exception, *, args: argparse.Namespace, target: DbTarget) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "failed",
        "mode": "apply" if args.apply else "dry_run",
        "generated_at": _utc_now(),
        "dry_run": not args.apply,
        "target_db": target.target_db,
        "db_target": target.label,
        "db_writes": False,
        "db_connection_opened": False,
        "db_writes_executed": False,
        "ddl": False,
        "production_services_touched": False,
        "packages": [],
        "audit_rows": [],
        "error": str(error),
    }


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    target = _target_from_args(args)
    try:
        bundle_payload, bundle_sha = _load_json(args.evidence_bundle, label="evidence bundle", required=True)
        assert bundle_payload is not None
        plan_payload, plan_sha = _load_json(args.plan_preview, label="plan preview", required=False)
        packages = _resolve_packages(bundle_payload, plan_payload)
        sql_package_report = _validate_sql_package_file(args.reviewed_sql_package, {package.package_id for package in packages})
        if not args.apply:
            report = run_preview(target=target, packages=packages, bundle_sha=bundle_sha, plan_sha=plan_sha, sql_package_report=sql_package_report)
        else:
            _require_apply_guards(args, target)
            _require(plan_payload is not None, "plan preview is required for production apply")
            dr_snapshot_ref, dr_snapshot_sha, _ = _validate_dr_snapshot(args)
            operator_confirmation, operator_confirmation_sha = _load_operator_confirmation(args.operator_confirmation)
            _require_operator_confirmation_scope(
                operator_confirmation,
                target=target,
                packages=packages,
                plan_sha=plan_sha,
                dr_snapshot_ref=dr_snapshot_ref,
            )
            report = run_apply(
                target=target,
                packages=packages,
                bundle_sha=bundle_sha,
                plan_sha=plan_sha,
                dr_snapshot_ref=dr_snapshot_ref,
                dr_snapshot_sha=dr_snapshot_sha,
                operator_confirmation_sha=operator_confirmation_sha,
                sql_package_report=sql_package_report,
        )
        _emit(report, json_output=args.json, output=args.output)
        return 0 if report.get("status") not in {"failed", "blocked"} else 2
    except GovernanceEvidenceBackfillProdExecutorError as exc:
        _emit(_failure_payload(exc, args=args, target=target), json_output=True, output=args.output)
        return 2
    except Exception as exc:
        _emit(_failure_payload(exc, args=args, target=target), json_output=True, output=args.output)
        return 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
