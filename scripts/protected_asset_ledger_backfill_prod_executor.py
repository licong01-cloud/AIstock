"""Production-capable protected asset ledger backfill executor.

Default mode is an offline preview: it validates reviewed ledger input and emits
JSON without opening a database connection. The --apply path is intentionally
hard gated by an explicit flag, exact token, two environment guards, production
DB targeting, a verified DR snapshot/reference, a passed plan preview, and an
approved operator confirmation before any DB connection is attempted.

Ledger scope is intentionally narrow: only append/idempotent writes to
strategy_pkg.package_asset are supported, each package is handled in its own
transaction, audit rows are JSON serializable, and this executor performs no DDL
and no service calls.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


SCHEMA_VERSION = "aistock_protected_asset_ledger_backfill_prod_executor_v1"
PLAN_SCHEMA_VERSION = "aistock_protected_asset_ledger_backfill_plan_v1"
CONFIRM_APPLY = "APPLY_PROTECTED_ASSET_LEDGER_BACKFILL_PROD"
ENV_APPLY_ENABLED = "AISTOCK_PROTECTED_ASSET_LEDGER_BACKFILL_PROD_APPLY_ENABLED"
ENV_MUTEX_HELD = "AISTOCK_PROTECTED_ASSET_LEDGER_BACKFILL_MUTEX_HELD"
ALLOWED_PACKAGE_STATUSES = {"BACKTEST_APPROVED", "SELECTION_ENABLED", "PAPER_ENABLED"}
ALLOWED_TABLES = {"strategy_pkg.package_asset"}
EXPECTED_PACKAGE_COUNT = 4
REQUIRED_ASSET_TYPE = "protected_asset_ledger_evidence"
REQUIRED_ASSET_REF = "governance/protected_asset_ledger_backfill"
DANGEROUS_SQL_VERBS = ("DROP", "ALTER", "TRUNCATE", "CREATE", "GRANT", "REVOKE", "COMMENT", "VACUUM", "COPY", "MERGE")
PACKAGE_ID_RE = re.compile(r"\bpkg_[A-Za-z0-9_\-]+\b")


class ProtectedAssetLedgerBackfillProdExecutorError(RuntimeError):
    """Raised when the production ledger backfill executor refuses to proceed."""


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
            "application_name": "AIstock-protected-asset-ledger-prod-backfill",
            "options": "-c client_encoding=utf8",
        }


@dataclass(frozen=True)
class LedgerRow:
    natural_key: dict[str, Any]
    columns: dict[str, Any]
    action: str = "apply_protected_asset_ledger"
    table: str = "strategy_pkg.package_asset"


@dataclass(frozen=True)
class LedgerPackage:
    package_id: str
    manifest_sha256: str | None
    package_status: str | None
    rows: list[LedgerRow]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ProtectedAssetLedgerBackfillProdExecutorError(message)


def _env_truthy(name: str) -> bool:
    value = (os.getenv(name) or "").strip().lower()
    return value in {"1", "true", "yes", "y", "on"}


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
        raise ProtectedAssetLedgerBackfillProdExecutorError(f"{label} file is invalid JSON: {exc}") from exc
    _require(isinstance(payload, dict), f"{label} file must contain a JSON object")
    return payload, hashlib.sha256(data).hexdigest()


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
        except Exception:  # pragma: no cover
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
    return DbTarget(args.target_db, args.db_host, args.db_port, args.db_name, args.db_user, password)


def _connect(target: DbTarget) -> Any:
    try:
        import psycopg2  # type: ignore
    except ImportError as exc:  # pragma: no cover
        raise ProtectedAssetLedgerBackfillProdExecutorError("psycopg2 is required for production DB apply") from exc
    try:
        return psycopg2.connect(**target.as_psycopg2_kwargs())
    except Exception as exc:  # pragma: no cover
        raise ProtectedAssetLedgerBackfillProdExecutorError(f"failed to connect to DB target {target.label}: {exc}") from exc


def _text(value: Any, *, label: str, required: bool = True) -> str | None:
    if value is None and not required:
        return None
    _require(isinstance(value, str) and value.strip(), f"{label} must be a non-empty string")
    return str(value).strip()


def _default_ledger_columns(package_id: str, manifest_sha256: str | None) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "source": "protected_asset_ledger_backfill_prod_executor",
        "asset_ref": REQUIRED_ASSET_REF,
    }
    if manifest_sha256:
        metadata["manifest_sha256"] = manifest_sha256
    return {
        "package_id": package_id,
        "asset_type": REQUIRED_ASSET_TYPE,
        "asset_ref": REQUIRED_ASSET_REF,
        "asset_sha256": manifest_sha256,
        "metadata": metadata,
        "asset_role": "governance_evidence",
        "protected_asset": True,
        "source_uri": "strategy_pkg.package",
    }


def _normalize_row(raw: dict[str, Any], *, package_id: str) -> LedgerRow:
    table = raw.get("table", "strategy_pkg.package_asset")
    _require(table in ALLOWED_TABLES, f"unsupported ledger table: {table}")
    natural_key = raw.get("natural_key") or {}
    columns = raw.get("columns") or {}
    _require(isinstance(natural_key, dict), "ledger row natural_key must be an object")
    _require(isinstance(columns, dict), "ledger row columns must be an object")
    expected_natural_key = {
        "package_id": package_id,
        "asset_type": REQUIRED_ASSET_TYPE,
        "asset_ref": REQUIRED_ASSET_REF,
    }
    _require(natural_key == expected_natural_key, f"ledger row natural_key must be exactly {expected_natural_key}")
    _require(columns.get("package_id") == package_id, f"ledger row must bind package_id to package {package_id}")
    _require(columns.get("asset_type") == REQUIRED_ASSET_TYPE, f"ledger row asset_type must be {REQUIRED_ASSET_TYPE}")
    _require(columns.get("asset_ref") == REQUIRED_ASSET_REF, f"ledger row asset_ref must be {REQUIRED_ASSET_REF}")
    _require(columns.get("protected_asset") is True, "ledger row must set protected_asset=true")
    _require(str(columns.get("asset_role", "")) == "governance_evidence", "ledger row asset_role must be governance_evidence")
    _require("asset_id" not in columns, "ledger row must not set asset_id; DB default/sequence must own it")
    return LedgerRow(natural_key=natural_key, columns=columns, action=str(raw.get("action") or "apply_protected_asset_ledger"))


def _package_has_planned_asset(raw: dict[str, Any]) -> bool:
    planned = raw.get("asset_planned", raw.get("ledger_planned", 1))
    try:
        return int(planned) > 0
    except (TypeError, ValueError):
        return bool(planned)


def _normalize_package(raw: dict[str, Any], *, label: str) -> LedgerPackage:
    package_id = str(_text(raw.get("package_id") or raw.get("id"), label="package_id"))
    manifest_sha256 = _text(raw.get("manifest_sha256"), label=f"{package_id}.manifest_sha256", required=False)
    package_status = _text(raw.get("package_status"), label=f"{package_id}.package_status", required=False)
    if package_status is not None:
        _require(package_status in ALLOWED_PACKAGE_STATUSES, f"package_status for {package_id} is not approved for production backfill: {package_status}")
    raw_rows = raw.get("rows")
    if raw_rows is None:
        _require(_package_has_planned_asset(raw), f"{label} package {package_id} has no planned protected asset ledger row")
        raw_rows = [{
            "table": "strategy_pkg.package_asset",
            "natural_key": {"package_id": package_id, "asset_type": REQUIRED_ASSET_TYPE, "asset_ref": REQUIRED_ASSET_REF},
            "columns": _default_ledger_columns(package_id, manifest_sha256),
        }]
    _require(isinstance(raw_rows, list) and raw_rows, f"{label} package {package_id} must include planned ledger rows")
    return LedgerPackage(package_id, manifest_sha256, package_status, [_normalize_row(row, package_id=package_id) for row in raw_rows])


def _normalize_packages(payload: dict[str, Any], *, label: str) -> list[LedgerPackage]:
    packages = payload.get("packages")
    _require(isinstance(packages, list), f"{label}.packages must be a list")
    normalized: list[LedgerPackage] = []
    seen: set[str] = set()
    for raw in packages:
        _require(isinstance(raw, dict), f"{label}.packages entries must be objects")
        package = _normalize_package(raw, label=label)
        _require(package.package_id not in seen, f"duplicate package_id in {label}: {package.package_id}")
        seen.add(package.package_id)
        normalized.append(package)
    _require(bool(normalized), f"{label} must contain at least one package")
    return normalized


def _package_index(packages: Iterable[LedgerPackage]) -> dict[str, LedgerPackage]:
    return {package.package_id: package for package in packages}


def _validate_plan_preview(plan: dict[str, Any]) -> list[LedgerPackage]:
    _require(plan.get("schema_version") == PLAN_SCHEMA_VERSION, f"plan preview schema_version must be {PLAN_SCHEMA_VERSION}")
    _require(plan.get("status") == "passed", "plan preview must have status=passed")
    _require(plan.get("dry_run") is True, "plan preview must be a dry-run report")
    _require(plan.get("db_writes") is False, "plan preview must be generated without DB writes")
    _require(plan.get("ddl") is False, "plan preview must declare ddl=false")
    _require(plan.get("blocked_packages") in ({}, None), "plan preview must not contain blocked packages")
    if "db_connection_opened" in plan:
        _require(plan.get("db_connection_opened") is False, "production plan preview must be offline and must not open a DB connection")
    packages = _normalize_packages(plan, label="plan preview")
    _require(len(packages) == EXPECTED_PACKAGE_COUNT, f"plan preview must contain exactly {EXPECTED_PACKAGE_COUNT} packages")
    return packages


def _validate_bundle_alignment(plan_packages: list[LedgerPackage], bundle_payload: dict[str, Any] | None) -> None:
    if not bundle_payload or "packages" not in bundle_payload:
        return
    bundle_packages = _normalize_packages(bundle_payload, label="evidence bundle")
    plan_by_id = _package_index(plan_packages)
    bundle_by_id = _package_index(bundle_packages)
    _require(set(bundle_by_id) == set(plan_by_id), "evidence bundle package_id set must match plan preview package_id set")
    for package_id, bundle_package in bundle_by_id.items():
        plan_package = plan_by_id[package_id]
        if bundle_package.manifest_sha256:
            _require(bundle_package.manifest_sha256 == plan_package.manifest_sha256, f"evidence bundle manifest mismatch for {package_id}")
        if bundle_package.package_status:
            _require(bundle_package.package_status == plan_package.package_status, f"evidence bundle package_status mismatch for {package_id}")


def _resolve_packages(bundle: dict[str, Any] | None, plan: dict[str, Any] | None) -> list[LedgerPackage]:
    if plan is not None:
        plan_packages = _validate_plan_preview(plan)
        _validate_bundle_alignment(plan_packages, bundle)
        return plan_packages
    _require(bundle is not None, "offline preview requires --evidence-bundle or --plan-preview")
    return _normalize_packages(bundle, label="evidence bundle")


def _validate_dr_snapshot(args: argparse.Namespace) -> tuple[str, str | None]:
    snapshot_ref = (args.dr_snapshot_ref or "").strip()
    snapshot_sha: str | None = None
    _require(bool(args.dr_snapshot), "DR snapshot file is required for production apply")
    if args.dr_snapshot:
        snapshot_payload, snapshot_sha = _load_json(args.dr_snapshot, label="DR snapshot", required=True)
        assert snapshot_payload is not None
        status = str(snapshot_payload.get("status", "")).strip().lower()
        _require(status in {"verified", "passed", "completed"}, "DR snapshot must be verified before production apply")
        snapshot_ref = snapshot_ref or str(snapshot_payload.get("snapshot_id") or snapshot_payload.get("snapshot_ref") or snapshot_payload.get("checksum") or Path(args.dr_snapshot).name)
    _require(snapshot_ref, "DR snapshot ref is required for production apply")
    return snapshot_ref, snapshot_sha


def _load_operator_confirmation(value: str | None) -> tuple[dict[str, Any], str]:
    _require(value is not None and str(value).strip(), "operator confirmation is required for production apply")
    raw = str(value).strip()
    path = Path(raw)
    if path.exists():
        payload, sha = _load_json(raw, label="operator confirmation", required=True)
        assert payload is not None and sha is not None
        _require(str(payload.get("status", "")).strip().lower() == "approved", "operator confirmation must have status=approved")
        _require(bool(str(payload.get("confirmation") or payload.get("typed_confirmation") or "").strip()), "operator confirmation must include typed confirmation text")
        return payload, sha
    return {"status": "approved", "confirmation": raw}, hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _require_operator_confirmation_scope(payload: dict[str, Any], *, target: DbTarget, packages: list[LedgerPackage], plan_sha: str | None, dr_snapshot_ref: str) -> None:
    text = str(payload.get("confirmation") or payload.get("typed_confirmation") or "").strip()
    _require(CONFIRM_APPLY in text, "operator confirmation must include the exact production confirmation token")
    _require(target.label in text or target.dbname in text, "operator confirmation must include the target DB label or DB name")
    _require(plan_sha is not None and plan_sha in text, "operator confirmation must include the plan preview SHA256")
    _require(dr_snapshot_ref in text, "operator confirmation must include the DR snapshot ref")
    missing = [package.package_id for package in packages if package.package_id not in text]
    _require(not missing, f"operator confirmation must include every package id: {', '.join(missing)}")


def _require_apply_guards(args: argparse.Namespace, target: DbTarget) -> None:
    _require(args.confirm_apply == CONFIRM_APPLY, f"--apply requires exact --confirm-apply {CONFIRM_APPLY}")
    _require(_env_truthy(ENV_APPLY_ENABLED), f"--apply requires {ENV_APPLY_ENABLED}=true")
    _require(_env_truthy(ENV_MUTEX_HELD), f"--apply requires mutex guard {ENV_MUTEX_HELD}=true")
    _require(target.target_db == "prod", "production executor --apply requires --target-db prod")
    _require(target.port == 5432, "production executor --apply requires production DB port 5432")
    dbname_lower = target.dbname.lower()
    _require("dev" not in dbname_lower and "test" not in dbname_lower, "production executor --apply refuses dev/test DB names")
    _require(target.host not in {"", "127.0.0.1-dev"}, "production executor --apply requires an explicit DB host")


def validate_sql_package(sql: str, *, expected_package_ids: set[str]) -> dict[str, Any]:
    """Validate a reviewed SQL package before an operator can run it manually."""
    upper = sql.upper()
    destructive = [verb for verb in DANGEROUS_SQL_VERBS if re.search(rf"\b{verb}\b", upper)]
    if destructive:
        raise ProtectedAssetLedgerBackfillProdExecutorError(f"reviewed SQL package contains forbidden SQL verb(s): {', '.join(destructive)}")
    _require("STRATEGY_PKG.PACKAGE_ASSET" in upper, "reviewed SQL package must target strategy_pkg.package_asset")
    unexpected = sorted(set(PACKAGE_ID_RE.findall(sql)) - expected_package_ids)
    if unexpected:
        raise ProtectedAssetLedgerBackfillProdExecutorError(f"reviewed SQL package references package ids outside the approved plan: {', '.join(unexpected)}")
    return {"status": "passed", "destructive_sql": False, "unexpected_package_ids": [], "expected_package_ids": sorted(expected_package_ids), "sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest()}


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
    for index, key in enumerate(columns):
        if _canonicalize(_row_get(existing, key, index, None)) != _canonicalize(columns[key]):
            return False
    return True


def _insert_row(cur: Any, row: LedgerRow) -> str:
    where_sql, params = _natural_key_where(row.natural_key)
    columns = [_safe_identifier(column, label="row column") for column in sorted(row.columns)]
    cur.execute(f"SELECT {', '.join(columns)} FROM strategy_pkg.package_asset WHERE {where_sql} LIMIT 1", params)
    existing = cur.fetchone() if hasattr(cur, "fetchone") else None
    if isinstance(existing, dict) and not all(column in existing for column in columns):
        existing = None
    if existing is not None:
        _require(_existing_payload_matches(existing, {column: row.columns[column] for column in columns}), f"existing row conflict for strategy_pkg.package_asset natural_key={row.natural_key}")
        return "idempotent_existing"
    insert_columns = [_safe_identifier(column, label="row column") for column in row.columns]
    values = [_json_db_value(row.columns[column]) for column in insert_columns]
    cur.execute(f"INSERT INTO strategy_pkg.package_asset ({', '.join(insert_columns)}) VALUES ({', '.join(['%s'] * len(insert_columns))})", values)
    return "inserted"


def _lock_and_check_package(cur: Any, package: LedgerPackage) -> dict[str, Any]:
    cur.execute("SELECT package_id, manifest_sha256, package_status FROM strategy_pkg.package WHERE package_id = %s FOR UPDATE", (package.package_id,))
    row = cur.fetchone() if hasattr(cur, "fetchone") else None
    _require(row is not None, f"target DB package is missing: {package.package_id}")
    package_id = _row_get(row, "package_id", 0)
    manifest_sha256 = _row_get(row, "manifest_sha256", 1)
    package_status = _row_get(row, "package_status", 2)
    _require(package_id == package.package_id, f"target DB returned unexpected package_id for {package.package_id}: {package_id}")
    if package.manifest_sha256:
        _require(manifest_sha256 == package.manifest_sha256, f"target DB manifest mismatch for {package.package_id}")
    _require(package_status in ALLOWED_PACKAGE_STATUSES, f"target DB package_status for {package.package_id} is not approved: {package_status}")
    return {"manifest_sha256": manifest_sha256, "package_status": package_status}


def _apply_package(conn: Any, package: LedgerPackage, *, plan_sha: str, dr_snapshot_ref: str, operator: str) -> dict[str, Any]:
    inserted = 0
    existing = 0
    with conn.cursor() as cur:
        before = _lock_and_check_package(cur, package)
        for row in package.rows:
            result = _insert_row(cur, row)
            inserted += 1 if result == "inserted" else 0
            existing += 1 if result == "idempotent_existing" else 0
    conn.commit()
    return {
        "package_id": package.package_id,
        "action": "protected_asset_ledger_backfill_apply",
        "operator": operator,
        "dry_run": False,
        "status": "committed",
        "rows_inserted": inserted,
        "rows_applied": inserted,
        "rows_idempotent_existing": existing,
        "row_count": len(package.rows),
        "tables": ["strategy_pkg.package_asset"],
        "manifest_sha256": before.get("manifest_sha256"),
        "package_status_before": before["package_status"],
        "plan_preview_sha256": plan_sha,
        "plan_hash": plan_sha,
        "dr_snapshot_ref": dr_snapshot_ref,
        "committed_at": _utc_now(),
    }


def _planned_rows(package: LedgerPackage) -> list[dict[str, Any]]:
    return [
        {
            "table": row.table,
            "action": row.action,
            "natural_key": row.natural_key,
            "columns": row.columns,
        }
        for row in package.rows
    ]


def _base_report(*, status: str, mode: str, target: DbTarget, dry_run: bool, packages: list[LedgerPackage], bundle_sha: str | None, plan_sha: str | None, dr_snapshot_ref: str | None = None, operator_confirmation_sha256: str | None = None, sql_package_report: dict[str, Any] | None = None) -> dict[str, Any]:
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
        "package_ids": [package.package_id for package in packages],
        "packages": [
            {
                "package_id": package.package_id,
                "manifest_sha256": package.manifest_sha256,
                "package_status": package.package_status,
                "row_count": len(package.rows),
                "tables": ["strategy_pkg.package_asset"],
                "planned_rows": _planned_rows(package),
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
            "Default preview is offline and opens no DB connection.",
            "No DDL is executed by this executor.",
            "Only strategy_pkg.package_asset protected asset ledger rows are in scope.",
            "No production service calls, file copies, deletes, or manifest mutations are performed.",
            "Production apply requires exact token, env flag, mutex marker, DR snapshot/ref, passed plan preview, approved operator confirmation, and per-package transactions.",
        ],
    }


def run_preview(*, target: DbTarget, packages: list[LedgerPackage], bundle_sha: str | None, plan_sha: str | None, sql_package_report: dict[str, Any] | None) -> dict[str, Any]:
    report = _base_report(status="passed", mode="offline_dry_run", target=target, dry_run=True, packages=packages, bundle_sha=bundle_sha, plan_sha=plan_sha, sql_package_report=sql_package_report)
    report["audit_rows"] = [{"package_id": package.package_id, "action": "protected_asset_ledger_backfill_preview", "dry_run": True, "row_count": len(package.rows), "tables": ["strategy_pkg.package_asset"], "manifest_sha256": package.manifest_sha256, "planned_rows": _planned_rows(package)} for package in packages]
    report["final_status"] = "preview_passed"
    report["rows_inserted"] = 0
    report["rows_idempotent_existing"] = 0
    report["per_package_breakdown"] = report["audit_rows"]
    return report


def run_apply(*, target: DbTarget, packages: list[LedgerPackage], bundle_sha: str | None, plan_sha: str | None, dr_snapshot_ref: str, dr_snapshot_sha: str | None, operator_confirmation_sha: str, operator: str, sql_package_report: dict[str, Any] | None) -> dict[str, Any]:
    conn = _connect(target)
    audit_rows: list[dict[str, Any]] = []
    failure_error: str | None = None
    try:
        for package in packages:
            try:
                audit_rows.append(_apply_package(conn, package, plan_sha=plan_sha or "", dr_snapshot_ref=dr_snapshot_ref, operator=operator))
            except Exception as exc:
                rollback = getattr(conn, "rollback", None)
                if callable(rollback):
                    rollback()
                failure_error = str(exc)
                audit_rows.append({"package_id": package.package_id, "action": "protected_asset_ledger_backfill_apply", "operator": operator, "dry_run": False, "status": "rolled_back", "error": failure_error, "row_count": len(package.rows), "tables": ["strategy_pkg.package_asset"], "manifest_sha256": package.manifest_sha256, "plan_preview_sha256": plan_sha, "plan_hash": plan_sha, "dr_snapshot_ref": dr_snapshot_ref, "rolled_back_at": _utc_now()})
                break
    finally:
        close = getattr(conn, "close", None)
        if callable(close):
            close()
    report = _base_report(status="failed" if failure_error else "applied", mode="apply", target=target, dry_run=False, packages=packages, bundle_sha=bundle_sha, plan_sha=plan_sha, dr_snapshot_ref=dr_snapshot_ref, operator_confirmation_sha256=operator_confirmation_sha, sql_package_report=sql_package_report)
    committed_count = sum(1 for row in audit_rows if row.get("status") == "committed")
    report["db_connection_opened"] = True
    report["db_writes"] = committed_count > 0
    report["db_writes_executed"] = committed_count > 0
    report["dr_snapshot_sha256"] = dr_snapshot_sha
    report["audit_rows"] = audit_rows
    report["rows_inserted"] = sum(int(row.get("rows_inserted", 0)) for row in audit_rows)
    report["rows_idempotent_existing"] = sum(int(row.get("rows_idempotent_existing", 0)) for row in audit_rows)
    report["final_status"] = "failed" if failure_error else "applied"
    report["per_package_breakdown"] = audit_rows
    report["transactions"] = [{"package_id": row["package_id"], "status": row["status"], "committed_at": row.get("committed_at"), "rolled_back_at": row.get("rolled_back_at")} for row in audit_rows]
    if failure_error:
        report["error"] = failure_error
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Execute or preview production protected asset ledger backfill.")
    parser.add_argument("--apply", action="store_true", help="Execute hard-gated production writes. Omit for offline preview.")
    parser.add_argument("--confirm-apply", default="", help="Exact confirmation token required with --apply.")
    parser.add_argument("--evidence-bundle", help="Reviewed ledger evidence bundle JSON. Required if --plan-preview is omitted.")
    parser.add_argument("--plan-preview", help="Passed ledger plan preview JSON. Required with --apply.")
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
    return {"schema_version": SCHEMA_VERSION, "status": "failed", "mode": "apply" if args.apply else "offline_dry_run", "generated_at": _utc_now(), "dry_run": not args.apply, "target_db": target.target_db, "db_target": target.label, "db_writes": False, "db_connection_opened": False, "db_writes_executed": False, "ddl": False, "production_services_touched": False, "packages": [], "audit_rows": [], "error": str(error)}


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    target = _target_from_args(args)
    try:
        bundle_payload, bundle_sha = _load_json(args.evidence_bundle, label="evidence bundle", required=False)
        plan_payload, plan_sha = _load_json(args.plan_preview, label="plan preview", required=False)
        packages = _resolve_packages(bundle_payload, plan_payload)
        sql_package_report = None
        if args.reviewed_sql_package:
            path = Path(args.reviewed_sql_package)
            _require(path.exists(), f"reviewed SQL package file does not exist: {args.reviewed_sql_package}")
            sql_package_report = validate_sql_package(path.read_text(encoding="utf-8-sig"), expected_package_ids={package.package_id for package in packages})
        if not args.apply:
            report = run_preview(target=target, packages=packages, bundle_sha=bundle_sha, plan_sha=plan_sha, sql_package_report=sql_package_report)
        else:
            _require_apply_guards(args, target)
            _require(plan_payload is not None, "plan preview is required for production apply")
            _require(all(package.manifest_sha256 for package in packages), "plan preview must include manifest_sha256 for every package before production apply")
            dr_snapshot_ref, dr_snapshot_sha = _validate_dr_snapshot(args)
            operator_confirmation, operator_confirmation_sha = _load_operator_confirmation(args.operator_confirmation)
            _require_operator_confirmation_scope(operator_confirmation, target=target, packages=packages, plan_sha=plan_sha, dr_snapshot_ref=dr_snapshot_ref)
            operator = str(operator_confirmation.get("operator") or operator_confirmation.get("operator_id") or "operator_confirmation")
            report = run_apply(target=target, packages=packages, bundle_sha=bundle_sha, plan_sha=plan_sha, dr_snapshot_ref=dr_snapshot_ref, dr_snapshot_sha=dr_snapshot_sha, operator_confirmation_sha=operator_confirmation_sha, operator=operator, sql_package_report=sql_package_report)
        _emit(report, json_output=args.json, output=args.output)
        return 0 if report.get("status") not in {"failed", "blocked"} else 2
    except ProtectedAssetLedgerBackfillProdExecutorError as exc:
        _emit(_failure_payload(exc, args=args, target=target), json_output=True, output=args.output)
        return 2
    except Exception as exc:
        _emit(_failure_payload(exc, args=args, target=target), json_output=True, output=args.output)
        return 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
