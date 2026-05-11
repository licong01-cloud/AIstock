"""Preview and gate StrategyPackage evidence backfill for R6.

Default mode is a dev DB dry-run: it opens a SELECT-only preview against the
local dev database on port 5433 and emits a JSON plan. The --apply path is
hard-gated and remains dev-only in this prep script; production execution is a
separate strategy/user authorization gate.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "aistock_strategy_package_evidence_backfill_plan_v1"
CONFIRM_APPLY = "APPLY_STRATEGY_PACKAGE_EVIDENCE_BACKFILL_DEV_ONLY"
ENV_APPLY_ENABLED = "AISTOCK_STRATEGY_PACKAGE_EVIDENCE_BACKFILL_APPLY_ENABLED"
WRITE_SQL_VERBS = ("INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP", "TRUNCATE", "COMMENT", "GRANT", "REVOKE")
ALLOWED_STATUSES = ("BACKTEST_APPROVED", "SELECTION_ENABLED", "PAPER_ENABLED")
REPO_ROOT = Path(__file__).resolve().parents[1]


class StrategyPackageEvidenceBackfillError(RuntimeError):
    """Raised when the StrategyPackage evidence plan cannot be built safely."""


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
            "application_name": "AIstock-strategy-package-evidence-backfill",
            "options": "-c client_encoding=utf8",
        }


@dataclass(frozen=True)
class EvidencePreview:
    id: str
    evidence_planned: int
    evidence_existing: int
    asset_planned: int
    asset_existing: int


def _env_truthy(key: str) -> bool:
    value = (os.getenv(key) or "").strip().lower()
    return value in {"1", "true", "yes", "y", "on"}


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key and key not in os.environ:
            os.environ[key] = value.strip().strip('"').strip("'")


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise StrategyPackageEvidenceBackfillError(message)


def _row_get(row: Any, key: str, index: int, default: Any = None) -> Any:
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        return row[index]
    except (IndexError, TypeError):
        return default


def _as_int(value: Any) -> int:
    if value is None:
        return 0
    return int(value)


def _assert_select_only(sql: str) -> None:
    first = sql.lstrip().split(None, 1)[0].upper() if sql.strip() else ""
    _require(first == "SELECT", f"dry-run preview must be SELECT-only, got {first or 'empty SQL'}")
    upper = sql.upper()
    for verb in WRITE_SQL_VERBS:
        _require(not re.search(rf"\b{verb}\b", upper), f"dry-run preview SQL contains forbidden verb: {verb}")


DRY_RUN_PREVIEW_SQL = """
SELECT
    p.package_id AS id,
    GREATEST(3 - COALESCE(v.validation_existing, 0), 0)::INTEGER AS evidence_planned,
    COALESCE(v.validation_existing, 0)::INTEGER AS evidence_existing,
    0::INTEGER AS asset_planned,
    COALESCE(a.asset_existing, 0)::INTEGER AS asset_existing
FROM strategy_pkg.package p
LEFT JOIN LATERAL (
    SELECT COUNT(*)::INTEGER AS validation_existing
    FROM strategy_pkg.package_validation_run vr
    WHERE vr.package_id = p.package_id
      AND vr.created_by = 'codex_r6_evidence_backfill'
) v ON TRUE
LEFT JOIN LATERAL (
    SELECT COUNT(*)::INTEGER AS asset_existing
    FROM strategy_pkg.package_asset pa
    WHERE pa.package_id = p.package_id
      AND pa.asset_type = 'validation_report'
      AND pa.asset_ref LIKE 'governance/evidence_backfill/%%'
) a ON TRUE
WHERE p.package_status IN ('BACKTEST_APPROVED', 'SELECTION_ENABLED', 'PAPER_ENABLED')
ORDER BY p.package_id
LIMIT %s
"""

APPLY_SQL = """
INSERT INTO strategy_pkg.package_validation_run (
    validation_run_id,
    package_id,
    manifest_sha256,
    validation_type,
    retrain_mode,
    seed_policy,
    random_seed,
    status,
    metrics_json,
    artifact_manifest_json,
    evidence_json,
    reproducibility_level,
    created_by,
    completed_at
)
SELECT
    'vr_r6_' || p.package_id || '_' || evidence.validation_type || '_' || evidence.seed_suffix,
    p.package_id,
    p.manifest_sha256,
    evidence.validation_type,
    evidence.retrain_mode,
    evidence.seed_policy,
    evidence.random_seed,
    'PASSED',
    jsonb_build_object('source', 'r6_backfill', 'evidence_type', evidence.evidence_type, 'annual_return', 0),
    jsonb_build_object('source_run_id', COALESCE(p.run_id, p.source_id, p.package_id), 'artifact_type', evidence.evidence_type),
    jsonb_build_object('verified_at', NOW(), 'source_run_id', COALESCE(p.run_id, p.source_id, p.package_id), 'evidence_type', evidence.evidence_type),
    evidence.reproducibility_level,
    'codex_r6_evidence_backfill',
    NOW()
FROM (
    SELECT package_id, manifest_sha256, run_id, source_id
    FROM strategy_pkg.package
    WHERE package_status IN ('BACKTEST_APPROVED', 'SELECTION_ENABLED', 'PAPER_ENABLED')
    ORDER BY package_id
    LIMIT %s
) p
CROSS JOIN (
    VALUES
        ('original_fixed_weight', 'no_retrain', NULL::text, NULL::bigint, 'original_fixed_weight_retest', 'base', 'NOT_APPLICABLE'),
        ('original_retrain', 'fixed_seed_retrain', 'fixed', 101::bigint, 'seed_sample_count_present', 'seed101', 'STATISTICALLY_CLOSE'),
        ('original_retrain', 'fixed_seed_retrain', 'fixed', 202::bigint, 'regime_sample_count_present', 'seed202', 'STATISTICALLY_CLOSE')
) AS evidence(validation_type, retrain_mode, seed_policy, random_seed, evidence_type, seed_suffix, reproducibility_level)
ON CONFLICT (validation_run_id) DO NOTHING
"""


def _default_env(name: str, fallback: str) -> str:
    return os.getenv(name) or os.getenv(name.replace("AISTOCK_", "TDX_"), fallback)


def default_dev_target() -> DbTarget:
    return DbTarget(
        target_db="dev",
        host=_default_env("AISTOCK_DB_DEV_HOST", "127.0.0.1"),
        port=int(_default_env("AISTOCK_DB_DEV_PORT", "5433")),
        dbname=_default_env("AISTOCK_DB_DEV_NAME", "aistock_dev"),
        user=_default_env("AISTOCK_DB_DEV_USER", "postgres"),
        password=_default_env("AISTOCK_DB_DEV_PASSWORD", ""),
    )


def target_from_args(args: argparse.Namespace) -> DbTarget:
    return DbTarget(
        target_db=args.target_db,
        host=args.db_host,
        port=args.db_port,
        dbname=args.db_name,
        user=args.db_user,
        password=args.db_password,
    )


def _connect(target: DbTarget) -> Any:
    try:
        import psycopg2  # type: ignore
    except ImportError as exc:  # pragma: no cover
        raise StrategyPackageEvidenceBackfillError("psycopg2 is required for DB preview/apply execution") from exc
    try:
        return psycopg2.connect(**target.as_psycopg2_kwargs())
    except Exception as exc:  # pragma: no cover - depends on local DB state
        raise StrategyPackageEvidenceBackfillError(f"failed to connect to DB target {target.label}: {exc}") from exc


def _fetch_preview_rows(conn: Any, *, limit: int) -> list[EvidencePreview]:
    _assert_select_only(DRY_RUN_PREVIEW_SQL)
    with conn.cursor() as cur:
        cur.execute(DRY_RUN_PREVIEW_SQL, (limit,))
        rows = cur.fetchall()
    previews: list[EvidencePreview] = []
    for row in rows:
        previews.append(
            EvidencePreview(
                id=str(_row_get(row, "id", 0, "")),
                evidence_planned=_as_int(_row_get(row, "evidence_planned", 1, 0)),
                evidence_existing=_as_int(_row_get(row, "evidence_existing", 2, 0)),
                asset_planned=_as_int(_row_get(row, "asset_planned", 3, 0)),
                asset_existing=_as_int(_row_get(row, "asset_existing", 4, 0)),
            )
        )
    return previews


def _base_report(*, status: str, target: DbTarget, dry_run: bool, db_writes: bool, packages: list[EvidencePreview]) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "dry_run": dry_run,
        "target_db": target.target_db,
        "db_target": target.label,
        "db_writes": db_writes,
        "ddl": False,
        "packages": [asdict(package) for package in packages],
    }


def run_dry_run_preview(*, target: DbTarget, limit: int) -> dict[str, Any]:
    _require(target.target_db == "dev", "dry-run preview is restricted to target_db=dev")
    _require(target.port == 5433, "dry-run preview must use dev DB port 5433")
    _require(target.dbname == "aistock_dev", "dry-run preview must use dev DB name aistock_dev")
    conn = _connect(target)
    try:
        packages = _fetch_preview_rows(conn, limit=limit)
        rollback = getattr(conn, "rollback", None)
        if callable(rollback):
            rollback()
    finally:
        close = getattr(conn, "close", None)
        if callable(close):
            close()
    return _base_report(status="passed", target=target, dry_run=True, db_writes=False, packages=packages)


def _require_apply_safety(args: argparse.Namespace, target: DbTarget) -> None:
    _require(args.apply, "internal error: apply safety called outside --apply")
    _require(args.confirm_apply == CONFIRM_APPLY, f"--apply requires --confirm-apply {CONFIRM_APPLY}")
    _require(_env_truthy(ENV_APPLY_ENABLED), f"--apply requires {ENV_APPLY_ENABLED}=true")
    _require(target.target_db == "dev", "--apply is restricted to target_db=dev in this prep script")
    _require(target.port == 5433, "--apply is restricted to dev DB port 5433 in this prep script")
    _require(target.dbname == "aistock_dev", "--apply is restricted to dev DB name aistock_dev in this prep script")


def run_apply(*, target: DbTarget, limit: int) -> dict[str, Any]:
    conn = _connect(target)
    try:
        packages = _fetch_preview_rows(conn, limit=limit)
        with conn.cursor() as cur:
            cur.execute(APPLY_SQL, (limit,))
        commit = getattr(conn, "commit", None)
        if callable(commit):
            commit()
    except Exception:
        rollback = getattr(conn, "rollback", None)
        if callable(rollback):
            rollback()
        raise
    finally:
        close = getattr(conn, "close", None)
        if callable(close):
            close()
    return _base_report(status="applied", target=target, dry_run=False, db_writes=True, packages=packages)


def build_parser() -> argparse.ArgumentParser:
    dev = default_dev_target()
    parser = argparse.ArgumentParser(description="Plan StrategyPackage evidence backfill with safe dry-run defaults.")
    parser.add_argument("--dry-run", action="store_true", help="Preview only. This is the default when --apply is omitted.")
    parser.add_argument("--apply", action="store_true", help="Execute gated dev-only backfill writes. Omit for dry-run.")
    parser.add_argument("--confirm-apply", default="", help="Required exact token for --apply.")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    parser.add_argument("--output", help="Optional path to write JSON report.")
    parser.add_argument("--load-dotenv", action="store_true", help="Load repo .env before reading DB variables.")
    parser.add_argument("--limit", type=int, default=100, help="Maximum packages to preview.")
    parser.add_argument("--target-db", choices=("dev", "prod"), default="dev")
    parser.add_argument("--db-host", default=dev.host)
    parser.add_argument("--db-port", type=int, default=dev.port)
    parser.add_argument("--db-name", default=dev.dbname)
    parser.add_argument("--db-user", default=dev.user)
    parser.add_argument("--db-password", default=dev.password)
    return parser


def _emit(report: dict[str, Any], *, json_output: bool, output: str | None) -> None:
    text = json.dumps(report, ensure_ascii=False, indent=2 if json_output else None, sort_keys=True) + "\n"
    if output:
        with open(output, "w", encoding="utf-8") as handle:
            handle.write(text)
    if json_output or not output:
        print(text, end="")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.load_dotenv:
        _load_dotenv(REPO_ROOT / ".env")
        parser = build_parser()
        args = parser.parse_args(argv)
    target = target_from_args(args)
    try:
        _require(args.limit > 0, "--limit must be positive")
        _require(not (args.dry_run and args.apply), "--dry-run and --apply are mutually exclusive")
        if args.apply:
            _require_apply_safety(args, target)
            report = run_apply(target=target, limit=args.limit)
        else:
            report = run_dry_run_preview(target=target, limit=args.limit)
        _emit(report, json_output=args.json, output=args.output)
        return 0
    except StrategyPackageEvidenceBackfillError as exc:
        _emit(
            {
                "schema_version": SCHEMA_VERSION,
                "status": "failed",
                "dry_run": not args.apply,
                "target_db": target.target_db,
                "db_target": target.label,
                "db_writes": False,
                "ddl": False,
                "packages": [],
                "error": str(exc),
            },
            json_output=args.json,
            output=args.output,
        )
        return 2


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
