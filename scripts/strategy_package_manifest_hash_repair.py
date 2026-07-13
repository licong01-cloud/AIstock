"""Dry-run and apply helper for StrategyPackage manifest_sha256 repairs.

Default mode is read-only. Production apply is intentionally gated by both an
operator flag and an environment confirmation token; scratch/dev apply is
restricted to explicit dev database credentials. No manifest_json payload is
mutated.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import psycopg2
import psycopg2.extras

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.strategy_package.repository import StrategyPackageRepository  # noqa: E402
from backend.services.trading_core.errors import InvalidStateTransitionError, StrategyPackageValidationError  # noqa: E402

APPLY_CONFIRM_ENV = "STRATEGY_PACKAGE_MANIFEST_HASH_REPAIR_APPLY"
APPLY_CONFIRM_VALUE = "I_UNDERSTAND_PRODUCTION_DML"
DEFAULT_OPERATOR = "strategy_package_manifest_hash_repair"
TARGET_PROD = "prod"
TARGET_DEV = "dev"


class ManifestHashRepairScriptError(RuntimeError):
    """Raised when the repair script cannot safely continue."""


def _load_env_file(path: Path | None) -> None:
    if path is None or not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text or text.startswith("#") or "=" not in text:
            continue
        key, value = text.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _db_config(*, target_db: str) -> dict[str, Any]:
    if target_db == TARGET_DEV:
        required = [
            "TDX_DB_DEV_HOST",
            "TDX_DB_DEV_PORT",
            "TDX_DB_DEV_NAME",
            "TDX_DB_DEV_USER",
            "TDX_DB_DEV_PASSWORD",
        ]
        missing = [key for key in required if not os.environ.get(key)]
        if missing:
            raise ManifestHashRepairScriptError(f"missing dev database environment keys: {missing}")
        cfg = {
            "host": os.environ["TDX_DB_DEV_HOST"],
            "port": int(os.environ["TDX_DB_DEV_PORT"]),
            "dbname": os.environ["TDX_DB_DEV_NAME"],
            "user": os.environ["TDX_DB_DEV_USER"],
            "password": os.environ["TDX_DB_DEV_PASSWORD"],
        }
        host = str(cfg["host"]).lower()
        dbname = str(cfg["dbname"]).lower()
        if host not in {"127.0.0.1", "localhost"} or not any(
            marker in dbname for marker in ("dev", "scratch", "test")
        ):
            raise ManifestHashRepairScriptError(
                f"refusing dev target because it does not look like a local scratch/dev DB: "
                f"host={cfg['host']} dbname={cfg['dbname']}"
            )
        return cfg

    required = ["TDX_DB_HOST", "TDX_DB_PORT", "TDX_DB_NAME", "TDX_DB_USER", "TDX_DB_PASSWORD"]
    missing = [key for key in required if not os.environ.get(key)]
    if missing:
        raise ManifestHashRepairScriptError(f"missing database environment keys: {missing}")
    return {
        "host": os.environ["TDX_DB_HOST"],
        "port": int(os.environ["TDX_DB_PORT"]),
        "dbname": os.environ["TDX_DB_NAME"],
        "user": os.environ["TDX_DB_USER"],
        "password": os.environ["TDX_DB_PASSWORD"],
    }


def _target_metadata(cfg: dict[str, Any], *, target_db: str) -> dict[str, Any]:
    return {
        "target_db": target_db,
        "host": cfg["host"],
        "port": cfg["port"],
        "dbname": cfg["dbname"],
        "user": cfg["user"],
        "password_configured": bool(cfg.get("password")),
    }


@contextmanager
def _env_conn_factory(*, env_file: Path | None, target_db: str, readonly: bool) -> Iterator[Any]:
    _load_env_file(env_file)
    cfg = _db_config(target_db=target_db)
    conn = psycopg2.connect(**cfg)
    if readonly:
        conn.set_session(readonly=True, autocommit=True)
    try:
        yield conn
    finally:
        conn.close()


def _repo_from_env(*, env_file: Path | None, target_db: str, readonly: bool) -> StrategyPackageRepository:
    def factory() -> Iterator[Any]:
        return _env_conn_factory(env_file=env_file, target_db=target_db, readonly=readonly)

    return StrategyPackageRepository(conn_factory=factory)


def _repair_plan(drift: dict[str, Any]) -> dict[str, Any]:
    plan = drift.get("repair_plan")
    if not isinstance(plan, dict):
        return {}
    return plan


def _filter_report(report: dict[str, Any], *, package_id_prefix: str | None) -> dict[str, Any]:
    if not package_id_prefix:
        return dict(report)
    filtered = dict(report)
    filtered["drifted"] = [
        drift
        for drift in report.get("drifted") or []
        if str(drift.get("package_id") or "").startswith(package_id_prefix)
    ]
    filtered["drifted_count"] = len(filtered["drifted"])
    filtered["filter"] = {"package_id_prefix": package_id_prefix}
    return filtered


def classify_drift_actions(report: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    repairable: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    for drift in report.get("drifted") or []:
        plan = _repair_plan(drift)
        classification = plan.get("classification") if isinstance(plan.get("classification"), dict) else {}
        item = {
            "package_id": drift.get("package_id"),
            "package_status": drift.get("package_status"),
            "stored_sha256": drift.get("stored_sha256"),
            "computed_sha256": drift.get("computed_sha256"),
            "recommended_action": plan.get("recommended_action"),
            "classification": classification.get("classification"),
            "repair_allowed": classification.get("repair_allowed"),
            "reason": classification.get("reason"),
            "missing_current_model_default_keys": classification.get("missing_current_model_default_keys") or [],
        }
        if plan.get("recommended_action") == "repair_manifest_hash" and classification.get("repair_allowed") is True:
            repairable.append(item)
        else:
            blocked.append(item)
    return {"repairable": repairable, "blocked": blocked}


def build_dry_run_report(
    report: dict[str, Any],
    *,
    target: dict[str, Any] | None = None,
    package_id_prefix: str | None = None,
) -> dict[str, Any]:
    filtered = _filter_report(report, package_id_prefix=package_id_prefix)
    actions = classify_drift_actions(filtered)
    return {
        "mode": "dry_run",
        "target": target or {},
        "filter": filtered.get("filter"),
        "total_scanned": report.get("total_scanned"),
        "clean_count": report.get("clean_count"),
        "drifted_count": report.get("drifted_count"),
        "filtered_drifted_count": filtered.get("drifted_count"),
        "repairable_count": len(actions["repairable"]),
        "blocked_count": len(actions["blocked"]),
        "repairable": actions["repairable"],
        "blocked": actions["blocked"],
    }


def apply_repairs(
    repo: StrategyPackageRepository,
    report: dict[str, Any],
    *,
    operator: str,
    target: dict[str, Any] | None = None,
    package_id_prefix: str | None = None,
) -> dict[str, Any]:
    filtered = _filter_report(report, package_id_prefix=package_id_prefix)
    actions = classify_drift_actions(filtered)
    if actions["blocked"]:
        raise ManifestHashRepairScriptError(
            "manifest hash repair apply blocked because non-repairable drift exists: "
            + json.dumps(actions["blocked"], ensure_ascii=False, sort_keys=True)
        )
    repaired: list[dict[str, Any]] = []
    for item in actions["repairable"]:
        package_id = str(item["package_id"])
        record = repo.repair_manifest_hash(
            package_id,
            operator=operator,
            confirm_stored_sha256=str(item["stored_sha256"]),
            confirm_computed_sha256=str(item["computed_sha256"]),
        )
        repaired.append({"package_id": package_id, "manifest_sha256": record.manifest_sha256})
    after = repo.validate_manifest_integrity(limit=int(report.get("total_scanned") or 500))
    after_filtered = _filter_report(after, package_id_prefix=package_id_prefix)
    after_actions = classify_drift_actions(after_filtered)
    return {
        "mode": "apply",
        "target": target or {},
        "filter": after_filtered.get("filter"),
        "operator": operator,
        "repaired_count": len(repaired),
        "repaired": repaired,
        "after_clean_count": after.get("clean_count"),
        "after_drifted_count": after.get("drifted_count"),
        "after_filtered_drifted_count": after_filtered.get("drifted_count"),
        "after_repairable_count": len(after_actions["repairable"]),
        "after_blocked_count": len(after_actions["blocked"]),
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Repair StrategyPackage manifest_sha256 drift safely.")
    parser.add_argument("--env-file", type=Path, default=Path(os.environ.get("AISTOCK_ENV_FILE", ".env")))
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--target-db", choices=(TARGET_PROD, TARGET_DEV), default=TARGET_PROD)
    parser.add_argument("--package-id-prefix", help="optional scratch/dev filter for repair actions")
    parser.add_argument("--operator", default=DEFAULT_OPERATOR)
    parser.add_argument("--apply", action="store_true", help="perform production DML; dry-run is the default")
    parser.add_argument(
        "--confirm-production-dml",
        action="store_true",
        help="required with --apply; environment token is also required",
    )
    parser.add_argument(
        "--confirm-scratch-dml",
        action="store_true",
        help="required with --apply --target-db dev",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    if args.limit <= 0:
        raise ManifestHashRepairScriptError("--limit must be positive")
    _load_env_file(args.env_file)
    target = _target_metadata(_db_config(target_db=args.target_db), target_db=args.target_db)
    if args.apply:
        if args.target_db == TARGET_PROD:
            if not args.confirm_production_dml:
                raise ManifestHashRepairScriptError("--apply on prod requires --confirm-production-dml")
            if os.environ.get(APPLY_CONFIRM_ENV) != APPLY_CONFIRM_VALUE:
                raise ManifestHashRepairScriptError(
                    f"--apply on prod requires {APPLY_CONFIRM_ENV}={APPLY_CONFIRM_VALUE}"
                )
        elif not args.confirm_scratch_dml:
            raise ManifestHashRepairScriptError("--apply --target-db dev requires --confirm-scratch-dml")
    repo = _repo_from_env(env_file=args.env_file, target_db=args.target_db, readonly=not args.apply)
    report = repo.validate_manifest_integrity(limit=args.limit)
    if args.apply:
        result = apply_repairs(
            repo,
            report,
            operator=args.operator,
            target=target,
            package_id_prefix=args.package_id_prefix,
        )
    else:
        result = build_dry_run_report(report, target=target, package_id_prefix=args.package_id_prefix)
    text = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text + "\n", encoding="utf-8")
    print(text)
    if result.get("blocked_count", 0):
        return 2
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ManifestHashRepairScriptError, InvalidStateTransitionError, StrategyPackageValidationError) as exc:
        print(json.dumps({"ok": False, "error": str(exc), "context": getattr(exc, "context", {})}, ensure_ascii=False))
        raise SystemExit(1)
