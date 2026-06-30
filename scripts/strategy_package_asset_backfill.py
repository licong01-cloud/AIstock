"""Dry-run and gated apply helper for StrategyPackage runtime asset backfill.

Default mode is read-only. Apply mode performs production DML only when both an
operator flag and an environment confirmation token are present.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import psycopg2

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.strategy_package.package_asset_backfill import PackageAssetBackfillService  # noqa: E402
from backend.services.strategy_package.package_asset_freeze import (  # noqa: E402
    PackageAssetFreezeService,
    StrategyPackageAssetSource,
)
from backend.services.strategy_package.package_asset_store import (  # noqa: E402
    PACKAGE_ASSET_URI_SCHEME,
    LocalPackageAssetStore,
    PackageAssetBlob,
    PackageAssetStore,
)
from backend.services.strategy_package.repository import StrategyPackageRepository  # noqa: E402
from backend.services.trading_core.errors import (  # noqa: E402
    InvalidStateTransitionError,
    PackageAssetInvalidError,
    StrategyPackageValidationError,
)

APPLY_CONFIRM_ENV = "STRATEGY_PACKAGE_ASSET_BACKFILL_APPLY"
APPLY_CONFIRM_VALUE = "I_UNDERSTAND_PRODUCTION_DML"
DEFAULT_OPERATOR = "strategy_package_asset_backfill"
TARGET_PROD = "prod"
TARGET_DEV = "dev"


class DryRunPackageAssetStore(PackageAssetStore):
    """Read existing blobs but do not persist new blobs during dry-run."""

    def __init__(self, delegate: PackageAssetStore | None = None) -> None:
        self.delegate = delegate or LocalPackageAssetStore()
        self._blobs: dict[str, bytes] = {}

    def put(self, data: bytes, *, kind: str, sha256: str | None = None) -> PackageAssetBlob:
        payload = bytes(data)
        digest = hashlib.sha256(payload).hexdigest()
        expected = str(sha256 or "").strip().lower()
        if expected and expected != digest:
            raise PackageAssetInvalidError(
                "strategy package asset sha256 mismatch",
                context={
                    "reason_code": "strategy_package_asset_sha_mismatch",
                    "asset_kind": kind,
                    "expected_sha256": expected,
                    "actual_sha256": digest,
                    "dry_run": True,
                },
            )
        uri = f"{PACKAGE_ASSET_URI_SCHEME}://blobs/{digest}"
        self._blobs[uri] = payload
        return PackageAssetBlob(kind=str(kind), uri=uri, sha256=digest, size_bytes=len(payload))

    def get(self, uri: str) -> bytes:
        if uri in self._blobs:
            return self._blobs[uri]
        base_uri = str(uri).split("?", 1)[0]
        if base_uri in self._blobs:
            return self._blobs[base_uri]
        return self.delegate.get(uri)

    def exists(self, uri: str) -> bool:
        if uri in self._blobs:
            return True
        base_uri = str(uri).split("?", 1)[0]
        if base_uri in self._blobs:
            return True
        return self.delegate.exists(uri)


class AssetBackfillScriptError(RuntimeError):
    """Raised when the backfill script cannot safely continue."""


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
            raise AssetBackfillScriptError(f"missing dev database environment keys: {missing}")
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
            raise AssetBackfillScriptError(
                "refusing dev target because it does not look like a local scratch/dev DB: "
                f"host={cfg['host']} dbname={cfg['dbname']}"
            )
        return cfg

    required = ["TDX_DB_HOST", "TDX_DB_PORT", "TDX_DB_NAME", "TDX_DB_USER", "TDX_DB_PASSWORD"]
    missing = [key for key in required if not os.environ.get(key)]
    if missing:
        raise AssetBackfillScriptError(f"missing database environment keys: {missing}")
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


def _service_from_env(
    *,
    env_file: Path | None,
    target_db: str,
    readonly: bool,
) -> PackageAssetBackfillService:
    repo = _repo_from_env(env_file=env_file, target_db=target_db, readonly=readonly)

    def source_conn_factory() -> Iterator[Any]:
        return _env_conn_factory(env_file=env_file, target_db=target_db, readonly=True)

    store = LocalPackageAssetStore()
    asset_store: PackageAssetStore = DryRunPackageAssetStore(store) if readonly else store
    freezer = PackageAssetFreezeService(
        asset_store=asset_store,
        source=StrategyPackageAssetSource(conn_factory=source_conn_factory),
    )
    return PackageAssetBackfillService(repository=repo, asset_freezer=freezer)


def build_report(
    service: PackageAssetBackfillService,
    *,
    mode: str,
    limit: int,
    target: dict[str, Any] | None = None,
    package_ids: list[str] | None = None,
    package_id_prefix: str | None = None,
    operator: str,
) -> dict[str, Any]:
    plan = service.build_plan(limit=limit, package_ids=package_ids, package_id_prefix=package_id_prefix)
    if mode == "apply" and plan.to_report()["counts"].get("unrecoverable", 0):
        report = plan.to_report()
        report["mode"] = "apply_blocked"
        report["apply_blocked_reason"] = "unrecoverable_packages_present"
        report["target"] = target or {}
        report["operator"] = operator
        report["filter"] = {
            "package_ids": package_ids or [],
            "package_id_prefix": package_id_prefix,
        }
        return report
    result = service.apply_plan(plan, operator=operator) if mode == "apply" else plan
    report = result.to_report()
    report["mode"] = mode
    report["target"] = target or {}
    report["operator"] = operator
    report["filter"] = {
        "package_ids": package_ids or [],
        "package_id_prefix": package_id_prefix,
    }
    return report


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill StrategyPackage runtime assets into package-owned storage.")
    parser.add_argument("--env-file", type=Path, default=Path(os.environ.get("AISTOCK_ENV_FILE", ".env")))
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--target-db", choices=(TARGET_PROD, TARGET_DEV), default=TARGET_PROD)
    parser.add_argument("--package-id", action="append", default=[], help="specific package_id to scan; repeatable")
    parser.add_argument("--package-id-prefix", help="optional package_id prefix filter")
    parser.add_argument("--operator", default=DEFAULT_OPERATOR)
    parser.add_argument("--apply", action="store_true", help="perform DML; dry-run is the default")
    parser.add_argument(
        "--confirm-production-dml",
        action="store_true",
        help="required with --apply --target-db prod; environment token is also required",
    )
    parser.add_argument(
        "--confirm-scratch-dml",
        action="store_true",
        help="required with --apply --target-db dev",
    )
    return parser.parse_args()


def _validate_apply_gate(args: argparse.Namespace) -> None:
    if not args.apply:
        return
    if args.target_db == TARGET_PROD:
        if not args.confirm_production_dml:
            raise AssetBackfillScriptError("--apply on prod requires --confirm-production-dml")
        if os.environ.get(APPLY_CONFIRM_ENV) != APPLY_CONFIRM_VALUE:
            raise AssetBackfillScriptError(f"--apply on prod requires {APPLY_CONFIRM_ENV}={APPLY_CONFIRM_VALUE}")
    elif not args.confirm_scratch_dml:
        raise AssetBackfillScriptError("--apply --target-db dev requires --confirm-scratch-dml")


def main() -> int:
    args = _parse_args()
    if args.limit <= 0:
        raise AssetBackfillScriptError("--limit must be positive")
    _load_env_file(args.env_file)
    _validate_apply_gate(args)
    target = _target_metadata(_db_config(target_db=args.target_db), target_db=args.target_db)
    service = _service_from_env(env_file=args.env_file, target_db=args.target_db, readonly=not args.apply)
    mode = "apply" if args.apply else "dry_run"
    report = build_report(
        service,
        mode=mode,
        limit=args.limit,
        target=target,
        package_ids=args.package_id,
        package_id_prefix=args.package_id_prefix,
        operator=args.operator,
    )
    text = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text + "\n", encoding="utf-8")
    print(text)
    if report["counts"].get("unrecoverable", 0):
        return 2
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (AssetBackfillScriptError, InvalidStateTransitionError, StrategyPackageValidationError) as exc:
        print(json.dumps({"ok": False, "error": str(exc), "context": getattr(exc, "context", {})}, ensure_ascii=False))
        raise SystemExit(1)
