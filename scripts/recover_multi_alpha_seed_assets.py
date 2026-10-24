"""Original-seed asset audit and DEV-only recovery. No production write mode."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def dev_connection_factory(env: dict):
    import psycopg2

    expected = env.get("TDX_DB_DEV_NAME")
    if not expected or expected == env.get("TDX_DB_NAME"):
        raise ValueError("explicit, separate existing DEV database required")

    @contextmanager
    def connect(*, autocommit=False, manage_transaction=True):
        if autocommit or not manage_transaction:
            raise ValueError("DEV recovery requires a managed transaction")
        conn = psycopg2.connect(
            host=env["TDX_DB_DEV_HOST"],
            port=env["TDX_DB_DEV_PORT"],
            dbname=expected,
            user=env["TDX_DB_DEV_USER"],
            password=env["TDX_DB_DEV_PASSWORD"],
            connect_timeout=10,
        )
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT current_database()")
                if cur.fetchone()[0] != expected:
                    raise ValueError("DEV identity mismatch")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    return connect


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package-id", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--api-base", default="http://127.0.0.1:8001/api/v1")
    parser.add_argument("--apply-dev", action="store_true")
    args = parser.parse_args()
    from dotenv import dotenv_values, load_dotenv

    env = dict(dotenv_values(args.source_root / ".env"))
    load_dotenv(args.source_root / ".env", override=False)
    os.environ.setdefault("AISTOCK_PREDICTION_STORE_ROOT", str(args.source_root / "rdagent_assets/prediction_store"))
    from backend.services.strategy_package.frozen_runtime_self_check import FrozenRuntimeSelfCheckService
    from backend.services.strategy_package.models import StrategyPackageManifest
    from backend.services.strategy_package.multi_alpha_promotion import MultiAlphaPackagePromotionService
    from backend.services.strategy_package.package_asset_freeze import (
        PackageAssetFreezeService,
        StrategyPackageAssetSource,
    )
    from backend.services.strategy_package.package_asset_store import LocalPackageAssetStore
    from backend.services.strategy_package.repository import StrategyPackageRepository
    from backend.services.strategy_package.seed_asset_recovery import SeedAssetRecoveryService
    import requests

    source_root = Path(
        os.getenv("AISTOCK_PACKAGE_ASSET_STORE_ROOT") or args.source_root / "rdagent_assets/package_assets"
    ).resolve()
    output = args.output.resolve()
    if output == source_root or source_root in output.parents or output in source_root.parents:
        parser.error("output must be separate from the production asset store")
    output.mkdir(parents=True, exist_ok=True)

    def read_parent():
        response = requests.get(f"{args.api_base.rstrip('/')}/strategy-packages/{args.package_id}", timeout=30)
        response.raise_for_status()
        return StrategyPackageManifest.model_validate(response.json()["package"]["manifest"])

    parent = read_parent()
    (output / "parent-manifest.json").write_text(parent.model_dump_json(indent=2), encoding="utf-8")
    source_store = LocalPackageAssetStore(root=source_root)
    stage = LocalPackageAssetStore(root=output / "staging-cas")
    source = StrategyPackageAssetSource()
    freezer = PackageAssetFreezeService(asset_store=stage, source=source)
    checker = FrozenRuntimeSelfCheckService(asset_store=stage, cache_root=output / "runtime-cache")
    promotion = MultiAlphaPackagePromotionService(asset_freezer=freezer, frozen_runtime_self_check=checker)
    service = SeedAssetRecoveryService(
        promotion=promotion, source_store=source_store, staging_store=stage, self_check=checker
    )
    receipts = []

    def receipt(row):
        receipts.append(row)
        path = output / f"seed-{len(receipts):02d}.json"
        path.write_text(json.dumps(row, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(
            json.dumps(
                {
                    "seed_index": len(receipts),
                    "seed": row["seed_run_id"],
                    "status": row["status"],
                    "checks": {k: v["status"] for k, v in row["checks"].items()},
                },
                ensure_ascii=False,
            ),
            flush=True,
        )

    plan = service.audit(parent, version=args.version, on_receipt=receipt)
    payload = plan.to_payload()
    if args.apply_dev:
        factory = dev_connection_factory(env)
        repository = StrategyPackageRepository(conn_factory=factory)
        with factory() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT current_database(), count(*) FROM strategy_pkg.package GROUP BY 1")
                identity, before = cur.fetchone()
        try:
            record = service.persist(plan, repository=repository, current_parent=read_parent())
            payload["dev"] = {
                "database": identity,
                "status": "persisted_draft",
                "package_id": record.package_id,
                "manifest_sha256": record.manifest_sha256,
            }
        except Exception as exc:
            if plan.ready:
                raise
            payload["dev"] = {
                "database": identity,
                "status": "blocked_no_write",
                "reason": getattr(exc, "context", {}).get("reason_code", type(exc).__name__),
            }
        with factory() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM strategy_pkg.package")
                payload["dev"].update(package_count_before=before, package_count_after=cur.fetchone()[0])
    current = read_parent()
    payload["parent_unchanged"] = current.manifest_sha256 == parent.manifest_sha256
    if not payload["parent_unchanged"]:
        raise RuntimeError("parent identity changed during recovery; do not publish this plan")
    (output / "recovery-receipt.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
    )
    print(
        json.dumps({k: v for k, v in payload.items() if k not in ("manifest", "receipts")}, ensure_ascii=False),
        flush=True,
    )
    return 0 if plan.ready else 2


if __name__ == "__main__":
    raise SystemExit(main())
