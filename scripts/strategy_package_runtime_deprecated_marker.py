"""Dry-run and gated apply helper for legacy StrategyPackage deprecation markers.

Default mode is read-only. Apply mode inserts append-only package_status_event
rows only when both an operator flag and an environment confirmation token are
present. It does not modify package_status, manifest_json, manifest_sha256, or
package_asset rows.
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

APPLY_CONFIRM_ENV = "STRATEGY_PACKAGE_DEPRECATED_MARKER_APPLY"
APPLY_CONFIRM_VALUE = "I_UNDERSTAND_PRODUCTION_DML"
BATCH_ID = "strategy_package_freeze_completeness_20260701"
DEFAULT_OPERATOR = "strategy_package_runtime_deprecated_marker"
TARGET_PROD = "prod"
TARGET_DEV = "dev"
DEPRECATION_REASON = "strategy_package_runtime_deprecated"
RETRACT_REASON = "strategy_package_runtime_deprecation_retracted"
GOOD_SELF_CONTAINED_PACKAGE_IDS = {
    "pkg_5a5ccb56ea5c4e3daaf6d836c8edfc27",
    "pkg_b668f8a633c44b72a5d557a2cb8970e3",
}
RETIRED_PACKAGE_IDS = {
    "pkg_b4ce634c24bd470fac2c7b581a4e106f",
    "pkg_95523262439644e49ae52f9b5087165d",
}
TARGET_PACKAGE_IDS = [
    "pkg_006a42323f7c4e81a468fdaad2cb16a3",
    "pkg_09750b4944ca434db03efd399ccf2144",
    "pkg_1de32357724a4c5b874f2abd90f22da5",
    "pkg_2563063e544f4d1fa601e740d019f8c7",
    "pkg_2a9fccb83da840c9a27a2d7a4118af9a",
    "pkg_378eb9c91e104c64935404e257e932ee",
    "pkg_99142cb1440c40a7824e83902f4e7da9",
    "pkg_a2f53f3f2f3e4095a910b939464c35e6",
    "pkg_b2faccade8d549af9621c51d285bdc06",
    "pkg_c4703dfc2fdf4e548cf8dd3027ef228b",
    "pkg_cfa3c5b4068d4db1ad06db352bfece93",
]


class DeprecatedMarkerScriptError(RuntimeError):
    """Raised when the deprecation marker script cannot safely continue."""


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
            raise DeprecatedMarkerScriptError(f"missing dev database environment keys: {missing}")
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
            raise DeprecatedMarkerScriptError(
                "refusing dev target because it does not look like a local scratch/dev DB: "
                f"host={cfg['host']} dbname={cfg['dbname']}"
            )
        return cfg

    required = ["TDX_DB_HOST", "TDX_DB_PORT", "TDX_DB_NAME", "TDX_DB_USER", "TDX_DB_PASSWORD"]
    missing = [key for key in required if not os.environ.get(key)]
    if missing:
        raise DeprecatedMarkerScriptError(f"missing database environment keys: {missing}")
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
def _connect(*, env_file: Path | None, target_db: str, readonly: bool) -> Iterator[Any]:
    _load_env_file(env_file)
    cfg = _db_config(target_db=target_db)
    conn = psycopg2.connect(**cfg)
    if readonly:
        conn.set_session(readonly=True, autocommit=True)
    try:
        yield conn
    finally:
        conn.close()


def _fetch_rows(conn: Any) -> list[dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT p.package_id,
                   p.package_name,
                   p.package_status,
                   p.manifest_sha256,
                   COUNT(a.asset_id) FILTER (WHERE a.asset_type IN ('model_weight', 'factor_code', 'factor_schema', 'model_code')) AS runtime_asset_count,
                   EXISTS (
                       SELECT 1
                       FROM strategy_pkg.package_status_event e
                       WHERE e.package_id = p.package_id
                         AND e.reason = %s
                         AND e.context->>'batch_id' = %s
                   ) AS already_marked
            FROM strategy_pkg.package p
            LEFT JOIN strategy_pkg.package_asset a ON a.package_id = p.package_id
            WHERE p.package_id = ANY(%s)
            GROUP BY p.package_id, p.package_name, p.package_status, p.manifest_sha256
            ORDER BY p.package_id
            """,
            (DEPRECATION_REASON, BATCH_ID, TARGET_PACKAGE_IDS),
        )
        return [dict(row) for row in cur.fetchall()]


def build_plan(rows: list[dict[str, Any]], *, target: dict[str, Any] | None = None) -> dict[str, Any]:
    by_id = {str(row["package_id"]): row for row in rows}
    items: list[dict[str, Any]] = []
    for package_id in TARGET_PACKAGE_IDS:
        row = by_id.get(package_id)
        if row is None:
            items.append({"package_id": package_id, "action": "blocked", "reason_code": "strategy_package_marker_target_missing"})
            continue
        status = str(row["package_status"])
        runtime_asset_count = int(row.get("runtime_asset_count") or 0)
        if package_id in GOOD_SELF_CONTAINED_PACKAGE_IDS:
            action = "blocked"
            reason_code = "strategy_package_marker_target_is_self_contained"
        elif package_id in RETIRED_PACKAGE_IDS or status == "RETIRED":
            action = "blocked"
            reason_code = "strategy_package_marker_target_retired"
        elif bool(row.get("already_marked")):
            action = "skipped_already_marked"
            reason_code = None
        else:
            action = "insert_deprecation_event"
            reason_code = "strategy_package_legacy_freeze_incomplete_not_repaired"
        items.append(
            {
                "package_id": package_id,
                "package_name": row.get("package_name"),
                "package_status": status,
                "manifest_sha256": row.get("manifest_sha256"),
                "runtime_asset_count": runtime_asset_count,
                "action": action,
                "reason_code": reason_code,
            }
        )
    counts: dict[str, int] = {}
    for item in items:
        counts[item["action"]] = counts.get(item["action"], 0) + 1
    blocked = [item for item in items if item["action"] == "blocked"]
    return {
        "mode": "dry_run",
        "batch_id": BATCH_ID,
        "target": target or {},
        "target_package_count": len(TARGET_PACKAGE_IDS),
        "counts": counts,
        "blocked_count": len(blocked),
        "blocked": blocked,
        "items": items,
        "sql_effect": "append-only package_status_event rows; package_status, manifest_json, manifest_sha256, package_asset unchanged",
        "protected_exclusions": {
            "good_self_contained_package_ids": sorted(GOOD_SELF_CONTAINED_PACKAGE_IDS),
            "retired_package_ids": sorted(RETIRED_PACKAGE_IDS),
        },
    }


def apply_plan(conn: Any, plan: dict[str, Any], *, operator: str) -> dict[str, Any]:
    if plan.get("blocked_count"):
        raise DeprecatedMarkerScriptError("apply blocked because target validation has blocked rows")
    targets = [item for item in plan["items"] if item["action"] == "insert_deprecation_event"]
    inserted: list[dict[str, Any]] = []
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        for item in targets:
            context = {
                "batch_id": BATCH_ID,
                "reason_code": "strategy_package_legacy_freeze_incomplete_not_repaired",
                "deprecation_reason": "legacy frozen package lacks Alpha158 schema and/or MODEL_CODE; QE source retained as last-resort fallback",
                "manifest_sha256": item.get("manifest_sha256"),
                "runtime_asset_count": item.get("runtime_asset_count"),
                "status_preserved": True,
                "operator": operator,
            }
            cur.execute(
                """
                INSERT INTO strategy_pkg.package_status_event (
                    package_id, from_status, to_status, reason, context
                )
                SELECT p.package_id, p.package_status, p.package_status, %s, %s
                FROM strategy_pkg.package p
                WHERE p.package_id = %s
                  AND p.package_status <> 'RETIRED'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM strategy_pkg.package_status_event e
                      WHERE e.package_id = p.package_id
                        AND e.reason = %s
                        AND e.context->>'batch_id' = %s
                  )
                RETURNING event_id, package_id, to_status, created_at
                """,
                (
                    DEPRECATION_REASON,
                    psycopg2.extras.Json(context),
                    item["package_id"],
                    DEPRECATION_REASON,
                    BATCH_ID,
                ),
            )
            row = cur.fetchone()
            if row is not None:
                inserted.append(dict(row))
    conn.commit()
    return {
        "mode": "apply",
        "batch_id": BATCH_ID,
        "operator": operator,
        "inserted_count": len(inserted),
        "inserted": inserted,
        "skipped_already_marked_count": plan.get("counts", {}).get("skipped_already_marked", 0),
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Mark legacy incomplete frozen StrategyPackages as deprecated via audit events.")
    parser.add_argument("--env-file", type=Path, default=Path(os.environ.get("AISTOCK_ENV_FILE", ".env")))
    parser.add_argument("--output", type=Path)
    parser.add_argument("--target-db", choices=(TARGET_PROD, TARGET_DEV), default=TARGET_PROD)
    parser.add_argument("--operator", default=DEFAULT_OPERATOR)
    parser.add_argument("--apply", action="store_true", help="perform production DML; dry-run is the default")
    parser.add_argument("--confirm-production-dml", action="store_true", help="required with --apply --target-db prod")
    parser.add_argument("--confirm-scratch-dml", action="store_true", help="required with --apply --target-db dev")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    _load_env_file(args.env_file)
    target = _target_metadata(_db_config(target_db=args.target_db), target_db=args.target_db)
    if args.apply:
        if args.target_db == TARGET_PROD:
            if not args.confirm_production_dml:
                raise DeprecatedMarkerScriptError("--apply on prod requires --confirm-production-dml")
            if os.environ.get(APPLY_CONFIRM_ENV) != APPLY_CONFIRM_VALUE:
                raise DeprecatedMarkerScriptError(f"--apply on prod requires {APPLY_CONFIRM_ENV}={APPLY_CONFIRM_VALUE}")
        elif not args.confirm_scratch_dml:
            raise DeprecatedMarkerScriptError("--apply --target-db dev requires --confirm-scratch-dml")
    with _connect(env_file=args.env_file, target_db=args.target_db, readonly=not args.apply) as conn:
        plan = build_plan(_fetch_rows(conn), target=target)
        result = apply_plan(conn, plan, operator=args.operator) if args.apply else plan
    text = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True, default=str)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 2 if result.get("blocked_count", 0) else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (DeprecatedMarkerScriptError, psycopg2.Error) as exc:
        print(json.dumps({"ok": False, "error": str(exc), "context": getattr(exc, "context", {})}, ensure_ascii=False))
        raise SystemExit(1)
