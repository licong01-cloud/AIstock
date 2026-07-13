from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any


def _default_repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _load_env_file(env_file: Path) -> None:
    if not env_file.exists():
        raise FileNotFoundError(f"env file not found: {env_file}")
    for raw_line in env_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _force_pg_readonly() -> None:
    existing = os.environ.get("PGOPTIONS", "").strip()
    readonly = "-c default_transaction_read_only=on"
    if readonly not in existing:
        os.environ["PGOPTIONS"] = f"{existing} {readonly}".strip()


def _bootstrap_repo(repo_root: Path, env_file: Path | None) -> None:
    repo_root = repo_root.resolve()
    os.chdir(repo_root)
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    _load_env_file(env_file or repo_root / ".env")
    _force_pg_readonly()


def _write_json(path: Path | None, payload: dict[str, Any]) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read-only StrategyPackage frozen runtime feature-count audit."
    )
    parser.add_argument("--repo-root", type=Path, default=_default_repo_root())
    parser.add_argument("--env-file", type=Path, default=None)
    parser.add_argument("--package-id", action="append", default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--json-output", type=Path, default=None)
    parser.add_argument(
        "--fail-on-broken",
        action="store_true",
        help="Return non-zero when audited packages include feature-count mismatches.",
    )
    return parser.parse_args()


def _fetch_package_ids(selected: list[str] | None, limit: int | None) -> list[tuple[str, str]]:
    from backend.db.pg_pool import get_conn

    with get_conn(autocommit=False, manage_transaction=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION READ ONLY")
            if selected:
                cur.execute(
                    """
                    SELECT package_id, package_status
                    FROM strategy_pkg.package
                    WHERE package_id = ANY(%s)
                    ORDER BY package_id
                    """,
                    (selected,),
                )
            else:
                sql = """
                    SELECT package_id, package_status
                    FROM strategy_pkg.package
                    WHERE package_status NOT IN ('RETIRED')
                    ORDER BY package_status, package_id
                """
                params: tuple[Any, ...] = ()
                if limit:
                    sql += " LIMIT %s"
                    params = (limit,)
                cur.execute(sql, params)
            return [(str(pkg), str(status)) for pkg, status in cur.fetchall()]


def main() -> int:
    args = _parse_args()
    _bootstrap_repo(args.repo_root, args.env_file)

    from backend.inference_engine import load_model_from_pkl
    from backend.services.strategy_package.live_inference import QEExperimentRuntimeAssetResolver
    from backend.services.strategy_package.repository import StrategyPackageRepository

    repo = StrategyPackageRepository()
    resolver = QEExperimentRuntimeAssetResolver()
    packages = _fetch_package_ids(args.package_id, args.limit)
    rows: list[dict[str, Any]] = []
    counts = {"self_contained": 0, "broken_alpha158": 0, "error": 0}

    print(f"{'package':40} {'status':18} {'factor_order':>12} {'model_exp':>9} verdict")
    for package_id, status in packages:
        row: dict[str, Any] = {"package_id": package_id, "status": status}
        try:
            record = repo.get(package_id)
            manifest = record.current_manifest()
            source = resolver.load_source_for_strategy_package(
                source_type=record.source_type,
                source_id="qe_DELETED_SOURCE_READONLY_AUDIT",
                loop_id="loop_DELETED_READONLY_AUDIT",
                run_id=None,
                manifest=manifest,
                package_id=package_id,
            )
            prepared = resolver.prepare_workspace(
                package_id=package_id,
                manifest_sha256=manifest.manifest_sha256,
                source=source,
                runtime_config=None,
                path_converter=None,
            )
            factor_count = len(prepared.factor_order)
            _, _, _, expected = load_model_from_pkl(str(prepared.model_params_path))
            row.update(
                {
                    "model_params_origin": source.model_params_origin,
                    "factor_order_count": factor_count,
                    "model_expected_features": expected,
                }
            )
            if expected == factor_count:
                row["verdict"] = "self_contained"
                counts["self_contained"] += 1
            else:
                row["verdict"] = "feature_count_mismatch"
                row["missing_features"] = int(expected) - int(factor_count)
                counts["broken_alpha158"] += 1
            print(
                f"{package_id:40} {status:18} {factor_count:12} {str(expected):>9} "
                f"{row['verdict']}"
            )
        except Exception as exc:
            row.update({"verdict": "error", "error": f"{type(exc).__name__}: {exc}"})
            counts["error"] += 1
            print(f"{package_id:40} {status:18} {'?':>12} {'?':>9} ERROR {type(exc).__name__}: {exc}")
        rows.append(row)

    payload = {"counts": counts, "total": len(rows), "rows": rows}
    _write_json(args.json_output, payload)
    print(f"SUMMARY: {json.dumps({'total': len(rows), **counts}, ensure_ascii=False)}")
    if counts["error"]:
        return 2
    if args.fail_on_broken and counts["broken_alpha158"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
