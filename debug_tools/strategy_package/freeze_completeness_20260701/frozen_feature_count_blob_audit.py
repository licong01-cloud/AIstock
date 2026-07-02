from __future__ import annotations

import argparse
import io
import json
import pickle
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
    parser = argparse.ArgumentParser(description="Read-only quick MODEL_WEIGHT blob feature-count audit.")
    parser.add_argument("--repo-root", type=Path, default=_default_repo_root())
    parser.add_argument("--env-file", type=Path, default=None)
    parser.add_argument("--package-id", action="append", default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--json-output", type=Path, default=None)
    return parser.parse_args()


def _fetch_packages(selected: list[str] | None, limit: int | None) -> list[tuple[str, str, int, str | None]]:
    from backend.db.pg_pool import get_conn

    with get_conn(autocommit=False, manage_transaction=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION READ ONLY")
            params: tuple[Any, ...]
            if selected:
                sql = """
                    SELECT p.package_id,
                           p.package_status,
                           jsonb_array_length(COALESCE(p.manifest_json->'factor_set','[]'::jsonb)) AS dynamic_count,
                           a.asset_ref
                    FROM strategy_pkg.package p
                    LEFT JOIN LATERAL (
                        SELECT asset_ref
                        FROM strategy_pkg.package_asset
                        WHERE package_id = p.package_id AND lower(asset_type) = 'model_weight'
                        ORDER BY created_at DESC NULLS LAST
                        LIMIT 1
                    ) a ON TRUE
                    WHERE p.package_id = ANY(%s)
                    ORDER BY p.package_id
                """
                params = (selected,)
            else:
                sql = """
                    SELECT p.package_id,
                           p.package_status,
                           jsonb_array_length(COALESCE(p.manifest_json->'factor_set','[]'::jsonb)) AS dynamic_count,
                           a.asset_ref
                    FROM strategy_pkg.package p
                    LEFT JOIN LATERAL (
                        SELECT asset_ref
                        FROM strategy_pkg.package_asset
                        WHERE package_id = p.package_id AND lower(asset_type) = 'model_weight'
                        ORDER BY created_at DESC NULLS LAST
                        LIMIT 1
                    ) a ON TRUE
                    WHERE p.package_status NOT IN ('RETIRED')
                    ORDER BY p.package_status, p.package_id
                """
                params = ()
                if limit:
                    sql += " LIMIT %s"
                    params = (limit,)
            cur.execute(sql, params)
            return [(str(pkg), str(status), int(dynamic_count or 0), asset_ref) for pkg, status, dynamic_count, asset_ref in cur.fetchall()]


def _model_feature_count(blob: bytes) -> int | None:
    obj = pickle.load(io.BytesIO(blob))
    inner = getattr(obj, "model", obj)
    for attr in ("n_features_", "n_features_in_"):
        value = getattr(inner, attr, None)
        if value:
            return int(value)
    booster = getattr(inner, "booster_", None) or getattr(inner, "_Booster", None)
    if booster is not None and hasattr(booster, "num_feature"):
        return int(booster.num_feature())
    state_dict = getattr(inner, "state_dict", None)
    if callable(state_dict):
        tensors = state_dict()
        for tensor in tensors.values():
            shape = getattr(tensor, "shape", None)
            if shape is not None and len(shape) == 2:
                return int(shape[1])
    return None


def main() -> int:
    args = _parse_args()
    _bootstrap_repo(args.repo_root, args.env_file)

    from backend.services.strategy_package.package_asset_store import LocalPackageAssetStore

    store = LocalPackageAssetStore()
    rows = []
    counts = {"self_contained": 0, "feature_count_mismatch": 0, "unknown": 0, "error": 0}
    print(f"{'package':40} {'status':18} {'dyn':>4} {'model_exp':>9} verdict")
    for package_id, status, dynamic_count, asset_ref in _fetch_packages(args.package_id, args.limit):
        row: dict[str, Any] = {
            "package_id": package_id,
            "status": status,
            "dynamic_count": dynamic_count,
            "asset_ref": asset_ref,
        }
        if not asset_ref:
            row.update({"verdict": "error", "error": "missing model_weight asset_ref"})
            counts["error"] += 1
            print(f"{package_id:40} {status:18} {dynamic_count:>4} {'NO_BLOB':>9} missing model_weight")
            rows.append(row)
            continue
        try:
            expected = _model_feature_count(store.get(asset_ref))
            row["model_expected_features"] = expected
            if expected is None:
                row["verdict"] = "unknown"
                counts["unknown"] += 1
            elif expected == dynamic_count:
                row["verdict"] = "self_contained"
                counts["self_contained"] += 1
            else:
                row["verdict"] = "feature_count_mismatch"
                row["missing_features"] = int(expected) - int(dynamic_count)
                counts["feature_count_mismatch"] += 1
            print(f"{package_id:40} {status:18} {dynamic_count:>4} {str(expected):>9} {row['verdict']}")
        except Exception as exc:
            row.update({"verdict": "error", "error": f"{type(exc).__name__}: {exc}"})
            counts["error"] += 1
            print(f"{package_id:40} {status:18} {dynamic_count:>4} {'ERR':>9} {type(exc).__name__}: {exc}")
        rows.append(row)

    payload = {"counts": counts, "total": len(rows), "rows": rows}
    _write_json(args.json_output, payload)
    print(f"SUMMARY: {json.dumps({'total': len(rows), **counts}, ensure_ascii=False)}")
    return 2 if counts["error"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
