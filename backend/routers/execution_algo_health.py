"""Read-only execution algorithm model-cache health endpoint."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fastapi import APIRouter

from backend.db.pg_pool import get_conn
from backend.services.strategy_package.model_asset_resolver import ModelAssetResolver
from backend.services.trading_core.errors import DataUnavailableError
from backend.services.trading_core.execution_algo_capabilities import required_runtime_asset_keys


router = APIRouter(prefix="/execution-algos", tags=["execution-algos"])


def _coerce_config(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _config_with_row_namespace(default_config: dict[str, Any], row_namespace: Any) -> dict[str, Any]:
    merged = dict(default_config)
    namespace = str(row_namespace or "").strip()
    if namespace:
        merged["asset_namespace"] = namespace
    return merged


def _positive_file(path: Path) -> bool:
    try:
        return path.exists() and path.is_file() and path.stat().st_size > 0
    except OSError:
        return False


def _empty_or_invalid_file(path: Path) -> bool:
    try:
        return path.exists() and (not path.is_file() or path.stat().st_size <= 0)
    except OSError:
        return False


def _asset_health(
    *,
    resolver: ModelAssetResolver,
    algo_code: str,
    asset_namespace: str,
    config_key: str,
    raw_path: Any,
) -> dict[str, Any]:
    original_path = str(raw_path or "").strip()
    base = {
        "config_key": config_key,
        "configured_path": original_path,
        "status": "missing",
        "resolved_path": None,
        "source": None,
        "reason": None,
        "asset_namespace": asset_namespace,
    }
    if not original_path:
        return {**base, "reason": "config key is empty"}

    direct_path = Path(original_path)
    if _positive_file(direct_path):
        return {
            **base,
            "status": "ok",
            "resolved_path": str(direct_path),
            "source": "configured_path",
        }
    if _empty_or_invalid_file(direct_path):
        return {**base, "reason": "configured path is empty or not a file"}

    cache_destination = resolver._cache_destination(asset_namespace, original_path)
    if _positive_file(cache_destination):
        try:
            resolver._validate_cached_asset(
                cache_destination,
                original_path,
                f"execution_algorithm_catalog:{algo_code}",
                algo_code=algo_code,
                asset_namespace=asset_namespace,
                config_key=config_key,
            )
        except DataUnavailableError as exc:
            return {
                **base,
                "resolved_path": str(cache_destination),
                "source": "hashed_cache",
                "reason": f"hashed cache metadata invalid: {exc.message}",
            }
        return {
            **base,
            "status": "cached",
            "resolved_path": str(cache_destination),
            "source": "hashed_cache",
        }
    if _empty_or_invalid_file(cache_destination):
        return {**base, "reason": "hashed cache path is empty or not a file"}

    for candidate in resolver._candidate_paths(original_path, algo_code=asset_namespace):
        if candidate == direct_path:
            continue
        if _positive_file(candidate):
            return {
                **base,
                "status": "cached",
                "resolved_path": str(candidate),
                "source": "legacy_cache",
            }
        if _empty_or_invalid_file(candidate):
            return {**base, "reason": "legacy cache path is empty or not a file"}

    return {
        **base,
        "reason": "no readable model file found at configured path or read-only cache candidates",
    }


def _algo_status(asset_statuses: list[str]) -> str:
    if not asset_statuses:
        return "ok"
    if all(status == "ok" for status in asset_statuses):
        return "ok"
    if all(status in {"ok", "cached"} for status in asset_statuses):
        return "cached"
    return "missing"


def _fetch_enabled_algos() -> list[dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'execution_algorithm_catalog'
                      AND column_name = 'asset_namespace'
                )
                """
            )
            row = cur.fetchone()
            has_asset_namespace = bool(row and (row[0] if not hasattr(row, "get") else row.get("exists")))
            asset_namespace_select = "asset_namespace" if has_asset_namespace else "NULL::text AS asset_namespace"
            cur.execute(
                f"""
                SELECT algo_code, algo_name, default_config, is_enabled, supported_freqs, min_bars, {asset_namespace_select}
                FROM execution_algorithm_catalog
                WHERE is_enabled = TRUE
                ORDER BY sort_order, id
                """
            )
            cols = [desc[0] for desc in cur.description]
            rows: list[dict[str, Any]] = []
            for row in cur.fetchall():
                rows.append(dict(row) if isinstance(row, dict) else dict(zip(cols, row)))
            return rows


@router.get("/health")
def get_execution_algo_health() -> dict[str, Any]:
    """List enabled execution algos and read-only runtime model-cache health."""

    resolver = ModelAssetResolver()
    algos: list[dict[str, Any]] = []
    for row in _fetch_enabled_algos():
        algo_code = str(row.get("algo_code") or "").strip().upper()
        default_config = _coerce_config(row.get("default_config"))
        asset_keys = required_runtime_asset_keys(algo_code)
        try:
            asset_namespace = resolver.asset_namespace_for(
                algo_code,
                _config_with_row_namespace(default_config, row.get("asset_namespace")),
            )
        except DataUnavailableError as exc:
            asset_namespace = str(row.get("asset_namespace") or default_config.get("asset_namespace") or algo_code)
            assets = [
                {
                    "config_key": key,
                    "configured_path": str(default_config.get(key) or ""),
                    "status": "missing",
                    "resolved_path": None,
                    "source": None,
                    "reason": exc.message,
                    "asset_namespace": asset_namespace,
                }
                for key in asset_keys
            ] or [
                {
                    "config_key": "asset_namespace",
                    "configured_path": asset_namespace,
                    "status": "missing",
                    "resolved_path": None,
                    "source": None,
                    "reason": exc.message,
                    "asset_namespace": asset_namespace,
                }
            ]
            status = "missing"
            algos.append(
                {
                    "algo_code": algo_code,
                    "algo_name": row.get("algo_name"),
                    "is_enabled": row.get("is_enabled"),
                    "supported_freqs": row.get("supported_freqs") or [],
                    "min_bars": row.get("min_bars"),
                    "default_config": default_config,
                    "asset_namespace": asset_namespace,
                    "required_runtime_asset_keys": list(asset_keys),
                    "status": status,
                    "assets": assets,
                }
            )
            continue
        assets = [
            _asset_health(
                resolver=resolver,
                algo_code=algo_code,
                asset_namespace=asset_namespace,
                config_key=key,
                raw_path=default_config.get(key),
            )
            for key in asset_keys
        ]
        status = _algo_status([str(asset["status"]) for asset in assets])
        algos.append(
            {
                "algo_code": algo_code,
                "algo_name": row.get("algo_name"),
                "is_enabled": row.get("is_enabled"),
                "supported_freqs": row.get("supported_freqs") or [],
                "min_bars": row.get("min_bars"),
                "default_config": default_config,
                "asset_namespace": asset_namespace,
                "required_runtime_asset_keys": list(asset_keys),
                "status": status,
                "assets": assets,
            }
        )

    status_counts = {"ok": 0, "cached": 0, "missing": 0}
    for algo in algos:
        status_counts[str(algo["status"])] += 1

    overall_status = "missing" if status_counts["missing"] else "cached" if status_counts["cached"] else "ok"
    return {
        "overall_status": overall_status,
        "status_counts": status_counts,
        "cache_root": str(resolver.cache_root),
        "generated_at": datetime.now(UTC).isoformat(),
        "algos": algos,
    }
