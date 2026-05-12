"""Validation helpers for execution_algorithm_catalog migrations.

These helpers are intentionally side-effect-free by default: migration smoke
checks should prove model assets are reachable before catalog rows become usable,
not silently create/copy assets during validation.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Protocol

from backend.services.strategy_package.model_asset_resolver import ModelAssetResolver
from backend.services.trading_core.errors import DataUnavailableError
from backend.services.trading_core.execution_algo_capabilities import required_runtime_asset_keys


class ExecutionAlgoArtifactMissingError(DataUnavailableError):
    """Raised when an enabled catalog algo references an unreachable model."""

    error_code = "EXECUTION_ALGO_ARTIFACT_MISSING"


class RuntimeAssetResolver(Protocol):
    def resolve_runtime_asset(
        self,
        *,
        manifest: Any,
        config_key: str,
        copy_missing: bool = True,
    ) -> Any: ...


@dataclass(frozen=True)
class CatalogAlgoValidationResult:
    """Successful validation result for one runtime asset key."""

    algo_code: str
    config_key: str
    original_path: str
    resolved_path: str | None = None


@dataclass(frozen=True)
class _CatalogPolicy:
    algo_code: str
    algo_config: Mapping[str, Any]


@dataclass(frozen=True)
class _CatalogValidationManifest:
    package_id: str
    minute_execution_policy: _CatalogPolicy


_MODEL_PATH_KEY_CANDIDATES = ("model_path", "early_model_path", "late_model_path")


def validate_enabled_default_config_model_paths(
    catalog_rows: Iterable[Mapping[str, Any]],
    *,
    resolver: RuntimeAssetResolver | None = None,
    copy_missing: bool = False,
) -> list[CatalogAlgoValidationResult]:
    """Validate enabled catalog default_config model paths.

    Args:
        catalog_rows: Rows containing at least algo_code, default_config, and
            optionally is_enabled. Disabled rows are skipped.
        resolver: Injectable resolver for offline unit tests; defaults to
            ModelAssetResolver for production/migration validation.
        copy_missing: Passed through to the resolver. Defaults to False so a
            migration validation cannot hide missing assets by copying them.

    Raises:
        ExecutionAlgoArtifactMissingError: if a required model path is missing,
            invalid, or unreachable through the resolver.
    """

    active_resolver = resolver or ModelAssetResolver()
    results: list[CatalogAlgoValidationResult] = []

    for row in catalog_rows:
        algo_code = _normalize_algo_code(row.get("algo_code"))
        if not algo_code or not _is_enabled(row.get("is_enabled", True)):
            continue

        default_config = _coerce_default_config(row.get("default_config"), algo_code=algo_code)
        default_config = _with_row_asset_namespace(default_config, row.get("asset_namespace"))
        asset_keys = _runtime_asset_keys(algo_code, default_config)
        if not asset_keys:
            continue

        manifest = _CatalogValidationManifest(
            package_id=f"execution_algorithm_catalog:{algo_code}",
            minute_execution_policy=_CatalogPolicy(
                algo_code=algo_code,
                algo_config=default_config,
            ),
        )

        for config_key in asset_keys:
            original_path = str(default_config.get(config_key) or "").strip()
            if not original_path:
                raise _artifact_missing(
                    algo_code=algo_code,
                    config_key=config_key,
                    asset_path=original_path,
                    cause=None,
                    message=f"{algo_code} default_config.{config_key} is required for migration validation",
                )
            try:
                resolved = active_resolver.resolve_runtime_asset(
                    manifest=manifest,
                    config_key=config_key,
                    copy_missing=copy_missing,
                )
            except DataUnavailableError as exc:
                raise _artifact_missing(
                    algo_code=algo_code,
                    config_key=config_key,
                    asset_path=original_path,
                    cause=exc,
                    message=f"{algo_code} default_config.{config_key} is not accessible from AIstock backend",
                ) from exc

            results.append(
                CatalogAlgoValidationResult(
                    algo_code=algo_code,
                    config_key=config_key,
                    original_path=original_path,
                    resolved_path=str(getattr(resolved, "resolved_path", "") or "") or None,
                )
            )

    return results


def validate_enabled_default_config_model_paths_from_db(
    connection: Any,
    *,
    resolver: RuntimeAssetResolver | None = None,
    copy_missing: bool = False,
) -> list[CatalogAlgoValidationResult]:
    """Load enabled catalog rows from a DB-API connection and validate assets."""

    asset_namespace_select = "asset_namespace" if _has_asset_namespace_column(connection) else "NULL AS asset_namespace"
    cursor = connection.cursor()
    cursor.execute(
        f"""
        SELECT algo_code, default_config, is_enabled, {asset_namespace_select}
        FROM public.execution_algorithm_catalog
        WHERE is_enabled = TRUE
        ORDER BY algo_code
        """
    )
    columns = [desc[0] for desc in cursor.description]
    rows = [dict(zip(columns, values)) for values in cursor.fetchall()]
    return validate_enabled_default_config_model_paths(
        rows,
        resolver=resolver,
        copy_missing=copy_missing,
    )


def _normalize_algo_code(value: Any) -> str:
    return str(value or "").strip().upper()


def _is_enabled(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip().lower() not in {"", "0", "false", "f", "no", "n", "off"}
    return bool(value)


def _coerce_default_config(value: Any, *, algo_code: str) -> Mapping[str, Any]:
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError as exc:
            raise _artifact_missing(
                algo_code=algo_code,
                config_key="default_config",
                asset_path="",
                cause=exc,
                message=f"{algo_code} default_config must be valid JSON for migration validation",
            ) from exc
        if isinstance(parsed, Mapping):
            return parsed
    raise _artifact_missing(
        algo_code=algo_code,
        config_key="default_config",
        asset_path="",
        cause=None,
        message=f"{algo_code} default_config must be a JSON object for migration validation",
    )


def _with_row_asset_namespace(default_config: Mapping[str, Any], asset_namespace: Any) -> Mapping[str, Any]:
    namespace = str(asset_namespace or "").strip()
    if not namespace:
        return default_config
    merged = dict(default_config)
    merged["asset_namespace"] = namespace
    return merged


def _runtime_asset_keys(algo_code: str, default_config: Mapping[str, Any]) -> tuple[str, ...]:
    capability_keys = required_runtime_asset_keys(algo_code)
    if capability_keys:
        return capability_keys
    # Migration validation should still fail early for newly registered model
    # algos if capability metadata lags but default_config already declares
    # conventional model path keys.
    return tuple(key for key in _MODEL_PATH_KEY_CANDIDATES if key in default_config)


def _has_asset_namespace_column(connection: Any) -> bool:
    cursor = connection.cursor()
    cursor.execute(
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
    row = cursor.fetchone()
    return bool(row and (row[0] if not hasattr(row, "get") else row.get("exists")))


def _artifact_missing(
    *,
    algo_code: str,
    config_key: str,
    asset_path: str,
    cause: BaseException | None,
    message: str,
) -> ExecutionAlgoArtifactMissingError:
    context: dict[str, Any] = {
        "algo_code": algo_code,
        "config_key": config_key,
        "asset_path": asset_path,
        "catalog_table": "execution_algorithm_catalog",
        "validation": "default_config_model_paths",
    }
    if isinstance(cause, DataUnavailableError):
        context["resolver_error_code"] = cause.error_code
        context["resolver_message"] = cause.message
        context["resolver_context"] = cause.context
    elif cause is not None:
        context["cause"] = f"{type(cause).__name__}: {cause}"
    return ExecutionAlgoArtifactMissingError(
        f"EXECUTION_ALGO_ARTIFACT_MISSING: {message}",
        context=context,
    )
