"""Execution algorithm capability metadata for authoritative minute trading."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .errors import DataUnavailableError, StrategyPackageValidationError


@dataclass(frozen=True)
class ExecutionAlgoCapability:
    """Paper v2 runtime requirements that cannot be inferred from registration."""

    algo_code: str
    min_required_bars: int = 1
    runtime_asset_keys: tuple[str, ...] = ()


DEFAULT_EXECUTION_ALGO_CAPABILITY = ExecutionAlgoCapability(algo_code="*", min_required_bars=1)

EXECUTION_ALGO_CAPABILITIES: dict[str, ExecutionAlgoCapability] = {
    "V24_PLAN": ExecutionAlgoCapability(
        algo_code="V24_PLAN",
        min_required_bars=31,
        runtime_asset_keys=("model_path",),
    ),
    "V25_TWO_STAGE": ExecutionAlgoCapability(
        algo_code="V25_TWO_STAGE",
        min_required_bars=240,
        runtime_asset_keys=("early_model_path", "late_model_path"),
    ),
}


def normalize_execution_algo_code(algo_code: Any) -> str:
    normalized = str(algo_code or "").strip().upper()
    if not normalized:
        raise StrategyPackageValidationError("minute execution algo_code is required")
    return normalized


def get_execution_algo_capability(algo_code: Any) -> ExecutionAlgoCapability:
    normalized = normalize_execution_algo_code(algo_code)
    return EXECUTION_ALGO_CAPABILITIES.get(
        normalized,
        ExecutionAlgoCapability(algo_code=normalized, min_required_bars=DEFAULT_EXECUTION_ALGO_CAPABILITY.min_required_bars),
    )


def required_runtime_asset_keys(algo_code: Any) -> tuple[str, ...]:
    return get_execution_algo_capability(algo_code).runtime_asset_keys


def required_minute_bars_for_policy(policy_json: dict[str, Any], *, package_id: str | None = None) -> int:
    if not isinstance(policy_json, dict):
        raise StrategyPackageValidationError(
            "minute execution policy must be an object",
            context={"package_id": package_id},
        )
    config = policy_json.get("algo_config") or {}
    if not isinstance(config, dict):
        raise StrategyPackageValidationError(
            "minute execution algo_config must be an object",
            context={"package_id": package_id},
        )
    configured = config.get("min_observed_bars") or config.get("min_required_bars")
    if configured is not None:
        try:
            value = int(configured)
        except (TypeError, ValueError) as exc:
            raise StrategyPackageValidationError(
                "minute execution min_required_bars must be an integer",
                context={"package_id": package_id, "min_required_bars": configured},
            ) from exc
        if value <= 0:
            raise StrategyPackageValidationError(
                "minute execution min_required_bars must be positive",
                context={"package_id": package_id, "min_required_bars": configured},
            )
        return value
    capability = get_execution_algo_capability(policy_json.get("algo_code"))
    return capability.min_required_bars


def validate_runtime_asset_paths(
    *,
    algo_code: Any,
    algo_config: dict[str, Any],
    package_id: str | None = None,
) -> dict[str, Path]:
    """Validate required runtime asset files and return resolved local paths."""

    normalized = normalize_execution_algo_code(algo_code)
    if not isinstance(algo_config, dict):
        raise StrategyPackageValidationError(
            "minute execution algo_config must be an object",
            context={"package_id": package_id, "algo_code": normalized},
        )
    resolved: dict[str, Path] = {}
    for key in required_runtime_asset_keys(normalized):
        raw_path = str(algo_config.get(key) or "").strip()
        if not raw_path:
            raise DataUnavailableError(
                f"{normalized} requires config.{key}",
                context={"package_id": package_id, "algo_code": normalized, "config_key": key},
            )
        path = Path(raw_path)
        if not path.exists() or not path.is_file():
            raise DataUnavailableError(
                f"{normalized} {key} is not accessible from AIstock backend",
                context={
                    "package_id": package_id,
                    "algo_code": normalized,
                    "config_key": key,
                    "asset_path": raw_path,
                },
            )
        if path.stat().st_size <= 0:
            raise DataUnavailableError(
                f"{normalized} {key} asset is empty",
                context={
                    "package_id": package_id,
                    "algo_code": normalized,
                    "config_key": key,
                    "asset_path": raw_path,
                },
            )
        resolved[key] = path
    return resolved
