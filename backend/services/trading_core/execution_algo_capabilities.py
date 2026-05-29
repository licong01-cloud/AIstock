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
    historical_min_required_bars: int = 1
    historical_requires_full_day: bool = False
    live_supported: bool = False
    live_min_start_bars: int = 1
    live_step_mode: str = "unsupported"
    plan_horizon_bars: int | None = None
    runtime_asset_keys: tuple[str, ...] = ()

    @property
    def min_required_bars(self) -> int:
        """Backward-compatible historical requirement used by day replay."""

        return self.historical_min_required_bars


DEFAULT_EXECUTION_ALGO_CAPABILITY = ExecutionAlgoCapability(algo_code="*")

EXECUTION_ALGO_CAPABILITIES: dict[str, ExecutionAlgoCapability] = {
    "CLOSE_PRICE": ExecutionAlgoCapability(
        algo_code="CLOSE_PRICE",
        historical_min_required_bars=1,
        live_supported=True,
        live_min_start_bars=1,
        live_step_mode="close_bar_only",
    ),
    "TWAP": ExecutionAlgoCapability(
        algo_code="TWAP",
        historical_min_required_bars=1,
        live_supported=True,
        live_min_start_bars=1,
        live_step_mode="streaming_step",
    ),
    "VWAP": ExecutionAlgoCapability(
        algo_code="VWAP",
        historical_min_required_bars=1,
        live_supported=False,
        live_min_start_bars=1,
        live_step_mode="unsupported",
    ),
    "POV": ExecutionAlgoCapability(
        algo_code="POV",
        historical_min_required_bars=1,
        live_supported=True,
        live_min_start_bars=1,
        live_step_mode="streaming_step",
    ),
    "V24_PLAN": ExecutionAlgoCapability(
        algo_code="V24_PLAN",
        historical_min_required_bars=31,
        live_supported=False,
        live_min_start_bars=31,
        live_step_mode="unsupported",
        runtime_asset_keys=("model_path",),
    ),
    "V25_TWO_STAGE": ExecutionAlgoCapability(
        algo_code="V25_TWO_STAGE",
        historical_min_required_bars=240,
        historical_requires_full_day=True,
        live_supported=True,
        live_min_start_bars=1,
        live_step_mode="persisted_plan",
        plan_horizon_bars=240,
        runtime_asset_keys=("early_model_path", "late_model_path"),
    ),
    "V25_1_SMALL_CAP": ExecutionAlgoCapability(
        algo_code="V25_1_SMALL_CAP",
        historical_min_required_bars=240,
        historical_requires_full_day=True,
        live_supported=True,
        live_min_start_bars=1,
        live_step_mode="persisted_plan",
        plan_horizon_bars=240,
        runtime_asset_keys=("early_model_path", "late_model_path"),
    ),
    "SNIPER_MINIQMT": ExecutionAlgoCapability(
        algo_code="SNIPER_MINIQMT",
        historical_min_required_bars=1,
        live_supported=True,
        live_min_start_bars=1,
        live_step_mode="miniqmt_event_driven_tick",
    ),
    "BEST_LIMIT_MINIQMT": ExecutionAlgoCapability(
        algo_code="BEST_LIMIT_MINIQMT",
        historical_min_required_bars=1,
        live_supported=True,
        live_min_start_bars=1,
        live_step_mode="miniqmt_event_driven_tick",
    ),
    "TWAP_LITE_MINIQMT": ExecutionAlgoCapability(
        algo_code="TWAP_LITE_MINIQMT",
        historical_min_required_bars=1,
        live_supported=True,
        live_min_start_bars=1,
        live_step_mode="miniqmt_event_driven_timer",
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
        ExecutionAlgoCapability(
            algo_code=normalized,
            historical_min_required_bars=DEFAULT_EXECUTION_ALGO_CAPABILITY.historical_min_required_bars,
            live_supported=False,
        ),
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
    return capability.historical_min_required_bars


def require_execution_algo_supports_mode(
    policy_json: dict[str, Any],
    *,
    mode: str,
    package_id: str | None = None,
) -> ExecutionAlgoCapability:
    """Validate that a policy can run in the requested Paper v2 session mode."""

    if not isinstance(policy_json, dict):
        raise StrategyPackageValidationError(
            "minute execution policy must be an object",
            context={"package_id": package_id, "mode": mode},
        )
    normalized_mode = str(mode or "").strip().upper()
    capability = get_execution_algo_capability(policy_json.get("algo_code"))
    if normalized_mode in {"REPLAY_ONLY", "HISTORICAL", "HISTORICAL_REPLAY"}:
        return capability
    if normalized_mode in {"LIVE_ONLY", "LIVE", "CATCHUP_THEN_LIVE"}:
        if not capability.live_supported:
            from .errors import AlgoRealtimeUnsupportedError

            raise AlgoRealtimeUnsupportedError(
                "minute execution algorithm is not declared real-time safe for Paper v2",
                context={
                    "package_id": package_id,
                    "algo_code": capability.algo_code,
                    "mode": normalized_mode,
                    "historical_min_required_bars": capability.historical_min_required_bars,
                    "live_min_start_bars": capability.live_min_start_bars,
                    "plan_horizon_bars": capability.plan_horizon_bars,
                    "reason": "live incremental adapter is not available; no algorithm fallback is allowed",
                },
            )
        return capability
    from .errors import AlgoModeUnsupportedError

    raise AlgoModeUnsupportedError(
        "unsupported Paper v2 execution mode",
        context={"package_id": package_id, "mode": normalized_mode, "algo_code": capability.algo_code},
    )


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
