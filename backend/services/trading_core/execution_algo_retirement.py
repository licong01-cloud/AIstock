"""Single code authority for retired execution algorithms.

Retirement applies only to explicit execution-algorithm semantic fields.  It
does not scan arbitrary text, filenames, stock pools, universes, reports, or
historical artifacts for matching strings.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .errors import UnsupportedFeatureError


V25_EXECUTION_ALGO_RETIRED = "V25_EXECUTION_ALGO_RETIRED"
RETIRED_EXECUTION_ALGO_CODES = frozenset({"V25_TWO_STAGE", "V25_1_SMALL_CAP"})


class ExecutionAlgoRetiredError(UnsupportedFeatureError):
    """Raised before a retired execution algorithm can create new work."""

    error_code = V25_EXECUTION_ALGO_RETIRED


def normalize_execution_algo_code_for_retirement(value: Any) -> str:
    """Normalize one already-identified execution-algorithm semantic value."""

    return str(value or "").strip().upper()


def is_retired_execution_algo(value: Any) -> bool:
    return normalize_execution_algo_code_for_retirement(value) in RETIRED_EXECUTION_ALGO_CODES


def execution_algo_retirement_projection(value: Any) -> dict[str, Any]:
    """Return API/catalog projection fields without mutating historical data."""

    algo_code = normalize_execution_algo_code_for_retirement(value)
    retired = algo_code in RETIRED_EXECUTION_ALGO_CODES
    return {
        "retired": retired,
        "selectable": not retired,
        "activatable": not retired,
        "retirement_reason_code": V25_EXECUTION_ALGO_RETIRED if retired else None,
    }


def require_execution_algo_active(
    value: Any,
    *,
    operation: str,
    semantic_path: str = "algo_code",
    context: Mapping[str, Any] | None = None,
) -> str:
    """Fail before any model, workspace, database-write, or broker side effect."""

    algo_code = normalize_execution_algo_code_for_retirement(value)
    if algo_code not in RETIRED_EXECUTION_ALGO_CODES:
        return algo_code
    raise ExecutionAlgoRetiredError(
        f"execution algorithm {algo_code} is retired and cannot create new executable work",
        context={
            **dict(context or {}),
            "reason_code": V25_EXECUTION_ALGO_RETIRED,
            "algo_code": algo_code,
            "operation": str(operation or "unknown"),
            "semantic_path": semantic_path,
            "fallback_used": False,
            "side_effect_started": False,
            "historical_artifacts_readable": True,
        },
    )


def require_execution_policy_active(
    policy_json: Mapping[str, Any],
    *,
    operation: str,
    semantic_path: str = "policy_json.algo_code",
    context: Mapping[str, Any] | None = None,
) -> str:
    """Validate only the policy's explicit top-level ``algo_code`` field."""

    if not isinstance(policy_json, Mapping):
        return ""
    return require_execution_algo_active(
        policy_json.get("algo_code"),
        operation=operation,
        semantic_path=semantic_path,
        context=context,
    )


def require_strategy_manifest_execution_algos_active(
    manifest: Any,
    *,
    operation: str,
    context: Mapping[str, Any] | None = None,
) -> None:
    """Check only declared StrategyPackage execution-policy semantic paths."""

    if hasattr(manifest, "model_dump"):
        payload = manifest.model_dump(mode="python")
    elif isinstance(manifest, Mapping):
        payload = dict(manifest)
    else:
        return

    minute_policy = payload.get("minute_execution_policy")
    if isinstance(minute_policy, Mapping):
        require_execution_algo_active(
            minute_policy.get("algo_code"),
            operation=operation,
            semantic_path="manifest.minute_execution_policy.algo_code",
            context=context,
        )

    backtest_context = payload.get("backtest_context")
    execution = backtest_context.get("execution") if isinstance(backtest_context, Mapping) else None
    if isinstance(execution, Mapping):
        require_execution_algo_active(
            execution.get("execution_algo"),
            operation=operation,
            semantic_path="manifest.backtest_context.execution.execution_algo",
            context=context,
        )
