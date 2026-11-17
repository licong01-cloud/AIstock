"""Resolve failed deterministic account attempts without changing their records."""
from __future__ import annotations

from collections import Counter
import hashlib
import json
from typing import Any

from .absolute_target_consumer import validate_absolute_result
from .experiment_config_builders import build_config_from_custom_evo_loop
from .qe_absolute_targets import validate_mode
from .runtime_contract import merge_qe_minute_runtime_contract


# These are the scientific/execution inputs outside the frozen target contract.
# Routing, submission IDs and process metadata are deliberately not identities.
_ACCOUNT_FIELDS = (
    "strategy_params", "execution_algo", "execution_algo_params", "data_split",
    "stock_pool", "universe_selection", "suspend_filter_strict",
    "filter_suspended_on_signal", "runtime_flags", "sector_blacklist",
    "unfilled_handler", "unfilled_handler_params",
)


def _object(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict):
        raise ValueError("QE_CUSTOM_EVO_OUTCOME_OBJECT_REQUIRED")
    return value


def _identity(config: dict[str, Any]) -> str | None:
    contract = validate_mode(config)
    if contract is None:
        # Stochastic/model experiments and ordinary additions stay independent.
        return None
    value = {key: config.get(key) for key in _ACCOUNT_FIELDS}
    value["absolute_target_replay"] = contract
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _recorded_identity(recorded: dict[str, Any], configured: dict[str, Any]) -> str | None:
    if not recorded.get("model_params"):
        return _identity(recorded)
    # The production config_record stores flattened *execution* custom_params
    # under this legacy name. Reproduce the actual serializer; never drop these
    # fields blindly or accept arbitrary hyperparameters as harmless metadata.
    source = recorded.get("runtime_contract_source")
    if not isinstance(source, str) or not source:
        raise ValueError("QE_CUSTOM_EVO_OUTCOME_RUNTIME_SOURCE_REQUIRED")
    planned = build_config_from_custom_evo_loop(configured, {})
    expected = merge_qe_minute_runtime_contract(
        planned.build_custom_params(), config=recorded,
        execution_algo=planned.execution_algo, execution_algo_params=planned.execution_algo_params,
        source=source, require_minute=True,
    )
    if recorded["model_params"] != expected:
        raise ValueError("QE_CUSTOM_EVO_OUTCOME_RUNTIME_PARAMS_MISMATCH")
    return _identity({**recorded, "model_params": None})


def resolve_custom_evo_attempts(
    loops_config: list[dict[str, Any]], rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """A later verified identical replay can resolve an earlier failed attempt.

    Do not group by label, model, seed or arm alone. Require the complete pinned
    contract, matching execution settings and the actual recorded configuration.
    Missing receipts, partial windows, later failures and cancellations remain
    unresolved. This function neither submits work nor mutates its arguments.
    """
    configs = {int(cfg.get("loop_index") or n): cfg for n, cfg in enumerate(loops_config, 1)}
    actual = {int(row["loop_index"]): row for row in rows}
    if len(configs) != len(loops_config) or len(actual) != len(rows):
        raise ValueError("QE_CUSTOM_EVO_OUTCOME_DUPLICATE_LOOP_INDEX")
    identities: dict[int, str] = {}
    verified: dict[str, list[int]] = {}
    unverified_completed: list[int] = []
    for index, cfg in configs.items():
        if cfg.get("absolute_target_replay") is None:
            continue
        row = actual.get(index)
        if row is None:
            continue
        try:
            identity = _identity(cfg)
            recorded = _object(row.get("config_json"))
            if identity is None or _recorded_identity(recorded, cfg) != identity:
                raise ValueError("QE_CUSTOM_EVO_OUTCOME_SNAPSHOT_MISMATCH")
            identities[index] = identity
            if row["status"] == "completed":
                if not row.get("experiment_id"):
                    raise ValueError("QE_CUSTOM_EVO_OUTCOME_EXPERIMENT_REQUIRED")
                metrics = _object(row.get("metrics_json"))
                validate_absolute_result(metrics.get("absolute_target_result"), cfg["absolute_target_replay"])
                verified.setdefault(identity, []).append(index)
        except (ValueError, TypeError, KeyError):
            # An incomplete proof cannot resolve a failure or establish success.
            if row.get("status") == "completed":
                unverified_completed.append(index)

    recovered: dict[int, int] = {}
    for index, row in actual.items():
        if row.get("status") != "failed" or index not in identities:
            continue
        replacements = [candidate for candidate in verified.get(identities[index], []) if candidate > index]
        if replacements:
            recovered[index] = min(replacements)

    counts = Counter(
        "invalid_result" if index in unverified_completed else str(row["status"])
        for index, row in actual.items() if index not in recovered
    )
    return {
        "expected_count": len(loops_config) - len(recovered),
        "status_counts": dict(counts),
        "effective_loop_indexes": sorted(index for index in configs if index not in recovered),
        "recovered_attempts": [
            {"loop_index": index, "resolved_by_loop_index": replacement}
            for index, replacement in sorted(recovered.items())
        ],
        "unverified_completed_loop_indexes": sorted(unverified_completed),
        "historical_status_counts": dict(Counter(str(row["status"]) for row in rows)),
    }
