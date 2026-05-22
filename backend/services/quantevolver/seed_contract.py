"""Fail-fast fixed-seed contract for QE experiment generation and execution."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from fastapi import HTTPException

from .experiment_config import normalize_qe_random_seed

SEED_ALIAS_KEYS = ("random_seed", "seed", "loop_seed", "random_state", "torch_seed", "numpy_seed")


def _pop_seed_aliases(source: dict[str, Any], *, prefer_existing: Any = None) -> Any:
    """Pop seed aliases from a mutable mapping and return the first explicit value."""

    seed_value = prefer_existing
    for source_key in SEED_ALIAS_KEYS:
        if source.get(source_key) not in (None, ""):
            if seed_value in (None, ""):
                seed_value = source.get(source_key)
            source.pop(source_key, None)
    return seed_value


def ensure_loop_fixed_seed(loop: dict[str, Any], *, context: str, trainable: bool | None = None) -> int | None:
    """Normalize runtime_flags.random_seed on a loop dict or fail before scheduling.

    The generation boundary is authoritative: aliases may arrive from top-level
    loop fields, strategy_params, or model_params, but the normalized executable
    contract is always loop.runtime_flags.random_seed.
    """

    if trainable is None:
        trainable = not bool(loop.get("backtest_only"))

    runtime_flags = dict(loop.get("runtime_flags") or {})
    seed_value = _pop_seed_aliases(runtime_flags, prefer_existing=runtime_flags.get("random_seed"))

    seed_value = _pop_seed_aliases(loop, prefer_existing=seed_value)

    strategy_params = loop.get("strategy_params")
    if isinstance(strategy_params, Mapping):
        mutable_strategy = dict(strategy_params)
        seed_value = _pop_seed_aliases(mutable_strategy, prefer_existing=seed_value)
        loop["strategy_params"] = mutable_strategy

    model_params = loop.get("model_params")
    if isinstance(model_params, Mapping):
        mutable_model = dict(model_params)
        seed_value = _pop_seed_aliases(mutable_model, prefer_existing=seed_value)
        loop["model_params"] = mutable_model

    if seed_value not in (None, ""):
        runtime_flags["random_seed"] = normalize_qe_random_seed(
            seed_value,
            field_name=f"{context}.runtime_flags.random_seed",
        )
        loop["runtime_flags"] = runtime_flags
        return int(runtime_flags["random_seed"])

    loop["runtime_flags"] = runtime_flags
    if trainable:
        raise ValueError(f"{context}: runtime_flags.random_seed is required for trainable QE loops")
    return None


def normalize_single_experiment_seed_config(config_json: Mapping[str, Any]) -> dict[str, Any]:
    """Return single-experiment config with custom_params.random_seed normalized."""

    config = dict(config_json or {})
    custom_params = dict(config.get("custom_params") or {})
    seed_value = custom_params.get("random_seed")
    seed_value = _pop_seed_aliases(config, prefer_existing=seed_value)
    seed_value = _pop_seed_aliases(custom_params, prefer_existing=seed_value)
    if seed_value in (None, ""):
        raise ValueError("single_experiment.custom_params.random_seed is required")
    custom_params["random_seed"] = normalize_qe_random_seed(
        seed_value,
        field_name="single_experiment.custom_params.random_seed",
    )
    config["custom_params"] = custom_params
    return config


def ensure_template_fixed_seeds(template_kind: str, config_json: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and normalize fixed seeds at the template/MCP generation boundary."""

    config = dict(config_json or {})
    if template_kind == "custom_evo":
        loops = config.get("loops") or []
        if isinstance(loops, list):
            normalized_loops: list[Any] = []
            for idx, raw_loop in enumerate(loops, start=1):
                if isinstance(raw_loop, Mapping):
                    loop = dict(raw_loop)
                    ensure_loop_fixed_seed(loop, context=f"custom_evo.loops[{idx}]")
                    normalized_loops.append(loop)
                else:
                    normalized_loops.append(raw_loop)
            config["loops"] = normalized_loops
        return config
    if template_kind == "single_experiment":
        return normalize_single_experiment_seed_config(config)
    return config


def raise_http_seed_error(exc: ValueError) -> None:
    raise HTTPException(status_code=400, detail=str(exc)) from exc
