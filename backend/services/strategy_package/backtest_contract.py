"""Backtest runtime contract helpers shared by QE packages and Paper v2.

The contract is deliberately limited to strategy semantics that must remain
reproducible across QE, Paper v2, and MiniQMT paths. Platform capabilities such
as HMM, ST PIT risk data, and event-signal policies stay in the runtime profile
or platform services and are never re-injected from frozen QE custom_params.
"""

from __future__ import annotations

from typing import Any

from backend.services.selection_center.runtime_profile import (
    SelectionRuntimeProfile,
    normalize_selection_runtime_config,
    parse_selection_runtime_profile,
)
from backend.services.strategy_package.models import StrategyPackageManifest
from backend.services.strategy_package.runtime_variant import RuntimeVariantValidationStatus
from backend.services.trading_core.errors import StrategyPackageValidationError, UnsupportedFeatureError


SCORE_WEIGHTED_V1_IDS = {
    "score_weighted_topk",
    "score_weighted_topk_v1",
    "ScoreWeightedTopkStrategy",
    "SuspendFilterScoreWeightedTopkStrategy",
}
SCORE_WEIGHTED_V2_IDS = {
    "score_weighted_topk_v2",
    "ScoreWeightedTopkStrategyV2",
    "SuspendFilterScoreWeightedTopkStrategyV2",
    "score_weighted_topk_v2_capacity_v1",
    "ScoreWeightedTopkStrategyV2CapacityV1",
    "SuspendFilterScoreWeightedTopkStrategyV2CapacityV1",
}
SCORE_WEIGHTED_V2_CAPACITY_V1_IDS = {
    "score_weighted_topk_v2_capacity_v1",
    "ScoreWeightedTopkStrategyV2CapacityV1",
    "SuspendFilterScoreWeightedTopkStrategyV2CapacityV1",
}

SCORE_WEIGHTED_DEFAULTS: dict[str, Any] = {
    "weight_method": "softmax",
    "temperature": 1.0,
    "score_clip_quantile": 0.0,
    "max_weight": 0.05,
    "min_weight": 0.005,
    "max_position_ratio": 0.95,
    "enable_dynamic_ndrop": True,
    "max_n_drop": 5,
    "min_n_drop": 0,
    "threshold_method": "adaptive",
    "min_improvement": 0.01,
    "adaptive_multiplier": 0.5,
    "threshold_floor": 0.005,
    "min_trade_price": 0.5,
    "max_trade_price": 5000.0,
    "max_single_order_value": 5_000_000.0,
    "lot_size": 100,
    "hold_thresh": 0,
    "only_tradable": True,
    "forbid_all_trade_at_limit": False,
    "risk_degree": 1.0,
}
SCORE_WEIGHTED_CAPACITY_V1_DEFAULTS: dict[str, Any] = {
    **SCORE_WEIGHTED_DEFAULTS,
    "max_single_order_value": 1_000_000_000.0,
    "max_weight": 0.05,
    "max_position_ratio": 0.95,
}


def build_backtest_runtime_contract(
    manifest: StrategyPackageManifest,
    runtime_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Normalize package source evidence needed by Paper v2 runtime."""

    manifest = _effective_manifest_for_contract(manifest, runtime_config)
    evidence = _runtime_evidence(manifest)
    daily_strategy = evidence.get("daily_strategy") if isinstance(evidence.get("daily_strategy"), dict) else {}
    custom_params = daily_strategy.get("custom_params") if isinstance(daily_strategy, dict) else None
    if not isinstance(custom_params, dict):
        custom_params = {}
    strategy_config = manifest.strategy_config or {}
    strategy_marker = str(
        custom_params.get("strategy_class")
        or custom_params.get("strategy_id")
        or daily_strategy.get("strategy_id")
        or strategy_config.get("strategy_id")
        or "score_weighted_topk_v2"
    ).strip()
    if strategy_marker not in (SCORE_WEIGHTED_V1_IDS | SCORE_WEIGHTED_V2_IDS | SCORE_WEIGHTED_V2_CAPACITY_V1_IDS):
        if manifest.is_legacy_runtime_manifest:
            strategy_marker = "score_weighted_topk_v2"
        else:
            raise UnsupportedFeatureError(
                "Paper v2 does not support the StrategyPackage daily strategy contract yet",
                context={"package_id": manifest.package_id, "strategy_marker": strategy_marker},
            )
    strategy_family = _portfolio_strategy_family(strategy_marker)
    portfolio_params = _portfolio_strategy_params(
        strategy_family=strategy_family,
        strategy_marker=strategy_marker,
        custom_params=custom_params,
        manifest=manifest,
    )
    capacity_profile = _portfolio_capacity_profile(strategy_marker, strategy_family)
    return {
        "contract_version": "qe_paper_runtime_contract_v1",
        "source": manifest.source.model_dump(mode="json"),
        "portfolio_strategy": {
            "strategy_id": strategy_marker,
            "strategy_marker": strategy_marker,
            "strategy_family": strategy_family,
            "capacity_profile": capacity_profile,
            "params": portfolio_params,
        },
        "runtime_features": {
            # Platform-owned runtime capabilities are recorded as metadata only.
            "hmm": _platform_feature_contract("hmm"),
            "industry_blacklist": _industry_blacklist_contract(custom_params),
            "tradability": _tradability_contract(custom_params),
            "risk_policy": _platform_feature_contract("risk_policy"),
            "event_signal_policy": _platform_feature_contract("event_signal_policy"),
            "variant": _runtime_variant_contract(runtime_config),
        },
        "execution_policy_reference": {
            "authority": "validated_execution_policy",
            "package_bound": False,
            "source_execution_evidence": evidence.get("execution") or {},
        },
    }


def validate_runtime_profile_matches_backtest_contract(
    manifest: StrategyPackageManifest,
    runtime_profile: SelectionRuntimeProfile,
    *,
    runtime_config: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Validate StrategyPackage-bound runtime semantics.

    HMM, ST PIT risk policy, event-signal policy, tradability, and blacklist
    settings are platform runtime choices. This check intentionally leaves them
    to their dedicated platform parsers/preflights instead of binding them to
    historical QE custom_params.
    """

    contract = build_backtest_runtime_contract(manifest, runtime_config)
    return contract


def normalize_runtime_config_with_backtest_contract(
    manifest: StrategyPackageManifest,
    runtime_config: dict[str, Any] | None,
    *,
    context: dict[str, Any] | None = None,
    include_contract: bool = False,
    inherit_source_defaults: bool = True,
) -> dict[str, Any]:
    """Normalize a Paper v2 runtime config against the strategy contract.

    Only StrategyPackage-owned selection semantics are inherited from the frozen
    contract. Platform runtime choices remain caller-provided or defaulted by
    ``SelectionRuntimeProfile``.
    """

    if runtime_config is not None and not isinstance(runtime_config, dict):
        raise StrategyPackageValidationError(
            "runtime_config must be an object",
            context={**(context or {}), "runtime_config_type": type(runtime_config).__name__},
        )
    config = dict(runtime_config or {})
    effective_manifest = _effective_manifest_for_contract(manifest, config)
    contract = build_backtest_runtime_contract(manifest, config)
    raw_profile = config.get("runtime_profile")
    if raw_profile is not None and not isinstance(raw_profile, dict):
        raise StrategyPackageValidationError(
            "runtime_config.runtime_profile must be an object",
            context={**(context or {}), "runtime_profile_type": type(raw_profile).__name__},
        )
    profile_payload: dict[str, Any] = dict(raw_profile or {})
    if inherit_source_defaults:
        _apply_backtest_context_defaults(effective_manifest, profile_payload, config)

    _normalize_selection_profile(
        profile_payload=profile_payload,
        config=config,
        context={**(context or {}), "package_id": manifest.package_id},
    )

    config["runtime_profile"] = profile_payload
    normalized = normalize_selection_runtime_config(config)
    runtime_profile = parse_selection_runtime_profile(normalized)
    validate_runtime_profile_matches_backtest_contract(
        manifest,
        runtime_profile,
        runtime_config=normalized,
        context={**(context or {}), "package_id": manifest.package_id},
    )
    if include_contract:
        normalized["qe_backtest_runtime_contract"] = contract
    return normalized


def _portfolio_strategy_family(strategy_marker: str) -> str:
    marker = strategy_marker.strip()
    if marker in SCORE_WEIGHTED_V2_IDS:
        return "score_weighted_topk_v2"
    if marker in SCORE_WEIGHTED_V1_IDS:
        return "score_weighted_topk_v1"
    raise UnsupportedFeatureError(
        "Paper v2 does not support the QE portfolio strategy contract yet",
        context={
            "strategy_marker": strategy_marker,
            "supported": sorted(SCORE_WEIGHTED_V1_IDS | SCORE_WEIGHTED_V2_IDS | SCORE_WEIGHTED_V2_CAPACITY_V1_IDS),
        },
    )


def _portfolio_strategy_params(
    *,
    strategy_family: str,
    strategy_marker: str,
    custom_params: dict[str, Any],
    manifest: StrategyPackageManifest,
) -> dict[str, Any]:
    defaults = (
        SCORE_WEIGHTED_CAPACITY_V1_DEFAULTS
        if strategy_marker.strip() in SCORE_WEIGHTED_V2_CAPACITY_V1_IDS
        else SCORE_WEIGHTED_DEFAULTS
    )
    params = dict(defaults)
    topk = custom_params.get("topk") or _runtime_topk(manifest)
    n_drop = custom_params.get("n_drop") or _runtime_n_drop(manifest)
    if topk is None:
        raise StrategyPackageValidationError(
            "runtime profile selection.top_k is required for alpha-core StrategyPackage contracts",
            context={"package_id": manifest.package_id, "strategy_marker": strategy_marker},
        )
    params["topk"] = int(topk)
    params["n_drop"] = int(n_drop or 0)
    for key in defaults:
        if key in custom_params:
            params[key] = custom_params[key]
    params["strategy_family"] = strategy_family
    params["strategy_id"] = strategy_marker
    params["capacity_profile"] = _portfolio_capacity_profile(strategy_marker, strategy_family)
    params["topk"] = int(params["topk"])
    params["n_drop"] = int(params["n_drop"])
    params["max_n_drop"] = int(params["max_n_drop"])
    params["min_n_drop"] = int(params["min_n_drop"])
    params["hold_thresh"] = float(params.get("hold_thresh") or 0)
    return params


def _runtime_evidence(manifest: StrategyPackageManifest) -> dict[str, Any]:
    if manifest.backtest_context:
        return manifest.backtest_context
    strategy_config = manifest.strategy_config or {}
    custom_params = strategy_config.get("custom_params") if isinstance(strategy_config, dict) else None
    if not isinstance(custom_params, dict):
        custom_params = {}
    daily_strategy = {
        "strategy_id": strategy_config.get("strategy_id") if isinstance(strategy_config, dict) else None,
        "custom_params": custom_params,
    }
    if custom_params.get("topk") is not None:
        daily_strategy["topk"] = custom_params["topk"]
    elif manifest.portfolio_policy is not None:
        daily_strategy["topk"] = manifest.portfolio_policy.topk
    if custom_params.get("n_drop") is not None:
        daily_strategy["n_drop"] = custom_params["n_drop"]
    elif manifest.portfolio_policy is not None:
        daily_strategy["n_drop"] = manifest.portfolio_policy.n_drop
    return {
        "schema_version": "legacy_manifest_runtime_context_v1",
        "authority": "legacy_runtime_manifest_compatibility_only",
        "daily_strategy": daily_strategy,
        "selection_runtime": strategy_config.get("selection_runtime") if isinstance(strategy_config.get("selection_runtime"), dict) else {},
        "execution": {},
    }


def _runtime_topk(manifest: StrategyPackageManifest) -> int | None:
    daily_strategy = _runtime_evidence(manifest).get("daily_strategy")
    if isinstance(daily_strategy, dict) and daily_strategy.get("topk") is not None:
        return int(daily_strategy["topk"])
    if manifest.is_legacy_runtime_manifest and manifest.portfolio_policy is not None:
        return int(manifest.portfolio_policy.topk)
    return None


def _runtime_n_drop(manifest: StrategyPackageManifest) -> int | None:
    daily_strategy = _runtime_evidence(manifest).get("daily_strategy")
    if isinstance(daily_strategy, dict) and daily_strategy.get("n_drop") is not None:
        return int(daily_strategy["n_drop"])
    if manifest.is_legacy_runtime_manifest and manifest.portfolio_policy is not None:
        return int(manifest.portfolio_policy.n_drop)
    return None


def _portfolio_capacity_profile(strategy_marker: str, strategy_family: str) -> str | None:
    if strategy_marker.strip() in SCORE_WEIGHTED_V2_CAPACITY_V1_IDS:
        return "capacity_parameterized_v1"
    if strategy_family == "score_weighted_topk_v2":
        return "legacy_5m_cap"
    return None


def _platform_feature_contract(feature_name: str) -> dict[str, Any]:
    return {
        "enabled": False,
        "authority": "platform_runtime",
        "feature": feature_name,
        "package_bound": False,
    }


def _runtime_variant_contract(runtime_config: dict[str, Any] | None) -> dict[str, Any]:
    variant = (runtime_config or {}).get("runtime_variant")
    if not isinstance(variant, dict):
        return {"enabled": False, "package_bound": False}
    variant_config = variant.get("variant_config") if isinstance(variant.get("variant_config"), dict) else {}
    return {
        "enabled": True,
        "package_bound": False,
        "variant_id": variant.get("variant_id"),
        "variant_hash": variant.get("variant_hash"),
        "variant_kind": variant.get("variant_kind"),
        "paper_candidate": bool(variant.get("paper_candidate")),
        "validation_status": variant.get("validation_status"),
        "strategy_config_overlay": "strategy_config" in variant_config,
        "portfolio_policy_overlay": "portfolio_policy" in variant_config,
    }


def _industry_blacklist_contract(custom_params: dict[str, Any]) -> dict[str, Any]:
    values = custom_params.get("sector_blacklist") or custom_params.get("industry_blacklist") or []
    if isinstance(values, str):
        values = [item.strip() for item in values.replace(";", ",").split(",") if item.strip()]
    if not isinstance(values, list):
        values = []
    normalized: list[str] = []
    seen: set[str] = set()
    for item in values:
        text = str(item or "").strip()
        if text and text not in seen:
            normalized.append(text)
            seen.add(text)
    return {
        "enabled": bool(normalized or custom_params.get("sector_blacklist_enabled") or custom_params.get("blacklist_enabled")),
        "values": normalized,
    }


def _tradability_contract(custom_params: dict[str, Any]) -> dict[str, Any]:
    if "filter_suspended_on_signal" in custom_params:
        exclude_suspended = bool(custom_params.get("filter_suspended_on_signal"))
    elif "exclude_suspended" in custom_params:
        exclude_suspended = bool(custom_params.get("exclude_suspended"))
    else:
        # New QE configs force signal-side suspension filtering by default.
        exclude_suspended = True
    return {
        "exclude_suspended": exclude_suspended,
        "suspend_filter_file": custom_params.get("suspend_filter_file"),
        "suspend_filter_strict": bool(custom_params.get("suspend_filter_strict", True)),
    }


def _apply_backtest_context_defaults(
    manifest: StrategyPackageManifest,
    profile_payload: dict[str, Any],
    config: dict[str, Any],
) -> None:
    selection_payload = dict(profile_payload.get("selection") or {})
    daily_strategy = _runtime_evidence(manifest).get("daily_strategy")
    if not isinstance(daily_strategy, dict):
        daily_strategy = {}
    if selection_payload.get("top_k") is None and "top_k" not in config and daily_strategy.get("topk") is not None:
        selection_payload["top_k"] = daily_strategy.get("topk")
    if selection_payload.get("daily_strategy_id") is None:
        strategy_id = daily_strategy.get("strategy_id")
        if strategy_id:
            selection_payload["daily_strategy_id"] = strategy_id
    if not selection_payload.get("daily_strategy_params") and isinstance(daily_strategy.get("custom_params"), dict):
        selection_payload["daily_strategy_params"] = daily_strategy["custom_params"]
    profile_payload["selection"] = selection_payload


def _normalize_selection_profile(
    *,
    profile_payload: dict[str, Any],
    config: dict[str, Any],
    context: dict[str, Any],
) -> None:
    selection_payload = dict(profile_payload.get("selection") or {})
    if "top_k" in config and "top_k" not in selection_payload:
        selection_payload["top_k"] = config["top_k"]
    if selection_payload.get("top_k") is None:
        selection_payload.pop("top_k", None)
    if "top_k" in selection_payload:
        try:
            top_k = int(selection_payload["top_k"])
        except (TypeError, ValueError) as exc:
            raise StrategyPackageValidationError(
                "Paper v2 runtime top_k must be an integer",
                context={**context, "runtime_top_k": selection_payload["top_k"]},
            ) from exc
        if top_k <= 0 or top_k > 50:
            raise StrategyPackageValidationError(
                "Paper v2 runtime top_k must be between 1 and 50",
                context={**context, "runtime_top_k": top_k, "max_top_k": 50},
            )
        selection_payload["top_k"] = top_k
    profile_payload["selection"] = selection_payload


def _effective_manifest_for_contract(
    manifest: StrategyPackageManifest,
    runtime_config: dict[str, Any] | None = None,
) -> StrategyPackageManifest:
    variant = (runtime_config or {}).get("runtime_variant")
    if not isinstance(variant, dict):
        return manifest
    if variant.get("validation_status") != RuntimeVariantValidationStatus.VALIDATION_PASSED.value or not bool(
        variant.get("paper_candidate")
    ):
        raise StrategyPackageValidationError(
            "runtime variant must be a validated paper candidate before strategy contract use",
            context={
                "package_id": manifest.package_id,
                "runtime_variant_id": variant.get("variant_id"),
                "validation_status": variant.get("validation_status"),
                "paper_candidate": variant.get("paper_candidate"),
            },
        )
    if variant.get("manifest_sha256") != manifest.manifest_sha256:
        raise StrategyPackageValidationError(
            "runtime variant manifest hash does not match StrategyPackage manifest",
            context={
                "package_id": manifest.package_id,
                "runtime_variant_id": variant.get("variant_id"),
                "variant_manifest_sha256": variant.get("manifest_sha256"),
                "manifest_sha256": manifest.manifest_sha256,
            },
        )
    variant_config = variant.get("variant_config")
    if not isinstance(variant_config, dict):
        return manifest
    updates: dict[str, Any] = {}
    for key in ("strategy_config", "portfolio_policy"):
        if key in variant_config:
            updates[key] = _merge_model_or_dict(getattr(manifest, key), variant_config[key])
    if not updates:
        return manifest
    from backend.services.strategy_package.manifest import freeze_manifest

    return freeze_manifest(manifest.model_copy(update=updates))


def _merge_model_or_dict(current: Any, overlay: Any) -> Any:
    if hasattr(current, "model_copy"):
        if not isinstance(overlay, dict):
            raise StrategyPackageValidationError(
                "runtime variant model overlays must be objects",
                context={"overlay_type": type(overlay).__name__},
            )
        return current.model_copy(update=overlay)
    if isinstance(current, dict):
        if not isinstance(overlay, dict):
            raise StrategyPackageValidationError(
                "runtime variant dict overlays must be objects",
                context={"overlay_type": type(overlay).__name__},
            )
        return _deep_merge_dicts(current, overlay)
    return overlay


def _deep_merge_dicts(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge_dicts(merged[key], value)
        else:
            merged[key] = value
    return merged
