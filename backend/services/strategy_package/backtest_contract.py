"""Backtest runtime contract helpers shared by QE packages and Paper v2.

Paper v2 must execute the same strategy contract that QE backtested.  The
StrategyPackage v1 manifest kept most QE configuration in ``strategy_config``;
these helpers normalize that legacy payload into explicit checks used by Paper.
"""

from __future__ import annotations

from typing import Any

from backend.services.selection_center.runtime_profile import (
    SelectionRuntimeProfile,
    normalize_selection_runtime_config,
    parse_selection_runtime_profile,
)
from backend.services.strategy_package.execution_policy import normalize_execution_policy_json
from backend.services.strategy_package.models import StrategyPackageManifest
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


def build_backtest_runtime_contract(manifest: StrategyPackageManifest) -> dict[str, Any]:
    """Normalize the frozen QE runtime configuration embedded in a package."""

    strategy_config = manifest.strategy_config or {}
    custom_params = strategy_config.get("custom_params")
    if not isinstance(custom_params, dict):
        custom_params = {}
    strategy_marker = str(
        custom_params.get("strategy_class")
        or custom_params.get("strategy_id")
        or strategy_config.get("strategy_id")
        or ""
    ).strip()
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
            "hmm": _hmm_contract(custom_params),
            "industry_blacklist": _industry_blacklist_contract(custom_params),
            "tradability": _tradability_contract(custom_params),
            "risk_policy": _risk_policy_contract(custom_params),
            "event_signal_policy": _event_signal_policy_contract(custom_params),
        },
        "minute_execution_policy": manifest.minute_execution_policy.model_dump(mode="json"),
    }


def validate_execution_policy_matches_manifest(
    manifest: StrategyPackageManifest,
    policy_json: dict[str, Any],
    *,
    context: dict[str, Any] | None = None,
) -> None:
    """Reject Paper-only execution policy changes.

    A policy can enter Paper only when it is the same minute execution contract
    that QE backtested for this StrategyPackage.  A catalog-level validation for
    an algorithm is not sufficient.
    """

    expected = normalize_execution_policy_json(manifest.minute_execution_policy.model_dump(mode="json"))
    actual = normalize_execution_policy_json(policy_json)
    if actual != expected:
        raise StrategyPackageValidationError(
            "paper v2 execution policy must match the QE backtest minute execution contract",
            context={
                **(context or {}),
                "package_id": manifest.package_id,
                "expected_algo_code": expected.get("algo_code"),
                "actual_algo_code": actual.get("algo_code"),
                "expected_policy": expected,
                "actual_policy": actual,
            },
        )


def validate_runtime_profile_matches_backtest_contract(
    manifest: StrategyPackageManifest,
    runtime_profile: SelectionRuntimeProfile,
    *,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Reject runtime-profile options that were not in the QE backtest config."""

    contract = build_backtest_runtime_contract(manifest)
    features = contract["runtime_features"]
    params = contract["portfolio_strategy"]["params"]
    topk = int(params["topk"])
    requested_topk = runtime_profile.selection.top_k
    if requested_topk is not None and int(requested_topk) != topk:
        raise StrategyPackageValidationError(
            "paper v2 runtime top_k must match the QE backtest portfolio strategy",
            context={
                **(context or {}),
                "package_id": manifest.package_id,
                "qe_topk": topk,
                "runtime_top_k": requested_topk,
            },
        )

    tradability_contract = features["tradability"]
    if bool(runtime_profile.tradability.exclude_suspended) != bool(tradability_contract["exclude_suspended"]):
        raise StrategyPackageValidationError(
            "Paper v2 tradability.exclude_suspended must match the QE backtest contract",
            context={
                **(context or {}),
                "package_id": manifest.package_id,
                "qe_exclude_suspended": tradability_contract["exclude_suspended"],
                "runtime_exclude_suspended": runtime_profile.tradability.exclude_suspended,
            },
        )

    hmm_contract = features["hmm"]
    if hmm_contract["enabled"]:
        if not runtime_profile.hmm.enabled:
            raise StrategyPackageValidationError(
                "QE backtest enabled HMM but Paper v2 runtime profile did not enable HMM",
                context={**(context or {}), "package_id": manifest.package_id, "hmm_contract": hmm_contract},
            )
        expected_preset = hmm_contract.get("signal_preset")
        if expected_preset and runtime_profile.hmm.signal_preset != expected_preset:
            raise StrategyPackageValidationError(
                "Paper v2 HMM signal_preset must match the QE backtest HMM contract",
                context={
                    **(context or {}),
                    "package_id": manifest.package_id,
                    "qe_hmm_signal_preset": expected_preset,
                    "runtime_hmm_signal_preset": runtime_profile.hmm.signal_preset,
                },
            )
        expected_snapshot = hmm_contract.get("model_snapshot_id") or hmm_contract.get("model_version_id")
        if expected_snapshot and runtime_profile.hmm.model_snapshot_id != expected_snapshot:
            raise StrategyPackageValidationError(
                "Paper v2 HMM model_snapshot_id must match the QE backtest HMM contract",
                context={
                    **(context or {}),
                    "package_id": manifest.package_id,
                    "qe_hmm_model_snapshot_id": expected_snapshot,
                    "runtime_hmm_model_snapshot_id": runtime_profile.hmm.model_snapshot_id,
                },
            )
        expected_coefficients = hmm_contract.get("coefficients_file")
        if expected_coefficients and runtime_profile.hmm.coefficients_path != expected_coefficients:
            raise StrategyPackageValidationError(
                "Paper v2 HMM coefficients_path must match the QE backtest HMM contract",
                context={
                    **(context or {}),
                    "package_id": manifest.package_id,
                    "qe_hmm_coefficients_path": expected_coefficients,
                    "runtime_hmm_coefficients_path": runtime_profile.hmm.coefficients_path,
                },
            )
    elif runtime_profile.hmm.enabled:
        raise StrategyPackageValidationError(
            "Paper v2 cannot enable HMM because the QE backtest contract did not enable HMM",
            context={**(context or {}), "package_id": manifest.package_id},
        )

    expected_blacklist = features["industry_blacklist"]["values"]
    runtime_blacklist = runtime_profile.industry_blacklist
    if runtime_blacklist != expected_blacklist:
        raise StrategyPackageValidationError(
            "Paper v2 industry blacklist must match the QE backtest contract",
            context={
                **(context or {}),
                "package_id": manifest.package_id,
                "qe_industry_blacklist": expected_blacklist,
                "runtime_industry_blacklist": runtime_blacklist,
            },
        )

    risk_contract = features["risk_policy"]
    event_signal_contract = features.get("event_signal_policy", {"enabled": False, "policy": {}})
    event_signal_enabled = bool(event_signal_contract["enabled"])
    if risk_contract["enabled"] and not runtime_profile.risk_policy.enabled:
        raise StrategyPackageValidationError(
            "QE backtest enabled risk_policy but Paper v2 runtime profile did not enable it",
            context={**(context or {}), "package_id": manifest.package_id, "risk_policy_contract": risk_contract},
        )
    if not risk_contract["enabled"] and not event_signal_enabled and runtime_profile.risk_policy.enabled:
        raise StrategyPackageValidationError(
            "Paper v2 cannot enable risk_policy because the QE backtest contract did not enable it",
            context={**(context or {}), "package_id": manifest.package_id},
        )
    _validate_event_signal_policy_contract(
        runtime_profile.risk_policy.model_dump(mode="json"),
        event_signal_contract,
        context={**(context or {}), "package_id": manifest.package_id},
    )
    return contract


def _validate_event_signal_policy_contract(
    risk_policy: dict[str, Any],
    contract: dict[str, Any],
    *,
    context: dict[str, Any],
) -> None:
    providers = risk_policy.get("providers") or []
    runtime_enabled = "event_signal_policy" in providers
    if runtime_enabled and not contract["enabled"]:
        raise StrategyPackageValidationError(
            "Paper v2 cannot enable event_signal_policy because the QE backtest contract did not enable it",
            context={**context, "event_signal_policy_contract": contract},
        )
    if not contract["enabled"]:
        return
    policy = dict(contract.get("policy") or {})
    expected_profile_id = policy.get("event_signal_profile_id")
    expected_asof_policy = policy.get("asof_policy") or "effective_trade_date"
    expected_merge_policy = policy.get("signal_merge_policy") or "block_first"
    if not runtime_enabled:
        raise StrategyPackageValidationError(
            "QE backtest enabled event_signal_policy but Paper v2 runtime profile did not enable it",
            context={**context, "event_signal_policy_contract": contract},
        )
    checks = {
        "event_signal_profile_id": expected_profile_id,
        "event_signal_asof_policy": expected_asof_policy,
        "event_signal_merge_policy": expected_merge_policy,
    }
    for key, expected in checks.items():
        actual = risk_policy.get(key)
        if actual != expected:
            raise StrategyPackageValidationError(
                f"Paper v2 {key} must match the QE backtest event_signal_policy contract",
                context={**context, f"expected_{key}": expected, f"runtime_{key}": actual},
            )


def normalize_runtime_config_with_backtest_contract(
    manifest: StrategyPackageManifest,
    runtime_config: dict[str, Any] | None,
    *,
    context: dict[str, Any] | None = None,
    include_contract: bool = False,
) -> dict[str, Any]:
    """Apply the frozen QE runtime contract to a Paper v2 runtime config.

    Paper may still carry score-production or session options, but portfolio
    strategy, HMM, blacklist, suspension filtering, and risk switches must be
    inherited from the backtest contract unless the caller supplies the same
    values explicitly.  Conflicting runtime/UI values fail before execution.
    """

    if runtime_config is not None and not isinstance(runtime_config, dict):
        raise StrategyPackageValidationError(
            "runtime_config must be an object",
            context={**(context or {}), "runtime_config_type": type(runtime_config).__name__},
        )
    contract = build_backtest_runtime_contract(manifest)
    config = dict(runtime_config or {})
    raw_profile = config.get("runtime_profile")
    if raw_profile is not None and not isinstance(raw_profile, dict):
        raise StrategyPackageValidationError(
            "runtime_config.runtime_profile must be an object",
            context={**(context or {}), "runtime_profile_type": type(raw_profile).__name__},
        )
    profile_payload: dict[str, Any] = dict(raw_profile or {})

    strategy_params = contract["portfolio_strategy"]["params"]
    _apply_selection_contract(
        profile_payload=profile_payload,
        config=config,
        expected_topk=int(strategy_params["topk"]),
        context={**(context or {}), "package_id": manifest.package_id},
    )
    _apply_tradability_contract(
        profile_payload=profile_payload,
        config=config,
        contract=contract["runtime_features"]["tradability"],
        context={**(context or {}), "package_id": manifest.package_id},
    )
    _apply_hmm_contract(
        profile_payload=profile_payload,
        config=config,
        contract=contract["runtime_features"]["hmm"],
        context={**(context or {}), "package_id": manifest.package_id},
    )
    _apply_industry_blacklist_contract(
        profile_payload=profile_payload,
        config=config,
        contract=contract["runtime_features"]["industry_blacklist"],
        context={**(context or {}), "package_id": manifest.package_id},
    )
    _apply_risk_policy_contract(
        profile_payload=profile_payload,
        config=config,
        contract=contract["runtime_features"]["risk_policy"],
        event_signal_contract=contract["runtime_features"]["event_signal_policy"],
        context={**(context or {}), "package_id": manifest.package_id},
    )

    config["runtime_profile"] = profile_payload
    normalized = normalize_selection_runtime_config(config)
    runtime_profile = parse_selection_runtime_profile(normalized)
    validate_runtime_profile_matches_backtest_contract(
        manifest,
        runtime_profile,
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
    params["topk"] = int(custom_params.get("topk") or manifest.portfolio_policy.topk)
    params["n_drop"] = int(custom_params.get("n_drop") or manifest.portfolio_policy.n_drop)
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


def _portfolio_capacity_profile(strategy_marker: str, strategy_family: str) -> str | None:
    if strategy_marker.strip() in SCORE_WEIGHTED_V2_CAPACITY_V1_IDS:
        return "capacity_parameterized_v1"
    if strategy_family == "score_weighted_topk_v2":
        return "legacy_5m_cap"
    return None


def _hmm_contract(custom_params: dict[str, Any]) -> dict[str, Any]:
    return {
        "enabled": bool(custom_params.get("enable_sector_hmm", False)),
        "model_snapshot_id": custom_params.get("hmm_model_snapshot_id") or custom_params.get("hmm_model_version_id"),
        "signal_preset": custom_params.get("hmm_signal_preset"),
        "coefficients_file": custom_params.get("hmm_coefficients_file"),
        "model_version_id": custom_params.get("hmm_model_version_id"),
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


def _risk_policy_contract(custom_params: dict[str, Any]) -> dict[str, Any]:
    policy = custom_params.get("risk_policy")
    if not isinstance(policy, dict):
        return {"enabled": False, "policy": {}}
    return {"enabled": bool(policy.get("enabled", False)), "policy": policy}


def _event_signal_policy_contract(custom_params: dict[str, Any]) -> dict[str, Any]:
    policy = custom_params.get("event_signal_policy")
    if not isinstance(policy, dict):
        return {"enabled": False, "policy": {}}
    return {"enabled": bool(policy.get("enabled", False)), "policy": policy}


def _apply_selection_contract(
    *,
    profile_payload: dict[str, Any],
    config: dict[str, Any],
    expected_topk: int,
    context: dict[str, Any],
) -> None:
    selection_payload = dict(profile_payload.get("selection") or {})
    if "top_k" in config and "top_k" not in selection_payload:
        selection_payload["top_k"] = config["top_k"]
    if "top_k" in selection_payload and int(selection_payload["top_k"]) != expected_topk:
        raise StrategyPackageValidationError(
            "Paper v2 runtime top_k must match the QE backtest portfolio strategy",
            context={**context, "qe_topk": expected_topk, "runtime_top_k": selection_payload["top_k"]},
        )
    selection_payload["top_k"] = expected_topk
    profile_payload["selection"] = selection_payload


def _apply_tradability_contract(
    *,
    profile_payload: dict[str, Any],
    config: dict[str, Any],
    contract: dict[str, Any],
    context: dict[str, Any],
) -> None:
    tradability_payload = dict(profile_payload.get("tradability") or {})
    if "exclude_suspended" in config and "exclude_suspended" not in tradability_payload:
        tradability_payload["exclude_suspended"] = config["exclude_suspended"]
    expected = bool(contract["exclude_suspended"])
    if "exclude_suspended" in tradability_payload and bool(tradability_payload["exclude_suspended"]) != expected:
        raise StrategyPackageValidationError(
            "Paper v2 tradability.exclude_suspended must match the QE backtest contract",
            context={
                **context,
                "qe_exclude_suspended": expected,
                "runtime_exclude_suspended": tradability_payload["exclude_suspended"],
            },
        )
    tradability_payload["exclude_suspended"] = expected
    profile_payload["tradability"] = tradability_payload


def _apply_hmm_contract(
    *,
    profile_payload: dict[str, Any],
    config: dict[str, Any],
    contract: dict[str, Any],
    context: dict[str, Any],
) -> None:
    hmm_payload = dict(profile_payload.get("hmm") or {})
    if "hmm" in config:
        if not isinstance(config["hmm"], dict):
            raise StrategyPackageValidationError("runtime_config.hmm must be an object", context=context)
        merged = dict(config["hmm"])
        merged.update(hmm_payload)
        hmm_payload = merged
    legacy_map = {
        "enable_sector_hmm": "enabled",
        "hmm_model_snapshot_id": "model_snapshot_id",
        "hmm_model_version_id": "model_snapshot_id",
        "hmm_signal_preset": "signal_preset",
        "hmm_coefficients_path": "coefficients_path",
        "hmm_coefficients_file": "coefficients_path",
    }
    for legacy_key, profile_key in legacy_map.items():
        if legacy_key in config and profile_key not in hmm_payload:
            hmm_payload[profile_key] = config[legacy_key]

    expected_enabled = bool(contract["enabled"])
    if "enabled" in hmm_payload and bool(hmm_payload["enabled"]) != expected_enabled:
        raise StrategyPackageValidationError(
            "Paper v2 HMM enabled flag must match the QE backtest contract",
            context={**context, "qe_hmm_enabled": expected_enabled, "runtime_hmm_enabled": hmm_payload["enabled"]},
        )
    if not expected_enabled:
        extra = {
            key: value
            for key, value in hmm_payload.items()
            if key != "enabled" and value not in (None, "", False)
        }
        if extra:
            raise StrategyPackageValidationError(
                "Paper v2 cannot carry HMM runtime fields when the QE backtest contract did not enable HMM",
                context={**context, "runtime_hmm_fields": extra},
            )
        hmm_payload["enabled"] = False
        profile_payload["hmm"] = hmm_payload
        return

    expected_snapshot = contract.get("model_snapshot_id") or contract.get("model_version_id")
    expected_preset = contract.get("signal_preset")
    expected_coefficients = contract.get("coefficients_file")
    missing = [
        key
        for key, value in {
            "model_snapshot_id": expected_snapshot,
            "signal_preset": expected_preset,
        }.items()
        if not value
    ]
    if missing:
        raise StrategyPackageValidationError(
            "QE HMM backtest contract is missing fields required by Paper v2 runtime",
            context={**context, "missing_contract_fields": missing, "hmm_contract": contract},
        )
    _set_or_match(hmm_payload, "enabled", True, "Paper v2 HMM enabled flag must match the QE backtest contract", context)
    _set_or_match(
        hmm_payload,
        "model_snapshot_id",
        expected_snapshot,
        "Paper v2 HMM model_snapshot_id must match the QE backtest contract",
        context,
    )
    _set_or_match(
        hmm_payload,
        "signal_preset",
        expected_preset,
        "Paper v2 HMM signal_preset must match the QE backtest contract",
        context,
    )
    if expected_coefficients:
        _set_or_match(
            hmm_payload,
            "coefficients_path",
            expected_coefficients,
            "Paper v2 HMM coefficients_path must match the QE backtest contract",
            context,
        )
    profile_payload["hmm"] = hmm_payload


def _apply_industry_blacklist_contract(
    *,
    profile_payload: dict[str, Any],
    config: dict[str, Any],
    contract: dict[str, Any],
    context: dict[str, Any],
) -> None:
    expected = list(contract["values"])
    value_set = "industry_blacklist" in profile_payload
    runtime_values = profile_payload.get("industry_blacklist")
    if runtime_values is None and "industry_blacklist" in config:
        runtime_values = config["industry_blacklist"]
        value_set = True
    if runtime_values is None and "sector_blacklist" in config:
        runtime_values = config["sector_blacklist"]
        value_set = True
    if runtime_values is not None and not isinstance(runtime_values, list):
        raise StrategyPackageValidationError(
            "runtime industry_blacklist must be a list",
            context={**context, "runtime_industry_blacklist": runtime_values},
        )
    normalized = [str(item).strip() for item in (runtime_values or []) if str(item).strip()]
    if value_set and normalized != expected:
        raise StrategyPackageValidationError(
            "Paper v2 industry blacklist must match the QE backtest contract",
            context={**context, "qe_industry_blacklist": expected, "runtime_industry_blacklist": normalized},
        )
    profile_payload["industry_blacklist"] = expected


def _apply_risk_policy_contract(
    *,
    profile_payload: dict[str, Any],
    config: dict[str, Any],
    contract: dict[str, Any],
    event_signal_contract: dict[str, Any],
    context: dict[str, Any],
) -> None:
    risk_payload = dict(profile_payload.get("risk_policy") or {})
    if "risk_policy" in config:
        if not isinstance(config["risk_policy"], dict):
            raise StrategyPackageValidationError("runtime_config.risk_policy must be an object", context=context)
        merged = dict(config["risk_policy"])
        merged.update(risk_payload)
        risk_payload = merged
    _apply_event_signal_policy_contract(risk_payload=risk_payload, contract=event_signal_contract, context=context)

    expected_enabled = bool(contract["enabled"] or event_signal_contract["enabled"])
    if "enabled" in risk_payload and bool(risk_payload["enabled"]) != expected_enabled:
        raise StrategyPackageValidationError(
            "Paper v2 risk_policy enabled flag must match the QE backtest contract",
            context={
                **context,
                "qe_risk_policy_enabled": expected_enabled,
                "runtime_risk_policy_enabled": risk_payload["enabled"],
            },
        )
    if contract["enabled"]:
        expected_policy = dict(contract.get("policy") or {})
        expected_policy["enabled"] = True
        expected_policy.update({key: value for key, value in risk_payload.items() if key not in expected_policy})
        risk_payload = expected_policy
    else:
        risk_payload["enabled"] = expected_enabled
    profile_payload["risk_policy"] = risk_payload


def _apply_event_signal_policy_contract(
    *,
    risk_payload: dict[str, Any],
    contract: dict[str, Any],
    context: dict[str, Any],
) -> None:
    providers = list(risk_payload.get("providers") or ["st_pit"])
    has_runtime_provider = "event_signal_policy" in providers
    if has_runtime_provider and not contract["enabled"]:
        raise StrategyPackageValidationError(
            "Paper v2 cannot enable event_signal_policy because the QE backtest contract did not enable it",
            context={**context, "event_signal_policy_contract": contract},
        )
    if not contract["enabled"]:
        return

    policy = dict(contract.get("policy") or {})
    expected_profile_id = policy.get("event_signal_profile_id")
    expected_asof_policy = policy.get("asof_policy") or "effective_trade_date"
    expected_merge_policy = policy.get("signal_merge_policy") or "block_first"
    missing = [
        key
        for key, value in {
            "event_signal_profile_id": expected_profile_id,
            "asof_policy": expected_asof_policy,
            "signal_merge_policy": expected_merge_policy,
        }.items()
        if not value
    ]
    if missing:
        raise StrategyPackageValidationError(
            "QE event_signal_policy contract is missing fields required by Paper v2 runtime",
            context={**context, "missing_contract_fields": missing, "event_signal_policy_contract": contract},
        )
    if "event_signal_policy" not in providers:
        providers.append("event_signal_policy")
    risk_payload["providers"] = providers
    _set_or_match(
        risk_payload,
        "event_signal_profile_id",
        expected_profile_id,
        "Paper v2 event_signal_profile_id must match the QE backtest event_signal_policy contract",
        context,
    )
    _set_or_match(
        risk_payload,
        "event_signal_asof_policy",
        expected_asof_policy,
        "Paper v2 event_signal_asof_policy must match the QE backtest event_signal_policy contract",
        context,
    )
    _set_or_match(
        risk_payload,
        "event_signal_merge_policy",
        expected_merge_policy,
        "Paper v2 event_signal_merge_policy must match the QE backtest event_signal_policy contract",
        context,
    )


def _set_or_match(
    payload: dict[str, Any],
    key: str,
    expected: Any,
    message: str,
    context: dict[str, Any],
) -> None:
    if key in payload and payload[key] not in (None, "") and payload[key] != expected:
        raise StrategyPackageValidationError(
            message,
            context={**context, f"expected_{key}": expected, f"runtime_{key}": payload[key]},
        )
    payload[key] = expected
