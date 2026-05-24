"""Runtime profile parsing for package-based selection and Paper v2.

The runtime profile contains mutable operational choices that must not mutate
the frozen StrategyPackage manifest. Unknown keys are rejected inside the
profile itself; score-production keys remain owned by StrategyPackageRuntime
and must resolve to authoritative live/latest-data selection artifacts.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from backend.services.trading_core.errors import StrategyPackageValidationError


RUNTIME_CONFIG_SCOPE_KEY = "runtime_config_scope"
RUNTIME_PROFILE_BINDING_KEY = "runtime_profile_binding"
RUNTIME_PROFILE_ACTIVATION_KEY = "runtime_profile_activation"
DEFAULT_RUNTIME_PROFILE_VERSION_ID = "platform_default_runtime_profile_v1"
NON_TRADING_RUNTIME_SCOPES = frozenset({"diagnostic_preview", "non_trading_preview", "read_only_preview"})
TRADING_RUNTIME_SCOPES = frozenset({"trading", "paper_trading", "miniqmt_sim", "live_trading"})
RUNTIME_PROFILE_BINDING_SOURCES = frozenset(
    {
        "platform_default",
        "runtime_profile_version",
        "selection_runtime_profile_version",
        "paper_runtime_profile_version",
        "runtime_config_activation",
        "package_runtime_release",
        "portfolio_binding_version",
        "ad_hoc_non_trading_preview",
    }
)

# These caller-provided keys can change selection, targets, orders, holdings or
# NAV. Trading paths must therefore use a versioned runtime profile activation
# (or another explicit binding carrying a version/hash) instead of ad-hoc input.
BEHAVIOR_CHANGING_RUNTIME_CONFIG_KEYS = frozenset(
    {
        "runtime_profile",
        "runtime_variant_id",
        "top_k",
        "exclude_suspended",
        "industry_blacklist",
        "sector_blacklist",
        "hmm",
        "hmm_config_id",
        "hmm_model_config_id",
        "enable_sector_hmm",
        "hmm_model_snapshot_id",
        "hmm_model_version_id",
        "hmm_signal_preset",
        "hmm_coefficients_path",
        "hmm_coefficients_file",
        "risk_policy",
        "package_weights",
        "st_pit_authoritative",
        "enforce_st_pit_contract",
        "display_top_n",
        "total_equity",
    }
)

_PROFILE_HASH_EXCLUDED_KEYS = frozenset(
    {
        RUNTIME_CONFIG_SCOPE_KEY,
        RUNTIME_PROFILE_BINDING_KEY,
        RUNTIME_PROFILE_ACTIVATION_KEY,
        "paper_v2_session",
        "paper_v2_replay",
        "selection_source",
        "point_in_time_context",
        "package_runtime_configs",
        "package_health",
        "validated_execution_policy",
        "qe_backtest_runtime_contract",
        "current_prices",
        "current_price_context",
        "valid_no_candidate",
        "no_candidate_reason",
        "_miniqmt_current_positions",
        "_miniqmt_latest_cash",
        "_miniqmt_total_equity",
    }
)


class RuntimeHMMProfile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    model_config_id: str | None = None
    model_snapshot_id: str | None = None
    signal_preset: str | None = None
    coefficients_path: str | None = None

    @field_validator("model_config_id", "model_snapshot_id", "signal_preset", "coefficients_path")
    @classmethod
    def _strip_optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = str(value).strip()
        return value or None


class RuntimeTradabilityProfile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    exclude_suspended: bool = True


class RuntimeSelectionProfile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    top_k: int | None = Field(default=None, gt=0, le=50)
    daily_strategy_id: str | None = None
    daily_strategy_params: dict[str, Any] = Field(default_factory=dict)

    @field_validator("daily_strategy_id")
    @classmethod
    def _strip_daily_strategy_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None


class RuntimeRiskScoreOverlayProfile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    negative_multiplier_floor: float = Field(default=0.70, gt=0, le=1)
    positive_multiplier_cap: float = Field(default=1.10, ge=1)


class RuntimeRiskPolicyProfile(BaseModel):
    """Platform risk policy profile shared by Selection Center and Paper v2.

    The current provider is ST PIT. Future event-signal consumption must be
    expressed by this platform profile instead of StrategyPackage runtime_config.
    """

    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    policy_version: str = "platform_risk_policy_v1"
    providers: list[Literal["st_pit", "announcement_risk", "event_signal_policy"]] = Field(default_factory=lambda: ["st_pit"])
    st_universe_key: str = "shsz_st_pit_active_v1"
    event_signal_profile_id: str | None = None
    event_signal_asof_policy: Literal["disabled", "effective_trade_date"] = "disabled"
    event_signal_merge_policy: Literal["disabled", "block_first"] = "disabled"
    hard_actions: list[Literal["block_buy", "force_exit"]] = Field(
        default_factory=lambda: ["block_buy", "force_exit"]
    )
    visible_time_mode: Literal["next_trading_session", "trade_date"] = "next_trading_session"
    strict_data_ready: bool = True
    score_overlay: RuntimeRiskScoreOverlayProfile = Field(default_factory=RuntimeRiskScoreOverlayProfile)

    @field_validator("policy_version", "st_universe_key", "event_signal_profile_id")
    @classmethod
    def _strip_optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = str(value or "").strip()
        if not value:
            raise ValueError("risk policy text fields cannot be empty")
        return value

    @field_validator("providers", "hard_actions")
    @classmethod
    def _dedupe_text_list(cls, value: list[Any]) -> list[Any]:
        normalized: list[Any] = []
        seen: set[str] = set()
        for item in value or []:
            text = str(item or "").strip()
            if not text or text in seen:
                continue
            normalized.append(text)
            seen.add(text)
        return normalized


class SelectionRuntimeProfile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    industry_blacklist: list[str] = Field(default_factory=list)
    hmm: RuntimeHMMProfile = Field(default_factory=RuntimeHMMProfile)
    tradability: RuntimeTradabilityProfile = Field(default_factory=RuntimeTradabilityProfile)
    selection: RuntimeSelectionProfile = Field(default_factory=RuntimeSelectionProfile)
    risk_policy: RuntimeRiskPolicyProfile = Field(default_factory=RuntimeRiskPolicyProfile)

    @field_validator("industry_blacklist")
    @classmethod
    def _normalize_industry_blacklist(cls, value: list[Any]) -> list[str]:
        normalized: list[str] = []
        seen: set[str] = set()
        for item in value or []:
            text = str(item or "").strip()
            if not text or text in seen:
                continue
            normalized.append(text)
            seen.add(text)
        return normalized


def parse_selection_runtime_profile(runtime_config: dict[str, Any] | None) -> SelectionRuntimeProfile:
    """Parse the runtime profile with legacy key compatibility.

    Legacy top-level keys are accepted so existing tests and API callers can
    continue to pass ``top_k`` and ``exclude_suspended`` while the normalized
    payload stored on runs always includes ``runtime_profile``.
    """

    config = runtime_config or {}
    raw_profile = config.get("runtime_profile")
    if raw_profile is not None and not isinstance(raw_profile, dict):
        raise StrategyPackageValidationError(
            "runtime_config.runtime_profile must be an object",
            context={"runtime_profile_type": type(raw_profile).__name__},
        )

    payload: dict[str, Any] = dict(raw_profile or {})

    if "industry_blacklist" not in payload:
        blacklist = config.get("industry_blacklist", config.get("sector_blacklist"))
        if blacklist is not None:
            payload["industry_blacklist"] = blacklist

    tradability_payload = dict(payload.get("tradability") or {})
    if "exclude_suspended" in config and "exclude_suspended" not in tradability_payload:
        tradability_payload["exclude_suspended"] = config["exclude_suspended"]
    payload["tradability"] = tradability_payload

    selection_payload = dict(payload.get("selection") or {})
    if "top_k" in config and "top_k" not in selection_payload:
        selection_payload["top_k"] = config["top_k"]
    if "daily_strategy_id" in config and "daily_strategy_id" not in selection_payload:
        selection_payload["daily_strategy_id"] = config["daily_strategy_id"]
    if "daily_strategy_params" in config and "daily_strategy_params" not in selection_payload:
        selection_payload["daily_strategy_params"] = config["daily_strategy_params"]
    payload["selection"] = selection_payload

    hmm_payload = dict(payload.get("hmm") or {})
    if "hmm" in config:
        if not isinstance(config["hmm"], dict):
            raise StrategyPackageValidationError("runtime_config.hmm must be an object")
        merged = dict(config["hmm"])
        merged.update(hmm_payload)
        hmm_payload = merged
    if "enable_sector_hmm" in config and "enabled" not in hmm_payload:
        hmm_payload["enabled"] = bool(config["enable_sector_hmm"])
    if "hmm_config_id" in config and "model_config_id" not in hmm_payload:
        hmm_payload["model_config_id"] = config["hmm_config_id"]
    if "hmm_model_config_id" in config and "model_config_id" not in hmm_payload:
        hmm_payload["model_config_id"] = config["hmm_model_config_id"]
    if "hmm_model_snapshot_id" in config and "model_snapshot_id" not in hmm_payload:
        hmm_payload["model_snapshot_id"] = config["hmm_model_snapshot_id"]
    if "hmm_model_version_id" in config and "model_snapshot_id" not in hmm_payload:
        hmm_payload["model_snapshot_id"] = config["hmm_model_version_id"]
    if "hmm_signal_preset" in config and "signal_preset" not in hmm_payload:
        hmm_payload["signal_preset"] = config["hmm_signal_preset"]
    if "hmm_coefficients_path" in config and "coefficients_path" not in hmm_payload:
        hmm_payload["coefficients_path"] = config["hmm_coefficients_path"]
    if "hmm_coefficients_file" in config and "coefficients_path" not in hmm_payload:
        hmm_payload["coefficients_path"] = config["hmm_coefficients_file"]
    payload["hmm"] = hmm_payload

    risk_policy_payload = dict(payload.get("risk_policy") or {})
    if "risk_policy" in config:
        if not isinstance(config["risk_policy"], dict):
            raise StrategyPackageValidationError("runtime_config.risk_policy must be an object")
        merged = dict(config["risk_policy"])
        merged.update(risk_policy_payload)
        risk_policy_payload = merged
    payload["risk_policy"] = risk_policy_payload

    try:
        profile = SelectionRuntimeProfile.model_validate(payload)
    except ValueError as exc:
        raise StrategyPackageValidationError(
            "runtime profile validation failed",
            context={"error": str(exc), "runtime_profile": payload},
        ) from exc

    if profile.hmm.enabled:
        if not profile.hmm.model_snapshot_id and not profile.hmm.model_config_id:
            raise StrategyPackageValidationError(
                "HMM runtime profile requires model_snapshot_id or model_config_id when enabled",
                context={"runtime_profile": profile.model_dump(mode="json")},
            )
        if not profile.hmm.signal_preset:
            raise StrategyPackageValidationError(
                "HMM runtime profile requires signal_preset when enabled",
                context={"runtime_profile": profile.model_dump(mode="json")},
            )
    if profile.risk_policy.enabled and not profile.risk_policy.providers:
        raise StrategyPackageValidationError(
            "risk policy requires at least one provider when enabled",
            context={"runtime_profile": profile.model_dump(mode="json")},
        )
    if "announcement_risk" in profile.risk_policy.providers and profile.risk_policy.enabled:
        raise StrategyPackageValidationError(
            "announcement_risk provider is not implemented yet; use platform event_signal_policy when ready",
            context={"runtime_profile": profile.model_dump(mode="json")},
        )
    if "event_signal_policy" in profile.risk_policy.providers and profile.risk_policy.enabled:
        if not profile.risk_policy.event_signal_profile_id:
            raise StrategyPackageValidationError(
                "event_signal_policy provider requires event_signal_profile_id when enabled",
                context={"runtime_profile": profile.model_dump(mode="json")},
            )
        if (
            profile.risk_policy.event_signal_asof_policy == "disabled"
            or profile.risk_policy.event_signal_merge_policy == "disabled"
        ):
            raise StrategyPackageValidationError(
                "event_signal_policy provider requires enabled as-of and merge policies",
                context={"runtime_profile": profile.model_dump(mode="json")},
            )
    return profile


def normalize_selection_runtime_config(runtime_config: dict[str, Any] | None) -> dict[str, Any]:
    config = dict(runtime_config or {})
    profile = parse_selection_runtime_profile(config)
    config["runtime_profile"] = profile.model_dump(mode="json")
    return config


def runtime_behavior_keys(runtime_config: dict[str, Any] | None) -> list[str]:
    """Return behavior-changing keys provided by the caller.

    The check is intentionally shallow at the top-level plus per-package dicts:
    those are the API shapes accepted by Selection Center and Paper v2. Generated
    runtime metadata is excluded by checking the raw caller config before later
    normalization adds a default ``runtime_profile``.
    """

    config = runtime_config or {}
    if not isinstance(config, dict):
        return []
    keys: set[str] = set()
    for key, value in config.items():
        text_key = str(key)
        if text_key in BEHAVIOR_CHANGING_RUNTIME_CONFIG_KEYS:
            keys.add(text_key)
        if isinstance(value, dict):
            nested = set(str(nested_key) for nested_key in value)
            if nested.intersection(BEHAVIOR_CHANGING_RUNTIME_CONFIG_KEYS):
                keys.add(f"{text_key}.*")
    return sorted(keys)


def runtime_config_has_version_binding(runtime_config: dict[str, Any] | None) -> bool:
    config = runtime_config or {}
    if not isinstance(config, dict):
        return False
    activation = config.get(RUNTIME_PROFILE_ACTIVATION_KEY)
    if isinstance(activation, dict) and activation.get("profile_version_id") and activation.get("config_sha256"):
        return True
    binding = config.get(RUNTIME_PROFILE_BINDING_KEY)
    return bool(
        isinstance(binding, dict)
        and binding.get("profile_version_id")
        and binding.get("config_sha256")
        and binding.get("source")
    )


def runtime_config_scope(runtime_config: dict[str, Any] | None) -> str | None:
    raw = (runtime_config or {}).get(RUNTIME_CONFIG_SCOPE_KEY) if isinstance(runtime_config or {}, dict) else None
    if raw is None:
        return None
    scope = str(raw).strip()
    return scope or None


def is_non_trading_runtime_config(runtime_config: dict[str, Any] | None) -> bool:
    return runtime_config_scope(runtime_config) in NON_TRADING_RUNTIME_SCOPES


def ensure_runtime_config_version_boundary(
    runtime_config: dict[str, Any] | None,
    *,
    context: dict[str, Any] | None = None,
    allow_non_trading_preview: bool = False,
) -> None:
    """Fail fast when ad-hoc runtime choices enter a trading execution path."""

    config = runtime_config or {}
    if not isinstance(config, dict):
        raise StrategyPackageValidationError(
            "runtime_config must be an object",
            context={**(context or {}), "runtime_config_type": type(config).__name__},
        )
    keys = runtime_behavior_keys(config)
    if not keys:
        return
    scope = runtime_config_scope(config)
    if allow_non_trading_preview and scope in NON_TRADING_RUNTIME_SCOPES:
        return
    if runtime_config_has_version_binding(config):
        binding = validate_runtime_profile_binding(
            config,
            context=context,
            require_trade_enabled=scope not in NON_TRADING_RUNTIME_SCOPES,
        )
        if binding["source"] == "platform_default":
            raise StrategyPackageValidationError(
                "platform default runtime profile cannot bind caller-provided behavior-changing runtime_config",
                context={**(context or {}), "behavior_keys": keys, "runtime_profile_binding": binding},
            )
        return
    raise StrategyPackageValidationError(
        "runtime_config changes trading behavior without a versioned runtime profile activation",
        context={
            **(context or {}),
            "behavior_keys": keys,
            "runtime_config_scope": scope,
            "required": {
                RUNTIME_PROFILE_ACTIVATION_KEY: ["profile_version_id", "config_sha256"],
                RUNTIME_PROFILE_BINDING_KEY: ["source", "profile_version_id", "config_sha256"],
            },
            "preview_scopes": sorted(NON_TRADING_RUNTIME_SCOPES),
        },
    )


def mark_non_trading_preview_runtime_config(
    runtime_config: dict[str, Any],
    *,
    reason: str,
) -> dict[str, Any]:
    config = dict(runtime_config)
    config[RUNTIME_CONFIG_SCOPE_KEY] = "non_trading_preview"
    binding = dict(config.get(RUNTIME_PROFILE_BINDING_KEY) or {})
    if not binding:
        binding = {
            "source": "ad_hoc_non_trading_preview",
            "profile_version_id": "unversioned_preview_runtime_profile",
            "config_sha256": runtime_profile_config_sha256(config),
            "trade_enabled": False,
            "reason": reason,
        }
    else:
        binding["trade_enabled"] = False
        binding.setdefault("reason", reason)
    config[RUNTIME_PROFILE_BINDING_KEY] = binding
    return config


def attach_default_runtime_profile_binding(runtime_config: dict[str, Any]) -> dict[str, Any]:
    """Attach an auditable platform-default profile binding to default runs."""

    config = dict(runtime_config)
    if runtime_config_has_version_binding(config):
        return config
    config[RUNTIME_PROFILE_BINDING_KEY] = {
        "source": "platform_default",
        "profile_version_id": DEFAULT_RUNTIME_PROFILE_VERSION_ID,
        "config_sha256": runtime_profile_config_sha256(config),
        "trade_enabled": True,
    }
    return config


def refresh_generated_runtime_profile_binding(runtime_config: dict[str, Any]) -> dict[str, Any]:
    """Refresh generated binding hashes after system-owned config finalization."""

    config = dict(runtime_config)
    binding = config.get(RUNTIME_PROFILE_BINDING_KEY)
    if not isinstance(binding, dict):
        return config
    if binding.get("source") not in {"platform_default", "ad_hoc_non_trading_preview"}:
        return config
    refreshed = dict(binding)
    refreshed["config_sha256"] = runtime_profile_config_sha256(config)
    config[RUNTIME_PROFILE_BINDING_KEY] = refreshed
    return config


def attach_activation_runtime_profile_binding(
    runtime_config: dict[str, Any],
    *,
    activation: dict[str, Any],
) -> dict[str, Any]:
    config = dict(runtime_config)
    required = ("profile_version_id", "config_sha256")
    missing = [key for key in required if not activation.get(key)]
    if missing:
        raise StrategyPackageValidationError(
            "runtime profile activation snapshot is missing version/hash fields",
            context={"missing_fields": missing, "activation": activation},
        )
    config[RUNTIME_PROFILE_BINDING_KEY] = {
        "source": "runtime_config_activation",
        "activation_id": activation.get("activation_id"),
        "profile_id": activation.get("profile_id"),
        "profile_name": activation.get("profile_name"),
        "profile_version_id": activation["profile_version_id"],
        "version_no": activation.get("version_no"),
        "config_sha256": activation["config_sha256"],
        "trade_enabled": True,
    }
    return config


def validate_runtime_profile_binding(
    runtime_config: dict[str, Any] | None,
    *,
    context: dict[str, Any] | None = None,
    require_trade_enabled: bool = True,
) -> dict[str, Any]:
    config = runtime_config or {}
    if not isinstance(config, dict):
        raise StrategyPackageValidationError(
            "runtime_config must be an object",
            context={**(context or {}), "runtime_config_type": type(config).__name__},
        )
    binding = config.get(RUNTIME_PROFILE_BINDING_KEY)
    if not isinstance(binding, dict):
        raise StrategyPackageValidationError(
            "runtime_config requires runtime_profile_binding for trading execution",
            context={**(context or {}), "binding_key": RUNTIME_PROFILE_BINDING_KEY},
        )
    source = str(binding.get("source") or "").strip()
    version_id = str(binding.get("profile_version_id") or "").strip()
    config_hash = str(binding.get("config_sha256") or "").strip()
    if source not in RUNTIME_PROFILE_BINDING_SOURCES:
        raise StrategyPackageValidationError(
            "runtime_profile_binding source is not an approved version source",
            context={
                **(context or {}),
                "runtime_profile_binding": binding,
                "allowed_sources": sorted(RUNTIME_PROFILE_BINDING_SOURCES),
            },
        )
    if not source or not version_id or not config_hash:
        raise StrategyPackageValidationError(
            "runtime_profile_binding requires source, profile_version_id and config_sha256",
            context={**(context or {}), "runtime_profile_binding": binding},
        )
    if source == "ad_hoc_non_trading_preview" and binding.get("trade_enabled") is not False:
        raise StrategyPackageValidationError(
            "ad-hoc preview runtime_profile_binding must be marked trade_enabled=false",
            context={**(context or {}), "runtime_profile_binding": binding},
        )
    if require_trade_enabled and binding.get("trade_enabled") is False:
        raise StrategyPackageValidationError(
            "non-trading runtime_config cannot enter Paper v2 or MiniQMT execution",
            context={**(context or {}), "runtime_profile_binding": binding},
        )
    activation = config.get(RUNTIME_PROFILE_ACTIVATION_KEY)
    if source == "runtime_config_activation":
        if not isinstance(activation, dict):
            raise StrategyPackageValidationError(
                "runtime_config_activation binding requires activation snapshot",
                context={**(context or {}), "runtime_profile_binding": binding},
            )
        if activation.get("profile_version_id") != version_id or activation.get("config_sha256") != config_hash:
            raise StrategyPackageValidationError(
                "runtime_config_activation binding does not match activation snapshot",
                context={
                    **(context or {}),
                    "runtime_profile_binding": binding,
                    "runtime_profile_activation": activation,
                },
            )
    if source == "platform_default":
        expected = runtime_profile_config_sha256(config)
        if version_id != DEFAULT_RUNTIME_PROFILE_VERSION_ID or config_hash != expected:
            raise StrategyPackageValidationError(
                "platform default runtime profile binding hash mismatch",
                context={
                    **(context or {}),
                    "profile_version_id": version_id,
                    "config_sha256": config_hash,
                    "expected_profile_version_id": DEFAULT_RUNTIME_PROFILE_VERSION_ID,
                    "expected_config_sha256": expected,
                },
            )
    return binding


def runtime_profile_config_sha256(runtime_config: dict[str, Any] | None) -> str:
    config = {
        key: value
        for key, value in dict(runtime_config or {}).items()
        if key not in _PROFILE_HASH_EXCLUDED_KEYS and not str(key).startswith("_")
    }
    normalized = normalize_selection_runtime_config(config)
    payload: dict[str, Any] = {"runtime_profile": normalized["runtime_profile"]}
    for key in ("runtime_variant", "selection_artifact_config", "selection_artifact", "model", "metadata"):
        if key in normalized:
            payload[key] = normalized[key]
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
