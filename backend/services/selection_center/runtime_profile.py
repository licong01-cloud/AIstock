"""Runtime profile parsing for package-based selection and Paper v2.

The runtime profile contains mutable operational choices that must not mutate
the frozen StrategyPackage manifest. Unknown keys are rejected inside the
profile itself; score-production keys remain owned by StrategyPackageRuntime
and must resolve to authoritative live/latest-data selection artifacts.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from backend.services.trading_core.errors import StrategyPackageValidationError


class RuntimeHMMProfile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    model_snapshot_id: str | None = None
    signal_preset: str | None = None
    coefficients_path: str | None = None

    @field_validator("model_snapshot_id", "signal_preset", "coefficients_path")
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
        if not profile.hmm.model_snapshot_id:
            raise StrategyPackageValidationError(
                "HMM runtime profile requires model_snapshot_id when enabled",
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
