"""Unified strategy runtime config contract.

The contract separates strategy semantics from platform/runtime capabilities so
QE, Paper v2, and future miniQMT adapters can prove they run the same strategy
without forcing adapter-specific data snapshots into a StrategyPackage.
"""

from __future__ import annotations

import hashlib
import json
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .models import StrategyPackageManifest


class RuntimeAdapterKind(str, Enum):
    QE_QLIB_BIN = "qe_qlib_bin"
    PAPER_V2_DB = "paper_v2_db"
    MINI_QMT = "mini_qmt"


class ModelWeightPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    default_weight_source: Literal["backtest_manifest"] = "backtest_manifest"
    rolling_retrain_enabled: bool = False
    rolling_window_years: int | None = Field(default=None, gt=0)
    retrain_schedule: str | None = None


class HMMUsagePolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    signal_namespace: str = "paper_v2_hmm"
    require_realtime_prediction: bool = True


class TailHandlingPolicy(BaseModel):
    model_config = ConfigDict(extra="allow")

    enabled: bool = True
    mode: str = "qe_compatible"


class StrategySemanticsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    alpha_mode: str
    alpha_components: list[dict[str, Any]]
    alpha_combination_policy: dict[str, Any]
    factor_set: list[dict[str, Any]]
    model_assets: list[dict[str, Any]]
    model_weight_policy: ModelWeightPolicy = Field(default_factory=ModelWeightPolicy)
    source_lineage: dict[str, Any] = Field(default_factory=dict)
    training_evidence: dict[str, Any] = Field(default_factory=dict)
    hmm_usage_policy: HMMUsagePolicy = Field(default_factory=HMMUsagePolicy)

    @model_validator(mode="after")
    def _must_have_core_assets(self) -> "StrategySemanticsConfig":
        if not self.factor_set:
            raise ValueError("strategy semantics require at least one factor")
        if not self.model_assets:
            raise ValueError("strategy semantics require at least one model asset")
        return self


class PlatformHMMCapability(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    provider: str = "paper_v2_hmm_predictor"
    rolling_train_supported: bool = True
    realtime_prediction_supported: bool = True
    active_model_version: str | None = None


class PlatformUniverseCapability(BaseModel):
    model_config = ConfigDict(extra="forbid")

    st_pit_filter_enabled: bool = True
    source: str = "paper_v2_platform_latest"
    snapshot_id: str | None = None
    generated_at: str | None = None


class PlatformCapabilities(BaseModel):
    model_config = ConfigDict(extra="forbid")

    hmm: PlatformHMMCapability = Field(default_factory=PlatformHMMCapability)
    universe: PlatformUniverseCapability = Field(default_factory=PlatformUniverseCapability)


class RuntimeAdapterConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: RuntimeAdapterKind
    data_source: str
    execution_target: str
    qlib_provider_uri: str | None = None
    db_profile: str | None = None
    broker_profile: str | None = None

    @model_validator(mode="after")
    def _validate_adapter_contract(self) -> "RuntimeAdapterConfig":
        if self.kind == RuntimeAdapterKind.QE_QLIB_BIN and not self.qlib_provider_uri:
            raise ValueError("qe_qlib_bin adapter requires qlib_provider_uri")
        if self.kind == RuntimeAdapterKind.PAPER_V2_DB and not self.db_profile:
            raise ValueError("paper_v2_db adapter requires db_profile")
        if self.kind == RuntimeAdapterKind.MINI_QMT and not self.broker_profile:
            raise ValueError("mini_qmt adapter requires broker_profile")
        return self


class UnifiedStrategyRuntimeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["unified_strategy_runtime_config_v1"] = "unified_strategy_runtime_config_v1"
    strategy_semantics: StrategySemanticsConfig
    platform_capabilities: PlatformCapabilities = Field(default_factory=PlatformCapabilities)
    adapter: RuntimeAdapterConfig
    config_sha256: str | None = None
    runtime_config_sha256: str | None = None

    def freeze(self) -> "UnifiedStrategyRuntimeConfig":
        config_sha256 = compute_strategy_semantics_sha256(self.strategy_semantics)
        runtime_config_sha256 = compute_runtime_config_sha256(
            self.model_copy(update={"config_sha256": config_sha256, "runtime_config_sha256": None})
        )
        return self.model_copy(
            update={
                "config_sha256": config_sha256,
                "runtime_config_sha256": runtime_config_sha256,
            }
        )


_HISTORICAL_PLATFORM_KEYS = {
    "_precomputed_hmm_coefficients_json",
    "hmm_coefficients_snapshot_id",
    "hmm_model_snapshot_id",
    "hmm_snapshot_id",
    "st_pit_data_range",
    "st_pit_end_date",
    "st_pit_snapshot_id",
    "st_pit_start_date",
}


def _canonical_json(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _sha256(payload: Any) -> str:
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def compute_strategy_semantics_sha256(config: StrategySemanticsConfig) -> str:
    return _sha256(config.model_dump(mode="json"))


def compute_runtime_config_sha256(config: UnifiedStrategyRuntimeConfig) -> str:
    payload = config.model_dump(mode="json")
    payload["runtime_config_sha256"] = None
    return _sha256(payload)


def strip_historical_platform_keys(payload: dict[str, Any]) -> dict[str, Any]:
    """Remove QE backtest-only platform snapshots from strategy semantics."""

    return {key: value for key, value in payload.items() if key not in _HISTORICAL_PLATFORM_KEYS}


def build_unified_runtime_config_from_manifest(
    manifest: StrategyPackageManifest,
    *,
    adapter: RuntimeAdapterConfig,
    platform_capabilities: PlatformCapabilities | None = None,
    model_weight_policy: ModelWeightPolicy | None = None,
    hmm_usage_policy: HMMUsagePolicy | None = None,
) -> UnifiedStrategyRuntimeConfig:
    model_assets = manifest.model_asset if isinstance(manifest.model_asset, list) else [manifest.model_asset]
    source_lineage = manifest.source.model_dump(mode="json")
    training_evidence = {
        "backtest_summary": manifest.backtest_summary.model_dump(mode="json"),
        "source_evidence_schema_version": (manifest.source_evidence or {}).get("schema_version"),
        "backtest_context_schema_version": (manifest.backtest_context or {}).get("schema_version"),
    }

    config = UnifiedStrategyRuntimeConfig(
        strategy_semantics=StrategySemanticsConfig(
            alpha_mode=manifest.alpha_mode.value,
            alpha_components=[component.model_dump(mode="json") for component in manifest.alpha_components],
            alpha_combination_policy=manifest.alpha_combination_policy.model_dump(mode="json"),
            factor_set=[factor.model_dump(mode="json") for factor in manifest.factor_set],
            model_assets=[asset.model_dump(mode="json") for asset in model_assets],
            model_weight_policy=model_weight_policy or ModelWeightPolicy(),
            source_lineage=source_lineage,
            training_evidence=training_evidence,
            hmm_usage_policy=hmm_usage_policy or HMMUsagePolicy(),
        ),
        platform_capabilities=platform_capabilities or PlatformCapabilities(),
        adapter=adapter,
    )
    return config.freeze()


def default_qe_qlib_adapter(qlib_provider_uri: str = "qlib://qe-backtest-default") -> RuntimeAdapterConfig:
    return RuntimeAdapterConfig(
        kind=RuntimeAdapterKind.QE_QLIB_BIN,
        data_source="qlib_bin",
        execution_target="qe_backtest",
        qlib_provider_uri=qlib_provider_uri,
    )


def default_paper_v2_db_adapter(db_profile: str = "paper_v2_runtime_default") -> RuntimeAdapterConfig:
    return RuntimeAdapterConfig(
        kind=RuntimeAdapterKind.PAPER_V2_DB,
        data_source="paper_v2_realtime_db",
        execution_target="aistock_paper",
        db_profile=db_profile,
    )


def build_default_runtime_config_bundle(manifest: StrategyPackageManifest) -> dict[str, Any]:
    qe_config = build_unified_runtime_config_from_manifest(manifest, adapter=default_qe_qlib_adapter())
    paper_config = build_unified_runtime_config_from_manifest(manifest, adapter=default_paper_v2_db_adapter())
    return {
        "schema_version": qe_config.schema_version,
        "config_sha256": qe_config.config_sha256,
        "qe_backtest": qe_config.model_dump(mode="json"),
        "paper_v2": paper_config.model_dump(mode="json"),
        "equivalence": {
            "strategy_semantics_shared": qe_config.config_sha256 == paper_config.config_sha256,
            "adapter_specific_runtime_hashes": qe_config.runtime_config_sha256 != paper_config.runtime_config_sha256,
        },
    }
