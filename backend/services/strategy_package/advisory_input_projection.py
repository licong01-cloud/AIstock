"""Pure Advisory input projection from an already admitted frozen manifest."""

from __future__ import annotations

import re
from typing import Any, Literal, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from backend.data_service.preprocessor import get_required_data_window
from backend.services.strategy_package.models import (
    AlphaMode,
    FactorAsset,
    RuntimeAssetManifest,
    StrategyPackageManifest,
    StrategyPackageCanonicalPitBindingV2,
)
from backend.services.strategy_package.canonical_pit_compatibility import (
    require_canonical_pit_strategy_package,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


PROJECTION_SCHEMA_VERSION = "strategy_package_advisory_input_projection_v1"
HISTORICAL_RANGE_PROJECTION_SCHEMA_VERSION = "strategy_package_historical_range_input_projection_v1"
PROJECTION_SOURCE = "ADMITTED_MANIFEST_ONLY"
REASON_INPUT_PROJECTION_UNAVAILABLE = "ADVISORY_INPUT_PROJECTION_UNAVAILABLE"
REASON_INPUT_PROJECTION_CONFLICT = "ADVISORY_INPUT_PROJECTION_CONFLICT"

SELECTION_QUERY_CONTRACT_ID = "strategy_package_live_inference_inputs"
SELECTION_QUERY_CONTRACT_VERSION = "v2"
SELECTION_PIT_UNIVERSE_KEY = "shsz_st_pit_active_v1"
SELECTION_QUERY_CONTRACT_PAYLOAD = {
    "contract_id": SELECTION_QUERY_CONTRACT_ID,
    "contract_version": SELECTION_QUERY_CONTRACT_VERSION,
    "logical_inputs": [
        {
            "source_role": "pit_universe",
            "dataset_id": "market.stock_universe_pit",
            "query_template_id": "StockUniversePitService.get_eligible_codes",
            "query_template_version": "v1",
            "fixed_parameters": {
                "universe_key": SELECTION_PIT_UNIVERSE_KEY,
                "ensure": True,
            },
        },
        {
            "source_role": "market_history",
            "dataset_id": "market.kline_daily_raw",
            "query_template_id": "get_history_window",
            "query_template_version": "v1",
        },
        {
            "source_role": "fundamental_moneyflow",
            "dataset_id": "timescaledb.fundamental_moneyflow",
            "query_template_id": "timescaledb_adapter.fetch_fundamental_data_ts",
            "query_template_version": "v1",
        },
        {
            "source_role": "trading_calendar",
            "dataset_id": "market.trading_calendar",
            "query_template_id": "InferenceEngine.trade_date_and_window_resolution",
            "query_template_version": "v1",
        },
        {
            "source_role": "reference_price",
            "dataset_id": "market.kline_daily_raw",
            "query_template_id": "SelectionArtifact.reference_price",
            "query_template_version": "v1",
        },
    ],
}
SELECTION_QUERY_CONTRACT_HASH = canonical_json_sha256(SELECTION_QUERY_CONTRACT_PAYLOAD)

HISTORICAL_RANGE_QUERY_CONTRACT_ID = "strategy_package_historical_range_inference_inputs"
HISTORICAL_RANGE_QUERY_CONTRACT_VERSION = "v1"
HISTORICAL_RANGE_QUERY_CONTRACT_PAYLOAD = {
    "contract_id": HISTORICAL_RANGE_QUERY_CONTRACT_ID,
    "contract_version": HISTORICAL_RANGE_QUERY_CONTRACT_VERSION,
    "include_reference_price": False,
    "logical_inputs": [
        {
            **item,
            "fixed_parameters": {**item["fixed_parameters"], "ensure": False},
        }
        if item["source_role"] == "pit_universe"
        else dict(item)
        for item in SELECTION_QUERY_CONTRACT_PAYLOAD["logical_inputs"]
        if item["source_role"] != "reference_price"
    ],
}
HISTORICAL_RANGE_QUERY_CONTRACT_HASH = canonical_json_sha256(HISTORICAL_RANGE_QUERY_CONTRACT_PAYLOAD)

CANONICAL_SELECTION_QUERY_CONTRACT_ID = "strategy_package_canonical_pit_live_inference_inputs"
CANONICAL_SELECTION_QUERY_CONTRACT_VERSION = "v1"
CANONICAL_SELECTION_QUERY_CONTRACT_PAYLOAD = {
    "contract_id": CANONICAL_SELECTION_QUERY_CONTRACT_ID,
    "contract_version": CANONICAL_SELECTION_QUERY_CONTRACT_VERSION,
    "pit_training_identity_source": "strategy_package_manifest",
    "pit_runtime_identity_source": "selection_runtime_authority_lease",
    "logical_inputs": [
        {
            "source_role": "pit_universe",
            "dataset_id": "market.stock_universe_pit",
            "query_template_id": "CanonicalPitAuthorityResolver.resolve_live_binding",
            "query_template_version": "v2",
            "parameter_source": "selection_runtime_authority_lease",
        },
        *[
        item
        for item in SELECTION_QUERY_CONTRACT_PAYLOAD["logical_inputs"]
        if item["source_role"] != "pit_universe"
        ],
    ],
}
CANONICAL_SELECTION_QUERY_CONTRACT_HASH = canonical_json_sha256(CANONICAL_SELECTION_QUERY_CONTRACT_PAYLOAD)

CANONICAL_HISTORICAL_QUERY_CONTRACT_ID = "strategy_package_canonical_pit_historical_inference_inputs"
CANONICAL_HISTORICAL_QUERY_CONTRACT_VERSION = "v1"
CANONICAL_HISTORICAL_QUERY_CONTRACT_PAYLOAD = {
    "contract_id": CANONICAL_HISTORICAL_QUERY_CONTRACT_ID,
    "contract_version": CANONICAL_HISTORICAL_QUERY_CONTRACT_VERSION,
    "pit_identity_source": "strategy_package_manifest.canonical_pit_binding",
    "pit_universe_source": "frozen_release_snapshot",
    "include_reference_price": False,
    "logical_inputs": [
        {
            "source_role": "pit_universe",
            "dataset_id": "market.stock_universe_pit",
            "query_template_id": "StockUniversePitService.get_eligible_codes",
            "query_template_version": "v2",
            "parameter_source": "strategy_package_manifest.canonical_pit_binding.frozen_universe_key",
            "ensure": False,
        },
        *[
        item
        for item in CANONICAL_SELECTION_QUERY_CONTRACT_PAYLOAD["logical_inputs"]
        if item["source_role"] not in {"pit_universe", "reference_price"}
        ],
    ],
}
CANONICAL_HISTORICAL_QUERY_CONTRACT_HASH = canonical_json_sha256(
    CANONICAL_HISTORICAL_QUERY_CONTRACT_PAYLOAD
)


def get_strategy_package_inference_required_window(factor_order: tuple[str, ...] | list[str]) -> int:
    """Return the exact window used by the authoritative StrategyPackage WSL runner."""

    max_window = 61
    for factor in factor_order:
        for match in re.findall(r"(\d+)\s*d", str(factor), flags=re.IGNORECASE):
            max_window = max(max_window, int(match) + 5)
        if "250" in str(factor):
            max_window = max(max_window, 260)
    return max_window


class AdvisoryInputProjectionError(ValueError):
    """Program-local projection failure; never a StrategyPackage admission result."""

    def __init__(self, reason_code: str, message: str, *, context: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.context = context or {}


class StrategyPackageAdvisoryInputLegV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    alpha_component_id: str = Field(min_length=1, max_length=160)
    factor_order: tuple[str, ...] = Field(min_length=1)
    factor_order_hash: str = Field(min_length=64, max_length=64)
    required_window: int = Field(ge=1)
    window_resolution: Literal["trading_day"] = "trading_day"
    alpha158_alias_set_hash: str = Field(min_length=64, max_length=64)
    dynamic_factor_ref_set_hash: str = Field(min_length=64, max_length=64)

    @field_validator("factor_order_hash", "alpha158_alias_set_hash", "dynamic_factor_ref_set_hash")
    @classmethod
    def _hash(cls, value: str) -> str:
        normalized = value.strip().lower()
        if len(normalized) != 64 or any(char not in "0123456789abcdef" for char in normalized):
            raise ValueError("projection hash fields must be lowercase sha256")
        return normalized

    @model_validator(mode="after")
    def _closed(self) -> "StrategyPackageAdvisoryInputLegV1":
        if len(self.factor_order) != len(set(self.factor_order)):
            raise ValueError("factor_order must not contain duplicate factors")
        if self.factor_order_hash != canonical_json_sha256(list(self.factor_order)):
            raise ValueError("factor_order_hash does not match factor_order")
        if self.required_window != get_required_data_window(list(self.factor_order)):
            raise ValueError("required_window does not match the exact factor_order")
        return self


class StrategyPackageAdvisoryInputProjectionV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[PROJECTION_SCHEMA_VERSION] = PROJECTION_SCHEMA_VERSION
    projection_source: Literal[PROJECTION_SOURCE] = PROJECTION_SOURCE
    package_id: str = Field(min_length=1, max_length=160)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    alpha_mode: AlphaMode
    selection_query_contract_id: Literal[SELECTION_QUERY_CONTRACT_ID] = SELECTION_QUERY_CONTRACT_ID
    selection_query_contract_version: Literal[SELECTION_QUERY_CONTRACT_VERSION] = SELECTION_QUERY_CONTRACT_VERSION
    selection_query_contract_hash: Literal[SELECTION_QUERY_CONTRACT_HASH] = SELECTION_QUERY_CONTRACT_HASH
    legs: tuple[StrategyPackageAdvisoryInputLegV1, ...] = Field(min_length=1)
    projection_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("manifest_sha256", "projection_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip().lower()
        if len(normalized) != 64 or any(char not in "0123456789abcdef" for char in normalized):
            raise ValueError("projection identity fields must be lowercase sha256")
        return normalized

    @model_validator(mode="after")
    def _closed(self) -> "StrategyPackageAdvisoryInputProjectionV1":
        component_ids = tuple(item.alpha_component_id for item in self.legs)
        if len(component_ids) != len(set(component_ids)):
            raise ValueError("projection must contain one leg per alpha component")
        payload = self.model_dump(mode="json", exclude={"projection_hash"})
        digest = canonical_json_sha256(payload)
        if self.projection_hash is not None and self.projection_hash != digest:
            raise ValueError("projection_hash does not match canonical projection")
        object.__setattr__(self, "projection_hash", digest)
        return self


class StrategyPackageAdvisoryInputProjectionV2(BaseModel):
    """Canonical package projection; rolling runtime identity is supplied by W4."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["strategy_package_advisory_input_projection_v2"] = (
        "strategy_package_advisory_input_projection_v2"
    )
    projection_source: Literal[PROJECTION_SOURCE] = PROJECTION_SOURCE
    package_id: str = Field(min_length=1, max_length=160)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    alpha_mode: AlphaMode
    canonical_pit_binding: StrategyPackageCanonicalPitBindingV2
    selection_query_contract_id: Literal[CANONICAL_SELECTION_QUERY_CONTRACT_ID] = (
        CANONICAL_SELECTION_QUERY_CONTRACT_ID
    )
    selection_query_contract_version: Literal[CANONICAL_SELECTION_QUERY_CONTRACT_VERSION] = (
        CANONICAL_SELECTION_QUERY_CONTRACT_VERSION
    )
    selection_query_contract_hash: Literal[CANONICAL_SELECTION_QUERY_CONTRACT_HASH] = (
        CANONICAL_SELECTION_QUERY_CONTRACT_HASH
    )
    legs: tuple[StrategyPackageAdvisoryInputLegV1, ...] = Field(min_length=1)
    projection_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("manifest_sha256", "projection_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip().lower()
        if not _is_sha256(normalized):
            raise ValueError("canonical projection identity fields must be lowercase sha256")
        return normalized

    @model_validator(mode="after")
    def _closed(self) -> "StrategyPackageAdvisoryInputProjectionV2":
        component_ids = tuple(item.alpha_component_id for item in self.legs)
        if len(component_ids) != len(set(component_ids)):
            raise ValueError("projection must contain one leg per alpha component")
        digest = canonical_json_sha256(self.model_dump(mode="json", exclude={"projection_hash"}))
        if self.projection_hash is not None and self.projection_hash != digest:
            raise ValueError("projection_hash does not match canonical projection")
        object.__setattr__(self, "projection_hash", digest)
        return self


class StrategyPackageHistoricalRangeInputProjectionV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[HISTORICAL_RANGE_PROJECTION_SCHEMA_VERSION] = (
        HISTORICAL_RANGE_PROJECTION_SCHEMA_VERSION
    )
    projection_source: Literal[PROJECTION_SOURCE] = PROJECTION_SOURCE
    package_id: str = Field(min_length=1, max_length=160)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    alpha_mode: AlphaMode
    query_contract_id: Literal[HISTORICAL_RANGE_QUERY_CONTRACT_ID] = HISTORICAL_RANGE_QUERY_CONTRACT_ID
    query_contract_version: Literal[HISTORICAL_RANGE_QUERY_CONTRACT_VERSION] = (
        HISTORICAL_RANGE_QUERY_CONTRACT_VERSION
    )
    query_contract_hash: Literal[HISTORICAL_RANGE_QUERY_CONTRACT_HASH] = HISTORICAL_RANGE_QUERY_CONTRACT_HASH
    pit_universe_key: Literal[SELECTION_PIT_UNIVERSE_KEY] = SELECTION_PIT_UNIVERSE_KEY
    pit_universe_policy: Literal["REQUIRE_EXISTING_READ_ONLY"] = "REQUIRE_EXISTING_READ_ONLY"
    pit_universe_ensure: Literal[False] = False
    legs: tuple[StrategyPackageAdvisoryInputLegV1, ...] = Field(min_length=1)
    projection_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("manifest_sha256", "projection_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip().lower()
        if not _is_sha256(normalized):
            raise ValueError("historical projection identity fields must be lowercase sha256")
        return normalized

    @model_validator(mode="after")
    def _closed(self) -> "StrategyPackageHistoricalRangeInputProjectionV1":
        component_ids = tuple(item.alpha_component_id for item in self.legs)
        if len(component_ids) != len(set(component_ids)):
            raise ValueError("historical projection must contain one leg per alpha component")
        digest = canonical_json_sha256(self.model_dump(mode="json", exclude={"projection_hash"}))
        if self.projection_hash is not None and self.projection_hash != digest:
            raise ValueError("historical projection_hash does not match canonical projection")
        object.__setattr__(self, "projection_hash", digest)
        return self


class StrategyPackageHistoricalRangeInputProjectionV2(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["strategy_package_historical_range_input_projection_v2"] = (
        "strategy_package_historical_range_input_projection_v2"
    )
    projection_source: Literal[PROJECTION_SOURCE] = PROJECTION_SOURCE
    package_id: str = Field(min_length=1, max_length=160)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    alpha_mode: AlphaMode
    canonical_pit_binding: StrategyPackageCanonicalPitBindingV2
    query_contract_id: Literal[CANONICAL_HISTORICAL_QUERY_CONTRACT_ID] = (
        CANONICAL_HISTORICAL_QUERY_CONTRACT_ID
    )
    query_contract_version: Literal[CANONICAL_HISTORICAL_QUERY_CONTRACT_VERSION] = (
        CANONICAL_HISTORICAL_QUERY_CONTRACT_VERSION
    )
    query_contract_hash: Literal[CANONICAL_HISTORICAL_QUERY_CONTRACT_HASH] = (
        CANONICAL_HISTORICAL_QUERY_CONTRACT_HASH
    )
    pit_universe_key: str = Field(min_length=1, max_length=240)
    pit_universe_policy: Literal["REQUIRE_FROZEN_READ_ONLY"] = "REQUIRE_FROZEN_READ_ONLY"
    pit_universe_ensure: Literal[False] = False
    legs: tuple[StrategyPackageAdvisoryInputLegV1, ...] = Field(min_length=1)
    projection_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("manifest_sha256", "projection_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip().lower()
        if not _is_sha256(normalized):
            raise ValueError("historical projection identity fields must be lowercase sha256")
        return normalized

    @model_validator(mode="after")
    def _closed(self) -> "StrategyPackageHistoricalRangeInputProjectionV2":
        if self.pit_universe_key != self.canonical_pit_binding.frozen_universe_key:
            raise ValueError("historical projection universe key differs from frozen package binding")
        component_ids = tuple(item.alpha_component_id for item in self.legs)
        if len(component_ids) != len(set(component_ids)):
            raise ValueError("historical projection must contain one leg per alpha component")
        digest = canonical_json_sha256(self.model_dump(mode="json", exclude={"projection_hash"}))
        if self.projection_hash is not None and self.projection_hash != digest:
            raise ValueError("historical projection_hash does not match canonical projection")
        object.__setattr__(self, "projection_hash", digest)
        return self


StrategyPackageHistoricalRangeInputProjection: TypeAlias = (
    StrategyPackageHistoricalRangeInputProjectionV1 | StrategyPackageHistoricalRangeInputProjectionV2
)


def project_advisory_inputs(
    manifest: StrategyPackageManifest,
) -> StrategyPackageAdvisoryInputProjectionV1:
    """Project exact factor orders without repository, asset, model, or inference access."""

    manifest_sha256 = str(manifest.manifest_sha256 or "").strip().lower()
    if not _is_sha256(manifest_sha256):
        _unavailable(manifest, "admitted manifest does not carry its frozen manifest_sha256")

    return StrategyPackageAdvisoryInputProjectionV1(
        package_id=manifest.package_id,
        manifest_sha256=manifest_sha256,
        alpha_mode=manifest.alpha_mode,
        legs=_project_legs(manifest),
    )


def project_canonical_advisory_inputs(
    manifest: StrategyPackageManifest,
) -> StrategyPackageAdvisoryInputProjectionV2:
    """Project a canonical package without inventing a rolling runtime lease."""

    manifest_sha256 = str(manifest.manifest_sha256 or "").strip().lower()
    if not _is_sha256(manifest_sha256):
        _unavailable(manifest, "admitted manifest does not carry its frozen manifest_sha256")
    binding = require_canonical_pit_strategy_package(manifest, operation="advisory_prediction")
    return StrategyPackageAdvisoryInputProjectionV2(
        package_id=manifest.package_id,
        manifest_sha256=manifest_sha256,
        alpha_mode=manifest.alpha_mode,
        canonical_pit_binding=binding,
        legs=_project_legs(manifest),
    )


def project_historical_range_inputs(
    manifest: StrategyPackageManifest,
) -> StrategyPackageHistoricalRangeInputProjection:
    """Project the admitted package with an explicit read-only PIT query contract."""

    manifest_sha256 = str(manifest.manifest_sha256 or "").strip().lower()
    if not _is_sha256(manifest_sha256):
        _unavailable(manifest, "admitted manifest does not carry its frozen manifest_sha256")
    if manifest.is_canonical_pit_v2_manifest:
        binding = require_canonical_pit_strategy_package(manifest, operation="historical_reproduction")
        return StrategyPackageHistoricalRangeInputProjectionV2(
            package_id=manifest.package_id,
            manifest_sha256=manifest_sha256,
            alpha_mode=manifest.alpha_mode,
            canonical_pit_binding=binding,
            pit_universe_key=binding.frozen_universe_key,
            legs=_project_legs(manifest),
        )
    return StrategyPackageHistoricalRangeInputProjectionV1(
        package_id=manifest.package_id,
        manifest_sha256=manifest_sha256,
        alpha_mode=manifest.alpha_mode,
        legs=_project_legs(manifest),
    )


def _project_legs(manifest: StrategyPackageManifest) -> tuple[StrategyPackageAdvisoryInputLegV1, ...]:
    if manifest.alpha_mode is AlphaMode.SINGLE_ALPHA:
        return (_single_leg(manifest),)
    if manifest.alpha_mode is AlphaMode.MULTI_ALPHA:
        return _multi_legs(manifest)
    _unavailable(manifest, f"unsupported alpha mode: {manifest.alpha_mode}")


def _single_leg(manifest: StrategyPackageManifest) -> StrategyPackageAdvisoryInputLegV1:
    if len(manifest.alpha_components) != 1:
        _conflict(manifest, "single-alpha manifest does not contain exactly one persisted component")
    component = manifest.alpha_components[0]
    aliases = _runtime_aliases(manifest.runtime_assets, manifest=manifest, component_id=component.alpha_id)
    dynamic_names = tuple(item.factor_name for item in manifest.factor_set)
    dynamic_refs = tuple(item.factor_id for item in manifest.factor_set)
    return _build_leg(
        manifest=manifest,
        component_id=component.alpha_id,
        aliases=aliases,
        dynamic_names=dynamic_names,
        dynamic_refs=dynamic_refs,
    )


def _multi_legs(manifest: StrategyPackageManifest) -> tuple[StrategyPackageAdvisoryInputLegV1, ...]:
    raw_multi = manifest.source_evidence.get("multi_alpha")
    raw_legs = raw_multi.get("legs") if isinstance(raw_multi, dict) else None
    if not isinstance(raw_legs, list) or not raw_legs:
        _unavailable(manifest, "native multi-alpha manifest lacks persisted leg runtime metadata")

    metadata_by_leg: dict[str, dict[str, Any]] = {}
    for raw_leg in raw_legs:
        if not isinstance(raw_leg, dict) or not str(raw_leg.get("leg_id") or "").strip():
            _unavailable(manifest, "native multi-alpha leg metadata lacks leg_id")
        leg_id = str(raw_leg["leg_id"]).strip()
        if leg_id in metadata_by_leg:
            _conflict(manifest, f"native multi-alpha leg metadata is duplicated: {leg_id}")
        metadata_by_leg[leg_id] = raw_leg

    component_ids = tuple(component.alpha_id for component in manifest.alpha_components)
    if len(component_ids) != len(set(component_ids)):
        _conflict(manifest, "native multi-alpha component identities are duplicated")
    if set(metadata_by_leg) != set(component_ids):
        _unavailable(
            manifest,
            "native multi-alpha persisted leg metadata does not exactly cover manifest components",
            context={"component_ids": list(component_ids), "metadata_leg_ids": list(metadata_by_leg)},
        )

    factor_index = _factor_index(manifest)
    projected: list[StrategyPackageAdvisoryInputLegV1] = []
    for component in manifest.alpha_components:
        raw_leg = metadata_by_leg[component.alpha_id]
        try:
            runtime_assets = RuntimeAssetManifest.model_validate(raw_leg.get("runtime_assets"))
        except ValidationError as exc:
            _unavailable(
                manifest,
                f"native multi-alpha leg {component.alpha_id} runtime metadata is unavailable",
                context={"validation_errors": exc.errors(include_url=False)},
            )
        aliases = _runtime_aliases(runtime_assets, manifest=manifest, component_id=component.alpha_id)
        dynamic_refs = tuple(component.lineage.factor_artifact_refs)
        if not dynamic_refs:
            _unavailable(manifest, f"native multi-alpha leg {component.alpha_id} has no persisted factor refs")
        dynamic_names = tuple(
            _resolve_factor_ref(
                manifest=manifest,
                component_id=component.alpha_id,
                factor_ref=factor_ref,
                factor_index=factor_index,
            ).factor_name
            for factor_ref in dynamic_refs
        )
        projected.append(
            _build_leg(
                manifest=manifest,
                component_id=component.alpha_id,
                aliases=aliases,
                dynamic_names=dynamic_names,
                dynamic_refs=dynamic_refs,
            )
        )
    return tuple(projected)


def _runtime_aliases(
    runtime_assets: RuntimeAssetManifest | None,
    *,
    manifest: StrategyPackageManifest,
    component_id: str,
) -> tuple[str, ...]:
    if runtime_assets is None:
        _unavailable(manifest, f"component {component_id} lacks persisted runtime assets")
    aliases = tuple(runtime_assets.alpha158.aliases)
    if runtime_assets.alpha158.enabled and not aliases:
        _unavailable(manifest, f"component {component_id} has enabled Alpha158 without persisted aliases")
    return aliases


def _factor_index(manifest: StrategyPackageManifest) -> dict[str, tuple[FactorAsset, ...]]:
    index: dict[str, list[FactorAsset]] = {}
    for factor in manifest.factor_set:
        for identity in dict.fromkeys((factor.factor_id, factor.factor_name)):
            index.setdefault(identity, []).append(factor)
    return {identity: tuple(items) for identity, items in index.items()}


def _resolve_factor_ref(
    *,
    manifest: StrategyPackageManifest,
    component_id: str,
    factor_ref: str,
    factor_index: dict[str, tuple[FactorAsset, ...]],
) -> FactorAsset:
    matches = factor_index.get(factor_ref, ())
    if not matches:
        _unavailable(manifest, f"component {component_id} factor ref is missing: {factor_ref}")
    if len(matches) != 1:
        _conflict(manifest, f"component {component_id} factor ref is ambiguous: {factor_ref}")
    return matches[0]


def _build_leg(
    *,
    manifest: StrategyPackageManifest,
    component_id: str,
    aliases: tuple[str, ...],
    dynamic_names: tuple[str, ...],
    dynamic_refs: tuple[str, ...],
) -> StrategyPackageAdvisoryInputLegV1:
    factor_order = aliases + dynamic_names
    if not factor_order:
        _unavailable(manifest, f"component {component_id} has no persisted factors")
    duplicates = sorted({item for item in factor_order if factor_order.count(item) > 1})
    if duplicates:
        _conflict(
            manifest,
            f"component {component_id} factor order contains duplicate identities",
            context={"duplicates": duplicates},
        )
    return StrategyPackageAdvisoryInputLegV1(
        alpha_component_id=component_id,
        factor_order=factor_order,
        factor_order_hash=canonical_json_sha256(list(factor_order)),
        required_window=get_required_data_window(list(factor_order)),
        alpha158_alias_set_hash=canonical_json_sha256(list(aliases)),
        dynamic_factor_ref_set_hash=canonical_json_sha256(list(dynamic_refs)),
    )


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(char in "0123456789abcdef" for char in value)


def _unavailable(
    manifest: StrategyPackageManifest,
    message: str,
    *,
    context: dict[str, Any] | None = None,
) -> None:
    raise AdvisoryInputProjectionError(
        REASON_INPUT_PROJECTION_UNAVAILABLE,
        message,
        context={"package_id": manifest.package_id, **(context or {})},
    )


def _conflict(
    manifest: StrategyPackageManifest,
    message: str,
    *,
    context: dict[str, Any] | None = None,
) -> None:
    raise AdvisoryInputProjectionError(
        REASON_INPUT_PROJECTION_CONFLICT,
        message,
        context={"package_id": manifest.package_id, **(context or {})},
    )
