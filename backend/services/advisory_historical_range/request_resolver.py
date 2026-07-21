"""Admission-preserving request resolution for Phase 1R historical research."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol

from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.models import (
    ExistingProgramSpecV1,
    HistoricalRangeAdmittedComponentV1,
    HistoricalRangeAdmittedPackageProjectionV1,
    HistoricalRangeAlphaMode,
    HistoricalRangeFrozenProgramV1,
    HistoricalRangeResearchBatchRequestV1,
    ResearchProgramSpecV1,
)
from backend.services.advisory_program import AdvisoryProgram, AdvisoryStrategyBindingVersion
from backend.services.strategy_package.advisory_input_projection import (
    StrategyPackageHistoricalRangeInputProjectionV1,
    get_strategy_package_inference_required_window,
    project_historical_range_inputs,
)
from backend.services.strategy_package.models import StrategyPackageManifest
from backend.services.strategy_package.repository import StrategyPackageRecord
from backend.services.trading_core.errors import DataUnavailableError, RuntimeConfigInvalidError


class StrategyPackageIdentityReader(Protocol):
    def get(self, package_id: str) -> StrategyPackageRecord: ...


class AdvisoryProgramIdentityReader(Protocol):
    def get_program(self, program_id: str) -> AdvisoryProgram: ...

    def list_binding_versions(self, program_id: str) -> list[AdvisoryStrategyBindingVersion]: ...


@dataclass(frozen=True)
class HistoricalRangeResolvedPackageV1:
    record: StrategyPackageRecord
    manifest: StrategyPackageManifest
    historical_projection: StrategyPackageHistoricalRangeInputProjectionV1
    admitted_projection: HistoricalRangeAdmittedPackageProjectionV1


class HistoricalRangeAdmittedPackageResolver:
    """Read exact admitted identity without repeating admission or health checks."""

    def __init__(self, *, package_reader: StrategyPackageIdentityReader) -> None:
        if package_reader is None:
            raise ValueError("package_reader is required")
        self._package_reader = package_reader

    def resolve(self, package_id: str) -> HistoricalRangeResolvedPackageV1:
        normalized = str(package_id or "").strip()
        if not normalized:
            raise ValueError("package_id is required")
        record = self._package_reader.get(normalized)
        manifest = record.current_manifest()
        if (
            record.package_id != normalized
            or manifest.package_id != normalized
            or manifest.package_version != record.package_version
            or manifest.manifest_sha256 != record.manifest_sha256
        ):
            raise RuntimeConfigInvalidError(
                "persisted StrategyPackage identity is internally inconsistent",
                context={"package_id": normalized},
            )
        historical = project_historical_range_inputs(manifest)
        component_by_id = {item.alpha_id: item for item in manifest.alpha_components}
        weights = dict(manifest.alpha_combination_policy.weights)
        if set(weights) != set(component_by_id):
            raise RuntimeConfigInvalidError(
                "admitted StrategyPackage weights do not exactly cover its components",
                context={"package_id": normalized},
            )
        components = tuple(
            HistoricalRangeAdmittedComponentV1(
                component_id=leg.alpha_component_id,
                weight=Decimal(str(weights[leg.alpha_component_id])),
                factor_order=leg.factor_order,
                required_window=get_strategy_package_inference_required_window(leg.factor_order),
                buffer_trading_days=5,
                runtime_input_identity_hash=canonical_json_sha256(
                    {
                        "query_contract_hash": historical.query_contract_hash,
                        "pit_universe_policy": historical.pit_universe_policy,
                        "pit_universe_ensure": historical.pit_universe_ensure,
                        "leg": leg.model_dump(mode="json"),
                    }
                ),
                lookback_contract_hash=canonical_json_sha256(
                    {
                        "component_id": leg.alpha_component_id,
                        "factor_order_hash": leg.factor_order_hash,
                        "required_window": get_strategy_package_inference_required_window(leg.factor_order),
                        "buffer_trading_days": 5,
                        "window_resolution": "trading_calendar",
                    }
                ),
            )
            for leg in historical.legs
        )
        admitted = HistoricalRangeAdmittedPackageProjectionV1(
            package_id=manifest.package_id,
            package_version=manifest.package_version,
            manifest_sha256=str(manifest.manifest_sha256),
            alpha_mode=HistoricalRangeAlphaMode(manifest.alpha_mode.value),
            components=components,
        )
        return HistoricalRangeResolvedPackageV1(
            record=record,
            manifest=manifest,
            historical_projection=historical,
            admitted_projection=admitted,
        )


class HistoricalRangeProgramResolver:
    def __init__(
        self,
        *,
        package_resolver: HistoricalRangeAdmittedPackageResolver,
        program_reader: AdvisoryProgramIdentityReader | None = None,
    ) -> None:
        self._package_resolver = package_resolver
        self._program_reader = program_reader

    def freeze_programs(
        self,
        *,
        request: HistoricalRangeResearchBatchRequestV1,
        code_release_id: str,
        code_release_hash: str,
        selection_semantics_version: str,
        selection_semantics_hash: str,
        list_semantics_version: str,
        list_semantics_hash: str,
    ) -> tuple[HistoricalRangeFrozenProgramV1, ...]:
        resolved_packages: dict[str, HistoricalRangeResolvedPackageV1] = {}
        return tuple(
            self._freeze_one(
                spec=spec,
                code_release_id=code_release_id,
                code_release_hash=code_release_hash,
                selection_semantics_version=selection_semantics_version,
                selection_semantics_hash=selection_semantics_hash,
                list_semantics_version=list_semantics_version,
                list_semantics_hash=list_semantics_hash,
                resolved_packages=resolved_packages,
            )
            for spec in request.program_specs
        )

    def _freeze_one(
        self,
        *,
        spec: ExistingProgramSpecV1 | ResearchProgramSpecV1,
        code_release_id: str,
        code_release_hash: str,
        selection_semantics_version: str,
        selection_semantics_hash: str,
        list_semantics_version: str,
        list_semantics_hash: str,
        resolved_packages: dict[str, HistoricalRangeResolvedPackageV1],
    ) -> HistoricalRangeFrozenProgramV1:
        if isinstance(spec, ExistingProgramSpecV1):
            program, binding = self._existing_program(spec)
            package_id = self._single_bound_package(binding)
            research_program_id = program.program_id
            source_program_id = program.program_id
            source_program_version = program.version
            source_binding_version_id = binding.binding_version_id
            runtime_config = dict(binding.runtime_config_json)
            review_policy = dict(program.review_policy)
            program_config = {
                "program_id": program.program_id,
                "program_version": program.version,
                "target_count": program.target_count,
                "entry_price_basis": program.entry_price_basis,
                "exit_price_basis": program.exit_price_basis,
                "binding_version_id": binding.binding_version_id,
                "runtime_config": runtime_config,
                "review_policy": review_policy,
            }
            style_profile_ref = None
            style_profile_hash = None
        else:
            package_id = spec.package_id
            research_program_id = spec.research_program_id
            source_program_id = None
            source_program_version = None
            source_binding_version_id = None
            runtime_config = dict(spec.runtime_config)
            review_policy = dict(spec.review_policy)
            program_config = spec.semantic_payload()
            style_profile_ref = spec.style_profile_ref
            style_profile_hash = spec.style_profile_hash
        resolved_package = resolved_packages.get(package_id)
        if resolved_package is None:
            resolved_package = self._package_resolver.resolve(package_id)
            resolved_packages[package_id] = resolved_package
        manifest = resolved_package.manifest
        projection = resolved_package.admitted_projection
        return HistoricalRangeFrozenProgramV1(
            research_program_id=research_program_id,
            source_program_id=source_program_id,
            source_program_version=source_program_version,
            source_binding_version_id=source_binding_version_id,
            package_id=manifest.package_id,
            package_version=manifest.package_version,
            manifest_sha256=str(manifest.manifest_sha256),
            alpha_mode=HistoricalRangeAlphaMode(manifest.alpha_mode.value),
            program_config=program_config,
            program_config_hash=canonical_json_sha256(program_config),
            runtime_config=runtime_config,
            runtime_config_hash=canonical_json_sha256(runtime_config),
            review_policy=review_policy,
            review_policy_hash=canonical_json_sha256(review_policy),
            style_profile_ref=style_profile_ref,
            style_profile_hash=style_profile_hash,
            code_release_id=str(code_release_id),
            code_release_hash=str(code_release_hash),
            selection_semantics_version=str(selection_semantics_version),
            selection_semantics_hash=str(selection_semantics_hash),
            list_semantics_version=str(list_semantics_version),
            list_semantics_hash=str(list_semantics_hash),
            target_package_asset_root_hash=_package_asset_root_hash(manifest),
            input_warmup_contract_hash=canonical_json_sha256(
                [
                    {
                        "component_id": item.component_id,
                        "lookback_contract_hash": item.lookback_contract_hash,
                    }
                    for item in projection.components
                ]
            ),
            admitted_package_projection_hash=canonical_json_sha256(projection.model_dump(mode="json")),
            admitted_package_projection=projection,
        )

    def _existing_program(
        self,
        spec: ExistingProgramSpecV1,
    ) -> tuple[AdvisoryProgram, AdvisoryStrategyBindingVersion]:
        if self._program_reader is None:
            raise DataUnavailableError(
                "existing Program resolution requires an Advisory Program identity reader",
                context={"program_id": spec.program_id},
            )
        program = self._program_reader.get_program(spec.program_id)
        if program.version != spec.expected_program_version:
            raise RuntimeConfigInvalidError(
                "existing Program version differs from the requested frozen identity",
                context={
                    "program_id": spec.program_id,
                    "expected_program_version": spec.expected_program_version,
                    "actual_program_version": program.version,
                },
            )
        matching = [
            item
            for item in self._program_reader.list_binding_versions(spec.program_id)
            if item.binding_version_id == spec.expected_binding_version_id
        ]
        if len(matching) != 1 or matching[0].program_version != program.version:
            raise RuntimeConfigInvalidError(
                "existing Program binding differs from the requested frozen identity",
                context={
                    "program_id": spec.program_id,
                    "expected_binding_version_id": spec.expected_binding_version_id,
                },
            )
        return program, matching[0]

    @staticmethod
    def _single_bound_package(binding: AdvisoryStrategyBindingVersion) -> str:
        package_ids = tuple(str(item or "").strip() for item in binding.package_ids)
        if len(package_ids) != 1 or not package_ids[0]:
            raise RuntimeConfigInvalidError(
                "each historical Program requires one single-Alpha package or one native multi-Alpha parent",
                context={"binding_version_id": binding.binding_version_id, "package_ids": list(package_ids)},
            )
        return package_ids[0]


def _package_asset_root_hash(manifest: StrategyPackageManifest) -> str:
    multi_alpha = (
        manifest.source_evidence.get("multi_alpha")
        if isinstance(manifest.source_evidence, dict)
        else None
    )
    return canonical_json_sha256(
        {
            "manifest_sha256": manifest.manifest_sha256,
            "factor_set": [item.model_dump(mode="json") for item in manifest.factor_set],
            "model_asset": (
                [item.model_dump(mode="json") for item in manifest.model_asset]
                if isinstance(manifest.model_asset, list)
                else manifest.model_asset.model_dump(mode="json")
            ),
            "runtime_assets": manifest.runtime_assets.model_dump(mode="json") if manifest.runtime_assets else None,
            "multi_alpha_legs": (
                multi_alpha.get("legs") if isinstance(multi_alpha, dict) else None
            ),
        }
    )
