"""Promote validated multi-alpha combine-backtest runs into StrategyPackages."""

from __future__ import annotations

import hashlib
import json
import math
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from backend.services.multi_alpha.combine_backtest import MultiAlphaCombineBacktestRepository
from backend.services.qe_archive.multi_alpha_provenance import MultiAlphaProvenanceResolver, SeedProvenance
from backend.services.trading_core.errors import DataUnavailableError, StrategyPackageValidationError, TradingCoreError

from .components import StrategyPackageComponentService
from .frozen_runtime_self_check import FrozenRuntimeSelfCheckService, attach_runtime_asset_admission
from .manifest import freeze_manifest
from .models import (
    AlphaCombinationPolicy,
    AlphaComponent,
    AlphaLineage,
    AlphaMode,
    AssetCheck,
    BacktestSummary,
    FactorAsset,
    ModelAsset,
    PackageStatus,
    RuntimeAssetManifest,
    SourceType,
    StrategyPackageComponentRecord,
    StrategyPackageManifest,
    StrategyPackageSource,
)
from .package_asset_freeze import PackageAssetFreezeService
from .qe_source_resolver import QEExperimentSourceResolver
from .repository import StrategyPackageRecord, StrategyPackageRepository
from .validators import StrategyPackageValidator


MULTI_ALPHA_PACKAGE_PROMOTE_CONFIRMATION = "MULTI_ALPHA_PACKAGE_PROMOTE"
MULTI_ALPHA_PROMOTION_PROVIDER_VERSION = "multi_alpha_package_promotion_v1"
MULTI_ALPHA_SIGNAL_ADMISSION_SCHEMA = "multi_alpha_signal_admission_v1"
MULTI_ALPHA_COMBINED_SIGNAL_SMOKE_SCHEMA = "multi_alpha_parent_combined_signal_smoke_v1"
SUPPORTED_P0_WEIGHTING_SCHEME = "ic_weighted"


@dataclass(frozen=True)
class MultiAlphaPackagePromotionResult:
    package: StrategyPackageRecord
    components: list[StrategyPackageComponentRecord]
    paper_admission: dict[str, Any]
    source_run_id: str
    auto_component_materialization: list[dict[str, Any]] = field(default_factory=list)

    def to_response(self) -> dict[str, Any]:
        return {
            "ok": True,
            "package_id": self.package.package_id,
            "alpha_mode": self.package.alpha_mode.value,
            "manifest_sha256": self.package.manifest_sha256,
            "source_run_id": self.source_run_id,
            "paper_admission": self.paper_admission,
            "auto_component_materialization": self.auto_component_materialization,
            "package": {
                "package_id": self.package.package_id,
                "package_name": self.package.package_name,
                "package_version": self.package.package_version,
                "package_status": self.package.package_status.value,
                "alpha_mode": self.package.alpha_mode.value,
                "manifest_sha256": self.package.manifest_sha256,
                "prediction_ref_uri": self.package.prediction_ref_uri,
                "prediction_ref_sha256": self.package.prediction_ref_sha256,
            },
            "components": [component.model_dump(mode="json") for component in self.components],
        }


@dataclass(frozen=True)
class _ParentLegAssetPlan:
    leg_id: str
    seed_run_ids: tuple[str, ...]
    terminal_weight: float
    manifest: StrategyPackageManifest
    seed_provenance: tuple[SeedProvenance, ...]
    materialization: dict[str, Any] = field(default_factory=dict)


class MultiAlphaPackagePromotionService:
    """P0 package/signal-layer promotion; no Paper execution or live inference."""

    def __init__(
        self,
        *,
        combine_repository: MultiAlphaCombineBacktestRepository | Any | None = None,
        package_repository: StrategyPackageRepository | Any | None = None,
        component_service: StrategyPackageComponentService | None = None,
        validator: StrategyPackageValidator | None = None,
        provenance_resolver: MultiAlphaProvenanceResolver | Any | None = None,
        source_resolver: QEExperimentSourceResolver | None = None,
        asset_freezer: PackageAssetFreezeService | None = None,
        frozen_runtime_self_check: FrozenRuntimeSelfCheckService | Any | None = None,
        prediction_ref_roots: Sequence[str | Path] | None = None,
    ) -> None:
        self.combine_repository = combine_repository or MultiAlphaCombineBacktestRepository()
        if component_service is not None:
            self.component_service = component_service
            self.package_repository = package_repository or component_service.repository
        else:
            self.package_repository = package_repository or StrategyPackageRepository()
        self.validator = validator or StrategyPackageValidator()
        self.provenance_resolver = provenance_resolver or MultiAlphaProvenanceResolver()
        self.source_resolver = source_resolver or QEExperimentSourceResolver()
        self.asset_freezer = asset_freezer or PackageAssetFreezeService()
        self.frozen_runtime_self_check = frozen_runtime_self_check or FrozenRuntimeSelfCheckService(
            asset_store=getattr(self.asset_freezer, "asset_store", None)
        )
        if component_service is None:
            self.component_service = StrategyPackageComponentService(
                repository=self.package_repository,
                asset_freezer=self.asset_freezer,
                frozen_runtime_self_check=self.frozen_runtime_self_check,
            )
        self.prediction_ref_roots = tuple(Path(root) for root in (prediction_ref_roots or ()))

    def promote_from_combine_run(
        self,
        *,
        combine_backtest_run_id: str,
        weighting_scheme: str,
        topk: int,
        confirmation: str,
        component_package_ids: Mapping[str, str] | None = None,
        weight_policy: Mapping[str, Any],
        scheme_result_id: str | None = None,
        secondary_topk: Sequence[int] | None = None,
        package_name: str | None = None,
        promotion_gate: Mapping[str, Any] | None = None,
    ) -> MultiAlphaPackagePromotionResult:
        run_id = _required_text(combine_backtest_run_id, "combine_backtest_run_id")
        scheme = _required_text(weighting_scheme, "weighting_scheme")
        if confirmation != MULTI_ALPHA_PACKAGE_PROMOTE_CONFIRMATION:
            _fail_manifest_incomplete(
                "confirmation token is required for multi-alpha package promotion",
                confirmation=confirmation,
                expected_confirmation=MULTI_ALPHA_PACKAGE_PROMOTE_CONFIRMATION,
            )
        if scheme != SUPPORTED_P0_WEIGHTING_SCHEME:
            _fail(
                "P0 multi-alpha package promotion only supports ic_weighted",
                reason_code="multi_alpha_scheme_not_succeeded",
                weighting_scheme=scheme,
                supported_weighting_schemes=[SUPPORTED_P0_WEIGHTING_SCHEME],
            )
        try:
            topk_value = int(topk)
        except (TypeError, ValueError) as exc:
            raise StrategyPackageValidationError(
                "topk must be an integer",
                context={"reason_code": "multi_alpha_manifest_incomplete", "topk": topk},
            ) from exc
        if topk_value <= 0:
            _fail_manifest_incomplete("topk must be positive", topk=topk)
        try:
            secondary_topk_values = [int(value) for value in (secondary_topk or [])]
        except (TypeError, ValueError) as exc:
            raise StrategyPackageValidationError(
                "secondary_topk values must be integers",
                context={"reason_code": "multi_alpha_manifest_incomplete", "secondary_topk": list(secondary_topk or [])},
            ) from exc
        if any(value <= 0 for value in secondary_topk_values):
            _fail_manifest_incomplete("secondary_topk values must be positive", secondary_topk=secondary_topk_values)

        bundle = self.combine_repository.get_run(run_id)
        if bundle is None:
            raise DataUnavailableError(
                "multi-alpha combine-backtest run does not exist",
                context={"reason_code": "multi_alpha_combine_run_missing", "run_id": run_id},
            )
        run = _as_mapping(bundle.get("run"), field_name="run")
        if str(run.get("status") or "").lower() != "succeeded":
            _fail(
                "multi-alpha combine-backtest run is not succeeded",
                reason_code="multi_alpha_scheme_not_succeeded",
                run_id=run_id,
                run_status=run.get("status"),
                run_reason=run.get("reason"),
            )

        roster = _normalize_roster(run.get("roster_json"), run_id=run_id)
        scheme_result = _select_scheme_result(
            bundle.get("scheme_results") or [],
            run_id=run_id,
            weighting_scheme=scheme,
            scheme_result_id=scheme_result_id,
        )
        _validate_scheme_succeeded(scheme_result, run_id=run_id, weighting_scheme=scheme)
        _validate_promotion_gate(scheme_result, promotion_gate or {}, run_id=run_id, weighting_scheme=scheme)

        requested_component_ids = _normalize_component_package_ids(component_package_ids)
        if requested_component_ids:
            _fail(
                "component_package_ids are not supported for parent-only multi-alpha promotion",
                reason_code="multi_alpha_promotion_component_package_ids_unsupported",
                run_id=run_id,
                roster_leg_ids=[item["leg_id"] for item in roster],
                requested_leg_ids=sorted(requested_component_ids),
            )
        prediction_ref = _extract_prediction_ref(
            scheme_result,
            run=run,
            workspace_roots=self.prediction_ref_roots,
        )
        weights = _extract_terminal_weights(
            scheme_result.get("weights_json"),
            expected_leg_ids=[item["leg_id"] for item in roster],
            run_id=run_id,
            weighting_scheme=scheme,
        )

        leg_evidence = [
            self._prepare_parent_leg_asset_plan(
                leg_id=item["leg_id"],
                seed_run_ids=tuple(item["seed_run_ids"]),
                terminal_weight=weights[item["leg_id"]],
                run_id=run_id,
            )
            for item in roster
        ]
        _assert_unique_parent_leg_model_ids(leg_evidence, run_id=run_id)
        backtest_config = _as_mapping(run.get("backtest_config_json") or {}, field_name="backtest_config_json")
        strategy_snapshot = _build_strategy_snapshot(
            topk=topk_value,
            secondary_topk=secondary_topk_values,
            backtest_config=backtest_config,
        )
        normalized_weight_policy = _validate_weight_policy(weight_policy, run_id=run_id, weighting_scheme=scheme)
        scheme_result_key = str(scheme_result.get("id") or scheme_result_id or scheme)
        package_id = _stable_package_id(
            run_id=run_id,
            weighting_scheme=scheme,
            topk=topk_value,
            leg_evidence=leg_evidence,
            prediction_ref=prediction_ref,
            weight_policy=normalized_weight_policy,
            scheme_result_id=scheme_result_key,
        )
        manifest = self._build_manifest(
            package_id=package_id,
            package_name=package_name or _default_package_name(leg_evidence, scheme, topk_value, package_id),
            run=run,
            scheme_result=scheme_result,
            scheme_result_id=scheme_result_key,
            weighting_scheme=scheme,
            topk=topk_value,
            secondary_topk=secondary_topk_values,
            leg_evidence=leg_evidence,
            weights=weights,
            prediction_ref=prediction_ref,
            weight_policy=normalized_weight_policy,
            strategy_snapshot=strategy_snapshot,
        )
        frozen = freeze_manifest(manifest)
        self.validator.validate_manifest(frozen)
        frozen_assets = self.asset_freezer.freeze_manifest_assets(frozen)
        self_check_result = self.frozen_runtime_self_check.assert_manifest_self_contained(frozen_assets.manifest)
        parent_manifest = _with_signal_admission_evidence(
            frozen_assets.manifest,
            self_check_result,
            provider_version=MULTI_ALPHA_PROMOTION_PROVIDER_VERSION,
        )
        parent_manifest = attach_runtime_asset_admission(parent_manifest, self_check_result)
        self.validator.validate_manifest(parent_manifest)
        parent = self.package_repository.save_manifest_with_assets(parent_manifest, frozen_assets.assets)
        parent = self.package_repository.update_artifact_refs(
            parent.package_id,
            prediction_ref_uri=prediction_ref["uri"],
            prediction_ref_sha256=prediction_ref["sha256"],
        )
        return MultiAlphaPackagePromotionResult(
            package=parent,
            components=[],
            source_run_id=run_id,
            paper_admission=_paper_admission(),
            auto_component_materialization=[_jsonable(leg.materialization) for leg in leg_evidence],
        )

    def _prepare_parent_leg_asset_plan(
        self,
        *,
        leg_id: str,
        seed_run_ids: tuple[str, ...],
        terminal_weight: float,
        run_id: str,
    ) -> _ParentLegAssetPlan:
        if terminal_weight <= 0 or not math.isfinite(terminal_weight):
            _fail_manifest_incomplete(
                "terminal component weight must be positive and finite",
                run_id=run_id,
                leg_id=leg_id,
                component_weight=terminal_weight,
            )
        seed_sources = self._resolve_leg_seed_sources(leg_id=leg_id, seed_run_ids=seed_run_ids, run_id=run_id)
        manifest = self._build_parent_leg_asset_manifest(
            leg_id=leg_id,
            seed_run_ids=seed_run_ids,
            seed_sources=seed_sources,
            run_id=run_id,
        )
        frozen_assets = self.asset_freezer.freeze_manifest_assets(manifest)
        self.frozen_runtime_self_check.assert_manifest_self_contained(frozen_assets.manifest)
        component = frozen_assets.manifest.alpha_components[0]
        model_assets = frozen_assets.manifest.model_asset if isinstance(frozen_assets.manifest.model_asset, list) else [frozen_assets.manifest.model_asset]
        model = model_assets[0] if model_assets else None
        return _ParentLegAssetPlan(
            leg_id=leg_id,
            seed_run_ids=seed_run_ids,
            terminal_weight=terminal_weight,
            manifest=frozen_assets.manifest,
            seed_provenance=tuple(seed_sources),
            materialization={
                "leg_id": leg_id,
                "mode": "parent_leg_inlined_package_asset",
                "seed_run_ids": list(seed_run_ids),
                "seed_source_count": len(seed_sources),
                "model_id": component.model_id,
                "model_asset_sha256": getattr(model, "sha256", None),
                "factor_count": len(frozen_assets.manifest.factor_set),
                "alpha158_enabled": bool(
                    frozen_assets.manifest.runtime_assets
                    and frozen_assets.manifest.runtime_assets.alpha158.enabled
                ),
                "alpha158_schema_sha256": (
                    frozen_assets.manifest.runtime_assets.alpha158.sha256
                    if frozen_assets.manifest.runtime_assets
                    and frozen_assets.manifest.runtime_assets.alpha158.enabled
                    else None
                ),
            },
        )

    def _resolve_leg_seed_sources(
        self,
        *,
        leg_id: str,
        seed_run_ids: tuple[str, ...],
        run_id: str,
    ) -> list[SeedProvenance]:
        resolved: list[SeedProvenance] = []
        for seed_ref in seed_run_ids:
            try:
                provenance = self.provenance_resolver.resolve_seed(seed_ref)
            except TradingCoreError as exc:
                raise StrategyPackageValidationError(
                    "multi-alpha leg seed provenance resolver failed",
                    context={
                        "reason_code": "multi_alpha_seed_unresolved",
                        "run_id": run_id,
                        "leg_id": leg_id,
                        "seed_ref": seed_ref,
                        "upstream_error_code": exc.error_code,
                        "upstream_context": exc.context,
                    },
                ) from exc
            except Exception as exc:
                raise StrategyPackageValidationError(
                    "multi-alpha leg seed provenance resolver failed",
                    context={
                        "reason_code": "multi_alpha_seed_unresolved",
                        "run_id": run_id,
                        "leg_id": leg_id,
                        "seed_ref": seed_ref,
                        "upstream_error_type": type(exc).__name__,
                        "upstream_error": str(exc),
                    },
                ) from exc
            if not provenance.resolved:
                _fail(
                    "multi-alpha leg seed could not be resolved to QE provenance",
                    reason_code="multi_alpha_seed_unresolved",
                    run_id=run_id,
                    leg_id=leg_id,
                    seed_ref=seed_ref,
                    seed_ref_kind=provenance.seed_ref_kind,
                    resolve_method=provenance.resolve_method,
                    resolve_note=provenance.resolve_note,
                )
            if not (provenance.source_task_id and _resolver_loop_id(provenance)) and not provenance.source_experiment_id:
                _fail(
                    "multi-alpha leg seed provenance lacks QE source coordinates",
                    reason_code="multi_alpha_seed_source_incomplete",
                    run_id=run_id,
                    leg_id=leg_id,
                    seed_ref=seed_ref,
                    provenance=provenance.to_meta(),
                )
            resolved.append(provenance)
        if not resolved:
            _fail("roster item requires seed_run_ids", reason_code="multi_alpha_roster_mismatch", run_id=run_id, leg_id=leg_id)
        return resolved

    def _build_parent_leg_asset_manifest(
        self,
        *,
        leg_id: str,
        seed_run_ids: tuple[str, ...],
        seed_sources: Sequence[SeedProvenance],
        run_id: str,
    ) -> StrategyPackageManifest:
        primary = seed_sources[0]
        try:
            loop_id = _resolver_loop_id(primary)
            if primary.source_task_id and loop_id:
                base = self.source_resolver.build_from_evolution_loop(
                    qe_task_id=primary.source_task_id,
                    qe_loop_id=loop_id,
                )
                base_source = {
                    "source_type": SourceType.QE_EVOLUTION_LOOP.value,
                    "source_id": primary.source_task_id,
                    "loop_id": loop_id,
                    "experiment_id": primary.source_experiment_id,
                }
            elif primary.source_experiment_id:
                base = self.source_resolver.build_from_experiment(primary.source_experiment_id)
                base_source = {
                    "source_type": SourceType.QE_EXPERIMENT.value,
                    "source_id": primary.source_experiment_id,
                    "loop_id": None,
                    "experiment_id": primary.source_experiment_id,
                }
            else:
                _fail(
                    "multi-alpha leg seed provenance lacks QE source coordinates",
                    reason_code="multi_alpha_seed_source_incomplete",
                    run_id=run_id,
                    leg_id=leg_id,
                    seed_ref=primary.seed_ref,
                    provenance=primary.to_meta(),
                )
        except StrategyPackageValidationError as exc:
            if exc.context.get("reason_code", "").startswith("multi_alpha_"):
                raise
            raise StrategyPackageValidationError(
                "failed to materialize multi-alpha parent leg assets from QE source",
                context={
                    "reason_code": "multi_alpha_component_auto_materialize_failed",
                    "run_id": run_id,
                    "leg_id": leg_id,
                    "seed_ref": primary.seed_ref,
                    "source_task_id": primary.source_task_id,
                    "source_loop_id": primary.source_loop_id,
                    "source_experiment_id": primary.source_experiment_id,
                    "upstream_error_code": exc.error_code,
                    "upstream_context": exc.context,
                },
            ) from exc
        except DataUnavailableError as exc:
            raise StrategyPackageValidationError(
                "failed to materialize multi-alpha parent leg assets from QE source",
                context={
                    "reason_code": "multi_alpha_component_auto_materialize_failed",
                    "run_id": run_id,
                    "leg_id": leg_id,
                    "seed_ref": primary.seed_ref,
                    "source_task_id": primary.source_task_id,
                    "source_loop_id": primary.source_loop_id,
                    "source_experiment_id": primary.source_experiment_id,
                    "upstream_error_code": exc.error_code,
                    "upstream_context": exc.context,
                },
            ) from exc
        except TradingCoreError as exc:
            raise StrategyPackageValidationError(
                "failed to materialize multi-alpha parent leg assets from QE source",
                context={
                    "reason_code": "multi_alpha_component_auto_materialize_failed",
                    "run_id": run_id,
                    "leg_id": leg_id,
                    "seed_ref": primary.seed_ref,
                    "source_task_id": primary.source_task_id,
                    "source_loop_id": primary.source_loop_id,
                    "source_experiment_id": primary.source_experiment_id,
                    "upstream_error_code": exc.error_code,
                    "upstream_context": exc.context,
                },
            ) from exc
        except Exception as exc:
            raise StrategyPackageValidationError(
                "failed to materialize multi-alpha parent leg assets from QE source",
                context={
                    "reason_code": "multi_alpha_component_auto_materialize_failed",
                    "run_id": run_id,
                    "leg_id": leg_id,
                    "seed_ref": primary.seed_ref,
                    "source_task_id": primary.source_task_id,
                    "source_loop_id": primary.source_loop_id,
                    "source_experiment_id": primary.source_experiment_id,
                    "upstream_error_type": type(exc).__name__,
                    "upstream_error": str(exc),
                },
            ) from exc

        if base.alpha_mode != AlphaMode.SINGLE_ALPHA:
            _fail(
                "parent leg asset source must resolve to a single-alpha QE manifest",
                reason_code="multi_alpha_component_auto_materialize_failed",
                run_id=run_id,
                leg_id=leg_id,
                seed_ref=primary.seed_ref,
                source_alpha_mode=base.alpha_mode.value,
            )
        seed_digest = _seed_roster_digest(leg_id=leg_id, seed_run_ids=seed_run_ids, seed_sources=seed_sources)
        package_id = _stable_parent_leg_asset_id(run_id=run_id, leg_id=leg_id, seed_digest=seed_digest)
        component = base.alpha_components[0].model_copy(
            update={
                "alpha_id": leg_id,
                "alpha_name": leg_id,
                "component_weight": 1.0,
            }
        )
        source_evidence = dict(base.source_evidence or {})
        source_evidence["seed_run_ids"] = list(seed_run_ids)
        source_evidence["multi_alpha_parent_leg_asset"] = {
            "schema_version": "multi_alpha_parent_leg_asset_materialization_v1",
            "component_materialization": "parent_leg_inline_from_combine_roster",
            "combine_backtest_run_id": run_id,
            "leg_id": leg_id,
            "seed_run_ids": list(seed_run_ids),
            "seed_roster_digest": seed_digest,
            "primary_qe_source": base_source,
            "seed_provenance": [source.to_meta() for source in seed_sources],
        }
        return freeze_manifest(
            base.model_copy(
                update={
                    "package_id": package_id,
                    "package_name": _default_parent_leg_asset_name(leg_id, run_id, package_id),
                    "source": StrategyPackageSource(
                        source_type=SourceType.MULTI_ALPHA_COMBINE_RUN,
                        source_id=run_id,
                        loop_id=f"component:{leg_id}:{seed_digest[:16]}",
                        run_id=run_id,
                        created_at=base.source.created_at,
                    ),
                    "alpha_components": [component],
                    "alpha_combination_policy": AlphaCombinationPolicy(method="identity", weights={leg_id: 1.0}),
                    "source_evidence": _jsonable(source_evidence),
                    "manifest_sha256": None,
                }
            )
        )

    def _build_manifest(
        self,
        *,
        package_id: str,
        package_name: str,
        run: Mapping[str, Any],
        scheme_result: Mapping[str, Any],
        scheme_result_id: str,
        weighting_scheme: str,
        topk: int,
        secondary_topk: list[int],
        leg_evidence: Sequence[_ParentLegAssetPlan],
        weights: Mapping[str, float],
        prediction_ref: Mapping[str, str],
        weight_policy: Mapping[str, Any],
        strategy_snapshot: Mapping[str, Any],
    ) -> StrategyPackageManifest:
        factor_set = _merge_factor_assets([leg.manifest for leg in leg_evidence])
        model_assets = _merge_model_assets([leg.manifest for leg in leg_evidence])
        runtime_assets = _merge_runtime_assets([leg.manifest for leg in leg_evidence], package_id=package_id)
        if not factor_set or not model_assets:
            _fail_manifest_incomplete(
                "multi-alpha parent manifest requires per-leg factor_set and model_asset",
                package_id=package_id,
                factor_count=len(factor_set),
                model_asset_count=len(model_assets),
            )
        components = [_component_from_leg_plan(leg) for leg in leg_evidence]
        source_evidence = {
            "schema_version": "multi_alpha_package_promotion_source_evidence_v1",
            "authority": "parent_package_asset_runtime_authority",
            "multi_alpha": {
                "source_type": "multi_alpha_combine_backtest",
                "combine_backtest_run_id": run["id"],
                "weighting_scheme": weighting_scheme,
                "scheme_result_id": scheme_result_id,
                "runtime_provider_version": MULTI_ALPHA_PROMOTION_PROVIDER_VERSION,
                "combined_prediction_ref_uri": prediction_ref["uri"],
                "combined_prediction_ref_sha256": prediction_ref["sha256"],
                "combined_prediction_ref_source": prediction_ref.get("ref_source") or "explicit_scheme_result",
                "weight_policy": dict(weight_policy),
                "terminal_weights": dict(weights),
                "per_window_weights": _jsonable(scheme_result.get("per_window_weights_json") or []),
                "legs": [
                    {
                        "leg_id": leg.leg_id,
                        "seed_run_ids": list(leg.seed_run_ids),
                        "ensemble_method": "mean_by_trade_date_instrument",
                        "terminal_weight": leg.terminal_weight,
                        "model_id": leg.manifest.alpha_components[0].model_id,
                        "factor_artifact_refs": list(
                            leg.manifest.alpha_components[0].lineage.factor_artifact_refs
                            or leg.manifest.alpha_components[0].factor_ids
                        ),
                        "runtime_assets": (
                            leg.manifest.runtime_assets.model_dump(mode="json")
                            if leg.manifest.runtime_assets is not None
                            else RuntimeAssetManifest().model_dump(mode="json")
                        ),
                        "seed_provenance": [source.to_meta() for source in leg.seed_provenance],
                    }
                    for leg in leg_evidence
                ],
                "paper_runtime_diagnostics": _paper_admission(),
            },
        }
        backtest_context = {
            "schema_version": "multi_alpha_backtest_context_v1",
            "authority": "source_evidence_not_runtime_authority",
            "combine_backtest_run_id": run["id"],
            "oos_start": _jsonable(run.get("oos_start")),
            "oos_end": _jsonable(run.get("oos_end")),
            "daily_strategy": {
                "topk": topk,
                "secondary_topk": secondary_topk,
                "n_drop": strategy_snapshot["n_drop"],
                "topk_variants": sorted(set([topk, *secondary_topk])),
            },
            "universe": {
                "stock_pool": strategy_snapshot["stock_pool"],
                "filtered_pool": strategy_snapshot["filtered_pool"],
            },
            "label": {"label_horizon": strategy_snapshot["label_horizon"]},
            "execution": {"execution_algo": strategy_snapshot["execution_algo"]},
            "weight_policy": dict(weight_policy),
            "runtime_provider_version": MULTI_ALPHA_PROMOTION_PROVIDER_VERSION,
            "raw_backtest_config": _jsonable(run.get("backtest_config_json") or {}),
        }
        return StrategyPackageManifest(
            manifest_version="alpha_core_v1",
            package_id=package_id,
            package_name=package_name,
            package_version="1.0.0",
            source=StrategyPackageSource(
                source_type=SourceType.MULTI_ALPHA_COMBINE_RUN,
                source_id=str(run["id"]),
                loop_id=f"{scheme_result_id}:topk{topk}",
                run_id=str(run["id"]),
                created_at=_stable_created_at(run.get("created_at")),
            ),
            alpha_mode=AlphaMode.MULTI_ALPHA,
            alpha_components=components,
            alpha_combination_policy=AlphaCombinationPolicy(
                method=weighting_scheme,
                weights=dict(weights),
                normalization_scope="per_trade_date_universe",
                conflict_resolution="weighted_sum",
            ),
            factor_set=factor_set,
            model_asset=model_assets,
            runtime_assets=runtime_assets,
            source_evidence=source_evidence,
            backtest_context=backtest_context,
            backtest_summary=BacktestSummary(
                annual_return=_float_or_none(scheme_result.get("cagr")),
                max_drawdown=_float_or_none(scheme_result.get("max_drawdown")),
                raw_metrics={
                    key: _jsonable(scheme_result.get(key))
                    for key in (
                        "cagr",
                        "max_drawdown",
                        "sharpe",
                        "calmar",
                        "topk_return_20",
                        "topk_hit_rate_20",
                        "turnover",
                        "vs_baseline_sharpe_delta",
                        "vs_baseline_calmar_delta",
                    )
                    if key in scheme_result
                },
            ),
            asset_checks=[
                AssetCheck(
                    check_name="multi_alpha_package_promotion_p0",
                    passed=True,
                    message=(
                        "MULTI_ALPHA manifest frozen from validated combine-backtest evidence; "
                        "signal admission is governed by frozen self-check and deterministic selection evidence. "
                        "Paper runtime dry-run is optional diagnostic evidence."
                    ),
                    context={
                        "signal_admission_required": True,
                        "dry_run_required_for_signal_admission": False,
                    },
                )
            ],
            package_status=PackageStatus.ASSET_VALIDATED,
        )


def _fail(message: str, *, reason_code: str, **context: Any) -> None:
    raise StrategyPackageValidationError(message, context={"reason_code": reason_code, **context})


def _fail_manifest_incomplete(message: str, **context: Any) -> None:
    _fail(message, reason_code="multi_alpha_manifest_incomplete", **context)


def _required_text(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        _fail_manifest_incomplete(f"{field_name} is required", field_name=field_name)
    return text


def _as_mapping(value: Any, *, field_name: str) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError as exc:
            raise StrategyPackageValidationError(
                f"{field_name} must be JSON object compatible",
                context={"reason_code": "multi_alpha_manifest_incomplete", "field": field_name},
            ) from exc
        if isinstance(parsed, Mapping):
            return dict(parsed)
    _fail_manifest_incomplete(f"{field_name} must be an object", field_name=field_name, value_type=type(value).__name__)
    raise AssertionError("unreachable")


def _normalize_roster(value: Any, *, run_id: str) -> list[dict[str, Any]]:
    if isinstance(value, str) and value.strip():
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise StrategyPackageValidationError(
                "combine-backtest roster_json must be valid JSON",
                context={"reason_code": "multi_alpha_roster_mismatch", "run_id": run_id},
            ) from exc
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        _fail(
            "combine-backtest roster_json must be a list",
            reason_code="multi_alpha_roster_mismatch",
            run_id=run_id,
            roster_type=type(value).__name__,
        )
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, Mapping):
            _fail("roster item must be an object", reason_code="multi_alpha_roster_mismatch", run_id=run_id)
        leg_id = str(item.get("leg_id") or item.get("id") or "").strip()
        seeds_raw = item.get("seed_run_ids") or item.get("run_ids") or []
        seed_run_ids = tuple(str(seed or "").strip() for seed in seeds_raw if str(seed or "").strip())
        if not leg_id or not seed_run_ids:
            _fail(
                "roster item requires leg_id and seed_run_ids",
                reason_code="multi_alpha_roster_mismatch",
                run_id=run_id,
                leg_id=leg_id,
            )
        if leg_id in seen:
            _fail("roster leg_id must be unique", reason_code="multi_alpha_roster_mismatch", run_id=run_id, leg_id=leg_id)
        seen.add(leg_id)
        normalized.append({"leg_id": leg_id, "seed_run_ids": seed_run_ids, "metadata": dict(item.get("metadata") or {})})
    if len(normalized) < 2:
        _fail("MULTI_ALPHA promotion requires at least two legs", reason_code="multi_alpha_roster_mismatch", run_id=run_id)
    return normalized


def _select_scheme_result(
    rows: Sequence[Any],
    *,
    run_id: str,
    weighting_scheme: str,
    scheme_result_id: str | None,
) -> dict[str, Any]:
    candidates = [_as_mapping(row, field_name="scheme_result") for row in rows]
    if scheme_result_id:
        candidates = [
            row
            for row in candidates
            if str(row.get("id") or row.get("scheme_result_id") or "").strip() == scheme_result_id
        ]
    else:
        candidates = [row for row in candidates if str(row.get("weighting_scheme") or "").strip() == weighting_scheme]
    if len(candidates) != 1:
        _fail(
            "expected exactly one successful combine-backtest scheme_result",
            reason_code="multi_alpha_scheme_not_succeeded",
            run_id=run_id,
            weighting_scheme=weighting_scheme,
            scheme_result_id=scheme_result_id,
            matched_count=len(candidates),
        )
    return candidates[0]


def _validate_scheme_succeeded(row: Mapping[str, Any], *, run_id: str, weighting_scheme: str) -> None:
    if str(row.get("weighting_scheme") or "") != weighting_scheme:
        _fail(
            "scheme_result weighting_scheme mismatch",
            reason_code="multi_alpha_scheme_not_succeeded",
            run_id=run_id,
            expected_weighting_scheme=weighting_scheme,
            actual_weighting_scheme=row.get("weighting_scheme"),
        )
    if bool(row.get("skipped")):
        _fail(
            "scheme_result is skipped",
            reason_code="multi_alpha_scheme_not_succeeded",
            run_id=run_id,
            weighting_scheme=weighting_scheme,
            skipped_reason=row.get("skipped_reason"),
        )
    missing = [key for key in ("cagr", "max_drawdown", "sharpe", "calmar") if not _is_finite_number(row.get(key))]
    if missing:
        _fail(
            "scheme_result metrics are incomplete",
            reason_code="multi_alpha_scheme_not_succeeded",
            run_id=run_id,
            weighting_scheme=weighting_scheme,
            missing_or_non_finite_metrics=missing,
        )


def _validate_promotion_gate(
    row: Mapping[str, Any],
    gate: Mapping[str, Any],
    *,
    run_id: str,
    weighting_scheme: str,
) -> None:
    for raw_key, raw_threshold in gate.items():
        key = str(raw_key)
        metric_key = key[4:] if key.startswith("min_") else key
        if not _is_finite_number(raw_threshold):
            _fail_manifest_incomplete(
                "promotion_gate threshold must be finite",
                run_id=run_id,
                weighting_scheme=weighting_scheme,
                gate_key=key,
                threshold=raw_threshold,
            )
        actual = row.get(metric_key)
        if not _is_finite_number(actual) or float(actual) < float(raw_threshold):
            _fail(
                "scheme_result metrics are below promotion gate",
                reason_code="multi_alpha_metrics_below_gate",
                run_id=run_id,
                weighting_scheme=weighting_scheme,
                metric=metric_key,
                actual=actual,
                required_min=float(raw_threshold),
            )


def _extract_terminal_weights(value: Any, *, expected_leg_ids: Sequence[str], run_id: str, weighting_scheme: str) -> dict[str, float]:
    payload = _as_mapping(value or {}, field_name="weights_json")
    source = payload.get("leg_weights") if isinstance(payload.get("leg_weights"), Mapping) else payload
    weights: dict[str, float] = {}
    for leg_id in expected_leg_ids:
        if leg_id not in source:
            _fail(
                "weights_json missing terminal leg weight",
                reason_code="multi_alpha_roster_mismatch",
                run_id=run_id,
                weighting_scheme=weighting_scheme,
                leg_id=leg_id,
                expected_leg_ids=list(expected_leg_ids),
                actual_leg_ids=sorted(str(key) for key in source),
            )
        try:
            weight = float(source[leg_id])
        except (TypeError, ValueError) as exc:
            raise StrategyPackageValidationError(
                "terminal leg weight must be numeric",
                context={
                    "reason_code": "multi_alpha_manifest_incomplete",
                    "run_id": run_id,
                    "weighting_scheme": weighting_scheme,
                    "leg_id": leg_id,
                    "value": source[leg_id],
                },
            ) from exc
        if weight <= 0 or not math.isfinite(weight):
            _fail_manifest_incomplete(
                "terminal leg weight must be positive and finite",
                run_id=run_id,
                weighting_scheme=weighting_scheme,
                leg_id=leg_id,
                value=source[leg_id],
            )
        weights[leg_id] = weight
    return weights


def _extract_prediction_ref(
    row: Mapping[str, Any],
    *,
    run: Mapping[str, Any],
    workspace_roots: Sequence[Path] = (),
) -> dict[str, str]:
    candidates: list[Any] = [
        row.get("combined_prediction_ref"),
        row.get("prediction_ref"),
        {
            "uri": row.get("combined_prediction_ref_uri") or row.get("prediction_ref_uri"),
            "sha256": row.get("combined_prediction_ref_sha256") or row.get("prediction_ref_sha256"),
        },
    ]
    weights = row.get("weights_json") or {}
    if isinstance(weights, str) and weights.strip():
        try:
            weights = json.loads(weights)
        except json.JSONDecodeError as exc:
            raise StrategyPackageValidationError(
                "scheme_result weights_json must be valid JSON",
                context={"reason_code": "multi_alpha_manifest_incomplete", "weighting_scheme": row.get("weighting_scheme")},
            ) from exc
    if isinstance(weights, Mapping):
        candidates.extend(
            [
                weights.get("combined_prediction_ref"),
                weights.get("prediction_ref"),
                {
                    "uri": weights.get("combined_prediction_ref_uri") or weights.get("prediction_ref_uri"),
                    "sha256": weights.get("combined_prediction_ref_sha256") or weights.get("prediction_ref_sha256"),
                },
                weights.get("prediction_store_manifest"),
            ]
        )
    for candidate in candidates:
        ref = _prediction_ref_from_candidate(candidate)
        if ref:
            return ref

    workspace_ref, attempted_paths = _workspace_prediction_ref(
        run=run,
        weighting_scheme=str(row.get("weighting_scheme") or "").strip(),
        workspace_roots=workspace_roots,
    )
    if workspace_ref is not None:
        return workspace_ref
    _fail(
        "combined prediction ref is required for multi-alpha package promotion",
        reason_code="multi_alpha_prediction_ref_missing",
        run_id=run.get("id"),
        weighting_scheme=row.get("weighting_scheme"),
        pred_persisted=row.get("pred_persisted"),
        attempted_local_prediction_paths=attempted_paths,
    )
    raise AssertionError("unreachable")


def _prediction_ref_from_candidate(candidate: Any) -> dict[str, str] | None:
    if not isinstance(candidate, Mapping):
        return None
    artifacts = candidate.get("artifacts")
    if isinstance(artifacts, Sequence) and not isinstance(artifacts, (str, bytes)):
        for item in artifacts:
            if isinstance(item, Mapping) and str(item.get("artifact_type") or item.get("type") or "") == "prediction":
                return _prediction_ref_from_candidate(item)
    uri = str(candidate.get("uri") or candidate.get("prediction_ref_uri") or "").strip()
    sha = str(candidate.get("sha256") or candidate.get("prediction_ref_sha256") or "").strip().lower()
    if uri and _is_sha256(sha):
        return {"uri": uri, "sha256": sha}
    return None


def _workspace_prediction_ref(
    *,
    run: Mapping[str, Any],
    weighting_scheme: str,
    workspace_roots: Sequence[Path],
) -> tuple[dict[str, str] | None, list[str]]:
    run_id = str(run.get("id") or "").strip()
    scheme = str(weighting_scheme or "").strip()
    if not run_id or not scheme:
        return None, []
    attempted: list[str] = []
    backtest_name = f"combined_{scheme}"
    for root in _combine_workspace_roots(workspace_roots):
        root_resolved = root.resolve()
        candidate = (root_resolved / run_id / backtest_name / "combined_prediction.pkl").resolve()
        if root_resolved not in candidate.parents:
            _fail(
                "combine prediction ref path escapes configured workspace root",
                reason_code="multi_alpha_prediction_ref_path_escape",
                run_id=run_id,
                weighting_scheme=scheme,
                workspace_root=str(root_resolved),
                prediction_path=str(candidate),
            )
        candidate_text = str(candidate)
        if candidate_text in attempted:
            continue
        attempted.append(candidate_text)
        if not candidate.exists():
            continue
        if not candidate.is_file():
            _fail(
                "combine prediction ref path is not a file",
                reason_code="multi_alpha_prediction_ref_missing",
                run_id=run_id,
                weighting_scheme=scheme,
                prediction_path=candidate_text,
            )
        try:
            size_bytes = candidate.stat().st_size
            if size_bytes <= 0:
                _fail(
                    "combine prediction ref file is empty",
                    reason_code="multi_alpha_prediction_ref_missing",
                    run_id=run_id,
                    weighting_scheme=scheme,
                    prediction_path=candidate_text,
                    prediction_size_bytes=size_bytes,
                )
            sha = _sha256_file(candidate)
        except OSError as exc:
            raise StrategyPackageValidationError(
                "combine prediction ref file cannot be read",
                context={
                    "reason_code": "multi_alpha_prediction_ref_unreadable",
                    "run_id": run_id,
                    "weighting_scheme": scheme,
                    "prediction_path": candidate_text,
                    "error": f"{type(exc).__name__}: {exc}",
                },
            ) from exc
        return {
            "uri": candidate.as_uri(),
            "sha256": sha,
            "ref_source": "combine_backtest_local_workspace",
        }, attempted
    return None, attempted


def _combine_workspace_roots(extra_roots: Sequence[Path]) -> list[Path]:
    roots: list[Path] = []
    roots.extend(Path(root) for root in extra_roots)
    env_root = os.getenv("AISTOCK_MULTI_ALPHA_BACKTEST_ROOT")
    if env_root:
        roots.append(Path(env_root))
    roots.append(Path("rdagent_assets/multi_alpha_combine_backtests"))
    roots.append(Path(__file__).resolve().parents[3] / "rdagent_assets" / "multi_alpha_combine_backtests")
    deduped: list[Path] = []
    seen: set[str] = set()
    for root in roots:
        try:
            key = str(root.expanduser().resolve())
        except OSError:
            key = str(root.expanduser())
        if key in seen:
            continue
        seen.add(key)
        deduped.append(root.expanduser())
    return deduped


def _sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _normalize_component_package_ids(value: Mapping[str, str] | None) -> dict[str, str]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        _fail("component_package_ids must be an object", reason_code="multi_alpha_roster_mismatch")
    if not value:
        return {}
    normalized = {str(key).strip(): str(val).strip() for key, val in value.items() if str(key).strip() and str(val).strip()}
    if len(normalized) != len(value):
        _fail("component_package_ids contains empty leg_id or package_id", reason_code="multi_alpha_roster_mismatch")
    return normalized


def _resolver_loop_id(provenance: SeedProvenance) -> str | None:
    text = str(provenance.source_loop_id or "").strip()
    if text.startswith("Loop"):
        return text
    if "_Loop" in text:
        suffix = text.rsplit("_Loop", 1)[1]
        if suffix.isdigit():
            return f"Loop{suffix}"
    if "_L" in text:
        suffix = text.rsplit("_L", 1)[1]
        if suffix.isdigit():
            return f"Loop{suffix}"
    if provenance.source_loop_index is not None:
        return f"Loop{provenance.source_loop_index}"
    return text or None


def _validate_weight_policy(value: Mapping[str, Any], *, run_id: str, weighting_scheme: str) -> dict[str, Any]:
    payload = _as_mapping(value, field_name="weight_policy")
    mode = str(payload.get("mode") or "").strip()
    if not mode:
        _fail_manifest_incomplete("weight_policy.mode is required", run_id=run_id, weighting_scheme=weighting_scheme)
    if mode == "live_rolling_ic_weighted":
        _fail_manifest_incomplete(
            "P0 cannot promote live_rolling_ic_weighted policy before P1 weight service; use frozen_backtest_terminal_weights",
            run_id=run_id,
            weighting_scheme=weighting_scheme,
            weight_policy_mode=mode,
        )
    if mode != "frozen_backtest_terminal_weights":
        _fail_manifest_incomplete(
            "P0 multi-alpha promotion requires weight_policy.mode=frozen_backtest_terminal_weights",
            run_id=run_id,
            weighting_scheme=weighting_scheme,
            weight_policy_mode=mode,
        )
    return _jsonable(payload)


def _stable_created_at(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str) and value.strip():
        text = value.strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return datetime(1970, 1, 1, tzinfo=timezone.utc)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return datetime(1970, 1, 1, tzinfo=timezone.utc)


def _is_finite_number(value: Any) -> bool:
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def _float_or_none(value: Any) -> float | None:
    return float(value) if _is_finite_number(value) else None


def _is_sha256(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return len(text) == 64 and all(ch in "0123456789abcdef" for ch in text)


def _jsonable(value: Any) -> Any:
    return json.loads(json.dumps(value, ensure_ascii=False, default=str))


def _canonical_json(value: Any) -> str:
    return json.dumps(_jsonable(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _paper_admission() -> dict[str, Any]:
    return {
        "eligible": True,
        "blocking": [],
        "diagnostic_only": True,
        "required_for_signal_admission": False,
        "dry_run_endpoint": "paper-runtime-dry-run",
    }


def _with_signal_admission_evidence(
    manifest: StrategyPackageManifest,
    self_check_result: Any,
    *,
    provider_version: str,
) -> StrategyPackageManifest:
    if manifest.alpha_mode != AlphaMode.MULTI_ALPHA:
        return manifest
    signal_evidence = _signal_admission_evidence(
        manifest,
        self_check_result,
        provider_version=provider_version,
    )
    source_evidence = _jsonable(manifest.source_evidence or {})
    multi_alpha = dict(source_evidence.get("multi_alpha") or {})
    multi_alpha.pop("paper_admission", None)
    multi_alpha["signal_admission"] = signal_evidence
    source_evidence["multi_alpha"] = multi_alpha
    return freeze_manifest(
        manifest.model_copy(
            update={
                "source_evidence": source_evidence,
                "manifest_sha256": None,
            }
        )
    )


def _signal_admission_evidence(
    manifest: StrategyPackageManifest,
    self_check_result: Any,
    *,
    provider_version: str,
) -> dict[str, Any]:
    context = self_check_result.to_context() if hasattr(self_check_result, "to_context") else {}
    combined_signal_smoke = context.get("combined_signal_smoke") or getattr(self_check_result, "combined_signal_smoke", None)
    origin = context.get("origin") or getattr(self_check_result, "origin", None)
    if origin != "package_asset":
        raise StrategyPackageValidationError(
            "MULTI_ALPHA parent self-check evidence must originate from package assets",
            context={
                "reason_code": "multi_alpha_signal_self_check_failed",
                "package_id": manifest.package_id,
                "manifest_sha256": manifest.manifest_sha256,
                "self_check_origin": origin,
            },
        )
    if not isinstance(combined_signal_smoke, Mapping):
        raise StrategyPackageValidationError(
            "MULTI_ALPHA parent self-check did not return combined signal smoke evidence",
            context={
                "reason_code": "multi_alpha_signal_selection_artifact_unavailable",
                "package_id": manifest.package_id,
                "manifest_sha256": manifest.manifest_sha256,
            },
        )
    leg_count = _positive_int(combined_signal_smoke.get("leg_count"))
    if combined_signal_smoke.get("schema_version") != MULTI_ALPHA_COMBINED_SIGNAL_SMOKE_SCHEMA or leg_count is None:
        raise StrategyPackageValidationError(
            "MULTI_ALPHA parent self-check combined signal smoke is empty or invalid",
            context={
                "reason_code": "multi_alpha_signal_selection_artifact_empty",
                "package_id": manifest.package_id,
                "manifest_sha256": manifest.manifest_sha256,
                "combined_signal_smoke": _jsonable(combined_signal_smoke),
            },
        )
    if combined_signal_smoke.get("deterministic_replay") is not True:
        raise StrategyPackageValidationError(
            "MULTI_ALPHA parent self-check combined signal smoke is not deterministic",
            context={
                "reason_code": "multi_alpha_signal_selection_artifact_nondeterministic",
                "package_id": manifest.package_id,
                "manifest_sha256": manifest.manifest_sha256,
                "combined_signal_smoke": _jsonable(combined_signal_smoke),
            },
        )
    evidence = {
        "schema_version": MULTI_ALPHA_SIGNAL_ADMISSION_SCHEMA,
        "self_check_passed": True,
        "self_check_origin": "package_asset",
        "self_check_manifest_sha256": manifest.manifest_sha256,
        "combined_signal_smoke": _jsonable(combined_signal_smoke),
        "deterministic": True,
        "leg_count": leg_count,
        "provider_version": provider_version,
        "paper_runtime_dry_run_required": False,
        "persisted_for_hot_path": True,
    }
    return _jsonable(evidence)


def _positive_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _build_strategy_snapshot(*, topk: int, secondary_topk: list[int], backtest_config: Mapping[str, Any]) -> dict[str, Any]:
    snapshot = {
        "topk": topk,
        "secondary_topk": secondary_topk,
        "n_drop": _first_present(backtest_config, ("n_drop", "ndrop", "n_drop")),
        "stock_pool": _first_present(backtest_config, ("stock_pool", "universe", "market_universe", "strategy")),
        "filtered_pool": _first_present(backtest_config, ("filtered_pool", "filtered_pool_name", "pool_name")),
        "label_horizon": _first_present(backtest_config, ("label_horizon", "horizon", "label_horizon_days")),
        "execution_algo": _first_present(backtest_config, ("execution_algo", "algo_code", "strategy_id", "strategy")),
    }
    missing = [key for key, value in snapshot.items() if value in (None, "", []) and key != "secondary_topk"]
    if missing:
        _fail_manifest_incomplete(
            "backtest_config_json is missing fields required to freeze MULTI_ALPHA manifest",
            missing_fields=missing,
            backtest_config_keys=sorted(backtest_config),
        )
    return snapshot


def _first_present(mapping: Mapping[str, Any], keys: Sequence[str]) -> Any:
    for key in keys:
        if key in mapping and mapping[key] not in (None, ""):
            return mapping[key]
    strategy_kwargs = mapping.get("strategy_kwargs")
    if isinstance(strategy_kwargs, Mapping):
        for key in keys:
            if key in strategy_kwargs and strategy_kwargs[key] not in (None, ""):
                return strategy_kwargs[key]
    port_config = mapping.get("port_analysis_config")
    if isinstance(port_config, Mapping):
        strategy = port_config.get("strategy")
        if isinstance(strategy, Mapping):
            kwargs = strategy.get("kwargs")
            if isinstance(kwargs, Mapping):
                for key in keys:
                    if key in kwargs and kwargs[key] not in (None, ""):
                        return kwargs[key]
    return None


def _stable_package_id(
    *,
    run_id: str,
    weighting_scheme: str,
    topk: int,
    leg_evidence: Sequence[_ParentLegAssetPlan],
    prediction_ref: Mapping[str, str],
    weight_policy: Mapping[str, Any],
    scheme_result_id: str,
) -> str:
    payload = {
        "run_id": run_id,
        "weighting_scheme": weighting_scheme,
        "scheme_result_id": scheme_result_id,
        "topk": topk,
        "legs": [
            {
                "leg_id": leg.leg_id,
                "seed_run_ids": list(leg.seed_run_ids),
                "leg_manifest_sha256": leg.manifest.manifest_sha256,
                "model_id": leg.manifest.alpha_components[0].model_id,
                "terminal_weight": leg.terminal_weight,
            }
            for leg in leg_evidence
        ],
        "prediction_ref_sha256": prediction_ref["sha256"],
        "weight_policy": weight_policy,
        "runtime_provider_version": MULTI_ALPHA_PROMOTION_PROVIDER_VERSION,
    }
    digest = hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()
    return f"pkg_ma_{digest[:24]}"


def _seed_roster_digest(
    *,
    leg_id: str,
    seed_run_ids: Sequence[str],
    seed_sources: Sequence[SeedProvenance],
) -> str:
    payload = {
        "leg_id": leg_id,
        "seed_run_ids": list(seed_run_ids),
        "seed_sources": [
            {
                "seed_ref": source.seed_ref,
                "seed_ref_kind": source.seed_ref_kind,
                "source_experiment_id": source.source_experiment_id,
                "source_task_id": source.source_task_id,
                "source_loop_id": source.source_loop_id,
                "source_loop_index": source.source_loop_index,
                "source_run_id": source.source_run_id,
            }
            for source in seed_sources
        ],
    }
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def _default_package_name(leg_evidence: Sequence[_ParentLegAssetPlan], weighting_scheme: str, topk: int, package_id: str) -> str:
    legs = "_".join(leg.leg_id.split("_h20")[0][:12] for leg in leg_evidence)
    return f"MA{len(leg_evidence)}_{legs}_{weighting_scheme}_topk{topk}_{package_id[-8:]}"[:120]


def _stable_parent_leg_asset_id(*, run_id: str, leg_id: str, seed_digest: str) -> str:
    payload = {
        "run_id": run_id,
        "leg_id": leg_id,
        "seed_digest": seed_digest,
        "materialization": "multi_alpha_parent_leg_asset_v1",
    }
    digest = hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()
    return f"pkg_mal_{digest[:24]}"


def _default_parent_leg_asset_name(leg_id: str, run_id: str, package_id: str) -> str:
    leg = "".join(ch if ch.isalnum() or ch in "_-" else "_" for ch in leg_id)[:48]
    run_suffix = str(run_id)[-12:]
    return f"MAL_{leg}_{run_suffix}_{package_id[-8:]}"[:120]


def _component_from_leg_plan(leg: _ParentLegAssetPlan) -> AlphaComponent:
    child_component = leg.manifest.alpha_components[0]
    return AlphaComponent(
        alpha_id=leg.leg_id,
        alpha_name=child_component.alpha_name or leg.leg_id,
        component_weight=leg.terminal_weight,
        factor_ids=list(child_component.factor_ids),
        model_id=child_component.model_id,
        model_ref=child_component.model_ref,
        holding_period=child_component.holding_period,
        rebalance_frequency=child_component.rebalance_frequency,
        score_direction=child_component.score_direction,
        score_normalization="rank",
        risk_tags=list(child_component.risk_tags),
        metrics_snapshot=child_component.metrics_snapshot,
        lineage=AlphaLineage(
            qe_artifact_id="multi-seed",
            factor_artifact_refs=list(child_component.lineage.factor_artifact_refs or child_component.factor_ids),
            model_artifact_ref=f"parent_package_asset:model_id:{child_component.model_id}",
        ),
    )


def _assert_unique_parent_leg_model_ids(legs: Sequence[_ParentLegAssetPlan], *, run_id: str) -> None:
    seen: dict[str, dict[str, Any]] = {}
    for leg in legs:
        component = leg.manifest.alpha_components[0]
        model_id = str(component.model_id or "").strip()
        models = leg.manifest.model_asset if isinstance(leg.manifest.model_asset, list) else [leg.manifest.model_asset]
        model = models[0] if models else None
        if not model_id:
            _fail(
                "multi-alpha parent leg model_id is required",
                reason_code="multi_alpha_promotion_parent_model_id_collision",
                run_id=run_id,
                leg_id=leg.leg_id,
            )
        current = {
            "leg_id": leg.leg_id,
            "model_id": model_id,
            "sha256": getattr(model, "sha256", None),
            "asset_ref": getattr(model, "asset_ref", None),
        }
        previous = seen.get(model_id)
        if previous is not None:
            _fail(
                "multi-alpha parent requires unique model_id per leg",
                reason_code="multi_alpha_promotion_parent_model_id_collision",
                run_id=run_id,
                model_id=model_id,
                first_leg_id=previous["leg_id"],
                second_leg_id=leg.leg_id,
                first_sha256=previous.get("sha256"),
                second_sha256=current.get("sha256"),
                first_asset_ref=previous.get("asset_ref"),
                second_asset_ref=current.get("asset_ref"),
                model_id_uniqueness_policy="unique_per_leg",
            )
        seen[model_id] = current


def _merge_factor_assets(manifests: Sequence[StrategyPackageManifest]) -> list[FactorAsset]:
    merged: dict[str, FactorAsset] = {}
    key_sources: dict[tuple[str, str], FactorAsset] = {}
    for manifest in manifests:
        for factor in manifest.factor_set:
            for key_type, raw_key in (("factor_id", factor.factor_id), ("factor_name", factor.factor_name)):
                key = str(raw_key or "").strip()
                if not key:
                    continue
                existing = key_sources.get((key_type, key))
                if existing is not None and str(existing.sha256 or "").strip().lower() != str(factor.sha256 or "").strip().lower():
                    _fail(
                        "multi-alpha parent factor ref collision detected while merging per-leg assets",
                        reason_code="multi_alpha_promotion_parent_factor_ref_collision",
                        factor_key_type=key_type,
                        factor_key=key,
                        first_factor_id=existing.factor_id,
                        first_factor_name=existing.factor_name,
                        first_sha256=existing.sha256,
                        second_factor_id=factor.factor_id,
                        second_factor_name=factor.factor_name,
                        second_sha256=factor.sha256,
                    )
                key_sources[(key_type, key)] = factor
            merge_key = str(factor.factor_id or factor.factor_name or "").strip()
            if not merge_key:
                _fail(
                    "multi-alpha parent factor asset is missing id/name",
                    reason_code="multi_alpha_promotion_parent_factor_ref_collision",
                )
            existing = merged.get(merge_key)
            if existing is not None and str(existing.sha256 or "").strip().lower() != str(factor.sha256 or "").strip().lower():
                _fail(
                    "multi-alpha parent factor id collision detected while merging per-leg assets",
                    reason_code="multi_alpha_promotion_parent_factor_ref_collision",
                    factor_key_type="factor_id",
                    factor_key=merge_key,
                    first_sha256=existing.sha256,
                    second_sha256=factor.sha256,
                )
            merged[merge_key] = factor
    return [merged[key] for key in sorted(merged)]


def _merge_model_assets(manifests: Sequence[StrategyPackageManifest]) -> list[ModelAsset]:
    merged: dict[str, ModelAsset] = {}
    for manifest in manifests:
        assets = manifest.model_asset if isinstance(manifest.model_asset, list) else [manifest.model_asset]
        for asset in assets:
            key = str(asset.model_id or "").strip()
            if not key:
                _fail(
                    "multi-alpha parent model asset is missing model_id",
                    reason_code="multi_alpha_promotion_parent_model_id_collision",
                    package_id=manifest.package_id,
                )
            existing = merged.get(key)
            if existing is not None and str(existing.sha256 or "").strip().lower() != str(asset.sha256 or "").strip().lower():
                _fail(
                    "multi-alpha parent model_id collision detected while merging per-leg assets",
                    reason_code="multi_alpha_promotion_parent_model_id_collision",
                    model_id=key,
                    first_sha256=existing.sha256,
                    second_sha256=asset.sha256,
                    first_asset_ref=existing.asset_ref,
                    second_asset_ref=asset.asset_ref,
                )
            merged[key] = asset
    return [merged[key] for key in sorted(merged)]


def _merge_runtime_assets(
    manifests: Sequence[StrategyPackageManifest],
    *,
    package_id: str,
) -> RuntimeAssetManifest:
    selected: RuntimeAssetManifest | None = None
    for manifest in manifests:
        runtime_assets = manifest.runtime_assets
        alpha158 = runtime_assets.alpha158 if runtime_assets is not None else None
        if runtime_assets is None or alpha158 is None:
            _fail(
                "multi-alpha parent requires explicit per-leg runtime assets",
                reason_code="multi_alpha_promotion_parent_runtime_assets_missing",
                package_id=package_id,
                leg_manifest_package_id=manifest.package_id,
            )
        if not alpha158.enabled:
            continue
        if not alpha158.asset_ref or not alpha158.sha256 or not alpha158.aliases:
            _fail(
                "multi-alpha parent requires complete Alpha158 runtime assets for enabled legs",
                reason_code="multi_alpha_promotion_parent_runtime_assets_missing",
                package_id=package_id,
                leg_manifest_package_id=manifest.package_id,
                asset_ref=alpha158.asset_ref,
                sha256=alpha158.sha256,
                alias_count=len(alpha158.aliases or []),
            )
        if selected is None:
            selected = runtime_assets
            continue
        current = selected.alpha158
        if (
            current.asset_ref != alpha158.asset_ref
            or str(current.sha256 or "").strip().lower() != str(alpha158.sha256 or "").strip().lower()
            or list(current.aliases) != list(alpha158.aliases)
        ):
            _fail(
                "multi-alpha parent Alpha158 runtime assets differ across legs",
                reason_code="multi_alpha_promotion_parent_runtime_assets_missing",
                package_id=package_id,
                first_sha256=current.sha256,
                second_sha256=alpha158.sha256,
                first_alias_count=len(current.aliases),
                second_alias_count=len(alpha158.aliases),
            )
    return selected or RuntimeAssetManifest()
