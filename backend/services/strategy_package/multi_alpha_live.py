"""MULTI_ALPHA live selection artifact generation.

This module is deliberately limited to StrategyPackage signal artifacts. It
does not touch Paper v2 execution runtimes, schedulers, brokers, or portfolios.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Callable, Mapping, Sequence

import pandas as pd

from backend.services.multi_alpha.orthogonality import normalize_prediction_frame
from backend.services.selection_center.runtime_profile import parse_selection_runtime_profile
from backend.services.strategy_package.live_inference import AUTHORITATIVE_SELECTION_SCOPE, win_to_wsl_path
from backend.services.strategy_package.models import (
    AlphaComponent,
    AlphaMode,
    FactorAsset,
    ModelAsset,
    RuntimeAssetManifest,
    SelectionScoreArtifactStatus,
    StrategyPackageManifest,
)
from backend.services.strategy_package.selection_artifact import (
    SelectionScoreArtifact,
    SELECTION_SCORE_ARTIFACT_CONTRACT_V2,
    build_reference_price_source_receipt,
    build_manifest_asset_closure,
    build_selection_artifact_v2_provenance,
    selection_artifact_runtime_hash,
    selection_artifact_runtime_hash_v2,
)
from backend.services.trading_calendar_status import TradingCalendarStatusService
from backend.services.trading_core.errors import (
    ArtifactGenerationFailedError,
    DataUnavailableError,
    RuntimeConfigInvalidError,
    StrategyPackageValidationError,
    TradingCoreError,
    UnsupportedFeatureError,
)


LIVE_MULTI_ALPHA_SELECTION_SOURCE_TYPE = "live_multi_alpha_inference_v1"
MULTI_ALPHA_LIVE_PROVIDER_VERSION = "multi_alpha_live_selection_provider_v1"

REASON_RUNTIME_NOT_ENABLED = "multi_alpha_runtime_not_enabled"
REASON_LEG_MISSING = "multi_alpha_leg_missing"
REASON_CHILD_MANIFEST_MISMATCH = "multi_alpha_child_manifest_mismatch"
REASON_SEED_PREDICTION_MISSING = "multi_alpha_seed_prediction_missing"
REASON_COMPONENT_COVERAGE_LOW = "multi_alpha_component_coverage_low"
REASON_WEIGHT_UNAVAILABLE = "multi_alpha_weight_unavailable"
REASON_LABEL_WINDOW_INSUFFICIENT = "multi_alpha_label_window_insufficient"
REASON_WEIGHT_ALL_NON_POSITIVE = "multi_alpha_weight_all_non_positive"
REASON_TOPK_RUNTIME_MISMATCH = "multi_alpha_topk_runtime_mismatch"
REASON_PREDICTION_NOT_AUTHORITATIVE = "multi_alpha_prediction_not_authoritative"
REASON_DEADLINE_EXCEEDED = "multi_alpha_selection_artifact_deadline_exceeded"
REASON_PARENT_LEG_MAPPING_MISSING = "multi_alpha_parent_leg_mapping_missing"
REASON_PARENT_LEG_SEED_METADATA_MISSING = "multi_alpha_parent_leg_seed_metadata_missing"
REASON_PARENT_LEG_MODEL_ID_MISSING = "multi_alpha_parent_leg_model_id_missing"
REASON_PARENT_LEG_MODEL_ASSET_MISSING = "multi_alpha_parent_leg_model_asset_missing"
REASON_PARENT_LEG_MODEL_ASSET_AMBIGUOUS = "multi_alpha_parent_leg_model_asset_ambiguous"
REASON_PARENT_LEG_FACTOR_REFS_MISSING = "multi_alpha_parent_leg_factor_refs_missing"
REASON_PARENT_LEG_FACTOR_ASSET_MISSING = "multi_alpha_parent_leg_factor_asset_missing"
REASON_PARENT_LEG_FACTOR_ASSET_AMBIGUOUS = "multi_alpha_parent_leg_factor_asset_ambiguous"
REASON_PARENT_ALPHA158_SCHEMA_MISSING = "multi_alpha_parent_alpha158_schema_missing"
REASON_PARENT_ALPHA158_SCHEMA_MISMATCH = "multi_alpha_parent_alpha158_schema_mismatch"
REASON_PARENT_LEG_RUNTIME_ASSETS_INCOMPLETE = "multi_alpha_parent_leg_runtime_assets_incomplete"
REASON_PARENT_LEG_INFERENCE_EMPTY = "multi_alpha_parent_leg_inference_empty"


@dataclass(frozen=True)
class MultiAlphaWeightArtifact:
    artifact_id: str
    artifact_sha256: str
    weights: dict[str, float]
    metadata: dict[str, Any]


@dataclass(frozen=True)
class ParentLegRuntimeSlice:
    parent_package_id: str
    parent_manifest_sha256: str
    leg_id: str
    component: AlphaComponent
    model_asset: ModelAsset
    factor_set: tuple[FactorAsset, ...]
    runtime_assets: RuntimeAssetManifest
    seed_run_ids: tuple[str, ...]
    ensemble_method: str
    terminal_weight: float | None
    legacy_child_ref_ignored: bool


@dataclass(frozen=True)
class ParentLegLiveInferenceResult:
    seed_frames: dict[str, pd.DataFrame]
    live_result: Any
    source: Any
    prepared: Any


@dataclass(frozen=True)
class _CombinedLiveInferenceResult:
    scores: list[dict[str, Any]]
    universe_count: int
    source_read_receipts: list[dict[str, Any]]
    input_context: dict[str, Any]


class MultiAlphaLiveError(RuntimeError):
    """Fail-loud MULTI_ALPHA signal-layer error with explicit reason_code."""

    def __init__(self, message: str, *, reason_code: str, context: Mapping[str, Any] | None = None) -> None:
        payload = {"reason_code": reason_code, **dict(context or {})}
        super().__init__(message)
        self.message = message
        self.context = payload


class StaticMultiAlphaMetricProvider:
    """Test/dry-run helper: read rolling-IC rows from runtime_config metadata."""

    def load_metric_rows(
        self,
        *,
        runtime_config: Mapping[str, Any],
        leg_ids: Sequence[str],
        apply_date: date,
        metric: str,
    ) -> list[dict[str, Any]]:
        artifact_config = _artifact_config(runtime_config)
        rows = artifact_config.get("multi_alpha_weight_history") or artifact_config.get("weight_history") or []
        if not isinstance(rows, list):
            _raise(
                "multi_alpha weight_history must be a list",
                REASON_WEIGHT_UNAVAILABLE,
                apply_date=apply_date.isoformat(),
                metric=metric,
                value_type=type(rows).__name__,
            )
        selected: list[dict[str, Any]] = []
        allowed = set(leg_ids)
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            leg_id = str(row.get("leg_id") or "").strip()
            if leg_id in allowed:
                selected.append(dict(row))
        return selected


class MultiAlphaWeightService:
    """Compute auditable MULTI_ALPHA live/frozen weights without silent fallback."""

    def __init__(
        self,
        *,
        metric_provider: Any | None = None,
        calendar_provider: Any | None = None,
    ) -> None:
        self.metric_provider = metric_provider or StaticMultiAlphaMetricProvider()
        self.calendar_provider = calendar_provider or TradingCalendarStatusService()

    def weights_for_apply_date(
        self,
        *,
        manifest: StrategyPackageManifest,
        apply_date: date,
        leg_ids: Sequence[str],
        runtime_config: Mapping[str, Any] | None = None,
    ) -> MultiAlphaWeightArtifact:
        evidence = _multi_alpha_evidence(manifest)
        policy = _weight_policy(manifest)
        mode = str(policy.get("mode") or "").strip()
        if mode == "frozen_backtest_terminal_weights":
            weights = _normalize_positive_weights(
                _terminal_weights(evidence, expected_leg_ids=leg_ids),
                reason_code=REASON_WEIGHT_UNAVAILABLE,
                context={"package_id": manifest.package_id, "mode": mode},
            )
            return self._artifact(
                manifest=manifest,
                apply_date=apply_date,
                mode=mode,
                weights=weights,
                metadata={"source": "frozen_backtest_terminal_weights", "sample_counts": None},
            )
        if mode != "live_rolling_ic_weighted":
            _raise(
                "MULTI_ALPHA weight_policy.mode is not supported by live provider",
                REASON_WEIGHT_UNAVAILABLE,
                package_id=manifest.package_id,
                mode=mode,
                supported=["live_rolling_ic_weighted", "frozen_backtest_terminal_weights"],
            )

        metric = str(policy.get("metric") or "rank_ic").strip()
        lookback = _positive_int(policy.get("lookback_trading_days"), default=252, field_name="lookback_trading_days")
        min_periods = _positive_int(policy.get("min_periods"), default=60, field_name="min_periods")
        label_horizon = _positive_int(policy.get("label_horizon"), default=_label_horizon(manifest), field_name="label_horizon")
        settlement_lag = _non_negative_int(
            policy.get("settlement_lag_trading_days", policy.get("settlement_lag", 1)),
            default=1,
            field_name="settlement_lag_trading_days",
        )
        clip_negative = bool(policy.get("clip_negative_to_zero", True))
        rows = self.metric_provider.load_metric_rows(
            runtime_config=runtime_config or {},
            leg_ids=leg_ids,
            apply_date=apply_date,
            metric=metric,
        )
        if not rows:
            _raise(
                "live rolling IC weight history is unavailable",
                REASON_WEIGHT_UNAVAILABLE,
                package_id=manifest.package_id,
                apply_date=apply_date.isoformat(),
                metric=metric,
                leg_ids=list(leg_ids),
            )

        trading_days = self._trading_days_for_weight_window(
            apply_date=apply_date,
            lookback_trading_days=lookback,
            label_horizon=label_horizon,
            settlement_lag=settlement_lag,
            runtime_config=runtime_config or {},
        )
        cutoff_date = _shift_trading_days(trading_days, apply_date, -settlement_lag)
        if cutoff_date is None:
            _raise(
                "trading calendar cannot resolve settlement-lag cutoff for live rolling weights",
                REASON_LABEL_WINDOW_INSUFFICIENT,
                package_id=manifest.package_id,
                apply_date=apply_date.isoformat(),
                settlement_lag_trading_days=settlement_lag,
            )
        window_start_index = max(0, trading_days.index(apply_date) - lookback) if apply_date in trading_days else 0
        window_days = set(trading_days[window_start_index : trading_days.index(apply_date)] if apply_date in trading_days else trading_days)

        values_by_leg: dict[str, list[float]] = {leg_id: [] for leg_id in leg_ids}
        rejected_unmatured = 0
        for row in rows:
            leg_id = str(row.get("leg_id") or "").strip()
            if leg_id not in values_by_leg:
                continue
            label_date = _parse_date(row.get("label_date") or row.get("trade_date") or row.get("date"))
            if label_date is None or label_date not in window_days:
                continue
            matured = _shift_trading_days(trading_days, label_date, label_horizon)
            if matured is None or matured > cutoff_date:
                rejected_unmatured += 1
                continue
            value = _finite_float(row.get(metric, row.get("metric_value", row.get("rank_ic"))))
            if value is not None:
                values_by_leg[leg_id].append(value)

        sample_counts = {leg_id: len(values) for leg_id, values in values_by_leg.items()}
        insufficient = {leg_id: count for leg_id, count in sample_counts.items() if count < min_periods}
        if insufficient:
            _raise(
                "live rolling IC label window has insufficient mature samples",
                REASON_LABEL_WINDOW_INSUFFICIENT,
                package_id=manifest.package_id,
                apply_date=apply_date.isoformat(),
                metric=metric,
                min_periods=min_periods,
                sample_counts=sample_counts,
                rejected_unmatured_count=rejected_unmatured,
                cutoff_date=cutoff_date.isoformat(),
            )

        raw = {
            leg_id: (sum(values) / len(values))
            for leg_id, values in values_by_leg.items()
        }
        clipped = {
            leg_id: max(0.0, value) if clip_negative else value
            for leg_id, value in raw.items()
        }
        if sum(value for value in clipped.values() if value > 0) <= 0:
            _raise(
                "live rolling IC weights are all non-positive after clipping",
                REASON_WEIGHT_ALL_NON_POSITIVE,
                package_id=manifest.package_id,
                apply_date=apply_date.isoformat(),
                metric=metric,
                raw_weights=raw,
                clipped_weights=clipped,
            )
        weights = _normalize_positive_weights(
            clipped,
            reason_code=REASON_WEIGHT_ALL_NON_POSITIVE,
            context={"package_id": manifest.package_id, "apply_date": apply_date.isoformat()},
        )
        return self._artifact(
            manifest=manifest,
            apply_date=apply_date,
            mode=mode,
            weights=weights,
            metadata={
                "source": "live_rolling_ic_weighted",
                "metric": metric,
                "lookback_trading_days": lookback,
                "min_periods": min_periods,
                "label_horizon": label_horizon,
                "settlement_lag_trading_days": settlement_lag,
                "cutoff_date": cutoff_date.isoformat(),
                "sample_counts": sample_counts,
                "raw_metric_mean": raw,
            },
        )

    def _trading_days_for_weight_window(
        self,
        *,
        apply_date: date,
        lookback_trading_days: int,
        label_horizon: int,
        settlement_lag: int,
        runtime_config: Mapping[str, Any],
    ) -> list[date]:
        artifact_config = _artifact_config(runtime_config)
        configured_days = artifact_config.get("trading_days")
        if configured_days is not None:
            days = [_parse_date(item) for item in configured_days]
            normalized = sorted({item for item in days if item is not None})
            if apply_date not in normalized:
                normalized.append(apply_date)
                normalized = sorted(set(normalized))
            return normalized
        calendar_span = max(lookback_trading_days + label_horizon + settlement_lag + 30, 90) * 2
        start_date = apply_date - timedelta(days=calendar_span)
        return self.calendar_provider.list_trading_days(start_date, apply_date)

    @staticmethod
    def _artifact(
        *,
        manifest: StrategyPackageManifest,
        apply_date: date,
        mode: str,
        weights: Mapping[str, float],
        metadata: Mapping[str, Any],
    ) -> MultiAlphaWeightArtifact:
        payload = {
            "schema_version": "multi_alpha_weight_artifact_v1",
            "provider_version": MULTI_ALPHA_LIVE_PROVIDER_VERSION,
            "package_id": manifest.package_id,
            "manifest_sha256": manifest.manifest_sha256,
            "apply_date": apply_date.isoformat(),
            "mode": mode,
            "weights": dict(weights),
            "metadata": dict(metadata),
        }
        digest = _canonical_sha256(payload)
        return MultiAlphaWeightArtifact(
            artifact_id=f"maw_{digest[:24]}",
            artifact_sha256=digest,
            weights={key: float(value) for key, value in weights.items()},
            metadata=payload,
        )


class MultiAlphaLivePredictionProvider:
    """Generate authoritative live selection artifacts for frozen MULTI_ALPHA packages."""

    def __init__(
        self,
        *,
        package_repository: Any,
        artifact_repository: Any,
        runtime_asset_resolver: Any,
        live_inference_provider: Any,
        weight_service: MultiAlphaWeightService | None = None,
        reference_price_loader: Callable[[list[str], date], dict[str, float]] | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.package_repository = package_repository
        self.artifact_repository = artifact_repository
        self.runtime_asset_resolver = runtime_asset_resolver
        self.live_inference_provider = live_inference_provider
        self.weight_service = weight_service or MultiAlphaWeightService()
        self.reference_price_loader = reference_price_loader or (lambda _symbols, _trade_date: {})
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def generate_artifacts(
        self,
        *,
        package_id: str,
        trade_dates: Sequence[date],
        data_source: str,
        runtime_config: Mapping[str, Any] | None = None,
        include_reference_price: bool = True,
        cutoff_date: date | None = None,
        inference_backend: str,
    ) -> list[SelectionScoreArtifact]:
        config = dict(runtime_config or {})
        self._ensure_enabled(config)
        self._ensure_deadline(config, package_id=package_id)
        record = self.package_repository.get(package_id)
        manifest = record.current_manifest()
        if manifest.alpha_mode != AlphaMode.MULTI_ALPHA:
            _raise(
                "MultiAlphaLivePredictionProvider requires alpha_mode=MULTI_ALPHA",
                REASON_RUNTIME_NOT_ENABLED,
                package_id=package_id,
                alpha_mode=manifest.alpha_mode.value,
            )
        if not manifest.manifest_sha256:
            _raise("MULTI_ALPHA manifest must be frozen for live selection", REASON_LEG_MISSING, package_id=package_id)

        evidence = _multi_alpha_evidence(manifest)
        legs = _legs(evidence, package_id=package_id)
        leg_ids = [leg["leg_id"] for leg in legs]
        topk = self._runtime_topk(manifest, config)
        coverage_threshold = self._coverage_threshold(config, topk=topk)
        normalization_method = _normalization_method(manifest)
        runtime_hash = multi_alpha_selection_artifact_runtime_hash_v2(manifest, config)
        leg_slices = _parent_leg_runtime_slices(manifest, evidence=evidence, package_id=package_id)

        artifacts: list[SelectionScoreArtifact] = []
        for current_date in sorted(set(trade_dates)):
            score_trade_date = cutoff_date or current_date
            leg_frames: dict[str, pd.DataFrame] = {}
            component_metadata: dict[str, Any] = {}
            leg_executions: dict[str, ParentLegLiveInferenceResult] = {}
            for leg_slice in leg_slices:
                leg_execution = self._run_parent_leg_live_inference(
                    manifest=manifest,
                    leg_slice=leg_slice,
                    trade_date=current_date,
                    cutoff_date=cutoff_date,
                    runtime_config=config,
                    inference_backend=inference_backend,
                )
                seed_frames = leg_execution.seed_frames
                leg_executions[leg_slice.leg_id] = leg_execution
                ensemble = _ensemble_seed_frames(
                    seed_frames,
                    leg_id=leg_slice.leg_id,
                    model_id=leg_slice.model_asset.model_id,
                    package_id=package_id,
                    trade_date=score_trade_date,
                    allow_empty=True,
                )
                normalized = _normalize_leg_frame(
                    ensemble,
                    package_id=package_id,
                    leg_id=leg_slice.leg_id,
                    model_id=leg_slice.model_asset.model_id,
                    method=normalization_method,
                )
                leg_frames[leg_slice.leg_id] = normalized
                component_sha = _frame_sha256(normalized, score_column="normalized_score")
                component_metadata[leg_slice.leg_id] = {
                    "component_score_artifact_id": f"macs_{component_sha[:24]}",
                    "component_score_artifact_sha256": component_sha,
                    "child_package_id": None,
                    "child_manifest_sha256": None,
                    "runtime_source": "parent_package_asset",
                    "runtime_package_id": leg_slice.parent_package_id,
                    "model_params_origin": "package_asset",
                    "model_id": leg_slice.model_asset.model_id,
                    "model_asset_ref": leg_slice.model_asset.asset_ref,
                    "model_asset_sha256": leg_slice.model_asset.sha256,
                    "factor_count": len(leg_slice.factor_set),
                    "factor_artifact_refs": [factor.factor_name or factor.factor_id for factor in leg_slice.factor_set],
                    "alpha158_schema_sha256": leg_slice.runtime_assets.alpha158.sha256,
                    "seed_run_ids": list(leg_slice.seed_run_ids),
                    "seed_count": len(seed_frames),
                    "ensemble_method": leg_slice.ensemble_method,
                    "seed_runtime_mode": "frozen_representative_model_replayed_for_legacy_seed_metadata",
                    "legacy_child_ref_ignored": leg_slice.legacy_child_ref_ignored,
                    "candidate_count": int(len(normalized)),
                    "inference_universe_count": leg_execution.live_result.universe_count,
                    "source_read_receipts": list(leg_execution.live_result.source_read_receipts or []),
                    "input_context": dict(leg_execution.live_result.input_context or {}),
                }

            aligned = _align_component_frames(leg_frames, allow_empty=True)
            component_candidate_universe_size = int(len(aligned))
            raw_empty = component_candidate_universe_size == 0
            if not raw_empty and component_candidate_universe_size < coverage_threshold:
                _raise(
                    "MULTI_ALPHA component candidate coverage is below threshold",
                    REASON_COMPONENT_COVERAGE_LOW,
                    package_id=package_id,
                    trade_date=current_date.isoformat(),
                    component_candidate_universe_size=component_candidate_universe_size,
                    coverage_threshold=coverage_threshold,
                    final_topk=topk,
                    leg_candidate_counts={leg_id: int(len(frame)) for leg_id, frame in leg_frames.items()},
                )

            weight_artifact = self.weight_service.weights_for_apply_date(
                manifest=manifest,
                apply_date=current_date,
                leg_ids=leg_ids,
                runtime_config=config,
            )
            combined = (
                _combine_aligned(aligned, weights=weight_artifact.weights, leg_ids=leg_ids)
                if not raw_empty
                else pd.DataFrame(columns=[*aligned.columns, "combined_score"])
            )
            rows = (
                self._artifact_rows(
                    combined,
                    leg_ids=leg_ids,
                    weights=weight_artifact.weights,
                    topk=topk,
                    trade_date=score_trade_date,
                    include_reference_price=include_reference_price,
                )
                if not raw_empty
                else []
            )
            combined_sha = _canonical_sha256(rows)
            aggregate_source_receipts: list[dict[str, Any]] = []
            for leg_id, execution in sorted(leg_executions.items()):
                for receipt in execution.live_result.source_read_receipts or []:
                    aggregate_source_receipts.append({**dict(receipt), "leg_id": leg_id})
            aggregate_input_context = _aggregate_parent_leg_input_context(
                leg_executions=leg_executions,
                requested_trade_date=current_date,
            )
            parent_parity_payload = {
                "parent_package_id": package_id,
                "parent_manifest_sha256": manifest.manifest_sha256,
                "leg_ids": leg_ids,
                "component_score_artifact_sha256": {
                    leg_id: item["component_score_artifact_sha256"] for leg_id, item in component_metadata.items()
                },
                "weight_artifact_id": weight_artifact.artifact_id,
                "weight_artifact_sha256": weight_artifact.artifact_sha256,
                "combined_score_artifact_sha256": combined_sha,
                "normalization_method": normalization_method,
                "weights": weight_artifact.weights,
            }
            parent_parity_hash = _canonical_sha256(parent_parity_payload)
            extra_asset_entries = [
                {
                    "asset_role": "multi_alpha_leg_runtime",
                    "asset_id": leg_slice.leg_id,
                    "asset_ref": leg_slice.model_asset.asset_ref,
                    "sha256": leg_slice.model_asset.sha256,
                    "model_id": leg_slice.model_asset.model_id,
                    "factor_sha256": sorted(str(factor.sha256 or "") for factor in leg_slice.factor_set),
                    "seed_run_ids": list(leg_slice.seed_run_ids),
                    "admissibility": "PROSPECTIVE_FIRST_OBSERVED",
                }
                for leg_slice in leg_slices
            ]
            extra_asset_entries.append(
                {
                    "asset_role": "multi_alpha_weight_artifact",
                    "asset_id": weight_artifact.artifact_id,
                    "asset_ref": "multi_alpha_weight",
                    "sha256": weight_artifact.artifact_sha256,
                    "apply_date": current_date.isoformat(),
                    "admissibility": "PROSPECTIVE_FIRST_OBSERVED",
                }
            )
            asset_closure, asset_closure_status, asset_reason_codes = build_manifest_asset_closure(
                manifest,
                extra_entries=extra_asset_entries,
            )
            provider_semantics = {
                "provider_semantics_id": "multi_alpha_live_inference_v2",
                "provider_version": MULTI_ALPHA_LIVE_PROVIDER_VERSION,
                "inference_backend": inference_backend,
                "runtime_source": "parent_package_asset",
                "normalization_method": normalization_method,
                "combine_method": "weighted_normalized_score",
                "weight_artifact_sha256": weight_artifact.artifact_sha256,
                "parent_parity_hash": parent_parity_hash,
            }
            aggregate_result = _CombinedLiveInferenceResult(
                scores=rows,
                universe_count=int(aggregate_input_context["parent_input_universe_count"]),
                source_read_receipts=aggregate_source_receipts,
                input_context=aggregate_input_context,
            )
            provenance = build_selection_artifact_v2_provenance(
                result=aggregate_result,
                requested_trade_date=current_date,
                cutoff_date=cutoff_date,
                include_reference_price=include_reference_price,
                asset_closure=asset_closure,
                asset_closure_status=asset_closure_status,
                asset_reason_codes=asset_reason_codes,
                provider_semantics=provider_semantics,
                additional_source_receipts=(
                    [
                        build_reference_price_source_receipt(
                            symbols=[str(row["symbol"]) for row in rows],
                            trade_date=score_trade_date,
                            price_by_symbol={
                                str(row["symbol"]): float(row["reference_price"])
                                for row in rows
                                if row.get("reference_price") is not None
                            },
                        )
                    ]
                    if include_reference_price
                    else []
                ),
            )
            metadata = {
                "source_type": LIVE_MULTI_ALPHA_SELECTION_SOURCE_TYPE,
                "authority_scope": AUTHORITATIVE_SELECTION_SCOPE,
                "provider_version": MULTI_ALPHA_LIVE_PROVIDER_VERSION,
                "component_score_artifact_ids": {
                    leg_id: item["component_score_artifact_id"] for leg_id, item in component_metadata.items()
                },
                "component_score_artifact_sha256": {
                    leg_id: item["component_score_artifact_sha256"] for leg_id, item in component_metadata.items()
                },
                "weight_artifact_id": weight_artifact.artifact_id,
                "weight_artifact_sha256": weight_artifact.artifact_sha256,
                "combined_score_artifact_sha256": combined_sha,
                "multi_alpha_parent_parity_hash": parent_parity_hash,
                "multi_alpha_parent_parity": parent_parity_payload,
                "component_manifest_sha256": {leg_slice.leg_id: manifest.manifest_sha256 for leg_slice in leg_slices},
                "runtime_source": "parent_package_asset",
                "runtime_package_id": package_id,
                "model_params_origin": "package_asset",
                "seed_runtime_mode": "frozen_representative_model_replayed_for_legacy_seed_metadata",
                "legacy_child_ref_ignored": {
                    leg_slice.leg_id: leg_slice.legacy_child_ref_ignored for leg_slice in leg_slices
                },
                "seed_run_ids": {leg_slice.leg_id: list(leg_slice.seed_run_ids) for leg_slice in leg_slices},
                "combine_backtest_run_id": evidence.get("combine_backtest_run_id"),
                "normalization_method": normalization_method,
                "final_topk": topk,
                "component_candidate_universe_size": component_candidate_universe_size,
                "parent_input_universe_count": aggregate_input_context["parent_input_universe_count"],
                "coverage_threshold": coverage_threshold,
                "weight_policy": _weight_policy(manifest),
                "weights": weight_artifact.weights,
                "weight_artifact": weight_artifact.metadata,
                "component_artifacts": component_metadata,
                "runtime_config_hash": runtime_hash,
                "trade_date_requested": current_date.isoformat(),
                "cutoff_date": cutoff_date.isoformat() if cutoff_date else None,
                "score_trade_date": score_trade_date.isoformat(),
                "reference_price_trade_date": score_trade_date.isoformat() if include_reference_price else None,
                "inference_backend": inference_backend,
                "prediction_source_policy": "live_inference_only",
                "candidate_outcome": "VALID_NO_CANDIDATE" if raw_empty else "CANDIDATES_PRESENT",
                "empty_stage": "alpha_raw" if raw_empty else None,
                "provider_semantics_id": provenance.provider_semantics_id,
                "provider_semantics_hash": provenance.provider_semantics_hash,
                "provider_semantics": provider_semantics,
                "artifact_input_context": provenance.artifact_input_context,
                "source_read_receipts": provenance.source_read_receipts,
                "asset_closure": provenance.asset_closure,
                "asset_closure_status": provenance.asset_closure_status,
                "capture_prerequisite_reason_codes": provenance.reason_codes,
            }
            artifact = SelectionScoreArtifact(
                package_id=package_id,
                manifest_sha256=manifest.manifest_sha256,
                trade_date=current_date,
                data_source=data_source,
                runtime_config_hash=runtime_hash,
                scores_json=rows,
                score_count=len(rows),
                universe_count=provenance.universe_count,
                top_score_symbol=rows[0]["symbol"] if rows else None,
                status=SelectionScoreArtifactStatus.SUCCEEDED,
                metadata=metadata,
                artifact_contract_version=SELECTION_SCORE_ARTIFACT_CONTRACT_V2,
                artifact_input_context_hash=provenance.artifact_input_context_hash,
                source_revision_set_hash=provenance.source_revision_set_hash,
                asset_closure_hash=provenance.asset_closure_hash,
            )
            artifacts.append(self.artifact_repository.save(artifact))
        return artifacts

    @staticmethod
    def _ensure_enabled(config: Mapping[str, Any]) -> None:
        artifact_config = _artifact_config(config)
        enabled = artifact_config.get("multi_alpha_live_inference_enabled", True)
        if enabled is not True:
            _raise(
                "MULTI_ALPHA live inference provider is disabled",
                REASON_RUNTIME_NOT_ENABLED,
                configured_value=enabled,
            )

    def _ensure_deadline(self, config: Mapping[str, Any], *, package_id: str) -> None:
        artifact_config = _artifact_config(config)
        raw = artifact_config.get("multi_alpha_deadline_at") or artifact_config.get("deadline_at")
        if raw is None:
            return
        deadline = _parse_datetime(raw)
        if deadline is None:
            raise RuntimeConfigInvalidError(
                "selection_artifact_config.multi_alpha_deadline_at must be an ISO datetime",
                context={"reason_code": REASON_DEADLINE_EXCEEDED, "deadline_at": raw, "package_id": package_id},
            )
        now = self.clock()
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        if deadline.tzinfo is None:
            deadline = deadline.replace(tzinfo=timezone.utc)
        if now > deadline:
            _raise(
                "MULTI_ALPHA selection artifact deadline exceeded",
                REASON_DEADLINE_EXCEEDED,
                package_id=package_id,
                now=now.isoformat(),
                deadline_at=deadline.isoformat(),
            )

    @staticmethod
    def _runtime_topk(manifest: StrategyPackageManifest, config: Mapping[str, Any]) -> int:
        profile = parse_selection_runtime_profile(dict(config))
        daily_strategy = (manifest.backtest_context or {}).get("daily_strategy")
        manifest_topk = int(daily_strategy.get("topk")) if isinstance(daily_strategy, Mapping) and daily_strategy.get("topk") else None
        variants = set()
        if isinstance(daily_strategy, Mapping):
            for value in daily_strategy.get("topk_variants") or []:
                try:
                    variants.add(int(value))
                except (TypeError, ValueError):
                    continue
            for value in daily_strategy.get("secondary_topk") or []:
                try:
                    variants.add(int(value))
                except (TypeError, ValueError):
                    continue
        if manifest_topk is not None:
            variants.add(manifest_topk)
        requested = profile.selection.top_k
        if requested is None:
            if manifest_topk is None:
                _raise(
                    "MULTI_ALPHA runtime top_k is required when manifest has no topk",
                    REASON_TOPK_RUNTIME_MISMATCH,
                    package_id=manifest.package_id,
                )
            return manifest_topk
        topk = int(requested)
        if variants and topk not in variants:
            _raise(
                "MULTI_ALPHA runtime top_k is not allowed by frozen manifest variants",
                REASON_TOPK_RUNTIME_MISMATCH,
                package_id=manifest.package_id,
                requested_topk=topk,
                allowed_topk_variants=sorted(variants),
            )
        return topk

    @staticmethod
    def _coverage_threshold(config: Mapping[str, Any], *, topk: int) -> int:
        artifact_config = _artifact_config(config)
        raw = artifact_config.get("component_coverage_threshold", artifact_config.get("component_candidate_universe_min"))
        if raw is None:
            return topk
        threshold = _positive_int(raw, default=topk, field_name="component_coverage_threshold")
        return max(topk, threshold)

    def _run_parent_leg_live_inference(
        self,
        *,
        manifest: StrategyPackageManifest,
        leg_slice: ParentLegRuntimeSlice,
        trade_date: date,
        cutoff_date: date | None,
        runtime_config: Mapping[str, Any],
        inference_backend: str,
    ) -> ParentLegLiveInferenceResult:
        if not leg_slice.seed_run_ids:
            _raise(
                "MULTI_ALPHA leg has no seed_run_ids",
                REASON_PARENT_LEG_SEED_METADATA_MISSING,
                package_id=leg_slice.parent_package_id,
                leg_id=leg_slice.leg_id,
                model_id=leg_slice.model_asset.model_id,
            )
        representative_seed = leg_slice.seed_run_ids[0]
        seed_config = _runtime_config_for_parent_leg(runtime_config, leg_slice=leg_slice, seed_run_id=representative_seed)
        source_loader = getattr(self.runtime_asset_resolver, "load_source_for_strategy_package_leg", None)
        if not callable(source_loader):
            _raise(
                "MULTI_ALPHA parent package runtime resolver does not support per-leg package assets",
                REASON_PARENT_LEG_RUNTIME_ASSETS_INCOMPLETE,
                package_id=leg_slice.parent_package_id,
                leg_id=leg_slice.leg_id,
                model_id=leg_slice.model_asset.model_id,
            )
        try:
            source = source_loader(
                manifest=manifest,
                package_id=leg_slice.parent_package_id,
                leg_id=leg_slice.leg_id,
                model_asset=leg_slice.model_asset,
                factor_set=list(leg_slice.factor_set),
                runtime_assets=leg_slice.runtime_assets,
            )
            prepared = self.runtime_asset_resolver.prepare_workspace(
                package_id=leg_slice.parent_package_id,
                manifest_sha256=leg_slice.parent_manifest_sha256,
                source=source,
                runtime_config=seed_config,
                path_converter=win_to_wsl_path if inference_backend == "wsl" else None,
                cache_namespace=f"leg_{leg_slice.leg_id}",
            )
            result = self.live_inference_provider.run(
                workspace=prepared,
                trade_date=trade_date,
                cutoff_date=cutoff_date,
            )
        except TradingCoreError as exc:
            exc.context.setdefault("package_id", leg_slice.parent_package_id)
            exc.context.setdefault("leg_id", leg_slice.leg_id)
            exc.context.setdefault("model_id", leg_slice.model_asset.model_id)
            exc.context.setdefault("runtime_source", "parent_package_asset")
            raise
        if result.scores is None or not isinstance(result.scores, list):
            _raise(
                "MULTI_ALPHA parent leg live inference did not return a score payload",
                REASON_SEED_PREDICTION_MISSING,
                package_id=leg_slice.parent_package_id,
                leg_id=leg_slice.leg_id,
                model_id=leg_slice.model_asset.model_id,
                trade_date=(cutoff_date or trade_date).isoformat(),
            )
        representative = _live_result_to_frame(
            result.scores,
            package_id=leg_slice.parent_package_id,
            leg_id=leg_slice.leg_id,
            model_id=leg_slice.model_asset.model_id,
            seed_run_id=representative_seed,
            trade_date=cutoff_date or trade_date,
            allow_empty=True,
        )
        if representative.empty and not _has_positive_actual_universe_count(result):
            _raise(
                "empty MULTI_ALPHA parent leg inference requires a positive actual input universe",
                REASON_SEED_PREDICTION_MISSING,
                package_id=leg_slice.parent_package_id,
                leg_id=leg_slice.leg_id,
                model_id=leg_slice.model_asset.model_id,
                trade_date=trade_date.isoformat(),
            )
        return ParentLegLiveInferenceResult(
            seed_frames={seed_run_id: representative.copy() for seed_run_id in leg_slice.seed_run_ids},
            live_result=result,
            source=source,
            prepared=prepared,
        )

    def _artifact_rows(
        self,
        combined: pd.DataFrame,
        *,
        leg_ids: Sequence[str],
        weights: Mapping[str, float],
        topk: int,
        trade_date: date,
        include_reference_price: bool,
    ) -> list[dict[str, Any]]:
        selected = combined.sort_values(["combined_score", "instrument"], ascending=[False, True]).reset_index(drop=True)
        selected["rank"] = selected.index + 1
        selected = selected.head(topk).copy()
        symbols = selected["instrument"].astype(str).tolist()
        reference_prices = self.reference_price_loader(symbols, trade_date) if include_reference_price else {}
        target_weight = 1.0 / float(topk)
        rows: list[dict[str, Any]] = []
        missing_prices: list[str] = []
        for item in selected.to_dict(orient="records"):
            symbol = str(item["instrument"])
            reference_price = reference_prices.get(symbol)
            if include_reference_price and reference_price is None:
                missing_prices.append(symbol)
            component_scores = {
                leg_id: {
                    "raw_score": float(item[f"raw__{leg_id}"]),
                    "normalized_score": float(item[f"norm__{leg_id}"]),
                    "weight": float(weights[leg_id]),
                }
                for leg_id in leg_ids
            }
            rows.append(
                {
                    "symbol": symbol,
                    "score": float(item["combined_score"]),
                    "rank": int(item["rank"]),
                    "target_weight": target_weight,
                    "reference_price": reference_price,
                    "component_scores": component_scores,
                    "reason": "live_multi_alpha_inference_score",
                }
            )
        if missing_prices:
            raise DataUnavailableError(
                "reference prices are missing for MULTI_ALPHA selection artifact rows",
                context={
                    "reason_code": REASON_COMPONENT_COVERAGE_LOW,
                    "trade_date": trade_date.isoformat(),
                    "missing_price_count": len(missing_prices),
                    "missing_price_examples": missing_prices[:20],
                },
            )
        return rows


def _multi_alpha_evidence(manifest: StrategyPackageManifest) -> dict[str, Any]:
    evidence = manifest.source_evidence.get("multi_alpha") if isinstance(manifest.source_evidence, Mapping) else None
    if not isinstance(evidence, Mapping):
        _raise(
            "MULTI_ALPHA manifest is missing source_evidence.multi_alpha",
            REASON_PARENT_LEG_MAPPING_MISSING,
            package_id=manifest.package_id,
        )
    return dict(evidence)


def multi_alpha_runtime_config_for_hash(
    manifest: StrategyPackageManifest,
    runtime_config: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Inject MULTI_ALPHA score-production keys into the artifact lookup hash."""

    config = dict(runtime_config or {})
    artifact = _artifact_config(config)
    artifact["multi_alpha_final_topk"] = MultiAlphaLivePredictionProvider._runtime_topk(manifest, config)
    artifact["multi_alpha_provider_version"] = MULTI_ALPHA_LIVE_PROVIDER_VERSION
    config["selection_artifact_config"] = artifact
    return config


def multi_alpha_selection_artifact_runtime_hash(
    manifest: StrategyPackageManifest,
    runtime_config: Mapping[str, Any] | None,
) -> str:
    return selection_artifact_runtime_hash(multi_alpha_runtime_config_for_hash(manifest, runtime_config))


def multi_alpha_selection_artifact_runtime_hash_v2(
    manifest: StrategyPackageManifest,
    runtime_config: Mapping[str, Any] | None,
) -> str:
    """v2 parent key; keeps multi-alpha score inputs while separating legacy rows."""

    return selection_artifact_runtime_hash_v2(multi_alpha_runtime_config_for_hash(manifest, runtime_config))


def _weight_policy(manifest: StrategyPackageManifest) -> dict[str, Any]:
    evidence = _multi_alpha_evidence(manifest)
    for candidate in (
        evidence.get("weight_policy"),
        (manifest.backtest_context or {}).get("weight_policy") if isinstance(manifest.backtest_context, Mapping) else None,
    ):
        if isinstance(candidate, Mapping):
            return dict(candidate)
    _raise("MULTI_ALPHA manifest is missing weight_policy", REASON_WEIGHT_UNAVAILABLE, package_id=manifest.package_id)
    raise AssertionError("unreachable")


def _terminal_weights(evidence: Mapping[str, Any], *, expected_leg_ids: Sequence[str]) -> dict[str, float]:
    raw = evidence.get("terminal_weights")
    if not isinstance(raw, Mapping):
        _raise("MULTI_ALPHA terminal weights are missing", REASON_WEIGHT_UNAVAILABLE, expected_leg_ids=list(expected_leg_ids))
    weights = {str(key): float(value) for key, value in raw.items() if str(key) in set(expected_leg_ids)}
    missing = sorted(set(expected_leg_ids) - set(weights))
    if missing:
        _raise("MULTI_ALPHA terminal weights do not cover all legs", REASON_WEIGHT_UNAVAILABLE, missing_leg_ids=missing)
    return weights


def _legs(evidence: Mapping[str, Any], *, package_id: str) -> list[dict[str, Any]]:
    raw = evidence.get("legs")
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        _raise("MULTI_ALPHA evidence legs must be a list", REASON_PARENT_LEG_MAPPING_MISSING, package_id=package_id)
    legs: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, Mapping):
            _raise("MULTI_ALPHA leg entry must be an object", REASON_PARENT_LEG_MAPPING_MISSING, package_id=package_id)
        leg_id = str(item.get("leg_id") or "").strip()
        child_package_id = str(item.get("child_package_id") or "").strip()
        child_sha = str(item.get("child_manifest_sha256") or "").strip().lower()
        seed_run_ids = [str(seed or "").strip() for seed in item.get("seed_run_ids") or [] if str(seed or "").strip()]
        if not leg_id:
            _raise(
                "MULTI_ALPHA leg is incomplete",
                REASON_PARENT_LEG_MAPPING_MISSING,
                package_id=package_id,
                leg_id=leg_id,
            )
        if leg_id in seen:
            _raise("MULTI_ALPHA leg_id must be unique", REASON_PARENT_LEG_MAPPING_MISSING, package_id=package_id, leg_id=leg_id)
        if not seed_run_ids:
            _raise(
                "MULTI_ALPHA leg seed_run_ids are required",
                REASON_PARENT_LEG_SEED_METADATA_MISSING,
                package_id=package_id,
                leg_id=leg_id,
            )
        seen.add(leg_id)
        legs.append(
            {
                "leg_id": leg_id,
                "child_package_id": child_package_id or None,
                "child_manifest_sha256": child_sha or None,
                "seed_run_ids": seed_run_ids,
                "ensemble_method": item.get("ensemble_method") or "mean_by_trade_date_instrument",
                "terminal_weight": item.get("terminal_weight"),
                "runtime_assets": (
                    item.get("runtime_assets")
                    if isinstance(item.get("runtime_assets"), Mapping)
                    else item.get("seed_runtime_assets")
                    if isinstance(item.get("seed_runtime_assets"), Mapping)
                    else None
                ),
            }
        )
    if len(legs) < 2:
        _raise("MULTI_ALPHA live inference requires at least two legs", REASON_PARENT_LEG_MAPPING_MISSING, package_id=package_id)
    return legs


def _parent_leg_runtime_slices(
    manifest: StrategyPackageManifest,
    *,
    evidence: Mapping[str, Any],
    package_id: str,
) -> list[ParentLegRuntimeSlice]:
    manifest_sha = str(manifest.manifest_sha256 or "").strip().lower()
    components = {component.alpha_id: component for component in manifest.alpha_components}
    legs = _legs(evidence, package_id=package_id)
    leg_ids = [leg["leg_id"] for leg in legs]
    if set(components) != set(leg_ids):
        _raise(
            "MULTI_ALPHA parent component ids and source_evidence legs do not match",
            REASON_PARENT_LEG_MAPPING_MISSING,
            package_id=package_id,
            component_leg_ids=sorted(components),
            evidence_leg_ids=sorted(leg_ids),
        )
    weight_ids = set(manifest.alpha_combination_policy.weights)
    if weight_ids and weight_ids != set(leg_ids):
        _raise(
            "MULTI_ALPHA parent weights do not match source_evidence legs",
            REASON_PARENT_LEG_MAPPING_MISSING,
            package_id=package_id,
            weight_leg_ids=sorted(weight_ids),
            evidence_leg_ids=sorted(leg_ids),
        )
    parent_runtime_assets = _parent_runtime_assets(manifest, package_id=package_id)
    model_index = _parent_model_index(manifest, package_id=package_id)
    factor_index = _parent_factor_index(manifest, package_id=package_id)
    slices: list[ParentLegRuntimeSlice] = []
    seen_component_model_ids: dict[str, str] = {}
    for leg in legs:
        leg_id = str(leg["leg_id"])
        component = components[leg_id]
        model_id = str(component.model_id or "").strip()
        if not model_id:
            _raise(
                "MULTI_ALPHA parent component is missing model_id",
                REASON_PARENT_LEG_MODEL_ID_MISSING,
                package_id=package_id,
                leg_id=leg_id,
            )
        previous_leg_id = seen_component_model_ids.get(model_id)
        if previous_leg_id is not None:
            _raise(
                "MULTI_ALPHA parent runtime requires unique model_id per leg",
                REASON_PARENT_LEG_MODEL_ASSET_AMBIGUOUS,
                package_id=package_id,
                model_id=model_id,
                first_leg_id=previous_leg_id,
                second_leg_id=leg_id,
                model_id_uniqueness_policy="unique_per_leg",
            )
        seen_component_model_ids[model_id] = leg_id
        model_asset = model_index.get(model_id)
        if model_asset is None:
            _raise(
                "MULTI_ALPHA parent model_asset is missing for leg model_id",
                REASON_PARENT_LEG_MODEL_ASSET_MISSING,
                package_id=package_id,
                leg_id=leg_id,
                model_id=model_id,
            )
        factor_refs = [str(ref or "").strip() for ref in component.lineage.factor_artifact_refs or [] if str(ref or "").strip()]
        if not factor_refs:
            _raise(
                "MULTI_ALPHA parent component is missing factor_artifact_refs",
                REASON_PARENT_LEG_FACTOR_REFS_MISSING,
                package_id=package_id,
                leg_id=leg_id,
                model_id=model_id,
            )
        runtime_assets = _leg_runtime_assets(
            leg,
            parent_runtime_assets=parent_runtime_assets,
            package_id=package_id,
            leg_id=leg_id,
            model_id=model_id,
        )
        factors = tuple(_resolve_parent_factor_ref(ref, factor_index=factor_index, package_id=package_id, leg_id=leg_id, model_id=model_id) for ref in factor_refs)
        _ensure_leg_runtime_assets_complete(
            package_id=package_id,
            leg_id=leg_id,
            model_id=model_id,
            model_asset=model_asset,
            factors=factors,
            runtime_assets=runtime_assets,
        )
        legacy_ref = str(component.lineage.model_artifact_ref or "").strip()
        slices.append(
            ParentLegRuntimeSlice(
                parent_package_id=package_id,
                parent_manifest_sha256=manifest_sha,
                leg_id=leg_id,
                component=component,
                model_asset=model_asset,
                factor_set=factors,
                runtime_assets=runtime_assets,
                seed_run_ids=tuple(leg["seed_run_ids"]),
                ensemble_method=str(leg.get("ensemble_method") or "mean_by_trade_date_instrument"),
                terminal_weight=_finite_float(leg.get("terminal_weight")),
                legacy_child_ref_ignored=legacy_ref.startswith("child_package:"),
            )
        )
    return slices


def _runtime_config_for_parent_leg(
    runtime_config: Mapping[str, Any],
    *,
    leg_slice: ParentLegRuntimeSlice,
    seed_run_id: str,
) -> dict[str, Any]:
    config = dict(runtime_config)
    artifact = dict(_artifact_config(config))
    artifact["multi_alpha_leg_id"] = leg_slice.leg_id
    artifact["multi_alpha_seed_run_id"] = seed_run_id
    artifact["multi_alpha_runtime_source"] = "parent_package_asset"
    artifact.pop("model_params_path", None)
    config["selection_artifact_config"] = artifact
    return config



def _parent_runtime_assets(manifest: StrategyPackageManifest, *, package_id: str) -> RuntimeAssetManifest | None:
    runtime_assets = manifest.runtime_assets
    alpha158 = runtime_assets.alpha158 if runtime_assets is not None else None
    if (
        runtime_assets is not None
        and alpha158 is not None
        and alpha158.enabled
        and (not alpha158.asset_ref or not alpha158.sha256 or not alpha158.aliases)
    ):
        _raise(
            "MULTI_ALPHA parent package has incomplete frozen Alpha158 schema",
            REASON_PARENT_ALPHA158_SCHEMA_MISSING,
            package_id=package_id,
            asset_ref=getattr(alpha158, "asset_ref", None),
            sha256=getattr(alpha158, "sha256", None),
            alias_count=len(getattr(alpha158, "aliases", []) or []),
        )
    return runtime_assets


def _leg_runtime_assets(
    leg: Mapping[str, Any],
    *,
    parent_runtime_assets: RuntimeAssetManifest | None,
    package_id: str,
    leg_id: str,
    model_id: str,
) -> RuntimeAssetManifest:
    raw = leg.get("runtime_assets")
    if isinstance(raw, Mapping):
        try:
            runtime_assets = RuntimeAssetManifest.model_validate(raw)
        except Exception as exc:
            _raise(
                "MULTI_ALPHA parent leg runtime_assets payload is invalid",
                REASON_PARENT_LEG_RUNTIME_ASSETS_INCOMPLETE,
                package_id=package_id,
                leg_id=leg_id,
                model_id=model_id,
                error=f"{type(exc).__name__}: {exc}",
            )
    elif parent_runtime_assets is not None:
        runtime_assets = parent_runtime_assets
    else:
        _raise(
            "MULTI_ALPHA parent leg is missing runtime asset mapping",
            REASON_PARENT_ALPHA158_SCHEMA_MISSING,
            package_id=package_id,
            leg_id=leg_id,
            model_id=model_id,
        )
    alpha158 = runtime_assets.alpha158
    if alpha158.enabled and (not alpha158.asset_ref or not alpha158.sha256 or not alpha158.aliases):
        _raise(
            "MULTI_ALPHA parent leg Alpha158 schema mapping is incomplete",
            REASON_PARENT_ALPHA158_SCHEMA_MISSING,
            package_id=package_id,
            leg_id=leg_id,
            model_id=model_id,
            asset_ref=alpha158.asset_ref,
            sha256=alpha158.sha256,
            alias_count=len(alpha158.aliases or []),
        )
    if alpha158.enabled:
        parent_alpha158 = parent_runtime_assets.alpha158 if parent_runtime_assets is not None else None
        if parent_alpha158 is None or not parent_alpha158.enabled:
            _raise(
                "MULTI_ALPHA parent package is missing leg Alpha158 schema asset",
                REASON_PARENT_ALPHA158_SCHEMA_MISSING,
                package_id=package_id,
                leg_id=leg_id,
                model_id=model_id,
                leg_asset_ref=alpha158.asset_ref,
                leg_sha256=alpha158.sha256,
            )
        if (
            parent_alpha158.asset_ref != alpha158.asset_ref
            or str(parent_alpha158.sha256 or "").strip().lower() != str(alpha158.sha256 or "").strip().lower()
            or list(parent_alpha158.aliases) != list(alpha158.aliases)
        ):
            _raise(
                "MULTI_ALPHA parent leg Alpha158 schema is not backed by parent runtime_assets",
                REASON_PARENT_ALPHA158_SCHEMA_MISMATCH,
                package_id=package_id,
                leg_id=leg_id,
                model_id=model_id,
                leg_asset_ref=alpha158.asset_ref,
                leg_sha256=alpha158.sha256,
                parent_asset_ref=getattr(parent_alpha158, "asset_ref", None),
                parent_sha256=getattr(parent_alpha158, "sha256", None),
            )
    return runtime_assets


def _parent_model_index(manifest: StrategyPackageManifest, *, package_id: str) -> dict[str, ModelAsset]:
    models = manifest.model_asset if isinstance(manifest.model_asset, list) else [manifest.model_asset]
    index: dict[str, ModelAsset] = {}
    seen_sha: dict[str, str | None] = {}
    for model in models:
        if not isinstance(model, ModelAsset):
            _raise(
                "MULTI_ALPHA parent model_asset entry is invalid",
                REASON_PARENT_LEG_MODEL_ASSET_MISSING,
                package_id=package_id,
            )
        model_id = str(model.model_id or "").strip()
        if not model_id:
            _raise(
                "MULTI_ALPHA parent model_asset is missing model_id",
                REASON_PARENT_LEG_MODEL_ID_MISSING,
                package_id=package_id,
            )
        sha = str(model.sha256 or "").strip().lower() or None
        if model_id in index:
            if seen_sha.get(model_id) != sha:
                _raise(
                    "MULTI_ALPHA parent has multiple model assets for one model_id",
                    REASON_PARENT_LEG_MODEL_ASSET_AMBIGUOUS,
                    package_id=package_id,
                    model_id=model_id,
                    first_sha256=seen_sha.get(model_id),
                    second_sha256=sha,
                )
            _raise(
                "MULTI_ALPHA parent model_id maps to multiple model assets",
                REASON_PARENT_LEG_MODEL_ASSET_AMBIGUOUS,
                package_id=package_id,
                model_id=model_id,
                sha256=sha,
            )
        index[model_id] = model
        seen_sha[model_id] = sha
    return index


def _parent_factor_index(
    manifest: StrategyPackageManifest,
    *,
    package_id: str,
) -> dict[str, dict[str, FactorAsset]]:
    index: dict[str, dict[str, FactorAsset]] = {"factor_id": {}, "factor_name": {}}
    sha_seen: dict[tuple[str, str], str | None] = {}
    for factor in manifest.factor_set:
        if not isinstance(factor, FactorAsset):
            _raise(
                "MULTI_ALPHA parent factor_set entry is invalid",
                REASON_PARENT_LEG_FACTOR_ASSET_MISSING,
                package_id=package_id,
            )
        for key_name, raw_key in (("factor_id", factor.factor_id), ("factor_name", factor.factor_name)):
            key = str(raw_key or "").strip()
            if not key:
                continue
            sha = str(factor.sha256 or "").strip().lower() or None
            seen_key = (key_name, key)
            if key in index[key_name]:
                if sha_seen.get(seen_key) != sha:
                    _raise(
                        "MULTI_ALPHA parent factor ref collides with different sha256",
                        REASON_PARENT_LEG_FACTOR_ASSET_AMBIGUOUS,
                        package_id=package_id,
                        factor_key_type=key_name,
                        factor_key=key,
                        first_sha256=sha_seen.get(seen_key),
                        second_sha256=sha,
                    )
                _raise(
                    "MULTI_ALPHA parent factor ref maps to multiple factor assets",
                    REASON_PARENT_LEG_FACTOR_ASSET_AMBIGUOUS,
                    package_id=package_id,
                    factor_key_type=key_name,
                    factor_key=key,
                    sha256=sha,
                )
            index[key_name][key] = factor
            sha_seen[seen_key] = sha
    return index


def _resolve_parent_factor_ref(
    ref: str,
    *,
    factor_index: Mapping[str, Mapping[str, FactorAsset]],
    package_id: str,
    leg_id: str,
    model_id: str,
) -> FactorAsset:
    matches: dict[tuple[str, str], FactorAsset] = {}
    by_name = factor_index.get("factor_name", {}).get(ref)
    if by_name is not None:
        matches[("factor_name", ref)] = by_name
    by_id = factor_index.get("factor_id", {}).get(ref)
    if by_id is not None:
        matches[("factor_id", ref)] = by_id
    unique = {id(asset): asset for asset in matches.values()}
    if not unique:
        _raise(
            "MULTI_ALPHA parent factor asset is missing for component ref",
            REASON_PARENT_LEG_FACTOR_ASSET_MISSING,
            package_id=package_id,
            leg_id=leg_id,
            model_id=model_id,
            factor_ref=ref,
        )
    if len(unique) > 1:
        _raise(
            "MULTI_ALPHA parent factor ref is ambiguous",
            REASON_PARENT_LEG_FACTOR_ASSET_AMBIGUOUS,
            package_id=package_id,
            leg_id=leg_id,
            model_id=model_id,
            factor_ref=ref,
            match_keys=[f"{kind}:{key}" for kind, key in matches],
        )
    return next(iter(unique.values()))


def _ensure_leg_runtime_assets_complete(
    *,
    package_id: str,
    leg_id: str,
    model_id: str,
    model_asset: ModelAsset,
    factors: Sequence[FactorAsset],
    runtime_assets: RuntimeAssetManifest,
) -> None:
    missing: list[dict[str, Any]] = []
    if not model_asset.asset_ref or not model_asset.sha256:
        missing.append(
            {
                "asset_kind": "model_weight",
                "model_id": model_id,
                "asset_ref": model_asset.asset_ref,
                "sha256": model_asset.sha256,
            }
        )
    if model_asset.model_code_required and not model_asset.model_code_assets:
        missing.append({"asset_kind": "model_code", "model_id": model_id, "reason": "model_code_required"})
    for code_asset in model_asset.model_code_assets or []:
        if not code_asset.asset_ref or not code_asset.sha256:
            missing.append(
                {
                    "asset_kind": "model_code",
                    "model_id": model_id,
                    "relative_path": code_asset.relative_path,
                    "asset_ref": code_asset.asset_ref,
                    "sha256": code_asset.sha256,
                }
            )
    for factor in factors:
        if not factor.asset_ref or not factor.sha256:
            missing.append(
                {
                    "asset_kind": "factor_code",
                    "factor_id": factor.factor_id,
                    "factor_name": factor.factor_name,
                    "asset_ref": factor.asset_ref,
                    "sha256": factor.sha256,
                }
            )
    alpha158 = runtime_assets.alpha158
    if alpha158.enabled and (not alpha158.asset_ref or not alpha158.sha256 or not alpha158.aliases):
        missing.append(
            {
                "asset_kind": "factor_schema",
                "logical_name": "alpha158_schema",
                "asset_ref": alpha158.asset_ref,
                "sha256": alpha158.sha256,
                "alias_count": len(alpha158.aliases or []),
            }
        )
    if missing:
        _raise(
            "MULTI_ALPHA parent leg runtime assets are incomplete",
            REASON_PARENT_LEG_RUNTIME_ASSETS_INCOMPLETE,
            package_id=package_id,
            leg_id=leg_id,
            model_id=model_id,
            missing_assets=missing,
        )


def _live_result_to_frame(
    rows: Sequence[Mapping[str, Any]],
    *,
    package_id: str,
    leg_id: str,
    model_id: str,
    seed_run_id: str,
    trade_date: date,
    allow_empty: bool = False,
) -> pd.DataFrame:
    if not rows:
        if allow_empty:
            return pd.DataFrame(columns=["trade_date", "instrument", "score"])
        _raise(
            "live inference returned no score rows for required MULTI_ALPHA seed",
            REASON_SEED_PREDICTION_MISSING,
            package_id=package_id,
            leg_id=leg_id,
            model_id=model_id,
            seed_run_id=seed_run_id,
            trade_date=trade_date.isoformat(),
        )
    normalized: list[dict[str, Any]] = []
    for row in rows:
        symbol = row.get("symbol") or row.get("instrument")
        score = _finite_float(row.get("score"))
        if symbol is None or score is None:
            continue
        normalized.append({"trade_date": trade_date, "instrument": str(symbol), "score": score})
    if not normalized:
        _raise(
            "live inference returned no finite score rows for required MULTI_ALPHA seed",
            REASON_SEED_PREDICTION_MISSING,
            package_id=package_id,
            leg_id=leg_id,
            model_id=model_id,
            seed_run_id=seed_run_id,
            trade_date=trade_date.isoformat(),
        )
    return normalize_prediction_frame(pd.DataFrame(normalized), run_id=f"{leg_id}:{seed_run_id}")


def _ensemble_seed_frames(
    seed_frames: Mapping[str, pd.DataFrame],
    *,
    leg_id: str,
    model_id: str,
    package_id: str,
    trade_date: date,
    allow_empty: bool = False,
) -> pd.DataFrame:
    merged: pd.DataFrame | None = None
    for seed_run_id, frame in seed_frames.items():
        selected = frame[["trade_date", "instrument", "score"]].rename(columns={"score": f"score__{seed_run_id}"})
        merged = selected if merged is None else merged.merge(selected, on=["trade_date", "instrument"], how="inner")
    if merged is None or merged.empty:
        if allow_empty:
            return pd.DataFrame(columns=["trade_date", "instrument", "score"])
        _raise(
            "MULTI_ALPHA seed score panels have no common candidates",
            REASON_SEED_PREDICTION_MISSING,
            package_id=package_id,
            leg_id=leg_id,
            model_id=model_id,
            trade_date=trade_date.isoformat(),
            seed_run_ids=list(seed_frames),
        )
    score_cols = [col for col in merged.columns if str(col).startswith("score__")]
    merged["score"] = merged[score_cols].mean(axis=1)
    return merged[["trade_date", "instrument", "score"]].sort_values(["trade_date", "instrument"]).reset_index(drop=True)


def _normalize_leg_frame(
    frame: pd.DataFrame,
    *,
    package_id: str,
    leg_id: str,
    model_id: str,
    method: str,
) -> pd.DataFrame:
    data = frame.copy()
    data["raw_score"] = pd.to_numeric(data["score"], errors="coerce")
    if data["raw_score"].isna().any():
        raise ArtifactGenerationFailedError(
            "MULTI_ALPHA leg score contains non-finite values",
            context={
                "reason_code": REASON_SEED_PREDICTION_MISSING,
                "package_id": package_id,
                "leg_id": leg_id,
                "model_id": model_id,
            },
        )
    if method == "zscore":
        data["normalized_score"] = data.groupby("trade_date")["raw_score"].transform(_zscore)
    elif method == "rank":
        data["normalized_score"] = data.groupby("trade_date")["raw_score"].transform(_rank_score)
    else:
        _raise("unsupported MULTI_ALPHA normalization method", REASON_COMPONENT_COVERAGE_LOW, leg_id=leg_id, method=method)
    return data[["trade_date", "instrument", "raw_score", "normalized_score"]]


def _align_component_frames(
    frames: Mapping[str, pd.DataFrame],
    *,
    allow_empty: bool = False,
) -> pd.DataFrame:
    merged: pd.DataFrame | None = None
    for leg_id, frame in frames.items():
        selected = frame[["trade_date", "instrument", "raw_score", "normalized_score"]].rename(
            columns={"raw_score": f"raw__{leg_id}", "normalized_score": f"norm__{leg_id}"}
        )
        merged = selected if merged is None else merged.merge(selected, on=["trade_date", "instrument"], how="inner")
    if merged is None or merged.empty:
        if allow_empty:
            columns = ["trade_date", "instrument"]
            for leg_id in frames:
                columns.extend([f"raw__{leg_id}", f"norm__{leg_id}"])
            return pd.DataFrame(columns=columns)
        _raise("MULTI_ALPHA legs have no common candidates", REASON_COMPONENT_COVERAGE_LOW)
    return merged.sort_values(["trade_date", "instrument"]).reset_index(drop=True)


def _combine_aligned(aligned: pd.DataFrame, *, weights: Mapping[str, float], leg_ids: Sequence[str]) -> pd.DataFrame:
    result = aligned.copy()
    result["combined_score"] = 0.0
    for leg_id in leg_ids:
        if leg_id not in weights:
            _raise("MULTI_ALPHA weights are missing a leg", REASON_WEIGHT_UNAVAILABLE, leg_id=leg_id)
        result["combined_score"] += float(weights[leg_id]) * result[f"norm__{leg_id}"]
    return result


def _zscore(series: pd.Series) -> pd.Series:
    std = float(series.std(ddof=0))
    if not math.isfinite(std) or std <= 0:
        return pd.Series(0.0, index=series.index)
    mean = float(series.mean())
    return (series - mean) / std


def _rank_score(series: pd.Series) -> pd.Series:
    n = len(series)
    if n <= 1:
        return pd.Series(0.0, index=series.index)
    ranks = series.rank(method="average", ascending=True)
    return ((ranks - 1) / (n - 1)) * 2 - 1


def _normalization_method(manifest: StrategyPackageManifest) -> str:
    context = manifest.backtest_context or {}
    raw_config = context.get("raw_backtest_config") if isinstance(context, Mapping) else {}
    candidates = [
        context.get("normalize_method") if isinstance(context, Mapping) else None,
        raw_config.get("normalize_method") if isinstance(raw_config, Mapping) else None,
        getattr(manifest.alpha_combination_policy, "normalization_scope", None),
    ]
    for candidate in candidates:
        text = str(candidate or "").strip().lower()
        if text in {"zscore", "rank"}:
            return text
    return "zscore"


def _label_horizon(manifest: StrategyPackageManifest) -> int:
    label = (manifest.backtest_context or {}).get("label") if isinstance(manifest.backtest_context, Mapping) else None
    if isinstance(label, Mapping) and label.get("label_horizon") is not None:
        return _positive_int(label.get("label_horizon"), default=20, field_name="label_horizon")
    return 20


def _artifact_config(runtime_config: Mapping[str, Any] | None) -> dict[str, Any]:
    config = runtime_config or {}
    artifact = config.get("selection_artifact_config")
    if artifact is None:
        artifact = config.get("selection_artifact")
    if artifact is None:
        return {}
    if not isinstance(artifact, Mapping):
        raise RuntimeConfigInvalidError(
            "runtime_config.selection_artifact_config must be an object",
            context={"selection_artifact_config_type": type(artifact).__name__},
        )
    return dict(artifact)


def _aggregate_parent_leg_input_context(
    *,
    leg_executions: Mapping[str, ParentLegLiveInferenceResult],
    requested_trade_date: date,
) -> dict[str, Any]:
    """Prove all parent legs were inferred against one compatible PIT context."""

    required_fields = (
        "effective_trade_date",
        "score_trade_date",
        "pit_mode",
        "calendar_version",
        "calendar_hash",
        "calendar_source",
    )
    baseline: dict[str, Any] | None = None
    baseline_universe_hash: str | None = None
    baseline_universe_count: int | None = None
    per_leg_universe_counts: dict[str, int] = {}
    for leg_id, execution in sorted(leg_executions.items()):
        result = execution.live_result
        raw_context = getattr(result, "input_context", None)
        if not isinstance(raw_context, Mapping):
            raise DataUnavailableError(
                "MULTI_ALPHA leg inference is missing an input context",
                context={"reason_code": "ADVISORY_PHASE0A2C_SOURCE_RECEIPT_INCOMPLETE", "leg_id": leg_id},
            )
        missing = [name for name in required_fields if not raw_context.get(name)]
        if missing:
            raise DataUnavailableError(
                "MULTI_ALPHA leg input context is incomplete",
                context={
                    "reason_code": "ADVISORY_PHASE0A2C_SOURCE_RECEIPT_INCOMPLETE",
                    "leg_id": leg_id,
                    "missing_fields": missing,
                },
            )
        raw_count = getattr(result, "universe_count", None)
        if isinstance(raw_count, bool):
            raw_count = None
        try:
            universe_count = int(raw_count)
        except (TypeError, ValueError) as exc:
            raise DataUnavailableError(
                "MULTI_ALPHA leg inference is missing an actual input universe count",
                context={"reason_code": "ADVISORY_PHASE0A2C_UNIVERSE_RECEIPT_INCOMPLETE", "leg_id": leg_id},
            ) from exc
        if universe_count < len(getattr(result, "scores", []) or []):
            raise DataUnavailableError(
                "MULTI_ALPHA leg input universe count is smaller than its score rows",
                context={
                    "reason_code": "ADVISORY_PHASE0A2C_UNIVERSE_RECEIPT_INCOMPLETE",
                    "leg_id": leg_id,
                    "universe_count": universe_count,
                    "score_row_count": len(getattr(result, "scores", []) or []),
                },
            )
        if universe_count <= 0:
            raise DataUnavailableError(
                "MULTI_ALPHA leg inference input universe must be positive",
                context={
                    "reason_code": "ADVISORY_PHASE0A_VALID_NO_CANDIDATE_EVIDENCE_INCOMPLETE",
                    "leg_id": leg_id,
                    "universe_count": universe_count,
                },
            )
        universe_input_hash = raw_context.get("universe_input_hash")
        if not isinstance(universe_input_hash, str) or len(universe_input_hash) != 64:
            raise DataUnavailableError(
                "MULTI_ALPHA leg input context is missing a canonical universe hash",
                context={
                    "reason_code": "ADVISORY_PHASE0A2C_UNIVERSE_RECEIPT_INCOMPLETE",
                    "leg_id": leg_id,
                },
            )
        candidate = {field: raw_context[field] for field in required_fields}
        if baseline is None:
            baseline = candidate
        elif candidate != baseline:
            raise DataUnavailableError(
                "MULTI_ALPHA legs were inferred with different PIT input contexts",
                context={
                    "reason_code": "ADVISORY_PHASE0A2C_LINEAGE_MISMATCH",
                    "baseline": baseline,
                    "leg_id": leg_id,
                    "leg_context": candidate,
                },
            )
        if baseline_universe_hash is None:
            baseline_universe_hash = universe_input_hash
            baseline_universe_count = universe_count
        elif universe_input_hash != baseline_universe_hash or universe_count != baseline_universe_count:
            raise DataUnavailableError(
                "MULTI_ALPHA legs were inferred with different input universes",
                context={
                    "reason_code": "ADVISORY_PHASE0A2C_LINEAGE_MISMATCH",
                    "baseline_universe_hash": baseline_universe_hash,
                    "baseline_universe_count": baseline_universe_count,
                    "leg_id": leg_id,
                    "leg_universe_hash": universe_input_hash,
                    "leg_universe_count": universe_count,
                },
            )
        per_leg_universe_counts[leg_id] = universe_count
    if baseline is None or baseline_universe_hash is None or baseline_universe_count is None:
        raise DataUnavailableError(
            "MULTI_ALPHA parent has no leg input contexts",
            context={"reason_code": "ADVISORY_PHASE0A2C_SOURCE_RECEIPT_INCOMPLETE"},
        )
    return {
        "requested_trade_date": requested_trade_date.isoformat(),
        **baseline,
        "universe_input_hash": baseline_universe_hash,
        "parent_input_universe_count": baseline_universe_count,
        "per_leg_universe_counts": per_leg_universe_counts,
    }


def _has_positive_actual_universe_count(result: Any) -> bool:
    value = getattr(result, "universe_count", None)
    if isinstance(value, bool):
        return False
    try:
        return int(value) > 0
    except (TypeError, ValueError):
        return False


def _normalize_positive_weights(raw: Mapping[str, float], *, reason_code: str, context: Mapping[str, Any]) -> dict[str, float]:
    values = {str(key): float(value) for key, value in raw.items()}
    total = sum(value for value in values.values() if value > 0 and math.isfinite(value))
    if total <= 0:
        _raise("MULTI_ALPHA weights contain no positive finite values", reason_code, **dict(context), weights=values)
    return {key: (value / total if value > 0 and math.isfinite(value) else 0.0) for key, value in values.items()}


def _positive_int(value: Any, *, default: int, field_name: str) -> int:
    if value is None:
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeConfigInvalidError(f"{field_name} must be an integer", context={field_name: value}) from exc
    if parsed <= 0:
        raise RuntimeConfigInvalidError(f"{field_name} must be positive", context={field_name: value})
    return parsed


def _non_negative_int(value: Any, *, default: int, field_name: str) -> int:
    if value is None:
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeConfigInvalidError(f"{field_name} must be an integer", context={field_name: value}) from exc
    if parsed < 0:
        raise RuntimeConfigInvalidError(f"{field_name} must be non-negative", context={field_name: value})
    return parsed


def _finite_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def _parse_date(value: Any) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, time.min, tzinfo=timezone.utc)
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _shift_trading_days(trading_days: Sequence[date], anchor: date, offset: int) -> date | None:
    ordered = sorted(set(trading_days))
    if anchor not in ordered:
        return None
    target = ordered.index(anchor) + offset
    if target < 0 or target >= len(ordered):
        return None
    return ordered[target]


def _frame_sha256(frame: pd.DataFrame, *, score_column: str) -> str:
    selected = frame[["trade_date", "instrument", score_column]].copy()
    selected["trade_date"] = selected["trade_date"].astype(str)
    selected = selected.sort_values(["trade_date", "instrument"]).reset_index(drop=True)
    return _canonical_sha256(selected.to_dict(orient="records"))


def _canonical_sha256(payload: Any) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _raise(message: str, reason_code: str, **context: Any) -> None:
    error_context = {"reason_code": reason_code, **context}
    if reason_code in {
        REASON_WEIGHT_UNAVAILABLE,
        REASON_LABEL_WINDOW_INSUFFICIENT,
        REASON_WEIGHT_ALL_NON_POSITIVE,
        REASON_SEED_PREDICTION_MISSING,
        REASON_COMPONENT_COVERAGE_LOW,
        REASON_CHILD_MANIFEST_MISMATCH,
        REASON_LEG_MISSING,
        REASON_PARENT_LEG_MAPPING_MISSING,
        REASON_PARENT_LEG_SEED_METADATA_MISSING,
        REASON_PARENT_LEG_MODEL_ASSET_MISSING,
        REASON_PARENT_LEG_MODEL_ASSET_AMBIGUOUS,
        REASON_PARENT_LEG_MODEL_ID_MISSING,
        REASON_PARENT_LEG_FACTOR_REFS_MISSING,
        REASON_PARENT_LEG_FACTOR_ASSET_MISSING,
        REASON_PARENT_LEG_FACTOR_ASSET_AMBIGUOUS,
        REASON_PARENT_ALPHA158_SCHEMA_MISSING,
        REASON_PARENT_ALPHA158_SCHEMA_MISMATCH,
        REASON_PARENT_LEG_RUNTIME_ASSETS_INCOMPLETE,
        REASON_PARENT_LEG_INFERENCE_EMPTY,
    }:
        raise DataUnavailableError(message, context=error_context)
    if reason_code == REASON_RUNTIME_NOT_ENABLED:
        raise UnsupportedFeatureError(message, context=error_context)
    if reason_code == REASON_TOPK_RUNTIME_MISMATCH:
        raise RuntimeConfigInvalidError(message, context=error_context)
    if reason_code == REASON_DEADLINE_EXCEEDED:
        raise RuntimeConfigInvalidError(message, context=error_context)
    raise StrategyPackageValidationError(message, context=error_context)
