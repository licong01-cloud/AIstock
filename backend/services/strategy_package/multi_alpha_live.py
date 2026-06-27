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
from backend.services.strategy_package.models import AlphaMode, SelectionScoreArtifactStatus, StrategyPackageManifest
from backend.services.strategy_package.selection_artifact import (
    SelectionScoreArtifact,
    selection_artifact_runtime_hash,
)
from backend.services.trading_calendar_status import TradingCalendarStatusService
from backend.services.trading_core.errors import (
    ArtifactGenerationFailedError,
    DataUnavailableError,
    RuntimeConfigInvalidError,
    StrategyPackageValidationError,
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


@dataclass(frozen=True)
class MultiAlphaWeightArtifact:
    artifact_id: str
    artifact_sha256: str
    weights: dict[str, float]
    metadata: dict[str, Any]


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
        runtime_hash = multi_alpha_selection_artifact_runtime_hash(manifest, config)

        artifacts: list[SelectionScoreArtifact] = []
        for current_date in sorted(set(trade_dates)):
            score_trade_date = cutoff_date or current_date
            leg_frames: dict[str, pd.DataFrame] = {}
            component_metadata: dict[str, Any] = {}
            for leg in legs:
                child = self._validate_child(leg, parent_manifest=manifest)
                seed_frames = self._run_seed_live_inference(
                    leg=leg,
                    child_record=child,
                    trade_date=current_date,
                    cutoff_date=cutoff_date,
                    runtime_config=config,
                    inference_backend=inference_backend,
                )
                ensemble = _ensemble_seed_frames(
                    seed_frames,
                    leg_id=leg["leg_id"],
                    package_id=package_id,
                    trade_date=score_trade_date,
                )
                normalized = _normalize_leg_frame(ensemble, leg_id=leg["leg_id"], method=normalization_method)
                leg_frames[leg["leg_id"]] = normalized
                component_sha = _frame_sha256(normalized, score_column="normalized_score")
                component_metadata[leg["leg_id"]] = {
                    "component_score_artifact_id": f"macs_{component_sha[:24]}",
                    "component_score_artifact_sha256": component_sha,
                    "child_package_id": leg["child_package_id"],
                    "child_manifest_sha256": leg["child_manifest_sha256"],
                    "seed_run_ids": list(leg["seed_run_ids"]),
                    "seed_count": len(seed_frames),
                    "ensemble_method": leg.get("ensemble_method") or "mean_by_trade_date_instrument",
                    "candidate_count": int(len(normalized)),
                }

            aligned = _align_component_frames(leg_frames)
            component_candidate_universe_size = int(len(aligned))
            if component_candidate_universe_size < coverage_threshold:
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
            combined = _combine_aligned(aligned, weights=weight_artifact.weights, leg_ids=leg_ids)
            rows = self._artifact_rows(
                combined,
                leg_ids=leg_ids,
                weights=weight_artifact.weights,
                topk=topk,
                trade_date=score_trade_date,
                include_reference_price=include_reference_price,
            )
            combined_sha = _canonical_sha256(rows)
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
                "component_manifest_sha256": {
                    leg["leg_id"]: leg["child_manifest_sha256"] for leg in legs
                },
                "seed_run_ids": {leg["leg_id"]: list(leg["seed_run_ids"]) for leg in legs},
                "combine_backtest_run_id": evidence.get("combine_backtest_run_id"),
                "normalization_method": normalization_method,
                "final_topk": topk,
                "component_candidate_universe_size": component_candidate_universe_size,
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
            }
            artifact = SelectionScoreArtifact(
                package_id=package_id,
                manifest_sha256=manifest.manifest_sha256,
                trade_date=current_date,
                data_source=data_source,
                runtime_config_hash=runtime_hash,
                scores_json=rows,
                score_count=len(rows),
                universe_count=component_candidate_universe_size,
                top_score_symbol=rows[0]["symbol"] if rows else None,
                status=SelectionScoreArtifactStatus.SUCCEEDED,
                metadata=metadata,
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

    def _validate_child(self, leg: Mapping[str, Any], *, parent_manifest: StrategyPackageManifest) -> Any:
        child_package_id = str(leg.get("child_package_id") or "").strip()
        if not child_package_id:
            _raise(
                "MULTI_ALPHA leg is missing child_package_id",
                REASON_LEG_MISSING,
                package_id=parent_manifest.package_id,
                leg_id=leg.get("leg_id"),
            )
        try:
            child = self.package_repository.get(child_package_id)
        except DataUnavailableError as exc:
            raise DataUnavailableError(
                "MULTI_ALPHA child package does not exist",
                context={
                    "reason_code": REASON_LEG_MISSING,
                    "package_id": parent_manifest.package_id,
                    "leg_id": leg.get("leg_id"),
                    "child_package_id": child_package_id,
                },
            ) from exc
        expected_sha = str(leg.get("child_manifest_sha256") or "").strip().lower()
        if child.manifest_sha256 != expected_sha:
            _raise(
                "MULTI_ALPHA child manifest sha does not match frozen parent manifest",
                REASON_CHILD_MANIFEST_MISMATCH,
                package_id=parent_manifest.package_id,
                leg_id=leg.get("leg_id"),
                child_package_id=child_package_id,
                expected_child_manifest_sha256=expected_sha,
                actual_child_manifest_sha256=child.manifest_sha256,
            )
        return child

    def _run_seed_live_inference(
        self,
        *,
        leg: Mapping[str, Any],
        child_record: Any,
        trade_date: date,
        cutoff_date: date | None,
        runtime_config: Mapping[str, Any],
        inference_backend: str,
    ) -> dict[str, pd.DataFrame]:
        seed_run_ids = [str(item or "").strip() for item in leg.get("seed_run_ids") or [] if str(item or "").strip()]
        if not seed_run_ids:
            _raise(
                "MULTI_ALPHA leg has no seed_run_ids",
                REASON_SEED_PREDICTION_MISSING,
                leg_id=leg.get("leg_id"),
                child_package_id=child_record.package_id,
            )
        frames: dict[str, pd.DataFrame] = {}
        for seed_run_id in seed_run_ids:
            seed_config = _runtime_config_for_seed(runtime_config, leg=leg, seed_run_id=seed_run_id, child_record=child_record)
            if not _seed_has_runtime_binding(seed_config, child_record=child_record, seed_run_id=seed_run_id):
                _raise(
                    "MULTI_ALPHA seed runtime asset binding is missing",
                    REASON_SEED_PREDICTION_MISSING,
                    leg_id=leg.get("leg_id"),
                    child_package_id=child_record.package_id,
                    seed_run_id=seed_run_id,
                    child_record_run_id=child_record.run_id,
                )
            source_loader = getattr(self.runtime_asset_resolver, "load_source_for_strategy_package", None)
            if callable(source_loader):
                source = source_loader(
                    source_type=child_record.source_type,
                    source_id=child_record.source_id,
                    loop_id=child_record.loop_id,
                    run_id=seed_run_id,
                )
            else:
                source = self.runtime_asset_resolver.load_source(child_record.source_id)
            prepared = self.runtime_asset_resolver.prepare_workspace(
                package_id=child_record.package_id,
                manifest_sha256=child_record.manifest_sha256,
                source=source,
                runtime_config=seed_config,
                path_converter=win_to_wsl_path if inference_backend == "wsl" else None,
            )
            result = self.live_inference_provider.run(
                workspace=prepared,
                trade_date=trade_date,
                cutoff_date=cutoff_date,
            )
            frame = _live_result_to_frame(
                result.scores,
                package_id=child_record.package_id,
                leg_id=str(leg.get("leg_id")),
                seed_run_id=seed_run_id,
                trade_date=cutoff_date or trade_date,
            )
            if frame.empty:
                _raise(
                    "MULTI_ALPHA seed live inference returned no usable scores",
                    REASON_SEED_PREDICTION_MISSING,
                    leg_id=leg.get("leg_id"),
                    child_package_id=child_record.package_id,
                    seed_run_id=seed_run_id,
                    trade_date=trade_date.isoformat(),
                )
            frames[seed_run_id] = frame
        return frames

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
            REASON_LEG_MISSING,
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
        _raise("MULTI_ALPHA evidence legs must be a list", REASON_LEG_MISSING, package_id=package_id)
    legs: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, Mapping):
            _raise("MULTI_ALPHA leg entry must be an object", REASON_LEG_MISSING, package_id=package_id)
        leg_id = str(item.get("leg_id") or "").strip()
        child_package_id = str(item.get("child_package_id") or "").strip()
        child_sha = str(item.get("child_manifest_sha256") or "").strip().lower()
        seed_run_ids = [str(seed or "").strip() for seed in item.get("seed_run_ids") or [] if str(seed or "").strip()]
        if not leg_id or not child_package_id or not child_sha:
            _raise(
                "MULTI_ALPHA leg is incomplete",
                REASON_LEG_MISSING,
                package_id=package_id,
                leg_id=leg_id,
                child_package_id=child_package_id,
            )
        if leg_id in seen:
            _raise("MULTI_ALPHA leg_id must be unique", REASON_LEG_MISSING, package_id=package_id, leg_id=leg_id)
        seen.add(leg_id)
        legs.append(
            {
                "leg_id": leg_id,
                "child_package_id": child_package_id,
                "child_manifest_sha256": child_sha,
                "seed_run_ids": seed_run_ids,
                "ensemble_method": item.get("ensemble_method") or "mean_by_trade_date_instrument",
                "seed_runtime_assets": item.get("seed_runtime_assets") if isinstance(item.get("seed_runtime_assets"), Mapping) else {},
            }
        )
    if len(legs) < 2:
        _raise("MULTI_ALPHA live inference requires at least two legs", REASON_LEG_MISSING, package_id=package_id)
    return legs


def _runtime_config_for_seed(
    runtime_config: Mapping[str, Any],
    *,
    leg: Mapping[str, Any],
    seed_run_id: str,
    child_record: Any,
) -> dict[str, Any]:
    config = dict(runtime_config)
    artifact = dict(_artifact_config(config))
    artifact["multi_alpha_leg_id"] = leg.get("leg_id")
    artifact["multi_alpha_seed_run_id"] = seed_run_id
    explicit_model_path = _seed_model_params_path(artifact, leg=leg, seed_run_id=seed_run_id)
    if explicit_model_path:
        artifact["model_params_path"] = explicit_model_path
    config["selection_artifact_config"] = artifact
    return config


def _seed_model_params_path(artifact_config: Mapping[str, Any], *, leg: Mapping[str, Any], seed_run_id: str) -> str | None:
    mapping = artifact_config.get("seed_model_params_paths") or artifact_config.get("multi_alpha_seed_model_params_paths")
    leg_id = str(leg.get("leg_id") or "")
    if isinstance(mapping, Mapping):
        direct = mapping.get(seed_run_id)
        nested = mapping.get(leg_id)
        if direct:
            return str(direct)
        if isinstance(nested, Mapping) and nested.get(seed_run_id):
            return str(nested[seed_run_id])
    leg_assets = leg.get("seed_runtime_assets")
    if isinstance(leg_assets, Mapping):
        seed_asset = leg_assets.get(seed_run_id)
        if isinstance(seed_asset, Mapping) and seed_asset.get("model_params_path"):
            return str(seed_asset["model_params_path"])
    return None


def _seed_has_runtime_binding(config: Mapping[str, Any], *, child_record: Any, seed_run_id: str) -> bool:
    artifact = _artifact_config(config)
    if artifact.get("model_params_path"):
        return True
    return str(child_record.run_id or "").strip() == seed_run_id


def _live_result_to_frame(
    rows: Sequence[Mapping[str, Any]],
    *,
    package_id: str,
    leg_id: str,
    seed_run_id: str,
    trade_date: date,
) -> pd.DataFrame:
    if not rows:
        _raise(
            "live inference returned no score rows for required MULTI_ALPHA seed",
            REASON_SEED_PREDICTION_MISSING,
            package_id=package_id,
            leg_id=leg_id,
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
            seed_run_id=seed_run_id,
            trade_date=trade_date.isoformat(),
        )
    return normalize_prediction_frame(pd.DataFrame(normalized), run_id=f"{leg_id}:{seed_run_id}")


def _ensemble_seed_frames(
    seed_frames: Mapping[str, pd.DataFrame],
    *,
    leg_id: str,
    package_id: str,
    trade_date: date,
) -> pd.DataFrame:
    merged: pd.DataFrame | None = None
    for seed_run_id, frame in seed_frames.items():
        selected = frame[["trade_date", "instrument", "score"]].rename(columns={"score": f"score__{seed_run_id}"})
        merged = selected if merged is None else merged.merge(selected, on=["trade_date", "instrument"], how="inner")
    if merged is None or merged.empty:
        _raise(
            "MULTI_ALPHA seed score panels have no common candidates",
            REASON_SEED_PREDICTION_MISSING,
            package_id=package_id,
            leg_id=leg_id,
            trade_date=trade_date.isoformat(),
            seed_run_ids=list(seed_frames),
        )
    score_cols = [col for col in merged.columns if str(col).startswith("score__")]
    merged["score"] = merged[score_cols].mean(axis=1)
    return merged[["trade_date", "instrument", "score"]].sort_values(["trade_date", "instrument"]).reset_index(drop=True)


def _normalize_leg_frame(frame: pd.DataFrame, *, leg_id: str, method: str) -> pd.DataFrame:
    data = frame.copy()
    data["raw_score"] = pd.to_numeric(data["score"], errors="coerce")
    if data["raw_score"].isna().any():
        raise ArtifactGenerationFailedError(
            "MULTI_ALPHA leg score contains non-finite values",
            context={"reason_code": REASON_SEED_PREDICTION_MISSING, "leg_id": leg_id},
        )
    if method == "zscore":
        data["normalized_score"] = data.groupby("trade_date")["raw_score"].transform(_zscore)
    elif method == "rank":
        data["normalized_score"] = data.groupby("trade_date")["raw_score"].transform(_rank_score)
    else:
        _raise("unsupported MULTI_ALPHA normalization method", REASON_COMPONENT_COVERAGE_LOW, leg_id=leg_id, method=method)
    return data[["trade_date", "instrument", "raw_score", "normalized_score"]]


def _align_component_frames(frames: Mapping[str, pd.DataFrame]) -> pd.DataFrame:
    merged: pd.DataFrame | None = None
    for leg_id, frame in frames.items():
        selected = frame[["trade_date", "instrument", "raw_score", "normalized_score"]].rename(
            columns={"raw_score": f"raw__{leg_id}", "normalized_score": f"norm__{leg_id}"}
        )
        merged = selected if merged is None else merged.merge(selected, on=["trade_date", "instrument"], how="inner")
    if merged is None or merged.empty:
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
    }:
        raise DataUnavailableError(message, context=error_context)
    if reason_code == REASON_RUNTIME_NOT_ENABLED:
        raise UnsupportedFeatureError(message, context=error_context)
    if reason_code == REASON_TOPK_RUNTIME_MISMATCH:
        raise RuntimeConfigInvalidError(message, context=error_context)
    if reason_code == REASON_DEADLINE_EXCEEDED:
        raise RuntimeConfigInvalidError(message, context=error_context)
    raise StrategyPackageValidationError(message, context=error_context)
