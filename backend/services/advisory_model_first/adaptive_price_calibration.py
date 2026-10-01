from __future__ import annotations

import json
import math
import os
import tempfile
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any, Callable, Mapping, NoReturn, Sequence

import numpy as np
import psutil
from pydantic import ValidationError

from backend.services.advisory_model_first.adaptive_price_calibration_contracts import (
    ADAPTIVE_ARM_ID,
    BOOTSTRAP_SAMPLES,
    BOOTSTRAP_SEED,
    MIN_FIT_ROWS,
    NOMINAL_COVERAGE,
    AdaptivePriceArmMetricsV1,
    AdaptivePriceBootstrapV1,
    AdaptivePriceCalibrationReceiptV1,
    AdaptivePriceCalibrationResultV1,
    AdaptivePriceDailyStateV1,
    AdaptivePriceNavigationGateV1,
    FrozenAdaptivePriceCalibrationRequestV1,
    build_adaptive_price_calibration_request,
    build_adaptive_price_receipt,
    build_adaptive_price_result,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.historical_price_replay import (
    AdvisoryHistoricalPriceReplayArtifact,
    PostgresHistoricalPriceReplaySource,
    _settle_day,
    read_historical_price_replay_artifact,
)
from backend.services.advisory_model_first.historical_price_replay_contracts import (
    AdvisoryHistoricalPriceOutcomeRowV1,
    AdvisoryHistoricalPricePredictionRowV1,
)
from backend.services.advisory_model_first.prediction_source import sha256_file
from backend.services.advisory_model_first.price_range_contracts import canonical_json_sha256
from backend.services.advisory_model_first.research_control import (
    AdvisoryResearchTrialRegistryV1,
)
from backend.services.advisory_model_first.research_control_contracts import (
    AdvisoryResearchTrialRecordV1,
    ConsumedWindowV1,
    DecisionUse,
    EvidenceReferenceV1,
    ObjectiveContract,
    ResearchResultClass,
    ResearchStudyType,
    build_trial_record,
)


ADAPTIVE_RUN_ROOT = "adaptive_price_calibration_runs"
EXPERIMENT_PREFIX = "ADVISORY-PRICE-ADAPTIVE-CALIBRATION-V1"
RESEARCH_STAGE = "P1-A-PRICE-ADAPTIVE-CALIBRATION-EXPLORATORY"
FAMILY_ID = "ADVISORY-PRICE-ADAPTIVE-CALIBRATION-V1"
UNIQUE_VARIABLE = "STATIC_V4_CONTROL_VS_ROLLING_20D_MATURED_CQR"


@dataclass(frozen=True)
class AdaptivePriceCalibrationArtifact:
    path: Path
    request: FrozenAdaptivePriceCalibrationRequestV1
    result: AdaptivePriceCalibrationResultV1
    registry_records: tuple[AdvisoryResearchTrialRecordV1, ...]
    receipt: AdaptivePriceCalibrationReceiptV1


@dataclass(frozen=True)
class AdaptivePriceCalibrationDelivery:
    artifact: AdaptivePriceCalibrationArtifact
    registry_delivery: Mapping[str, Any]


def prepare_adaptive_price_calibration_request(
    *,
    source_replay_root: str | Path,
    output_root: str | Path,
    registry_path: str | Path,
    repository_commit: str,
) -> FrozenAdaptivePriceCalibrationRequestV1:
    source_root = Path(source_replay_root).resolve()
    model_root = Path(output_root).resolve()
    artifact = _read_source(source_root)
    request = artifact.request
    receipt = artifact.receipt
    files = _source_files(source_root)
    dataset_identity = _source_dataset_identity(artifact)
    return build_adaptive_price_calibration_request(
        output_root=str(model_root),
        registry_path=str(Path(registry_path).resolve()),
        source_replay_root=str(source_root),
        source_replay_id=request.replay_id,
        source_request_file_sha256=sha256_file(files["request"]),
        source_prediction_file_sha256=sha256_file(files["prediction"]),
        source_outcome_file_sha256=sha256_file(files["outcome"]),
        source_manifest_file_sha256=sha256_file(files["manifest"]),
        source_receipt_file_sha256=sha256_file(files["receipt"]),
        source_prediction_snapshot_sha256=receipt.prediction_snapshot_sha256,
        source_outcome_sha256=receipt.outcome_sha256,
        source_price_range_bundle_id=request.price_range_bundle_id,
        source_price_range_manifest_sha256=request.price_range_manifest_sha256,
        decision_start_trade_date=request.decision_start_trade_date,
        decision_end_trade_date=request.decision_end_trade_date,
        dataset_identity=dataset_identity,
        repository_commit=repository_commit,
    )


class AdvisoryAdaptivePriceCalibrationService:
    def __init__(
        self,
        *,
        data_source: Any | None = None,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        self._data_source = data_source or PostgresHistoricalPriceReplaySource()
        self._now_provider = now_provider or (lambda: datetime.now(timezone.utc))

    def run(
        self,
        *,
        request: FrozenAdaptivePriceCalibrationRequestV1,
    ) -> AdaptivePriceCalibrationDelivery:
        started = time.perf_counter()
        target = Path(request.output_root).resolve() / ADAPTIVE_RUN_ROOT / request.request_id
        if target.exists():
            artifact = read_adaptive_price_calibration_artifact(target)
            _verify_request_match(request, artifact.request)
            _verify_source_identity(request, _read_source(Path(request.source_replay_root)))
            registry = AdvisoryResearchTrialRegistryV1(request.registry_path).append_batch(
                artifact.registry_records
            )
            return AdaptivePriceCalibrationDelivery(
                artifact=AdaptivePriceCalibrationArtifact(
                    path=artifact.path,
                    request=artifact.request,
                    result=artifact.result,
                    registry_records=artifact.registry_records,
                    receipt=artifact.receipt.model_copy(update={"status": "ALREADY_MATERIALIZED"}),
                ),
                registry_delivery=registry,
            )

        source_started = time.perf_counter()
        source = _read_source(Path(request.source_replay_root))
        _verify_source_identity(request, source)
        predictions = _read_prediction_rows(source)
        source_rows = tuple(
            AdvisoryHistoricalPriceOutcomeRowV1.model_validate(item)
            for item in source.outcome["rows"]
        )
        source_seconds = time.perf_counter() - source_started

        evaluation_started = time.perf_counter()
        adaptive_rows, states = self._evaluate_chronologically(
            request=request,
            predictions=predictions,
            source_rows=source_rows,
            pit_universe_key=source.request.pit_universe_key,
        )
        active_dates = frozenset(
            item.decision_as_of_trade_date for item in states if item.mode == "ADAPTIVE_ACTIVE"
        )
        static_active_rows = tuple(
            item for item in source_rows if item.decision_as_of_trade_date in active_dates
        )
        adaptive_active_rows = tuple(
            item for item in adaptive_rows if item.decision_as_of_trade_date in active_dates
        )
        static_all = _arm_metrics(source_rows)
        adaptive_all = _arm_metrics(adaptive_rows)
        static_active = _arm_metrics(static_active_rows)
        adaptive_active = _arm_metrics(adaptive_active_rows)
        bootstrap = _cluster_bootstrap(
            static_rows=static_active_rows,
            adaptive_rows=adaptive_active_rows,
        )
        coverage_improvement = _coverage_error_improvement(static_active, adaptive_active)
        model_width_ratio = _ratio(
            adaptive_active.model_mean_width_bps,
            static_active.model_mean_width_bps,
            name="model width",
        )
        business_width_ratio = _ratio(
            adaptive_active.business_mean_width_bps,
            static_active.business_mean_width_bps,
            name="business width",
        )
        rescue, harm = _tick_effects(static_active_rows, adaptive_active_rows)
        gate = _navigation_gate(
            static=static_active,
            adaptive=adaptive_active,
            coverage_error_improvement=coverage_improvement,
            model_width_ratio=model_width_ratio,
            business_width_ratio=business_width_ratio,
            bootstrap=bootstrap,
        )
        evaluation_seconds = time.perf_counter() - evaluation_started
        peak_rss = int(psutil.Process(os.getpid()).memory_info().rss)
        if peak_rss > request.resource_max_rss_bytes:
            _raise(
                "adaptive price calibration exceeded the RSS limit",
                "ADVISORY_ADAPTIVE_PRICE_EVALUATION_FAILED",
                peak_rss_bytes=peak_rss,
                resource_max_rss_bytes=request.resource_max_rss_bytes,
            )
        result = build_adaptive_price_result(
            request_id=request.request_id,
            request_sha256=request.request_sha256,
            source_replay_id=request.source_replay_id,
            static_all=static_all,
            adaptive_all=adaptive_all,
            static_active=static_active,
            adaptive_active=adaptive_active,
            daily_states=states,
            coverage_error_improvement=coverage_improvement,
            model_width_ratio=model_width_ratio,
            business_width_ratio=business_width_ratio,
            tick_rounding_rescue_count=rescue,
            tick_rounding_harm_count=harm,
            bootstrap=bootstrap,
            gate=gate,
            stage_timings_seconds={
                "source_validation": source_seconds,
                "chronological_evaluation": evaluation_seconds,
                "total_before_publish": time.perf_counter() - started,
            },
            peak_rss_bytes=peak_rss,
        )
        artifact = _publish(
            target=target,
            request=request,
            result=result,
            published_at=self._now_provider(),
        )
        registry = AdvisoryResearchTrialRegistryV1(request.registry_path).append_batch(
            artifact.registry_records
        )
        return AdaptivePriceCalibrationDelivery(artifact=artifact, registry_delivery=registry)

    def _evaluate_chronologically(
        self,
        *,
        request: FrozenAdaptivePriceCalibrationRequestV1,
        predictions: Sequence[AdvisoryHistoricalPricePredictionRowV1],
        source_rows: Sequence[AdvisoryHistoricalPriceOutcomeRowV1],
        pit_universe_key: str,
    ) -> tuple[tuple[AdvisoryHistoricalPriceOutcomeRowV1, ...], tuple[AdaptivePriceDailyStateV1, ...]]:
        prediction_groups = _group_by_decision(predictions)
        source_by_key = {_row_key(item): item for item in source_rows}
        if len(source_by_key) != len(source_rows):
            _raise(
                "adaptive source outcome contains duplicate identities",
                "ADVISORY_ADAPTIVE_PRICE_SOURCE_INVALID",
            )
        residual_rows = tuple(
            item
            for item in source_rows
            if item.model_prediction_status == "AVAILABLE" and item.market_outcome_status == "AVAILABLE"
        )
        output: list[AdvisoryHistoricalPriceOutcomeRowV1] = []
        states: list[AdaptivePriceDailyStateV1] = []
        for decision_date, group in prediction_groups:
            fit_rows, fit_dates = _mature_fit_rows(
                residual_rows,
                decision_date=decision_date,
                lookback_target_dates=request.lookback_target_dates,
            )
            active = (
                len(fit_dates) >= request.min_fit_target_dates
                and len(fit_rows) >= request.min_fit_rows
            )
            delta = _fit_delta(fit_rows, nominal_coverage=request.nominal_coverage) if active else 0.0
            target_dates = {item.target_trade_date for item in group}
            if len(target_dates) != 1:
                _raise(
                    "adaptive decision group has multiple target dates",
                    "ADVISORY_ADAPTIVE_PRICE_SOURCE_INVALID",
                    decision_date=decision_date.isoformat(),
                )
            target_date = next(iter(target_dates))
            states.append(
                AdaptivePriceDailyStateV1(
                    decision_as_of_trade_date=decision_date,
                    target_trade_date=target_date,
                    mode="ADAPTIVE_ACTIVE" if active else "WARMUP_STATIC_FALLBACK",
                    delta=delta,
                    fit_target_date_count=len(fit_dates),
                    fit_row_count=len(fit_rows),
                    fit_target_start=fit_dates[0] if fit_dates else None,
                    fit_target_end=fit_dates[-1] if fit_dates else None,
                )
            )
            adjusted = tuple(_apply_delta(item, delta=delta) for item in group)
            snapshot = self._data_source.load_day(
                symbols=tuple(item.symbol for item in adjusted),
                decision_trade_date=decision_date,
                target_trade_date=target_date,
                pit_universe_key=pit_universe_key,
            )
            static_evaluated = tuple(_settle_day(group, snapshot=snapshot))
            _verify_source_drift(
                static_evaluated,
                source_by_key,
                compare_projection=True,
            )
            evaluated = tuple(_settle_day(adjusted, snapshot=snapshot))
            _verify_source_drift(
                evaluated,
                source_by_key,
                compare_projection=False,
            )
            output.extend(evaluated)
        if tuple(_row_key(item) for item in output) != tuple(_row_key(item) for item in source_rows):
            _raise(
                "adaptive evaluated identities differ from source replay",
                "ADVISORY_ADAPTIVE_PRICE_SOURCE_DRIFT",
            )
        return tuple(output), tuple(states)
def read_adaptive_price_calibration_artifact(
    root: str | Path,
) -> AdaptivePriceCalibrationArtifact:
    path = Path(root).resolve()
    try:
        request = FrozenAdaptivePriceCalibrationRequestV1.model_validate_json(
            (path / "request.json").read_text(encoding="utf-8")
        )
        result = AdaptivePriceCalibrationResultV1.model_validate_json(
            (path / "result.json").read_text(encoding="utf-8")
        )
        registry_payload = json.loads((path / "registry_records.json").read_text(encoding="utf-8"))
        registry_records = tuple(
            AdvisoryResearchTrialRecordV1.model_validate(item) for item in registry_payload
        )
        manifest = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
        receipt = AdaptivePriceCalibrationReceiptV1.model_validate_json(
            (path / "receipt.json").read_text(encoding="utf-8")
        )
    except (OSError, ValueError, TypeError, json.JSONDecodeError, ValidationError) as exc:
        _raise(
            "adaptive calibration artifact cannot be read",
            "ADVISORY_ADAPTIVE_PRICE_ARTIFACT_CONFLICT",
            error_type=type(exc).__name__,
        )
    expected_files = {
        name: _file_identity(path / name)
        for name in ("request.json", "result.json", "registry_records.json")
    }
    expected_record = _registry_record(
        request=request,
        result=result,
        temp_root=path,
        final_root=path,
    )
    if (
        manifest.get("schema_version") != "advisory_adaptive_price_calibration_manifest_v1"
        or manifest.get("request_id") != request.request_id
        or manifest.get("request_sha256") != request.request_sha256
        or manifest.get("result_sha256") != result.result_sha256
        or manifest.get("files") != expected_files
        or receipt.request_id != request.request_id
        or receipt.request_sha256 != request.request_sha256
        or receipt.result_sha256 != result.result_sha256
        or receipt.registry_records_sha256 != sha256_file(path / "registry_records.json")
        or receipt.manifest_sha256 != sha256_file(path / "manifest.json")
        or receipt.selected_trial_count != int(result.gate.all_pass)
        or registry_records != (expected_record,)
    ):
        _raise(
            "adaptive calibration artifact identities are inconsistent",
            "ADVISORY_ADAPTIVE_PRICE_ARTIFACT_CONFLICT",
        )
    return AdaptivePriceCalibrationArtifact(
        path=path,
        request=request,
        result=result,
        registry_records=registry_records,
        receipt=receipt,
    )


def _read_source(root: Path) -> AdvisoryHistoricalPriceReplayArtifact:
    try:
        return read_historical_price_replay_artifact(root)
    except AdvisoryModelFirstError as exc:
        _raise(
            "adaptive calibration source replay is invalid",
            "ADVISORY_ADAPTIVE_PRICE_SOURCE_INVALID",
            source_reason_code=exc.reason_code,
        )


def _verify_source_identity(
    request: FrozenAdaptivePriceCalibrationRequestV1,
    source: AdvisoryHistoricalPriceReplayArtifact,
) -> None:
    files = _source_files(source.path)
    actual = {
        "source_replay_id": source.request.replay_id,
        "source_request_file_sha256": sha256_file(files["request"]),
        "source_prediction_file_sha256": sha256_file(files["prediction"]),
        "source_outcome_file_sha256": sha256_file(files["outcome"]),
        "source_manifest_file_sha256": sha256_file(files["manifest"]),
        "source_receipt_file_sha256": sha256_file(files["receipt"]),
        "source_prediction_snapshot_sha256": source.receipt.prediction_snapshot_sha256,
        "source_outcome_sha256": source.receipt.outcome_sha256,
        "source_price_range_bundle_id": source.request.price_range_bundle_id,
        "source_price_range_manifest_sha256": source.request.price_range_manifest_sha256,
        "decision_start_trade_date": source.request.decision_start_trade_date,
        "decision_end_trade_date": source.request.decision_end_trade_date,
    }
    for name, value in actual.items():
        if getattr(request, name) != value:
            _raise(
                "adaptive calibration source identity changed",
                "ADVISORY_ADAPTIVE_PRICE_SOURCE_INVALID",
                field=name,
            )
    if request.dataset_identity != _source_dataset_identity(source):
        _raise(
            "adaptive calibration dataset identity differs from source replay",
            "ADVISORY_ADAPTIVE_PRICE_SOURCE_INVALID",
            field="dataset_identity",
        )


def _read_prediction_rows(
    source: AdvisoryHistoricalPriceReplayArtifact,
) -> tuple[AdvisoryHistoricalPricePredictionRowV1, ...]:
    try:
        payload = json.loads((source.path / "prediction" / "prediction.json").read_text(encoding="utf-8"))
        rows = tuple(
            AdvisoryHistoricalPricePredictionRowV1.model_validate(item)
            for item in payload.get("rows") or ()
        )
    except (OSError, ValueError, TypeError, json.JSONDecodeError, ValidationError) as exc:
        _raise(
            "adaptive source predictions cannot be read",
            "ADVISORY_ADAPTIVE_PRICE_SOURCE_INVALID",
            error_type=type(exc).__name__,
        )
    if not rows or canonical_json_sha256([item.model_dump(mode="json") for item in rows]) != (
        source.receipt.prediction_snapshot_sha256
    ):
        _raise(
            "adaptive source prediction identity is inconsistent",
            "ADVISORY_ADAPTIVE_PRICE_SOURCE_INVALID",
        )
    return rows


def _group_by_decision(
    rows: Sequence[AdvisoryHistoricalPricePredictionRowV1],
) -> tuple[tuple[date, tuple[AdvisoryHistoricalPricePredictionRowV1, ...]], ...]:
    grouped: dict[date, list[AdvisoryHistoricalPricePredictionRowV1]] = {}
    for item in rows:
        grouped.setdefault(item.decision_as_of_trade_date, []).append(item)
    return tuple(
        (key, tuple(sorted(value, key=lambda item: item.symbol)))
        for key, value in sorted(grouped.items())
    )


def _mature_fit_rows(
    rows: Sequence[AdvisoryHistoricalPriceOutcomeRowV1],
    *,
    decision_date: date,
    lookback_target_dates: int,
) -> tuple[tuple[AdvisoryHistoricalPriceOutcomeRowV1, ...], tuple[date, ...]]:
    eligible = tuple(item for item in rows if item.target_trade_date <= decision_date)
    dates = tuple(sorted({item.target_trade_date for item in eligible}))[-lookback_target_dates:]
    selected_dates = frozenset(dates)
    selected = tuple(item for item in eligible if item.target_trade_date in selected_dates)
    if any(item.target_trade_date > decision_date for item in selected):
        _raise(
            "adaptive calibration fit consumed an immature outcome",
            "ADVISORY_ADAPTIVE_PRICE_PIT_VIOLATION",
            decision_date=decision_date.isoformat(),
        )
    return selected, dates


def _fit_delta(
    rows: Sequence[AdvisoryHistoricalPriceOutcomeRowV1],
    *,
    nominal_coverage: float,
) -> float:
    scores: list[float] = []
    for item in rows:
        assert item.calibrated_gap_q10 is not None
        assert item.calibrated_gap_q90 is not None
        assert item.actual_entry_gap_return is not None
        scores.append(
            max(
                item.calibrated_gap_q10 - item.actual_entry_gap_return,
                item.actual_entry_gap_return - item.calibrated_gap_q90,
                0.0,
            )
        )
    values = np.asarray(scores, dtype=float)
    if len(values) < MIN_FIT_ROWS or not np.isfinite(values).all():
        _raise(
            "adaptive calibration residual window is invalid",
            "ADVISORY_ADAPTIVE_PRICE_EVALUATION_FAILED",
            row_count=len(values),
        )
    rank = min(math.ceil((len(values) + 1) * nominal_coverage), len(values))
    return float(np.sort(values)[rank - 1])


def _apply_delta(
    item: AdvisoryHistoricalPricePredictionRowV1,
    *,
    delta: float,
) -> AdvisoryHistoricalPricePredictionRowV1:
    if not math.isfinite(delta) or delta < 0.0 or item.calibrated_gap_q10 - delta <= -1.0:
        _raise(
            "adaptive calibration delta produces an invalid interval",
            "ADVISORY_ADAPTIVE_PRICE_EVALUATION_FAILED",
            symbol=item.symbol,
        )
    return AdvisoryHistoricalPricePredictionRowV1.model_validate(
        {
            **item.model_dump(mode="python"),
            "calibrated_gap_q10": item.calibrated_gap_q10 - delta,
            "calibrated_gap_q90": item.calibrated_gap_q90 + delta,
        }
    )


def _verify_source_drift(
    evaluated: Sequence[AdvisoryHistoricalPriceOutcomeRowV1],
    source_by_key: Mapping[tuple[date, date, str], AdvisoryHistoricalPriceOutcomeRowV1],
    *,
    compare_projection: bool,
) -> None:
    for actual in evaluated:
        expected = source_by_key.get(_row_key(actual))
        if expected is None or actual.market_outcome_status != expected.market_outcome_status:
            _raise(
                "adaptive market status differs from immutable replay",
                "ADVISORY_ADAPTIVE_PRICE_SOURCE_DRIFT",
                key=str(_row_key(actual)),
            )
        if compare_projection:
            for field in AdvisoryHistoricalPriceOutcomeRowV1.model_fields:
                expected_value = getattr(expected, field)
                actual_value = getattr(actual, field)
                if isinstance(expected_value, float) and isinstance(actual_value, float):
                    matches = math.isclose(
                        actual_value,
                        expected_value,
                        rel_tol=1e-12,
                        abs_tol=1e-9,
                    )
                else:
                    matches = actual_value == expected_value
                if not matches:
                    _raise(
                        "adaptive static projection differs from immutable replay",
                        "ADVISORY_ADAPTIVE_PRICE_SOURCE_DRIFT",
                        key=str(_row_key(actual)),
                        field=field,
                    )
            continue
        if expected.actual_open is None:
            if actual.actual_open is not None:
                _raise(
                    "adaptive actual open differs from immutable replay",
                    "ADVISORY_ADAPTIVE_PRICE_SOURCE_DRIFT",
                    key=str(_row_key(actual)),
                )
        elif actual.actual_open is None or not math.isclose(
            actual.actual_open,
            expected.actual_open,
            rel_tol=1e-12,
            abs_tol=1e-9,
        ):
            _raise(
                "adaptive actual open differs from immutable replay",
                "ADVISORY_ADAPTIVE_PRICE_SOURCE_DRIFT",
                key=str(_row_key(actual)),
            )
        if actual.market_outcome_reason != expected.market_outcome_reason:
            _raise(
                "adaptive market reason differs from immutable replay",
                "ADVISORY_ADAPTIVE_PRICE_SOURCE_DRIFT",
                key=str(_row_key(actual)),
            )
        if actual.model_prediction_status != expected.model_prediction_status:
            _raise(
                "adaptive PIT context differs from immutable replay",
                "ADVISORY_ADAPTIVE_PRICE_SOURCE_DRIFT",
                key=str(_row_key(actual)),
            )
        if actual.model_prediction_reason != expected.model_prediction_reason:
            _raise(
                "adaptive model reason differs from immutable replay",
                "ADVISORY_ADAPTIVE_PRICE_SOURCE_DRIFT",
                key=str(_row_key(actual)),
            )
        for field in ("decision_reference_price", "actual_entry_gap_return"):
            expected_value = getattr(expected, field)
            actual_value = getattr(actual, field)
            if expected_value is None:
                if actual_value is not None:
                    _raise(
                        "adaptive price context differs from immutable replay",
                        "ADVISORY_ADAPTIVE_PRICE_SOURCE_DRIFT",
                        key=str(_row_key(actual)),
                        field=field,
                    )
            elif actual_value is None or not math.isclose(
                actual_value, expected_value, rel_tol=1e-12, abs_tol=1e-9
            ):
                _raise(
                    "adaptive price context differs from immutable replay",
                    "ADVISORY_ADAPTIVE_PRICE_SOURCE_DRIFT",
                    key=str(_row_key(actual)),
                    field=field,
                )


def _arm_metrics(rows: Sequence[AdvisoryHistoricalPriceOutcomeRowV1]) -> AdaptivePriceArmMetricsV1:
    supported = tuple(
        item
        for item in rows
        if item.model_prediction_status == "AVAILABLE" and item.market_outcome_status == "AVAILABLE"
    )
    if not supported:
        return AdaptivePriceArmMetricsV1(
            decision_date_count=len({item.decision_as_of_trade_date for item in rows}),
            candidate_count=len(rows),
            market_available_count=sum(item.market_outcome_status == "AVAILABLE" for item in rows),
            not_applicable_count=sum(item.market_outcome_status == "NOT_APPLICABLE" for item in rows),
            market_unavailable_count=sum(item.market_outcome_status == "UNAVAILABLE" for item in rows),
            supported_row_count=0,
            model_unavailable_count=sum(
                item.market_outcome_status == "AVAILABLE" and item.model_prediction_status == "UNAVAILABLE"
                for item in rows
            ),
        )
    actual = np.asarray([item.actual_entry_gap_return for item in supported], dtype=float)
    lower = np.asarray([item.calibrated_gap_q10 for item in supported], dtype=float)
    middle = np.asarray([item.calibrated_gap_q50 for item in supported], dtype=float)
    upper = np.asarray([item.calibrated_gap_q90 for item in supported], dtype=float)
    business_width = np.asarray([item.interval_width_bps for item in supported], dtype=float)
    business_error = np.asarray([item.absolute_mid_error_bps for item in supported], dtype=float)
    if not all(np.isfinite(value).all() for value in (actual, lower, middle, upper, business_width, business_error)):
        _raise(
            "adaptive arm metrics contain non-finite values",
            "ADVISORY_ADAPTIVE_PRICE_EVALUATION_FAILED",
        )
    model_inside = (actual >= lower) & (actual <= upper)
    business_inside = np.asarray([bool(item.covered) for item in supported], dtype=bool)
    return AdaptivePriceArmMetricsV1(
        decision_date_count=len({item.decision_as_of_trade_date for item in rows}),
        candidate_count=len(rows),
        market_available_count=sum(item.market_outcome_status == "AVAILABLE" for item in rows),
        not_applicable_count=sum(item.market_outcome_status == "NOT_APPLICABLE" for item in rows),
        market_unavailable_count=sum(item.market_outcome_status == "UNAVAILABLE" for item in rows),
        supported_row_count=len(supported),
        model_unavailable_count=sum(
            item.market_outcome_status == "AVAILABLE" and item.model_prediction_status == "UNAVAILABLE"
            for item in rows
        ),
        model_coverage=float(model_inside.mean()),
        model_lower_miss_rate=float((actual < lower).mean()),
        model_upper_miss_rate=float((actual > upper).mean()),
        model_mean_width_bps=float(((upper - lower) * 10_000.0).mean()),
        model_median_width_bps=float(np.median((upper - lower) * 10_000.0)),
        model_mean_absolute_mid_error_bps=float((np.abs(actual - middle) * 10_000.0).mean()),
        model_median_absolute_mid_error_bps=float(np.median(np.abs(actual - middle) * 10_000.0)),
        business_coverage=float(business_inside.mean()),
        business_lower_miss_rate=float(np.mean([bool(item.lower_miss) for item in supported])),
        business_upper_miss_rate=float(np.mean([bool(item.upper_miss) for item in supported])),
        business_mean_width_bps=float(business_width.mean()),
        business_median_width_bps=float(np.median(business_width)),
        business_mean_absolute_mid_error_bps=float(business_error.mean()),
        business_median_absolute_mid_error_bps=float(np.median(business_error)),
    )


def _cluster_bootstrap(
    *,
    static_rows: Sequence[AdvisoryHistoricalPriceOutcomeRowV1],
    adaptive_rows: Sequence[AdvisoryHistoricalPriceOutcomeRowV1],
) -> AdaptivePriceBootstrapV1:
    static_by_key = {_row_key(item): item for item in static_rows}
    adaptive_by_key = {_row_key(item): item for item in adaptive_rows}
    if static_by_key.keys() != adaptive_by_key.keys():
        _raise(
            "adaptive bootstrap arms use different support",
            "ADVISORY_ADAPTIVE_PRICE_EVALUATION_FAILED",
        )
    by_date: dict[date, list[tuple[bool, bool]]] = {}
    for key in sorted(static_by_key):
        static = static_by_key[key]
        adaptive = adaptive_by_key[key]
        if static.model_prediction_status != "AVAILABLE" or static.market_outcome_status != "AVAILABLE":
            continue
        by_date.setdefault(static.decision_as_of_trade_date, []).append(
            (bool(static.model_space_covered), bool(adaptive.model_space_covered))
        )
    improvements = np.asarray(
        [
            abs(mean(float(left) for left, _ in values) - NOMINAL_COVERAGE)
            - abs(mean(float(right) for _, right in values) - NOMINAL_COVERAGE)
            for _, values in sorted(by_date.items())
        ],
        dtype=float,
    )
    if not len(improvements) or not np.isfinite(improvements).all():
        _raise(
            "adaptive bootstrap has no valid decision clusters",
            "ADVISORY_ADAPTIVE_PRICE_EVALUATION_FAILED",
        )
    rng = np.random.default_rng(BOOTSTRAP_SEED)
    sampled = rng.choice(improvements, size=(BOOTSTRAP_SAMPLES, len(improvements)), replace=True).mean(axis=1)
    return AdaptivePriceBootstrapV1(
        cluster_count=len(improvements),
        point=float(improvements.mean()),
        lower_95=float(np.quantile(sampled, 0.025)),
        upper_95=float(np.quantile(sampled, 0.975)),
    )


def _coverage_error_improvement(
    static: AdaptivePriceArmMetricsV1,
    adaptive: AdaptivePriceArmMetricsV1,
) -> float:
    assert static.model_coverage is not None and adaptive.model_coverage is not None
    return float(
        abs(static.model_coverage - NOMINAL_COVERAGE)
        - abs(adaptive.model_coverage - NOMINAL_COVERAGE)
    )


def _ratio(numerator: float | None, denominator: float | None, *, name: str) -> float:
    if numerator is None or denominator is None or denominator <= 0.0:
        _raise(
            f"adaptive {name} ratio is undefined",
            "ADVISORY_ADAPTIVE_PRICE_EVALUATION_FAILED",
        )
    return float(numerator / denominator)


def _navigation_gate(
    *,
    static: AdaptivePriceArmMetricsV1,
    adaptive: AdaptivePriceArmMetricsV1,
    coverage_error_improvement: float,
    model_width_ratio: float,
    business_width_ratio: float,
    bootstrap: AdaptivePriceBootstrapV1,
) -> AdaptivePriceNavigationGateV1:
    assert static.model_lower_miss_rate is not None
    assert static.model_upper_miss_rate is not None
    assert adaptive.model_lower_miss_rate is not None
    assert adaptive.model_upper_miss_rate is not None
    assert static.business_coverage is not None
    assert adaptive.business_coverage is not None
    checks = {
        "support_pass": bootstrap.cluster_count >= 60 and adaptive.supported_row_count >= 1000,
        "model_coverage_improvement_pass": coverage_error_improvement >= 0.02,
        "bootstrap_lower_bound_pass": bootstrap.lower_95 > 0.0,
        "model_width_pass": model_width_ratio <= 1.25,
        "miss_rates_pass": (
            adaptive.model_lower_miss_rate <= static.model_lower_miss_rate + 1e-12
            and adaptive.model_upper_miss_rate <= static.model_upper_miss_rate + 1e-12
        ),
        "business_metrics_pass": (
            adaptive.business_coverage + 1e-12 >= static.business_coverage
            and business_width_ratio <= 1.25
        ),
        "identity_pass": True,
    }
    passed = all(checks.values())
    return AdaptivePriceNavigationGateV1(
        **checks,
        all_pass=passed,
        selected_candidate=ADAPTIVE_ARM_ID if passed else None,
    )


def _tick_effects(
    static_rows: Sequence[AdvisoryHistoricalPriceOutcomeRowV1],
    adaptive_rows: Sequence[AdvisoryHistoricalPriceOutcomeRowV1],
) -> tuple[int, int]:
    static_by_key = {_row_key(item): item for item in static_rows}
    adaptive_by_key = {_row_key(item): item for item in adaptive_rows}
    if static_by_key.keys() != adaptive_by_key.keys():
        _raise(
            "adaptive tick comparison arms use different support",
            "ADVISORY_ADAPTIVE_PRICE_EVALUATION_FAILED",
        )
    rescue = harm = 0
    for key, static in static_by_key.items():
        adaptive = adaptive_by_key[key]
        if static.covered is None or adaptive.covered is None:
            continue
        rescue += int(not static.covered and adaptive.covered)
        harm += int(static.covered and not adaptive.covered)
    return rescue, harm


def _publish(
    *,
    target: Path,
    request: FrozenAdaptivePriceCalibrationRequestV1,
    result: AdaptivePriceCalibrationResultV1,
    published_at: datetime,
) -> AdaptivePriceCalibrationArtifact:
    if published_at.tzinfo is None or published_at.utcoffset() is None:
        _raise(
            "adaptive calibration publish time must be timezone-aware",
            "ADVISORY_ADAPTIVE_PRICE_EVALUATION_FAILED",
        )
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=".adaptive-price-", dir=target.parent))
    try:
        _write_json(temporary / "request.json", request.model_dump(mode="json"))
        _write_json(temporary / "result.json", result.model_dump(mode="json"))
        record = _registry_record(
            request=request,
            result=result,
            temp_root=temporary,
            final_root=target,
        )
        _write_json(temporary / "registry_records.json", [record.model_dump(mode="json")])
        manifest = {
            "schema_version": "advisory_adaptive_price_calibration_manifest_v1",
            "request_id": request.request_id,
            "request_sha256": request.request_sha256,
            "result_sha256": result.result_sha256,
            "files": {
                name: _file_identity(temporary / name)
                for name in ("request.json", "result.json", "registry_records.json")
            },
        }
        _write_json(temporary / "manifest.json", manifest)
        receipt = build_adaptive_price_receipt(
            status="PUBLISHED",
            request_id=request.request_id,
            request_sha256=request.request_sha256,
            result_sha256=result.result_sha256,
            registry_records_sha256=sha256_file(temporary / "registry_records.json"),
            manifest_sha256=sha256_file(temporary / "manifest.json"),
            published_at=published_at.astimezone(timezone.utc),
            selected_trial_count=int(result.gate.all_pass),
        )
        _write_json(temporary / "receipt.json", receipt.model_dump(mode="json"))
        try:
            os.replace(temporary, target)
        except OSError:
            if not target.exists():
                raise
            existing = read_adaptive_price_calibration_artifact(target)
            _verify_request_match(request, existing.request)
            return AdaptivePriceCalibrationArtifact(
                path=existing.path,
                request=existing.request,
                result=existing.result,
                registry_records=existing.registry_records,
                receipt=existing.receipt.model_copy(update={"status": "ALREADY_MATERIALIZED"}),
            )
        return read_adaptive_price_calibration_artifact(target)
    finally:
        if temporary.exists():
            for child in temporary.iterdir():
                if child.is_file():
                    child.unlink(missing_ok=True)
            temporary.rmdir()


def _registry_record(
    *,
    request: FrozenAdaptivePriceCalibrationRequestV1,
    result: AdaptivePriceCalibrationResultV1,
    temp_root: Path,
    final_root: Path,
) -> AdvisoryResearchTrialRecordV1:
    refs = tuple(
        EvidenceReferenceV1(
            role=role,
            artifact_uri=(final_root / name).as_posix(),
            sha256=sha256_file(temp_root / name),
            size_bytes=(temp_root / name).stat().st_size,
        )
        for role, name in (
            ("adaptive_price_request", "request.json"),
            ("adaptive_price_result", "result.json"),
        )
    )
    return build_trial_record(
        experiment_id=f"{EXPERIMENT_PREFIX}-{request.source_replay_id}",
        attempt_id=request.request_id,
        research_stage=RESEARCH_STAGE,
        study_type=ResearchStudyType.EXPLORATORY_SCREEN,
        hypothesis_family_id=FAMILY_ID,
        parent_lineage=(request.source_replay_id, request.source_price_range_bundle_id),
        unique_variable=UNIQUE_VARIABLE,
        objective_contract=ObjectiveContract.RISK_MANAGED_ADVISORY,
        dataset_identity=request.dataset_identity,
        schema_identity=request.schema_identity,
        policy_identity=request.policy_identity,
        planned_trial_count=request.planned_trial_count,
        generated_trial_count=2,
        evaluated_trial_count=2,
        selected_trial_count=int(result.gate.all_pass),
        consumed_windows=(
            ConsumedWindowV1(
                window_id="ADVISORY_PRICE_V4_TEST_HISTORICAL_REPLAY",
                dataset_identity=request.dataset_identity,
                start_date=request.decision_start_trade_date,
                end_date=request.decision_end_trade_date,
            ),
        ),
        result_class=ResearchResultClass.EXPLORATORY,
        decision_use=DecisionUse.NAVIGATION_ONLY,
        evidence_refs=refs,
        recorded_at=request.created_at,
    )


def _verify_request_match(
    expected: FrozenAdaptivePriceCalibrationRequestV1,
    actual: FrozenAdaptivePriceCalibrationRequestV1,
) -> None:
    if (
        expected.request_id != actual.request_id
        or expected.request_sha256 != actual.request_sha256
        or expected.functional_payload() != actual.functional_payload()
    ):
        _raise(
            "adaptive calibration exact retry request differs from published artifact",
            "ADVISORY_ADAPTIVE_PRICE_ARTIFACT_CONFLICT",
        )


def _source_files(root: Path) -> dict[str, Path]:
    return {
        "request": root / "prediction" / "request.json",
        "prediction": root / "prediction" / "prediction.json",
        "outcome": root / "settlement" / "outcome.json",
        "manifest": root / "settlement" / "manifest.json",
        "receipt": root / "settlement" / "receipt.json",
    }


def _source_dataset_identity(source: AdvisoryHistoricalPriceReplayArtifact) -> str:
    return canonical_json_sha256(
        {
            "source_replay_id": source.request.replay_id,
            "source_request_sha256": source.request.request_sha256,
            "prediction_snapshot_sha256": source.receipt.prediction_snapshot_sha256,
            "outcome_sha256": source.receipt.outcome_sha256,
            "pit_universe_key": source.request.pit_universe_key,
            "decision_start_trade_date": source.request.decision_start_trade_date.isoformat(),
            "decision_end_trade_date": source.request.decision_end_trade_date.isoformat(),
        }
    )


def _row_key(item: Any) -> tuple[date, date, str]:
    return item.decision_as_of_trade_date, item.target_trade_date, item.symbol


def _file_identity(path: Path) -> dict[str, Any]:
    return {"sha256": sha256_file(path), "size_bytes": path.stat().st_size}


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )


def _raise(message: str, reason_code: str, **context: Any) -> NoReturn:
    raise AdvisoryModelFirstError(message, reason_code=reason_code, context=context)


__all__ = [
    "ADAPTIVE_RUN_ROOT",
    "AdaptivePriceCalibrationArtifact",
    "AdaptivePriceCalibrationDelivery",
    "AdvisoryAdaptivePriceCalibrationService",
    "prepare_adaptive_price_calibration_request",
    "read_adaptive_price_calibration_artifact",
]
