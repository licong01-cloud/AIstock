from __future__ import annotations

import logging
import os
import time
from datetime import date
from typing import Any, Mapping

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v1 import (
    CATEGORICAL_FEATURE_COLUMNS,
    MODEL_FEATURE_COLUMNS,
)
from backend.services.advisory_model_first.model_bundle import (
    LoadedAdvisoryModelBundle,
    load_exact_shadow_bundle,
)
from backend.services.advisory_model_first.outcome_inference import (
    score_outcome_bundle,
    unavailable_outcome_envelope,
)
from backend.services.advisory_model_first.outcome_runtime_bundle import (
    load_exact_outcome_bundle,
)
from backend.services.advisory_model_first.price_range_inference import (
    score_price_range_bundle,
    unavailable_price_range_envelope,
)
from backend.services.advisory_model_first.price_range_runtime_bundle import (
    load_exact_price_range_bundle,
)
from backend.services.advisory_model_first.quality_contracts import (
    ENSEMBLE_SCORE_POLICY,
    QUALITY_SEEDS,
    SELECTION_PRIOR_POLICY,
)
from backend.services.advisory_model_first.realtime_feature_source import (
    PostgresAdvisoryReviewSource,
    PostgresRealtimeFeatureSource,
)
from backend.services.advisory_model_first.reranker_training import _coerce_numeric_feature_dtypes
from backend.services.advisory_model_first.shared_feature_builder import build_advisory_feature_matrix
from backend.services.advisory_model_first.target_binding import (
    BINDING_VERSION_ID,
    FUND_LEG_ID,
    LSTM_LEG_ID,
    MANIFEST_SHA256,
    PACKAGE_ID,
    PROGRAM_ID,
    RUNTIME_SEMANTICS_HASH,
    RUNTIME_SEMANTICS_PAYLOAD,
    STYLE_PROFILE_ID,
    STYLE_PROFILE_HASH,
    TERMINAL_WEIGHTS,
)
from backend.services.advisory_program import AdvisoryProgramService
from backend.services.selection_center.models import SelectionRunStatus
from backend.services.selection_center.service import SelectionCenterService
from backend.services.trading_core.errors import DataUnavailableError


LOGGER = logging.getLogger(__name__)


class AdvisoryModelShadowService:
    def __init__(
        self,
        *,
        program_service: AdvisoryProgramService | None = None,
        selection_service: SelectionCenterService | None = None,
        review_source: Any | None = None,
        feature_source: PostgresRealtimeFeatureSource | None = None,
        model_root_provider: Any | None = None,
        bundle_loader: Any = load_exact_shadow_bundle,
        outcome_bundle_loader: Any = load_exact_outcome_bundle,
        outcome_scorer: Any = score_outcome_bundle,
        price_range_bundle_loader: Any = load_exact_price_range_bundle,
        price_range_scorer: Any = score_price_range_bundle,
    ) -> None:
        self._program_service = program_service or AdvisoryProgramService()
        self._selection_service = selection_service or SelectionCenterService()
        self._review_source = review_source or PostgresAdvisoryReviewSource()
        self._feature_source = feature_source or PostgresRealtimeFeatureSource()
        self._model_root_provider = model_root_provider or (
            lambda: os.getenv("AISTOCK_ADVISORY_MODEL_ROOT", "").strip()
        )
        self._bundle_loader = bundle_loader
        self._outcome_bundle_loader = outcome_bundle_loader
        self._outcome_scorer = outcome_scorer
        self._price_range_bundle_loader = price_range_bundle_loader
        self._price_range_scorer = price_range_scorer

    def model_shadow(self, *, program_id: str, target_trade_date: date) -> dict[str, Any]:
        started = time.monotonic()
        try:
            result = self._model_shadow(program_id=program_id, target_trade_date=target_trade_date)
        except AdvisoryModelFirstError as exc:
            LOGGER.warning(
                "advisory model shadow unavailable program_id=%s target_trade_date=%s reason_code=%s context=%s elapsed_ms=%d",
                program_id,
                target_trade_date.isoformat(),
                exc.reason_code,
                exc.context,
                round((time.monotonic() - started) * 1000),
            )
            return _unavailable_response(
                program_id=program_id,
                target_trade_date=target_trade_date,
                reason_code=exc.reason_code,
                message=str(exc),
            )
        except DataUnavailableError as exc:
            LOGGER.warning(
                "advisory model persisted input unavailable program_id=%s target_trade_date=%s "
                "source_error_code=%s context=%s elapsed_ms=%d",
                program_id,
                target_trade_date.isoformat(),
                exc.error_code,
                exc.context,
                round((time.monotonic() - started) * 1000),
            )
            return _unavailable_response(
                program_id=program_id,
                target_trade_date=target_trade_date,
                reason_code="ADVISORY_MODEL_SELECTION_INPUT_UNAVAILABLE",
                message="persisted Advisory or Selection input is unavailable",
            )
        except Exception:
            LOGGER.exception(
                "advisory model shadow failed unexpectedly program_id=%s target_trade_date=%s elapsed_ms=%d",
                program_id,
                target_trade_date.isoformat(),
                round((time.monotonic() - started) * 1000),
            )
            raise
        LOGGER.info(
            "advisory model shadow completed program_id=%s target_trade_date=%s candidate_count=%d elapsed_ms=%d",
            program_id,
            target_trade_date.isoformat(),
            result["candidate_count"],
            round((time.monotonic() - started) * 1000),
        )
        return result

    def _model_shadow(self, *, program_id: str, target_trade_date: date) -> dict[str, Any]:
        program = self._program_service.get_program(program_id)
        binding = self._program_service.active_binding(program_id)
        package_ids = tuple(program.package_ids)
        if (
            program_id != PROGRAM_ID
            or package_ids != (PACKAGE_ID,)
            or binding.get("binding_version_id") != BINDING_VERSION_ID
            or tuple(binding.get("package_ids") or ()) != (PACKAGE_ID,)
        ):
            raise AdvisoryModelFirstError(
                "no model bundle is bound to this Advisory Program identity",
                reason_code="ADVISORY_MODEL_BUNDLE_NOT_AVAILABLE_FOR_PACKAGE",
            )
        model_root = str(self._model_root_provider() or "").strip()
        if not model_root:
            raise AdvisoryModelFirstError(
                "AISTOCK_ADVISORY_MODEL_ROOT is not configured",
                reason_code="ADVISORY_MODEL_ROOT_NOT_CONFIGURED",
            )
        bundle = self._bundle_loader(
            model_root=model_root,
            package_id=PACKAGE_ID,
            manifest_sha256=MANIFEST_SHA256,
            style_profile_hash=STYLE_PROFILE_HASH,
        )
        _validate_bundle_runtime(bundle)

        list_version, list_items = self._selection_list_context(
            program_id=program_id,
            target_trade_date=target_trade_date,
        )
        if list_version.get("binding_version_id") != binding["binding_version_id"]:
            raise AdvisoryModelFirstError(
                "recommendation list binding differs from the active model binding",
                reason_code="ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH",
            )
        review_run_id = str(list_version.get("review_run_id") or "").strip()
        if not review_run_id:
            raise AdvisoryModelFirstError(
                "recommendation list does not identify its persisted Advisory review run",
                reason_code="ADVISORY_MODEL_SELECTION_INPUT_UNAVAILABLE",
            )
        review_run = self._review_source.get(review_run_id)
        review_selection_run_ids = tuple(
            str(value).strip() for value in review_run.selection_run_ids if str(value).strip()
        )
        if (
            review_run.program_id != program_id
            or review_run.binding_version_id != binding["binding_version_id"]
            or review_run.trade_date != target_trade_date
        ):
            raise AdvisoryModelFirstError(
                "persisted Advisory review run identity differs from the recommendation list",
                reason_code="ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH",
            )
        selection_run_id = str(review_run.selection_run_id or "").strip()
        if not selection_run_id or review_selection_run_ids != (selection_run_id,):
            raise AdvisoryModelFirstError(
                "persisted Advisory review does not identify exactly one Selection run",
                reason_code="ADVISORY_MODEL_SELECTION_INPUT_UNAVAILABLE",
                context={"selection_run_count": len(review_selection_run_ids)},
            )
        selection_run = self._selection_service.get_run(selection_run_id)
        if (
            selection_run.status != SelectionRunStatus.SUCCEEDED
            or selection_run.trade_date != target_trade_date
            or tuple(selection_run.package_ids) != (PACKAGE_ID,)
            or selection_run.manifest_sha256_by_package.get(PACKAGE_ID) != MANIFEST_SHA256
        ):
            raise AdvisoryModelFirstError(
                "persisted Selection run identity differs from the model target",
                reason_code="ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH",
            )
        decision_date = _resolve_decision_date(list_version=list_version, selection_run=selection_run)
        if decision_date >= target_trade_date:
            raise AdvisoryModelFirstError(
                "persisted Selection decision clock is invalid",
                reason_code="ADVISORY_MODEL_DECISION_CLOCK_MISMATCH",
                context={
                    "decision_as_of_trade_date": decision_date.isoformat(),
                    "target_trade_date": target_trade_date.isoformat(),
                },
            )
        candidates = _candidate_frame(
            selection_run.aggregate_results,
            program_id=program_id,
            binding_version_id=binding["binding_version_id"],
            decision_date=decision_date,
            target_trade_date=target_trade_date,
            target_count=int(program.target_count),
            bundle=bundle,
        )
        realtime = self._feature_source.load(
            symbols=candidates["instrument"].tolist(),
            decision_as_of_trade_date=decision_date,
            target_trade_date=target_trade_date,
            continuation_cutoff=date.fromisoformat(str(bundle.manifest["continuation_cutoff"])),
            hmm_models=bundle.hmm_models,
        )
        built = build_advisory_feature_matrix(
            candidates=candidates,
            candidate_daily=realtime.candidate_daily,
            candidate_static=realtime.candidate_static,
            market_daily=realtime.market_daily,
            benchmark_daily=realtime.benchmark_daily,
            suspend_rows=realtime.suspend_rows,
            hmm_states=realtime.hmm_states,
        )
        if len(built.coverage) != 1 or built.coverage.iloc[0]["status"] != "available":
            missing = built.coverage.iloc[0].get("required_missing_columns", []) if len(built.coverage) else []
            raise AdvisoryModelFirstError(
                "realtime feature matrix has required-value gaps",
                reason_code="ADVISORY_MODEL_FEATURE_REQUIRED_VALUE_MISSING",
                context={"required_missing_columns": list(missing)},
            )
        if len(built.features) != len(candidates):
            raise AdvisoryModelFirstError(
                "realtime feature matrix does not preserve the candidate group",
                reason_code="ADVISORY_MODEL_CANDIDATE_GROUP_INCOMPLETE",
                context={"candidate_count": len(candidates), "feature_count": len(built.features)},
            )
        scored = _score(bundle, built.features)
        outcome = self._outcome_shadow(
            model_root=model_root,
            parent_bundle=bundle,
            features=built.features,
            scored_candidates=scored,
            program_id=program_id,
            target_trade_date=target_trade_date,
        )
        price_range = self._price_range_shadow(
            model_root=model_root,
            parent_bundle=bundle,
            features=built.features,
            scored_candidates=scored,
            outcome=outcome,
            realtime=realtime,
            list_items=list_items,
            review_policy=program.review_policy,
            review_policy_sha256=program.review_policy_sha256,
            program_id=program_id,
            target_trade_date=target_trade_date,
        )
        shortlist_count = min(5, len(scored))
        return {
            "status": "EXPERIMENTAL_SHADOW",
            "calibration_state": bundle.manifest["calibration_state"],
            "program_id": program_id,
            "binding_version_id": binding["binding_version_id"],
            "package_id": PACKAGE_ID,
            "manifest_sha256": MANIFEST_SHA256,
            "decision_as_of_trade_date": decision_date.isoformat(),
            "target_trade_date": target_trade_date.isoformat(),
            "selection_runtime_semantics_hash": bundle.manifest["selection_runtime_semantics_hash"],
            "model_version": bundle.manifest["request_id"],
            "bundle_id": bundle.bundle_id,
            "feature_schema_version": bundle.manifest["feature_schema_version"],
            "candidate_count": len(scored),
            "shortlist_count": shortlist_count,
            "candidates": scored,
            "baselines": bundle.baselines,
            "hmm_unavailable": list(realtime.hmm_unavailable),
            "outcome": outcome,
            "price_range": price_range,
            "reason_code": None,
            "message": None,
        }

    def _price_range_shadow(
        self,
        *,
        model_root: str,
        parent_bundle: LoadedAdvisoryModelBundle,
        features: pd.DataFrame,
        scored_candidates: list[dict[str, Any]],
        outcome: Mapping[str, Any],
        realtime: Any,
        list_items: list[dict[str, Any]],
        review_policy: Mapping[str, Any],
        review_policy_sha256: str,
        program_id: str,
        target_trade_date: date,
    ) -> dict[str, Any]:
        started = time.monotonic()
        try:
            if outcome.get("status") != "EXPERIMENTAL_SHADOW":
                raise AdvisoryModelFirstError(
                    "M3 outcome prediction is unavailable for price projection",
                    reason_code="ADVISORY_PRICE_RANGE_OUTCOME_IDENTITY_MISMATCH",
                )
            expected_symbols = [str(item["symbol"]) for item in scored_candidates]
            _validate_review_policy_identity(
                list_items=list_items,
                expected_symbols=expected_symbols,
                review_policy_sha256=review_policy_sha256,
            )
            outcome_bundle_id = str(outcome.get("outcome_bundle_id") or "")
            price_bundle = self._price_range_bundle_loader(
                model_root=model_root,
                package_id=PACKAGE_ID,
                manifest_sha256=MANIFEST_SHA256,
                style_profile_hash=STYLE_PROFILE_HASH,
                parent_bundle_id=parent_bundle.bundle_id,
                outcome_bundle_id=outcome_bundle_id,
            )
            candidates = self._price_range_scorer(
                price_bundle,
                features,
                contexts=realtime.price_range_contexts,
                context_unavailable=realtime.price_range_unavailable,
                outcome_candidates=outcome.get("candidates") or [],
                review_policy=review_policy,
                review_policy_sha256=review_policy_sha256,
                target_trade_date=target_trade_date,
            )
            candidate_by_symbol = {str(item.get("symbol")): item for item in candidates}
            if len(candidate_by_symbol) != len(candidates) or set(candidate_by_symbol) != set(expected_symbols):
                raise AdvisoryModelFirstError(
                    "price-range inference does not preserve the M2 candidate group",
                    reason_code="ADVISORY_PRICE_RANGE_INFERENCE_FAILED",
                )
            candidates = [candidate_by_symbol[symbol] for symbol in expected_symbols]
        except AdvisoryModelFirstError as exc:
            LOGGER.warning(
                "advisory price-range shadow unavailable program_id=%s target_trade_date=%s "
                "reason_code=%s context=%s elapsed_ms=%d",
                program_id,
                target_trade_date.isoformat(),
                exc.reason_code,
                exc.context,
                round((time.monotonic() - started) * 1000),
            )
            return unavailable_price_range_envelope(
                reason_code=exc.reason_code,
                message=str(exc),
            )
        except Exception as exc:
            LOGGER.exception(
                "advisory price-range shadow failed unexpectedly program_id=%s target_trade_date=%s elapsed_ms=%d",
                program_id,
                target_trade_date.isoformat(),
                round((time.monotonic() - started) * 1000),
            )
            return unavailable_price_range_envelope(
                reason_code="ADVISORY_PRICE_RANGE_INFERENCE_FAILED",
                message=f"unexpected price-range inference failure: {type(exc).__name__}",
            )
        LOGGER.info(
            "advisory price-range shadow completed program_id=%s target_trade_date=%s "
            "candidate_count=%d unavailable_count=%d elapsed_ms=%d",
            program_id,
            target_trade_date.isoformat(),
            len(candidates),
            sum(item.get("status") != "EXPERIMENTAL_SHADOW" for item in candidates),
            round((time.monotonic() - started) * 1000),
        )
        return {
            "status": "EXPERIMENTAL_SHADOW",
            "calibration_state": "UNCALIBRATED",
            "price_range_bundle_id": price_bundle.price_range_bundle_id,
            "parent_bundle_id": parent_bundle.bundle_id,
            "outcome_bundle_id": outcome_bundle_id,
            "model_version": price_bundle.manifest["request_id"],
            "price_basis": "UNADJUSTED_CNY_DECISION_CLOSE",
            "candidates": candidates,
            "reason_code": None,
            "message": None,
        }

    def _outcome_shadow(
        self,
        *,
        model_root: str,
        parent_bundle: LoadedAdvisoryModelBundle,
        features: pd.DataFrame,
        scored_candidates: list[dict[str, Any]],
        program_id: str,
        target_trade_date: date,
    ) -> dict[str, Any]:
        started = time.monotonic()
        try:
            outcome_bundle = self._outcome_bundle_loader(
                model_root=model_root,
                package_id=PACKAGE_ID,
                manifest_sha256=MANIFEST_SHA256,
                style_profile_hash=STYLE_PROFILE_HASH,
                parent_bundle_id=parent_bundle.bundle_id,
            )
            predictions = self._outcome_scorer(outcome_bundle, features)
            prediction_by_symbol = {str(item["symbol"]): item for item in predictions}
            expected_symbols = [str(item["symbol"]) for item in scored_candidates]
            if (
                len(prediction_by_symbol) != len(predictions)
                or len(set(expected_symbols)) != len(expected_symbols)
                or set(prediction_by_symbol) != set(expected_symbols)
                or len(predictions) != len(expected_symbols)
            ):
                raise AdvisoryModelFirstError(
                    "outcome inference does not preserve the M2 candidate group",
                    reason_code="ADVISORY_OUTCOME_INFERENCE_FAILED",
                    context={
                        "m2_candidate_count": len(expected_symbols),
                        "outcome_candidate_count": len(predictions),
                    },
                )
            ordered = [prediction_by_symbol[symbol] for symbol in expected_symbols]
        except AdvisoryModelFirstError as exc:
            LOGGER.warning(
                "advisory outcome shadow unavailable program_id=%s target_trade_date=%s "
                "reason_code=%s context=%s elapsed_ms=%d",
                program_id,
                target_trade_date.isoformat(),
                exc.reason_code,
                exc.context,
                round((time.monotonic() - started) * 1000),
            )
            return unavailable_outcome_envelope(reason_code=exc.reason_code, message=str(exc))
        except Exception as exc:
            LOGGER.exception(
                "advisory outcome shadow failed unexpectedly program_id=%s target_trade_date=%s elapsed_ms=%d",
                program_id,
                target_trade_date.isoformat(),
                round((time.monotonic() - started) * 1000),
            )
            return unavailable_outcome_envelope(
                reason_code="ADVISORY_OUTCOME_INFERENCE_FAILED",
                message=f"unexpected outcome inference failure: {type(exc).__name__}",
            )
        LOGGER.info(
            "advisory outcome shadow completed program_id=%s target_trade_date=%s candidate_count=%d elapsed_ms=%d",
            program_id,
            target_trade_date.isoformat(),
            len(ordered),
            round((time.monotonic() - started) * 1000),
        )
        return {
            "status": "EXPERIMENTAL_SHADOW",
            "calibration_state": "UNCALIBRATED",
            "outcome_bundle_id": outcome_bundle.outcome_bundle_id,
            "parent_bundle_id": parent_bundle.bundle_id,
            "model_version": outcome_bundle.manifest["request_id"],
            "horizons": list(outcome_bundle.manifest["horizons"]),
            "candidates": ordered,
            "reason_code": None,
            "message": None,
        }

    def _selection_list_context(
        self,
        *,
        program_id: str,
        target_trade_date: date,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        versions = self._program_service.recommendation_list_versions(program_id, limit=500, offset=0)
        matching = [
            version
            for version in versions
            if date.fromisoformat(str(version.get("target_trade_date") or version["trade_date"])[:10])
            == target_trade_date
        ]
        if not matching:
            raise AdvisoryModelFirstError(
                "target date has no persisted Advisory recommendation list",
                reason_code="ADVISORY_MODEL_SELECTION_INPUT_UNAVAILABLE",
            )
        detail = self._program_service.recommendation_list_version_detail(matching[0]["list_version_id"])
        items = list(detail.get("items") or [])
        if not items:
            raise AdvisoryModelFirstError(
                "target recommendation list has no persisted candidates",
                reason_code="ADVISORY_MODEL_SELECTION_INPUT_UNAVAILABLE",
            )
        return dict(detail["list_version"]), items


def _validate_bundle_runtime(bundle: LoadedAdvisoryModelBundle) -> None:
    manifest = bundle.manifest
    schema_version = manifest.get("schema_version", "advisory_model_bundle_v1")
    expected_calibration = (
        "UNCALIBRATED" if schema_version == "advisory_model_bundle_v1" else "NOT_APPLICABLE_RANKING_SCORE"
    )
    terminal_weights = manifest.get("terminal_weights") or {}
    if schema_version == "advisory_model_bundle_v2" and not _valid_m5_runtime_policy(bundle):
        raise AdvisoryModelFirstError(
            "M5A model bundle runtime policy is incompatible with the frozen quality contract",
            reason_code="ADVISORY_M5_RUNTIME_POLICY_MISMATCH",
        )
    if (
        schema_version not in {"advisory_model_bundle_v1", "advisory_model_bundle_v2"}
        or manifest.get("status") != "EXPERIMENTAL_SHADOW"
        or manifest.get("calibration_state") != expected_calibration
        or manifest.get("selection_runtime_semantics_hash") != RUNTIME_SEMANTICS_HASH
        or manifest.get("selection_runtime_semantics") != RUNTIME_SEMANTICS_PAYLOAD
        or manifest.get("style_profile_id") != STYLE_PROFILE_ID
        or not _matches_terminal_weights(terminal_weights)
        or tuple(bundle.feature_schema.get("trained_feature_names") or ()) != tuple(MODEL_FEATURE_COLUMNS)
    ):
        raise AdvisoryModelFirstError(
            "model bundle runtime semantics are incompatible with the Advisory shadow path",
            reason_code="ADVISORY_MODEL_RUNTIME_SEMANTICS_MISMATCH",
        )


def _valid_m5_runtime_policy(bundle: LoadedAdvisoryModelBundle) -> bool:
    manifest = bundle.manifest
    try:
        model_weight = float(manifest.get("model_weight"))
    except (TypeError, ValueError):
        return False
    return (
        manifest.get("ensemble_score_policy") == ENSEMBLE_SCORE_POLICY
        and manifest.get("selection_prior_policy") == SELECTION_PRIOR_POLICY
        and manifest.get("explanation_policy") == "MODEL_MEMBER_RAW_CONTRIBUTION_MEAN_V1"
        and tuple(manifest.get("seeds") or ()) == QUALITY_SEEDS
        and model_weight in {0.25, 0.5, 0.75, 1.0}
        and len(bundle.boosters) == len(QUALITY_SEEDS)
        and bundle.booster is None
    )


def _validate_review_policy_identity(
    *,
    list_items: list[dict[str, Any]],
    expected_symbols: list[str],
    review_policy_sha256: str,
) -> None:
    expected = set(expected_symbols)
    hashes_by_symbol: dict[str, set[str]] = {}
    for item in list_items:
        symbol = str(item.get("symbol") or "").strip().upper()
        if symbol not in expected:
            continue
        evidence = item.get("evidence_json")
        value = evidence.get("review_policy_sha256") if isinstance(evidence, Mapping) else None
        hashes_by_symbol.setdefault(symbol, set()).add(str(value or "").strip())
    if set(hashes_by_symbol) != expected or any(
        values != {review_policy_sha256} for values in hashes_by_symbol.values()
    ):
        raise AdvisoryModelFirstError(
            "recommendation list review policy identity differs from the active Program",
            reason_code="ADVISORY_PRICE_RANGE_POLICY_IDENTITY_MISMATCH",
            context={
                "expected_symbol_count": len(expected),
                "observed_symbol_count": len(hashes_by_symbol),
            },
        )


def _matches_terminal_weights(value: Any) -> bool:
    if not isinstance(value, Mapping) or set(value) != set(TERMINAL_WEIGHTS):
        return False
    try:
        return all(
            np.isclose(
                float(value[leg_id]),
                float(expected_weight),
                rtol=0.0,
                atol=1e-10,
            )
            for leg_id, expected_weight in TERMINAL_WEIGHTS.items()
        )
    except (TypeError, ValueError):
        return False


def _resolve_decision_date(
    *,
    list_version: Mapping[str, Any],
    selection_run: Any,
) -> date:
    values: set[date] = set()
    explicit = list_version.get("selection_as_of_trade_date")
    if explicit:
        values.add(date.fromisoformat(str(explicit)[:10]))
    for candidate in selection_run.aggregate_results:
        raw = candidate.selection_entry_price_time
        if raw:
            values.add(date.fromisoformat(str(raw)[:10]))
    if len(values) != 1:
        raise AdvisoryModelFirstError(
            "persisted Selection inputs do not identify one decision cutoff date",
            reason_code="ADVISORY_MODEL_DECISION_CLOCK_MISMATCH",
            context={"date_count": len(values)},
        )
    return next(iter(values))


def _candidate_frame(
    rows: list[Any],
    *,
    program_id: str,
    binding_version_id: str,
    decision_date: date,
    target_trade_date: date,
    target_count: int,
    bundle: LoadedAdvisoryModelBundle,
) -> pd.DataFrame:
    if target_count < 1 or target_count > 20:
        raise AdvisoryModelFirstError(
            "model shadow candidate count must be between 1 and 20",
            reason_code="ADVISORY_MODEL_CANDIDATE_GROUP_INCOMPLETE",
            context={"target_count": target_count},
        )
    selected = sorted(
        (row for row in rows if int(row.rank) <= target_count), key=lambda row: (int(row.rank), row.symbol)
    )
    expected_ranks = list(range(1, len(selected) + 1))
    actual_ranks = [int(row.rank) for row in selected]
    symbols = [str(row.symbol).upper() for row in selected]
    if (
        not selected
        or len(selected) > target_count
        or actual_ranks != expected_ranks
        or len(set(symbols)) != len(symbols)
    ):
        raise AdvisoryModelFirstError(
            "persisted Selection candidate group is incomplete or non-contiguous",
            reason_code="ADVISORY_MODEL_CANDIDATE_GROUP_INCOMPLETE",
            context={"expected_count": target_count, "actual_count": len(selected), "actual_ranks": actual_ranks},
        )
    terminal_weights = bundle.manifest.get("terminal_weights") or {}
    candidate_group_size = len(selected)
    payloads: list[dict[str, Any]] = []
    for row in selected:
        scores = row.component_scores or {}
        legs: dict[str, Mapping[str, Any]] = {}
        for leg_id in (LSTM_LEG_ID, FUND_LEG_ID):
            leg = scores.get(leg_id)
            if not isinstance(leg, Mapping):
                raise AdvisoryModelFirstError(
                    "persisted Selection candidate is missing a frozen Alpha leg",
                    reason_code="ADVISORY_MODEL_CANDIDATE_GROUP_INCOMPLETE",
                    context={"symbol": row.symbol, "leg_id": leg_id},
                )
            required = {"raw_score", "normalized_score", "leg_rank", "weight"}
            if not required.issubset(leg):
                raise AdvisoryModelFirstError(
                    "persisted Selection Alpha leg is incomplete",
                    reason_code="ADVISORY_MODEL_CANDIDATE_GROUP_INCOMPLETE",
                    context={"symbol": row.symbol, "leg_id": leg_id},
                )
            if not np.isclose(float(leg["weight"]), float(terminal_weights[leg_id]), rtol=0.0, atol=1e-10):
                raise AdvisoryModelFirstError(
                    "persisted Selection Alpha weight differs from the model runtime semantics",
                    reason_code="ADVISORY_MODEL_RUNTIME_SEMANTICS_MISMATCH",
                    context={"symbol": row.symbol, "leg_id": leg_id},
                )
            legs[leg_id] = leg
        weighted_score = sum(
            float(legs[leg_id]["normalized_score"]) * float(terminal_weights[leg_id])
            for leg_id in (LSTM_LEG_ID, FUND_LEG_ID)
        )
        if not np.isfinite(weighted_score) or not np.isclose(float(row.score), weighted_score, rtol=0.0, atol=1e-8):
            raise AdvisoryModelFirstError(
                "persisted Selection score differs from the frozen Alpha-leg combination",
                reason_code="ADVISORY_MODEL_RUNTIME_SEMANTICS_MISMATCH",
                context={"symbol": row.symbol},
            )
        payloads.append(
            {
                "trade_date": pd.Timestamp(decision_date),
                "decision_as_of_trade_date": pd.Timestamp(decision_date),
                "target_trade_date": pd.Timestamp(target_trade_date),
                "instrument": str(row.symbol).upper(),
                "program_id": program_id,
                "binding_version_id": binding_version_id,
                "package_id": PACKAGE_ID,
                "manifest_sha256": MANIFEST_SHA256,
                "selection_runtime_semantics_hash": RUNTIME_SEMANTICS_HASH,
                "selection_source_rank": int(scores.get("raw_rank") or row.rank),
                "selection_effective_rank": int(row.rank),
                "candidate_group_size": candidate_group_size,
                "combined_score": float(row.score),
                f"raw__{LSTM_LEG_ID}": float(legs[LSTM_LEG_ID]["raw_score"]),
                f"norm__{LSTM_LEG_ID}": float(legs[LSTM_LEG_ID]["normalized_score"]),
                f"rank__{LSTM_LEG_ID}": int(legs[LSTM_LEG_ID]["leg_rank"]),
                f"weight__{LSTM_LEG_ID}": float(legs[LSTM_LEG_ID]["weight"]),
                f"raw__{FUND_LEG_ID}": float(legs[FUND_LEG_ID]["raw_score"]),
                f"norm__{FUND_LEG_ID}": float(legs[FUND_LEG_ID]["normalized_score"]),
                f"rank__{FUND_LEG_ID}": int(legs[FUND_LEG_ID]["leg_rank"]),
                f"weight__{FUND_LEG_ID}": float(legs[FUND_LEG_ID]["weight"]),
            }
        )
    return pd.DataFrame(payloads)


def _score(bundle: LoadedAdvisoryModelBundle, features: pd.DataFrame) -> list[dict[str, Any]]:
    matrix = _coerce_numeric_feature_dtypes(features.loc[:, MODEL_FEATURE_COLUMNS])
    vocabulary = bundle.feature_schema.get("categorical_vocabulary") or {}
    for column in CATEGORICAL_FEATURE_COLUMNS:
        categories = tuple(int(value) for value in vocabulary.get(column) or ())
        if not categories:
            raise AdvisoryModelFirstError(
                "model bundle categorical vocabulary is empty",
                reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
                context={"feature": column},
            )
        numeric = pd.to_numeric(matrix[column], errors="coerce")
        unseen = numeric.notna() & ~numeric.isin(categories)
        if unseen.any():
            missing_indicator = f"{column}__missing"
            matrix.loc[unseen, missing_indicator] = 1
            numeric = numeric.mask(unseen)
        matrix[column] = pd.Categorical(numeric, categories=categories)
    schema_version = bundle.manifest.get("schema_version", "advisory_model_bundle_v1")
    boosters = (bundle.booster,) if schema_version == "advisory_model_bundle_v1" else bundle.boosters
    if not boosters or any(tuple(booster.feature_name()) != tuple(MODEL_FEATURE_COLUMNS) for booster in boosters):
        raise AdvisoryModelFirstError(
            "LightGBM feature order differs from the frozen feature schema",
            reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
        )
    try:
        raw_scores = [np.asarray(booster.predict(matrix), dtype=float) for booster in boosters]
        raw_contributions = [
            np.asarray(booster.predict(matrix, pred_contrib=True), dtype=float) for booster in boosters
        ]
    except Exception as exc:
        LOGGER.exception(
            "LightGBM shadow inference failed bundle_id=%s candidate_count=%d",
            bundle.bundle_id,
            len(matrix),
        )
        raise AdvisoryModelFirstError(
            "LightGBM shadow inference failed",
            reason_code="ADVISORY_MODEL_REALTIME_DATA_UNAVAILABLE",
            context={"error_type": type(exc).__name__},
        ) from exc
    if any(score.shape != (len(features),) for score in raw_scores) or any(
        contribution.shape != (len(features), len(MODEL_FEATURE_COLUMNS) + 1) for contribution in raw_contributions
    ):
        raise AdvisoryModelFirstError(
            "LightGBM shadow output dimensions are invalid",
            reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
        )
    if any(not np.isfinite(score).all() for score in raw_scores) or any(
        not np.isfinite(contribution).all() for contribution in raw_contributions
    ):
        raise AdvisoryModelFirstError(
            "LightGBM shadow output contains a non-finite value",
            reason_code="ADVISORY_MODEL_REALTIME_DATA_UNAVAILABLE",
        )
    if schema_version == "advisory_model_bundle_v1":
        scores = raw_scores[0]
        contributions = raw_contributions[0]
        score_components: list[dict[str, float] | None] = [None] * len(features)
    else:
        percentile_scores = np.column_stack([_runtime_percentile(raw, features) for raw in raw_scores])
        ensemble_scores = percentile_scores.mean(axis=1)
        declared_group_sizes = pd.to_numeric(features["candidate_group_size"], errors="raise").to_numpy(dtype=float)
        if (
            len(set(declared_group_sizes.tolist())) != 1
            or declared_group_sizes[0] < len(features)
            or declared_group_sizes[0] < 1
        ):
            raise AdvisoryModelFirstError(
                "M5A runtime candidate group identity is invalid",
                reason_code="ADVISORY_M5_RUNTIME_POLICY_MISMATCH",
            )
        selection_ranks = pd.to_numeric(features["selection_effective_rank"], errors="raise").to_numpy(dtype=float)
        if (selection_ranks < 1).any() or (selection_ranks > declared_group_sizes).any():
            raise AdvisoryModelFirstError(
                "M5A runtime selection rank is outside the frozen candidate group",
                reason_code="ADVISORY_M5_RUNTIME_POLICY_MISMATCH",
            )
        selection_prior = (declared_group_sizes - selection_ranks) / np.maximum(declared_group_sizes - 1.0, 1.0)
        model_weight = float(bundle.manifest["model_weight"])
        scores = model_weight * ensemble_scores + (1.0 - model_weight) * selection_prior
        contributions = np.mean(np.stack(raw_contributions, axis=0), axis=0) * model_weight
        score_components = [
            {
                "ensemble_score": float(ensemble_scores[index]),
                "selection_prior": float(selection_prior[index]),
                "model_weight": model_weight,
            }
            for index in range(len(features))
        ]
    order = sorted(range(len(features)), key=lambda index: (-scores[index], str(features.iloc[index]["instrument"])))
    rank_by_index = {index: rank for rank, index in enumerate(order, start=1)}
    output: list[dict[str, Any]] = []
    for index, row in features.reset_index(drop=True).iterrows():
        top_indices = np.argsort(np.abs(contributions[index, :-1]), kind="stable")[-5:][::-1]
        item = {
            "symbol": str(row["instrument"]),
            "selection_effective_rank": int(row["selection_effective_rank"]),
            "selection_score": float(row["parent_combined_score"]),
            "advisory_model_rank": int(rank_by_index[index]),
            "advisory_model_score": float(scores[index]),
            "is_top5": rank_by_index[index] <= 5,
            "top_feature_contributions": [
                {
                    "feature": MODEL_FEATURE_COLUMNS[int(feature_index)],
                    "contribution": float(contributions[index, feature_index]),
                }
                for feature_index in top_indices
            ],
        }
        if score_components[index] is not None:
            item["score_components"] = score_components[index]
            item["explanation_policy"] = bundle.manifest["explanation_policy"]
        output.append(item)
    return sorted(output, key=lambda item: (item["advisory_model_rank"], item["symbol"]))


def _runtime_percentile(raw_scores: np.ndarray, features: pd.DataFrame) -> np.ndarray:
    order = sorted(
        range(len(features)),
        key=lambda index: (-raw_scores[index], str(features.iloc[index]["instrument"])),
    )
    denominator = max(len(order) - 1, 1)
    output = np.empty(len(order), dtype=float)
    for position, index in enumerate(order):
        output[index] = 1.0 - position / denominator
    return output


def _unavailable_response(
    *,
    program_id: str,
    target_trade_date: date,
    reason_code: str,
    message: str,
) -> dict[str, Any]:
    return {
        "status": "MODEL_UNAVAILABLE",
        "calibration_state": "UNCALIBRATED",
        "program_id": program_id,
        "binding_version_id": None,
        "package_id": None,
        "manifest_sha256": None,
        "decision_as_of_trade_date": None,
        "target_trade_date": target_trade_date.isoformat(),
        "selection_runtime_semantics_hash": None,
        "model_version": None,
        "bundle_id": None,
        "feature_schema_version": None,
        "candidate_count": 0,
        "shortlist_count": 0,
        "candidates": [],
        "baselines": {},
        "hmm_unavailable": [],
        "outcome": unavailable_outcome_envelope(
            reason_code="ADVISORY_OUTCOME_BUNDLE_NOT_AVAILABLE",
            message="parent model shadow is unavailable",
        ),
        "price_range": unavailable_price_range_envelope(
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_NOT_AVAILABLE_FOR_PACKAGE",
            message="parent model shadow is unavailable",
        ),
        "reason_code": reason_code,
        "message": message,
    }
