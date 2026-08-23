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
from backend.services.advisory_model_first.meta_label_bundle import (
    load_exact_meta_label_runtime_bundle,
    score_meta_label_bundle,
)
from backend.services.advisory_model_first.model_binding_resolution import (
    AdvisoryModelBindingResolutionV1,
    AdvisoryModelBindingResolver,
    META_LABEL_MODEL_ROLE,
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
from backend.services.advisory_model_first.reranker_training import (
    _coerce_numeric_feature_dtypes,
)
from backend.services.advisory_model_first.shared_feature_builder import (
    build_advisory_feature_matrix,
)
from backend.services.advisory_program import AdvisoryProgramService
from backend.services.selection_center.canonical_pit_runtime import (
    has_canonical_pit_runtime_profile,
    require_canonical_pit_generation_current,
    require_canonical_pit_runtime_binding,
)
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
        meta_label_bundle_loader: Any = load_exact_meta_label_runtime_bundle,
        meta_label_scorer: Any = score_meta_label_bundle,
        outcome_bundle_loader: Any = load_exact_outcome_bundle,
        outcome_scorer: Any = score_outcome_bundle,
        price_range_bundle_loader: Any = load_exact_price_range_bundle,
        price_range_scorer: Any = score_price_range_bundle,
        binding_resolver: AdvisoryModelBindingResolver | None = None,
    ) -> None:
        self._program_service = program_service or AdvisoryProgramService()
        self._selection_service = selection_service or SelectionCenterService()
        self._review_source = review_source or PostgresAdvisoryReviewSource()
        self._feature_source = feature_source or PostgresRealtimeFeatureSource()
        self._model_root_provider = model_root_provider or (
            lambda: os.getenv("AISTOCK_ADVISORY_MODEL_ROOT", "").strip()
        )
        self._bundle_loader = bundle_loader
        self._meta_label_bundle_loader = meta_label_bundle_loader
        self._meta_label_scorer = meta_label_scorer
        self._outcome_bundle_loader = outcome_bundle_loader
        self._outcome_scorer = outcome_scorer
        self._price_range_bundle_loader = price_range_bundle_loader
        self._price_range_scorer = price_range_scorer
        self._binding_resolver = binding_resolver or AdvisoryModelBindingResolver()

    def model_root(self) -> str:
        return str(self._model_root_provider() or "").strip()

    def model_shadow(
        self, *, program_id: str, target_trade_date: date
    ) -> dict[str, Any]:
        return self._visible_model_shadow(
            program_id=program_id,
            target_trade_date=target_trade_date,
            frozen_program=None,
            frozen_binding=None,
        )

    def model_shadow_for_forward(
        self,
        *,
        program: Any,
        binding_version_id: str,
        target_trade_date: date,
        list_version_id: str,
        review_run_id: str,
        selection_run_id: str,
    ) -> dict[str, Any]:
        return self._visible_model_shadow(
            program_id=str(program.program_id),
            target_trade_date=target_trade_date,
            frozen_program=program,
            frozen_binding={
                "binding_version_id": binding_version_id,
                "package_ids": list(program.package_ids),
            },
            frozen_input_ids={
                "list_version_id": list_version_id,
                "review_run_id": review_run_id,
                "selection_run_id": selection_run_id,
            },
        )

    def _visible_model_shadow(
        self,
        *,
        program_id: str,
        target_trade_date: date,
        frozen_program: Any | None,
        frozen_binding: Mapping[str, Any] | None,
        frozen_input_ids: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        started = time.monotonic()
        try:
            result = self._model_shadow(
                program_id=program_id,
                target_trade_date=target_trade_date,
                frozen_program=frozen_program,
                frozen_binding=frozen_binding,
                frozen_input_ids=frozen_input_ids,
            )
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

    def _model_shadow(
        self,
        *,
        program_id: str,
        target_trade_date: date,
        frozen_program: Any | None = None,
        frozen_binding: Mapping[str, Any] | None = None,
        frozen_input_ids: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        program = frozen_program or self._program_service.get_program(program_id)
        binding = (
            dict(frozen_binding)
            if frozen_binding is not None
            else self._program_service.active_binding(program_id)
        )
        package_ids = tuple(program.package_ids)
        if (
            len(package_ids) != 1
            or tuple(binding.get("package_ids") or ()) != package_ids
        ):
            raise AdvisoryModelFirstError(
                "no model bundle is bound to this Advisory Program identity",
                reason_code="ADVISORY_MODEL_BUNDLE_NOT_AVAILABLE_FOR_PACKAGE",
            )
        model_root = self.model_root()
        if not model_root:
            raise AdvisoryModelFirstError(
                "AISTOCK_ADVISORY_MODEL_ROOT is not configured",
                reason_code="ADVISORY_MODEL_ROOT_NOT_CONFIGURED",
            )
        if not self._binding_resolver.is_configured(
            model_root=model_root,
            program_id=program_id,
            binding_version_id=str(binding.get("binding_version_id") or ""),
        ):
            raise AdvisoryModelFirstError(
                "no exact model descriptor is configured for this Advisory Program binding",
                reason_code="ADVISORY_MODEL_BUNDLE_NOT_AVAILABLE_FOR_PACKAGE",
            )
        list_version, list_items = self._selection_list_context(
            program_id=program_id,
            target_trade_date=target_trade_date,
            list_version_id=(frozen_input_ids or {}).get("list_version_id"),
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
        expected_review_run_id = str(
            (frozen_input_ids or {}).get("review_run_id") or ""
        ).strip()
        if expected_review_run_id and review_run_id != expected_review_run_id:
            raise AdvisoryModelFirstError(
                "forward recommendation list differs from the frozen review run",
                reason_code="ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH",
            )
        review_run = self._review_source.get(review_run_id)
        review_selection_run_ids = tuple(
            str(value).strip()
            for value in review_run.selection_run_ids
            if str(value).strip()
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
        expected_selection_run_id = str(
            (frozen_input_ids or {}).get("selection_run_id") or ""
        ).strip()
        if expected_selection_run_id and selection_run_id != expected_selection_run_id:
            raise AdvisoryModelFirstError(
                "forward review differs from the frozen Selection run",
                reason_code="ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH",
            )
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
            or tuple(selection_run.package_ids) != package_ids
        ):
            raise AdvisoryModelFirstError(
                "persisted Selection run identity differs from the model target",
                reason_code="ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH",
            )
        resolution = self._binding_resolver.resolve(
            model_root=model_root,
            program=program,
            active_binding=binding,
            selection_run=selection_run,
        )
        is_meta_label = resolution.model_role == META_LABEL_MODEL_ROLE
        if is_meta_label:
            bundle = self._meta_label_bundle_loader(
                model_root=model_root,
                bundle_id=resolution.bundle_id,
                bundle_manifest_sha256=resolution.bundle_manifest_sha256,
            )
            _validate_meta_label_bundle_runtime(bundle, resolution=resolution)
        else:
            bundle = self._bundle_loader(
                model_root=model_root,
                package_id=resolution.package_id,
                manifest_sha256=resolution.manifest_sha256,
                style_profile_hash=resolution.style_profile_hash,
            )
            _validate_bundle_runtime(bundle, resolution=resolution)
        decision_date = _resolve_decision_date(
            list_version=list_version, selection_run=selection_run
        )
        if decision_date >= target_trade_date:
            raise AdvisoryModelFirstError(
                "persisted Selection decision clock is invalid",
                reason_code="ADVISORY_MODEL_DECISION_CLOCK_MISMATCH",
                context={
                    "decision_as_of_trade_date": decision_date.isoformat(),
                    "target_trade_date": target_trade_date.isoformat(),
                },
            )
        if is_meta_label and int(program.target_count) != 20:
            raise AdvisoryModelFirstError(
                "meta-label challenger requires the frozen Selection Top20 candidate group",
                reason_code="ADVISORY_MODEL_RUNTIME_SEMANTICS_MISMATCH",
                context={"program_target_count": int(program.target_count)},
            )
        candidates = _candidate_frame(
            selection_run.aggregate_results,
            program_id=program_id,
            binding_version_id=binding["binding_version_id"],
            decision_date=decision_date,
            target_trade_date=target_trade_date,
            target_count=20 if is_meta_label else int(program.target_count),
            bundle=bundle,
            resolution=resolution,
        )
        if is_meta_label and len(candidates) != 20:
            raise AdvisoryModelFirstError(
                "meta-label challenger requires a complete Selection Top20 candidate group",
                reason_code="ADVISORY_MODEL_CANDIDATE_GROUP_INCOMPLETE",
                context={"expected_count": 20, "actual_count": len(candidates)},
            )
        pit_universe_key: str | None = None
        if has_canonical_pit_runtime_profile(selection_run.runtime_config):
            runtime_lease = require_canonical_pit_runtime_binding(
                selection_run.runtime_config,
                trade_date=decision_date,
            )
            require_canonical_pit_generation_current(selection_run.runtime_config)
            pit_universe_key = runtime_lease.universe_key
        realtime = self._feature_source.load(
            symbols=candidates["instrument"].tolist(),
            decision_as_of_trade_date=decision_date,
            target_trade_date=target_trade_date,
            continuation_cutoff=date.fromisoformat(
                str(
                    bundle["continuation_cutoff"]
                    if is_meta_label
                    else bundle.manifest["continuation_cutoff"]
                )
            ),
            hmm_models=bundle["hmm_models"] if is_meta_label else bundle.hmm_models,
            **(
                {"pit_universe_key": pit_universe_key}
                if pit_universe_key is not None
                else {}
            ),
        )
        built = build_advisory_feature_matrix(
            candidates=candidates,
            candidate_daily=realtime.candidate_daily,
            candidate_static=realtime.candidate_static,
            market_daily=realtime.market_daily,
            benchmark_daily=realtime.benchmark_daily,
            suspend_rows=realtime.suspend_rows,
            hmm_states=realtime.hmm_states,
            component_roles=resolution.component_roles,
        )
        if len(built.coverage) != 1 or built.coverage.iloc[0]["status"] != "available":
            missing = (
                built.coverage.iloc[0].get("required_missing_columns", [])
                if len(built.coverage)
                else []
            )
            raise AdvisoryModelFirstError(
                "realtime feature matrix has required-value gaps",
                reason_code="ADVISORY_MODEL_FEATURE_REQUIRED_VALUE_MISSING",
                context={"required_missing_columns": list(missing)},
            )
        if len(built.features) != len(candidates):
            raise AdvisoryModelFirstError(
                "realtime feature matrix does not preserve the candidate group",
                reason_code="ADVISORY_MODEL_CANDIDATE_GROUP_INCOMPLETE",
                context={
                    "candidate_count": len(candidates),
                    "feature_count": len(built.features),
                },
            )
        if is_meta_label:
            scored = format_meta_label_candidates(
                self._meta_label_scorer(bundle, built.features),
                features=built.features,
            )
            outcome = unavailable_outcome_envelope(
                reason_code="ADVISORY_OUTCOME_BUNDLE_NOT_AVAILABLE",
                message="meta-label entry-priority challenger does not provide an outcome child model",
            )
            price_range = unavailable_price_range_envelope(
                reason_code="ADVISORY_PRICE_RANGE_BUNDLE_NOT_AVAILABLE_FOR_PACKAGE",
                message="meta-label entry-priority challenger does not provide a price-range child model",
            )
        else:
            scored = _score(bundle, built.features)
            outcome = self._outcome_shadow(
                model_root=model_root,
                parent_bundle=bundle,
                features=built.features,
                scored_candidates=scored,
                program_id=program_id,
                target_trade_date=target_trade_date,
                resolution=resolution,
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
                resolution=resolution,
            )
        manifest = bundle["manifest"] if is_meta_label else bundle.manifest
        shortlist_count = min(5, len(scored))
        return {
            "status": "EXPERIMENTAL_SHADOW",
            "calibration_state": manifest["calibration_state"],
            "program_id": program_id,
            "binding_version_id": binding["binding_version_id"],
            "package_id": resolution.package_id,
            "manifest_sha256": resolution.manifest_sha256,
            "style_profile_id": resolution.style_profile_id,
            "style_profile_hash": resolution.style_profile_hash,
            "model_descriptor_sha256": resolution.descriptor_sha256,
            "model_role": resolution.model_role,
            "shadow_policy_sha256": resolution.shadow_policy_sha256,
            **(
                {
                    "shadow_policy": bundle["shadow_policy"],
                    "cost_policy": bundle["cost_policy"],
                    "cost_policy_sha256": bundle["cost_policy_sha256"],
                    "evaluation_contract_version": "advisory_forward_model_evaluation_v1",
                }
                if is_meta_label
                else {}
            ),
            "shadow_policy_maturity_horizon_days": (
                bundle["shadow_policy_maturity_horizon_days"] if is_meta_label else None
            ),
            "decision_as_of_trade_date": decision_date.isoformat(),
            "target_trade_date": target_trade_date.isoformat(),
            "selection_runtime_semantics_hash": resolution.selection_runtime_semantics_hash,
            "model_version": manifest["request_id"],
            "bundle_id": resolution.bundle_id,
            "feature_schema_version": manifest["feature_schema_version"],
            "candidate_count": len(scored),
            "shortlist_count": shortlist_count,
            "candidates": scored,
            "baselines": bundle["baselines"] if is_meta_label else bundle.baselines,
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
        resolution: AdvisoryModelBindingResolutionV1,
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
            price_range_outcome_bundle_id = str(
                outcome.get("parent_outcome_bundle_id") or outcome_bundle_id
            )
            price_bundle = self._price_range_bundle_loader(
                model_root=model_root,
                package_id=resolution.package_id,
                manifest_sha256=resolution.manifest_sha256,
                style_profile_hash=resolution.style_profile_hash,
                parent_bundle_id=parent_bundle.bundle_id,
                outcome_bundle_id=price_range_outcome_bundle_id,
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
            if len(candidate_by_symbol) != len(candidates) or set(
                candidate_by_symbol
            ) != set(expected_symbols):
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
            "calibration_state": price_bundle.manifest["calibration_state"],
            "price_range_bundle_id": price_bundle.price_range_bundle_id,
            "parent_bundle_id": parent_bundle.bundle_id,
            "outcome_bundle_id": price_range_outcome_bundle_id,
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
        resolution: AdvisoryModelBindingResolutionV1,
    ) -> dict[str, Any]:
        started = time.monotonic()
        try:
            outcome_bundle = self._outcome_bundle_loader(
                model_root=model_root,
                package_id=resolution.package_id,
                manifest_sha256=resolution.manifest_sha256,
                style_profile_hash=resolution.style_profile_hash,
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
            return unavailable_outcome_envelope(
                reason_code=exc.reason_code, message=str(exc)
            )
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
            "calibration_state": outcome_bundle.manifest.get(
                "calibration_state", "UNCALIBRATED"
            ),
            "calibration_policy_version": outcome_bundle.manifest.get(
                "calibration_policy_version"
            ),
            "parent_outcome_bundle_id": outcome_bundle.manifest.get(
                "parent_outcome_bundle_id"
            ),
            "binary_calibration_state": outcome_bundle.manifest.get(
                "binary_calibration_state", "UNCALIBRATED"
            ),
            "return_interval_calibration_state": outcome_bundle.manifest.get(
                "return_interval_calibration_state", "UNCALIBRATED"
            ),
            "path_upper_calibration_state": outcome_bundle.manifest.get(
                "path_upper_calibration_state", "UNCALIBRATED"
            ),
            "holding_calibration_state": outcome_bundle.manifest.get(
                "holding_calibration_state", "UNCALIBRATED"
            ),
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
        list_version_id: str | None = None,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        if list_version_id:
            detail = self._program_service.recommendation_list_version_detail(
                list_version_id
            )
            version = dict(detail.get("list_version") or {})
            raw_target = version.get("target_trade_date") or version.get("trade_date")
            if (
                version.get("program_id") != program_id
                or not raw_target
                or date.fromisoformat(str(raw_target)[:10]) != target_trade_date
            ):
                raise AdvisoryModelFirstError(
                    "frozen recommendation list identity differs from the forward target",
                    reason_code="ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH",
                )
            items = list(detail.get("items") or [])
            if not items:
                raise AdvisoryModelFirstError(
                    "target recommendation list has no persisted candidates",
                    reason_code="ADVISORY_MODEL_SELECTION_INPUT_UNAVAILABLE",
                )
            return version, items
        versions = self._program_service.recommendation_list_versions(
            program_id, limit=500, offset=0
        )
        matching = [
            version
            for version in versions
            if date.fromisoformat(
                str(version.get("target_trade_date") or version["trade_date"])[:10]
            )
            == target_trade_date
        ]
        if not matching:
            raise AdvisoryModelFirstError(
                "target date has no persisted Advisory recommendation list",
                reason_code="ADVISORY_MODEL_SELECTION_INPUT_UNAVAILABLE",
            )
        detail = self._program_service.recommendation_list_version_detail(
            matching[0]["list_version_id"]
        )
        items = list(detail.get("items") or [])
        if not items:
            raise AdvisoryModelFirstError(
                "target recommendation list has no persisted candidates",
                reason_code="ADVISORY_MODEL_SELECTION_INPUT_UNAVAILABLE",
            )
        return dict(detail["list_version"]), items


def _validate_bundle_runtime(
    bundle: LoadedAdvisoryModelBundle,
    *,
    resolution: AdvisoryModelBindingResolutionV1,
) -> None:
    manifest = bundle.manifest
    schema_version = manifest.get("schema_version", "advisory_model_bundle_v1")
    expected_calibration = (
        "UNCALIBRATED"
        if schema_version == "advisory_model_bundle_v1"
        else "NOT_APPLICABLE_RANKING_SCORE"
    )
    terminal_weights = manifest.get("terminal_weights") or {}
    if schema_version == "advisory_model_bundle_v2" and not _valid_m5_runtime_policy(
        bundle
    ):
        raise AdvisoryModelFirstError(
            "M5A model bundle runtime policy is incompatible with the frozen quality contract",
            reason_code="ADVISORY_M5_RUNTIME_POLICY_MISMATCH",
        )
    if (
        schema_version not in {"advisory_model_bundle_v1", "advisory_model_bundle_v2"}
        or manifest.get("status") != "EXPERIMENTAL_SHADOW"
        or manifest.get("calibration_state") != expected_calibration
        or manifest.get("package_id") != resolution.package_id
        or manifest.get("manifest_sha256") != resolution.manifest_sha256
        or manifest.get("style_profile_id") != resolution.style_profile_id
        or manifest.get("style_profile_hash") != resolution.style_profile_hash
        or manifest.get("selection_runtime_semantics_hash")
        != resolution.selection_runtime_semantics_hash
        or manifest.get("feature_schema_version") != resolution.feature_schema_version
        or manifest.get("feature_schema_hash") != resolution.feature_schema_hash
        or bundle.bundle_id != resolution.bundle_id
        or bundle.manifest_file_sha256 != resolution.bundle_manifest_sha256
        or not _matches_terminal_weights(
            terminal_weights, component_roles=resolution.component_roles
        )
        or tuple(bundle.feature_schema.get("trained_feature_names") or ())
        != tuple(MODEL_FEATURE_COLUMNS)
    ):
        raise AdvisoryModelFirstError(
            "model bundle runtime semantics are incompatible with the Advisory shadow path",
            reason_code="ADVISORY_MODEL_RUNTIME_SEMANTICS_MISMATCH",
        )


def validate_frozen_bundle_runtime(
    bundle: LoadedAdvisoryModelBundle,
    *,
    resolution: AdvisoryModelBindingResolutionV1,
) -> None:
    """Validate the same frozen runtime contract for explicit research inference."""

    _validate_bundle_runtime(bundle, resolution=resolution)


def _validate_meta_label_bundle_runtime(
    bundle: Mapping[str, Any],
    *,
    resolution: AdvisoryModelBindingResolutionV1,
) -> None:
    manifest = bundle.get("manifest")
    feature_schema = bundle.get("feature_schema")
    if not isinstance(manifest, Mapping) or not isinstance(feature_schema, Mapping):
        raise AdvisoryModelFirstError(
            "meta-label runtime bundle is incomplete",
            reason_code="ADVISORY_META_LABEL_BUNDLE_INVALID",
        )
    if (
        resolution.model_role != META_LABEL_MODEL_ROLE
        or manifest.get("schema_version") != "advisory_meta_label_bundle_v1"
        or manifest.get("model_role") != META_LABEL_MODEL_ROLE
        or manifest.get("status") != "EXPERIMENTAL_MODEL"
        or manifest.get("calibration_state") != "UNCALIBRATED"
        or manifest.get("program_id") != resolution.program_id
        or manifest.get("binding_version_id") != resolution.binding_version_id
        or manifest.get("package_id") != resolution.package_id
        or manifest.get("manifest_sha256") != resolution.manifest_sha256
        or manifest.get("style_profile_id") != resolution.style_profile_id
        or manifest.get("style_profile_hash") != resolution.style_profile_hash
        or manifest.get("shadow_policy_sha256") != resolution.shadow_policy_sha256
        or manifest.get("feature_schema_version") != resolution.feature_schema_version
        or manifest.get("feature_schema_hash") != resolution.feature_schema_hash
        or feature_schema.get("schema_version") != resolution.feature_schema_version
        or feature_schema.get("feature_schema_hash") != resolution.feature_schema_hash
        or manifest.get("bundle_id") != resolution.bundle_id
        or bundle.get("manifest_file_sha256") != resolution.bundle_manifest_sha256
        or tuple(feature_schema.get("trained_feature_names") or ())
        != tuple(MODEL_FEATURE_COLUMNS)
        or set(feature_schema.get("categorical_vocabulary") or {})
        != set(CATEGORICAL_FEATURE_COLUMNS)
        or any(
            not tuple(
                (feature_schema.get("categorical_vocabulary") or {}).get(column) or ()
            )
            for column in CATEGORICAL_FEATURE_COLUMNS
        )
        or not _matches_terminal_weights(
            resolution.terminal_weights,
            component_roles=resolution.component_roles,
        )
    ):
        raise AdvisoryModelFirstError(
            "meta-label bundle runtime semantics are incompatible with the Advisory shadow path",
            reason_code="ADVISORY_MODEL_RUNTIME_SEMANTICS_MISMATCH",
        )


def validate_meta_label_bundle_runtime(
    bundle: Mapping[str, Any],
    *,
    resolution: AdvisoryModelBindingResolutionV1,
) -> None:
    """Validate the production meta-label contract for explicit historical inference."""

    _validate_meta_label_bundle_runtime(bundle, resolution=resolution)


def _valid_m5_runtime_policy(bundle: LoadedAdvisoryModelBundle) -> bool:
    manifest = bundle.manifest
    try:
        model_weight = float(manifest.get("model_weight"))
    except (TypeError, ValueError):
        return False
    return (
        manifest.get("ensemble_score_policy") == ENSEMBLE_SCORE_POLICY
        and manifest.get("selection_prior_policy") == SELECTION_PRIOR_POLICY
        and manifest.get("explanation_policy")
        == "MODEL_MEMBER_RAW_CONTRIBUTION_MEAN_V1"
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
        value = (
            evidence.get("review_policy_sha256")
            if isinstance(evidence, Mapping)
            else None
        )
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


def _matches_terminal_weights(
    value: Any, *, component_roles: Mapping[str, str]
) -> bool:
    component_ids = set(component_roles.values())
    if not isinstance(value, Mapping) or set(value) != component_ids:
        return False
    try:
        weights = [float(value[component_id]) for component_id in component_ids]
        return all(
            np.isfinite(weight) and weight > 0 for weight in weights
        ) and np.isclose(sum(weights), 1.0, rtol=0.0, atol=1e-10)
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
    bundle: Any,
    resolution: AdvisoryModelBindingResolutionV1,
) -> pd.DataFrame:
    if target_count < 1 or target_count > 20:
        raise AdvisoryModelFirstError(
            "model shadow candidate count must be between 1 and 20",
            reason_code="ADVISORY_MODEL_CANDIDATE_GROUP_INCOMPLETE",
            context={"target_count": target_count},
        )
    selected = sorted(
        (row for row in rows if int(row.rank) <= target_count),
        key=lambda row: (int(row.rank), row.symbol),
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
            context={
                "expected_count": target_count,
                "actual_count": len(selected),
                "actual_ranks": actual_ranks,
            },
        )
    terminal_weights = (
        resolution.terminal_weights
        if resolution.model_role == META_LABEL_MODEL_ROLE
        else bundle.manifest.get("terminal_weights") or {}
    )
    candidate_group_size = len(selected)
    payloads: list[dict[str, Any]] = []
    component_ids = tuple(resolution.component_roles[role] for role in ("lstm", "fund"))
    for row in selected:
        scores = row.component_scores or {}
        legs: dict[str, Mapping[str, Any]] = {}
        for leg_id in component_ids:
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
            if not np.isclose(
                float(leg["weight"]),
                float(terminal_weights[leg_id]),
                rtol=0.0,
                atol=1e-10,
            ):
                raise AdvisoryModelFirstError(
                    "persisted Selection Alpha weight differs from the model runtime semantics",
                    reason_code="ADVISORY_MODEL_RUNTIME_SEMANTICS_MISMATCH",
                    context={"symbol": row.symbol, "leg_id": leg_id},
                )
            legs[leg_id] = leg
        weighted_score = sum(
            float(legs[leg_id]["normalized_score"]) * float(terminal_weights[leg_id])
            for leg_id in component_ids
        )
        if not np.isfinite(weighted_score) or not np.isclose(
            float(row.score), weighted_score, rtol=0.0, atol=1e-8
        ):
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
                "package_id": resolution.package_id,
                "manifest_sha256": resolution.manifest_sha256,
                "selection_runtime_semantics_hash": resolution.selection_runtime_semantics_hash,
                "selection_source_rank": int(scores.get("raw_rank") or row.rank),
                "selection_effective_rank": int(row.rank),
                "candidate_group_size": candidate_group_size,
                "combined_score": float(row.score),
                **{
                    f"{prefix}__{leg_id}": (
                        float(legs[leg_id][field])
                        if field != "leg_rank"
                        else int(legs[leg_id][field])
                    )
                    for leg_id in component_ids
                    for prefix, field in (
                        ("raw", "raw_score"),
                        ("norm", "normalized_score"),
                        ("rank", "leg_rank"),
                        ("weight", "weight"),
                    )
                },
            }
        )
    return pd.DataFrame(payloads)


def build_frozen_candidate_frame(
    rows: list[Any],
    *,
    program_id: str,
    binding_version_id: str,
    decision_date: date,
    target_trade_date: date,
    target_count: int,
    bundle: LoadedAdvisoryModelBundle,
    resolution: AdvisoryModelBindingResolutionV1,
) -> pd.DataFrame:
    """Build the production-identical model input frame for a frozen research parent."""

    return _candidate_frame(
        rows,
        program_id=program_id,
        binding_version_id=binding_version_id,
        decision_date=decision_date,
        target_trade_date=target_trade_date,
        target_count=target_count,
        bundle=bundle,
        resolution=resolution,
    )


def prepare_frozen_feature_matrix(
    bundle: LoadedAdvisoryModelBundle,
    features: pd.DataFrame,
) -> pd.DataFrame:
    """Prepare the canonical LightGBM matrix without requiring LightGBM in this process."""

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
    return matrix


def _score(
    bundle: LoadedAdvisoryModelBundle, features: pd.DataFrame
) -> list[dict[str, Any]]:
    matrix = prepare_frozen_feature_matrix(bundle, features)
    schema_version = bundle.manifest.get("schema_version", "advisory_model_bundle_v1")
    boosters = (
        (bundle.booster,)
        if schema_version == "advisory_model_bundle_v1"
        else bundle.boosters
    )
    booster_feature_names = (
        [tuple(booster.feature_name()) for booster in boosters] if boosters else []
    )
    try:
        raw_scores = [
            np.asarray(booster.predict(matrix), dtype=float) for booster in boosters
        ]
        raw_contributions = [
            np.asarray(booster.predict(matrix, pred_contrib=True), dtype=float)
            for booster in boosters
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
    return score_frozen_feature_matrix_from_booster_outputs(
        bundle,
        features,
        raw_scores=raw_scores,
        raw_contributions=raw_contributions,
        booster_feature_names=booster_feature_names,
    )


def format_meta_label_candidates(
    scored: pd.DataFrame,
    *,
    features: pd.DataFrame,
) -> list[dict[str, Any]]:
    required = {
        "instrument",
        "selection_effective_rank",
        "take_probability",
        "skip_probability",
        "advisory_model_confidence",
        "entry_priority_rank",
        "selection_exit_rank",
        "model_status",
        "calibration_state",
    }
    if not isinstance(scored, pd.DataFrame) or not required.issubset(scored):
        raise AdvisoryModelFirstError(
            "meta-label scorer output is incomplete",
            reason_code="ADVISORY_META_LABEL_SCORING_INVALID",
        )
    expected = features[
        [
            "instrument",
            "decision_as_of_trade_date",
            "target_trade_date",
            "selection_effective_rank",
            "parent_combined_score",
        ]
    ].copy()
    expected["instrument"] = expected["instrument"].astype(str)
    actual = scored.copy()
    actual["instrument"] = actual["instrument"].astype(str)
    if (
        len(actual) != len(expected)
        or actual["instrument"].nunique() != len(actual)
        or expected["instrument"].nunique() != len(expected)
        or set(actual["instrument"]) != set(expected["instrument"])
    ):
        raise AdvisoryModelFirstError(
            "meta-label scorer does not preserve the complete candidate group",
            reason_code="ADVISORY_MODEL_CANDIDATE_GROUP_INCOMPLETE",
        )
    actual = actual.merge(
        expected,
        on="instrument",
        how="left",
        validate="one_to_one",
        suffixes=("", "_expected"),
    )
    numeric_columns = (
        "take_probability",
        "skip_probability",
        "advisory_model_confidence",
        "entry_priority_rank",
        "selection_effective_rank",
        "selection_effective_rank_expected",
        "selection_exit_rank",
        "parent_combined_score",
    )
    try:
        numeric = {
            column: pd.to_numeric(actual[column], errors="raise")
            for column in numeric_columns
        }
    except (TypeError, ValueError) as exc:
        raise AdvisoryModelFirstError(
            "meta-label scorer output contains an invalid numeric value",
            reason_code="ADVISORY_META_LABEL_SCORING_INVALID",
        ) from exc
    try:
        decision_dates_match = pd.to_datetime(
            actual["decision_as_of_trade_date"]
        ).equals(pd.to_datetime(actual["decision_as_of_trade_date_expected"]))
        target_dates_match = pd.to_datetime(actual["target_trade_date"]).equals(
            pd.to_datetime(actual["target_trade_date_expected"])
        )
    except (TypeError, ValueError) as exc:
        raise AdvisoryModelFirstError(
            "meta-label scorer output contains an invalid decision clock",
            reason_code="ADVISORY_META_LABEL_SCORING_INVALID",
        ) from exc
    entry_ranks = numeric["entry_priority_rank"].to_numpy(dtype=float)
    selection_ranks = numeric["selection_effective_rank"].to_numpy(dtype=float)
    expected_selection_ranks = numeric["selection_effective_rank_expected"].to_numpy(
        dtype=float
    )
    exit_ranks = numeric["selection_exit_rank"].to_numpy(dtype=float)
    ranks = sorted(entry_ranks.astype(int).tolist())
    if (
        ranks != list(range(1, len(actual) + 1))
        or not np.isfinite(
            np.column_stack([numeric[column] for column in numeric_columns])
        ).all()
        or not np.array_equal(entry_ranks, np.rint(entry_ranks))
        or not np.array_equal(selection_ranks, np.rint(selection_ranks))
        or not np.array_equal(exit_ranks, np.rint(exit_ranks))
        or not decision_dates_match
        or not target_dates_match
        or not np.allclose(
            numeric["take_probability"] + numeric["skip_probability"],
            1.0,
            rtol=0.0,
            atol=1e-10,
        )
        or not np.allclose(
            numeric["advisory_model_confidence"],
            abs(numeric["take_probability"] - 0.5) * 2.0,
            rtol=0.0,
            atol=1e-10,
        )
        or (
            (numeric["take_probability"] < 0.0) | (numeric["take_probability"] > 1.0)
        ).any()
        or not np.array_equal(selection_ranks, expected_selection_ranks)
        or not np.array_equal(exit_ranks, expected_selection_ranks)
        or set(actual["model_status"]) != {"EXPERIMENTAL_MODEL"}
        or set(actual["calibration_state"]) != {"UNCALIBRATED"}
    ):
        raise AdvisoryModelFirstError(
            "meta-label scorer output violates the frozen entry/exit rank contract",
            reason_code="ADVISORY_META_LABEL_SCORING_INVALID",
        )
    output = [
        {
            "symbol": str(row.instrument),
            "selection_effective_rank": int(row.selection_effective_rank_expected),
            "selection_exit_rank": int(row.selection_exit_rank),
            "selection_score": float(row.parent_combined_score),
            "advisory_model_rank": int(row.entry_priority_rank),
            "entry_priority_rank": int(row.entry_priority_rank),
            "advisory_model_score": float(row.take_probability),
            "take_probability": float(row.take_probability),
            "skip_probability": float(row.skip_probability),
            "advisory_model_confidence": float(row.advisory_model_confidence),
            "model_status": str(row.model_status),
            "calibration_state": str(row.calibration_state),
            "is_top5": int(row.entry_priority_rank) <= 5,
            "top_feature_contributions": [],
        }
        for row in actual.itertuples(index=False)
    ]
    return sorted(
        output, key=lambda item: (item["advisory_model_rank"], item["symbol"])
    )


def score_frozen_feature_matrix_from_booster_outputs(
    bundle: LoadedAdvisoryModelBundle,
    features: pd.DataFrame,
    *,
    raw_scores: list[Any],
    raw_contributions: list[Any],
    booster_feature_names: list[tuple[str, ...]],
) -> list[dict[str, Any]]:
    """Apply the production M5A ensemble/rank semantics to verified booster outputs."""

    schema_version = bundle.manifest.get("schema_version", "advisory_model_bundle_v1")
    expected_member_count = (
        1 if schema_version == "advisory_model_bundle_v1" else len(bundle.boosters)
    )
    scores_by_member = [np.asarray(score, dtype=float) for score in raw_scores]
    contributions_by_member = [
        np.asarray(value, dtype=float) for value in raw_contributions
    ]
    if (
        expected_member_count < 1
        or len(scores_by_member) != expected_member_count
        or len(contributions_by_member) != expected_member_count
        or len(booster_feature_names) != expected_member_count
        or any(
            tuple(names) != tuple(MODEL_FEATURE_COLUMNS)
            for names in booster_feature_names
        )
    ):
        raise AdvisoryModelFirstError(
            "LightGBM feature order differs from the frozen feature schema",
            reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
        )
    if any(score.shape != (len(features),) for score in scores_by_member) or any(
        contribution.shape != (len(features), len(MODEL_FEATURE_COLUMNS) + 1)
        for contribution in contributions_by_member
    ):
        raise AdvisoryModelFirstError(
            "LightGBM shadow output dimensions are invalid",
            reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
        )
    if any(not np.isfinite(score).all() for score in scores_by_member) or any(
        not np.isfinite(contribution).all() for contribution in contributions_by_member
    ):
        raise AdvisoryModelFirstError(
            "LightGBM shadow output contains a non-finite value",
            reason_code="ADVISORY_MODEL_REALTIME_DATA_UNAVAILABLE",
        )
    if schema_version == "advisory_model_bundle_v1":
        scores = scores_by_member[0]
        contributions = contributions_by_member[0]
        score_components: list[dict[str, float] | None] = [None] * len(features)
    else:
        percentile_scores = np.column_stack(
            [_runtime_percentile(raw, features) for raw in scores_by_member]
        )
        ensemble_scores = percentile_scores.mean(axis=1)
        declared_group_sizes = pd.to_numeric(
            features["candidate_group_size"], errors="raise"
        ).to_numpy(dtype=float)
        if (
            len(set(declared_group_sizes.tolist())) != 1
            or declared_group_sizes[0] < len(features)
            or declared_group_sizes[0] < 1
        ):
            raise AdvisoryModelFirstError(
                "M5A runtime candidate group identity is invalid",
                reason_code="ADVISORY_M5_RUNTIME_POLICY_MISMATCH",
            )
        selection_ranks = pd.to_numeric(
            features["selection_effective_rank"], errors="raise"
        ).to_numpy(dtype=float)
        if (selection_ranks < 1).any() or (
            selection_ranks > declared_group_sizes
        ).any():
            raise AdvisoryModelFirstError(
                "M5A runtime selection rank is outside the frozen candidate group",
                reason_code="ADVISORY_M5_RUNTIME_POLICY_MISMATCH",
            )
        selection_prior = (declared_group_sizes - selection_ranks) / np.maximum(
            declared_group_sizes - 1.0, 1.0
        )
        model_weight = float(bundle.manifest["model_weight"])
        scores = model_weight * ensemble_scores + (1.0 - model_weight) * selection_prior
        contributions = (
            np.mean(np.stack(contributions_by_member, axis=0), axis=0) * model_weight
        )
        score_components = [
            {
                "ensemble_score": float(ensemble_scores[index]),
                "selection_prior": float(selection_prior[index]),
                "model_weight": model_weight,
            }
            for index in range(len(features))
        ]
    order = sorted(
        range(len(features)),
        key=lambda index: (-scores[index], str(features.iloc[index]["instrument"])),
    )
    rank_by_index = {index: rank for rank, index in enumerate(order, start=1)}
    output: list[dict[str, Any]] = []
    for index, row in features.reset_index(drop=True).iterrows():
        top_indices = np.argsort(np.abs(contributions[index, :-1]), kind="stable")[-5:][
            ::-1
        ]
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
    return sorted(
        output, key=lambda item: (item["advisory_model_rank"], item["symbol"])
    )


def score_frozen_feature_matrix(
    bundle: LoadedAdvisoryModelBundle,
    features: pd.DataFrame,
) -> list[dict[str, Any]]:
    """Score one complete frozen feature matrix with production runtime semantics."""

    return _score(bundle, features)


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
        "model_role": None,
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
