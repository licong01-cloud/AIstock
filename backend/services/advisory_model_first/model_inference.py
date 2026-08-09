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
from backend.services.advisory_model_first.realtime_feature_source import PostgresRealtimeFeatureSource
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
    STYLE_PROFILE_HASH,
)
from backend.services.advisory_program import AdvisoryProgramService
from backend.services.selection_center.models import SelectionRunStatus
from backend.services.selection_center.service import SelectionCenterService


LOGGER = logging.getLogger(__name__)


class AdvisoryModelShadowService:
    def __init__(
        self,
        *,
        program_service: AdvisoryProgramService | None = None,
        selection_service: SelectionCenterService | None = None,
        feature_source: PostgresRealtimeFeatureSource | None = None,
        model_root_provider: Any | None = None,
        bundle_loader: Any = load_exact_shadow_bundle,
    ) -> None:
        self._program_service = program_service or AdvisoryProgramService()
        self._selection_service = selection_service or SelectionCenterService()
        self._feature_source = feature_source or PostgresRealtimeFeatureSource()
        self._model_root_provider = model_root_provider or (
            lambda: os.getenv("AISTOCK_ADVISORY_MODEL_ROOT", "").strip()
        )
        self._bundle_loader = bundle_loader

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
        source_run_ids = {
            str((item.get("evidence_json") or {}).get("source_run_id") or "").strip()
            for item in list_items
        }
        source_run_ids.discard("")
        if len(source_run_ids) != 1:
            raise AdvisoryModelFirstError(
                "recommendation list does not identify exactly one persisted Selection run",
                reason_code="ADVISORY_MODEL_SELECTION_INPUT_UNAVAILABLE",
                context={"source_run_count": len(source_run_ids)},
            )
        selection_run = self._selection_service.get_run(next(iter(source_run_ids)))
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
        decision_date = _resolve_decision_date(list_version=list_version, list_items=list_items, selection_run=selection_run)
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
        shortlist_count = min(5, len(scored))
        return {
            "status": "EXPERIMENTAL_SHADOW",
            "calibration_state": "UNCALIBRATED",
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
    if (
        manifest.get("status") != "EXPERIMENTAL_SHADOW"
        or manifest.get("calibration_state") != "UNCALIBRATED"
        or manifest.get("selection_runtime_semantics_hash") != RUNTIME_SEMANTICS_HASH
        or tuple(bundle.feature_schema.get("trained_feature_names") or ()) != tuple(MODEL_FEATURE_COLUMNS)
    ):
        raise AdvisoryModelFirstError(
            "model bundle runtime semantics are incompatible with the Advisory shadow path",
            reason_code="ADVISORY_MODEL_RUNTIME_SEMANTICS_MISMATCH",
        )


def _resolve_decision_date(
    *,
    list_version: Mapping[str, Any],
    list_items: list[dict[str, Any]],
    selection_run: Any,
) -> date:
    values: set[date] = set()
    explicit = list_version.get("selection_as_of_trade_date")
    if explicit:
        values.add(date.fromisoformat(str(explicit)[:10]))
    for item in list_items:
        evidence = item.get("evidence_json") or {}
        raw = evidence.get("reference_price_trade_date") or evidence.get("selection_entry_price_time")
        if raw:
            values.add(date.fromisoformat(str(raw)[:10]))
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
    selected = sorted((row for row in rows if int(row.rank) <= target_count), key=lambda row: (int(row.rank), row.symbol))
    expected_ranks = list(range(1, len(selected) + 1))
    actual_ranks = [int(row.rank) for row in selected]
    symbols = [str(row.symbol).upper() for row in selected]
    if len(selected) != target_count or actual_ranks != expected_ranks or len(set(symbols)) != len(symbols):
        raise AdvisoryModelFirstError(
            "persisted Selection candidate group is incomplete or non-contiguous",
            reason_code="ADVISORY_MODEL_CANDIDATE_GROUP_INCOMPLETE",
            context={"expected_count": target_count, "actual_count": len(selected), "actual_ranks": actual_ranks},
        )
    terminal_weights = bundle.manifest.get("terminal_weights") or {}
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
                "candidate_group_size": target_count,
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
    model_feature_names = tuple(bundle.booster.feature_name())
    if model_feature_names != tuple(MODEL_FEATURE_COLUMNS):
        raise AdvisoryModelFirstError(
            "LightGBM feature order differs from the frozen feature schema",
            reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
        )
    try:
        scores = np.asarray(bundle.booster.predict(matrix), dtype=float)
        contributions = np.asarray(bundle.booster.predict(matrix, pred_contrib=True), dtype=float)
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
    if scores.shape != (len(features),) or contributions.shape != (len(features), len(MODEL_FEATURE_COLUMNS) + 1):
        raise AdvisoryModelFirstError(
            "LightGBM shadow output dimensions are invalid",
            reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
        )
    if not np.isfinite(scores).all() or not np.isfinite(contributions).all():
        raise AdvisoryModelFirstError(
            "LightGBM shadow output contains a non-finite value",
            reason_code="ADVISORY_MODEL_REALTIME_DATA_UNAVAILABLE",
        )
    order = sorted(range(len(features)), key=lambda index: (-scores[index], str(features.iloc[index]["instrument"])))
    rank_by_index = {index: rank for rank, index in enumerate(order, start=1)}
    output: list[dict[str, Any]] = []
    for index, row in features.reset_index(drop=True).iterrows():
        top_indices = np.argsort(np.abs(contributions[index, :-1]), kind="stable")[-5:][::-1]
        output.append(
            {
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
        )
    return sorted(output, key=lambda item: (item["advisory_model_rank"], item["symbol"]))


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
        "reason_code": reason_code,
        "message": message,
    }
