from __future__ import annotations

import hashlib
import json
import math
import os
import re
import sys
import tempfile
import time
from functools import lru_cache
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

import numpy as np
import pandas as pd

from backend.data_service.api import get_history_window
from backend.data_service.moneyflow_contract import MONEYFLOW_FIELD_MAP
from backend.data_service.preprocessor import compute_precomputed_factors, validate_precomputed_factors
from backend.db.pg_pool import get_conn
from backend.inference_engine import (
    _apply_saved_qe_infer_processors,
    _fetch_inference_fundamental_data,
    load_model_from_pkl,
    predict_scores,
)
from backend.services.advisory_model_first.contracts import PredictionArtifactDescriptor
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.independent_package_alpha_audit_contracts import (
    FACTOR_GROUP_CLOSURES,
    PACKAGE_ARM_IDS,
    AdvisoryIndependentPackageAlphaAuditRequestV1,
    FrozenPackageAuditArmV1,
)
from backend.services.advisory_model_first.prediction_source import ExactPredictionSource
from backend.services.dataset_release.pit import FrozenPitSnapshot, filter_frame_to_pit_spans
from backend.services.factor_validator import FactorValidator
from backend.services.market_data.instrument_validator import normalize_ts_code
from backend.services.model_store.artifact_store import PredictionArtifactStore
from backend.services.strategy_package.advisory_input_projection import (
    CANONICAL_HISTORICAL_QUERY_CONTRACT_HASH,
    get_strategy_package_inference_required_window,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


REASON_ASSET_INVALID = "ADVISORY_PACKAGE_BATCH_ASSET_INVALID"
REASON_SOURCE_READ_FAILED = "ADVISORY_PACKAGE_BATCH_SOURCE_READ_FAILED"
REASON_PREDICTION_INVALID = "ADVISORY_PACKAGE_BATCH_PREDICTION_INVALID"
REASON_FUTURE_DEPENDENCY = "ADVISORY_PACKAGE_BATCH_FUTURE_DEPENDENCY_DETECTED"
REASON_LIVE_PARITY = "ADVISORY_PACKAGE_BATCH_LIVE_PARITY_FAILED"

_STATIC_ALIASES = (
    "daily_basic.h5",
    "moneyflow.h5",
    "sector_data.h5",
    "bak_basic.h5",
    "cyq_perf.h5",
    "margin_detail.h5",
)
_FORBIDDEN_FACTOR_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("negative_shift", re.compile(r"\.shift\s*\(\s*-\d+")),
    ("centered_rolling", re.compile(r"\.rolling\s*\([^\n)]*center\s*=\s*True")),
    ("backward_fill", re.compile(r"\.bfill\s*\(")),
    ("reverse_slice", re.compile(r"\[\s*::\s*-1\s*\]")),
)

_STATIC_FIELD_MAPPING = {
    **MONEYFLOW_FIELD_MAP,
    "close": "db_close",
    "turnover_rate": "db_turnover_rate",
    "turnover_rate_f": "db_turnover_rate_f",
    "volume_ratio": "db_volume_ratio",
    "pe": "db_pe",
    "pe_ttm": "db_pe_ttm",
    "pb": "db_pb",
    "ps": "db_ps",
    "ps_ttm": "db_ps_ttm",
    "dv_ratio": "db_dv_ratio",
    "dv_ttm": "db_dv_ttm",
    "total_share": "db_total_share",
    "float_share": "db_float_share",
    "free_share": "db_free_share",
    "total_mv": "db_total_mv",
    "circ_mv": "db_circ_mv",
}


@dataclass(frozen=True)
class BatchSourcePanels:
    daily: pd.DataFrame
    static_raw: pd.DataFrame
    history_start: date
    decision_end: date
    source_receipts: tuple[dict[str, Any], ...]
    market_interval_read_count: int = 1
    static_interval_read_count: int = 1
    static_precomputed: bool = False

    def prefix(self, cutoff: date, *, instruments: set[str] | None = None) -> "BatchSourcePanels":
        daily = _slice_panel(self.daily, cutoff=cutoff, instruments=instruments)
        static = _slice_panel(self.static_raw, cutoff=cutoff, instruments=instruments)
        return BatchSourcePanels(
            daily=daily,
            static_raw=static,
            history_start=self.history_start,
            decision_end=cutoff,
            source_receipts=self.source_receipts,
            market_interval_read_count=0,
            static_interval_read_count=0,
            static_precomputed=self.static_precomputed,
        )


@dataclass(frozen=True)
class LoadedPackageModel:
    arm: FrozenPackageAuditArmV1
    workspace: Path
    model: Any
    model_kind: str
    inner_model: Any
    expected_feature_count: int
    factor_order: tuple[str, ...]
    primary_assets: Mapping[str, Any]


@dataclass(frozen=True)
class PackagePredictionBatchResult:
    predictions: Mapping[str, pd.DataFrame]
    coverage_daily: pd.DataFrame
    prediction_descriptors: Mapping[str, PredictionArtifactDescriptor]
    prediction_store_run_ids: Mapping[str, str]
    batch_receipt: Mapping[str, Any]
    causality_parity_receipt: Mapping[str, Any]


SourceLoader = Callable[[Sequence[str], date, date], BatchSourcePanels]
FactorRunner = Callable[[Path, str, BatchSourcePanels, Sequence[date], Path], pd.DataFrame]
ModelLoader = Callable[[Path], tuple[Any, str, Any, int]]
ModelPredictor = Callable[[Any, Any, str, pd.DataFrame], np.ndarray]
HistoryStartResolver = Callable[[date, int], date]


class StrategyPackageBatchPredictionRunner:
    """Generate fixed-window package predictions without per-day processes or reads."""

    def __init__(
        self,
        *,
        source_loader: SourceLoader | None = None,
        factor_runner: FactorRunner | None = None,
        model_loader: ModelLoader = load_model_from_pkl,
        model_predictor: ModelPredictor = predict_scores,
        history_start_resolver: HistoryStartResolver | None = None,
    ) -> None:
        self._source_loader = source_loader or load_bounded_source_panels
        self._factor_runner = factor_runner or run_factor_group_batch
        self._model_loader = model_loader
        self._model_predictor = model_predictor
        self._history_start_resolver = history_start_resolver or resolve_batch_history_start
        self._requires_factor_resource_receipt = factor_runner is None

    def run(
        self,
        *,
        request: AdvisoryIndependentPackageAlphaAuditRequestV1,
        pit_snapshot: FrozenPitSnapshot,
        decision_dates: Sequence[pd.Timestamp | date],
        temp_root: str | Path,
    ) -> PackagePredictionBatchResult:
        started = time.monotonic()
        decisions = _normalize_decision_dates(decision_dates)
        if tuple(item.date() for item in decisions) != tuple(sorted(item.date() for item in decisions)):
            _raise("batch decision dates are not ordered", REASON_PREDICTION_INVALID)
        if len(decisions) != 386 or decisions[0].date() != request.decision_date_start or decisions[-1].date() != request.decision_date_end:
            _raise(
                "batch decision date roster differs from the frozen N1 window",
                REASON_PREDICTION_INVALID,
                count=len(decisions),
            )
        if pit_snapshot.spans_sha256 != request.pit_spans_sha256:
            _raise("batch PIT snapshot differs from the frozen request", REASON_PREDICTION_INVALID)

        workspaces = {arm.arm_id: _verified_workspace(arm) for arm in request.packages}
        factor_orders = {arm.arm_id: _factor_order(workspaces[arm.arm_id]) for arm in request.packages}
        for arm in request.packages:
            if len(factor_orders[arm.arm_id]) != arm.factor_count:
                _raise(
                    "package factor order count differs from the frozen package",
                    REASON_ASSET_INVALID,
                    arm_id=arm.arm_id,
                )
        _verify_factor_group_equivalence(request.packages, factor_orders)
        required_window = max(get_strategy_package_inference_required_window(order) for order in factor_orders.values())
        history_start = self._history_start_resolver(request.decision_date_start, required_window)
        universe = sorted({span.ts_code for span in pit_snapshot.spans})
        source = self._source_loader(universe, history_start, request.decision_date_end)
        if source.market_interval_read_count != 1 or source.static_interval_read_count != 1:
            _raise("batch source loader did not perform exactly one bounded interval read", REASON_SOURCE_READ_FAILED)
        _validate_source_panels(source, universe=universe, decision_end=request.decision_date_end)
        if source.history_start != history_start:
            _raise(
                "batch source history_start differs from the resolved calendar boundary",
                REASON_SOURCE_READ_FAILED,
                expected=history_start.isoformat(),
                actual=source.history_start.isoformat(),
            )

        loaded_models: dict[str, LoadedPackageModel] = {}
        model_load_counts = {arm_id: 0 for arm_id in PACKAGE_ARM_IDS}
        for arm in request.packages:
            workspace = workspaces[arm.arm_id]
            model, model_kind, inner_model, expected_count = self._model_loader(workspace / "model" / "params.pkl")
            model_load_counts[arm.arm_id] += 1
            loaded_models[arm.arm_id] = LoadedPackageModel(
                arm=arm,
                workspace=workspace,
                model=model,
                model_kind=model_kind,
                inner_model=inner_model,
                expected_feature_count=int(expected_count or 0),
                factor_order=tuple(factor_orders[arm.arm_id]),
                primary_assets=_workspace_primary_assets(workspace),
            )

        temp_path = Path(temp_root)
        temp_path.mkdir(parents=True, exist_ok=True)
        prediction_parts: dict[str, list[pd.DataFrame]] = {arm_id: [] for arm_id in PACKAGE_ARM_IDS}
        coverage_parts: list[pd.DataFrame] = []
        factor_group_primary_count = 0
        factor_group_diagnostic_count = 0
        packages_by_closure = _packages_by_factor_closure(request.packages)
        temp_peak_bytes = 0
        factor_resource_receipts: list[dict[str, Any]] = []

        # Cross-sectional factors in the frozen packages make one union-wide
        # factor matrix semantically different from running the live policy on
        # each date's PIT universe. Batch mode therefore shares the interval
        # read and loaded models, while evaluating both closures in-process for
        # each decision date. It does not create per-day processes, DB reads,
        # workspaces, or models.
        for decision in decisions:
            active = _pit_members(pit_snapshot, decision.date())
            day_source = source.prefix(decision.date(), instruments=active)
            for closure in request.factor_group_closures:
                group = packages_by_closure[closure]
                representative = group[0]
                features = self._factor_runner(
                    workspaces[representative.arm_id],
                    closure,
                    day_source,
                    [decision.date()],
                    temp_path,
                )
                factor_group_primary_count += 1
                _validate_feature_matrix(
                    features,
                    factor_orders[representative.arm_id],
                    pd.DatetimeIndex([decision]),
                )
                resource_receipt = dict(features.attrs.get("factor_resource_receipt") or {})
                if self._requires_factor_resource_receipt and not resource_receipt:
                    _raise("factor batch omitted its resource receipt", REASON_SOURCE_READ_FAILED)
                if resource_receipt:
                    resource_receipt.update(
                        {
                            "mode": "PRIMARY_PIT_DAY",
                            "closure_sha256": closure,
                            "decision_date": decision.date().isoformat(),
                        }
                    )
                    factor_resource_receipts.append(resource_receipt)
                    temp_peak_bytes = max(
                        temp_peak_bytes,
                        int(resource_receipt.get("temp_peak_bytes") or 0),
                    )
                for arm in group:
                    scored, coverage = self._score_package_features(
                        loaded_models[arm.arm_id],
                        features,
                        pit_snapshot=pit_snapshot,
                        decisions=pd.DatetimeIndex([decision]),
                    )
                    prediction_parts[arm.arm_id].append(scored)
                    coverage_parts.append(coverage)
                del features
            del day_source

        predictions = {
            arm_id: pd.concat(parts).sort_index()
            for arm_id, parts in prediction_parts.items()
        }

        diagnostics: list[dict[str, Any]] = []
        for closure in request.factor_group_closures:
            group = packages_by_closure[closure]
            representative = group[0]
            for anchor in request.causality_anchor_dates:
                active = _pit_members(pit_snapshot, anchor)
                diagnostic_cutoff = (
                    anchor if anchor == request.decision_date_end else request.decision_date_end
                )
                prefix = source.prefix(diagnostic_cutoff, instruments=active)
                features = self._factor_runner(
                    workspaces[representative.arm_id],
                    closure,
                    prefix,
                    [anchor],
                    temp_path,
                )
                factor_group_diagnostic_count += 1
                resource_receipt = dict(features.attrs.get("factor_resource_receipt") or {})
                if self._requires_factor_resource_receipt and not resource_receipt:
                    _raise("diagnostic factor batch omitted its resource receipt", REASON_SOURCE_READ_FAILED)
                if resource_receipt:
                    resource_receipt.update(
                        {
                            "mode": (
                                "ISOLATED_END_DATE_PARITY"
                                if anchor == request.decision_date_end
                                else "FUTURE_POISON"
                            ),
                            "closure_sha256": closure,
                            "anchor_date": anchor.isoformat(),
                        }
                    )
                    factor_resource_receipts.append(resource_receipt)
                    temp_peak_bytes = max(temp_peak_bytes, int(resource_receipt.get("temp_peak_bytes") or 0))
                for arm in group:
                    replay, _ = self._score_package_features(
                        loaded_models[arm.arm_id],
                        features,
                        pit_snapshot=pit_snapshot,
                        decisions=pd.DatetimeIndex([pd.Timestamp(anchor)]),
                    )
                    full = predictions[arm.arm_id]
                    full = full[pd.to_datetime(full.index.get_level_values("datetime")).date == anchor]
                    diagnostics.append(
                        _compare_anchor_predictions(
                            arm_id=arm.arm_id,
                            anchor=anchor,
                            full=full,
                            replay=replay,
                            isolated_end_date=anchor == request.decision_date_end,
                            atol=request.score_parity_atol,
                            minimum_spearman=request.rank_parity_min_spearman,
                        )
                    )
                del features, prefix

        if model_load_counts != {arm_id: 1 for arm_id in PACKAGE_ARM_IDS}:
            _raise("one or more package models were not loaded exactly once", REASON_ASSET_INVALID)
        expected_primary_count = len(decisions) * len(request.factor_group_closures)
        if factor_group_primary_count != expected_primary_count or factor_group_diagnostic_count != 6:
            _raise(
                "factor group execution counts differ from the frozen batch contract",
                REASON_PREDICTION_INVALID,
                primary=factor_group_primary_count,
                expected_primary=expected_primary_count,
                diagnostic=factor_group_diagnostic_count,
            )

        elapsed_before_publish = time.monotonic() - started
        if temp_peak_bytes > request.resource_max_temp_bytes or elapsed_before_publish > request.resource_max_wall_seconds:
            _raise(
                "package batch exceeded its frozen temp or wall-time limit",
                "ADVISORY_PACKAGE_ALPHA_AUDIT_RESOURCE_LIMIT_EXCEEDED",
                temp_peak_bytes=temp_peak_bytes,
                temp_limit_bytes=request.resource_max_temp_bytes,
                elapsed_seconds=round(elapsed_before_publish, 3),
                wall_limit_seconds=request.resource_max_wall_seconds,
            )

        prediction_descriptors, prediction_run_ids = _publish_prediction_store(
            request=request,
            predictions=predictions,
        )
        prediction_identity = {
            arm_id: descriptor.model_dump(mode="json")
            for arm_id, descriptor in sorted(prediction_descriptors.items())
        }
        causality_payload = {
            "schema_version": "advisory_package_batch_causality_parity_receipt_v1",
            "anchor_dates": [item.isoformat() for item in request.causality_anchor_dates],
            "score_parity_atol": request.score_parity_atol,
            "rank_parity_min_spearman": request.rank_parity_min_spearman,
            "checks": diagnostics,
            "status": "PASS",
        }
        causality_payload["receipt_sha256"] = canonical_json_sha256(causality_payload)
        batch_receipt: dict[str, Any] = {
            "schema_version": "advisory_strategy_package_batch_prediction_receipt_v1",
            "request_sha256": request.request_sha256,
            "query_contract_sha256": CANONICAL_HISTORICAL_QUERY_CONTRACT_HASH,
            "history_start": source.history_start.isoformat(),
            "decision_end": source.decision_end.isoformat(),
            "decision_date_count": len(decisions),
            "pit_instrument_count": len(universe),
            "market_interval_read_count": source.market_interval_read_count,
            "static_interval_read_count": source.static_interval_read_count,
            "static_h5_physical_file_count": 1,
            "static_h5_hardlink_alias_count": len(_STATIC_ALIASES),
            "primary_factor_group_run_count": factor_group_primary_count,
            "primary_decision_batch_count": len(decisions),
            "primary_factor_group_run_count_per_decision": len(request.factor_group_closures),
            "primary_execution_semantics": "IN_PROCESS_EXACT_PIT_UNIVERSE_PER_DECISION",
            "diagnostic_factor_group_run_count": factor_group_diagnostic_count,
            "factor_group_total_run_count": factor_group_primary_count + factor_group_diagnostic_count,
            "model_load_count_by_arm": model_load_counts,
            "daily_wsl_process_count": 0,
            "daily_db_query_count": 0,
            "source_receipts": list(source.source_receipts),
            "factor_resource_receipts": factor_resource_receipts,
            "prediction_identity_sha256": canonical_json_sha256(prediction_identity),
            "causality_parity_sha256": causality_payload["receipt_sha256"],
            "temp_peak_bytes": temp_peak_bytes,
            "wall_seconds": round(time.monotonic() - started, 3),
            "status": "COMPLETE",
        }
        batch_receipt["receipt_sha256"] = canonical_json_sha256(batch_receipt)
        return PackagePredictionBatchResult(
            predictions=predictions,
            coverage_daily=pd.concat(coverage_parts, ignore_index=True).sort_values(
                ["arm_id", "decision_as_of_trade_date"]
            ),
            prediction_descriptors=prediction_descriptors,
            prediction_store_run_ids=prediction_run_ids,
            batch_receipt=batch_receipt,
            causality_parity_receipt=causality_payload,
        )

    def _score_package_features(
        self,
        loaded: LoadedPackageModel,
        features: pd.DataFrame,
        *,
        pit_snapshot: FrozenPitSnapshot,
        decisions: pd.DatetimeIndex,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        order = list(loaded.factor_order)
        matrix = features.reindex(columns=order).copy()
        processed = _apply_saved_qe_infer_processors(
            matrix,
            task_dir=loaded.workspace,
            primary_assets=dict(loaded.primary_assets),
        )
        if not processed.index.equals(matrix.index) or list(processed.columns) != order:
            _raise(
                "saved QE infer processors changed batch feature keys or order",
                REASON_PREDICTION_INVALID,
                arm_id=loaded.arm.arm_id,
            )
        if loaded.expected_feature_count and loaded.expected_feature_count != len(order):
            _raise(
                "package model feature count differs from the frozen factor order",
                REASON_ASSET_INVALID,
                arm_id=loaded.arm.arm_id,
                expected=loaded.expected_feature_count,
                actual=len(order),
            )
        numeric = processed.apply(pd.to_numeric, errors="coerce")
        values = numeric.to_numpy(dtype="float64", copy=False)
        invalid = pd.isna(values) | ~np.isfinite(values)
        valid_rows = ~invalid.any(axis=1)
        if not valid_rows.any():
            _raise(
                "package batch has no fully scorable feature rows",
                REASON_PREDICTION_INVALID,
                arm_id=loaded.arm.arm_id,
            )
        scorable = numeric.loc[valid_rows]
        old_strict = os.environ.get("AISTOCK_STRICT_INFERENCE")
        os.environ["AISTOCK_STRICT_INFERENCE"] = "1"
        try:
            raw_scores = self._model_predictor(
                loaded.model,
                loaded.inner_model,
                loaded.model_kind,
                scorable,
            )
        finally:
            if old_strict is None:
                os.environ.pop("AISTOCK_STRICT_INFERENCE", None)
            else:
                os.environ["AISTOCK_STRICT_INFERENCE"] = old_strict
        values_out = np.asarray(raw_scores).reshape(-1)
        if len(values_out) != len(scorable) or not np.isfinite(values_out).all():
            _raise(
                "package batch model returned invalid scores",
                REASON_PREDICTION_INVALID,
                arm_id=loaded.arm.arm_id,
            )
        scored = pd.DataFrame({"score": values_out.astype(float)}, index=scorable.index)
        scored, pit_receipt = filter_frame_to_pit_spans(scored, pit_snapshot)
        scored = scored[
            pd.to_datetime(scored.index.get_level_values("datetime")).normalize().isin(decisions)
        ].sort_index()
        if scored.empty or scored.index.has_duplicates:
            _raise(
                "package batch produced empty or duplicate PIT-filtered scores",
                REASON_PREDICTION_INVALID,
                arm_id=loaded.arm.arm_id,
            )
        coverage = _prediction_coverage(
            arm_id=loaded.arm.arm_id,
            decisions=decisions,
            pit_snapshot=pit_snapshot,
            feature_index=processed.index,
            valid_index=scorable.index,
            score_index=scored.index,
            invalid_mask=invalid,
            feature_columns=order,
            pit_receipt=pit_receipt,
        )
        return scored, coverage


def load_bounded_source_panels(universe: Sequence[str], start: date, end: date) -> BatchSourcePanels:
    if not universe or start > end:
        _raise("batch source request is empty or inverted", REASON_SOURCE_READ_FAILED)
    normalized = sorted({normalize_ts_code(item) for item in universe})
    started = time.monotonic()
    daily = get_history_window(
        normalized,
        start=datetime.combine(start, datetime.min.time()),
        end=datetime.combine(end, datetime.min.time()),
        fields=["open", "high", "low", "close", "volume", "amount", "factor"],
        freq="1d",
        adj="front",
        allow_xtquant_fallback=False,
        allow_tushare_adj_fallback=False,
    )
    market_seconds = time.monotonic() - started
    started = time.monotonic()
    static = _fetch_inference_fundamental_data(universe=normalized, start_date=start, end_date=end)
    static_seconds = time.monotonic() - started
    daily = _canonical_panel_index(daily, label="market history")
    static = _canonical_panel_index(static, label="fundamental/static")
    if daily.empty or static.empty:
        _raise(
            "bounded batch source returned an empty market or static panel",
            REASON_SOURCE_READ_FAILED,
            daily_rows=len(daily),
            static_rows=len(static),
        )
    static = _prepare_static_panel(static, daily)
    receipts = (
        {
            "source_role": "market_history",
            "query_contract": "get_history_window.timescaledb_only.v1",
            "start": start.isoformat(),
            "end": end.isoformat(),
            "row_count": len(daily),
            "content_sha256": _frame_sha256(daily),
            "elapsed_seconds": round(market_seconds, 3),
        },
        {
            "source_role": "fundamental_moneyflow_sector",
            "query_contract": "fetch_fundamental_data_ts.unbounded_by_natural_days.v1",
            "start": start.isoformat(),
            "end": end.isoformat(),
            "row_count": len(static),
            "content_sha256": _frame_sha256(static),
            "elapsed_seconds": round(static_seconds, 3),
        },
    )
    return BatchSourcePanels(
        daily=daily,
        static_raw=static,
        history_start=start,
        decision_end=end,
        source_receipts=receipts,
        static_precomputed=True,
    )


def resolve_batch_history_start(decision_start: date, required_window: int) -> date:
    offset = int(required_window) + 4
    if offset < 4:
        _raise("batch required history window is invalid", REASON_SOURCE_READ_FAILED)
    sql = """
        SELECT cal_date
        FROM market.trading_calendar
        WHERE cal_date <= %s AND is_trading = TRUE
        ORDER BY cal_date DESC
        OFFSET %s LIMIT 1
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (decision_start, offset))
                row = cur.fetchone()
    except Exception as exc:
        _raise(
            "batch trading-calendar history start cannot be resolved",
            REASON_SOURCE_READ_FAILED,
            error_type=type(exc).__name__,
        )
    if not row or not isinstance(row[0], date):
        _raise("batch trading calendar has insufficient lookback", REASON_SOURCE_READ_FAILED)
    return row[0]


def run_factor_group_batch(
    workspace: Path,
    closure_sha256: str,
    source: BatchSourcePanels,
    decision_dates: Sequence[date],
    temp_root: Path,
) -> pd.DataFrame:
    workspace = Path(workspace)
    order, calculations = _validated_factor_group(
        str(workspace.resolve()),
        closure_sha256,
        _sha256_file(workspace / "factor_order.json"),
        _sha256_file(workspace / "strategy_package_factor_entry.py"),
    )
    daily = _canonical_panel_index(source.daily, label="factor market input")
    static = (
        _canonical_panel_index(source.static_raw, label="precomputed static input")
        if source.static_precomputed
        else _prepare_static_panel(source.static_raw, daily)
    )
    decisions = pd.DatetimeIndex(pd.to_datetime(list(decision_dates))).normalize()
    base = daily[pd.to_datetime(daily.index.get_level_values("datetime")).normalize().isin(decisions)]
    if base.empty:
        _raise("factor batch has no market rows on requested decision dates", REASON_SOURCE_READ_FAILED)
    feature_index = base.index.sort_values()
    feature_values = np.empty((len(feature_index), len(order)), dtype="float64")
    factor_temp_peak = 0
    temp_root = Path(temp_root)
    temp_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=f"n2b_factor_{closure_sha256[:8]}_", dir=temp_root) as tmp:
        root = Path(tmp)
        daily.to_hdf(root / "daily_pv.h5", key="data", mode="w")
        static.to_parquet(root / "static_factors.parquet")
        physical_h5 = root / "static_factors.h5"
        static.to_hdf(physical_h5, key="data", mode="w")
        physical_inode = physical_h5.stat().st_ino
        for alias in _STATIC_ALIASES:
            target = root / alias
            try:
                os.link(physical_h5, target)
            except OSError as exc:
                _raise(
                    "batch static H5 hard-link alias cannot be created",
                    REASON_SOURCE_READ_FAILED,
                    alias=alias,
                    error_type=type(exc).__name__,
                )
            if target.stat().st_ino != physical_inode:
                _raise("batch static H5 alias is not a hard link", REASON_SOURCE_READ_FAILED, alias=alias)
        factor_temp_peak = max(factor_temp_peak, _tree_physical_size(root))
        old_cwd = Path.cwd()
        old_tempdir = tempfile.tempdir
        factor_work_root = root / "factor_work"
        factor_work_root.mkdir()
        try:
            tempfile.tempdir = str(factor_work_root)
            os.chdir(root)
            for column_index, (factor_name, calculation) in enumerate(zip(order, calculations)):
                part = calculation()
                series = _factor_series(part, factor_name=factor_name)
                dates = pd.to_datetime(series.index.get_level_values("datetime")).normalize()
                series = series[dates.isin(decisions)]
                feature_values[:, column_index] = series.reindex(feature_index).to_numpy(dtype="float64")
                factor_temp_peak = max(
                    factor_temp_peak,
                    _tree_physical_size(root) + int(series.memory_usage(index=True, deep=True)),
                )
        finally:
            os.chdir(old_cwd)
            tempfile.tempdir = old_tempdir
    features = pd.DataFrame(feature_values, index=feature_index, columns=order)
    features.attrs["factor_resource_receipt"] = {
        "static_h5_physical_file_count": 1,
        "static_h5_hardlink_alias_count": len(_STATIC_ALIASES),
        "temp_peak_bytes": factor_temp_peak,
        "feature_row_count": len(features),
        "feature_count": len(order),
    }
    return features


@lru_cache(maxsize=16)
def _validated_factor_group(
    workspace_text: str,
    closure_sha256: str,
    _factor_order_sha256: str,
    _factor_entry_sha256: str,
) -> tuple[tuple[str, ...], tuple[Callable[[], Any], ...]]:
    workspace = Path(workspace_text)
    order = tuple(_factor_order(workspace))
    factor_order_payload = json.loads((workspace / "factor_order.json").read_text(encoding="utf-8"))
    if factor_order_payload.get("alpha158_factors"):
        _raise("N2-B batch supports only the frozen all-dynamic package roster", REASON_ASSET_INVALID)
    old_dont_write_bytecode = sys.dont_write_bytecode
    sys.dont_write_bytecode = True
    try:
        module = FactorValidator().validate_and_load(
            f"n2b_{closure_sha256[:16]}",
            str(workspace / "strategy_package_factor_entry.py"),
        )
    finally:
        sys.dont_write_bytecode = old_dont_write_bytecode
    factor_files = getattr(module, "_FACTOR_FILES", None)
    if not isinstance(factor_files, dict) or tuple(factor_files) != order:
        _raise("factor entry mapping differs from factor_order", REASON_ASSET_INVALID)
    _scan_factor_sources(factor_files, workspace=workspace)
    calculations = tuple(
        getattr(module, name)
        for name in sorted(dir(module))
        if name.startswith("calculate_") and callable(getattr(module, name))
    )
    if len(calculations) != len(order):
        _raise(
            "factor entry calculation count differs from factor_order",
            REASON_ASSET_INVALID,
            expected=len(order),
            actual=len(calculations),
        )
    return order, calculations


def _prepare_static_panel(static_raw: pd.DataFrame, daily: pd.DataFrame) -> pd.DataFrame:
    static = _canonical_panel_index(static_raw, label="static raw input").rename(columns=_STATIC_FIELD_MAPPING)
    try:
        static = compute_precomputed_factors(static, daily)
    except Exception as exc:
        _raise(
            "batch precomputed factor preparation failed",
            REASON_SOURCE_READ_FAILED,
            error_type=type(exc).__name__,
            error=str(exc),
        )
    valid, missing = validate_precomputed_factors(static)
    if not valid:
        _raise(
            "batch precomputed factor panel is incomplete",
            REASON_SOURCE_READ_FAILED,
            missing_fields=list(missing),
        )
    return static.sort_index()


def _factor_series(value: Any, *, factor_name: str) -> pd.Series:
    if isinstance(value, pd.Series):
        frame = value.to_frame(factor_name)
    elif isinstance(value, pd.DataFrame):
        frame = value.copy()
    else:
        _raise(
            "factor calculation returned a non-tabular value",
            REASON_ASSET_INVALID,
            factor_name=factor_name,
            actual_type=type(value).__name__,
        )
    if len(frame.columns) != 1:
        _raise(
            "factor calculation must return exactly one column",
            REASON_ASSET_INVALID,
            factor_name=factor_name,
            columns=list(map(str, frame.columns)),
        )
    frame.columns = [factor_name]
    frame = _canonical_panel_index(frame, label=f"factor {factor_name}")
    return pd.to_numeric(frame[factor_name], errors="coerce")


def _scan_factor_sources(factor_files: Mapping[str, Any], *, workspace: Path) -> None:
    findings: list[dict[str, str]] = []
    workspace_root = workspace.resolve()
    for factor_name, raw_path in sorted(factor_files.items()):
        path = Path(str(raw_path)).resolve()
        if workspace_root not in path.parents:
            _raise(
                "frozen factor source escapes its described workspace",
                REASON_ASSET_INVALID,
                factor_name=factor_name,
                path=str(path),
            )
        if not path.is_file():
            _raise(
                "frozen factor source is missing",
                REASON_ASSET_INVALID,
                factor_name=factor_name,
                path=str(path),
            )
        text = path.read_text(encoding="utf-8")
        for pattern_name, pattern in _FORBIDDEN_FACTOR_PATTERNS:
            if pattern.search(text):
                findings.append({"factor_name": str(factor_name), "pattern": pattern_name})
    if findings:
        _raise(
            "frozen factor closure contains an explicit future-looking operator",
            REASON_FUTURE_DEPENDENCY,
            findings=findings,
        )


def _publish_prediction_store(
    *,
    request: AdvisoryIndependentPackageAlphaAuditRequestV1,
    predictions: Mapping[str, pd.DataFrame],
) -> tuple[dict[str, PredictionArtifactDescriptor], dict[str, str]]:
    store = PredictionArtifactStore(request.prediction_store_root)
    run_ids: dict[str, str] = {}
    for arm_id in PACKAGE_ARM_IDS:
        frame = predictions[arm_id]
        run_id = f"n2b_{request.request_sha256[:16]}_{arm_id.lower()}"
        with tempfile.TemporaryDirectory(prefix=f"{run_id}_") as tmp:
            path = Path(tmp) / "pred.pkl"
            frame[["score"]].to_pickle(path)
            expected_sha256 = _sha256_file(path)
            if store.manifest_path(run_id).exists():
                manifest = store.load_manifest(run_id)
                metadata = manifest.get("metadata") if isinstance(manifest, Mapping) else None
                artifacts = manifest.get("artifacts") if isinstance(manifest, Mapping) else None
                prediction_items = [
                    item
                    for item in artifacts or []
                    if isinstance(item, Mapping) and item.get("artifact_type") == "prediction"
                ]
                if (
                    not isinstance(metadata, Mapping)
                    or metadata.get("purpose") != "N2B_NAVIGATION_ONLY"
                    or metadata.get("request_sha256") != request.request_sha256
                    or metadata.get("arm_id") != arm_id
                    or metadata.get("sealed_holdout_accessed") is not False
                    or len(prediction_items) != 1
                    or prediction_items[0].get("sha256") != expected_sha256
                ):
                    _raise(
                        "existing N2-B Prediction Store run differs from exact retry content",
                        REASON_PREDICTION_INVALID,
                        run_id=run_id,
                    )
            else:
                with path.open("rb") as stream:
                    store.write_artifacts(
                        run_key=run_id,
                        files={"prediction": ("pred.pkl", stream)},
                        metadata={
                            "purpose": "N2B_NAVIGATION_ONLY",
                            "request_sha256": request.request_sha256,
                            "arm_id": arm_id,
                            "sealed_holdout_accessed": False,
                        },
                    )
        run_ids[arm_id] = run_id
    source = ExactPredictionSource(request.prediction_store_root)
    descriptors = {arm_id: source.describe(run_id) for arm_id, run_id in run_ids.items()}
    for arm_id, descriptor in descriptors.items():
        if descriptor.date_start != request.decision_date_start.isoformat() or descriptor.date_end != request.decision_date_end.isoformat():
            _raise(
                "Prediction Store descriptor does not cover the exact N1 window",
                REASON_PREDICTION_INVALID,
                arm_id=arm_id,
                date_start=descriptor.date_start,
                date_end=descriptor.date_end,
            )
    return descriptors, run_ids


def _prediction_coverage(
    *,
    arm_id: str,
    decisions: pd.DatetimeIndex,
    pit_snapshot: FrozenPitSnapshot,
    feature_index: pd.MultiIndex,
    valid_index: pd.MultiIndex,
    score_index: pd.MultiIndex,
    invalid_mask: np.ndarray,
    feature_columns: Sequence[str],
    pit_receipt: Mapping[str, Any],
) -> pd.DataFrame:
    feature_dates = pd.to_datetime(feature_index.get_level_values("datetime")).normalize()
    valid_dates = pd.to_datetime(valid_index.get_level_values("datetime")).normalize()
    score_dates = pd.to_datetime(score_index.get_level_values("datetime")).normalize()
    invalid_by_date = pd.Series(invalid_mask.any(axis=1), index=feature_dates).groupby(level=0).sum()
    invalid_cells_by_date = pd.Series(invalid_mask.sum(axis=1), index=feature_dates).groupby(level=0).sum()
    feature_counts = pd.Series(1, index=feature_dates).groupby(level=0).sum()
    valid_counts = pd.Series(1, index=valid_dates).groupby(level=0).sum()
    score_counts = pd.Series(1, index=score_dates).groupby(level=0).sum()
    rows: list[dict[str, Any]] = []
    for decision in decisions:
        pit_count = len(_pit_members(pit_snapshot, decision.date()))
        score_count = int(score_counts.get(decision, 0))
        rows.append(
            {
                "arm_id": arm_id,
                "decision_as_of_trade_date": decision,
                "pit_member_count": pit_count,
                "feature_input_count": int(feature_counts.get(decision, 0)),
                "fully_scorable_feature_count": int(valid_counts.get(decision, 0)),
                "finite_score_count": score_count,
                "missing_feature_row_count": int(invalid_by_date.get(decision, 0)),
                "missing_feature_cell_count": int(invalid_cells_by_date.get(decision, 0)),
                "pit_or_market_absent_count": max(pit_count - score_count, 0),
                "top50_status": "COMPLETE" if score_count >= 50 else "DATA_UNAVAILABLE",
                "feature_count": len(feature_columns),
                "pit_rows_removed_total": int(pit_receipt.get("rows_removed") or 0),
            }
        )
    return pd.DataFrame(rows)


def _compare_anchor_predictions(
    *,
    arm_id: str,
    anchor: date,
    full: pd.DataFrame,
    replay: pd.DataFrame,
    isolated_end_date: bool,
    atol: float,
    minimum_spearman: float,
) -> dict[str, Any]:
    left = _flat_scores(full)
    right = _flat_scores(replay)
    common = left.merge(right, on="instrument", suffixes=("__full", "__replay"), validate="one_to_one")
    if len(common) < 50:
        _raise(
            "causality/parity anchor has fewer than 50 common scores",
            REASON_LIVE_PARITY if isolated_end_date else REASON_FUTURE_DEPENDENCY,
            arm_id=arm_id,
            anchor=anchor.isoformat(),
            common_count=len(common),
        )
    left_norm = _zscore(common["score__full"])
    right_norm = _zscore(common["score__replay"])
    max_abs_delta = float(np.max(np.abs(left_norm - right_norm)))
    spearman = float(pd.Series(left_norm).corr(pd.Series(right_norm), method="spearman"))
    top50_full = tuple(
        left.sort_values(["score", "instrument"], ascending=[False, True]).head(50)["instrument"]
    )
    top50_replay = tuple(
        right.sort_values(["score", "instrument"], ascending=[False, True]).head(50)["instrument"]
    )
    if isolated_end_date:
        passed = top50_full == top50_replay and math.isfinite(spearman) and spearman >= minimum_spearman
        reason = REASON_LIVE_PARITY
    else:
        passed = (
            set(left["instrument"]) == set(right["instrument"])
            and top50_full == top50_replay
            and max_abs_delta <= atol
        )
        reason = REASON_FUTURE_DEPENDENCY
    if not passed:
        _raise(
            "batch causality/parity anchor differs from its frozen replay",
            reason,
            arm_id=arm_id,
            anchor=anchor.isoformat(),
            full_count=len(left),
            replay_count=len(right),
            common_count=len(common),
            max_abs_normalized_score_delta=max_abs_delta,
            spearman=spearman,
            top50_exact=top50_full == top50_replay,
        )
    return {
        "arm_id": arm_id,
        "anchor_date": anchor.isoformat(),
        "mode": "ISOLATED_END_DATE_PARITY" if isolated_end_date else "PREFIX_FUTURE_POISON",
        "full_count": len(left),
        "replay_count": len(right),
        "common_count": len(common),
        "key_set_exact": set(left["instrument"]) == set(right["instrument"]),
        "top50_exact": top50_full == top50_replay,
        "max_abs_normalized_score_delta": max_abs_delta,
        "score_spearman": spearman,
        "status": "PASS",
    }


def _flat_scores(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or not isinstance(frame.index, pd.MultiIndex):
        _raise("anchor prediction frame is empty or unindexed", REASON_PREDICTION_INVALID)
    value = frame[["score"]].reset_index()
    value["instrument"] = value["instrument"].map(normalize_ts_code)
    if value["instrument"].duplicated().any() or not np.isfinite(value["score"].to_numpy(float)).all():
        _raise("anchor prediction frame has duplicate or invalid scores", REASON_PREDICTION_INVALID)
    return value[["instrument", "score"]]


def _zscore(values: pd.Series) -> np.ndarray:
    numeric = values.to_numpy(dtype=float)
    std = float(np.std(numeric, ddof=0))
    return (numeric - float(np.mean(numeric))) / std if std > 0 else np.zeros(len(numeric), dtype=float)


def _normalize_decision_dates(values: Sequence[pd.Timestamp | date]) -> pd.DatetimeIndex:
    dates = pd.DatetimeIndex(pd.to_datetime(list(values))).normalize()
    if dates.has_duplicates:
        _raise("batch decision dates contain duplicates", REASON_PREDICTION_INVALID)
    return dates.sort_values()


def _verified_workspace(arm: FrozenPackageAuditArmV1) -> Path:
    root = Path(arm.workspace_root)
    if not root.is_dir():
        _raise("frozen package workspace is missing", REASON_ASSET_INVALID, arm_id=arm.arm_id, path=str(root))
    actual: list[tuple[str, str, int]] = []
    for descriptor in arm.workspace_files:
        path = root / descriptor.relative_path
        if not path.is_file():
            _raise(
                "frozen package workspace file is missing",
                REASON_ASSET_INVALID,
                arm_id=arm.arm_id,
                relative_path=descriptor.relative_path,
            )
        digest = _sha256_file(path)
        if digest != descriptor.sha256 or path.stat().st_size != descriptor.size_bytes:
            _raise(
                "frozen package workspace file identity drifted",
                REASON_ASSET_INVALID,
                arm_id=arm.arm_id,
                relative_path=descriptor.relative_path,
            )
        actual.append((descriptor.relative_path, digest, path.stat().st_size))
    if len(actual) != len(arm.workspace_files):
        _raise("frozen package workspace descriptor count drifted", REASON_ASSET_INVALID)
    described = {item.relative_path for item in arm.workspace_files}
    observed = {
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file()
    }
    if observed != described:
        _raise(
            "frozen package workspace file roster drifted",
            REASON_ASSET_INVALID,
            missing=sorted(described - observed),
            extra=sorted(observed - described),
        )
    return root


def _factor_order(workspace: Path) -> list[str]:
    try:
        payload = json.loads((workspace / "factor_order.json").read_text(encoding="utf-8"))
        order = payload["factor_order"]
    except Exception as exc:
        _raise(
            "frozen package factor_order cannot be read",
            REASON_ASSET_INVALID,
            workspace=str(workspace),
            error_type=type(exc).__name__,
        )
    if not isinstance(order, list) or not order or len(order) != len(set(map(str, order))):
        _raise("frozen package factor_order is empty or duplicate", REASON_ASSET_INVALID)
    return [str(item) for item in order]


def _workspace_primary_assets(workspace: Path) -> dict[str, Any]:
    try:
        manifest = json.loads((workspace / "manifest.json").read_text(encoding="utf-8"))
    except Exception as exc:
        _raise(
            "frozen package runtime manifest cannot be read",
            REASON_ASSET_INVALID,
            workspace=str(workspace),
            error_type=type(exc).__name__,
        )
    primary = manifest.get("primary_assets")
    if not isinstance(primary, dict):
        _raise("frozen package runtime manifest has no primary_assets", REASON_ASSET_INVALID)
    return primary


def _verify_factor_group_equivalence(
    packages: Sequence[FrozenPackageAuditArmV1],
    orders: Mapping[str, Sequence[str]],
) -> None:
    grouped = _packages_by_factor_closure(packages)
    if tuple(grouped) != FACTOR_GROUP_CLOSURES:
        _raise("factor closure roster differs from the frozen contract", REASON_ASSET_INVALID)
    for closure, group in grouped.items():
        first = tuple(orders[group[0].arm_id])
        if any(tuple(orders[item.arm_id]) != first for item in group[1:]):
            _raise(
                "packages sharing one factor closure have different factor orders",
                REASON_ASSET_INVALID,
                closure_sha256=closure,
            )


def _packages_by_factor_closure(
    packages: Sequence[FrozenPackageAuditArmV1],
) -> dict[str, list[FrozenPackageAuditArmV1]]:
    grouped: dict[str, list[FrozenPackageAuditArmV1]] = {}
    for item in packages:
        grouped.setdefault(item.factor_closure_sha256, []).append(item)
    return grouped


def _validate_source_panels(source: BatchSourcePanels, *, universe: Sequence[str], decision_end: date) -> None:
    if source.daily.empty or source.static_raw.empty:
        _raise("batch source panels are empty", REASON_SOURCE_READ_FAILED)
    if source.decision_end != decision_end:
        _raise("batch source decision_end differs from request", REASON_SOURCE_READ_FAILED)
    latest_daily = pd.to_datetime(source.daily.index.get_level_values("datetime")).max().date()
    latest_static = pd.to_datetime(source.static_raw.index.get_level_values("datetime")).max().date()
    if latest_daily > decision_end or latest_static > decision_end:
        _raise(
            "batch source panel contains rows after the decision window",
            REASON_FUTURE_DEPENDENCY,
            latest_daily=latest_daily.isoformat(),
            latest_static=latest_static.isoformat(),
        )
    observed = set(source.daily.index.get_level_values("instrument").astype(str))
    if not observed.issubset(set(map(normalize_ts_code, universe))):
        _raise("batch market panel contains instruments outside the PIT union", REASON_SOURCE_READ_FAILED)


def _validate_feature_matrix(
    features: pd.DataFrame,
    factor_order: Sequence[str],
    decisions: pd.DatetimeIndex,
) -> None:
    if features.empty or not isinstance(features.index, pd.MultiIndex):
        _raise("batch factor matrix is empty or unindexed", REASON_PREDICTION_INVALID)
    if list(features.index.names) != ["datetime", "instrument"] or features.index.has_duplicates:
        _raise("batch factor matrix keys are invalid", REASON_PREDICTION_INVALID)
    if list(features.columns) != list(factor_order):
        _raise("batch factor matrix order differs from factor_order", REASON_PREDICTION_INVALID)
    observed_dates = pd.DatetimeIndex(pd.to_datetime(features.index.get_level_values("datetime")).normalize().unique())
    if not set(observed_dates).issubset(set(decisions)):
        _raise("batch factor matrix contains dates outside the requested decisions", REASON_FUTURE_DEPENDENCY)


def _canonical_panel_index(frame: pd.DataFrame, *, label: str) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        _raise(f"{label} is empty", REASON_SOURCE_READ_FAILED)
    value = frame.copy()
    if not isinstance(value.index, pd.MultiIndex) or set(value.index.names) != {"datetime", "instrument"}:
        _raise(f"{label} must use datetime/instrument MultiIndex", REASON_SOURCE_READ_FAILED)
    reset = value.reset_index()
    reset["datetime"] = pd.to_datetime(reset["datetime"]).dt.normalize()
    try:
        reset["instrument"] = reset["instrument"].map(normalize_ts_code)
    except Exception as exc:
        _raise(
            f"{label} contains an invalid instrument",
            REASON_SOURCE_READ_FAILED,
            error_type=type(exc).__name__,
        )
    value = reset.set_index(["datetime", "instrument"]).sort_index()
    if value.index.has_duplicates:
        _raise(f"{label} contains duplicate datetime/instrument rows", REASON_SOURCE_READ_FAILED)
    return value


def _slice_panel(frame: pd.DataFrame, *, cutoff: date, instruments: set[str] | None) -> pd.DataFrame:
    dates = pd.DatetimeIndex(pd.to_datetime(frame.index.get_level_values("datetime"))).normalize()
    stop = int(dates.searchsorted(pd.Timestamp(cutoff), side="right"))
    bounded = frame.iloc[:stop]
    if instruments is not None:
        normalized = {normalize_ts_code(item) for item in instruments}
        keep = bounded.index.get_level_values("instrument").astype(str).isin(normalized)
        bounded = bounded.iloc[np.flatnonzero(keep)]
    return bounded.copy()


def _pit_members(snapshot: FrozenPitSnapshot, trade_date: date) -> set[str]:
    return {
        span.ts_code
        for span in snapshot.spans
        if span.eligible_start <= trade_date <= span.eligible_end
    }


def _frame_sha256(frame: pd.DataFrame) -> str:
    value = frame.sort_index().sort_index(axis=1)
    digest = hashlib.sha256()
    digest.update(json.dumps(list(map(str, value.index.names)), separators=(",", ":")).encode())
    digest.update(json.dumps(list(map(str, value.columns)), separators=(",", ":")).encode())
    digest.update(pd.util.hash_pandas_object(value, index=True).to_numpy().tobytes())
    return digest.hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _tree_physical_size(root: Path) -> int:
    seen: set[tuple[int, int]] = set()
    total = 0
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        stat = path.stat()
        identity = (int(stat.st_dev), int(stat.st_ino))
        if identity in seen:
            continue
        seen.add(identity)
        total += int(stat.st_size)
    return total


def _raise(message: str, reason_code: str, **context: Any) -> None:
    raise AdvisoryModelFirstError(message, reason_code=reason_code, context=context)


__all__ = [
    "BatchSourcePanels",
    "PackagePredictionBatchResult",
    "StrategyPackageBatchPredictionRunner",
    "load_bounded_source_panels",
    "resolve_batch_history_start",
    "run_factor_group_batch",
]
