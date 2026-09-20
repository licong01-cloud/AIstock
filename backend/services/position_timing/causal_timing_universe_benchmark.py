"""PT-NEXT-025 frozen-model universe transport benchmark.

Offline only: no database, market network, registry, card, or service adapter.
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import Future, ProcessPoolExecutor
from datetime import date
import hashlib
import json
import math
import multiprocessing
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping
import warnings

import numpy as np
import pandas as pd

from .action_value import ActionValueError, market_features
from .action_value_data import BENCHMARK, DailyCandidate, file_reference
from .artifact_store import _exclusive_file_lock
from .causal_timing_benchmark import (
    _environment_identity,
    _joint_block_intervals,
    _model_environment,
    _performance,
    _stock_summaries,
    _symbol_frame,
)
from .causal_timing_contracts import (
    BH,
    CYCLE5,
    CYCLE5_GBDT,
    CYCLE5_RIDGE,
    E0,
    FEATURE_ORDER,
    TERMINAL_DATE,
    TEST_FIRST_EXECUTION,
    TEST_LAST_DECISION,
)
from .causal_timing_model import validate_model
from .causal_timing_replay import _prediction_arrays, replay_policy
from .contracts import canonical_sha256
from .core_tactical_timing import build_research_features
from .fundamental_screen import R8_DATASET_SHA256, R8_MANIFEST_FILE_SHA256
from .fundamental_timing_benchmark import _clean_repository_commit
from .pattern_close_cash_benchmark import check_ref, publish_frame, publish_json, read_json
from .pattern_close_cash_replay import CAPITAL
from .r8_proxy_screen import (
    MAX_TOTAL_MV,
    MIN_TOTAL_MV,
    U0,
    open_r8_proxy_sources,
    read_proxy_frames,
    screen_masks_for_symbol,
    source_audit,
)


PIPELINE_ID = "POSITION_TIMING_CAUSAL_UNIVERSE_TRANSPORT_V1"
FOLDER = "causal_timing_universe_transport_v1"
REQUEST_SCHEMA = "position_timing_causal_universe_transport_request_v1"
MANIFEST_SCHEMA = "position_timing_causal_universe_transport_files_v1"
RECEIPT_SCHEMA = "position_timing_causal_universe_transport_receipt_v1"
REPORT_SCHEMA = "position_timing_causal_universe_transport_report_v1"

U0_BRIDGE = "U0_BRIDGE_50_500B"
LARGE = "LARGE_GT_500B"
SMALL = "SMALL_LT_50B"
ALL_PIT = "ALL_PIT"
POOL_IDS = (U0_BRIDGE, LARGE, SMALL, ALL_PIT)
FORMAL_POOLS = (ALL_PIT, LARGE)
POLICY_IDS = (BH, CYCLE5, CYCLE5_RIDGE, CYCLE5_GBDT)

PARENT_BUNDLE_ID = "dfe8a85496a96f35b03d5a380f001645a999878f70461715adc73a9cde9854e6"
PARENT_MANIFEST_CANONICAL_SHA256 = "2ad1f15097f617473ea0f2b620e30922913f76ba48c98ea445ea661854bf2ec0"
MODEL_SHA256 = {
    "ridge": "aac2246286a863e02ecc985236c1ff6d52e22bf99277c996671cb5543ea4c450",
    "gbdt": "52b27c56494b4e72add859dbfd1555a72531aa96586ad2a8e5001280a9c2503a",
}

WORKER_COUNT = 8
MAX_IN_FLIGHT = 8
CHUNK_SIZE = 64
FORMAL_ENDPOINT_COUNT = 4

CONTRACT: dict[str, Any] = {
    "pipeline_id": PIPELINE_ID,
    "candidate_manifest_file_sha256": R8_MANIFEST_FILE_SHA256,
    "candidate_dataset_sha256": R8_DATASET_SHA256,
    "parent_bundle_id": PARENT_BUNDLE_ID,
    "parent_manifest_canonical_sha256": PARENT_MANIFEST_CANONICAL_SHA256,
    "model_sha256": MODEL_SHA256,
    "pools": {
        U0_BRIDGE: {"min_total_mv_wanyuan": MIN_TOTAL_MV, "max_total_mv_wanyuan": MAX_TOTAL_MV},
        LARGE: {"min_exclusive_total_mv_wanyuan": MAX_TOTAL_MV},
        SMALL: {"max_exclusive_total_mv_wanyuan": MIN_TOTAL_MV},
        ALL_PIT: {"market_cap_required": False},
    },
    "enrollment": "FIRST_PIT_AND_FEATURE_READY_SESSION_T_MINUS_1_MARKET_CAP_THEN_HOLD_COHORT",
    "policies": list(POLICY_IDS),
    "execution_view": E0,
    "capital_cny": str(CAPITAL),
    "test_first_execution": TEST_FIRST_EXECUTION.isoformat(),
    "test_last_decision": TEST_LAST_DECISION.isoformat(),
    "terminal_date": TERMINAL_DATE.isoformat(),
    "statistics": {
        "formal_pools": list(FORMAL_POOLS),
        "formal_endpoint_count": FORMAL_ENDPOINT_COUNT,
        "bootstrap_replicates": 5_000,
        "bootstrap_block_sessions": 25,
        "bootstrap_seed": 20260919,
        "familywise_method": "BONFERRONI",
        "economic_threshold_bps": 0.0,
    },
    "models_refit": False,
    "minute_execution_read": False,
    "market_impact_simulated": False,
    "selected_for_live": 0,
    "database_read": False,
    "database_write": False,
    "market_network_accessed": False,
    "runtime_action_performed": False,
    "service_process_control_performed": False,
}
CONTRACT_SHA256 = canonical_sha256(CONTRACT)


def _source_files(repository: Path) -> dict[str, dict[str, Any]]:
    names = (
        "backend/services/position_timing/causal_timing_universe_benchmark.py",
        "backend/services/position_timing/causal_timing_contracts.py",
        "backend/services/position_timing/causal_timing_model.py",
        "backend/services/position_timing/causal_timing_replay.py",
        "backend/services/position_timing/causal_timing_execution.py",
        "backend/services/position_timing/causal_timing_benchmark.py",
        "backend/services/position_timing/action_value.py",
        "backend/services/position_timing/action_value_data.py",
        "backend/services/position_timing/core_tactical_timing.py",
        "backend/services/position_timing/fundamental_timing_benchmark.py",
        "backend/services/position_timing/pattern_close_cash_replay.py",
        "backend/services/position_timing/r8_proxy_screen.py",
    )
    return {name: file_reference(repository / name) for name in names}


def _frame_sha256(frame: pd.DataFrame) -> str:
    return hashlib.sha256(
        frame.to_json(orient="table", index=False, date_format="iso", double_precision=15).encode()
    ).hexdigest()


def _parent_bundle(timing_root: Path) -> tuple[Path, dict[str, Any], dict[str, Any]]:
    root = timing_root / "research" / "causal_timing_v1" / "bundles" / PARENT_BUNDLE_ID
    manifest_ref = file_reference(root / "manifest.json")
    manifest = read_json(root / "manifest.json")
    identity = canonical_sha256({key: value for key, value in manifest.items() if key != "manifest_sha256"})
    if (
        manifest.get("request_sha256") != PARENT_BUNDLE_ID
        or manifest.get("manifest_sha256") != PARENT_MANIFEST_CANONICAL_SHA256
        or identity != PARENT_MANIFEST_CANONICAL_SHA256
    ):
        raise ActionValueError("UNIVERSE_PARENT_MANIFEST_DRIFT")
    for reference in manifest.get("files", {}).values():
        check_ref(reference)
    models_ref = manifest.get("files", {}).get("models.json")
    if not isinstance(models_ref, Mapping):
        raise ActionValueError("UNIVERSE_PARENT_MODELS_REFERENCE_MISSING")
    check_ref(models_ref)
    models = read_json(root / "models.json")
    if set(models) != set(MODEL_SHA256):
        raise ActionValueError("UNIVERSE_PARENT_MODEL_SET_DRIFT")
    for model_id, expected in MODEL_SHA256.items():
        validate_model(models[model_id])
        if models[model_id].get("model_sha256") != expected:
            raise ActionValueError("UNIVERSE_PARENT_MODEL_IDENTITY_DRIFT", model_id=model_id)
    return root, {
        "manifest": manifest_ref,
        "manifest_canonical_sha256": manifest["manifest_sha256"],
        "models": models_ref,
    }, models


def prepare(*, timing_root: Path, repository_root: Path, candidate_root: Path) -> Path:
    repository = repository_root.resolve()
    root = timing_root.resolve()
    candidate_root = candidate_root.resolve()
    if (
        not timing_root.is_absolute()
        or not repository_root.is_absolute()
        or not candidate_root.is_absolute()
        or repository != Path(__file__).resolve().parents[3]
        or root.is_relative_to(repository)
        or root.is_relative_to(candidate_root)
    ):
        raise ActionValueError("UNIVERSE_PATH_SCOPE_INVALID")
    commit = _clean_repository_commit(repository)
    sources = _source_files(repository)
    candidate = DailyCandidate.open(candidate_root)
    identity = open_r8_proxy_sources(candidate_root)
    frames = read_proxy_frames(identity)
    proxy_audit = source_audit(identity, frames)
    if candidate.calendar[0].date() != date(2018, 8, 1) or candidate.calendar[-1].date() != TERMINAL_DATE:
        raise ActionValueError("UNIVERSE_CALENDAR_DRIFT")
    if identity.candidate.dataset_sha256 != R8_DATASET_SHA256:
        raise ActionValueError("UNIVERSE_CANDIDATE_DRIFT")
    parent_root, parent_identity, _ = _parent_bundle(root)
    request: dict[str, Any] = {
        "schema_version": REQUEST_SCHEMA,
        "contract": CONTRACT,
        "contract_sha256": CONTRACT_SHA256,
        "repository_commit": commit,
        "repository_root": repository.as_posix(),
        "source_code": sources,
        "environment": _environment_identity(),
        "model_environment": _model_environment(),
        "timing_root": root.as_posix(),
        "candidate_root": candidate.root.as_posix(),
        "candidate_manifest": identity.candidate.manifest_reference,
        "candidate_dataset_manifest_sha256": identity.candidate.dataset_sha256,
        "candidate_deployment_content_sha256": identity.candidate.manifest["deployment_content_sha256"],
        "calendar": [str(value.date()) for value in candidate.calendar],
        "symbols": list(candidate.symbols),
        "daily_source_data": candidate.references,
        "proxy_source_audit": proxy_audit,
        "parent_bundle_root": parent_root.as_posix(),
        "parent_bundle": parent_identity,
        "source_preflight_complete": True,
        "outcomes_read": False,
        "database_read": False,
        "database_write": False,
        "market_network_accessed": False,
        "runtime_action_performed": False,
        "service_process_control_performed": False,
        "research_worker_processes_used": True,
    }
    request["request_sha256"] = canonical_sha256(request)
    path = root / "research" / FOLDER / "requests" / f"{request['request_sha256']}.json"
    publish_json(path, request)
    return path


def load_request(path: Path) -> dict[str, Any]:
    request = read_json(path)
    digest = canonical_sha256({key: value for key, value in request.items() if key != "request_sha256"})
    if (
        request.get("request_sha256") != digest
        or request.get("schema_version") != REQUEST_SCHEMA
        or request.get("contract_sha256") != CONTRACT_SHA256
        or canonical_sha256(request.get("contract")) != CONTRACT_SHA256
        or request.get("source_preflight_complete") is not True
        or request.get("outcomes_read") is not False
    ):
        raise ActionValueError("UNIVERSE_REQUEST_DRIFT")
    root = Path(request["timing_root"]).resolve()
    repository = Path(request["repository_root"]).resolve()
    expected = root / "research" / FOLDER / "requests" / f"{digest}.json"
    if path.resolve() != expected or repository != Path(__file__).resolve().parents[3]:
        raise ActionValueError("UNIVERSE_REQUEST_SCOPE_DRIFT")
    if request.get("environment") != _environment_identity() or request.get("model_environment") != _model_environment():
        raise ActionValueError("UNIVERSE_ENVIRONMENT_DRIFT")
    if request.get("candidate_manifest", {}).get("sha256") != R8_MANIFEST_FILE_SHA256:
        raise ActionValueError("UNIVERSE_CANDIDATE_MANIFEST_DRIFT")
    for key in (
        "database_read",
        "database_write",
        "market_network_accessed",
        "runtime_action_performed",
        "service_process_control_performed",
    ):
        if request.get(key) is not False:
            raise ActionValueError("UNIVERSE_REQUEST_BOUNDARY_DRIFT", field=key)
    for reference in (request["candidate_manifest"], request["parent_bundle"]["manifest"], request["parent_bundle"]["models"]):
        check_ref(reference)
    for group in ("source_code", "daily_source_data"):
        for reference in request[group].values():
            check_ref(reference)
    if _source_files(repository) != request["source_code"]:
        raise ActionValueError("UNIVERSE_CODE_DRIFT")
    return request


def _first_candidate(mask: np.ndarray, first: int, last: int) -> int | None:
    values = np.flatnonzero(mask & (np.arange(len(mask)) >= first) & (np.arange(len(mask)) <= last))
    return int(values[0]) if len(values) else None


def enrollment_by_pool(
    *, bars: pd.DataFrame, daily: pd.DataFrame, bak: pd.DataFrame, ready: np.ndarray,
) -> tuple[dict[str, int | None], dict[str, dict[str, Any]]]:
    """Freeze each cohort at its first causal decision session."""
    masks, coverage, unknown = screen_masks_for_symbol(
        dates=bars.index,
        pit_active=bars.pit_active.to_numpy(bool),
        feature_ready=ready,
        daily=daily,
        bak=bak,
    )
    first = int(bars.index.get_loc(pd.Timestamp(TEST_FIRST_EXECUTION))) - 1
    last = int(bars.index.get_loc(pd.Timestamp(TEST_LAST_DECISION)))
    u0 = _first_candidate(masks[U0], first, last)
    boundary = u0 if u0 is not None else last
    u0_unknown = bool(unknown[U0][first:boundary + 1].any())
    if u0_unknown:
        u0 = None

    lagged_mv = daily.reindex(pd.DatetimeIndex(bars.index))["db_total_mv"].shift(1).to_numpy(float)
    pit = bars.pit_active.to_numpy(bool)
    ready_values = np.asarray(ready, dtype=bool)
    common = pit & ready_values
    known = np.isfinite(lagged_mv)
    masks_by_pool = {
        LARGE: common & known & (lagged_mv > MAX_TOTAL_MV),
        SMALL: common & known & (lagged_mv < MIN_TOTAL_MV),
        ALL_PIT: common,
    }
    enrollments = {U0_BRIDGE: u0}
    enrollments.update({pool: _first_candidate(mask, first, last) for pool, mask in masks_by_pool.items()})
    audits: dict[str, dict[str, Any]] = {
        U0_BRIDGE: {
            "status": "ENROLLMENT_UNKNOWN" if u0_unknown else "ENROLLED" if u0 is not None else "NOT_ENROLLED",
            "enrollment_ordinal": u0,
            "coverage": coverage[U0],
            "market_cap_unknown_before_enrollment": u0_unknown,
        }
    }
    for pool, mask in masks_by_pool.items():
        enrollment = enrollments[pool]
        audits[pool] = {
            "status": "ENROLLED" if enrollment is not None else "NOT_ENROLLED",
            "enrollment_ordinal": enrollment,
            "eligible_session_count": int(mask[first:last + 1].sum()),
            "market_cap_known_session_count": int((common & known)[first:last + 1].sum()),
            "market_cap_unknown_session_count": int((common & ~known)[first:last + 1].sum()),
        }
    return enrollments, audits


def _details_for_summary(
    *, symbol: str, pool: str, days: pd.DataFrame, fills: pd.DataFrame,
    raw_details: list[dict[str, Any]], enrollment: int,
) -> list[dict[str, Any]]:
    raw = {(item["execution_view"], item["policy_id"]): item for item in raw_details}
    details: list[dict[str, Any]] = []
    for (view, policy), group in days.groupby(["execution_view", "policy_id"], sort=False):
        selected = fills.loc[(fills.execution_view == view) & (fills.policy_id == policy)] if not fills.empty else fills
        completed = selected.loc[selected.status.eq("FILLED")] if not selected.empty else selected
        last = group.sort_values("ordinal", kind="stable").iloc[-1]
        source = raw[(view, policy)]
        details.append({
            "symbol": symbol,
            "pool_id": pool,
            "execution_view": view,
            "policy_id": policy,
            "enrollment_ordinal": enrollment,
            "fees_cny": float(completed.fee.fillna(0).sum()) if not completed.empty else 0.0,
            "turnover_ratio": float(completed.notional.fillna(0).sum() / float(CAPITAL)) if not completed.empty else 0.0,
            "terminal_status": "CASH" if float(last.virtual_units) == 0 else "RESIDUAL_POSITION_NOT_CLEARED",
            "terminal_cash_cny": float(last.cash),
            "terminal_virtual_units": float(last.virtual_units),
            "model_rejected_count": int(source["model_rejected_count"]),
            "model_unavailable_count": int(source["model_unavailable_count"]),
        })
    return details


def _symbol_task(
    symbol: str,
    bars: pd.DataFrame,
    daily: pd.DataFrame,
    bak: pd.DataFrame,
    benchmark: pd.Series,
    models: Mapping[str, Mapping[str, Any]],
    terminal_ordinal: int,
) -> tuple[str, pd.DataFrame, pd.DataFrame, dict[str, dict[str, Any]], dict[str, list[float]]]:
    adjusted, pattern, trend, ready = build_research_features(symbol, bars)
    features = market_features(bars, benchmark).loc[:, list(FEATURE_ORDER)]
    predictions = _prediction_arrays(features, models)
    enrollments, audits = enrollment_by_pool(bars=bars, daily=daily, bak=bak, ready=ready)
    stock_frames: list[pd.DataFrame] = []
    fill_frames: list[pd.DataFrame] = []
    paths: dict[str, list[float]] = {}
    start_ordinal = int(bars.index.get_loc(pd.Timestamp(TEST_FIRST_EXECUTION))) - 1
    common_ordinals = np.arange(start_ordinal, terminal_ordinal + 1)
    for pool in POOL_IDS:
        enrollment = enrollments[pool]
        if enrollment is None:
            continue
        day_frames: list[pd.DataFrame] = []
        local_fills: list[pd.DataFrame] = []
        raw_details: list[dict[str, Any]] = []
        for policy in POLICY_IDS:
            days, fills, detail = replay_policy(
                symbol=symbol,
                bars=bars,
                adjusted=adjusted,
                pattern=pattern,
                trend=trend,
                ready=ready,
                model_features=features,
                policy_id=policy,
                execution_view=E0,
                enrollment_ordinal=enrollment,
                terminal_ordinal=terminal_ordinal,
                minute_source=None,
                models=models,
                model_predictions=predictions,
            )
            days["pool_id"] = pool
            day_frames.append(days)
            if not fills.empty:
                fills["pool_id"] = pool
                local_fills.append(fills)
            raw_details.append(detail)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", FutureWarning)
            day_frame = pd.concat(day_frames, ignore_index=True)
        fill_frame = pd.concat(local_fills, ignore_index=True) if local_fills else pd.DataFrame()
        details = _details_for_summary(
            symbol=symbol,
            pool=pool,
            days=day_frame,
            fills=fill_frame,
            raw_details=raw_details,
            enrollment=enrollment,
        )
        stocks = _stock_summaries(
            day_frame,
            details,
            window_start_ordinal=int(bars.index.get_loc(pd.Timestamp(TEST_FIRST_EXECUTION))) - 1,
            terminal_ordinal=terminal_ordinal,
        )
        stocks["pool_id"] = pool
        stock_frames.append(stocks)
        for policy in POLICY_IDS:
            ordered = day_frame.loc[day_frame.policy_id.eq(policy)].set_index("ordinal").reindex(common_ordinals)
            nav = ordered.nav.to_numpy(float)
            nav[(common_ordinals <= enrollment) & ~np.isfinite(nav)] = float(CAPITAL)
            paths[f"{pool}|{policy}"] = nav.tolist()
        if not fill_frame.empty:
            fill_frames.append(fill_frame)
    return (
        symbol,
        pd.concat(stock_frames, ignore_index=True) if stock_frames else pd.DataFrame(),
        pd.concat(fill_frames, ignore_index=True) if fill_frames else pd.DataFrame(),
        audits,
        paths,
    )


def _ordered(
    inputs: Iterable[tuple[Any, ...]], *, worker_count: int,
) -> Iterator[tuple[str, pd.DataFrame, pd.DataFrame, dict[str, dict[str, Any]], dict[str, list[float]]]]:
    if worker_count == 1:
        for args in inputs:
            yield _symbol_task(*args)
        return
    executor = ProcessPoolExecutor(max_workers=worker_count, mp_context=multiprocessing.get_context("spawn"))
    pending: dict[int, tuple[str, Future[Any]]] = {}
    iterator = iter(inputs)
    submitted = consumed = 0
    exhausted = False
    try:
        while not exhausted or pending:
            while not exhausted and len(pending) < MAX_IN_FLIGHT:
                try:
                    args = next(iterator)
                except StopIteration:
                    exhausted = True
                    break
                pending[submitted] = (args[0], executor.submit(_symbol_task, *args))
                submitted += 1
            expected, future = pending.pop(consumed)
            result = future.result()
            if result[0] != expected:
                raise ActionValueError("UNIVERSE_WORKER_ORDER_DRIFT")
            yield result
            consumed += 1
    finally:
        for _, future in pending.values():
            future.cancel()
        executor.shutdown(wait=True, cancel_futures=True)


def _bridge(
    *, parent_root: Path, stocks: pd.DataFrame, pool_daily: pd.DataFrame, fills: pd.DataFrame,
    full_population: bool,
) -> dict[str, Any]:
    new_stocks = stocks.loc[stocks.pool_id.eq(U0_BRIDGE)].copy()
    bridge_symbols = set(new_stocks.symbol)
    old_stocks = pd.read_parquet(parent_root / "stocks.parquet")
    old_stocks = old_stocks.loc[
        old_stocks.execution_view.eq(E0)
        & old_stocks.policy_id.isin(POLICY_IDS)
        & old_stocks.symbol.isin(bridge_symbols)
    ].copy()
    keys = ["symbol", "execution_view", "policy_id"]
    bridge_columns = [
        "terminal_nav_cny", "max_drawdown", "fees_cny", "turnover_ratio",
        "average_exposure", "conditional_exposure",
    ]
    diagnostic_columns = ["model_rejected_count", "model_unavailable_count"]
    columns = bridge_columns + diagnostic_columns
    left = new_stocks.loc[:, keys + columns].sort_values(keys, kind="stable").reset_index(drop=True)
    right = old_stocks.loc[:, keys + columns].sort_values(keys, kind="stable").reset_index(drop=True)
    stock_keys_equal = left.loc[:, keys].equals(right.loc[:, keys])
    stock_max_abs = {
        column: float(np.max(np.abs(left[column].to_numpy(float) - right[column].to_numpy(float))))
        if len(left) == len(right) and len(left) else math.inf
        for column in columns
    }

    pool_keys_equal = True
    pool_max_abs_nav = 0.0
    if full_population:
        new_pool = pool_daily.loc[pool_daily.pool_id.eq(U0_BRIDGE)].copy()
        old_pool = pd.read_parquet(parent_root / "pool_daily.parquet")
        old_pool = old_pool.loc[
            old_pool.execution_view.eq(E0)
            & old_pool.pool_id.eq("stock_universe")
            & old_pool.policy_id.isin(POLICY_IDS)
        ].copy()
        pool_keys = ["execution_view", "policy_id", "ordinal"]
        left_pool = new_pool.sort_values(pool_keys, kind="stable").reset_index(drop=True)
        right_pool = old_pool.sort_values(pool_keys, kind="stable").reset_index(drop=True)
        pool_keys_equal = left_pool.loc[:, pool_keys].equals(right_pool.loc[:, pool_keys])
        pool_max_abs_nav = (
            float(np.max(np.abs(left_pool.nav.to_numpy(float) - right_pool.nav.to_numpy(float))))
            if len(left_pool) == len(right_pool) and len(left_pool) else math.inf
        )

    new_fills = fills.loc[fills.pool_id.eq(U0_BRIDGE)].drop(columns="pool_id").copy()
    old_fills = pd.read_parquet(parent_root / "fills.parquet")
    old_fills = old_fills.loc[
        old_fills.execution_view.eq(E0)
        & old_fills.policy_id.isin(POLICY_IDS)
        & old_fills.symbol.isin(bridge_symbols)
    ].copy()
    common = sorted(set(new_fills) & set(old_fills))
    fill_keys = [name for name in ("symbol", "policy_id", "execution_ordinal", "authority", "side", "status") if name in common]
    left_fills = new_fills.loc[:, common].sort_values(fill_keys, kind="stable").reset_index(drop=True)
    right_fills = old_fills.loc[:, common].sort_values(fill_keys, kind="stable").reset_index(drop=True)
    fills_equal = _frame_sha256(left_fills) == _frame_sha256(right_fills)
    status = (
        "EXACT"
        if stock_keys_equal
        and pool_keys_equal
        and fills_equal
        and all(stock_max_abs[column] == 0.0 for column in bridge_columns)
        and pool_max_abs_nav == 0.0
        else "DRIFT"
    )
    result = {
        "status": status,
        "new_stock_rows": len(left),
        "parent_stock_rows": len(right),
        "stock_keys_equal": stock_keys_equal,
        "stock_max_abs_difference": stock_max_abs,
        "diagnostic_difference_expected": {
            "fields": diagnostic_columns,
            "reason": "PARENT_RECONSTRUCTED_REJECTIONS_FROM_FILL_ROWS_NEW_RUN_USES_REPLAY_STATE",
        },
        "pool_path_exact_required": full_population,
        "pool_keys_equal": pool_keys_equal,
        "pool_max_abs_nav_difference_cny": pool_max_abs_nav,
        "new_fill_rows": len(left_fills),
        "parent_fill_rows": len(right_fills),
        "fills_canonical_equal": fills_equal,
    }
    return {**result, "bridge_sha256": canonical_sha256(result)}


def _summary(stocks: pd.DataFrame, pool_daily: pd.DataFrame) -> dict[str, Any]:
    pool_summaries: list[dict[str, Any]] = []
    for (pool, policy), group in pool_daily.groupby(["pool_id", "policy_id"], sort=False):
        ordered = group.sort_values("ordinal", kind="stable")
        metrics = _performance(ordered.nav.to_numpy(float))
        members = stocks.loc[
            stocks.pool_id.eq(pool) & stocks.policy_id.eq(policy) & stocks.status.eq("COMPLETE")
        ]
        pool_summaries.append({
            "pool_id": pool,
            "policy_id": policy,
            "account_count": int(ordered.expected_account_count.max()),
            "unknown_account_count": int(ordered.unknown_account_count.max()),
            **metrics,
            "per_stock_terminal_return_median": float((members.terminal_nav_cny / float(CAPITAL) - 1).median()),
            "per_stock_max_drawdown_median": float(members.max_drawdown.median()),
            "joint_success_fraction": float(members.joint_success.mean()),
            "average_exposure_mean": float(members.average_exposure.mean()),
            "conditional_exposure_mean": float(members.conditional_exposure.mean()),
            "turnover_ratio_mean": float(members.turnover_ratio.mean()),
            "fees_cny_mean": float(members.fees_cny.mean()),
            "model_rejected_count": int(members.model_rejected_count.sum()),
            "model_unavailable_count": int(members.model_unavailable_count.sum()),
        })

    formal: list[dict[str, Any]] = []
    for pool in FORMAL_POOLS:
        candidate = pool_daily.loc[
            pool_daily.pool_id.eq(pool) & pool_daily.policy_id.eq(CYCLE5_GBDT)
        ].sort_values("ordinal", kind="stable")
        baseline = pool_daily.loc[
            pool_daily.pool_id.eq(pool) & pool_daily.policy_id.eq(BH)
        ].sort_values("ordinal", kind="stable")
        merged = candidate.merge(baseline, on="ordinal", suffixes=("_candidate", "_baseline"), validate="one_to_one")
        if merged.empty or int(merged.unknown_account_count_candidate.max()) > 0 or int(merged.unknown_account_count_baseline.max()) > 0:
            formal.append({"pool_id": pool, "effect_evidence": "INCOMPLETE_VALUATION"})
            continue
        candidate_nav = merged.nav_candidate.to_numpy(float)
        baseline_nav = merged.nav_baseline.to_numpy(float)
        nominal = _joint_block_intervals(candidate_nav, baseline_nav, alpha=0.05)
        adjusted = _joint_block_intervals(candidate_nav, baseline_nav, alpha=0.05 / FORMAL_ENDPOINT_COUNT)
        endpoint = {
            name: "SUPPORTED" if interval["lower"] > 0 else "NEGATIVE" if interval["upper"] < 0 else "INCONCLUSIVE"
            for name, interval in adjusted.items()
        }
        effect = (
            "JOINT_SUPPORTED_EXPLORATORY"
            if all(value == "SUPPORTED" for value in endpoint.values())
            else "JOINT_NEGATIVE"
            if all(value == "NEGATIVE" for value in endpoint.values())
            else "INCONCLUSIVE"
        )
        c_metrics = _performance(candidate_nav)
        b_metrics = _performance(baseline_nav)
        formal.append({
            "pool_id": pool,
            "candidate": CYCLE5_GBDT,
            "baseline": BH,
            "terminal_excess_bps": float((candidate_nav[-1] / candidate_nav[0] - baseline_nav[-1] / baseline_nav[0]) * 10_000),
            "mdd_improvement_bps": float((c_metrics["max_drawdown"] - b_metrics["max_drawdown"]) * 10_000),
            "nominal_intervals": nominal,
            "familywise_adjusted_intervals": adjusted,
            "endpoint_evidence": endpoint,
            "effect_evidence": effect,
        })
    return {
        "schema_version": REPORT_SCHEMA,
        "result_class": "EXPLORATORY_UNIVERSE_TRANSPORT",
        "selected_for_live": 0,
        "pool_summaries": pool_summaries,
        "formal_comparisons": formal,
        "claims": {
            "live_alpha_supported": False,
            "sealed_holdout": False,
            "market_impact_simulated": False,
            "minute_fill_proven": False,
        },
    }


def _capacity(fills: pd.DataFrame) -> dict[str, Any]:
    filled = fills.loc[fills.status.eq("FILLED") & fills.notional.notna()].copy() if not fills.empty else fills
    values = pd.to_numeric(filled.notional, errors="coerce").dropna().to_numpy(float) if not filled.empty else np.array([])
    return {
        "status": "CAPACITY_NOT_EVALUATED_NO_AUTHORITATIVE_TURNOVER_NOTIONAL",
        "market_impact_simulated": False,
        "filled_parent_order_count": len(values),
        "parent_order_notional_cny": {
            "min": float(np.min(values)) if len(values) else None,
            "median": float(np.median(values)) if len(values) else None,
            "p95": float(np.quantile(values, 0.95)) if len(values) else None,
            "max": float(np.max(values)) if len(values) else None,
        },
    }


def _seal(root: Path, names: list[str], *, request_sha256: str) -> dict[str, Any]:
    payload = {
        "schema_version": MANIFEST_SCHEMA,
        "request_sha256": request_sha256,
        "files": {name: file_reference(root / name) for name in names},
    }
    payload["manifest_sha256"] = canonical_sha256(payload)
    publish_json(root / "manifest.json", payload)
    return payload


def inspect(root: Path, *, request_sha256: str | None = None) -> dict[str, Any]:
    manifest = read_json(root / "manifest.json")
    identity = canonical_sha256({key: value for key, value in manifest.items() if key != "manifest_sha256"})
    if manifest.get("schema_version") != MANIFEST_SCHEMA or manifest.get("manifest_sha256") != identity:
        raise ActionValueError("UNIVERSE_MANIFEST_DRIFT")
    if request_sha256 is not None and manifest.get("request_sha256") != request_sha256:
        raise ActionValueError("UNIVERSE_MANIFEST_REQUEST_DRIFT")
    if set(item.name for item in root.iterdir()) != set(manifest["files"]) | {"manifest.json"}:
        raise ActionValueError("UNIVERSE_BUNDLE_MEMBER_DRIFT")
    for reference in manifest["files"].values():
        check_ref(reference)
    return {
        "status": "VERIFIED",
        "bundle": root.resolve().as_posix(),
        "request_sha256": manifest["request_sha256"],
        "manifest_sha256": manifest["manifest_sha256"],
    }


def run(path: Path, *, worker_count: int = WORKER_COUNT, symbol_limit: int | None = None) -> dict[str, Any]:
    request = load_request(path)
    digest = request["request_sha256"]
    root = Path(request["timing_root"]) / "research" / FOLDER
    suffix = f"-pilot{symbol_limit}" if symbol_limit is not None else ""
    bundle = root / "bundles" / f"{digest}{suffix}"
    with _exclusive_file_lock(root / "locks" / f"{digest}{suffix}.lock"):
        if (bundle / "manifest.json").exists():
            return {**inspect(bundle, request_sha256=digest), "status": "ALREADY_MATERIALIZED"}
        candidate = DailyCandidate.open(Path(request["candidate_root"]))
        identity = open_r8_proxy_sources(candidate.root)
        frames = read_proxy_frames(identity)
        if list(candidate.symbols) != request["symbols"] or [str(value.date()) for value in candidate.calendar] != request["calendar"]:
            raise ActionValueError("UNIVERSE_POPULATION_DRIFT")
        if source_audit(identity, frames) != request["proxy_source_audit"]:
            raise ActionValueError("UNIVERSE_PROXY_SOURCE_DRIFT")
        parent_root, parent_identity, models = _parent_bundle(Path(request["timing_root"]))
        if parent_root.as_posix() != request["parent_bundle_root"] or parent_identity != request["parent_bundle"]:
            raise ActionValueError("UNIVERSE_PARENT_BUNDLE_DRIFT")
        print(json.dumps({"stage": "RUN_AFTER_FULL_SOURCE_PREFLIGHT", "outcomes_read": True}), flush=True)
        symbols = candidate.symbols[:symbol_limit] if symbol_limit is not None else candidate.symbols
        benchmark = candidate.bars(BENCHMARK).close
        terminal = int(candidate.calendar.get_loc(pd.Timestamp(TERMINAL_DATE)))
        start = int(candidate.calendar.get_loc(pd.Timestamp(TEST_FIRST_EXECUTION))) - 1
        common_ordinals = np.arange(start, terminal + 1)

        def inputs() -> Iterator[tuple[Any, ...]]:
            for symbol in symbols:
                yield (
                    symbol,
                    candidate.bars(symbol),
                    _symbol_frame(frames.daily, symbol),
                    _symbol_frame(frames.bak, symbol),
                    benchmark,
                    models,
                    terminal,
                )

        totals: dict[tuple[str, str], np.ndarray] = {}
        counts: dict[tuple[str, str], np.ndarray] = {}
        expected: Counter[str] = Counter()
        enrollment_counts: dict[str, Counter[str]] = {pool: Counter() for pool in POOL_IDS}
        stock_frames: list[pd.DataFrame] = []
        fill_frames: list[pd.DataFrame] = []
        enrollment_audits: list[dict[str, Any]] = []
        chunks: list[dict[str, Any]] = []
        chunk_items: list[dict[str, Any]] = []
        for completed, (symbol, symbol_stocks, symbol_fills, audits, paths) in enumerate(
            _ordered(inputs(), worker_count=worker_count), start=1
        ):
            enrollment_audits.append({"symbol": symbol, "pools": audits})
            for pool, audit in audits.items():
                enrollment_counts[pool][audit["status"]] += 1
            if not symbol_stocks.empty:
                stock_frames.append(symbol_stocks)
                for pool in symbol_stocks.pool_id.unique():
                    expected[pool] += 1
                    for policy in POLICY_IDS:
                        rows = symbol_stocks.loc[
                            symbol_stocks.pool_id.eq(pool) & symbol_stocks.policy_id.eq(policy)
                        ]
                        if len(rows) != 1:
                            raise ActionValueError("UNIVERSE_STOCK_SUMMARY_INCOMPLETE", symbol=symbol, pool=pool, policy=policy)
                    for policy in POLICY_IDS:
                        key = (pool, policy)
                        nav = np.asarray(paths[f"{pool}|{policy}"], dtype=float)
                        totals.setdefault(key, np.zeros(len(common_ordinals)))[np.isfinite(nav)] += nav[np.isfinite(nav)]
                        counts.setdefault(key, np.zeros(len(common_ordinals), dtype=int))[:] += np.isfinite(nav)
            if not symbol_fills.empty:
                fill_frames.append(symbol_fills)
            chunk_items.append({
                "symbol": symbol,
                "stocks_sha256": _frame_sha256(symbol_stocks),
                "fills_sha256": _frame_sha256(symbol_fills),
                "enrollment_audit_sha256": canonical_sha256(audits),
            })
            if len(chunk_items) == CHUNK_SIZE or completed == len(symbols):
                payload = {
                    "chunk_index": len(chunks),
                    "symbols": [item["symbol"] for item in chunk_items],
                    "items": chunk_items,
                }
                chunks.append({**payload, "chunk_sha256": canonical_sha256(payload)})
                chunk_items = []
            if completed % CHUNK_SIZE == 0 or completed == len(symbols):
                print(json.dumps({"stage": "REPLAY", "symbols_complete": completed, "total": len(symbols)}), flush=True)

        stocks = pd.concat(stock_frames, ignore_index=True) if stock_frames else pd.DataFrame()
        fills = pd.concat(fill_frames, ignore_index=True) if fill_frames else pd.DataFrame()
        pool_rows: list[dict[str, Any]] = []
        for (pool, policy), values in totals.items():
            for index, ordinal in enumerate(common_ordinals):
                known = int(counts[(pool, policy)][index])
                missing = int(expected[pool]) - known
                pool_rows.append({
                    "execution_view": E0,
                    "policy_id": policy,
                    "pool_id": pool,
                    "ordinal": int(ordinal),
                    "valuation_date": str(candidate.calendar[ordinal].date()),
                    "nav": values[index] if missing == 0 else math.nan,
                    "known_nav_cny": values[index],
                    "account_count": known,
                    "expected_account_count": int(expected[pool]),
                    "unknown_account_count": missing,
                })
        pool_daily = pd.DataFrame(pool_rows)
        bridge = _bridge(
            parent_root=parent_root,
            stocks=stocks,
            pool_daily=pool_daily,
            fills=fills,
            full_population=symbol_limit is None,
        )
        if bridge["status"] != "EXACT":
            raise ActionValueError("UNIVERSE_U0_BRIDGE_DRIFT", audit=bridge)
        report = _summary(stocks, pool_daily)
        report.update({
            "request_sha256": digest,
            "full_population": symbol_limit is None,
            "symbol_limit": symbol_limit,
            "enrollment_counts": {pool: dict(value) for pool, value in enrollment_counts.items()},
            "u0_bridge": bridge,
            "capacity": _capacity(fills),
            "historical_trial_context": {
                "parent_result_class": "EXPLORATORY_HYPOTHESIS_GENERATED",
                "parent_selected_for_live": 0,
                "parent_formal_endpoint_count": 8,
                "current_formal_endpoint_count": FORMAL_ENDPOINT_COUNT,
            },
        })
        trial_spec = {
            "schema_version": "position_timing_causal_universe_transport_trial_spec_v1",
            "request_sha256": digest,
            "contract": CONTRACT,
            "contract_sha256": CONTRACT_SHA256,
            "models_refit": False,
            "formal_family_size": FORMAL_ENDPOINT_COUNT,
            "diagnostic_pools": [U0_BRIDGE, SMALL],
            "runtime_parameter_selection_from_outcomes": False,
        }
        trial_spec["trial_spec_sha256"] = canonical_sha256(trial_spec)
        source_receipt = {
            "schema_version": "position_timing_causal_universe_transport_source_audit_v1",
            "proxy": request["proxy_source_audit"],
            "parent_bundle": request["parent_bundle"],
            "parent_model_sha256": MODEL_SHA256,
            "source_preflight_complete": True,
            "outcomes_read_stage": "RUN_AFTER_FULL_SOURCE_PREFLIGHT",
        }
        source_receipt["source_audit_sha256"] = canonical_sha256(source_receipt)
        capacity = _capacity(fills)
        publish_json(bundle / "request.json", request)
        publish_json(bundle / "source_audit.json", source_receipt)
        publish_json(bundle / "models_reference.json", {"parent": request["parent_bundle"], "model_sha256": MODEL_SHA256})
        publish_json(bundle / "trial_spec.json", trial_spec)
        publish_json(bundle / "bridge_audit.json", bridge)
        publish_json(bundle / "capacity_diagnostics.json", capacity)
        publish_json(bundle / "chunk_manifests.json", chunks)
        publish_json(bundle / "enrollment_audit.json", enrollment_audits)
        publish_json(bundle / "report.json", report)
        publish_frame(bundle / "stocks.parquet", stocks)
        publish_frame(bundle / "pool_daily.parquet", pool_daily)
        publish_frame(bundle / "fills.parquet", fills)
        receipt = {
            "schema_version": RECEIPT_SCHEMA,
            "request_sha256": digest,
            "symbol_count": len(symbols),
            "full_population": symbol_limit is None,
            "chunk_count": len(chunks),
            "chunk_symbol_count": sum(len(item["symbols"]) for item in chunks),
            "enrollment_counts": {pool: dict(value) for pool, value in enrollment_counts.items()},
            "u0_bridge_status": bridge["status"],
            "source_preflight_complete": True,
            "outcomes_read_stage": "RUN_AFTER_FULL_SOURCE_PREFLIGHT",
            "result_class": "EXPLORATORY_UNIVERSE_TRANSPORT",
            "selected_for_live": 0,
            "models_refit": False,
            "minute_execution_read": False,
            "market_impact_simulated": False,
            "database_read": False,
            "database_write": False,
            "market_network_accessed": False,
            "runtime_action_performed": False,
            "service_process_control_performed": False,
            "research_worker_processes_used": worker_count > 1,
        }
        receipt["receipt_sha256"] = canonical_sha256(receipt)
        publish_json(bundle / "receipt.json", receipt)
        names = [
            "request.json", "source_audit.json", "models_reference.json", "trial_spec.json",
            "bridge_audit.json", "capacity_diagnostics.json", "chunk_manifests.json",
            "enrollment_audit.json", "report.json", "stocks.parquet", "pool_daily.parquet",
            "fills.parquet", "receipt.json",
        ]
        _seal(bundle, names, request_sha256=digest)
        return inspect(bundle, request_sha256=digest)


def verify_parallel(path: Path, *, size: int = 16) -> dict[str, Any]:
    request = load_request(path)
    candidate = DailyCandidate.open(Path(request["candidate_root"]))
    identity = open_r8_proxy_sources(candidate.root)
    frames = read_proxy_frames(identity)
    _, _, models = _parent_bundle(Path(request["timing_root"]))
    benchmark = candidate.bars(BENCHMARK).close
    terminal = int(candidate.calendar.get_loc(pd.Timestamp(TERMINAL_DATE)))
    symbols = candidate.symbols[:size]

    def materialize() -> list[tuple[Any, ...]]:
        return [(
            symbol,
            candidate.bars(symbol),
            _symbol_frame(frames.daily, symbol),
            _symbol_frame(frames.bak, symbol),
            benchmark,
            models,
            terminal,
        ) for symbol in symbols]

    def digest(result: tuple[str, pd.DataFrame, pd.DataFrame, dict[str, dict[str, Any]], dict[str, list[float]]]) -> str:
        return canonical_sha256({
            "symbol": result[0],
            "stocks": _frame_sha256(result[1]),
            "fills": _frame_sha256(result[2]),
            "audits": result[3],
            "paths": result[4],
        })

    sequential = {value[0]: digest(value) for value in _ordered(materialize(), worker_count=1)}
    parallel = {value[0]: digest(value) for value in _ordered(materialize(), worker_count=WORKER_COUNT)}
    if sequential != parallel:
        raise ActionValueError("UNIVERSE_PARALLEL_RESULT_DRIFT")
    value = {
        "status": "EXACT",
        "size": size,
        "request_sha256": request["request_sha256"],
        "symbols": list(symbols),
        "result_sha256_by_symbol": sequential,
        "artifact_written": False,
    }
    return {**value, "audit_sha256": canonical_sha256(value)}


def main() -> None:
    parser = argparse.ArgumentParser(__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare_parser = commands.add_parser("prepare")
    prepare_parser.add_argument("--timing-root", type=Path, required=True)
    prepare_parser.add_argument("--repository-root", type=Path, required=True)
    prepare_parser.add_argument("--candidate-root", type=Path, required=True)
    run_parser = commands.add_parser("run")
    run_parser.add_argument("--request", type=Path, required=True)
    run_parser.add_argument("--worker-count", type=int, default=WORKER_COUNT)
    run_parser.add_argument("--symbol-limit", type=int)
    inspect_parser = commands.add_parser("inspect")
    inspect_parser.add_argument("--bundle", type=Path, required=True)
    verify_parser = commands.add_parser("verify-parallel")
    verify_parser.add_argument("--request", type=Path, required=True)
    verify_parser.add_argument("--size", type=int, default=16)
    args = parser.parse_args()
    if args.command == "prepare":
        print(prepare(timing_root=args.timing_root, repository_root=args.repository_root, candidate_root=args.candidate_root))
    elif args.command == "run":
        print(json.dumps(run(args.request, worker_count=args.worker_count, symbol_limit=args.symbol_limit), ensure_ascii=False))
    elif args.command == "verify-parallel":
        print(json.dumps(verify_parallel(args.request, size=args.size), ensure_ascii=False))
    else:
        print(json.dumps(inspect(args.bundle), ensure_ascii=False))


if __name__ == "__main__":
    main()
