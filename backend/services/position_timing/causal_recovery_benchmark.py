"""PT-NEXT-026 small-cap recovery-value benchmark.

Offline only: no database, market network, registry, card, or service adapter.
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import Future, ProcessPoolExecutor
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
import hashlib
import json
import math
import multiprocessing
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping

import numpy as np
import pandas as pd

from .action_value import ActionValueError, market_features
from .action_value_data import BENCHMARK, DailyCandidate, file_reference
from .artifact_store import _exclusive_file_lock
from .causal_recovery_model import (
    FEATURE_ORDER as RECOVERY_FEATURE_ORDER,
    FEATURE_SPEC_SHA256,
    LABEL,
    fit_gbdt,
    fit_ridge,
    predict as predict_recovery,
    validate_model as validate_recovery_model,
)
from .causal_timing_benchmark import (
    _environment_identity,
    _model_environment,
    _performance,
    _symbol_frame,
)
from .causal_timing_contracts import (
    E0,
    FEATURE_ORDER as MARKET_FEATURE_ORDER,
    TERMINAL_DATE,
    TEST_FIRST_EXECUTION,
    TEST_LAST_DECISION,
    TEST_LAST_EXECUTION,
)
from .causal_timing_execution import daily_quote, execute_quote
from .causal_timing_model import predict as predict_trim, validate_model as validate_trim_model
from .causal_timing_replay import ReplayState, _cycle_candidate, _valuation
from .contracts import canonical_sha256
from .core_tactical_timing import S1, CorePolicyState, _floor_gap, _reset_anchor
from .fundamental_screen import R8_DATASET_SHA256, R8_MANIFEST_FILE_SHA256
from .fundamental_timing_benchmark import _clean_repository_commit
from .pattern_close_cash_benchmark import check_ref, publish_frame, publish_json, read_json
from .pattern_close_cash_replay import Account, ZERO, dec, positive
from .r8_proxy_screen import MIN_TOTAL_MV, open_r8_proxy_sources, read_proxy_frames, source_audit


PIPELINE_ID = "POSITION_TIMING_SMALLCAP_RECOVERY_VALUE_V1"
FOLDER = "causal_recovery_value_v1"
REQUEST_SCHEMA = "position_timing_smallcap_recovery_request_v1"
MANIFEST_SCHEMA = "position_timing_smallcap_recovery_files_v1"
RECEIPT_SCHEMA = "position_timing_smallcap_recovery_receipt_v1"
REPORT_SCHEMA = "position_timing_smallcap_recovery_report_v1"

PARENT_BUNDLE_ID = "dfe8a85496a96f35b03d5a380f001645a999878f70461715adc73a9cde9854e6"
PARENT_MANIFEST_CANONICAL_SHA256 = "2ad1f15097f617473ea0f2b620e30922913f76ba48c98ea445ea661854bf2ec0"
PARENT_GBDT_SHA256 = "52b27c56494b4e72add859dbfd1555a72531aa96586ad2a8e5001280a9c2503a"

CAPITAL = Decimal("5000000")
TACTICAL_FRACTION = Decimal("0.20")
WAIT_SESSIONS = 5
MAX_WAIT_SESSIONS = 20
LABEL_HORIZON_SESSIONS = 21
BH = "BH500"
PARENT = "PARENT_FIXED5_GBDT_V1"
RIDGE = "RECOVERY_RIDGE_V1"
GBDT = "RECOVERY_GBDT_V1"
POLICY_IDS = (BH, PARENT, RIDGE, GBDT)
RECOVERY_MODEL_BY_POLICY = {RIDGE: "ridge", GBDT: "gbdt"}

WORKER_COUNT = 8
MAX_IN_FLIGHT = 8
CHUNK_SIZE = 64
BOOTSTRAP_REPLICATES = 5_000
BOOTSTRAP_BLOCK = 25
BOOTSTRAP_SEED = 20260920

CONTRACT: dict[str, Any] = {
    "pipeline_id": PIPELINE_ID,
    "candidate_manifest_file_sha256": R8_MANIFEST_FILE_SHA256,
    "candidate_dataset_sha256": R8_DATASET_SHA256,
    "parent_bundle_id": PARENT_BUNDLE_ID,
    "parent_manifest_canonical_sha256": PARENT_MANIFEST_CANONICAL_SHA256,
    "parent_gbdt_sha256": PARENT_GBDT_SHA256,
    "population": {
        "id": "SMALL_LT_50B",
        "t_minus_1_total_mv_wanyuan_max_exclusive": MIN_TOTAL_MV,
        "unknown": "EXCLUDE_NO_FILL",
        "cohort_after_first_enrollment": True,
    },
    "capital_cny": str(CAPITAL),
    "account": "INDEPENDENT_CASH_NO_LEVERAGE_NO_INJECTION",
    "policies": list(POLICY_IDS),
    "sell_side": "PARENT_GBDT_ACCEPTED_20_PERCENT_TRIM",
    "recovery": {
        "label": LABEL,
        "label_event_source": "CAUSAL_RULE_CANDIDATE_FILLED_BEFORE_PARENT_MODEL_GATE",
        "wait_sessions": WAIT_SESSIONS,
        "max_wait_sessions": MAX_WAIT_SESSIONS,
        "label_horizon_sessions": LABEL_HORIZON_SESSIONS,
        "decision_threshold_bps": 0.0,
        "oracle_policy_access": False,
    },
    "feature_order": list(RECOVERY_FEATURE_ORDER),
    "feature_spec_sha256": FEATURE_SPEC_SHA256,
    "execution_view": E0,
    "test_first_execution": TEST_FIRST_EXECUTION.isoformat(),
    "test_last_decision": TEST_LAST_DECISION.isoformat(),
    "test_last_execution": TEST_LAST_EXECUTION.isoformat(),
    "terminal_date": TERMINAL_DATE.isoformat(),
    "statistics": {
        "formal_comparison_count": 2,
        "endpoint_count": 2,
        "family_size": 4,
        "bootstrap_replicates": BOOTSTRAP_REPLICATES,
        "bootstrap_block_sessions": BOOTSTRAP_BLOCK,
        "seed": BOOTSTRAP_SEED,
        "familywise_method": "BONFERRONI",
        "economic_threshold_bps": 0.0,
    },
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
        "backend/services/position_timing/causal_recovery_benchmark.py",
        "backend/services/position_timing/causal_recovery_model.py",
        "backend/services/position_timing/causal_timing_contracts.py",
        "backend/services/position_timing/causal_timing_model.py",
        "backend/services/position_timing/causal_timing_replay.py",
        "backend/services/position_timing/causal_timing_execution.py",
        "backend/services/position_timing/action_value.py",
        "backend/services/position_timing/action_value_data.py",
        "backend/services/position_timing/core_tactical_timing.py",
        "backend/services/position_timing/pattern_close_cash_replay.py",
        "backend/services/position_timing/r8_proxy_screen.py",
    )
    return {name: file_reference(repository / name) for name in names}


def _frame_sha256(frame: pd.DataFrame) -> str:
    return hashlib.sha256(
        frame.sort_index(axis=1).sort_values(list(frame.columns), kind="stable").to_json(
            orient="records", date_format="iso", double_precision=15,
        ).encode("utf-8")
    ).hexdigest() if not frame.empty else hashlib.sha256(b"[]").hexdigest()


def _parent_bundle(timing_root: Path) -> tuple[Path, dict[str, Any], dict[str, Any]]:
    root = timing_root / "research" / "causal_timing_v1" / "bundles" / PARENT_BUNDLE_ID
    manifest = read_json(root / "manifest.json")
    canonical = canonical_sha256({key: value for key, value in manifest.items() if key != "manifest_sha256"})
    if manifest.get("manifest_sha256") != canonical or canonical != PARENT_MANIFEST_CANONICAL_SHA256:
        raise ActionValueError("RECOVERY_PARENT_MANIFEST_DRIFT")
    for reference in manifest["files"].values():
        check_ref(reference)
    models_ref = manifest["files"].get("models.json")
    if models_ref is None:
        raise ActionValueError("RECOVERY_PARENT_MODELS_MISSING")
    models = read_json(root / "models.json")
    trim_model = models.get("gbdt")
    if trim_model is None:
        raise ActionValueError("RECOVERY_PARENT_GBDT_MISSING")
    validate_trim_model(trim_model)
    if trim_model.get("model_sha256") != PARENT_GBDT_SHA256:
        raise ActionValueError("RECOVERY_PARENT_GBDT_IDENTITY_DRIFT")
    return root, manifest, trim_model


def prepare(*, timing_root: Path, repository_root: Path, candidate_root: Path) -> Path:
    timing_root, repository, candidate_root = timing_root.resolve(), repository_root.resolve(), candidate_root.resolve()
    if not timing_root.is_absolute() or timing_root.is_relative_to(repository) or timing_root.is_relative_to(candidate_root):
        raise ActionValueError("RECOVERY_ROOT_ISOLATION_INVALID")
    candidate = DailyCandidate.open(candidate_root)
    identity = open_r8_proxy_sources(candidate_root)
    frames = read_proxy_frames(identity)
    if identity.candidate.dataset_sha256 != R8_DATASET_SHA256:
        raise ActionValueError("RECOVERY_CANDIDATE_IDENTITY_DRIFT")
    if candidate.calendar[0].date() != date(2018, 8, 1) or candidate.calendar[-1].date() != TERMINAL_DATE:
        raise ActionValueError("RECOVERY_CALENDAR_DRIFT")
    parent_root, parent_manifest, trim_model = _parent_bundle(timing_root)
    source_code = _source_files(repository)
    request: dict[str, Any] = {
        "schema_version": REQUEST_SCHEMA,
        "contract": CONTRACT,
        "contract_sha256": CONTRACT_SHA256,
        "repository_root": repository.as_posix(),
        "repository_commit": _clean_repository_commit(repository),
        "timing_root": timing_root.as_posix(),
        "candidate_root": candidate_root.as_posix(),
        "candidate_manifest": dict(identity.candidate.manifest_reference),
        "candidate_dataset_manifest_sha256": identity.candidate.dataset_sha256,
        "calendar": [str(value.date()) for value in candidate.calendar],
        "symbols": candidate.symbols,
        "daily_source_data": candidate.references,
        "proxy_source_audit": source_audit(identity, frames),
        "parent_bundle_root": parent_root.as_posix(),
        "parent_bundle": {
            "manifest": file_reference(parent_root / "manifest.json"),
            "manifest_canonical_sha256": parent_manifest["manifest_sha256"],
            "models": file_reference(parent_root / "models.json"),
            "gbdt_model_sha256": trim_model["model_sha256"],
        },
        "source_code": source_code,
        "environment": _environment_identity(),
        "model_environment": _model_environment(),
        "source_preflight_complete": True,
        "outcomes_read": False,
        "database_read": False,
        "database_write": False,
        "market_network_accessed": False,
        "runtime_action_performed": False,
        "service_process_control_performed": False,
    }
    request["request_sha256"] = canonical_sha256(request)
    path = timing_root / "research" / FOLDER / "requests" / f"{request['request_sha256']}.json"
    if path.exists():
        if read_json(path) != request:
            raise ActionValueError("RECOVERY_REQUEST_IDENTITY_COLLISION")
    else:
        publish_json(path, request)
    return path


def load_request(path: Path) -> dict[str, Any]:
    request = read_json(path.resolve())
    digest = request.get("request_sha256")
    if request.get("schema_version") != REQUEST_SCHEMA or canonical_sha256({k: v for k, v in request.items() if k != "request_sha256"}) != digest:
        raise ActionValueError("RECOVERY_REQUEST_DRIFT")
    if request.get("contract_sha256") != CONTRACT_SHA256 or request.get("contract") != CONTRACT:
        raise ActionValueError("RECOVERY_CONTRACT_DRIFT")
    repository = Path(request["repository_root"])
    if _clean_repository_commit(repository) != request["repository_commit"] or _source_files(repository) != request["source_code"]:
        raise ActionValueError("RECOVERY_CODE_DRIFT")
    for reference in (request["candidate_manifest"], request["parent_bundle"]["manifest"], request["parent_bundle"]["models"]):
        check_ref(reference)
    for reference in request["daily_source_data"].values():
        check_ref(reference)
    return request


def _lagged_market_cap(daily: pd.DataFrame, dates: pd.Index) -> np.ndarray:
    return daily.reindex(pd.DatetimeIndex(dates))["db_total_mv"].shift(1).to_numpy(float)


def _small_enrollment(
    *, bars: pd.DataFrame, ready: np.ndarray, lagged_mv: np.ndarray, first: int,
) -> tuple[int | None, dict[str, Any]]:
    common = bars.pit_active.to_numpy(bool) & np.asarray(ready, dtype=bool)
    known = np.isfinite(lagged_mv)
    eligible = common & known & (lagged_mv < MIN_TOTAL_MV) & (np.arange(len(bars)) >= first)
    indexes = np.flatnonzero(eligible)
    enrollment = int(indexes[0]) if len(indexes) else None
    return enrollment, {
        "status": "ENROLLED" if enrollment is not None else "NOT_ENROLLED",
        "enrollment_ordinal": enrollment,
        "eligible_session_count": int(eligible.sum()),
        "market_cap_unknown_session_count": int((common & ~known & (np.arange(len(bars)) >= first)).sum()),
    }


def _recovery_features(
    base: pd.DataFrame, lagged_mv: np.ndarray, *, decision: int, trim_execution: int,
    adjusted: pd.DataFrame, trim_adjusted_price: Decimal,
) -> dict[str, float] | None:
    if decision < trim_execution or not np.isfinite(lagged_mv[decision]) or lagged_mv[decision] < 0:
        return None
    row = base.iloc[decision]
    values = row.loc[list(MARKET_FEATURE_ORDER)].to_numpy(float)
    current = adjusted.close.iloc[decision]
    if not np.isfinite(values).all() or not positive(current) or trim_adjusted_price <= ZERO:
        return None
    payload = {name: float(row[name]) for name in MARKET_FEATURE_ORDER}
    payload.update({
        "sessions_since_trim": float(decision - trim_execution),
        "return_since_trim_bps": float((dec(current) / trim_adjusted_price - Decimal(1)) * Decimal(10000)),
        "log1p_total_mv_wanyuan": float(np.log1p(lagged_mv[decision])),
    })
    return payload


def _buyback_arm(
    *, symbol: str, bars: pd.DataFrame, adjusted: pd.DataFrame, cash: Decimal,
    decision: int, due: int, horizon: int,
) -> tuple[Decimal | None, str, int | None]:
    account = Account(cash=cash, units=ZERO, entry=None, bought_on=decision, bought_once=True)
    status, execution = "NOT_ATTEMPTED", None
    for ordinal in range(due, horizon + 1):
        fill = execute_quote(
            account, symbol=symbol,
            quote=daily_quote(bars.iloc[ordinal].to_dict(), trade_date=bars.index[ordinal].date()),
            ordinal=ordinal, side="BUY",
        )
        status = str(fill["status"])
        if status == "FILLED":
            execution = ordinal
            break
    mark = dec(adjusted.close.iloc[horizon]) if positive(adjusted.close.iloc[horizon]) else None
    return account.wealth(mark), status, execution


def recovery_labels_from_fills(
    *, symbol: str, bars: pd.DataFrame, adjusted: pd.DataFrame, base_features: pd.DataFrame,
    lagged_mv: np.ndarray, fills: pd.DataFrame,
) -> pd.DataFrame:
    if fills.empty:
        return pd.DataFrame(columns=list(("symbol", "event_id", "decision_date", "label_available_at", LABEL, *RECOVERY_FEATURE_ORDER)))
    sells = fills.loc[
        fills.side.eq("SELL") & fills.status.eq("FILLED")
        & ~fills.authority.eq("TERMINAL_LIQUIDATION")
    ].sort_values("execution_ordinal", kind="stable")
    rows: list[dict[str, Any]] = []
    for event_number, item in enumerate(sells.itertuples()):
        execution = int(item.execution_ordinal)
        cash = dec(item.notional) - dec(item.fee)
        if cash <= ZERO or not positive(adjusted.close.iloc[execution]):
            continue
        trim_price = dec(adjusted.close.iloc[execution])
        for offset in range(MAX_WAIT_SESSIONS):
            decision = execution + offset
            horizon = decision + LABEL_HORIZON_SESSIONS
            if horizon >= len(bars):
                break
            features = _recovery_features(
                base_features, lagged_mv, decision=decision, trim_execution=execution,
                adjusted=adjusted, trim_adjusted_price=trim_price,
            )
            if features is None:
                continue
            now, now_status, now_execution = _buyback_arm(
                symbol=symbol, bars=bars, adjusted=adjusted, cash=cash,
                decision=decision, due=decision + 1, horizon=horizon,
            )
            wait, wait_status, wait_execution = _buyback_arm(
                symbol=symbol, bars=bars, adjusted=adjusted, cash=cash,
                decision=decision, due=decision + WAIT_SESSIONS, horizon=horizon,
            )
            if now is None or wait is None:
                continue
            rows.append({
                "symbol": symbol,
                "event_id": f"{symbol}:{event_number}",
                "trim_execution_ordinal": execution,
                "decision_ordinal": decision,
                "decision_date": str(bars.index[decision].date()),
                "label_window_end": str(bars.index[horizon].date()),
                "label_available_at": str(bars.index[horizon].date()),
                LABEL: float((now - wait) / CAPITAL * Decimal(10000)),
                "recover_now_status": now_status,
                "recover_now_execution_ordinal": now_execution,
                "wait_status": wait_status,
                "wait_execution_ordinal": wait_execution,
                **features,
            })
    return pd.DataFrame(rows)


@dataclass
class RecoveryState:
    cycle: ReplayState
    trim_execution: int | None = None
    trim_adjusted_price: Decimal | None = None
    recovery_authorized: bool = False
    recovery_rejected: int = 0
    recovery_unavailable: int = 0
    forced_recovery: int = 0


def replay_policy(
    *, symbol: str, bars: pd.DataFrame, adjusted: pd.DataFrame, pattern: pd.DataFrame,
    trend: pd.DataFrame, ready: np.ndarray, base_features: pd.DataFrame,
    lagged_mv: np.ndarray, policy_id: str, enrollment_ordinal: int, terminal_ordinal: int,
    trim_model: Mapping[str, Any], recovery_models: Mapping[str, Mapping[str, Any]],
    accept_all_trim_candidates_for_labels: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    if policy_id not in POLICY_IDS:
        raise ActionValueError("RECOVERY_POLICY_UNKNOWN", policy_id=policy_id)
    account = Account(cash=CAPITAL)
    state = RecoveryState(ReplayState(CorePolicyState(account, Account(cash=CAPITAL))))
    raw_rows = bars.to_dict("records")
    trim_predictions = np.full(len(bars), np.nan)
    if accept_all_trim_candidates_for_labels and policy_id != PARENT:
        raise ActionValueError("RECOVERY_LABEL_REFERENCE_POLICY_INVALID")
    if policy_id != BH and not accept_all_trim_candidates_for_labels:
        finite_market = np.isfinite(
            base_features.loc[:, list(MARKET_FEATURE_ORDER)].to_numpy(float)
        ).all(axis=1)
        if finite_market.any():
            trim_predictions[finite_market] = predict_trim(
                trim_model, base_features.loc[finite_market]
            )
    rows: list[dict[str, Any]] = []
    fills: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    recovery_model = recovery_models.get(RECOVERY_MODEL_BY_POLICY.get(policy_id, ""))

    for decision in range(enrollment_ordinal, terminal_ordinal):
        target = decision + 1
        reference_value = adjusted.close.iloc[decision]
        reference = dec(reference_value) if positive(reference_value) else state.cycle.last_mark
        bar = raw_rows[target]
        factor = dec(bar["factor"]) if positive(bar.get("factor")) else ZERO
        plan: str | None = None
        authority = "HOLD"
        sell_fraction: Decimal | None = None
        r0_value: bool | None = None
        model_value: float | None = None
        model_status = "NOT_APPLICABLE"

        ordinary = bars.index[target].date() <= TEST_LAST_EXECUTION
        if not account.bought_once:
            if ordinary and bool(bars.pit_active.iloc[decision]):
                plan, authority = "BUY", "COMMON_INITIAL_ENTRY"
        elif policy_id != BH and ordinary:
            if state.trim_execution is not None and account.cash > ZERO:
                if policy_id == PARENT:
                    state.recovery_authorized |= target >= state.trim_execution + WAIT_SESSIONS
                    authority = "FIXED_5_SESSION_RECOVERY"
                else:
                    forced = target >= state.trim_execution + MAX_WAIT_SESSIONS
                    if forced:
                        state.forced_recovery += int(not state.recovery_authorized)
                        state.recovery_authorized = True
                        model_status = "FORCED_MAX_WAIT"
                    elif not state.recovery_authorized:
                        values = _recovery_features(
                            base_features, lagged_mv, decision=decision,
                            trim_execution=state.trim_execution, adjusted=adjusted,
                            trim_adjusted_price=state.trim_adjusted_price or Decimal(1),
                        )
                        if recovery_model is None or values is None:
                            state.recovery_unavailable += 1
                            model_status = "MODEL_UNAVAILABLE"
                        else:
                            model_value = float(predict_recovery(recovery_model, values)[0])
                            state.recovery_authorized = model_value > 0.0
                            model_status = "ACCEPT" if state.recovery_authorized else "REJECT"
                            state.recovery_rejected += int(not state.recovery_authorized)
                    authority = "MODEL_RECOVERY"
                if state.recovery_authorized:
                    plan = "BUY"
            elif state.trim_execution is None and bars.index[decision].date() <= TEST_LAST_DECISION:
                reason, sell_fraction, r0_value, _ = _cycle_candidate(
                    state.cycle, symbol=symbol, decision=decision, execution_factor=factor,
                    reference=reference, pattern=pattern, trend=trend, ready=ready,
                )
                if reason is not None and target + MAX_WAIT_SESSIONS <= terminal_ordinal:
                    if accept_all_trim_candidates_for_labels:
                        model_status = "LABEL_REFERENCE_CAUSAL_RULE_ACCEPT"
                        plan, authority = "SELL", reason
                    elif not np.isfinite(trim_predictions[decision]):
                        state.cycle.model_unavailable += 1
                        model_status = "TRIM_MODEL_UNAVAILABLE"
                    else:
                        model_value = float(trim_predictions[decision])
                        allowed = model_value > 0.0
                        model_status = "TRIM_ACCEPT" if allowed else "TRIM_REJECT"
                        state.cycle.model_rejected += int(not allowed)
                        if allowed:
                            plan, authority = "SELL", reason
                        else:
                            state.cycle.core.pending_risk = False
                            state.cycle.core.pending_trend = False
                            if r0_value is not None:
                                state.cycle.core.r0_edge = r0_value

        if plan is not None:
            fill = execute_quote(
                account, symbol=symbol,
                quote=daily_quote(bar, trade_date=bars.index[target].date()),
                ordinal=target, side=plan,
                budget=CAPITAL if authority == "COMMON_INITIAL_ENTRY" else None,
                sell_fraction=sell_fraction or Decimal(1),
            )
            counts[f"{plan}:{fill['status']}"] += 1
            fills.append({
                "symbol": symbol, "policy_id": policy_id, "execution_view": E0,
                "decision_ordinal": decision, "execution_ordinal": target,
                "decision_date": str(bars.index[decision].date()),
                "execution_date": str(bars.index[target].date()),
                "authority": authority, "model_prediction_bps": model_value,
                "model_status": model_status,
                "planned_sell_fraction": float(sell_fraction) if sell_fraction is not None else None,
                **fill,
            })
            if fill["status"] == "FILLED":
                if authority == "COMMON_INITIAL_ENTRY":
                    _reset_anchor(state.cycle.core, S1)
                elif plan == "SELL":
                    state.cycle.core.trim_stage = 1
                    state.cycle.core.last_trim_execution = target
                    state.cycle.core.pending_risk = False
                    state.cycle.core.pending_trend = False
                    state.trim_execution = target
                    state.trim_adjusted_price = dec(adjusted.close.iloc[target])
                    state.recovery_authorized = False
                else:
                    _reset_anchor(state.cycle.core, S1)
                    state.trim_execution = None
                    state.trim_adjusted_price = None
                    state.recovery_authorized = False
            if r0_value is not None and (plan != "SELL" or fill["status"] == "FILLED"):
                state.cycle.core.r0_edge = r0_value
        elif r0_value is not None:
            state.cycle.core.r0_edge = r0_value

        wealth, stale = _valuation(account, adjusted.close.iloc[target], state.cycle)
        rows.append({
            "symbol": symbol, "policy_id": policy_id, "execution_view": E0,
            "ordinal": target, "valuation_date": str(bars.index[target].date()),
            "nav": float(wealth) if wealth is not None else np.nan,
            "cash": float(account.cash), "virtual_units": float(account.units),
            "exposure": (
                float(account.units * state.cycle.last_mark / wealth)
                if account.units and state.cycle.last_mark is not None and wealth else 0.0
            ),
            "stale_mark": stale,
            "valuation_status": "KNOWN" if wealth is not None else "UNKNOWN",
            "trim_execution_ordinal": state.trim_execution,
            "floor_gap_units": float(_floor_gap(state.cycle.core)) if state.cycle.core.full_units_anchor else 0.0,
        })

    terminal_fill: dict[str, Any] | None = None
    if account.units > ZERO:
        terminal_fill = execute_quote(
            account, symbol=symbol,
            quote=daily_quote(raw_rows[terminal_ordinal], trade_date=bars.index[terminal_ordinal].date()),
            ordinal=terminal_ordinal, side="SELL", sell_fraction=Decimal(1),
        )
        fills.append({
            "symbol": symbol, "policy_id": policy_id, "execution_view": E0,
            "decision_ordinal": terminal_ordinal - 1, "execution_ordinal": terminal_ordinal,
            "decision_date": str(bars.index[terminal_ordinal - 1].date()),
            "execution_date": str(bars.index[terminal_ordinal].date()),
            "authority": "TERMINAL_LIQUIDATION", **terminal_fill,
        })
        wealth = account.wealth(state.cycle.last_mark)
        if rows:
            rows[-1].update({
                "nav": float(wealth) if wealth is not None else np.nan,
                "cash": float(account.cash), "virtual_units": float(account.units),
                "exposure": (
                    float(account.units * state.cycle.last_mark / wealth)
                    if account.units and state.cycle.last_mark is not None and wealth else 0.0
                ),
            })
    terminal_wealth = account.wealth(state.cycle.last_mark)
    detail = {
        "symbol": symbol, "policy_id": policy_id, "status": "REPLAYED",
        "enrollment_ordinal": enrollment_ordinal,
        "terminal_nav_cny": float(terminal_wealth) if terminal_wealth is not None else None,
        "terminal_cash_cny": float(account.cash),
        "terminal_virtual_units": float(account.units),
        "fees_cny": float(account.fees),
        "trim_model_rejected_count": state.cycle.model_rejected,
        "trim_model_unavailable_count": state.cycle.model_unavailable,
        "recovery_model_rejected_count": state.recovery_rejected,
        "recovery_model_unavailable_count": state.recovery_unavailable,
        "forced_recovery_count": state.forced_recovery,
        "counts": dict(counts),
    }
    return pd.DataFrame(rows), pd.DataFrame(fills), detail


def _symbol_inputs(
    symbol: str, bars: pd.DataFrame, daily: pd.DataFrame, benchmark: pd.Series,
) -> dict[str, Any]:
    from .core_tactical_timing import build_research_features

    adjusted, pattern, trend, ready = build_research_features(symbol, bars)
    base = market_features(bars, benchmark).loc[:, list(MARKET_FEATURE_ORDER)]
    lagged_mv = _lagged_market_cap(daily, bars.index)
    return {
        "adjusted": adjusted, "pattern": pattern, "trend": trend, "ready": ready,
        "base": base, "lagged_mv": lagged_mv,
    }


def _reference_task(
    symbol: str, bars: pd.DataFrame, daily: pd.DataFrame, benchmark: pd.Series,
    trim_model: Mapping[str, Any], terminal_ordinal: int,
) -> tuple[str, pd.DataFrame, dict[str, Any]]:
    values = _symbol_inputs(symbol, bars, daily, benchmark)
    enrollment, audit = _small_enrollment(
        bars=bars, ready=values["ready"], lagged_mv=values["lagged_mv"], first=0,
    )
    if enrollment is None:
        return symbol, pd.DataFrame(), audit
    _, fills, _ = replay_policy(
        symbol=symbol, bars=bars, adjusted=values["adjusted"], pattern=values["pattern"],
        trend=values["trend"], ready=values["ready"], base_features=values["base"],
        lagged_mv=values["lagged_mv"], policy_id=PARENT, enrollment_ordinal=enrollment,
        terminal_ordinal=terminal_ordinal, trim_model=trim_model, recovery_models={},
        accept_all_trim_candidates_for_labels=True,
    )
    labels = recovery_labels_from_fills(
        symbol=symbol, bars=bars, adjusted=values["adjusted"], base_features=values["base"],
        lagged_mv=values["lagged_mv"], fills=fills,
    )
    return symbol, labels, audit


def _replay_task(
    symbol: str, bars: pd.DataFrame, daily: pd.DataFrame, benchmark: pd.Series,
    trim_model: Mapping[str, Any], recovery_models: Mapping[str, Mapping[str, Any]],
    first_test_decision: int, terminal_ordinal: int,
) -> tuple[str, pd.DataFrame, pd.DataFrame, list[dict[str, Any]], dict[str, Any]]:
    values = _symbol_inputs(symbol, bars, daily, benchmark)
    enrollment, audit = _small_enrollment(
        bars=bars, ready=values["ready"], lagged_mv=values["lagged_mv"], first=first_test_decision,
    )
    if enrollment is None:
        return symbol, pd.DataFrame(), pd.DataFrame(), [], audit
    day_frames: list[pd.DataFrame] = []
    fill_frames: list[pd.DataFrame] = []
    details: list[dict[str, Any]] = []
    for policy in POLICY_IDS:
        days, fills, detail = replay_policy(
            symbol=symbol, bars=bars, adjusted=values["adjusted"], pattern=values["pattern"],
            trend=values["trend"], ready=values["ready"], base_features=values["base"],
            lagged_mv=values["lagged_mv"], policy_id=policy, enrollment_ordinal=enrollment,
            terminal_ordinal=terminal_ordinal, trim_model=trim_model, recovery_models=recovery_models,
        )
        day_frames.append(days)
        if not fills.empty:
            fill_frames.append(fills)
        details.append(detail)
    return (
        symbol,
        pd.concat(
            [frame.dropna(axis=1, how="all") for frame in day_frames],
            ignore_index=True,
        ),
        pd.concat(fill_frames, ignore_index=True) if fill_frames else pd.DataFrame(),
        details,
        audit,
    )


def _ordered_call(
    inputs: Iterable[tuple[Any, ...]], *, worker_count: int, function: Any,
) -> Iterator[tuple[Any, ...]]:
    if worker_count == 1:
        for args in inputs:
            yield function(*args)
        return
    executor = ProcessPoolExecutor(max_workers=worker_count, mp_context=multiprocessing.get_context("spawn"))
    pending: dict[int, tuple[str, Future[Any]]] = {}
    iterator, submitted, consumed, exhausted = iter(inputs), 0, 0, False
    try:
        while not exhausted or pending:
            while not exhausted and len(pending) < MAX_IN_FLIGHT:
                try:
                    args = next(iterator)
                except StopIteration:
                    exhausted = True
                    break
                pending[submitted] = (str(args[0]), executor.submit(function, *args))
                submitted += 1
            expected, future = pending.pop(consumed)
            result = future.result()
            if result[0] != expected:
                raise ActionValueError("RECOVERY_WORKER_ORDER_DRIFT")
            yield result
            consumed += 1
    finally:
        for _, future in pending.values():
            future.cancel()
        executor.shutdown(wait=True, cancel_futures=True)


def _stock_summaries(days: pd.DataFrame, details: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    detail_map = {item["policy_id"]: item for item in details}
    for policy, group in days.groupby("policy_id", sort=False):
        ordered = group.sort_values("ordinal", kind="stable")
        nav = ordered.nav.to_numpy(float)
        perf = _performance(nav)
        known = np.isfinite(nav)
        exposure = ordered.exposure.to_numpy(float)
        detail = detail_map[str(policy)]
        rows.append({
            "symbol": str(ordered.symbol.iloc[0]), "policy_id": str(policy),
            "terminal_nav_cny": perf.get("terminal_nav_cny"),
            "total_return": perf.get("total_return"), "max_drawdown": perf.get("max_drawdown"),
            "average_exposure": float(np.mean(exposure[known])) if known.any() else None,
            "fees_cny": detail["fees_cny"],
            "trim_model_rejected_count": detail["trim_model_rejected_count"],
            "recovery_model_rejected_count": detail["recovery_model_rejected_count"],
            "recovery_model_unavailable_count": detail["recovery_model_unavailable_count"],
            "forced_recovery_count": detail["forced_recovery_count"],
        })
    return pd.DataFrame(rows)


def _research_diagnostics(stocks: pd.DataFrame, fills: pd.DataFrame) -> dict[str, Any]:
    baseline = stocks.loc[stocks.policy_id.eq(BH)].set_index("symbol")
    paired: list[dict[str, Any]] = []
    for policy, frame in stocks.groupby("policy_id", sort=False):
        if policy == BH:
            continue
        candidate = frame.set_index("symbol")
        values = candidate.join(
            baseline[["terminal_nav_cny", "max_drawdown"]],
            rsuffix="_bh",
            how="inner",
            validate="one_to_one",
        )
        terminal_win = values.terminal_nav_cny > values.terminal_nav_cny_bh
        mdd_win = values.max_drawdown > values.max_drawdown_bh
        paired.append({
            "candidate": str(policy), "baseline": BH,
            "paired_stock_count": len(values),
            "terminal_win_rate": float(terminal_win.mean()),
            "mdd_win_rate": float(mdd_win.mean()),
            "dual_win_rate": float((terminal_win & mdd_win).mean()),
            "mean_stock_return": float(frame.total_return.mean()),
            "median_stock_return": float(frame.total_return.median()),
            "mean_stock_max_drawdown": float(frame.max_drawdown.mean()),
            "median_stock_max_drawdown": float(frame.max_drawdown.median()),
            "mean_exposure": float(frame.average_exposure.mean()),
            "mean_fees_cny": float(frame.fees_cny.mean()),
            "forced_recovery_count": int(frame.forced_recovery_count.sum()),
        })

    waits: list[dict[str, Any]] = []
    filled = fills.loc[fills.status.eq("FILLED")].sort_values(
        ["policy_id", "symbol", "execution_ordinal"], kind="stable"
    )
    for policy in (PARENT, RIDGE, GBDT):
        policy_waits: list[int] = []
        unmatched = 0
        for _, frame in filled.loc[filled.policy_id.eq(policy)].groupby("symbol", sort=False):
            pending: int | None = None
            for item in frame.itertuples():
                if item.side == "SELL" and item.authority != "TERMINAL_LIQUIDATION":
                    if pending is not None:
                        unmatched += 1
                    pending = int(item.execution_ordinal)
                elif item.side == "BUY" and item.authority in {
                    "FIXED_5_SESSION_RECOVERY", "MODEL_RECOVERY",
                } and pending is not None:
                    policy_waits.append(int(item.execution_ordinal) - pending)
                    pending = None
            unmatched += int(pending is not None)
        values = np.asarray(policy_waits, dtype=float)
        waits.append({
            "policy_id": policy, "completed_cycle_count": len(policy_waits),
            "unmatched_filled_trim_count": unmatched,
            "mean_sessions": float(np.mean(values)) if len(values) else None,
            "median_sessions": float(np.median(values)) if len(values) else None,
            "p90_sessions": float(np.quantile(values, 0.9)) if len(values) else None,
            "max_sessions": int(np.max(values)) if len(values) else None,
        })
    return {"paired_stock": paired, "recovery_wait": waits}


def _joint_intervals(candidate_nav: np.ndarray, baseline_nav: np.ndarray, *, alpha: float) -> dict[str, dict[str, float]]:
    if (
        len(candidate_nav) < 3 or len(candidate_nav) != len(baseline_nav)
        or not np.isfinite(candidate_nav).all() or not np.isfinite(baseline_nav).all()
        or np.any(candidate_nav <= 0) or np.any(baseline_nav <= 0)
    ):
        raise ActionValueError("RECOVERY_INTERVAL_INPUT_INVALID")
    paired = np.column_stack((
        candidate_nav[1:] / candidate_nav[:-1] - 1,
        baseline_nav[1:] / baseline_nav[:-1] - 1,
    ))
    block = min(BOOTSTRAP_BLOCK, len(paired))
    count = math.ceil(len(paired) / block)
    rng = np.random.default_rng(BOOTSTRAP_SEED)
    starts = rng.integers(0, len(paired), size=(BOOTSTRAP_REPLICATES, count))
    indexes = (starts[..., None] + np.arange(block)) % len(paired)
    samples = paired[indexes.reshape(BOOTSTRAP_REPLICATES, -1)[:, :len(paired)]]
    curves = np.concatenate((
        np.ones((BOOTSTRAP_REPLICATES, 1, 2)),
        np.cumprod(1.0 + samples, axis=1),
    ), axis=1)
    terminal = (curves[:, -1, 0] - curves[:, -1, 1]) * 10_000
    peaks = np.maximum.accumulate(curves, axis=1)
    drawdowns = np.min(curves / peaks - 1.0, axis=1)
    mdd = (drawdowns[:, 0] - drawdowns[:, 1]) * 10_000

    def interval(values: np.ndarray) -> dict[str, float]:
        lower, upper = np.quantile(values, (alpha / 2, 1 - alpha / 2))
        return {"lower": float(lower), "upper": float(upper)}

    return {"terminal_excess_bps": interval(terminal), "mdd_improvement_bps": interval(mdd)}


def _formal_report(pool_daily: pd.DataFrame) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    alpha = 0.05 / 4
    for policy in (RIDGE, GBDT):
        left = pool_daily.loc[pool_daily.policy_id.eq(policy)].sort_values("ordinal")
        right = pool_daily.loc[pool_daily.policy_id.eq(BH)].sort_values("ordinal")
        merged = left.merge(right, on="ordinal", suffixes=("_candidate", "_baseline"), validate="one_to_one")
        if merged[["nav_candidate", "nav_baseline"]].isna().any().any():
            results.append({"candidate": policy, "baseline": BH, "status": "UNAVAILABLE"})
            continue
        candidate = merged.nav_candidate.to_numpy(float)
        baseline = merged.nav_baseline.to_numpy(float)
        intervals = _joint_intervals(candidate, baseline, alpha=alpha)
        mdd_candidate = _performance(candidate)["max_drawdown"]
        mdd_baseline = _performance(baseline)["max_drawdown"]
        denominator = float(CAPITAL) * int(merged.account_count_candidate.iloc[-1])
        terminal_bps = float((candidate[-1] - baseline[-1]) / denominator * 10_000)
        mdd_bps = float((mdd_candidate - mdd_baseline) * 10000)
        endpoint_states = {
            "terminal": (
                "SUPPORTED" if intervals["terminal_excess_bps"]["lower"] > 0 else
                "NEGATIVE" if intervals["terminal_excess_bps"]["upper"] < 0 else "INCONCLUSIVE"
            ),
            "mdd": (
                "SUPPORTED" if intervals["mdd_improvement_bps"]["lower"] > 0 else
                "NEGATIVE" if intervals["mdd_improvement_bps"]["upper"] < 0 else "INCONCLUSIVE"
            ),
        }
        results.append({
            "candidate": policy, "baseline": BH, "status": "COMPLETE",
            "terminal_excess_bps": terminal_bps, "mdd_improvement_bps": mdd_bps,
            "familywise_intervals": intervals, "endpoint_states": endpoint_states,
            "joint_state": "JOINT_SUPPORTED_EXPLORATORY" if set(endpoint_states.values()) == {"SUPPORTED"} else "INCONCLUSIVE",
        })
    return results


def _seal(root: Path, names: list[str], *, request_sha256: str) -> dict[str, Any]:
    payload = {
        "schema_version": MANIFEST_SCHEMA, "request_sha256": request_sha256,
        "files": {name: file_reference(root / name) for name in names},
    }
    payload["manifest_sha256"] = canonical_sha256(payload)
    publish_json(root / "manifest.json", payload)
    return payload


def inspect(root: Path, *, request_sha256: str | None = None) -> dict[str, Any]:
    manifest = read_json(root / "manifest.json")
    identity = canonical_sha256({key: value for key, value in manifest.items() if key != "manifest_sha256"})
    if manifest.get("schema_version") != MANIFEST_SCHEMA or manifest.get("manifest_sha256") != identity:
        raise ActionValueError("RECOVERY_MANIFEST_DRIFT")
    if request_sha256 is not None and manifest.get("request_sha256") != request_sha256:
        raise ActionValueError("RECOVERY_MANIFEST_REQUEST_DRIFT")
    if {item.name for item in root.iterdir()} != set(manifest["files"]) | {"manifest.json"}:
        raise ActionValueError("RECOVERY_BUNDLE_MEMBER_DRIFT")
    for reference in manifest["files"].values():
        check_ref(reference)
    return {
        "status": "VERIFIED", "bundle": root.resolve().as_posix(),
        "request_sha256": manifest["request_sha256"], "manifest_sha256": manifest["manifest_sha256"],
    }


def run(path: Path, *, worker_count: int = WORKER_COUNT, symbol_limit: int | None = None) -> dict[str, Any]:
    request = load_request(path)
    digest = request["request_sha256"]
    artifact_root = Path(request["timing_root"]) / "research" / FOLDER
    suffix = f"-pilot{symbol_limit}" if symbol_limit is not None else ""
    bundle = artifact_root / "bundles" / f"{digest}{suffix}"
    with _exclusive_file_lock(artifact_root / "locks" / f"{digest}{suffix}.lock"):
        if (bundle / "manifest.json").exists():
            return {**inspect(bundle, request_sha256=digest), "status": "ALREADY_MATERIALIZED"}
        candidate = DailyCandidate.open(Path(request["candidate_root"]))
        identity = open_r8_proxy_sources(candidate.root)
        frames = read_proxy_frames(identity)
        if list(candidate.symbols) != request["symbols"] or [str(value.date()) for value in candidate.calendar] != request["calendar"]:
            raise ActionValueError("RECOVERY_POPULATION_DRIFT")
        if source_audit(identity, frames) != request["proxy_source_audit"]:
            raise ActionValueError("RECOVERY_SOURCE_AUDIT_DRIFT")
        parent_root, parent_manifest, trim_model = _parent_bundle(Path(request["timing_root"]))
        if parent_root.as_posix() != request["parent_bundle_root"] or parent_manifest["manifest_sha256"] != request["parent_bundle"]["manifest_canonical_sha256"]:
            raise ActionValueError("RECOVERY_PARENT_DRIFT")
        print(json.dumps({"stage": "RUN_AFTER_FULL_SOURCE_PREFLIGHT", "outcomes_read": True}), flush=True)
        symbols = candidate.symbols[:symbol_limit] if symbol_limit is not None else candidate.symbols
        benchmark = candidate.bars(BENCHMARK).close
        terminal = int(candidate.calendar.get_loc(pd.Timestamp(TERMINAL_DATE)))
        first_test_decision = int(candidate.calendar.get_loc(pd.Timestamp(TEST_FIRST_EXECUTION))) - 1

        def reference_inputs() -> Iterator[tuple[Any, ...]]:
            for symbol in symbols:
                yield (
                    symbol, candidate.bars(symbol), _symbol_frame(frames.daily, symbol),
                    benchmark, trim_model, terminal,
                )

        label_frames: list[pd.DataFrame] = []
        label_audits: list[dict[str, Any]] = []
        for completed, (symbol, labels, audit) in enumerate(
            _ordered_call(reference_inputs(), worker_count=worker_count, function=_reference_task), start=1
        ):
            label_audits.append({"symbol": symbol, **audit})
            if not labels.empty:
                label_frames.append(labels)
            if completed % 256 == 0 or completed == len(symbols):
                print(json.dumps({"stage": "LABELS", "symbols_complete": completed, "total": len(symbols)}), flush=True)
        labels = pd.concat(label_frames, ignore_index=True) if label_frames else pd.DataFrame()
        if labels.empty:
            raise ActionValueError("RECOVERY_LABEL_POPULATION_EMPTY")
        label_sha = _frame_sha256(labels)
        recovery_models = {
            "ridge": fit_ridge(labels, source_sha256=label_sha, request_sha256=digest),
            "gbdt": fit_gbdt(labels, source_sha256=label_sha, request_sha256=digest),
        }
        for model in recovery_models.values():
            validate_recovery_model(model)

        def replay_inputs() -> Iterator[tuple[Any, ...]]:
            for symbol in symbols:
                yield (
                    symbol, candidate.bars(symbol), _symbol_frame(frames.daily, symbol), benchmark,
                    trim_model, recovery_models, first_test_decision, terminal,
                )

        audits: list[dict[str, Any]] = []
        stock_frames: list[pd.DataFrame] = []
        fill_frames: list[pd.DataFrame] = []
        chunk_manifests: list[dict[str, Any]] = []
        chunk_items: list[dict[str, Any]] = []
        common_ordinals = np.arange(first_test_decision, terminal + 1)
        totals: dict[str, np.ndarray] = {policy: np.zeros(len(common_ordinals)) for policy in POLICY_IDS}
        counts: dict[str, np.ndarray] = {policy: np.zeros(len(common_ordinals), dtype=int) for policy in POLICY_IDS}
        enrolled = 0
        for completed, (symbol, days, fills, details, audit) in enumerate(
            _ordered_call(replay_inputs(), worker_count=worker_count, function=_replay_task), start=1
        ):
            audits.append({"symbol": symbol, **audit})
            symbol_stocks = pd.DataFrame()
            if not days.empty:
                enrolled += 1
                symbol_stocks = _stock_summaries(days, details)
                stock_frames.append(symbol_stocks)
                if not fills.empty:
                    fill_frames.append(fills)
                for policy, group in days.groupby("policy_id", sort=False):
                    ordered = group.set_index("ordinal").reindex(common_ordinals)
                    nav = ordered.nav.to_numpy(float)
                    pre = common_ordinals <= int(audit["enrollment_ordinal"])
                    nav[pre & ~np.isfinite(nav)] = float(CAPITAL)
                    valid = np.isfinite(nav)
                    totals[str(policy)][valid] += nav[valid]
                    counts[str(policy)][valid] += 1
            item = {
                "symbol": symbol, "stocks_sha256": _frame_sha256(symbol_stocks),
                "fills_sha256": _frame_sha256(fills),
                "enrollment_audit_sha256": canonical_sha256(audit),
            }
            chunk_items.append(item)
            if len(chunk_items) == CHUNK_SIZE or completed == len(symbols):
                payload = {
                    "chunk_index": len(chunk_manifests),
                    "symbols": [value["symbol"] for value in chunk_items], "items": chunk_items,
                }
                chunk_manifests.append({**payload, "chunk_sha256": canonical_sha256(payload)})
                chunk_items = []
            if completed % 256 == 0 or completed == len(symbols):
                print(json.dumps({"stage": "REPLAY", "symbols_complete": completed, "total": len(symbols)}), flush=True)

        stocks = pd.concat(stock_frames, ignore_index=True) if stock_frames else pd.DataFrame()
        fills = pd.concat(fill_frames, ignore_index=True) if fill_frames else pd.DataFrame()
        if sum(len(item["symbols"]) for item in chunk_manifests) != len(symbols):
            raise ActionValueError("RECOVERY_CHUNK_POPULATION_INCOMPLETE")
        pool_rows: list[dict[str, Any]] = []
        for policy in POLICY_IDS:
            for index, ordinal in enumerate(common_ordinals):
                known = int(counts[policy][index])
                pool_rows.append({
                    "policy_id": policy, "ordinal": int(ordinal),
                    "valuation_date": str(candidate.calendar[ordinal].date()),
                    "nav": float(totals[policy][index]) if known == enrolled else math.nan,
                    "known_nav_cny": float(totals[policy][index]),
                    "account_count": known, "expected_account_count": enrolled,
                    "unknown_account_count": enrolled - known,
                })
        pool_daily = pd.DataFrame(pool_rows)
        summaries: dict[str, Any] = {}
        for policy, group in pool_daily.groupby("policy_id", sort=False):
            perf = _performance(group.sort_values("ordinal").nav.to_numpy(float))
            summaries[str(policy)] = {**perf, "account_count": enrolled}
        formal = _formal_report(pool_daily)
        research_diagnostics = _research_diagnostics(stocks, fills)
        diagnostics: list[dict[str, Any]] = []
        parent_path = pool_daily.loc[pool_daily.policy_id.eq(PARENT)].sort_values("ordinal")
        for policy in (RIDGE, GBDT):
            path_frame = pool_daily.loc[pool_daily.policy_id.eq(policy)].sort_values("ordinal")
            diagnostics.append({
                "candidate": policy, "baseline": PARENT,
                "terminal_delta_cny": float(path_frame.nav.iloc[-1] - parent_path.nav.iloc[-1]),
                "mdd_improvement": float(
                    _performance(path_frame.nav.to_numpy(float))["max_drawdown"]
                    - _performance(parent_path.nav.to_numpy(float))["max_drawdown"]
                ),
            })
        report = {
            "schema_version": REPORT_SCHEMA, "request_sha256": digest,
            "result_class": "EXPLORATORY_SMALLCAP_RECOVERY", "selected_for_live": 0,
            "capital_cny": float(CAPITAL), "symbol_count": len(symbols),
            "enrolled_count": enrolled, "label_count": len(labels),
            "summaries": summaries, "formal_comparisons": formal,
            "parent_diagnostics": diagnostics,
            "research_diagnostics": research_diagnostics,
            "models": {
                key: {"model_sha256": value["model_sha256"], "validation": value["validation"]}
                for key, value in recovery_models.items()
            },
            "claims": {
                "live_alpha_supported": False, "oracle_used_as_target": False,
                "market_impact_simulated": False,
            },
        }
        trial_spec = {
            "schema_version": "position_timing_smallcap_recovery_trial_spec_v1",
            "request_sha256": digest, "contract": CONTRACT, "contract_sha256": CONTRACT_SHA256,
            "formal_family_size": 4, "runtime_parameter_selection_from_outcomes": False,
            "prior_smallcap_diagnostic_observed": True,
        }
        trial_spec["trial_spec_sha256"] = canonical_sha256(trial_spec)
        causality = {
            "schema_version": "position_timing_smallcap_recovery_causality_v1",
            "request_sha256": digest, "oracle_policy_access": False,
            "training_label_cutoff": "2022-12-31", "preprocessing_fit_scope": "TRAIN_ONLY",
            "test_labels_used_for_fit": False, "market_cap_availability": "T_MINUS_1",
            "training_label_event_source": "CAUSAL_RULE_CANDIDATE_FILLED_BEFORE_PARENT_MODEL_GATE",
            "parent_model_in_sample_selection_used": False,
            "status": "PASS_CONTRACT_AND_DIRECT_TESTS",
        }
        causality["causality_sha256"] = canonical_sha256(causality)
        receipt = {
            "schema_version": RECEIPT_SCHEMA, "request_sha256": digest,
            "symbol_count": len(symbols), "enrolled_count": enrolled,
            "label_count": len(labels), "chunk_count": len(chunk_manifests),
            "outcomes_read_stage": "RUN_AFTER_FULL_SOURCE_PREFLIGHT",
            "source_preflight_complete": True,
            "result_class": "EXPLORATORY_SMALLCAP_RECOVERY", "selected_for_live": 0,
            "database_read": False, "database_write": False,
            "market_network_accessed": False, "runtime_action_performed": False,
            "service_process_control_performed": False,
            "market_impact_simulated": False,
            "research_worker_processes_used": worker_count > 1,
        }
        receipt["receipt_sha256"] = canonical_sha256(receipt)
        publish_json(bundle / "request.json", request)
        publish_json(bundle / "source_audit.json", {"proxy": request["proxy_source_audit"], "source_preflight_complete": True})
        publish_json(bundle / "models.json", recovery_models)
        publish_json(bundle / "parent_model_reference.json", request["parent_bundle"])
        publish_json(bundle / "trial_spec.json", trial_spec)
        publish_json(bundle / "causality_receipt.json", causality)
        publish_json(bundle / "chunk_manifests.json", chunk_manifests)
        publish_json(bundle / "enrollment_audit.json", audits)
        publish_json(bundle / "label_enrollment_audit.json", label_audits)
        publish_json(bundle / "report.json", report)
        publish_json(bundle / "receipt.json", receipt)
        publish_frame(bundle / "labels.parquet", labels)
        publish_frame(bundle / "stocks.parquet", stocks)
        publish_frame(bundle / "pool_daily.parquet", pool_daily)
        publish_frame(bundle / "fills.parquet", fills)
        names = [
            "request.json", "source_audit.json", "models.json", "parent_model_reference.json",
            "trial_spec.json", "causality_receipt.json", "chunk_manifests.json",
            "enrollment_audit.json", "label_enrollment_audit.json", "report.json", "receipt.json",
            "labels.parquet", "stocks.parquet", "pool_daily.parquet", "fills.parquet",
        ]
        _seal(bundle, names, request_sha256=digest)
        return inspect(bundle, request_sha256=digest)


def verify_parallel(path: Path, *, size: int = 8) -> dict[str, Any]:
    request = load_request(path)
    candidate = DailyCandidate.open(Path(request["candidate_root"]))
    identity = open_r8_proxy_sources(candidate.root)
    frames = read_proxy_frames(identity)
    _, _, trim_model = _parent_bundle(Path(request["timing_root"]))
    symbols = candidate.symbols[:size]
    benchmark = candidate.bars(BENCHMARK).close
    terminal = int(candidate.calendar.get_loc(pd.Timestamp(TERMINAL_DATE)))

    def inputs() -> Iterator[tuple[Any, ...]]:
        for symbol in symbols:
            yield (
                symbol, candidate.bars(symbol), _symbol_frame(frames.daily, symbol),
                benchmark, trim_model, terminal,
            )

    sequential = list(_ordered_call(inputs(), worker_count=1, function=_reference_task))
    parallel = list(_ordered_call(inputs(), worker_count=WORKER_COUNT, function=_reference_task))
    left = canonical_sha256([
        {"symbol": symbol, "labels": _frame_sha256(labels), "audit": audit}
        for symbol, labels, audit in sequential
    ])
    right = canonical_sha256([
        {"symbol": symbol, "labels": _frame_sha256(labels), "audit": audit}
        for symbol, labels, audit in parallel
    ])
    if left != right:
        raise ActionValueError("RECOVERY_PARALLEL_DRIFT")
    return {"status": "EXACT", "symbol_count": size, "audit_sha256": left}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare_parser = commands.add_parser("prepare")
    prepare_parser.add_argument("--timing-root", type=Path, required=True)
    prepare_parser.add_argument("--repository-root", type=Path, required=True)
    prepare_parser.add_argument("--candidate-root", type=Path, required=True)
    run_parser = commands.add_parser("run")
    run_parser.add_argument("--request", type=Path, required=True)
    run_parser.add_argument("--workers", type=int, default=WORKER_COUNT)
    run_parser.add_argument("--symbol-limit", type=int)
    inspect_parser = commands.add_parser("inspect")
    inspect_parser.add_argument("--bundle", type=Path, required=True)
    parallel_parser = commands.add_parser("verify-parallel")
    parallel_parser.add_argument("--request", type=Path, required=True)
    parallel_parser.add_argument("--size", type=int, default=8)
    args = parser.parse_args()
    if args.command == "prepare":
        result: Any = {"request": prepare(timing_root=args.timing_root, repository_root=args.repository_root, candidate_root=args.candidate_root).as_posix()}
    elif args.command == "run":
        result = run(args.request, worker_count=args.workers, symbol_limit=args.symbol_limit)
    elif args.command == "inspect":
        result = inspect(args.bundle)
    else:
        result = verify_parallel(args.request, size=args.size)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
