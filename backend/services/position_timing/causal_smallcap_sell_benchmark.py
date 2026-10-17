"""PT-NEXT-027 small-cap SELL-vs-HOLD action-value benchmark.

Offline only: no database, market network, registry, card, or service adapter.
The experiment changes only the model that gates a frozen 20% tactical trim;
all timing paths use the same fixed-five-session recovery implementation.
"""
from __future__ import annotations

import argparse
from datetime import date
from decimal import Decimal
import json
import math
from pathlib import Path
from typing import Any, Iterator, Mapping

import numpy as np
import pandas as pd

from .action_value import ActionValueError
from .action_value_data import BENCHMARK, DailyCandidate, file_reference
from .artifact_store import _exclusive_file_lock
from .causal_recovery_benchmark import (
    PARENT_BUNDLE_ID,
    PARENT_GBDT_SHA256,
    PARENT_MANIFEST_CANONICAL_SHA256,
    _frame_sha256,
    _ordered_call,
    _parent_bundle,
    _performance,
    _small_enrollment,
    _symbol_inputs,
    replay_policy as _recovery_replay_policy,
)
from .causal_smallcap_sell_model import (
    FEATURE_ORDER,
    LABEL,
    LABEL_CONTRACT_ID,
    POPULATION_ID,
    fit_gbdt,
    fit_ridge,
    validate_model,
)
from .causal_timing_benchmark import (
    _environment_identity,
    _model_environment,
    _symbol_frame,
)
from .causal_timing_contracts import (
    E0,
    FEATURE_SPEC_SHA256,
    TERMINAL_DATE,
    TEST_FIRST_EXECUTION,
    TEST_LAST_DECISION,
    TEST_LAST_EXECUTION,
)
from .causal_timing_execution import affordable_for_budget, daily_quote, execute_quote
from .contracts import canonical_sha256
from .fundamental_screen import R8_DATASET_SHA256, R8_MANIFEST_FILE_SHA256
from .fundamental_timing_benchmark import _clean_repository_commit
from .pattern_close_cash_benchmark import check_ref, publish_frame, publish_json, read_json
from .pattern_close_cash_replay import Account, dec, fee, positive
from .r8_proxy_screen import MIN_TOTAL_MV, open_r8_proxy_sources, read_proxy_frames, source_audit


PIPELINE_ID = "POSITION_TIMING_SMALLCAP_SELL_ACTION_VALUE_V1"
FOLDER = "causal_smallcap_sell_value_v1"
REQUEST_SCHEMA = "position_timing_smallcap_sell_request_v1"
MANIFEST_SCHEMA = "position_timing_smallcap_sell_files_v1"
RECEIPT_SCHEMA = "position_timing_smallcap_sell_receipt_v1"
REPORT_SCHEMA = "position_timing_smallcap_sell_report_v1"

CAPITAL = Decimal("5000000")
TACTICAL_FRACTION = Decimal("0.20")
WAIT_SESSIONS = 5
LABEL_HORIZON_SESSIONS = 21
BH = "BH500"
PARENT = "PARENT_GBDT_FIXED5_V1"
RIDGE = "SMALLCAP_SELL_RIDGE_FIXED5_V1"
GBDT = "SMALLCAP_SELL_GBDT_FIXED5_V1"
POLICY_IDS = (BH, PARENT, RIDGE, GBDT)
MODEL_BY_POLICY = {RIDGE: "ridge", GBDT: "gbdt"}

WORKER_COUNT = 8
MAX_IN_FLIGHT = 8
CHUNK_SIZE = 64
BOOTSTRAP_REPLICATES = 5_000
BOOTSTRAP_BLOCK = 25
BOOTSTRAP_SEED = 20260921

CONTRACT: dict[str, Any] = {
    "pipeline_id": PIPELINE_ID,
    "candidate_manifest_file_sha256": R8_MANIFEST_FILE_SHA256,
    "candidate_dataset_sha256": R8_DATASET_SHA256,
    "parent_bundle_id": PARENT_BUNDLE_ID,
    "parent_manifest_canonical_sha256": PARENT_MANIFEST_CANONICAL_SHA256,
    "parent_gbdt_sha256": PARENT_GBDT_SHA256,
    "population": {
        "id": POPULATION_ID,
        "t_minus_1_total_mv_wanyuan_max_exclusive": MIN_TOTAL_MV,
        "unknown": "EXCLUDE_NO_FILL",
        "cohort_after_first_enrollment": True,
    },
    "capital_cny": str(CAPITAL),
    "account": "INDEPENDENT_CASH_NO_LEVERAGE_NO_INJECTION",
    "policies": list(POLICY_IDS),
    "candidate_event": "FROZEN_CAUSAL_RULE_CANDIDATE_WITHOUT_PARENT_MODEL_GATE",
    "only_variable": "SELL_GATE_MODEL",
    "tactical_fraction": str(TACTICAL_FRACTION),
    "recovery": {
        "policy": "FIXED_5_SESSION_RECOVERY_ALL_TIMING_PATHS",
        "wait_sessions": WAIT_SESSIONS,
    },
    "label": {
        "id": LABEL_CONTRACT_ID,
        "column": LABEL,
        "comparison": "SELL_FIXED5_MINUS_HOLD",
        "horizon_sessions": LABEL_HORIZON_SESSIONS,
        "parent_model_policy_access": False,
        "oracle_policy_access": False,
    },
    "feature_order": list(FEATURE_ORDER),
    "feature_spec_sha256": FEATURE_SPEC_SHA256,
    "models": ["RIDGE_CLOSED_FORM_V1", "LIGHTGBM_REGRESSION_V1"],
    "decision_threshold_bps": 0.0,
    "execution_view": E0,
    "test_first_execution": TEST_FIRST_EXECUTION.isoformat(),
    "test_last_decision": TEST_LAST_DECISION.isoformat(),
    "test_last_execution": TEST_LAST_EXECUTION.isoformat(),
    "terminal_date": TERMINAL_DATE.isoformat(),
    "statistics": {
        "formal_comparison_count": 4,
        "endpoint_count": 2,
        "family_size": 8,
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
        "backend/services/position_timing/causal_smallcap_sell_benchmark.py",
        "backend/services/position_timing/causal_smallcap_sell_model.py",
        "backend/services/position_timing/causal_recovery_benchmark.py",
        "backend/services/position_timing/causal_timing_contracts.py",
        "backend/services/position_timing/causal_timing_execution.py",
        "backend/services/position_timing/causal_timing_model.py",
        "backend/services/position_timing/core_tactical_timing.py",
        "backend/services/position_timing/pattern_close_cash_replay.py",
        "backend/services/position_timing/r8_proxy_screen.py",
    )
    return {name: file_reference(repository / name) for name in names}


def prepare(*, timing_root: Path, repository_root: Path, candidate_root: Path) -> Path:
    timing_root = timing_root.resolve()
    repository = repository_root.resolve()
    candidate_root = candidate_root.resolve()
    if (
        not timing_root.is_absolute()
        or timing_root.is_relative_to(repository)
        or timing_root.is_relative_to(candidate_root)
    ):
        raise ActionValueError("SMALLCAP_SELL_ROOT_ISOLATION_INVALID")
    candidate = DailyCandidate.open(candidate_root)
    identity = open_r8_proxy_sources(candidate_root)
    frames = read_proxy_frames(identity)
    if identity.candidate.dataset_sha256 != R8_DATASET_SHA256:
        raise ActionValueError("SMALLCAP_SELL_CANDIDATE_IDENTITY_DRIFT")
    if (
        candidate.calendar[0].date() != date(2018, 8, 1)
        or candidate.calendar[-1].date() != TERMINAL_DATE
    ):
        raise ActionValueError("SMALLCAP_SELL_CALENDAR_DRIFT")
    parent_root, parent_manifest, parent_model = _parent_bundle(timing_root)
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
            "gbdt_model_sha256": parent_model["model_sha256"],
        },
        "source_code": _source_files(repository),
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
            raise ActionValueError("SMALLCAP_SELL_REQUEST_IDENTITY_COLLISION")
    else:
        publish_json(path, request)
    return path


def load_request(path: Path) -> dict[str, Any]:
    request = read_json(path.resolve())
    digest = request.get("request_sha256")
    if (
        request.get("schema_version") != REQUEST_SCHEMA
        or canonical_sha256(
            {key: value for key, value in request.items() if key != "request_sha256"}
        )
        != digest
    ):
        raise ActionValueError("SMALLCAP_SELL_REQUEST_DRIFT")
    if request.get("contract_sha256") != CONTRACT_SHA256 or request.get("contract") != CONTRACT:
        raise ActionValueError("SMALLCAP_SELL_CONTRACT_DRIFT")
    repository = Path(request["repository_root"])
    if (
        _clean_repository_commit(repository) != request["repository_commit"]
        or _source_files(repository) != request["source_code"]
    ):
        raise ActionValueError("SMALLCAP_SELL_CODE_DRIFT")
    for reference in (
        request["candidate_manifest"],
        request["parent_bundle"]["manifest"],
        request["parent_bundle"]["models"],
    ):
        check_ref(reference)
    for reference in request["daily_source_data"].values():
        check_ref(reference)
    return request


def _standardized_label(
    *,
    symbol: str,
    bars: pd.DataFrame,
    adjusted: pd.DataFrame,
    decision: int,
    sell_execution: int,
) -> dict[str, Any]:
    horizon = decision + LABEL_HORIZON_SESSIONS
    if horizon >= len(bars) or sell_execution > horizon:
        return {"status": "LABEL_IMMATURE"}
    if not positive(adjusted.close.iloc[horizon]):
        return {"status": "LABEL_TERMINAL_UNAVAILABLE"}
    decision_bar = bars.iloc[decision]
    if not positive(decision_bar.close) or not positive(decision_bar.factor):
        return {"status": "LABEL_INITIAL_STATE_UNAVAILABLE"}
    raw = dec(decision_bar.close)
    factor = dec(decision_bar.factor)
    quantity = affordable_for_budget(symbol, CAPITAL, raw, CAPITAL)
    if quantity <= 0:
        return {"status": "LABEL_INITIAL_QUANTITY_UNAVAILABLE"}
    units = Decimal(quantity) / factor
    initial_notional = raw * quantity
    initial_fee = fee(initial_notional, "BUY")
    residual = CAPITAL - initial_notional - initial_fee
    adjusted_entry = (initial_notional + initial_fee) / units
    action = Account(
        cash=residual,
        units=units,
        entry=adjusted_entry,
        bought_on=decision - 1,
        fees=initial_fee,
        bought_once=True,
    )
    hold = Account(
        cash=residual,
        units=units,
        entry=adjusted_entry,
        bought_on=decision - 1,
        fees=initial_fee,
        bought_once=True,
    )
    sell = execute_quote(
        action,
        symbol=symbol,
        quote=daily_quote(
            bars.iloc[sell_execution].to_dict(),
            trade_date=bars.index[sell_execution].date(),
        ),
        ordinal=sell_execution,
        side="SELL",
        sell_fraction=TACTICAL_FRACTION,
    )
    buy_status = "NOT_APPLICABLE"
    buy_execution: int | None = None
    if sell["status"] == "FILLED":
        for ordinal in range(sell_execution + WAIT_SESSIONS, horizon + 1):
            buy = execute_quote(
                action,
                symbol=symbol,
                quote=daily_quote(
                    bars.iloc[ordinal].to_dict(), trade_date=bars.index[ordinal].date()
                ),
                ordinal=ordinal,
                side="BUY",
            )
            buy_status = str(buy["status"])
            if buy_status == "FILLED":
                buy_execution = ordinal
                break
    mark = dec(adjusted.close.iloc[horizon])
    action_wealth = action.wealth(mark)
    hold_wealth = hold.wealth(mark)
    if action_wealth is None or hold_wealth is None:
        return {"status": "LABEL_TERMINAL_UNAVAILABLE"}
    return {
        "status": "MATURED",
        "sell_status": sell["status"],
        "buy_status": buy_status,
        "buy_execution_ordinal": buy_execution,
        "label_window_end": str(bars.index[horizon].date()),
        "label_available_at": str(bars.index[horizon].date()),
        LABEL: float((action_wealth - hold_wealth) / CAPITAL * Decimal(10000)),
        "action_wealth_cny": float(action_wealth),
        "hold_wealth_cny": float(hold_wealth),
        "standardized_initial_quantity": quantity,
        "standardized_initial_buy_fee": float(initial_fee),
        "standardized_initial_residual_cash": float(residual),
    }


def sell_labels_from_fills(
    *,
    symbol: str,
    bars: pd.DataFrame,
    adjusted: pd.DataFrame,
    features: pd.DataFrame,
    fills: pd.DataFrame,
) -> pd.DataFrame:
    if fills.empty:
        return pd.DataFrame(
            columns=("symbol", "decision_date", "label_available_at", LABEL, *FEATURE_ORDER)
        )
    selected = fills.loc[
        fills.side.eq("SELL")
        & fills.status.eq("FILLED")
        & ~fills.authority.eq("TERMINAL_LIQUIDATION")
    ].sort_values(["decision_ordinal", "execution_ordinal"], kind="stable")
    rows: list[dict[str, Any]] = []
    for event_number, item in enumerate(selected.itertuples()):
        decision = int(item.decision_ordinal)
        values = features.iloc[decision].loc[list(FEATURE_ORDER)].to_numpy(float)
        if not np.isfinite(values).all():
            continue
        label = _standardized_label(
            symbol=symbol,
            bars=bars,
            adjusted=adjusted,
            decision=decision,
            sell_execution=int(item.execution_ordinal),
        )
        if label["status"] != "MATURED":
            continue
        rows.append(
            {
                "symbol": symbol,
                "event_id": f"{symbol}:{event_number}",
                "decision_ordinal": decision,
                "decision_date": str(bars.index[decision].date()),
                "reference_authority": item.authority,
                "reference_sell_execution_ordinal": int(item.execution_ordinal),
                "reference_fill_sha256": canonical_sha256(
                    {
                        "symbol": symbol,
                        "authority": item.authority,
                        "decision_ordinal": decision,
                        "execution_ordinal": int(item.execution_ordinal),
                    }
                ),
                **{name: float(features.iloc[decision][name]) for name in FEATURE_ORDER},
                **label,
            }
        )
    return pd.DataFrame(rows)


def _run_policy(
    *,
    symbol: str,
    bars: pd.DataFrame,
    values: Mapping[str, Any],
    policy_id: str,
    enrollment: int,
    terminal: int,
    parent_model: Mapping[str, Any],
    sell_models: Mapping[str, Mapping[str, Any]],
    label_reference: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    if label_reference:
        internal_policy = "PARENT_FIXED5_GBDT_V1"
        model = parent_model
    elif policy_id == BH:
        internal_policy = "BH500"
        model = parent_model
    else:
        internal_policy = "PARENT_FIXED5_GBDT_V1"
        model = parent_model if policy_id == PARENT else sell_models[MODEL_BY_POLICY[policy_id]]
    days, fills, detail = _recovery_replay_policy(
        symbol=symbol,
        bars=bars,
        adjusted=values["adjusted"],
        pattern=values["pattern"],
        trend=values["trend"],
        ready=values["ready"],
        base_features=values["base"],
        lagged_mv=values["lagged_mv"],
        policy_id=internal_policy,
        enrollment_ordinal=enrollment,
        terminal_ordinal=terminal,
        trim_model=model,
        recovery_models={},
        accept_all_trim_candidates_for_labels=label_reference,
    )
    if label_reference:
        return days, fills, detail
    days = days.assign(policy_id=policy_id)
    if not fills.empty:
        fills = fills.assign(policy_id=policy_id)
    detail = {**detail, "policy_id": policy_id}
    return days, fills, detail


def _reference_task(
    symbol: str,
    bars: pd.DataFrame,
    daily: pd.DataFrame,
    benchmark: pd.Series,
    parent_model: Mapping[str, Any],
    terminal: int,
) -> tuple[str, pd.DataFrame, dict[str, Any]]:
    values = _symbol_inputs(symbol, bars, daily, benchmark)
    enrollment, audit = _small_enrollment(
        bars=bars, ready=values["ready"], lagged_mv=values["lagged_mv"], first=0
    )
    if enrollment is None:
        return symbol, pd.DataFrame(), audit
    _, fills, _ = _run_policy(
        symbol=symbol,
        bars=bars,
        values=values,
        policy_id=PARENT,
        enrollment=enrollment,
        terminal=terminal,
        parent_model=parent_model,
        sell_models={},
        label_reference=True,
    )
    labels = sell_labels_from_fills(
        symbol=symbol,
        bars=bars,
        adjusted=values["adjusted"],
        features=values["base"],
        fills=fills,
    )
    return symbol, labels, audit


def _replay_task(
    symbol: str,
    bars: pd.DataFrame,
    daily: pd.DataFrame,
    benchmark: pd.Series,
    parent_model: Mapping[str, Any],
    sell_models: Mapping[str, Mapping[str, Any]],
    first: int,
    terminal: int,
) -> tuple[str, pd.DataFrame, pd.DataFrame, list[dict[str, Any]], dict[str, Any]]:
    values = _symbol_inputs(symbol, bars, daily, benchmark)
    enrollment, audit = _small_enrollment(
        bars=bars, ready=values["ready"], lagged_mv=values["lagged_mv"], first=first
    )
    if enrollment is None:
        return symbol, pd.DataFrame(), pd.DataFrame(), [], audit
    day_frames: list[pd.DataFrame] = []
    fill_frames: list[pd.DataFrame] = []
    details: list[dict[str, Any]] = []
    for policy in POLICY_IDS:
        days, fills, detail = _run_policy(
            symbol=symbol,
            bars=bars,
            values=values,
            policy_id=policy,
            enrollment=enrollment,
            terminal=terminal,
            parent_model=parent_model,
            sell_models=sell_models,
        )
        day_frames.append(days.dropna(axis=1, how="all"))
        if not fills.empty:
            fill_frames.append(fills)
        details.append(detail)
    return (
        symbol,
        pd.concat(day_frames, ignore_index=True),
        pd.concat(fill_frames, ignore_index=True) if fill_frames else pd.DataFrame(),
        details,
        audit,
    )


def _stock_summaries(days: pd.DataFrame, details: list[dict[str, Any]]) -> pd.DataFrame:
    detail_map = {item["policy_id"]: item for item in details}
    rows: list[dict[str, Any]] = []
    for policy, frame in days.groupby("policy_id", sort=False):
        ordered = frame.sort_values("ordinal", kind="stable")
        nav = ordered.nav.to_numpy(float)
        perf = _performance(nav)
        finite = np.isfinite(nav)
        detail = detail_map[str(policy)]
        exposure = ordered.exposure.to_numpy(float)
        rows.append(
            {
                "symbol": str(ordered.symbol.iloc[0]),
                "policy_id": str(policy),
                "terminal_nav_cny": perf.get("terminal_nav_cny"),
                "total_return": perf.get("total_return"),
                "max_drawdown": perf.get("max_drawdown"),
                "average_exposure": float(np.mean(exposure[finite])) if finite.any() else None,
                "fees_cny": detail["fees_cny"],
                "trim_model_rejected_count": detail["trim_model_rejected_count"],
                "trim_model_unavailable_count": detail["trim_model_unavailable_count"],
            }
        )
    return pd.DataFrame(rows)


def _joint_intervals(
    candidate_nav: np.ndarray, baseline_nav: np.ndarray, *, alpha: float
) -> dict[str, dict[str, float]]:
    if (
        len(candidate_nav) < 3
        or len(candidate_nav) != len(baseline_nav)
        or not np.isfinite(candidate_nav).all()
        or not np.isfinite(baseline_nav).all()
        or np.any(candidate_nav <= 0)
        or np.any(baseline_nav <= 0)
    ):
        raise ActionValueError("SMALLCAP_SELL_INTERVAL_INPUT_INVALID")
    paired = np.column_stack(
        (
            candidate_nav[1:] / candidate_nav[:-1] - 1,
            baseline_nav[1:] / baseline_nav[:-1] - 1,
        )
    )
    block = min(BOOTSTRAP_BLOCK, len(paired))
    count = math.ceil(len(paired) / block)
    rng = np.random.default_rng(BOOTSTRAP_SEED)
    starts = rng.integers(0, len(paired), size=(BOOTSTRAP_REPLICATES, count))
    indexes = (starts[..., None] + np.arange(block)) % len(paired)
    samples = paired[indexes.reshape(BOOTSTRAP_REPLICATES, -1)[:, : len(paired)]]
    curves = np.concatenate(
        (np.ones((BOOTSTRAP_REPLICATES, 1, 2)), np.cumprod(1.0 + samples, axis=1)),
        axis=1,
    )
    terminal = (curves[:, -1, 0] - curves[:, -1, 1]) * 10_000
    peaks = np.maximum.accumulate(curves, axis=1)
    drawdowns = np.min(curves / peaks - 1.0, axis=1)
    mdd = (drawdowns[:, 0] - drawdowns[:, 1]) * 10_000

    def interval(values: np.ndarray) -> dict[str, float]:
        lower, upper = np.quantile(values, (alpha / 2, 1 - alpha / 2))
        return {"lower": float(lower), "upper": float(upper)}

    return {
        "terminal_excess_bps": interval(terminal),
        "mdd_improvement_bps": interval(mdd),
    }


def _formal_report(pool_daily: pd.DataFrame) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    alpha = 0.05 / 8
    for candidate in (RIDGE, GBDT):
        for baseline in (BH, PARENT):
            left = pool_daily.loc[pool_daily.policy_id.eq(candidate)].sort_values("ordinal")
            right = pool_daily.loc[pool_daily.policy_id.eq(baseline)].sort_values("ordinal")
            merged = left.merge(
                right, on="ordinal", suffixes=("_candidate", "_baseline"), validate="one_to_one"
            )
            if merged[["nav_candidate", "nav_baseline"]].isna().any().any():
                results.append({"candidate": candidate, "baseline": baseline, "status": "UNAVAILABLE"})
                continue
            candidate_nav = merged.nav_candidate.to_numpy(float)
            baseline_nav = merged.nav_baseline.to_numpy(float)
            intervals = _joint_intervals(candidate_nav, baseline_nav, alpha=alpha)
            denominator = float(CAPITAL) * int(merged.account_count_candidate.iloc[-1])
            terminal_bps = float((candidate_nav[-1] - baseline_nav[-1]) / denominator * 10_000)
            mdd_bps = float(
                (_performance(candidate_nav)["max_drawdown"] - _performance(baseline_nav)["max_drawdown"])
                * 10_000
            )
            endpoint_states = {
                "terminal": (
                    "SUPPORTED"
                    if intervals["terminal_excess_bps"]["lower"] > 0
                    else "NEGATIVE"
                    if intervals["terminal_excess_bps"]["upper"] < 0
                    else "INCONCLUSIVE"
                ),
                "mdd": (
                    "SUPPORTED"
                    if intervals["mdd_improvement_bps"]["lower"] > 0
                    else "NEGATIVE"
                    if intervals["mdd_improvement_bps"]["upper"] < 0
                    else "INCONCLUSIVE"
                ),
            }
            results.append(
                {
                    "candidate": candidate,
                    "baseline": baseline,
                    "status": "COMPLETE",
                    "terminal_excess_bps": terminal_bps,
                    "mdd_improvement_bps": mdd_bps,
                    "familywise_intervals": intervals,
                    "endpoint_states": endpoint_states,
                    "joint_state": (
                        "JOINT_SUPPORTED_EXPLORATORY"
                        if set(endpoint_states.values()) == {"SUPPORTED"}
                        else "INCONCLUSIVE"
                    ),
                }
            )
    return results


def _research_diagnostics(stocks: pd.DataFrame, fills: pd.DataFrame) -> dict[str, Any]:
    comparisons: list[dict[str, Any]] = []
    for policy in (PARENT, RIDGE, GBDT):
        candidate = stocks.loc[stocks.policy_id.eq(policy)].set_index("symbol")
        for baseline in (BH, PARENT):
            if policy == PARENT and baseline == PARENT:
                continue
            reference = stocks.loc[stocks.policy_id.eq(baseline)].set_index("symbol")
            paired = candidate.join(
                reference[["terminal_nav_cny", "max_drawdown"]],
                rsuffix="_baseline",
                how="inner",
                validate="one_to_one",
            )
            terminal_win = paired.terminal_nav_cny > paired.terminal_nav_cny_baseline
            mdd_win = paired.max_drawdown > paired.max_drawdown_baseline
            comparisons.append(
                {
                    "candidate": policy,
                    "baseline": baseline,
                    "paired_stock_count": len(paired),
                    "terminal_win_rate": float(terminal_win.mean()),
                    "mdd_win_rate": float(mdd_win.mean()),
                    "dual_win_rate": float((terminal_win & mdd_win).mean()),
                    "mean_exposure": float(candidate.average_exposure.mean()),
                    "mean_fees_cny": float(candidate.fees_cny.mean()),
                    "model_rejected_count": int(candidate.trim_model_rejected_count.sum()),
                    "model_unavailable_count": int(candidate.trim_model_unavailable_count.sum()),
                }
            )
    cycles: list[dict[str, Any]] = []
    if not fills.empty:
        completed = fills.loc[fills.status.eq("FILLED")].sort_values(
            ["policy_id", "symbol", "execution_ordinal"], kind="stable"
        )
        for policy in (PARENT, RIDGE, GBDT):
            waits: list[int] = []
            unmatched = 0
            for _, frame in completed.loc[completed.policy_id.eq(policy)].groupby("symbol", sort=False):
                pending: int | None = None
                for item in frame.itertuples():
                    if item.side == "SELL" and item.authority != "TERMINAL_LIQUIDATION":
                        unmatched += int(pending is not None)
                        pending = int(item.execution_ordinal)
                    elif item.side == "BUY" and item.authority == "FIXED_5_SESSION_RECOVERY" and pending is not None:
                        waits.append(int(item.execution_ordinal) - pending)
                        pending = None
                unmatched += int(pending is not None)
            values = np.asarray(waits, dtype=float)
            cycles.append(
                {
                    "policy_id": policy,
                    "completed_cycle_count": len(waits),
                    "unmatched_filled_trim_count": unmatched,
                    "mean_sessions": float(np.mean(values)) if len(values) else None,
                    "median_sessions": float(np.median(values)) if len(values) else None,
                    "minimum_sessions": int(np.min(values)) if len(values) else None,
                }
            )
    return {"paired_stock": comparisons, "fixed5_cycles": cycles}


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
    identity = canonical_sha256(
        {key: value for key, value in manifest.items() if key != "manifest_sha256"}
    )
    if manifest.get("schema_version") != MANIFEST_SCHEMA or manifest.get("manifest_sha256") != identity:
        raise ActionValueError("SMALLCAP_SELL_MANIFEST_DRIFT")
    if request_sha256 is not None and manifest.get("request_sha256") != request_sha256:
        raise ActionValueError("SMALLCAP_SELL_MANIFEST_REQUEST_DRIFT")
    if {item.name for item in root.iterdir()} != set(manifest["files"]) | {"manifest.json"}:
        raise ActionValueError("SMALLCAP_SELL_BUNDLE_MEMBER_DRIFT")
    for reference in manifest["files"].values():
        check_ref(reference)
    return {
        "status": "VERIFIED",
        "bundle": root.resolve().as_posix(),
        "request_sha256": manifest["request_sha256"],
        "manifest_sha256": manifest["manifest_sha256"],
    }


def run(
    path: Path, *, worker_count: int = WORKER_COUNT, symbol_limit: int | None = None
) -> dict[str, Any]:
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
        if (
            list(candidate.symbols) != request["symbols"]
            or [str(value.date()) for value in candidate.calendar] != request["calendar"]
        ):
            raise ActionValueError("SMALLCAP_SELL_POPULATION_DRIFT")
        if source_audit(identity, frames) != request["proxy_source_audit"]:
            raise ActionValueError("SMALLCAP_SELL_SOURCE_AUDIT_DRIFT")
        parent_root, parent_manifest, parent_model = _parent_bundle(Path(request["timing_root"]))
        if (
            parent_root.as_posix() != request["parent_bundle_root"]
            or parent_manifest["manifest_sha256"]
            != request["parent_bundle"]["manifest_canonical_sha256"]
        ):
            raise ActionValueError("SMALLCAP_SELL_PARENT_DRIFT")
        print(
            json.dumps({"stage": "RUN_AFTER_FULL_SOURCE_PREFLIGHT", "outcomes_read": True}),
            flush=True,
        )
        symbols = candidate.symbols[:symbol_limit] if symbol_limit is not None else candidate.symbols
        benchmark = candidate.bars(BENCHMARK).close
        terminal = int(candidate.calendar.get_loc(pd.Timestamp(TERMINAL_DATE)))
        first_test_decision = int(candidate.calendar.get_loc(pd.Timestamp(TEST_FIRST_EXECUTION))) - 1

        def reference_inputs() -> Iterator[tuple[Any, ...]]:
            for symbol in symbols:
                yield (
                    symbol,
                    candidate.bars(symbol),
                    _symbol_frame(frames.daily, symbol),
                    benchmark,
                    parent_model,
                    terminal,
                )

        label_frames: list[pd.DataFrame] = []
        label_audits: list[dict[str, Any]] = []
        for completed, (symbol, labels, audit) in enumerate(
            _ordered_call(reference_inputs(), worker_count=worker_count, function=_reference_task),
            start=1,
        ):
            label_audits.append({"symbol": symbol, **audit})
            if not labels.empty:
                label_frames.append(labels)
            if completed % 256 == 0 or completed == len(symbols):
                print(
                    json.dumps({"stage": "LABELS", "symbols_complete": completed, "total": len(symbols)}),
                    flush=True,
                )
        labels = pd.concat(label_frames, ignore_index=True) if label_frames else pd.DataFrame()
        if labels.empty:
            raise ActionValueError("SMALLCAP_SELL_LABEL_POPULATION_EMPTY")
        label_sha = _frame_sha256(labels)
        models = {
            "ridge": fit_ridge(labels, source_sha256=label_sha, request_sha256=digest),
            "gbdt": fit_gbdt(labels, source_sha256=label_sha, request_sha256=digest),
        }
        for model in models.values():
            validate_model(model)

        def replay_inputs() -> Iterator[tuple[Any, ...]]:
            for symbol in symbols:
                yield (
                    symbol,
                    candidate.bars(symbol),
                    _symbol_frame(frames.daily, symbol),
                    benchmark,
                    parent_model,
                    models,
                    first_test_decision,
                    terminal,
                )

        audits: list[dict[str, Any]] = []
        stock_frames: list[pd.DataFrame] = []
        fill_frames: list[pd.DataFrame] = []
        chunk_manifests: list[dict[str, Any]] = []
        chunk_items: list[dict[str, Any]] = []
        ordinals = np.arange(first_test_decision, terminal + 1)
        totals = {policy: np.zeros(len(ordinals)) for policy in POLICY_IDS}
        counts = {policy: np.zeros(len(ordinals), dtype=int) for policy in POLICY_IDS}
        enrolled = 0
        for completed, (symbol, days, fills, details, audit) in enumerate(
            _ordered_call(replay_inputs(), worker_count=worker_count, function=_replay_task),
            start=1,
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
                    ordered = group.set_index("ordinal").reindex(ordinals)
                    nav = ordered.nav.to_numpy(float)
                    pre = ordinals <= int(audit["enrollment_ordinal"])
                    nav[pre & ~np.isfinite(nav)] = float(CAPITAL)
                    valid = np.isfinite(nav)
                    totals[str(policy)][valid] += nav[valid]
                    counts[str(policy)][valid] += 1
            chunk_items.append(
                {
                    "symbol": symbol,
                    "stocks_sha256": _frame_sha256(symbol_stocks),
                    "fills_sha256": _frame_sha256(fills),
                    "enrollment_audit_sha256": canonical_sha256(audit),
                }
            )
            if len(chunk_items) == CHUNK_SIZE or completed == len(symbols):
                payload = {
                    "chunk_index": len(chunk_manifests),
                    "symbols": [item["symbol"] for item in chunk_items],
                    "items": chunk_items,
                }
                chunk_manifests.append({**payload, "chunk_sha256": canonical_sha256(payload)})
                chunk_items = []
            if completed % 256 == 0 or completed == len(symbols):
                print(
                    json.dumps({"stage": "REPLAY", "symbols_complete": completed, "total": len(symbols)}),
                    flush=True,
                )
        stocks = pd.concat(stock_frames, ignore_index=True) if stock_frames else pd.DataFrame()
        fills = pd.concat(fill_frames, ignore_index=True) if fill_frames else pd.DataFrame()
        if sum(len(item["symbols"]) for item in chunk_manifests) != len(symbols):
            raise ActionValueError("SMALLCAP_SELL_CHUNK_POPULATION_INCOMPLETE")
        pool_rows: list[dict[str, Any]] = []
        for policy in POLICY_IDS:
            for index, ordinal in enumerate(ordinals):
                known = int(counts[policy][index])
                pool_rows.append(
                    {
                        "policy_id": policy,
                        "ordinal": int(ordinal),
                        "valuation_date": str(candidate.calendar[ordinal].date()),
                        "nav": float(totals[policy][index]) if known == enrolled else math.nan,
                        "known_nav_cny": float(totals[policy][index]),
                        "account_count": known,
                        "expected_account_count": enrolled,
                        "unknown_account_count": enrolled - known,
                    }
                )
        pool_daily = pd.DataFrame(pool_rows)
        summaries: dict[str, Any] = {}
        for policy, group in pool_daily.groupby("policy_id", sort=False):
            summaries[str(policy)] = {
                **_performance(group.sort_values("ordinal").nav.to_numpy(float)),
                "account_count": enrolled,
            }
        formal = _formal_report(pool_daily)
        model_summary = {
            key: {"model_sha256": value["model_sha256"], "validation": value["validation"]}
            for key, value in models.items()
        }
        alpha_supported = any(
            item["baseline"] == BH and item.get("joint_state") == "JOINT_SUPPORTED_EXPLORATORY"
            for item in formal
        )
        mismatch_supported = any(
            item["baseline"] == PARENT and item.get("joint_state") == "JOINT_SUPPORTED_EXPLORATORY"
            for item in formal
        )
        result_class = (
            "EXPLORATORY_ALPHA_AND_DOMAIN_SUPPORTED"
            if alpha_supported and mismatch_supported
            else "EXPLORATORY_ALPHA_SUPPORTED"
            if alpha_supported
            else "EXPLORATORY_DOMAIN_MISMATCH_SUPPORTED"
            if mismatch_supported
            else "EXPLORATORY_SMALLCAP_SELL_INCONCLUSIVE"
        )
        report = {
            "schema_version": REPORT_SCHEMA,
            "request_sha256": digest,
            "result_class": result_class,
            "selected_for_live": 0,
            "capital_cny": float(CAPITAL),
            "symbol_count": len(symbols),
            "enrolled_count": enrolled,
            "label_count": len(labels),
            "summaries": summaries,
            "formal_comparisons": formal,
            "research_diagnostics": _research_diagnostics(stocks, fills),
            "models": model_summary,
            "claims": {
                "alpha_supported_exploratory": alpha_supported,
                "domain_mismatch_supported_exploratory": mismatch_supported,
                "live_alpha_supported": False,
                "oracle_used_as_target": False,
                "market_impact_simulated": False,
            },
        }
        trial_spec = {
            "schema_version": "position_timing_smallcap_sell_trial_spec_v1",
            "request_sha256": digest,
            "contract": CONTRACT,
            "contract_sha256": CONTRACT_SHA256,
            "formal_family_size": 8,
            "runtime_parameter_selection_from_outcomes": False,
            "hypothesis_generated_from_prior_test_diagnostics": True,
        }
        trial_spec["trial_spec_sha256"] = canonical_sha256(trial_spec)
        causality = {
            "schema_version": "position_timing_smallcap_sell_causality_v1",
            "request_sha256": digest,
            "oracle_policy_access": False,
            "parent_model_training_label_selection_used": False,
            "training_label_cutoff": "2022-12-31",
            "preprocessing_fit_scope": "TRAIN_ONLY",
            "test_labels_used_for_fit": False,
            "market_cap_availability": "T_MINUS_1",
            "label_event_source": "CAUSAL_RULE_CANDIDATE_WITHOUT_PARENT_MODEL_GATE",
            "status": "PASS_CONTRACT_AND_DIRECT_TESTS",
        }
        causality["causality_sha256"] = canonical_sha256(causality)
        receipt = {
            "schema_version": RECEIPT_SCHEMA,
            "request_sha256": digest,
            "symbol_count": len(symbols),
            "enrolled_count": enrolled,
            "label_count": len(labels),
            "chunk_count": len(chunk_manifests),
            "outcomes_read_stage": "RUN_AFTER_FULL_SOURCE_PREFLIGHT",
            "source_preflight_complete": True,
            "result_class": report["result_class"],
            "selected_for_live": 0,
            "database_read": False,
            "database_write": False,
            "market_network_accessed": False,
            "runtime_action_performed": False,
            "service_process_control_performed": False,
            "market_impact_simulated": False,
            "research_worker_processes_used": worker_count > 1,
        }
        receipt["receipt_sha256"] = canonical_sha256(receipt)
        publish_json(bundle / "request.json", request)
        publish_json(
            bundle / "source_audit.json",
            {"proxy": request["proxy_source_audit"], "source_preflight_complete": True},
        )
        publish_json(bundle / "models.json", models)
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
            "request.json",
            "source_audit.json",
            "models.json",
            "parent_model_reference.json",
            "trial_spec.json",
            "causality_receipt.json",
            "chunk_manifests.json",
            "enrollment_audit.json",
            "label_enrollment_audit.json",
            "report.json",
            "receipt.json",
            "labels.parquet",
            "stocks.parquet",
            "pool_daily.parquet",
            "fills.parquet",
        ]
        _seal(bundle, names, request_sha256=digest)
        return inspect(bundle, request_sha256=digest)


def verify_parallel(path: Path, *, size: int = 8) -> dict[str, Any]:
    request = load_request(path)
    candidate = DailyCandidate.open(Path(request["candidate_root"]))
    identity = open_r8_proxy_sources(candidate.root)
    frames = read_proxy_frames(identity)
    _, _, parent_model = _parent_bundle(Path(request["timing_root"]))
    symbols = candidate.symbols[:size]
    benchmark = candidate.bars(BENCHMARK).close
    terminal = int(candidate.calendar.get_loc(pd.Timestamp(TERMINAL_DATE)))

    def inputs() -> Iterator[tuple[Any, ...]]:
        for symbol in symbols:
            yield (
                symbol,
                candidate.bars(symbol),
                _symbol_frame(frames.daily, symbol),
                benchmark,
                parent_model,
                terminal,
            )

    sequential = list(_ordered_call(inputs(), worker_count=1, function=_reference_task))
    parallel = list(_ordered_call(inputs(), worker_count=WORKER_COUNT, function=_reference_task))
    left = canonical_sha256(
        [
            {"symbol": symbol, "labels": _frame_sha256(labels), "audit": audit}
            for symbol, labels, audit in sequential
        ]
    )
    right = canonical_sha256(
        [
            {"symbol": symbol, "labels": _frame_sha256(labels), "audit": audit}
            for symbol, labels, audit in parallel
        ]
    )
    if left != right:
        raise ActionValueError("SMALLCAP_SELL_PARALLEL_DRIFT")
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
    run_parser.add_argument("--worker-count", type=int, default=WORKER_COUNT)
    run_parser.add_argument("--symbol-limit", type=int)
    inspect_parser = commands.add_parser("inspect")
    inspect_parser.add_argument("--bundle", type=Path, required=True)
    parallel_parser = commands.add_parser("verify-parallel")
    parallel_parser.add_argument("--request", type=Path, required=True)
    parallel_parser.add_argument("--size", type=int, default=8)
    args = parser.parse_args()
    if args.command == "prepare":
        result: Any = prepare(
            timing_root=args.timing_root,
            repository_root=args.repository_root,
            candidate_root=args.candidate_root,
        )
        print(json.dumps({"request": result.resolve().as_posix()}))
    elif args.command == "run":
        print(
            json.dumps(
                run(args.request, worker_count=args.worker_count, symbol_limit=args.symbol_limit),
                indent=2,
            )
        )
    elif args.command == "inspect":
        print(json.dumps(inspect(args.bundle), indent=2))
    else:
        print(json.dumps(verify_parallel(args.request, size=args.size), indent=2))


if __name__ == "__main__":
    main()
