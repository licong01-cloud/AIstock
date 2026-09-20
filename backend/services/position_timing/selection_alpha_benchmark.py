"""Immutable PT-NEXT-028 R8 selection-alpha cohort benchmark."""
from __future__ import annotations

import argparse
from concurrent.futures import Future, ProcessPoolExecutor
from datetime import date
from decimal import Decimal
import hashlib
import json
import multiprocessing
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence

import numpy as np
import pandas as pd

from .action_value import ActionValueError
from .action_value_data import BENCHMARK, DailyCandidate, file_reference
from .artifact_store import _exclusive_file_lock
from .contracts import canonical_sha256
from .fundamental_screen import (
    R8_DATASET_SHA256,
    R8_MANIFEST_FILE_SHA256,
)
from .fundamental_timing_benchmark import _clean_repository_commit, _environment_identity
from .pattern_adj_factor_restatement import (
    audit_candidate_adj_factor_restatement,
    open_adj_factor_restatement_authority,
)
from .pattern_close_cash_benchmark import check_ref, publish_frame, publish_json, read_json
from .pattern_close_cash_replay import Account, dec, execute, execution_status, fee, positive
from .pattern_universe_benchmark import (
    EXPECTED_ADJ_FACTOR_RESTATEMENT_AUTHORITY_SHA256,
    QLIB_ADJUSTED_FACTOR_CONTRACT_SHA256,
    _audit_qlib_adjusted_factor_integrity,
)
from .r8_proxy_screen import (
    SCREEN_CONTRACT,
    SCREEN_CONTRACT_SHA256,
    SCREEN_IDS,
    ProxyFrames,
    ProxySourceIdentity,
    open_r8_proxy_sources,
    read_proxy_frames,
    screen_masks_for_symbol,
    source_audit,
)


PIPELINE_ID = "POSITION_TIMING_SELECTION_ALPHA_BASELINE_V1"
FOLDER = "selection_alpha_baseline_v1"
REQUEST_SCHEMA = "position_timing_selection_alpha_request_v1"
RECEIPT_SCHEMA = "position_timing_selection_alpha_receipt_v1"
MANIFEST_SCHEMA = "position_timing_selection_alpha_files_v1"
REPORT_SCHEMA = "position_timing_selection_alpha_report_v1"

CAPITAL = Decimal("5000000")
HORIZONS = (20, 60, 120, 252)
PRIMARY_HORIZON = 120
TERMINAL_SELLABILITY_DEFER = 5
COMPARISONS = (
    ("U1_MINUS_U0_120D", SCREEN_IDS[1], SCREEN_IDS[0]),
    ("U2_MINUS_U0_120D", SCREEN_IDS[2], SCREEN_IDS[0]),
    ("U2_MINUS_U1_120D", SCREEN_IDS[2], SCREEN_IDS[1]),
)
FAMILY_SIZE = len(COMPARISONS)
BOOTSTRAP_REPLICATES = 5_000
BOOTSTRAP_BLOCK_MONTHS = 12
BOOTSTRAP_SEED = 20260921
WORKER_COUNT = 8
MAX_IN_FLIGHT = 16
CHUNK_SIZE = 128

CONTRACT = {
    "pipeline_id": PIPELINE_ID,
    "screens": SCREEN_CONTRACT,
    "screen_contract_sha256": SCREEN_CONTRACT_SHA256,
    "decision_schedule": "FIRST_GLOBAL_TRADING_SESSION_OF_CALENDAR_MONTH",
    "screen_availability": "PREVIOUS_GLOBAL_TRADING_SESSION",
    "execution": "DECISION_T_BUY_ON_T_PLUS_1_CLOSE_ONCE_NO_RETRY",
    "capital_per_stock_event_cny": str(CAPITAL),
    "horizons_sessions_after_execution": list(HORIZONS),
    "primary_horizon": PRIMARY_HORIZON,
    "primary_outcome": "NET_MTM_AFTER_ESTIMATED_EXIT_COST",
    "terminal_execution_claimed": False,
    "terminal_sellability_defer_sessions": TERMINAL_SELLABILITY_DEFER,
    "formal_comparisons": [name for name, _, _ in COMPARISONS],
    "family_size": FAMILY_SIZE,
    "diagnostic_horizons": [value for value in HORIZONS if value != PRIMARY_HORIZON],
    "bootstrap": {
        "replicates": BOOTSTRAP_REPLICATES,
        "block_months": BOOTSTRAP_BLOCK_MONTHS,
        "seed": BOOTSTRAP_SEED,
        "family_size": FAMILY_SIZE,
    },
    "parallel_execution": {
        "executor": "PROCESS_POOL_EXECUTOR",
        "start_method": "spawn",
        "worker_count": WORKER_COUNT,
        "max_in_flight": MAX_IN_FLIGHT,
        "chunk_size": CHUNK_SIZE,
        "writer": "PARENT_PROCESS_ONLY",
        "result_order": "CANONICAL_SYMBOL_ORDER",
    },
    "result_class": "EXPLORATORY_HYPOTHESIS_GENERATED",
    "selected_trial_count": 0,
    "strict_financial_pit_claimed": False,
    "database_read": False,
    "database_write": False,
    "network_accessed": False,
    "live_market_read": False,
    "runtime_action_performed": False,
    "service_process_control_performed": False,
    "other_module_write": False,
}
CONTRACT_SHA256 = canonical_sha256(CONTRACT)


def _source_files(repository: Path) -> dict[str, dict[str, Any]]:
    names = (
        "backend/services/position_timing/selection_alpha_benchmark.py",
        "backend/services/position_timing/r8_proxy_screen.py",
        "backend/services/position_timing/fundamental_screen.py",
        "backend/services/position_timing/fundamental_timing_benchmark.py",
        "backend/services/position_timing/action_value.py",
        "backend/services/position_timing/action_value_data.py",
        "backend/services/position_timing/artifact_store.py",
        "backend/services/position_timing/contracts.py",
        "backend/services/position_timing/pattern_adj_factor_restatement.py",
        "backend/services/position_timing/pattern_close_cash_benchmark.py",
        "backend/services/position_timing/pattern_close_cash_replay.py",
        "backend/services/position_timing/pattern_universe_benchmark.py",
        "backend/services/position_timing/policy.py",
        "backend/execution_algos/board_lot.py",
    )
    return {name: file_reference(repository / name) for name in names}


def _proxy_source_audit(
    candidate: DailyCandidate,
    identity: ProxySourceIdentity,
    frames: ProxyFrames,
) -> dict[str, Any]:
    audit = source_audit(identity, frames)
    symbols = set(candidate.symbols)
    audit["candidate_symbols_missing_daily"] = sorted(
        symbols - set(frames.daily.index.get_level_values("instrument"))
    )
    audit["candidate_symbols_missing_bak"] = sorted(
        symbols - set(frames.bak.index.get_level_values("instrument"))
    )
    audit["audit_sha256"] = canonical_sha256(
        {key: value for key, value in audit.items() if key != "audit_sha256"}
    )
    return audit


def _monthly_decision_ordinals(calendar: pd.DatetimeIndex) -> tuple[int, ...]:
    if calendar.empty or not calendar.is_unique or not calendar.is_monotonic_increasing:
        raise ActionValueError("SELECTION_ALPHA_CALENDAR_INVALID")
    periods = calendar.to_period("M")
    return tuple(
        ordinal
        for ordinal in range(len(calendar) - 1)
        if ordinal == 0 or periods[ordinal] != periods[ordinal - 1]
    )


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
        raise ActionValueError("SELECTION_ALPHA_PATH_SCOPE_INVALID")
    commit = _clean_repository_commit(repository)
    source_code = _source_files(repository)
    identity = open_r8_proxy_sources(candidate_root)
    candidate = DailyCandidate.open(candidate_root)
    if (candidate.calendar[0].date(), candidate.calendar[-1].date(), len(candidate.calendar)) != (
        date(2018, 8, 1),
        date(2026, 8, 31),
        1961,
    ):
        raise ActionValueError("SELECTION_ALPHA_CALENDAR_DRIFT")
    if list(candidate.symbols) != sorted(candidate.symbols) or len(candidate.symbols) != 5_144:
        raise ActionValueError("SELECTION_ALPHA_POPULATION_DRIFT")
    authority = open_adj_factor_restatement_authority(
        candidate_root=candidate.root,
        expected_candidate_manifest_sha256=R8_MANIFEST_FILE_SHA256,
        expected_authority_canonical_sha256=EXPECTED_ADJ_FACTOR_RESTATEMENT_AUTHORITY_SHA256,
    )
    restatement = audit_candidate_adj_factor_restatement(candidate, authority)
    candidate_identity = canonical_sha256(
        {
            "candidate_manifest": identity.candidate.manifest_reference,
            "candidate_dataset_manifest_sha256": identity.candidate.dataset_sha256,
        }
    )
    print(json.dumps({"stage": "SOURCE_FACTOR_PREFLIGHT", "outcomes_read": False}), flush=True)
    factors = _audit_qlib_adjusted_factor_integrity(
        candidate,
        symbols=candidate.symbols,
        start=candidate.calendar[0].date(),
        end=candidate.calendar[-1].date(),
        candidate_source_sha256=candidate_identity,
    )
    candidate.bars(BENCHMARK)
    frames = read_proxy_frames(identity)
    proxy_audit = _proxy_source_audit(candidate, identity, frames)
    if (
        not factors.get("coverage_complete")
        or not restatement.get("coverage_complete")
    ):
        raise ActionValueError("SELECTION_ALPHA_SOURCE_PREFLIGHT_FAILED")
    if _source_files(repository) != source_code:
        raise ActionValueError("SELECTION_ALPHA_CODE_DRIFT")
    decisions = _monthly_decision_ordinals(candidate.calendar)
    request: dict[str, Any] = {
        "schema_version": REQUEST_SCHEMA,
        "contract": CONTRACT,
        "contract_sha256": CONTRACT_SHA256,
        "repository_commit": commit,
        "repository_root": repository.as_posix(),
        "source_code": source_code,
        "environment": _environment_identity(),
        "timing_root": root.as_posix(),
        "candidate_root": candidate.root.as_posix(),
        "candidate_manifest": identity.candidate.manifest_reference,
        "candidate_dataset_manifest_sha256": identity.candidate.dataset_sha256,
        "candidate_deployment_content_sha256": identity.candidate.manifest[
            "deployment_content_sha256"
        ],
        "symbols": candidate.symbols,
        "calendar": [str(value.date()) for value in candidate.calendar],
        "decision_ordinals": decisions,
        "decision_dates": [str(candidate.calendar[value].date()) for value in decisions],
        "source_data": candidate.references,
        "factor_inventory_source": identity.inventory_reference,
        "daily_basic_source": identity.daily_basic_reference,
        "bak_basic_source": identity.bak_basic_reference,
        "proxy_source_audit": proxy_audit,
        "restatement_authority": authority.authority_reference,
        "restatement_audit": restatement,
        "factor_audit": factors,
        "qlib_contract_sha256": QLIB_ADJUSTED_FACTOR_CONTRACT_SHA256,
        "source_preflight_complete": True,
        "outcomes_read": False,
        "database_read": False,
        "database_write": False,
        "network_accessed": False,
        "live_market_read": False,
        "runtime_action_performed": False,
        "service_process_control_performed": False,
        "other_module_write": False,
        "research_worker_processes_used": True,
    }
    request["request_sha256"] = canonical_sha256(request)
    path = root / "research" / FOLDER / "requests" / f"{request['request_sha256']}.json"
    publish_json(path, request)
    return path


def load_request(path: Path) -> dict[str, Any]:
    request = read_json(path)
    digest = canonical_sha256(
        {key: value for key, value in request.items() if key != "request_sha256"}
    )
    if (
        request.get("request_sha256") != digest
        or request.get("schema_version") != REQUEST_SCHEMA
        or request.get("contract_sha256") != CONTRACT_SHA256
        or canonical_sha256(request.get("contract")) != CONTRACT_SHA256
    ):
        raise ActionValueError("SELECTION_ALPHA_REQUEST_DRIFT")
    root = Path(request["timing_root"]).resolve()
    repository = Path(request["repository_root"]).resolve()
    expected = root / "research" / FOLDER / "requests" / f"{digest}.json"
    if path.resolve() != expected or repository != Path(__file__).resolve().parents[3]:
        raise ActionValueError("SELECTION_ALPHA_REQUEST_SCOPE_DRIFT")
    if request.get("environment") != _environment_identity():
        raise ActionValueError("SELECTION_ALPHA_ENVIRONMENT_DRIFT")
    false_fields = (
        "outcomes_read",
        "database_read",
        "database_write",
        "network_accessed",
        "live_market_read",
        "runtime_action_performed",
        "service_process_control_performed",
        "other_module_write",
    )
    if (
        request.get("source_preflight_complete") is not True
        or request.get("candidate_manifest", {}).get("sha256") != R8_MANIFEST_FILE_SHA256
        or request.get("candidate_dataset_manifest_sha256") != R8_DATASET_SHA256
        or request.get("qlib_contract_sha256") != QLIB_ADJUSTED_FACTOR_CONTRACT_SHA256
        or any(request.get(key) is not False for key in false_fields)
    ):
        raise ActionValueError("SELECTION_ALPHA_REQUEST_BOUNDARY_DRIFT")
    if tuple(request.get("decision_ordinals") or ()) != _monthly_decision_ordinals(
        pd.DatetimeIndex(pd.to_datetime(request["calendar"]))
    ):
        raise ActionValueError("SELECTION_ALPHA_DECISION_SCHEDULE_DRIFT")
    for audit_name in ("proxy_source_audit", "restatement_audit", "factor_audit"):
        audit = request[audit_name]
        if audit.get("audit_sha256") != canonical_sha256(
            {key: value for key, value in audit.items() if key != "audit_sha256"}
        ):
            raise ActionValueError("SELECTION_ALPHA_PREFLIGHT_AUDIT_DRIFT", audit=audit_name)
    for reference in (
        request["candidate_manifest"],
        request["factor_inventory_source"],
        request["daily_basic_source"],
        request["bak_basic_source"],
        request["restatement_authority"],
    ):
        check_ref(reference)
    for group in ("source_code", "source_data"):
        for reference in request[group].values():
            check_ref(reference)
    if _source_files(repository) != request["source_code"]:
        raise ActionValueError("SELECTION_ALPHA_CODE_DRIFT")
    return request


def _symbol_frame(frame: pd.DataFrame, symbol: str, source: str) -> pd.DataFrame:
    try:
        return frame.xs(symbol, level="instrument", drop_level=True)
    except KeyError:
        # A proxy source may legitimately have no observation for an eligible
        # PIT symbol.  Preserve that coverage gap as UNKNOWN instead of either
        # dropping the symbol or turning missingness into a negative signal.
        # File/schema/identity failures remain fail-closed in read_proxy_frames.
        return pd.DataFrame(
            index=pd.DatetimeIndex([], name="datetime"),
            columns=frame.columns,
            dtype=float,
        )


def _bar_record(records: Sequence[Mapping[str, Any]], ordinal: int) -> dict[str, Any]:
    return dict(records[ordinal])


def _screen_state(pass_mask: np.ndarray, unknown_mask: np.ndarray, ordinal: int) -> str:
    if bool(unknown_mask[ordinal]):
        return "UNKNOWN"
    return "PASS" if bool(pass_mask[ordinal]) else "FAIL"


def _terminal_net_mtm(account: Account, bar: Mapping[str, Any]) -> tuple[float | None, float | None]:
    if not account.units:
        return float(account.cash), 0.0
    if not positive(bar.get("close")) or not positive(bar.get("factor")):
        return None, None
    notional = account.units * dec(bar["close"]) * dec(bar["factor"])
    exit_cost = fee(notional, "SELL")
    return float(account.cash + notional - exit_cost), float(exit_cost)


def _path_max_drawdown(
    account: Account,
    bars: pd.DataFrame,
    *,
    start: int,
    end: int,
) -> float | None:
    if not account.units:
        return 0.0
    raw = pd.to_numeric(bars.close.iloc[start : end + 1], errors="coerce").to_numpy(float)
    factor = pd.to_numeric(bars.factor.iloc[start : end + 1], errors="coerce").to_numpy(float)
    adjusted = raw * factor
    if not len(adjusted) or not np.isfinite(adjusted).all() or (adjusted <= 0).any():
        return None
    wealth = float(account.cash) + float(account.units) * adjusted
    wealth = np.r_[float(CAPITAL), wealth]
    peaks = np.maximum.accumulate(wealth)
    return float(np.min(wealth / peaks - 1.0))


def _sellability(
    records: Sequence[Mapping[str, Any]],
    *,
    nominal: int,
) -> tuple[str, int | None, str | None]:
    statuses: list[str] = []
    final = min(len(records) - 1, nominal + TERMINAL_SELLABILITY_DEFER)
    for ordinal in range(nominal, final + 1):
        status = execution_status(_bar_record(records, ordinal), "SELL")
        statuses.append(status)
        if status == "EXECUTABLE":
            return "SELLABLE", ordinal - nominal, status
    return "UNAVAILABLE_WITHIN_DEFER", None, statuses[-1] if statuses else None


def _replay_symbol_task(
    symbol: str,
    bars: pd.DataFrame,
    daily: pd.DataFrame,
    bak: pd.DataFrame,
    decision_ordinals: Sequence[int],
) -> tuple[str, pd.DataFrame, pd.DataFrame]:
    ready = np.ones(len(bars), dtype=bool)
    masks, _, unknown = screen_masks_for_symbol(
        dates=bars.index,
        pit_active=bars.pit_active.to_numpy(bool),
        feature_ready=ready,
        daily=daily,
        bak=bak,
    )
    records = bars.to_dict("records")
    decisions: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []
    for decision in decision_ordinals:
        states = {
            screen: _screen_state(masks[screen], unknown[screen], decision)
            for screen in SCREEN_IDS
        }
        decisions.append(
            {
                "symbol": symbol,
                "decision_ordinal": decision,
                "decision_date": str(bars.index[decision].date()),
                **{f"{screen}_state": state for screen, state in states.items()},
            }
        )
        if states[SCREEN_IDS[0]] != "PASS":
            continue
        entry = decision + 1
        account = Account(cash=CAPITAL)
        entry_result = execute(
            account,
            symbol=symbol,
            bar=_bar_record(records, entry),
            ordinal=entry,
            side="BUY",
            reference=Decimal(1),
            guarded=False,
        )
        entry_filled = entry_result["status"] == "FILLED"
        for horizon in HORIZONS:
            terminal = entry + horizon
            base = {
                "symbol": symbol,
                "decision_ordinal": decision,
                "decision_date": str(bars.index[decision].date()),
                "entry_ordinal": entry,
                "entry_date": str(bars.index[entry].date()),
                "horizon": horizon,
                "entry_status": entry_result["status"],
                "entry_filled": entry_filled,
                "buy_fee_cny": float(account.fees),
                **{f"{screen}_state": state for screen, state in states.items()},
            }
            if terminal >= len(bars):
                events.append(
                    {
                        **base,
                        "maturity_status": "HORIZON_NOT_MATURE",
                        "terminal_ordinal": None,
                        "terminal_date": None,
                        "net_mtm_after_estimated_exit_cost_cny": None,
                        "net_return": None,
                        "max_drawdown": None,
                        "estimated_exit_fee_cny": None,
                        "terminal_sellability": None,
                        "sellable_defer_sessions": None,
                        "sellability_last_status": None,
                    }
                )
                continue
            terminal_bar = _bar_record(records, terminal)
            terminal_wealth, estimated_exit_fee = _terminal_net_mtm(account, terminal_bar)
            if terminal_wealth is None:
                maturity = "TERMINAL_VALUATION_UNKNOWN"
                terminal_return = None
            else:
                maturity = "EVALUATED"
                terminal_return = terminal_wealth / float(CAPITAL) - 1.0
            if entry_filled:
                sellability, defer, last_status = _sellability(records, nominal=terminal)
                drawdown = _path_max_drawdown(account, bars, start=entry, end=terminal)
            else:
                sellability, defer, last_status = "NOT_APPLICABLE_CASH", None, None
                drawdown = 0.0
            events.append(
                {
                    **base,
                    "maturity_status": maturity,
                    "terminal_ordinal": terminal,
                    "terminal_date": str(bars.index[terminal].date()),
                    "net_mtm_after_estimated_exit_cost_cny": terminal_wealth,
                    "net_return": terminal_return,
                    "max_drawdown": drawdown,
                    "estimated_exit_fee_cny": estimated_exit_fee,
                    "terminal_sellability": sellability,
                    "sellable_defer_sessions": defer,
                    "sellability_last_status": last_status,
                }
            )
    return symbol, pd.DataFrame(events), pd.DataFrame(decisions)


def _ordered_replays(
    inputs: Iterable[tuple[str, pd.DataFrame, pd.DataFrame, pd.DataFrame, Sequence[int]]],
    *,
    worker_count: int,
    max_in_flight: int,
) -> Iterator[tuple[str, pd.DataFrame, pd.DataFrame]]:
    if worker_count < 1 or max_in_flight < worker_count:
        raise ActionValueError("SELECTION_ALPHA_PARALLEL_CONTRACT_INVALID")
    if worker_count == 1:
        for args in inputs:
            yield _replay_symbol_task(*args)
        return
    executor = ProcessPoolExecutor(
        max_workers=worker_count,
        mp_context=multiprocessing.get_context("spawn"),
    )
    pending: dict[int, tuple[str, Future[Any]]] = {}
    iterator = iter(inputs)
    submitted = consumed = 0
    exhausted = False
    try:
        while not exhausted or pending:
            while not exhausted and len(pending) < max_in_flight:
                try:
                    args = next(iterator)
                except StopIteration:
                    exhausted = True
                    break
                pending[submitted] = (args[0], executor.submit(_replay_symbol_task, *args))
                submitted += 1
            if consumed in pending:
                expected, future = pending.pop(consumed)
                result = future.result()
                if result[0] != expected:
                    raise ActionValueError("SELECTION_ALPHA_WORKER_ORDER_DRIFT")
                yield result
                consumed += 1
    finally:
        for _, future in pending.values():
            future.cancel()
        executor.shutdown(wait=True, cancel_futures=True)


def _concat(frames: Iterable[pd.DataFrame]) -> pd.DataFrame:
    nonempty = [frame for frame in frames if not frame.empty]
    return pd.concat(nonempty, ignore_index=True) if nonempty else pd.DataFrame()


def _seal(root: Path, names: list[str], *, request_sha256: str) -> dict[str, Any]:
    value = {
        "schema_version": MANIFEST_SCHEMA,
        "request_sha256": request_sha256,
        "files": {name: file_reference(root / name) for name in names},
    }
    value["manifest_sha256"] = canonical_sha256(value)
    publish_json(root / "manifest.json", value)
    return value


def inspect(root: Path, *, request_sha256: str | None = None) -> dict[str, Any]:
    root = root.resolve()
    manifest = read_json(root / "manifest.json")
    digest = canonical_sha256(
        {key: value for key, value in manifest.items() if key != "manifest_sha256"}
    )
    if (
        manifest.get("schema_version") != MANIFEST_SCHEMA
        or manifest.get("manifest_sha256") != digest
        or (request_sha256 is not None and manifest.get("request_sha256") != request_sha256)
    ):
        raise ActionValueError("SELECTION_ALPHA_MANIFEST_DRIFT")
    for name, reference in manifest["files"].items():
        target = (root / name).resolve()
        if not target.is_relative_to(root) or Path(reference["path"]).resolve() != target:
            raise ActionValueError("SELECTION_ALPHA_MANIFEST_SCOPE_DRIFT")
        check_ref(reference)
    return {
        "status": "VERIFIED",
        "bundle": root.as_posix(),
        "manifest_sha256": manifest["manifest_sha256"],
        "request_sha256": manifest["request_sha256"],
    }


def _bootstrap_monthly(values: np.ndarray) -> dict[str, Any]:
    values = np.asarray(values, dtype=float)
    finite = np.isfinite(values)
    values = values[finite]
    if not len(values):
        return {
            "status": "UNAVAILABLE",
            "reason": "NO_COMPLETE_COMMON_MONTH",
            "power_status": "NOT_COMPUTABLE",
        }
    rng = np.random.default_rng(BOOTSTRAP_SEED)
    blocks = int(np.ceil(len(values) / BOOTSTRAP_BLOCK_MONTHS))
    starts = rng.integers(0, len(values), size=(BOOTSTRAP_REPLICATES, blocks))
    indexes = (
        (starts[..., None] + np.arange(BOOTSTRAP_BLOCK_MONTHS)) % len(values)
    ).reshape(BOOTSTRAP_REPLICATES, -1)[:, : len(values)]
    estimates = values[indexes].mean(axis=1) * 10_000.0
    alpha = 0.05 / FAMILY_SIZE
    family_interval = [
        float(np.quantile(estimates, alpha / 2)),
        float(np.quantile(estimates, 1 - alpha / 2)),
    ]
    evidence = (
        "SUPPORTED"
        if family_interval[0] > 0
        else "NEGATIVE"
        if family_interval[1] < 0
        else "INCONCLUSIVE"
    )
    return {
        "status": "ESTIMATED",
        "estimand": "COMMON_MONTH_EQUAL_WEIGHT_FORWARD_RETURN_DIFFERENCE_BPS",
        "observed_months": int(len(values)),
        "point_bps": float(values.mean() * 10_000.0),
        "nominal_95_interval_bps": [
            float(np.quantile(estimates, 0.025)),
            float(np.quantile(estimates, 0.975)),
        ],
        "familywise_interval_bps": family_interval,
        "family_size": FAMILY_SIZE,
        "economic_threshold_bps": 0.0,
        "evidence_state": evidence,
        "power_status": "NOT_COMPUTABLE",
        "power_reason": "NO_FROZEN_SELECTION_ORACLE_SCALE",
    }


def _cohort_monthly(events: pd.DataFrame, decisions: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for horizon in HORIZONS:
        horizon_events = events.loc[events.horizon.eq(horizon)]
        for screen in SCREEN_IDS:
            state = f"{screen}_state"
            for decision_date, decision_group in decisions.groupby("decision_date", sort=True):
                expected = int(decision_group[state].eq("PASS").sum())
                unknown = int(decision_group[state].eq("UNKNOWN").sum())
                selected = horizon_events.loc[
                    horizon_events.decision_date.eq(decision_date)
                    & horizon_events[state].eq("PASS")
                ]
                if expected == 0:
                    status = "EMPTY"
                elif len(selected) != expected:
                    status = "EVENT_MATERIALIZATION_MISSING"
                elif selected.maturity_status.eq("HORIZON_NOT_MATURE").any():
                    status = "HORIZON_NOT_MATURE"
                elif not selected.net_return.notna().all():
                    status = "COHORT_VALUATION_INCOMPLETE"
                else:
                    status = "COMPLETE"
                complete = status == "COMPLETE"
                rows.append(
                    {
                        "decision_date": decision_date,
                        "horizon": horizon,
                        "screen_id": screen,
                        "status": status,
                        "expected_count": expected,
                        "unknown_count": unknown,
                        "event_count": int(len(selected)),
                        "entry_filled_count": int(selected.entry_filled.sum()) if len(selected) else 0,
                        "cash_no_entry_count": int((~selected.entry_filled).sum()) if len(selected) else 0,
                        "terminal_valued_count": int(selected.net_return.notna().sum()) if len(selected) else 0,
                        "mean_net_return": float(selected.net_return.mean()) if complete else None,
                        "median_net_return": float(selected.net_return.median()) if complete else None,
                        "mean_max_drawdown": (
                            float(selected.max_drawdown.mean())
                            if complete and selected.max_drawdown.notna().all()
                            else None
                        ),
                    }
                )
    return pd.DataFrame(rows)


def _benchmark_horizon_returns(
    benchmark: pd.DataFrame,
    decision_ordinals: Sequence[int],
) -> dict[str, Any]:
    values: dict[str, list[float]] = {str(value): [] for value in HORIZONS}
    close = pd.to_numeric(benchmark.close, errors="coerce").to_numpy(float)
    for decision in decision_ordinals:
        entry = decision + 1
        for horizon in HORIZONS:
            terminal = entry + horizon
            if terminal >= len(close):
                continue
            if np.isfinite(close[entry]) and np.isfinite(close[terminal]) and close[entry] > 0:
                values[str(horizon)].append(float(close[terminal] / close[entry] - 1.0))
    return {
        horizon: {
            "observation_count": len(items),
            "mean_forward_return": float(np.mean(items)) if items else None,
            "median_forward_return": float(np.median(items)) if items else None,
        }
        for horizon, items in values.items()
    }


def _build_report(
    *,
    events: pd.DataFrame,
    decisions: pd.DataFrame,
    benchmark: pd.DataFrame,
    decision_ordinals: Sequence[int],
) -> tuple[dict[str, Any], pd.DataFrame]:
    cohort = _cohort_monthly(events, decisions)
    formal: list[dict[str, Any]] = []
    primary = cohort.loc[cohort.horizon.eq(PRIMARY_HORIZON)]
    for name, left, right in COMPARISONS:
        left_rows = primary.loc[
            primary.screen_id.eq(left) & primary.status.eq("COMPLETE"),
            ["decision_date", "mean_net_return"],
        ].rename(columns={"mean_net_return": "left"})
        right_rows = primary.loc[
            primary.screen_id.eq(right) & primary.status.eq("COMPLETE"),
            ["decision_date", "mean_net_return"],
        ].rename(columns={"mean_net_return": "right"})
        paired = left_rows.merge(right_rows, on="decision_date", how="inner", validate="one_to_one")
        inference = _bootstrap_monthly((paired.left - paired.right).to_numpy(float))
        formal.append(
            {
                "comparison": name,
                "left_screen": left,
                "right_screen": right,
                "horizon": PRIMARY_HORIZON,
                **inference,
            }
        )
    diagnostic: list[dict[str, Any]] = []
    for horizon in HORIZONS:
        for screen in SCREEN_IDS:
            selected = events.loc[
                events.horizon.eq(horizon)
                & events[f"{screen}_state"].eq("PASS")
                & events.net_return.notna()
            ]
            diagnostic.append(
                {
                    "screen_id": screen,
                    "horizon": horizon,
                    "diagnostic_only": horizon != PRIMARY_HORIZON,
                    "evaluated_event_count": int(len(selected)),
                    "mean_event_return": float(selected.net_return.mean()) if len(selected) else None,
                    "median_event_return": float(selected.net_return.median()) if len(selected) else None,
                    "event_win_rate": float(selected.net_return.gt(0).mean()) if len(selected) else None,
                    "mean_event_max_drawdown": (
                        float(selected.max_drawdown.mean())
                        if len(selected) and selected.max_drawdown.notna().all()
                        else None
                    ),
                    "entry_fill_rate": float(selected.entry_filled.mean()) if len(selected) else None,
                    "terminal_sellable_rate": (
                        float(selected.terminal_sellability.eq("SELLABLE").mean())
                        if len(selected)
                        else None
                    ),
                }
            )
    evidence = [row.get("evidence_state") for row in formal]
    result_class = (
        "EXPLORATORY_SELECTION_ALPHA_SUPPORTED"
        if "SUPPORTED" in evidence
        else "EXPLORATORY_SELECTION_ALPHA_NO_SUPPORTED_SCREEN"
    )
    report = {
        "schema_version": REPORT_SCHEMA,
        "result_class": result_class,
        "selected_trial_count": 0,
        "selected_for_live": 0,
        "strict_financial_pit_claimed": False,
        "formal_family": formal,
        "screen_horizon_diagnostics": diagnostic,
        "cohort_status_counts": (
            cohort.groupby(["screen_id", "horizon", "status"], sort=True)
            .size()
            .rename("count")
            .reset_index()
            .to_dict("records")
        ),
        "decision_record_count": int(len(decisions)),
        "decision_month_count": int(decisions.decision_date.nunique()),
        "event_count": int(len(events)),
        "benchmark_context": {
            "symbol": BENCHMARK,
            "diagnostic_only": True,
            "forward_returns": _benchmark_horizon_returns(
                benchmark, decision_ordinals
            ),
        },
        "stop_rule": (
            "DO_NOT_SCAN_EXISTING_U0_U1_U2_THRESHOLDS_OR_PROXY_FIELD_COMBINATIONS"
            if "SUPPORTED" not in evidence
            else "FREEZE_SUPPORTED_SCREEN_AS_FUTURE_PT_NEXT_029_HYPOTHESIS_ONLY"
        ),
    }
    return report, cohort


def _validate_population(candidate: DailyCandidate, request: Mapping[str, Any]) -> None:
    if (
        list(candidate.symbols) != request["symbols"]
        or [str(value.date()) for value in candidate.calendar] != request["calendar"]
    ):
        raise ActionValueError("SELECTION_ALPHA_POPULATION_DRIFT")


def _bar_inputs(
    candidate: DailyCandidate,
    request: Mapping[str, Any],
    symbols: Iterable[str],
    frames: ProxyFrames,
) -> Iterator[tuple[str, pd.DataFrame, pd.DataFrame, pd.DataFrame, Sequence[int]]]:
    for symbol in symbols:
        bars = candidate.bars(symbol)
        for field in (
            "open",
            "high",
            "low",
            "close",
            "volume",
            "factor",
            "up_limit_price",
            "down_limit_price",
        ):
            key = f"{symbol}:{field}"
            if candidate.references[key] != request["source_data"][key]:
                raise ActionValueError("SELECTION_ALPHA_BAR_SOURCE_DRIFT", symbol=symbol)
        yield (
            symbol,
            bars,
            _symbol_frame(frames.daily, symbol, "daily_basic"),
            _symbol_frame(frames.bak, symbol, "bak_basic"),
            request["decision_ordinals"],
        )


def run(path: Path) -> dict[str, Any]:
    request = load_request(path)
    digest = request["request_sha256"]
    root = Path(request["timing_root"]) / "research" / FOLDER
    bundle = root / "bundles" / digest
    with _exclusive_file_lock(root / "locks" / f"{digest}.lock"):
        if (bundle / "manifest.json").exists():
            return {**inspect(bundle, request_sha256=digest), "status": "ALREADY_MATERIALIZED"}
        candidate = DailyCandidate.open(Path(request["candidate_root"]))
        identity = open_r8_proxy_sources(candidate.root)
        _validate_population(candidate, request)
        frames = read_proxy_frames(identity)
        if _proxy_source_audit(candidate, identity, frames) != request["proxy_source_audit"]:
            raise ActionValueError("SELECTION_ALPHA_PROXY_SOURCE_DRIFT")
        print(
            json.dumps({"stage": "RUN_AFTER_FULL_SOURCE_PREFLIGHT", "outcomes_read": True}),
            flush=True,
        )
        chunks: list[dict[str, Any]] = []
        for offset in range(0, len(candidate.symbols), CHUNK_SIZE):
            chunk = root / "chunks" / digest / f"{offset // CHUNK_SIZE:04d}"
            if (chunk / "manifest.json").exists():
                inspect(chunk, request_sha256=digest)
            else:
                selected = candidate.symbols[offset : offset + CHUNK_SIZE]
                event_frames: list[pd.DataFrame] = []
                decision_frames: list[pd.DataFrame] = []
                inputs = _bar_inputs(candidate, request, selected, frames)
                for _, events, decisions in _ordered_replays(
                    inputs,
                    worker_count=WORKER_COUNT,
                    max_in_flight=MAX_IN_FLIGHT,
                ):
                    event_frames.append(events)
                    decision_frames.append(decisions)
                publish_frame(chunk / "events.parquet", _concat(event_frames))
                publish_frame(chunk / "decisions.parquet", _concat(decision_frames))
                _seal(
                    chunk,
                    ["events.parquet", "decisions.parquet"],
                    request_sha256=digest,
                )
            chunks.append(file_reference(chunk / "manifest.json"))
            print(
                json.dumps(
                    {
                        "stage": "CHUNK_COMPLETE",
                        "symbols_complete": min(offset + CHUNK_SIZE, len(candidate.symbols)),
                        "total": len(candidate.symbols),
                    }
                ),
                flush=True,
            )
        event_frames = []
        decision_frames = []
        for reference in chunks:
            check_ref(reference)
            chunk = Path(reference["path"]).parent
            inspect(chunk, request_sha256=digest)
            event_frames.append(pd.read_parquet(chunk / "events.parquet"))
            decision_frames.append(pd.read_parquet(chunk / "decisions.parquet"))
        events = _concat(event_frames)
        decisions = _concat(decision_frames)
        benchmark = candidate.bars(BENCHMARK)
        for field in ("open", "high", "low", "close", "volume"):
            key = f"{BENCHMARK}:{field}"
            if candidate.references[key] != request["source_data"][key]:
                raise ActionValueError("SELECTION_ALPHA_BENCHMARK_SOURCE_DRIFT")
        report, cohort = _build_report(
            events=events,
            decisions=decisions,
            benchmark=benchmark,
            decision_ordinals=request["decision_ordinals"],
        )
        load_request(path)
        publish_json(bundle / "request.json", request)
        publish_json(bundle / "source_audit.json", request["proxy_source_audit"])
        publish_frame(bundle / "events.parquet", events)
        publish_frame(bundle / "decisions.parquet", decisions)
        publish_frame(bundle / "cohort_monthly.parquet", cohort)
        publish_json(bundle / "report.json", report)
        receipt = {
            "schema_version": RECEIPT_SCHEMA,
            "request_sha256": digest,
            "chunks": chunks,
            "symbol_count": len(candidate.symbols),
            "decision_month_count": len(request["decision_ordinals"]),
            "event_count": int(len(events)),
            "screen_count": len(SCREEN_IDS),
            "formal_family_size": FAMILY_SIZE,
            "outcomes_read_stage": "RUN_AFTER_FULL_SOURCE_PREFLIGHT",
            "source_preflight_complete": True,
            "result_class": report["result_class"],
            "selected_trial_count": 0,
            "selected_for_live": 0,
            "database_read": False,
            "database_write": False,
            "network_accessed": False,
            "live_market_read": False,
            "runtime_action_performed": False,
            "service_process_control_performed": False,
            "other_module_write": False,
            "research_worker_processes_used": True,
            "parallel_execution": CONTRACT["parallel_execution"],
            "strict_financial_pit_claimed": False,
            "corporate_action_authority_read": False,
            "account_economics_simulated": False,
            "broker_account_clearing": False,
        }
        publish_json(bundle / "receipt.json", receipt)
        _seal(
            bundle,
            [
                "request.json",
                "source_audit.json",
                "events.parquet",
                "decisions.parquet",
                "cohort_monthly.parquet",
                "report.json",
                "receipt.json",
            ],
            request_sha256=digest,
        )
        return inspect(bundle, request_sha256=digest)


def verify_parallel(path: Path, *, size: int = 8) -> dict[str, Any]:
    request = load_request(path)
    candidate = DailyCandidate.open(Path(request["candidate_root"]))
    identity = open_r8_proxy_sources(candidate.root)
    frames = read_proxy_frames(identity)
    symbols = candidate.symbols[:size]
    inputs = list(_bar_inputs(candidate, request, symbols, frames))

    def digest(result: tuple[str, pd.DataFrame, pd.DataFrame]) -> str:
        symbol, events, decisions = result
        return canonical_sha256(
            {
                "symbol": symbol,
                "events": hashlib.sha256(
                    events.to_json(orient="table", index=False, double_precision=15).encode()
                ).hexdigest(),
                "decisions": hashlib.sha256(
                    decisions.to_json(orient="table", index=False, double_precision=15).encode()
                ).hexdigest(),
            }
        )

    sequential = {
        result[0]: digest(result)
        for result in _ordered_replays(inputs, worker_count=1, max_in_flight=1)
    }
    parallel = {
        result[0]: digest(result)
        for result in _ordered_replays(
            inputs,
            worker_count=WORKER_COUNT,
            max_in_flight=MAX_IN_FLIGHT,
        )
    }
    if sequential != parallel:
        raise ActionValueError("SELECTION_ALPHA_PARALLEL_RESULT_DRIFT")
    audit = {
        "schema_version": "position_timing_selection_alpha_parallel_verification_v1",
        "request_sha256": request["request_sha256"],
        "symbols": symbols,
        "result_sha256_by_symbol": sequential,
        "artifact_written": False,
    }
    return {"status": "EXACT", **audit, "audit_sha256": canonical_sha256(audit)}


def main() -> None:
    parser = argparse.ArgumentParser(__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare_parser = commands.add_parser("prepare")
    prepare_parser.add_argument("--timing-root", type=Path, required=True)
    prepare_parser.add_argument("--repository-root", type=Path, required=True)
    prepare_parser.add_argument("--candidate-root", type=Path, required=True)
    commands.add_parser("run").add_argument("--request", type=Path, required=True)
    verify_parser = commands.add_parser("verify-parallel")
    verify_parser.add_argument("--request", type=Path, required=True)
    verify_parser.add_argument("--size", type=int, default=8)
    commands.add_parser("inspect").add_argument("--bundle", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "prepare":
        print(
            prepare(
                timing_root=args.timing_root,
                repository_root=args.repository_root,
                candidate_root=args.candidate_root,
            )
        )
    elif args.command == "run":
        print(json.dumps(run(args.request), ensure_ascii=False))
    elif args.command == "verify-parallel":
        print(json.dumps(verify_parallel(args.request, size=args.size), ensure_ascii=False))
    else:
        print(json.dumps(inspect(args.bundle), ensure_ascii=False))


if __name__ == "__main__":
    main()
