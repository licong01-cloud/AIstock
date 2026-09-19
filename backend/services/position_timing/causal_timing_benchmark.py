"""Immutable WSL benchmark for PT-NEXT-024 causal timing research."""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import Future, ProcessPoolExecutor
from datetime import date
from decimal import Decimal
import hashlib
import importlib.metadata
import json
from itertools import groupby
import math
import multiprocessing
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping

import numpy as np
import pandas as pd

from .action_value import ActionValueError, market_features
from .action_value_data import BENCHMARK, DailyCandidate, file_reference
from .artifact_store import _exclusive_file_lock
from .causal_timing_contracts import (
    BH, CONTRACT, CONTRACT_SHA256, E0, FEATURE_ORDER, FOLDER,
    MANIFEST_SCHEMA, REPORT_SCHEMA, REQUEST_SCHEMA, RECEIPT_SCHEMA,
    TERMINAL_DATE, TEST_FIRST_EXECUTION, TEST_LAST_DECISION,
)
from .causal_timing_execution import MINUTE_FIELDS, MinuteExecutionSource
from .causal_timing_model import fit_gbdt, fit_ridge, validate_model
from .causal_timing_oracle import (
    event_oracle_from_legacy_fills, labels_from_legacy_fills, restricted_account_oracle,
)
from .causal_timing_replay import replay_symbol
from .contracts import canonical_sha256
from .core_tactical_timing import S1, build_research_features
from .fundamental_screen import R8_DATASET_SHA256, R8_MANIFEST_FILE_SHA256
from .fundamental_timing_benchmark import _clean_repository_commit, _environment_identity
from .pattern_close_cash_benchmark import check_ref, publish_frame, publish_json, read_json
from .pattern_close_cash_replay import CAPITAL
from .pattern_close_cash_report import INDEX_CODES, index_returns
from .policy import component_cost_for_parent_notionals
from .contracts import TriggerSide
from .pattern_universe_benchmark import CandidatePoolMemberships, POOL_IDS, open_candidate_pool_memberships
from .r8_proxy_screen import (
    U0, ProxySourceIdentity, open_r8_proxy_sources, read_proxy_frames,
    screen_masks_for_symbol, source_audit,
)


CHUNK_SIZE = 64
WORKER_COUNT = 8
MAX_IN_FLIGHT = 8
BOOTSTRAP_REPLICATES = 5_000
BOOTSTRAP_BLOCK = 25
BOOTSTRAP_SEED = 20260919
PARENT_BUNDLE_ID = "3f8667817971ae7b10a867d28ac2b1a39d7b635ac49b2e82e0eee1e565c4462c"
PARENT_MANIFEST_FILE_SHA256 = "6e6c5f96a066c8a951d949aec34cc0b54b24ac954fce0c12771161ed81ec540b"
PARENT_MANIFEST_CANONICAL_SHA256 = "1b352b7f8d25cb501df2b48fc4becf0cce8693240d3d33e069f05ce2313d2794"


def _source_files(repository: Path) -> dict[str, dict[str, Any]]:
    names = (
        "backend/services/position_timing/causal_timing_contracts.py",
        "backend/services/position_timing/causal_timing_execution.py",
        "backend/services/position_timing/causal_timing_replay.py",
        "backend/services/position_timing/causal_timing_oracle.py",
        "backend/services/position_timing/causal_timing_model.py",
        "backend/services/position_timing/causal_timing_benchmark.py",
        "backend/services/position_timing/action_value.py",
        "backend/services/position_timing/action_value_data.py",
        "backend/services/position_timing/core_tactical_timing.py",
        "backend/services/position_timing/pattern_close_cash_replay.py",
        "backend/services/position_timing/r8_proxy_screen.py",
    )
    return {name: file_reference(repository / name) for name in names}


def _frame_sha256(frame: pd.DataFrame) -> str:
    return hashlib.sha256(
        frame.to_json(orient="table", index=False, date_format="iso", double_precision=15).encode()
    ).hexdigest()


def _model_environment() -> dict[str, str]:
    value = {
        "lightgbm": importlib.metadata.version("lightgbm"),
        "numpy": np.__version__, "pandas": pd.__version__,
    }
    if value["lightgbm"] != "4.6.0":
        raise ActionValueError("CAUSAL_LIGHTGBM_VERSION_DRIFT", observed=value["lightgbm"])
    return {**value, "model_environment_sha256": canonical_sha256(value)}


def _pool_expectations(identity: ProxySourceIdentity) -> dict[str, dict[str, Any]]:
    sidecars = ((identity.candidate.manifest.get("st_pit_manifest") or {}).get("index_membership_sidecars") or {})
    if set(sidecars) != set(POOL_IDS):
        raise ActionValueError("CAUSAL_POOL_SET_DRIFT")
    return {pool: {"path": spec["path"], "sha256": spec["sha256"], "size_bytes": spec["size"]}
            for pool, spec in sidecars.items()}


def _memberships(candidate: DailyCandidate, identity: ProxySourceIdentity) -> CandidatePoolMemberships:
    return open_candidate_pool_memberships(
        candidate,
        expected_candidate_manifest_sha256=R8_MANIFEST_FILE_SHA256,
        expected_candidate_dataset_sha256=R8_DATASET_SHA256,
        expected_pool_files=_pool_expectations(identity),
    )


def _parent_bundle(timing_root: Path) -> tuple[Path, dict[str, Any]]:
    root = timing_root / "research" / "core_tactical_proxy_v1" / "bundles" / PARENT_BUNDLE_ID
    manifest_path = root / "manifest.json"
    reference = file_reference(manifest_path)
    manifest = read_json(manifest_path)
    if (
        reference["sha256"] != PARENT_MANIFEST_FILE_SHA256
        or manifest.get("manifest_sha256") != PARENT_MANIFEST_CANONICAL_SHA256
        or manifest.get("request_sha256") != PARENT_BUNDLE_ID
    ):
        raise ActionValueError("CAUSAL_PARENT_BUNDLE_DRIFT")
    for child in manifest.get("files", {}).values():
        check_ref(child)
    return root, {"manifest": reference, "manifest_canonical_sha256": manifest["manifest_sha256"]}


def _minute_required_symbols(candidate: DailyCandidate) -> tuple[str, ...]:
    """Symbols whose PIT life intersects the public minute-execution window."""
    start = pd.Timestamp(TEST_FIRST_EXECUTION)
    end = pd.Timestamp(TERMINAL_DATE)
    active = candidate.spans.loc[(candidate.spans.end >= start) & (candidate.spans.start <= end), "symbol"]
    return tuple(sorted(active.unique()))


def _minute_preflight(candidate: DailyCandidate) -> dict[str, Any]:
    root = candidate.root / "components" / "minute_bin_candidate"
    source = MinuteExecutionSource(root)
    missing: list[str] = []
    inventory: list[tuple[str, int]] = []
    required_symbols = _minute_required_symbols(candidate)
    for symbol in required_symbols:
        folder = root / "features" / symbol.lower()
        for field in MINUTE_FIELDS:
            path = folder / f"{field}.1min.bin"
            if not path.is_file() or path.stat().st_size < 8:
                missing.append(f"{symbol}:{field}")
            else:
                inventory.append((path.relative_to(root).as_posix(), path.stat().st_size))
    if missing:
        raise ActionValueError("CAUSAL_MINUTE_PREFLIGHT_INCOMPLETE", count=len(missing), sample=missing[:20])
    value = {
        **source.identity(), "symbol_count": len(required_symbols), "file_count": len(inventory),
        "required_symbols_sha256": canonical_sha256(required_symbols),
        "pre_execution_history_only_symbol_count": len(candidate.symbols) - len(required_symbols),
        "inventory_sha256": canonical_sha256(inventory), "coverage_complete": True,
        "content_identity_authority": "R8_CANDIDATE_DEPLOYMENT_CONTENT_SHA256_PLUS_PATH_SIZE_INVENTORY",
    }
    return {**value, "audit_sha256": canonical_sha256(value)}


def prepare(*, timing_root: Path, repository_root: Path, candidate_root: Path) -> Path:
    repository, root, candidate_root = repository_root.resolve(), timing_root.resolve(), candidate_root.resolve()
    if (
        not timing_root.is_absolute() or not repository_root.is_absolute() or not candidate_root.is_absolute()
        or repository != Path(__file__).resolve().parents[3]
        or root.is_relative_to(repository) or root.is_relative_to(candidate_root)
    ):
        raise ActionValueError("CAUSAL_PATH_SCOPE_INVALID")
    commit = _clean_repository_commit(repository)
    source_code = _source_files(repository)
    candidate = DailyCandidate.open(candidate_root)
    identity = open_r8_proxy_sources(candidate_root)
    memberships = _memberships(candidate, identity)
    if candidate.calendar[0].date() != date(2018, 8, 1) or candidate.calendar[-1].date() != TERMINAL_DATE:
        raise ActionValueError("CAUSAL_DAILY_CALENDAR_DRIFT")
    if identity.candidate.dataset_sha256 != R8_DATASET_SHA256:
        raise ActionValueError("CAUSAL_CANDIDATE_DRIFT")
    parent_path, parent = _parent_bundle(root)
    proxy_frames = read_proxy_frames(identity)
    proxy_audit = source_audit(identity, proxy_frames)
    if proxy_audit.get("audit_sha256") != canonical_sha256({k: v for k, v in proxy_audit.items() if k != "audit_sha256"}):
        raise ActionValueError("CAUSAL_PROXY_AUDIT_DRIFT")
    minute_audit = _minute_preflight(candidate)
    index_spec = identity.candidate.manifest["components"]["index_daily"]
    index_path = (candidate.root / index_spec["path"]).resolve()
    index_ref = file_reference(index_path)
    if index_ref["sha256"] != index_spec["sha256"] or index_ref["size_bytes"] != index_spec["size"]:
        raise ActionValueError("CAUSAL_INDEX_SOURCE_DRIFT")
    if _source_files(repository) != source_code:
        raise ActionValueError("CAUSAL_CODE_DRIFT")
    request: dict[str, Any] = {
        "schema_version": REQUEST_SCHEMA, "contract": CONTRACT, "contract_sha256": CONTRACT_SHA256,
        "repository_commit": commit, "repository_root": repository.as_posix(), "source_code": source_code,
        "environment": _environment_identity(), "model_environment": _model_environment(),
        "timing_root": root.as_posix(),
        "candidate_root": candidate.root.as_posix(), "candidate_manifest": identity.candidate.manifest_reference,
        "candidate_dataset_manifest_sha256": identity.candidate.dataset_sha256,
        "candidate_deployment_content_sha256": identity.candidate.manifest["deployment_content_sha256"],
        "calendar": [str(value.date()) for value in candidate.calendar], "symbols": candidate.symbols,
        "daily_source_data": candidate.references, "pool_sidecars": memberships.references,
        "proxy_source_audit": proxy_audit, "minute_source_audit": minute_audit,
        "index_source": index_ref, "parent_bundle_root": parent_path.as_posix(), "parent_bundle": parent,
        "source_preflight_complete": True, "outcomes_read": False,
        "database_read": False, "database_write": False, "market_network_accessed": False,
        "runtime_action_performed": False, "service_process_control_performed": False,
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
        request.get("request_sha256") != digest or request.get("schema_version") != REQUEST_SCHEMA
        or request.get("contract_sha256") != CONTRACT_SHA256
        or canonical_sha256(request.get("contract")) != CONTRACT_SHA256
        or request.get("source_preflight_complete") is not True or request.get("outcomes_read") is not False
    ):
        raise ActionValueError("CAUSAL_REQUEST_DRIFT")
    root, repository = Path(request["timing_root"]).resolve(), Path(request["repository_root"]).resolve()
    expected = root / "research" / FOLDER / "requests" / f"{digest}.json"
    if path.resolve() != expected or repository != Path(__file__).resolve().parents[3]:
        raise ActionValueError("CAUSAL_REQUEST_SCOPE_DRIFT")
    if request.get("environment") != _environment_identity():
        raise ActionValueError("CAUSAL_ENVIRONMENT_DRIFT")
    if request.get("model_environment") != _model_environment():
        raise ActionValueError("CAUSAL_MODEL_ENVIRONMENT_DRIFT")
    if request.get("candidate_manifest", {}).get("sha256") != R8_MANIFEST_FILE_SHA256:
        raise ActionValueError("CAUSAL_CANDIDATE_MANIFEST_DRIFT")
    for key in ("database_read", "database_write", "market_network_accessed", "runtime_action_performed",
                "service_process_control_performed"):
        if request.get(key) is not False:
            raise ActionValueError("CAUSAL_REQUEST_BOUNDARY_DRIFT", field=key)
    for reference in (request["candidate_manifest"], request["index_source"], request["parent_bundle"]["manifest"]):
        check_ref(reference)
    for group in ("source_code", "daily_source_data", "pool_sidecars"):
        for reference in request[group].values():
            check_ref(reference)
    if _source_files(repository) != request["source_code"]:
        raise ActionValueError("CAUSAL_CODE_DRIFT")
    return request


def _symbol_frame(frame: pd.DataFrame, symbol: str) -> pd.DataFrame:
    try:
        return frame.xs(symbol, level="instrument", drop_level=True)
    except KeyError as exc:
        raise ActionValueError("CAUSAL_PROXY_SYMBOL_MISSING", symbol=symbol) from exc


def _enrollment(
    bars: pd.DataFrame, daily: pd.DataFrame, bak: pd.DataFrame,
    adjusted: pd.DataFrame, pattern: pd.DataFrame, trend: pd.DataFrame, ready: np.ndarray,
) -> tuple[int | None, dict[str, Any]]:
    masks, coverage, unknown = screen_masks_for_symbol(
        dates=bars.index, pit_active=bars.pit_active.to_numpy(bool), feature_ready=ready,
        daily=daily, bak=bak,
    )
    first_decision = int(bars.index.get_loc(pd.Timestamp(TEST_FIRST_EXECUTION))) - 1
    last_decision = int(bars.index.get_loc(pd.Timestamp(TEST_LAST_DECISION)))
    candidates = np.flatnonzero(masks[U0] & (np.arange(len(bars)) >= first_decision)
                                & (np.arange(len(bars)) <= last_decision))
    enrollment = int(candidates[0]) if len(candidates) else None
    boundary = enrollment if enrollment is not None else last_decision
    identity_unknown = bool(unknown[U0][first_decision:boundary + 1].any())
    if identity_unknown:
        enrollment = None
    return enrollment, {
        "status": "ENROLLMENT_UNKNOWN" if identity_unknown else "ENROLLED" if enrollment is not None else "NOT_ENROLLED",
        "enrollment_ordinal": enrollment, "coverage": coverage[U0],
    }


def _legacy_symbol_inputs(
    *, symbol: str, bars: pd.DataFrame, daily: pd.DataFrame, bak: pd.DataFrame,
    benchmark: pd.Series, legacy_fills: pd.DataFrame, include_event_oracle: bool = True,
) -> dict[str, Any]:
    adjusted, pattern, trend, ready = build_research_features(symbol, bars)
    # market_features expects raw+factor and performs the one adjustment itself.
    features = market_features(bars, benchmark).loc[:, list(FEATURE_ORDER)]
    enrollment, audit = _enrollment(bars, daily, bak, adjusted, pattern, trend, ready)
    labels = labels_from_legacy_fills(
        symbol=symbol, bars=bars, adjusted=adjusted, market_features=features, fills=legacy_fills,
    )
    events = (
        event_oracle_from_legacy_fills(symbol=symbol, bars=bars, adjusted=adjusted, fills=legacy_fills)
        if include_event_oracle else pd.DataFrame()
    )
    return {"adjusted": adjusted, "pattern": pattern, "trend": trend, "ready": ready,
            "features": features, "enrollment": enrollment, "audit": audit,
            "labels": labels, "event_oracle": events}


_WORKER_MINUTE: dict[str, MinuteExecutionSource] = {}


def _minute(root: str) -> MinuteExecutionSource:
    if root not in _WORKER_MINUTE:
        _WORKER_MINUTE[root] = MinuteExecutionSource(Path(root))
    return _WORKER_MINUTE[root]


def _replay_task(
    symbol: str, bars: pd.DataFrame, daily: pd.DataFrame, bak: pd.DataFrame,
    benchmark: pd.Series, legacy_fills: pd.DataFrame, models: Mapping[str, Mapping[str, Any]],
    minute_root: str, terminal_ordinal: int,
) -> tuple[str, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any], dict[str, Any] | None]:
    inputs = _legacy_symbol_inputs(
        symbol=symbol, bars=bars, daily=daily, bak=bak, benchmark=benchmark, legacy_fills=legacy_fills,
    )
    if inputs["enrollment"] is None:
        return symbol, pd.DataFrame(), pd.DataFrame(), inputs["labels"], inputs["event_oracle"], inputs["audit"], None
    minute_source = _minute(minute_root)
    days, fills, details = replay_symbol(
        symbol=symbol, bars=bars, adjusted=inputs["adjusted"], pattern=inputs["pattern"],
        trend=inputs["trend"], ready=inputs["ready"], model_features=inputs["features"],
        enrollment_ordinal=inputs["enrollment"], terminal_ordinal=terminal_ordinal,
        minute_source=minute_source, models=models,
    )
    try:
        oracle = restricted_account_oracle(
            symbol=symbol, bars=bars, adjusted=inputs["adjusted"],
            enrollment_ordinal=inputs["enrollment"], terminal_ordinal=terminal_ordinal,
        )
        return symbol, days, fills, inputs["labels"], inputs["event_oracle"], inputs["audit"], oracle
    finally:
        minute_source.clear_symbol_cache(symbol)


def _label_task(
    symbol: str, bars: pd.DataFrame, daily: pd.DataFrame, bak: pd.DataFrame,
    benchmark: pd.Series, legacy_fills: pd.DataFrame,
) -> tuple[str, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    inputs = _legacy_symbol_inputs(
        symbol=symbol, bars=bars, daily=daily, bak=bak,
        benchmark=benchmark, legacy_fills=legacy_fills, include_event_oracle=False,
    )
    return symbol, inputs["labels"], inputs["event_oracle"], inputs["audit"]


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
                pending[submitted] = (args[0], executor.submit(function, *args))
                submitted += 1
            expected, future = pending.pop(consumed)
            result = future.result()
            if result[0] != expected:
                raise ActionValueError("CAUSAL_WORKER_ORDER_DRIFT")
            yield result
            consumed += 1
    finally:
        for _, future in pending.values():
            future.cancel()
        executor.shutdown(wait=True, cancel_futures=True)


def _ordered(
    inputs: Iterable[tuple[Any, ...]], *, worker_count: int,
) -> Iterator[tuple[Any, ...]]:
    yield from _ordered_call(inputs, worker_count=worker_count, function=_replay_task)


def _performance(values: np.ndarray) -> dict[str, Any]:
    valid = np.isfinite(values)
    if not valid.all() or len(values) < 2 or values[0] <= 0:
        return {"status": "UNAVAILABLE"}
    peak = np.maximum.accumulate(values)
    return {
        "status": "COMPLETE", "terminal_nav_cny": float(values[-1]),
        "total_return": float(values[-1] / values[0] - 1),
        "max_drawdown": float(np.min(values / peak - 1)),
    }


def _stock_summaries(
    days: pd.DataFrame, details: list[dict[str, Any]], *, window_start_ordinal: int, terminal_ordinal: int,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    detail_map = {(item["execution_view"], item["policy_id"]): item for item in details}
    for (view, policy), frame in days.groupby(["execution_view", "policy_id"], sort=False):
        ordered = frame.sort_values("ordinal", kind="stable")
        path = np.r_[float(CAPITAL), ordered.nav.to_numpy(float)]
        metrics = _performance(path)
        detail = detail_map[(view, policy)]
        window_sessions = terminal_ordinal - window_start_ordinal + 1
        exposure = ordered.exposure.to_numpy(float)
        tactical_cash = (
            ordered.cash.to_numpy(float) >= float(CAPITAL) * 0.01
        ) & (ordered.virtual_units.to_numpy(float) > 0)
        longest_tactical_cash = max(
            (len(list(run)) for value, run in groupby(tactical_cash) if value),
            default=0,
        )
        invested = np.isfinite(exposure) & (exposure > 0)
        rows.append({
            "symbol": detail["symbol"], "execution_view": view, "policy_id": policy,
            "enrollment_ordinal": detail["enrollment_ordinal"], **metrics,
            "average_exposure": float(np.nansum(exposure) / window_sessions),
            "invested_session_fraction": float(np.sum(exposure > 0) / window_sessions),
            "conditional_exposure": float(np.mean(exposure[invested])) if invested.any() else 0.0,
            "longest_tactical_cash_sessions": int(longest_tactical_cash),
            "turnover_ratio": detail["turnover_ratio"],
            "fees_cny": detail["fees_cny"], "terminal_status": detail["terminal_status"],
            "terminal_cash_cny": detail["terminal_cash_cny"],
            "terminal_virtual_units": detail["terminal_virtual_units"],
            "model_rejected_count": detail["model_rejected_count"],
            "model_unavailable_count": detail["model_unavailable_count"],
        })
    frame = pd.DataFrame(rows)
    bh = frame.loc[frame.policy_id.eq(BH), ["symbol", "execution_view", "terminal_nav_cny", "max_drawdown"]]
    bh = bh.rename(columns={"terminal_nav_cny": "bh_terminal_nav_cny", "max_drawdown": "bh_max_drawdown"})
    frame = frame.merge(bh, on=["symbol", "execution_view"], how="left", validate="many_to_one")
    frame["terminal_excess_cny"] = frame.terminal_nav_cny - frame.bh_terminal_nav_cny
    frame["mdd_improvement"] = frame.max_drawdown - frame.bh_max_drawdown
    frame["joint_success"] = (frame.terminal_excess_cny > 0) & (frame.mdd_improvement > 0)
    return frame


def _joint_block_intervals(
    candidate_nav: np.ndarray, baseline_nav: np.ndarray, *, alpha: float,
) -> dict[str, dict[str, float]]:
    if (len(candidate_nav) < 3 or len(candidate_nav) != len(baseline_nav)
            or not np.isfinite(candidate_nav).all() or not np.isfinite(baseline_nav).all()
            or np.any(candidate_nav <= 0) or np.any(baseline_nav <= 0)):
        return {
            "terminal_excess_bps": {"lower": math.nan, "upper": math.nan},
            "mdd_improvement_bps": {"lower": math.nan, "upper": math.nan},
        }
    paired = np.column_stack((candidate_nav[1:] / candidate_nav[:-1] - 1,
                              baseline_nav[1:] / baseline_nav[:-1] - 1))
    block = min(BOOTSTRAP_BLOCK, len(paired))
    count = math.ceil(len(paired) / block)
    rng = np.random.default_rng(BOOTSTRAP_SEED)
    starts = rng.integers(0, len(paired), size=(BOOTSTRAP_REPLICATES, count))
    indexes = (starts[..., None] + np.arange(block)) % len(paired)
    samples = paired[indexes.reshape(BOOTSTRAP_REPLICATES, -1)[:, :len(paired)]]
    curves = np.concatenate(
        (np.ones((BOOTSTRAP_REPLICATES, 1, 2)), np.cumprod(1.0 + samples, axis=1)),
        axis=1,
    )
    terminal = (curves[:, -1, 0] - curves[:, -1, 1]) * 10_000
    peaks = np.maximum.accumulate(curves, axis=1)
    drawdowns = np.min(curves / peaks - 1.0, axis=1)
    mdd_improvement = (drawdowns[:, 0] - drawdowns[:, 1]) * 10_000

    def interval(values: np.ndarray) -> dict[str, float]:
        lower, upper = np.quantile(values, (alpha / 2, 1 - alpha / 2))
        return {"lower": float(lower), "upper": float(upper)}

    return {"terminal_excess_bps": interval(terminal),
            "mdd_improvement_bps": interval(mdd_improvement)}


def _finite_summary(values: Iterable[Any]) -> dict[str, Any]:
    array = np.asarray(list(values), dtype=float)
    array = array[np.isfinite(array)]
    if not len(array):
        return {"count": 0, "mean": None, "median": None, "quantiles": {}}
    return {
        "count": len(array), "mean": float(array.mean()), "median": float(np.median(array)),
        "quantiles": {
            str(q): float(np.quantile(array, q)) for q in (0.05, 0.25, 0.75, 0.95)
        },
    }


def _oracle_summary(events: pd.DataFrame, rows: list[dict[str, Any]]) -> dict[str, Any]:
    event_complete = events.loc[events.status.eq("MATURED")] if not events.empty else events
    complete = [item for item in rows if item.get("status") == "COMPLETE"]
    best_excess = [
        (item["best_terminal"]["terminal_nav_cny"] - item["bh"]["terminal_nav_cny"])
        / float(CAPITAL) * 10_000
        for item in complete if item.get("bh") is not None
    ]
    constrained: dict[str, Any] = {}
    for key in ("mdd_20", "mdd_30"):
        values = []
        no_path = 0
        for item in complete:
            value = item.get("constrained", {}).get(key)
            if not isinstance(value, dict) or item.get("bh") is None:
                no_path += 1
                continue
            values.append(
                (value["terminal_nav_cny"] - item["bh"]["terminal_nav_cny"])
                / float(CAPITAL) * 10_000
            )
        constrained[key] = {"excess_vs_bh_bps": _finite_summary(values), "no_feasible_path": no_path}
    return {
        "event_oracle": {
            "row_count": len(events), "matured_count": len(event_complete),
            "best_action_value_bps": _finite_summary(
                event_complete.oracle_net_action_value_bps if not event_complete.empty else []
            ),
            "fixed5_action_value_bps": _finite_summary(
                event_complete.fixed5_net_action_value_bps.dropna() if not event_complete.empty else []
            ),
            "best_delay_sessions": _finite_summary(
                event_complete.best_delay_sessions if not event_complete.empty else []
            ),
        },
        "restricted_account_oracle": {
            "status_counts": dict(Counter(item.get("status") for item in rows)),
            "complete_count": len(complete),
            "path_count": _finite_summary(item["path_count"] for item in complete),
            "best_excess_vs_bh_bps": _finite_summary(best_excess),
            "constrained": constrained,
            "policy_access": False,
        },
    }


def _execution_diagnostics(fills: pd.DataFrame) -> dict[str, Any]:
    if fills.empty:
        return {"status": "UNAVAILABLE", "reason": "NO_FILLS"}
    selected = fills.loc[
        fills.status.eq("FILLED") & fills.raw_price.notna()
        & ~fills.authority.eq("TERMINAL_LIQUIDATION")
    ].copy()
    keys = ["symbol", "policy_id", "authority", "decision_ordinal", "execution_ordinal", "side"]
    base = selected.loc[selected.execution_view.eq(E0), keys + ["raw_price", "raw_quantity"]]
    result: dict[str, Any] = {
        "status": "PRICE_PROXY_DIAGNOSTIC_NOT_QUEUE_PROVEN",
        "matching_contract": keys,
    }
    for view in ("MINUTE_CLOSE_PROXY", "SCHEDULED_1000_PROXY"):
        other = selected.loc[selected.execution_view.eq(view), keys + ["raw_price", "raw_quantity"]]
        paired = base.merge(other, on=keys, suffixes=("_daily", "_minute"), validate="one_to_one")
        same_quantity = paired.raw_quantity_daily.eq(paired.raw_quantity_minute)
        values = (
            (paired.loc[same_quantity, "raw_price_minute"]
             / paired.loc[same_quantity, "raw_price_daily"] - 1) * 10_000
        )
        result[view] = {
            "matched_fill_count": len(paired), "same_quantity_count": int(same_quantity.sum()),
            "minute_minus_daily_price_bps": _finite_summary(values),
        }
    return result


def _fill_diagnostics(fills: pd.DataFrame) -> dict[str, Any]:
    if fills.empty:
        return {"status": "UNAVAILABLE", "fill_status_counts": {}}
    counts = fills.groupby(["execution_view", "policy_id", "status"], dropna=False).size()
    recoveries: list[int] = []
    holding_ages: list[int] = []
    for _, group in fills.sort_values("execution_ordinal", kind="stable").groupby(
        ["symbol", "execution_view", "policy_id"], sort=False,
    ):
        entry = group.loc[
            group.authority.eq("COMMON_INITIAL_ENTRY") & group.status.eq("FILLED"),
            "execution_ordinal",
        ]
        if entry.empty:
            continue
        initial = int(entry.iloc[0])
        prior_sell: int | None = None
        for item in group.itertuples():
            if item.status == "FILLED" and item.side == "SELL" and item.authority != "TERMINAL_LIQUIDATION":
                prior_sell = int(item.execution_ordinal)
                holding_ages.append(prior_sell - initial)
            elif item.status == "FILLED" and item.authority == "FIXED_5_SESSION_RECOVERY" and prior_sell is not None:
                recoveries.append(int(item.execution_ordinal) - prior_sell)
    buckets = {
        "LE_20": sum(value <= 20 for value in holding_ages),
        "D21_60": sum(20 < value <= 60 for value in holding_ages),
        "GT_60": sum(value > 60 for value in holding_ages),
    }
    return {
        "status": "COMPLETE",
        "fill_status_counts": {"|".join(map(str, key)): int(value) for key, value in counts.items()},
        "fixed5_recovery_wait_sessions": _finite_summary(recoveries),
        "timing_sell_holding_age_sessions": {"summary": _finite_summary(holding_ages), "buckets": buckets},
    }


def _strata_diagnostics(
    pool_daily: pd.DataFrame, index_returns_by_code: Mapping[str, np.ndarray],
) -> dict[str, Any]:
    selected = pool_daily.loc[
        pool_daily.execution_view.eq(E0) & pool_daily.pool_id.eq("stock_universe")
    ].copy()
    selected["year"] = pd.to_datetime(selected.valuation_date).dt.year
    annual: list[dict[str, Any]] = []
    regimes: list[dict[str, Any]] = []
    csi300 = index_returns_by_code["000300.SH"]
    for policy, group in selected.groupby("policy_id", sort=False):
        ordered = group.sort_values("ordinal", kind="stable")
        for year, period in ordered.groupby("year", sort=True):
            metrics = _performance(period.nav.to_numpy(float))
            annual.append({"policy_id": policy, "year": int(year), **metrics})
        nav = ordered.nav.to_numpy(float)
        returns = np.r_[np.nan, nav[1:] / nav[:-1] - 1]
        index_values = csi300[ordered.ordinal.to_numpy(int)]
        for regime, mask in (
            ("CSI300_UP_OR_FLAT_DAY", index_values >= 0),
            ("CSI300_DOWN_DAY", index_values < 0),
        ):
            valid = mask & np.isfinite(returns) & np.isfinite(index_values)
            regimes.append({
                "policy_id": policy, "regime": regime, "sessions": int(valid.sum()),
                "compounded_return_on_regime_days": (
                    float(np.prod(1 + returns[valid]) - 1) if valid.any() else None
                ),
            })
    return {
        "annual": annual, "market_regime": regimes,
        "holding_age": "SEE_FILL_DIAGNOSTICS",
        "diagnostic_only": True,
    }


def _report(stocks: pd.DataFrame, pool_daily: pd.DataFrame, audits: list[dict[str, Any]],
            models: Mapping[str, Mapping[str, Any]], index_background: Mapping[str, Any]) -> dict[str, Any]:
    comparisons = (("CYCLE5_UNFILTERED_V1", BH), ("CYCLE5_RIDGE_V1", BH),
                   ("CYCLE5_GBDT_V1", BH), ("CYCLE5_UNFILTERED_V1", "LEGACY_S1"))
    formal: list[dict[str, Any]] = []
    alpha = 0.05 / 8
    e0 = pool_daily.loc[(pool_daily.execution_view == E0) & (pool_daily.pool_id == "stock_universe")]
    for candidate, baseline in comparisons:
        left = e0.loc[e0.policy_id.eq(candidate)].sort_values("ordinal")
        right = e0.loc[e0.policy_id.eq(baseline)].sort_values("ordinal")
        merged = left.merge(right, on="ordinal", suffixes=("_candidate", "_baseline"), validate="one_to_one")
        if merged.empty:
            formal.append({
                "candidate": candidate, "baseline": baseline,
                "effect_evidence": "INCOMPLETE_VALUATION",
                "reason": "NO_PAIRED_POOL_PATH",
            })
            continue
        candidate_nav = merged.nav_candidate.to_numpy(float)
        baseline_nav = merged.nav_baseline.to_numpy(float)
        unknown_candidate = int(merged.unknown_account_count_candidate.max())
        unknown_baseline = int(merged.unknown_account_count_baseline.max())
        if (
            unknown_candidate > 0
            or unknown_baseline > 0
            or not np.isfinite(candidate_nav).all()
            or not np.isfinite(baseline_nav).all()
        ):
            formal.append({
                "candidate": candidate, "baseline": baseline,
                "effect_evidence": "INCOMPLETE_VALUATION",
                "candidate_unknown_account_count": unknown_candidate,
                "baseline_unknown_account_count": unknown_baseline,
            })
            continue
        terminal = float(candidate_nav[-1] - baseline_nav[-1])
        mdd_candidate = _performance(merged.nav_candidate.to_numpy(float)).get("max_drawdown")
        mdd_baseline = _performance(merged.nav_baseline.to_numpy(float)).get("max_drawdown")
        nominal = _joint_block_intervals(candidate_nav, baseline_nav, alpha=0.05)
        adjusted = _joint_block_intervals(candidate_nav, baseline_nav, alpha=alpha)
        endpoint_evidence = {
            name: (
                "SUPPORTED" if interval["lower"] > 0
                else "NEGATIVE" if interval["upper"] < 0
                else "INCONCLUSIVE"
            )
            for name, interval in adjusted.items()
        }
        effect = (
            "JOINT_SUPPORTED_EXPLORATORY"
            if all(value == "SUPPORTED" for value in endpoint_evidence.values())
            else "JOINT_NEGATIVE"
            if all(value == "NEGATIVE" for value in endpoint_evidence.values())
            else "INCONCLUSIVE"
        )
        formal.append({
            "candidate": candidate, "baseline": baseline,
            "terminal_difference_cny": terminal,
            "mdd_improvement": (mdd_candidate - mdd_baseline),
            "nominal_intervals": nominal, "adjusted_intervals": adjusted,
            "endpoint_evidence": endpoint_evidence, "effect_evidence": effect,
        })
    summaries = []
    for (view, policy), group in stocks.groupby(["execution_view", "policy_id"], sort=False):
        complete = group.loc[group.status.eq("COMPLETE")]
        terminal_win = complete.terminal_excess_cny > 0
        mdd_win = complete.mdd_improvement > 0
        summaries.append({
            "execution_view": view, "policy_id": policy, "stock_count": len(group),
            "complete_count": len(complete), "terminal_return_median": float((complete.terminal_nav_cny / float(CAPITAL) - 1).median()),
            "max_drawdown_median": float(complete.max_drawdown.median()),
            "joint_success_count": int(complete.joint_success.sum()),
            "joint_success_fraction": float(complete.joint_success.mean()),
            "average_exposure_mean": float(complete.average_exposure.mean()),
            "conditional_exposure_mean": float(complete.conditional_exposure.mean()),
            "turnover_ratio_mean": float(complete.turnover_ratio.mean()),
            "longest_tactical_cash_sessions_median": float(
                complete.longest_tactical_cash_sessions.median()
            ),
            "fees_cny_mean": float(complete.fees_cny.mean()),
            "joint_outcomes": {
                "both_improved": int((terminal_win & mdd_win).sum()),
                "terminal_only": int((terminal_win & ~mdd_win).sum()),
                "mdd_only": int((~terminal_win & mdd_win).sum()),
                "neither": int((~terminal_win & ~mdd_win).sum()),
                "unknown": int(len(group) - len(complete)),
            },
        })
    pool_summaries: list[dict[str, Any]] = []
    for (view, policy, pool), group in pool_daily.groupby(
        ["execution_view", "policy_id", "pool_id"], sort=False,
    ):
        ordered = group.sort_values("ordinal", kind="stable")
        metrics = _performance(ordered.nav.to_numpy(float))
        members = stocks.loc[stocks[f"member_{pool}"] & stocks.execution_view.eq(view)
                             & stocks.policy_id.eq(policy) & stocks.status.eq("COMPLETE")]
        pool_summaries.append({
            "execution_view": view, "policy_id": policy, "pool_id": pool,
            "account_count": int(ordered.expected_account_count.max()),
            "unknown_account_count": int(ordered.unknown_account_count.max()), **metrics,
            "per_stock_terminal_return_median": (
                float((members.terminal_nav_cny / float(CAPITAL) - 1).median()) if not members.empty else None
            ),
            "per_stock_max_drawdown_median": float(members.max_drawdown.median()) if not members.empty else None,
            "joint_success_fraction": float(members.joint_success.mean()) if not members.empty else None,
            "index_background": index_background.get(pool),
        })
    return {
        "schema_version": REPORT_SCHEMA, "result_class": "EXPLORATORY_HYPOTHESIS_GENERATED",
        "selected_for_live": 0, "summaries": summaries, "pool_summaries": pool_summaries,
        "formal_comparisons": formal,
        "enrollment_counts": dict(Counter(item["status"] for item in audits)),
        "pool_member_change_symbol_counts": {
            pool: int(sum(
                item.get("pool_membership_change_counts", {}).get(pool, 0) > 0
                for item in audits
            ))
            for pool in POOL_IDS[1:]
        },
        "index_background": index_background,
        "models": {key: {"model_sha256": value["model_sha256"], "validation": value["validation"]}
                   for key, value in models.items()},
        "claims": {"live_alpha_supported": False, "minute_fill_proven": False, "strict_financial_pit": False},
    }


def _seal(root: Path, names: list[str], *, request_sha256: str) -> dict[str, Any]:
    payload = {"schema_version": MANIFEST_SCHEMA, "request_sha256": request_sha256,
               "files": {name: file_reference(root / name) for name in names}}
    payload["manifest_sha256"] = canonical_sha256(payload)
    publish_json(root / "manifest.json", payload)
    return payload


def inspect(root: Path, *, request_sha256: str | None = None) -> dict[str, Any]:
    manifest = read_json(root / "manifest.json")
    identity = canonical_sha256({key: value for key, value in manifest.items() if key != "manifest_sha256"})
    if manifest.get("schema_version") != MANIFEST_SCHEMA or manifest.get("manifest_sha256") != identity:
        raise ActionValueError("CAUSAL_MANIFEST_DRIFT")
    if request_sha256 is not None and manifest.get("request_sha256") != request_sha256:
        raise ActionValueError("CAUSAL_MANIFEST_REQUEST_DRIFT")
    if set(item.name for item in root.iterdir()) != set(manifest["files"]) | {"manifest.json"}:
        raise ActionValueError("CAUSAL_BUNDLE_MEMBER_DRIFT")
    for reference in manifest["files"].values():
        check_ref(reference)
    return {"status": "VERIFIED", "bundle": root.resolve().as_posix(),
            "request_sha256": manifest["request_sha256"], "manifest_sha256": manifest["manifest_sha256"]}


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
        memberships = _memberships(candidate, identity)
        if list(candidate.symbols) != request["symbols"] or [str(value.date()) for value in candidate.calendar] != request["calendar"]:
            raise ActionValueError("CAUSAL_POPULATION_DRIFT")
        if source_audit(identity, frames) != request["proxy_source_audit"]:
            raise ActionValueError("CAUSAL_PROXY_SOURCE_DRIFT")
        if _minute_preflight(candidate) != request["minute_source_audit"]:
            raise ActionValueError("CAUSAL_MINUTE_SOURCE_DRIFT")
        parent_path, parent_identity = _parent_bundle(Path(request["timing_root"]))
        if parent_path.as_posix() != request["parent_bundle_root"] or parent_identity != request["parent_bundle"]:
            raise ActionValueError("CAUSAL_PARENT_BUNDLE_DRIFT")
        print(json.dumps({"stage": "RUN_AFTER_FULL_SOURCE_PREFLIGHT", "outcomes_read": True}), flush=True)
        symbols = candidate.symbols[:symbol_limit] if symbol_limit is not None else candidate.symbols
        benchmark = candidate.bars(BENCHMARK).close
        parent_fills = pd.read_parquet(Path(request["parent_bundle_root"]) / "fills.parquet")
        parent_fills = parent_fills.loc[(parent_fills.screen_id == U0) & (parent_fills.policy_id == S1)].copy()
        legacy_groups = {symbol: group for symbol, group in parent_fills.groupby("symbol", sort=False)}
        empty_legacy = parent_fills.iloc[0:0]
        labels_frames: list[pd.DataFrame] = []
        # First pass freezes labels and models; no public policy outcome is read here.
        def label_inputs() -> Iterator[tuple[Any, ...]]:
            for symbol in symbols:
                yield (
                    symbol, candidate.bars(symbol), _symbol_frame(frames.daily, symbol),
                    _symbol_frame(frames.bak, symbol), benchmark,
                    legacy_groups.get(symbol, empty_legacy),
                )

        for completed, (_, symbol_labels, _, _) in enumerate(
            _ordered_call(label_inputs(), worker_count=worker_count, function=_label_task), start=1
        ):
            if not symbol_labels.empty:
                labels_frames.append(symbol_labels)
            if completed % 256 == 0 or completed == len(symbols):
                print(json.dumps({"stage": "LABELS", "symbols_complete": completed, "total": len(symbols)}), flush=True)
        labels = pd.concat(labels_frames, ignore_index=True) if labels_frames else pd.DataFrame()
        if labels.empty:
            raise ActionValueError("CAUSAL_LABEL_POPULATION_EMPTY")
        label_sha = _frame_sha256(labels.sort_values(["symbol", "decision_date"], kind="stable"))
        models = {
            "ridge": fit_ridge(labels, source_sha256=label_sha, request_sha256=digest),
            "gbdt": fit_gbdt(labels, source_sha256=label_sha, request_sha256=digest),
        }
        for model in models.values():
            validate_model(model)
        terminal_ordinal = int(candidate.calendar.get_loc(pd.Timestamp(TERMINAL_DATE)))
        minute_root = (candidate.root / "components" / "minute_bin_candidate").as_posix()
        audits: list[dict[str, Any]] = []
        stock_frames: list[pd.DataFrame] = []
        fill_frames: list[pd.DataFrame] = []
        event_frames: list[pd.DataFrame] = []
        oracle_rows: list[dict[str, Any]] = []
        chunk_manifests: list[dict[str, Any]] = []
        chunk_items: list[dict[str, Any]] = []
        first_execution_ordinal = int(candidate.calendar.get_loc(pd.Timestamp(TEST_FIRST_EXECUTION)))
        common_ordinals = np.arange(first_execution_ordinal - 1, terminal_ordinal + 1)
        totals: dict[tuple[str, str, str], np.ndarray] = {}
        counts: dict[tuple[str, str, str], np.ndarray] = {}
        expected_pool_counts: Counter[str] = Counter()
        pit_masks: dict[str, np.ndarray] = {}

        def task_inputs() -> Iterator[tuple[Any, ...]]:
            for symbol in symbols:
                bars = candidate.bars(symbol)
                pit_masks[symbol] = bars.pit_active.to_numpy(bool)
                yield (
                    symbol, bars, _symbol_frame(frames.daily, symbol),
                    _symbol_frame(frames.bak, symbol), benchmark,
                    legacy_groups.get(symbol, empty_legacy), models, minute_root, terminal_ordinal,
                )

        for completed, (symbol, days, fills, replay_labels, events, audit, oracle) in enumerate(
            _ordered(task_inputs(), worker_count=worker_count), start=1
        ):
            if not events.empty:
                event_frames.append(events)
            if oracle is not None:
                oracle_rows.append({"symbol": symbol, **oracle})
            pool_ids = ["stock_universe"]
            membership_changes: dict[str, int] = {}
            if audit["status"] == "ENROLLED":
                pit = pit_masks[symbol]
                for pool in POOL_IDS[1:]:
                    member = memberships.effective_mask(
                        pool_id=pool, symbol=symbol, calendar=candidate.calendar, stock_pit_mask=pit,
                    )
                    path = member[int(audit["enrollment_ordinal"]):terminal_ordinal + 1].astype(int)
                    membership_changes[pool] = int(np.abs(np.diff(path)).sum()) if len(path) > 1 else 0
                    if member[int(audit["enrollment_ordinal"])]:
                        pool_ids.append(pool)
                expected_pool_counts.update(pool_ids)
            audit = {**audit, "pool_membership_change_counts": membership_changes}
            audits.append({"symbol": symbol, **audit})
            if days.empty:
                chunk_items.append({
                    "symbol": symbol,
                    "stocks_sha256": _frame_sha256(pd.DataFrame()),
                    "fills_sha256": _frame_sha256(fills),
                    "labels_sha256": _frame_sha256(replay_labels),
                    "event_oracle_sha256": _frame_sha256(events),
                    "restricted_oracle_sha256": None,
                    "enrollment_audit_sha256": canonical_sha256(audit),
                })
                if len(chunk_items) == CHUNK_SIZE or completed == len(symbols):
                    payload = {
                        "chunk_index": len(chunk_manifests),
                        "symbols": [item["symbol"] for item in chunk_items],
                        "items": chunk_items,
                    }
                    chunk_manifests.append({**payload, "chunk_sha256": canonical_sha256(payload)})
                    chunk_items = []
                if completed % CHUNK_SIZE == 0 or completed == len(symbols):
                    print(json.dumps({"stage": "REPLAY", "symbols_complete": completed, "total": len(symbols)}), flush=True)
                pit_masks.pop(symbol, None)
                continue
            details = []
            for (view, policy), group in days.groupby(["execution_view", "policy_id"], sort=False):
                # Reconstruct detail fields from final path and fill ledger for summary.
                selected_fills = fills.loc[(fills.execution_view == view) & (fills.policy_id == policy)] if not fills.empty else pd.DataFrame()
                completed_fills = (
                    selected_fills.loc[selected_fills.status.eq("FILLED")]
                    if not selected_fills.empty else selected_fills
                )
                last = group.sort_values("ordinal").iloc[-1]
                details.append({
                    "symbol": symbol, "execution_view": view, "policy_id": policy,
                    "enrollment_ordinal": audit["enrollment_ordinal"],
                    "fees_cny": float(completed_fills.fee.fillna(0).sum()) if not completed_fills.empty else 0.0,
                    "turnover_ratio": (
                        float(completed_fills.notional.fillna(0).sum() / float(CAPITAL))
                        if not completed_fills.empty else 0.0
                    ),
                    "terminal_status": "CASH" if float(last.virtual_units) == 0 else "RESIDUAL_POSITION_NOT_CLEARED",
                    "terminal_cash_cny": float(last.cash), "terminal_virtual_units": float(last.virtual_units),
                    "model_rejected_count": int(selected_fills.model_status.eq("REJECT").sum()) if "model_status" in selected_fills else 0,
                    "model_unavailable_count": int(selected_fills.model_status.eq("MODEL_UNAVAILABLE").sum()) if "model_status" in selected_fills else 0,
                })
                ordered = group.set_index("ordinal").reindex(common_ordinals)
                pre = common_ordinals <= int(audit["enrollment_ordinal"])
                nav = ordered.nav.to_numpy(float)
                nav[pre & ~np.isfinite(nav)] = float(CAPITAL)
                for pool in pool_ids:
                    key = (view, policy, pool)
                    totals.setdefault(key, np.zeros(len(common_ordinals)))[np.isfinite(nav)] += nav[np.isfinite(nav)]
                    counts.setdefault(key, np.zeros(len(common_ordinals), dtype=int))[:] += np.isfinite(nav)
            symbol_stocks = _stock_summaries(
                days, details, window_start_ordinal=int(common_ordinals[0]),
                terminal_ordinal=terminal_ordinal,
            )
            for pool in POOL_IDS:
                symbol_stocks[f"member_{pool}"] = pool in pool_ids
            stock_frames.append(symbol_stocks)
            if not fills.empty:
                fill_frames.append(fills)
            chunk_items.append({
                "symbol": symbol,
                "stocks_sha256": _frame_sha256(symbol_stocks),
                "fills_sha256": _frame_sha256(fills),
                "labels_sha256": _frame_sha256(replay_labels),
                "event_oracle_sha256": _frame_sha256(events),
                "restricted_oracle_sha256": canonical_sha256(oracle) if oracle is not None else None,
                "enrollment_audit_sha256": canonical_sha256(audit),
            })
            if len(chunk_items) == CHUNK_SIZE or completed == len(symbols):
                payload = {
                    "chunk_index": len(chunk_manifests), "symbols": [item["symbol"] for item in chunk_items],
                    "items": chunk_items,
                }
                chunk_manifests.append({**payload, "chunk_sha256": canonical_sha256(payload)})
                chunk_items = []
            if completed % CHUNK_SIZE == 0 or completed == len(symbols):
                print(json.dumps({"stage": "REPLAY", "symbols_complete": completed, "total": len(symbols)}), flush=True)
            pit_masks.pop(symbol, None)
        stocks = pd.concat(stock_frames, ignore_index=True) if stock_frames else pd.DataFrame()
        fills = pd.concat(fill_frames, ignore_index=True) if fill_frames else pd.DataFrame()
        events = pd.concat(event_frames, ignore_index=True) if event_frames else pd.DataFrame()
        chunk_symbol_count = sum(len(item["symbols"]) for item in chunk_manifests)
        if chunk_symbol_count != len(symbols):
            raise ActionValueError(
                "CAUSAL_CHUNK_POPULATION_INCOMPLETE",
                expected=len(symbols), observed=chunk_symbol_count,
            )
        pool_rows: list[dict[str, Any]] = []
        for (view, policy, pool), values in totals.items():
            for index, ordinal in enumerate(common_ordinals):
                account_count = int(counts[(view, policy, pool)][index])
                expected = int(expected_pool_counts[pool])
                unknown = expected - account_count
                pool_rows.append({"execution_view": view, "policy_id": policy, "pool_id": pool,
                                  "ordinal": int(ordinal), "valuation_date": str(candidate.calendar[ordinal].date()),
                                  "nav": values[index] if unknown == 0 else math.nan,
                                  "known_nav_cny": values[index], "account_count": account_count,
                                  "expected_account_count": expected, "unknown_account_count": unknown})
        pool_daily = pd.DataFrame(pool_rows)
        index_frame = pd.read_hdf(request["index_source"]["path"], key="data")
        index_daily = index_returns(index_frame, candidate.calendar)
        start_ordinal, end_ordinal = int(common_ordinals[0]), int(common_ordinals[-1])
        index_background: dict[str, Any] = {}
        for pool, code in INDEX_CODES.items():
            values = index_daily[code][start_ordinal + 1:end_ordinal + 1]
            index_background[pool] = {
                "index_code": code, "sessions": len(values),
                "total_return": float(np.prod(1.0 + values) - 1.0) if np.isfinite(values).all() else None,
            }
        report = _report(stocks, pool_daily, audits, models, index_background)
        report.update({
            "request_sha256": digest, "symbol_limit": symbol_limit,
            "full_population": symbol_limit is None,
            "oracle_summary": _oracle_summary(events, oracle_rows),
            "execution_diagnostics": _execution_diagnostics(fills),
            "fill_diagnostics": _fill_diagnostics(fills),
            "strata_diagnostics": _strata_diagnostics(pool_daily, index_daily),
        })
        cost_sensitivity: list[dict[str, Any]] = []
        if not fills.empty:
            filled = fills.loc[fills.status.eq("FILLED") & fills.notional.notna()].copy()
            for (view, policy), group in filled.groupby(["execution_view", "policy_id"], sort=False):
                row: dict[str, Any] = {"execution_view": view, "policy_id": policy,
                                       "method": "STATIC_FEE_ONLY_NO_ACCOUNT_FEEDBACK"}
                for parent_count in (1, 2, 3):
                    total = Decimal(0)
                    for item in group.itertuples():
                        notional = Decimal(str(item.notional))
                        notions = tuple(notional / parent_count for _ in range(parent_count))
                        total += component_cost_for_parent_notionals(
                            side=TriggerSide(str(item.side)), notionals=notions,
                        )["total"]
                    row[f"fee_{parent_count}_parents_cny"] = float(total)
                cost_sensitivity.append(row)
        max_train_label = max(
            value for value in pd.to_datetime(labels.label_available_at).dt.date
            if value <= date(2022, 12, 31)
        )
        causality = {
            "schema_version": "position_timing_causal_timing_causality_receipt_v1",
            "request_sha256": digest,
            "feature_input_basis_sha256": models["ridge"]["feature_input_basis_sha256"],
            "training_max_label_available_at": max_train_label.isoformat(),
            "training_label_cutoff": "2022-12-31", "preprocessing_fit_scope": "TRAIN_ONLY",
            "oracle_imported_by_policy": False, "model_effective_at": "2024-07-01T09:00:00+08:00",
            "negative_tests": ["FUTURE_LABEL_CROSS_SPLIT", "VALIDATION_FEATURE_PERTURBATION", "CONTRACT_DRIFT"],
            "status": "PASS_RUNTIME_INVARIANTS_UNIT_NEGATIVES_REFERENCED",
        }
        causality["causality_sha256"] = canonical_sha256(causality)
        trial_spec = {
            "schema_version": "position_timing_causal_timing_trial_spec_v1",
            "request_sha256": digest, "contract": CONTRACT, "contract_sha256": CONTRACT_SHA256,
            "parent_evidence_observed": True, "formal_family_size": 8,
            "runtime_parameter_selection_from_outcomes": False,
        }
        trial_spec["trial_spec_sha256"] = canonical_sha256(trial_spec)
        publish_json(bundle / "request.json", request)
        publish_json(bundle / "source_audit.json", {
            "proxy": request["proxy_source_audit"], "minute": request["minute_source_audit"],
            "source_preflight_complete": True,
        })
        publish_json(bundle / "models.json", models)
        publish_json(bundle / "trial_spec.json", trial_spec)
        publish_json(bundle / "causality_receipt.json", causality)
        publish_json(bundle / "cost_sensitivity.json", cost_sensitivity)
        publish_json(bundle / "chunk_manifests.json", chunk_manifests)
        publish_json(bundle / "report.json", report)
        publish_frame(bundle / "labels.parquet", labels)
        publish_frame(bundle / "stocks.parquet", stocks)
        publish_frame(bundle / "pool_daily.parquet", pool_daily)
        publish_frame(bundle / "fills.parquet", fills)
        publish_frame(bundle / "event_oracle.parquet", events)
        publish_json(bundle / "restricted_oracle.json", oracle_rows)
        publish_json(bundle / "enrollment_audit.json", audits)
        receipt = {
            "schema_version": RECEIPT_SCHEMA, "request_sha256": digest,
            "symbol_count": len(symbols), "full_population": symbol_limit is None,
            "label_count": len(labels), "enrolled_count": int(sum(item["status"] == "ENROLLED" for item in audits)),
            "chunk_count": len(chunk_manifests), "chunk_symbol_count": chunk_symbol_count,
            "outcomes_read_stage": "RUN_AFTER_FULL_SOURCE_PREFLIGHT",
            "source_preflight_complete": True, "result_class": "EXPLORATORY_HYPOTHESIS_GENERATED",
            "selected_for_live": 0, "database_read": False, "database_write": False,
            "market_network_accessed": False, "runtime_action_performed": False,
            "service_process_control_performed": False, "research_worker_processes_used": worker_count > 1,
        }
        receipt["receipt_sha256"] = canonical_sha256(receipt)
        publish_json(bundle / "receipt.json", receipt)
        names = ["request.json", "source_audit.json", "models.json", "trial_spec.json",
                 "causality_receipt.json", "cost_sensitivity.json", "chunk_manifests.json", "report.json", "labels.parquet",
                 "stocks.parquet", "pool_daily.parquet", "fills.parquet", "event_oracle.parquet",
                 "restricted_oracle.json", "enrollment_audit.json", "receipt.json"]
        _seal(bundle, names, request_sha256=digest)
        return inspect(bundle, request_sha256=digest)


def verify_parallel(path: Path, *, size: int = 32) -> dict[str, Any]:
    request = load_request(path)
    full_bundle = Path(request["timing_root"]) / "research" / FOLDER / "bundles" / request["request_sha256"]
    inspect(full_bundle, request_sha256=request["request_sha256"])
    models = read_json(full_bundle / "models.json")
    for value in models.values():
        validate_model(value)
    candidate = DailyCandidate.open(Path(request["candidate_root"]))
    identity = open_r8_proxy_sources(candidate.root)
    frames = read_proxy_frames(identity)
    benchmark = candidate.bars(BENCHMARK).close
    parent = pd.read_parquet(Path(request["parent_bundle_root"]) / "fills.parquet")
    parent = parent.loc[(parent.screen_id == U0) & (parent.policy_id == S1)]
    groups = {symbol: group for symbol, group in parent.groupby("symbol", sort=False)}
    empty = parent.iloc[0:0]
    symbols = candidate.symbols[:size]
    terminal = int(candidate.calendar.get_loc(pd.Timestamp(TERMINAL_DATE)))
    minute_root = (candidate.root / "components" / "minute_bin_candidate").as_posix()

    def materialize_inputs() -> list[tuple[Any, ...]]:
        return [(
            symbol, candidate.bars(symbol), _symbol_frame(frames.daily, symbol),
            _symbol_frame(frames.bak, symbol), benchmark, groups.get(symbol, empty),
            models, minute_root, terminal,
        ) for symbol in symbols]

    def digest(result: tuple[Any, ...]) -> str:
        symbol, days, fills, labels, events, audit, oracle = result
        return canonical_sha256({
            "symbol": symbol, "days": _frame_sha256(days), "fills": _frame_sha256(fills),
            "labels": _frame_sha256(labels), "events": _frame_sha256(events),
            "audit": audit, "oracle": oracle,
        })

    sequential = {result[0]: digest(result) for result in _ordered(materialize_inputs(), worker_count=1)}
    parallel = {result[0]: digest(result) for result in _ordered(materialize_inputs(), worker_count=WORKER_COUNT)}
    if sequential != parallel:
        raise ActionValueError("CAUSAL_PARALLEL_RESULT_DRIFT")
    audit = {"status": "EXACT", "size": size, "request_sha256": request["request_sha256"],
             "symbols": symbols, "result_sha256_by_symbol": sequential, "artifact_written": False}
    return {**audit, "audit_sha256": canonical_sha256(audit)}


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
    verify_parser.add_argument("--size", type=int, default=32)
    args = parser.parse_args()
    if args.command == "prepare":
        print(prepare(timing_root=args.timing_root, repository_root=args.repository_root,
                      candidate_root=args.candidate_root))
    elif args.command == "run":
        print(json.dumps(run(args.request, worker_count=args.worker_count, symbol_limit=args.symbol_limit), ensure_ascii=False))
    elif args.command == "verify-parallel":
        print(json.dumps(verify_parallel(args.request, size=args.size), ensure_ascii=False))
    else:
        print(json.dumps(inspect(args.bundle), ensure_ascii=False))


if __name__ == "__main__":
    main()
