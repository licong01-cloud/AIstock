"""Immutable WSL benchmark for PT-NEXT-022 fundamental screens and timing."""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import Future, ProcessPoolExecutor
from datetime import date
import hashlib
import importlib.metadata
import json
import multiprocessing
from pathlib import Path
import platform
import subprocess
from typing import Any, Iterable, Iterator, Mapping

import numpy as np
import pandas as pd

from .action_value import ActionValueError
from .action_value_data import DailyCandidate, file_reference
from .artifact_store import _exclusive_file_lock
from .contracts import canonical_sha256
from .fundamental_screen import (
    R8_DATASET_SHA256,
    R8_MANIFEST_FILE_SHA256,
    SCREEN_CONTRACT,
    SCREEN_CONTRACT_SHA256,
    first_enrollment_ordinal,
    open_r8_candidate_identity,
    p1_mask_for_symbol,
    read_total_market_cap,
    unavailable_financial_groups,
)
from .fundamental_timing import (
    POLICY_CONTRACT,
    POLICY_CONTRACT_SHA256,
    POLICY_IDS,
    build_research_features,
    replay_fundamental_policies,
)
from .pattern_adj_factor_restatement import (
    audit_candidate_adj_factor_restatement,
    open_adj_factor_restatement_authority,
)
from .pattern_close_cash_benchmark import check_ref, publish_frame, publish_json, read_json
from .pattern_close_cash_replay import CAPITAL
from .pattern_close_cash_report import INDEX_CODES, comparison, index_returns, performance
from .pattern_universe_benchmark import (
    EXPECTED_ADJ_FACTOR_RESTATEMENT_AUTHORITY_SHA256,
    POOL_IDS,
    QLIB_ADJUSTED_FACTOR_CONTRACT_SHA256,
    CandidatePoolMemberships,
    _audit_qlib_adjusted_factor_integrity,
    open_candidate_pool_memberships,
)


PIPELINE_ID = "POSITION_TIMING_FUNDAMENTAL_TIMING_V1"
FOLDER = "fundamental_timing_v1"
REQUEST_SCHEMA = "position_timing_fundamental_timing_request_v1"
RECEIPT_SCHEMA = "position_timing_fundamental_timing_receipt_v1"
MANIFEST_SCHEMA = "position_timing_fundamental_timing_files_v1"
REPORT_SCHEMA = "position_timing_fundamental_timing_report_v1"
CHUNK_SIZE = 128
WORKER_COUNT = 8
MAX_IN_FLIGHT = 16
BOOTSTRAP_REPLICATES = 5_000
BOOTSTRAP_BLOCK = 25
BOOTSTRAP_SEED = 20260919
FAMILY_SIZE = 6

CONTRACT = {
    "pipeline_id": PIPELINE_ID,
    "screens": SCREEN_CONTRACT,
    "screen_contract_sha256": SCREEN_CONTRACT_SHA256,
    "policies": POLICY_CONTRACT,
    "policy_contract_sha256": POLICY_CONTRACT_SHA256,
    "hypothesis_family": [f"{screen}:{policy}" for screen in ("P1", "P2", "P3") for policy in POLICY_IDS],
    "family_size": FAMILY_SIZE,
    "capital_per_stock_account_cny": str(CAPITAL),
    "benchmark": "SAME_STOCK_BUY_AND_HOLD",
    "index_background": INDEX_CODES,
    "pool_ids": list(POOL_IDS),
    "bootstrap": {
        "replicates": BOOTSTRAP_REPLICATES,
        "block_sessions": BOOTSTRAP_BLOCK,
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
    "corporate_action_authority_read": False,
    "account_economics_simulated": False,
    "broker_account_clearing": False,
}
CONTRACT_SHA256 = canonical_sha256(CONTRACT)


def _clean_repository_commit(repository: Path) -> str:
    """Freeze source identity without importing unrelated online services."""

    command = ["git"]
    cwd: Path | None = repository
    git_pointer = repository / ".git"
    if platform.system() == "Linux" and git_pointer.is_file():
        pointer = git_pointer.read_text(encoding="utf-8").strip()
        git_directory = pointer.removeprefix("gitdir:").strip()
        if len(git_directory) > 3 and git_directory[1:3] == ":/":
            windows_root = subprocess.run(
                ["wslpath", "-w", str(repository)],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
            command = ["/mnt/c/Program Files/Git/cmd/git.exe", "-C", windows_root]
            cwd = None
    status = subprocess.run(
        [*command, "status", "--porcelain"],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if status:
        raise ActionValueError("REPOSITORY_NOT_CLEAN")
    commit = subprocess.run(
        [*command, "rev-parse", "HEAD"],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip().lower()
    if len(commit) != 40 or any(value not in "0123456789abcdef" for value in commit):
        raise ActionValueError("FUNDAMENTAL_CODE_IDENTITY_INVALID")
    return commit


def _environment_identity() -> dict[str, str]:
    value = {
        "system": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
        "python": platform.python_version(),
        "numpy": np.__version__,
        "pandas": pd.__version__,
        "pyarrow": importlib.metadata.version("pyarrow"),
    }
    return {**value, "environment_sha256": canonical_sha256(value)}


def _source_files(repository: Path) -> dict[str, dict[str, Any]]:
    names = (
        "backend/services/position_timing/fundamental_screen.py",
        "backend/services/position_timing/fundamental_timing.py",
        "backend/services/position_timing/fundamental_timing_benchmark.py",
        "backend/services/position_timing/action_value_data.py",
        "backend/services/position_timing/pattern_close_cash_replay.py",
        "backend/services/position_timing/pattern_strategy.py",
        "backend/services/position_timing/pattern_strategy_evolution.py",
        "backend/services/position_timing/pattern_universe_benchmark.py",
        "backend/services/position_timing/policy.py",
    )
    return {name: file_reference(repository / name) for name in names}


def _pool_expectations(identity: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    manifest = identity["manifest"]
    sidecars = ((manifest.get("st_pit_manifest") or {}).get("index_membership_sidecars") or {})
    if set(sidecars) != set(POOL_IDS):
        raise ActionValueError("FUNDAMENTAL_POOL_SET_DRIFT")
    return {
        pool: {
            "path": spec["path"],
            "sha256": spec["sha256"],
            "size_bytes": spec["size"],
        }
        for pool, spec in sidecars.items()
    }


def _open_memberships(candidate: DailyCandidate, identity: Mapping[str, Any]) -> CandidatePoolMemberships:
    return open_candidate_pool_memberships(
        candidate,
        expected_candidate_manifest_sha256=R8_MANIFEST_FILE_SHA256,
        expected_candidate_dataset_sha256=R8_DATASET_SHA256,
        expected_pool_files=_pool_expectations(identity),
    )


def _identity_payload(identity: Any) -> dict[str, Any]:
    return {
        "manifest": identity.manifest,
        "manifest_reference": identity.manifest_reference,
    }


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
        raise ActionValueError("FUNDAMENTAL_PATH_SCOPE_INVALID")
    commit = _clean_repository_commit(repository)
    source_code = _source_files(repository)
    identity = open_r8_candidate_identity(candidate_root)
    candidate = DailyCandidate.open(candidate_root)
    memberships = _open_memberships(candidate, _identity_payload(identity))
    if (
        candidate.calendar[0].date(), candidate.calendar[-1].date(), len(candidate.calendar)
    ) != (date(2018, 8, 1), date(2026, 8, 31), 1961):
        raise ActionValueError("FUNDAMENTAL_CALENDAR_DRIFT")
    if list(candidate.symbols) != sorted(candidate.symbols):
        raise ActionValueError("FUNDAMENTAL_SYMBOL_ORDER_DRIFT")

    authority = open_adj_factor_restatement_authority(
        candidate_root=candidate.root,
        expected_candidate_manifest_sha256=R8_MANIFEST_FILE_SHA256,
        expected_authority_canonical_sha256=EXPECTED_ADJ_FACTOR_RESTATEMENT_AUTHORITY_SHA256,
    )
    restatement = audit_candidate_adj_factor_restatement(candidate, authority)
    candidate_identity = canonical_sha256({
        "candidate_manifest": memberships.candidate_manifest_reference,
        "candidate_dataset_manifest_sha256": memberships.candidate_dataset_manifest_sha256,
        "pool_sidecars": memberships.references,
    })
    print(json.dumps({"stage": "SOURCE_FACTOR_PREFLIGHT", "outcomes_read": False}), flush=True)
    factors = _audit_qlib_adjusted_factor_integrity(
        candidate,
        symbols=candidate.symbols,
        start=candidate.calendar[0].date(),
        end=candidate.calendar[-1].date(),
        candidate_source_sha256=candidate_identity,
    )
    market_cap = read_total_market_cap(identity)
    candidate_symbols = set(candidate.symbols)
    mv_symbols = set(market_cap.index.get_level_values("instrument"))
    market_cap_audit = {
        "schema_version": "position_timing_total_mv_source_audit_v1",
        "row_count": int(len(market_cap)),
        "symbol_count": int(len(mv_symbols)),
        "candidate_symbol_count": len(candidate.symbols),
        "candidate_symbols_missing": sorted(candidate_symbols - mv_symbols),
        "start": str(market_cap.index.get_level_values("datetime").min().date()),
        "end": str(market_cap.index.get_level_values("datetime").max().date()),
        "unit": "CNY_10000",
        "finite_positive": True,
        "source": identity.daily_basic_reference,
    }
    market_cap_audit["audit_sha256"] = canonical_sha256(market_cap_audit)
    if (
        not factors.get("coverage_complete")
        or not restatement.get("coverage_complete")
        or market_cap_audit["candidate_symbols_missing"]
    ):
        raise ActionValueError("FUNDAMENTAL_SOURCE_PREFLIGHT_FAILED")
    index_spec = identity.manifest["components"]["index_daily"]
    index_path = (candidate.root / index_spec["path"]).resolve()
    index_ref = file_reference(index_path)
    if index_ref["sha256"] != index_spec["sha256"] or index_ref["size_bytes"] != index_spec["size"]:
        raise ActionValueError("FUNDAMENTAL_INDEX_SOURCE_DRIFT")
    if _source_files(repository) != source_code:
        raise ActionValueError("FUNDAMENTAL_CODE_DRIFT")
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
        "candidate_manifest": identity.manifest_reference,
        "candidate_dataset_manifest_sha256": identity.dataset_sha256,
        "candidate_deployment_content_sha256": identity.manifest["deployment_content_sha256"],
        "symbols": candidate.symbols,
        "calendar": [str(value.date()) for value in candidate.calendar],
        "source_data": candidate.references,
        "pool_sidecars": memberships.references,
        "index_source": index_ref,
        "daily_basic_source": identity.daily_basic_reference,
        "market_cap_audit": market_cap_audit,
        "financial_groups": unavailable_financial_groups(),
        "restatement_authority": authority.authority_reference,
        "restatement_audit": restatement,
        "factor_audit": factors,
        "qlib_contract_sha256": QLIB_ADJUSTED_FACTOR_CONTRACT_SHA256,
        "source_preflight_complete": True,
        "outcomes_read": False,
        "database_read": False,
        "database_write": False,
        "network_accessed": False,
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
    ):
        raise ActionValueError("FUNDAMENTAL_REQUEST_DRIFT")
    root = Path(request["timing_root"]).resolve()
    repository = Path(request["repository_root"]).resolve()
    expected = root / "research" / FOLDER / "requests" / f"{digest}.json"
    if path.resolve() != expected or repository != Path(__file__).resolve().parents[3]:
        raise ActionValueError("FUNDAMENTAL_REQUEST_SCOPE_DRIFT")
    if request.get("environment") != _environment_identity():
        raise ActionValueError("FUNDAMENTAL_ENVIRONMENT_DRIFT")
    if (
        request.get("source_preflight_complete") is not True
        or request.get("candidate_manifest", {}).get("sha256") != R8_MANIFEST_FILE_SHA256
        or request.get("candidate_dataset_manifest_sha256") != R8_DATASET_SHA256
        or request.get("qlib_contract_sha256") != QLIB_ADJUSTED_FACTOR_CONTRACT_SHA256
        or any(request.get(key) is not False for key in (
            "outcomes_read", "database_read", "database_write", "network_accessed",
            "runtime_action_performed", "service_process_control_performed",
        ))
    ):
        raise ActionValueError("FUNDAMENTAL_REQUEST_BOUNDARY_DRIFT")
    for audit_name in ("market_cap_audit", "restatement_audit", "factor_audit"):
        audit = request[audit_name]
        if audit.get("audit_sha256") != canonical_sha256({k: v for k, v in audit.items() if k != "audit_sha256"}):
            raise ActionValueError("FUNDAMENTAL_PREFLIGHT_AUDIT_DRIFT", audit=audit_name)
    for reference in (
        request["candidate_manifest"], request["index_source"], request["daily_basic_source"],
        request["restatement_authority"],
    ):
        check_ref(reference)
    for group in ("source_code", "source_data", "pool_sidecars"):
        for reference in request[group].values():
            check_ref(reference)
    if _source_files(repository) != request["source_code"]:
        raise ActionValueError("FUNDAMENTAL_CODE_DRIFT")
    return request


def _replay_symbol_task(
    symbol: str,
    bars: pd.DataFrame,
    market_cap: pd.Series,
) -> tuple[str, pd.DataFrame, pd.DataFrame, list[dict[str, Any]], dict[str, Any]]:
    _, pattern, trend, ready = build_research_features(symbol, bars)
    mask, coverage = p1_mask_for_symbol(
        market_cap,
        dates=bars.index,
        pit_active=bars.pit_active.to_numpy(bool),
        feature_ready=ready,
    )
    aligned_market_cap = pd.to_numeric(
        market_cap.reindex(pd.DatetimeIndex(bars.index)), errors="coerce"
    ).shift(1)
    pit = bars.pit_active.to_numpy(bool)
    unknown = aligned_market_cap.isna().to_numpy(bool) & pit
    unknown_intervals: list[dict[str, str]] = []
    interval_start: int | None = None
    for ordinal, value in enumerate(np.r_[unknown, False]):
        if value and interval_start is None:
            interval_start = ordinal
        elif not value and interval_start is not None:
            unknown_intervals.append({
                "start": str(pd.Timestamp(bars.index[interval_start]).date()),
                "end": str(pd.Timestamp(bars.index[ordinal - 1]).date()),
            })
            interval_start = None
    enrollment = first_enrollment_ordinal(mask, final_decision_ordinal=len(bars) - 1)
    enrollment_unknown = unknown & ready
    boundary = enrollment if enrollment is not None else len(bars) - 1
    enrollment_identity_unknown = bool(enrollment_unknown[:boundary + 1].any())
    if enrollment_identity_unknown:
        enrollment = None
    audit = {
        "symbol": symbol,
        "screen_id": "P1",
        "status": (
            "ENROLLMENT_UNKNOWN" if enrollment_identity_unknown else
            "ENROLLED" if enrollment is not None else
            "NOT_ENROLLED"
        ),
        "enrollment_ordinal": enrollment,
        "coverage": coverage,
        "unknown_intervals": unknown_intervals,
    }
    if enrollment is None:
        details = [
            {
                "symbol": symbol,
                "screen_id": "P1",
                "policy_id": policy,
                "status": "ENROLLMENT_UNKNOWN" if enrollment_identity_unknown else "NOT_ENROLLED",
                "enrollment_ordinal": None,
                "counts": {},
            }
            for policy in POLICY_IDS
        ]
        return symbol, pd.DataFrame(), pd.DataFrame(), details, audit
    days, fills, details = replay_fundamental_policies(symbol, bars, enrollment_ordinal=enrollment)
    details = [{**detail, "screen_id": "P1"} for detail in details]
    return symbol, days, fills, details, audit


def _ordered_replays(
    inputs: Iterable[tuple[str, pd.DataFrame, pd.Series]],
    *,
    worker_count: int,
    max_in_flight: int,
) -> Iterator[tuple[str, pd.DataFrame, pd.DataFrame, list[dict[str, Any]], dict[str, Any]]]:
    if worker_count < 1 or max_in_flight < worker_count:
        raise ActionValueError("FUNDAMENTAL_PARALLEL_CONTRACT_INVALID")
    if worker_count == 1:
        for args in inputs:
            yield _replay_symbol_task(*args)
        return
    executor = ProcessPoolExecutor(max_workers=worker_count, mp_context=multiprocessing.get_context("spawn"))
    pending: dict[int, tuple[str, Future[Any]]] = {}
    iterator = iter(inputs)
    submitted = consumed = 0
    exhausted = False
    try:
        while not exhausted or pending:
            while not exhausted and len(pending) < max_in_flight:
                try:
                    symbol, bars, market_cap = next(iterator)
                except StopIteration:
                    exhausted = True
                    break
                pending[submitted] = (
                    symbol,
                    executor.submit(_replay_symbol_task, symbol, bars, market_cap),
                )
                submitted += 1
            if consumed in pending:
                expected, future = pending.pop(consumed)
                result = future.result()
                if result[0] != expected:
                    raise ActionValueError("FUNDAMENTAL_WORKER_ORDER_DRIFT")
                yield result
                consumed += 1
    finally:
        for _, future in pending.values():
            future.cancel()
        executor.shutdown(wait=True, cancel_futures=True)


def _empty_total(size: int) -> dict[str, np.ndarray]:
    return {
        "timing_sum": np.zeros(size),
        "hold_sum": np.zeros(size),
        "expected": np.zeros(size, dtype=np.int64),
        "paired": np.zeros(size, dtype=np.int64),
        "unknown": np.zeros(size, dtype=np.int64),
    }


def _add_path(
    *,
    symbol: str,
    policy_id: str,
    path: pd.DataFrame | None,
    detail: Mapping[str, Any],
    pit: np.ndarray,
    memberships: CandidatePoolMemberships,
    calendar: pd.DatetimeIndex,
    totals: dict[tuple[str, str], dict[str, np.ndarray]],
) -> dict[str, Any]:
    size = len(calendar)
    full = np.full((size, 2), np.nan)
    start = detail.get("enrollment_ordinal")
    if path is not None and not path.empty:
        ordered = path.sort_values("ordinal", kind="stable")
        ordinals = ordered.ordinal.to_numpy(int)
        full[ordinals, 0] = ordered.timing_nav.to_numpy(float)
        full[ordinals, 1] = ordered.hold_nav.to_numpy(float)
    returns = np.full_like(full, np.nan)
    returns[1:] = full[1:] / full[:-1] - 1
    valid = np.isfinite(returns).all(axis=1)
    if start is not None:
        account_active = np.arange(size) > int(start)
        for pool in POOL_IDS:
            member = memberships.effective_mask(
                pool_id=pool,
                symbol=symbol,
                calendar=calendar,
                stock_pit_mask=pit,
            )
            expected = account_active & np.r_[False, member[:-1]]
            paired = expected & valid
            total = totals.setdefault((policy_id, pool), _empty_total(size))
            total["timing_sum"] += np.where(paired, returns[:, 0], 0.0)
            total["hold_sum"] += np.where(paired, returns[:, 1], 0.0)
            total["expected"] += expected
            total["paired"] += paired
            total["unknown"] += expected & ~valid
    result: dict[str, Any] = {
        "screen_id": "P1",
        "symbol": symbol,
        "policy_id": policy_id,
        "status": detail["status"],
        "enrollment_ordinal": start,
    }
    if start is None or path is None or path.empty:
        return result
    terminal = detail["terminal"]
    timing_liq = terminal["timing"]["liquidatable_nav_cny"]
    hold_liq = terminal["hold"]["liquidatable_nav_cny"]
    result.update({
        "enrollment_date": str(calendar[int(start)].date()),
        "enrollment_year": int(calendar[int(start)].year),
        "end_date": str(calendar[-1].date()),
        "timing_terminal_mtm_return": full[-1, 0] / float(CAPITAL) - 1 if np.isfinite(full[-1, 0]) else None,
        "hold_terminal_mtm_return": full[-1, 1] / float(CAPITAL) - 1 if np.isfinite(full[-1, 1]) else None,
        "timing_terminal_liquidatable_return": timing_liq / float(CAPITAL) - 1 if timing_liq is not None else None,
        "hold_terminal_liquidatable_return": hold_liq / float(CAPITAL) - 1 if hold_liq is not None else None,
        "terminal_liquidatable_excess_return": (
            (timing_liq - hold_liq) / float(CAPITAL)
            if timing_liq is not None and hold_liq is not None else None
        ),
        "timing_terminal_liquidation_status": terminal["timing"]["liquidation_status"],
        "hold_terminal_liquidation_status": terminal["hold"]["liquidation_status"],
        "timing_fees_cny": terminal["timing"]["fees_cny"],
        "hold_fees_cny": terminal["hold"]["fees_cny"],
        "paired_unknown_sessions": int((~valid[int(start) + 1:]).sum()),
        "daily_path_complete": bool(valid[int(start) + 1:].all()),
        "invested_session_fraction": float((path.timing_exposure.to_numpy(float) > 0).mean()),
        "average_exposure": float(np.nanmean(path.timing_exposure.to_numpy(float))),
        "conditional_exposure": (
            float(np.nanmean(path.loc[path.timing_exposure > 0, "timing_exposure"]))
            if (path.timing_exposure > 0).any() else 0.0
        ),
    })
    metrics = comparison(
        returns[int(start) + 1:, 0],
        returns[int(start) + 1:, 1],
        np.full(size - int(start) - 1, np.nan),
    )
    result.update({
        key: value for key, value in pd.json_normalize(metrics, sep="_").iloc[0].to_dict().items()
        if not key.startswith("index_") and "_minus_index" not in key
    })
    return result


def _partial_frame(totals: Mapping[tuple[str, str], Mapping[str, np.ndarray]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for (policy, pool), total in totals.items():
        for ordinal in np.flatnonzero(total["expected"] > 0):
            rows.append({
                "policy_id": policy,
                "pool": pool,
                "ordinal": int(ordinal),
                **{name: values[ordinal] for name, values in total.items()},
            })
    return pd.DataFrame(rows)


def _merge_partials(frames: Iterable[pd.DataFrame], size: int) -> dict[tuple[str, str], dict[str, np.ndarray]]:
    totals: dict[tuple[str, str], dict[str, np.ndarray]] = {}
    for frame in frames:
        if frame.empty:
            continue
        for (policy, pool), group in frame.groupby(["policy_id", "pool"], sort=False):
            total = totals.setdefault((str(policy), str(pool)), _empty_total(size))
            ordinal = group.ordinal.to_numpy(int)
            for name in total:
                total[name][ordinal] += group[name].to_numpy(total[name].dtype)
    return totals


def _concat(frames: Iterable[pd.DataFrame]) -> pd.DataFrame:
    columns: list[str] = []
    records: list[dict[str, Any]] = []
    for frame in frames:
        for name in frame.columns:
            if str(name) not in columns:
                columns.append(str(name))
        records.extend(frame.to_dict("records"))
    return pd.DataFrame.from_records(records, columns=columns)


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
    digest = canonical_sha256({key: value for key, value in manifest.items() if key != "manifest_sha256"})
    if (
        manifest.get("schema_version") != MANIFEST_SCHEMA
        or manifest.get("manifest_sha256") != digest
        or (request_sha256 is not None and manifest.get("request_sha256") != request_sha256)
    ):
        raise ActionValueError("FUNDAMENTAL_MANIFEST_DRIFT")
    for name, reference in manifest["files"].items():
        target = (root / name).resolve()
        if not target.is_relative_to(root) or Path(reference["path"]).resolve() != target:
            raise ActionValueError("FUNDAMENTAL_MANIFEST_SCOPE_DRIFT")
        check_ref(reference)
    return {
        "status": "VERIFIED",
        "bundle": root.as_posix(),
        "manifest_sha256": manifest["manifest_sha256"],
        "request_sha256": manifest["request_sha256"],
    }


def _bootstrap(values: np.ndarray) -> dict[str, Any]:
    finite = np.isfinite(values)
    if not finite.any():
        return {"status": "UNAVAILABLE", "reason": "NO_PAIRED_OBSERVED_DAILY_DIFFERENCE"}
    sessions = len(values)
    rng = np.random.default_rng(BOOTSTRAP_SEED)
    starts = rng.integers(0, sessions, size=(BOOTSTRAP_REPLICATES, int(np.ceil(sessions / BOOTSTRAP_BLOCK))))
    indexes = ((starts[..., None] + np.arange(BOOTSTRAP_BLOCK)) % sessions).reshape(BOOTSTRAP_REPLICATES, -1)[:, :sessions]
    estimates = np.nanmean(values[indexes], axis=1) * 10_000.0
    alpha = 0.05 / FAMILY_SIZE
    family_interval = [
        float(np.nanquantile(estimates, alpha / 2)),
        float(np.nanquantile(estimates, 1 - alpha / 2)),
    ]
    evidence = (
        "SUPPORTED" if family_interval[0] > 0 else
        "NEGATIVE" if family_interval[1] < 0 else
        "INCONCLUSIVE"
    )
    return {
        "status": "ESTIMATED",
        "estimand": "PAIRED_OBSERVED_POOL_DAILY_RETURN_DIFFERENCE_BPS",
        "observed_sessions": int(finite.sum()),
        "point_bps": float(np.nanmean(values) * 10_000.0),
        "nominal_95_interval_bps": [float(np.nanquantile(estimates, .025)), float(np.nanquantile(estimates, .975))],
        "familywise_interval_bps": family_interval,
        "family_size": FAMILY_SIZE,
        "economic_threshold_bps": 0.0,
        "evidence_state": evidence,
    }


def _build_report(
    *,
    totals: Mapping[tuple[str, str], Mapping[str, np.ndarray]],
    stocks: pd.DataFrame,
    fills: pd.DataFrame,
    audits: list[dict[str, Any]],
    calendar: pd.DatetimeIndex,
    index_frame: pd.DataFrame,
) -> tuple[dict[str, Any], pd.DataFrame]:
    indexes = index_returns(index_frame, calendar)
    pool_rows: list[dict[str, Any]] = []
    daily_rows: list[dict[str, Any]] = []
    family: list[dict[str, Any]] = []
    for policy in POLICY_IDS:
        for pool in POOL_IDS:
            total = totals.get((policy, pool), _empty_total(len(calendar)))
            paired = total["paired"] > 0
            timing = np.full(len(calendar), np.nan)
            hold = np.full(len(calendar), np.nan)
            timing[paired] = total["timing_sum"][paired] / total["paired"][paired]
            hold[paired] = total["hold_sum"][paired] / total["paired"][paired]
            benchmark = indexes[INDEX_CODES[pool]]
            eligible = paired & np.isfinite(timing) & np.isfinite(hold)
            pool_rows.append({
                "screen_id": "P1",
                "policy_id": policy,
                "pool": pool,
                "index_code": INDEX_CODES[pool],
                "paired_sessions": int(eligible.sum()),
                "max_expected_accounts": int(total["expected"].max()),
                "max_paired_accounts": int(total["paired"].max()),
                "unknown_account_sessions": int(total["unknown"].sum()),
                "comparison": comparison(timing[eligible], hold[eligible], benchmark[eligible]),
            })
            for ordinal in np.flatnonzero(total["expected"] > 0):
                daily_rows.append({
                    "screen_id": "P1",
                    "policy_id": policy,
                    "pool": pool,
                    "date": str(calendar[ordinal].date()),
                    "ordinal": int(ordinal),
                    "timing_return_observed": timing[ordinal],
                    "hold_return_observed": hold[ordinal],
                    "index_return": benchmark[ordinal],
                    "expected_accounts": int(total["expected"][ordinal]),
                    "paired_accounts": int(total["paired"][ordinal]),
                    "unknown_accounts": int(total["unknown"][ordinal]),
                })
            if pool == "stock_universe":
                values = np.full(len(calendar), np.nan)
                values[eligible] = timing[eligible] - hold[eligible]
                family.append({"screen_id": "P1", "policy_id": policy, **_bootstrap(values)})
    for screen in ("P2", "P3"):
        for policy in POLICY_IDS:
            family.append({
                "screen_id": screen,
                "policy_id": policy,
                "status": "UNAVAILABLE",
                "reason": "FINANCIAL_PIT_INPUT_NOT_DELIVERED",
                "family_size": FAMILY_SIZE,
            })

    csi300 = indexes["000300.SH"]
    for row_index, row in stocks.iterrows():
        if pd.isna(row.get("enrollment_ordinal")):
            continue
        begin = int(row["enrollment_ordinal"]) + 1
        summary = performance(csi300[begin:])
        stocks.at[row_index, "benchmark"] = "000300.SH"
        stocks.at[row_index, "index_total_return"] = summary["total_return"]
        timing_total = row.get("timing_total_return")
        hold_total = row.get("hold_total_return")
        index_total = summary["total_return"]
        stocks.at[row_index, "timing_minus_index"] = (
            float(timing_total) - float(index_total)
            if pd.notna(timing_total) and index_total is not None else np.nan
        )
        stocks.at[row_index, "hold_minus_index"] = (
            float(hold_total) - float(index_total)
            if pd.notna(hold_total) and index_total is not None else np.nan
        )
    distribution: list[dict[str, Any]] = []
    annual: list[dict[str, Any]] = []
    for policy, group in stocks.groupby("policy_id", sort=True):
        delta = pd.to_numeric(group.terminal_liquidatable_excess_return, errors="coerce").dropna()
        distribution.append({
            "screen_id": "P1", "policy_id": policy,
            "paired_terminal_symbols": int(len(delta)),
            "win_fraction": float((delta > 0).mean()) if len(delta) else None,
            "mean": float(delta.mean()) if len(delta) else None,
            "quantiles": {str(q): float(delta.quantile(q)) for q in (.05, .25, .5, .75, .95)} if len(delta) else {},
            "bottom_5pct_mean": (
                float(delta.nsmallest(max(1, int(np.ceil(len(delta) * .05)))).mean())
                if len(delta) else None
            ),
            "bottom_5pct_contribution_to_population_mean": (
                float(delta.nsmallest(max(1, int(np.ceil(len(delta) * .05)))).sum() / len(delta))
                if len(delta) else None
            ),
        })
        for year, cohort in group.groupby("enrollment_year", dropna=True, sort=True):
            value = pd.to_numeric(cohort.terminal_liquidatable_excess_return, errors="coerce").dropna()
            annual.append({
                "screen_id": "P1", "policy_id": policy, "enrollment_year": int(year),
                "paired_symbols": int(len(value)),
                "mean_terminal_excess": float(value.mean()) if len(value) else None,
                "median_terminal_excess": float(value.median()) if len(value) else None,
                "diagnostic_only": True,
            })
    fill_summary = []
    if not fills.empty:
        for key, count in fills.groupby(["policy_id", "role", "side", "authority", "status"], dropna=False).size().items():
            fill_summary.append({
                "policy_id": key[0], "role": key[1], "side": key[2],
                "authority": key[3], "status": key[4], "count": int(count),
            })
    coverage = Counter(audit["status"] for audit in audits)
    p1_state_counts = Counter()
    unknown_interval_count = 0
    for audit in audits:
        values = audit["coverage"]
        p1_state_counts.update({
            "expected": int(values["expected"]),
            "PASS": int(values["screen_pass"]),
            "FAIL": int(values["fail"]),
            "UNKNOWN": int(values["unknown"]),
            "NOT_APPLICABLE": int(values["not_applicable"]),
            "ENROLLMENT_ELIGIBLE": int(values["enrollment_eligible"]),
            "ENROLLMENT_UNKNOWN_SESSIONS": int(values["enrollment_unknown"]),
        })
        unknown_interval_count += len(audit["unknown_intervals"])
    return {
        "schema_version": REPORT_SCHEMA,
        "result_class": "EXPLORATORY_HYPOTHESIS_GENERATED",
        "selected_trial_count": 0,
        "family_size": FAMILY_SIZE,
        "financial_groups": unavailable_financial_groups(),
        "screen_coverage": {
            "P1": {
                **dict(p1_state_counts),
                "enrolled_symbols": int(coverage["ENROLLED"]),
                "not_enrolled_symbols": int(coverage["NOT_ENROLLED"]),
                "enrollment_unknown_symbols": int(coverage["ENROLLMENT_UNKNOWN"]),
                "unknown_interval_count": unknown_interval_count,
            },
            **unavailable_financial_groups(),
        },
        "pools": pool_rows,
        "familywise_daily_difference": family,
        "terminal_excess_distributions": distribution,
        "enrollment_year_diagnostic": annual,
        "fill_summary": fill_summary,
        "index_basis": "OFFICIAL_PRICE_INDEX_NOT_TOTAL_RETURN",
        "shared_cash_portfolio": False,
        "coverage_rule": "FULL_POPULATION_AND_PAIRED_OBSERVED_REPORTED_SEPARATELY",
    }, pd.DataFrame(daily_rows)


def _validate_population(candidate: DailyCandidate, request: Mapping[str, Any]) -> None:
    if list(candidate.symbols) != request["symbols"] or [str(x.date()) for x in candidate.calendar] != request["calendar"]:
        raise ActionValueError("FUNDAMENTAL_POPULATION_DRIFT")


def _bar_inputs(
    candidate: DailyCandidate,
    request: Mapping[str, Any],
    symbols: Iterable[str],
    market_cap_groups: Any,
) -> Iterator[tuple[str, pd.DataFrame, pd.Series]]:
    for symbol in symbols:
        bars = candidate.bars(symbol)
        for field in ("open", "high", "low", "close", "volume", "factor", "up_limit_price", "down_limit_price"):
            key = f"{symbol}:{field}"
            if candidate.references[key] != request["source_data"][key]:
                raise ActionValueError("FUNDAMENTAL_BAR_SOURCE_DRIFT", symbol=symbol)
        try:
            mv = market_cap_groups.get_group(symbol)
        except KeyError as exc:
            raise ActionValueError("FUNDAMENTAL_TOTAL_MV_SYMBOL_MISSING", symbol=symbol) from exc
        mv.index = mv.index.droplevel("instrument")
        yield symbol, bars, mv


def run(path: Path) -> dict[str, Any]:
    request = load_request(path)
    digest = request["request_sha256"]
    root = Path(request["timing_root"]) / "research" / FOLDER
    bundle = root / "bundles" / digest
    with _exclusive_file_lock(root / "locks" / f"{digest}.lock"):
        if (bundle / "manifest.json").exists():
            return {**inspect(bundle, request_sha256=digest), "status": "ALREADY_MATERIALIZED"}
        candidate = DailyCandidate.open(Path(request["candidate_root"]))
        identity = open_r8_candidate_identity(candidate.root)
        memberships = _open_memberships(candidate, _identity_payload(identity))
        _validate_population(candidate, request)
        market_cap = read_total_market_cap(identity)
        market_cap_groups = market_cap.groupby(level="instrument", sort=False)
        print(json.dumps({"stage": "RUN_AFTER_FULL_SOURCE_PREFLIGHT", "outcomes_read": True}), flush=True)
        chunks: list[dict[str, Any]] = []
        for offset in range(0, len(candidate.symbols), CHUNK_SIZE):
            chunk = root / "chunks" / digest / f"{offset // CHUNK_SIZE:04d}"
            if (chunk / "manifest.json").exists():
                inspect(chunk, request_sha256=digest)
            else:
                selected = candidate.symbols[offset:offset + CHUNK_SIZE]
                totals: dict[tuple[str, str], dict[str, np.ndarray]] = {}
                stock_rows: list[dict[str, Any]] = []
                fill_frames: list[pd.DataFrame] = []
                diagnostics: list[dict[str, Any]] = []
                audits: list[dict[str, Any]] = []
                pit_masks: dict[str, np.ndarray] = {}

                def inputs() -> Iterator[tuple[str, pd.DataFrame, pd.Series]]:
                    for symbol, bars, mv in _bar_inputs(candidate, request, selected, market_cap_groups):
                        pit_masks[symbol] = bars.pit_active.to_numpy(bool)
                        yield symbol, bars, mv

                for symbol, days, fills, details, audit in _ordered_replays(
                    inputs(), worker_count=WORKER_COUNT, max_in_flight=MAX_IN_FLIGHT,
                ):
                    if not fills.empty:
                        fill_frames.append(fills)
                    audits.append(audit)
                    diagnostics.extend(details)
                    for detail in details:
                        selected_path = None
                        if detail["status"] == "REPLAYED":
                            selected_path = days.loc[days.policy_id == detail["policy_id"]]
                        stock_rows.append(_add_path(
                            symbol=symbol,
                            policy_id=detail["policy_id"],
                            path=selected_path,
                            detail=detail,
                            pit=pit_masks[symbol],
                            memberships=memberships,
                            calendar=candidate.calendar,
                            totals=totals,
                        ))
                    pit_masks.pop(symbol)
                if pit_masks:
                    raise ActionValueError("FUNDAMENTAL_PARENT_INPUT_CACHE_DRIFT")
                publish_frame(chunk / "partials.parquet", _partial_frame(totals))
                publish_frame(chunk / "stocks.parquet", pd.DataFrame(stock_rows))
                publish_frame(chunk / "fills.parquet", _concat(fill_frames))
                publish_json(chunk / "diagnostics.json", diagnostics)
                publish_json(chunk / "screen_audit.json", audits)
                _seal(
                    chunk,
                    ["partials.parquet", "stocks.parquet", "fills.parquet", "diagnostics.json", "screen_audit.json"],
                    request_sha256=digest,
                )
            chunks.append(file_reference(chunk / "manifest.json"))
            print(json.dumps({
                "stage": "CHUNK_COMPLETE",
                "symbols_complete": min(offset + CHUNK_SIZE, len(candidate.symbols)),
                "total": len(candidate.symbols),
            }), flush=True)

        partials: list[pd.DataFrame] = []
        stocks_frames: list[pd.DataFrame] = []
        fills_frames: list[pd.DataFrame] = []
        diagnostics: list[dict[str, Any]] = []
        audits: list[dict[str, Any]] = []
        for reference in chunks:
            check_ref(reference)
            chunk = Path(reference["path"]).parent
            inspect(chunk, request_sha256=digest)
            partials.append(pd.read_parquet(chunk / "partials.parquet"))
            stocks_frames.append(pd.read_parquet(chunk / "stocks.parquet"))
            fills_frames.append(pd.read_parquet(chunk / "fills.parquet"))
            diagnostics.extend(read_json(chunk / "diagnostics.json"))
            audits.extend(read_json(chunk / "screen_audit.json"))
        totals = _merge_partials(partials, len(candidate.calendar))
        stocks = pd.concat(stocks_frames, ignore_index=True)
        fills = _concat(fills_frames)
        check_ref(request["index_source"])
        index_frame = pd.read_hdf(request["index_source"]["path"], key="data")
        report, pool_daily = _build_report(
            totals=totals,
            stocks=stocks,
            fills=fills,
            audits=audits,
            calendar=candidate.calendar,
            index_frame=index_frame,
        )
        load_request(path)
        publish_json(bundle / "request.json", request)
        publish_json(bundle / "report.json", report)
        publish_frame(bundle / "stocks.parquet", stocks)
        publish_frame(bundle / "pool_daily.parquet", pool_daily)
        publish_frame(bundle / "fills.parquet", fills)
        publish_json(bundle / "screen_audit.json", audits)
        receipt = {
            "schema_version": RECEIPT_SCHEMA,
            "request_sha256": digest,
            "chunks": chunks,
            "symbol_count": len(candidate.symbols),
            "screen_count": 3,
            "policy_count": len(POLICY_IDS),
            "hypothesis_family_size": FAMILY_SIZE,
            "financial_groups": unavailable_financial_groups(),
            "outcomes_read_stage": "RUN_AFTER_FULL_SOURCE_PREFLIGHT",
            "source_preflight_complete": True,
            "result_class": "EXPLORATORY_HYPOTHESIS_GENERATED",
            "selected_trial_count": 0,
            "database_read": False,
            "database_write": False,
            "network_accessed": False,
            "runtime_action_performed": False,
            "service_process_control_performed": False,
            "research_worker_processes_used": True,
            "parallel_execution": CONTRACT["parallel_execution"],
            "corporate_action_authority_read": False,
            "account_economics_simulated": False,
            "broker_account_clearing": False,
        }
        publish_json(bundle / "receipt.json", receipt)
        _seal(
            bundle,
            ["request.json", "report.json", "stocks.parquet", "pool_daily.parquet", "fills.parquet", "screen_audit.json", "receipt.json"],
            request_sha256=digest,
        )
        return inspect(bundle, request_sha256=digest)


def verify_parallel(path: Path, *, size: int = 16) -> dict[str, Any]:
    request = load_request(path)
    candidate = DailyCandidate.open(Path(request["candidate_root"]))
    identity = open_r8_candidate_identity(candidate.root)
    market_cap = read_total_market_cap(identity)
    groups = market_cap.groupby(level="instrument", sort=False)
    symbols = candidate.symbols[:size]
    inputs = list(_bar_inputs(candidate, request, symbols, groups))

    def digest(result: tuple[Any, ...]) -> str:
        symbol, days, fills, details, audit = result
        return canonical_sha256({
            "symbol": symbol,
            "days": hashlib.sha256(days.to_json(orient="table", index=False, double_precision=15).encode()).hexdigest(),
            "fills": hashlib.sha256(fills.to_json(orient="table", index=False, double_precision=15).encode()).hexdigest(),
            "details": details,
            "audit": audit,
        })

    sequential = {result[0]: digest(result) for result in _ordered_replays(inputs, worker_count=1, max_in_flight=1)}
    parallel = {result[0]: digest(result) for result in _ordered_replays(inputs, worker_count=WORKER_COUNT, max_in_flight=MAX_IN_FLIGHT)}
    if sequential != parallel:
        raise ActionValueError("FUNDAMENTAL_PARALLEL_RESULT_DRIFT")
    audit = {
        "schema_version": "position_timing_fundamental_parallel_verification_v1",
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
    commands.add_parser("verify-parallel").add_argument("--request", type=Path, required=True)
    commands.add_parser("inspect").add_argument("--bundle", type=Path, required=True)
    args = vars(parser.parse_args())
    command = args.pop("command")
    if command == "prepare":
        result = {"request": file_reference(prepare(**args))}
    elif command == "run":
        result = run(args["request"])
    elif command == "verify-parallel":
        result = verify_parallel(args["request"])
    else:
        result = inspect(args["bundle"])
    print(json.dumps(result, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
