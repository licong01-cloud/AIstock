"""Immutable offline benchmark for the frozen PT-NEXT-021 strategy set."""

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
from pathlib import PureWindowsPath
import re
from typing import Any, Iterable, Iterator, Mapping

import numpy as np
import pandas as pd

from .action_value import ActionValueError
from .action_value_data import DailyCandidate, file_reference
from .action_value_pipeline import _clean_repository_commit
from .artifact_store import _exclusive_file_lock
from .contracts import canonical_sha256
from .pattern_adj_factor_restatement import (
    audit_candidate_adj_factor_restatement,
    open_adj_factor_restatement_authority,
)
from .pattern_close_cash_benchmark import (
    INDEX_FILE_SHA,
    check_ref,
    publish_frame,
    publish_json,
    read_json,
    sources,
)
from .pattern_close_cash_replay import CAPITAL
from .pattern_close_cash_report import INDEX_CODES, comparison, index_returns, performance
from .pattern_strategy_evolution import (
    START_MODES,
    STRATEGIES,
    STRATEGY_SET_SHA256,
    replay_strategy_set,
)
from .pattern_universe_benchmark import (
    EXPECTED_ADJ_FACTOR_RESTATEMENT_AUTHORITY_SHA256,
    EXPECTED_PARENT_MANIFEST_SHA256,
    EXPECTED_POOL_FILES,
    POOL_IDS,
    QLIB_ADJUSTED_FACTOR_CONTRACT_SHA256,
    CandidatePoolMemberships,
    _audit_qlib_adjusted_factor_integrity,
    open_candidate_pool_memberships,
)
from .policy import (
    EXIT_GUARD_RULE_DEFAULT_SNAPSHOT_V1,
    PERSONAL_MANUAL_COMPONENT_COST_V1,
    PRICE_GUARD_RULE_DEFAULT_SNAPSHOT_V1,
)


PIPELINE_ID = "POSITION_TIMING_CLOSE_CASH_STRATEGY_EVOLUTION_V1"
FOLDER = "pattern_strategy_evolution_v1"
REQUEST_SCHEMA = "position_timing_strategy_evolution_request_v2"
RECEIPT_SCHEMA = "position_timing_strategy_evolution_receipt_v2"
MANIFEST_SCHEMA = "position_timing_strategy_evolution_files_v1"
CHUNK_SIZE = 128
WORKER_COUNT = 8
MAX_IN_FLIGHT = 16
BOOTSTRAP_REPLICATES = 5_000
BOOTSTRAP_BLOCK = 25
BOOTSTRAP_SEED = 20260918
FAMILY_SIZE = 54
BASELINE_CLOSE_CASH_REQUEST_SHA256 = "745c4ea3b5038532535bb86c71ac192891ac30cbac532ae6f81c543511c9f2ff"
BASELINE_CLOSE_CASH_MANIFEST_SHA256 = "a304822c9c6bb09ca4fae151d35e6dad48c17944226f0cf5c47258b02191d444"
BASELINE_CLOSE_CASH_MANIFEST_FILE_SHA256 = "58c4127d3f501647c9add304e8f31668124394b4e77b4336021f786cf04f22fe"
EXPECTED_R5_CANDIDATE_MANIFEST_SHA256 = "7b5402c38b4b279140375fa6517595f88bdfb472e617faf8b032c04f0d33d1c1"
EXPECTED_R7_CANDIDATE_MANIFEST_SHA256 = "084ffe869dafe919e73e884afa6a833c497df4ab09e1bfb7f649993043bc6900"
EXPECTED_R7_CANDIDATE_DATASET_SHA256 = "c11e16ee15719c0b96af4a6fafcaa338858f541eedc6b3a0768b34621e202836"
EXPECTED_R5_SUSPEND_SHA256 = "f8af78479eb6d8709a037e5abe19258e65c5c895eaccc4ccd79c6f96753697f9"
EXPECTED_R7_SUSPEND_SHA256 = "7f1895fe3c3a025708c1afa525d17e594f536180a86d6a9cf22b3c25d87bee3f"
EXPECTED_R7_DEPLOYMENT_CONTENT_SHA256 = "372c41a147fe252596379878ebbeadc6601b195d744a144026a6308fdc73f257"
_WINDOWS_ABSOLUTE = re.compile(r"^[A-Za-z]:[\\/]")
UNIFORM_PERIOD_START = date(2023, 8, 8)
CONTRACT = {
    "pipeline_id": PIPELINE_ID,
    "capital_per_stock_per_account_cny": "10000000",
    "strategy_set": [item.identity for item in STRATEGIES],
    "strategy_set_sha256": STRATEGY_SET_SHA256,
    "start_modes": list(START_MODES),
    "decision": "T_CLOSE",
    "fill": "T_PLUS_1_CLOSE",
    "price_guard_default": PRICE_GUARD_RULE_DEFAULT_SNAPSHOT_V1,
    "open_gap_variant": "REAL_CONTEXT_WITH_EXPLICIT_NON_BINDING_OPEN_GAP_THRESHOLDS",
    "exit_guard": EXIT_GUARD_RULE_DEFAULT_SNAPSHOT_V1,
    "cost_policy": PERSONAL_MANUAL_COMPONENT_COST_V1,
    "quantity": "LEGAL_RAW_BUY_AND_INTERMEDIATE_SELL_VIRTUAL_UNITS",
    "cash": "NATURAL_REINVESTMENT_NO_INTER_STOCK_TRANSFER_NO_INTEREST",
    "leverage": False,
    "slippage_bps": 0,
    "market_impact": False,
    "benchmark_by_pool": INDEX_CODES,
    "pool_aggregation": "LAGGED_PIT_EQUAL_ACCOUNT_DAILY_PERCENT_RETURN",
    "terminal": "COMMON_FINAL_CLOSE_MTM_SEPARATE_SELLABILITY",
    "bootstrap": {
        "replicates": BOOTSTRAP_REPLICATES,
        "block_sessions": BOOTSTRAP_BLOCK,
        "seed": BOOTSTRAP_SEED,
        "family_size": FAMILY_SIZE,
    },
    "result_class": "EXPLORATORY_HYPOTHESIS_GENERATED",
    "corporate_action_authority_read": False,
    "account_economics_simulated": False,
    "broker_account_clearing": False,
    "parallel_execution": {
        "executor": "PROCESS_POOL_EXECUTOR",
        "start_method": "spawn",
        "worker_count": WORKER_COUNT,
        "max_in_flight": MAX_IN_FLIGHT,
        "chunk_size": CHUNK_SIZE,
        "writer": "PARENT_PROCESS_ONLY",
        "result_order": "CANONICAL_SYMBOL_ORDER",
    },
}


def _environment_identity() -> dict[str, str]:
    identity = {
        "system": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
        "python": platform.python_version(),
        "numpy": np.__version__,
        "pandas": pd.__version__,
        "pyarrow": importlib.metadata.version("pyarrow"),
    }
    return {**identity, "environment_sha256": canonical_sha256(identity)}


def _portable_reference_path(reference: Mapping[str, Any]) -> Path:
    """Resolve an immutable Windows-authored reference from WSL without rewriting it."""

    raw = str(reference.get("path") or "")
    direct = Path(raw)
    candidates = [direct]
    if platform.system() == "Linux" and _WINDOWS_ABSOLUTE.match(raw):
        windows = PureWindowsPath(raw)
        candidates.insert(0, Path("/mnt") / windows.drive[0].lower() / Path(*windows.parts[1:]))
    for candidate in candidates:
        if not candidate.is_file():
            continue
        observed = file_reference(candidate)
        if observed["sha256"] == reference.get("sha256") and observed["size_bytes"] == reference.get("size_bytes"):
            return candidate.resolve()
    raise ActionValueError("STRATEGY_EVOLUTION_PORTABLE_REFERENCE_DRIFT", path=raw)


def _validate_baseline_bundle(bundle: Path) -> dict[str, Any]:
    """Validate the immutable r5 bundle while preserving its Windows-authored manifest."""

    root = bundle.resolve()
    manifest_path = root / "manifest.json"
    manifest_reference = file_reference(manifest_path)
    manifest = read_json(manifest_path)
    expected_files = {
        "request.json",
        "report.json",
        "stocks.parquet",
        "pool_daily.parquet",
        "receipt.json",
    }
    identity = {key: value for key, value in manifest.items() if key != "manifest_sha256"}
    if (
        manifest_reference["sha256"] != BASELINE_CLOSE_CASH_MANIFEST_FILE_SHA256
        or manifest.get("manifest_sha256") != BASELINE_CLOSE_CASH_MANIFEST_SHA256
        or canonical_sha256(identity) != BASELINE_CLOSE_CASH_MANIFEST_SHA256
        or manifest.get("request_sha256") != BASELINE_CLOSE_CASH_REQUEST_SHA256
        or manifest.get("schema_version") != "position_timing_close_cash_files_v1"
        or set(manifest.get("files") or {}) != expected_files
    ):
        raise ActionValueError("STRATEGY_EVOLUTION_BASELINE_IDENTITY_DRIFT")
    resolved_files = {name: _portable_reference_path(reference) for name, reference in manifest["files"].items()}
    request = read_json(resolved_files["request.json"])
    if (
        request.get("request_sha256") != BASELINE_CLOSE_CASH_REQUEST_SHA256
        or canonical_sha256({key: value for key, value in request.items() if key != "request_sha256"})
        != BASELINE_CLOSE_CASH_REQUEST_SHA256
    ):
        raise ActionValueError("STRATEGY_EVOLUTION_BASELINE_REQUEST_DRIFT")
    return {
        "manifest": manifest,
        "manifest_reference": manifest_reference,
        "resolved_files": {name: path.as_posix() for name, path in resolved_files.items()},
    }


def _read_candidate_manifest(root: Path, *, expected_sha256: str) -> tuple[dict[str, Any], dict[str, Any]]:
    path = root.resolve() / "qe_dataset_manifest.json"
    reference = file_reference(path)
    if reference["sha256"] != expected_sha256:
        raise ActionValueError("STRATEGY_EVOLUTION_CANDIDATE_MANIFEST_DRIFT")
    payload = read_json(path)
    identity = {key: value for key, value in payload.items() if key != "dataset_manifest_sha256"}
    if payload.get("dataset_manifest_sha256") != canonical_sha256(identity):
        raise ActionValueError("STRATEGY_EVOLUTION_CANDIDATE_CANONICAL_DRIFT")
    return payload, reference


def _manifest_component_reference(root: Path, manifest: Mapping[str, Any], name: str) -> dict[str, Any]:
    component = (manifest.get("components") or {}).get(name)
    if not isinstance(component, Mapping):
        raise ActionValueError("STRATEGY_EVOLUTION_COMPONENT_MISSING", component=name)
    path = (root.resolve() / str(component.get("path") or "")).resolve()
    if not path.is_relative_to(root.resolve()):
        raise ActionValueError("STRATEGY_EVOLUTION_COMPONENT_SCOPE_DRIFT", component=name)
    reference = file_reference(path)
    if reference["sha256"] != component.get("sha256") or reference["size_bytes"] != component.get("size"):
        raise ActionValueError("STRATEGY_EVOLUTION_COMPONENT_IDENTITY_DRIFT", component=name)
    return reference


def _normalized_suspend_rows(frame: pd.DataFrame) -> set[tuple[str, str, str, str | None]]:
    required = {"trade_date", "ts_code", "suspend_type", "suspend_timing"}
    if set(frame.columns) != required:
        raise ActionValueError("STRATEGY_EVOLUTION_SUSPEND_SCHEMA_DRIFT")
    rows: set[tuple[str, str, str, str | None]] = set()
    for row in frame.itertuples(index=False):
        timing = None if pd.isna(row.suspend_timing) else str(row.suspend_timing)
        rows.add(
            (
                pd.Timestamp(row.trade_date).date().isoformat(),
                str(row.ts_code),
                str(row.suspend_type),
                timing,
            )
        )
    if len(rows) != len(frame):
        raise ActionValueError("STRATEGY_EVOLUTION_SUSPEND_DUPLICATE_DRIFT")
    return rows


def _dataset_delta_audit(*, prior_candidate_root: Path, candidate_root: Path) -> dict[str, Any]:
    prior_root = prior_candidate_root.resolve()
    current_root = candidate_root.resolve()
    prior, prior_ref = _read_candidate_manifest(prior_root, expected_sha256=EXPECTED_R5_CANDIDATE_MANIFEST_SHA256)
    current, current_ref = _read_candidate_manifest(current_root, expected_sha256=EXPECTED_R7_CANDIDATE_MANIFEST_SHA256)
    if (
        current.get("dataset_manifest_sha256") != EXPECTED_R7_CANDIDATE_DATASET_SHA256
        or current.get("deployment_content_sha256") != EXPECTED_R7_DEPLOYMENT_CONTENT_SHA256
        or current.get("revision") != "20260918-r7"
    ):
        raise ActionValueError("STRATEGY_EVOLUTION_R7_IDENTITY_DRIFT")

    unchanged_components = (
        "day_calendar",
        "day_meta_export",
        "day_provider_catalog",
        "day_selection_universe",
        "index_daily",
        "index_meta",
        "adj_factor_restatement_authority",
        "rights_issue_authority",
    )
    component_audit: dict[str, Any] = {}
    for name in unchanged_components:
        prior_component = _manifest_component_reference(prior_root, prior, name)
        current_component = _manifest_component_reference(current_root, current, name)
        same = {key: prior_component[key] == current_component[key] for key in ("sha256", "size_bytes")}
        if not all(same.values()):
            raise ActionValueError("STRATEGY_EVOLUTION_UNEXPECTED_COMPONENT_DELTA", component=name)
        component_audit[name] = {
            "sha256": current_component["sha256"],
            "size_bytes": current_component["size_bytes"],
        }
    prior_sidecars = (prior.get("st_pit_manifest") or {}).get("index_membership_sidecars") or {}
    current_sidecars = (current.get("st_pit_manifest") or {}).get("index_membership_sidecars") or {}
    if prior_sidecars != current_sidecars:
        raise ActionValueError("STRATEGY_EVOLUTION_POOL_SIDECAR_DELTA")

    prior_suspend = _manifest_component_reference(prior_root, prior, "suspend_data")
    current_suspend = _manifest_component_reference(current_root, current, "suspend_data")
    if prior_suspend["sha256"] != EXPECTED_R5_SUSPEND_SHA256 or current_suspend["sha256"] != EXPECTED_R7_SUSPEND_SHA256:
        raise ActionValueError("STRATEGY_EVOLUTION_SUSPEND_IDENTITY_DRIFT")
    prior_rows = _normalized_suspend_rows(pd.read_parquet(prior_suspend["path"]))
    current_rows = _normalized_suspend_rows(pd.read_parquet(current_suspend["path"]))
    raw_added = sorted(current_rows - prior_rows)
    raw_removed = sorted(prior_rows - current_rows)
    prior_strategy_keys = {(row[1], row[0]) for row in prior_rows if row[2] == "S"}
    current_strategy_keys = {(row[1], row[0]) for row in current_rows if row[2] == "S"}
    strategy_added = sorted(current_strategy_keys - prior_strategy_keys)
    strategy_removed = sorted(prior_strategy_keys - current_strategy_keys)
    affected_symbols = sorted({row[0] for row in strategy_added + strategy_removed})
    expected_dates = [
        "2025-11-27",
        "2025-11-28",
        "2025-12-01",
        "2025-12-02",
        "2025-12-03",
        "2025-12-04",
        "2025-12-05",
    ]
    if (
        strategy_removed
        or affected_symbols != ["688766.SH"]
        or [row[1] for row in strategy_added] != expected_dates
        or raw_removed
        != [
            ("2018-08-28", "000979.SZ", "S", None),
            ("2025-11-26", "688766.SH", "S", "09:30-09:30"),
        ]
        or raw_added
        != [
            ("2018-08-28", "000979.SZ", "S", "10:37-15:00"),
            ("2025-11-26", "688766.SH", "S", None),
            *[(day, "688766.SH", "S", None) for day in expected_dates],
        ]
    ):
        raise ActionValueError("STRATEGY_EVOLUTION_SUSPEND_DELTA_DRIFT")
    identity = {
        "schema_version": "position_timing_r5_r7_strategy_input_delta_v1",
        "prior_candidate_manifest": prior_ref,
        "prior_candidate_dataset_manifest_sha256": prior["dataset_manifest_sha256"],
        "current_candidate_manifest": current_ref,
        "current_candidate_dataset_manifest_sha256": current["dataset_manifest_sha256"],
        "current_deployment_content_sha256": current["deployment_content_sha256"],
        "current_revision": current["revision"],
        "unchanged_components": component_audit,
        "pool_sidecars_sha256": canonical_sha256(current_sidecars),
        "prior_suspend": prior_suspend,
        "current_suspend": current_suspend,
        "prior_suspend_row_count": len(prior_rows),
        "current_suspend_row_count": len(current_rows),
        "raw_added_rows": [
            {
                "trade_date": row[0],
                "symbol": row[1],
                "suspend_type": row[2],
                "suspend_timing": row[3],
            }
            for row in raw_added
        ],
        "raw_removed_rows": [
            {
                "trade_date": row[0],
                "symbol": row[1],
                "suspend_type": row[2],
                "suspend_timing": row[3],
            }
            for row in raw_removed
        ],
        "strategy_suspension_added": [
            {"symbol": symbol, "trade_date": trade_date} for symbol, trade_date in strategy_added
        ],
        "strategy_suspension_removed": [],
        "affected_symbols": affected_symbols,
        "r5_evolution_chunks_reusable": False,
    }
    return {**identity, "audit_sha256": canonical_sha256(identity)}


def _r7_pool_memberships(candidate: DailyCandidate) -> CandidatePoolMemberships:
    return open_candidate_pool_memberships(
        candidate,
        expected_candidate_manifest_sha256=EXPECTED_R7_CANDIDATE_MANIFEST_SHA256,
        expected_candidate_dataset_sha256=EXPECTED_R7_CANDIDATE_DATASET_SHA256,
        expected_pool_files=EXPECTED_POOL_FILES,
    )


def _replay_symbol_task(
    symbol: str, bars: pd.DataFrame
) -> tuple[str, pd.DataFrame, pd.DataFrame, list[dict[str, Any]]]:
    days, fills, details = replay_strategy_set(symbol, bars)
    return symbol, days, fills, details


def _ordered_replays(
    inputs: Iterable[tuple[str, pd.DataFrame]],
    *,
    worker_count: int,
    max_in_flight: int,
) -> Iterator[tuple[str, pd.DataFrame, pd.DataFrame, list[dict[str, Any]]]]:
    if worker_count < 1 or max_in_flight < worker_count:
        raise ActionValueError("STRATEGY_EVOLUTION_PARALLEL_CONTRACT_INVALID")
    if worker_count == 1:
        for symbol, bars in inputs:
            yield _replay_symbol_task(symbol, bars)
        return

    executor = ProcessPoolExecutor(
        max_workers=worker_count,
        mp_context=multiprocessing.get_context("spawn"),
    )
    pending: dict[int, tuple[str, Future[Any]]] = {}
    iterator = iter(inputs)
    submit_index = 0
    consume_index = 0
    exhausted = False
    try:
        while not exhausted or pending:
            while not exhausted and len(pending) < max_in_flight:
                try:
                    symbol, bars = next(iterator)
                except StopIteration:
                    exhausted = True
                    break
                pending[submit_index] = (
                    symbol,
                    executor.submit(_replay_symbol_task, symbol, bars),
                )
                submit_index += 1
            if consume_index not in pending:
                if exhausted:
                    break
                continue
            expected_symbol, future = pending.pop(consume_index)
            result = future.result()
            if result[0] != expected_symbol:
                raise ActionValueError("STRATEGY_EVOLUTION_WORKER_ORDER_DRIFT")
            yield result
            consume_index += 1
    finally:
        for _, future in pending.values():
            future.cancel()
        executor.shutdown(wait=True, cancel_futures=True)


def _frame_sha256(frame: pd.DataFrame) -> str:
    payload = frame.to_json(
        orient="table",
        index=False,
        date_format="iso",
        double_precision=15,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _symbol_result_sha256(result: tuple[str, pd.DataFrame, pd.DataFrame, list[dict[str, Any]]]) -> str:
    symbol, days, fills, details = result
    return canonical_sha256(
        {
            "symbol": symbol,
            "days_sha256": _frame_sha256(days),
            "fills_sha256": _frame_sha256(fills),
            "details_sha256": canonical_sha256(details),
        }
    )


def _concat_frames_by_records(frames: Iterable[pd.DataFrame]) -> pd.DataFrame:
    """Preserve frame/row/column order without pandas' all-NA concat inference."""

    columns: list[str] = []
    records: list[dict[str, Any]] = []
    for frame in frames:
        for column in frame.columns:
            if column not in columns:
                columns.append(str(column))
        records.extend(frame.to_dict(orient="records"))
    return pd.DataFrame.from_records(records, columns=columns)


def _parallel_verification_symbols(symbols: Iterable[str], *, size: int = 32) -> list[str]:
    population = tuple(symbols)
    available = set(population)
    mandatory: list[str] = []
    for predicate in (
        lambda value: value.startswith("000"),
        lambda value: value.startswith("300"),
        lambda value: value.startswith("600"),
        lambda value: value.startswith("688"),
    ):
        selected = next((symbol for symbol in population if predicate(symbol)), None)
        if selected is None:
            raise ActionValueError("STRATEGY_EVOLUTION_PARALLEL_SAMPLE_INCOMPLETE")
        mandatory.append(selected)
    if "688766.SH" not in available:
        raise ActionValueError("STRATEGY_EVOLUTION_VERSION_DELTA_SYMBOL_MISSING")
    mandatory.append("688766.SH")
    ranked = sorted(
        (symbol for symbol in population if symbol not in mandatory),
        key=lambda symbol: (hashlib.sha256(symbol.encode("ascii")).hexdigest(), symbol),
    )
    selected = set(mandatory + ranked[: max(0, size - len(mandatory))])
    if len(selected) != size:
        raise ActionValueError("STRATEGY_EVOLUTION_PARALLEL_SAMPLE_SIZE_DRIFT")
    return [symbol for symbol in population if symbol in selected]


def _seal(root: Path, names: list[str], *, request_hash: str) -> dict[str, Any]:
    files = {name: file_reference(root / name) for name in names}
    value = {
        "schema_version": MANIFEST_SCHEMA,
        "request_sha256": request_hash,
        "files": files,
    }
    value["manifest_sha256"] = canonical_sha256(value)
    publish_json(root / "manifest.json", value)
    return value


def inspect(root: Path, *, request_hash: str | None = None) -> dict[str, Any]:
    manifest = read_json(root / "manifest.json")
    expected_hash = canonical_sha256({key: value for key, value in manifest.items() if key != "manifest_sha256"})
    if (
        manifest.get("schema_version") != MANIFEST_SCHEMA
        or manifest.get("manifest_sha256") != expected_hash
        or (request_hash is not None and manifest.get("request_sha256") != request_hash)
    ):
        raise ActionValueError("STRATEGY_EVOLUTION_MANIFEST_DRIFT")
    for name, reference in manifest["files"].items():
        target = (root / name).resolve()
        if Path(reference["path"]).resolve() != target or not target.is_relative_to(root.resolve()):
            raise ActionValueError("STRATEGY_EVOLUTION_FILE_SCOPE_DRIFT")
        check_ref(reference)
    if "receipt.json" in manifest["files"]:
        expected = {
            "request.json",
            "report.json",
            "stocks.parquet",
            "pool_daily.parquet",
            "fills.parquet",
            "receipt.json",
        }
        if set(manifest["files"]) != expected:
            raise ActionValueError("STRATEGY_EVOLUTION_BUNDLE_INCOMPLETE")
        request = read_json(root / "request.json")
        receipt = read_json(root / "receipt.json")
        if (
            request.get("request_sha256") != manifest["request_sha256"]
            or canonical_sha256({key: value for key, value in request.items() if key != "request_sha256"})
            != manifest["request_sha256"]
            or receipt.get("request_sha256") != manifest["request_sha256"]
            or len(receipt.get("chunks", [])) != (len(request["symbols"]) + CHUNK_SIZE - 1) // CHUNK_SIZE
        ):
            raise ActionValueError("STRATEGY_EVOLUTION_BUNDLE_IDENTITY_DRIFT")
        for reference in receipt["chunks"]:
            check_ref(reference)
            inspect(Path(reference["path"]).parent, request_hash=manifest["request_sha256"])
    return {
        "status": "VERIFIED",
        "bundle": root.as_posix(),
        "manifest_sha256": manifest["manifest_sha256"],
        "request_sha256": manifest["request_sha256"],
    }


def prepare(
    *,
    timing_root: Path,
    repository_root: Path,
    parent_pattern_bundle: Path,
    baseline_close_cash_bundle: Path,
    prior_candidate_root: Path,
    candidate_root: Path,
) -> Path:
    repository = repository_root.resolve()
    root = timing_root.resolve()
    absolute_inputs = (
        timing_root,
        repository_root,
        parent_pattern_bundle,
        baseline_close_cash_bundle,
        prior_candidate_root,
        candidate_root,
    )
    if (
        not all(path.is_absolute() for path in absolute_inputs)
        or root.is_relative_to(repository)
        or root.is_relative_to(candidate_root.resolve())
        or root.is_relative_to(prior_candidate_root.resolve())
        or repository != Path(__file__).resolve().parents[3]
    ):
        raise ActionValueError("STRATEGY_EVOLUTION_PATH_SCOPE_INVALID")
    commit = _clean_repository_commit(repository)
    code = sources(repository)
    candidate = DailyCandidate.open(candidate_root)
    pools = _r7_pool_memberships(candidate)
    if (
        candidate.calendar[0].date(),
        candidate.calendar[-1].date(),
        len(candidate.calendar),
    ) != (date(2018, 8, 1), date(2026, 8, 31), 1961):
        raise ActionValueError("STRATEGY_EVOLUTION_CALENDAR_DRIFT")
    parent_ref = file_reference(parent_pattern_bundle / "manifest.json")
    parent = read_json(Path(parent_ref["path"]))
    if (
        parent.get("manifest_sha256") != EXPECTED_PARENT_MANIFEST_SHA256
        or canonical_sha256({key: value for key, value in parent.items() if key != "manifest_sha256"})
        != EXPECTED_PARENT_MANIFEST_SHA256
    ):
        raise ActionValueError("STRATEGY_EVOLUTION_PARENT_IDENTITY_DRIFT")
    authority = open_adj_factor_restatement_authority(
        candidate_root=candidate.root,
        expected_candidate_manifest_sha256=EXPECTED_R7_CANDIDATE_MANIFEST_SHA256,
        expected_authority_canonical_sha256=EXPECTED_ADJ_FACTOR_RESTATEMENT_AUTHORITY_SHA256,
    )
    restatement = audit_candidate_adj_factor_restatement(candidate, authority)
    version_delta = _dataset_delta_audit(
        prior_candidate_root=prior_candidate_root,
        candidate_root=candidate.root,
    )
    candidate_identity = canonical_sha256(
        {
            "candidate_manifest": pools.candidate_manifest_reference,
            "candidate_dataset_manifest_sha256": pools.candidate_dataset_manifest_sha256,
            "pool_sidecars": pools.references,
        }
    )
    print(
        json.dumps(
            {
                "stage": "SOURCE_FACTOR_PREFLIGHT",
                "symbols": len(candidate.symbols),
                "outcomes_read": False,
            }
        ),
        flush=True,
    )
    factors = _audit_qlib_adjusted_factor_integrity(
        candidate,
        symbols=candidate.symbols,
        start=candidate.calendar[0].date(),
        end=candidate.calendar[-1].date(),
        candidate_source_sha256=candidate_identity,
    )
    if not factors["coverage_complete"] or not restatement["coverage_complete"]:
        raise ActionValueError("STRATEGY_EVOLUTION_SOURCE_PREFLIGHT_FAILED")
    index_ref = file_reference(candidate.root / "components/index_context/index_daily.h5")
    if index_ref["sha256"] != INDEX_FILE_SHA:
        raise ActionValueError("STRATEGY_EVOLUTION_INDEX_SOURCE_DRIFT")
    check_ref(parent_ref)
    baseline_verified = _validate_baseline_bundle(baseline_close_cash_bundle)
    baseline_ref = baseline_verified["manifest_reference"]
    if sources(repository) != code:
        raise ActionValueError("STRATEGY_EVOLUTION_CODE_DRIFT")
    request: dict[str, Any] = {
        "schema_version": REQUEST_SCHEMA,
        "contract": CONTRACT,
        "contract_sha256": canonical_sha256(CONTRACT),
        "repository_commit": commit,
        "repository_root": repository.as_posix(),
        "source_code": code,
        "environment": _environment_identity(),
        "timing_root": root.as_posix(),
        "candidate_root": candidate.root.as_posix(),
        "candidate_manifest": pools.candidate_manifest_reference,
        "candidate_dataset_manifest_sha256": pools.candidate_dataset_manifest_sha256,
        "parent_manifest": parent_ref,
        "parent_manifest_sha256": EXPECTED_PARENT_MANIFEST_SHA256,
        "baseline_close_cash_manifest": baseline_ref,
        "baseline_close_cash_manifest_sha256": BASELINE_CLOSE_CASH_MANIFEST_SHA256,
        "dataset_version_delta_audit": version_delta,
        "symbols": candidate.symbols,
        "pool_sidecars": pools.references,
        "calendar": [str(stamp.date()) for stamp in candidate.calendar],
        "index_source": index_ref,
        "source_data": candidate.references,
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
    identity = canonical_sha256({key: value for key, value in request.items() if key != "request_sha256"})
    if (
        request.get("request_sha256") != identity
        or request.get("contract_sha256") != canonical_sha256(CONTRACT)
        or canonical_sha256(request.get("contract")) != canonical_sha256(CONTRACT)
        or request.get("schema_version") != REQUEST_SCHEMA
    ):
        raise ActionValueError("STRATEGY_EVOLUTION_REQUEST_DRIFT")
    owner = Path(request["timing_root"]).resolve()
    repository = Path(request["repository_root"]).resolve()
    expected = owner / "research" / FOLDER / "requests" / f"{identity}.json"
    if (
        path.resolve() != expected
        or repository != Path(__file__).resolve().parents[3]
        or owner.is_relative_to(repository)
        or owner.is_relative_to(Path(request["candidate_root"]).resolve())
    ):
        raise ActionValueError("STRATEGY_EVOLUTION_REQUEST_SCOPE_DRIFT")
    if (
        request.get("source_preflight_complete") is not True
        or any(
            request.get(key) is not False
            for key in (
                "outcomes_read",
                "database_read",
                "database_write",
                "network_accessed",
                "runtime_action_performed",
                "service_process_control_performed",
            )
        )
        or request.get("research_worker_processes_used") is not True
    ):
        raise ActionValueError("STRATEGY_EVOLUTION_REQUEST_BOUNDARY_DRIFT")
    expected_environment = _environment_identity()
    if request.get("environment") != expected_environment:
        raise ActionValueError("STRATEGY_EVOLUTION_ENVIRONMENT_DRIFT")
    for name in ("factor_audit", "restatement_audit"):
        audit = request[name]
        if audit.get("coverage_complete") is not True or audit.get("audit_sha256") != canonical_sha256(
            {key: value for key, value in audit.items() if key != "audit_sha256"}
        ):
            raise ActionValueError("STRATEGY_EVOLUTION_PREFLIGHT_DRIFT")
    if (
        request["candidate_manifest"]["sha256"] != EXPECTED_R7_CANDIDATE_MANIFEST_SHA256
        or request.get("candidate_dataset_manifest_sha256") != EXPECTED_R7_CANDIDATE_DATASET_SHA256
        or request.get("parent_manifest_sha256") != EXPECTED_PARENT_MANIFEST_SHA256
        or request.get("baseline_close_cash_manifest_sha256") != BASELINE_CLOSE_CASH_MANIFEST_SHA256
        or request["index_source"]["sha256"] != INDEX_FILE_SHA
        or request["qlib_contract_sha256"] != QLIB_ADJUSTED_FACTOR_CONTRACT_SHA256
    ):
        raise ActionValueError("STRATEGY_EVOLUTION_AUTHORITY_DRIFT")
    for reference in (
        request["candidate_manifest"],
        request["parent_manifest"],
        request["index_source"],
        request["restatement_authority"],
    ):
        check_ref(reference)
    _validate_baseline_bundle(_portable_reference_path(request["baseline_close_cash_manifest"]).parent)
    version_delta = request.get("dataset_version_delta_audit") or {}
    if (
        version_delta.get("audit_sha256")
        != canonical_sha256({key: value for key, value in version_delta.items() if key != "audit_sha256"})
        or version_delta.get("affected_symbols") != ["688766.SH"]
        or version_delta.get("r5_evolution_chunks_reusable") is not False
        or version_delta.get("current_candidate_dataset_manifest_sha256") != EXPECTED_R7_CANDIDATE_DATASET_SHA256
        or version_delta.get("current_deployment_content_sha256") != EXPECTED_R7_DEPLOYMENT_CONTENT_SHA256
        or version_delta.get("current_revision") != "20260918-r7"
    ):
        raise ActionValueError("STRATEGY_EVOLUTION_VERSION_DELTA_DRIFT")
    for group in ("source_code", "source_data", "pool_sidecars"):
        for reference in request[group].values():
            check_ref(reference)
    if sources(repository) != request["source_code"]:
        raise ActionValueError("STRATEGY_EVOLUTION_CODE_DRIFT")
    return request


def _empty_totals(calendar_size: int) -> dict[str, np.ndarray]:
    return {
        "timing_sum": np.zeros(calendar_size),
        "hold_sum": np.zeros(calendar_size),
        "expected": np.zeros(calendar_size, dtype=np.int64),
        "paired": np.zeros(calendar_size, dtype=np.int64),
        "unknown": np.zeros(calendar_size, dtype=np.int64),
    }


def _scope_masks(
    *,
    memberships: CandidatePoolMemberships,
    pool_id: str,
    symbol: str,
    calendar: pd.DatetimeIndex,
    stock_pit_mask: np.ndarray,
) -> Iterable[tuple[str, int, np.ndarray]]:
    membership = memberships.effective_mask(
        pool_id=pool_id,
        symbol=symbol,
        calendar=calendar,
        stock_pit_mask=stock_pit_mask,
    )
    dynamic = np.r_[False, membership[:-1]]
    yield "dynamic", 0, dynamic
    for year in sorted(set(calendar.year)):
        first = int(np.flatnonzero(calendar.year == year)[0])
        anchor = max(0, first - 1)
        yield "annual_fixed", year, (calendar.year == year) & bool(membership[anchor])


def _add_symbol_path(
    *,
    symbol: str,
    strategy_id: str,
    start_mode: str,
    path: pd.DataFrame | None,
    detail: dict[str, Any],
    stock_pit_mask: np.ndarray,
    memberships: CandidatePoolMemberships,
    calendar: pd.DatetimeIndex,
    totals: dict[tuple[str, str, str, str, int], dict[str, np.ndarray]],
) -> dict[str, Any]:
    n = len(calendar)
    full = np.full((n, 2), np.nan)
    start = detail.get("start_ordinal")
    if path is not None and not path.empty:
        ordered = path.sort_values("ordinal", kind="stable")
        ordinals = ordered.ordinal.to_numpy(int)
        full[ordinals, 0] = ordered.timing_nav.to_numpy(float)
        full[ordinals, 1] = ordered.hold_nav.to_numpy(float)
    returns = np.full_like(full, np.nan)
    returns[1:] = full[1:] / full[:-1] - 1
    valid = np.isfinite(returns).all(axis=1)
    for pool_id in POOL_IDS:
        for cohort_mode, year, expected in _scope_masks(
            memberships=memberships,
            pool_id=pool_id,
            symbol=symbol,
            calendar=calendar,
            stock_pit_mask=stock_pit_mask,
        ):
            key = (strategy_id, start_mode, pool_id, cohort_mode, year)
            aggregate = totals.setdefault(key, _empty_totals(n))
            paired = expected & valid
            aggregate["timing_sum"] += np.where(paired, returns[:, 0], 0.0)
            aggregate["hold_sum"] += np.where(paired, returns[:, 1], 0.0)
            aggregate["expected"] += expected
            aggregate["paired"] += paired
            aggregate["unknown"] += expected & ~valid
    result: dict[str, Any] = {
        "symbol": symbol,
        "strategy_id": strategy_id,
        "start_mode": start_mode,
        "status": detail["status"],
        "start_ordinal": start,
    }
    if path is None or path.empty:
        return result
    result.update(
        {
            "start": str(calendar[int(start)].date()),
            "end": str(calendar[-1].date()),
            "timing_terminal_mtm_return": full[-1, 0] / float(CAPITAL) - 1 if np.isfinite(full[-1, 0]) else None,
            "hold_terminal_mtm_return": full[-1, 1] / float(CAPITAL) - 1 if np.isfinite(full[-1, 1]) else None,
            "terminal_excess_return": (
                (full[-1, 0] - full[-1, 1]) / float(CAPITAL) if np.isfinite(full[-1]).all() else None
            ),
            "paired_unknown_sessions": int((~valid[int(start) + 1 :]).sum()),
            "daily_path_complete": bool(valid[int(start) + 1 :].all()),
            "timing_fees_cny": detail["terminal"]["timing"]["fees_cny"],
            "hold_fees_cny": detail["terminal"]["hold"]["fees_cny"],
            "timing_terminal_status": detail["terminal"]["timing"]["status"],
        }
    )
    stock_comparison = comparison(
        returns[int(start) + 1 :, 0],
        returns[int(start) + 1 :, 1],
        np.full(len(calendar) - int(start) - 1, np.nan),
    )
    flat = pd.json_normalize(stock_comparison, sep="_").iloc[0].to_dict()
    result.update(
        {key: value for key, value in flat.items() if not key.startswith("index_") and "_minus_index" not in key}
    )
    exposure = path.timing_exposure.to_numpy(float)
    invested = np.isfinite(exposure) & (exposure > 0)
    result.update(
        {
            "invested_session_fraction": float(invested.mean()),
            "conditional_exposure": float(exposure[invested].mean()) if invested.any() else 0.0,
            "average_exposure": float(np.nanmean(exposure)),
            "stale_mark_sessions": int(path.stale_mark.sum()),
        }
    )
    return result


def _partial_frame(
    totals: dict[tuple[str, str, str, str, int], dict[str, np.ndarray]],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for (strategy, start_mode, pool, cohort_mode, year), aggregate in totals.items():
        selected = np.flatnonzero(aggregate["expected"] > 0)
        for ordinal in selected:
            rows.append(
                {
                    "strategy_id": strategy,
                    "start_mode": start_mode,
                    "pool": pool,
                    "cohort_mode": cohort_mode,
                    "cohort_year": year,
                    "ordinal": ordinal,
                    **{name: values[ordinal] for name, values in aggregate.items()},
                }
            )
    return pd.DataFrame(rows)


def _merge_partials(
    partial_frames: Iterable[pd.DataFrame], calendar_size: int
) -> dict[tuple[str, str, str, str, int], dict[str, np.ndarray]]:
    totals: dict[tuple[str, str, str, str, int], dict[str, np.ndarray]] = {}
    for frame in partial_frames:
        for key, group in frame.groupby(
            ["strategy_id", "start_mode", "pool", "cohort_mode", "cohort_year"],
            sort=False,
        ):
            aggregate = totals.setdefault(tuple(key), _empty_totals(calendar_size))
            ordinals = group.ordinal.to_numpy(int)
            for name in aggregate:
                aggregate[name][ordinals] += group[name].to_numpy(aggregate[name].dtype)
    return totals


def _bootstrap_family(
    *,
    series: dict[tuple[str, str], np.ndarray],
    sessions: int,
) -> list[dict[str, Any]]:
    rng = np.random.default_rng(BOOTSTRAP_SEED)
    starts = rng.integers(0, sessions, size=(BOOTSTRAP_REPLICATES, int(np.ceil(sessions / BOOTSTRAP_BLOCK))))
    offsets = np.arange(BOOTSTRAP_BLOCK)
    indices = ((starts[..., None] + offsets) % sessions).reshape(BOOTSTRAP_REPLICATES, -1)[:, :sessions]
    adjusted_alpha = 0.05 / FAMILY_SIZE
    output: list[dict[str, Any]] = []
    for (strategy, pool), values in series.items():
        finite = np.isfinite(values)
        if not finite.any():
            output.append(
                {
                    "strategy_id": strategy,
                    "pool": pool,
                    "status": "UNAVAILABLE",
                    "reason": "NO_PAIRED_OBSERVED_DAILY_DIFFERENCE",
                }
            )
            continue
        sampled = values[indices]
        estimates = np.nanmean(sampled, axis=1) * 10_000.0
        point = float(np.nanmean(values) * 10_000.0)
        output.append(
            {
                "strategy_id": strategy,
                "pool": pool,
                "status": "ESTIMATED",
                "estimand": "PAIRED_OBSERVED_POOL_DAILY_RETURN_DIFFERENCE_BPS",
                "observed_sessions": int(finite.sum()),
                "point_bps": point,
                "nominal_95_interval_bps": [
                    float(np.nanquantile(estimates, 0.025)),
                    float(np.nanquantile(estimates, 0.975)),
                ],
                "familywise_interval_bps": [
                    float(np.nanquantile(estimates, adjusted_alpha / 2)),
                    float(np.nanquantile(estimates, 1 - adjusted_alpha / 2)),
                ],
                "family_size": FAMILY_SIZE,
            }
        )
    return output


def _baseline_equivalence(
    *,
    stocks: pd.DataFrame,
    baseline_manifest_path: Path,
    affected_symbols: Iterable[str] = (),
) -> dict[str, Any]:
    manifest = read_json(baseline_manifest_path)
    reference = manifest["files"]["stocks.parquet"]
    baseline_path = _portable_reference_path(reference)
    baseline = pd.read_parquet(baseline_path).sort_values("symbol", kind="stable")
    current = stocks.loc[stocks.strategy_id.eq("A0") & stocks.start_mode.eq("OWN_START")].sort_values(
        "symbol", kind="stable"
    )
    if list(current.symbol) != list(baseline.symbol):
        raise ActionValueError("STRATEGY_EVOLUTION_A0_POPULATION_DRIFT")
    columns = (
        "start",
        "end",
        "paired_unknown_sessions",
        "invested_session_fraction",
        "conditional_exposure",
        "average_exposure",
        "stale_mark_sessions",
        "timing_minus_hold",
        "timing_status",
        "timing_sessions",
        "timing_total_return",
        "timing_annualized_return",
        "timing_max_drawdown",
        "hold_status",
        "hold_sessions",
        "hold_total_return",
        "hold_annualized_return",
        "hold_max_drawdown",
    )
    allowed = set(affected_symbols)
    if not allowed.issubset(set(current.symbol)):
        raise ActionValueError("STRATEGY_EVOLUTION_A0_AFFECTED_SYMBOL_DRIFT")
    mismatches: list[dict[str, Any]] = []
    affected_differences: list[dict[str, Any]] = []

    def encoded(value: Any) -> Any:
        if pd.isna(value):
            return None
        return value.item() if isinstance(value, np.generic) else value

    for column in columns:
        left, right = current[column].reset_index(drop=True), baseline[column].reset_index(drop=True)
        if pd.api.types.is_numeric_dtype(right):
            equal = np.isclose(
                pd.to_numeric(left, errors="coerce"),
                pd.to_numeric(right, errors="coerce"),
                rtol=0,
                atol=1e-12,
                equal_nan=True,
            )
        else:
            equal = left.fillna("<NA>").astype(str).eq(right.fillna("<NA>").astype(str)).to_numpy()
        for ordinal in np.flatnonzero(~np.asarray(equal)):
            difference = {
                "symbol": str(current.iloc[ordinal].symbol),
                "field": column,
                "current": encoded(left.iloc[ordinal]),
                "baseline": encoded(right.iloc[ordinal]),
            }
            if difference["symbol"] in allowed:
                affected_differences.append(difference)
            elif len(mismatches) < 20:
                mismatches.append(difference)
    if mismatches:
        raise ActionValueError(
            "STRATEGY_EVOLUTION_A0_BASELINE_MISMATCH",
            mismatch_examples=mismatches,
        )
    audit = {
        "status": "EXACT_UNAFFECTED_SYMBOLS_WITH_VERSION_DELTA_DIAGNOSTIC",
        "symbol_count": len(current),
        "unaffected_symbol_count": len(current) - len(allowed),
        "affected_symbols": sorted(allowed),
        "affected_field_differences": affected_differences,
        "fields": list(columns),
        "numeric_absolute_tolerance": 1e-12,
        "baseline_manifest_sha256": BASELINE_CLOSE_CASH_MANIFEST_SHA256,
    }
    audit["audit_sha256"] = canonical_sha256(audit)
    return audit


def _build_report(
    *,
    totals: dict[tuple[str, str, str, str, int], dict[str, np.ndarray]],
    diagnostics: list[dict[str, Any]],
    stocks: pd.DataFrame,
    fills: pd.DataFrame,
    calendar: pd.DatetimeIndex,
    index_frame: pd.DataFrame,
    memberships: CandidatePoolMemberships,
) -> tuple[dict[str, Any], pd.DataFrame]:
    indexes = index_returns(index_frame, calendar)
    first: dict[tuple[str, str], int] = {}
    counters: dict[tuple[str, str], Counter[str]] = {}
    for item in diagnostics:
        key = (item["strategy_id"], item["start_mode"])
        counters.setdefault(key, Counter()).update(item.get("counts", {}))
        if item["status"] == "REPLAYED":
            value = int(item["start_ordinal"]) + 1
            first[key] = min(first.get(key, value), value)
    daily_rows: list[dict[str, Any]] = []
    pool_results: list[dict[str, Any]] = []
    bootstrap_series: dict[tuple[str, str], np.ndarray] = {}
    strategy_series: dict[tuple[str, str], np.ndarray] = {}
    for (strategy, start_mode, pool, cohort_mode, year), aggregate in sorted(totals.items()):
        threshold = first.get((strategy, start_mode), len(calendar))
        eligible = (aggregate["expected"] > 0) & (np.arange(len(calendar)) >= threshold)
        selected = np.flatnonzero(eligible)
        if not len(selected):
            continue
        paired = aggregate["paired"]
        timing = np.divide(
            aggregate["timing_sum"],
            paired,
            out=np.full(len(calendar), np.nan),
            where=paired > 0,
        )
        hold = np.divide(
            aggregate["hold_sum"],
            paired,
            out=np.full(len(calendar), np.nan),
            where=paired > 0,
        )
        complete = (aggregate["unknown"] == 0) & (paired > 0)
        index_values = indexes[INDEX_CODES[pool]]
        full = comparison(
            np.where(complete, timing, np.nan)[eligible],
            np.where(complete, hold, np.nan)[eligible],
            index_values[eligible],
        )
        observed = comparison(timing[eligible], hold[eligible], index_values[eligible])
        uniform = eligible & (calendar.date >= UNIFORM_PERIOD_START)
        annual = {
            str(year_value): comparison(
                timing[eligible & (calendar.year == year_value)],
                hold[eligible & (calendar.year == year_value)],
                index_values[eligible & (calendar.year == year_value)],
            )
            for year_value in sorted(set(calendar.year))
            if (eligible & (calendar.year == year_value)).any()
        }
        pool_results.append(
            {
                "strategy_id": strategy,
                "start_mode": start_mode,
                "pool": pool,
                "cohort_mode": cohort_mode,
                "cohort_year": year or None,
                "index_code": INDEX_CODES[pool],
                "first_date": str(calendar[selected[0]].date()),
                "last_date": str(calendar[selected[-1]].date()),
                "incomplete_sessions": int((eligible & ~complete).sum()),
                "missing_index_sessions": int((eligible & ~np.isfinite(index_values)).sum()),
                "population_unique_symbols": int(memberships.intervals[pool].symbol.nunique()),
                "full_population": full,
                "paired_observed_only_not_full_population": observed,
                "uniform_2023_08_08_to_2026_08_31_paired_observed": comparison(
                    timing[uniform], hold[uniform], index_values[uniform]
                ),
                "annual_paired_observed_diagnostic": annual,
            }
        )
        for ordinal in selected:
            daily_rows.append(
                {
                    "strategy_id": strategy,
                    "start_mode": start_mode,
                    "pool": pool,
                    "cohort_mode": cohort_mode,
                    "cohort_year": year,
                    "date": str(calendar[ordinal].date()),
                    "ordinal": ordinal,
                    "timing_return_observed": timing[ordinal],
                    "hold_return_observed": hold[ordinal],
                    "index_return": index_values[ordinal],
                    "expected_accounts": int(aggregate["expected"][ordinal]),
                    "paired_accounts": int(paired[ordinal]),
                    "unknown_accounts": int(aggregate["unknown"][ordinal]),
                }
            )
        if start_mode == "COMMON_START" and cohort_mode == "dynamic":
            values = np.full(len(calendar), np.nan)
            values[eligible] = timing[eligible]
            strategy_series[(strategy, pool)] = values
            if strategy != "A0":
                bootstrap_series[(strategy, pool)] = values - hold

    csi300 = indexes["000300.SH"]
    for row_index, row in stocks.iterrows():
        start = row.get("start_ordinal")
        if pd.isna(start):
            continue
        begin = int(start) + 1
        index_summary = performance(csi300[begin:])
        for key, value in index_summary.items():
            stocks.at[row_index, f"index_{key}"] = value
        stocks.at[row_index, "benchmark"] = "000300.SH"
        timing_total = row.get("timing_total_return")
        hold_total = row.get("hold_total_return")
        index_total = index_summary["total_return"]
        stocks.at[row_index, "timing_minus_index"] = (
            float(timing_total) - float(index_total) if pd.notna(timing_total) and index_total is not None else np.nan
        )
        stocks.at[row_index, "hold_minus_index"] = (
            float(hold_total) - float(index_total) if pd.notna(hold_total) and index_total is not None else np.nan
        )

    contrast_specs = {
        "A1_MINUS_A0": (("A1", 1.0), ("A0", -1.0)),
        "A2_MINUS_A0": (("A2", 1.0), ("A0", -1.0)),
        "A3_INTERACTION": (("A3", 1.0), ("A2", -1.0), ("A1", -1.0), ("A0", 1.0)),
        "C0_MINUS_A3": (("C0", 1.0), ("A3", -1.0)),
        "C1_MINUS_C0": (("C1", 1.0), ("C0", -1.0)),
        "C2_MINUS_C0": (("C2", 1.0), ("C0", -1.0)),
    }
    mechanism_contrasts: list[dict[str, Any]] = []
    for name, terms in contrast_specs.items():
        for pool in POOL_IDS:
            arrays = [strategy_series.get((strategy, pool)) for strategy, _ in terms]
            if any(value is None for value in arrays):
                mechanism_contrasts.append({"contrast": name, "pool": pool, "status": "UNAVAILABLE"})
                continue
            stacked = np.vstack(arrays)
            complete = np.isfinite(stacked).all(axis=0)
            estimate = sum(weight * np.asarray(value) for (_, weight), value in zip(terms, arrays, strict=True))
            mechanism_contrasts.append(
                {
                    "contrast": name,
                    "pool": pool,
                    "status": "ESTIMATED" if complete.any() else "UNAVAILABLE",
                    "paired_sessions": int(complete.sum()),
                    "mean_daily_difference_bps": (
                        float(np.mean(estimate[complete]) * 10_000.0) if complete.any() else None
                    ),
                    "diagnostic_only": True,
                }
            )
    distributions: list[dict[str, Any]] = []
    for (strategy, start_mode), group in stocks.groupby(["strategy_id", "start_mode"], sort=True):
        delta = pd.to_numeric(group.terminal_excess_return, errors="coerce").dropna()
        distributions.append(
            {
                "strategy_id": strategy,
                "start_mode": start_mode,
                "paired_terminal_symbols": int(len(delta)),
                "win_fraction": float((delta > 0).mean()) if len(delta) else None,
                "mean": float(delta.mean()) if len(delta) else None,
                "quantiles": (
                    {str(q): float(delta.quantile(q)) for q in (0.05, 0.25, 0.5, 0.75, 0.95)} if len(delta) else {}
                ),
            }
        )
    fill_summary: list[dict[str, Any]] = []
    reentry_summary: list[dict[str, Any]] = []
    post_exit_summary: list[dict[str, Any]] = []
    if not fills.empty:
        grouped = fills.groupby(
            ["strategy_id", "start_mode", "role", "side", "authority", "status"],
            dropna=False,
        ).size()
        for key, count in grouped.items():
            fill_summary.append(
                {
                    "strategy_id": key[0],
                    "start_mode": key[1],
                    "role": key[2],
                    "side": key[3],
                    "authority": key[4],
                    "status": key[5],
                    "count": int(count),
                }
            )
        for (strategy, start_mode), group in fills.groupby(["strategy_id", "start_mode"], sort=True):
            reentry = pd.to_numeric(
                group.get(
                    "sessions_since_last_exit",
                    pd.Series(np.nan, index=group.index),
                ),
                errors="coerce",
            ).dropna()
            reentry_summary.append(
                {
                    "strategy_id": strategy,
                    "start_mode": start_mode,
                    "observations": int(len(reentry)),
                    "median_sessions": float(reentry.median()) if len(reentry) else None,
                }
            )
            for horizon in (5, 20, 60):
                field = f"post_exit_return_{horizon}d"
                values = pd.to_numeric(
                    group.get(field, pd.Series(np.nan, index=group.index)),
                    errors="coerce",
                ).dropna()
                post_exit_summary.append(
                    {
                        "strategy_id": strategy,
                        "start_mode": start_mode,
                        "horizon_sessions": horizon,
                        "observations": int(len(values)),
                        "mean": float(values.mean()) if len(values) else None,
                        "median": float(values.median()) if len(values) else None,
                    }
                )
    report = {
        "schema_version": "position_timing_strategy_evolution_report_v1",
        "result_class": "EXPLORATORY_HYPOTHESIS_GENERATED",
        "strategy_set_sha256": STRATEGY_SET_SHA256,
        "selected_trial_count": 0,
        "pools": pool_results,
        "terminal_excess_distributions": distributions,
        "familywise_daily_difference": _bootstrap_family(
            series=bootstrap_series,
            sessions=len(calendar),
        ),
        "fill_summary": fill_summary,
        "reentry_interval_diagnostic": reentry_summary,
        "post_exit_return_diagnostic": post_exit_summary,
        "mechanism_contrasts": mechanism_contrasts,
        "path_counters": [
            {
                "strategy_id": key[0],
                "start_mode": key[1],
                "counts": dict(value),
            }
            for key, value in sorted(counters.items())
        ],
        "index_basis": "OFFICIAL_PRICE_INDEX_NOT_TOTAL_RETURN",
        "shared_cash_portfolio": False,
        "coverage_rule": "FULL_POPULATION_AND_PAIRED_OBSERVED_REPORTED_SEPARATELY",
    }
    return report, pd.DataFrame(daily_rows)


def _validate_candidate_population(candidate: DailyCandidate, request: Mapping[str, Any]) -> None:
    if (
        list(candidate.symbols) != request["symbols"]
        or [str(stamp.date()) for stamp in candidate.calendar] != request["calendar"]
    ):
        raise ActionValueError("STRATEGY_EVOLUTION_POPULATION_DRIFT")


def _validated_bar_inputs(
    *,
    candidate: DailyCandidate,
    request: Mapping[str, Any],
    symbols: Iterable[str],
) -> Iterator[tuple[str, pd.DataFrame]]:
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
                raise ActionValueError("STRATEGY_EVOLUTION_BAR_SOURCE_DRIFT", symbol=symbol)
        yield symbol, bars


def verify_parallel(path: Path) -> dict[str, Any]:
    """Compare fixed single-process and 8-process results without publishing artifacts."""

    request = load_request(path)
    candidate = DailyCandidate.open(Path(request["candidate_root"]))
    _validate_candidate_population(candidate, request)
    symbols = _parallel_verification_symbols(candidate.symbols)
    inputs = list(
        _validated_bar_inputs(
            candidate=candidate,
            request=request,
            symbols=symbols,
        )
    )
    sequential = {
        symbol: _symbol_result_sha256(result)
        for result in _ordered_replays(
            inputs,
            worker_count=1,
            max_in_flight=1,
        )
        for symbol in (result[0],)
    }
    parallel = {
        symbol: _symbol_result_sha256(result)
        for result in _ordered_replays(
            inputs,
            worker_count=WORKER_COUNT,
            max_in_flight=MAX_IN_FLIGHT,
        )
        for symbol in (result[0],)
    }
    if sequential != parallel or list(sequential) != symbols:
        raise ActionValueError("STRATEGY_EVOLUTION_PARALLEL_RESULT_DRIFT")
    audit = {
        "schema_version": "position_timing_parallel_verification_v1",
        "request_sha256": request["request_sha256"],
        "symbols": symbols,
        "sequential_worker_count": 1,
        "parallel_worker_count": WORKER_COUNT,
        "max_in_flight": MAX_IN_FLIGHT,
        "start_method": "spawn",
        "result_sha256_by_symbol": sequential,
        "artifact_written": False,
        "outcomes_read_stage": "AFTER_FULL_SOURCE_PREFLIGHT",
    }
    audit["audit_sha256"] = canonical_sha256(audit)
    return {"status": "EXACT", **audit}


def run(path: Path) -> dict[str, Any]:
    request = load_request(path)
    digest = request["request_sha256"]
    root = Path(request["timing_root"]) / "research" / FOLDER
    bundle = root / "bundles" / digest
    with _exclusive_file_lock(root / "locks" / f"{digest}.lock"):
        if (bundle / "manifest.json").exists():
            return {**inspect(bundle, request_hash=digest), "status": "ALREADY_MATERIALIZED"}
        candidate = DailyCandidate.open(Path(request["candidate_root"]))
        memberships = _r7_pool_memberships(candidate)
        _validate_candidate_population(candidate, request)
        print(
            json.dumps({"stage": "RUN_AFTER_FULL_SOURCE_PREFLIGHT", "outcomes_read": True}),
            flush=True,
        )
        chunks: list[dict[str, Any]] = []
        for offset in range(0, len(candidate.symbols), CHUNK_SIZE):
            chunk = root / "chunks" / digest / f"{offset // CHUNK_SIZE:04d}"
            if (chunk / "manifest.json").exists():
                inspect(chunk, request_hash=digest)
            else:
                totals: dict[tuple[str, str, str, str, int], dict[str, np.ndarray]] = {}
                stock_rows: list[dict[str, Any]] = []
                fill_frames: list[pd.DataFrame] = []
                details: list[dict[str, Any]] = []
                selected_symbols = candidate.symbols[offset : offset + CHUNK_SIZE]
                pit_masks: dict[str, np.ndarray] = {}

                def chunk_inputs() -> Iterator[tuple[str, pd.DataFrame]]:
                    for selected_symbol, selected_bars in _validated_bar_inputs(
                        candidate=candidate,
                        request=request,
                        symbols=selected_symbols,
                    ):
                        pit_masks[selected_symbol] = selected_bars.pit_active.to_numpy(bool)
                        yield selected_symbol, selected_bars

                for symbol, days, fills, symbol_details in _ordered_replays(
                    chunk_inputs(),
                    worker_count=WORKER_COUNT,
                    max_in_flight=MAX_IN_FLIGHT,
                ):
                    if not fills.empty:
                        fill_frames.append(fills)
                    details.extend(symbol_details)
                    for detail in symbol_details:
                        selected = None
                        if detail["status"] == "REPLAYED":
                            selected = days.loc[
                                (days.strategy_id == detail["strategy_id"]) & (days.start_mode == detail["start_mode"])
                            ]
                        stock_rows.append(
                            _add_symbol_path(
                                symbol=symbol,
                                strategy_id=detail["strategy_id"],
                                start_mode=detail["start_mode"],
                                path=selected,
                                detail=detail,
                                stock_pit_mask=pit_masks[symbol],
                                memberships=memberships,
                                calendar=candidate.calendar,
                                totals=totals,
                            )
                        )
                    pit_masks.pop(symbol)
                if pit_masks:
                    raise ActionValueError("STRATEGY_EVOLUTION_PARENT_INPUT_CACHE_DRIFT")
                publish_frame(chunk / "partials.parquet", _partial_frame(totals))
                publish_frame(chunk / "stocks.parquet", pd.DataFrame(stock_rows))
                publish_frame(
                    chunk / "fills.parquet",
                    _concat_frames_by_records(fill_frames),
                )
                publish_json(chunk / "diagnostics.json", details)
                _seal(
                    chunk,
                    ["partials.parquet", "stocks.parquet", "fills.parquet", "diagnostics.json"],
                    request_hash=digest,
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

        partial_frames: list[pd.DataFrame] = []
        stock_frames: list[pd.DataFrame] = []
        fill_frames = []
        diagnostics: list[dict[str, Any]] = []
        for reference in chunks:
            check_ref(reference)
            chunk = Path(reference["path"]).parent
            inspect(chunk, request_hash=digest)
            partial_frames.append(pd.read_parquet(chunk / "partials.parquet"))
            stock_frames.append(pd.read_parquet(chunk / "stocks.parquet"))
            frame = pd.read_parquet(chunk / "fills.parquet")
            if not frame.empty:
                fill_frames.append(frame)
            diagnostics.extend(read_json(chunk / "diagnostics.json"))
        totals = _merge_partials(partial_frames, len(candidate.calendar))
        stocks = pd.concat(stock_frames, ignore_index=True)
        fills = _concat_frames_by_records(fill_frames)
        check_ref(request["index_source"])
        index_frame = pd.read_hdf(request["index_source"]["path"], key="data")
        report, daily = _build_report(
            totals=totals,
            diagnostics=diagnostics,
            stocks=stocks,
            fills=fills,
            calendar=candidate.calendar,
            index_frame=index_frame,
            memberships=memberships,
        )
        report["a0_own_start_baseline_equivalence"] = _baseline_equivalence(
            stocks=stocks,
            baseline_manifest_path=Path(request["baseline_close_cash_manifest"]["path"]),
            affected_symbols=request["dataset_version_delta_audit"]["affected_symbols"],
        )
        load_request(path)
        publish_json(bundle / "request.json", request)
        publish_json(bundle / "report.json", report)
        publish_frame(bundle / "stocks.parquet", stocks)
        publish_frame(bundle / "pool_daily.parquet", daily)
        publish_frame(bundle / "fills.parquet", fills)
        receipt = {
            "schema_version": RECEIPT_SCHEMA,
            "request_sha256": digest,
            "chunks": chunks,
            "symbol_count": len(candidate.symbols),
            "strategy_count": len(STRATEGIES),
            "start_mode_count": len(START_MODES),
            "outcomes_read_stage": "RUN_AFTER_FULL_SOURCE_PREFLIGHT",
            "source_preflight_complete": True,
            "result_class": "EXPLORATORY_HYPOTHESIS_GENERATED",
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
            "selected_trial_count": 0,
        }
        publish_json(bundle / "receipt.json", receipt)
        _seal(
            bundle,
            [
                "request.json",
                "report.json",
                "stocks.parquet",
                "pool_daily.parquet",
                "fills.parquet",
                "receipt.json",
            ],
            request_hash=digest,
        )
        return inspect(bundle, request_hash=digest)


def main() -> None:
    parser = argparse.ArgumentParser(__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare_parser = commands.add_parser("prepare")
    for name in (
        "timing-root",
        "repository-root",
        "parent-pattern-bundle",
        "baseline-close-cash-bundle",
        "prior-candidate-root",
        "candidate-root",
    ):
        prepare_parser.add_argument(f"--{name}", type=Path, required=True)
    commands.add_parser("verify-parallel").add_argument("--request", type=Path, required=True)
    commands.add_parser("run").add_argument("--request", type=Path, required=True)
    commands.add_parser("inspect").add_argument("--bundle", type=Path, required=True)
    args = vars(parser.parse_args())
    command = args.pop("command")
    if command == "prepare":
        result = {"request": file_reference(prepare(**args))}
    elif command == "verify-parallel":
        result = verify_parallel(args["request"])
    elif command == "run":
        result = run(args["request"])
    else:
        result = inspect(args["bundle"])
    print(json.dumps(result, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
