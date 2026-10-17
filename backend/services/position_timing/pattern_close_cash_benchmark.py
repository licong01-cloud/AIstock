"""Immutable, offline prepare/run/inspect for the independent close-cash experiment."""
from __future__ import annotations

import argparse
from datetime import date
import io
import json
from pathlib import Path
import platform
from typing import Any

import numpy as np
import pandas as pd

from .action_value import ActionValueError
from .action_value_data import DailyCandidate, file_reference
from .action_value_pipeline import _clean_repository_commit
from .artifact_store import PositionTimingArtifactStore, _exclusive_file_lock
from .contracts import canonical_json_bytes, canonical_sha256
from .pattern_adj_factor_restatement import audit_candidate_adj_factor_restatement, open_adj_factor_restatement_authority
from .pattern_close_cash_replay import replay
from .pattern_close_cash_report import INDEX_CODES, build_report
from .pattern_strategy import TEMPLATE_BY_ID
from .pattern_universe_benchmark import (
    EXPECTED_ADJ_FACTOR_RESTATEMENT_AUTHORITY_SHA256, EXPECTED_CANDIDATE_MANIFEST_SHA256,
    EXPECTED_PARENT_MANIFEST_SHA256, QLIB_ADJUSTED_FACTOR_CONTRACT_SHA256,
    _audit_qlib_adjusted_factor_integrity, open_candidate_pool_memberships,
)
from .policy import EXIT_GUARD_RULE_DEFAULT_SNAPSHOT_V1, PERSONAL_MANUAL_COMPONENT_COST_V1, PRICE_GUARD_RULE_DEFAULT_SNAPSHOT_V1

FOLDER = "pattern_close_cash_benchmark_v1"
INDEX_FILE_SHA = "875c9560e10c9a6d13f30f5fbfb44bc9aba16a3ef39a41fe1acd4a91e4f4c5dd"
CONTRACT = {
    "capital_per_stock_per_account_cny": "10000000", "strategy": TEMPLATE_BY_ID["R0"].identity,
    "price_guard": PRICE_GUARD_RULE_DEFAULT_SNAPSHOT_V1, "exit_guard": EXIT_GUARD_RULE_DEFAULT_SNAPSHOT_V1,
    "cost_policy": PERSONAL_MANUAL_COMPONENT_COST_V1, "decision": "T_CLOSE", "fill": "T_PLUS_1_CLOSE",
    "quantity": "LEGAL_RAW_BUY_QUANTITY_DIVIDED_BY_FACTOR_VIRTUAL_UNITS_FULL_VIRTUAL_EXIT",
    "sizing": "AVAILABLE_CASH_INCLUDING_FEES_NATURAL_REINVESTMENT", "cash_interest": 0,
    "leverage": False, "cash_transfers": False, "slippage_bps": 0, "market_impact": False,
    "benchmark_by_pool": INDEX_CODES, "pool_aggregation": "LAGGED_PIT_EQUAL_ACCOUNT_DAILY_PERCENT_RETURN",
    "terminal": "COMMON_FINAL_CLOSE_MTM_SEPARATE_SELLABILITY", "warmup": "PATTERN_FEATURES_READY_NOT_MODEL_TRAINING",
    "result_class": "EXPLORATORY", "corporate_action_authority_read": False,
    "account_economics_simulated": False, "broker_account_clearing": False,
}


def publish_json(path: Path, value: Any) -> None:
    PositionTimingArtifactStore._publish_immutable(path, canonical_json_bytes(value))


def publish_frame(path: Path, frame: pd.DataFrame) -> None:
    stream = io.BytesIO()
    frame.to_parquet(stream, index=False)
    PositionTimingArtifactStore._publish_immutable(path, stream.getvalue())


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def check_ref(ref: dict[str, Any]) -> None:
    if file_reference(Path(ref["path"])) != ref:
        raise ActionValueError("CLOSE_CASH_IMMUTABLE_FILE_DRIFT", path=ref["path"])


def sources(repository: Path) -> dict[str, Any]:
    own = repository / "backend/services/position_timing"
    # Bind all timing implementation files, not mutable defaults or an abbreviated
    # entrypoint-only identity. Read-only shared pure dependencies are bound too.
    paths = list(own.glob("*.py"))
    paths += list((repository / "backend/services/trading_core").glob("*.py"))
    paths += [repository / "backend/execution_algos/board_lot.py"]
    return {p.relative_to(repository).as_posix(): file_reference(p) for p in sorted(paths)}


def prepare(*, timing_root: Path, repository_root: Path, parent_pattern_bundle: Path, candidate_root: Path) -> Path:
    repository = repository_root.resolve()
    root = timing_root.resolve()
    if not all(p.is_absolute() for p in (timing_root, repository_root, parent_pattern_bundle, candidate_root)) or root.is_relative_to(repository) or root.is_relative_to(candidate_root.resolve()) or repository != Path(__file__).resolve().parents[3]:
        raise ActionValueError("CLOSE_CASH_PATH_SCOPE_INVALID")
    commit = _clean_repository_commit(repository)
    code = sources(repository)
    candidate = DailyCandidate.open(candidate_root)
    pools = open_candidate_pool_memberships(candidate)
    if (candidate.calendar[0].date(), candidate.calendar[-1].date(), len(candidate.calendar)) != (date(2018, 8, 1), date(2026, 8, 31), 1961):
        raise ActionValueError("CLOSE_CASH_CALENDAR_DRIFT")
    parent_ref = file_reference(parent_pattern_bundle / "manifest.json")
    parent = read_json(Path(parent_ref["path"]))
    if parent.get("manifest_sha256") != EXPECTED_PARENT_MANIFEST_SHA256 or canonical_sha256({k: v for k, v in parent.items() if k != "manifest_sha256"}) != EXPECTED_PARENT_MANIFEST_SHA256:
        raise ActionValueError("CLOSE_CASH_PARENT_IDENTITY_DRIFT")
    # No parent outcomes/receipts read: only frozen manifest identity.
    authority = open_adj_factor_restatement_authority(candidate_root=candidate.root,
        expected_candidate_manifest_sha256=EXPECTED_CANDIDATE_MANIFEST_SHA256,
        expected_authority_canonical_sha256=EXPECTED_ADJ_FACTOR_RESTATEMENT_AUTHORITY_SHA256)
    restatement = audit_candidate_adj_factor_restatement(candidate, authority)
    identity = canonical_sha256({"candidate_manifest": pools.candidate_manifest_reference,
                               "candidate_dataset_manifest_sha256": pools.candidate_dataset_manifest_sha256,
                               "pool_sidecars": pools.references})
    print(json.dumps({"stage": "SOURCE_FACTOR_PREFLIGHT", "symbols": len(candidate.symbols), "outcomes_read": False}), flush=True)
    factors = _audit_qlib_adjusted_factor_integrity(candidate, symbols=candidate.symbols,
        start=candidate.calendar[0].date(), end=candidate.calendar[-1].date(), candidate_source_sha256=identity)
    if not factors["coverage_complete"] or not restatement["coverage_complete"]:
        raise ActionValueError("CLOSE_CASH_SOURCE_PREFLIGHT_FAILED")
    index_ref = file_reference(candidate.root / "components/index_context/index_daily.h5")
    if index_ref["sha256"] != INDEX_FILE_SHA:
        raise ActionValueError("CLOSE_CASH_INDEX_SOURCE_DRIFT")
    check_ref(parent_ref)
    if sources(repository) != code:
        raise ActionValueError("CLOSE_CASH_SOURCE_CODE_DRIFT")
    request = {"schema_version": "position_timing_close_cash_request_v1", "contract": CONTRACT,
               "contract_sha256": canonical_sha256(CONTRACT), "repository_commit": commit,
               "repository_root": repository.as_posix(), "source_code": code,
               "environment": {"python": platform.python_version(), "numpy": np.__version__, "pandas": pd.__version__},
               "timing_root": root.as_posix(), "candidate_root": candidate.root.as_posix(),
               "candidate_manifest": pools.candidate_manifest_reference, "parent_manifest": parent_ref,
               "symbols": candidate.symbols, "pool_sidecars": pools.references,
               "calendar": [str(d.date()) for d in candidate.calendar],
               "index_source": index_ref, "source_data": candidate.references,
               "restatement_authority": authority.authority_reference,
               "restatement_audit": restatement, "factor_audit": factors,
               "qlib_contract_sha256": QLIB_ADJUSTED_FACTOR_CONTRACT_SHA256,
               "source_preflight_complete": True, "outcomes_read": False,
               "database_read": False, "database_write": False, "network_accessed": False,
               "runtime_action_performed": False, "process_control_performed": False}
    request["request_sha256"] = canonical_sha256(request)
    path = root / "research" / FOLDER / "requests" / f"{request['request_sha256']}.json"
    publish_json(path, request)
    return path


def load_request(path: Path) -> dict[str, Any]:
    request = read_json(path)
    identity = canonical_sha256({k: v for k, v in request.items() if k != "request_sha256"})
    if request.get("request_sha256") != identity or request.get("contract_sha256") != canonical_sha256(CONTRACT) or canonical_sha256(request.get("contract")) != canonical_sha256(CONTRACT) or request.get("schema_version") != "position_timing_close_cash_request_v1":
        raise ActionValueError("CLOSE_CASH_REQUEST_DRIFT")
    owner = Path(request["timing_root"]).resolve()
    repository = Path(request["repository_root"]).resolve()
    expected_path = owner / "research" / FOLDER / "requests" / f"{identity}.json"
    if path.resolve() != expected_path or repository != Path(__file__).resolve().parents[3] or owner.is_relative_to(repository) or owner.is_relative_to(Path(request["candidate_root"]).resolve()):
        raise ActionValueError("CLOSE_CASH_REQUEST_SCOPE_DRIFT")
    if request.get("source_preflight_complete") is not True or any(request.get(k) is not False for k in ("outcomes_read", "database_read", "database_write", "network_accessed", "runtime_action_performed", "process_control_performed")):
        raise ActionValueError("CLOSE_CASH_REQUEST_BOUNDARY_DRIFT")
    if request.get("environment") != {"python": platform.python_version(), "numpy": np.__version__, "pandas": pd.__version__}:
        raise ActionValueError("CLOSE_CASH_ENVIRONMENT_DRIFT")
    for name in ("factor_audit", "restatement_audit"):
        audit = request[name]
        if audit.get("coverage_complete") is not True or audit["audit_sha256"] != canonical_sha256({k: v for k, v in audit.items() if k != "audit_sha256"}):
            raise ActionValueError("CLOSE_CASH_PREFLIGHT_DRIFT")
    if request["candidate_manifest"]["sha256"] != EXPECTED_CANDIDATE_MANIFEST_SHA256 or request["index_source"]["sha256"] != INDEX_FILE_SHA or request["qlib_contract_sha256"] != QLIB_ADJUSTED_FACTOR_CONTRACT_SHA256:
        raise ActionValueError("CLOSE_CASH_AUTHORITY_DRIFT")
    for ref in [request[k] for k in ("candidate_manifest", "parent_manifest", "index_source", "restatement_authority")]:
        check_ref(ref)
    for group in ("source_code", "source_data", "pool_sidecars"):
        for ref in request[group].values():
            check_ref(ref)
    if sources(Path(request["repository_root"])) != request["source_code"]:
        raise ActionValueError("CLOSE_CASH_CODE_DRIFT")
    return request


def seal(root: Path, names: list[str], *, request_hash: str) -> dict[str, Any]:
    files = {name: file_reference(root / name) for name in names}
    value = {"schema_version": "position_timing_close_cash_files_v1", "request_sha256": request_hash, "files": files}
    value["manifest_sha256"] = canonical_sha256(value)
    publish_json(root / "manifest.json", value)
    return value


def inspect(root: Path, *, request_hash: str | None = None) -> dict[str, Any]:
    manifest = read_json(root / "manifest.json")
    if manifest.get("schema_version") != "position_timing_close_cash_files_v1" or manifest.get("manifest_sha256") != canonical_sha256({k: v for k, v in manifest.items() if k != "manifest_sha256"}) or (request_hash is not None and manifest["request_sha256"] != request_hash):
        raise ActionValueError("CLOSE_CASH_MANIFEST_DRIFT")
    for name, ref in manifest["files"].items():
        if Path(ref["path"]).resolve() != (root / name).resolve() or not (root / name).resolve().is_relative_to(root.resolve()):
            raise ActionValueError("CLOSE_CASH_FILE_SCOPE_DRIFT")
        check_ref(ref)
    if "receipt.json" in manifest["files"]:
        if set(manifest["files"]) != {"request.json", "report.json", "stocks.parquet", "pool_daily.parquet", "receipt.json"}:
            raise ActionValueError("CLOSE_CASH_BUNDLE_INCOMPLETE")
        receipt = read_json(root / "receipt.json")
        request = read_json(root / "request.json")
        if request.get("request_sha256") != manifest["request_sha256"] or canonical_sha256({k: v for k, v in request.items() if k != "request_sha256"}) != manifest["request_sha256"] or receipt.get("request_sha256") != manifest["request_sha256"] or len(receipt["chunks"]) != (len(request["symbols"]) + 31) // 32:
            raise ActionValueError("CLOSE_CASH_BUNDLE_IDENTITY_DRIFT")
        for chunk in receipt["chunks"]:
            check_ref(chunk)
            inspect(Path(chunk["path"]).parent, request_hash=manifest["request_sha256"])
    return {"status": "VERIFIED", "bundle": root.as_posix(), "manifest_sha256": manifest["manifest_sha256"],
            "request_sha256": manifest["request_sha256"]}


def run(path: Path) -> dict[str, Any]:
    request = load_request(path)
    digest = request["request_sha256"]
    root = Path(request["timing_root"]) / "research" / FOLDER
    bundle = root / "bundles" / digest
    with _exclusive_file_lock(root / "locks" / f"{digest}.lock"):
        if (bundle / "manifest.json").exists():
            return {**inspect(bundle, request_hash=digest), "status": "ALREADY_MATERIALIZED"}
        candidate = DailyCandidate.open(Path(request["candidate_root"]))
        pools = open_candidate_pool_memberships(candidate)
        if list(candidate.symbols) != request["symbols"] or [str(d.date()) for d in candidate.calendar] != request["calendar"]:
            raise ActionValueError("CLOSE_CASH_POPULATION_DRIFT")
        # Full file identity preflight above precedes all research outcome reads.
        print(json.dumps({"stage": "RUN_AFTER_FULL_SOURCE_PREFLIGHT", "outcomes_read": True}), flush=True)
        chunks = []
        for offset in range(0, len(candidate.symbols), 32):
            chunk = root / "chunks" / digest / f"{offset // 32:04d}"
            if (chunk / "manifest.json").exists():
                inspect(chunk, request_hash=digest)
            else:
                days, fills, details = [], [], []
                for symbol in candidate.symbols[offset:offset+32]:
                    bars = candidate.bars(symbol)
                    for field in ("open", "high", "low", "close", "volume", "factor", "up_limit_price", "down_limit_price"):
                        key = f"{symbol}:{field}"
                        if candidate.references[key] != request["source_data"][key]:
                            raise ActionValueError("CLOSE_CASH_BAR_SOURCE_DRIFT", symbol=symbol)
                    frame, events, detail = replay(symbol, bars)
                    if not frame.empty:
                        days.append(frame.assign(symbol=symbol))
                    fills.extend(events)
                    details.append(detail)
                publish_frame(chunk / "days.parquet", pd.concat(days, ignore_index=True) if days else pd.DataFrame())
                publish_frame(chunk / "fills.parquet", pd.DataFrame(fills))
                publish_json(chunk / "diagnostics.json", details)
                seal(chunk, ["days.parquet", "fills.parquet", "diagnostics.json"], request_hash=digest)
            chunks.append(file_reference(chunk / "manifest.json"))
            print(json.dumps({"stage": "CHUNK_COMPLETE", "symbols_complete": min(offset+32, len(candidate.symbols)), "total": len(candidate.symbols)}), flush=True)
        def frames():
            for ref in chunks:
                check_ref(ref)
                chunk = Path(ref["path"]).parent
                inspect(chunk, request_hash=digest)
                yield pd.read_parquet(chunk / "days.parquet"), read_json(chunk / "diagnostics.json")
        check_ref(request["index_source"])
        index_frame = pd.read_hdf(request["index_source"]["path"], key="data")
        report, stocks, daily = build_report(frames(), memberships=pools, calendar=candidate.calendar, index_frame=index_frame)
        load_request(path)  # Postflight: input and code identities still frozen.
        publish_json(bundle / "request.json", request)
        publish_json(bundle / "report.json", report)
        publish_frame(bundle / "stocks.parquet", stocks)
        publish_frame(bundle / "pool_daily.parquet", daily)
        receipt = {"schema_version": "position_timing_close_cash_receipt_v1", "request_sha256": digest,
                   "chunks": chunks, "symbol_count": len(candidate.symbols),
                   "outcomes_read_stage": "RUN_AFTER_FULL_SOURCE_PREFLIGHT", "source_preflight_complete": True,
                   "result_class": "EXPLORATORY", "database_read": False, "database_write": False,
                   "network_accessed": False, "runtime_action_performed": False, "process_control_performed": False,
                   "corporate_action_authority_read": False, "account_economics_simulated": False,
                   "broker_account_clearing": False}
        publish_json(bundle / "receipt.json", receipt)
        seal(bundle, ["request.json", "report.json", "stocks.parquet", "pool_daily.parquet", "receipt.json"], request_hash=digest)
        return inspect(bundle, request_hash=digest)


def main() -> None:
    parser = argparse.ArgumentParser(__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    p = commands.add_parser("prepare")
    for name in ("timing-root", "repository-root", "parent-pattern-bundle", "candidate-root"):
        p.add_argument(f"--{name}", type=Path, required=True)
    commands.add_parser("run").add_argument("--request", type=Path, required=True)
    commands.add_parser("inspect").add_argument("--bundle", type=Path, required=True)
    args = vars(parser.parse_args())
    command = args.pop("command")
    if command == "prepare":
        path = prepare(**args)
        result = {"request": file_reference(path)}
    elif command == "run":
        result = run(args["request"])
    else:
        result = inspect(args["bundle"])
    print(json.dumps(result, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
