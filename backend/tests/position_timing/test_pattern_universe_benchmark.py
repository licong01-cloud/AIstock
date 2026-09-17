from __future__ import annotations

from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.services.position_timing.action_value import ActionValueError
from backend.services.position_timing.action_value_data import DailyCandidate, file_reference
from backend.services.position_timing.contracts import canonical_json_bytes, canonical_sha256
from backend.services.position_timing import pattern_universe_benchmark as benchmark


def _membership_candidate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    stock_rows: tuple[str, ...] = ("000001.SZ\t2024-01-02\t2024-01-05",),
    index_rows: tuple[str, ...] = ("000001.SZ\t2024-01-02\t2024-01-05",),
) -> DailyCandidate:
    root = tmp_path / "candidate"
    pools = root / "stock_pools"
    pools.mkdir(parents=True)
    pool_files: dict[str, dict[str, object]] = {}
    for pool_id in benchmark.POOL_IDS:
        name = "stock_universe.txt" if pool_id == "stock_universe" else f"index_pool__{pool_id}.txt"
        path = pools / name
        rows = stock_rows if pool_id == "stock_universe" else index_rows
        path.write_text("\n".join(rows) + "\n", encoding="utf-8")
        reference = file_reference(path)
        pool_files[pool_id] = {
            "path": f"stock_pools/{name}",
            "sha256": reference["sha256"],
            "size_bytes": reference["size_bytes"],
        }
    manifest: dict[str, object] = {
        "schema_version": "fixture",
        "availability_status": "CANDIDATE_READY",
        "st_pit_manifest": {
            "index_membership_sidecars": {
                pool_id: {
                    "path": values["path"],
                    "sha256": values["sha256"],
                    "size": values["size_bytes"],
                }
                for pool_id, values in pool_files.items()
            }
        },
    }
    manifest["dataset_manifest_sha256"] = canonical_sha256(manifest)
    manifest_path = root / "qe_dataset_manifest.json"
    manifest_path.write_bytes(canonical_json_bytes(manifest))
    monkeypatch.setattr(
        benchmark, "EXPECTED_CANDIDATE_MANIFEST_SHA256", file_reference(manifest_path)["sha256"]
    )
    monkeypatch.setattr(
        benchmark, "EXPECTED_CANDIDATE_DATASET_SHA256", manifest["dataset_manifest_sha256"]
    )
    monkeypatch.setattr(benchmark, "EXPECTED_POOL_FILES", pool_files)
    calendar = pd.date_range("2024-01-02", periods=4, freq="D")
    spans = pd.read_csv(
        pools / "stock_universe.txt",
        sep="\t",
        names=["symbol", "start", "end"],
        dtype=str,
    )
    spans[["start", "end"]] = spans[["start", "end"]].apply(pd.to_datetime)
    return DailyCandidate(root, calendar, spans, set(), {})


def test_frozen_authorities_match_r5_candidate(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    candidate = _membership_candidate(tmp_path, monkeypatch)
    memberships = benchmark.open_candidate_pool_memberships(candidate)
    assert memberships.symbols == ("000001.SZ",)
    assert tuple(memberships.references) == benchmark.POOL_IDS
    assert memberships.candidate_dataset_manifest_sha256 == benchmark.EXPECTED_CANDIDATE_DATASET_SHA256

    csi300 = Path(memberships.references["csi300"]["path"])
    csi300.write_text("000001.SZ\t2024-01-03\t2024-01-05\n", encoding="utf-8")
    with pytest.raises(ActionValueError, match="PATTERN_POOL_FILE_IDENTITY_MISMATCH"):
        benchmark.open_candidate_pool_memberships(candidate)


def test_pit_membership_projection_preserves_population_clock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    candidate = _membership_candidate(
        tmp_path,
        monkeypatch,
        stock_rows=(
            "000001.SZ\t2024-01-02\t2024-01-02",
            "000001.SZ\t2024-01-04\t2024-01-05",
        ),
    )
    memberships = benchmark.open_candidate_pool_memberships(candidate)
    mask = memberships.effective_mask(
        pool_id="csi300",
        symbol="000001.SZ",
        calendar=candidate.calendar,
        stock_pit_mask=np.array([True, False, True, True]),
    )
    assert mask.tolist() == [True, False, True, True]
    with pytest.raises(ActionValueError, match="PATTERN_POOL_MASK_SPEC_INVALID"):
        memberships.effective_mask(
            pool_id="current_members",
            symbol="000001.SZ",
            calendar=candidate.calendar,
            stock_pit_mask=mask,
        )


def test_same_stock_buy_and_hold_and_market_context_are_distinct():
    rows = [
        {
            "symbol": "000001.SZ",
            "valuation_date": date(2024, 1, 2),
            "comparison": "P_MINUS_BUY_AND_HOLD",
            "incremental_net_value_bps": -10.0,
            "policy_wealth_cny": 100000.0,
            "baseline_wealth_cny": 100100.0,
            "policy_quantity": 0,
            "baseline_quantity": 100,
            "policy_exposure": 0.0,
            "policy_authority": "WAIT",
        },
        {
            "symbol": "000001.SZ",
            "valuation_date": date(2024, 1, 3),
            "comparison": "P_MINUS_BUY_AND_HOLD",
            "incremental_net_value_bps": 5.0,
            "policy_wealth_cny": 100100.0,
            "baseline_wealth_cny": 100150.0,
            "policy_quantity": 100,
            "baseline_quantity": 100,
            "policy_exposure": 0.5,
            "policy_authority": "OPEN",
        },
    ]
    partial, summary = benchmark._path_daily_partials(
        rows,
        pool_id="csi300",
        strategy_id=benchmark.PRIMARY_STRATEGY,
        terminal_status="TERMINAL_LIQUIDATED",
    )
    assert partial["timing_increment_bps_sum"].tolist() == [-10.0, 5.0]
    assert summary["timing_increment_total_bps"] == pytest.approx(-5.0)
    assert "market" not in summary


def test_parent_artifact_identity_is_hash_bound_and_read_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    root = tmp_path / "parent"
    root.mkdir()
    (root / "manifest.json").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(benchmark, "EXPECTED_PARENT_MANIFEST_SHA256", "a" * 64)
    monkeypatch.setattr(
        benchmark,
        "inspect_pattern_bundle",
        lambda _root: {
            "manifest": {"manifest_sha256": "a" * 64},
            "request": {"request_sha256": "r" * 64},
        },
    )
    loaded = benchmark.load_frozen_parent_evidence(root)
    assert loaded.request_sha256 == "r" * 64
    monkeypatch.setattr(
        benchmark,
        "inspect_pattern_bundle",
        lambda _root: {
            "manifest": {"manifest_sha256": "b" * 64},
            "request": {"request_sha256": "r" * 64},
        },
    )
    with pytest.raises(
        ActionValueError,
        match="PATTERN_BENCHMARK_PARENT_IDENTITY_MISMATCH",
    ):
        benchmark.load_frozen_parent_evidence(root)


def _adjusted_request_fixture(tmp_path: Path) -> dict[str, object]:
    pool_sidecars = {pool_id: {} for pool_id in benchmark.POOL_IDS}
    candidate_manifest = {
        "path": (tmp_path / "candidate-manifest.json").resolve().as_posix(),
        "sha256": benchmark.EXPECTED_CANDIDATE_MANIFEST_SHA256,
        "size_bytes": 1,
    }
    candidate_identity_sha256 = canonical_sha256(
        {
            "candidate_manifest": candidate_manifest,
            "candidate_dataset_manifest_sha256": (
                benchmark.EXPECTED_CANDIDATE_DATASET_SHA256
            ),
            "pool_sidecars": pool_sidecars,
        }
    )
    factor_audit: dict[str, object] = {
        "schema_version": "position_timing_qlib_adjusted_factor_integrity_audit_v1",
        "contract_sha256": benchmark.QLIB_ADJUSTED_FACTOR_CONTRACT_SHA256,
        "candidate_source_sha256": candidate_identity_sha256,
        "scope": {
            "symbols_sha256": canonical_sha256(("000001.SZ",)),
            "symbol_count": 1,
            "start": "2018-08-01",
            "end": "2026-08-31",
        },
        "observed_factor_row_count": 2,
        "material_factor_change_count": 1,
        "factor_series_sha256": "f" * 64,
        "invalid_factor_symbol_count": 0,
        "invalid_factor_symbols": [],
        "insufficient_factor_symbol_count": 0,
        "insufficient_factor_symbols": [],
        "coverage_complete": True,
        "corporate_action_authority_read": False,
        "account_economics_simulated": False,
        "broker_account_clearing": False,
        "outcomes_read": False,
        "database_read": False,
        "database_write": False,
        "network_accessed": False,
        "runtime_action_performed": False,
    }
    factor_audit["audit_sha256"] = canonical_sha256(factor_audit)
    restatement_audit: dict[str, object] = {
        "schema_version": "fixture",
        "coverage_complete": True,
    }
    restatement_audit["audit_sha256"] = canonical_sha256(restatement_audit)
    preflight: dict[str, object] = {
        "schema_version": "position_timing_pattern_adjusted_source_preflight_audit_v1",
        "candidate_manifest_sha256": benchmark.EXPECTED_CANDIDATE_MANIFEST_SHA256,
        "qlib_adjusted_factor_contract_sha256": (
            benchmark.QLIB_ADJUSTED_FACTOR_CONTRACT_SHA256
        ),
        "qlib_adjusted_factor_integrity_audit_sha256": factor_audit[
            "audit_sha256"
        ],
        "adj_factor_restatement_audit_sha256": restatement_audit["audit_sha256"],
        "invalid_factor_symbol_count": 0,
        "insufficient_factor_symbol_count": 0,
        "corporate_action_authority_read": False,
        "account_economics_simulated": False,
        "broker_account_clearing": False,
        "outcomes_read": False,
    }
    preflight["audit_sha256"] = canonical_sha256(preflight)
    request: dict[str, object] = {
        "schema_version": benchmark.REQUEST_SCHEMA,
        "pipeline_id": benchmark.PIPELINE_ID,
        "repository_root": (tmp_path / "repo").resolve().as_posix(),
        "repository_commit": "1" * 40,
        "timing_root": (tmp_path / "timing").resolve().as_posix(),
        "candidate_root": (tmp_path / "candidate").resolve().as_posix(),
        "candidate_manifest": candidate_manifest,
        "candidate_manifest_sha256": benchmark.EXPECTED_CANDIDATE_MANIFEST_SHA256,
        "candidate_dataset_manifest_sha256": (
            benchmark.EXPECTED_CANDIDATE_DATASET_SHA256
        ),
        "candidate_identity_sha256": candidate_identity_sha256,
        "candidate_data_reference_count": 0,
        "candidate_data_references_sha256": canonical_sha256({}),
        "pool_sidecars": pool_sidecars,
        "pool_ids": benchmark.POOL_IDS,
        "population_symbols": ("000001.SZ",),
        "population_symbols_sha256": canonical_sha256(("000001.SZ",)),
        "parent_manifest_sha256": benchmark.EXPECTED_PARENT_MANIFEST_SHA256,
        "qlib_adjusted_factor_contract": benchmark.QLIB_ADJUSTED_FACTOR_CONTRACT,
        "qlib_adjusted_factor_contract_sha256": (
            benchmark.QLIB_ADJUSTED_FACTOR_CONTRACT_SHA256
        ),
        "qlib_adjusted_factor_integrity_audit": factor_audit,
        "qlib_adjusted_factor_integrity_audit_sha256": factor_audit[
            "audit_sha256"
        ],
        "adj_factor_restatement_authority_canonical_sha256": (
            benchmark.EXPECTED_ADJ_FACTOR_RESTATEMENT_AUTHORITY_SHA256
        ),
        "adj_factor_restatement_audit": restatement_audit,
        "adj_factor_restatement_audit_sha256": restatement_audit["audit_sha256"],
        "source_preflight_audit": preflight,
        "source_preflight_audit_sha256": preflight["audit_sha256"],
        "benchmark_contract": benchmark.BENCHMARK_CONTRACT,
        "benchmark_contract_sha256": benchmark.BENCHMARK_CONTRACT_SHA256,
        "chunk_size": 64,
        "source_code": {
            role: {"path": filename, "sha256": "2" * 64, "size_bytes": 1}
            for role, filename in benchmark.SOURCE_CODE_FILES.items()
        },
        "result_class": benchmark.RESULT_CLASS,
        "selected_trial_count": 0,
        "registry_write": False,
        "current_write": False,
        "serving_model_artifact_write": False,
        "card_write": False,
        "alert_write": False,
        "order_write": False,
        "database_write": False,
        "runtime_write": False,
    }
    request["request_sha256"] = canonical_sha256(request)
    return request


def test_qlib_adjusted_bars_reconstruct_export_units_without_account_actions():
    bars = pd.DataFrame(
        {
            "open": [10.0, 10.5],
            "high": [10.2, 10.8],
            "low": [9.8, 10.1],
            "close": [10.1, 10.6],
            "volume": [1000.0, 1200.0],
            "factor": [2.0, 2.5],
            "up_limit": [11.0, 11.55],
            "down_limit": [9.0, 9.45],
        },
        index=pd.to_datetime(["2024-01-02", "2024-01-03"]),
    )

    adjusted = benchmark._qlib_adjusted_bars(bars, symbol="000001.SZ")

    assert adjusted["close"].tolist() == pytest.approx([20.2, 26.5])
    assert adjusted["volume"].tolist() == pytest.approx([500.0, 480.0])
    assert adjusted["up_limit"].tolist() == pytest.approx([22.0, 28.875])
    assert adjusted["source_adjustment_factor"].tolist() == [2.0, 2.5]
    assert adjusted["factor"].tolist() == [1.0, 1.0]


def test_prepare_cli_has_no_account_level_corporate_action_inputs():
    parser = benchmark._parser()
    subparsers = next(
        action
        for action in parser._actions
        if action.__class__.__name__ == "_SubParsersAction"
    )
    prepare_help = subparsers.choices["prepare"].format_help()

    assert "--corporate-action-snapshot" not in prepare_help
    assert "--corporate-action-full-scope-authority" not in prepare_help
    assert "--rights-issue" not in prepare_help


def test_adjusted_request_is_hash_bound_and_rejects_account_authorities(
    tmp_path: Path,
):
    request = _adjusted_request_fixture(tmp_path)
    request_path = tmp_path / "request.json"
    request_path.write_bytes(canonical_json_bytes(request))
    assert canonical_json_bytes(benchmark._load_request(request_path)) == (
        canonical_json_bytes(request)
    )

    forbidden = dict(request)
    forbidden["corporate_action_snapshot"] = {"sha256": "3" * 64}
    forbidden.pop("request_sha256")
    forbidden["request_sha256"] = canonical_sha256(forbidden)
    request_path.write_bytes(canonical_json_bytes(forbidden))
    with pytest.raises(
        ActionValueError, match="PATTERN_BENCHMARK_REQUEST_IDENTITY_MISMATCH"
    ):
        benchmark._load_request(request_path)

    hash_only = dict(request)
    hash_only["rights_issue_authority_canonical_sha256"] = "4" * 64
    hash_only.pop("request_sha256")
    hash_only["request_sha256"] = canonical_sha256(hash_only)
    request_path.write_bytes(canonical_json_bytes(hash_only))
    with pytest.raises(
        ActionValueError, match="PATTERN_BENCHMARK_REQUEST_IDENTITY_MISMATCH"
    ):
        benchmark._load_request(request_path)

    drifted = dict(request)
    drifted["qlib_adjusted_factor_contract"] = {
        **benchmark.QLIB_ADJUSTED_FACTOR_CONTRACT,
        "corporate_action_accounting": "SIMULATED",
    }
    drifted.pop("request_sha256")
    drifted["request_sha256"] = canonical_sha256(drifted)
    request_path.write_bytes(canonical_json_bytes(drifted))
    with pytest.raises(
        ActionValueError, match="PATTERN_BENCHMARK_REQUEST_IDENTITY_MISMATCH"
    ):
        benchmark._load_request(request_path)
