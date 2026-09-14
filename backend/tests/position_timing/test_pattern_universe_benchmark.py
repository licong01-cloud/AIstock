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


def test_research_question_and_exploratory_scope_are_frozen():
    assert benchmark.BENCHMARK_CONTRACT["primary_comparator"] == "SAME_STOCK_BUY_AND_HOLD"
    assert benchmark.BENCHMARK_CONTRACT["main_family_size"] == 6
    assert benchmark.RESULT_CLASS == "EXPLORATORY_CROSS_SYMBOL_EXTERNAL_VALIDITY_NOT_TEMPORAL_HOLDOUT"
    assert benchmark.BENCHMARK_CONTRACT["selection"] is False
    assert benchmark.BENCHMARK_CONTRACT["serving"] is False


def test_frozen_authorities_match_r4_candidate(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
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
        rows, pool_id="csi300", strategy_id=benchmark.PRIMARY_STRATEGY
    )
    assert partial["timing_increment_bps_sum"].tolist() == [-10.0, 5.0]
    assert summary["timing_increment_total_bps"] == pytest.approx(-5.0)
    assert "market" not in summary


def test_drawdown_includes_the_frozen_initial_capital():
    assert benchmark._max_drawdown_bps(
        np.array([0.9, 1.1]), initial_value=1.0
    ) == pytest.approx(-1000.0)


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


def test_candidate_and_comparator_share_terminal_and_accounting_contracts():
    terminal = benchmark.BENCHMARK_CONTRACT["terminal"]
    assert terminal == {
        "mode": "TERMINAL_LIQUIDATED",
        "max_defer_trading_days": 5,
    }
    assert benchmark.BENCHMARK_CONTRACT["primary_comparator"] == "SAME_STOCK_BUY_AND_HOLD"


def test_six_pool_familywise_classification_is_frozen():
    assert benchmark.FAMILY_SIZE == 6
    assert benchmark.FAMILYWISE_CONFIDENCE == pytest.approx(1 - 0.05 / 6)
    assert benchmark._effect_evidence(
        {"lower_bps": 0.01, "upper_bps": 1.0}, coverage_complete=True
    ) == "SUPPORTED"
    assert benchmark._effect_evidence(
        {"lower_bps": -2.0, "upper_bps": -0.01}, coverage_complete=True
    ) == "NEGATIVE"
    assert benchmark._effect_evidence(
        {"lower_bps": -1.0, "upper_bps": 1.0}, coverage_complete=True
    ) == "INCONCLUSIVE"


def test_chunk_manifest_and_exact_retry_are_immutable(tmp_path: Path):
    target = tmp_path / "chunk"
    identity = {"request_sha256": "a" * 64, "chunk_ordinal": 0}
    daily = pd.DataFrame(
        {
            "pool_id": ["stock_universe"],
            "strategy_id": [benchmark.PRIMARY_STRATEGY],
            "valuation_date": pd.to_datetime(["2024-01-02"]),
            "active_sleeve_count": [1],
        }
    )
    fills = pd.DataFrame({"fill_status": ["FILLED"]})
    summaries = pd.DataFrame({"symbol": ["000001.SZ"]})
    diagnostics = {"diagnostics_sha256": "fixture"}
    first = benchmark._publish_chunk(
        target=target,
        chunk_identity=identity,
        daily=daily,
        fills=fills,
        summaries=summaries,
        diagnostics=diagnostics,
    )
    second = benchmark._publish_chunk(
        target=target,
        chunk_identity=identity,
        daily=daily,
        fills=fills,
        summaries=summaries,
        diagnostics=diagnostics,
    )
    assert first["manifest_sha256"] == second["manifest_sha256"]
    (target / "diagnostics.json").write_text("tampered", encoding="utf-8")
    with pytest.raises(
        ActionValueError, match="PATTERN_BENCHMARK_CHUNK_FILE_IDENTITY_MISMATCH"
    ):
        benchmark.inspect_chunk(target, expected_identity=identity)


def test_bundle_inspection_recursively_validates_external_chunk(tmp_path: Path):
    chunk = tmp_path / "chunk"
    chunk_identity = {"request_sha256": "a" * 64, "chunk_ordinal": 0}
    benchmark._publish_chunk(
        target=chunk,
        chunk_identity=chunk_identity,
        daily=pd.DataFrame({"value": [1]}),
        fills=pd.DataFrame({"value": [1]}),
        summaries=pd.DataFrame({"value": [1]}),
        diagnostics={"diagnostics_sha256": "fixture"},
    )
    request = {
        "schema_version": benchmark.REQUEST_SCHEMA,
        "pipeline_id": benchmark.PIPELINE_ID,
        "benchmark_contract": benchmark.BENCHMARK_CONTRACT,
        "benchmark_contract_sha256": benchmark.BENCHMARK_CONTRACT_SHA256,
        "pool_ids": benchmark.POOL_IDS,
        "population_symbols": (),
        "population_symbols_sha256": canonical_sha256(()),
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
    request_path = tmp_path / "request.json"
    request_path.write_bytes(canonical_json_bytes(request))
    receipt = {
        "schema_version": benchmark.RECEIPT_SCHEMA,
        "pipeline_id": benchmark.PIPELINE_ID,
        "request_sha256": request["request_sha256"],
        "result_class": benchmark.RESULT_CLASS,
        "selected_trial_count": 0,
        **benchmark.EXTERNAL_WRITE_RECEIPT_FLAGS,
    }
    receipt["receipt_sha256"] = canonical_sha256(receipt)
    coverage = {"status": "fixture"}
    coverage["coverage_sha256"] = canonical_sha256(coverage)
    bundle = tmp_path / request["request_sha256"]
    benchmark._publish_bundle(
        bundle=bundle,
        request_path=request_path,
        chunk_roots=(chunk,),
        daily=pd.DataFrame({"value": [1]}),
        pool_summary=pd.DataFrame({"value": [1]}),
        symbol_summary=pd.DataFrame({"value": [1]}),
        coverage=coverage,
        receipt=receipt,
    )
    benchmark.inspect_bundle(bundle)
    (chunk / "diagnostics.json").write_text("tampered", encoding="utf-8")
    with pytest.raises(
        ActionValueError, match="PATTERN_BENCHMARK_EXTERNAL_CHUNK_CHANGED"
    ):
        benchmark.inspect_bundle(bundle)


def test_receipt_declares_all_external_writes_false():
    assert benchmark.EXTERNAL_WRITE_RECEIPT_FLAGS
    assert all(value is False for value in benchmark.EXTERNAL_WRITE_RECEIPT_FLAGS.values())
