from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from backend.services.quantevolver import config_composer as composer_module
from backend.services.quantevolver.qe_active_dataset_profile import (
    ACTIVE_PROFILE_ENV,
    QEActiveDatasetProfileError,
    enforce_qe_universe_topk,
    get_qe_dataset_profile_summary,
    is_pure_star50_universe,
    load_active_qe_profile,
    reject_client_dataset_internals,
    resolve_active_dataset_node_binding,
    resolve_active_qe_dataset,
)
from backend.services.quantevolver.qe_dataset_contract import QE_DIRECT_V2_INDEX_CODES
from backend.services.dataset_release.canonical import digest_named_fields
from backend.services.dataset_release.shared_sector_context import (
    RELEASE_SW_L2_CODE_MAP_SCHEMA,
    RELEASE_SW_L2_MEMBER_BACKED_SCHEMA,
    SECTOR_QUOTE_AVAILABILITY_SCHEMA,
)
from backend.services.quantevolver.experiment_config import ExperimentConfig
from backend.services.quantevolver.experiment_config_builders import (
    build_config_from_custom_evo_loop,
    build_config_from_strategy_evo_loop,
)
from scripts.qe_active_dataset_profile import (
    _activate,
    _audit_runtime_binding,
    _runtime_bindings,
    _sha256,
    _validate,
)


def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _write(path: Path, payload: bytes) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return _sha(payload)


def _canonical(value: dict) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()


@pytest.mark.parametrize(
    "stock_pool",
    ["star50", "科创50", "000688.SH", "index_pool__star50", "index_pool__star50.txt"],
)
def test_star50_top20_contract_accepts_aliases_and_defaults(stock_pool: str) -> None:
    assert is_pure_star50_universe(stock_pool=stock_pool) is True
    assert enforce_qe_universe_topk(None, stock_pool=stock_pool)["topk"] == 20
    assert enforce_qe_universe_topk({"topk": 20}, stock_pool=stock_pool)["topk"] == 20


@pytest.mark.parametrize(
    "stock_pool",
    [
        "/tmp/qe/pools/index_pool__star50.txt",
        r"X:\qe\pools\index_pool__star50.txt",
    ],
)
def test_star50_top20_contract_accepts_pool_sidecar_paths(stock_pool: str) -> None:
    assert is_pure_star50_universe(stock_pool=stock_pool) is True
    assert enforce_qe_universe_topk(None, stock_pool=stock_pool)["topk"] == 20


@pytest.mark.parametrize("topk", [10, 50, 20.0, True, "20"])
def test_star50_top20_contract_rejects_explicit_non_integer_20(topk: object) -> None:
    with pytest.raises(QEActiveDatasetProfileError, match="qe_star50_topk_required"):
        enforce_qe_universe_topk(
            {"topk": topk},
            universe_selection={"mode": "single_index", "pool_ids": ["star50"]},
        )


def test_star50_top20_contract_does_not_rewrite_union_or_other_pool() -> None:
    assert (
        enforce_qe_universe_topk(
            {"topk": 50},
            universe_selection={"mode": "index_union", "pool_ids": ["csi300", "star50"]},
        )["topk"]
        == 50
    )
    assert (
        enforce_qe_universe_topk(
            {"topk": 50},
            universe_selection={"mode": "single_index", "pool_ids": ["star100"]},
        )["topk"]
        == 50
    )


def test_star50_top20_contract_applies_to_normalized_single_pool_union() -> None:
    selection = {"mode": "index_union", "pool_ids": ["star50"]}

    assert is_pure_star50_universe(universe_selection=selection) is True
    assert enforce_qe_universe_topk(None, universe_selection=selection)["topk"] == 20
    with pytest.raises(QEActiveDatasetProfileError, match="qe_star50_topk_required"):
        enforce_qe_universe_topk({"topk": 50}, universe_selection=selection)


def test_star50_identity_accepts_frozen_selection_pins_with_extra_fields() -> None:
    assert (
        is_pure_star50_universe(
            universe_selection={
                "mode": "single_index",
                "pool_ids": ["star50"],
                "instrument_name": "index_pool__star50",
                "membership_revision": "pit-rev",
            }
        )
        is True
    )


def test_experiment_config_is_final_star50_top20_consumer_guard() -> None:
    config = ExperimentConfig(
        factor_names=["alpha"],
        model_id="model",
        stock_pool="index_pool__star50",
    )
    assert config.strategy_params == {"topk": 20}

    with pytest.raises(ValueError, match="qe_star50_topk_required"):
        ExperimentConfig(
            factor_names=["alpha"],
            model_id="model",
            stock_pool="index_pool__star50",
            strategy_params={"topk": 50},
        )


def _fixture_profile(
    tmp_path: Path,
    *,
    with_gap: bool = False,
    with_sector_policy: bool = False,
    with_sector_context: bool = False,
) -> Path:
    assert not (with_sector_policy and with_sector_context)
    candidate = tmp_path / "candidate"
    pool_root = tmp_path / "pools"
    day = candidate / "components" / "daily_bin_candidate"
    calendar = b"2018-08-01\n2022-12-30\n2023-01-03\n2024-06-28\n2024-07-01\n2026-08-27\n2026-08-28\n2026-08-31\n"
    calendar_sha = _write(day / "calendars" / "day.txt", calendar)
    stock_content = b"000001.SZ\t2018-08-01\t2026-08-31\n"
    stock_sha = _write(day / "instruments" / "stock_universe.txt", stock_content)
    benchmark_content = b"000300.SH\t2018-08-01\t2026-08-31\n"
    benchmark_sha = _write(
        day / "instruments" / "benchmark.txt",
        benchmark_content,
    )
    day_all_sha = _write(day / "instruments" / "all.txt", stock_content + benchmark_content)
    day_meta_sha = _write(
        day / "meta_export.json",
        _canonical(
            {
                "snapshot_id": "daily_bin_candidate",
                "universe_key": "aistock_equity_pit_canonical_v2",
                "rule_version": "pit-v2",
            }
        ),
    )
    minute = candidate / "components" / "minute_bin_candidate"
    minute_all_sha = _write(minute / "instruments" / "all.txt", stock_content)
    minute_calendar_sha = _write(minute / "calendars" / "1min.txt", calendar)
    minute_meta_sha = _write(
        minute / "meta_export.json",
        _canonical(
            {
                "snapshot_id": "minute_bin_candidate",
                "universe_key": "aistock_equity_pit_canonical_v2",
                "rule_version": "pit-v2",
            }
        ),
    )
    csi300 = b"000001.SZ\t2024-07-01\t2026-08-31\n000002.SZ\t2024-07-01\t2026-08-31\n"
    csi500 = b"000002.SZ\t2024-07-01\t2026-08-31\n000003.SZ\t2024-07-01\t2026-08-31\n"
    csi300_sha = _write(pool_root / "index_pool__csi300.txt", csi300)
    csi500_sha = _write(pool_root / "index_pool__csi500.txt", csi500)
    universes = {
        "stock_universe": {
            "label": "全市场股票池",
            "filename": "stock_universe.txt",
            "sha256": stock_sha,
            "membership_revision": "pit-v2",
        },
        "csi300": {
            "label": "沪深300",
            "filename": "index_pool__csi300.txt",
            "sha256": csi300_sha,
            "membership_revision": "csi300-v1",
        },
        "csi500": {
            "label": "中证500",
            "filename": "index_pool__csi500.txt",
            "sha256": csi500_sha,
            "membership_revision": "csi500-v1",
        },
    }
    gaps = (
        [{"symbol": "000002.SZ", "start": "2025-01-01", "end": "2025-01-31", "components": ["1min"]}]
        if with_gap
        else []
    )
    receipt = {
        "schema_version": "qe_index_pool_coverage_receipt_v1",
        "release_id": "qe-hmm-v2-20260831",
        "cutoff": "2026-08-31",
        "pools": {
            "stock_universe": {"available_start": "2018-08-01", "available_end": "2026-08-31", "gaps": []},
            "csi300": {"available_start": "2018-08-01", "available_end": "2026-08-31", "gaps": gaps},
            "csi500": {"available_start": "2018-08-01", "available_end": "2026-08-31", "gaps": []},
        },
    }
    receipt_payload = _canonical(receipt)
    receipt_path = tmp_path / "coverage.json"
    receipt_sha = _write(receipt_path, receipt_payload)
    factor_meta = {
        "schema_version": "qe_direct_factor_h5_static_v2",
        "start": "2018-08-01",
        "end": "2026-08-31",
        "universe_key": "aistock_equity_pit_canonical_v2",
    }
    factor_meta_sha = _write(
        candidate / "components" / "factor_h5_static_candidate_v2" / "meta.json",
        _canonical(factor_meta),
    )
    sector_data_sha = _write(
        candidate / "components" / "factor_h5_static_candidate_v2" / "sector_data.h5",
        b"sector-data-fixture",
    )
    sector_policy_pins = None
    if with_sector_policy:
        import pandas as pd

        factor_root = candidate / "components" / "factor_h5_static_candidate_v2"
        code_map = {
            "schema_version": "qe_sw_l2_code_map_v1",
            "ordered_codes": ["801010.SI", "801020.SI"],
        }
        code_map["code_map_digest"] = digest_named_fields(
            "dataset_release_sw_l2_code_map_v1",
            {"ordered_codes": code_map["ordered_codes"]},
        )
        code_map_sha = _write(
            factor_root / "sector_code_map.json",
            _canonical(code_map),
        )
        membership_path = factor_root / "sector_membership_spans.parquet"
        membership_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            [
                {"instrument": "000001.SZ", "start_date": "2018-08-01", "end_date": "2026-08-31", "l2_code_id": 0},
                {"instrument": "000002.SZ", "start_date": "2018-08-01", "end_date": "2026-08-31", "l2_code_id": 1},
                {"instrument": "000003.SZ", "start_date": "2018-08-01", "end_date": "2026-08-31", "l2_code_id": 1},
            ]
        ).to_parquet(membership_path, index=False)
        sector_policy_pins = {
            "schema_version": "qe_sector_policy_input_v1",
            "membership_file": membership_path.name,
            "membership_sha256": _sha(membership_path.read_bytes()),
            "code_map_file": "sector_code_map.json",
            "code_map_sha256": code_map_sha,
            "start": "2018-08-01",
            "end": "2026-08-31",
            "universe_key": "aistock_equity_pit_canonical_v2",
        }
    index_sha = _write(
        candidate / "components" / "index_context" / "index_daily.h5",
        b"index-fixture",
    )
    suspend_meta_sha = _write(
        candidate / "components" / "suspend_d_daily_candidate_v2" / "meta.json",
        _canonical(
            {
                "schema_version": "qe_direct_suspend_d_v1",
                "component": "suspend_d",
                "start": "2018-08-01",
                "end": "2026-08-31",
                "universe_key": "aistock_equity_pit_canonical_v2",
                "source_table": "market.suspend_d",
                "suspend_type": "S",
            }
        ),
    )
    suspend_parquet_sha = _write(
        candidate / "components" / "suspend_d_daily_candidate_v2" / "suspend_d.parquet",
        b"suspend-parquet",
    )
    components = {
        "factor_meta": factor_meta,
        "factor_meta_sha256": factor_meta_sha,
        "day_pins": {
            "snapshot_id": "daily_bin_candidate",
            "universe_key": "aistock_equity_pit_canonical_v2",
            "rule_version": "pit-v2",
            "instruments_sha256": day_all_sha,
            "calendar_sha256": calendar_sha,
            "meta_export_sha256": day_meta_sha,
        },
        "minute_pins": {
            "snapshot_id": "minute_bin_candidate",
            "universe_key": "aistock_equity_pit_canonical_v2",
            "rule_version": "pit-v2",
            "instruments_sha256": minute_all_sha,
            "calendar_sha256": minute_calendar_sha,
            "meta_export_sha256": minute_meta_sha,
        },
        "benchmark_instruments_sha256": benchmark_sha,
        "index_pins": {"sha256": index_sha, "max_date": "2026-08-31", "codes": list(QE_DIRECT_V2_INDEX_CODES)},
        "suspend_pins": {
            "dataset_id": "suspend_d_daily_candidate_v2",
            "schema_version": "qe_direct_suspend_d_v1",
            "source_contract": "market.suspend_d",
            "metadata_sha256": suspend_meta_sha,
            "parquet_sha256": suspend_parquet_sha,
        },
    }
    if sector_policy_pins is not None:
        components["sector_policy_pins"] = sector_policy_pins
    if with_sector_context:
        import pandas as pd

        sector_root = candidate / "components" / "sector_context_candidate_v1"
        authority = {"authority_id": "fixture", "authority_sha256": "a" * 64}
        entries = [{"l2_code_id": index * 2 + 1, "canonical_l2_code": f"801{index:03d}.SI"} for index in range(131)]
        member_codes = sorted(row["canonical_l2_code"] for row in entries)
        code_map = {
            "schema_version": RELEASE_SW_L2_CODE_MAP_SCHEMA,
            "mapping_authority": authority,
            "entries": entries,
            "member_backed_codes": member_codes,
            "code_map_digest": digest_named_fields(
                RELEASE_SW_L2_CODE_MAP_SCHEMA,
                {"mapping_authority": authority, "entries": entries},
            ),
            "member_backed_digest": digest_named_fields(
                RELEASE_SW_L2_MEMBER_BACKED_SCHEMA,
                {"mapping_authority": authority, "member_backed_codes": member_codes},
            ),
        }
        code_map_sha = _write(sector_root / "sector_code_map.json", _canonical(code_map))
        market_path = sector_root / "market_context.parquet"
        market_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            {
                "trade_date": ["2018-08-01", "2026-08-31"],
                "sw_daily_total_vol": [1.0, 2.0],
            }
        ).to_parquet(market_path, index=False)
        membership_path = sector_root / "sector_membership_spans.parquet"
        pd.DataFrame(
            [
                {
                    "instrument": "000001.SZ",
                    "start_date": "2018-08-01",
                    "end_date": "2026-08-31",
                    "l2_code_id": 21,
                },
                {
                    "instrument": "000002.SZ",
                    "start_date": "2018-08-01",
                    "end_date": "2026-08-31",
                    "l2_code_id": 41,
                },
                {
                    "instrument": "000003.SZ",
                    "start_date": "2018-08-01",
                    "end_date": "2026-08-31",
                    "l2_code_id": 41,
                },
            ]
        ).to_parquet(membership_path, index=False)
        quote_entries = [
            {
                "canonical_l2_code": row["canonical_l2_code"],
                "availability_spans": [
                    {"start_date": "2018-08-01", "end_date": "2026-08-31"}
                ],
            }
            for row in sorted(code_map["entries"], key=lambda row: row["canonical_l2_code"])
        ]
        quote_payload = {
            "schema_version": SECTOR_QUOTE_AVAILABILITY_SCHEMA,
            "mapping_authority": authority,
            "entries": quote_entries,
            "quote_availability_digest": digest_named_fields(
                SECTOR_QUOTE_AVAILABILITY_SCHEMA,
                {"mapping_authority": authority, "entries": quote_entries},
            ),
        }
        quote_path = sector_root / "sector_quote_availability.json"
        quote_sha = _write(quote_path, _canonical(quote_payload))
        context_receipt_sha = _write(
            sector_root / "component_receipt.json",
            _canonical({"schema_version": "aistock_sector_context_receipt_v1"}),
        )
        components["sector_context_pins"] = {
            "schema_version": "aistock_sector_context_pins_v1",
            "component_root": "components/sector_context_candidate_v1",
            "code_map_file": "sector_code_map.json",
            "code_map_sha256": code_map_sha,
            "code_map_digest": code_map["code_map_digest"],
            "market_context_file": "market_context.parquet",
            "market_context_sha256": _sha(market_path.read_bytes()),
            "market_volume_definition": "sum_market_sw_daily_vol_all_rows_v1",
            "market_start": "2018-08-01",
            "market_end": "2026-08-31",
            "membership_file": "sector_membership_spans.parquet",
            "membership_sha256": _sha(membership_path.read_bytes()),
            "membership_start": "2018-08-01",
            "membership_end": "2026-08-31",
            "quote_availability_file": "sector_quote_availability.json",
            "quote_availability_sha256": quote_sha,
            "quote_availability_digest": quote_payload["quote_availability_digest"],
            "quote_availability_schema": SECTOR_QUOTE_AVAILABILITY_SCHEMA,
            "receipt_file": "component_receipt.json",
            "receipt_sha256": context_receipt_sha,
            "sector_data_sha256": sector_data_sha,
            "source_dataset_manifest_sha256": "b" * 64,
            "authority_id": authority["authority_id"],
            "authority_sha256": authority["authority_sha256"],
        }
        manifest_identity = {
            "schema_version": "qe_dataset_manifest_v1",
            "release_id": "qe-hmm-v2-20260831",
            "cutoff_trade_date": "2026-08-31",
        }
        manifest_sha = _sha(
            json.dumps(
                manifest_identity,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        manifest = {**manifest_identity, "dataset_manifest_sha256": manifest_sha}
        manifest_file_sha = _write(
            candidate / "qe_dataset_manifest.json",
            _canonical(manifest),
        )
        components["dataset_manifest_sha256"] = manifest_sha
        components["dataset_manifest_file_sha256"] = manifest_file_sha
    schema_version = "aistock_active_dataset_profile_v1"
    if with_sector_policy:
        schema_version = "aistock_active_dataset_profile_v2"
    elif with_sector_context:
        schema_version = "aistock_active_dataset_profile_v3"
    profile = {
        "schema_version": schema_version,
        "generation": "20260906-v1",
        "release_id": "qe-hmm-v2-20260831",
        "cutoff": "2026-08-31",
        "controller_paths": {
            "candidate_root": str(candidate),
            "stock_pool_root": str(pool_root),
            "coverage_receipt_path": str(receipt_path),
        },
        "components": components,
        "node_bindings": {
            "wsl2-5080": {"candidate_root": "/mnt/x/candidate"},
            "rdagent-node1": {"candidate_root": "/home/lc999/candidate"},
        },
        "consumers": {
            "qe": {
                "defaults": {
                    "train_start": "2018-08-01",
                    "train_end": "2022-12-30",
                    "valid_start": "2023-01-03",
                    "valid_end": "2024-06-28",
                    "test_start": "2024-07-01",
                    "test_end": "2026-08-31",
                    "signal_end": "2026-08-31",
                    "backtest_end": "2026-08-28",
                },
                "default_universe": {"mode": "stock_universe", "pool_ids": []},
                "universes": universes,
                "coverage_receipt_sha256": receipt_sha,
            }
        },
    }
    if with_sector_context:
        profile["consumers"]["qe"]["required_components"] = sorted(
            {
                "day",
                "minute",
                "factor",
                "index",
                "suspend",
                "benchmark",
                "stock_pools",
                "coverage",
                "manifest",
                "sector_context",
            }
        )
        profile["consumers"].update(
            {
                "hmm": {"required_components": sorted({"factor", "index", "manifest", "sector_context"})},
                "selection": {
                    "required_components": sorted(
                        {"day", "minute", "factor", "index", "suspend", "stock_pools", "manifest"}
                    )
                },
                "advisory": {
                    "required_components": sorted(
                        {"day", "minute", "factor", "index", "suspend", "stock_pools", "manifest"}
                    )
                },
            }
        )
    profile_path = tmp_path / "active.json"
    _write(profile_path, _canonical(profile))
    return profile_path


def test_legacy_summary_is_explicit_when_profile_not_activated(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(ACTIVE_PROFILE_ENV, raising=False)
    summary = get_qe_dataset_profile_summary()
    assert summary["mode"] == "legacy_default_not_activated"
    assert summary["defaults"]["test_end"] == "2026-06-30"


def test_profile_resolves_node_dates_and_stock_universe(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = _fixture_profile(tmp_path)
    monkeypatch.setenv(ACTIVE_PROFILE_ENV, str(path))
    profile = load_active_qe_profile()
    assert profile is not None
    resolved = resolve_active_qe_dataset(node_id="wsl2-5080", label_horizon=1, profile=profile)
    assert resolved is not None
    assert resolved.data_split["test_end"] == "2026-08-31"
    assert resolved.data_split["backtest_end"] == "2026-08-28"
    assert resolved.binding.provider_uri_day == "/mnt/x/candidate/components/daily_bin_candidate"
    assert resolved.binding.selection_pins["mode"] == "stock_universe"
    assert resolved.stock_pool_content is None
    assert resolved.outcome_observable_end == "2026-08-28"
    persisted = resolved.apply({})["_qe_active_dataset_summary"]
    assert persisted["profile_sha256"] == profile.profile_sha256
    assert persisted["resolved_at_utc"].endswith("+00:00")
    assert "profile_sha256" not in resolved.profile_summary


def test_explicit_date_override_is_bounded_by_active_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = _fixture_profile(tmp_path)
    monkeypatch.setenv(ACTIVE_PROFILE_ENV, str(path))
    profile = load_active_qe_profile()

    earlier = resolve_active_qe_dataset(
        node_id="wsl2-5080",
        data_split={"test_end": "2026-08-28"},
        label_horizon=1,
        profile=profile,
    )
    assert earlier is not None
    assert earlier.data_split["test_end"] == "2026-08-28"
    assert earlier.data_split["backtest_end"] == "2026-08-28"
    assert earlier.outcome_observable_end == "2026-08-27"

    with pytest.raises(QEActiveDatasetProfileError, match="qe_dataset_window_outside_release"):
        resolve_active_qe_dataset(
            node_id="wsl2-5080",
            data_split={"test_end": "2026-09-01"},
            profile=profile,
        )


def test_single_and_union_sidecars_are_deterministic(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = _fixture_profile(tmp_path)
    monkeypatch.setenv(ACTIVE_PROFILE_ENV, str(path))
    profile = load_active_qe_profile()
    single = resolve_active_qe_dataset(
        node_id="rdagent-node1",
        universe_selection={"mode": "single_index", "pool_ids": ["csi300"]},
        profile=profile,
    )
    union_a = resolve_active_qe_dataset(
        node_id="rdagent-node1",
        universe_selection={"mode": "index_union", "pool_ids": ["csi500", "csi300"]},
        profile=profile,
    )
    union_b = resolve_active_qe_dataset(
        node_id="rdagent-node1",
        universe_selection={"mode": "index_union", "pool_ids": ["csi300", "csi500"]},
        profile=profile,
    )
    assert single is not None and single.binding.selection_pins["instrument_name"] == "index_pool__csi300"
    assert union_a is not None and union_b is not None
    assert union_a.stock_pool_content == union_b.stock_pool_content
    assert union_a.binding.selection_pins == union_b.binding.selection_pins
    assert union_a.stock_pool_content.count("000002.SZ") == 1


def test_sector_blacklist_materializes_run_scoped_pool_without_changing_p00(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = _fixture_profile(tmp_path, with_sector_policy=True)
    monkeypatch.setenv(ACTIVE_PROFILE_ENV, str(path))
    profile = load_active_qe_profile()
    assert profile is not None

    baseline = resolve_active_qe_dataset(
        node_id="wsl2-5080",
        universe_selection={"mode": "single_index", "pool_ids": ["csi300"]},
        profile=profile,
    )
    filtered = resolve_active_qe_dataset(
        node_id="wsl2-5080",
        universe_selection={"mode": "single_index", "pool_ids": ["csi300"]},
        custom_params={"sector_blacklist": ["801010.SI"]},
        profile=profile,
    )

    assert baseline is not None and filtered is not None
    assert baseline.stock_pool_content == ("000001.SZ\t2024-07-01\t2026-08-31\n000002.SZ\t2024-07-01\t2026-08-31\n")
    assert filtered.stock_pool_content == "000002.SZ\t2024-07-01\t2026-08-31\n"
    assert filtered.binding.selection_pins["instrument_name"] == "index_pool__csi300"
    assert (
        filtered.binding.selection_pins["instruments_sha256"] != baseline.binding.selection_pins["instruments_sha256"]
    )
    applied = filtered.apply({"sector_blacklist": ["801010.SI"]})
    assert applied["_qe_sector_blacklist_policy"]["blacklist_excluded_count"] == 1
    assert applied["_qe_run_stock_pool_content"] == filtered.stock_pool_content


def test_v3_profile_uses_shared_sparse_sector_context(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = _fixture_profile(tmp_path, with_sector_context=True)
    monkeypatch.setenv(ACTIVE_PROFILE_ENV, str(path))
    profile = load_active_qe_profile()
    assert profile is not None
    assert profile.raw["schema_version"] == "aistock_active_dataset_profile_v3"

    filtered = resolve_active_qe_dataset(
        node_id="wsl2-5080",
        universe_selection={"mode": "single_index", "pool_ids": ["csi300"]},
        custom_params={"sector_blacklist": ["801010.SI"]},
        profile=profile,
    )

    assert filtered is not None
    assert filtered.stock_pool_content == "000002.SZ\t2024-07-01\t2026-08-31\n"
    assert filtered.sector_blacklist_policy is not None
    assert filtered.sector_blacklist_policy["blacklist_excluded_count"] == 1


def test_v3_sector_blacklist_starts_at_test_without_truncating_training_universe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import pandas as pd

    path = _fixture_profile(tmp_path, with_sector_context=True)
    raw = json.loads(path.read_text(encoding="utf-8"))
    sector_pins = raw["components"]["sector_context_pins"]
    membership_path = (
        Path(raw["controller_paths"]["candidate_root"])
        / sector_pins["component_root"]
        / sector_pins["membership_file"]
    )
    membership = pd.read_parquet(membership_path)
    membership["start_date"] = "2024-07-01"
    membership.to_parquet(membership_path, index=False)
    sector_pins["membership_start"] = "2024-07-01"
    sector_pins["membership_sha256"] = _sha(membership_path.read_bytes())
    path.write_bytes(_canonical(raw))
    monkeypatch.setenv(ACTIVE_PROFILE_ENV, str(path))
    profile = load_active_qe_profile()
    assert profile is not None
    hmm_identity = {
        "enable_sector_hmm": True,
        "hmm_model_version_id": "ecee7c40-6764-49ad-bc0f-1c6c6b390504",
        "hmm_signal_preset": "preset_A",
        "hmm_signal_presets": {"preset_A": {"coefficient_sha256": "b" * 64}},
    }

    p10 = resolve_active_qe_dataset(
        node_id="wsl2-5080",
        custom_params=hmm_identity,
        profile=profile,
    )
    p11 = resolve_active_qe_dataset(
        node_id="wsl2-5080",
        custom_params={**hmm_identity, "sector_blacklist": ["801020.SI"]},
        profile=profile,
    )

    assert p10 is not None and p11 is not None
    assert p10.stock_pool_content is None
    assert p11.data_split == p10.data_split
    assert p11.stock_pool_content == "000001.SZ\t2018-08-01\t2026-08-31\n"
    assert p11.sector_blacklist_policy is not None
    assert p11.sector_blacklist_policy["policy_start"] == "2024-07-01"
    applied = p11.apply({**hmm_identity, "sector_blacklist": ["801020.SI"]})
    for key, value in hmm_identity.items():
        assert applied[key] == value


def test_active_profile_derives_shared_node_runtime_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = _fixture_profile(tmp_path, with_sector_context=True)
    monkeypatch.setenv(ACTIVE_PROFILE_ENV, str(path))

    binding = resolve_active_dataset_node_binding(node_id="rdagent-node1")

    assert binding is not None
    assert binding["candidate_root"] == "/home/lc999/candidate"
    assert binding["qlib_data_path"].endswith("/components/daily_bin_candidate")
    assert binding["qlib_minute_path"].endswith("/components/minute_bin_candidate")
    assert binding["factor_data_dir"].endswith("/components/factor_h5_static_candidate_v2")
    assert binding["sector_context_dir"].endswith("/components/sector_context_candidate_v1")


def test_active_profile_node_runtime_binding_fails_closed_for_unknown_node(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = _fixture_profile(tmp_path, with_sector_context=True)
    monkeypatch.setenv(ACTIVE_PROFILE_ENV, str(path))

    with pytest.raises(
        QEActiveDatasetProfileError,
        match="qe_active_dataset_node_binding_missing",
    ):
        resolve_active_dataset_node_binding(node_id="unknown-node")


def test_sector_blacklist_requires_v2_frozen_policy_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = _fixture_profile(tmp_path)
    monkeypatch.setenv(ACTIVE_PROFILE_ENV, str(path))
    profile = load_active_qe_profile()

    with pytest.raises(QEActiveDatasetProfileError, match="qe_sector_blacklist_frozen_mapping_missing"):
        resolve_active_qe_dataset(
            node_id="wsl2-5080",
            custom_params={"sector_blacklist": ["801010.SI"]},
            profile=profile,
        )


def test_profile_enabled_failures_do_not_fall_back(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    missing = tmp_path / "missing.json"
    monkeypatch.setenv(ACTIVE_PROFILE_ENV, str(missing))
    with pytest.raises(QEActiveDatasetProfileError, match="qe_active_dataset_profile_missing"):
        load_active_qe_profile()


def test_composer_rejects_active_profile_without_run_scoped_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(composer_module, "load_active_qe_profile", lambda: object())
    with pytest.raises(RuntimeError, match="qe_active_dataset_binding_missing"):
        composer_module._reject_unbound_active_dataset(
            {},
            operation="compose_experiment_in_memory",
        )

    monkeypatch.setattr(composer_module, "load_active_qe_profile", lambda: None)
    composer_module._reject_unbound_active_dataset(
        {},
        operation="compose_experiment_in_memory",
    )


def test_public_creation_rejects_server_owned_dataset_fields() -> None:
    with pytest.raises(QEActiveDatasetProfileError, match="qe_dataset_internal_input_forbidden"):
        reject_client_dataset_internals({"_qe_direct_v2_dataset_binding": {"provider_uri_day": "/client/path"}})


def test_resolver_rejects_non_positive_label_horizon(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = _fixture_profile(tmp_path)
    monkeypatch.setenv(ACTIVE_PROFILE_ENV, str(path))
    profile = load_active_qe_profile()

    with pytest.raises(QEActiveDatasetProfileError, match="qe_dataset_window_outside_release"):
        resolve_active_qe_dataset(
            node_id="wsl2-5080",
            label_horizon=0,
            profile=profile,
        )


def test_unknown_node_sidecar_tamper_and_coverage_gap_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _fixture_profile(tmp_path, with_gap=True)
    monkeypatch.setenv(ACTIVE_PROFILE_ENV, str(path))
    profile = load_active_qe_profile()
    with pytest.raises(QEActiveDatasetProfileError, match="qe_dataset_node_binding_missing"):
        resolve_active_qe_dataset(node_id="unknown", profile=profile)
    with pytest.raises(QEActiveDatasetProfileError, match="qe_universe_window_coverage_incomplete"):
        resolve_active_qe_dataset(
            node_id="wsl2-5080",
            universe_selection={"mode": "single_index", "pool_ids": ["csi300"]},
            profile=profile,
        )
    (tmp_path / "pools" / "index_pool__csi500.txt").write_text("tampered\n", encoding="utf-8")
    with pytest.raises(QEActiveDatasetProfileError, match="qe_universe_sidecar_hash_mismatch"):
        resolve_active_qe_dataset(
            node_id="wsl2-5080",
            universe_selection={"mode": "single_index", "pool_ids": ["csi500"]},
            profile=profile,
        )


def test_profile_cli_validate_and_atomic_activate(tmp_path: Path) -> None:
    source = _fixture_profile(tmp_path)
    digest = _sha256(source)
    before = source.read_bytes()

    validated = _validate(source)
    assert validated["profile_sha256"] == digest

    target = tmp_path / "state" / "active.json"
    target.parent.mkdir()
    activated = _activate(
        source=source,
        target=target,
        expected_source_sha256=digest,
        expected_current_sha256=None,
    )
    assert activated["status"] == "activated"
    assert target.read_bytes() == before
    assert source.read_bytes() == before

    absent = tmp_path / "state" / "must-not-exist.json"
    with pytest.raises(RuntimeError, match="source profile digest differs"):
        _activate(
            source=source,
            target=absent,
            expected_source_sha256="0" * 64,
            expected_current_sha256=None,
        )
    assert not absent.exists()


def test_profile_cli_derives_node_runtime_bindings_without_cache_paths(tmp_path: Path) -> None:
    source = _fixture_profile(tmp_path)

    result = _runtime_bindings(source)

    assert result["schema_version"] == "aistock_qe_runtime_bindings_v1"
    assert result["generation"] == "20260906-v1"
    assert sorted(result["nodes"]) == ["rdagent-node1", "wsl2-5080"]
    local = result["nodes"]["wsl2-5080"]
    assert local["factor_data_dir"] == "/mnt/x/candidate/components/factor_h5_static_candidate_v2"
    assert local["qlib_data_path"] == "/mnt/x/candidate/components/daily_bin_candidate"
    assert local["qlib_minute_path"] == "/mnt/x/candidate/components/minute_bin_candidate"
    assert local["environment"] == {
        "QE_DATASET_IDENTITY_ROOTS": "/mnt/x/candidate",
        "QE_QLIB_DATA_PATH": "/mnt/x/candidate/components/daily_bin_candidate",
        "QLIB_DATA_PATH_WSL": "/mnt/x/candidate/components/daily_bin_candidate",
        "QLIB_DAY_DATA": "/mnt/x/candidate/components/daily_bin_candidate",
        "QLIB_MINUTE_PATH_WSL": "/mnt/x/candidate/components/minute_bin_candidate",
        "QLIB_MINUTE_DATA": "/mnt/x/candidate/components/minute_bin_candidate",
        "RDAGENT_FACTOR_DATA_WSL": "/mnt/x/candidate/components/factor_h5_static_candidate_v2",
    }
    assert "QE_FACTOR_DATA_DIR" not in local["environment"]


def test_v3_runtime_bindings_expose_one_release_for_all_consumers(tmp_path: Path) -> None:
    source = _fixture_profile(tmp_path, with_sector_context=True)

    result = _runtime_bindings(source, node_id="wsl2-5080")

    local = result["nodes"]["wsl2-5080"]
    assert local["dataset_manifest_path"] == "/mnt/x/candidate/qe_dataset_manifest.json"
    assert local["sector_context_dir"] == ("/mnt/x/candidate/components/sector_context_candidate_v1")
    assert set(local["consumer_requirements"]) == {
        "qe",
        "hmm",
        "selection",
        "advisory",
    }
    assert local["environment"]["AISTOCK_DATASET_ROOT"] == "/mnt/x/candidate"


def test_profile_cli_runtime_binding_audit_fails_closed_on_stale_path(tmp_path: Path) -> None:
    source = _fixture_profile(tmp_path)
    expected = {
        "factor_data_dir": "/home/lc999/candidate/components/factor_h5_static_candidate_v2",
        "qlib_data_path": "/home/lc999/candidate/components/daily_bin_candidate",
        "qlib_minute_path": "/home/lc999/candidate/components/minute_bin_candidate",
    }

    result = _audit_runtime_binding(
        source,
        node_id="rdagent-node1",
        actual=expected,
    )
    assert result["status"] == "runtime_bindings_match"

    with pytest.raises(RuntimeError, match="runtime bindings differ from active profile"):
        _audit_runtime_binding(
            source,
            node_id="rdagent-node1",
            actual={**expected, "qlib_data_path": "/home/lc999/r6/day"},
        )


def test_profile_cli_runtime_bindings_reject_unknown_node(tmp_path: Path) -> None:
    source = _fixture_profile(tmp_path)

    with pytest.raises(RuntimeError, match="node is absent from active profile"):
        _runtime_bindings(source, node_id="missing-node")


def test_profile_activation_rejects_suspend_source_contract_drift(tmp_path: Path) -> None:
    source = _fixture_profile(tmp_path)
    profile = json.loads(source.read_text(encoding="utf-8"))
    profile["components"]["suspend_pins"]["source_contract"] = "tushare_suspend_d_shsz_S_v1"
    source.write_bytes(_canonical(profile))

    with pytest.raises(
        QEActiveDatasetProfileError,
        match="suspend metadata source_table differs from the active profile",
    ):
        _validate(source)


def test_profile_activation_replaces_exactly_pinned_invalid_current_profile(
    tmp_path: Path,
) -> None:
    source = _fixture_profile(tmp_path / "source")
    target = _fixture_profile(tmp_path / "target")
    invalid_profile = json.loads(target.read_text(encoding="utf-8"))
    invalid_profile["components"]["suspend_pins"]["source_contract"] = "tushare_suspend_d_shsz_S_v1"
    target.write_bytes(_canonical(invalid_profile))
    invalid_bytes = target.read_bytes()
    invalid_digest = _sha256(target)

    with pytest.raises(
        QEActiveDatasetProfileError,
        match="suspend metadata source_table differs from the active profile",
    ):
        _validate(target)

    with pytest.raises(RuntimeError, match="current target digest differs"):
        _activate(
            source=source,
            target=target,
            expected_source_sha256=_sha256(source),
            expected_current_sha256="0" * 64,
        )
    assert target.read_bytes() == invalid_bytes

    activated = _activate(
        source=source,
        target=target,
        expected_source_sha256=_sha256(source),
        expected_current_sha256=invalid_digest,
    )

    assert activated["status"] == "activated"
    assert target.read_bytes() == source.read_bytes()


def test_profile_summary_does_not_expose_paths_or_hashes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = _fixture_profile(tmp_path)
    monkeypatch.setenv(ACTIVE_PROFILE_ENV, str(path))

    summary = get_qe_dataset_profile_summary()
    serialized = json.dumps(summary, ensure_ascii=False, sort_keys=True)

    assert summary["mode"] == "active_profile"
    assert summary["release_id"] == "qe-hmm-v2-20260831"
    assert "candidate_root" not in serialized
    assert "stock_pool_root" not in serialized
    assert "sha256" not in serialized


def test_resolver_rejects_component_hash_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = _fixture_profile(tmp_path)
    monkeypatch.setenv(ACTIVE_PROFILE_ENV, str(path))
    profile = load_active_qe_profile()
    minute_calendar = tmp_path / "candidate" / "components" / "minute_bin_candidate" / "calendars" / "1min.txt"
    minute_calendar.write_bytes(b"tampered\n")

    with pytest.raises(QEActiveDatasetProfileError, match="qe_dataset_component_identity_mismatch"):
        resolve_active_qe_dataset(node_id="wsl2-5080", profile=profile)


def test_evolution_builders_preserve_resolved_binding_and_node() -> None:
    binding = {"schema_version": "qe_direct_v2_dataset_binding_v3", "marker": "immutable"}
    split = {
        "train_start": "2018-08-01",
        "train_end": "2022-12-31",
        "valid_start": "2023-01-01",
        "valid_end": "2024-06-30",
        "test_start": "2024-07-01",
        "test_end": "2026-08-31",
        "backtest_end": "2026-08-28",
    }
    custom = {
        "_qe_direct_v2_dataset_binding": binding,
        "stock_pool": "index_pool__csi300",
    }
    custom_cfg = build_config_from_custom_evo_loop(
        {
            "factor_keys": ["factor_a||catalog"],
            "model_id": "model_a",
            "strategy_params": {"topk": 20},
            "runtime_flags": {"random_seed": 123},
            "label_horizon": 20,
            "data_split": split,
            "stock_pool": "index_pool__csi300",
            "custom_params": custom,
            "node_id": "rdagent-node1",
        },
        {"node_id": "wsl2-5080"},
    )
    assert custom_cfg.node_id == "rdagent-node1"
    assert custom_cfg.data_split == split
    assert custom_cfg.build_custom_params()["_qe_direct_v2_dataset_binding"] == binding

    strategy_cfg = build_config_from_strategy_evo_loop(
        {"factor_list": ["factor_a"], "model_id": "model_a", "data_split": {"test_end": "old"}},
        {
            "data_split": split,
            "stock_pool": "index_pool__csi300",
            "custom_params": custom,
            "node_id": "rdagent-node1",
            "strategy_params": {"topk": 20},
        },
        {"label_horizon": 20, "node_id": "wsl2-5080"},
    )
    assert strategy_cfg.node_id == "rdagent-node1"
    assert strategy_cfg.data_split == split
    assert strategy_cfg.build_custom_params()["_qe_direct_v2_dataset_binding"] == binding
