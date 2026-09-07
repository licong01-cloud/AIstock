from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from backend.services.quantevolver import config_composer as composer_module
from backend.services.quantevolver.qe_active_dataset_profile import (
    ACTIVE_PROFILE_ENV,
    QEActiveDatasetProfileError,
    get_qe_dataset_profile_summary,
    load_active_qe_profile,
    reject_client_dataset_internals,
    resolve_active_qe_dataset,
)
from backend.services.quantevolver.qe_dataset_contract import QE_DIRECT_V2_INDEX_CODES
from backend.services.quantevolver.experiment_config_builders import (
    build_config_from_custom_evo_loop,
    build_config_from_strategy_evo_loop,
)
from scripts.qe_active_dataset_profile import _activate, _sha256, _validate


def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _write(path: Path, payload: bytes) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return _sha(payload)


def _canonical(value: dict) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _fixture_profile(tmp_path: Path, *, with_gap: bool = False) -> Path:
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
    index_sha = _write(
        candidate / "components" / "index_context" / "index_daily.h5",
        b"index-fixture",
    )
    suspend_meta_sha = _write(
        candidate / "components" / "suspend_d_daily_candidate_v2" / "meta.json",
        b"suspend-meta",
    )
    suspend_parquet_sha = _write(
        candidate / "components" / "suspend_d_daily_candidate_v2" / "suspend_d.parquet",
        b"suspend-parquet",
    )
    profile = {
        "schema_version": "aistock_active_dataset_profile_v1",
        "generation": "20260906-v1",
        "release_id": "qe-hmm-v2-20260831",
        "cutoff": "2026-08-31",
        "controller_paths": {
            "candidate_root": str(candidate),
            "stock_pool_root": str(pool_root),
            "coverage_receipt_path": str(receipt_path),
        },
        "components": {
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
                "source_contract": "tushare_suspend_d_shsz_S_v1",
                "metadata_sha256": suspend_meta_sha,
                "parquet_sha256": suspend_parquet_sha,
            },
        },
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
        reject_client_dataset_internals(
            {"_qe_direct_v2_dataset_binding": {"provider_uri_day": "/client/path"}}
        )


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


def test_unknown_node_sidecar_tamper_and_coverage_gap_fail_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
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
    minute_calendar = (
        tmp_path
        / "candidate"
        / "components"
        / "minute_bin_candidate"
        / "calendars"
        / "1min.txt"
    )
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
