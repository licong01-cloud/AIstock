from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest
import yaml

from backend.services.dataset_release.canonical import digest_named_fields
from backend.services.dataset_release.shared_sector_context import (
    RELEASE_SW_L2_CODE_MAP_SCHEMA,
    RELEASE_SW_L2_MEMBER_BACKED_SCHEMA,
)
from backend.services.quantevolver.config_composer import (
    ConfigComposer,
    QE_DIRECT_V2_DATASET_BINDING_FILE,
    QE_UNIVERSE_COVERAGE_RECEIPT_FILE,
)
from backend.services.quantevolver.qe_active_dataset_profile import (
    QE_RUN_COVERAGE_RECEIPT_PARAM,
    QE_RUN_STOCK_POOL_CONTENT_PARAM,
)
from backend.services.quantevolver.qe_dataset_contract import (
    QE_DIRECT_V2_DATASET_BINDING_PARAM,
    QE_DIRECT_V2_INDEX_CODES,
    QEDirectV2DatasetBinding,
)
from backend.services.quantevolver.qe_sector_blacklist_policy import (
    QESectorBlacklistPolicyError,
    SECTOR_BLACKLIST_POLICY_PARAM,
    materialize_sector_blacklist_universe,
    requested_sector_codes,
)
from scripts.qe_build_frozen_suspend_filter import build_suspend_filter_payload
from backend.services.quantevolver.qe_validate_direct_v2_dataset import (
    DirectV2DatasetValidationError,
    validate_binding,
)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True), encoding="utf-8")


def _component(root: Path, name: str, *, freq: str, start: str) -> dict[str, str]:
    component = root / "components" / name
    (component / "instruments").mkdir(parents=True)
    (component / "calendars").mkdir(parents=True)
    instruments = component / "instruments" / "all.txt"
    calendar = component / "calendars" / f"{freq}.txt"
    meta = component / "meta_export.json"
    stock_payload = f"000001.SZ\t{start}\t2026-08-31\n"
    instruments.write_text(stock_payload, encoding="utf-8")
    calendar.write_text("2026-08-28\n2026-08-31\n", encoding="utf-8")
    meta_payload = {
        "snapshot_id": "daily_bin_candidate" if freq == "day" else "minute_bin_candidate",
        "start": start,
        "end": "2026-08-31",
        "universe_key": "aistock_equity_pit_canonical_v2",
        "rule_version": "shsz_a_252td_st_delist_asof_v2",
    }
    if freq == "day":
        benchmark_line = f"000300.SH\t{start}\t2026-08-31\n"
        (component / "instruments" / "stock_universe.txt").write_text(stock_payload, encoding="utf-8")
        (component / "instruments" / "benchmark.txt").write_text(benchmark_line, encoding="utf-8")
        instruments.write_text(stock_payload + benchmark_line, encoding="utf-8")
        meta_payload["benchmark_only"] = {
            "schema_version": "qe_direct_daily_benchmark_v1",
            "code": "000300.SH",
            "start": start,
            "end": "2026-08-31",
            "rows": 2,
            "fields": ["open", "high", "low", "close", "volume", "amount"],
            "source": "components/index_context/index_daily.h5",
            "provider_catalog": "instruments/all.txt",
            "selection_universe": "instruments/stock_universe.txt",
            "benchmark_universe": "instruments/benchmark.txt",
            "selection_eligible": False,
        }
    _write_json(meta, meta_payload)
    return {
        "snapshot_id": "daily_bin_candidate" if freq == "day" else "minute_bin_candidate",
        "universe_key": "aistock_equity_pit_canonical_v2",
        "rule_version": "shsz_a_252td_st_delist_asof_v2",
        "instruments_sha256": _sha(instruments),
        "calendar_sha256": _sha(calendar),
        "meta_export_sha256": _sha(meta),
    }


def _fixture(tmp_path: Path) -> tuple[dict, Path]:
    root = tmp_path / "20260831-qe-hmm-v2-candidate"
    day_pins = _component(root, "daily_bin_candidate", freq="day", start="2018-08-01")
    minute_pins = _component(root, "minute_bin_candidate", freq="1min", start="2024-01-02")

    factor = root / "components" / "factor_h5_static_candidate_v2"
    factor.mkdir(parents=True)
    factor_meta = {
        "schema_version": "qe_direct_factor_h5_static_v2",
        "start": "2018-08-01",
        "end": "2026-08-31",
        "universe_key": "aistock_equity_pit_canonical_v2",
    }
    _write_json(factor / "meta.json", factor_meta)

    index_path = root / "components" / "index_context" / "index_daily.h5"
    index_path.parent.mkdir(parents=True)
    index_frame = pd.DataFrame(
        {"close": range(len(QE_DIRECT_V2_INDEX_CODES))},
        index=pd.MultiIndex.from_product(
            [[pd.Timestamp("2026-08-31")], QE_DIRECT_V2_INDEX_CODES],
            names=["trade_date", "ts_code"],
        ),
    )
    index_frame.to_hdf(index_path, key="index_daily")

    suspend = root / "components" / "suspend_d_daily_candidate_v2"
    suspend.mkdir(parents=True)
    suspend_frame = pd.DataFrame([{"trade_date": "2026-08-28", "ts_code": "000001.SZ", "suspend_type": "S"}])
    suspend_frame.to_parquet(suspend / "suspend_d.parquet", index=False)
    suspend_meta = {
        "schema_version": "qe_direct_suspend_d_v1",
        "component": "suspend_d",
        "start": "2018-08-01",
        "end": "2026-08-31",
        "universe_key": "aistock_equity_pit_canonical_v2",
        "source_table": "market.suspend_d",
        "suspend_type": "S",
        "daily_row_counts": {"2026-08-28": 1, "2026-08-31": 0},
    }
    _write_json(suspend / "meta.json", suspend_meta)

    posix_root = "/mnt/x/AIstock_dataset_candidates/20260831-qe-hmm-v2-candidate"
    binding = {
        "schema_version": "qe_direct_v2_dataset_binding_v2",
        "release_id": "qe_hmm_full_v2_20260831",
        "cutoff": "2026-08-31",
        "candidate_root": posix_root,
        "provider_uri_day": f"{posix_root}/components/daily_bin_candidate",
        "provider_uri_1min": f"{posix_root}/components/minute_bin_candidate",
        "factor_data_dir": f"{posix_root}/components/factor_h5_static_candidate_v2",
        "index_context_path": f"{posix_root}/components/index_context/index_daily.h5",
        "suspend_data_dir": f"{posix_root}/components/suspend_d_daily_candidate_v2",
        "factor_meta": factor_meta,
        "factor_meta_sha256": _sha(factor / "meta.json"),
        "day_pins": day_pins,
        "minute_pins": minute_pins,
        "selection_pins": {
            "stock_pool": "stock_universe",
            "instruments_sha256": _sha(
                root / "components" / "daily_bin_candidate" / "instruments" / "stock_universe.txt"
            ),
            "benchmark_code": "000300.SH",
            "benchmark_instruments_sha256": _sha(
                root / "components" / "daily_bin_candidate" / "instruments" / "benchmark.txt"
            ),
        },
        "index_pins": {
            "sha256": _sha(index_path),
            "max_date": "2026-08-31",
            "codes": list(QE_DIRECT_V2_INDEX_CODES),
        },
        "suspend_pins": {
            "dataset_id": "suspend_d_daily_candidate_v2",
            "schema_version": "qe_direct_suspend_d_v1",
            "source_contract": "market.suspend_d",
            "metadata_sha256": _sha(suspend / "meta.json"),
            "parquet_sha256": _sha(suspend / "suspend_d.parquet"),
        },
    }
    return binding, root


def _with_local_paths(binding: dict, root: Path) -> dict:
    value = json.loads(json.dumps(binding))
    local_root = root.as_posix()
    value.update(
        {
            "candidate_root": local_root,
            "provider_uri_day": f"{local_root}/components/daily_bin_candidate",
            "provider_uri_1min": f"{local_root}/components/minute_bin_candidate",
            "factor_data_dir": f"{local_root}/components/factor_h5_static_candidate_v2",
            "index_context_path": f"{local_root}/components/index_context/index_daily.h5",
            "suspend_data_dir": f"{local_root}/components/suspend_d_daily_candidate_v2",
        }
    )
    return value


def _custom_params(binding: dict) -> dict:
    return {
        QE_DIRECT_V2_DATASET_BINDING_PARAM: binding,
        "disable_alpha158": True,
        "label_horizon": 20,
        "risk_policy": {
            "enabled": True,
            "providers": ["st_pit"],
            "hard_actions": ["block_buy", "force_exit"],
            "policy_version": "stock_event_risk_policy_v1",
            "visible_time_mode": "next_trading_session",
            "strict_data_ready": True,
        },
    }


def test_direct_v2_binding_roundtrip_and_rejects_cross_release_path(tmp_path: Path) -> None:
    raw, _ = _fixture(tmp_path)
    binding = QEDirectV2DatasetBinding.from_mapping(raw)
    assert binding.as_dict() == raw

    wrong = dict(raw)
    wrong["provider_uri_1min"] = "/home/lc999/data/qlib_minute_bin"
    with pytest.raises(ValueError, match="outside the selected candidate release"):
        QEDirectV2DatasetBinding.from_mapping(wrong)

    bad_codes = json.loads(json.dumps(raw))
    bad_codes["index_pins"]["codes"] = bad_codes["index_pins"]["codes"][:-1]
    with pytest.raises(ValueError, match="index code contract differs"):
        QEDirectV2DatasetBinding.from_mapping(bad_codes)


def test_direct_v2_fresh_process_validator_detects_hash_drift(tmp_path: Path) -> None:
    raw, root = _fixture(tmp_path)
    raw = _with_local_paths(raw, root)
    binding_file = tmp_path / "qe_direct_v2_dataset_binding.json"
    _write_json(binding_file, raw)
    assert validate_binding(binding_file)["release_id"] == "qe_hmm_full_v2_20260831"

    (root / "components" / "minute_bin_candidate" / "calendars" / "1min.txt").write_text(
        "2026-08-31\n", encoding="utf-8"
    )
    with pytest.raises(RuntimeError, match="qe_direct_v2_hash_mismatch"):
        validate_binding(binding_file)


def test_direct_v2_composer_uses_only_bound_paths_and_direct_suspend_meta(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    raw, _ = _fixture(tmp_path)
    binding = QEDirectV2DatasetBinding.from_mapping(raw)
    params = _custom_params(raw)
    params["stock_pool"] = "all"
    split = {
        "train_start": "2018-08-01",
        "train_end": "2023-10-27",
        "valid_start": "2023-11-28",
        "valid_end": "2024-05-29",
        "test_start": "2026-08-28",
        "test_end": "2026-08-31",
        "backtest_start": "2026-08-28",
        "backtest_end": "2026-08-31",
    }
    composer = ConfigComposer()
    monkeypatch.setattr(
        composer,
        "_fetch_workspace_config",
        lambda *_args, **_kwargs: {
            "workspace_base": "/tmp/qe_workspace",
            "qlib_data_path": "/home/lc999/data/qlib_bin",
            "qlib_minute_path": "/home/lc999/data/qlib_minute_bin",
            "factor_data_dir": "/home/lc999/data/factor_data",
        },
    )
    monkeypatch.setattr(
        composer,
        "_get_factors_info",
        lambda *_args, **_kwargs: [
            {
                "factor_name": "DemoFactor",
                "source": "custom",
                "code_text": "def calculate_DemoFactor(instruments, start_date, end_date):\n    return None\n",
            }
        ],
    )
    monkeypatch.setattr(composer, "_get_model_info", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(composer, "_get_strategy_info", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(composer, "_get_read_exp_res_content", lambda: "# read")
    monkeypatch.setattr(
        composer,
        "_prepare_suspend_filter_runtime",
        lambda **kwargs: (kwargs["custom_params"], None),
    )

    result = composer.compose_experiment_in_memory(
        factor_names=["DemoFactor"],
        model_id=None,
        strategy_id=None,
        data_split=split,
        custom_params=params,
        experiment_name="direct-v2-test",
        skip_db_save=True,
        execution_algo="TWAP",
        execution_algo_params={},
        node_id="wsl2-5080",
    )

    assert json.loads(result["experiment_files"][QE_DIRECT_V2_DATASET_BINDING_FILE]) == raw
    assert result["direct_v2_dataset_binding"] == raw
    conf = result["experiment_files"]["conf.yaml"]
    assert binding.provider_uri_day in conf
    assert binding.provider_uri_1min in conf
    assert "market: &market stock_universe" in conf
    assert "market: &market all" not in conf
    parsed_conf = yaml.safe_load(conf)
    assert parsed_conf["market"] == "stock_universe"
    assert parsed_conf["port_analysis_config"]["backtest"]["exchange_kwargs"]["codes"] == "all"
    prepare = result["experiment_files"]["prepare_factors.py"]
    assert repr(dict(binding.factor_meta)) in prepare
    assert binding.factor_meta_sha256 in prepare
    risk_spec = json.loads(result["experiment_files"]["qe_frozen_build_spec.json"])
    assert risk_spec["dataset"]["contract_id"] == binding.release_id
    assert risk_spec["pins"] == {
        **dict(binding.day_pins),
        "instruments_file": "stock_universe.txt",
        "instruments_sha256": binding.selection_pins["instruments_sha256"],
    }
    assert risk_spec["suspend"]["provider_uri"] == binding.suspend_data_dir
    assert risk_spec["suspend"]["metadata_name"] == "meta.json"
    assert "python qe_validate_direct_v2_dataset.py" in result["wsl_command_core"]
    assert f"export QE_INDEX_CONTEXT_PATH={binding.index_context_path}" in result["wsl_command_core"]
    serialized = json.dumps(result, ensure_ascii=False, default=str)
    assert "/home/lc999/data/qlib_bin" not in serialized
    assert "/home/lc999/data/qlib_minute_bin" not in serialized
    assert "/home/lc999/data/factor_data" not in serialized

    local_spec = json.loads(json.dumps(risk_spec))
    local_spec["provider_uri_day"] = str(
        tmp_path / "20260831-qe-hmm-v2-candidate" / "components" / "daily_bin_candidate"
    )
    local_spec["suspend"]["provider_uri"] = str(
        tmp_path / "20260831-qe-hmm-v2-candidate" / "components" / "suspend_d_daily_candidate_v2"
    )
    payload = build_suspend_filter_payload(local_spec)
    assert payload["suspended_row_count"] == 1
    assert payload["suspended_by_date"]["2026-08-28"] == ["000001.SZ"]
    assert payload["suspended_by_date"]["2026-08-31"] == []


def test_direct_v2_binding_rejects_benchmark_in_selection_universe(tmp_path: Path) -> None:
    raw, root = _fixture(tmp_path)
    raw = _with_local_paths(raw, root)
    binding_file = tmp_path / QE_DIRECT_V2_DATASET_BINDING_FILE
    stock_path = root / "components" / "daily_bin_candidate" / "instruments" / "stock_universe.txt"
    stock_path.write_text(
        stock_path.read_text(encoding="utf-8") + "000300.SH\t2018-08-01\t2026-08-31\n",
        encoding="utf-8",
    )
    raw["selection_pins"]["instruments_sha256"] = _sha(stock_path)
    _write_json(binding_file, raw)

    with pytest.raises(RuntimeError, match="qe_direct_v2_selection_contains_benchmark"):
        validate_binding(binding_file)


def test_direct_v2_binding_rejects_unbound_named_stock_pool(tmp_path: Path) -> None:
    raw, _ = _fixture(tmp_path)
    params = _custom_params(raw)
    params["stock_pool"] = "filtered_pool_20260831"

    composer = ConfigComposer.__new__(ConfigComposer)
    with pytest.raises(ValueError, match="qe_direct_v2_stock_pool_outside_binding"):
        composer._compose_conf_yaml(
            factors_info=[],
            model_info=None,
            strategy_info=None,
            data_split={
                "train_start": "2018-08-01",
                "train_end": "2023-10-27",
                "valid_start": "2023-11-28",
                "valid_end": "2024-05-31",
                "test_start": "2024-07-01",
                "test_end": "2024-12-31",
                "backtest_end": "2024-12-30",
            },
            custom_params=params,
            has_custom_factors=False,
            has_alpha158=False,
        )


def _v3_binding(raw: dict, *, sidecar: str, receipt: str) -> dict:
    value = json.loads(json.dumps(raw))
    value["schema_version"] = "qe_direct_v2_dataset_binding_v3"
    value["selection_pins"] = {
        "mode": "single_index",
        "pool_ids": ["csi300"],
        "instrument_name": "index_pool__csi300",
        "instruments_file": "index_pool__csi300.txt",
        "instruments_sha256": hashlib.sha256(sidecar.encode()).hexdigest(),
        "membership_revision": "csi300-pit-v1",
        "coverage_receipt_sha256": hashlib.sha256(receipt.encode()).hexdigest(),
        "benchmark_code": "000300.SH",
        "benchmark_instruments_sha256": raw["selection_pins"]["benchmark_instruments_sha256"],
    }
    return value


def _v3_receipt() -> str:
    return json.dumps(
        {
            "schema_version": "qe_index_pool_coverage_receipt_v1",
            "release_id": "qe_hmm_full_v2_20260831",
            "cutoff": "2026-08-31",
            "pools": {
                "csi300": {
                    "available_start": "2018-08-01",
                    "available_end": "2026-08-31",
                    "gaps": [],
                }
            },
        },
        sort_keys=True,
    )


def test_direct_v2_v3_validator_accepts_packaged_index_sidecar(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw, root = _fixture(tmp_path)
    sidecar = "000001.SZ\t2018-08-01\t2026-08-31\n"
    receipt = _v3_receipt()
    raw = _with_local_paths(_v3_binding(raw, sidecar=sidecar, receipt=receipt), root)
    _write_json(tmp_path / QE_DIRECT_V2_DATASET_BINDING_FILE, raw)
    (tmp_path / "index_pool__csi300.txt").write_bytes(sidecar.encode("utf-8"))
    (tmp_path / QE_UNIVERSE_COVERAGE_RECEIPT_FILE).write_bytes(receipt.encode("utf-8"))
    monkeypatch.chdir(tmp_path)

    validated = validate_binding(tmp_path / QE_DIRECT_V2_DATASET_BINDING_FILE)

    assert validated["selection_pins"]["mode"] == "single_index"


def test_direct_v2_v3_validator_rejects_non_iso_sidecar_dates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw, root = _fixture(tmp_path)
    sidecar = "000001.SZ\tnot-a-date\t2026-08-31\n"
    receipt = _v3_receipt()
    raw = _with_local_paths(_v3_binding(raw, sidecar=sidecar, receipt=receipt), root)
    _write_json(tmp_path / QE_DIRECT_V2_DATASET_BINDING_FILE, raw)
    (tmp_path / "index_pool__csi300.txt").write_bytes(sidecar.encode("utf-8"))
    (tmp_path / QE_UNIVERSE_COVERAGE_RECEIPT_FILE).write_bytes(receipt.encode("utf-8"))
    monkeypatch.chdir(tmp_path)

    with pytest.raises(DirectV2DatasetValidationError, match="qe_direct_v2_selection_contract_invalid"):
        validate_binding(tmp_path / QE_DIRECT_V2_DATASET_BINDING_FILE)


def test_direct_v2_v3_composer_builds_blacklist_filtered_stock_universe_overlay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw, _ = _fixture(tmp_path)
    sidecar = "000001.SZ\t2018-08-01\t2026-08-31\n"
    receipt = _v3_receipt()
    raw = _v3_binding(raw, sidecar=sidecar, receipt=receipt)
    raw["selection_pins"].update(
        {
            "mode": "stock_universe",
            "pool_ids": [],
            "instrument_name": "stock_universe",
            "instruments_file": "stock_universe.txt",
            "membership_revision": "stock-universe-pit-v2|sector-blacklist:test",
        }
    )
    params = _custom_params(raw)
    params.update(
        {
            "stock_pool": "stock_universe",
            QE_RUN_STOCK_POOL_CONTENT_PARAM: sidecar,
            QE_RUN_COVERAGE_RECEIPT_PARAM: receipt,
            SECTOR_BLACKLIST_POLICY_PARAM: {
                "schema_version": "qe_sector_blacklist_policy_v1",
                "requested": True,
                "enabled": True,
                "effective": True,
                "blacklist_excluded_count": 11,
            },
        }
    )
    composer = ConfigComposer()
    monkeypatch.setattr(
        composer,
        "_fetch_workspace_config",
        lambda *_args, **_kwargs: {"workspace_base": "/tmp/qe_workspace"},
    )
    monkeypatch.setattr(
        composer,
        "_get_factors_info",
        lambda *_args, **_kwargs: [
            {
                "factor_name": "DemoFactor",
                "source": "custom",
                "code_text": "def calculate_DemoFactor(instruments, start_date, end_date):\n    return None\n",
            }
        ],
    )
    monkeypatch.setattr(composer, "_get_model_info", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(composer, "_get_strategy_info", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(composer, "_get_read_exp_res_content", lambda: "# read")
    monkeypatch.setattr(
        composer,
        "_prepare_suspend_filter_runtime",
        lambda **kwargs: (kwargs["custom_params"], None),
    )

    result = composer.compose_experiment_in_memory(
        factor_names=["DemoFactor"],
        data_split={
            "train_start": "2018-08-01",
            "train_end": "2023-10-27",
            "valid_start": "2023-11-28",
            "valid_end": "2024-05-29",
            "test_start": "2026-08-28",
            "test_end": "2026-08-31",
            "backtest_end": "2026-08-28",
        },
        custom_params=params,
        experiment_name="direct-v3-test",
        skip_db_save=True,
        execution_algo="TWAP",
        node_id="wsl2-5080",
    )

    files = result["experiment_files"]
    assert files["stock_universe.txt"] == sidecar
    assert files[QE_UNIVERSE_COVERAGE_RECEIPT_FILE] == receipt
    conf = yaml.safe_load(files["conf.yaml"])
    assert conf["qlib_init"]["provider_uri"]["day"] == "/tmp/qe_workspace/direct-v3-test/qe_provider_day"
    assert conf["qlib_init"]["provider_uri"]["1min"] == raw["provider_uri_1min"]
    assert conf["market"] == "stock_universe"
    assert SECTOR_BLACKLIST_POLICY_PARAM not in files["conf.yaml"]
    command = result["wsl_command_core"]
    assert "ln -sfn" in command
    assert "cp -f stock_universe.txt" in command
    assert raw["provider_uri_day"] in command
    risk_spec = json.loads(files["qe_frozen_build_spec.json"])
    assert risk_spec["provider_uri_day"].endswith("/qe_provider_day")
    assert risk_spec["pins"]["instruments_file"] == "stock_universe.txt"


BLACKLIST_CALENDAR = [
    dt.date(2026, 8, 3),
    dt.date(2026, 8, 4),
    dt.date(2026, 8, 5),
    dt.date(2026, 8, 6),
]


def _blacklist_frozen_inputs(
    tmp_path: Path,
    *,
    unknown: bool = False,
    omit_last: bool = False,
    reverse_rows: bool = False,
) -> dict[str, object]:
    code_map: dict[str, object] = {
        "schema_version": "qe_sw_l2_code_map_v1",
        "ordered_codes": ["801010.SI", "801020.SI"],
    }
    code_map["code_map_digest"] = digest_named_fields(
        "dataset_release_sw_l2_code_map_v1",
        {"ordered_codes": code_map["ordered_codes"]},
    )
    map_path = tmp_path / "sector_code_map.json"
    map_path.write_text(json.dumps(code_map, sort_keys=True), encoding="utf-8")
    rows = [
        {
            "instrument": "000001.SZ",
            "start_date": "2026-08-03",
            "end_date": "2026-08-04",
            "l2_code_id": 0,
        },
        {
            "instrument": "000001.SZ",
            "start_date": "2026-08-05",
            "end_date": "2026-08-06",
            "l2_code_id": -1 if unknown else 1,
        },
        {
            "instrument": "000002.SZ",
            "start_date": "2026-08-03",
            "end_date": "2026-08-05" if omit_last else "2026-08-06",
            "l2_code_id": 1,
        },
    ]
    if reverse_rows:
        rows.reverse()
    membership_path = tmp_path / "sector_membership_spans.parquet"
    pd.DataFrame(rows).to_parquet(membership_path, index=False)
    return {
        "schema_version": "qe_sector_policy_input_v1",
        "membership_file": membership_path.name,
        "membership_sha256": _sha(membership_path),
        "code_map_file": map_path.name,
        "code_map_sha256": _sha(map_path),
        "start": BLACKLIST_CALENDAR[0].isoformat(),
        "end": BLACKLIST_CALENDAR[-1].isoformat(),
        "universe_key": "aistock_equity_pit_canonical_v2",
    }


def test_materialize_sector_blacklist_preserves_pit_transitions(tmp_path: Path) -> None:
    result = materialize_sector_blacklist_universe(
        base_intervals=[
            ("000001.SZ", BLACKLIST_CALENDAR[0], BLACKLIST_CALENDAR[-1]),
            ("000002.SZ", BLACKLIST_CALENDAR[0], BLACKLIST_CALENDAR[-1]),
        ],
        calendar=BLACKLIST_CALENDAR,
        window_start=BLACKLIST_CALENDAR[0],
        window_end=BLACKLIST_CALENDAR[-1],
        factor_root=tmp_path,
        pins=_blacklist_frozen_inputs(tmp_path),
        blacklist_codes=["801010.SI"],
    )

    assert result.instruments_content == ("000001.SZ\t2026-08-05\t2026-08-06\n000002.SZ\t2026-08-03\t2026-08-06\n")
    assert result.diagnostics["blacklist_excluded_count"] == 1
    assert result.diagnostics["blacklist_excluded_membership_days"] == 2
    assert result.diagnostics["effective"] is True


def test_materialize_sector_blacklist_preserves_pre_policy_intervals_without_membership(
    tmp_path: Path,
) -> None:
    pins = _blacklist_frozen_inputs(tmp_path)
    membership_path = tmp_path / str(pins["membership_file"])
    membership = pd.read_parquet(membership_path)
    membership = membership[membership["end_date"] >= "2026-08-05"].copy()
    membership.loc[membership["start_date"] < "2026-08-05", "start_date"] = "2026-08-05"
    membership = pd.concat(
        [
            membership,
            pd.DataFrame(
                [
                    {
                        "instrument": "000004.SZ",
                        "start_date": "2026-08-05",
                        "end_date": "2026-08-06",
                        "l2_code_id": 0,
                    }
                ]
            ),
        ],
        ignore_index=True,
    ).sort_values(["instrument", "start_date"], ignore_index=True)
    membership.to_parquet(membership_path, index=False)
    pins["membership_sha256"] = _sha(membership_path)
    pins["start"] = "2026-08-05"

    result = materialize_sector_blacklist_universe(
        base_intervals=[
            ("000001.SZ", BLACKLIST_CALENDAR[0], BLACKLIST_CALENDAR[-1]),
            ("000002.SZ", BLACKLIST_CALENDAR[0], BLACKLIST_CALENDAR[-1]),
            ("000003.SZ", BLACKLIST_CALENDAR[0], dt.date(2026, 8, 4)),
            ("000004.SZ", BLACKLIST_CALENDAR[0], BLACKLIST_CALENDAR[-1]),
        ],
        calendar=BLACKLIST_CALENDAR,
        window_start=BLACKLIST_CALENDAR[0],
        policy_start=dt.date(2026, 8, 5),
        window_end=BLACKLIST_CALENDAR[-1],
        factor_root=tmp_path,
        pins=pins,
        blacklist_codes=["801020.SI"],
    )

    assert result.instruments_content == (
        "000001.SZ\t2026-08-03\t2026-08-04\n"
        "000002.SZ\t2026-08-03\t2026-08-04\n"
        "000003.SZ\t2026-08-03\t2026-08-04\n"
        "000004.SZ\t2026-08-03\t2026-08-06\n"
    )
    assert result.diagnostics["window_start"] == "2026-08-03"
    assert result.diagnostics["policy_start"] == "2026-08-05"
    assert result.diagnostics["blacklist_excluded_count"] == 2


def test_materialize_sector_blacklist_rejects_empty_policy_universe_despite_training_rows(
    tmp_path: Path,
) -> None:
    pins = _blacklist_frozen_inputs(tmp_path)
    membership_path = tmp_path / str(pins["membership_file"])
    membership = pd.read_parquet(membership_path)
    membership = membership[membership["instrument"] == "000002.SZ"].copy()
    membership["start_date"] = "2026-08-05"
    membership.to_parquet(membership_path, index=False)
    pins["membership_sha256"] = _sha(membership_path)
    pins["start"] = "2026-08-05"

    with pytest.raises(QESectorBlacklistPolicyError, match="qe_sector_blacklist_universe_empty"):
        materialize_sector_blacklist_universe(
            base_intervals=[
                ("000002.SZ", BLACKLIST_CALENDAR[0], BLACKLIST_CALENDAR[-1]),
                ("000003.SZ", BLACKLIST_CALENDAR[0], dt.date(2026, 8, 4)),
            ],
            calendar=BLACKLIST_CALENDAR,
            window_start=BLACKLIST_CALENDAR[0],
            policy_start=dt.date(2026, 8, 5),
            window_end=BLACKLIST_CALENDAR[-1],
            factor_root=tmp_path,
            pins=pins,
            blacklist_codes=["801020.SI"],
        )


def test_materialize_sector_blacklist_accepts_shared_sparse_release_ids(tmp_path: Path) -> None:
    pins = _blacklist_frozen_inputs(tmp_path)
    authority = {"authority_id": "fixture", "authority_sha256": "a" * 64}
    entries = [{"l2_code_id": index * 2 + 1, "canonical_l2_code": f"801{index:03d}.SI"} for index in range(131)]
    codes = sorted(row["canonical_l2_code"] for row in entries)
    code_map = {
        "schema_version": RELEASE_SW_L2_CODE_MAP_SCHEMA,
        "mapping_authority": authority,
        "entries": entries,
        "member_backed_codes": codes,
        "code_map_digest": digest_named_fields(
            RELEASE_SW_L2_CODE_MAP_SCHEMA,
            {"mapping_authority": authority, "entries": entries},
        ),
        "member_backed_digest": digest_named_fields(
            RELEASE_SW_L2_MEMBER_BACKED_SCHEMA,
            {"mapping_authority": authority, "member_backed_codes": codes},
        ),
    }
    map_path = tmp_path / "sector_code_map.json"
    map_path.write_text(json.dumps(code_map), encoding="utf-8")
    pins["code_map_sha256"] = _sha(map_path)
    membership_path = tmp_path / "sector_membership_spans.parquet"
    membership = pd.read_parquet(membership_path)
    membership["l2_code_id"] = membership["l2_code_id"].map({0: 21, 1: 41})
    membership.to_parquet(membership_path, index=False)
    pins["membership_sha256"] = _sha(membership_path)

    result = materialize_sector_blacklist_universe(
        base_intervals=[
            ("000001.SZ", BLACKLIST_CALENDAR[0], BLACKLIST_CALENDAR[-1]),
            ("000002.SZ", BLACKLIST_CALENDAR[0], BLACKLIST_CALENDAR[-1]),
        ],
        calendar=BLACKLIST_CALENDAR,
        window_start=BLACKLIST_CALENDAR[0],
        window_end=BLACKLIST_CALENDAR[-1],
        factor_root=tmp_path,
        pins=pins,
        blacklist_codes=["801010.SI"],
    )

    assert result.diagnostics["blacklist_excluded_count"] == 1
    assert result.diagnostics["code_map_digest"] == code_map["code_map_digest"]


@pytest.mark.parametrize(
    ("fixture_kwargs", "reason_code"),
    [
        ({"unknown": True}, "qe_sector_blacklist_membership_unknown"),
        ({"omit_last": True}, "qe_sector_blacklist_membership_incomplete"),
    ],
)
def test_materialize_sector_blacklist_fails_closed_on_membership_gaps(
    tmp_path: Path,
    fixture_kwargs: dict[str, bool],
    reason_code: str,
) -> None:
    pins = _blacklist_frozen_inputs(tmp_path, **fixture_kwargs)
    with pytest.raises(QESectorBlacklistPolicyError, match=reason_code):
        materialize_sector_blacklist_universe(
            base_intervals=[
                ("000001.SZ", BLACKLIST_CALENDAR[0], BLACKLIST_CALENDAR[-1]),
                ("000002.SZ", BLACKLIST_CALENDAR[0], BLACKLIST_CALENDAR[-1]),
            ],
            calendar=BLACKLIST_CALENDAR,
            window_start=BLACKLIST_CALENDAR[0],
            window_end=BLACKLIST_CALENDAR[-1],
            factor_root=tmp_path,
            pins=pins,
            blacklist_codes=["801010.SI"],
        )


def test_materialize_sector_blacklist_rejects_hash_drift(tmp_path: Path) -> None:
    pins = _blacklist_frozen_inputs(tmp_path)
    (tmp_path / "sector_code_map.json").write_text("{}", encoding="utf-8")
    with pytest.raises(
        QESectorBlacklistPolicyError,
        match="qe_sector_blacklist_frozen_mapping_hash_mismatch",
    ):
        materialize_sector_blacklist_universe(
            base_intervals=[("000001.SZ", BLACKLIST_CALENDAR[0], BLACKLIST_CALENDAR[-1])],
            calendar=BLACKLIST_CALENDAR,
            window_start=BLACKLIST_CALENDAR[0],
            window_end=BLACKLIST_CALENDAR[-1],
            factor_root=tmp_path,
            pins=pins,
            blacklist_codes=["801010.SI"],
        )


def test_sector_blacklist_request_is_canonical_and_snapshot_must_match() -> None:
    assert requested_sector_codes({"sector_blacklist": ["801020.si", "801010.SI"]}) == ("801010.SI", "801020.SI")
    with pytest.raises(QESectorBlacklistPolicyError, match="snapshot differs"):
        requested_sector_codes(
            {
                "sector_blacklist": ["801010.SI"],
                "sector_blacklist_snapshot": {"items": [{"sw2_code": "801020.SI"}]},
            }
        )

    with pytest.raises(QESectorBlacklistPolicyError, match="requires at least one"):
        requested_sector_codes({"sector_blacklist_enabled": True, "sector_blacklist": []})


def test_materialize_sector_blacklist_rejects_noncanonical_membership_order(
    tmp_path: Path,
) -> None:
    pins = _blacklist_frozen_inputs(tmp_path, reverse_rows=True)
    with pytest.raises(
        QESectorBlacklistPolicyError,
        match="canonical instrument/date order",
    ):
        materialize_sector_blacklist_universe(
            base_intervals=[("000001.SZ", BLACKLIST_CALENDAR[0], BLACKLIST_CALENDAR[-1])],
            calendar=BLACKLIST_CALENDAR,
            window_start=BLACKLIST_CALENDAR[0],
            window_end=BLACKLIST_CALENDAR[-1],
            factor_root=tmp_path,
            pins=pins,
            blacklist_codes=["801010.SI"],
        )
