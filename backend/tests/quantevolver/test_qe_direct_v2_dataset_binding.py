from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from backend.services.quantevolver.config_composer import (
    ConfigComposer,
    QE_DIRECT_V2_DATASET_BINDING_FILE,
)
from backend.services.quantevolver.qe_dataset_contract import (
    QE_DIRECT_V2_DATASET_BINDING_PARAM,
    QE_DIRECT_V2_INDEX_CODES,
    QEDirectV2DatasetBinding,
)
from scripts.qe_build_frozen_suspend_filter import build_suspend_filter_payload
from backend.services.quantevolver.qe_validate_direct_v2_dataset import validate_binding


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
    instruments.write_text(f"000001.SZ\t{start}\t2026-08-31\n", encoding="utf-8")
    calendar.write_text("2026-08-28\n2026-08-31\n", encoding="utf-8")
    _write_json(
        meta,
        {
            "snapshot_id": "daily_bin_candidate" if freq == "day" else "minute_bin_candidate",
            "start": start,
            "end": "2026-08-31",
            "universe_key": "aistock_equity_pit_canonical_v2",
            "rule_version": "shsz_a_252td_st_delist_asof_v2",
        },
    )
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
    suspend_frame = pd.DataFrame(
        [{"trade_date": "2026-08-28", "ts_code": "000001.SZ", "suspend_type": "S"}]
    )
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
        "schema_version": "qe_direct_v2_dataset_binding_v1",
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
    prepare = result["experiment_files"]["prepare_factors.py"]
    assert repr(dict(binding.factor_meta)) in prepare
    assert binding.factor_meta_sha256 in prepare
    risk_spec = json.loads(result["experiment_files"]["qe_frozen_build_spec.json"])
    assert risk_spec["dataset"]["contract_id"] == binding.release_id
    assert risk_spec["pins"] == dict(binding.day_pins)
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
        tmp_path
        / "20260831-qe-hmm-v2-candidate"
        / "components"
        / "suspend_d_daily_candidate_v2"
    )
    payload = build_suspend_filter_payload(local_spec)
    assert payload["suspended_row_count"] == 1
    assert payload["suspended_by_date"]["2026-08-28"] == ["000001.SZ"]
    assert payload["suspended_by_date"]["2026-08-31"] == []
