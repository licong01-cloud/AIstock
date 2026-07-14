from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from backend.data_service.moneyflow_contract import MONEYFLOW_UNIT_CONTRACT_VERSION
from backend.services.quantevolver.config_composer import ConfigComposer
from backend.services.quantevolver.config_composer import RDAGENT_DEFAULT_DATA_SPLIT
from backend.services.quantevolver.qe_dataset_contract import QE_ST_PIT_UNIVERSE_KEY


def _patch_universe(monkeypatch) -> None:
    monkeypatch.setattr(
        ConfigComposer,
        "_resolve_factor_cache_universe_metadata",
        lambda self, *, start_date, end_date: {
            "data_freshness_profile": "qe_backtest_coverage",
            "universe_key": QE_ST_PIT_UNIVERSE_KEY,
            "universe_rule_version": "rule_v1",
            "universe_fingerprint_sha256": "fp-test",
            "index_policy": "st_pit_buy_eligible_reindexed_v1",
            "coverage_semantics": "st_pit_buy_eligible_suspend_excluded_non_warmup_v1",
        },
    )


def _script(monkeypatch) -> str:
    _patch_universe(monkeypatch)
    code = "def calculate_DemoFactor(instruments, start_date, end_date):\n    return None\n"
    script = ConfigComposer()._compose_prepare_factors(
        [{"factor_name": "DemoFactor", "code_text": code}],
        factor_data_dir="/tmp/factor_data",
        data_split=dict(RDAGENT_DEFAULT_DATA_SPLIT),
    )
    assert script is not None
    compile(script, "prepare_factors.py", "exec")
    return script


def test_prepare_factors_generates_official_cache_hit_contract(monkeypatch) -> None:
    script = _script(monkeypatch)

    assert "official_factor_cache_hit_validation_v1" in script
    assert "def _validate_official_cache_hit_contract" in script
    assert "hash_mismatch" in script
    assert "window_not_covered" in script
    assert "non_official_cache_root" in script
    assert "cache contract" in script
    assert "with _exclusive_cache_lock(lock_name)" in script
    assert "_publish_parquet_atomic(cache_path, save_df)" in script
    assert "with _exclusive_cache_lock('global_meta')" in script
    assert "os.replace(tmp_path, cache_path)" in script
    assert "_validate_factor_data_dataset_contract()" in script


def test_prepare_factors_rejects_mounted_dataset_identity_mismatch(monkeypatch, tmp_path: Path) -> None:
    script = _script(monkeypatch)
    ns: dict[str, object] = {}
    exec(script, ns)
    ns["FACTOR_DATA_DIR"] = str(tmp_path)
    expected = dict(ns["QE_DATASET_EXPECTED_META"])
    (tmp_path / "meta.json").write_text(json.dumps(expected), encoding="utf-8")

    ns["_validate_factor_data_dataset_contract"]()
    expected["snapshot_id"] = "wrong_snapshot"
    (tmp_path / "meta.json").write_text(json.dumps(expected), encoding="utf-8")

    with pytest.raises(RuntimeError, match="dataset contract mismatch"):
        ns["_validate_factor_data_dataset_contract"]()


def test_prepare_factors_atomic_writer_publishes_valid_parquet_and_metadata(monkeypatch, tmp_path: Path) -> None:
    script = _script(monkeypatch)
    cache_root = tmp_path / "factor_values"
    single_dir = cache_root / "single"
    single_dir.mkdir(parents=True)
    ns: dict[str, object] = {}
    exec(script, ns)
    ns["FACTOR_CACHE_SINGLE_DIR"] = str(single_dir)
    ns["FACTOR_CACHE_META"] = str(cache_root / "_meta.json")
    code = "def calculate_DemoFactor(instruments, start_date, end_date):\n    return None\n"
    idx = pd.MultiIndex.from_product(
        [[pd.Timestamp("2018-08-01"), pd.Timestamp("2026-06-30")], ["000001.SZ"]],
        names=["datetime", "instrument"],
    )
    frame = pd.DataFrame({"DemoFactor": [1.0, 2.0]}, index=idx)

    with ns["_exclusive_cache_lock"]("factor:DemoFactor"):
        ns["_write_cache"]("DemoFactor", code, frame)

    cache_path = single_dir / "DemoFactor.parquet"
    assert cache_path.read_bytes()[:4] == b"PAR1"
    assert pd.read_parquet(cache_path).shape == (2, 1)
    meta = json.loads((cache_root / "_meta.json").read_text(encoding="utf-8"))
    assert meta["factors"]["DemoFactor"]["universe_key"] == QE_ST_PIT_UNIVERSE_KEY
    assert not list(cache_root.rglob("*.tmp"))


def test_prepare_factors_contract_accepts_official_cache_subwindow(monkeypatch, tmp_path: Path) -> None:
    script = _script(monkeypatch)
    cache_root = tmp_path / "factor_values"
    single_dir = cache_root / "single"
    single_dir.mkdir(parents=True)
    code = "def calculate_DemoFactor(instruments, start_date, end_date):\n    return None\n"
    code_hash = hashlib.sha256(code.encode()).hexdigest()[:16]
    idx = pd.MultiIndex.from_product(
        [[pd.Timestamp("2018-08-01"), pd.Timestamp("2026-06-30")], ["000001.SZ"]],
        names=["datetime", "instrument"],
    )
    pd.DataFrame({"value": [1.0, 2.0]}, index=idx).to_parquet(single_dir / "DemoFactor.parquet")
    (cache_root / "_meta.json").write_text(
        json.dumps(
            {
                "source_system": "official_offline_backtest_factor_data",
                "as_of_date": "2026-06-30",
                "moneyflow_unit_contract_version": MONEYFLOW_UNIT_CONTRACT_VERSION,
                "factors": {
                    "DemoFactor": {
                        "source_hash_raw": code_hash,
                        "date_range": "2018-08-01~2026-06-30",
                        "as_of_date": "2026-06-30",
                        "window_train_start": "2018-08-01",
                        "window_backtest_end": "2026-06-30",
                        "data_source_mode": "official_offline_backtest_factor_data",
                        "universe_key": QE_ST_PIT_UNIVERSE_KEY,
                        "universe_fingerprint_sha256": "fp-test",
                        "index_policy": "st_pit_buy_eligible_reindexed_v1",
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    ns: dict[str, object] = {}
    exec(script, ns)
    ns["FACTOR_CACHE_SINGLE_DIR"] = str(single_dir)
    ns["FACTOR_CACHE_META"] = str(cache_root / "_meta.json")

    contract = ns["_validate_official_cache_hit_contract"]("DemoFactor", code)
    assert contract["schema_version"] == "official_factor_cache_hit_validation_v1"
    assert contract["gate_status"] == "passed"
    assert contract["official_cache_hit"] is True

    df = ns["_try_cache_hit"]("DemoFactor", code)
    assert list(df.columns) == ["DemoFactor"]


def test_prepare_factors_contract_classifies_hash_mismatch(monkeypatch, tmp_path: Path) -> None:
    script = _script(monkeypatch)
    cache_root = tmp_path / "factor_values"
    single_dir = cache_root / "single"
    single_dir.mkdir(parents=True)
    idx = pd.MultiIndex.from_product(
        [[pd.Timestamp("2018-08-01"), pd.Timestamp("2026-04-30")], ["000001.SZ"]],
        names=["datetime", "instrument"],
    )
    pd.DataFrame({"value": [1.0, 2.0]}, index=idx).to_parquet(single_dir / "DemoFactor.parquet")
    (cache_root / "_meta.json").write_text(
        json.dumps(
            {
                "factors": {
                    "DemoFactor": {
                        "source_hash_raw": "wrong-hash",
                        "date_range": "2018-08-01~2026-04-30",
                        "as_of_date": "2026-04-30",
                        "window_train_start": "2018-08-01",
                        "data_source_mode": "official_offline_backtest_factor_data",
                        "universe_key": QE_ST_PIT_UNIVERSE_KEY,
                        "universe_fingerprint_sha256": "fp-test",
                        "index_policy": "st_pit_buy_eligible_reindexed_v1",
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    ns: dict[str, object] = {}
    exec(script, ns)
    ns["FACTOR_CACHE_SINGLE_DIR"] = str(single_dir)
    ns["FACTOR_CACHE_META"] = str(cache_root / "_meta.json")
    code = "def calculate_DemoFactor(instruments, start_date, end_date):\n    return None\n"

    contract = ns["_validate_official_cache_hit_contract"]("DemoFactor", code)

    assert contract["gate_status"] == "failed"
    assert contract["miss_reasons"]["hash_mismatch"] == ["DemoFactor"]
