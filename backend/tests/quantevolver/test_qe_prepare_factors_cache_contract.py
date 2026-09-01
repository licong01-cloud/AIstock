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


def _observation_panel(reference_factor_names: list[str]) -> dict[str, object]:
    return {
        "schema_version": "qe_observation_panel_v1",
        "panel_id": "ma_e16_ce3_reference_v1",
        "mode": "reference_factor_intersection",
        "reference_factor_names": reference_factor_names,
    }


def _panel_script(
    monkeypatch,
    *,
    factor_names: list[str] | None = None,
    reference_factor_names: list[str] | None = None,
) -> str:
    _patch_universe(monkeypatch)
    names = factor_names or ["BaseA", "BaseB", "Candidate"]
    references = reference_factor_names or ["BaseA", "BaseB"]
    factors_info = [
        {
            "factor_name": name,
            "code_text": f"def calculate_{name}(instruments, start_date, end_date):\n    return None\n",
        }
        for name in names
    ]
    script = ConfigComposer()._compose_prepare_factors(
        factors_info,
        factor_data_dir="/tmp/factor_data",
        data_split=dict(RDAGENT_DEFAULT_DATA_SPLIT),
        custom_params={
            "disable_alpha158": True,
            "observation_panel": _observation_panel(references),
        },
    )
    assert script is not None
    compile(script, "prepare_factors.py", "exec")
    return script


def _legacy_multi_factor_script(monkeypatch) -> str:
    _patch_universe(monkeypatch)
    factors_info = [
        {
            "factor_name": name,
            "code_text": f"def calculate_{name}(instruments, start_date, end_date):\n    return None\n",
        }
        for name in ["BaseA", "BaseB"]
    ]
    script = ConfigComposer()._compose_prepare_factors(
        factors_info,
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


def test_prepare_factors_embeds_normalized_observation_panel_contract(monkeypatch) -> None:
    script = _panel_script(monkeypatch)
    ns: dict[str, object] = {}
    exec(script, ns)

    assert ns["QE_OBSERVATION_PANEL_CONTRACT"] == _observation_panel(["BaseA", "BaseB"])
    assert "def _apply_observation_panel" in script
    assert "qe_observation_panel_empty" in script


@pytest.mark.parametrize(
    ("custom_params", "reason_code"),
    [
        (
            {"disable_alpha158": True, "observation_panel": "not-an-object"},
            "qe_observation_panel_contract_invalid",
        ),
        (
            {
                "disable_alpha158": True,
                "observation_panel": {
                    **_observation_panel(["BaseA"]),
                    "unexpected": True,
                },
            },
            "qe_observation_panel_unknown_keys",
        ),
        (
            {
                "disable_alpha158": True,
                "observation_panel": {
                    **_observation_panel(["BaseA"]),
                    "schema_version": "qe_observation_panel_v2",
                },
            },
            "qe_observation_panel_schema_invalid",
        ),
        (
            {
                "disable_alpha158": True,
                "observation_panel": {
                    **_observation_panel(["BaseA"]),
                    "mode": "outer_union",
                },
            },
            "qe_observation_panel_mode_invalid",
        ),
        (
            {
                "disable_alpha158": True,
                "observation_panel": {
                    **_observation_panel(["BaseA"]),
                    "panel_id": "contains spaces",
                },
            },
            "qe_observation_panel_id_invalid",
        ),
        (
            {
                "disable_alpha158": True,
                "observation_panel": _observation_panel([]),
            },
            "qe_observation_panel_reference_factors_invalid",
        ),
        (
            {
                "disable_alpha158": True,
                "observation_panel": _observation_panel(["BaseA", "BaseA"]),
            },
            "qe_observation_panel_reference_factors_duplicate",
        ),
        (
            {
                "disable_alpha158": True,
                "observation_panel": _observation_panel(["Missing"]),
            },
            "qe_observation_panel_reference_factor_not_configured",
        ),
        (
            {
                "disable_alpha158": False,
                "observation_panel": _observation_panel(["BaseA"]),
            },
            "qe_observation_panel_alpha158_not_supported",
        ),
    ],
)
def test_observation_panel_contract_rejects_invalid_declarations(
    custom_params: dict[str, object],
    reason_code: str,
) -> None:
    with pytest.raises(ValueError, match=reason_code):
        ConfigComposer._normalize_observation_panel_contract(
            custom_params,
            ["BaseA", "BaseB", "Candidate"],
        )


def test_observation_panel_keeps_identical_reference_index_for_sparse_candidate(monkeypatch) -> None:
    script = _panel_script(monkeypatch)
    ns: dict[str, object] = {}
    exec(script, ns)
    base_index = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp("2026-01-05"), "000001.SZ"),
            (pd.Timestamp("2026-01-05"), "000002.SZ"),
        ],
        names=["datetime", "instrument"],
    )
    candidate_index = base_index.append(
        pd.MultiIndex.from_tuples(
            [(pd.Timestamp("2026-01-05"), "920001.BJ")],
            names=["datetime", "instrument"],
        )
    )
    combined = pd.concat(
        [
            pd.DataFrame({"BaseA": [1.0, 2.0]}, index=base_index),
            pd.DataFrame({"BaseB": [3.0, 4.0]}, index=base_index),
            pd.DataFrame({"Candidate": [5.0, float("nan"), 6.0]}, index=candidate_index),
        ],
        axis=1,
    )

    filtered = ns["_apply_observation_panel"](combined)

    pd.testing.assert_index_equal(filtered.index, base_index)
    assert pd.isna(filtered.loc[base_index[1], "Candidate"])


def test_observation_panel_contract_is_identical_for_matched_ce3_and_ce4(monkeypatch) -> None:
    ce3_script = _panel_script(
        monkeypatch,
        factor_names=["BaseA", "BaseB"],
        reference_factor_names=["BaseA", "BaseB"],
    )
    ce4_script = _panel_script(
        monkeypatch,
        factor_names=["BaseA", "BaseB", "Candidate"],
        reference_factor_names=["BaseA", "BaseB"],
    )
    ce3_ns: dict[str, object] = {}
    ce4_ns: dict[str, object] = {}
    exec(ce3_script, ce3_ns)
    exec(ce4_script, ce4_ns)
    assert ce3_ns["QE_OBSERVATION_PANEL_CONTRACT"] == ce4_ns["QE_OBSERVATION_PANEL_CONTRACT"]

    reference_index = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp("2026-01-05"), "000001.SZ"),
            (pd.Timestamp("2026-01-05"), "000002.SZ"),
        ],
        names=["datetime", "instrument"],
    )
    expanded_index = reference_index.append(
        pd.MultiIndex.from_tuples(
            [(pd.Timestamp("2026-01-05"), "920001.BJ")],
            names=["datetime", "instrument"],
        )
    )
    ce3_frame = pd.DataFrame(
        {"BaseA": [1.0, 2.0], "BaseB": [3.0, 4.0]},
        index=reference_index,
    )
    ce4_frame = ce3_frame.join(
        pd.DataFrame({"Candidate": [5.0, 6.0, 7.0]}, index=expanded_index),
        how="outer",
    )

    ce3_filtered = ce3_ns["_apply_observation_panel"](ce3_frame)
    ce4_filtered = ce4_ns["_apply_observation_panel"](ce4_frame)
    pd.testing.assert_index_equal(ce3_filtered.index, ce4_filtered.index)
    pd.testing.assert_index_equal(ce3_filtered.index, reference_index)


def test_observation_panel_legacy_mode_preserves_outer_union(monkeypatch) -> None:
    script = _legacy_multi_factor_script(monkeypatch)
    ns: dict[str, object] = {}
    exec(script, ns)
    base_a = pd.DataFrame(
        {"BaseA": [1.0]},
        index=pd.Index(["000001.SZ"], name="instrument"),
    )
    base_b = pd.DataFrame(
        {"BaseB": [2.0]},
        index=pd.Index(["000002.SZ"], name="instrument"),
    )
    selected = ns["_select_factor_results"](
        ["BaseA", "BaseB"],
        {"BaseA": base_a, "BaseB": base_b},
    )
    combined = pd.concat(selected, axis=1)

    assert ns["QE_OBSERVATION_PANEL_CONTRACT"] == {}
    assert len(selected) == 2
    assert selected[0] is base_a
    assert selected[1] is base_b
    assert ns["_apply_observation_panel"](combined) is combined
    assert list(combined.index) == ["000001.SZ", "000002.SZ"]


def test_observation_panel_legacy_mode_omits_failed_factor_without_aborting(
    monkeypatch,
) -> None:
    script = _legacy_multi_factor_script(monkeypatch)
    ns: dict[str, object] = {}
    exec(script, ns)
    base_a = pd.DataFrame({"BaseA": [1.0]})

    selected = ns["_select_factor_results"](
        ["BaseA", "BaseB"],
        {"BaseA": base_a, "BaseB": None},
    )

    assert len(selected) == 1
    assert selected[0] is base_a


def test_observation_panel_missing_reference_column_fails_closed(monkeypatch) -> None:
    script = _panel_script(monkeypatch)
    ns: dict[str, object] = {}
    exec(script, ns)
    combined = pd.DataFrame({"BaseA": [1.0], "Candidate": [2.0]})

    with pytest.raises(RuntimeError, match="qe_observation_panel_reference_factor_missing"):
        ns["_apply_observation_panel"](combined)


def test_observation_panel_empty_intersection_fails_closed(monkeypatch) -> None:
    script = _panel_script(monkeypatch)
    ns: dict[str, object] = {}
    exec(script, ns)
    combined = pd.DataFrame(
        {
            "BaseA": [1.0, float("nan")],
            "BaseB": [float("nan"), 2.0],
            "Candidate": [3.0, 4.0],
        }
    )

    with pytest.raises(RuntimeError, match="qe_observation_panel_empty"):
        ns["_apply_observation_panel"](combined)


@pytest.mark.parametrize(
    ("factor_results", "reason_code"),
    [
        (
            {"BaseA": pd.DataFrame({"BaseA": [1.0]}), "BaseB": None},
            "qe_factor_result_incomplete",
        ),
        (
            {
                "BaseA": pd.DataFrame({"BaseA": [1.0]}),
                "BaseB": pd.DataFrame({"wrong": [2.0]}),
            },
            "qe_factor_result_column_missing",
        ),
    ],
)
def test_prepare_factors_missing_configured_output_fails_closed(
    monkeypatch,
    factor_results: dict[str, pd.DataFrame | None],
    reason_code: str,
) -> None:
    script = _panel_script(monkeypatch)
    ns: dict[str, object] = {}
    exec(script, ns)

    with pytest.raises(RuntimeError, match=reason_code):
        ns["_select_factor_results"](["BaseA", "BaseB"], factor_results)


def test_ma_e16_in_memory_compose_keeps_observation_panel_out_of_strategy_kwargs(
    monkeypatch,
) -> None:
    composer = ConfigComposer()
    factor_names = ["BaseA", "BaseB", "Candidate"]
    monkeypatch.setattr(
        composer,
        "_get_factors_info",
        lambda *_args, **_kwargs: [
            {
                "factor_name": name,
                "source": "manual",
                "code_text": (
                    f"def calculate_{name}(instruments, start_date, end_date):\n"
                    "    return None\n"
                ),
            }
            for name in factor_names
        ],
    )
    monkeypatch.setattr(composer, "_get_model_info", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        composer,
        "_get_strategy_info",
        lambda *_args, **_kwargs: {
            "strategy_id": "score_weighted_topk_v2",
            "source_code": "class ScoreWeightedTopkStrategyV2(object):\n    pass\n",
            "portfolio_config": {
                "class": "ScoreWeightedTopkStrategyV2",
                "kwargs": {"topk": 50, "n_drop": 1},
            },
        },
    )
    monkeypatch.setattr(
        composer,
        "_fetch_workspace_config",
        lambda *_args, **_kwargs: {
            "workspace_base": "/tmp/qe_workspace",
            "qlib_data_path": "/tmp/qlib_day",
            "qlib_minute_path": "/tmp/qlib_minute",
            "factor_data_dir": "/tmp/factor_data",
        },
    )
    monkeypatch.setattr(
        composer,
        "_prepare_risk_policy_runtime",
        lambda **kwargs: (kwargs["custom_params"], None),
    )
    monkeypatch.setattr(
        composer,
        "_prepare_suspend_filter_runtime",
        lambda **kwargs: (kwargs["custom_params"], None),
    )
    monkeypatch.setattr(composer, "_get_read_exp_res_content", lambda: "# read_exp_res")
    _patch_universe(monkeypatch)

    result = composer.compose_experiment_in_memory(
        factor_names=factor_names,
        model_id=None,
        strategy_id="score_weighted_topk_v2",
        data_split=dict(RDAGENT_DEFAULT_DATA_SPLIT),
        custom_params={
            "disable_alpha158": True,
            "topk": 50,
            "n_drop": 1,
            "observation_panel": _observation_panel(["BaseA", "BaseB"]),
        },
        skip_db_save=True,
        execution_algo="CLOSE_PRICE",
        execution_algo_params={},
        task_id="qe_ma_e16_contract",
        loop_index=1,
    )

    conf_yaml = result["experiment_files"]["conf.yaml"]
    prepare_factors = result["experiment_files"]["prepare_factors.py"]
    assert "observation_panel" not in conf_yaml
    assert "class: ScoreWeightedTopkStrategyV2" in conf_yaml
    assert "qe_observation_panel_v1" in prepare_factors
    compile(prepare_factors, "prepare_factors.py", "exec")
