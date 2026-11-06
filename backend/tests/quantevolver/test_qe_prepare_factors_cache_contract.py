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
    feature_processing: dict | None = None,
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
            **({"feature_processing": feature_processing} if feature_processing is not None else {}),
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


def _direct_cache_fixture(monkeypatch, tmp_path):
    import datetime
    from types import SimpleNamespace

    import backend.services.quantevolver.config_composer as module

    data = tmp_path / "inputs"
    data.mkdir()
    (data / "daily_pv.h5").write_bytes(b"immutable-input-1")
    meta = {"schema_version": "qe_direct_factor_h5_static_v2", "start": "2018-08-01",
            "end": "2026-06-30", "universe_key": QE_ST_PIT_UNIVERSE_KEY, "files": ["daily_pv.h5"]}
    (data / "meta.json").write_text(json.dumps(meta), encoding="utf-8")
    binding = SimpleNamespace(
        release_id="frozen-research", cutoff=datetime.date(2026, 6, 30), factor_meta=meta,
        factor_meta_sha256=hashlib.sha256((data / "meta.json").read_bytes()).hexdigest(),
        day_pins={"universe_key": QE_ST_PIT_UNIVERSE_KEY, "rule_version": "rule_v1"},
        selection_pins={"instruments_sha256": "fp-test"},
    )
    monkeypatch.setattr(module, "_direct_v2_dataset_binding", lambda params: binding)
    script = _script(monkeypatch)
    namespace = {"__name__": "contract_test"}
    exec(script, namespace)
    root = tmp_path / "factor_values"
    (root / "single").mkdir(parents=True)
    namespace.update(FACTOR_DATA_DIR=str(data), FACTOR_CACHE_SINGLE_DIR=str(root / "single"),
                     FACTOR_CACHE_META=str(root / "_meta.json"))
    index = pd.MultiIndex.from_product(
        [[pd.Timestamp("2018-08-01"), pd.Timestamp("2026-06-30")], ["000001.SZ"]],
        names=["datetime", "instrument"],
    )
    frame = pd.DataFrame({"DemoFactor": [1., 2.]}, index=index)
    code = "def calculate_DemoFactor(instruments, start_date, end_date):\n    return None\n"
    with namespace["_exclusive_cache_lock"]("factor:DemoFactor"):
        namespace["_write_cache"]("DemoFactor", code, frame)
    _cache_path, metadata_path = namespace["_factor_cache_paths"]("DemoFactor", code)
    return namespace, Path(metadata_path).parent, data, code, script


def test_direct_cache_publication_and_exact_input_readback(monkeypatch, tmp_path):
    namespace, root, data, code, _script_text = _direct_cache_fixture(monkeypatch, tmp_path)
    entry = json.loads((root / "_meta.json").read_text())["factors"]["DemoFactor"]
    assert entry["qe_input_identity"]["input_files_sha256"] == {
        "daily_pv.h5": hashlib.sha256((data / "daily_pv.h5").read_bytes()).hexdigest()}
    assert entry["cache_file_sha256"] == hashlib.sha256((root / "single/DemoFactor.parquet").read_bytes()).hexdigest()
    assert namespace["_validate_official_cache_hit_contract"]("DemoFactor", code)["official_cache_hit"]
    assert namespace["_try_cache_hit"]("DemoFactor", code)["DemoFactor"].tolist() == [1., 2.]


def test_direct_cache_legacy_identity_rejected_without_recompute_or_write(monkeypatch, tmp_path):
    namespace, root, _data, code, _script_text = _direct_cache_fixture(monkeypatch, tmp_path)
    path = root / "_meta.json"
    metadata = json.loads(path.read_text())
    del metadata["factors"]["DemoFactor"]["qe_input_identity"]
    path.write_text(json.dumps(metadata))
    before = {p: p.read_bytes() for p in (path, root / "single/DemoFactor.parquet")}
    with pytest.raises(RuntimeError, match="qe_factor_cache_input_identity_unverified"):
        namespace["_execute_factor_locked"]("DemoFactor", code, str(tmp_path))
    assert {p: p.read_bytes() for p in before} == before
    assert not (tmp_path / "_factor_DemoFactor").exists()


def test_direct_cache_changed_inputs_same_metadata_rejected_in_fresh_reader(monkeypatch, tmp_path):
    namespace, root, data, code, script = _direct_cache_fixture(monkeypatch, tmp_path)
    (data / "daily_pv.h5").write_bytes(b"different-input-version")
    with pytest.raises(RuntimeError, match="qe_factor_cache_source_changed"):
        namespace["_try_cache_hit"]("DemoFactor", code)
    fresh = {"__name__": "contract_test_fresh"}
    exec(script, fresh)
    fresh.update(FACTOR_DATA_DIR=str(data), FACTOR_CACHE_SINGLE_DIR=namespace["FACTOR_CACHE_SINGLE_DIR"],
                 FACTOR_CACHE_META=namespace["FACTOR_CACHE_META"])
    assert fresh["_try_cache_hit"]("DemoFactor", code) is None
    assert Path(fresh["_factor_cache_paths"]("DemoFactor", code)[1]).parent != root


def test_direct_cache_changed_values_rejected_even_with_matching_input_tags(monkeypatch, tmp_path):
    namespace, root, _data, code, _script_text = _direct_cache_fixture(monkeypatch, tmp_path)
    path = root / "single/DemoFactor.parquet"
    frame = pd.read_parquet(path)
    frame.iloc[0, 0] = 99.
    frame.to_parquet(path)
    with pytest.raises(RuntimeError, match="qe_factor_cache_value_hash_mismatch"):
        namespace["_try_cache_hit"]("DemoFactor", code)


def test_direct_cache_input_identity_is_path_independent(monkeypatch, tmp_path):
    import shutil

    namespace, _root, data, _code, _script_text = _direct_cache_fixture(monkeypatch, tmp_path)
    expected = namespace["_cache_input_identity"]()
    other = tmp_path / "other_node"
    shutil.copytree(data, other)
    namespace.update(FACTOR_DATA_DIR=str(other), _QE_CACHE_INPUT_STATE=None)
    assert namespace["_cache_input_identity"]() == expected


def test_direct_cache_provenance_fresh_process_does_not_relabel_legacy(monkeypatch, tmp_path):
    import os
    import subprocess
    import sys

    namespace, root, data, code, script = _direct_cache_fixture(monkeypatch, tmp_path)
    meta = root / "_meta.json"
    payload = json.loads(meta.read_text())
    payload["factors"]["DemoFactor"].pop("qe_input_identity")
    meta.write_text(json.dumps(payload))
    before = meta.read_bytes()
    path = tmp_path / "generated_prepare.py"
    path.write_text(script, encoding="utf-8")
    runner = "import runpy,sys; ns=runpy.run_path(sys.argv[1]); ns['_try_cache_hit']('DemoFactor',sys.argv[2])"
    result = subprocess.run([sys.executable, "-c", runner, str(path), code],
        env={**os.environ, "FACTOR_CACHE_DIR": str(Path(namespace["FACTOR_CACHE_META"]).parent), "RDAGENT_FACTOR_DATA_DIR": str(data),
             "RDAGENT_FACTOR_DATA_WSL": str(data), "PYTHONIOENCODING": "utf-8"},
        capture_output=True, encoding="utf-8", timeout=45)
    assert result.returncode != 0 and "qe_factor_cache_input_identity_unverified" in result.stderr
    assert meta.read_bytes() == before


def test_direct_cache_new_version_computes_then_hits_same_window_without_legacy_mutation(monkeypatch, tmp_path):
    namespace, _version, data, _old_code, _script_text = _direct_cache_fixture(monkeypatch, tmp_path)
    index = pd.MultiIndex.from_product(
        [[pd.Timestamp("2018-07-31"), pd.Timestamp("2018-08-01"), pd.Timestamp("2026-06-30")], ["000001.SZ"]],
        names=["datetime", "instrument"],
    )
    values = pd.DataFrame({"raw": [2., 3., 4.]}, index=index)
    values.to_hdf(data / "daily_pv.h5", key="data", mode="w")
    namespace["_QE_CACHE_INPUT_STATE"] = None  # a new input version, in a fresh run
    legacy_root = Path(namespace["FACTOR_CACHE_META"]).parent
    legacy_file = legacy_root / "single/DemoFactor.parquet"
    pd.DataFrame({"value": [999.]}, index=index[:1]).to_parquet(legacy_file)
    legacy_meta = legacy_root / "_meta.json"
    legacy_meta.write_text('{"source": "old-unbound-cache"}')
    before = {p: p.read_bytes() for p in (legacy_file, legacy_meta)}
    code = (
        "import pandas as pd\nfrom pathlib import Path\n"
        "data = pd.read_hdf(Path(__file__).parent / 'daily_pv.h5')\n"
        "data = (data * 2).rename(columns={'raw': 'DemoFactor'})\n"
        "data.to_hdf(Path(__file__).parent / 'result.h5', key='data')\n"
    )
    work = tmp_path / "work"
    work.mkdir()
    first = namespace["execute_factor"]("DemoFactor", code, str(work))
    second = namespace["execute_factor"]("DemoFactor", code, str(work))
    pd.testing.assert_frame_equal(first, second)
    pd.testing.assert_frame_equal(first, (values.iloc[1:] * 2).rename(columns={"raw": "DemoFactor"}))
    version_file, meta = map(Path, namespace["_factor_cache_paths"]("DemoFactor", code))
    assert "qe_versions" in version_file.parts and version_file != legacy_file
    assert len(pd.read_parquet(version_file)) == 3  # immutable warmup retained
    assert {p: p.read_bytes() for p in before} == before
    expected = {p: p.read_bytes() for p in (version_file, meta)}
    with pytest.raises(RuntimeError, match="qe_factor_cache_immutable_version_exists"):
        namespace["_write_cache"]("DemoFactor", code, first)
    assert {p: p.read_bytes() for p in expected} == expected


def test_direct_cache_partial_version_does_not_recompute_or_overwrite(monkeypatch, tmp_path):
    namespace, _root, _data, code, _script_text = _direct_cache_fixture(monkeypatch, tmp_path)
    other = code + "# next source version\n"
    cache_path, _meta_path = map(Path, namespace["_factor_cache_paths"]("DemoFactor", other))
    cache_path.parent.mkdir(parents=True)
    cache_path.write_bytes(b"incomplete-published-file")
    with pytest.raises(RuntimeError, match="qe_factor_cache_version_partial_requires_reconciliation"):
        namespace["execute_factor"]("DemoFactor", other, str(tmp_path))
    assert cache_path.read_bytes() == b"incomplete-published-file"


def test_direct_cache_window_change_gets_independent_immutable_version(monkeypatch, tmp_path):
    namespace, root, _data, code, _script_text = _direct_cache_fixture(monkeypatch, tmp_path)
    before = {p: p.read_bytes() for p in (root / "_meta.json", root / "single/DemoFactor.parquet")}
    namespace["TRAIN_START"] = "2018-09-01"
    assert Path(namespace["_factor_cache_paths"]("DemoFactor", code)[1]).parent != root
    assert namespace["_try_cache_hit"]("DemoFactor", code) is None
    assert {p: p.read_bytes() for p in before} == before


def test_direct_cache_off_still_rejects_source_change_during_computation(monkeypatch, tmp_path):
    namespace, _root, data, code, _script_text = _direct_cache_fixture(monkeypatch, tmp_path)
    namespace["FACTOR_CACHE_SINGLE_DIR"] = ""
    frame = pd.DataFrame({"DemoFactor": [1.]}, index=pd.MultiIndex.from_tuples(
        [(pd.Timestamp("2026-06-30"), "000001.SZ")], names=["datetime", "instrument"]))
    work = tmp_path / "work"
    work.mkdir()

    def compute(*args, **kwargs):
        (data / "daily_pv.h5").write_bytes(b"changed while computing")
        frame.to_hdf(work / "_factor_DemoFactor/result.h5", key="data")
        return b""

    monkeypatch.setattr(namespace["subprocess"], "check_output", compute)
    with pytest.raises(RuntimeError, match="qe_factor_cache_source_changed"):
        namespace["execute_factor"]("DemoFactor", code, str(work))


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


@pytest.fixture
def panel_composer(monkeypatch):
    monkeypatch.setattr(
        "backend.services.quantevolver.config_composer.load_active_qe_profile",
        lambda: None,
    )
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
    return composer


def test_ma_e16_in_memory_compose_keeps_observation_panel_out_of_strategy_kwargs(panel_composer) -> None:
    composer = panel_composer
    factor_names = ["BaseA", "BaseB", "Candidate"]

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


@pytest.mark.parametrize("excluded,clip", [([], True), (["BaseA"], True), ([], False)])
def test_feature_processing_real_panel_output_keeps_cache_and_identity(monkeypatch, tmp_path, excluded, clip):
    import pyarrow.parquet as pq

    options = {"exclude_from_model": excluded, "clip_outlier": clip}
    script = _panel_script(monkeypatch, reference_factor_names=["BaseA"], feature_processing=options)
    index = pd.MultiIndex.from_product(
        [pd.date_range("2024-01-02", periods=3), ["000001.SZ"]], names=["datetime", "instrument"],
    )
    original = pd.DataFrame(
        {"BaseA": [1., 2., float("nan")], "BaseB": [4., float("nan"), 6.], "Candidate": [7., 8., 9.]},
        index=index,
    )
    shared = tmp_path / "shared.parquet"
    original.to_parquet(shared)
    before = shared.read_bytes()
    workspace = tmp_path / "workspace"
    (workspace / "factors").mkdir(parents=True)
    for name in original.columns:
        (workspace / "factors" / f"{name}.py").write_text("# supplied cached factor", encoding="utf-8")
    (workspace / "conf.yaml").write_text(
        "data_handler_config:\n  data_loader:\n    class: DynamicFactorsOnlyLoader\n", encoding="utf-8",
    )
    ns = {"__file__": str(workspace / "prepare_factors.py")}
    exec(script, ns)
    monkeypatch.chdir(tmp_path)  # restore cwd even though generated main changes it
    ns["_validate_factor_data_dataset_contract"] = lambda: None
    ns["link_all_files_to_dir"] = lambda *_: None
    ns["execute_factor"] = lambda name, *_: pd.read_parquet(shared, columns=[name])
    ns["main"]()
    out = workspace / "combined_factors_df.parquet"
    actual = pd.read_parquet(out)
    expected = original.iloc[:2].drop(columns=excluded)
    expected.columns = pd.MultiIndex.from_product([["feature"], expected.columns])
    pd.testing.assert_frame_equal(actual, expected)
    assert shared.read_bytes() == before
    identity = json.loads(pq.read_schema(out).metadata[b"qe_feature_processing"])
    assert identity == {
        **options, "computed_factor_names": list(original.columns),
        "model_feature_names": list(expected.columns.get_level_values(1)),
    }
    assert (workspace / ".factor_env").read_text().strip() == f"export num_features={len(expected.columns)}"


def test_feature_processing_b_a_c_b_yaml_isolation_and_real_normalizer(panel_composer, monkeypatch):
    import copy
    import sys
    import yaml

    composer = panel_composer
    params = {"disable_alpha158": True, "observation_panel": _observation_panel(["BaseA"])}
    configurations = []
    for options in (None, {"exclude_from_model": ["BaseA"]}, {"clip_outlier": False}, None):
        cp = {**params, **({"feature_processing": options} if options is not None else {})}
        before = copy.deepcopy(cp)
        result = composer.compose_experiment_in_memory(
            factor_names=["BaseA", "BaseB", "Candidate"], model_id=None,
            strategy_id="score_weighted_topk_v2", data_split=dict(RDAGENT_DEFAULT_DATA_SPLIT),
            custom_params=cp, skip_db_save=True, execution_algo="TWAP",
        )
        assert cp == before
        configurations.append(yaml.safe_load(result["experiment_files"]["conf.yaml"]))
    baseline, ablation, unclipped, baseline_again = configurations
    assert baseline == ablation == baseline_again  # only prepare_factors model columns differ
    assert unclipped != baseline
    clip_kwargs = unclipped["data_handler_config"]["infer_processors"][0]["kwargs"]
    assert clip_kwargs["clip_outlier"] is False
    adjusted = copy.deepcopy(unclipped)
    adjusted["data_handler_config"]["infer_processors"][0]["kwargs"]["clip_outlier"] = True
    assert adjusted == baseline  # includes model, label, strategy, cost, split and all fit settings
    assert "feature_processing" not in yaml.safe_dump(baseline)
    assert "feature_processing" not in yaml.safe_dump(unclipped)

    # Run Qlib's actual append processing path: both DK_L and DK_I share infer processors.
    # API-only installations omit Qlib; YAML contracts above still run there.
    pytest.importorskip("qlib", reason="Real processor contract requires the existing Qlib worker/test environment")
    from qlib.data.dataset.handler import DataHandlerLP
    from qlib.data.dataset.loader import StaticDataLoader
    from backend.services.quantevolver import qe_custom_loaders

    monkeypatch.setitem(sys.modules, "qe_custom_loaders", qe_custom_loaders)
    index = pd.MultiIndex.from_product(
        [pd.bdate_range(RDAGENT_DEFAULT_DATA_SPLIT["train_start"], RDAGENT_DEFAULT_DATA_SPLIT["test_end"]),
         ["000001.SZ", "000002.SZ"]], names=["datetime", "instrument"],
    )
    frame = pd.DataFrame([float(i % 5) for i in range(len(index))], index=index,
                         columns=pd.MultiIndex.from_tuples([("feature", "BaseA")]))
    outlier = (pd.Timestamp("2020-01-02"), "000001.SZ")
    frame.loc[outlier, ("feature", "BaseA")] = 1000.
    frame[("label", "LABEL0")] = [float(i % 7) for i in range(len(index))]
    for conf, clip in ((baseline, True), (unclipped, False)):
        handler_conf = conf["data_handler_config"]
        handler = DataHandlerLP(
            instruments=None, data_loader=StaticDataLoader(frame.copy()),
            infer_processors=handler_conf["infer_processors"],
            learn_processors=handler_conf["learn_processors"],
        )
        training = handler.fetch(col_set="feature", data_key=DataHandlerLP.DK_L)
        inference = handler.fetch(col_set="feature", data_key=DataHandlerLP.DK_I)
        pd.testing.assert_frame_equal(training, inference.loc[training.index])
        assert training.loc[outlier, "BaseA"] <= 3. if clip else training.loc[outlier, "BaseA"] > 3.


@pytest.mark.parametrize("options", [
    {"exclude_from_model": ["Unknown"]}, {"exclude_from_model": ["BaseA", "BaseA"]},
    {"exclude_from_model": ["BaseA", "BaseB", "Candidate"]},
    {"exclude_from_model": "BaseA"}, {"clip_outlier": "false"}, {"clip_outlier": 0},
    {"typo": False},
])
def test_feature_processing_rejects_silent_misconfiguration(monkeypatch, options):
    with pytest.raises(ValueError, match="qe_feature_processing_invalid"):
        _panel_script(monkeypatch, feature_processing=options)


def test_feature_processing_public_request_and_retry_snapshot_are_request_local(monkeypatch, tmp_path):
    import copy

    monkeypatch.setenv("QE_LIVE_LOG_DIR", str(tmp_path / "qe_logs"))
    from backend.routers.quantevolver_evolution import CustomEvoLoopConfig
    from backend.services.quantevolver.experiment_config_builders import (
        build_config_from_custom_evo_loop, build_config_from_retry_loop,
    )
    from tests.fixtures.sample_configs import CUSTOM_EVO_LOOP_MINIMAL, CUSTOM_EVO_TASK

    original = copy.deepcopy(CUSTOM_EVO_LOOP_MINIMAL)
    diagnostic = {
        "raw_state_controls": {
            "schema_version": "qe_raw_state_controls_v1", "source_factor": "Alpha001",
            "valid_column": "r8_input_is_valid", "zero_column": "r8_input_is_reported_zero",
        },
    }
    for options in (None, {"exclude_from_model": ["Alpha001"]}, {"clip_outlier": False}, diagnostic, None):
        request = CustomEvoLoopConfig(**{
            **original, "disable_alpha158": True,
            **({"feature_processing": options} if options is not None else {}),
        })
        public = request.model_dump()
        cfg = build_config_from_custom_evo_loop(public, CUSTOM_EVO_TASK)
        params = cfg.build_custom_params()
        assert params.get("feature_processing") == options
        assert "feature_processing" not in cfg.build_strategy_params()
        snapshot = json.loads(json.dumps({
            "factor_list": cfg.factor_names, "model_id": cfg.model_id,
            "model_params": params, "disable_alpha158": True,
        }))
        restored = build_config_from_retry_loop(snapshot, CUSTOM_EVO_TASK)
        assert restored.build_custom_params().get("feature_processing") == options
        assert public["feature_processing"] == options
    assert CUSTOM_EVO_LOOP_MINIMAL == original


@pytest.mark.parametrize("mode", [{"backtest_only": True}, {"prediction_replay": True}, {"alpha_mode": "multi"}])
def test_feature_processing_cannot_silently_skip_training(mode):
    from backend.services.quantevolver.experiment_config_builders import build_config_from_custom_evo_loop
    from tests.fixtures.sample_configs import CUSTOM_EVO_LOOP_MINIMAL, CUSTOM_EVO_TASK

    with pytest.raises(ValueError, match="feature_processing: input changes require"):
        build_config_from_custom_evo_loop({
            **CUSTOM_EVO_LOOP_MINIMAL, **mode, "disable_alpha158": True,
            "feature_processing": {"clip_outlier": False},
        }, CUSTOM_EVO_TASK)


RAW_STATE_SOURCE = "research_reported_short_inventory_to_float"
RAW_STATE_COLUMNS = ["r8_input_is_valid", "r8_input_is_reported_zero"]


def _raw_state_options(*, include_value=False):
    return {
        "raw_state_controls": {
            "schema_version": "qe_raw_state_controls_v1",
            "source_factor": RAW_STATE_SOURCE,
            "valid_column": RAW_STATE_COLUMNS[0],
            "zero_column": RAW_STATE_COLUMNS[1],
        },
        "exclude_from_model": ["PDH"] + ([] if include_value else [RAW_STATE_SOURCE]),
        "clip_outlier": True,
    }


@pytest.mark.parametrize("change", [
    {"schema_version": "v2"}, {"source_factor": "unknown"}, {"source_factor": []},
    {"valid_column": "AF0"}, {"zero_column": RAW_STATE_COLUMNS[0]},
    {"valid_column": "bad name"}, {"valid_column": None}, {"formula": "F.shift(1)"},
])
def test_raw_state_controls_reject_invalid_declarations(change):
    options = _raw_state_options()
    options["raw_state_controls"].update(change)
    with pytest.raises(ValueError, match="qe_feature_processing_invalid"):
        ConfigComposer._normalize_feature_processing(
            {"disable_alpha158": True, "feature_processing": options}, ["AF0", "PDH", RAW_STATE_SOURCE],
        )


@pytest.mark.parametrize("declaration", [None, [], {}, {"schema_version": "qe_raw_state_controls_v1"}])
def test_raw_state_controls_require_complete_declaration(declaration):
    options = _raw_state_options()
    options["raw_state_controls"] = declaration
    with pytest.raises(ValueError, match="qe_feature_processing_invalid"):
        ConfigComposer._normalize_feature_processing(
            {"disable_alpha158": True, "feature_processing": options}, ["AF0", "PDH", RAW_STATE_SOURCE],
        )


@pytest.mark.parametrize("values", [
    [0., .01], [0., float("inf")], [0., -float("inf")],
    ["0", "-1"], [True, False], [0j, -1j],
])
def test_raw_state_controls_reject_invalid_source(monkeypatch, values):
    ns = {}
    exec(_panel_script(
        monkeypatch, factor_names=["AF0", "PDH", RAW_STATE_SOURCE],
        reference_factor_names=["AF0"], feature_processing=_raw_state_options(),
    ), ns)
    with pytest.raises(ValueError, match="qe_raw_state_controls_invalid"):
        ns["_derive_raw_state_controls"](pd.DataFrame({RAW_STATE_SOURCE: values}))


def test_raw_state_controls_three_states_and_ambiguous_input_fail_closed(monkeypatch):
    ns = {}
    exec(_panel_script(
        monkeypatch, factor_names=["AF0", "PDH", RAW_STATE_SOURCE],
        reference_factor_names=["AF0"], feature_processing=_raw_state_options(),
    ), ns)
    original = pd.DataFrame({RAW_STATE_SOURCE: pd.array([pd.NA, 0., -.002, -0.], dtype="Float64")})
    before = original.copy(deep=True)
    out = ns["_derive_raw_state_controls"](original)
    pd.testing.assert_frame_equal(original, before)
    pd.testing.assert_series_equal(out[RAW_STATE_SOURCE], original[RAW_STATE_SOURCE])
    assert out[RAW_STATE_COLUMNS].values.tolist() == [[0, 0], [1, 1], [1, 0], [1, 1]]
    assert out[RAW_STATE_COLUMNS].dtypes.tolist() == ["int8", "int8"]
    for bad in (original.loc[[0, 0]], out, original.rename(columns={RAW_STATE_SOURCE: "unknown"})):
        with pytest.raises(ValueError, match="qe_raw_state_controls_invalid"):
            ns["_derive_raw_state_controls"](bad)
    # Reject ambiguity before concat's historical duplicate-column removal can hide it.
    factors = {name: pd.DataFrame({name: [0.]}) for name in ["AF0", "PDH", RAW_STATE_SOURCE]}
    for bad in (factors[RAW_STATE_SOURCE].loc[[0, 0]], factors[RAW_STATE_SOURCE].assign(AF0=0.)):
        with pytest.raises(ValueError, match="qe_raw_state_controls_invalid"):
            ns["_select_factor_results"](list(factors), {**factors, RAW_STATE_SOURCE: bad})


def test_raw_state_controls_fresh_process_two_arms_cache_and_projection(monkeypatch, tmp_path):
    """Run the emitted script unchanged, including identity validation and cache reads."""
    import os
    import subprocess
    import sys

    import pyarrow.parquet as pq

    names = [f"AF{i}" for i in range(11)] + ["PDH", RAW_STATE_SOURCE]
    cache_root = tmp_path / "factor_values"
    data_dir = tmp_path / "factor_data"
    data_dir.mkdir()
    monkeypatch.setenv("FACTOR_CACHE_DIR", str(cache_root))
    monkeypatch.setenv("RDAGENT_FACTOR_DATA_DIR", str(data_dir))
    scripts = [_panel_script(
        monkeypatch, factor_names=names, reference_factor_names=["AF0", "PDH"],
        feature_processing=_raw_state_options(include_value=include),
    ) for include in (False, True)]
    ns = {}
    exec(scripts[0], ns)
    (data_dir / "meta.json").write_text(json.dumps(ns["QE_DATASET_EXPECTED_META"]), encoding="utf-8")
    index = pd.MultiIndex.from_product(
        [[pd.Timestamp(ns["TRAIN_START"]), pd.Timestamp(ns["TEST_END"])], ["000001.SZ", "000002.SZ", "000003.SZ"]],
        names=["datetime", "instrument"],
    )
    original = pd.DataFrame({name: [1., 2., 3., 4., 5., 6.] for name in names}, index=index)
    original[RAW_STATE_SOURCE] = [float("nan"), 0., -.002, -.1, 0., float("nan")]
    original.loc[index[-1], "PDH"] = float("nan")  # only the original reference mask removes this row
    for name in names:
        code = f"def calculate_{name}(instruments, start_date, end_date):\n    return None\n"
        with ns["_exclusive_cache_lock"]("factor:" + name):
            ns["_write_cache"](name, code, original[[name]])
    paths = list((cache_root / "single").glob("*.parquet")) + [cache_root / "_meta.json", data_dir / "meta.json"]
    hashes = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
    outputs = []
    for include, script in zip((False, True), scripts):
        work = tmp_path / ("ZF" if include else "Z")
        (work / "factors").mkdir(parents=True)
        for name in names:
            (work / "factors" / f"{name}.py").write_text(
                f"def calculate_{name}(instruments, start_date, end_date):\n    return None\n", encoding="utf-8",
            )
        (work / "prepare_factors.py").write_text(script, encoding="utf-8")
        (work / "conf.yaml").write_text(
            "data_handler_config:\n  data_loader:\n    class: DynamicFactorsOnlyLoader\n", encoding="utf-8",
        )
        result = subprocess.run(
            [sys.executable, str(work / "prepare_factors.py")], cwd=work,
            env={**os.environ, "PYTHONIOENCODING": "utf-8"}, capture_output=True, text=True, encoding="utf-8", timeout=45,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert result.stderr.count("CACHE HIT (") == 13
        out_path = work / "combined_factors_df.parquet"
        output = pd.read_parquet(out_path)["feature"]
        expected_columns = names[:11] + RAW_STATE_COLUMNS + ([RAW_STATE_SOURCE] if include else [])
        assert output.columns.tolist() == expected_columns
        assert output.index.equals(original.index[:-1])
        pd.testing.assert_frame_equal(output[names[:11]], original.loc[output.index, names[:11]])
        assert output[RAW_STATE_COLUMNS].values.tolist() == [[0, 0], [1, 1], [1, 0], [1, 0], [1, 1]]
        assert output[RAW_STATE_COLUMNS].dtypes.tolist() == ["int8", "int8"]
        if include:
            pd.testing.assert_series_equal(output[RAW_STATE_SOURCE], original.loc[output.index, RAW_STATE_SOURCE])
        identity = json.loads(pq.read_schema(out_path).metadata[b"qe_feature_processing"])
        assert identity["computed_factor_names"] == names
        assert identity["derived_control_names"] == RAW_STATE_COLUMNS
        assert identity["model_feature_names"] == expected_columns
        assert (work / ".factor_env").read_text().strip() == f"export num_features={14 if include else 13}"
        outputs.append(output)
    pd.testing.assert_frame_equal(outputs[0], outputs[1].drop(columns=RAW_STATE_SOURCE))
    assert {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths} == hashes


def test_raw_state_controls_compose_isolation_and_real_normalizer(panel_composer, monkeypatch):
    import copy
    import sys

    import yaml

    composer = panel_composer
    names = ["BaseA", "BaseB", "Candidate"]
    options = _raw_state_options(include_value=True)
    options["exclude_from_model"] = ["BaseB"]
    options["raw_state_controls"]["source_factor"] = "Candidate"
    configs = []
    for processing in (None, options, None):
        params = {"disable_alpha158": True, "observation_panel": _observation_panel(["BaseA"])}
        if processing is not None:
            params["feature_processing"] = processing
        before = copy.deepcopy(params)
        result = composer.compose_experiment_in_memory(
            factor_names=names, model_id=None, strategy_id="score_weighted_topk_v2",
            data_split=dict(RDAGENT_DEFAULT_DATA_SPLIT), custom_params=params, skip_db_save=True, execution_algo="TWAP",
        )
        assert params == before
        configs.append(yaml.safe_load(result["experiment_files"]["conf.yaml"]))
        if processing is None:
            assert "_derive_raw_state_controls" not in result["experiment_files"]["prepare_factors.py"]
        else:
            ns = {}
            exec(result["experiment_files"]["prepare_factors.py"], ns)
    assert configs[0] == configs[1] == configs[2]  # full model/label/fit/strategy/fees configuration
    pytest.importorskip("qlib", reason="Real processor contract requires the existing Qlib worker/test environment")
    from qlib.data.dataset.handler import DataHandlerLP
    from qlib.data.dataset.loader import StaticDataLoader
    from backend.services.quantevolver import qe_custom_loaders

    monkeypatch.setitem(sys.modules, "qe_custom_loaders", qe_custom_loaders)
    index = pd.MultiIndex.from_product(
        [pd.bdate_range(RDAGENT_DEFAULT_DATA_SPLIT["train_start"], RDAGENT_DEFAULT_DATA_SPLIT["test_end"]), ["000001.SZ"]],
        names=["datetime", "instrument"],
    )
    raw = pd.DataFrame({name: [float(i % 3) * -.01 for i in range(len(index))] for name in names}, index=index)
    raw.loc[index[::3], "Candidate"] = float("nan")
    derived = ns["_derive_raw_state_controls"](raw).loc[:, ns["QE_FEATURE_PROCESSING"]["model_feature_names"]]
    derived.columns = pd.MultiIndex.from_product([["feature"], derived.columns])
    derived[("label", "LABEL0")] = 1.
    handler = DataHandlerLP(
        instruments=None, data_loader=StaticDataLoader(derived),
        infer_processors=configs[1]["data_handler_config"]["infer_processors"],
        learn_processors=configs[1]["data_handler_config"]["learn_processors"],
    )
    training = handler.fetch(col_set="feature", data_key=DataHandlerLP.DK_L)
    inference = handler.fetch(col_set="feature", data_key=DataHandlerLP.DK_I)
    pd.testing.assert_frame_equal(training, inference.loc[training.index])
    assert set(RAW_STATE_COLUMNS).issubset(training.columns)
    assert training.notna().all().all()
