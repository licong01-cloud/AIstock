from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pytest
import yaml

from backend.services.quantevolver.trend_entry_consumer import (
    BASE_DEPENDENCIES, PARAM, apply_trend_entry, bind_trend_entry_hooks, extract_trend_entry,
)
from scripts.qe_trend_entry_admission import (
    BASE_SOURCE_SHA, CONTRACT, MANIFEST_FILE, PANEL_FILE, SCHEMA, TrendEntryContractError,
    canonical_sha, seal, validate_manifest,
)

ROOT = Path(__file__).resolve().parents[3]


def fixture(tmp_path):
    binding = {"schema_version": "qe_direct_v2_dataset_binding_v3", "candidate_root": "/release",
               "provider_uri_day": "/release/day", "provider_uri_1min": "/release/min",
               "release_id": "test-frozen", "day_pins": {"hash": "a"}, "minute_pins": {"hash": "b"},
               "selection_pins": {"instrument_name": "stock_universe", "coverage_receipt_sha256": "old"}}
    port = {"strategy": {"class": "SuspendFilterScoreWeightedTopkStrategyV2",
                         "module_path": "qe_suspend_filter_score_weighted_strategy",
                         "kwargs": {"topk": 20, "min_n_drop": 0, "enable_dynamic_ndrop": True}},
            "executor": {"kwargs": {"inner_strategy": {"class": "TailTWAPWithLimitStrategy", "module_path": "tail_twap_strategy",
                                                        "kwargs": {"unfilled_handler": "TAIL_BOOST"}}}},
            "backtest": {"start_time": "2024-07-01", "end_time": "2026-09-30", "account": 10_000_000}}
    config = {"port_analysis_config": port, "market": "stock_universe",
              "qlib_init": {"provider_uri": {"day": "/release/day", "1min": "/release/min"}},
              "task": {"record": [{"class": "PortAnaRecord", "kwargs": {"config": deepcopy(port)}}]}}
    files = {name: (ROOT / "scripts" / name).read_text(encoding="utf-8") for name in BASE_DEPENDENCIES}
    files.update({"conf.yaml": yaml.safe_dump(config), "custom_strategy.py":
                  (ROOT / "scripts/score_weighted_strategy_v2_deterministic_v1.py").read_text(encoding="utf-8")})
    asset = tmp_path / PANEL_FILE
    asset.write_bytes(b"transport-test-not-runtime-admission")
    manifest = seal({"schema_version": SCHEMA, "contract": CONTRACT, "arm": "S10", "seed": 42,
                     "policy": "post_decision_natural_buy_finite_r20_r60_positive", "source_sha256": BASE_SOURCE_SHA,
                     "dataset_binding": binding, "dataset_manifest_identity": "1" * 64,
                     "dataset_manifest_file_sha256": "2" * 64,
                     "original_port_sha256": canonical_sha(port), "source_receipts": {"research": "3" * 64},
                     "panel": {"path": PANEL_FILE, "sha256": hashlib.sha256(asset.read_bytes()).hexdigest(),
                               "rows": 2_682_417, "days": 549, "start": "2024-07-01", "end": "2026-09-30",
                               "index_sha256": "4" * 64},
                     "account": {"role": "final", "start": "2024-07-01", "end": "2026-09-30", "days": 549}})
    return {"experiment_files": files, "direct_v2_dataset_binding": deepcopy(binding)}, manifest, str(asset)


def test_absent_parameter_is_identity_and_does_not_read_assets():
    result = {"sentinel": object()}
    assert apply_trend_entry(result, None, scripts_dir=Path("nonexistent")) is result
    assert extract_trend_entry({"existing": 1}) == ({"existing": 1}, None)


def test_exact_original_source_restored_and_changed_source_rejected():
    original = (ROOT / "scripts/score_weighted_strategy_v2_deterministic_v1.py").read_text(encoding="utf-8")
    adapted, receipt = bind_trend_entry_hooks(original)
    assert receipt["restored_ast_equal"] and receipt["observation_hooks"] == 3
    assert "self._qe_record_trend_slot" in adapted and "self._qe_capture_trend_context" in adapted
    with pytest.raises(TrendEntryContractError, match="BASE_SOURCE_DRIFT"):
        bind_trend_entry_hooks(original + "\n# changed\n")


def test_composer_transport_and_both_record_configs_consume_same_optional_policy(tmp_path):
    result, manifest, path = fixture(tmp_path)
    params, study = extract_trend_entry({PARAM: manifest, "qe_trend_entry_panel_path": path, "random_seed": 42})
    assert params == {"random_seed": 42}
    source = deepcopy(manifest)
    result["direct_v2_dataset_binding"]["selection_pins"]["coverage_receipt_sha256"] = "new-consumer"
    out = apply_trend_entry(result, study, scripts_dir=ROOT / "scripts")
    config = yaml.safe_load(out["experiment_files"]["conf.yaml"])
    port = config["port_analysis_config"]
    assert port == config["task"]["record"][0]["kwargs"]["config"]
    assert port["strategy"]["class"] == "TrendEntryScoreWeightedTopkStrategyV2"
    assert port["executor"]["kwargs"]["inner_strategy"]["kwargs"]["unfilled_handler"] == "TAIL_BOOST"
    transported = validate_manifest(json.loads(out["experiment_files"][MANIFEST_FILE]))
    assert transported["execution_files_sha256"]["tail_twap_strategy.py"] == BASE_DEPENDENCIES["tail_twap_strategy.py"]
    assert manifest == source
    assert PANEL_FILE + ".b64" in out["experiment_files"]


@pytest.mark.parametrize("defect", ["binding", "window", "source", "tail", "asset", "policy"])
def test_drift_fails_composer_before_execution(tmp_path, defect):
    result, m, path = fixture(tmp_path)
    if defect == "binding":
        result["direct_v2_dataset_binding"]["release_id"] = "other"
    elif defect == "source":
        result["experiment_files"]["custom_strategy.py"] += "\n"
    elif defect == "asset":
        Path(path).write_bytes(b"wrong")
    else:
        config = yaml.safe_load(result["experiment_files"]["conf.yaml"])
        port = config["port_analysis_config"]
        if defect == "window":
            port["backtest"]["end_time"] = "2026-09-29"
        elif defect == "tail":
            port["executor"]["kwargs"]["inner_strategy"]["kwargs"]["unfilled_handler"] = "TAIL_SUBSTITUTE"
        else:
            port["strategy"]["kwargs"]["min_n_drop"] = 1
        m["original_port_sha256"] = canonical_sha(port)
        m = seal(m)
        result["experiment_files"]["conf.yaml"] = yaml.safe_dump(config)
    with pytest.raises(TrendEntryContractError):
        apply_trend_entry(result, (m, path), scripts_dir=ROOT / "scripts")


def test_conflicting_consumers_and_unsigned_change_rejected(tmp_path):
    _, m, path = fixture(tmp_path)
    with pytest.raises(TrendEntryContractError, match="OTHER_OVERLAY"):
        extract_trend_entry({PARAM: m, "qe_trend_entry_panel_path": path, "qe_l2_natural_entry_manifest": {"x": 1}})
    m["seed"] = 1
    with pytest.raises(TrendEntryContractError, match="MANIFEST_HASH"):
        validate_manifest(m)
