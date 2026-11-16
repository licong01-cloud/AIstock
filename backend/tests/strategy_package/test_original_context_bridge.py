from __future__ import annotations

import hashlib
import json

import pytest
import yaml

from backend.services.strategy_package.original_context_bridge import context_from_artifacts
from backend.services.trading_core.errors import StrategyPackageValidationError


def fixture_files():
    selection = {"instruments_file": "stock_universe.txt", "instrument_name": "stock_universe",
                 "instruments_sha256": "a" * 64, "mode": "stock_universe", "pool_ids": [],
                 "membership_revision": "pit-frozen"}
    conf = {"market": "stock_universe", "data_handler_config": {"data_loader": {"kwargs": {"label_horizon": 20}}},
            "port_analysis_config": {"strategy": {"kwargs": {"topk": 20, "n_drop": 1, "max_n_drop": 2}},
                                     "executor": {"kwargs": {"inner_strategy": {"class": "TailTWAPWithLimitStrategy",
                                                  "module_path": "tail_twap_strategy", "kwargs": {"unfilled_handler": "TAIL_BOOST"}}}},
                                     "backtest": {"exchange_kwargs": {"min_cost": 5}}}}
    values = {"conf.yaml": yaml.safe_dump(conf),
              "qe_direct_v2_dataset_binding.json": json.dumps({"selection_pins": selection}),
              "qe_frozen_build_spec.json": json.dumps({"pins": {"instruments_sha256": "a" * 64}})}
    return {k: {"content": v.encode(), "sha256": hashlib.sha256(v.encode()).hexdigest()} for k, v in values.items()}


def bridge(files=None, **overrides):
    args = dict(run_id="run", child_id="child", attempt_id="selected", files=files or fixture_files(), topk=20,
                original_config={}, algo_resolver=lambda inner: "TWAP")
    args.update(overrides)
    return context_from_artifacts(**args)


def test_extracts_exact_original_pins_preserves_drop_semantics_and_source():
    original = {"immutable": "retained"}
    config, receipt = bridge(original_config=original)
    assert original == {"immutable": "retained"}
    assert config["n_drop"] == 1
    assert receipt["daily_strategy"]["max_n_drop"] == 2
    assert config["label_horizon"] == 20
    assert config["filtered_pool"]["membership_revision"] == "pit-frozen"
    assert receipt["legacy_filtered_pool_alias"] == "UNKNOWN"
    assert receipt["source_rows_modified"] is False
    assert receipt == bridge(original_config=original)[1]


def test_rejects_corrupt_or_conflicting_original_context():
    files = fixture_files()
    files["conf.yaml"]["sha256"] = "0" * 64
    with pytest.raises(StrategyPackageValidationError, match="SHA"):
        bridge(files)
    with pytest.raises(StrategyPackageValidationError, match="conflicts"):
        bridge(original_config={"n_drop": 2})
    with pytest.raises(StrategyPackageValidationError, match="topk"):
        bridge(topk=50)
    with pytest.raises(StrategyPackageValidationError, match="legacy"):
        bridge(original_config={"filtered_pool": "unproven"})


def test_original_retired_execution_still_rejected():
    with pytest.raises(Exception, match="retired"):
        bridge(algo_resolver=lambda inner: "V25_TWO_STAGE")
