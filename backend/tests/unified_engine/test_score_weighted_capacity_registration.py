from __future__ import annotations

import importlib.util
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
REGISTER_SCRIPT = PROJECT_ROOT / "scripts" / "register_score_weighted_strategy_v2_capacity_v1.py"
STRATEGY_SOURCE = PROJECT_ROOT / "scripts" / "score_weighted_strategy_v2_capacity_v1.py"


def _load_register_module():
    spec = importlib.util.spec_from_file_location("register_score_weighted_strategy_v2_capacity_v1", REGISTER_SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_capacity_strategy_registration_metadata_has_new_strategy_id_and_file():
    module = _load_register_module()

    assert module.STRATEGY_ID == "score_weighted_topk_v2_capacity_v1"
    assert module.CLASS_NAME == "ScoreWeightedTopkStrategyV2CapacityV1"
    assert module.STRATEGY_FILE == STRATEGY_SOURCE
    assert module.DEFAULT_KWARGS["max_single_order_value"] == 1_000_000_000.0


def test_capacity_strategy_param_schema_exposes_capacity_fields():
    module = _load_register_module()
    fields = {field["name"]: field for field in module.PARAM_SCHEMA}

    assert fields["max_single_order_value"]["default"] == 1_000_000_000.0
    assert fields["max_weight"]["default"] == 0.05
    assert fields["max_position_ratio"]["default"] == 0.95
