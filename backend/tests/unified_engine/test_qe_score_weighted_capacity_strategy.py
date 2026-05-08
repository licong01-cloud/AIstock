from __future__ import annotations

import ast
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
COMPOSER_SOURCE = (PROJECT_ROOT / "backend" / "services" / "quantevolver" / "config_composer.py").read_text(
    encoding="utf-8"
)
LEGACY_REGISTER_PATH = PROJECT_ROOT / "scripts" / "register_score_weighted_strategy_v2.py"
CAPACITY_REGISTER_PATH = PROJECT_ROOT / "scripts" / "register_score_weighted_strategy_v2_capacity_v1.py"


def _literal_assignment(path: Path, name: str):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == name:
                    return ast.literal_eval(node.value)
    raise AssertionError(f"{name} assignment not found in {path}")


def test_legacy_score_weighted_v2_registration_defaults_remain_unchanged() -> None:
    defaults = _literal_assignment(LEGACY_REGISTER_PATH, "DEFAULT_KWARGS")
    schema = _literal_assignment(LEGACY_REGISTER_PATH, "PARAM_SCHEMA")

    assert _literal_assignment(LEGACY_REGISTER_PATH, "STRATEGY_ID") == "score_weighted_topk_v2"
    assert defaults["max_weight"] == 0.05
    assert defaults["max_position_ratio"] == 0.95
    assert "max_single_order_value" not in defaults
    assert {item["name"] for item in schema} >= {"max_weight", "max_position_ratio"}


@pytest.mark.xfail(
    not CAPACITY_REGISTER_PATH.exists(),
    reason=(
        "Agent B capacity strategy asset registration is not present yet; "
        "new strategy must use a new strategy_id and expose capacity schema."
    ),
    strict=True,
)
def test_capacity_strategy_registration_schema_exposes_capacity_fields() -> None:
    defaults = _literal_assignment(CAPACITY_REGISTER_PATH, "DEFAULT_KWARGS")
    schema = _literal_assignment(CAPACITY_REGISTER_PATH, "PARAM_SCHEMA")
    schema_names = {item["name"] for item in schema}

    assert _literal_assignment(CAPACITY_REGISTER_PATH, "STRATEGY_ID") == "score_weighted_topk_v2_capacity_v1"
    assert defaults["max_single_order_value"] == 1_000_000_000.0
    assert defaults["max_weight"] == 0.05
    assert defaults["max_position_ratio"] == 0.95
    assert {"max_single_order_value", "max_weight", "max_position_ratio"} <= schema_names


def test_config_composer_keeps_capacity_params_as_strategy_kwargs() -> None:
    allowed_block_start = COMPOSER_SOURCE.index("_SCORE_WEIGHTED_TOPK_ALLOWED_KEYS")
    allowed_block = COMPOSER_SOURCE[allowed_block_start : allowed_block_start + 900]
    non_strategy_block_start = COMPOSER_SOURCE.index("_NON_STRATEGY_PARAMS")
    non_strategy_block_end = COMPOSER_SOURCE.index("} | _PTNN_HP_KEYS", non_strategy_block_start)
    non_strategy_block = COMPOSER_SOURCE[non_strategy_block_start:non_strategy_block_end]

    for field in ("max_single_order_value", "max_weight", "max_position_ratio"):
        assert field in allowed_block
        assert field not in non_strategy_block
