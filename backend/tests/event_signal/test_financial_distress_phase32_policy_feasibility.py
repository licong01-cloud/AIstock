import importlib.util
import sys
from pathlib import Path


_SCRIPT_PATH = Path(__file__).resolve().parents[3] / "scripts" / "financial_distress_phase32_direct_risk_policy_feasibility.py"
_SPEC = importlib.util.spec_from_file_location("financial_distress_phase32_direct_risk_policy_feasibility", _SCRIPT_PATH)
phase32 = importlib.util.module_from_spec(_SPEC)
assert _SPEC and _SPEC.loader
sys.modules[_SPEC.name] = phase32
_SPEC.loader.exec_module(phase32)


def test_phase32_policy_decision_marks_persistent_direct_downside_candidate():
    decision = phase32.policy_decision(
        "rule_a",
        {
            20: {
                "valid_returns": 80,
                "median_return": -0.035,
                "negative_return_rate": 0.66,
                "p25_return": -0.08,
                "loss_10pct_rate": 0.22,
                "missing_price_rate": 0.05,
            },
            60: {
                "valid_returns": 70,
                "median_return": -0.045,
                "negative_return_rate": 0.70,
                "p25_return": -0.11,
                "loss_10pct_rate": 0.25,
                "missing_price_rate": 0.10,
            },
        },
    )

    assert decision["direct_policy_decision"] == "RISK_DOWNWEIGHT_CANDIDATE"
    assert decision["policy_shape"] == "avoid_new_buy_60td"
    assert decision["action_boundary"] == "no_hard_ban_no_forced_sell_research_only"


def test_phase32_policy_decision_rejects_positive_or_mixed_direct_returns():
    decision = phase32.policy_decision(
        "rule_b",
        {
            20: {
                "valid_returns": 120,
                "median_return": 0.015,
                "negative_return_rate": 0.45,
                "p25_return": -0.02,
                "loss_10pct_rate": 0.05,
                "missing_price_rate": 0.02,
            },
            60: {
                "valid_returns": 110,
                "median_return": 0.025,
                "negative_return_rate": 0.42,
                "p25_return": -0.01,
                "loss_10pct_rate": 0.03,
                "missing_price_rate": 0.05,
            },
        },
    )

    assert decision["direct_policy_decision"] == "REJECT_OR_MIXED"
    assert decision["policy_shape"] == "watchlist_no_policy"


def test_phase32_aggregate_return_rows_computes_tail_and_missing_rates():
    rows = [
        {"rule_key": "r1", "window": 20, "ret": -0.12, "missing_price": False, "missing_benchmark": False},
        {"rule_key": "r1", "window": 20, "ret": -0.04, "missing_price": False, "missing_benchmark": False},
        {"rule_key": "r1", "window": 20, "ret": 0.02, "missing_price": False, "missing_benchmark": False},
        {"rule_key": "r1", "window": 20, "ret": None, "missing_price": True, "missing_benchmark": False},
    ]

    [agg] = phase32.aggregate_return_rows(rows, group_fields=("rule_key", "window"), return_field="ret")

    assert agg["rows"] == 4
    assert agg["valid_returns"] == 3
    assert agg["negative_return_rate"] == 2 / 3
    assert agg["loss_10pct_rate"] == 1 / 3
    assert agg["missing_price_rate"] == 1 / 4
