"""Independent fixed-endpoint Exit label/audit; no execution or package gate."""
from backend.services.advisory_model_first.generic_daily_price_input_v1 import FEATURES, KEY, ROSTER, _day
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import POLICY as ENTRY_POLICY, POLICY_SHA256 as ENTRY_POLICY_SHA256
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

HYPOTHESIS = "EXIT5-REMAINING-VALUE-AUDIT-1"
POLICY = dict(schema_version="exit_remaining_value_fixed_5td_v1", entry_policy_sha256=ENTRY_POLICY_SHA256,
    entry="ORIGINAL_TOP5_T_OPEN", endpoint="ORIGINAL_T_PLUS_4_CLOSE", decision="S_CLOSE_NEXT_U_OPEN_SCENARIO",
    reference="S_QUANTITY_EQUIVALENT_NET_LIQUIDATION", quantity="FROZEN_PROVIDER_POLICY_UNIT_STAKE",
    buy_bps=ENTRY_POLICY["buy_bps"], sell_bps=ENTRY_POLICY["sell_bps"],
    fees="S_U_E_DECLARED_SELL_SCHEDULE_ONCE_EACH", action="FIRST_POSITIVE_EXECUTABLE_ELSE_ORIGINAL_E",
    missing="KEEP_EPISODES_UNKNOWN_NO_DELAY_NO_FUTURE_FILTER", evidence_use="NAVIGATION_ONLY")
POLICY_SHA256 = sha(POLICY)
AUDIT = dict(hypothesis=HYPOTHESIS, alpha=1., blocks=5, warmup_blocks=1, max_physical_fits=4,
    embargo_complete_sessions=1, preprocessing="PAST_TRAIN_MEDIAN_FLAGS_CONTINUOUS_MEAN_STD",
    features=(*FEATURES, "remaining_session_fraction"), bootstrap_block=5, bootstrap_replicates=2000,
    bootstrap_seed=20261008, min_intervention_episodes=20, min_intervention_cohorts=20,
    min_intervention_coverage=.1, objective_contract="RISK_MANAGED_ADVISORY", decision_use="NAVIGATION_ONLY")
AUDIT_SHA256 = sha(AUDIT)
GEOMETRY = (*ROSTER, "episode_id", "entry_date", "endpoint_date", "s_date", "u_date",
            "remaining_sessions", "geometry_status", "policy_sha256")
QUOTE_FIELDS = ("trade_date", "instrument", "raw_open_cny", "raw_close_cny", "policy_price_per_raw_cny",
                "suspended", "tradability_unknown", "up_limit", "down_limit")


def ordered_calendar(values):
    days = tuple(_day(value) for value in values)
    if not days or len(days) > 5000 or list(days) != sorted(set(days)):
        raise ValueError("Exit5 needs the original unique ordered trading calendar")
    return days


__all__ = ["AUDIT", "AUDIT_SHA256", "FEATURES", "GEOMETRY", "HYPOTHESIS", "KEY", "POLICY", "POLICY_SHA256",
           "QUOTE_FIELDS", "ROSTER", "ordered_calendar"]
