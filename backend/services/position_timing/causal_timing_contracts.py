"""Frozen PT-NEXT-024 causal research contracts.

This module is intentionally offline-only.  It owns experiment identities and
time boundaries; it has no database, runtime-card, router, or registry adapter.
"""
from __future__ import annotations

from datetime import date
from typing import Any, Mapping

from .action_value import CORE_INFORMATION_BLOCK, ActionValueError, feature_contract
from .contracts import canonical_sha256
from .core_tactical_timing import POLICY_CONTRACT_SHA256 as LEGACY_POLICY_SHA256
from .fundamental_screen import R8_DATASET_SHA256, R8_MANIFEST_FILE_SHA256
from .r8_proxy_screen import SCREEN_CONTRACT_SHA256, U0


PIPELINE_ID = "POSITION_TIMING_CAUSAL_TIMING_V1"
FOLDER = "causal_timing_v1"
REQUEST_SCHEMA = "position_timing_causal_timing_request_v1"
MANIFEST_SCHEMA = "position_timing_causal_timing_files_v1"
RECEIPT_SCHEMA = "position_timing_causal_timing_receipt_v1"
REPORT_SCHEMA = "position_timing_causal_timing_report_v1"

TRAIN_START = date(2018, 8, 1)
TRAIN_END = date(2022, 12, 31)
VALID_START = date(2023, 1, 1)
VALID_END = date(2024, 6, 30)
TEST_FIRST_EXECUTION = date(2024, 7, 1)
TEST_LAST_DECISION = date(2026, 8, 27)
TEST_LAST_EXECUTION = date(2026, 8, 28)
TERMINAL_DATE = date(2026, 8, 31)

CAPITAL_CNY = "10000000"
TACTICAL_FRACTION = "0.20"
CORE_FLOOR = "0.80"
RECOVERY_DELAY_SESSIONS = 5
LABEL_HORIZON_SESSIONS = 21
ORACLE_BUYBACK_MAX_SESSIONS = 20
ORACLE_GRID_SESSIONS = 20

BH = "BH100"
PASSIVE80 = "PASSIVE80"
PASSIVE70 = "PASSIVE70"
LEGACY_S1 = "LEGACY_S1"
LEGACY_S2 = "LEGACY_S2"
CYCLE5 = "CYCLE5_UNFILTERED_V1"
CYCLE5_RIDGE = "CYCLE5_RIDGE_V1"
CYCLE5_GBDT = "CYCLE5_GBDT_V1"
POLICY_IDS = (BH, PASSIVE80, PASSIVE70, LEGACY_S1, LEGACY_S2, CYCLE5, CYCLE5_RIDGE, CYCLE5_GBDT)
MODEL_POLICY_IDS = (CYCLE5_RIDGE, CYCLE5_GBDT)

E0 = "DAILY_CLOSE"
E1 = "MINUTE_CLOSE_PROXY"
E2 = "SCHEDULED_1000_PROXY"
EXECUTION_VIEW_IDS = (E0, E1, E2)
EXECUTION_MINUTE = {E1: "15:00", E2: "10:00"}

MARKET_FEATURES, _, CORE_FEATURE_SPEC_SHA256 = feature_contract(CORE_INFORMATION_BLOCK)
FEATURE_ORDER = MARKET_FEATURES
FEATURE_SPEC_SHA256 = canonical_sha256({
    "schema": "position_timing_causal_market_features_v1",
    "feature_order": FEATURE_ORDER,
    "source_core_feature_spec_sha256": CORE_FEATURE_SPEC_SHA256,
    "state_features_included": False,
    "imputer": "NONE_REQUIRE_FINITE",
})
RIDGE_SPEC = {
    "model_family": "RIDGE_CLOSED_FORM_V1",
    "objective": "MSE_BPS",
    "l2_alpha": 0.001,
    "fit_intercept": True,
    "standardization": "TRAIN_MEAN_STD_ZERO_VARIANCE_SCALE_ONE",
    "decision_threshold_bps": 0.0,
}
GBDT_SPEC = {
    "model_family": "LIGHTGBM_REGRESSION_V1",
    "library_version": "4.6.0",
    "objective": "regression",
    "metric": "l2",
    "num_boost_round": 100,
    "learning_rate": 0.05,
    "max_depth": 3,
    "num_leaves": 7,
    "min_data_in_leaf": 200,
    "feature_fraction": 1.0,
    "bagging_fraction": 1.0,
    "bagging_freq": 0,
    "lambda_l2": 1.0,
    "seed": 20260919,
    "deterministic": True,
    "force_col_wise": True,
    "num_threads": 1,
    "decision_threshold_bps": 0.0,
}

CONTRACT: dict[str, Any] = {
    "pipeline_id": PIPELINE_ID,
    "candidate": {
        "manifest_file_sha256": R8_MANIFEST_FILE_SHA256,
        "canonical_sha256": R8_DATASET_SHA256,
    },
    "screen": {"id": U0, "contract_sha256": SCREEN_CONTRACT_SHA256},
    "legacy_policy_sha256": LEGACY_POLICY_SHA256,
    "capital_cny": CAPITAL_CNY,
    "account": "INDEPENDENT_CASH_NO_LEVERAGE_NO_INJECTION",
    "time": {
        "train": [TRAIN_START.isoformat(), TRAIN_END.isoformat()],
        "validation": [VALID_START.isoformat(), VALID_END.isoformat()],
        "test_first_execution": TEST_FIRST_EXECUTION.isoformat(),
        "test_last_decision": TEST_LAST_DECISION.isoformat(),
        "test_last_execution": TEST_LAST_EXECUTION.isoformat(),
        "terminal": TERMINAL_DATE.isoformat(),
        "decision_time": "20:00_ASIA_SHANGHAI",
    },
    "cycle": {
        "tactical_fraction": TACTICAL_FRACTION,
        "core_floor": CORE_FLOOR,
        "recovery_delay_sessions": RECOVERY_DELAY_SESSIONS,
        "label_horizon_sessions": LABEL_HORIZON_SESSIONS,
    },
    "policies": list(POLICY_IDS),
    "execution_views": list(EXECUTION_VIEW_IDS),
    "execution_minutes": EXECUTION_MINUTE,
    "feature_order": list(FEATURE_ORDER),
    "feature_spec_sha256": FEATURE_SPEC_SHA256,
    "feature_input_basis": "RAW_CNY_RAW_VOLUME_WITH_FACTOR_FOR_RETURN_NORMALIZATION",
    "models": {"ridge": RIDGE_SPEC, "gbdt": GBDT_SPEC},
    "oracle": {
        "event_buyback_max_sessions": ORACLE_BUYBACK_MAX_SESSIONS,
        "restricted_grid_sessions": ORACLE_GRID_SESSIONS,
        "policy_access": False,
    },
    "statistics": {
        "formal_comparison_count": 4,
        "endpoint_count": 2,
        "family_size": 8,
        "alpha": 0.05,
        "bootstrap_replicates": 5000,
        "bootstrap_block_sessions": 25,
        "seed": 20260919,
        "economic_threshold_bps": 0.0,
    },
    "result_class": "EXPLORATORY_HYPOTHESIS_GENERATED",
    "selected_for_live": 0,
    "database_read": False,
    "database_write": False,
    "market_network_accessed": False,
    "runtime_action_performed": False,
    "service_process_control_performed": False,
}
CONTRACT_SHA256 = canonical_sha256(CONTRACT)
FEATURE_INPUT_BASIS_SHA256 = canonical_sha256(
    {"feature_order": FEATURE_ORDER, "feature_spec_sha256": FEATURE_SPEC_SHA256,
     "input_basis": CONTRACT["feature_input_basis"]}
)


def validate_contract(payload: Mapping[str, Any]) -> None:
    """Fail closed when a frozen request does not carry the exact contract."""
    if payload.get("contract_sha256") != CONTRACT_SHA256:
        raise ActionValueError("CAUSAL_TIMING_CONTRACT_DRIFT")
    if canonical_sha256(payload.get("contract")) != CONTRACT_SHA256:
        raise ActionValueError("CAUSAL_TIMING_CONTRACT_PAYLOAD_DRIFT")


def split_for_decision(day: date, *, label_available_at: date | None = None) -> str:
    """Return a temporal split only when its label has matured inside the split."""
    if TRAIN_START <= day <= TRAIN_END:
        split, boundary = "TRAIN", TRAIN_END
    elif VALID_START <= day <= VALID_END:
        split, boundary = "VALIDATION", VALID_END
    elif day < TEST_FIRST_EXECUTION:
        return "OUTSIDE"
    elif day <= TEST_LAST_DECISION:
        return "TEST"
    else:
        return "OUTSIDE"
    if label_available_at is None or label_available_at > boundary:
        return "IMMATURE"
    return split
