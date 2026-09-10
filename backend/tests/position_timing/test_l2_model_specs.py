import numpy as np
import pandas as pd

from backend.services.position_timing.contracts import (
    POSITION_TIMING_L2_RESEARCH_CONTRACT_V1,
    canonical_sha256,
)
from backend.services.position_timing.learnability_pipeline import (
    MODEL_ORDER,
    build_l2_cpcv_paths,
    frozen_model_runtime_identities,
    frozen_numeric_runtime_identity,
    run_l2_crossfit,
)


def test_frozen_l2_model_specs_have_exact_two_model_identity() -> None:
    contract = POSITION_TIMING_L2_RESEARCH_CONTRACT_V1
    assert tuple(model.model_id for model in contract.models) == MODEL_ORDER
    assert contract.hypothesis_count == 2
    assert contract.cross_validation_spec["parameter_search"] is False
    assert contract.cross_validation_spec["final_refit"] is False
    identities = frozen_model_runtime_identities()
    assert tuple(identities) == MODEL_ORDER
    assert identities["SKLEARN_RIDGE_V1"]["package_version"] == "1.8.0"
    assert identities["LIGHTGBM_GBDT_V1"]["package_version"] == "4.6.0"
    assert identities["SKLEARN_RIDGE_V1"]["get_params_deep_false"]["alpha"] == 100
    assert identities["LIGHTGBM_GBDT_V1"]["get_params_deep_false"]["n_estimators"] == 300
    assert identities["LIGHTGBM_GBDT_V1"]["early_stopping"] is False
    assert identities["SKLEARN_RIDGE_V1"]["identity_sha256"] != identities["LIGHTGBM_GBDT_V1"]["identity_sha256"]
    numeric = frozen_numeric_runtime_identity()
    assert numeric["packages"]["scikit-learn"] == "1.8.0"
    assert numeric["packages"]["lightgbm"] == "4.6.0"
    assert numeric["identity_sha256"] == canonical_sha256(
        {key: value for key, value in numeric.items() if key != "identity_sha256"}
    )


def test_ridge_crossfit_produces_exact_seven_oof_predictions() -> None:
    calendar = pd.bdate_range("2018-08-01", periods=1700)
    cohorts = calendar[::20][:80]
    feature_order = POSITION_TIMING_L2_RESEARCH_CONTRACT_V1.feature_order
    records = []
    for cohort_index, cohort in enumerate(cohorts):
        position = calendar.get_loc(cohort)
        for sign in (-1.0, 1.0):
            values = {
                feature: float(cohort_index + offset + sign / 10.0) for offset, feature in enumerate(feature_order)
            }
            values.update(
                {
                    "entry_decision_date": cohort,
                    "entry_trade_date": calendar[position + 1],
                    "effective_terminal_trade_date": calendar[position + 25],
                    "target_available": True,
                    "full_exit_incremental_net_value_bps": sign * (10 + cohort_index / 10),
                }
            )
            records.append(values)
    rows = pd.DataFrame(records)
    rows.attrs["trading_calendar"] = [value.isoformat() for value in calendar]
    paths = build_l2_cpcv_paths(rows, request_sha256="3" * 64)
    result = run_l2_crossfit(rows=rows, paths=paths, model_id="SKLEARN_RIDGE_V1")
    assert np.all(result.oof_counts == 7)
    assert np.isfinite(result.predictions).all()
    assert set(result.target_exposures).issubset({0.0, 0.25, 0.5, 1.0})
