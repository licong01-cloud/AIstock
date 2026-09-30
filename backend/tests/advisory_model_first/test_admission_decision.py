from __future__ import annotations

from datetime import date

import pandas as pd
import pytest
from pydantic import ValidationError

from backend.services.advisory_model_first.score_hmm_admission_contracts import (
    PACKAGE_SCORE_CALIBRATION_ONLY,
    SCORE_HMM_EXECUTABLE_ARM_IDS,
    SCORE_HMM_SOURCE_UNAVAILABLE_ARM_IDS,
    SCORE_PLUS_MARKET_HMM,
    SCORE_PLUS_RAW_MARKET_SHAPE,
    AdvisoryAdmissionDecisionV1,
)
from backend.services.advisory_model_first.score_hmm_admission_pipeline import build_admission_decisions
from backend.tests.advisory_model_first.test_score_hmm_admission_pipeline import build_test_request


def _parent() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "decision_as_of_trade_date": pd.Timestamp("2025-01-02"),
            "target_trade_date": pd.Timestamp("2025-01-03"),
            "instrument": [f"{index:06d}.SZ" for index in range(1, 7)],
            "selection_effective_rank": range(1, 7),
        }
    )


def _oof() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for arm_id in SCORE_HMM_EXECUTABLE_ARM_IDS:
        for rank in range(1, 7):
            if arm_id == SCORE_PLUS_MARKET_HMM:
                status, lower, probability = "SOURCE_UNAVAILABLE", None, None
            elif arm_id == PACKAGE_SCORE_CALIBRATION_ONLY and rank == 1:
                status, lower, probability = "AVAILABLE", 2.0, 0.7
            else:
                status, lower, probability = "AVAILABLE", -1.0, 0.7
            rows.append(
                {
                    "arm_id": arm_id,
                    "decision_as_of_trade_date": pd.Timestamp("2025-01-02"),
                    "target_trade_date": pd.Timestamp("2025-01-03"),
                    "instrument": f"{rank:06d}.SZ",
                    "selection_effective_rank": rank,
                    "prediction_status": status,
                    "expected_net_return_bps_primary": 3.0 if status == "AVAILABLE" else None,
                    "expected_net_return_lcb80_bps_primary": lower,
                    "positive_probability_primary": probability,
                    "primary_target_known": not (arm_id == PACKAGE_SCORE_CALIBRATION_ONLY and rank == 1),
                }
            )
    return pd.DataFrame(rows)


def test_admission_keeps_exact_parent_top5_allows_zero_to_five_and_never_backfills_rank6() -> None:
    request = build_test_request().model_copy(update={"expected_top5_row_count": 5})
    decisions = build_admission_decisions(oof_predictions=_oof(), parent_top20=_parent(), request=request)

    assert len(decisions) == 25
    assert decisions["parent_rank"].max() == 5
    assert set(decisions["instrument"]) <= {f"{index:06d}.SZ" for index in range(1, 6)}
    take_counts = decisions.groupby("arm_id")["action"].apply(lambda values: int(values.eq("TAKE").sum()))
    assert take_counts[PACKAGE_SCORE_CALIBRATION_ONLY] == 1
    assert take_counts[SCORE_PLUS_RAW_MARKET_SHAPE] == 0
    assert decisions.loc[
        decisions["arm_id"].eq(SCORE_PLUS_RAW_MARKET_SHAPE), "day_state"
    ].eq("NO_ELIGIBLE_RECOMMENDATION").all()
    assert decisions.loc[decisions["arm_id"].eq(SCORE_PLUS_MARKET_HMM), "day_state"].eq(
        "ADMISSION_UNAVAILABLE"
    ).all()
    assert set(decisions.loc[decisions["arm_id"].isin(SCORE_HMM_SOURCE_UNAVAILABLE_ARM_IDS), "reason_code"]) == {
        "NOT_RUN_SOURCE_UNAVAILABLE"
    }


def test_action_is_independent_of_future_label_availability() -> None:
    request = build_test_request().model_copy(update={"expected_top5_row_count": 5})
    decisions = build_admission_decisions(oof_predictions=_oof(), parent_top20=_parent(), request=request)
    row = decisions.loc[
        decisions["arm_id"].eq(PACKAGE_SCORE_CALIBRATION_ONLY) & decisions["parent_rank"].eq(1)
    ].iloc[0]
    assert row["action"] == "TAKE"
    assert not bool(row["label_evaluable"])
    assert row["evaluation_reason_code"] == "LABEL_NOT_EVALUABLE"


def test_take_contract_rejects_nonpositive_lower_bound() -> None:
    with pytest.raises(ValidationError):
        AdvisoryAdmissionDecisionV1(
            request_sha256="1" * 64,
            arm_id=PACKAGE_SCORE_CALIBRATION_ONLY,
            decision_as_of_trade_date=date(2025, 1, 2),
            target_trade_date=date(2025, 1, 3),
            instrument="000001.SZ",
            parent_rank=1,
            action="TAKE",
            reason_code="TAKE_POSITIVE_VALUE",
            arm_available=True,
            label_evaluable=True,
            evaluation_reason_code=None,
            primary_expected_net_return_bps=1.0,
            primary_expected_net_return_lcb80_bps=0.0,
            primary_positive_probability=0.7,
        )
