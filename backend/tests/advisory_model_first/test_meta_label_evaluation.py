from __future__ import annotations

import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.meta_label_evaluation import evaluate_meta_label_validation_blocks


def test_meta_label_evaluator_requires_exact_validation_date_coverage() -> None:
    with pytest.raises(AdvisoryModelFirstError) as excinfo:
        evaluate_meta_label_validation_blocks(
            rankings=pd.DataFrame(),
            predictions=pd.DataFrame(
                {"decision_as_of_trade_date": [pd.Timestamp("2026-01-02")], "instrument": ["000001.SZ"]}
            ),
            validation_blocks=[0],
            block_by_date={"2026-01-02": 0, "2026-01-05": 0},
            daily=pd.DataFrame(),
            benchmark_daily=pd.DataFrame(),
            suspend_rows=pd.DataFrame(),
            trading_calendar=[],
            policy=None,
            policy_sha256="a" * 64,
            cost_policy=None,
            request_id="x",
        )
    assert excinfo.value.reason_code == "ADVISORY_META_LABEL_EVALUATION_INVALID"
