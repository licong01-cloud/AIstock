from __future__ import annotations

import pandas as pd

from backend.services.advisory_model_first.policy_contracts import AdvisoryPolicySplitV1
from backend.services.advisory_model_first.policy_cpcv import build_policy_cpcv_paths


def test_cpcv_purges_real_information_intervals_and_enumerates_paths() -> None:
    dates = pd.bdate_range("2026-01-02", periods=32)
    labels = pd.DataFrame(
        [
            {
                "decision_as_of_trade_date": decision,
                "label_information_start": decision,
                "label_information_end": dates[min(index + 2, len(dates) - 1)],
                "label_status": "MATURED",
                "take_label": index % 2,
            }
            for index, decision in enumerate(dates[:24])
        ]
    )
    policy = AdvisoryPolicySplitV1(group_count=4, validation_group_count=1, embargo_trading_days=0)
    result = build_policy_cpcv_paths(
        labels,
        split_policy=policy,
        trading_calendar=dates,
        request_sha256="a" * 64,
    )
    assert len(result.paths) == 4
    assert all(path["status"] == "READY" for path in result.paths)
    assert any(path["purged_dates"] for path in result.paths)
    assert len(result.block_by_date) == 24
