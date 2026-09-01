from __future__ import annotations

import pandas as pd

from backend.services.advisory_model_first.policy_cpcv import calculate_policy_pbo


def test_pbo_uses_complete_complementary_block_partitions() -> None:
    scores = pd.DataFrame(
        [
            {
                "trial_id": trial,
                "block_id": block,
                "mean_net_excess_return_bps": value,
            }
            for trial, values in (("stable", [2, 2, 2, 2]), ("unstable", [9, 9, -9, -9]))
            for block, value in enumerate(values)
        ]
    )
    receipt = calculate_policy_pbo(scores, group_count=4)
    assert receipt["status"] == "COMPUTED"
    assert receipt["partition_count"] == 6
    assert 0.0 <= receipt["pbo"] <= 1.0


def test_pbo_is_explicitly_not_computable_for_one_trial() -> None:
    scores = pd.DataFrame(
        [
            {"trial_id": "only", "block_id": block, "mean_net_excess_return_bps": 1.0}
            for block in range(4)
        ]
    )
    receipt = calculate_policy_pbo(scores, group_count=4)
    assert receipt["status"] == "NOT_COMPUTABLE"
    assert receipt["reason_code"] == "NOT_COMPUTABLE_INSUFFICIENT_TRIALS"
