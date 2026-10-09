import numpy as np
import pytest

from backend.services.advisory_model_first.cross_package_validation_statistics_v1 import (
    holm_adjust, paired_skip_attribution, synchronous_block_inference,
)


def test_correlated_package_columns_share_days_and_no_null_compression():
    days = list(range(120))
    path = np.random.default_rng(3).normal(20, 10, 120)
    values = np.column_stack([path, path, path])
    values[20, 2] = np.nan
    result = synchronous_block_inference(daily_values=values, original_days=days, expected_days=days,
        block_span=5, replicates=1000)
    assert result["intervals"][0] == result["intervals"][1]
    assert result["intervals"][2] is None and result["family_size"] == 3
    assert result["adjusted_pvalues"][2] == 1
    with pytest.raises(ValueError, match="axis"):
        synchronous_block_inference(daily_values=values[1:], original_days=days[1:], expected_days=days, block_span=5)


def test_skip_loss_missed_profit_unknown_cash_are_not_mixed():
    result = paired_skip_attribution(baseline_net_bps=[-20, 30, -40, 10], model_net_bps=[0, 0, 0, 10],
        known_skip=[True, True, False, False], unknown_skip=[False, False, True, False])
    assert result["known_avoided_loss_bps"] == 20 and result["known_missed_profit_bps"] == 30
    assert result["unknown_cash_increment_bps"] == 40 and result["total_increment_bps"] == 30
    assert np.allclose(holm_adjust([.01, .04, 1]), [.03, .08, 1])
def test_all_columns_have_holes_returns_stable_unknown_statistics():
    import numpy as np
    from backend.services.advisory_model_first.cross_package_validation_statistics_v1 import synchronous_block_inference
    days = [f"2026-03-{i:02}" for i in range(1, 21)]
    values = np.zeros((20, 2))
    values[1, :] = np.nan
    result = synchronous_block_inference(daily_values=values, original_days=days, expected_days=days,
        block_span=5, replicates=1000)
    assert result['status'] == 'INCOMPLETE_AXIS'
    assert result['mde_80pct_family_approx_bps'] == [None, None]
    assert result['adjusted_pvalues'] == [1., 1.]


def test_short_axis_and_related_package_aggregates_keep_unknowns():
    from backend.services.advisory_model_first.cross_package_validation_statistics_v1 import package_axis_aggregates
    short = synchronous_block_inference(daily_values=[[1.], [2.]], original_days=[1,2], expected_days=[1,2],
        block_span=5, replicates=1000)
    assert short["status"] == "INSUFFICIENT_AXIS" and short["intervals"] == [None]
    packages = ["same_a", "same_b", "independent"]
    result = package_axis_aggregates(daily_values=[[10.,10.,40.],[10.,np.nan,40.]],
        package_ids=packages, expected_packages=packages, source_groups={"same_a":"parent", "same_b":"parent", "independent":"other"})
    assert result["complete_day_count"] == 1
    assert result["equal_package_daily"][0] == 20. and result["equal_parent_cluster_daily"][0] == 25.
    assert np.isnan(result["equal_package_daily"][1]) and np.isnan(result["equal_parent_cluster_daily"][1])
    with pytest.raises(ValueError,match="full declared package"):
        package_axis_aggregates(daily_values=[[10.,40.]], package_ids=packages[1:], expected_packages=packages,
            source_groups={"same_b":"parent", "independent":"other"})
