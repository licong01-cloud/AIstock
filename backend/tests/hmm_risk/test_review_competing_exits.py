import pandas as pd
import pytest

from scripts.hmm_risk.review_competing_exits import INPUT, review as exits


def exit_bars():
    return pd.DataFrame([["a", "sell", f"2026-05-12 09:3{i}", "688696.SH", 0, 689, 746, 3,
                          True, 10000, n] for i, n in enumerate([229, 230, 230])], columns=INPUT)


def test_zero_target_uses_the_revised_opening_balance_before_execution():
    r = exits(exit_bars().iloc[::-1])
    assert r.status.eq("VERIFIED").all() and r.revised_sold.tolist() == [248, 249, 249]
    assert exits(exit_bars().assign(target_quantity=1)).status.eq("OUTSIDE_ZERO_TARGET_TWAP").all()


def test_zero_target_requires_original_parity_and_real_incremental_volume():
    data = exit_bars()
    data.loc[0, "original_sold"] = 228
    assert exits(data).status.eq("ORIGINAL_NOT_REPRODUCED").all()
    insufficient = exits(exit_bars().assign(volume_shares=1000))
    assert insufficient.status.eq("EXTRA_VOLUME_UNVERIFIED").all() and insufficient.revised_sold.isna().all()
    with pytest.raises(ValueError, match="duplicate"):
        exits(pd.concat([data, data.iloc[:1]]))
