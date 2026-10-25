"""Projected-source safety, not duplicate GP5 label construction tests."""
import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError

from backend.services.advisory_model_first.generic_moneyflow_price_5td_contracts_v1 import (
    FEATURES, KEY, MONEYFLOW_FEATURES, POLICY, POLICY_SHA256, ROSTER, GenericPrice5TDConfigurationV1,
)
from backend.services.advisory_model_first.generic_moneyflow_price_5td_source_v1 import (
    join_moneyflow_features_v1, moneyflow_values, read_moneyflow_development_v1,
)


def development_frames():
    calendar = pd.bdate_range("2024-07-01", periods=90)
    config = GenericPrice5TDConfigurationV1(train_start=calendar[0].date(), train_end=calendar[39].date(),
        validation_start=calendar[40].date(), validation_end=calendar[59].date(), test_start=calendar[60].date(),
        test_end=calendar[79].date(), label_cutoff=calendar[89].date())
    records = []
    for day in range(61):
        for rank in range(1, 6):
            records.append(dict(decision_as_of_trade_date=calendar[day], target_trade_date=calendar[day+1],
                instrument=f"{rank:06d}.SZ", selection_effective_rank=rank, candidate_group_size=5,
                **{name: .01 for name in FEATURES}, observed_gap_bps=50., gross_terminal_ratio=1.03,
                path_min_ratio=.96, label_information_end=calendar[day+5], label_status="AVAILABLE",
                label_reason="COMPLETE", label_contract=POLICY["label_contract"], policy_sha256=POLICY_SHA256))
    original = pd.DataFrame(records)
    funding = original.loc[:, KEY].assign(large_order_imbalance_D=.2, large_order_turnover_share_D=.3,
        large_order_imbalance_5D=-.1, moneyflow_feature_status="AVAILABLE",
        moneyflow_feature_visible_through=original[KEY[0]])
    return original, funding, config, [day.date().isoformat() for day in calendar]


def projected_development(tmp_path):
    original, funding, config, calendar = development_frames()
    dates = tuple(pd.Timestamp(day).date() for day in calendar[:60])
    # Only projected D rows may survive; immature/test targets are deliberately toxic.
    boundary = ((original[KEY[0]].le(pd.Timestamp(config.train_end)) & original.label_information_end.gt(pd.Timestamp(config.train_end)))
                | original.label_information_end.gt(pd.Timestamp(config.validation_end)))
    original.loc[boundary, ["observed_gap_bps", "gross_terminal_ratio", "path_min_ratio"]] = np.inf
    funding.loc[funding[KEY[0]].gt(pd.Timestamp(config.validation_end)), MONEYFLOW_FEATURES] = np.inf
    funding["gross_value_ratio"] = "FORBIDDEN_LEGACY_TARGET"
    funding.loc[0, list(MONEYFLOW_FEATURES)] = np.nan
    funding.loc[0, "moneyflow_feature_status"] = "UNKNOWN_ZERO_DENOMINATOR"
    gp5, flow = tmp_path/"gp5.parquet", tmp_path/"moneyflow.parquet"
    original.to_parquet(gp5, index=False)
    funding.to_parquet(flow, index=False)
    rows = read_moneyflow_development_v1(gp5_rows=gp5, moneyflow_rows=flow, configuration=config,
                                        decision_dates=dates, calendar=calendar)
    return rows, config, dates, calendar


def test_projection_excludes_test_legacy_and_immature_numbers(tmp_path, monkeypatch):
    from backend.services.advisory_model_first import generic_moneyflow_price_5td_source_v1 as source
    calls, read = [], source.pq.read_table
    def record(path, **kwargs):
        calls.append((str(path), kwargs))
        assert kwargs["columns"] and kwargs["filters"]
        return read(path, **kwargs)
    monkeypatch.setattr(source.pq, "read_table", record)
    rows, config, _, _ = projected_development(tmp_path)
    assert len(rows) == 300 and rows[KEY[0]].max().date() == config.validation_end
    assert rows.label_status.eq("IMMATURE").sum() == 50
    assert rows.loc[rows.label_status.eq("IMMATURE"), ["observed_gap_bps", "gross_terminal_ratio", "path_min_ratio"]].isna().all().all()
    assert rows.loc[0, list(MONEYFLOW_FEATURES)].isna().all()
    assert all("gross_value_ratio" not in call[1]["columns"] for call in calls)
    numeric = [call for call in calls if "gross_terminal_ratio" in call[1]["columns"]]
    assert len(numeric) == 2 and all(any(part[0] == "label_information_end" for part in call[1]["filters"]) for call in numeric)


@pytest.mark.parametrize("mutation", ["duplicate", "missing_key", "future", "bad_rank"])
def test_join_rejects_identity_conflicts(mutation):
    original, funding, _, calendar = development_frames()
    original, funding = original.head(5), funding.head(5)
    if mutation == "duplicate":
        funding = pd.concat([funding, funding.head(1)])
    elif mutation == "missing_key":
        funding = funding.iloc[1:]
    elif mutation == "future":
        funding.loc[0, "moneyflow_feature_visible_through"] = pd.Timestamp(calendar[1])
    else:
        original = original.copy()
        original.loc[0, "selection_effective_rank"] = 2
    with pytest.raises(ValueError):
        join_moneyflow_features_v1(original=original.loc[:, [*ROSTER, *FEATURES]], funding=funding,
                                   decision_dates=[calendar[0]], calendar=calendar)


@pytest.mark.parametrize("bad", [True, "0.2", float("inf"), -1.01, 1.01])
def test_known_moneyflow_units_are_not_clipped(bad):
    frame = pd.DataFrame([{name: .2 for name in MONEYFLOW_FEATURES}], dtype=object)
    frame.loc[0, MONEYFLOW_FEATURES[0]] = bad
    with pytest.raises((ValueError, AdvisoryModelFirstError)):
        moneyflow_values(frame)
    with pytest.raises(ValueError):
        moneyflow_values(pd.DataFrame([dict(zip(MONEYFLOW_FEATURES, [0., -0.01, -1.], strict=True))]))
