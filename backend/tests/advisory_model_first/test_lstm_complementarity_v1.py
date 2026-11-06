"""Conditional rank information is distinct from low score correlation or deployment."""

from copy import deepcopy

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first import lstm_complementarity_v1 as core
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import POLICY


def example():
    days = pd.to_datetime([core.FIRST, core.LAST])
    records = []
    for d in days:
        for rank in range(1, 51):
            y = float(rank)
            records.append(dict(**core.LSTM, decision_as_of_trade_date=d, target_trade_date=d + pd.offsets.BDay(1),
                instrument=f"{rank:06}.SZ", selection_effective_rank=rank, candidate_group_size=50, parent_score=-rank,
                **{name: np.sin(rank * (i + 1)) for i, name in enumerate(core.spo.FEATURES)},
                label_information_end=d + pd.offsets.BDay(5), valuation_status="AVAILABLE",
                hypothetical_liquidation_net_bps=y, observed_gap_bps=0.,
                valuation_gross_terminal_ratio=(1 + y / 10000) * (1 + POLICY["buy_bps"] / 10000) / (1 - POLICY["sell_bps"] / 10000),
                exit_execution_status="NOMINAL_EXIT_ELIGIBLE_NOT_FILL_PROOF", valuation_policy_sha256=core.spo.base.VALUATION_POLICY_SHA256))
    frame = pd.DataFrame(records)
    m = dict(arm="candidate", cutoff=core.CUTOFF, contract_sha256=core.spo.sha(core.spo.CONTRACT),
             fields=list(core.spo.FEATURES), median=[0.] * 8, scale=[1.] * 8, coef=list(range(16)))
    m["model_payload_sha256"] = core.spo.sha(m)
    return frame, m, days


def test_copy_parent_has_no_extra_rank_information():
    p = np.arange(50.)
    out = core.partial_rank_correlation(p, p * 5 + 10, np.sin(p))
    assert out["value"] is None and out["status"] == "NO_REMAINING_RANK_VARIATION"
    out = core.partial_rank_correlation(p, -p, np.sin(p))
    assert out["value"] is None  # Perfect anticorrelation alone is not complementarity.


def test_conditional_signal_and_unknown_are_distinguished():
    p = np.arange(50.)
    signal = np.sin(p * 1.7)
    out = core.partial_rank_correlation(p, signal, signal)
    assert out["value"] == pytest.approx(1.) and out["projections"] == 2
    assert core.partial_rank_correlation(np.ones(50), signal, signal)["status"] == "DEGENERATE_PARENT"
    signal[0] = np.nan
    assert core.partial_rank_correlation(p, signal, signal)["status"] == "UNKNOWN_INPUT"


def test_component_conservation_and_unknown_slot():
    inputs, model, days = example()
    inputs.loc[0, core.spo.FEATURES] = np.nan
    pred = core.predict_lstm(inputs, model, days)
    comp = core.score_components(inputs, pred, model)
    np.testing.assert_allclose(comp.stock_component + comp.parent_component, pred.decision_score, equal_nan=True)
    assert pred.iloc[0].advisory_rank == 1 and pred.iloc[0].no_rank_advice
    altered = pred.copy()
    altered.loc[1, "decision_score"] += 1
    with pytest.raises(ValueError, match="reconstruct"):
        core.score_components(inputs, altered, model)


@pytest.mark.parametrize("change", ["package", "manifest", "model", "date", "rank"])
def test_wrong_target_rejected(change):
    frame, model, days = example()
    if change == "package":
        frame["package_id"] = "other"
    elif change == "manifest":
        frame["manifest_sha256"] = "b" * 64
    elif change == "model":
        model = deepcopy(model)
        model["cutoff"] = "2026-04-01"
    elif change == "date":
        frame.loc[frame[core.DAY].eq(days[0]), core.DAY] = pd.Timestamp("2026-03-12")
    else:
        frame.loc[0, "selection_effective_rank"] = 2
    with pytest.raises(ValueError):
        core.predict_lstm(frame, model, days)


def test_future_finance_rejected_and_immature_preserved():
    frame, model, days = example()
    pred = core.predict_lstm(frame, model, days)
    finance = frame.loc[:, [*core.KEY, *core.FINANCE]].copy()
    with pytest.raises(ValueError, match="future"):
        core.stress_report(pred, finance, days)
    future = finance.label_information_end.gt(pd.Timestamp(core.LAST))
    finance.loc[future, ["valuation_status", "exit_execution_status"]] = "IMMATURE"
    finance.loc[future, ["hypothetical_liquidation_net_bps", "valuation_gross_terminal_ratio", "observed_gap_bps"]] = np.nan
    report, panel = core.stress_report(pred, finance, days)
    assert report["original_days"] == 2 and report["mature_days"] == 1
    assert not report["production_ready"] and not report["independent_oos_evidence"]
    assert pd.isna(panel.iloc[1].delta)


def test_incomplete_finance_keeps_original_relation_day():
    frame, model, days = example()
    pred = core.predict_lstm(frame, model, days)
    finance = frame.loc[:, [*core.KEY, *core.FINANCE]].copy()
    finance.loc[:, ["valuation_status", "exit_execution_status"]] = "IMMATURE"
    finance.loc[:, ["hypothetical_liquidation_net_bps", "valuation_gross_terminal_ratio", "observed_gap_bps"]] = np.nan
    panel = core.relation_panel(frame, pred, finance, model)
    assert len(panel) == 2 and panel.original_candidates.eq(50).all()
    assert panel.partial_rank_ic.isna().all() and panel.known_finance_rows.eq(0).all()
    assert panel.relation_status.eq("INCOMPLETE_FINANCE").all()
