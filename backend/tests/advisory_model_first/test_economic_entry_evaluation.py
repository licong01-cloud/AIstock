from __future__ import annotations

import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_entry_evaluation import query_actual_open_conditions, matched_entry_priorities, economic_intervention_attribution
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_entry_training import train_economic_entry_model
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError

pytest_plugins = ("backend.tests.advisory_model_first.test_economic_entry_model",)  # Reuse one leaf fixture.


def test_conditional_queries_ignore_future_paths_and_keep_unavailable_candidates(study):
    fitted = train_economic_entry_model(**{key: study[key] for key in ("features", "labels", "request")})
    labels = [label for label in study["labels"] if label.decision_date >= fitted.request.test_start]
    candidates = pd.DataFrame([{KEY[0]:pd.Timestamp(label.decision_date), KEY[1]:pd.Timestamp(label.target_date),
                               "instrument":label.instrument, "selection_effective_rank":label.selection_rank} for label in labels])
    references = candidates.copy()
    references["target_reference_raw_cny"] = 10.
    references["reference_visible_through"] = references[KEY[0]]
    references["source_sha256"] = study["identity"].reference_source_sha256
    prices = candidates.loc[:, [KEY[1], "instrument"]].rename(columns={KEY[1]:"trade_date"})
    prices["raw_open_cny"] = [round(label.actual_open_raw_cny, 2) for label in labels]
    prices["suspended"] = False
    prices["tradability_unknown"] = False
    prices["up_limit"], prices["down_limit"] = 11., 9.
    prices["source_sha256"] = study["identity"].price_source_sha256
    prices["price_coordinate_sha256"] = study["identity"].price_coordinate_sha256
    args = dict(fitted=fitted, identity=study["identity"], bundle_sha256="1"*64, candidates=candidates,
                features=study["features"], references=references, prices=prices)
    before = query_actual_open_conditions(**args)
    prices["raw_close_cny"] = -99999
    prices["raw_high_cny"] = 99999
    study["features"]["future_net_return_bps"] = 1e6
    pd.testing.assert_frame_equal(before, query_actual_open_conditions(**args))
    prices.loc[0, "tradability_unknown"] = True
    after = query_actual_open_conditions(**args)
    assert len(after) == len(before)
    assert after.iloc[0]["model_action"] == "UNAVAILABLE"
    assert after.iloc[0]["rule_action"] == "UNAVAILABLE"
    prices.loc[0, "tradability_unknown"] = False
    prices.loc[0, "up_limit"] = prices.loc[0, "raw_open_cny"]
    blocked = query_actual_open_conditions(**args)
    assert blocked.iloc[0]["model_action"] == "UNAVAILABLE"
    assert blocked.iloc[0]["reason_code"] == "OPEN_LIMIT_OR_EXECUTABILITY_UNPROVEN"
    study["features"].loc[0, "feature_visible_through"] = pd.Timestamp("2030-01-01")
    with pytest.raises(AdvisoryModelFirstError):
        query_actual_open_conditions(**args)


def test_fixed_top5_skip_never_refills_rank6_or_relables_unknown_as_model_take():
    decisions = pd.DataFrame({KEY[0]:pd.Timestamp("2025-01-02"),
                              "instrument":[f"{rank:06d}.SZ" for rank in range(1,7)],
                              "selection_effective_rank":range(1,7),
                              "model_action":["TAKE","SKIP","UNAVAILABLE","SKIP","TAKE","TAKE"],
                              "rule_action":["SKIP"]*6})
    priorities = matched_entry_priorities(decisions, arm="model")
    assert priorities["entry_priority_rank"].tolist() == [1,3,5]
    assert decisions.loc[2,"model_action"] == "UNAVAILABLE"
    assert matched_entry_priorities(decisions, arm="rule").empty


def test_positive_unknown_control_returns_are_not_model_take_evidence():
    day = pd.Timestamp("2025-01-02")
    decisions = pd.DataFrame({KEY[0]:day, "instrument":["000001.SZ","000002.SZ","000003.SZ"],
                              "selection_effective_rank":[1,2,3], "model_action":["SKIP","SKIP","UNAVAILABLE"],
                              "expected_net_return_bps":[50.,-10.,None], "daily_mark_drawdown_q90_bps":[1000.,900.,None]})
    baseline = pd.DataFrame({"entry_signal_date":day, "instrument":decisions["instrument"], "net_return_bps":[10.,-10.,100.]})
    result = economic_intervention_attribution(decisions=decisions, baseline_episodes=baseline, model_episodes=baseline.iloc[2:])
    assert result["actual_model_take_episodes"] == 0
    assert result["research_unknown_baseline_control_episodes"] == 1
    assert result["model_take_value_evidence"] == "NO_MODEL_TAKE"
    assert result["skipped_baseline_profitable_episodes"] == result["skipped_baseline_loss_episodes"] == 1
    empty = economic_intervention_attribution(decisions=decisions, baseline_episodes=pd.DataFrame(), model_episodes=pd.DataFrame())
    assert empty["actual_model_take_episodes"] == empty["baseline_entered_episode_count"] == 0
