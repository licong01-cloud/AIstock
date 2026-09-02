from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.qe_alpha_mve_contracts import (
    build_default_proposals,
)
from backend.services.advisory_model_first.qe_alpha_mve_pipeline import (
    _validate_static_fields,
    compile_proposal_scores,
    evaluate_proposals,
)
from backend.tests.advisory_model_first.test_qe_alpha_mve_contracts import (
    make_qe_alpha_mve_request,
)


def _price_panel() -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=60)
    rows: list[dict[str, object]] = []
    for symbol_index, symbol in enumerate(("000001.SZ", "000002.SZ", "000003.SZ")):
        for day_index, day in enumerate(dates):
            close = 10.0 + symbol_index + day_index * (0.02 + symbol_index * 0.005)
            rows.append(
                {
                    "datetime": day,
                    "instrument": symbol,
                    "open": close - 0.1,
                    "high": close + 0.2,
                    "low": close - 0.2,
                    "close": close,
                    "volume": 1000.0 + symbol_index * 100.0 + day_index,
                    "amount": 10000.0 + day_index,
                }
            )
    return pd.DataFrame(rows)


def test_expression_scores_are_invariant_to_post_decision_poison() -> None:
    proposal = build_default_proposals()[0]
    panel = _price_panel()
    cutoff = pd.Timestamp("2024-02-15")
    baseline = compile_proposal_scores(panel=panel, proposals=(proposal,))
    poisoned_panel = panel.copy()
    future = poisoned_panel["datetime"] > cutoff
    poisoned_panel.loc[future, ["open", "high", "low", "close", "volume", "amount"]] = 999999.0
    poisoned = compile_proposal_scores(panel=poisoned_panel, proposals=(proposal,))

    columns = ["datetime", "instrument", proposal.proposal_id]
    pd.testing.assert_frame_equal(
        baseline.loc[baseline["datetime"] <= cutoff, columns].reset_index(drop=True),
        poisoned.loc[poisoned["datetime"] <= cutoff, columns].reset_index(drop=True),
    )


def test_safe_division_preserves_missing_instead_of_zero_fallback() -> None:
    from backend.services.advisory_model_first.qe_alpha_mve_contracts import (
        QEAlphaProposalV1,
    )
    from backend.services.strategy_package.runtime_variant import canonical_json_sha256

    expression = {
        "op": "SAFE_DIVIDE",
        "args": [
            {"op": "FIELD", "field": "close"},
            {"op": "FIELD", "field": "volume"},
        ],
    }
    proposal = QEAlphaProposalV1(
        proposal_id="N3_PRICE_VOLUME_BEHAVIOR_99",
        family="PRICE_VOLUME_BEHAVIOR",
        economic_hypothesis="test only",
        expression=expression,
        expression_sha256=canonical_json_sha256(expression),
        source_fields=("close", "volume"),
    )
    panel = pd.DataFrame(
        {
            "datetime": pd.to_datetime(["2024-01-02", "2024-01-03"]),
            "instrument": ["000001.SZ", "000001.SZ"],
            "close": [10.0, 11.0],
            "volume": [0.0, 2.0],
        }
    )
    result = compile_proposal_scores(panel=panel, proposals=(proposal,))

    assert pd.isna(result.loc[0, proposal.proposal_id])
    assert result.loc[1, proposal.proposal_id] == 5.5


def test_same_date_transform_ignores_nonmember_extreme_poison() -> None:
    from backend.services.advisory_model_first.qe_alpha_mve_contracts import (
        QEAlphaProposalV1,
    )
    from backend.services.strategy_package.runtime_variant import canonical_json_sha256

    expression = {
        "op": "SAME_DATE_ZSCORE",
        "args": [{"op": "FIELD", "field": "close"}],
    }
    proposal = QEAlphaProposalV1(
        proposal_id="N3_PRICE_VOLUME_BEHAVIOR_98",
        family="PRICE_VOLUME_BEHAVIOR",
        economic_hypothesis="PIT membership test only",
        expression=expression,
        expression_sha256=canonical_json_sha256(expression),
        source_fields=("close",),
    )
    panel = pd.DataFrame(
        {
            "datetime": pd.to_datetime(["2024-01-02"] * 3),
            "instrument": ["000001.SZ", "000002.SZ", "NEWIPO.SZ"],
            "close": [10.0, 12.0, 100.0],
            "pit_eligible": [True, True, False],
        }
    )
    baseline = compile_proposal_scores(panel=panel, proposals=(proposal,))
    poisoned_panel = panel.copy()
    poisoned_panel.loc[poisoned_panel["instrument"].eq("NEWIPO.SZ"), "close"] = 999999.0
    poisoned = compile_proposal_scores(panel=poisoned_panel, proposals=(proposal,))

    pd.testing.assert_series_equal(
        baseline.loc[:1, proposal.proposal_id],
        poisoned.loc[:1, proposal.proposal_id],
    )
    assert pd.isna(baseline.loc[2, proposal.proposal_id])


def test_static_schema_missing_proposal_field_fails_before_execution(tmp_path) -> None:
    path = tmp_path / "static_factors.parquet"
    pd.DataFrame(
        {
            "datetime": [pd.Timestamp("2024-07-04")],
            "instrument": ["000001.SZ"],
            "unrelated": [1.0],
        }
    ).to_parquet(path, index=False)

    with pytest.raises(AdvisoryModelFirstError) as exc_info:
        _validate_static_fields(path, build_default_proposals())
    assert exc_info.value.reason_code == "ADVISORY_QE_ALPHA_MVE_SOURCE_IDENTITY_MISMATCH"


def test_frontier_selects_only_the_pre_registered_strong_proposal() -> None:
    request = make_qe_alpha_mve_request()
    dates = pd.bdate_range(request.signal_start, periods=382)
    symbols = [f"{index:06d}.SZ" for index in range(30)]
    rng = np.random.default_rng(20260902)
    outcome_rows: list[dict[str, object]] = []
    score_rows: list[dict[str, object]] = []
    strong = request.proposals[0].proposal_id
    for date_index, day in enumerate(dates):
        for symbol_index, symbol in enumerate(symbols):
            target = float(symbol_index * 20.0 + (date_index % 5))
            outcome_rows.append(
                {
                    "arm_id": "CURRENT_IC_PARENT",
                    "decision_as_of_trade_date": day,
                    "instrument": symbol,
                    "score": float(rng.normal()),
                    "economic_net_excess_bps": target,
                    "outcome_known": True,
                }
            )
            row: dict[str, object] = {"datetime": day, "instrument": symbol}
            for proposal in request.proposals:
                row[proposal.proposal_id] = target if proposal.proposal_id == strong else float(rng.normal())
            score_rows.append(row)
    _, daily, summary, frontier = evaluate_proposals(
        panel=pd.DataFrame(),
        outcomes=pd.DataFrame(outcome_rows),
        proposal_scores=pd.DataFrame(score_rows),
        request=request,
    )

    assert len(daily) == 24 * 382
    assert summary["trial_count"] == 24
    assert frontier["selected_proposal_id"] == strong
    assert frontier["eligible_proposal_ids"] == [strong]
    strong_summary = next(item for item in summary["proposals"] if item["proposal_id"] == strong)
    assert strong_summary["familywise_rank_ic_lower"] > 0
    assert strong_summary["familywise_top5_lift_lower_bps"] > 0
    assert strong_summary["decision_use"] == "NAVIGATION_ONLY"
