from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first import causal_admission_v2_pipeline as pipeline

from backend.services.advisory_model_first.causal_admission_v2_contracts import (
    CAUSAL_ADMISSION_FEATURE_COLUMNS,
    EXPANDING_ARM_ID,
    INNER_END,
    INNER_START,
    STATIC_ARM_ID,
    build_causal_admission_request,
)
from backend.services.advisory_model_first.causal_admission_v2_pipeline import (
    apply_inner_holm_bonferroni,
    build_causal_admission_decisions,
    build_causal_admission_panel,
    build_probability_readout,
    evaluate_causal_phase,
    run_chronological_predictions,
    _load_r1_verified_sources,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError


def build_test_request(**overrides):
    values = {
        "parent_v1_bundle_path": "/tmp/v1",
        "parent_v1_bundle_id": "1" * 64,
        "parent_v1_request_sha256": "2" * 64,
        "parent_v1_manifest_sha256": "3" * 64,
        "package_id": "pkg_test",
        "manifest_sha256": "4" * 64,
        "program_id": "program_test",
        "binding_version_id": "binding_test",
        "policy_identity": "5" * 64,
        "shadow_policy_sha256": "6" * 64,
        "cost_policy_sha256": "7" * 64,
        "dataset_identity": "8" * 64,
        "cumulative_evaluated_trial_count_prior": 1284,
        "cumulative_candidate_index_prior": 88,
        "registry_path": "/tmp/registry.jsonl",
        "registry_sha256_at_request": "9" * 64,
        "registry_record_count_at_request": 32,
        "auxiliary_route_path": "/tmp/current_auxiliary_route.md",
        "auxiliary_route_sha256_at_request": "b" * 64,
        "repository_root": "/tmp/repo",
        "repository_commit": "a" * 40,
        "output_root": "/tmp/output",
        "created_at": datetime(2026, 9, 7, tzinfo=timezone.utc),
    }
    values.update(overrides)
    return build_causal_admission_request(**values)


def _population(dates: pd.DatetimeIndex) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    rows = []
    labels = []
    for day_number, decision in enumerate(dates):
        target = decision + pd.offsets.BDay(1)
        for rank in range(1, 21):
            base = {
                "decision_as_of_trade_date": decision,
                "target_trade_date": target,
                "instrument": f"{rank:06d}.SZ",
                "selection_effective_rank": rank,
            }
            rows.append(
                {
                    **base,
                    **{
                        feature: float(day_number + rank + index) / 100.0
                        for index, feature in enumerate(CAUSAL_ADMISSION_FEATURE_COLUMNS[:12])
                    },
                }
            )
            labels.append(
                {
                    **base,
                    "net_return_bps": float((rank % 3 - 1) * 20 + day_number % 7),
                    "label_information_end": target,
                    "label_status": "MATURED",
                }
            )
    raw = pd.DataFrame(
        [
            {
                "decision_as_of_trade_date": decision,
                **{
                    feature: float(day_number + index) / 100.0
                    for index, feature in enumerate(CAUSAL_ADMISSION_FEATURE_COLUMNS[12:])
                },
            }
            for day_number, decision in enumerate(dates)
        ]
    )
    return pd.DataFrame(rows), raw, pd.DataFrame(labels)


def test_panel_preserves_exact_population_and_normal_missing_values() -> None:
    dates = pd.bdate_range("2024-01-02", periods=386)
    score, raw, labels = _population(dates)
    raw.loc[10, CAUSAL_ADMISSION_FEATURE_COLUMNS[-1]] = np.nan

    panel = build_causal_admission_panel(
        score_features=score,
        raw_market_features=raw,
        primary_labels=labels,
    )

    assert len(panel) == 7720
    assert panel.groupby("decision_as_of_trade_date").size().eq(20).all()
    assert panel[CAUSAL_ADMISSION_FEATURE_COLUMNS[-1]].isna().sum() == 20


def test_expanding_predictions_use_only_mature_past_and_have_three_inner_fits() -> None:
    prior = pd.bdate_range(end=pd.Timestamp(INNER_START) - pd.offsets.BDay(1), periods=70)
    available_inner = pd.bdate_range(INNER_START, INNER_END)
    inner = pd.DatetimeIndex([*available_inner[:47], available_inner[-1]])
    score, raw, labels = _population(prior.append(inner))
    panel = score.merge(
        labels,
        on=[
            "decision_as_of_trade_date",
            "target_trade_date",
            "instrument",
            "selection_effective_rank",
        ],
        validate="one_to_one",
    ).merge(raw, on="decision_as_of_trade_date", validate="many_to_one")

    result = run_chronological_predictions(
        panel=panel,
        prediction_dates=inner,
        arm_id=EXPANDING_ARM_ID,
        request=build_test_request(),
        phase="INNER",
    )

    assert len(result.predictions) == 48 * 20
    assert len(result.fit_receipts) == 3
    assert "net_return_bps" not in result.predictions
    assert "label_information_end" not in result.predictions
    calibration = build_probability_readout(result.predictions, panel)
    assert calibration["row_count"] == 48 * 20
    assert 0.0 <= calibration["brier"] <= 1.0
    unavailable = panel.copy()
    first = result.predictions.iloc[0]
    mask = unavailable["decision_as_of_trade_date"].eq(first["decision_as_of_trade_date"]) & unavailable[
        "instrument"
    ].eq(first["instrument"])
    unavailable.loc[mask, "label_status"] = "NOT_ENTERED_LIMIT_UP"
    unavailable.loc[mask, "net_return_bps"] = np.nan
    unavailable_readout = build_probability_readout(result.predictions, unavailable)
    assert unavailable_readout["normal_unavailable_row_count"] == 1
    for receipt in result.fit_receipts:
        assert receipt["train_date_end"] < receipt["fit_cutoff"]
        assert receipt["max_label_information_end"] < receipt["fit_cutoff"]


def test_static_inner_predictions_ignore_poisoned_inner_outcomes() -> None:
    prior = pd.bdate_range(end=pd.Timestamp(INNER_START) - pd.offsets.BDay(1), periods=70)
    available_inner = pd.bdate_range(INNER_START, INNER_END)
    inner = pd.DatetimeIndex([*available_inner[:47], available_inner[-1]])
    score, raw, labels = _population(prior.append(inner))
    panel = score.merge(
        labels,
        on=[
            "decision_as_of_trade_date",
            "target_trade_date",
            "instrument",
            "selection_effective_rank",
        ],
        validate="one_to_one",
    ).merge(raw, on="decision_as_of_trade_date", validate="many_to_one")
    request = build_test_request()
    baseline = run_chronological_predictions(
        panel=panel,
        prediction_dates=inner,
        arm_id=STATIC_ARM_ID,
        request=request,
        phase="INNER",
    ).predictions
    poisoned = panel.copy()
    poisoned.loc[poisoned["decision_as_of_trade_date"].isin(inner), "net_return_bps"] += 1e6
    repeated = run_chronological_predictions(
        panel=poisoned,
        prediction_dates=inner,
        arm_id=STATIC_ARM_ID,
        request=request,
        phase="INNER",
    ).predictions

    pd.testing.assert_series_equal(baseline["expected_net_return_bps"], repeated["expected_net_return_bps"])


def test_action_has_exact_top5_and_allows_no_recommendation_day() -> None:
    request = build_test_request()
    day = pd.Timestamp(INNER_START)
    predictions = pd.DataFrame(
        {
            "phase": ["INNER"] * 20,
            "arm_id": [EXPANDING_ARM_ID] * 20,
            "decision_as_of_trade_date": [day] * 20,
            "selection_effective_rank": range(1, 21),
            "expected_net_return_bps": [0.0] * 20,
        }
    )

    decisions = build_causal_admission_decisions(predictions, request)

    assert len(decisions) == 5
    assert set(decisions["action"]) == {"SKIP"}
    assert set(decisions["day_state"]) == {"NO_ELIGIBLE_RECOMMENDATION"}
    assert decisions["action_contract_sha256"].str.fullmatch(r"[0-9a-f]{64}").all()

    tampered = decisions.copy()
    tampered.loc[0, "action"] = "TAKE"
    with pytest.raises(AdvisoryModelFirstError) as caught:
        evaluate_causal_phase(
            arm_id=EXPANDING_ARM_ID,
            phase="INNER",
            decisions=tampered,
            sources={},
            request=request,
        )
    assert caught.value.reason_code == "ADVISORY_CAUSAL_ACTION_INVALID"


def test_holm_bonferroni_is_step_down_and_does_not_change_point_eligibility() -> None:
    metrics = {
        "R1_STATIC_RIDGE_V1": {
            "one_sided_p_value_vs_minimum_economic_lift": 0.01,
            "eligible": True,
        },
        "R1_EXPANDING_20D_RIDGE_V1": {
            "one_sided_p_value_vs_minimum_economic_lift": 0.04,
            "eligible": True,
        },
    }

    apply_inner_holm_bonferroni(metrics, familywise_alpha=0.05)

    assert metrics["R1_STATIC_RIDGE_V1"]["holm_adjusted_alpha"] == 0.025
    assert metrics["R1_STATIC_RIDGE_V1"]["holm_reject"] is True
    assert metrics["R1_EXPANDING_20D_RIDGE_V1"]["holm_adjusted_alpha"] == 0.05
    assert metrics["R1_EXPANDING_20D_RIDGE_V1"]["holm_reject"] is True
    assert all(item["eligible"] for item in metrics.values())


def test_r1_rebinds_only_mutable_parent_control_evidence(monkeypatch) -> None:
    observed = {}

    class Parent:
        evidence_refs = (
            SimpleNamespace(role="immutable_data"),
            SimpleNamespace(role="score_hmm_trial_registry"),
            SimpleNamespace(role="score_hmm_main_route"),
        )

        def model_copy(self, *, update):
            observed.update(update)
            return self

    monkeypatch.setattr(pipeline, "_load_verified_sources", lambda parent: {"parent": parent})

    result = _load_r1_verified_sources(Parent())

    assert [item.role for item in observed["evidence_refs"]] == ["immutable_data"]
    assert result["parent"].__class__ is Parent
