from __future__ import annotations

from decimal import Decimal

import numpy as np
import pandas as pd

from backend.services.position_timing.contracts import TriggerSide
from backend.services.position_timing.contracts import (
    POSITION_TIMING_L2_RESEARCH_CONTRACT_V1,
    canonical_sha256,
)
from backend.services.position_timing.learnability_pipeline import (
    CrossfitResult,
    FrozenL2LearnabilityRequestV1,
    PopulationBuildResult,
    SOURCE_ROLES,
    _sell_cost_array,
    circular_block_interval,
    classify_effect,
    evaluate_l2_policy,
    inspect_l2_learnability_bundle,
    map_monotone_exposure,
    run_l2_learnability_audit,
)
from backend.services.position_timing.policy import (
    EXIT_GUARD_RULE_DEFAULT_SNAPSHOT_V1,
    PERSONAL_MANUAL_COMPONENT_COST_V1,
    component_cost_for_parent_notionals,
)
from backend.services.advisory_model_first.policy_contracts import AdvisoryPolicySplitV1
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1


def test_monotone_policy_mapping_uses_only_frozen_four_exposures() -> None:
    mapped = map_monotone_exposure(
        np.asarray([-1.0, 0.0, 0.1, 1.0, 2.0]),
        q50=0.5,
        q75=1.5,
    )
    assert mapped.tolist() == [1.0, 1.0, 0.5, 0.25, 0.0]


def test_effect_classification_and_ridge_selection_are_orthogonal_to_power() -> None:
    assert classify_effect(lower_bps=0.01, upper_bps=2.0) == "SUPPORTED"
    assert classify_effect(lower_bps=-2.0, upper_bps=0.0) == "NEGATIVE"
    assert classify_effect(lower_bps=-2.0, upper_bps=1.0) == "INCONCLUSIVE"
    first = circular_block_interval([1.0, 2.0, 3.0, 4.0], alpha=0.025, seed=11)
    second = circular_block_interval([1.0, 2.0, 3.0, 4.0], alpha=0.025, seed=11)
    assert first == second


def test_vector_cost_matches_componentized_parent_order_authority() -> None:
    notionals = np.asarray([40_000.0, 120_000.0])
    actual = _sell_cost_array(
        notionals,
        parent_order_count=1,
        policy=PERSONAL_MANUAL_COMPONENT_COST_V1,
    )
    expected = [
        float(
            component_cost_for_parent_notionals(
                side=TriggerSide.SELL,
                notionals=(Decimal(str(value)),),
            )["total"]
        )
        for value in notionals
    ]
    assert np.allclose(actual, expected, rtol=0, atol=1e-10)
    split = _sell_cost_array(
        notionals,
        parent_order_count=3,
        policy=PERSONAL_MANUAL_COMPONENT_COST_V1,
    )
    assert np.all(split >= actual)


def test_policy_path_never_readds_exposure_and_reports_cost_sensitivity() -> None:
    rows = []
    exposures = []
    for cohort in range(4):
        decision = pd.Timestamp("2025-01-02") + pd.offsets.BDay(cohort * 20)
        for holding_session, gross, mapped in ((1, 10_500.0, 0.50), (2, 11_000.0, 1.00)):
            rows.append(
                {
                    "episode_ordinal": cohort,
                    "cohort_ordinal": cohort,
                    "canonical_symbol": "600000.SH",
                    "entry_decision_date": decision,
                    "holding_session": holding_session,
                    "initial_quantity": 1000,
                    "st_flag": False,
                    "action_status_code": 0,
                    "action_full_gross_notional_cny": gross,
                    "terminal_full_gross_notional_cny": 9_000.0,
                    "entry_gross_notional_cny": 10_000.0,
                    "target_available": True,
                    "full_exit_incremental_net_value_bps": 100.0 + holding_session,
                    "market_regime_down": 0.0,
                    "market_regime_up_or_flat": 1.0,
                }
            )
            exposures.append(mapped)
    frame = pd.DataFrame(rows)
    result, cohorts, episodes = evaluate_l2_policy(
        rows=frame,
        crossfit=CrossfitResult(
            model_id="SKLEARN_RIDGE_V1",
            predictions=np.ones(len(frame)),
            target_exposures=np.asarray(exposures, dtype=np.float32),
            oof_counts=np.full(len(frame), 7, dtype=np.int8),
            path_diagnostics=(),
        ),
        cost_policy=PERSONAL_MANUAL_COMPONENT_COST_V1,
        model_offset=0,
        deployment_cell_counts={"HOLDING|SELL|AGE_1_3|UP_OR_FLAT|0": 1},
    )
    assert result.paired_episode_count == 4
    assert len(cohorts) == 4
    assert (episodes["action_side"] == "SELL").all()
    assert (episodes["policy_action_sell_quantity"] == 500).all()
    assert (episodes["policy_terminal_sell_quantity"] == 500).all()
    assert result.deployment_weighted_status == "AVAILABLE_DIAGNOSTIC_ONLY"
    assert set(result.cost_sensitivity) == {
        "parent_orders_1",
        "parent_orders_2",
        "parent_orders_3",
    }


def test_immutable_bundle_readback_keeps_two_hypotheses_and_no_runtime_write(tmp_path, monkeypatch) -> None:
    calendar = pd.bdate_range("2018-08-01", periods=1700)
    cohorts = calendar[::20][:80]
    feature_order = POSITION_TIMING_L2_RESEARCH_CONTRACT_V1.feature_order
    row_records = []
    episode_records = []
    for cohort_index, cohort in enumerate(cohorts):
        position = calendar.get_loc(cohort)
        for member in range(2):
            episode_ordinal = cohort_index * 2 + member
            episode_records.append(
                {
                    "episode_ordinal": episode_ordinal,
                    "episode_id": f"ptl2ep_{episode_ordinal:024x}",
                    "population_status": "READY",
                }
            )
            for holding_session, gross in ((1, 10_500.0 + member), (2, 11_000.0 + member)):
                values = {
                    feature: float(cohort_index + offset + member / 10.0)
                    for offset, feature in enumerate(feature_order)
                }
                values.update(
                    {
                        "episode_ordinal": episode_ordinal,
                        "cohort_ordinal": cohort_index,
                        "canonical_symbol": "600000.SH",
                        "entry_decision_date": cohort,
                        "entry_trade_date": calendar[position + 1],
                        "review_decision_date": calendar[position + holding_session],
                        "target_action_date": calendar[position + holding_session + 1],
                        "effective_terminal_trade_date": calendar[position + 25],
                        "holding_session": holding_session,
                        "initial_quantity": 1000,
                        "st_flag": False,
                        "action_status_code": 0,
                        "action_full_gross_notional_cny": gross,
                        "terminal_full_gross_notional_cny": 9_000.0,
                        "entry_gross_notional_cny": 10_000.0,
                        "target_available": True,
                        "full_exit_incremental_net_value_bps": (
                            100.0 + holding_session if member else -100.0 - holding_session
                        ),
                        "market_regime_down": 0.0,
                        "market_regime_up_or_flat": 1.0,
                        "market_regime_unknown": 0.0,
                    }
                )
                row_records.append(values)
    rows = pd.DataFrame(row_records)
    rows.attrs["trading_calendar"] = [value.isoformat() for value in calendar]
    availability = {feature: {"available": len(rows), "missing": 0} for feature in feature_order}
    population = PopulationBuildResult(
        episodes=pd.DataFrame(episode_records),
        rows=rows,
        source_identity={
            "request_sha256": "PLACEHOLDER_REPLACED_BELOW",
            "dataset_identity_sha256": "5" * 64,
            "cohort_count": len(cohorts),
            "episode_count": len(episode_records),
            "review_row_count": len(rows),
            "evaluable_review_row_count": len(rows),
            "population_status_counts": {"READY": len(episode_records)},
        },
        feature_availability=availability,
    )
    historical = tmp_path / "historical.jsonl"
    historical.write_text("{}\n", encoding="utf-8")
    source_hashes = {
        "cost_policy": canonical_sha256(PERSONAL_MANUAL_COMPONENT_COST_V1),
        "exit_guard": "a" * 64,
    }
    source_refs = {
        role: EvidenceReferenceV1(
            role=f"position_timing_l2_{role}",
            artifact_uri=historical.as_posix(),
            sha256=source_hashes.get(role, "4" * 64),
            size_bytes=3,
        )
        for role in SOURCE_ROLES
    }
    numeric_runtime = {"runtime": "test"}
    numeric_runtime["identity_sha256"] = canonical_sha256(numeric_runtime)
    values = {
        "contract_sha256": canonical_sha256(POSITION_TIMING_L2_RESEARCH_CONTRACT_V1),
        "dataset_identity_sha256": "5" * 64,
        "feature_schema_sha256": "6" * 64,
        "candidate_root": tmp_path.as_posix(),
        "daily_provider_root": tmp_path.as_posix(),
        "suspend_root": tmp_path.as_posix(),
        "timing_artifact_root": (tmp_path / "timing").as_posix(),
        "historical_registry_path": historical.as_posix(),
        "output_root": (tmp_path / "timing").as_posix(),
        "repository_root": tmp_path.as_posix(),
        "repository_commit": "7" * 40,
        "source_refs": source_refs,
        "model_runtime_identities": {
            "SKLEARN_RIDGE_V1": {"identity_sha256": "8" * 64},
            "LIGHTGBM_GBDT_V1": {"identity_sha256": "9" * 64},
        },
        "numeric_runtime_identity": numeric_runtime,
        "notional_observations": ({"card_id": "card", "planned_full_notional_cny": "10000"},),
        "notional_distribution_sha256": canonical_sha256(({"card_id": "card", "planned_full_notional_cny": "10000"},)),
        "prospective_event_counts": {"CARD_ISSUED": 1},
        "prospective_outcome_event_count": 0,
        "prospective_intervention_intent_count": 1,
        "deployment_cell_counts": {"HOLDING|SELL|AGE_1_3|UP_OR_FLAT|0": 1},
        "historical_registry_context_count": 0,
        "cost_policy": PERSONAL_MANUAL_COMPONENT_COST_V1,
        "cost_policy_sha256": canonical_sha256(PERSONAL_MANUAL_COMPONENT_COST_V1),
        "exit_guard_policy": EXIT_GUARD_RULE_DEFAULT_SNAPSHOT_V1,
        "exit_guard_snapshot_sha256": "a" * 64,
        "split_policy": AdvisoryPolicySplitV1(random_seed=20260903),
    }
    placeholder = FrozenL2LearnabilityRequestV1.model_construct(
        request_id="ptl2req_" + "0" * 24,
        request_sha256="0" * 64,
        created_at=pd.Timestamp("2026-09-06", tz="UTC").to_pydatetime(),
        **values,
    )
    digest = canonical_sha256(placeholder.functional_payload())
    request = FrozenL2LearnabilityRequestV1(
        request_id=f"ptl2req_{digest[:24]}",
        request_sha256=digest,
        created_at=pd.Timestamp("2026-09-06", tz="UTC").to_pydatetime(),
        **values,
    )
    population.source_identity["request_sha256"] = request.request_sha256
    request_path = tmp_path / "request.json"
    request_path.write_text(request.model_dump_json(), encoding="utf-8")

    monkeypatch.setattr(
        "backend.services.position_timing.learnability_pipeline._verify_request_sources",
        lambda _request: None,
    )
    monkeypatch.setattr(
        "backend.services.position_timing.learnability_pipeline.materialize_l2_population",
        lambda _request: population,
    )

    def fake_crossfit(*, rows, paths, model_id):
        assert len(paths) == 28
        mapped = np.where(rows["holding_session"].to_numpy() == 1, 0.5, 1.0)
        return CrossfitResult(
            model_id=model_id,
            predictions=np.ones(len(rows)),
            target_exposures=mapped.astype(np.float32),
            oof_counts=np.full(len(rows), 7, dtype=np.int8),
            path_diagnostics=(),
        )

    monkeypatch.setattr(
        "backend.services.position_timing.learnability_pipeline.run_l2_crossfit",
        fake_crossfit,
    )
    monkeypatch.setattr(
        "backend.services.position_timing.learnability_pipeline._deliver_registry",
        lambda **_kwargs: {"global_registry_unchanged": True},
    )
    first = run_l2_learnability_audit(request_path)
    loaded = inspect_l2_learnability_bundle(first["bundle_path"])
    assert loaded["receipt"].hypothesis_count == 2
    assert loaded["receipt"].runtime_model_written is False
    assert loaded["receipt"].global_registry_written is False
    assert loaded["receipt"].current_route_written is False
    assert sum(record.selected_trial_count for record in loaded["records"]) <= 1
    second = run_l2_learnability_audit(request_path)
    assert second["status"] == "EXACT_RETRY"
    assert second["bundle_id"] == first["bundle_id"]
