from __future__ import annotations

from datetime import date

import pytest
from pydantic import ValidationError

from backend.services.advisory_model_first.action_value_contracts import (
    AdvisoryActionRole,
    AdvisoryActionValueStatus,
    AdvisoryEvidenceLevel,
    AdvisoryInterventionEvidenceClass,
    build_incremental_value_label,
    build_intervention_support,
)
from backend.services.advisory_model_first.research_control_contracts import DecisionUse


def _label_values() -> dict[str, object]:
    return {
        "role": AdvisoryActionRole.ENTRY_GUARD,
        "decision_use": DecisionUse.NAVIGATION_ONLY,
        "evidence_level": AdvisoryEvidenceLevel.HISTORICAL_REPLAY,
        "sealed_holdout_accessed": False,
        "decision_date": date(2026, 1, 2),
        "target_action_date": date(2026, 1, 5),
        "effective_action_date": date(2026, 1, 5),
        "instrument": "000001.SZ",
        "episode_id": "episode-1",
        "baseline_action": "ENTER",
        "intervention_action": "SKIP",
        "status": AdvisoryActionValueStatus.AVAILABLE,
        "baseline_net_value_bps": -10.0,
        "action_net_value_bps": 0.0,
        "incremental_net_value_bps": 10.0,
        "baseline_policy_sha256": "a" * 64,
        "intervention_policy_sha256": "b" * 64,
        "cost_policy_sha256": "c" * 64,
        "shadow_simulator_sha256": "d" * 64,
        "information_start": date(2026, 1, 2),
        "information_end": date(2026, 1, 20),
        "reason_code": "TEST",
    }


def test_historical_replay_cannot_claim_activation_or_sealed_access() -> None:
    with pytest.raises(ValidationError):
        build_incremental_value_label(**{**_label_values(), "decision_use": DecisionUse.ACTIVATION_EVIDENCE})
    with pytest.raises(ValidationError):
        build_incremental_value_label(**{**_label_values(), "sealed_holdout_accessed": True})


def test_sealed_and_prospective_evidence_levels_have_distinct_access_semantics() -> None:
    with pytest.raises(ValidationError):
        build_incremental_value_label(
            **{
                **_label_values(),
                "evidence_level": AdvisoryEvidenceLevel.SEALED_HOLDOUT_CONFIRMATION,
                "sealed_holdout_accessed": False,
            }
        )
    with pytest.raises(ValidationError):
        build_incremental_value_label(
            **{
                **_label_values(),
                "evidence_level": AdvisoryEvidenceLevel.PROSPECTIVE_OOS,
                "sealed_holdout_accessed": True,
            }
        )


def test_incremental_numeric_identity_is_fail_closed() -> None:
    with pytest.raises(ValidationError):
        build_incremental_value_label(**{**_label_values(), "incremental_net_value_bps": 9.0})


def test_intervention_support_is_derived_from_pre_registered_thresholds() -> None:
    support = build_intervention_support(
        role=AdvisoryActionRole.EXIT,
        intervention_policy_sha256="e" * 64,
        total_decision_count=100,
        intervention_count=30,
        decision_day_count=40,
        intervention_day_count=20,
        intervention_day_fraction=0.5,
        intervention_days_by_regime={"UP": 10, "DOWN": 10},
        required_regimes=("UP", "DOWN"),
        minimum_intervention_count=20,
        minimum_intervention_day_fraction=0.25,
        minimum_days_per_required_regime=10,
        block_length_trading_days=20,
        effective_intervention_block_count=1,
        minimum_effective_intervention_block_count=1,
    )
    assert support.evidence_class == AdvisoryInterventionEvidenceClass.CONFIRMATORY_ELIGIBLE
    assert support.reason_codes == ()
