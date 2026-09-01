from __future__ import annotations

from datetime import date, datetime

import pandas as pd
import pytest

from backend.services.advisory_model_first.action_value_contracts import (
    AdvisoryActionValueStatus,
    AdvisoryEvidenceLevel,
    AdvisoryInterventionEvidenceClass,
)
from backend.services.advisory_model_first.entry_guard_decision import (
    EntryGuardMode,
    EntryGuardMarketObservationV1,
    build_entry_guard_policy,
    build_entry_guard_signal,
    evaluate_entry_guard,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.incremental_value_labels import (
    build_entry_incremental_value_labels,
    build_intervention_support_from_labels,
)


BASELINE_POLICY = "a" * 64
COST_POLICY = "b" * 64


def _decision(mode: EntryGuardMode, *, open_price: float | None, suspended: bool = False):
    signal = build_entry_guard_signal(
        decision_date=date(2026, 1, 2),
        target_trade_date=date(2026, 1, 5),
        instrument="000001.SZ",
        selection_rank=1,
        reference_price=10.0,
        entry_gap_q10=-0.01,
        entry_gap_q50=0.0,
        entry_gap_q90=0.02,
        max_acceptable_gap_bps=250.0,
        max_buy_price=10.25,
        source_binding_sha256="c" * 64,
        feature_schema_sha256="d" * 64,
        information_cutoff=datetime(2026, 1, 2, 15, 0),
    )
    observation = EntryGuardMarketObservationV1(
        target_trade_date=date(2026, 1, 5),
        instrument="000001.SZ",
        observed_at=datetime(2026, 1, 5, 9, 31),
        open_price=open_price,
        limit_up_price=11.0,
        limit_down_price=9.0,
        suspended=suspended,
    )
    return evaluate_entry_guard(
        policy=build_entry_guard_policy(mode),
        signal=signal,
        observation=observation,
    )


def _baseline(net_return_bps: float = -100.0, *, policy: str = BASELINE_POLICY) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "decision_as_of_trade_date": pd.Timestamp("2026-01-02"),
                "target_trade_date": pd.Timestamp("2026-01-05"),
                "instrument": "000001.SZ",
                "episode_label_id": "advpolep_entry_test",
                "shadow_policy_sha256": policy,
                "cost_policy_sha256": COST_POLICY,
                "label_status": "MATURED",
                "label_information_end": pd.Timestamp("2026-01-20"),
                "net_return_bps": net_return_bps,
            }
        ]
    )


def test_entry_skip_uses_cash_zero_and_preserves_incremental_identity() -> None:
    result = build_entry_incremental_value_labels(
        decisions=[_decision(EntryGuardMode.FIXED_GAP_3, open_price=10.4)],
        baseline_episode_labels=_baseline(-100.0),
        baseline_policy_sha256=BASELINE_POLICY,
        cost_policy_sha256=COST_POLICY,
    )
    label = result.labels[0]
    assert label.status == AdvisoryActionValueStatus.AVAILABLE
    assert label.baseline_net_value_bps == -100.0
    assert label.action_net_value_bps == 0.0
    assert label.incremental_net_value_bps == 100.0
    assert label.intervention_action == "SKIP"


def test_entry_accept_is_identity_and_reduce_has_no_numeric_position_value() -> None:
    accept = build_entry_incremental_value_labels(
        decisions=[_decision(EntryGuardMode.NO_GUARD, open_price=10.2)],
        baseline_episode_labels=_baseline(75.0),
        baseline_policy_sha256=BASELINE_POLICY,
        cost_policy_sha256=COST_POLICY,
    ).labels[0]
    reduce = build_entry_incremental_value_labels(
        decisions=[_decision(EntryGuardMode.FIXED_GAP_3, open_price=10.2)],
        baseline_episode_labels=_baseline(75.0),
        baseline_policy_sha256=BASELINE_POLICY,
        cost_policy_sha256=COST_POLICY,
    ).labels[0]
    assert accept.incremental_net_value_bps == 0.0
    assert reduce.status == AdvisoryActionValueStatus.NON_NUMERIC_ADVICE_ONLY
    assert reduce.baseline_net_value_bps is None
    assert reduce.action_net_value_bps is None
    assert reduce.incremental_net_value_bps is None


def test_entry_waiting_and_policy_mismatch_fail_without_zero_fill() -> None:
    waiting = build_entry_incremental_value_labels(
        decisions=[_decision(EntryGuardMode.FIXED_GAP_3, open_price=None, suspended=True)],
        baseline_episode_labels=_baseline(75.0),
        baseline_policy_sha256=BASELINE_POLICY,
        cost_policy_sha256=COST_POLICY,
    ).labels[0]
    assert waiting.status == AdvisoryActionValueStatus.WAITING
    assert waiting.incremental_net_value_bps is None
    with pytest.raises(AdvisoryModelFirstError) as excinfo:
        build_entry_incremental_value_labels(
            decisions=[_decision(EntryGuardMode.NO_GUARD, open_price=10.0)],
            baseline_episode_labels=_baseline(policy="f" * 64),
            baseline_policy_sha256=BASELINE_POLICY,
            cost_policy_sha256=COST_POLICY,
        )
    assert excinfo.value.reason_code == "ADVISORY_ACTION_VALUE_POLICY_MISMATCH"


def test_intervention_support_stays_exploratory_when_pre_registered_minimums_fail() -> None:
    result = build_entry_incremental_value_labels(
        decisions=[_decision(EntryGuardMode.FIXED_GAP_3, open_price=10.4)],
        baseline_episode_labels=_baseline(-100.0),
        baseline_policy_sha256=BASELINE_POLICY,
        cost_policy_sha256=COST_POLICY,
    )
    support = build_intervention_support_from_labels(
        labels=result.labels,
        intervention_policy_sha256=result.labels[0].intervention_policy_sha256,
        regimes_by_decision_date={date(2026, 1, 2): "DOWN"},
        required_regimes=("DOWN", "UP"),
        minimum_intervention_count=10,
        minimum_intervention_day_fraction=0.5,
        minimum_days_per_required_regime=2,
        block_length_trading_days=20,
        minimum_effective_intervention_block_count=2,
    )
    assert support.evidence_class == AdvisoryInterventionEvidenceClass.EXPLORATORY_ONLY
    assert "INTERVENTION_COUNT_BELOW_MINIMUM" in support.reason_codes
    assert "REQUIRED_REGIME_SUPPORT_BELOW_MINIMUM" in support.reason_codes
    assert "EFFECTIVE_BLOCK_COUNT_BELOW_MINIMUM" in support.reason_codes


def test_entry_accept_is_not_counted_as_an_intervention() -> None:
    result = build_entry_incremental_value_labels(
        decisions=[_decision(EntryGuardMode.NO_GUARD, open_price=10.0)],
        baseline_episode_labels=_baseline(20.0),
        baseline_policy_sha256=BASELINE_POLICY,
        cost_policy_sha256=COST_POLICY,
    )
    support = build_intervention_support_from_labels(
        labels=result.labels,
        intervention_policy_sha256=result.labels[0].intervention_policy_sha256,
        regimes_by_decision_date={date(2026, 1, 2): "UP"},
        required_regimes=("UP",),
        minimum_intervention_count=1,
        minimum_intervention_day_fraction=0.1,
        minimum_days_per_required_regime=1,
        block_length_trading_days=20,
        minimum_effective_intervention_block_count=1,
    )
    assert support.intervention_count == 0
    assert support.intervention_day_count == 0
    assert support.evidence_class == AdvisoryInterventionEvidenceClass.EXPLORATORY_ONLY


def test_entry_builder_cannot_relabel_development_data_as_sealed_holdout() -> None:
    with pytest.raises(AdvisoryModelFirstError) as excinfo:
        build_entry_incremental_value_labels(
            decisions=[_decision(EntryGuardMode.NO_GUARD, open_price=10.0)],
            baseline_episode_labels=_baseline(20.0),
            baseline_policy_sha256=BASELINE_POLICY,
            cost_policy_sha256=COST_POLICY,
            evidence_level=AdvisoryEvidenceLevel.SEALED_HOLDOUT_CONFIRMATION,
        )
    assert excinfo.value.reason_code == "ADVISORY_EVIDENCE_LEVEL_VIOLATION"
