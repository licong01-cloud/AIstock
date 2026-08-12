from __future__ import annotations

from datetime import date

import pytest

from backend.services.dataset_release.contracts import (
    Component,
    ComponentAction,
    ValidationCompatibility,
)
from backend.services.dataset_release.decision import (
    BaselineCandidate,
    ComponentDecisionInput,
    FrozenReuseEvidence,
    build_action_plan,
    decide_component,
    select_reuse_baseline,
)
from backend.services.dataset_release.errors import DecisionError


def _digest(char: str) -> str:
    return char * 64


def _evidence(key: str) -> FrozenReuseEvidence:
    return FrozenReuseEvidence(
        source_release_id="20260630-qe_hmm_full_v1-full-source-candidate",
        source_release_digest=_digest("1"),
        source_attestation_key=_digest("2"),
        artifact_id=f"artifact-{key}",
        component_partition_key=key,
        manifest_root=_digest("3"),
        file_identity=_digest("4"),
        reuse_mode="sealed_partition",
        mutation_set=(f"{key}.new",),
        compatibility_reason="semantic and artifact fingerprints match",
    )


def test_planner_emits_complete_mixed_component_partition_actions() -> None:
    plan = build_action_plan(
        [
            ComponentDecisionInput(
                Component.DAILY_BIN,
                "2026-07",
                appended_source_partitions=("2026-07",),
                frozen_reuse=_evidence("daily:2026-07"),
            ),
            ComponentDecisionInput(
                Component.MINUTE_BIN,
                "000001.SZ:2026-06",
                fingerprints_equal=True,
                frozen_reuse=_evidence("minute:000001.SZ:2026-06"),
            ),
            ComponentDecisionInput(
                Component.FACTOR_H5_STATIC,
                "2026-07",
                invalidated_scopes=("moneyflow->rolling_5_20",),
                frozen_reuse=_evidence("factor:2026-07"),
            ),
            ComponentDecisionInput(
                Component.DOMESTIC_INDEX_CONTEXT,
                "2026-07",
                component_identity_equal=True,
                manifest_root_equal=True,
                source_equivalence_current=True,
                validation_current=True,
            ),
        ]
    )
    actions = {item.component: item.action for item in plan.actions}
    assert actions == {
        Component.DAILY_BIN: ComponentAction.INCREMENTAL,
        Component.MINUTE_BIN: ComponentAction.REUSE,
        Component.FACTOR_H5_STATIC: ComponentAction.SELECTIVE_REBUILD,
        Component.DOMESTIC_INDEX_CONTEXT: ComponentAction.NOOP,
    }
    assert len(plan.digest) == 64


def test_priority_prefers_reattest_then_resume_then_reuse() -> None:
    reattest = decide_component(
        ComponentDecisionInput(
            Component.DAILY_BIN,
            "all",
            source_equivalence_current=True,
            validation_compatibility=(ValidationCompatibility.VALIDATOR_STRENGTHENING_COMPATIBLE),
            checkpoint_valid=True,
            fingerprints_equal=True,
        )
    )
    assert reattest.action is ComponentAction.REATTEST

    resume = decide_component(
        ComponentDecisionInput(
            Component.DAILY_BIN,
            "all",
            checkpoint_valid=True,
            fingerprints_equal=True,
        )
    )
    assert resume.action is ComponentAction.RESUME


def test_incompatible_producer_cannot_use_incremental_shortcut() -> None:
    result = decide_component(
        ComponentDecisionInput(
            Component.DAILY_BIN,
            "2026-07",
            producer_compatible=False,
            appended_source_partitions=("2026-07",),
            frozen_reuse=_evidence("daily:2026-07"),
        )
    )
    assert result.action is ComponentAction.FULL_REBUILD


def test_reuse_requires_frozen_evidence() -> None:
    with pytest.raises(DecisionError, match="frozen baseline"):
        decide_component(
            ComponentDecisionInput(
                Component.MINUTE_BIN,
                "2026-07",
                fingerprints_equal=True,
            )
        )


def test_highest_cutoff_artifact_conflict_is_not_resolved_by_latest_order() -> None:
    values = [
        BaselineCandidate(
            "a",
            _digest("1"),
            _digest("2"),
            date(2026, 6, 30),
            _digest("3"),
            True,
            True,
            True,
        ),
        BaselineCandidate(
            "b",
            _digest("4"),
            _digest("5"),
            date(2026, 6, 30),
            _digest("6"),
            True,
            True,
            True,
        ),
    ]
    with pytest.raises(DecisionError) as captured:
        select_reuse_baseline(values, target_cutoff=date(2026, 7, 31))
    assert captured.value.code == "REUSE_BASELINE_CONFLICT"
