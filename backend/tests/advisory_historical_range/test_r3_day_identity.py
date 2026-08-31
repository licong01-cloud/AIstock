from __future__ import annotations

from backend.services.advisory_historical_range.models import (
    HistoricalRangeArtifactKind,
    build_day_input_hash_v3,
)
from backend.tests.advisory_historical_range.conftest import artifact_ref, digest


def _hash(*, candidate_seed: str = "candidate", mark_seed: str = "mark", predecessor_seed: str | None = None) -> str:
    return build_day_input_hash_v3(
        candidate_input_hash=digest("candidate-input"),
        candidate_artifact_ref=artifact_ref(HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT, candidate_seed),
        decision_mark_set_ref=artifact_ref(HistoricalRangeArtifactKind.DECISION_MARK_SET, mark_seed),
        decision_mark_policy_hash=digest("mark-policy"),
        previous_list_hash=digest("previous-list") if predecessor_seed is not None else None,
        previous_day_receipt_ref=(
            artifact_ref(HistoricalRangeArtifactKind.DAY_RECEIPT, predecessor_seed)
            if predecessor_seed is not None
            else None
        ),
        list_semantics_version="advisory_historical_range_list_semantics_v2",
        list_semantics_hash=digest("list-semantics"),
    )


def test_r3_day_input_hash_closes_every_direct_evidence_edge() -> None:
    baseline = _hash(predecessor_seed="predecessor")

    assert baseline != _hash(candidate_seed="candidate-other", predecessor_seed="predecessor")
    assert baseline != _hash(mark_seed="mark-other", predecessor_seed="predecessor")
    assert baseline != _hash(predecessor_seed="predecessor-other")


def test_r3_day_input_hash_requires_complete_predecessor_pair() -> None:
    candidate_ref = artifact_ref(HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT, "candidate")
    mark_ref = artifact_ref(HistoricalRangeArtifactKind.DECISION_MARK_SET, "mark")

    try:
        build_day_input_hash_v3(
            candidate_input_hash=digest("candidate-input"),
            candidate_artifact_ref=candidate_ref,
            decision_mark_set_ref=mark_ref,
            decision_mark_policy_hash=digest("mark-policy"),
            previous_list_hash=digest("previous-list"),
            previous_day_receipt_ref=None,
            list_semantics_version="advisory_historical_range_list_semantics_v2",
            list_semantics_hash=digest("list-semantics"),
        )
    except ValueError as exc:
        assert "previous list/day receipt" in str(exc)
    else:
        raise AssertionError("incomplete predecessor identity must fail visibly")
