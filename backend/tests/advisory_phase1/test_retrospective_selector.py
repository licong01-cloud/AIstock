from __future__ import annotations

from datetime import UTC, datetime
import inspect

import pytest

from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.models import (
    HistoricalRangeArtifactKind,
    HistoricalRangeArtifactRefV1,
    HistoricalRangeLineageIdentity,
)
from backend.services.advisory_phase1.observation_selector import (
    FixtureObservationVersionSelector,
    ObservationSelectionPolicy,
    ObservationSelectionRequest,
    ObservationSelectionStatus,
    ObservationStatus,
    REASON_OBSERVATION_FORMAL_RANGE_FORBIDDEN,
    build_fixture_observation_version,
)
from backend.services.advisory_phase1.retrospective_selector import (
    RETROSPECTIVE_SELECTOR_POLICY_HASH,
    RetrospectiveObservationSelector,
    RetrospectiveObservationVersion,
    RetrospectiveSelectionRequest,
)
from backend.services.advisory_phase1.retrospective_selector_postgres import (
    PostgresRetrospectiveObservationSelector,
)


def _ref(kind: HistoricalRangeArtifactKind, char: str) -> HistoricalRangeArtifactRefV1:
    digest = char * 64
    namespace = {
        HistoricalRangeArtifactKind.REQUEST: "requests",
        HistoricalRangeArtifactKind.FROZEN_PROGRAM: "frozen-programs",
        HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT: "candidate-artifacts",
        HistoricalRangeArtifactKind.OUTCOME: "outcomes",
    }[kind]
    return HistoricalRangeArtifactRefV1(
        artifact_kind=kind,
        relative_path=f"{namespace}/{digest}.json",
        producer_contract_version="test_v1",
        payload_schema_version="test_v1",
        semantic_content_hash=digest,
        payload_sha256=digest,
        file_sha256=digest,
    )


def _lineage(*, run: str, day: str, candidate_char: str) -> HistoricalRangeLineageIdentity:
    return HistoricalRangeLineageIdentity(
        historical_range_request_ref=_ref(HistoricalRangeArtifactKind.REQUEST, "a"),
        historical_range_frozen_program_ref=_ref(HistoricalRangeArtifactKind.FROZEN_PROGRAM, "b"),
        range_run_id=run,
        range_day_run_id=day,
        candidate_artifact_ref=_ref(HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT, candidate_char),
        package_id="pkg-1",
        manifest_sha256="c" * 64,
        code_release_hash="d" * 64,
        signal_source_revision_set_hash="e" * 64,
        oos_interval_hash="f" * 64,
    )


def test_formal_selector_rejects_range_lineage_without_fallback() -> None:
    version = build_fixture_observation_version(
        canonical_signal_id="signal-1",
        observation_revision_no=1,
        supersedes_observation_version_id=None,
        evidence_available_at=datetime(2026, 7, 10, tzinfo=UTC),
        admission_scope_id="scope-1",
        admission_scope_hash="1" * 64,
        handoff_readiness_hash="2" * 64,
        signal_source_revision_set_id="source-1",
        signal_source_revision_set_hash="3" * 64,
        observation_status=ObservationStatus.COMPLETE,
        capability="FULL",
        stage_content_hashes=("4" * 64,),
    ).model_copy(update={"lineage_source_type": "HISTORICAL_RANGE_RESEARCH"})
    request = ObservationSelectionRequest(
        selection_policy=ObservationSelectionPolicy.EXACT_REVISION_V1,
        canonical_signal_id="signal-1",
        requested_source_cutoff=datetime(2026, 7, 11, tzinfo=UTC),
        required_observation_status=ObservationStatus.COMPLETE,
        required_capability="FULL",
        admission_scope_id="scope-1",
        admission_scope_hash="1" * 64,
        handoff_readiness_hash="2" * 64,
        signal_source_revision_set_hash="3" * 64,
        explicit_observation_version_id=version.observation_version_id,
    )
    result = FixtureObservationVersionSelector().select(request=request, observation_versions=(version,))
    assert result.selection_status is ObservationSelectionStatus.CONFLICT
    assert result.reason_codes == (REASON_OBSERVATION_FORMAL_RANGE_FORBIDDEN,)


def test_retrospective_selector_accepts_exact_range_and_preserves_all_lineage() -> None:
    payload = {
        "schema_version": "advisory_phase1_retrospective_observation_v1",
        "canonical_signal_id": "signal-1",
        "symbol": "000001.SZ",
        "stages": {"selection_effective": {"rank": 1}},
    }
    payload_hash = canonical_json_sha256(payload)
    first_lineage = _lineage(run="run-1", day="day-1", candidate_char="1")
    second_lineage = _lineage(run="run-2", day="day-2", candidate_char="2")
    outcome = _ref(HistoricalRangeArtifactKind.OUTCOME, "3")
    observations = tuple(
        RetrospectiveObservationVersion(
            canonical_signal_id="signal-1",
            observation_version_id=f"osv_{payload_hash[:20]}",
            observation_content_hash=payload_hash,
            evidence_available_at=datetime(2026, 7, 10, tzinfo=UTC),
            lineage=lineage,
            candidate_artifact_ref=lineage.candidate_artifact_ref,
            outcome_refs=(outcome,),
            observation_payload=payload,
        )
        for lineage in (first_lineage, second_lineage)
    )
    candidate_refs = tuple(
        sorted(
            (first_lineage.candidate_artifact_ref, second_lineage.candidate_artifact_ref),
            key=lambda item: item.semantic_content_hash,
        )
    )
    request = RetrospectiveSelectionRequest(
        range_run_ids=("run-1", "run-2"),
        candidate_artifact_refs=candidate_refs,
        outcome_refs=(outcome,),
        requested_source_cutoff=datetime(2026, 7, 11, tzinfo=UTC),
    )
    mappings = RetrospectiveObservationSelector().select(request=request, observations=observations)
    assert len(mappings) == 1
    assert mappings[0].selection_policy_hash == RETROSPECTIVE_SELECTOR_POLICY_HASH
    assert mappings[0].selected_lineage_refs == tuple(
        sorted(
            (
                str(first_lineage.range_lineage_identity_hash),
                str(second_lineage.range_lineage_identity_hash),
            )
        )
    )


def test_retrospective_selector_rejects_partial_candidate_or_outcome_coverage() -> None:
    payload = {
        "schema_version": "advisory_phase1_retrospective_observation_v1",
        "canonical_signal_id": "signal-1",
    }
    payload_hash = canonical_json_sha256(payload)
    first = _lineage(run="run-1", day="day-1", candidate_char="1")
    second = _lineage(run="run-2", day="day-2", candidate_char="2")
    outcome_a = _ref(HistoricalRangeArtifactKind.OUTCOME, "3")
    outcome_b = _ref(HistoricalRangeArtifactKind.OUTCOME, "4")
    observation = RetrospectiveObservationVersion(
        canonical_signal_id="signal-1",
        observation_version_id=f"osv_{payload_hash[:20]}",
        observation_content_hash=payload_hash,
        evidence_available_at=datetime(2026, 7, 10, tzinfo=UTC),
        lineage=first,
        candidate_artifact_ref=first.candidate_artifact_ref,
        outcome_refs=(outcome_a,),
        observation_payload=payload,
    )

    with pytest.raises(ValueError, match="ADVISORY_HR_DATASET_BRIDGE_LINEAGE_CONFLICT"):
        RetrospectiveObservationSelector().select(
            request=RetrospectiveSelectionRequest(
                range_run_ids=("run-1", "run-2"),
                candidate_artifact_refs=tuple(
                    sorted(
                        (first.candidate_artifact_ref, second.candidate_artifact_ref),
                        key=lambda item: item.semantic_content_hash,
                    )
                ),
                outcome_refs=(outcome_a,),
                requested_source_cutoff=datetime(2026, 7, 11, tzinfo=UTC),
            ),
            observations=(observation,),
        )

    with pytest.raises(ValueError, match="ADVISORY_HR_DATASET_BRIDGE_LINEAGE_CONFLICT"):
        RetrospectiveObservationSelector().select(
            request=RetrospectiveSelectionRequest(
                range_run_ids=("run-1",),
                candidate_artifact_refs=(first.candidate_artifact_ref,),
                outcome_refs=(outcome_a, outcome_b),
                requested_source_cutoff=datetime(2026, 7, 11, tzinfo=UTC),
            ),
            observations=(observation,),
        )


def test_postgres_retrospective_selector_qualifies_joined_ordering_keys() -> None:
    source = inspect.getsource(
        PostgresRetrospectiveObservationSelector.select_exact
    )

    assert "ORDER BY lineage.canonical_signal_id" in source
    assert "version.observation_version_id" in source
    assert "lineage.lineage_id" in source
