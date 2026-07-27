from __future__ import annotations

from datetime import date, timedelta
from types import SimpleNamespace

import pytest

from backend.services.advisory_historical_range.models import (
    HistoricalRangeArtifactKind,
    HistoricalRangeArtifactRefV1,
    HistoricalRangeEvaluationWindowType,
    HistoricalRangeOutcomeProjection,
    HistoricalRangeOutcomeRefreshRequestV1,
    HistoricalRangeOutcomeRevisionReason,
    HistoricalRangeOutcomeSubjectType,
)
from backend.services.advisory_historical_range.outcome_planner import (
    HistoricalRangeOutcomePlanner,
    HistoricalRangeOutcomeSubjectSeedV1,
)


def _ref(kind: HistoricalRangeArtifactKind, char: str) -> HistoricalRangeArtifactRefV1:
    digest = char * 64
    namespace = {
        HistoricalRangeArtifactKind.REQUEST: "requests",
        HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT: "candidate-artifacts",
        HistoricalRangeArtifactKind.DAY_RECEIPT: "day-receipts",
        HistoricalRangeArtifactKind.RANGE_RECEIPT: "range-receipts",
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


class _Seeds:
    def __init__(self, seeds: tuple[HistoricalRangeOutcomeSubjectSeedV1, ...]) -> None:
        self._seeds = seeds
        self.requested_limits: list[int] = []

    def list_subject_seeds(self, *, request, after_key, limit):
        del request
        self.requested_limits.append(limit)
        selected = self._seeds
        if after_key is not None:
            selected = tuple(
                item for item in selected if (item.range_run_id, item.subject_type.value, item.subject_id) >= after_key
            )
        return selected[:limit]


class _Calendar:
    def timeline(
        self,
        *,
        policy_bundle_hash: str,
        decision_trade_date: date,
        horizon_trade_days: int,
    ):
        assert policy_bundle_hash == "a" * 64
        return (
            decision_trade_date,
            decision_trade_date + timedelta(days=1),
            decision_trade_date + timedelta(days=2),
            decision_trade_date + timedelta(days=horizon_trade_days + 2),
        )

    def next_trading_day(self, *, policy_bundle_hash: str, current_trade_date: date):
        assert policy_bundle_hash == "a" * 64
        return current_trade_date + timedelta(days=1)


def _request() -> HistoricalRangeOutcomeRefreshRequestV1:
    return HistoricalRangeOutcomeRefreshRequestV1(
        batch_id="batch-1",
        range_run_ids=("run-1",),
        label_as_of_trade_date=date(2026, 7, 21),
        policy_bundle_ref=_ref(HistoricalRangeArtifactKind.REQUEST, "a"),
        policy_bundle_hash="a" * 64,
        requested_subject_types=tuple(HistoricalRangeOutcomeSubjectType),
        requested_projections=tuple(sorted(HistoricalRangeOutcomeProjection, key=lambda item: item.value)),
        horizons=(1, 5),
        producer_code_hash="b" * 64,
        outcome_contract_version="r4_v1",
        operation_idempotency_key="planner-1",
        expected_batch_row_version=1,
    )


def test_refresh_request_default_projection_order_is_validator_compatible() -> None:
    payload = _request().model_dump(
        mode="python",
        exclude={"request_hash", "requested_projections"},
    )
    request = HistoricalRangeOutcomeRefreshRequestV1.model_validate(payload)

    assert request.requested_projections == (
        HistoricalRangeOutcomeProjection.EXECUTABLE,
        HistoricalRangeOutcomeProjection.RECOMMENDATION,
    )


def _seed(
    subject_type: HistoricalRangeOutcomeSubjectType,
    subject_id: str,
    ref: HistoricalRangeArtifactRefV1,
) -> HistoricalRangeOutcomeSubjectSeedV1:
    return HistoricalRangeOutcomeSubjectSeedV1(
        range_run_id="run-1",
        subject_type=subject_type,
        subject_id=subject_id,
        subject_ref=ref,
        decision_trade_date=date(2026, 7, 1),
        label_as_of_trade_date=date(2026, 7, 21),
        source_revision_refs=(ref,),
        intended_entry_trade_date=date(2026, 7, 2),
        earliest_sell_trade_date=date(2026, 7, 3),
        exit_trade_date=(date(2026, 7, 10) if subject_type is HistoricalRangeOutcomeSubjectType.EPISODE else None),
    )


def test_planner_expands_all_four_subjects_with_episode_zero_sentinel() -> None:
    candidate_ref = _ref(HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT, "c")
    day_ref = _ref(HistoricalRangeArtifactKind.DAY_RECEIPT, "d")
    range_ref = _ref(HistoricalRangeArtifactKind.RANGE_RECEIPT, "e")
    seeds = tuple(
        sorted(
            (
                _seed(HistoricalRangeOutcomeSubjectType.CANDIDATE, "candidate-1", candidate_ref),
                _seed(HistoricalRangeOutcomeSubjectType.EPISODE, "episode-1", day_ref),
                _seed(HistoricalRangeOutcomeSubjectType.LIST_VERSION, "list-1", day_ref),
                _seed(HistoricalRangeOutcomeSubjectType.RANGE, "run-1", range_ref),
            ),
            key=lambda item: (item.range_run_id, item.subject_type.value, item.subject_id),
        )
    )
    planner = HistoricalRangeOutcomePlanner(
        subject_provider=_Seeds(seeds),
        calendar=_Calendar(),
        producer_code_hash="b" * 64,
        outcome_contract_version="r4_v1",
    )

    result = planner.plan_slice(request=_request(), cursor=None, limit=100)

    episode_items = [item for item in result.items if item.subject_type is HistoricalRangeOutcomeSubjectType.EPISODE]
    fixed_items = [item for item in result.items if item.subject_type is not HistoricalRangeOutcomeSubjectType.EPISODE]
    assert len(episode_items) == 2
    assert all(
        item.evaluation_window_type is HistoricalRangeEvaluationWindowType.EPISODE_LIFECYCLE
        and item.horizon_trade_days == 0
        for item in episode_items
    )
    assert len(fixed_items) == 12
    assert all(
        item.evaluation_window_type is HistoricalRangeEvaluationWindowType.FIXED_HORIZON
        and item.horizon_trade_days in {1, 5}
        for item in fixed_items
    )


def test_planner_cursor_preserves_remaining_projection_horizon_items() -> None:
    candidate_ref = _ref(HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT, "c")
    planner = HistoricalRangeOutcomePlanner(
        subject_provider=_Seeds(
            (
                _seed(
                    HistoricalRangeOutcomeSubjectType.CANDIDATE,
                    "candidate-1",
                    candidate_ref,
                ),
            )
        ),
        calendar=_Calendar(),
        producer_code_hash="b" * 64,
        outcome_contract_version="r4_v1",
    )

    first = planner.plan_slice(request=_request(), cursor=None, limit=2)
    second = planner.plan_slice(request=_request(), cursor=first.next_cursor, limit=10)

    first_keys = {item.outcome_logical_id for item in first.items}
    second_keys = {item.outcome_logical_id for item in second.items}
    assert first_keys.isdisjoint(second_keys)
    assert len(first.items) + len(second.items) == 4


def test_planner_exhausts_after_cursor_consumes_last_subject() -> None:
    candidate_ref = _ref(HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT, "c")
    planner = HistoricalRangeOutcomePlanner(
        subject_provider=_Seeds(
            (
                _seed(
                    HistoricalRangeOutcomeSubjectType.CANDIDATE,
                    "candidate-1",
                    candidate_ref,
                ),
            )
        ),
        calendar=_Calendar(),
        producer_code_hash="b" * 64,
        outcome_contract_version="r4_v1",
    )

    first = planner.plan_slice(request=_request(), cursor=None, limit=4)
    assert first.exhausted
    assert first.next_cursor is None


def test_planner_cursor_advances_across_subject_boundary_without_duplicates() -> None:
    first_ref = _ref(HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT, "c")
    second_ref = _ref(HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT, "d")
    planner = HistoricalRangeOutcomePlanner(
        subject_provider=_Seeds(
            (
                _seed(HistoricalRangeOutcomeSubjectType.CANDIDATE, "candidate-1", first_ref),
                _seed(HistoricalRangeOutcomeSubjectType.CANDIDATE, "candidate-2", second_ref),
            )
        ),
        calendar=_Calendar(),
        producer_code_hash="b" * 64,
        outcome_contract_version="r4_v1",
    )

    first = planner.plan_slice(request=_request(), cursor=None, limit=3)
    second = planner.plan_slice(request=_request(), cursor=first.next_cursor, limit=3)
    third = planner.plan_slice(request=_request(), cursor=second.next_cursor, limit=3)

    items = (*first.items, *second.items, *third.items)
    assert len(items) == 8
    assert len({item.outcome_logical_id for item in items}) == 8
    assert third.exhausted


def test_planner_bounds_subject_prefetch_before_multi_item_expansion() -> None:
    candidate_ref = _ref(HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT, "c")
    provider = _Seeds(
        tuple(
            _seed(
                HistoricalRangeOutcomeSubjectType.CANDIDATE,
                f"candidate-{index:03d}",
                candidate_ref,
            )
            for index in range(100)
        )
    )
    planner = HistoricalRangeOutcomePlanner(
        subject_provider=provider,
        calendar=_Calendar(),
        producer_code_hash="b" * 64,
        outcome_contract_version="r4_v1",
    )

    result = planner.plan_slice(request=_request(), cursor=None, limit=500)

    assert provider.requested_limits == [51]
    assert len(result.items) == 204
    assert not result.exhausted


def test_planner_filters_an_exact_outcome_logical_id_subset() -> None:
    candidate_ref = _ref(HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT, "c")
    planner = HistoricalRangeOutcomePlanner(
        subject_provider=_Seeds(
            (
                _seed(
                    HistoricalRangeOutcomeSubjectType.CANDIDATE,
                    "candidate-1",
                    candidate_ref,
                ),
            )
        ),
        calendar=_Calendar(),
        producer_code_hash="b" * 64,
        outcome_contract_version="r4_v1",
    )
    complete = planner.plan_slice(request=_request(), cursor=None, limit=100)
    selected_id = str(complete.items[2].outcome_logical_id)
    payload = _request().model_dump(mode="python", exclude={"request_hash"})
    payload["requested_outcome_logical_ids"] = (selected_id,)
    targeted = planner.plan_slice(
        request=HistoricalRangeOutcomeRefreshRequestV1.model_validate(payload),
        cursor=None,
        limit=100,
    )

    assert tuple(str(item.outcome_logical_id) for item in targeted.items) == (
        selected_id,
    )


def test_refresh_request_rejects_unsorted_outcome_logical_ids() -> None:
    payload = _request().model_dump(mode="python", exclude={"request_hash"})
    payload["requested_outcome_logical_ids"] = ("logical-b", "logical-a")

    with pytest.raises(ValueError, match="requested_outcome_logical_ids"):
        HistoricalRangeOutcomeRefreshRequestV1.model_validate(payload)


def test_planner_scans_bounded_seed_pages_for_a_late_exact_logical_id() -> None:
    candidate_ref = _ref(HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT, "c")
    seeds = tuple(
        _seed(
            HistoricalRangeOutcomeSubjectType.CANDIDATE,
            f"candidate-{index:03d}",
            candidate_ref,
        )
        for index in range(100)
    )
    provider = _Seeds(seeds)
    planner = HistoricalRangeOutcomePlanner(
        subject_provider=provider,
        calendar=_Calendar(),
        producer_code_hash="b" * 64,
        outcome_contract_version="r4_v1",
    )
    late_planner = HistoricalRangeOutcomePlanner(
        subject_provider=_Seeds((seeds[-1],)),
        calendar=_Calendar(),
        producer_code_hash="b" * 64,
        outcome_contract_version="r4_v1",
    )
    selected_id = str(
        late_planner.plan_slice(request=_request(), cursor=None, limit=10)
        .items[0]
        .outcome_logical_id
    )
    payload = _request().model_dump(mode="python", exclude={"request_hash"})
    payload["requested_outcome_logical_ids"] = (selected_id,)

    targeted = planner.plan_slice(
        request=HistoricalRangeOutcomeRefreshRequestV1.model_validate(payload),
        cursor=None,
        limit=50,
    )

    assert tuple(str(item.outcome_logical_id) for item in targeted.items) == (
        selected_id,
    )
    assert targeted.exhausted
    assert provider.requested_limits == [51, 51]


def test_planner_does_not_reapply_an_already_completed_calculation_correction() -> None:
    candidate_ref = _ref(HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT, "c")
    seed = _seed(
        HistoricalRangeOutcomeSubjectType.CANDIDATE,
        "candidate-1",
        candidate_ref,
    )
    baseline = HistoricalRangeOutcomePlanner(
        subject_provider=_Seeds((seed,)),
        calendar=_Calendar(),
        producer_code_hash="b" * 64,
        outcome_contract_version="r4_v1",
    ).plan_slice(request=_request(), cursor=None, limit=100)
    source_hash = baseline.items[0].source_revision_set_hash
    predecessor = SimpleNamespace(
        producer_code_hash="b" * 64,
        outcome_contract_version="r4_v1",
        source_revision_set_hash=source_hash,
    )
    payload = _request().model_dump(mode="python", exclude={"request_hash"})
    payload.update(
        correction_reason=HistoricalRangeOutcomeRevisionReason.CALCULATION_CORRECTION,
        correction_evidence_ref=_ref(HistoricalRangeArtifactKind.REQUEST, "f"),
        operation_idempotency_key="planner-correction-retry",
    )
    planner = HistoricalRangeOutcomePlanner(
        subject_provider=_Seeds((seed,)),
        calendar=_Calendar(),
        producer_code_hash="b" * 64,
        outcome_contract_version="r4_v1",
        latest_outcome=lambda _logical_id: predecessor,
    )

    result = planner.plan_slice(
        request=HistoricalRangeOutcomeRefreshRequestV1.model_validate(payload),
        cursor=None,
        limit=100,
    )

    assert result.items == ()
    assert result.exhausted
