"""Retrospective capture readback regressions.

Covers two production bridge defect fixes:
- PostgreSQL retrospective capture-plan readback must restore the model's
  deterministic ``(decision_as_of_trade_date, canonical_signal_id)`` order
  instead of trusting the persisted ``plan_hash`` ordering, on both the
  capture-foundation loader and the snapshot-writer loader;
- PostgreSQL NUMERIC readback comparison must apply the same normalization
  to the actual and expected rows so equivalent decimals are not reported
  as conflicts while genuinely different values still fail closed.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest

from backend.services.advisory_historical_range.models import (
    HistoricalRangeArtifactKind,
)
from backend.services.advisory_phase1.capture_foundation import (
    OBSERVATION_CAPTURE_PURPOSE,
    RETROSPECTIVE_CAPTURE_BATCH_SCHEMA_VERSION,
    PostgresCaptureBatchRepository,
    RetrospectiveObservationCaptureBatchRequestV1,
    RetrospectiveObservationCaptureBinding,
    RetrospectiveObservationCapturePlan,
)
from backend.services.advisory_phase1.observation_capture import (
    materialize_retrospective_observation_row_bundle,
)
from backend.services.advisory_phase1.observation_capture_postgres import (
    _assert_row_equal,
)
from backend.services.advisory_phase1.snapshot_writer import (
    _load_persisted_capture_request_read_only,
)
from backend.services.advisory_phase1.source_ledger import SourceLedgerError
from backend.tests.advisory_historical_range.test_r4_bridge_maturity_dedup import (
    _lineage_for,
    _policy_with_projections,
)
from backend.tests.advisory_historical_range.test_r4_dataset_bridge import (
    _bridge_projection,
    _ref,
)


def _plan_variant(
    plan: RetrospectiveObservationCapturePlan,
    *,
    decision: date,
    target: date,
) -> RetrospectiveObservationCapturePlan:
    payload = plan.model_dump(mode="python")
    payload["decision_as_of_trade_date"] = decision
    payload["selection_as_of_trade_date"] = decision
    payload["target_trade_date"] = target
    payload["canonical_signal_scope_hash"] = "0" * 64
    payload["canonical_signal_id"] = f"acs_{'0' * 20}"
    payload.pop("plan_hash", None)
    return RetrospectiveObservationCapturePlan.model_validate(payload)


def _two_plans() -> tuple[
    RetrospectiveObservationCapturePlan, RetrospectiveObservationCapturePlan
]:
    candidate_ref = _ref(HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT, "b")
    policy = _policy_with_projections(("RETURN_GROSS",))
    policy_hash = str(policy.policy_bundle_hash)
    policy_ref = _ref(HistoricalRangeArtifactKind.REQUEST, "a").model_copy(
        update={
            "relative_path": f"requests/{policy_hash}.json",
            "payload_sha256": policy_hash,
            "semantic_content_hash": policy_hash,
            "file_sha256": policy_hash,
        }
    )
    lineage = _lineage_for(candidate_ref, day_run_id="day-1")
    plan, _stages, _fact, _owner, _payload = _bridge_projection(
        lineage=lineage,
        policy_ref=policy_ref,
        policy=policy,
    )
    earlier = _plan_variant(
        plan,
        decision=date(2026, 7, 3),
        target=date(2026, 7, 6),
    )
    later = _plan_variant(
        plan,
        decision=date(2026, 7, 6),
        target=date(2026, 7, 7),
    )
    # The deterministic model order is (decision date, signal id); the
    # persisted plan_hash order must differ for this pair or the regression
    # cannot prove that readback restores the model order.
    assert (earlier.decision_as_of_trade_date, earlier.canonical_signal_id) < (
        later.decision_as_of_trade_date,
        later.canonical_signal_id,
    )
    assert later.plan_hash is not None and earlier.plan_hash is not None
    assert later.plan_hash < earlier.plan_hash
    return earlier, later


def _capture_request(
    earlier: RetrospectiveObservationCapturePlan,
    later: RetrospectiveObservationCapturePlan,
) -> RetrospectiveObservationCaptureBatchRequestV1:
    capture_id = "ahr_obs_cap_readback_test"
    return RetrospectiveObservationCaptureBatchRequestV1(
        capture_batch_id=capture_id,
        binding=RetrospectiveObservationCaptureBinding(
            capture_batch_id=capture_id,
            capture_fencing_token=1,
            range_scope=earlier.range_scope,
        ),
        plans=(earlier, later),
    )


class _PlanRowsCursor:
    """Minimal cursor returning plan rows in persisted plan_hash order."""

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows
        self.queries: list[str] = []

    def execute(self, sql: str, _params: Any = None) -> None:
        self.queries.append(sql)

    def fetchall(self) -> list[dict[str, Any]]:
        return self._rows


def _plan_hash_ordered_rows(
    request: RetrospectiveObservationCaptureBatchRequestV1,
) -> list[dict[str, Any]]:
    return [
        {"plan_payload_jsonb": plan.model_dump(mode="json")}
        for plan in sorted(request.plans, key=lambda item: str(item.plan_hash))
    ]


def _batch_row(
    request: RetrospectiveObservationCaptureBatchRequestV1,
) -> dict[str, Any]:
    return {
        "capture_batch_id": request.capture_batch_id,
        "capture_purpose": OBSERVATION_CAPTURE_PURPOSE,
        "capture_request_schema_version": RETROSPECTIVE_CAPTURE_BATCH_SCHEMA_VERSION,
        "request_payload_jsonb": {},
        "binding_jsonb": request.binding.model_dump(mode="json"),
        "capture_request_hash": request.capture_request_hash,
        "capture_status": "COMPLETE",
        "row_version": 1,
        "fencing_token": 1,
        "lease_expires_at": None,
        "capture_attempt_no": 1,
        "predecessor_capture_batch_id": None,
        "membership_count": 2,
        "membership_hash": "9" * 64,
        "capture_receipt_hash": "8" * 64,
        "reason_codes": [],
        "created_at": datetime(2026, 7, 10, tzinfo=UTC),
        "updated_at": datetime(2026, 7, 10, tzinfo=UTC),
    }


def test_postgres_capture_plan_readback_restores_model_order() -> None:
    earlier, later = _two_plans()
    request = _capture_request(earlier, later)
    cursor = _PlanRowsCursor(_plan_hash_ordered_rows(request))
    # Sanity: the persisted order really is the wrong (plan_hash) order.
    assert [row["plan_payload_jsonb"]["decision_as_of_trade_date"] for row in cursor._rows] == [
        later.decision_as_of_trade_date.isoformat(),
        earlier.decision_as_of_trade_date.isoformat(),
    ]

    batch = PostgresCaptureBatchRepository._load_row(
        cursor, _batch_row(request), lock_children=False
    )

    assert isinstance(batch.request, RetrospectiveObservationCaptureBatchRequestV1)
    assert batch.request == request
    assert tuple(
        (plan.decision_as_of_trade_date, plan.canonical_signal_id)
        for plan in batch.request.plans
    ) == (
        (earlier.decision_as_of_trade_date, earlier.canonical_signal_id),
        (later.decision_as_of_trade_date, later.canonical_signal_id),
    )


def test_snapshot_writer_capture_plan_readback_restores_model_order() -> None:
    earlier, later = _two_plans()
    request = _capture_request(earlier, later)
    cursor = _PlanRowsCursor(_plan_hash_ordered_rows(request))

    parsed = _load_persisted_capture_request_read_only(cursor, _batch_row(request))

    assert isinstance(parsed, RetrospectiveObservationCaptureBatchRequestV1)
    assert parsed == request
    assert [plan.decision_as_of_trade_date for plan in parsed.plans] == [
        earlier.decision_as_of_trade_date,
        later.decision_as_of_trade_date,
    ]


def test_readback_numeric_equivalence_uses_symmetric_normalization() -> None:
    # PostgreSQL NUMERIC scale differences must not be reported as conflicts.
    _assert_row_equal(
        {"score": Decimal("0.90"), "ratio": Decimal("1.500"), "name": "x"},
        {"score": "0.9", "ratio": "1.5", "name": "x"},
        reason="equivalent numeric readback",
    )
    _assert_row_equal(
        {"score": Decimal("0.9"), "name": "x"},
        {"score": Decimal("0.90"), "name": "x"},
        reason="equivalent decimal readback",
    )

    with pytest.raises(SourceLedgerError, match="mismatched fields: score"):
        _assert_row_equal(
            {"score": Decimal("0.91"), "name": "x"},
            {"score": "0.9", "name": "x"},
            reason="genuinely different numeric readback",
        )


def test_retrospective_stage_candidate_scores_use_normalized_decimal_form() -> None:
    # BUG-862: production candidate facts carry score strings with trailing
    # zeros (for example "5.089064296700").  The NUMERIC(38,12) readback is
    # compared in normalized form, so a payload built with str(score) can
    # never round-trip; the retrospective builder must emit the same
    # normalized decimal representation at construction time.
    with pytest.raises(SourceLedgerError, match="mismatched fields"):
        _assert_row_equal(
            {"score_decimal": Decimal("5.089064296700")},
            {"score_decimal": "5.089064296700"},
            reason="trailing-zero payload cannot round-trip",
        )

    candidate_ref = _ref(HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT, "b")
    policy = _policy_with_projections(("RETURN_GROSS",))
    policy_hash = str(policy.policy_bundle_hash)
    policy_ref = _ref(HistoricalRangeArtifactKind.REQUEST, "a").model_copy(
        update={
            "relative_path": f"requests/{policy_hash}.json",
            "payload_sha256": policy_hash,
            "semantic_content_hash": policy_hash,
            "file_sha256": policy_hash,
        }
    )
    lineage = _lineage_for(candidate_ref, day_run_id="day-1")
    plan, stages, candidate_fact, _owner, _payload = _bridge_projection(
        lineage=lineage,
        policy_ref=policy_ref,
        policy=policy,
    )
    trailing_zero_fact = dict(candidate_fact)
    score_keys = (
        "alpha_raw_score",
        "hmm_adjusted_score",
        "risk_policy_adjusted_score",
        "selection_effective_score",
    )
    assert any(trailing_zero_fact.get(key) is not None for key in score_keys)
    for key in score_keys:
        if trailing_zero_fact.get(key) is not None:
            trailing_zero_fact[key] = "5.089064296700"

    rows = materialize_retrospective_observation_row_bundle(
        plan=plan,
        stage_payload=stages,
        candidate_fact=trailing_zero_fact,
        created_by_capture_batch_id="preflight",
    )

    included = [
        payload
        for payload in rows.candidate_payload_rows
        if payload["membership_status"] == "INCLUDED"
    ]
    assert included, "fixture must produce included stage candidates"
    for payload in included:
        assert payload["score_decimal"] == "5.0890642967"
        assert payload["input_score_decimal"] == "5.0890642967"
        # Exact NUMERIC round-trip: the normalized payload matches the
        # normalized database representation, so the readback contract holds.
        _assert_row_equal(
            {
                "score_decimal": Decimal("5.089064296700"),
                "input_score_decimal": Decimal("5.089064296700"),
            },
            {
                "score_decimal": payload["score_decimal"],
                "input_score_decimal": payload["input_score_decimal"],
            },
            reason="stage candidate score readback",
        )
