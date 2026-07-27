"""BUG-866: snapshot-level composite source revision union regressions.

R4 freezes one source revision set per subject (symbol x decision window x
label-as-of), so a multi-symbol program legitimately captures labels under
several per-subject sets.  The snapshot references one deterministic union of
every selected subject's source members:

1. each constituent set is re-derived through the standard outcome source
   builder and accepted only when its id/hash exactly matches the frozen
   calculation evidence;
2. all constituent header identities (query registry, requested cutoff,
   label-as-of, research-only, schema version) must be identical;
3. members merge by member_key: same key and same payload dedupes, same key
   with a different payload fails closed;
4. the union is built with the formal builder, frozen through the formal
   idempotent repository contract, contains exactly the constituent members,
   and every constituent is a strict subset;
5. an exact retry returns the same composite id/hash.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, time
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from backend.services.advisory_historical_range import (
    dataset_bridge_postgres as bridge_postgres,
)
from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.dataset_bridge import (
    HistoricalRangeBridgeLabelV1,
    HistoricalRangeDatasetBridgeError,
)
from backend.services.advisory_historical_range.dataset_bridge_postgres import (
    PostgresHistoricalRangeBridgeAdapters,
)
from backend.services.advisory_historical_range.models import (
    HistoricalRangeArtifactKind,
)
from backend.services.advisory_phase1.calculation_evidence import (
    CalculationEvidenceBundle,
)
from backend.services.advisory_phase1.capture_foundation import (
    CaptureBatch,
    CaptureBatchStatus,
    RetrospectiveLabelCaptureBatchRequestV1,
)
from backend.services.advisory_phase1.outcome_engine import (
    EntryStatus,
    MaturityStatus,
    OutcomeCalculationResult,
    OutcomeEventStatus,
    OutcomeOwner,
    OwnerType,
    Projection,
)
from backend.services.advisory_phase1.source_revision import (
    AvailabilityRequirement,
    SourceRevisionKind,
    SourceRevisionMemberInput,
    SourceRevisionSet,
    build_source_revision_set,
)
from backend.tests.advisory_historical_range.test_r4_dataset_bridge import (
    _ref,
    _request,
)
from backend.tests.advisory_historical_range.test_r4_dataset_bridge_postgres import (
    _label_capture_request,
)

_LABEL_AS_OF = date(2026, 7, 24)
_LABEL_AS_OF_TS = datetime.combine(_LABEL_AS_OF, time(23, 59, 59), tzinfo=UTC)
_REGISTRY_HASH = "a" * 64


def _member(
    *,
    symbol: str,
    start: date,
    end: date,
    role: str,
    content_tag: str,
) -> SourceRevisionMemberInput:
    partition = {
        "symbol": symbol,
        "start_trade_date": start,
        "end_trade_date": end,
        "label_as_of_trade_date": _LABEL_AS_OF,
    }
    content_hash = canonical_json_sha256(
        {"role": role, "partition": partition, "tag": content_tag}
    )
    return SourceRevisionMemberInput(
        source_role=role,
        dataset_name=f"market.{role.lower()}",
        query_template_id=f"advisory_hr_r4_{role.lower()}",
        query_template_version="1",
        query_template_hash=canonical_json_sha256({"role": role, "version": "1"}),
        bound_parameter_hash=canonical_json_sha256(partition),
        enforced_cutoff_predicate_hash=canonical_json_sha256({"predicate": "cutoff"}),
        partition_key=partition,
        revision_kind=SourceRevisionKind.PARTITION_CONTENT_HASH,
        revision_id=f"ahr-r4-{role.lower()}-{content_hash[:32]}",
        availability_requirement=AvailabilityRequirement.LABEL_AS_OF,
        business_min_date=start,
        business_max_date=end,
        available_at_min=_LABEL_AS_OF_TS,
        available_at_max=_LABEL_AS_OF_TS,
        schema_fingerprint=f"advisory_hr_r4_{role.lower()}_v1",
        row_count=3,
        partition_content_hash=content_hash,
        quality_status="PASS",
        reason_codes=(),
        research_only=True,
    )


def _revision_set(
    *,
    symbol: str,
    start: date,
    end: date,
    query_registry_hash: str = _REGISTRY_HASH,
    requested_source_cutoff: datetime = _LABEL_AS_OF_TS,
    label_as_of_ts: datetime = _LABEL_AS_OF_TS,
    content_tag: str = "base",
) -> SourceRevisionSet:
    # research_only is not a parameter: the formal builder itself enforces
    # research-only sets, so the union resolver can only ever see True.
    return build_source_revision_set(
        query_registry_hash=query_registry_hash,
        requested_source_cutoff=requested_source_cutoff,
        label_as_of_ts=label_as_of_ts,
        research_only=True,
        members=[
            _member(
                symbol=symbol, start=start, end=end, role=role, content_tag=content_tag
            )
            for role in ("PRICE_PATH", "ADJUSTMENT_PATH", "TRADABILITY_PATH")
        ],
    )


def _evidence(revision_set: SourceRevisionSet) -> CalculationEvidenceBundle:
    return CalculationEvidenceBundle(
        evidence_payload={
            "label_source_revision_set_id": revision_set.source_revision_set_id,
            "label_source_revision_set_hash": revision_set.source_revision_set_hash,
        }
    )


def _label(
    *,
    signal: str,
    symbol: str,
    decision: date,
    exit_date: date | None,
    revision_set: SourceRevisionSet,
) -> HistoricalRangeBridgeLabelV1:
    evidence = _evidence(revision_set)
    owner = OutcomeOwner(
        owner_type=OwnerType.CANDIDATE,
        owner_key=f"owner-{signal}",
        canonical_signal_id=signal,
        observation_version_id=f"osv_{signal}",
        candidate_stage_evidence_id=f"ase_{signal}",
        symbol=symbol,
        decision_as_of_trade_date=decision,
    )
    result = OutcomeCalculationResult(
        owner=owner,
        projection=Projection.RETURN_GROSS,
        horizon_trading_days=1,
        decision_trade_date=decision,
        intended_entry_trade_date=decision,
        earliest_sell_eligible_trade_date=exit_date or _LABEL_AS_OF,
        exit_trade_date=exit_date,
        scheduled_maturity_ts=_LABEL_AS_OF_TS,
        maturity_status=MaturityStatus.MATURED,
        outcome_event_status=OutcomeEventStatus.NONE,
        entry_status=EntryStatus.EXECUTABLE,
        projection_value_decimal=Decimal("1.5"),
        source_closed_at=_LABEL_AS_OF_TS,
        calculation_evidence=evidence,
    )
    outcome_ref = _ref(HistoricalRangeArtifactKind.OUTCOME, canonical_json_sha256(signal)[0])
    return HistoricalRangeBridgeLabelV1(
        canonical_signal_id=signal,
        label_version_id=f"olbv_{signal}",
        label_content_hash="b" * 64,
        observation_version_id=f"osv_{signal}",
        symbol=symbol,
        projection=Projection.RETURN_GROSS,
        horizon_trade_days=1,
        outcome_version_id=f"ov_{signal}",
        outcome_content_hash="c" * 64,
        outcome_ref=outcome_ref,
        label_as_of_trade_date=_LABEL_AS_OF,
        accepted_outcome_refs=(outcome_ref,),
        historical_range_policy_bundle_hash="d" * 64,
        historical_range_policy_bundle_ref=_ref(HistoricalRangeArtifactKind.REQUEST, "f"),
        policy_component_set_hash="e" * 64,
        outcome_result=result,
        calculation_evidence=evidence,
    )


def _label_batch(capture_id: str, revision_set: SourceRevisionSet) -> CaptureBatch:
    template = _label_capture_request()
    binding = template.binding.model_copy(
        update={
            "capture_batch_id": capture_id,
            "label_source_revision_set_id": revision_set.source_revision_set_id,
            "label_source_revision_set_hash": revision_set.source_revision_set_hash,
            "binding_hash": None,
        }
    )
    request = template.model_copy(
        update={
            "capture_batch_id": capture_id,
            "binding": binding,
            "label_source_revision_set_id": revision_set.source_revision_set_id,
            "label_source_revision_set_hash": revision_set.source_revision_set_hash,
            "capture_request_hash": None,
        }
    )
    # Re-validate so the deterministic request hash is recomputed for the
    # updated identity instead of carrying the template's stale digest.
    request = RetrospectiveLabelCaptureBatchRequestV1.model_validate(
        request.model_dump(mode="python")
    )
    return CaptureBatch(
        request=request,
        status=CaptureBatchStatus.COMPLETE,
        row_version=1,
        fencing_token=1,
        capture_attempt_no=1,
        membership_count=2,
        membership_hash="1" * 64,
        capture_receipt_hash="2" * 64,
        created_at=datetime(2026, 7, 25, tzinfo=UTC),
        updated_at=datetime(2026, 7, 25, tzinfo=UTC),
    )


class _FakeOutcomeSourceProvider:
    def __init__(self, sets: dict[tuple[str, date, date], SourceRevisionSet]) -> None:
        self._sets = sets
        self.request_hashes: list[str] = []
        self.calls: list[tuple[str, date, date, datetime]] = []

    def begin_operation(self, request_hash: str) -> None:
        self.request_hashes.append(request_hash)

    def resolve_source_revision_bundle(
        self,
        *,
        symbol: str,
        start_trade_date: date,
        end_trade_date: date,
        label_as_of_ts: datetime,
    ) -> Any:
        self.calls.append((symbol, start_trade_date, end_trade_date, label_as_of_ts))
        return SimpleNamespace(
            source_revision_set=self._sets[(symbol, start_trade_date, end_trade_date)]
        )


class _FakeFreezeRepository:
    def __init__(self) -> None:
        self.frozen: list[SourceRevisionSet] = []

    def freeze(self, revision_set: SourceRevisionSet) -> SourceRevisionSet:
        self.frozen.append(revision_set)
        return revision_set


def _adapter() -> PostgresHistoricalRangeBridgeAdapters:
    adapter = object.__new__(PostgresHistoricalRangeBridgeAdapters)
    adapter._conn_factory = None
    return adapter


def _patch_dependencies(
    monkeypatch: pytest.MonkeyPatch,
    provider: _FakeOutcomeSourceProvider,
    freeze_repository: _FakeFreezeRepository,
) -> None:
    monkeypatch.setattr(
        bridge_postgres,
        "PostgresHistoricalRangeOutcomeSourceProvider",
        lambda *, conn_factory: provider,
    )
    monkeypatch.setattr(
        bridge_postgres,
        "PostgresSourceRevisionRepository",
        lambda *, conn_factory: freeze_repository,
    )


def _resolve(
    adapter: PostgresHistoricalRangeBridgeAdapters,
    *,
    batches: tuple[CaptureBatch, ...],
    labels: tuple[HistoricalRangeBridgeLabelV1, ...],
) -> tuple[str, str, str, datetime]:
    return adapter._resolve_snapshot_source_revision(
        request=_request(),
        batches=batches,
        labels=labels,
    )


def test_multi_subject_union_freezes_and_is_exactly_retryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    set_a = _revision_set(symbol="AAA", start=date(2026, 7, 6), end=date(2026, 7, 8))
    set_b = _revision_set(symbol="BBB", start=date(2026, 7, 7), end=date(2026, 7, 9))
    labels = (
        _label(signal="sig-a1", symbol="AAA", decision=date(2026, 7, 6), exit_date=date(2026, 7, 8), revision_set=set_a),
        _label(signal="sig-a2", symbol="AAA", decision=date(2026, 7, 6), exit_date=date(2026, 7, 8), revision_set=set_a),
        _label(signal="sig-b1", symbol="BBB", decision=date(2026, 7, 7), exit_date=date(2026, 7, 9), revision_set=set_b),
    )
    batches = (
        _label_batch("ahr_lbl_cap_a", set_a),
        _label_batch("ahr_lbl_cap_b", set_b),
    )
    provider = _FakeOutcomeSourceProvider(
        {
            ("AAA", date(2026, 7, 6), date(2026, 7, 8)): set_a,
            ("BBB", date(2026, 7, 7), date(2026, 7, 9)): set_b,
        }
    )
    freeze_repository = _FakeFreezeRepository()
    _patch_dependencies(monkeypatch, provider, freeze_repository)
    adapter = _adapter()

    first = _resolve(adapter, batches=batches, labels=labels)
    second = _resolve(adapter, batches=batches, labels=labels)

    # (5) an exact retry returns the same composite identity and the formal
    # freeze is invoked with the identical union both times.
    assert first == second
    union_id, union_hash, union_registry_hash, union_cutoff = first
    assert union_registry_hash == _REGISTRY_HASH
    assert union_cutoff == _LABEL_AS_OF_TS
    assert union_id not in {
        set_a.source_revision_set_id,
        set_b.source_revision_set_id,
    }
    assert [item.source_revision_set_id for item in freeze_repository.frozen] == [
        union_id,
        union_id,
    ]
    union = freeze_repository.frozen[0]
    assert union.source_revision_set_hash == union_hash
    # (4) union members are exactly the constituent members; every
    # constituent is a strict subset; no missing or extra members.
    union_members = {member.member_key: member for member in union.members}
    assert len(union.members) == len(set_a.members) + len(set_b.members)
    for revision_set in (set_a, set_b):
        for member in revision_set.members:
            assert union_members[member.member_key].content_payload() == (
                member.content_payload()
            )
    # (2) the union header matches the shared constituent header identity.
    assert union.query_registry_hash == set_a.query_registry_hash
    assert union.requested_source_cutoff == set_a.requested_source_cutoff
    assert union.label_as_of_ts == set_a.label_as_of_ts
    assert union.research_only is True
    # (1) each constituent was re-derived once per resolve from the frozen
    # subject timeline; the duplicate AAA group shares one derivation.
    expected_calls = {
        ("AAA", date(2026, 7, 6), date(2026, 7, 8), _LABEL_AS_OF_TS),
        ("BBB", date(2026, 7, 7), date(2026, 7, 9), _LABEL_AS_OF_TS),
    }
    assert set(provider.calls[:2]) == expected_calls
    assert set(provider.calls[2:]) == expected_calls
    assert len(provider.calls) == 4
    assert len(provider.request_hashes) == 2


@pytest.mark.parametrize(
    "variant",
    (
        {"query_registry_hash": "f" * 64},
        {"requested_source_cutoff": datetime(2026, 7, 23, 23, 59, 59, tzinfo=UTC)},
        {"label_as_of_ts": datetime(2026, 7, 25, 23, 59, 59, tzinfo=UTC)},
    ),
)
def test_constituent_header_mismatch_fails_closed(
    monkeypatch: pytest.MonkeyPatch, variant: dict[str, Any]
) -> None:
    set_a = _revision_set(symbol="AAA", start=date(2026, 7, 6), end=date(2026, 7, 8))
    set_b = _revision_set(
        symbol="BBB", start=date(2026, 7, 7), end=date(2026, 7, 9), **variant
    )
    labels = (
        _label(signal="sig-a1", symbol="AAA", decision=date(2026, 7, 6), exit_date=date(2026, 7, 8), revision_set=set_a),
        _label(signal="sig-b1", symbol="BBB", decision=date(2026, 7, 7), exit_date=date(2026, 7, 9), revision_set=set_b),
    )
    batches = (
        _label_batch("ahr_lbl_cap_a", set_a),
        _label_batch("ahr_lbl_cap_b", set_b),
    )
    provider = _FakeOutcomeSourceProvider(
        {
            ("AAA", date(2026, 7, 6), date(2026, 7, 8)): set_a,
            ("BBB", date(2026, 7, 7), date(2026, 7, 9)): set_b,
        }
    )
    freeze_repository = _FakeFreezeRepository()
    _patch_dependencies(monkeypatch, provider, freeze_repository)

    with pytest.raises(
        HistoricalRangeDatasetBridgeError,
        match="header identities differ",
    ):
        _resolve(_adapter(), batches=batches, labels=labels)
    assert freeze_repository.frozen == []


def test_conflicting_derivation_inputs_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    set_a = _revision_set(symbol="AAA", start=date(2026, 7, 6), end=date(2026, 7, 8))
    # Two labels claim the same evidence pair but carry different frozen
    # timelines; the resolver must never pick one derivation arbitrarily.
    labels = (
        _label(signal="sig-a1", symbol="AAA", decision=date(2026, 7, 6), exit_date=date(2026, 7, 8), revision_set=set_a),
        _label(signal="sig-a2", symbol="AAA", decision=date(2026, 7, 6), exit_date=date(2026, 7, 9), revision_set=set_a),
    )
    batches = (_label_batch("ahr_lbl_cap_a", set_a),)
    provider = _FakeOutcomeSourceProvider({})
    freeze_repository = _FakeFreezeRepository()
    _patch_dependencies(monkeypatch, provider, freeze_repository)

    with pytest.raises(
        HistoricalRangeDatasetBridgeError,
        match="conflicting derivation inputs",
    ):
        _resolve(_adapter(), batches=batches, labels=labels)
    assert freeze_repository.frozen == []


def test_same_key_different_payload_conflicts_after_grouping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    set_a = _revision_set(symbol="AAA", start=date(2026, 7, 6), end=date(2026, 7, 8))
    # A second group whose set shares one member key with set_a but carries a
    # drifted payload under that key.
    drifted_member = _member(
        symbol="AAA",
        start=date(2026, 7, 6),
        end=date(2026, 7, 8),
        role="PRICE_PATH",
        content_tag="drifted",
    )
    kept = [member for member in set_a.members if member.source_role != "PRICE_PATH"]
    set_b = build_source_revision_set(
        query_registry_hash=_REGISTRY_HASH,
        requested_source_cutoff=_LABEL_AS_OF_TS,
        label_as_of_ts=_LABEL_AS_OF_TS,
        research_only=True,
        members=[drifted_member, *kept],
    )
    assert drifted_member.member_key in {
        member.member_key for member in set_a.members
    }
    labels = (
        _label(signal="sig-a1", symbol="AAA", decision=date(2026, 7, 6), exit_date=date(2026, 7, 8), revision_set=set_a),
        _label(signal="sig-b1", symbol="BBB", decision=date(2026, 7, 7), exit_date=date(2026, 7, 9), revision_set=set_b),
    )
    batches = (
        _label_batch("ahr_lbl_cap_a", set_a),
        _label_batch("ahr_lbl_cap_b", set_b),
    )
    provider = _FakeOutcomeSourceProvider(
        {
            ("AAA", date(2026, 7, 6), date(2026, 7, 8)): set_a,
            ("BBB", date(2026, 7, 7), date(2026, 7, 9)): set_b,
        }
    )
    freeze_repository = _FakeFreezeRepository()
    _patch_dependencies(monkeypatch, provider, freeze_repository)

    with pytest.raises(
        HistoricalRangeDatasetBridgeError,
        match="conflict under one member key",
    ):
        _resolve(_adapter(), batches=batches, labels=labels)
    assert freeze_repository.frozen == []


def test_rederived_set_differing_from_evidence_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evidence_set = _revision_set(symbol="AAA", start=date(2026, 7, 6), end=date(2026, 7, 8))
    drifted_set = _revision_set(
        symbol="AAA", start=date(2026, 7, 6), end=date(2026, 7, 8), content_tag="drifted"
    )
    labels = (
        _label(signal="sig-a1", symbol="AAA", decision=date(2026, 7, 6), exit_date=date(2026, 7, 8), revision_set=evidence_set),
    )
    batches = (_label_batch("ahr_lbl_cap_a", evidence_set),)
    provider = _FakeOutcomeSourceProvider(
        {
            ("AAA", date(2026, 7, 6), date(2026, 7, 8)): drifted_set,
        }
    )
    freeze_repository = _FakeFreezeRepository()
    _patch_dependencies(monkeypatch, provider, freeze_repository)

    with pytest.raises(
        HistoricalRangeDatasetBridgeError,
        match="differs from frozen label evidence",
    ):
        _resolve(_adapter(), batches=batches, labels=labels)
    assert freeze_repository.frozen == []


def test_capture_pairs_differing_from_label_evidence_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    set_a = _revision_set(symbol="AAA", start=date(2026, 7, 6), end=date(2026, 7, 8))
    stranger = _revision_set(symbol="ZZZ", start=date(2026, 7, 6), end=date(2026, 7, 8))
    labels = (
        _label(signal="sig-a1", symbol="AAA", decision=date(2026, 7, 6), exit_date=date(2026, 7, 8), revision_set=set_a),
    )
    batches = (_label_batch("ahr_lbl_cap_a", stranger),)
    provider = _FakeOutcomeSourceProvider({})
    freeze_repository = _FakeFreezeRepository()
    _patch_dependencies(monkeypatch, provider, freeze_repository)

    with pytest.raises(
        HistoricalRangeDatasetBridgeError,
        match="differ from frozen label evidence",
    ):
        _resolve(_adapter(), batches=batches, labels=labels)
    assert freeze_repository.frozen == []


def test_missing_exit_timeline_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    set_a = _revision_set(symbol="AAA", start=date(2026, 7, 6), end=date(2026, 7, 8))
    labels = (
        _label(signal="sig-a1", symbol="AAA", decision=date(2026, 7, 6), exit_date=None, revision_set=set_a),
    )
    batches = (_label_batch("ahr_lbl_cap_a", set_a),)
    provider = _FakeOutcomeSourceProvider({})
    freeze_repository = _FakeFreezeRepository()
    _patch_dependencies(monkeypatch, provider, freeze_repository)

    with pytest.raises(
        HistoricalRangeDatasetBridgeError,
        match="lacks the frozen exit timeline",
    ):
        _resolve(_adapter(), batches=batches, labels=labels)
    assert freeze_repository.frozen == []
