"""BUG-874: the build request inherits the frozen union evidence registry identity.

The snapshot source revision union is re-derived from frozen label evidence
and inherits the common constituent header, including the query registry
hash.  The retrospective build request must declare exactly that evidence
identity - never a configured descriptor - so the seal-time authority check
and the relational closure check compare two views of the same evidence:

1. constituents sharing one registry hash resolve to a union whose registry
   hash the build request inherits exactly, field by field;
2. constituents with conflicting registry hashes fail closed before any
   union is built or frozen; no value is picked arbitrarily and no
   configured fallback is consulted;
3. the bridge service and composition no longer accept a configured query
   registry hash at all, so the configuration layer cannot override the
   evidence identity;
4. an exact retry returns the same resolve triple and the same build
   request identity;
5. the persisted union set, the build request, and everything derived from
   the request (snapshot manifest, verification receipt) share one registry
   hash;
6. the formal path is untouched: no formal module changes, and the formal
   suites keep passing unchanged.
"""

from __future__ import annotations

import inspect
from datetime import UTC, date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from backend.services.advisory_historical_range import composition
from backend.services.advisory_historical_range.dataset_bridge import (
    HistoricalRangeDatasetBridgeError,
)
from backend.services.advisory_historical_range.dataset_bridge_postgres import (
    PostgresHistoricalRangeBridgeAdapters,
)
from backend.services.advisory_historical_range.models import (
    HistoricalRangeArtifactKind,
    HistoricalRangeOutcomePolicyBundleV1,
    HistoricalRangePolicyComponentV1,
)
from backend.services.advisory_phase1.capture_foundation import (
    CaptureBatch,
    CaptureBatchStatus,
)
from backend.services.advisory_phase1.dataset_build import (
    FrozenIdentity,
)
from backend.tests.advisory_historical_range.test_r4_bridge_source_revision_union import (
    _LABEL_AS_OF_TS,
    _REGISTRY_HASH,
    _adapter,
    _FakeFreezeRepository,
    _FakeOutcomeSourceProvider,
    _label,
    _label_batch,
    _patch_dependencies,
    _resolve,
    _revision_set,
)
from backend.tests.advisory_historical_range.test_r4_dataset_bridge import (
    _ref,
    _request,
)
from backend.tests.advisory_historical_range.test_r4_dataset_bridge_postgres import (
    _capture_request,
    _projection_fixture,
)

_POLICY_COMPONENT_HASHES = {
    role: character * 64
    for role, character in zip(
        (
            "BARRIER",
            "BENCHMARK",
            "CALENDAR",
            "CASH_RETURN",
            "CORPORATE_ACTION",
            "COST",
            "EXECUTION",
            "MARKET_DATA",
            "TERMINAL",
        ),
        "abcdefabc",
        strict=True,
    )
}


def _policy() -> HistoricalRangeOutcomePolicyBundleV1:
    return HistoricalRangeOutcomePolicyBundleV1(
        package_id="pkg-1",
        manifest_sha256="1" * 64,
        alpha_mode="single_alpha",
        style_family="TREND",
        style_resolution_reason="FROZEN_TEST_POLICY",
        calendar_version="calendar-v1",
        calendar_hash=_POLICY_COMPONENT_HASHES["CALENDAR"],
        components=tuple(
            HistoricalRangePolicyComponentV1(
                component_role=role,
                component_ref=f"components/{role.lower()}-v1",
                component_hash=component_hash,
            )
            for role, component_hash in sorted(_POLICY_COMPONENT_HASHES.items())
        ),
        horizons=(1,),
        projections_by_horizon={1: ("RETURN_GROSS",)},
        candidate_reference_notional="100000",
        benchmark_portfolio_notional="100000",
    )


def _constituents(
    **set_b_overrides,
) -> tuple:
    set_a = _revision_set(symbol="AAA", start=date(2026, 7, 6), end=date(2026, 7, 8))
    set_b = _revision_set(
        symbol="BBB", start=date(2026, 7, 7), end=date(2026, 7, 9), **set_b_overrides
    )
    labels = (
        _label(
            signal="sig-a1",
            symbol="AAA",
            decision=date(2026, 7, 6),
            exit_date=date(2026, 7, 8),
            revision_set=set_a,
        ),
        _label(
            signal="sig-b1",
            symbol="BBB",
            decision=date(2026, 7, 7),
            exit_date=date(2026, 7, 9),
            revision_set=set_b,
        ),
    )
    batches = (
        _label_batch("ahr_lbl_cap_reg_a", set_a),
        _label_batch("ahr_lbl_cap_reg_b", set_b),
    )
    provider = _FakeOutcomeSourceProvider(
        {
            ("AAA", date(2026, 7, 6), date(2026, 7, 8)): set_a,
            ("BBB", date(2026, 7, 7), date(2026, 7, 9)): set_b,
        }
    )
    return set_a, set_b, labels, batches, provider


def _observation_batch() -> CaptureBatch:
    return CaptureBatch(
        request=_capture_request(),
        status=CaptureBatchStatus.COMPLETE,
        row_version=1,
        fencing_token=1,
        capture_attempt_no=1,
        membership_count=1,
        membership_hash="1" * 64,
        capture_receipt_hash="2" * 64,
        created_at=datetime(2026, 7, 25, tzinfo=UTC),
        updated_at=datetime(2026, 7, 25, tzinfo=UTC),
    )


def _build_request_for(
    adapter: PostgresHistoricalRangeBridgeAdapters,
    *,
    batches: tuple,
    labels: tuple,
    snapshot_source: tuple,
):
    """Drive the real ``_build_request`` with only unrelated seams stubbed.

    Selection replay and label selection are verified by their own suites;
    here they are stubbed so this test isolates the registry identity seam.
    """
    _, observation = _projection_fixture()
    policy = _policy()
    adapter._artifact_store = SimpleNamespace(
        load=lambda ref: SimpleNamespace(payload=policy.model_dump(mode="json"))
    )
    adapter._writer = SimpleNamespace(
        schemas={"observations": object()},
        schema_fingerprint=lambda role: "f" * 64,
    )
    adapter._partition_policy_id = "ADVISORY_PHASE1R_RETROSPECTIVE_RANGE_PARTITION_V1"
    adapter._code_commit = "c" * 40
    adapter._query_registry_version = "advisory_hr_r4_query_registry_v1"
    adapter._replay_selected_observation_mappings = (
        lambda **kwargs: (FrozenIdentity(identity_id="mapping-1", identity_hash="8" * 64),)
    )
    adapter._select_labels = (
        lambda labels: (FrozenIdentity(identity_id="label-1", identity_hash="9" * 64),)
    )
    # The label fixture pins the policy hash to "d"*64, so the request must
    # reference a policy bundle artifact under that exact payload hash.
    return adapter._build_request(
        request=_request(policy_ref=_ref(HistoricalRangeArtifactKind.REQUEST, "d")),
        batches=(*batches, _observation_batch()),
        observations=(observation,),
        labels=labels,
        snapshot_source=snapshot_source,
    )


def test_request_inherits_common_constituent_registry_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, _, labels, batches, provider = _constituents()
    freeze_repository = _FakeFreezeRepository()
    _patch_dependencies(monkeypatch, provider, freeze_repository)
    adapter = _adapter()

    snapshot_source = _resolve(adapter, batches=batches, labels=labels)
    assert snapshot_source[2] == _REGISTRY_HASH

    build_request = _build_request_for(
        adapter, batches=batches, labels=labels, snapshot_source=snapshot_source
    )

    assert build_request.query_registry_hash == _REGISTRY_HASH
    assert build_request.snapshot_source_revision_set_id == snapshot_source[0]
    assert build_request.snapshot_source_revision_set_hash == snapshot_source[1]
    # BUG-875: the request cutoff is the evidence cutoff projected onto the
    # UTC calendar; the fixture labels' decision dates end well before the
    # label-as-of bound, so this also proves no date_end fallback remains.
    assert build_request.requested_source_cutoff == date(2026, 7, 24)
    assert build_request.requested_source_cutoff == snapshot_source[3].astimezone(
        UTC
    ).date()
    assert build_request.requested_source_cutoff > build_request.date_end


def test_constituent_registry_conflict_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, _, labels, batches, provider = _constituents(query_registry_hash="f" * 64)
    freeze_repository = _FakeFreezeRepository()
    _patch_dependencies(monkeypatch, provider, freeze_repository)

    with pytest.raises(
        HistoricalRangeDatasetBridgeError,
        match="header identities differ",
    ):
        _resolve(_adapter(), batches=batches, labels=labels)
    # Fail before any union is built or frozen; nothing is picked
    # arbitrarily and no configured value is consulted.
    assert freeze_repository.frozen == []


def test_constituent_cutoff_conflict_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, _, labels, batches, provider = _constituents(
        requested_source_cutoff=datetime(2026, 7, 23, 23, 59, 59, tzinfo=UTC)
    )
    freeze_repository = _FakeFreezeRepository()
    _patch_dependencies(monkeypatch, provider, freeze_repository)

    with pytest.raises(
        HistoricalRangeDatasetBridgeError,
        match="header identities differ",
    ):
        _resolve(_adapter(), batches=batches, labels=labels)
    assert freeze_repository.frozen == []


def test_same_instant_in_another_timezone_normalizes_to_one_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The BBB constituent expresses the same cutoff instant at +08:00; the
    # formal builder normalizes to UTC, so the header check compares equal
    # and the projected request date is identical.
    _, _, labels, batches, provider = _constituents(
        requested_source_cutoff=_LABEL_AS_OF_TS.astimezone(
            timezone(timedelta(hours=8))
        )
    )
    freeze_repository = _FakeFreezeRepository()
    _patch_dependencies(monkeypatch, provider, freeze_repository)
    adapter = _adapter()

    snapshot_source = _resolve(adapter, batches=batches, labels=labels)
    build_request = _build_request_for(
        adapter, batches=batches, labels=labels, snapshot_source=snapshot_source
    )

    assert snapshot_source[3] == _LABEL_AS_OF_TS
    assert build_request.requested_source_cutoff == date(2026, 7, 24)


def test_config_layer_cannot_override_evidence_identity() -> None:
    adapter_params = inspect.signature(
        PostgresHistoricalRangeBridgeAdapters.__init__
    ).parameters
    assert "query_registry_hash" not in adapter_params
    service_params = inspect.signature(
        composition.build_historical_range_dataset_bridge_service
    ).parameters
    assert "query_registry_hash" not in service_params


def test_exact_retry_returns_the_same_registry_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, _, labels, batches, provider = _constituents()
    freeze_repository = _FakeFreezeRepository()
    _patch_dependencies(monkeypatch, provider, freeze_repository)
    adapter = _adapter()

    first_source = _resolve(adapter, batches=batches, labels=labels)
    second_source = _resolve(adapter, batches=batches, labels=labels)
    assert first_source == second_source

    first_request = _build_request_for(
        adapter, batches=batches, labels=labels, snapshot_source=first_source
    )
    second_request = _build_request_for(
        adapter, batches=batches, labels=labels, snapshot_source=second_source
    )
    assert first_request.model_dump(mode="json") == second_request.model_dump(mode="json")


def test_persisted_union_request_and_manifest_share_one_registry_hash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    set_a, _, labels, batches, provider = _constituents()
    freeze_repository = _FakeFreezeRepository()
    _patch_dependencies(monkeypatch, provider, freeze_repository)
    adapter = _adapter()

    snapshot_source = _resolve(adapter, batches=batches, labels=labels)
    build_request = _build_request_for(
        adapter, batches=batches, labels=labels, snapshot_source=snapshot_source
    )
    persisted_union = freeze_repository.frozen[0]

    # The seal-time authority check compares the persisted set row against
    # the build request, and the snapshot manifest and verification receipt
    # are written from the same request field, so all three views must carry
    # the one evidence-derived registry identity.
    assert persisted_union.query_registry_hash == set_a.query_registry_hash
    assert build_request.query_registry_hash == persisted_union.query_registry_hash
    assert build_request.query_registry_hash == snapshot_source[2]
    # BUG-875: the same three-way closure holds for the requested source
    # cutoff - the persisted union header, the resolve triple, and the
    # request field projected onto the UTC calendar.
    assert persisted_union.requested_source_cutoff == set_a.requested_source_cutoff
    assert snapshot_source[3] == persisted_union.requested_source_cutoff
    assert build_request.requested_source_cutoff == (
        persisted_union.requested_source_cutoff.astimezone(UTC).date()
    )
