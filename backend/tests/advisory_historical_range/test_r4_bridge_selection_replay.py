"""BUG-873: build-time selection replay against frozen label-capture groups.

Retrospective label capture runs the selector once per (range scope, label
source revision) group, so every frozen ``selected_mapping_hash`` binds that
group's selector context.  The dataset build must restore each group's frozen
selector context from the persisted label capture request, replay the
selector inside that group, and only accept mappings that reproduce the
frozen identities per signal:

1. replayed mappings are verified against the frozen ones field-by-field and
   merged deterministically by mapping identity, independent of batch order;
2. planned-label drift, source revision drift, stage evidence drift, replay
   drift, conflicting terminals for one signal, and one mapping id carrying
   conflicting content all fail closed;
3. an exact retry returns the same merged identities.
"""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from typing import Any

import pytest

from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.dataset_bridge import (
    HistoricalRangeDatasetBridgeError,
)
from backend.services.advisory_historical_range.dataset_bridge_postgres import (
    PostgresHistoricalRangeBridgeAdapters,
)
from backend.services.advisory_phase1.capture_foundation import CaptureBatch
from backend.services.advisory_phase1.label_capture import (
    RetrospectiveLabelCaptureBatchRequestV1,
)
from backend.tests.advisory_historical_range.test_r4_bridge_source_revision_union import (
    _label,
    _label_batch,
)
from backend.tests.advisory_historical_range.test_r4_dataset_bridge import (
    _request,
)
from backend.tests.advisory_historical_range.test_r4_dataset_bridge_postgres import (
    _projection_fixture,
)

_SOURCE = SimpleNamespace(
    source_revision_set_id="label-source-1",
    source_revision_set_hash="6" * 64,
)


def _fixture() -> dict[str, Any]:
    plan, observation = _projection_fixture()
    signal = plan.canonical_signal_id
    label = _label(
        signal=signal,
        symbol="AAA",
        decision=date(2026, 7, 6),
        exit_date=date(2026, 7, 8),
        revision_set=_SOURCE,
    )
    batch = _label_batch("ahr_lbl_cap_replay_a", _SOURCE)
    return {
        "plan": plan,
        "observation": observation,
        "signal": signal,
        "label": label,
        "batch": batch,
        "mapping": batch.request.selected_observation_mappings[0],
    }


def _adapter() -> PostgresHistoricalRangeBridgeAdapters:
    adapter = object.__new__(PostgresHistoricalRangeBridgeAdapters)
    adapter._conn_factory = None
    return adapter


def _fake_selector(replays: list[tuple[Any, ...]]):
    calls: list[tuple[int, int]] = []

    def _select(*, request, observations, labels):
        calls.append((len(observations), len(labels)))
        return replays[min(len(calls), len(replays)) - 1]

    _select.calls = calls
    return _select


def _replay_for(fixture: dict[str, Any], **overrides: Any) -> SimpleNamespace:
    mapping = fixture["mapping"]
    values = {
        "canonical_signal_id": fixture["signal"],
        "selected_mapping_id": str(mapping.selected_mapping_id),
        "selected_mapping_hash": str(mapping.selected_mapping_hash),
        "observation_version_id": str(mapping.terminal_observation_version_id),
        "observation_content_hash": str(mapping.terminal_observation_content_hash),
        "selected_lineage_refs": tuple(mapping.selected_lineage_refs),
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _batch_with_mapping(
    fixture: dict[str, Any],
    capture_id: str,
    **mapping_overrides: Any,
) -> CaptureBatch:
    template = fixture["batch"]
    mapping = template.request.selected_observation_mappings[0].model_copy(
        update=mapping_overrides
    )
    binding = template.request.binding.model_copy(
        update={
            "capture_batch_id": capture_id,
            "selected_observation_mapping_set_hash": canonical_json_sha256(
                [mapping.canonical_identity()]
            ),
            "binding_hash": None,
        }
    )
    request = template.request.model_copy(
        update={
            "capture_batch_id": capture_id,
            "binding": binding,
            "selected_observation_mappings": (mapping,),
            "capture_request_hash": None,
        }
    )
    request = RetrospectiveLabelCaptureBatchRequestV1.model_validate(
        request.model_dump(mode="python")
    )
    return template.model_copy(update={"request": request})


def _replay(
    adapter: PostgresHistoricalRangeBridgeAdapters,
    fixture: dict[str, Any],
    *,
    batches: tuple[CaptureBatch, ...],
    labels: tuple[Any, ...] | None = None,
) -> tuple[Any, ...]:
    return adapter._replay_selected_observation_mappings(
        request=_request(),
        batches=batches,
        observations=(fixture["observation"],),
        labels=(fixture["label"],) if labels is None else labels,
    )


def test_replay_merges_frozen_identities_deterministically() -> None:
    fixture = _fixture()
    adapter = _adapter()
    adapter._select_observations = _fake_selector([(_replay_for(fixture),)])

    first = _replay(adapter, fixture, batches=(fixture["batch"],))
    second = _replay(adapter, fixture, batches=(fixture["batch"],))

    assert first == second
    assert [item.identity_id for item in first] == ["mapping-1"]
    assert [item.identity_hash for item in first] == ["8" * 64]


def test_batch_order_does_not_change_the_merged_set() -> None:
    fixture = _fixture()
    other = _label_batch("ahr_lbl_cap_replay_b", _SOURCE)
    adapter = _adapter()
    adapter._select_observations = _fake_selector(
        [(_replay_for(fixture),), (_replay_for(fixture),)]
    )

    forward = _replay(adapter, fixture, batches=(fixture["batch"], other))
    reversed_order = _replay(adapter, fixture, batches=(other, fixture["batch"]))

    assert forward == reversed_order
    assert len(forward) == 1


def test_replay_drift_fails_closed() -> None:
    fixture = _fixture()
    adapter = _adapter()
    adapter._select_observations = _fake_selector(
        [(_replay_for(fixture, selected_mapping_hash="7" * 64),)]
    )

    with pytest.raises(
        HistoricalRangeDatasetBridgeError,
        match="differs from frozen label capture mapping",
    ):
        _replay(adapter, fixture, batches=(fixture["batch"],))


def test_missing_planned_label_fails_closed() -> None:
    fixture = _fixture()
    adapter = _adapter()
    adapter._select_observations = _fake_selector([(_replay_for(fixture),)])

    with pytest.raises(
        HistoricalRangeDatasetBridgeError,
        match="planned labels differ",
    ):
        _replay(adapter, fixture, batches=(fixture["batch"],), labels=())


def test_source_revision_drift_fails_closed() -> None:
    fixture = _fixture()
    drifted_label = _label(
        signal=fixture["signal"],
        symbol="AAA",
        decision=date(2026, 7, 6),
        exit_date=date(2026, 7, 8),
        revision_set=SimpleNamespace(
            source_revision_set_id="label-source-2",
            source_revision_set_hash="5" * 64,
        ),
    )
    adapter = _adapter()
    adapter._select_observations = _fake_selector([(_replay_for(fixture),)])

    with pytest.raises(
        HistoricalRangeDatasetBridgeError,
        match="source revision identity differs",
    ):
        _replay(
            adapter,
            fixture,
            batches=(fixture["batch"],),
            labels=(drifted_label,),
        )


def test_missing_label_captures_fail_closed() -> None:
    fixture = _fixture()
    adapter = _adapter()
    adapter._select_observations = _fake_selector([(_replay_for(fixture),)])

    with pytest.raises(
        HistoricalRangeDatasetBridgeError,
        match="requires frozen label captures",
    ):
        _replay(adapter, fixture, batches=())


def test_stage_evidence_drift_fails_closed() -> None:
    fixture = _fixture()
    tampered = _batch_with_mapping(
        fixture,
        "ahr_lbl_cap_replay_c",
        candidate_stage_evidence_id="ase_tampered",
    )
    adapter = _adapter()
    adapter._select_observations = _fake_selector([(_replay_for(fixture),)])

    with pytest.raises(
        HistoricalRangeDatasetBridgeError,
        match="stage evidence differs",
    ):
        _replay(adapter, fixture, batches=(tampered,))


def test_conflicting_terminals_for_one_signal_fail_closed() -> None:
    fixture = _fixture()
    conflicting = _batch_with_mapping(
        fixture,
        "ahr_lbl_cap_replay_d",
        selected_mapping_id="mapping-2",
        selected_mapping_hash="4" * 64,
        terminal_observation_version_id="osv_conflicting",
        terminal_observation_content_hash="3" * 64,
    )
    adapter = _adapter()
    adapter._select_observations = _fake_selector(
        [
            (_replay_for(fixture),),
            (
                _replay_for(
                    fixture,
                    selected_mapping_id="mapping-2",
                    selected_mapping_hash="4" * 64,
                    observation_version_id="osv_conflicting",
                    observation_content_hash="3" * 64,
                ),
            ),
        ]
    )

    with pytest.raises(
        HistoricalRangeDatasetBridgeError,
        match="conflicting terminals",
    ):
        _replay(adapter, fixture, batches=(fixture["batch"], conflicting))


def test_one_mapping_id_with_conflicting_content_fails_closed() -> None:
    fixture = _fixture()
    conflicting = _batch_with_mapping(
        fixture,
        "ahr_lbl_cap_replay_e",
        selected_mapping_hash="4" * 64,
    )
    adapter = _adapter()
    adapter._select_observations = _fake_selector(
        [
            (_replay_for(fixture),),
            (_replay_for(fixture, selected_mapping_hash="4" * 64),),
        ]
    )

    with pytest.raises(
        HistoricalRangeDatasetBridgeError,
        match="conflicting content",
    ):
        _replay(adapter, fixture, batches=(fixture["batch"], conflicting))
