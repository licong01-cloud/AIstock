"""BUG-926 snapshot-level policy-set authority regressions."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from backend.services.advisory_historical_range.artifact_store import (
    HistoricalRangeArtifactStore,
)
from backend.services.advisory_historical_range.canonical import canonical_json_sha256
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


_REQUEST_HASH = "1" * 64
_ROLES = (
    "BARRIER",
    "BENCHMARK",
    "CALENDAR",
    "CASH_RETURN",
    "CORPORATE_ACTION",
    "COST",
    "EXECUTION",
    "MARKET_DATA",
    "TERMINAL",
)


def _publish_policy(
    store: HistoricalRangeArtifactStore,
    *,
    package_id: str,
    marker: str,
    changed_role: str | None = None,
):
    component_hashes = {role: (("f" if role == changed_role else marker) * 64) for role in _ROLES}
    bundle = HistoricalRangeOutcomePolicyBundleV1(
        package_id=package_id,
        manifest_sha256=marker * 64,
        alpha_mode="single_alpha" if package_id == "pkg-a" else "multi_alpha",
        style_family="TREND" if package_id == "pkg-a" else "REVERSAL",
        style_resolution_reason="FROZEN_TEST_POLICY",
        calendar_version="calendar-v1",
        calendar_hash=component_hashes["CALENDAR"],
        components=tuple(
            HistoricalRangePolicyComponentV1(
                component_role=role,
                component_ref=f"components/{package_id}/{role.lower()}.json",
                component_hash=component_hashes[role],
            )
            for role in _ROLES
        ),
        horizons=(5,),
        projections_by_horizon={5: ("RETURN_GROSS",)},
        candidate_reference_notional="100000",
        benchmark_portfolio_notional="100000",
    )
    stored = store.publish_payload(
        artifact_kind=HistoricalRangeArtifactKind.REQUEST,
        producer_contract_version="test_range_policy_v1",
        payload_schema_version=bundle.schema_version,
        resolved_request_hash=_REQUEST_HASH,
        payload=bundle.model_dump(mode="json", exclude={"policy_bundle_id", "policy_bundle_hash"}),
    )
    assert stored.ref.payload_sha256 == bundle.policy_bundle_hash
    return bundle, stored.ref, component_hashes


def _component_set_hash(component_hashes: dict[str, str]) -> str:
    return canonical_json_sha256(
        [{"component_role": role, "component_hash": component_hashes[role]} for role in sorted(component_hashes)]
    )


def _adapter(store: HistoricalRangeArtifactStore):
    adapter = object.__new__(PostgresHistoricalRangeBridgeAdapters)
    adapter._artifact_store = store
    return adapter


def _request(*entries):
    return SimpleNamespace(
        request_hash=_REQUEST_HASH,
        policy_bundle_refs=tuple(entry[1] for entry in entries),
        policy_component_hashes={entry[1].payload_sha256: entry[2] for entry in entries},
    )


def _label(entry, *, signal: str):
    return SimpleNamespace(
        canonical_signal_id=signal,
        historical_range_policy_bundle_ref=entry[1],
        historical_range_policy_bundle_hash=entry[1].payload_sha256,
        policy_component_set_hash=_component_set_hash(entry[2]),
    )


def test_single_policy_snapshot_retains_original_identity(tmp_path) -> None:
    store = HistoricalRangeArtifactStore(root=tmp_path / "artifact-store")
    policy = _publish_policy(store, package_id="pkg-a", marker="a")

    authority = _adapter(store)._resolve_snapshot_policy_authority(
        request=_request(policy),
        labels=(_label(policy, signal="signal-a"),),
    )

    assert authority.policy_bundle_id == policy[0].policy_bundle_id
    assert authority.policy_bundle_hash == policy[1].payload_sha256
    assert authority.policy_bundle_ref == policy[1]
    assert authority.component_hashes == policy[2]
    assert authority.component_set_hash == _component_set_hash(policy[2])


def test_multi_policy_snapshot_publishes_deterministic_complete_union(tmp_path) -> None:
    store = HistoricalRangeArtifactStore(root=tmp_path / "artifact-store")
    policy_a = _publish_policy(store, package_id="pkg-a", marker="a")
    policy_b = _publish_policy(
        store,
        package_id="pkg-b",
        marker="b",
        changed_role="BENCHMARK",
    )
    adapter = _adapter(store)
    request = _request(policy_a, policy_b)
    labels = (
        _label(policy_b, signal="signal-b"),
        _label(policy_a, signal="signal-a"),
    )

    first = adapter._resolve_snapshot_policy_authority(
        request=request,
        labels=labels,
    )
    second = adapter._resolve_snapshot_policy_authority(
        request=request,
        labels=tuple(reversed(labels)),
    )

    assert first == second
    assert first.policy_bundle_ref not in {policy_a[1], policy_b[1]}
    assert first.policy_bundle_hash not in {
        policy_a[1].payload_sha256,
        policy_b[1].payload_sha256,
    }
    assert first.component_hashes["BENCHMARK"] not in {
        policy_a[2]["BENCHMARK"],
        policy_b[2]["BENCHMARK"],
    }
    envelope = store.load(first.policy_bundle_ref)
    assert envelope.upstream_refs == tuple(
        sorted(
            (policy_a[1], policy_b[1]),
            key=lambda item: (
                item.artifact_kind.value,
                item.semantic_content_hash,
                item.relative_path,
            ),
        )
    )
    members = envelope.payload["members"]
    assert [item["policy_bundle_hash"] for item in members] == sorted(
        (policy_a[1].payload_sha256, policy_b[1].payload_sha256)
    )
    assert {item["policy_bundle_ref"]["semantic_content_hash"] for item in members} == {
        policy_a[1].semantic_content_hash,
        policy_b[1].semantic_content_hash,
    }
    assert envelope.payload["aggregate_component_set_hash"] == (first.component_set_hash)


def test_policy_union_rejects_multiple_component_sets_for_one_policy(tmp_path) -> None:
    store = HistoricalRangeArtifactStore(root=tmp_path / "artifact-store")
    policy = _publish_policy(store, package_id="pkg-a", marker="a")
    request = _request(policy)
    first_label = _label(policy, signal="signal-a")
    conflicting_label = SimpleNamespace(
        **{
            **vars(_label(policy, signal="signal-b")),
            "policy_component_set_hash": "c" * 64,
        }
    )

    with pytest.raises(
        HistoricalRangeDatasetBridgeError,
        match="one exact component set",
    ):
        _adapter(store)._resolve_snapshot_policy_authority(
            request=request,
            labels=(first_label, conflicting_label),
        )
