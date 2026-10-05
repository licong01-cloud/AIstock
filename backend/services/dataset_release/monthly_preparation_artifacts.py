"""Normalize independently eligible source inputs, never a full ready graph.

The provider and row rules are the existing ArtifactReadySourceBuilder rules.
Only the component selection and the private, non-publication envelope differ.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping

from .artifact_ready_source import (
    ArtifactReadySourceBuilder,
    _SealedSnapshotView,
    _dedupe_refs,
)
from .canonical import digest_named_fields
from .cas_store import CASRef
from .contracts import Component
from .monthly_component_preparation import ComponentPreparationError, component_dependencies
from .monthly_preparation_source import PreparationSourceSnapshot
from .monthly_unified import SOURCE_GATES


PREPARATION_ARTIFACT_SCHEMA = "aistock_monthly_preparation_artifacts_v1"
_SUPPORTED = frozenset({Component.DAILY_BIN, Component.MINUTE_BIN, Component.DOMESTIC_INDEX_CONTEXT})
_LOGICAL = {
    Component.DAILY_BIN: "day",
    Component.MINUTE_BIN: "minute",
    Component.DOMESTIC_INDEX_CONTEXT: "index",
}
_GATES = {
    "day": frozenset({"calendar_lifecycle", "daily_price", "adj_factor_history", "suspend_limit", "pit_stock_pools"}),
    "minute": frozenset(
        {"calendar_lifecycle", "daily_price", "minute_price", "adj_factor_history", "suspend_limit", "pit_stock_pools"}
    ),
    "index": frozenset({"calendar_lifecycle", "pit_stock_pools"}),
}


@dataclass(frozen=True, slots=True)
class PreparationArtifactBundle:
    reference: CASRef
    component_manifests: Mapping[Component, CASRef]
    qfq_authority_ref: CASRef | None
    provider_receipt_refs: tuple[CASRef, ...]
    derived_source_receipt_refs: tuple[CASRef, ...]


def require_preparation_domain_audit(
    *, profile: Any, snapshot: PreparationSourceSnapshot, audit: Mapping[str, Any], required_gates: frozenset[str]
) -> None:
    """Bind scoped checks to the actual private snapshot, never to a PASS flag."""
    if (
        not isinstance(snapshot, PreparationSourceSnapshot)
        or audit.get("schema_version") != "aistock_monthly_preparation_source_audit_v1"
        or audit.get("operation_id") != snapshot.operation_id
        or audit.get("cutoff") != snapshot.official_cutoff.isoformat()
        or audit.get("audit_start") not in {
            profile.start_date.isoformat(), snapshot.official_cutoff.replace(day=1).isoformat(),
        }
        or audit.get("source_manifest_ref") != snapshot.source_manifest_ref.as_dict()
        or snapshot.official_cutoff != snapshot.pit_snapshot.cutoff
        or not required_gates
        or not required_gates <= set(SOURCE_GATES)
    ):
        raise ComponentPreparationError("private domain audit identity differs")
    gates = audit.get("gates")
    if not isinstance(gates, list) or any(not isinstance(gate, Mapping) for gate in gates):
        raise ComponentPreparationError("private domain audit is missing")
    by_gate = {gate.get("gate_id"): gate for gate in gates}
    if len(by_gate) != len(gates) or set(by_gate) != set(SOURCE_GATES):
        raise ComponentPreparationError("private domain audit gate set differs")
    for gate in gates:
        fields = (
            "expected_count",
            "observed_count",
            "explained_missing_count",
            "unexplained_missing_count",
            "duplicate_count",
            "invalid_value_count",
        )
        if (
            any(type(gate.get(key)) is not int or gate[key] < 0 for key in fields)
            or gate["expected_count"]
            != gate["observed_count"] + gate["explained_missing_count"] + gate["unexplained_missing_count"]
            or not str(gate.get("snapshot_group_id") or "").startswith("postgres:")
            or gate.get("snapshot_group_id") != gates[0].get("snapshot_group_id")
            or gate["explained_missing_count"]
            and not gate.get("exception_refs")
        ):
            raise ComponentPreparationError("private domain audit counts or snapshot differ")
    for name in required_gates:
        if any(
            by_gate[name][field] != 0
            for field in ("unexplained_missing_count", "duplicate_count", "invalid_value_count")
        ):
            raise ComponentPreparationError(f"private domain remains blocked: {name}")


def normalize_preparation_artifacts(
    builder: ArtifactReadySourceBuilder,
    *,
    snapshot: PreparationSourceSnapshot,
    audit: Mapping[str, Any],
    components: tuple[Component, ...],
    checkpoint: Callable[[], None] = lambda: None,
) -> PreparationArtifactBundle:
    """Use formal producer rules only after each selected domain is healthy."""
    if (
        not isinstance(snapshot, PreparationSourceSnapshot)
        or not components
        or len(components) != len(set(components))
        or not set(components) <= _SUPPORTED
        or audit.get("operation_id") != snapshot.operation_id
        or audit.get("cutoff") != snapshot.official_cutoff.isoformat()
        or audit.get("audit_start") not in {
            builder.profile.start_date.isoformat(), snapshot.official_cutoff.replace(day=1).isoformat(),
        }
        or audit.get("schema_version") != "aistock_monthly_preparation_source_audit_v1"
        or audit.get("source_manifest_ref") != snapshot.source_manifest_ref.as_dict()
        or snapshot.official_cutoff != snapshot.pit_snapshot.cutoff
    ):
        raise ComponentPreparationError("private normalization identity or selection differs")
    gates = audit.get("gates")
    if not isinstance(gates, list) or any(not isinstance(gate, Mapping) for gate in gates):
        raise ComponentPreparationError("private normalization audit is missing")
    by_gate = {gate.get("gate_id"): gate for gate in gates}
    if len(by_gate) != len(gates) or set(by_gate) != set(SOURCE_GATES):
        raise ComponentPreparationError("private normalization audit gate set differs")
    for gate in gates:
        fields = (
            "expected_count",
            "observed_count",
            "explained_missing_count",
            "unexplained_missing_count",
            "duplicate_count",
            "invalid_value_count",
        )
        if (
            any(type(gate.get(key)) is not int or gate[key] < 0 for key in fields)
            or gate["expected_count"]
            != gate["observed_count"] + gate["explained_missing_count"] + gate["unexplained_missing_count"]
            or not str(gate.get("snapshot_group_id") or "").startswith("postgres:")
            or gate.get("snapshot_group_id") != gates[0].get("snapshot_group_id")
            or gate["explained_missing_count"]
            and not gate.get("exception_refs")
        ):
            raise ComponentPreparationError("private normalization audit counts or identity differ")
    dependencies = component_dependencies()
    available = {part.spec.dataset for part in snapshot.partitions} | {"stock_universe_pit"}
    for component in components:
        logical = _LOGICAL[component]
        if set(dependencies[logical]) - available:
            raise ComponentPreparationError(f"private normalization source is incomplete: {logical}")
        for gate_id in _GATES[logical]:
            gate = by_gate.get(gate_id)
            if not gate or any(
                gate.get(key) != 0
                for key in (
                    "unexplained_missing_count",
                    "duplicate_count",
                    "invalid_value_count",
                )
            ):
                raise ComponentPreparationError(f"private normalization domain remains blocked: {logical}:{gate_id}")

    view = _SealedSnapshotView(builder.cas, snapshot)
    days = builder._trading_dates(view, snapshot.official_cutoff)
    selected = set(components)
    price = bool(selected & {Component.DAILY_BIN, Component.MINUTE_BIN})
    providers: list[CASRef] = []
    proofs: list[CASRef] = []
    daily = adj = limit = minute = index = ()
    daily_summary: Mapping[str, Any] = {}
    qfq_summary: Mapping[str, Any] = {}
    limit_summary: Mapping[str, Any] = {}
    minute_summary: Mapping[str, Any] = {}
    qfq_ref: CASRef | None = None
    if price:
        suspended = builder._full_day_suspensions(view)
        daily, refs, derived, daily_summary, overlay_keys = builder._daily_entries(
            view,
            snapshot=snapshot,
            trading_dates=days,
            suspended=suspended,
            checkpoint=checkpoint,
        )
        providers.extend(refs)
        proofs.extend(derived)
        adj, refs, derived, qfq_summary, qfq_ref = builder._adj_factor_entries(
            view,
            snapshot=snapshot,
            checkpoint=checkpoint,
            daily_overlay_keys=overlay_keys,
        )
        providers.extend(refs)
        proofs.extend((*derived, qfq_ref))
        limit, refs, derived, limit_summary = builder._limit_entries(
            view,
            snapshot=snapshot,
            trading_dates=days,
            daily_entries=daily,
            adj_entries=adj,
            checkpoint=checkpoint,
        )
        providers.extend(refs)
        proofs.extend(derived)
        if Component.MINUTE_BIN in selected:
            minute, refs, derived, minute_summary = builder._minute_entries(
                view,
                snapshot=snapshot,
                trading_dates=days,
                suspended=suspended,
                checkpoint=checkpoint,
            )
            providers.extend(refs)
            proofs.extend(derived)
    if selected & {Component.DAILY_BIN, Component.DOMESTIC_INDEX_CONTEXT}:
        index, refs, derived = builder._index_entries(
            view, cutoff=snapshot.official_cutoff, trading_dates=days, checkpoint=checkpoint
        )
        providers.extend(refs)
        proofs.extend(derived)
    manifests = {}
    for component in sorted(components, key=lambda item: item.value):
        raw = builder._raw_entries(view, component)
        derived = index
        details: Mapping[str, Any] = {}
        if component in {Component.DAILY_BIN, Component.MINUTE_BIN}:
            assert qfq_ref is not None
            derived = (*daily, *adj, *limit, *(index if component is Component.DAILY_BIN else minute))
            details = {
                "qfq_source_summary": dict(qfq_summary),
                "qfq_denominator_authority_ref": qfq_ref.as_dict(),
                "daily_provider_summary": dict(daily_summary),
                "stk_limit_rule_overlay_summary": dict(limit_summary),
                **({"minute_coverage": dict(minute_summary)} if component is Component.MINUTE_BIN else {}),
            }
        manifests[component] = builder._seal_component_manifest(
            component,
            source_content_root=snapshot.source_content_root,
            partitions=(*raw, *derived),
            details=details,
        )
    provider_refs, derived_refs = _dedupe_refs(providers), _dedupe_refs(proofs)
    body = {
        "schema_version": PREPARATION_ARTIFACT_SCHEMA,
        "operation_id": snapshot.operation_id,
        "profile": builder.profile.profile,
        "cutoff": snapshot.official_cutoff.isoformat(),
        "preparation_source_manifest_ref": snapshot.source_manifest_ref.as_dict(),
        "source_content_root": snapshot.source_content_root,
        "pit_snapshot_ref": snapshot.pit_snapshot_ref.as_dict(),
        "pit_snapshot_digest": snapshot.pit_snapshot_digest,
        "component_manifests": {component.value: ref.as_dict() for component, ref in manifests.items()},
        "qfq_denominator_authority_ref": qfq_ref.as_dict() if qfq_ref else None,
        "qfq_source_summary": dict(qfq_summary),
        "provider_receipt_refs": [ref.as_dict() for ref in provider_refs],
        "derived_source_receipt_refs": [ref.as_dict() for ref in derived_refs],
        "consistent_input_set_complete": False,
        "publication_allowed": False,
        "database_write_performed": False,
    }
    ref = builder.cas.put_json({**body, "canonical_digest": digest_named_fields(PREPARATION_ARTIFACT_SCHEMA, body)})
    builder.cas.verify(ref)
    checkpoint()
    return PreparationArtifactBundle(ref, manifests, qfq_ref, provider_refs, derived_refs)
