"""Fail-closed mixed component planner over frozen source/artifact evidence."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Sequence

from .artifact_ready_source import load_artifact_ready_contract
from .canonical import digest_named_fields, ensure_sha256
from .canonical_lineage import (
    CANONICAL_LINEAGE_CAPABILITY,
    CANONICAL_LINEAGE_SCHEMA,
    lineage_bucket,
    lineage_event_key,
    planned_lineage_paths,
)
from .cas_store import CASRef, CASStore
from .component_artifact_manifest import (
    AdjSeriesEvidence,
    ComponentArtifactEvidence,
    ComponentArtifactManifest,
    ComponentArtifactManifestError,
    SourcePartitionEvidence,
    normalize_current_source_partition,
)
from .contracts import Component, ComponentAction
from .decision import ActionPlan, ComponentPlan, FrozenReuseEvidence
from .pit import FrozenPitSnapshot
from .profile import DatasetProfile


_DATE_PARTITION = re.compile(
    r"^(?P<start>\d{4}-\d{2}-\d{2})_(?P<end>\d{4}-\d{2}-\d{2})"
    r"(?P<suffix>_bucket-\d{4})?$"
)
_FACTOR_PARTITION_DATASETS = (
    "daily_pv",
    "daily_basic",
    "moneyflow",
    "bak_basic",
    "cyq_perf",
    "sector_data",
    "margin_detail",
    "static_factors",
)


@dataclass(frozen=True, slots=True)
class CurrentAdjSeriesAuthority:
    qfq_denominator_by_code: Mapping[str, str]
    ordered_adj_digest_by_code: Mapping[str, str]
    adj_row_count_by_code: Mapping[str, int]
    monthly_ordered_adj_by_code: Mapping[str, Mapping[str, Mapping[str, Any]]]
    complete: bool = True

    @classmethod
    def from_summary(cls, value: Mapping[str, Any] | None) -> "CurrentAdjSeriesAuthority | None":
        if not isinstance(value, Mapping) or value.get("qfq_authority_complete") is not True:
            return None
        try:
            denominators = {
                _instrument(code): _decimal_text(raw)
                for code, raw in _mapping(value["qfq_denominator_by_code"]).items()
            }
            ordered = {
                _instrument(code): ensure_sha256(str(raw), field=f"qfq_ordered_adj_digest:{code}")
                for code, raw in _mapping(value["qfq_ordered_adj_digest_by_code"]).items()
            }
            counts = {
                _instrument(code): _positive_int(raw, field=f"qfq row count:{code}")
                for code, raw in _mapping(value["qfq_adj_row_count_by_code"]).items()
            }
        except (KeyError, TypeError, ValueError, ComponentArtifactManifestError):
            return None
        if not denominators or set(denominators) != set(ordered) or set(denominators) != set(counts):
            return None
        monthly_raw = value.get("qfq_monthly_ordered_adj_digest_by_code") or {}
        monthly = _normalize_adj_monthly(monthly_raw) if isinstance(monthly_raw, Mapping) else {}
        return cls(
            qfq_denominator_by_code={code: denominators[code] for code in sorted(denominators)},
            ordered_adj_digest_by_code={code: ordered[code] for code in sorted(ordered)},
            adj_row_count_by_code={code: counts[code] for code in sorted(counts)},
            monthly_ordered_adj_by_code=monthly,
        )


@dataclass(frozen=True, slots=True)
class CurrentComponentAuthority:
    partitions: tuple[Mapping[str, Any], ...]
    adj_series: CurrentAdjSeriesAuthority | None = None


@dataclass(frozen=True, slots=True)
class ArtifactReadyPlanningAuthority:
    contract_ref: CASRef
    effective_content_root: str
    provenance_root: str
    components: Mapping[Component, CurrentComponentAuthority]


def load_artifact_ready_planning_authority(
    cas: CASStore,
    profile: DatasetProfile,
    snapshot: Any,
) -> ArtifactReadyPlanningAuthority:
    reference = getattr(snapshot, "artifact_ready_contract_ref", None)
    raw_root = getattr(snapshot, "artifact_ready_content_root", None)
    raw_provenance = getattr(snapshot, "artifact_ready_provenance_root", None)
    if reference is None or raw_root is None or raw_provenance is None:
        raise ComponentArtifactManifestError("frozen snapshot lacks artifact-ready planning authority")
    loaded = load_artifact_ready_contract(
        cas,
        profile,
        reference,
        expected_source_content_root=str(snapshot.source_content_root),
        expected_pit_snapshot_digest=str(snapshot.pit_snapshot_digest),
    )
    if (
        loaded.artifact_ready_effective_content_root != raw_root
        or loaded.artifact_ready_provenance_root != raw_provenance
    ):
        raise ComponentArtifactManifestError("frozen artifact-ready planning roots differ")
    components: dict[Component, CurrentComponentAuthority] = {}
    for component in Component:
        manifest = cas.get_json_bounded(
            loaded.component_manifest_refs[component.value],
            max_bytes=32 * 1024 * 1024,
        )
        if not isinstance(manifest, Mapping):
            raise ComponentArtifactManifestError("artifact-ready planning component is invalid")
        effective = manifest.get("effective_partitions")
        if not isinstance(effective, list) or not all(isinstance(item, Mapping) for item in effective):
            raise ComponentArtifactManifestError("artifact-ready effective partition projection is invalid")
        # Re-run the common component partition validator here; the artifact-
        # ready loader already proved the manifest Merkle/root graph.
        for item in effective:
            normalize_current_source_partition(item)
        details = manifest.get("details")
        qfq_summary = details.get("qfq_source_summary") if isinstance(details, Mapping) else None
        adj = (
            CurrentAdjSeriesAuthority.from_summary(qfq_summary)
            if component is not Component.DOMESTIC_INDEX_CONTEXT
            else None
        )
        if component is not Component.DOMESTIC_INDEX_CONTEXT and adj is None:
            raise ComponentArtifactManifestError("artifact-ready component lacks complete QFQ authority")
        components[component] = CurrentComponentAuthority(
            partitions=tuple(dict(item) for item in effective),
            adj_series=adj,
        )
    return ArtifactReadyPlanningAuthority(
        contract_ref=loaded.reference,
        effective_content_root=loaded.artifact_ready_effective_content_root,
        provenance_root=loaded.artifact_ready_provenance_root,
        components=components,
    )


def pit_span_digest_by_code(
    snapshot: FrozenPitSnapshot,
) -> dict[str, str]:
    """Return per-code PIT identities stable across a natural cutoff extension.

    Frozen PIT spans are clipped to the release cutoff.  Hashing that clipped
    date directly makes every still-open instrument look historically revised
    on every monthly run.  A span ending exactly at the release cutoff is
    therefore represented by an explicit open-at-cutoff sentinel.  A genuine
    backdated end/start/reason revision remains concrete and changes identity.
    """

    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for span in snapshot.spans:
        value = span.as_dict()
        if span.eligible_end == snapshot.cutoff:
            value = {**value, "eligible_end": "<OPEN_AT_RELEASE_CUTOFF>"}
        grouped.setdefault(span.ts_code, []).append(value)
    return {
        code: digest_named_fields(
            "dataset_release_pit_span_by_code_v1",
            {"ts_code": code, "spans": grouped[code]},
        )
        for code in sorted(grouped)
    }


@dataclass(frozen=True, slots=True)
class MixedPlannerContext:
    source_release_id: str
    source_release_digest: str
    source_attestation_key: str
    dataset_start: date
    cutoff: date
    current_pit_snapshot_digest: str
    current_pit_instruments: tuple[str, ...]
    current_pit_span_digest_by_code: Mapping[str, str]

    def __post_init__(self) -> None:
        ensure_sha256(self.source_release_digest, field="source_release_digest")
        ensure_sha256(self.source_attestation_key, field="source_attestation_key")
        ensure_sha256(self.current_pit_snapshot_digest, field="current_pit_snapshot_digest")
        codes = tuple(sorted({_instrument(value) for value in self.current_pit_instruments}))
        span_codes = {
            _instrument(code): ensure_sha256(str(digest), field=f"current_pit_span_digest:{code}")
            for code, digest in self.current_pit_span_digest_by_code.items()
        }
        if set(codes) != set(span_codes):
            raise ComponentArtifactManifestError("current PIT instruments and per-code span digests differ")


def build_mixed_action_plan(
    *,
    baseline: ComponentArtifactManifest | None,
    current: Mapping[Component, CurrentComponentAuthority],
    context: MixedPlannerContext | None,
    compatible: bool,
) -> ActionPlan:
    plans: list[ComponentPlan] = []
    for component in Component:
        authority = current.get(component)
        component_baseline = baseline.component(component) if baseline is not None else None
        plans.append(
            _plan_component(
                component,
                baseline=component_baseline,
                authority=authority,
                baseline_pit_digest=(baseline.pit_snapshot_digest if baseline is not None else None),
                baseline_cutoff=(baseline.cutoff if baseline is not None else None),
                baseline_metadata=(baseline.candidate_metadata if baseline is not None else {}),
                context=context,
                compatible=compatible,
            )
        )
    return ActionPlan(tuple(plans))


def _plan_component(
    component: Component,
    *,
    baseline: ComponentArtifactEvidence | None,
    authority: CurrentComponentAuthority | None,
    baseline_pit_digest: str | None,
    baseline_cutoff: date | None,
    baseline_metadata: Mapping[str, Mapping[str, Any]],
    context: MixedPlannerContext | None,
    compatible: bool,
) -> ComponentPlan:
    if (
        baseline is None
        or not baseline.complete
        or authority is None
        or context is None
        or not compatible
        or baseline.component_identity is None
        or baseline.component_manifest_root is None
        or baseline.file_identity is None
        or baseline.filesystem_tree_merkle is None
        or baseline.component_root_relative_path is None
    ):
        return _full(component, authority, "complete compatible component artifact evidence is unavailable")
    try:
        current = tuple(normalize_current_source_partition(value) for value in authority.partitions)
    except Exception:
        return _full(component, authority, "current artifact-ready partition authority is incomplete")
    current_by_id = {item.identity: item for item in current}
    if not current_by_id or len(current_by_id) != len(current):
        return _full(component, authority, "current artifact-ready partitions are empty or duplicated")
    previous_by_id = baseline.source_by_identity
    if _schema_drift(previous_by_id, current_by_id):
        return _full(component, authority, "source schema/table-schema drift is not reusable")
    if component is not Component.DOMESTIC_INDEX_CONTEXT:
        if (
            authority.adj_series is None
            or set(authority.adj_series.qfq_denominator_by_code) != set(context.current_pit_instruments)
            or baseline.adj_series is None
            or set(baseline.adj_series.qfq_denominator_by_code) != set(baseline.pit_instruments)
        ):
            return _full(
                component,
                authority,
                "QFQ authority code set differs from the frozen PIT universe",
            )

    exact_common = set(previous_by_id).intersection(current_by_id)
    changed = {
        identity for identity in exact_common if not _same_partition(previous_by_id[identity], current_by_id[identity])
    }
    previous_only = set(previous_by_id).difference(current_by_id)
    current_only = set(current_by_id).difference(previous_by_id)
    tail_pairs = _tail_pairs(
        previous_by_id,
        current_by_id,
        previous_only=previous_only,
        current_only=current_only,
    )
    previous_only.difference_update(tail_pairs)
    current_only.difference_update(tail_pairs.values())
    sparse_historical_additions = {
        identity
        for identity in current_only
        if current_by_id[identity].dataset == "stk_limit_rule_coverage"
    }
    current_only.difference_update(sparse_historical_additions)
    if previous_only:
        return _full(
            component,
            authority,
            "source partitions were removed/rekeyed without a proven monthly prefix",
        )

    replace_targets: set[str] = set()
    create_targets: set[str] = set()
    edges: set[str] = set()
    scopes: list[Mapping[str, Any]] = []
    selective_codes: set[str] = set()
    selective = False
    incremental = False

    for identity in sorted(sparse_historical_additions):
        observed = current_by_id[identity]
        affected = tuple(observed.affected_instruments)
        changed_months = tuple(str(item["month"]) for item in observed.monthly_content_leaves)
        if not affected or not changed_months or set(affected).difference(context.current_pit_instruments):
            return _full(
                component,
                authority,
                "rule-derived limit coverage lacks exact affected instruments/months",
            )
        rules = tuple(
            rule
            for rule in baseline.append_rules
            if observed.dataset in rule.datasets
            or (observed.dataset == "stk_limit_rule_coverage" and "stk_limit" in rule.datasets)
        )
        if not rules or component not in {Component.DAILY_BIN, Component.MINUTE_BIN}:
            return _full(
                component,
                authority,
                "rule-derived limit coverage lacks an exact bin mutation rule",
            )
        for rule in rules:
            bounded_replace, _bounded_create = rule.targets_for_instruments(affected)
            replace_targets.update(
                path
                for path in bounded_replace
                if not path.startswith("qlib/calendars/")
                and not path.startswith("qlib/instruments/")
                and not _immutable_csv_lineage(path)
            )
            edges.update(rule.dependency_edges)
        selective_codes.update(affected)
        scopes.append(
            {
                "kind": "historical_source_revision",
                "source_partition": identity,
                "months": list(changed_months),
                "affected_instruments": list(affected),
            }
        )
        selective = True

    for identity in sorted(changed):
        previous = previous_by_id[identity]
        observed = current_by_id[identity]
        affected_instruments: tuple[str, ...] = ()
        changed_months = _changed_months(previous, observed)
        if changed_months is None:
            return _full(
                component,
                authority,
                "historical revision lacks exact monthly content leaves",
            )
        dependencies = _artifact_dependencies(baseline, identity)
        if not dependencies:
            return _full(
                component,
                authority,
                "historical revision lacks an exact artifact dependency target",
            )
        for partition in dependencies:
            if component not in {
                Component.FACTOR_H5_STATIC,
                Component.DAILY_BIN,
                Component.MINUTE_BIN,
            }:
                replace_targets.update(file.relative_path for file in partition.files)
            edges.update(partition.dependency_edges)
        if component in {Component.DAILY_BIN, Component.MINUTE_BIN}:
            requested = set(baseline.pit_instruments)
            if observed.dataset == "stk_limit_rule_coverage":
                requested = set(previous.affected_instruments).union(observed.affected_instruments)
                if not requested or requested.difference(context.current_pit_instruments):
                    return _full(
                        component,
                        authority,
                        "rule-derived limit revision lacks exact affected instruments",
                    )
            if component is Component.DAILY_BIN and observed.dataset == "index_daily_merged":
                requested = set(baseline.instrument_file_targets).difference(baseline.pit_instruments)
            selective_codes.update(requested)
            affected_instruments = tuple(sorted(requested))
            rules = tuple(
                rule
                for rule in baseline.append_rules
                if observed.dataset in rule.datasets
                or (observed.dataset == "stk_limit_rule_coverage" and "stk_limit" in rule.datasets)
            )
            if not rules:
                return _full(
                    component,
                    authority,
                    "historical bin revision lacks exact per-code mutation rules",
                )
            for rule in rules:
                bounded_replace, _bounded_create = rule.targets_for_instruments(tuple(sorted(requested)))
                replace_targets.update(
                    path
                    for path in bounded_replace
                    if not path.startswith("qlib/calendars/")
                    and not path.startswith("qlib/instruments/")
                    and not _immutable_csv_lineage(path)
                )
        scopes.append(
            {
                "kind": "historical_source_revision",
                "source_partition": identity,
                "months": list(changed_months),
                **({"affected_instruments": list(affected_instruments)} if affected_instruments else {}),
            }
        )
        selective = True

    appended_identities = set(current_only).union(tail_pairs.values())
    for identity in sorted(appended_identities):
        observed = current_by_id[identity]
        rules = tuple(rule for rule in baseline.append_rules if observed.dataset in rule.datasets)
        if not rules:
            return _full(
                component,
                authority,
                "monthly tail lacks an exact component append mutation rule",
            )
        for rule in rules:
            requested_instruments = baseline.pit_instruments
            if component is Component.FACTOR_H5_STATIC:
                # Factor append artifacts are partition files plus monolithic
                # aggregates.  Their rule declares exact whole-file targets;
                # it intentionally has no per-instrument files.
                requested_instruments = ()
            elif component is Component.DAILY_BIN and observed.dataset == "index_daily_merged":
                requested_instruments = tuple(
                    sorted(set(baseline.instrument_file_targets).difference(baseline.pit_instruments))
                )
            rule_replace, rule_create = rule.targets_for_instruments(requested_instruments)
            if component in {Component.DAILY_BIN, Component.MINUTE_BIN}:
                # Ordinary tails preserve immutable canonical CSV bases and
                # add one release-local delta per affected instrument.  Qlib
                # feature files remain exact whole-file mutation targets.
                rule_replace = tuple(path for path in rule_replace if not path.startswith("csv/"))
                delta_key = context.cutoff.strftime("%Y%m")
                for code in requested_instruments:
                    rule_create = (
                        *rule_create,
                        f"csv_deltas/{delta_key}/{code.casefold()}.csv",
                    )
                rule_create = (
                    *rule_create,
                    f"csv_deltas/{delta_key}/manifest.json",
                )
            replace_targets.update(path for path in rule_replace if not _immutable_csv_lineage(path))
            create_targets.update(rule_create)
            edges.update(rule.dependency_edges)
        old_identity = next((old for old, new in tail_pairs.items() if new == identity), None)
        scopes.append(
            {
                "kind": "monthly_tail_extension" if old_identity else "source_partition_append",
                "source_partition": identity,
                "extended_from": old_identity,
                "new_months": list(
                    _new_months(previous_by_id[old_identity], observed)
                    if old_identity
                    else tuple(item["month"] for item in observed.monthly_content_leaves)
                ),
            }
        )
        incremental = True

    if component is Component.FACTOR_H5_STATIC and appended_identities:
        new_months = {
            month
            for scope in scopes
            if scope.get("kind") in {"monthly_tail_extension", "source_partition_append"}
            for month in scope.get("new_months") or ()
        }
        if not new_months:
            return _full(
                component,
                authority,
                "factor tail lacks exact output-month create targets",
            )
        for month in sorted(new_months):
            for dataset in _FACTOR_PARTITION_DATASETS:
                create_targets.add(f"partitions/{dataset}/{month}.parquet")

    pit_snapshot_changed = (
        component is not Component.DOMESTIC_INDEX_CONTEXT and baseline_pit_digest != context.current_pit_snapshot_digest
    )
    old_instruments = set(baseline.pit_instruments)
    current_instruments = set(context.current_pit_instruments)
    new_instruments = tuple(sorted(current_instruments.difference(old_instruments)))
    removed_instruments = tuple(sorted(old_instruments.difference(current_instruments)))
    if pit_snapshot_changed:
        if removed_instruments:
            return _full(
                component,
                authority,
                "PIT instrument removal lacks explicit staging-only delete semantics",
            )
        if baseline.pit_mutation_rule is None:
            return _full(component, authority, "PIT change lacks an exact mutation rule")
        prior_spans = dict(baseline.pit_span_digest_by_code or {})
        current_spans = dict(context.current_pit_span_digest_by_code)
        changed_instruments = tuple(
            sorted(
                code
                for code in old_instruments.intersection(current_instruments)
                if prior_spans.get(code) != current_spans.get(code)
            )
        )
        if not changed_instruments and not new_instruments:
            baseline_cutoff = max(
                (partition.end for partition in baseline.artifact_partitions if partition.end is not None),
                default=None,
            )
            if baseline_cutoff is None or baseline_cutoff >= context.cutoff:
                return _full(
                    component,
                    authority,
                    "PIT root changed without a bounded per-code span difference",
                )
            scopes.append(
                {
                    "kind": "pit_cutoff_extension",
                    "from": baseline_cutoff.isoformat(),
                    "to": context.cutoff.isoformat(),
                    "changed_instruments": [],
                    "new_instruments": [],
                }
            )
            if not changed and not appended_identities:
                return _full(
                    component,
                    authority,
                    "PIT cutoff extension lacks a component tail mutation",
                )
        else:
            try:
                pit_replace, pit_create = baseline.pit_mutation_rule.targets_for_instruments(
                    changed_instruments,
                    create_for_instruments=new_instruments,
                    instrument_file_targets=baseline.instrument_file_targets,
                )
            except ComponentArtifactManifestError:
                return _full(
                    component,
                    authority,
                    "PIT change lacks exact affected-instrument writer targets",
                )
            replace_targets.update(path for path in pit_replace if not _immutable_csv_lineage(path))
            create_targets.update(pit_create)
            if component in {Component.DAILY_BIN, Component.MINUTE_BIN}:
                create_targets.update(f"csv/{code.casefold()}.csv" for code in new_instruments)
            edges.update(baseline.pit_mutation_rule.dependency_edges)
            scopes.append(
                {
                    "kind": "pit_span_change",
                    "new_instruments": list(new_instruments),
                    "removed_instruments": list(removed_instruments),
                    "changed_instruments": list(changed_instruments),
                    "same_instrument_span_revision": bool(changed_instruments),
                }
            )
            if changed_instruments:
                selective = True
            else:
                incremental = True

    adj_changed = any(
        previous_by_id.get(identity, current_by_id.get(identity)).dataset == "adj_factor"
        for identity in set(changed).union(appended_identities)
    )
    if adj_changed and component in {
        Component.DAILY_BIN,
        Component.MINUTE_BIN,
        Component.FACTOR_H5_STATIC,
    }:
        result = _apply_adj_invalidation(
            component,
            baseline=baseline,
            current=authority.adj_series,
            only_tail=not changed and bool(appended_identities),
            dataset_start=context.dataset_start,
            cutoff=context.cutoff,
        )
        if result is None:
            return _full(
                component,
                authority,
                "adj-factor change lacks complete per-code denominator/ordered-series authority",
            )
        adj_replace, adj_edges, adj_scopes, adj_selective = result
        replace_targets.update(path for path in adj_replace if not _immutable_csv_lineage(path))
        edges.update(adj_edges)
        scopes.extend(adj_scopes)
        selective = selective or adj_selective

    if component is Component.FACTOR_H5_STATIC and selective:
        factor_months = _factor_selective_months(
            scopes,
            dataset_start=context.dataset_start,
            cutoff=context.cutoff,
        )
        if not factor_months:
            return _full(
                component,
                authority,
                "factor selective rebuild lacks exact affected output months",
            )
        known_files = set(baseline.all_file_paths)
        for month in factor_months:
            for dataset in _FACTOR_PARTITION_DATASETS:
                relative = f"partitions/{dataset}/{month}.parquet"
                if relative in known_files:
                    replace_targets.add(relative)
                else:
                    create_targets.add(relative)

    if component is Component.DOMESTIC_INDEX_CONTEXT and selective and incremental:
        # Index context is small and its current materializers intentionally
        # implement either a tail append or a same-cutoff selective patch, not
        # both in one writer transaction.  A full rebuild is the safe explicit
        # strategy for the combined case.
        return _full(
            component,
            authority,
            "index tail plus historical revision requires one clean full rebuild",
        )

    if selective and component in {Component.DAILY_BIN, Component.MINUTE_BIN}:
        pit_scope = any(scope.get("kind") == "pit_span_change" for scope in scopes)
        tail_scope = any(scope.get("kind") in {"monthly_tail_extension", "source_partition_append"} for scope in scopes)
        for scope in scopes:
            kind = scope.get("kind")
            # Only scopes that prove a historical value/span change may
            # switch an existing instrument to a full-history override.
            # Ordinary qfq_series_tail scopes exist for every monthly stock,
            # while new PIT instruments have no baseline segment to
            # supersede; including either would recreate a full-market
            # historical export or make the override contract impossible.
            if kind in {
                "qfq_denominator_change",
                "qfq_historical_numerator_revision",
            }:
                instrument = scope.get("instrument")
                if instrument:
                    selective_codes.add(_instrument(instrument))
            if kind == "pit_span_change":
                selective_codes.update(_instrument(value) for value in scope.get("changed_instruments") or ())
        if not selective_codes:
            return _full(
                component,
                authority,
                "selective bin rebuild lacks affected-instrument authority",
            )
        replace_targets = {
            path
            for path in replace_targets
            if not path.startswith("csv/")
            and (not path.startswith("qlib/calendars/") or pit_scope or tail_scope)
            and (not path.startswith("qlib/instruments/") or (pit_scope and not path.endswith("index.txt")))
        }
        # A historical revision and an ordinary monthly tail are two separate
        # bounded jobs.  Keep the whole tail as immutable delta segments and
        # add full-history overrides only for the instruments proven affected
        # by historical/QFQ/PIT authority.  Promoting every tail instrument to
        # ``selective_codes`` turns one denominator correction into a full
        # market, full-history rebuild and defeats the memory bound.
        revision_key = digest_named_fields(
            "dataset_release_csv_selective_override_v1",
            {
                "component": component.value,
                "cutoff": context.cutoff,
                "codes": sorted(selective_codes),
                "scopes": scopes,
            },
        )[:16]
        create_targets.add(f"csv_overrides/{revision_key}/manifest.json")
        create_targets.update(f"csv_overrides/{revision_key}/{code.casefold()}.csv" for code in selective_codes)

    if not changed and not appended_identities and not sparse_historical_additions and not pit_snapshot_changed:
        # Complete equality is component-local; another component may still rebuild.
        evidence = _reuse_evidence(
            baseline,
            context,
            replace=(),
            create=(),
            scopes=(),
            mode="exact_component_reuse",
        )
        return ComponentPlan(
            component=component,
            partition_key="all",
            action=ComponentAction.REUSE,
            reason="component source partitions, PIT dependency and artifact identities match",
            changed_fingerprints=(),
            invalidation_edges=(),
            estimated_work={"source_rows": 0},
            frozen_reuse=evidence,
        )

    if selective:
        action = ComponentAction.SELECTIVE_REBUILD
    elif incremental:
        action = ComponentAction.INCREMENTAL
    else:
        return _full(component, authority, "component mutation classification is incomplete")
    canonical_lineage: Mapping[str, Any] | None = None
    if component in {Component.DAILY_BIN, Component.MINUTE_BIN}:
        try:
            canonical_lineage, lineage_targets = _canonical_lineage_plan(
                component=component,
                baseline=baseline,
                baseline_cutoff=baseline_cutoff,
                baseline_metadata=baseline_metadata,
                context=context,
                action=action,
                replace=tuple(sorted(replace_targets)),
                create=tuple(sorted(create_targets)),
                scopes=tuple(scopes),
            )
        except ComponentArtifactManifestError:
            return _full(
                component,
                authority,
                "canonical lineage targets cannot be frozen exactly",
            )
        create_targets.update(lineage_targets)
    if not replace_targets and not create_targets:
        return _full(component, authority, "component mutation has no exact writer targets")
    if set(replace_targets).intersection(create_targets):
        return _full(component, authority, "component replace/create targets overlap")
    evidence = _reuse_evidence(
        baseline,
        context,
        replace=tuple(sorted(replace_targets)),
        create=tuple(sorted(create_targets)),
        scopes=tuple(scopes),
        mode=action.value.casefold(),
        canonical_lineage=canonical_lineage,
    )
    changed_rows = sum(
        current_by_id[identity].row_count
        for identity in set(changed).union(appended_identities).union(sparse_historical_additions)
    )
    return ComponentPlan(
        component=component,
        partition_key="all",
        action=action,
        reason=(
            "bounded dependency graph and exact private writer targets prove a selective rebuild"
            if action is ComponentAction.SELECTIVE_REBUILD
            else "monthly content-prefix and exact writer targets prove an incremental tail"
        ),
        changed_fingerprints=("source_input_digest",) + (("pit_snapshot_digest",) if pit_snapshot_changed else ()),
        invalidation_edges=tuple(sorted(edges)),
        estimated_work={"source_rows": changed_rows, "writer_targets": len(replace_targets) + len(create_targets)},
        frozen_reuse=evidence,
    )


def _apply_adj_invalidation(
    component: Component,
    *,
    baseline: ComponentArtifactEvidence,
    current: CurrentAdjSeriesAuthority | None,
    only_tail: bool,
    dataset_start: date,
    cutoff: date,
) -> tuple[set[str], set[str], list[Mapping[str, Any]], bool] | None:
    previous: AdjSeriesEvidence | None = baseline.adj_series
    if previous is None or current is None or not current.complete:
        return None
    old_codes = set(previous.qfq_denominator_by_code)
    new_codes = set(current.qfq_denominator_by_code)
    if old_codes.difference(new_codes):
        return None
    targets: set[str] = set()
    edges: set[str] = set()
    scopes: list[Mapping[str, Any]] = []
    selective = False
    for code in sorted(old_codes):
        denominator_changed = previous.qfq_denominator_by_code[code] != current.qfq_denominator_by_code[code]
        ordered_changed = previous.ordered_adj_digest_by_code[code] != current.ordered_adj_digest_by_code[code]
        old_rows = previous.adj_row_count_by_code[code]
        new_rows = current.adj_row_count_by_code[code]
        if new_rows < old_rows:
            return None
        if not denominator_changed and not ordered_changed:
            continue
        writer_targets = tuple(path for path in baseline.adj_writer_targets(code) if not _immutable_csv_lineage(path))
        if not writer_targets:
            return None
        targets.update(writer_targets)
        if denominator_changed and component in {Component.DAILY_BIN, Component.MINUTE_BIN}:
            edges.add(f"adj_factor.denominator->{component.value}")
            scopes.append(
                {
                    "kind": "qfq_denominator_change",
                    "instrument": code,
                    "start": dataset_start.isoformat(),
                    "end": cutoff.isoformat(),
                }
            )
            selective = True
            continue
        if only_tail and not denominator_changed and new_rows > old_rows:
            edges.add(f"adj_factor.series_tail->{component.value}")
            scopes.append(
                {
                    "kind": "qfq_series_tail",
                    "instrument": code,
                    "old_row_count": old_rows,
                    "new_row_count": new_rows,
                }
            )
            continue
        edges.add(
            "adj_factor.numerator->price_rolling_10"
            if component is Component.FACTOR_H5_STATIC
            else f"adj_factor.numerator->{component.value}"
        )
        changed_months = _adj_changed_months(previous, current, code)
        scopes.append(
            {
                "kind": "qfq_historical_numerator_revision",
                "instrument": code,
                "months": list(changed_months),
                "downstream_observations": (10 if component is Component.FACTOR_H5_STATIC else 0),
                "fallback_scope": ("instrument_full_history" if not changed_months else "changed_months"),
            }
        )
        selective = True
    return targets, edges, scopes, selective


def _adj_changed_months(
    previous: AdjSeriesEvidence,
    current: CurrentAdjSeriesAuthority,
    code: str,
) -> tuple[str, ...]:
    old = previous.monthly_ordered_adj_by_code.get(code, {})
    new = current.monthly_ordered_adj_by_code.get(code, {})
    if not old or not new:
        # Missing optional range evidence safely degrades to instrument history;
        # it never guesses a date from row_count/min/max.
        return ()
    return tuple(
        sorted(month for month in set(old).union(new) if dict(old.get(month) or {}) != dict(new.get(month) or {}))
    )


def _factor_selective_months(
    scopes: Sequence[Mapping[str, Any]],
    *,
    dataset_start: date,
    cutoff: date,
) -> tuple[str, ...]:
    months: set[str] = set()
    full_history = False
    downstream_starts: list[str] = []
    for scope in scopes:
        kind = str(scope.get("kind", ""))
        if kind == "historical_source_revision":
            values = tuple(str(value) for value in scope.get("months") or ())
            months.update(values)
            if values:
                # The revised source may feed rolling or as-of state.  Without
                # an exact convergence proof in the frozen source authority,
                # every following month remains affected.
                downstream_starts.append(min(values))
        elif kind == "qfq_historical_numerator_revision":
            values = tuple(str(value) for value in scope.get("months") or ())
            if values:
                months.update(values)
                if int(scope.get("downstream_observations", 0)) > 0:
                    downstream_starts.append(min(values))
            else:
                full_history = True
        elif kind in {"qfq_denominator_change", "pit_span_change"}:
            full_history = True
    if full_history:
        downstream_starts.append(f"{dataset_start.year:04d}-{dataset_start.month:02d}")
    if downstream_starts:
        try:
            first = min(date.fromisoformat(f"{value}-01") for value in downstream_starts)
        except ValueError:
            return ()
        cursor = first
        while cursor <= cutoff:
            months.add(f"{cursor.year:04d}-{cursor.month:02d}")
            cursor = date(cursor.year + 1, 1, 1) if cursor.month == 12 else date(cursor.year, cursor.month + 1, 1)
    return tuple(sorted(months))


def _tail_pairs(
    previous: Mapping[str, SourcePartitionEvidence],
    current: Mapping[str, SourcePartitionEvidence],
    *,
    previous_only: set[str],
    current_only: set[str],
) -> dict[str, str]:
    pairs: dict[str, str] = {}
    claimed: set[str] = set()
    for old_identity in sorted(previous_only):
        old = previous[old_identity]
        candidates = [
            identity
            for identity in current_only
            if identity not in claimed and _is_monthly_tail(old, current[identity])
        ]
        if len(candidates) == 1:
            pairs[old_identity] = candidates[0]
            claimed.add(candidates[0])
    return pairs


def _is_monthly_tail(
    previous: SourcePartitionEvidence,
    current: SourcePartitionEvidence,
) -> bool:
    old_match = _DATE_PARTITION.fullmatch(previous.partition_key)
    new_match = _DATE_PARTITION.fullmatch(current.partition_key)
    if (
        previous.dataset != current.dataset
        or old_match is None
        or new_match is None
        or old_match.group("start") != new_match.group("start")
        or old_match.group("suffix") != new_match.group("suffix")
        or old_match.group("end") >= new_match.group("end")
        or previous.schema_digest != current.schema_digest
        or previous.source_table_schema_digest != current.source_table_schema_digest
        or not previous.monthly_content_leaves
    ):
        return False
    old = tuple((str(item["month"]), str(item["leaf_identity"])) for item in previous.monthly_content_leaves)
    new = tuple((str(item["month"]), str(item["leaf_identity"])) for item in current.monthly_content_leaves)
    return len(new) > len(old) and new[: len(old)] == old


def _new_months(
    previous: SourcePartitionEvidence,
    current: SourcePartitionEvidence,
) -> tuple[str, ...]:
    return tuple(str(item["month"]) for item in current.monthly_content_leaves[len(previous.monthly_content_leaves) :])


def _changed_months(
    previous: SourcePartitionEvidence,
    current: SourcePartitionEvidence,
) -> tuple[str, ...] | None:
    if not previous.monthly_content_leaves or not current.monthly_content_leaves:
        return None
    old = {str(item["month"]): str(item["leaf_identity"]) for item in previous.monthly_content_leaves}
    new = {str(item["month"]): str(item["leaf_identity"]) for item in current.monthly_content_leaves}
    return tuple(sorted(month for month in set(old).union(new) if old.get(month) != new.get(month)))


def _artifact_dependencies(
    baseline: ComponentArtifactEvidence,
    source_identity: str,
) -> tuple[Any, ...]:
    return tuple(item for item in baseline.artifact_partitions if source_identity in item.source_partition_identities)


def _canonical_lineage_plan(
    *,
    component: Component,
    baseline: ComponentArtifactEvidence,
    baseline_cutoff: date | None,
    baseline_metadata: Mapping[str, Mapping[str, Any]],
    context: MixedPlannerContext,
    action: ComponentAction,
    replace: tuple[str, ...],
    create: tuple[str, ...],
    scopes: tuple[Mapping[str, Any], ...],
) -> tuple[Mapping[str, Any], tuple[str, ...]]:
    if component not in {Component.DAILY_BIN, Component.MINUTE_BIN}:
        raise ComponentArtifactManifestError("canonical lineage is supported only for daily/minute bins")
    if baseline.component_manifest_root is None or baseline.file_identity is None or baseline_cutoff is None:
        raise ComponentArtifactManifestError("canonical lineage baseline identity is incomplete")
    planned_codes: set[str] = set()
    for relative in create:
        normalized = str(relative).replace("\\", "/").casefold()
        parts = normalized.split("/")
        if normalized.endswith(".csv") and (
            (len(parts) == 2 and parts[0] == "csv") or (len(parts) == 3 and parts[0] in {"csv_deltas", "csv_overrides"})
        ):
            planned_codes.add(_instrument(parts[-1][:-4].upper()))
    if not planned_codes:
        raise ComponentArtifactManifestError("canonical lineage plan contains no exact CSV instruments")
    mutation_identity = digest_named_fields(
        "dataset_release_canonical_lineage_mutation_v1",
        {
            "component": component.value,
            "cutoff": context.cutoff,
            "action": action.value,
            "baseline_component_manifest_root": baseline.component_manifest_root,
            "baseline_file_identity": baseline.file_identity,
            "replace_existing_targets": list(replace),
            "create_new_targets": list(create),
            "invalidation_scopes": [dict(value) for value in scopes],
            "planned_instruments": sorted(planned_codes),
        },
    )
    metadata_path = f"{component.value}/materialization_receipt.json"
    metadata = baseline_metadata.get(metadata_path)
    is_v3 = bool(isinstance(metadata, Mapping) and metadata.get("schema_version") == CANONICAL_LINEAGE_SCHEMA)
    baseline_lineage_root = (
        ensure_sha256(
            str(metadata.get("manifest_identity", "")),
            field="baseline_lineage_root",
        )
        if is_v3 and metadata is not None
        else None
    )
    baseline_identity = baseline_lineage_root if baseline_lineage_root is not None else baseline.component_manifest_root
    event_key = lineage_event_key(
        dataset=component.value,
        cutoff=context.cutoff.isoformat(),
        action=action.value,
        baseline_identity=baseline_identity,
        mutation_identity=mutation_identity,
    )
    targets = set(
        planned_lineage_paths(
            event_key=event_key,
            instruments=tuple(sorted(planned_codes)),
        )
    )
    anchor_key: str | None = None
    if not is_v3:
        anchor_identity = {
            "source_release_id": context.source_release_id,
            "source_release_digest": context.source_release_digest,
            "component_file_identity": baseline.file_identity,
            "component_manifest_root": baseline.component_manifest_root,
        }
        anchor_key = lineage_event_key(
            dataset=component.value,
            cutoff=baseline_cutoff.isoformat(),
            action="LEGACY_ANCHOR",
            baseline_identity=baseline.component_manifest_root,
            mutation_identity=digest_named_fields(
                "dataset_release_canonical_lineage_anchor_key_v1",
                anchor_identity,
            ),
        )
        targets.update(
            planned_lineage_paths(
                event_key=anchor_key,
                instruments=baseline.pit_instruments,
                anchor=True,
            )
        )
    return (
        {
            "capability": CANONICAL_LINEAGE_CAPABILITY,
            "baseline_schema_version": (CANONICAL_LINEAGE_SCHEMA if is_v3 else "legacy_v1_or_composite_v1"),
            "baseline_lineage_root": baseline_lineage_root,
            "event_key": event_key,
            "mutation_identity": mutation_identity,
            "planned_buckets": tuple(sorted({lineage_bucket(code) for code in planned_codes})),
            "anchor_key": anchor_key,
        },
        tuple(sorted(targets)),
    )


def _immutable_csv_lineage(relative_path: str) -> bool:
    """Return true for append/override history that is never rewritten.

    Segment bytes and their manifests remain lineage evidence even when a
    later full-history override supersedes them.  They may be referenced by a
    component manifest, but may never become a future writer target.
    """

    normalized = str(relative_path).replace("\\", "/").casefold()
    return (
        normalized.startswith("csv_deltas/")
        or normalized.startswith("csv_overrides/")
        or normalized.startswith("csv_lineage/")
    )


def _schema_drift(
    previous: Mapping[str, SourcePartitionEvidence],
    current: Mapping[str, SourcePartitionEvidence],
) -> bool:
    for identity in set(previous).intersection(current):
        if (
            previous[identity].schema_digest != current[identity].schema_digest
            or previous[identity].source_table_schema_digest != current[identity].source_table_schema_digest
        ):
            return True
    return False


def _same_partition(
    previous: SourcePartitionEvidence,
    current: SourcePartitionEvidence,
) -> bool:
    return (
        previous.content_digest == current.content_digest
        and previous.schema_digest == current.schema_digest
        and previous.source_table_schema_digest == current.source_table_schema_digest
        and previous.source_code_membership_digest == current.source_code_membership_digest
        and tuple(previous.monthly_content_leaves) == tuple(current.monthly_content_leaves)
    )


def _reuse_evidence(
    baseline: ComponentArtifactEvidence,
    context: MixedPlannerContext,
    *,
    replace: tuple[str, ...],
    create: tuple[str, ...],
    scopes: tuple[Mapping[str, Any], ...],
    mode: str,
    canonical_lineage: Mapping[str, Any] | None = None,
) -> FrozenReuseEvidence:
    assert baseline.component_identity is not None
    assert baseline.component_manifest_root is not None
    assert baseline.file_identity is not None
    return FrozenReuseEvidence(
        source_release_id=context.source_release_id,
        source_release_digest=context.source_release_digest,
        source_attestation_key=context.source_attestation_key,
        artifact_id=baseline.component_identity,
        component_partition_key="all",
        manifest_root=baseline.filesystem_tree_merkle,
        file_identity=baseline.file_identity,
        reuse_mode=mode,
        mutation_set=tuple(sorted((*replace, *create))),
        compatibility_reason=("versioned semantic/producer/artifact contracts and exact component manifest match"),
        replace_existing_targets=replace,
        create_new_targets=create,
        invalidation_scopes=scopes,
        component_root_relative_path=baseline.component_root_relative_path,
        canonical_lineage=canonical_lineage,
    )


def _full(
    component: Component,
    authority: CurrentComponentAuthority | None,
    reason: str,
) -> ComponentPlan:
    rows = 0
    if authority is not None:
        for item in authority.partitions:
            value = item.get("row_count")
            if type(value) is int and value > 0:
                rows += value
    return ComponentPlan(
        component=component,
        partition_key="all",
        action=ComponentAction.FULL_REBUILD,
        reason=f"{reason}; fail closed for this component only",
        changed_fingerprints=("source_input_digest",),
        invalidation_edges=("artifact_mutation_scope->unproven",),
        estimated_work={"source_rows": rows},
    )


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError("mapping required")
    return value


def _instrument(value: Any) -> str:
    code = str(value).strip().upper()
    if re.fullmatch(r"[0-9]{6}\.(?:SH|SZ)", code) is None:
        raise ComponentArtifactManifestError("QFQ instrument code is invalid")
    return code


def _decimal_text(value: Any) -> str:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ComponentArtifactManifestError("QFQ denominator is invalid") from exc
    if not parsed.is_finite() or parsed <= 0:
        raise ComponentArtifactManifestError("QFQ denominator must be finite/positive")
    text = format(parsed.normalize(), "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def _positive_int(value: Any, *, field: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _normalize_adj_monthly(
    value: Mapping[str, Any],
) -> dict[str, Mapping[str, Mapping[str, Any]]]:
    result: dict[str, Mapping[str, Mapping[str, Any]]] = {}
    for raw_code, raw_months in value.items():
        code = _instrument(raw_code)
        if not isinstance(raw_months, Mapping):
            return {}
        months: dict[str, Mapping[str, Any]] = {}
        for month, raw in raw_months.items():
            if not isinstance(raw, Mapping):
                return {}
            try:
                digest = ensure_sha256(str(raw["ordered_digest"]), field=f"adj month:{code}:{month}")
                rows = _positive_int(raw["row_count"], field="adj month row count")
            except (KeyError, ValueError):
                return {}
            months[str(month)] = {"ordered_digest": digest, "row_count": rows}
        result[code] = {month: months[month] for month in sorted(months)}
    return result


__all__ = [
    "ArtifactReadyPlanningAuthority",
    "CurrentAdjSeriesAuthority",
    "CurrentComponentAuthority",
    "MixedPlannerContext",
    "build_mixed_action_plan",
    "load_artifact_ready_planning_authority",
    "pit_span_digest_by_code",
]
