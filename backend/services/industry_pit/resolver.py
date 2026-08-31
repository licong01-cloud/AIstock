"""Order-invariant, fail-closed C-013 industry authority resolver."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import date
from typing import Any, Iterable, Mapping

from backend.services.dataset_release.canonical import canonical_json_bytes

from .contracts import (
    DUAL_AUTHORITY_RESOLUTION_SCHEMA,
    AlignmentState,
    AuthorityReceipt,
    AuthorityType,
    CandidateInterval,
    DualAuthorityResolution,
    IndustryPitContractError,
    ResearchBasis,
    Resolution,
    ResolutionRequest,
    ResolvedIndustryIdentity,
    UnavailableIndustryIdentity,
    UnavailableReason,
    make_candidate_interval,
)


@dataclass(frozen=True, slots=True)
class _IndexedInterval:
    value: CandidateInterval
    original_row_hashes: tuple[str, ...]
    exact_duplicate_collapsed: bool


def _semantic_duplicate_key(value: CandidateInterval) -> bytes:
    payload = value.as_dict(include_row_hash=False)
    payload.pop("source_ids", None)
    payload.pop("source_hashes", None)
    payload.pop("lineage_hashes", None)
    return canonical_json_bytes(payload)


def _conflict_identity(value: _IndexedInterval) -> Mapping[str, Any]:
    row = value.value
    return {
        "canonical_symbol": row.canonical_symbol,
        "authority_type": row.authority_type.value,
        "valid_from": row.valid_from.isoformat(),
        "valid_to_exclusive": row.valid_to_exclusive.isoformat() if row.valid_to_exclusive else None,
        "known_from": row.known_from.isoformat() if row.known_from else None,
        "identity": row.identity.as_dict() if row.identity else None,
        "identity_hash": row.identity.identity_hash if row.identity else None,
        "authority_identity": dict(row.authority_identity),
        "unavailable_reason": row.unavailable_reason.value if row.unavailable_reason else None,
        "row_hashes": list(value.original_row_hashes),
        "source_ids": list(row.source_ids),
        "source_hashes": list(row.source_hashes),
    }


def _in_half_open(day: date, start: date | None, end: date | None) -> bool:
    return start is not None and day >= start and (end is None or day < end)


class IndustryPitResolver:
    """Resolve one authority without input-order or arbitrary tie-breaks."""

    def __init__(
        self,
        *,
        receipt: AuthorityReceipt,
        intervals: Iterable[CandidateInterval],
        known_taxonomy_versions: Iterable[tuple[str, str]],
    ) -> None:
        self.receipt = receipt
        self._known_taxonomies = frozenset(
            (str(contract).strip(), str(version).strip()) for contract, version in known_taxonomy_versions
        )
        if not self._known_taxonomies:
            raise IndustryPitContractError("resolver requires a non-empty taxonomy allowlist")
        grouped: dict[bytes, list[CandidateInterval]] = defaultdict(list)
        self._global_authority_mismatch = False
        for interval in intervals:
            if (
                interval.authority_type is not receipt.authority_type
                or interval.authority_receipt_hash != receipt.receipt_hash
            ):
                self._global_authority_mismatch = True
            grouped[_semantic_duplicate_key(interval)].append(interval)

        normalized: list[_IndexedInterval] = []
        for key in sorted(grouped):
            group = grouped[key]
            first = group[0]
            source_hashes = sorted({value for item in group for value in item.source_hashes})
            source_ids = sorted({value for item in group for value in item.source_ids})
            lineage_hashes = sorted({value for item in group for value in item.lineage_hashes})
            merged = make_candidate_interval(
                canonical_symbol=first.canonical_symbol,
                authority_type=first.authority_type,
                taxonomy_contract_id=first.taxonomy_contract_id,
                taxonomy_version=first.taxonomy_version,
                authority_receipt_hash=first.authority_receipt_hash,
                valid_from=first.valid_from,
                valid_to_exclusive=first.valid_to_exclusive,
                eligible_from=first.eligible_from,
                eligible_to_exclusive=first.eligible_to_exclusive,
                causal_use_from=first.causal_use_from,
                causal_use_to_exclusive=first.causal_use_to_exclusive,
                known_from=first.known_from,
                source_effective_field=first.source_effective_field,
                source_last_updated_at=first.source_last_updated_at,
                research_basis=first.research_basis,
                non_as_known_taxonomy=first.non_as_known_taxonomy,
                identity=first.identity,
                authority_identity=first.authority_identity,
                unavailable_reason=first.unavailable_reason,
                conflict_candidates=first.conflict_candidates,
                source_ids=source_ids,
                source_hashes=source_hashes,
                lineage_hashes=lineage_hashes,
            )
            normalized.append(
                _IndexedInterval(
                    value=merged,
                    original_row_hashes=tuple(sorted(item.row_hash for item in group)),
                    exact_duplicate_collapsed=len(group) > 1,
                )
            )

        by_symbol: dict[str, list[_IndexedInterval]] = defaultdict(list)
        for item in normalized:
            by_symbol[item.value.canonical_symbol].append(item)
        self._by_symbol = {
            symbol: tuple(
                sorted(
                    values,
                    key=lambda item: (
                        item.value.valid_from,
                        item.value.valid_to_exclusive or date.max,
                        item.value.row_hash,
                    ),
                )
            )
            for symbol, values in by_symbol.items()
        }

    def transition_dates(self, canonical_symbol: str) -> tuple[date, ...]:
        """Return every date at which this symbol's resolution may change."""

        dates: set[date] = set()
        for item in self._by_symbol.get(canonical_symbol, ()):
            row = item.value
            for value in (
                row.valid_from,
                row.valid_to_exclusive,
                row.eligible_from,
                row.eligible_to_exclusive,
                row.causal_use_from,
                row.causal_use_to_exclusive,
                row.known_from,
            ):
                if value is not None:
                    dates.add(value)
        return tuple(sorted(dates))

    def candidate_summary(self, canonical_symbol: str) -> tuple[Mapping[str, Any], ...]:
        """Return a compact, canonically ordered audit projection for one symbol."""

        output = []
        for item in self._by_symbol.get(canonical_symbol, ()):
            row = item.value
            output.append(
                {
                    "valid_from": row.valid_from.isoformat(),
                    "valid_to_exclusive": row.valid_to_exclusive.isoformat() if row.valid_to_exclusive else None,
                    "causal_use_from": row.causal_use_from.isoformat() if row.causal_use_from else None,
                    "causal_use_to_exclusive": (
                        row.causal_use_to_exclusive.isoformat() if row.causal_use_to_exclusive else None
                    ),
                    "known_from": row.known_from.isoformat() if row.known_from else None,
                    "identity": row.identity.as_dict() if row.identity else None,
                    "identity_hash": row.identity.identity_hash if row.identity else None,
                    "authority_identity": dict(row.authority_identity),
                    "unavailable_reason": row.unavailable_reason.value if row.unavailable_reason else None,
                    "row_hashes": list(item.original_row_hashes),
                    "source_ids": list(row.source_ids),
                    "source_hashes": list(row.source_hashes),
                    "exact_duplicate_collapsed": item.exact_duplicate_collapsed,
                }
            )
        return tuple(sorted(output, key=canonical_json_bytes))

    def resolve(self, request: ResolutionRequest) -> Resolution:
        if request.authority_type is not self.receipt.authority_type:
            return self._unavailable(request, UnavailableReason.AUTHORITY_SOURCE_MISMATCH)
        if self._global_authority_mismatch or request.authority_receipt_hash != self.receipt.receipt_hash:
            return self._unavailable(request, UnavailableReason.AUTHORITY_SOURCE_MISMATCH)
        if request.knowledge_time_policy is not self.receipt.knowledge_time_policy:
            return self._unavailable(request, UnavailableReason.AUTHORITY_SOURCE_MISMATCH)
        if request.research_basis is not self.receipt.research_basis:
            return self._unavailable(request, UnavailableReason.AUTHORITY_SOURCE_MISMATCH)
        taxonomy_key = (request.taxonomy_contract_id, request.taxonomy_version)
        if taxonomy_key not in self._known_taxonomies:
            return self._unavailable(request, UnavailableReason.TAXONOMY_VERSION_UNAVAILABLE)
        if (
            request.taxonomy_contract_id != self.receipt.taxonomy_contract_id
            or request.taxonomy_version != self.receipt.taxonomy_version
        ):
            return self._unavailable(request, UnavailableReason.TAXONOMY_VERSION_UNAVAILABLE)

        values = self._by_symbol.get(request.canonical_symbol, ())
        covering: list[_IndexedInterval] = []
        for item in values:
            row = item.value
            if not _in_half_open(request.trade_date, row.eligible_from, row.eligible_to_exclusive):
                continue
            if request.research_basis is ResearchBasis.AS_PUBLISHED_PIT:
                if _in_half_open(request.trade_date, row.causal_use_from, row.causal_use_to_exclusive):
                    covering.append(item)
            elif _in_half_open(request.trade_date, row.valid_from, row.valid_to_exclusive):
                covering.append(item)

        if not covering:
            reason = (
                UnavailableReason.CLASSIFICATION_AUTHORITY_UNAVAILABLE
                if request.authority_type is AuthorityType.CLASSIFICATION
                else UnavailableReason.MEMBERSHIP_BOUNDARY_UNAVAILABLE
            )
            return self._unavailable(request, reason)
        if len(covering) > 1:
            conflicts = tuple(_conflict_identity(item) for item in covering)
            starts = {item.value.valid_from for item in covering}
            identities = {
                item.value.identity.identity_hash if item.value.identity else item.value.unavailable_reason.value
                for item in covering
            }
            reason = (
                UnavailableReason.SAME_BOUNDARY_IDENTITY_CONFLICT
                if len(starts) == 1 and len(identities) > 1
                else UnavailableReason.INTERVAL_OVERLAP
            )
            return self._unavailable(request, reason, conflicts=conflicts)

        item = covering[0]
        row = item.value
        if row.unavailable_reason is not None:
            return self._unavailable(request, row.unavailable_reason, conflicts=row.conflict_candidates)
        assert row.identity is not None
        if request.research_basis is ResearchBasis.AS_PUBLISHED_PIT and (
            row.known_from is None or row.known_from > request.trade_date
        ):
            reason = (
                UnavailableReason.CLASSIFICATION_KNOWLEDGE_TIME_UNVERIFIED
                if request.authority_type is AuthorityType.CLASSIFICATION
                else UnavailableReason.MEMBERSHIP_BOUNDARY_UNAVAILABLE
            )
            return self._unavailable(request, reason, conflicts=(_conflict_identity(item),))
        position = values.index(item)
        sequential = (
            position > 0
            and values[position - 1].value.valid_to_exclusive == row.valid_from
        ) or (
            position + 1 < len(values)
            and row.valid_to_exclusive == values[position + 1].value.valid_from
        )
        return ResolvedIndustryIdentity(
            status="resolved",
            canonical_symbol=request.canonical_symbol,
            trade_date=request.trade_date,
            authority_type=request.authority_type,
            identity=row.identity,
            authority_identity=row.authority_identity,
            valid_from=row.valid_from,
            valid_to_exclusive=row.valid_to_exclusive,
            known_from=row.known_from,
            taxonomy_contract_id=row.taxonomy_contract_id,
            taxonomy_version=row.taxonomy_version,
            source_ids=row.source_ids,
            source_hashes=row.source_hashes,
            row_hashes=item.original_row_hashes,
            authority_receipt_hash=row.authority_receipt_hash,
            non_as_known_taxonomy=row.non_as_known_taxonomy,
            alignment_state=AlignmentState.UNAVAILABLE,
            exact_duplicate_collapsed=item.exact_duplicate_collapsed,
            sequential_interval_resolved=sequential,
        )

    @staticmethod
    def _unavailable(
        request: ResolutionRequest,
        reason: UnavailableReason,
        *,
        conflicts: Iterable[Mapping[str, Any]] = (),
    ) -> UnavailableIndustryIdentity:
        canonical = sorted((dict(value) for value in conflicts), key=canonical_json_bytes)
        return UnavailableIndustryIdentity(
            status="unavailable",
            canonical_symbol=request.canonical_symbol,
            trade_date=request.trade_date,
            authority_type=request.authority_type,
            reason=reason,
            conflict_candidates=tuple(canonical),
            authority_receipt_hash=request.authority_receipt_hash,
        )


def resolve_dual_authority(
    *,
    classification_resolver: IndustryPitResolver,
    index_membership_resolver: IndustryPitResolver,
    classification_request: ResolutionRequest,
    index_membership_request: ResolutionRequest,
) -> DualAuthorityResolution:
    if classification_request.canonical_symbol != index_membership_request.canonical_symbol:
        raise IndustryPitContractError("dual authority requests must use one canonical symbol")
    if classification_request.trade_date != index_membership_request.trade_date:
        raise IndustryPitContractError("dual authority requests must use one trade date")
    classification = classification_resolver.resolve(classification_request)
    index_membership = index_membership_resolver.resolve(index_membership_request)
    if isinstance(classification, ResolvedIndustryIdentity) and isinstance(
        index_membership, ResolvedIndustryIdentity
    ):
        aligned = (
            classification.taxonomy_version == index_membership.taxonomy_version
            and classification.identity.identity_hash == index_membership.identity.identity_hash
        )
        alignment = AlignmentState.ALIGNED if aligned else AlignmentState.UNALIGNED
    else:
        alignment = AlignmentState.UNAVAILABLE
    classification = replace(classification, alignment_state=alignment)
    index_membership = replace(index_membership, alignment_state=alignment)
    return DualAuthorityResolution(
        schema_version=DUAL_AUTHORITY_RESOLUTION_SCHEMA,
        canonical_symbol=classification_request.canonical_symbol,
        trade_date=classification_request.trade_date,
        classification=classification,
        index_membership=index_membership,
        alignment_state=alignment,
    )


__all__ = ["IndustryPitResolver", "resolve_dual_authority"]
