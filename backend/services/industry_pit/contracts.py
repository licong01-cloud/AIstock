"""Shared C-013 industry PIT schemas and canonical identities.

This module is deliberately consumer-neutral.  It contains no HMM, QE,
Selection, Paper, or Advisory policy and performs no database access.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, replace
from datetime import date, datetime
from enum import Enum
from typing import Any, Mapping, Sequence

from backend.services.dataset_release.canonical import (
    CanonicalizationError,
    canonical_json_bytes,
    digest_named_fields,
    ensure_sha256,
    sha256_hex,
)


CLASSIFICATION_CANDIDATE_SCHEMA = "stock_industry_classification_pit_candidate_v1"
INDEX_MEMBERSHIP_CANDIDATE_SCHEMA = "sw_industry_index_membership_pit_candidate_v1"
AUTHORITY_RECEIPT_SCHEMA = "industry_pit_authority_receipt_v1"
DUAL_AUTHORITY_RESOLUTION_SCHEMA = "industry_pit_dual_authority_resolution_v1"
CANDIDATE_BUNDLE_SCHEMA = "industry_pit_candidate_bundle_v1"
PREFLIGHT_REPORT_SCHEMA = "industry_pit_full_denominator_preflight_v1"
TAXONOMY_CATALOG_SCHEMA = "sw2021_taxonomy_catalog_v1"

_SYMBOL_RE = re.compile(r"^[0-9]{6}\.(?:SH|SZ)$")
_CODE_RE = re.compile(r"^[0-9]{6}$")


class IndustryPitContractError(ValueError):
    """Raised when a C-013 authority contract is malformed or inconsistent."""

    code = "INDUSTRY_PIT_CONTRACT_INVALID"


class AuthorityType(str, Enum):
    CLASSIFICATION = "stock_industry_classification_pit"
    INDEX_MEMBERSHIP = "sw_industry_index_membership_pit"


class ResearchBasis(str, Enum):
    AS_PUBLISHED_PIT = "as_published_pit"
    STABLE_TAXONOMY_BACKCAST = "stable_taxonomy_backcast"
    REVISED_HISTORY = "revised_history"


class KnowledgeTimePolicy(str, Enum):
    CAUSAL_DAILY_NEXT_TRADE = "causal_daily_next_trade_v1"
    NON_AS_KNOWN_RESEARCH = "non_as_known_research_v1"


class AlignmentState(str, Enum):
    ALIGNED = "aligned"
    UNALIGNED = "unaligned"
    UNAVAILABLE = "unavailable"


class UnavailableReason(str, Enum):
    CLASSIFICATION_KNOWLEDGE_TIME_UNVERIFIED = "classification_knowledge_time_unverified"
    CLASSIFICATION_AUTHORITY_UNAVAILABLE = "classification_authority_unavailable"
    MEMBERSHIP_BOUNDARY_UNAVAILABLE = "membership_boundary_unavailable"
    SAME_BOUNDARY_IDENTITY_CONFLICT = "same_boundary_identity_conflict"
    INDUSTRY_IDENTITY_AMBIGUOUS = "industry_identity_ambiguous"
    TAXONOMY_VERSION_UNAVAILABLE = "taxonomy_version_unavailable"
    INTERVAL_OVERLAP = "interval_overlap"
    AUTHORITY_SOURCE_MISMATCH = "authority_source_mismatch"
    CATALOG_IDENTITY_INVALID = "catalog_identity_invalid"
    INVALID_DATE = "invalid_date"
    WRITER_READBACK_HASH_MISMATCH = "writer_readback_hash_mismatch"


def require_symbol(value: str) -> str:
    symbol = str(value or "").strip().upper()
    if not _SYMBOL_RE.fullmatch(symbol):
        raise IndustryPitContractError(f"canonical_symbol is invalid: {value!r}")
    return symbol


def require_date(value: date, *, field: str) -> date:
    if not isinstance(value, date) or isinstance(value, datetime):
        raise IndustryPitContractError(f"{field} must be a date")
    return value


def require_nonempty(value: str, *, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise IndustryPitContractError(f"{field} must be non-empty")
    return text


@dataclass(frozen=True, slots=True)
class TaxonomyIdentity:
    l1_code: str
    l1_name: str
    l2_code: str
    l2_name: str
    l3_code: str
    l3_name: str

    def __post_init__(self) -> None:
        for field in ("l1_code", "l2_code", "l3_code"):
            value = str(getattr(self, field) or "").strip()
            if not _CODE_RE.fullmatch(value):
                raise IndustryPitContractError(f"{field} must be a six-digit taxonomy code")
            object.__setattr__(self, field, value)
        for field in ("l1_name", "l2_name", "l3_name"):
            object.__setattr__(self, field, require_nonempty(getattr(self, field), field=field))
        if self.l2_code[:2] != self.l1_code[:2] or self.l3_code[:4] != self.l2_code[:4]:
            raise IndustryPitContractError("taxonomy hierarchy is inconsistent")

    @property
    def leaf_code(self) -> str:
        return self.l3_code

    def as_dict(self) -> dict[str, str]:
        return {
            "l1_code": self.l1_code,
            "l1_name": self.l1_name,
            "l2_code": self.l2_code,
            "l2_name": self.l2_name,
            "l3_code": self.l3_code,
            "l3_name": self.l3_name,
        }

    @property
    def identity_hash(self) -> str:
        return digest_named_fields("industry_taxonomy_identity_v1", self.as_dict())


@dataclass(frozen=True, slots=True)
class AuthorityReceipt:
    authority_type: AuthorityType
    authority_schema: str
    authority_version: str
    taxonomy_contract_id: str
    taxonomy_version: str
    knowledge_time_policy: KnowledgeTimePolicy
    research_basis: ResearchBasis
    source_ids: tuple[str, ...]
    source_hashes: tuple[str, ...]
    frozen_denominator: int
    denominator_digest: str

    def __post_init__(self) -> None:
        expected_schema = (
            CLASSIFICATION_CANDIDATE_SCHEMA
            if self.authority_type is AuthorityType.CLASSIFICATION
            else INDEX_MEMBERSHIP_CANDIDATE_SCHEMA
        )
        if self.authority_schema != expected_schema:
            raise IndustryPitContractError("authority_type and authority_schema differ")
        for field in ("authority_version", "taxonomy_contract_id", "taxonomy_version"):
            object.__setattr__(self, field, require_nonempty(getattr(self, field), field=field))
        normalized_hashes = tuple(sorted({ensure_sha256(value, field="source_hash") for value in self.source_hashes}))
        normalized_ids = tuple(sorted({require_nonempty(value, field="source_id") for value in self.source_ids}))
        if not normalized_hashes or not normalized_ids:
            raise IndustryPitContractError("authority receipt requires source identities and hashes")
        object.__setattr__(self, "source_ids", normalized_ids)
        object.__setattr__(self, "source_hashes", normalized_hashes)
        if type(self.frozen_denominator) is not int or self.frozen_denominator < 0:
            raise IndustryPitContractError("frozen_denominator must be a non-negative integer")
        object.__setattr__(
            self,
            "denominator_digest",
            ensure_sha256(self.denominator_digest, field="denominator_digest"),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": AUTHORITY_RECEIPT_SCHEMA,
            "authority_type": self.authority_type.value,
            "authority_schema": self.authority_schema,
            "authority_version": self.authority_version,
            "taxonomy_contract_id": self.taxonomy_contract_id,
            "taxonomy_version": self.taxonomy_version,
            "knowledge_time_policy": self.knowledge_time_policy.value,
            "research_basis": self.research_basis.value,
            "source_ids": list(self.source_ids),
            "source_hashes": list(self.source_hashes),
            "frozen_denominator": self.frozen_denominator,
            "denominator_digest": self.denominator_digest,
        }

    @property
    def receipt_hash(self) -> str:
        return digest_named_fields(AUTHORITY_RECEIPT_SCHEMA, self.as_dict())


def authority_receipt_from_mapping(value: Mapping[str, Any]) -> AuthorityReceipt:
    expected = {
        "schema_version",
        "authority_type",
        "authority_schema",
        "authority_version",
        "taxonomy_contract_id",
        "taxonomy_version",
        "knowledge_time_policy",
        "research_basis",
        "source_ids",
        "source_hashes",
        "frozen_denominator",
        "denominator_digest",
        "receipt_hash",
    }
    if not isinstance(value, Mapping) or set(value) != expected:
        raise IndustryPitContractError("authority receipt payload keys differ from schema")
    if value.get("schema_version") != AUTHORITY_RECEIPT_SCHEMA:
        raise IndustryPitContractError("authority receipt schema is invalid")
    try:
        receipt = AuthorityReceipt(
            authority_type=AuthorityType(str(value["authority_type"])),
            authority_schema=str(value["authority_schema"]),
            authority_version=str(value["authority_version"]),
            taxonomy_contract_id=str(value["taxonomy_contract_id"]),
            taxonomy_version=str(value["taxonomy_version"]),
            knowledge_time_policy=KnowledgeTimePolicy(str(value["knowledge_time_policy"])),
            research_basis=ResearchBasis(str(value["research_basis"])),
            source_ids=tuple(value["source_ids"]),
            source_hashes=tuple(value["source_hashes"]),
            frozen_denominator=int(value["frozen_denominator"]),
            denominator_digest=str(value["denominator_digest"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise IndustryPitContractError(f"authority receipt payload is invalid: {exc}") from exc
    if receipt.receipt_hash != value.get("receipt_hash"):
        raise IndustryPitContractError("authority receipt hash mismatch")
    return receipt


@dataclass(frozen=True, slots=True)
class CandidateInterval:
    schema_version: str
    canonical_symbol: str
    authority_type: AuthorityType
    taxonomy_contract_id: str
    taxonomy_version: str
    authority_receipt_hash: str
    valid_from: date
    valid_to_exclusive: date | None
    eligible_from: date
    eligible_to_exclusive: date | None
    causal_use_from: date | None
    causal_use_to_exclusive: date | None
    known_from: date | None
    source_effective_field: str
    source_last_updated_at: str | None
    research_basis: ResearchBasis
    non_as_known_taxonomy: bool
    identity: TaxonomyIdentity | None
    authority_identity: Mapping[str, str]
    unavailable_reason: UnavailableReason | None
    conflict_candidates: tuple[Mapping[str, Any], ...]
    source_ids: tuple[str, ...]
    source_hashes: tuple[str, ...]
    lineage_hashes: tuple[str, ...]
    row_hash: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "canonical_symbol", require_symbol(self.canonical_symbol))
        expected_schema = (
            CLASSIFICATION_CANDIDATE_SCHEMA
            if self.authority_type is AuthorityType.CLASSIFICATION
            else INDEX_MEMBERSHIP_CANDIDATE_SCHEMA
        )
        if self.schema_version != expected_schema:
            raise IndustryPitContractError("candidate schema and authority type differ")
        for field in ("taxonomy_contract_id", "taxonomy_version", "source_effective_field"):
            object.__setattr__(self, field, require_nonempty(getattr(self, field), field=field))
        object.__setattr__(
            self,
            "authority_receipt_hash",
            ensure_sha256(self.authority_receipt_hash, field="authority_receipt_hash"),
        )
        require_date(self.valid_from, field="valid_from")
        if self.valid_to_exclusive is not None:
            require_date(self.valid_to_exclusive, field="valid_to_exclusive")
            if self.valid_to_exclusive <= self.valid_from:
                raise IndustryPitContractError("valid interval must be non-empty and half-open")
        require_date(self.eligible_from, field="eligible_from")
        if self.eligible_to_exclusive is not None:
            require_date(self.eligible_to_exclusive, field="eligible_to_exclusive")
            if self.eligible_to_exclusive <= self.eligible_from:
                raise IndustryPitContractError("eligible interval must be non-empty and half-open")
        if self.causal_use_from is not None:
            require_date(self.causal_use_from, field="causal_use_from")
        if self.causal_use_to_exclusive is not None:
            require_date(self.causal_use_to_exclusive, field="causal_use_to_exclusive")
            if self.causal_use_from is None or self.causal_use_to_exclusive <= self.causal_use_from:
                raise IndustryPitContractError("causal interval must be non-empty and half-open")
        if self.known_from is not None:
            require_date(self.known_from, field="known_from")
        if (self.identity is None) == (self.unavailable_reason is None):
            raise IndustryPitContractError("candidate interval must be exactly resolved or unavailable")
        normalized_authority_identity = {
            require_nonempty(key, field="authority_identity key"): require_nonempty(
                value, field=f"authority_identity.{key}"
            )
            for key, value in self.authority_identity.items()
        }
        if self.identity is not None and not normalized_authority_identity:
            raise IndustryPitContractError("resolved interval requires an authority-specific identity")
        object.__setattr__(
            self,
            "authority_identity",
            {key: normalized_authority_identity[key] for key in sorted(normalized_authority_identity)},
        )
        if self.identity is not None and self.conflict_candidates:
            raise IndustryPitContractError("resolved interval cannot carry conflict candidates")
        normalized_ids = tuple(sorted({require_nonempty(value, field="source_id") for value in self.source_ids}))
        normalized_sources = tuple(sorted({ensure_sha256(value, field="source_hash") for value in self.source_hashes}))
        normalized_lineage = tuple(sorted({ensure_sha256(value, field="lineage_hash") for value in self.lineage_hashes}))
        if not normalized_ids or not normalized_sources or not normalized_lineage:
            raise IndustryPitContractError("candidate interval requires source identities and lineage hashes")
        object.__setattr__(self, "source_ids", normalized_ids)
        object.__setattr__(self, "source_hashes", normalized_sources)
        object.__setattr__(self, "lineage_hashes", normalized_lineage)
        object.__setattr__(self, "conflict_candidates", canonical_conflicts(self.conflict_candidates))
        object.__setattr__(self, "row_hash", ensure_sha256(self.row_hash, field="row_hash"))
        if self.row_hash != sha256_hex(canonical_json_bytes(self.as_dict(include_row_hash=False))):
            raise IndustryPitContractError("candidate row_hash mismatch")
        if self.research_basis is ResearchBasis.AS_PUBLISHED_PIT and self.non_as_known_taxonomy:
            raise IndustryPitContractError("as_published_pit cannot be marked non_as_known_taxonomy")
        if self.research_basis is not ResearchBasis.AS_PUBLISHED_PIT and not self.non_as_known_taxonomy:
            raise IndustryPitContractError("non-causal research basis must be marked non_as_known_taxonomy")

    def as_dict(self, *, include_row_hash: bool = True) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "schema_version": self.schema_version,
            "canonical_symbol": self.canonical_symbol,
            "authority_type": self.authority_type.value,
            "taxonomy_contract_id": self.taxonomy_contract_id,
            "taxonomy_version": self.taxonomy_version,
            "authority_receipt_hash": self.authority_receipt_hash,
            "valid_from": self.valid_from.isoformat(),
            "valid_to_exclusive": self.valid_to_exclusive.isoformat() if self.valid_to_exclusive else None,
            "eligible_from": self.eligible_from.isoformat(),
            "eligible_to_exclusive": self.eligible_to_exclusive.isoformat() if self.eligible_to_exclusive else None,
            "causal_use_from": self.causal_use_from.isoformat() if self.causal_use_from else None,
            "causal_use_to_exclusive": (
                self.causal_use_to_exclusive.isoformat() if self.causal_use_to_exclusive else None
            ),
            "known_from": self.known_from.isoformat() if self.known_from else None,
            "source_effective_field": self.source_effective_field,
            "source_last_updated_at": self.source_last_updated_at,
            "research_basis": self.research_basis.value,
            "non_as_known_taxonomy": self.non_as_known_taxonomy,
            "identity": self.identity.as_dict() if self.identity else None,
            "authority_identity": dict(self.authority_identity),
            "unavailable_reason": self.unavailable_reason.value if self.unavailable_reason else None,
            "conflict_candidates": [dict(value) for value in self.conflict_candidates],
            "source_ids": list(self.source_ids),
            "source_hashes": list(self.source_hashes),
            "lineage_hashes": list(self.lineage_hashes),
        }
        if include_row_hash:
            payload["row_hash"] = self.row_hash
        return payload

    def with_alignment_sources(self, source_hashes: Sequence[str]) -> "CandidateInterval":
        merged = tuple(sorted({*self.source_hashes, *source_hashes}))
        payload = self.as_dict(include_row_hash=False)
        payload["source_hashes"] = list(merged)
        return replace(self, source_hashes=merged, row_hash=sha256_hex(canonical_json_bytes(payload)))


def canonical_conflicts(values: Sequence[Mapping[str, Any]]) -> tuple[Mapping[str, Any], ...]:
    normalized: dict[bytes, Mapping[str, Any]] = {}
    for value in values:
        if not isinstance(value, Mapping):
            raise IndustryPitContractError("conflict candidate must be a mapping")
        item = dict(value)
        encoded = canonical_json_bytes(item)
        normalized[encoded] = item
    return tuple(normalized[key] for key in sorted(normalized))


def make_candidate_interval(
    *,
    canonical_symbol: str,
    authority_type: AuthorityType,
    taxonomy_contract_id: str,
    taxonomy_version: str,
    authority_receipt_hash: str,
    valid_from: date,
    valid_to_exclusive: date | None,
    eligible_from: date,
    eligible_to_exclusive: date | None,
    causal_use_from: date | None,
    causal_use_to_exclusive: date | None,
    known_from: date | None,
    source_effective_field: str,
    source_last_updated_at: str | None,
    research_basis: ResearchBasis,
    non_as_known_taxonomy: bool,
    identity: TaxonomyIdentity | None,
    authority_identity: Mapping[str, str],
    unavailable_reason: UnavailableReason | None,
    conflict_candidates: Sequence[Mapping[str, Any]] = (),
    source_ids: Sequence[str],
    source_hashes: Sequence[str],
    lineage_hashes: Sequence[str],
) -> CandidateInterval:
    schema = (
        CLASSIFICATION_CANDIDATE_SCHEMA
        if authority_type is AuthorityType.CLASSIFICATION
        else INDEX_MEMBERSHIP_CANDIDATE_SCHEMA
    )
    provisional = {
        "schema_version": schema,
        "canonical_symbol": require_symbol(canonical_symbol),
        "authority_type": authority_type.value,
        "taxonomy_contract_id": taxonomy_contract_id,
        "taxonomy_version": taxonomy_version,
        "authority_receipt_hash": authority_receipt_hash,
        "valid_from": valid_from.isoformat(),
        "valid_to_exclusive": valid_to_exclusive.isoformat() if valid_to_exclusive else None,
        "eligible_from": eligible_from.isoformat(),
        "eligible_to_exclusive": eligible_to_exclusive.isoformat() if eligible_to_exclusive else None,
        "causal_use_from": causal_use_from.isoformat() if causal_use_from else None,
        "causal_use_to_exclusive": causal_use_to_exclusive.isoformat() if causal_use_to_exclusive else None,
        "known_from": known_from.isoformat() if known_from else None,
        "source_effective_field": source_effective_field,
        "source_last_updated_at": source_last_updated_at,
        "research_basis": research_basis.value,
        "non_as_known_taxonomy": non_as_known_taxonomy,
        "identity": identity.as_dict() if identity else None,
        "authority_identity": dict(authority_identity),
        "unavailable_reason": unavailable_reason.value if unavailable_reason else None,
        "conflict_candidates": [dict(value) for value in canonical_conflicts(conflict_candidates)],
        "source_ids": sorted(set(source_ids)),
        "source_hashes": sorted(set(source_hashes)),
        "lineage_hashes": sorted(set(lineage_hashes)),
    }
    try:
        row_hash = sha256_hex(canonical_json_bytes(provisional))
    except CanonicalizationError as exc:
        raise IndustryPitContractError(str(exc)) from exc
    return CandidateInterval(
        schema_version=schema,
        canonical_symbol=canonical_symbol,
        authority_type=authority_type,
        taxonomy_contract_id=taxonomy_contract_id,
        taxonomy_version=taxonomy_version,
        authority_receipt_hash=authority_receipt_hash,
        valid_from=valid_from,
        valid_to_exclusive=valid_to_exclusive,
        eligible_from=eligible_from,
        eligible_to_exclusive=eligible_to_exclusive,
        causal_use_from=causal_use_from,
        causal_use_to_exclusive=causal_use_to_exclusive,
        known_from=known_from,
        source_effective_field=source_effective_field,
        source_last_updated_at=source_last_updated_at,
        research_basis=research_basis,
        non_as_known_taxonomy=non_as_known_taxonomy,
        identity=identity,
        authority_identity=dict(authority_identity),
        unavailable_reason=unavailable_reason,
        conflict_candidates=tuple(conflict_candidates),
        source_ids=tuple(source_ids),
        source_hashes=tuple(source_hashes),
        lineage_hashes=tuple(lineage_hashes),
        row_hash=row_hash,
    )


def candidate_interval_from_mapping(value: Mapping[str, Any]) -> CandidateInterval:
    if not isinstance(value, Mapping):
        raise IndustryPitContractError("candidate interval payload must be a mapping")
    expected_keys = {
        "schema_version",
        "canonical_symbol",
        "authority_type",
        "taxonomy_contract_id",
        "taxonomy_version",
        "authority_receipt_hash",
        "valid_from",
        "valid_to_exclusive",
        "eligible_from",
        "eligible_to_exclusive",
        "causal_use_from",
        "causal_use_to_exclusive",
        "known_from",
        "source_effective_field",
        "source_last_updated_at",
        "research_basis",
        "non_as_known_taxonomy",
        "identity",
        "authority_identity",
        "unavailable_reason",
        "conflict_candidates",
        "source_ids",
        "source_hashes",
        "lineage_hashes",
        "row_hash",
    }
    if set(value) != expected_keys:
        raise IndustryPitContractError("candidate interval payload keys differ from schema")

    def optional_date(field: str) -> date | None:
        raw = value.get(field)
        if raw is None:
            return None
        try:
            return date.fromisoformat(str(raw))
        except ValueError as exc:
            raise IndustryPitContractError(f"{field} is invalid") from exc

    identity_payload = value.get("identity")
    identity = TaxonomyIdentity(**dict(identity_payload)) if isinstance(identity_payload, Mapping) else None
    try:
        interval = make_candidate_interval(
            canonical_symbol=str(value["canonical_symbol"]),
            authority_type=AuthorityType(str(value["authority_type"])),
            taxonomy_contract_id=str(value["taxonomy_contract_id"]),
            taxonomy_version=str(value["taxonomy_version"]),
            authority_receipt_hash=str(value["authority_receipt_hash"]),
            valid_from=date.fromisoformat(str(value["valid_from"])),
            valid_to_exclusive=optional_date("valid_to_exclusive"),
            eligible_from=date.fromisoformat(str(value["eligible_from"])),
            eligible_to_exclusive=optional_date("eligible_to_exclusive"),
            causal_use_from=optional_date("causal_use_from"),
            causal_use_to_exclusive=optional_date("causal_use_to_exclusive"),
            known_from=optional_date("known_from"),
            source_effective_field=str(value["source_effective_field"]),
            source_last_updated_at=(
                str(value["source_last_updated_at"]) if value.get("source_last_updated_at") is not None else None
            ),
            research_basis=ResearchBasis(str(value["research_basis"])),
            non_as_known_taxonomy=bool(value["non_as_known_taxonomy"]),
            identity=identity,
            authority_identity=dict(value.get("authority_identity") or {}),
            unavailable_reason=(
                UnavailableReason(str(value["unavailable_reason"]))
                if value.get("unavailable_reason") is not None
                else None
            ),
            conflict_candidates=tuple(value.get("conflict_candidates") or ()),
            source_ids=tuple(value.get("source_ids") or ()),
            source_hashes=tuple(value.get("source_hashes") or ()),
            lineage_hashes=tuple(value.get("lineage_hashes") or ()),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise IndustryPitContractError(f"candidate interval payload is invalid: {exc}") from exc
    if interval.schema_version != value["schema_version"] or interval.row_hash != value["row_hash"]:
        raise IndustryPitContractError("candidate interval readback hash mismatch")
    return interval


@dataclass(frozen=True, slots=True)
class ResolutionRequest:
    canonical_symbol: str
    trade_date: date
    authority_type: AuthorityType
    taxonomy_contract_id: str
    taxonomy_version: str
    authority_receipt_hash: str
    knowledge_time_policy: KnowledgeTimePolicy
    research_basis: ResearchBasis

    def __post_init__(self) -> None:
        object.__setattr__(self, "canonical_symbol", require_symbol(self.canonical_symbol))
        require_date(self.trade_date, field="trade_date")
        object.__setattr__(
            self,
            "authority_receipt_hash",
            ensure_sha256(self.authority_receipt_hash, field="authority_receipt_hash"),
        )
        for field in ("taxonomy_contract_id", "taxonomy_version"):
            object.__setattr__(self, field, require_nonempty(getattr(self, field), field=field))


@dataclass(frozen=True, slots=True)
class ResolvedIndustryIdentity:
    status: str
    canonical_symbol: str
    trade_date: date
    authority_type: AuthorityType
    identity: TaxonomyIdentity
    authority_identity: Mapping[str, str]
    valid_from: date
    valid_to_exclusive: date | None
    known_from: date | None
    taxonomy_contract_id: str
    taxonomy_version: str
    source_ids: tuple[str, ...]
    source_hashes: tuple[str, ...]
    row_hashes: tuple[str, ...]
    authority_receipt_hash: str
    non_as_known_taxonomy: bool
    alignment_state: AlignmentState
    exact_duplicate_collapsed: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "canonical_symbol": self.canonical_symbol,
            "trade_date": self.trade_date.isoformat(),
            "authority_type": self.authority_type.value,
            "identity": self.identity.as_dict(),
            "authority_identity": dict(self.authority_identity),
            "valid_from": self.valid_from.isoformat(),
            "valid_to_exclusive": self.valid_to_exclusive.isoformat() if self.valid_to_exclusive else None,
            "known_from": self.known_from.isoformat() if self.known_from else None,
            "taxonomy_contract_id": self.taxonomy_contract_id,
            "taxonomy_version": self.taxonomy_version,
            "source_ids": list(self.source_ids),
            "source_hashes": list(self.source_hashes),
            "row_hashes": list(self.row_hashes),
            "authority_receipt_hash": self.authority_receipt_hash,
            "non_as_known_taxonomy": self.non_as_known_taxonomy,
            "alignment_state": self.alignment_state.value,
            "exact_duplicate_collapsed": self.exact_duplicate_collapsed,
        }


@dataclass(frozen=True, slots=True)
class UnavailableIndustryIdentity:
    status: str
    canonical_symbol: str
    trade_date: date
    authority_type: AuthorityType
    reason: UnavailableReason
    conflict_candidates: tuple[Mapping[str, Any], ...]
    authority_receipt_hash: str
    alignment_state: AlignmentState = AlignmentState.UNAVAILABLE

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "canonical_symbol": self.canonical_symbol,
            "trade_date": self.trade_date.isoformat(),
            "authority_type": self.authority_type.value,
            "reason": self.reason.value,
            "conflict_candidates": [dict(value) for value in self.conflict_candidates],
            "authority_receipt_hash": self.authority_receipt_hash,
            "alignment_state": self.alignment_state.value,
        }


Resolution = ResolvedIndustryIdentity | UnavailableIndustryIdentity


@dataclass(frozen=True, slots=True)
class DualAuthorityResolution:
    schema_version: str
    canonical_symbol: str
    trade_date: date
    classification: Resolution
    index_membership: Resolution
    alignment_state: AlignmentState

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "canonical_symbol": self.canonical_symbol,
            "trade_date": self.trade_date.isoformat(),
            "classification": self.classification.as_dict(),
            "index_membership": self.index_membership.as_dict(),
            "alignment_state": self.alignment_state.value,
        }


__all__ = [
    "AUTHORITY_RECEIPT_SCHEMA",
    "CANDIDATE_BUNDLE_SCHEMA",
    "CLASSIFICATION_CANDIDATE_SCHEMA",
    "DUAL_AUTHORITY_RESOLUTION_SCHEMA",
    "INDEX_MEMBERSHIP_CANDIDATE_SCHEMA",
    "PREFLIGHT_REPORT_SCHEMA",
    "TAXONOMY_CATALOG_SCHEMA",
    "AlignmentState",
    "AuthorityReceipt",
    "AuthorityType",
    "CandidateInterval",
    "DualAuthorityResolution",
    "IndustryPitContractError",
    "KnowledgeTimePolicy",
    "ResearchBasis",
    "Resolution",
    "ResolutionRequest",
    "ResolvedIndustryIdentity",
    "TaxonomyIdentity",
    "UnavailableIndustryIdentity",
    "UnavailableReason",
    "authority_receipt_from_mapping",
    "candidate_interval_from_mapping",
    "make_candidate_interval",
    "require_date",
    "require_symbol",
]
