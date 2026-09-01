"""C-013 shared industry-PIT adapter for HMM stock-fact aggregation.

The adapter keeps stock classification and published-index membership as two
independent authorities.  Classification decides contributor ownership.  The
index authority is used only to project the already-aligned taxonomy identity
onto the stable 31/131 HMM sector codes.
"""

from __future__ import annotations

import hashlib
import re
from bisect import bisect_left
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

from backend.services.dataset_release.canonical import canonical_json_bytes, digest_named_fields
from backend.services.industry_pit.artifact_store import CandidateBundleReadback, read_candidate_bundle
from backend.services.industry_pit.candidate_builder import FrozenDenominator
from backend.services.industry_pit.contracts import (
    CLASSIFICATION_CANDIDATE_SCHEMA,
    AuthorityReceipt,
    IndustryPitContractError,
    KnowledgeTimePolicy,
    ResearchBasis,
    ResolutionRequest,
    ResolvedIndustryIdentity,
    TaxonomyIdentity,
    UnavailableIndustryIdentity,
    UnavailableReason,
    make_candidate_interval,
)
from backend.services.industry_pit.resolver import IndustryPitResolver, resolve_dual_authority

from .state_model_set import StateModelSetError


HMM_INDUSTRY_PIT_AUTHORITY_SCHEMA = "hmm_risk_industry_pit_authority_v1"
HMM_INDUSTRY_PIT_PREFLIGHT_SCHEMA = "hmm_risk_industry_pit_601d_preflight_v2"
HMM_MAPPING_MANIFEST_SCHEMA = "hmm_risk_pit_mapping_manifest_v3"
HMM_L1_CODE_PROJECTION_SCHEMA = "hmm_risk_industry_l1_code_projection_v1"
HMM_L1_CODE_PROJECTION_ROW_SCHEMA = "hmm_risk_industry_l1_code_projection_row_v1"
HMM_L2_CODE_PROJECTION_SCHEMA = "hmm_risk_industry_l2_code_projection_v1"
HMM_L2_CODE_PROJECTION_ROW_SCHEMA = "hmm_risk_industry_l2_code_projection_row_v1"
HMM_INDUSTRY_RESEARCH_BASIS_SCHEMA = "hmm_risk_industry_pit_research_basis_v1"
HMM_STABLE_BACKCAST_CANDIDATE_SCHEMA = "hmm_risk_stable_taxonomy_backcast_candidate_v1"
HMM_G2A_DATA_A_CONTRACT_VERSION = "c013_g2a_data_a_v1"
HMM_L1_CODE_PROJECTION_VERSION = "sw2021_taxonomy_to_published_l1_v1"
HMM_L2_CODE_PROJECTION_VERSION = "sw2021_taxonomy_to_published_member_backed_l2_v1"
EXPECTED_PREFLIGHT_TRADING_DAYS = 601
_CANONICAL_SW_L1_CODE = re.compile(r"^801[0-9]{3}[.]SI$")
_CANONICAL_SW_L2_CODE = re.compile(r"^801[0-9]{3}[.]SI$")
_TAXONOMY_CODE = re.compile(r"^[0-9]{6}$")


def _require_sha256(value: Any, field: str) -> str:
    normalized = str(value or "").strip().lower()
    if len(normalized) != 64 or any(character not in "0123456789abcdef" for character in normalized):
        raise StateModelSetError(f"{field} must be a lowercase SHA-256")
    return normalized


def _require_nonempty_provenance_sequence(
    value: Any,
    field: str,
    *,
    sha256: bool = False,
) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)) or not value:
        raise StateModelSetError(f"{field} must be a non-empty sequence")
    normalized: list[str] = []
    for raw in value:
        if not isinstance(raw, str) or not raw.strip():
            raise StateModelSetError(f"{field} contains an invalid identity")
        normalized.append(_require_sha256(raw, field) if sha256 else raw.strip())
    if len(set(normalized)) != len(normalized):
        raise StateModelSetError(f"{field} contains duplicate identities")
    return tuple(normalized)


def _request(resolver: IndustryPitResolver, *, symbol: str, trade_date: date) -> ResolutionRequest:
    receipt = resolver.receipt
    return ResolutionRequest(
        canonical_symbol=symbol,
        trade_date=trade_date,
        authority_type=receipt.authority_type,
        taxonomy_contract_id=receipt.taxonomy_contract_id,
        taxonomy_version=receipt.taxonomy_version,
        authority_receipt_hash=receipt.receipt_hash,
        knowledge_time_policy=receipt.knowledge_time_policy,
        research_basis=receipt.research_basis,
    )


def build_l1_code_projection_authority(
    *,
    taxonomy_contract_id: str,
    taxonomy_version: str,
    projection_version: str,
    taxonomy_rows: Sequence[Mapping[str, Any]],
    published_index_rows: Sequence[Mapping[str, Any]],
    source_ids: Sequence[str],
    source_hashes: Sequence[str],
) -> Mapping[str, Any]:
    """Build the frozen SW2021 taxonomy-code to published-index-code authority.

    The join key is the numeric ``industry_code`` carried by both source
    authorities.  Names are checked only as a readback invariant and are never
    used to infer or repair a mapping.
    """

    taxonomy: dict[str, str] = {}
    for raw in taxonomy_rows:
        code = str(raw.get("industry_code") or "").strip()
        name = str(raw.get("industry_name") or raw.get("l1_name") or "").strip()
        if _TAXONOMY_CODE.fullmatch(code) is None or not code.endswith("0000") or not name:
            raise StateModelSetError("HMM industry PIT taxonomy L1 projection source row is invalid")
        if code in taxonomy:
            raise StateModelSetError("HMM industry PIT taxonomy L1 projection source is duplicated")
        taxonomy[code] = name
    published: dict[str, tuple[str, str]] = {}
    for raw in published_index_rows:
        code = str(raw.get("industry_code") or "").strip()
        index_code = str(raw.get("index_code") or "").strip()
        name = str(raw.get("industry_name") or "").strip()
        if _TAXONOMY_CODE.fullmatch(code) is None or _CANONICAL_SW_L1_CODE.fullmatch(index_code) is None or not name:
            raise StateModelSetError("HMM industry PIT published-index L1 projection source row is invalid")
        if code in published:
            raise StateModelSetError("HMM industry PIT published-index L1 projection source is duplicated")
        published[code] = (index_code, name)
    if len(taxonomy) != 31 or set(taxonomy) != set(published):
        raise StateModelSetError("HMM industry PIT L1 projection sources do not close the same 31 taxonomy codes")
    rows: list[dict[str, str]] = []
    canonical_codes: set[str] = set()
    for taxonomy_code, taxonomy_name in sorted(taxonomy.items()):
        canonical_code, canonical_name = published[taxonomy_code]
        if taxonomy_name != canonical_name:
            raise StateModelSetError("HMM industry PIT L1 projection source names differ for one numeric taxonomy code")
        if canonical_code in canonical_codes:
            raise StateModelSetError("HMM industry PIT L1 projection canonical code is duplicated")
        canonical_codes.add(canonical_code)
        row_body = {
            "taxonomy_l1_code": taxonomy_code,
            "taxonomy_l1_name": taxonomy_name,
            "canonical_l1_code": canonical_code,
            "canonical_l1_name": canonical_name,
        }
        rows.append(
            {
                **row_body,
                "row_hash": digest_named_fields(HMM_L1_CODE_PROJECTION_ROW_SCHEMA, row_body),
            }
        )
    normalized_ids = sorted({str(value).strip() for value in source_ids if str(value).strip()})
    normalized_hashes = sorted(
        {_require_sha256(value, "industry_pit.l1_projection.source_hash") for value in source_hashes}
    )
    if not normalized_ids or not normalized_hashes:
        raise StateModelSetError("HMM industry PIT L1 projection requires source identities and hashes")
    body = {
        "schema_version": HMM_L1_CODE_PROJECTION_SCHEMA,
        "projection_version": str(projection_version).strip(),
        "taxonomy_contract_id": str(taxonomy_contract_id).strip(),
        "taxonomy_version": str(taxonomy_version).strip(),
        "source_ids": normalized_ids,
        "source_hashes": normalized_hashes,
        "rows": rows,
    }
    if not body["projection_version"] or not body["taxonomy_contract_id"] or not body["taxonomy_version"]:
        raise StateModelSetError("HMM industry PIT L1 projection version identity is incomplete")
    return {
        **body,
        "canonical_hash": digest_named_fields(HMM_L1_CODE_PROJECTION_SCHEMA, body),
    }


def build_l2_code_projection_authority(
    *,
    taxonomy_contract_id: str,
    taxonomy_version: str,
    projection_version: str,
    taxonomy_rows: Sequence[Mapping[str, Any]],
    published_index_rows: Sequence[Mapping[str, Any]],
    member_index_rows: Sequence[Mapping[str, Any]],
    l1_projection_authority: Mapping[str, Any],
    source_ids: Sequence[str],
    source_hashes: Sequence[str],
) -> Mapping[str, Any]:
    """Freeze the full 131-member-backed published SW2021 L2 catalog.

    Historical stock classification remains owned by C-013.  This authority
    only projects an already-resolved taxonomy L2 identity onto its published
    index code.  The 131 denominator is supplied by the frozen member catalog,
    never inferred from L2 identities observed in a training window.
    """

    expected_l1_keys = {
        "schema_version",
        "projection_version",
        "taxonomy_contract_id",
        "taxonomy_version",
        "source_ids",
        "source_hashes",
        "rows",
        "canonical_hash",
    }
    if (
        projection_version != HMM_L2_CODE_PROJECTION_VERSION
        or not str(taxonomy_contract_id).strip()
        or not str(taxonomy_version).strip()
        or not isinstance(l1_projection_authority, Mapping)
        or set(l1_projection_authority) != expected_l1_keys
        or l1_projection_authority.get("schema_version") != HMM_L1_CODE_PROJECTION_SCHEMA
        or l1_projection_authority.get("projection_version") != HMM_L1_CODE_PROJECTION_VERSION
        or l1_projection_authority.get("taxonomy_contract_id") != taxonomy_contract_id
        or l1_projection_authority.get("taxonomy_version") != taxonomy_version
    ):
        raise StateModelSetError("HMM industry PIT L2 projection requires the matching frozen L1 projection")
    l1_body = {key: value for key, value in l1_projection_authority.items() if key != "canonical_hash"}
    if digest_named_fields(HMM_L1_CODE_PROJECTION_SCHEMA, l1_body) != _require_sha256(
        l1_projection_authority.get("canonical_hash"), "industry_pit.l2_projection.l1_projection_hash"
    ):
        raise StateModelSetError("HMM industry PIT L2 projection L1 authority hash is invalid")
    raw_l1_rows = l1_projection_authority.get("rows")
    if not isinstance(raw_l1_rows, list) or len(raw_l1_rows) != 31:
        raise StateModelSetError("HMM industry PIT L2 projection L1 authority must contain exactly 31 rows")
    l1_source_ids = l1_projection_authority.get("source_ids")
    l1_source_hashes = l1_projection_authority.get("source_hashes")
    if (
        not isinstance(l1_source_ids, list)
        or not l1_source_ids
        or l1_source_ids != sorted({str(value).strip() for value in l1_source_ids if str(value).strip()})
        or not isinstance(l1_source_hashes, list)
        or not l1_source_hashes
        or l1_source_hashes
        != sorted({_require_sha256(value, "industry_pit.l2_projection.l1_source_hash") for value in l1_source_hashes})
    ):
        raise StateModelSetError("HMM industry PIT L2 projection L1 provenance is invalid")
    l1_by_taxonomy: dict[str, tuple[str, str]] = {}
    for raw in raw_l1_rows:
        expected_l1_row_keys = {
            "taxonomy_l1_code",
            "taxonomy_l1_name",
            "canonical_l1_code",
            "canonical_l1_name",
            "row_hash",
        }
        if not isinstance(raw, Mapping) or set(raw) != expected_l1_row_keys:
            raise StateModelSetError("HMM industry PIT L2 projection L1 row is invalid")
        taxonomy_l1_code = str(raw.get("taxonomy_l1_code") or "").strip()
        taxonomy_l1_name = str(raw.get("taxonomy_l1_name") or "").strip()
        canonical_l1_code = str(raw.get("canonical_l1_code") or "").strip()
        canonical_l1_name = str(raw.get("canonical_l1_name") or "").strip()
        row_body = {key: raw[key] for key in expected_l1_row_keys if key != "row_hash"}
        if (
            _TAXONOMY_CODE.fullmatch(taxonomy_l1_code) is None
            or _CANONICAL_SW_L1_CODE.fullmatch(canonical_l1_code) is None
            or taxonomy_l1_name != canonical_l1_name
            or taxonomy_l1_code in l1_by_taxonomy
            or digest_named_fields(HMM_L1_CODE_PROJECTION_ROW_SCHEMA, row_body)
            != _require_sha256(raw.get("row_hash"), "industry_pit.l2_projection.l1_row_hash")
        ):
            raise StateModelSetError("HMM industry PIT L2 projection L1 row identity is invalid")
        l1_by_taxonomy[taxonomy_l1_code] = (canonical_l1_code, canonical_l1_name)
    if len(l1_by_taxonomy) != 31 or len({value[0] for value in l1_by_taxonomy.values()}) != 31:
        raise StateModelSetError("HMM industry PIT L2 projection L1 authority does not close 31 sectors")

    taxonomy: dict[str, tuple[str, str, str]] = {}
    for raw in taxonomy_rows:
        if not isinstance(raw, Mapping):
            raise StateModelSetError("HMM industry PIT taxonomy L2 projection source row is invalid")
        taxonomy_l1_code = str(raw.get("taxonomy_l1_code") or "").strip()
        taxonomy_l1_name = str(raw.get("taxonomy_l1_name") or "").strip()
        taxonomy_l2_code = str(raw.get("taxonomy_l2_code") or "").strip()
        taxonomy_l2_name = str(raw.get("taxonomy_l2_name") or "").strip()
        if (
            _TAXONOMY_CODE.fullmatch(taxonomy_l1_code) is None
            or _TAXONOMY_CODE.fullmatch(taxonomy_l2_code) is None
            or not taxonomy_l1_name
            or not taxonomy_l2_name
            or l1_by_taxonomy.get(taxonomy_l1_code, (None, None))[1] != taxonomy_l1_name
            or taxonomy_l2_code in taxonomy
        ):
            raise StateModelSetError("HMM industry PIT taxonomy L2 projection source row is invalid")
        taxonomy[taxonomy_l2_code] = (taxonomy_l1_code, taxonomy_l1_name, taxonomy_l2_name)

    published: dict[str, tuple[str, str, str]] = {}
    for raw in published_index_rows:
        if not isinstance(raw, Mapping):
            raise StateModelSetError("HMM industry PIT published-index L2 projection source row is invalid")
        taxonomy_l2_code = str(raw.get("industry_code") or "").strip()
        canonical_l2_code = str(raw.get("index_code") or "").strip()
        canonical_l2_name = str(raw.get("industry_name") or "").strip()
        canonical_l1_code = str(raw.get("parent_code") or "").strip()
        if (
            str(raw.get("level") or "").strip().upper() != "L2"
            or _TAXONOMY_CODE.fullmatch(taxonomy_l2_code) is None
            or _CANONICAL_SW_L2_CODE.fullmatch(canonical_l2_code) is None
            or _CANONICAL_SW_L1_CODE.fullmatch(canonical_l1_code) is None
            or not canonical_l2_name
            or taxonomy_l2_code in published
        ):
            raise StateModelSetError("HMM industry PIT published-index L2 projection source row is invalid")
        published[taxonomy_l2_code] = (canonical_l2_code, canonical_l2_name, canonical_l1_code)
    if len(taxonomy) != 134 or len(published) != 134 or set(taxonomy) != set(published):
        raise StateModelSetError("HMM industry PIT L2 projection sources do not close the same 134 taxonomy codes")
    if len({value[0] for value in published.values()}) != 134:
        raise StateModelSetError("HMM industry PIT published-index L2 projection source has duplicate index codes")

    member_owners: dict[str, set[str]] = defaultdict(set)
    for raw in member_index_rows:
        if not isinstance(raw, Mapping):
            raise StateModelSetError("HMM industry PIT member-backed L2 source row is invalid")
        source_l1_code = str(raw.get("l1_code") or "").strip()
        canonical_l2_code = str(raw.get("l2_code") or "").strip()
        if _CANONICAL_SW_L1_CODE.fullmatch(source_l1_code) is not None:
            canonical_l1_code = source_l1_code
        elif _TAXONOMY_CODE.fullmatch(source_l1_code) is not None:
            projected_l1 = l1_by_taxonomy.get(source_l1_code)
            canonical_l1_code = projected_l1[0] if projected_l1 is not None else ""
        else:
            canonical_l1_code = ""
        if not canonical_l1_code or _CANONICAL_SW_L2_CODE.fullmatch(canonical_l2_code) is None:
            raise StateModelSetError("HMM industry PIT member-backed L2 source row is invalid")
        member_owners[canonical_l2_code].add(canonical_l1_code)
    published_by_index = {value[0]: value for value in published.values()}
    if len(member_owners) != 131 or not set(member_owners).issubset(published_by_index):
        raise StateModelSetError("HMM industry PIT member-backed L2 catalog must contain exactly 131 published codes")
    for canonical_l2_code, owners in member_owners.items():
        if len(owners) != 1 or next(iter(owners)) != published_by_index[canonical_l2_code][2]:
            raise StateModelSetError("HMM industry PIT member-backed L2 parent ownership differs")

    member_backed_set = set(member_owners)
    rows: list[dict[str, str]] = []
    for taxonomy_l2_code, (canonical_l2_code, canonical_l2_name, canonical_l1_code) in sorted(published.items()):
        if canonical_l2_code not in member_backed_set:
            continue
        taxonomy_l1_code, taxonomy_l1_name, taxonomy_l2_name = taxonomy[taxonomy_l2_code]
        projected_l1_code, projected_l1_name = l1_by_taxonomy[taxonomy_l1_code]
        if (
            canonical_l1_code != projected_l1_code
            or canonical_l2_name != taxonomy_l2_name
            or projected_l1_name != taxonomy_l1_name
        ):
            raise StateModelSetError("HMM industry PIT L2 projection parent or name authority differs")
        row_body = {
            "taxonomy_l1_code": taxonomy_l1_code,
            "taxonomy_l1_name": taxonomy_l1_name,
            "taxonomy_l2_code": taxonomy_l2_code,
            "taxonomy_l2_name": taxonomy_l2_name,
            "canonical_l1_code": projected_l1_code,
            "canonical_l1_name": projected_l1_name,
            "canonical_l2_code": canonical_l2_code,
            "canonical_l2_name": canonical_l2_name,
        }
        rows.append({**row_body, "row_hash": digest_named_fields(HMM_L2_CODE_PROJECTION_ROW_SCHEMA, row_body)})
    if len(rows) != 131:
        raise StateModelSetError("HMM industry PIT L2 projection output does not close 131 member-backed sectors")

    normalized_ids = sorted(_require_nonempty_provenance_sequence(source_ids, "industry_pit.l2_projection.source_ids"))
    normalized_hashes = sorted(
        _require_nonempty_provenance_sequence(
            source_hashes,
            "industry_pit.l2_projection.source_hashes",
            sha256=True,
        )
    )
    body = {
        "schema_version": HMM_L2_CODE_PROJECTION_SCHEMA,
        "projection_version": str(projection_version).strip(),
        "taxonomy_contract_id": str(taxonomy_contract_id).strip(),
        "taxonomy_version": str(taxonomy_version).strip(),
        "l1_projection_sha256": str(l1_projection_authority["canonical_hash"]),
        "source_ids": normalized_ids,
        "source_hashes": normalized_hashes,
        "rows": rows,
    }
    if not body["projection_version"] or not body["taxonomy_contract_id"] or not body["taxonomy_version"]:
        raise StateModelSetError("HMM industry PIT L2 projection version identity is incomplete")
    return {**body, "canonical_hash": digest_named_fields(HMM_L2_CODE_PROJECTION_SCHEMA, body)}


@dataclass(frozen=True, slots=True)
class HMMIndustryProjection:
    status: str
    canonical_symbol: str
    trade_date: date
    l1_code: str | None
    l1_name: str | None
    l2_code: str | None
    l2_name: str | None
    reason_code: str | None
    classification_receipt_hash: str
    index_membership_receipt_hash: str
    classification_row_hashes: tuple[str, ...]
    index_membership_row_hashes: tuple[str, ...]
    alignment_state: str
    classification_research_basis: str
    non_as_known_taxonomy: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "canonical_symbol": self.canonical_symbol,
            "trade_date": self.trade_date.isoformat(),
            "l1_code": self.l1_code,
            "l1_name": self.l1_name,
            "l2_code": self.l2_code,
            "l2_name": self.l2_name,
            "reason_code": self.reason_code,
            "classification_receipt_hash": self.classification_receipt_hash,
            "index_membership_receipt_hash": self.index_membership_receipt_hash,
            "classification_row_hashes": list(self.classification_row_hashes),
            "index_membership_row_hashes": list(self.index_membership_row_hashes),
            "alignment_state": self.alignment_state,
            "classification_research_basis": self.classification_research_basis,
            "non_as_known_taxonomy": self.non_as_known_taxonomy,
        }


class HMMIndustryPitAdapter:
    """Read one immutable shared PIT bundle without copying resolver logic."""

    def __init__(self, *, authority_bundle: CandidateBundleReadback) -> None:
        classification_receipt = authority_bundle.classification_receipt
        index_receipt = authority_bundle.index_membership_receipt
        if classification_receipt.denominator_digest != index_receipt.denominator_digest:
            raise StateModelSetError("HMM industry PIT authority denominator digests differ")
        if classification_receipt.frozen_denominator != index_receipt.frozen_denominator:
            raise StateModelSetError("HMM industry PIT authority denominator counts differ")
        known = {
            (classification_receipt.taxonomy_contract_id, classification_receipt.taxonomy_version),
            (index_receipt.taxonomy_contract_id, index_receipt.taxonomy_version),
        }
        self.authority_bundle = authority_bundle
        self.classification_resolver = IndustryPitResolver(
            receipt=classification_receipt,
            intervals=authority_bundle.classification_intervals,
            known_taxonomy_versions=known,
        )
        self.index_membership_resolver = IndustryPitResolver(
            receipt=index_receipt,
            intervals=authority_bundle.index_membership_intervals,
            known_taxonomy_versions=known,
        )
        self._taxonomy_l1, self._taxonomy_l2_by_l1 = self._build_taxonomy_projection()
        self._classification_lookup: dict[tuple[str, str], dict[str, str]] | None = None
        self._constituents: dict[str, dict[str, Any]] | None = None
        self._l1_projection_by_taxonomy: dict[str, str] | None = None
        self._l1_projection_sha256: str | None = None
        self._l2_projection_by_taxonomy: dict[str, tuple[str, str, str]] | None = None
        self._l2_projection_sha256: str | None = None
        self._source_classification_receipt_hash = classification_receipt.receipt_hash
        self._research_basis_contract_sha256: str | None = None
        self._active_classification_basis = classification_receipt.research_basis.value
        self._active_non_as_known_taxonomy = False
        self._stable_backcast_candidate_sha256: str | None = None

    @classmethod
    def from_artifact_root(
        cls,
        *,
        artifact_root: Path,
        forbidden_roots: Sequence[Path],
        expected_identity: Mapping[str, Any],
    ) -> "HMMIndustryPitAdapter":
        bundle = read_candidate_bundle(artifact_root=artifact_root, forbidden_roots=forbidden_roots)
        expected_keys = {
            "schema_version",
            "bundle_hash",
            "classification_candidate_hash",
            "index_membership_candidate_hash",
            "classification_receipt_hash",
            "index_membership_receipt_hash",
            "preflight_canonical_hash",
        }
        if set(expected_identity) != expected_keys:
            raise StateModelSetError("HMM industry PIT authority identity keys differ from schema")
        if expected_identity.get("schema_version") != HMM_INDUSTRY_PIT_AUTHORITY_SCHEMA:
            raise StateModelSetError("HMM industry PIT authority schema is invalid")
        observed = {
            "bundle_hash": bundle.manifest.get("bundle_hash"),
            "classification_candidate_hash": bundle.manifest.get("classification_candidate_hash"),
            "index_membership_candidate_hash": bundle.manifest.get("index_membership_candidate_hash"),
            "classification_receipt_hash": bundle.classification_receipt.receipt_hash,
            "index_membership_receipt_hash": bundle.index_membership_receipt.receipt_hash,
            "preflight_canonical_hash": bundle.preflight_report.get("canonical_hash"),
        }
        for field, observed_value in observed.items():
            expected_value = _require_sha256(expected_identity.get(field), f"industry_pit.{field}")
            if observed_value != expected_value:
                raise StateModelSetError(f"HMM industry PIT {field} differs from frozen authority")
        return cls(authority_bundle=bundle)

    def _build_taxonomy_projection(self) -> tuple[dict[str, str], dict[str, set[tuple[str, str]]]]:
        l1_values: dict[str, set[str]] = defaultdict(set)
        l2_by_l1: dict[str, set[tuple[str, str]]] = defaultdict(set)
        for interval in self.authority_bundle.classification_intervals:
            if interval.identity is None or interval.unavailable_reason is not None:
                continue
            identity = interval.identity
            l1_values[identity.l1_code].add(identity.l1_name)
            l2_by_l1[identity.l1_code].add((identity.l2_code, identity.l2_name))
        conflicts = sorted(code for code, names in l1_values.items() if len(names) != 1)
        if conflicts:
            raise StateModelSetError(f"HMM industry PIT classification L1 names conflict: {conflicts}")
        if len(l1_values) != 31:
            raise StateModelSetError(
                f"HMM industry PIT classification projection must contain canonical L1=31; actual={len(l1_values)}"
            )
        return ({code: next(iter(names)) for code, names in sorted(l1_values.items())}, l2_by_l1)

    def bind_l1_code_projection(self, authority: Mapping[str, Any]) -> None:
        expected_keys = {
            "schema_version",
            "projection_version",
            "taxonomy_contract_id",
            "taxonomy_version",
            "source_ids",
            "source_hashes",
            "rows",
            "canonical_hash",
        }
        if (
            not isinstance(authority, Mapping)
            or set(authority) != expected_keys
            or authority.get("schema_version") != HMM_L1_CODE_PROJECTION_SCHEMA
            or authority.get("projection_version") != HMM_L1_CODE_PROJECTION_VERSION
            or authority.get("taxonomy_contract_id") != self.classification_resolver.receipt.taxonomy_contract_id
            or authority.get("taxonomy_version") != self.classification_resolver.receipt.taxonomy_version
        ):
            raise StateModelSetError("HMM industry PIT L1 code projection authority is invalid")
        body = {key: value for key, value in authority.items() if key != "canonical_hash"}
        if digest_named_fields(HMM_L1_CODE_PROJECTION_SCHEMA, body) != _require_sha256(
            authority.get("canonical_hash"), "industry_pit.l1_projection.canonical_hash"
        ):
            raise StateModelSetError("HMM industry PIT L1 code projection hash is invalid")
        source_ids = authority.get("source_ids")
        source_hashes = authority.get("source_hashes")
        if (
            not str(authority.get("projection_version") or "").strip()
            or not isinstance(source_ids, list)
            or not source_ids
            or source_ids != sorted({str(value).strip() for value in source_ids if str(value).strip()})
            or not isinstance(source_hashes, list)
            or not source_hashes
            or source_hashes
            != sorted({_require_sha256(value, "industry_pit.l1_projection.source_hash") for value in source_hashes})
        ):
            raise StateModelSetError("HMM industry PIT L1 code projection provenance is invalid")
        raw_rows = authority.get("rows")
        if not isinstance(raw_rows, list) or len(raw_rows) != 31:
            raise StateModelSetError("HMM industry PIT L1 code projection must contain exactly 31 rows")
        projection_by_taxonomy: dict[str, str] = {}
        for raw in raw_rows:
            expected_row_keys = {
                "taxonomy_l1_code",
                "taxonomy_l1_name",
                "canonical_l1_code",
                "canonical_l1_name",
                "row_hash",
            }
            if not isinstance(raw, Mapping) or set(raw) != expected_row_keys:
                raise StateModelSetError("HMM industry PIT L1 code projection row is invalid")
            taxonomy_code = str(raw.get("taxonomy_l1_code") or "").strip()
            taxonomy_name = str(raw.get("taxonomy_l1_name") or "").strip()
            canonical_code = str(raw.get("canonical_l1_code") or "").strip()
            canonical_name = str(raw.get("canonical_l1_name") or "").strip()
            row_body = {key: raw[key] for key in expected_row_keys if key != "row_hash"}
            if (
                _TAXONOMY_CODE.fullmatch(taxonomy_code) is None
                or not _CANONICAL_SW_L1_CODE.fullmatch(canonical_code)
                or taxonomy_name != canonical_name
                or self._taxonomy_l1.get(taxonomy_code) != taxonomy_name
                or digest_named_fields(HMM_L1_CODE_PROJECTION_ROW_SCHEMA, row_body)
                != _require_sha256(raw.get("row_hash"), "industry_pit.l1_projection.row_hash")
                or taxonomy_code in projection_by_taxonomy
            ):
                raise StateModelSetError("HMM industry PIT L1 code projection row identity is invalid")
            projection_by_taxonomy[taxonomy_code] = canonical_code
        if set(projection_by_taxonomy) != set(self._taxonomy_l1) or len(set(projection_by_taxonomy.values())) != 31:
            raise StateModelSetError("HMM industry PIT L1 code projection does not close the frozen 31-sector catalog")
        rows: list[dict[str, str]] = []
        for taxonomy_code, name in sorted(self._taxonomy_l1.items()):
            l1_code = projection_by_taxonomy[taxonomy_code]
            rows.append(
                {
                    "level": "L1",
                    "index_code": l1_code,
                    "industry_code": taxonomy_code,
                    "industry_name": name,
                }
            )
        lookup: dict[tuple[str, str], dict[str, str]] = {}
        for row in rows:
            value = {
                "level": "L1",
                "index_code": row["index_code"],
                "industry_code": row["industry_code"],
                "name": row["industry_name"],
            }
            for alias in (row["index_code"], row["industry_code"]):
                key = ("L1", alias)
                if key in lookup and lookup[key] != value:
                    raise StateModelSetError(f"HMM industry PIT L1 projection alias conflicts: {key}")
                lookup[key] = value
        canonical_hash = str(authority["canonical_hash"])
        if self._l1_projection_sha256 is not None:
            if self._l1_projection_sha256 != canonical_hash:
                raise StateModelSetError("HMM industry PIT L1 code projection cannot be rebound")
            return
        self._classification_lookup = lookup
        self._l1_projection_by_taxonomy = projection_by_taxonomy
        self._l1_projection_sha256 = canonical_hash

    def bind_l2_code_projection(self, authority: Mapping[str, Any]) -> None:
        if self._l1_projection_by_taxonomy is None or self._classification_lookup is None:
            raise StateModelSetError("HMM industry PIT L1 code projection must be bound before L2")
        expected_keys = {
            "schema_version",
            "projection_version",
            "taxonomy_contract_id",
            "taxonomy_version",
            "l1_projection_sha256",
            "source_ids",
            "source_hashes",
            "rows",
            "canonical_hash",
        }
        if (
            not isinstance(authority, Mapping)
            or set(authority) != expected_keys
            or authority.get("schema_version") != HMM_L2_CODE_PROJECTION_SCHEMA
            or authority.get("projection_version") != HMM_L2_CODE_PROJECTION_VERSION
            or authority.get("taxonomy_contract_id") != self.classification_resolver.receipt.taxonomy_contract_id
            or authority.get("taxonomy_version") != self.classification_resolver.receipt.taxonomy_version
            or authority.get("l1_projection_sha256") != self._l1_projection_sha256
        ):
            raise StateModelSetError("HMM industry PIT L2 code projection authority is invalid")
        body = {key: value for key, value in authority.items() if key != "canonical_hash"}
        canonical_hash = _require_sha256(authority.get("canonical_hash"), "industry_pit.l2_projection.canonical_hash")
        if digest_named_fields(HMM_L2_CODE_PROJECTION_SCHEMA, body) != canonical_hash:
            raise StateModelSetError("HMM industry PIT L2 code projection hash is invalid")
        source_ids = authority.get("source_ids")
        source_hashes = authority.get("source_hashes")
        if (
            not isinstance(source_ids, list)
            or not source_ids
            or source_ids != sorted({str(value).strip() for value in source_ids if str(value).strip()})
            or not isinstance(source_hashes, list)
            or not source_hashes
            or source_hashes
            != sorted({_require_sha256(value, "industry_pit.l2_projection.source_hash") for value in source_hashes})
        ):
            raise StateModelSetError("HMM industry PIT L2 code projection provenance is invalid")
        raw_rows = authority.get("rows")
        if not isinstance(raw_rows, list) or len(raw_rows) != 131:
            raise StateModelSetError("HMM industry PIT L2 code projection must contain exactly 131 rows")

        observed: dict[str, tuple[str, str]] = {}
        for taxonomy_l1_code, values in self._taxonomy_l2_by_l1.items():
            for taxonomy_l2_code, taxonomy_l2_name in values:
                previous = observed.setdefault(taxonomy_l2_code, (taxonomy_l1_code, taxonomy_l2_name))
                if previous != (taxonomy_l1_code, taxonomy_l2_name):
                    raise StateModelSetError("HMM industry PIT observed taxonomy L2 identity conflicts")

        l2_projection: dict[str, tuple[str, str, str]] = {}
        canonical_l2_codes: set[str] = set()
        constituents: dict[str, dict[str, Any]] = {
            canonical_l1_code: {
                "schema_version": "hmm_risk_l1_pit_l2_constituents_v3",
                "l1_code": canonical_l1_code,
                "l2_codes": [],
                "classification_authority_receipt_hash": self.classification_resolver.receipt.receipt_hash,
                "l1_projection_authority": HMM_L1_CODE_PROJECTION_SCHEMA,
                "l2_projection_authority": HMM_L2_CODE_PROJECTION_SCHEMA,
                "l2_projection_sha256": canonical_hash,
            }
            for canonical_l1_code in self._l1_projection_by_taxonomy.values()
        }
        l2_lookup: dict[tuple[str, str], dict[str, str]] = {}
        expected_row_keys = {
            "taxonomy_l1_code",
            "taxonomy_l1_name",
            "taxonomy_l2_code",
            "taxonomy_l2_name",
            "canonical_l1_code",
            "canonical_l1_name",
            "canonical_l2_code",
            "canonical_l2_name",
            "row_hash",
        }
        for raw in raw_rows:
            if not isinstance(raw, Mapping) or set(raw) != expected_row_keys:
                raise StateModelSetError("HMM industry PIT L2 code projection row is invalid")
            row_body = {key: raw[key] for key in expected_row_keys if key != "row_hash"}
            taxonomy_l1_code = str(raw["taxonomy_l1_code"]).strip()
            taxonomy_l1_name = str(raw["taxonomy_l1_name"]).strip()
            taxonomy_l2_code = str(raw["taxonomy_l2_code"]).strip()
            taxonomy_l2_name = str(raw["taxonomy_l2_name"]).strip()
            canonical_l1_code = str(raw["canonical_l1_code"]).strip()
            canonical_l1_name = str(raw["canonical_l1_name"]).strip()
            canonical_l2_code = str(raw["canonical_l2_code"]).strip()
            canonical_l2_name = str(raw["canonical_l2_name"]).strip()
            if (
                _TAXONOMY_CODE.fullmatch(taxonomy_l1_code) is None
                or _TAXONOMY_CODE.fullmatch(taxonomy_l2_code) is None
                or _CANONICAL_SW_L1_CODE.fullmatch(canonical_l1_code) is None
                or _CANONICAL_SW_L2_CODE.fullmatch(canonical_l2_code) is None
                or self._taxonomy_l1.get(taxonomy_l1_code) != taxonomy_l1_name
                or self._l1_projection_by_taxonomy.get(taxonomy_l1_code) != canonical_l1_code
                or taxonomy_l1_name != canonical_l1_name
                or taxonomy_l2_name != canonical_l2_name
                or taxonomy_l2_code in l2_projection
                or canonical_l2_code in canonical_l2_codes
                or digest_named_fields(HMM_L2_CODE_PROJECTION_ROW_SCHEMA, row_body)
                != _require_sha256(raw.get("row_hash"), "industry_pit.l2_projection.row_hash")
            ):
                raise StateModelSetError("HMM industry PIT L2 code projection row identity is invalid")
            l2_projection[taxonomy_l2_code] = (canonical_l2_code, canonical_l2_name, canonical_l1_code)
            canonical_l2_codes.add(canonical_l2_code)
            constituents[canonical_l1_code]["l2_codes"].append(canonical_l2_code)
            value = {
                "level": "L2",
                "index_code": canonical_l2_code,
                "industry_code": taxonomy_l2_code,
                "name": canonical_l2_name,
            }
            for alias in (canonical_l2_code, taxonomy_l2_code):
                key = ("L2", alias)
                if key in l2_lookup and l2_lookup[key] != value:
                    raise StateModelSetError(f"HMM industry PIT L2 projection alias conflicts: {key}")
                l2_lookup[key] = value
        for taxonomy_l2_code, (taxonomy_l1_code, taxonomy_l2_name) in observed.items():
            projected = l2_projection.get(taxonomy_l2_code)
            if (
                projected is None
                or projected[1] != taxonomy_l2_name
                or projected[2] != self._l1_projection_by_taxonomy[taxonomy_l1_code]
            ):
                raise StateModelSetError("HMM industry PIT observed taxonomy L2 escapes the frozen 131 projection")
        if len(l2_projection) != 131 or len(canonical_l2_codes) != 131:
            raise StateModelSetError("HMM industry PIT L2 code projection does not close the frozen 131-sector catalog")
        if any(not value["l2_codes"] for value in constituents.values()):
            raise StateModelSetError("HMM industry PIT L2 code projection leaves an L1 sector without constituents")
        for value in constituents.values():
            value["l2_codes"] = sorted(value["l2_codes"])
        if self._l2_projection_sha256 is not None:
            if self._l2_projection_sha256 != canonical_hash:
                raise StateModelSetError("HMM industry PIT L2 code projection cannot be rebound")
            return
        self._classification_lookup = {**self._classification_lookup, **l2_lookup}
        self._constituents = constituents
        self._l2_projection_by_taxonomy = l2_projection
        self._l2_projection_sha256 = canonical_hash

    def bind_research_basis_contract(self, authority: Mapping[str, Any]) -> None:
        expected_keys = {
            "schema_version",
            "contract_version",
            "active_mode",
            "historical_classification_basis",
            "historical_non_as_known_taxonomy",
            "forward_classification_basis",
            "forward_non_as_known_taxonomy",
            "canonical_hash",
        }
        if (
            not isinstance(authority, Mapping)
            or set(authority) != expected_keys
            or authority.get("schema_version") != HMM_INDUSTRY_RESEARCH_BASIS_SCHEMA
            or authority.get("contract_version") != HMM_G2A_DATA_A_CONTRACT_VERSION
            or authority.get("historical_classification_basis") != ResearchBasis.STABLE_TAXONOMY_BACKCAST.value
            or authority.get("historical_non_as_known_taxonomy") is not True
            or authority.get("forward_classification_basis") != ResearchBasis.AS_PUBLISHED_PIT.value
            or authority.get("forward_non_as_known_taxonomy") is not False
            or authority.get("active_mode") not in {"historical_replay", "forward"}
        ):
            raise StateModelSetError("HMM industry PIT research-basis contract is invalid")
        body = {key: value for key, value in authority.items() if key != "canonical_hash"}
        canonical_hash = digest_named_fields(HMM_INDUSTRY_RESEARCH_BASIS_SCHEMA, body)
        if canonical_hash != _require_sha256(
            authority.get("canonical_hash"), "industry_pit.research_basis.canonical_hash"
        ):
            raise StateModelSetError("HMM industry PIT research-basis contract hash is invalid")
        if self._research_basis_contract_sha256 is not None:
            if self._research_basis_contract_sha256 != canonical_hash:
                raise StateModelSetError("HMM industry PIT research-basis contract cannot be rebound")
            return
        source_receipt = self.authority_bundle.classification_receipt
        if (
            source_receipt.research_basis is not ResearchBasis.AS_PUBLISHED_PIT
            or source_receipt.knowledge_time_policy is not KnowledgeTimePolicy.CAUSAL_DAILY_NEXT_TRADE
        ):
            raise StateModelSetError("HMM industry PIT source classification authority is not as-published PIT")
        active_mode = str(authority["active_mode"])
        if active_mode == "historical_replay":
            self._activate_stable_taxonomy_backcast()
        else:
            self.classification_resolver = IndustryPitResolver(
                receipt=source_receipt,
                intervals=self.authority_bundle.classification_intervals,
                known_taxonomy_versions={(source_receipt.taxonomy_contract_id, source_receipt.taxonomy_version)},
            )
            self._active_classification_basis = ResearchBasis.AS_PUBLISHED_PIT.value
            self._active_non_as_known_taxonomy = False
            self._stable_backcast_candidate_sha256 = None
        if self._constituents is not None:
            active_receipt_hash = self.classification_resolver.receipt.receipt_hash
            self._constituents = {
                code: {**value, "classification_authority_receipt_hash": active_receipt_hash}
                for code, value in self._constituents.items()
            }
        self._research_basis_contract_sha256 = canonical_hash

    def _activate_stable_taxonomy_backcast(self) -> None:
        source_receipt = self.authority_bundle.classification_receipt
        source_candidate_hash = _require_sha256(
            self.authority_bundle.manifest.get("classification_candidate_hash"),
            "industry_pit.classification_candidate_hash",
        )
        derived_receipt = AuthorityReceipt(
            authority_type=source_receipt.authority_type,
            authority_schema=CLASSIFICATION_CANDIDATE_SCHEMA,
            authority_version="hmm_risk_stable_taxonomy_backcast_v1",
            taxonomy_contract_id=source_receipt.taxonomy_contract_id,
            taxonomy_version=source_receipt.taxonomy_version,
            knowledge_time_policy=KnowledgeTimePolicy.NON_AS_KNOWN_RESEARCH,
            research_basis=ResearchBasis.STABLE_TAXONOMY_BACKCAST,
            source_ids=(*source_receipt.source_ids, "derived:hmm_risk:stable_taxonomy_backcast_v1"),
            source_hashes=(
                *source_receipt.source_hashes,
                source_receipt.receipt_hash,
                source_candidate_hash,
            ),
            frozen_denominator=source_receipt.frozen_denominator,
            denominator_digest=source_receipt.denominator_digest,
        )
        derived_intervals = []
        for row in self.authority_bundle.classification_intervals:
            identity = row.identity
            authority_identity = dict(row.authority_identity)
            unavailable_reason = row.unavailable_reason
            conflicts = row.conflict_candidates
            if (
                identity is None
                and unavailable_reason is UnavailableReason.CLASSIFICATION_KNOWLEDGE_TIME_UNVERIFIED
                and len(conflicts) == 1
            ):
                conflict = conflicts[0]
                raw_identity = conflict.get("identity") if isinstance(conflict, Mapping) else None
                raw_authority = conflict.get("authority_identity") if isinstance(conflict, Mapping) else None
                if not isinstance(raw_identity, Mapping) or not isinstance(raw_authority, Mapping):
                    raise StateModelSetError("HMM stable taxonomy backcast conflict identity is malformed")
                try:
                    identity = TaxonomyIdentity(
                        l1_code=str(raw_identity["l1_code"]),
                        l1_name=str(raw_identity["l1_name"]),
                        l2_code=str(raw_identity["l2_code"]),
                        l2_name=str(raw_identity["l2_name"]),
                        l3_code=str(raw_identity["l3_code"]),
                        l3_name=str(raw_identity["l3_name"]),
                    )
                except (KeyError, TypeError, ValueError) as exc:
                    raise StateModelSetError("HMM stable taxonomy backcast identity is invalid") from exc
                if conflict.get("identity_hash") != identity.identity_hash:
                    raise StateModelSetError("HMM stable taxonomy backcast identity hash differs")
                authority_identity = {str(key): str(value) for key, value in raw_authority.items()}
                expected_authority = {
                    "classification_l1_code": identity.l1_code,
                    "classification_l2_code": identity.l2_code,
                    "classification_l3_code": identity.l3_code,
                }
                if authority_identity != expected_authority:
                    raise StateModelSetError("HMM stable taxonomy backcast authority identity differs")
                conflict_source_ids = _require_nonempty_provenance_sequence(
                    conflict.get("source_ids"),
                    "HMM stable taxonomy backcast conflict provenance source_ids",
                )
                conflict_source_hashes = _require_nonempty_provenance_sequence(
                    conflict.get("source_hashes"),
                    "HMM stable taxonomy backcast conflict provenance source_hashes",
                    sha256=True,
                )
                if (
                    conflict.get("industry_code") != identity.l3_code
                    or conflict.get("lineage_hash") not in row.lineage_hashes
                    or not set(conflict_source_ids).issubset(row.source_ids)
                    or not set(conflict_source_hashes).issubset(row.source_hashes)
                ):
                    raise StateModelSetError("HMM stable taxonomy backcast conflict provenance differs")
                unavailable_reason = None
                conflicts = ()
            derived_intervals.append(
                make_candidate_interval(
                    canonical_symbol=row.canonical_symbol,
                    authority_type=row.authority_type,
                    taxonomy_contract_id=row.taxonomy_contract_id,
                    taxonomy_version=row.taxonomy_version,
                    authority_receipt_hash=derived_receipt.receipt_hash,
                    valid_from=row.valid_from,
                    valid_to_exclusive=row.valid_to_exclusive,
                    eligible_from=row.eligible_from,
                    eligible_to_exclusive=row.eligible_to_exclusive,
                    causal_use_from=row.valid_from,
                    causal_use_to_exclusive=row.valid_to_exclusive,
                    known_from=row.known_from,
                    source_effective_field=row.source_effective_field,
                    source_last_updated_at=row.source_last_updated_at,
                    research_basis=ResearchBasis.STABLE_TAXONOMY_BACKCAST,
                    non_as_known_taxonomy=True,
                    identity=identity,
                    authority_identity=authority_identity if identity is not None else {},
                    unavailable_reason=unavailable_reason,
                    conflict_candidates=conflicts if identity is None else (),
                    source_ids=(*row.source_ids, "derived:hmm_risk:stable_taxonomy_backcast_v1"),
                    source_hashes=(*row.source_hashes, row.row_hash, source_candidate_hash),
                    lineage_hashes=(*row.lineage_hashes, row.row_hash),
                )
            )
        candidate_body = {
            "contract_version": HMM_G2A_DATA_A_CONTRACT_VERSION,
            "source_candidate_hash": source_candidate_hash,
            "source_receipt_hash": source_receipt.receipt_hash,
            "derived_receipt_hash": derived_receipt.receipt_hash,
            "row_hashes": sorted(row.row_hash for row in derived_intervals),
        }
        self.classification_resolver = IndustryPitResolver(
            receipt=derived_receipt,
            intervals=tuple(derived_intervals),
            known_taxonomy_versions={(derived_receipt.taxonomy_contract_id, derived_receipt.taxonomy_version)},
        )
        self._active_classification_basis = ResearchBasis.STABLE_TAXONOMY_BACKCAST.value
        self._active_non_as_known_taxonomy = True
        self._stable_backcast_candidate_sha256 = digest_named_fields(
            HMM_STABLE_BACKCAST_CANDIDATE_SCHEMA, candidate_body
        )

    @property
    def classification_lookup(self) -> dict[tuple[str, str], dict[str, str]]:
        if self._classification_lookup is None:
            raise StateModelSetError("HMM industry PIT L1 code projection has not been bound")
        return self._classification_lookup

    @property
    def constituents(self) -> dict[str, dict[str, Any]]:
        if self._constituents is None:
            raise StateModelSetError("HMM industry PIT L2 code projection has not been bound")
        return self._constituents

    def resolve(self, symbol: str, trade_date: date) -> HMMIndustryProjection:
        if self._research_basis_contract_sha256 is None:
            raise StateModelSetError("HMM industry PIT research-basis contract has not been bound")
        if self._classification_lookup is None or self._l2_projection_by_taxonomy is None:
            raise StateModelSetError("HMM industry PIT L1/L2 code projections have not been bound")
        dual = resolve_dual_authority(
            classification_resolver=self.classification_resolver,
            index_membership_resolver=self.index_membership_resolver,
            classification_request=_request(self.classification_resolver, symbol=symbol, trade_date=trade_date),
            index_membership_request=_request(self.index_membership_resolver, symbol=symbol, trade_date=trade_date),
        )
        classification = dual.classification
        index_membership = dual.index_membership
        classification_hashes = (
            classification.row_hashes if isinstance(classification, ResolvedIndustryIdentity) else ()
        )
        index_hashes = index_membership.row_hashes if isinstance(index_membership, ResolvedIndustryIdentity) else ()
        if isinstance(classification, UnavailableIndustryIdentity):
            reason = f"classification:{classification.reason.value}"
        else:
            l1 = self._classification_lookup.get(("L1", classification.identity.l1_code))
            l2 = self._l2_projection_by_taxonomy.get(classification.identity.l2_code)
            if l1 is None:
                raise StateModelSetError("HMM industry PIT resolved identity escapes the 31 L1 projection")
            if l2 is None or l2[2] != l1["index_code"]:
                raise StateModelSetError("HMM industry PIT resolved identity escapes the 131 L2 projection")
            return HMMIndustryProjection(
                status="resolved",
                canonical_symbol=symbol,
                trade_date=trade_date,
                l1_code=str(l1["index_code"]),
                l1_name=str(l1["name"]),
                l2_code=l2[0],
                l2_name=l2[1],
                reason_code=None,
                classification_receipt_hash=classification.authority_receipt_hash,
                index_membership_receipt_hash=index_membership.authority_receipt_hash,
                classification_row_hashes=classification_hashes,
                index_membership_row_hashes=index_hashes,
                alignment_state=dual.alignment_state.value,
                classification_research_basis=self._active_classification_basis,
                non_as_known_taxonomy=self._active_non_as_known_taxonomy,
            )
        return HMMIndustryProjection(
            status="unavailable",
            canonical_symbol=symbol,
            trade_date=trade_date,
            l1_code=None,
            l1_name=None,
            l2_code=None,
            l2_name=None,
            reason_code=reason,
            classification_receipt_hash=self.classification_resolver.receipt.receipt_hash,
            index_membership_receipt_hash=self.index_membership_resolver.receipt.receipt_hash,
            classification_row_hashes=classification_hashes,
            index_membership_row_hashes=index_hashes,
            alignment_state=dual.alignment_state.value,
            classification_research_basis=self._active_classification_basis,
            non_as_known_taxonomy=self._active_non_as_known_taxonomy,
        )

    def mapping_manifest(self, *, universe_key: str, source_start: date, source_end: date) -> Mapping[str, Any]:
        if (
            self._constituents is None
            or self._l1_projection_sha256 is None
            or self._l2_projection_sha256 is None
            or self._l2_projection_by_taxonomy is None
        ):
            raise StateModelSetError("HMM industry PIT L1/L2 code projections have not been bound")
        if self._research_basis_contract_sha256 is None:
            raise StateModelSetError("HMM industry PIT research-basis contract has not been bound")
        constituents_hash = hashlib.sha256(canonical_json_bytes(self._constituents)).hexdigest()
        return {
            "schema_version": HMM_MAPPING_MANIFEST_SCHEMA,
            "universe_key": universe_key,
            "source_window_start": source_start.isoformat(),
            "source_window_end": source_end.isoformat(),
            "canonical_l1_count": 31,
            "canonical_l2_count": len(self._l2_projection_by_taxonomy),
            "source_classification_authority_receipt_hash": self._source_classification_receipt_hash,
            "classification_authority_receipt_hash": self.classification_resolver.receipt.receipt_hash,
            "index_membership_authority_receipt_hash": self.index_membership_resolver.receipt.receipt_hash,
            "classification_candidate_hash": self.authority_bundle.manifest["classification_candidate_hash"],
            "stable_backcast_candidate_sha256": self._stable_backcast_candidate_sha256,
            "index_membership_candidate_hash": self.authority_bundle.manifest["index_membership_candidate_hash"],
            "candidate_bundle_hash": self.authority_bundle.manifest["bundle_hash"],
            "candidate_preflight_canonical_hash": self.authority_bundle.preflight_report["canonical_hash"],
            "research_basis_contract_sha256": self._research_basis_contract_sha256,
            "active_classification_basis": self._active_classification_basis,
            "non_as_known_taxonomy": self._active_non_as_known_taxonomy,
            "l1_code_projection_sha256": self._l1_projection_sha256,
            "l2_code_projection_sha256": self._l2_projection_sha256,
            "constituent_manifest_hash": constituents_hash,
        }

    def preflight(
        self,
        denominator: FrozenDenominator,
        *,
        expected_trading_days: int = EXPECTED_PREFLIGHT_TRADING_DAYS,
    ) -> Mapping[str, Any]:
        if self._research_basis_contract_sha256 is None:
            raise StateModelSetError("HMM industry PIT research-basis contract has not been bound")
        if self._l1_projection_sha256 is None:
            raise StateModelSetError("HMM industry PIT L1 code projection has not been bound")
        if self._l2_projection_sha256 is None:
            raise StateModelSetError("HMM industry PIT L2 code projection has not been bound")
        if len(denominator.trading_dates) != expected_trading_days:
            raise StateModelSetError(
                "HMM industry PIT preflight trading-day count differs from the approved contract: "
                f"expected={expected_trading_days} actual={len(denominator.trading_dates)}"
            )
        status_counts: Counter[str] = Counter()
        index_status_counts: Counter[str] = Counter()
        alignment_counts: Counter[str] = Counter()
        reason_counts: Counter[str] = Counter()
        reason_by_sector: dict[str, Counter[str]] = defaultdict(Counter)
        reason_date_deltas: dict[str, list[int]] = {}
        total = 0
        global_dates = denominator.trading_dates
        for span in denominator.universe_spans:
            span_dates = denominator.dates_for_span(span)
            if not span_dates:
                continue
            global_start = bisect_left(global_dates, span_dates[0])
            boundaries = {0, len(span_dates)}
            for transition in {
                *self.classification_resolver.transition_dates(span.canonical_symbol),
                *self.index_membership_resolver.transition_dates(span.canonical_symbol),
            }:
                offset = bisect_left(span_dates, transition)
                if 0 < offset < len(span_dates):
                    boundaries.add(offset)
            ordered = sorted(boundaries)
            for left, right in zip(ordered, ordered[1:]):
                count = right - left
                trade_date = span_dates[left]
                dual = resolve_dual_authority(
                    classification_resolver=self.classification_resolver,
                    index_membership_resolver=self.index_membership_resolver,
                    classification_request=_request(
                        self.classification_resolver,
                        symbol=span.canonical_symbol,
                        trade_date=trade_date,
                    ),
                    index_membership_request=_request(
                        self.index_membership_resolver,
                        symbol=span.canonical_symbol,
                        trade_date=trade_date,
                    ),
                )
                classification = dual.classification
                total += count
                status_counts[classification.status] += count
                index_status_counts[dual.index_membership.status] += count
                alignment_counts[dual.alignment_state.value] += count
                if isinstance(classification, UnavailableIndustryIdentity):
                    reason = f"classification:{classification.reason.value}"
                    reason_counts[reason] += count
                    sector = "unavailable"
                    reason_by_sector[sector][reason] += count
                    deltas = reason_date_deltas.setdefault(reason, [0] * (len(global_dates) + 1))
                    deltas[global_start + left] += 1
                    deltas[global_start + right] -= 1
        if total != denominator.total_opportunities or sum(status_counts.values()) != total:
            raise StateModelSetError(
                "HMM industry PIT preflight denominator closure failed: "
                f"observed={total} expected={denominator.total_opportunities}"
            )
        reason_by_date: dict[str, dict[str, int]] = defaultdict(dict)
        for reason, deltas in sorted(reason_date_deltas.items()):
            active = 0
            for index, trade_date in enumerate(global_dates):
                active += deltas[index]
                if active:
                    reason_by_date[trade_date.isoformat()][reason] = active
        payload = {
            "schema_version": HMM_INDUSTRY_PIT_PREFLIGHT_SCHEMA,
            "window_start": denominator.window_start.isoformat(),
            "window_end": denominator.window_end.isoformat(),
            "trading_day_count": len(global_dates),
            "denominator_digest": denominator.digest,
            "total_opportunities": total,
            "resolved": status_counts["resolved"],
            "unavailable": status_counts["unavailable"],
            "coverage_ratio": format(Decimal(status_counts["resolved"]) / Decimal(total), ".12f"),
            "unavailable_by_reason": dict(sorted(reason_counts.items())),
            "unavailable_by_date": {
                day: dict(sorted(values.items())) for day, values in sorted(reason_by_date.items())
            },
            "unavailable_by_sector": {
                sector: dict(sorted(values.items())) for sector, values in sorted(reason_by_sector.items())
            },
            "index_membership_diagnostic": dict(sorted(index_status_counts.items())),
            "authority_alignment_diagnostic": dict(sorted(alignment_counts.items())),
            "l1_code_projection_status": (
                "bound" if self._l1_projection_sha256 is not None else "unavailable_pending_versioned_crosswalk"
            ),
            "l1_code_projection_sha256": self._l1_projection_sha256,
            "l2_code_projection_status": "bound",
            "l2_code_projection_sha256": self._l2_projection_sha256,
            "candidate_bundle_hash": self.authority_bundle.manifest["bundle_hash"],
            "source_classification_authority_receipt_hash": self._source_classification_receipt_hash,
            "classification_authority_receipt_hash": self.classification_resolver.receipt.receipt_hash,
            "index_membership_authority_receipt_hash": self.index_membership_resolver.receipt.receipt_hash,
            "research_basis_contract_sha256": self._research_basis_contract_sha256,
            "active_classification_basis": self._active_classification_basis,
            "non_as_known_taxonomy": self._active_non_as_known_taxonomy,
            "stable_backcast_candidate_sha256": self._stable_backcast_candidate_sha256,
            "closure": {
                "resolved_plus_unavailable": status_counts["resolved"] + status_counts["unavailable"],
                "expected_denominator": total,
                "passed": True,
            },
            "fit_count": 0,
            "selection_performed": False,
            "d5_performed": False,
            "d6_performed": False,
            "model_or_ready_written": False,
        }
        return {**payload, "canonical_hash": digest_named_fields(HMM_INDUSTRY_PIT_PREFLIGHT_SCHEMA, payload)}


__all__ = [
    "EXPECTED_PREFLIGHT_TRADING_DAYS",
    "HMM_INDUSTRY_PIT_AUTHORITY_SCHEMA",
    "HMM_INDUSTRY_PIT_PREFLIGHT_SCHEMA",
    "HMM_INDUSTRY_RESEARCH_BASIS_SCHEMA",
    "HMM_G2A_DATA_A_CONTRACT_VERSION",
    "HMM_L1_CODE_PROJECTION_SCHEMA",
    "HMM_L1_CODE_PROJECTION_VERSION",
    "HMM_L1_CODE_PROJECTION_ROW_SCHEMA",
    "HMM_L2_CODE_PROJECTION_SCHEMA",
    "HMM_L2_CODE_PROJECTION_VERSION",
    "HMM_L2_CODE_PROJECTION_ROW_SCHEMA",
    "HMM_MAPPING_MANIFEST_SCHEMA",
    "HMM_STABLE_BACKCAST_CANDIDATE_SCHEMA",
    "HMMIndustryPitAdapter",
    "HMMIndustryProjection",
    "IndustryPitContractError",
    "build_l1_code_projection_authority",
    "build_l2_code_projection_authority",
]
