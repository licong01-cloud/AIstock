"""Build stock-level Shenwan sector facts and C-013 candidate artifacts.

The legacy :class:`SectorDataBuilder` remains the explicitly activated
production materializer.  :class:`SectorDataCandidateBuilder` is the P3A
candidate-only path: it consumes the immutable dual-authority bundle, performs
no database writes, retains every frozen symbol/day opportunity, and writes a
repo-external normalized candidate for downstream adapters.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging
import os
import re
import shutil
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, List, Mapping, Sequence

from ..db.pg_pool import get_conn
from .dataset_release.canonical import canonical_json_bytes, digest_named_fields
from .industry_pit.artifact_store import (
    CandidateBundleReadback,
    read_candidate_bundle,
    require_repo_external_root,
)
from .industry_pit.contracts import (
    AlignmentState,
    AuthorityType,
    ResolutionRequest,
    ResolvedIndustryIdentity,
    UnavailableIndustryIdentity,
)
from .industry_pit.resolver import IndustryPitResolver, resolve_dual_authority
from .stock_universe_pit_service import (
    DEFAULT_ST_PIT_UNIVERSE_KEY,
    IMMUTABLE_QE_ST_PIT_UNIVERSE_PREFIX,
)


logger = logging.getLogger(__name__)


class SectorDataBuildContractError(RuntimeError):
    """Raised when sector-data source or candidate contracts are ambiguous."""


SECTOR_DATA_ASSIGNMENT_SCHEMA = "sector_data_dual_authority_assignment_v1"
SECTOR_DATA_FACT_SCHEMA = "sector_data_dual_authority_fact_v1"
SECTOR_DATA_REPORT_SCHEMA = "sector_data_dual_authority_report_v1"
SECTOR_DATA_MANIFEST_SCHEMA = "sector_data_dual_authority_candidate_v1"
SECTOR_DATA_OPPORTUNITY_SCHEMA = "sector_data_dual_authority_opportunity_v1"

_GIT_OBJECT_RE = re.compile(r"^[0-9a-f]{40,64}$")
_INDEX_CODE_RE = re.compile(r"^(?P<code>[0-9]{6})(?:[.]SI)?$")

SW_DAILY_FIELDS = (
    "open",
    "high",
    "low",
    "close",
    "pct_change",
    "vol",
    "amount",
    "pe",
    "pb",
    "total_mv",
)
MONEYFLOW_FIELDS = (
    "buy_sm_amount",
    "sell_sm_amount",
    "buy_md_amount",
    "sell_md_amount",
    "buy_lg_amount",
    "sell_lg_amount",
    "buy_elg_amount",
    "sell_elg_amount",
    "net_mf_amount",
    "buy_elg_vol",
    "sell_elg_vol",
    "net_mf_vol",
)


@dataclass(frozen=True, slots=True)
class SectorDataSourceDay:
    """One bounded source partition for the candidate-only builder."""

    trade_date: dt.date
    symbols: tuple[str, ...]
    sw_daily_by_index_l2: Mapping[str, Mapping[str, Any]]
    moneyflow_by_symbol: Mapping[str, Mapping[str, Any]]


@dataclass(frozen=True, slots=True)
class SectorDataCandidateDay:
    trade_date: dt.date
    assignments: tuple[Mapping[str, Any], ...]
    sector_facts: tuple[Mapping[str, Any], ...]


@dataclass(frozen=True, slots=True)
class SectorDataCandidateReadback:
    artifact_root: Path
    manifest: Mapping[str, Any]
    report: Mapping[str, Any]
    assignment_rows: int
    sector_fact_rows: int


def _decimal_text(value: Any, *, field: str) -> str:
    if value is None or isinstance(value, bool):
        raise SectorDataBuildContractError(f"{field} must be a finite numeric value")
    try:
        number = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise SectorDataBuildContractError(f"{field} must be a finite numeric value") from exc
    if not number.is_finite():
        raise SectorDataBuildContractError(f"{field} must be a finite numeric value")
    if number == 0:
        return "0"
    text = format(number.normalize(), "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def _canonical_symbol(value: str) -> str:
    symbol = str(value or "").strip().upper()
    if not re.fullmatch(r"[0-9]{6}[.](?:SH|SZ)", symbol):
        raise SectorDataBuildContractError(f"invalid canonical symbol: {value!r}")
    return symbol


def _canonical_index_code(value: str) -> str:
    match = _INDEX_CODE_RE.fullmatch(str(value or "").strip().upper())
    if match is None:
        raise SectorDataBuildContractError(f"invalid Shenwan index code: {value!r}")
    return match.group("code")


def _numeric_record(
    value: Mapping[str, Any],
    *,
    fields: Sequence[str],
    source: str,
) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise SectorDataBuildContractError(f"{source} row must be a mapping")
    missing = [field for field in fields if field not in value]
    if missing:
        raise SectorDataBuildContractError(f"{source} row is missing fields: {missing}")
    return {
        field: _decimal_text(value[field], field=f"{source}.{field}")
        for field in fields
    }


def _normalized_numeric_rows(
    values: Mapping[str, Mapping[str, Any]],
    *,
    key_normalizer,
    fields: Sequence[str],
    source: str,
    allowed_keys: frozenset[str] | None = None,
) -> tuple[dict[str, dict[str, str]], frozenset[str]]:
    output: dict[str, dict[str, str]] = {}
    invalid: set[str] = set()
    for raw_key in sorted(values, key=lambda value: str(value)):
        key = key_normalizer(raw_key)
        if allowed_keys is not None and key not in allowed_keys:
            continue
        try:
            record = _numeric_record(values[raw_key], fields=fields, source=f"{source}[{raw_key}]")
        except SectorDataBuildContractError:
            if key in output:
                raise SectorDataBuildContractError(
                    f"{source} contains conflicting valid/invalid rows after normalization: {key}"
                )
            invalid.add(key)
            continue
        if key in invalid:
            raise SectorDataBuildContractError(
                f"{source} contains conflicting valid/invalid rows after normalization: {key}"
            )
        previous = output.get(key)
        if previous is not None and previous != record:
            raise SectorDataBuildContractError(
                f"{source} contains conflicting rows after identity normalization: {key}"
            )
        output[key] = record
    return output, frozenset(invalid)


def _row_with_hash(schema: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    row = dict(payload)
    row["row_hash"] = digest_named_fields(schema, payload)
    return row


def _resolution_projection(value: ResolvedIndustryIdentity | UnavailableIndustryIdentity) -> Mapping[str, Any]:
    if isinstance(value, UnavailableIndustryIdentity):
        return {
            "status": "unavailable",
            "reason": value.reason.value,
            "authority_receipt_hash": value.authority_receipt_hash,
            "conflict_candidates": [dict(item) for item in value.conflict_candidates],
        }
    return {
        "status": "resolved",
        "identity_codes": {
            "l1_code": value.identity.l1_code,
            "l2_code": value.identity.l2_code,
            "l3_code": value.identity.l3_code,
        },
        "identity_hash": value.identity.identity_hash,
        "authority_identity": dict(value.authority_identity),
        "valid_from": value.valid_from.isoformat(),
        "valid_to_exclusive": value.valid_to_exclusive.isoformat() if value.valid_to_exclusive else None,
        "known_from": value.known_from.isoformat() if value.known_from else None,
        "taxonomy_contract_id": value.taxonomy_contract_id,
        "taxonomy_version": value.taxonomy_version,
        "candidate_row_hashes": list(value.row_hashes),
        "authority_receipt_hash": value.authority_receipt_hash,
        "non_as_known_taxonomy": value.non_as_known_taxonomy,
        "resolution_hash": digest_named_fields(
            "sector_data_authority_resolution_ref_v1",
            value.as_dict(),
        ),
    }


def _l2_authority_identity_hash(
    value: ResolvedIndustryIdentity,
    *,
    authority_type: AuthorityType,
) -> str:
    prefix = "classification" if authority_type is AuthorityType.CLASSIFICATION else "index"
    if value.authority_type is not authority_type:
        raise SectorDataBuildContractError("sector L2 identity authority type is inconsistent")
    return digest_named_fields(
        "sector_data_l2_authority_identity_v1",
        {
            "authority_type": authority_type.value,
            "taxonomy_contract_id": value.taxonomy_contract_id,
            "taxonomy_version": value.taxonomy_version,
            "taxonomy_l1_code": value.identity.l1_code,
            "taxonomy_l1_name": value.identity.l1_name,
            "taxonomy_l2_code": value.identity.l2_code,
            "taxonomy_l2_name": value.identity.l2_name,
            "authority_l1_code": value.authority_identity[f"{prefix}_l1_code"],
            "authority_l2_code": value.authority_identity[f"{prefix}_l2_code"],
        },
    )


class SectorDataCandidateBuilder:
    """Build normalized sector assignments/facts without database mutation."""

    def __init__(self, *, authority_bundle: CandidateBundleReadback) -> None:
        classification_receipt = authority_bundle.classification_receipt
        index_receipt = authority_bundle.index_membership_receipt
        if classification_receipt.denominator_digest != index_receipt.denominator_digest:
            raise SectorDataBuildContractError("dual authority denominator digests differ")
        if classification_receipt.frozen_denominator != index_receipt.frozen_denominator:
            raise SectorDataBuildContractError("dual authority denominator counts differ")
        known = {
            (
                classification_receipt.taxonomy_contract_id,
                classification_receipt.taxonomy_version,
            ),
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

    @classmethod
    def from_artifact_root(
        cls,
        *,
        artifact_root: Path,
        forbidden_roots: Sequence[Path],
    ) -> "SectorDataCandidateBuilder":
        return cls(
            authority_bundle=read_candidate_bundle(
                artifact_root=artifact_root,
                forbidden_roots=forbidden_roots,
            )
        )

    def _resolve(self, symbol: str, trade_date: dt.date):
        classification_receipt = self.classification_resolver.receipt
        index_receipt = self.index_membership_resolver.receipt
        return resolve_dual_authority(
            classification_resolver=self.classification_resolver,
            index_membership_resolver=self.index_membership_resolver,
            classification_request=ResolutionRequest(
                canonical_symbol=symbol,
                trade_date=trade_date,
                authority_type=AuthorityType.CLASSIFICATION,
                taxonomy_contract_id=classification_receipt.taxonomy_contract_id,
                taxonomy_version=classification_receipt.taxonomy_version,
                authority_receipt_hash=classification_receipt.receipt_hash,
                knowledge_time_policy=classification_receipt.knowledge_time_policy,
                research_basis=classification_receipt.research_basis,
            ),
            index_membership_request=ResolutionRequest(
                canonical_symbol=symbol,
                trade_date=trade_date,
                authority_type=AuthorityType.INDEX_MEMBERSHIP,
                taxonomy_contract_id=index_receipt.taxonomy_contract_id,
                taxonomy_version=index_receipt.taxonomy_version,
                authority_receipt_hash=index_receipt.receipt_hash,
                knowledge_time_policy=index_receipt.knowledge_time_policy,
                research_basis=index_receipt.research_basis,
            ),
        )

    def build_day(self, source: SectorDataSourceDay) -> SectorDataCandidateDay:
        if not isinstance(source.trade_date, dt.date) or isinstance(source.trade_date, dt.datetime):
            raise SectorDataBuildContractError("trade_date must be a date")
        symbols = tuple(sorted(_canonical_symbol(value) for value in source.symbols))
        if not symbols or len(symbols) != len(set(symbols)):
            raise SectorDataBuildContractError("source day symbols must be non-empty and unique")
        resolutions = {symbol: self._resolve(symbol, source.trade_date) for symbol in symbols}
        required_index_codes = frozenset(
            _canonical_index_code(result.index_membership.authority_identity["index_l2_code"])
            for result in resolutions.values()
            if isinstance(result.index_membership, ResolvedIndustryIdentity)
        )
        moneyflow, invalid_moneyflow = _normalized_numeric_rows(
            source.moneyflow_by_symbol,
            key_normalizer=_canonical_symbol,
            fields=MONEYFLOW_FIELDS,
            source="moneyflow",
            allowed_keys=frozenset(symbols),
        )
        sw_daily, invalid_sw_daily = _normalized_numeric_rows(
            source.sw_daily_by_index_l2,
            key_normalizer=_canonical_index_code,
            fields=SW_DAILY_FIELDS,
            source="sw_daily",
            allowed_keys=required_index_codes,
        )

        expected_by_l2: Counter[str] = Counter()
        resolved_by_l2: Counter[str] = Counter()
        aggregate: dict[str, dict[str, Decimal]] = defaultdict(
            lambda: {field: Decimal("0") for field in MONEYFLOW_FIELDS}
        )
        for symbol in symbols:
            classification = resolutions[symbol].classification
            if not isinstance(classification, ResolvedIndustryIdentity):
                continue
            l2_code = classification.identity.l2_code
            expected_by_l2[l2_code] += 1
            record = moneyflow.get(symbol)
            if record is None:
                continue
            resolved_by_l2[l2_code] += 1
            for field in MONEYFLOW_FIELDS:
                aggregate[l2_code][field] += Decimal(record[field])

        fact_rows_by_key: dict[tuple[str, str], Mapping[str, Any]] = {}
        for symbol in symbols:
            resolution = resolutions[symbol]
            classification = resolution.classification
            index_membership = resolution.index_membership
            if (
                resolution.alignment_state is not AlignmentState.ALIGNED
                or not isinstance(classification, ResolvedIndustryIdentity)
                or not isinstance(index_membership, ResolvedIndustryIdentity)
            ):
                continue
            classification_l2 = classification.identity.l2_code
            index_l2 = _canonical_index_code(index_membership.authority_identity["index_l2_code"])
            key = (classification_l2, index_l2)
            classification_l2_hash = _l2_authority_identity_hash(
                classification,
                authority_type=AuthorityType.CLASSIFICATION,
            )
            index_l2_hash = _l2_authority_identity_hash(
                index_membership,
                authority_type=AuthorityType.INDEX_MEMBERSHIP,
            )
            if key in fact_rows_by_key:
                existing = fact_rows_by_key[key]
                if (
                    existing["classification_l2_identity_hash"] != classification_l2_hash
                    or existing["index_l2_identity_hash"] != index_l2_hash
                ):
                    raise SectorDataBuildContractError(
                        "sector fact L2 key maps to conflicting authority identities"
                    )
                continue
            published = sw_daily.get(index_l2)
            contributor_count = resolved_by_l2[classification_l2]
            if published is None or contributor_count == 0:
                continue
            expected_count = expected_by_l2[classification_l2]
            coverage = Decimal(contributor_count) / Decimal(expected_count)
            payload = {
                "schema_version": SECTOR_DATA_FACT_SCHEMA,
                "trade_date": source.trade_date.isoformat(),
                "classification_l2_code": classification_l2,
                "classification_l2_identity_hash": classification_l2_hash,
                "index_l2_code": index_l2,
                "index_l2_identity_hash": index_l2_hash,
                "classification_authority_receipt_hash": classification.authority_receipt_hash,
                "index_membership_authority_receipt_hash": index_membership.authority_receipt_hash,
                "sw_daily": published,
                "moneyflow_aggregate": {
                    field: _decimal_text(value, field=f"aggregate.{field}")
                    for field, value in aggregate[classification_l2].items()
                },
                "contributor_coverage": {
                    "expected": expected_count,
                    "resolved": contributor_count,
                    "ratio": _decimal_text(coverage, field="contributor_coverage.ratio"),
                },
            }
            fact_rows_by_key[key] = _row_with_hash(SECTOR_DATA_FACT_SCHEMA, payload)

        assignments: list[Mapping[str, Any]] = []
        for symbol in symbols:
            resolution = resolutions[symbol]
            classification = resolution.classification
            index_membership = resolution.index_membership
            reasons: list[str] = []
            if isinstance(classification, UnavailableIndustryIdentity):
                reasons.append(f"classification:{classification.reason.value}")
            if isinstance(index_membership, UnavailableIndustryIdentity):
                reasons.append(f"index_membership:{index_membership.reason.value}")
            if resolution.alignment_state is AlignmentState.UNALIGNED:
                reasons.append("authority_unaligned")
            fact_row = None
            if isinstance(classification, ResolvedIndustryIdentity):
                if symbol in invalid_moneyflow:
                    reasons.append("contributor_moneyflow_invalid")
                elif symbol not in moneyflow:
                    reasons.append("contributor_moneyflow_unavailable")
                if isinstance(index_membership, ResolvedIndustryIdentity):
                    index_l2 = _canonical_index_code(index_membership.authority_identity["index_l2_code"])
                    if index_l2 in invalid_sw_daily:
                        reasons.append("published_sector_fact_invalid")
                    elif index_l2 not in sw_daily:
                        reasons.append("published_sector_fact_unavailable")
                    if resolution.alignment_state is AlignmentState.ALIGNED:
                        fact_row = fact_rows_by_key.get((classification.identity.l2_code, index_l2))
                        if fact_row is None and index_l2 in sw_daily:
                            reasons.append("sector_moneyflow_aggregate_unavailable")
            reasons = sorted(set(reasons))
            status = (
                "unaligned"
                if resolution.alignment_state is AlignmentState.UNALIGNED
                else ("unavailable" if reasons else "resolved")
            )
            payload = {
                "schema_version": SECTOR_DATA_ASSIGNMENT_SCHEMA,
                "canonical_symbol": symbol,
                "trade_date": source.trade_date.isoformat(),
                "status": status,
                "alignment_state": resolution.alignment_state.value,
                "classification": _resolution_projection(classification),
                "index_membership": _resolution_projection(index_membership),
                "sector_fact_row_hash": fact_row["row_hash"] if status == "resolved" and fact_row else None,
                "unavailable_reasons": reasons,
            }
            assignments.append(_row_with_hash(SECTOR_DATA_ASSIGNMENT_SCHEMA, payload))
        return SectorDataCandidateDay(
            trade_date=source.trade_date,
            assignments=tuple(assignments),
            sector_facts=tuple(
                fact_rows_by_key[key] for key in sorted(fact_rows_by_key)
            ),
        )


def _write_json_file(path: Path, payload: Mapping[str, Any]) -> tuple[str, int]:
    encoded = canonical_json_bytes(payload) + b"\n"
    path.write_bytes(encoded)
    return hashlib.sha256(encoded).hexdigest(), len(encoded)


def _file_observation(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def write_sector_data_candidate(
    *,
    artifact_root: Path,
    forbidden_roots: Sequence[Path],
    authority_bundle: CandidateBundleReadback,
    days: Iterable[SectorDataCandidateDay],
    expected_opportunities: int,
    expected_opportunity_digest: str,
    candidate_scope: str,
    producer_commit: str,
    producer_tree: str,
) -> SectorDataCandidateReadback:
    if candidate_scope not in {"full", "sample"}:
        raise SectorDataBuildContractError("candidate_scope must be full or sample")
    if type(expected_opportunities) is not int or expected_opportunities <= 0:
        raise SectorDataBuildContractError("expected_opportunities must be positive")
    if not re.fullmatch(r"[0-9a-f]{64}", str(expected_opportunity_digest or "")):
        raise SectorDataBuildContractError("expected_opportunity_digest must be a SHA-256 digest")
    if not _GIT_OBJECT_RE.fullmatch(producer_commit) or not _GIT_OBJECT_RE.fullmatch(producer_tree):
        raise SectorDataBuildContractError("producer commit/tree identity is invalid")
    classification_receipt = authority_bundle.classification_receipt
    index_receipt = authority_bundle.index_membership_receipt
    if classification_receipt.denominator_digest != index_receipt.denominator_digest:
        raise SectorDataBuildContractError("dual authority denominator digests differ")
    if candidate_scope == "full" and expected_opportunities != classification_receipt.frozen_denominator:
        raise SectorDataBuildContractError("full candidate opportunity count differs from authority denominator")
    target = require_repo_external_root(artifact_root, forbidden_roots=forbidden_roots)
    if target.exists():
        raise SectorDataBuildContractError(f"refusing to overwrite candidate artifact root: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.parent / f".{target.name}.tmp-{uuid.uuid4().hex}"
    temporary.mkdir(parents=False, exist_ok=False)
    assignment_count = 0
    fact_count = 0
    status_counts: Counter[str] = Counter()
    alignment_counts: Counter[str] = Counter()
    reason_counts: Counter[str] = Counter()
    reason_by_date: dict[str, Counter[str]] = defaultdict(Counter)
    reason_by_sector: dict[str, Counter[str]] = defaultdict(Counter)
    previous_day: dt.date | None = None
    previous_assignment_key: tuple[str, str] | None = None
    previous_fact_key: tuple[str, str, str] | None = None
    assignment_digest = hashlib.sha256()
    fact_digest = hashlib.sha256()
    opportunity_digest = hashlib.sha256()
    try:
        assignments_path = temporary / "assignments.jsonl"
        facts_path = temporary / "sector_facts.jsonl"
        with assignments_path.open("wb") as assignment_handle, facts_path.open("wb") as fact_handle:
            for day in days:
                if previous_day is not None and day.trade_date <= previous_day:
                    raise SectorDataBuildContractError("candidate days must be strictly increasing")
                previous_day = day.trade_date
                fact_hashes = {str(row.get("row_hash")) for row in day.sector_facts}
                for row in day.sector_facts:
                    key = (
                        str(row.get("trade_date")),
                        str(row.get("classification_l2_code")),
                        str(row.get("index_l2_code")),
                    )
                    if previous_fact_key is not None and key <= previous_fact_key:
                        raise SectorDataBuildContractError("sector fact rows are not globally canonical")
                    previous_fact_key = key
                    encoded = canonical_json_bytes(row) + b"\n"
                    fact_handle.write(encoded)
                    fact_digest.update(encoded)
                    fact_count += 1
                for row in day.assignments:
                    key = (str(row.get("trade_date")), str(row.get("canonical_symbol")))
                    if previous_assignment_key is not None and key <= previous_assignment_key:
                        raise SectorDataBuildContractError("assignment rows are not globally canonical")
                    previous_assignment_key = key
                    fact_ref = row.get("sector_fact_row_hash")
                    if fact_ref is not None and fact_ref not in fact_hashes:
                        raise SectorDataBuildContractError("assignment references a non-local sector fact")
                    encoded = canonical_json_bytes(row) + b"\n"
                    assignment_handle.write(encoded)
                    assignment_digest.update(encoded)
                    opportunity_digest.update(
                        canonical_json_bytes(
                            {
                                "schema_version": SECTOR_DATA_OPPORTUNITY_SCHEMA,
                                "trade_date": key[0],
                                "canonical_symbol": key[1],
                            }
                        )
                        + b"\n"
                    )
                    assignment_count += 1
                    status = str(row.get("status"))
                    alignment = str(row.get("alignment_state"))
                    status_counts[status] += 1
                    alignment_counts[alignment] += 1
                    classification = row.get("classification") or {}
                    sector = str(
                        (classification.get("identity_codes") or {}).get("l1_code")
                        or "unavailable"
                    )
                    for reason in row.get("unavailable_reasons") or []:
                        reason_text = str(reason)
                        reason_counts[reason_text] += 1
                        reason_by_date[key[0]][reason_text] += 1
                        reason_by_sector[sector][reason_text] += 1
        if assignment_count != expected_opportunities:
            raise SectorDataBuildContractError(
                "sector candidate denominator closure failed: "
                f"observed={assignment_count} expected={expected_opportunities}"
            )
        if opportunity_digest.hexdigest() != expected_opportunity_digest:
            raise SectorDataBuildContractError("sector candidate opportunity identity digest mismatch")
        assignments_size = assignments_path.stat().st_size
        facts_size = facts_path.stat().st_size
        report = {
            "schema_version": SECTOR_DATA_REPORT_SCHEMA,
            "candidate_scope": candidate_scope,
            "source_denominator_digest": classification_receipt.denominator_digest,
            "expected_opportunities": expected_opportunities,
            "opportunity_digest": expected_opportunity_digest,
            "assignment_rows": assignment_count,
            "sector_fact_rows": fact_count,
            "status_counts": dict(sorted(status_counts.items())),
            "alignment_counts": dict(sorted(alignment_counts.items())),
            "unavailable_by_reason": dict(sorted(reason_counts.items())),
            "unavailable_by_date": {
                key: dict(sorted(value.items())) for key, value in sorted(reason_by_date.items())
            },
            "unavailable_by_sector": {
                key: dict(sorted(value.items())) for key, value in sorted(reason_by_sector.items())
            },
            "closure": {
                "resolved_plus_unaligned_plus_unavailable": sum(status_counts.values()),
                "expected_denominator": expected_opportunities,
                "passed": True,
            },
            "production_database_writes": 0,
            "production_activation": False,
        }
        report["canonical_hash"] = digest_named_fields(SECTOR_DATA_REPORT_SCHEMA, report)
        report_sha, report_size = _write_json_file(temporary / "candidate_report.json", report)
        candidate_hash = digest_named_fields(
            SECTOR_DATA_MANIFEST_SCHEMA,
            {
                "industry_bundle_hash": authority_bundle.manifest["bundle_hash"],
                "classification_authority_receipt_hash": classification_receipt.receipt_hash,
                "index_membership_authority_receipt_hash": index_receipt.receipt_hash,
                "source_denominator_digest": classification_receipt.denominator_digest,
                "candidate_scope": candidate_scope,
                "expected_opportunities": expected_opportunities,
                "opportunity_digest": expected_opportunity_digest,
                "assignments_sha256": assignment_digest.hexdigest(),
                "sector_facts_sha256": fact_digest.hexdigest(),
                "report_canonical_hash": report["canonical_hash"],
                "producer_commit": producer_commit,
                "producer_tree": producer_tree,
            },
        )
        manifest = {
            "schema_version": SECTOR_DATA_MANIFEST_SCHEMA,
            "candidate_scope": candidate_scope,
            "candidate_hash": candidate_hash,
            "industry_bundle_hash": authority_bundle.manifest["bundle_hash"],
            "classification_authority_receipt_hash": classification_receipt.receipt_hash,
            "index_membership_authority_receipt_hash": index_receipt.receipt_hash,
            "source_denominator_digest": classification_receipt.denominator_digest,
            "expected_opportunities": expected_opportunities,
            "opportunity_digest": expected_opportunity_digest,
            "producer_commit": producer_commit,
            "producer_tree": producer_tree,
            "files": {
                "assignments.jsonl": {
                    "sha256": assignment_digest.hexdigest(),
                    "size_bytes": assignments_size,
                    "row_count": assignment_count,
                },
                "sector_facts.jsonl": {
                    "sha256": fact_digest.hexdigest(),
                    "size_bytes": facts_size,
                    "row_count": fact_count,
                },
                "candidate_report.json": {
                    "sha256": report_sha,
                    "size_bytes": report_size,
                },
            },
        }
        _write_json_file(temporary / "candidate_manifest.json", manifest)
        read_sector_data_candidate(
            artifact_root=temporary,
            forbidden_roots=forbidden_roots,
        )
        if target.exists():
            raise SectorDataBuildContractError(f"refusing to overwrite candidate artifact root: {target}")
        os.rename(temporary, target)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return read_sector_data_candidate(
        artifact_root=target,
        forbidden_roots=forbidden_roots,
    )


def _read_json_file(path: Path) -> Mapping[str, Any]:
    if not path.is_file() or path.is_symlink():
        raise SectorDataBuildContractError(f"candidate file is missing or unsafe: {path.name}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SectorDataBuildContractError(f"candidate JSON is invalid: {path.name}") from exc
    if not isinstance(value, Mapping):
        raise SectorDataBuildContractError(f"candidate JSON must be an object: {path.name}")
    return value


def _validate_jsonl(
    path: Path,
    *,
    schema: str,
    key_fields: Sequence[str],
    expected_entry: Mapping[str, Any],
    fact_hashes: set[str] | None = None,
    opportunity_hasher=None,
    expected_receipt_hashes: tuple[str, str] | None = None,
) -> int:
    if not path.is_file() or path.is_symlink():
        raise SectorDataBuildContractError(f"candidate file is missing or unsafe: {path.name}")
    observed_sha, observed_size = _file_observation(path)
    if observed_sha != expected_entry.get("sha256") or observed_size != expected_entry.get("size_bytes"):
        raise SectorDataBuildContractError(f"candidate file hash/size mismatch: {path.name}")
    count = 0
    previous: tuple[str, ...] | None = None
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                raise SectorDataBuildContractError(f"blank candidate row: {path.name}:{line_number}")
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise SectorDataBuildContractError(
                    f"candidate row is invalid JSON: {path.name}:{line_number}"
                ) from exc
            if not isinstance(row, Mapping) or row.get("schema_version") != schema:
                raise SectorDataBuildContractError(f"candidate row schema mismatch: {path.name}:{line_number}")
            if schema == SECTOR_DATA_ASSIGNMENT_SCHEMA:
                expected_keys = {
                    "schema_version",
                    "canonical_symbol",
                    "trade_date",
                    "status",
                    "alignment_state",
                    "classification",
                    "index_membership",
                    "sector_fact_row_hash",
                    "unavailable_reasons",
                    "row_hash",
                }
                if set(row) != expected_keys:
                    raise SectorDataBuildContractError("assignment row fields differ from schema")
                status = row.get("status")
                alignment = row.get("alignment_state")
                reasons = row.get("unavailable_reasons")
                if status not in {"resolved", "unaligned", "unavailable"}:
                    raise SectorDataBuildContractError("assignment status is invalid")
                if alignment not in {value.value for value in AlignmentState}:
                    raise SectorDataBuildContractError("assignment alignment state is invalid")
                if not isinstance(reasons, list) or reasons != sorted(set(map(str, reasons))):
                    raise SectorDataBuildContractError("assignment unavailable reasons are not canonical")
                if status == "resolved" and (
                    alignment != AlignmentState.ALIGNED.value
                    or reasons
                    or row.get("sector_fact_row_hash") is None
                ):
                    raise SectorDataBuildContractError("resolved assignment is semantically inconsistent")
                if status == "unaligned" and (
                    alignment != AlignmentState.UNALIGNED.value
                    or "authority_unaligned" not in reasons
                    or row.get("sector_fact_row_hash") is not None
                ):
                    raise SectorDataBuildContractError("unaligned assignment is semantically inconsistent")
                if status == "unavailable" and (not reasons or row.get("sector_fact_row_hash") is not None):
                    raise SectorDataBuildContractError("unavailable assignment is semantically inconsistent")
                for authority in ("classification", "index_membership"):
                    projection = row.get(authority)
                    if not isinstance(projection, Mapping) or projection.get("status") not in {
                        "resolved",
                        "unavailable",
                    }:
                        raise SectorDataBuildContractError("assignment authority projection is invalid")
                    expected_projection_keys = (
                        {
                            "status",
                            "identity_codes",
                            "identity_hash",
                            "authority_identity",
                            "valid_from",
                            "valid_to_exclusive",
                            "known_from",
                            "taxonomy_contract_id",
                            "taxonomy_version",
                            "candidate_row_hashes",
                            "authority_receipt_hash",
                            "non_as_known_taxonomy",
                            "resolution_hash",
                        }
                        if projection.get("status") == "resolved"
                        else {
                            "status",
                            "reason",
                            "authority_receipt_hash",
                            "conflict_candidates",
                        }
                    )
                    if set(projection) != expected_projection_keys:
                        raise SectorDataBuildContractError(
                            "assignment authority projection fields differ from schema"
                        )
                if expected_receipt_hashes is not None and (
                    row["classification"].get("authority_receipt_hash") != expected_receipt_hashes[0]
                    or row["index_membership"].get("authority_receipt_hash") != expected_receipt_hashes[1]
                ):
                    raise SectorDataBuildContractError("assignment authority receipt binding is invalid")
            elif schema == SECTOR_DATA_FACT_SCHEMA:
                expected_keys = {
                    "schema_version",
                    "trade_date",
                    "classification_l2_code",
                    "classification_l2_identity_hash",
                    "index_l2_code",
                    "index_l2_identity_hash",
                    "classification_authority_receipt_hash",
                    "index_membership_authority_receipt_hash",
                    "sw_daily",
                    "moneyflow_aggregate",
                    "contributor_coverage",
                    "row_hash",
                }
                if set(row) != expected_keys:
                    raise SectorDataBuildContractError("sector fact row fields differ from schema")
                if set(row.get("sw_daily") or {}) != set(SW_DAILY_FIELDS):
                    raise SectorDataBuildContractError("sector fact sw_daily fields differ from schema")
                if set(row.get("moneyflow_aggregate") or {}) != set(MONEYFLOW_FIELDS):
                    raise SectorDataBuildContractError("sector fact moneyflow fields differ from schema")
                coverage = row.get("contributor_coverage")
                if not isinstance(coverage, Mapping) or set(coverage) != {"expected", "resolved", "ratio"}:
                    raise SectorDataBuildContractError("sector fact contributor coverage is invalid")
                if (
                    type(coverage.get("expected")) is not int
                    or type(coverage.get("resolved")) is not int
                    or coverage["expected"] <= 0
                    or coverage["resolved"] <= 0
                    or coverage["resolved"] > coverage["expected"]
                ):
                    raise SectorDataBuildContractError("sector fact contributor counts are invalid")
                if expected_receipt_hashes is not None and (
                    row.get("classification_authority_receipt_hash") != expected_receipt_hashes[0]
                    or row.get("index_membership_authority_receipt_hash") != expected_receipt_hashes[1]
                ):
                    raise SectorDataBuildContractError("sector fact authority receipt binding is invalid")
            row_without_hash = {key: value for key, value in row.items() if key != "row_hash"}
            if row.get("row_hash") != digest_named_fields(schema, row_without_hash):
                raise SectorDataBuildContractError(f"candidate row hash mismatch: {path.name}:{line_number}")
            key = tuple(str(row.get(field)) for field in key_fields)
            if previous is not None and key <= previous:
                raise SectorDataBuildContractError(f"candidate row order is not canonical: {path.name}")
            previous = key
            if opportunity_hasher is not None:
                opportunity_hasher.update(
                    canonical_json_bytes(
                        {
                            "schema_version": SECTOR_DATA_OPPORTUNITY_SCHEMA,
                            "trade_date": str(row.get("trade_date")),
                            "canonical_symbol": str(row.get("canonical_symbol")),
                        }
                    )
                    + b"\n"
                )
            if fact_hashes is not None:
                fact_ref = row.get("sector_fact_row_hash")
                if fact_ref is not None and fact_ref not in fact_hashes:
                    raise SectorDataBuildContractError("assignment references an unknown sector fact")
            count += 1
    if count != expected_entry.get("row_count"):
        raise SectorDataBuildContractError(f"candidate row count mismatch: {path.name}")
    return count


def _validate_count_mapping(
    value: Any,
    *,
    label: str,
    allowed_keys: set[str] | None = None,
) -> Mapping[str, int]:
    if not isinstance(value, Mapping):
        raise SectorDataBuildContractError(f"sector candidate {label} must be an object")
    output: dict[str, int] = {}
    for raw_key, count in value.items():
        key = str(raw_key)
        if not key or type(count) is not int or count < 0:
            raise SectorDataBuildContractError(f"sector candidate {label} contains an invalid count")
        if allowed_keys is not None and key not in allowed_keys:
            raise SectorDataBuildContractError(f"sector candidate {label} contains an unknown key")
        output[key] = count
    return output


def _validate_nested_count_mapping(value: Any, *, label: str) -> None:
    if not isinstance(value, Mapping):
        raise SectorDataBuildContractError(f"sector candidate {label} must be an object")
    for key, counts in value.items():
        if not str(key):
            raise SectorDataBuildContractError(f"sector candidate {label} contains an empty key")
        _validate_count_mapping(counts, label=f"{label}.{key}")


def read_sector_data_candidate(
    *,
    artifact_root: Path,
    forbidden_roots: Sequence[Path],
) -> SectorDataCandidateReadback:
    root = require_repo_external_root(artifact_root, forbidden_roots=forbidden_roots)
    if root.is_symlink():
        raise SectorDataBuildContractError("candidate artifact root cannot be a symbolic link")
    manifest = _read_json_file(root / "candidate_manifest.json")
    if manifest.get("schema_version") != SECTOR_DATA_MANIFEST_SCHEMA:
        raise SectorDataBuildContractError("sector candidate manifest schema is invalid")
    expected_manifest_keys = {
        "schema_version",
        "candidate_scope",
        "candidate_hash",
        "industry_bundle_hash",
        "classification_authority_receipt_hash",
        "index_membership_authority_receipt_hash",
        "source_denominator_digest",
        "expected_opportunities",
        "opportunity_digest",
        "producer_commit",
        "producer_tree",
        "files",
    }
    if set(manifest) != expected_manifest_keys:
        raise SectorDataBuildContractError("sector candidate manifest fields differ from schema")
    if manifest.get("candidate_scope") not in {"full", "sample"}:
        raise SectorDataBuildContractError("sector candidate scope is invalid")
    if not re.fullmatch(r"[0-9a-f]{64}", str(manifest.get("opportunity_digest") or "")):
        raise SectorDataBuildContractError("sector candidate opportunity digest is invalid")
    for field in (
        "candidate_hash",
        "industry_bundle_hash",
        "classification_authority_receipt_hash",
        "index_membership_authority_receipt_hash",
        "source_denominator_digest",
    ):
        if not re.fullmatch(r"[0-9a-f]{64}", str(manifest.get(field) or "")):
            raise SectorDataBuildContractError(f"sector candidate {field} is invalid")
    if type(manifest.get("expected_opportunities")) is not int or manifest["expected_opportunities"] <= 0:
        raise SectorDataBuildContractError("sector candidate expected opportunity count is invalid")
    if not _GIT_OBJECT_RE.fullmatch(str(manifest.get("producer_commit") or "")) or not _GIT_OBJECT_RE.fullmatch(
        str(manifest.get("producer_tree") or "")
    ):
        raise SectorDataBuildContractError("sector candidate producer identity is invalid")
    expected_files = {"assignments.jsonl", "sector_facts.jsonl", "candidate_report.json"}
    files = manifest.get("files")
    if not isinstance(files, Mapping) or set(files) != expected_files:
        raise SectorDataBuildContractError("sector candidate manifest file set is invalid")
    actual_files = {path.name for path in root.iterdir() if path.is_file()}
    if actual_files != {*expected_files, "candidate_manifest.json"}:
        raise SectorDataBuildContractError("sector candidate directory file set is invalid")
    for name, entry in files.items():
        expected_entry_keys = {"sha256", "size_bytes"}
        if name.endswith(".jsonl"):
            expected_entry_keys.add("row_count")
        if not isinstance(entry, Mapping) or set(entry) != expected_entry_keys:
            raise SectorDataBuildContractError(f"sector candidate file entry is invalid: {name}")
        if not re.fullmatch(r"[0-9a-f]{64}", str(entry.get("sha256") or "")):
            raise SectorDataBuildContractError(f"sector candidate file digest is invalid: {name}")
        if type(entry.get("size_bytes")) is not int or entry["size_bytes"] < 0:
            raise SectorDataBuildContractError(f"sector candidate file size is invalid: {name}")
        if name.endswith(".jsonl") and (
            type(entry.get("row_count")) is not int or entry["row_count"] < 0
        ):
            raise SectorDataBuildContractError(f"sector candidate row count is invalid: {name}")
    report = _read_json_file(root / "candidate_report.json")
    report_sha, report_size = _file_observation(root / "candidate_report.json")
    if (
        report_sha != files["candidate_report.json"].get("sha256")
        or report_size != files["candidate_report.json"].get("size_bytes")
        or report.get("schema_version") != SECTOR_DATA_REPORT_SCHEMA
    ):
        raise SectorDataBuildContractError("sector candidate report readback mismatch")
    if (
        report.get("candidate_scope") != manifest.get("candidate_scope")
        or report.get("expected_opportunities") != manifest.get("expected_opportunities")
        or report.get("opportunity_digest") != manifest.get("opportunity_digest")
        or report.get("source_denominator_digest") != manifest.get("source_denominator_digest")
    ):
        raise SectorDataBuildContractError("sector candidate report/manifest contract mismatch")
    expected_report_keys = {
        "schema_version",
        "candidate_scope",
        "source_denominator_digest",
        "expected_opportunities",
        "opportunity_digest",
        "assignment_rows",
        "sector_fact_rows",
        "status_counts",
        "alignment_counts",
        "unavailable_by_reason",
        "unavailable_by_date",
        "unavailable_by_sector",
        "closure",
        "production_database_writes",
        "production_activation",
        "canonical_hash",
    }
    if set(report) != expected_report_keys:
        raise SectorDataBuildContractError("sector candidate report fields differ from schema")
    if (
        type(report.get("assignment_rows")) is not int
        or report["assignment_rows"] < 0
        or type(report.get("sector_fact_rows")) is not int
        or report["sector_fact_rows"] < 0
        or report.get("production_database_writes") != 0
        or report.get("production_activation") is not False
    ):
        raise SectorDataBuildContractError("sector candidate report scalar fields are invalid")
    status_counts = _validate_count_mapping(
        report.get("status_counts"),
        label="status_counts",
        allowed_keys={"resolved", "unaligned", "unavailable"},
    )
    alignment_counts = _validate_count_mapping(
        report.get("alignment_counts"),
        label="alignment_counts",
        allowed_keys={value.value for value in AlignmentState},
    )
    _validate_count_mapping(report.get("unavailable_by_reason"), label="unavailable_by_reason")
    _validate_nested_count_mapping(report.get("unavailable_by_date"), label="unavailable_by_date")
    _validate_nested_count_mapping(report.get("unavailable_by_sector"), label="unavailable_by_sector")
    closure = report.get("closure")
    if not isinstance(closure, Mapping) or set(closure) != {
        "resolved_plus_unaligned_plus_unavailable",
        "expected_denominator",
        "passed",
    }:
        raise SectorDataBuildContractError("sector candidate report closure is invalid")
    if (
        report.get("assignment_rows") != manifest.get("expected_opportunities")
        or closure.get("resolved_plus_unaligned_plus_unavailable")
        != manifest.get("expected_opportunities")
        or closure.get("expected_denominator") != manifest.get("expected_opportunities")
        or closure.get("passed") is not True
        or sum(status_counts.values()) != manifest.get("expected_opportunities")
        or sum(alignment_counts.values()) != manifest.get("expected_opportunities")
    ):
        raise SectorDataBuildContractError("sector candidate report denominator closure is invalid")
    receipt_hashes = (
        str(manifest["classification_authority_receipt_hash"]),
        str(manifest["index_membership_authority_receipt_hash"]),
    )
    report_without_hash = {key: value for key, value in report.items() if key != "canonical_hash"}
    if report.get("canonical_hash") != digest_named_fields(SECTOR_DATA_REPORT_SCHEMA, report_without_hash):
        raise SectorDataBuildContractError("sector candidate report canonical hash mismatch")
    fact_hashes: set[str] = set()
    facts_path = root / "sector_facts.jsonl"
    fact_rows = _validate_jsonl(
        facts_path,
        schema=SECTOR_DATA_FACT_SCHEMA,
        key_fields=("trade_date", "classification_l2_code", "index_l2_code"),
        expected_entry=files["sector_facts.jsonl"],
        expected_receipt_hashes=receipt_hashes,
    )
    if fact_rows != report.get("sector_fact_rows"):
        raise SectorDataBuildContractError("sector candidate fact row count differs from report")
    with facts_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            fact_hashes.add(str(json.loads(line)["row_hash"]))
    opportunity_hasher = hashlib.sha256()
    assignment_rows = _validate_jsonl(
        root / "assignments.jsonl",
        schema=SECTOR_DATA_ASSIGNMENT_SCHEMA,
        key_fields=("trade_date", "canonical_symbol"),
        expected_entry=files["assignments.jsonl"],
        fact_hashes=fact_hashes,
        opportunity_hasher=opportunity_hasher,
        expected_receipt_hashes=receipt_hashes,
    )
    if assignment_rows != manifest.get("expected_opportunities"):
        raise SectorDataBuildContractError("sector candidate denominator readback mismatch")
    if opportunity_hasher.hexdigest() != manifest.get("opportunity_digest"):
        raise SectorDataBuildContractError("sector candidate opportunity readback mismatch")
    candidate_hash = digest_named_fields(
        SECTOR_DATA_MANIFEST_SCHEMA,
        {
            "industry_bundle_hash": manifest.get("industry_bundle_hash"),
            "classification_authority_receipt_hash": manifest.get(
                "classification_authority_receipt_hash"
            ),
            "index_membership_authority_receipt_hash": manifest.get(
                "index_membership_authority_receipt_hash"
            ),
            "source_denominator_digest": manifest.get("source_denominator_digest"),
            "candidate_scope": manifest.get("candidate_scope"),
            "expected_opportunities": manifest.get("expected_opportunities"),
            "opportunity_digest": manifest.get("opportunity_digest"),
            "assignments_sha256": files["assignments.jsonl"].get("sha256"),
            "sector_facts_sha256": files["sector_facts.jsonl"].get("sha256"),
            "report_canonical_hash": report.get("canonical_hash"),
            "producer_commit": manifest.get("producer_commit"),
            "producer_tree": manifest.get("producer_tree"),
        },
    )
    if candidate_hash != manifest.get("candidate_hash"):
        raise SectorDataBuildContractError("sector candidate canonical hash mismatch")
    return SectorDataCandidateReadback(
        artifact_root=root,
        manifest=manifest,
        report=report,
        assignment_rows=assignment_rows,
        sector_fact_rows=fact_rows,
    )


_PREFLIGHT_DAY_SQL = """\
WITH authoritative_universes AS (
    SELECT universe_key
    FROM market.stock_universe_pit_state
    WHERE status = 'ready'
      AND dirty = FALSE
      AND start_date <= %(trade_date)s
      AND end_date >= %(trade_date)s
      AND (
          universe_key = %(live_universe_key)s
          OR universe_key LIKE %(qe_universe_pattern)s
      )
),
eligible AS (
    SELECT DISTINCT spans.ts_code
    FROM market.stock_universe_pit_spans AS spans
    JOIN authoritative_universes USING (universe_key)
    WHERE spans.eligible_start <= %(trade_date)s
      AND spans.eligible_end >= %(trade_date)s
),
active AS (
    SELECT
        member.ts_code,
        member.l1_code,
        member.l2_code,
        member.in_date,
        MAX(member.in_date) OVER (PARTITION BY member.ts_code) AS latest_in_date
    FROM market.sw_index_member AS member
    JOIN eligible USING (ts_code)
    WHERE member.in_date <= %(trade_date)s
      AND (member.out_date >= %(trade_date)s OR member.out_date IS NULL)
),
latest AS (
    SELECT ts_code, l1_code, l2_code, in_date
    FROM active
    WHERE in_date = latest_in_date
),
mapping_summary AS (
    SELECT
        ts_code,
        COUNT(*) AS latest_mapping_count,
        COUNT(*) FILTER (
            WHERE NULLIF(BTRIM(l1_code), '') IS NULL
               OR NULLIF(BTRIM(l2_code), '') IS NULL
               OR in_date IS NULL
        ) AS invalid_identity_count
    FROM latest
    GROUP BY ts_code
),
canonical_pit AS (
    SELECT
        latest.ts_code,
        MIN(latest.l1_code) AS l1_code,
        MIN(latest.l2_code) AS l2_code,
        MIN(latest.in_date) AS in_date
    FROM latest
    JOIN mapping_summary USING (ts_code)
    WHERE mapping_summary.latest_mapping_count = 1
      AND mapping_summary.invalid_identity_count = 0
    GROUP BY latest.ts_code
),
l2_moneyflow AS (
    SELECT
        pit.l2_code,
        SUM(mf.buy_sm_amount) AS buy_sm_amount,
        SUM(mf.sell_sm_amount) AS sell_sm_amount,
        SUM(mf.buy_md_amount) AS buy_md_amount,
        SUM(mf.sell_md_amount) AS sell_md_amount,
        SUM(mf.buy_lg_amount) AS buy_lg_amount,
        SUM(mf.sell_lg_amount) AS sell_lg_amount,
        SUM(mf.buy_elg_amount) AS buy_elg_amount,
        SUM(mf.sell_elg_amount) AS sell_elg_amount,
        SUM(mf.net_mf_amount) AS net_mf_amount,
        SUM(mf.buy_elg_vol) AS buy_elg_vol,
        SUM(mf.sell_elg_vol) AS sell_elg_vol,
        SUM(mf.net_mf_vol) AS net_mf_vol
    FROM canonical_pit pit
    JOIN market.moneyflow_ts mf
      ON mf.ts_code = pit.ts_code
     AND mf.trade_date = %(trade_date)s
    GROUP BY pit.l2_code
),
unpublished_l2 AS (
    -- BUG-929: Shenwan does not publish quotes for L2 indices with fewer
    -- than five member stocks (market.sw_index_classify.is_pub = '0'), and
    -- tushare withdrew their historical sw_daily rows on 2026-04-28.
    -- Requiring sw_daily facts for these indices is unsatisfiable, so the
    -- contract exempts their member stocks from the hard-failure count and
    -- reports them separately as a warning instead.
    SELECT index_code
    FROM market.sw_index_classify
    WHERE is_pub = '0'
)
SELECT
    (SELECT CASE WHEN EXISTS (SELECT 1 FROM authoritative_universes) THEN 0 ELSE 1 END),
    (
        SELECT COUNT(*)
        FROM eligible
        LEFT JOIN mapping_summary USING (ts_code)
        WHERE mapping_summary.ts_code IS NULL
    ),
    (SELECT COUNT(*) FROM mapping_summary WHERE latest_mapping_count <> 1),
    (SELECT COUNT(*) FROM mapping_summary WHERE invalid_identity_count <> 0),
    (
        SELECT COUNT(*)
        FROM canonical_pit pit
        LEFT JOIN market.sw_daily sd
          ON sd.ts_code = pit.l2_code
         AND sd.trade_date = %(trade_date)s
        WHERE (
            sd.ts_code IS NULL
            OR sd.open IS NULL
            OR sd.high IS NULL
            OR sd.low IS NULL
            OR sd.close IS NULL
            OR sd.pct_change IS NULL
            OR sd.vol IS NULL
            OR sd.amount IS NULL
            OR sd.pe IS NULL
            OR sd.pb IS NULL
            OR sd.total_mv IS NULL
        )
          AND pit.l2_code NOT IN (SELECT index_code FROM unpublished_l2)
    ),
    (
        SELECT COUNT(*)
        FROM (SELECT DISTINCT l2_code FROM canonical_pit) pit
        LEFT JOIN l2_moneyflow mf USING (l2_code)
        WHERE mf.l2_code IS NULL
           OR mf.buy_sm_amount IS NULL
           OR mf.sell_sm_amount IS NULL
           OR mf.buy_md_amount IS NULL
           OR mf.sell_md_amount IS NULL
           OR mf.buy_lg_amount IS NULL
           OR mf.sell_lg_amount IS NULL
           OR mf.buy_elg_amount IS NULL
           OR mf.sell_elg_amount IS NULL
           OR mf.net_mf_amount IS NULL
           OR mf.buy_elg_vol IS NULL
           OR mf.sell_elg_vol IS NULL
           OR mf.net_mf_vol IS NULL
    ),
    (
        SELECT COUNT(*)
        FROM canonical_pit pit
        LEFT JOIN market.sw_daily sd
          ON sd.ts_code = pit.l2_code
         AND sd.trade_date = %(trade_date)s
        WHERE (
            sd.ts_code IS NULL
            OR sd.open IS NULL
            OR sd.high IS NULL
            OR sd.low IS NULL
            OR sd.close IS NULL
            OR sd.pct_change IS NULL
            OR sd.vol IS NULL
            OR sd.amount IS NULL
            OR sd.pe IS NULL
            OR sd.pb IS NULL
            OR sd.total_mv IS NULL
        )
          AND pit.l2_code IN (SELECT index_code FROM unpublished_l2)
    )
"""


_DELETE_STALE_DAY_SQL = """\
WITH authoritative_universes AS (
    SELECT universe_key
    FROM market.stock_universe_pit_state
    WHERE status = 'ready'
      AND dirty = FALSE
      AND start_date <= %(trade_date)s
      AND end_date >= %(trade_date)s
      AND (
          universe_key = %(live_universe_key)s
          OR universe_key LIKE %(qe_universe_pattern)s
      )
),
eligible AS (
    SELECT DISTINCT spans.ts_code
    FROM market.stock_universe_pit_spans AS spans
    JOIN authoritative_universes USING (universe_key)
    WHERE spans.eligible_start <= %(trade_date)s
      AND spans.eligible_end >= %(trade_date)s
),
active AS (
    SELECT
        member.ts_code,
        member.in_date,
        MAX(member.in_date) OVER (PARTITION BY member.ts_code) AS latest_in_date
    FROM market.sw_index_member AS member
    JOIN eligible USING (ts_code)
    WHERE member.in_date <= %(trade_date)s
      AND (member.out_date >= %(trade_date)s OR member.out_date IS NULL)
),
pit AS (
    SELECT ts_code
    FROM active
    WHERE in_date = latest_in_date
)
DELETE FROM market.sector_data target
WHERE target.trade_date = %(trade_date)s
  AND NOT EXISTS (
      SELECT 1
      FROM pit
      WHERE pit.ts_code = target.ts_code
  )
"""


_BUILD_DAY_SQL = """\
WITH authoritative_universes AS (
    SELECT universe_key
    FROM market.stock_universe_pit_state
    WHERE status = 'ready'
      AND dirty = FALSE
      AND start_date <= %(trade_date)s
      AND end_date >= %(trade_date)s
      AND (
          universe_key = %(live_universe_key)s
          OR universe_key LIKE %(qe_universe_pattern)s
      )
),
eligible AS (
    SELECT DISTINCT spans.ts_code
    FROM market.stock_universe_pit_spans AS spans
    JOIN authoritative_universes USING (universe_key)
    WHERE spans.eligible_start <= %(trade_date)s
      AND spans.eligible_end >= %(trade_date)s
),
active AS (
    SELECT
        member.ts_code,
        member.l2_code,
        member.in_date,
        MAX(member.in_date) OVER (PARTITION BY member.ts_code) AS latest_in_date
    FROM market.sw_index_member AS member
    JOIN eligible USING (ts_code)
    WHERE member.in_date <= %(trade_date)s
      AND (member.out_date >= %(trade_date)s OR member.out_date IS NULL)
),
pit AS (
    SELECT ts_code, l2_code
    FROM active
    WHERE in_date = latest_in_date
),
l2_mf AS (
    SELECT
        pit.l2_code,
        SUM(mf.buy_sm_amount)   AS agg_buy_sm_amt,
        SUM(mf.sell_sm_amount)  AS agg_sell_sm_amt,
        SUM(mf.buy_md_amount)   AS agg_buy_md_amt,
        SUM(mf.sell_md_amount)  AS agg_sell_md_amt,
        SUM(mf.buy_lg_amount)   AS agg_buy_lg_amt,
        SUM(mf.sell_lg_amount)  AS agg_sell_lg_amt,
        SUM(mf.buy_elg_amount)  AS agg_buy_elg_amt,
        SUM(mf.sell_elg_amount) AS agg_sell_elg_amt,
        SUM(mf.net_mf_amount)   AS agg_net_amt,
        SUM(mf.buy_elg_vol)     AS agg_buy_elg_vol,
        SUM(mf.sell_elg_vol)    AS agg_sell_elg_vol,
        SUM(mf.net_mf_vol)      AS agg_net_vol
    FROM market.moneyflow_ts mf
    JOIN pit ON mf.ts_code = pit.ts_code
    WHERE mf.trade_date = %(trade_date)s
    GROUP BY pit.l2_code
)
INSERT INTO market.sector_data (
    trade_date, ts_code,
    sw2_open, sw2_high, sw2_low, sw2_close, sw2_pct_change,
    sw2_vol, sw2_amount, sw2_pe, sw2_pb, sw2_total_mv,
    sw2_mf_buy_sm_amt, sw2_mf_sell_sm_amt,
    sw2_mf_buy_md_amt, sw2_mf_sell_md_amt,
    sw2_mf_buy_lg_amt, sw2_mf_sell_lg_amt,
    sw2_mf_buy_elg_amt, sw2_mf_sell_elg_amt,
    sw2_mf_net_amt,
    sw2_mf_buy_elg_vol, sw2_mf_sell_elg_vol,
    sw2_mf_net_vol
)
SELECT
    %(trade_date)s,
    pit.ts_code,
    sd.open, sd.high, sd.low, sd.close, sd.pct_change,
    sd.vol, sd.amount, sd.pe, sd.pb, sd.total_mv,
    l2_mf.agg_buy_sm_amt, l2_mf.agg_sell_sm_amt,
    l2_mf.agg_buy_md_amt, l2_mf.agg_sell_md_amt,
    l2_mf.agg_buy_lg_amt, l2_mf.agg_sell_lg_amt,
    l2_mf.agg_buy_elg_amt, l2_mf.agg_sell_elg_amt,
    l2_mf.agg_net_amt,
    l2_mf.agg_buy_elg_vol, l2_mf.agg_sell_elg_vol,
    l2_mf.agg_net_vol
FROM pit
JOIN market.sw_daily sd
  ON pit.l2_code = sd.ts_code
 AND sd.trade_date = %(trade_date)s
LEFT JOIN l2_mf
  ON pit.l2_code = l2_mf.l2_code
ON CONFLICT (trade_date, ts_code) DO UPDATE SET
    sw2_open            = EXCLUDED.sw2_open,
    sw2_high            = EXCLUDED.sw2_high,
    sw2_low             = EXCLUDED.sw2_low,
    sw2_close           = EXCLUDED.sw2_close,
    sw2_pct_change      = EXCLUDED.sw2_pct_change,
    sw2_vol             = EXCLUDED.sw2_vol,
    sw2_amount          = EXCLUDED.sw2_amount,
    sw2_pe              = EXCLUDED.sw2_pe,
    sw2_pb              = EXCLUDED.sw2_pb,
    sw2_total_mv        = EXCLUDED.sw2_total_mv,
    sw2_mf_buy_sm_amt   = EXCLUDED.sw2_mf_buy_sm_amt,
    sw2_mf_sell_sm_amt  = EXCLUDED.sw2_mf_sell_sm_amt,
    sw2_mf_buy_md_amt   = EXCLUDED.sw2_mf_buy_md_amt,
    sw2_mf_sell_md_amt  = EXCLUDED.sw2_mf_sell_md_amt,
    sw2_mf_buy_lg_amt   = EXCLUDED.sw2_mf_buy_lg_amt,
    sw2_mf_sell_lg_amt  = EXCLUDED.sw2_mf_sell_lg_amt,
    sw2_mf_buy_elg_amt  = EXCLUDED.sw2_mf_buy_elg_amt,
    sw2_mf_sell_elg_amt = EXCLUDED.sw2_mf_sell_elg_amt,
    sw2_mf_net_amt      = EXCLUDED.sw2_mf_net_amt,
    sw2_mf_buy_elg_vol  = EXCLUDED.sw2_mf_buy_elg_vol,
    sw2_mf_sell_elg_vol = EXCLUDED.sw2_mf_sell_elg_vol,
    sw2_mf_net_vol      = EXCLUDED.sw2_mf_net_vol
"""


_TRADE_DATES_SQL = """\
SELECT DISTINCT trade_date
FROM market.moneyflow_ts
WHERE trade_date BETWEEN %(start)s AND %(end)s
ORDER BY trade_date
"""


class SectorDataBuilder:
    """Build market.sector_data from PIT membership and source market facts."""

    def build_date(self, trade_date: dt.date) -> int:
        """Build one day atomically and return the inserted or updated row count."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                params = {
                    "trade_date": trade_date,
                    "live_universe_key": DEFAULT_ST_PIT_UNIVERSE_KEY,
                    "qe_universe_pattern": f"{IMMUTABLE_QE_ST_PIT_UNIVERSE_PREFIX}%",
                }
                cur.execute(_PREFLIGHT_DAY_SQL, params)
                (
                    universe_not_ready,
                    missing_mapping,
                    ambiguous,
                    invalid_identity,
                    missing_sector_facts,
                    missing_moneyflow_facts,
                    unpublished_l2_exempted,
                ) = cur.fetchone()
                if (
                    universe_not_ready
                    or missing_mapping
                    or ambiguous
                    or invalid_identity
                    or missing_sector_facts
                    or missing_moneyflow_facts
                ):
                    raise SectorDataBuildContractError(
                        "SECTOR_DATA_PIT_CONTRACT_INVALID: "
                        f"trade_date={trade_date}, "
                        f"universe_not_ready={universe_not_ready}, "
                        f"missing_pit_mappings={missing_mapping}, "
                        f"ambiguous_latest_mappings={ambiguous}, "
                        f"invalid_mapping_identities={invalid_identity}, "
                        f"missing_sw_daily_facts={missing_sector_facts}, "
                        f"missing_l2_moneyflow_facts={missing_moneyflow_facts}"
                    )
                if unpublished_l2_exempted:
                    logger.warning(
                        "sector_data build_date %s: %d stocks exempted from the "
                        "sw_daily contract (unpublished is_pub=0 L2 indices); "
                        "they are skipped for this day",
                        trade_date,
                        unpublished_l2_exempted,
                    )
                cur.execute(_DELETE_STALE_DAY_SQL, params)
                cur.execute(_BUILD_DAY_SQL, params)
                rows = cur.rowcount
            conn.commit()
        return rows

    def build_range(self, start_date: dt.date, end_date: dt.date) -> int:
        """Build an inclusive date range and return the total written row count."""
        trade_dates = self._get_trade_dates(start_date, end_date)
        if not trade_dates:
            logger.warning(
                "sector_data build_range: no trade dates in %s ~ %s",
                start_date,
                end_date,
            )
            return 0

        total = 0
        for index, trade_date in enumerate(trade_dates, 1):
            rows = self.build_date(trade_date)
            total += rows
            if rows > 0 and index % 50 == 0:
                logger.info(
                    "sector_data progress: %d/%d dates, %d total rows (latest: %s = %d rows)",
                    index,
                    len(trade_dates),
                    total,
                    trade_date,
                    rows,
                )

        logger.info(
            "sector_data build_range complete: %d dates, %d total rows",
            len(trade_dates),
            total,
        )
        return total

    def _get_trade_dates(self, start: dt.date, end: dt.date) -> List[dt.date]:
        """Return source trading dates in the requested range."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(_TRADE_DATES_SQL, {"start": start, "end": end})
                return [row[0] for row in cur.fetchall()]
