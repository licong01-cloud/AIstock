"""Candidate-only C-013 builders and frozen-denominator preflight."""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

from backend.services.dataset_release.canonical import canonical_json_bytes, digest_named_fields, sha256_hex

from .contracts import (
    PREFLIGHT_REPORT_SCHEMA,
    TAXONOMY_CATALOG_SCHEMA,
    AuthorityReceipt,
    AuthorityType,
    CandidateInterval,
    IndustryPitContractError,
    ResolutionRequest,
    ResolvedIndustryIdentity,
    TaxonomyIdentity,
    UnavailableIndustryIdentity,
    UnavailableReason,
    make_candidate_interval,
    require_date,
    require_symbol,
)
from .resolver import IndustryPitResolver, resolve_dual_authority


SW2021_CLASSIFICATION_VALID_FROM = date(2021, 7, 30)
SW2021_CAUSAL_DAILY_FROM = date(2021, 8, 2)
SW2021_INDEX_SWITCH_FROM = date(2021, 12, 13)
SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


@dataclass(frozen=True, slots=True)
class UniverseSpan:
    canonical_symbol: str
    eligible_start: date
    eligible_end: date | None

    def __post_init__(self) -> None:
        object.__setattr__(self, "canonical_symbol", require_symbol(self.canonical_symbol))
        require_date(self.eligible_start, field="eligible_start")
        if self.eligible_end is not None:
            require_date(self.eligible_end, field="eligible_end")
            if self.eligible_end < self.eligible_start:
                raise IndustryPitContractError("universe span is inverted")

    @property
    def end_exclusive(self) -> date | None:
        return self.eligible_end + timedelta(days=1) if self.eligible_end else None

    def as_dict(self) -> dict[str, str | None]:
        return {
            "canonical_symbol": self.canonical_symbol,
            "eligible_start": self.eligible_start.isoformat(),
            "eligible_end": self.eligible_end.isoformat() if self.eligible_end else None,
        }


@dataclass(frozen=True, slots=True)
class FrozenDenominator:
    window_start: date
    window_end: date
    trading_dates: tuple[date, ...]
    universe_spans: tuple[UniverseSpan, ...]
    total_opportunities: int
    digest: str

    @classmethod
    def build(
        cls,
        *,
        window_start: date,
        window_end: date,
        trading_dates: Iterable[date],
        universe_spans: Iterable[UniverseSpan],
    ) -> "FrozenDenominator":
        require_date(window_start, field="window_start")
        require_date(window_end, field="window_end")
        if window_end < window_start:
            raise IndustryPitContractError("denominator window is inverted")
        dates = tuple(sorted(set(trading_dates)))
        if not dates or dates[0] < window_start or dates[-1] > window_end:
            raise IndustryPitContractError("trading calendar is empty or escapes the frozen window")
        spans = tuple(sorted(universe_spans, key=lambda value: (value.canonical_symbol, value.eligible_start)))
        if not spans:
            raise IndustryPitContractError("frozen universe is empty")
        previous_by_symbol: dict[str, UniverseSpan] = {}
        for span in spans:
            previous = previous_by_symbol.get(span.canonical_symbol)
            if previous is not None and (previous.eligible_end is None or span.eligible_start <= previous.eligible_end):
                raise IndustryPitContractError(f"overlapping universe spans: {span.canonical_symbol}")
            previous_by_symbol[span.canonical_symbol] = span
        total = 0
        for span in spans:
            start = max(window_start, span.eligible_start)
            end = min(window_end, span.eligible_end or window_end)
            if start <= end:
                total += bisect_right(dates, end) - bisect_left(dates, start)
        if total <= 0:
            raise IndustryPitContractError("frozen denominator contains no symbol-day opportunities")
        payload = {
            "schema_version": "industry_pit_frozen_denominator_v1",
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "trading_dates": [value.isoformat() for value in dates],
            "universe_spans": [value.as_dict() for value in spans],
            "total_opportunities": total,
        }
        return cls(
            window_start=window_start,
            window_end=window_end,
            trading_dates=dates,
            universe_spans=spans,
            total_opportunities=total,
            digest=sha256_hex(canonical_json_bytes(payload)),
        )

    def dates_for_span(self, span: UniverseSpan) -> tuple[date, ...]:
        start = max(self.window_start, span.eligible_start)
        end = min(self.window_end, span.eligible_end or self.window_end)
        if start > end:
            return ()
        left = bisect_left(self.trading_dates, start)
        right = bisect_right(self.trading_dates, end)
        return self.trading_dates[left:right]


@dataclass(frozen=True, slots=True)
class TaxonomyCatalog:
    contract_id: str
    version: str
    source_sha256: str
    identities: Mapping[str, TaxonomyIdentity]
    catalog_hash: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": TAXONOMY_CATALOG_SCHEMA,
            "contract_id": self.contract_id,
            "version": self.version,
            "source_sha256": self.source_sha256,
            "identities": {code: self.identities[code].as_dict() for code in sorted(self.identities)},
            "catalog_hash": self.catalog_hash,
        }


def build_taxonomy_catalog(
    rows: Iterable[Mapping[str, Any]],
    *,
    source_sha256: str,
    contract_id: str = "sw2021_classification_catalog_v1",
    version: str = "SW2021",
) -> TaxonomyCatalog:
    identities: dict[str, TaxonomyIdentity] = {}
    conflicts: set[str] = set()
    for raw in rows:
        code = str(raw.get("industry_code") or "").strip().zfill(6)
        names = tuple(str(raw.get(field) or "").strip() for field in ("l1_name", "l2_name", "l3_name"))
        if not code.isdigit() or len(code) != 6 or not all(names):
            continue
        identity = TaxonomyIdentity(
            l1_code=f"{code[:2]}0000",
            l1_name=names[0],
            l2_code=f"{code[:4]}00",
            l2_name=names[1],
            l3_code=code,
            l3_name=names[2],
        )
        existing = identities.get(code)
        if existing is not None and existing != identity:
            conflicts.add(code)
        identities[code] = identity
    if conflicts:
        raise IndustryPitContractError(f"catalog identity conflicts: {sorted(conflicts)}")
    if not identities:
        raise IndustryPitContractError("taxonomy catalog contains no complete L3 identities")
    payload = {
        "schema_version": TAXONOMY_CATALOG_SCHEMA,
        "contract_id": contract_id,
        "version": version,
        "source_sha256": source_sha256,
        "identities": {code: identities[code].as_dict() for code in sorted(identities)},
    }
    return TaxonomyCatalog(
        contract_id=contract_id,
        version=version,
        source_sha256=source_sha256,
        identities=identities,
        catalog_hash=sha256_hex(canonical_json_bytes(payload)),
    )


def _parse_date(value: Any, *, field: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    try:
        return date.fromisoformat(text[:10])
    except ValueError as exc:
        raise IndustryPitContractError(f"{field} is invalid: {value!r}") from exc


def _lineage_datetime(value: Any) -> str | None:
    if value is None or str(value).strip() in {"", "NaT", "nan", "None"}:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).strip())
        except ValueError as exc:
            raise IndustryPitContractError(f"source_last_updated_at is invalid: {value!r}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=SHANGHAI_TZ)
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _spans_by_symbol(denominator: FrozenDenominator) -> Mapping[str, tuple[UniverseSpan, ...]]:
    grouped: dict[str, list[UniverseSpan]] = defaultdict(list)
    for span in denominator.universe_spans:
        grouped[span.canonical_symbol].append(span)
    return {symbol: tuple(values) for symbol, values in grouped.items()}


def _numeric_symbol_registry(denominator: FrozenDenominator) -> Mapping[str, str]:
    grouped: dict[str, set[str]] = defaultdict(set)
    for span in denominator.universe_spans:
        grouped[span.canonical_symbol[:6]].add(span.canonical_symbol)
    conflicts = {code: values for code, values in grouped.items() if len(values) != 1}
    if conflicts:
        raise IndustryPitContractError(f"numeric symbol maps to multiple canonical symbols: {sorted(conflicts)}")
    return {code: next(iter(values)) for code, values in grouped.items()}


def _known_from(valid_from: date, *, next_valid_from: date | None) -> date | None:
    if valid_from == SW2021_CLASSIFICATION_VALID_FROM:
        return SW2021_CAUSAL_DAILY_FROM
    if next_valid_from == SW2021_CLASSIFICATION_VALID_FROM:
        # The immediately superseded classification is proven to be the
        # pre-launch identity at the taxonomy transition boundary.  Earlier
        # first-publication time is not inferred from 计入日期 or 更新日期.
        return SW2021_CLASSIFICATION_VALID_FROM
    return None


def build_classification_intervals(
    history_rows: Iterable[Mapping[str, Any]],
    *,
    catalog: TaxonomyCatalog,
    receipt: AuthorityReceipt,
    denominator: FrozenDenominator,
    classification_source_hash: str,
) -> tuple[tuple[CandidateInterval, ...], Mapping[str, Any]]:
    if receipt.authority_type is not AuthorityType.CLASSIFICATION:
        raise IndustryPitContractError("classification builder received the wrong authority receipt")
    registry = _numeric_symbol_registry(denominator)
    spans_by_symbol = _spans_by_symbol(denominator)
    normalized: dict[str, list[dict[str, Any]]] = defaultdict(list)
    unmapped_source_symbols: set[str] = set()
    exact_seen: set[bytes] = set()
    exact_duplicate_count = 0
    for raw in history_rows:
        numeric = str(raw.get("stock_code") or "").strip().split(".")[0].zfill(6)
        symbol = registry.get(numeric)
        if symbol is None:
            unmapped_source_symbols.add(numeric)
            continue
        row = {
            "stock_code": numeric,
            "classification_valid_from": _parse_date(
                raw.get("classification_valid_from"), field="classification_valid_from"
            ).isoformat(),
            "industry_code": str(raw.get("industry_code") or "").strip().zfill(6),
            "source_last_updated_at": _lineage_datetime(raw.get("source_last_updated_at")),
        }
        encoded = canonical_json_bytes(row)
        if encoded in exact_seen:
            exact_duplicate_count += 1
            continue
        exact_seen.add(encoded)
        row["lineage_hash"] = sha256_hex(encoded)
        normalized[symbol].append(row)

    output: list[CandidateInterval] = []
    same_boundary_conflicts = 0
    invalid_catalog_rows = 0
    for symbol in sorted(normalized):
        by_date: dict[date, list[dict[str, Any]]] = defaultdict(list)
        for row in normalized[symbol]:
            by_date[date.fromisoformat(row["classification_valid_from"])].append(row)
        boundaries = sorted(by_date)
        for index, valid_from in enumerate(boundaries):
            next_valid = boundaries[index + 1] if index + 1 < len(boundaries) else None
            rows = by_date[valid_from]
            identities = {row["industry_code"] for row in rows}
            known_from = _known_from(valid_from, next_valid_from=next_valid)
            if next_valid is None:
                causal_end = None
            else:
                following = boundaries[index + 2] if index + 2 < len(boundaries) else None
                next_known = _known_from(next_valid, next_valid_from=following)
                causal_end = next_known if next_known is not None else next_valid
            if len(identities) != 1:
                same_boundary_conflicts += 1
                reason = UnavailableReason.SAME_BOUNDARY_IDENTITY_CONFLICT
                identity = None
                conflicts = [
                    {
                        "industry_code": row["industry_code"],
                        "lineage_hash": row["lineage_hash"],
                    }
                    for row in rows
                ]
                causal_start = valid_from
            else:
                code = next(iter(identities))
                identity = catalog.identities.get(code)
                conflicts = []
                if identity is None:
                    invalid_catalog_rows += 1
                    reason = UnavailableReason.CATALOG_IDENTITY_INVALID
                    causal_start = valid_from
                elif known_from is None:
                    reason = UnavailableReason.CLASSIFICATION_KNOWLEDGE_TIME_UNVERIFIED
                    causal_start = valid_from
                    identity = None
                else:
                    reason = None
                    causal_start = known_from
            source_updated_values = sorted(
                {row["source_last_updated_at"] for row in rows if row["source_last_updated_at"]}
            )
            source_last_updated = source_updated_values[-1] if source_updated_values else None
            lineage_hashes = [row["lineage_hash"] for row in rows]
            for span in spans_by_symbol[symbol]:
                eligible_from = max(span.eligible_start, denominator.window_start)
                eligible_to = min(
                    span.end_exclusive or denominator.window_end + timedelta(days=1),
                    denominator.window_end + timedelta(days=1),
                )
                if eligible_to <= eligible_from:
                    continue
                output.append(
                    make_candidate_interval(
                        canonical_symbol=symbol,
                        authority_type=AuthorityType.CLASSIFICATION,
                        taxonomy_contract_id=catalog.contract_id,
                        taxonomy_version=catalog.version,
                        authority_receipt_hash=receipt.receipt_hash,
                        valid_from=valid_from,
                        valid_to_exclusive=next_valid,
                        eligible_from=eligible_from,
                        eligible_to_exclusive=eligible_to,
                        causal_use_from=causal_start,
                        causal_use_to_exclusive=causal_end,
                        known_from=known_from,
                        source_effective_field="计入日期",
                        source_last_updated_at=source_last_updated,
                        research_basis=receipt.research_basis,
                        non_as_known_taxonomy=False,
                        identity=identity,
                        authority_identity=(
                            {
                                "classification_l1_code": identity.l1_code,
                                "classification_l2_code": identity.l2_code,
                                "classification_l3_code": identity.l3_code,
                            }
                            if identity is not None
                            else {}
                        ),
                        unavailable_reason=reason,
                        conflict_candidates=conflicts,
                        source_ids=receipt.source_ids,
                        source_hashes=receipt.source_hashes,
                        lineage_hashes=lineage_hashes,
                    )
                )
    diagnostics = {
        "source_row_count": len(exact_seen) + exact_duplicate_count,
        "exact_duplicate_collapsed": exact_duplicate_count,
        "same_boundary_identity_conflict": same_boundary_conflicts,
        "catalog_identity_invalid": invalid_catalog_rows,
        "source_symbol_unmapped_to_frozen_universe": len(unmapped_source_symbols),
        "candidate_interval_count": len(output),
        "classification_source_sha256": classification_source_hash,
    }
    return tuple(output), diagnostics


def build_index_membership_intervals(
    evidence_rows: Iterable[Mapping[str, Any]],
    *,
    catalog: TaxonomyCatalog,
    receipt: AuthorityReceipt,
    denominator: FrozenDenominator,
) -> tuple[tuple[CandidateInterval, ...], Mapping[str, Any]]:
    if receipt.authority_type is not AuthorityType.INDEX_MEMBERSHIP:
        raise IndustryPitContractError("index membership builder received the wrong authority receipt")
    spans_by_symbol = _spans_by_symbol(denominator)
    output: list[CandidateInterval] = []
    evidence_count = 0
    for raw in evidence_rows:
        symbol = require_symbol(str(raw.get("canonical_symbol") or ""))
        if symbol not in spans_by_symbol:
            continue
        enter = _parse_date(raw.get("membership_enter_date"), field="membership_enter_date")
        exit_exclusive = (
            _parse_date(raw.get("membership_exit_date_exclusive"), field="membership_exit_date_exclusive")
            if raw.get("membership_exit_date_exclusive")
            else None
        )
        known_from = _parse_date(raw.get("known_from"), field="known_from")
        if exit_exclusive is not None and exit_exclusive <= enter:
            raise IndustryPitContractError("index membership evidence interval is inverted")
        identity = catalog.identities.get(str(raw.get("industry_code") or "").strip().zfill(6))
        if identity is None:
            raise IndustryPitContractError("index membership evidence references an unknown catalog identity")
        index_identity = {
            field: str(raw.get(field) or "").strip()
            for field in ("index_l1_code", "index_l2_code", "index_l3_code")
        }
        if not all(index_identity.values()):
            raise IndustryPitContractError("index membership evidence requires exact L1/L2/L3 index codes")
        source_hash = str(raw.get("source_sha256") or "").strip()
        lineage = sha256_hex(canonical_json_bytes(dict(raw)))
        evidence_count += 1
        for span in spans_by_symbol[symbol]:
            eligible_from = max(span.eligible_start, denominator.window_start)
            eligible_to = min(
                span.end_exclusive or denominator.window_end + timedelta(days=1),
                denominator.window_end + timedelta(days=1),
            )
            if eligible_to <= eligible_from:
                continue
            output.append(
                make_candidate_interval(
                    canonical_symbol=symbol,
                    authority_type=AuthorityType.INDEX_MEMBERSHIP,
                    taxonomy_contract_id=catalog.contract_id,
                    taxonomy_version=catalog.version,
                    authority_receipt_hash=receipt.receipt_hash,
                    valid_from=enter,
                    valid_to_exclusive=exit_exclusive,
                    eligible_from=eligible_from,
                    eligible_to_exclusive=eligible_to,
                    causal_use_from=max(enter, known_from),
                    causal_use_to_exclusive=exit_exclusive,
                    known_from=known_from,
                    source_effective_field="membership_enter_date/membership_exit_date_exclusive",
                    source_last_updated_at=None,
                    research_basis=receipt.research_basis,
                    non_as_known_taxonomy=False,
                    identity=identity,
                    authority_identity=index_identity,
                    unavailable_reason=None,
                    source_ids=tuple(
                        sorted({*receipt.source_ids, str(raw.get("source_url") or "")} - {""})
                    ),
                    source_hashes=tuple(sorted({*receipt.source_hashes, source_hash})),
                    lineage_hashes=(lineage,),
                )
            )
    diagnostics = {
        "authoritative_evidence_row_count": evidence_count,
        "candidate_interval_count": len(output),
        "missing_evidence_policy": UnavailableReason.MEMBERSHIP_BOUNDARY_UNAVAILABLE.value,
    }
    return tuple(output), diagnostics


def full_denominator_preflight(
    *,
    denominator: FrozenDenominator,
    classification_resolver: IndustryPitResolver,
    index_membership_resolver: IndustryPitResolver,
    conflict_inventory: Mapping[str, Mapping[str, Any]],
    mandatory_symbols: Sequence[str],
) -> Mapping[str, Any]:
    classification_counts: Counter[str] = Counter()
    index_counts: Counter[str] = Counter()
    alignment_counts: Counter[str] = Counter()
    reason_counts: Counter[str] = Counter()
    reason_date_deltas: dict[str, list[int]] = {}
    reason_by_sector: dict[str, Counter[str]] = defaultdict(Counter)
    regression: dict[str, Counter[str]] = {symbol: Counter() for symbol in sorted(conflict_inventory)}
    mandatory_details: dict[str, list[Mapping[str, Any]]] = {
        require_symbol(symbol): [] for symbol in mandatory_symbols
    }

    classification_receipt = classification_resolver.receipt
    index_receipt = index_membership_resolver.receipt
    total = 0
    global_dates = denominator.trading_dates
    mandatory_days = (
        date(2021, 7, 30),
        date(2021, 8, 2),
        date(2021, 12, 10),
        date(2021, 12, 13),
    )

    def resolve_day(symbol: str, trade_date: date):
        return resolve_dual_authority(
            classification_resolver=classification_resolver,
            index_membership_resolver=index_membership_resolver,
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

    for span in denominator.universe_spans:
        symbol = span.canonical_symbol
        span_dates = denominator.dates_for_span(span)
        if not span_dates:
            continue
        global_start = bisect_left(global_dates, span_dates[0])
        boundaries = {0, len(span_dates)}
        transition_dates = {
            *classification_resolver.transition_dates(symbol),
            *index_membership_resolver.transition_dates(symbol),
        }
        for transition in transition_dates:
            offset = bisect_left(span_dates, transition)
            if 0 < offset < len(span_dates):
                boundaries.add(offset)
        ordered_boundaries = sorted(boundaries)
        for left, right in zip(ordered_boundaries, ordered_boundaries[1:]):
            if left == right:
                continue
            segment_count = right - left
            total += segment_count
            result = resolve_day(symbol, span_dates[left])
            classification_counts[result.classification.status] += segment_count
            index_counts[result.index_membership.status] += segment_count
            alignment_counts[result.alignment_state.value] += segment_count
            sector = "unavailable"
            if isinstance(result.classification, ResolvedIndustryIdentity):
                sector = result.classification.identity.l1_code
            for authority_result in (result.classification, result.index_membership):
                if isinstance(authority_result, UnavailableIndustryIdentity):
                    key = f"{authority_result.authority_type.value}:{authority_result.reason.value}"
                    reason_counts[key] += segment_count
                    reason_by_sector[sector][key] += segment_count
                    deltas = reason_date_deltas.setdefault(key, [0] * (len(global_dates) + 1))
                    deltas[global_start + left] += 1
                    deltas[global_start + right] -= 1
            if symbol in regression:
                regression[symbol][f"classification_{result.classification.status}"] += segment_count
                regression[symbol][f"index_{result.index_membership.status}"] += segment_count
                regression[symbol][f"alignment_{result.alignment_state.value}"] += segment_count
        if symbol in mandatory_details:
            for mandatory_day in mandatory_days:
                offset = bisect_left(span_dates, mandatory_day)
                if offset < len(span_dates) and span_dates[offset] == mandatory_day:
                    mandatory_details[symbol].append(resolve_day(symbol, mandatory_day).as_dict())

    reason_by_date: dict[str, dict[str, int]] = defaultdict(dict)
    for key, deltas in sorted(reason_date_deltas.items()):
        active = 0
        for index, trade_date in enumerate(global_dates):
            active += deltas[index]
            if active:
                reason_by_date[trade_date.isoformat()][key] = active

    closure = classification_counts["resolved"] + classification_counts["unavailable"]
    index_closure = index_counts["resolved"] + index_counts["unavailable"]
    if total != denominator.total_opportunities or closure != total or index_closure != total:
        raise IndustryPitContractError(
            "full denominator closure failed: "
            f"observed={total} expected={denominator.total_opportunities} "
            f"classification={closure} index={index_closure}"
        )
    compact = {
        "schema_version": PREFLIGHT_REPORT_SCHEMA,
        "window_start": denominator.window_start.isoformat(),
        "window_end": denominator.window_end.isoformat(),
        "denominator_digest": denominator.digest,
        "total_opportunities": total,
        "classification": dict(sorted(classification_counts.items())),
        "index_membership": dict(sorted(index_counts.items())),
        "alignment": dict(sorted(alignment_counts.items())),
        "coverage": {
            "classification_resolved_ratio": format(
                Decimal(classification_counts["resolved"]) / Decimal(total), ".12f"
            ),
            "index_membership_resolved_ratio": format(
                Decimal(index_counts["resolved"]) / Decimal(total), ".12f"
            ),
            "alignment_aligned_ratio": format(Decimal(alignment_counts["aligned"]) / Decimal(total), ".12f"),
        },
        "unavailable_by_reason": dict(sorted(reason_counts.items())),
        "unavailable_by_date": {day: dict(sorted(values.items())) for day, values in sorted(reason_by_date.items())},
        "unavailable_by_sector": {
            sector: dict(sorted(values.items())) for sector, values in sorted(reason_by_sector.items())
        },
        "conflict_inventory": {
            symbol: {
                **dict(conflict_inventory[symbol]),
                "candidate_counts": dict(sorted(regression[symbol].items())),
                "classification_candidates": list(classification_resolver.candidate_summary(symbol)),
                "index_membership_candidates": list(index_membership_resolver.candidate_summary(symbol)),
            }
            for symbol in sorted(conflict_inventory)
        },
        "mandatory_regression": {
            symbol: values for symbol, values in sorted(mandatory_details.items())
        },
        "closure": {
            "classification_resolved_plus_unavailable": closure,
            "index_resolved_plus_unavailable": index_closure,
            "expected_denominator": total,
            "passed": True,
        },
    }
    compact["canonical_hash"] = digest_named_fields(PREFLIGHT_REPORT_SCHEMA, compact)
    return compact


__all__ = [
    "SW2021_CAUSAL_DAILY_FROM",
    "SW2021_CLASSIFICATION_VALID_FROM",
    "SW2021_INDEX_SWITCH_FROM",
    "FrozenDenominator",
    "TaxonomyCatalog",
    "UniverseSpan",
    "build_classification_intervals",
    "build_index_membership_intervals",
    "build_taxonomy_catalog",
    "full_denominator_preflight",
]
