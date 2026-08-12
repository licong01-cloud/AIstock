"""Deterministic PIT Shenwan L2 enrichment over already-frozen source rows."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from backend.services.industry_code_map import build_sw_l2_code_map

from .canonical import digest_named_fields
from .errors import SourceManifestError


SECTOR_ENRICHMENT_SCHEMA = "dataset_release_sector_l2_enrichment_v1"
UNKNOWN_L2_CODE_ID = -1


@dataclass(frozen=True, slots=True)
class _MembershipSpan:
    ts_code: str
    in_date: date
    out_date: date | None
    l2_code: str


@dataclass(frozen=True, slots=True)
class FrozenSectorEnricher:
    code_map: Mapping[str, int]
    memberships: Mapping[str, tuple[_MembershipSpan, ...]]

    @classmethod
    def build(
        cls,
        classify_rows: Iterable[Mapping[str, Any]],
        member_rows: Iterable[Mapping[str, Any]],
    ) -> "FrozenSectorEnricher":
        classify_codes: list[str] = []
        for row in classify_rows:
            code = str(row.get("index_code", "")).strip()
            level = str(row.get("level", "")).strip().upper()
            if not code or level != "L2":
                raise SourceManifestError("frozen SW classification row is not a valid L2 code")
            classify_codes.append(code)
        code_map = build_sw_l2_code_map(classify_codes)
        if not code_map or len(code_map) > 32_767:
            raise SourceManifestError("frozen SW L2 code map is empty or exceeds int16")

        grouped: dict[str, list[_MembershipSpan]] = {}
        seen: set[tuple[str, date, date | None, str]] = set()
        for row in member_rows:
            ts_code = str(row.get("ts_code", "")).strip().upper()
            l2_code = str(row.get("l2_code", "")).strip()
            if not ts_code or not l2_code:
                raise SourceManifestError("frozen SW member identity is incomplete")
            if l2_code not in code_map:
                raise SourceManifestError("frozen SW member references an unknown L2 classification")
            in_date = _as_date(row.get("in_date"), field="in_date")
            out_date = _as_optional_date(row.get("out_date"), field="out_date")
            if out_date is not None and out_date < in_date:
                raise SourceManifestError("frozen SW member interval is inverted")
            identity = (ts_code, in_date, out_date, l2_code)
            if identity in seen:
                raise SourceManifestError("frozen SW member row is duplicated")
            seen.add(identity)
            grouped.setdefault(ts_code, []).append(_MembershipSpan(ts_code, in_date, out_date, l2_code))
        memberships = {
            code: tuple(
                sorted(
                    values,
                    key=lambda item: (
                        item.in_date,
                        item.out_date is not None,
                        item.out_date or date.min,
                        item.l2_code,
                    ),
                )
            )
            for code, values in sorted(grouped.items())
        }
        return cls(code_map=code_map, memberships=memberships)

    @property
    def code_map_digest(self) -> str:
        return digest_named_fields(
            "dataset_release_sw_l2_code_map_v1",
            {"ordered_codes": list(self.code_map)},
        )

    @property
    def membership_digest(self) -> str:
        return digest_named_fields(
            "dataset_release_sw_l2_memberships_v1",
            {
                "rows": [
                    {
                        "ts_code": span.ts_code,
                        "in_date": span.in_date,
                        "out_date": span.out_date,
                        "l2_code": span.l2_code,
                    }
                    for values in self.memberships.values()
                    for span in values
                ]
            },
        )

    def enrich(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        ts_code = str(payload.get("ts_code", "")).strip().upper()
        trade_date = _as_date(payload.get("trade_date"), field="trade_date")
        candidates = [
            span
            for span in self.memberships.get(ts_code, ())
            if span.in_date <= trade_date and (span.out_date is None or span.out_date >= trade_date)
        ]
        selected: _MembershipSpan | None = None
        if candidates:
            candidates.sort(
                key=lambda item: (
                    item.in_date,
                    item.out_date is not None,
                    item.out_date or date.min,
                    item.l2_code,
                ),
                reverse=True,
            )
            selected = candidates[0]
            top_key = (
                selected.in_date,
                selected.out_date is not None,
                selected.out_date or date.min,
            )
            ambiguous = {
                item.l2_code
                for item in candidates
                if (
                    item.in_date,
                    item.out_date is not None,
                    item.out_date or date.min,
                )
                == top_key
            }
            if len(ambiguous) > 1:
                raise SourceManifestError("frozen SW membership has an ambiguous PIT winner")
        # ``-1`` is reserved for a date with no valid PIT membership.  A member
        # interval that points at a missing classification is an inconsistent
        # frozen authority and is rejected by ``build`` above; never collapse
        # that distinct failure into the missing-membership sentinel.
        l2_code_id = int(self.code_map[selected.l2_code]) if selected is not None else UNKNOWN_L2_CODE_ID
        return {**dict(payload), "l2_code_id": l2_code_id}

    def receipt(
        self,
        *,
        classify_partitions: Sequence[Mapping[str, Any]],
        member_partitions: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        return {
            "schema_version": SECTOR_ENRICHMENT_SCHEMA,
            "asof_policy": "in_date_lte_trade_date_out_date_null_or_gte_v1",
            "winner_policy": "in_date_desc_out_date_desc_nulls_last_v1",
            "mapping_policy": "sorted_unique_sw_l2_zero_based_v1",
            "unknown_l2_code_id": UNKNOWN_L2_CODE_ID,
            "code_count": len(self.code_map),
            "code_map_digest": self.code_map_digest,
            "membership_digest": self.membership_digest,
            "classify_partitions": [dict(value) for value in classify_partitions],
            "member_partitions": [dict(value) for value in member_partitions],
            "safety": {
                "database_writes": 0,
                "provider_database_writes": 0,
                "production_writes": 0,
                "production_deletes": 0,
                "production_pointer_changes": 0,
                "service_process_controls": 0,
                "candidate_writes": 0,
            },
        }


def _as_date(value: Any, *, field: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError) as exc:
        raise SourceManifestError(f"frozen SW {field} is invalid") from exc


def _as_optional_date(value: Any, *, field: str) -> date | None:
    if value in {None, ""}:
        return None
    return _as_date(value, field=field)


__all__ = [
    "FrozenSectorEnricher",
    "SECTOR_ENRICHMENT_SCHEMA",
    "UNKNOWN_L2_CODE_ID",
]
