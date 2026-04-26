"""Industry lookup providers for Selection Center runtime filters.

The authoritative PIT stock-to-industry mapping for runtime selection comes
from Tushare ``index_member_all`` data stored in ``market.sw_index_member``.
This module is intentionally local to Selection/Paper v2 so it does not change
the legacy ``backend/data_service`` semantics.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Callable, Iterator, Protocol

from backend.db.pg_pool import get_conn
from backend.services.trading_core.errors import DataUnavailableError

ConnFactory = Callable[[], Iterator[Any]]


@dataclass(frozen=True)
class IndustryInfo:
    """Point-in-time Shenwan industry mapping for one stock."""

    symbol: str
    l1_code: str | None = None
    l1_name: str | None = None
    l2_code: str | None = None
    l2_name: str | None = None
    l3_code: str | None = None
    l3_name: str | None = None
    in_date: date | None = None
    out_date: date | None = None
    source: str = "market.sw_index_member"

    def values_by_level(self) -> dict[str, str]:
        values: dict[str, str] = {}
        for key in ("l1_code", "l1_name", "l2_code", "l2_name", "l3_code", "l3_name"):
            value = getattr(self, key)
            if value is not None and str(value).strip():
                values[key] = str(value).strip()
        return values

    def match_blacklist(self, blacklist: list[str]) -> tuple[str, str] | None:
        """Return ``(blacklist_item, matched_field)`` for an exact match."""

        normalized = {_normalize_match_key(item): item for item in blacklist if str(item or "").strip()}
        if not normalized:
            return None
        for field, value in self.values_by_level().items():
            key = _normalize_match_key(value)
            if key in normalized:
                return normalized[key], field
        return None

    def to_context(self) -> dict[str, Any]:
        return {
            "l1_code": self.l1_code,
            "l1_name": self.l1_name,
            "l2_code": self.l2_code,
            "l2_name": self.l2_name,
            "l3_code": self.l3_code,
            "l3_name": self.l3_name,
            "in_date": self.in_date.isoformat() if self.in_date else None,
            "out_date": self.out_date.isoformat() if self.out_date else None,
            "industry_source": self.source,
        }


class IndustryLookupProvider(Protocol):
    def get_industries(self, symbols: list[str], trade_date: date) -> dict[str, IndustryInfo]:
        ...


class DbSwIndustryLookupProvider:
    """Batch PIT lookup in ``market.sw_index_member``."""

    def __init__(self, conn_factory: ConnFactory | None = None) -> None:
        self._conn_factory = conn_factory or get_conn

    def get_industries(self, symbols: list[str], trade_date: date) -> dict[str, IndustryInfo]:
        normalized = _dedupe_symbols(symbols)
        if not normalized:
            return {}
        try:
            with self._conn_factory() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        WITH ranked AS (
                            SELECT
                                ts_code, l1_code, l1_name, l2_code, l2_name,
                                l3_code, l3_name, in_date, out_date,
                                ROW_NUMBER() OVER (
                                    PARTITION BY ts_code
                                    ORDER BY in_date DESC NULLS LAST,
                                             out_date DESC NULLS LAST,
                                             l3_code NULLS LAST
                                ) AS rn
                            FROM market.sw_index_member
                            WHERE ts_code = ANY(%s)
                              AND in_date <= %s
                              AND (out_date IS NULL OR out_date >= %s)
                        )
                        SELECT ts_code, l1_code, l1_name, l2_code, l2_name,
                               l3_code, l3_name, in_date, out_date
                        FROM ranked
                        WHERE rn = 1
                        ORDER BY ts_code
                        """,
                        (normalized, trade_date, trade_date),
                    )
                    rows = cur.fetchall()
        except Exception as exc:
            raise DataUnavailableError(
                "sw_index_member industry lookup failed",
                context={"trade_date": trade_date.isoformat(), "symbol_count": len(normalized)},
            ) from exc

        result: dict[str, IndustryInfo] = {}
        for row in rows:
            (
                symbol,
                l1_code,
                l1_name,
                l2_code,
                l2_name,
                l3_code,
                l3_name,
                in_date,
                out_date,
            ) = row
            result[str(symbol)] = IndustryInfo(
                symbol=str(symbol),
                l1_code=_clean(l1_code),
                l1_name=_clean(l1_name),
                l2_code=_clean(l2_code),
                l2_name=_clean(l2_name),
                l3_code=_clean(l3_code),
                l3_name=_clean(l3_name),
                in_date=in_date,
                out_date=out_date,
                source="market.sw_index_member",
            )
        return result


def industry_info_from_candidate(symbol: str, component_scores: dict[str, Any] | None) -> IndustryInfo | None:
    """Build industry info from explicit candidate metadata when present."""

    scores = component_scores or {}
    l1_code = _first(scores, "l1_code", "sw_l1_code", "industry_code_l1")
    l1_name = _first(
        scores,
        "l1_name",
        "sw_l1_name",
        "industry",
        "industry_name",
        "sector",
        "sw_l1",
        "cs_industry",
    )
    l2_code = _first(scores, "l2_code", "sw_l2_code", "industry_code_l2")
    l2_name = _first(scores, "l2_name", "sw_l2_name", "sw2", "sw2_name")
    l3_code = _first(scores, "l3_code", "sw_l3_code", "industry_code_l3")
    l3_name = _first(scores, "l3_name", "sw_l3_name")
    if not any((l1_code, l1_name, l2_code, l2_name, l3_code, l3_name)):
        return None
    return IndustryInfo(
        symbol=symbol,
        l1_code=l1_code,
        l1_name=l1_name,
        l2_code=l2_code,
        l2_name=l2_name,
        l3_code=l3_code,
        l3_name=l3_name,
        source="candidate.component_scores",
    )


def _dedupe_symbols(symbols: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for symbol in symbols:
        text = str(symbol or "").strip()
        if text and text not in seen:
            result.append(text)
            seen.add(text)
    return result


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _first(scores: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = _clean(scores.get(key))
        if value:
            return value
    return None


def _normalize_match_key(value: Any) -> str:
    return str(value or "").strip().casefold()
