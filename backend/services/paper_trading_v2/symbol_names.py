"""Display-only stock-name enrichment for Paper Trading v2.

Names are never part of selection, order generation, matching, or risk logic.
This module resolves names from market reference tables after trading records
already exist so UI/read APIs can show human-readable labels.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Iterable, Iterator

import psycopg2.extras

from backend.db.pg_pool import get_conn

logger = logging.getLogger(__name__)

ConnFactory = Callable[[], Iterator[Any]]


def _normalize_symbols(symbols: Iterable[str | None]) -> list[str]:
    normalized = sorted({str(symbol).strip() for symbol in symbols if str(symbol or "").strip()})
    return normalized


def _symbol_name_from_row(row: dict[str, Any]) -> str | None:
    for key in ("stock_name", "symbol_name"):
        value = row.get(key)
        if value:
            return str(value)
    metadata = row.get("metadata")
    if isinstance(metadata, dict):
        for key in ("stock_name", "symbol_name"):
            value = metadata.get(key)
            if value:
                return str(value)
    return None


class PaperV2SymbolNameResolver:
    """Resolve stock names for display without changing trading decisions."""

    def __init__(self, conn_factory: ConnFactory | None = None) -> None:
        self._conn_factory = conn_factory or get_conn

    def resolve(self, symbols: Iterable[str | None]) -> dict[str, str]:
        normalized = _normalize_symbols(symbols)
        if not normalized:
            return {}
        resolved = self._resolve_from_stock_basic(normalized)
        missing = [symbol for symbol in normalized if symbol not in resolved]
        if missing:
            resolved.update(self._resolve_from_symbol_dim(missing))
        return resolved

    def enrich_rows(self, rows: Iterable[dict[str, Any]], *, symbol_key: str = "symbol") -> list[dict[str, Any]]:
        copied = [dict(row) for row in rows]
        names = self.resolve(row.get(symbol_key) for row in copied)
        for row in copied:
            symbol = str(row.get(symbol_key) or "").strip()
            name = _symbol_name_from_row(row) or names.get(symbol)
            if name:
                row["stock_name"] = name
                row["symbol_name"] = name
        return copied

    def enrich_nested_rows(self, rows: Iterable[dict[str, Any]], names: dict[str, str], *, symbol_key: str = "symbol") -> list[dict[str, Any]]:
        enriched: list[dict[str, Any]] = []
        for row in rows:
            copied = dict(row)
            symbol = str(copied.get(symbol_key) or "").strip()
            name = _symbol_name_from_row(copied) or names.get(symbol)
            if name:
                copied["stock_name"] = name
                copied["symbol_name"] = name
            enriched.append(copied)
        return enriched

    def _resolve_from_stock_basic(self, symbols: list[str]) -> dict[str, str]:
        return self._safe_resolve(
            """
            SELECT ts_code, name
            FROM market.stock_basic
            WHERE ts_code = ANY(%s)
            """,
            symbols,
            source="market.stock_basic",
        )

    def _resolve_from_symbol_dim(self, symbols: list[str]) -> dict[str, str]:
        return self._safe_resolve(
            """
            SELECT ts_code, name
            FROM market.symbol_dim
            WHERE ts_code = ANY(%s)
            """,
            symbols,
            source="market.symbol_dim",
        )

    def _safe_resolve(self, sql: str, symbols: list[str], *, source: str) -> dict[str, str]:
        try:
            with self._conn_factory() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(sql, (symbols,))
                    rows = cur.fetchall()
        except Exception as exc:  # pragma: no cover - display enrichment must fail open.
            logger.debug("Paper v2 symbol-name lookup skipped for %s: %s", source, exc)
            return {}
        result: dict[str, str] = {}
        for row in rows:
            symbol = str(row.get("ts_code") or "").strip()
            name = str(row.get("name") or "").strip()
            if symbol and name:
                result[symbol] = name
        return result
