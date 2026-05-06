from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any, Iterable, Optional, Sequence

import numpy as np
import pandas as pd
import psycopg2.extras as pgx

from ...db.pg_pool import get_conn
from ..stock_universe_pit_service import (
    DEFAULT_ST_PIT_RULE_VERSION,
    DEFAULT_ST_PIT_START_DATE,
    DEFAULT_ST_PIT_UNIVERSE_KEY,
    StockUniversePitService,
)


OFFICIAL_FACTOR_UNIVERSE_KEY = DEFAULT_ST_PIT_UNIVERSE_KEY
OFFICIAL_FACTOR_UNIVERSE_RULE_VERSION = DEFAULT_ST_PIT_RULE_VERSION
OFFICIAL_FACTOR_SNAPSHOT_UNIVERSE_MODE = "st_pit_window_union_v1"
OFFICIAL_FACTOR_INDEX_POLICY = "st_pit_buy_eligible_reindexed_v1"
OFFICIAL_FACTOR_COVERAGE_SEMANTICS = "st_pit_buy_eligible_suspend_excluded_non_warmup_v1"


def _as_date(value: str | dt.date | pd.Timestamp | None) -> dt.date:
    if value is None:
        raise ValueError("date value is required")
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return pd.Timestamp(value).date()


def _normalize_instrument(value: Any) -> str:
    text = str(value).strip().upper()
    if "." in text:
        return text
    if len(text) >= 8 and text[:2] in {"SH", "SZ", "BJ"}:
        return f"{text[2:]}.{text[:2]}"
    return text


@dataclass(frozen=True)
class FactorUniverseMetadata:
    universe_key: str
    universe_rule_version: str
    universe_scope: str | None
    universe_fingerprint_sha256: str
    stock_universe_mode: str
    snapshot_universe_mode: str
    index_policy: str
    coverage_semantics: str
    universe_start_date: str | None
    universe_end_date: str | None
    universe_generated_at: str | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "universe_key": self.universe_key,
            "universe_rule_version": self.universe_rule_version,
            "universe_scope": self.universe_scope,
            "universe_fingerprint_sha256": self.universe_fingerprint_sha256,
            "stock_universe_mode": self.stock_universe_mode,
            "snapshot_universe_mode": self.snapshot_universe_mode,
            "index_policy": self.index_policy,
            "coverage_semantics": self.coverage_semantics,
            "universe_start_date": self.universe_start_date,
            "universe_end_date": self.universe_end_date,
            "universe_generated_at": self.universe_generated_at,
        }


class FactorUniverseMaskService:
    """Shared ST PIT universe service for factor metrics and caches."""

    def __init__(self, pit_service: Optional[StockUniversePitService] = None) -> None:
        self._pit_service = pit_service or StockUniversePitService()

    def ensure_ready(
        self,
        *,
        start_date: str | dt.date,
        end_date: str | dt.date,
        universe_key: str = OFFICIAL_FACTOR_UNIVERSE_KEY,
        strict: bool = True,
    ) -> dict[str, Any]:
        return self._pit_service.ensure_st_pit_universe(
            universe_key=universe_key,
            start_date=_as_date(start_date),
            end_date=_as_date(end_date),
            rule_version=OFFICIAL_FACTOR_UNIVERSE_RULE_VERSION,
            strict=strict,
            rebuild_if_stale=True,
        )

    def get_state(
        self,
        *,
        universe_key: str = OFFICIAL_FACTOR_UNIVERSE_KEY,
    ) -> dict[str, Any]:
        return self._pit_service.get_status(universe_key=universe_key)

    def metadata(
        self,
        *,
        start_date: str | dt.date,
        end_date: str | dt.date,
        universe_key: str = OFFICIAL_FACTOR_UNIVERSE_KEY,
        ensure: bool = True,
    ) -> dict[str, Any]:
        if ensure:
            self.ensure_ready(start_date=start_date, end_date=end_date, universe_key=universe_key)
        state = self.get_state(universe_key=universe_key)
        if state.get("status") != "ready" or state.get("dirty"):
            raise RuntimeError(f"ST PIT universe is not ready: {state}")
        meta = FactorUniverseMetadata(
            universe_key=universe_key,
            universe_rule_version=str(state.get("rule_version") or OFFICIAL_FACTOR_UNIVERSE_RULE_VERSION),
            universe_scope=state.get("scope"),
            universe_fingerprint_sha256=str(state.get("source_fingerprint_sha256") or ""),
            stock_universe_mode="pit_spans",
            snapshot_universe_mode=OFFICIAL_FACTOR_SNAPSHOT_UNIVERSE_MODE,
            index_policy=OFFICIAL_FACTOR_INDEX_POLICY,
            coverage_semantics=OFFICIAL_FACTOR_COVERAGE_SEMANTICS,
            universe_start_date=str(state.get("start_date")) if state.get("start_date") else None,
            universe_end_date=str(state.get("end_date")) if state.get("end_date") else None,
            universe_generated_at=str(state.get("generated_at")) if state.get("generated_at") else None,
        )
        return meta.as_dict()

    def get_window_union_instruments(
        self,
        *,
        start_date: str | dt.date,
        end_date: str | dt.date,
        universe_key: str = OFFICIAL_FACTOR_UNIVERSE_KEY,
        ensure: bool = True,
    ) -> list[str]:
        start = _as_date(start_date)
        end = _as_date(end_date)
        if ensure:
            self.ensure_ready(start_date=start, end_date=end, universe_key=universe_key)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT ts_code
                      FROM market.stock_universe_pit_spans
                     WHERE universe_key = %s
                       AND eligible_start <= %s
                       AND eligible_end >= %s
                     ORDER BY ts_code
                    """,
                    (universe_key, end, start),
                )
                return [str(row[0]) for row in cur.fetchall()]

    def load_spans(
        self,
        *,
        start_date: str | dt.date,
        end_date: str | dt.date,
        universe_key: str = OFFICIAL_FACTOR_UNIVERSE_KEY,
        instruments: Optional[Iterable[str]] = None,
        ensure: bool = True,
    ) -> list[dict[str, Any]]:
        start = _as_date(start_date)
        end = _as_date(end_date)
        if ensure:
            self.ensure_ready(start_date=start, end_date=end, universe_key=universe_key)
        params: list[Any] = [universe_key, end, start]
        inst_sql = ""
        if instruments is not None:
            normalized = sorted({_normalize_instrument(inst) for inst in instruments})
            if not normalized:
                return []
            inst_sql = " AND ts_code = ANY(%s)"
            params.append(normalized)
        with get_conn() as conn:
            with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
                cur.execute(
                    f"""
                    SELECT ts_code, eligible_start, eligible_end,
                           entry_reason, exit_reason, entry_event_date,
                           exit_event_date, terminal_exit, rule_version
                      FROM market.stock_universe_pit_spans
                     WHERE universe_key = %s
                       AND eligible_start <= %s
                       AND eligible_end >= %s
                       {inst_sql}
                     ORDER BY ts_code, eligible_start, eligible_end
                    """,
                    params,
                )
                return [dict(row) for row in cur.fetchall()]

    def build_eligible_mask(
        self,
        dates: Sequence[Any] | pd.DatetimeIndex,
        instruments: Sequence[Any],
        *,
        start_date: str | dt.date | None = None,
        end_date: str | dt.date | None = None,
        universe_key: str = OFFICIAL_FACTOR_UNIVERSE_KEY,
        ensure: bool = True,
    ) -> np.ndarray:
        date_index = pd.DatetimeIndex(dates).normalize()
        inst_list = [_normalize_instrument(inst) for inst in instruments]
        mask = np.zeros((len(date_index), len(inst_list)), dtype=bool)
        if len(date_index) == 0 or not inst_list:
            return mask

        start = _as_date(start_date) if start_date is not None else date_index.min().date()
        end = _as_date(end_date) if end_date is not None else date_index.max().date()
        col_by_inst = {inst: i for i, inst in enumerate(inst_list)}
        spans = self.load_spans(
            start_date=start,
            end_date=end,
            universe_key=universe_key,
            instruments=inst_list,
            ensure=ensure,
        )
        for span in spans:
            col = col_by_inst.get(_normalize_instrument(span["ts_code"]))
            if col is None:
                continue
            s = pd.Timestamp(max(_as_date(span["eligible_start"]), start))
            e = pd.Timestamp(min(_as_date(span["eligible_end"]), end))
            row_mask = (date_index >= s) & (date_index <= e)
            if row_mask.any():
                mask[row_mask, col] = True
        return mask

    def build_eligible_index(
        self,
        *,
        start_date: str | dt.date,
        end_date: str | dt.date,
        universe_key: str = OFFICIAL_FACTOR_UNIVERSE_KEY,
        instruments: Optional[Iterable[str]] = None,
        ensure: bool = True,
    ) -> pd.MultiIndex:
        start = _as_date(start_date)
        end = _as_date(end_date)
        if ensure:
            self.ensure_ready(start_date=start, end_date=end, universe_key=universe_key)
        params: list[Any] = [universe_key, start, end]
        inst_sql = ""
        if instruments is not None:
            normalized = sorted({_normalize_instrument(inst) for inst in instruments})
            if not normalized:
                return pd.MultiIndex.from_arrays([[], []], names=["datetime", "instrument"])
            inst_sql = " AND s.ts_code = ANY(%s)"
            params.append(normalized)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT c.cal_date::date AS trade_date, s.ts_code
                      FROM market.trading_calendar c
                      JOIN market.stock_universe_pit_spans s
                        ON c.cal_date BETWEEN s.eligible_start AND s.eligible_end
                     WHERE s.universe_key = %s
                       AND c.is_trading = TRUE
                       AND c.cal_date BETWEEN %s AND %s
                       {inst_sql}
                     ORDER BY c.cal_date, s.ts_code
                    """,
                    params,
                )
                rows = cur.fetchall()
        if not rows:
            return pd.MultiIndex.from_arrays([[], []], names=["datetime", "instrument"])
        return pd.MultiIndex.from_arrays(
            [
                pd.to_datetime([row[0] for row in rows]),
                [str(row[1]) for row in rows],
            ],
            names=["datetime", "instrument"],
        )


def normalize_factor_universe_metadata(meta: Optional[dict[str, Any]]) -> dict[str, Any]:
    meta = dict(meta or {})
    return {
        "universe_key": meta.get("universe_key") or OFFICIAL_FACTOR_UNIVERSE_KEY,
        "universe_rule_version": meta.get("universe_rule_version") or OFFICIAL_FACTOR_UNIVERSE_RULE_VERSION,
        "universe_fingerprint_sha256": meta.get("universe_fingerprint_sha256") or "",
        "index_policy": meta.get("index_policy") or OFFICIAL_FACTOR_INDEX_POLICY,
        "coverage_semantics": meta.get("coverage_semantics") or OFFICIAL_FACTOR_COVERAGE_SEMANTICS,
    }
