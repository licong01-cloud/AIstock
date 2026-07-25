"""Read-only PostgreSQL source for C-007-A L1 stock-fact preparation."""

from __future__ import annotations

import hashlib
import heapq
import itertools
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from .state_model_set import StateModelSetError, canonical_json_bytes
from .stock_fact_observation import (
    MIN_COVERAGE,
    L1DailyAggregate,
    ObservationCoverageError,
    aggregate_l1_day,
    build_classification_lookup,
)


@dataclass(frozen=True)
class StockFactSourceSpec:
    universe_key: str
    universe_rule_version: str
    source_start: date
    source_end: date

    def validate(self) -> None:
        if not self.universe_key.strip() or not self.universe_rule_version.strip():
            raise StateModelSetError("universe key/rule version are required")
        if self.source_start > self.source_end:
            raise StateModelSetError("stock-fact source window is invalid")


class PostgresStockFactReader:
    """Stream explicit stock facts from an already read-only connection."""

    def __init__(self, conn: Any, spec: StockFactSourceSpec) -> None:
        spec.validate()
        self._conn = conn
        self.spec = spec

    def validate_source(self) -> dict[str, Any]:
        with self._conn.cursor() as cursor:
            cursor.execute("SHOW transaction_read_only")
            row = cursor.fetchone()
            if not row or str(row[0]).lower() not in {"on", "true"}:
                raise StateModelSetError("stock-fact source connection must be transaction_read_only")
            cursor.execute(
                """
                SELECT universe_key,rule_version,scope,start_date,end_date,status,dirty,
                       source_fingerprint_sha256,generated_at
                FROM market.stock_universe_pit_state WHERE universe_key=%s
                """,
                (self.spec.universe_key,),
            )
            state = cursor.fetchone()
            if not state:
                raise StateModelSetError("requested PIT universe state is missing")
            if (
                str(state[1]) != self.spec.universe_rule_version
                or str(state[5]) != "ready"
                or bool(state[6])
                or state[3] > self.spec.source_start
                or state[4] < self.spec.source_end
            ):
                raise StateModelSetError(
                    "requested PIT universe is not ready/clean or does not cover the source window"
                )
            cursor.execute(
                """
                SELECT table_name,column_name,data_type
                FROM information_schema.columns
                WHERE table_schema='market' AND table_name=ANY(%s)
                ORDER BY table_name,ordinal_position
                """,
                (
                    [
                        "kline_daily_raw",
                        "daily_basic",
                        "moneyflow_ts",
                        "stk_limit",
                        "suspend_d",
                        "trading_calendar",
                        "sw_index_member",
                        "sw_index_classify",
                        "stock_universe_pit_spans",
                    ],
                ),
            )
            column_contract = [tuple(item) for item in cursor.fetchall()]
        return {
            "schema_version": "hmm_risk_postgres_stock_fact_source_v1",
            "universe_key": str(state[0]),
            "universe_rule_version": str(state[1]),
            "universe_scope": str(state[2]),
            "universe_start": state[3].isoformat(),
            "universe_end": state[4].isoformat(),
            "universe_source_fingerprint_sha256": str(state[7] or ""),
            "universe_generated_at": None if state[8] is None else state[8].isoformat(),
            "column_contract_sha256": hashlib.sha256(canonical_json_bytes(column_contract)).hexdigest(),
        }

    def load_classification_lookup(self) -> dict[tuple[str, str], dict[str, str]]:
        with self._conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT level,index_code,industry_code,industry_name
                FROM market.sw_index_classify
                WHERE level IN ('L1','L2')
                ORDER BY level,index_code
                """
            )
            rows = [
                {
                    "level": item[0],
                    "index_code": item[1],
                    "industry_code": item[2],
                    "industry_name": item[3],
                }
                for item in cursor.fetchall()
            ]
        return build_classification_lookup(rows)

    def validate_fact_uniqueness(self) -> None:
        with self._conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT source_name,conflict_groups FROM (
                  SELECT 'kline_daily_raw' source_name,count(*) conflict_groups FROM (
                    SELECT trade_date,ts_code FROM market.kline_daily_raw t
                    WHERE trade_date BETWEEN %s AND %s GROUP BY trade_date,ts_code
                    HAVING count(*)>1 AND count(DISTINCT to_jsonb(t))>1
                  ) q
                  UNION ALL
                  SELECT 'daily_basic',count(*) FROM (
                    SELECT trade_date,ts_code FROM market.daily_basic t
                    WHERE trade_date BETWEEN %s AND %s GROUP BY trade_date,ts_code
                    HAVING count(*)>1 AND count(DISTINCT to_jsonb(t))>1
                  ) q
                  UNION ALL
                  SELECT 'moneyflow_ts',count(*) FROM (
                    SELECT trade_date,ts_code FROM market.moneyflow_ts t
                    WHERE trade_date BETWEEN %s AND %s GROUP BY trade_date,ts_code
                    HAVING count(*)>1 AND count(DISTINCT to_jsonb(t))>1
                  ) q
                  UNION ALL
                  SELECT 'stk_limit',count(*) FROM (
                    SELECT trade_date,ts_code FROM market.stk_limit t
                    WHERE trade_date BETWEEN %s AND %s GROUP BY trade_date,ts_code
                    HAVING count(*)>1 AND count(DISTINCT to_jsonb(t))>1
                  ) q
                ) duplicates WHERE conflict_groups>0
                """,
                (
                    self.spec.source_start,
                    self.spec.source_end,
                    self.spec.source_start - timedelta(days=10),
                    self.spec.source_end,
                    self.spec.source_start,
                    self.spec.source_end,
                    self.spec.source_start,
                    self.spec.source_end,
                ),
            )
            duplicates = cursor.fetchall()
        if duplicates:
            raise StateModelSetError(f"stock-fact source contains conflicting duplicate keys: {duplicates}")

    def iter_mapping_source_rows(self, *, fetch_size: int = 10_000) -> Iterator[dict[str, Any]]:
        cursor = self._conn.cursor(name="hmm_risk_mapping_source")
        cursor.itersize = fetch_size
        cursor.execute(
            """
            WITH calendar AS (
              SELECT cal_date::date trade_date FROM market.trading_calendar
              WHERE is_trading=true AND cal_date BETWEEN %s AND %s
            )
            SELECT c.trade_date,s.ts_code,m.l1_code,m.l2_code,m.in_date,m.out_date,
                   s.eligible_start,s.eligible_end,
                   l1.index_code canonical_l1_code,l1.industry_name canonical_l1_name,
                   l2.index_code canonical_l2_code,l2.industry_name canonical_l2_name
            FROM calendar c
            JOIN market.stock_universe_pit_spans s
              ON s.universe_key=%s AND s.eligible_start<=c.trade_date
             AND (s.eligible_end IS NULL OR s.eligible_end>=c.trade_date)
            JOIN market.sw_index_member m
              ON m.ts_code=s.ts_code AND m.in_date<=c.trade_date
             AND (m.out_date IS NULL OR m.out_date>=c.trade_date)
            JOIN market.sw_index_classify l1
              ON l1.level='L1' AND m.l1_code IN (l1.index_code,l1.industry_code)
            JOIN market.sw_index_classify l2
              ON l2.level='L2' AND m.l2_code IN (l2.index_code,l2.industry_code)
            ORDER BY c.trade_date,s.ts_code,l1.index_code,l2.index_code,m.in_date,m.out_date NULLS LAST
            """,
            (self.spec.source_start, self.spec.source_end, self.spec.universe_key),
        )
        try:
            for row in cursor:
                yield {
                    "trade_date": row[0],
                    "symbol": row[1],
                    "source_l1_code": row[2],
                    "source_l2_code": row[3],
                    "in_date": row[4],
                    "out_date": row[5],
                    "eligible_start": row[6],
                    "eligible_end": row[7],
                    "l1_code": row[8],
                    "l1_name": row[9],
                    "l2_code": row[10],
                    "l2_name": row[11],
                }
        finally:
            cursor.close()

    def iter_stock_fact_rows(
        self,
        *,
        fetch_size: int = 10_000,
        sector_level: str = "L1",
    ) -> Iterator[dict[str, Any]]:
        if sector_level not in {"L1", "L2"}:
            raise StateModelSetError("stock fact read level must be L1 or L2")
        history_start = self.spec.source_start - timedelta(days=60)
        cursor_name = "hmm_risk_stock_fact_source" if sector_level == "L1" else "hmm_risk_stock_fact_source_l2"
        cursor = self._conn.cursor(name=cursor_name)
        cursor.itersize = fetch_size
        order_by = (
            "c.trade_date,c.l1_code,c.ts_code,c.l2_code"
            if sector_level == "L1"
            else "c.trade_date,c.l2_code,c.ts_code,c.l1_code"
        )
        cursor.execute(
            f"""
            WITH calendar_history AS (
              SELECT cal_date::date trade_date,
                     lag(cal_date::date,1) OVER (ORDER BY cal_date) previous_trade_date
              FROM market.trading_calendar
              WHERE is_trading=true AND cal_date BETWEEN %s AND %s
            ), price_base AS (
              SELECT DISTINCT trade_date,ts_code,open_li,high_li,low_li,close_li,volume_hand,amount_li
              FROM market.kline_daily_raw
              WHERE trade_date BETWEEN %s AND %s
            ), price_history AS (
              SELECT trade_date,ts_code,open_li,high_li,low_li,close_li,volume_hand,amount_li,
                     lag(trade_date,1) OVER w previous_price_date,
                     lag(close_li,1) OVER w previous_close_li,
                     lag(trade_date,5) OVER w previous_price_5_date,
                     lag(close_li,5) OVER w previous_close_5_li,
                     lag(trade_date,10) OVER w previous_price_10_date,
                     lag(close_li,10) OVER w previous_close_10_li
              FROM price_base
              WINDOW w AS (PARTITION BY ts_code ORDER BY trade_date)
            ), basic_base AS (
              SELECT DISTINCT trade_date,ts_code,total_mv,circ_mv
              FROM market.daily_basic
              WHERE trade_date BETWEEN %s AND %s
            ), basic_history AS (
              SELECT trade_date,ts_code,total_mv,
                     lag(trade_date,1) OVER w previous_basic_date,
                     lag(circ_mv,1) OVER w previous_circ_mv
              FROM basic_base
              WINDOW w AS (PARTITION BY ts_code ORDER BY trade_date)
            ), moneyflow_base AS (
              SELECT DISTINCT trade_date,ts_code,buy_sm_amount,sell_sm_amount,
                              buy_elg_amount,sell_elg_amount,net_mf_amount
              FROM market.moneyflow_ts
              WHERE trade_date BETWEEN %s AND %s
            ), limit_base AS (
              SELECT DISTINCT trade_date,ts_code,up_limit
              FROM market.stk_limit
              WHERE trade_date BETWEEN %s AND %s
            ), mapping_source AS (
              SELECT p.trade_date,s.ts_code,s.eligible_start,s.eligible_end,
                     l1.index_code l1_code,l1.industry_name l1_name,
                     l2.index_code l2_code,l2.industry_name l2_name
              FROM price_history p
              JOIN market.stock_universe_pit_spans s
                ON s.ts_code=p.ts_code AND s.universe_key=%s AND s.eligible_start<=p.trade_date
               AND (s.eligible_end IS NULL OR s.eligible_end>=p.trade_date)
              JOIN market.sw_index_member m
                ON m.ts_code=s.ts_code AND m.in_date<=p.trade_date
               AND (m.out_date IS NULL OR m.out_date>=p.trade_date)
              JOIN market.sw_index_classify l1
                ON l1.level='L1' AND m.l1_code IN (l1.index_code,l1.industry_code)
              JOIN market.sw_index_classify l2
                ON l2.level='L2' AND m.l2_code IN (l2.index_code,l2.industry_code)
              WHERE p.trade_date BETWEEN %s AND %s
            ), canonical_identity AS (
              SELECT trade_date,ts_code,eligible_start,eligible_end,
                     l1_code,l1_name,l2_code,l2_name
              FROM mapping_source
              GROUP BY trade_date,ts_code,eligible_start,eligible_end,
                       l1_code,l1_name,l2_code,l2_name
            ), counted AS (
              SELECT c.*,count(*) OVER (PARTITION BY trade_date,ts_code) canonical_identity_count
              FROM canonical_identity c
            )
            SELECT c.trade_date,c.ts_code,c.l1_code,c.l1_name,c.l2_code,c.l2_name,
                   c.eligible_start,c.canonical_identity_count,
                   p.open_li,p.high_li,p.low_li,p.close_li,p.volume_hand,p.amount_li,
                   p.previous_price_date,p.previous_close_li,p.previous_price_5_date,p.previous_close_5_li,
                   p.previous_price_10_date,p.previous_close_10_li,
                   db.total_mv,ch.previous_trade_date,db.previous_basic_date,db.previous_circ_mv,
                   mf.buy_sm_amount,mf.sell_sm_amount,mf.buy_elg_amount,mf.sell_elg_amount,mf.net_mf_amount,
                   lim.up_limit
            FROM counted c
            LEFT JOIN calendar_history ch ON ch.trade_date=c.trade_date
            LEFT JOIN price_history p ON p.trade_date=c.trade_date AND p.ts_code=c.ts_code
            LEFT JOIN basic_history db ON db.trade_date=c.trade_date AND db.ts_code=c.ts_code
            LEFT JOIN moneyflow_base mf ON mf.trade_date=c.trade_date AND mf.ts_code=c.ts_code
            LEFT JOIN limit_base lim ON lim.trade_date=c.trade_date AND lim.ts_code=c.ts_code
            ORDER BY {order_by}
            """,
            (
                history_start,
                self.spec.source_end,
                history_start,
                self.spec.source_end,
                history_start,
                self.spec.source_end,
                self.spec.source_start,
                self.spec.source_end,
                self.spec.source_start,
                self.spec.source_end,
                self.spec.universe_key,
                self.spec.source_start,
                self.spec.source_end,
            ),
        )
        try:
            for row in cursor:
                if int(row[7]) != 1:
                    raise StateModelSetError(
                        f"symbol/date resolves to multiple canonical identities: {row[1]}/{row[0]}"
                    )
                eligible_start = row[6]
                previous_close = row[15] if row[14] is not None and row[14] >= eligible_start else None
                previous_close_5 = row[17] if row[16] is not None and row[16] >= eligible_start else None
                previous_close_10 = row[19] if row[18] is not None and row[18] >= eligible_start else None
                previous_circ_mv = (
                    row[23] if row[21] is not None and row[21] == row[22] and row[21] >= eligible_start else None
                )
                yield {
                    "trade_date": row[0],
                    "symbol": row[1],
                    "l1_code": row[2],
                    "l1_name": row[3],
                    "l2_code": row[4],
                    "l2_name": row[5],
                    "is_suspended": False,
                    "open_yuan": _scaled(row[8], 1000.0),
                    "high_yuan": _scaled(row[9], 1000.0),
                    "low_yuan": _scaled(row[10], 1000.0),
                    "close_yuan": _scaled(row[11], 1000.0),
                    "volume_shares": _scaled(row[12], 0.01),
                    "amount_cny": _scaled(row[13], 1000.0),
                    "prev_close_yuan": _scaled(previous_close, 1000.0),
                    "prev_close_5_yuan": _scaled(previous_close_5, 1000.0),
                    "prev_close_10_yuan": _scaled(previous_close_10, 1000.0),
                    "total_mv_cny": _scaled(row[20], 0.0001),
                    "prev_circ_mv_cny": _scaled(previous_circ_mv, 0.0001),
                    "buy_sm_amount_cny": _scaled(row[24], 0.0001),
                    "sell_sm_amount_cny": _scaled(row[25], 0.0001),
                    "buy_elg_amount_cny": _scaled(row[26], 0.0001),
                    "sell_elg_amount_cny": _scaled(row[27], 0.0001),
                    "net_mf_amount_cny": _scaled(row[28], 0.0001),
                    "up_limit_yuan": None if row[29] is None else float(row[29]),
                }
        finally:
            cursor.close()

    def iter_missing_price_rows(
        self,
        *,
        fetch_size: int = 2_000,
        sector_level: str = "L1",
    ) -> Iterator[dict[str, Any]]:
        """Yield eligible, non-suspended symbol-days missing canonical price facts."""

        if sector_level not in {"L1", "L2"}:
            raise StateModelSetError("missing-price read level must be L1 or L2")

        history_start = self.spec.source_start - timedelta(days=60)
        cursor_name = "hmm_risk_missing_price_source" if sector_level == "L1" else "hmm_risk_missing_price_source_l2"
        cursor = self._conn.cursor(name=cursor_name)
        cursor.itersize = fetch_size
        order_by = (
            "c.trade_date,c.l1_code,c.ts_code,c.l2_code"
            if sector_level == "L1"
            else "c.trade_date,c.l2_code,c.ts_code,c.l1_code"
        )
        cursor.execute(
            f"""
            WITH calendar_history AS (
              SELECT cal_date::date trade_date,
                     lag(cal_date::date,1) OVER (ORDER BY cal_date) previous_trade_date
              FROM market.trading_calendar
              WHERE is_trading=true AND cal_date BETWEEN %s AND %s
            ), calendar_base AS (
              SELECT trade_date,previous_trade_date FROM calendar_history
              WHERE trade_date BETWEEN %s AND %s
            ), basic_base AS (
              SELECT DISTINCT trade_date,ts_code,total_mv,circ_mv
              FROM market.daily_basic WHERE trade_date BETWEEN %s AND %s
            ), basic_history AS (
              SELECT trade_date,ts_code,total_mv,
                     lag(trade_date,1) OVER w previous_basic_date,
                     lag(circ_mv,1) OVER w previous_circ_mv
              FROM basic_base
              WINDOW w AS (PARTITION BY ts_code ORDER BY trade_date)
            ), price_base AS (
              SELECT DISTINCT trade_date,ts_code FROM market.kline_daily_raw
              WHERE trade_date BETWEEN %s AND %s
            ), mapping_source AS (
              SELECT c.trade_date,c.previous_trade_date,s.ts_code,s.eligible_start,s.eligible_end,
                     l1.index_code l1_code,l1.industry_name l1_name,
                     l2.index_code l2_code,l2.industry_name l2_name
              FROM calendar_base c
              JOIN market.stock_universe_pit_spans s
                ON s.universe_key=%s AND s.eligible_start<=c.trade_date
               AND (s.eligible_end IS NULL OR s.eligible_end>=c.trade_date)
              JOIN market.sw_index_member m
                ON m.ts_code=s.ts_code AND m.in_date<=c.trade_date
               AND (m.out_date IS NULL OR m.out_date>=c.trade_date)
              JOIN market.sw_index_classify l1
                ON l1.level='L1' AND m.l1_code IN (l1.index_code,l1.industry_code)
              JOIN market.sw_index_classify l2
                ON l2.level='L2' AND m.l2_code IN (l2.index_code,l2.industry_code)
            ), canonical_identity AS (
              SELECT trade_date,previous_trade_date,ts_code,eligible_start,eligible_end,
                     l1_code,l1_name,l2_code,l2_name
              FROM mapping_source
              GROUP BY trade_date,previous_trade_date,ts_code,eligible_start,eligible_end,
                       l1_code,l1_name,l2_code,l2_name
            ), counted AS (
              SELECT c.*,count(*) OVER (PARTITION BY trade_date,ts_code) canonical_identity_count
              FROM canonical_identity c
            )
            SELECT c.trade_date,c.ts_code,c.l1_code,c.l1_name,c.l2_code,c.l2_name,
                   c.canonical_identity_count,c.eligible_start,db.total_mv,c.previous_trade_date,
                   db.previous_basic_date,db.previous_circ_mv
            FROM counted c
            LEFT JOIN price_base p ON p.trade_date=c.trade_date AND p.ts_code=c.ts_code
            LEFT JOIN basic_history db ON db.trade_date=c.trade_date AND db.ts_code=c.ts_code
            WHERE p.ts_code IS NULL
              AND NOT EXISTS (
                SELECT 1 FROM market.suspend_d sd
                WHERE sd.trade_date=c.trade_date AND sd.ts_code=c.ts_code
              )
            ORDER BY {order_by}
            """,
            (
                history_start,
                self.spec.source_end,
                self.spec.source_start,
                self.spec.source_end,
                history_start,
                self.spec.source_end,
                history_start,
                self.spec.source_end,
                self.spec.universe_key,
            ),
        )
        try:
            for row in cursor:
                if int(row[6]) != 1:
                    raise StateModelSetError(
                        f"symbol/date resolves to multiple canonical identities: {row[1]}/{row[0]}"
                    )
                previous_circ_mv = row[11] if row[9] is not None and row[9] == row[10] and row[9] >= row[7] else None
                yield {
                    "trade_date": row[0],
                    "symbol": row[1],
                    "l1_code": row[2],
                    "l1_name": row[3],
                    "l2_code": row[4],
                    "l2_name": row[5],
                    "is_suspended": False,
                    "open_yuan": None,
                    "high_yuan": None,
                    "low_yuan": None,
                    "close_yuan": None,
                    "volume_shares": None,
                    "amount_cny": None,
                    "prev_close_yuan": None,
                    "prev_close_5_yuan": None,
                    "prev_close_10_yuan": None,
                    "total_mv_cny": _scaled(row[8], 0.0001),
                    "prev_circ_mv_cny": _scaled(previous_circ_mv, 0.0001),
                    "buy_sm_amount_cny": None,
                    "sell_sm_amount_cny": None,
                    "buy_elg_amount_cny": None,
                    "sell_elg_amount_cny": None,
                    "net_mf_amount_cny": None,
                    "up_limit_yuan": None,
                }
        finally:
            cursor.close()


def _scaled(value: Any, divisor: float) -> float | None:
    if value is None:
        return None
    return float(value) / divisor


def load_mapping_manifest(reader: PostgresStockFactReader) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    digest = hashlib.sha256()
    count = 0
    l1_l2: dict[str, set[str]] = {}
    prior: tuple[Any, ...] | None = None
    canonical_identity_count = 0
    previous_canonical: tuple[date, str, str, str] | None = None
    for row in reader.iter_mapping_source_rows():
        serialized = {key: (value.isoformat() if isinstance(value, date) else value) for key, value in row.items()}
        digest.update(canonical_json_bytes(serialized))
        digest.update(b"\n")
        count += 1
        l1_l2.setdefault(str(row["l1_code"]), set()).add(str(row["l2_code"]))
        canonical = (row["trade_date"], row["symbol"], row["l1_code"], row["l2_code"])
        if canonical != previous_canonical:
            canonical_identity_count += 1
            previous_canonical = canonical
        ordering = (
            row["trade_date"],
            row["symbol"],
            row["l1_code"],
            row["l2_code"],
            row["in_date"],
            row["out_date"] or date.max,
        )
        if prior is not None and ordering < prior:
            raise StateModelSetError("mapping source rows are not in canonical order")
        prior = ordering
    if len(l1_l2) != 31 or len({code for codes in l1_l2.values() for code in codes}) != 131:
        raise StateModelSetError("mapping source does not cover canonical L1=31/L2=131")
    constituents = {
        code: {
            "schema_version": "hmm_risk_l1_pit_l2_constituents_v1",
            "l1_code": code,
            "l2_codes": sorted(codes),
            "source_window_start": reader.spec.source_start.isoformat(),
            "source_window_end": reader.spec.source_end.isoformat(),
        }
        for code, codes in sorted(l1_l2.items())
    }
    manifest = {
        "schema_version": "hmm_risk_pit_mapping_manifest_v1",
        "universe_key": reader.spec.universe_key,
        "source_window_start": reader.spec.source_start.isoformat(),
        "source_window_end": reader.spec.source_end.isoformat(),
        "source_row_count": count,
        "canonical_identity_count": canonical_identity_count,
        "source_jsonl_sha256": digest.hexdigest(),
        "canonical_l1_count": len(l1_l2),
        "canonical_l2_count": len({code for codes in l1_l2.values() for code in codes}),
        "constituent_manifest_hash": hashlib.sha256(canonical_json_bytes(constituents)).hexdigest(),
    }
    return manifest, constituents


def load_daily_aggregates(
    reader: PostgresStockFactReader,
    *,
    min_coverage: float = MIN_COVERAGE,
    sector_level: str = "L1",
) -> tuple[list[L1DailyAggregate], dict[str, Any]]:
    if sector_level not in {"L1", "L2"}:
        raise StateModelSetError("daily aggregate level must be L1 or L2")
    digest = hashlib.sha256()
    raw_count = 0
    aggregates: list[L1DailyAggregate] = []
    invalid_sector_dates: list[dict[str, Any]] = []

    missing_rows = list(
        reader.iter_missing_price_rows()
        if sector_level == "L1"
        else reader.iter_missing_price_rows(sector_level=sector_level)
    )
    sort_code = "l1_code" if sector_level == "L1" else "l2_code"
    merged_rows = heapq.merge(
        reader.iter_stock_fact_rows()
        if sector_level == "L1"
        else reader.iter_stock_fact_rows(sector_level=sector_level),
        iter(missing_rows),
        key=lambda row: (row["trade_date"], row[sort_code], row["symbol"], row["l1_code"], row["l2_code"]),
    )

    def rows_with_hash() -> Iterator[dict[str, Any]]:
        nonlocal raw_count
        for row in merged_rows:
            serialized = {key: (value.isoformat() if isinstance(value, date) else value) for key, value in row.items()}
            digest.update(canonical_json_bytes(serialized))
            digest.update(b"\n")
            raw_count += 1
            if sector_level == "L2":
                projected = dict(row)
                projected["l1_code"] = row["l2_code"]
                projected["l1_name"] = row["l2_name"]
                yield projected
            else:
                yield row

    for _, group in itertools.groupby(
        rows_with_hash(),
        key=lambda row: (row["trade_date"], row["l1_code"]),
    ):
        try:
            aggregates.append(aggregate_l1_day(list(group), min_coverage=min_coverage))
        except ObservationCoverageError as exc:
            identity = (
                {"l1_code": exc.l1_code} if sector_level == "L1" else {"sector_level": "L2", "sector_code": exc.l1_code}
            )
            invalid_sector_dates.append(
                {
                    "trade_date": exc.trade_date.isoformat(),
                    **identity,
                    "reason": "stock_coverage_insufficient",
                    "count_coverage": exc.count_coverage,
                    "weight_coverage": exc.weight_coverage,
                    "missing_evidence": list(exc.missing_evidence),
                }
            )
    if not aggregates:
        raise StateModelSetError("PostgreSQL stock-fact source produced no aggregates")
    manifest = {
        "schema_version": "hmm_risk_stock_fact_dataset_manifest_v1",
        "source_window_start": reader.spec.source_start.isoformat(),
        "source_window_end": reader.spec.source_end.isoformat(),
        "raw_row_count": raw_count,
        "missing_non_suspended_price_row_count": len(missing_rows),
        "raw_jsonl_sha256": digest.hexdigest(),
        "aggregate_row_count": len(aggregates),
        "invalid_l1_date_count": len(invalid_sector_dates),
        "invalid_l1_dates": invalid_sector_dates,
        "aggregate_sha256": hashlib.sha256(
            canonical_json_bytes(
                [
                    {
                        **item.__dict__,
                        "trade_date": item.trade_date.isoformat(),
                    }
                    for item in aggregates
                ]
            )
        ).hexdigest(),
        "min_count_coverage": min_coverage,
        "min_weight_coverage": min_coverage,
    }
    if sector_level == "L2":
        manifest["schema_version"] = "hmm_risk_direct_l2_stock_fact_dataset_manifest_v1"
        manifest["direct_sector_level"] = "L2"
        manifest["invalid_sector_date_count"] = manifest.pop("invalid_l1_date_count")
        manifest["invalid_sector_dates"] = manifest.pop("invalid_l1_dates")
    return aggregates, manifest
