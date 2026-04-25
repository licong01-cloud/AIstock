"""Data completeness checker — tiered direct-table validation.

Does NOT depend on market.data_stats (which is manually refreshed).
All queries run directly against data tables with statement_timeout guards.
Uses the tiered strategy from the data health plan:

  light  — <20M rows:  MAX + 1-day COUNT + 5-day gap scan
  medium — ~20M rows:   MAX + 3-day COUNT + 5-day gap scan
  heavy  — billions:    MAX + latest-day DISTINCT-ts_code + 3-day EXISTS

Typical usage::

    from .data_completeness import DataCompletenessChecker, CHECK_TIERS

    checker = DataCompletenessChecker(db_cfg)
    results = checker.check_all()
    for r in results:
        print(r.summary())
"""
from __future__ import annotations

import datetime as dt
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras as pgx


# ---------------------------------------------------------------------------
# Tier definitions — which tables get what level of checking
# ---------------------------------------------------------------------------

@dataclass
class TierConfig:
    name: str
    tables: List[str]
    count_days: int         # how many recent trading days to COUNT(*)
    gap_days: int           # how far back to scan for gaps
    timeout: str            # statement_timeout per query


LIGHT_TIER = TierConfig(
    name="light",
    tables=[
        "adj_factor", "stk_limit", "stock_st", "bak_basic",
        "index_daily", "sw_daily", "margin_detail", "cyq_perf",
        "stock_basic", "sector_data",
    ],
    count_days=1,
    gap_days=5,
    timeout="10s",
)

MEDIUM_TIER = TierConfig(
    name="medium",
    tables=["kline_daily_raw", "daily_basic", "stock_moneyflow_ts"],
    count_days=3,
    gap_days=5,
    timeout="30s",
)

HEAVY_TIER = TierConfig(
    name="heavy",
    tables=["kline_minute_raw"],
    count_days=1,   # only COUNT(DISTINCT ts_code) for latest day
    gap_days=3,     # EXISTS check only
    timeout="60s",
)

ALL_TIERS = [LIGHT_TIER, MEDIUM_TIER, HEAVY_TIER]

# Tables that should NOT have row-count expectations checked
# (variable-row tables: only change on announcement dates)
NO_COUNT_TABLES = frozenset({"stock_st", "bak_basic", "sw_index_member", "stock_basic", "stk_limit"})

# Tables where gap detection doesn't apply (static/reference tables with
# no meaningful daily date column — e.g. list_date tracks IPO dates, not
# trading days, so "gaps" are always expected)
NO_GAP_TABLES = frozenset({"stock_basic", "sw_index_member", "stock_st"})

# Tables where exact stock count doesn't apply (only a subset of stocks)
LOW_COVERAGE_TABLES = frozenset({"stock_moneyflow_ts", "margin_detail"})

# Tables where MAX(date) < latest_trading does NOT indicate staleness.
# These use event-driven date columns (IPO list_date, announcement ann_date,
# rebalance in_date) that only change on specific events, not every trading day.
NO_FRESHNESS_TABLES = frozenset({"stock_basic", "stock_st", "sw_index_member"})

# Tables with inherent T+1 upstream delay — data for trading day T
# is only published by Tushare on T+1.
T_PLUS_1_TABLES = frozenset({"margin_detail"})

# Map dataset name → table.column for direct queries
DATASET_TABLE_MAP: Dict[str, Tuple[str, str]] = {
    "adj_factor":           ("market.adj_factor",           "trade_date"),
    "stk_limit":            ("market.stk_limit",            "trade_date"),
    "stock_st":             ("market.stock_st",             "ann_date"),
    "bak_basic":            ("market.bak_basic",            "trade_date"),
    "index_daily":          ("market.index_daily",          "trade_date"),
    "sw_daily":             ("market.sw_daily",             "trade_date"),
    "margin_detail":        ("market.margin_detail",        "trade_date"),
    "cyq_perf":             ("market.cyq_perf",             "trade_date"),
    "stock_basic":          ("market.stock_basic",          "list_date"),
    "sw_index_member":      ("market.sw_index_member",      "in_date"),
    "sector_data":          ("market.sector_data",          "trade_date"),
    "kline_daily_raw":      ("market.kline_daily_raw",      "trade_date"),
    "daily_basic":          ("market.daily_basic",          "trade_date"),
    "stock_moneyflow_ts":   ("market.moneyflow_ts",         "trade_date"),
    "kline_minute_raw":     ("market.kline_minute_raw",     "trade_time"),
}


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

@dataclass
class DatasetCheckResult:
    dataset: str
    table_name: str
    date_column: str
    tier: str
    max_date: Optional[dt.date] = None
    expected_date: Optional[dt.date] = None
    status: str = "unknown"          # ok | stale | empty | low_coverage | gap | error
    row_counts: Dict[str, int] = field(default_factory=dict)  # date_str → count
    expected_rows: Optional[int] = None
    coverage_pct: Optional[float] = None
    gaps: List[str] = field(default_factory=list)             # missing trading dates
    error_message: str = ""
    elapsed_ms: float = 0.0

    @property
    def is_fresh(self) -> bool:
        if self.max_date is None or self.expected_date is None:
            return False
        return self.max_date >= self.expected_date

    def summary(self) -> Dict[str, Any]:
        return {
            "dataset": self.dataset,
            "tier": self.tier,
            "status": self.status,
            "max_date": str(self.max_date) if self.max_date else None,
            "expected_date": str(self.expected_date) if self.expected_date else None,
            "is_fresh": self.is_fresh,
            "row_counts": self.row_counts,
            "expected_rows": self.expected_rows,
            "coverage_pct": self.coverage_pct,
            "gaps": self.gaps,
            "elapsed_ms": self.elapsed_ms,
        }


# ---------------------------------------------------------------------------
# Checker
# ---------------------------------------------------------------------------

class DataCompletenessChecker:
    """Run tiered completeness checks across all registered datasets."""

    def __init__(self, db_cfg: Dict[str, Any]) -> None:
        self._db_cfg = db_cfg
        self._listed_stocks: Optional[int] = None
        self._listed_stocks_ts: float = 0.0
        self._index_count: Optional[int] = None
        self._sw_l2_count: Optional[int] = None

    # -- helpers --------------------------------------------------------------

    def _conn(self):
        return psycopg2.connect(**self._db_cfg)

    def _get_listed_stocks(self) -> int:
        """Cached count of currently listed stocks (1 hour TTL)."""
        now = time.time()
        if self._listed_stocks is not None and (now - self._listed_stocks_ts) < 3600:
            return self._listed_stocks
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM market.stock_basic WHERE list_status = 'L'")
                self._listed_stocks = cur.fetchone()[0]
        self._listed_stocks_ts = now
        return self._listed_stocks

    def _get_index_count(self) -> int:
        if self._index_count is not None:
            return self._index_count
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM market.index_basic")
                self._index_count = cur.fetchone()[0] or 0
        return self._index_count

    def _get_sw_l2_count(self) -> int:
        if self._sw_l2_count is not None:
            return self._sw_l2_count
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(DISTINCT index_code) FROM market.sw_index_classify WHERE level = 'L2'")
                self._sw_l2_count = cur.fetchone()[0] or 0
        return self._sw_l2_count

    def _get_latest_trading_day(self) -> Optional[dt.date]:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT MAX(cal_date) FROM market.trading_calendar"
                    " WHERE is_trading = TRUE AND cal_date <= CURRENT_DATE"
                )
                row = cur.fetchone()
                return row[0] if row else None

    def _get_recent_trading_days(self, count: int) -> List[dt.date]:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT cal_date FROM market.trading_calendar"
                    " WHERE is_trading = TRUE AND cal_date <= CURRENT_DATE"
                    " ORDER BY cal_date DESC LIMIT %s",
                    (count,),
                )
                return [r[0] for r in cur.fetchall()]

    def _checked_query(self, sql: str, params: tuple = (), timeout: str = "10s") -> List[Any]:
        """Execute a query with statement_timeout guard. Returns fetchall()."""
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET LOCAL statement_timeout = '{timeout}'")
                cur.execute(sql, params)
                return cur.fetchall()

    # -- per-tier checks ------------------------------------------------------

    def _check_max_date(self, table_name: str, date_col: str, timeout: str) -> Tuple[Optional[dt.date], float]:
        """Return (max_date, elapsed_ms)."""
        t0 = time.time()
        rows = self._checked_query(
            f"SELECT MAX({date_col})::date FROM {table_name}",
            timeout=timeout,
        )
        elapsed = (time.time() - t0) * 1000
        if rows and rows[0] and rows[0][0]:
            return rows[0][0], elapsed
        return None, elapsed

    def _check_row_counts(
        self, table_name: str, date_col: str, days: List[dt.date], timeout: str,
    ) -> Tuple[Dict[str, int], float]:
        """Return ({date_str: count}, elapsed_ms)."""
        if not days:
            return {}, 0.0
        t0 = time.time()
        result: Dict[str, int] = {}
        for d in days:
            d_str = d.isoformat()
            next_d = d + dt.timedelta(days=1)
            rows = self._checked_query(
                f"SELECT COUNT(*) FROM {table_name}"
                f" WHERE {date_col} >= %s AND {date_col} < %s",
                (d, next_d),
                timeout=timeout,
            )
            result[d_str] = rows[0][0] if rows else 0
        elapsed = (time.time() - t0) * 1000
        return result, elapsed

    def _check_distinct_codes(
        self, table_name: str, date_col: str, day: dt.date, timeout: str,
    ) -> Tuple[Optional[int], float]:
        """COUNT(DISTINCT ts_code) for one day. For heavy tier (minute data)."""
        t0 = time.time()
        next_d = day + dt.timedelta(days=1)
        rows = self._checked_query(
            f"SELECT COUNT(DISTINCT ts_code) FROM {table_name}"
            f" WHERE {date_col} >= %s AND {date_col} < %s",
            (day, next_d),
            timeout=timeout,
        )
        elapsed = (time.time() - t0) * 1000
        return (rows[0][0] if rows else None), elapsed

    def _check_gaps(
        self, table_name: str, date_col: str, start: dt.date, end: dt.date, timeout: str,
    ) -> Tuple[List[str], float]:
        """Return missing trading dates between start and end (inclusive)."""
        t0 = time.time()
        rows = self._checked_query(
            "SELECT tc.cal_date FROM market.trading_calendar tc"
            " WHERE tc.is_trading = TRUE"
            "   AND tc.cal_date >= %s AND tc.cal_date <= %s"
            "   AND tc.cal_date NOT IN ("
            "       SELECT DISTINCT {dc} FROM {tbl}"
            "       WHERE {dc} >= %s AND {dc} <= %s"
            "   )"
            " ORDER BY tc.cal_date".format(dc=date_col, tbl=table_name),
            (start, end, start, end),
            timeout=timeout,
        )
        elapsed = (time.time() - t0) * 1000
        return [str(r[0]) for r in rows], elapsed

    def _check_exists(
        self, table_name: str, date_col: str, days: List[dt.date], timeout: str,
    ) -> Tuple[Dict[str, bool], float]:
        """Lightweight EXISTS check per day (for heavy tier gap detection)."""
        t0 = time.time()
        result: Dict[str, bool] = {}
        for d in days:
            next_d = d + dt.timedelta(days=1)
            rows = self._checked_query(
                f"SELECT 1 FROM {table_name}"
                f" WHERE {date_col} >= %s AND {date_col} < %s LIMIT 1",
                (d, next_d),
                timeout=timeout,
            )
            result[d.isoformat()] = len(rows) > 0
        elapsed = (time.time() - t0) * 1000
        return result, elapsed

    def _expected_rows(self, dataset: str) -> Optional[int]:
        """Compute expected row count for one trading day."""
        if dataset in NO_COUNT_TABLES:
            return None
        listed = self._get_listed_stocks()
        if dataset in {"sw_daily", "sector_data"}:
            return self._get_sw_l2_count()
        if dataset == "index_daily":
            # index_daily row count varies; no reliable external reference.
            # Check MAX(date) and gaps, but skip coverage check.
            return None
        # per-stock-per-day tables
        return listed

    def _coverage_threshold(self, dataset: str) -> float:
        """Minimum acceptable coverage ratio."""
        if dataset in LOW_COVERAGE_TABLES:
            return 0.70
        return 0.90

    # -- main check methods ---------------------------------------------------

    def _check_light_or_medium(self, cfg: TierConfig, latest_trading: dt.date) -> List[DatasetCheckResult]:
        results: List[DatasetCheckResult] = []
        recent_days = self._get_recent_trading_days(cfg.count_days)
        gap_days = self._get_recent_trading_days(cfg.gap_days)

        for ds in cfg.tables:
            info = DATASET_TABLE_MAP.get(ds)
            if not info:
                continue
            table_name, date_col = info

            result = DatasetCheckResult(
                dataset=ds,
                table_name=table_name,
                date_column=date_col,
                tier=cfg.name,
                expected_date=latest_trading,
            )
            t_start = time.time()

            try:
                # 1) MAX date
                mx, mx_ms = self._check_max_date(table_name, date_col, cfg.timeout)
                result.max_date = mx

                # 2) Row counts (skip for no-count tables)
                if ds not in NO_COUNT_TABLES:
                    counts, cnt_ms = self._check_row_counts(table_name, date_col, recent_days, cfg.timeout)
                    result.row_counts = counts
                else:
                    cnt_ms = 0.0

                # 3) Expected rows
                expected = self._expected_rows(ds)
                result.expected_rows = expected

                # 4) Gap detection (skip for tables where "gaps" are meaningless)
                if gap_days and ds not in NO_GAP_TABLES:
                    gap_start = gap_days[-1]  # oldest
                    gap_end = gap_days[0]     # newest
                    gaps, gap_ms = self._check_gaps(table_name, date_col, gap_start, gap_end, cfg.timeout)
                    result.gaps = gaps
                else:
                    gap_ms = 0.0

                # 5) Determine status
                if mx is None:
                    result.status = "empty"
                elif ds in NO_FRESHNESS_TABLES:
                    # Event-driven tables (stock_basic, stock_st, sw_index_member):
                    # date column tracks IPO/announcement/rebalance events, not
                    # daily trading — having data at all means OK.
                    result.status = "ok"
                elif ds in T_PLUS_1_TABLES:
                    # T+1 upstream sources (margin_detail): data for day T
                    # arrives on T+1, so being 1 day behind is normal.
                    if mx >= latest_trading - dt.timedelta(days=1):
                        result.status = "ok"
                    else:
                        result.status = "stale"
                elif mx < latest_trading:
                    result.status = "stale"
                elif result.gaps:
                    result.status = "gap"
                else:
                    # Check coverage
                    if ds not in NO_COUNT_TABLES and expected:
                        threshold = self._coverage_threshold(ds)
                        # Check the latest day's count against expected
                        latest_count = result.row_counts.get(str(latest_trading))
                        if latest_count is not None and latest_count >= 0:
                            result.coverage_pct = latest_count / expected if expected > 0 else 0
                            if result.coverage_pct < threshold:
                                result.status = "low_coverage"
                            else:
                                result.status = "ok"
                        else:
                            result.status = "ok"  # no count data but date is fresh
                    else:
                        result.status = "ok"

            except Exception as exc:
                result.status = "error"
                result.error_message = str(exc)

            result.elapsed_ms = (time.time() - t_start) * 1000
            results.append(result)

        return results

    def _check_heavy(self, cfg: TierConfig, latest_trading: dt.date) -> List[DatasetCheckResult]:
        """Heavy tier: kline_minute_raw — minimal checks only."""
        results: List[DatasetCheckResult] = []

        for ds in cfg.tables:
            info = DATASET_TABLE_MAP.get(ds)
            if not info:
                continue
            table_name, date_col = info

            result = DatasetCheckResult(
                dataset=ds,
                table_name=table_name,
                date_column=date_col,
                tier=cfg.name,
                expected_date=latest_trading,
            )
            t_start = time.time()

            try:
                # 1) MAX date
                mx, mx_ms = self._check_max_date(table_name, date_col, cfg.timeout)
                result.max_date = mx

                # 2) COUNT(DISTINCT ts_code) for latest day only
                distinct, dist_ms = self._check_distinct_codes(
                    table_name, date_col, latest_trading, cfg.timeout,
                )
                if distinct is not None:
                    result.row_counts[str(latest_trading)] = distinct
                    listed = self._get_listed_stocks()
                    result.expected_rows = listed
                    result.coverage_pct = distinct / listed if listed > 0 else 0

                # 3) EXISTS for last 3 trading days
                recent_3 = self._get_recent_trading_days(3)
                exists_map, exist_ms = self._check_exists(
                    table_name, date_col, recent_3, cfg.timeout,
                )
                for d_str, has_data in exists_map.items():
                    if not has_data:
                        result.gaps.append(d_str)

                # 4) Determine status
                if mx is None:
                    result.status = "empty"
                elif mx < latest_trading:
                    result.status = "stale"
                elif result.gaps:
                    result.status = "gap"
                elif result.coverage_pct is not None and result.coverage_pct < 0.90:
                    result.status = "low_coverage"
                else:
                    result.status = "ok"

            except Exception as exc:
                result.status = "error"
                result.error_message = str(exc)

            result.elapsed_ms = (time.time() - t_start) * 1000
            results.append(result)

        return results

    # -- public API -----------------------------------------------------------

    def check_all(self) -> List[DatasetCheckResult]:
        """Run completeness checks on ALL datasets across all tiers."""
        latest_trading = self._get_latest_trading_day()
        if latest_trading is None:
            raise RuntimeError("trading_calendar has no data — cannot determine latest trading day")

        all_results: List[DatasetCheckResult] = []

        for cfg in ALL_TIERS:
            if cfg.name == "heavy":
                all_results.extend(self._check_heavy(cfg, latest_trading))
            else:
                all_results.extend(self._check_light_or_medium(cfg, latest_trading))

        return all_results

    def check_datasets(self, datasets: List[str]) -> List[DatasetCheckResult]:
        """Run completeness checks on specific datasets only (used by weekend compensation)."""
        latest_trading = self._get_latest_trading_day()
        if latest_trading is None:
            raise RuntimeError("trading_calendar has no data")

        results: List[DatasetCheckResult] = []
        ds_set = set(datasets)

        for cfg in ALL_TIERS:
            matching = [ds for ds in cfg.tables if ds in ds_set]
            if not matching:
                continue
            sub_cfg = TierConfig(
                name=cfg.name, tables=matching,
                count_days=cfg.count_days, gap_days=cfg.gap_days, timeout=cfg.timeout,
            )
            if cfg.name == "heavy":
                results.extend(self._check_heavy(sub_cfg, latest_trading))
            else:
                results.extend(self._check_light_or_medium(sub_cfg, latest_trading))

        return results
