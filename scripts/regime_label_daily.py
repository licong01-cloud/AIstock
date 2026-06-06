"""T10: Daily market regime label computation.

Per data_warehouse_extension_design_20260510.md §8.
Status: SKELETON - not yet wired into cron.

Computes simple_quadrant regime classification from CSI300 6m-return and
60d-volatility percentiles, writes to market.regime_label.

Usage (manual):
    python scripts/regime_label_daily.py --date 2026-05-10
    python scripts/regime_label_daily.py --backfill --start 2024-01-01 --end 2026-05-10

Future methods (P2):
    python scripts/regime_label_daily.py --date 2026-05-10 --method hmm_viterbi
    python scripts/regime_label_daily.py --date 2026-05-10 --method ensemble

Boundaries:
- Reads market.index_daily (assumed exists, ingested by Tushare externally).
- Writes market.regime_label only.
- Does NOT touch qe_archive / paper_v2_* schemas.
- ON CONFLICT (trade_date, source_method) DO UPDATE for re-run idempotency.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
from dataclasses import dataclass

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

CSI300_INDEX_CODE = "000300.SH"  # adjust if market.index_daily uses different convention
HISTORY_LOOKBACK_YEARS = 5  # for percentile baseline
SIX_MONTH_TRADING_SESSIONS = 126


@dataclass(frozen=True)
class RegimeSignal:
    """Raw signals that drive a regime classification decision."""

    trade_date: dt.date
    csi300_6m_ret: float | None
    csi300_60d_vol: float | None
    ret_pct_5y: float | None
    vol_pct_5y: float | None

    def to_json(self) -> str:
        return json.dumps(
            {
                "csi300_6m_ret": self.csi300_6m_ret,
                "csi300_60d_vol": self.csi300_60d_vol,
                "ret_pct_5y": self.ret_pct_5y,
                "vol_pct_5y": self.vol_pct_5y,
            }
        )


@dataclass(frozen=True)
class RegimeLabel:
    trade_date: dt.date
    regime: str
    confidence: float
    source_method: str
    source_signal: RegimeSignal


def _connect_pg():
    load_dotenv()
    host = os.environ["TDX_DB_HOST"]
    port = int(os.environ.get("TDX_DB_PORT", "5432"))
    dbname = os.environ.get("TDX_DB_NAME", "aistock")
    user = os.environ["TDX_DB_USER"]
    password = os.environ["TDX_DB_PASSWORD"]
    return psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)


def fetch_csi300_6m_return(conn, trade_date: dt.date) -> float | None:
    """6-month (126 trading days) CSI300 return ending on trade_date."""
    # TODO: confirm market.index_daily column names with actual schema
    sql = """
        WITH ranked AS (
            SELECT close, ROW_NUMBER() OVER (ORDER BY trade_date DESC) AS rn
            FROM market.index_daily
            WHERE index_code = %s AND trade_date <= %s
        )
        SELECT today.close / lookback.close - 1.0 AS ret_6m
        FROM ranked AS today
        JOIN ranked AS lookback
          ON lookback.rn = today.rn + %s
        WHERE today.rn = 1
        LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(sql, (CSI300_INDEX_CODE, trade_date, SIX_MONTH_TRADING_SESSIONS))
        row = cur.fetchone()
        return float(row[0]) if row and row[0] is not None else None


def fetch_csi300_60d_volatility(conn, trade_date: dt.date) -> float | None:
    """Annualised 60-day rolling volatility of CSI300 daily returns."""
    sql = """
        WITH returns AS (
            SELECT trade_date, close / LAG(close) OVER (ORDER BY trade_date) - 1.0 AS r
            FROM market.index_daily
            WHERE index_code = %s
              AND trade_date <= %s
              AND trade_date >= %s - INTERVAL '120 days'
            ORDER BY trade_date DESC
            LIMIT 60
        )
        SELECT STDDEV_SAMP(r) * SQRT(252) FROM returns
    """
    with conn.cursor() as cur:
        cur.execute(sql, (CSI300_INDEX_CODE, trade_date, trade_date))
        row = cur.fetchone()
        return float(row[0]) if row and row[0] is not None else None


def fetch_csi300_trading_dates(
    conn,
    start: dt.date,
    end: dt.date,
    *,
    index_code: str = CSI300_INDEX_CODE,
) -> list[dt.date]:
    """Actual index trading dates in [start, end], excluding market holidays."""
    sql = """
        SELECT trade_date
        FROM market.index_daily
        WHERE index_code = %s
          AND trade_date >= %s
          AND trade_date <= %s
        ORDER BY trade_date ASC
    """
    with conn.cursor() as cur:
        cur.execute(sql, (index_code, start, end))
        return [row[0] for row in cur.fetchall()]


def fetch_percentile(conn, trade_date: dt.date, value: float, signal: str) -> float | None:
    """Percentile rank of `value` against last 5 years of similar signals.

    Implementation choice: pull historical signal series into Python and use
    a manual rank computation (count of values <= target / N).

    Why this over a single SQL `PERCENT_RANK() OVER (...)` window query:
    - The historical series for ret_6m / vol_60d is itself a derived rolling
      computation (LAG / window over close), which would require nesting
      window functions inside another window — supported by Postgres but
      hard to reason about and brittle if `market.index_daily` schema shifts.
    - Pulling ~1260 daily rows (5y * 252) per signal per call is cheap and
      keeps the percentile contract obvious for the simple_quadrant method.
    - We avoid scipy here to keep the regime job dependency-light; a manual
      rank works for our monotone, no-NaN history.

    Caller passes the already-computed `value` for `trade_date`; we compare
    against the historical distribution of the same signal across the prior
    5 years (excluding `trade_date` itself).

    Returns None if fewer than 60 historical observations exist (guards
    against early-history degeneracy).
    """
    history_start = trade_date - dt.timedelta(days=int(HISTORY_LOOKBACK_YEARS * 365.25))

    if signal == "ret_6m":
        sql = """
            WITH base AS (
                SELECT
                    trade_date,
                    close,
                    LAG(close, %s) OVER (ORDER BY trade_date) AS lookback_close
                FROM market.index_daily
                WHERE index_code = %s
                  AND trade_date <= %s
                  AND trade_date >= %s - INTERVAL '2 years'
            )
            SELECT trade_date, close / lookback_close - 1.0 AS ret
            FROM base
            WHERE trade_date >= %s
              AND trade_date < %s
        """
        params = (
            SIX_MONTH_TRADING_SESSIONS,
            CSI300_INDEX_CODE,
            trade_date,
            history_start,
            history_start,
            trade_date,
        )
    elif signal == "vol_60d":
        sql = """
            WITH returns AS (
                SELECT
                    trade_date,
                    close / LAG(close) OVER (ORDER BY trade_date) - 1.0 AS r
                FROM market.index_daily
                WHERE index_code = %s
                  AND trade_date <= %s
                  AND trade_date >= %s - INTERVAL '120 days'
            )
            SELECT
                trade_date,
                STDDEV_SAMP(r) OVER (
                    ORDER BY trade_date
                    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) * SQRT(252) AS vol
            FROM returns
            WHERE trade_date >= %s
              AND trade_date < %s
        """
        params = (CSI300_INDEX_CODE, trade_date, history_start, history_start, trade_date)
    else:
        raise ValueError(f"unknown signal {signal!r}; expected 'ret_6m' or 'vol_60d'")

    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    history = [float(r[1]) for r in rows if r[1] is not None]
    if len(history) < 60:
        return None

    n_le = sum(1 for h in history if h <= value)
    return round(n_le / len(history), 4)


def classify_simple_quadrant(signal: RegimeSignal) -> tuple[str, float]:
    """Map ret_pct + vol_pct to one of 5 regime buckets.

    Returns: (regime, confidence)
    """
    if signal.ret_pct_5y is None or signal.vol_pct_5y is None:
        # Not enough history -> default to oscillation with low confidence
        return ("oscillation", 0.0)

    ret_pct = signal.ret_pct_5y
    vol_pct = signal.vol_pct_5y

    if ret_pct > 0.6 and vol_pct < 0.4:
        regime = "bull"
    elif ret_pct < 0.4 and vol_pct > 0.6:
        regime = "bear"
    elif vol_pct > 0.6:
        regime = "high_vol"
    elif vol_pct < 0.4:
        regime = "low_vol"
    else:
        regime = "oscillation"

    # Confidence: distance from quadrant centre (max 0.5 per dimension -> sqrt(0.5) max)
    centre_dist = ((ret_pct - 0.5) ** 2 + (vol_pct - 0.5) ** 2) ** 0.5
    confidence = min(centre_dist / 0.5, 1.0)
    return (regime, round(confidence, 3))


def compute_regime_for_date(conn, trade_date: dt.date, method: str = "simple_quadrant") -> RegimeLabel:
    if method != "simple_quadrant":
        raise NotImplementedError(f"method {method!r} pending P2 implementation")

    ret_6m = fetch_csi300_6m_return(conn, trade_date)
    vol_60d = fetch_csi300_60d_volatility(conn, trade_date)
    if ret_6m is None or vol_60d is None:
        raise ValueError(f"missing CSI300 data for {trade_date}; cannot classify")

    ret_pct = fetch_percentile(conn, trade_date, ret_6m, "ret_6m")
    vol_pct = fetch_percentile(conn, trade_date, vol_60d, "vol_60d")
    signal = RegimeSignal(
        trade_date=trade_date,
        csi300_6m_ret=ret_6m,
        csi300_60d_vol=vol_60d,
        ret_pct_5y=ret_pct,
        vol_pct_5y=vol_pct,
    )
    regime, confidence = classify_simple_quadrant(signal)
    return RegimeLabel(
        trade_date=trade_date,
        regime=regime,
        confidence=confidence,
        source_method=method,
        source_signal=signal,
    )


def upsert_regime_label(conn, label: RegimeLabel) -> None:
    sql = """
        INSERT INTO market.regime_label
            (trade_date, regime, regime_confidence, source_method, source_signal_json, labeled_at)
        VALUES
            (%(trade_date)s, %(regime)s, %(confidence)s, %(method)s, %(signal_json)s::jsonb, NOW())
        ON CONFLICT (trade_date, source_method) DO UPDATE SET
            regime = EXCLUDED.regime,
            regime_confidence = EXCLUDED.regime_confidence,
            source_signal_json = EXCLUDED.source_signal_json,
            labeled_at = NOW()
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            {
                "trade_date": label.trade_date,
                "regime": label.regime,
                "confidence": label.confidence,
                "method": label.source_method,
                "signal_json": label.source_signal.to_json(),
            },
        )
    conn.commit()


def main() -> int:
    parser = argparse.ArgumentParser(description="Compute and persist daily market regime labels")
    parser.add_argument("--date", type=str, help="trade_date YYYY-MM-DD; default = today")
    parser.add_argument("--method", type=str, default="simple_quadrant",
                        choices=["simple_quadrant", "hmm_viterbi", "bbq", "ensemble"])
    parser.add_argument("--backfill", action="store_true",
                        help="backfill range; requires --start and --end")
    parser.add_argument("--start", type=str, help="backfill start YYYY-MM-DD")
    parser.add_argument("--end", type=str, help="backfill end YYYY-MM-DD")
    parser.add_argument("--dry-run", action="store_true", help="compute but do not write")
    parser.add_argument(
        "--dry-run-1m",
        action="store_true",
        help="shortcut: backfill last ~30 days ending --date (or today), no DB write",
    )
    args = parser.parse_args()

    if args.dry_run_1m:
        end = dt.date.fromisoformat(args.date) if args.date else dt.date.today()
        args.backfill = True
        args.start = (end - dt.timedelta(days=30)).isoformat()
        args.end = end.isoformat()
        args.dry_run = True

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    conn = _connect_pg()
    try:
        if args.backfill:
            if not (args.start and args.end):
                parser.error("--backfill requires --start and --end")
            start = dt.date.fromisoformat(args.start)
            end = dt.date.fromisoformat(args.end)
            for trade_date in fetch_csi300_trading_dates(conn, start, end):
                try:
                    label = compute_regime_for_date(conn, trade_date, method=args.method)
                    if not args.dry_run:
                        upsert_regime_label(conn, label)
                    logger.info("%s %s confidence=%.3f", trade_date, label.regime, label.confidence)
                except (ValueError, NotImplementedError) as exc:
                    logger.warning("skip %s: %s", trade_date, exc)
        else:
            target = dt.date.fromisoformat(args.date) if args.date else dt.date.today()
            label = compute_regime_for_date(conn, target, method=args.method)
            if not args.dry_run:
                upsert_regime_label(conn, label)
            logger.info("%s %s confidence=%.3f", target, label.regime, label.confidence)
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
