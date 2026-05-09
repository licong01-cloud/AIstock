from __future__ import annotations

"""Sector-level HMM training and persistence.

Implements per-sector 2-state GaussianHMM for Shenwan L1 industries,
following the P1 design in p4-p1-p5-strategy-enhancement.

Key classes:
- SectorHMMConfig: dataclass holding all HMM hyper-parameters.
- SectorHMMTrainer: builds observation matrices, trains models, persists to JSON.
"""

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class SectorHMMConfig:
    """Configuration for sector-level HMM training and inference."""

    n_states: int = 2
    history_years: float = 3.0
    min_trading_days: int = 120
    cooldown_days: int = 3
    trending_coeff: float = 1.5
    fading_coeff: float = 0.5
    neutral_coeff: float = 1.0
    sector_level: str = "L2"  # "L1" or "L2" — L2 recommended for finer granularity


# ---------------------------------------------------------------------------
# Database helper (thin wrapper around project pg_pool)
# ---------------------------------------------------------------------------

def _get_db_engine():
    """Return the project's ``get_conn`` context-manager.

    Importing lazily so the module can be loaded even when the DB is
    unavailable (e.g. during unit-testing with mocks).
    """
    from ...db.pg_pool import get_conn
    return get_conn


# ---------------------------------------------------------------------------
# SQL templates
# ---------------------------------------------------------------------------

# Fetch Shenwan L1 industry list
# Actual columns: index_code, industry_name, parent_code, level, industry_code, is_pub, src
_SW_L1_LIST_SQL = """\
SELECT DISTINCT index_code, industry_name
FROM market.sw_index_classify
WHERE level = 'L1'
ORDER BY index_code
"""

# Fetch sector daily OHLCV for a given sector code within a date range
# Actual columns: ts_code, trade_date, name, open, low, high, close, change, pct_change, vol, amount, pe, pb, float_mv, total_mv
_SW_DAILY_SQL = """\
SELECT trade_date, open, high, low, close, vol, amount, pct_change
FROM market.sw_daily
WHERE ts_code = %(sector_code)s
  AND trade_date BETWEEN %(start_date)s AND %(end_date)s
ORDER BY trade_date
"""

# Fetch CSI 300 index daily close for excess-return calculation
# CSI300 is in market.index_daily (not sw_daily), column is pct_chg (not pct_change)
_CSI300_DAILY_SQL = """\
SELECT trade_date, close, pct_chg AS pct_change
FROM market.index_daily
WHERE ts_code = '000300.SH'
  AND trade_date BETWEEN %(start_date)s AND %(end_date)s
ORDER BY trade_date
"""

# Total market volume per day (sum across all L1 sectors in sw_daily)
_MARKET_VOL_SQL = """\
SELECT trade_date, SUM(vol) AS total_vol
FROM market.sw_daily
WHERE trade_date BETWEEN %(start_date)s AND %(end_date)s
GROUP BY trade_date
ORDER BY trade_date
"""

# Count of limit-up stocks per sector per day.
# daily_basic doesn't have pct_change, so we use kline_daily_raw.
# Limit-up approximation: close_li / pre_close >= 1.098 (9.8% threshold)
# Pre-close is computed from previous day's close_li.
# Fallback: if kline_daily_raw is unavailable, return empty dict (graceful degradation).
_LIMIT_UP_SQL = """\
WITH stock_pct AS (
    SELECT k.trade_date, k.ts_code,
           CASE WHEN LAG(k.close_li) OVER (PARTITION BY k.ts_code ORDER BY k.trade_date) > 0
                THEN (k.close_li - LAG(k.close_li) OVER (PARTITION BY k.ts_code ORDER BY k.trade_date))
                     / LAG(k.close_li) OVER (PARTITION BY k.ts_code ORDER BY k.trade_date) * 100
                ELSE 0 END AS pct_change
    FROM market.kline_daily_raw k
    JOIN market.sw_index_member m ON k.ts_code = m.ts_code
    WHERE m.l1_code = %(sector_code)s
      AND k.trade_date BETWEEN %(start_date)s AND %(end_date)s
      AND m.in_date <= k.trade_date
      AND (m.out_date IS NULL OR m.out_date >= k.trade_date)
)
SELECT trade_date,
       COUNT(*) FILTER (WHERE pct_change >= 9.8) AS limit_up_cnt,
       COUNT(*) AS total_cnt
FROM stock_pct
WHERE pct_change IS NOT NULL
GROUP BY trade_date
ORDER BY trade_date
"""


# ---------------------------------------------------------------------------
# SectorHMMTrainer
# ---------------------------------------------------------------------------

class SectorHMMTrainer:
    """Train a 2-state GaussianHMM for each Shenwan L1 industry."""

    def __init__(
        self,
        config: Optional[SectorHMMConfig] = None,
        *,
        db_conn_factory=None,
    ):
        self.config = config or SectorHMMConfig()
        # Allow injecting a connection factory for testing
        self._get_conn = db_conn_factory or _get_db_engine()
        # Batch-preloaded data cache: {sector_code: {trade_date: row_dict}}
        self._sector_data_cache: Optional[Dict[str, Dict]] = None
        self._csi300_cache: Optional[Dict] = None
        self._market_vol_cache: Optional[Dict] = None

    def preload_data(self, start_date: date, end_date: date) -> None:
        """一次性批量加载所有数据到内存，避免逐行业逐日查询 DB.

        调用后 _build_obs_7dim 将使用内存缓存，不再查询 DB。
        """
        from psycopg2.extras import RealDictCursor
        import pandas as pd

        logger.info(f"HMM preload_data: loading sector_data for {start_date} ~ {end_date}")
        t0 = __import__("time").time()

        with self._get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # 1. 一次性加载所有行业的 sector_data
                cur.execute("""
                    SELECT m.l2_code, sd.trade_date,
                           sd.sw2_pct_change, sd.sw2_vol, sd.sw2_amount,
                           sd.sw2_mf_net_amt, sd.sw2_mf_buy_elg_amt, sd.sw2_mf_sell_elg_amt
                    FROM market.sector_data sd
                    JOIN market.sw_index_member m ON sd.ts_code = m.ts_code
                    WHERE sd.trade_date BETWEEN %s AND %s
                      AND m.in_date <= sd.trade_date
                      AND (m.out_date IS NULL OR m.out_date >= sd.trade_date)
                """, (start_date, end_date))
                all_rows = cur.fetchall()

                # 2. CSI300 一次
                cur.execute("""
                    SELECT trade_date, pct_chg FROM market.index_daily
                    WHERE ts_code = '000300.SH' AND trade_date BETWEEN %s AND %s
                    ORDER BY trade_date
                """, (start_date, end_date))
                self._csi300_cache = {r["trade_date"]: float(r["pct_chg"] or 0) for r in cur.fetchall()}

                # 3. market volume 一次
                cur.execute("""
                    SELECT trade_date, SUM(vol) FROM market.sw_daily
                    WHERE trade_date BETWEEN %s AND %s
                    GROUP BY trade_date ORDER BY trade_date
                """, (start_date, end_date))
                self._market_vol_cache = {r["trade_date"]: float(r["sum"] or 0) for r in cur.fetchall()}

        # 按 l2_code → trade_date 建索引
        self._sector_data_cache: Dict[str, Dict] = {}
        for row in all_rows:
            l2 = row["l2_code"]
            if l2 not in self._sector_data_cache:
                self._sector_data_cache[l2] = {}
            td = row["trade_date"]
            if td not in self._sector_data_cache[l2]:
                self._sector_data_cache[l2][td] = {
                    "sw2_pct_change": float(row["sw2_pct_change"] or 0),
                    "sw2_vol": float(row["sw2_vol"] or 0),
                    "sw2_amount": float(row["sw2_amount"] or 0),
                    "sw2_mf_net_amt": float(row["sw2_mf_net_amt"] or 0),
                    "sw2_mf_buy_elg_amt": float(row["sw2_mf_buy_elg_amt"] or 0),
                    "sw2_mf_sell_elg_amt": float(row["sw2_mf_sell_elg_amt"] or 0),
                }

        elapsed = __import__("time").time() - t0
        n_sectors = len(self._sector_data_cache)
        n_rows = len(all_rows)
        logger.info(f"HMM preload_data: {n_rows} rows, {n_sectors} sectors, "
                     f"{len(self._csi300_cache)} CSI300, {len(self._market_vol_cache)} mkt_vol, "
                     f"took {elapsed:.1f}s")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def train_all_sectors(self) -> Dict[str, Any]:
        """Train HMM for every SW L1 sector.

        Returns
        -------
        dict
            ``{sector_code: {transmat, means, covars, state_labels, ...}}``
        """
        from hmmlearn.hmm import GaussianHMM

        sectors = self._fetch_sector_list()
        if not sectors:
            logger.warning("No SW L1 sectors found – nothing to train.")
            return {}

        models: Dict[str, Any] = {}
        for sector_code, sector_name, industry_code in sectors:
            # For L2 sectors, data is directly in sw_daily; for L1, aggregate from L2
            if self.config.sector_level == "L1":
                l2_codes = self._fetch_l2_codes_for_l1(industry_code)
                if not l2_codes:
                    logger.warning(
                        "No L2 sub-sectors found for %s (%s, industry_code=%s), skipping.",
                        sector_code, sector_name, industry_code,
                    )
                    continue
            else:
                # L2: use the sector code directly
                l2_codes = [sector_code]

            try:
                obs = self._build_observation_matrix(sector_code, l2_codes=l2_codes)
            except Exception:
                logger.warning(
                    "Failed to build observation matrix for %s (%s), skipping.",
                    sector_code, sector_name, exc_info=True,
                )
                continue

            if obs.shape[0] < self.config.min_trading_days:
                logger.warning(
                    "Sector %s (%s) has only %d trading days (< %d), skipping.",
                    sector_code, sector_name, obs.shape[0],
                    self.config.min_trading_days,
                )
                continue

            try:
                hmm = GaussianHMM(
                    n_components=self.config.n_states,
                    covariance_type="full",
                    n_iter=100,
                    random_state=42,
                )
                hmm.fit(obs)
            except Exception:
                logger.error(
                    "HMM fitting failed for %s (%s), skipping.",
                    sector_code, sector_name, exc_info=True,
                )
                continue

            state_labels = self._label_states(hmm)

            models[sector_code] = {
                "sector_code": sector_code,
                "sector_name": sector_name,
                "n_states": self.config.n_states,
                "transmat": hmm.transmat_.tolist(),
                "means": hmm.means_.tolist(),
                "covars": hmm.covars_.tolist(),
                "state_labels": {str(k): v for k, v in state_labels.items()},
                "l2_codes": l2_codes,
                "industry_code": industry_code,
                "trained_at": datetime.now(timezone.utc).isoformat(),
                "training_days": int(obs.shape[0]),
            }
            logger.info(
                "Trained HMM for %s (%s): %d days, labels=%s",
                sector_code, sector_name, obs.shape[0], state_labels,
            )

        logger.info("Sector HMM training complete: %d/%d sectors.", len(models), len(sectors))
        return models

    # ------------------------------------------------------------------
    # Observation matrix
    # ------------------------------------------------------------------

    def _build_observation_matrix(self, sector_code: str, l2_codes: List[str] = None) -> np.ndarray:
        """Build a (T, 4) observation matrix for *sector_code*.

        Columns:
            0 – daily return (pct_change / 100)
            1 – 20-day rolling mean of excess return vs CSI 300
            2 – sector volume / total market volume
            3 – limit-up ratio (limit-up count / total stocks in sector)

        Rows with any NaN are dropped.
        """
        end_date = date.today()
        start_date = end_date - timedelta(days=int(self.config.history_years * 365))

        sector_df = self._query_sector_daily(sector_code, start_date, end_date, l2_codes=l2_codes)
        csi300_df = self._query_csi300_daily(start_date, end_date)
        market_vol = self._query_market_volume(start_date, end_date)
        limit_up = self._query_limit_up(sector_code, start_date, end_date)

        # Align all series by trade_date
        dates = sorted(sector_df.keys())
        rows: List[List[float]] = []
        # We need 20 prior days for rolling mean, so start from index 19
        for i, td in enumerate(dates):
            sec = sector_df.get(td)
            csi = csi300_df.get(td)
            mvol = market_vol.get(td)
            lu = limit_up.get(td)

            if sec is None or csi is None or mvol is None:
                continue

            daily_ret = sec["pct_change"] / 100.0

            # 20-day rolling mean of excess return
            window_excesses: List[float] = []
            for j in range(max(0, i - 19), i + 1):
                d2 = dates[j]
                s2 = sector_df.get(d2)
                c2 = csi300_df.get(d2)
                if s2 is not None and c2 is not None:
                    window_excesses.append(s2["pct_change"] / 100.0 - c2["pct_change"] / 100.0)
            if not window_excesses:
                continue
            excess_20d_mean = sum(window_excesses) / len(window_excesses)

            # Volume ratio
            vol_ratio = sec["vol"] / mvol if mvol > 0 else 0.0

            # Limit-up ratio
            if lu is not None and lu["total"] > 0:
                lu_ratio = lu["limit_up"] / lu["total"]
            else:
                lu_ratio = 0.0

            row = [daily_ret, excess_20d_mean, vol_ratio, lu_ratio]
            if any(np.isnan(v) for v in row):
                continue
            rows.append(row)

        if not rows:
            return np.empty((0, 4), dtype=np.float64)
        return np.array(rows, dtype=np.float64)

    # ------------------------------------------------------------------
    # State labelling
    # ------------------------------------------------------------------

    @staticmethod
    def _label_states(model) -> Dict[int, str]:
        """Label HMM states by comparing daily-return component of means.

        The state whose mean daily return (column 0) is higher is labelled
        ``"trending"``; the other is ``"fading"``.
        """
        means = model.means_  # shape (n_states, n_features)
        if means[0, 0] > means[1, 0]:
            return {0: "trending", 1: "fading"}
        else:
            return {0: "fading", 1: "trending"}

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    @staticmethod
    def save_models(models: Dict[str, Any], path: str) -> None:
        """Persist trained HMM parameters to a JSON file."""
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            json.dump(models, f, ensure_ascii=False, indent=2)
        logger.info("Saved %d sector HMM models to %s", len(models), path)

    @staticmethod
    def load_models(path: str) -> Dict[str, Any]:
        """Load HMM parameters from a JSON file."""
        with open(path, "r", encoding="utf-8") as f:
            models = json.load(f)
        logger.info("Loaded %d sector HMM models from %s", len(models), path)
        return models

    # ------------------------------------------------------------------
    # Private DB helpers
    # ------------------------------------------------------------------

    def _fetch_sector_list(self) -> List[Tuple[str, str, str]]:
        """Return list of (sector_code, sector_name, industry_code) for configured level."""
        level = self.config.sector_level or "L2"
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT index_code, industry_name, industry_code
                    FROM market.sw_index_classify
                    WHERE level = %s
                    ORDER BY index_code
                """, (level,))
                return [(row[0], row[1], row[2]) for row in cur.fetchall()]

    def _fetch_l2_codes_for_l1(self, l1_industry_code: str) -> List[str]:
        """Return L2 sector codes belonging to a given L1 industry_code.

        L1 industry_code is like '430000', L2 codes share the first 2 digits.
        """
        prefix = l1_industry_code[:2] if l1_industry_code else ""
        if not prefix:
            return []
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT index_code
                    FROM market.sw_index_classify
                    WHERE level = 'L2'
                      AND LEFT(industry_code, 2) = %s
                """, (prefix,))
                return [row[0] for row in cur.fetchall()]

    def _query_sector_daily(
        self, sector_code: str, start_date: date, end_date: date,
        l2_codes: List[str] = None,
    ) -> Dict[date, Dict[str, float]]:
        """Fetch sector daily data keyed by trade_date.

        Since sw_daily only has L2/L3 data, we aggregate L2 sub-sectors
        into L1 using amount-weighted pct_change.
        """
        codes = l2_codes or [sector_code]
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT d.trade_date,
                           SUM(d.amount * d.pct_change) / NULLIF(SUM(d.amount), 0) AS pct_change,
                           SUM(d.vol) AS vol,
                           SUM(d.amount) AS amount
                    FROM market.sw_daily d
                    WHERE d.ts_code = ANY(%(codes)s)
                      AND d.trade_date BETWEEN %(start_date)s AND %(end_date)s
                    GROUP BY d.trade_date
                    ORDER BY d.trade_date
                """, {
                    "codes": codes,
                    "start_date": start_date,
                    "end_date": end_date,
                })
                result: Dict[date, Dict[str, float]] = {}
                for row in cur.fetchall():
                    td = row[0] if isinstance(row[0], date) else row[0].date()
                    result[td] = {
                        "pct_change": float(row[1] or 0),
                        "vol": float(row[2] or 0),
                        "amount": float(row[3] or 0),
                    }
                return result

    def _query_csi300_daily(
        self, start_date: date, end_date: date,
    ) -> Dict[date, Dict[str, float]]:
        """Fetch CSI 300 index daily data keyed by trade_date."""
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(_CSI300_DAILY_SQL, {
                    "start_date": start_date,
                    "end_date": end_date,
                })
                result: Dict[date, Dict[str, float]] = {}
                for row in cur.fetchall():
                    td = row[0] if isinstance(row[0], date) else row[0].date()
                    result[td] = {
                        "close": float(row[1] or 0),
                        "pct_change": float(row[2] or 0),
                    }
                return result

    def _query_market_volume(
        self, start_date: date, end_date: date,
    ) -> Dict[date, float]:
        """Fetch total market volume per day."""
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(_MARKET_VOL_SQL, {
                    "start_date": start_date,
                    "end_date": end_date,
                })
                return {
                    (row[0] if isinstance(row[0], date) else row[0].date()): float(row[1] or 0)
                    for row in cur.fetchall()
                }

    def _query_limit_up(
        self, sector_code: str, start_date: date, end_date: date,
    ) -> Dict[date, Dict[str, float]]:
        """Fetch limit-up counts per day for a sector."""
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(_LIMIT_UP_SQL, {
                    "sector_code": sector_code,
                    "start_date": start_date,
                    "end_date": end_date,
                })
                result: Dict[date, Dict[str, float]] = {}
                for row in cur.fetchall():
                    td = row[0] if isinstance(row[0], date) else row[0].date()
                    result[td] = {
                        "limit_up": float(row[1] or 0),
                        "total": float(row[2] or 0),
                    }
                return result


# ---------------------------------------------------------------------------
# SectorHMMInference
# ---------------------------------------------------------------------------

class SectorHMMInference:
    """Infer per-sector hidden states and output heat coefficients.

    Loads trained HMM parameters from a JSON file, reconstructs
    ``GaussianHMM`` objects for Viterbi decoding, and maintains a
    cooldown cache to prevent coefficient flapping.
    """

    def __init__(
        self,
        model_path: str,
        config: Optional[SectorHMMConfig] = None,
        *,
        db_conn_factory=None,
    ):
        self.config = config or SectorHMMConfig()
        self._model_path = model_path
        self._get_conn = db_conn_factory or _get_db_engine()

        # Load raw model parameters from JSON
        self._raw_models: Dict[str, Any] = SectorHMMTrainer.load_models(model_path)

        # Reconstruct GaussianHMM objects for Viterbi decoding
        self._hmm_models: Dict[str, Any] = {}
        self._state_labels: Dict[str, Dict[str, str]] = {}
        self._reconstruct_models()

        # Internal trainer instance for building observation matrices
        self._trainer = SectorHMMTrainer(
            config=self.config,
            db_conn_factory=db_conn_factory,
        )

        # Preloaded data caches (populated by preload_data())
        self._sector_data_cache: Optional[Dict[str, Dict]] = None
        self._csi300_cache: Optional[Dict] = None
        self._market_vol_cache: Optional[Dict] = None

        # Cooldown state cache:
        # {sector_code: {"last_state": str, "last_switch_date": date, "current_coeff": float}}
        self._cooldown_cache: Dict[str, Dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # Model reconstruction
    # ------------------------------------------------------------------

    def _reconstruct_models(self) -> None:
        """Rebuild ``GaussianHMM`` objects from stored parameters."""
        try:
            from hmmlearn.hmm import GaussianHMM
        except ImportError:
            logger.error("hmmlearn is not installed; inference will return neutral coefficients.")
            return

        for sector_code, params in self._raw_models.items():
            try:
                n_states = params.get("n_states", self.config.n_states)
                cov_type = params.get("covariance_type", "full")
                hmm = GaussianHMM(
                    n_components=n_states,
                    covariance_type=cov_type,
                )
                hmm.startprob_ = np.full(n_states, 1.0 / n_states)
                hmm.transmat_ = np.array(params["transmat"], dtype=np.float64)
                hmm.means_ = np.array(params["means"], dtype=np.float64)
                covars = np.array(params["covars"], dtype=np.float64)
                if cov_type == "full":
                    # Force symmetry on covariance matrices (JSON round-trip precision loss)
                    for i in range(covars.shape[0]):
                        covars[i] = (covars[i] + covars[i].T) / 2
                        covars[i] += np.eye(covars[i].shape[0]) * 1e-6
                elif cov_type == "diag":
                    # diag covars: shape (n_states, n_features) or (n_states, n_features, n_features)
                    if covars.ndim == 3:
                        covars = np.array([np.diag(covars[i]) for i in range(covars.shape[0])])
                    covars = np.maximum(covars, 1e-6)
                hmm.covars_ = covars

                self._hmm_models[sector_code] = hmm
                self._state_labels[sector_code] = params.get("state_labels", {})
            except Exception:
                logger.warning(
                    "Failed to reconstruct HMM for sector %s, will use neutral coeff.",
                    sector_code, exc_info=True,
                )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def preload_data(self, start_date: date, end_date: date) -> None:
        """Batch-load all data into memory for fast inference.

        Delegates to the internal trainer's preload_data(), then copies
        the caches to self so _build_obs_7dim() can use them directly.
        """
        self._trainer.preload_data(start_date, end_date)
        # Copy caches from trainer to self (used by _build_obs_7dim)
        self._sector_data_cache = self._trainer._sector_data_cache
        self._csi300_cache = self._trainer._csi300_cache
        self._market_vol_cache = self._trainer._market_vol_cache

    def get_sector_coefficients(self, trade_date: date) -> Dict[str, float]:
        """Return ``{sector_code: heat_coefficient}`` for all trained sectors.

        Mapping:
            - trending → ``config.trending_coeff`` (default 1.5)
            - fading   → ``config.fading_coeff``   (default 0.5)
            - no model → ``config.neutral_coeff``   (default 1.0)
            - cooldown → previous coefficient value
        """
        coefficients: Dict[str, float] = {}

        for sector_code in self._raw_models:
            if sector_code not in self._hmm_models:
                # Model reconstruction failed – neutral
                coefficients[sector_code] = self.config.neutral_coeff
                continue

            try:
                decoded_state = self._decode_state(sector_code, trade_date)
            except Exception:
                logger.warning(
                    "Viterbi decode failed for %s on %s, using neutral coeff.",
                    sector_code, trade_date, exc_info=True,
                )
                coefficients[sector_code] = self.config.neutral_coeff
                continue

            # Map state label to coefficient
            if decoded_state == "trending":
                new_coeff = self.config.trending_coeff
            elif decoded_state == "fading":
                new_coeff = self.config.fading_coeff
            else:
                new_coeff = self.config.neutral_coeff

            # Check cooldown
            in_cooldown = self._check_cooldown(sector_code, decoded_state, trade_date)

            if in_cooldown:
                # During cooldown, keep previous coefficient
                cached = self._cooldown_cache.get(sector_code)
                coefficients[sector_code] = (
                    cached["current_coeff"] if cached else self.config.neutral_coeff
                )
            else:
                # Update cache if state changed
                cached = self._cooldown_cache.get(sector_code)
                if cached is None or cached["last_state"] != decoded_state:
                    self._cooldown_cache[sector_code] = {
                        "last_state": decoded_state,
                        "last_switch_date": trade_date,
                        "current_coeff": new_coeff,
                    }
                else:
                    # Same state, just update coeff (no switch)
                    cached["current_coeff"] = new_coeff

                coefficients[sector_code] = new_coeff

        return coefficients

    # ------------------------------------------------------------------
    # Viterbi decoding
    # ------------------------------------------------------------------

    def _decode_state(self, sector_code: str, trade_date: date) -> str:
        """Forward-filter the hidden state for *sector_code* on *trade_date*.

        Builds the observation matrix up to *trade_date*, then uses the
        forward algorithm (not Viterbi) to compute the marginal posterior
        probability of each state at the last time step.  This is strictly
        causal — only observations up to *trade_date* influence the result.
        """
        hmm = self._hmm_models[sector_code]
        labels = self._state_labels[sector_code]

        # Build observation matrix up to trade_date
        obs = self._build_obs_up_to(sector_code, trade_date)

        if obs.shape[0] == 0:
            raise ValueError(
                f"No observation data for sector {sector_code} up to {trade_date}"
            )

        # Forward filtering — causal, no look-ahead
        from hmmlearn import _hmmc
        log_frameprob = hmm._compute_log_likelihood(obs)
        _, fwd_lattice = _hmmc.forward_log(
            hmm.startprob_, hmm.transmat_, log_frameprob,
        )
        # fwd_lattice[-1] contains log P(state, o_1:T) — marginalize to posterior
        log_posterior = fwd_lattice[-1]
        log_posterior = log_posterior - np.max(log_posterior)  # numerical stability
        posteriors = np.exp(log_posterior)
        posteriors /= posteriors.sum()

        last_state_idx = int(posteriors.argmax())
        state_label = labels.get(str(last_state_idx), "unknown")

        return state_label

    def _build_obs_up_to(self, sector_code: str, trade_date: date) -> np.ndarray:
        """Build observation matrix from history start up to *trade_date*.

        Reads model metadata (rolling_window, obs_features, zscore params)
        to build the same observation matrix used during training.
        Uses sector_data table (L2 level) + CSI300 + market volume.
        """
        start_date = trade_date - timedelta(
            days=int(self.config.history_years * 365)
        )

        model_info = self._raw_models.get(sector_code, {})
        rolling_window = model_info.get("rolling_window", 20)
        n_features = len(model_info.get("means", [[]])[0]) if model_info.get("means") else 4

        # For new 7-dim models, query sector_data table (has money flow data)
        if n_features >= 7:
            return self._build_obs_7dim(sector_code, start_date, trade_date, rolling_window, model_info)

        # Legacy 4-dim models: use old logic
        l2_codes = model_info.get("l2_codes")
        sector_daily = self._trainer._query_sector_daily(
            sector_code, start_date, trade_date, l2_codes=l2_codes,
        )
        csi300_daily = self._trainer._query_csi300_daily(start_date, trade_date)
        market_vol = self._trainer._query_market_volume(start_date, trade_date)
        limit_up = self._trainer._query_limit_up(sector_code, start_date, trade_date)

        dates = sorted(sector_daily.keys())
        rows: List[List[float]] = []
        win = max(rolling_window, 2)

        for i, td in enumerate(dates):
            sec = sector_daily.get(td)
            csi = csi300_daily.get(td)
            mvol = market_vol.get(td)
            lu = limit_up.get(td)

            if sec is None or csi is None or mvol is None:
                continue

            daily_ret = sec["pct_change"] / 100.0

            window_excesses: List[float] = []
            for j in range(max(0, i - win + 1), i + 1):
                d2 = dates[j]
                s2 = sector_daily.get(d2)
                c2 = csi300_daily.get(d2)
                if s2 is not None and c2 is not None:
                    window_excesses.append(
                        s2["pct_change"] / 100.0 - c2["pct_change"] / 100.0
                    )
            if not window_excesses:
                continue
            excess_mean = sum(window_excesses) / len(window_excesses)

            vol_ratio = sec["vol"] / mvol if mvol > 0 else 0.0

            if lu is not None and lu["total"] > 0:
                lu_ratio = lu["limit_up"] / lu["total"]
            else:
                lu_ratio = 0.0

            row = [daily_ret, excess_mean, vol_ratio, lu_ratio]
            if any(np.isnan(v) for v in row):
                continue
            rows.append(row)

        return np.array(rows, dtype=np.float64) if rows else np.empty((0, 4), dtype=np.float64)

    def _build_obs_7dim(
        self, sector_code: str, start_date: date, end_date: date,
        rolling_window: int, model_info: Dict,
    ) -> np.ndarray:
        """Build 7-dim observation matrix for new models using sector_data table.

        Features: daily_return, excess_return_Nd, volume_ratio, limit_up_ratio,
                  volatility_Nd, net_mf_ratio, elg_net_mf_ratio

        Supports batch-preloaded data via preload_data() for massive speedup.
        Falls back to per-query DB access if no cache loaded.
        """
        # ── 使用预加载缓存 ──
        if self._sector_data_cache is not None:
            sector_dict = self._sector_data_cache.get(sector_code, {})
            sorted_data = [
                {"trade_date": td, **v}
                for td, v in sorted(sector_dict.items())
                if start_date <= td <= end_date
            ]
            csi300 = self._csi300_cache or {}
            market_vol = self._market_vol_cache or {}
        else:
            # 回退：逐次查 DB
            from psycopg2.extras import RealDictCursor

            with self._get_conn() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT DISTINCT ON (m.l2_code, sd.trade_date)
                            sd.trade_date, sd.sw2_pct_change, sd.sw2_vol, sd.sw2_amount,
                            sd.sw2_mf_net_amt, sd.sw2_mf_buy_elg_amt, sd.sw2_mf_sell_elg_amt
                        FROM market.sector_data sd
                        JOIN market.sw_index_member m ON sd.ts_code = m.ts_code
                        WHERE m.l2_code = %s
                          AND sd.trade_date BETWEEN %s AND %s
                          AND m.in_date <= sd.trade_date
                          AND (m.out_date IS NULL OR m.out_date >= sd.trade_date)
                        ORDER BY m.l2_code, sd.trade_date, sd.ts_code
                    """, (sector_code, start_date, end_date))
                    sector_rows = cur.fetchall()

                    cur.execute("""
                        SELECT trade_date, pct_chg FROM market.index_daily
                        WHERE ts_code = '000300.SH' AND trade_date BETWEEN %s AND %s
                        ORDER BY trade_date
                    """, (start_date, end_date))
                    csi300 = {r["trade_date"]: float(r["pct_chg"] or 0) for r in cur.fetchall()}

                    cur.execute("""
                        SELECT trade_date, SUM(vol) FROM market.sw_daily
                        WHERE trade_date BETWEEN %s AND %s
                        GROUP BY trade_date ORDER BY trade_date
                    """, (start_date, end_date))
                    market_vol = {r["trade_date"]: float(r["sum"] or 0) for r in cur.fetchall()}

            if not sector_rows:
                return np.empty((0, 7), dtype=np.float64)

            sorted_data = sorted(sector_rows, key=lambda x: x["trade_date"])
        win = max(rolling_window, 2)
        rows: List[List[float]] = []

        for i, rec in enumerate(sorted_data):
            td = rec["trade_date"]
            pct = float(rec["sw2_pct_change"] or 0)
            vol = float(rec["sw2_vol"] or 0)
            amount = float(rec["sw2_amount"] or 0)
            mf_net = float(rec["sw2_mf_net_amt"] or 0)
            mf_buy_elg = float(rec["sw2_mf_buy_elg_amt"] or 0)
            mf_sell_elg = float(rec["sw2_mf_sell_elg_amt"] or 0)

            csi_pct = csi300.get(td)
            mvol = market_vol.get(td)
            if csi_pct is None or mvol is None:
                continue

            daily_ret = pct / 100.0

            # Excess return N-day mean
            csi_window = []
            for j in range(max(0, i - win + 1), i + 1):
                d2 = sorted_data[j]["trade_date"]
                c2 = csi300.get(d2)
                if c2 is not None:
                    csi_window.append(float(sorted_data[j]["sw2_pct_change"] or 0) / 100.0 - c2 / 100.0)
            excess_nd = float(np.mean(csi_window)) if csi_window else 0.0

            vol_ratio = vol / mvol if mvol > 0 else 0.0
            lu_ratio = 0.0  # Limit-up not available in real-time inference (Qlib bin is WSL-only)

            # N-day volatility
            ret_window = [float(sorted_data[j]["sw2_pct_change"] or 0) / 100.0
                          for j in range(max(0, i - win + 1), i + 1)]
            volatility = float(np.std(ret_window)) if len(ret_window) > 1 else 0.0

            mf_net_ratio = mf_net / amount if amount > 0 else 0.0
            elg_net = mf_buy_elg - mf_sell_elg
            elg_ratio = elg_net / amount if amount > 0 else 0.0

            row = [daily_ret, excess_nd, vol_ratio, lu_ratio, volatility, mf_net_ratio, elg_ratio]
            if any(np.isnan(v) or np.isinf(v) for v in row):
                continue
            rows.append(row)

        if not rows:
            return np.empty((0, 7), dtype=np.float64)

        obs = np.array(rows, dtype=np.float64)

        # Apply z-score if model was trained with it
        if "zscore_mean" in model_info:
            zscore_mean = np.array(model_info["zscore_mean"])
            zscore_std = np.array(model_info["zscore_std"])
            obs = (obs - zscore_mean) / zscore_std

        return obs

    # ------------------------------------------------------------------
    # Cooldown logic
    # ------------------------------------------------------------------

    def _check_cooldown(
        self, sector_code: str, new_state: str, trade_date: date,
    ) -> bool:
        """Return ``True`` if *sector_code* is still within the cooldown window.

        A cooldown is active when:
        1. The sector has a cached state, AND
        2. The new decoded state differs from the cached state, AND
        3. Fewer than ``cooldown_days`` calendar days have elapsed since the
           last state switch.
        """
        cached = self._cooldown_cache.get(sector_code)
        if cached is None:
            # First time seeing this sector – no cooldown
            return False

        if cached["last_state"] == new_state:
            # Same state – no cooldown needed
            return False

        # State differs – check if cooldown period has elapsed
        days_since_switch = (trade_date - cached["last_switch_date"]).days
        if days_since_switch < self.config.cooldown_days:
            return True

        # Cooldown expired
        return False
