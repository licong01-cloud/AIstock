#!/usr/bin/env python3
"""Offline screen for redefined sector-rotation HMM models.

This script intentionally does not register HMM snapshots, does not write DB
rows, and does not submit QE tasks. It trains unsupervised HMM candidates with
state utility labels derived from future sector-rotation targets, then compares
their sector ranking quality with existing precomputed HMM coefficient assets.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
from hmmlearn.hmm import GaussianHMM


ROOT = Path(__file__).resolve().parents[1]
HORIZONS = (5, 10, 20)
HORIZON_WEIGHTS = {5: 0.35, 10: 0.35, 20: 0.30}
DEFAULT_OUTPUT_DIR = ROOT / ".codex_tmp" / "hmm_sector_rotation_redefine_20260509"


ABS_FEATURES = (
    "ret_1d",
    "excess_3d",
    "excess_5d",
    "excess_10d",
    "excess_20d",
    "volatility_10d",
    "vol_share_5d",
    "net_mf_ratio_5d",
    "elg_mf_ratio_5d",
)

XRANK_FEATURES = (
    "xrank_excess_3d",
    "xrank_excess_5d",
    "xrank_excess_10d",
    "xrank_excess_20d",
    "xrank_vol_share_5d",
    "xrank_net_mf_ratio_5d",
    "xrank_elg_mf_ratio_5d",
    "xrank_volatility_10d_neg",
)

MARKET_FEATURES = (
    "mkt_ret_5d",
    "mkt_ret_20d",
    "mkt_volatility_20d",
    "mkt_drawdown_20d",
)

RISK_FEATURES = (
    "xrank_volatility_20d_neg",
    "xrank_vol_share_20d_neg",
    "xrank_mf_noise_5d_neg",
)

DAILY_REGIME_FEATURES = (
    "mkt_ret_5d",
    "mkt_ret_20d",
    "mkt_volatility_20d",
    "mkt_drawdown_20d",
    "day_excess_5d_std",
    "day_excess_20d_spread",
    "day_net_mf_5d_std",
    "day_volatility_20d_std",
)


@dataclass(frozen=True)
class DBConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str


@dataclass(frozen=True)
class CandidateSpec:
    name: str
    description: str
    scope: str
    features: tuple[str, ...]
    target: str
    n_states: int = 3
    min_self_trans: float = 0.76
    alpha_smooth: float = 0.08
    covariance_type: str = "diag"
    preprocess: str = "winsor_zscore"
    daily_features: tuple[str, ...] = ()
    ridge_alpha: float = 5.0


CANDIDATES: tuple[CandidateSpec, ...] = (
    CandidateSpec(
        name="ROT_RELRANK_PER_SECTOR_v1",
        description=(
            "Per-sector HMM using absolute sector observations; hidden states "
            "are labeled by future 5/10/20d cross-sectional relative-rank utility."
        ),
        scope="per_sector",
        features=ABS_FEATURES,
        target="target_rank_utility",
    ),
    CandidateSpec(
        name="ROT_XRANK_PER_SECTOR_v1",
        description=(
            "Per-sector HMM using same-day industry cross-sectional rank features; "
            "designed to make emissions directly comparable for sector rotation."
        ),
        scope="per_sector",
        features=XRANK_FEATURES,
        target="target_rank_utility",
    ),
    CandidateSpec(
        name="ROT_XRANK_POOLED_GLOBAL_v1",
        description=(
            "One pooled HMM trained on all sectors with per-sector sequence lengths; "
            "tests whether a shared rotation-state machine is stronger than 131 "
            "separate HMMs."
        ),
        scope="pooled",
        features=XRANK_FEATURES,
        target="target_rank_utility",
    ),
    CandidateSpec(
        name="ROT_MKTCOND_POOLED_v1",
        description=(
            "Pooled sector-rotation HMM with market trend, volatility, and drawdown "
            "conditioning features."
        ),
        scope="pooled",
        features=XRANK_FEATURES + MARKET_FEATURES,
        target="target_rank_utility",
    ),
    CandidateSpec(
        name="ROT_TOPBOTTOM_STICKY_v1",
        description=(
            "Pooled sticky-HMM approximation: top/bottom sector-spread objective "
            "and higher self-transition floor to reduce state churn."
        ),
        scope="pooled",
        features=XRANK_FEATURES + MARKET_FEATURES,
        target="target_topbot_utility",
        n_states=4,
        min_self_trans=0.88,
        alpha_smooth=0.12,
    ),
    CandidateSpec(
        name="ROT_DRAWDOWN_RISK_v1",
        description=(
            "Pooled market-conditioned HMM with explicit low-volatility, low-crowding, "
            "and flow-noise risk-state features."
        ),
        scope="pooled",
        features=XRANK_FEATURES + MARKET_FEATURES + RISK_FEATURES,
        target="target_rank_utility",
        n_states=4,
        min_self_trans=0.84,
        alpha_smooth=0.12,
    ),
    CandidateSpec(
        name="ROT_REGIME_LINEAR_v1",
        description=(
            "Daily market/dispersion regime HMM; each hidden regime owns a train-only "
            "ridge map from sector cross-sectional features to future relative-rank utility."
        ),
        scope="daily_regime_linear",
        features=XRANK_FEATURES,
        daily_features=DAILY_REGIME_FEATURES,
        target="target_rank_utility",
        n_states=3,
        min_self_trans=0.82,
        alpha_smooth=0.10,
        ridge_alpha=8.0,
    ),
    CandidateSpec(
        name="ROT_REGIME_TOPBOT_LINEAR_v1",
        description=(
            "Daily regime HMM plus state-specific ridge maps trained on top/bottom "
            "sector-spread utility instead of smooth relative rank."
        ),
        scope="daily_regime_linear",
        features=XRANK_FEATURES + RISK_FEATURES,
        daily_features=DAILY_REGIME_FEATURES,
        target="target_topbot_utility",
        n_states=4,
        min_self_trans=0.86,
        alpha_smooth=0.12,
        ridge_alpha=12.0,
    ),
)


def parse_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def json_default(value: Any) -> Any:
    if isinstance(value, (date, datetime, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, np.ndarray):
        return value.tolist()
    return str(value)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=json_default),
        encoding="utf-8",
    )


def read_env_file(path: Path) -> None:
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8-sig", errors="ignore").splitlines():
        text = line.strip()
        if not text or text.startswith("#") or "=" not in text:
            continue
        key, value = text.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def windows_to_wsl_path(path: str) -> str:
    text = str(path).replace("\\", "/")
    if len(text) >= 2 and text[1] == ":":
        return f"/mnt/{text[0].lower()}{text[2:]}"
    return text


def candidate_hosts(initial: str) -> list[str]:
    hosts: list[str] = []
    for item in (initial, os.getenv("TDX_DB_HOST"), "127.0.0.1", "localhost"):
        if item and item not in hosts:
            hosts.append(str(item))
    try:
        ip = subprocess.check_output(
            "sed -n 's/^nameserver //p' /etc/resolv.conf | head -1",
            shell=True,
            text=True,
            timeout=3,
        ).strip()
        if ip and ip not in hosts:
            hosts.append(ip)
    except Exception:
        pass
    return hosts


def connect_readonly(cfg: DBConfig):
    errors: list[str] = []
    for host in candidate_hosts(cfg.host):
        try:
            conn = psycopg2.connect(
                host=host,
                port=cfg.port,
                dbname=cfg.dbname,
                user=cfg.user,
                password=cfg.password,
                connect_timeout=5,
            )
            conn.set_session(readonly=True, autocommit=True)
            print(f"DB connected readonly via host={host}")
            return conn, host
        except Exception as exc:
            errors.append(f"{host}: {str(exc).splitlines()[0]}")
    raise RuntimeError("Cannot connect to DB. Tried: " + "; ".join(errors))


def safe_tstat(values: Iterable[float]) -> float | None:
    arr = pd.Series(list(values), dtype="float64").replace([np.inf, -np.inf], np.nan).dropna()
    if len(arr) < 2:
        return None
    std = float(arr.std(ddof=1))
    if std <= 0 or math.isnan(std):
        return None
    return float(arr.mean() / (std / math.sqrt(len(arr))))


def rank_scale_series(series: pd.Series) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    valid = values.notna()
    out = pd.Series(np.nan, index=series.index, dtype="float64")
    n = int(valid.sum())
    if n <= 1:
        out.loc[valid] = 0.0
        return out
    ranks = values.loc[valid].rank(method="average", pct=True)
    out.loc[valid] = ranks * 2.0 - 1.0
    return out


def zscore_series(series: pd.Series) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    mean = values.mean(skipna=True)
    std = values.std(skipna=True, ddof=0)
    if not std or math.isnan(float(std)):
        return pd.Series(0.0, index=series.index, dtype="float64")
    return ((values - mean) / std).astype("float64")


def trailing_sum(values: pd.Series, window: int) -> pd.Series:
    return values.rolling(window=window, min_periods=max(2, min(window, 5))).sum()


def trailing_mean(values: pd.Series, window: int) -> pd.Series:
    return values.rolling(window=window, min_periods=max(2, min(window, 5))).mean()


def trailing_std(values: pd.Series, window: int) -> pd.Series:
    return values.rolling(window=window, min_periods=max(2, min(window, 5))).std(ddof=0)


def future_sum(values: pd.Series, horizon: int) -> pd.Series:
    parts = [values.shift(-step) for step in range(1, horizon + 1)]
    return pd.concat(parts, axis=1).sum(axis=1, min_count=horizon)


def fetch_sector_rows(conn, start: date, end: date) -> pd.DataFrame:
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT DISTINCT ON (m.l2_code, sd.trade_date)
            m.l2_code AS sector_code,
            COALESCE(m.l2_name, m.l2_code) AS sector_name,
            sd.trade_date,
            sd.sw2_pct_change,
            sd.sw2_vol,
            sd.sw2_amount,
            sd.sw2_mf_net_amt,
            sd.sw2_mf_buy_elg_amt,
            sd.sw2_mf_sell_elg_amt
        FROM market.sector_data sd
        JOIN market.sw_index_member m ON sd.ts_code = m.ts_code
        WHERE sd.trade_date BETWEEN %s AND %s
          AND m.in_date <= sd.trade_date
          AND (m.out_date IS NULL OR m.out_date >= sd.trade_date)
        ORDER BY m.l2_code, sd.trade_date, sd.ts_code
        """,
        (start, end),
    )
    rows = cur.fetchall()
    cur.close()
    return pd.DataFrame(rows)


def fetch_index_returns(conn, start: date, end: date) -> pd.DataFrame:
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    for code in ("000300.SH", "399300.SZ"):
        cur.execute(
            """
            SELECT trade_date, pct_chg
            FROM market.index_daily
            WHERE ts_code = %s AND trade_date BETWEEN %s AND %s
            ORDER BY trade_date
            """,
            (code, start, end),
        )
        rows = cur.fetchall()
        if rows:
            cur.close()
            df = pd.DataFrame(rows)
            df["bench_ret"] = pd.to_numeric(df["pct_chg"], errors="coerce").fillna(0.0) / 100.0
            df = df[["trade_date", "bench_ret"]].sort_values("trade_date")
            df["mkt_ret_5d"] = trailing_sum(df["bench_ret"], 5)
            df["mkt_ret_20d"] = trailing_sum(df["bench_ret"], 20)
            df["mkt_volatility_20d"] = trailing_std(df["bench_ret"], 20)
            index_level = (1.0 + df["bench_ret"]).cumprod()
            rolling_max = index_level.rolling(window=20, min_periods=5).max()
            df["mkt_drawdown_20d"] = index_level / rolling_max - 1.0
            print(f"Loaded benchmark {code}: {len(df)} rows")
            return df.drop(columns=["bench_ret"])
    cur.close()
    raise RuntimeError("No CSI300 benchmark rows found")


def fetch_market_volume(conn, start: date, end: date) -> pd.DataFrame:
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT trade_date, SUM(vol) AS market_vol
        FROM market.sw_daily
        WHERE trade_date BETWEEN %s AND %s
        GROUP BY trade_date
        ORDER BY trade_date
        """,
        (start, end),
    )
    rows = cur.fetchall()
    cur.close()
    return pd.DataFrame(rows)


def build_panel(sector_rows: pd.DataFrame, index_df: pd.DataFrame, market_vol_df: pd.DataFrame) -> pd.DataFrame:
    if sector_rows.empty:
        raise RuntimeError("No sector rows loaded")

    df = sector_rows.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    index_df = index_df.copy()
    market_vol_df = market_vol_df.copy()
    index_df["trade_date"] = pd.to_datetime(index_df["trade_date"]).dt.date
    market_vol_df["trade_date"] = pd.to_datetime(market_vol_df["trade_date"]).dt.date

    # Keep raw benchmark return for excess-return construction.
    bench_for_excess = index_df[["trade_date"]].copy()
    cur_cols = ["trade_date", "pct_chg"]
    # The index_df passed to this function no longer has pct_chg; rebuild from
    # market feature mkt_ret_5d when unavailable is not valid, so fetch-sector
    # joins compute excess after a second light merge below.
    del bench_for_excess, cur_cols

    # Recreate a benchmark return table from input by reloading mkt 1d is not
    # possible here; it is injected by caller through a private column.
    if "_bench_ret" not in index_df.columns:
        raise RuntimeError("index_df must contain _bench_ret for panel construction")

    df = df.merge(index_df, on="trade_date", how="left")
    df = df.merge(market_vol_df, on="trade_date", how="left")
    for col in (
        "sw2_pct_change",
        "sw2_vol",
        "sw2_amount",
        "sw2_mf_net_amt",
        "sw2_mf_buy_elg_amt",
        "sw2_mf_sell_elg_amt",
        "market_vol",
        "_bench_ret",
    ):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df[df["market_vol"].fillna(0.0) > 0].copy()

    df["ret_1d"] = df["sw2_pct_change"].fillna(0.0) / 100.0
    df["excess_1d"] = df["ret_1d"] - df["_bench_ret"].fillna(0.0)
    df["vol_share_1d"] = df["sw2_vol"].fillna(0.0) / df["market_vol"]
    amount = df["sw2_amount"].replace(0.0, np.nan)
    df["net_mf_ratio_1d"] = df["sw2_mf_net_amt"].fillna(0.0) / amount
    df["elg_mf_ratio_1d"] = (
        df["sw2_mf_buy_elg_amt"].fillna(0.0) - df["sw2_mf_sell_elg_amt"].fillna(0.0)
    ) / amount
    df["net_mf_ratio_1d"] = df["net_mf_ratio_1d"].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    df["elg_mf_ratio_1d"] = df["elg_mf_ratio_1d"].replace([np.inf, -np.inf], np.nan).fillna(0.0)

    pieces: list[pd.DataFrame] = []
    for sector_code, g in df.sort_values(["sector_code", "trade_date"]).groupby("sector_code", sort=True):
        g = g.copy().sort_values("trade_date")
        for window in (3, 5, 10, 20):
            g[f"excess_{window}d"] = trailing_mean(g["excess_1d"], window)
            g[f"vol_share_{window}d"] = trailing_mean(g["vol_share_1d"], window)
            g[f"volatility_{window}d"] = trailing_std(g["ret_1d"], window)
        g["net_mf_ratio_5d"] = trailing_mean(g["net_mf_ratio_1d"], 5)
        g["elg_mf_ratio_5d"] = trailing_mean(g["elg_mf_ratio_1d"], 5)
        g["mf_noise_5d"] = trailing_std(g["net_mf_ratio_1d"], 5)
        for horizon in HORIZONS:
            g[f"future_excess_{horizon}d"] = future_sum(g["excess_1d"], horizon)
        pieces.append(g)

    panel = pd.concat(pieces, ignore_index=True)
    panel = panel.sort_values(["trade_date", "sector_code"]).reset_index(drop=True)

    for col in (
        "excess_3d",
        "excess_5d",
        "excess_10d",
        "excess_20d",
        "vol_share_5d",
        "vol_share_20d",
        "volatility_10d",
        "volatility_20d",
        "net_mf_ratio_5d",
        "elg_mf_ratio_5d",
        "mf_noise_5d",
    ):
        panel[f"xrank_{col}"] = panel.groupby("trade_date", group_keys=False)[col].transform(rank_scale_series)
        panel[f"xz_{col}"] = panel.groupby("trade_date", group_keys=False)[col].transform(zscore_series)

    for col in ("volatility_10d", "volatility_20d", "vol_share_20d", "mf_noise_5d"):
        panel[f"xrank_{col}_neg"] = -panel[f"xrank_{col}"]

    for horizon in HORIZONS:
        future_col = f"future_excess_{horizon}d"
        rank_col = f"future_rank_{horizon}d"
        panel[rank_col] = panel.groupby("trade_date", group_keys=False)[future_col].transform(rank_scale_series)

        def _topbot(group: pd.Series) -> pd.Series:
            values = pd.to_numeric(group, errors="coerce")
            out = pd.Series(np.nan, index=group.index, dtype="float64")
            valid = values.dropna()
            if len(valid) < 10:
                return out
            lo = valid.quantile(0.20)
            hi = valid.quantile(0.80)
            out.loc[values <= lo] = -1.0
            out.loc[values >= hi] = 1.0
            out.loc[(values > lo) & (values < hi)] = 0.0
            return out

        panel[f"future_topbot_{horizon}d"] = panel.groupby("trade_date", group_keys=False)[future_col].transform(_topbot)

    panel["target_rank_utility"] = 0.0
    panel["target_topbot_utility"] = 0.0
    for horizon, weight in HORIZON_WEIGHTS.items():
        panel["target_rank_utility"] += weight * panel[f"future_rank_{horizon}d"]
        panel["target_topbot_utility"] += weight * panel[f"future_topbot_{horizon}d"]

    def _qspread(series: pd.Series) -> float:
        values = pd.to_numeric(series, errors="coerce").dropna()
        if len(values) < 5:
            return float("nan")
        return float(values.quantile(0.80) - values.quantile(0.20))

    daily_stats = panel.groupby("trade_date").agg(
        day_excess_5d_std=("excess_5d", "std"),
        day_excess_20d_spread=("excess_20d", _qspread),
        day_net_mf_5d_std=("net_mf_ratio_5d", "std"),
        day_volatility_20d_std=("volatility_20d", "std"),
    ).reset_index()
    panel = panel.merge(daily_stats, on="trade_date", how="left")

    return panel


def fit_preprocess(train_df: pd.DataFrame, features: tuple[str, ...], winsor_q: float) -> dict[str, Any]:
    obs = train_df.loc[:, list(features)].replace([np.inf, -np.inf], np.nan).dropna()
    if obs.empty:
        raise RuntimeError("No rows available for preprocessing")
    arr = obs.to_numpy(dtype=np.float64)
    lower = np.quantile(arr, winsor_q, axis=0)
    upper = np.quantile(arr, 1.0 - winsor_q, axis=0)
    clipped = np.clip(arr, lower, upper)
    mean = clipped.mean(axis=0)
    std = clipped.std(axis=0)
    std = np.where(std < 1e-10, 1.0, std)
    return {
        "winsor_q": winsor_q,
        "winsor_lower": lower.tolist(),
        "winsor_upper": upper.tolist(),
        "zscore_mean": mean.tolist(),
        "zscore_std": std.tolist(),
    }


def apply_preprocess(df: pd.DataFrame, features: tuple[str, ...], params: dict[str, Any]) -> np.ndarray:
    arr = df.loc[:, list(features)].to_numpy(dtype=np.float64)
    lower = np.asarray(params["winsor_lower"], dtype=np.float64)
    upper = np.asarray(params["winsor_upper"], dtype=np.float64)
    mean = np.asarray(params["zscore_mean"], dtype=np.float64)
    std = np.asarray(params["zscore_std"], dtype=np.float64)
    return (np.clip(arr, lower, upper) - mean) / std


def make_hmm(spec: CandidateSpec, args: argparse.Namespace, *, target_init: bool = True) -> GaussianHMM:
    return GaussianHMM(
        n_components=spec.n_states,
        covariance_type=spec.covariance_type,
        n_iter=args.n_iter,
        min_covar=args.min_covar,
        random_state=args.random_state,
        init_params="" if target_init else "stmc",
        params="stmc",
    )


def initialize_hmm_from_target(hmm: GaussianHMM, obs: np.ndarray, target: np.ndarray, spec: CandidateSpec, args: argparse.Namespace) -> None:
    n_states = spec.n_states
    hmm.startprob_ = np.full(n_states, 1.0 / n_states, dtype=np.float64)
    hmm.transmat_ = smooth_transition(np.eye(n_states, dtype=np.float64), spec.alpha_smooth, spec.min_self_trans)

    target = np.asarray(target, dtype=np.float64)
    finite = np.isfinite(target)
    if finite.sum() < n_states * 5:
        finite = np.ones(len(obs), dtype=bool)
        target = obs[:, 0]

    order = np.argsort(target[finite], kind="mergesort")
    finite_idx = np.where(finite)[0][order]
    bins = np.array_split(finite_idx, n_states)
    means = []
    covars = []
    global_mean = np.nanmean(obs, axis=0)
    global_var = np.nanvar(obs, axis=0) + args.min_covar
    for idx, bin_idx in enumerate(bins):
        if len(bin_idx) == 0:
            means.append(global_mean)
            covars.append(global_var)
            continue
        chunk = obs[bin_idx]
        means.append(np.nanmean(chunk, axis=0))
        covars.append(np.nanvar(chunk, axis=0) + args.min_covar)
    hmm.means_ = np.asarray(means, dtype=np.float64)
    if spec.covariance_type == "diag":
        hmm.covars_ = np.clip(np.asarray(covars, dtype=np.float64), args.min_covar, args.max_covar)
    else:
        mats = []
        for var in covars:
            mats.append(np.diag(np.clip(var, args.min_covar, args.max_covar)))
        hmm.covars_ = np.asarray(mats, dtype=np.float64)


def fix_diag_covariance(hmm: GaussianHMM, min_covar: float, max_covar: float) -> dict[str, Any]:
    covars = np.asarray(hmm.covars_, dtype=np.float64)
    diag = np.asarray([np.diag(covars[i]) for i in range(covars.shape[0])]) if covars.ndim == 3 else covars.copy()
    mask = (diag < min_covar) | (diag > max_covar)
    fixed = np.clip(diag, min_covar, max_covar)
    hmm.covars_ = fixed
    return {
        "covariance_fixed": bool(mask.sum()),
        "covariance_anomaly_count": int(mask.sum()),
        "covariance_min_after": float(np.min(fixed)),
        "covariance_max_after": float(np.max(fixed)),
    }


def smooth_transition(transmat: np.ndarray, alpha: float, min_self: float) -> np.ndarray:
    n = transmat.shape[0]
    smoothed = np.zeros_like(transmat, dtype=np.float64)
    for i in range(n):
        row = np.asarray(transmat[i], dtype=np.float64) + alpha
        row = row / row.sum()
        if row[i] < min_self:
            other_sum = row.sum() - row[i]
            row[i] = min_self
            for j in range(n):
                if j == i:
                    continue
                row[j] = row[j] * (1.0 - min_self) / other_sum if other_sum > 0 else (1.0 - min_self) / (n - 1)
        smoothed[i] = row / row.sum()
    return smoothed


def forward_posteriors(hmm: GaussianHMM, obs: np.ndarray) -> np.ndarray:
    from hmmlearn import _hmmc

    startprob = np.asarray(hmm.startprob_, dtype=np.float64)
    transmat = np.asarray(hmm.transmat_, dtype=np.float64)
    log_frameprob = hmm._compute_log_likelihood(obs)
    _, fwd_lattice = _hmmc.forward_log(startprob, transmat, log_frameprob)

    n_states = fwd_lattice.shape[1]
    finite = np.isfinite(fwd_lattice)
    safe_lattice = np.where(finite, fwd_lattice, -np.inf)
    max_log = np.max(safe_lattice, axis=1, keepdims=True)
    bad_rows = ~np.isfinite(max_log[:, 0])
    max_log[bad_rows, 0] = 0.0
    shifted = np.where(finite, fwd_lattice - max_log, -np.inf)
    post = np.exp(shifted)
    post = np.where(np.isfinite(post), post, 0.0)
    post_sum = post.sum(axis=1, keepdims=True)
    bad_rows = bad_rows | (post_sum[:, 0] <= 0) | (~np.isfinite(post_sum[:, 0]))
    post_sum[bad_rows, 0] = 1.0
    post = post / post_sum
    if np.any(bad_rows):
        post[bad_rows, :] = 1.0 / n_states
    return post


def state_utilities(post: np.ndarray, target: np.ndarray) -> dict[str, Any]:
    states = post.argmax(axis=1)
    utilities: dict[int, float] = {}
    samples: dict[int, int] = {}
    for state in range(post.shape[1]):
        mask = (states == state) & np.isfinite(target)
        samples[state] = int(mask.sum())
        utilities[state] = float(np.mean(target[mask])) if mask.any() else 0.0
    values = np.asarray([utilities[i] for i in range(post.shape[1])], dtype=np.float64)
    centered = values - float(values.mean())
    std = float(centered.std())
    if std > 1e-12:
        centered = centered / std
    return {
        "raw": {str(i): float(utilities[i]) for i in range(post.shape[1])},
        "score": {str(i): float(centered[i]) for i in range(post.shape[1])},
        "samples": {str(i): samples[i] for i in range(post.shape[1])},
        "ordered_states": [int(i) for i in np.argsort(values)],
    }


def scores_from_posteriors(post: np.ndarray, utility_info: dict[str, Any]) -> np.ndarray:
    score_vec = np.asarray([utility_info["score"][str(i)] for i in range(post.shape[1])], dtype=np.float64)
    return post @ score_vec


def valid_feature_frame(df: pd.DataFrame, features: tuple[str, ...], target: str | None = None) -> pd.DataFrame:
    needed = list(features) + ([target] if target else [])
    out = df.replace([np.inf, -np.inf], np.nan).dropna(subset=needed).copy()
    return out


def train_per_sector_candidate(panel: pd.DataFrame, spec: CandidateSpec, args: argparse.Namespace) -> tuple[pd.DataFrame, dict[str, Any]]:
    train_start = parse_date(args.train_start)
    train_end = parse_date(args.train_end)
    train_all = valid_feature_frame(
        panel[(panel["trade_date"] >= train_start) & (panel["trade_date"] <= train_end)],
        spec.features,
        spec.target,
    )
    preprocess = fit_preprocess(train_all, spec.features, args.winsor_q)
    predictions: list[pd.DataFrame] = []
    model_meta: dict[str, Any] = {}
    skipped: list[dict[str, Any]] = []
    cov_fixed_count = 0

    sector_codes = sorted(panel["sector_code"].dropna().unique().tolist())
    if args.max_sectors:
        sector_codes = sector_codes[: args.max_sectors]

    for idx, sector_code in enumerate(sector_codes):
        sector_df = panel[panel["sector_code"] == sector_code].sort_values("trade_date")
        train_df = valid_feature_frame(
            sector_df[(sector_df["trade_date"] >= train_start) & (sector_df["trade_date"] <= train_end)],
            spec.features,
            spec.target,
        )
        if len(train_df) < args.min_trading_days:
            skipped.append({"sector_code": sector_code, "reason": "insufficient_train_rows", "rows": len(train_df)})
            continue
        try:
            obs_train = apply_preprocess(train_df, spec.features, preprocess)
            hmm = make_hmm(spec, args, target_init=True)
            train_target = train_df[spec.target].to_numpy(dtype=np.float64)
            initialize_hmm_from_target(hmm, obs_train, train_target, spec, args)
            hmm.fit(obs_train)
            cov_stats = fix_diag_covariance(hmm, args.min_covar, args.max_covar)
            hmm.transmat_ = smooth_transition(hmm.transmat_, spec.alpha_smooth, spec.min_self_trans)
            train_post = forward_posteriors(hmm, obs_train)
            utility_info = state_utilities(train_post, train_target)

            full_df = valid_feature_frame(sector_df, spec.features)
            full_obs = apply_preprocess(full_df, spec.features, preprocess)
            full_post = forward_posteriors(hmm, full_obs)
            pred = full_df[["trade_date", "sector_code", "sector_name"]].copy()
            pred["candidate"] = spec.name
            pred["score"] = scores_from_posteriors(full_post, utility_info)
            pred["decoded_state"] = full_post.argmax(axis=1)
            predictions.append(pred)

            if cov_stats["covariance_fixed"]:
                cov_fixed_count += 1
            model_meta[sector_code] = {
                "sector_name": str(train_df["sector_name"].iloc[0]),
                "training_rows": int(len(train_df)),
                "state_utilities": utility_info,
                "transmat": hmm.transmat_.tolist(),
                "self_transition": {str(i): float(hmm.transmat_[i, i]) for i in range(spec.n_states)},
                **cov_stats,
            }
        except Exception as exc:
            skipped.append({"sector_code": sector_code, "reason": repr(exc), "rows": len(train_df)})
        if (idx + 1) % 25 == 0:
            print(f"  {spec.name}: trained/scored {idx + 1}/{len(sector_codes)} sectors")

    if not predictions:
        raise RuntimeError(f"{spec.name}: no predictions produced")
    meta = {
        "candidate": asdict(spec),
        "preprocess": preprocess,
        "model_count": len(model_meta),
        "skipped": skipped,
        "covariance_fixed_sector_count": cov_fixed_count,
        "models": model_meta,
    }
    return pd.concat(predictions, ignore_index=True), meta


def ridge_fit_predictor(train_df: pd.DataFrame, features: tuple[str, ...], target: str, alpha: float) -> np.ndarray:
    frame = valid_feature_frame(train_df, features, target)
    if len(frame) < max(20, len(features) * 3):
        raise ValueError("not enough rows for ridge fit")
    x = frame.loc[:, list(features)].to_numpy(dtype=np.float64)
    y = frame[target].to_numpy(dtype=np.float64)
    x = np.column_stack([np.ones(len(x), dtype=np.float64), x])
    xtx = x.T @ x
    penalty = np.eye(xtx.shape[0], dtype=np.float64) * alpha
    penalty[0, 0] = 0.0
    rhs = x.T @ y
    try:
        return np.linalg.solve(xtx + penalty, rhs)
    except np.linalg.LinAlgError:
        return np.linalg.pinv(xtx + penalty) @ rhs


def ridge_predict(df: pd.DataFrame, features: tuple[str, ...], weights: np.ndarray) -> np.ndarray:
    x = df.loc[:, list(features)].to_numpy(dtype=np.float64)
    x = np.column_stack([np.ones(len(x), dtype=np.float64), x])
    return x @ weights


def train_daily_regime_linear_candidate(panel: pd.DataFrame, spec: CandidateSpec, args: argparse.Namespace) -> tuple[pd.DataFrame, dict[str, Any]]:
    train_start = parse_date(args.train_start)
    train_end = parse_date(args.train_end)
    train_panel = valid_feature_frame(
        panel[(panel["trade_date"] >= train_start) & (panel["trade_date"] <= train_end)],
        spec.features + spec.daily_features,
        spec.target,
    )
    if train_panel.empty:
        raise RuntimeError(f"{spec.name}: no train rows")

    daily = panel[["trade_date", *spec.daily_features]].drop_duplicates("trade_date").sort_values("trade_date")
    daily_train = valid_feature_frame(
        daily[(daily["trade_date"] >= train_start) & (daily["trade_date"] <= train_end)],
        spec.daily_features,
    )
    if len(daily_train) < 120:
        raise RuntimeError(f"{spec.name}: not enough daily regime rows")
    daily_preprocess = fit_preprocess(daily_train, spec.daily_features, args.winsor_q)
    daily_obs = apply_preprocess(daily_train, spec.daily_features, daily_preprocess)
    # Initialize daily regimes by market/risk pressure, not by future sector labels.
    daily_init_target = (
        pd.to_numeric(daily_train["mkt_ret_20d"], errors="coerce").fillna(0.0)
        - pd.to_numeric(daily_train["mkt_volatility_20d"], errors="coerce").fillna(0.0)
        + pd.to_numeric(daily_train["day_excess_20d_spread"], errors="coerce").fillna(0.0)
    ).to_numpy(dtype=np.float64)
    hmm = make_hmm(spec, args, target_init=True)
    initialize_hmm_from_target(hmm, daily_obs, daily_init_target, spec, args)
    hmm.fit(daily_obs)
    cov_stats = fix_diag_covariance(hmm, args.min_covar, args.max_covar)
    hmm.transmat_ = smooth_transition(hmm.transmat_, spec.alpha_smooth, spec.min_self_trans)

    full_daily = valid_feature_frame(daily, spec.daily_features)
    full_daily_obs = apply_preprocess(full_daily, spec.daily_features, daily_preprocess)
    daily_post = forward_posteriors(hmm, full_daily_obs)
    full_daily = full_daily[["trade_date"]].copy()
    full_daily["regime_state"] = daily_post.argmax(axis=1)
    train_states = full_daily[(full_daily["trade_date"] >= train_start) & (full_daily["trade_date"] <= train_end)]
    train_with_state = train_panel.merge(train_states, on="trade_date", how="inner")

    global_weights = ridge_fit_predictor(train_with_state, spec.features, spec.target, spec.ridge_alpha)
    weights_by_state: dict[int, np.ndarray] = {}
    state_rows: dict[str, int] = {}
    for state in range(spec.n_states):
        state_df = train_with_state[train_with_state["regime_state"] == state]
        state_rows[str(state)] = int(len(state_df))
        try:
            weights_by_state[state] = ridge_fit_predictor(state_df, spec.features, spec.target, spec.ridge_alpha)
        except ValueError:
            weights_by_state[state] = global_weights

    score_frame = valid_feature_frame(panel, spec.features + spec.daily_features).merge(full_daily, on="trade_date", how="inner")
    score_parts: list[pd.DataFrame] = []
    for state, group in score_frame.groupby("regime_state", sort=True):
        weights = weights_by_state.get(int(state), global_weights)
        pred = group[["trade_date", "sector_code", "sector_name"]].copy()
        pred["candidate"] = spec.name
        pred["score"] = ridge_predict(group, spec.features, weights)
        pred["decoded_state"] = int(state)
        score_parts.append(pred)
    if not score_parts:
        raise RuntimeError(f"{spec.name}: no predictions produced")

    meta = {
        "candidate": asdict(spec),
        "daily_preprocess": daily_preprocess,
        "model_count": 1,
        "daily_regime_rows": int(len(daily_train)),
        "state_training_rows": state_rows,
        "global_weights": global_weights.tolist(),
        "state_weights": {str(k): v.tolist() for k, v in weights_by_state.items()},
        "transmat": hmm.transmat_.tolist(),
        "self_transition": {str(i): float(hmm.transmat_[i, i]) for i in range(spec.n_states)},
        **cov_stats,
    }
    return pd.concat(score_parts, ignore_index=True), meta


def train_pooled_candidate(panel: pd.DataFrame, spec: CandidateSpec, args: argparse.Namespace) -> tuple[pd.DataFrame, dict[str, Any]]:
    train_start = parse_date(args.train_start)
    train_end = parse_date(args.train_end)
    train_panel = valid_feature_frame(
        panel[(panel["trade_date"] >= train_start) & (panel["trade_date"] <= train_end)],
        spec.features,
        spec.target,
    )
    preprocess = fit_preprocess(train_panel, spec.features, args.winsor_q)

    sector_codes = sorted(panel["sector_code"].dropna().unique().tolist())
    if args.max_sectors:
        sector_codes = sector_codes[: args.max_sectors]

    train_obs_parts: list[np.ndarray] = []
    train_target_parts: list[np.ndarray] = []
    lengths: list[int] = []
    skipped: list[dict[str, Any]] = []
    for sector_code in sector_codes:
        sector_train = valid_feature_frame(
            train_panel[train_panel["sector_code"] == sector_code].sort_values("trade_date"),
            spec.features,
            spec.target,
        )
        if len(sector_train) < args.min_trading_days:
            skipped.append({"sector_code": sector_code, "reason": "insufficient_train_rows", "rows": len(sector_train)})
            continue
        train_obs_parts.append(apply_preprocess(sector_train, spec.features, preprocess))
        train_target_parts.append(sector_train[spec.target].to_numpy(dtype=np.float64))
        lengths.append(len(sector_train))

    if not train_obs_parts:
        raise RuntimeError(f"{spec.name}: no pooled training sequences")

    hmm = make_hmm(spec, args, target_init=True)
    train_obs = np.vstack(train_obs_parts)
    train_target_all = np.concatenate(train_target_parts)
    initialize_hmm_from_target(hmm, train_obs, train_target_all, spec, args)
    hmm.fit(train_obs, lengths=lengths)
    cov_stats = fix_diag_covariance(hmm, args.min_covar, args.max_covar)
    hmm.transmat_ = smooth_transition(hmm.transmat_, spec.alpha_smooth, spec.min_self_trans)

    train_post_parts: list[np.ndarray] = []
    offset = 0
    for length in lengths:
        part = train_obs[offset : offset + length]
        train_post_parts.append(forward_posteriors(hmm, part))
        offset += length
    utility_info = state_utilities(np.vstack(train_post_parts), train_target_all)

    predictions: list[pd.DataFrame] = []
    scored_sector_count = 0
    for sector_code in sector_codes:
        sector_df = valid_feature_frame(panel[panel["sector_code"] == sector_code].sort_values("trade_date"), spec.features)
        if len(sector_df) < args.min_scoring_days:
            continue
        try:
            obs = apply_preprocess(sector_df, spec.features, preprocess)
            post = forward_posteriors(hmm, obs)
            pred = sector_df[["trade_date", "sector_code", "sector_name"]].copy()
            pred["candidate"] = spec.name
            pred["score"] = scores_from_posteriors(post, utility_info)
            pred["decoded_state"] = post.argmax(axis=1)
            predictions.append(pred)
            scored_sector_count += 1
        except Exception as exc:
            skipped.append({"sector_code": sector_code, "reason": repr(exc), "rows": len(sector_df)})

    if not predictions:
        raise RuntimeError(f"{spec.name}: no pooled predictions produced")

    meta = {
        "candidate": asdict(spec),
        "preprocess": preprocess,
        "model_count": 1,
        "pooled_training_rows": int(train_obs.shape[0]),
        "pooled_sequence_count": len(lengths),
        "scored_sector_count": scored_sector_count,
        "skipped": skipped,
        "state_utilities": utility_info,
        "transmat": hmm.transmat_.tolist(),
        "self_transition": {str(i): float(hmm.transmat_[i, i]) for i in range(spec.n_states)},
        **cov_stats,
    }
    return pd.concat(predictions, ignore_index=True), meta


def evaluate_predictions(
    name: str,
    predictions: pd.DataFrame,
    panel: pd.DataFrame,
    start: date,
    end: date,
    top_quantile: float,
) -> dict[str, Any]:
    cols = ["trade_date", "sector_code"] + [f"future_excess_{h}d" for h in HORIZONS] + [f"future_rank_{h}d" for h in HORIZONS]
    merged = predictions.merge(panel[cols], on=["trade_date", "sector_code"], how="inner")
    merged = merged[(merged["trade_date"] >= start) & (merged["trade_date"] <= end)].copy()
    merged = merged.replace([np.inf, -np.inf], np.nan).dropna(subset=["score"])

    result: dict[str, Any] = {
        "name": name,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "rows": int(len(merged)),
        "date_count": int(merged["trade_date"].nunique()) if not merged.empty else 0,
        "sector_count": int(merged["sector_code"].nunique()) if not merged.empty else 0,
        "horizons": {},
    }

    for horizon in HORIZONS:
        future_col = f"future_excess_{horizon}d"
        rank_col = f"future_rank_{horizon}d"
        rankics: list[float] = []
        spread_returns: list[float] = []
        spread_ranks: list[float] = []
        hit_rates: list[float] = []
        changed_fractions: list[float] = []

        eval_df = merged.dropna(subset=[future_col, rank_col])
        previous_sign: pd.Series | None = None
        for _, group in eval_df.groupby("trade_date", sort=True):
            if len(group) < 10 or group["score"].nunique() <= 1:
                continue
            corr = group["score"].rank(method="average").corr(group[future_col].rank(method="average"))
            if pd.notna(corr):
                rankics.append(float(corr))
            n_tail = max(1, int(math.floor(len(group) * top_quantile)))
            ordered = group.sort_values("score")
            bottom = ordered.head(n_tail)
            top = ordered.tail(n_tail)
            spread_returns.append(float(top[future_col].mean() - bottom[future_col].mean()))
            spread_ranks.append(float(top[rank_col].mean() - bottom[rank_col].mean()))
            median_future = float(group[future_col].median())
            hit_rates.append(float((top[future_col] > median_future).mean()))
            signs = group.set_index("sector_code")["score"].apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
            if previous_sign is not None:
                aligned = signs.align(previous_sign, join="inner")
                if len(aligned[0]) > 0:
                    changed_fractions.append(float((aligned[0] != aligned[1]).mean()))
            previous_sign = signs

        result["horizons"][str(horizon)] = {
            "rankic_mean": float(np.mean(rankics)) if rankics else None,
            "rankic_tstat": safe_tstat(rankics),
            "rankic_date_count": len(rankics),
            "top_bottom_spread_mean": float(np.mean(spread_returns)) if spread_returns else None,
            "top_bottom_spread_pct": float(np.mean(spread_returns) * 100.0) if spread_returns else None,
            "top_bottom_spread_tstat": safe_tstat(spread_returns),
            "top_bottom_rank_spread_mean": float(np.mean(spread_ranks)) if spread_ranks else None,
            "top_hit_rate": float(np.mean(hit_rates)) if hit_rates else None,
            "score_sign_changed_fraction": float(np.mean(changed_fractions)) if changed_fractions else None,
        }
    return result


def flatten_metric_rows(metrics: dict[str, dict[str, Any]], split: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for name, payload in metrics.items():
        base = {
            "name": name,
            "split": split,
            "start": payload["start"],
            "end": payload["end"],
            "rows": payload["rows"],
            "date_count": payload["date_count"],
            "sector_count": payload["sector_count"],
        }
        for horizon, hrow in payload["horizons"].items():
            row = dict(base)
            row["horizon"] = int(horizon)
            row.update(hrow)
            rows.append(row)
    return rows


def load_baseline_coefficients(items: list[str] | None) -> dict[str, pd.DataFrame]:
    result: dict[str, pd.DataFrame] = {}
    for item in items or []:
        if "=" not in item:
            raise ValueError(f"Invalid --baseline-coefficients value: {item!r}; expected NAME=PATH")
        name, raw_path = item.split("=", 1)
        path = Path(windows_to_wsl_path(raw_path))
        payload = json.loads(path.read_text(encoding="utf-8"))
        daily = payload.get("daily_coefficients")
        if not isinstance(daily, dict):
            raise ValueError(f"{path} does not contain daily_coefficients")
        rows: list[dict[str, Any]] = []
        for trade_date, coeffs in daily.items():
            if not isinstance(coeffs, dict):
                continue
            for sector_code, score in coeffs.items():
                rows.append(
                    {
                        "trade_date": parse_date(str(trade_date)),
                        "sector_code": str(sector_code),
                        "sector_name": str(sector_code),
                        "candidate": name,
                        "score": float(score),
                        "decoded_state": None,
                    }
                )
        result[name] = pd.DataFrame(rows)
        print(f"Loaded baseline coefficients {name}: {len(rows)} rows from {path}")
    return result


def metric_value(metrics: dict[str, Any], horizon: int, key: str) -> float:
    value = metrics.get("horizons", {}).get(str(horizon), {}).get(key)
    return float(value) if value is not None and not pd.isna(value) else float("nan")


def candidate_decision(name: str, val_metrics: dict[str, Any], test_metrics: dict[str, Any], baselines: dict[str, dict[str, Any]]) -> dict[str, Any]:
    test_rankic_10 = metric_value(test_metrics, 10, "rankic_mean")
    test_spread_10 = metric_value(test_metrics, 10, "top_bottom_spread_mean")
    val_rankic_10 = metric_value(val_metrics, 10, "rankic_mean")
    val_spread_10 = metric_value(val_metrics, 10, "top_bottom_spread_mean")
    composite = 0.0
    for value, weight in (
        (test_rankic_10, 0.35),
        (val_rankic_10, 0.25),
        (test_spread_10, 12.0),
        (val_spread_10, 8.0),
    ):
        if not math.isnan(value):
            composite += weight * value

    baseline_best = {}
    for key in ("rankic_mean", "top_bottom_spread_mean"):
        vals = {
            bname: metric_value(btest, 10, key)
            for bname, btest in baselines.items()
        }
        finite = {k: v for k, v in vals.items() if not math.isnan(v)}
        baseline_best[key] = max(finite.values()) if finite else float("nan")

    beats_baseline_rankic = not math.isnan(test_rankic_10) and (
        math.isnan(baseline_best["rankic_mean"]) or test_rankic_10 > baseline_best["rankic_mean"]
    )
    beats_baseline_spread = not math.isnan(test_spread_10) and (
        math.isnan(baseline_best["top_bottom_spread_mean"]) or test_spread_10 > baseline_best["top_bottom_spread_mean"]
    )
    valuable = bool(
        beats_baseline_rankic
        and beats_baseline_spread
        and not math.isnan(val_rankic_10)
        and not math.isnan(val_spread_10)
        and val_spread_10 > 0
    )
    return {
        "name": name,
        "composite_score": composite,
        "test_rankic_10d": test_rankic_10,
        "test_spread_10d": test_spread_10,
        "val_rankic_10d": val_rankic_10,
        "val_spread_10d": val_spread_10,
        "beats_baseline_rankic_10d": beats_baseline_rankic,
        "beats_baseline_spread_10d": beats_baseline_spread,
        "valuable_for_qe_candidate": valuable,
    }


def write_report(
    output_dir: Path,
    args: argparse.Namespace,
    decisions: list[dict[str, Any]],
    val_metrics: dict[str, dict[str, Any]],
    test_metrics: dict[str, dict[str, Any]],
    metas: dict[str, dict[str, Any]],
) -> None:
    lines: list[str] = []
    lines.append("# HMM Sector-Rotation Redefinition Offline Screen")
    lines.append("")
    lines.append(f"- Generated at: {datetime.now(timezone.utc).astimezone().isoformat()}")
    lines.append(f"- Train: `{args.train_start}` to `{args.train_end}`")
    lines.append(f"- Validation: `{args.val_start}` to `{args.val_end}`")
    lines.append(f"- Test/QE-like holdout: `{args.test_start}` to `{args.test_end}`")
    lines.append("- Registry/QE impact: none; read-only DB plus local diagnostic artifacts only.")
    lines.append("")
    lines.append("## Ranking")
    lines.append("")
    lines.append("| Rank | Candidate | Composite | Test RankIC 10d | Test Spread 10d | Val RankIC 10d | Val Spread 10d | QE candidate |")
    lines.append("| ---: | --- | ---: | ---: | ---: | ---: | ---: | --- |")
    for idx, row in enumerate(sorted(decisions, key=lambda x: x["composite_score"], reverse=True), start=1):
        lines.append(
            "| {rank} | `{name}` | {comp:.6f} | {tri:.6f} | {tsp:.6f} | {vri:.6f} | {vsp:.6f} | {qe} |".format(
                rank=idx,
                name=row["name"],
                comp=row["composite_score"],
                tri=row["test_rankic_10d"],
                tsp=row["test_spread_10d"],
                vri=row["val_rankic_10d"],
                vsp=row["val_spread_10d"],
                qe="yes" if row["valuable_for_qe_candidate"] else "no",
            )
        )
    lines.append("")
    lines.append("## Candidate Definitions")
    lines.append("")
    for name, meta in metas.items():
        spec = meta.get("candidate", {})
        if not spec:
            continue
        lines.append(f"### {name}")
        lines.append("")
        lines.append(f"- Scope: `{spec.get('scope')}`; states: `{spec.get('n_states')}`; target: `{spec.get('target')}`")
        lines.append(f"- Description: {spec.get('description')}")
        lines.append(f"- Features: `{', '.join(spec.get('features', []))}`")
        if "model_count" in meta:
            lines.append(f"- Trained models: `{meta.get('model_count')}`; skipped: `{len(meta.get('skipped', []))}`")
        lines.append("")
    lines.append("## Files")
    lines.append("")
    lines.append("- `candidate_metrics.csv`: flattened validation/test metrics.")
    lines.append("- `candidate_metrics.json`: nested metric payload.")
    lines.append("- `candidate_decisions.json`: promotion gate summary.")
    lines.append("- `model_meta.json`: HMM model-state utility and transition summaries.")
    (output_dir / "analysis_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Offline screen for redefined sector-rotation HMM candidates")
    parser.add_argument("--train-start", default="2022-09-01")
    parser.add_argument("--train-end", default="2025-05-30")
    parser.add_argument("--val-start", default="2025-06-02")
    parser.add_argument("--val-end", default="2025-08-29")
    parser.add_argument("--test-start", default="2025-09-01")
    parser.add_argument("--test-end", default="2026-04-27")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--env-file", default=None, help="Optional .env file to load before DB defaults are resolved")
    parser.add_argument("--baseline-coefficients", action="append", default=[])
    parser.add_argument("--candidate", action="append", choices=[c.name for c in CANDIDATES])
    parser.add_argument("--max-sectors", type=int, default=0)
    parser.add_argument("--min-trading-days", type=int, default=240)
    parser.add_argument("--min-scoring-days", type=int, default=60)
    parser.add_argument("--n-iter", type=int, default=160)
    parser.add_argument("--winsor-q", type=float, default=0.01)
    parser.add_argument("--min-covar", type=float, default=1e-3)
    parser.add_argument("--max-covar", type=float, default=10.0)
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--top-quantile", type=float, default=0.20)
    parser.add_argument("--include-inverted", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--db-host", default=os.getenv("TDX_DB_HOST", "127.0.0.1"))
    parser.add_argument("--db-port", type=int, default=int(os.getenv("TDX_DB_PORT", "5432")))
    parser.add_argument("--db-name", default=os.getenv("TDX_DB_NAME", "aistock"))
    parser.add_argument("--db-user", default=os.getenv("TDX_DB_USER", "postgres"))
    parser.add_argument("--db-password", default=os.getenv("TDX_DB_PASSWORD", ""))
    return parser


def main() -> None:
    env_parser = argparse.ArgumentParser(add_help=False)
    env_parser.add_argument("--env-file", default=None)
    env_args, _ = env_parser.parse_known_args()
    read_env_file(ROOT / ".env")
    if env_args.env_file:
        read_env_file(Path(windows_to_wsl_path(env_args.env_file)))
    args = build_arg_parser().parse_args()
    output_dir = Path(windows_to_wsl_path(args.output_dir)).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    started = time.time()

    train_start = parse_date(args.train_start)
    test_end = parse_date(args.test_end)
    data_start = train_start - timedelta(days=90)
    data_end = test_end + timedelta(days=30)

    conn, db_host = connect_readonly(
        DBConfig(
            host=args.db_host,
            port=args.db_port,
            dbname=args.db_name,
            user=args.db_user,
            password=args.db_password or os.getenv("TDX_DB_PASSWORD", ""),
        )
    )

    print(f"Loading sector panel {data_start} to {data_end}")
    sector_rows = fetch_sector_rows(conn, data_start, data_end)
    index_df = fetch_index_returns(conn, data_start, data_end)
    # Preserve 1d benchmark return for panel construction.
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT trade_date, pct_chg
        FROM market.index_daily
        WHERE ts_code = '000300.SH' AND trade_date BETWEEN %s AND %s
        ORDER BY trade_date
        """,
        (data_start, data_end),
    )
    bench_rows = pd.DataFrame(cur.fetchall())
    cur.close()
    if bench_rows.empty:
        raise RuntimeError("No 000300.SH rows found for _bench_ret")
    bench_rows["trade_date"] = pd.to_datetime(bench_rows["trade_date"]).dt.date
    bench_rows["_bench_ret"] = pd.to_numeric(bench_rows["pct_chg"], errors="coerce").fillna(0.0) / 100.0
    index_df["trade_date"] = pd.to_datetime(index_df["trade_date"]).dt.date
    index_df = index_df.merge(bench_rows[["trade_date", "_bench_ret"]], on="trade_date", how="left")

    market_vol_df = fetch_market_volume(conn, data_start, data_end)
    conn.close()
    panel = build_panel(sector_rows, index_df, market_vol_df)
    panel_path = output_dir / "sector_rotation_panel.parquet"
    try:
        panel.to_parquet(panel_path, index=False)
    except Exception:
        panel_path = output_dir / "sector_rotation_panel.csv"
        panel.to_csv(panel_path, index=False)
    print(f"Panel rows={len(panel)}, sectors={panel['sector_code'].nunique()}, dates={panel['trade_date'].nunique()}")

    candidate_names = set(args.candidate or [c.name for c in CANDIDATES])
    candidate_specs = [c for c in CANDIDATES if c.name in candidate_names]
    baselines = load_baseline_coefficients(args.baseline_coefficients)

    val_start = parse_date(args.val_start)
    val_end = parse_date(args.val_end)
    test_start = parse_date(args.test_start)
    val_metrics: dict[str, dict[str, Any]] = {}
    test_metrics: dict[str, dict[str, Any]] = {}
    metas: dict[str, dict[str, Any]] = {}

    for name, pred in baselines.items():
        val_metrics[name] = evaluate_predictions(name, pred, panel, val_start, val_end, args.top_quantile)
        test_metrics[name] = evaluate_predictions(name, pred, panel, test_start, test_end, args.top_quantile)

    for spec in candidate_specs:
        print(f"Training candidate {spec.name} ({spec.scope})")
        t0 = time.time()
        if spec.scope == "per_sector":
            pred, meta = train_per_sector_candidate(panel, spec, args)
        elif spec.scope == "pooled":
            pred, meta = train_pooled_candidate(panel, spec, args)
        elif spec.scope == "daily_regime_linear":
            pred, meta = train_daily_regime_linear_candidate(panel, spec, args)
        else:
            raise ValueError(f"Unknown candidate scope: {spec.scope}")
        pred_path = output_dir / f"predictions_{spec.name}.csv"
        pred.to_csv(pred_path, index=False)
        val_metrics[spec.name] = evaluate_predictions(spec.name, pred, panel, val_start, val_end, args.top_quantile)
        test_metrics[spec.name] = evaluate_predictions(spec.name, pred, panel, test_start, test_end, args.top_quantile)
        meta["elapsed_seconds"] = round(time.time() - t0, 3)
        metas[spec.name] = meta
        if args.include_inverted:
            inv_name = f"{spec.name}__INV"
            inv_pred = pred.copy()
            inv_pred["candidate"] = inv_name
            inv_pred["score"] = -pd.to_numeric(inv_pred["score"], errors="coerce")
            inv_pred.to_csv(output_dir / f"predictions_{inv_name}.csv", index=False)
            val_metrics[inv_name] = evaluate_predictions(inv_name, inv_pred, panel, val_start, val_end, args.top_quantile)
            test_metrics[inv_name] = evaluate_predictions(inv_name, inv_pred, panel, test_start, test_end, args.top_quantile)
            inv_meta = dict(meta)
            inv_meta["base_candidate"] = spec.name
            inv_meta["orientation"] = "inverted_score"
            metas[inv_name] = inv_meta
        print(f"Finished {spec.name} in {meta['elapsed_seconds']}s")

    baseline_test_metrics = {name: test_metrics[name] for name in baselines}
    decisions = [
        candidate_decision(name, val_metrics[name], test_metrics[name], baseline_test_metrics)
        for name in val_metrics
    ]
    decisions = sorted(decisions, key=lambda row: row["composite_score"], reverse=True)

    metrics_payload = {"validation": val_metrics, "test": test_metrics}
    write_json(output_dir / "candidate_metrics.json", metrics_payload)
    write_json(output_dir / "candidate_decisions.json", decisions)
    write_json(output_dir / "model_meta.json", metas)
    write_json(
        output_dir / "run_context.json",
        {
            "args": vars(args),
            "db_host_used": db_host,
            "data_start": data_start,
            "data_end": data_end,
            "panel_path": str(panel_path),
            "elapsed_seconds": round(time.time() - started, 3),
            "candidate_count": len(candidate_specs),
            "baseline_count": len(baselines),
        },
    )

    flat_rows = flatten_metric_rows(val_metrics, "validation") + flatten_metric_rows(test_metrics, "test")
    pd.DataFrame(flat_rows).to_csv(output_dir / "candidate_metrics.csv", index=False)
    write_report(output_dir, args, decisions, val_metrics, test_metrics, metas)

    print(json.dumps({"output_dir": str(output_dir), "top_decisions": decisions[:8]}, ensure_ascii=False, indent=2, default=json_default))


if __name__ == "__main__":
    main()
