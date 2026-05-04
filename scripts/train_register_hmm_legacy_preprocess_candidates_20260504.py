#!/usr/bin/env python3
"""Train/register legacy sector-HMM preprocessing candidates.

This one-off helper supports the 2026-05-04 HMM QE campaign. It registers the
generated snapshots under a hidden model_type so the normal QE selector can
remain limited to the retained Loop2/Loop10 baselines while custom QE loops can
reference the experimental snapshots by snapshot_id.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shutil
import subprocess
import sys
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import psycopg2
import psycopg2.extras
from hmmlearn.hmm import GaussianHMM

ROOT = Path(__file__).resolve().parents[1]
RDAGENT_ROOT = ROOT.parent / "RD-Agent-main"
for item in (str(RDAGENT_ROOT), str(ROOT)):
    if item not in sys.path:
        sys.path.insert(0, item)

from model_training.common.data_loader import (  # noqa: E402
    get_db_conn,
    get_limit_up_ratio_by_sector,
    load_csi300_daily,
    load_l2_sector_data,
    load_market_total_volume,
    load_sector_stock_mapping,
    read_qlib_calendar,
)
from model_training.hmm.config import HMMTrainConfig  # noqa: E402
from model_training.hmm.train_sector_hmm import (  # noqa: E402
    build_observation_matrix,
    covariance_bound_stats,
    smooth_transition_matrix,
    validate_and_fix_covariance,
)


MODELS_ROOT = ROOT / "backend" / "data" / "hmm_models"
TMP_ROOT = ROOT / ".codex_tmp" / "hmm_registry_updates"
MODEL_TYPE = "sector_hmm_experimental_preprocess_20260504"
SNAPSHOT_DATE = "2026-05-04"
COEFF_START = date(2024, 7, 1)
COEFF_END = date(2026, 4, 27)
COEFF_FILENAME = (
    f"coefficients_preset_A_{COEFF_START.isoformat()}_{COEFF_END.isoformat()}.json"
)

BASE_CONFIG: dict[str, Any] = {
    "n_states": 3,
    "covariance_type": "diag",
    "n_iter": 300,
    "rolling_window": 3,
    "zscore": False,
    "use_limit_down": False,
    "train_start": date(2022, 1, 1),
    "train_end": date(2024, 6, 30),
    "val_start": date(2024, 7, 1),
    "val_end": date(2025, 3, 31),
    "coefficient_start": COEFF_START,
    "coefficient_end": COEFF_END,
    "sector_level": "L2",
    "cooldown_days": 3,
    "min_trading_days": 120,
}

BASES: list[dict[str, Any]] = [
    {
        "base_key": "L2",
        "display_prefix": "HMM_EXP_L2_preproc",
        "coeffs": {"fading": 0.96, "neutral": 1.0, "trending": 1.05},
        "description": (
            "Loop2 old-covfix coefficient map: fading penalty 0.96 and "
            "trending boost 1.05."
        ),
    },
    {
        "base_key": "L10",
        "display_prefix": "HMM_EXP_L10_preproc",
        "coeffs": {"fading": 0.96, "neutral": 1.0, "trending": 1.0},
        "description": (
            "Loop10 retained-best coefficient map: fading penalty 0.96 and "
            "no trending boost."
        ),
    },
]

PREPROCESS_MODES: list[dict[str, Any]] = [
    {
        "mode": "train_zscore",
        "suffix": "train_zscore",
        "description": "Train-window-only global z-score over legacy 7-dim observations.",
    },
    {
        "mode": "winsor_zscore",
        "suffix": "winsor01_zscore",
        "description": "Train-window global 1%/99% winsorization followed by z-score.",
        "winsor_q": 0.01,
    },
    {
        "mode": "robust_zscore",
        "suffix": "robust_zscore",
        "description": "Train-window median/MAD robust z-score clipped to +/-6.",
        "clip": 6.0,
    },
    {
        "mode": "sector_cs_rank",
        "suffix": "sector_cs_rank",
        "description": "Same-date sector cross-sectional percentile rank scaled to [-1, 1].",
    },
    {
        "mode": "sector_cs_zscore",
        "suffix": "sector_cs_zscore",
        "description": "Same-date sector cross-sectional z-score by feature.",
    },
]


def parse_date(value: Any) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def json_default(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            default=json_default,
        ),
        encoding="utf-8",
    )


def read_env_file(path: Path) -> None:
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        text = line.strip()
        if not text or text.startswith("#") or "=" not in text:
            continue
        key, value = text.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


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


def find_db_host(initial: str, port: int, user: str, password: str, dbname: str) -> str:
    errors: list[str] = []
    for host in candidate_hosts(initial):
        try:
            conn = psycopg2.connect(
                host=host,
                port=port,
                dbname=dbname,
                user=user,
                password=password,
                connect_timeout=5,
                application_name="AIstock-HMM-preprocess-candidates-20260504-probe",
            )
            conn.close()
            print(f"DB connected via host={host}")
            return host
        except Exception as exc:
            errors.append(f"{host}: {str(exc).splitlines()[0]}")
    raise RuntimeError("Cannot connect to DB. Tried: " + "; ".join(errors))


def db_connect() -> psycopg2.extensions.connection:
    read_env_file(ROOT / ".env")
    password = os.getenv("TDX_DB_PASSWORD", "")
    if not password:
        raise RuntimeError("TDX_DB_PASSWORD is required; refusing to embed DB secrets")
    return psycopg2.connect(
        host=os.getenv("TDX_DB_HOST", "127.0.0.1"),
        port=int(os.getenv("TDX_DB_PORT", "5432")),
        dbname=os.getenv("TDX_DB_NAME", "aistock"),
        user=os.getenv("TDX_DB_USER", "postgres"),
        password=password,
        application_name="AIstock-HMM-preprocess-candidates-20260504-register",
        options="-c client_encoding=utf8",
    )


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def wsl_to_windows_path(path: Path | str) -> str:
    text = str(path)
    if text.startswith("/mnt/") and len(text) > 6 and text[6] == "/":
        drive = text[5].upper()
        rest = text[6:].replace("/", "\\")
        return f"{drive}:{rest}"
    return text


def safe_model_dir(config_id: str) -> Path:
    target = (MODELS_ROOT / config_id / SNAPSHOT_DATE).resolve()
    root = MODELS_ROOT.resolve()
    if root not in target.parents:
        raise RuntimeError(f"unsafe HMM model directory: {target}")
    return target


def make_cfg(db_host: str) -> HMMTrainConfig:
    return HMMTrainConfig(
        n_states=int(BASE_CONFIG["n_states"]),
        covariance_type=str(BASE_CONFIG["covariance_type"]),
        n_iter=int(BASE_CONFIG["n_iter"]),
        rolling_window=int(BASE_CONFIG["rolling_window"]),
        zscore=False,
        use_limit_down=bool(BASE_CONFIG["use_limit_down"]),
        train_start=parse_date(BASE_CONFIG["train_start"]),
        train_end=parse_date(BASE_CONFIG["train_end"]),
        val_start=parse_date(BASE_CONFIG["val_start"]),
        val_end=parse_date(BASE_CONFIG["val_end"]),
        sector_level=str(BASE_CONFIG["sector_level"]),
        cooldown_days=int(BASE_CONFIG["cooldown_days"]),
        min_trading_days=int(BASE_CONFIG["min_trading_days"]),
        db_host=db_host,
        db_port=int(os.getenv("TDX_DB_PORT", "5432")),
        db_user=os.getenv("TDX_DB_USER", "postgres"),
        db_password=os.getenv("TDX_DB_PASSWORD", ""),
        db_name=os.getenv("TDX_DB_NAME", "aistock"),
    )


def load_training_raw(
    cfg: HMMTrainConfig,
) -> tuple[dict[str, tuple[np.ndarray, list[date], list[dict[str, Any]]]], dict[str, Any]]:
    conn = get_db_conn(cfg.db_host, cfg.db_port, cfg.db_user, cfg.db_password, cfg.db_name)
    data_start = cfg.train_start - timedelta(days=60)
    sector_data = load_l2_sector_data(conn, data_start, cfg.train_end)
    csi300 = load_csi300_daily(conn, data_start, cfg.train_end)
    market_vol = load_market_total_volume(conn, data_start, cfg.train_end)
    sector_stocks = load_sector_stock_mapping(conn, cfg.sector_level)
    calendar = read_qlib_calendar(cfg.qlib_bin_dir)
    limit_up_data, limit_down_data = get_limit_up_ratio_by_sector(
        cfg.qlib_bin_dir,
        sector_stocks,
        calendar,
        cfg.train_start,
        cfg.train_end,
    )
    conn.close()

    raw: dict[str, tuple[np.ndarray, list[date], list[dict[str, Any]]]] = {}
    skipped = 0
    for sector_code, data_list in sorted(sector_data.items()):
        train_data = [r for r in data_list if cfg.train_start <= r["trade_date"] <= cfg.train_end]
        if len(train_data) < cfg.min_trading_days:
            skipped += 1
            continue
        obs, obs_dates = build_observation_matrix(
            train_data,
            csi300,
            market_vol,
            limit_up_data.get(sector_code, {}),
            limit_down_data.get(sector_code, {}),
            rolling_window=cfg.rolling_window,
            use_limit_down=cfg.use_limit_down,
        )
        if obs.shape[0] < cfg.min_trading_days:
            skipped += 1
            continue
        raw[str(sector_code)] = (obs, obs_dates, train_data)
    if not raw:
        raise RuntimeError("No trainable sectors for HMM preprocess experiment")
    meta = {
        "sector_count_raw": len(sector_data),
        "sector_count_trainable": len(raw),
        "skipped": skipped,
        "csi300_days": len(csi300),
        "market_vol_days": len(market_vol),
    }
    print(f"Loaded training observations: {meta}")
    return raw, meta


def fit_preprocess(
    mode_cfg: dict[str, Any],
    raw: dict[str, tuple[np.ndarray, list[date], list[dict[str, Any]]]],
) -> dict[str, Any]:
    mode = str(mode_cfg["mode"])
    all_obs = np.vstack([obs for obs, _, _ in raw.values()])
    params: dict[str, Any] = {
        "schema": "legacy_sector_hmm_preprocess_v1",
        "mode": mode,
        "fit_scope": (
            "train_window_only"
            if not mode.startswith("sector_cs_")
            else "same_date_sector_cross_section"
        ),
        "feature_count": int(all_obs.shape[1]),
        "train_observation_count": int(all_obs.shape[0]),
    }
    if mode == "train_zscore":
        mean = all_obs.mean(axis=0)
        std = np.where(all_obs.std(axis=0) < 1e-10, 1.0, all_obs.std(axis=0))
        params.update({"mean": mean.tolist(), "std": std.tolist()})
    elif mode == "winsor_zscore":
        q = float(mode_cfg.get("winsor_q", 0.01))
        lower = np.quantile(all_obs, q, axis=0)
        upper = np.quantile(all_obs, 1.0 - q, axis=0)
        clipped = np.clip(all_obs, lower, upper)
        mean = clipped.mean(axis=0)
        std = np.where(clipped.std(axis=0) < 1e-10, 1.0, clipped.std(axis=0))
        params.update(
            {
                "winsor_q": q,
                "lower": lower.tolist(),
                "upper": upper.tolist(),
                "mean": mean.tolist(),
                "std": std.tolist(),
            }
        )
    elif mode == "robust_zscore":
        median = np.median(all_obs, axis=0)
        mad = np.median(np.abs(all_obs - median), axis=0)
        scale = 1.4826 * mad
        iqr_scale = (
            np.quantile(all_obs, 0.75, axis=0) - np.quantile(all_obs, 0.25, axis=0)
        ) / 1.349
        std = all_obs.std(axis=0)
        scale = np.where(scale < 1e-10, iqr_scale, scale)
        scale = np.where(scale < 1e-10, std, scale)
        scale = np.where(scale < 1e-10, 1.0, scale)
        params.update(
            {
                "median": median.tolist(),
                "scale": scale.tolist(),
                "clip": float(mode_cfg.get("clip", 6.0)),
            }
        )
    elif mode in {"sector_cs_rank", "sector_cs_zscore"}:
        params.update({"same_date_only": True})
    else:
        raise ValueError(f"Unsupported preprocess mode: {mode}")
    return params


def _rank_scale(values: np.ndarray) -> np.ndarray:
    n = values.shape[0]
    if n <= 1:
        return np.zeros(n, dtype=np.float64)
    order = np.argsort(values, kind="mergesort")
    ranks = np.empty(n, dtype=np.float64)
    ranks[order] = np.arange(n, dtype=np.float64)
    return ranks / max(n - 1, 1) * 2.0 - 1.0


def apply_preprocess(
    raw: dict[str, tuple[np.ndarray, list[date], Any]],
    params: dict[str, Any],
) -> dict[str, np.ndarray]:
    mode = str(params["mode"])
    if mode == "train_zscore":
        mean = np.asarray(params["mean"], dtype=np.float64)
        std = np.asarray(params["std"], dtype=np.float64)
        return {code: (obs - mean) / std for code, (obs, _, _) in raw.items()}
    if mode == "winsor_zscore":
        lower = np.asarray(params["lower"], dtype=np.float64)
        upper = np.asarray(params["upper"], dtype=np.float64)
        mean = np.asarray(params["mean"], dtype=np.float64)
        std = np.asarray(params["std"], dtype=np.float64)
        return {
            code: (np.clip(obs, lower, upper) - mean) / std
            for code, (obs, _, _) in raw.items()
        }
    if mode == "robust_zscore":
        median = np.asarray(params["median"], dtype=np.float64)
        scale = np.asarray(params["scale"], dtype=np.float64)
        clip = float(params.get("clip", 6.0))
        return {
            code: np.clip((obs - median) / scale, -clip, clip)
            for code, (obs, _, _) in raw.items()
        }
    if mode not in {"sector_cs_rank", "sector_cs_zscore"}:
        raise ValueError(f"Unsupported preprocess mode: {mode}")

    transformed = {code: np.zeros_like(obs, dtype=np.float64) for code, (obs, _, _) in raw.items()}
    date_rows: dict[date, list[tuple[str, int, np.ndarray]]] = {}
    for code, (obs, dates_out, _) in raw.items():
        for idx, td in enumerate(dates_out):
            date_rows.setdefault(td, []).append((code, idx, obs[idx]))
    for _td, items in date_rows.items():
        mat = np.vstack([row for _, _, row in items])
        if mode == "sector_cs_rank":
            out = np.zeros_like(mat, dtype=np.float64)
            for col in range(mat.shape[1]):
                out[:, col] = _rank_scale(mat[:, col])
        else:
            mean = mat.mean(axis=0)
            std = np.where(mat.std(axis=0) < 1e-10, 1.0, mat.std(axis=0))
            out = (mat - mean) / std
        for pos, (code, idx, _) in enumerate(items):
            transformed[code][idx] = out[pos]
    return transformed


def label_states(hmm: GaussianHMM, n_states: int) -> dict[str, str]:
    means_ret = np.asarray(hmm.means_)[:, 0]
    if n_states == 2:
        trending_idx = int(np.argmax(means_ret))
        fading_idx = 1 - trending_idx
        return {str(trending_idx): "trending", str(fading_idx): "fading"}
    sorted_idx = np.argsort(means_ret)
    return {
        str(int(sorted_idx[-1])): "trending",
        str(int(sorted_idx[0])): "fading",
        str(int(sorted_idx[1])): "neutral",
    }


def train_models(
    cfg: HMMTrainConfig,
    raw: dict[str, tuple[np.ndarray, list[date], list[dict[str, Any]]]],
    params: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    transformed = apply_preprocess(raw, params)
    models: dict[str, Any] = {}
    fixed_count = 0
    total_anomalies = 0
    failed: list[str] = []
    for sector_code, (raw_obs, _dates, train_data) in sorted(raw.items()):
        obs_train = transformed[sector_code]
        sector_name = train_data[0].get("l2_name", sector_code)
        try:
            hmm = GaussianHMM(
                n_components=cfg.n_states,
                covariance_type=cfg.covariance_type,
                n_iter=cfg.n_iter,
                min_covar=1e-3,
                random_state=42,
            )
            hmm.fit(obs_train)
            fixed, anomaly_count = validate_and_fix_covariance(
                hmm,
                max_covar=10.0,
                min_covar=1e-3,
            )
            cov_stats = covariance_bound_stats(hmm, max_covar=10.0, min_covar=1e-3)
            if fixed:
                fixed_count += 1
                total_anomalies += int(anomaly_count)
            hmm.transmat_ = smooth_transition_matrix(
                hmm.transmat_,
                alpha=0.1,
                min_self_trans=0.3,
            )
            labels = label_states(hmm, cfg.n_states)
            trending_state = int([k for k, v in labels.items() if v == "trending"][0])
            p_stay = float(hmm.transmat_[trending_state, trending_state])
            avg_duration = 1.0 / (1.0 - p_stay) if p_stay < 1.0 else float("inf")
            info: dict[str, Any] = {
                "sector_code": sector_code,
                "sector_name": sector_name,
                "n_states": cfg.n_states,
                "covariance_type": cfg.covariance_type,
                "transmat": hmm.transmat_.tolist(),
                "means": hmm.means_.tolist(),
                "covars": hmm.covars_.tolist(),
                "state_labels": labels,
                "trending_avg_duration_days": (
                    round(avg_duration, 1) if math.isfinite(avg_duration) else "inf"
                ),
                "training_days": int(raw_obs.shape[0]),
                "obs_features": list(cfg.obs_features),
                "rolling_window": cfg.rolling_window,
                "use_limit_down": cfg.use_limit_down,
                "covariance_fixed": bool(fixed),
                "covariance_anomaly_count": int(anomaly_count),
                "preprocess": params,
                **cov_stats,
            }
            if params["mode"] == "train_zscore":
                info["zscore_mean"] = params["mean"]
                info["zscore_std"] = params["std"]
            models[sector_code] = info
        except Exception as exc:
            failed.append(f"{sector_code}: {exc}")
    if not models:
        raise RuntimeError("All HMM sectors failed to train")
    metrics = {
        "preprocess_mode": params["mode"],
        "sector_count": len(models),
        "failed_count": len(failed),
        "failed_examples": failed[:10],
        "covariance_fixed_sector_count": fixed_count,
        "covariance_anomaly_count": total_anomalies,
    }
    print(f"Trained mode={params['mode']}: {metrics}")
    return models, metrics


def restore_hmm(info: dict[str, Any]) -> GaussianHMM:
    hmm = GaussianHMM(
        n_components=int(info["n_states"]),
        covariance_type=str(info["covariance_type"]),
    )
    n_states = int(info["n_states"])
    hmm.startprob_ = np.full(n_states, 1.0 / n_states, dtype=np.float64)
    hmm.transmat_ = np.asarray(info["transmat"], dtype=np.float64)
    hmm.means_ = np.asarray(info["means"], dtype=np.float64)
    covars = np.asarray(info["covars"], dtype=np.float64)
    if info["covariance_type"] == "diag":
        if covars.ndim == 3:
            covars = np.asarray([np.diag(covars[i]) for i in range(covars.shape[0])])
        covars = np.maximum(covars, 1e-6)
    elif info["covariance_type"] == "full":
        for i in range(covars.shape[0]):
            covars[i] = (covars[i] + covars[i].T) / 2
            covars[i] += np.eye(covars[i].shape[0]) * 1e-6
    hmm.covars_ = covars
    return hmm


def forward_filter_posteriors(hmm: GaussianHMM, obs: np.ndarray) -> np.ndarray:
    from hmmlearn import _hmmc

    log_frameprob = hmm._compute_log_likelihood(obs)
    _, fwd_lattice = _hmmc.forward_log(hmm.startprob_, hmm.transmat_, log_frameprob)
    row_max = np.max(fwd_lattice, axis=1, keepdims=True)
    row_max = np.where(np.isfinite(row_max), row_max, 0.0)
    posteriors = np.exp(fwd_lattice - row_max)
    denom = posteriors.sum(axis=1, keepdims=True)
    n_states = posteriors.shape[1]
    return np.divide(
        posteriors,
        denom,
        out=np.full_like(posteriors, 1.0 / max(n_states, 1)),
        where=denom > 0,
    )


def load_full_raw(
    cfg: HMMTrainConfig,
    end_d: date,
) -> tuple[dict[str, tuple[np.ndarray, list[date], Any]], dict[str, str], dict[str, Any]]:
    history_start = COEFF_START - timedelta(days=int(3.0 * 365 + 30))
    conn = get_db_conn(cfg.db_host, cfg.db_port, cfg.db_user, cfg.db_password, cfg.db_name)
    sector_data = load_l2_sector_data(conn, history_start, end_d)
    csi300 = load_csi300_daily(conn, history_start, end_d)
    market_vol = load_market_total_volume(conn, history_start, end_d)
    sector_stocks = load_sector_stock_mapping(conn, cfg.sector_level)
    calendar = read_qlib_calendar(cfg.qlib_bin_dir)
    limit_up_data, limit_down_data = get_limit_up_ratio_by_sector(
        cfg.qlib_bin_dir,
        sector_stocks,
        calendar,
        history_start,
        end_d,
    )
    cur = conn.cursor()
    cur.execute(
        """
        SELECT ts_code, l2_code FROM market.sw_index_member
        WHERE in_date <= %s AND (out_date IS NULL OR out_date >= %s)
        """,
        (end_d, COEFF_START),
    )
    stock_sector_map = {str(ts): str(code) for ts, code in cur.fetchall() if ts and code}
    cur.close()
    conn.close()

    raw: dict[str, tuple[np.ndarray, list[date], Any]] = {}
    for sector_code, data_list in sorted(sector_data.items()):
        obs, obs_dates = build_observation_matrix(
            data_list,
            csi300,
            market_vol,
            limit_up_data.get(sector_code, {}),
            limit_down_data.get(sector_code, {}),
            rolling_window=cfg.rolling_window,
            use_limit_down=cfg.use_limit_down,
        )
        if obs.shape[0] >= 20:
            raw[str(sector_code)] = (obs, obs_dates, data_list)
    meta = {
        "history_start": history_start.isoformat(),
        "raw_sector_count": len(raw),
        "csi300_days": len(csi300),
        "market_vol_days": len(market_vol),
        "stock_sector_map_count": len(stock_sector_map),
    }
    return raw, stock_sector_map, meta


def precompute_coefficients(
    cfg: HMMTrainConfig,
    models: dict[str, Any],
    params: dict[str, Any],
    preset_coeffs: dict[str, float],
) -> tuple[dict[str, Any], dict[str, Any]]:
    raw, stock_sector_map, load_meta = load_full_raw(cfg, COEFF_END)
    transformed = apply_preprocess(raw, params)
    daily: dict[str, dict[str, float]] = {}
    decoded_sector_count = 0
    for sector_code, info in sorted(models.items()):
        if sector_code not in raw:
            continue
        obs = transformed[sector_code]
        dates_out = raw[sector_code][1]
        if obs.shape[0] < 20:
            continue
        try:
            posteriors = forward_filter_posteriors(restore_hmm(info), obs)
        except Exception as exc:
            print(f"WARNING: forward filter failed {sector_code}: {exc}")
            continue
        labels = info["state_labels"]
        states = posteriors.argmax(axis=1)
        any_label = False
        for idx, td in enumerate(dates_out):
            if COEFF_START <= td <= COEFF_END:
                label = labels.get(str(int(states[idx])), "neutral")
                daily.setdefault(td.isoformat(), {})[sector_code] = float(
                    preset_coeffs.get(label, 1.0)
                )
                any_label = True
        if any_label:
            decoded_sector_count += 1
    if not daily:
        raise RuntimeError("No HMM coefficients generated")
    payload = {
        "model_path": None,
        "preset_key": "preset_A",
        "preset_coeffs": preset_coeffs,
        "test_start": COEFF_START.isoformat(),
        "backtest_end": COEFF_END.isoformat(),
        "sector_count": decoded_sector_count,
        "dynamic_coefficients": False,
        "daily_coefficients": {d: daily[d] for d in sorted(daily)},
        "stock_sector_map": stock_sector_map,
        "preprocess": params,
        "generation_mode": "legacy_preprocess_direct_precompute_v1",
    }
    meta = dict(load_meta)
    meta.update({"date_count": len(daily), "decoded_sector_count": decoded_sector_count})
    return payload, meta


def candidate_config_json(
    base: dict[str, Any],
    mode_cfg: dict[str, Any],
    params: dict[str, Any],
) -> dict[str, Any]:
    cfg = dict(BASE_CONFIG)
    cfg.update(
        {
            "zscore": params["mode"] == "train_zscore",
            "preprocess_mode": params["mode"],
            "preprocess": params,
            "coefficient_start": COEFF_START.isoformat(),
            "coefficient_end": COEFF_END.isoformat(),
            "signal_presets": {
                "preset_A": {
                    "label": f"{base['base_key']} {mode_cfg['suffix']} preset_A",
                    "description": base["description"],
                    "coefficients": base["coeffs"],
                }
            },
            "strict_no_leakage": True,
            "precomputed_only": True,
            "coefficient_windows": [
                {
                    "role": "qe_default_window_20260504_hmm_preprocess",
                    "preset": "preset_A",
                    "test_start": COEFF_START.isoformat(),
                    "backtest_end": COEFF_END.isoformat(),
                    "strict_no_leakage": True,
                }
            ],
            "candidate_family": "legacy_hmm_input_preprocess_20260504",
            "candidate_base": base["base_key"],
            "candidate_description": mode_cfg["description"],
        }
    )
    return {k: (v.isoformat() if isinstance(v, date) else v) for k, v in cfg.items()}


def register_candidate(
    conn: psycopg2.extensions.connection,
    base: dict[str, Any],
    mode_cfg: dict[str, Any],
    params: dict[str, Any],
    models: dict[str, Any],
    metrics: dict[str, Any],
    coeff_payload: dict[str, Any],
    coeff_meta: dict[str, Any],
    *,
    dry_run: bool,
) -> dict[str, Any]:
    display_name = f"{base['display_prefix']}_{mode_cfg['suffix']}__qe20260504"
    config_id = str(uuid.uuid4())
    model_dir = safe_model_dir(config_id)
    model_path = model_dir / "models.json"
    coeff_path = model_dir / COEFF_FILENAME
    db_model_path = wsl_to_windows_path(model_path.resolve())
    db_coeff_path = wsl_to_windows_path(coeff_path.resolve())
    config_json = candidate_config_json(base, mode_cfg, params)
    metric_payload = {
        "snapshot_display_name": display_name,
        "display_name": display_name,
        "training_metrics": metrics,
        "coefficient_meta": coeff_meta,
        "model_sha256": None,
        "coefficients_sha256": None,
        "created_by": "scripts/train_register_hmm_legacy_preprocess_candidates_20260504.py",
    }
    if not dry_run:
        model_dir.mkdir(parents=True, exist_ok=True)
        write_json(model_path, models)
        coeff_payload = dict(coeff_payload)
        coeff_payload["model_path"] = db_model_path
        write_json(coeff_path, coeff_payload)
        metric_payload["model_sha256"] = sha256_file(model_path)
        metric_payload["coefficients_sha256"] = sha256_file(coeff_path)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO model_train_configs
                    (config_id, model_type, display_name, config_json, cron_enabled)
                VALUES (%s, %s, %s, %s, false)
                """,
                (
                    config_id,
                    MODEL_TYPE,
                    display_name,
                    json.dumps(config_json, ensure_ascii=False, default=json_default),
                ),
            )
            cur.execute(
                """
                INSERT INTO model_train_snapshots
                    (config_id, model_path, sector_count, status, metrics_json)
                VALUES (%s, %s, %s, 'completed', %s)
                RETURNING snapshot_id
                """,
                (
                    config_id,
                    db_model_path,
                    int(metrics.get("sector_count") or 0),
                    json.dumps(metric_payload, ensure_ascii=False, default=json_default),
                ),
            )
            snapshot_id = cur.fetchone()[0]
            cur.execute(
                """
                INSERT INTO model_train_jobs
                    (config_id, snapshot_id, status, started_at, completed_at)
                VALUES (%s, %s, 'completed', NOW(), NOW())
                RETURNING job_id
                """,
                (config_id, snapshot_id),
            )
            job_id = cur.fetchone()[0]
        conn.commit()
    else:
        snapshot_id = "DRY_RUN"
        job_id = "DRY_RUN"
    return {
        "base_key": base["base_key"],
        "mode": mode_cfg["mode"],
        "display_name": display_name,
        "config_id": config_id,
        "snapshot_id": snapshot_id,
        "job_id": job_id,
        "model_type": MODEL_TYPE,
        "model_path": db_model_path,
        "coefficients_path": db_coeff_path,
        "metrics": metrics,
        "coefficient_meta": coeff_meta,
    }


def backup_existing(conn: psycopg2.extensions.connection, path: Path) -> None:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT c.*, s.snapshot_id, s.model_path, s.sector_count,
                   s.status AS snapshot_status, s.trained_at, s.metrics_json
            FROM model_train_configs c
            LEFT JOIN model_train_snapshots s ON s.config_id = c.config_id
            WHERE c.model_type = %s
               OR c.display_name LIKE 'HMM_EXP_L%%_preproc%%__qe20260504'
            ORDER BY c.created_at, s.trained_at
            """,
            (MODEL_TYPE,),
        )
        rows = [dict(row) for row in cur.fetchall()]
    write_json(path, {"created_at": datetime.now(timezone.utc), "rows": rows})


def cleanup_existing(conn: psycopg2.extensions.connection, *, delete_files: bool) -> int:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT c.config_id, s.model_path
            FROM model_train_configs c
            LEFT JOIN model_train_snapshots s ON s.config_id = c.config_id
            WHERE c.model_type = %s
               OR c.display_name LIKE 'HMM_EXP_L%%_preproc%%__qe20260504'
            """,
            (MODEL_TYPE,),
        )
        rows = [dict(row) for row in cur.fetchall()]
        cur.execute(
            """
            DELETE FROM model_train_jobs
            WHERE config_id IN (
                SELECT config_id FROM model_train_configs
                WHERE model_type = %s
                   OR display_name LIKE 'HMM_EXP_L%%_preproc%%__qe20260504'
            )
            """,
            (MODEL_TYPE,),
        )
        cur.execute(
            """
            DELETE FROM model_train_snapshots
            WHERE config_id IN (
                SELECT config_id FROM model_train_configs
                WHERE model_type = %s
                   OR display_name LIKE 'HMM_EXP_L%%_preproc%%__qe20260504'
            )
            """,
            (MODEL_TYPE,),
        )
        cur.execute(
            """
            DELETE FROM model_train_configs
            WHERE model_type = %s
               OR display_name LIKE 'HMM_EXP_L%%_preproc%%__qe20260504'
            """,
            (MODEL_TYPE,),
        )
    conn.commit()
    if delete_files:
        root = MODELS_ROOT.resolve()
        seen: set[Path] = set()
        for row in rows:
            targets = []
            if row.get("model_path"):
                targets.append(Path(str(row["model_path"])).resolve().parent.parent)
            if row.get("config_id"):
                targets.append((MODELS_ROOT / str(row["config_id"])).resolve())
            for target in targets:
                if target in seen or root not in target.parents:
                    continue
                seen.add(target)
                if target.exists():
                    shutil.rmtree(target)
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--modes", nargs="*", default=[m["mode"] for m in PREPROCESS_MODES])
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--replace-existing", action="store_true")
    parser.add_argument("--delete-existing-files", action="store_true")
    parser.add_argument("--result-path", default=None)
    args = parser.parse_args()

    read_env_file(ROOT / ".env")
    db_port = int(os.getenv("TDX_DB_PORT", "5432"))
    db_user = os.getenv("TDX_DB_USER", "postgres")
    db_password = os.getenv("TDX_DB_PASSWORD", "")
    db_name = os.getenv("TDX_DB_NAME", "aistock")
    if not db_password:
        raise RuntimeError("TDX_DB_PASSWORD is required")
    db_host = find_db_host(
        os.getenv("TDX_DB_HOST", "127.0.0.1"),
        db_port,
        db_user,
        db_password,
        db_name,
    )
    os.environ["TDX_DB_HOST"] = db_host

    selected_modes = [m for m in PREPROCESS_MODES if m["mode"] in set(args.modes)]
    missing = sorted(set(args.modes) - {m["mode"] for m in selected_modes})
    if missing:
        raise ValueError(f"Unknown preprocess modes: {missing}")

    cfg = make_cfg(db_host)
    raw, train_meta = load_training_raw(cfg)
    conn = db_connect()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    TMP_ROOT.mkdir(parents=True, exist_ok=True)
    backup_path = TMP_ROOT / f"hmm_preprocess_registry_before_{timestamp}.json"
    backup_existing(conn, backup_path)
    removed = 0
    if args.replace_existing and not args.dry_run:
        removed = cleanup_existing(conn, delete_files=bool(args.delete_existing_files))

    results: list[dict[str, Any]] = []
    for mode_cfg in selected_modes:
        params = fit_preprocess(mode_cfg, raw)
        models, metrics = train_models(cfg, raw, params)
        metrics["training_meta"] = train_meta
        for base in BASES:
            coeff_payload, coeff_meta = precompute_coefficients(
                cfg,
                models,
                params,
                base["coeffs"],
            )
            result = register_candidate(
                conn,
                base,
                mode_cfg,
                params,
                models,
                metrics,
                coeff_payload,
                coeff_meta,
                dry_run=bool(args.dry_run),
            )
            results.append(result)
            print(f"Registered {result['display_name']} snapshot={result['snapshot_id']}")
    conn.close()

    output = {
        "created_at": datetime.now(timezone.utc),
        "model_type": MODEL_TYPE,
        "snapshot_date": SNAPSHOT_DATE,
        "coefficient_file": COEFF_FILENAME,
        "backup_path": str(backup_path.resolve()),
        "replace_existing": bool(args.replace_existing),
        "removed_existing_rows": removed,
        "dry_run": bool(args.dry_run),
        "modes": [m["mode"] for m in selected_modes],
        "results": results,
    }
    result_path = (
        Path(args.result_path)
        if args.result_path
        else TMP_ROOT / f"hmm_preprocess_registry_result_{timestamp}.json"
    )
    write_json(result_path, output)
    print(json.dumps({"status": "ok", "result_path": str(result_path), "count": len(results)}))


if __name__ == "__main__":
    main()
