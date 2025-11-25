"""DeepAR-style RNN inference entry (Universe-level, new program only).

This script loads a trained DeepAR-style RNN model (daily / 60m),
performs batch inference for all symbols in its universe at a given
`as_of` time, and writes standardized signals to:

- app.quant_unified_signal
- app.model_inference_run

It mirrors the LSTM shared inference flow but operates on daily/60m
aggregated series built by `quant_datasets.deepar_dataset`.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import torch

from next_app.backend.db.pg_pool import get_conn
from next_app.backend.quant_datasets.deepar_dataset import (
    DeepARDatasetConfig,
    load_deepar_daily_for_symbol,
    load_deepar_60m_for_symbol,
)
from next_app.backend.quant_models.deepar.train import DeepARRegressor
from next_app.backend.quant_models.lstm.train_shared import (
    _load_static_features_for_symbols,
    _load_universe_config,
    _load_universe_symbols,
)
from next_app.backend.quant_models.lstm.train_per_stock import _build_sequences


# ---------------------------------------------------------------------------
# CLI & helpers
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DeepAR-style RNN inference (Universe-level)")
    parser.add_argument(
        "--universe-name",
        type=str,
        default="ALL_EQ_CLEAN",
        help=(
            "Universe name used to locate model file deepar_{UNIVERSE}_{freq}.pt; "
            "should match training."
        ),
    )
    parser.add_argument(
        "--freq",
        type=str,
        choices=["1d", "60m"],
        default="1d",
        help="target frequency: 1d (daily) or 60m",
    )
    parser.add_argument(
        "--as-of",
        type=str,
        default=None,
        help="as-of datetime, e.g. 2024-01-01T15:00:00 (default: current UTC)",
    )
    parser.add_argument(
        "--device",
        type=str,
        default=None,
        help="override device, e.g. cpu / cuda",
    )
    parser.add_argument(
        "--schedule-name",
        type=str,
        default=None,
        help="optional: scheduler name to record in model_inference_run",
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="extra JSON config snapshot (optional)",
    )
    return parser.parse_args()


def _to_dt(value: str) -> dt.datetime:
    return dt.datetime.fromisoformat(value)


def _now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _load_extra_config(config_str: Optional[str]) -> Dict[str, Any]:
    if not config_str:
        return {}
    try:
        return json.loads(config_str)
    except Exception:  # noqa: BLE001
        return {"raw": config_str}


def _model_name_for_freq(freq: str) -> str:
    if freq == "60m":
        return "DEEPAR_60M"
    return "DEEPAR_DAILY"


def _model_path_for_universe(universe_name: str, freq: str) -> Path:
    base_dir = Path(__file__).resolve().parents[3] / "models" / "deepar"
    safe_universe = universe_name.replace(" ", "_")
    return base_dir / f"deepar_{safe_universe}_{freq}.pt"


def _load_model(
    universe_name: str,
    freq: str,
    device: str,
) -> Tuple[DeepARRegressor, Dict[str, Any], Dict[str, Any], Dict[str, int], str]:
    """Load trained DeepAR model and metadata."""

    path = _model_path_for_universe(universe_name, freq)
    if not path.exists():
        raise FileNotFoundError(f"DeepAR model file not found for universe={universe_name}, freq={freq}: {path}")

    payload = torch.load(path, map_location=device)
    state_dict = payload["state_dict"]
    train_cfg: Dict[str, Any] = payload.get("train_cfg", {})
    dataset_meta: Dict[str, Any] = payload.get("dataset_meta", {})
    symbol2id: Dict[str, int] = payload.get("symbol2id", {})
    stored_freq = payload.get("freq") or dataset_meta.get("freq") or freq

    if stored_freq != freq:
        raise ValueError(f"freq mismatch between payload ({stored_freq}) and requested ({freq})")

    feature_columns = dataset_meta.get("feature_columns") or []
    static_columns = dataset_meta.get("static_columns") or []
    dyn_dim = int(len(feature_columns))
    static_dim = int(len(static_columns))

    if dyn_dim <= 0:
        raise ValueError("invalid dyn_dim from dataset_meta; feature_columns must be non-empty")

    hidden_size = int(train_cfg.get("hidden_size", 64))
    num_layers = int(train_cfg.get("num_layers", 1))
    symbol_emb_dim = int(train_cfg.get("symbol_emb_dim", 16))
    num_symbols = max(symbol2id.values()) + 1 if symbol2id else 0

    model = DeepARRegressor(
        dyn_input_size=dyn_dim,
        static_input_size=static_dim,
        num_symbols=num_symbols,
        symbol_emb_dim=symbol_emb_dim,
        hidden_size=hidden_size,
        num_layers=num_layers,
    ).to(device)
    model.load_state_dict(state_dict)
    model.eval()

    return model, train_cfg, dataset_meta, symbol2id, str(path)


# ---------------------------------------------------------------------------
# Build inference batch
# ---------------------------------------------------------------------------


def _build_inference_batch(
    symbols: Sequence[str],
    symbol2id: Mapping[str, int],
    as_of: dt.datetime,
    freq: str,
    seq_len: int,
    expected_feature_columns: Sequence[str],
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, List[str], int]:
    """Build a DeepAR inference batch (one sample per symbol).

    For each symbol:
    - load daily / 60m series over a history window (based on DeepARDatasetConfig.history_years),
    - use `_build_sequences` to transform into (X, y) sequences,
    - keep the last sequence as the inference sample.

    Returns: x_dyn_batch, x_static_batch, symbol_ids_batch, symbols_kept, total_rows.
    """

    if not symbols:
        raise ValueError("no symbols provided for inference batch")

    cfg = DeepARDatasetConfig()

    # Static features as of as_of.date
    static_map, _static_meta = _load_static_features_for_symbols(symbols, as_of_date=as_of.date())

    xs_dyn: List[np.ndarray] = []
    xs_static: List[np.ndarray] = []
    sym_ids: List[np.ndarray] = []
    symbols_kept: List[str] = []
    total_rows = 0

    history_days = int(cfg.history_years * 365) + 5

    for ts_code in symbols:
        if ts_code not in symbol2id:
            continue
        if ts_code not in static_map:
            # Skip symbols without static features
            continue

        if freq == "60m":
            start_time = as_of - dt.timedelta(days=history_days)
            df = load_deepar_60m_for_symbol(ts_code, start_time, as_of, cfg)
        else:
            end_date = as_of.date()
            start_date = end_date - dt.timedelta(days=history_days)
            df = load_deepar_daily_for_symbol(ts_code, start_date, end_date, cfg)

        if df.empty:
            continue

        total_rows += int(len(df))

        try:
            x_dyn_all, _y_dummy, meta = _build_sequences(df, seq_len=seq_len)
        except Exception:  # noqa: BLE001
            # insufficient data, etc.
            continue

        if x_dyn_all.shape[0] == 0:
            continue

        feature_columns = meta.get("feature_columns") or []
        if expected_feature_columns and list(expected_feature_columns) != list(feature_columns):
            # To avoid silent feature-mismatch errors, skip this symbol
            continue

        x_dyn_last = x_dyn_all[-1]  # (seq_len, dyn_dim)
        s_vec = static_map[ts_code]

        xs_dyn.append(x_dyn_last[None, ...])
        xs_static.append(s_vec[None, :])
        sym_ids.append(np.array([symbol2id[ts_code]], dtype=np.int64))
        symbols_kept.append(ts_code)

    if not xs_dyn:
        raise ValueError("no effective samples for DeepAR inference")

    x_dyn_batch = np.concatenate(xs_dyn, axis=0)
    x_static_batch = np.concatenate(xs_static, axis=0)
    symbol_ids_batch = np.concatenate(sym_ids, axis=0)

    return x_dyn_batch, x_static_batch, symbol_ids_batch, symbols_kept, total_rows


# ---------------------------------------------------------------------------
# Write to quant_unified_signal / model_inference_run
# ---------------------------------------------------------------------------


def _upsert_quant_unified_signal(
    symbol: str,
    as_of_time: dt.datetime,
    frequency: str,
    horizon: str,
    direction: Optional[str],
    expected_return: Optional[float],
    expected_volatility: Optional[float],
    model_votes: Optional[Dict[str, Any]],
    model_versions: Optional[Dict[str, Any]],
) -> None:
    """Insert or update DeepAR prediction into app.quant_unified_signal."""

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO app.quant_unified_signal (
                    symbol,
                    as_of_time,
                    frequency,
                    horizon,
                    direction,
                    prob_up,
                    prob_down,
                    prob_flat,
                    confidence,
                    expected_return,
                    expected_volatility,
                    risk_score,
                    regime,
                    liquidity_label,
                    microstructure_label,
                    anomaly_flags,
                    suggested_position_delta,
                    suggested_t0_action,
                    model_votes,
                    ensemble_method,
                    data_coverage,
                    model_versions,
                    quality_flags
                )
                VALUES (
                    %s, %s, %s, %s,
                    %s,
                    %s, %s, %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                )
                ON CONFLICT (symbol, as_of_time, frequency, horizon)
                DO UPDATE SET
                    direction = EXCLUDED.direction,
                    prob_up = EXCLUDED.prob_up,
                    prob_down = EXCLUDED.prob_down,
                    prob_flat = EXCLUDED.prob_flat,
                    confidence = EXCLUDED.confidence,
                    expected_return = EXCLUDED.expected_return,
                    expected_volatility = EXCLUDED.expected_volatility,
                    risk_score = EXCLUDED.risk_score,
                    regime = EXCLUDED.regime,
                    liquidity_label = EXCLUDED.liquidity_label,
                    microstructure_label = EXCLUDED.microstructure_label,
                    anomaly_flags = EXCLUDED.anomaly_flags,
                    suggested_position_delta = EXCLUDED.suggested_position_delta,
                    suggested_t0_action = EXCLUDED.suggested_t0_action,
                    model_votes = EXCLUDED.model_votes,
                    ensemble_method = EXCLUDED.ensemble_method,
                    data_coverage = EXCLUDED.data_coverage,
                    model_versions = EXCLUDED.model_versions,
                    quality_flags = EXCLUDED.quality_flags,
                    updated_at = NOW()
                """,
                (
                    symbol,
                    as_of_time,
                    frequency,
                    horizon,
                    direction,
                    None,
                    None,
                    None,
                    None,
                    expected_return,
                    expected_volatility,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    json.dumps(model_votes, ensure_ascii=False) if model_votes is not None else None,
                    None,
                    None,
                    json.dumps(model_versions, ensure_ascii=False) if model_versions is not None else None,
                    None,
                ),
            )


def _record_inference_run(
    model_name: str,
    schedule_name: Optional[str],
    universe_name: str,
    as_of_time: dt.datetime,
    symbols_covered: int,
    config_snapshot: Dict[str, Any],
    metrics: Dict[str, Any],
    status: str = "SUCCESS",
) -> None:
    start_ts = _now_utc()
    end_ts = start_ts
    duration = 0.0

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO app.model_inference_run (
                    model_name,
                    schedule_name,
                    config_snapshot,
                    status,
                    start_time,
                    end_time,
                    duration_seconds,
                    symbols_covered,
                    time_of_data,
                    metrics_json
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    model_name,
                    schedule_name,
                    json.dumps(config_snapshot, ensure_ascii=False),
                    status,
                    start_ts,
                    end_ts,
                    duration,
                    symbols_covered,
                    as_of_time,
                    json.dumps(metrics, ensure_ascii=False),
                ),
            )


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def main() -> None:
    args = _parse_args()

    universe_name = args.universe_name
    freq = args.freq
    as_of = _to_dt(args.as_of) if args.as_of else _now_utc()

    device = args.device or ("cuda" if torch.cuda.is_available() else "cpu")
    extra_cfg = _load_extra_config(args.config)

    model_name = _model_name_for_freq(freq)

    print(
        f"[INFO] running {model_name} inference for universe={universe_name} "
        f"freq={freq} device={device}",
    )

    # 1) Load model
    try:
        model, train_cfg, dataset_meta, symbol2id, model_path = _load_model(
            universe_name=universe_name,
            freq=freq,
            device=device,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] failed to load DeepAR model: {exc}")
        return

    seq_len = int(train_cfg.get("seq_len", 60))

    symbols = sorted(symbol2id.keys())
    if not symbols:
        print("[WARN] model has empty symbol2id mapping, nothing to infer")
        return

    feature_columns = dataset_meta.get("feature_columns") or []

    # 2) Build inference batch
    try:
        x_dyn, x_static, sym_ids, symbols_kept, total_rows = _build_inference_batch(
            symbols=symbols,
            symbol2id=symbol2id,
            as_of=as_of,
            freq=freq,
            seq_len=seq_len,
            expected_feature_columns=feature_columns,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] failed to build DeepAR inference batch: {exc}")
        metrics = {
            "rows": 0,
            "symbols_covered": 0,
            "error": str(exc),
        }
        config_snapshot = {
            "universe_name": universe_name,
            "freq": freq,
            "as_of": as_of.isoformat(),
            "model_path": model_path,
            **train_cfg,
            **extra_cfg,
        }
        _record_inference_run(
            model_name=model_name,
            schedule_name=args.schedule_name,
            universe_name=universe_name,
            as_of_time=as_of,
            symbols_covered=0,
            config_snapshot=config_snapshot,
            metrics=metrics,
            status="ERROR",
        )
        return

    if x_dyn.shape[0] == 0:
        print("[WARN] no valid samples for DeepAR inference")
        metrics = {
            "rows": int(total_rows),
            "symbols_covered": 0,
            "note": "no valid samples",
        }
        config_snapshot = {
            "universe_name": universe_name,
            "freq": freq,
            "as_of": as_of.isoformat(),
            "model_path": model_path,
            **train_cfg,
            **extra_cfg,
        }
        _record_inference_run(
            model_name=model_name,
            schedule_name=args.schedule_name,
            universe_name=universe_name,
            as_of_time=as_of,
            symbols_covered=0,
            config_snapshot=config_snapshot,
            metrics=metrics,
            status="SUCCESS",
        )
        return

    # 3) Forward pass
    with torch.no_grad():
        x_dyn_t = torch.from_numpy(x_dyn.astype(np.float32)).to(device)
        x_static_t = torch.from_numpy(x_static.astype(np.float32)).to(device)
        sym_ids_t = torch.from_numpy(sym_ids.astype(np.int64)).to(device)
        mu, sigma = model(x_dyn_t, x_static_t, sym_ids_t)
        mu_np = mu.cpu().numpy().reshape(-1)
        sigma_np = sigma.cpu().numpy().reshape(-1)

    # 4) Write to quant_unified_signal
    horizon = "60m" if freq == "60m" else "1d"

    for ts_code, mu_i, sigma_i in zip(symbols_kept, mu_np, sigma_np):
        direction: Optional[str]
        if mu_i > 0:
            direction = "UP"
        elif mu_i < 0:
            direction = "DOWN"
        else:
            direction = "FLAT"

        expected_return = float(mu_i)
        expected_volatility = float(abs(sigma_i))

        model_votes = {
            model_name: {
                "pred_mu": expected_return,
                "pred_sigma": expected_volatility,
            }
        }
        model_versions = {
            model_name: {
                "model_path": model_path,
                "universe_name": universe_name,
                "freq": freq,
            }
        }

        _upsert_quant_unified_signal(
            symbol=ts_code,
            as_of_time=as_of,
            frequency=freq,
            horizon=horizon,
            direction=direction,
            expected_return=expected_return,
            expected_volatility=expected_volatility,
            model_votes=model_votes,
            model_versions=model_versions,
        )

    # 5) Record model_inference_run
    metrics = {
        "rows": int(total_rows),
        "symbols_covered": int(len(symbols_kept)),
        "pred_mu_mean": float(np.mean(mu_np)) if mu_np.size > 0 else 0.0,
        "pred_mu_std": float(np.std(mu_np)) if mu_np.size > 0 else 0.0,
        "pred_sigma_mean": float(np.mean(sigma_np)) if sigma_np.size > 0 else 0.0,
        "pred_sigma_std": float(np.std(sigma_np)) if sigma_np.size > 0 else 0.0,
    }
    config_snapshot = {
        "universe_name": universe_name,
        "freq": freq,
        "as_of": as_of.isoformat(),
        "model_path": model_path,
        **train_cfg,
        **extra_cfg,
    }

    _record_inference_run(
        model_name=model_name,
        schedule_name=args.schedule_name,
        universe_name=universe_name,
        as_of_time=as_of,
        symbols_covered=len(symbols_kept),
        config_snapshot=config_snapshot,
        metrics=metrics,
        status="SUCCESS",
    )

    print(
        f"[INFO] DeepAR inference done for universe={universe_name}, "
        f"freq={freq}, symbols={len(symbols_kept)}, as_of={as_of}",
    )


if __name__ == "__main__":
    main()
