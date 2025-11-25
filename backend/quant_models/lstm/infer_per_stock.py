"""Per-stock LSTM 推理入口（新程序目录，仅依赖已有表和训练好的模型权重）。

约定：
- 模型权重由 `train_per_stock.py` 训练并保存在 `models/lstm_per_stock/{SYMBOL}_5m.pt`；
- 只读 TimescaleDB 中的市场数据表和聚合特征表，不修改旧程序；
- 推理结果写入：
  - `app.model_inference_run`：记录一次推理 run 的元信息；
  - `app.quant_unified_signal`：存放标准化信号记录（支持后续多模型集成）。
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import numpy as np
import torch

from next_app.backend.db.pg_pool import get_conn
from next_app.backend.quant_datasets.lstm_dataset import (
    LSTMDatasetConfig,
    load_lstm_timeseries_for_symbol,
)
from .train_per_stock import LSTMRegressor


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Per-stock LSTM inference")
    parser.add_argument("--symbol", required=True, help="ts_code, e.g. SH600000")
    parser.add_argument(
        "--as-of",
        type=str,
        default=None,
        help="as-of datetime, e.g. 2024-01-01T15:00:00 (默认当前 UTC 时间)",
    )
    parser.add_argument(
        "--lookback-minutes",
        type=int,
        default=None,
        help="历史回看窗口（分钟），默认按训练时的 seq_len*5 估算",
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
        help="可选：推理调度名称，用于写入 model_inference_run",
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
    except Exception:
        return {"raw": config_str}


def _model_path_for_symbol(ts_code: str) -> Path:
    base_dir = Path(__file__).resolve().parents[3] / "models" / "lstm_per_stock"
    safe_symbol = ts_code.replace(".", "_")
    return base_dir / f"{safe_symbol}_5m.pt"


def _load_model(
    ts_code: str,
    device: str,
) -> Tuple[LSTMRegressor, Dict[str, Any], Dict[str, Any], str]:
    """加载已训练好的 per-stock LSTM 模型及其元信息."""

    path = _model_path_for_symbol(ts_code)
    if not path.exists():
        raise FileNotFoundError(f"model file not found for {ts_code}: {path}")

    payload = torch.load(path, map_location=device)
    state_dict = payload["state_dict"]
    train_cfg: Dict[str, Any] = payload.get("train_cfg", {})
    dataset_meta: Dict[str, Any] = payload.get("dataset_meta", {})

    n_features = int(dataset_meta.get("n_features"))
    hidden_size = int(train_cfg.get("hidden_size", 64))
    num_layers = int(train_cfg.get("num_layers", 1))

    model = LSTMRegressor(
        input_size=n_features,
        hidden_size=hidden_size,
        num_layers=num_layers,
    ).to(device)
    model.load_state_dict(state_dict)
    model.eval()

    return model, train_cfg, dataset_meta, str(path)


def _upsert_quant_unified_signal(
    symbol: str,
    as_of_time: dt.datetime,
    frequency: str,
    horizon: str,
    direction: Optional[str],
    expected_return: Optional[float],
    model_votes: Optional[Dict[str, Any]],
    model_versions: Optional[Dict[str, Any]],
) -> None:
    """将 LSTM 推理结果写入/更新 app.quant_unified_signal."""

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
                    None,
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
    symbol: str,
    as_of_time: dt.datetime,
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
                    1,
                    as_of_time,
                    json.dumps(metrics, ensure_ascii=False),
                ),
            )


def main() -> None:
    args = _parse_args()
    ts_code = args.symbol
    as_of = _to_dt(args.as_of) if args.as_of else _now_utc()

    device = args.device or ("cuda" if torch.cuda.is_available() else "cpu")
    extra_cfg = _load_extra_config(args.config)

    print(f"[INFO] running LSTM_PER_STOCK inference for {ts_code} on device={device}")

    # 1) 加载模型及元信息
    model, train_cfg, dataset_meta, model_path = _load_model(ts_code, device)
    seq_len = int(train_cfg.get("seq_len", 60))

    # 2) 计算回看窗口并加载最近一段 5m 数据
    lookback_minutes = args.lookback_minutes or seq_len * 5
    start_time = as_of - dt.timedelta(minutes=lookback_minutes)

    cfg = LSTMDatasetConfig()
    df = load_lstm_timeseries_for_symbol(ts_code, start_time, as_of, cfg)
    if df.empty:
        print(f"[WARN] no data for {ts_code} in range {start_time} ~ {as_of}")
        metrics = {"rows": 0, "note": "no data"}
        config_snapshot = {
            "symbol": ts_code,
            "as_of": as_of.isoformat(),
            "lookback_minutes": lookback_minutes,
            "model_path": model_path,
            **train_cfg,
            **extra_cfg,
        }
        _record_inference_run(
            model_name="LSTM_PER_STOCK",
            schedule_name=args.schedule_name,
            symbol=ts_code,
            as_of_time=as_of,
            config_snapshot=config_snapshot,
            metrics=metrics,
            status="SUCCESS",
        )
        return

    num_df = df.select_dtypes(include=["number"]).copy()
    feature_columns = dataset_meta.get("feature_columns") or list(num_df.columns)
    missing_cols = [c for c in feature_columns if c not in num_df.columns]
    if missing_cols:
        msg = f"missing feature columns for inference: {missing_cols}"
        print(f"[ERROR] {msg}")
        metrics = {"rows": int(len(df)), "error": msg}
        config_snapshot = {
            "symbol": ts_code,
            "as_of": as_of.isoformat(),
            "lookback_minutes": lookback_minutes,
            "model_path": model_path,
            **train_cfg,
            **extra_cfg,
        }
        _record_inference_run(
            model_name="LSTM_PER_STOCK",
            schedule_name=args.schedule_name,
            symbol=ts_code,
            as_of_time=as_of,
            config_snapshot=config_snapshot,
            metrics=metrics,
            status="ERROR",
        )
        return

    num_df = num_df[feature_columns]
    values = num_df.values
    if values.shape[0] < seq_len:
        print(
            f"[WARN] not enough rows for inference: got {values.shape[0]} < seq_len={seq_len}",
        )
        metrics = {"rows": int(values.shape[0]), "note": "not enough rows"}
        config_snapshot = {
            "symbol": ts_code,
            "as_of": as_of.isoformat(),
            "lookback_minutes": lookback_minutes,
            "model_path": model_path,
            **train_cfg,
            **extra_cfg,
        }
        _record_inference_run(
            model_name="LSTM_PER_STOCK",
            schedule_name=args.schedule_name,
            symbol=ts_code,
            as_of_time=as_of,
            config_snapshot=config_snapshot,
            metrics=metrics,
            status="SUCCESS",
        )
        return

    # 3) 读取训练时的标准化参数，对最近 seq_len 条样本做归一化
    mean = np.asarray(dataset_meta.get("mean"), dtype=np.float32)
    std = np.asarray(dataset_meta.get("std"), dtype=np.float32) + 1e-8

    last_seq = values[-seq_len:]
    norm_seq = (last_seq - mean) / std
    x = norm_seq.astype(np.float32)[None, ...]  # (1, seq_len, n_features)

    with torch.no_grad():
        x_tensor = torch.from_numpy(x).to(device)
        pred = model(x_tensor).cpu().numpy().reshape(-1)[0]

    direction: Optional[str]
    if pred > 0:
        direction = "UP"
    elif pred < 0:
        direction = "DOWN"
    else:
        direction = "FLAT"

    expected_return = float(pred)

    model_votes = {
        "LSTM_PER_STOCK": {
            "pred_log_return": expected_return,
        }
    }
    model_versions = {
        "LSTM_PER_STOCK": {
            "model_path": model_path,
        }
    }

    # 4) 写入 quant_unified_signal（5m 频率、5m horizon）
    _upsert_quant_unified_signal(
        symbol=ts_code,
        as_of_time=as_of,
        frequency="5m",
        horizon="5m",
        direction=direction,
        expected_return=expected_return,
        model_votes=model_votes,
        model_versions=model_versions,
    )

    metrics = {
        "rows": int(len(df)),
        "seq_len": seq_len,
        "pred_log_return": expected_return,
    }
    config_snapshot = {
        "symbol": ts_code,
        "as_of": as_of.isoformat(),
        "lookback_minutes": lookback_minutes,
        "model_path": model_path,
        **train_cfg,
        **extra_cfg,
    }

    _record_inference_run(
        model_name="LSTM_PER_STOCK",
        schedule_name=args.schedule_name,
        symbol=ts_code,
        as_of_time=as_of,
        config_snapshot=config_snapshot,
        metrics=metrics,
        status="SUCCESS",
    )

    print(
        f"[INFO] inference done for {ts_code} as_of={as_of}, pred_log_return={expected_return:.6f}",
    )


if __name__ == "__main__":
    main()
