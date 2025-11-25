"""Shared LSTM 推理入口（Universe 级批量推理，位于新程序目录）。

与 `train_shared.py` 和设计文档第 6 章保持一致：
- 使用已训练好的 `LSTM_SHARED` 模型权重（models/lstm_shared/shared_{UNIVERSE}_5m.pt）；
- 只读 TimescaleDB 中的行情/高频特征与静态特征表，不修改旧程序；
- 对 Universe 中的所有股票在给定 `as_of` 时刻做一次前向预测：
  - 输出下一根 5m log return 的预测值 `y_shared`；
  - 将结果写入：
    - `app.quant_unified_signal`（frequency='5m', horizon='5m'，model_votes 中包含 LSTM_SHARED）；
    - `app.model_inference_run`（记录一次 Universe 级推理 run 的元信息）。

注意：
- 本脚本不会触发任何训练，仅做推理；
- 推理 Universe 默认取模型权重中保存的 symbol2id 键集；
- Universe/静态特征表的具体配置和填充逻辑由其它脚本负责，不在此脚本中修改。
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
from next_app.backend.quant_datasets.lstm_dataset import (
    LSTMDatasetConfig,
    load_lstm_timeseries_for_symbol,
)
from .train_per_stock import _build_sequences
from .train_shared import (
    SharedLSTMRegressor,
    _load_static_features_for_symbols,
)


# ---------------------------------------------------------------------------
# CLI & 工具函数
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Shared LSTM inference (Universe-level)")
    parser.add_argument(
        "--universe-name",
        type=str,
        default="ALL_EQ_CLEAN",
        help=(
            "Universe 名称，用于定位模型文件 shared_{UNIVERSE}_5m.pt；"
            "应与训练时使用的一致"
        ),
    )
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
        help=(
            "历史回看窗口（分钟），默认按训练时的 seq_len*5 估算；"
            "窗口内的数据用于构造最后一个序列"
        ),
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


def _model_path_for_universe(universe_name: str) -> Path:
    base_dir = Path(__file__).resolve().parents[3] / "models" / "lstm_shared"
    safe_universe = universe_name.replace(" ", "_")
    return base_dir / f"shared_{safe_universe}_5m.pt"


def _load_model(
    universe_name: str,
    device: str,
) -> Tuple[SharedLSTMRegressor, Dict[str, Any], Dict[str, Any], Dict[str, int], str]:
    """加载已训练好的 shared LSTM 模型及其元信息."""

    path = _model_path_for_universe(universe_name)
    if not path.exists():
        raise FileNotFoundError(f"shared model file not found for universe={universe_name}: {path}")

    payload = torch.load(path, map_location=device)
    state_dict = payload["state_dict"]
    train_cfg: Dict[str, Any] = payload.get("train_cfg", {})
    dataset_meta: Dict[str, Any] = payload.get("dataset_meta", {})
    symbol2id: Dict[str, int] = payload.get("symbol2id", {})

    feature_columns = dataset_meta.get("feature_columns") or []
    static_columns = dataset_meta.get("static_columns") or []
    dyn_dim = int(len(feature_columns))
    static_dim = int(len(static_columns))

    hidden_size = int(train_cfg.get("hidden_size", 64))
    num_layers = int(train_cfg.get("num_layers", 1))
    symbol_emb_dim = int(train_cfg.get("symbol_emb_dim", 16))
    num_symbols = max(symbol2id.values()) + 1 if symbol2id else 0

    if dyn_dim <= 0:
        raise ValueError("invalid dyn_dim from dataset_meta; feature_columns must be non-empty")

    model = SharedLSTMRegressor(
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
# 构建推理批次（每个 symbol 一条样本）
# ---------------------------------------------------------------------------


def _build_inference_batch(
    symbols: Sequence[str],
    symbol2id: Mapping[str, int],
    as_of: dt.datetime,
    lookback_minutes: int,
    seq_len: int,
    expected_feature_columns: Sequence[str],
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, List[str], int]:
    """为给定股票集合构建 shared LSTM 推理批次.

    每只股票取一个样本（最后一个序列）：
    - x_dyn: 通过 `_build_sequences` 在给定时间窗内构造，然后取最后一条；
    - x_static: 通过 `_load_static_features_for_symbols` 预先加载；
    - symbol_id: 由 symbol2id 映射；
    返回：x_dyn_batch, x_static_batch, symbol_ids_batch, symbols_kept, total_rows.
    """

    if not symbols:
        raise ValueError("no symbols provided for inference batch")

    cfg = LSTMDatasetConfig()

    # 统一加载静态特征（按 as_of.date）
    static_map, _static_meta = _load_static_features_for_symbols(symbols, as_of_date=as_of.date())

    xs_dyn: List[np.ndarray] = []
    xs_static: List[np.ndarray] = []
    sym_ids: List[np.ndarray] = []
    symbols_kept: List[str] = []
    total_rows = 0

    start_time = as_of - dt.timedelta(minutes=lookback_minutes)

    for ts_code in symbols:
        if ts_code not in symbol2id:
            continue
        if ts_code not in static_map:
            # 无静态特征则暂时跳过该股票
            continue

        df = load_lstm_timeseries_for_symbol(ts_code, start_time, as_of, cfg)
        if df.empty:
            continue
        total_rows += int(len(df))

        try:
            x_dyn_all, _y_dummy, meta = _build_sequences(df, seq_len=seq_len)
        except Exception:
            # 数据点不足等情况，跳过该票
            continue

        if x_dyn_all.shape[0] == 0:
            continue

        feature_columns = meta.get("feature_columns") or []
        if expected_feature_columns and list(expected_feature_columns) != list(feature_columns):
            # 特征列不一致时，为避免 silent 错误，跳过该票
            continue

        x_dyn_last = x_dyn_all[-1]  # (seq_len, dyn_dim)
        s_vec = static_map[ts_code]  # (static_dim,)

        xs_dyn.append(x_dyn_last[None, ...])
        xs_static.append(s_vec[None, :])
        sym_ids.append(np.array([symbol2id[ts_code]], dtype=np.int64))
        symbols_kept.append(ts_code)

    if not xs_dyn:
        raise ValueError("no effective samples for shared LSTM inference")

    x_dyn_batch = np.concatenate(xs_dyn, axis=0)
    x_static_batch = np.concatenate(xs_static, axis=0)
    symbol_ids_batch = np.concatenate(sym_ids, axis=0)

    return x_dyn_batch, x_static_batch, symbol_ids_batch, symbols_kept, total_rows


# ---------------------------------------------------------------------------
# 写入 quant_unified_signal / model_inference_run
# ---------------------------------------------------------------------------


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
    """将 shared LSTM 推理结果写入/更新 app.quant_unified_signal."""

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
    as_of = _to_dt(args.as_of) if args.as_of else _now_utc()

    device = args.device or ("cuda" if torch.cuda.is_available() else "cpu")
    extra_cfg = _load_extra_config(args.config)

    print(f"[INFO] running LSTM_SHARED inference for universe={universe_name} on device={device}")

    # 1) 加载模型
    try:
        model, train_cfg, dataset_meta, symbol2id, model_path = _load_model(universe_name, device)
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] failed to load shared model: {exc}")
        return

    seq_len = int(train_cfg.get("seq_len", 60))
    lookback_minutes = args.lookback_minutes or seq_len * 5

    symbols = sorted(symbol2id.keys())
    if not symbols:
        print("[WARN] model has empty symbol2id mapping, nothing to infer")
        return

    feature_columns = dataset_meta.get("feature_columns") or []

    # 2) 构建推理批次
    try:
        x_dyn, x_static, sym_ids, symbols_kept, total_rows = _build_inference_batch(
            symbols=symbols,
            symbol2id=symbol2id,
            as_of=as_of,
            lookback_minutes=lookback_minutes,
            seq_len=seq_len,
            expected_feature_columns=feature_columns,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] failed to build inference batch: {exc}")
        metrics = {
            "rows": 0,
            "symbols_covered": 0,
            "error": str(exc),
        }
        config_snapshot = {
            "universe_name": universe_name,
            "as_of": as_of.isoformat(),
            "lookback_minutes": lookback_minutes,
            "model_path": model_path,
            **train_cfg,
            **extra_cfg,
        }
        _record_inference_run(
            model_name="LSTM_SHARED",
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
        print("[WARN] no valid samples for shared inference")
        metrics = {
            "rows": int(total_rows),
            "symbols_covered": 0,
            "note": "no valid samples",
        }
        config_snapshot = {
            "universe_name": universe_name,
            "as_of": as_of.isoformat(),
            "lookback_minutes": lookback_minutes,
            "model_path": model_path,
            **train_cfg,
            **extra_cfg,
        }
        _record_inference_run(
            model_name="LSTM_SHARED",
            schedule_name=args.schedule_name,
            universe_name=universe_name,
            as_of_time=as_of,
            symbols_covered=0,
            config_snapshot=config_snapshot,
            metrics=metrics,
            status="SUCCESS",
        )
        return

    # 3) 前向推理
    with torch.no_grad():
        x_dyn_t = torch.from_numpy(x_dyn.astype(np.float32)).to(device)
        x_static_t = torch.from_numpy(x_static.astype(np.float32)).to(device)
        sym_ids_t = torch.from_numpy(sym_ids.astype(np.int64)).to(device)
        preds = model(x_dyn_t, x_static_t, sym_ids_t).cpu().numpy().reshape(-1)

    # 4) 写入 quant_unified_signal
    for ts_code, pred in zip(symbols_kept, preds):
        direction: Optional[str]
        if pred > 0:
            direction = "UP"
        elif pred < 0:
            direction = "DOWN"
        else:
            direction = "FLAT"

        expected_return = float(pred)
        model_votes = {
            "LSTM_SHARED": {
                "pred_log_return": expected_return,
            }
        }
        model_versions = {
            "LSTM_SHARED": {
                "model_path": model_path,
                "universe_name": universe_name,
            }
        }

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

    # 5) 写入 model_inference_run
    metrics = {
        "rows": int(total_rows),
        "symbols_covered": int(len(symbols_kept)),
        "pred_mean": float(np.mean(preds)) if preds.size > 0 else 0.0,
        "pred_std": float(np.std(preds)) if preds.size > 0 else 0.0,
    }
    config_snapshot = {
        "universe_name": universe_name,
        "as_of": as_of.isoformat(),
        "lookback_minutes": lookback_minutes,
        "model_path": model_path,
        **train_cfg,
        **extra_cfg,
    }

    _record_inference_run(
        model_name="LSTM_SHARED",
        schedule_name=args.schedule_name,
        universe_name=universe_name,
        as_of_time=as_of,
        symbols_covered=len(symbols_kept),
        config_snapshot=config_snapshot,
        metrics=metrics,
        status="SUCCESS",
    )

    print(
        f"[INFO] shared inference done for universe={universe_name}, "
        f"symbols={len(symbols_kept)}, as_of={as_of}"
    )


if __name__ == "__main__":
    main()
