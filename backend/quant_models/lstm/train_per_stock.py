"""Per-stock LSTM 训练入口（真实训练逻辑，位于新程序目录中）。

注意：
- 本文件位于 next_app.backend.quant_models.lstm 下，不修改任何旧程序文件；
- 只依赖 TimescaleDB 中已有的行情/高频聚合表，通过
  ``LSTMDatasetConfig`` / ``load_lstm_timeseries_for_symbol`` 读取数据；
- 使用 PyTorch 实现一个简单的单步收益率预测 LSTM：
  - 输入：过去 seq_len 根 5m bar 的多维特征（K 线 + 高频特征）；
  - 目标：下一根 5m bar 的对数收益 ``log(close_{t+1} / close_t)``；
- 训练完成后：
  - 将模型权重保存到 ``next_app/backend/models/lstm_per_stock`` 目录；
  - 在 ``app.model_train_run`` 中记录一条训练 run 及关键指标。
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd
import torch
from torch import nn
from torch.utils.data import DataLoader, Dataset

from next_app.backend.db.pg_pool import get_conn
from next_app.backend.quant_datasets.lstm_dataset import (
    LSTMDatasetConfig,
    load_lstm_timeseries_for_symbol,
)


@dataclass
class TrainConfig:
    seq_len: int = 60
    hidden_size: int = 64
    num_layers: int = 1
    batch_size: int = 64
    epochs: int = 10
    lr: float = 1e-3
    device: str = "cuda" if torch.cuda.is_available() else "cpu"
    val_ratio: float = 0.2
    per_stock_mode: str = "FULL_LSTM"


class LSTMDataset(Dataset):
    def __init__(self, x: np.ndarray, y: np.ndarray) -> None:
        self.x = x.astype(np.float32)
        self.y = y.astype(np.float32)

    def __len__(self) -> int:  # noqa: D401
        return self.x.shape[0]

    def __getitem__(self, idx: int) -> Tuple[torch.Tensor, torch.Tensor]:  # noqa: D401
        return torch.from_numpy(self.x[idx]), torch.from_numpy(self.y[idx])


class LSTMRegressor(nn.Module):
    def __init__(self, input_size: int, hidden_size: int, num_layers: int = 1) -> None:
        super().__init__()
        self.lstm = nn.LSTM(
            input_size=input_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
        )
        self.head = nn.Linear(hidden_size, 1)

    def forward(self, x: torch.Tensor) -> torch.Tensor:  # noqa: D401
        out, _ = self.lstm(x)
        last = out[:, -1, :]
        return self.head(last).squeeze(-1)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Per-stock LSTM training")
    parser.add_argument("--symbol", required=True, help="ts_code, e.g. SH600000")
    parser.add_argument("--start", required=True, help="start datetime, e.g. 2020-01-01T09:30:00")
    parser.add_argument("--end", required=True, help="end datetime, e.g. 2024-01-01T15:00:00")
    parser.add_argument("--seq-len", type=int, default=60, help="sequence length (number of 5m bars)")
    parser.add_argument("--hidden-size", type=int, default=64, help="LSTM hidden size")
    parser.add_argument("--num-layers", type=int, default=1, help="number of LSTM layers")
    parser.add_argument("--batch-size", type=int, default=64, help="mini-batch size")
    parser.add_argument("--epochs", type=int, default=10, help="training epochs")
    parser.add_argument("--lr", type=float, default=1e-3, help="learning rate")
    parser.add_argument("--device", type=str, default=None, help="override device, e.g. cpu / cuda")
    parser.add_argument("--config", type=str, default=None, help="extra JSON config snapshot (optional)")
    return parser.parse_args()


def _to_dt(value: str) -> dt.datetime:
    return dt.datetime.fromisoformat(value)


def _load_extra_config(config_str: Optional[str]) -> Dict[str, Any]:
    if not config_str:
        return {}
    try:
        return json.loads(config_str)
    except Exception:
        return {"raw": config_str}


def _build_sequences(df: pd.DataFrame, seq_len: int) -> Tuple[np.ndarray, np.ndarray, Dict[str, Any]]:
    """将连续 5m 时序数据转换为 (X, y) 序列.

    - 特征：去掉非数值列，全部标准化后作为输入；
    - 目标：下一根 bar 的 log return: log(close_{t+1} / close_t)。
    """

    df = df.copy()
    # 保留数值列
    num_df = df.select_dtypes(include=["number"]).copy()
    if "close" not in num_df.columns:
        raise ValueError("input DataFrame must contain 'close' column for target construction")

    # 计算目标：下一步 log return
    close = num_df["close"].values
    next_close = np.roll(close, -1)
    log_ret = np.log(next_close / close)
    log_ret = log_ret[:-1]
    num_df = num_df.iloc[:-1, :]

    # 标准化特征
    feat_values = num_df.values
    mean = feat_values.mean(axis=0, keepdims=True)
    std = feat_values.std(axis=0, keepdims=True) + 1e-8
    feat_norm = (feat_values - mean) / std

    n = feat_norm.shape[0]
    if n <= seq_len:
        raise ValueError(f"not enough data points ({n}) for seq_len={seq_len}")

    xs: list[np.ndarray] = []
    ys: list[float] = []
    for i in range(0, n - seq_len):
        xs.append(feat_norm[i : i + seq_len])
        ys.append(log_ret[i + seq_len - 1])

    x_arr = np.stack(xs, axis=0)
    y_arr = np.array(ys, dtype=np.float32)
    meta = {
        "n_samples": int(x_arr.shape[0]),
        "n_features": int(x_arr.shape[2]),
        "feature_columns": list(num_df.columns),
        "mean": mean.squeeze().tolist(),
        "std": std.squeeze().tolist(),
    }
    return x_arr, y_arr, meta


def _train_lstm(
    x: np.ndarray,
    y: np.ndarray,
    cfg: TrainConfig,
    device: str,
) -> Tuple[LSTMRegressor, Dict[str, Any]]:
    n_samples, seq_len, n_features = x.shape
    ds = LSTMDataset(x, y)
    val_size = max(1, int(n_samples * cfg.val_ratio))
    train_size = n_samples - val_size
    train_ds, val_ds = torch.utils.data.random_split(ds, [train_size, val_size])

    train_loader = DataLoader(train_ds, batch_size=cfg.batch_size, shuffle=True)
    val_loader = DataLoader(val_ds, batch_size=cfg.batch_size, shuffle=False)

    model = LSTMRegressor(input_size=n_features, hidden_size=cfg.hidden_size, num_layers=cfg.num_layers).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=cfg.lr)
    criterion = nn.MSELoss()

    best_val_loss = float("inf")
    best_state: Dict[str, Any] | None = None

    for epoch in range(1, cfg.epochs + 1):
        model.train()
        train_loss = 0.0
        n_train = 0
        for xb, yb in train_loader:
            xb = xb.to(device)
            yb = yb.to(device)
            optimizer.zero_grad()
            pred = model(xb)
            loss = criterion(pred, yb)
            loss.backward()
            optimizer.step()
            train_loss += loss.item() * xb.size(0)
            n_train += xb.size(0)
        train_loss /= max(1, n_train)

        model.eval()
        val_loss = 0.0
        n_val = 0
        with torch.no_grad():
            for xb, yb in val_loader:
                xb = xb.to(device)
                yb = yb.to(device)
                pred = model(xb)
                loss = criterion(pred, yb)
                val_loss += loss.item() * xb.size(0)
                n_val += xb.size(0)
        val_loss /= max(1, n_val)

        print(f"[EPOCH] {epoch}/{cfg.epochs} train_loss={train_loss:.6f} val_loss={val_loss:.6f}")

        if val_loss < best_val_loss:
            best_val_loss = val_loss
            best_state = model.state_dict()

    if best_state is not None:
        model.load_state_dict(best_state)

    metrics = {
        "train_samples": int(train_size),
        "val_samples": int(val_size),
        "best_val_loss": float(best_val_loss),
    }
    return model, metrics


def _save_model(model: LSTMRegressor, ts_code: str, train_cfg: TrainConfig, dataset_meta: Dict[str, Any]) -> str:
    base_dir = Path(__file__).resolve().parents[3] / "models" / "lstm_per_stock"
    base_dir.mkdir(parents=True, exist_ok=True)
    safe_symbol = ts_code.replace(".", "_")
    path = base_dir / f"{safe_symbol}_5m.pt"
    payload = {
        "state_dict": model.state_dict(),
        "train_cfg": asdict(train_cfg),
        "dataset_meta": dataset_meta,
    }
    torch.save(payload, path)
    return str(path)


def _record_train_run(
    model_name: str,
    symbol: str,
    time_start: dt.datetime,
    time_end: dt.datetime,
    data_granularity: str,
    config_snapshot: Dict[str, Any],
    metrics: Dict[str, Any],
    log_path: Optional[str],
) -> None:
    start_ts = dt.datetime.now(dt.timezone.utc)
    end_ts = start_ts
    duration = 0.0

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO app.model_train_run (
                    model_name,
                    config_snapshot,
                    status,
                    start_time,
                    end_time,
                    duration_seconds,
                    symbols_covered_count,
                    time_range_start,
                    time_range_end,
                    data_granularity,
                    metrics_json,
                    log_path
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    model_name,
                    json.dumps(config_snapshot, ensure_ascii=False),
                    "SUCCESS",
                    start_ts,
                    end_ts,
                    duration,
                    1,
                    time_start,
                    time_end,
                    data_granularity,
                    json.dumps(metrics, ensure_ascii=False),
                    log_path,
                ),
            )


def main() -> None:
    args = _parse_args()
    ts_code = args.symbol
    start = _to_dt(args.start)
    end = _to_dt(args.end)

    train_cfg = TrainConfig(
        seq_len=args.seq_len,
        hidden_size=args.hidden_size,
        num_layers=args.num_layers,
        batch_size=args.batch_size,
        epochs=args.epochs,
        lr=args.lr,
        device=args.device or ("cuda" if torch.cuda.is_available() else "cpu"),
    )
    extra_cfg = _load_extra_config(args.config)

    print(f"[INFO] training LSTM_PER_STOCK for {ts_code} on device={train_cfg.device}")

    cfg = LSTMDatasetConfig()
    df: pd.DataFrame = load_lstm_timeseries_for_symbol(ts_code, start, end, cfg)
    if df.empty:
        print(f"[WARN] no data for {ts_code} in range {start} ~ {end}")
        metrics = {"rows": 0}
        _record_train_run(
            model_name="LSTM_PER_STOCK",
            symbol=ts_code,
            time_start=start,
            time_end=end,
            data_granularity="5m",
            config_snapshot={"symbol": ts_code, **asdict(train_cfg), **extra_cfg},
            metrics=metrics,
            log_path=None,
        )
        return

    print(f"[INFO] loaded LSTM dataset for {ts_code}: rows={len(df)}, cols={len(df.columns)}")

    x, y, meta = _build_sequences(df, seq_len=train_cfg.seq_len)
    model, train_metrics = _train_lstm(x, y, train_cfg, train_cfg.device)
    model_path = _save_model(model, ts_code, train_cfg, meta)
    print(f"[INFO] saved model to {model_path}")

    metrics = {"rows": int(len(df)), **train_metrics}
    _record_train_run(
        model_name="LSTM_PER_STOCK",
        symbol=ts_code,
        time_start=start,
        time_end=end,
        data_granularity="5m",
        config_snapshot={"symbol": ts_code, **asdict(train_cfg), **extra_cfg},
        metrics=metrics,
        log_path=model_path,
    )


if __name__ == "__main__":
    main()
