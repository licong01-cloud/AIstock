"""Per-stock refinement 模块（基于 shared LSTM 输出 + 该股特有因子）.

设计与 `docs/quant_model_evaluation.md` 第 6 章一致：

- 底层：Universe 级 shared LSTM (`LSTM_SHARED`)
  - 已在 `train_shared.py` / `infer_shared.py` 中实现，模型文件为
    `models/lstm_shared/shared_{UNIVERSE}_5m.pt`；

- 上层：per-stock refinement 层 (`LSTM_REFINEMENT`)
  - 训练：
    - 在给定时间区间内，基于 **全 Universe** 的样本，构建：
      - 特征：`[y_shared, 静态特征向量]`；
      - 目标：残差 `residual = y_true - y_shared`；
    - 用一个轻量 MLP 学习 residual（多股票联合训练，但输出是 per-stock 的修正）；
    - 训练 run 记录到 `app.model_train_run`（`model_name = 'LSTM_REFINEMENT'`）。
  - 推理：
    - 对单只股票：
      - 使用 shared LSTM 在给定 `as_of` 处计算 `y_shared`；
      - 结合静态特征，使用 refinement MLP 预测 residual，并得到 `y_final = y_shared + residual_pred`；
      - 将结果写入：
        - `app.quant_unified_signal`：`expected_return = y_final`，
          `model_votes` 中同时包含 `LSTM_SHARED` 与 `LSTM_REFINEMENT`；
        - `app.model_inference_run`：记录一次 refinement 推理 run。

注意：
- 本模块不重训 LSTM，只在已训练好的 `LSTM_SHARED` 基础上做上层 refinement；
- 仅新增 `next_app.backend` 下的代码，不修改旧程序。
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import torch
from torch import nn
from torch.utils.data import DataLoader, Dataset

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
# 配置 & 数据集定义
# ---------------------------------------------------------------------------


@dataclass
class RefineTrainConfig:
    hidden_size: int = 32
    num_layers: int = 1
    batch_size: int = 128
    epochs: int = 10
    lr: float = 1e-3
    device: str = "cuda" if torch.cuda.is_available() else "cpu"
    val_ratio: float = 0.2


class RefineDataset(Dataset):
    """refinement 训练数据集.

    每个样本包含：
    - x_feat: (feat_dim,) = [y_shared, 静态特征向量 ...];
    - y_residual: 标量 residual = y_true - y_shared。
    """

    def __init__(self, x_feat: np.ndarray, y_residual: np.ndarray) -> None:
        assert x_feat.shape[0] == y_residual.shape[0]
        self.x = x_feat.astype(np.float32)
        self.y = y_residual.astype(np.float32)

    def __len__(self) -> int:  # noqa: D401
        return self.x.shape[0]

    def __getitem__(self, idx: int) -> Tuple[torch.Tensor, torch.Tensor]:  # noqa: D401
        return (
            torch.from_numpy(self.x[idx]),
            torch.tensor(self.y[idx], dtype=torch.float32),
        )


class RefinementMLP(nn.Module):
    """简单的两层 MLP，用于学习 residual."""

    def __init__(self, input_dim: int, hidden_size: int, num_layers: int = 1) -> None:
        super().__init__()
        layers: List[nn.Module] = []
        in_dim = input_dim
        for _ in range(max(1, num_layers)):
            layers.append(nn.Linear(in_dim, hidden_size))
            layers.append(nn.ReLU())
            in_dim = hidden_size
        layers.append(nn.Linear(in_dim, 1))
        self.net = nn.Sequential(*layers)

    def forward(self, x: torch.Tensor) -> torch.Tensor:  # noqa: D401
        return self.net(x).squeeze(-1)


# ---------------------------------------------------------------------------
# CLI & 工具函数
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Per-stock refinement based on shared LSTM")
    parser.add_argument(
        "--mode",
        type=str,
        choices=["train", "infer"],
        required=True,
        help="train: 训练 refinement MLP; infer: 对单只股票执行 refinement 推理",
    )

    # 通用 / 训练参数
    parser.add_argument(
        "--universe-name",
        type=str,
        default="ALL_EQ_CLEAN",
        help="Universe 名称，需与 LSTM_SHARED 训练时使用的一致",
    )
    parser.add_argument("--start", type=str, help="训练区间起始时间 (train 模式必填)")
    parser.add_argument("--end", type=str, help="训练区间结束时间 (train 模式必填)")
    parser.add_argument("--batch-size", type=int, default=128, help="refinement MLP 训练 batch 大小")
    parser.add_argument("--epochs", type=int, default=10, help="refinement MLP 训练 epoch 数")
    parser.add_argument("--hidden-size", type=int, default=32, help="refinement MLP 隐层宽度")
    parser.add_argument("--num-layers", type=int, default=1, help="refinement MLP 隐层层数")
    parser.add_argument("--lr", type=float, default=1e-3, help="学习率")
    parser.add_argument("--device", type=str, default=None, help="override device, e.g. cpu / cuda")

    # 推理参数
    parser.add_argument("--symbol", type=str, help="infer 模式下需要推理的 ts_code，如 SH600000")
    parser.add_argument(
        "--as-of",
        type=str,
        default=None,
        help="推理 as-of 时间 (infer 模式)，默认当前 UTC 时间",
    )
    parser.add_argument(
        "--lookback-minutes",
        type=int,
        default=None,
        help="推理回看窗口（分钟），默认按 shared 的 seq_len*5",
    )
    parser.add_argument(
        "--schedule-name",
        type=str,
        default=None,
        help="infer 模式下写入 model_inference_run 的 schedule_name",
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="额外 JSON 配置快照 (train/infer 模式均可选)",
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


# ---------------------------------------------------------------------------
# shared 模型加载与训练数据构建
# ---------------------------------------------------------------------------


def _model_path_for_universe(universe_name: str) -> Path:
    base_dir = Path(__file__).resolve().parents[3] / "models" / "lstm_shared"
    safe_universe = universe_name.replace(" ", "_")
    return base_dir / f"shared_{safe_universe}_5m.pt"


def _refine_model_path_for_universe(universe_name: str) -> Path:
    base_dir = Path(__file__).resolve().parents[3] / "models" / "lstm_refinement"
    base_dir.mkdir(parents=True, exist_ok=True)
    safe_universe = universe_name.replace(" ", "_")
    return base_dir / f"refine_{safe_universe}_5m.pt"


def _load_shared_model(
    universe_name: str,
    device: str,
) -> Tuple[SharedLSTMRegressor, Dict[str, Any], Dict[str, Any], Dict[str, int], str]:
    """加载已训练好的 shared LSTM 模型及其元信息（与 infer_shared 保持一致）."""

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


def _build_refine_training_dataset(
    symbols: Sequence[str],
    symbol2id: Mapping[str, int],
    start: dt.datetime,
    end: dt.datetime,
    seq_len: int,
    expected_feature_columns: Sequence[str],
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, int]:
    """为 refinement 训练构建 shared LSTM 级别的训练集.

    返回：
    - x_dyn_all: (N, seq_len, dyn_dim)
    - x_static_all: (N, static_dim)
    - sym_ids_all: (N,)
    - y_true_all: (N,)
    - total_rows: 原始 DataFrame 行数总和
    """

    if not symbols:
        raise ValueError("Universe symbols is empty for refinement training")

    cfg = LSTMDatasetConfig()

    # 加载静态特征（按 end.date）
    static_map, _static_meta = _load_static_features_for_symbols(symbols, as_of_date=end.date())

    xs_dyn: List[np.ndarray] = []
    xs_static: List[np.ndarray] = []
    ys_true: List[np.ndarray] = []
    sym_ids: List[np.ndarray] = []

    total_rows = 0

    for ts_code in symbols:
        if ts_code not in symbol2id:
            continue
        if ts_code not in static_map:
            continue

        df = load_lstm_timeseries_for_symbol(ts_code, start, end, cfg)
        if df.empty:
            continue
        total_rows += int(len(df))

        try:
            x_dyn, y, meta = _build_sequences(df, seq_len=seq_len)
        except Exception:
            # 数据点不足等情况，跳过该票
            continue

        if x_dyn.shape[0] == 0:
            continue

        feature_columns = meta.get("feature_columns") or []
        if expected_feature_columns and list(expected_feature_columns) != list(feature_columns):
            # 特征列不一致时，为避免 silent 错误，跳过该票
            continue

        s_vec = static_map[ts_code]
        n_samples = x_dyn.shape[0]
        x_static_sym = np.repeat(s_vec[None, :], n_samples, axis=0)

        xs_dyn.append(x_dyn)
        xs_static.append(x_static_sym)
        ys_true.append(y)
        sym_ids.append(np.full(shape=(n_samples,), fill_value=symbol2id[ts_code], dtype=np.int64))

    if not xs_dyn:
        raise ValueError("no effective samples for refinement training")

    x_dyn_all = np.concatenate(xs_dyn, axis=0)
    x_static_all = np.concatenate(xs_static, axis=0)
    y_true_all = np.concatenate(ys_true, axis=0)
    sym_ids_all = np.concatenate(sym_ids, axis=0)

    return x_dyn_all, x_static_all, sym_ids_all, y_true_all, total_rows


# ---------------------------------------------------------------------------
# refinement 训练 & 保存
# ---------------------------------------------------------------------------


def _train_refinement_mlp(
    x_feat: np.ndarray,
    y_residual: np.ndarray,
    cfg: RefineTrainConfig,
    device: str,
) -> Tuple[RefinementMLP, Dict[str, Any]]:
    ds = RefineDataset(x_feat, y_residual)
    n_samples = len(ds)

    val_size = max(1, int(n_samples * cfg.val_ratio))
    train_size = n_samples - val_size
    train_ds, val_ds = torch.utils.data.random_split(ds, [train_size, val_size])

    train_loader = DataLoader(train_ds, batch_size=cfg.batch_size, shuffle=True)
    val_loader = DataLoader(val_ds, batch_size=cfg.batch_size, shuffle=False)

    input_dim = x_feat.shape[1]
    model = RefinementMLP(input_dim=input_dim, hidden_size=cfg.hidden_size, num_layers=cfg.num_layers).to(device)

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

            batch_size = xb.size(0)
            train_loss += loss.item() * batch_size
            n_train += batch_size

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
                batch_size = xb.size(0)
                val_loss += loss.item() * batch_size
                n_val += batch_size

        val_loss /= max(1, n_val)

        print(f"[REFINE][EPOCH] {epoch}/{cfg.epochs} train_loss={train_loss:.6f} val_loss={val_loss:.6f}")

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


def _save_refinement_model(
    model: RefinementMLP,
    universe_name: str,
    refine_cfg: RefineTrainConfig,
    shared_model_path: str,
    shared_train_cfg: Mapping[str, Any],
    shared_dataset_meta: Mapping[str, Any],
) -> str:
    path = _refine_model_path_for_universe(universe_name)

    payload = {
        "state_dict": model.state_dict(),
        "refine_cfg": asdict(refine_cfg),
        "shared_model_path": shared_model_path,
        "shared_train_cfg": dict(shared_train_cfg),
        "shared_dataset_meta": dict(shared_dataset_meta),
    }
    torch.save(payload, path)
    return str(path)


def _record_train_run(
    model_name: str,
    universe_name: str,
    time_start: dt.datetime,
    time_end: dt.datetime,
    symbols_covered_count: int,
    config_snapshot: Dict[str, Any],
    metrics: Dict[str, Any],
    log_path: Optional[str],
) -> None:
    from next_app.backend.quant_models.lstm.train_shared import _now_utc  # avoid duplication

    start_ts = _now_utc()
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
                    symbols_covered_count,
                    time_start,
                    time_end,
                    "5m",
                    json.dumps(metrics, ensure_ascii=False),
                    log_path,
                ),
            )


# ---------------------------------------------------------------------------
# 推理：写入 quant_unified_signal / model_inference_run
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
    """将 refinement 推理结果写入/更新 app.quant_unified_signal."""

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


# ---------------------------------------------------------------------------
# main: 训练 / 推理入口
# ---------------------------------------------------------------------------


def _main_train(args: argparse.Namespace) -> None:
    if not args.start or not args.end:
        raise SystemExit("train 模式必须提供 --start 与 --end")

    universe_name = args.universe_name
    start = _to_dt(args.start)
    end = _to_dt(args.end)

    refine_cfg = RefineTrainConfig(
        hidden_size=args.hidden_size,
        num_layers=args.num_layers,
        batch_size=args.batch_size,
        epochs=args.epochs,
        lr=args.lr,
        device=args.device or ("cuda" if torch.cuda.is_available() else "cpu"),
    )
    extra_cfg = _load_extra_config(args.config)

    print(
        f"[INFO] training LSTM_REFINEMENT on universe={universe_name} "
        f"device={refine_cfg.device}"
    )

    # 1) 加载 shared 模型
    shared_model, shared_train_cfg, shared_dataset_meta, symbol2id, shared_model_path = _load_shared_model(
        universe_name, refine_cfg.device
    )

    seq_len = int(shared_train_cfg.get("seq_len", 60))
    feature_columns = shared_dataset_meta.get("feature_columns") or []

    symbols = sorted(symbol2id.keys())
    if not symbols:
        print(f"[WARN] shared model for universe={universe_name} has empty symbol2id; abort refinement")
        return

    # 2) 构建训练数据（多股票联合）
    try:
        x_dyn, x_static, sym_ids, y_true, total_rows = _build_refine_training_dataset(
            symbols=symbols,
            symbol2id=symbol2id,
            start=start,
            end=end,
            seq_len=seq_len,
            expected_feature_columns=feature_columns,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] failed to build refinement dataset: {exc}")
        metrics = {"rows": 0, "symbols_covered_count": 0, "error": str(exc)}
        _record_train_run(
            model_name="LSTM_REFINEMENT",
            universe_name=universe_name,
            time_start=start,
            time_end=end,
            symbols_covered_count=0,
            config_snapshot={
                "universe_name": universe_name,
                "shared_model_path": shared_model_path,
                "shared_train_cfg": shared_train_cfg,
                **asdict(refine_cfg),
                **extra_cfg,
            },
            metrics=metrics,
            log_path=None,
        )
        return

    print(
        f"[INFO] built refinement dataset: samples={x_dyn.shape[0]}, seq_len={x_dyn.shape[1]}, "
        f"dyn_dim={x_dyn.shape[2]}, static_dim={x_static.shape[1]}"
    )

    # 3) 计算 y_shared 与 residual
    with torch.no_grad():
        x_dyn_t = torch.from_numpy(x_dyn.astype(np.float32)).to(refine_cfg.device)
        x_static_t = torch.from_numpy(x_static.astype(np.float32)).to(refine_cfg.device)
        sym_ids_t = torch.from_numpy(sym_ids.astype(np.int64)).to(refine_cfg.device)
        y_shared = shared_model(x_dyn_t, x_static_t, sym_ids_t).cpu().numpy().reshape(-1)

    residual = y_true - y_shared

    # 4) 构建 refinement 特征：[y_shared, static_vec]
    x_feat = np.concatenate([y_shared[:, None], x_static], axis=1)

    # 5) 训练 refinement MLP
    refine_model, refine_metrics = _train_refinement_mlp(
        x_feat=x_feat,
        y_residual=residual,
        cfg=refine_cfg,
        device=refine_cfg.device,
    )

    # 6) 保存模型
    refine_model_path = _save_refinement_model(
        model=refine_model,
        universe_name=universe_name,
        refine_cfg=refine_cfg,
        shared_model_path=shared_model_path,
        shared_train_cfg=shared_train_cfg,
        shared_dataset_meta=shared_dataset_meta,
    )
    print(f"[INFO] saved refinement model to {refine_model_path}")

    metrics = {
        "rows": int(total_rows),
        "symbols_covered_count": int(len(symbols)),
        "refine_train_samples": int(refine_metrics["train_samples"]),
        "refine_val_samples": int(refine_metrics["val_samples"]),
        "refine_best_val_loss": float(refine_metrics["best_val_loss"]),
    }

    _record_train_run(
        model_name="LSTM_REFINEMENT",
        universe_name=universe_name,
        time_start=start,
        time_end=end,
        symbols_covered_count=len(symbols),
        config_snapshot={
            "universe_name": universe_name,
            "shared_model_path": shared_model_path,
            "shared_train_cfg": shared_train_cfg,
            **asdict(refine_cfg),
            **extra_cfg,
        },
        metrics=metrics,
        log_path=refine_model_path,
    )


def _main_infer(args: argparse.Namespace) -> None:
    if not args.symbol:
        raise SystemExit("infer 模式必须提供 --symbol")

    universe_name = args.universe_name
    ts_code = args.symbol
    as_of = _to_dt(args.as_of) if args.as_of else _now_utc()

    device = args.device or ("cuda" if torch.cuda.is_available() else "cpu")
    extra_cfg = _load_extra_config(args.config)

    print(
        f"[INFO] running LSTM_REFINEMENT inference for symbol={ts_code} "
        f"universe={universe_name} device={device}"
    )

    # 1) 加载 refinement 模型
    refine_path = _refine_model_path_for_universe(universe_name)
    if not refine_path.exists():
        print(f"[ERROR] refinement model not found for universe={universe_name}: {refine_path}")
        return

    refine_payload = torch.load(refine_path, map_location=device)
    state_dict_refine = refine_payload["state_dict"]
    refine_cfg_dict: Dict[str, Any] = refine_payload.get("refine_cfg", {})
    shared_model_path: str = refine_payload.get("shared_model_path")
    shared_train_cfg: Dict[str, Any] = refine_payload.get("shared_train_cfg", {})
    shared_dataset_meta: Dict[str, Any] = refine_payload.get("shared_dataset_meta", {})

    refine_cfg = RefineTrainConfig(
        hidden_size=int(refine_cfg_dict.get("hidden_size", 32)),
        num_layers=int(refine_cfg_dict.get("num_layers", 1)),
        batch_size=int(refine_cfg_dict.get("batch_size", 128)),
        epochs=int(refine_cfg_dict.get("epochs", 10)),
        lr=float(refine_cfg_dict.get("lr", 1e-3)),
        device=device,
        val_ratio=float(refine_cfg_dict.get("val_ratio", 0.2)),
    )

    # 重建 refinement MLP 结构
    static_columns = shared_dataset_meta.get("static_columns") or []
    static_dim = int(len(static_columns))
    input_dim = 1 + static_dim  # [y_shared] + static_vec
    refine_model = RefinementMLP(input_dim=input_dim, hidden_size=refine_cfg.hidden_size, num_layers=refine_cfg.num_layers).to(device)
    refine_model.load_state_dict(state_dict_refine)
    refine_model.eval()

    # 2) 加载 shared 模型（从 shared_model_path 重建）
    if not shared_model_path:
        print("[ERROR] shared_model_path missing in refinement payload")
        return

    shared_payload = torch.load(shared_model_path, map_location=device)
    shared_state_dict = shared_payload["state_dict"]
    shared_symbol2id: Dict[str, int] = shared_payload.get("symbol2id", {})

    feature_columns = shared_dataset_meta.get("feature_columns") or []
    dyn_dim = int(len(feature_columns))
    static_dim = int(len(static_columns))

    hidden_size_shared = int(shared_train_cfg.get("hidden_size", 64))
    num_layers_shared = int(shared_train_cfg.get("num_layers", 1))
    symbol_emb_dim = int(shared_train_cfg.get("symbol_emb_dim", 16))
    num_symbols = max(shared_symbol2id.values()) + 1 if shared_symbol2id else 0

    shared_model = SharedLSTMRegressor(
        dyn_input_size=dyn_dim,
        static_input_size=static_dim,
        num_symbols=num_symbols,
        symbol_emb_dim=symbol_emb_dim,
        hidden_size=hidden_size_shared,
        num_layers=num_layers_shared,
    ).to(device)
    shared_model.load_state_dict(shared_state_dict)
    shared_model.eval()

    if ts_code not in shared_symbol2id:
        print(f"[WARN] symbol={ts_code} not in shared model universe, skip refinement")
        return

    seq_len = int(shared_train_cfg.get("seq_len", 60))
    lookback_minutes = args.lookback_minutes or seq_len * 5

    # 3) 构建该股票当下的 shared 输入（单样本）
    cfg_ds = LSTMDatasetConfig()
    start_time = as_of - dt.timedelta(minutes=lookback_minutes)
    df = load_lstm_timeseries_for_symbol(ts_code, start_time, as_of, cfg_ds)
    if df.empty:
        print(f"[WARN] no data for {ts_code} in range {start_time} ~ {as_of}")
        metrics = {"rows": 0, "note": "no data"}
        config_snapshot = {
            "universe_name": universe_name,
            "symbol": ts_code,
            "as_of": as_of.isoformat(),
            "lookback_minutes": lookback_minutes,
            "refine_model_path": str(refine_path),
            "shared_model_path": shared_model_path,
            **shared_train_cfg,
            **asdict(refine_cfg),
            **extra_cfg,
        }
        _record_inference_run(
            model_name="LSTM_REFINEMENT",
            schedule_name=args.schedule_name,
            universe_name=universe_name,
            symbol=ts_code,
            as_of_time=as_of,
            config_snapshot=config_snapshot,
            metrics=metrics,
            status="SUCCESS",
        )
        return

    num_df = df.select_dtypes(include=["number"]).copy()
    missing_cols = [c for c in feature_columns if c not in num_df.columns]
    if missing_cols:
        msg = f"missing feature columns for refinement inference: {missing_cols}"
        print(f"[ERROR] {msg}")
        metrics = {"rows": int(len(df)), "error": msg}
        config_snapshot = {
            "universe_name": universe_name,
            "symbol": ts_code,
            "as_of": as_of.isoformat(),
            "lookback_minutes": lookback_minutes,
            "refine_model_path": str(refine_path),
            "shared_model_path": shared_model_path,
            **shared_train_cfg,
            **asdict(refine_cfg),
            **extra_cfg,
        }
        _record_inference_run(
            model_name="LSTM_REFINEMENT",
            schedule_name=args.schedule_name,
            universe_name=universe_name,
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
            f"[WARN] not enough rows for refinement inference: got {values.shape[0]} < seq_len={seq_len}",
        )
        metrics = {"rows": int(values.shape[0]), "note": "not enough rows"}
        config_snapshot = {
            "universe_name": universe_name,
            "symbol": ts_code,
            "as_of": as_of.isoformat(),
            "lookback_minutes": lookback_minutes,
            "refine_model_path": str(refine_path),
            "shared_model_path": shared_model_path,
            **shared_train_cfg,
            **asdict(refine_cfg),
            **extra_cfg,
        }
        _record_inference_run(
            model_name="LSTM_REFINEMENT",
            schedule_name=args.schedule_name,
            universe_name=universe_name,
            symbol=ts_code,
            as_of_time=as_of,
            config_snapshot=config_snapshot,
            metrics=metrics,
            status="SUCCESS",
        )
        return

    # 使用与训练 shared 一致的 `_build_sequences` 方式构建序列，并取最后一个样本
    try:
        x_dyn_all, _y_dummy, meta = _build_sequences(df, seq_len=seq_len)
    except Exception as exc:  # noqa: BLE001
        msg = f"failed to build sequences for refinement inference: {exc}"
        print(f"[ERROR] {msg}")
        metrics = {"rows": int(len(df)), "error": msg}
        config_snapshot = {
            "universe_name": universe_name,
            "symbol": ts_code,
            "as_of": as_of.isoformat(),
            "lookback_minutes": lookback_minutes,
            "refine_model_path": str(refine_path),
            "shared_model_path": shared_model_path,
            **shared_train_cfg,
            **asdict(refine_cfg),
            **extra_cfg,
        }
        _record_inference_run(
            model_name="LSTM_REFINEMENT",
            schedule_name=args.schedule_name,
            universe_name=universe_name,
            symbol=ts_code,
            as_of_time=as_of,
            config_snapshot=config_snapshot,
            metrics=metrics,
            status="ERROR",
        )
        return

    if x_dyn_all.shape[0] == 0:
        print("[WARN] no effective sequences for refinement inference")
        metrics = {"rows": int(len(df)), "note": "no effective sequences"}
        config_snapshot = {
            "universe_name": universe_name,
            "symbol": ts_code,
            "as_of": as_of.isoformat(),
            "lookback_minutes": lookback_minutes,
            "refine_model_path": str(refine_path),
            "shared_model_path": shared_model_path,
            **shared_train_cfg,
            **asdict(refine_cfg),
            **extra_cfg,
        }
        _record_inference_run(
            model_name="LSTM_REFINEMENT",
            schedule_name=args.schedule_name,
            universe_name=universe_name,
            symbol=ts_code,
            as_of_time=as_of,
            config_snapshot=config_snapshot,
            metrics=metrics,
            status="SUCCESS",
        )
        return

    # 静态特征
    static_map, _static_meta = _load_static_features_for_symbols([ts_code], as_of_date=as_of.date())
    if ts_code not in static_map:
        print(f"[WARN] no static features for {ts_code}, skip refinement")
        return

    s_vec = static_map[ts_code]
    x_dyn_last = x_dyn_all[-1]  # (seq_len, dyn_dim)

    # shared 前向
    with torch.no_grad():
        x_dyn_t = torch.from_numpy(x_dyn_last.astype(np.float32)[None, ...]).to(device)
        x_static_t = torch.from_numpy(s_vec.astype(np.float32)[None, ...]).to(device)
        sym_id_t = torch.tensor([shared_symbol2id[ts_code]], dtype=torch.long, device=device)
        y_shared = shared_model(x_dyn_t, x_static_t, sym_id_t).cpu().numpy().reshape(-1)[0]

    # refinement 前向：构造特征 [y_shared, static_vec]
    x_feat = np.concatenate([[y_shared], s_vec.astype(np.float32)], axis=0)[None, :]

    with torch.no_grad():
        x_feat_t = torch.from_numpy(x_feat.astype(np.float32)).to(device)
        residual_pred = refine_model(x_feat_t).cpu().numpy().reshape(-1)[0]

    y_final = float(y_shared + residual_pred)

    # 写入 quant_unified_signal
    if y_final > 0:
        direction = "UP"
    elif y_final < 0:
        direction = "DOWN"
    else:
        direction = "FLAT"

    model_votes = {
        "LSTM_SHARED": {
            "pred_log_return": float(y_shared),
        },
        "LSTM_REFINEMENT": {
            "pred_log_return": float(y_final),
            "residual_pred": float(residual_pred),
        },
    }
    model_versions = {
        "LSTM_SHARED": {
            "model_path": shared_model_path,
            "universe_name": universe_name,
        },
        "LSTM_REFINEMENT": {
            "model_path": str(refine_path),
            "universe_name": universe_name,
        },
    }

    _upsert_quant_unified_signal(
        symbol=ts_code,
        as_of_time=as_of,
        frequency="5m",
        horizon="5m",
        direction=direction,
        expected_return=y_final,
        model_votes=model_votes,
        model_versions=model_versions,
    )

    metrics = {
        "rows": int(len(df)),
        "seq_len": seq_len,
        "y_shared": float(y_shared),
        "y_final": float(y_final),
        "residual_pred": float(residual_pred),
    }
    config_snapshot = {
        "universe_name": universe_name,
        "symbol": ts_code,
        "as_of": as_of.isoformat(),
        "lookback_minutes": lookback_minutes,
        "refine_model_path": str(refine_path),
        "shared_model_path": shared_model_path,
        **shared_train_cfg,
        **asdict(refine_cfg),
        **extra_cfg,
    }

    _record_inference_run(
        model_name="LSTM_REFINEMENT",
        schedule_name=args.schedule_name,
        universe_name=universe_name,
        symbol=ts_code,
        as_of_time=as_of,
        config_snapshot=config_snapshot,
        metrics=metrics,
        status="SUCCESS",
    )

    print(
        f"[INFO] refinement inference done for {ts_code} as_of={as_of}, "
        f"y_shared={y_shared:.6f}, y_final={y_final:.6f}"
    )


def main() -> None:
    args = _parse_args()
    if args.mode == "train":
        _main_train(args)
    else:
        _main_infer(args)


if __name__ == "__main__":
    main()
