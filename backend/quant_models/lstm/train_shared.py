"""Universe 级 shared LSTM 训练入口（新程序目录）。

设计要点（与 `docs/quant_model_evaluation.md` 第 6 章一致）：
- Universe：由 app.model_universe_config 控制，默认使用“全市场合格 Universe”；
- 输入：
  - 5 分钟 K 线 + 高频聚合特征（通过 `quant_datasets.lstm_dataset` 加载）；
  - symbol embedding（ts_code → symbol_id → nn.Embedding）；
  - 静态特征（行业、市值、波动水平等，来自 app.stock_static_features，经数值编码后作为静态协变量）；
- 输出：
  - 通用预测 `y_shared`（下一根 5m 对数收益），模型名为 `LSTM_SHARED`；
  - 训练 run 信息写入 app.model_train_run。

注意：
- 仅在 next_app.backend 下新增代码，不修改任何旧程序文件；
- 静态特征与 Universe 配置均通过新建表管理，由本脚本读取，不直接改动旧 schema 脚本。
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import torch
from torch import nn
from torch.utils.data import DataLoader, Dataset

from next_app.backend.db.pg_pool import get_conn
from next_app.backend.quant_datasets.lstm_dataset import (
    LSTMDatasetConfig,
    get_core_universe,
    load_lstm_timeseries_for_symbol,
)
from .train_per_stock import _build_sequences


# ---------------------------------------------------------------------------
# 配置 & 数据集定义
# ---------------------------------------------------------------------------


@dataclass
class SharedTrainConfig:
    seq_len: int = 60
    hidden_size: int = 64
    num_layers: int = 1
    symbol_emb_dim: int = 16
    batch_size: int = 64
    epochs: int = 10
    lr: float = 1e-3
    device: str = "cuda" if torch.cuda.is_available() else "cpu"
    val_ratio: float = 0.2


class SharedLSTMDataset(Dataset):
    """多股票联合的 LSTM 数据集.

    每个样本包含：
    - x_dyn: (seq_len, dyn_dim) 归一化后的动态特征;
    - x_static: (static_dim,) 归一化后的静态特征（行业/市值/波动等编码）;
    - symbol_id: int，对应 embedding 输入;
    - y: 标量目标（下一步 log return）。
    """

    def __init__(
        self,
        x_dyn: np.ndarray,
        x_static: np.ndarray,
        symbol_ids: np.ndarray,
        y: np.ndarray,
    ) -> None:
        assert x_dyn.shape[0] == x_static.shape[0] == y.shape[0] == symbol_ids.shape[0]
        self.x_dyn = x_dyn.astype(np.float32)
        self.x_static = x_static.astype(np.float32)
        self.symbol_ids = symbol_ids.astype(np.int64)
        self.y = y.astype(np.float32)

    def __len__(self) -> int:  # noqa: D401
        return self.x_dyn.shape[0]

    def __getitem__(
        self, idx: int
    ) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:  # noqa: D401
        return (
            torch.from_numpy(self.x_dyn[idx]),
            torch.from_numpy(self.x_static[idx]),
            torch.tensor(self.symbol_ids[idx], dtype=torch.long),
            torch.from_numpy(np.array(self.y[idx: idx + 1])).squeeze(0),
        )


class SharedLSTMRegressor(nn.Module):
    """带 symbol embedding + 静态特征的 shared LSTM 回归器."""

    def __init__(
        self,
        dyn_input_size: int,
        static_input_size: int,
        num_symbols: int,
        symbol_emb_dim: int,
        hidden_size: int,
        num_layers: int = 1,
    ) -> None:
        super().__init__()
        self.symbol_emb = nn.Embedding(num_symbols, symbol_emb_dim)
        lstm_input_size = dyn_input_size + static_input_size + symbol_emb_dim
        self.lstm = nn.LSTM(
            input_size=lstm_input_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
        )
        self.head = nn.Linear(hidden_size, 1)

    def forward(
        self,
        x_dyn: torch.Tensor,
        x_static: torch.Tensor,
        symbol_ids: torch.Tensor,
    ) -> torch.Tensor:  # noqa: D401
        # x_dyn: (B, T, dyn_dim)
        # x_static: (B, static_dim)
        # symbol_ids: (B,)
        emb = self.symbol_emb(symbol_ids)  # (B, emb_dim)
        emb_exp = emb.unsqueeze(1).expand(-1, x_dyn.size(1), -1)  # (B, T, emb_dim)
        static_exp = x_static.unsqueeze(1).expand(-1, x_dyn.size(1), -1)  # (B, T, static_dim)
        x = torch.cat([x_dyn, static_exp, emb_exp], dim=-1)
        out, _ = self.lstm(x)
        last = out[:, -1, :]
        return self.head(last).squeeze(-1)


# ---------------------------------------------------------------------------
# CLI & 工具函数
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Shared LSTM training (Universe-level)")
    parser.add_argument(
        "--universe-name",
        type=str,
        default="ALL_EQ_CLEAN",
        help="Universe 名称，对应 app.model_universe_config.universe_name",
    )
    parser.add_argument("--start", required=True, help="start datetime, e.g. 2020-01-01T09:30:00")
    parser.add_argument("--end", required=True, help="end datetime, e.g. 2024-01-01T15:00:00")
    parser.add_argument("--seq-len", type=int, default=60, help="sequence length (number of 5m bars)")
    parser.add_argument("--hidden-size", type=int, default=64, help="LSTM hidden size")
    parser.add_argument("--num-layers", type=int, default=1, help="number of LSTM layers")
    parser.add_argument("--symbol-emb-dim", type=int, default=16, help="symbol embedding dim")
    parser.add_argument("--batch-size", type=int, default=64, help="mini-batch size")
    parser.add_argument("--epochs", type=int, default=10, help="training epochs")
    parser.add_argument("--lr", type=float, default=1e-3, help="learning rate")
    parser.add_argument("--device", type=str, default=None, help="override device, e.g. cpu / cuda")
    parser.add_argument("--config", type=str, default=None, help="extra JSON config snapshot (optional)")
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
# Universe & 静态特征加载
# ---------------------------------------------------------------------------


def _load_universe_config(universe_name: str) -> Dict[str, Any]:
    """从 app.model_universe_config 读取 Universe 配置.

    若未找到对应名称，则返回一个默认配置（全市场合格 Universe）。
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT config_json
                  FROM app.model_universe_config
                 WHERE universe_name = %s AND enabled = TRUE
                 LIMIT 1
                """,
                (universe_name,),
            )
            row = cur.fetchone()
    if row is None:
        # 默认配置：以全量 5m K 线中出现过的 ts_code 作为 Universe
        return {"source": "ALL_EQ_CLEAN"}
    return row[0]  # type: ignore[no-any-return]


def _load_universe_symbols(
    universe_name: str,
    cfg: Mapping[str, Any],
    start: dt.datetime,
    end: dt.datetime,
) -> List[str]:
    """根据配置加载 Universe 的 ts_code 列表.

    当前支持几种模式（可通过 config_json.source 指定）：
    - "ALL_EQ_CLEAN"（默认）：
      - 从 market.kline_5m 中提取在给定时间范围内出现过的全部 ts_code；
      - 预留垃圾股过滤钩子（可通过 config_json 追加规则）。
    - "CORE_UNIVERSE":
      - 通过 app.watchlist_* 获取 CoreUniverse，可选按分类过滤。
    """

    source = cfg.get("source", "ALL_EQ_CLEAN")

    if source == "CORE_UNIVERSE":
        categories = cfg.get("categories") or None
        if categories is not None and not isinstance(categories, Sequence):
            categories = [str(categories)]
        symbols = get_core_universe(categories=categories)
        return sorted(set(symbols))

    # 默认：从 5m K 线中提取 ts_code
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ts_code
                  FROM market.kline_5m
                 WHERE bucket >= %s
                   AND bucket < %s
                 ORDER BY ts_code
                """,
                (start, end),
            )
            rows = cur.fetchall()
    symbols = [r[0] for r in rows]

    # 预留：根据 cfg 中的规则进一步过滤（如市值/流动性/黑名单等），暂不具体实现。
    # cfg 可包含：min_avg_turnover、exclude_prefixes、include_exchanges 等键。

    return symbols


def _load_static_features_for_symbols(
    symbols: Sequence[str],
    as_of_date: dt.date,
) -> Tuple[Dict[str, np.ndarray], Dict[str, Any]]:
    """为给定股票集合加载最新静态特征并进行数值化+归一化.

    返回：
    - symbol2vec: {ts_code: 静态特征向量(float32)}；
    - meta: {static_columns, static_mean, static_std} 便于后续推理使用。
    """

    if not symbols:
        return {}, {"static_columns": [], "static_mean": [], "static_std": []}

    with get_conn() as conn:
        sql = """
            SELECT
              ts_code,
              as_of_date,
              industry,
              sub_industry,
              size_bucket,
              volatility_bucket,
              liquidity_bucket,
              extra_json
            FROM app.stock_static_features
            WHERE ts_code = ANY(%s)
              AND as_of_date <= %s
            ORDER BY ts_code, as_of_date DESC
        """
        df = pd.read_sql(sql, conn, params=(list(symbols), as_of_date))

    if df.empty:
        return {}, {"static_columns": [], "static_mean": [], "static_std": []}

    # 每只股票取最新一条记录
    df = df.sort_values(["ts_code", "as_of_date"]).drop_duplicates("ts_code", keep="last")
    df = df.set_index("ts_code")

    # 选取需要编码的静态字段
    static_cols = [
        "industry",
        "sub_industry",
        "size_bucket",
        "volatility_bucket",
        "liquidity_bucket",
    ]
    feat_df = df[static_cols].copy()

    # 将类别字段编码为整数，再标准化
    num_df = pd.DataFrame(index=feat_df.index)
    for col in static_cols:
        cat = feat_df[col].astype("category")
        codes = cat.cat.codes.astype(float)  # -1 代表缺失
        num_df[col] = codes

    mean = num_df.mean(axis=0)
    std = num_df.std(axis=0) + 1e-8
    norm_df = (num_df - mean) / std

    symbol2vec: Dict[str, np.ndarray] = {}
    for ts_code, row in norm_df.iterrows():
        symbol2vec[ts_code] = row.values.astype(np.float32)

    meta = {
        "static_columns": static_cols,
        "static_mean": mean.tolist(),
        "static_std": std.tolist(),
    }
    return symbol2vec, meta


# ---------------------------------------------------------------------------
# 构建 shared 训练数据集
# ---------------------------------------------------------------------------


def _build_shared_dataset(
    symbols: Sequence[str],
    start: dt.datetime,
    end: dt.datetime,
    seq_len: int,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, Dict[str, Any], int, Dict[str, int]]:
    """为多个股票构建 shared LSTM 训练集.

    使用与 per-stock 相同的 `_build_sequences` 逻辑（每只股票内做标准化），
    再附加静态特征和 symbol_id。

    返回：
    - x_dyn: (N, seq_len, dyn_dim)
    - x_static: (N, static_dim)
    - symbol_ids: (N,)
    - y: (N,)
    - dataset_meta: 包含动态特征列、静态特征 meta 等；
    - total_rows: 原始 DataFrame 行数总和（用于记录 metrics）；
    - symbol2id: ts_code → int 映射。
    """

    cfg = LSTMDatasetConfig()

    # 先加载静态特征
    symbol_list = list(symbols)
    if not symbol_list:
        raise ValueError("Universe symbols is empty")

    static_map, static_meta = _load_static_features_for_symbols(symbol_list, as_of_date=end.date())

    symbol2id: Dict[str, int] = {}
    for i, s in enumerate(symbol_list):
        symbol2id[s] = i

    xs_dyn: List[np.ndarray] = []
    xs_static: List[np.ndarray] = []
    ys: List[np.ndarray] = []
    sym_ids: List[np.ndarray] = []

    feature_columns: List[str] | None = None
    total_rows = 0

    for ts_code in symbol_list:
        if ts_code not in static_map:
            # 若无静态特征，暂时跳过该股票
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

        if feature_columns is None:
            feature_columns = list(meta.get("feature_columns", []))

        # 静态特征向量
        s_vec = static_map[ts_code]
        n_samples = x_dyn.shape[0]
        x_static_sym = np.repeat(s_vec[None, :], n_samples, axis=0)

        xs_dyn.append(x_dyn)
        xs_static.append(x_static_sym)
        ys.append(y)
        sym_ids.append(np.full(shape=(n_samples,), fill_value=symbol2id[ts_code], dtype=np.int64))

    if not xs_dyn:
        raise ValueError("no effective samples for shared LSTM training")

    x_dyn_all = np.concatenate(xs_dyn, axis=0)
    x_static_all = np.concatenate(xs_static, axis=0)
    y_all = np.concatenate(ys, axis=0)
    sym_ids_all = np.concatenate(sym_ids, axis=0)

    dataset_meta = {
        "feature_columns": feature_columns or [],
        **static_meta,
    }

    return x_dyn_all, x_static_all, sym_ids_all, y_all, dataset_meta, total_rows, symbol2id


# ---------------------------------------------------------------------------
# 训练与模型保存 / 记录
# ---------------------------------------------------------------------------


def _train_shared_lstm(
    x_dyn: np.ndarray,
    x_static: np.ndarray,
    symbol_ids: np.ndarray,
    y: np.ndarray,
    cfg: SharedTrainConfig,
    device: str,
    num_symbols: int,
) -> Tuple[SharedLSTMRegressor, Dict[str, Any]]:
    n_samples, seq_len, dyn_dim = x_dyn.shape
    static_dim = x_static.shape[1]

    ds = SharedLSTMDataset(x_dyn, x_static, symbol_ids, y)

    val_size = max(1, int(n_samples * cfg.val_ratio))
    train_size = n_samples - val_size
    train_ds, val_ds = torch.utils.data.random_split(ds, [train_size, val_size])

    train_loader = DataLoader(train_ds, batch_size=cfg.batch_size, shuffle=True)
    val_loader = DataLoader(val_ds, batch_size=cfg.batch_size, shuffle=False)

    model = SharedLSTMRegressor(
        dyn_input_size=dyn_dim,
        static_input_size=static_dim,
        num_symbols=num_symbols,
        symbol_emb_dim=cfg.symbol_emb_dim,
        hidden_size=cfg.hidden_size,
        num_layers=cfg.num_layers,
    ).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=cfg.lr)
    criterion = nn.MSELoss()

    best_val_loss = float("inf")
    best_state: Dict[str, Any] | None = None

    for epoch in range(1, cfg.epochs + 1):
        model.train()
        train_loss = 0.0
        n_train = 0
        for x_dyn_b, x_static_b, sym_id_b, y_b in train_loader:
            x_dyn_b = x_dyn_b.to(device)
            x_static_b = x_static_b.to(device)
            sym_id_b = sym_id_b.to(device)
            y_b = y_b.to(device)

            optimizer.zero_grad()
            pred = model(x_dyn_b, x_static_b, sym_id_b)
            loss = criterion(pred, y_b)
            loss.backward()
            optimizer.step()

            batch_size = x_dyn_b.size(0)
            train_loss += loss.item() * batch_size
            n_train += batch_size

        train_loss /= max(1, n_train)

        model.eval()
        val_loss = 0.0
        n_val = 0
        with torch.no_grad():
            for x_dyn_b, x_static_b, sym_id_b, y_b in val_loader:
                x_dyn_b = x_dyn_b.to(device)
                x_static_b = x_static_b.to(device)
                sym_id_b = sym_id_b.to(device)
                y_b = y_b.to(device)

                pred = model(x_dyn_b, x_static_b, sym_id_b)
                loss = criterion(pred, y_b)

                batch_size = x_dyn_b.size(0)
                val_loss += loss.item() * batch_size
                n_val += batch_size

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


def _save_model(
    model: SharedLSTMRegressor,
    universe_name: str,
    train_cfg: SharedTrainConfig,
    dataset_meta: Dict[str, Any],
    symbol2id: Mapping[str, int],
) -> str:
    base_dir = Path(__file__).resolve().parents[3] / "models" / "lstm_shared"
    base_dir.mkdir(parents=True, exist_ok=True)
    safe_universe = universe_name.replace(" ", "_")
    path = base_dir / f"shared_{safe_universe}_5m.pt"

    payload = {
        "state_dict": model.state_dict(),
        "train_cfg": asdict(train_cfg),
        "dataset_meta": dataset_meta,
        "symbol2id": dict(symbol2id),
        "universe_name": universe_name,
    }
    torch.save(payload, path)
    return str(path)


def _record_train_run(
    model_name: str,
    universe_name: str,
    time_start: dt.datetime,
    time_end: dt.datetime,
    data_granularity: str,
    symbols_covered_count: int,
    config_snapshot: Dict[str, Any],
    metrics: Dict[str, Any],
    log_path: Optional[str],
) -> None:
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
                    data_granularity,
                    json.dumps(metrics, ensure_ascii=False),
                    log_path,
                ),
            )


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def main() -> None:
    args = _parse_args()

    universe_name = args.universe_name
    start = _to_dt(args.start)
    end = _to_dt(args.end)

    train_cfg = SharedTrainConfig(
        seq_len=args.seq_len,
        hidden_size=args.hidden_size,
        num_layers=args.num_layers,
        symbol_emb_dim=args.symbol_emb_dim,
        batch_size=args.batch_size,
        epochs=args.epochs,
        lr=args.lr,
        device=args.device or ("cuda" if torch.cuda.is_available() else "cpu"),
    )
    extra_cfg = _load_extra_config(args.config)

    print(f"[INFO] training LSTM_SHARED on universe={universe_name} device={train_cfg.device}")

    # Universe 配置 & 股票列表
    uni_cfg = _load_universe_config(universe_name)
    symbols = _load_universe_symbols(universe_name, uni_cfg, start, end)
    if not symbols:
        print(f"[WARN] no symbols for universe={universe_name}")
        metrics = {"rows": 0, "symbols_covered_count": 0}
        _record_train_run(
            model_name="LSTM_SHARED",
            universe_name=universe_name,
            time_start=start,
            time_end=end,
            data_granularity="5m",
            symbols_covered_count=0,
            config_snapshot={
                "universe_name": universe_name,
                "universe_config": uni_cfg,
                **asdict(train_cfg),
                **extra_cfg,
            },
            metrics=metrics,
            log_path=None,
        )
        return

    print(f"[INFO] universe={universe_name} symbols={len(symbols)}")

    try:
        x_dyn, x_static, sym_ids, y, dataset_meta, total_rows, symbol2id = _build_shared_dataset(
            symbols=symbols,
            start=start,
            end=end,
            seq_len=train_cfg.seq_len,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] failed to build shared dataset: {exc}")
        metrics = {"rows": 0, "symbols_covered_count": 0, "error": str(exc)}
        _record_train_run(
            model_name="LSTM_SHARED",
            universe_name=universe_name,
            time_start=start,
            time_end=end,
            data_granularity="5m",
            symbols_covered_count=0,
            config_snapshot={
                "universe_name": universe_name,
                "universe_config": uni_cfg,
                **asdict(train_cfg),
                **extra_cfg,
            },
            metrics=metrics,
            log_path=None,
        )
        return

    print(
        f"[INFO] built shared dataset: samples={x_dyn.shape[0]}, seq_len={x_dyn.shape[1]}, "
        f"dyn_dim={x_dyn.shape[2]}, static_dim={x_static.shape[1]}",
    )

    model, train_metrics = _train_shared_lstm(
        x_dyn,
        x_static,
        sym_ids,
        y,
        cfg=train_cfg,
        device=train_cfg.device,
        num_symbols=len(symbol2id),
    )

    model_path = _save_model(
        model=model,
        universe_name=universe_name,
        train_cfg=train_cfg,
        dataset_meta=dataset_meta,
        symbol2id=symbol2id,
    )
    print(f"[INFO] saved shared model to {model_path}")

    metrics = {
        "rows": int(total_rows),
        "symbols_covered_count": int(len(symbol2id)),
        **train_metrics,
    }
    _record_train_run(
        model_name="LSTM_SHARED",
        universe_name=universe_name,
        time_start=start,
        time_end=end,
        data_granularity="5m",
        symbols_covered_count=len(symbol2id),
        config_snapshot={
            "universe_name": universe_name,
            "universe_config": uni_cfg,
            **asdict(train_cfg),
            **extra_cfg,
        },
        metrics=metrics,
        log_path=model_path,
    )


if __name__ == "__main__":
    main()
