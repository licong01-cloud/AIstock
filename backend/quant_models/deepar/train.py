"""Universe-level DeepAR-style RNN training script (new program only).

Design goals (aligned with docs/quant_model_evaluation.md §3.5 & §5.2.5):
- Train a multi-stock RNN with Gaussian output (mean + volatility) at daily / 60m frequency.
- Reuse existing Universe config and static features tables:
  - app.model_universe_config
  - app.stock_static_features
- Reuse high-frequency aggregated factors (app.ts_lstm_trade_agg) via deepar_dataset helpers.
- Record all training runs in app.model_train_run (model_name = DEEPAR_DAILY / DEEPAR_60M).

This script lives under next_app/backend and does not touch legacy programs.
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
from next_app.backend.quant_datasets.deepar_dataset import (
    DeepARDatasetConfig,
    load_deepar_daily_for_symbol,
    load_deepar_60m_for_symbol,
)
from next_app.backend.quant_models.lstm.train_per_stock import _build_sequences
from next_app.backend.quant_models.lstm.train_shared import (
    _load_static_features_for_symbols,
    _load_universe_config,
    _load_universe_symbols,
)


# ---------------------------------------------------------------------------
# Config & dataset
# ---------------------------------------------------------------------------


@dataclass
class DeepARTrainConfig:
    seq_len: int = 60
    hidden_size: int = 64
    num_layers: int = 1
    symbol_emb_dim: int = 16
    batch_size: int = 64
    epochs: int = 10
    lr: float = 1e-3
    device: str = "cuda" if torch.cuda.is_available() else "cpu"
    val_ratio: float = 0.2


class DeepARDataset(Dataset):
    """Multi-stock DeepAR-style dataset.

    Each sample contains:
    - x_dyn: (seq_len, dyn_dim) normalized dynamic features
    - x_static: (static_dim,) normalized static features
    - symbol_id: int (for embedding)
    - y: scalar target (next-step log return)
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
            torch.from_numpy(np.array(self.y[idx : idx + 1])).squeeze(0),
        )


# ---------------------------------------------------------------------------
# Model definition (Gaussian output: mean + volatility)
# ---------------------------------------------------------------------------


class DeepARRegressor(nn.Module):
    """DeepAR-style RNN with symbol embedding + static features and Gaussian output."""

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
        self.proj_mu = nn.Linear(hidden_size, 1)
        self.proj_log_sigma = nn.Linear(hidden_size, 1)

    def forward(
        self,
        x_dyn: torch.Tensor,
        x_static: torch.Tensor,
        symbol_ids: torch.Tensor,
    ) -> Tuple[torch.Tensor, torch.Tensor]:  # noqa: D401
        # x_dyn: (B, T, dyn_dim)
        # x_static: (B, static_dim)
        # symbol_ids: (B,)
        emb = self.symbol_emb(symbol_ids)  # (B, emb_dim)
        emb_exp = emb.unsqueeze(1).expand(-1, x_dyn.size(1), -1)  # (B, T, emb_dim)
        static_exp = x_static.unsqueeze(1).expand(-1, x_dyn.size(1), -1)  # (B, T, static_dim)
        x = torch.cat([x_dyn, static_exp, emb_exp], dim=-1)
        out, _ = self.lstm(x)
        last = out[:, -1, :]
        mu = self.proj_mu(last).squeeze(-1)
        log_sigma = self.proj_log_sigma(last).squeeze(-1)
        sigma = torch.nn.functional.softplus(log_sigma) + 1e-6
        return mu, sigma


# ---------------------------------------------------------------------------
# CLI & helpers
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DeepAR-style RNN training (Universe-level)")
    parser.add_argument(
        "--universe-name",
        type=str,
        default="ALL_EQ_CLEAN",
        help="Universe name, see app.model_universe_config.universe_name",
    )
    parser.add_argument(
        "--freq",
        type=str,
        choices=["1d", "60m"],
        default="1d",
        help="target frequency: 1d (daily) or 60m",
    )
    parser.add_argument("--start", required=True, help="start datetime, e.g. 2018-01-01T00:00:00")
    parser.add_argument("--end", required=True, help="end datetime, e.g. 2024-01-01T00:00:00")
    parser.add_argument("--seq-len", type=int, default=60, help="conditioning window length (steps)")
    parser.add_argument("--hidden-size", type=int, default=64, help="RNN hidden size")
    parser.add_argument("--num-layers", type=int, default=1, help="number of RNN layers")
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
    except Exception:  # noqa: BLE001
        return {"raw": config_str}


def _model_name_for_freq(freq: str) -> str:
    if freq == "60m":
        return "DEEPAR_60M"
    return "DEEPAR_DAILY"


# ---------------------------------------------------------------------------
# Build DeepAR training dataset
# ---------------------------------------------------------------------------


def _build_deepar_dataset(
    symbols: Sequence[str],
    start: dt.datetime,
    end: dt.datetime,
    freq: str,
    seq_len: int,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, Dict[str, Any], int, Dict[str, int]]:
    """Build multi-stock DeepAR training dataset.

    Reuses `_build_sequences` from LSTM per-stock training to construct sequences
    and next-step log-return targets, then attaches static features and symbol_id.

    Returns:
    - x_dyn: (N, seq_len, dyn_dim)
    - x_static: (N, static_dim)
    - symbol_ids: (N,)
    - y: (N,)
    - dataset_meta: feature/static metadata
    - total_rows: total raw rows across all symbols
    - symbol2id: mapping ts_code -> int
    """

    if not symbols:
        raise ValueError("Universe symbols is empty")

    cfg = DeepARDatasetConfig()
    symbol_list = list(symbols)

    # Load static features once (as-of end.date)
    static_map, static_meta = _load_static_features_for_symbols(symbol_list, as_of_date=end.date())

    symbol2id: Dict[str, int] = {s: i for i, s in enumerate(symbol_list)}

    xs_dyn: List[np.ndarray] = []
    xs_static: List[np.ndarray] = []
    ys: List[np.ndarray] = []
    sym_ids: List[np.ndarray] = []

    feature_columns: List[str] | None = None
    total_rows = 0

    for ts_code in symbol_list:
        if ts_code not in static_map:
            # Skip symbols without static features for now
            continue

        if freq == "60m":
            df = load_deepar_60m_for_symbol(ts_code, start, end, cfg)
        else:
            df = load_deepar_daily_for_symbol(ts_code, start.date(), end.date(), cfg)

        if df.empty:
            continue

        total_rows += int(len(df))

        try:
            x_dyn, y_arr, meta = _build_sequences(df, seq_len=seq_len)
        except Exception:  # noqa: BLE001
            # Not enough data etc; skip this symbol
            continue

        if x_dyn.shape[0] == 0:
            continue

        if feature_columns is None:
            feature_columns = list(meta.get("feature_columns", []))

        s_vec = static_map[ts_code]
        n_samples = x_dyn.shape[0]
        x_static_sym = np.repeat(s_vec[None, :], n_samples, axis=0)

        xs_dyn.append(x_dyn)
        xs_static.append(x_static_sym)
        ys.append(y_arr)
        sym_ids.append(np.full(shape=(n_samples,), fill_value=symbol2id[ts_code], dtype=np.int64))

    if not xs_dyn:
        raise ValueError("no effective samples for DeepAR training")

    x_dyn_all = np.concatenate(xs_dyn, axis=0)
    x_static_all = np.concatenate(xs_static, axis=0)
    y_all = np.concatenate(ys, axis=0)
    sym_ids_all = np.concatenate(sym_ids, axis=0)

    dataset_meta = {
        "feature_columns": feature_columns or [],
        **static_meta,
        "freq": freq,
    }

    return x_dyn_all, x_static_all, sym_ids_all, y_all, dataset_meta, total_rows, symbol2id


# ---------------------------------------------------------------------------
# Training & model saving / run recording
# ---------------------------------------------------------------------------


def _train_deepar_rnn(
    x_dyn: np.ndarray,
    x_static: np.ndarray,
    symbol_ids: np.ndarray,
    y: np.ndarray,
    cfg: DeepARTrainConfig,
    device: str,
    num_symbols: int,
) -> Tuple[DeepARRegressor, Dict[str, Any]]:
    n_samples, seq_len, dyn_dim = x_dyn.shape
    static_dim = x_static.shape[1]

    ds = DeepARDataset(x_dyn, x_static, symbol_ids, y)

    val_size = max(1, int(n_samples * cfg.val_ratio))
    train_size = n_samples - val_size
    train_ds, val_ds = torch.utils.data.random_split(ds, [train_size, val_size])

    train_loader = DataLoader(train_ds, batch_size=cfg.batch_size, shuffle=True)
    val_loader = DataLoader(val_ds, batch_size=cfg.batch_size, shuffle=False)

    model = DeepARRegressor(
        dyn_input_size=dyn_dim,
        static_input_size=static_dim,
        num_symbols=num_symbols,
        symbol_emb_dim=cfg.symbol_emb_dim,
        hidden_size=cfg.hidden_size,
        num_layers=cfg.num_layers,
    ).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=cfg.lr)

    log_2pi = float(np.log(2.0 * np.pi))

    def _gaussian_nll(mu: torch.Tensor, sigma: torch.Tensor, target: torch.Tensor) -> torch.Tensor:
        # Negative log-likelihood of N(mu, sigma^2) up to constant
        return 0.5 * (((target - mu) / sigma) ** 2 + 2.0 * torch.log(sigma) + log_2pi)

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
            mu_b, sigma_b = model(x_dyn_b, x_static_b, sym_id_b)
            nll = _gaussian_nll(mu_b, sigma_b, y_b)
            loss = nll.mean()
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

                mu_b, sigma_b = model(x_dyn_b, x_static_b, sym_id_b)
                nll = _gaussian_nll(mu_b, sigma_b, y_b)
                loss = nll.mean()

                batch_size = x_dyn_b.size(0)
                val_loss += loss.item() * batch_size
                n_val += batch_size

        val_loss /= max(1, n_val)

        print(f"[EPOCH] {epoch}/{cfg.epochs} train_nll={train_loss:.6f} val_nll={val_loss:.6f}")

        if val_loss < best_val_loss:
            best_val_loss = val_loss
            best_state = model.state_dict()

    if best_state is not None:
        model.load_state_dict(best_state)

    metrics = {
        "train_samples": int(train_size),
        "val_samples": int(val_size),
        "best_val_nll": float(best_val_loss),
    }
    return model, metrics


def _save_model(
    model: DeepARRegressor,
    universe_name: str,
    freq: str,
    train_cfg: DeepARTrainConfig,
    dataset_meta: Dict[str, Any],
    symbol2id: Mapping[str, int],
) -> str:
    base_dir = Path(__file__).resolve().parents[3] / "models" / "deepar"
    base_dir.mkdir(parents=True, exist_ok=True)
    safe_universe = universe_name.replace(" ", "_")
    path = base_dir / f"deepar_{safe_universe}_{freq}.pt"

    payload = {
        "state_dict": model.state_dict(),
        "train_cfg": asdict(train_cfg),
        "dataset_meta": dataset_meta,
        "symbol2id": dict(symbol2id),
        "universe_name": universe_name,
        "freq": freq,
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
    freq = args.freq
    start = _to_dt(args.start)
    end = _to_dt(args.end)

    if start >= end:
        raise SystemExit("start must be earlier than end")

    train_cfg = DeepARTrainConfig(
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

    model_name = _model_name_for_freq(freq)

    print(
        f"[INFO] training {model_name} on universe={universe_name} "
        f"freq={freq} device={train_cfg.device}",
    )

    # Universe config & symbols
    uni_cfg = _load_universe_config(universe_name)
    symbols = _load_universe_symbols(universe_name, uni_cfg, start, end)
    if not symbols:
        print(f"[WARN] no symbols for universe={universe_name}")
        metrics = {"rows": 0, "symbols_covered_count": 0}
        _record_train_run(
            model_name=model_name,
            universe_name=universe_name,
            time_start=start,
            time_end=end,
            data_granularity=freq,
            symbols_covered_count=0,
            config_snapshot={
                "universe_name": universe_name,
                "universe_config": uni_cfg,
                "freq": freq,
                **asdict(train_cfg),
                **extra_cfg,
            },
            metrics=metrics,
            log_path=None,
        )
        return

    print(f"[INFO] universe={universe_name} symbols={len(symbols)} freq={freq}")

    try:
        x_dyn, x_static, sym_ids, y, dataset_meta, total_rows, symbol2id = _build_deepar_dataset(
            symbols=symbols,
            start=start,
            end=end,
            freq=freq,
            seq_len=train_cfg.seq_len,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] failed to build DeepAR dataset: {exc}")
        metrics = {"rows": 0, "symbols_covered_count": 0, "error": str(exc)}
        _record_train_run(
            model_name=model_name,
            universe_name=universe_name,
            time_start=start,
            time_end=end,
            data_granularity=freq,
            symbols_covered_count=0,
            config_snapshot={
                "universe_name": universe_name,
                "universe_config": uni_cfg,
                "freq": freq,
                **asdict(train_cfg),
                **extra_cfg,
            },
            metrics=metrics,
            log_path=None,
        )
        return

    print(
        f"[INFO] built DeepAR dataset: samples={x_dyn.shape[0]}, seq_len={x_dyn.shape[1]}, "
        f"dyn_dim={x_dyn.shape[2]}, static_dim={x_static.shape[1]}",
    )

    model, train_metrics = _train_deepar_rnn(
        x_dyn=x_dyn,
        x_static=x_static,
        symbol_ids=sym_ids,
        y=y,
        cfg=train_cfg,
        device=train_cfg.device,
        num_symbols=len(symbol2id),
    )

    model_path = _save_model(
        model=model,
        universe_name=universe_name,
        freq=freq,
        train_cfg=train_cfg,
        dataset_meta=dataset_meta,
        symbol2id=symbol2id,
    )
    print(f"[INFO] saved DeepAR model to {model_path}")

    metrics = {
        "rows": int(total_rows),
        "symbols_covered_count": int(len(symbol2id)),
        **train_metrics,
    }
    _record_train_run(
        model_name=model_name,
        universe_name=universe_name,
        time_start=start,
        time_end=end,
        data_granularity=freq,
        symbols_covered_count=len(symbol2id),
        config_snapshot={
            "universe_name": universe_name,
            "universe_config": uni_cfg,
            "freq": freq,
            **asdict(train_cfg),
            **extra_cfg,
        },
        metrics=metrics,
        log_path=model_path,
    )


if __name__ == "__main__":
    main()
