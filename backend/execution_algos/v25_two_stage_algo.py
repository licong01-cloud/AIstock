"""Strict V25 two-stage minute execution algorithm.

This backend adapter is self-contained so V25_TWO_STAGE does not depend on the
local git-ignored rl_execution package.  Missing model files, unavailable CUDA,
invalid market context, or invalid plans fail fast; it never falls back to TWAP.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np

from .base_algo import BaseExecutionAlgo, OrderState, StepResult
from .registry import register


EARLY_WEIGHT = 0.8879
LATE_WEIGHT = 0.1121
EARLY_LEN = 30
LATE_LEN = 210
TOTAL_LEN = 240
GAP_RATIO_EDGES = [-0.70, -0.50, -0.30, -0.10, 0.10, 0.30, 0.50, 0.70]


class V25TwoStageUnavailableError(RuntimeError):
    """Raised when V25_TWO_STAGE cannot run authoritatively."""


def _gap_ratio_to_bucket(gap_ratio: float) -> int:
    for i, edge in enumerate(GAP_RATIO_EDGES):
        if gap_ratio < edge:
            return i
    return len(GAP_RATIO_EDGES)


def _load_state(torch: Any, path: str, device: Any) -> Any:
    ckpt = torch.load(path, map_location=device, weights_only=False)
    if isinstance(ckpt, dict) and "model" in ckpt:
        return ckpt["model"]
    return ckpt


def _make_model_classes(torch: Any):
    nn = torch.nn
    functional = torch.nn.functional

    class EarlyPlanNetEnhanced(nn.Module):
        def __init__(self) -> None:
            super().__init__()
            self.mlp = nn.Sequential(
                nn.Linear(15, 128),
                nn.ReLU(),
                nn.Dropout(0.2),
                nn.Linear(128, 256),
                nn.ReLU(),
                nn.Dropout(0.2),
                nn.Linear(256, EARLY_LEN),
                nn.Softmax(dim=-1),
            )

        def forward(self, gap_bucket, gap_ratio, gap_ratio_signed, limit_pct, is_buy, day_features):
            gap_bucket_norm = gap_bucket.float() / 8.0
            x = torch.cat(
                [
                    gap_ratio.unsqueeze(1),
                    gap_ratio_signed.unsqueeze(1),
                    limit_pct.unsqueeze(1),
                    is_buy.unsqueeze(1),
                    gap_bucket_norm.unsqueeze(1),
                    day_features,
                ],
                dim=1,
            )
            return self.mlp(x)

    class LatePlanNet(nn.Module):
        def __init__(self, gap_buckets: int = 9, gap_emb_dim: int = 16, late_len: int = LATE_LEN) -> None:
            super().__init__()
            self.gap_embedding = nn.Embedding(gap_buckets, gap_emb_dim)
            self.mlp = nn.Sequential(
                nn.Linear(gap_emb_dim + 5, 128),
                nn.ReLU(),
                nn.Dropout(0.2),
                nn.Linear(128, 256),
                nn.ReLU(),
                nn.Dropout(0.2),
                nn.Linear(256, late_len),
            )

        def forward(self, gap_bucket_idx, gap_ratio, is_buy, early_weight, early_peak_pos, early_concentration):
            gap_emb = self.gap_embedding(gap_bucket_idx)
            extra = torch.stack([gap_ratio, is_buy, early_weight, early_peak_pos, early_concentration], dim=1)
            return functional.softmax(self.mlp(torch.cat([gap_emb, extra], dim=1)), dim=-1)

    return EarlyPlanNetEnhanced, LatePlanNet


@register
class V25TwoStageAlgo(BaseExecutionAlgo):
    ALGO_CODE = "V25_TWO_STAGE"

    def __init__(self, config: Dict[str, Any] | None = None) -> None:
        super().__init__(config)
        early_model_path = str(self.config.get("early_model_path") or "").strip()
        late_model_path = str(self.config.get("late_model_path") or "").strip()
        if not early_model_path or not late_model_path:
            raise V25TwoStageUnavailableError(
                "V25_TWO_STAGE requires config.early_model_path and config.late_model_path"
            )
        if not Path(early_model_path).exists():
            raise V25TwoStageUnavailableError(
                f"V25_TWO_STAGE early_model_path does not exist: {early_model_path}"
            )
        if not Path(late_model_path).exists():
            raise V25TwoStageUnavailableError(
                f"V25_TWO_STAGE late_model_path does not exist: {late_model_path}"
            )

        try:
            import torch
        except Exception as exc:  # pragma: no cover - optional runtime dependency
            raise V25TwoStageUnavailableError(
                f"V25_TWO_STAGE requires torch: {type(exc).__name__}: {exc}"
            ) from exc

        device_name = str(self.config.get("device") or "cpu")
        if device_name.startswith("cuda") and not torch.cuda.is_available():
            raise V25TwoStageUnavailableError("V25_TWO_STAGE requested CUDA but torch.cuda.is_available() is false")

        self._torch = torch
        self._device = torch.device(device_name)
        EarlyPlanNetEnhanced, LatePlanNet = _make_model_classes(torch)
        self._early_model = EarlyPlanNetEnhanced().to(self._device)
        self._late_model = LatePlanNet().to(self._device)
        self._early_model.load_state_dict(_load_state(torch, early_model_path, self._device))
        self._late_model.load_state_dict(_load_state(torch, late_model_path, self._device))
        self._early_model.eval()
        self._late_model.eval()
        self._plan: Optional[np.ndarray] = None
        self._plan_key: Optional[tuple[str, str]] = None

    def compute_step(
        self,
        state: OrderState,
        bar_data: Dict[str, Any],
        market_context: Dict[str, Any],
    ) -> Optional[StepResult]:
        if self.is_complete(state):
            return None

        remaining = state.total_quantity - state.executed_quantity
        if remaining <= 0:
            state.is_complete = True
            return None

        close_arr = self._require_array(market_context, "full_day_close")
        vol_arr = self._require_array(market_context, "full_day_volume")
        high_arr = self._require_array(market_context, "full_day_high")
        low_arr = self._require_array(market_context, "full_day_low")
        if not (len(close_arr) == len(vol_arr) == len(high_arr) == len(low_arr)):
            raise V25TwoStageUnavailableError("V25_TWO_STAGE full-day arrays must have equal length")
        if len(close_arr) < TOTAL_LEN:
            raise V25TwoStageUnavailableError("V25_TWO_STAGE requires at least 240 minute bars")

        prev_close = self._require_positive(market_context, "prev_close")
        cur_price = float(bar_data.get("close") or 0)
        open_price = float(bar_data.get("open") or close_arr[0])
        if cur_price <= 0 or open_price <= 0:
            raise V25TwoStageUnavailableError("V25_TWO_STAGE requires positive open/close prices")

        stock_id = str(market_context.get("stock_id") or state.symbol)
        side = state.side.upper()
        if side not in {"BUY", "SELL"}:
            raise V25TwoStageUnavailableError(f"V25_TWO_STAGE unsupported side: {state.side}")
        is_buy = side == "BUY"
        limit_pct = float(market_context.get("limit_pct") or self._infer_limit_pct(stock_id))
        if limit_pct <= 0:
            raise V25TwoStageUnavailableError("V25_TWO_STAGE requires positive limit_pct")
        day_features = self._day_features(market_context.get("day_features"))

        plan_key = (stock_id, side)
        if self._plan is None or self._plan_key != plan_key:
            self._plan = self._generate_plan(
                open_price=open_price,
                prev_close=prev_close,
                stock_id=stock_id,
                is_buy=is_buy,
                limit_pct=limit_pct,
                day_features=day_features,
            )
            self._plan_key = plan_key

        cur_step = int(state.step)
        if cur_step < 0:
            raise V25TwoStageUnavailableError("V25_TWO_STAGE state.step cannot be negative")
        if cur_step >= min(len(close_arr), TOTAL_LEN) - 1:
            step_qty = remaining
        else:
            remaining_weight = float(self._plan[cur_step:].sum())
            if remaining_weight <= 1e-8:
                raise V25TwoStageUnavailableError("V25_TWO_STAGE remaining plan weight is zero")
            frac = float(self._plan[cur_step]) / remaining_weight
            step_qty = self._round_lot(int(remaining * frac))
            step_qty = min(step_qty, remaining)

        state.step += 1
        if step_qty <= 0:
            return None

        state.executed_quantity += step_qty
        if state.executed_quantity >= state.total_quantity:
            state.is_complete = True

        return StepResult(
            symbol=state.symbol,
            side=state.side,
            quantity=step_qty,
            price=cur_price,
            reason=f"V25_TWO_STAGE step {state.step}/{min(len(close_arr), TOTAL_LEN)}",
        )

    def _generate_plan(
        self,
        *,
        open_price: float,
        prev_close: float,
        stock_id: str,
        is_buy: bool,
        limit_pct: float,
        day_features: np.ndarray,
    ) -> np.ndarray:
        gap_pct = np.clip((open_price - prev_close) / prev_close, -0.20, 0.20)
        gap_ratio = float(gap_pct / limit_pct)
        gap_bucket = _gap_ratio_to_bucket(gap_ratio)
        torch = self._torch
        with torch.no_grad():
            gb = torch.LongTensor([gap_bucket]).to(self._device)
            gr_abs = torch.FloatTensor([abs(gap_ratio)]).to(self._device)
            gr_signed = torch.FloatTensor([gap_ratio]).to(self._device)
            lp = torch.FloatTensor([limit_pct]).to(self._device)
            ib = torch.FloatTensor([1.0 if is_buy else 0.0]).to(self._device)
            df = torch.FloatTensor([day_features.astype(np.float32)]).to(self._device)
            pred_early = self._early_model(gb, gr_abs, gr_signed, lp, ib, df).cpu().numpy()[0]
            early_weight_raw = float(pred_early.sum())
            early_peak_pos = float(pred_early.argmax() / max(EARLY_LEN - 1, 1))
            early_mean = float(pred_early.mean())
            early_concentration = float(pred_early.max() / (early_mean + 1e-8))
            ew = torch.FloatTensor([early_weight_raw]).to(self._device)
            epp = torch.FloatTensor([early_peak_pos]).to(self._device)
            ec = torch.FloatTensor([early_concentration]).to(self._device)
            pred_late = self._late_model(gb, gr_abs, ib, ew, epp, ec).cpu().numpy()[0]

        plan = np.concatenate([pred_early * EARLY_WEIGHT, pred_late * LATE_WEIGHT]).astype(np.float64)
        if len(plan) != TOTAL_LEN or np.isnan(plan).any() or plan.sum() <= 1e-8:
            raise V25TwoStageUnavailableError(
                f"V25_TWO_STAGE generated invalid plan for {stock_id}: len={len(plan)} sum={plan.sum()}"
            )
        plan = plan / plan.sum()
        early_sum = float(plan[:EARLY_LEN].sum())
        late_sum = float(plan[EARLY_LEN:].sum())
        if abs(early_sum - EARLY_WEIGHT) > 1e-4 or abs(late_sum - LATE_WEIGHT) > 1e-4:
            raise V25TwoStageUnavailableError(
                f"V25_TWO_STAGE weight mismatch: early={early_sum:.6f} late={late_sum:.6f}"
            )
        return plan

    @staticmethod
    def _require_array(ctx: Dict[str, Any], key: str) -> np.ndarray:
        val = ctx.get(key)
        if val is None:
            raise V25TwoStageUnavailableError(f"V25_TWO_STAGE requires market_context.{key}")
        arr = np.asarray(val, dtype=np.float64)
        if arr.ndim != 1 or arr.size == 0 or np.isnan(arr).any():
            raise V25TwoStageUnavailableError(f"V25_TWO_STAGE market_context.{key} is invalid")
        return arr

    @staticmethod
    def _require_positive(ctx: Dict[str, Any], key: str) -> float:
        val = float(ctx.get(key) or 0)
        if val <= 0:
            raise V25TwoStageUnavailableError(f"V25_TWO_STAGE requires positive {key}")
        return val

    @staticmethod
    def _day_features(value: Any) -> np.ndarray:
        if value is None:
            return np.zeros(10, dtype=np.float32)
        arr = np.asarray(value, dtype=np.float32)
        if arr.shape != (10,) or np.isnan(arr).any():
            raise V25TwoStageUnavailableError("V25_TWO_STAGE day_features must be a 10-element array")
        return arr

    @staticmethod
    def _infer_limit_pct(stock_id: str) -> float:
        code = stock_id.split(".")[0]
        if code.startswith(("300", "301", "688", "689")):
            return 0.20
        return 0.10
