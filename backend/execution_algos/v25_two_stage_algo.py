"""Strict V25 two-stage minute execution algorithm.

This is the Paper v2 adapter for the shared V25 core. Missing model files,
unavailable CUDA, invalid market context, or invalid plans fail fast; it never
falls back to TWAP. Normal market non-tradable states such as suspension or
limit-up/limit-down blocks are classified explicitly instead of being treated as
configuration/model failures.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np

from .base_algo import BaseExecutionAlgo, OrderState, StepResult
from .board_lot import round_to_board_lot
from .registry import register
from .v25_core import (
    EARLY_LEN,
    LATE_LEN,
    TOTAL_LEN,
    REASON_LIMIT_DATA_MISSING_DUE_TO_SUSPEND,
    REASON_PREV_CLOSE_MISSING_WITH_SUSPEND,
    REASON_PRICE_MISSING_WITH_SUSPEND,
    REASON_SUSPENDED_BY_EXCHANGE,
    REASON_SUSPENDED_BY_SUSPEND_D,
    V25MarketAction,
    V25TwoStageCore,
    V25TwoStageCoreError,
    classify_v25_minute_market_state,
    infer_limit_pct,
)


class V25TwoStageUnavailableError(RuntimeError):
    """Raised when V25_TWO_STAGE cannot run authoritatively."""


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
    HANDLES_MARKET_STATE = True

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
        self._core = V25TwoStageCore(
            early_predictor=self._predict_early,
            late_predictor=self._predict_late,
        )
        self._plan: Optional[np.ndarray] = None
        self._plan_key: Optional[tuple[str, str]] = None
        self._plan_metadata: dict[str, Any] = {}
        self._last_no_fill_reason: Optional[str] = None
        self._last_no_fill_context: dict[str, Any] = {}

    def compute_step(
        self,
        state: OrderState,
        bar_data: Dict[str, Any],
        market_context: Dict[str, Any],
    ) -> Optional[StepResult]:
        self._last_no_fill_reason = None
        self._last_no_fill_context = {}
        if self.is_complete(state):
            return None

        remaining = state.total_quantity - state.executed_quantity
        if remaining <= 0:
            state.is_complete = True
            return None

        cur_price = float(bar_data.get("close") or 0)
        prev_close = market_context.get("prev_close")
        limit_up = bar_data.get("limit_up") or market_context.get("limit_up")
        limit_down = bar_data.get("limit_down") or market_context.get("limit_down")
        price_basis = str(bar_data.get("price_basis") or market_context.get("price_basis") or "raw")
        limit_price_basis = str(
            bar_data.get("limit_price_basis")
            or market_context.get("limit_price_basis")
            or market_context.get("prev_close_basis")
            or price_basis
        )
        market_state = classify_v25_minute_market_state(
            side=state.side,
            price=cur_price,
            volume=bar_data.get("volume"),
            prev_close=prev_close,
            limit_up=limit_up,
            limit_down=limit_down,
            is_suspended=bool(bar_data.get("is_suspended")),
            suspend_status=market_context.get("suspend_status"),
            require_limit_price=True,
            price_basis=price_basis,
            limit_price_basis=limit_price_basis,
        )
        if market_state.is_error:
            raise V25TwoStageUnavailableError(
                f"V25_TWO_STAGE market data error: {market_state.reason}"
            )
        if market_state.action == V25MarketAction.SKIP and market_state.reason in {
            REASON_SUSPENDED_BY_SUSPEND_D,
            REASON_SUSPENDED_BY_EXCHANGE,
            REASON_PREV_CLOSE_MISSING_WITH_SUSPEND,
            REASON_PRICE_MISSING_WITH_SUSPEND,
            REASON_LIMIT_DATA_MISSING_DUE_TO_SUSPEND,
        }:
            self._last_no_fill_reason = market_state.reason
            self._last_no_fill_context = dict(market_state.context)
            state.step += 1
            return None

        close_arr = self._require_array(market_context, "full_day_close")
        vol_arr = self._require_array(market_context, "full_day_volume")
        high_arr = self._require_array(market_context, "full_day_high")
        low_arr = self._require_array(market_context, "full_day_low")
        if not (len(close_arr) == len(vol_arr) == len(high_arr) == len(low_arr)):
            raise V25TwoStageUnavailableError("V25_TWO_STAGE full-day arrays must have equal length")
        realtime_streaming = bool(market_context.get("v25_realtime_streaming") or market_context.get("observed_only"))
        if len(close_arr) < TOTAL_LEN and not realtime_streaming:
            raise V25TwoStageUnavailableError("V25_TWO_STAGE requires at least 240 minute bars")

        open_arr = market_context.get("full_day_open")
        if open_arr is not None:
            open_price = float(self._require_array(market_context, "full_day_open")[0])
        else:
            open_price = float(bar_data.get("open") or close_arr[0])
        if cur_price <= 0 or open_price <= 0:
            raise V25TwoStageUnavailableError("V25_TWO_STAGE requires positive open/close prices")

        stock_id = str(market_context.get("stock_id") or state.symbol)
        side = state.side.upper()
        if side not in {"BUY", "SELL"}:
            raise V25TwoStageUnavailableError(f"V25_TWO_STAGE unsupported side: {state.side}")
        is_buy = side == "BUY"
        limit_pct = float(market_context.get("limit_pct") or infer_limit_pct(stock_id))
        if limit_pct <= 0:
            raise V25TwoStageUnavailableError("V25_TWO_STAGE requires positive limit_pct")
        day_features = self._day_features(market_context.get("day_features"))

        plan_key = (stock_id, side)
        if self._plan is None or self._plan_key != plan_key:
            self._plan = self._generate_plan(
                open_price=open_price,
                prev_close=float(prev_close),
                stock_id=stock_id,
                is_buy=is_buy,
                limit_pct=limit_pct,
                day_features=day_features,
            )
            self._plan_key = plan_key

        cur_step = int(state.step)
        if cur_step < 0:
            raise V25TwoStageUnavailableError("V25_TWO_STAGE state.step cannot be negative")
        horizon = TOTAL_LEN if realtime_streaming else min(len(close_arr), TOTAL_LEN)
        if horizon <= 0:
            raise V25TwoStageUnavailableError("V25_TWO_STAGE execution horizon is invalid")

        if market_state.action == V25MarketAction.SKIP:
            self._last_no_fill_reason = market_state.reason
            self._last_no_fill_context = dict(market_state.context)
            state.step += 1
            return None

        if market_state.action == V25MarketAction.P0_FORCE:
            step_qty = remaining
            reason = market_state.reason
        elif cur_step >= horizon - 1:
            step_qty = remaining
            reason = f"V25_TWO_STAGE step {state.step + 1}/{horizon}"
        else:
            remaining_weight = float(self._plan[cur_step:].sum())
            if remaining_weight <= 1e-8:
                raise V25TwoStageUnavailableError("V25_TWO_STAGE remaining plan weight is zero")
            frac = float(self._plan[cur_step]) / remaining_weight
            step_qty = self._round_lot(int(remaining * frac))
            step_qty = min(step_qty, remaining)
            reason = f"V25_TWO_STAGE step {state.step + 1}/{horizon}"

        step_qty = round_to_board_lot(step_qty, stock_id, side=side)
        state.step += 1
        if step_qty <= 0:
            self._last_no_fill_reason = "board_lot_zero"
            self._last_no_fill_context = {"cur_step": cur_step, "remaining_quantity": remaining}
            return None

        state.executed_quantity += step_qty
        if state.executed_quantity >= state.total_quantity:
            state.is_complete = True

        return StepResult(
            symbol=state.symbol,
            side=state.side,
            quantity=step_qty,
            price=cur_price,
            reason=reason,
        )

    def _predict_early(
        self,
        gap_bucket: int,
        gap_ratio_abs: float,
        gap_ratio_signed: float,
        limit_pct: float,
        is_buy: float,
        day_features: np.ndarray,
    ) -> np.ndarray:
        torch = self._torch
        with torch.no_grad():
            gb = torch.LongTensor([gap_bucket]).to(self._device)
            gr_abs = torch.FloatTensor([gap_ratio_abs]).to(self._device)
            gr_signed = torch.FloatTensor([gap_ratio_signed]).to(self._device)
            lp = torch.FloatTensor([limit_pct]).to(self._device)
            ib = torch.FloatTensor([is_buy]).to(self._device)
            df = torch.FloatTensor([day_features.astype(np.float32)]).to(self._device)
            return self._early_model(gb, gr_abs, gr_signed, lp, ib, df).cpu().numpy()[0]

    def _predict_late(
        self,
        gap_bucket: int,
        gap_ratio_abs: float,
        is_buy: float,
        early_weight: float,
        early_peak_pos: float,
        early_concentration: float,
    ) -> np.ndarray:
        torch = self._torch
        with torch.no_grad():
            gb = torch.LongTensor([gap_bucket]).to(self._device)
            gr_abs = torch.FloatTensor([gap_ratio_abs]).to(self._device)
            ib = torch.FloatTensor([is_buy]).to(self._device)
            ew = torch.FloatTensor([early_weight]).to(self._device)
            epp = torch.FloatTensor([early_peak_pos]).to(self._device)
            ec = torch.FloatTensor([early_concentration]).to(self._device)
            return self._late_model(gb, gr_abs, ib, ew, epp, ec).cpu().numpy()[0]

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
        try:
            result = self._core.generate_plan(
                open_price=open_price,
                prev_close=prev_close,
                stock_id=stock_id,
                side="BUY" if is_buy else "SELL",
                limit_pct=limit_pct,
                day_features=day_features,
            )
        except V25TwoStageCoreError as exc:
            raise V25TwoStageUnavailableError(str(exc)) from exc
        self._plan_metadata = dict(result.metadata)
        return result.weights

    @staticmethod
    def _require_array(ctx: Dict[str, Any], key: str) -> np.ndarray:
        val = ctx.get(key)
        if val is None:
            raise V25TwoStageUnavailableError(f"V25_TWO_STAGE requires market_context.{key}")
        arr = np.asarray(val, dtype=np.float64)
        if arr.ndim != 1 or arr.size == 0 or np.isnan(arr).any():
            raise V25TwoStageUnavailableError(f"V25_TWO_STAGE market_context.{key} is invalid")
        return arr

    def _day_features(self, value: Any) -> np.ndarray:
        if value is None:
            if bool(self.config.get("allow_default_day_features")):
                return np.zeros(10, dtype=np.float32)
            raise V25TwoStageUnavailableError(
                "V25_TWO_STAGE requires market_context.day_features; "
                "set allow_default_day_features=true only for an explicitly audited diagnostic run"
            )
        arr = np.asarray(value, dtype=np.float32)
        if arr.shape != (10,) or np.isnan(arr).any():
            raise V25TwoStageUnavailableError("V25_TWO_STAGE day_features must be a 10-element array")
        return arr
