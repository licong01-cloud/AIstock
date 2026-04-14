"""v24 Plan Net 执行策略 (TailTWAPWithV24PlanStrategy)。

继承 TailTWAPWithLimitStrategy，完整复用：
  - P0 涨跌停全量执行 (任意时刻最高优先级)
  - P4 尾盘未成交再分配 (TAIL_BOOST / TAIL_SUBSTITUTE, 14:55 触发)

执行逻辑（贯穿全天 240 分钟）：
  - t=0~29  (09:30~09:59): WARMUP 均匀预分配 warmup_alloc 总额 (B1 默认 20%)
                           同时采集 $close/$volume/$high/$low/$up_limit_price/$down_limit_price/$prev_close
  - t=30    (10:00):       首次调用 plan_model 生成 210 维 softmax 分布, 缓存
  - t=30~239 (10:00~15:00): 按 plan[t-30] 条件比例执行 remaining
                           (P4 在 t=235 触发后, 叠加 extra 追加执行)
  - t=239   最后一分钟:     强制清空所有 remaining + extra

v24 plan 贯穿 t=30~239 全程 (210 分钟), 不在 t=210 截断。
extra 资金再分配在 plan 分配基础上追加, 而非替换 plan。

B1 最优配置 (评估 PA=+6.35 bps vs v20 基线 +5.11 bps, 买入侧 +30%):
  - 启用 Plan Net
  - 不启用 Layer 1.5 WARMUP 查表 (warmup_alloc 固定 20%)
  - 不启用 Layer 3 Correction
  - 不启用 Layer 1 追价规则 R6/R7

网络与特征工程内联于本文件, 不依赖 rl_execution 包, 因为 qlib 回测
在独立的 exp_dir 工作目录中执行。

错误处理原则：
  - 任何改变执行分配方式的 fallback 必须 logger.warning 明确告警
  - 关键数据缺失 (prev_close, up_limit_price, down_limit_price) 直接 raise, 不静默降级
  - plan 生成失败将记录股票为"降级执行", 使用 TWAP 直至结束
"""
from __future__ import annotations

import logging

import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from qlib.backtest.decision import Order, OrderDir, TradeDecisionWO
from qlib.backtest.utils import get_start_end_idx

from tail_twap_strategy import (
    TailTWAPWithLimitStrategy,
    TAIL_START_OFFSET,
    REALLOC_OFFSET,
    BLOCKED_FILL_THRESHOLD,
)


logger = logging.getLogger(__name__)

# v24 Plan Net 常量
WARMUP_MINUTES = 30
PLAN_LEN = 210
FULL_DAY_MINUTES = 240
GAP_RATIO_EDGES = [-0.70, -0.50, -0.30, -0.10, 0.10, 0.30, 0.50, 0.70]


# ============================================================
# ExecutionPlanNetV24 (inline, 与 rl_execution/network/execution_plan_net_v24.py 一致)
# ============================================================
class ExecutionPlanNetV24(nn.Module):
    """v24 执行计划生成网络 — 归一化缺口感知 + 买卖不对称。

    输入:
      minute_feats: [B, 30, 5] — 开盘30分钟特征 (rets/vol_ratio/hl_range/vwap_dev/rsi)
      day_feats: [B, 10] — 日级特征
      gap_bucket_idx: [B] — 归一化缺口桶索引 (0~8)
      gap_ratio: [B] — 归一化缺口值
      gap_ratio_signed: [B] — 归一化缺口 × 方向
      limit_pct: [B] — 涨跌停幅度 (0.10/0.20)
      is_buy: [B] — 方向 (1.0=买, 0.0=卖)

    输出: [B, 210] softmax 执行权重分布
    """

    def __init__(self, minute_dim: int = 5, day_dim: int = 10,
                 gap_buckets: int = 9, gap_emb_dim: int = 8,
                 plan_len: int = 210, cnn_channels: int = 64,
                 hidden_dim: int = 256):
        super().__init__()
        self.plan_len = plan_len

        self.cnn = nn.Sequential(
            nn.Conv1d(minute_dim, cnn_channels, kernel_size=5, padding=2),
            nn.BatchNorm1d(cnn_channels),
            nn.ReLU(),
            nn.MaxPool1d(2),
            nn.Conv1d(cnn_channels, cnn_channels, kernel_size=3, padding=1),
            nn.BatchNorm1d(cnn_channels),
            nn.ReLU(),
            nn.MaxPool1d(2),
            nn.Conv1d(cnn_channels, cnn_channels, kernel_size=3, padding=1),
            nn.BatchNorm1d(cnn_channels),
            nn.ReLU(),
            nn.AdaptiveAvgPool1d(1),
        )

        self.gap_embedding = nn.Embedding(gap_buckets, gap_emb_dim)

        fusion_dim = cnn_channels + day_dim + gap_emb_dim + 4
        self.mlp = nn.Sequential(
            nn.Linear(fusion_dim, hidden_dim),
            nn.ReLU(),
            nn.Dropout(0.1),
            nn.Linear(hidden_dim, hidden_dim),
            nn.ReLU(),
            nn.Dropout(0.1),
            nn.Linear(hidden_dim, plan_len),
        )

    def forward(self, minute_feats, day_feats, gap_bucket_idx,
                gap_ratio, gap_ratio_signed, limit_pct, is_buy):
        x = minute_feats.transpose(1, 2)
        cnn_out = self.cnn(x).squeeze(-1)
        gap_emb = self.gap_embedding(gap_bucket_idx)
        extra = torch.stack([gap_ratio, gap_ratio_signed,
                             limit_pct, is_buy], dim=1)
        fused = torch.cat([cnn_out, day_feats, gap_emb, extra], dim=1)
        logits = self.mlp(fused)
        return F.softmax(logits, dim=-1)


# ============================================================
# 工具函数
# ============================================================
def _get_limit_pct(stock_id: str) -> float:
    """沪深主板 10%, 创业板/科创板 20%。"""
    code = stock_id.split(".")[0]
    if code.startswith(("300", "301", "688", "689")):
        return 0.20
    return 0.10


def _gap_ratio_to_bucket(gap_ratio: float) -> int:
    for i, edge in enumerate(GAP_RATIO_EDGES):
        if gap_ratio < edge:
            return i
    return len(GAP_RATIO_EDGES)


class _DataMissingError(Exception):
    """数据缺失异常，用于显式失败而非静默 fallback。"""
    pass


# ============================================================
# 主策略类
# ============================================================
class TailTWAPWithV24PlanStrategy(TailTWAPWithLimitStrategy):
    """v24 Plan Net 分钟级执行 + 尾盘未成交再分配 (继承父类)。

    核心差异 vs TailTWAPWithLimitStrategy:
      - 整天 210 分钟 (t=30~239) 都用 v24 plan 驱动, 不在 t=210 截断
      - 尾盘未成交再分配的 extra 在 plan 分配基础上叠加追加

    继承能力 (完全复用父类):
      - P0 涨跌停全量执行
      - P4 TAIL_BOOST / TAIL_SUBSTITUTE 尾盘未成交再分配 (_do_realloc* 方法)
    """

    def __init__(
        self,
        model_path: str,
        warmup_minutes: int = WARMUP_MINUTES,
        warmup_alloc: float = 0.20,
        device: str = "cpu",
        # 父类参数
        start_time=None,
        end_time=None,
        split_count=None,
        lookback_days=None,
        participation_rate=None,
        unfilled_handler=None,
        unfilled_trigger_minute=None,
        unfilled_backup_depth=None,
        **kwargs,
    ):
        super().__init__(
            start_time=start_time,
            end_time=end_time,
            split_count=split_count,
            lookback_days=lookback_days,
            participation_rate=participation_rate,
            unfilled_handler=unfilled_handler,
            unfilled_trigger_minute=unfilled_trigger_minute,
            unfilled_backup_depth=unfilled_backup_depth,
            **kwargs,
        )
        if not model_path:
            raise ValueError("TailTWAPWithV24PlanStrategy 要求提供 model_path")
        self._model_path = str(model_path)
        self._warmup_minutes = int(warmup_minutes)
        if self._warmup_minutes != WARMUP_MINUTES:
            raise ValueError(
                f"warmup_minutes 必须为 {WARMUP_MINUTES} (训练时固定值), "
                f"传入 {warmup_minutes} 会与模型输入维度不匹配"
            )
        self._warmup_alloc = float(warmup_alloc)
        if not (0.0 < self._warmup_alloc <= 1.0):
            raise ValueError(f"warmup_alloc 必须在 (0, 1] 范围内, 传入 {warmup_alloc}")
        self._device = torch.device(device)

        self._plan_model = ExecutionPlanNetV24().to(self._device)
        ckpt = torch.load(self._model_path, map_location=self._device, weights_only=False)
        state = ckpt["model"] if isinstance(ckpt, dict) and "model" in ckpt else ckpt
        self._plan_model.load_state_dict(state)
        self._plan_model.eval()
        logger.info("[TailTWAPv24] loaded plan model from %s", self._model_path)

    def reset(self, outer_trade_decision=None, **kwargs):
        super().reset(outer_trade_decision=outer_trade_decision, **kwargs)
        if outer_trade_decision is not None:
            # 每只股票的 210 维 plan 分布 (延迟到 t=30 时生成)
            self._v24_plans: dict[str, np.ndarray] = {}
            # 标记 plan 生成是否已尝试 (避免每分钟重试)
            self._v24_plan_generated: dict[str, bool] = {}
            # 标记 plan 生成失败, 后续走降级 TWAP
            self._v24_plan_failed: set[str] = set()
            # 分钟级数据缓存: 在 warmup 期间逐分钟累积
            self._v24_close_buf: dict[str, list] = {}
            self._v24_vol_buf: dict[str, list] = {}
            self._v24_high_buf: dict[str, list] = {}
            self._v24_low_buf: dict[str, list] = {}
            self._v24_lu_buf: dict[str, list] = {}   # $up_limit_price 序列 (真实涨停价, 元)
            self._v24_ld_buf: dict[str, list] = {}   # $down_limit_price 序列 (真实跌停价, 元)
            self._v24_pc_buf: dict[str, list] = {}   # $prev_close 序列 (昨收价, 元)
            # 停牌检测标记 (整天 high==low 或 vol.sum()==0)
            self._v24_suspended: set[str] = set()

    # ------------------------------------------------------------
    # 数据收集: warmup 期间逐分钟累积分钟特征所需数据
    # ------------------------------------------------------------
    def _collect_bar(self, stock_id, trade_start_time, trade_end_time):
        """在 warmup 期间逐分钟收集 close/vol/high/low/up_limit_price/down_limit_price/prev_close。

        任一关键字段缺失则本根不追加, 并记录 warning。
        """
        try:
            close = self.trade_exchange.get_close(
                stock_id, trade_start_time, trade_end_time,
                method="ts_data_last",
            )
        except (KeyError, IndexError, ValueError) as e:
            logger.warning(
                "[TailTWAPv24] collect_bar get_close failed stock=%s t=%s: %s",
                stock_id, trade_start_time, e,
            )
            return

        if close is None or np.isnan(close) or close <= 0:
            logger.warning(
                "[TailTWAPv24] collect_bar invalid close stock=%s t=%s close=%s",
                stock_id, trade_start_time, close,
            )
            return

        try:
            vol = self.trade_exchange.quote.get_data(
                stock_id, trade_start_time, trade_end_time,
                field="$volume", method="ts_data_last",
            )
            high = self.trade_exchange.quote.get_data(
                stock_id, trade_start_time, trade_end_time,
                field="$high", method="ts_data_last",
            )
            low = self.trade_exchange.quote.get_data(
                stock_id, trade_start_time, trade_end_time,
                field="$low", method="ts_data_last",
            )
            lu = self.trade_exchange.quote.get_data(
                stock_id, trade_start_time, trade_end_time,
                field="$up_limit_price", method="ts_data_last",
            )
            ld = self.trade_exchange.quote.get_data(
                stock_id, trade_start_time, trade_end_time,
                field="$down_limit_price", method="ts_data_last",
            )
            pc = self.trade_exchange.quote.get_data(
                stock_id, trade_start_time, trade_end_time,
                field="$prev_close", method="ts_data_last",
            )
        except (KeyError, IndexError, ValueError) as e:
            logger.warning(
                "[TailTWAPv24] collect_bar quote fields failed stock=%s t=%s: %s",
                stock_id, trade_start_time, e,
            )
            return

        # 关键字段不得为 None 或 NaN — 任何缺失都丢弃本根而非用替代值
        if vol is None or np.isnan(vol):
            logger.warning(
                "[TailTWAPv24] collect_bar missing volume stock=%s t=%s",
                stock_id, trade_start_time,
            )
            return
        if high is None or np.isnan(high):
            logger.warning(
                "[TailTWAPv24] collect_bar missing high stock=%s t=%s",
                stock_id, trade_start_time,
            )
            return
        if low is None or np.isnan(low):
            logger.warning(
                "[TailTWAPv24] collect_bar missing low stock=%s t=%s",
                stock_id, trade_start_time,
            )
            return
        if lu is None or np.isnan(lu) or lu <= 0:
            logger.warning(
                "[TailTWAPv24] collect_bar missing up_limit_price stock=%s t=%s lu=%s",
                stock_id, trade_start_time, lu,
            )
            return
        if ld is None or np.isnan(ld) or ld <= 0:
            logger.warning(
                "[TailTWAPv24] collect_bar missing down_limit_price stock=%s t=%s ld=%s",
                stock_id, trade_start_time, ld,
            )
            return
        if pc is None or np.isnan(pc) or pc <= 0:
            logger.warning(
                "[TailTWAPv24] collect_bar missing prev_close stock=%s t=%s pc=%s",
                stock_id, trade_start_time, pc,
            )
            return

        self._v24_close_buf.setdefault(stock_id, []).append(float(close))
        self._v24_vol_buf.setdefault(stock_id, []).append(float(vol))
        self._v24_high_buf.setdefault(stock_id, []).append(float(high))
        self._v24_low_buf.setdefault(stock_id, []).append(float(low))
        self._v24_lu_buf.setdefault(stock_id, []).append(float(lu))
        self._v24_ld_buf.setdefault(stock_id, []).append(float(ld))
        self._v24_pc_buf.setdefault(stock_id, []).append(float(pc))

    # ------------------------------------------------------------
    # 特征工程 (与 v24_gen_plan_data.py 严格对齐)
    # ------------------------------------------------------------
    def _build_v24_inputs(self, close_arr, vol_arr, high_arr, low_arr,
                          lu_arr, ld_arr, prev_close, is_buy, limit_pct):
        """构造 v24 Plan Net 前 30 分钟特征 + 10 维日特征。

        严格对齐 v24_gen_plan_data.py 的特征定义。

        注意: 训练时 avg_vol 和 day_feat[3] 的分母使用全天 240 分钟的 vol.sum(),
        执行时只有前 30 分钟数据, 采用下述近似方案:
          - avg_vol: 用前 30 分钟平均 (vol_arr[:W].mean())
          - day_feat[3]: 假设全天 vol ≈ w_vol.sum() * (240/W) 做归一化估算
        """
        W = self._warmup_minutes
        # ── 5 维分钟特征 ──
        rets = np.zeros(W, dtype=np.float64)
        rets[1:] = (close_arr[1:W] - close_arr[:W - 1]) / (close_arr[:W - 1] + 1e-8)

        # 训练时 avg_vol = vol.mean() 用全天平均;
        # 执行时仅前 30 分钟, 用前 30 分钟均值 (已知近似)
        avg_vol = vol_arr[:W].mean() + 1e-8
        vol_ratio = vol_arr[:W] / avg_vol

        hl_range = (high_arr[:W] - low_arr[:W]) / (close_arr[:W] + 1e-8)

        cum_pv = np.cumsum(close_arr[:W] * vol_arr[:W])
        cum_v = np.cumsum(vol_arr[:W])
        vwap = cum_pv / (cum_v + 1e-8)
        vwap_dev = (close_arr[:W] - vwap) / (vwap + 1e-8)

        rsi_arr = np.full(W, 0.5, dtype=np.float64)
        for i in range(15, W):
            d = np.diff(close_arr[max(i - 14, 0):i + 1])
            if len(d) > 0:
                g = np.where(d > 0, d, 0).mean()
                l_ = np.where(d < 0, -d, 0).mean()
                rsi_arr[i] = g / (g + l_ + 1e-8)

        minute_feats = np.stack(
            [rets, vol_ratio, hl_range, vwap_dev, rsi_arr], axis=1
        ).astype(np.float32)

        # ── 10 维日特征 (严格对齐 v24_gen_plan_data.py line 93~107) ──
        open_price = float(close_arr[0])
        price_30m = float(close_arr[W - 1])
        day_feat = np.zeros(10, dtype=np.float32)

        # [0] 30分钟相对 prev_close 涨跌幅
        day_feat[0] = (price_30m - prev_close) / (prev_close + 1e-8)
        # [1] 30分钟相对开盘涨跌幅
        day_feat[1] = (price_30m - open_price) / (open_price + 1e-8)
        # [2] 分钟收益率 std
        day_feat[2] = float(np.std(rets[1:])) if len(rets) > 2 else 0.0
        # [3] 前30分钟成交量占全天比例 (执行时用近似: 假设全天 vol = w_vol.sum() * 240/W)
        # 这样 day_feat[3] 恒等于 W/240 ≈ 0.125, 与训练集中位数一致
        # 注意: 这不是完美等价, 但优于当前的恒等 1.0 bug
        day_feat[3] = float(W) / float(FULL_DAY_MINUTES)
        # [4] 最后一分钟 vol_ratio
        day_feat[4] = float(vol_ratio[-1]) if len(vol_ratio) > 0 else 1.0
        # [5] 前30分钟振幅 (clip to [0, 0.2] / 0.2)
        day_feat[5] = float(np.clip(
            (high_arr[:W].max() - low_arr[:W].min()) / (prev_close + 1e-8),
            0, 0.2,
        )) / 0.2
        # [6] 买卖方向: 训练时 is_buy=1 → day_feat[6]=0, is_buy=0 → day_feat[6]=1
        #     (见 v24_gen_plan_data.py 为买入和卖出分别生成时的设置)
        day_feat[6] = 0.0 if is_buy else 1.0
        # [7] warmup期间涨停bar比例 (与训练对齐: 训练用boolean $limit_up均值)
        day_feat[7] = float(np.sum(close_arr[:W] >= lu_arr[:W] - 1e-4) / W)
        # [8] warmup期间跌停bar比例 (与训练对齐: 训练用boolean $limit_down均值)
        day_feat[8] = float(np.sum(close_arr[:W] <= ld_arr[:W] + 1e-4) / W)
        # [9] 距离涨停价比例
        day_feat[9] = (prev_close * (1 + limit_pct) - price_30m) / (price_30m + 1e-8)

        if np.isnan(day_feat).any() or np.isinf(day_feat).any():
            raise _DataMissingError(
                f"day_feat 包含 NaN/Inf: {day_feat.tolist()}"
            )
        if np.isnan(minute_feats).any() or np.isinf(minute_feats).any():
            raise _DataMissingError(
                f"minute_feats 包含 NaN/Inf"
            )

        return minute_feats, day_feat

    def _generate_plan_for_stock(self, stock_id, direction):
        """在 t=30 时刻为 stock_id 生成 210 维 plan 分布。

        任一关键数据缺失 → raise _DataMissingError, 由调用方记录并降级。
        """
        limit_pct = _get_limit_pct(stock_id)

        # 从 warmup 期间累积的 buffer 读取前 30 分钟数据
        close_list = self._v24_close_buf.get(stock_id, [])
        vol_list = self._v24_vol_buf.get(stock_id, [])
        high_list = self._v24_high_buf.get(stock_id, [])
        low_list = self._v24_low_buf.get(stock_id, [])
        lu_list = self._v24_lu_buf.get(stock_id, [])
        ld_list = self._v24_ld_buf.get(stock_id, [])
        pc_list = self._v24_pc_buf.get(stock_id, [])

        W = self._warmup_minutes
        if (len(close_list) < W or len(vol_list) < W or len(high_list) < W
                or len(low_list) < W or len(lu_list) < W or len(ld_list) < W
                or len(pc_list) < W):
            raise _DataMissingError(
                f"warmup buffer 不足 {W} 根 "
                f"(close={len(close_list)}, vol={len(vol_list)}, "
                f"high={len(high_list)}, low={len(low_list)}, "
                f"lu={len(lu_list)}, ld={len(ld_list)}, pc={len(pc_list)})"
            )

        close_arr = np.asarray(close_list[:W], dtype=np.float64)
        vol_arr = np.asarray(vol_list[:W], dtype=np.float64)
        high_arr = np.asarray(high_list[:W], dtype=np.float64)
        low_arr = np.asarray(low_list[:W], dtype=np.float64)
        lu_arr = np.asarray(lu_list[:W], dtype=np.float64)
        ld_arr = np.asarray(ld_list[:W], dtype=np.float64)
        pc_arr = np.asarray(pc_list[:W], dtype=np.float64)

        # 停牌/一字板检测 (与训练 should_skip_stock_day 对齐)
        if (high_arr.max() - low_arr.min()) < 1e-6:
            if vol_arr.sum() < 1e-6:
                raise _DataMissingError("suspended (high==low, zero volume)")
            else:
                # 一字板 (涨停/跌停锁定): P0 已在 warmup 期间处理了顺向全量执行
                # (买入+跌停→全量买入, 卖出+涨停→全量卖出)
                # 反向 (买入涨停/卖出跌停) 降级 TWAP, 每分钟尝试等待开板
                raise _DataMissingError("limit_locked (一字板, high==low, vol>0)")
        elif vol_arr.sum() < 1e-6:
            raise _DataMissingError("zero_volume in warmup period")

        # prev_close: 直接从 $prev_close bin 读取 (真实昨收价, 元)
        # 使用前 30 分钟中位数提高鲁棒性 (同一天内应全部相同)
        prev_close = float(np.median(pc_arr))
        if prev_close <= 1e-4:
            raise _DataMissingError(f"invalid prev_close: {prev_close}")

        open_price = float(close_arr[0])

        # gap_ratio 计算
        gap_pct = (open_price - prev_close) / prev_close
        gap_pct = float(np.clip(gap_pct, -0.20, 0.20))
        gap_ratio = gap_pct / limit_pct

        is_buy = (direction == Order.BUY)

        minute_feats, day_feat = self._build_v24_inputs(
            close_arr, vol_arr, high_arr, low_arr, lu_arr, ld_arr,
            prev_close, is_buy, limit_pct,
        )

        gap_bucket = _gap_ratio_to_bucket(gap_ratio)
        gap_ratio_signed = gap_ratio * (1.0 if is_buy else -1.0)

        with torch.no_grad():
            mf = torch.from_numpy(minute_feats).unsqueeze(0).to(self._device)
            df = torch.from_numpy(day_feat).unsqueeze(0).to(self._device)
            gb = torch.LongTensor([gap_bucket]).to(self._device)
            gr = torch.FloatTensor([gap_ratio]).to(self._device)
            grs = torch.FloatTensor([gap_ratio_signed]).to(self._device)
            lp = torch.FloatTensor([limit_pct]).to(self._device)
            ib = torch.FloatTensor([1.0 if is_buy else 0.0]).to(self._device)
            plan = self._plan_model(mf, df, gb, gr, grs, lp, ib)[0].cpu().numpy()

        plan = plan.astype(np.float64)
        if np.isnan(plan).any() or plan.sum() <= 1e-8:
            raise _DataMissingError(
                f"plan 输出异常 sum={plan.sum()}, nan={np.isnan(plan).any()}"
            )
        logger.info(
            "[TailTWAPv24] generated plan stock=%s gap_ratio=%.4f bucket=%d is_buy=%s",
            stock_id, gap_ratio, gap_bucket, is_buy,
        )
        return plan

    # ------------------------------------------------------------
    # 主决策入口
    # ------------------------------------------------------------
    def generate_trade_decision(self, execute_result=None):
        if len(self.outer_trade_decision.get_decision()) == 0:
            return TradeDecisionWO(order_list=[], strategy=self)

        trade_step = self.trade_calendar.get_trade_step()
        start_idx, end_idx = get_start_end_idx(
            self.trade_calendar, self.outer_trade_decision
        )
        trade_len = end_idx - start_idx + 1

        if trade_step < start_idx or trade_step > end_idx:
            return TradeDecisionWO(order_list=[], strategy=self)

        rel_trade_step = trade_step - start_idx

        # 更新已执行数量 (同父类)
        if execute_result is not None:
            for order, _, _, _ in execute_result:
                if order.stock_id in self.trade_amount_remain:
                    self.trade_amount_remain[order.stock_id] -= order.deal_amount
                if order.stock_id in self._realloc_extra:
                    self._realloc_extra[order.stock_id] = max(
                        0, self._realloc_extra[order.stock_id] - order.deal_amount
                    )

        trade_start_time, trade_end_time = self.trade_calendar.get_step_time(
            trade_step
        )

        is_last_step = (rel_trade_step == trade_len - 1)

        # ================================================
        # P4: 尾盘触发闲置资金再分配 (仅一次) — 完全复用父类逻辑
        # ================================================
        if rel_trade_step >= self._realloc_offset and not self._realloc_done:
            self._realloc_done = True
            if self._unfilled_handler == "TAIL_SUBSTITUTE":
                self._do_realloc_substitute(trade_start_time, trade_end_time)
            else:
                self._do_realloc(trade_start_time, trade_end_time)

        order_list = []
        for order in self.outer_trade_decision.get_decision():
            # 停牌检测
            if self.trade_exchange.check_stock_suspended(
                stock_id=order.stock_id,
                start_time=trade_start_time,
                end_time=trade_end_time,
            ):
                continue

            amount_remain = self.trade_amount_remain[order.stock_id]
            if amount_remain <= 1e-5:
                continue

            # 父类 reset 已初始化 self._original_amount[order.stock_id] = order.amount
            # 此处不再重复记录, 避免覆盖原始值

            # ================================================
            # P0: 涨跌停全量执行 (全天有效, 复用父类结构)
            # ================================================
            if order.stock_id not in self._p0_done:
                try:
                    close_price = self.trade_exchange.get_close(
                        order.stock_id,
                        trade_start_time,
                        trade_end_time,
                        method="ts_data_last",
                    )
                    if (
                        close_price is not None
                        and not np.isnan(close_price)
                        and close_price > 0
                    ):
                        limit_up = self.trade_exchange.quote.get_data(
                            order.stock_id, trade_start_time, trade_end_time,
                            field="$up_limit_price", method="ts_data_last",
                        )
                        limit_down = self.trade_exchange.quote.get_data(
                            order.stock_id, trade_start_time, trade_end_time,
                            field="$down_limit_price", method="ts_data_last",
                        )

                        if (
                            order.direction == Order.BUY
                            and limit_down is not None
                            and not np.isnan(limit_down)
                            and close_price <= limit_down
                        ):
                            order_list.append(Order(
                                stock_id=order.stock_id,
                                amount=amount_remain,
                                start_time=trade_start_time,
                                end_time=trade_end_time,
                                direction=order.direction,
                            ))
                            self._p0_done.add(order.stock_id)
                            continue

                        if (
                            order.direction == Order.SELL
                            and limit_up is not None
                            and not np.isnan(limit_up)
                            and close_price >= limit_up
                        ):
                            order_list.append(Order(
                                stock_id=order.stock_id,
                                amount=amount_remain,
                                start_time=trade_start_time,
                                end_time=trade_end_time,
                                direction=order.direction,
                            ))
                            self._p0_done.add(order.stock_id)
                            continue
                except (KeyError, IndexError) as e:
                    logger.warning(
                        "[TailTWAPv24] P0 涨跌停检测失败 stock=%s: %s",
                        order.stock_id, e,
                    )

            # ================================================
            # 计算本分钟执行量 — v24 plan 贯穿 t=30~239
            # ================================================
            amount_delta_target = 0.0

            if rel_trade_step < self._warmup_minutes:
                # --- t=0~29: WARMUP 均匀预执行 warmup_alloc ---
                # 同时采集分钟数据供 t=30 plan 生成使用
                self._collect_bar(order.stock_id, trade_start_time, trade_end_time)

                warmup_total = self._original_amount[order.stock_id] * self._warmup_alloc
                amount_delta = warmup_total / self._warmup_minutes
                # 最后一根 warmup: 补齐本段累计量
                if rel_trade_step == self._warmup_minutes - 1:
                    target_executed = warmup_total
                    already = self._original_amount[order.stock_id] - amount_remain
                    amount_delta = max(target_executed - already, 0.0)

                _unit = self.trade_exchange.get_amount_of_trade_unit(
                    stock_id=order.stock_id,
                    start_time=order.start_time,
                    end_time=order.end_time,
                )
                if _unit is not None and _unit > 0:
                    amount_delta_target = min(
                        np.round(amount_delta / _unit) * _unit,
                        amount_remain,
                    )
                else:
                    amount_delta_target = min(amount_delta, amount_remain)
            else:
                # --- t=30~239: v24 plan 贯穿全程, extra 叠加 ---
                # 首次生成 plan (仅在 t=30 尝试一次)
                if not self._v24_plan_generated.get(order.stock_id, False):
                    self._v24_plan_generated[order.stock_id] = True
                    try:
                        plan = self._generate_plan_for_stock(
                            order.stock_id, order.direction,
                        )
                        self._v24_plans[order.stock_id] = plan
                    except _DataMissingError as e:
                        logger.warning(
                            "[TailTWAPv24] plan 生成失败, 降级 TWAP stock=%s: %s",
                            order.stock_id, e,
                        )
                        self._v24_plan_failed.add(order.stock_id)

                plan = self._v24_plans.get(order.stock_id)
                plan_idx = rel_trade_step - self._warmup_minutes  # [0, 210)

                # remaining_minutes_in_day = 从当前步 (含) 到 end 的步数
                remaining_day_steps = end_idx - trade_step + 1
                if remaining_day_steps <= 0:
                    continue

                # ── 计算 base amount_delta (来自原 remaining) ──
                if order.stock_id in self._v24_plan_failed or plan is None:
                    # 降级 TWAP: 剩余均匀分配
                    base_delta = amount_remain / remaining_day_steps
                elif 0 <= plan_idx < len(plan):
                    remaining_weight = float(plan[plan_idx:].sum())
                    if remaining_weight > 1e-8:
                        frac = float(plan[plan_idx]) / remaining_weight
                        frac = float(np.clip(frac, 0.0, 1.0))
                        base_delta = amount_remain * frac
                    else:
                        # plan 尾部全 0 (异常): 明确告警 + 均匀兜底
                        logger.warning(
                            "[TailTWAPv24] plan 尾部权重为 0, 剩余均匀执行 "
                            "stock=%s plan_idx=%d remaining=%d",
                            order.stock_id, plan_idx, remaining_day_steps,
                        )
                        base_delta = amount_remain / remaining_day_steps
                else:
                    # plan_idx 越界 (不应发生, plan_len=210 覆盖 t=30~239)
                    logger.warning(
                        "[TailTWAPv24] plan_idx 越界 stock=%s plan_idx=%d, 均匀兜底",
                        order.stock_id, plan_idx,
                    )
                    base_delta = amount_remain / remaining_day_steps

                # ── 叠加 extra 追加量 (14:55 后才有值, 仅 BUY 方向) ──
                extra = self._realloc_extra.get(order.stock_id, 0)
                extra_delta = 0.0
                if extra > 1e-5 and order.direction == Order.BUY:
                    extra_delta = extra / max(remaining_day_steps, 1)

                amount_delta = base_delta + extra_delta

                # 最后一根: 强制清空 remaining + extra
                if is_last_step:
                    amount_delta = amount_remain + self._realloc_extra.get(
                        order.stock_id, 0
                    )

                _unit = self.trade_exchange.get_amount_of_trade_unit(
                    stock_id=order.stock_id,
                    start_time=order.start_time,
                    end_time=order.end_time,
                )
                max_amount = amount_remain + self._realloc_extra.get(order.stock_id, 0)
                if _unit is not None and _unit > 0:
                    amount_delta_target = min(
                        np.round(amount_delta / _unit) * _unit,
                        max_amount,
                    )
                else:
                    amount_delta_target = min(amount_delta, max_amount)

            if amount_delta_target > 1e-5:
                order_list.append(Order(
                    stock_id=order.stock_id,
                    amount=amount_delta_target,
                    start_time=trade_start_time,
                    end_time=trade_end_time,
                    direction=order.direction,
                ))

        # ================================================
        # TAIL_SUBSTITUTE: 为备选股生成买入订单 (尾盘段, 复用父类逻辑)
        # 备选股不在 outer_trade_decision 中, 没有 v24 plan, 用均匀 TWAP
        # ================================================
        if (rel_trade_step >= TAIL_START_OFFSET
                and self._unfilled_handler == "TAIL_SUBSTITUTE"):
            existing_sids = {o.stock_id for o in order_list}
            for sid, extra in self._realloc_extra.items():
                if extra <= 1e-5 or sid in existing_sids:
                    continue
                # 备选股不在原始订单中, 需检查可交易性
                if self.trade_exchange.check_stock_suspended(
                    stock_id=sid,
                    start_time=trade_start_time,
                    end_time=trade_end_time,
                ):
                    continue
                remaining_steps = end_idx - trade_step + 1
                if remaining_steps <= 0:
                    continue
                if is_last_step:
                    amount_delta_target = extra
                else:
                    amount_delta_target = extra / remaining_steps
                _unit = self.trade_exchange.get_amount_of_trade_unit(
                    stock_id=sid,
                    start_time=trade_start_time,
                    end_time=trade_end_time,
                )
                if _unit is not None and _unit > 0:
                    amount_delta_target = min(
                        np.round(amount_delta_target / _unit) * _unit, extra
                    )
                if amount_delta_target > 1e-5:
                    order_list.append(Order(
                        stock_id=sid,
                        amount=amount_delta_target,
                        start_time=trade_start_time,
                        end_time=trade_end_time,
                        direction=Order.BUY,
                    ))

        return TradeDecisionWO(order_list=order_list, strategy=self)
