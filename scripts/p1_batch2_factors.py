#!/usr/bin/env python3
"""P1 第二批因子：补齐每类到 10 个。

行业板块+5, 统计分布+7, 质量+8, 高频衍生+6, 残差Beta+6, 多源融合+5
"""
import asyncio
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
os.environ.setdefault("TDX_DB_HOST", "127.0.0.1")
os.environ.setdefault("TDX_DB_PASSWORD", "lc78080808")

from backend.services.manual_factor_service import ManualFactorService

FACTORS = {

    # ══════════════════════════════════════════
    # A. 行业板块 补充 (sector_data)
    # ══════════════════════════════════════════

    "m_industry_vol_ratio": {
        "description": "行业成交量比率：个股成交量/行业成交量占比的10日变化，资金关注度迁移信号",
        "code": '''import pandas as pd
import numpy as np
FACTOR_NAME = "m_industry_vol_ratio"
pv = pd.read_hdf("daily_pv.h5")
sector = pd.read_hdf("sector_data.h5")
vol_ratio = pv["volume"] / sector["sw2_vol"].replace(0, np.nan)
vol_ratio = vol_ratio.replace([np.inf, -np.inf], np.nan)
factor = vol_ratio.groupby(level="instrument").pct_change(10)
factor = factor.replace([np.inf, -np.inf], np.nan)
factor = factor.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_industry_pb_deviation": {
        "description": "个股PB与行业PB偏离度：log(个股PB/行业PB)，行业内相对PB估值信号",
        "code": '''import pandas as pd
import numpy as np
FACTOR_NAME = "m_industry_pb_deviation"
db = pd.read_hdf("daily_basic.h5")
sector = pd.read_hdf("sector_data.h5")
ratio = db["db_pb"] / sector["sw2_pb"].replace(0, np.nan)
ratio = ratio.replace([np.inf, -np.inf], np.nan)
factor = -np.log(ratio.clip(lower=0.01))
factor = factor.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_industry_relative_strength_5d": {
        "description": "个股5日涨幅相对行业排名：截面行业中性短期强度",
        "code": '''import pandas as pd
import numpy as np
FACTOR_NAME = "m_industry_relative_strength_5d"
pv = pd.read_hdf("daily_pv.h5")
sector = pd.read_hdf("sector_data.h5")
stock_ret = pv["close"].groupby(level="instrument").pct_change(5)
ind_ret = sector["sw2_close"].groupby(level="instrument").pct_change(5)
excess = stock_ret - ind_ret
factor = excess.groupby(level="datetime").rank(pct=True)
factor = factor.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_industry_mf_large_divergence": {
        "description": "行业大单资金流背离：行业大单净流入vs行业涨跌的背离度",
        "code": '''import pandas as pd
import numpy as np
FACTOR_NAME = "m_industry_mf_large_divergence"
sector = pd.read_hdf("sector_data.h5")
big_net = (sector["sw2_mf_buy_lg_amt"] - sector["sw2_mf_sell_lg_amt"]) + \
          (sector["sw2_mf_buy_elg_amt"] - sector["sw2_mf_sell_elg_amt"])
big_net_rank = big_net.groupby(level="datetime").rank(pct=True)
ret_rank = sector["sw2_pct_change"].groupby(level="datetime").rank(pct=True)
factor = big_net_rank - ret_rank
factor = factor.groupby(level="instrument").rolling(5, min_periods=3).mean().droplevel(0)
factor = factor.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_industry_reversal_20d": {
        "description": "行业20日反转：行业近20日涨幅反向，行业轮动均值回复信号",
        "code": '''import pandas as pd
import numpy as np
FACTOR_NAME = "m_industry_reversal_20d"
sector = pd.read_hdf("sector_data.h5")
ind_ret_20d = sector["sw2_close"].groupby(level="instrument").pct_change(20)
factor = -ind_ret_20d
factor = factor.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    # ══════════════════════════════════════════
    # B. 统计分布 补充 (STAT)
    # ══════════════════════════════════════════

    "m_return_kurtosis_20d": {
        "description": "20日收益峰度(反向)：高峰度=厚尾=极端风险高，低峰度溢价",
        "code": '''import pandas as pd
import numpy as np
FACTOR_NAME = "m_return_kurtosis_20d"
pv = pd.read_hdf("daily_pv.h5")
ret = pv["close"].groupby(level="instrument").pct_change(1)
kurt = ret.groupby(level="instrument").rolling(20, min_periods=15).kurt().droplevel(0)
factor = -kurt
factor = factor.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_positive_return_ratio_20d": {
        "description": "20日正收益比率：正收益天数占比，衡量趋势稳定性",
        "code": '''import pandas as pd
import numpy as np
FACTOR_NAME = "m_positive_return_ratio_20d"
pv = pd.read_hdf("daily_pv.h5")
ret = pv["close"].groupby(level="instrument").pct_change(1)
pos = (ret > 0).astype(float)
factor = pos.groupby(level="instrument").rolling(20, min_periods=15).mean().droplevel(0)
factor = factor.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_downside_vol_ratio_20d": {
        "description": "下行波动比率：负收益波动率/总波动率，越高=下行风险越集中",
        "code": '''import pandas as pd
import numpy as np
FACTOR_NAME = "m_downside_vol_ratio_20d"
pv = pd.read_hdf("daily_pv.h5")
ret = pv["close"].groupby(level="instrument").pct_change(1)
neg_ret = ret.clip(upper=0)
down_vol = neg_ret.groupby(level="instrument").rolling(20, min_periods=15).std().droplevel(0)
total_vol = ret.groupby(level="instrument").rolling(20, min_periods=15).std().droplevel(0)
ratio = down_vol / total_vol.replace(0, np.nan)
factor = -ratio
factor = factor.replace([np.inf, -np.inf], np.nan)
factor = factor.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_vol_of_vol_20d": {
        "description": "波动率的波动率(反向)：20日波动率的10日标准差，衡量波动率稳定性",
        "code": '''import pandas as pd
import numpy as np
FACTOR_NAME = "m_vol_of_vol_20d"
pv = pd.read_hdf("daily_pv.h5")
ret = pv["close"].groupby(level="instrument").pct_change(1)
vol = ret.groupby(level="instrument").rolling(5, min_periods=3).std().droplevel(0)
vol_of_vol = vol.groupby(level="instrument").rolling(20, min_periods=15).std().droplevel(0)
factor = -vol_of_vol
factor = factor.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_up_down_vol_asymmetry": {
        "description": "涨跌量不对称性：上涨日均成交量/下跌日均成交量-1，正=上涨放量",
        "code": '''import pandas as pd
import numpy as np
FACTOR_NAME = "m_up_down_vol_asymmetry"
pv = pd.read_hdf("daily_pv.h5")
ret = pv["close"].groupby(level="instrument").pct_change(1)
vol = pv["volume"]
up_mask = ret > 0
down_mask = ret < 0
up_vol = (vol * up_mask).groupby(level="instrument").rolling(20, min_periods=10).sum().droplevel(0)
down_vol = (vol * down_mask).groupby(level="instrument").rolling(20, min_periods=10).sum().droplevel(0)
up_days = up_mask.astype(float).groupby(level="instrument").rolling(20, min_periods=10).sum().droplevel(0)
down_days = down_mask.astype(float).groupby(level="instrument").rolling(20, min_periods=10).sum().droplevel(0)
avg_up_vol = up_vol / up_days.replace(0, np.nan)
avg_down_vol = down_vol / down_days.replace(0, np.nan)
factor = avg_up_vol / avg_down_vol.replace(0, np.nan) - 1
factor = factor.replace([np.inf, -np.inf], np.nan)
factor = factor.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_return_autocorr_5d": {
        "description": "5日收益自相关系数(反向)：高自相关=趋势延续，低自相关=随机游走，A股反转为主",
        "code": '''import pandas as pd
import numpy as np
FACTOR_NAME = "m_return_autocorr_5d"
pv = pd.read_hdf("daily_pv.h5")
ret = pv["close"].groupby(level="instrument").pct_change(1)
ret_lag1 = ret.groupby(level="instrument").shift(1)
aligned = pd.DataFrame({"r": ret, "r1": ret_lag1}).dropna()
autocorr = aligned["r"].groupby(level="instrument").rolling(20, min_periods=15).corr(aligned["r1"]).droplevel(0)
factor = -autocorr
factor = factor.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_max_return_20d": {
        "description": "20日最大单日涨幅(反向)：Bali et al.(2011)最大日收益彩票效应，高值被高估",
        "code": '''import pandas as pd
import numpy as np
FACTOR_NAME = "m_max_return_20d"
pv = pd.read_hdf("daily_pv.h5")
ret = pv["close"].groupby(level="instrument").pct_change(1)
max_ret = ret.groupby(level="instrument").rolling(20, min_periods=15).max().droplevel(0)
factor = -max_ret
factor = factor.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    # ══════════════════════════════════════════
    # C. 质量因子 补充 (QUAL)
    # ══════════════════════════════════════════

    "m_roe_stability_4q": {
        "description": "ROE稳定性：bb_npr的60日波动率反向，盈利越稳定越好",
        "code": '''import pandas as pd
import numpy as np
FACTOR_NAME = "m_roe_stability_4q"
bb = pd.read_hdf("bak_basic.h5")
npr = bb["bb_npr"]
vol = npr.groupby(level="instrument").rolling(60, min_periods=40).std().droplevel(0)
factor = -vol
factor = factor.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_revenue_growth_accel": {
        "description": "营收增速加速度：bb_rev_yoy的20日变化，正=增速提升",
        "code": '''import pandas as pd
import numpy as np
FACTOR_NAME = "m_revenue_growth_accel"
bb = pd.read_hdf("bak_basic.h5")
rev_yoy = bb["bb_rev_yoy"]
factor = rev_yoy.groupby(level="instrument").diff(20)
factor = factor.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_earnings_surprise": {
        "description": "盈利惊喜：个股净利润增速vs截面中位数的偏差，超预期信号",
        "code": '''import pandas as pd
import numpy as np
FACTOR_NAME = "m_earnings_surprise"
bb = pd.read_hdf("bak_basic.h5")
profit_yoy = bb["bb_profit_yoy"]
median = profit_yoy.groupby(level="datetime").transform("median")
factor = profit_yoy - median
factor = factor.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_eps_momentum_20d": {
        "description": "EPS动量：bb_eps的20日变化率，盈利改善趋势",
        "code": '''import pandas as pd
import numpy as np
FACTOR_NAME = "m_eps_momentum_20d"
bb = pd.read_hdf("bak_basic.h5")
eps = bb["bb_eps"]
factor = eps.groupby(level="instrument").pct_change(20)
factor = factor.replace([np.inf, -np.inf], np.nan)
factor = factor.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_bvps_growth_20d": {
        "description": "每股净资产增长率：bb_bvps的20日变化率，资产质量改善",
        "code": '''import pandas as pd
import numpy as np
FACTOR_NAME = "m_bvps_growth_20d"
bb = pd.read_hdf("bak_basic.h5")
bvps = bb["bb_bvps"]
factor = bvps.groupby(level="instrument").pct_change(20)
factor = factor.replace([np.inf, -np.inf], np.nan)
factor = factor.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_profit_to_asset_ratio": {
        "description": "盈利资产比：bb_eps*bb_total_share/bb_total_assets截面排名，ROA代理",
        "code": '''import pandas as pd
import numpy as np
FACTOR_NAME = "m_profit_to_asset_ratio"
bb = pd.read_hdf("bak_basic.h5")
db = pd.read_hdf("daily_basic.h5")
profit_proxy = bb["bb_eps"] * db["db_total_share"]
asset = bb["bb_total_assets"].replace(0, np.nan)
roa = profit_proxy / asset
roa = roa.replace([np.inf, -np.inf], np.nan)
factor = roa.groupby(level="datetime").rank(pct=True)
factor = factor.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_pe_dyn_vs_static_gap": {
        "description": "动态PE与静态PE差异：(db_pe-bb_pe_dyn)/db_pe，差异大=盈利预期变动",
        "code": '''import pandas as pd
import numpy as np
FACTOR_NAME = "m_pe_dyn_vs_static_gap"
db = pd.read_hdf("daily_basic.h5")
bb = pd.read_hdf("bak_basic.h5")
pe_static = db["db_pe"]
pe_dyn = bb["bb_pe_dyn"]
gap = (pe_static - pe_dyn) / pe_static.abs().replace(0, np.nan)
gap = gap.replace([np.inf, -np.inf], np.nan)
factor = gap.groupby(level="datetime").rank(pct=True)
factor = factor.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_undp_change_20d": {
        "description": "未分配利润变化率：bb_undp的20日变化率，利润留存能力",
        "code": '''import pandas as pd
import numpy as np
FACTOR_NAME = "m_undp_change_20d"
bb = pd.read_hdf("bak_basic.h5")
undp = bb["bb_undp"]
factor = undp.groupby(level="instrument").pct_change(20)
factor = factor.replace([np.inf, -np.inf], np.nan)
factor = factor.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_liquid_asset_ratio_change": {
        "description": "流动资产占比变化：bb_liquid_assets/bb_total_assets的20日变化，流动性改善",
        "code": '''import pandas as pd
import numpy as np
FACTOR_NAME = "m_liquid_asset_ratio_change"
bb = pd.read_hdf("bak_basic.h5")
ratio = bb["bb_liquid_assets"] / bb["bb_total_assets"].replace(0, np.nan)
ratio = ratio.replace([np.inf, -np.inf], np.nan)
factor = ratio.groupby(level="instrument").diff(20)
factor = factor.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    # ══════════════════════════════════════════
    # D. 高频衍生 补充 (TECH)
    # ══════════════════════════════════════════

    "m_intraday_range_ratio_5d": {
        "description": "日内波动幅度比5日均值：(high-low)/close的5日均值，衡量日内波动集中度",
        "code": '''import pandas as pd
import numpy as np
FACTOR_NAME = "m_intraday_range_ratio_5d"
pv = pd.read_hdf("daily_pv.h5")
range_ratio = (pv["high"] - pv["low"]) / pv["close"].replace(0, np.nan)
factor = range_ratio.groupby(level="instrument").rolling(5, min_periods=3).mean().droplevel(0)
factor = -factor
factor = factor.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_price_efficiency_10d": {
        "description": "价格效率：abs(close[t]-close[t-10])/(sum of abs daily changes)，趋势效率",
        "code": '''import pandas as pd
import numpy as np
FACTOR_NAME = "m_price_efficiency_10d"
pv = pd.read_hdf("daily_pv.h5")
close = pv["close"]
net_move = (close - close.groupby(level="instrument").shift(10)).abs()
daily_chg = close.groupby(level="instrument").diff(1).abs()
sum_daily = daily_chg.groupby(level="instrument").rolling(10, min_periods=8).sum().droplevel(0)
factor = net_move / sum_daily.replace(0, np.nan)
factor = factor.replace([np.inf, -np.inf], np.nan)
factor = factor.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_volume_ma_ratio": {
        "description": "成交量均线比：5日均量/20日均量-1，量能放大=短期关注度提升",
        "code": '''import pandas as pd
import numpy as np
FACTOR_NAME = "m_volume_ma_ratio"
pv = pd.read_hdf("daily_pv.h5")
vol = pv["volume"]
ma5 = vol.groupby(level="instrument").rolling(5, min_periods=3).mean().droplevel(0)
ma20 = vol.groupby(level="instrument").rolling(20, min_periods=15).mean().droplevel(0)
factor = ma5 / ma20.replace(0, np.nan) - 1
factor = factor.replace([np.inf, -np.inf], np.nan)
factor = factor.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_high_low_channel_pos": {
        "description": "价格在20日高低通道中的位置：(close-low20)/(high20-low20)，趋势位置",
        "code": '''import pandas as pd
import numpy as np
FACTOR_NAME = "m_high_low_channel_pos"
pv = pd.read_hdf("daily_pv.h5")
close = pv["close"]
high20 = close.groupby(level="instrument").rolling(20, min_periods=15).max().droplevel(0)
low20 = close.groupby(level="instrument").rolling(20, min_periods=15).min().droplevel(0)
channel = high20 - low20
factor = (close - low20) / channel.replace(0, np.nan)
factor = factor.replace([np.inf, -np.inf], np.nan)
factor = factor.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_gap_frequency_20d": {
        "description": "20日跳空频率(反向)：open!=prev_close的天数占比，高跳空=高波动=风险",
        "code": '''import pandas as pd
import numpy as np
FACTOR_NAME = "m_gap_frequency_20d"
pv = pd.read_hdf("daily_pv.h5")
prev_close = pv["close"].groupby(level="instrument").shift(1)
gap = (pv["open"] / prev_close - 1).abs()
has_gap = (gap > 0.005).astype(float)
factor = -has_gap.groupby(level="instrument").rolling(20, min_periods=15).mean().droplevel(0)
factor = factor.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_amount_concentration_10d": {
        "description": "成交额集中度：10日内最大成交日金额/总金额，高集中度=异常交易日",
        "code": '''import pandas as pd
import numpy as np
FACTOR_NAME = "m_amount_concentration_10d"
pv = pd.read_hdf("daily_pv.h5")
amt = pv["amount"]
max_amt = amt.groupby(level="instrument").rolling(10, min_periods=8).max().droplevel(0)
sum_amt = amt.groupby(level="instrument").rolling(10, min_periods=8).sum().droplevel(0)
factor = -max_amt / sum_amt.replace(0, np.nan)
factor = factor.replace([np.inf, -np.inf], np.nan)
factor = factor.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    # ══════════════════════════════════════════
    # E. 残差/Beta 补充 (CORR)
    # ══════════════════════════════════════════

    "m_beta_change_20d": {
        "description": "Beta变化率：60日Beta的20日变化，Beta上升=系统风险增加",
        "code": '''import pandas as pd
import numpy as np
FACTOR_NAME = "m_beta_change_20d"
pv = pd.read_hdf("daily_pv.h5")
sector = pd.read_hdf("sector_data.h5")
stock_ret = pv["close"].groupby(level="instrument").pct_change(1)
mkt_ret = sector["sw2_pct_change"] / 100.0
aligned = pd.DataFrame({"s": stock_ret, "m": mkt_ret}).dropna()
corr = aligned["s"].groupby(level="instrument").rolling(60, min_periods=40).corr(aligned["m"]).droplevel(0)
stock_std = stock_ret.groupby(level="instrument").rolling(60, min_periods=40).std().droplevel(0)
mkt_std = mkt_ret.groupby(level="instrument").rolling(60, min_periods=40).std().droplevel(0)
beta = corr * (stock_std / mkt_std.replace(0, np.nan))
beta = beta.replace([np.inf, -np.inf], np.nan)
factor = -beta.groupby(level="instrument").diff(20)
factor = factor.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_residual_vol_change_20d": {
        "description": "残差波动率20日变化：特质风险的变化趋势，上升=风险增加",
        "code": '''import pandas as pd
import numpy as np
FACTOR_NAME = "m_residual_vol_change_20d"
pv = pd.read_hdf("daily_pv.h5")
sector = pd.read_hdf("sector_data.h5")
stock_ret = pv["close"].groupby(level="instrument").pct_change(1)
mkt_ret = sector["sw2_pct_change"] / 100.0
residual = stock_ret - mkt_ret
res_vol = residual.groupby(level="instrument").rolling(60, min_periods=40).std().droplevel(0)
factor = -res_vol.groupby(level="instrument").diff(20)
factor = factor.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_corr_volume_return_20d": {
        "description": "量价相关性：20日成交量vs收益率的相关系数，正相关=量价齐升",
        "code": '''import pandas as pd
import numpy as np
FACTOR_NAME = "m_corr_volume_return_20d"
pv = pd.read_hdf("daily_pv.h5")
ret = pv["close"].groupby(level="instrument").pct_change(1)
vol = pv["volume"]
aligned = pd.DataFrame({"r": ret, "v": vol}).dropna()
corr = aligned["r"].groupby(level="instrument").rolling(20, min_periods=15).corr(aligned["v"]).droplevel(0)
factor = corr.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_mkt_sensitivity_asymmetry": {
        "description": "市场敏感度不对称性：上涨日beta-下跌日beta，负=下跌时更敏感=风险",
        "code": '''import pandas as pd
import numpy as np
FACTOR_NAME = "m_mkt_sensitivity_asymmetry"
pv = pd.read_hdf("daily_pv.h5")
sector = pd.read_hdf("sector_data.h5")
stock_ret = pv["close"].groupby(level="instrument").pct_change(1)
mkt_ret = sector["sw2_pct_change"] / 100.0
up_mask = mkt_ret > 0
stock_up = stock_ret.where(up_mask)
stock_down = stock_ret.where(~up_mask)
mkt_up = mkt_ret.where(up_mask)
mkt_down = mkt_ret.where(~up_mask)
beta_up_proxy = stock_up.groupby(level="instrument").rolling(40, min_periods=20).mean().droplevel(0)
beta_down_proxy = stock_down.groupby(level="instrument").rolling(40, min_periods=20).mean().droplevel(0)
factor = beta_up_proxy - beta_down_proxy
factor = factor.replace([np.inf, -np.inf], np.nan)
factor = factor.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_excess_return_consistency_20d": {
        "description": "超额收益一致性：20日内个股跑赢行业的天数占比，高一致性=稳定alpha",
        "code": '''import pandas as pd
import numpy as np
FACTOR_NAME = "m_excess_return_consistency_20d"
pv = pd.read_hdf("daily_pv.h5")
sector = pd.read_hdf("sector_data.h5")
stock_ret = pv["close"].groupby(level="instrument").pct_change(1)
mkt_ret = sector["sw2_pct_change"] / 100.0
excess = stock_ret - mkt_ret
outperform = (excess > 0).astype(float)
factor = outperform.groupby(level="instrument").rolling(20, min_periods=15).mean().droplevel(0)
factor = factor.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_corr_decay_5_20": {
        "description": "短长期相关性衰减：5日vs20日与行业相关性的差，正=短期脱钩=特质性增强",
        "code": '''import pandas as pd
import numpy as np
FACTOR_NAME = "m_corr_decay_5_20"
pv = pd.read_hdf("daily_pv.h5")
sector = pd.read_hdf("sector_data.h5")
stock_ret = pv["close"].groupby(level="instrument").pct_change(1)
mkt_ret = sector["sw2_pct_change"] / 100.0
aligned = pd.DataFrame({"s": stock_ret, "m": mkt_ret}).dropna()
corr5 = aligned["s"].groupby(level="instrument").rolling(5, min_periods=4).corr(aligned["m"]).droplevel(0)
corr20 = aligned["s"].groupby(level="instrument").rolling(20, min_periods=15).corr(aligned["m"]).droplevel(0)
factor = corr20 - corr5
factor = factor.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    # ══════════════════════════════════════════
    # F. 多源融合 补充
    # ══════════════════════════════════════════

    "m_value_momentum_quality_3d": {
        "description": "价值-动量-质量三因子融合：低PE排名+正动量排名+高利润增速排名",
        "code": '''import pandas as pd
import numpy as np
FACTOR_NAME = "m_value_momentum_quality_3d"
pv = pd.read_hdf("daily_pv.h5")
db = pd.read_hdf("daily_basic.h5")
bb = pd.read_hdf("bak_basic.h5")
pe_rank = 1.0 - db["db_pe_ttm"].groupby(level="datetime").rank(pct=True)
mom_rank = pv["close"].groupby(level="instrument").pct_change(10).groupby(level="datetime").rank(pct=True)
profit_rank = bb["bb_profit_yoy"].groupby(level="datetime").rank(pct=True)
factor = (pe_rank + mom_rank + profit_rank) / 3.0
factor = factor.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_turnover_mf_divergence": {
        "description": "换手率vs资金流背离：低换手但大单净流入=悄悄建仓，双源融合",
        "code": '''import pandas as pd
import numpy as np
FACTOR_NAME = "m_turnover_mf_divergence"
db = pd.read_hdf("daily_basic.h5")
mf = pd.read_hdf("moneyflow.h5")
tr_rank = 1.0 - db["db_turnover_rate"].groupby(level="datetime").rank(pct=True)
big_net = (mf["mf_lg_buy_amt"] - mf["mf_lg_sell_amt"]) + (mf["mf_elg_buy_amt"] - mf["mf_elg_sell_amt"])
mf_rank = big_net.groupby(level="instrument").rolling(10, min_periods=5).sum().droplevel(0).groupby(level="datetime").rank(pct=True)
factor = tr_rank * mf_rank
factor = factor.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_industry_value_momentum_fusion": {
        "description": "行业价值动量融合：低行业PE排名+行业正动量排名+个股超额排名，四源",
        "code": '''import pandas as pd
import numpy as np
FACTOR_NAME = "m_industry_value_momentum_fusion"
pv = pd.read_hdf("daily_pv.h5")
sector = pd.read_hdf("sector_data.h5")
db = pd.read_hdf("daily_basic.h5")
ind_pe_rank = 1.0 - sector["sw2_pe"].groupby(level="datetime").rank(pct=True)
ind_mom = sector["sw2_close"].groupby(level="instrument").pct_change(10)
ind_mom_rank = ind_mom.groupby(level="datetime").rank(pct=True)
stock_ret = pv["close"].groupby(level="instrument").pct_change(10)
ind_ret = sector["sw2_close"].groupby(level="instrument").pct_change(10)
excess = stock_ret - ind_ret
excess_rank = excess.groupby(level="datetime").rank(pct=True)
factor = (ind_pe_rank + ind_mom_rank + excess_rank) / 3.0
factor = factor.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_chip_value_quality_fusion": {
        "description": "筹码-价值-质量融合：获利盘排名+低PE排名+盈利增速排名，三源",
        "code": '''import pandas as pd
import numpy as np
FACTOR_NAME = "m_chip_value_quality_fusion"
cyq = pd.read_hdf("cyq_perf.h5")
db = pd.read_hdf("daily_basic.h5")
bb = pd.read_hdf("bak_basic.h5")
chip_rank = cyq["cp_winner_rate"].groupby(level="datetime").rank(pct=True)
pe_rank = 1.0 - db["db_pe_ttm"].groupby(level="datetime").rank(pct=True)
profit_rank = bb["bb_profit_yoy"].groupby(level="datetime").rank(pct=True)
factor = (chip_rank + pe_rank + profit_rank) / 3.0
factor = factor.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_five_source_alpha": {
        "description": "五源alpha：量价+基本面+资金流+筹码+行业全维度融合，最大信息覆盖",
        "code": '''import pandas as pd
import numpy as np
FACTOR_NAME = "m_five_source_alpha"
pv = pd.read_hdf("daily_pv.h5")
db = pd.read_hdf("daily_basic.h5")
mf = pd.read_hdf("moneyflow.h5")
cyq = pd.read_hdf("cyq_perf.h5")
sector = pd.read_hdf("sector_data.h5")
d1 = pv["close"].groupby(level="instrument").pct_change(10).groupby(level="datetime").rank(pct=True)
d2 = 1.0 - db["db_pe_ttm"].groupby(level="datetime").rank(pct=True)
big_net = (mf["mf_lg_buy_amt"] - mf["mf_lg_sell_amt"]) + (mf["mf_elg_buy_amt"] - mf["mf_elg_sell_amt"])
d3 = big_net.groupby(level="instrument").rolling(10, min_periods=5).sum().droplevel(0).groupby(level="datetime").rank(pct=True)
d4 = cyq["cp_winner_rate"].groupby(level="instrument").diff(10).groupby(level="datetime").rank(pct=True)
d5 = 1.0 - sector["sw2_pe"].groupby(level="datetime").rank(pct=True)
factor = (d1 + d2 + d3 + d4 + d5) / 5.0
factor = factor.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

}


async def main():
    svc = ManualFactorService()
    t_total = time.time()
    factor_list = list(FACTORS.keys())

    print("=" * 60)
    print(f"Step 1: Save {len(FACTORS)} factors to DB + LLM classify")
    print("=" * 60)
    save_ok = []
    for i, fname in enumerate(factor_list):
        fdef = FACTORS[fname]
        print(f"  [{i+1}/{len(FACTORS)}] {fname}...", end=" ", flush=True)
        try:
            r = await svc.save_factor(fname, fdef["code"], fdef["description"])
            cls = r.get("classification", {}) or {}
            print(f"OK ({cls.get('category', '-')})")
            save_ok.append(fname)
        except Exception as e:
            print(f"FAIL: {e}")

    print(f"\nSaved: {len(save_ok)}/{len(FACTORS)}")

    print("\n" + "=" * 60)
    print("Step 2: Compute independent metrics")
    print("=" * 60)
    batch_size = 5
    all_metrics = {}
    for i in range(0, len(save_ok), batch_size):
        batch = save_ok[i:i+batch_size]
        print(f"  Batch {i//batch_size+1}: {batch[0]}..{batch[-1]}")
        try:
            result = await svc.batch_compute_metrics(factor_names=batch)
            if result.get("success"):
                for fn, w in result.get("factors", {}).items():
                    all_metrics[fn] = w.get("full", {})
                elog = result.get("execution_log", {})
                ok = sum(1 for v in elog.values() if v.get("status") == "ok")
                err = sum(1 for v in elog.values() if v.get("status") == "error")
                print(f"    OK={ok} ERR={err} dur={result.get('total_duration_sec', 0):.0f}s")
                for fn, el in elog.items():
                    if el.get("status") == "error":
                        print(f"    ERR {fn}: {el.get('error', '')[:100]}")
            else:
                print(f"    FAIL: {result.get('error', '')[:150]}")
        except Exception as e:
            print(f"    ERROR: {e}")

    print(f"\n{'='*85}")
    print(f"Total: {time.time()-t_total:.0f}s | Saved: {len(save_ok)} | Metrics: {len(all_metrics)}")
    print(f"\n{'Factor':<45} {'IC':>8} {'ICIR':>8} {'Sharpe':>8} {'AnnRet':>8}")
    print("-" * 85)
    for fname in factor_list:
        fm = all_metrics.get(fname, {})
        ic = fm.get("ic_mean")
        icir = fm.get("icir")
        sh = fm.get("top_sharpe")
        ar = fm.get("top_annual_return")
        ic_s = f"{ic:.4f}" if ic is not None else "-"
        icir_s = f"{icir:.3f}" if icir is not None else "-"
        sh_s = f"{sh:.2f}" if sh is not None else "-"
        ar_s = f"{ar*100:.1f}%" if ar is not None else "-"
        print(f"{fname:<45} {ic_s:>8} {icir_s:>8} {sh_s:>8} {ar_s:>8}")

    strong = [(f, m) for f, m in all_metrics.items()
              if m.get("ic_mean") is not None and abs(m["ic_mean"]) > 0.02]
    if strong:
        print(f"\nStrong factors (|IC| > 0.02): {len(strong)}")
        for f, m in sorted(strong, key=lambda x: abs(x[1]["ic_mean"]), reverse=True):
            print(f"  {f}: IC={m['ic_mean']:.4f}, Sharpe={m.get('top_sharpe', 0):.2f}")


if __name__ == "__main__":
    asyncio.run(main())
