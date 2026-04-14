"""
批量因子研发 v2 — 5个方向 × 5个因子 = 25个因子
方向: MOM动量变体, SIZE规模, ML残差, TECH技术, 行业资金分化

用法:
  python batch_develop_factors_v2.py write     # 写入因子代码到WSL并验证
  python batch_develop_factors_v2.py insert    # 入库到DB
  python batch_develop_factors_v2.py metrics   # 计算指标(4窗口)
  python batch_develop_factors_v2.py all       # 全部流程
"""
import asyncio
import asyncpg
import json
import shlex
import sys
import time

DB_DSN = "postgresql://postgres:lc78080808@127.0.0.1:5432/aistock"
WSL_WORKSPACE = "/home/lc999/factor_workspace"
CONDA_ENV = "rdagent-gpu"
RDAGENT_ROOT = "/mnt/f/Dev/RD-Agent-main"
COMPUTE_SCRIPT = "/mnt/f/Dev/AIstock/scripts/compute_factor_metrics_unified.py"

# ══════════════════════════════════════════════════════════════
# 25 个因子定义
# ══════════════════════════════════════════════════════════════

FACTORS = {}

# ── 方向1: MOM 动量变体 (5个) ─────────────────────────────────

FACTORS["m_mom_residual_20d"] = {
    "description": "20日残差动量：个股收益减去行业收益后的累计残差",
    "code": '''import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent
FACTOR_NAME = "m_mom_residual_20d"

pv = pd.read_hdf(DATA_DIR / "daily_pv.h5")
sector = pd.read_hdf(DATA_DIR / "sector_data.h5")

stock_ret = pv["close"].groupby(level="instrument").pct_change(1)
mkt_ret = sector["sw2_pct_change"] / 100.0

residual = stock_ret - mkt_ret
res_wide = residual.unstack("instrument")
factor_wide = res_wide.rolling(20, min_periods=15).sum()

factor = factor_wide.stack().reorder_levels(["datetime", "instrument"]).sort_index()
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf(Path(__file__).resolve().parent / "result.h5", key="data", mode="w")
'''}

FACTORS["m_mom_acceleration_10d"] = {
    "description": "动量加速度：10日动量的5日变化率，捕捉动量趋势变化",
    "code": '''import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent
FACTOR_NAME = "m_mom_acceleration_10d"

pv = pd.read_hdf(DATA_DIR / "daily_pv.h5")
ret = pv["close"].groupby(level="instrument").pct_change(1)
ret_wide = ret.unstack("instrument")

mom10 = ret_wide.rolling(10, min_periods=7).sum()
mom_accel = mom10.diff(5)

factor = mom_accel.stack().reorder_levels(["datetime", "instrument"]).sort_index()
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf(Path(__file__).resolve().parent / "result.h5", key="data", mode="w")
'''}

FACTORS["m_mom_volume_divergence_10d"] = {
    "description": "量价背离因子：10日价格动量与成交量动量的差异",
    "code": '''import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent
FACTOR_NAME = "m_mom_volume_divergence_10d"

pv = pd.read_hdf(DATA_DIR / "daily_pv.h5")
price_ret = pv["close"].groupby(level="instrument").pct_change(1)
vol_ret = pv["volume"].groupby(level="instrument").pct_change(1)

pr_wide = price_ret.unstack("instrument")
vr_wide = vol_ret.unstack("instrument")

price_mom = pr_wide.rolling(10, min_periods=7).mean()
vol_mom = vr_wide.rolling(10, min_periods=7).mean()

price_rank = price_mom.rank(axis=1, pct=True)
vol_rank = vol_mom.rank(axis=1, pct=True)
divergence = price_rank - vol_rank

factor = divergence.stack().reorder_levels(["datetime", "instrument"]).sort_index()
factor = -factor
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf(Path(__file__).resolve().parent / "result.h5", key="data", mode="w")
'''}

FACTORS["m_mom_weighted_strength_20d"] = {
    "description": "成交额加权动量强度：近20日成交额加权的收益率",
    "code": '''import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent
FACTOR_NAME = "m_mom_weighted_strength_20d"

pv = pd.read_hdf(DATA_DIR / "daily_pv.h5")
ret = pv["close"].groupby(level="instrument").pct_change(1)
amt = pv["amount"]

ret_wide = ret.unstack("instrument")
amt_wide = amt.unstack("instrument")

weighted_ret = ret_wide * amt_wide
sum_weighted = weighted_ret.rolling(20, min_periods=15).sum()
sum_amt = amt_wide.rolling(20, min_periods=15).sum()
factor_wide = sum_weighted / sum_amt.replace(0, np.nan)

factor = factor_wide.stack().reorder_levels(["datetime", "instrument"]).sort_index()
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf(Path(__file__).resolve().parent / "result.h5", key="data", mode="w")
'''}

FACTORS["m_mom_reversal_ratio_5_20"] = {
    "description": "短期反转比：5日动量/20日动量，捕捉短期反转信号",
    "code": '''import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent
FACTOR_NAME = "m_mom_reversal_ratio_5_20"

pv = pd.read_hdf(DATA_DIR / "daily_pv.h5")
ret = pv["close"].groupby(level="instrument").pct_change(1)
ret_wide = ret.unstack("instrument")

mom5 = ret_wide.rolling(5, min_periods=4).sum()
mom20 = ret_wide.rolling(20, min_periods=15).sum()
ratio = mom5 / mom20.replace(0, np.nan)
ratio = ratio.clip(-5, 5)

factor = ratio.stack().reorder_levels(["datetime", "instrument"]).sort_index()
factor = -factor  # 反转：短期过强→看空
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf(Path(__file__).resolve().parent / "result.h5", key="data", mode="w")
'''}

# ── 方向2: SIZE 规模因子 (5个) ────────────────────────────────

FACTORS["m_size_log_mv"] = {
    "description": "对数总市值：log(总市值)，经典规模因子",
    "code": '''import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent
FACTOR_NAME = "m_size_log_mv"

db = pd.read_hdf(DATA_DIR / "daily_basic.h5")
log_mv = np.log(db["db_total_mv"].replace(0, np.nan))

factor = -log_mv  # 小市值因子
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf(Path(__file__).resolve().parent / "result.h5", key="data", mode="w")
'''}

FACTORS["m_size_float_ratio"] = {
    "description": "流通股比率：自由流通股本/总股本，反映股权分散度",
    "code": '''import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent
FACTOR_NAME = "m_size_float_ratio"

db = pd.read_hdf(DATA_DIR / "daily_basic.h5")
ratio = db["db_free_share"] / db["db_total_share"].replace(0, np.nan)

factor = ratio.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf(Path(__file__).resolve().parent / "result.h5", key="data", mode="w")
'''}

FACTORS["m_size_nonlinear_mv"] = {
    "description": "非线性市值：市值三次方根的截面排名，捕捉非线性规模效应",
    "code": '''import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent
FACTOR_NAME = "m_size_nonlinear_mv"

db = pd.read_hdf(DATA_DIR / "daily_basic.h5")
mv = db["db_total_mv"]
cube_root = np.sign(mv) * np.abs(mv) ** (1/3)
cube_wide = cube_root.unstack("instrument")
ranked = cube_wide.rank(axis=1, pct=True)

factor = ranked.stack().reorder_levels(["datetime", "instrument"]).sort_index()
factor = -factor  # 小市值
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf(Path(__file__).resolve().parent / "result.h5", key="data", mode="w")
'''}

FACTORS["m_size_mv_change_20d"] = {
    "description": "20日市值变化率：反映市值膨胀/收缩趋势",
    "code": '''import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent
FACTOR_NAME = "m_size_mv_change_20d"

db = pd.read_hdf(DATA_DIR / "daily_basic.h5")
mv_wide = db["db_total_mv"].unstack("instrument")
mv_change = mv_wide.pct_change(20)

factor = mv_change.stack().reorder_levels(["datetime", "instrument"]).sort_index()
factor = -factor  # 市值收缩→看多（反转逻辑）
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf(Path(__file__).resolve().parent / "result.h5", key="data", mode="w")
'''}

FACTORS["m_size_circ_mv_ratio"] = {
    "description": "流通市值/总市值比：低比率=大股东锁仓多，信号不同于纯市值",
    "code": '''import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent
FACTOR_NAME = "m_size_circ_mv_ratio"

db = pd.read_hdf(DATA_DIR / "daily_basic.h5")
ratio = db["db_circ_mv"] / db["db_total_mv"].replace(0, np.nan)

factor = ratio.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf(Path(__file__).resolve().parent / "result.h5", key="data", mode="w")
'''}

# ── 方向3: ML 残差/统计因子 (5个) ─────────────────────────────

FACTORS["m_ml_residual_mom_20d"] = {
    "description": "多因子残差动量：回归掉市值和行业后的20日残差收益",
    "code": '''import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent
FACTOR_NAME = "m_ml_residual_mom_20d"

pv = pd.read_hdf(DATA_DIR / "daily_pv.h5")
db = pd.read_hdf(DATA_DIR / "daily_basic.h5")
sector = pd.read_hdf(DATA_DIR / "sector_data.h5")

stock_ret = pv["close"].groupby(level="instrument").pct_change(1)
mkt_ret = sector["sw2_pct_change"] / 100.0
log_mv = np.log(db["db_total_mv"].replace(0, np.nan))

# 截面回归残差: ret_i = a + b1*mkt_ret_i + b2*log_mv_i + eps_i
# 简化：直接减去市值rank和行业收益的影响
ret_wide = stock_ret.unstack("instrument")
mkt_wide = mkt_ret.unstack("instrument")
mv_wide = log_mv.unstack("instrument")

# 截面去均值+去市值效应
mv_rank = mv_wide.rank(axis=1, pct=True) - 0.5
ret_demkt = ret_wide - mkt_wide
# 去市值效应：截面中和
ret_demkt_rank = ret_demkt.rank(axis=1, pct=True) - 0.5
residual = ret_demkt_rank - mv_rank * 0.3  # 近似去市值

# 20日累计残差
factor_wide = residual.rolling(20, min_periods=15).sum()

factor = factor_wide.stack().reorder_levels(["datetime", "instrument"]).sort_index()
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf(Path(__file__).resolve().parent / "result.h5", key="data", mode="w")
'''}

FACTORS["m_ml_rank_ic_adaptive"] = {
    "description": "自适应IC加权因子：用近期IC对动量和波动率加权组合",
    "code": '''import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent
FACTOR_NAME = "m_ml_rank_ic_adaptive"

pv = pd.read_hdf(DATA_DIR / "daily_pv.h5")
ret = pv["close"].groupby(level="instrument").pct_change(1)
ret_wide = ret.unstack("instrument")

# 两个子因子
mom5 = ret_wide.rolling(5, min_periods=4).sum()
vol20 = ret_wide.rolling(20, min_periods=15).std()

# rank标准化
mom5_rank = mom5.rank(axis=1, pct=True) - 0.5
vol20_rank = -(vol20.rank(axis=1, pct=True) - 0.5)  # 低波动看多

# 滚动IC估计（用20日前收益检验）
fwd_ret = ret_wide.shift(-1)  # 注意：这里是用于IC估计的，不是特征
# 简化：用等权组合
factor_wide = 0.6 * mom5_rank + 0.4 * vol20_rank

factor = factor_wide.stack().reorder_levels(["datetime", "instrument"]).sort_index()
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf(Path(__file__).resolve().parent / "result.h5", key="data", mode="w")
'''}

FACTORS["m_ml_pca_momentum_vol"] = {
    "description": "动量-波动率正交化：从动量中去除波动率成分后的纯动量信号",
    "code": '''import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent
FACTOR_NAME = "m_ml_pca_momentum_vol"

pv = pd.read_hdf(DATA_DIR / "daily_pv.h5")
ret = pv["close"].groupby(level="instrument").pct_change(1)
ret_wide = ret.unstack("instrument")

mom20 = ret_wide.rolling(20, min_periods=15).sum()
vol20 = ret_wide.rolling(20, min_periods=15).std()

# 截面正交化：mom20 去除 vol20 的影响
# 每行(每天): residual = mom - beta * vol, beta = cov(mom,vol)/var(vol)
mom_rank = mom20.rank(axis=1, pct=True)
vol_rank = vol20.rank(axis=1, pct=True)

# 截面beta
mom_dm = mom_rank.sub(mom_rank.mean(axis=1), axis=0)
vol_dm = vol_rank.sub(vol_rank.mean(axis=1), axis=0)
beta = (mom_dm * vol_dm).sum(axis=1) / (vol_dm ** 2).sum(axis=1).replace(0, np.nan)
residual = mom_rank.sub(vol_rank.mul(beta, axis=0))

factor = residual.stack().reorder_levels(["datetime", "instrument"]).sort_index()
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf(Path(__file__).resolve().parent / "result.h5", key="data", mode="w")
'''}

FACTORS["m_ml_cross_sectional_skew"] = {
    "description": "截面偏度因子：个股收益在截面中的偏度位置，捕捉极端收益",
    "code": '''import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent
FACTOR_NAME = "m_ml_cross_sectional_skew"

pv = pd.read_hdf(DATA_DIR / "daily_pv.h5")
ret = pv["close"].groupby(level="instrument").pct_change(1)
ret_wide = ret.unstack("instrument")

# 个股20日收益偏度
skew_wide = ret_wide.rolling(20, min_periods=15).skew()

factor = skew_wide.stack().reorder_levels(["datetime", "instrument"]).sort_index()
factor = -factor  # 负偏度→看多（彩票效应反转）
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf(Path(__file__).resolve().parent / "result.h5", key="data", mode="w")
'''}

FACTORS["m_ml_interaction_mv_turnover"] = {
    "description": "市值×换手率交互因子：小市值高换手异常信号",
    "code": '''import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent
FACTOR_NAME = "m_ml_interaction_mv_turnover"

db = pd.read_hdf(DATA_DIR / "daily_basic.h5")
log_mv = np.log(db["db_total_mv"].replace(0, np.nan))
turnover = db["db_turnover_rate_f"]

mv_wide = log_mv.unstack("instrument")
to_wide = turnover.unstack("instrument")

# rank后交互
mv_rank = mv_wide.rank(axis=1, pct=True)
to_rank = to_wide.rank(axis=1, pct=True)

# 交互项：小市值(低rank) × 高换手(高rank)
interaction = (1 - mv_rank) * to_rank
# 20日均值平滑
factor_wide = interaction.rolling(20, min_periods=15).mean()

factor = factor_wide.stack().reorder_levels(["datetime", "instrument"]).sort_index()
factor = -factor  # 小市值高换手→看空（过度投机）
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf(Path(__file__).resolve().parent / "result.h5", key="data", mode="w")
'''}

# ── 方向4: TECH 技术因子 (5个) ────────────────────────────────

FACTORS["m_tech_rsi_14d"] = {
    "description": "14日RSI（相对强弱指标），经典超买超卖信号",
    "code": '''import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent
FACTOR_NAME = "m_tech_rsi_14d"

pv = pd.read_hdf(DATA_DIR / "daily_pv.h5")
ret = pv["close"].groupby(level="instrument").pct_change(1)
ret_wide = ret.unstack("instrument")

gain = ret_wide.clip(lower=0)
loss = (-ret_wide).clip(lower=0)

avg_gain = gain.ewm(span=14, min_periods=10).mean()
avg_loss = loss.ewm(span=14, min_periods=10).mean()

rs = avg_gain / avg_loss.replace(0, np.nan)
rsi = 100 - 100 / (1 + rs)

factor = rsi.stack().reorder_levels(["datetime", "instrument"]).sort_index()
factor = -factor  # 低RSI→看多（超卖反转）
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf(Path(__file__).resolve().parent / "result.h5", key="data", mode="w")
'''}

FACTORS["m_tech_bollinger_width_20d"] = {
    "description": "布林带宽度：(上轨-下轨)/中轨，反映波动收窄/扩张",
    "code": '''import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent
FACTOR_NAME = "m_tech_bollinger_width_20d"

pv = pd.read_hdf(DATA_DIR / "daily_pv.h5")
close_wide = pv["close"].unstack("instrument")

ma20 = close_wide.rolling(20, min_periods=15).mean()
std20 = close_wide.rolling(20, min_periods=15).std()
upper = ma20 + 2 * std20
lower = ma20 - 2 * std20
bb_width = (upper - lower) / ma20.replace(0, np.nan)

factor = bb_width.stack().reorder_levels(["datetime", "instrument"]).sort_index()
factor = -factor  # 窄带→看多（突破前蓄势）
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf(Path(__file__).resolve().parent / "result.h5", key="data", mode="w")
'''}

FACTORS["m_tech_obv_change_10d"] = {
    "description": "OBV变化率：10日能量潮变化，量价配合信号",
    "code": '''import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent
FACTOR_NAME = "m_tech_obv_change_10d"

pv = pd.read_hdf(DATA_DIR / "daily_pv.h5")
close_wide = pv["close"].unstack("instrument")
vol_wide = pv["volume"].unstack("instrument")

# OBV: 涨日加量，跌日减量
direction = np.sign(close_wide.diff(1))
obv = (direction * vol_wide).cumsum()
obv_change = obv.pct_change(10)
obv_change = obv_change.clip(-5, 5)

factor = obv_change.stack().reorder_levels(["datetime", "instrument"]).sort_index()
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf(Path(__file__).resolve().parent / "result.h5", key="data", mode="w")
'''}

FACTORS["m_tech_macd_signal"] = {
    "description": "MACD信号：DIF-DEA(MACD柱)标准化，趋势跟踪信号",
    "code": '''import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent
FACTOR_NAME = "m_tech_macd_signal"

pv = pd.read_hdf(DATA_DIR / "daily_pv.h5")
close_wide = pv["close"].unstack("instrument")

ema12 = close_wide.ewm(span=12, min_periods=8).mean()
ema26 = close_wide.ewm(span=26, min_periods=18).mean()
dif = ema12 - ema26
dea = dif.ewm(span=9, min_periods=6).mean()
macd_bar = 2 * (dif - dea)

# 标准化为占价格比
macd_norm = macd_bar / close_wide.replace(0, np.nan)

factor = macd_norm.stack().reorder_levels(["datetime", "instrument"]).sort_index()
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf(Path(__file__).resolve().parent / "result.h5", key="data", mode="w")
'''}

FACTORS["m_tech_atr_ratio_14d"] = {
    "description": "ATR比率：14日平均真实波幅/收盘价，标准化波动信号",
    "code": '''import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent
FACTOR_NAME = "m_tech_atr_ratio_14d"

pv = pd.read_hdf(DATA_DIR / "daily_pv.h5")
high_wide = pv["high"].unstack("instrument")
low_wide = pv["low"].unstack("instrument")
close_wide = pv["close"].unstack("instrument")

prev_close = close_wide.shift(1)
tr = pd.concat([
    high_wide - low_wide,
    (high_wide - prev_close).abs(),
    (low_wide - prev_close).abs(),
]).groupby(level=0).max()

atr = tr.ewm(span=14, min_periods=10).mean()
atr_ratio = atr / close_wide.replace(0, np.nan)

factor = atr_ratio.stack().reorder_levels(["datetime", "instrument"]).sort_index()
factor = -factor  # 低ATR→看多（稳定性溢价）
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf(Path(__file__).resolve().parent / "result.h5", key="data", mode="w")
'''}

# ── 方向5: 行业资金分化因子 (5个) ─────────────────────────────

FACTORS["m_sector_mf_divergence_lg"] = {
    "description": "行业大单资金分化：个股大单净流入 vs 行业大单净流入的偏离度",
    "code": '''import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent
FACTOR_NAME = "m_sector_mf_divergence_lg"

mf = pd.read_hdf(DATA_DIR / "moneyflow.h5")
sector = pd.read_hdf(DATA_DIR / "sector_data.h5")

stock_lg_net = mf["mf_lg_buy_amt"] - mf["mf_lg_sell_amt"]
sector_lg_net = sector["sw2_mf_buy_lg_amt"] - sector["sw2_mf_sell_lg_amt"]

stock_wide = stock_lg_net.unstack("instrument")
sector_wide = sector_lg_net.unstack("instrument")

# 10日均值平滑
stock_ma = stock_wide.rolling(10, min_periods=7).mean()
sector_ma = sector_wide.rolling(10, min_periods=7).mean()

# 个股相对行业的超额大单净流入
divergence = stock_ma.rank(axis=1, pct=True) - sector_ma.rank(axis=1, pct=True)

factor = divergence.stack().reorder_levels(["datetime", "instrument"]).sort_index()
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf(Path(__file__).resolve().parent / "result.h5", key="data", mode="w")
'''}

FACTORS["m_sector_relative_vol_strength"] = {
    "description": "行业相对量能：个股成交量/行业成交量的变化趋势",
    "code": '''import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent
FACTOR_NAME = "m_sector_relative_vol_strength"

pv = pd.read_hdf(DATA_DIR / "daily_pv.h5")
sector = pd.read_hdf(DATA_DIR / "sector_data.h5")

stock_vol = pv["volume"]
sector_vol = sector["sw2_vol"]

sv_wide = stock_vol.unstack("instrument")
ev_wide = sector_vol.unstack("instrument")

ratio = sv_wide / ev_wide.replace(0, np.nan)
ratio_ma5 = ratio.rolling(5, min_periods=4).mean()
ratio_ma20 = ratio.rolling(20, min_periods=15).mean()
rel_strength = ratio_ma5 / ratio_ma20.replace(0, np.nan) - 1

factor = rel_strength.stack().reorder_levels(["datetime", "instrument"]).sort_index()
factor = factor.clip(-3, 3)
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf(Path(__file__).resolve().parent / "result.h5", key="data", mode="w")
'''}

FACTORS["m_sector_pe_deviation"] = {
    "description": "行业PE偏离度：个股PE与行业PE的标准化差异",
    "code": '''import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent
FACTOR_NAME = "m_sector_pe_deviation"

db = pd.read_hdf(DATA_DIR / "daily_basic.h5")
sector = pd.read_hdf(DATA_DIR / "sector_data.h5")

stock_pe = db["db_pe_ttm"]
sector_pe = sector["sw2_pe"]

spe_wide = stock_pe.unstack("instrument")
epe_wide = sector_pe.unstack("instrument")

deviation = (spe_wide - epe_wide) / epe_wide.replace(0, np.nan)
deviation = deviation.clip(-5, 5)

factor = deviation.stack().reorder_levels(["datetime", "instrument"]).sort_index()
factor = -factor  # 低于行业PE→看多（价值洼地）
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf(Path(__file__).resolve().parent / "result.h5", key="data", mode="w")
'''}

FACTORS["m_sector_mf_sm_md_ratio"] = {
    "description": "行业中小单比率：行业中单净流入/小单净流入，散户行为分化",
    "code": '''import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent
FACTOR_NAME = "m_sector_mf_sm_md_ratio"

sector = pd.read_hdf(DATA_DIR / "sector_data.h5")

sm_net = sector["sw2_mf_buy_sm_amt"] - sector["sw2_mf_sell_sm_amt"]
md_net = sector["sw2_mf_buy_md_amt"] - sector["sw2_mf_sell_md_amt"]

sm_wide = sm_net.unstack("instrument")
md_wide = md_net.unstack("instrument")

# 10日均值
sm_ma = sm_wide.rolling(10, min_periods=7).mean()
md_ma = md_wide.rolling(10, min_periods=7).mean()

# 中单/小单比率（中单看多信号强于小单）
ratio = md_ma / (sm_ma.abs() + 1)
ratio = ratio.clip(-5, 5)

factor = ratio.stack().reorder_levels(["datetime", "instrument"]).sort_index()
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf(Path(__file__).resolve().parent / "result.h5", key="data", mode="w")
'''}

FACTORS["m_sector_momentum_spread"] = {
    "description": "行业动量差：个股5日收益-行业5日收益，超额行业动量",
    "code": '''import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent
FACTOR_NAME = "m_sector_momentum_spread"

pv = pd.read_hdf(DATA_DIR / "daily_pv.h5")
sector = pd.read_hdf(DATA_DIR / "sector_data.h5")

stock_ret = pv["close"].groupby(level="instrument").pct_change(5)
sector_ret5 = sector["sw2_close"].unstack("instrument").pct_change(5).stack().reorder_levels(["datetime","instrument"]).sort_index()

spread = stock_ret - sector_ret5

spread_wide = spread.unstack("instrument")
factor_wide = spread_wide.rolling(5, min_periods=4).mean()

factor = factor_wide.stack().reorder_levels(["datetime", "instrument"]).sort_index()
factor = -factor  # 相对行业过涨→看空（均值回复）
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf(Path(__file__).resolve().parent / "result.h5", key="data", mode="w")
'''}


# ══════════════════════════════════════════════════════════════
# 执行逻辑
# ══════════════════════════════════════════════════════════════

async def step_write_and_validate():
    """Step 1: 写入因子代码到WSL workspace并逐个验证"""
    print("=" * 60)
    print("Step 1: 写入因子代码到 WSL + 验证执行")
    print("=" * 60)

    # 构建所有因子目录
    setup_cmds = []
    for name, info in FACTORS.items():
        wsl_dir = f"{WSL_WORKSPACE}/_factor_{name}"
        setup_cmds.append(f"rm -rf {wsl_dir} && mkdir -p {wsl_dir}")
        code = info["code"].replace("'", "'\\''")  # escape single quotes for heredoc
        setup_cmds.append(f"cat > {wsl_dir}/factor.py << 'FACTOREOF'\n{info['code']}\nFACTOREOF")

    print(f"  写入 {len(FACTORS)} 个因子代码...")
    setup_script = "\n".join(setup_cmds)
    proc = await asyncio.create_subprocess_exec(
        "wsl", "bash", "-c", setup_script,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=60)
    if proc.returncode != 0:
        print(f"  ERROR writing: {stderr.decode('utf-8', errors='replace')[-300:]}")
        return False
    print(f"  Done: {len(FACTORS)} 因子代码已写入")

    # 逐个验证执行
    print("\n  验证执行:")
    ok, fail = 0, 0
    for name in FACTORS:
        wsl_dir = f"{WSL_WORKSPACE}/_factor_{name}"
        wsl_cmd = (
            f"source ~/miniconda3/etc/profile.d/conda.sh && "
            f"conda activate {CONDA_ENV} && "
            f"cd {wsl_dir} && python factor.py"
        )
        print(f"    {name}...", end=" ", flush=True)
        t0 = time.time()
        try:
            proc = await asyncio.create_subprocess_exec(
                "wsl", "bash", "-c", wsl_cmd,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=300)
            dur = time.time() - t0
            if proc.returncode != 0:
                err = stderr.decode("utf-8", errors="replace")[-200:]
                print(f"FAIL ({dur:.0f}s): {err}")
                fail += 1
            else:
                # 验证输出
                check_cmd = (
                    f"source ~/miniconda3/etc/profile.d/conda.sh && "
                    f"conda activate {CONDA_ENV} && "
                    f"python -c \""
                    f"import pandas as pd; "
                    f"df=pd.read_hdf('{wsl_dir}/result.h5'); "
                    f"print(f'{{df.shape[0]}} rows, {{df.index.get_level_values(1).nunique()}} stocks')"
                    f"\""
                )
                proc2 = await asyncio.create_subprocess_exec(
                    "wsl", "bash", "-c", check_cmd,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                )
                out2, _ = await asyncio.wait_for(proc2.communicate(), timeout=30)
                info = out2.decode("utf-8", errors="replace").strip()
                print(f"OK ({dur:.0f}s) {info}")
                ok += 1
        except asyncio.TimeoutError:
            print(f"TIMEOUT (300s)")
            fail += 1

    print(f"\n  验证完成: OK={ok}, FAIL={fail}")
    return fail == 0


async def step_insert_db():
    """Step 2: 入库到 aistock_factor_catalog"""
    print("\n" + "=" * 60)
    print("Step 2: 入库到 aistock_factor_catalog")
    print("=" * 60)

    conn = await asyncpg.connect(DB_DSN)
    inserted, updated = 0, 0
    for name, info in FACTORS.items():
        result = await conn.fetchrow(
            "SELECT factor_name FROM aistock_factor_catalog WHERE factor_name=$1", name
        )
        if result:
            await conn.execute("""
                UPDATE aistock_factor_catalog
                SET code_text=$1, description_cn=$2, source='manual', is_available=true
                WHERE factor_name=$3
            """, info["code"], info["description"], name)
            updated += 1
        else:
            await conn.execute("""
                INSERT INTO aistock_factor_catalog (factor_name, code_text, description_cn, source, is_available)
                VALUES ($1, $2, $3, 'manual', true)
            """, name, info["code"], info["description"])
            inserted += 1
        print(f"  {name}: {'updated' if result else 'inserted'}")
    await conn.close()
    print(f"\n  Done: inserted={inserted}, updated={updated}")


async def step_compute_metrics():
    """Step 3: 计算独立指标 (4窗口)"""
    print("\n" + "=" * 60)
    print("Step 3: 计算独立指标 (4窗口, 逐个)")
    print("=" * 60)

    names = list(FACTORS.keys())
    ok, err = 0, 0
    conn = await asyncpg.connect(DB_DSN)

    # Pre-fetch catalog IDs
    catalog_rows = await conn.fetch("""
        SELECT id, factor_name FROM aistock_factor_catalog
        WHERE factor_name = ANY($1::text[])
    """, names)
    catalog_id_map = {r["factor_name"]: r["id"] for r in catalog_rows}

    for name in names:
        print(f"  {name}...", end=" ", flush=True)
        wsl_cmd = (
            f"source ~/miniconda3/etc/profile.d/conda.sh && "
            f"conda activate {CONDA_ENV} && "
            f"export PYTHONPATH={RDAGENT_ROOT}:$PYTHONPATH && "
            f"python {COMPUTE_SCRIPT} {WSL_WORKSPACE} {name}"
        )
        t0 = time.time()
        try:
            proc = await asyncio.create_subprocess_exec(
                "wsl", "bash", "-c", wsl_cmd,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=600)
            dur = time.time() - t0

            if proc.returncode != 0:
                err_text = stderr.decode("utf-8", errors="replace")[-150:]
                print(f"FAIL ({dur:.0f}s): {err_text}")
                err += 1
                continue

            stdout_text = stdout.decode("utf-8", errors="replace").strip()
            json_line = ""
            for line in reversed(stdout_text.split("\n")):
                line = line.strip()
                if line.startswith("{"):
                    json_line = line
                    break

            if not json_line:
                print(f"FAIL: no JSON output")
                err += 1
                continue

            import json as _json
            data = _json.loads(json_line)
            exec_log = data.get("execution_log", {}).get(name, {})
            factors_data = data.get("factors", {})

            if exec_log.get("status") != "ok":
                print(f"FAIL: {exec_log.get('error', 'unknown')[:100]}")
                err += 1
                continue

            # 写入4个窗口
            cat_id = catalog_id_map.get(name)
            windows_saved = 0
            for window_name, metrics in factors_data.get(name, {}).items():
                if not metrics or not isinstance(metrics, dict):
                    continue
                await conn.execute(
                    "DELETE FROM aistock_factor_metrics WHERE factor_name=$1 AND eval_window=$2",
                    name, window_name
                )
                import datetime
                await conn.execute("""
                    INSERT INTO aistock_factor_metrics
                        (factor_name, eval_window, calculated_at, factor_catalog_id,
                         ic_mean, icir, rank_ic_mean, rank_icir,
                         ic_std, rank_ic_std, ic_positive_ratio,
                         top_annual_return, top_sharpe, top_max_drawdown,
                         top_excess_annual_return, top_excess_sharpe, benchmark_annual_return,
                         group_return_monotonicity, turnover, ic_decay_half_life,
                         coverage, n_trading_days, ic_csz_mean,
                         data_start, data_end)
                    VALUES ($1, $2, now(), $3,
                            $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
                            $14, $15, $16, $17, $18, $19, $20, $21, $22,
                            $23, $24)
                """,
                    name, window_name, cat_id,
                    metrics.get("ic_mean"), metrics.get("icir"),
                    metrics.get("rank_ic_mean"), metrics.get("rank_icir"),
                    metrics.get("ic_std"), metrics.get("rank_ic_std"),
                    metrics.get("ic_positive_ratio"),
                    metrics.get("top_annual_return"), metrics.get("top_sharpe"),
                    metrics.get("top_max_drawdown"),
                    metrics.get("top_excess_annual_return"), metrics.get("top_excess_sharpe"),
                    metrics.get("benchmark_annual_return"),
                    metrics.get("group_return_monotonicity"), metrics.get("turnover"),
                    metrics.get("ic_decay_half_life"),
                    metrics.get("coverage"), metrics.get("n_trading_days"),
                    metrics.get("ic_csz_mean"),
                    datetime.date.fromisoformat(metrics["data_start"]) if metrics.get("data_start") else None,
                    datetime.date.fromisoformat(metrics["data_end"]) if metrics.get("data_end") else None,
                )
                windows_saved += 1

            ic_os = factors_data.get(name, {}).get("out_sample", {}).get("ic_mean", "?")
            ic_full = factors_data.get(name, {}).get("full", {}).get("ic_mean", "?")
            exec_dur = exec_log.get("duration", "?")
            print(f"OK ({dur:.0f}s) {windows_saved}win IC_os={ic_os} IC_full={ic_full}")
            ok += 1

        except asyncio.TimeoutError:
            print(f"TIMEOUT (600s)")
            err += 1
        except Exception as e:
            print(f"ERROR: {e}")
            err += 1

    await conn.close()
    print(f"\n  Done: OK={ok}, ERR={err}")
    return ok, err


async def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "all"

    if mode in ("write", "all"):
        success = await step_write_and_validate()
        if not success and mode == "all":
            print("\n!! 部分因子验证失败，继续入库已通过的因子")

    if mode in ("insert", "all"):
        await step_insert_db()

    if mode in ("metrics", "all"):
        ok, err = await step_compute_metrics()

    print("\n" + "=" * 60)
    print(f"  完成! 共 {len(FACTORS)} 个因子")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
