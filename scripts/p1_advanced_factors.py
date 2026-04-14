#!/usr/bin/env python3
"""P1 高级跨数据集因子研发 + 入库 + 指标计算。

包含：残差/Beta、条件因子、非线性交互、多源深度融合。
"""
import asyncio
import json
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
    # A. 残差/Beta 因子族（CORR/STAT 补强）
    # ══════════════════════════════════════════

    "m_beta_60d": {
        "description": "60日市场Beta：个股收益对行业收益的回归斜率，高Beta=高系统风险暴露",
        "code": r'''import pandas as pd
import numpy as np

FACTOR_NAME = "m_beta_60d"

pv = pd.read_hdf("daily_pv.h5")
sector = pd.read_hdf("sector_data.h5")

stock_ret = pv["close"].groupby(level="instrument").pct_change(1)
mkt_ret = sector["sw2_pct_change"] / 100.0  # 行业涨跌幅(%)转小数

# 60日滚动Beta = Cov(stock, market) / Var(market)
def rolling_beta(stock, market, window=60):
    """对齐后计算滚动beta。"""
    aligned = pd.DataFrame({"s": stock, "m": market}).dropna()
    if len(aligned) < window:
        return pd.Series(dtype=float, index=stock.index)
    cov = aligned["s"].rolling(window, min_periods=40).cov(aligned["m"])
    var = aligned["m"].rolling(window, min_periods=40).var()
    beta = cov / var.replace(0, np.nan)
    return beta

# 简化版：截面回归用 correlation * (std_stock / std_market) 近似
stock_std = stock_ret.groupby(level="instrument").rolling(60, min_periods=40).std().droplevel(0)
mkt_std = mkt_ret.groupby(level="instrument").rolling(60, min_periods=40).std().droplevel(0)

# 计算相关系数
aligned = pd.DataFrame({"s": stock_ret, "m": mkt_ret}).dropna()
corr = aligned["s"].groupby(level="instrument").rolling(60, min_periods=40).corr(aligned["m"]).droplevel(0)

beta = corr * (stock_std / mkt_std.replace(0, np.nan))
beta = beta.replace([np.inf, -np.inf], np.nan)
# 反向使用：低Beta溢价
factor = -beta
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_idio_vol_60d": {
        "description": "60日特质波动率(反向)：去除行业收益后的残差波动率，低特质波动溢价(Ang2009)",
        "code": r'''import pandas as pd
import numpy as np

FACTOR_NAME = "m_idio_vol_60d"

pv = pd.read_hdf("daily_pv.h5")
sector = pd.read_hdf("sector_data.h5")

stock_ret = pv["close"].groupby(level="instrument").pct_change(1)
mkt_ret = sector["sw2_pct_change"] / 100.0

# 残差 = stock_ret - beta * mkt_ret
# 简化: 用 stock_ret - mkt_ret 近似残差（假设beta~1）
residual = stock_ret - mkt_ret

# 60日残差波动率
idio_vol = residual.groupby(level="instrument").rolling(60, min_periods=40).std().droplevel(0)

# 反向：低特质波动溢价
factor = -idio_vol
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_residual_momentum_20d": {
        "description": "20日残差动量(Blitz2011)：去除行业收益后的累计残差收益，纯个股alpha信号",
        "code": r'''import pandas as pd
import numpy as np

FACTOR_NAME = "m_residual_momentum_20d"

pv = pd.read_hdf("daily_pv.h5")
sector = pd.read_hdf("sector_data.h5")

stock_ret = pv["close"].groupby(level="instrument").pct_change(1)
mkt_ret = sector["sw2_pct_change"] / 100.0

# 残差收益
residual = stock_ret - mkt_ret

# 20日累计残差收益
factor = residual.groupby(level="instrument").rolling(20, min_periods=15).sum().droplevel(0)
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_r_squared_60d": {
        "description": "60日R-squared(反向)：个股收益被行业解释的比例，低R2=高特质性=潜在alpha",
        "code": r'''import pandas as pd
import numpy as np

FACTOR_NAME = "m_r_squared_60d"

pv = pd.read_hdf("daily_pv.h5")
sector = pd.read_hdf("sector_data.h5")

stock_ret = pv["close"].groupby(level="instrument").pct_change(1)
mkt_ret = sector["sw2_pct_change"] / 100.0

# R^2 = corr^2
aligned = pd.DataFrame({"s": stock_ret, "m": mkt_ret}).dropna()
corr = aligned["s"].groupby(level="instrument").rolling(60, min_periods=40).corr(aligned["m"]).droplevel(0)
r_squared = corr ** 2

# 反向：低R2 = 高特质性
factor = -r_squared
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    # ══════════════════════════════════════════
    # B. 条件因子 + 非线性交互
    # ══════════════════════════════════════════

    "m_conditional_momentum_20d": {
        "description": "条件动量：高波动时取反转(负动量)，低波动时取延续(正动量)，自适应信号",
        "code": r'''import pandas as pd
import numpy as np

FACTOR_NAME = "m_conditional_momentum_20d"

pv = pd.read_hdf("daily_pv.h5")

stock_ret = pv["close"].groupby(level="instrument").pct_change(1)
mom_20d = pv["close"].groupby(level="instrument").pct_change(20)

# 20日波动率
vol_20d = stock_ret.groupby(level="instrument").rolling(20, min_periods=15).std().droplevel(0)
# 波动率截面中位数
vol_median = vol_20d.groupby(level="datetime").transform("median")

# 高波动: 反转信号(-mom)，低波动: 动量信号(+mom)
is_high_vol = vol_20d > vol_median
factor = mom_20d.copy()
factor[is_high_vol] = -factor[is_high_vol]

factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_rank_interaction_pe_mom": {
        "description": "PE排名x动量排名交互项：低PE+正动量=价值确认，非线性捕捉",
        "code": r'''import pandas as pd
import numpy as np

FACTOR_NAME = "m_rank_interaction_pe_mom"

pv = pd.read_hdf("daily_pv.h5")
db = pd.read_hdf("daily_basic.h5")

# 动量排名（截面）
mom_10d = pv["close"].groupby(level="instrument").pct_change(10)
mom_rank = mom_10d.groupby(level="datetime").rank(pct=True)

# PE排名（截面），反向：低PE=高rank
pe_ttm = db["db_pe_ttm"]
pe_rank = 1.0 - pe_ttm.groupby(level="datetime").rank(pct=True)  # 低PE -> 高值

# 交互项：两个rank相乘（值 0-1 之间）
factor = pe_rank * mom_rank
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    # ══════════════════════════════════════════
    # C. 多源深度融合因子
    # ══════════════════════════════════════════

    "m_smart_money_momentum": {
        "description": "聪明资金动量：大单净流入方向的价格动量，过滤散户噪音后的真实趋势",
        "code": r'''import pandas as pd
import numpy as np

FACTOR_NAME = "m_smart_money_momentum"

pv = pd.read_hdf("daily_pv.h5")
mf = pd.read_hdf("moneyflow.h5")

# 大单+超大单净额
big_net = (mf["mf_lg_buy_amt"] - mf["mf_lg_sell_amt"]) + (mf["mf_elg_buy_amt"] - mf["mf_elg_sell_amt"])

# 大单净额方向（10日累计）
big_net_10d = big_net.groupby(level="instrument").rolling(10, min_periods=5).sum().droplevel(0)
big_direction = np.sign(big_net_10d)  # +1 or -1

# 价格动量
mom_10d = pv["close"].groupby(level="instrument").pct_change(10)

# 聪明资金方向的动量（同向=正，逆向=负）
factor = mom_10d * big_direction
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_earnings_quality_composite": {
        "description": "盈利质量复合因子：营收增速×毛利率变化×行业相对PE，多维度融合质量信号",
        "code": r'''import pandas as pd
import numpy as np

FACTOR_NAME = "m_earnings_quality_composite"

bb = pd.read_hdf("bak_basic.h5")
db = pd.read_hdf("daily_basic.h5")
sector = pd.read_hdf("sector_data.h5")

# 1. 营收增速排名
rev_yoy_rank = bb["bb_rev_yoy"].groupby(level="datetime").rank(pct=True)

# 2. 毛利率20日变化排名
gpr_change = bb["bb_gpr"].groupby(level="instrument").diff(20)
gpr_change_rank = gpr_change.groupby(level="datetime").rank(pct=True)

# 3. 行业相对PE排名（低PE=高值）
pe_ratio = db["db_pe_ttm"] / sector["sw2_pe"].replace(0, np.nan)
pe_ratio = pe_ratio.replace([np.inf, -np.inf], np.nan)
pe_relative_rank = 1.0 - pe_ratio.groupby(level="datetime").rank(pct=True)

# 等权复合
factor = (rev_yoy_rank + gpr_change_rank + pe_relative_rank) / 3.0
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_chip_cost_momentum_fusion": {
        "description": "筹码成本动量融合：筹码获利盘×成本重心偏移×价格动量，三源信号叠加",
        "code": r'''import pandas as pd
import numpy as np

FACTOR_NAME = "m_chip_cost_momentum_fusion"

pv = pd.read_hdf("daily_pv.h5")
cyq = pd.read_hdf("cyq_perf.h5")

# 1. 获利盘比例截面排名
winner_rank = cyq["cp_winner_rate"].groupby(level="datetime").rank(pct=True)

# 2. 价格vs加权成本偏离 (close/cost - 1)
cost_deviation = pv["close"] / cyq["cp_weight_avg"].replace(0, np.nan) - 1
cost_deviation = cost_deviation.replace([np.inf, -np.inf], np.nan)
cost_rank = cost_deviation.groupby(level="datetime").rank(pct=True)

# 3. 10日动量排名
mom_10d = pv["close"].groupby(level="instrument").pct_change(10)
mom_rank = mom_10d.groupby(level="datetime").rank(pct=True)

# 等权融合（三个rank都在[0,1]之间）
factor = (winner_rank + cost_rank + mom_rank) / 3.0
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_quad_source_alpha": {
        "description": "四源融合alpha：量价动量×基本面质量×资金流方向×筹码支撑，最大化信息融合",
        "code": r'''import pandas as pd
import numpy as np

FACTOR_NAME = "m_quad_source_alpha"

pv = pd.read_hdf("daily_pv.h5")
db = pd.read_hdf("daily_basic.h5")
mf = pd.read_hdf("moneyflow.h5")
cyq = pd.read_hdf("cyq_perf.h5")

# 维度1: 量价 - 10日动量截面排名
mom_10d = pv["close"].groupby(level="instrument").pct_change(10)
dim1 = mom_10d.groupby(level="datetime").rank(pct=True)

# 维度2: 基本面 - 低PE截面排名
dim2 = 1.0 - db["db_pe_ttm"].groupby(level="datetime").rank(pct=True)

# 维度3: 资金流 - 大单净额10日累计截面排名
big_net = (mf["mf_lg_buy_amt"] - mf["mf_lg_sell_amt"]) + (mf["mf_elg_buy_amt"] - mf["mf_elg_sell_amt"])
big_net_10d = big_net.groupby(level="instrument").rolling(10, min_periods=5).sum().droplevel(0)
dim3 = big_net_10d.groupby(level="datetime").rank(pct=True)

# 维度4: 筹码 - 获利盘变化截面排名（上升=正面）
winner_chg = cyq["cp_winner_rate"].groupby(level="instrument").diff(10)
dim4 = winner_chg.groupby(level="datetime").rank(pct=True)

# 等权融合
factor = (dim1 + dim2 + dim3 + dim4) / 4.0
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    # ══════════════════════════════════════════
    # D. 高频衍生 + 其他补充
    # ══════════════════════════════════════════

    "m_atr_14d_inv": {
        "description": "14日ATR反向：低真实波幅溢价，比简单波动率更准确的风险度量",
        "code": r'''import pandas as pd
import numpy as np

FACTOR_NAME = "m_atr_14d_inv"

pv = pd.read_hdf("daily_pv.h5")

high = pv["high"]
low = pv["low"]
close = pv["close"]
prev_close = close.groupby(level="instrument").shift(1)

# True Range = max(H-L, |H-prevC|, |L-prevC|)
tr1 = high - low
tr2 = (high - prev_close).abs()
tr3 = (low - prev_close).abs()
true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

# 14日ATR
atr_14 = true_range.groupby(level="instrument").rolling(14, min_periods=10).mean().droplevel(0)

# 标准化：ATR / close（消除价格量纲）
atr_pct = atr_14 / close.replace(0, np.nan)

# 反向：低ATR溢价
factor = -atr_pct
factor = factor.replace([np.inf, -np.inf], np.nan)
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_overnight_return_5d": {
        "description": "5日平均隔夜收益率：open/prev_close-1的5日均值，衡量盘后信息反应",
        "code": r'''import pandas as pd
import numpy as np

FACTOR_NAME = "m_overnight_return_5d"

pv = pd.read_hdf("daily_pv.h5")

prev_close = pv["close"].groupby(level="instrument").shift(1)
overnight = pv["open"] / prev_close - 1

# 5日均值
factor = overnight.groupby(level="instrument").rolling(5, min_periods=3).mean().droplevel(0)
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_holder_concentration_change": {
        "description": "股东集中度变化：股东人数20日减少率，股东减少=筹码集中=正面信号",
        "code": r'''import pandas as pd
import numpy as np

FACTOR_NAME = "m_holder_concentration_change"

bb = pd.read_hdf("bak_basic.h5")

holder_num = bb["bb_holder_num"]
# 20日变化率（减少为负）
holder_chg = holder_num.groupby(level="instrument").pct_change(20)

# 反向：股东减少=正值
factor = -holder_chg
factor = factor.replace([np.inf, -np.inf], np.nan)
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_industry_relative_mf_divergence": {
        "description": "个股vs行业资金流背离：个股大单净流入高于行业水平=超额资金关注，四源融合",
        "code": r'''import pandas as pd
import numpy as np

FACTOR_NAME = "m_industry_relative_mf_divergence"

mf = pd.read_hdf("moneyflow.h5")
sector = pd.read_hdf("sector_data.h5")
db = pd.read_hdf("daily_basic.h5")

# 个股大单净额 / 流通市值（标准化）
stock_big_net = (mf["mf_lg_buy_amt"] - mf["mf_lg_sell_amt"]) + (mf["mf_elg_buy_amt"] - mf["mf_elg_sell_amt"])
stock_mf_pct = stock_big_net / db["db_circ_mv"].replace(0, np.nan)

# 行业净资金流 / 行业市值
industry_mf_pct = sector["sw2_mf_net_amt"] / sector["sw2_total_mv"].replace(0, np.nan)

# 个股超额 = 个股资金流强度 - 行业资金流强度
excess_mf = stock_mf_pct - industry_mf_pct
excess_mf = excess_mf.replace([np.inf, -np.inf], np.nan)

# 10日平滑
factor = excess_mf.groupby(level="instrument").rolling(10, min_periods=5).mean().droplevel(0)
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

    # Step 1: save all factors
    print("=" * 60)
    print(f"Step 1: Save {len(FACTORS)} factors to DB + LLM classify")
    print("=" * 60)
    save_results = {}
    for i, (fname, fdef) in enumerate(FACTORS.items()):
        print(f"  [{i+1}/{len(FACTORS)}] {fname}...", end=" ", flush=True)
        try:
            r = await svc.save_factor(
                factor_name=fname,
                code_text=fdef["code"],
                description=fdef["description"],
            )
            cls = r.get("classification", {}) or {}
            cat = cls.get("category", "-")
            print(f"OK ({cat})")
            save_results[fname] = r
        except Exception as e:
            print(f"FAIL: {e}")
            save_results[fname] = {"error": str(e)}

    saved_names = [f for f, r in save_results.items() if "error" not in r]
    print(f"\nSaved: {len(saved_names)}/{len(FACTORS)}")

    # Step 2: batch compute metrics (5 per batch)
    print("\n" + "=" * 60)
    print("Step 2: Compute independent metrics")
    print("=" * 60)
    batch_size = 5
    all_metrics = {}
    for i in range(0, len(saved_names), batch_size):
        batch = saved_names[i:i+batch_size]
        print(f"  Batch {i//batch_size+1}: {', '.join(batch)}")
        try:
            result = await svc.batch_compute_metrics(factor_names=batch)
            if result.get("success"):
                for fname, w in result.get("factors", {}).items():
                    all_metrics[fname] = w.get("full", {})
                elog = result.get("execution_log", {})
                ok_cnt = sum(1 for v in elog.values() if v.get("status") == "ok")
                err_cnt = sum(1 for v in elog.values() if v.get("status") == "error")
                print(f"    OK={ok_cnt} ERR={err_cnt} dur={result.get('total_duration_sec', 0):.0f}s")
                for fn, el in elog.items():
                    if el.get("status") == "error":
                        err_msg = el.get("error", "")[:120]
                        print(f"    ERR {fn}: {err_msg}")
            else:
                print(f"    BATCH FAILED: {result.get('error', '')[:200]}")
        except Exception as e:
            print(f"    BATCH ERROR: {e}")

    # Step 3: Summary
    print(f"\n{'='*80}")
    print(f"Total: {time.time()-t_total:.0f}s")
    print(f"Saved: {len(saved_names)}/{len(FACTORS)}")
    print(f"With metrics: {len(all_metrics)}/{len(saved_names)}")

    print(f"\n{'Factor':<45} {'IC':>8} {'ICIR':>8} {'Sharpe':>8} {'AnnRet':>8}")
    print("-" * 85)
    for fname in FACTORS:
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

    # Highlight strong factors
    strong = [(f, m) for f, m in all_metrics.items()
              if m.get("ic_mean") is not None and abs(m["ic_mean"]) > 0.02]
    if strong:
        print(f"\nStrong factors (|IC| > 0.02):")
        for f, m in sorted(strong, key=lambda x: abs(x[1]["ic_mean"]), reverse=True):
            print(f"  {f}: IC={m['ic_mean']:.4f}, Sharpe={m.get('top_sharpe', 0):.2f}")


if __name__ == "__main__":
    asyncio.run(main())
