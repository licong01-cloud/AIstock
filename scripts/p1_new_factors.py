#!/usr/bin/env python3
"""P1 新因子批量研发 + 入库 + 指标计算。

直接调用 ManualFactorService 完成全流程。
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

# ══════════════════════════════════════════
# 因子定义
# ══════════════════════════════════════════

FACTORS = {

    # ── P1-A: 行业维度因子（sector_data 开发）──

    "m_industry_momentum_20d": {
        "description": "申万二级行业20日动量：行业近20日累计涨幅，捕捉行业轮动信号",
        "code": '''import pandas as pd
import numpy as np

FACTOR_NAME = "m_industry_momentum_20d"

sector = pd.read_hdf("sector_data.h5")
sw2_close = sector["sw2_close"]
# 20日行业涨幅
industry_mom = sw2_close.groupby(level="instrument").pct_change(20)
industry_mom = industry_mom.rename(FACTOR_NAME)

result_df = industry_mom.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_stock_vs_industry_mom_20d": {
        "description": "个股20日动量减去所属行业20日动量：行业中性动量，剥离行业beta后的个股alpha信号",
        "code": '''import pandas as pd
import numpy as np

FACTOR_NAME = "m_stock_vs_industry_mom_20d"

pv = pd.read_hdf("daily_pv.h5")
sector = pd.read_hdf("sector_data.h5")

required_sector_columns = {"sw2_close", "l2_code_id"}
missing_sector_columns = required_sector_columns.difference(sector.columns)
if missing_sector_columns:
    raise KeyError(f"sector_data.h5 missing required columns: {sorted(missing_sector_columns)}")
if not sector.index.is_unique:
    raise ValueError("sector_data.h5 index must be unique by (datetime, instrument)")

# 个股20日动量
stock_mom = (
    pv["close"]
    .sort_index()
    .groupby(level="instrument", sort=False)
    .pct_change(20, fill_method=None)
)

# 先构造唯一的 (datetime, l2_code_id) 行业面板，再沿行业自身时序计算。
# 禁止沿 instrument 直接 pct_change，否则个股换行业时会把两个行业串成一条序列。
sector_rows = sector[["sw2_close", "l2_code_id"]].reset_index()
l2_numeric = pd.to_numeric(sector_rows["l2_code_id"], errors="raise")
finite_l2 = l2_numeric.notna()
if ((l2_numeric[finite_l2] % 1) != 0).any():
    raise ValueError("l2_code_id must contain integer category ids")
sector_rows["l2_code_id"] = l2_numeric
known_sector_rows = sector_rows[finite_l2 & (l2_numeric >= 0)].copy()

conflicts = (
    known_sector_rows.groupby(["datetime", "l2_code_id"])["sw2_close"]
    .nunique(dropna=True)
)
if (conflicts > 1).any():
    bad_key = conflicts[conflicts > 1].index[0]
    raise ValueError(f"conflicting sw2_close values for sector-day {bad_key}")

sector_panel = (
    known_sector_rows.dropna(subset=["sw2_close"])
    .drop_duplicates(["datetime", "l2_code_id"])
    .set_index(["datetime", "l2_code_id"])["sw2_close"]
    .unstack("l2_code_id")
    .sort_index()
)
industry_mom_panel = sector_panel.pct_change(20, fill_method=None)
industry_mom_long = (
    industry_mom_panel.rename_axis(index="datetime", columns="l2_code_id")
    .reset_index()
    .melt(
        id_vars="datetime",
        var_name="l2_code_id",
        value_name="industry_mom",
    )
    .set_index(["datetime", "l2_code_id"])["industry_mom"]
)

# 按当日 PIT membership 映射回股票；unknown=-1 保持缺失，不静默回退。
membership = sector_rows.loc[
    finite_l2 & (l2_numeric >= 0), ["datetime", "instrument", "l2_code_id"]
]
industry_mom = (
    membership.merge(
        industry_mom_long.reset_index(),
        on=["datetime", "l2_code_id"],
        how="left",
        validate="many_to_one",
    )
    .set_index(["datetime", "instrument"])["industry_mom"]
    .reindex(stock_mom.index)
)

# 行业中性动量 = 个股动量 - 行业动量
factor = stock_mom - industry_mom
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_industry_pe_deviation": {
        "description": "个股PE与行业PE的偏离度：log(个股PE/行业PE)，捕捉行业内相对估值信号",
        "code": '''import pandas as pd
import numpy as np

FACTOR_NAME = "m_industry_pe_deviation"

db = pd.read_hdf("daily_basic.h5")
sector = pd.read_hdf("sector_data.h5")

pe_stock = db["db_pe_ttm"]
pe_industry = sector["sw2_pe"]

# 行业内相对PE偏离 = log(个股PE / 行业PE)
# 避免除以0和负数
ratio = pe_stock / pe_industry
ratio = ratio.replace([np.inf, -np.inf], np.nan)
# 使用log压缩分布
factor = np.log(ratio.clip(lower=0.01))
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_industry_mf_strength_10d": {
        "description": "行业10日净资金流强度：行业净资金流10日均值/行业总市值，反映板块资金偏好",
        "code": '''import pandas as pd
import numpy as np

FACTOR_NAME = "m_industry_mf_strength_10d"

sector = pd.read_hdf("sector_data.h5")

net_mf = sector["sw2_mf_net_amt"]
total_mv = sector["sw2_total_mv"]

# 10日滚动平均净资金流 / 总市值
net_mf_ma10 = net_mf.groupby(level="instrument").rolling(10, min_periods=5).mean().droplevel(0)
factor = net_mf_ma10 / total_mv.replace(0, np.nan)
factor = factor.replace([np.inf, -np.inf], np.nan)
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_industry_breadth_10d": {
        "description": "行业景气宽度：行业10日涨幅相对全行业的排名百分位，反映板块强弱",
        "code": '''import pandas as pd
import numpy as np

FACTOR_NAME = "m_industry_breadth_10d"

sector = pd.read_hdf("sector_data.h5")
sw2_close = sector["sw2_close"]

# 行业10日涨幅
industry_ret_10d = sw2_close.groupby(level="instrument").pct_change(10)

# 截面排名百分位
factor = industry_ret_10d.groupby(level="datetime").rank(pct=True)
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    # ── P1-B: 统计分布因子（STAT 补强）──

    "m_return_skewness_20d": {
        "description": "20日收益偏度：Boyer et al.(2010)——高偏度被高估，反向使用（取负号）",
        "code": '''import pandas as pd
import numpy as np

FACTOR_NAME = "m_return_skewness_20d"

pv = pd.read_hdf("daily_pv.h5")
ret = pv["close"].groupby(level="instrument").pct_change(1)

# 20日滚动偏度（取负号：高偏度 -> 低信号）
skew = ret.groupby(level="instrument").rolling(20, min_periods=15).skew().droplevel(0)
factor = -skew  # 反向：低偏度溢价
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_max_drawdown_20d": {
        "description": "20日最大回撤（取负号使大回撤为正值）：下行风险因子，低回撤溢价",
        "code": '''import pandas as pd
import numpy as np

FACTOR_NAME = "m_max_drawdown_20d"

pv = pd.read_hdf("daily_pv.h5")
close = pv["close"]

def rolling_max_drawdown(group, window=20):
    rolling_max = group.rolling(window, min_periods=10).max()
    drawdown = group / rolling_max - 1  # 负值
    return drawdown.rolling(window, min_periods=10).min()

# 反向：高回撤 -> 低值（惩罚高回撤）
mdd = close.groupby(level="instrument").apply(rolling_max_drawdown).droplevel(0)
factor = mdd  # 已经是负值，值越大（越接近0）=回撤越小=越好
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_tail_risk_5pct": {
        "description": "5%分位收益（VaR代理）：衡量尾部风险暴露，低尾部风险溢价",
        "code": '''import pandas as pd
import numpy as np

FACTOR_NAME = "m_tail_risk_5pct"

pv = pd.read_hdf("daily_pv.h5")
ret = pv["close"].groupby(level="instrument").pct_change(1)

# 60日滚动5%分位收益（VaR），越大=风险越小
factor = ret.groupby(level="instrument").rolling(60, min_periods=40).quantile(0.05).droplevel(0)
# 反向：负值越大=极端损失越小=越安全
factor = -factor  # 取负号使低尾部风险为正
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    # ── P1-C: 质量因子重构（QUAL 重建）──

    "m_profit_yoy_change": {
        "description": "净利润同比增速的变化率：盈利加速度因子，捕捉盈利趋势拐点",
        "code": '''import pandas as pd
import numpy as np

FACTOR_NAME = "m_profit_yoy_change"

bb = pd.read_hdf("bak_basic.h5")
profit_yoy = bb["bb_profit_yoy"]

# 20日变化（基本面数据更新较慢，用较长窗口）
profit_change = profit_yoy.groupby(level="instrument").diff(20)
factor = profit_change.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_gpr_npr_spread_change": {
        "description": "毛利率与净利率差值的变化：成本控制能力变动，差值缩小=费用管控改善",
        "code": '''import pandas as pd
import numpy as np

FACTOR_NAME = "m_gpr_npr_spread_change"

bb = pd.read_hdf("bak_basic.h5")
gpr = bb["bb_gpr"]
npr = bb["bb_npr"]

# 毛利率-净利率的差（费用率代理）
spread = gpr - npr

# 20日变化（差值缩小 = 费用率下降 = 正面信号）
factor = -spread.groupby(level="instrument").diff(20)
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    # ── P1-D: 多源融合因子 ──

    "m_value_lowliq_composite": {
        "description": "低估值×低流动性复合因子：PE排名百分位反转×换手率反转，价值+流动性双溢价",
        "code": '''import pandas as pd
import numpy as np

FACTOR_NAME = "m_value_lowliq_composite"

db = pd.read_hdf("daily_basic.h5")
pe_ttm = db["db_pe_ttm"]
turnover = db["db_turnover_rate"]

# 截面排名百分位
pe_rank = pe_ttm.groupby(level="datetime").rank(pct=True)
tr_rank = turnover.groupby(level="datetime").rank(pct=True)

# 反向：低PE + 低换手 = 高值
factor = (1 - pe_rank) * 0.5 + (1 - tr_rank) * 0.5
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_momentum_profit_confirmed": {
        "description": "有盈利支撑的动量：5日涨幅×净利润同比增速截面排名，过滤纯炒作型动量",
        "code": '''import pandas as pd
import numpy as np

FACTOR_NAME = "m_momentum_profit_confirmed"

pv = pd.read_hdf("daily_pv.h5")
bb = pd.read_hdf("bak_basic.h5")

# 5日动量
mom_5d = pv["close"].groupby(level="instrument").pct_change(5)

# 盈利同比排名（截面）
profit_rank = bb["bb_profit_yoy"].groupby(level="datetime").rank(pct=True)

# 交互：动量 × 盈利排名（盈利好+涨的才是好信号）
factor = mom_5d * profit_rank
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_chip_moneyflow_divergence": {
        "description": "筹码松动+资金流入背离因子：获利盘比例下降但大单净流入增加，主力吸筹信号",
        "code": '''import pandas as pd
import numpy as np

FACTOR_NAME = "m_chip_moneyflow_divergence"

cyq = pd.read_hdf("cyq_perf.h5")
mf = pd.read_hdf("moneyflow.h5")

# 获利盘比例10日变化（下降=筹码松动）
winner_change = cyq["cp_winner_rate"].groupby(level="instrument").diff(10)

# 大单净额10日均值截面排名
lg_net = mf["mf_lg_buy_amt"] - mf["mf_lg_sell_amt"]
elg_net = mf["mf_elg_buy_amt"] - mf["mf_elg_sell_amt"]
big_net = lg_net + elg_net
big_net_ma = big_net.groupby(level="instrument").rolling(10, min_periods=5).mean().droplevel(0)
big_net_rank = big_net_ma.groupby(level="datetime").rank(pct=True)

# 背离信号：获利盘下降（负值）但大单流入（高排名）
# winner_change 越负 + big_net_rank 越高 = 背离越强
factor = -winner_change * big_net_rank
factor = factor.replace([np.inf, -np.inf], np.nan)
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    # ── P1-E: 高频衍生因子 ──

    "m_close_location_value": {
        "description": "日内价格位置：(close-low)/(high-low)，反映多空力量对比",
        "code": '''import pandas as pd
import numpy as np

FACTOR_NAME = "m_close_location_value"

pv = pd.read_hdf("daily_pv.h5")
high = pv["high"]
low = pv["low"]
close = pv["close"]

# (close-low)/(high-low)，接近1=收在日内高位=多头强势
hl_range = high - low
factor = (close - low) / hl_range.replace(0, np.nan)
# 5日平滑
factor = factor.groupby(level="instrument").rolling(5, min_periods=3).mean().droplevel(0)
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
''',
    },

    "m_volume_price_divergence_10d": {
        "description": "量价背离因子：10日价格涨幅与成交量涨幅的差异，量价齐升为正信号",
        "code": '''import pandas as pd
import numpy as np

FACTOR_NAME = "m_volume_price_divergence_10d"

pv = pd.read_hdf("daily_pv.h5")

# 10日价格变化
price_chg = pv["close"].groupby(level="instrument").pct_change(10)
# 10日成交量变化
vol_chg = pv["volume"].groupby(level="instrument").pct_change(10)

# 截面标准化后相乘（量价齐升 or 量价齐跌 = 高值）
price_rank = price_chg.groupby(level="datetime").rank(pct=True) - 0.5
vol_rank = vol_chg.groupby(level="datetime").rank(pct=True) - 0.5
factor = price_rank * vol_rank
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

    # Step 1: 批量入库（save_factor 不需要 WSL）
    print("=" * 60)
    print("Step 1: 批量入库因子到 DB + LLM 分类")
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
            grade = cls.get("grade", "-")
            print(f"OK (cat={cat}, grade={grade})")
            save_results[fname] = r
        except Exception as e:
            print(f"FAIL: {e}")
            save_results[fname] = {"error": str(e)}

    saved_names = [f for f, r in save_results.items() if "error" not in r]
    print(f"\n入库成功: {len(saved_names)}/{len(FACTORS)}")

    # Step 2: 批量计算独立指标（使用 batch_compute_metrics，每批 5 个）
    print("\n" + "=" * 60)
    print("Step 2: 批量计算独立指标")
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
                print(f"    OK={ok_cnt}, ERR={err_cnt}, dur={result.get('total_duration_sec', 0):.0f}s")
                # 打印执行错误详情
                for fn, el in elog.items():
                    if el.get("status") == "error":
                        print(f"    {fn}: {el.get('error', '')[:150]}")
            else:
                print(f"    BATCH FAILED: {result.get('error', '')[:200]}")
        except Exception as e:
            print(f"    BATCH ERROR: {e}")

    # Step 3: 汇总
    print(f"\n{'='*80}")
    print(f"Total: {time.time()-t_total:.0f}s")
    print(f"Factors saved: {len(saved_names)}/{len(FACTORS)}")
    print(f"Factors with metrics: {len(all_metrics)}/{len(saved_names)}")

    print(f"\n{'Factor':<45} {'IC':>8} {'Sharpe':>8} {'AnnRet':>8}")
    print("-" * 75)
    for fname in FACTORS:
        fm = all_metrics.get(fname, {})
        cls = (save_results.get(fname, {}).get("classification", {}) or {})
        ic = fm.get("ic_mean")
        sh = fm.get("top_sharpe")
        ar = fm.get("top_annual_return")
        ic_s = f"{ic:.4f}" if ic is not None else "-"
        sh_s = f"{sh:.2f}" if sh is not None else "-"
        ar_s = f"{ar*100:.1f}%" if ar is not None else "-"
        cat = cls.get("category", "-")
        print(f"{fname:<45} {ic_s:>8} {sh_s:>8} {ar_s:>8}  {cat}")


if __name__ == "__main__":
    asyncio.run(main())
