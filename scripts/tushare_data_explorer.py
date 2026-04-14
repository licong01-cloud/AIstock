"""
Tushare 数据源探索分析脚本
检查高优先级数据接口的可用性、覆盖率、数据质量
用于评估哪些数据可用于因子研发
"""
import tushare as ts
import pandas as pd
import time
import sys
import os
from datetime import datetime, timedelta

# 从 .env 读取 token
TOKEN = "befde989d7691539d75ec722e88ccc61b1932d1eab8260f785a4822e"
pro = ts.pro_api(TOKEN)

# 测试日期
TEST_DATE = "20260327"  # 最近交易日
TEST_CODE = "000001.SZ"  # 平安银行
RECENT_QUARTER = "20251231"

def safe_call(api_name, **kwargs):
    """安全调用 Tushare API，返回 DataFrame 或 None"""
    try:
        time.sleep(0.3)
        func = getattr(pro, api_name)
        df = func(**kwargs)
        return df
    except Exception as e:
        return f"ERROR: {e}"


def analyze_api(name, description, df, date_col=None):
    """分析 API 返回的数据"""
    print(f"\n{'='*70}")
    print(f"  {name} — {description}")
    print(f"{'='*70}")

    if isinstance(df, str):  # Error
        print(f"  !! {df}")
        return {"name": name, "status": "ERROR", "error": df}

    if df is None or df.empty:
        print(f"  !! 无数据返回")
        return {"name": name, "status": "EMPTY"}

    print(f"  行数: {len(df)}")
    print(f"  列数: {len(df.columns)}")
    print(f"  字段: {', '.join(df.columns.tolist())}")

    # 数值列统计
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    if numeric_cols:
        non_null = {col: df[col].notna().sum() for col in numeric_cols}
        null_pct = {col: f"{df[col].isna().mean()*100:.1f}%" for col in numeric_cols}
        print(f"  数值列 NULL 率: {null_pct}")

    # 日期范围
    if date_col and date_col in df.columns:
        dates = pd.to_datetime(df[date_col], errors='coerce')
        valid_dates = dates.dropna()
        if len(valid_dates) > 0:
            print(f"  日期范围: {valid_dates.min().strftime('%Y-%m-%d')} ~ {valid_dates.max().strftime('%Y-%m-%d')}")

    # 股票覆盖
    for col in ['ts_code', 'code']:
        if col in df.columns:
            unique_codes = df[col].nunique()
            print(f"  股票覆盖: {unique_codes} 只")
            break

    # 样本数据
    print(f"  样本 (前3行):")
    sample = df.head(3).to_string(index=False, max_colwidth=20)
    for line in sample.split('\n'):
        print(f"    {line}")

    return {
        "name": name,
        "status": "OK",
        "rows": len(df),
        "cols": len(df.columns),
        "columns": df.columns.tolist(),
    }


results = []
print("=" * 70)
print("  Tushare 数据源可用性探索")
print(f"  测试日期: {TEST_DATE}  测试股票: {TEST_CODE}")
print(f"  执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 70)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 第一组: 日频数据（可直接用于因子）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

print("\n\n" + "━"*70)
print("  第一组: 日频全市场数据（可直接转h5）")
print("━"*70)

# 1. 融资融券 - margin_detail
df = safe_call("margin_detail", trade_date=TEST_DATE)
r = analyze_api("margin_detail", "融资融券交易明细（日频，按交易日）", df, "trade_date")
results.append(r)

# 2. 涨跌停统计 - limit_list_d
df = safe_call("limit_list_d", trade_date=TEST_DATE)
r = analyze_api("limit_list_d", "每日涨跌停统计（日频，全市场）", df, "trade_date")
results.append(r)

# 3. 涨跌停价格 - stk_limit
df = safe_call("stk_limit", trade_date=TEST_DATE)
r = analyze_api("stk_limit", "涨跌停价格（日频，已入库参考）", df, "trade_date")
results.append(r)

# 4. 大宗交易 - block_trade
df = safe_call("block_trade", trade_date=TEST_DATE)
r = analyze_api("block_trade", "大宗交易（日频，按交易日）", df, "trade_date")
results.append(r)

# 5. 龙虎榜 - top_list
df = safe_call("top_list", trade_date=TEST_DATE)
r = analyze_api("top_list", "龙虎榜每日明细（日频）", df, "trade_date")
results.append(r)

# 6. 龙虎榜机构交易 - top_inst
df = safe_call("top_inst", trade_date=TEST_DATE)
r = analyze_api("top_inst", "龙虎榜机构交易明细（日频）", df, "trade_date")
results.append(r)

# 7. 股票技术因子 - stk_factor
df = safe_call("stk_factor", trade_date=TEST_DATE)
r = analyze_api("stk_factor", "股票技术因子（Tushare预计算，日频）", df, "trade_date")
results.append(r)

# 8. 每日股东户数
df = safe_call("stk_holdernumber", ts_code=TEST_CODE, start_date="20250101", end_date="20260401")
r = analyze_api("stk_holdernumber", "股东户数（按股票查，非日频）", df, "end_date")
results.append(r)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 第二组: 非日频/事件驱动数据（需转换为日频因子）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

print("\n\n" + "━"*70)
print("  第二组: 事件驱动/非日频数据（需forward-fill转日频）")
print("━"*70)

# 9. 业绩预告 - forecast
df = safe_call("forecast", ann_date=TEST_DATE)
if isinstance(df, str) or df is None or (hasattr(df, 'empty') and df.empty):
    # 尝试按期间查
    df = safe_call("forecast", period=RECENT_QUARTER)
r = analyze_api("forecast", "业绩预告（事件驱动，按公告日/报告期）", df, "ann_date")
results.append(r)

# 10. 股东增减持 - stk_holdertrade
df = safe_call("stk_holdertrade", ann_date=TEST_DATE)
if isinstance(df, str) or df is None or (hasattr(df, 'empty') and df.empty):
    df = safe_call("stk_holdertrade", ts_code=TEST_CODE, start_date="20250101", end_date="20260401")
r = analyze_api("stk_holdertrade", "股东增减持（事件驱动）", df, "ann_date")
results.append(r)

# 11. 股权质押 - pledge_stat
df = safe_call("pledge_stat", ts_code=TEST_CODE)
r = analyze_api("pledge_stat", "股权质押统计（按股票）", df, "end_date")
results.append(r)

# 12. 分红送股 - dividend
df = safe_call("dividend", ts_code=TEST_CODE)
r = analyze_api("dividend", "分红送股数据（事件驱动）", df, "ex_date")
results.append(r)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 第三组: 北向资金/港股通
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

print("\n\n" + "━"*70)
print("  第三组: 北向资金/港股通")
print("━"*70)

# 13. 沪深港通持股 - hk_hold
df = safe_call("hk_hold", trade_date=TEST_DATE)
r = analyze_api("hk_hold", "北向资金持股明细（日频，按交易日）", df, "trade_date")
results.append(r)

# 14. 沪深港通十大成交 - hsgt_top10
df = safe_call("hsgt_top10", trade_date=TEST_DATE)
r = analyze_api("hsgt_top10", "北向资金十大成交股（日频）", df, "trade_date")
results.append(r)

# 15. 港股通每日资金流 - moneyflow_hsgt
df = safe_call("moneyflow_hsgt", trade_date=TEST_DATE)
r = analyze_api("moneyflow_hsgt", "沪深港通资金流向（日频）", df, "trade_date")
results.append(r)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 第四组: 财务数据（季频，可转日频）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

print("\n\n" + "━"*70)
print("  第四组: 财务数据（季频，forward-fill转日频）")
print("━"*70)

# 16. 财务指标 - fina_indicator
df = safe_call("fina_indicator", ts_code=TEST_CODE, start_date="20240101", end_date="20260101")
r = analyze_api("fina_indicator", "财务指标（季频，60+字段）", df, "ann_date")
results.append(r)

# 17. 利润表 - income
df = safe_call("income", ts_code=TEST_CODE, start_date="20240101", end_date="20260101")
r = analyze_api("income", "利润表（季频）", df, "ann_date")
results.append(r)

# 18. 现金流量表 - cashflow
df = safe_call("cashflow", ts_code=TEST_CODE, start_date="20240101", end_date="20260101")
r = analyze_api("cashflow", "现金流量表（季频）", df, "ann_date")
results.append(r)

# 19. 分析师预测 - report_rc
df = safe_call("report_rc", ts_code=TEST_CODE)
r = analyze_api("report_rc", "卖方盈利预测（分析师一致预期）", df, "report_date")
results.append(r)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 第五组: 其他潜在数据源
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

print("\n\n" + "━"*70)
print("  第五组: 其他潜在数据源")
print("━"*70)

# 20. 股票回购 - repurchase
df = safe_call("repurchase", ann_date=TEST_DATE)
if isinstance(df, str) or df is None or (hasattr(df, 'empty') and df.empty):
    df = safe_call("repurchase", start_date="20260101", end_date="20260401")
r = analyze_api("repurchase", "股票回购（事件驱动）", df, "ann_date")
results.append(r)

# 21. 限售股解禁 - share_float
df = safe_call("share_float", ts_code=TEST_CODE)
r = analyze_api("share_float", "限售股解禁计划（事件驱动）", df, "float_date")
results.append(r)

# 22. 股票账户统计 - stk_account
df = safe_call("stk_account", start_date="20260101", end_date="20260401")
r = analyze_api("stk_account", "股票账户统计（周频，市场情绪）", df, "date")
results.append(r)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 综合评估
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

print("\n\n" + "="*70)
print("  综合评估: 因子研发优先级")
print("="*70)

ok_apis = [r for r in results if r.get("status") == "OK"]
err_apis = [r for r in results if r.get("status") == "ERROR"]
empty_apis = [r for r in results if r.get("status") == "EMPTY"]

print(f"\n  可用: {len(ok_apis)}  |  空数据: {len(empty_apis)}  |  错误: {len(err_apis)}")

if err_apis:
    print(f"\n  !! 不可用接口:")
    for r in err_apis:
        print(f"    - {r['name']}: {r.get('error','')[:80]}")

if empty_apis:
    print(f"\n  !! 空数据接口:")
    for r in empty_apis:
        print(f"    - {r['name']}")

print(f"\n  可用接口详情:")
for r in ok_apis:
    rows = r.get("rows", 0)
    cols = r.get("cols", 0)
    print(f"    {r['name']:25s}  {rows:>6d} 行  {cols:>3d} 列")

print(f"\n{'='*70}")
print(f"  探索完成")
print(f"{'='*70}")
