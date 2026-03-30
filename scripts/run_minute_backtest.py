"""分钟线回测验证 — 验证数据准确性 + 涨跌停限制 + 交易撮合
从分钟线 bin 聚合日线信号，再用分钟线价格撮合，全面验证数据链路。

数据: 2025-11 ~ 2026-02 分钟线 bin (全量 5489 股)
训练信号: 2025-12-01 ~ 2025-12-31 (用日线动量因子)
回测: 2026-01-02 ~ 2026-02-28
"""
import qlib
from qlib.data import D
import pandas as pd
import numpy as np
from collections import OrderedDict

# ========== 配置 ==========
PROVIDER_URI = "/mnt/f/Dev/AIstock/qlib_minute_full/bin"
BACKTEST_START = "2026-01-02"
BACKTEST_END = "2026-02-28"
TOPK = 30          # 每日持仓 top 30
INIT_CASH = 1e7    # 1000万
OPEN_COST = 0.0005
CLOSE_COST = 0.0015
MIN_COST = 5.0

qlib.init(provider_uri=PROVIDER_URI, region="cn")

# ========== Step 1: 获取全市场分钟线数据 ==========
print("=== Step 1: 加载分钟线数据 ===")
instruments = D.instruments("all")
inst_list = D.list_instruments(instruments=instruments, as_list=True, freq="1min")
print(f"  总股票数: {len(inst_list)}")

# 加载回测区间 + 前20天(用于计算动量) 的日线级数据
# 从分钟线聚合日线: 每天最后一分钟的 close, 全天 volume 之和
fields = ["$open", "$high", "$low", "$close", "$volume", "$amount",
          "$limit_up", "$limit_down"]

print("  加载分钟线数据中...")
df_min = D.features(
    instruments=inst_list,
    fields=fields,
    start_time="2025-12-01",
    end_time=BACKTEST_END,
    freq="1min",
)
print(f"  分钟线数据: {df_min.shape[0]:,} bars, {df_min.shape[1]} 列")

# ========== Step 2: 聚合日线 ==========
print("\n=== Step 2: 聚合日线数据 ===")
df_min = df_min.reset_index()
df_min["date"] = df_min["datetime"].dt.date

daily_agg = df_min.groupby(["instrument", "date"]).agg(
    open=("$open", "first"),
    high=("$high", "max"),
    low=("$low", "min"),
    close=("$close", "last"),
    volume=("$volume", "sum"),
    amount=("$amount", "sum"),
    limit_up_bars=("$limit_up", "sum"),
    limit_down_bars=("$limit_down", "sum"),
    bar_count=("$close", "count"),
).reset_index()

n_days = daily_agg["date"].nunique()
n_stocks = daily_agg["instrument"].nunique()
print(f"  日线数据: {len(daily_agg):,} 行, {n_stocks} 只股票, {n_days} 个交易日")

# 统计涨跌停
lu_days = daily_agg[daily_agg["limit_up_bars"] > 0]
ld_days = daily_agg[daily_agg["limit_down_bars"] > 0]
# 一字板: 全天所有 bar 都涨停/跌停
full_lu = daily_agg[daily_agg["limit_up_bars"] == daily_agg["bar_count"]]
full_ld = daily_agg[daily_agg["limit_down_bars"] == daily_agg["bar_count"]]
print(f"  有涨停 bar 的日股对: {len(lu_days):,}, 一字涨停: {len(full_lu):,}")
print(f"  有跌停 bar 的日股对: {len(ld_days):,}, 一字跌停: {len(full_ld):,}")

# ========== Step 3: 计算信号(简单5日动量) ==========
print("\n=== Step 3: 计算日线动量信号 ===")
daily_agg = daily_agg.sort_values(["instrument", "date"])
daily_agg["ret_5d"] = daily_agg.groupby("instrument")["close"].pct_change(5)
# 信号 = 5 日反转 (过去5日跌多的，未来反弹)
daily_agg["signal"] = -daily_agg["ret_5d"]

# 标记一字板不可交易
daily_agg["is_limit_up"] = daily_agg["limit_up_bars"] == daily_agg["bar_count"]
daily_agg["is_limit_down"] = daily_agg["limit_down_bars"] == daily_agg["bar_count"]

# ========== Step 4: 逐日回测 ==========
print("\n=== Step 4: 逐日回测 ===")
backtest_dates = sorted([d for d in daily_agg["date"].unique()
                         if d >= pd.Timestamp(BACKTEST_START).date()])
print(f"  回测交易日: {len(backtest_dates)}, {backtest_dates[0]} ~ {backtest_dates[-1]}")

portfolio = OrderedDict()  # instrument -> shares
cash = INIT_CASH
nav_history = []
trade_log = []
blocked_by_limit = 0
total_trades = 0

for i, today in enumerate(backtest_dates):
    today_data = daily_agg[daily_agg["date"] == today].copy()
    today_data = today_data.dropna(subset=["signal", "close"])
    today_data = today_data[today_data["volume"] > 0]  # 排除停牌

    # 排除一字涨停(不可买入) — 但已持有的可以继续持有
    buyable = today_data[~today_data["is_limit_up"]].copy()
    # 排除一字跌停(不可卖出) — 已持有的被锁定
    sellable_mask = ~today_data["is_limit_down"]

    # 目标持仓: 从可买入的股票中选 top-K
    buyable_sorted = buyable.sort_values("signal", ascending=False)
    target_stocks = set(buyable_sorted.head(TOPK)["instrument"].values)

    # 计算当前持仓市值
    portfolio_value = 0
    for inst, shares in portfolio.items():
        row = today_data[today_data["instrument"] == inst]
        if len(row) > 0:
            portfolio_value += shares * row.iloc[0]["close"]

    total_value = cash + portfolio_value

    # 卖出: 不在目标中的股票
    new_portfolio = OrderedDict()
    for inst, shares in portfolio.items():
        if inst not in target_stocks:
            row = today_data[today_data["instrument"] == inst]
            if len(row) == 0:
                new_portfolio[inst] = shares  # 停牌，保留
                continue
            r = row.iloc[0]
            if r["is_limit_down"]:
                # 跌停无法卖出
                new_portfolio[inst] = shares
                blocked_by_limit += 1
                continue
            # 卖出
            sell_price = r["close"]  # 用收盘价模拟
            proceeds = shares * sell_price
            cost = max(proceeds * CLOSE_COST, MIN_COST)
            cash += proceeds - cost
            total_trades += 1
            trade_log.append({
                "date": today, "instrument": inst, "action": "sell",
                "price": sell_price, "shares": shares, "cost": cost
            })
        else:
            new_portfolio[inst] = shares

    # 买入: 在目标中但未持有的
    stocks_to_buy = [s for s in target_stocks if s not in new_portfolio]
    if stocks_to_buy:
        per_stock_cash = cash / (len(stocks_to_buy) + 1)  # 留一份现金缓冲
        for inst in stocks_to_buy:
            row = today_data[today_data["instrument"] == inst]
            if len(row) == 0:
                continue
            r = row.iloc[0]
            if r["is_limit_up"]:
                blocked_by_limit += 1
                continue
            buy_price = r["close"]
            if buy_price <= 0:
                continue
            shares = int(per_stock_cash / buy_price / 100) * 100  # 100股整手
            if shares <= 0:
                continue
            cost_amount = shares * buy_price
            fee = max(cost_amount * OPEN_COST, MIN_COST)
            if cost_amount + fee > cash:
                continue
            cash -= cost_amount + fee
            new_portfolio[inst] = new_portfolio.get(inst, 0) + shares
            total_trades += 1
            trade_log.append({
                "date": today, "instrument": inst, "action": "buy",
                "price": buy_price, "shares": shares, "cost": fee
            })

    portfolio = new_portfolio

    # 日终净值
    port_val = sum(
        shares * today_data[today_data["instrument"] == inst].iloc[0]["close"]
        for inst, shares in portfolio.items()
        if len(today_data[today_data["instrument"] == inst]) > 0
    )
    nav = cash + port_val
    nav_history.append({"date": today, "nav": nav, "cash": cash,
                        "positions": len(portfolio), "port_val": port_val})

    if (i + 1) % 10 == 0 or i == 0 or i == len(backtest_dates) - 1:
        ret_pct = (nav / INIT_CASH - 1) * 100
        print(f"  [{i+1:3d}/{len(backtest_dates)}] {today} NAV={nav:,.0f} "
              f"({ret_pct:+.2f}%) 持仓={len(portfolio)} 现金={cash:,.0f}")

# ========== Step 5: 回测结果分析 ==========
print("\n=== Step 5: 回测结果 ===")
nav_df = pd.DataFrame(nav_history)
nav_df["date"] = pd.to_datetime(nav_df["date"])
nav_df = nav_df.set_index("date")
nav_df["daily_ret"] = nav_df["nav"].pct_change()

final_nav = nav_df["nav"].iloc[-1]
total_ret = final_nav / INIT_CASH - 1
trading_days = len(nav_df)
ann_ret = (1 + total_ret) ** (252 / trading_days) - 1
daily_std = nav_df["daily_ret"].std()
ann_vol = daily_std * np.sqrt(252)
sharpe = ann_ret / ann_vol if ann_vol > 0 else 0

# 最大回撤
nav_df["peak"] = nav_df["nav"].cummax()
nav_df["drawdown"] = (nav_df["nav"] - nav_df["peak"]) / nav_df["peak"]
max_dd = nav_df["drawdown"].min()
max_dd_date = nav_df["drawdown"].idxmin()

print(f"  初始资金: {INIT_CASH:,.0f}")
print(f"  最终净值: {final_nav:,.0f}")
print(f"  总收益率: {total_ret*100:.2f}%")
print(f"  年化收益: {ann_ret*100:.2f}%")
print(f"  年化波动: {ann_vol*100:.2f}%")
print(f"  Sharpe:   {sharpe:.3f}")
print(f"  最大回撤: {max_dd*100:.2f}% ({max_dd_date.strftime('%Y-%m-%d')})")
print(f"  交易日数: {trading_days}")
print(f"  总交易次数: {total_trades}")
print(f"  涨跌停阻止交易: {blocked_by_limit} 次")

# ========== Step 6: 数据质量验证 ==========
print("\n=== Step 6: 数据质量验证 ===")
# 检查价格合理性
price_check = daily_agg[daily_agg["date"] >= pd.Timestamp(BACKTEST_START).date()]
zero_close = (price_check["close"] <= 0).sum()
nan_close = price_check["close"].isna().sum()
zero_vol = (price_check["volume"] == 0).sum()
avg_bars = price_check["bar_count"].mean()
print(f"  close <= 0: {zero_close}")
print(f"  close NaN: {nan_close}")
print(f"  volume == 0 (停牌): {zero_vol}")
print(f"  平均 bars/天/股: {avg_bars:.1f}")

# 涨跌停验证: 一字涨停日的 open == high == low == close
if len(full_lu) > 0:
    sample_lu = full_lu.head(5)
    print(f"\n  一字涨停样本 (前5):")
    for _, r in sample_lu.iterrows():
        ohlc_eq = (r["open"] == r["high"] == r["low"] == r["close"])
        print(f"    {r['instrument']} {r['date']}: O={r['open']:.2f} H={r['high']:.2f} "
              f"L={r['low']:.2f} C={r['close']:.2f} OHLC一致={ohlc_eq}")

# 成交量合理性
vol_stats = price_check[price_check["volume"] > 0]["volume"]
print(f"\n  成交量统计 (非停牌):")
print(f"    min={vol_stats.min():,.0f}  median={vol_stats.median():,.0f}  "
      f"max={vol_stats.max():,.0f}")

# 涨跌幅分布
price_check_sorted = price_check.sort_values(["instrument", "date"])
price_check_sorted["daily_ret"] = price_check_sorted.groupby("instrument")["close"].pct_change()
ret_valid = price_check_sorted["daily_ret"].dropna()
print(f"\n  日收益率分布:")
print(f"    min={ret_valid.min()*100:.2f}%  5%={ret_valid.quantile(0.05)*100:.2f}%  "
      f"median={ret_valid.median()*100:.2f}%")
print(f"    95%={ret_valid.quantile(0.95)*100:.2f}%  max={ret_valid.max()*100:.2f}%")
extreme_ret = (ret_valid.abs() > 0.22).sum()
print(f"    |ret| > 22% (异常): {extreme_ret} / {len(ret_valid)} "
      f"({extreme_ret/len(ret_valid)*100:.4f}%)")

print("\n=== 回测完成 ===")
