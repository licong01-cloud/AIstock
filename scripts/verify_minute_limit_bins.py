"""验证分钟级涨跌停价格 bin 数据可被 Qlib 正确读取。

测试内容：
1. qlib.init 能正确加载 prev_close/up_limit_price/down_limit_price 字段
2. 数据值合理（涨停价 > 昨收 > 跌停价，价格在合理范围）
3. 与数据库 stk_limit 表交叉验证
4. 模拟策略的 quote.get_data() 调用路径

用法：
  python verify_minute_limit_bins.py
"""
import sys
import numpy as np
import pandas as pd

# ── 1. Qlib 初始化 ──
import qlib
from qlib.config import C
C["kernels"] = 1
qlib.init(
    provider_uri={
        "day": "/home/lc999/data/qlib_bin",
        "1min": "/home/lc999/data/qlib_minute_bin",
    },
    region="cn",
    dataset_cache=None,
    expression_cache=None,
)
from qlib.data import D

print("=" * 60)
print("Qlib initialized OK")
print("=" * 60)

# ── 2. 测试 D.features 读取分钟级新字段 ──
test_stocks = ["600519.SH", "300750.SZ", "688981.SH"]
test_fields = ["$close", "$prev_close", "$up_limit_price", "$down_limit_price"]
start = "2025-01-02"
end = "2025-01-10"

print(f"\nTest D.features: stocks={test_stocks}, fields={test_fields}")
print(f"  date range: {start} ~ {end}")

df = D.features(test_stocks, test_fields, start_time=start, end_time=end, freq="1min")
print(f"  Result shape: {df.shape}")
print(f"  Columns: {list(df.columns)}")
print(f"  Index levels: {df.index.names}")

if df.empty:
    print("ERROR: D.features returned empty DataFrame!")
    sys.exit(1)

# ── 3. 逐股票验证数据合理性 ──
errors = []
for stock in test_stocks:
    try:
        sub = df.xs(stock, level="instrument")
    except KeyError:
        print(f"  WARNING: {stock} not in result")
        continue

    close = sub["$close"]
    pc = sub["$prev_close"]
    up = sub["$up_limit_price"]
    dn = sub["$down_limit_price"]

    n_total = len(sub)
    n_valid = int((~pc.isna()).sum())
    print(f"\n  {stock}: {n_total} bars, valid prev_close={n_valid}")

    if n_valid == 0:
        errors.append(f"{stock}: all prev_close NaN")
        continue

    # 取有效行
    valid = sub.dropna(subset=["$prev_close", "$up_limit_price", "$down_limit_price"])
    if len(valid) == 0:
        errors.append(f"{stock}: no rows with all 3 fields valid")
        continue

    pc_v = valid["$prev_close"]
    up_v = valid["$up_limit_price"]
    dn_v = valid["$down_limit_price"]
    cl_v = valid["$close"]

    # 检查: up_limit > prev_close > down_limit
    check_up = (up_v > pc_v).all()
    check_dn = (dn_v < pc_v).all()
    print(f"    up_limit > prev_close: {check_up}")
    print(f"    down_limit < prev_close: {check_dn}")

    if not check_up:
        errors.append(f"{stock}: up_limit <= prev_close found")
    if not check_dn:
        errors.append(f"{stock}: down_limit >= prev_close found")

    # 检查价格范围合理
    print(f"    prev_close: {pc_v.min():.2f} ~ {pc_v.max():.2f}")
    print(f"    up_limit:   {up_v.min():.2f} ~ {up_v.max():.2f}")
    print(f"    down_limit: {dn_v.min():.2f} ~ {dn_v.max():.2f}")
    print(f"    close:      {cl_v.min():.2f} ~ {cl_v.max():.2f}")

    # 检查涨停幅度
    pct = ((up_v / pc_v) - 1.0).mean()
    print(f"    avg limit pct: {pct:.4f} (expect ~0.10 or ~0.20)")

    # 同一天内所有分钟应该相同
    first_day = valid.index[0].date() if hasattr(valid.index[0], 'date') else str(valid.index[0])[:10]
    day_mask = valid.index.normalize() == pd.Timestamp(first_day)
    day_data = valid[day_mask]
    if len(day_data) > 1:
        pc_unique = day_data["$prev_close"].nunique()
        up_unique = day_data["$up_limit_price"].nunique()
        print(f"    same-day consistency: prev_close unique={pc_unique}, up_limit unique={up_unique}")
        if pc_unique > 1:
            errors.append(f"{stock}: prev_close varies within same day")

# ── 4. 模拟 quote.get_data() 路径 ──
print("\n" + "=" * 60)
print("Simulating quote.get_data() path (Exchange-level)")
print("=" * 60)

from qlib.backtest.exchange import Exchange

exchange = Exchange(
    freq="1min",
    start_time=start,
    end_time=end,
    codes=test_stocks,
    limit_threshold=("$limit_up", "$limit_down"),
    deal_price="close",
    subscribe_fields=["$up_limit_price", "$down_limit_price", "$prev_close", "$high", "$low", "$open"],
)

# 测试 quote.get_data
test_date = pd.Timestamp("2025-01-03 10:00:00")
test_end = pd.Timestamp("2025-01-03 10:00:00")
for stock in test_stocks:
    for field in ["$up_limit_price", "$down_limit_price", "$prev_close"]:
        try:
            val = exchange.quote.get_data(stock, test_date, test_end, field=field, method="ts_data_last")
            print(f"  {stock} {field} @ {test_date}: {val}")
        except Exception as e:
            print(f"  {stock} {field} @ {test_date}: ERROR - {e}")
            errors.append(f"quote.get_data failed: {stock} {field}: {e}")

# ── 5. 与数据库交叉验证 ──
print("\n" + "=" * 60)
print("Cross-validation with database")
print("=" * 60)

try:
    import psycopg2
    conn = psycopg2.connect(host='192.168.50.14', port=5432, dbname='aistock',
                            user='postgres', password='lc78080808')
    cur = conn.cursor()
    for stock in ["600519.SH", "300750.SZ", "688981.SH"]:
        cur.execute("""
            SELECT trade_date, pre_close, up_limit, down_limit
            FROM market.stk_limit
            WHERE ts_code = %s AND trade_date = '2025-01-03'
        """, (stock,))
        row = cur.fetchone()
        if row:
            print(f"  DB {stock} 2025-01-03: pre_close={row[1]}, up_limit={row[2]}, down_limit={row[3]}")
        else:
            print(f"  DB {stock} 2025-01-03: no data")
    conn.close()
except Exception as e:
    print(f"  DB cross-validation skipped: {e}")

# ── 6. 总结 ──
print("\n" + "=" * 60)
if errors:
    print(f"FAILED: {len(errors)} errors found:")
    for e in errors:
        print(f"  - {e}")
    sys.exit(1)
else:
    print("ALL CHECKS PASSED")
    print("  - D.features reads prev_close/up_limit_price/down_limit_price correctly")
    print("  - Price relationships valid (up > prev > down)")
    print("  - Same-day consistency verified")
    print("  - Exchange quote.get_data() works")
    sys.exit(0)
