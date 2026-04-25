"""验证修复后的规则分类."""
import os
import sys
from dotenv import load_dotenv
load_dotenv(r"F:\Dev\AIstock\.env")

sys.path.insert(0, r"F:\Dev\AIstock")

from backend.services.quantevolver.factor_analyst import _classify_by_rules

cases = [
    # (name, code_snippet, expected_category_hint)
    ("Value_PBInv_Momentum_20D",
     "df['open'] df['close'] df['volume']  # 这里只用了 price_volume 字段",
     "VAL (名字含 value 和 pbinv)"),
    ("dynamic_flow_volatility_sentiment",
     "mf_elg_buy_amt mf_elg_sell_amt db_turnover_rate db_pb",
     "复合因子 (db_pb=VAL + db_turnover_rate=LIQ)→置信度降级，LLM 决定"),
    ("industry_stock_momentum_diff_10d",
     "sw2_close ref($close, 10) pct_chg",
     "MOM (计算逻辑命中)"),
    ("Alpha_PB_Simple",
     "db_pb",
     "VAL (数据列单一命中)"),
    ("TurnoverRate_ZScore_10D",
     "db_turnover_rate",
     "LIQ (数据列单一命中)"),
    ("Size_LogCirMV",
     "db_circ_mv",
     "SIZE (数据列单一命中)"),
    ("ROE_Growth_3Y",
     "bb_roe bb_growth",
     "未定义 bb_roe → 走名字 QUAL (roe_/growth 匹配)"),
    ("MF_MainNet_20D",
     "mf_buy_elg_vol mf_sell_elg_vol",
     "MF (数据列命中)"),
    ("Chip_WinnerRate_5D",
     "cp_winner_rate",
     "CHIP (数据列命中)"),
    ("Pure_Momentum_20D",
     "df['close'] / df['close'].shift(20) - 1",
     "名字/计算逻辑 MOM"),
]

print(f"{'FACTOR':<42} {'CAT':<6} {'REASON'}")
print("-" * 120)
for name, code, expected in cases:
    cat, reason = _classify_by_rules(name, code_text=code, expression="")
    ok = "[OK]" if cat else "[?]"
    print(f"{ok} {name:<40} {cat or '-':<6} {(reason or '')[:80]}")
    print(f"  期望: {expected}")
    print()
