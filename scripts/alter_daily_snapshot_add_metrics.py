"""为 daily_snapshot 新增 6 个指标字段 — 支持完整图表体系.

新增字段:
  excess_return        当日超额收益 (daily_return - benchmark_return)
  win_rate_trade       截至当日的交易胜率 (累积 win_count/sell_count)
  sharpe_rolling_20    20日滚动 Sharpe Ratio
  volatility_rolling_20 20日滚动年化波动率
  long_exposure        多头暴露 (stock_value / total_value)
  cum_cost             累计总费用

用法:
  cd F:\\Dev\\AIstock && python scripts/alter_daily_snapshot_add_metrics.py
"""
import os

import psycopg2
from dotenv import load_dotenv


def main() -> None:
    load_dotenv(override=True)
    cfg = dict(
        host=os.getenv("TDX_DB_HOST", "localhost"),
        port=int(os.getenv("TDX_DB_PORT", "5432")),
        user=os.getenv("TDX_DB_USER", "postgres"),
        password=os.getenv("TDX_DB_PASSWORD", ""),
        dbname=os.getenv("TDX_DB_NAME", "aistock"),
    )
    conn = psycopg2.connect(**cfg)
    conn.autocommit = True

    columns = [
        ("excess_return",        "NUMERIC(12,6)", "当日超额收益 (daily_return - benchmark_return)"),
        ("win_rate_trade",       "NUMERIC(10,6)", "截至当日的交易胜率 (累积 win_count/sell_count)"),
        ("sharpe_rolling_20",    "NUMERIC(10,6)", "20日滚动Sharpe Ratio"),
        ("volatility_rolling_20","NUMERIC(10,6)", "20日滚动年化波动率"),
        ("long_exposure",        "NUMERIC(10,6)", "多头暴露 (stock_value/total_value)"),
        ("cum_cost",             "NUMERIC(14,2)", "累计总费用 (佣金+印花税+过户费+滑点)"),
    ]

    try:
        with conn.cursor() as cur:
            for col_name, col_type, comment in columns:
                cur.execute(f"""
                    ALTER TABLE paper_trading.daily_snapshot
                    ADD COLUMN IF NOT EXISTS {col_name} {col_type};
                """)
                cur.execute(
                    f"COMMENT ON COLUMN paper_trading.daily_snapshot.{col_name} IS %s;",
                    (comment,),
                )
                print(f"  + {col_name:30s} {col_type:20s} -- {comment}")

            # 回填已有数据的 excess_return 和 long_exposure（纯派生字段）
            cur.execute("""
                UPDATE paper_trading.daily_snapshot
                SET excess_return = daily_return - COALESCE(benchmark_return, 0),
                    long_exposure = CASE WHEN total_value > 0
                                        THEN stock_value / total_value
                                        ELSE 0 END
                WHERE excess_return IS NULL;
            """)
            backfilled = cur.rowcount
            if backfilled > 0:
                print(f"\n  回填 excess_return + long_exposure: {backfilled} 行")

    finally:
        conn.close()

    print("\npaper_trading.daily_snapshot 6 个新字段已添加。")


if __name__ == "__main__":
    main()
