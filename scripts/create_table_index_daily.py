from __future__ import annotations

import os

import psycopg2
from dotenv import load_dotenv


# 加载 .env 配置
load_dotenv(override=True)

DB_CFG = dict(
    host=os.getenv("TDX_DB_HOST", "localhost"),
    port=int(os.getenv("TDX_DB_PORT", "5432")),
    user=os.getenv("TDX_DB_USER", "postgres"),
    password=os.getenv("TDX_DB_PASSWORD", ""),
    dbname=os.getenv("TDX_DB_NAME", "aistock"),
    application_name="AIstock-create-index-daily-table",
)


DDL_SQL = r"""
CREATE SCHEMA IF NOT EXISTS market;

CREATE TABLE IF NOT EXISTS market.index_daily (
    ts_code    VARCHAR(32)  NOT NULL,
    trade_date DATE         NOT NULL,
    close      NUMERIC(20,4),
    open       NUMERIC(20,4),
    high       NUMERIC(20,4),
    low        NUMERIC(20,4),
    pre_close  NUMERIC(20,4),
    change     NUMERIC(20,4),
    pct_chg    NUMERIC(20,4),
    vol        NUMERIC(24,4),
    amount     NUMERIC(24,4),
    PRIMARY KEY (ts_code, trade_date)
);

COMMENT ON TABLE market.index_daily IS 'Tushare index_daily 指数日线行情';

COMMENT ON COLUMN market.index_daily.ts_code    IS 'TS 指数代码';
COMMENT ON COLUMN market.index_daily.trade_date IS '交易日';
COMMENT ON COLUMN market.index_daily.close      IS '收盘点位';
COMMENT ON COLUMN market.index_daily.open       IS '开盘点位';
COMMENT ON COLUMN market.index_daily.high       IS '最高点位';
COMMENT ON COLUMN market.index_daily.low        IS '最低点位';
COMMENT ON COLUMN market.index_daily.pre_close  IS '昨日收盘点';
COMMENT ON COLUMN market.index_daily.change     IS '涨跌点';
COMMENT ON COLUMN market.index_daily.pct_chg    IS '涨跌幅(%)';
COMMENT ON COLUMN market.index_daily.vol        IS '成交量(手)';
COMMENT ON COLUMN market.index_daily.amount     IS '成交额(千元)';
"""


HYPERTABLE_SQL = r"""
SELECT create_hypertable('market.index_daily', 'trade_date', if_not_exists => TRUE);
"""


def main() -> None:
    conn = psycopg2.connect(**DB_CFG)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(DDL_SQL)
            # 将 index_daily 转为 TimescaleDB hypertable（如果尚未创建）。
            try:
                cur.execute(HYPERTABLE_SQL)
            except Exception as exc:  # noqa: BLE001
                # 若数据库未安装 Timescale 扩展或已是 hypertable，可以忽略错误。
                print(f"[WARN] create_hypertable for market.index_daily failed or skipped: {exc}")
        print("[OK] market.index_daily table created/updated successfully")
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass


if __name__ == "__main__":
    main()
