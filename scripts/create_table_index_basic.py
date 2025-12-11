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
    application_name="AIstock-create-index-basic-table",
)


DDL_SQL = r"""
CREATE SCHEMA IF NOT EXISTS market;

CREATE TABLE IF NOT EXISTS market.index_basic (
    ts_code      VARCHAR(32)  PRIMARY KEY,
    name         VARCHAR(100),
    fullname     VARCHAR(200),
    market       VARCHAR(32),
    publisher    VARCHAR(64),
    index_type   VARCHAR(64),
    category     VARCHAR(64),
    base_date    DATE,
    base_point   NUMERIC(20,4),
    list_date    DATE,
    weight_rule  VARCHAR(128),
    "desc"       TEXT,
    exp_date     DATE
);

COMMENT ON TABLE market.index_basic IS 'Tushare index_basic 指数基础信息';

COMMENT ON COLUMN market.index_basic.ts_code     IS 'TS指数代码';
COMMENT ON COLUMN market.index_basic.name       IS '指数简称';
COMMENT ON COLUMN market.index_basic.fullname   IS '指数全称';
COMMENT ON COLUMN market.index_basic.market     IS '市场（如沪深等）';
COMMENT ON COLUMN market.index_basic.publisher  IS '发布方';
COMMENT ON COLUMN market.index_basic.index_type IS '指数风格（规模/行业/策略等）';
COMMENT ON COLUMN market.index_basic.category   IS '指数类别';
COMMENT ON COLUMN market.index_basic.base_date  IS '基期日期';
COMMENT ON COLUMN market.index_basic.base_point IS '基点';
COMMENT ON COLUMN market.index_basic.list_date  IS '发布日期';
COMMENT ON COLUMN market.index_basic.weight_rule IS '加权方式';
COMMENT ON COLUMN market.index_basic."desc"    IS '指数简介';
COMMENT ON COLUMN market.index_basic.exp_date   IS '终止日期';
"""


def main() -> None:
    conn = psycopg2.connect(**DB_CFG)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(DDL_SQL)
        print("[OK] market.index_basic table created/updated successfully")
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
