#!/usr/bin/env python
"""创建 market.xtquant_pershare_index 表 + TimescaleDB hypertable + data_stats_config 注册

字段来源：xtquant/config/pershare_new.ini（PershareIndex 每股主要指标）
建表模式与 create_daily_basic_table.py 完全一致。
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

    try:
        with conn, conn.cursor() as cur:
            # ------------------------------------------------------------------
            # 1. 建表
            # ------------------------------------------------------------------
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS market.xtquant_pershare_index (
                    report_date       DATE    NOT NULL,
                    ts_code           TEXT    NOT NULL,
                    ann_date          DATE,
                    s_fa_ocfps                    NUMERIC,
                    s_fa_bps                      NUMERIC,
                    s_fa_eps_basic                NUMERIC,
                    s_fa_eps_diluted              NUMERIC,
                    s_fa_undistributedps          NUMERIC,
                    s_fa_surpluscapitalps         NUMERIC,
                    adjusted_earnings_per_share   NUMERIC,
                    du_return_on_equity           NUMERIC,
                    sales_gross_profit            NUMERIC,
                    equity_roe                    NUMERIC,
                    net_roe                       NUMERIC,
                    total_roe                     NUMERIC,
                    gross_profit                  NUMERIC,
                    net_profit                    NUMERIC,
                    actual_tax_rate               NUMERIC,
                    inc_revenue_rate              NUMERIC,
                    du_profit_rate                NUMERIC,
                    inc_net_profit_rate           NUMERIC,
                    adjusted_net_profit_rate      NUMERIC,
                    inc_total_revenue_annual      NUMERIC,
                    inc_net_profit_to_shareholders_annual NUMERIC,
                    adjusted_profit_to_profit_annual     NUMERIC,
                    pre_pay_operate_income        NUMERIC,
                    sales_cash_flow               NUMERIC,
                    gear_ratio                    NUMERIC,
                    inventory_turnover            NUMERIC,
                    PRIMARY KEY (report_date, ts_code)
                );
                """
            )

            # ------------------------------------------------------------------
            # 2. 列注释
            # ------------------------------------------------------------------
            cur.execute(
                "COMMENT ON TABLE market.xtquant_pershare_index IS "
                "'xtquant PershareIndex 每股主要指标（按报告截止日）';"
            )
            comments = {
                "report_date": "报告截止日（m_timetag）",
                "ts_code": "股票代码（xtquant格式 如000001.SZ）",
                "ann_date": "公告日期（m_anntime）",
                "s_fa_ocfps": "每股经营活动现金流量",
                "s_fa_bps": "每股净资产",
                "s_fa_eps_basic": "基本每股收益",
                "s_fa_eps_diluted": "稀释每股收益",
                "s_fa_undistributedps": "每股未分配利润",
                "s_fa_surpluscapitalps": "每股资本公积金",
                "adjusted_earnings_per_share": "扣非每股收益",
                "du_return_on_equity": "净资产收益率",
                "sales_gross_profit": "销售毛利率",
                "equity_roe": "加权净资产收益率",
                "net_roe": "摊薄净资产收益率",
                "total_roe": "摊薄总资产收益率",
                "gross_profit": "毛利率",
                "net_profit": "净利率",
                "actual_tax_rate": "实际税率",
                "inc_revenue_rate": "主营收入同比增长",
                "du_profit_rate": "净利润同比增长",
                "inc_net_profit_rate": "归属母公司净利润同比增长",
                "adjusted_net_profit_rate": "扣非净利润同比增长",
                "inc_total_revenue_annual": "营业总收入滚动环比增长",
                "inc_net_profit_to_shareholders_annual": "归属净利润滚动环比增长",
                "adjusted_profit_to_profit_annual": "扣非净利润滚动环比增长",
                "pre_pay_operate_income": "预收款/营业收入",
                "sales_cash_flow": "销售现金流/营业收入",
                "gear_ratio": "资产负债比率",
                "inventory_turnover": "存货周转率",
            }
            for col, desc in comments.items():
                cur.execute(
                    f"COMMENT ON COLUMN market.xtquant_pershare_index.{col} IS %s;",
                    (desc,),
                )

            # ------------------------------------------------------------------
            # 3. TimescaleDB hypertable
            # ------------------------------------------------------------------
            cur.execute(
                "SELECT create_hypertable("
                "'market.xtquant_pershare_index','report_date', if_not_exists => TRUE);"
            )

            # ------------------------------------------------------------------
            # 4. 辅助索引
            # ------------------------------------------------------------------
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_xtq_pershare_ts_code
                    ON market.xtquant_pershare_index (ts_code, report_date DESC);
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_xtq_pershare_ann_date
                    ON market.xtquant_pershare_index (ann_date DESC)
                    WHERE ann_date IS NOT NULL;
                """
            )

            # ------------------------------------------------------------------
            # 5. 注册到 data_stats_config（数据看板）
            # ------------------------------------------------------------------
            cur.execute(
                """
                INSERT INTO market.data_stats_config
                    (data_kind, table_name, date_column, enabled, extra_info)
                VALUES (
                    'xtquant_pershare_index',
                    'market.xtquant_pershare_index',
                    'report_date',
                    TRUE,
                    jsonb_build_object(
                        'desc', 'xtquant PershareIndex 每股主要指标',
                        'source', 'xtquant'
                    )
                )
                ON CONFLICT (data_kind) DO UPDATE
                    SET table_name   = EXCLUDED.table_name,
                        date_column  = EXCLUDED.date_column,
                        enabled      = EXCLUDED.enabled,
                        extra_info   = EXCLUDED.extra_info;
                """
            )
    finally:
        conn.close()

    print("market.xtquant_pershare_index table and data_stats_config ensured.")


if __name__ == "__main__":
    main()
