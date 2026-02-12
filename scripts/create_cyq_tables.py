"""
创建 cyq_perf 和 cyq_chips 数据库表
包含完整的字段注释和 TimescaleDB hypertable 配置
"""
import os
import sys
from pathlib import Path

# 加载环境变量
from dotenv import load_dotenv
dotenv_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path)

import psycopg2
from psycopg2 import sql


def get_db_connection():
    """获取数据库连接"""
    return psycopg2.connect(
        host=os.getenv("TDX_DB_HOST", "localhost"),
        port=os.getenv("TDX_DB_PORT", "5432"),
        user=os.getenv("TDX_DB_USER"),
        password=os.getenv("TDX_DB_PASSWORD"),
        dbname=os.getenv("TDX_DB_NAME", "aistock"),
        options="-c client_encoding=utf8",
    )


def create_cyq_perf_table(conn):
    """创建 cyq_perf 表"""
    with conn.cursor() as cur:
        # 创建表
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS market.cyq_perf (
                trade_date DATE NOT NULL,           -- 交易日期
                ts_code VARCHAR(20) NOT NULL,       -- 股票代码
                his_low NUMERIC(18,4),              -- 历史最低价
                his_high NUMERIC(18,4),             -- 历史最高价
                cost_5pct NUMERIC(18,4),            -- 5分位成本
                cost_15pct NUMERIC(18,4),           -- 15分位成本
                cost_50pct NUMERIC(18,4),           -- 50分位成本
                cost_85pct NUMERIC(18,4),           -- 85分位成本
                cost_95pct NUMERIC(18,4),            -- 95分位成本
                weight_avg NUMERIC(18,4),           -- 加权平均成本
                winner_rate NUMERIC(18,4),          -- 胜率
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (trade_date, ts_code)
            );
            """
        )
        
        # 转换为 TimescaleDB hypertable
        cur.execute(
            """
            SELECT create_hypertable('market.cyq_perf', 'trade_date', if_not_exists => TRUE);
            """
        )
        
        # 表注释
        cur.execute(
            """
            COMMENT ON TABLE market.cyq_perf IS 'Tushare cyq_perf 每日筹码及胜率（按交易日）';
            """
        )
        
        # 字段注释
        comments = [
            ("trade_date", "交易日期 YYYYMMDD"),
            ("ts_code", "TS股票代码"),
            ("his_low", "历史最低价（自上市以来）"),
            ("his_high", "历史最高价（自上市以来）"),
            ("cost_5pct", "5%分位成本（低位筹码成本）"),
            ("cost_15pct", "15%分位成本"),
            ("cost_50pct", "50%分位成本（筹码中位数成本）"),
            ("cost_85pct", "85%分位成本"),
            ("cost_95pct", "95%分位成本（高位筹码成本）"),
            ("weight_avg", "加权平均成本（按筹码量加权）"),
            ("winner_rate", "胜率（当前价高于持仓成本的股票占比%）"),
            ("created_at", "记录创建时间"),
            ("updated_at", "记录更新时间"),
        ]
        
        for col, comment in comments:
            cur.execute(
                sql.SQL("COMMENT ON COLUMN market.cyq_perf.{} IS %s;").format(sql.Identifier(col)),
                (comment,)
            )
        
        conn.commit()
        print("✓ cyq_perf 表创建成功")


def create_cyq_chips_table(conn):
    """创建 cyq_chips 表"""
    with conn.cursor() as cur:
        # 创建表
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS market.cyq_chips (
                trade_date DATE NOT NULL,           -- 交易日期
                ts_code VARCHAR(20) NOT NULL,       -- 股票代码
                price NUMERIC(18,4) NOT NULL,       -- 成本价格
                percent NUMERIC(18,4),              -- 价格占比（%）
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (trade_date, ts_code, price)
            );
            """
        )
        
        # 转换为 TimescaleDB hypertable
        cur.execute(
            """
            SELECT create_hypertable('market.cyq_chips', 'trade_date', if_not_exists => TRUE);
            """
        )
        
        # 表注释
        cur.execute(
            """
            COMMENT ON TABLE market.cyq_chips IS 'Tushare cyq_chips 每日筹码分布（按交易日）';
            """
        )
        
        # 字段注释
        comments = [
            ("trade_date", "交易日期 YYYYMMDD"),
            ("ts_code", "TS股票代码"),
            ("price", "成本价格（筹码分布的价位）"),
            ("percent", "价格占比（该价位筹码占总筹码的百分比%）"),
            ("created_at", "记录创建时间"),
            ("updated_at", "记录更新时间"),
        ]
        
        for col, comment in comments:
            cur.execute(
                sql.SQL("COMMENT ON COLUMN market.cyq_chips.{} IS %s;").format(sql.Identifier(col)),
                (comment,)
            )
        
        conn.commit()
        print("✓ cyq_chips 表创建成功")


def register_data_stats_config(conn):
    """注册数据到 data_stats_config"""
    with conn.cursor() as cur:
        # 注册 cyq_perf
        cur.execute(
            """
            INSERT INTO market.data_stats_config (data_kind, table_name, date_column, enabled, extra_info)
            VALUES (
                'cyq_perf',
                'market.cyq_perf',
                'trade_date',
                TRUE,
                jsonb_build_object(
                    'desc', 'Tushare cyq_perf 每日筹码及胜率',
                    'source', 'tushare',
                    'api', 'cyq_perf',
                    'update_time', '每日18-19点',
                    'fields_count', 11
                )
            )
            ON CONFLICT (data_kind) DO UPDATE
                SET table_name = EXCLUDED.table_name,
                    date_column = EXCLUDED.date_column,
                    enabled = EXCLUDED.enabled,
                    extra_info = EXCLUDED.extra_info;
            """
        )
        
        # 注册 cyq_chips
        cur.execute(
            """
            INSERT INTO market.data_stats_config (data_kind, table_name, date_column, enabled, extra_info)
            VALUES (
                'cyq_chips',
                'market.cyq_chips',
                'trade_date',
                TRUE,
                jsonb_build_object(
                    'desc', 'Tushare cyq_chips 每日筹码分布',
                    'source', 'tushare',
                    'api', 'cyq_chips',
                    'update_time', '每日18-19点',
                    'fields_count', 4
                )
            )
            ON CONFLICT (data_kind) DO UPDATE
                SET table_name = EXCLUDED.table_name,
                    date_column = EXCLUDED.date_column,
                    enabled = EXCLUDED.enabled,
                    extra_info = EXCLUDED.extra_info;
            """
        )
        
        conn.commit()
        print("✓ data_stats_config 注册成功")


def verify_tables(conn):
    """验证表创建结果"""
    with conn.cursor() as cur:
        # 检查 cyq_perf 表结构
        cur.execute(
            """
            SELECT column_name, data_type, col_description(
                (table_schema || '.' || table_name)::regclass::oid, ordinal_position
            ) as comment
            FROM information_schema.columns
            WHERE table_schema = 'market' AND table_name = 'cyq_perf'
            ORDER BY ordinal_position;
            """
        )
        cyq_perf_cols = cur.fetchall()
        
        print("\n=== cyq_perf 表结构 ===")
        for col in cyq_perf_cols:
            print(f"  {col[0]}: {col[1]} - {col[2]}")
        
        # 检查 cyq_chips 表结构
        cur.execute(
            """
            SELECT column_name, data_type, col_description(
                (table_schema || '.' || table_name)::regclass::oid, ordinal_position
            ) as comment
            FROM information_schema.columns
            WHERE table_schema = 'market' AND table_name = 'cyq_chips'
            ORDER BY ordinal_position;
            """
        )
        cyq_chips_cols = cur.fetchall()
        
        print("\n=== cyq_chips 表结构 ===")
        for col in cyq_chips_cols:
            print(f"  {col[0]}: {col[1]} - {col[2]}")
        
        # 检查 data_stats_config 注册
        cur.execute(
            """
            SELECT data_kind, table_name, enabled, extra_info
            FROM market.data_stats_config
            WHERE data_kind IN ('cyq_perf', 'cyq_chips');
            """
        )
        configs = cur.fetchall()
        
        print("\n=== data_stats_config 注册 ===")
        for cfg in configs:
            print(f"  {cfg[0]}: {cfg[1]} (enabled={cfg[2]})")
            print(f"    配置: {cfg[3]}")


if __name__ == '__main__':
    print("=" * 60)
    print("创建 cyq_perf 和 cyq_chips 数据库表")
    print("=" * 60)
    
    try:
        conn = get_db_connection()
        print("✓ 数据库连接成功\n")
        
        # 创建表
        create_cyq_perf_table(conn)
        create_cyq_chips_table(conn)
        
        # 注册配置
        register_data_stats_config(conn)
        
        # 验证
        print()
        verify_tables(conn)
        
        print("\n" + "=" * 60)
        print("[OK] 所有表创建和注册完成")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n[ERROR] 错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if conn:
            conn.close()
