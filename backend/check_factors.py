#!/usr/bin/env python3
"""检查指定因子的状态"""
import sys
sys.path.insert(0, 'F:/Dev/AIstock/backend')

from db.pg_pool import get_conn
from psycopg2.extras import RealDictCursor

task_id = '2026-03-13_12-46-42-151164'
factor_names = ['Flow_Price_Ratio_Large_Buy', 'Price_Cost_Deviation', 'Price_Deviation_Historical_High']

with get_conn() as conn:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # 查询这3个因子是否在 catalog 中
        cur.execute('''
            SELECT factor_name, is_sota_factor, ic, sharpe, annualized_return,
                   correlation_filtered, error_message
            FROM aistock_factor_catalog
            WHERE source_task_id = %s AND factor_name = ANY(%s)
        ''', (task_id, factor_names))

        results = cur.fetchall()

        if results:
            print('=== 因子在数据库中存在 ===')
            for r in results:
                print(f"因子: {r['factor_name']}")
                print(f"  SOTA: {r['is_sota_factor']}")
                print(f"  IC: {r['ic']}")
                print(f"  相关性过滤: {r['correlation_filtered']}")
                print(f"  错误信息: {r['error_message']}")
                print()
        else:
            print('=== 这3个因子完全不在数据库中 ===')
            print('说明改造过程失败，未能成功生成可执行的因子代码')

        # 查询这个 task 总共有多少因子
        cur.execute('''
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN is_sota_factor THEN 1 ELSE 0 END) as sota_count
            FROM aistock_factor_catalog
            WHERE source_task_id = %s
        ''', (task_id,))

        stats = cur.fetchone()
        print(f"\n=== Task 统计 ===")
        print(f"总因子数: {stats['total']}")
        print(f"SOTA 因子数: {stats['sota_count']}")
