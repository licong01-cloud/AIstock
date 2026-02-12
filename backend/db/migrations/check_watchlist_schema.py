#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
检查自选股票池表结构
"""

import os
import sys
import psycopg2

# 设置环境变量
os.environ['TDX_DB_HOST'] = '127.0.0.1'
os.environ['TDX_DB_PORT'] = '5432'
os.environ['TDX_DB_NAME'] = 'aistock'
os.environ['TDX_DB_USER'] = 'postgres'
os.environ['TDX_DB_PASSWORD'] = 'lc78080808'

def check_schema():
    """检查数据库表结构"""
    try:
        conn = psycopg2.connect(
            host=os.environ.get('TDX_DB_HOST', '127.0.0.1'),
            port=int(os.environ.get('TDX_DB_PORT', '5432')),
            database=os.environ.get('TDX_DB_NAME', 'aistock'),
            user=os.environ.get('TDX_DB_USER', 'postgres'),
            password=os.environ.get('TDX_DB_PASSWORD', '')
        )
        cursor = conn.cursor()
        
        # 检查 watchlist_items 表结构
        print("=== 检查 app.watchlist_items 表结构 ===")
        cursor.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = 'app' 
              AND table_name = 'watchlist_items'
            ORDER BY ordinal_position
        """)
        
        for row in cursor.fetchall():
            print(f"  {row[0]}: {row[1]} (nullable: {row[2]}, default: {row[3]})")
        
        # 检查 tasks 表是否存在
        print("\n=== 检查 app.tasks 表 ===")
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'app' 
                  AND table_name = 'tasks'
            )
        """)
        exists = cursor.fetchone()[0]
        print(f"  tasks 表存在: {exists}")
        
        if exists:
            cursor.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'app' 
                  AND table_name = 'tasks'
                ORDER BY ordinal_position
            """)
            for row in cursor.fetchall():
                print(f"    {row[0]}: {row[1]}")
        
        conn.close()
        return True
    except Exception as e:
        print(f"检查失败: {e}")
        return False

if __name__ == '__main__':
    check_schema()
