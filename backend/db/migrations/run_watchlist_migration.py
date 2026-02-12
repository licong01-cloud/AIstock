#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
自选股票池 TASK 支持功能 - 数据库迁移执行脚本
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

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def get_conn():
    """获取数据库连接"""
    return psycopg2.connect(
        host=os.environ.get('TDX_DB_HOST', '127.0.0.1'),
        port=int(os.environ.get('TDX_DB_PORT', '5432')),
        database=os.environ.get('TDX_DB_NAME', 'aistock'),
        user=os.environ.get('TDX_DB_USER', 'postgres'),
        password=os.environ.get('TDX_DB_PASSWORD', '')
    )

def main():
    try:
        conn = get_conn()
        cursor = conn.cursor()
        
        # 读取SQL文件
        sql_file_path = os.path.join(
            os.path.dirname(__file__), 
            'add_watchlist_task_support.sql'
        )
        
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql = f.read()
        
        # 执行SQL
        cursor.execute(sql)
        conn.commit()
        print('数据库迁移成功')
        return True
    except Exception as e:
        print(f'数据库迁移失败: {e}')
        if 'conn' in locals():
            conn.rollback()
        return False
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
