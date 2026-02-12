#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
创建 app.tasks 表
"""

import os
import psycopg2

# 设置环境变量
os.environ['TDX_DB_HOST'] = '127.0.0.1'
os.environ['TDX_DB_PORT'] = '5432'
os.environ['TDX_DB_NAME'] = 'aistock'
os.environ['TDX_DB_USER'] = 'postgres'
os.environ['TDX_DB_PASSWORD'] = 'lc78080808'

def create_tasks_table():
    """创建 app.tasks 表"""
    try:
        conn = psycopg2.connect(
            host=os.environ.get('TDX_DB_HOST', '127.0.0.1'),
            port=int(os.environ.get('TDX_DB_PORT', '5432')),
            database=os.environ.get('TDX_DB_NAME', 'aistock'),
            user=os.environ.get('TDX_DB_USER', 'postgres'),
            password=os.environ.get('TDX_DB_PASSWORD', '')
        )
        cursor = conn.cursor()
        
        # 创建 tasks 表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS app.tasks (
                id VARCHAR(50) PRIMARY KEY,
                name VARCHAR(200) NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        
        # 创建索引
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_tasks_created_at 
            ON app.tasks (created_at DESC)
        """)
        
        # 创建更新时间触发器
        cursor.execute("""
            CREATE OR REPLACE FUNCTION update_tasks_updated_at()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = NOW();
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql
        """)
        
        cursor.execute("""
            CREATE TRIGGER trigger_update_tasks_updated_at
            BEFORE UPDATE ON app.tasks
            FOR EACH ROW
            EXECUTE FUNCTION update_tasks_updated_at()
        """)
        
        conn.commit()
        print('app.tasks 表创建成功')
        return True
    except Exception as e:
        print(f'创建失败: {e}')
        return False
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == '__main__':
    create_tasks_table()
