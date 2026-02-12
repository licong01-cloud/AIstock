"""运行数据库迁移脚本"""
import os
import psycopg2
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 获取数据库连接信息
db_config = {
    'host': os.getenv('TDX_DB_HOST', '127.0.0.1'),
    'port': os.getenv('TDX_DB_PORT', '5432'),
    'database': os.getenv('TDX_DB_NAME', 'aistock'),
    'user': os.getenv('TDX_DB_USER', 'postgres'),
    'password': os.getenv('TDX_DB_PASSWORD', '')
}

# 读取SQL文件
sql_file = 'backend/migrations/create_rdagent_candidate_tables.sql'
with open(sql_file, 'r', encoding='utf-8') as f:
    sql_script = f.read()

# 执行SQL
try:
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    
    print(f"正在执行迁移脚本: {sql_file}")
    cur.execute(sql_script)
    conn.commit()
    
    print("✅ 数据库表创建成功！")
    
    # 验证表是否创建
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'rdagent' 
        AND table_name IN ('rdagent_candidate_tasks', 'rdagent_candidate_loops')
        ORDER BY table_name
    """)
    tables = cur.fetchall()
    print(f"\n已创建的表:")
    for table in tables:
        print(f"  - rdagent.{table[0]}")
    
    cur.close()
    conn.close()
    
except Exception as e:
    print(f"❌ 迁移失败: {e}")
    raise
