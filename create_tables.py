"""创建Task同步系统所需的数据库表"""
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

# 连接数据库
conn = psycopg2.connect(
    host=os.getenv("TDX_DB_HOST", "127.0.0.1"),
    port=int(os.getenv("TDX_DB_PORT", 5432)),
    database=os.getenv("TDX_DB_NAME", "aistock"),
    user=os.getenv("TDX_DB_USER", "postgres"),
    password=os.getenv("TDX_DB_PASSWORD", "")
)

conn.autocommit = True
cur = conn.cursor()

print("=" * 60)
print("创建Task同步系统数据库表")
print("=" * 60)

# 读取SQL文件
with open("create_catalog_tables.sql", "r", encoding="utf-8") as f:
    sql = f.read()

# 执行SQL
try:
    cur.execute(sql)
    print("\n✅ 所有表创建成功！")
except Exception as e:
    print(f"\n❌ 创建表失败: {e}")
    conn.rollback()

# 验证表是否创建成功
print("\n验证表结构...")
tables = [
    "rdagent.rdagent_factor_catalog",
    "rdagent.rdagent_loop_catalog",
    "rdagent.rdagent_alpha_baseline_factors",
    "rdagent.rdagent_factor_order",
    "aistock_task_catalog"
]

for table in tables:
    try:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f"  ✅ {table}: {count} 条记录")
    except Exception as e:
        print(f"  ❌ {table}: {e}")

cur.close()
conn.close()

print("\n" + "=" * 60)
print("数据库表创建完成")
print("=" * 60)
