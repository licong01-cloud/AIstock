"""
执行数据库迁移脚本
"""
import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent  # 指向AIstock/backend
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv

# 加载AIstock的.env文件
aistock_env_path = project_root.parent / ".env"
if aistock_env_path.exists():
    load_dotenv(aistock_env_path)

from db.pg_pool import get_conn


def run_migration():
    """执行SQL迁移脚本"""
    sql_file = Path(__file__).parent / "add_api_configs_table.sql"
    
    print(f"读取迁移脚本：{sql_file}")
    with open(sql_file, 'r', encoding='utf-8') as f:
        sql_content = f.read()
    
    print("开始执行数据库迁移...")
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(sql_content)
        conn.commit()
        cursor.close()
    
    print("✓ 数据库迁移成功")


if __name__ == "__main__":
    run_migration()
