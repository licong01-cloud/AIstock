"""关联本地资产包到 Catalog 表

根据 RD-Agent 侧的 registry.sqlite 数据，将 asset_bundle_id 关联到策略和 Loop
"""
import sys
import os
import sqlite3
from pathlib import Path

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent))

import psycopg2
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 数据库连接配置
DB_HOST = os.getenv("TDX_DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("TDX_DB_PORT", "5432")
DB_NAME = os.getenv("TDX_DB_NAME", "aistock")
DB_USER = os.getenv("TDX_DB_USER", "postgres")
DB_PASSWORD = os.getenv("TDX_DB_PASSWORD", "")

# RD-Agent registry 路径
REGISTRY_PATH = Path(r"F:\Dev\RD-Agent-main\RDagentDB\registry.sqlite")

# 本地资产包目录
LOCAL_BUNDLES_DIR = Path(r"F:\Dev\AIstock\backend\data\rdagent_assets\production_bundles")

def get_pg_connection():
    """获取 PostgreSQL 连接"""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def get_registry_connection():
    """获取 RD-Agent registry 连接"""
    if not REGISTRY_PATH.exists():
        raise FileNotFoundError(f"Registry 文件不存在: {REGISTRY_PATH}")
    return sqlite3.connect(str(REGISTRY_PATH))

def analyze_registry():
    """分析 RD-Agent registry 数据结构"""
    print("\n" + "=" * 80)
    print("分析 RD-Agent registry.sqlite")
    print("=" * 80)
    
    conn = get_registry_connection()
    cursor = conn.cursor()
    
    # 查看所有表
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    print(f"\nRegistry 中的表:")
    for table in tables:
        print(f"  - {table[0]}")
    
    # 查看策略表结构
    if 'strategies' in [t[0] for t in tables]:
        cursor.execute("PRAGMA table_info(strategies)")
        columns = cursor.fetchall()
        print(f"\nstrategies 表结构:")
        for col in columns:
            print(f"  {col[1]}: {col[2]}")
        
        # 查看策略数据
        cursor.execute("SELECT * FROM strategies LIMIT 5")
        rows = cursor.fetchall()
        print(f"\nstrategies 表数据 (前5条):")
        for row in rows:
            print(f"  {row}")
    
    # 查看资产包表结构
    if 'asset_bundles' in [t[0] for t in tables]:
        cursor.execute("PRAGMA table_info(asset_bundles)")
        columns = cursor.fetchall()
        print(f"\nasset_bundles 表结构:")
        for col in columns:
            print(f"  {col[1]}: {col[2]}")
        
        # 查看资产包数据
        cursor.execute("SELECT * FROM asset_bundles LIMIT 5")
        rows = cursor.fetchall()
        print(f"\nasset_bundles 表数据 (前5条):")
        for row in rows:
            print(f"  {row}")
    
    conn.close()

def link_bundles_to_strategies():
    """关联资产包到策略"""
    print("\n" + "=" * 80)
    print("关联资产包到策略")
    print("=" * 80)
    
    # 获取本地资产包列表
    local_bundles = {d.name for d in LOCAL_BUNDLES_DIR.iterdir() if d.is_dir()}
    print(f"\n本地资产包数量: {len(local_bundles)}")
    
    # 查询数据库中需要更新的策略
    pg_conn = get_pg_connection()
    pg_cur = pg_conn.cursor()
    
    pg_cur.execute("""
        SELECT strategy_id, example_task_run_id, example_loop_id
        FROM aistock_strategy_catalog
        WHERE asset_bundle_id IS NULL
    """)
    
    strategies = pg_cur.fetchall()
    print(f"需要更新 asset_bundle_id 的策略数量: {len(strategies)}")
    
    # 尝试从 RD-Agent registry 获取关联信息
    try:
        reg_conn = get_registry_connection()
        reg_cur = reg_conn.cursor()
        
        # 查询资产包与策略的关联
        reg_cur.execute("""
            SELECT bundle_id, strategy_id, task_run_id, loop_id
            FROM asset_bundles
            WHERE bundle_id IN ({})
        """.format(','.join(['?' for _ in local_bundles])), list(local_bundles))
        
        bundle_mappings = reg_cur.fetchall()
        print(f"Registry 中找到的资产包关联数量: {len(bundle_mappings)}")
        
        # 创建映射字典
        bundle_to_strategy = {}
        for bundle_id, strategy_id, task_run_id, loop_id in bundle_mappings:
            if strategy_id:
                bundle_to_strategy[strategy_id] = bundle_id
            elif task_run_id and loop_id:
                # 通过 task_run_id 和 loop_id 关联
                bundle_to_strategy[f"{task_run_id}_{loop_id}"] = bundle_id
        
        reg_conn.close()
        
    except Exception as e:
        print(f"⚠️ 无法从 Registry 获取关联信息: {e}")
        bundle_to_strategy = {}
    
    # 更新数据库
    updated_count = 0
    for strategy_id, task_run_id, loop_id in strategies:
        # 尝试通过 strategy_id 关联
        bundle_id = bundle_to_strategy.get(strategy_id)
        
        # 如果没有找到，尝试通过 task_run_id 和 loop_id 关联
        if not bundle_id and task_run_id and loop_id:
            bundle_id = bundle_to_strategy.get(f"{task_run_id}_{loop_id}")
        
        if bundle_id:
            pg_cur.execute("""
                UPDATE aistock_strategy_catalog
                SET asset_bundle_id = %s
                WHERE strategy_id = %s
            """, (bundle_id, strategy_id))
            updated_count += 1
            print(f"  ✅ 更新策略 {strategy_id} -> {bundle_id}")
    
    pg_conn.commit()
    pg_conn.close()
    
    print(f"\n成功更新 {updated_count} 个策略的 asset_bundle_id")

def link_bundles_to_loops():
    """关联资产包到 Loop"""
    print("\n" + "=" * 80)
    print("关联资产包到 Loop")
    print("=" * 80)
    
    # 获取本地资产包列表
    local_bundles = {d.name for d in LOCAL_BUNDLES_DIR.iterdir() if d.is_dir()}
    
    # 查询数据库中需要更新的 Loop
    pg_conn = get_pg_connection()
    pg_cur = pg_conn.cursor()
    
    pg_cur.execute("""
        SELECT loop_id, task_run_id, workspace_id
        FROM aistock_loop_catalog
        WHERE asset_bundle_id IS NULL
    """)
    
    loops = pg_cur.fetchall()
    print(f"需要更新 asset_bundle_id 的 Loop 数量: {len(loops)}")
    
    # 尝试从 RD-Agent registry 获取关联信息
    try:
        reg_conn = get_registry_connection()
        reg_cur = reg_conn.cursor()
        
        # 查询资产包与 Loop 的关联
        reg_cur.execute("""
            SELECT bundle_id, strategy_id, task_run_id, loop_id, workspace_id
            FROM asset_bundles
            WHERE bundle_id IN ({})
        """.format(','.join(['?' for _ in local_bundles])), list(local_bundles))
        
        bundle_mappings = reg_cur.fetchall()
        print(f"Registry 中找到的资产包关联数量: {len(bundle_mappings)}")
        
        # 创建映射字典
        bundle_to_loop = {}
        for bundle_id, strategy_id, task_run_id, loop_id, workspace_id in bundle_mappings:
            if task_run_id and loop_id:
                bundle_to_loop[f"{task_run_id}_{loop_id}"] = bundle_id
            elif workspace_id:
                bundle_to_loop[workspace_id] = bundle_id
        
        reg_conn.close()
        
    except Exception as e:
        print(f"⚠️ 无法从 Registry 获取关联信息: {e}")
        bundle_to_loop = {}
    
    # 更新数据库
    updated_count = 0
    for loop_id, task_run_id, workspace_id in loops:
        # 尝试通过 task_run_id 和 loop_id 关联
        bundle_id = bundle_to_loop.get(f"{task_run_id}_{loop_id}")
        
        # 如果没有找到，尝试通过 workspace_id 关联
        if not bundle_id and workspace_id:
            bundle_id = bundle_to_loop.get(workspace_id)
        
        if bundle_id:
            pg_cur.execute("""
                UPDATE aistock_loop_catalog
                SET asset_bundle_id = %s
                WHERE loop_id = %s
            """, (bundle_id, loop_id))
            updated_count += 1
            print(f"  ✅ 更新 Loop {loop_id} -> {bundle_id}")
    
    pg_conn.commit()
    pg_conn.close()
    
    print(f"\n成功更新 {updated_count} 个 Loop 的 asset_bundle_id")

def verify_links():
    """验证关联结果"""
    print("\n" + "=" * 80)
    print("验证关联结果")
    print("=" * 80)
    
    pg_conn = get_pg_connection()
    pg_cur = pg_conn.cursor()
    
    # 验证策略关联
    pg_cur.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(asset_bundle_id) as has_bundle,
            COUNT(*) - COUNT(asset_bundle_id) as no_bundle
        FROM aistock_strategy_catalog
    """)
    
    total, has_bundle, no_bundle = pg_cur.fetchone()
    print(f"\n策略关联统计:")
    print(f"  总策略数: {total}")
    print(f"  有 asset_bundle_id: {has_bundle}")
    print(f"  无 asset_bundle_id: {no_bundle}")
    
    # 验证 Loop 关联
    pg_cur.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(asset_bundle_id) as has_bundle,
            COUNT(*) - COUNT(asset_bundle_id) as no_bundle
        FROM aistock_loop_catalog
    """)
    
    total, has_bundle, no_bundle = pg_cur.fetchone()
    print(f"\nLoop 关联统计:")
    print(f"  总 Loop 数: {total}")
    print(f"  有 asset_bundle_id: {has_bundle}")
    print(f"  无 asset_bundle_id: {no_bundle}")
    
    # 显示有 asset_bundle_id 的策略示例
    pg_cur.execute("""
        SELECT strategy_id, asset_bundle_id
        FROM aistock_strategy_catalog
        WHERE asset_bundle_id IS NOT NULL
        LIMIT 10
    """)
    
    rows = pg_cur.fetchall()
    print(f"\n有 asset_bundle_id 的策略示例 (前10条):")
    for strategy_id, bundle_id in rows:
        bundle_exists = (LOCAL_BUNDLES_DIR / bundle_id).exists()
        status = "✅ 存在" if bundle_exists else "❌ 不存在"
        print(f"  {strategy_id}: {bundle_id} - {status}")
    
    pg_conn.close()

if __name__ == "__main__":
    print("=" * 80)
    print("关联本地资产包到 Catalog 表")
    print("=" * 80)
    
    try:
        # 分析 registry 结构
        analyze_registry()
        
        # 关联资产包到策略
        link_bundles_to_strategies()
        
        # 关联资产包到 Loop
        link_bundles_to_loops()
        
        # 验证关联结果
        verify_links()
        
        print("\n" + "=" * 80)
        print("✅ 关联完成")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ 发生错误: {e}")
        import traceback
        traceback.print_exc()
