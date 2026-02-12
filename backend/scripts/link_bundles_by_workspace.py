"""通过 workspace_id 关联资产包到 Catalog 表"""
import sys
import os
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

def get_bundle_workspace_mapping():
    """获取资产包与 workspace 的映射关系"""
    bundle_workspace_map = {}
    
    for bundle_dir in LOCAL_BUNDLES_DIR.iterdir():
        if bundle_dir.is_dir():
            bundle_id = bundle_dir.name
            subdirs = [d for d in bundle_dir.iterdir() if d.is_dir()]
            
            if subdirs:
                # 假设第一个子目录就是 workspace_id
                workspace_id = subdirs[0].name
                bundle_workspace_map[workspace_id] = bundle_id
    
    return bundle_workspace_map

def link_bundles_to_strategies():
    """通过 workspace_id 关联资产包到策略"""
    print("\n" + "=" * 80)
    print("通过 workspace_id 关联资产包到策略")
    print("=" * 80)
    
    # 获取资产包映射
    bundle_workspace_map = get_bundle_workspace_mapping()
    print(f"\n资产包映射数量: {len(bundle_workspace_map)}")
    
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
    
    # 查询 Loop 表获取 workspace_id
    updated_count = 0
    for strategy_id, task_run_id, loop_id in strategies:
        if loop_id:
            # 通过 loop_id 查询 workspace_id
            pg_cur.execute("""
                SELECT workspace_id
                FROM aistock_loop_catalog
                WHERE loop_id = %s
            """, (loop_id,))
            
            row = pg_cur.fetchone()
            if row:
                workspace_id = row[0]
                bundle_id = bundle_workspace_map.get(workspace_id)
                
                if bundle_id:
                    pg_cur.execute("""
                        UPDATE aistock_strategy_catalog
                        SET asset_bundle_id = %s
                        WHERE strategy_id = %s
                    """, (bundle_id, strategy_id))
                    updated_count += 1
                    print(f"  ✅ 更新策略 {strategy_id} -> {bundle_id} (workspace: {workspace_id})")
    
    pg_conn.commit()
    pg_conn.close()
    
    print(f"\n成功更新 {updated_count} 个策略的 asset_bundle_id")

def link_bundles_to_loops():
    """通过 workspace_id 关联资产包到 Loop"""
    print("\n" + "=" * 80)
    print("通过 workspace_id 关联资产包到 Loop")
    print("=" * 80)
    
    # 获取资产包映射
    bundle_workspace_map = get_bundle_workspace_mapping()
    
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
    
    # 更新数据库
    updated_count = 0
    for loop_id, task_run_id, workspace_id in loops:
        bundle_id = bundle_workspace_map.get(workspace_id)
        
        if bundle_id:
            pg_cur.execute("""
                UPDATE aistock_loop_catalog
                SET asset_bundle_id = %s
                WHERE loop_id = %s
            """, (bundle_id, loop_id))
            updated_count += 1
            print(f"  ✅ 更新 Loop {loop_id} -> {bundle_id} (workspace: {workspace_id})")
    
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
    
    # 测试特定策略
    test_strategy_id = "0104bde26e3351cd89f7b28bc186d7b7"
    pg_cur.execute("""
        SELECT strategy_id, asset_bundle_id, example_task_run_id, example_loop_id
        FROM aistock_strategy_catalog
        WHERE strategy_id = %s
    """, (test_strategy_id,))
    
    row = pg_cur.fetchone()
    if row:
        strategy_id, bundle_id, task_run_id, loop_id = row
        print(f"\n测试策略 {test_strategy_id}:")
        print(f"  asset_bundle_id: {bundle_id}")
        print(f"  example_task_run_id: {task_run_id}")
        print(f"  example_loop_id: {loop_id}")
        
        if bundle_id:
            bundle_exists = (LOCAL_BUNDLES_DIR / bundle_id).exists()
            print(f"  资产包存在: {'✅ 是' if bundle_exists else '❌ 否'}")
    
    pg_conn.close()

if __name__ == "__main__":
    print("=" * 80)
    print("通过 workspace_id 关联资产包到 Catalog 表")
    print("=" * 80)
    
    try:
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
