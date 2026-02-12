import psycopg2
import os
import uuid

def force_fix_strategies():
    conn = psycopg2.connect(
        host=os.getenv("TDX_DB_HOST", "127.0.0.1"),
        port=int(os.getenv("TDX_DB_PORT", "5432")),
        user=os.getenv("TDX_DB_USER", "postgres"),
        password=os.getenv("TDX_DB_PASSWORD", "lc78080808"),
        dbname=os.getenv("TDX_DB_NAME", "aistock")
    )
    try:
        with conn.cursor() as cur:
            task_id = '2026-01-06_06-00-53-321254'
            loop_id = 0
            
            # 使用与 inference_engine.py / service 完全一致的逻辑
            # 必须使用 NAMESPACE_URL 生成
            sid_loop = str(uuid.uuid5(uuid.NAMESPACE_URL, f"rdagent_loop:{task_id}:{loop_id}"))
            
            print(f"Force inserting strategy_id: {sid_loop}")
            
            # 显式插入到 trading.strategy，并满足 CHECK 约束
            cur.execute("""
                INSERT INTO trading.strategy (
                    strategy_id, source_id, source_strategy_key, strategy_name, 
                    strategy_kind, output_mode, enabled, created_at, updated_at
                ) 
                VALUES (%s, 1, %s, %s, 'portfolio', 'topk', True, NOW(), NOW()) 
                ON CONFLICT (strategy_id) DO UPDATE SET 
                    strategy_name = EXCLUDED.strategy_name,
                    updated_at = NOW();
            """, (sid_loop, f"rdagent_loop:{task_id}:{loop_id}", f"RD-Agent Loop {task_id}:{loop_id}"))
            
            conn.commit()
            print("Commit successful.")
            
            # 立即验证是否存在
            cur.execute("SELECT strategy_id, strategy_name FROM trading.strategy WHERE strategy_id = %s", (sid_loop,))
            row = cur.fetchone()
            if row:
                print(f"Verification Success: {row}")
            else:
                print("Verification FAILED: Still not found in DB!")
                
    finally:
        conn.close()

if __name__ == "__main__":
    force_fix_strategies()
