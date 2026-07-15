"""为 watchlist_item_categories 表增加分类级别的加入时间追踪字段

对应设计：docs/architecture/watchlist_category_entry_tracking_design_20260719.md

变更内容：
- 增加 added_at: 股票加入该分类的时间戳
- 增加 entry_price_snapshot: 加入该分类时的价格快照
- 增加 entry_date_snapshot: 加入该分类时的基准日期
- 创建索引优化查询性能
- 回填历史数据

本 migration 可重复执行（幂等性）。
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import List

# 添加 backend 目录到 Python path，支持直接执行
_backend_dir = Path(__file__).resolve().parents[2]
if str(_backend_dir) not in sys.path:
    sys.path.insert(0, str(_backend_dir))

from db.pg_pool import get_conn


DDL: List[str] = [
    # 1. 增加字段（使用 ADD COLUMN IF NOT EXISTS 保证幂等性）
    """
    ALTER TABLE app.watchlist_item_categories
        ADD COLUMN IF NOT EXISTS added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        ADD COLUMN IF NOT EXISTS entry_price_snapshot NUMERIC,
        ADD COLUMN IF NOT EXISTS entry_date_snapshot DATE;
    """,
    # 2. 创建索引（按分类查询最近加入的股票）
    """
    CREATE INDEX IF NOT EXISTS idx_watchlist_item_categories_added
        ON app.watchlist_item_categories(category_id, added_at DESC);
    """,
    # 3. 创建索引（按股票和分类查询加入日期）
    """
    CREATE INDEX IF NOT EXISTS idx_watchlist_item_categories_entry_date
        ON app.watchlist_item_categories(item_id, category_id, entry_date_snapshot DESC);
    """,
    # 4. 添加 PostgreSQL COMMENT（符合 RULE-DB-COMMENT-001）
    """
    COMMENT ON COLUMN app.watchlist_item_categories.added_at IS
        '股票加入该分类的时间戳，用于分类级别的加入时间追踪和收益计算基准；默认为 NOW()，表示记录创建时间';
    """,
    """
    COMMENT ON COLUMN app.watchlist_item_categories.entry_price_snapshot IS
        '股票加入该分类时的价格快照（原始价格，未复权），用于计算该分类下的收益；NULL 表示加入时未记录价格或价格获取失败';
    """,
    """
    COMMENT ON COLUMN app.watchlist_item_categories.entry_date_snapshot IS
        '股票加入该分类时的基准日期，用于复权调整和收益计算；通常与 added_at 日期一致，但可能根据选股任务的 as_of 日期调整；NULL 表示使用 added_at 日期';
    """,
]

# 回填历史数据的 SQL
BACKFILL_SQL = """
UPDATE app.watchlist_item_categories wic
   SET entry_date_snapshot = COALESCE(wi.entry_as_of, wi.created_at::date),
       entry_price_snapshot = wi.entry_price
  FROM app.watchlist_items wi
 WHERE wic.item_id = wi.id
   AND wic.entry_date_snapshot IS NULL;
"""


def apply_migration() -> None:
    """应用 migration：增加字段、索引、comment 并回填历史数据"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            # 执行所有 DDL
            for sql in DDL:
                cur.execute(sql)

            # 回填历史数据
            cur.execute(BACKFILL_SQL)
            affected = cur.rowcount
            print(f"[OK] 回填了 {affected} 条历史关联记录的 entry_date_snapshot 和 entry_price_snapshot")


def verify_migration() -> None:
    """验证 migration 执行结果"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            # 验证1：检查字段是否存在
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'app'
                  AND table_name = 'watchlist_item_categories'
                  AND column_name IN ('added_at', 'entry_price_snapshot', 'entry_date_snapshot')
                ORDER BY column_name;
            """)
            columns = [row[0] for row in cur.fetchall()]
            expected_columns = ['added_at', 'entry_date_snapshot', 'entry_price_snapshot']
            assert columns == expected_columns, f"字段验证失败: {columns} != {expected_columns}"
            print(f"[OK] 字段验证通过: {columns}")

            # 验证2：检查索引是否存在
            cur.execute("""
                SELECT indexname
                FROM pg_indexes
                WHERE schemaname = 'app'
                  AND tablename = 'watchlist_item_categories'
                  AND indexname IN (
                      'idx_watchlist_item_categories_added',
                      'idx_watchlist_item_categories_entry_date'
                  )
                ORDER BY indexname;
            """)
            indexes = [row[0] for row in cur.fetchall()]
            expected_indexes = [
                'idx_watchlist_item_categories_added',
                'idx_watchlist_item_categories_entry_date'
            ]
            assert indexes == expected_indexes, f"索引验证失败: {indexes} != {expected_indexes}"
            print(f"[OK] 索引验证通过: {indexes}")

            # 验证3：检查 comment 是否存在
            cur.execute("""
                SELECT column_name, col_description(
                    (table_schema||'.'||table_name)::regclass::oid,
                    ordinal_position
                ) as comment
                FROM information_schema.columns
                WHERE table_schema = 'app'
                  AND table_name = 'watchlist_item_categories'
                  AND column_name IN ('added_at', 'entry_price_snapshot', 'entry_date_snapshot')
                ORDER BY column_name;
            """)
            comments = [(row[0], row[1]) for row in cur.fetchall()]
            for col_name, comment in comments:
                assert comment is not None and len(comment) > 0, f"字段 {col_name} 缺少 comment"
            print(f"[OK] Comment 验证通过: {len(comments)} 个字段都有 comment")

            # 验证4：检查回填数据
            cur.execute("""
                SELECT COUNT(*)
                FROM app.watchlist_item_categories
                WHERE entry_date_snapshot IS NOT NULL;
            """)
            backfilled_count = cur.fetchone()[0]
            print(f"[OK] 回填数据验证: {backfilled_count} 条记录有 entry_date_snapshot")

            # 验证5：检查 added_at 没有 NULL（因为有 DEFAULT NOW()）
            cur.execute("""
                SELECT COUNT(*)
                FROM app.watchlist_item_categories
                WHERE added_at IS NULL;
            """)
            null_count = cur.fetchone()[0]
            assert null_count == 0, f"发现 {null_count} 条记录的 added_at 为 NULL"
            print(f"[OK] added_at 验证通过: 没有 NULL 值")


if __name__ == "__main__":
    import sys
    from dotenv import load_dotenv

    # 设置 UTF-8 输出编码避免 Windows 控制台编码问题
    if sys.platform == 'win32':
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

    # 加载 .env 文件（从主仓库根目录）
    # 直接使用绝对路径定位主仓库
    env_path = Path("F:/Dev/AIstock/.env")
    if env_path.exists():
        load_dotenv(env_path, override=True)
        print(f"[OK] 加载环境变量: {env_path}")
    else:
        print(f"[WARNING] 未找到 .env 文件: {env_path}")

    print("\n" + "=" * 70)
    print("[START] 开始应用 watchlist_category_entry_tracking migration...")
    print("=" * 70)

    try:
        apply_migration()
        print("\n" + "=" * 70)
        print("[TEST] 开始验证 migration 执行结果...")
        print("=" * 70)
        verify_migration()
        print("\n" + "=" * 70)
        print("[SUCCESS] Migration 完成并验证通过")
        print("=" * 70)
    except Exception as e:
        print("\n" + "=" * 70)
        print(f"[ERROR] Migration 失败: {e}")
        print("=" * 70)
        raise
