"""
手动验证脚本：测试自选股分类级别的加入时间和收益计算功能
"""
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from datetime import date, timedelta
from dotenv import load_dotenv

# 加载环境变量
load_dotenv(Path("F:/Dev/AIstock/.env"))

from backend.repositories.watchlist_repo_impl import watchlist_repo
from backend.db.pg_pool import get_conn

def cleanup_test_data():
    """清理测试数据"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM app.watchlist_item_categories WHERE item_id IN (SELECT id FROM app.watchlist_items WHERE code LIKE 'TEST_%')")
            cur.execute("DELETE FROM app.watchlist_items WHERE code LIKE 'TEST_%'")
            cur.execute("DELETE FROM app.watchlist_categories WHERE name LIKE 'TEST_VERIFY_%'")
            conn.commit()
    print("[OK] 测试数据已清理")


def test_category_entry_tracking():
    """测试分类级别的加入价格和时间记录"""
    print("\n" + "="*70)
    print("测试：同一只股票加入不同分类，记录各自的加入时间和价格")
    print("="*70)

    # 清理测试数据
    cleanup_test_data()

    # 创建测试分类
    cat1_id = watchlist_repo.create_category("TEST_VERIFY_CAT_1", "验证测试分类1")
    cat2_id = watchlist_repo.create_category("TEST_VERIFY_CAT_2", "验证测试分类2")
    print(f"[OK] 创建测试分类: cat1_id={cat1_id}, cat2_id={cat2_id}")

    # 第一次加入：股票加入分类1，价格 10.0
    entry_date_1 = date.today() - timedelta(days=10)
    print(f"\n[TEST] 将 TEST_000001.SZ 加入分类1，价格=10.0，日期={entry_date_1}")
    result1 = watchlist_repo.add_items_bulk_with_meta(
        category_id=cat1_id,
        items=[
            {
                "code": "TEST_000001.SZ",
                "name": "测试股票1",
                "entry_price": 10.0,
                "entry_as_of": entry_date_1,
            }
        ],
        on_conflict="ignore",
    )
    print(f"[OK] 加入结果: added={result1['added']}, skipped={result1['skipped']}")
    item_id = result1["item_ids_by_code"]["TEST_000001.SZ"]
    print(f"[OK] 股票 item_id={item_id}")

    # 第二次加入：同一只股票加入分类2，价格 12.0
    entry_date_2 = date.today() - timedelta(days=5)
    print(f"\n[TEST] 将 TEST_000001.SZ 加入分类2，价格=12.0，日期={entry_date_2}")
    result2 = watchlist_repo.add_items_bulk_with_meta(
        category_id=cat2_id,
        items=[
            {
                "code": "TEST_000001.SZ",
                "name": "测试股票1",
                "entry_price": 12.0,
                "entry_as_of": entry_date_2,
            }
        ],
        on_conflict="ignore",
    )
    print(f"[OK] 加入结果: added={result2['added']}, skipped={result2['skipped']}")

    # 验证数据库记录
    print(f"\n[TEST] 验证数据库记录...")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT category_id, entry_date_snapshot, entry_price_snapshot, added_at
                FROM app.watchlist_item_categories
                WHERE item_id = %s
                ORDER BY category_id
                """,
                (item_id,),
            )
            rows = cur.fetchall()
            print(f"[OK] 找到 {len(rows)} 条关联记录")

            for i, row in enumerate(rows, 1):
                cat_id, entry_date, entry_price, added_at = row
                print(f"\n  记录 {i}:")
                print(f"    category_id: {cat_id}")
                print(f"    entry_date_snapshot: {entry_date}")
                print(f"    entry_price_snapshot: {entry_price}")
                print(f"    added_at: {added_at}")

            # 断言验证
            assert len(rows) == 2, f"应该有2条记录，实际 {len(rows)} 条"
            assert rows[0][0] == cat1_id, f"第1条记录的 category_id 应该是 {cat1_id}"
            assert rows[0][1] == entry_date_1, f"第1条记录的 entry_date_snapshot 应该是 {entry_date_1}"
            assert float(rows[0][2]) == 10.0, f"第1条记录的 entry_price_snapshot 应该是 10.0"
            assert rows[1][0] == cat2_id, f"第2条记录的 category_id 应该是 {cat2_id}"
            assert rows[1][1] == entry_date_2, f"第2条记录的 entry_date_snapshot 应该是 {entry_date_2}"
            assert float(rows[1][2]) == 12.0, f"第2条记录的 entry_price_snapshot 应该是 12.0"

    print("\n[OK] 测试通过：不同分类记录了各自的加入时间和价格")

    # 测试查询返回分类级别的价格
    print("\n" + "="*70)
    print("测试：查询特定分类时，返回该分类的加入价格")
    print("="*70)

    result1 = watchlist_repo.list_items(category_id=cat1_id, page=1, page_size=10)
    items1 = result1["items"]
    print(f"\n[TEST] 查询分类1 (id={cat1_id}):")
    print(f"  找到 {len(items1)} 只股票")
    if items1:
        item = items1[0]
        print(f"  code: {item['code']}")
        print(f"  category_entry_price: {item.get('category_entry_price')}")
        print(f"  category_entry_date: {item.get('category_entry_date')}")
        assert item["category_entry_price"] == 10.0, f"分类1的 category_entry_price 应该是 10.0，实际 {item['category_entry_price']}"
        assert item["category_entry_date"] == entry_date_1.isoformat(), f"分类1的 category_entry_date 应该是 {entry_date_1}"

    result2 = watchlist_repo.list_items(category_id=cat2_id, page=1, page_size=10)
    items2 = result2["items"]
    print(f"\n[TEST] 查询分类2 (id={cat2_id}):")
    print(f"  找到 {len(items2)} 只股票")
    if items2:
        item = items2[0]
        print(f"  code: {item['code']}")
        print(f"  category_entry_price: {item.get('category_entry_price')}")
        print(f"  category_entry_date: {item.get('category_entry_date')}")
        assert item["category_entry_price"] == 12.0, f"分类2的 category_entry_price 应该是 12.0，实际 {item['category_entry_price']}"
        assert item["category_entry_date"] == entry_date_2.isoformat(), f"分类2的 category_entry_date 应该是 {entry_date_2}"

    print("\n[OK] 测试通过：不同分类返回各自的加入价格")

    # 清理测试数据
    cleanup_test_data()

    print("\n" + "="*70)
    print("[SUCCESS] 所有测试通过!")
    print("="*70)


if __name__ == "__main__":
    test_category_entry_tracking()
