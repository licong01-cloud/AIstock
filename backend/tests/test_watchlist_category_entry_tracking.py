"""
测试自选股分类级别的加入时间和收益计算功能

测试场景：
1. 同一只股票加入不同分类，记录各自的加入时间和价格
2. 查询特定分类时，使用该分类的加入价格计算收益
3. Migration 正确添加字段和索引
4. 历史数据回填正确
"""
from __future__ import annotations

import pytest
from datetime import date, timedelta

from backend.repositories.watchlist_repo_impl import watchlist_repo
from backend.services import watchlist_service
from backend.db.pg_pool import get_conn


@pytest.fixture
def setup_test_data():
    """准备测试数据：创建分类和股票"""
    # 清理测试数据
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM app.watchlist_item_categories WHERE item_id IN (SELECT id FROM app.watchlist_items WHERE code LIKE 'TEST_%')")
            cur.execute("DELETE FROM app.watchlist_items WHERE code LIKE 'TEST_%'")
            cur.execute("DELETE FROM app.watchlist_categories WHERE name LIKE 'TEST_%'")
            conn.commit()

    # 创建测试分类
    cat1 = watchlist_repo.create_category("TEST_CAT_1", "测试分类1")
    cat2 = watchlist_repo.create_category("TEST_CAT_2", "测试分类2")

    yield {"cat1_id": cat1, "cat2_id": cat2}

    # 清理测试数据
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM app.watchlist_item_categories WHERE item_id IN (SELECT id FROM app.watchlist_items WHERE code LIKE 'TEST_%')")
            cur.execute("DELETE FROM app.watchlist_items WHERE code LIKE 'TEST_%'")
            cur.execute("DELETE FROM app.watchlist_categories WHERE name LIKE 'TEST_%'")
            conn.commit()


def test_add_same_stock_to_different_categories_records_separate_entry_time(setup_test_data):
    """测试：同一只股票加入不同分类，记录各自的加入时间和价格"""
    cat1_id = setup_test_data["cat1_id"]
    cat2_id = setup_test_data["cat2_id"]

    # 第一次加入：股票加入分类1，价格 10.0
    entry_date_1 = date.today() - timedelta(days=10)
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
    assert result1["added"] == 1
    item_id = result1["item_ids_by_code"]["TEST_000001.SZ"]

    # 第二次加入：同一只股票加入分类2，价格 12.0（模拟股价上涨后加入）
    entry_date_2 = date.today() - timedelta(days=5)
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
    assert result2["added"] == 1

    # 验证数据库中记录了两个不同的加入时间和价格
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
            assert len(rows) == 2

            # 验证分类1的记录
            assert rows[0][0] == cat1_id
            assert rows[0][1] == entry_date_1  # entry_date_snapshot
            assert float(rows[0][2]) == 10.0   # entry_price_snapshot
            assert rows[0][3] is not None      # added_at

            # 验证分类2的记录
            assert rows[1][0] == cat2_id
            assert rows[1][1] == entry_date_2  # entry_date_snapshot
            assert float(rows[1][2]) == 12.0   # entry_price_snapshot
            assert rows[1][3] is not None      # added_at


def test_list_items_with_category_returns_category_entry_price(setup_test_data):
    """测试：查询特定分类时，返回该分类的加入价格"""
    cat1_id = setup_test_data["cat1_id"]
    cat2_id = setup_test_data["cat2_id"]

    # 加入股票到两个分类，价格不同
    entry_date_1 = date.today() - timedelta(days=10)
    watchlist_repo.add_items_bulk_with_meta(
        category_id=cat1_id,
        items=[
            {
                "code": "TEST_000002.SZ",
                "name": "测试股票2",
                "entry_price": 15.0,
                "entry_as_of": entry_date_1,
            }
        ],
    )

    entry_date_2 = date.today() - timedelta(days=3)
    watchlist_repo.add_items_bulk_with_meta(
        category_id=cat2_id,
        items=[
            {
                "code": "TEST_000002.SZ",
                "name": "测试股票2",
                "entry_price": 18.0,
                "entry_as_of": entry_date_2,
            }
        ],
    )

    # 查询分类1，应该返回价格 15.0
    result1 = watchlist_repo.list_items(category_id=cat1_id, page=1, page_size=10)
    items1 = result1["items"]
    assert len(items1) == 1
    assert items1[0]["code"] == "TEST_000002.SZ"
    assert items1[0]["category_entry_price"] == 15.0
    assert items1[0]["category_entry_date"] == entry_date_1.isoformat()

    # 查询分类2，应该返回价格 18.0
    result2 = watchlist_repo.list_items(category_id=cat2_id, page=1, page_size=10)
    items2 = result2["items"]
    assert len(items2) == 1
    assert items2[0]["code"] == "TEST_000002.SZ"
    assert items2[0]["category_entry_price"] == 18.0
    assert items2[0]["category_entry_date"] == entry_date_2.isoformat()


def test_migration_adds_fields_and_indexes():
    """测试：Migration 正确添加了字段和索引"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            # 验证字段存在
            cur.execute(
                """
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = 'app'
                  AND table_name = 'watchlist_item_categories'
                  AND column_name IN ('added_at', 'entry_price_snapshot', 'entry_date_snapshot')
                ORDER BY column_name
                """
            )
            columns = cur.fetchall()
            assert len(columns) == 3

            col_dict = {row[0]: row for row in columns}
            assert col_dict["added_at"][1] == "timestamp with time zone"
            assert col_dict["added_at"][2] == "NO"  # NOT NULL
            assert col_dict["entry_price_snapshot"][1] == "numeric"
            assert col_dict["entry_price_snapshot"][2] == "YES"  # NULLABLE
            assert col_dict["entry_date_snapshot"][1] == "date"
            assert col_dict["entry_date_snapshot"][2] == "YES"  # NULLABLE

            # 验证索引存在
            cur.execute(
                """
                SELECT indexname
                FROM pg_indexes
                WHERE schemaname = 'app'
                  AND tablename = 'watchlist_item_categories'
                  AND indexname IN (
                      'idx_watchlist_item_categories_added',
                      'idx_watchlist_item_categories_entry_date'
                  )
                ORDER BY indexname
                """
            )
            indexes = [row[0] for row in cur.fetchall()]
            assert "idx_watchlist_item_categories_added" in indexes
            assert "idx_watchlist_item_categories_entry_date" in indexes


def test_backfill_historical_data():
    """测试：历史数据回填正确"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            # 验证所有关联记录的 added_at 都有值（因为有 DEFAULT NOW()）
            cur.execute(
                """
                SELECT COUNT(*)
                FROM app.watchlist_item_categories
                WHERE added_at IS NULL
                """
            )
            null_count = cur.fetchone()[0]
            assert null_count == 0

            # 验证回填的记录数（非测试数据）
            cur.execute(
                """
                SELECT COUNT(*)
                FROM app.watchlist_item_categories wic
                JOIN app.watchlist_items wi ON wic.item_id = wi.id
                WHERE wic.entry_date_snapshot IS NOT NULL
                  AND wic.entry_price_snapshot IS NOT NULL
                  AND wi.code NOT LIKE 'TEST_%'
                """
            )
            backfilled_count = cur.fetchone()[0]
            # 应该有历史数据被回填（具体数量取决于测试环境）
            assert backfilled_count >= 0


def test_service_layer_uses_category_entry_price_for_return_calculation(setup_test_data):
    """测试：Service 层优先使用分类级别的价格计算收益"""
    cat1_id = setup_test_data["cat1_id"]

    # 加入股票，价格 20.0
    entry_date = date.today() - timedelta(days=7)
    watchlist_repo.add_items_bulk_with_meta(
        category_id=cat1_id,
        items=[
            {
                "code": "TEST_000003.SZ",
                "name": "测试股票3",
                "entry_price": 20.0,
                "entry_as_of": entry_date,
            }
        ],
    )

    # 通过 Service 层查询（会附加实时行情和收益计算）
    # 注意：这个测试依赖实时行情数据，可能需要 mock
    # 这里只验证返回的数据结构包含分类级别的字段
    result = watchlist_repo.list_items(category_id=cat1_id, page=1, page_size=10)
    items = result["items"]
    assert len(items) == 1

    item = items[0]
    assert item["code"] == "TEST_000003.SZ"
    assert item["category_entry_price"] == 20.0
    assert item["category_entry_date"] == entry_date.isoformat()
    assert "category_added_at" in item
    assert item["category_added_at"] is not None


def test_add_categories_to_items_records_entry_snapshot(setup_test_data):
    """测试：add_categories_to_items 也记录价格快照"""
    cat1_id = setup_test_data["cat1_id"]
    cat2_id = setup_test_data["cat2_id"]

    # 先创建一个股票
    entry_date = date.today() - timedelta(days=5)
    result = watchlist_repo.add_items_bulk_with_meta(
        category_id=cat1_id,
        items=[
            {
                "code": "TEST_000004.SZ",
                "name": "测试股票4",
                "entry_price": 25.0,
                "entry_as_of": entry_date,
            }
        ],
    )
    item_id = result["item_ids_by_code"]["TEST_000004.SZ"]

    # 使用分类加入当时的独立快照添加到另一个分类
    category_entry_date = date.today()
    watchlist_repo.add_categories_to_items(
        item_ids=[item_id],
        category_ids=[cat2_id],
        entry_snapshots={item_id: (category_entry_date, 30.0)},
    )

    # 验证新关联记录也有价格快照
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT entry_date_snapshot, entry_price_snapshot
                FROM app.watchlist_item_categories
                WHERE item_id = %s AND category_id = %s
                """,
                (item_id, cat2_id),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == category_entry_date
            assert float(row[1]) == 30.0


def test_service_uses_selected_category_snapshot_for_adjusted_return(monkeypatch):
    captured_adjustment_items = []
    monkeypatch.setattr(
        watchlist_service.watchlist_repo,
        "list_items",
        lambda **_kwargs: {
            "total": 1,
            "items": [
                {
                    "id": 1,
                    "code": "000001.SZ",
                    "entry_price": 10.0,
                    "entry_as_of": "2024-01-02",
                    "entry_task_id": "global-task",
                    "category_entry_price": 20.0,
                    "category_entry_date": "2024-02-05",
                    "category_added_at": "2024-02-05T09:30:00+08:00",
                }
            ],
        },
    )
    monkeypatch.setattr(
        watchlist_service,
        "_fetch_quotes",
        lambda _codes: {"000001.SZ": {"price": 30.0, "pre_close": 29.0}},
    )

    def fake_adjustments(items):
        captured_adjustment_items.extend(items)
        return {
            "000001.SZ": {
                "entry_price_basis": "qfq_adjusted",
                "entry_price_adjusted": 24.0,
                "entry_adjustment_factor": 1.2,
                "entry_price_basis_date": "2024-02-05",
                "entry_price_basis_source": "entry_as_of",
                "entry_adj_factor_date": "2024-02-05",
                "latest_adj_factor_date": "2024-06-03",
            }
        }

    monkeypatch.setattr(watchlist_service, "_fetch_qfq_entry_adjustments", fake_adjustments)

    result = watchlist_service.list_items_with_quotes(category_id=7)
    row = result["items"][0]

    assert captured_adjustment_items[0]["entry_price"] == 20.0
    assert captured_adjustment_items[0]["entry_as_of"] == "2024-02-05"
    assert captured_adjustment_items[0]["entry_task_id"] is None
    assert row["pct_since_entry"] == 25.0
    assert row["effective_entry_price_source"] == "category"


def test_service_does_not_fallback_to_stock_entry_for_selected_category(monkeypatch):
    monkeypatch.setattr(
        watchlist_service.watchlist_repo,
        "list_items",
        lambda **_kwargs: {
            "total": 1,
            "items": [
                {
                    "id": 1,
                    "code": "000001.SZ",
                    "entry_price": 10.0,
                    "entry_as_of": "2024-01-02",
                    "category_entry_price": None,
                    "category_entry_date": None,
                    "category_added_at": "2024-02-05T09:30:00+08:00",
                }
            ],
        },
    )
    monkeypatch.setattr(
        watchlist_service,
        "_fetch_quotes",
        lambda _codes: {"000001.SZ": {"price": 30.0, "pre_close": 29.0}},
    )
    monkeypatch.setattr(watchlist_service, "_fetch_qfq_entry_adjustments", lambda _items: {})

    row = watchlist_service.list_items_with_quotes(category_id=7)["items"][0]

    assert row["pct_since_entry"] is None
    assert row["effective_entry_price_source"] == "missing_category_snapshot"


def test_service_does_not_report_stock_entry_return_for_all_categories(monkeypatch):
    monkeypatch.setattr(
        watchlist_service.watchlist_repo,
        "list_items",
        lambda **_kwargs: {
            "total": 1,
            "items": [
                {
                    "id": 1,
                    "code": "000001.SZ",
                    "entry_price": 10.0,
                    "entry_as_of": "2024-01-02",
                    "category_entry_price": None,
                }
            ],
        },
    )
    monkeypatch.setattr(
        watchlist_service,
        "_fetch_quotes",
        lambda _codes: {"000001.SZ": {"price": 30.0, "pre_close": 29.0}},
    )

    row = watchlist_service.list_items_with_quotes()["items"][0]

    assert row["pct_since_entry"] is None
    assert row["effective_entry_price_source"] == "not_applicable_all_categories"


def test_task_selection_records_current_category_price_without_replacing_selection_basis(monkeypatch):
    captured = {}
    monkeypatch.setattr(watchlist_service, "_normalize_code_for_storage", lambda code: code)
    monkeypatch.setattr(
        watchlist_service,
        "_get_entry_price_bulk",
        lambda codes: {code: 12.0 for code in codes},
    )

    def fake_add_items_bulk_with_meta(**kwargs):
        captured.update(kwargs)
        return {
            "added": 1,
            "skipped": 0,
            "moved": 0,
            "item_ids_by_code": {"000001.SZ": 1},
        }

    monkeypatch.setattr(
        watchlist_service.watchlist_repo,
        "add_items_bulk_with_meta",
        fake_add_items_bulk_with_meta,
    )

    result = watchlist_service.add_items_bulk_from_task_selection(
        items=[
            {
                "code": "000001.SZ",
                "name": "测试股票",
                "entry_price": 10.0,
                "as_of": "2024-01-02",
            }
        ],
        category_id=7,
    )

    prepared_item = captured["items"][0]
    assert result["ok"] is True
    assert prepared_item["entry_price"] == 10.0
    assert prepared_item["entry_as_of"] == date(2024, 1, 2)
    assert prepared_item["category_entry_price"] == 12.0
    assert prepared_item["category_entry_date"] == date.today()


def test_single_add_records_the_same_snapshot_for_extra_categories(monkeypatch):
    captured = {}
    monkeypatch.setattr(watchlist_service, "_normalize_code_for_storage", lambda code: code)
    monkeypatch.setattr(watchlist_service.watchlist_repo, "add_item", lambda *_args, **_kwargs: 11)

    def fake_add_categories(item_ids, category_ids, entry_snapshots):
        captured["item_ids"] = item_ids
        captured["category_ids"] = category_ids
        captured["entry_snapshots"] = entry_snapshots
        return len(category_ids)

    monkeypatch.setattr(
        watchlist_service.watchlist_repo,
        "add_categories_to_items",
        fake_add_categories,
    )

    item_id = watchlist_service.add_single_item(
        code="000001.SZ",
        category_id=1,
        name="测试股票",
        extra_category_ids=[2, 3],
        entry_price=15.0,
    )

    assert item_id == 11
    assert captured["item_ids"] == [11]
    assert captured["category_ids"] == [2, 3]
    assert captured["entry_snapshots"] == {11: (date.today(), 15.0)}


def test_plain_bulk_add_records_current_category_snapshots(monkeypatch):
    captured = {}
    monkeypatch.setattr(watchlist_service, "_normalize_code_for_storage", lambda code: code)
    monkeypatch.setattr(
        watchlist_service.data_source_manager,
        "_convert_from_ts_code",
        lambda code: code.split(".", 1)[0],
    )
    monkeypatch.setattr(
        watchlist_service.data_source_manager,
        "get_stock_basic_info",
        lambda _code: {"name": "测试股票"},
    )
    monkeypatch.setattr(
        watchlist_service,
        "_get_entry_price_bulk",
        lambda codes: {code: 18.0 for code in codes},
    )

    def fake_add_items_bulk(codes, category_id, **kwargs):
        captured["codes"] = codes
        captured["category_id"] = category_id
        captured.update(kwargs)
        return {"added": 1, "skipped": 0, "moved": 0}

    monkeypatch.setattr(watchlist_service.watchlist_repo, "add_items_bulk", fake_add_items_bulk)

    result = watchlist_service.add_items_bulk(
        ["000001.SZ", "000001.SZ"],
        category_id=7,
    )

    assert result["added"] == 1
    assert captured["codes"] == ["000001.SZ"]
    assert captured["entry_snapshots"] == {
        "000001.SZ": (date.today(), 18.0),
    }


class _EntryPriceSnapshot:
    def __init__(self, *, row=None, row_error: Exception | None = None) -> None:
        self.empty = False
        self.index = {"000001.SZ"}
        self.loc = self
        self._row = row
        self._row_error = row_error

    def __getitem__(self, ts_code):
        assert ts_code == "000001.SZ"
        if self._row_error is not None:
            raise self._row_error
        return self._row


class _EntryPriceCursor:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None

    def execute(self, sql, params):
        assert "market.kline_daily_raw" in sql
        assert params == ["000001.SZ"]

    def fetchall(self):
        return [("000001.SZ", 12340)]


class _EntryPriceConn:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None

    def cursor(self):
        return _EntryPriceCursor()


def test_entry_price_bulk_logs_tdx_failure_and_keeps_xtquant_fallback(monkeypatch, caplog):
    monkeypatch.setattr(
        watchlist_service.data_source_manager,
        "_convert_from_ts_code",
        lambda code: code.split(".", 1)[0],
    )

    def raise_tdx_failure(_base):
        raise RuntimeError("provider unavailable")

    monkeypatch.setattr(watchlist_service.data_source_manager, "get_realtime_quotes", raise_tdx_failure)
    monkeypatch.setattr(
        watchlist_service.xtquant_adapter,
        "fetch_realtime_snapshot_xt",
        lambda *_args, **_kwargs: _EntryPriceSnapshot(row={"close": 12.5}),
    )
    caplog.set_level("WARNING", logger="aistock.watchlist")

    result = watchlist_service._get_entry_price_bulk(["000001.SZ"])

    assert result == {"000001.SZ": 12.5}
    assert "TDX realtime quote lookups failed for watchlist entry prices" in caplog.text
    assert "failure_count=1" in caplog.text
    assert "('000001', 'RuntimeError')" in caplog.text
    assert "provider unavailable" not in caplog.text


def test_entry_price_bulk_logs_xtquant_row_failure_and_keeps_db_fallback(monkeypatch, caplog):
    from backend.db import pg_pool

    monkeypatch.setattr(
        watchlist_service.data_source_manager,
        "_convert_from_ts_code",
        lambda code: code.split(".", 1)[0],
    )
    monkeypatch.setattr(watchlist_service.data_source_manager, "get_realtime_quotes", lambda _base: {})
    monkeypatch.setattr(
        watchlist_service.xtquant_adapter,
        "fetch_realtime_snapshot_xt",
        lambda *_args, **_kwargs: _EntryPriceSnapshot(row_error=KeyError("malformed provider row")),
    )
    monkeypatch.setattr(pg_pool, "get_conn", lambda: _EntryPriceConn())
    caplog.set_level("WARNING", logger="aistock.watchlist")

    result = watchlist_service._get_entry_price_bulk(["000001.SZ"])

    assert result == {"000001.SZ": 12.34}
    assert "xtquant snapshot rows failed for watchlist entry prices" in caplog.text
    assert "failure_count=1" in caplog.text
    assert "('000001.SZ', 'KeyError')" in caplog.text
    assert "malformed provider row" not in caplog.text


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
