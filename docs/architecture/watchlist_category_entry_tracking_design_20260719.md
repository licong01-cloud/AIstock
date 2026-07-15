# 自选股分类级别加入时间追踪设计方案

> 设计日期：2026-07-19
> 状态：设计稿
> 分类：F1（数据库 schema 变更 + 业务逻辑改动）
> 关联 Issue：无（用户需求）

## 1. 需求背景

### 1.1 当前问题

当前自选股功能存在以下问题：

1. **只记录股票级别的加入时间**：`watchlist_items` 表中 `code` 是 UNIQUE 约束，同一股票只有一条记录
2. **无法记录每个分类的加入时间**：`watchlist_item_categories` 关联表只有 `(item_id, category_id)` 主键，没有时间戳
3. **无法按分类计算收益**：收益计算基于股票级别的 `entry_price`，不是分类级别的

### 1.2 用户场景

用户希望实现：

- 股票 A 今天加入"自选1"分类，entry_price = 25.50
- 股票 A 明天加入"自选2"分类，entry_price = 28.80
- 查看"自选1"时：显示收益 = (当前价 - 25.50) / 25.50
- 查看"自选2"时：显示收益 = (当前价 - 28.80) / 28.80

### 1.3 核心需求

**统计特定分类中的股票，加入这个分类以后的收益**

## 2. 设计方案

### 2.1 方案选择：关联表增加时间戳（推荐）

**优点：**
- 改动最小，风险可控
- 满足核心需求
- 向后兼容，平滑迁移
- 查询性能影响小

**缺点：**
- 不支持完整的加入/移除历史（如股票从分类移除后再加入）

### 2.2 数据库 Schema 变更

#### DDL 变更

```sql
-- Phase 1: 增加字段（向后兼容）
ALTER TABLE app.watchlist_item_categories 
    ADD COLUMN IF NOT EXISTS added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS entry_price_snapshot NUMERIC,
    ADD COLUMN IF NOT EXISTS entry_date_snapshot DATE;

-- Phase 2: 创建索引
CREATE INDEX IF NOT EXISTS idx_watchlist_item_categories_added 
    ON app.watchlist_item_categories(category_id, added_at DESC);

CREATE INDEX IF NOT EXISTS idx_watchlist_item_categories_entry_date
    ON app.watchlist_item_categories(item_id, category_id, entry_date_snapshot DESC);

-- Phase 3: 添加 Comment
COMMENT ON COLUMN app.watchlist_item_categories.added_at IS 
    '股票加入该分类的时间戳，用于分类级别的加入时间追踪和收益计算基准';

COMMENT ON COLUMN app.watchlist_item_categories.entry_price_snapshot IS 
    '股票加入该分类时的价格快照（原始价格），用于计算该分类下的收益；NULL 表示加入时未记录价格';

COMMENT ON COLUMN app.watchlist_item_categories.entry_date_snapshot IS 
    '股票加入该分类时的基准日期，用于复权调整；通常与 added_at 日期一致，但可能根据选股任务的 as_of 日期调整';

-- Phase 4: 回填历史数据
UPDATE app.watchlist_item_categories wic
   SET entry_date_snapshot = COALESCE(wi.entry_as_of, wi.created_at::date),
       entry_price_snapshot = wi.entry_price
  FROM app.watchlist_items wi
 WHERE wic.item_id = wi.id
   AND wic.entry_date_snapshot IS NULL;
```

#### 字段说明

| 字段 | 类型 | 约束 | 说明 |
|------|------|------|------|
| `added_at` | TIMESTAMPTZ | NOT NULL DEFAULT NOW() | 加入该分类的时间戳 |
| `entry_price_snapshot` | NUMERIC | NULLABLE | 加入该分类时的价格快照（原始价格） |
| `entry_date_snapshot` | DATE | NULLABLE | 加入该分类时的基准日期 |

### 2.3 代码改动范围

#### 2.3.1 Repository 层 (`backend/repositories/watchlist_repo_impl.py`)

**改动点 1：`add_items_bulk_with_meta` 方法**

插入关联关系时记录分类级别的加入信息：

```python
def add_items_bulk_with_meta(...):
    # 获取当前价格快照（如果未提供）
    entry_date = date.today()
    price_map = {}  # {code: price}
    
    # ... 现有逻辑获取价格 ...
    
    # 构造关联表插入数据，包含价格和日期快照
    map_rows = []
    for iid in item_ids:
        code = code_to_id_reverse.get(iid)
        price = price_map.get(code) or prepared_item_map.get(code, {}).get('entry_price')
        map_rows.append((
            iid, 
            category_id,
            entry_date,
            price
        ))
    
    pg_extras.execute_values(
        cur,
        """
        INSERT INTO app.watchlist_item_categories(
            item_id, 
            category_id, 
            entry_date_snapshot, 
            entry_price_snapshot
        ) 
        VALUES %s 
        ON CONFLICT (item_id, category_id) DO NOTHING
        """,
        map_rows,
        page_size=1000,
    )
```

**改动点 2：`list_items` 方法**

查询时返回分类级别的加入信息：

```python
def list_items(...):
    sql = f"""
        SELECT i.id, i.code, i.name, i.note, 
               i.created_at, i.updated_at,
               COALESCE(string_agg(DISTINCT c.name, ',' ORDER BY c.name), '') AS cat_names,
               COALESCE(array_agg(DISTINCT c.id), ARRAY[]::BIGINT[]) AS cat_ids,
               -- 分类级别的加入信息
               w.added_at AS category_added_at,
               w.entry_price_snapshot AS category_entry_price,
               w.entry_date_snapshot AS category_entry_date,
               -- 原有字段
               a.analysis_date AS last_analysis_time,
               a.rating AS last_rating,
               ...
          FROM app.watchlist_items i
          LEFT JOIN app.watchlist_item_categories w ON w.item_id = i.id
          LEFT JOIN app.watchlist_categories c ON c.id = w.category_id
         WHERE {where}  -- category_id 过滤
      ORDER BY {order_expr} {dir_kw} NULLS LAST, i.code ASC
    """
```

#### 2.3.2 Service 层 (`backend/services/watchlist_service.py`)

**改动点 1：收益计算函数**

修改 `_compute_realtime_fields` 函数，优先使用分类级别的加入价格：

```python
def _compute_realtime_fields(
    q: Dict[str, Any],
    entry_price: Optional[float] = None,
    entry_price_for_return: Optional[float] = None,
    category_entry_price: Optional[float] = None,  # 新增参数
) -> Dict[str, Optional[float]]:
    # ... 现有逻辑 ...
    
    # 计算加入以来涨幅 - 优先使用分类级别的加入价格
    pct_since_entry = None
    basis_entry_price = (
        _optional_float(category_entry_price) or  # 优先
        _optional_float(entry_price_for_return) or 
        _optional_float(entry_price)
    )
    
    if effective_price is not None and basis_entry_price is not None:
        try:
            pct_since_entry = (effective_price - basis_entry_price) / basis_entry_price * 100.0
        except Exception:
            pct_since_entry = None
    
    return {
        "last": effective_price,
        "pct_change": pct,
        "pct_since_entry": pct_since_entry,
        "pct_since_entry_basis": "category" if category_entry_price else "item",
        ...
    }
```

**改动点 2：列表查询服务**

修改 `list_items_with_quotes` 函数，传递分类级别的价格信息：

```python
def list_items_with_quotes(...):
    base = watchlist_repo.list_items(...)
    items: List[Dict[str, Any]] = base.get("items", [])
    
    # ... 获取行情和复权调整 ...
    
    enriched: List[Dict[str, Any]] = []
    for it in items:
        code = str(it.get("code"))
        entry_price = it.get("entry_price")
        category_entry_price = it.get("category_entry_price")  # 新增
        category_entry_date = it.get("category_entry_date")     # 新增
        
        # 如果查询特定分类，优先使用分类级别的价格进行复权调整
        if category_entry_price and category_entry_date:
            # 基于 category_entry_date 进行复权调整
            # ...
            entry_price_for_return = adjusted_category_price
        else:
            entry_price_for_return = entry_adjustment.get("entry_price_adjusted")
        
        q = quotes_raw.get(code, {})
        rt = _compute_realtime_fields(
            q, 
            entry_price=entry_price, 
            entry_price_for_return=entry_price_for_return,
            category_entry_price=category_entry_price  # 新增
        )
        
        row = dict(it)
        for k, v in rt.items():
            row[k] = v
        
        # 添加分类级别的元数据
        row["category_added_at"] = it.get("category_added_at")
        row["category_entry_price"] = category_entry_price
        row["category_entry_date"] = category_entry_date
        
        enriched.append(row)
    
    return {"total": base.get("total", len(enriched)), "items": enriched}
```

**改动点 3：批量添加服务**

修改 `add_items_bulk_from_task_selection` 函数，在插入时记录价格快照：

```python
def add_items_bulk_from_task_selection(...):
    # ... 现有逻辑 ...
    
    prepared.append({
        "code": ts_code,
        "name": display_name,
        "entry_price": float(p),  # 股票级别
        "entry_rank": it.get("rank"),
        "entry_source": entry_source,
        "entry_as_of": as_of,
        # 新增：用于关联表的价格快照
        "_category_entry_price": float(p),
        "_category_entry_date": as_of,
    })
```

#### 2.3.3 Migration 脚本

创建新的 migration 脚本：

```python
# backend/db/migrations/add_watchlist_category_entry_tracking_20260719.py
"""
为 watchlist_item_categories 表增加分类级别的加入时间追踪字段

对应设计：docs/architecture/watchlist_category_entry_tracking_design_20260719.md
"""
from ..pg_pool import get_conn

DDL = [
    """
    ALTER TABLE app.watchlist_item_categories 
        ADD COLUMN IF NOT EXISTS added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        ADD COLUMN IF NOT EXISTS entry_price_snapshot NUMERIC,
        ADD COLUMN IF NOT EXISTS entry_date_snapshot DATE;
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_watchlist_item_categories_added 
        ON app.watchlist_item_categories(category_id, added_at DESC);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_watchlist_item_categories_entry_date
        ON app.watchlist_item_categories(item_id, category_id, entry_date_snapshot DESC);
    """,
    """
    COMMENT ON COLUMN app.watchlist_item_categories.added_at IS 
        '股票加入该分类的时间戳，用于分类级别的加入时间追踪和收益计算基准';
    """,
    """
    COMMENT ON COLUMN app.watchlist_item_categories.entry_price_snapshot IS 
        '股票加入该分类时的价格快照（原始价格），用于计算该分类下的收益；NULL 表示加入时未记录价格';
    """,
    """
    COMMENT ON COLUMN app.watchlist_item_categories.entry_date_snapshot IS 
        '股票加入该分类时的基准日期，用于复权调整；通常与 added_at 日期一致，但可能根据选股任务的 as_of 日期调整';
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

def apply_migration():
    """应用 migration"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            for sql in DDL:
                cur.execute(sql)
            
            # 回填历史数据
            cur.execute(BACKFILL_SQL)
            affected = cur.rowcount
            print(f"✅ 回填了 {affected} 条历史数据")

if __name__ == "__main__":
    from pathlib import Path
    from dotenv import load_dotenv
    
    env_path = Path(__file__).resolve().parents[3] / ".env"
    load_dotenv(env_path, override=True)
    
    print("🚀 开始应用 watchlist_category_entry_tracking migration...")
    apply_migration()
    print("✅ Migration 完成")
```

### 2.4 前端改动（可选）

前端 UI 需要显示分类级别的加入信息：

1. **列表页面**：显示"加入该分类的时间"和"加入以来收益"
2. **详情页面**：显示不同分类下的不同收益表现
3. **筛选排序**：支持按"加入该分类时间"排序

## 3. 测试计划

### 3.1 单元测试

**测试文件：** `backend/tests/services/test_watchlist_category_tracking.py`

测试用例：
1. `test_add_item_records_category_entry_price` - 验证加入时记录价格快照
2. `test_same_stock_different_categories_different_prices` - 验证同一股票在不同分类的不同价格
3. `test_list_items_returns_category_level_info` - 验证查询返回分类级别信息
4. `test_return_calculation_uses_category_price` - 验证收益计算使用分类价格

### 3.2 集成测试

**测试场景：**

```python
# 场景1：股票多次加入不同分类
1. 创建分类 "自选1" 和 "自选2"
2. 股票 A (000001.SZ) 加入 "自选1"，记录 price=25.50, date=2026-07-19
3. 模拟价格变化到 28.80
4. 股票 A 加入 "自选2"，记录 price=28.80, date=2026-07-20
5. 验证：
   - 查询 "自选1"：entry_price=25.50, 收益基于 25.50
   - 查询 "自选2"：entry_price=28.80, 收益基于 28.80

# 场景2：历史数据回填
1. 查询 migration 前的历史数据
2. 应用 migration
3. 验证历史数据正确回填 entry_date_snapshot 和 entry_price_snapshot
```

### 3.3 数据完整性验证

```sql
-- 验证：所有关联都有加入时间
SELECT COUNT(*) 
FROM app.watchlist_item_categories 
WHERE added_at IS NULL;
-- 预期：0

-- 验证：历史数据正确回填
SELECT COUNT(*) 
FROM app.watchlist_item_categories wic
JOIN app.watchlist_items wi ON wi.id = wic.item_id
WHERE wic.entry_price_snapshot IS NOT NULL
  AND wi.entry_price IS NOT NULL;
-- 预期：> 0

-- 验证：新增数据包含价格快照
SELECT * 
FROM app.watchlist_item_categories 
WHERE added_at >= '2026-07-19'::date
  AND entry_price_snapshot IS NULL
LIMIT 10;
-- 预期：仅在价格获取失败时为空
```

## 4. 验收标准

### 4.1 功能验收

- [ ] ✅ 股票加入分类时，正确记录 `added_at`、`entry_price_snapshot`、`entry_date_snapshot`
- [ ] ✅ 同一股票加入不同分类时，每个分类独立记录加入信息
- [ ] ✅ 查询特定分类时，返回该分类级别的加入信息
- [ ] ✅ 收益计算优先使用分类级别的 `entry_price_snapshot`
- [ ] ✅ 历史数据正确回填，无数据丢失
- [ ] ✅ 所有新增字段都有 PostgreSQL COMMENT

### 4.2 性能验收

- [ ] ✅ 列表查询性能无明显下降（< 10% 延迟增加）
- [ ] ✅ 批量添加操作性能无明显下降
- [ ] ✅ 索引正确创建，查询计划合理

### 4.3 兼容性验收

- [ ] ✅ 现有 API 接口向后兼容
- [ ] ✅ 前端可以正常展示新字段（如果未使用则不显示）
- [ ] ✅ Migration 可重复执行（幂等性）

## 5. 风险评估

### 5.1 低风险

- DDL 变更是 ADD COLUMN，向后兼容
- 新增字段允许 NULL，不影响现有数据
- 使用索引优化查询性能

### 5.2 需要注意

- **历史数据回填**：需要在低峰期执行，避免长时间锁表
- **价格快照准确性**：依赖实时行情接口的可用性
- **复权调整**：需要确保分类级别的复权计算正确

### 5.3 回滚方案

如果出现问题，可以快速回滚：

```sql
-- 回滚步骤1：删除索引
DROP INDEX IF EXISTS idx_watchlist_item_categories_added;
DROP INDEX IF EXISTS idx_watchlist_item_categories_entry_date;

-- 回滚步骤2：删除字段（可选，建议保留）
-- ALTER TABLE app.watchlist_item_categories 
--     DROP COLUMN IF EXISTS added_at,
--     DROP COLUMN IF EXISTS entry_price_snapshot,
--     DROP COLUMN IF EXISTS entry_date_snapshot;
```

## 6. 实施步骤

### 6.1 Phase 1：数据库 Migration（1天）

1. 创建 migration 脚本
2. 在开发环境验证
3. 在测试环境执行并验证数据完整性

### 6.2 Phase 2：后端代码改动（2天）

1. 修改 Repository 层插入和查询逻辑
2. 修改 Service 层收益计算逻辑
3. 编写单元测试和集成测试

### 6.3 Phase 3：验证和上线（1天）

1. 在测试环境完整验证
2. 性能测试
3. 生产环境低峰期执行 migration
4. 部署代码并验证

## 7. 后续优化方向

### 7.1 完整历史追踪（未来）

如果需要支持"股票从分类移除后再加入"的场景，可以升级为独立历史表：

```sql
CREATE TABLE app.watchlist_category_entries (
    id BIGSERIAL PRIMARY KEY,
    item_id BIGINT NOT NULL,
    category_id BIGINT NOT NULL,
    added_at TIMESTAMPTZ NOT NULL,
    entry_price NUMERIC NOT NULL,
    entry_date DATE NOT NULL,
    removed_at TIMESTAMPTZ,  -- NULL 表示仍在该分类中
    UNIQUE(item_id, category_id, added_at)
);
```

### 7.2 分类级别的止损/止盈

未来可以基于分类级别的加入价格，设置分类级别的止损/止盈规则。

## 8. 附录

### 8.1 相关文件清单

**DDL/Migration:**
- `backend/db/migrations/add_watchlist_category_entry_tracking_20260719.py`
- `backend/db/init_watchlist_schema.py` (补充新字段的 DDL)

**Repository:**
- `backend/repositories/watchlist_repo_impl.py`

**Service:**
- `backend/services/watchlist_service.py`

**测试:**
- `backend/tests/services/test_watchlist_category_tracking.py`
- `backend/tests/integration/test_watchlist_category_flow.py`

**文档:**
- `docs/architecture/watchlist_category_entry_tracking_design_20260719.md` (本文档)

### 8.2 设计变更记录

| 日期 | 版本 | 变更内容 | 变更人 |
|------|------|----------|--------|
| 2026-07-19 | v1.0 | 初始设计 | Claude |

---

## Design Acceptance Index

用于 aistock-feature-workflow 验收矩阵：

1. **[DESIGN-001]** 数据库 schema 增加 `added_at`、`entry_price_snapshot`、`entry_date_snapshot` 字段
2. **[DESIGN-002]** 所有新增字段有 PostgreSQL COMMENT
3. **[DESIGN-003]** 创建索引优化查询性能
4. **[DESIGN-004]** Repository 层插入时记录分类级别的价格快照
5. **[DESIGN-005]** Repository 层查询时返回分类级别的加入信息
6. **[DESIGN-006]** Service 层收益计算优先使用分类级别的加入价格
7. **[DESIGN-007]** 历史数据正确回填
8. **[DESIGN-008]** Migration 脚本幂等且可重复执行
9. **[DESIGN-009]** 单元测试覆盖核心逻辑
10. **[DESIGN-010]** 集成测试验证端到端流程
