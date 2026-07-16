# Phase 0 代码审查报告

> **审查日期**: 2026-07-16  
> **审查人**: Kiro (Claude Code)  
> **审查范围**: Phase 0 数据源抽象层全部源码  
> **审查模式**: 严格审查（禁止简化实现、静默错误、业务逻辑偏移）

---

## 🎯 审查目标

根据用户要求，本次审查**严格禁止**以下情况：

1. ❌ **简化版实现** - 所有功能必须完整实现
2. ❌ **静默错误** - 所有异常必须明确处理并上报
3. ❌ **业务逻辑偏移** - 必须严格符合设计文档
4. ❌ **未经确认的门禁和审批功能** - Phase 0 不包含任何审批流程

---

## 📊 审查统计

| 指标 | 数值 | 状态 |
|------|------|------|
| 审查文件数 | 6 | ✅ |
| 代码总行数 | 1,940 | ✅ |
| 语法检查 | 通过 | ✅ |
| 隔离约束检查 | 通过 | ✅ |
| 异常处理检查 | **1 个问题** | ⚠️ |
| 业务逻辑检查 | **1 个问题** | ⚠️ |

---

## 🔴 严重问题（必须修复）

### 问题 1: 静默异常处理（违反禁止静默错误原则）

**位置**: `backend/services/hmm_data_source/cache_manager.py:270` 和 `291`

**问题代码**:
```python
# Line 268-271
try:
    all_metadata = json.loads(metadata_path.read_text())
except:
    all_metadata = {}

# Line 288-292
try:
    all_metadata = json.loads(metadata_path.read_text())
    return all_metadata.get(artifact_name)
except:
    return None
```

**问题描述**:
- 使用裸 `except:` 捕获所有异常
- 静默忽略 JSON 解析错误
- 用户要求**严格禁止静默错误**

**影响级别**: 🔴 **严重** - 违反核心审查原则

**修复方案**:
```python
# 修复 Line 268-271
try:
    all_metadata = json.loads(metadata_path.read_text())
except json.JSONDecodeError as e:
    # 元数据损坏，记录警告但不阻塞操作
    import logging
    logging.warning(f"Corrupted metadata for {loop_ref}, resetting: {e}")
    all_metadata = {}
except Exception as e:
    # 其他错误应该上报
    raise CacheError(f"Failed to read metadata: {e}")

# 修复 Line 288-292
try:
    all_metadata = json.loads(metadata_path.read_text())
    return all_metadata.get(artifact_name)
except json.JSONDecodeError as e:
    import logging
    logging.warning(f"Corrupted metadata for {loop_ref}/{artifact_name}: {e}")
    return None
except Exception as e:
    raise CacheError(f"Failed to load metadata: {e}")
```

**验证方法**:
```bash
cd /f/Dev/AIstock
grep -n "except:" backend/services/hmm_data_source/cache_manager.py
# 应该只有具体的异常类型，没有裸 except:
```

---

### 问题 2: 访问生产表（潜在隔离违规）

**位置**: `backend/services/hmm_data_source/realtime_source.py:220`

**问题代码**:
```python
query = """
SELECT
    trade_date,
    symbol,
    score
FROM model_train_predictions
WHERE trade_date >= %(start_date)s
  AND trade_date <= %(end_date)s
ORDER BY trade_date, symbol
"""
```

**问题描述**:
- `model_train_predictions` 是 QE 系统的生产表
- 虽然是只读操作，但仍然违反了**完全隔离**的约束
- 设计文档明确要求："前期要求做到完全隔离，作为独立研究生产线"

**影响级别**: 🔴 **严重** - 违反隔离约束

**当前状态分析**:
这个查询是在 **RealtimeDataSource** 中，用于生产环境（Phase 2 风险监控）。但是：

1. **Phase 0-1 不会使用这个类**（只用 BacktestDataSource）
2. **Phase 2 实施时需要重新评估**数据源

**修复方案 A（推荐）- 延迟到 Phase 2**:
```python
# realtime_source.py:198-237
async def _query_predictions_from_db(
    self,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """
    从数据库查询预测分数
    
    NOTE: Phase 0-1 不使用此方法。
    Phase 2 实施时需要确定数据源：
    - 选项 1: 使用独立的预测表（hmm_evolution.predictions）
    - 选项 2: 通过 API 调用获取预测（不直接查询 QE 表）
    - 选项 3: 使用专门的数据服务
    
    当前实现仅为占位，Phase 2 时必须重新设计。
    """
    raise NotImplementedError(
        "RealtimeDataSource is not implemented in Phase 0-1. "
        "Will be redesigned in Phase 2 based on actual requirements."
    )
```

**修复方案 B（立即修复）- 移除生产表引用**:
```python
async def _query_predictions_from_db(
    self,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """
    从数据库查询预测分数
    
    Phase 2 实现时，应该从独立的预测表查询，
    例如 hmm_evolution.realtime_predictions
    """
    async with get_conn() as conn:
        async with conn.cursor() as cur:
            # 查询独立的预测表（需要在 Phase 2 创建）
            query = """
            SELECT
                trade_date,
                symbol,
                score
            FROM hmm_evolution.realtime_predictions
            WHERE trade_date >= %(start_date)s
              AND trade_date <= %(end_date)s
            ORDER BY trade_date, symbol
            """
            await cur.execute(query, {
                'start_date': start_date,
                'end_date': end_date,
            })
            rows = await cur.fetchall()
            
            if rows:
                df = pd.DataFrame(rows, columns=['trade_date', 'symbol', 'score'])
                return df
            else:
                return pd.DataFrame(columns=['trade_date', 'symbol', 'score'])
```

**推荐决策**: 
- **Phase 0 验收**: 采用**方案 A**（NotImplementedError），因为 Phase 0-1 根本不会用到这个方法
- **Phase 2 设计**: 在 Phase 2 架构设计时，由你决定是否需要 RealtimeDataSource，以及它应该从哪里获取数据

---

## ⚠️ 次要问题（建议修复）

### 问题 3: 缺少日志记录

**位置**: 所有源文件

**问题描述**:
- 代码中几乎没有日志记录
- 调试和监控会比较困难

**影响级别**: ⚠️ **中等**

**修复建议**:
```python
# 在每个关键操作前后添加日志
import logging
logger = logging.getLogger(__name__)

# 示例：backtest_source.py
async def get_predictions(self, start_date, end_date):
    logger.info(f"Fetching predictions for [{start_date}, {end_date}]")
    try:
        # ... 业务逻辑
        logger.info(f"Successfully fetched {len(df)} predictions")
        return df
    except Exception as e:
        logger.error(f"Failed to fetch predictions: {e}")
        raise
```

**优先级**: 低（可以在后续优化中添加）

---

### 问题 4: 交易日历计算未经验证

**位置**: `backend/services/hmm_data_source/backtest_source.py:493-534`

**问题描述**:
- `_get_nth_trading_day` 方法使用了真实交易日历（已修复之前的简化实现）
- 但没有单元测试验证其正确性

**影响级别**: ⚠️ **中等**

**验证建议**:
```python
# 添加到 test_backtest_source.py
@pytest.mark.asyncio
async def test_get_nth_trading_day():
    source = BacktestDataSource(base_loop_ref="test/Loop1")
    
    # 测试：2024-01-02 后的第 5 个交易日
    result = await source._get_nth_trading_day(date(2024, 1, 2), 5)
    
    # 应该跳过周末
    assert result > date(2024, 1, 2)
    
    # 测试：边界情况
    with pytest.raises(DataSourceError):
        # 未来太远，无交易日数据
        await source._get_nth_trading_day(date(2030, 1, 1), 100)
```

**优先级**: 中（建议在 Day 5 验收时补充）

---

## ✅ 通过的检查项

### 1. 语法检查 ✅
```bash
python -m py_compile backend/services/hmm_data_source/*.py
# 全部通过
```

### 2. 隔离约束检查 ✅

| 检查项 | 状态 | 说明 |
|--------|------|------|
| 不导入生产表模块 | ✅ | 无 `model_train_configs`, `strategy_packages` 等导入 |
| 不写入生产表 | ✅ | 无 UPDATE/DELETE/INSERT 到生产表 |
| 只下载 artifact 文件 | ✅ | 只下载 pred.pkl 和 label.pkl |
| 缓存目录隔离 | ✅ | 使用独立的 `tmp/hmm_evolution_cache/` |
| 只读 market.* 表 | ✅ | 只有 SELECT 查询 |
| 不调用 QE 配置 API | ✅ | 只调用 `download_artifact` |
| 不调用模拟盘 API | ✅ | 无 paper_v2 相关调用 |
| 无硬编码路径 | ✅ | 所有路径可配置 |
| 无硬编码凭证 | ✅ | 无密码、token 等 |

**验证命令**:
```bash
# 检查生产表引用
grep -rn "strategy_packages\|paper_v2\|model_train_configs" \
  backend/services/hmm_data_source/*.py
# 结果：无引用（除了 realtime_source.py 的 model_train_predictions）

# 检查写操作
grep -rn "UPDATE\|DELETE\|INSERT INTO" \
  backend/services/hmm_data_source/*.py
# 结果：无写操作

# 检查硬编码路径
grep -rn "/f/Dev/\|C:\\\|D:\\\|/home/" \
  backend/services/hmm_data_source/*.py
# 结果：无硬编码路径
```

### 3. 完整实现检查 ✅

| 功能模块 | 实现状态 | 说明 |
|----------|----------|------|
| HMMDataSourceInterface | ✅ 完整 | 5 个抽象方法定义清晰 |
| BacktestDataSource | ✅ 完整 | 所有方法实现，无 TODO |
| RealtimeDataSource | ⚠️ 部分 | Phase 0-1 不使用，Phase 2 重新设计 |
| ArtifactCacheManager | ✅ 完整 | 缓存管理功能完整 |
| 异常定义 | ✅ 完整 | 5 个专用异常类 |
| 数据模型 | ✅ 完整 | Pydantic 模型完整 |

**验证**:
- `base.py` 中的 `pass` 都是抽象方法占位符（符合预期）
- 所有具体实现类的方法都有完整逻辑
- 无 `TODO`、`FIXME`、`XXX` 等标记

### 4. 异常处理检查 ⚠️ (除了问题 1)

**良好的异常处理示例**:
```python
# backtest_source.py:169
except Exception as e:
    raise DataSourceError(f"Failed to load predictions: {e}")

# backtest_source.py:326
except Exception as e:
    last_error = e
    if attempt < max_retries - 1:
        wait_time = 2 ** attempt
        await asyncio.sleep(wait_time)
```

**统计**:
- 总计 20 个异常处理块
- 18 个正确处理（明确类型 + 上报）
- 2 个静默处理（cache_manager.py，已标记为问题 1）

### 5. 业务逻辑检查 ✅ (除了问题 2)

**核心逻辑验证**:

1. **BacktestDataSource.get_predictions()** ✅
   - 检查缓存 → 缓存未命中 → 下载 artifact → 保存缓存 → 反序列化 → 标准化格式
   - 日期范围验证
   - 数据格式标准化（支持 DataFrame 和 Dict 两种格式）

2. **BacktestDataSource.get_labels()** ✅
   - 与 get_predictions() 类似流程
   - **交易日历计算**：使用真实 `market.trade_cal`（已修复简化实现）
   - 计算 label_date（T + horizon_days 个交易日）

3. **BacktestDataSource.get_sector_mapping()** ✅
   - 查询 `market.sw_member` 和 `market.stock_basic`
   - 时间点查询（in_date <= trade_date <= out_date）
   - 只返回 L2 级别板块

4. **ArtifactCacheManager** ✅
   - SHA256 完整性校验
   - 元数据管理（metadata.json）
   - 缓存查询和清理

**符合设计文档**: ✅ 所有业务逻辑与详细设计文档一致

---

## 📋 检查清单总结

| 检查类别 | 通过项 | 失败项 | 状态 |
|----------|--------|--------|------|
| **禁止简化实现** | 5/6 | 1 (RealtimeDataSource) | ⚠️ |
| **禁止静默错误** | 18/20 | 2 (cache_manager.py) | 🔴 |
| **禁止业务逻辑偏移** | 全部 | 0 | ✅ |
| **禁止未经确认的审批** | 全部 | 0 | ✅ |
| **隔离约束** | 9/10 | 1 (realtime_source.py) | ⚠️ |
| **语法和导入** | 全部 | 0 | ✅ |

---

## 🎯 审查结论

### 总体评价: ⚠️ **有条件通过**

**优点**:
1. ✅ 代码结构清晰，接口设计合理
2. ✅ 类型注解完整，文档字符串详细
3. ✅ 核心业务逻辑正确（BacktestDataSource）
4. ✅ 隔离约束基本满足
5. ✅ 交易日历计算已使用真实数据（非简化）

**必须修复的问题**:
1. 🔴 **问题 1**: cache_manager.py 的静默异常处理（违反核心原则）
2. 🔴 **问题 2**: realtime_source.py 访问生产表（隔离违规）

**建议修复的问题**:
3. ⚠️ **问题 3**: 添加日志记录（可选，后续优化）
4. ⚠️ **问题 4**: 补充交易日历计算的单元测试（建议）

---

## 🚦 验收决策建议

### 方案 A: 立即修复后合入（推荐）

**步骤**:
1. 修复问题 1（cache_manager.py 异常处理）- **5 分钟**
2. 修复问题 2（realtime_source.py 改为 NotImplementedError）- **2 分钟**
3. 重新运行语法检查和隔离测试 - **2 分钟**
4. 合入主分支

**预计时间**: 10 分钟

**风险**: 极低（只是明确错误处理和延迟实现）

---

### 方案 B: 仅修复静默错误，保留 RealtimeDataSource 现状

**理由**:
- RealtimeDataSource 在 Phase 0-1 完全不会被调用
- 可以在 Phase 2 设计时再决定如何处理

**步骤**:
1. 修复问题 1（cache_manager.py）- **5 分钟**
2. 在 RealtimeDataSource 顶部添加注释说明 Phase 2 需要重新评估
3. 合入主分支

**风险**: 低（但留下了技术债务）

---

## 📝 修复清单

### 必修项（阻塞验收）

- [ ] 修复 `cache_manager.py:270` - 明确 JSON 异常处理
- [ ] 修复 `cache_manager.py:291` - 明确 JSON 异常处理
- [ ] 处理 `realtime_source.py:220` - 生产表引用（方案 A 或 B）
- [ ] 运行 `pytest tests/backend/services/hmm_data_source/test_isolation_constraints.py -v`
- [ ] 确认所有隔离测试通过

### 选修项（建议但不阻塞）

- [ ] 添加日志记录到关键操作
- [ ] 补充交易日历计算的单元测试
- [ ] 添加性能测试（验证 < 30s 首次加载）

---

## ✍️ 审查签字

**审查人**: Kiro (Claude Code)  
**审查日期**: 2026-07-16  
**审查结论**: ⚠️ **有条件通过** - 修复 2 个严重问题后可合入  
**推荐方案**: 方案 A（立即修复，10 分钟）

---

**下一步**: 等待你的决策：
1. 是否采用方案 A 或方案 B？
2. 是否需要我立即修复这些问题？
3. 还有其他需要审查的方面吗？
