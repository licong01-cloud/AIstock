# QE系统分析：数据完整性、API访问模式与阻塞问题

**分析日期**: 2026-03-04
**分析范围**: QE演进任务、组合任务、Agent阻塞、批量LLM分析

---

## 一、数据收集完整性分析

### 1.1 演进任务收集的数据

**数据来源**: `qe_evolution_service.py` 第154-254行 `_build_full_evolution_history()`

**已收集的数据**:
```python
- loop_index: Loop序号
- action_type: 演进动作类型（factor_add/model_tune等）
- config_json: 完整配置（factor_list, model_id, model_params, data_split）
- metrics_json: 回测指标（IC, ICIR, Rank IC, annualized_return, max_drawdown, information_ratio）
- agent_analysis: 4个Agent的完整分析结果
  - analyst: 诊断报告
  - evaluator: SOTA判断 + 历史SOTA指标
  - researcher: 配置草案 + action_type
  - reviewer: 审核结果 + 验证后配置
- is_sota: SOTA标记
```

**衍生统计数据**:
```python
- IC趋势: ic_trend数组
- 最佳IC: best_ic
- action_type统计: {action_type: {count, avg_ic_delta}}
- 失败方法记录: [{loop_index, action_type, ic_delta}]
- SOTA Loop索引和指标
- 相对上一轮的delta: {IC: delta_value}
```

### 1.2 数据完整性评估

**✅ 充分的数据**:
1. **指标数据**: 6个核心指标完整（IC/ICIR/Rank IC/年化收益/最大回撤/信息比率）
2. **配置数据**: 因子列表、模型ID、超参数、数据切分完整
3. **演进历史**: 全量历史（不再LIMIT 3），支持全局最优决策
4. **趋势分析**: IC趋势、action_type效果统计、失败方法记录

**❌ 缺失的数据**:
1. **训练过程数据**: 无训练loss曲线、收敛速度、early stop epoch
2. **因子贡献度**: 无单因子在组合中的边际贡献分析
3. **模型复杂度**: 无参数量、训练时间、推理速度
4. **数据质量**: 无缺失率、异常值比例、特征相关性矩阵
5. **回测细节**: 无逐日收益曲线、分年度表现、行业分布

**⚠️ 潜在问题**:
- **Researcher Agent** 可能需要训练收敛信息来判断是否需要调整学习率/batch_size
- **Evaluator Agent** 缺少稳定性指标（如分年度IC标准差）来判断SOTA的鲁棒性
- **Analyst Agent** 无法诊断训练失败的根因（过拟合/欠拟合/数据问题）

---

## 二、API访问模式分析

### 2.1 QE Workspace Client（RDAgent交互）

**文件**: `qe_workspace_client.py`

**访问模式**: ✅ **完全通过API**

```python
- create_and_run_loop(): POST {base_url}/tasks/{task_id}/loops
- get_loop_status(): GET {base_url}/tasks/{task_id}/loops/{loop_id}/status
- get_loop_metrics(): GET {base_url}/tasks/{task_id}/loops/{loop_id}/metrics
- stream_task_logs(): GET {base_url}/tasks/{task_id}/logs (SSE)
- download_loop_assets(): GET {base_url}/tasks/{task_id}/loops/{loop_id}/assets/download
- cleanup_task_workspace(): DELETE {base_url}/tasks/{task_id}
```

**结论**: ✅ **无直接文件系统访问**，所有RDAgent侧数据通过HTTP API获取

### 2.2 演进历史构建（数据库访问）

**文件**: `qe_evolution_service.py` 第154-254行

**访问模式**: ✅ **完全通过数据库**

```sql
SELECT loop_index, action_type, config_json, metrics_json, agent_analysis, is_sota
FROM qe_evolution_loops
WHERE task_id = %s AND status = 'completed'
ORDER BY loop_index ASC
```

**结论**: ✅ **无文件系统访问**，历史数据从PostgreSQL读取

### 2.3 因子/模型数据访问

**文件**: `factor_analyst.py`, `model_analyst.py`

**访问模式**: ✅ **完全通过数据库**

```sql
- aistock_factor_catalog: 因子元数据
- aistock_factor_metrics: 独立评测指标
- qe_factor_classification: 分类评级结果
- qe_model_catalog: 模型元数据
```

**结论**: ✅ **无文件系统访问**，所有因子/模型数据从数据库读取

### 2.4 总结

**✅ 系统设计符合API优先原则**:
- RDAgent侧数据: 100% API访问
- AIstock侧数据: 100% 数据库访问
- 无直接文件系统读取（除资产下载后的本地解压）

---

## 三、Agent阻塞问题分析（核心问题）

### 3.1 因子分析Agent - **严重阻塞风险** ⚠️

**问题代码**: `factor_analyst.py` 第784-825行

```python
def batch_analyze_all_factors(self, use_llm: bool = False, ...):
    """批量分析因子 - 同步函数"""
    factors = self._get_all_factors(source_filter)
    for f in factors:  # 🔴 同步循环
        try:
            self.analyze_single_factor(  # 🔴 同步LLM调用
                factor_name=f["factor_name"],
                factor_source=f["source"],
                use_llm=use_llm,
            )
            analyzed += 1
        except Exception as e:
            errors.append(f"{f['factor_name']}: {e}")
```

**LLM调用**: `factor_analyst.py` 第537行

```python
response = llm.completion(  # 🔴 同步阻塞调用
    messages=[...],
    temperature=0.2,
    max_tokens=1500,
    response_format={"type": "json_object"},
    **kwargs
)
```

**API路由**: `quantevolver.py` 第706-721行

```python
@router.post("/factor-analyst/batch-analyze")
async def batch_analyze_factors(req: BatchAnalyzeRequest):
    fa = FactorAnalyst()
    result = await asyncio.to_thread(  # ⚠️ 在线程池中运行
        fa.batch_analyze_all_factors,
        use_llm=req.use_llm,
        source_filter=req.source_filter,
        factor_names=req.factor_names,
    )
    return result
```

**阻塞分析**:

| 场景 | 因子数量 | 单次LLM耗时 | 总耗时 | 阻塞影响 |
|------|---------|------------|--------|---------|
| Alpha158 | 158 | 3-5秒 | **8-13分钟** | ⚠️ 中等 |
| Alpha360 | 360 | 3-5秒 | **18-30分钟** | 🔴 严重 |
| 全部因子 | 1000+ | 3-5秒 | **50-83分钟** | 🔴 极严重 |

**问题根源**:
1. ❌ `batch_analyze_all_factors` 是**同步函数**，在for循环中逐个调用LLM
2. ❌ `llm.completion()` 是**同步阻塞调用**，无async包装
3. ⚠️ `asyncio.to_thread()` 将任务放入线程池，但**仍占用一个线程**长达数十分钟
4. ❌ **无后台任务机制**，API请求必须等待全部完成才返回

**对其他请求的影响**:
- ✅ **不会阻塞其他API请求**（FastAPI使用线程池隔离）
- ⚠️ **会占用一个线程池线程**（默认线程池大小40，长时间占用会耗尽）
- ❌ **用户体验极差**（前端请求超时，无进度反馈）

### 3.2 演进任务Agent - **长时间占用但不阻塞** ⚠️

**问题代码**: `qe_evolution_service.py` 第256-511行

```python
async def start_task_loop(self, task_id: str):
    """异步后台执行状态机 - 但实际是长时间运行的单个async任务"""
    while current_loop < max_loops:  # 🔴 可能运行数小时
        # 1. 组装配置
        # 2. 调用RDAgent执行（轮询等待）
        while True:  # 🔴 轮询循环，2秒sleep
            status_resp = await self.workspace_client.get_loop_status(...)
            if rd_status == "completed":
                break
            await asyncio.sleep(2)  # ⚠️ 至少会yield控制权

        # 3. 获取指标
        metrics = await self.workspace_client.get_loop_metrics(...)

        # 4. 运行4个Agent（串行）
        analyst_report = await self.agents.run_analyst(...)  # 🔴 LLM调用
        is_sota = await self.agents.run_evaluator(...)       # 🔴 LLM调用
        next_config_draft = await self.agents.run_researcher(...)  # 🔴 LLM调用
        next_config = await self.agents.run_reviewer(...)    # 🔴 LLM调用

        # 5. 更新数据库
        current_loop += 1
```

**Agent LLM调用**: `qe_evolution_agents.py` 第42-44行

```python
async def async_call_llm(self, agent_type: str, system_prompt: str, user_prompt: str):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, self._call_llm, ...)  # ✅ 使用executor包装
```

**阻塞分析**:

| 阶段 | 耗时 | 是否阻塞其他请求 |
|------|------|----------------|
| RDAgent训练 | 10-60分钟/Loop | ✅ 不阻塞（轮询+sleep） |
| Analyst Agent | 5-10秒 | ✅ 不阻塞（run_in_executor） |
| Evaluator Agent | 3-5秒 | ✅ 不阻塞（run_in_executor） |
| Researcher Agent | 5-10秒 | ✅ 不阻塞（run_in_executor） |
| Reviewer Agent | 3-5秒 | ✅ 不阻塞（run_in_executor） |
| **单Loop总计** | **10-60分钟** | ✅ 不阻塞 |
| **5 Loops总计** | **1-5小时** | ✅ 不阻塞 |

**结论**:
- ✅ **不会阻塞其他API请求**（所有LLM调用都用run_in_executor包装）
- ✅ **不会阻塞事件循环**（轮询时使用await asyncio.sleep(2)）
- ⚠️ **长时间占用一个async任务**（但不影响并发处理能力）
- ⚠️ **会占用线程池线程**（4个Agent调用，每次占用1个线程5-10秒）

### 3.3 模型分析Agent - **与因子分析相同问题** ⚠️

**问题代码**: `model_analyst.py` 第205行

```python
def batch_analyze_all_models(self, use_llm: bool = False):
    """批量分析模型 - 同步函数"""
    models = self._get_all_models()
    for m in models:  # 🔴 同步循环
        self.analyze_single_model(...)  # 🔴 同步LLM调用
```

**阻塞分析**: 与因子分析完全相同，模型数量通常较少（10-50个），影响相对较小

---

## 四、批量LLM分析对AIstock页面的影响

### 4.1 技术架构

**FastAPI异步模型**:
```
用户请求 → Uvicorn → FastAPI → 路由处理器
                                    ↓
                        async def handler():
                            await asyncio.to_thread(sync_func)
                                    ↓
                            ThreadPoolExecutor (默认40线程)
```

### 4.2 影响分析

**场景1: 批量因子分析进行中（1000个因子，耗时60分钟）**

| 其他请求类型 | 是否被阻塞 | 原因 |
|------------|----------|------|
| 查看因子列表 | ✅ 不阻塞 | 独立数据库查询，不依赖线程池 |
| 查看实验详情 | ✅ 不阻塞 | 独立数据库查询 |
| 创建新实验 | ✅ 不阻塞 | 独立async任务 |
| 启动演进任务 | ✅ 不阻塞 | 独立async任务 |
| **再次触发批量分析** | ⚠️ **可能阻塞** | 如果线程池已满（40个长任务） |
| 其他CPU密集操作 | ⚠️ **可能变慢** | 线程池资源竞争 |

**场景2: 多个批量分析并发（5个用户同时触发）**

```
线程池状态:
- 总容量: 40线程
- 已占用: 5线程（每个批量分析占1个，持续60分钟）
- 剩余: 35线程
- 风险: ⚠️ 中等（仍有余量，但长时间占用不健康）
```

### 4.3 实际影响评估

**✅ 正常情况（单用户偶尔触发）**:
- 其他页面访问: **完全不受影响**
- 数据库查询: **完全不受影响**
- 其他API调用: **完全不受影响**

**⚠️ 异常情况（多用户频繁触发或恶意攻击）**:
- 线程池耗尽: **新的CPU密集任务会排队等待**
- 数据库连接池: **可能因长时间占用而耗尽**（批量分析会频繁查询DB）
- 内存占用: **LLM调用会占用内存，1000次调用可能累积数GB**

---

## 五、问题总结与建议

### 5.1 数据完整性问题

**问题**: 缺少训练过程数据、因子贡献度、模型复杂度等

**建议**:
1. **Phase 0优先**: 在因子分析Agent中补充多持有期IC、多窗口稳定性（已在Phase 0计划中）
2. **Phase 2扩展**: 在`get_loop_metrics` API中返回训练loss曲线、early stop epoch
3. **Phase 3增强**: 在Researcher Agent中增加"训练诊断"提示词，利用训练数据判断超参数调整方向

### 5.2 阻塞问题（优先级P0）

**问题**: 批量因子/模型分析是同步阻塞函数，长时间占用线程池

**建议**:

#### 方案A: 后台任务 + 进度追踪（推荐）

```python
# 1. 引入后台任务表
CREATE TABLE qe_batch_analysis_tasks (
    task_id UUID PRIMARY KEY,
    task_type VARCHAR(50),  -- 'factor_batch' / 'model_batch'
    status VARCHAR(20),     -- 'pending' / 'running' / 'completed' / 'failed'
    total_count INT,
    processed_count INT,
    error_count INT,
    created_at TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP
);

# 2. 修改API路由
@router.post("/factor-analyst/batch-analyze")
async def batch_analyze_factors(req: BatchAnalyzeRequest, background_tasks: BackgroundTasks):
    task_id = str(uuid.uuid4())
    # 创建任务记录
    _create_batch_task(task_id, 'factor_batch', total_count)
    # 后台执行
    background_tasks.add_task(_run_batch_analysis_background, task_id, req)
    return {"task_id": task_id, "status": "pending"}

# 3. 新增进度查询API
@router.get("/factor-analyst/batch-analyze/{task_id}/progress")
async def get_batch_progress(task_id: str):
    return _get_task_progress(task_id)
```

**优点**:
- ✅ API立即返回，不阻塞用户
- ✅ 前端可轮询进度，显示进度条
- ✅ 任务失败可重试
- ✅ 支持取消任务

#### 方案B: 异步并发 + 限流（性能优化）

```python
async def batch_analyze_all_factors_async(self, use_llm: bool = False, ...):
    """异步批量分析 - 并发控制"""
    factors = self._get_all_factors(source_filter)
    semaphore = asyncio.Semaphore(5)  # 最多5个并发LLM调用

    async def analyze_one(f):
        async with semaphore:
            return await asyncio.to_thread(
                self.analyze_single_factor,
                factor_name=f["factor_name"],
                factor_source=f["source"],
                use_llm=use_llm,
            )

    results = await asyncio.gather(*[analyze_one(f) for f in factors], return_exceptions=True)
    return _summarize_results(results)
```

**优点**:
- ✅ 5倍速度提升（5个并发）
- ✅ 仍使用asyncio，不额外占用线程
- ✅ 限流避免LLM API过载

**缺点**:
- ⚠️ 仍需等待全部完成（建议结合方案A）

### 5.3 推荐实施方案

**Phase 0（立即实施）**:
1. ✅ 实施方案A：后台任务 + 进度追踪
2. ✅ 添加批量分析任务表和进度API
3. ✅ 前端改造：提交后跳转到进度页面

**Phase 1（性能优化）**:
1. ✅ 实施方案B：异步并发（5个并发）
2. ✅ 添加任务取消功能
3. ✅ 添加失败重试机制

**Phase 2（监控告警）**:
1. ✅ 监控线程池使用率
2. ✅ 监控LLM API调用频率
3. ✅ 添加并发限制（同时最多3个批量任务）

---

## 六、结论

### 6.1 数据完整性

**评分**: ⭐⭐⭐⭐☆ (4/5)

- ✅ 核心指标完整
- ✅ 演进历史完整
- ❌ 缺少训练过程数据
- ❌ 缺少因子贡献度分析

### 6.2 API访问模式

**评分**: ⭐⭐⭐⭐⭐ (5/5)

- ✅ 100% API访问（RDAgent侧）
- ✅ 100% 数据库访问（AIstock侧）
- ✅ 无直接文件系统访问

### 6.3 阻塞问题

**评分**: ⭐⭐☆☆☆ (2/5)

- 🔴 批量因子分析：严重阻塞（60分钟占用1线程）
- 🔴 批量模型分析：中等阻塞（10分钟占用1线程）
- ✅ 演进任务Agent：不阻塞（正确使用async）
- ⚠️ 线程池资源管理：缺少并发限制

### 6.4 对AIstock页面的影响

**评分**: ⭐⭐⭐⭐☆ (4/5)

- ✅ 正常情况：完全不影响其他页面
- ⚠️ 异常情况：多用户并发可能耗尽线程池
- ❌ 用户体验：批量分析无进度反馈

---

**最终建议**: 立即实施后台任务机制（方案A），解决批量分析的阻塞和用户体验问题。
