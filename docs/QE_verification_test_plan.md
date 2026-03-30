# QE 模块全量验证测试流程

> 版本: v1.0 | 日期: 2026-03-06
> 覆盖范围: 基础数据补齐 → 因子/模型独立指标 → LLM 分析 → QE 多入口演进 → 端到端验证
> 前置: QE_pending_work_checklist.md 中 A~E 所有代码开发完成

---

## 测试流程总览

```
Phase 1: 环境准备与 DDL 迁移
    ↓
Phase 2: 基础数据补齐与验证
    ├─ 2A: Loop 指标映射补齐
    ├─ 2B: Loop 增强诊断回填
    └─ 2C: 模型训练数据补齐
    ↓
Phase 3: 因子独立指标计算
    ├─ 3A: Uvicorn 超时修复验证
    ├─ 3B: 引擎内部并行验证
    ├─ 3C: API 并行优化验证
    └─ 3D: 全量因子指标计算
    ↓
Phase 4: LLM 分析与分类
    ├─ 4A: 因子 LLM 批量重分析
    ├─ 4B: 因子分类/评级/双输出验证
    ├─ 4C: 模型 LLM 批量重分析
    └─ 4D: 模型等级/双输出验证
    ↓
Phase 5: QE 演进功能验证
    ├─ 5A: 演进架构（定时扫描 + webhook）
    ├─ 5B: 因子选择三层漏斗
    ├─ 5C: EvolutionFactorAgent 7 步编排
    ├─ 5D: AI 配置生成（smart-select）
    └─ 5E: 多入口演进体系
    ↓
Phase 6: 端到端集成验证
    ├─ 6A: 入口 A — 手动创建演进实验
    ├─ 6B: 入口 B — 从因子分析触发演进
    ├─ 6C: 入口 C — 从模型选择触发演进
    └─ 6D: 入口 D — RDAgent Task → 演进
    ↓
Phase 7: 性能与稳定性验证
```

---

## Phase 1: 环境准备与 DDL 迁移

### 1.1 数据库 DDL 迁移

**操作**：
```bash
cd F:\Dev\AIstock\backend
python -m db.init_quant_schema
```

**验证**：
```sql
-- 确认训练诊断列已创建
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'aistock_model_catalog'
  AND column_name IN (
    'best_epoch', 'total_epochs', 'convergence_ratio', 'overfit_ratio',
    'training_failed', 'train_loss_final', 'val_loss_final', 'training_curves',
    'analysis_profile', 'model_grade', 'grade_reason', 'training_quality_score'
  )
ORDER BY column_name;
```

**预期**: 12 行，每列的 data_type 匹配设计（integer/double precision/boolean/jsonb/varchar/text）

- [ ] 通过

### 1.2 服务启动验证

**操作**：
1. 重启 RDAgent API 服务（确认 `timeout_keep_alive=300` 生效）
2. 重启 AIStock 后端

**验证**：
```bash
# RDAgent 端
curl http://localhost:9000/docs  # Swagger 可访问

# AIStock 端
curl http://localhost:8001/api/v1/rdagent/sync/status  # 返回 JSON
```

- [ ] 通过

---

## Phase 2: 基础数据补齐与验证

### 2A: Loop 指标映射补齐

**操作**：
```bash
psql -f F:\Dev\AIstock\backend\scripts\backfill_loop_metrics_mapping.sql
```

**验证**：
```sql
-- 检查映射后的短键是否有值
SELECT loop_id,
       metrics_json->>'sharpe' AS sharpe,
       metrics_json->>'annualized_return' AS annualized_return,
       metrics_json->>'Rank_IC' AS rank_ic
FROM qe_evolution_loops
WHERE status = 'completed'
ORDER BY created_at DESC
LIMIT 5;
```

**预期**: sharpe/annualized_return/Rank_IC 列均有数值，不为 NULL

- [ ] 通过

**前端验证**：
- [ ] 打开演进页面 → 已完成 Loop 的 Sharpe 卡片显示数值 + delta
- [ ] SOTA 阶梯图 Y 轴有数值
- [ ] Trajectory 轨迹图数据点完整
- [ ] Leaderboard 排行有 Sharpe/年化列

### 2B: Loop 增强诊断回填

**操作**：
```bash
cd F:\Dev\AIstock\backend
python scripts/backfill_loop_enhanced_metrics.py --dry-run    # 先预检
python scripts/backfill_loop_enhanced_metrics.py               # 执行回填
```

**验证**：
```sql
-- 检查 enhanced_metrics 非空
SELECT loop_id,
       (metrics_json->'enhanced_metrics') IS NOT NULL AS has_enhanced,
       jsonb_object_keys(metrics_json->'enhanced_metrics') AS sections
FROM qe_evolution_loops
WHERE status = 'completed'
LIMIT 5;
```

**前端验证**（5 个增强 Tab）：
- [ ] IC 诊断 Tab → 时间序列折线图（IC 值随日期变化）
- [ ] 训练过程 Tab → Loss 曲线图（train/val loss vs epoch）
- [ ] 收益曲线 Tab → 累计收益 + 回撤面积图
- [ ] 交易效率 Tab → 3 个数值卡片（换手率/交易成本/滑点）
- [ ] 预测行为 Tab → 4 个数值卡片（预测分布/偏度/峰度/覆盖率）

### 2C: 模型训练数据补齐

**操作**：
```bash
cd F:\Dev\AIstock\backend
python scripts/backfill_model_training_data.py --dry-run      # 预检
python scripts/backfill_model_training_data.py                  # 执行
```

**验证**：
```sql
SELECT model_id, best_epoch, total_epochs,
       convergence_ratio, overfit_ratio,
       train_loss_final, val_loss_final,
       training_failed
FROM aistock_model_catalog
WHERE is_sota = TRUE
ORDER BY created_at DESC
LIMIT 10;
```

**预期**: SOTA 模型的 best_epoch 等训练字段有值（部分模型可能无训练诊断数据则为 NULL）

- [ ] 通过

---

## Phase 3: 因子独立指标计算

### 3A: Uvicorn 超时修复验证

**操作**：调用已知的 23 因子 Task 的指标计算 API

```bash
# 之前 504 超时的 Task
curl -s http://localhost:9000/api/extractors/sota_factors/v2/2026-02-02_14-58-13-793902/factor_metrics | python -m json.tool | head -20
```

**预期**: 返回 200 OK，包含 23 个因子的指标数据（耗时 < 30s）

- [ ] 通过（之前为 504 超时）

### 3B: 引擎内部并行验证

**操作**：在 RDAgent 侧查看日志确认并行执行

```bash
# 观察日志输出
# 应看到: "Using ThreadPoolExecutor(max_workers=4) for XX factors"
```

**验证**：
- [ ] 日志中出现 ThreadPoolExecutor 并行日志
- [ ] 23 因子 Task 耗时从 60-90s 降至 20-30s

### 3C: API 并行优化验证

**操作**：调用批量端点

```bash
curl -X POST http://localhost:9000/api/extractors/sota_factors/v2/batch/factor_metrics \
  -H "Content-Type: application/json" \
  -d '{"task_ids": ["2026-02-02_14-58-13-793902", "2026-02-02_12-00-00-000000"]}'
```

**预期**: 两个 Task 并行计算，总耗时 ≈ 单个最大耗时（而非两倍）

- [ ] 通过

### 3D: 全量因子指标计算

**操作**（SSE 流式端点）：

```bash
curl -X POST http://localhost:8001/api/v1/rdagent/sync/factor_metrics/sync-stream \
  -H "Content-Type: application/json" \
  -d '{"from_catalog": true, "concurrency": 4}'
```

**验证**：
```sql
-- 覆盖率检查
SELECT COUNT(DISTINCT factor_name) AS total_factors
FROM aistock_factor_metrics
WHERE eval_window = 'full';

-- 对比因子总数
SELECT COUNT(DISTINCT factor_name) AS catalog_factors
FROM aistock_factor_catalog
WHERE source_task_id IS NOT NULL;
```

**预期**: `total_factors` ≈ `catalog_factors`（覆盖率 > 90%）

- [ ] 通过
- [ ] SSE 流式进度正常推送

---

## Phase 4: LLM 分析与分类

### 4A: 因子 LLM 批量重分析

**操作**（通过前端或 API）：

```bash
# API 方式
POST /api/v1/quantevolver/factor-analyst/batch-analyze-stream
Content-Type: application/json
{"use_llm": true}

# 或前端: 因子管理页 → 点击"批量分析"
```

**预期**: SSE 流式推送进度，100+ 因子约 12 分钟完成

- [ ] SSE 进度条正常显示
- [ ] 无 beforeunload 中断

### 4B: 因子分类/评级/双输出验证

```sql
-- 分类验证：时序类因子应标记为 time_series
SELECT factor_name, factor_dimension
FROM qe_factor_classification
WHERE factor_dimension = 'time_series'
ORDER BY factor_name
LIMIT 20;
```

**预期**: 包含 MA5, MA10, STD20, ROC10, EMA5 等含时间窗口的因子

```sql
-- 评级分布验证
SELECT grade, COUNT(*) as cnt
FROM qe_factor_classification
GROUP BY grade
ORDER BY grade;
```

**预期**: S < A < B < C（正态分布，不应大量集中在某一等级）

```sql
-- 双输出验证（description + factor_profile）
SELECT factor_name, description, factor_profile
FROM qe_factor_classification
WHERE factor_profile IS NOT NULL
LIMIT 5;
```

**预期**: description 有文本，factor_profile 有 JSONB 数据

```sql
-- 双重校验验证：规则置信度 >= 2 以规则为准
SELECT factor_name, factor_dimension, classification_reason
FROM qe_factor_classification
WHERE classification_reason LIKE '%rule%'
LIMIT 5;
```

**验证清单**：
- [ ] 时序因子分类正确（MA/STD/ROC/EMA 类）
- [ ] 截面因子分类正确（PE/PB/MKT_CAP 类）
- [ ] 评级分布合理
- [ ] factor_profile JSONB 非空
- [ ] 8 维度评级（非旧版 5 维度）
- [ ] 双重校验机制生效

### 4C: 模型 LLM 批量重分析

**操作**（通过 API 或前端）：

```bash
POST /api/v1/quantevolver/model-analyst/batch-analyze-stream
Content-Type: application/json
{"use_llm": true}
```

**预期**: SSE 流式推送，所有 SOTA 模型完成分析

- [ ] 通过

### 4D: 模型等级/双输出验证

```sql
-- 双输出验证
SELECT model_id, model_grade, training_quality_score,
       description IS NOT NULL AS has_desc,
       analysis_profile IS NOT NULL AS has_profile
FROM aistock_model_catalog
WHERE is_sota = TRUE
ORDER BY training_quality_score DESC NULLS LAST
LIMIT 10;
```

**预期**: model_grade 有值(S/A/B/C/D)，analysis_profile 非空

```sql
-- 等级分布
SELECT model_grade, COUNT(*) as cnt
FROM aistock_model_catalog
WHERE model_grade IS NOT NULL
GROUP BY model_grade
ORDER BY model_grade;
```

**预期**: 等级分布合理

```sql
-- analysis_profile 内容抽样
SELECT model_id,
       analysis_profile->>'model_grade' AS grade,
       analysis_profile->>'training_quality_score' AS tq_score,
       analysis_profile->>'ic' AS ic,
       analysis_profile->>'convergence_ratio' AS convergence
FROM aistock_model_catalog
WHERE analysis_profile IS NOT NULL
LIMIT 5;
```

**验证清单**：
- [ ] model_grade 有值
- [ ] description 有文本（100-300 字）
- [ ] analysis_profile JSONB 包含完整结构化数据
- [ ] training_quality_score 范围 0-100
- [ ] training_failed=TRUE 的模型 grade 为 D
- [ ] best_epoch=0 的模型 score 低于正常模型

---

## Phase 5: QE 演进功能验证

### 5A: 演进架构验证

**5A.1 定时扫描器**：
```bash
# 查看 AIStock 日志，确认 60s 定时扫描在运行
grep "evolution_scan" /path/to/aistock/logs/latest.log | tail -5
```

- [ ] 定时扫描器每 60s 执行一次
- [ ] 扫描器检测到已完成 Loop 并触发 process_completed_loop

**5A.2 热重载稳定性**：
```bash
# 触发 uvicorn reload（修改任意 .py 文件保存）
# 观察是否挂起
```

- [ ] 热重载不挂起，定时扫描器正常恢复

### 5B: 因子选择三层漏斗

```bash
# 测试 prefilter（SQL 层）
curl -s http://localhost:8001/api/v1/quantevolver/factors/prefilter \
  -X POST -H "Content-Type: application/json" \
  -d '{"min_ic": 0.03, "max_count": 50}'
```

**预期**: 返回 IC > 0.03 的因子列表

- [ ] prefilter 正确过滤
- [ ] screening（LLM 二筛）正确缩减候选
- [ ] deep_select（LLM 深选）给出最终推荐 + 理由

### 5C: EvolutionFactorAgent 7 步编排

**验证**: 创建一个演进实验，观察 Agent 日志中的 7 步执行：

```
Step 1: 获取当前实验状态  ← Tool
Step 2: 分析历史 Loop 趋势  ← LLM
Step 3: 获取因子池数据  ← Tool
Step 4: 筛选候选因子  ← LLM
Step 5: 获取模型配置模板  ← Tool
Step 6: 生成实验配置  ← LLM
Step 7: 提交配置  ← Tool
```

- [ ] 7 步编排完整执行
- [ ] Tool-LLM 交替模式正确
- [ ] 生成的配置包含因子列表 + 模型超参

### 5D: AI 配置生成（smart-select）

**验证**: smart-select prompt 是否注入了增强数据

- [ ] prompt 包含多持有期 IC 数据
- [ ] prompt 包含因子分类信息
- [ ] prompt 包含模型训练诊断
- [ ] 生成的配置合理（因子数量 20-80，模型超参在合理范围）

### 5E: 多入口演进体系

**5E.1 演进目标/指引输入**：
- [ ] 创建演进对话框要求输入演进目标
- [ ] 目标文本注入 Agent 提示词

**5E.2 Compose 模式**：
- [ ] 前端创建对话框 compose 模式可用
- [ ] 因子选择 + 模型选择 + 目标输入联动

---

## Phase 6: 端到端集成验证

### 6A: 入口 A — 手动创建演进实验

**操作**：
1. 前端 → QE 演进页 → 点击"创建演进实验"
2. 输入演进目标："提升模型 Sharpe 至 2.0 以上"
3. 选择初始因子集 + 模型

**验证**：
- [ ] 实验创建成功，跳转到实验详情页
- [ ] Loop 0 自动提交并开始执行
- [ ] 实验 ID 格式为 `qe_YYYYMMDD_HHMMSS`
- [ ] 配置区域显示结构化信息（action badge + 因子列表 + 模型信息）
- [ ] Loop 完成后核心指标有值
- [ ] Loop 完成后 5 个增强 Tab 有数据
- [ ] 定时扫描 60s 内检测到完成并触发下一轮

### 6B: 入口 B — 从因子分析触发演进

**操作**：
1. 因子管理页 → 选择高评级因子
2. 点击"以这些因子开始演进"

**验证**：
- [ ] 预选因子自动填充到创建对话框
- [ ] 实验配置中包含选中的因子

### 6C: 入口 C — 从模型选择触发演进

**操作**：
1. 模型管理页 → 选择 SOTA 模型
2. 点击"以此模型开始演进"

**验证**：
- [ ] 预选模型自动填充
- [ ] 实验配置中包含选中模型的超参

### 6D: 入口 D — RDAgent Task → 真实实验 → 演进

**操作**：
1. 同步一个 RDAgent Task
2. 查看 Task 的 SOTA 因子和模型
3. 以 Task 的 SOTA 资产开始演进

**验证**：
- [ ] Task 同步时训练诊断数据已写入
- [ ] SOTA 因子有独立指标
- [ ] SOTA 模型有分析 profile
- [ ] 演进实验可从 Task 资产开始

---

## Phase 7: 性能与稳定性验证

### 7A: 因子指标计算性能

| 指标 | 当前基线 | 目标 | 实际值 |
|------|---------|------|--------|
| 单 Task 23 因子计算 | 60-90s (504) | < 30s | ___s |
| 50 Task 全量计算 | ~150 min | < 45 min | ___min |
| SSE 流式响应延迟 | N/A | < 2s | ___s |

- [ ] 单 Task 不超时
- [ ] 全量计算在合理时间内完成
- [ ] SSE 实时推送无断流

### 7B: 因子 LLM 分析性能

| 指标 | 目标 | 实际值 |
|------|------|--------|
| 100 因子批量分析 | < 15 min | ___min |
| 单因子 LLM 调用 | < 8000 tokens | ___tokens |
| SSE 进度推送 | 无断流 | _____ |

- [ ] Token 预算 < 8000/次
- [ ] 5x 加速（对比旧版串行）

### 7C: 定时扫描器稳定性

- [ ] 连续运行 24h 无内存泄漏
- [ ] 热重载后 Scanner 自动恢复
- [ ] Loop 完成检测延迟 < 60s

### 7D: 前端交互稳定性

- [ ] beforeunload 警告正常（批量分析中关闭页面）
- [ ] 轮询 10s/15s 不影响页面性能
- [ ] 长时间挂机页面不崩溃

---

## 验证结果汇总

| Phase | 子项 | 通过 | 总数 | 通过率 |
|-------|------|------|------|--------|
| 1. 环境准备 | | /2 | 2 | |
| 2. 数据补齐 | | /12 | 12 | |
| 3. 因子指标 | | /6 | 6 | |
| 4. LLM 分析 | | /12 | 12 | |
| 5. QE 演进 | | /10 | 10 | |
| 6. 端到端 | | /12 | 12 | |
| 7. 性能稳定 | | /8 | 8 | |
| **合计** | | **/62** | **62** | |

---

## 附录: 快速命令参考

```bash
# DDL 迁移
cd F:\Dev\AIstock\backend && python -m db.init_quant_schema

# A1: Loop 指标映射
psql -f backend/scripts/backfill_loop_metrics_mapping.sql

# A2: Loop 增强诊断回填
python backend/scripts/backfill_loop_enhanced_metrics.py

# C3: 模型训练数据回填
python backend/scripts/backfill_model_training_data.py

# B5/3D: 全量因子指标计算（SSE）
curl -X POST http://localhost:8001/api/v1/rdagent/sync/factor_metrics/sync-stream \
  -H "Content-Type: application/json" -d '{"from_catalog": true, "concurrency": 4}'

# D1: 因子 LLM 批量重分析
# 通过前端触发或 API POST /api/v1/quantevolver/factor-analyst/batch-analyze-stream

# E3: 模型 LLM 批量重分析
# 通过前端触发或 API POST /api/v1/quantevolver/model-analyst/batch-analyze-stream
```

---

## 附录: 修改文件清单

| 文件 | 变更类型 | 任务编号 |
|------|---------|---------|
| `backend/scripts/backfill_loop_metrics_mapping.sql` | 新建 | A1 |
| `backend/scripts/backfill_loop_enhanced_metrics.py` | 新建 | A2 |
| `backend/scripts/backfill_model_training_data.py` | 新建 | C3 |
| `RD-Agent-main/debug_tools/start_api.py` | 修改 | B1 |
| `RD-Agent-main/rdagent/app/factor_metrics/engine.py` | 修改 | B2 |
| `backend/services/rdagent_factor_metrics_sync.py` | 修改 | B3 |
| `backend/routers/rdagent_sync_admin.py` | 修改 | B3 |
| `RD-Agent-main/rdagent/app/api_endpoints/sota_factors_api.py` | 修改 | B4 |
| `backend/db/init_quant_schema.py` | 修改 | C1+E1 |
| `backend/services/rdagent_model_catalog_sync.py` | 修改 | C2 |
| `backend/services/quantevolver/model_analyst.py` | 重写 | C4+E2 |
