# QE 实验模块整改设计方案 v2

> 版本: v2.0
> 日期: 2026-03-02
> 基于: QE实验模块整改方案.md (v1 bug修复) + QE_Analysis_and_Design_v1.md (整体设计)
> 范围: 实验ID改造、统一workspace结构、实验删除功能、命名清理、bug修复、隔离性保障

---

## 目录

1. [核心改进总览](#1-核心改进总览)
2. [实验ID格式改造](#2-实验id格式改造)
3. [统一Workspace结构（单次实验与演进实验）](#3-统一workspace结构)
4. [统一实验结果统计](#4-统一实验结果统计)
5. [实验记录删除功能](#5-实验记录删除功能)
6. [核心Bug修复：metrics 404问题](#6-核心bug修复)
7. [rdagent命名清理](#7-rdagent命名清理)
8. [API路由重构](#8-api路由重构)
9. [数据库迁移方案](#9-数据库迁移方案)
10. [QE与RDAgent隔离性保障](#10-qe与rdagent隔离性保障)
11. [涉及修改的完整文件清单](#11-涉及修改的完整文件清单)
12. [实施顺序与验证](#12-实施顺序与验证)

---

## 1. 核心改进总览

本次整改包含以下核心改进，按优先级排列：

| # | 改进项 | 优先级 | 说明 |
|---|--------|--------|------|
| 1 | **Bug修复：metrics 404** | P0 | task_id构造错误导致实验结果无法获取 |
| 2 | **实验ID改为日期时间格式** | P0 | 从UUID改为 `qe_20260302_143025`，直观可读 |
| 3 | **统一workspace结构** | P1 | 单次实验与演进实验共用同一workspace，支持从单次实验直接开始演进 |
| 4 | **统一实验结果统计** | P1 | 消除单次实验和演进实验两套结果体系 |
| 5 | **实验删除功能** | P1 | 清理失败实验的workspace文件和DB记录 |
| 6 | **rdagent命名清理** | P2 | QE自身代码中不当的rdagent命名改为qe前缀 |
| 7 | **API路由重构** | P2 | loop路由嵌套在task下，双参数替代单参数编码 |

---

## 2. 实验ID格式改造

### 2.1 当前问题

```python
# config_composer.py 行155
experiment_id = str(uuid.uuid4())[:8]  # → "8c4c74fe"
experiment_name = f"qe_exp_{experiment_id}"  # → "qe_exp_8c4c74fe"
```

- UUID hex无语义，无法直观看出实验创建时间
- experiment_id和experiment_name是两个不同的值，容易混淆
- 演进任务的task_id格式 `Evo_{uuid4().hex[:8]}` 与实验ID风格不统一

### 2.2 方案：纯日期时间格式（方案D）

```
experiment_id = "qe_20260302_143025"
experiment_name = "qe_20260302_143025"  （两者统一，不再分离）
```

- 精确到秒，用户手动创建实验不可能同秒触发两次
- 演进任务自动创建Loop间隔至少几分钟，无冲突风险
- DB层面加UNIQUE约束，极端冲突时追加 `_2` 后缀

### 2.3 代码改动

**文件**: `config_composer.py`

```python
# 改前：
experiment_id = str(uuid.uuid4())[:8]
if not experiment_name:
    experiment_name = f"qe_exp_{experiment_id}"

# 改后：
from datetime import datetime

def _generate_experiment_id() -> str:
    """生成基于日期时间的实验ID，格式: qe_YYYYMMDD_HHMMSS"""
    return f"qe_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

experiment_id = _generate_experiment_id()
experiment_name = experiment_id  # 两者统一
```

**冲突处理**（DB插入时）：

```python
def _generate_unique_experiment_id(self) -> str:
    """生成唯一实验ID，冲突时追加后缀"""
    base_id = f"qe_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM qe_experiments WHERE experiment_id = %s",
                (base_id,),
            )
            if not cur.fetchone():
                return base_id
            # 极端冲突：追加序号
            for i in range(2, 100):
                candidate = f"{base_id}_{i}"
                cur.execute(
                    "SELECT 1 FROM qe_experiments WHERE experiment_id = %s",
                    (candidate,),
                )
                if not cur.fetchone():
                    return candidate
    raise RuntimeError(f"无法生成唯一实验ID: {base_id}")
```

**演进任务ID也统一格式**：

```python
# qe_evolution_service.py 改前：
task_id = f"Evo_{uuid4().hex[:8]}"

# 改后：
task_id = f"evo_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
```

### 2.4 影响范围

| 文件 | 改动 |
|------|------|
| `config_composer.py` | `compose_experiment()` 和 `compose_experiment_in_memory()` 中的ID生成 |
| `qe_evolution_service.py` | `create_task()` 中的task_id生成 |
| 前端 | 无需改动，experiment_id仍为字符串类型 |
| DB | 无需改动，experiment_id仍为TEXT类型 |



---

## 3. 统一Workspace结构（单次实验与演进实验）

### 3.1 当前问题

单次实验和演进实验使用不同的workspace结构和命名规则：

| 维度 | 单次实验 | 演进实验 |
|------|---------|---------|
| **task_id** | `f"{experiment_name}_{experiment_id}"` (错误拼接) | `Evo_{uuid4().hex[:8]}` |
| **workspace路径** | `qe_workspace/qe_exp_8c4c74fe/` (无Loop子目录) | `qe_workspace/Evo_a1b2c3d4/Evo_a1b2c3d4_L0/` |
| **loop_id格式** | 无（单次执行） | `{task_id}_L{current_loop}` |
| **结果存储** | `qe_experiments.result_metrics` | `qe_evolution_loops.metrics_json` |

**核心矛盾**：用户完成一次单次实验后，如果想基于该实验继续演进，无法直接在同一workspace下追加Loop，必须创建全新的演进任务，丢失了单次实验的workspace上下文。

### 3.2 统一方案

所有实验（无论单次还是演进）共用同一workspace结构：

```
qe_workspace/
  qe_20260302_143025/           ← task_id = experiment_id（统一后）
    Loop1/                      ← 单次实验 = Loop1
      conf.yaml
      prepare_factors.py
      factors/
      custom_model.py
      run.sh
      status.txt                ← pending → running → completed/failed
      qlib_results.json         ← 回测结果
      mlruns/                   ← QLib训练产物
    Loop2/                      ← 演进第2轮（如果用户选择继续演进）
      ...
    Loop3/
      ...
```

**关键设计**：
- 单次实验执行时，自动创建 `Loop1/` 子目录
- 用户对已完成的单次实验发起演进时，直接在同一workspace下创建 `Loop2/`、`Loop3/`...
- `task_id` 统一为 `experiment_id`（即 `qe_20260302_143025`）
- `loop_id` 统一为 `Loop{N}`（从1开始编号）

### 3.3 代码改动

#### 3.3.1 单次实验执行（quantevolver.py `run_experiment`）

```python
# 改前（行2260-2261）：
rdagent_task_id = f"{experiment_name}_{experiment_id}"
loop_index = 0

# 改后：
qe_task_id = experiment_name  # experiment_name = experiment_id（已统一）
loop_index = 1                # Loop从1开始编号
```

`compose_experiment_in_memory` 调用时，`experiment_name` 参数改为 `f"{qe_task_id}/Loop{loop_index}"`，使文件生成到正确的子目录。

#### 3.3.2 演进实验执行（qe_evolution_service.py）

```python
# 改前（行246）：
loop_id = f"{task_id}_L{current_loop}"

# 改后：
loop_index = current_loop + 1  # 演进从Loop2开始（Loop1是基础单次实验）
loop_id = f"Loop{loop_index}"
```

演进任务的 `task_id` 不再独立生成UUID，而是直接使用 `base_experiment_id`：

```python
# 改前：
task_id = f"Evo_{uuid4().hex[:8]}"

# 改后：
task_id = base_experiment_id  # 复用基础实验的ID作为task_id
```

#### 3.3.3 从单次实验无缝开始演进

新增逻辑：当用户对一个已完成的单次实验发起演进时：

```python
async def create_task(self, task_name, target_desc, max_loops, base_experiment_id):
    # 验证基础实验存在且已完成
    exp = self._get_experiment(base_experiment_id)
    if exp["status"] != "completed":
        raise ValueError("基础实验尚未完成，无法开始演进")
    
    # task_id 直接使用基础实验ID（workspace已存在Loop1）
    task_id = base_experiment_id
    
    # 演进从 current_loop=1 开始（下一个Loop编号 = current_loop + 1 = Loop2）
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO qe_evolution_tasks 
                (task_id, task_name, target_desc, max_loops, current_loop, status, base_experiment_id)
                VALUES (%s, %s, %s, %s, 1, 'pending', %s)
            """, (task_id, task_name, target_desc, max_loops, base_experiment_id))
        conn.commit()
    return task_id
```

### 3.4 workspace目录对照

| 场景 | 改前 | 改后 |
|------|------|------|
| 单次实验 | `qe_workspace/qe_exp_8c4c74fe/` (平铺) | `qe_workspace/qe_20260302_143025/Loop1/` |
| 演进Loop0 | `qe_workspace/Evo_a1b2c3d4/Evo_a1b2c3d4_L0/` | `qe_workspace/qe_20260302_143025/Loop2/` |
| 演进Loop1 | `qe_workspace/Evo_a1b2c3d4/Evo_a1b2c3d4_L1/` | `qe_workspace/qe_20260302_143025/Loop3/` |

---

## 4. 统一实验结果统计

### 4.1 当前问题

单次实验和演进实验的结果存储在不同位置：

| 来源 | 存储位置 | 字段 |
|------|---------|------|
| 单次实验 | `qe_experiments.result_metrics` | JSONB，直接存回测指标 |
| 演进Loop | `qe_evolution_loops.metrics_json` | JSONB，每个Loop独立存储 |

前端需要分别查询两张表才能获取完整的实验结果，且无法统一排序、对比。

### 4.2 统一方案

所有Loop的结果统一写入 `qe_experiments` 表，通过新增字段关联：

```sql
-- qe_experiments 表新增字段
ALTER TABLE qe_experiments ADD COLUMN loop_index INTEGER DEFAULT 1;
ALTER TABLE qe_experiments ADD COLUMN parent_experiment_id TEXT;
ALTER TABLE qe_experiments ADD COLUMN is_evolution_loop BOOLEAN DEFAULT FALSE;
```

**数据模型**：

```
qe_experiments 表：
┌─────────────────────────┬────────────┬──────────────────────────┬──────────┐
│ experiment_id           │ loop_index │ parent_experiment_id     │ is_evo   │
├─────────────────────────┼────────────┼──────────────────────────┼──────────┤
│ qe_20260302_143025      │ 1          │ NULL                     │ false    │  ← 单次实验
│ qe_20260302_143025_L2   │ 2          │ qe_20260302_143025       │ true     │  ← 演进Loop2
│ qe_20260302_143025_L3   │ 3          │ qe_20260302_143025       │ true     │  ← 演进Loop3
└─────────────────────────┴────────────┴──────────────────────────┴──────────┘
```

### 4.3 统一查询

```sql
-- 获取某个实验（含所有演进Loop）的完整结果
SELECT experiment_id, loop_index, result_metrics, status, is_sota
FROM qe_experiments
WHERE experiment_id = 'qe_20260302_143025' 
   OR parent_experiment_id = 'qe_20260302_143025'
ORDER BY loop_index ASC;
```

### 4.4 演进Loop写入逻辑

演进服务每完成一个Loop，除了更新 `qe_evolution_loops` 外，同时在 `qe_experiments` 中插入一条记录：

```python
# qe_evolution_service.py 中 Loop 完成后
cur.execute("""
    INSERT INTO qe_experiments 
    (experiment_id, experiment_name, loop_index, parent_experiment_id, 
     is_evolution_loop, factor_names, model_id, strategy_id, 
     result_metrics, status, is_sota)
    VALUES (%s, %s, %s, %s, TRUE, %s, %s, %s, %s, 'completed', %s)
    ON CONFLICT (experiment_id) DO UPDATE SET
        result_metrics = EXCLUDED.result_metrics,
        status = EXCLUDED.status,
        is_sota = EXCLUDED.is_sota
""", (
    f"{task_id}_L{loop_index}",  # experiment_id
    f"{task_id} Loop{loop_index}",  # experiment_name
    loop_index,
    task_id,  # parent_experiment_id
    json.dumps(config.get("factor_list", [])),
    config.get("model_id"),
    config.get("strategy_id"),
    json.dumps(metrics),
    is_sota,
))
```

### 4.5 前端统一展示

实验列表页增加分组展示：

```
▼ qe_20260302_143025 (3 Loops)          IC: 0.0423  年化: 15.6%  状态: 演进中
    Loop1 (基础实验)                     IC: 0.0423  年化: 15.6%  ✅ 已完成
    Loop2 (param_tune)                   IC: 0.0456  年化: 17.2%  ⭐ SOTA
    Loop3 (factor_swap)                  IC: 0.0412  年化: 14.8%  ✅ 已完成
```

---

## 5. 实验记录删除功能

### 5.1 当前状态

QE实验模块完全缺少删除功能。失败的实验记录和workspace文件会持续累积，无法清理。

### 5.2 删除API设计

```
DELETE /api/v1/quantevolver/experiments/{experiment_id}
Query: cleanup_workspace=true  (默认true，是否同时清理WSL侧workspace文件)
```

### 5.3 删除流程

```
用户点击删除
    │
    ▼
1. 检查实验状态（running状态禁止删除）
    │
    ▼
2. 清理WSL侧workspace文件（通过RDAgent API）
    │  DELETE /tasks/{task_id}
    │  → 删除 qe_workspace/{experiment_id}/ 整个目录
    │
    ▼
3. 清理DB记录（按依赖顺序）
    │  a. qe_sota_registry（通过loop_id级联删除）
    │  b. qe_evolution_loops（通过task_id级联删除）
    │  c. qe_evolution_tasks（task_id = experiment_id）
    │  d. qe_factor_experiment_metrics（experiment_id）
    │  e. qe_experiments（experiment_id + 所有子Loop记录）
    │
    ▼
4. 返回删除结果
```

### 5.4 后端实现

```python
@router.delete("/experiments/{experiment_id}")
async def delete_experiment(
    experiment_id: str,
    cleanup_workspace: bool = True,
):
    """删除QE实验及其所有关联数据"""
    
    # 1. 检查实验存在且非运行中
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT status FROM qe_experiments WHERE experiment_id = %s",
                (experiment_id,),
            )
            exp = cur.fetchone()
            if not exp:
                raise HTTPException(status_code=404, detail="实验不存在")
            if exp["status"] == "running":
                raise HTTPException(status_code=409, detail="实验正在运行中，请先停止")
    
    errors = []
    
    # 2. 清理WSL侧workspace
    if cleanup_workspace:
        try:
            async with QEWorkspaceClient() as client:
                await client.cleanup_task_workspace(experiment_id)
        except Exception as e:
            errors.append(f"workspace清理失败: {e}")
            logger.warning(f"Workspace cleanup failed for {experiment_id}: {e}")
    
    # 3. 清理DB记录（事务内）
    with get_conn() as conn:
        with conn.cursor() as cur:
            # 删除演进相关记录（级联删除会处理sota_registry）
            cur.execute(
                "DELETE FROM qe_evolution_tasks WHERE task_id = %s",
                (experiment_id,),
            )
            # 删除因子实验指标
            cur.execute(
                "DELETE FROM qe_factor_experiment_metrics WHERE experiment_id = %s",
                (experiment_id,),
            )
            # 删除所有子Loop记录
            cur.execute(
                "DELETE FROM qe_experiments WHERE parent_experiment_id = %s",
                (experiment_id,),
            )
            # 删除主实验记录
            cur.execute(
                "DELETE FROM qe_experiments WHERE experiment_id = %s",
                (experiment_id,),
            )
        conn.commit()
    
    return {
        "ok": True,
        "experiment_id": experiment_id,
        "warnings": errors if errors else None,
    }
```

### 5.5 前端交互

实验列表操作列增加删除按钮，点击后弹出确认对话框：

```
确认删除实验 qe_20260302_143025？

此操作将：
• 删除WSL侧实验workspace目录及所有文件
• 删除数据库中的实验记录、演进记录、指标数据
• 此操作不可撤销

☑ 同时清理workspace文件

[取消]  [确认删除]
```

### 5.6 RDAgent侧API

`QEWorkspaceClient.cleanup_task_workspace()` 已存在（见 `qe_rdagent_api_client.py`），调用 `DELETE /tasks/{task_id}` 删除整个workspace目录。无需新增RDAgent侧API。


---

## 6. 核心Bug修复：metrics 404问题

### 6.1 根因分析

**Bug位置**: `quantevolver.py` 行2260

```python
# 当前代码（错误）：
rdagent_task_id = f"{experiment_name}_{experiment_id}"
# experiment_name = "qe_exp_8c4c74fe"
# experiment_id   = "8c4c74fe"
# → rdagent_task_id = "qe_exp_8c4c74fe_8c4c74fe"  ← 重复拼接！
```

**故障链**：

```
1. run_experiment() 构造 rdagent_task_id = "qe_exp_8c4c74fe_8c4c74fe"
2. RDAgent API 基于此 task_id 创建目录: qe_workspace/qe_exp_8c4c74fe_8c4c74fe/
3. 但 wsl_command 中 cd 到的是: qe_workspace/qe_exp_8c4c74fe/ (基于 experiment_name)
4. 实验文件生成在 experiment_name 目录
5. _poll_and_sync 用 rdagent_task_id 查询 metrics → 在错误目录下查找 → 404
```

### 6.2 修复方案

结合第2节（ID格式改造）和第3节（workspace统一），修复后的逻辑：

```python
# 修复后：
qe_task_id = experiment_name  # experiment_name = experiment_id = "qe_20260302_143025"
loop_index = 1                # Loop从1开始

# workspace路径: qe_workspace/qe_20260302_143025/Loop1/
# API查询路径:   /tasks/qe_20260302_143025/loops/Loop1/metrics
# 两者一致 → 不再404
```

### 6.3 涉及改动

| 文件 | 行号 | 改动 |
|------|------|------|
| `quantevolver.py` | 2260 | `rdagent_task_id = f"{experiment_name}_{experiment_id}"` → `qe_task_id = experiment_name` |
| `quantevolver.py` | 2261 | `loop_index = 0` → `loop_index = 1` |
| `quantevolver.py` | 2272-2296 | 所有 `rdagent_task_id` → `qe_task_id`，`rdagent_loop_id` → `qe_loop_id` |
| `quantevolver.py` | 2290 | `_poll_and_sync` 参数名同步改 |
| `_poll_and_sync` | 2131 | 参数名 `rdagent_task_id, rdagent_loop_id` → `qe_task_id, qe_loop_id` |

---

## 7. rdagent命名清理

### 7.1 清理原则

- QE自身代码中语义为"QE实验"的 `rdagent_*` 命名 → 改为 `qe_*`
- 语义确实指向"RDAgent数据/资产"的命名 → 保留不改

### 7.2 必须整改清单

| 当前名称 | 文件 | 整改为 |
|----------|------|--------|
| `rdagent_task_id` | DB列 `qe_experiments` | `qe_task_id` |
| `rdagent_loop_id` | DB列 `qe_experiments` | `qe_loop_id` |
| `rdagent_task_id` / `rdagent_loop_id` | `quantevolver.py` Python变量 | `qe_task_id` / `qe_loop_id` |
| `RdagentApiClient` | `qe_rdagent_api_client.py` 类名 | `QEWorkspaceClient` |
| `qe_rdagent_api_client.py` | 文件名 | `qe_workspace_client.py` |
| `rdagent_client` | `qe_evolution_service.py` 属性 | `workspace_client` |
| `_rdagent_config_cache` | `config_composer.py` 类属性 | `_workspace_config_cache` |
| `_fetch_rdagent_config` | `config_composer.py` 方法 | `_fetch_workspace_config` |
| `_get_rdagent_api_base` | `qe_file_sync_client.py` 函数 | `_get_qe_api_base` |
| `rdagent_task_id` / `rdagent_loop_id` | `experiments/page.tsx` TS类型 | `qe_task_id` / `qe_loop_id` |
| `rdagent_task_id` / `rdagent_loop_id` | `useExperimentSSE.ts` | `qe_task_id` / `qe_loop_id` |

### 7.3 保留不改清单

| 名称 | 原因 |
|------|------|
| `rdagent_task_sync` | 因子来源标识，语义为"从RDAgent Task同步来的因子" |
| `rdagent_sota` | 因子来源标识，同上 |
| `trading.rdagent_signal` | 信号表名，属于RDAgent产出的数据域 |
| `/rdagent/catalogs/factors` | API路径，访问的是RDAgent因子目录 |
| `rdagent_assets/` | 文件路径，存放RDAgent产出的资产 |
| `conda activate rdagent-gpu` | WSL环境名，基础设施配置 |
| `RDAGENT_FACTOR_DATA_WSL` 等环境变量 | 指向RDAgent侧数据路径 |
| `RDAGENT_RESULTS_API_BASE_URL` | 环境变量，指向RDAgent API服务 |

---

## 8. API路由重构

### 8.1 当前问题

RDAgent侧QE API使用单参数编码task_id到loop_id中：

```
GET /loops/{loop_id}/status
# loop_id = "Evo_a1b2c3d4_L0"
# 需要 rsplit("_L", 1) 反解析出 task_id 和 loop_index
```

这种设计脆弱且不直观，当task_id本身包含 `_L` 时会解析错误。

### 8.2 改为双参数嵌套路由

```
# 改前：
GET  /loops/{loop_id}/status
GET  /loops/{loop_id}/metrics
GET  /loops/{loop_id}/assets/download

# 改后：
GET  /tasks/{task_id}/loops/{loop_id}/status
GET  /tasks/{task_id}/loops/{loop_id}/metrics
GET  /tasks/{task_id}/loops/{loop_id}/assets/download
```

### 8.3 RDAgent侧改动

**文件**: `RD-Agent-main/rdagent/app/api_endpoints/qe_evolution_api.py`

```python
# 改前（行195）：
loop_id = f"{task_id}_L{request.loop_index}"

# 改后：
loop_id = f"Loop{request.loop_index}"

# 改前（行219-223, 238-242, 257-259）：
# 删除所有 rsplit("_L", 1) 解析逻辑

# 改后：handler签名直接接收两个路径参数
@router.get("/tasks/{task_id}/loops/{loop_id}/status")
async def get_loop_status(task_id: str, loop_id: str):
    workspace_path = WORKSPACE_BASE / task_id / loop_id
    ...

@router.get("/tasks/{task_id}/loops/{loop_id}/metrics")
async def get_loop_metrics(task_id: str, loop_id: str):
    workspace_path = WORKSPACE_BASE / task_id / loop_id
    ...
```

### 8.4 AIstock侧API Client改动

**文件**: `qe_workspace_client.py`（重命名后）

```python
class QEWorkspaceClient:
    # 改前：
    async def get_loop_status(self, loop_id: str) -> Dict:
        url = f"{self.base_url}/loops/{loop_id}/status"
    
    # 改后：
    async def get_loop_status(self, task_id: str, loop_id: str) -> Dict:
        url = f"{self.base_url}/tasks/{task_id}/loops/{loop_id}/status"
    
    # get_loop_metrics、download_loop_assets 同理改为双参数
```

### 8.5 调用方适配

所有调用 `get_loop_status(loop_id)` 的地方改为 `get_loop_status(task_id, loop_id)`：

| 文件 | 函数 | 改动 |
|------|------|------|
| `quantevolver.py` | `_poll_and_sync` | `client.get_loop_status(qe_loop_id)` → `client.get_loop_status(qe_task_id, qe_loop_id)` |
| `quantevolver.py` | `get_experiment_run_status` | 同上 |
| `quantevolver.py` | `stream_experiment_logs` | 同上 |
| `quantevolver.py` | `get_experiment_enhanced_metrics` | 同上 |
| `qe_evolution_service.py` | `start_task_loop` | `self.rdagent_client.get_loop_status(rd_loop_id)` → `self.workspace_client.get_loop_status(task_id, loop_id)` |

---

## 9. 数据库迁移方案

### 9.1 列名重命名

```sql
-- 1. qe_experiments 表列名重命名
ALTER TABLE qe_experiments RENAME COLUMN rdagent_task_id TO qe_task_id;
ALTER TABLE qe_experiments RENAME COLUMN rdagent_loop_id TO qe_loop_id;
```

### 9.2 新增字段

```sql
-- 2. qe_experiments 表新增统一结构字段
ALTER TABLE qe_experiments ADD COLUMN IF NOT EXISTS loop_index INTEGER DEFAULT 1;
ALTER TABLE qe_experiments ADD COLUMN IF NOT EXISTS parent_experiment_id TEXT;
ALTER TABLE qe_experiments ADD COLUMN IF NOT EXISTS is_evolution_loop BOOLEAN DEFAULT FALSE;

-- 3. 添加 UNIQUE 约束（支持新ID格式的唯一性）
-- experiment_id 列已有 UNIQUE 约束（DDL中定义），无需额外添加
```

### 9.3 历史数据迁移

```sql
-- 4. 已有实验记录的 qe_task_id 回填（如果之前为空或错误值）
UPDATE qe_experiments 
SET qe_task_id = experiment_name,
    qe_loop_id = 'Loop1',
    loop_index = 1
WHERE qe_task_id IS NULL OR qe_task_id LIKE '%_%_%';
-- 匹配旧格式 "qe_exp_xxx_xxx" 的错误拼接值
```

### 9.4 init_catalog_db.py DDL更新

```python
# 改前（行393-394）：
"rdagent_task_id TEXT,"
"rdagent_loop_id TEXT,"

# 改后：
"qe_task_id TEXT,"
"qe_loop_id TEXT,"
"loop_index INTEGER DEFAULT 1,"
"parent_experiment_id TEXT,"
"is_evolution_loop BOOLEAN DEFAULT FALSE,"
```

迁移代码中的列名也需同步更新（行522-523）：

```python
# 改前：
("rdagent_task_id", "TEXT"),
("rdagent_loop_id", "TEXT"),

# 改后：
("qe_task_id", "TEXT"),
("qe_loop_id", "TEXT"),
("loop_index", "INTEGER DEFAULT 1"),
("parent_experiment_id", "TEXT"),
("is_evolution_loop", "BOOLEAN DEFAULT FALSE"),
```

### 9.5 迁移执行顺序

1. 先执行 `ALTER TABLE RENAME COLUMN`（列名重命名）
2. 再执行 `ALTER TABLE ADD COLUMN`（新增字段）
3. 最后执行 `UPDATE` 回填历史数据
4. 更新 `init_catalog_db.py` DDL（确保新环境初始化正确）


---

## 10. QE与RDAgent隔离性保障

### 10.1 隔离原则

QE实验模块与RDAgent Task同步模块必须完全隔离：

```
┌─────────────────────────────────────────────────────────────────┐
│                        AIstock Backend                          │
│                                                                 │
│  ┌──────────────────────┐         ┌──────────────────────────┐  │
│  │  RDAgent Task同步模块 │         │  QE实验模块              │  │
│  │                      │         │                          │  │
│  │  服务:               │         │  服务:                   │  │
│  │  rdagent_task_sync   │         │  config_composer         │  │
│  │  rdagent_candidate   │         │  qe_evolution_service    │  │
│  │  rdagent_catalog_etl │         │  qe_workspace_client     │  │
│  │                      │         │  qe_file_sync_client     │  │
│  │  API Client:         │         │                          │  │
│  │  RDAgentResultsApi   │         │  API Client:             │  │
│  │  Client              │         │  QEWorkspaceClient       │  │
│  │                      │         │                          │  │
│  │  DB表:               │         │  DB表:                   │  │
│  │  rdagent_candidate_  │         │  qe_experiments          │  │
│  │  tasks               │         │  qe_evolution_tasks      │  │
│  │  aistock_factor_     │  只读   │  qe_evolution_loops      │  │
│  │  catalog        ─────┼────→────│  qe_factor_experiment_   │  │
│  │  aistock_model_      │  引用   │  metrics                 │  │
│  │  catalog        ─────┼────→────│  qe_sota_registry        │  │
│  │  aistock_strategy_   │         │                          │  │
│  │  catalog        ─────┼────→────│                          │  │
│  └──────────────────────┘         └──────────────────────────┘  │
│                                                                 │
│  QE只读引用catalog表（因子代码、模型代码、策略代码），            │
│  绝不写入catalog表，绝不触碰RDAgent的workspace。                │
└─────────────────────────────────────────────────────────────────┘
```

### 10.2 隔离验证清单

| 检查项 | 验证方法 | 预期结果 |
|--------|---------|---------|
| QE不写入catalog表 | grep所有QE服务文件中的INSERT/UPDATE SQL | 无任何对 `aistock_factor_catalog`、`aistock_model_catalog`、`aistock_strategy_catalog` 的写操作 |
| QE不使用RDAgent API Client | grep QE文件中的import | 不导入 `RDAgentResultsApiClient`，只使用 `QEWorkspaceClient` |
| RDAgent不依赖QE | grep RDAgent同步服务中的QE引用 | 零匹配 `qe_experiments`、`QEWorkspaceClient`、`ConfigComposer` |
| workspace物理隔离 | 检查文件路径 | QE只在 `qe_workspace/` 下操作，RDAgent在 `rdagent_workspace/` 下操作 |
| DB表无交叉外键 | 检查DDL | `qe_experiments` 与 `rdagent_candidate_tasks` 之间无外键约束 |
| API路由隔离 | 检查路由前缀 | QE路由: `/quantevolver/*`，RDAgent路由: `/rdagent/*` |

### 10.3 本次整改的隔离性影响

本次整改100%限于QE实验子系统，不干扰任何其他功能：

**RDAgent Task同步 — 完全隔离**：
- `rdagent_candidate_service.py` 零引用QE变量
- `rdagent_task_sync_service.py` 零引用QE变量
- Task同步使用 `RDAgentResultsApiClient`（独立文件），与QE的 `QEWorkspaceClient` 完全不同

**因子库/模型库/策略库 — 完全隔离**：
- catalog路由文件不引用任何QE变量
- catalog服务文件不依赖 `QEWorkspaceClient` 或 `ConfigComposer`

**`QEWorkspaceClient` 消费方 — 全部在QE内**：
- 仅被 `qe_evolution_service.py` 和 `quantevolver.py`（QE实验endpoint部分）引用
- 无任何外部文件引用

**DB列名改动 — 无级联风险**：
- `qe_task_id`/`qe_loop_id` 是 `qe_experiments` 表的普通TEXT列，无外键约束
- 引用这两列的SQL仅在 `quantevolver.py` 和 `config_composer.py` 中

---

## 11. 涉及修改的完整文件清单

### 11.1 AIstock后端

| 文件 | 改动要点 |
|------|----------|
| `routers/quantevolver.py` | task_id构造修复、变量重命名 `rdagent_*` → `qe_*`、`_poll_and_sync` 参数改名、双参数API调用、新增DELETE endpoint |
| `services/quantevolver/qe_rdagent_api_client.py` → **`qe_workspace_client.py`** | 文件重命名、类名 `RdagentApiClient` → `QEWorkspaceClient`、方法签名改为双参数 |
| `services/quantevolver/qe_evolution_service.py` | import路径更新、属性名 `rdagent_client` → `workspace_client`、task_id/loop_id格式统一、演进从Loop2开始 |
| `services/quantevolver/config_composer.py` | ID生成改为datetime格式、`_rdagent_config_cache` → `_workspace_config_cache`、`_fetch_rdagent_config` → `_fetch_workspace_config`、SELECT列名更新 |
| `services/quantevolver/qe_file_sync_client.py` | `_get_rdagent_api_base` → `_get_qe_api_base` |
| `init_catalog_db.py` | DDL列名更新 + 新增字段 + migration列名更新 |

### 11.2 AIstock前端

| 文件 | 改动要点 |
|------|----------|
| `frontend/src/app/quantevolver/experiments/page.tsx` | TS类型 `rdagent_task_id` → `qe_task_id`、`rdagent_loop_id` → `qe_loop_id`、新增删除按钮和确认对话框、新增分组展示（演进Loop） |
| `frontend/src/app/quantevolver/components/useExperimentSSE.ts` | 日志显示字段名更新 |

### 11.3 RDAgent侧

| 文件 | 改动要点 |
|------|----------|
| `rdagent/app/api_endpoints/qe_evolution_api.py` | 路由重构为嵌套结构 `/tasks/{task_id}/loops/{loop_id}/*`、loop_id格式改为 `Loop{N}`、删除 `rsplit("_L", 1)` 解析逻辑、WORKSPACE_BASE路径确认 |

---

## 12. 实施顺序与验证

### 12.1 实施顺序

按依赖关系分阶段执行，每阶段完成后独立验证：

**Phase 1: DB迁移 + RDAgent侧路由重构**（无代码依赖，可并行）

| 步骤 | 操作 | 说明 |
|------|------|------|
| 1a | 执行DB迁移SQL | 列名重命名 + 新增字段 + 历史数据回填 |
| 1b | RDAgent侧 `qe_evolution_api.py` | 路由重构 + loop_id格式 + 删除rsplit解析 |

**Phase 2: AIstock后端核心改动**（依赖Phase 1）

| 步骤 | 操作 | 说明 |
|------|------|------|
| 2a | 文件重命名 `qe_rdagent_api_client.py` → `qe_workspace_client.py` | 类名改、方法签名改为双参数 |
| 2b | `qe_evolution_service.py` | import路径、属性名、loop_id格式、task_id复用experiment_id |
| 2c | `config_composer.py` | ID生成改为datetime、方法名重命名、SELECT列名 |
| 2d | `quantevolver.py` | task_id构造修复、变量重命名、双参数调用、新增DELETE endpoint |
| 2e | `qe_file_sync_client.py` | 函数名重命名 |
| 2f | `init_catalog_db.py` | DDL更新 |

**Phase 3: 前端适配**（依赖Phase 2）

| 步骤 | 操作 | 说明 |
|------|------|------|
| 3a | `experiments/page.tsx` | TS类型字段名 + 删除按钮 + 分组展示 |
| 3b | `useExperimentSSE.ts` | 日志字段名 |

### 12.2 验证方案

**Phase 1 验证**：
- 确认DB迁移后 `qe_experiments` 表列名正确（`qe_task_id`、`qe_loop_id`）
- 确认RDAgent侧新路由 `/tasks/{task_id}/loops/{loop_id}/status` 可访问

**Phase 2 验证（核心功能）**：

| 测试项 | 操作 | 预期结果 |
|--------|------|---------|
| 创建实验 | 前端创建新QE实验 | experiment_id格式为 `qe_YYYYMMDD_HHMMSS` |
| 执行实验 | 一键执行 | workspace路径 `qe_workspace/{experiment_id}/Loop1/` 正确创建 |
| 获取结果 | 等待实验完成 | `_poll_and_sync` 成功获取metrics（不再404） |
| 查看状态 | 调用 `get_experiment_run_status` | 返回正确状态和metrics |
| 查看日志 | SSE日志流 | 正常流式输出 |
| 删除实验 | 调用DELETE API | workspace文件和DB记录全部清理 |
| 开始演进 | 对已完成实验发起演进 | 在同一workspace下创建Loop2 |

**Phase 3 验证（前端）**：
- 实验列表正常显示，字段名正确
- 删除按钮功能正常
- 演进Loop分组展示正确

**回归验证**：

| 测试项 | 预期结果 |
|--------|---------|
| RDAgent Task同步 | 刷新、V2对齐功能正常 |
| 因子库CRUD | 因子列表、详情、分类功能正常 |
| 策略库CRUD | 策略列表、详情功能正常 |
| 模型库 | 模型列表功能正常 |
