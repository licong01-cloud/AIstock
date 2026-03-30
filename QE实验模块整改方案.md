# QE 实验模块整改方案

## Context

QE 实验完成后无法获取回测结果（metrics API 404），根因：

1. `quantevolver.py:2260` 构造 `rdagent_task_id = f"{experiment_name}_{experiment_id}"` = `"qe_exp_8c4c74fe_8c4c74fe"`
2. RDAgent API 基于此 task_id 创建目录 `qe_workspace/qe_exp_8c4c74fe_8c4c74fe/`
3. 但 `wsl_command` 中 cd 到的是 `qe_workspace/qe_exp_8c4c74fe/`（基于 experiment_name）
4. **结果文件生成在 experiment_name 目录，但 API 在 rdagent_task_id 目录下查找 → 404**

同时，QE 代码中大量使用 `rdagent` 命名，QE 与 RDAgent 是两个隔离应用，需清理命名边界。

---

## 一、核心 bug 修复 + 目录结构整改

### 目标目录结构

```
qe_workspace/
  qe_exp_8c4c74fe/          ← task_id = experiment_name（实验工作区）
    Loop1/                  ← 第 1 次 loop（从 1 开始编号）
      status.txt / config.json / run.log
    Loop2/                  ← 第 2 次 loop
```

### 改动 1：task_id 直接用 experiment_name（修复根因）

**文件**: `F:/Dev/AIstock/backend/routers/quantevolver.py`

- 行 2260: `rdagent_task_id = f"{experiment_name}_{experiment_id}"` → `qe_task_id = experiment_name`
- 行 2261: `loop_index = 0` → `loop_index = 1`（Loop 从 1 开始编号）
- 行 2272-2273: `rdagent_loop_id` → `qe_loop_id`
- 行 2276-2296: 所有 `rdagent_task_id` → `qe_task_id`，`rdagent_loop_id` → `qe_loop_id`
- 行 2290: `_poll_and_sync` 参数同步改名

### 改动 2：API 路由重构 — loop 路由嵌套在 task 下

**文件**: `F:/Dev/RD-Agent-main/rdagent/app/api_endpoints/qe_evolution_api.py`

当前路由：
```
GET  /loops/{loop_id}/status          ← loop_id 中编码了 task_id，用 rsplit("_L", 1) 反解析
GET  /loops/{loop_id}/metrics
GET  /loops/{loop_id}/assets/download
```

改为：
```
GET  /tasks/{task_id}/loops/{loop_id}/status     ← 直接传两个参数，无需反解析
GET  /tasks/{task_id}/loops/{loop_id}/metrics
GET  /tasks/{task_id}/loops/{loop_id}/assets/download
```

具体改动：
- 行 195: `loop_id = f"{task_id}_L{request.loop_index}"` → `loop_id = f"Loop{request.loop_index}"`
- 删除所有 `rsplit("_L", 1)` 解析逻辑（行 219-223, 238-242, 257-259）
- 各 handler 签名改为接收 `task_id: str, loop_id: str` 两个路径参数
- 行 30: `WORKSPACE_BASE` 默认路径改为 `Path(__file__).resolve().parents[3] / "qe_workspace"`

### 改动 3：API Client 方法签名改为双参数

**文件**: `F:/Dev/AIstock/backend/services/quantevolver/qe_rdagent_api_client.py` → **重命名为** `qe_workspace_client.py`

- 类名: `RdagentApiClient` → `QEWorkspaceClient`
- `get_loop_status(loop_id)` → `get_loop_status(task_id, loop_id)`，URL: `/tasks/{task_id}/loops/{loop_id}/status`
- `get_loop_metrics(loop_id)` → `get_loop_metrics(task_id, loop_id)`，URL: `/tasks/{task_id}/loops/{loop_id}/metrics`
- `download_loop_assets(loop_id, dest_dir)` → `download_loop_assets(task_id, loop_id, dest_dir)`，URL: `/tasks/{task_id}/loops/{loop_id}/assets/download`

### 改动 4：调用方适配新签名

**文件**: `F:/Dev/AIstock/backend/routers/quantevolver.py`

- `_poll_and_sync`（行 2131）：参数名改，调用 `get_loop_status`/`get_loop_metrics` 改为双参数
- `get_experiment_run_status`（行 2305）：同上
- `stream_experiment_logs`（行 2367）：同上
- `get_experiment_enhanced_metrics`（行 2425）：改为双参数 URL

**文件**: `F:/Dev/AIstock/backend/services/quantevolver/qe_evolution_service.py`

- 行 10-11: import 路径改
- 行 24: `self.rdagent_client` → `self.workspace_client`
- 行 246: `loop_id = f"{task_id}_L{current_loop}"` → `loop_id = f"Loop{current_loop + 1}"`
- 所有 `self.rdagent_client.xxx(loop_id)` → `self.workspace_client.xxx(task_id, loop_id)`

---

## 二、rdagent 命名清理

### 必须整改（QE 自身代码中的不当命名）

| 当前名称 | 位置 | 整改 |
|----------|------|------|
| `rdagent_task_id` | DB 列 `qe_experiments` | → `qe_task_id` |
| `rdagent_loop_id` | DB 列 `qe_experiments` | → `qe_loop_id` |
| `rdagent_task_id` / `rdagent_loop_id` | `quantevolver.py` Python 变量 | → `qe_task_id` / `qe_loop_id` |
| `RdagentApiClient` | `qe_rdagent_api_client.py:10` 类名 | → `QEWorkspaceClient` |
| `qe_rdagent_api_client.py` | 文件名 | → `qe_workspace_client.py` |
| `rdagent_client` | `qe_evolution_service.py:24` 属性 | → `workspace_client` |
| `_rdagent_config_cache` | `config_composer.py:98` 类属性 | → `_workspace_config_cache` |
| `_fetch_rdagent_config` | `config_composer.py:100` 方法 | → `_fetch_workspace_config` |
| `_get_rdagent_api_base` | `qe_file_sync_client.py:19` 函数 | → `_get_qe_api_base` |

### 暂不整改（语义正确的 RDAgent 数据域引用）

| 名称 | 原因 |
|------|------|
| `rdagent_task_sync` | 因子来源标识，表示"从 RDAgent Task 同步来的因子"，语义正确 |
| `rdagent_sota` | 因子来源标识，同上 |
| `rdagent_factor/template/module_level` | 代码类型分类器，描述 RDAgent 产出的代码格式 |
| `trading.rdagent_signal` | 信号表名，属于 RDAgent 产出的数据 |
| `/rdagent/catalogs/factors` | API 路径，访问的是 RDAgent 因子目录 |
| `rdagent_assets/` | 文件路径，存放 RDAgent 产出的资产 |
| `conda activate rdagent-gpu` | WSL 环境名，属于基础设施配置 |
| `RDAGENT_FACTOR_DATA_WSL` 等环境变量 | 指向 RDAgent 侧的数据路径，语义正确 |

---

## 三、DB 列名迁移

**文件**: `F:/Dev/AIstock/backend/init_catalog_db.py`

`qe_experiments` 表（行 393-394, 522-523）：
- `rdagent_task_id TEXT` → `qe_task_id TEXT`
- `rdagent_loop_id TEXT` → `qe_loop_id TEXT`

迁移 SQL：
```sql
ALTER TABLE qe_experiments RENAME COLUMN rdagent_task_id TO qe_task_id;
ALTER TABLE qe_experiments RENAME COLUMN rdagent_loop_id TO qe_loop_id;
```

---

## 四、涉及修改的完整文件清单

### AIstock 侧（F:/Dev/AIstock/backend/）

| 文件 | 改动要点 |
|------|----------|
| `routers/quantevolver.py` | task_id 构造逻辑、变量重命名 rdagent_* → qe_*、_poll_and_sync 参数、双参数 API 调用 |
| `services/quantevolver/qe_rdagent_api_client.py` → `qe_workspace_client.py` | 文件重命名、类名改、方法签名改为双参数 |
| `services/quantevolver/qe_evolution_service.py` | import 路径、属性名、loop_id 格式、API 调用双参数 |
| `services/quantevolver/config_composer.py` | `_rdagent_config_cache` → `_workspace_config_cache`、`_fetch_rdagent_config` → `_fetch_workspace_config`、`list_experiments` 中 SELECT 列名 |
| `services/quantevolver/qe_file_sync_client.py` | `_get_rdagent_api_base` → `_get_qe_api_base` |
| `init_catalog_db.py` | DB DDL 列名 + migration 列名 |
| `frontend/src/app/quantevolver/experiments/page.tsx` | TS 类型 rdagent_task_id → qe_task_id, rdagent_loop_id → qe_loop_id |
| `frontend/src/app/quantevolver/components/useExperimentSSE.ts` | 日志显示字段名 |

### RDAgent 侧（F:/Dev/RD-Agent-main/）

| 文件 | 改动要点 |
|------|----------|
| `rdagent/app/api_endpoints/qe_evolution_api.py` | 路由重构为嵌套结构、loop_id 格式 `Loop{N}`、WORKSPACE_BASE 路径、删除 rsplit 解析 |

---

## 五、影响隔离性分析

### 结论：本次修改 100% 限于 QE 实验子系统，不干扰任何其他功能

### 5.1 RDAgent Task 同步 — 完全隔离

| 检查项 | 结果 |
|--------|------|
| `rdagent_candidate_service.py` 是否引用 QE 变量？ | **否** — 零匹配 `rdagent_task_id`/`rdagent_loop_id`/`RdagentApiClient` |
| `rdagent_task_sync_service.py` 是否引用 QE 变量？ | **否** — 零匹配 |
| Task 同步用的 API Client 是哪个？ | `RDAgentResultsApiClient`（在 `rdagent_results_api_client.py`），与 QE 的 `RdagentApiClient`（在 `qe_rdagent_api_client.py`）**完全不同的类、不同的文件** |
| Task 同步的 DB 表？ | `rdagent.rdagent_candidate_tasks`，与 `qe_experiments` 无任何关联 |

### 5.2 因子库（因子目录 / 因子同步）— 完全隔离

| 检查项 | 结果 |
|--------|------|
| 因子库路由文件 | `rdagent_catalog_admin.py`、`rdagent.py` — 不引用任何 QE 变量 |
| 因子库服务文件 | `rdagent_factor_catalog_sync.py`、`rdagent_catalog_etl_service.py` — 不引用任何 QE 变量 |
| 因子库是否依赖 `qe_rdagent_api_client.py`？ | **否** — 零引用 |
| 因子库是否依赖 `ConfigComposer`？ | **否** — `ConfigComposer` 仅被 `quantevolver.py` 和 `qe_evolution_service.py` 导入 |

### 5.3 策略库 — 完全隔离

| 检查项 | 结果 |
|--------|------|
| 策略库路由 | `rdagent_catalog_admin.py`（行 819-1305）、`quantevolver.py`（行 388-671）— 策略 CRUD 部分不涉及 QE 实验变量 |
| 策略库服务 | 不依赖 `qe_rdagent_api_client.py` 或 QE 实验代码 |

### 5.4 `qe_rdagent_api_client.py`（即将重命名）— 消费方完全在 QE 内

全部消费方（共 3 个文件）：
1. `services/quantevolver/qe_evolution_service.py` — QE 演进服务
2. `routers/quantevolver.py`（行 2133, 2220, 2271, 2308, 2367, 2439）— 全部在 QE 实验相关的 endpoint 内（lazy import）
3. 无任何其他文件引用

### 5.5 DB 列名改动 — 无级联风险

- `rdagent_task_id` 和 `rdagent_loop_id` 是 `qe_experiments` 表的普通 TEXT 列
- **无外键约束** — 没有其他表引用这两列
- 引用这两列的 SQL 仅在 2 个文件中：`routers/quantevolver.py` 和 `services/quantevolver/config_composer.py`
- `qe_experiments` 表仅被 QE 子系统的 6 个文件访问，全部在 `quantevolver/` 路径下

### 5.6 `_get_rdagent_api_base`（qe_file_sync_client.py）— 模块私有函数

- 以 `_` 开头的模块私有函数，仅在 `qe_file_sync_client.py` 内部被 `__init__` 调用
- 不被任何外部文件导入或调用
- 与 `rdagent_task_sync_service.py` 共享同一环境变量 `RDAGENT_RESULTS_API_BASE_URL`，但**函数本身完全独立**
- 重命名仅影响函数名，不影响环境变量读取逻辑

### 5.7 Frontend — 仅 QE 实验页面

- `rdagent_task_id` / `rdagent_loop_id` 仅出现在 2 个前端文件中：
  - `frontend/src/app/quantevolver/experiments/page.tsx`（行 25-26）— TS 类型定义
  - `frontend/src/app/quantevolver/components/useExperimentSSE.ts`（行 230）— 日志显示
- 均在 `quantevolver/` 目录下，不影响 Task 同步页面或其他页面

---

## 六、实施顺序

1. **DB 迁移** — 执行 ALTER TABLE 列名重命名
2. **RDAgent 侧** `qe_evolution_api.py` — 路由重构 + loop_id 格式 + WORKSPACE_BASE
3. **AIstock 侧** 文件重命名 `qe_rdagent_api_client.py` → `qe_workspace_client.py`，类名改，方法签名改
4. **AIstock 侧** `qe_evolution_service.py` — import、属性名、loop_id 格式
5. **AIstock 侧** `quantevolver.py` — task_id 构造、变量重命名、_poll_and_sync、双参数调用
6. **AIstock 侧** `config_composer.py` — 方法名重命名、SELECT 列名
7. **AIstock 侧** `qe_file_sync_client.py` — 函数名重命名
8. **AIstock 侧** `init_catalog_db.py` — DDL 列名 + migration 列名
9. **Frontend** — TS 类型字段名 + SSE 日志字段名

---

## 七、验证方式

1. 创建新 QE 实验 → 执行 → 确认 `qe_workspace/{experiment_name}/Loop1/` 目录正确生成
2. 确认 `_poll_and_sync` 轮询成功获取 metrics（不再 404）
3. 确认 `result_metrics` 正确写入 DB `qe_experiments` 表
4. 确认实验日志 SSE 正常流式输出
5. 确认 `get_experiment_run_status` API 返回正确状态
6. **回归验证**：确认 RDAgent Task 同步页面正常工作（刷新、V2 对齐）
7. **回归验证**：确认因子库 CRUD、因子同步功能正常
8. **回归验证**：确认策略库 CRUD 功能正常
