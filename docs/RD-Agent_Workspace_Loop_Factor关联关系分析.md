"""
RD-Agent Workspace、Loop、Factor关联关系分析
==============================================

## 1. 核心概念

### Task Run (任务运行)
- **定义**: 一次完整的RD-Agent实验执行
- **标识**: task_run_id (格式: YYYY-MM-DD_HH-MM-SS-XXXXXX)
- **示例**: 2025-12-29_05-17-56-204326

### Loop (循环)
- **定义**: 任务运行中的一个迭代步骤
- **标识**: (task_run_id, loop_id)
- **示例**: (2025-12-29_05-17-56-204326, 1)
- **类型**: 
  - model_loop: 模型训练/推理循环
  - factor_loop: 因子生成/评估循环

### Workspace (工作区)
- **定义**: 存储实验数据和代码的目录
- **标识**: workspace_id (MD5哈希)
- **示例**: b3caf6168516403580ea6ad430c1e31c
- **类型**:
  - experiment_workspace: 主实验工作区
  - factor_workspace: 因子工作区
  - model_workspace: 模型工作区

### Factor (因子)
- **定义**: 量化因子，用于预测股票收益
- **标识**: factor_name
- **来源**:
  - qlib_alpha158: Qlib内置的158个Alpha因子
  - rdagent_generated: RD-Agent生成的因子
  - sota: 最先进的技术因子

## 2. 数据库表结构

### workspaces表
```sql
CREATE TABLE workspaces (
    workspace_id TEXT PRIMARY KEY,
    task_run_id TEXT,
    loop_id INTEGER,
    workspace_role TEXT,  -- experiment_workspace, factor_workspace, model_workspace
    experiment_type TEXT,  -- model, factor
    status TEXT,
    workspace_path TEXT,
    meta_path TEXT,
    summary_path TEXT,
    manifest_path TEXT,
    created_at_utc TEXT,
    updated_at_utc TEXT
);
```

**关键字段**:
- workspace_id: 唯一标识
- task_run_id + loop_id: 关联到loops表
- workspace_role: 工作区角色
- workspace_path: 物理路径

### loops表
```sql
CREATE TABLE loops (
    task_run_id TEXT,
    loop_id INTEGER,
    action TEXT,  -- model, factor
    status TEXT,
    has_result INTEGER,
    is_solidified INTEGER,
    asset_bundle_id TEXT,
    sync_status TEXT,
    log_dir TEXT,
    materialization_status TEXT,
    PRIMARY KEY (task_run_id, loop_id)
);
```

**关键字段**:
- task_run_id + loop_id: 唯一标识
- action: 循环类型
- is_solidified: 是否已固化
- asset_bundle_id: 资产包ID

### factor_registry表
```sql
CREATE TABLE factor_registry (
    factor_id TEXT PRIMARY KEY,
    factor_name TEXT,
    expression TEXT,
    performance_json TEXT,
    asset_bundle_id TEXT,
    workspace_id TEXT,
    task_run_id TEXT,
    loop_id INTEGER,
    created_at_utc TEXT,
    updated_at_utc TEXT
);
```

**关键字段**:
- factor_name: 因子名称
- expression: 因子表达式
- workspace_id + task_run_id + loop_id: 关联到具体实验
- asset_bundle_id: 资产包ID

## 3. 关联关系图

```
task_run_id: 2025-12-29_05-17-56-204326
│
├── loop_id: 1 (model_loop)
│   ├── action: model
│   ├── is_solidified: true
│   ├── asset_bundle_id: 645f3a32-3bb9-45c6-9587-45c03a1d967d
│   │
│   ├── workspace_id: b3caf6168516403580ea6ad430c1e31c (experiment_workspace)
│   │   ├── workspace_path: F:\Dev\RD-Agent-main\git_ignore_folder\RD-Agent_workspace\b3caf6168516403580ea6ad430c1e31c
│   │   ├── factor_meta.json: { factors: [...] }
│   │   ├── model_meta.json: { model_type: "LGBModel", ... }
│   │   ├── conf_*.yaml: 配置文件
│   │   ├── read_exp_res.py: Python实现
│   │   ├── model.pkl: 模型权重
│   │   └── mlruns/
│   │       └── params.pkl: 模型权重 (被排除！)
│   │
│   └── workspace_id: e8b9693e0a3b5047b2ac0cf5439730fc (factor_workspace)
│       ├── workspace_path: F:\Dev\RD-Agent-main\git_ignore_folder\RD-Agent_workspace\e8b9693e0a3b5047b2ac0cf5439730fc
│       └── factor_meta.json: { factors: [...] }
│
└── loop_id: 2 (factor_loop)
    ├── action: factor
    ├── is_solidified: false
    │
    └── workspace_id: xxx (factor_workspace)
        └── factor_meta.json: { factors: [...] }
```

## 4. 因子命名冲突场景

### 场景1: 同名因子，不同workspace
```
task_run_id: 2025-12-29_05-17-56-204326
├── loop_id: 1
│   └── workspace_id: b3caf6168516403580ea6ad430c1e31c
│       └── factor_meta.json: { factors: [{ name: "MA", expression: "MA(close, 5)" }] }
│
└── loop_id: 2
    └── workspace_id: e8b9693e0a3b5047b2ac0cf5439730fc
        └── factor_meta.json: { factors: [{ name: "MA", expression: "MA(close, 10)" }] }
```

**问题**: 两个MA因子表达式不同，但名称相同

### 场景2: 同名因子，不同task_run_id
```
task_run_id: 2025-12-29_05-17-56-204326
└── loop_id: 1
    └── workspace_id: b3caf6168516403580ea6ad430c1e31c
        └── factor_meta.json: { factors: [{ name: "Alpha158", expression: "..." }] }

task_run_id: 2025-12-30_10-20-30-123456
└── loop_id: 1
    └── workspace_id: xxx
        └── factor_meta.json: { factors: [{ name: "Alpha158", expression: "..." }] }
```

**问题**: 相同的Alpha158因子，但来自不同实验

### 场景3: Alpha158基础因子重复出现
```
每个model_loop都会包含完整的Alpha158因子集
- task_run_id_1/loop_1: Alpha158[0], Alpha158[1], ..., Alpha158[157]
- task_run_id_1/loop_2: Alpha158[0], Alpha158[1], ..., Alpha158[157]
- task_run_id_2/loop_1: Alpha158[0], Alpha158[1], ..., Alpha158[157]
```

**问题**: 158个Alpha158因子在每个循环中重复出现

## 5. Log目录中的关联信息

### Log目录结构
```
F:\Dev\RD-Agent-main\log\
└── 2025-12-29_05-17-56-204326\
    └── messages.msg
```

### Log消息类型
1. **FactorTask消息**: 包含因子任务信息
   ```python
   {
       "tag": "loop_1_generate_factor",
       "content": [
           FactorTask(
               factor_name="MA",
               factor_description="移动平均线",
               factor_formulation="MA(close, 5)",
               variables={}
           )
       ]
   }
   ```

2. **HypothesisFeedback消息**: 包含决策信息
   ```python
   {
       "tag": "loop_1_feedback",
       "content": HypothesisFeedback(
           decision=True,
           observations="因子表现良好",
           hypothesis_evaluation="...",
           new_hypothesis="..."
       )
   }
   ```

3. **Workspace路径消息**: 包含workspace_path
   ```python
   {
       "tag": "loop_1_experiment",
       "content": Experiment(
           experiment_workspace=Workspace(
               workspace_path="F:\Dev\RD-Agent-main\git_ignore_folder\RD-Agent_workspace\b3caf6168516403580ea6ad430c1e31c"
           )
       )
   }
   ```

## 6. 关联关系查询SQL

### 查询Loop的所有Workspace
```sql
SELECT workspace_id, workspace_role, experiment_type, workspace_path
FROM workspaces
WHERE task_run_id = '2025-12-29_05-17-56-204326' AND loop_id = 1;
```

### 查询Workspace的因子
```sql
SELECT factor_name, expression, performance_json
FROM factor_registry
WHERE workspace_id = 'b3caf6168516403580ea6ad430c1e31c';
```

### 查询Loop的资产包
```sql
SELECT asset_bundle_id, is_solidified
FROM loops
WHERE task_run_id = '2025-12-29_05-17-56-204326' AND loop_id = 1;
```

### 查询因子的所有出现
```sql
SELECT 
    fr.factor_name,
    fr.expression,
    fr.workspace_id,
    fr.task_run_id,
    fr.loop_id,
    l.action,
    l.is_solidified
FROM factor_registry fr
JOIN loops l ON fr.task_run_id = l.task_run_id AND fr.loop_id = l.loop_id
WHERE fr.factor_name = 'MA';
```

## 7. 关键发现

### 发现1: 一个Loop可以有多个Workspace
- experiment_workspace: 主实验
- factor_workspace: 因子实验
- model_workspace: 模型实验

### 发现2: 一个Workspace可以有多个因子
- factor_meta.json中的factors数组
- 每个因子有独立的性能数据

### 发现3: 同名因子可能来自不同Loop
- 需要通过(task_run_id, loop_id, workspace_id)区分
- 或者通过因子表达式区分

### 发现4: 资产包按Loop粒度打包
- 一个Loop对应一个asset_bundle_id
- 包含该Loop所有Workspace的资产

### 发现5: Log目录包含完整关联信息
- FactorTask消息: 因子定义
- HypothesisFeedback消息: 决策信息
- Workspace路径消息: 物理路径
"""

print(__doc__)
