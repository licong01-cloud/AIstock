"""
RD-Agent资产包打包逻辑分析报告
================================

## 1. 资产包打包流程

### 入口点
- 文件: `F:\Dev\RD-Agent-main\tools\backfill_registry_artifacts.py`
- 模式: `--mode solidify-all`
- 函数: `solidify_loop_assets(task_run_id, loop_id, db_path)`

### 核心逻辑
文件: `F:\Dev\RD-Agent-main\rdagent\utils\solidification.py`

#### 步骤1: 获取Loop的所有Workspace
```python
ws_rows = conn.execute(
    "SELECT workspace_id, workspace_path, experiment_type, workspace_role FROM workspaces 
     WHERE task_run_id = ? AND loop_id = ?",
    (task_run_id, loop_id),
).fetchall()
```

#### 步骤2: 创建资产包目录
```python
asset_bundle_id = str(uuid.uuid4())
bundle_root = repo_root / "RDagentDB" / "production_bundles" / asset_bundle_id
```

#### 步骤3: 扁平化拷贝资产（关键问题所在！）

**A. YAML配置文件**
- 拷贝所有 *.yaml 和 *.yml 文件到 bundle_root
- 冲突时使用 workspace_id 前缀

**B. Python实现代码**
- 使用 os.walk 深度遍历 workspace
- 排除目录: mlruns, .git, __pycache__, data, result, node_modules, .venv
- 排除文件: read_exp_res.py, runtime_info.py, setup.py, __init__.py, test.py, generated.py
- 拷贝所有其他 .py 文件到 bundle_root

**C. 模型权重文件**
- 根目录下的 model.pkl 优先
- 冲突时使用 workspace_id 前缀

**D. MLruns目录处理（问题根源！）**
```python
mlruns_dir = ws_path / "mlruns"
if mlruns_dir.exists():
    for m_root, m_dirs, m_files in os.walk(str(mlruns_dir)):
        for mf in m_files:
            # 仅提取 mlruns 下的权重和预测结果
            # 排除 params.pkl, config.pkl 等配置类文件
            if mf.endswith(".pkl") and mf not in ("params.pkl", "config.pkl"):
                target_name = f"{ws_id}_{mf}"
                shutil.copy2(source_file, bundle_root / target_name)
```

**关键问题**: 这里排除了 params.pkl！而 params.pkl 正是模型权重文件！

#### 步骤4: 持久化因子注册信息
- 读取 factor_meta.json 和 factor_perf.json
- 调用 reg.upsert_factor_registry() 注册因子

## 2. Workspace、Loop、Factor关联关系

### 数据库表结构

**workspaces表**
- workspace_id: 唯一标识
- task_run_id: 任务运行ID
- loop_id: 循环ID
- workspace_role: 角色（experiment_workspace, factor_workspace等）
- experiment_type: 实验类型（model, factor）
- workspace_path: 物理路径

**loops表**
- task_run_id: 任务运行ID
- loop_id: 循环ID
- action: 动作类型（model, factor）
- is_solidified: 是否已固化
- asset_bundle_id: 资产包ID

**factor_registry表**
- factor_name: 因子名称
- expression: 因子表达式
- performance_json: 性能数据
- asset_bundle_id: 资产包ID
- workspace_id: 工作区ID
- task_run_id: 任务运行ID
- loop_id: 循环ID

### 关联关系

```
task_run_id (任务)
  ├── loop_id (循环)
  │   ├── workspace_id (实验工作区)
  │   │   ├── factor_meta.json (因子元数据)
  │   │   ├── model_meta.json (模型元数据)
  │   │   ├── *.yaml (配置文件)
  │   │   ├── *.py (Python代码)
  │   │   ├── model.pkl (模型权重)
  │   │   └── mlruns/
  │   │       └── params.pkl (模型权重 - 被排除！)
  │   └── workspace_id (因子工作区)
  │       ├── factor_meta.json
  │       └── *.yaml
  └── asset_bundle_id (资产包)
      └── production_bundles/{asset_bundle_id}/
          ├── *.yaml (配置)
          ├── *.py (代码)
          └── *.pkl (权重/结果)
```

## 3. 问题分析

### 问题1: params.pkl被排除
```python
# 第195行
if mf.endswith(".pkl") and mf not in ("params.pkl", "config.pkl"):
```

**影响**: 
- 模型权重文件 params.pkl 被排除
- 导致资产包中没有可加载的模型文件

**原因**: 
- 代码注释说"仅提取 mlruns 下的权重和预测结果"
- 但 params.pkl 正是模型权重文件
- 可能是误认为 params.pkl 是配置文件

### 问题2: Python文件过滤过严
```python
# 第160行
if f in ("read_exp_res.py", "runtime_info.py", "setup.py", "__init__.py", "test.py", "generated.py"):
    continue
```

**影响**:
- read_exp_res.py 被排除
- 但这个文件可能包含核心推理逻辑

### 问题3: 扁平化结构导致文件丢失
- 所有文件都拷贝到 bundle_root 根目录
- 冲突时使用 workspace_id 前缀
- 但没有保留原始目录结构
- 导致某些文件可能被覆盖或丢失

## 4. 因子命名区分方案

### 当前问题
- 多个因子可能使用相同的名字
- 例如: "Alpha158", "MA", "RSI" 等
- 不同workspace中的同名因子无法区分

### 建议方案

**方案1: 命名空间前缀**
```
格式: {task_run_id}_{loop_id}_{workspace_id}_{factor_name}

示例:
2025-12-29_05-17-56-204326_1_b3caf6168516403580ea6ad430c1e31c_Alpha158
```

**方案2: 因子指纹去重**
- 使用因子表达式计算MD5指纹
- 相同表达式的因子视为同一个
- 不同表达式的同名因子自动区分

**方案3: 层级命名**
```
格式: {factor_name}@{workspace_id_short}

示例:
Alpha158@b3caf6
MA@e8b969
```

**推荐方案**: 方案2（因子指纹去重）
- 优点: 自动去重，语义清晰
- 实现: 在导出时计算因子表达式的MD5
- 兼容: 保持原有因子名称不变
"""

print(__doc__)
