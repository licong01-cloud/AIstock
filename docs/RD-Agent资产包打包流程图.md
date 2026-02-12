"""
RD-Agent资产包打包流程图
==========================

## 完整流程

```
┌─────────────────────────────────────────────────────────────┐
│  sync_all_to_aistock.py (主入口)                            │
│  --mode solidify-all                                        │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  backfill_registry_artifacts.py                              │
│  遍历所有 loops，筛选未固化的实验循环                         │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  solidify_loop_assets(task_run_id, loop_id)                 │
│  F:\Dev\RD-Agent-main\rdagent\utils\solidification.py       │
└────────────────────┬────────────────────────────────────────┘
                     │
        ┌────────────┴────────────┐
        ▼                         ▼
┌──────────────────┐    ┌──────────────────┐
│ 查询workspaces   │    │ 创建资产包目录    │
│ 按loop筛选       │    │ 生成UUID作为ID   │
└────────┬─────────┘    └────────┬─────────┘
         │                      │
         └──────────┬───────────┘
                    ▼
┌─────────────────────────────────────────────────────────────┐
│  遍历每个workspace                                           │
│  按优先级排序: experiment_workspace 优先                     │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  Step 1: 拷贝YAML配置文件                                    │
│  - *.yaml, *.yml                                            │
│  - 冲突时加 workspace_id 前缀                               │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  Step 2: 拷贝Python实现代码                                  │
│  - os.walk 深度遍历                                          │
│  - 排除目录: mlruns, .git, __pycache__, data, result...     │
│  - 排除文件: read_exp_res.py, runtime_info.py, setup.py...  │
│  - 冲突时加 workspace_id 前缀                               │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  Step 3: 拷贝模型权重文件                                    │
│  - 根目录下的 model.pkl 优先                                 │
│  - 冲突时加 workspace_id 前缀                               │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  Step 4: 处理MLruns目录 (问题根源！)                         │
│  - os.walk 遍历 mlruns/                                     │
│  - 提取 .pkl 文件                                           │
│  - ❌ 排除 params.pkl, config.pkl (关键问题！)              │
│  - 冲突时加 workspace_id 前缀                               │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  Step 5: 持久化因子注册信息                                  │
│  - 读取 factor_meta.json                                    │
│  - 读取 factor_perf.json                                    │
│  - 调用 reg.upsert_factor_registry()                        │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  Step 6: 更新Loop状态                                        │
│  - reg.upsert_loop()                                        │
│  - 设置 is_solidified=True                                  │
│  - 设置 asset_bundle_id                                     │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  返回 asset_bundle_id                                        │
└─────────────────────────────────────────────────────────────┘
```

## 关键代码位置

### 1. 主入口
```python
# F:\Dev\RD-Agent-main\tools\backfill_registry_artifacts.py
# Line 2242-2285
elif args.mode == "solidify-all":
    unique_loops = {}
    for ws in _iter_workspaces(cur):
        if ws.workspace_role == "experiment_workspace":
            loop_key = (ws.task_run_id, ws.loop_id)
            unique_loops[loop_key] = ws
    
    for (tr_id, l_id), ws in unique_loops.items():
        bundle_id = solidify_loop_assets(tr_id, l_id, db_path=db_path)
```

### 2. 资产包创建
```python
# F:\Dev\RD-Agent-main\rdagent\utils\solidification.py
# Line 96-99
asset_bundle_id = str(uuid.uuid4())
repo_root = reg.config.db_path.parent.parent
bundle_root = repo_root / "RDagentDB" / "production_bundles" / asset_bundle_id
bundle_root.mkdir(parents=True, exist_ok=True)
```

### 3. YAML拷贝
```python
# Line 124-146
for yaml_file in ws_path.glob("*.yaml"):
    target_name = yaml_file.name
    if (bundle_root / target_name).exists():
        target_name = f"{ws_id}_{yaml_file.name}"
    shutil.copy2(yaml_file, bundle_root / target_name)
```

### 4. Python拷贝
```python
# Line 148-173
for root, dirs, files in os.walk(str(ws_path)):
    dirs[:] = [d for d in dirs if d not in ("mlruns", ".git", "__pycache__", "data", "result", "node_modules", ".venv")]
    for f in files:
        if f.endswith(".py"):
            if f in ("read_exp_res.py", "runtime_info.py", "setup.py", "__init__.py", "test.py", "generated.py"):
                continue
            shutil.copy2(f_path, bundle_root / target_name)
```

### 5. MLruns处理 (问题！)
```python
# Line 188-209
mlruns_dir = ws_path / "mlruns"
if mlruns_dir.exists():
    for m_root, m_dirs, m_files in os.walk(str(mlruns_dir)):
        for mf in m_files:
            # ❌ 问题：这里排除了 params.pkl
            if mf.endswith(".pkl") and mf not in ("params.pkl", "config.pkl"):
                target_name = f"{ws_id}_{mf}"
                shutil.copy2(source_file, bundle_root / target_name)
```

### 6. 因子注册
```python
# Line 229-273
factor_meta_path = ws_path / "factor_meta.json"
if factor_meta_path.exists():
    meta = json.loads(factor_meta_path.read_text(encoding="utf-8"))
    factors = meta.get("factors") or []
    for f in factors:
        reg.upsert_factor_registry(
            factor_name=f_name,
            expression=expression,
            performance_json=f_perf,
            asset_bundle_id=asset_bundle_id,
            workspace_id=ws_id,
            task_run_id=task_run_id,
            loop_id=loop_id
        )
```

## 数据流向

```
RD-Agent Workspace
├── b3caf6168516403580ea6ad430c1e31c/
│   ├── conf_*.yaml          ──┐
│   ├── read_exp_res.py      ──┼──→ Asset Bundle
│   ├── model.pkl            ──┤   (扁平化结构)
│   ├── mlruns/              ──┤   └── 645f3a32-3bb9-45c6-9587-45c03a1d967d/
│   │   └── params.pkl       ──┘       ├── conf_*.yaml
│   └── factor_meta.json             ├── read_exp_res.py (被排除！)
│                                        ├── model.pkl
│                                        └── *.pkl (不含 params.pkl)
```

## 问题总结

### 问题1: params.pkl被排除
- **位置**: solidification.py:195
- **代码**: `if mf.endswith(".pkl") and mf not in ("params.pkl", "config.pkl")`
- **影响**: 模型权重文件丢失
- **修复**: 移除 params.pkl 的排除条件

### 问题2: read_exp_res.py被排除
- **位置**: solidification.py:160
- **代码**: `if f in ("read_exp_res.py", ...)`
- **影响**: Python实现代码丢失
- **修复**: 从排除列表中移除 read_exp_res.py

### 问题3: 扁平化结构
- **问题**: 所有文件放在根目录，可能冲突
- **影响**: 文件覆盖或丢失
- **修复**: 保留workspace_id子目录结构
"""

print(__doc__)
