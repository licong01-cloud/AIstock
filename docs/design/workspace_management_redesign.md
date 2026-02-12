# Workspace 管理功能重新设计方案

> 文档版本: v1.0  
> 创建时间: 2026-02-12  
> 状态: 设计中

---

## 一、问题描述

### 1.1 现象

在 `http://localhost:3000/rdagent/tasks-sync` 页面，每个 task 行有一个 **"Workspace"** 按钮，点击后应弹窗显示该 task 对应的所有 workspace 目录列表，并提供删除功能。

**当前问题：** 大量 task 存在以下情况，导致点击按钮无法获取 workspace 列表：
- 回测未执行成功的 LOOP
- 在因子开发阶段就报错退出
- 仅完成了假设阶段

只要 task 生成了 workspace 目录（即 `git_ignore_folder/RD-Agent_workspace/<hex32>/` 存在），就应该能在 UI 中展示和管理。

### 1.2 根因分析

**当前 RDAgent 侧 API `GET /tasks/{task_id}/workspaces` 的实现逻辑（`results_api_server.py:2519-2618`）：**

```
遍历 log/{task_id}/Loop_*/running/runner result/*.pkl
  → pickle 反序列化每个 .pkl 文件
    → 从对象中提取 experiment_workspace.workspace_path
    → 从对象中提取 sub_workspace_list[*].workspace_path
  → 收集所有 workspace 路径
  → 过滤掉不存在的目录
  → 用 du -sb 计算大小
```

**失败原因：**
1. **依赖 pickle 反序列化**：如果 LOOP 在因子开发阶段报错退出，`running/runner result/` 目录下可能没有 `.pkl` 文件，或 pickle 文件不完整/损坏无法反序列化
2. **依赖 `runner result` 目录存在**：如果 LOOP 在假设阶段就失败，根本不会产生 `running/runner result/` 目录
3. **workspace 目录实际已创建**：RDAgent 在因子代码生成阶段就会创建 workspace 目录（`git_ignore_folder/RD-Agent_workspace/<hex32>/`），但如果后续步骤失败，pickle 中不会记录该路径

**RDAgent 侧还有另一个函数 `_extract_workspace_ids_from_log_dir`（`results_api_server.py:755-787`）：**

```python
def _extract_workspace_ids_from_log_dir(*, task_id: str) -> list[str]:
    # 扫描 log/{task_id}/ 下所有文件的二进制内容
    # 用正则 r"RD-Agent_workspace/([0-9a-f]{32})" 匹配 workspace ID
    # 过滤掉不存在的 workspace 目录
    # 返回存在的 workspace ID 列表
```

这个函数通过**文本扫描**而非 pickle 反序列化来发现 workspace ID，覆盖面更广，能发现：
- pickle 文件中引用的 workspace
- 日志文件中记录的 workspace
- 任何包含 workspace 路径的文本文件

但这个函数**没有被 `/tasks/{task_id}/workspaces` API 使用**，仅被其他内部函数调用。

---

## 二、与 TASK 同步功能的独立性分析

### 2.1 TASK 同步功能的代码路径

| 组件 | 文件 | 关键函数/路由 |
|------|------|--------------|
| 前端加载候选 | `page.tsx:71-98` | `loadCandidates()` → `GET /api/v1/rdagent/tasks/sync-candidates` |
| 前端执行同步 | `page.tsx:108-136` | `handleSyncSelected()` → `POST /api/v1/rdagent/tasks/sync` |
| 前端启用/禁用选股 | `page.tsx:138-150` | `toggleEnableForSelection()` → `POST /api/v1/rdagent/tasks/{id}/enable_for_selection` |
| 后端路由 | `routers/rdagent.py:195-197` | `list_task_sync_candidates()` → `rdagent_task_sync_service.list_sync_candidates()` |
| 后端路由 | `routers/rdagent.py:211-241` | `sync_tasks()` → `rdagent_task_sync_service.sync_task_from_log()` |
| 后端服务 | `rdagent_task_sync_service.py:815-1110` | `list_sync_candidates()` — 从 RDAgent API 获取 task 列表 + V2 对齐信息 |
| 后端服务 | `rdagent_task_sync_service.py:165-172` | `TaskSyncResult` — 同步结果模型 |

### 2.2 Workspace 管理功能的代码路径

| 组件 | 文件 | 关键函数/路由 |
|------|------|--------------|
| 前端加载 workspace | `page.tsx:272-289` | `loadWorkspaceInfo()` → **直接调用** `http://127.0.0.1:9000/tasks/{taskId}/workspaces` |
| 前端删除 task | `page.tsx:292-310` | `deleteTask()` → **直接调用** `http://127.0.0.1:9000/tasks/{taskId}` (DELETE) |
| RDAgent API | `results_api_server.py:2519-2618` | `GET /tasks/{task_id}/workspaces` — pickle 解析方式 |
| RDAgent API | `results_api_server.py:2620-2732` | `DELETE /tasks/{task_id}` — 删除 task + workspace |

### 2.3 独立性结论

**两个功能完全独立：**
- **TASK 同步**：前端 → AIstock 后端 (`/api/v1/rdagent/tasks/sync-candidates`, `/api/v1/rdagent/tasks/sync`) → `rdagent_task_sync_service.py`
- **Workspace 管理**：前端 → **直接调用 RDAgent API** (`http://127.0.0.1:9000/tasks/{taskId}/workspaces`, `DELETE /tasks/{taskId}`)

Workspace 管理功能**不经过 AIstock 后端**，直接从前端调用 RDAgent 侧 API（端口 9000）。两者没有共享任何 API 路由或服务代码。

**本次改造只需修改 RDAgent 侧的 `/tasks/{task_id}/workspaces` API 实现，不会影响 TASK 同步功能的任何逻辑。**

---

## 三、设计方案

### 3.1 核心思路

改造 RDAgent 侧 `GET /tasks/{task_id}/workspaces` API，采用**双重发现策略**：

1. **策略 A（文本扫描）**：复用已有的 `_extract_workspace_ids_from_log_dir` 逻辑，通过正则扫描 log 目录下所有文件内容，匹配 `RD-Agent_workspace/<hex32>` 模式的 workspace ID
2. **策略 B（pickle 解析，现有逻辑）**：保留现有的 pickle 反序列化逻辑作为补充

两种策略的结果取**并集**，确保覆盖所有可能的 workspace。

### 3.2 为什么选择文本扫描而非直接扫描 workspace 目录

**不能直接扫描 `git_ignore_folder/RD-Agent_workspace/` 目录**，因为：
- 该目录下可能有上百个 workspace，属于不同的 task
- 没有从 workspace ID 反向映射到 task ID 的索引
- 全量扫描会导致所有 task 返回相同的 workspace 列表

**文本扫描的优势：**
- `_extract_workspace_ids_from_log_dir` 已经是经过验证的函数，被多处内部代码使用
- 它扫描的是 `log/{task_id}/` 目录，天然按 task 隔离
- 不仅能从 pickle 中发现 workspace，还能从日志文件、JSON 文件等任何文本中发现
- 即使 pickle 反序列化失败，只要文件的二进制内容中包含 workspace 路径字符串，就能被发现

### 3.3 详细设计

#### 3.3.1 修改 RDAgent 侧 API

**文件：** `f:/Dev/RD-Agent-main/rdagent/app/results_api_server.py`

**修改 `get_task_workspaces` 函数（L2519-L2618）：**

```python
@app.get("/tasks/{task_id}/workspaces", summary="获取Task的workspace信息")
def get_task_workspaces(task_id: str) -> dict[str, Any]:
    """获取指定task的workspace信息。
    
    双重发现策略：
    1. 文本扫描：通过正则匹配 log 目录下所有文件中的 workspace ID
    2. Pickle解析：从 session pickle 中提取 workspace_path（兼容旧逻辑）
    两种策略结果取并集，确保覆盖所有 workspace。
    """
    import subprocess
    
    try:
        _ensure_task_log_dir(task_id)
        task_dir = (_log_root() / task_id).resolve()
        ws_root = _workspace_root()
        
        result = {
            "ok": True,
            "task_id": task_id,
            "task_dir": str(task_dir),
            "workspaces": [],
            "total_size_mb": 0
        }
        
        # ===== 策略 A：文本扫描（主策略，覆盖面最广）=====
        # 复用 _extract_workspace_ids_from_log_dir 的逻辑
        workspace_ids = set()
        try:
            ids_from_scan = _extract_workspace_ids_from_log_dir(task_id=task_id)
            workspace_ids.update(ids_from_scan)
        except Exception:
            pass
        
        # ===== 策略 B：Pickle 解析（补充策略）=====
        workspace_paths_from_pkl = set()
        try:
            for loop_dir in task_dir.iterdir():
                if not loop_dir.is_dir() or not loop_dir.name.startswith("Loop_"):
                    continue
                runner_result_dir = loop_dir / "running" / "runner result"
                if not runner_result_dir.exists():
                    continue
                for pkl_file in runner_result_dir.rglob("*.pkl"):
                    try:
                        obj = _pickle_load_compat(pkl_file)
                        ws_path = getattr(getattr(obj, "experiment_workspace", None), "workspace_path", None)
                        if ws_path and isinstance(ws_path, (str, Path)):
                            workspace_paths_from_pkl.add(str(ws_path).strip())
                        sub_ws_list = getattr(obj, "sub_workspace_list", None) or []
                        for sub_ws in sub_ws_list:
                            if sub_ws is None:
                                continue
                            sub_ws_path = getattr(sub_ws, "workspace_path", None)
                            if sub_ws_path and isinstance(sub_ws_path, (str, Path)):
                                workspace_paths_from_pkl.add(str(sub_ws_path).strip())
                    except Exception:
                        continue
        except Exception:
            pass
        
        # 从 pickle 路径中提取 workspace ID 并合并
        for ws_path_str in workspace_paths_from_pkl:
            for m in _ws_re.finditer(ws_path_str):
                workspace_ids.add(m.group(1).lower())
        
        # ===== 合并结果，构建 workspace 信息列表 =====
        total_size_bytes = 0
        for ws_id in sorted(workspace_ids):
            try:
                ws_path = ws_root / ws_id
                if not ws_path.exists() or not ws_path.is_dir():
                    continue
                
                ws_size_mb = 0.0
                try:
                    du_result = subprocess.run(
                        ["du", "-sb", str(ws_path)],
                        capture_output=True, text=True, timeout=5
                    )
                    if du_result.returncode == 0:
                        ws_size_bytes = int(du_result.stdout.split()[0])
                        ws_size_mb = round(ws_size_bytes / (1024 * 1024), 2)
                        total_size_bytes += ws_size_bytes
                except Exception:
                    pass
                
                result["workspaces"].append({
                    "name": ws_id,
                    "path": str(ws_path),
                    "size_mb": ws_size_mb
                })
            except Exception:
                continue
        
        result["total_size_mb"] = round(total_size_bytes / (1024 * 1024), 2)
        return result
        
    except Exception as e:
        return {
            "ok": False,
            "task_id": task_id,
            "error": str(e),
            "workspaces": []
        }
```

#### 3.3.2 修改 RDAgent 侧 DELETE API

**同步修改 `delete_task` 函数（L2620-L2732）中的 workspace 发现逻辑：**

在删除 task 时，也需要使用双重发现策略来确保删除所有 workspace：

```python
@app.delete("/tasks/{task_id}", summary="删除Task及其所有数据")
def delete_task(task_id: str) -> dict[str, Any]:
    """删除指定task的日志目录和所有workspace目录。"""
    import shutil
    import subprocess
    
    try:
        task_dir = (_log_root() / task_id).resolve()
        if not task_dir.exists():
            return {"ok": False, "task_id": task_id, "error": "Task目录不存在"}
        
        ws_root = _workspace_root()
        deleted_items = []
        total_size_mb = 0.0
        
        # 1. 双重发现策略收集所有 workspace ID
        workspace_ids = set()
        
        # 策略 A：文本扫描
        try:
            ids_from_scan = _extract_workspace_ids_from_log_dir(task_id=task_id)
            workspace_ids.update(ids_from_scan)
        except Exception:
            pass
        
        # 策略 B：Pickle 解析
        try:
            for loop_dir in task_dir.iterdir():
                if not loop_dir.is_dir() or not loop_dir.name.startswith("Loop_"):
                    continue
                runner_result_dir = loop_dir / "running" / "runner result"
                if not runner_result_dir.exists():
                    continue
                for pkl_file in runner_result_dir.rglob("*.pkl"):
                    try:
                        obj = _pickle_load_compat(pkl_file)
                        ws_path = getattr(getattr(obj, "experiment_workspace", None), "workspace_path", None)
                        if ws_path and isinstance(ws_path, (str, Path)):
                            for m in _ws_re.finditer(str(ws_path)):
                                workspace_ids.add(m.group(1).lower())
                        sub_ws_list = getattr(obj, "sub_workspace_list", None) or []
                        for sub_ws in sub_ws_list:
                            if sub_ws is None:
                                continue
                            sub_ws_path = getattr(sub_ws, "workspace_path", None)
                            if sub_ws_path and isinstance(sub_ws_path, (str, Path)):
                                for m in _ws_re.finditer(str(sub_ws_path)):
                                    workspace_ids.add(m.group(1).lower())
                    except Exception:
                        continue
        except Exception:
            pass
        
        # 2. 计算并删除 task 目录
        # ... (保持现有逻辑)
        
        # 3. 删除 workspace 目录（使用 workspace_ids）
        for ws_id in workspace_ids:
            ws_path = ws_root / ws_id
            # ... (保持现有删除逻辑)
        
        return {
            "ok": True,
            "task_id": task_id,
            "deleted_items": deleted_items,
            "total_size_mb": round(total_size_mb, 2),
            "message": f"成功删除task {task_id}及{len(workspace_ids)}个workspace"
        }
    except Exception as e:
        return {"ok": False, "task_id": task_id, "error": f"删除失败: {str(e)}"}
```

#### 3.3.3 前端无需修改

前端代码（`page.tsx:272-310`）的逻辑已经是正确的：
- `loadWorkspaceInfo()` 调用 `GET http://127.0.0.1:9000/tasks/{taskId}/workspaces` 获取 workspace 列表
- `deleteTask()` 调用 `DELETE http://127.0.0.1:9000/tasks/{taskId}` 删除 task
- 弹窗展示 workspace 列表和删除确认

API 返回的数据结构不变（`ok`, `task_id`, `task_dir`, `workspaces[]`, `total_size_mb`），前端无需任何修改。

---

## 四、影响范围分析

### 4.1 修改文件清单

| 文件 | 修改内容 | 影响范围 |
|------|----------|----------|
| `rdagent/app/results_api_server.py` | 修改 `get_task_workspaces` 函数 | 仅影响 `GET /tasks/{task_id}/workspaces` API |
| `rdagent/app/results_api_server.py` | 修改 `delete_task` 函数 | 仅影响 `DELETE /tasks/{task_id}` API |

### 4.2 不受影响的功能

| 功能 | 原因 |
|------|------|
| TASK 同步（sync-candidates / sync） | 使用完全不同的 API 路径和服务代码 |
| LOOP 详情（candidate-loops） | 使用 AIstock 后端的 `rdagent_candidate_service` |
| Task 刷新（refresh） | 使用 AIstock 后端的 `rdagent_candidate_service` |
| 启用/禁用选股 | 使用 AIstock 后端的 `rdagent_task_sync_service` |
| 其他 RDAgent API | `_extract_workspace_ids_from_log_dir` 是只读函数，不会被修改 |

### 4.3 API 契约不变

修改前后 API 的请求/响应格式完全一致：

**请求：** `GET /tasks/{task_id}/workspaces`

**响应：**
```json
{
  "ok": true,
  "task_id": "xxxxxxxx",
  "task_dir": "/path/to/log/xxxxxxxx",
  "workspaces": [
    {
      "name": "abcdef1234567890abcdef1234567890",
      "path": "/path/to/RD-Agent_workspace/abcdef1234567890abcdef1234567890",
      "size_mb": 12.34
    }
  ],
  "total_size_mb": 12.34
}
```

---

## 五、实施步骤

### Step 1: 修改 RDAgent 侧 `get_task_workspaces` 函数

- 在 `results_api_server.py` 中修改 `get_task_workspaces`（L2519-L2618）
- 添加策略 A（文本扫描），保留策略 B（pickle 解析）作为补充
- 两种策略结果取并集

### Step 2: 修改 RDAgent 侧 `delete_task` 函数

- 在 `results_api_server.py` 中修改 `delete_task`（L2620-L2732）
- 使用与 Step 1 相同的双重发现策略
- 确保删除时能覆盖所有 workspace

### Step 3: 测试验证

1. **找一个因子开发阶段报错的 task**，确认修改后能获取到 workspace 列表
2. **找一个回测未成功的 task**，确认修改后能获取到 workspace 列表
3. **找一个正常完成的 task**，确认修改后结果与之前一致（向后兼容）
4. **测试删除功能**，确认能正确删除所有发现的 workspace
5. **验证 TASK 同步功能不受影响**：执行一次完整的 sync-candidates → sync 流程

### Step 4: 前端验证

- 在 tasks-sync 页面点击各种状态 task 的 "Workspace" 按钮
- 确认弹窗正确显示 workspace 列表
- 确认删除功能正常工作

---

## 六、风险评估

| 风险 | 概率 | 影响 | 缓解措施 |
|------|------|------|----------|
| 文本扫描误匹配（非本 task 的 workspace ID 出现在日志中） | 低 | 低 | `_extract_workspace_ids_from_log_dir` 已有 `(ws_root / ws).exists()` 过滤；且扫描范围限定在 `log/{task_id}/` 目录内 |
| 文本扫描性能（大量文件） | 低 | 低 | 已有 `max_files=200` 和 `max_read_bytes=256KB` 限制 |
| RDAgent API 服务重启 | 无 | 无 | API 使用 reload 模式，修改后自动生效 |
| TASK 同步功能受影响 | 无 | 无 | 两个功能完全独立，不共享任何代码路径 |

---

## 七、附录：关键代码位置索引

| 代码 | 文件 | 行号 |
|------|------|------|
| 前端 `loadWorkspaceInfo` | `frontend/src/app/rdagent/tasks-sync/page.tsx` | L272-289 |
| 前端 `deleteTask` | `frontend/src/app/rdagent/tasks-sync/page.tsx` | L292-310 |
| 前端 workspace 弹窗 UI | `frontend/src/app/rdagent/tasks-sync/page.tsx` | L734-876 |
| RDAgent `get_task_workspaces` | `rdagent/app/results_api_server.py` | L2519-2618 |
| RDAgent `delete_task` | `rdagent/app/results_api_server.py` | L2620-2732 |
| RDAgent `_extract_workspace_ids_from_log_dir` | `rdagent/app/results_api_server.py` | L755-787 |
| RDAgent `_workspace_root` | `rdagent/app/results_api_server.py` | L78-79 |
| RDAgent `_ws_re` 正则 | `rdagent/app/results_api_server.py` | L128 |
| AIstock TASK 同步路由 | `backend/routers/rdagent.py` | L195-241 |
| AIstock TASK 同步服务 | `backend/services/rdagent_task_sync_service.py` | L815-1110 |
| AIstock candidate-loops 路由 | `backend/routers/rdagent.py` | L1349-1373 |
| AIstock refresh 路由 | `backend/routers/rdagent.py` | L1396-1435 |
