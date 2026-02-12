# AIstock后端服务重启指南

## 已完成的修复

✅ 在 `backend/main.py` 中注册了 `rdagent_llm_config` 路由
✅ 路由路径：`/api/v1/rdagent/llm-config/*`

## 需要执行的操作

### 1. 重启AIstock后端服务

找到当前运行AIstock后端的终端窗口，按 `Ctrl+C` 停止服务，然后重新启动：

```powershell
# 在AIstock conda环境中
conda activate AIstock
cd F:\Dev\AIstock\backend
python -m uvicorn main:app --host 0.0.0.0 --port 8001 --reload
```

### 2. 验证API是否可访问

重启后，在浏览器或PowerShell中测试API：

```powershell
# 测试providers接口
curl http://localhost:8001/api/v1/rdagent/llm-config/providers

# 测试models接口
curl http://localhost:8001/api/v1/rdagent/llm-config/models
```

**预期结果**：
- ✅ 返回JSON数据（即使是空数组也正常）
- ❌ 如果返回404，说明路由注册有问题

### 3. 检查数据库表是否已创建

如果API返回数据库错误，需要执行SQL脚本创建表：

```powershell
# 连接PostgreSQL并执行初始化脚本
psql -U postgres -d aistock -f F:\Dev\AIstock\backend\database\init_rdagent_llm_tables.sql
```

### 4. 刷新前端页面

后端服务正常后，刷新前端页面：
- http://localhost:3000/config/rdagent-llm

## 当前API路径

后端路由定义：
```python
router = APIRouter(prefix="/api/v1/rdagent/llm-config", tags=["rdagent-llm-config"])
```

注册方式：
```python
app.include_router(rdagent_llm_config.router)  # 不需要额外的prefix
```

最终API路径：
- GET `/api/v1/rdagent/llm-config/providers`
- GET `/api/v1/rdagent/llm-config/models`
- GET `/api/v1/rdagent/llm-config/stage-mappings`
- GET `/api/v1/rdagent/llm-config/current-config`
- GET `/api/v1/rdagent/llm-config/change-logs`

## 故障排查

### 问题1：404 Not Found

**原因**：后端服务未重启，路由未生效

**解决方案**：重启后端服务

### 问题2：数据库错误

**原因**：数据库表未创建

**解决方案**：执行 `init_rdagent_llm_tables.sql` 脚本

### 问题3：RD-Agent API连接失败

**原因**：RD-Agent Results API未启动

**解决方案**：
```bash
# 在WSL中启动RD-Agent API
cd /mnt/f/Dev/RD-Agent-main
conda activate rdagent-gpu
python -m rdagent.app.cli results_api --host 127.0.0.1 --port 9000
```

---

**重要**：必须先重启AIstock后端服务，路由才会生效！
