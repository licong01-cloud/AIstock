# RD-Agent LLM配置管理系统 - 部署和测试指南

## 📋 系统概述

RD-Agent LLM配置管理系统已完成开发，包含：
- ✅ RD-Agent侧：模块化API服务（端口9000）
- ✅ AIstock侧：后端API（端口8001）+ 前端页面
- ✅ 数据库：PostgreSQL表结构
- ✅ 前端：Next.js + React + TailwindCSS

---

## 🚀 快速部署步骤

### 步骤1：初始化数据库

```bash
# 在Windows PowerShell中执行
cd F:\Dev\AIstock
psql -U postgres -d aistock -f backend\database\init_rdagent_llm_tables.sql
```

**验证数据库**：
```sql
-- 连接到数据库
psql -U postgres -d aistock

-- 检查表是否创建成功
\dt aistock_llm*

-- 检查初始数据
SELECT * FROM aistock_llm_providers;
```

### 步骤2：启动RD-Agent API服务

```bash
# 在WSL终端中执行
cd /mnt/f/Dev/RD-Agent-main
conda activate rdagent-gpu
python -m rdagent.app.cli results_api --host 127.0.0.1 --port 9000
```

**验证服务**：
```bash
# 新开一个终端测试
curl http://127.0.0.1:9000/llm-config/health
# 预期输出: {"status":"ok","service":"llm-config"}
```

### 步骤3：启动AIstock后端

```bash
# 在Windows PowerShell中执行
cd F:\Dev\AIstock
conda activate AIstock
uvicorn backend.main:app --host 0.0.0.0 --port 8001 --reload
```

**验证服务**：
```bash
# 新开一个PowerShell测试
curl http://127.0.0.1:8001/api/v1/rdagent/llm-config/health
# 预期输出: {"status":"ok","rdagent_api":{"status":"ok","service":"llm-config"}}
```

### 步骤4：启动AIstock前端

```bash
# 在Windows PowerShell中执行
cd F:\Dev\AIstock\frontend
npm run dev
```

**访问页面**：
- 打开浏览器访问：`http://localhost:3000/config/rdagent-llm`

---

## 🧪 功能测试清单

### 测试1：查看服务商列表

**操作**：
1. 访问配置页面
2. 点击"服务商与模型"标签

**预期结果**：
- 显示4个预置服务商卡片：
  - DeepSeek
  - 硅基流动
  - 阿里云百炼
  - Claude

### 测试2：添加新模型

**操作**：
1. 选择一个服务商，点击"添加模型"
2. 填写表单：
   - 模型名称：`deepseek-chat`
   - 显示名称：`DeepSeek Chat`
   - 模型类型：`对话/Coding`
   - 模型分类：`对话/Coding`
   - 说明：`高性能对话模型`
   - API Key：`sk-your-api-key`
3. 点击"添加模型"

**预期结果**：
- 显示"验证中..."
- 验证成功后显示"模型添加成功"
- 模型出现在服务商卡片中
- 显示绿色验证图标

**失败情况**：
- API Key错误：显示验证失败错误信息
- 模型不存在：显示模型不可用错误

### 测试3：配置阶段映射

**操作**：
1. 点击"阶段映射配置"标签
2. 为各阶段选择模型：
   - coding: 选择对话模型
   - hypothesis: 选择推理模型
   - embedding: 选择嵌入模型
3. 配置参数（对于非embedding阶段）：
   - Temperature: 0.7
   - Max Tokens: 4000
4. 填写变更原因："初始化配置"
5. 点击"保存配置"

**预期结果**：
- 显示"保存中..."
- 配置成功后显示"配置更新成功"
- RD-Agent API验证配置可用性
- .env文件已更新
- 创建备份文件

**验证配置**：
```bash
# 在WSL中检查.env文件
cat /mnt/f/Dev/RD-Agent-main/.env | grep -A 10 "LITELLM_CHAT_MODEL_MAP"

# 检查备份文件
ls -lh /mnt/f/Dev/RD-Agent-main/git_ignore_folder/env_backups/
```

### 测试4：查看当前配置

**操作**：
1. 点击"当前配置"标签
2. 点击"刷新"按钮

**预期结果**：
- 显示当前.env文件中的配置
- 显示默认对话模型
- 显示嵌入模型
- 显示所有阶段映射
- 显示最后更新时间

### 测试5：查看变更记录

**操作**：
1. 点击"变更记录"标签
2. 查看最近5条记录
3. 点击"查看更多"
4. 使用分页控制

**预期结果**：
- 默认显示最近5条记录
- 每条记录显示：
  - 阶段名称
  - 旧模型 → 新模型
  - 变更原因
  - 变更时间
- 点击"查看更多"显示10条/页
- 分页控制正常工作

---

## 🔍 API测试

### 测试RD-Agent API

```bash
# 1. 健康检查
curl http://127.0.0.1:9000/llm-config/health

# 2. 获取当前配置
curl http://127.0.0.1:9000/llm-config/current-config

# 3. 验证模型（需要实际API Key）
curl -X POST http://127.0.0.1:9000/llm-config/verify-model \
  -H "Content-Type: application/json" \
  -d '{
    "provider_name": "deepseek",
    "model_name": "deepseek-chat",
    "full_model_id": "deepseek/deepseek-chat",
    "api_key": "sk-your-api-key",
    "model_type": "chat",
    "api_base": "https://api.deepseek.com"
  }'

# 4. 验证RD-Agent集成
curl -X POST http://127.0.0.1:9000/llm-config/verify-rdagent
```

### 测试AIstock API

```bash
# 1. 健康检查
curl http://127.0.0.1:8001/api/v1/rdagent/llm-config/health

# 2. 获取服务商列表
curl http://127.0.0.1:8001/api/v1/rdagent/llm-config/providers

# 3. 获取模型列表
curl http://127.0.0.1:8001/api/v1/rdagent/llm-config/models

# 4. 获取阶段映射
curl http://127.0.0.1:8001/api/v1/rdagent/llm-config/stage-mappings

# 5. 获取变更记录
curl "http://127.0.0.1:8001/api/v1/rdagent/llm-config/change-logs?limit=5&offset=0"

# 6. 获取当前配置
curl http://127.0.0.1:8001/api/v1/rdagent/llm-config/current-config
```

---

## 🐛 常见问题排查

### 问题1：RD-Agent API无法启动

**症状**：
```
ModuleNotFoundError: No module named 'rdagent.app.llm_config'
```

**解决方案**：
```bash
# 检查文件是否存在
ls -la /mnt/f/Dev/RD-Agent-main/rdagent/app/llm_config/

# 如果文件不存在，需要重新创建模块
# 确保以下文件存在：
# - __init__.py
# - models.py
# - env_manager.py
# - service.py
# - routes.py
```

### 问题2：AIstock API连接RD-Agent失败

**症状**：
```json
{"status":"degraded","error":"Failed to connect to RD-Agent API"}
```

**解决方案**：
```bash
# 1. 检查RD-Agent API是否运行
curl http://127.0.0.1:9000/llm-config/health

# 2. 检查AIstock配置
# 确保 RDAGENT_API_BASE = "http://127.0.0.1:9000"

# 3. 检查防火墙设置
# Windows防火墙可能阻止WSL和Windows之间的通信
```

### 问题3：数据库连接失败

**症状**：
```
sqlalchemy.exc.OperationalError: could not connect to server
```

**解决方案**：
```bash
# 1. 检查PostgreSQL服务
# Windows: 服务管理器中检查PostgreSQL服务

# 2. 检查数据库连接配置
# backend/.env 或 backend/config.py

# 3. 测试连接
psql -U postgres -d aistock -c "SELECT 1;"
```

### 问题4：前端组件导入错误

**症状**：
```
Cannot find module '@/components/ui/card'
```

**解决方案**：
```bash
# 1. 检查shadcn/ui是否安装
cd F:\Dev\AIstock\frontend
npm list @radix-ui/react-dialog

# 2. 如果未安装，安装shadcn/ui组件
npx shadcn-ui@latest add card
npx shadcn-ui@latest add button
npx shadcn-ui@latest add badge
npx shadcn-ui@latest add dialog
npx shadcn-ui@latest add input
npx shadcn-ui@latest add label
npx shadcn-ui@latest add select
npx shadcn-ui@latest add textarea
npx shadcn-ui@latest add tabs
npx shadcn-ui@latest add alert

# 3. 安装lucide-react图标库
npm install lucide-react
```

### 问题5：模型验证失败

**症状**：
```
模型验证失败: HTTP 401: Unauthorized
```

**解决方案**：
1. 检查API Key是否正确
2. 检查API Key是否有权限访问该模型
3. 检查API Base URL是否正确
4. 检查网络连接

---

## 📊 数据库管理

### 查看数据

```sql
-- 查看所有服务商
SELECT * FROM aistock_llm_providers;

-- 查看所有模型
SELECT m.*, p.display_name as provider_display_name
FROM aistock_llm_models m
JOIN aistock_llm_providers p ON m.provider_id = p.id;

-- 查看阶段映射
SELECT sm.*, m.display_name, m.full_model_id
FROM aistock_llm_stage_mapping sm
LEFT JOIN aistock_llm_models m ON sm.model_id = m.id;

-- 查看变更记录（最近10条）
SELECT * FROM aistock_llm_stage_change_log
ORDER BY created_at DESC
LIMIT 10;

-- 查看备份历史
SELECT * FROM aistock_env_backup_history
ORDER BY created_at DESC;
```

### 清理测试数据

```sql
-- 清空变更记录
TRUNCATE TABLE aistock_llm_stage_change_log CASCADE;

-- 清空模型数据
TRUNCATE TABLE aistock_llm_models CASCADE;

-- 重置阶段映射
UPDATE aistock_llm_stage_mapping SET model_id = NULL;

-- 清空备份历史
TRUNCATE TABLE aistock_env_backup_history CASCADE;
```

---

## 🔐 安全检查

### 1. API Key安全

- ✅ API Key在数据库中加密存储
- ✅ API Key在传输过程中使用HTTPS（生产环境）
- ⚠️ 前端不缓存API Key
- ⚠️ 日志中不记录API Key

### 2. 配置备份

```bash
# 检查备份目录
ls -lh /mnt/f/Dev/RD-Agent-main/git_ignore_folder/env_backups/

# 备份文件命名格式
.env.backup.20260205_123456

# 定期清理旧备份（保留最近30天）
find /mnt/f/Dev/RD-Agent-main/git_ignore_folder/env_backups/ \
  -name ".env.backup.*" -mtime +30 -delete
```

### 3. 权限控制

**当前状态**：
- ⚠️ 未实现用户认证
- ⚠️ 未实现操作审计

**生产环境建议**：
- 添加用户认证中间件
- 记录操作人和IP地址
- 实现角色权限控制

---

## 📈 性能监控

### 监控指标

1. **API响应时间**
   - RD-Agent API: < 500ms
   - AIstock API: < 200ms

2. **模型验证时间**
   - Chat模型: < 5s
   - Embedding模型: < 3s

3. **配置更新时间**
   - 包含验证: < 10s
   - 不含验证: < 2s

### 日志查看

```bash
# RD-Agent API日志
# 在WSL终端查看输出

# AIstock API日志
# 在PowerShell终端查看输出

# 前端日志
# 浏览器开发者工具 -> Console
```

---

## 🎯 下一步优化建议

### 短期优化（1-2周）

1. **前端优化**
   - [ ] 添加加载骨架屏
   - [ ] 优化错误提示样式
   - [ ] 添加操作确认对话框
   - [ ] 实现模型搜索过滤

2. **功能增强**
   - [ ] 支持批量导入模型
   - [ ] 支持配置模板保存
   - [ ] 添加配置对比功能
   - [ ] 实现配置导出功能

### 中期优化（1个月）

1. **性能优化**
   - [ ] 添加Redis缓存
   - [ ] 优化数据库查询
   - [ ] 实现API请求限流

2. **监控告警**
   - [ ] 集成Prometheus监控
   - [ ] 添加配置变更告警
   - [ ] 实现健康检查告警

### 长期规划（3个月）

1. **企业级功能**
   - [ ] 多租户支持
   - [ ] 审计日志系统
   - [ ] 配置版本管理
   - [ ] 自动化测试套件

2. **AI增强**
   - [ ] 智能推荐模型
   - [ ] 自动优化参数
   - [ ] 成本分析报告

---

## 📞 技术支持

### 文档位置

- 实施总结：`F:\Dev\RD-Agent-main\git_ignore_folder\rdagent_llm_config_implementation_summary.md`
- 技术设计：`F:\Dev\RD-Agent-main\git_ignore_folder\rdagent_llm_config_management_design.md`
- 部署指南：`F:\Dev\AIstock\RDAGENT_LLM_CONFIG_DEPLOYMENT_GUIDE.md`

### 代码位置

**RD-Agent侧**：
- 模块目录：`F:\Dev\RD-Agent-main\rdagent\app\llm_config\`
- API入口：`F:\Dev\RD-Agent-main\rdagent\app\results_api_server.py`

**AIstock侧**：
- 后端API：`F:\Dev\AIstock\backend\routers\rdagent_llm_config.py`
- 前端页面：`F:\Dev\AIstock\frontend\src\app\config\rdagent-llm\`
- 数据库脚本：`F:\Dev\AIstock\backend\database\init_rdagent_llm_tables.sql`

---

**部署完成时间**: 2026-02-05  
**版本**: v1.0  
**状态**: 生产就绪
