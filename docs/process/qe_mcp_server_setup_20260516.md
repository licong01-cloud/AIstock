# QE MCP Server 接入配置说明（2026-05-16）

## 1. 文档目标

本文说明如何把 AIstock 新增的 QE MCP server 接入 Codex CLI、Codex App、Claude Code 和其他支持 MCP stdio 的 AI 工具。当前配置按用户要求默认指向本机生产后端端口 `8001`。

本次涉及三个 MCP server：

| MCP server | 脚本 | 后端地址 | 用途 |
|---|---|---|---|
| `aistock-validation` | `scripts/aistock_mcp_server.py` | `http://127.0.0.1:8001/api/v1/validation` | 自动化流水线、验证计划、BUG/运行记录查询与触发。 |
| `aistock-qe-experiment` | `scripts/aistock_qe_experiment_mcp_server.py` | `http://127.0.0.1:8001/api/v1` | QE 单次实验、自定义演进、待执行模板的查询、创建、校验、物化和确认执行。 |
| `aistock-qe-archive` | `scripts/aistock_qe_archive_mcp_server.py` | `http://127.0.0.1:8001/api/v1/qe-archive` | QE 数仓健康检查、历史回填、outbox/skip/job 查询、因子/模型/seed/超参历史统计。 |

## 2. 当前安全边界

1. MCP server 只作为 loopback HTTP 薄封装，不导入 QE scheduler、不直接写 DB、不直接调 RD-Agent workspace。
2. MCP 代码会拒绝非 loopback 地址，只允许 `127.0.0.1`、`localhost`、`::1`。
3. 写入或执行类工具必须提供确认 token，例如：
   - `QE_EXPERIMENT_RUN`
   - `QE_CUSTOM_EVO_RUN`
   - `QE_TEMPLATE_MATERIALIZE`
   - `QE_ARCHIVE_BACKFILL`
   - `QE_ARCHIVE_WORKER_RUN`
4. 本文只配置客户端，不重启生产后端 `8001`，也不重启前端 `3000`。
5. 如果生产 `8001` 尚未加载包含 QE MCP v1 后端接口的代码，MCP server 可以被 AI 工具加载，但部分工具会返回后端 `404`。尤其是 `/api/v1/qe-templates` 需要生产后端部署并加载本分支代码后才能完整可用。

## 3. 前置条件

### 3.1 Python 环境

推荐使用 AIstock conda 环境中的 Python：

```powershell
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -c "import mcp, httpx; print('ok')"
```

如果失败，安装依赖：

```powershell
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pip install mcp httpx
```

### 3.2 后端端口

按生产端口配置时，后端应监听：

```text
http://127.0.0.1:8001
```

只读 smoke 检查：

```powershell
Invoke-WebRequest -UseBasicParsing http://127.0.0.1:8001/api/v1/qe-archive/health
Invoke-WebRequest -UseBasicParsing http://127.0.0.1:8001/api/v1/qe-templates?limit=1
```

其中 `qe-archive/health` 用于确认数仓接口可用；`qe-templates` 用于确认本次 QE 待执行模板接口已经被生产后端加载。

## 4. 项目级 `.mcp.json`

仓库根目录提供项目级 MCP 定义文件：

```text
.mcp.json
```

当前内容使用生产端口 `8001`：

```json
{
  "mcpServers": {
    "aistock-validation": {
      "command": "python",
      "args": ["scripts/aistock_mcp_server.py"],
      "env": {
        "AISTOCK_VALIDATION_BASE_URL": "http://127.0.0.1:8001/api/v1/validation"
      }
    },
    "aistock-qe-experiment": {
      "command": "python",
      "args": ["scripts/aistock_qe_experiment_mcp_server.py"],
      "env": {
        "AISTOCK_QE_EXPERIMENT_BASE_URL": "http://127.0.0.1:8001/api/v1"
      }
    },
    "aistock-qe-archive": {
      "command": "python",
      "args": ["scripts/aistock_qe_archive_mcp_server.py"],
      "env": {
        "AISTOCK_QE_ARCHIVE_BASE_URL": "http://127.0.0.1:8001/api/v1/qe-archive"
      }
    }
  }
}
```

如果客户端从仓库根目录启动，且启动环境中的 `python` 已安装 `mcp` 和 `httpx`，可以直接复用该文件。

## 5. Codex CLI 配置

本机 Codex CLI 使用：

```text
C:\Users\lc999\.codex\config.toml
```

已添加以下 MCP server 配置，使用绝对 Python 和绝对脚本路径，避免重启后找不到环境：

```toml
[mcp_servers.aistock-validation]
command = "C:/Users/lc999/miniconda3/envs/AIstock/python.exe"
args = ["F:/Dev/AIstock_worktrees/qe-mcp-template-archive-20260516/scripts/aistock_mcp_server.py"]
cwd = "F:/Dev/AIstock_worktrees/qe-mcp-template-archive-20260516"
env = { AISTOCK_VALIDATION_BASE_URL = "http://127.0.0.1:8001/api/v1/validation" }
enabled = true

[mcp_servers.aistock-qe-experiment]
command = "C:/Users/lc999/miniconda3/envs/AIstock/python.exe"
args = ["F:/Dev/AIstock_worktrees/qe-mcp-template-archive-20260516/scripts/aistock_qe_experiment_mcp_server.py"]
cwd = "F:/Dev/AIstock_worktrees/qe-mcp-template-archive-20260516"
env = { AISTOCK_QE_EXPERIMENT_BASE_URL = "http://127.0.0.1:8001/api/v1" }
enabled = true

[mcp_servers.aistock-qe-archive]
command = "C:/Users/lc999/miniconda3/envs/AIstock/python.exe"
args = ["F:/Dev/AIstock_worktrees/qe-mcp-template-archive-20260516/scripts/aistock_qe_archive_mcp_server.py"]
cwd = "F:/Dev/AIstock_worktrees/qe-mcp-template-archive-20260516"
env = { AISTOCK_QE_ARCHIVE_BASE_URL = "http://127.0.0.1:8001/api/v1/qe-archive" }
enabled = true
```

修改后需要完全退出 Codex CLI，并重新打开新的会话。当前已经运行的会话不会自动出现新 MCP 工具。

## 6. Codex App 配置

为了兼容 Codex App 的全局配置，本机同时写入：

```text
C:\Users\lc999\.codex\config.json
```

配置内容如下：

```json
{
  "mcpServers": {
    "aistock-validation": {
      "command": "C:/Users/lc999/miniconda3/envs/AIstock/python.exe",
      "args": ["F:/Dev/AIstock_worktrees/qe-mcp-template-archive-20260516/scripts/aistock_mcp_server.py"],
      "cwd": "F:/Dev/AIstock_worktrees/qe-mcp-template-archive-20260516",
      "env": {
        "AISTOCK_VALIDATION_BASE_URL": "http://127.0.0.1:8001/api/v1/validation"
      },
      "transport": "stdio"
    },
    "aistock-qe-experiment": {
      "command": "C:/Users/lc999/miniconda3/envs/AIstock/python.exe",
      "args": ["F:/Dev/AIstock_worktrees/qe-mcp-template-archive-20260516/scripts/aistock_qe_experiment_mcp_server.py"],
      "cwd": "F:/Dev/AIstock_worktrees/qe-mcp-template-archive-20260516",
      "env": {
        "AISTOCK_QE_EXPERIMENT_BASE_URL": "http://127.0.0.1:8001/api/v1"
      },
      "transport": "stdio"
    },
    "aistock-qe-archive": {
      "command": "C:/Users/lc999/miniconda3/envs/AIstock/python.exe",
      "args": ["F:/Dev/AIstock_worktrees/qe-mcp-template-archive-20260516/scripts/aistock_qe_archive_mcp_server.py"],
      "cwd": "F:/Dev/AIstock_worktrees/qe-mcp-template-archive-20260516",
      "env": {
        "AISTOCK_QE_ARCHIVE_BASE_URL": "http://127.0.0.1:8001/api/v1/qe-archive"
      },
      "transport": "stdio"
    }
  }
}
```

修改后需要完全退出 Codex App，并确认后台没有残留 Codex 进程，再重新打开新窗口。当前已打开的 Codex App 会话不会自动刷新工具列表。

## 7. Claude Code 配置

Claude Code 可直接读取项目 `.mcp.json`，但需要在本机未提交文件中显式启用：

```text
.claude/settings.local.json
```

示例：

```json
{
  "enabledMcpjsonServers": [
    "aistock-validation",
    "aistock-qe-experiment",
    "aistock-qe-archive"
  ]
}
```

如果 Claude Code 启动环境中的 `python` 不是 AIstock conda 环境，可以在用户级 Claude 配置中改用绝对 Python 路径，或从已激活 `AIstock` conda 环境的终端启动 Claude Code。

## 8. 其他 AI 工具通用配置

任何支持 MCP stdio 的工具都可以使用以下通用配置。不同工具的配置文件位置不同，但 server 定义字段通常一致：

```json
{
  "mcpServers": {
    "aistock-qe-experiment": {
      "command": "C:/Users/lc999/miniconda3/envs/AIstock/python.exe",
      "args": ["F:/Dev/AIstock_worktrees/qe-mcp-template-archive-20260516/scripts/aistock_qe_experiment_mcp_server.py"],
      "cwd": "F:/Dev/AIstock_worktrees/qe-mcp-template-archive-20260516",
      "env": {
        "AISTOCK_QE_EXPERIMENT_BASE_URL": "http://127.0.0.1:8001/api/v1"
      },
      "transport": "stdio"
    },
    "aistock-qe-archive": {
      "command": "C:/Users/lc999/miniconda3/envs/AIstock/python.exe",
      "args": ["F:/Dev/AIstock_worktrees/qe-mcp-template-archive-20260516/scripts/aistock_qe_archive_mcp_server.py"],
      "cwd": "F:/Dev/AIstock_worktrees/qe-mcp-template-archive-20260516",
      "env": {
        "AISTOCK_QE_ARCHIVE_BASE_URL": "http://127.0.0.1:8001/api/v1/qe-archive"
      },
      "transport": "stdio"
    }
  }
}
```

## 9. 重启后的验收方法

重启 Codex CLI 或 Codex App 后，检查工具列表中是否出现：

```text
aistock-validation
aistock-qe-experiment
aistock-qe-archive
```

建议先调用只读工具：

1. `aistock-qe-archive/qe_archive_health`
2. `aistock-qe-experiment/qe_experiment_list`
3. `aistock-qe-experiment/qe_custom_evo_list_tasks`
4. `aistock-qe-experiment/qe_template_get`（已知模板 ID 时；前提是生产后端已经加载 `/qe-templates`）

不要用执行类工具做 smoke test，除非明确提供确认 token 并确认要真实执行。

## 10. 故障排查

| 现象 | 常见原因 | 处理方式 |
|---|---|---|
| MCP 工具列表里看不到 server | Codex/Claude 没有完全重启，或配置文件未被当前客户端读取 | 完全退出客户端和后台进程后重启；确认配置文件位置。 |
| server 启动失败，提示缺少 `mcp` | 当前 Python 不是 AIstock 环境 | 使用绝对路径 `C:/Users/lc999/miniconda3/envs/AIstock/python.exe`，或安装 `mcp httpx`。 |
| 工具调用返回连接失败 | `8001` 后端未启动 | 启动或恢复生产后端；本文不负责重启生产服务。 |
| `qe_archive_health` 可用但 `qe-templates` 返回 404 | 生产 `8001` 尚未部署或加载 QE MCP v1 模板接口 | 合入/部署本分支后，由用户确认是否重启生产后端。 |
| 执行类工具报确认字段错误 | 未提供确认 token | 按工具要求提供 `QE_EXPERIMENT_RUN`、`QE_CUSTOM_EVO_RUN` 等确认 token。 |

## 11. 本机已完成的配置

本次已完成以下本机配置：

1. `C:\Users\lc999\.codex\config.toml`：新增 `aistock-validation`、`aistock-qe-experiment`、`aistock-qe-archive`，全部指向 `8001`。
2. `C:\Users\lc999\.codex\config.json`：新增同名 MCP server，供 Codex App 兼容读取。
3. `F:\Dev\AIstock_worktrees\qe-mcp-template-archive-20260516\.mcp.json`：项目级 MCP 定义改为生产端口 `8001`。
4. 修改前已自动备份 Codex CLI 配置，备份文件位于 `C:\Users\lc999\.codex\config.toml.bak_*_qe_mcp_prod`。
