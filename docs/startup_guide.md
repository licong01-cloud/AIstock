# AIstock 本地启动说明（含 TDX Go 后端，端口 19080）

## 1. 环境与目录约定

假设 AIstock 根目录为：

- `F:\Dev\AIstock`  
（旧位置示例：`C:\Users\lc999\NewAIstock\AIstock`）

下面统一记为 `<AIstock_ROOT>`。

关键子目录：

- `<AIstock_ROOT>\backend`
- `<AIstock_ROOT>\frontend`
- `<AIstock_ROOT>\tdx-api-main`（Go 版 TDX HTTP 后端，源码在 `web` 子目录中）

确保本机已安装：

- Anaconda / Miniconda（包含 `AIstock` 或 `aistock` 环境）
- Node.js（建议 ≥ 18）
- Go（建议 ≥ 1.20）
- PostgreSQL / TimescaleDB（端口 5432）

---

## 2. 端口与配置总览

### 2.1 `.env` 中的主要配置

位于：`<AIstock_ROOT>\.env`

- **TDX API 后端地址**
  - `TDX_API_BASE="http://localhost:19080"`
  - AIstock 后端通过此地址访问 TDX Go 服务

- **数据库配置**
  - `TDX_DB_HOST="127.0.0.1"`
  - `TDX_DB_PORT="5432"`
  - `TDX_DB_NAME="aistock"`
  - `TDX_DB_USER="postgres"`
  - `TDX_DB_PASSWORD="******"`

- **其他端口**
  - `MINIQMT_PORT="58610"`（如启用 MiniQMT）
  - `SMTP_PORT="465"`（邮件）

### 2.2 各服务默认端口

- **TDX Go 后端（`tdx-api-main/web/server.go`）**
  - 环境变量：`TDX_HTTP_PORT`
  - 默认：`8080`
  - 本项目约定：显式设置为 **`19080`**，与 `.env` 中 `TDX_API_BASE` 对齐

- **AIstock 后端（FastAPI）**
  - 启动命令中指定：`--port 8001`
  - 访问：`http://localhost:8001`

- **Next.js 前端**
  - `npm run dev` 默认：`3000`
  - 访问：`http://localhost:3000`

---

## 3. 启动顺序总览

推荐启动顺序：

1. 启动 PostgreSQL / TimescaleDB（如果未常驻）
2. 启动 **TDX Go 后端**（端口 19080）
3. 启动 **AIstock 后端（FastAPI，端口 8001）**
4. 启动 **Next.js 前端（端口 3000）**

下面是每一步的详细命令。

---

## 4. 启动 TDX Go 后端（端口 19080）

TDX HTTP 服务入口：`<AIstock_ROOT>\tdx-api-main\web\server.go`

`server.go` 中的监听代码：

```go
port := os.Getenv("TDX_HTTP_PORT")
if port == "" {
    port = "8080"
}
addr := ":" + port
log.Fatal(http.ListenAndServe(addr, nil))
```

### 4.1 开发模式（推荐：在 `web` 目录中运行）

在 PowerShell 中：

```powershell
cd <AIstock_ROOT>\tdx-api-main\web

# 设置 HTTP 端口为 19080（与 .env 的 TDX_API_BASE 一致）
$env:TDX_HTTP_PORT = "19080"

# 在当前目录一次性编译并运行所有 Go 文件（server.go、tasks.go 等）
go run .
```

启动成功后日志会类似：

```text
服务启动成功，访问 http://localhost:19080
```

可用浏览器测试：

- `http://localhost:19080/api/health`
- `http://localhost:19080/api/quote?code=000001.SZ`

### 4.2 编译后运行（可选）

```powershell
cd <AIstock_ROOT>\tdx-api-main\web
$env:TDX_HTTP_PORT = "19080"

go build -o tdx_api .
.\tdx_api
```

---

## 5. 启动 AIstock 后端（FastAPI，端口 8001）

后端入口：`<AIstock_ROOT>\backend\main.py`，应用对象为 `backend.main:app`。

### 5.1 启动命令

在新的 PowerShell 窗口中：

```powershell
cd <AIstock_ROOT>

# 激活 Conda 环境
conda activate AIstock   # 或 aistock，视你的环境名而定

# 如暂时不需要启用 ingestion 调度，可选地关闭调度器（避免 DB 不可用时报错）
# $env:DISABLE_INGESTION_SCHEDULER = "1"

# 启动 FastAPI 后端
uvicorn backend.main:app --host 0.0.0.0 --port 8001
```

启动成功后可访问：

- 健康检查：`http://localhost:8001/api/v1/health`
- Qlib 相关接口：`http://localhost:8001/api/v1/qlib/...`

---

## 6. 启动 Next.js 前端（端口 3000）

前端目录：`<AIstock_ROOT>\frontend`

### 6.1 开发模式

在第三个 PowerShell 窗口中：

```powershell
cd <AIstock_ROOT>\frontend

# 安装依赖（首次或依赖有变更时执行一次）
# npm install

# 启动开发服务器
npm run dev
# 或使用 pnpm/yarn：
# pnpm dev
```

默认访问地址：

- `http://localhost:3000`

前端通过封装的 `buildBackendUrl` 访问 `http://localhost:8001` 的后端 API。

---

## 7. 运行检查与常见问题

### 7.1 端口冲突

- `19080` 被占用：
  - 可临时改用其他端口（如 `19081`）：
    - 设置：`$env:TDX_HTTP_PORT = "19081"`
    - 同时修改 `.env` 中的 `TDX_API_BASE="http://localhost:19081"`

- `8001` 被占用：
  - 后端改用其他端口，例如：
    ```powershell
    uvicorn backend.main:app --host 0.0.0.0 --port 8002
    ```
  - 如前端硬编码了后端地址，需要同步调整对应配置。

### 7.2 数据库未启动

- 若 AIstock 后端启动时日志有 `psycopg2.OperationalError: connection refused` 等错误：
  - 确认 PostgreSQL / TimescaleDB 已在 `127.0.0.1:5432` 运行；
  - 或暂时设置：
    ```powershell
    $env:DISABLE_INGESTION_SCHEDULER = "1"
    ```
    再启动后端，跳过调度器对 DB 的强依赖（其它 API 仍可用）。

---

## 8. 启动顺序速查（TL;DR）

1. **TDX Go 后端（19080）**
   ```powershell
   cd <AIstock_ROOT>\tdx-api-main\web
   $env:TDX_HTTP_PORT = "19080"
   go run .
   ```

2. **AIstock 后端（8001）**
   ```powershell
   cd <AIstock_ROOT>
   conda activate AIstock   # 或 aistock
   # 可选：$env:DISABLE_INGESTION_SCHEDULER = "1"
   uvicorn backend.main:app --host 0.0.0.0 --port 8001
   ```

3. **前端（3000）**
   ```powershell
   cd <AIstock_ROOT>\frontend
   npm run dev
   ```

按上述三步依次在三个终端窗口中执行，即可完成本地开发环境的完整启动。
