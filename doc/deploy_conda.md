# next_app 新架构（FastAPI + React）基于 conda 的部署与运行说明

本说明仅针对 **新应用**（`next_app` 目录下的 FastAPI 后端 + React/Next 前端），
不会修改或影响现有 Streamlit + tdx_backend 架构。

---

## 1. 准备工作

- 已安装 Anaconda/Miniconda
- 已安装 Git、Node.js（建议 >= 18）
- 当前代码仓库路径：`c:/Users/lc999/NewAIstock/AIstock`

---

## 2. 创建并激活 conda 虚拟环境

```bash
# 1）创建新环境（名称可以自定义，例如 aistock-next）
conda create -n aistock-next python=3.11 -y

# 2）激活环境
conda activate aistock-next
```

> 后续所有后端相关命令，均在激活后的 `aistock-next` 环境中执行。

---

## 3. 安装后端依赖（FastAPI + Uvicorn 等）

在仓库根目录执行：

```bash
cd c:/Users/lc999/NewAIstock/AIstock

# 基础 Web 依赖
pip install fastapi uvicorn[standard] pydantic "python-dotenv>=1.0.0"

# 如需访问 PostgreSQL/TimescaleDB，可根据现有项目 requirements 安装：
# 例如（请参考已有 requirements.txt）：
# pip install psycopg2-binary
# pip install pandas numpy
```

> 当前新后端骨架仅依赖 FastAPI + Pydantic + python-dotenv，
> 后续接入真实数据库和数据访问逻辑时，再按需要补充依赖。

---

## 4. 运行新 FastAPI 后端（端口 8001）

新后端应用入口：`next_app/backend/main.py`，应用对象为 `app`。

在仓库根目录执行：

```bash
cd c:/Users/lc999/aistock/aiagents-stock-main

uvicorn next_app.backend.main:app --host 127.0.0.1 --port 8001 --reload
```

启动成功后，可以访问：

- 健康检查：`http://127.0.0.1:8001/api/v1/health`
- 股票分析 stub：`POST http://127.0.0.1:8001/api/v1/analysis/stock`

示例请求体（可用 curl 或 Postman 测试）：

```json
{
  "ts_code": "000001.SZ",
  "start_date": "2024-01-01",
  "end_date": "2024-06-30",
  "holding_price": 10.5,
  "holding_ratio": 0.3
}
```

当前返回值为占位数据（stub），用于验证路由与架构无误，后续会接入真实分析逻辑。

> 注意：
> - 旧应用的 tdx_backend 仍然运行在 9000 端口，新后端使用 8001，二者互不影响；
> - 旧的 `run.py` 启动流程不需要任何修改。

---

## 5. 前端（React/Next.js）环境建议

前端代码将放在：`next_app/frontend/` 目录。

### 5.1 Node.js 与包管理器

建议：

- Node.js 版本：>= 18
- 包管理器：npm 或 pnpm（按个人习惯）

检查 Node 版本：

```bash
node -v
npm -v
```

### 5.2 初始化 Next.js 应用（示例）

> 以下仅为建议流程，新前端尚未在仓库中初始化，可在准备好时执行。

```bash
cd c:/Users/lc999/NewAIstock/AIstock/frontend

# 使用 create-next-app 初始化（以 TypeScript 为例）
npx create-next-app@latest frontend \
  --typescript \
  --eslint \
  --src-dir \
  --app \
  --import-alias "@/*"
```

初始化完成后，目录大致为：

```text
next_app/frontend/
  package.json
  next.config.js
  src/
    app/...
    components/...
    ...
```

### 5.3 配置前端调用新后端

- 在前端代码中，将 API 基础地址设为：`http://127.0.0.1:8001/api/v1`；
- 建议在 `src/lib/api.ts` 或类似位置封装 HTTP 客户端。

示例（使用 fetch）：

```ts
const API_BASE = process.env.NEXT_PUBLIC_API_BASE ?? "http://127.0.0.1:8001/api/v1";

export async function analyzeStock(payload: any) {
  const res = await fetch(`${API_BASE}/analysis/stock`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!res.ok) {
    throw new Error(`Request failed: ${res.status}`);
  }
  return res.json();
}
```

运行前端开发服务器：

```bash
cd c:/Users/lc999/NewAIstock/AIstock/frontend
npm install
npm run dev
```

访问：`http://localhost:3000`（或 create-next-app 提示的端口）。

---

## 6. 与旧应用的并行运行说明

- 旧应用启动方式（不变）：
  - `python run.py`
  - 会启动：
    - tdx_backend（默认 `http://127.0.0.1:9000`）
    - Streamlit 前端（默认 `http://127.0.0.1:8503`）

- 新应用启动方式（独立）：
  - 先激活 conda 环境：`conda activate aistock-next`
  - 启动新后端：`uvicorn next_app.backend.main:app --host 127.0.0.1 --port 8001 --reload`
  - 启动新前端（待初始化后）：`npm run dev` 在 `next_app/frontend` 目录。

- 端口规划：
  - 旧后端：9000
  - 旧前端：8503
  - 新后端：8001
  - 新前端：3000（或其它默认前端端口）

> 两个体系完全独立运行，可以在浏览器中分别访问旧 UI 与新 UI，
> 共享的仅有数据库与外部数据源，不共享进程与端口。

---

## 7. 后续扩展建议

- 当股票分析模块接入真实逻辑后，建议在本文件追加：
  - 必要的数据库迁移步骤；
  - 新增的 Python/Node 依赖列表；
  - 生产部署（如 gunicorn + nginx 或 docker-compose）示例。

- 当前文档定位为 **开发与本地运行指引**，
  目的是方便在独立 conda 环境中快速启动和调试新架构。
