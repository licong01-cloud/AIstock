# AIstock 与 RD-Agent 迁移到 F 盘方案（基于文件拷贝）

## 1. 目标与约定

- **目标**：将当前位于 `C:\Users\lc999\NewAIstock\AIstock` 下的 AIstock 项目，以及同一目录中的 `RD-Agent-main`，整体迁移到 **F 盘**，并保证：
  - 两个项目仍然使用各自独立的 git 仓库；
  - 迁移过程仅采用 **文件拷贝**（包括 `.git` 目录），**不通过 git clone 重新拉取**；
  - 迁移后 AIstock 与 RD-Agent 之间可以在同一 IDE 工作区中互相查看源码、共享数据路径。

- **新目录约定**：

  ```text
  F:\Dev\AIstock\          # AIstock 仓库根
  F:\Dev\RD-Agent-main\    # RD-Agent 仓库根
  ```

  后续如需统一数据目录，可以再规划：

  ```text
  F:\Data\qlib_data\       # （可选）统一的 qlib 数据根
  ```

- **IDE 工作区根推荐**：
  - 推荐在 VSCode 中直接打开 **`F:\Dev`** 作为工作区根目录，
  - 这样可以在一个工作区内同时浏览：
    - `F:\Dev\AIstock\...`
    - `F:\Dev\RD-Agent-main\...`

---

## 2. 迁移前准备

1. **停止所有相关服务**：
   - 关闭 TDX Go 后端窗口（`tdx-api-main/web`）；
   - 关闭 AIstock FastAPI 后端窗口；
   - 关闭 Next.js 前端窗口；
   - 关闭任何在使用 `C:\Users\lc999\NewAIstock\AIstock` 的 VSCode/终端。

2. **确认当前有效目录**：
   - AIstock 根：`C:\Users\lc999\NewAIstock\AIstock`
   - RD-Agent 根（当前在 AIstock 下）：`C:\Users\lc999\NewAIstock\AIstock\RD-Agent-main`

---

## 3. 在 F 盘创建目标目录

在 PowerShell 中执行（只需一次）：

```powershell
mkdir F:\Dev
mkdir F:\Dev\AIstock
mkdir F:\Dev\RD-Agent-main
```

如未来需要单独的数据区，可预留：

```powershell
mkdir F:\Data
mkdir F:\Data\qlib_data
```

---

## 4. 迁移 AIstock 仓库（使用文件拷贝）

### 4.1 使用资源管理器拷贝（简单方式）

1. 打开资源管理器，定位到：`C:\Users\lc999\NewAIstock\AIstock`
2. 复制整个 `AIstock` 文件夹到 `F:\Dev` 下，得到：`F:\Dev\AIstock`
3. 确认 `F:\Dev\AIstock` 下包含 `.git` 目录（说明 git 历史也已一起迁移）。

### 4.2 使用 robocopy 拷贝（更稳健，可选）

在 PowerShell 中执行：

```powershell
robocopy C:\Users\lc999\NewAIstock\AIstock F:\Dev\AIstock /MIR
```

> 说明：`/MIR` 会镜像源目录到目标，包括 `.git` 在内的所有文件。

无论使用哪种方式，目标是：

```text
F:\Dev\AIstock\   # 完整复制 C 盘 AIstock，包括 .git
```

---

## 5. 迁移 RD-Agent-main 仓库（使用文件拷贝）

RD-Agent-main 当前位于 AIstock 目录下：

- `C:\Users\lc999\NewAIstock\AIstock\RD-Agent-main`

### 5.1 粗粒度拷贝

同样使用资源管理器或 robocopy：

```powershell
robocopy C:\Users\lc999\NewAIstock\AIstock\RD-Agent-main F:\Dev\RD-Agent-main /MIR
```

迁移完成后期望结构：

```text
F:\Dev\RD-Agent-main\   # 含 .git 与全部源码
```

> 注意：本步骤仍然是**纯文件拷贝**，没有使用 `git clone`。

---

## 6. 更新 AIstock 的本地配置

迁移后，AIstock 的“.真实路径”变为 `F:\Dev\AIstock`。需要更新若干路径相关的配置文件。

### 6.1 更新 `.env` 中的路径

编辑：`F:\Dev\AIstock\.env`

重点修改以下条目：

```env
# RD-Agent 根路径（Windows 与 WSL）
QLIB_RDAGENT_ROOT_WIN="F:\Dev\RD-Agent-main"
QLIB_RDAGENT_ROOT_WSL="/mnt/f/Dev/RD-Agent-main"

# Qlib CSV / BIN 根路径（方案 A：仍放在 AIstock 仓库内）
QLIB_CSV_ROOT_WIN="F:/Dev/AIstock/qlib_csv"
QLIB_BIN_ROOT_WIN="F:/Dev/AIstock/qlib_bin"

# （可选）如果未来将 qlib 数据迁出仓库，则可改为：
# QLIB_CSV_ROOT_WIN="F:/Data/qlib_data/qlib_csv"
# QLIB_BIN_ROOT_WIN="F:/Data/qlib_data/qlib_bin"

# 公告 PDF 根目录，如有变更也一并调整
ANNOUNCE_PDF_ROOT="F:\\AIstockDB\\data\\anns"   # 示例，视实际目录而定
```

> 说明：
> - Windows 下的 `\` 在 `.env` 中建议写成 `\\` 或使用 `/`，避免转义问题；
> - WSL 路径 `/mnt/f/...` 需与实际挂载盘符对应。

### 6.2 更新一键启动脚本 `start_all_ai_stock.bat`

编辑：`F:\Dev\AIstock\start_all_ai_stock.bat`

将顶部根目录路径修改为：

```bat
set AIROOT=F:\Dev\AIstock
```

其余命令结构不变：

- TDX Go 后端：`%AIROOT%\tdx-api-main\web` + `TDX_HTTP_PORT=19080`
- AIstock 后端：`uvicorn backend.main:app --port 8001`
- 前端：`npm run dev`

### 6.3 更新启动文档中的默认路径（可选）

编辑：`F:\Dev\AIstock\docs\startup_guide.md`

- 将默认的 `<AIstock_ROOT>` 示例从 `C:\Users\lc999\NewAIstock\AIstock` 更新为 `F:\Dev\AIstock`；
- 可添加简短备注说明“已从 C 盘迁移到 F 盘”。

---

## 7. VSCode / IDE 工作区根路径建议

为了在开发中方便地 **同时查看 AIstock 与 RD-Agent-main 的源码**，并让 AI 助手也能引用 RD-Agent 实现细节进行分析，推荐：

- 在 VSCode 中选择 **`F:\Dev`** 作为工作区根目录：

  - 这样在资源管理器侧边栏会同时看到：
    - `F:\Dev\AIstock\...`
    - `F:\Dev\RD-Agent-main\...`

- 或者使用 VSCode 多根工作区（multi-root workspace）：

  - 将以下两个文件夹都加入工作区：
    - `F:\Dev\AIstock`
    - `F:\Dev\RD-Agent-main`

> 重要说明：即使 IDE 工作区根包含 `RD-Agent-main`，当前约定仍然是：**只在 AIstock 项目中进行代码修改**，RD-Agent-main 作为只读参考仓库，不在本次任务中修改其源码。

---

## 8. 在 F 盘验证运行

在 `F:\Dev\AIstock` 下，按如下步骤验证迁移是否成功：

### 8.1 启动 TDX Go 后端（端口 19080）

```powershell
cd F:\Dev\AIstock\tdx-api-main\web
$env:TDX_HTTP_PORT = "19080"
go run .
```

预期：终端输出“服务启动成功，访问 http://localhost:19080”。

### 8.2 启动 AIstock 后端（FastAPI, 端口 8001）

```powershell
cd F:\Dev\AIstock
conda activate AIstock   # 或 aistock

# 可选：若暂时不希望调度器访问 DB，可先关闭
# $env:DISABLE_INGESTION_SCHEDULER = "1"

uvicorn backend.main:app --host 0.0.0.0 --port 8001
```

预期：`/api/v1/health` 可正常访问。

### 8.3 启动 Next.js 前端（端口 3000）

```powershell
cd F:\Dev\AIstock\frontend
npm run dev
```

预期：浏览器访问 `http://localhost:3000` 可以正常打开前端页面，且页面中的后端请求均指向 `http://localhost:8001`，功能正常。

### 8.4 使用一键启动脚本（可选）

也可以在 `F:\Dev\AIstock` 直接执行：

```powershell
start_all_ai_stock.bat
```

批处理将自动在三个新窗口中依次启动：

1. TDX Go 后端（19080）
2. AIstock 后端（8001）
3. 前端（3000）

---

## 9. 保留与清理建议

- **短期内保留 C 盘旧版本**：
  - 建议保留 `C:\Users\lc999\NewAIstock\AIstock` 为只读备份一段时间；
  - 完全确认 F 盘运行稳定后，可以压缩归档旧目录并删除，以释放 C 盘空间。

- **记录本次迁移信息**：
  - 本文档即为迁移备忘录，推荐在后续有路径变更时继续更新；
  - 如未来将 qlib 数据迁出 AIstock 仓库，建议在本文件中追加说明，并更新 `.env` 示例配置。

---

## 10. 小结

- 本迁移方案只使用 **文件拷贝** 的方式，将 AIstock 与 RD-Agent-main 从 C 盘迁移到 F 盘，
  保留原有 git 历史与远程仓库配置；
- 推荐在 VSCode 中以 **`F:\Dev`** 为工作区根，使 AIstock 与 RD-Agent 仓库可以在同一工作区中联动开发、互相参考源码；
- 路径相关的关键配置集中在 AIstock 的 `.env`、`start_all_ai_stock.bat` 及 `docs/startup_guide.md` 中，迁移后需一并更新。
