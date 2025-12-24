# QMT 模拟盘（xtquant + miniQMT）集成与安装配置指南（AIstock）

> 目标：在 **本机 Windows** 上运行 miniQMT（模拟盘/实盘均可），AIstock 通过 `xtquant` 读取 **资金** 与 **持仓**（并为后续下单执行预留接口）。  
> 本仓库已提供后端 API：`/api/v1/qmt/*` 用于查询状态、连接、资金与持仓快照。

---

## 1. 架构说明（直连阶段）

- **miniQMT**：独立 Windows 客户端（需要安装并登录账户，模拟盘/实盘取决于账户）
- **xtquant**：Python 库（在 AIstock 的 Python 环境里 `import` 使用），负责与本机 miniQMT 通信
- **AIstock 后端**：FastAPI（`backend.main:app`），提供 QMT API：
  - `GET /api/v1/qmt/status`
  - `POST /api/v1/qmt/connect`
  - `GET /api/v1/qmt/account`
  - `GET /api/v1/qmt/positions`
  - `GET /api/v1/qmt/snapshot`

> 说明：`xtquant` **不是独立服务**，你可以直接在 AIstock 进程里 import。后续若要更高可用，可以再演进为独立 gateway 进程。

---

## 2. miniQMT 准备（Windows）

1. 安装 miniQMT 客户端（按你的券商/迅投版本安装）。
2. 启动 miniQMT 客户端并登录账户（模拟盘账户或实盘账户）。
3. 确保该账户处于可查询状态（资金/持仓可在客户端界面看到）。

> 中文安装目录说明：一般情况下 `MINIQMT_USERDATA_PATH` 使用中文路径也能工作（Python 本身支持 Unicode 路径）。  
> 但由于 xtquant 含有 native 组件（`.pyd/.dll`）且不同版本实现差异较大，若你遇到“导入失败/连接失败/路径解析异常”，建议优先把 miniQMT 安装到纯英文目录（例如 `F:\QMT_SIM\`），再重试以排除路径编码因素。

---

## 3. xtquant 安装（推荐：独立 conda 环境）

### 3.1 建议的环境策略

建议为执行/查询专门建一个环境（避免策略侧依赖升级影响 xtquant）：

```powershell
conda create -n aistock-qmt python=3.10 -y
conda activate aistock-qmt
```

> Python 版本需要与 **你拿到的 xtquant wheel** 匹配（例如 cp39/cp310 等），且建议使用 **64 位 Python**。

### 3.2 本仓库已内置 xtquant（推荐直接复用）

本项目根目录已包含：`AIstock/xtquant/`（含 `.pyd/.dll` 与 `doc/` 文档）。  
因此 **不一定需要 pip 安装**，只要满足：

- 运行后端的 Python 版本与 `xtquant/` 内的 `.pyd` 匹配（例如有 `datacenter.cp310-win_amd64.pyd` 则需 Python 3.10 64 位；同理 cp311/cp312/cp313）。
- 后端通过 `backend.main` 启动时会将项目根目录加入 `sys.path`，从而可 `import xtquant`。

> 若你不是通过 `backend.main` 启动（例如单独跑某些脚本），需要确保项目根目录在 `PYTHONPATH` 中。

### 3.3 仍需 pip 安装的场景（可选）

如果你希望把 xtquant 安装到某个 conda 环境（而不是依赖项目内置目录），可以用以下方式。

### 3.4 获取 xtquant 安装包（常见两种方式）

`xtquant` 往往不在公网 PyPI 提供，通常来自 miniQMT 的安装目录或随软件分发的 wheel。

- **方式 A（推荐）**：从 miniQMT 安装目录中找到 `xtquant-*.whl`
  - 典型位置示例（不同版本可能不同）：
    - miniQMT 安装目录下的 `python/`、`sdk/`、`api/`、`wheel/` 等子目录
    - 或者厂商提供的“量化 SDK/xtquant 安装包”压缩包
  - 安装：

```powershell
pip install "D:\path\to\xtquant-*.whl"
```

- **方式 B（备选）**：拿到 `xtquant/` 源码目录，加入 `PYTHONPATH`
  - 不推荐长期使用（环境不够可控），但可快速验证：

```powershell
$env:PYTHONPATH="D:\path\to\xtquant_src;$env:PYTHONPATH"
python -c "from xtquant import xttrader; print('xtquant ok')"
```

### 3.5 验证 xtquant 可用

```powershell
python -c "from xtquant import xttrader, xtdata; print('xtquant import ok')"
```

若失败：
- 检查 Python 版本/位数与 wheel 是否匹配
- 检查 VC++ 运行库/依赖（如有要求）
- 确认当前 conda 环境已激活且 pip 安装在该环境内

---

## 4. AIstock 配置（.env）

在 AIstock 根目录 `.env` 中添加/确认：

```env
MINIQMT_ENABLED="true"
MINIQMT_ACCOUNT_ID="你的模拟盘账户ID"
MINIQMT_MODE="SIM"  # SIM=模拟盘, LIVE=实盘（用于标注与风控分流）
MINIQMT_USERDATA_PATH="D:\迅投极速交易终端...\userdata_mini"
MINIQMT_SESSION_ID="123456"
```

说明：
- `MINIQMT_USERDATA_PATH`：**必填**。按 `xtquant/doc/xttrader.md`，它是 miniQMT 安装目录下的 `userdata_mini` 路径。
- `MINIQMT_SESSION_ID`：建议配置一个固定整数。若不配置，后端会用 PID 自动生成，通常也能工作，但不利于排查。
- `MINIQMT_HOST/MINIQMT_PORT`：在本仓库当前 “xtquant 直连交易” 模式下通常不必使用（保留兼容字段即可）。

同时你也可以直接参考本仓库提供的示例文件：
- `docs/miniqmt_env_example.env`（复制到项目根目录并重命名为 `.env`）

---

## 5. 启动与调用（验证 QMT 模拟盘资金/持仓）

### 5.1 启动 AIstock 后端

```powershell
cd F:\Dev\AIstock
conda activate AIstock   # 或你的后端环境
uvicorn backend.main:app --host 127.0.0.1 --port 8001
```

> 重要：后端使用哪个 Python 环境运行，就必须在那个环境中能 `import xtquant`。  
> 如果你把 xtquant 安装在 `aistock-qmt` 环境，而后端跑在 `AIstock` 环境，则后端仍然会报 “xtquant 导入失败”。

### 5.2 API 验证

1. 查看状态：
   - `GET http://localhost:8001/api/v1/qmt/status`
2. 连接：
   - `POST http://localhost:8001/api/v1/qmt/connect`
3. 获取资金：
   - `GET http://localhost:8001/api/v1/qmt/account`
4. 获取持仓：
   - `GET http://localhost:8001/api/v1/qmt/positions`
5. 一次性快照：
   - `GET http://localhost:8001/api/v1/qmt/snapshot`

如果 `.env` 改了但进程未重启，可调用：
- `POST /api/v1/qmt/reload`

---

## 6. 常见问题（FAQ）

### 6.1 为什么 status 显示 provider=simulator？

通常是后端环境无法导入 `xtquant`：
- xtquant 没安装在当前后端 Python 环境中
- wheel 与 Python 版本/位数不匹配

### 6.2 AIstock 重启会影响 miniQMT 吗？

- AIstock 重启会导致 xtquant 连接对象消失，需要重连
- miniQMT 客户端通常不受影响（仍可继续运行）

### 6.3 模拟盘与实盘如何切换？

本仓库当前接口层面用：
- `MINIQMT_ACCOUNT_ID` 决定连接哪个账户
- `MINIQMT_MODE` 用于模式标注与后续风控分流（SIM/LIVE）

实盘强烈建议再加一层“硬风控”（比如白名单、最大单笔金额、冷却期等），避免误触发。

---

## 7. 下一步（可选）

- 在 AIstock 内实现“下单意图（intent）”落库与幂等，模拟盘先跑闭环（signal → intent → order → fill → position）
- 后期若需要高可用/多策略并发，将 xtquant 执行抽出为独立 gateway 服务（本机 localhost）并保持接口不变


