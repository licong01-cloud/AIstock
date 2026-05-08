# QMT 配置核查 + vnpy_xt 社区情报（Task #2）

> 作者：env-poc teammate（Claude Code Opus 4.7）
> 时间：2026-05-08
> Worktree：`F:\Dev\AIstock-worktrees\paper-v2-vnpy-mvp-20260508`
> 上游 `.env`：`F:\Dev\AIstock\.env`（worktree 不带 untracked 文件，沿用主工作面）

---

## 1. miniQMT 配置盘点（脱敏）

来源：`F:\Dev\AIstock\.env` 第 48-58 行 `# MiniQMT量化交易配置`。

| 变量 | 当前值（脱敏） | 备注 |
| --- | --- | --- |
| `MINIQMT_ENABLED` | `false` | **⚠ 当前关闭。** 用户口头确认仿真服务已启动，但 .env 标记为关。PoC 启动前需用户确认是否要把它改为 `true`，或仅由 PoC 脚本本地覆盖。 |
| `MINIQMT_ACCOUNT_ID` | `622***03` | 仿真账号 ID（已脱敏前 3/末 2 位）。 |
| `MINIQMT_MODE` | `SIM` | 仿真盘。 |
| `MINIQMT_USERDATA_PATH` | `F:\QMT\QMT\userdata_mini` | miniQMT 客户端 userdata 目录，xtquant 连接必需。 |
| `MINIQMT_SESSION_ID` | `123456` | 与 `XtQuantTrader(path, session_id)` 构造相关；多 session 实例需要不同值。 |
| `MINIQMT_XTQUANT_DIR` | `""`（空） | 空则默认用 `<AIstock_ROOT>/xtquant`（见 `qmt_client.py:_resolve_xtquant_dir`）。 |
| `MINIQMT_CONNECT_TIMEOUT_SECONDS` | `15` | 连接超时秒数。 |
| `MINIQMT_HOST` | `127.0.0.1` | 本机连接。 |
| `MINIQMT_PORT` | `58610` | 默认端口。 |
| `QMT_TRADE_PASSWORD` | `2598**` | 交易密码（已脱敏后 2 位）。 |

**.env.example**（第 41-49 行）默认 `MINIQMT_ENABLED=true`，且示例 path 为 `F:/QMT_SIM/userdata_mini` —— 与生产 `.env` 路径不同，注意区分。

### 1.1 PoC 启动前需澄清

1. `MINIQMT_ENABLED=false` 但用户说仿真已启动 —— 是否需要 lead 通过用户确认 .env 修改/或 PoC 脚本只读环境变量并本地覆盖？
2. `MINIQMT_USERDATA_PATH=F:\QMT\QMT\userdata_mini` 是否就是当前正在跑的仿真服务对应目录？需要用户确认。
3. AIstock 根目录下应该存在 `xtquant/` 包（`MINIQMT_XTQUANT_DIR` 为空时 fallback），将由 PoC 步骤 0 验证存在。

---

## 2. AIstock 自研 `qmt_client.py` API 概览（仅参考，不复用）

文件：`F:\Dev\AIstock\backend\infra\qmt_client.py`（1199 行，本次只读前 200 行 + 函数清单 + xtquant 关键导入位点）。

### 2.1 类层次

```
BaseQMTClient（抽象接口）
  ├── SimulatorQMTClient（fallback：xtquant 不可用时返回零值）
  └── XtQuantQMTClient（xtquant 实现）
```

工厂：`build_qmt_client_from_env()`、单例 `get_qmt_client_singleton()` / `reset_qmt_client_singleton()`。

### 2.2 接口能力清单（`BaseQMTClient` + `XtQuantQMTClient`）

| 类别 | 方法 | 行号（XtQuant 实现） |
| --- | --- | --- |
| 连接 | `connect()` / `disconnect()` / `status()` | 302 / —— / 539 |
| 账户/持仓 | `get_account_info()` / `get_positions()` | 571 / 593 |
| 委托/成交 | `get_orders(cancelable_only=False)` / `get_trades()` | 632 / 675 |
| 下单 | `place_order(...)` | 981 |
| 撤单 | `cancel_order(order_id)` / `cancel_order_by_sysid(market, sysid)` | 1012 / 1025 |
| 历史/数据 | `get_local_data_range()` / `download_history_data()` / `download_financial_data()` | 712 / 817 / 886 |
| 任务跟踪 | `get_task_progress()` / `update_task_status()` | 919 / 924 |
| 行情/日历 | `get_latest_trading_day()` / `get_trading_calendar()` / `get_stock_list_in_sector()` | 931 / 972 / 963 |
| 新股/打新 | `query_new_purchase_limit()` / `query_ipo_data()` | 1037 / 1052 |
| 银证 | `bank_transfer_in/out` / `query_bank_info` | 1075 / 1091 / 1107 |

### 2.3 xtquant 调用关键位点

- `_resolve_xtquant_dir()`（252）：优先 `MINIQMT_XTQUANT_DIR`，回落到 `<repo_root>/xtquant`，并校验 `xttrader.py + __init__.py` 存在。
- `_ensure_xtquant()`（279）：lazy-import `from xtquant import xttrader, xttype`，失败抛 `QMTNotAvailableError`。
- 连接关键调用（349、398）：`XtQuantTrader(userdata_path, session_id)`（位置参数；用户数据路径 + 整数 session）。
- 签名约定写在注释里：`Per bundled doc: XtQuantTrader(path, session_id)`。

### 2.4 给 vnpy_xt 集成的启示

- xtquant 包必须随客户端版本配套（用 miniQMT 客户端「下载Python库」内置功能取，避免 pip 装不匹配版本）。
- session_id 必须是整数；多进程并发时需各自不同。
- userdata_path 必须是 miniQMT 客户端实际启动时用的 userdata 目录，不能任意填。
- xtquant 的 `XtQuantTrader` 启动后会拉起后台线程，连接失败时也可能残留（见 `connect()` 的清理注释）—— vnpy gateway 包装时需要保证 disconnect 干净。
- **不复用 `qmt_client.py` 代码**，但其 env 解析 + xtquant 路径 fallback 逻辑可作为 PoC 脚本的参照写法。

---

## 3. vnpy_xt 社区情报

### 3.1 仓库定位（关键纠偏）

`vnpy/vnpy_xt`（[GitHub](https://github.com/vnpy/vnpy_xt)）——**官方定位是迅投研「数据服务」接口**，不是交易 gateway 主线。但从 v3.9.2 起内置 `XtGateway`（实时行情），v3.9.4 加了涨跌停字段，**行情订阅可用**。

**交易下单**走两条路：
1. **官方 vnpy_xt 内 gateway** —— 社区实测可实盘，但「QMT 路径需要注释一行后缀」、「token 模式券商不支持，要改源码用 AccountId+QMT 路径连接」；建议自建 fork 包跑，不要改原文件。
2. **第三方 `ruyisee/vnpy_qmt`**（[GitHub](https://github.com/ruyisee/vnpy_qmt) / [Gitee](https://gitee.com/ruyisee/vnpy_qmt)）—— 专做 QMT trade gateway，pip 已发布；测试基于 vnpy 3.5；2026 年与 vnpy 4.x 兼容性需要自测。

### 3.2 版本演进

| 版本 | 关键变更 |
| --- | --- |
| 3.9.0 | 首次引入 vnpy_xt（数据服务） |
| 3.9.2（2024-07） | 增加 XtGateway 实时行情；xtdc 文件锁单例；适配 xtquant 240613.1.1 |
| 3.9.3 | 升级底层 API |
| 3.9.4 | 实时行情新增涨跌停价字段 |
| vnpy 4.0 | vnpy_xt 已完成 4.0 适配（带 ⬆️ 标记） |

明确 pin 的最后一个 xtquant 版本号是 **240613.1.1**；后续 vnpy_xt release 仅写「升级到当前版本」，需配套最新 miniQMT 客户端。

### 3.3 已知坑（社区反馈）

| # | 现象 | 影响 |
| --- | --- | --- |
| K1 | **Windows-only**：vnpy_xt 安装包硬限制 Windows，Linux `from xtquant import datacenter` 直接 ImportError；`os.symlink` 在 Linux 报 FileNotFoundError | 我们生产是 Win11 → 无影响；但要注意 PoC 脚本不要假定可在 Linux 运行 |
| K2 | **券商 QMT「只支持用户模式」**：vnpy_xt 连接券商 QMT 时报「服务器端只支持用户模式」，行情订阅失败，但下载历史数据可用 | 我们用迅投研 / miniQMT 仿真，预计无影响；若后期接券商版需重新评估 |
| K3 | **Token 模式券商不支持**：迅投研云 Token 与券商 QMT 接口不同步；接券商需改源码，用 AccountId + QMT path 模式连接 | 我们走 client 模式（AccountId + userdata_path），不受影响 |
| K4 | **xtquant 版本必须严格匹配本地 miniQMT 客户端**：从客户端「下载Python库」取包，pip 装的版本可能不匹配 | PoC 第 0 步必须验证 `<AIstock_ROOT>/xtquant` 与当前运行的 miniQMT 客户端配套 |
| K5 | **vnpy_xt 自带 gateway 路径处理需改源码**：源码里 QMT 路径加了后缀，实盘前要注释一行；建议自建 fork 而不是改 site-packages | 集成时若用 vnpy_xt 内置 gateway，需要预留补丁；如用 vnpy_qmt 则规避 |
| K6 | **xtquant 后台线程残留**：连接失败/反复 connect 会有线程泄漏（AIstock 自研 client 注释明确指出） | vnpy gateway 包装层 disconnect 必须显式 join/cleanup；测试要包含「失败重连」case |

### 3.4 选型建议（待 lead 拍板）

| 方案 | 优点 | 缺点 | 推荐度 |
| --- | --- | --- | --- |
| **A. vnpy + vnpy_xt（官方 gateway）** | 官方维护、4.0 适配、行情稳定 | 交易部分需小补丁；行情是主战场 | ★★★★（行情用） |
| **B. vnpy + vnpy_qmt（ruyisee）** | 专做 QMT 交易 gateway | 第三方维护、最后测试停在 vnpy 3.5、2026 兼容性未知 | ★★★（交易备选） |
| **C. vnpy + 自研薄 gateway 直调 xtquant** | 完全可控、复用 AIstock `qmt_client.py` 经验 | 工程量大、要重做撤单/回报路由 | ★★（最后兜底） |

**Day 1 PoC 建议**：先按 **A + 直接 xtquant 下单**（参考 AIstock `place_order`/`cancel_order` 实现，但不复用代码）—— 行情走 vnpy_xt XtGateway，下单先用 `xtquant.xttrader.XtQuantTrader` 直调验证连通性；vnpy_qmt 作为后备方案，在 A 方案行情稳定后再评估是否切到它做完整 vnpy 闭环。

---

## 4. 是否可启动 Task #3（vn.py PoC）

### 4.1 阻塞项

**无硬阻塞**。可启动。但启动前建议向 lead/用户确认：

1. **`MINIQMT_ENABLED=false` vs 「仿真已启动」的不一致** —— 是否同意 PoC 脚本本地覆盖该值（`os.environ`）而不改 .env 文件？
2. **`MINIQMT_USERDATA_PATH` 是否就是当前仿真服务对应路径**？错路径会导致 `XtQuantTrader` 构造失败。
3. **PoC 脚本目录** `worktree/backend/services/paper_trading_v2/poc/` 还不存在，PoC 启动时由我创建。
4. **pip 安装授权**：A2 已含 `pip install vnpy vnpy_xt`，但 vnpy_qmt 是否也允许装？若按 4.1 推荐方案 A，先只装 vnpy + vnpy_xt 即可。

### 4.2 PoC 步骤草案（待 #3 启动后细化）

0. 验证 `<AIstock_ROOT>/xtquant` 存在 + miniQMT 客户端版本配套
1. `pip install vnpy vnpy_xt` —— 记录解析到的版本号
2. 写最小连接脚本：构造 `XtQuantTrader(userdata_path, session_id)` → `start()` → `connect()` → 查询账户信息（仿真）
3. 订阅 1 只票（如 `600000.SH`）行情，记录 5 条 tick / minute bar
4. 提交 1 笔限价单（仿真，远离市价避免成交） → 拿 order_id
5. 查回报（status / orders）
6. 撤单 → 确认状态变化
7. 输出 `docs/analysis/vnpy_poc_result_20260508.md`，含全 traceback / 版本号 / 时长。

---

## 5. 关键文件路径

- 主 .env：`F:\Dev\AIstock\.env`（行 48-58）
- 自研 client：`F:\Dev\AIstock\backend\infra\qmt_client.py`（1199 行）
- xtquant 包预期位置：`F:\Dev\AIstock\xtquant\`（PoC 时验证存在）
- PoC 工作目录（待建）：`F:\Dev\AIstock-worktrees\paper-v2-vnpy-mvp-20260508\backend\services\paper_trading_v2\poc\`
- 本报告：`F:\Dev\AIstock-worktrees\paper-v2-vnpy-mvp-20260508\docs\analysis\qmt_vnpy_xt_recon_20260508.md`

---

## 6. 给 lead 的关键发现

1. **`MINIQMT_ENABLED=false` 是潜在阻塞**，需用户确认 PoC 启动方式（覆盖 env vs 改 .env）。
2. **vnpy_xt 是「数据接口」定位**，自带 XtGateway 行情可用；交易方面建议 PoC 先直调 xtquant，vnpy_qmt 留作后备评估。
3. **xtquant 版本必须配套本地 miniQMT 客户端**，PoC 第 0 步先验证 `<repo>/xtquant/` 存在与配套，避免 pip 装到不匹配版本。
4. **Windows-only 限制不影响我们**（生产 Win11），但 PoC 脚本/CI 不能假设跨平台。
5. **session_id 整数 + userdata_path 必须与运行中 miniQMT 实际目录一致**，否则 `XtQuantTrader()` 构造即失败。

判定：**Task #3 可在确认 5.1 的 4 个澄清项后启动**。
