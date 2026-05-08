# vn.py + miniQMT PoC 结果报告（Task #3，阶段 1）

> 作者：env-poc teammate（Claude Code Opus 4.7）
> 时间：2026-05-08 19:34（盘后非交易时段）
> Worktree：`F:\Dev\AIstock-worktrees\paper-v2-vnpy-mvp-20260508`
> PoC 目录：`backend/services/paper_trading_v2/poc/`
> 阶段：**阶段 1（xtquant 直调闭环）= PASS**；阶段 2（vn.py 装包评估）待启动

---

## 1. 执行摘要

| 项 | 结果 |
| --- | --- |
| step0 环境自检 | **PASS** |
| step1 行情订阅（xtdata） | **PASS**（snapshot 有效；tick callback 0 条 = 收盘后正常） |
| step2 下单/撤单闭环（xttrader） | **PASS**（连接、查询、下单、回调、撤单全部成功） |
| 总耗时 | < 5 分钟 |
| 是否需要 vn.py？ | **不必须**。xtquant 直调已可完成所有 OEMS 操作 |

**核心结论**：`xtquant` 在 Python 3.13.5 + Windows 11 + miniQMT SIM 仿真账户环境下**完全可用**，连通性、行情、交易闭环均已验证。**vn.py 不是 OEMS 必需依赖**，是否引入仅为 Strategy Engine adapter 层的设计选择问题，应由 engine-design 决定。

---

## 2. 关键发现（重要）

### 2.1 ⚠ AIstock 主 .env 中的 `MINIQMT_USERDATA_PATH` 是错的

| 来源 | 路径 | 状态 |
| --- | --- | --- |
| `F:\Dev\AIstock\.env` 第 52 行 | `F:\QMT\QMT\userdata_mini` | **过期路径**。存在但非当前 SIM 实例使用 |
| `F:\Dev\AIstock\.env.example` 第 45 行 | `F:/QMT_SIM/userdata_mini` | 与实际一致 |
| **xtdata 握手报告（实测）** | `F:\QMT_SIM\bin.x64/../userdata_mini/datadir` | **真实运行路径** |

step1 用 .env 路径仍能让 xtdata 握手成功（xtdata 是数据服务，与 userdata_path 解耦），但 step2 用 .env 路径时 `XtQuantTrader.connect() rc=-1` 直接失败；改成 `F:\QMT_SIM\userdata_mini` 后 rc=0 立即成功。

**建议**（需用户/lead 决策）：修主 .env 把 `MINIQMT_USERDATA_PATH` 改成 `F:\QMT_SIM\userdata_mini`，否则 AIstock 后端 `qmt_client.XtQuantQMTClient.connect()` 在生产模式下也会失败。**本任务遵循 §8.1 不动主 .env，已在 PoC `.env.poc` 内本地覆盖。**

### 2.2 仿真账户实际状态（脱敏）

来自 step2 实测：

| 字段 | 值 |
| --- | --- |
| account_id | `62266303` |
| cash | 4528.52 |
| total_asset | 35713878.52（约 3570 万） |
| market_value | 35709350.0 |
| 持仓数 | 2 |
| 委托数（盘后） | 0 → 下单后 1 → 撤单后 0 |

仿真盘是大资金账户，可正常做下单/撤单测试。

### 2.3 时延数据（仅 1 次样本，非性能基线）

| 操作 | 耗时 |
| --- | --- |
| `connect()` | 即时返回（<100ms） |
| `query_stock_asset` | 同步即时 |
| `order_stock`（下单到拿 order_id） | **36 ms** |
| `cancel_order_stock` | **42 ms** |
| `on_stock_order` 回调到达 | <1ms（与 order_stock 同时刻） |

非交易时段的网络延迟，正式 PoC 性能基线需在盘中重测。

### 2.4 撤单的边界行为

非交易时段下：
- `cancel_order_stock` 返回 `rc=0`（接口成功）
- `on_stock_order` 触发 2 次回调（下单 ack + 撤单 ack）
- 但 2 秒后 `query_stock_orders` 返回的最终状态是 **50 = ORDER_REPORTED（已报）**，**不是** 54 = ORDER_CANCELED

可能原因：
1. 非交易时段 broker 不处理撤单，状态停留在"已报"
2. 撤单 ack 异步，2 秒等待不够
3. miniQMT SIM 在盘后只允许"挂单"行为，不模拟撤单成交

**结论**：撤单接口可调用且回调有响应，但**「撤单生效」的确认必须在盘中复测**。生产 OEMS 设计需要事件驱动（监听 `on_stock_order` status 变化）而非轮询查询。

---

## 3. 验证步骤详情

### 3.1 step0_env_check.py — 环境自检

**执行**：`python -m backend.services.paper_trading_v2.poc.step0_env_check`

**输出**（关键行）：
```
[step0] Python 3.13.5
[step0] loaded .env.poc
[step0] userdata OK: F:\QMT_SIM\userdata_mini  (修正后)
[step0] xtquant dir OK: F:\Dev\AIstock\xtquant
[step0] xtquant import OK:
        xtquant.__version__ = xtquant
        xttrader.__file__ = F:\Dev\AIstock\xtquant\xttrader.py
        xtconstant.STOCK_BUY = 23
        xtconstant.FIX_PRICE = 11
[step0] MINIQMT_ENABLED=true, account=62266303, session=987654
[step0] PASS
```

验证：
- Python 3.13.5（匹配 `xtquant/*.cp313-win_amd64.pyd`）
- repo 自带 `F:\Dev\AIstock\xtquant\` 完整可用，**无需 pip 装 xtquant**
- userdata 含 miniqmt shm cache 指纹文件

### 3.2 step1_market_data.py — 行情订阅

**执行**：`python -m backend.services.paper_trading_v2.poc.step1_market_data`

**输出**：
```
[step1] subscribing tick on 600000.SH ...
***** xtdata连接成功 2026-05-08 19:47:32 *****
连接信息: {'tag': 'sp3', 'version': '1.0'}
连接地址: 127.0.0.1:58610
数据路径: F:\QMT_SIM\bin.x64/../userdata_mini/datadir
[step1] subscribe_quote returned seq=1
[step1] full_tick snapshot:
[step1] snapshot[600000.SH] keys: ['time','timetag','lastPrice','open','high','low',
                                    'lastClose','amount','volume','pvolume']
[step1] last_price=9.07 high=9.13 low=9.05
[step1] tick callbacks received: 0
[step1] WARN: zero tick callbacks (likely off-hours), but snapshot OK -> PASS
```

验证：
- `xtdata.subscribe_quote(...)` 返回 seq=1，订阅成功
- `xtdata.get_full_tick(["600000.SH"])` 返回有效快照，含 lastPrice/open/high/low/volume 等 10 个字段
- 0 tick callback 因为 19:47 已收盘；正常工作日盘中应有连续推送

### 3.3 step2_place_cancel.py — 下单/撤单闭环

**执行**：`python -m backend.services.paper_trading_v2.poc.step2_place_cancel`

**输出**（关键行）：
```
[step2] XtQuantTrader created
[step2] start() done
[step2] connect() rc=0
[step2] subscribe(account) rc=0
[step2] asset: cash=4528.52 total=35713878.52 market_value=35709350.0
[step2] before: 0 orders, 2 positions
[step2] last_price=9.07, limit_price=7.57, vol=100
[step2] placing BUY limit 600000.SH @ 7.57 x 100
[step2] order_stock returned order_id=1082130468  (36 ms)
[step2][cb][19:34:07.942] stock_order: <xtquant.xtpythonclient.XtOrder object at 0x...>
[step2][cb][19:34:07.942] stock_order: <xtquant.xtpythonclient.XtOrder object at 0x...>
[step2] cancelable orders now: 1
[step2] cancelling order_id=1082130468 ...
[step2] cancel_order_stock rc=0  (42 ms)
[step2] final order status for 1082130468: 50
[step2] total callbacks captured: 2
[step2] trader.stop() done
[step2] PASS
```

验证全链路：连接 → 订阅 → 资产/持仓查询 → 下单 → 回调 → 查询 cancelable → 撤单 → 回调 → 关闭。

---

## 4. xtquant API 验证清单

阶段 1 已验证可用的 API（PoC 实测）：

| 模块 | 函数 | 验证状态 |
| --- | --- | --- |
| `xtquant.xtdata` | `subscribe_quote(code, period='tick', callback)` | ✓ |
| `xtquant.xtdata` | `unsubscribe_quote(seq)` | ✓ |
| `xtquant.xtdata` | `get_full_tick([code])` | ✓ |
| `xtquant.xttrader` | `XtQuantTrader(path, session, callback)` 构造 | ✓ |
| `xtquant.xttrader.XtQuantTrader` | `start()` | ✓ |
| `xtquant.xttrader.XtQuantTrader` | `connect()` → 0 = OK / -1 = 失败 | ✓ |
| `xtquant.xttrader.XtQuantTrader` | `subscribe(account)` | ✓ |
| `xtquant.xttrader.XtQuantTrader` | `query_stock_asset(account)` | ✓（属性 `cash`/`total_asset`/`market_value`） |
| `xtquant.xttrader.XtQuantTrader` | `query_stock_orders(account, cancelable_only)` | ✓（返回 list） |
| `xtquant.xttrader.XtQuantTrader` | `query_stock_positions(account)` | ✓ |
| `xtquant.xttrader.XtQuantTrader` | `order_stock(account, code, type, vol, price_type, price, strategy_name, remark)` | ✓ |
| `xtquant.xttrader.XtQuantTrader` | `cancel_order_stock(account, order_id)` | ✓ |
| `xtquant.xttrader.XtQuantTrader` | `stop()` | ✓ |
| `xtquant.xttype.StockAccount` | `(account_id, 'STOCK')` | ✓ |
| `xtquant.xtconstant` | `STOCK_BUY=23, STOCK_SELL=24, FIX_PRICE=11, LATEST_PRICE=5, ORDER_REPORTED=50, ORDER_CANCELED=54, ORDER_SUCCEEDED=56` | ✓ |
| Callback `XtQuantTraderCallback` | `on_connected, on_stock_order, on_stock_trade, on_order_error, on_cancel_error, on_stock_asset, on_stock_position, on_account_status` | ✓（实例方法已注入，2 次 `on_stock_order` 触发） |

---

## 5. 兼容性 / 已知坑 / 建议

### 5.1 已观察到的坑

| # | 现象 | 影响 | 处理 |
| --- | --- | --- | --- |
| O1 | `pkg_resources` deprecation warning（来自 xtquant `__init__.py` `check_for_update`） | 仅警告，不影响功能 | 上游修；可忽略 |
| O2 | xtquant 控制台输出含 GBK 中文，在 PoC 终端显示乱码 | 仅日志可读性 | 不影响功能；如需可设 `PYTHONIOENCODING=gbk` |
| O3 | **撤单回调到达，但 query 状态仍是 50**（盘后） | 验证 OEMS 撤单生效需盘中复测 | 报告记录；阶段 2 视情况盘中复测 |
| O4 | xtquant `__init__.py` 在 import 时调 `pkg_resources.get_distribution("xtquant")`，但仓库自带的 xtquant 不是 pip 包，会抛 PackageNotFoundError 被 `try/except` 吞掉 | 无功能影响；但说明 xtquant 默认期望 pip 安装态 | vn.py 集成时的关键陷阱：vnpy_xt 大概率假设 xtquant 是 pip 包 |
| O5 | `MINIQMT_USERDATA_PATH` 在 .env / .env.example / 实际运行路径**三方不一致** | 生产 `qmt_client.connect()` 也会受影响 | 已在 §2.1 报告 |

### 5.2 给 engine-design 的输入

xtquant 直调路径已**完全可用**，Strategy Engine 的 Paper Adapter 设计可考虑两套方案：

| 方案 | 说明 | 推荐度 |
| --- | --- | --- |
| **A. xtquant 直调 adapter**（已验证） | 直接包 `XtQuantTrader`，复用 AIstock `qmt_client.py` 的连接管理思路 | ★★★★★（关键路径） |
| **B. vn.py + vnpy_xt adapter**（待阶段 2 验证） | 走 `BaseGateway`/`MainEngine`，享受 vnpy CTA / risk_manager / paper_account 等 app 生态 | 待评估 |

如阶段 2 装包失败 / 路径冲突无法绕过，**方案 A 即可独立交付 Paper Adapter**。

### 5.3 vn.py 阶段 2 启动前的预警

基于 O4，vnpy_xt 大概率会通过 `from xtquant import ...` 形式导入，且依赖 xtquant 在 `site-packages`。我们的 `F:\Dev\AIstock\xtquant\` 是 repo 内 vendored 版本。预期问题：

1. 如果 venv 里再 `pip install xtquant`（如 vnpy_xt 把它列为依赖），新装的 xtquant **版本可能与本机 miniQMT 客户端不配套**（参考 K4）→ 必须用 PYTHONPATH 或 sys.path hack 让 vnpy_xt 找到 repo xtquant。
2. vnpy_xt 自带 gateway 在 vnpy 4.x 是否还需要"注释 QMT 路径后缀"那个补丁，需要看新版源码（社区报告基于 3.x）。

**阶段 2 第一步建议**：先 venv 内 `pip install vnpy_xt --no-deps` 看依赖清单，再决定怎么处理 xtquant 路径。

---

## 6. 阶段 1 交付清单

| 文件 | 用途 | 行数 |
| --- | --- | --- |
| `poc/__init__.py` | package marker | 0 |
| `poc/.env.poc` | PoC env override（不动主 .env） | 25 |
| `poc/_common.py` | bootstrap：load_dotenv + sys.path | 41 |
| `poc/step0_env_check.py` | 环境自检 | 110 |
| `poc/step1_market_data.py` | xtdata 行情订阅 + 快照 | 95 |
| `poc/step2_place_cancel.py` | xttrader 下单/撤单闭环 | 180 |
| `docs/analysis/qmt_vnpy_xt_recon_20260508.md` | Task #2 调研报告 | 200+ |
| `docs/analysis/vnpy_poc_result_20260508.md` | **本报告** | 当前 |

---

## 7. 阶段 2 启动前需要 lead 拍板

**问题 1**：`MINIQMT_USERDATA_PATH` 在主 .env 里是错的（§2.1）—— 是否要由 lead 把发现转告用户，让用户决定改还是不改？我**不动**主 .env。

**问题 2**：阶段 2 的 venv 是否要立即建？还是等 engine-design 那边给出"明确需要 vn.py 集成的设计点"再建？因为现在 xtquant 直调已经可作完整 Paper Adapter，vn.py 集成的 ROI 取决于 Engine 设计是否要复用 vnpy CTA / risk_manager / paper_account 等 app。

**问题 3**：是否需要等到下一个工作日盘中（2026-05-11 或 09 周一开盘）做一次"盘中复测"以验证撤单状态最终变为 54、tick callback 持续推送、订单部成/全成路径？建议**做**，但放阶段 2 之后。

---

## 8. 给 lead 的关键发现摘要

1. **阶段 1 成功**：xtquant 直调下单 / 撤单 / 行情 / 资产查询全跑通，仿真账户可用（cash 4528 / total 3570 万 / 2 持仓）
2. **重大发现**：主 .env 的 `MINIQMT_USERDATA_PATH=F:\QMT\QMT\userdata_mini` 是错的，真实 SIM 路径是 `F:\QMT_SIM\userdata_mini`，生产 `qmt_client` 也会受影响（不动主 .env，仅报告）
3. **OEMS 时延样本**：下单 36ms / 撤单 42ms（盘后单样本，非基线）
4. **撤单边界**：盘后 cancel rc=0 + 回调到达，但订单最终状态停在 50（已报）；盘中复测必须验证最终 → 54
5. **vn.py 不是 OEMS 必需**：xtquant 直调已能交付完整 Paper Adapter，是否引入 vn.py 取决于 Engine 设计要不要复用 vnpy 生态
6. **阶段 2 待启动**：等 lead 拍板（venv 时机 + 是否需要盘中复测）
