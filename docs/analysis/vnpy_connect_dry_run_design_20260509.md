# vn.py + vendored xtquant connect 层 dry-run 实证设计

> **作者**：engine-design teammate
> **日期**：2026-05-09
> **任务**：Task #17 (3) 修订版（按 Lead 2026-05-09 派单具体规格重写）
> **范围**：纸面设计 + step5 脚本设计稿；不写代码、不连真实账户、不下单
> **依赖**：
> - `vnpy_poc_result_20260508.md`（PoC 阶段 1 PASS — xtquant 直调闭环）
> - `qmt_vnpy_xt_recon_20260508.md`
> - `strategy_engine_design_20260508.md` §3.6.2"对接现有代码"列（方案 A vs 方案 B）
> - 现有 PoC：`backend/services/paper_trading_v2/poc/`
>   - `step0_env_check.py` / `step1_market_data.py` / `step2_place_cancel.py`（已 PASS）
>   - `step3_vnpy_smoke.py`（vnpy 装包烟测）
>   - `step3b_vendored_pythonpath_probe.py`（PYTHONPATH hack 引导 vendored xtquant 已通过）
>   - `step4_intraday_revalidate.py`（盘中复测，task #10）
>
> **本文档定位**：把 step3b 已验证的 "PYTHONPATH hack → vendored xtquant 被 vnpy_xt 吸纳"前提**继续向下推**——验证 `vnpy_xt.gateway.XtGateway.connect()` 在 dry-run 模式（不真连账户）下能否正确触发 init / on_connected callback / connect_status 流转。
>
> **预期产出**：`step5_vnpy_connect_dry_run.py`（不在本任务实施；env-poc 接力）+ 实施检查清单 + 预期 callback 序列 + fail 场景库 → 最终一份 dry-run 报告 + go/no-go 决策。

---

## 1. 设计目标与边界

### 1.1 目标

| # | 目标 | 验收 |
| --- | --- | --- |
| G1 | 在 PoC venv（`.venv-vnpy-poc/`）+ PYTHONPATH 引导 vendored xtquant 的前提下，验证 vnpy_xt 的 Gateway 能完成 init + connect 握手序列 | step5 PASS：观察到 `XtGateway.__init__` 完成 + `on_connected` callback 至少 1 次触发 + `connect_status` 转为 `True` |
| G2 | 验证 `connect()` 不需要真实下单 / 不需要订阅 / 不需要 query 即可建立基础会话 | step5 在仅调 connect → 等 N 秒 → close 路径下整体无异常 |
| G3 | 测量 vnpy_xt connect 路径触发的 callback 序列 + 时序，建立"参考 trace" | 报告内含 callback name 列表 + 时间戳间隔 + 每个 callback 的 payload shape 摘要 |
| G4 | 给 env-poc 一份可直接照做的实施检查清单（前置条件 / 运行命令 / 期望输出 / fail 处置） | 检查清单 ≥ 8 项；每条 fail 有处置建议 |
| G5 | 给 Lead 一份 go/no-go 决策建议（方案 A xtquant 直调 vs 方案 B vnpy_xt） | 决策矩阵 ≥ 4 行覆盖典型 PASS/FAIL 组合 |

### 1.2 不目标 / 严格禁止

- ❌ **不下任何真实订单**（`xttrader.order_stock` / `vnpy_xt` 同等接口必须不被调用）
- ❌ **不订阅行情**（不调 `xtdata.subscribe_quote` / vnpy `subscribe`）
- ❌ **不 query 资产 / 持仓 / 委托**（虽然 query 不会改变账户，但 connect 期触发 query 已超 dry-run 范围）
- ❌ 不评估 vn.py 完整 main_engine（多 gateway / event_engine / 风控 / GUI）— 与本任务正交
- ❌ 不修改主 `.env`（PoC 用 `.env.poc` 与 `.venv-vnpy-poc/` 隔离）
- ❌ 不评估性能基线（延迟 / 吞吐）— 留给 task #10 盘中复测

---

## 2. 前置条件（PoC 环境状态）

### 2.1 已具备（可直接复用）

```
F:\Dev\AIstock-worktrees\paper-v2-vnpy-mvp-20260508\backend\services\paper_trading_v2\poc\
├── .venv-vnpy-poc\           # PoC 专属 venv（含 vnpy + vnpy_xt）
│   └── Lib\site-packages\
│       ├── vnpy\
│       ├── vnpy_xt\
│       └── (no xtquant — 必须不装，由 PYTHONPATH 引导 vendored)
├── .env.poc                   # PoC 专属 env（账号 62266303 / userdata_path = F:\QMT_SIM\userdata_mini）
├── _common.py                 # PoC 共用：load_dotenv / sys.path 注入 / log helpers
├── step0_env_check.py         # ✓ PASS
├── step1_market_data.py       # ✓ PASS
├── step2_place_cancel.py      # ✓ PASS（含 36ms 下单延迟数据）
├── step3_vnpy_smoke.py        # vnpy 装包烟测
├── step3b_vendored_pythonpath_probe.py  # ✓ PASS（PYTHONPATH 引导 vendored xtquant 已验证）
└── step4_intraday_revalidate.py  # task #10 盘中复测
```

### 2.2 step5 需要的新增前置（实施前由 env-poc 检查）

| 项 | 检查命令 |
| --- | --- |
| `.venv-vnpy-poc/` 存在 + 激活 | `ls .venv-vnpy-poc/Scripts/python.exe`（Windows）|
| 该 venv 内 `pip list` 含 `vnpy`、`vnpy_xt`，**不**含 `xtquant` | `pip list \| grep -E "vnpy\|xtquant"` |
| `F:\Dev\AIstock\xtquant\` 目录存在（vendored xtquant） | `ls F:\Dev\AIstock\xtquant\xttrader.py` |
| `.env.poc` 含 `MINIQMT_USERDATA_PATH=F:\QMT_SIM\userdata_mini`（PoC §2.1 已确认正确路径）| `grep MINIQMT_USERDATA_PATH .env.poc` |
| `step3b` 上次跑过 PASS（确保 PYTHONPATH hack 仍然 work） | 复跑 step3b |
| miniQMT 仿真服务进程在运行（仅本 step 需要 — connect 必须能握手）| Windows 任务管理器看 `XtMiniQmt.exe` 在跑 |

---

## 3. step5 脚本设计稿

### 3.1 文件路径与运行方式

```
路径： backend/services/paper_trading_v2/poc/step5_vnpy_connect_dry_run.py
运行： cd <repo>/backend/services/paper_trading_v2/poc
       set PYTHONPATH=F:\Dev\AIstock              # Windows cmd
       .\.venv-vnpy-poc\Scripts\python.exe step5_vnpy_connect_dry_run.py
       # 或 bash:
       PYTHONPATH=F:/Dev/AIstock ./.venv-vnpy-poc/Scripts/python.exe step5_vnpy_connect_dry_run.py
```

### 3.2 脚本结构（伪代码 / docstring 化）

```python
"""Step 5 — vnpy_xt Gateway connect dry-run

Pre-conditions (assert at start, fail-fast if any missing):
  - .venv-vnpy-poc activated
  - vnpy + vnpy_xt installed in venv site-packages
  - xtquant NOT installed in venv (vendored takes precedence)
  - PYTHONPATH includes F:\\Dev\\AIstock (vendored xtquant)
  - .env.poc loaded (MINIQMT_USERDATA_PATH = F:\\QMT_SIM\\userdata_mini)
  - miniQMT SIM service process running

What this script does:
  1. boot: load env, sanity-check vendored xtquant resolves
  2. construct minimal vnpy event_engine (occupies its own thread)
  3. instantiate XtGateway(event_engine, gateway_name="XT_DRY_RUN")
  4. register tap callbacks on:
       - on_connected
       - on_disconnected
       - on_log
       - on_account / on_position / on_contract  (passive — only logs if fired)
  5. call gw.connect(setting={ ... SIM account ... })
  6. wait up to 10 seconds, recording every callback fired (name, ts, payload shape)
  7. assert connect_status == True (or False with explanation)
  8. call gw.close()
  9. emit summary (callback trace + assertions)

What this script does NOT do (strict):
  - NEVER calls subscribe / order / cancel / query_*
  - NEVER touches xttrader.order_stock or any trade-modifying API
  - NEVER subscribes to tick streams
  - on_account / on_position callbacks are just logged for shape audit;
    no value-level assertion (because dry-run does not load real positions)

Fail-fast:
  any pre-condition violation       → sys.exit(1) with explanatory message
  XtGateway init raises             → log full traceback + sys.exit(1)
  connect() raises                  → log full traceback + sys.exit(1)
  on_connected NOT received in 10s  → log connect_status + sys.exit(1)
  on_disconnected received before close → log + sys.exit(1)
  close() raises                    → log + sys.exit(1)

Output:
  stdout: human-readable trace (timestamps + callback names + payload shapes)
  step5_trace.json: machine-readable trace for the report writeup
"""

# ----- skeleton -----
def main() -> int:
    assert_pre_conditions()
    ee = _make_event_engine()
    trace = CallbackTrace()
    gw = _make_gateway(ee, trace)

    setting = _load_sim_setting()
    log("calling connect ...")
    gw.connect(setting)

    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if trace.has("on_connected") and gw.connect_status is True:
            break
        time.sleep(0.1)

    if not trace.has("on_connected"):
        log_error("on_connected NOT received in 10s")
        gw.close()
        ee.stop()
        return 1

    log(f"connect_status={gw.connect_status}; trace summary:")
    trace.dump()

    log("calling close ...")
    gw.close()
    ee.stop()

    trace.write_json("step5_trace.json")
    log("[step5] PASS")
    return 0
```

### 3.3 callback tap 实现规范

```python
class CallbackTrace:
    """Records callback name + ts + payload shape. NO payload values.

    payload shape = sorted(field names) + types (Pydantic-like introspection).
    Avoids leaking real account data (positions / cash) into the report.
    """

    def __init__(self) -> None:
        self.events: list[dict] = []

    def record(self, name: str, payload: object | None) -> None:
        shape = self._summarize(payload) if payload is not None else None
        self.events.append({
            "ts": datetime.utcnow().isoformat(),
            "name": name,
            "shape": shape,
        })

    def has(self, name: str) -> bool:
        return any(e["name"] == name for e in self.events)

    def dump(self) -> None:
        for e in self.events:
            print(f"  [{e['ts']}] {e['name']}  shape={e['shape']}")

    def write_json(self, path: str) -> None:
        Path(path).write_text(json.dumps(self.events, ensure_ascii=False, indent=2))

    @staticmethod
    def _summarize(payload: object) -> dict:
        # Use vars() / dataclasses.asdict / pydantic.dict() —
        # but ONLY return field names + types, not values.
        ...
```

---

## 4. 预期 callback 序列（reference trace）

基于 `step3b` 已验证的 vendored xtquant 路径 + miniQMT SIM 仿真账户特性，**期望** step5 看到以下 callback 序列（实际可能略有差异，需 env-poc 实测确认）：

```
T+0.0s   [INFO]   gw.connect(setting=...) called
T+0.0s   on_log   "vnpy_xt: initializing XtSession ..."
T+0.1s   on_log   "vnpy_xt: connecting to userdata_path F:\\QMT_SIM\\userdata_mini"
T+0.3s   on_log   "vnpy_xt: XtQuantTrader.connect() rc=0"
T+0.3s   on_connected   payload_shape=None  ← G1 关键断言
T+0.5s   on_log   "vnpy_xt: subscribing account event for 62266303"
T+0.6s   on_account   shape=AccountData{accountid, balance, ...}
T+0.7s   on_position   shape=PositionData{symbol, volume, ...}  (×N if 持仓 N 个)
T+0.8s   on_contract   (可能不来；miniQMT 不一定推送 contract)
T+5.0s   [INFO]   wait 5s elapsed; closing
T+5.0s   gw.close() called
T+5.1s   on_disconnected   payload_shape=None
T+5.1s   [INFO]   ee.stop()
T+5.2s   [step5] PASS
```

**关键不变量**：
- `on_connected` **必须**在 T+10s 内出现（否则 G1 fail）
- `gw.connect_status`（属性）应在 `on_connected` 后变 True
- `on_disconnected` 应仅在 `close()` 后出现，**不应在 connect 期间提前出现**
- `on_account` / `on_position` 出现是**好兆头**（说明 vnpy_xt 与 vendored xtquant 配合后能完整推送），但不出现也不算 fail（不同 miniQMT 版本行为可能差异）

---

## 5. fail 场景库

实施时若遇下列场景，按建议处置：

| # | 场景 | 症状 | 根因假设 | 处置 |
| --- | --- | --- | --- | --- |
| F1 | 前置条件不满足 | `assert_pre_conditions()` 抛 | venv 缺包 / xtquant 误装 / PYTHONPATH 未设 | 按 §2.2 检查清单逐项核对；step3b 不通过则 step5 不能跑 |
| F2 | `XtGateway` import 失败 | `ImportError: vnpy_xt.gateway` / `cannot import name XtGateway` | vnpy_xt 包结构变化（版本不匹配） | 报告 vnpy_xt 版本 + 实际包结构（`pip show vnpy_xt`）→ Lead 决定 |
| F3 | `XtGateway(event_engine=ee)` 抛 TypeError | 必填参数差异 | vnpy_xt 版本与 vnpy 主版本不匹配 | 调 `inspect.signature(XtGateway.__init__)` 报告所需参数；尝试匹配 |
| F4 | `gw.connect(setting)` 抛异常 | xtquant 错误码 -1 | userdata_path 错误 / miniQMT 服务未跑 / session 冲突 | 验证 PoC §2.1 修复（F:\QMT_SIM\userdata_mini）；查 miniQMT 进程 |
| F5 | `connect()` 不抛但 10s 内无 `on_connected` | 静默挂起 | vnpy_xt 内部回调链路未触达；可能事件转发缺失 | 检查是否有 `on_log` 触发；如有 log 但无 connected → vnpy_xt bug，倾向方案 A |
| F6 | 收到 `on_connected` 但 `connect_status=False` | 状态不一致 | vnpy_xt 状态机内部 race | 报告 + 标记 vnpy_xt 不可靠，倾向方案 A |
| F7 | 提前收到 `on_disconnected` | 握手中断 | miniQMT session 冲突（如另一进程已用同 session_id） | 改 session_id 重试；若仍失败 → SIM 账户问题 |
| F8 | 收到 `on_account` / `on_position`，但 payload shape 字段名与 PoC step2 实测的 xtquant 直调字段不一致 | shape 漂移 | vnpy_xt 重命名字段（balance vs total_asset 等） | 记录映射表（参 step3b probe 2 的字段对照）→ 用于方案 B 翻译层成本评估 |
| F9 | `close()` 阻塞 / 抛异常 | 关闭不干净 | vnpy_xt 内部资源释放 bug | 报告 + 标记 — 切换 portfolio 时风险高；倾向方案 A |
| F10 | 脚本结束后 .venv-vnpy-poc 残留 zombie 线程 | event_engine 线程未 join | `ee.stop()` 调用不完整 | 加 `ee.join(timeout=2)` + 强制 sys.exit(0) |

---

## 6. 实施检查清单（env-poc 直接照做）

env-poc 在跑 step5 前**逐项打钩**：

- [ ] **C1 venv 状态**：`.venv-vnpy-poc/` 存在；激活后 `python -c "import vnpy, vnpy_xt; print(vnpy.__version__, vnpy_xt.__version__)"` 输出版本号
- [ ] **C2 xtquant 不存在**：venv 内 `pip list | findstr xtquant` 返回空（注意 Windows findstr）
- [ ] **C3 vendored xtquant 可达**：`ls F:\Dev\AIstock\xtquant\xttrader.py`
- [ ] **C4 PYTHONPATH 设置**：`echo %PYTHONPATH%` 含 `F:\Dev\AIstock`
- [ ] **C5 .env.poc 正确**：`MINIQMT_USERDATA_PATH=F:\QMT_SIM\userdata_mini`（与 PoC 阶段 1 修复一致）
- [ ] **C6 step3b 复跑 PASS**：确保 PYTHONPATH hack 在当前会话仍 work
- [ ] **C7 miniQMT SIM 进程在运行**：任务管理器看 `XtMiniQmt.exe`
- [ ] **C8 SIM 账户空闲**：账户 62266303 当前没有其他进程占用 session 987654（若有，改 session_id）
- [ ] **C9 dry-run 守卫**：脚本内 hardcode `DRY_RUN=True`，并在所有可能下单路径前 `assert DRY_RUN`（防误触）
- [ ] **C10 输出文件路径**：`step5_trace.json` 写入 PoC 目录，且 .gitignore 已加（避免账户数据泄露）

实施成功后产出：
- `step5_vnpy_connect_dry_run.py` 落盘 PoC 目录
- `step5_trace.json` 包含完整 callback trace（脱敏，仅 shape）
- 报告文档：`docs/analysis/vnpy_dry_run_result_20260509.md`（占位，env-poc 接力创建）

---

## 7. 报告模板（dry-run 完成后 env-poc 产出）

`docs/analysis/vnpy_dry_run_result_20260509.md`（占位路径，本任务不创建）：

```markdown
# vnpy_xt connect 层 dry-run 报告

## 1. 执行摘要
| 项 | 结果 |
| --- | --- |
| step5 整体 | PASS / FAIL |
| on_connected 是否触达 | ... |
| close 是否干净 | ... |
| callback shape 与 xtquant 直调一致性 | ... / 100% / 80% |
| 是否推荐方案 B vnpy_xt | go / no-go |

## 2. 环境
- vnpy version
- vnpy_xt version
- vendored xtquant 路径
- Python / Windows / miniQMT 版本

## 3. 完整 callback trace（来自 step5_trace.json）

## 4. F# fail 场景命中（如有）

## 5. callback shape 对比表（vnpy_xt vs xtquant 直调）
（用于方案 B 翻译层成本评估）

## 6. 推荐方案
- A xtquant 直调（默认）
- B vnpy_xt + PYTHONPATH hack（仅在 step5 PASS + shape 映射 ≥ 80% 时考虑）

## 7. 下一步动作
- 回灌 strategy_engine_design §3.6.2 "对接现有代码" 列
- 决定 PR-014（task #17 (4) 计划）是否触发
```

---

## 8. 决策矩阵（dry-run 完成后用）

| step5 整体 | callback shape 一致性 | close 干净度 | 推荐 |
| --- | --- | --- | --- |
| PASS | ≥ 80% | 干净 | **方案 B vnpy_xt**（生态价值；PR-014 触发） |
| PASS | 50-80% | 干净 | **方案 A xtquant 直调**（翻译层成本超过 vn.py 收益） |
| PASS | 任意 | 不干净（F9） | **方案 A**（切换 portfolio 风险高） |
| FAIL F1-F4 | - | - | **方案 A**（环境/握手层不可靠） |
| FAIL F5-F7 | - | - | **方案 A**（vnpy_xt 内部状态机不可靠） |

**默认值**：在报告产出前，`strategy_engine_design §3.6.2` "对接现有代码"列保持"方案 A 推荐 + 方案 B 待 step5 验证"措辞。

---

## 9. 与现有 PoC 文档的关系

| PoC 阶段 | 范围 | 与本设计的关系 |
| --- | --- | --- |
| step0 env_check | 环境自检 | step5 前置条件 C1/C2/C3/C5 复用其逻辑 |
| step1 market_data | 行情订阅（xtdata） | 不复用；step5 严格不订阅行情 |
| step2 place_cancel | 下单/撤单（xttrader 直调） | 不复用；step5 严格不下单。但 step2 已建立的字段 shape（accountid / total_asset / market_value 等）作为 §5 F8 shape 对比基线 |
| step3 vnpy_smoke | vnpy 装包烟测 | step5 前置（vnpy import 必须 work） |
| step3b vendored_pythonpath_probe | PYTHONPATH 引导 vendored xtquant | **step5 关键前置**；step3b 不通过则 step5 不能跑 |
| step4 intraday_revalidate | 盘中复测（task #10） | 与 step5 正交（step4 用 xtquant 直调路径，step5 用 vnpy_xt 路径） |

---

## 10. 实施依赖与归属

| 项 | 归属 | 状态 |
| --- | --- | --- |
| 本设计文档 | engine-design teammate（本任务） | 交付（task #17 (3) 修订版） |
| step5 脚本编写 | env-poc teammate | 待派；依赖本设计 + step3b PASS |
| step5 执行 + 报告 | env-poc teammate | 盘后即可跑；不限交易日 |
| 报告 review + 决策 | Lead | step5 报告产出后 |
| §3.6.2 "对接现有代码"列回灌 | engine-design teammate（本人） | 报告产出后 |
| 是否触发 PR-014（方案 B 接入） | Lead 据决策矩阵判断 | 报告产出后 |

---

## 11. 风险 / 假设 / 限制

| # | 项 | 处置 |
| --- | --- | --- |
| R1 | dry-run 仅在盘后跑，可能错过盘中行情事件触发的 connect 路径分支 | 接受；盘中复测留给 PoC step4 task #10 |
| R2 | vnpy_xt 版本可能与 vnpy 主版本绑定（如 vnpy 4.x 仅兼容 vnpy_xt 4.x.y） | step5 输出版本，写入报告 |
| R3 | PySide6 依赖可能在 import vnpy 时被 lazy-load；headless 环境需 `QT_QPA_PLATFORM=offscreen` | step5 启动设环境变量；如不需要也无害 |
| R4 | vendored xtquant 与 vnpy_xt 内部期望的 xtquant 版本可能不一致 | step5 启动打印 `vnpy_xt.gateway.xt_gateway.xtquant.__file__` 确认引用的是 vendored |
| R5 | dry-run 守卫被绕过 → 误下单 → 破坏 SIM 账户 | C9 hardcode flag + 所有下单 API 前 assert；review 时强制看到 |
| R6 | `on_account` / `on_position` 推送的 payload 含真实账户余额 | trace.record() 仅存 shape 不存 value；step5_trace.json 与 .gitignore 配套 |
| R7 | step5 期间 miniQMT 服务崩溃 | F4/F7 处置；记录但不重试 |

---

## 12. 一句话总结

**step5 = 在 step3b（PYTHONPATH hack 已 PASS）基础上，验证 vnpy_xt Gateway 的 connect 握手是否真能产生 on_connected callback + connect_status=True**。dry-run 严格禁止下单 / 订阅 / query；仅做"哑联通"。10 fail 场景库 + 10 项检查清单覆盖实施期；产出 step5_trace.json + 独立报告 → 决策矩阵 → 回灌 §3.6.2 + 决定 PR-014 是否触发。

---

**End of vnpy connect dry-run design (修订版)**.
