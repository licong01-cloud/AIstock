# Paper Trading v2 PoC — vn.py + miniQMT 接入验证

> 状态：阶段 1 + 阶段 2 + Mitigation A 实证 完成（2026-05-08）
> 上下文文档：
> - `docs/discussion/agent_teams_session_handoff_20260508.md`（隔离边界 §3 / 禁区 §8.1）
> - `docs/analysis/qmt_vnpy_xt_recon_20260508.md`（Task #2 调研报告）
> - `docs/analysis/vnpy_poc_result_20260508.md`（阶段 1 结果）
> - `docs/analysis/vnpy_integration_feasibility_20260508.md`（阶段 2 + v1.1 翻案结论）

---

## 1. 目录结构

```
poc/
├── README.md                          ← 本文件
├── .env.poc.example                   ← env 模板（committed）
├── .env.poc                           ← 真实凭证（gitignored，自己 cp 一份填）
├── .gitignore                         ← 忽略 .env.poc 与 .venv-vnpy-poc/
├── __init__.py
├── _common.py                         ← 公共 bootstrap：load_dotenv + sys.path
├── step0_env_check.py                 ← 环境自检（路径、xtquant 包、Python 版本）
├── step1_market_data.py               ← 阶段 1：xtdata 行情订阅 + 快照
├── step2_place_cancel.py              ← 阶段 1：xttrader 下单/撤单闭环
├── step3_vnpy_smoke.py                ← 阶段 2：vnpy + vnpy_xt 装包 / Gateway 实例化
├── step3b_vendored_pythonpath_probe.py ← v1.1 增量：PYTHONPATH 注入 vendored xtquant
├── step4_intraday_revalidate.py       ← Task #10：盘中复测（撤单 50→54、tick 流、部成/全成）
└── .venv-vnpy-poc/                    ← 阶段 2 隔离 venv（gitignored，~800 MB）
```

## 2. 启动前准备

### 2.1 凭证

```bash
cd backend/services/paper_trading_v2/poc/
cp .env.poc.example .env.poc
# 编辑 .env.poc，填 MINIQMT_ACCOUNT_ID 等真实值
```

`.env.poc` 已被 gitignore；`.env.poc.example` 是模板。

### 2.2 验证 miniQMT 在跑

PoC 假设本机 miniQMT SIM 客户端已登录并在跑。可以通过以下任一方式确认：
- 看 `MINIQMT_USERDATA_PATH` 下的 `down_queue_xtmodel-0` mtime 是否是当天
- 任务管理器搜 `xtminiqmt`

⚠ **重要**：主 `F:\Dev\AIstock\.env` 已被 lead 在 2026-05-08 修复，原 `MINIQMT_USERDATA_PATH=F:\QMT\QMT\userdata_mini` 是过期路径，现已改为 `F:\QMT_SIM\userdata_mini`。如要回退看历史，看 `F:\Dev\AIstock\.env.bak.20260508`。

### 2.3 阶段 1 用 conda 主环境（无须 venv）

阶段 1 直接调 vendored xtquant，不依赖 vnpy。conda 主环境的 Python 3.13.5 即可。

### 2.4 阶段 2 用独立 venv

```bash
cd backend/services/paper_trading_v2/poc/
python -m venv .venv-vnpy-poc

# 推荐用国内镜像装，PyPI 直连下 PySide6_Addons (128 MB) 容易卡死。
.venv-vnpy-poc/Scripts/python.exe -m pip install \
    --progress-bar off --default-timeout=300 --retries 5 \
    -i https://pypi.tuna.tsinghua.edu.cn/simple \
    vnpy vnpy_xt

# v1.1 推荐做法：用 vendored xtquant 替换 pip xtquant
.venv-vnpy-poc/Scripts/python.exe -m pip uninstall -y xtquant
```

阶段 2 装完后 venv 占 ~800 MB（PySide6 三件套 + vnpy 4.3 + vnpy_xt 1.4.6 + 50 个依赖）。venv 已 gitignore。

---

## 3. 运行顺序与预期输出

### 3.1 阶段 1（关键路径，已 PASS 2026-05-08）

| Step | 用什么 Python | 命令 | 预期 |
| --- | --- | --- | --- |
| step0 | conda 主环境 | `python -m backend.services.paper_trading_v2.poc.step0_env_check` | `[step0] PASS`，打印 xtquant 路径与常量 |
| step1 | conda 主环境 | `python -m backend.services.paper_trading_v2.poc.step1_market_data` | `[step1] PASS`，含 lastPrice 等快照字段；盘后 tick callback 0 条是正常 |
| step2 | conda 主环境 | `python -m backend.services.paper_trading_v2.poc.step2_place_cancel` | `[step2] PASS`，下单 ~36 ms、撤单 ~42 ms、2 个 stock_order 回调 |

阶段 1 结果详见 `docs/analysis/vnpy_poc_result_20260508.md`。

### 3.2 阶段 2（vn.py 集成可行性，已 PASS 2026-05-08）

| Step | 用什么 Python | 命令 | 预期 |
| --- | --- | --- | --- |
| step3 (v1.0) | venv | `cd poc && ./.venv-vnpy-poc/Scripts/python.exe step3_vnpy_smoke.py` | S1+S2+S3 PASS；S2 报告 xtquant 解析到 site-packages 的 pip 版（如未跑 v1.1 的 uninstall 步骤） |
| step3b (v1.1) | venv + PYTHONPATH | `cd poc && PYTHONPATH=F:/Dev/AIstock ./.venv-vnpy-poc/Scripts/python.exe step3b_vendored_pythonpath_probe.py` | P1+P2+P3 PASS；vnpy_xt 1.4.6 在 vendored xtquant 上 import + Gateway 加载 + 子模块 ABI 全部兼容 |

阶段 2 结果与 R1 翻案详见 `docs/analysis/vnpy_integration_feasibility_20260508.md`（v1.1）。

### 3.3 Task #10 盘中复测（待执行：下周一 09:30 后）

| Step | 用什么 Python | 命令 | 预期 |
| --- | --- | --- | --- |
| step4 | conda 主环境 | `python -m backend.services.paper_trading_v2.poc.step4_intraday_revalidate` | V1：撤单状态 50→54；V2：tick callback 30 秒内 ≥5 条；V3：部成/全成路径（55 / 56） |

非交易时段会 WARN 但 V1 仍可跑（下远价单会被 SIM 直接拒/挂着，撤单接口仍可调）。

---

## 4. 故障排查

| 现象 | 原因 / 修复 |
| --- | --- |
| `XtQuantTrader.connect() rc=-1` | userdata_path 错。验证 `MINIQMT_USERDATA_PATH` 是否就是当前 miniQMT 实际目录（看 xtdata 握手 banner 的「数据路径」） |
| `RuntimeError: subscribe_quote failed: seq=-1` | xtdata 未握手成功。确认 miniQMT 客户端在跑、端口 58610 没被占 |
| pip 装 vnpy 卡 PySide6_Addons 几分钟 0 字节 | PyPI 直连慢/限速。kill 进程，加 `-i https://pypi.tuna.tsinghua.edu.cn/simple` 用清华镜像 |
| step3b 的 `[PRE FAIL] xtquant still in venv site-packages` | 你没卸 pip xtquant。先 `./.venv-vnpy-poc/Scripts/python.exe -m pip uninstall -y xtquant` |
| `pkg_resources is deprecated` 警告 | 来自 `xtquant/__init__.py:check_for_update()`，不影响功能，是上游问题 |
| 终端输出乱码（中文） | xtquant 用 GBK 输出，bash 默认 UTF-8。功能不受影响；如需可读，`chcp 936` 或 `set PYTHONIOENCODING=gbk` |

---

## 5. 隔离边界（务必遵守）

- **绝不**改主 `F:\Dev\AIstock\.env`、`F:\Dev\AIstock\xtquant\`（vendored 包）、`F:\Dev\AIstock\backend\infra\qmt_client.py`（生产 client）
- **绝不**让 PoC 跑在生产 8001 后端进程内
- **session_id 必须**与生产 / .env 错开（默认用 987654，主 .env 用 123456）
- **限价单必须**远离市价（`POC_LIMIT_PRICE_OFFSET=-1.50`）以避免 SIM 上真成交
- step2 / step4 启动前确认 SIM 账户里没有大量自动策略在跑，避免与人共享回报通道

---

## 6. 后续工作

- **Task #10**（pending）：下周一盘中跑 step4 验证 V1/V2/V3
- 若决定走方案 B（vn.py + vnpy_xt）：扩 step5_vnpy_connect.py 验证 `gateway.connect(setting)` + 行情订阅 + 下单的 schema 兼容性（v1.1 已证 import 层兼容）
- 若 Engine Adapter 选 xtquant 直调：可参考 step1+step2 把代码搬到 `backend/services/paper_trading_v2/adapters/qmt_direct.py`
