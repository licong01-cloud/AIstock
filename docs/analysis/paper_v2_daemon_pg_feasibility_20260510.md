# Paper v2 Daemon → PG 迁移可行性 Audit (2026-05-10)

> 状态：可行性 audit，无代码改动。等用户拍板路径 A/B/C 后再起实施 PR。

## 背景

A2 capture-gaps audit (commit `d50d3c5`) 提出：`backend/services/paper_trading_v2/daemon/event_log.py` 把 9 类事件落到 worktree-local SQLite (`var/paper_v2_sim/daemon_events.db`)，DW ETL 看不到。本文档分析迁移到 PG (`paper_v2.daemon_event_log` 或复用 `paper_v2.run_events`) 的三条路径，并推荐其一。

参考：

- B1 ETL 设计草案：commit `dbafb0d` (`docs/analysis/paper_v2_dw_etl_scope_20260510.md`)
- A2 capture-gaps audit：commit `d50d3c5` (`docs/analysis/paper_v2_capture_gaps_20260510.md`)
- C1 live-inference capture audit：commit `88bc89c` (`docs/analysis/live_inference_capture_audit_20260510.md`)
- T1 silent-fallback fix：commit `c7dee33`（`feedback_no_silent_errors`，本批次内）

---

## §1 daemon entry point

**关键发现：当前没有"长驻 daemon 进程"，也没有 subprocess 派生。** `PaperV2SimRunner` 是同进程同步 runner，构造方传入已 `connect()` 的 `SimGateway`，`run_intents(intents)` 顺序处理一批 OrderIntent，函数返回即结束。

| 调用点 | 路径 | 说明 |
|---|---|---|
| CLI demo | `backend/services/paper_trading_v2/daemon/demo_run.py:134` (`runner = PaperV2SimRunner(...)`) | `python -m backend.services.paper_trading_v2.daemon.demo_run` 单次驱动一个 portfolio 的 intent batch；进程级隔离的"daemon"实质上是 demo_run 这个一次性进程。 |
| 集成测试 | `backend/tests/paper_trading_v2/test_daemon_sim_e2e.py:159` (`runner = PaperV2SimRunner(...)`) | 测试 fixture 直接构造，不经 service.py / runner.py / scheduler.py。 |

**FastAPI 应用未引用 `PaperV2SimRunner`**：

```
$ grep -r 'PaperV2SimRunner' backend/services/ backend/routers/ | grep -v daemon/
（无结果）
```

`backend/services/paper_trading_v2/scheduler.py:55` 出现的 `daemon=True` 是 `threading.Thread(daemon=True)` 标记位，与本 daemon 子包无关。

**结论**：本节"daemon entry point"等同于"`PaperV2SimRunner` 实例化点"。生产化（接入 FastAPI app / live trading）的 spawn 决策尚未做出，B1 选择会反过来影响这个决策。

## §2 environment variable propagation

由于 §1 显示 daemon 当前**与 FastAPI app 同进程**（test fixture）或**独立 CLI Python 进程**（`demo_run.py`），环境变量传播分两种 case：

- **Case A — 同进程（test / 未来 in-app 调用）**：直接共享父进程 env。`backend/db/pg_pool.py:97-101` 通过 `os.getenv("TDX_DB_HOST"/"TDX_DB_PORT"/...)` 读取连接信息，沿用现有 `.env` 加载逻辑（FastAPI app 启动时 dotenv 已加载）。无需额外传播。
- **Case B — 独立 CLI 进程（`demo_run.py` 当前形态）**：`os.environ` 完整继承自 shell。如果 shell 已 `source .env` 或 `python-dotenv` autoload，可读 `TDX_DB_*`；否则需要 `demo_run.py` 在 `main()` 入口显式 `from dotenv import load_dotenv; load_dotenv()`。当前 `demo_run.py` **未做 dotenv 加载**（`Read demo_run.py:1-50`），即默认无法连 PG。

**与 `subprocess.Popen(env=...)` 无关**：repo 内 `backend/services/paper_trading_v2/` 没有任何 spawn 调用（已验证）。如果将来 FastAPI app 需要 spawn daemon 子进程，应使用 `env=os.environ.copy()` 完整继承，与现有的 RDAgent worker spawn 风格保持一致。

## §3 daemon 依赖

`backend/services/paper_trading_v2/daemon/event_log.py:35-46` 的全部 import：

```python
import json
import sqlite3            # ← 唯一 DB 客户端
import threading
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4
```

- 无 `psycopg2` / `psycopg` / `asyncpg` / `sqlalchemy`。
- daemon 子包内**只有 `event_log.py` 和 `sim_runner.py` 触及持久化**；`sim_runner.py` 不直连 DB，全部经 `DaemonEventLog`。

**对迁移的含义**：
- 路径 A（直连 PG）需要在 daemon 包内**新增 `psycopg2` 依赖**（生产 `repository.py` 已用 `psycopg2.extras`，全局已是依赖，不引入新包）。但这破坏 daemon 子包当前"零 PG 依赖、可独立部署"的边界。
- 路径 B（sync worker）让 daemon 维持纯 sqlite 依赖，新进程承担 PG 写。
- 路径 C（dual-write）daemon 同时承担两种 client，复杂度最高。

## §4 startup ordering

当前形态下 startup ordering 不构成问题：

- **Test fixture**：DB pool 在 conftest 启动前就由 pytest 初始化；daemon 实例化时 PG 已就绪。
- **`demo_run.py`**：完全不接 PG（用 `_FakeMarketDataProvider`），所以连接顺序不存在。

**生产化后的考量**（决定路径 A 时必须答）：

1. FastAPI app 启动顺序为：dotenv → `pg_pool` 懒加载（首次 `get_conn()` 才连）→ uvicorn worker 起。daemon 写入若发生在 lifespan startup 之后，PG 必然 ready。
2. 如果 daemon 形态是"FastAPI app 内子任务"（asyncio task / threading），它**继承 FastAPI 的 PG pool**，无独立 startup 逻辑，最简洁。
3. 如果 daemon 形态变为"独立 systemd / supervisord 进程"，必须实现：
   - 启动时显式 `load_dotenv()`
   - PG retry-connect (max N 次 + 指数退避)，否则启动竞态会让一次失败永久 down
   - **当前代码无任何 retry 逻辑**——`pg_pool.get_conn()` 直接走 `psycopg2.connect`，连接失败即 `OperationalError` 抛出。这是路径 A 必须新增的代码。

## §5 9 类事件 schema

`event_log.py:49-65` 定义了 `DaemonEventType` 枚举。`event_log.py:81-98` 定义 SQLite schema。9 类事件通用列（`event_log.py:82-95`）：

```
id INTEGER PRIMARY KEY AUTOINCREMENT
run_id TEXT NOT NULL
portfolio_id TEXT NOT NULL
package_id TEXT NOT NULL
event_type TEXT NOT NULL
event_seq INTEGER NOT NULL              -- 单 run_id 内单调递增
event_ts TEXT NOT NULL                  -- ISO8601 with tz
handle_id TEXT                          -- order 类事件填，run 类为 NULL
intent_id TEXT
symbol TEXT
payload_json TEXT NOT NULL              -- 事件特定结构
UNIQUE(run_id, event_seq)
```

| 事件类型 | 写入点 (sim_runner.py 行号) | payload 关键字段 | 触发频率 |
|---|---|---|---|
| `RUN_STARTED` | `:118-126` | `manifest_package_id`, `manifest_status`, `intent_count`, `algo_code` | per run（低，1×/run） |
| `INTENT_CREATED` | `:201-212` | `side`, `quantity`, `order_type`, `limit_price`, `target_trade_date` | per intent（中，topk-n_drop 量级） |
| `ORDER_SUBMITTED` | `:251-266` | `state`, `filled_quantity`, `avg_fill_price`, `rejection_reason` | per intent（中） |
| `FILL_RECEIVED` | `:135-145`（订阅 callback）| `fill_quantity`, `fill_price`, `venue`, `fill_ts` | per fill（**高**：每次 partial fill 都触发；分钟级 LocalSim 单 intent 可产生多条） |
| `ORDER_REJECTED` | `:217-226` + `:230-239` | `error_code`, `message`, `context` | per rejected intent（异常路径，低） |
| `ORDER_CANCELLED` | 当前未触发（仅枚举值）| 同 ORDER_REJECTED 形态 | 暂未实现路径 |
| `POSITION_UPDATED` | `:154-166` | `positions: {symbol → {quantity, available_quantity, avg_cost}}` | per run（1×/run，run 末尾批量快照）|
| `RUN_COMPLETED` | `:167-174` | `submitted`, `rejected`, `fills_received` | per run（1×/run） |
| `RUN_FAILED` | `:178-186` | `error`, `submitted`, `rejected` | per run 异常分支（低） |

**写频估计**：单 portfolio / 单交易日，topk=20、n_drop=5 → ~25 INTENT + ~25 ORDER_SUBMITTED + 0~75 FILL_RECEIVED + 1 RUN_STARTED + 1 POSITION_UPDATED + 1 RUN_COMPLETED ≈ 50–130 events/run。10 个 portfolio 并行 → 500–1300 events/day。这是个**低 QPS** 场景，PG 单连接足够。

## §6 三条迁移路径对比

| 路径 | 方法 | 优 | 劣 | 代码改动量 |
|---|---|---|---|---|
| **A. daemon 直连 PG** | `event_log.py` 增加 `psycopg2` 路径，`record()` 改写 PG `INSERT`；schema 通过 `init_trading_core_v2_schema.py` 扩 `paper_v2.daemon_event_log` 表 | DW 单一真源；ETL 与现有 `paper_v2.*` 表完全一致；event_seq UNIQUE 直接靠 PG 约束；时序与 live 路径 (`paper_v2.run_events`) 完全可 join | 引入 PG 依赖到 daemon 子包；需要 retry-connect 逻辑（防 startup 竞态）；连接池容量与 FastAPI app 共享，多 portfolio 并发需评估 | **中–高**：`event_log.py` 重写约 60 行；`init_trading_core_v2_schema.py` 加 `daemon_event_log` 表 + 索引；新写迁移 SQL；测试 (`test_daemon_sim_e2e.py:145`) 改用 PG fixture 或 InMemory 适配；**新增** `_connect_with_retry` 至少 30 行 |
| **B. sync worker：sqlite→PG replay** | daemon 不动；新 `etl/daemon_event_replay.py` 进程或 cron 任务定期扫 `var/paper_v2_sim/daemon_events.db`，按 watermark (last replayed `id`) 复制到 PG | daemon 完全不动，零回归风险；故障隔离（PG 暂停只影响 ETL，不影响 sim）；与现有 DW ETL 风格一致（B1 草案已基于 ETL 思路）| 数据有 lag（取决于 cron 周期）；存在双存储期（sqlite 仍是真源前 N 分钟）；watermark + dedup 逻辑要新写；DB 文件需要从 worktree 临时位置改到稳定位置（`var/paper_v2_sim/` 当前 gitignored，多机部署难找） | **中**：`event_log.py` 不动；新增 `backend/services/paper_trading_v2/daemon/etl_replay.py` 约 150 行；新增 `paper_v2.daemon_event_log` schema + 迁移；新增 watermark 表 `paper_v2.daemon_event_replay_state`；新增 cron / scheduler hook；新测试 ~3 个 |
| **C. dual-write** | daemon 在 `record()` 内同时写 sqlite + PG | 后向兼容（旧消费方仍读 sqlite）；DW ETL 立即可见；切流可分阶段（先双写一段，验证后下线 sqlite）| 部分失败语义最复杂（sqlite 成功 + PG 失败 / 反之的处理：raise？吞？补偿？三种语义都有先例）；2× 写延迟；与 `feedback_no_silent_errors` 撞墙——任一边失败必须 raise，否则又回到 silent 半状态 | **中–高**：`event_log.py` 加约 50 行 PG 路径 + 错误传播策略；schema 与 A 相同；测试需覆盖 4 种失败矩阵（sqlite OK/Fail × PG OK/Fail）|

### 路径 A 详细 footprint

- 修改：`backend/services/paper_trading_v2/daemon/event_log.py`
- 修改：`backend/db/init_trading_core_v2_schema.py` 新增 `paper_v2.daemon_event_log` (列与 §5 一致 + `id BIGSERIAL`)
- 新增：`backend/db/add_paper_v2_daemon_event_log_<date>.sql`
- 修改：`backend/tests/paper_trading_v2/test_daemon_sim_e2e.py`（`DaemonEventLog(db_path=...)` 改用 `DaemonEventLog(conn_factory=...)`）
- 新增 retry helper：`event_log.py` 内私有函数

### 路径 B 详细 footprint

- 不动：`event_log.py`、`sim_runner.py`、所有现有测试
- 新增：`backend/services/paper_trading_v2/daemon/etl_replay.py`
- 新增：`backend/db/init_trading_core_v2_schema.py` 加表 (同路径 A) + `paper_v2.daemon_event_replay_state(db_path TEXT, last_replayed_id BIGINT, updated_at TIMESTAMPTZ)`
- 新增：cron entry / FastAPI lifespan hook 调度 replay
- 新增：测试 `test_daemon_event_replay.py`（增量、watermark、dedup、grace 时间）
- **运维**：sqlite 文件位置标准化（移出 worktree 到固定目录，加监控）

### 路径 C 详细 footprint

- 修改：`event_log.py` 增加 PG 路径，并在 `record()` 顶部声明事务策略（PG 优先 + sqlite mirror 还是反过来）
- 失败矩阵决策：建议 PG 失败时 raise（DW 是真源），sqlite 失败时 log + 继续（本地副本不该阻塞 sim 流）
- schema / 迁移同路径 A
- 测试覆盖 4 种失败矩阵 + 重启恢复语义

## §7 推荐：路径 A

**理由**：

1. **可逆性强**：路径 A 直接写 PG，回滚等价于"删 `paper_v2.daemon_event_log` 表 + revert event_log.py"；路径 B 一旦 sqlite 文件位置改了、监控接上、cron 上线，运维状态难还原；路径 C 双写期间一旦发现 PG 数据漂移，需要回滚整张表+ revert 双写逻辑，最复杂。
2. **与 live trading 路径风险隔离**：daemon 当前**不在 live 关键路径**（§1 显示 FastAPI app 未引用 `PaperV2SimRunner`）。改 daemon 的 PG 写不会影响 `live_session.py` / `day_runner.py` / `repository.py:493 create_run`。即"这是个内化测试/CLI 工具迁移"，不是 hot path 改造。
3. **与 B1 ETL 草案 (`dbafb0d`) 完美匹配**：B1 把 `paper_v2.*` 21 张表纳入 DW ETL 范围；路径 A 让 daemon 事件直接进 `paper_v2.daemon_event_log`，B1 ETL 不需任何额外改动就把它当成第 22 张表。**路径 B 反而强迫 B1 维护两套 ingest（PG 直读 vs sqlite replay）**，把 ETL 复杂度推回 DW 侧。
4. **写频低（§5 估计 500–1300 events/day）**，PG 单连接绰绰有余，无须 connection pooling 调优。
5. **与本批次 T1 fix (`c7dee33`) 同向**：T1 把 silent fallback 替换为 explicit propagation；路径 C 的 dual-write 在部分失败时容易再造一个 silent 区间，方向不一致。

**前置条件 / 风险**：

- 必须在 `event_log.py` 同时实现 retry-connect（指数退避 3 次），否则 startup 竞态会让 sim run 启动即崩。
- `init_trading_core_v2_schema.py` 加表需要走"提交迁移 SQL → 等用户拍 D4 批次执行"标准流程，不能 RTK 自动跑 (`feedback_no_service_start`)。
- daemon 子包的"零 PG 依赖、可独立部署"边界放弃；如果未来真要做"边缘部署 sim runner"再剥离，但这是远期假设，当前没人提需求。
- 现有测试 fixture (`test_daemon_sim_e2e.py:145` 用 `DaemonEventLog(db_path=tmp_path/...)`) 需要重构为 `conn_factory` 风格 + 内存 PG 或 testcontainer，工作量比 A 表面看到的大。如果团队偏好测试改动小，可以保留 sqlite 后端为可选 backend（`db_path` xor `conn_factory`），生产用 PG。

**对 B1 mode-A vs mode-B 偏好的影响**：B1 草案 (`dbafb0d`) 未明确二分 mode-A/B；本文档语境中"mode-A = ETL 直读 PG 表"、"mode-B = ETL 走中间 staging / message bus"。路径 A 让 daemon 事件落入与 `paper_v2.run_events` 同等地位的 PG 表，**直接强化 mode-A**（DW ETL 单一 ingest 模式）。路径 B/C 都会迫使 ETL 至少处理一种"非 PG 直读"路径，把复杂度推到 B1 侧。

---

## 附录 A — 相关代码引用

- `backend/services/paper_trading_v2/daemon/event_log.py:1-269`（全文件）
- `backend/services/paper_trading_v2/daemon/sim_runner.py:1-287`（全文件）
- `backend/services/paper_trading_v2/daemon/demo_run.py:104-105`（DB 路径定位）
- `backend/services/paper_trading_v2/daemon/__init__.py:11-20`（公开 API）
- `backend/db/pg_pool.py:93-101`（`TDX_DB_*` env var convention）
- `backend/services/paper_trading_v2/repository.py:493`、`:1616 save_run_event`、`:1624 save_error`（live 路径 PG 写入参考）
- `docs/analysis/paper_v2_capture_gaps_20260510.md:21-33`（A2 capture gaps 全表）
- `docs/analysis/paper_v2_dw_etl_scope_20260510.md`（B1 ETL 草案，commit `dbafb0d`）
