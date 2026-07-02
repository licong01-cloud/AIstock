# MiniQMT 07-02 PRE_RUN_FAILED 多重交替失败只读 RCA（WSL QE 推理为主）

## 0. 只读边界

- 调查 worktree：`F:\Dev\AIstock_worktrees\miniqmt-prerun-multifailure-readonly-20260702`
- 分支：`analysis/miniqmt-prerun-multifailure-readonly-20260702`
- 调查对象：MiniQMT SIM account `62266303` / account group `ag_minqmt_62266303_sim`
- L2：`simrun_cc0ed89425aca9de`，binding `simbind_06efa40c99da8bc9`，package `pkg_a2f53f3f2f3e4095a910b939464c35e6`，manifest `77402e38e2cb215b213c7bd9e243bd2a74cdc855acb180cdbc5196b6916ef207`
- L16：`simrun_ecfeb56340adc33c`，binding `simbind_dcabd41bdbac1b1c`，package `pkg_378eb9c91e104c64935404e257e932ee`，manifest `2aae3560563bd669e5f1951c40ae939744f82a67be5b7479f239b9f910270300`
- 只读动作：代码阅读、API GET、MCP broker monitor GET、DB `readonly` transaction SELECT、文件/进程只读检查；未改代码、未启停服务、未重启 QMT/WSL、未 re-subscribe、未写生产 DB、未发/撤券商订单、未跑 operator/apply。
- DB 只读证据：`SHOW transaction_read_only = on`，查询后 `rollback()`。

> 证据限制：`paper_v2.simulation_daily_run.run_payload_json.pre_run_failure` 是 retry 原地覆盖字段，不是 append-only event stream。DB 中 `paper_v2.run_events` 对两条 `simrun_*` 计数为 0；因此无法从 DB 精确恢复每个 reason_code 的完整频次，只能用 `observed_count/first_observed_at/last_observed_at` 加 API/前序 RCA 的时间片样本还原。

## 1. 结论

07-02 MiniQMT L2/L16 已越过 BUG-567 manifest guard，当前失败发生在 broker submit 之前，且 `execution_plan_id=null`、`selection_evidence_id=null`、`broker_called=false`、`submitted_intents=0`。三类 pre-run blocker 不是 BUG-562/565/567 的回归，而是独立的运行期依赖问题：

1. **主阻塞：WSL live QE model inference failed**
   - 当前 WSL provider 不是远端 HTTP/RPC 服务，而是在 Windows backend 内通过 `wsl -d Ubuntu bash -lc ...` 启动本机 WSL 子进程，进入 `rdagent-gpu` conda env 后运行 `scripts/strategy_package_live_inference.py`。
   - 07-02 新 manifest 对应的 package-owned runtime workspace 已生成，`model/params.pkl` 也存在；失败不是 artifact 缺文件、不是网络超时、不是 binding/manifest 不一致。
   - 两个 package 的 `params.pkl` 均引用 `model.LSTM_10D_hs64_d02`，但 runtime workspace 没有 `model/model.py`，manifest 的 `model_asset.model_code_assets=[]` 且 `model_code_required=null`。`pickle.load()` 无法从 `model` namespace 解析 `LSTM_10D_hs64_d02`，抛 `AttributeError`。
   - 根因归类：**StrategyPackage asset freeze/backfill 后模型代码资产未被随模型权重自包含冻结，且 live inference preflight/prepare_workspace 对该 pickle 形态的 model-code 缺失未能 fail-fast，导致缺口延迟到 WSL 子进程 unpickle 阶段暴露。**

2. **并行阻塞：REALTIME_QUOTE_STALE**
   - 前序 RCA `F:\Dev\AIstock_worktrees\miniqmt-realtime-quote-stale-readonly-20260702\docs\handoff\miniqmt_realtime_quote_stale_readonly_rootcause_2026-07-02.md` 已确认：MiniQMT broker quote timestamp 会间歇前进，但持续落后 scheduler `as_of_time` 超过 `300s` fail-closed 阈值。
   - 本轮 12:40 再次观测到两条 run 最新值又被覆盖为 `REALTIME_QUOTE_STALE`：L2 `quote_timestamp=2026-07-02T11:30:00`、`quote_age=4258.25s`；L16 `quote_timestamp=2026-07-02T11:30:00`、`quote_age=4257.08s`。
   - 该 blocker 属 MiniQMT 行情/xtdata subscription freshness 链路，不属于 WSL/QE 推理链路。

3. **并行/瞬态阻塞：failed to load MiniQMT broker positions**
   - 该报错由 `_load_miniqmt_broker_positions()` 包装 `qmt_client.get_positions()` 异常产生，发生在 strategy-lot reconciliation 之前；不是 reconciliation 逻辑判定失败。
   - 当前 DB durable payload 已被后续 retry 覆盖，`simulation_daily_run.run_payload_json::text` 中未保留该字符串；当前 12:39 API/MCP snapshot 显示 QMT connected 且 positions=60，说明该位置加载失败不是持续性空持仓状态，更像 `query_stock_positions` 的瞬态异常/超时/连接抖动。
   - 所有持仓 `prev_close=0.0` 来自 xtquant position struct 未提供 `pre_close` 时的兼容字段，不是本轮 `failed to load positions` 或 `REALTIME_QUOTE_STALE` 的直接 reason_code。

综合判断：三类 blocker **不是同一底层根因**。WSL/QE 是 StrategyPackage runtime model-code materialization/import gap；quote stale 是 MiniQMT 行情新鲜度问题；positions load 是 MiniQMT broker position query 可用性问题。它们只是在同一 scheduler retry row 上交替覆盖，形成“多重交替失败”。

## 2. 交替失败全景与时间线

| 时间（Asia/Shanghai） | 证据来源 | L2/L16 状态 | 说明 |
|---|---|---|---|
| 09:10:30/31 | DB `simulation_daily_run.created_at` | 两条 run 创建为 `FAILED_RETRYABLE/PRE_RUN_FAILED` | `first_observed_at` 分别为 `2026-07-02T01:10:30.649717+00:00`、`2026-07-02T01:10:31.829932+00:00` |
| 09:52 左右 | 战略 session/前序 quote stale RCA | `REALTIME_QUOTE_STALE` | 例：`002049.SZ quote_timestamp=09:45:51`，`quote_age=414.76s > 300s` |
| 11:00-11:14 | 前序 quote stale RCA API samples | `REALTIME_QUOTE_STALE` 多次出现 | quote timestamp 从 `10:54:45/10:54:57` 前进到 `11:09:24`，但仍略超或大幅超 300s |
| 11:19 | 前序 quote stale RCA DB sample | 最新 payload 被覆盖为 `WSL live QE model inference failed`，`observed_count=82` | 证明同一 run row 在 stale 与 WSL blocker 间切换，而不是 append-only 多行 |
| 12:20-12:31 | 本轮 API/DB SELECT | 最新 payload 为 `WSL live QE model inference failed`，`observed_count=144->153` | 两条 run stderr 均含 `AttributeError: Can't get attribute 'LSTM_10D_hs64_d02' on <module 'model'>` |
| 12:39 | 本轮 QMT snapshot/API/MCP | QMT connected，account 正常，positions=60，`prev_close=0.0` 全持仓 | position query 当前可用，`prev_close=0.0` 持续存在 |
| 12:40-12:41 | 本轮 DB/API SELECT | 最新 payload 再次被覆盖为 `REALTIME_QUOTE_STALE`，`observed_count=165` | L2/L16 `quote_timestamp=11:30:00`，`quote_age≈4257-4258s` |

频次结论：`observed_count=165` 是同一 pre-run failure row 的累计观测次数，不是 WSL/quote/positions 的分项频次。由于 `paper_v2.run_events` 对这两条 run 为 0，且 `run_payload_json.pre_run_failure` 原地覆盖，当前无法只读恢复完整 per-reason count；只可确认 WSL 和 quote stale 均多次复现，positions load 至少在战略 session 观测过但当前 durable row 未保留。

## 3. WSL live QE model inference failed 根因

### 3.1 调用链与执行形态

代码锚点：

- `backend/services/simulation_runtime/scheduler.py:3550`：`_run_binding()` 先 `context_provider.load_context(...)`，再 `_run_selection_once_per_release(...)`，失败会被外层捕获为 pre-run failure。
- `backend/services/simulation_runtime/scheduler.py:5163`：`_run_selection_once_per_release(...)` 调用 `selection_service.run_selection(...)`，runtime_config 来自 release。
- `backend/services/simulation_runtime/selection.py:113`：`_ensure_authoritative_selection_artifact(...)` 在 selection 前确保 authoritative selection artifact；若 artifact 缺失且 `auto_generate=true`，会执行 live inference。
- `backend/services/simulation_runtime/selection.py:154`：先调用 `_require_live_inference_preflight(...)`。
- `backend/services/simulation_runtime/selection.py:155`：再调用 `selection_artifact_service.generate_from_live_inference(...)`。
- `backend/services/strategy_package/selection_artifact.py:425`：读取 package manifest，准备 runtime workspace。
- `backend/services/strategy_package/selection_artifact.py:441`：解析 live provider。
- `backend/services/strategy_package/selection_artifact.py:769`：Windows 默认 `inference_backend=wsl`，除非 runtime_config/env 改为 local。
- `backend/services/strategy_package/live_inference.py:2518`：`WslStrategyPackageInferenceProvider`。
- `backend/services/strategy_package/live_inference.py:2543-2566`：创建 `sp_live_inference_*` 临时 output，构造 `scripts/strategy_package_live_inference.py --runtime-workspace ... --trade-date ... --cutoff-date ...`，通过 `subprocess.run(["wsl", "-d", self.distro, "bash", "-lc", command])` 执行。
- `scripts/strategy_package_live_inference.py:113-142`：解析 `--runtime-workspace/--trade-date/--cutoff-date/--output-path`，调用 `InferenceEngine().run_inference(...)`。
- `backend/inference_engine.py:1270-1291`：`experiment_id + workspace_path` 进入 QE runtime cache mode，加载 runtime workspace manifest。
- `backend/inference_engine.py:1304-1306`：从 manifest 读取 `factor_entry_relpath` 与 `model_weight_relpath`。
- `backend/inference_engine.py:1353-1361`：把 `task_dir` 加到 `sys.path` 后调用 `load_model_from_pkl(model_file)`。
- `backend/inference_engine.py:198-211`：`load_model_from_pkl()` 只把 `model_file.parent` 加入 `sys.path`，随后 `pickle.load(f)`；当前异常就在这里抛出。

只读环境证据：

- `wsl -l -v` 显示 `Ubuntu` 与 `docker-desktop` 均为 `Running`。
- WSL provider 默认 `distro=Ubuntu`、`conda_env=rdagent-gpu`（`backend/services/strategy_package/live_inference.py:2530-2532`）。
- 本轮未执行 live inference runner，只读取已有 scheduler payload 与 artifact 文件。

结论：本次 “WSL live QE model inference failed” 不是独立常驻推理服务不可达；它是本机 WSL 子进程中的 Python unpickle/import 失败。

### 3.2 运行期 artifact 与 manifest 证据

DB 只读证据（12:30 左右）：

- L2 package `pkg_a2f53f3f...`：
  - `manifest_sha256=77402e38...`
  - `source_type=candidate_strategy_package`
  - `model_asset.model_id=__seed_LSTM_10D_hs64_d02__`
  - `model_asset.source_uri=qe-workspace://node/wsl2-5080/tasks/qe_20260601_172505_fe17/loops/Loop2/mlruns/artifacts/params.pkl`
  - `model_asset.sha256=336ac4c2ac0e7aa9a3679fca7a86e1e0c4995585b53e996e8ac6daf08f062a1b`
  - `model_code_required=null`
  - `model_code_assets=[]`
  - `strategy_pkg.package_asset` 中只有 `model_weight=1`，无 `model_code` 资产。
- L16 package `pkg_378eb9c9...`：
  - `manifest_sha256=2aae3560...`
  - `model_asset.model_id=__seed_LSTM_10D_hs64_d02__`
  - `model_asset.source_uri=qe-workspace://node/wsl2-5080/tasks/qe_20260520_215627_abbc/loops/Loop16/mlruns/artifacts/params.pkl`
  - `model_asset.sha256=186e2f2828055ad12bae62f11af8d3ab32db253871a51fcc10f7b52121f22978`
  - `model_code_required=null`
  - `model_code_assets=[]`
  - `strategy_pkg.package_asset` 中只有 `model_weight=1`，无 `model_code` 资产。

文件系统只读证据：

- L2 runtime workspace `F:\Dev\AIstock\rdagent_assets\strategy_package_runtime\pkg_a2f53f3f2f3e4095a910b939464c35e6\77402e38e2cb215b` 包含：
  - `factor_order.json`
  - `manifest.json`
  - `model\params.pkl`（680261 bytes）
  - `strategy_package_factor_entry.py`
  - **不包含 `model\model.py`**
- L16 runtime workspace `F:\Dev\AIstock\rdagent_assets\strategy_package_runtime\pkg_378eb9c91e104c64935404e257e932ee\2aae3560563bd669` 包含同类文件，`model\params.pkl` 为 766273 bytes，**也不包含 `model\model.py`**。
- 二进制只读搜索：两个 `model\params.pkl` 均包含字符串 `LSTM_10D_hs64_d02`，其他 runtime 文件不包含该字符串。
- runtime `manifest.json` diagnostics 显示：
  - `source_workspace_type=strategy_package_asset_store`
  - `model_params_origin=package_asset`
  - `model_candidate_count=1`
  - `model_source_path=...\mlruns\package_asset\artifacts\params.pkl`

解释：PR #1782 asset backfill 后，运行期不再依赖外部 QE workspace 作为权威源；`source_uri=qe-workspace://node/...` 是 provenance，不是当前 runtime authority。当前 runtime authority 是 package asset store，但该 store 只冻结了 `params.pkl`，没有随之冻结 pickled class 所需的 `model.py`。

### 3.3 异常详情

12:20-12:31 API/DB 观测到 L2/L16 最新 pre-run failure：

- `message=WSL live QE model inference failed`
- `reason_code=DATA_UNAVAILABLE`
- `data_source=MINIQMT_REALTIME`
- `returncode=1`
- L2 runner args：
  - `scripts/strategy_package_live_inference.py`
  - `--runtime-workspace rdagent_assets/strategy_package_runtime/pkg_a2f53f3f2f3e4095a910b939464c35e6/77402e38e2cb215b`
  - `--trade-date 2026-07-02`
  - `--cutoff-date 2026-07-01`
- L16 runner args：
  - `--runtime-workspace rdagent_assets/strategy_package_runtime/pkg_378eb9c91e104c64935404e257e932ee/2aae3560563bd669`
  - `--trade-date 2026-07-02`
  - `--cutoff-date 2026-07-01`
- stderr 核心栈：
  - `scripts/strategy_package_live_inference.py:135` 调 `InferenceEngine().run_inference(...)`
  - `backend/inference_engine.py:1236` 进入 `run_inference`
  - `backend/inference_engine.py:1361` 调 `load_model_from_pkl(model_file)`
  - `backend/inference_engine.py:211` 执行 `pickle.load(f)`
  - 抛 `AttributeError: Can't get attribute 'LSTM_10D_hs64_d02' on <module 'model' (<_frozen_importlib_external._NamespaceLoader object ...>)>`

### 3.4 为什么 preflight 没有提前挡住

代码里已有“应当挡住 model code 缺失”的设计：

- `backend/services/strategy_package/live_inference.py:427`：本地 pickle model module 集合为 `{"model"}`。
- `backend/services/strategy_package/live_inference.py:494-526`：`_require_model_code_for_pickled_local_modules(...)` 会扫描 `params.pkl`，若引用 `model` 且 workspace 缺 `model.py`，应抛 `DataUnavailableError(reason_code=strategy_package_model_code_missing)`。
- `backend/services/strategy_package/live_inference.py:1579-1586`：`prepare_workspace()` 在复制 `params.pkl` 后调用该检查。
- `backend/tests/strategy_package/test_runtime_package_assets_batch2.py:325-352`：测试期望 package asset `params.pkl` 引用 `model.LSTM_10D_hs64_d02` 且缺 `model.py` 时 fail-closed。
- `backend/tests/strategy_package/test_runtime_package_assets_batch2.py:354-382`、`backend/tests/strategy_package/test_freeze_completeness_build_gate.py:222-255`：测试期望存在 `model.py/helper.py` 时 materialize 到 `model/` 目录。

实际生产 payload 显示：

- `prepare_workspace()` 已成功生成 runtime workspace 和 `manifest.json`，说明未在 model-code preflight/prepare 阶段 fail-fast。
- 随后在 WSL 子进程 `pickle.load()` 才失败。

因此除数据缺失外，还有一个 guard gap：当前 pickle/torch serialization 形态下，`_require_model_code_for_pickled_local_modules()` 没有识别到缺失 `model.py`，否则错误应为 `strategy_package_model_code_missing`，而不是后续 `AttributeError`。

## 4. MiniQMT broker positions failure 根因

代码链路：

- `backend/services/simulation_runtime/scheduler.py:600-606`：MiniQMT context 入口 `_load_miniqmt_context(...)`。
- `backend/services/simulation_runtime/scheduler.py:632-636`：调用 `_load_miniqmt_broker_positions(qmt_client, ...)`。
- `backend/services/simulation_runtime/scheduler.py:637-652`：只有 broker positions 成功返回后，才从 strategy lots 投影并调用 `_reconcile_miniqmt_positions_with_broker(...)`。
- `backend/services/simulation_runtime/scheduler.py:1252-1283`：`_load_miniqmt_broker_positions()` 若 `qmt_client.get_positions()` 抛异常，会包装为 `DataUnavailableError("failed to load MiniQMT broker positions for strategy-lot reconciliation")`。
- `backend/infra/qmt_client.py:822-859`：`get_positions()` 调 xtquant `_trader.query_stock_positions(self._account)`，默认受 `MINIQMT_QUERY_TIMEOUT_SECONDS=2.0` 约束；异常被包装为 `QMTNotAvailableError("读取持仓失败: ...")`。

只读证据：

- 12:39 API/MCP `GET qmt/snapshot`：`connected=true`、`available_cash=24937348.53`、`total_asset=29478250.42`、`market_value=4722768.08`、`positions=60`。
- 12:39 API `GET qmt/status`：`connected=true`、`mode=SIM`、`account_id=62266303`、`last_error=null`。
- `simulation_daily_run.run_payload_json::text` 当前对 `failed to load MiniQMT broker positions` 搜索为 0 行；说明该失败已经被后续 WSL/quote retry 覆盖，DB row 不再保留原始 exception context。

判定：这条失败发生在 **QMT get_positions 查询阶段**，不是 strategy-lot reconciliation 算法对 positions 内容的业务失败。当前 positions 已可读，说明它不是持续性“持仓全丢失”；更可能是 xtquant position query 在某个 retry tick 上超时、异常或短暂不可用。因 durable payload 原地覆盖，当前无法只读给出当时底层 exception 的精确文本。

## 5. `prev_close=0.0` 根因与是否独立阻塞

代码锚点：

- `backend/infra/qmt_client.py:822-857`：`get_positions()` 组装持仓 dict。
- `backend/infra/qmt_client.py:844`：`current_price` 来自 `pos.last_price`。
- `backend/infra/qmt_client.py:845-846`：注释说明 xtquant 股票持仓结构当前未提供昨收，`prev_close` 用 `getattr(pos, "pre_close", 0.0)` 兼容，缺失时为 `0.0`。
- `backend/services/paper_trading_v2/broker/minqmtsim.py:1342-1378`：pre-trade quote 的 `pre_close` 是 quote row 字段，和 position snapshot 的 `prev_close` 不是同一字段来源。

只读证据：

- 12:39 QMT snapshot 中 positions=60，但样本与前序 RCA 均显示 `prev_close=0.0`。
- 当前 durable failure reason 在 WSL 与 `REALTIME_QUOTE_STALE` 间切换；没有观测到 `REALTIME_QUOTE_PRE_CLOSE_MISSING`。

判定：`prev_close=0.0` 是 QMT position metadata 不完整/兼容字段，和行情 stale 同向提示 MiniQMT 数据质量不完整，但不是本轮 pre-run fail 的直接 reason_code；也不是 `failed to load positions` 的直接根因。若后续逻辑依赖 position 昨收做风控/PnL，需单独评估，但本轮 pre-run 阻塞证据不指向它。

## 6. 历史对照

DB 只读对照：

- `market.trading_calendar`：`2026-06-24` 与 `2026-07-02` 均 `is_trading=true`，07-02 不是非交易日误判。
- 2026-06-24 MiniQMT SIM：
  - L2 `simrun_e7773697c31a56cb`：`SUCCEEDED`，`execution_plan_id=plan_5dc2ad4d23164845`，`broker_called=true`，`submitted_intents=2`。
  - L16 `simrun_f3c45da449252b7a`：`SUCCEEDED`，`execution_plan_id=plan_24ecc47e203fd525`，`broker_called=true`，`submitted_intents=43`。
- 2026-06-24 selection artifacts：
  - L2 old sha `b3fa7f6e...`：`SUCCEEDED`，`score_count=4893`，`inference_backend=wsl`。
  - L16 old sha `8f6d8b02...`：`SUCCEEDED`，`score_count=4893`，`inference_backend=wsl`。
- 2026-07-02 current sha `77402e38...` / `2aae3560...`：`strategy_pkg.selection_score_artifact` 对目标 package/trade_date 计数为 0。

解释：06-24 同一 L2/L16 slot 和 WSL inference 路径曾可完成 selection 并提交 MiniQMT；07-02 差异在于 package 已切到 PR #1782 asset backfill 后的 self-contained manifest + BUG-567 refreeze 后的新 binding。新 manifest 的 model weight 已冻结为 package asset，但未包含 pickled class 所需 `model.py`，因此 selection artifact 无法生成。

## 7. 是否同源

- WSL QE failure：依赖 StrategyPackage runtime artifact / WSL Python import namespace / model pickle class resolution。
- broker positions failure：依赖 MiniQMT xtquant `_trader.query_stock_positions` 查询与账户连接状态。
- quote stale：依赖 MiniQMT xtdata `get_full_tick` / quote cache freshness / subscription health。

三者不共享同一个底层 infra 依赖：WSL failure 即使 QMT 完全健康也会在 selection 阶段失败；quote/positions 即使 WSL inference 修复也可能在 context/pre-trade 阶段继续 fail-closed。当前“交替”来自 scheduler retry 对同一 `pre_run_failure` 字段的覆盖，不代表单一共因。

## 8. 自愈 vs 需处置

| 阻塞 | 自愈判断 | 建议处置方向（不实施） |
|---|---|---|
| WSL live QE model inference failed | 不会自然自愈。只要当前 self-contained package 缺 `model.py`，每次 unpickle 都会失败。 | 登记 StrategyPackage / live inference BUG：补齐模型代码资产冻结与 refreeze/rebuild；增强 preflight 对该 pickle/torch 形态的 model-code 缺失检测；必要时对已冻结 package 做受控 re-freeze/rebind。 |
| REALTIME_QUOTE_STALE | 可能间歇改善，但证据显示长期超 300s，不应依赖自然自愈。 | 延续前序 RCA 建议：受控检查/恢复 QMT/xtdata quote feed、订阅、行情权限或 backend quote health/self-heal。保留 300s fail-closed。 |
| failed to load MiniQMT broker positions | 当前已恢复可读；若复现，需要按 tick 取证。 | 建议增加 append-only/last-N pre-run failure evidence；检查 `MINIQMT_QUERY_TIMEOUT_SECONDS`、xtquant `query_stock_positions` 稳定性、QMT 客户端状态。 |
| `prev_close=0.0` | 不会由 retry 自愈；是 position struct 字段缺失/兼容默认。 | 不作为本轮 blocker 修；若需要昨收语义，应从 quote/kline 或专门持仓估值链路补，不应把 position `prev_close=0.0` 当作可用昨收。 |

## 9. 对 BUG-562/565/567 与 A event_loop 的影响

- BUG-562：当前两条 run `execution_plan_id=null`、`broker_called=false`、`submitted_intents=0`，失败在 broker side-effect 之前，不属于 RECONCILING/no-side-effect 恢复问题。
- BUG-565：当前没有进入 submit，非闭市 replay/时段门/跨日终结问题。
- BUG-567：当前 run 使用新 binding `simbind_06efa40c99da8bc9` / `simbind_dcabd41bdbac1b1c`，manifest sha 与当前 package sha 对齐；manifest identity guard 已越过。问题在更后面的 selection artifact/live inference 与 MiniQMT data gates。
- A event_loop：A/B 的差异在 submit 执行层；本轮三类 blocker 都发生在 submit 之前或 submit 前置数据 gate。A 上线不能绕过：
  - selection artifact live inference 仍是 A/B 共用前置；WSL/model-code 缺口会同样挡住 A。
  - MiniQMT quote freshness 也是 A event_loop 的 broker quote 前置依赖。`backend/services/miniqmt_execution_runtime/client.py:1354-1495` 要求 broker quote，并从 `quote_provider/qmt_client.query_quote/qmt_client.get_full_tick` 读取，quote 不新鲜时仍应 fail-closed。
  - MiniQMT positions/strategy-lot reconciliation 是统一 account-slot 语义前置，A 不应绕过。

## 10. 归类与建议给战略 session

建议拆为至少两个处置项：

1. **P1 / strategy_package + live_inference：package-owned model_weight 缺 model_code_assets 导致 WSL unpickle 失败**
   - 主修模块：`backend/services/strategy_package/package_asset_freeze.py`、`package_asset_backfill.py`、`live_inference.py`、相关 refreeze/rebind operator。
   - 修复方向：确保 pickled local model classes 的 `model.py`/helper.py 被纳入 `model_code_assets` 并在 runtime workspace materialize；增强 `_require_model_code_for_pickled_local_modules()` 对当前 pickle/torch payload 的检测；对已受影响 package 做受控 re-freeze/rebind。

2. **P1/P2 / simulation_runtime + MiniQMT data health：pre-run failure evidence 与 QMT quote/positions 健康**
   - quote stale 已有独立 RCA，建议按该 RCA 登记/处置。
   - positions load 需要更强 durable evidence（append-only 或 last-N），否则瞬态 `get_positions` 底层异常会被后续 retry 覆盖，无法精确归因。

不建议：

- 不建议放宽 300s quote freshness guard 来“跑通”；这是下单前价格安全 fail-closed。
- 不建议把 WSL failure 归因于 QMT 状态、行情订阅或 BUG-567 binding refreeze；现有证据指向 model code asset 缺失。
- 不建议在本窗口做任何 re-subscribe、QMT/WSL restart、binding/package rebuild、operator/apply 或 broker 操作。

## 11. Production gates / untouched

- `production_ddl_gate=noop`
- `production_frontend_dependency_gate=noop`
- `production_backend_dependency_gate=noop`
- `production_dml_gate=noop`
- 本轮未改代码、未启停服务、未重启 QMT/WSL、未 re-subscribe、未写生产 DB、未跑 apply/operator、未发/撤券商订单。
