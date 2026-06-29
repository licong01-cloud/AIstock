# BUG-553 MiniQMT A 影子 reject 场景 child order count drift 根因调查

- 日期：2026-06-29
- 模块：`miniqmt_execution_runtime`
- GitHub Issue：`#1734`
- 调查范围：只读取证与文档产出；未修改 `runtime.py` / `shadow.py` / `client.py` 等产品代码；未启停服务；未写生产 DB。
- 证据源：`F:\Dev\AIstock\tmp\miniqmt_execution_runtime\runtime-state.json`
- 目标 runtime：`mqrt_sim_c3f180d060aecb97a0fe8852`，reject A/B adapter runtimes：`mqrt_sim_c3f180d060aecb97a0fe8852_reject_a` / `mqrt_sim_c3f180d060aecb97a0fe8852_reject_b`

## 结论

判定为 **(a) A/event-loop 侧真实生命周期 bug（更精确地说，是 shadow event-loop replay 的状态复用 + reject 后 vn.py algo 未终止导致旧 algo 被后续 tick 重新驱动）**，不是单纯的 reconciler 裸数量口径过严。

关键结论：

1. A 多出的 30 个 `SUBMITTED` 子单不是新 parent intent、不是新标的，也不是 B 漏掉的独立需求；它们全部复用了同一批 30 个 `parent_intent_id` / 标的 / 数量。
2. 这 30 个 `SUBMITTED` 并非 reject 回调当场提交，也不是 A runtime 的 `on_timer` 触发；`_reject_a` 事件里没有 `TIMER`，额外提交发生在第三次 replay 的 `TICK` 阶段。
3. 触发链是：第一次 reject replay 后，A 的旧 vn.py algo 仍保持 `ACTIVE`；第三次同 scenario replay 复用同一个 `_reject_a` runtime id，新的 tick 到来时，旧 ACTIVE algo 被 `_dispatch_tick_to_vnpy_algos()` 再次驱动，SNIPER 对剩余量重新发出 submit；随后 reject 场景只 reject 了每个 parent 的最新新子单，旧 algo 发出的重试子单留在 `SUBMITTED`。
4. 第三次报告的 FATAL 是有效信号：A snapshot 中确实存在 B 没有的 30 个 open-like child orders。仅把 `_append_count_diff()` 改成忽略数量差会掩盖真实未终止/重复提交风险。

## 三次 reject 报告对比

| 报告时间 UTC | base runtime | report seq | input_event_count | parent 数 | A child orders | B child orders | differences |
|---|---|---:|---:|---:|---|---|---:|
| 2026-06-29T01:37:48 | `mqrt_sim_190a26379f6dff4c7530c7a3` | 3 | 25 | 8 | 8 `REJECTED` | 8 `REJECTED` | 0 |
| 2026-06-29T03:01:06 | `mqrt_sim_c3f180d060aecb97a0fe8852` | 3 | 91 | 30 | 30 `REJECTED` | 30 `REJECTED` | 0 |
| 2026-06-29T07:40:24 | `mqrt_sim_c3f180d060aecb97a0fe8852` | 190 | 91 | 30 | 60 `REJECTED` + 30 `SUBMITTED` | 60 `REJECTED` | 1 `FATAL` |

03:01 与 07:40 的输入元数据一致：`input_event_count=91`、`package_id=pkg_378eb9c91e104c64935404e257e932ee`、`binding_id=simbind_e03557474353c6bb`、`strategy_slot_id=codex_final_ms_l16_20260603`、`algo_config.timer_iterations=1`。第三次多出的不是更多 parent、更多标的或更多 timer，而是同一 `_reject_a` / `_reject_b` adapter runtime 已保留了前一次 reject replay 的状态。

## 30 个额外 A `SUBMITTED` 子单取证

第三次 `_reject_a` 最终状态：30 个 parent，每个 parent 都有 3 个 child：第一次 replay 的 `REJECTED`、第三次 replay 中旧 algo 被 tick 重新驱动产生的 `SUBMITTED`、第三次 replay 新 algo 的 `REJECTED`。B 侧每个 parent 只有两条 `REJECTED`。

| # | parent_intent_id | symbol | qty | A status/time/child | B status/time/child |
|---:|---|---|---:|---|---|
| 1 | `intent_01e8210442c6911a` | `300488.SZ` | 500 | REJECTED 02:39:01 a751f9ed<br>SUBMITTED 06:47:24 d9f53889<br>REJECTED 06:47:31 cd45b5fa | REJECTED 02:47:12 84bbdf51<br>REJECTED 07:04:36 41c21189 |
| 2 | `intent_301275f9c4e9af88` | `600378.SH` | 300 | REJECTED 02:41:27 2d4bdb96<br>SUBMITTED 06:52:05 fd3d47c9<br>REJECTED 06:52:13 eb384fb2 | REJECTED 02:52:19 1a0439d4<br>REJECTED 07:19:37 d49b230a |
| 3 | `intent_35c597fb65f601cf` | `688486.SH` | 337 | REJECTED 02:43:20 55fa34be<br>SUBMITTED 06:55:37 c454af4c<br>REJECTED 06:55:45 23c0a90b | REJECTED 02:57:48 7cc57130<br>REJECTED 07:33:08 e2a42a7c |
| 4 | `intent_3aeaa7fd84f74337` | `688596.SH` | 352 | REJECTED 02:43:57 5c7d6f8f<br>SUBMITTED 06:56:47 6252681e<br>REJECTED 06:56:54 8ff434b6 | REJECTED 02:59:58 44288e6f<br>REJECTED 07:38:13 c5886d99 |
| 5 | `intent_43cf8fc8f94790b5` | `300920.SZ` | 900 | REJECTED 02:39:25 7d411c15<br>SUBMITTED 06:48:11 d7cba32a<br>REJECTED 06:48:18 44b43715 | REJECTED 02:47:53 1a8228ac<br>REJECTED 07:06:52 4fe7becd |
| 6 | `intent_4fb6f5f1803b58e3` | `301233.SZ` | 400 | REJECTED 02:40:14 f56d989a<br>SUBMITTED 06:49:45 62ab3cc9<br>REJECTED 06:49:52 c3e2a6b0 | REJECTED 02:49:27 ce3e8116<br>REJECTED 07:11:40 b9dfb36c |
| 7 | `intent_639dde006027b2df` | `603159.SH` | 1100 | REJECTED 02:42:17 dd0a10f2<br>SUBMITTED 06:53:40 91f80dc9<br>REJECTED 06:53:48 c7eb8662 | REJECTED 02:54:33 21360647<br>REJECTED 07:25:22 fbe71f50 |
| 8 | `intent_63a013e5a776477d` | `600667.SH` | 700 | REJECTED 02:41:39 b124b209<br>SUBMITTED 06:52:29 7133730a<br>REJECTED 06:52:37 dfa3451b | REJECTED 02:52:52 25ef73cf<br>REJECTED 07:21:01 95601d6b |
| 9 | `intent_6eb72212296c688e` | `600237.SH` | 1600 | REJECTED 02:41:16 dd9f7958<br>SUBMITTED 06:51:42 b044e218<br>REJECTED 06:51:50 e3169a72 | REJECTED 02:51:48 80fd144b<br>REJECTED 07:18:15 5323edcd |
| 10 | `intent_701bb69c751f0f34` | `301115.SZ` | 100 | REJECTED 02:39:49 0bad2e67<br>SUBMITTED 06:48:57 85da2e5e<br>REJECTED 06:49:05 ea2abea7 | REJECTED 02:48:38 d0450a5a<br>REJECTED 07:09:12 853c3cb4 |
| 11 | `intent_711879b191bcb19c` | `603091.SH` | 400 | REJECTED 02:41:52 1840d881<br>SUBMITTED 06:52:52 062a6f99<br>REJECTED 06:53:00 920a284e | REJECTED 02:53:25 843f9762<br>REJECTED 07:22:27 732d8b78 |
| 12 | `intent_81c0da8e2ae54ced` | `301186.SZ` | 400 | REJECTED 02:40:01 8a2309eb<br>SUBMITTED 06:49:22 37a4fecf<br>REJECTED 06:49:29 4ecd51dd | REJECTED 02:49:02 55b258f7<br>REJECTED 07:10:24 f24ca861 |
| 13 | `intent_930dbbb6f52ac16d` | `600063.SH` | 2400 | REJECTED 02:41:04 545d249b<br>SUBMITTED 06:51:18 0cd25f54<br>REJECTED 06:51:25 8c3eaf24 | REJECTED 02:51:17 edefc543<br>REJECTED 07:16:55 cb8f72dd |
| 14 | `intent_9678a105f0f16ba1` | `301106.SZ` | 100 | REJECTED 02:39:37 141951f9<br>SUBMITTED 06:48:33 bdd7e1a5<br>REJECTED 06:48:42 50de0d8f | REJECTED 02:48:15 c8f6ce73<br>REJECTED 07:08:01 e2295c9c |
| 15 | `intent_9abf3b15c14d188f` | `001230.SZ` | 2000 | REJECTED 02:38:12 4ae3e226<br>SUBMITTED 06:45:49 06a9c123<br>REJECTED 06:45:56 d94e2cd4 | REJECTED 02:46:02 948d492e<br>REJECTED 07:00:16 fc0eba1c |
| 16 | `intent_9cfed9d70bed1dc2` | `002824.SZ` | 500 | REJECTED 02:38:48 30d3f2cd<br>SUBMITTED 06:47:00 7aec3f51<br>REJECTED 06:47:08 83ce4240 | REJECTED 02:46:52 7a3e3dd2<br>REJECTED 07:03:29 f9ae6c25 |
| 17 | `intent_a485bde96bde84b3` | `000970.SZ` | 1400 | REJECTED 02:38:01 9b661072<br>SUBMITTED 06:45:25 c49e3caa<br>REJECTED 06:45:33 f89c1dbc | REJECTED 02:45:48 fd9741fe<br>REJECTED 06:59:14 9e83063d |
| 18 | `intent_a50d0185f0f813cd` | `603661.SH` | 400 | REJECTED 02:42:42 6adfbde8<br>SUBMITTED 06:54:27 5d5ff63b<br>REJECTED 06:54:35 5d2142ed | REJECTED 02:55:47 b69342f4<br>REJECTED 07:28:26 3bc0fa14 |
| 19 | `intent_a59736814617d6a3` | `603500.SH` | 1500 | REJECTED 02:42:30 2e4b4463<br>SUBMITTED 06:54:03 09c16264<br>REJECTED 06:54:11 d62af19e | REJECTED 02:55:10 9e6e9e1e<br>REJECTED 07:26:55 e6349af8 |
| 20 | `intent_a6e364ced74883da` | `688548.SH` | 503 | REJECTED 02:43:32 4310d0d3<br>SUBMITTED 06:56:00 3fa68dbf<br>REJECTED 06:56:08 ab410f2b | REJECTED 02:58:30 69e0f899<br>REJECTED 07:34:48 317e528d |
| 21 | `intent_b4f337fb2bdaab14` | `301303.SZ` | 1100 | REJECTED 02:40:39 86155b3d<br>SUBMITTED 06:50:31 e43a2abd<br>REJECTED 06:50:39 5d16ca26 | REJECTED 02:50:20 4d3f2656<br>REJECTED 07:14:14 99058953 |
| 22 | `intent_b5eac708ccbca9c6` | `688584.SH` | 581 | REJECTED 02:43:45 b758b34a<br>SUBMITTED 06:56:24 89ef4530<br>REJECTED 06:56:32 19d3a53c | REJECTED 02:59:14 a88170a6<br>REJECTED 07:36:32 f1c808f4 |
| 23 | `intent_b6331259f0006bdc` | `688106.SH` | 575 | REJECTED 02:43:08 d62cde04<br>SUBMITTED 06:55:14 292fe56f<br>REJECTED 06:55:21 1fbb5057 | REJECTED 02:57:06 b5c7cce8<br>REJECTED 07:31:32 c4e78259 |
| 24 | `intent_b8528b2981883bb6` | `002254.SZ` | 1000 | REJECTED 02:38:36 44a33257<br>SUBMITTED 06:46:36 a96291c6<br>REJECTED 06:46:44 e48cf849 | REJECTED 02:46:35 b90750f1<br>REJECTED 07:02:25 4193aa47 |
| 25 | `intent_bad0b568309a8ea0` | `301288.SZ` | 1000 | REJECTED 02:40:27 585e8bc2<br>SUBMITTED 06:50:08 1e6997a5<br>REJECTED 06:50:16 875b14c2 | REJECTED 02:49:53 1af15d3e<br>REJECTED 07:12:55 825fc586 |
| 26 | `intent_c784b9db90f912e6` | `002167.SZ` | 900 | REJECTED 02:38:24 b2b48fc8<br>SUBMITTED 06:46:13 90131bf8<br>REJECTED 06:46:21 aa9af302 | REJECTED 02:46:18 e860ca5a<br>REJECTED 07:01:20 8b40d7d4 |
| 27 | `intent_d572674f8998f944` | `603155.SH` | 800 | REJECTED 02:42:05 6274d872<br>SUBMITTED 06:53:16 130ac113<br>REJECTED 06:53:23 2f5f756a | REJECTED 02:53:58 3fe614af<br>REJECTED 07:23:53 0317dc78 |
| 28 | `intent_dfeb398b7f7e0c13` | `301322.SZ` | 300 | REJECTED 02:40:51 685934e0<br>SUBMITTED 06:50:54 1d131430<br>REJECTED 06:51:03 a5bb4721 | REJECTED 02:50:48 24985d3d<br>REJECTED 07:15:33 801bdb0a |
| 29 | `intent_f410ed345c2df34d` | `603678.SH` | 200 | REJECTED 02:42:54 2bd9161a<br>SUBMITTED 06:54:51 b94ab73d<br>REJECTED 06:54:59 770eeb1b | REJECTED 02:56:26 fabb4876<br>REJECTED 07:29:58 62d8edba |
| 30 | `intent_ff9c39eb72d2bc8c` | `300843.SZ` | 100 | REJECTED 02:39:13 af43e35b<br>SUBMITTED 06:47:47 518e1cc0<br>REJECTED 06:47:55 2d8ec018 | REJECTED 02:47:32 98457103<br>REJECTED 07:05:44 6c89c2c4 |

聚合校验：

- A `_reject_a`：90 child orders = 60 `REJECTED` + 30 `SUBMITTED`；30 个 parent；30 个 symbol；每个 parent 恰好 3 个 child。
- B `_reject_b`：60 child orders = 60 `REJECTED`；30 个 parent；30 个 symbol；每个 parent 恰好 2 个 child。
- A/B 的 parent set、symbol set、单笔 quantity 完全一致；差异只在 A 每个 parent 额外保留 1 个旧 algo 重新提交的 open-like child。

## 触发路径

### A 侧实际路径

代码链：

1. `backend/services/miniqmt_execution_runtime/shadow.py:604-607`：`_shadow_adapter_runtime_id()` 只按 `runtime_id + scenario + runtime_kind` 生成 adapter runtime id；同一 base runtime 的多次 reject replay 都复用 `mqrt_sim_c3f180d060aecb97a0fe8852_reject_a`。
2. `shadow.py:348-369`：`MiniQMTShadowEventLoopAdapter.compute_shadow_snapshot()` 在同一个 repository/runtime id 上 `runtime.start()` 后调用 `_drive_event_loop_runtime()`，没有清理上一轮同 scenario replay 的 child/algo 状态。
3. `shadow.py:698-735`：`_drive_event_loop_runtime()` 处理 `parent_intent` 创建新 algo，处理 `tick` 时调用 `runtime.on_tick()`；本次 `_reject_a` 没有 `TIMER` 事件，因此额外 `SUBMITTED` 不是 timer 触发。
4. `runtime.py:328-340`：`on_tick()` 追加 `TICK` event 后调用 `_dispatch_tick_to_vnpy_algos()`。
5. `runtime.py:1679-1687`：`_dispatch_tick_to_vnpy_algos()` 遍历 `active_only=True` 的所有 vn.py algo；上一轮 reject 后遗留的旧 algo 仍是 `ACTIVE`，因此被第三次 replay 的 tick 再次驱动。
6. `runtime.py:1698-1727`：`_handle_vnpy_actions()` 收到 SNIPER core 的 `SUBMIT` action 后调用 `submit_child_order()`，于是旧 algo 产生额外 `SUBMITTED` child。
7. `shadow.py:811-825`：reject scenario 后续通过 `_record_shadow_order_status()` 调 `runtime.record_order_event()` 只更新 `_latest_child_by_parent()` 选中的最新 child。第三次 replay 中最新 child 是新建 algo 的 child，旧 algo 刚重发的 child 留在 `SUBMITTED`。
8. `runtime.py:558-623`：`record_order_event()` 对 reject child 调 `core.update_order(active=False)`，之后调用 `_terminalize_algo_if_all_children_terminal()`；但 `runtime.py:1139-1140` 对 vn.py instance 且 `command_id is None` 直接返回，不终止 algo，导致 reject 后 algo 保持 `ACTIVE`。

单 parent 示例 `intent_01e8210442c6911a / 300488.SZ`：

| event seq | event_time UTC | event_type | 含义 |
|---:|---|---|---|
| 179 | 06:47:16 | `ALGO_INSTANCE_CREATED` | 第三次 replay 创建新的 300488.SZ algo。 |
| 181 | 06:47:20 | `TICK` | 第三次 replay 的 tick 到达；旧 02:38 algo 仍 `ACTIVE`，被一起 dispatch。 |
| 182 | 06:47:26 | `CHILD_ORDER_SUBMITTED` | 旧 02:38 algo 重新提交 500 股，child `...d9f53889`，最终保持 `SUBMITTED`。 |
| 183 | 06:47:34 | `CHILD_ORDER_SUBMITTED` | 新 06:47 algo 提交 500 股，child `...cd45b5fa`，随后被 reject scenario 标记为 `REJECTED`。 |

这证明额外子单是旧 rejected algo 在后续 replay tick 上被重新驱动，不是 reject callback 立即重提，也不是 `on_timer()` 驱动。

### B 侧为何不发散

B adapter 走 compiler/managed preview：

1. `shadow.py:399-431`：`MiniQMTShadowCompilerAdapter.compute_shadow_snapshot()` 调 `MiniQMTExecutionRuntimeClient.build_managed_vnpy_order_requests()`。
2. `client.py:345-373`：B 对每个 parent 执行一次 create algo、一次 tick、`range(_timer_iterations)` 的静态 timer 循环；它是 compiler-style 的一次性 managed request build。
3. `shadow.py:432-436` 与 `shadow.py:773-809`：terminal shadow events 不是通过 `runtime.record_order_event()` 回灌 core，而是直接把 latest repo child upsert 为 `REJECTED` / `CANCELLED`。
4. 下一次同 scenario replay 时，B 的旧 core 仍以为旧 vt_orderid active，因此 tick 会产生 cancel/requote 意图；但 repo 中旧 child 已是 `REJECTED`，`_find_vnpy_active_children(... active_only=True)` 找不到可 cancel child，事件上表现为 `CHILD_ORDER_CANCEL_REQUESTED` 且 `cancel_acks=[]`，不会留下新的 open child。

B 因为没有真实 reject callback 驱动，也没有把旧 rejected algo 变成新的 submitted child，所以第三次是 60 个 `REJECTED` 而不是 90 个 child。

## 为什么前两次 differences=0，第三次才发散

- 01:37 使用不同 base runtime：`mqrt_sim_190a26379f6dff4c7530c7a3`，只有 8 个 parent，是该 `_reject_a/_reject_b` scenario runtime 的首次 replay，没有历史污染。
- 03:01 是 `mqrt_sim_c3f180d060aecb97a0fe8852` 的首次 reject replay，`_reject_a/_reject_b` 中尚无上一轮状态，因此 A/B 都是 30 `REJECTED`。
- 07:40 复用同一个 base runtime 与同一个 scenario adapter runtime id；03:01 留下的 30 个 A 侧 vn.py algo 仍是 `ACTIVE`，第三次 replay 的 tick 重新驱动旧 algo，产生 30 个额外 `SUBMITTED`。

因此非稳定复现的根因不是标的集变化或 timer 次数变化，而是重复 shadow replay 对同一 adapter runtime 的状态复用与 A reject 后 active algo 未终止共同作用。

## ADR0002 / A 设计判断

ADR0002 要求 A 是 durable、回调驱动的 event-loop runtime：算法实例由真实 tick/timer/fill/order callback 驱动，禁止把 A 简化成提交后查一次或合成 timer。由此可以推出：

- A 在真实事件后对未成交剩余量继续行动本身并不违背设计；这也是 A 相比 B 的价值。
- 但本案不是同一次真实交易生命周期内的合理重试。证据显示额外 `SUBMITTED` 来自上一轮 shadow reject replay 遗留的 old algo，在后续同 scenario replay 的 tick 上被重新触发；它跨 replay 重复了同一 parent 的完整数量，并且没有被本轮 reject scenario terminal event 覆盖。
- 因此这是 A/shadow replay 生命周期处理 bug，而不是 reconciler 过严误报。`MINIQMT_SHADOW_CHILD_ORDER_COUNT_DRIFT` 在本案中应该继续 FATAL，因为 A snapshot 里有 30 个 B 没有的 open-like child orders。

## 建议修复方向与 scope

推荐下一步登记实现类 BUG，优先级建议 P1，因为该问题会阻断 A 影子第 1 天 canary/shadow 证据判读。

建议修复 scope：

1. `backend/services/miniqmt_execution_runtime/runtime.py`
   - 处理 terminal order event 后，vn.py algo 不能因为 `command_id is None` 永久跳过 terminalization。
   - 可增加 vn.py-aware terminalization：当 child 全部 terminal 且 core 无 active order 时，将 algo 标记为 `FAILED` / `COMPLETED` / `CANCELLED`，避免后续 tick/timer dispatch。
   - 若业务明确允许 reject 后重试，则必须有显式 retry policy、retry budget、child lineage，并且 scenario replay 必须能把 retry child terminalize；不能 silent 留下 open child。
2. `backend/services/miniqmt_execution_runtime/shadow.py`
   - shadow adapter replay 应具备 idempotency：同一 base runtime / same scenario 多次 replay 不能复用未清理的 `_reject_a/_reject_b` 状态，或必须在 replay 前隔离 run attempt id。
   - `_record_shadow_order_status()` 不能只更新每个 parent 的 latest child 而漏掉同 parent 的 open-like retry child；至少在 terminal scenario 中应验证同 parent 无残留 open-like child。
3. `backend/tests/miniqmt_execution_runtime/test_miniqmt_phase5_shadow_reconciliation.py`
   - 增加 repeated reject replay 回归：同一 runtime 连续两次 reject scenario 后 A/B 不得出现额外 `SUBMITTED`。
   - 增加 reject terminalization 回归：A 侧 rejected child 不应使旧 vn.py algo 在后续 tick 中无边界重发。
4. Reconciler 方向
   - 不建议直接削弱 `_append_count_diff()` 为“数量差不 FATAL”。
   - 可以在修复后增强 reconciler 诊断字段：按 `parent_intent_id/symbol/side/quantity/status/open_like_quantity` 输出语义差异，帮助区分 harmless lineage 差异与真实 open exposure 差异；但本案的 FATAL 不应被豁免。

## 本轮未做

- 未修改产品代码。
- 未启动或停止任何服务。
- 未写生产 DB、未执行 DDL。
- 未提交修复 PR；本报告只作为 BUG-553 调查证据与后续实现任务输入。
