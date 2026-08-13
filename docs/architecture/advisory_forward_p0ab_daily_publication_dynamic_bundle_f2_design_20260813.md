# Advisory P0-A/P0-B 每日前向发布与动态模型分发详细设计

> 日期：2026-08-13
> Feature tier：F2
> 父级蓝图：`docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md` v3.0
> 当前阶段：`DESIGN_READY_FOR_IMPLEMENTATION`
> 适用范围：学术研究与模拟荐股观察，不构成实时投资建议，不连接实盘交易或下单执行

## 1. Background / 当前真实基线

当前 `main` 已具备按固定历史日期读取 persisted Advisory list、Selection run、数据库 decision-cutoff 行情，并输出 Top5、五期限收益/概率、持股周期和价格范围的能力。该能力只在目标多 Alpha Program 的 `decision=2026-07-15 / target=2026-07-16` 上完成按需 GET readback。

生产运行仍存在以下明确缺口：

1. 两个 `ENABLED` Program 虽配置 `daily_after_close`，但系统没有 Advisory 自动执行器。
2. 两个 Program 都没有自然向前产生过 `PUBLISHED` list、正式 review 或 episode。
3. `AdvisoryModelShadowService` 通过 `target_binding.py` 固定单一 Program、binding、package、manifest、style profile、两条 Alpha 腿和终端权重。
4. 现有 `run_review()` 在一个调用中同时创建 review run、`PUBLISHED` list、review decisions、episode、metrics 和 Program 状态；它要求目标日的 `next_open_executable`，不能直接表达“D 收盘发布、D+1 开盘再进入”。
5. 现有 PostgreSQL repository 将上述事实拆成多个独立事务；中途失败可能留下 review/list/episode 的部分写入。

本设计只解决父级蓝图 P0-A/P0-B，不处理历史补账、旧 replay、旧 batch、通用调度平台、通用 ModelOps 或模型再训练。

## 2. Scope / 交付范围与目标

### 2.1 P0-A

- 对每个符合现有配置的 `ENABLED + daily_after_close` Program，在交易日 D 收盘后自然生成目标日 D+1 的唯一 `PUBLISHED` baseline recommendation。
- 发布阶段只读取 `decision_as_of_trade_date=D` 及更早数据，不读取或假设 D+1 行情。
- D+1 权威 `market.kline_daily_raw.open_li` 到达后，消费已发布 list 和原 Selection run，使用既有 `AdvisoryListTransitionEngine` 推进 baseline episode。
- 同日持久化独立 model challenger observation；无兼容 bundle 时 baseline 仍成功，model 状态为 typed unavailable。
- API/UI 显示基线发布、目标日结算、challenger、错误和成熟度，不能用旧日期或按需 GET 冒充前向事实。

### 2.2 P0-B

- 用 Program active binding 和不可变模型描述符替代源码中的单一目标常量判断。
- 继续使用 exact package/manifest/style/bundle 文件，不扫描 `latest`，不按目录时间选择，不跨 Program 共享可变状态。
- 当前目标多 Alpha bundle 的模型字节、特征顺序和 point readback 语义保持不变。
- 单 Alpha 与原生多 Alpha 使用同一个解析合同；没有真实兼容 bundle 时明确返回 unavailable，禁止伪造模型或复制多 Alpha 模型。

## 3. Non-goals / 明确禁止

- 不回填 2026-07-17 以来未发布的历史日期，不恢复或归档旧 replay/batch/root。
- 不修改 StrategyPackage 准入，不增加策略包二次检查、审批、角色、人工 ACK 或收益门槛。
- 不修改 Selection、Paper v2、模拟盘、QMT、QE 或 RD-Agent 的业务逻辑和写入路径。
- 不下单、不分配资金、不生成 broker order，不把 model Top5 写回 baseline list 或 Selection rank。
- 不建设通用 scheduler、任务编排平台、模型注册中心、血缘仓库、缓存平台、历史证据平台或自动模型激活。
- 不为尚不存在的单 Alpha 模型预造 bundle；单 Alpha Program 仅完成 baseline 发布和 typed model unavailable。
- 不在本阶段训练模型，不处理 P0-C/P0-D 的 policy label、CPCV/PBO 或 meta-label 训练。

## 4. Contracts / 不可变业务合同

1. 一个 Program、一个 `target_trade_date` 最多存在一个 `PUBLISHED` baseline list。
2. `decision_as_of_trade_date < target_trade_date`，并且 target 必须是 decision 的下一交易日。
3. D 收盘发布不得读取 D+1 open/close、停牌结果或任何未来 outcome。
4. baseline list 来源只能是该 Program active binding 对应的单个 StrategyPackage 或原生 multi Alpha 父包 Selection 结果。
5. 发布 list 不因 D+1 结算而被回写；结算是独立追加事实，保留“昨晚建议”与“今早结果”的差异。
6. episode 只在权威 entry price 可用时创建；缺价、停牌或不可执行保持 `WAITING_DATA`/`NOT_ENTERED`，不得退回 signal close。
7. baseline episode 只由现有 Program review policy 和 `AdvisoryListTransitionEngine` 推进；P0-A 不复制第二套淘汰、替换、止盈或止损算法。
8. model challenger 不改变 baseline target count、daily replacement budget、Selection rank、Program episode 或 Program metrics。
9. 一个 Program 的失败不阻断其它 Program；同一 Program 的错误必须包含 program、decision、target、stage 和 reason code。
10. 所有正常输入校验自动通过；本设计没有人工审批、业务门禁或额外角色。

## 5. Date Clock / 两阶段日期合同

### 5.1 AFTER_CLOSE_PUBLISH

```text
decision_as_of_trade_date = D
target_trade_date = trading_calendar.next_trading_day(D)
selection_run.trade_date = target_trade_date
selection_artifact.cutoff_date = D
selection_artifact.cutoff_policy = FIXED_CUTOFF
runtime_profile.tradability.exclude_suspended = false
```

Selection 的业务日期仍为目标日，保证候选身份与现有 Advisory point inference 一致；所有数据读取通过现有 `advisory_date_context` 和固定 cutoff 收敛到 D。由于目标日停牌事实尚不可知，发布阶段不做目标日停牌过滤，D+1 结算阶段重新验证。

### 5.2 TARGET_OPEN_SETTLE

```text
settlement_trade_date = target_trade_date
candidate source = persisted selection_run_id from publication
entry/exit market source = market.kline_daily_raw(target_trade_date)
entry basis = Program.entry_price_basis
exit basis = Program.exit_price_basis
```

首版两个现有 Program 的 entry/exit basis 均为 `next_open_executable`。只有 `open_li > 0`、目标日 `suspend_d` 同步状态完整且不存在权威不可交易状态时才提供 entry/exit mark。`suspend_d` 同步未完成、目标日行缺失、价格非法或权威不可交易时写明确状态，不从 Selection reference price、signal close、上一日 close 或规则常量回退。该同步完整性检查使用现有数据刷新状态并在数据正确时自动通过，不增加人工审批。

### 5.3 顺序

- runner 每次先按 target 日期从早到晚重试当前 Program 已发布但未结算的 forward run，再考虑当天 D 的新发布。
- 正常行情完整时，前一 target 的结算在下一次 D 收盘发布前已完成。
- 若前序结算仍缺数据，新 baseline Selection 可以独立生成，但正式发布不跨越未知 episode state；该 Program 本次记录 `ADVISORY_FORWARD_PREVIOUS_SETTLEMENT_PENDING`。其它 Program 继续运行。
- 这是 episode 连续性的确定性校验，不是人工审批或发布门禁；数据准确时自动通过。
- pending settlement 按 `target_trade_date, program_id` 排序；同一 Program 的较早日期一旦返回 `WAITING_DATA/FAILED`，本轮对该 Program 的更晚日期返回明确 `SKIPPED_PREVIOUS_SETTLEMENT_PENDING` 且不执行 transition，其他 Program 继续，避免跨日 episode 状态倒序写入。

## 6. Architecture / 最小组件

```text
AdvisoryForwardScheduler (Advisory-only, opt-in)
  -> AdvisoryForwardService.run_once()
     -> settle pending target-open runs
     -> determine closed decision day D and next target day
     -> for each ENABLED daily_after_close Program independently
        -> existing Selection Center with explicit D cutoff
        -> build immutable pending baseline list
        -> atomic publication commit
           review_run + PUBLISHED list/items + forward_run
        -> exact Program model descriptor resolution
        -> existing M2/M3/M4 model inference
        -> forward model observation upsert-by-exact-identity

target day authoritative open arrives
  -> load persisted forward_run/list/selection
  -> existing AdvisoryListTransitionEngine
  -> atomic settlement commit
     daily review decisions + episode snapshots + metrics
     + Program status + review_run status + forward settlement payload
```

新增模块仅放在 Advisory 边界：

- `backend/services/advisory_forward/models.py`
- `backend/services/advisory_forward/repository.py`
- `backend/services/advisory_forward/service.py`
- `backend/services/advisory_forward/scheduler.py`
- `backend/services/advisory_model_first/model_binding_resolution.py`

`backend/services/advisory_program.py` 只提取纯 review evaluation port 和 atomic forward settlement 所需的最小适配，不改变手工 preview/run/replay 的既有语义。

## 7. Persistence / 最小 PostgreSQL 变化

迁移名称：

```text
backend/db/migrations/add_advisory_forward_publication_20260813.sql
backend/db/migrations/add_advisory_forward_publication_20260813.rollback.sql
```

### 7.1 `app.advisory_forward_run`

一个 Program/decision/target 的 durable 前向业务身份：

```text
forward_run_id                 PK
program_id                     FK advisory_program
program_version
binding_version_id             FK advisory_strategy_binding_version
decision_as_of_trade_date
target_trade_date
publication_status             PENDING|PUBLISHED|WAITING_DATA|FAILED
settlement_status              NOT_DUE|WAITING_DATA|SETTLED|NOT_ENTERED|FAILED
selection_run_id
review_run_id                  FK advisory_review_run, nullable until publish
list_version_id                FK advisory_recommendation_list_version, nullable until publish
active_episode_state_hash
publication_payload_sha256
settlement_payload_sha256
last_stage
last_reason_code
last_error_json
attempt_count
model_resolution_json
created_at / updated_at / published_at / settled_at
run_payload_json
```

约束：

- `UNIQUE(program_id, target_trade_date)`。
- `decision_as_of_trade_date < target_trade_date`。
- publication 为 `PUBLISHED` 时三个 source id 均非空。
- settlement 为终态时 `published_at` 非空。
- `PENDING/FAILED` 且尚未发布的 run 可在重试时跟随当前有效 binding 重新冻结输入；`PUBLISHED` 后 Program/binding/selection/日期和 publication hash 均不可变。

该表没有 owner、lease、fencing、审批或人工状态。调用开始时短事务 `INSERT ... ON CONFLICT DO UPDATE attempt_count=attempt_count+1`，不会把已有 `PENDING/FAILED/WAITING_DATA` 锁死；事实写入仍由唯一键和原子 commit 收敛。

`run_payload_json` 在 published 前保存当前 attempt 输入，published 后保存冻结 publication 和 settlement summary：review status、entered/held/exited/waiting count、episode ids、market dataset identity 和 reason。逐 symbol 正式结果继续写入现有 `app.advisory_daily_review`，episode 继续写入 `app.advisory_episode_return`；不新增重复 settlement 表。

### 7.2 `app.advisory_forward_model_observation`

一个前向发布日期的独立 challenger 事实：

```text
observation_id                 PK
forward_run_id                 UNIQUE FK advisory_forward_run
program_id / binding_version_id
decision_as_of_trade_date / target_trade_date
status                         EXPERIMENTAL_SHADOW|UNAVAILABLE|FAILED
reason_code / message
package_id / manifest_sha256
style_profile_id / style_profile_hash
model_descriptor_sha256
bundle_id / outcome_bundle_id / price_range_bundle_id
feature_schema_version
candidate_count / shortlist_count
maturity_trade_date
prediction_payload_json
created_at / updated_at
observation_payload_json
```

- publication 阶段把 descriptor identity 或 typed unavailable resolution 冻结在 forward run；`UNAVAILABLE` 是当天解析时的真实事实，不因以后新增 bundle 而历史回填。
- `FAILED` 只允许在同一 forward run、同一已冻结 exact descriptor 下显式重试并更新为成功；descriptor identity 变化时拒绝覆盖原 observation。
- `maturity_trade_date` 使用已冻结 outcome bundle 声明的最大预测 horizon 与所有候选 `holding_period.range_high_days` 的最大值，从 target 日按权威交易日历向后推进；没有成功 outcome bundle 时为 `NULL`，不得用自然日或固定20日猜测。
- payload 保存 baseline rank、model score/rank、Top5、outcome、price range 和 typed child status；不保存未来 realized return。

## 8. Atomicity And Idempotency / 事务边界

### 8.1 Publication commit

Selection 和模型计算不持有数据库长事务。Selection 成功后，`commit_publication()` 在单个 PostgreSQL 事务中：

1. `FOR UPDATE` 锁定 Program 和目标日有效 binding。
2. 验证 Program 仍可运行、binding/version 与计算输入一致、前序 settlement 已闭合。
3. 若已存在同 Program/target 的 publication，校验 decision、binding、selection 和 payload hash 完全一致后返回既有事实；不一致则 conflict。
4. 插入现有 `advisory_review_run(run_type=RUN, trade_date=target, status=WAITING_DATA)`。
5. 插入现有 `PUBLISHED` list version 和 items。
6. 更新 `advisory_forward_run` 为 `PUBLISHED/NOT_DUE`。
7. commit。

任何一步失败整体回滚，不出现 review 存在但 list 缺失，或 list 存在但 forward identity 缺失。

### 8.2 Challenger observation

Challenger 在 baseline publication commit 后运行。它失败不能回滚已发布 baseline；服务必须写 `UNAVAILABLE` 或 `FAILED` observation，并保留 reason。重试只针对同一 `forward_run_id + model_descriptor_sha256`。保存时先以 `FOR UPDATE` 锁定父 `forward_run`，使同一 forward 的并发首次 insert 串行收敛；不得仅使用共享锁后竞争 child unique key。

### 8.3 Settlement commit

市场数据和纯 transition 结果生成后，`commit_settlement()` 在单个事务中：

1. 锁定 forward run 和 Program。
2. 验证 forward run 已发布且未结算。
3. 重新计算当前 active episode state hash；与 evaluation 输入不一致时返回 conflict，调用方重新加载再评估。
4. 插入 episode snapshots 和 daily review decisions。
5. 插入 metric snapshot并更新 Program last review 状态/日期。
6. 更新既有 review run 状态。
7. 以 canonical payload hash 更新 forward run settlement status/summary。
8. commit。

forward row `FOR UPDATE`、终态不可逆、canonical payload hash 与按 `Program/target/symbol` 确定性生成的新 episode id共同保证重复调用不会创建第二批经济 episode。canonical settlement payload 必须覆盖逐 symbol decision 的 action/reason/价格/rank/score/evidence，以及结算后 episode 的状态、价格、收益、持有期和 evidence；只排除 `created_at/updated_at` 这类非经济运行时间戳。相同经济 payload 返回既有 settlement，任一逐股经济结果不同均明确 conflict。现有 append-only episode snapshot 表不新增伪造的唯一约束。

## 9. Baseline Publication Semantics

D 收盘 publication 不是虚构成交，因此 list item 采用以下既有动作词汇：

- 当前 active episode 且仍在候选深度内：`HOLD`，reason=`PENDING_TARGET_OPEN_REVIEW`。
- 当前 active episode 但缺少完整 rank 证据：`WAITING`，reason=`PENDING_TARGET_OPEN_REVIEW`。
- rank 不高于 `rank_enter_threshold` 且尚无 active episode：`WATCH`，reason=`PENDING_TARGET_OPEN_ENTRY`。
- 其它保留到 rank-exit 观察深度的候选：`WATCH`，reason=`OUTSIDE_ENTRY_THRESHOLD`。

publication 不创建 `ENTER/EXIT` episode。D+1 settlement 才产生正式 `ENTER/HOLD/EXIT/WAITING` daily review decisions。页面必须把“发布动作”和“结算动作”分栏展示，不能将 `WATCH` 翻译为已买入。

发布候选深度为 `max(target_count, rank_exit_threshold)`，最多 50，复用 `_review_runtime_config()`。Top20 baseline 与 Top5 challenger 从同一 persisted Selection run派生，不做每日候选并集。

## 10. Dynamic Exact Model Binding

### 10.1 Program model descriptor

新增不可变 repo-external descriptor：

```text
${AISTOCK_ADVISORY_MODEL_ROOT}/program_bindings/
  {program_id}/{binding_version_id}.json
```

合同 `advisory_program_model_binding_v1`：

```text
program_id
binding_version_id
package_ids                    exactly one native package
package_id
manifest_sha256
style_profile_id / style_profile_hash
selection_runtime_semantics_hash
feature_schema_version / feature_schema_hash
bundle_id / bundle_manifest_sha256
outcome_bundle_id              optional exact child
price_range_bundle_id          optional exact child
candidate_projection:
  schema_version = advisory_candidate_projection_v1
  component_roles = {lstm: <alpha_id>, fund: <alpha_id>}
descriptor_sha256
created_at
```

descriptor writer及正式 CLI `scripts/advisory_publish_program_model_descriptor.py` 只接受显式 model root和显式 JSON payload，使用临时文件、fsync、atomic replace；目标路径已存在且字节不同则拒绝覆盖。它不读取 `.env` 猜路径、不扫描 latest、不激活 scheduler，是 Program active strategy binding 到模型 artifact 的精确索引，不是策略包准入或模型审批。

现有目标多 Alpha descriptor 显式保存当前两条腿角色：

```text
lstm = a1_plus3_LSTM_h20
fund = new_FUNDGROWTH_h20
```

这只把现有训练合同从源码常量迁移到可哈希 artifact，不改变 bundle 或模型字节。descriptor 的创建、激活和运行时 readback分别报告；源码合入不自动写 repo-external artifact。

### 10.2 Resolution algorithm

`AdvisoryModelBindingResolver.resolve(program, active_binding, selection_run)`：

1. Program/binding 必须是现有 native single package 语义；手工多包已退役路径不进入模型解析。
2. 从成功 Selection run 取得唯一 package 与 `manifest_sha256_by_package`。
3. 精确打开 `{program_id}/{binding_version_id}.json`；文件不存在返回 `ADVISORY_MODEL_BUNDLE_NOT_AVAILABLE_FOR_PACKAGE`，并把本次 unavailable resolution 冻结到 forward run。
4. 校验 descriptor hash、Program/binding/package/manifest 和 Selection identity。
5. 通过现有 `load_exact_shadow_bundle(package, manifest, style_hash)` 加载 bundle，并校验 bundle id、manifest file hash、feature schema 和 runtime semantics。
6. outcome/price child 继续通过 exact parent identity 加载；任何 child 缺失只关闭对应 child envelope。

禁止行为：

- 列举 `shadow_bindings` 后选择唯一/最新文件。
- 读取 `activated_at` 最大值。
- 使用 package 名前缀、Alpha id 子串或当前 Program 名推断 style/角色。
- 跨 binding 或 Program 复用缓存对象。

### 10.3 Candidate projection

当前 `advisory_feature_schema_v1` 明确包含两腿 `lstm_* / fund_*` 特征。运行时从 descriptor 的 `component_roles` 读取实际 Alpha id，再从 Selection candidate `component_scores` 取 raw/normalized/rank/weight；终端权重和 runtime semantics 从 bundle manifest 读取并交叉校验。

`model_inference.py` 不再引用 `PACKAGE_ID`、`PROGRAM_ID`、`BINDING_VERSION_ID`、`LSTM_LEG_ID`、`FUND_LEG_ID`、`TERMINAL_WEIGHTS` 或 style 常量。`target_binding.py` 保留给现有训练请求/历史 artifact 身份使用，不再参与运行时路由。

单 Alpha 和其它多 Alpha 使用相同 resolver。只有它们存在真实 descriptor、bundle 和代码已支持的 feature schema/candidate projection schema 时才推理；否则 typed unavailable。P0-B 不预造单 Alpha feature schema，不用两腿空值、复制腿或默认权重冒充兼容。

## 11. Scheduler / 运行方式

新增 Advisory 专用 in-process scheduler，生命周期模式可参考现有 scheduler，但不 import Paper/QE 业务服务。

```text
AISTOCK_ADVISORY_FORWARD_SCHEDULER_ENABLED=false
AISTOCK_ADVISORY_FORWARD_POLL_SECONDS=300
AISTOCK_ADVISORY_FORWARD_AFTER_CLOSE_TIME=16:30:00
```

- 默认关闭，源码合入不等于调度激活。
- 默认关闭时，模块 import、API status 和后端启动不得解析或校验可选 poll interval；只有显式 `start()`/env autostart 激活时才校验 interval，非法值必须可见失败。
- after-close cutoff 默认 `16:30:00`；`AISTOCK_ADVISORY_FORWARD_AFTER_CLOSE_TIME` 显式配置必须被 service/status/runner真实使用，接受 `HH:MM` 或秒为 `00` 的 `HH:MM:SS`，非法值不得静默回退默认时间。
- 用户显式配置并重启后才自动运行；本任务不自行启停后端。
- `run_once()` 可被 scheduler 和 Advisory API 复用，不依赖线程本地状态。
- scheduler 每次只处理已存在的未结算 run，以及“本地今天是交易日且当前时间不早于 after-close time”时的当天 decision；周末、节假日或停机错过的 decision day不回补、不取最近交易日代替今天。
- 线程异常写结构化日志并保留后续 tick；Program 业务失败作为结果返回，不终止 scheduler。
- 不增加租约、owner、fencing、人工确认或独立审批。

## 12. API / UI

### 12.1 Backend API

```text
GET  /api/v1/advisory/forward/status
POST /api/v1/advisory/forward/run-once
GET  /api/v1/advisory/programs/{program_id}/forward-runs
GET  /api/v1/advisory/forward-runs/{forward_run_id}
```

`run-once` 是已有用户环境中的荐股研究命令，不增加角色或审批。它返回每个 Program 的独立结果：`PUBLISHED`、`IDEMPOTENT_REPLAY`、`WAITING_DATA`、`FAILED` 或 `SKIPPED_NOT_SCHEDULED`。HTTP 200 可包含多 Program 部分失败；请求级 schema/身份错误使用 4xx，未处理异常使用 5xx并记录 correlation id。

Program summary增加：

```text
latest_forward_decision_as_of_trade_date
latest_forward_target_trade_date
latest_forward_publication_status
latest_forward_settlement_status
latest_forward_reason_code
latest_model_observation_status
latest_model_bundle_id
forward_matured_episode_count
```

### 12.2 Frontend

现有 `/paper-v2/advisory` current view增加“前向发布”区域：

- Program 行显示最新 decision/target、baseline 发布、target-open 结算和 model observation 四个独立状态。
- 详情显示 baseline Top20、model Top5、收益/周期/价格范围、bundle identity和 reason。
- `UNAVAILABLE`、`WAITING_DATA`、`FAILED`、`EVIDENCE_IMMATURE` 使用不同文本和状态样式。
- 不显示买入按钮、下单按钮、仓位输入或 QMT/Paper/模拟盘链接。
- 页面不得因 model child失败隐藏 baseline list，也不得把最近一次旧成功显示为当前日期成功。

## 13. Error Contract / 错误可见性

至少提供：

```text
ADVISORY_FORWARD_NOT_AFTER_CLOSE
ADVISORY_FORWARD_DECISION_DAY_NOT_TRADING
ADVISORY_FORWARD_PREVIOUS_SETTLEMENT_PENDING
ADVISORY_FORWARD_SELECTION_WAITING_DATA
ADVISORY_FORWARD_SELECTION_FAILED
ADVISORY_FORWARD_PUBLICATION_CONFLICT
ADVISORY_FORWARD_TARGET_OPEN_WAITING_DATA
ADVISORY_FORWARD_ACTIVE_EPISODE_STATE_CONFLICT
ADVISORY_FORWARD_SETTLEMENT_FAILED
ADVISORY_MODEL_PROGRAM_DESCRIPTOR_NOT_CONFIGURED
ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID
ADVISORY_MODEL_CANDIDATE_PROJECTION_UNSUPPORTED
ADVISORY_MODEL_BUNDLE_NOT_AVAILABLE_FOR_PACKAGE
ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH
```

Typed业务缺失返回结构化状态；未知异常使用 `LOGGER.exception`，不得转成空候选、旧结果、默认价格、规则模型或 HTTP 200 假成功。

## 14. Concurrency And Memory

- Selection/feature/model 计算不持有 PostgreSQL 长事务。
- publication/settlement 使用短事务、Program row lock、唯一业务键和 active episode state hash。
- 不使用进程内 dict 作为权威幂等状态；scheduler restart 后从 PostgreSQL 继续。
- 不创建 SQLite、历史缓存或全市场常驻 DataFrame；每个 Program 最多处理 50 个候选，model feature读取沿用现有按 symbol/date 查询。
- `run_once` 默认顺序处理 Program，避免多个模型同时放大内存；一个 Program 完成后释放 DataFrame/bundle引用。
- 不引入后台训练或 HMM refit，P0-A/P0-B 的内存上限沿用现有在线推理进程预算。

## 15. Rollout / Rollback 与迁移

### 15.1 DEV-first

源码阶段只提交 migration/rollback 和 repository tests，不执行数据库。用户后续授权 DEV 后：

1. 从 `.env` 读取明确 DEV DSN，不猜测连接。
2. apply migration。
3. catalog读回两表、PK/FK/check/unique、comment。
4. 在事务内验证 publication/settlement状态/observation 的唯一键和 rollback，无永久业务行。
5. 重复 apply 验证幂等。

生产 DDL 需要用户对具体 migration/目标的独立授权；不要求每次 DDL 做生产全库备份。

### 15.2 Rollback

- 调度回滚：关闭 env 配置并由用户重启；手工 Program review 和已有 model-shadow GET 保持可用。
- 源码回滚：旧 `run_review`、replay、Selection、Paper 和模拟盘路径不依赖新表。
- DDL rollback 仅在两张新表无业务行时允许 drop；已有前向事实时拒绝破坏性回滚，采用源码停用。
- model descriptor 回滚：停止解析该 exact descriptor；不删除 bundle 或历史 observation。

## 16. Verification Plan / 验证方案

### 16.1 Backend direct tests

```text
backend/tests/advisory_model_first/test_forward_date_clock.py
backend/tests/advisory_model_first/test_forward_publication.py
backend/tests/advisory_model_first/test_forward_recovery.py
backend/tests/advisory_model_first/test_forward_postgres.py
backend/tests/advisory_model_first/test_forward_scheduler.py
backend/tests/advisory_model_first/test_dynamic_model_binding.py
backend/tests/advisory_model_first/test_forward_boundaries.py
```

必须覆盖：

- 周五 D 到周一 target、节假日 next trading day。
- publication 完全不查询 target market。
- target open缺失不创建 episode，后续价格到达后同 run结算。
- 相同请求幂等，不同 binding/selection/payload冲突。
- publication/settlement 中途异常整体回滚。
- active episode hash并发变化后拒绝旧 evaluation，重新加载成功。
- 多 Alpha exact descriptor成功，单 Alpha无 descriptor时 baseline成功且 model unavailable。
- descriptor/binding/bundle/leg mapping/hash任一不一致均 typed failure。
- model child失败不删除 M2或 baseline。
- 一个 Program失败不阻断另一个 Program。
- import/monkeypatch证明 Selection、Paper、模拟盘、QMT没有写入。

### 16.2 Frontend

- TypeScript typecheck。
- API contract tests。
- Playwright 375x812、768x1024、1440x900。
- baseline/model/settlement状态不混淆，长 reason不溢出，无失败请求或 console error。

### 16.3 Runtime acceptance

源码合入、DDL、descriptor创建、用户重启、scheduler激活和业务验收分开：

1. 用户授权 DEV/生产 migration 后读回 schema。
2. 创建并核验目标多 Alpha exact Program descriptor；单 Alpha不创建伪 descriptor。
3. 用户重启后先执行只读 health/identity。
4. 用户单独授权首次 business run 或启用 scheduler。
5. 两个 Program 同一 target 日均有真实 PUBLISHED baseline。
6. 多 Alpha有同日 challenger，单 Alpha typed unavailable。
7. target open到达后至少一个 episode创建，或所有未进入均有明确原因。
8. Paper、模拟盘、Selection既有结果和 QMT状态无 cross-write。

## 17. Design Acceptance Index

| ID | requirement |
|---|---|
| F-127 | ENABLED daily_after_close Program 按当前自然交易日幂等发布，不回填历史缺口 |
| F-128 | baseline、challenger、replay和按需GET身份分离，旧成功不冒充今日发布 |
| F-129 | forward model observation持久化完整身份、Top5、outcome、price和typed status |
| F-130 | Program/binding exact descriptor动态解析，不扫描latest，不依赖运行时target常量 |
| F-137 | 无资金/订单/QMT/Paper/模拟盘/Selection cross-write |
| F-140 | D收盘 publication 与 D+1 target-open settlement 分阶段，发布不读取target行情 |
| F-401 | P0-A runner 只处理当前自然交易日和已存在pending settlement，真实遵循显式after-close cutoff，并保证同一 Program settlement 按日期顺序阻塞 |
| F-402 | target market与suspend同步不完整时不创建episode且不做价格回退 |
| F-403 | baseline publication复用现有PUBLISHED list；结算复用现有review policy/transition/episode，不复制算法 |
| F-404 | publication和settlement各自单事务，无review/list/episode部分写入 |
| F-405 | 唯一键、覆盖逐股经济结果且排除运行时间戳的payload hash和active episode state hash处理并发；无lease/owner/审批状态机 |
| F-406 | model resolution在publication时冻结，未来descriptor不回刷既有forward day |
| F-407 | baseline与challenger分阶段失败，model失败不回滚baseline |
| F-408 | descriptor驱动runtime常量和candidate projection |
| F-409 | 当前两腿角色从descriptor读取并与bundle/Selection交叉校验；目标多Alpha模型字节不变 |
| F-410 | 单Alpha与原生多Alpha共用resolver；无真实兼容bundle时typed unavailable且baseline继续 |
| F-411 | scheduler仅处理Advisory自然当前日和pending settlement，默认关闭时不解析可选interval且无历史扫描 |
| F-412 | API/UI分开展示publication、settlement、challenger、maturity和错误 |
| F-413 | API/UI显示真实当日状态且无交易入口 |
| F-414 | 无简化版、静默错误、业务语义漂移、角色审批、二次准入或未经确认门禁 |
| F-415 | migration/rollback可重复验证，DEV-first；merge、DDL、descriptor、restart、activation分别授权和报告 |

## 18. Design Acceptance Matrix

本矩阵中的 `ready` 仅表示详细设计具有明确实现引用和可执行验证路径，不表示源码、DDL、runtime或业务验收已经完成。实现完成后必须把每行更新为实际代码/测试证据，不能沿用设计期状态申报交付。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-127 | `backend/services/advisory_forward/service.py`, `backend/services/advisory_forward/scheduler.py` | `backend/tests/advisory_model_first/test_forward_date_clock.py`, `backend/tests/advisory_model_first/test_forward_scheduler.py` | pass | none |
| F-128 | `backend/routers/advisory.py`, `frontend/src/lib/api/advisory.ts` | `backend/tests/advisory_model_first/test_forward_api.py`, `frontend/tests/paper-v2/paper-v2-advisory-ui.spec.ts` | pass | none |
| F-129 | `backend/services/advisory_forward/models.py`, `backend/services/advisory_forward/repository.py` | `backend/tests/advisory_model_first/test_forward_recovery.py`, `backend/tests/advisory_model_first/test_forward_postgres.py` | pass | none |
| F-130 | `backend/services/advisory_model_first/model_binding_resolution.py`, `scripts/advisory_publish_program_model_descriptor.py` | `backend/tests/advisory_model_first/test_dynamic_model_binding.py` | pass | none |
| F-137 | `backend/services/advisory_forward/service.py` | `backend/tests/advisory_model_first/test_forward_boundaries.py` | pass | none |
| F-140 | `backend/services/advisory_forward/service.py` | `backend/tests/advisory_model_first/test_forward_date_clock.py`, `backend/tests/advisory_model_first/test_forward_recovery.py` | pass | none |
| F-401 | `backend/services/advisory_forward/service.py`, `backend/services/advisory_forward/scheduler.py` | `backend/tests/advisory_model_first/test_forward_date_clock.py`, `backend/tests/advisory_model_first/test_forward_scheduler.py` | pass | none |
| F-402 | `backend/services/advisory_program.py`, `backend/services/advisory_forward/service.py` | `backend/tests/advisory_model_first/test_forward_boundaries.py`, `backend/tests/advisory_model_first/test_forward_recovery.py` | pass | none |
| F-403 | `backend/services/advisory_program.py`, `backend/services/advisory_forward/service.py` | `backend/tests/advisory_model_first/test_forward_publication.py`, `backend/tests/advisory_model_first/test_forward_recovery.py` | pass | none |
| F-404 | `backend/services/advisory_forward/repository.py` | `backend/tests/advisory_model_first/test_forward_postgres.py` | pass | none |
| F-405 | `backend/services/advisory_forward/service.py`, `backend/services/advisory_forward/repository.py`, `backend/services/advisory_forward/scheduler.py` | `backend/tests/advisory_model_first/test_forward_postgres.py`, `backend/tests/advisory_model_first/test_forward_recovery.py`, `backend/tests/advisory_model_first/test_forward_scheduler.py` | pass | none |
| F-406 | `backend/services/advisory_forward/service.py`, `backend/services/advisory_model_first/model_binding_resolution.py` | `backend/tests/advisory_model_first/test_forward_recovery.py`, `backend/tests/advisory_model_first/test_dynamic_model_binding.py` | pass | none |
| F-407 | `backend/services/advisory_forward/service.py`, `backend/services/advisory_forward/repository.py` | `backend/tests/advisory_model_first/test_forward_boundaries.py`, `backend/tests/advisory_model_first/test_forward_recovery.py` | pass | none |
| F-408 | `backend/services/advisory_model_first/model_binding_resolution.py`, `backend/services/advisory_model_first/model_inference.py` | `backend/tests/advisory_model_first/test_dynamic_model_binding.py`, `backend/tests/advisory_model_first/test_model_inference.py` | pass | none |
| F-409 | `backend/services/advisory_model_first/shared_feature_builder.py`, `backend/services/advisory_model_first/model_inference.py` | `backend/tests/advisory_model_first/test_dynamic_model_binding.py`, `backend/tests/advisory_model_first/test_model_inference.py` | pass | none |
| F-410 | `backend/services/advisory_model_first/model_binding_resolution.py`, `backend/services/advisory_forward/service.py` | `backend/tests/advisory_model_first/test_forward_boundaries.py`, `backend/tests/advisory_model_first/test_dynamic_model_binding.py` | pass | none |
| F-411 | `backend/services/advisory_forward/scheduler.py`, `backend/main.py` | `backend/tests/advisory_model_first/test_forward_scheduler.py`, `backend/tests/advisory_model_first/test_forward_date_clock.py` | pass | none |
| F-412 | `backend/routers/advisory.py`, `frontend/src/app/paper-v2/advisory/page.tsx` | `backend/tests/advisory_model_first/test_forward_api.py`, `frontend/tests/paper-v2/paper-v2-advisory-ui.spec.ts` | pass | none |
| F-413 | `frontend/src/app/paper-v2/advisory/page.tsx` | `frontend/tests/paper-v2/paper-v2-advisory-ui.spec.ts` | pass | none |
| F-414 | §19 repeated source review and changed scope | `python -m nox -s advisory_modeling_backend` | pass | none |
| F-415 | `backend/db/migrations/add_advisory_forward_publication_20260813.sql`, rollback migration | `backend/tests/advisory_model_first/test_forward_postgres.py` | pass | none |

## 19. DESIGN-COMPLIANCE-001 Design Review

1. **禁止简化交付**：设计覆盖两阶段真实日期、持久化、事务、调度、动态bundle、API/UI和运行验收；不以GET、mock、单Program或固定常量代替完整P0-A/P0-B。
2. **禁止静默错误**：每个阶段和model child均有typed status/reason；未知异常记录stack，不回退旧结果、默认价或规则模型。
3. **禁止业务语义漂移**：Selection rank、Program policy、episode engine、target count、Paper、模拟盘和QMT保持不变；发布list与target-open结算明确分离。
4. **禁止额外门禁审批**：仅保留输入正确性、唯一性和事务一致性校验；没有角色、人工审批、收益阈值、策略包二次准入或历史证据门禁。

正式审核记录：

- Round 1：修正F2章节/父级F-ID覆盖；冻结publication时model resolution；补充目标日`suspend_d`完整性。
- Round 2：删除与forward run和既有daily review/episode重复的settlement表；明确停机/周末不回补历史发布日期。
- Round 3：对照现有`advisory_review_run`、`PUBLISHED` list、daily review、episode和model-shadow读回约束复核，无新增逻辑缺口。
- Source Round 1：修复模型 descriptor/bundle manifest 身份未闭合、observation 时间戳伪冲突和前端 forward API失败连带主页面失败。
- Source Round 2：修复合法 active episode 变化导致永久阻塞、observation持久化失败不可恢复、Program/binding并发覆盖和 publication并发污染 settlement状态。
- Source Round 3：补齐active持仓不在候选时的WAITING发布、Program暂停状态保留、终态不可逆、确定性episode identity与scheduler同进程互斥。
- Source Round 4：补齐冻结前向Top5/outcome/holding/price UI、observation/forward交叉身份校验、最长预测成熟度和migration精确catalog读回。
- Source Round 5：对照实际测试文件与两表DDL重写本矩阵，删除不存在测试与不存在唯一约束的验收陈述；未发现简化版、静默错误、业务语义漂移或未经确认门禁审批。
- Source Round 6：修复 BUG-1057 settlement hash 只覆盖聚合计数/episode ids、可能把不同逐股价格或 action 静默判为幂等的问题；canonical payload现覆盖全部逐股经济字段并排除非确定性时间戳，同时增加终态冲突与事务rollback测试。
- Source Round 7：修复 BUG-1060 disabled scheduler 在模块import时解析可选 interval、把未启用功能变成后台启动门禁的问题；interval只在显式启动时校验，disabled import/status保持稳定。
- Source Round 8：修复 BUG-1061 设计声明的 after-close env 未被 service 使用、显式配置被静默忽略的问题；默认16:30保持不变，合法配置真实控制 publication due，非法值明确失败。
- Source Round 9：修复 BUG-1063 同一 Program 较早 settlement 阻塞后仍执行更晚日期、可能倒序改变 episode 状态的问题；runner 现在按 Program 传播本轮阻塞并返回明确 skip，其他 Program 不受影响。
- Source Round 10：修复 BUG-1065 并发首次保存同一 forward observation 时共享父锁无法阻止 child unique-key 竞争的问题；父行改为排它短锁，相同重放按既有payload幂等合同收敛。

## 20. Implementation Plan / 本阶段唯一实施顺序

1. 本详细设计通过F2 validator和正式审核。
2. 实现migration/model/service/repository/scheduler/API/UI及定向测试。
3. 执行两轮代码审核与修复，完成DESIGN-COMPLIANCE-001逐项核对。
4. 建立P0-A/P0-B阶段PR；合入由用户单独确认。
5. DEV/生产DDL、repo-external descriptor、用户重启、scheduler/business activation分别等待相应授权。
6. 完成两个现有Program单日真实publish和target-open readback后，P0-A/P0-B才从source complete转为runtime verified。
7. 不等待自然前向样本成熟即可并行进入P0-C详细设计；P0-D模型进入challenger必须以P0-A/P0-B运行链已可消费为前提。

## 21. Risks / 风险与失败模式

| 风险 | 可见行为 | 处理方式 |
|---|---|---|
| 目标日行情或`suspend_d`尚未同步 | settlement=`WAITING_DATA`并保存数据集/reason | 后续tick按同forward run重试，不回退价格 |
| 两个scheduler tick并发 | 唯一键或Program row lock命中 | 相同payload返回幂等结果，不同payload返回conflict |
| Selection耗时期间binding变化 | publication commit检测版本不一致 | 丢弃旧计算并在下一tick重算，不发布混合身份 |
| publication事务中途失败 | 事务整体回滚 | attempt保留FAILED reason，下一tick重试 |
| settlement evaluation后active episode变化 | state hash冲突 | 重新加载后重算，不提交过期决策 |
| descriptor缺失或损坏 | baseline PUBLISHED；model observation UNAVAILABLE/FAILED | 不扫描latest，不复制其它Program模型 |
| outcome/price child失败 | M2 challenger仍保存，child typed unavailable | 不删除baseline或已成功父输出 |
| scheduler未激活 | API明确显示configured/started/last-run分别状态 | 不把ENABLED Program或源码合入冒充已调度 |
| 前向样本未成熟 | metrics=`EVIDENCE_IMMATURE` | 不回看冻结test调参，不形成业务门禁 |

## 22. Production Gates / 生产影响与独立授权

本阶段不新增业务运行门禁或审批。以下只是动作边界，并分别记录状态：

| action | source交付时状态 | 独立授权 |
|---|---|---|
| 源码/PR合入 | pending user confirmation | 必须 |
| DEV migration apply/readback | pending | 必须 |
| 生产migration apply/readback | pending specific migration/target authorization | 必须 |
| 目标多Alpha Program descriptor写入 | pending exact artifact authorization | 必须 |
| 后端重启 | user-owned | 必须由用户执行或明确授权当前目标 |
| scheduler env激活 | disabled by default | 必须 |
| 首次forward business write | pending runtime acceptance authorization | 必须 |
| dependency install | noop | 不需要 |
| Paper/QMT/模拟盘runtime | noop | 不允许触碰 |

DDL前不设置每次生产全库备份门禁；数据库连接只从权威`.env`读取，不猜测、不输出密钥。
