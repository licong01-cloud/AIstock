# AIstock 荐股 Phase 0A.2 前瞻证据就绪、每日多 Program 执行与真实双轨验证 F2 详细设计

> 日期：2026-07-11
> Feature Tier：F2
> Task Tier：T3 设计驱动
> Module：Advisory / StrategyPackage / Selection evidence / Daily Program runner / Phase 0A audit / Phase 1 handoff
> Risk Level：高；涉及跨模块证据生产契约、Program binding 生命周期、每日执行幂等性和后续生产 DML
> Phase：0A.2，位于 Phase 0A.1 deterministic handoff 与 Phase 1 数据底座之间
> 父蓝图：`docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md`
> 前置设计：`docs/architecture/advisory_phase0a_candidate_authority_oos_data_availability_f1_design_20260710.md`
> 后继设计：`docs/architecture/advisory_phase1_pit_observation_labels_sealed_snapshot_f2_design_20260711.md`
> 当前状态：`design_ready`；本文只完成设计，不代表代码、策略包、Program、binding、生产 DML 或 Phase 1 source ledger 已产生
> 生产影响：本文为文档变更；DDL、DML、依赖、服务、调度、API、UI 和运行时均为 `noop`

## 0. 文档定位与权威边界

本文解决 2026-07-11 首次真实 L4 只读审计暴露的阶段断点：Phase 0A.1 已能对输入进行确定性 readiness/handoff 归一化，但现有生产记录没有足够的历史 binding、冻结 policy、决策时钟、runtime/HMM、PIT universe 和 source available-at 证据，因此合法策略包也无法从旧记录直接得到可消费 handoff。

Phase 0A.2 不降低 Phase 0A 判定标准，也不通过人工批准绕过阻塞。它补齐唯一上游 producer 和每日多 Program 执行器，使未来正确运行自然产生 Phase 0A 所要求的证据；同时为当前启用的单 Alpha 包和原生多 Alpha 父包建立相同体验、相互独立的真实验证路径。

权威优先级：

1. 用户已经确认的业务边界：多个策略包独立执行荐股；只支持单 Alpha 包和原生多 Alpha 父包；零人工审批、零角色授权、零运行时 DDL。
2. 父蓝图对数据隔离、PIT、OOS、模型阶段和 8 类自动技术门禁的定义。
3. Phase 0A 对 candidate authority、binding as-of、policy、runtime/HMM vintage、OOS 和 handoff 的冻结规则。
4. Phase 1 对 source availability、observation/version、label 和 SEALED snapshot 的唯一数据契约。
5. 2026-07-11 L4 只读审计及当前数据库、代码、不可变制品能够证明的事实。

本文不是新的全局开发标准，也不创建第二套 source ledger、policy 口径或审批体系。与 Phase 0A/1 冲突时，必须先同步修改这些正式设计，不能在实现代码中私自选择较宽松语义。

## 1. Background / 背景与真实审计结论

### 1.1 已执行的 L4 只读审计

2026-07-11 对当前启用的原生多 Alpha Program 执行了生产数据库只读探针：

```text
audit_id = l4_probe_multi_alpha_20260711
read_only = true
audit_manifest_hash = 6ace3066b142e5158e1f4b076e02865382ec13ffa166f789131d80e5edead4a0
handoff_readiness = BLOCKED
handoff_readiness_hash = 2d3a5d9222d3b491837bba7f1204de8008b1cf3e0ae3a51ed6eaa0d39d8f2ebf
formal_oos_intervals = 0
retrospective_intervals = 0
gap_intervals = 1
```

该探针没有执行 DDL/DML、没有重启服务、没有生成 Selection 或 Advisory 业务记录。它证明 resolver 能在真实 schema 上 fail-closed，但不证明当前 target 已可进入 Phase 1。

主要阻塞不是策略包资产不可执行，而是以下证据生产链不存在或不完整：

- 正式 `Phase0APolicyRegistry` 只有 `policy_version`，benchmark、cost、label、universe、embargo、prior 和 multiple-testing registry 未冻结。
- 现有 binding 的 `effective_from_trade_date/effective_to_trade_date` 为空，无法按历史 T 唯一解析。
- `DailySelectionEvidence` 没有完整 canonical decision clock、显式时区、data available-at、`phase0a_effective_config_chain` 和 runtime activation vintage。
- HMM、PIT universe、risk policy、package asset available-at 和 source available-at 证据不完整。
- 当前正式候选链不能表达合法空候选日。
- 没有真实 binding 切换样本，也没有可验证的 retired interval。
- `review_schedule={"frequency":"daily_after_close"}` 仅作为 Program metadata 保存；代码中没有按交易日枚举全部启用 Program 并调用正式 review 的调度器。

### 1.2 当前包与 binding 事实

L4 调查时的生产事实如下；这些 ID 只作为现状证据，执行时必须重新解析，不能当作永久配置：

| 类型 | Program/package | 当前证据结论 |
|---|---|---|
| 启用单 Alpha | `advp_ac2885f728a84409a263d30f06664196` / `pkg_378eb9c91e104c64935404e257e932ee` | Program 已启用；包当前 manifest 为 `2aae3560563bd669e5f1951c40ae939744f82a67be5b7479f239b9f910270300`，StrategyPackage selection readiness 返回 `ok=true` 且无 blocker，可作为 current-manifest 冷启动 target；其历史 evidence 不得自动继承 |
| 启用原生多 Alpha | `advp_1f537362f2f447e3882c3a7459c5726a` / `pkg_ma_8ec5e389fa2c5e484a1ac7e9` | 当前 manifest `f5b008d09fa1c36a1f3604333dee62fa66ba3c692fa07239b57e5690debb6016` 与选择证据一致，可作为前瞻验证父包；旧 null-date binding 不能作为正式历史 |
| 已归档单 Alpha 1 | `pkg_a2f53f3f2f3e4095a910b939464c35e6` | 当前 manifest 与历史选择 evidence manifest 不一致，且包已退役；不得恢复为 Phase 0A.2 正向 target |
| 已归档单 Alpha 2 | `pkg_09750b4944ca434db03efd399ccf2144` | 当前 manifest 与历史选择 evidence manifest 不一致，且包已退役；不得恢复为 Phase 0A.2 正向 target |

所有已观察 binding 的生效区间均为空。数据库中也没有可用于 L4 的合法空候选日或 binding rollover 样本。

单 Alpha 包当前 manifest 有 58 个受保护资产记录且已观察 hash 均存在，并有 7 个绑定当前 manifest 的 runtime release；最近 release 日期为 2026-07-10、状态为 `SIM_VALIDATING`。但当前 manifest 尚无 `DailySelectionEvidence`。已观察的 67 条历史 DSE 全部引用旧 manifest `8f6d8b0235459a0b657a3c0bb3a00e9a63707578e0bd7de978add42855d31ebf`，因此只能作为旧 signal identity 的 retrospective evidence，不能证明当前 manifest 已完成荐股冷启动。

两个当前 target 的 package asset metadata 均未提供可证明历史时点的 `available_at/data_cutoff`；正式 policy registry 和 Phase 1 source availability ledger 也尚未建立。这些缺口必须从正式 `T0` 前瞻记录，禁止用当前存在性反推过去。

### 1.3 历史窗口与调度事实

截至 2026-07-11 的只读快照中，单 Alpha 最近可见 DSE cutoff 为 `2026-06-15/16/17/18/22/23/24/25/26/29`，原生多 Alpha 最近可见 DSE cutoff 为 `2026-06-29、2026-07-02/03/06/07/08/09`。两条轨道在用户提出的两至三周观察窗口内只有 `2026-06-29` 一个精确共同 cutoff，不存在连续的双轨历史日序列。

现有多 Alpha 的 `2026-07-07` 至 `2026-07-10` review 记录均在 2026-07-10 晚间集中创建，说明它们是人工触发或回补记录，不是逐日 `daily_after_close` 前瞻执行证据。现有 replay 可以重算日期区间，但会写入 replay/review/list/Selection 相关记录，并按当前代码、当前 manifest 和当前配置解释历史；它只能标记为 `RETROSPECTIVE_RESEARCH_ONLY`，不能替代正式日调度或生成 formal OOS。

### 1.4 设计结论

不能通过下列方式把现有记录变成正式证据：

- 用当前 active binding 覆盖过去日期。
- 把 `created_at`、当前 manifest、文件 mtime 或当前 runtime hash 写成历史 available-at。
- 把已归档包当前 manifest 写回历史选择 evidence。
- 从回测、Paper、模拟盘收益或人工买入结果提取 cutoff、标签或 prior。
- 用人工确认、角色、审批记录或 bypass 把 `BLOCKED` 改成 `READY`。

正确路径是前瞻积累：先对当前单 Alpha manifest 和原生多 Alpha父包执行无业务写入的 current-manifest cold-start smoke；在首次 signal 之前冻结 policy、显式 dated binding、每日执行契约和 runtime/source evidence contract；之后每个交易日由正式 runner 调用 Selection producer 生成不可变证据。初期允许得到可消费的 `PARTIAL -> HANDOFF_EMITTED`，但只有满足 Phase 1 source ledger 和 Phase 0A embargo 后才升级为 `FORMAL_OOS + READY`。

## 2. Scope / 范围

### 2.1 In Scope

- 定义当前启用单 Alpha StrategyPackage 的 current-manifest 冷启动与证据隔离规则；只有当前包不再满足标准 preflight 时才通过标准发布流程创建新 identity。
- 复用当前启用的原生多 Alpha 父包，以新的显式 dated binding 开始前瞻证据区间。
- 为两个包各建或保留独立 Advisory Program；允许将来继续增加更多独立 Program。
- 新增按权威交易日、行情就绪水位和稳定业务键运行全部启用 Program 的每日执行契约，并定义单 Program 失败隔离、重试、恢复和批次回执。
- 冻结正式 Phase 0A policy registry 的文件、hash、effective range 和加载规则。
- 统一新建、更新、克隆、缺失绑定补建和显式 apply 的 binding 生效区间算法。
- 扩展正式 Selection evidence producer，使其保存 Phase 0A 所需 decision clock、runtime/config、HMM、PIT universe、risk、asset/source 和 stage lineage。
- 定义 `VALID_NO_CANDIDATE` 权威证据，不要求生产中人为制造空候选。
- 明确 Phase 0A.2 与 Phase 1 `advisory_source_availability_event` 的衔接，消除 Phase 0A/1 循环依赖。
- 定义只允许 exact-source 的历史修复规则、前瞻验证顺序、自动门禁和正向可达性。
- 冻结正式 `T0`、现存历史证据读取和 current-semantics replay 的分类边界。
- 给出未来代码、测试、生产程序化 DML 和 L4 复验的分阶段实施计划。

### 2.2 影响面

未来实现预计触及：

```text
backend/services/advisory_phase0a/
backend/services/advisory_program.py
backend/services/advisory_daily_runner.py
backend/services/selection_center/
backend/services/simulation_runtime/selection.py
backend/services/strategy_package/selection_artifact.py
backend/routers/advisory.py
frontend/src/app/paper-v2/advisory/page.tsx
backend/tests/advisory_phase0a/
backend/tests/watchlist/
backend/tests/selection_center/
scripts/advisory_phase0a_audit.py
```

具体写入范围必须在实现 Feature Card/工作树中按 ownership catalog 再确认。Phase 1 DDL/source ledger 仍归 Phase 1 设计所有，不在 Phase 0A.2 重复实现。

## 3. Non-Goals / 非目标

- 不在本文档变更中写业务代码、创建策略包、修改 Program/binding 或写生产数据库。
- 不复活、改写或重新命名两个已归档单 Alpha 包。
- 不创建只为通过测试的虚假模型、空资产包、伪造候选或伪造空候选日。
- 不要求当前单 Alpha包仅因历史 DSE 使用旧 manifest 就重新发布；是否需要新 identity 由 current-manifest cold-start 结果确定。
- 不恢复页面内手工跨包融合；一个 Program 仍只绑定一个单 Alpha 包或一个原生多 Alpha 父包。
- 不限制系统只能有一个策略包；不同 Program 可并行、独立执行和独立失败。
- 不改变 StrategyPackage 在 Selection、模拟盘或 Paper 中的 manifest/runtime 语义。
- 不读取回测结果、QE archive、Paper/模拟盘账户、订单、持仓或收益作为荐股训练/审计证据。
- 不建设 Phase 1 observation/label/snapshot 全量数据底座，不训练模型、不预测收益/持股周期/价格区间。
- 不新增审批、RBAC、authority decision、approval bundle、action authorization 或运行时 DDL。
- 不保证实施当天即可得到 formal OOS；embargo 和未来 source evidence 必须按交易日自然成熟。
- 不把两至三周前的 replay 日期设置为正式 `T0`，也不把集中回补记录解释为逐日前瞻运行。

## 4. Design Acceptance Index / 设计验收索引

| ID | Phase 0A.2 设计验收项 |
|---|---|
| F-031 | 当前单 Alpha target 按 current manifest 冷启动并与旧 manifest evidence 隔离；仅在 preflight 不通过时发布新 identity，归档包不被复活 |
| F-032 | 现有原生多 Alpha 父包可直接复用，并通过新的 dated binding 与其他 Program 独立执行 |
| F-033 | 所有新 binding 使用显式、无重叠的 `[effective_from_trade_date,effective_to_trade_date)`；legacy null 区间不被反推 |
| F-034 | 正式 Phase 0A policy registry 在首次 signal 前自动校验并冻结 hash/effective range，不含审批或角色字段 |
| F-035 | Selection producer 前瞻保存 decision clock、effective config、runtime/HMM、PIT universe、risk、asset/source 和完整 lineage |
| F-036 | 合法空候选具有不可变权威 header/artifact/evidence；真实验证等待自然事件，不伪造生产样本 |
| F-037 | Phase 0A.2 复用 Phase 1 source ledger；历史修复只接受 exact source，缺证据保持 retrospective/unavailable |
| F-038 | 正确数据存在自动可达的双轨正向路径：先 `PARTIAL -> HANDOFF_EMITTED`，满足 source ledger/embargo 后再 `READY`，且不影响 Selection/模拟盘/Paper |
| F-039 | 每日 runner 对全部启用 Program 独立执行，使用一个正式 Program/date 业务键、原子或可恢复持久化、失败隔离和确定性批次回执 |
| F-040 | 正式 `T0` 只能位于代码/策略/dated binding/runner 就绪后的首个未处理交易日；历史读取与 current-semantics replay 永久保持 retrospective |

## 5. Architecture / 总体架构

### 5.1 阶段位置

```text
Phase 0A read-only audit
  -> Phase 0A.1 deterministic readiness/handoff normalization
  -> Phase 0A.2 policy + dated binding + prospective evidence producers
  -> Phase 1 source ledger + observation/label/snapshot
  -> Phase 0B baseline audit
```

Phase 0A.2 不是新的数据仓库。它只补齐“正式业务运行如何产生可被 Phase 0A/1 消费的证据”。

### 5.2 双轨 target

```text
Track S: existing single-alpha package at current manifest
  -> existing dedicated Advisory Program S
  -> current-manifest cold-start smoke
  -> explicit successor binding S_v2 [T0, infinity)
  -> independent daily Selection evidence

Track M: existing native multi-alpha parent package
  -> existing Advisory Program M
  -> new explicit successor binding M_v2 [T0, infinity)
  -> independent daily Selection evidence with per-leg provenance
```

两条轨道可以在同一 audit request 中批量审计，但不共享 Program/binding lineage，不融合候选，也不把一条轨道的成功作为另一条轨道的 fallback。等价经济 signal 的训练样本去重仍由 Phase 0A.1 stable signal identity 处理。

### 5.3 循环依赖消除

Phase 1 source ledger 尚未启用时，正确且完整的包、binding、policy、clock、runtime 和 candidate authority 可以得到 `PARTIAL` handoff。该 handoff 允许 Phase 1 建立 prospective source availability/capture，不宣称 formal OOS。

Phase 1 observer 开始追加真实 `advisory_source_availability_event` 后，新 audit version 才能逐步形成 source-complete interval。达到 effective cutoff 后 20 个完整交易日 embargo，并满足全部 mandatory closure 时，scope 才从 `PARTIAL` 变为 `READY`。

因此 Phase 1 的进入条件调整为：

- 至少一个合法 `READY` 或 `PARTIAL` admission scope 已 `HANDOFF_EMITTED`。
- `PARTIAL` scope 只能执行与缺失 capability 对应的 source/capture 建设，不能进入 formal Phase 0B 指标。
- `BLOCKED` scope 不进入 Phase 1；修复 producer 后创建新 audit/handoff version。

## 6. StrategyPackage 与 Program 双轨契约

### 6.1 当前单 Alpha package 的 current-manifest 冷启动

真实 L4 单 Alpha 正向验证优先使用当前已启用 Program/package，而不是无条件再发布一个 package。正式 `T0` 前必须对 package `pkg_378eb9c91e104c64935404e257e932ee` 的当前 manifest 执行无生产业务写入的 preflight 与隔离 Selection smoke，并满足：

```text
program_id = advp_ac2885f728a84409a263d30f06664196
package_id = pkg_378eb9c91e104c64935404e257e932ee
manifest_sha256 = 2aae3560563bd669e5f1951c40ae939744f82a67be5b7479f239b9f910270300
alpha_mode = single_alpha
enabled = true
manifest_sha256 == manifest canonical hash
protected factor/model/preprocess assets all present
asset ids/hashes/CAS refs immutable
current-manifest runtime profile release id/version/hash resolvable
package created/frozen/available timestamps auditable
standard StrategyPackage preflight PASS
isolated current-manifest Selection smoke PASS
```

current-manifest smoke 只能证明从 smoke observed-at 开始具备可执行性，不能把该时间回填为资产的历史 available-at。旧 manifest `8f6d8b0235459a0b657a3c0bb3a00e9a63707578e0bd7de978add42855d31ebf` 的 67 条 DSE 保留原 identity 和 retrospective 分类，不得被当前 manifest 继承。

smoke 通过时复用现有 package 和独立 `single_package` Program，只创建从正式 `T0` 生效的 dated successor binding。smoke 若证明当前 manifest/资产闭包不可执行，才通过标准研发、发布和 StrategyPackage preflight 形成新的 package identity；不得原地复活两个已归档包，也不得继承旧包 OOS 身份。

Program 的 target count、review policy、HMM/risk runtime 和 style assignment 在首个 signal 前冻结；style 不根据未来收益回改。

### 6.2 现有原生多 Alpha parent

执行时重新解析 `pkg_ma_8ec5e389fa2c5e484a1ac7e9`。仅在以下条件仍成立时复用：

- package enabled，`alpha_mode=multi_alpha`。
- current manifest 与 registry manifest 精确一致。
- 所有 parent-owned leg、factor/model/preprocess 和 combine policy assets 可解析且 hash 完整。
- 每个 leg 的 score direction、weight、variant 和 combine order 可由 parent manifest 唯一确定。
- preflight 不使用单 Alpha fallback，也不展开为多个 Program。

不重新发布父包、不修改其 manifest。对现有 Program 创建新的 successor binding，并从未来的明确决策交易日开始采集证据。

### 6.3 多 Program 独立性

- 系统可以同时启用任意数量的合规 Program。
- 每个 Program 每个交易日独立获取 run key、candidate evidence、list version 和状态。
- 一个 Program 失败不回滚或阻塞其他 Program。
- 页面可同时展示多个 Program，但不提供跨 Program 候选融合或随机权重配置。
- Phase 0A audit 可批量执行，readiness、reason codes 和 handoff 按 scope 独立。

### 6.4 每日多 Program 执行器

现有 `review_schedule` 只是配置 metadata，不是执行器。Phase 0A.2 必须提供一个确定性的 Advisory daily runner；它只编排现有正式单包 Selection/Advisory 路径，不在 runner 内实现第二套选股算法。

每日流程固定为：

1. 在权威交易日 `T` 收盘数据及必要 ingestion/HMM/risk 输入达到冻结水位后，按交易日历计算 `target_trade_date=E(T+1)`。
2. 在一个一致读快照中获取全部 `ENABLED` Program id，按 `program_id` 稳定排序；快照之后新启用的 Program 从下一交易日开始。
3. 每个 Program 独立解析 `T` 生效的唯一 dated binding、当前 package/manifest、policy、runtime 和 source watermark；禁止使用 null-date binding、latest fallback 或手工 candidates。
4. 每个 Program 调用正式 `run_review_from_selection` 路径，持久化 Selection artifact/DSE、review/list 和 Phase 0A evidence；一个 Program 失败不回滚已成功 Program，也不阻止后续 Program。
5. 输出一个 batch receipt，逐 Program 保存 `SUCCEEDED/ALREADY_SUCCEEDED/WAITING_INPUT/FAILED`、业务键、binding/manifest/config hashes、Selection/review/list ids、reason codes、开始/结束时间和重试分类。

正式 daily run 的唯一业务键与现有数据库约束保持一致：

```text
daily_run_key = (program_id, target_trade_date, RUN)
```

`binding_version_id`、`decision_as_of_trade_date=T`、manifest、policy 和 effective config hashes 是该 key 的不可变 payload/conflict predicate，而不是允许同一 Program/date 产生第二个正式列表的额外维度。现有 `ux_advisory_review_run_one_run_per_program_date` 与 `ux_advisory_list_version_one_published_per_program_date` 是最终数据库幂等权威；人工正式触发与 scheduler 使用同一 key，先成功者产生结果，后到者返回 `ALREADY_SUCCEEDED`。

run acquisition 与 review/list/item/episode/decision 持久化必须在单 Program 事务中原子提交，或使用同一确定性 run id 的可恢复状态机完成；不得保留“review run 已写但 published list 永远无法重试”的半成品。输入尚未就绪时只写 runner receipt 并重试，不抢占正式 `RUN` key；已抢占后的进程崩溃必须按相同 key 恢复，不能另建第二条正式 run。

`REPLAY`、`PREVIEW` 与正式 `RUN` 分离。Replay 可有独立 request hash 和多次 attempt，但不能写 `PUBLISHED` list、不能占用 daily key，也不能被 source observer 识别为 prospective event。

## 7. Binding 生命周期契约

### 7.1 日期语义

`effective_from_trade_date/effective_to_trade_date` 统一表示 `decision_as_of_trade_date=T` 的左闭右开区间：

```text
binding applies when from <= T < to
```

`effective_to_trade_date` 等于 successor 的 `effective_from_trade_date`，不是“最后一个仍有效交易日”。数据库 comment、模型和 API 必须使用相同语义。

### 7.2 新 binding 生效日算法

正式路径禁止传 null。服务使用权威交易日历和 Program 已落库的 decision run 状态，确定：

```text
effective_from_trade_date = first trading date
  that is strictly after the latest already acquired/emitted decision date
  and is not earlier than the next calendar trading date at activation cutoff
```

如果同一交易日的正式 run key 已被获取，即使业务结果失败，新 binding 也不能追溯占用该日。页面只展示服务返回的默认日期；后端仍是唯一校验权威。任何显式请求早于该日期均返回稳定 reason code，不自动改成 latest/current date。

正式 `T0` 是同时满足以下条件后的首个未处理决策交易日 `T`：代码 release 已部署且 health 通过；policy version 已生效；目标 current manifest cold-start smoke 通过；Program successor binding 明确从 `T` 生效；daily runner 已启用；`T` 的输入可按冻结 cutoff 证明 available。`T0` 对应的荐股 target 为下一交易日 `E(T0+1)`。任何两至三周前的日期、当前日之前的历史 cutoff 或集中回补日期都不能被指定为正式 `T0`。

### 7.3 创建、更新、克隆与补建

- `create_program`：创建 Program 与首个 dated active binding；二者同一事务提交。
- `update_program`：仅当 package/runtime 语义变化时创建 successor dated binding；纯名称等非语义字段不切 binding。
- `clone_program`：新 Program 获取新的 dated binding 和独立 lineage，不复制旧 Program 的历史区间。
- `apply_binding`：要求显式或由服务确定的未来 effective date，使用 expected Program/binding version 防并发覆盖。
- `_ensure_active_binding`：不再静默生成 null-date active binding；缺失时生成明确 repair reason，只有强类型 repair command 可创建未来 dated binding。
- activate successor：先对 `app.advisory_program` 目标行执行 `SELECT ... FOR UPDATE`，再在同一事务内复核 expected Program/binding version、查询全部相交 interval、retire 原 active binding、设置其 `to=new.from` 并插入 successor；任一重叠、空区间或版本冲突使整个事务回滚。所有 binding writer 必须经过该锁顺序。

### 7.4 legacy null binding

- 保留原行，不原地补写猜测日期。
- null-date 行不能支持 formal as-of resolution。
- 若 exact source 能证明其真实区间，可由一次性 remediation request 追加确定性证据或新版本；不能仅依赖 `created_at/activated_at` 推断交易日。
- 正常迁移路径是从未来日期创建 successor；旧区间继续标记 `RETROSPECTIVE_RESEARCH_ONLY` 或 `UNAVAILABLE`。

## 8. 正式 Policy Registry 契约

### 8.1 存储与版本

Phase 0A.2 使用 repo-tracked immutable JSON registry，建议路径：

```text
backend/services/advisory_phase0a/policy_registry/<policy_id>/<version>.json
```

它是模块级业务配置，不是全局标准。运行时只读，不在数据库中复制一套 authority 表，也不提供通用在线编辑器。

最小 schema：

```text
schema_version
policy_registry_id
policy_version
serializer_version
frozen_at
effective_from_trade_date
effective_to_trade_date nullable
benchmark_policy
cost_policy
label_policy
universe_policy
embargo_policy
prior_policy
multiple_testing_policy
style_assignment_policy
registry_content_hash
```

`registry_content_hash` 对排除自身后的 canonical payload 计算。文件路径中的 id/version、payload id/version 和 hash 必须一致。已发布文件禁止覆盖；变更创建新 version 和 effective range。

### 8.2 v1 冻结来源

- benchmark 主口径复用 Phase 0A §14.5 的 `PIT_ELIGIBLE_UNIVERSE_EQ_WEIGHT_TOTAL_RETURN_V1`，外部指数只作 diagnostic。
- cost policy 必须显式列出买卖佣金、最低佣金、印花税、过户费、slippage/impact、lot/reference notional 和 effective range；禁止零成本默认。
- label/horizon、entry、barrier、terminal/censor 和 projection 复用 Phase 0A §14.3-14.4。
- embargo 固定 `ADVISORY_RESEARCH_EMBARGO_V1.minimum_trading_day_gap=20`。
- prior/multiple-testing registry 在未存在合法 prior 时仍必须显式保存 `entries=[]`、冻结时间和非空 registry hash；“没有 prior”不是“缺 registry”。
- calendar version/hash 必须来自实际交易日历 snapshot，不写死占位 hash。

### 8.3 自动加载与禁止字段

- audit request 引用 exact policy id/version/hash；loader 不接受 formal audit 使用任意 scratch JSON。
- 首个 signal producer 同时记录 policy hash 和 effective range；policy 未生效或 hash 不匹配时不产生 formal evidence。
- registry 不包含 `approved_by`、role、approval status、decision chain、revoke、action authorization 或人工签名。
- 配置文件由开发/发布流程部署；运行时没有修改或 DDL 权限。

## 9. Prospective Selection Evidence 契约

### 9.1 原则

现有业务结果不依赖 Phase 0A audit 是否运行。正式 Selection producer 在正常生成 artifact/DSE 时旁路写入完整、不可变、可 hash 的证据；capture 失败必须 fail loud 并只影响对应 evidence readiness，不能静默伪造字段或改变 Selection/模拟盘/Paper 的候选结果。

### 9.2 Canonical decision clock

每条 evidence 必须包含：

```text
decision_as_of_trade_date = T
target_trade_date = E(T+1)
effective_entry_trade_date = E
score_trade_date = T
reference_price_trade_date = T
decision_cutoff_ts with Asia/Shanghai offset
data_available_at with offset
decision_generated_at with offset
calendar_version/calendar_hash
requested/effective cutoff
immediate_after_data_refresh flag
```

`data_available_at <= decision_cutoff_ts` 才能支持 formal。时间戳无 timezone、target 不是下一交易日、或 T+1 数据进入 T feature 均 fail-closed。

### 9.3 Effective config/runtime chain

`phase0a_effective_config_chain` 保存按优先级排序的每一层，而不只保存 merge 后 JSON：

```text
layer_role
source_id/version/hash
available_at/effective range
semantic payload hash
overridden field paths
final_effective_config_hash
selection_runtime_semantics_id
adapter/query/code release ids and hashes
```

正式路径必须使用已发布 runtime profile binding。generated preview/default binding 仍可运行业务基线，但只能产生 retrospective/partial evidence，不能被 audit 提升为 formal。

### 9.4 HMM、risk 与 PIT universe

- HMM disabled 保存显式 `enabled=false/status=NOT_APPLICABLE`。
- HMM enabled 保存 exact snapshot/config、model/coefficient hash、trained/available/effective time、training information cutoff、signal preset、generation mode 和 input max-date hash；dynamic latest 不支持 formal。
- risk/行业黑名单/tradability 保存 policy id/hash、available-at、输入/输出 count、symbol-set hash 和逐 reason count。
- PIT universe 每层保存 policy id/hash、effective/available time、input/output/excluded count、symbol-set hash 和 source revision refs。
- package cohort 只用于 prior/survivorship 审计，不根据目标收益动态删包。

### 9.5 Package 与 stage lineage

单 Alpha evidence 保存 package/manifest、factor/model/preprocess 和 protected asset closure。原生多 Alpha另保存每个 leg 的 alpha/model/factor/schema/hash/available-at、component weight、score direction、variant、combine method/order 和 parent parity hash。

五层 stage evidence 仍为：

```text
alpha_raw
hmm_adjusted
risk_policy_adjusted
selection_effective
advisory_model
```

Phase 0A.2 只要求前四层按实际启用能力保存 candidate count、rank/score rows 和 content hash；disabled stage 标记 N/A，不复制前一层冒充。`advisory_model` 固定 unavailable。

### 9.6 Source evidence 与 Phase 1 衔接

producer 保存本次 signal 使用的 source role、dataset/partition、query template、parameter hash、业务日期、schema fingerprint、row count、partition/content hash、first observed time 和 refresh/job refs。该 payload 是 Phase 1 `advisory_source_revision_set` 的输入，不替代 `advisory_source_availability_event`。

Phase 1 observer 尚未启用时，这些字段可以使 identity/clock/candidate authority 闭合并产生 `PARTIAL`，但不得单独宣称历史 formal available-at。observer 启用后，evidence 必须引用匹配的 append-only availability event，才能升级对应 source capability。

当前 package asset 未记录历史 `available_at/data_cutoff` 时，current-manifest smoke 只追加 `first_observed_at` 和完整 asset/hash closure，并从 `T0` 起作为 prospective identity evidence；不得补写资产在旧 DSE cutoff 前已可用。正式 source observer 同样只从首次真实观察开始追加 event。

## 10. VALID_NO_CANDIDATE 契约

### 10.1 权威表达

正常 Selection pipeline 执行成功但零只股票通过时，允许生成：

```text
SelectionScoreArtifact.status = SUCCEEDED
scores_json = []
metadata.candidate_outcome = VALID_NO_CANDIDATE
metadata.authority_scope = authoritative single-package selection
metadata.universe_input_count > 0
metadata.stage/filter counts complete
metadata.stage/content hashes complete

DailySelectionEvidence.candidate_count = 0
evidence_payload.valid_no_candidate = true
evidence_payload.selection_score_artifact id/hash
evidence_payload.decision/runtime/universe/policy closure complete
```

合法空候选产生 observation/header、universe coverage 和 list-version no-candidate 状态，不生成候选 item，不创建虚假股票，也不复用上一日名单作为当天新候选。

### 10.2 自然样本规则

- 生产验证等待策略和行情自然产生零候选日。
- 不通过调高过滤阈值、修改包、注入空 scores 或手工删候选制造 L4 样本。
- 单元/集成 fixture 必须覆盖空候选正向契约，但 fixture 不能替代真实 L4 observation。
- 在真实样本出现前，该专项 evidence 标记 `NOT_OBSERVED`，不阻塞非空候选的 `PARTIAL/HANDOFF_EMITTED`。

## 11. 历史修复与数据治理

### 11.1 可接受的 exact source

历史 remediation 只接受能够逐字段证明当时身份和 available-at 的来源，例如：

- 当时不可变 manifest/CAS receipt 与受保护资产记录。
- 已存在且 hash 匹配的 SelectionScoreArtifact/DailySelectionEvidence/SelectionRun。
- 当时 runtime release/activation record。
- 带 provider/job/first-observed/revision/hash 的 ingestion event。
- 明确的 Program binding transaction/effective-date record。

修复程序必须强类型、幂等、小事务执行，并输出 planned/inserted/idempotent/conflict/failed counts、before/after hashes 和 readback receipt。

### 11.2 永久禁止的推断

- `created_at` 或 `activated_at` 自动映射为历史 decision trade date。
- 当前 manifest/hash 覆盖历史 manifest mismatch。
- 文件 mtime、当前 Git commit 或当前数据库存在性证明过去可用。
- 从回测 sample_end/raw_metrics、Paper/模拟盘表现或人工观察推断 cutoff/prior。
- 把 null binding 区间平均分配或按相邻记录猜测。

无法 exact 修复的记录原样保留，并按 Phase 0A 输出 `RETROSPECTIVE_RESEARCH_ONLY`、`NONE` 或对应 gap reason code。

### 11.3 历史读取、区间 replay 与正式前瞻的隔离

- 只读历史分析可直接消费已存在、identity/hash 匹配的 DSE；两条当前轨道若要求同日横向比较，现阶段仅 `2026-06-29` 是已观察的精确共同 cutoff。
- 每个 Program 可按自己的真实 DSE 日期独立观察列表变化，不要求伪造连续共同窗口。
- 使用当前 manifest/runtime/code 重算过去区间必须标记 `run_type=REPLAY`、`evidence_scope=RETROSPECTIVE_RESEARCH_ONLY` 和 current-semantics hashes；它回答“当前算法若作用于过去数据会怎样”，不回答“当时真实会荐出什么”。
- 现有 replay 会产生数据库写入，只有在用户另行明确授权生产 DML 后才能执行；文档、只读 audit 或模型训练准备不得隐式触发。
- 正式 `RUN`、`PUBLISHED` list、prospective source event 和 formal OOS 只接受 `T0` 及之后的 daily runner 证据。

## 12. Contracts / API、DB、UI 与 CLI 契约

### 12.1 API

未来 Program create/update/clone/apply binding API 必须返回：

```text
binding_version_id
effective_from_trade_date
effective_to_trade_date
binding_interval_semantics = LEFT_CLOSED_RIGHT_OPEN
program_version
binding_payload_hash
runtime_profile id/version/hash
```

过期 expected version、日期回溯、区间重叠或 package/preflight 失败返回稳定 reason code。API 不新增 approve/reject/revoke/authorize endpoint。

### 12.2 DB

Phase 0A.2 优先复用现有表和 JSON evidence，不新增 migration：

- `app.advisory_strategy_binding_version` 保存 dated intervals。
- `strategy_pkg.selection_score_artifact` 保存 candidate artifact/empty declaration。
- `selection.daily_selection_evidence` 保存 prospective evidence payload 和 immutable hash。
- `selection.run/package_result/excluded_result` 保存现有 Selection lineage。

`app.advisory_review_run.run_payload_json` 保存 daily run key、execution origin、decision/target dates、binding/manifest/policy/config hashes、source watermarks、attempt/resume 信息和 runner batch id。正式 `RUN` 继续使用现有 Program/date partial unique index；`REPLAY/PREVIEW` 不得规避正式唯一性生成 published list。

binding 并发使用 §7.3 固定的 Program row lock、expected version 和事务内 overlap query，不依赖新 exclusion constraint。daily runner 复用现有 review/list unique index，并按 §6.4 补齐事务或可恢复状态机。Phase 0A.2 的 `G-DEV-02=noop`；Phase 1 source tables仍由 Phase 1 dataset foundation migration 唯一创建。

### 12.3 UI

- 创建/编辑 Program 的包选择仍可列出多个合规包，但每个 Program 只能选择一个包。
- 单 Alpha 与原生多 Alpha parent 在使用感受上一致，不暴露 leg 手工组合控件。
- binding 生效日默认展示后端返回的下一合法决策交易日；用户不能选已获取 run 的日期。
- 页面不显示审批、角色或授权状态。
- Phase 0A.2 readiness/audit 不阻塞当前基线荐股页面；模型能力仍未进入页面。

### 12.4 CLI

未来扩展现有 CLI，而不建立人工操作链：

```text
validate-policy-registry
plan-dated-binding
run-daily-programs
verify-prospective-evidence
inspect-historical-evidence
replay-program-range
audit-targets
build-handoff-bundle
verify-handoff-readiness
```

任何生产 DML 使用单独强类型 command/request，默认 `--dry-run`，显式 apply 后 readback 验证；audit/handoff 命令保持 read-only。

## 13. Reason Code 基线

新增或统一：

```text
ADVISORY_PHASE0A_POLICY_REGISTRY_NOT_FROZEN
ADVISORY_PHASE0A_POLICY_REGISTRY_HASH_MISMATCH
ADVISORY_BINDING_EFFECTIVE_DATE_REQUIRED
ADVISORY_BINDING_EFFECTIVE_DATE_IN_PAST
ADVISORY_BINDING_INTERVAL_OVERLAP
ADVISORY_BINDING_EXPECTED_VERSION_CONFLICT
ADVISORY_PHASE0A_LEGACY_NULL_BINDING_RESEARCH_ONLY
ADVISORY_PHASE0A_PROSPECTIVE_EVIDENCE_INCOMPLETE
ADVISORY_PHASE0A_SOURCE_LEDGER_PENDING
ADVISORY_PHASE0A_VALID_NO_CANDIDATE
ADVISORY_PHASE0A_VALID_NO_CANDIDATE_EVIDENCE_INCOMPLETE
ADVISORY_PHASE0A_REAL_EMPTY_SAMPLE_NOT_OBSERVED
ADVISORY_PHASE0A_EMBARGO_MATURING
ADVISORY_DAILY_RUN_INPUT_NOT_READY
ADVISORY_DAILY_RUN_ALREADY_SUCCEEDED
ADVISORY_DAILY_RUN_PAYLOAD_CONFLICT
ADVISORY_DAILY_RUN_RESUME_REQUIRED
ADVISORY_HISTORICAL_REPLAY_RESEARCH_ONLY
ADVISORY_CURRENT_MANIFEST_EVIDENCE_NOT_OBSERVED
```

reason code 必须区分“可继续积累的 PARTIAL”和“身份冲突的 BLOCKED”。不得把 source-ledger pending、label pending 或 embargo maturing 误报为包资产损坏。

## 14. 自动门禁与正向可达性

### 14.1 8 类门禁

Phase 0A.2 只使用父蓝图的 8 类自动技术门禁：

```text
G-DEV-01 code_and_test
G-DEV-02 schema_migration
G-DEV-03 release_health
G-RUN-01 strategy_package_preflight
G-RUN-02 market_input_readiness
G-RUN-03 idempotency_concurrency
G-RUN-04 transaction_data_integrity
G-RUN-05 artifact_publish_cleanup
```

本文不增加第 9 类门禁。`G-DEV-02` 在 Phase 0A.2 无 migration 时为可验证 `noop`；未来 Phase 1 DDL 仍在开发/发布阶段应用。

### 14.2 Gate satisfiability matrix

| 检查点 | 唯一 producer | PASS 谓词 | 正向路径 | 失败处置 |
|---|---|---|---|---|
| Package | StrategyPackage registry/preflight | enabled；模式合法；current manifest/asset closure 完整 | 现有 single current manifest、现有 native multi 各一条 PASS | 对应 Program fail loud；single 仅在真实 preflight 失败时重新发布 |
| Policy | immutable policy registry loader | id/version/hash/effective range 与全部必填 policy 完整 | 首次 signal 前 registry hash 固定 | 新 policy version；不人工 bypass |
| Binding | AdvisoryProgram service/repository | 唯一 `[from,to)`；from 为未来未处理交易日；expected version 匹配 | S_v2/M_v2 各得到唯一 interval | 事务回滚并返回 exact reason |
| Clock/input | calendar/ingestion/runtime producers | T/E、cutoff、timezone、available-at、runtime/HMM/PIT 一致 | 正常交易日 evidence 完整 | scope PARTIAL/BLOCKED，不伪造值 |
| Candidate authority | Selection artifact + DSE producer | manifest/run/artifact/evidence/stage hash 闭合 | 非空或合法空候选均有权威 header | evidence capture 失败显式记录 |
| Daily orchestration | Advisory daily runner + review/list unique index | 全部 ENABLED Program 快照、每 Program/date 唯一 RUN、payload 无冲突、事务或恢复闭合 | 同日两个 Program 独立成功；重复触发返回同结果 | WAITING_INPUT 重试；单 Program FAILED 不阻断其他 Program |
| Handoff | Phase 0A/0A.1 | 每 scope 唯一 READY/PARTIAL/BLOCKED；hash 稳定 | 正确前瞻输入至少 PARTIAL/HANDOFF | BLOCKED 新输入新 audit，不原地放行 |
| Source maturity | Phase 1 source ledger | availability event/revision 与 signal source refs 一致 | observer 启用后的交易日逐步转 formal | 保持 PARTIAL，不回填猜测 |
| Embargo | Phase 0A policy/calendar | effective cutoff 后 20 个完整交易日 | 到期后重新审计形成 READY | `EMBARGO_MATURING`，不提前升级 |
| Replay boundary | run type + evidence classifier | REPLAY/PREVIEW 与 RUN/PUBLISHED/source event 严格分离 | 历史诊断可运行且始终 research-only | payload 冲突或越权发布 fail-closed |

### 14.3 双轨状态可达性

```text
PACKAGE_READY
  -> CURRENT_MANIFEST_SMOKE_PASSED
  -> POLICY_FROZEN
  -> DATED_BINDING_ACTIVE
  -> DAILY_RUN_ACQUIRED
  -> PROSPECTIVE_EVIDENCE_CAPTURED
  -> AUDITED_PARTIAL
  -> HANDOFF_EMITTED
  -> SOURCE_LEDGER_ACCUMULATING
  -> EMBARGO_MATURING
  -> AUDITED_READY
```

单 Alpha和原生多 Alpha必须各有完整正向 fixture。真实 L4 至少先证明两条轨道都可由同一 daily batch 独立达到 `HANDOFF_EMITTED`；`READY` 只能在真实 source/embargo 成熟后验证。空候选和 binding switch 的真实样本按自然发生独立补证，不得成为所有非空正常日的永久阻塞门禁。

## 15. Implementation Plan / 实施方案

### 15.1 Phase 0A.2A：Policy 与 schema contract

- 新增官方 immutable policy registry v1 和 loader/hash validator。
- 补齐 typed models、reason code 和正式 audit 禁止 scratch policy 规则。
- 用 fixture 验证完整 registry PASS、缺字段/hash/effective-range fail-closed。

### 15.2 Phase 0A.2B：Dated binding lifecycle

- 统一 create/update/clone/apply/_ensure active binding 的 effective-date 算法。
- 增加 expected version、无重叠、同事务 retire/insert 和日期语义测试。
- 前端使用后端 trading defaults，不再发送 null 或已处理日期。
- 保持多个 Program 独立，不恢复多包融合。

### 15.3 Phase 0A.2C：Prospective evidence producer

- 扩展 SelectionScoreArtifact/DailySelectionEvidence payload，保存 §9 全部 mandatory evidence。
- 复用当前 candidate 计算结果，旁路生成 canonical hashes；不二次运行策略。
- 实现显式 `VALID_NO_CANDIDATE` authority。
- 验证 capture 开/关和 failure 下 Selection、模拟盘、Paper 候选 parity。

### 15.4 Phase 0A.2D：每日多 Program runner

- 实现权威交易日/数据水位触发、ENABLED Program 稳定快照、逐 Program binding/config 解析和独立执行。
- 正式 `RUN` 使用 `(program_id,target_trade_date,RUN)` 唯一业务键，binding/manifest/policy/config 作为冲突谓词。
- 把 review/list/item/episode/decision 写入收敛到单 Program 原子事务或同 key 可恢复状态机；补齐 crash/resume、并发与重复触发测试。
- 生成 batch/per-Program receipt；区分 scheduled/manual/replay origin，禁止 candidates bypass 和 replay 发布。

### 15.5 Phase 0A.2E：本地与隔离集成验证

- 单 Alpha与原生多 Alpha fixture 覆盖 package -> binding -> selection -> audit -> handoff。
- 覆盖多个 Program、binding rollover、HMM enabled/disabled、risk/universe、非空/空候选。
- 覆盖同日重复调度、人工与调度竞态、一个 Program失败、WAITING_INPUT 重试、commit 崩溃恢复和 replay 隔离。
- 验证合法输入达到 `PARTIAL/HANDOFF_EMITTED`，缺 source ledger 不被误判为身份 BLOCKED。
- 运行 Feature Workflow、focused tests、changed-file lint/compile、diff check；广泛业务回归交给 CI/Validation Center。

### 15.6 Phase 0A.2F：生产双轨 onboarding

该阶段只在用户后续明确下达生产 DML 执行指令后启动；这是开发工具的生产安全边界，业务程序本身不创建审批或授权记录：

1. 对现有单 Alpha current manifest 和现有原生多 Alpha parent 执行无业务写入 preflight/隔离 smoke；single 只有在失败时才按标准流程发布新 identity。
2. 首次 signal 前冻结 policy/runtime/style 配置、runner config 和 request hashes。
3. 对现有单 Alpha Program S 创建 dated successor binding S_v2，对现有多 Alpha Program M 创建 dated successor binding M_v2。
4. 按 §7.2 计算正式 `T0`；不能选取历史日期或已存在 run 的日期。
5. 启用 daily runner，从 `T0` 的正常收盘数据开始独立执行所有 ENABLED Program，不人为生成候选或空候选。
6. 使用只读 audit 对 S/M 批量复验，期望至少 `PARTIAL -> HANDOFF_EMITTED`。

每笔 DML 都使用 dry-run plan、expected version、事务、row count/hash receipt 和 readback。执行失败不自动回滚另一个已成功 Program，也不删除旧 binding。

### 15.7 Phase 0A.2G：Phase 1 source/embargo 成熟

- 按 Phase 1 设计部署 source availability observer 和 append-only ledger。
- 继续积累新交易日 signal/source evidence。
- 20 个完整交易日 embargo 到期后创建新 audit version。
- 对 mandatory closure 完整的 scope 验证 `FORMAL_OOS + READY`；未完整 scope 继续 PARTIAL/blocked-by-reason。
- 自然出现空候选和后续真实 binding switch 时追加 L4 evidence，不追溯造样本。

## 16. Verification Plan / 验证方案

### 16.1 L0/L1

- policy registry canonical hash、effective range 和不可变版本测试。
- 交易日 effective-date、right-open interval、overlap 和 expected version 纯函数测试。
- decision clock、timezone、runtime/config chain、HMM、universe、source refs serializer/hash 测试。
- `VALID_NO_CANDIDATE` 完整/不完整契约测试。
- Phase 0A readiness 分类：identity-complete/source-pending 必须为 PARTIAL，不得误判永久 BLOCKED。

### 16.2 L2/L3

- PostgreSQL fixture 验证 binding retire/insert 原子性和重复 request 幂等。
- 单 Alpha package/program 正向 E2E。
- 原生多 Alpha parent/per-leg provenance 正向 E2E。
- 两个 Program 同日运行且 lineage/状态互不污染。
- daily runner 对 `(program_id,target_trade_date,RUN)` exactly-once、payload conflict、人工/调度竞态、WAITING_INPUT、crash/resume 和 batch receipt 的正向/反向 E2E。
- capture enabled/disabled/failure 的 Selection、模拟盘和 Paper candidate/result parity。
- 非空候选和合法空候选的 artifact/DSE/audit/handoff golden。
- legacy null binding、manifest mismatch、dynamic HMM latest、future leakage、guessed available-at 和 replay 尝试发布必须拒绝。

### 16.3 L4 真实只读验证

生产 DML onboarding 完成后，以 read-only session 执行：

1. 验证现有 single current manifest 与 native multi parent 的 smoke receipt、正式 `T0` 和 dated bindings。
2. 同一 daily batch 对两个 Program 执行；一个失败时另一个仍能完成并产生独立 receipt。
3. 相同 Program/date 重触发返回同一正式 run/list identity；不同 payload 返回冲突，不生成第二个 published list。
4. 相同 request/source watermark 重跑只读 audit，业务 hash 必须相同。
5. 两条 scope 至少达到 `PARTIAL/HANDOFF_EMITTED`，否则回到 producer contract 修复，不能解释为“门禁正常”。
6. Phase 1 source/embargo 成熟后复验 `READY`。
7. 空候选和 binding switch 只在真实自然样本存在时核验。

L4 始终只读；不得从 audit CLI 触发 binding/package/source 写入或 HMM generation-on-miss。

### 16.4 DESIGN-COMPLIANCE-001

- [x] 覆盖单 Alpha、原生多 Alpha和多个 Program 的完整设计，不是单包限制。
- [x] 归档包、null binding、manifest mismatch 和历史 available-at 均禁止猜测修复。
- [x] policy、binding、evidence producer、source ledger 与 embargo 的先后关系闭合。
- [x] 每日多 Program runner、正式唯一业务键、事务/恢复、失败隔离和 replay 边界闭合。
- [x] 正确数据具有 `PARTIAL/HANDOFF -> READY` 正向可达路径。
- [x] 空候选 fixture 与真实自然样本边界明确。
- [x] Selection、StrategyPackage、模拟盘和 Paper 隔离及 parity oracle 明确。
- [x] 零审批、零角色、零运行时 DDL和 8 类自动技术门禁保持不变。
- [x] 生产 DML、只读 audit、代码合入和运行激活被分别报告。

## 17. Rollout / Rollback / 发布与回滚

### 17.1 Rollout

1. 合入 policy/binding/evidence producer 代码和测试；不执行生产 DML。
2. 部署代码并验证 release health；Phase 0A.2 固定无 migration，`production_ddl_gate=noop`。
3. 对现有 single current manifest 和 native multi parent 执行隔离 smoke；仅在 single 失败时另行发布新 identity。
4. 使用程序化 request 创建两个 Program 的 future-effective successor binding，冻结 runner/policy/config，并确定正式 `T0`。
5. 激活 daily runner；观察正常业务日证据并执行只读 L4 audit/handoff。
6. Phase 1 source observer 单独发布并积累 evidence。
7. embargo 到期后复验 READY；不自动开始 Phase 0B 或模型训练。

### 17.2 Rollback

- policy：不覆盖旧文件；停止引用新 version，使用新的 future-effective version 修正。
- binding：不删除或修改历史行；创建新的 future-effective successor，旧 interval 保持可审计。
- evidence：不 UPDATE/DELETE immutable artifact/DSE；错误证据追加 invalidation/gap，并由新 run/version替代。
- package：默认复用的当前单 Alpha 包如需停止可按现有生命周期处置；若后来发布替代包，也不复活旧归档包或继承其 OOS identity。
- Program：可独立 disable，不影响其他 Program、Selection、模拟盘或 Paper。
- runner：可独立停用新批次；已成功 Program/date 保持只读，未完成 key 按相同 deterministic identity 恢复。
- source observer：可独立停用；已写 append-only event 保留。

## 18. Risks / Failure Modes / 风险与失败模式

| 风险 | 后果 | 强制处置 |
|---|---|---|
| 当前单 Alpha manifest 未真正执行就继承旧 DSE | L4 正向结果混淆两个 signal identity | current-manifest cold-start smoke；旧 manifest DSE 永久分离 |
| 不必要地重发单 Alpha 包 | 增加身份和证据碎片 | current manifest preflight 通过即复用；仅失败时标准 publish 新 identity |
| 复活归档包 | manifest 历史继续漂移 | 旧包保持 retired；替代包使用新 identity |
| binding 生效日仍为空或回溯 | as-of 永久歧义 | 后端强制未来交易日、expected version 和 `[from,to)` |
| policy 仍使用 scratch 空 JSON | 所有 target 永久 BLOCKED | 官方 immutable registry + formal loader hard gate |
| evidence producer要求 Phase 1 event 才能运行 | 再次形成循环依赖 | identity-complete/source-pending 分类为 PARTIAL，Phase 1 只消费该 scope 建 source 能力 |
| generated runtime 被当 formal | 语义 vintage 伪造 | 必须使用 release/activation binding；generated 只 retrospective/partial |
| 多 Alpha只记录 parent hash | leg/weight 漂移不可见 | per-leg closure + parent parity hash |
| capture 失败改变候选 | 破坏现有 Selection/Paper | 旁路 evidence、parity oracle、失败显式但不重算候选 |
| 为通过测试制造空候选 | 生产验证失真 | fixture 与 L4 分离；真实样本等待自然发生 |
| source ledger启用即回填过去 | 虚假 formal available-at | first observed only；历史未知保持 research/unavailable |
| embargo 被当作审批等待 | 阶段含义混乱 | 它是确定性统计时间规则，到期自动重审计，无人工放行 |
| 一个 Program失败阻塞全部 | 多策略包支持失效 | scope/program 独立状态、事务和 reason code |
| `review_schedule` metadata 被误认为调度器 | 实际没有每日荐股 | 独立 daily runner、运行健康和逐批 receipt |
| review run 与 list 分事务留下半成品 | 唯一键阻止后续恢复 | 单 Program 原子提交或同 key 可恢复状态机 |
| 历史 replay 冒充正式前瞻 | 泄漏且虚构 OOS | RUN/REPLAY/list/source event 强隔离；正式 T0 禁止回溯 |

## 19. Production Gates / 生产门禁

本文是文档-only：

```text
production_ddl_gate = noop
production_frontend_dependency_gate = noop
production_backend_dependency_gate = noop
production_dml_gate = noop
production_runtime_gate = noop
```

未来实现/运行必须分别报告：

- 代码是否合入及本地 main/origin main 是否同步。
- Phase 0A.2 `production_ddl_gate=noop` 是否得到验证；Phase 1 migration 必须另行报告，不能混入本阶段运行命令。
- current-manifest smoke、两个 dated successor binding 和正式 `T0` 是否已由程序化流程产生并 readback。
- daily runner 是否实际激活、最新 batch/Program receipt、是否存在未恢复 key；`review_schedule` metadata 不作为激活证据。
- source observer 是否启用、当前积累到哪个 trade date。
- L4 是 `PARTIAL/HANDOFF_EMITTED` 还是 `READY`，以及具体未成熟 reason code。
- 服务/调度是否实际激活；代码合入不等于运行激活。

## 20. Design Acceptance Matrix / 设计验收矩阵

本矩阵只表示 Phase 0A.2 设计闭合，不代表任何实现或生产数据已经产生。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-031 | §1.2-1.4、§6.1、§11、§15.6 | current-manifest smoke、旧 manifest evidence 隔离、条件式新 identity 和归档包不复活已定义 | design_ready | none |
| F-032 | §5.2、§6.2-6.3、§15.6 | 现有 native parent 复用、dated successor 和多 Program 独立路径已定义 | design_ready | none |
| F-033 | §7、§12.1-12.2、§16 | `[from,to)`、未来有效日、expected version、legacy null 与原子验证已定义 | design_ready | none |
| F-034 | §8、§14、§15.1 | immutable policy schema、hash/effective range、自动 loader 和零审批字段已定义 | design_ready | none |
| F-035 | §9、§12、§15.3、§16 | clock/config/runtime/HMM/universe/risk/asset/source/stage producer 与验证已定义 | design_ready | none |
| F-036 | §10、§13、§16 | 合法空候选 artifact/DSE/header、自然样本和 fixture/L4 边界已定义 | design_ready | none |
| F-037 | §5.3、§9.6、§11、§15.7 | Phase 1 ledger 复用、exact remediation 和 historical no-guess 边界已定义 | design_ready | none |
| F-038 | §5、§14-17、§19 | 双轨 PARTIAL/HANDOFF 到 READY、8 类门禁正向可达、隔离、发布与回滚已定义 | design_ready | none |
| F-039 | §1.3、§6.4、§12-16 | daily runner、正式唯一键、事务/恢复、失败隔离、批次回执和运行证据已定义 | design_ready | none |
| F-040 | §1.3-1.4、§7.2、§11.3、§16-17 | 正式 T0、共同历史窗口、current-semantics replay 和 formal OOS 隔离已定义 | design_ready | none |

### 20.1 实现验收记录

本表只记录已完成的实现切片；设计矩阵继续覆盖完整 F2 范围，未在本表列出的条目不得据此宣称已实现。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-034 | `backend/services/advisory_phase0a/policy.py` registry loader/hash validator；`models.py` typed request/registry contract；`audit_service.py` frozen id/hash/effective-range enforcement；`scripts/advisory_phase0a_audit.py validate-policy-registry` | `test_policy_registry.py` valid/tampered/missing/effective-range/prohibited-field/scratch-root cases；`test_audit_service.py` scratch/hash mismatch cases；focused suite 33 passed；Ruff passed；direct CLI registry validation passed | completed | none |

## 21. Exit Criteria / 设计退出条件

本文可标记 `design_ready` 的条件：

- F2 Feature Workflow validator 通过。
- Design Acceptance Matrix 无 gap。
- 父蓝图增加 Phase 0A.2 和 F-031 至 F-040。
- Phase 0A 文档记录真实 L4 结论和 Phase 0A.2 producer handoff。
- Phase 1 文档允许 `PARTIAL/HANDOFF_EMITTED` scope 建设 source capability，并禁止其进入 formal Phase 0B。
- `git diff --check` 通过。

用户确认本文后，才进入 Phase 0A.2 实现。真实业务验收仍需后续代码证据、现有 single current-manifest smoke、双轨 dated successor binding、daily runner 的正常交易日 prospective evidence、exactly-once/失败隔离 receipt、L4 `HANDOFF_EMITTED` receipt，以及 source/embargo 成熟后的 `READY` receipt。
