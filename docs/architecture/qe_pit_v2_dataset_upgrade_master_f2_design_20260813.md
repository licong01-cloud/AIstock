# QE / PIT v2 数据集升级与月度更新主计划 F2 详细设计

## 0. 文档身份

- 日期：2026-08-13
- 等级：F2（跨数据管线、QE/HMM、Selection、Paper/Simulation、StrategyPackage/Advisory 与生产激活）
- 状态：W0～W2源码已合入；W3短周期分片方案经Review-4A/4B/4C审核通过；W3-A已获授权进入独立实施，其源码/合入状态以Acceptance Matrix和window scope receipt为准；W3-B/C、真实数据构建和生产激活均未授权
- 上位业务设计：`docs/architecture/unified_canonical_equity_pit_f2_design_20260812.md`
- 月更底座设计：`docs/architecture/qe_monthly_dataset_release_productization_f2_design_20260811.md`
- 运维入口：`docs/operations/qe_backtest_dataset_monthly_update_runbook.md`
- 目标 authority：`aistock_equity_pit_canonical`
- 目标规则：`shsz_a_252td_st_delist_asof_v2`
- 目标 rolling key：`aistock_equity_pit_canonical_v2`

本文是后续实施、候选构建、跨模块验收和生产切换的主控计划，不替代上述业务规则和月更底层技术设计，
也不形成第二套数据管线或第二个股票池标准。若本文与上位业务设计冲突，必须停止实施并先修订设计。

## 1. Background / 背景

PIT v2业务规则、月度release控制面和资源有界materializer已经分别具备设计与部分源码基础，但生产消费者仍
存在v1默认值，真实v2 candidate、跨消费者验收和原子activation尚未闭合。若继续让各模块独立替换key，
QE frozen snapshot、在线rolling view、StrategyPackage和模拟盘将可能形成多套事实来源。本主计划用于把已有
能力收敛为一个可分批实施、可独立审核、最终单点激活的升级项目。

## 2. Scope / 范围

### 2.1 目标与不可突破边界

#### 2.1.1 目标

1. 将 PIT v2 作为未来 QE 正式实验、模型训练、Selection、荐股、Paper v2 和 simulation runtime 的唯一
   `ACTIVE_CANONICAL` 股票池规则。
2. QE/训练使用不可变 frozen snapshot，Selection/Paper/Simulation 使用 rolling view；二者必须具有相同
   `authority_id/rule_version/rule_parameters_digest`，不是两套可选股票池。
3. 在不覆盖现有生产数据、不重新导出已证明可复用组件的前提下，生成完整、独立、可签收的 v2 candidate。
4. 把后续普通月更收敛为一次 `monthly --candidate-only` 提交，由 durable planner 决定
   `REUSE/INCREMENTAL/SELECTIVE_REBUILD/FULL_REBUILD`。
5. 将数据语义正确性、内存/磁盘安全、性能、历史复现、消费者一致性和生产激活分别建模和验收。
6. 为每个实施阶段建立独立窗口、worktree、allowed write scope、交付物、入口门禁和退出门禁，避免多窗口
   共同修改公共契约。

#### 2.1.2 明确禁止

- 不覆盖、原地追加、移动或删除 2026-06-30、2026-07-31 生产/候选数据集。
- 不把 `all.txt`、manifest、digest 或少量样本冒充完整历史数据集。
- 不执行全局字符串替换把所有 `shsz_st_pit_active_v1` 改为 v2；历史 reproduction identity 必须保留。
- 不以数据缺失、停牌、涨跌停、无分钟成交或 provider 空结果缩短证券历史生命周期。
- 不把分批查询后仍保留全部 `frames`、全市场预分配矩阵或无界日志重新带回构建路径。
- 不通过扩大并发、降低主机内存保留、启用 WSL swap 或减少股票/日期/字段/指数来换取成功。
- 源码合入不自动授权真实数据导出、DEV/生产 DDL/DML、node1 分发、生产 activation、后端/Worker 重启或 cleanup。
- 真实候选构建开始后，构建窗口不得同时修改其所执行的 profile、planner、materializer、validator 或 Skill。
- 任一窗口不得进入、修改、清理或控制其他窗口的 worktree、任务或进程。

## 3. 当前基线与必须关闭的缺口

### 3.1 已有可复用能力

- `backend/services/canonical_equity_pit.py` 已定义 v2 authority、rule parameters digest、普通消费者约束和
  legacy reproduction 边界。
- `backend/services/dataset_release/` 已提供 durable control catalog、candidate-only Worker、lease/fence、CAS、
  COW、增量/选择性重建、资源门禁、TDX-first 分钟补齐、PIT snapshot、12指数/HMM、候选校验和签收。
- `configs/datasets/qe_backtest_monthly_v2.yaml` 已冻结 v2 月更语义与资源上限。
- `backend/services/dataset_release/retention.py` 已规定已发布或被实验/训练/生产/审计引用的数据集必须完整保留，
  自动删除永远不允许。
- W1公共authority/registry源码已由`4e1f667e`合入；W2 candidate-only release、严格PIT binding和首次迁移计划已由
  `589678f3`合入。W3必须消费其公开API，不回改W1/W2核心来规避消费者迁移。

### 3.2 当前未关闭缺口

1. Selection runtime profile、QE dataset contract、StrategyPackage/Advisory 等消费者仍存在v1默认值、旧dataset常量
   或SQL硬编码。
2. W3原设计一次性保留十个QE/HMM业务文件，不适合与持续实验和紧急BUG修复并存；缺少可抢占、可重放的短期文件
   租约和小PR顺序。
3. QE/HMM frozen dataset identity与在线rolling authority尚未形成机器可校验的candidate validation bundle与
   activation envelope。
4. 旧 StrategyPackage/模型是否只需重新认证还是必须重训，尚未按“截面依赖/时序独立”完成分类。
5. v2 真实小样本、全量candidate、跨组件digest和消费者shadow evidence尚未形成最终release receipt。
6. 2026-06-30、2026-07-31及失败候选的引用/重复副本/冷存储状态尚未形成可执行的retention inventory；
   磁盘紧张不能据此直接删除历史release。

## 4. Contracts / 核心决策与契约

### D-001 单一逻辑权威

任一时点只允许一个 `ACTIVE_CANONICAL`。v1在迁移期为
`DEPLOYED_LEGACY_PENDING_MIGRATION`；切换前若仍有已固定旧generation的任务，进入受租约约束的
`SESSION_PINNED_DRAINING`且禁止新admission；drain完成后为`ARCHIVED_NONCANONICAL`，仅可由显式、不可变、
只读`reproduction`使用。`SESSION_PINNED_DRAINING`不是第二个active authority。

### D-002 版本登记、单例指针与追加审计，而不是模块常量投票

registry使用三部分表达不同职责，避免一个可变行既冒充当前指针又冒充完整历史：

1. `market.stock_universe_pit_authority_versions`：按`(authority_id, rule_version)`保存不可覆盖的版本身份、
   rolling key、parameter digest、首次candidate/release证据和`DEPLOYED_LEGACY_PENDING_MIGRATION / ACTIVE_CANONICAL /
   SESSION_PINNED_DRAINING / ARCHIVED_NONCANONICAL / EMERGENCY_LEGACY_ROLLBACK`状态。
2. `market.stock_universe_pit_authority_pointer`：只允许`authority_id=aistock_equity_pit_canonical`一行，保存当前
   rule/key、`activation_generation`、activation envelope digest和expected source commit。
3. `market.stock_universe_pit_authority_events`：append-only记录每次prepare/activate/rollback的before/after generation、
   operator intent、candidate bundle/envelope/receipt digest和时间；不得用更新当前行抹掉历史。

W1提交精确`preflight.sql`、forward migration和`rollback.sql`。约束至少包括固定authority id、版本表主键、
pointer singleton、`activation_generation >= 0`、版本key唯一和最多一个`ACTIVE_CANONICAL`。migration只创建结构、
登记v1迁移态并建立指针，不激活v2。forward/rollback先在现有DEV数据库验证，生产执行仍需独立授权。

在线消费者只通过共享`CanonicalPitAuthorityResolver`读取pointer并校验被指向version及对应
`stock_universe_pit_state/spans`；不得各自保存生产默认key。激活在一个DB事务中先归档旧版本、激活新版本、
以`expected_previous_generation + expected_previous_key + expected_bundle_digest` CAS更新pointer并追加event；
影响行数和event数必须各为1，事务外观察者只能看到完整before或after。

QE/HMM计算节点不依赖在线DB：它们读取不可变dataset manifest中的同一authority/rule/parameter digest、
frozen snapshot digest、cutoff和release identity。控制面使用两个不可混淆的不可变对象：

- `canonical_pit_candidate_validation_bundle_v1`：由W7生成、W8审核，绑定candidate/release、rolling-at-cutoff、
  frozen digest、source/profile/toolchain和数据/资源/consumer验证；不包含未来分发结果。
- `canonical_pit_activation_envelope_v1`：W9引用candidate bundle digest和W8 receipt，在inactive distribution完成后
  加入各节点readback、pointer preconditions和drain readiness；最终DB CAS只绑定sealed envelope digest。

rolling/frozen parity不能由同一producer自证。W8使用不导入builder/materializer canonicalization实现的独立只读
validator，对DB中按固定SQL排序并裁剪到cutoff的spans重新编码/hash，再与sealed frozen snapshot digest比较；
同时用公告/交易日历/退市事件分层样本验证业务正确性。digest parity证明同一集合，事件oracle证明集合规则正确。

candidate control catalog使用`DRAFT → CANDIDATE_VALIDATED → INDEPENDENTLY_ATTESTED`；activation catalog使用
`ENVELOPE_DRAFT → DISTRIBUTED_INACTIVE → DRAIN_READY → READY_TO_ACTIVATE → ACTIVATED`。失败或回滚形成新的
append-only receipt，不能回写旧receipt。candidate bundle至少包含：

```text
schema_version, candidate_validation_id, created_at
authority_id, target_rule_version, target_rolling_key, rule_parameters_digest
rolling_observation: cutoff, ordered_span_encoding_version, row_count, digest, state/source digest
frozen_release: candidate_identity, release_id, allowlisted_root_id, artifact_root_digest,
                pit_snapshot_digest, calendar_digest, manifest_digest, signoff_receipt_digest
source_runtime: source_commit, profile_digest, toolchain_digest, consumer_inventory_digest
validation: independent_pit_receipt, component_receipt, resource_receipt, consumer_shadow_receipt
```

activation envelope至少包含：

```text
schema_version, activation_id, created_at
candidate_validation_bundle_digest, independent_w8_receipt_digest
expected_pointer_generation, expected_pointer_key, expected_pointer_envelope_digest
distribution: [{node_id, inactive_manifest_ref, manifest_digest, file_graph_digest, readback_receipt_digest}]
drain: admission_closed_at, active_pinned_leases, side_effecting_session_count, unbounded_session_count, readiness_receipt
```

两对象canonical JSON分别存入dataset release control CAS，内容SHA-256即各自digest；不存在私钥签名就不得称为
cryptographic signature。candidate字段变化产生新candidate bundle并使W8 receipt失效；distribution/drain变化只
产生新activation envelope，不使已封存candidate/W8证据失效。只有全部节点`DISTRIBUTED_INACTIVE`、无未处理
side-effecting/unbounded v1 session且readiness receipt PASS时，envelope才能seal为`READY_TO_ACTIVATE`。

### D-003 两阶段源码交付

1. **Enablement**：所有模块支持registry/v2/shadow，但生产仍解析到v1 pending-migration；不存在提前切换。
2. **Activation**：完整候选验收后，仅执行已合入的registry CAS operator动作；candidate和bundle使用内容digest，
   不虚构私钥签名，且W8后不再提交配置/源码。源码支持与生产数据激活不得混为一次不可回滚动作。

### D-004 不默认全量重新导出

planner必须按数据依赖决定动作：

- 原始日线、分钟、复权、指数源优先复用已验证内容和CAS，仅补missing keys或新截止日期。
- PIT snapshot、`all.txt`、股票池摘要必须按v2重新生成。
- 新增历史退市股对应的日线/分钟/复权/限制标识只重建受影响code/date分区。
- 与股票池无关的row-local因子复用已验证值；截面排名、中性化、行业相对、PIT mask等因子按依赖图重算
  受影响日期，必要时可以是全历史日期，但不能因此重查/常驻全部原始frame。
- planner无法证明安全复用时fail closed并选择更大重建范围；不得静默复用。

### D-005 历史release保留一份完整权威副本

2026-06-30、2026-07-31或其他被正式实验、训练、StrategyPackage、生产、审计引用的release，至少保留一份
完整不可变副本。可在v2稳定后把重复热副本迁移/去重到冷存储，但必须先完成引用审计、逐文件digest、
catalog路径更新和历史复现smoke。任何删除仍需精确路径的独立授权。

本升级项目只生成retention inventory，明确禁止移动、去重或删除2026-06-30和2026-07-31release。未来冷迁移必须
另立运维任务，执行`目标复制→逐文件digest/readback→catalog expected-old-path CAS→历史QE reproduction smoke→
保留rollback副本→精确源路径删除授权`；任何一步失败保留原路径。

`pit_v2_retention_inventory_v1`逐release冻结：release/candidate/dataset id、绝对路径、root id、volume serial、
file-graph/Merkle、marker和receipt digest、size、引用状态、reference evidence refs、retention class和扫描时间。
引用图必须读取并交叉验证：dataset release catalog、QE experiment/archive/evaluation、训练/模型登记、
StrategyPackage manifest/package asset、production/runtime profile与active session、审计hold/validation history、计算节点
manifest。`reference_absence_proven=true`仅在全部权威源成功扫描、无unknown/error、路径/identity一致且结果为零时成立；
任一源不可用或未分类即`reference_state_unsettled`并完整保留。

### 4.1 允许的现有API与禁止绕行

#### 4.1.1 允许复用

- `canonical_equity_pit.PitConsumerBinding`
- `canonical_equity_pit.require_canonical_consumer_binding`
- `canonical_equity_pit.require_canonical_rolling_universe_key`
- `dataset_release.pit.freeze_pit_snapshot`
- `dataset_release.pit.require_canonical_source_snapshot`
- `dataset_release.pit.require_canonical_frozen_snapshot`
- `dataset_release.pit.pit_spans_sha256`
- `dataset_release.retention.classify_dataset_retention`
- `StockUniversePitService.get_status/get_eligible_codes`
- 月更CLI、control catalog、Worker、receipt和candidate validator的现有公开入口。

#### 4.1.2 计划新增的最小API

- `CanonicalPitAuthorityResolver.resolve_live_binding()`：读取singleton registry和对应ready state，返回完整binding。
- `CanonicalPitAuthorityResolver.require_activation_target(...)`：对candidate receipt、rule/digest和CAS预期状态做只读preflight。
- `activate_canonical_pit_authority(...)`：只供精确migration/operator入口调用；单事务CAS，不暴露通用API。
- `DatasetPitBinding.from_release_manifest(...)`：从不可变release生成QE/训练binding，不查询在线DB。
- `require_strategy_package_pit_compatibility(...)`：区分普通运行、v2重新认证和历史reproduction。

名称可在实现审核中微调，但不得把这些职责拆回模块内硬编码或新增第二套PIT服务。

#### 4.1.3 禁止绕行

- 在线消费者直接查询“最新ready key”或按更新时间猜测active authority。
- QE计算节点回退查询生产DB补齐frozen identity。
- provider空结果视为无交易并继续签收。
- 旧包manifest原地修改为v2。
- activation失败后自动回退到v1并伪装为长期权威。

## 5. 专用窗口与所有权

每个窗口使用从其开始时最新`origin/main`创建的独立worktree。公共文件遵循单写者原则；并行只允许发生在
write scope完全不重叠且共同依赖已合入main时。

| 窗口 | 职责 | 允许写入范围（实施时登记精确文件） | 主要交付物 | 禁止事项 |
|---|---|---|---|---|
| W0 Program/Design | 主设计、Acceptance Index、跨窗口依赖、最终汇总 | 本设计及其feature记录 | 批次计划、scope矩阵、审核结论 | 不实现业务代码、不运行数据构建 |
| W1 PIT Core/Registry | authority resolver、registry migration、v1迁移态、v2 builder/state契约 | `canonical_equity_pit.py`、`stock_universe_pit_service.py`、PIT builder、精确migration及直接测试 | 单一registry和fail-closed公共API | 不修改QE/Selection/StrategyPackage消费者 |
| W2 Dataset Release | v2 profile、PIT snapshot、planner/materializer/validator、资源和月更Skill/Runbook | `dataset_release/**`、月更scripts/config、Skill/Runbook及直接测试 | 有界增量candidate能力 | 不改变消费者运行默认值、不运行真实全量 |
| W3-A Shared Dataset Consumer Contract | 中立的正式frozen manifest consumer adapter和legacy排除契约 | 新建`backend/services/canonical_pit_dataset_consumer.py`及直接测试 | 单一v2正式解析入口，复用W2 `DatasetPitBinding` | 不解析v1 reproduction、不回改W1/W2、不写QE/HMM业务文件 |
| W3-B QE Integration | QE正式实验、因子缓存和长期趋势读取接入frozen binding | 仅QE精确入口文件和直接测试，按短期租约登记 | QE正式任务绑定release identity且不查在线PIT DB | 不修改HMM、Selection、StrategyPackage |
| W3-C HMM Integration | HMM source/evolution与HMM-risk在线/离线边界迁移 | 仅HMM精确入口文件、repair script和直接测试，按短期租约登记 | frozen训练隔离DB；在线stock facts绑定rolling generation | 不修改QE、Selection、StrategyPackage |
| W3-D QE/HMM Validation | 在A/B/C均合入后的最终main上验证统一身份、reproduction和隔离 | 不修改业务源码；仅运行直接测试、inventory和结构化验证 | W3 source-ready receipt，失败返回责任切片 | 不修数据、不启动实验、不把旧实验当v2证据 |
| W4 Selection/Paper/Simulation | rolling resolver、选股/持仓退出、持久化runtime profile和模拟盘兼容 | `selection_center/**`、`paper_trading_v2/**`、`simulation_runtime/**`及其精确JSON migration/tests | 在线消费者统一binding、旧profile显式迁移和shadow能力 | 不改变订单可执行性业务语义 |
| W5 StrategyPackage/Advisory | package manifest/projection双读契约、旧包分类、Advisory查询迁移 | `strategy_package/**`、`advisory_*`精确scope、直接测试 | v2包重新认证/重训决策和v1 reproduction隔离 | 不改写已发布旧manifest或模型资产 |
| W6 Integration | 汇总最终main、消费者库存扫描、跨模块fixture、candidate bundle/activation envelope builder、共享inference入口 | 集成测试、inventory guard、`backend/inference_engine.py`精确scope和必要胶水；其他公共核心文件需W1交接 | final source-ready commit、无fallback共享推理和结构化receipt | 不运行生产或全量candidate |
| W7 Candidate Build | 小样本后执行真实v2 candidate-only任务，修复数据缺口 | 只写新candidate root、control catalog、candidate-local CAS/overlay和receipt | 完整v2 candidate | 不改源码、不写生产DB、不覆盖历史candidate |
| W8 Independent Validation | 只读审计candidate、旧数据对照、消费者shadow、性能/资源证据 | 仅validation receipt/允许的审计输出目录 | 独立PASS/FAIL与缺口清单 | 不修数据、不改candidate、不激活 |
| W9 Activation/Aftercare | enablement部署核验、inactive分发、最终DB CAS、激活后只读核验、精确cleanup计划 | 精确migration/distribution/activation receipt；运行时动作按独立授权 | active v2与回滚证据 | 不在W8后改源码/config，不推导重启、删除或cleanup授权 |

### 5.1 并行与串行规则

- W1和W2先串行合入：W2必须以W1最终公共binding为输入。
- W3内部固定`W3-A → W3-B → W3-C → W3-D`；每个源码切片从当时最新`origin/main`开始并形成独立小PR，
  不保留一个跨越全部QE/HMM文件的长期开发分支。
- W4、W5可与尚未占用同文件的W3切片并行；任意共同文件转交W6单写。W3-B与W3-C虽文件互斥，仍按上述顺序
  执行，以减少活跃业务窗口交接和最终identity漂移。
- W6只在W3-D、W4、W5均通过或明确记录阻断后开始。
- W7绑定W6最终commit、profile digest和toolchain SHA；W7运行期间W1-W6不得变更构建相关源码。
- W8可以只读观察W7，但正式签收必须绑定W7 terminal receipt和不可变candidate identity。
- W9的DEV migration和enablement部署准备可在W6 source-ready后进行；inactive data distribution与最终CAS必须在
  W8 PASS、用户对具体migration/candidate/activation授权后执行。

### 5.2 W3短期文件租约与BUG抢占

文件租约是并发写入协调，不是新的业务审批或人工发布门禁：

1. 每个W3源码切片只登记本PR实际要写的精确文件；租约从首次写入前的preflight开始，到PR合入或切片明确放弃时
   释放。目标是数小时内完成一个原子PR，持续时间只做telemetry，不以超时伪造失败或批准。
2. 切片入口只要求其目标文件完成提交/交接并且没有其他写入者；其他QE/HMM文件、实验和无交集开发无需停止。
   若目标文件在其他worktree仍dirty，状态为`WAITING_OWNER_HANDOFF`，不得stash、reset、复制覆盖或进入对方worktree修复。
3. preflight必须比较最新`origin/main`、open PR/已知活动分支的changed files、窗口登记和owner handoff。Git无法证明的
   未提交状态必须由原窗口结构化声明；不得把“未发现分支diff”解释为“没有dirty改动”。
4. 同一目标文件出现P0/P1或阻断当前实验的BUG时，BUG修复优先。W3在可测试的原子commit边界停止并释放租约，BUG先
   合入main；W3随后从新main重建/同步该切片并重跑全部直接证据，旧HEAD receipt失效。不得让BUG窗口和W3并行写同文件。
5. 运行中的实验继续固定其启动commit、dataset release和PIT identity；W3源码合入不重启、不切换或重放这些实验，
   旧实验结果不得作为v2消费者验收证据。
6. 合入前再次计算PR diff与新main/其他open PR交集；存在语义或文本冲突时停在源码层解决并重验，不通过合入顺序、
   自动冲突选择或运行时默认值掩盖冲突。

### 5.3 首次升级与常态月更的窗口差异

W0～W9是首次v2升级或规则/schema改变时的完整项目编排。v2稳定激活后的普通月份不重复启动所有研发窗口：

1. 月更operator只使用W7提交一次`monthly --candidate-only`并读取status/receipt。
2. W8消费自动生成的结构、采样、资源和consumer smoke receipt；只有typed异常才创建专项验证任务。
3. 新candidate若需要成为生产默认，仍由W9执行独立分发/activation授权；不要求W1～W6重复开发。
4. 仅当出现新provider/schema/业务规则、无法解释的数据差异、资源回归或代码BUG时，才按ownership唤醒对应
   W1～W6窗口；不得让月更operator临时改源码绕过阻断。

## 6. Implementation Plan / 整体阶段、入口与退出门禁

### P0 基线冻结与消费者库存（W0）

任务：

1. 记录最终`origin/main`、profile digest、toolchain SHA、现有production dataset identity和生产runtime identity。
   首次迁移计划固定`requested_cutoff=effective_cutoff=2026-07-31`，交易日历只负责证明该日期是完整交易日并冻结
   `calendar_digest`；不得在跨月执行时漂移。动态解析“上月最后完整交易日”只适用于v2激活后的普通monthly。
2. 对所有v1引用分类为`production_consumer`、`reproduction_only`、`candidate_builder`、`test_fixture`或`unknown`。
3. 建立6月30日、7月31日、失败候选和重复副本的只读retention inventory；未知引用一律保留。
4. 建立数据库migration、backend runtime、Worker、WSL、node1和cleanup的独立gate状态。

退出门禁：consumer inventory无`unknown`；历史数据路径只读；没有活动构建任务引用待操作目标。

### P1 设计确认（W0）

任务：完成本设计的架构、数据正确性、资源/性能、跨模块交付和生产安全审核；运行F2 design validator。

退出门禁：所有设计审核意见关闭；Design Acceptance Index稳定；用户明确确认进入实现。

### P2 Core与月更Enablement（W1→W2）

W1任务：

- 在DEV可验证的migration中创建singleton authority registry和注释/约束/回滚脚本。
- 公共resolver同时支持`DEPLOYED_LEGACY_PENDING_MIGRATION`与目标v2，但普通运行只能解析registry当前状态。
- v2 builder输出252交易日暖机、历史D/P生命周期、ST/退市as-of、exception ledger和参数digest。
- 所有registry/state/spans组合不完整、dirty、冲突或generation漂移均fail closed。

W2任务：

- 让v2 profile和release manifest消费W1 binding；rolling/frozen cutoff digest必须一致。
- 首次迁移使用独立、严格的durable request schema；control service与Worker resolution reader共同复验仓库白名单
  plan id、canonical plan digest、固定`2026-07-31` cutoff和sample/full scope，禁止CLI单方面注入或伪装普通monthly。
- 证明component planner不会把PIT变化错误降级为只更新`all.txt`。
- 证明日线/分钟/因子materializer持续使用流式分块、COW、单股/日期缺口补齐。
- 月更Skill/Runbook只暴露一次candidate提交、status、receipt和re-attest，不暴露activation。

退出门禁：两批PR均通过直接测试、F2设计矩阵、CI；生产runtime仍为v1 pending-migration。

### P3 消费者迁移（W3/W4/W5并行）

#### P3-A QE/HMM/训练（W3-A→W3-B→W3-C→W3-D）

##### W3-A 中立公共契约

- 新建`backend/services/canonical_pit_dataset_consumer.py`，只组合W1的
  `require_canonical_consumer_binding`与W2的`DatasetPitBinding.from_release_manifest()`；不得复制manifest字段校验、
  自行接受缺字段默认值或修改`dataset_release/pit.py`来适配消费者。
- 公开输入为不可变release manifest及显式usage mode：`formal_training/formal_prediction`只接受full canonical binding；
  sample和所有legacy/v1 manifest始终拒绝。历史reproduction继续由W3-B/C各自现有legacy reader显式处理，不允许
  W3-A发明通用v1默认值或把legacy identity投影成canonical binding。
- 返回统一identity projection：`authority_id/rule_version/rule_parameters_digest/release_id/cutoff/
  frozen_snapshot_digest/manifest_digest`。任何字段缺失、digest冲突、scope错误或v1普通运行均typed fail。
- 直接测试固定为`backend/tests/dataset_release/test_canonical_pit_dataset_consumer.py`，覆盖full PASS、sample拒绝、所有v1拒绝、
  manifest篡改和禁止在线DB fallback；显式reproduction PASS由W3-B/C的domain reader测试负责。

退出：PR-3A独立合入；公共API与W2 manifest兼容，QE/HMM尚未改变运行默认值。

##### W3-B QE小切片

- 精确候选文件：`quantevolver/qe_dataset_contract.py`、`experiment_config.py`、`config_composer.py`、
  `factor_universe_mask_service.py`、`long_trend_data_reader.py`和`scripts/backfill_factor_cache.py`。实施前按实际调用图
  进一步缩小；未进入最终PR的文件立即释放租约，不因候选清单而长期冻结。
- QE dataset contract从W3-A adapter获得frozen identity，显式v2正式实验/因子批算/长期趋势读取保存同一release identity；
  旧环境变量和硬编码dataset常量仅可在显式reproduction reader中解析，不能作为正式任务默认值。
- frozen factor universe/mask只消费candidate artifact，不调用在线`StockUniversePitService`或生产DB补齐；composer只
  传递已验证manifest ref/digest，不通过任意path/key覆盖生成另一套身份。
- 直接测试以`backend/tests/quantevolver/test_canonical_pit_dataset_binding.py`为主，并覆盖config serialization、
  factor-cache identity、long-trend manifest parity和v1 reproduction隔离。

退出：PR-3B合入；显式v2正式QE任务必须携带full binding。enablement阶段只增加该能力，不改变当前生产admission；
已开始的v1任务继续固定原commit/release，W9 activation才切换新任务默认admission。

##### W3-C HMM小切片

- 精确候选文件：`hmm_data_source/legacy_qe_artifact_manifests.py`、`hmm_evolution/universe.py`、
  `hmm_risk/stock_fact_repository.py`和`scripts/hmm_risk/repair_b3_stock_fact_gaps.py`。同样按实际调用图缩小并短租约执行。
- 显式v2 HMM训练/预测从W3-A adapter读取与QE相同的frozen identity；legacy QE manifest reader仅服务显式reproduction，
  不得把`shsz_st_pit_active_v1`或旧dataset prefix提升为正式默认值。
- `stock_fact_repository.py`在线查询通过W1 resolver绑定rolling key和`activation_generation`；离线训练、回测和预测
  数据读取不得导入/调用该repository来补齐frozen数据。repair脚本只处理显式plan和目标，不改变consumer binding。
- 直接测试以`backend/tests/hmm_data_source/test_isolation_constraints.py`及HMM evolution/risk直接测试为主；PR-3C同时
  新增`backend/tests/test_qe_hmm_canonical_pit_integration.py`，证明离线路径零在线DB调用、在线路径generation漂移
  fail closed、QE/HMM identity完全相同。

退出：PR-3C合入；HMM enablement完成但不代表模型已重训、重新认证或生产runtime已切换。

##### W3-D 最终验证（无业务源码PR）

- 从包含PR-3A/B/C的最新main运行QE/HMM直接测试、consumer inventory和结构化identity comparison；不在旧W3分支
  上拼接证据，不修改业务源码，不启动正式实验或训练。
- 必须证明：QE/HMM投影的canonical tuple完全一致；full v2 formal PASS；sample和普通v1拒绝；显式v1 reproduction
  仍可读；manifest/digest篡改fail closed；离线路径无在线DB fallback；已有运行实验仍固定旧commit/identity。
- 任一失败返回W3-A/B/C对应责任切片，新修复PR从最新main开始；修复合入后W3-D全部重跑，旧receipt作废。
- 对模型分类：截面rank/neutralization/label依赖者标记`retrain_required`；纯单股时序且PIT仅做外层过滤者可标记
  `revalidate_then_republish`，但不得直接沿用生产资格。

退出：最终main上的W3 source-ready证据PASS；不等于模型重训、真实candidate、runtime activation或重启完成。

#### P3-B Selection/Paper/Simulation（W4）

- Selection risk policy通过resolver获取rolling binding，不接受请求任意key。
- Paper/Simulation继承同一Selection binding，不形成独立配置。
- 已持久化的Paper/Simulation runtime profile和JSON配置增加版本化migration：legacy profile在enablement阶段可
  显式解析为v1迁移态，activation前必须迁移为`canonical_authority_pointer_v1`；未知/混合profile fail closed，
  不在读取时静默改写数据库。
- P0 inventory覆盖数据库JSON、active runtime release和未结束session。新profile版本保留旧hash，不原地改旧行；
  selection request、Paper session和simulation run在开始时冻结`activation_generation`。已开始session继续其冻结
  generation；发现中途漂移时拒绝混用并产生typed drift receipt，不自动切换。
- activation readiness先关闭v1新admission，再登记所有旧generation lease。已有只读、有限期QE/训练任务可进入
  `SESSION_PINNED_DRAINING`并按原frozen release完成；有撮合、下单、资金/持仓状态写入的Paper/Simulation session
  必须在最终CAS前自然终止。任何无明确终点、无法枚举或仍有side effect的session均阻断activation；不得由本任务
  擅自停止进程或会话。
- PIT只决定`eligible_for_new_position/holding_must_exit`；停牌、涨跌停、时段和报价仍由`orderable_now`决定。
- 增加shadow mode：同一trade date只读计算v1/v2差异和原因，不提交订单、不写正式荐股结果。

#### P3-C StrategyPackage/Advisory（W5）

- 新package manifest冻结authority、rule、parameter digest、release/cutoff/snapshot identity。
- 已发布v1 package完全不改写；未来继续运行者生成新v2版本并按模型分类重新认证或重训。
- manifest和advisory projection采用显式schema双读：v1 reader仅允许reproduction，v2 writer只写完整canonical
  binding；不得通过缺字段默认值把v1包升级成普通运行。
- Advisory historical range、catalog、input projection和feature source取消生产v1硬编码，统一从包/请求已验证binding读取。
- `backend/inference_engine.py`由W6作为共享单写scope迁移：删除PIT fallback，输入/输出receipt加入
  `authority_id/rule_version/rule_parameters_digest/activation_generation`，供W4和W5共同验证。
- reproduction输出不能进入荐股、Paper、Simulation或新的正式训练。

退出门禁：每个窗口直接测试通过、无未分类生产v1引用、无运行时默认提前切到v2。

### P4 集成与小样本源码验收（W6）

1. 构建changed-file consumer inventory guard；新增消费者若绕过resolver则CI失败。
2. 使用3～5只证券和数个关键交易日fixture覆盖正常股、IPO第251/252日、ST/摘帽、退市/吸收合并、长期停牌。
3. 证明rolling/frozen identity、all.txt、daily/minute/factor、Selection/Paper/StrategyPackage在同cutoff一致。
4. 证明普通v1被拒绝、显式历史reproduction仍成功。
5. 对每类incremental/selective结果与同输入clean full oracle比较ordered index、dtype、NaN mask和冻结数值容差；
   baseline Merkle必须前后不变，candidate manifest不得包含candidate root外的物理路径依赖。
6. component artifact manifest必须使用storage v2分片；canonical daily/minute lineage新writer只写v3，lineage reader
   显式支持legacy v1/composite与v3。daily/minute outer materialization/preparation schema另行显式支持v1/v2；
   旧worker不得resume v3 attempt。legacy达到16 MiB或预测下月超限时在仍可有界读取时迁移。
7. 以6000 instruments × 36 months合成producer验证每个CAS JSON硬上限32 MiB、目标8 MiB/128 rows、固定256
   lineage buckets和有界峰值内存。
8. 运行最小本地门禁和委托跨模块验证，生成绑定最终commit的receipt。

退出门禁：source-ready，不等于真实数据ready；所有生产gate仍为pending/noop。

### P5 极小真实数据候选（W7）

W2新增仅允许仓库白名单id的首次迁移入口，不接受任意路径/代码/日期：

```powershell
rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v2 initial-migration `
  --plan pit_v2_initial_20260731_v1 --scope sample --candidate-only
rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v2 status --latest
rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v2 receipt --run-id <run_id>
```

checked-in `configs/datasets/migrations/pit_v2_initial_20260731_v1.yaml`冻结如下语义；loader对canonical payload计算
SHA-256并在submission/receipt中保存`plan_digest`，任何修改必须升级plan id：

```text
cutoff: 2026-07-31
sample_instruments: [000001.SZ, 300379.SZ, 600462.SH, 600930.SH, 688981.SH]
event_windows:
  - 600930.SH: 2026-07-27..2026-07-31  # IPO第251/252交易日：2026-07-29/30
  - 600930.SH: 2026-07-02..2026-07-06  # adj-factor变化
  - 600462.SH: 2019-01-14..2019-01-16  # ST公告/实施
  - 600462.SH: 2022-04-28..2022-05-06  # ST恢复边界
  - 600462.SH: 2025-06-17..2025-06-24  # 终止上市风险事件
  - 600462.SH: 2025-07-18..2025-07-21  # delist生命周期终点
  - 300379.SZ: 2026-01-21..2026-01-23  # adj-factor与长期停牌期间
  - 300379.SZ: 2026-06-05..2026-06-09  # 长停恢复边界/创业板制度
  - 000001.SZ: 2025-06-11..2025-06-13  # 主板正常股/除权边界
  - 688981.SH: 2020-08-21..2020-08-25  # 科创板20%涨跌停制度
index_windows:
  - 000688.SH: 2019-12-31..2020-01-03  # required-from边界，2019-12-31必须排除
  - all_12_indices: 2026-07-30..2026-07-31
```

上述代码/日期来自2026-08-13本地authority只读探针；implementation必须再次读回source identity并将其digest
写入plan，若事件已被权威源修订则停止并先修订plan版本，不能在运行时临时换样本。测试数据写入新的sample
candidate root，不写生产DB，不触碰6月30日/7月31日目录。

验收：

- PIT/公告/交易日历/退市生命周期逐事件人工可解释。
- TDX分钟优先、Tushare仅补missing key；重叠冲突、非240根、40203均fail closed。
- daily/minute聚合、复权、涨跌停和factor小样本数值一致。
- 峰值资源低于profile hard cap，swap为0，无其他进程控制。
- 失败后只修源码或candidate-local缺口；修复合入后必须以新commit重新执行小样本，旧receipt失效。

退出门禁：小样本terminal PASS并绑定最终source commit；否则不得开始全量。

### P6 完整v2 candidate-only构建（W7）

1. 使用同一白名单计划提交一次full intent；不得并行启动第二个exporter：

   ```powershell
   rtk python scripts/update_backtest_dataset_monthly.py --profile qe_hmm_full_v2 initial-migration `
     --plan pit_v2_initial_20260731_v1 --scope full --candidate-only
   ```

   full必须与通过的sample具有相同plan/source commit/profile/toolchain digest；普通`monthly`不接受手工cutoff。
2. planner逐组件记录`REUSE/INCREMENTAL/SELECTIVE_REBUILD/FULL_REBUILD`和理由。
3. 缺失优先按本地DB/TDX/Tushare authority补齐；无法补齐进入typed exception ledger并阻断签收。
4. 候选完整物化到独立X盘目录；即使大量组件复用，也必须形成完整可读artifact graph和新release manifest。
5. 构建过程中只读汇报control status/events/receipt，不扫描全目录、不把完整日志读入内存。
6. 若全量构建暴露源码缺陷，W7将attempt置为typed terminal/waiting并保留checkpoint/失败证据；对应W1～W6
   在新任务commit修复并重新验证。恢复时必须重新解析source commit、profile/toolchain digest和release identity；
   不得让旧attempt在漂移源码上继续，也不得删除失败candidate来伪装恢复。

退出门禁：terminal `CANDIDATE_VALIDATED`、全部required validation PASS、catalog/marker/receipt一致。

### P7 独立数据审核（W8）

审核采用“全量结构/业务闭环门禁 + 独立分层数值oracle”，禁止用新旧逐行全量比较代替正确性：

| oracle | 范围 | authority/公式/容差 | PASS标准 |
|---|---|---|---|
| 文件与身份 | 全量 | candidate file graph、schema、ordered index、dtype、calendar/PIT/manifest SHA | 无候选外路径、缺失、重复、非有限值或digest漂移 |
| PIT生命周期 | 全量spans、全部D/P、全部ST snapshot gap和terminal事件 | `stock_basic`、交易日历、公告/event、ST snapshot；独立ordered encoding | 每个entry/exit绑定交易日序号、公告/实施/首个可决策时点；terminal后不得重入；gap全部闭环 |
| v1/v2差异 | 全量股票池集合摘要，差异记录逐项分类 | 252td、历史退市、ST/终止as-of或有source revision的修复 | 无unknown reason；不以v1一致作为v2正确性的充分条件 |
| QFQ/limit | 固定seed分层数值样本 + 全量schema/NaN/边界计数 | `QLIB_STOCK_VALUE_CONTRACT`：`raw_price/1000*adj/max_adj`、volume反向调整、raw CNY prev/limit；价格abs tol `1e-4` | 除权日、denominator变化、主板/创业板/科创板规则边界均匹配raw authority和公式 |
| suspend/minute | 全量coverage计数 + 事件样本 | `suspend_d`、TDX-first、Tushare missing-only、240 session rows | 整日停牌按prev-close QFQ/零量补齐；部分缺口、冲突、非240、40203 fail |
| factor/static | 全量key/schema/dtype/NaN mask摘要 + 每因子依赖族固定seed源值重算 | factor definition/version、原始/派生字段、有效观测窗口、PIT mask、float容差随schema冻结 | row-local复用和截面重算范围正确；121列、`l2_code_id int16/-1`不漂移 |
| 申万L2 | 全量区间重叠/映射摘要 + 变更边界样本 | `code_map_digest`、`membership_digest`、as-of、winner policy | 无歧义重叠；歧义区间fail closed；历史同日有独立source证据 |
| 资金流 | 全量分块公式parity | shares/CNY单位、raw H5/static、5/20 observation公式和相同NaN mask | 每chunk完全相同，不能由抽样替代 |
| 12指数/HMM | 全量12代码/required-from/coverage/逐字段值 | `.codex/skills/update-backtest-dataset/references/index-hmm-contract.md` | exact code/order/单位；股票与指数隔离；`000300.SH` benchmark不变；`000688.SH`从2020-01-02开始 |
| 消费者 | 每类至少一个candidate smoke | candidate bundle/envelope中的同一authority/rule/digest/generation | QE/HMM、Selection、Paper/Simulation、StrategyPackage无fallback且身份一致 |

数值样本按年份、板块、ST/非ST、退市/存续、IPO、停牌、除权、涨跌停制度和申万换档分层；固定seed、
样本清单、source revision和容差进入W8 receipt。抽样只用于独立数值复核；全量业务闭环、结构和指数/资金流
明确要求不因抽样而降低。

退出门禁：独立审核receipt PASS；所有差异已分类；没有未关闭数据缺口或资源违规。

### P8 StrategyPackage/模型重新认证（W3/W5，使用W8候选）

- `retrain_required`模型必须在v2 frozen snapshot上形成新模型/包版本和独立验证。
- `revalidate_then_republish`模型必须完成v1/v2影子回测、样本覆盖和阈值稳定性验证，再发布v2兼容包。
- 旧v1包继续只读复现；不得删除、改manifest或让其驱动激活后的新荐股/模拟盘。

退出门禁：拟继续生产/模拟运行的全部包均有v2兼容状态；未迁移包从active admission中显式排除。

### P9 Activation与后续月更（W9）

执行顺序：

1. 在W7以前完成全部source/config PR；DEV migration/apply/readback和rollback验证PASS，enablement源码进入main。
2. 若enablement影响backend/Worker，用户在生产仍指向v1迁移态时先完成重启；只读核验新代码已加载且v1业务未变。
3. W7形成不可变candidate bundle、W8形成绑定该bundle的独立receipt；此后禁止任何source/config修改。若发现缺陷，退回
   W1～W6修复，并使W7/W8证据失效后重跑。
4. 用户确认具体candidate release、migration、生产target和activation；生产target preflight必须匹配candidate bundle。
5. 执行生产DDL（若需要）并readback，但pointer仍指向v1；将v2 frozen artifact以inactive状态分发到全部计算节点，
   逐节点校验manifest和文件digest。任一节点缺失时不执行CAS。
6. 关闭v1新admission，枚举旧generation lease；等待所有side-effecting/unbounded session归零。有限只读旧QE任务
   可作为`SESSION_PINNED_DRAINING`继续，但不得产生新admission。将distribution和drain readback封装为activation
   envelope并生成独立readiness receipt。
7. 对在线rolling、离线frozen、消费者版本、candidate bundle、W8 receipt和sealed envelope做最终read-only
   preflight；在一个数据库事务中执行versions/pointer/event CAS。该DB事务是唯一commit point，不宣称DB与文件
   系统或多节点存在分布式原子事务。
8. 新QE/HMM任务由激活后的backend admission生成v2 frozen binding；有限只读旧generation任务按drain lease完成，
   最后一个lease结束后v1转`ARCHIVED_NONCANONICAL`。新Selection/Paper/Simulation固定新pointer generation；不存在
   跨CAS继续运行的旧side-effecting session。
9. 激活后只读核验registry generation、candidate bundle/activation envelope、QE/Selection/Paper/StrategyPackage业务smoke；失败时
   使用expected-current-generation CAS回滚pointer和version状态，不删除任何artifact。
10. 若PR修改`.codex/**`或`.claude/**`，客户端verify/install必须在W7之前完成并绑定source freeze；client reload与
   backend restart分别报告。
11. 后续每月只提交一次`qe_hmm_full_v2 monthly --candidate-only`，同cutoff由fresh probe决定NO_OP/reattest/rebuild。

rollback创建新的单调递增generation，并让其引用最后一个已验证完整release；绝不把generation计数倒退或覆盖
历史event，也不删除v2数据。紧急回到v1时必须标注
`EMERGENCY_LEGACY_ROLLBACK`和survivorship limitation，不能重新声明为长期权威；该状态禁止新的正式QE训练、
新StrategyPackage admission和长期运行，恢复到已验证v2是唯一退出路径。

## 7. Verification Plan / 数据组件与验收方案

### 7.1 数据组件任务与验收标准

| 组件 | v2动作原则 | 必须验证 | 默认禁止 |
|---|---|---|---|
| PIT snapshot/all.txt | 全量按v2生成 | 252交易日、D/P生命周期、ST/终止as-of、digest | 只改all.txt |
| Daily bin | 复用稳定分区，追加截止月并补退市缺口 | OHLCV/amount/factor/prev_close/limit字段、日期覆盖 | 全量frames合并 |
| Minute bin | TDX优先，Tushare missing-only | 240根、daily聚合、重叠一致、停牌语义 | provider冲突覆盖 |
| QFQ/adj | 受denominator变化的code重放必要历史 | factor和前复权样本、新旧同源一致 | 只修最新日期 |
| H5动态因子 | 依赖图驱动row-local复用/截面重算 | 股票池/PIT mask/窗口传播/非有限值 | 无证明整包复用 |
| Static/sector | 重建v2股票/PIT区间，保持121列 | `l2_code_id int16`、missing=-1、申万历史区间 | 字符串板块替代ID |
| Limit/suspend | append或受影响日期重建 | 同日limit_up/down、suspend、prev_close | 把不可交易等同不在PIT |
| 12-index/HMM | 指数只增量到cutoff | exact codes、000300.SH benchmark、单位/字段 | 扩展/替换指数清单 |
| Manifest/receipt | 新release完整生成 | artifact/source/PIT/validation/resource digest | 复用旧receipt冒充新签收 |

### 7.2 资源与性能验收

`qe_backtest_monthly_v2.yaml`按方向冻结资源合同：

| 字段 | 类型 | 合同 |
|---|---|---|
| heavy full concurrency | exact/max | 1；只允许更低并发，不允许更高 |
| aggregate private commit | hard max | 12 GiB；可以收紧，不能扩大 |
| Windows Job / hybrid Windows | hard max | 8 GiB / 4 GiB；可以收紧，不能扩大 |
| WSL memory high/max/swap | exact hard | 6/8/0 GiB；swap必须恒为0 |
| WSL start/emergency MemAvailable | minimum reserve | 12/6 GiB；不得降低，可提高 |
| host start/emergency available及commit headroom | minimum reserve | 16/8 GiB；不得降低，可提高 |
| DB pool/row-producing/provider concurrency | max | 4/1/1；不得提高 |
| minute batch/date chunk/row group/dump workers | pressure ladder max | 20→10→5；3→1月；100k→50k；8→4→2，只能在安全checkpoint后收紧 |
| H5 batch 100→50→20 | telemetry only | `reserved_profile_telemetry_not_consumed_v1`；不能宣称为有效降压；实际由单日切片和row-group限内存 |
| candidate free space | minimum reserve | `max(32 GiB, 1.25 × predicted_new_bytes)`；不得降低 |

验收要求：

1. 源码合入gate：同cache class、相同semantic workload的合成benchmark各至少3次，median要求
   `compute_seconds_new <= 1.10 × baseline`、`rows/s_new >= 0.90 × baseline`、row-producing query增量不超过
   `max(2,5%)`，peak commit不超过hard cap且不超过`max(1.10 × baseline, baseline+256 MiB)`。
2. 真实月更runtime gate：按stage记录source-read/provider/compute/validation/resource-wait、rows/bytes、query和I/O，
   resource receipt必须包含WSL start/emergency MemAvailable readback；
   >10%归一化退化先记warning，只有持续15分钟`throughput <70% baseline`或退化>30%才在checkpoint进入
   `WAITING_PERFORMANCE_REGRESSION`，不能自动加并发。
3. 资源等待不能计入compute regression，但必须单独报告；无可信DB revision ledger时，source freeze和publish前
   recheck仍可能各做一次全值扫描，“选择性少写”不等于“仅读新增月”。
4. 任一hard breach、swap使用、零进展30分钟或单SQL超过300秒，立即checkpoint并typed fail/wait。
5. 资源监管只能管理identity-bound当前task child，绝不停止其他窗口、QE实验或用户进程。

## 8. PR、审核与合入顺序

| PR | 窗口 | 内容 | 合入前条件 | 合入后仍不代表 |
|---|---|---|---|---|
| PR-0 | W0 | 本主设计 | 三轮设计审核+F2 validator | 允许实现/导出 |
| PR-1 | W1 | Core/registry/migration source | DEV migration fixture、contract tests | 生产DDL/DML |
| PR-2 | W2 | Dataset release v2 enablement | resource/materializer/validator matrix | 真实candidate完成 |
| PR-3A | W3-A | 中立正式frozen consumer adapter | full/sample/v1 rejection/tamper直接测试 | QE/HMM已迁移或legacy reproduction已验证 |
| PR-3B | W3-B | QE最小入口接入 | QE binding、cache、reader和serialization测试 | HMM已迁移或生产默认已切换 |
| PR-3C | W3-C | HMM最小入口接入 | HMM isolation、generation和identity测试 | 模型已重训或重新认证 |
| 无源码PR | W3-D | 最终main上的QE/HMM统一验证 | A/B/C均合入、inventory无unknown、identity/fallback矩阵PASS | runtime、真实训练或candidate已完成 |
| PR-4 | W4 | Selection/Paper/Simulation | rolling/shadow/orderability tests | 生产已切v2 |
| PR-5 | W5 | StrategyPackage/Advisory | immutable old package+compatibility tests | 旧包可直接运行v2 |
| PR-6 | W6 | 集成inventory/跨模块tests/candidate bundle+activation envelope/Skill及全部activation源码配置 | 最终HEAD receipt、无unknown引用 | candidate或activation完成 |
| 无源码PR | W9 | 仅执行已合入migration、inactive distribution和DB CAS | W8 PASS+用户对精确target授权 | 重启/cleanup已授权 |

每个PR先执行窗口直接测试和`git diff --check`，再由独立审核检查scope、业务语义、fail-closed、测试充分性和
DESIGN-COMPLIANCE-001。公共契约修改不得在多个PR中并行漂移；后续窗口必须从已合入main重新创建或同步。
W7开始后禁止新增activation源码/config PR；任何功能改动使W7/W8 receipts失效并回到P4。

## 9. 审核模型

### Review A：架构与单一权威

- 是否只有一个active authority和一个共享resolver。
- rolling/frozen是否仅为同规则不同物化。
- registry CAS、release manifest、reproduction和rollback是否闭合。
- 是否存在模块自行选择key、latest-ready猜测或DB fallback。

### Review B：数据正确性、资源和保留

- 252交易日、历史退市、ST/终止as-of、复权/涨跌停/因子/板块/指数是否有可执行oracle。
- 是否按依赖选择性重建且完整candidate可读。
- 是否彻底禁止无界frames、全矩阵、无界日志和自动删除。
- 6月30日/7月31日完整历史release是否至少保留一份。

### Review C：交付、运行时和生产安全

- 每个窗口scope和依赖是否互斥、可合入、可恢复。
- source merge、candidate、DEV/生产DDL/DML、activation、distribution、restart、cleanup是否分别报告。
- receipts是否绑定最终commit/candidate/generation。
- 用户重启前是否保持`runtime_activation=pending`，没有伪造生产完成。

所有P0/P1意见必须在本设计Review History中关闭；实现阶段的新问题回写对应Acceptance Index，不得口头豁免。

## 10. Risks / Failure Modes

| 风险 | 后果 | 控制 |
|---|---|---|
| 模块独立硬编码key | 多权威、部分切换 | singleton registry + inventory guard |
| v2数据未ready即激活 | 在线消费者失败或混用 | candidate receipt + CAS preflight + fail closed |
| PIT变更仅更新all.txt | 因子/bin与股票池不一致 | dependency planner + cross-component digest |
| 退市/ST源证据缺失 | 幸存者偏差或未来信息 | exception ledger阻断签收 |
| 分批查询仍累计frames | 数十GiB内存和系统卡顿 | stream/chunk/COW + hard commit cap |
| 为赶速度提高并发 | DB/WSL/主机资源失控 | concurrency=1 + pressure ladder |
| 旧包manifest原地改写 | 历史不可复现 | 新v2包版本，旧包只读 |
| 删除6月/7月历史release | 正式实验无法复现 | FULL_IMMUTABLE + 冷存储/去重优先 |
| 多窗口修改公共契约 | 合并冲突和语义漂移 | W3-A中立单写 + A/B/C串行小PR |
| 长期冻结QE/HMM业务文件 | 阻塞实验BUG、迫使并行覆盖 | 单PR短租约、仅目标文件交接、BUG优先抢占并从新main重放 |
| 旧实验被误当v2证据 | identity和runtime结论失真 | 实验固定启动commit/release；W3-D只接受最终main新证据 |
| receipt绑定旧commit | 验证证据失真 | 最终HEAD/candidate/generation强绑定 |

## 11. Rollout / Rollback

源码按PR-0至PR-6依赖顺序合入；任何enablement合入均不改变生产active authority。W7/W8完成真实candidate与
独立审核后，W9才可在用户对具体target、migration、candidate和activation授权下执行已合入的registry CAS。
用户完成运行时重启后再做只读业务核验。

回滚创建新generation引用最后一个已验证完整release，必须使用expected current generation CAS并保存
append-only回滚receipt；inactive artifact和节点manifest不需要删除或回写。不删除v2、v1或任何历史数据。
若紧急回到v1，状态为明确的legacy emergency并保留
survivorship limitation，不得把v1重新声明为长期权威。

## 12. Design Acceptance Index

| ID | 验收要求 |
|---|---|
| F-001 | v2为未来唯一ACTIVE_CANONICAL；v1仅迁移态和显式reproduction。 |
| F-002 | rolling/frozen共享authority/rule/parameters，cutoff snapshot digest可证明等价。 |
| F-003 | singleton pointer在单一DB事务内以generation CAS原子切换；不宣称跨节点/运行时原子，消费者不得猜latest。 |
| F-004 | QE/HMM计算面从不可变manifest读取binding，不回退在线DB。 |
| F-005 | 所有生产v1引用完成分类，unknown为零；历史identity不做全局替换。 |
| F-006 | 每个源码切片有独立worktree、短期精确文件租约、入口/退出门禁和结构化receipt；无关实验和开发不被冻结。 |
| F-007 | W3按A→B→C→D串行，W4/W5仅在文件互斥时并行；W7构建期间源码冻结。 |
| F-008 | v2 PIT覆盖252交易日、历史D/P、ST/终止as-of和exception ledger。 |
| F-009 | planner按依赖选择复用/增量/选择性/全重建，不默认全量重新导出。 |
| F-010 | PIT变化不会被错误降级为仅更新all.txt；完整candidate artifact graph可独立读取。 |
| F-011 | minute为TDX-first/Tushare missing-only，冲突/空结果/非240/40203 fail closed。 |
| F-012 | 复权、涨跌停、停牌、资金流、121静态列、申万ID、12指数/HMM有直接oracle。 |
| F-013 | QE/HMM/训练普通任务拒绝归档规则，正式结果绑定release identity。 |
| F-014 | Selection/Paper/Simulation共享rolling binding，PIT与orderable_now职责不混淆。 |
| F-015 | StrategyPackage旧manifest不改写；v2继续运行必须重新认证或重训。 |
| F-016 | 小样本失败修复后绑定新commit重跑，不能沿用旧receipt。 |
| F-017 | 全量候选只写新X盘root，不覆盖6月30日/7月31日或生产路径。 |
| F-018 | 审核使用全量结构/digest+分层数值采样，不执行新旧逐行全量比较。 |
| F-019 | 内存/并发/磁盘/swap硬边界和性能归一化门禁可执行且fail closed。 |
| F-020 | 已引用历史release保留一份完整副本；清理只针对证明无引用失败候选或重复副本。 |
| F-021 | 月更保持一次candidate-only提交，NO_OP/reattest/rebuild由fresh durable evidence决定。 |
| F-022 | source、candidate、DDL/DML、activation、distribution、restart和cleanup各自独立授权/回执。 |
| F-023 | activation CAS、计算节点digest、用户重启后smoke和rollback均绑定明确identity。 |
| F-024 | DESIGN-COMPLIANCE-001四项分别有直接证据，无简化、静默错误、业务漂移或私增门禁。 |
| F-025 | 每类incremental/selective结果与clean full在ordered index、dtype、NaN mask和数值容差上等价，baseline Merkle不变且无候选外路径依赖。 |
| F-026 | component manifest storage v2、canonical lineage v3、legacy迁移与6000×36容量门禁满足长期月更。 |
| F-027 | 首次迁移计划固定2026-07-31、样本代码/事件窗口/plan digest，并由同一durable pipeline运行sample和full。 |
| F-028 | W3-A只复用W1/W2公共API形成中立正式consumer adapter且拒绝全部legacy；QE/HMM不得复制v2解析器或互相反向依赖。 |
| F-029 | W3短租约允许同文件BUG优先抢占；仅目标文件需提交/交接，旧实验固定原commit且不作为v2验收。 |
| F-030 | W3-D在A/B/C合入后的最终main验证QE/HMM identity、v1 reproduction、sample/v1拒绝和零在线DB fallback。 |

## 13. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | §4 D-001；`canonical_equity_pit.py`与W1/W6 planned scope | `backend/tests/test_canonical_equity_pit.py`；planned `backend/tests/test_canonical_pit_consumer_inventory.py` | design_ready_for_review | none |
| F-002 | §4 D-001/D-002；`dataset_release/pit.py`与W1/W2 planned scope | `backend/tests/dataset_release/test_pit.py` | design_ready_for_review | none |
| F-003 | §4 D-002；planned registry migration与resolver | planned `backend/tests/test_canonical_equity_pit_authority_registry.py` | design_ready_for_review | none |
| F-004 | §4 D-002、§6 P3-A/W3-A；中立frozen binding adapter | `backend/tests/dataset_release/test_canonical_pit_dataset_consumer.py` | implementation_verified | none |
| F-005 | §6 P0/P3/P4；W0/W3/W4/W5/W6 inventory scope | planned `backend/tests/test_canonical_pit_consumer_inventory.py` | design_ready_for_review | none |
| F-006 | §5/§5.2；各切片task card、短租约和allowed write scope | artifact: `tests/aistock_validation/pit_v2/window_scope_receipt.json` | design_ready_for_review | none |
| F-007 | §5.1、§6；W3串行切片、W0/W6/W7 source freeze contract | artifact: `tests/aistock_validation/pit_v2/source_freeze_receipt.json` | design_ready_for_review | none |
| F-008 | §6 P2/P5/P7；PIT builder与W1/W7/W8 scope | `backend/tests/test_stock_universe_pit_spans.py` | design_ready_for_review | none |
| F-009 | §4 D-004、§6 P6；dependency planner/materializer | `backend/tests/dataset_release/test_dependency_graph.py` | design_ready_for_review | none |
| F-010 | §6 P4/P6、§7.1；candidate manifest/validator | `backend/tests/dataset_release/test_candidate_validator.py` | design_ready_for_review | none |
| F-011 | §6 P5、§7.1；minute source/overlay | `backend/tests/dataset_release/test_minute_overlay.py` | design_ready_for_review | none |
| F-012 | §7.1；component validators | `backend/tests/dataset_release/test_candidate_validator.py`；`backend/tests/dataset_release/test_index_context.py` | design_ready_for_review | none |
| F-013 | §6 P3-A/P8；W3-B/C QE/HMM/训练planned scope | planned `backend/tests/quantevolver/test_canonical_pit_dataset_binding.py`；`backend/tests/hmm_data_source/test_isolation_constraints.py` | design_ready_for_review | none |
| F-014 | §6 P3-B；Selection/Paper/Simulation planned scope | planned `backend/tests/selection_center/test_canonical_pit_runtime.py`；`backend/tests/paper_trading_v2/test_runtime_profile.py` | design_ready_for_review | none |
| F-015 | §6 P3-C/P8；StrategyPackage/Advisory planned scope | planned `backend/tests/strategy_package/test_canonical_pit_compatibility.py` | design_ready_for_review | none |
| F-016 | §6 P5；W6/W7 source identity contract | artifact: `tests/aistock_validation/pit_v2/small_candidate_receipt.json` | design_ready_for_review | none |
| F-017 | §6 P5/P6；W7/W8 path and Merkle contract | artifact: `tests/aistock_validation/pit_v2/historical_immutability_receipt.json` | design_ready_for_review | none |
| F-018 | §6 P7；W8 audit plan | artifact: `tests/aistock_validation/pit_v2/candidate_audit_receipt.json` | design_ready_for_review | none |
| F-019 | §7.2；resource supervisor和W7/W8 receipt | `backend/tests/dataset_release/test_synthetic_benchmark.py`；artifact: `tests/aistock_validation/pit_v2/resource_receipt.json` | design_ready_for_review | none |
| F-020 | §4 D-005、§6 P0；retention classifier和W0/W9 inventory | `backend/tests/dataset_release/test_retention.py`；artifact: `tests/aistock_validation/pit_v2/retention_inventory.json` | design_ready_for_review | none |
| F-021 | §6 P9；monthly CLI/control catalog | `backend/tests/scripts/test_update_backtest_dataset_monthly.py` | design_ready_for_review | none |
| F-022 | §6 P9、§8、§14；W9 action-scoped gates | artifact: `tests/aistock_validation/pit_v2/production_gate_receipt.json` | design_ready_for_review | none |
| F-023 | §6 P9、§11；W9 activation/rollback | artifact: `tests/aistock_validation/pit_v2/activation_identity_receipt.json` | design_ready_for_review | none |
| F-024 | §9、§15；W0及每个实现窗口 | artifact: `tests/aistock_validation/pit_v2/design_compliance_receipt.json` | design_ready_for_review | none |
| F-025 | §6 P4；incremental/materializer/validator planned scope | `backend/tests/dataset_release/test_candidate_validator.py`；planned `backend/tests/dataset_release/test_selective_clean_full_parity.py` | design_ready_for_review | none |
| F-026 | §6 P4；component manifest v2/canonical lineage v3 planned scope | `backend/tests/dataset_release/test_component_artifact_manifest.py`；`backend/tests/dataset_release/test_canonical_lineage.py` | design_ready_for_review | none |
| F-027 | §6 P2/P5/P6；initial migration plan、control service、resolution reader和CLI planned scope | `backend/tests/dataset_release/test_control_service.py`；`backend/tests/dataset_release/test_resolution_processor.py`；`backend/tests/scripts/test_update_backtest_dataset_monthly.py`；artifact: `tests/aistock_validation/pit_v2/small_candidate_receipt.json` | design_ready_for_review | none |
| F-028 | §5/§6 P3-A W3-A；neutral formal adapter与W1/W2 API边界 | `backend/services/canonical_pit_dataset_consumer.py`；`backend/tests/dataset_release/test_canonical_pit_dataset_consumer.py` | implementation_verified | none |
| F-029 | §5.2；短租约、BUG抢占、dirty handoff和实验identity边界 | artifact: `tests/aistock_validation/pit_v2/window_scope_receipt.json` | design_ready_for_review | none |
| F-030 | §6 P3-A W3-D；最终main统一身份和隔离矩阵 | planned `backend/tests/test_qe_hmm_canonical_pit_integration.py`；command: `python -m pytest backend/tests/dataset_release/test_canonical_pit_dataset_consumer.py backend/tests/quantevolver/test_canonical_pit_dataset_binding.py backend/tests/hmm_data_source/test_isolation_constraints.py backend/tests/test_qe_hmm_canonical_pit_integration.py -q` | design_ready_for_review | none |

设计通过只表示可以请求用户确认进入实施；不得把`designed`状态表述为源码、真实数据或生产完成。

## 14. 生产门禁状态

| 动作 | 当前状态 |
|---|---|
| W1 Core/Registry源码 | `merged_4e1f667e` |
| W2 Dataset Release源码 | `merged_589678f3` |
| W3消费者源码 | `not_started_pending_user_revised_design_confirmation` |
| W4～W6消费者/集成源码 | `not_started_or_separately_owned` |
| 真实小样本 | `not_run_not_authorized` |
| 真实全量candidate | `not_run_not_authorized` |
| DEV DDL/DML | `not_revalidated_in_this_design_revision` |
| 生产DDL/DML | `pending_separate_targeted_authorization` |
| production activation | `not_requested` |
| node1/计算节点distribution | `not_requested` |
| backend/Worker/runtime restart | `owner=user_not_requested` |
| 历史数据修改/覆盖 | `forbidden` |
| cleanup/delete | `not_requested_exact_paths_required` |

## 15. DESIGN-COMPLIANCE-001

1. **禁止简化交付**：小样本、`all.txt`、fixture、source-ready或单个消费者通过均不能冒充完整v2升级。
2. **禁止静默错误**：source/PIT/provider/digest/consumer/资源冲突一律typed fail/wait，不回退旧key或减少范围。
3. **禁止改变业务逻辑**：唯一authority、252交易日、历史退市PIT、as-of、TDX-first、12指数、121静态列、
   完整历史release保留和candidate-only语义不得被各窗口自行修改。
4. **禁止私增门禁**：普通月更不增加人工审批；只保留既有技术签收和生产DDL/DML、activation、restart、cleanup
   独立授权边界。

设计级逐项审查：

| 检查项 | 直接证据 | 设计结论 |
|---|---|---|
| 禁止简化交付 | P4～P9分别区分fixture、小样本、full candidate、独立审核、包认证和activation；F-010/F-016～F-018 | pass_design_only；任何子集不得声明完整升级 |
| 禁止静默错误 | D-002独立oracle、P5/P6 typed failure、P7全量闭环与provider冲突、F-008/F-011/F-012 | pass_design_only；无默认旧key/provider/DB fallback |
| 禁止改变业务逻辑 | D-001唯一authority、252td/退市/ST as-of、TDX-first、12指数/121列、历史release保留 | pass_design_only；业务语义均引用上位设计和冻结契约 |
| 禁止私增门禁 | §5.2声明文件租约仅为并发协调；§5.3普通月更只需一次candidate-only提交；§14仅保留动作级生产授权 | pass_design_only；短租约不是人工审批，首次升级验证不是每月新增门禁 |

以上只证明本设计满足规范；实现窗口仍须在各自最终HEAD重新执行同四项检查并提供直接代码/测试/运行证据。

## 16. Review History

| 轮次 | 审核范围 | 发现 | 修订 | 状态 |
|---|---|---|---|---|
| Draft-0 | 主设计初稿 | F2解析器未识别标准章节/编号/矩阵字段 | 标准化章节、F-xxx和矩阵列 | resolved |
| Review-1A | 架构与消费者 | registry历史不闭合、跨系统部分切换、bundle未成契约、W8后PR使证据失效；HMM-risk/持久profile/包双读/inference scope遗漏 | 改为versions+pointer+events；inactive分发后DB CAS；candidate bundle+activation envelope；取消W8后源码PR；补齐消费者scope | resolved |
| Review-1B | 数据、资源与保留 | 阈值方向/性能gate错误；首次20260731计划不可执行；oracle过度抽样；selective-clean-full与长期lineage缺验收；retention引用证明不闭合 | 明确max/min/exact与两类性能gate；新增白名单initial-migration plan；机器oracle矩阵；F-025/F-026；引用图schema且本项目禁止历史迁移/删除 | resolved |
| Review-2A | 架构限定复审 | bundle/distribution/W8循环依赖；归档v1与旧side-effecting session冲突 | 拆candidate bundle/envelope；新增admission close、lease inventory和`SESSION_PINNED_DRAINING`，side-effect session在CAS前归零 | resolved |
| Review-2B | 数据/资源限定复审 | WSL 12/6 GiB reserve遗漏；首次cutoff语义矛盾；outer schema与lineage schema混淆 | 补WSL reserve/readback；首次固定2026-07-31；区分outer v1/v2与lineage legacy/v3 | resolved |
| Review-3A | 架构最终复审 | 原Review-2A两项逐项复核 | PASS，无新P0/P1 | pass |
| Review-3B | 数据/资源最终复审 | 原Review-2B三项逐项复核 | PASS，无新P0/P1 | pass |
| Review-4A | W3并发与验收首轮 | F-030证据不可验证；W3-A strict v2与v1 reproduction职责冲突；W6仍依赖旧`W3_merged`；dirty handoff语义不严 | 增加跨域测试/精确命令；W3-A只处理正式v2、domain reader处理reproduction；改为W3-D gate；目标文件必须clean/committed | resolved |
| Review-4B | W3范围与状态复审 | 当前设计修订自身scope未登记；W3-D证据未明确绑定最终main；DEV DDL状态在本轮未重验 | 收据登记本分支两文件精确scope；增加最终main命令/结构化receipt/CI绑定；DEV状态改为本轮未重验 | resolved |
| Review-4C | 最终合入就绪复审 | 复核分片职责、文件租约/BUG抢占、实验固定身份、enablement/activation边界、最终main证据和两文件变更范围 | F2 30/30且0 warning；guardrail 0 finding；ownership 2/2；catalog integrity 7 passed；无新P0/P1 | pass_pending_user_confirmation |
| Review-5A | W3-A实现首轮 | Python `str+Enum`转换误拒合法usage；顶层新增测试未映射到CI定向域 | 枚举实例直接返回；测试迁入`backend/tests/dataset_release/`并同步设计/scope，不修改共享CI规则 | resolved |
| Review-5B | W3-A异常与边界复审 | 宽泛`ValueError`包装可能掩盖非契约错误；非Mapping输入缺直接证据 | 只包装`CanonicalPitContractError/PitSnapshotError`；补充typed-failure测试 | resolved |
| Review-5C | W3-A最终合入就绪复审 | 复核W1/W2 API复用、CAS digest、sample/v1/tamper拒绝、DB隔离和精确scope | direct/adjacent 26 passed；data-sync 161 passed；Qlib 15 passed；catalog 7 passed；F2 30/30；guardrail 0 finding；Ruff PASS | pass_pending_pr |
| Review-6A | W3-A嵌套身份完整性复审 | 外层浅拷贝允许嵌套manifest在digest计算后、W2解析前被调用方修改 | 只序列化一次并校验CAS digest；W1/W2从已验证字节反序列化的独立快照读取；增加TOCTOU回归测试 | resolved |
| Review-6B | W3-A真实CAS兼容复审 | 原digest测试与实现共用辅助函数，不能独立证明Control CAS引用兼容 | 使用真实`ControlStore`和`CASStore.put_json()`生成引用并通过正式adapter验证 | resolved |
| Review-6C | W3-A最终HEAD合入复审 | 合并最新main后逐项复核实现、测试、scope、PR设计链接和DESIGN-COMPLIANCE-001四项 | direct/adjacent 28 passed；data-sync 161 passed；Qlib 15 passed；catalog 7 passed；F2 30/30；guardrail 0 finding；ownership 4/4；无P0/P1/P2 | pass_ready_for_merge |
| Review-6D | PR机器可审计性复审 | 人类可读的`Exact write scope`/验收编号未匹配PR Quality固定元数据语法 | PR正文使用`Design Acceptance Matrix`和`Allowed write scope`，并列出四个精确文件及三项production gate | resolved_pending_ci_readback |
| Review-6E | 最新main漂移复审 | CI期间main前进至`458199cd`，新增Advisory P0-D源码/测试/设计和ownership登记 | 文件交集为零；无冲突合并；按最终main重跑28+161+15+7、F2、guardrail和ownership | pass_ready_for_merge |
