# QE / PIT v2 数据集升级与月度更新主计划 F2 详细设计

## 0. 文档身份

- 日期：2026-08-13
- 等级：F2（跨数据管线、QE/HMM、Selection、Paper/Simulation、StrategyPackage/Advisory 与生产激活）
- 状态：设计审核中；实现、真实数据构建和生产激活均未授权
- 上位业务设计：`docs/architecture/unified_canonical_equity_pit_f2_design_20260812.md`
- 月更底座设计：`docs/architecture/qe_monthly_dataset_release_productization_f2_design_20260811.md`
- 运维入口：`docs/operations/qe_backtest_dataset_monthly_update_runbook.md`
- 目标 authority：`aistock_equity_pit_canonical`
- 目标规则：`shsz_a_252td_st_delist_asof_v2`
- 目标 rolling key：`aistock_equity_pit_canonical_v2`

本文是后续实施、候选构建、跨模块验收和生产切换的主控计划，不替代上述业务规则和月更底层技术设计，
也不形成第二套数据管线或第二个股票池标准。若本文与上位业务设计冲突，必须停止实施并先修订设计。

## 1. 目标与不可突破边界

### 1.1 目标

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

### 1.2 明确禁止

- 不覆盖、原地追加、移动或删除 2026-06-30、2026-07-31 生产/候选数据集。
- 不把 `all.txt`、manifest、digest 或少量样本冒充完整历史数据集。
- 不执行全局字符串替换把所有 `shsz_st_pit_active_v1` 改为 v2；历史 reproduction identity 必须保留。
- 不以数据缺失、停牌、涨跌停、无分钟成交或 provider 空结果缩短证券历史生命周期。
- 不把分批查询后仍保留全部 `frames`、全市场预分配矩阵或无界日志重新带回构建路径。
- 不通过扩大并发、降低主机内存保留、启用 WSL swap 或减少股票/日期/字段/指数来换取成功。
- 源码合入不自动授权真实数据导出、DEV/生产 DDL/DML、node1 分发、生产 activation、后端/Worker 重启或 cleanup。
- 真实候选构建开始后，构建窗口不得同时修改其所执行的 profile、planner、materializer、validator 或 Skill。
- 任一窗口不得进入、修改、清理或控制其他窗口的 worktree、任务或进程。

## 2. 当前基线与必须关闭的缺口

### 2.1 已有可复用能力

- `backend/services/canonical_equity_pit.py` 已定义 v2 authority、rule parameters digest、普通消费者约束和
  legacy reproduction 边界。
- `backend/services/dataset_release/` 已提供 durable control catalog、candidate-only Worker、lease/fence、CAS、
  COW、增量/选择性重建、资源门禁、TDX-first 分钟补齐、PIT snapshot、12指数/HMM、候选校验和签收。
- `configs/datasets/qe_backtest_monthly_v2.yaml` 已冻结 v2 月更语义与资源上限。
- `backend/services/dataset_release/retention.py` 已规定已发布或被实验/训练/生产/审计引用的数据集必须完整保留，
  自动删除永远不允许。

### 2.2 当前未关闭缺口

1. `stock_universe_pit_service.py`、Selection runtime profile、QE dataset contract、StrategyPackage/Advisory 等仍存在
   v1 默认值或 SQL 硬编码。
2. 当前只有目标常量和候选profile，没有一个能被所有在线消费者共同读取、并可用CAS原子切换的 production
   authority registry。
3. QE/HMM frozen dataset identity 与在线 rolling authority 尚未形成统一、机器可校验的 activation bundle。
4. 旧 StrategyPackage/模型是否只需重新认证还是必须重训，尚未按“截面依赖/时序独立”完成分类。
5. v2 真实小样本、全量candidate、跨组件digest和消费者shadow evidence尚未形成最终release receipt。
6. 2026-06-30、2026-07-31及失败候选的引用/重复副本/冷存储状态尚未形成可执行的retention inventory；
   磁盘紧张不能据此直接删除历史release。

## 3. 核心决策

### D-001 单一逻辑权威

任一时点只允许一个 `ACTIVE_CANONICAL`。v1在迁移期为
`DEPLOYED_LEGACY_PENDING_MIGRATION`，激活后为`ARCHIVED_NONCANONICAL`，仅可由显式、不可变、只读
`reproduction`使用。

### D-002 中央 authority registry，而不是模块常量投票

新增一个现有 PIT 管线内的singleton registry（建议表名
`market.stock_universe_pit_authority`，最终DDL以DEV验证后的migration为准），至少保存：

```text
authority_id                     primary key
authority_status                 DEPLOYED_LEGACY_PENDING_MIGRATION | ACTIVE_CANONICAL
active_rule_version
rolling_universe_key
rule_parameters_digest
activation_generation            monotonic bigint
candidate_release_id
candidate_receipt_sha256
source_commit
activated_at
updated_at
```

在线消费者只通过共享 `CanonicalPitAuthorityResolver` 读取该registry并校验对应
`stock_universe_pit_state/spans`；不得各自保存生产默认key。切换使用
`expected_previous_generation + expected_previous_key` 的单事务CAS，影响行数必须为1。

QE/HMM计算节点不依赖在线DB：它们读取不可变dataset manifest中的同一authority/rule/parameter digest、
frozen snapshot digest、cutoff和release identity。activation bundle同时绑定rolling registry目标和frozen release，
但不把动态rolling内容digest写死在源码中。

### D-003 两阶段源码交付

1. **Enablement**：所有模块支持registry/v2/shadow，但生产仍解析到v1 pending-migration；不存在提前切换。
2. **Activation**：完整候选验收后，仅以签名candidate identity、registry CAS migration/DML和最小配置commit
   完成切换。源码支持与生产数据激活不得混为一次不可回滚动作。

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

## 4. 允许的现有API与禁止绕行

### 4.1 允许复用

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

### 4.2 计划新增的最小API

- `CanonicalPitAuthorityResolver.resolve_live_binding()`：读取singleton registry和对应ready state，返回完整binding。
- `CanonicalPitAuthorityResolver.require_activation_target(...)`：对candidate receipt、rule/digest和CAS预期状态做只读preflight。
- `activate_canonical_pit_authority(...)`：只供精确migration/operator入口调用；单事务CAS，不暴露通用API。
- `DatasetPitBinding.from_release_manifest(...)`：从不可变release生成QE/训练binding，不查询在线DB。
- `require_strategy_package_pit_compatibility(...)`：区分普通运行、v2重新认证和历史reproduction。

名称可在实现审核中微调，但不得把这些职责拆回模块内硬编码或新增第二套PIT服务。

### 4.3 禁止绕行

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
| W3 QE/HMM/Training | frozen release binding、正式实验/训练/因子缓存/HMM输入迁移 | `quantevolver/**`、`hmm_*`相关精确scope、直接测试 | QE/HMM拒绝非canonical普通配置 | 不修改rolling在线消费者 |
| W4 Selection/Paper/Simulation | rolling resolver、选股/持仓退出/模拟盘兼容 | `selection_center/**`、`paper_trading_v2/**`、`simulation_runtime/**`精确scope、直接测试 | 在线消费者统一binding和shadow能力 | 不改变订单可执行性业务语义 |
| W5 StrategyPackage/Advisory | package manifest兼容、旧包分类、Advisory查询契约迁移 | `strategy_package/**`、`advisory_*`精确scope、直接测试 | v2包重新认证/重训决策和reproduction隔离 | 不改写已发布旧manifest或模型资产 |
| W6 Integration | 汇总最终main、消费者库存扫描、跨模块fixture、activation bundle builder | 只写集成测试、inventory guard和必要胶水；公共核心文件需W1交接 | final source-ready commit和结构化receipt | 不运行生产或全量candidate |
| W7 Candidate Build | 小样本后执行真实v2 candidate-only任务，修复数据缺口 | 只写新candidate root、control catalog、candidate-local CAS/overlay和receipt | 完整v2 candidate | 不改源码、不写生产DB、不覆盖历史candidate |
| W8 Independent Validation | 只读审计candidate、旧数据对照、消费者shadow、性能/资源证据 | 仅validation receipt/允许的审计输出目录 | 独立PASS/FAIL与缺口清单 | 不修数据、不改candidate、不激活 |
| W9 Activation/Aftercare | DEV→生产migration、CAS切换、分发、用户重启后只读核验、精确cleanup计划 | 精确migration/activation receipt；运行时动作按独立授权 | active v2与回滚证据 | 不推导重启、删除或cleanup授权 |

### 5.1 并行与串行规则

- W1和W2先串行合入：W2必须以W1最终公共binding为输入。
- W3、W4、W5可在W1/W2合入后并行，但必须拥有互斥文件scope；任何共同文件转交W6单写。
- W6只在W3/W4/W5均合入或明确记录阻断后开始。
- W7绑定W6最终commit、profile digest和toolchain SHA；W7运行期间W1-W6不得变更构建相关源码。
- W8可以只读观察W7，但正式签收必须绑定W7 terminal receipt和不可变candidate identity。
- W9必须在W8 PASS、用户对具体migration/candidate/activation授权后开始。

## 6. 整体阶段、入口与退出门禁

### P0 基线冻结与消费者库存（W0）

任务：

1. 记录最终`origin/main`、profile digest、toolchain SHA、现有production dataset identity和生产runtime identity。
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
- 证明component planner不会把PIT变化错误降级为只更新`all.txt`。
- 证明日线/分钟/因子materializer持续使用流式分块、COW、单股/日期缺口补齐。
- 月更Skill/Runbook只暴露一次candidate提交、status、receipt和re-attest，不暴露activation。

退出门禁：两批PR均通过直接测试、F2设计矩阵、CI；生产runtime仍为v1 pending-migration。

### P3 消费者迁移（W3/W4/W5并行）

#### P3-A QE/HMM/训练（W3）

- QE dataset contract从release manifest获得authority/rule/digest/cutoff，不再接受在线rolling key代替frozen snapshot。
- 新正式实验、因子批算、HMM训练/预测均保存dataset release identity；普通任务拒绝v1。
- 历史任务只有显式reproduction可使用旧release。
- 对模型分类：截面rank/neutralization/label依赖者标记`retrain_required`；纯单股时序且PIT仅做外层过滤者
  可标记`revalidate_then_republish`，但不得直接沿用生产资格。

#### P3-B Selection/Paper/Simulation（W4）

- Selection risk policy通过resolver获取rolling binding，不接受请求任意key。
- Paper/Simulation继承同一Selection binding，不形成独立配置。
- PIT只决定`eligible_for_new_position/holding_must_exit`；停牌、涨跌停、时段和报价仍由`orderable_now`决定。
- 增加shadow mode：同一trade date只读计算v1/v2差异和原因，不提交订单、不写正式荐股结果。

#### P3-C StrategyPackage/Advisory（W5）

- 新package manifest冻结authority、rule、parameter digest、release/cutoff/snapshot identity。
- 已发布v1 package完全不改写；未来继续运行者生成新v2版本并按模型分类重新认证或重训。
- Advisory historical range、catalog、input projection和feature source取消生产v1硬编码，统一从包/请求已验证binding读取。
- reproduction输出不能进入荐股、Paper、Simulation或新的正式训练。

退出门禁：每个窗口直接测试通过、无未分类生产v1引用、无运行时默认提前切到v2。

### P4 集成与小样本源码验收（W6）

1. 构建changed-file consumer inventory guard；新增消费者若绕过resolver则CI失败。
2. 使用3～5只证券和数个关键交易日fixture覆盖正常股、IPO第251/252日、ST/摘帽、退市/吸收合并、长期停牌。
3. 证明rolling/frozen identity、all.txt、daily/minute/factor、Selection/Paper/StrategyPackage在同cutoff一致。
4. 证明普通v1被拒绝、显式历史reproduction仍成功。
5. 运行最小本地门禁和委托跨模块验证，生成绑定最终commit的receipt。

退出门禁：source-ready，不等于真实数据ready；所有生产gate仍为pending/noop。

### P5 极小真实数据候选（W7）

范围建议：3～5只证券、10～20个交易日；至少包含一个IPO边界、一个ST事件、一个历史退市股。测试数据写入
新的临时candidate root，不写生产DB，不触碰6月30日/7月31日目录。

验收：

- PIT/公告/交易日历/退市生命周期逐事件人工可解释。
- TDX分钟优先、Tushare仅补missing key；重叠冲突、非240根、40203均fail closed。
- daily/minute聚合、复权、涨跌停和factor小样本数值一致。
- 峰值资源低于profile hard cap，swap为0，无其他进程控制。
- 失败后只修源码或candidate-local缺口；修复合入后必须以新commit重新执行小样本，旧receipt失效。

退出门禁：小样本terminal PASS并绑定最终source commit；否则不得开始全量。

### P6 完整v2 candidate-only构建（W7）

1. 提交一次durable monthly intent；不得并行启动第二个exporter。
2. planner逐组件记录`REUSE/INCREMENTAL/SELECTIVE_REBUILD/FULL_REBUILD`和理由。
3. 缺失优先按本地DB/TDX/Tushare authority补齐；无法补齐进入typed exception ledger并阻断签收。
4. 候选完整物化到独立X盘目录；即使大量组件复用，也必须形成完整可读artifact graph和新release manifest。
5. 构建过程中只读汇报control status/events/receipt，不扫描全目录、不把完整日志读入内存。

退出门禁：terminal `CANDIDATE_VALIDATED`、全部required validation PASS、catalog/marker/receipt一致。

### P7 独立数据审核（W8）

审核采用“全量结构/摘要门禁 + 分层数值采样”，禁止新旧数据逐行全量比较：

- 全量：文件图、schema、日期范围、股票代码集合、PIT spans digest、calendar digest、空值/重复/非有限值计数、
  跨组件instruments parity和manifest SHA。
- 采样：按年份、板块、ST/非ST、退市/存续、IPO边界、停牌和涨跌停分层；固定seed并保存样本清单。
- v1/v2股票池差异必须归因于252交易日、历史退市股、ST/终止上市as-of或明确修复；无法解释差异为FAIL。
- ST PIT逐事件抽核公告时间、实施日、下一可决策时点和恢复证据。
- 涨跌停、prev_close、factor/QFQ在新旧相同证券/日期样本上校验；差异必须有源revision解释。
- 预计算因子检查股票池、PIT mask、row-local复用、截面重算范围和静态121列契约。
- 申万L2检查稳定`l2_code_id`、PIT区间和历史同日一致性。
- 12指数和HMM benchmark检查代码集合、日期覆盖、单位和逐字段provider parity。
- QE/HMM、Selection、Paper/Simulation、StrategyPackage分别做候选消费者smoke，结果绑定同一authority/rule/digest。

退出门禁：独立审核receipt PASS；所有差异已分类；没有未关闭数据缺口或资源违规。

### P8 StrategyPackage/模型重新认证（W3/W5，使用W8候选）

- `retrain_required`模型必须在v2 frozen snapshot上形成新模型/包版本和独立验证。
- `revalidate_then_republish`模型必须完成v1/v2影子回测、样本覆盖和阈值稳定性验证，再发布v2兼容包。
- 旧v1包继续只读复现；不得删除、改manifest或让其驱动激活后的新荐股/模拟盘。

退出门禁：拟继续生产/模拟运行的全部包均有v2兼容状态；未迁移包从active admission中显式排除。

### P9 Activation与后续月更（W9）

执行顺序：

1. 用户确认具体candidate release、migration、生产target和activation。
2. DEV migration/apply/readback和回滚脚本验证PASS。
3. 确认不可变source merge commit已进入main，生产target preflight匹配。
4. 执行生产DDL（若需要）并readback；再执行singleton registry CAS DML。
5. 分发冻结dataset manifest和必要artifact到计算节点，逐节点校验digest。
6. 用户执行backend/Worker/相关runtime重启或加载动作。
7. 重启后只读核验runtime identity、registry generation、QE/Selection/Paper/StrategyPackage业务smoke。
8. v1转`ARCHIVED_NONCANONICAL`；仅reproduction可读。
9. 后续每月只提交一次`qe_hmm_full_v2 monthly --candidate-only`，同cutoff由fresh probe决定NO_OP/reattest/rebuild。

rollback只切回最后一个已验证完整release/registry generation，不删除v2数据。紧急回到v1时必须标注
survivorship limitation，不能重新声明为长期权威。

## 7. 数据组件任务与验收标准

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

## 8. 资源与性能验收

以`qe_backtest_monthly_v2.yaml`冻结值为最低安全边界；实施不得降低：

- heavy full concurrency = 1
- aggregate private commit cap = 12 GiB
- Windows job commit cap = 8 GiB
- WSL memory max = 8 GiB，swap max = 0
- host start available/headroom = 16 GiB
- host emergency available/headroom = 8 GiB
- DB row query concurrency = 1，provider concurrency = 1
- minute code batch最大20，H5 batch按100→50→20降档
- candidate free-space floor = max(32 GiB, 1.25 × predicted new bytes)

验收要求：

1. 同cache class合成benchmark至少3次，比较median rows/s、bytes/s、query count、read/write bytes和peak commit。
2. 真实全量按stage记录wait/compute/provider时间；资源等待不能误报为算法性能退化。
3. 与相同有效工作量基线相比归一化吞吐退化超过10%进入`WAITING_PERFORMANCE_REGRESSION`并分析，不能自动加并发。
4. 任一hard breach、swap使用、零进展30分钟或单SQL超过300秒，checkpoint后typed fail/wait。
5. 资源监管只能管理identity-bound当前task child，绝不停止其他窗口、QE实验或用户进程。

## 9. PR、审核与合入顺序

| PR | 窗口 | 内容 | 合入前条件 | 合入后仍不代表 |
|---|---|---|---|---|
| PR-0 | W0 | 本主设计 | 三轮设计审核+F2 validator | 允许实现/导出 |
| PR-1 | W1 | Core/registry/migration source | DEV migration fixture、contract tests | 生产DDL/DML |
| PR-2 | W2 | Dataset release v2 enablement | resource/materializer/validator matrix | 真实candidate完成 |
| PR-3 | W3 | QE/HMM/Training | frozen/reproduction tests | 模型已重训 |
| PR-4 | W4 | Selection/Paper/Simulation | rolling/shadow/orderability tests | 生产已切v2 |
| PR-5 | W5 | StrategyPackage/Advisory | immutable old package+compatibility tests | 旧包可直接运行v2 |
| PR-6 | W6 | 集成inventory/跨模块tests/Skill | 最终HEAD receipt、无unknown引用 | candidate或activation完成 |
| PR-7 | W9 | 最小activation source/config（如需要） | W8 PASS+用户授权 | 重启/cleanup已授权 |

每个PR先执行窗口直接测试和`git diff --check`，再由独立审核检查scope、业务语义、fail-closed、测试充分性和
DESIGN-COMPLIANCE-001。公共契约修改不得在多个PR中并行漂移；后续窗口必须从已合入main重新创建或同步。

## 10. 审核模型

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

## 11. Design Acceptance Index

| ID | 验收要求 |
|---|---|
| M-001 | v2为未来唯一ACTIVE_CANONICAL；v1仅迁移态和显式reproduction。 |
| M-002 | rolling/frozen共享authority/rule/parameters，cutoff snapshot digest可证明等价。 |
| M-003 | singleton registry以generation CAS原子切换，消费者不得猜latest。 |
| M-004 | QE/HMM计算面从不可变manifest读取binding，不回退在线DB。 |
| M-005 | 所有生产v1引用完成分类，unknown为零；历史identity不做全局替换。 |
| M-006 | 每个实施窗口有独立worktree、单写scope、入口/退出门禁和结构化receipt。 |
| M-007 | W3/W4/W5只在公共契约合入后并行，W7构建期间源码冻结。 |
| M-008 | v2 PIT覆盖252交易日、历史D/P、ST/终止as-of和exception ledger。 |
| M-009 | planner按依赖选择复用/增量/选择性/全重建，不默认全量重新导出。 |
| M-010 | PIT变化不会被错误降级为仅更新all.txt；完整candidate artifact graph可独立读取。 |
| M-011 | minute为TDX-first/Tushare missing-only，冲突/空结果/非240/40203 fail closed。 |
| M-012 | 复权、涨跌停、停牌、资金流、121静态列、申万ID、12指数/HMM有直接oracle。 |
| M-013 | QE/HMM/训练普通任务拒绝归档规则，正式结果绑定release identity。 |
| M-014 | Selection/Paper/Simulation共享rolling binding，PIT与orderable_now职责不混淆。 |
| M-015 | StrategyPackage旧manifest不改写；v2继续运行必须重新认证或重训。 |
| M-016 | 小样本失败修复后绑定新commit重跑，不能沿用旧receipt。 |
| M-017 | 全量候选只写新X盘root，不覆盖6月30日/7月31日或生产路径。 |
| M-018 | 审核使用全量结构/digest+分层数值采样，不执行新旧逐行全量比较。 |
| M-019 | 内存/并发/磁盘/swap硬边界和性能归一化门禁可执行且fail closed。 |
| M-020 | 已引用历史release保留一份完整副本；清理只针对证明无引用失败候选或重复副本。 |
| M-021 | 月更保持一次candidate-only提交，NO_OP/reattest/rebuild由fresh durable evidence决定。 |
| M-022 | source、candidate、DDL/DML、activation、distribution、restart和cleanup各自独立授权/回执。 |
| M-023 | activation CAS、计算节点digest、用户重启后smoke和rollback均绑定明确identity。 |
| M-024 | DESIGN-COMPLIANCE-001四项分别有直接证据，无简化、静默错误、业务漂移或私增门禁。 |

## 12. 设计验收矩阵

| design_item | 设计位置 | 实现/验证责任窗口 | 当前状态 | 实现前缺口 |
|---|---|---|---|---|
| M-001～M-005 | §3、§4 | W1/W3/W4/W5/W6 | designed_pending_review | registry与消费者迁移未实现 |
| M-006～M-007 | §5、§6 | W0/W6 | designed_pending_review | 用户确认后创建实现任务 |
| M-008～M-012 | §6、§7 | W1/W2/W7/W8 | designed_pending_review | 真实v2 candidate未构建 |
| M-013～M-015 | §6 P3/P8 | W3/W4/W5/W8 | designed_pending_review | 消费者和包分类未迁移 |
| M-016～M-019 | §6 P5～P7、§8 | W6/W7/W8 | designed_pending_review | 小样本/全量资源证据未运行 |
| M-020～M-021 | §3 D-005、§6 P0/P9 | W0/W2/W9 | designed_pending_review | retention inventory未生成 |
| M-022～M-023 | §6 P9、§9 | W9 | designed_pending_review | 所有生产gate未授权 |
| M-024 | §10、§14 | W0及每个实现窗口 | designed_pending_review | 等待三轮设计审核 |

设计通过只表示可以请求用户确认进入实施；不得把`designed`状态表述为源码、真实数据或生产完成。

## 13. 生产门禁状态

| 动作 | 当前状态 |
|---|---|
| 源码实现 | `not_started_pending_user_design_confirmation` |
| 真实小样本 | `not_run_not_authorized` |
| 真实全量candidate | `not_run_not_authorized` |
| DEV DDL/DML | `not_run_pending_implementation` |
| 生产DDL/DML | `pending_separate_targeted_authorization` |
| production activation | `not_requested` |
| node1/计算节点distribution | `not_requested` |
| backend/Worker/runtime restart | `owner=user_not_requested` |
| 历史数据修改/覆盖 | `forbidden` |
| cleanup/delete | `not_requested_exact_paths_required` |

## 14. DESIGN-COMPLIANCE-001

1. **禁止简化交付**：小样本、`all.txt`、fixture、source-ready或单个消费者通过均不能冒充完整v2升级。
2. **禁止静默错误**：source/PIT/provider/digest/consumer/资源冲突一律typed fail/wait，不回退旧key或减少范围。
3. **禁止改变业务逻辑**：唯一authority、252交易日、历史退市PIT、as-of、TDX-first、12指数、121静态列、
   完整历史release保留和candidate-only语义不得被各窗口自行修改。
4. **禁止私增门禁**：普通月更不增加人工审批；只保留既有技术签收和生产DDL/DML、activation、restart、cleanup
   独立授权边界。

## 15. Review History

| 轮次 | 审核范围 | 发现 | 修订 | 状态 |
|---|---|---|---|---|
| Draft-0 | 主设计初稿 | 待独立审核 | 待修订 | open |

