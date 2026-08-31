# AIstock 唯一权威股票 PIT F2 详细设计

## 1. Background / 背景

AIstock 当前同时存在滚动 live PIT key、QE 数据集冻结 key 和旧静态导出过滤。虽然各路径均试图排除
ST 或退市风险，但它们没有共同的现行规则身份，历史退市股票还会因按截止日筛选 `list_status=L`
而从整个回测窗口消失。2026-07-31 候选审计进一步证明，日线、分钟和因子曾把数据首日误作准入
首日，并在部分 ST 事件边界上产生跨组件差异。

用户已批准建立一个持续演进的唯一权威 PIT 股票池。QE 回测、模型训练、选股中心、Paper v2 与
simulation runtime 必须使用同一规则；数据集内的冻结 snapshot 和实时滚动视图只是同一规则的不同
物化，不构成两个可选择的股票池。

## 2. Scope / 范围

### 2.1 Goals

- 建立唯一逻辑 authority `aistock_equity_pit_canonical`，任一时点只允许一个
  `ACTIVE_CANONICAL` 规则版本。
- 将沪深 A 股历史上市生命周期、ST 风险、终止上市风险、数据可用性和订单可执行性分层表达，
  但对所有选股消费者只暴露一个 `eligible_for_new_position` 结论。
- 历史退市股票不再因当前状态为 `D/P` 从历史窗口整体消失。
- IPO 暖机统一为上市后的第 252 个交易所交易日；模型额外观测要求不得创建第二股票池。
- QE、训练、选股与模拟盘强制校验相同 `rule_version`；冻结数据集还必须绑定
  `universe_digest`、cutoff 和 source identity。
- 被正式实验、训练、审计或生产引用的历史数据集完整、不可变留存；`all.txt`、manifest、PIT
  snapshot、source digest 和 receipt 是必需索引和证据，但不能替代完整数据集。
- 复用现有 `market.stock_universe_pit_*`、dataset release、selection risk policy 和 event signal
  平台，不建立第二套数据管线。

### 2.2 Non-Goals / 边界

- 本 feature 不覆盖、原地追加或删除任何现有生产数据集或 2026-07-31 候选。
- 本 feature 不自动执行真实数据导出、生产 DDL/DML、生产 PIT rebuild、生产 pointer 切换、服务
  启停或重启。
- 不把数据缺失、停牌、涨跌停或无报价解释为证券不属于历史 PIT 生命周期。
- 不允许普通 QE、训练、选股或模拟任务选择 `ARCHIVED_NONCANONICAL` 规则。
- 不保证在没有源证据时自动推断终止上市公告或 ST 撤销；缺少闭环必须 fail closed 并输出 typed
  exception ledger。

## 3. Architecture / 架构

### 3.1 Single logical authority

唯一 authority 由以下 identity 组成：

```text
authority_id      = aistock_equity_pit_canonical
authority_status  = ACTIVE_CANONICAL
rule_version      = shsz_a_252td_st_delist_asof_v2
rolling_key       = aistock_equity_pit_canonical_v2
snapshot_digest   = sha256(canonical clipped spans + rule parameters + source identity)
```

迁移期间，旧 `shsz_st_pit_active_v1` 标记为 `DEPLOYED_LEGACY_PENDING_MIGRATION`，只维持既有
production 与 2026-07-31 release 的复现/只读重验；它不是未来权威。只有 v2 candidate 完整验证并完成独立
activation 后，旧 key 和旧 QE dataset keys 才转为 `ARCHIVED_NONCANONICAL`。归档后只允许显式
reproduction；普通新任务即使手工传入旧 key 也必须被拒绝。

### 3.2 Rolling view and frozen snapshot

滚动视图服务实时选股、Paper v2 和 simulation；冻结 snapshot 服务 QE 回测和训练。二者必须具有
相同 `authority_id/rule_version/rule_parameters_digest`：

- rolling view：覆盖到当前决策日，可增量重建；
- frozen snapshot：从 rolling authority 在指定 cutoff 只读冻结，不可变；
- snapshot key/path 可以不同，但不得拥有独立业务规则；
- dataset snapshot 的 spans digest 必须等于同 cutoff rolling canonical spans 的 canonical digest；
- 无法证明等价时，候选不得签署，实时消费者不得退回旧规则。

### 3.3 State layers

同一 authority 保存或派生以下层级：

1. `security_lifecycle`：list date 至最后实际上市/可交易生命周期；
2. `regulatory_risk`：NORMAL、ST、*ST、退市风险、退市整理、终止上市；
3. `knowledge_asof`：公告时间、公告日、实施日和可用于决策的首个时点；
4. `research_eligibility`：252 交易日暖机、ST/终止风险禁买；
5. `execution_tradability`：停牌、报价、涨跌停及交易时段，仅影响订单执行。

对选股只输出一个 `eligible_for_new_position`。已有持仓退出通过 `holding_must_exit` 形成意图，必须等
`orderable_now=true` 后执行，不允许从组合中直接删除。

### 3.4 Source hierarchy

1. 上交所/深交所公告和实施决定；
2. 本地规范化 `market.anns/event_signal` 的公告时间和终止上市事件；
3. Tushare `stock_st` 每日快照和 `st` 公告/实施事件；
4. Tushare `bak_basic/stock_basic/namechange` 历史成员和名称；
5. Tushare/TDX 行情、复权和停复牌作为数据与执行交叉验证。

冲突不静默降级。权威来源冲突、ST 空洞无法由前后状态加事件闭合、终止上市事件缺少证据时生成
exception ledger 并阻止候选 signoff。

## 4. Contracts / API, DB and artifact contracts

### 4.1 Canonical rule contract

- 证券范围：沪深 A 股，排除 B 股、北交所、基金、指数及其他非股票证券；
- PIT 起点：数据集/任务窗口起点；
- IPO：`list_date` 后第 252 个 `market.trading_calendar.is_trading=true` 的交易日进入；
- ST 风险生效：以 `imp_date` 为监管状态日，以精确公告时间或保守的下一决策时点表达 knowledge；
- ST 恢复：不得早于公告已知时间和 `imp_date`，且需后续快照或正式事件确认；
- 终止风险：`stock_delisting_risk_warning/confirmed` 的有效决策日开始禁买；
- 生命周期终止：不晚于 `delist_date`，最后交易日和停牌由行情/停复牌验证；
- 任何缺行情日期均不缩短 lifecycle 或 research span。

### 4.2 Missing ST day reconstruction

缺失交易日 `D` 的 ST 状态按以下确定性状态机重建：

1. 取前一完整交易日状态；
2. 按 `imp_date=D` 应用全部 ST/恢复事件；
3. 与下一完整交易日快照核对；
4. 不一致时查 `namechange` 与 `market.anns/event_signal`；
5. 仍不闭合则 fail closed，不做前向填充猜测。

### 4.3 Delisted-stock acquisition

- 日线优先使用 canonical Tushare raw/pro_bar 等价值接口；单一端点空结果不是“无交易”的证明；
- 分钟线继续 TDX 优先、Tushare `stk_mins` 只补 missing key；provider 重叠值冲突失败；
- `adj_factor`、`daily_basic`、moneyflow、suspend、ST 和 event 必须按所需字段给出 coverage receipt；
- 长期停牌且无成交的股票保留生命周期和风险状态，但不伪造 OHLCV；
- provider 返回 241 根等非标准分钟结果必须按既有交易时段规范化，并通过 240 根和日线聚合 parity。

### 4.4 Consumer contract

QE、HMM/其他模型训练、selection、Paper v2 和 simulation runtime 均调用共享 canonical contract
validator。普通模式必须满足：

```text
authority_id == aistock_equity_pit_canonical
authority_status == ACTIVE_CANONICAL
rule_version == shsz_a_252td_st_delist_asof_v2
```

冻结数据集还必须满足 snapshot digest、cutoff、calendar digest 和 rolling-at-cutoff digest 等价。只有
显式 `reproduction_mode=true` 且绑定历史 dataset release identity 时，才能读取归档规则；该模式不得
产生实盘、模拟盘或新的正式训练结果。

### 4.5 Historical retention contract

- `experiment_referenced=true`、`training_referenced=true`、`production_activated=true` 或
  `audit_hold=true` 的 release：完整数据集永久/按治理期不可变保留；
- 不额外复制已有完整旧生产数据集，只在 catalog 中登记其 immutable path、artifact root 和 retention
  reason；
- `all.txt`、PIT snapshot、manifest、source/artifact digest、validation/attestation receipt 必须保留；
- 只有从未发布、从未引用的失败/临时候选才可成为清理候选；自动流程不得删除，精确清理仍需独立
  用户授权；
- 若未来采用 CAS 去重，只有在完整 artifact root 可无损重建且引用计数安全时才可回收物理重复块。

## 5. Design Acceptance Index / 设计验收索引

| ID | Acceptance |
|---|---|
| F-001 | 代码中只有一个 ACTIVE_CANONICAL authority；旧 v1 只能显式 reproduction。 |
| F-002 | rolling 与 frozen snapshot 共享 rule identity，snapshot 必须证明 cutoff digest 等价。 |
| F-003 | 252 交易日 IPO 暖机由交易日历计算，不使用 365 自然日或首条行情日。 |
| F-004 | 历史 D/P 股票按生命周期进入历史 PIT，不发生当前状态幸存者过滤。 |
| F-005 | ST 状态由 daily snapshot、event pub/imp、名称/公告交叉验证；缺失日不静默填充。 |
| F-006 | 终止上市和吸收合并使用 as-of announcement/event；未闭环股票进入 exception ledger。 |
| F-007 | 数据可用性和 execution tradability 不改变 research lifecycle spans。 |
| F-008 | 退市股日线/分钟/复权多源补齐 missing-only，空结果和冲突 fail closed。 |
| F-009 | QE、训练、selection、Paper v2、simulation 均拒绝非 canonical 普通配置。 |
| F-010 | 月度候选冻结同一 canonical snapshot，并强制跨组件 PIT digest 完全一致。 |
| F-011 | 已引用历史数据集完整保留；`all.txt` 不得冒充可复现数据集。 |
| F-012 | 失败/无引用候选仅登记为清理候选，自动流程不删除任何数据。 |
| F-013 | 小样本和真实构建均受现有内存、并发、磁盘门禁约束，不回归全量 frames 聚合。 |
| F-014 | 生产 DDL/DML、真实导出、activation、restart、cleanup 保持独立 gate。 |

## 6. Implementation Plan / 实施方案

1. 新增纯代码 canonical authority contract 和归档/reproduction 校验。
2. 扩展 PIT builder：252 交易日暖机、历史 D/P 生命周期、缺失 ST 日状态机、终止上市事件和 exception
   ledger；旧 v1 builder 保留只读复现能力。
3. 保持 `qe_hmm_full_v1` 语义 digest 不变，新增严格 overlay `qe_hmm_full_v2`；扩展 dataset release
   profile/PIT snapshot/validator 和 retention status contract。
4. 在 candidate build 发布前统一绑定 canonical validator；QE、训练、selection、Paper v2、simulation 的
   production 默认切换留在独立 activation gate，源码合入不提前改变运行时行为。
5. 更新月度配置、skill 和 runbook，明确完整历史数据集保留与 candidate-only 边界。
6. 使用极少量 synthetic/fixture 和少量退市股票探针验证；本 PR 不运行真实全量导出。

## 7. Verification Plan / 验证方案

- PIT builder unit tests：252 交易日、历史退市、ST 空洞、公告 as-of、停牌不截短。
- canonical authority tests：普通消费者拒绝 v1，显式 reproduction 只允许不可变 release。
- dataset release tests：rolling/frozen digest parity、跨组件 instruments parity、retention 分类。
- selection/Paper/simulation/QE direct contract tests。
- 3～5 只股票、数个交易日的 fixture 构建；不写生产 DB、不导出真实全量数据。
- `python scripts/aistock_feature_workflow.py validate --design <this-file> --tier F2`。
- changed-file compile/lint、targeted pytest、`git diff --check` 和 DESIGN-COMPLIANCE-001 四项检查。

## 8. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `backend/services/canonical_equity_pit.py` | `backend/tests/test_canonical_equity_pit.py` | implemented_local_verified | none |
| F-002 | `backend/services/dataset_release/pit.py`; `source_authority.py`; `build_processor.py` | `backend/tests/dataset_release/test_pit.py` | implemented_local_verified | none |
| F-003 | `scripts/build_stock_universe_pit_spans.py` | `backend/tests/test_stock_universe_pit_spans.py` | implemented_local_verified | none |
| F-004 | `scripts/build_stock_universe_pit_spans.py` historical D/P scope | `backend/tests/test_stock_universe_pit_spans.py` historical D fixture | implemented_local_verified | none |
| F-005 | `audit_st_snapshot_continuity`; `_audit_canonical_st_snapshots` | `backend/tests/test_stock_universe_pit_spans.py` interior/boundary gap cases | implemented_local_verified | none |
| F-006 | `_load_confirmed_delisting_events`; `audit_canonical_terminal_evidence` | `backend/tests/test_stock_universe_pit_spans.py` query/status/missing evidence cases | implemented_local_verified | none |
| F-007 | `CANONICAL_RULE_PARAMETERS`; `PitConsumerBinding` | `backend/tests/test_canonical_equity_pit.py` | implemented_local_verified | none |
| F-008 | `artifact_ready_source.py` daily pro_bar/adj/minute overlays; `artifact_ready_build_source.py` streaming merge | `backend/tests/dataset_release/test_artifact_ready_source.py`; `test_artifact_ready_build_source.py` | implemented_local_verified | none |
| F-009 | shared validator; v2 profile/API/Worker allowlist | `backend/tests/test_canonical_equity_pit.py`; `backend/tests/scripts/test_update_backtest_dataset_monthly.py` | implemented_source_ready | none |
| F-010 | v2 overlay profile + frozen/build binding | `backend/tests/dataset_release/test_profile.py`; `test_pit.py`; `test_build_processor.py` | implemented_local_verified | none |
| F-011 | `dataset_release/retention.py`; control status; skill/runbook | `backend/tests/dataset_release/test_retention.py`; CLI status tests | implemented_local_verified | none |
| F-012 | `RetentionDecision.automatic_deletion_allowed=false` | `backend/tests/dataset_release/test_retention.py` | implemented_local_verified | none |
| F-013 | resource supervisor; bounded D/P-only daily overlay; date-only ST audit | `backend/tests/dataset_release/test_synthetic_benchmark.py`; full `backend/tests/dataset_release` run | implemented_local_verified | none |
| F-014 | v1/v2 profile split; CLI/runbook/production gates | `backend/tests/test_canonical_equity_pit.py`; `python scripts/aistock_feature_workflow.py validate --design docs/architecture/unified_canonical_equity_pit_f2_design_20260812.md --tier F2` | implemented_local_verified | none |

## 9. Rollout / Rollback

1. source merge：只交付代码、fixture 和文档；v1 semantic digest 保持不变，canonical v2 尚未生产激活。
2. DEV：只在既有 DEV DB 验证 migration/build/readback；生产仍使用旧 key。
3. candidate：在新目录生成 canonical v2 小样本，再生成完整 immutable candidate；旧数据不改写。
4. acceptance：完成退市股、ST 空洞、跨组件、QE/训练/selection/simulation 验收。
5. production：经用户对具体 migration、candidate 和 activation 明确授权后，原子切换
   `ACTIVE_CANONICAL`；旧 v1 同时转 `ARCHIVED_NONCANONICAL`。
6. rollback：只回滚 active pointer/配置到最后一个已验证完整 release；不删除 v2 或历史数据。若回滚到
   v1，只允许紧急恢复并显式标记 survivorship limitation，不把它重新声明为长期权威。

## 10. Risks / Failure Modes

| Risk | Control |
|---|---|
| 把 rolling 与 snapshot 当成两个规则 | 强制相同 authority/rule/parameter digest 和 cutoff parity |
| 退市股历史行情端点空返回 | 多端点 missing-only probe；空结果不是成功；冲突 fail closed |
| ST 快照源端空洞 | 前后 snapshot + imp_date event 状态机；无法闭合则阻止签署 |
| 公告分类漏识别终止上市 | exception ledger + `market.anns/event_signal` 定向复核 |
| 旧消费者仍接受 v1 | 共享 validator + changed-file consumer inventory test |
| 历史数据占用空间 | 不重复复制；完整引用数据保留；CAS 安全去重另行设计和授权 |
| v2 尚未生成却提前切 runtime | source merge 与 activation 分离；不存在/非 ready 状态 fail closed |
| 大批退市分钟补齐再次占满内存 | 单股/日期分块、落盘、硬内存和并发上限、TDX/Tushare missing-only |

## 11. Production Gates / 生产门禁

- `production_ddl_gate=pending_separate_authorization`
- `production_dml_gate=pending_separate_authorization`
- `real_dataset_export=not_run_not_authorized_by_this_design_step`
- `production_activation=not_requested`
- `backend_restart_owner=user`
- `cleanup=not_requested`
- `historical_dataset_mutation=forbidden`

## 12. DESIGN-COMPLIANCE-001

1. 不以旧 v1 改名、只补 `all.txt` 或少量股票的子集冒充完整统一股票池。
2. provider 空结果、ST 空洞、公告缺口、跨组件 digest 冲突均显式失败，不静默降级。
3. 不改变已批准的唯一 authority、252 交易日、退市股历史 PIT 和完整历史数据集留存语义。
4. 不新增设计外人工门禁；生产 migration、activation、restart 和 cleanup 是既有独立安全边界。
