# Advisory N3 财务事件 Source Readiness F2 详细设计 v1.1

> 日期：2026-09-04  
> 状态：`IMPLEMENTED_LOCAL_SOURCE_SMOKE_READY_FORMAL_PENDING`  
> tier：`F2`  
> research stage：`N3_FINANCIAL_EVENT_SOURCE_READINESS`  
> objective contract：`ALPHA_RANKING`  
> study type：`ORACLE_DIAGNOSTIC`  
> decision use：`NAVIGATION_ONLY`  
> production gates：backend restart / DDL / DML / Tushare / network / backfill / factor catalog / StrategyPackage / runtime activation 均为 `noop`

## 1. 背景与当前事实

1. N3 融资融券正式 bundle `b50411d8...` 完成三个固定 trial、selected=0，且不可变 route 指向 `N3_FINANCIAL_EVENT_SOURCE_READINESS_DESIGN`。旧 proposal、overlay、腿间、分钟、generator 和 margin frontier 均已关闭，不得借事件源改名重跑。
2. 冻结父包输入为 N2-B `CURRENT_IC_PARENT`：`2024-07-04..2026-02-02` 共 386 个决策日、1,710,301 个 `decision date + instrument` 键、4,503 只股票。权威 parquet 大小 `119953459` bytes、SHA-256 `48598f1afe893c1718098f258a69cc579d831c5e4bea6d54b290c7ac0bd3b039`。
3. 仓库已有三类 raw 表、统一事件 adapter 和 date-only PIT 时钟，但这只能证明代码能力。DEV `aistock_dev` 五张相关表均为 0 行，不能提供训练样本；当前数据候选盘和模型 artifact 目录也没有财务事件冻结面板。
4. 严格只读、target-free 的设计探针确认主库当前有 forecast 70,561 行、express 14,299 行、fina indicator 314,059 行；开发窗口内统一事件有 27,883 条，全部为 `DATE_ONLY`。扩大到 252 个前置交易日后有 46,484 条，全部满足 `effective_trade_date = source_event_date 后首个交易日`。
5. 这些历史行最早于 2026-05 才被 AIstock 本地观察，晚于开发窗口大部分日期；因此它们不是逐日保存的 vintage。forecast 与 fina indicator 还存在同 business key 多版本。该事实不构成未来价格/收益字段泄漏，但可能包含公告后的源修订，只能支持探索性导航。
6. target-free 支持度探针表明：仅使用已有非中性 `event_signal`，最近 120 个交易日有事件的全候选/Top20/Top50 比例为 `64.31%/88.10%/83.68%`，386 日 Top20 支持数 min/median/max 为 `11/18/20`。这证明能够形成干预，不证明 Alpha、完整抓取或确认性 PIT。
7. v1.1 本地实现 smoke 从同一父包和 margin receipt 读取主库只读 snapshot，投影最早本地版本 84,272 行，其中 qualifying 44,953、neutral 39,319；120 日 Top20/Top50 disclosure 为 `100%/99.9948%`，qualifying 为 `87.8756%/83.4974%`，Top50 mixed qualifying 为 378/386 日。forecast/express/fina event-type 漂移为 `0/0/1.6528%`，全部低于预注册 2%；耗时 9.29 秒、最大采样 RSS 1.164GB、临时文件 7.11MB、8 次 SELECT、0 次数据库写、0 network/Tushare。bundle `46dfc443...` inspect 和同输入 exact retry 均通过；该 smoke 来自未合入源码且未 deliver registry/route，只证明实现与 source 可行，正式状态仍 pending。

## 2. Scope / 目标与终止条件

本阶段只回答：在不读取收益、标签或 sealed holdout、不联网和不回填的前提下，现有财务事件数据能否被原子投影为与 N2-B 父包键对齐、时钟因果、支持度充分且风险诚实标注的后续固定 learnability MVE 输入？

交付：

1. 冻结 source request、父包 identity、查询 schema、事件分类规则、允许字段和时间窗口。
2. 从只读 repeatable-read 事务生成 task-owned、content-addressed 事件 projection 和 target-free 支持度报告。
3. 区分 raw source absence、neutral disclosure、qualifying event 和 unavailable；禁止把没有 `event_signal` 行直接解释成“没有财务事件”。
4. 检查 date-only 时钟、source revision、分类漂移、事件类型 roster、Top20/50 干预支持和资源成本。
5. 只产生一个 typed terminal route，不训练模型、不读取经济结果。

终止状态：

- `SOURCE_READY_NAVIGATION_ONLY_NON_VINTAGE`：所有 schema/PIT/support/identity 条件通过；只允许进入新的财务事件固定 MVE 详细设计。
- `SOURCE_NOT_READY`：数据为空、PIT 不成立、支持不足、revision 分类漂移超限、身份漂移或读权限缺失；精确记录原因，不切换替代源。
- `INVALID`：代码、manifest、projection 或事务一致性失败；不发布 bundle，不改变 route。

## 3. 非目标与禁止项

- 不调用 Tushare，不执行历史回填、调度、DDL、DML 或数据库修复。
- 不读取 N2-B outcome/return/entry/exit/label 列；source readiness 全程 target-free。
- 不使用 sealed holdout，不把 2026 回填数据描述成 vintage、independent OOS 或 confirmation evidence。
- 不从旧事件研究的收益结果挑选 `loss`、`growth`、source、方向、lookback 或阈值。
- 不训练模型，不搜索窗口、事件子集或超参。
- 不写因子库、StrategyPackage、Selection、Advisory、Paper、QMT、仓位、订单或运行时 descriptor。
- 不建设通用事件平台、抓取器、缓存服务、UI、审批或自动演进循环。

## 4. 数据流与架构

```text
margin selected=0 receipt + frozen N2-B parent parquet
                         |
       parent key/score-only projection
                         |
production PostgreSQL REPEATABLE READ + READ ONLY
   | raw earliest-local-observation rows
   | trading calendar
   | existing event rule implementation
                         |
target-free classify + conservative next-trading-day clock
                         |
immutable event source projection + daily support report
                         |
PIT / revision / roster / resource / manifest readback
                         |
SOURCE_READY_NAVIGATION_ONLY_NON_VINTAGE
          or SOURCE_NOT_READY / INVALID
```

数据库只是一次只读源。正式后续 MVE 只允许消费已发布 projection，不得重新查询可变主库；这不是建立历史仓库，而是防止同一实验运行中数据身份漂移的最小必要冻结。

## 5. 输入、身份与只读事务契约

### 5.1 父包输入

- 文件必须是 N2-B bundle `bcdcb31d.../arm_signal_outcomes.parquet` 的上述 size/hash。
- 读取列白名单仅为 `arm_id/decision_as_of_trade_date/instrument/score`。
- `arm_id` 只能是 `CURRENT_IC_PARENT`；行数、日期数、股票数、唯一键和每天 score finite 性必须与 §1 一致。
- Parquet 出现 outcome、return、price、label 等列不构成读取授权；reader 必须在列投影层证明未加载它们。

### 5.2 数据库事务

- 连接凭据只从既有非秘密环境变量位置读取，artifact 不记录密码/token。
- 事务必须同时满足 `transaction_read_only=on` 与 `REPEATABLE READ`；连接后先 readback，任一不满足立即失败。
- 允许表只有 `market.trading_calendar`、三类 `market.tushare_*_raw`；现有 `market.event_signal/event_fact` 只作数量与时钟 parity 诊断，不作为 projection 的唯一来源。
- SQL 必须是固定 `SELECT`；query count、returned rows、bytes、elapsed 和数据库名称进入资源收据，host/user 不进入 artifact。
- 事务开始时冻结数据库 snapshot 标识和查询模板 hash；projection 完成前不得换连接或打开第二个数据库 snapshot。

### 5.3 Raw version 选择

每个 `source_record_key` 固定选择最早的本地观察版本：`ORDER BY first_seen_at ASC, raw_observation_id ASC`。这是防止 2026-05 之后同步修订进入本次 projection 的任务时点规则，不代表原公告日 vintage。

所有版本仍用于 target-free revision 诊断：用冻结 `financial_event_rules_v0_20260506` 对同 key 各版本分类，分别报告 raw 多版本率、event type 漂移率和 classification tuple 漂移率。不得根据“后来是否修订”删除单行或改变标签；漂移只决定整源是否可进入后续 MVE。

## 6. Source projection 与时间语义

### 6.1 固定 source roster

三类 source 全量进入 readiness，不按旧收益筛选：`tushare_forecast`、`tushare_express`、`tushare_fina_indicator`。

每个最早版本投影：`source_type/source_record_key/raw_observation_id/source_row_hash/instrument/source_event_date/report_period/event_family/event_type/should_signal/severity_score/confidence/effective_trade_date/source_time_quality/effective_rule`。不保存完整 `raw_payload`，但以 source row hash 和 projection manifest 绑定。

neutral disclosure 必须保留，`should_signal=false` 不等同 missing；无 raw row 只能标记 `NO_DISCLOSURE_OBSERVED_IN_FROZEN_SOURCE`，不能宣称公司没有披露或没有风险。

### 6.2 PIT 时钟

- source window 为冻结交易日历中首个决策日前 252 个交易日至最后决策日；设计事实对应 `2023-06-19..2026-02-02`。
- date-only raw 行只能从 `ann_date` 之后第一个交易日生效；同日即使盘前也不得生效。
- 每条 projection 必须满足 `source_event_date < effective_trade_date`，且 effective date 精确等于日历中的下一个交易日。
- 后续按决策日 T 只能读取 `effective_trade_date <= T` 的 projection 行。T+1、未来 ann_date、未来 report revision、outcome 或 label poison 均不得改变 T 的 source feature hash。
- 时间质量固定为 `DATE_ONLY_BACKFILLED_NON_VINTAGE`；不得升级为 `EXACT`。

### 6.3 支持度只作可行性

readiness 对 0/20/60/120/252 个交易日回看分别报告：全父包、Top20、Top50 最近 disclosure 与最近 qualifying event 的比例；每日支持 min/median/max、source/type 分布、neutral/qualifying 分布；Top50 同时存在 seen/unseen 的真实可干预日；正常缺失、source 空表和 schema/PIT error 分开计数。

固定 readiness 下限：

1. 386 个决策日、1,710,301 个父包键和 4,503 股票全部保留；
2. 三类 raw source 在窗口内均非空，每个标准季报期至少有一行；
3. raw projection 至少 20,000 行，qualifying event 至少 5,000 行；
4. 120 交易日 Top20 最近 disclosure 比例至少 0.70，且至少 380 日支持不少于 5 只；
5. 至少 300 日的 Top50 qualifying-event 状态同时包含 true/false，证明非恒等干预；
6. date-only next-trading-day 匹配率为 100%；
7. forecast/express/fina indicator 的 event-type drift 率分别不高于 0.02；
8. projection 无重复主键、非法日期、非有限 severity/confidence 或未知 source/type。

这些阈值完全来自 target-free 行数与时钟探针，只判断能否研究；不作为模型经济晋级标准。

## 7. Artifact、inspect 与 route

source bundle 固定成员：`source_request.json`、`parent_identity.json`、`event_source_projection.parquet`、`source_support_daily.parquet`、`source_revision_report.json`、`source_readiness_receipt.json`、`resource_report.json`、`registry_record.json`、`manifest.json`。

bundle id 是 request、parent identity、query template、rule source identity、projection member hash 的 canonical hash。临时目录完成 schema、row count、size、SHA-256、parquet readback和成员闭包后原子发布；partial、extra、mutation 均拒绝 inspect。

route 固定为：

- ready -> `N3_FINANCIAL_EVENT_INFORMATION_SET_MVE_DESIGN`
- not ready -> `N3_FINANCIAL_EVENT_SOURCE_GAP_DECISION`
- invalid -> route 保持 `N3_FINANCIAL_EVENT_SOURCE_READINESS_DESIGN`，只允许相同 request 修实现缺陷后 exact retry

readiness 不追加模型 trial；registry 记录 `study_type=ORACLE_DIAGNOSTIC`、`planned/generated/evaluated_model_trials=0/0/0`、`decision_use=NAVIGATION_ONLY` 和 consumed development window。exact retry 必须返回相同 bundle id，registry duplicate no-op、route exact no-op。

## 8. Implementation plan / 实施方案

允许范围：

1. `backend/services/advisory_model_first/financial_event_source_readiness.py`
2. `scripts/advisory_financial_event_source_readiness.py`
3. `backend/tests/advisory_model_first/test_financial_event_source_readiness.py`
4. `backend/tests/advisory_model_first/test_financial_event_source_delivery.py`
5. 必要的 exact CI classifier/ownership 单文件映射及其直接测试
6. 本详细设计与顶层蓝图的状态事实更新

顺序：contracts -> parent projection guard -> read-only raw reader -> classification/PIT -> support -> bundle/inspect -> route -> thin CLI -> direct tests。任何新表、联网、回填、通用平台或第二模型任务必须停止并另行设计。

## 9. 验证方案

1. Contract：固定枚举、false gates、父包行数/identity、0 model trial 和三类 route。
2. DB：read-only/repeatable-read readback；SELECT allowlist；DML/DDL/token/第二连接拒绝。
3. Parent：Parquet 列投影 spy；outcome/label poison 不读取；重复键、非有限 score、身份漂移失败。
4. Version：最早本地版本确定性；多版本分类统计；不得按 revision 删除行。
5. PIT：周末/节假日/同日/未来日期；next-trading-day 精确映射；future poison 不变。
6. Missing：neutral、no disclosure、正常缺失、空表、schema error 分型；不删父包股票/日期。
7. Support：0/20/60/120/252、Top20/50、每日支持、混合干预和阈值边界。
8. Delivery：partial/extra/mutation、manifest readback、atomic publish、inspect、exact retry、registry/route no-op。
9. 本地门禁：changed-file Ruff/format、py_compile、两个 direct test 文件、`git diff --check`、ownership/L0。
10. F2：`python scripts/aistock_feature_workflow.py validate --design docs/architecture/advisory_n3_financial_event_source_readiness_f2_detailed_design_20260904.md --tier F2`。

## 10. 资源、安全与生产边界

- raw rows 分 source 流式读取，projection 内存与 temp 上限各 8 GiB；wall time 仅 telemetry，不设 8/10 小时自动终止。
- 每类 source 只查询一次；不得逐候选逐日查询。目标正式运行应在分钟级完成，而不是每日重建工作区。
- 数据库 query/write、network、Tushare、sealed、factor/package/runtime/position/order 计数分别报告；除 read query 外全部必须为 0。
- 数据库只读 projection 是导航输入，不改变生产数据。DEV 空表状态一并记录，不要求为了测试复制或回填 DEV。
- 本阶段无需后端重启、依赖安装或 DDL。若实现发现需要这些动作，立即停止并通知用户。

## 11. 风险与控制

| 风险 | 控制 |
|---|---|
| 回填数据冒充历史 vintage | 明确 `DATE_ONLY_BACKFILLED_NON_VINTAGE`；只允许导航，不支持 confirmation/activation |
| latest row 吸收后续修订 | 固定最早本地观察版本；全部版本只做漂移报告 |
| 只读 live DB 在运行中变化 | 单一 repeatable-read/read-only snapshot；发布 immutable projection；后续 MVE 不再查 DB |
| `event_signal` 缺行被当作无事件 | raw neutral 全保留；no-disclosure 与 neutral/qualifying 分开 |
| 复用旧收益结论挑事件 | 三 source 全 roster；readiness 不读经济字段；旧实验留待 MVE multiplicity 计数 |
| T 日公告泄漏到 T 日决策 | date-only 一律下一交易日生效；100% parity 与 future poison test |
| 稀疏事件造成恒等候选 | Top20/50 支持与混合干预日硬检查；不足则 typed not-ready |
| source readiness 膨胀成平台 | 两模块、薄 CLI、两 direct tests；无 ingestion/scheduler/UI/cache |

## 12. Rollout 与后续

设计审查通过后实现最小 probe；源码必须先合入 clean main，再从 clean main 运行一次正式 source readiness。ready 只放行一个新的事件 MVE 详细设计，不自动训练、不自动启动 Tushare、不激活运行时。not-ready 只把精确数据缺口交给用户决定，不自行回填或换源。

评分/HMM 辅助线继续使用独立文件 `advisory_score_hmm_conditioned_admission_f2_detailed_design_20260904.md`，不得改变本 route 或共享本次 trial 身份。

## 13. Production Gates

```text
production_ddl_gate = noop
production_dml_gate = noop
dev_ddl_gate = noop
dev_dml_gate = noop
backend_restart_gate = noop
dependency_install_gate = noop
tushare_or_network_access = false
historical_backfill = false
sealed_holdout_access = false
factor_catalog_write = false
strategy_package_write = false
runtime_activation = false
position_or_order_write = false
```

## 14. Design Acceptance Index

| design_item | requirement |
|---|---|
| F-980 | 只从 margin selected=0 route 进入，旧 N3 frontier 不重跑，唯一后续为事件 source readiness |
| F-981 | 父包只读 key/score 四列，身份、行数、日期、股票和唯一键完整冻结 |
| F-982 | 数据库固定 repeatable-read/read-only SELECT allowlist，无 DDL/DML/Tushare/network/backfill |
| F-983 | 三类 raw source 使用最早本地观察版本，全版本 revision 只诊断、不按未来修订删行 |
| F-984 | neutral、qualifying、no disclosure、normal missing 和 invalid source 五类不混淆 |
| F-985 | date-only 一律公告日后首个交易日生效，T/T+1/future/label poison 不影响 T 特征 |
| F-986 | target-free source、Top20/50、每日混合干预与分类漂移支持阈值预注册 |
| F-987 | readiness 只产生 0 model trial 和一个 typed route，不读取经济结果或挑事件类型 |
| F-988 | content-addressed projection、闭包 manifest、inspect、atomic publish 和 exact retry 完整 |
| F-989 | 8 GiB 资源边界、批量读取、无逐日 DB 查询和无平台工程扩张 |
| F-990 | non-vintage 证据只能导航，所有生产、sealed、仓位、运行时和重启门禁保持关闭 |

## 15. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-980 | §1、§2、§7 route | artifact: `F:/Dev/AIstock_model_artifacts/advisory_n3_margin_information_set_formal_v1_20260904/margin_information_set_bundles/b50411d8d68838a3162d5d4e5070259af9a0ba02a515b556c8340ad968537ae4/learnability_receipt.json` | FORMAL_INPUT_VERIFIED | none |
| F-981 | §5.1 parent contract；`financial_event_source_readiness.py` | `backend/tests/advisory_model_first/test_financial_event_source_readiness.py` parent projection and poison tests | IMPLEMENTED_LOCAL_VERIFIED | approved_by_user: formal bundle is a separate clean-main state |
| F-982 | §5.2、§10；`read_database_snapshot` | `backend/tests/advisory_model_first/test_financial_event_source_delivery.py` session contract；artifact: local smoke `resource_report.json` in bundle `46dfc443...` | IMPLEMENTED_LOCAL_VERIFIED | approved_by_user: production access was read-only; all writes remain forbidden |
| F-983 | §5.3、§6.1；`project_earliest_raw_versions` | `backend/tests/advisory_model_first/test_financial_event_source_readiness.py` earliest-version and revision tests | IMPLEMENTED_LOCAL_VERIFIED | approved_by_user: no vintage claim or row filtering by future revision |
| F-984 | §2、§6.1、§9；raw neutral projection | `backend/tests/advisory_model_first/test_financial_event_source_readiness.py` neutral/qualifying separation | IMPLEMENTED_LOCAL_VERIFIED | approved_by_user: no neutral fill or stock/date deletion |
| F-985 | §6.2；calendar `bisect_right` clock | `backend/tests/advisory_model_first/test_financial_event_source_readiness.py` calendar and future poison tests | IMPLEMENTED_LOCAL_VERIFIED | approved_by_user: conservative date-only clock is frozen |
| F-986 | §6.3；`calculate_source_support`/`evaluate_readiness` | artifact: local bundle `46dfc443.../source_readiness_receipt.json`; `backend/tests/advisory_model_first/test_financial_event_source_readiness.py` support boundaries | IMPLEMENTED_LOCAL_VERIFIED | approved_by_user: thresholds decide research feasibility only |
| F-987 | §2、§3、§7；zero-trial registry record | `backend/tests/advisory_model_first/test_financial_event_source_delivery.py` route/trial/column-read tests | IMPLEMENTED_LOCAL_VERIFIED | approved_by_user: no model or economic evidence in readiness |
| F-988 | §7；build/inspect/deliver | `backend/tests/advisory_model_first/test_financial_event_source_delivery.py` manifest/mutation/retry tests；artifact: local exact retry bundle `46dfc443...` | IMPLEMENTED_LOCAL_VERIFIED | approved_by_user: formal bundle waits for clean-main implementation |
| F-989 | §8、§10；batch query and resource guard | `backend/tests/advisory_model_first/test_financial_event_source_delivery.py`; artifact: local smoke elapsed/RSS/temp/query receipt | IMPLEMENTED_LOCAL_VERIFIED | approved_by_user: no daily workspace or general event platform |
| F-990 | §3、§10、§12、§13；thin CLI and false gates | `backend/tests/advisory_model_first/test_financial_event_source_delivery.py`; command: `python scripts/aistock_feature_workflow.py validate --design docs/architecture/advisory_n3_financial_event_source_readiness_f2_detailed_design_20260904.md --tier F2` | IMPLEMENTED_LOCAL_VERIFIED_FORMAL_PENDING | approved_by_user: restart/DDL/DML/runtime remain separately owned |

## 16. DESIGN-COMPLIANCE-001

1. **禁止简化交付**：source identity、raw/neutral 语义、PIT、revision、支持度、artifact、route、资源和后续证据边界均为显式合同；不把 schema 或临时计数冒充 source-ready。
2. **禁止静默错误或伪成功**：正常 absence/missing 与事务/schema/PIT/identity error 分型；invalid 不发布；不存在 parent-only 或旧 `event_signal` 静默 fallback。
3. **禁止未经确认改变业务逻辑**：不改变父包、Selection、Top20、review policy、Entry/Exit、仓位或下单；ready 仅放行新的详细设计。
4. **禁止私增门禁或审批**：全部阈值只属于 target-free research feasibility；输入合格时自动执行。生产 restart/DDL/DML/activation 保持既有用户所有权。
