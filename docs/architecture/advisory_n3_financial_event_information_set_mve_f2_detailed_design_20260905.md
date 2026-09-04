# Advisory N3 财务事件信息集 Learnability MVE F2 详细设计 v1.2

> 日期：2026-09-05
> 状态：`IMPLEMENTED_LOCAL_VERIFIED_FORMAL_RUN_PENDING`
> tier：`F2`
> research stage：`N3_FINANCIAL_EVENT_INFORMATION_SET_MVE`
> objective contract：`ALPHA_RANKING`
> study type：`LEARNABILITY_AUDIT`
> decision use：`NAVIGATION_ONLY`
> evidence class：`EXPLORATORY_NON_VINTAGE`
> production gates：backend restart / DDL / DML / database access / network / Tushare / factor catalog / StrategyPackage / runtime activation / position / order 均为 `noop`

## 1. Background / 当前事实与业务问题

1. N3 融资融券正式 MVE `b50411d8...` selected=0，不可变 route 已完成后继 source readiness。
2. 财务事件 source-readiness F2 v1.2 源码经 PR #4288 合入；clean-main 正式 bundle `211b8db192c83b79f7731649e84a2f929c1d56579e337c438d84e90aa3fb7ead` 为 `SOURCE_READY_NAVIGATION_ONLY_NON_VINTAGE`，next task 固定为本设计。
3. 正式 projection `event_source_projection.parquet` 为 7,061,459 bytes、SHA-256 `d9bda2d23335354bb99f04c5a11643ee56347ae2e8ac871f7bae77e39030bded`，共 84,272 行：forecast 15,564、express 3,340、fina indicator 65,368；qualifying 44,953、neutral 39,319。
4. target-free 类型 roster：positive 为 `*_large_growth` 与 `financial_forecast_turnaround`；negative 为 `*_loss` 与 `*_large_decline`；`*_neutral` 只表示披露存在。severity 范围 `0..0.7`、confidence `0.5..0.78`，全部有限。
5. 120 日 Top20 disclosure/qualifying coverage 为 `100%/87.8756%`，Top50 qualifying coverage `83.4974%`，378/386 日有 Top50 mixed qualifying state。source 足以产生干预，不证明收益可学。
6. source 最早在 2026-05 才被本地观察，无法还原各历史决策日真正可见的 numeric revision。虽然最早本地版本与分类 drift 已受控，它仍是 `DATE_ONLY_BACKFILLED_NON_VINTAGE`；任何经济结果只能导航是否建设真实 vintage source，不能成为 confirmation 或 activation evidence。
7. 冻结父包仍为 N2-B `CURRENT_IC_PARENT`：1,710,301 行、386 日、4,503 股票、日期 `2024-07-04..2026-02-02`；父 parquet SHA-256 `48598f1afe893c1718098f258a69cc579d831c5e4bea6d54b290c7ac0bd3b039`。已知 outcome 1,709,387 行，其中 4,055 行 nonfinite；可评价 1,705,332 行，父 Top5 384 日可评价。
8. 当前 N3 累计 candidate index 为 80；本 MVE 固定增加三个 model trial，只有 signed-content trial 可选择。旧 proposal/overlay/腿间/分钟/generator/margin 均已消费，不得借事件字段重跑。

本 MVE 只回答：在同一父包、同一开发窗口、同一 H20 成本后超额 label 下，财务披露的 target-free existence 与 signed content 是否对父 Alpha 排名提供可归因增量？

## 2. Scope / 目标与终止条件

交付：

1. 只消费正式 immutable source bundle 与 N2-B/N1 immutable inputs，构造全父包键的 PIT event feature panel。
2. 固定 parent comparator、disclosure control、signed-content candidate 三个 Ridge trial，不搜索 source、event type、window、模型或超参。
3. 复用 N1 28-path CPCV/20 日 embargo/7 次 OOF，比较 RankIC、Top5 H20 net excess、干预、稳定性和 multiple testing。
4. 发布 content-addressed bundle、inspect、registry 和唯一 typed route；不训练最终模型、不写运行时。

终止状态：

- `EVENT_CANDIDATE_SELECTED_NAVIGATION_ONLY_NON_VINTAGE`：signed-content 通过预注册门槛；next task 为 `N3_FINANCIAL_EVENT_VINTAGE_SOURCE_DECISION`，不得直接 confirmation。
- `EVENT_FRONTIER_SELECTED_ZERO`：signed-content 未通过；只关闭本次 source/rule/features/Ridge/window exact frontier，next task 为 `N3_SCORE_HMM_ADMISSION_MVE_IMPLEMENTATION`。
- `EVENT_EXPLORATORY_INSUFFICIENT_SUPPORT`：feature 或 intervention 支持不足；selected=0，不外推为全部财务事件不可学，next task 同样为 score/HMM 实现。
- `INVALID`：identity/PIT/model/artifact/资源错误；不发布、不改 route，只允许 same-request 非经济修复 exact retry。

## 3. Non-goals / 禁止项

- 不重新查询 PostgreSQL，不调用 Tushare/网络，不回填或修订正式 projection。
- 不按本次收益挑 source、event type、方向、lookback、severity、confidence、模型或阈值。
- 不把 neutral 当 0 收益/负事件，不把 no-disclosure 声称为公司没有事件。
- 不读取 source raw payload、后来版本、报告期后的财务值或 sealed holdout。
- 不删除无事件、停牌、标签未知或正常数据缺失的股票/日期；缺失与 invalid 分型。
- 不建立事件平台、feature store、scheduler、UI、API 或通用模型框架。
- 不写数据库、因子库、StrategyPackage、Selection、Advisory runtime、Paper/QMT、仓位或订单。
- 不把 non-vintage 正结果称为确认性 Alpha，也不把负结果关闭整个 financial-event DGP。

## 4. Architecture / 数据流

```text
formal source bundle 211b8db1... (read-only)
  + N2-B CURRENT_IC_PARENT/outcome panel
  + N1 CPCV/regime identities
                    |
       exact identity + member closure
                    |
 effective_trade_date <= decision T
 rolling 20/60/120/252 event aggregation
                    |
 all 1,710,301 parent keys retained
                    |
 parent / disclosure / signed-content schemas
                    |
 fixed Ridge x 3 + 28 CPCV + 7 OOF
                    |
 paired RankIC / Top5 / support / stability
                    |
 immutable bundle + registry + main route
```

正式 source bundle 是唯一事件输入；运行期不得再次访问 live DB。所有 event features 先按 instrument/trading-session 批量构建，再按 parent date/key 向后连接；禁止 1.7M 行逐行扫描 source 或逐日重建工作区。

## 5. Contracts / 输入、身份与时钟

### 5.1 Required authorities

request 必须冻结并 readback：

- source bundle id/schema/manifest SHA、九成员 closure、source request/receipt/projection identities；
- source state `SOURCE_READY_NAVIGATION_ONLY_NON_VINTAGE`、time quality、rule version、source roster；
- N2-B parent file size/hash/arm/date/row/key/score/outcome identity；
- N1 bundle `74827d03...` 的 28 READY paths、regime daily、split/embargo identity；
- package/program/binding/manifest/style/terminal weights、selection semantics、baseline/shadow/cost policy hashes；
- current route 必须仍指向 `N3_FINANCIAL_EVENT_INFORMATION_SET_MVE_DESIGN` 与 source bundle `211b8db1...`；
- repository clean-main commit、output/registry/route path、资源和全部 false gates。

新增 `FrozenFinancialEventInformationSetRequestV1`。功能字段全部进入 canonical SHA，排除 `created_at/output_root`；request id 为 `advn3fevent_<sha256[:24]>`。正式 request 一旦写入只可 exact readback，不允许 CLI override trial、feature、window、threshold 或 route。

### 5.2 Parent/outcome projection

父 parquet 只读取 `arm_id/decision_as_of_trade_date/instrument/score/target_trade_date/economic_net_excess_bps/outcome_known`。只保留 `CURRENT_IC_PARENT`；父 key、score、target date 与 source-readiness parent identity 精确一致。label 列仅在 feature panel identity 完成并冻结后进入 cross-fit/evaluation。feature builder 的函数参数不得包含 outcome/return；future-label poison 不得改变 feature hash。

### 5.3 Event clock

- source row 只在 `effective_trade_date <= decision_as_of_trade_date` 时可见；date-only 公告同日不可用于同日决策。
- rolling age 使用与 N1 `market_calendar_identity` 同源的 Qlib day calendar，不用自然日。N1 的 identity 声明截止为资产版 `2026-06-30`，但其 606 日语义 hash 实际按 request `data_cutoff=2026-03-10` 生成；prepare 必须分别冻结二者，不能误用声明截止重算 hash。为覆盖首个 decision 的252日窗口，request 另冻结同源 `2023-01-01..2026-03-10` session list及hash。
- windows 固定为 event age gap `<=20/60/120/252`：T 当日已生效事件 age=0，因此与 source-readiness 的既有120d口径完全一致；不得在结果后改成自然日或 `<L` 口径。
- 同 instrument/source/event type/effective date 的多行全部按 projection identity 计数；不得结果后去重。projection 主键已在 source bundle 固定。
- T+1/future effective date、未来 source row、outcome、price 与 label poison 不得改变 T feature 或 request hash。

## 6. 固定方向映射、特征与缺失语义

### 6.1 Direction mapping

```text
+1: financial_forecast_large_growth
    financial_forecast_turnaround
    financial_express_large_growth
    financial_indicator_large_growth
-1: financial_forecast_loss
    financial_forecast_large_decline
    financial_express_loss
    financial_express_large_decline
    financial_indicator_large_decline
 0: financial_forecast_neutral
    financial_express_neutral
    financial_indicator_neutral
```

`signed_value = direction * severity_score * confidence`。未知 source/type、非有限 severity/confidence、`should_signal` 与 direction 矛盾均 fail closed；不使用结果数据重解释方向。

### 6.2 Disclosure control features

`EVENT_DISCLOSURE_CONTROL_V1` 在 `parent_rank_pct` 上固定增加：

1. `event_disclosure_seen_120`；
2. `event_qualifying_seen_120`；
3. `event_source_type_count_120`；
4. `event_disclosure_count_120_log1p`；
5. `event_neutral_count_120_log1p`；
6. `event_latest_disclosure_age_120`。

无 disclosure 时 seen/count/source count 为 0，age 固定为 121，并由 seen flag 明确区分；这是“冻结 source 未观察到披露”，不是 neutral 或公司无事件。

### 6.3 Signed content features

`EVENT_SIGNED_CONTENT_V1` 在 disclosure control 上固定增加 12 项：

1. `event_signed_value_sum_20`；
2. `event_signed_value_sum_60`；
3. `event_signed_value_sum_120`；
4. `event_signed_value_sum_252`；
5. `event_positive_count_20_log1p`；
6. `event_negative_count_20_log1p`；
7. `event_positive_count_120_log1p`；
8. `event_negative_count_120_log1p`；
9. `event_latest_qualifying_signed_value_120`；
10. `event_forecast_signed_value_sum_120`；
11. `event_express_signed_value_sum_120`；
12. `event_fina_indicator_signed_value_sum_120`。

latest qualifying tie 按 `(effective_trade_date DESC, source_type ASC, event_type ASC, source_record_key ASC)` 唯一决定。无 qualifying event 时 signed/count 为 0，且 `event_qualifying_seen_120=0`；不得用 median/neutral 填造事件。所有 count 先 `log1p`，signed value 保留规则单位，再由 train-only StandardScaler 处理。

### 6.4 Three schemas

| trial | features | selectable |
|---|---|---|
| `EVENT_PARENT_COMPARATOR_V1` | `parent_rank_pct` | no |
| `EVENT_DISCLOSURE_CONTROL_V1` | parent + §6.2 六项 | no |
| `EVENT_SIGNED_CONTENT_V1` | disclosure control + §6.3 十二项 | yes, at most once |

全部 1,710,301 parent keys 必须保留且 feature finite。任一 fold 的 signed feature 全常数、source member mutation、unknown type 或 key loss 使 candidate invalid；不得删列或退回 disclosure control 并声称 candidate 成功。

## 7. Fixed model and cross-fitting

三个 trial 统一：

```text
SimpleImputer(strategy="median")
StandardScaler()
Ridge(alpha=100.0, solver="lsqr", fit_intercept=True)
target = economic_net_excess_bps
```

所有 feature 已按 §6 保证 finite，imputer 仍作为训练 contract 守卫；出现全缺列立即失败。模型 family/alpha/solver/intercept 不搜索。

精确复用 N1 8 groups、28 READY paths 与 20 日 embargo。train/validation 日期不相交；只有 `outcome_known=true` 且 label finite 行训练。每个 parent row 必须作为 validation 恰好 7 次，按 prediction sum/count 聚合，禁止 materialize 28 份完整 panel。`planned/generated/evaluated_model_trials=3/3/3`，current parent 是冻结 baseline，不计新 trial。

## 8. Evaluation、support 与一次选择

### 8.1 Daily paired metrics

对 current parent、parent comparator、disclosure control、signed-content candidate 每日计算：

- full-cross-section Spearman RankIC；
- Top5 `economic_net_excess_bps`、Top5 instrument set、evaluable status；
- signed candidate 相对三 baseline 的 replacement count、Top5 lift、日 score Spearman；
- 相邻决策日 Top5 churn；
- source/qualifying/neutral/positive/negative coverage 和 feature variance；
- 四个连续时间 block、late half 与 N1 regime；
- 20 日 moving-block bootstrap 2,000 次、seed `20260905`；MDE、daily lift Sharpe、skew/kurtosis、DSR 作诊断。

任一 Top5 含 unknown/nonfinite label 时该模型当日 Top5 经济指标 typed unavailable，不用少于五只均值或填零。current-parent parity 必须证明 386 日 RankIC、386 日 Top5 set 及 384 个可评价日数值与 N2-B 一致。

### 8.2 Source/intervention support

source/feature 必须满足：

- source bundle member/schema/hash/84,272 rows 全部通过；
- 386 日、1,710,301 parent keys 全保留；
- Top20 120 日 disclosure/qualifying fraction 分别 `>=0.70`；
- 至少 380 日 Top20 disclosure count `>=5`，至少 300 日 Top50 qualifying state mixed；
- 三 source 均至少一个 qualifying row；positive/negative/neutral 三类均非空；
- 每项 candidate feature 在全窗口及每个 outer train 至少两个 finite unique values。

signed candidate 相对 current parent、parent comparator、disclosure control 分别要求：paired evaluable days `>=382`、intervention days `>=60`、intervention fraction `>=0.25`，并在 N1 每个实际 regime 至少 20 个 intervention days。support 不足只标记 `EXPLORATORY_INSUFFICIENT_SUPPORT`。

### 8.3 Multiplicity and selection

本 MVE 延续 cumulative candidate index 80，并把三个新 model trial 计入 81～83。signed candidate 相对 current parent 的 RankIC/Top5 两个 primary comparison 使用 one-sided cumulative Bonferroni `alpha=0.05/(83*2)`；相对 parent comparator 与 disclosure control 的四项使用 current-MVE family-wise `alpha=0.05/4`。两套区间都报告，DSR 不能替代经济门槛。

candidate 仅在以下全部通过时 selected=1：

1. source/identity/PIT/schema/CPCV/OOF/parent parity/support 全通过；
2. 相对三个 baseline 的 intervention support 全通过；
3. 相对 current parent 的 cumulative family-wise RankIC delta lower `>0`；
4. 相对 current parent 的 cumulative family-wise Top5 lift lower `>5 bps`；
5. 相对 parent comparator 与 disclosure control 的 current-family RankIC/Top5 lower 全部 `>0`；
6. late-half 两项 delta `>0`，四个时间 block 至少三个同时 RankIC delta/Top5 lift `>0`；
7. 无 constant/identity/PIT/nonfinite/resource/artifact 错误。

frontier 只选择一次。selected=1 仍为 non-vintage navigation，只说明值得投资真实 vintage source；selected=0 不允许改窗口/类型/符号/模型重跑。实现或输入身份错误且未利用经济结果时才允许 same-request exact retry。

## 9. Artifact、registry 与 route

bundle 固定成员：

```text
request.json
source_identity_receipt.json
feature_schema.json
event_feature_panel.parquet
feature_coverage_daily.parquet
oof_scores.parquet
fold_diagnostics.parquet
model_daily.parquet
model_summary.json
stability_report.json
frontier_receipt.json
resource_report.json
registry_records.json
learnability_receipt.json
manifest.json
```

bundle id 绑定 request 与全部 semantic member hash。临时目录完成 schema、row count、size、SHA-256、parquet readback和 closure 后原子发布；partial/extra/mutation 均拒绝 inspect。

registry 为三个 model trial 各写一条 `LEARNABILITY_AUDIT/NAVIGATION_ONLY/EXPLORATORY` record，明确 `source_time_quality=DATE_ONLY_BACKFILLED_NON_VINTAGE`。route：

- selected=1 -> `N3_FINANCIAL_EVENT_VINTAGE_SOURCE_DECISION`
- selected=0 或 insufficient support -> `N3_SCORE_HMM_ADMISSION_MVE_IMPLEMENTATION`
- invalid -> 保持 `N3_FINANCIAL_EVENT_INFORMATION_SET_MVE_DESIGN`

exact retry 必须返回相同 bundle id；重复 deliver registry duplicate-noop、route exact-noop。route 不得写 selected model descriptor 或 runtime eligibility。

## 10. Resource、failure 与安全边界

- parent parquet 与事件 projection 只读且按所需列加载；cross-fit 按 fold/模型流式累计 OOF prediction sum/count，禁止 materialize 28 份完整 panel。
- 正式运行固定 `max_rss_bytes=8589934592`、`max_temp_bytes=8589934592`；wall time 只记录 telemetry，不设置 8/10 小时终止门禁。
- 每阶段记录 row、byte、read、elapsed、peak RSS 和 temp bytes；资源越界、schema/identity/PIT/OOF 错误均 typed fail closed。
- DB query/write、network、Tushare、Qlib feature、factor/package/runtime/position/order write count 全部必须为 0；只允许从 N1 已绑定的 Qlib root 读取一次 day calendar 并复核606日语义hash，不读取行情/因子字段。
- sealed holdout 不进入 allowlist；request、receipt 与 resource report 固定 `sealed_holdout_accessed=false`。
- 正常“某股票无披露”是显式业务状态，保留股票和日期；文件缺失、hash/schema/key/split 错误是 invalid，二者不得混淆。
- artifact 与日志不得包含 password、token、private key 或 `.env` 内容，只记录非秘密路径、计数和 hash。

## 11. Implementation plan / 实施方案与精确文件范围

实现只允许以下范围：

1. `backend/services/advisory_model_first/financial_event_information_set_contracts.py`
2. `backend/services/advisory_model_first/financial_event_information_set_pipeline.py`
3. `scripts/advisory_financial_event_information_set_mve.py`
4. `backend/tests/advisory_model_first/test_financial_event_information_set_contracts.py`
5. `backend/tests/advisory_model_first/test_financial_event_information_set_pipeline.py`
6. `backend/tests/advisory_model_first/test_financial_event_information_set_delivery.py`
7. CI/ownership exact classifier mapping及其直接测试（仅在首轮 CI 证明薄 CLI 未覆盖时加入）
8. 本详细设计与顶层 Advisory 蓝图的事实、进度、route 更新

顺序固定为 contracts -> feature builder -> source/parent identity -> cross-fit -> paired evaluator -> immutable bundle/inspect -> registry/route -> thin CLI -> tests。优先复用已合入 research-control、trial-registry 与 bundle helper；不得借机建设通用特征平台、ingestion、scheduler、UI 或缓存服务。

正式经济实验必须在实现源码合入后的 clean `main` 上运行；未合入 worktree 只允许 fixture/合成数据验证，不形成正式研究证据。

## 12. Verification plan / 验证方案

1. Contracts：冻结字段、三个 trial、唯一 selectable trial、false gates、route 与 exact retry。
2. Source/identity：formal source bundle manifest/hash、parent parquet、N1 request/paths/regime、policy/label/universe parity，任一漂移 fail closed。
3. Event clock：`effective_trade_date<=T`；weekend/holiday；20/60/120/252 窗口；T+1/future/label poison 不改变 T 特征。
4. Direction/formula：十二 event type 的固定方向、`direction*severity*confidence`、source-specific sums、count log1p、latest value 与 age sentinel 逐值单测。
5. Missing：无披露/仅 neutral/仅 positive/仅 negative/同日多源/重复 source key；全部 parent key 保留，normal missing 不阻断也不删股。
6. CPCV：8 block、28 READY path、20 日 embargo、train/validation 隔离、每行恰好 7 OOF、三个 trial exact parity、train-only median/scaler。
7. Evaluation：current-parent parity、三个模型每日 RankIC/Top5、relative intervention、source/support、multiplicity、block/late-half、MDE/DSR diagnostics 与一次选择。
8. Delivery：partial/extra/mutation、manifest readback、inspect、atomic publish、registry append/duplicate no-op、route exact no-op、invalid 不写 registry/route。
9. 本地最小门禁：changed-file Ruff/format、py_compile、三个 direct test 文件、`git diff --check`、ownership/guardrail/L0。
10. 稳定后单次相关矩阵：`python -m nox -s advisory_modeling_backend`；F2 validator：`python scripts/aistock_feature_workflow.py validate --design docs/architecture/advisory_n3_financial_event_information_set_mve_f2_detailed_design_20260905.md --tier F2`。

## 13. Risks and controls / 风险与控制

| 风险 | 控制 |
|---|---|
| 后验财务分类被误当当时可见 vintage | evidence 固定 `EXPLORATORY_NON_VINTAGE`；正结果只路由到 vintage source decision |
| “有披露”本身代理规模/关注度 | 独立 disclosure-control trial；signed candidate 必须同时优于它与 parent-only |
| severity/confidence 和方向按结果调参 | 类型映射与乘法公式在经济运行前冻结；无权重/窗口/类型搜索 |
| 无披露股票被删除或填造事件值 | 以 parent keys 左连接；count/sum 为 0、age=121、seen=false，股票与日期全部保留 |
| neutral 被错误解释为零收益观点 | neutral 只进入 disclosure/neutral count，不进入 signed value |
| 同日多条披露造成任意覆盖 | source_record_key 去重后在窗口内确定性聚合；latest 使用 effective date、source type、record key 稳定排序 |
| parent rank 与披露支持导致伪增益 | 三 trial 同一 cross-fit；parent comparator 与 disclosure control 均为强制基线 |
| 同一窗口继续研究者过拟合 | 累计候选 80 冻结；新 trial 81..83 全登记；累计与本轮 family-wise 双重报告；frontier 只选一次 |
| oracle/诊断污染 sealed holdout | allowlist 不含 sealed holdout；访问标志、路径扫描和 receipt 均 fail closed |
| 三个 trial 被误报为三个 Alpha | 只有 signed-content selectable；两个 comparator 明确计为 non-selectable model trials |
| 正结果被直接接入荐股 | 无 final refit/model artifact/adapter/active pointer；route 只决定下一项研究任务 |
| 实现演变成平台工程 | 两模块、薄 CLI、三测试的 exact scope；不建 ingestion/scheduler/UI/cache |

## 14. Rollout and rollback / 发布与回滚

Rollout 仅指：F2 设计与源码在本分支共同审核 -> PR 合入 -> clean-main source/runtime identity readback -> 生成唯一冻结 request -> 正式开发窗口 MVE -> inspect/deliver。源码合入不等于实验成功，实验成功不等于 confirmation 或 activation。

回滚通过新 PR 回退源码；运行失败只删除 task-owned、未原子发布的临时目录。已发布 immutable bundle、append-only registry、已消费窗口/frontier 不覆盖、不删除、不改写。源 bundle、父 artifact、策略包、factor catalog 与 active runtime pointer 始终只读。

## 15. Production Gates / 生产影响

```text
production_ddl_gate = noop
production_dml_gate = noop
dev_ddl_gate = noop
dev_dml_gate = noop
backend_restart_gate = noop
dependency_install_gate = noop
database_access = false
network_access = false
tushare_access = false
qlib_calendar_read = true
qlib_feature_read = false
factor_catalog_write = false
strategy_package_write = false
runtime_activation = false
selection_or_advisory_business_write = false
position_or_order_write = false
sealed_holdout_access = false
```

本任务不需要重启或 DDL/DML；若未来出现相关操作，仍需由用户另行执行或明确授权。

## 16. Design Acceptance Index

| design_item | requirement |
|---|---|
| F-218 | 只消费 formal source-ready bundle、CURRENT_IC_PARENT 与 N1 frozen CPCV/regime，身份漂移 fail closed |
| F-219 | `effective_trade_date<=T` 且窗口边界确定；future/label poison 不改变特征 |
| F-220 | 十二 event type 使用冻结 direction；neutral 与 signed content 分离，公式无结果后调参 |
| F-221 | 全 1,710,301 parent key 保留；正常无披露显式表示，不阻断、不删股 |
| F-222 | 三个冻结 schema/trial，只有 signed-content selectable，固定 Ridge 无搜索 |
| F-223 | 28 CPCV、20 日 embargo、每 row 7 OOF、train-only preprocessing 完整 |
| F-224 | current parent、parent comparator、disclosure control 配对评价并去混淆 |
| F-225 | source/intervention support、MDE、stability、累计/current family-wise 全部预注册 |
| F-226 | 单次 0/1 frontier；positive/negative/insufficient/invalid route 与证据边界不混淆 |
| F-227 | immutable bundle、manifest、inspect、registry 与 exact retry no-op 完整 |
| F-228 | 8 GiB RSS/temp、无 wall gate、无 DB/network/Tushare/holdout/runtime 副作用 |
| F-229 | 文件范围、正式 clean-main 运行与后续 vintage-source 决策边界明确 |

## 17. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-218 | §5.1、§9；frozen request/source loader | artifact: formal source bundle `211b8db192c83b79f7731649e84a2f929c1d56579e337c438d84e90aa3fb7ead`; `backend/tests/advisory_model_first/test_financial_event_information_set_contracts.py` | IMPLEMENTED_LOCAL_VERIFIED | approved_by_user: formal experiment is a separate post-merge state |
| F-219 | §5.3、§6；`build_event_feature_panel` | `backend/tests/advisory_model_first/test_financial_event_information_set_pipeline.py` clock/window/future-outcome poison；command: direct pytest listed in §12 | IMPLEMENTED_LOCAL_VERIFIED | approved_by_user: formal experiment is a separate post-merge state |
| F-220 | §6.1；`EVENT_DIRECTION_BY_TYPE`与signed formula | `backend/tests/advisory_model_first/test_financial_event_information_set_contracts.py`; `backend/tests/advisory_model_first/test_financial_event_information_set_pipeline.py` direction contradiction | IMPLEMENTED_LOCAL_VERIFIED | approved_by_user: mapping is frozen before economic run |
| F-221 | §6.2-§6.4；prefix-window full-key builder | `backend/tests/advisory_model_first/test_financial_event_information_set_pipeline.py` missing-key retention；artifact: full 1,710,301-row target-free smoke | IMPLEMENTED_LOCAL_VERIFIED | approved_by_user: formal economic run remains pending |
| F-222 | §6.4、§7；`FinancialEventModelTrialV1` | `backend/tests/advisory_model_first/test_financial_event_information_set_contracts.py` exact three-trial/schema/drift tests | IMPLEMENTED_LOCAL_VERIFIED | approved_by_user: no model search |
| F-223 | §7；`run_event_crossfit` | `backend/tests/advisory_model_first/test_financial_event_information_set_pipeline.py` 28 path、84 fold、7 OOF tests | IMPLEMENTED_LOCAL_VERIFIED | approved_by_user: real-label cross-fit only after merge |
| F-224 | §8.1；`evaluate_event_models`与parent parity | `backend/tests/advisory_model_first/test_financial_event_information_set_pipeline.py` controls/select-once/parity tests | IMPLEMENTED_LOCAL_VERIFIED | approved_by_user: formal metrics do not yet exist |
| F-225 | §8.2-§8.3；support/inference/stability | source-readiness receipt；`backend/tests/advisory_model_first/test_financial_event_information_set_pipeline.py` support与one-sided alpha tests | IMPLEMENTED_LOCAL_VERIFIED | approved_by_user: economic thresholds remain unconsumed until formal run |
| F-226 | §2、§8.3、§9；receipt/route | `backend/tests/advisory_model_first/test_financial_event_information_set_delivery.py` zero route；contracts selected route | IMPLEMENTED_LOCAL_VERIFIED | approved_by_user: route write occurs only in formal deliver |
| F-227 | §9；publish/inspect/registry/deliver | `backend/tests/advisory_model_first/test_financial_event_information_set_delivery.py` mutation/partial/extra/duplicate-noop tests | IMPLEMENTED_LOCAL_VERIFIED | approved_by_user: formal immutable bundle remains pending |
| F-228 | §10、§15；frozen gates/resource/environment | `backend/tests/advisory_model_first/test_financial_event_information_set_contracts.py`; `backend/tests/advisory_model_first/test_financial_event_information_set_delivery.py`; command: `python scripts/advisory_financial_event_information_set_mve.py` | IMPLEMENTED_LOCAL_VERIFIED | approved_by_user: one Qlib calendar read only; no feature/DB/network/Tushare/runtime side effect |
| F-229 | §11、§14；exact scope、CI mapping与clean-main gate | `backend/tests/scripts/test_ci_change_classifier.py`; command: `python -m nox -s advisory_modeling_backend` | IMPLEMENTED_LOCAL_VERIFIED | approved_by_user: PR merge and formal run are distinct later states |

## 18. DESIGN-COMPLIANCE-001

1. **禁止简化交付**：设计覆盖 source/PIT、全 key/missing、三个模型、CPCV、经济评价、multiplicity、artifact、registry、route 与资源；不会把 readiness、coverage 或合成测试声称为模型效果。
2. **禁止静默错误或伪成功**：正常无披露与 identity/schema/PIT/资源错误严格分型；invalid 不发布、不写 registry/route；signed candidate 不得退化为 disclosure-only 或 parent-only 后仍报成功。
3. **禁止未经确认改变业务逻辑**：继承父股票池、H20 成本后超额 label、policy、Top5 和 Alpha-ranking 合同；不改变 Selection、Advisory、策略包排序、仓位或交易执行。
4. **禁止私增门禁/审批**：support/statistical 条件属于预注册研究合同，不新增生产人工 gate；合法输入时自动运行。restart、DDL/DML 与生产激活的用户所有权保持不变。

## 19. Source feasibility and current conclusion / 当前结论

当前结论为 `SOURCE_READY_IMPLEMENTATION_LOCAL_VERIFIED_FORMAL_RUN_PENDING`。formal source bundle 已证明 projection、类型、时钟与支持度可用于本次 non-vintage learnability MVE；源码现已实现冻结合同、全键PIT特征、三臂CPCV、配对评价、immutable delivery、三条trial registry和0/1 route。BUG-1360已把projection identity纠正为manifest中的完整64位SHA-256并增加exact identity回归，32个direct tests通过；同步最新主线后的完整`advisory_modeling_backend`矩阵为`876 passed/16 skipped`。真实target-free smoke在约9.93秒内完成1,710,301键/386日/22列，保留408个120日无披露行，1,095,669行具有非零120日signed sum，全部source/support门槛通过。该smoke未读取收益，不能证明事件内容可学习或形成荐股收益。只有源码合入并从clean main运行一次固定MVE，才允许按§9分流；任何结果都不得读取sealed holdout或直接形成factor、策略包、荐股runtime或激活结论。
