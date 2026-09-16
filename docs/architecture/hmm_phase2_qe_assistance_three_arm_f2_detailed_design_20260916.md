# HMM Evolution Phase 2：QE 场景三臂历史回放 F2 详细设计

> **设计层级**：F2
> **文档版本**：v1.3
> **日期**：2026-09-16
> **状态**：`D1_D2_D3B_D4_D5_D6_USER_APPROVED_IMPLEMENTATION_AUTHORIZED`
> **父权威**：`docs/architecture/hmm_evolution_and_risk_management_system_design_20260716.md` v2.58
> **唯一目标**：在一个冻结 QE 场景中比较无 HMM、历史旧 QE HMM 和一个新版 HMM 模块辅助，回答 HMM 辅助是否带来真实的成本后收益、回撤或换手增量；不建设通用平台，不替换旧版本，不把独立板块预测效果外推为 QE 增益。

## 0. Background / 背景、结论与批准边界

本设计已经找到可复用的真实历史场景、旧模型、原始预测、当前 v1.6 OOF 状态及其 exact C-013 authority。用户已于 2026-09-16 批准 D1、D2、D3-B、D4 及其 applicability 澄清、D5、D6；允许开始 HMM-owned adapter 实施，但正式三臂回放仍须等待 HMM/RD-Agent 两端源码闭合及独立实验授权。

1. exact v1.6 authority 已定位并验证，`mapping_manifest_sha256=e478722f700535ac4e37744a651291bc6d179cb899dccd28cdb957ed4491b82f`；
2. 该 authority 对 1,951,448 个 executable keys 明确返回 1,780,359 个 resolved 和 171,089 个 causal unavailable，而不是 100% resolved；
3. 已批准 `explicit non-applicable`：权威显式 unavailable 行不适用 HMM、保持 raw score 并完整记账；不得自行补行业、删除股票、缩窗或把 unavailable 伪装为 neutral。

当前授权允许分别实施 HMM adapter 与 RD-Agent consumer contract；不授权启动 QE 回放、提交 tail、数据库、runtime 或进程动作，也不得声称新版已接入。

## 1. 目标、范围与非目标

### 1.1 Scope / In scope

- 精确绑定一个历史 QE 场景及其无 HMM/旧 HMM 真实资产；
- 将 v1.6 `rotation_L1` 的冻结 OOF 状态转换为一个显式版本化 QE coefficient artifact；
- 以同一 raw Alpha prediction、同一股票池、策略、执行、成本、数据和回放窗口运行三臂；
- 报告成本后复合收益、年化收益、信息比率、最大回撤、换手、成本拖累、选股/持仓/成交差异；
- 保留旧 `hmm_model_version_id`、preset 和运行配置，不自动切换任何默认值；
- 新版失败或无增益时如实保留旧版与无 HMM 基线。

### 1.2 Non-goals / 非目标

- 不重新训练 Alpha、旧 HMM 或 v1.6；
- 不读取 2026-04-01 起的 rotation sealed tail；
- 不修改 QE 平台、调度器、Selection、Paper、Advisory、Dataset 或推荐模块；
- 不把 Phase 1 TopK 诊断冒充正式 QE 回放；
- 不搜索 coefficient 幅度、日期、股票池、策略、执行、成本、horizon 或模型；
- 不建设新的 registry、evidence store、训练服务、通用 adapter 平台或自动组合；
- 不执行 DDL/DML、依赖安装、runtime activation 或进程控制。

## 2. 已确认事实与不可变身份

### 2.1 历史 QE 锚点

冻结任务为 `qe_20260502_131502_9b54`，节点为 `rdagent-node1`，状态为 completed。任务配置中的 Loop1 与 Loop2 除 HMM 开关、模型版本、preset、label 和 loop index 外，其余核心配置相同：

| 字段 | 冻结值 |
|---|---|
| label horizon | 10D |
| stock pool | `filtered_pool_20260502` |
| model | `__seed_LGBModel_conservative_v1__` |
| factor count | 57 |
| strategy | `score_weighted_topk_v2` |
| execution | `V25_TWO_STAGE` |
| 历史完整结果区间 | 2024-07-01..2026-04-27，442 行 |

Loop1 无 HMM 的 `signals.parquet` 为 22,150 行，SHA-256 为 `700152f27453374a28c8c4bf13de805557515423f46c7f83f717bfaa731f3e55`，只作为历史结果核对。正式 prediction replay 不使用该派生 signal 文件。

权威 workspace catalog 中，Loop1 含 5 个 `*/artifacts/pred.pkl`，不满足现有 replay 的唯一 artifact cardinality，禁止人工挑选。Loop2 恰有一个 Qlib 原始模型 prediction artifact：

```text
source_task_id=qe_20260502_131502_9b54
source_loop_index=2
relative_path=mlruns/832588134649973354/1dc13cb7dc804ba7ad90a8ae9e3ca536/artifacts/pred.pkl
rows=2,045,269
range=2024-07-01..2026-04-28
columns=[score]
sha256=0957ae8a6527fb28ba337a449ce0f72dfe9f43513003492329d7e770aa9da8e2
```

HMM 调整发生在 strategy 阶段，`pred.pkl` 是调整前模型输出；因此三个 arm 统一使用现有 `prediction_replay=true`、上述 source task/loop/hash，不重训 Alpha，也不从 Loop2 的已调整 `signals.parquet` 反推 raw score。

### 2.2 历史旧 HMM 锚点

| 字段 | 冻结值 |
|---|---|
| snapshot id | `bbec3863-fb67-445f-938e-66f092d18696` |
| config id | `b99c907b-873a-4173-a4ee-5eab266f8c49` |
| display | `HMM_COVFIX_w3_raw_same_params__n3_diag_rw3_nozscore` |
| sector level | L2，131 sectors |
| train | 2022-01-01..2024-06-30 |
| validation | 2024-07-01..2025-03-31 |
| model SHA-256 | `1b2179f3267c441c99fcdf7b514272991007f28e196e8b835b2f00c67644bf63` |
| coefficient artifact SHA-256 | `28d65424c3983256d0d4c6bb27c02dfd8e91830fdba26bc5791be75f74e96a03` |
| 10D preset A | fading=0.98，neutral=1.00，trending=1.02 |

旧 arm 必须使用上述既有 snapshot/preset 和旧 consumer 语义，不重写旧 artifact，也不把其静态 5,847 股票映射升级后冒充同一旧版本。

### 2.3 当前 v1.6 锚点

| 字段 | 冻结值 |
|---|---|
| model contract | `hmm_risk_rotation_l1_g2a_v1_6` |
| model hash | `3956107600a3aef4b51ac1da0c56f7940ce49a34777c836e974d14a5b45fbee6` |
| mapping hash | `e478722f700535ac4e37744a651291bc6d179cb899dccd28cdb957ed4491b82f` |
| validation basis | `development_causal_oof` |
| tail accessed | false |
| 可比较区间 | 2024-07-02..2026-03-31 |
| 覆盖 | 423 dates × 31 L1 sectors = 13,113 rows |
| state counts | fading=2,961，neutral=7,191，trending=2,961 |

v1.6 是确定性 L1 资金流排序，不得称为旧 L2 HMM 的新训练版本。它在本设计中只是“新版 HMM 模块辅助候选”的冻结输入。

### 2.4 历史效果不是本次验收结论

历史完整窗口记录为：

| arm | annualized | IR | max drawdown | avg turnover |
|---|---:|---:|---:|---:|
| no HMM | 0.4621167522 | 1.9942392048 | -0.1658079910 | 0.083869 |
| old HMM | 0.4756167704 | 2.0645301081 | -0.1558938681 | 0.084421 |

但 Loop1/Loop2 的 22,150 条 signal 在 instrument/date/rank/target weight/target position 上相同，只有 3,076 条 score/signal 数值不同；两 arm 的日 return 却有 441 行差异，而 turnover/cost 完全相同。因此历史记录只能证明“曾观察到正向差异”，不能证明差异由 HMM 单独造成。本设计必须用同一 raw prediction 和同一回放运行身份重新闭合归因。

### 2.5 C-013 完整可执行面板预检

在零训练、零 QE 回放、零数据库写入的 fresh-process 只读预检中，使用 D1 唯一 Loop2 `pred.pkl`、冻结共同窗口和正式 v1.6 full C-013 authority，得到：

| 项目 | 只读结果 |
|---|---:|
| 原始 prediction file SHA-256 | `0957ae8a6527fb28ba337a449ce0f72dfe9f43513003492329d7e770aa9da8e2`，与 D1 一致 |
| authority bundle / mapping manifest | `203effb611d00edde4c0ee9c40f205759097628b8c5eb249907f3b33e6932ddf` / `e478722f700535ac4e37744a651291bc6d179cb899dccd28cdb957ed4491b82f` |
| 正式 source rows / unique executable keys | 1,951,448 / 1,951,448 |
| source dates / trade dates | 423 / 423 |
| source next-date vs historical Loop2 signal trade calendar | 423/423 dates，exact set equal |
| unique stocks | 4,684（SH 2,115；SZ 2,569） |
| C-013 resolved keys | 1,780,359 |
| unresolved keys / stocks | 171,089 / 476 |
| unresolved primary reason | 171,089 个全部为 `classification_authority_unavailable` |
| index-membership fallback readback | 0 个 resolved；全部为 `membership_boundary_unavailable` |
| affected trade dates | 423/423；每日 unavailable 330..476，median 409 |
| daily mapping coverage | min 89.8377%，median 91.1434%，max 92.7297% |
| raw Top50 impact | 422/21,150 rows；296/423 dates；37 stocks |
| duplicate executable keys | 0 |

正式 authority 的 full-denominator receipt 自身为 passed，因为其合同要求每个键明确归类为 resolved 或 causal unavailable，而不是伪造 100% 行业覆盖。因此当前问题不是 asset 缺失、HMM fit、tail、停牌或个别脏行，而是 QE assistance 如何处理“权威明确不可用”的业务语义尚未批准。不得把 476 只股票排除、回落 index membership、使用当前行业或缩短窗口来伪造闭合。

1,951,448 的分母不是把无关历史数据扩成门禁：当前正式 consumer 在 TopK 排序和权重计算之前对当日全部 normalized score 调用 HMM adjustment，因此每一条 score 都可能改变排序、持仓或权重，均属于 executable panel。只验证 TopK 或事后入选股票会产生选择后缺口并改变原策略语义。

## 3. 架构与 owner 边界

```text
Loop2 unique immutable raw pred.pkl + frozen QE scenario
                         |
          +--------------+--------------+
          |              |              |
       no HMM        old L2 HMM      new v1.6 L1 adapter
          |          existing asset    PIT maps by date
          |              |              |
          +--------------+--------------+
                         |
             same QE strategy/execution/data/cost
                         |
              three-arm result comparison
```

- HMM owner：新版 coefficient artifact schema、状态到系数公式、v1.6/mapping/source identity、HMM-owned builder/validator/CLI 和直接测试。
- RD-Agent consumer owner：策略模板读取 `stock_sector_applicability_by_date`、按日显式分派 applied/not-applicable、对真正缺失/重复/未知/非有限 fail closed 和对应测试。
- QE owner：只提供冻结场景的正式重放入口与结果；本 HMM 任务不修改 QE 平台源码。
- consumer PR 与 HMM adapter PR 均通过后才可执行三臂回放；任一缺失不得以本地 monkeypatch、复制 workspace 或手改策略文件绕过。

## 4. D1：冻结场景与回放窗口（USER_APPROVED_20260916）

正式窗口固定为 `2024-07-02..2026-03-31`：

- 起点为 v1.6 与历史 QE 第一个实际交易日的共同起点；
- 终点严格早于 2026-04-01 sealed tail；
- 三个 arm 使用完全相同的 canonical trading dates；
- 不允许用旧结果的 2026-04-01..2026-04-27 部分补齐新版；
- 不允许因个别日期缺失缩短窗口。输入不完整时整个正式运行 typed blocked。

三个 arm 必须共享 Loop2 唯一 raw `pred.pkl`、stock pool、model/factors、strategy、execution、cost、daily/minute input identity、RD-Agent consumer commit 和 environment identity。三个 arm 在同一新版 consumer build 中分别走 disabled、legacy-static 和 new-PIT 三个显式模式；只允许 HMM assist artifact/mode identity 不同。不得让旧 arm 继续运行旧 consumer、新 arm 运行新 consumer 后再比较，否则代码身份差异会破坏归因。legacy-static 模式必须以回归测试证明与当前旧策略逐行等价。

## 4.1 Contracts / 合同总览

正式合同由 D1 冻结场景、D2 符号安全公式、D3 PIT artifact、D4 consumer、D5 两级评估和 D6 裁决共同构成。任何一项未批准、未实现或 D3 authority/coverage 未闭合时，adapter 实施与三臂正式回放均不得开始；不得只取其中可运行部分作为简化版实验。三个 arm 均固定 `prediction_replay=true`、source task `qe_20260502_131502_9b54`、source loop `2` 和 prediction SHA-256 `0957ae8a6527fb28ba337a449ce0f72dfe9f43513003492329d7e770aa9da8e2`；source artifact cardinality、size 与 hash 任一漂移均 fail closed。

## 5. D2：新版 adapter 精确公式（USER_APPROVED_20260916，方案 A）

### 5.1 候选 A：符号安全的幅度调整（唯一推荐）

令原始 QE score 为有限实数 `x`，v1.6 state 的方向为：

```text
direction(fading)  = -1
direction(neutral) =  0
direction(trending)= +1
coefficient(fading/neutral/trending) = 0.98 / 1.00 / 1.02
adjusted_score = x + (coefficient(state) - 1.0) * abs(x)
```

性质：

- 正 score 下与旧 preset A 的 0.98/1.00/1.02 乘法一致；
- 负 score 下仍保证 fading 下调、trending 上调，不发生语义反转；
- `x=0` 保持 0，不另加常数或 scale floor；
- 不裁剪、不归一化、不搜索 amplitude；
- 该公式只调整 QE 排序输入，不改变 v1.6 的 rotation score/state，也不声明 0.02 是重新优化得到的最优值。

### 5.2 明确否决的替代

- **直接乘法** `x×0.98/1.00/1.02`：负 score 时方向反转，禁止用于新版。
- **rank-space 固定加减**：需要新增 rank shift 尺度，当前没有批准依据。
- **连续 rotation score 校准**：会新增校准函数和自由度，不属于本次单候选。

## 6. D3：PIT 映射与 artifact 合同（USER_APPROVED_20260916，方案 B）

新版 artifact schema 固定为 `hmm_risk_qe_assistance_coefficients_v1`，至少包含：

- `source_model_contract/model_hash`；
- `source_mapping_sha256`；
- `source_prediction_row_sha256`；
- `window_start/window_end`；
- `daily_coefficients[date][L1 sector]`，值严格为 0.98/1.00/1.02；
- `stock_sector_applicability_by_date[date][stock]`：覆盖全部 executable keys；每个值含 `status=applied|not_applicable_authority_unavailable`、`sector_code`（仅 applied 非空）、`reason_code`（仅 not-applicable 非空）和 authority source row hashes；
- `adapter_formula` canonical text/hash；
- `adjustment_mode=sign_safe_magnitude_v1`，consumer 必须按 D2 公式解释 coefficient；
- `tail_accessed=false`；
- 输入/输出 row count、date count、sector denominator 和 canonical artifact hash。

PIT map 的构建只能复用现有 `HMMIndustryPitAdapter`：从与 v1.6 `source_mapping_sha256=e478...` 精确一致的 immutable C-013 authority 创建 adapter，绑定 research-basis、L1 和 L2 projection，然后对正式 raw prediction 可执行面板中的每一个 `(trade_date, stock)` 调用 `resolve(stock, trade_date)`。resolver 必须对每个键返回可区分的 resolved identity 或 typed causal unavailable；任何缺行、异常、未知 reason 或重复仍是失败。无需新增数据库查询、数据准备逻辑或第二套行业解析器。`mapping_manifest(...)` 的 canonical hash、C-013 bundle hash、classification/index-membership receipt hashes 和实际参与构建的 source row hashes 必须进入 artifact identity。

日期语义固定为：`pred.pkl` 的索引日期是 prediction/source date `s`；冻结策略以 shift=1 在 canonical QE calendar 的下一个交易日 `t=next_trade_date(s)` 执行该 score，并在 `trade_date=t` 调用 HMM adjustment。v1.6 的 `t` 行只使用截至 `t-1` 的输入，因此 coefficient 与 PIT mapping 均按执行日 `t` 取值。不得把 source date `s` 直接当作执行日，不得再次把 v1.6 行左移，也不得使用 `t` 日收盘后信息。正式 builder 只构造执行日 `t` 位于 `2024-07-02..2026-03-31` 的 raw rows并读取同日 v1.6 OOF rows；任何执行日进入 `2026-04-01` 起 sealed tail 均立即拒绝，不得截断后继续。

canonical source panel identity 分两层持久化：原始 Loop2 `pred.pkl` 文件 SHA-256 固定为 D1 的值；共同窗口内按 `(source_date, trade_date, instrument, float64 score)` 排序后的 canonical row SHA-256 固定写入 `source_prediction_row_sha256`，并绑定 frozen QE calendar identity。`stock_sector_applicability_by_date` 只包含该共同窗口 executable raw panel 实际出现的股票，且每个 trade date 的 key 集必须与当日 executable raw panel 精确相等；resolved 行由其中的 `sector_code` 提供 PIT 映射，不再建立可漂移的第二份 map。`daily_coefficients` 每日必须恰有 31 个 canonical L1 sector。输入行顺序不得改变这些 canonical identities。

每个正式 score 行必须满足：source date 可唯一映射到完整 423 日共同执行日历中的下一交易日、股票/执行日键唯一、authority 返回受支持的 resolved 或 causal-unavailable 结果、resolved L1 执行日有唯一 v1.6 state、raw score 与应用后的 score 有限。任一未被 D3 明确批准的结果均中止整个 arm 并保留 typed reason；禁止默认 1.0、默认 neutral、静态最新行业、当前上市股票池、L2 近似、前填、跳过股票或缩短共同窗口。

### 6.1 D3 applicability 精确决策（USER_APPROVED_20260916）

**方案 A：100% resolved gate。** 任一 causal unavailable 都阻断整个新版 arm。该方案最严格，但 exact authority 已证明 423/423 日均有 unavailable，因而当前三臂实验确定不可执行；除非未来出现新的 PIT 证据并重新冻结 authority，否则只会永久停在 `BLOCKED_CONTRACT`。不得为了通过而补当前行业。

**方案 B：explicit non-applicable（已批准）。** artifact 对全部 1,951,448 个 executable keys 逐行持久化 applicability：

- `applied`：authority resolved 唯一 canonical L1，按 D2 调整；
- `not_applicable_authority_unavailable`：authority 原样返回已批准的 `classification_authority_unavailable`，该行不执行 HMM adjustment，`adjusted_score` 必须与 raw score float64 bitwise 相同；
- 任何缺行、未知 reason、identity/receipt 漂移、重复、resolved 后缺 state/coefficient 或非有限值：整个新版 arm fail closed。

方案 B 的“不调整”不是 neutral state，也不是查不到时默认系数 1.0：artifact 必须存在该键、原始 authority reason、source row hashes 和 `adjustment_applied=false`，consumer 必须显式识别该状态。结果报告必须给出全窗/逐日 applied 与 not-applicable 分母、Top50 影响以及两类股票的持仓/成交差异；不得只报告 applied 子集。该方案保持原始 QE 股票池和 raw score，不利用未来行业信息，并与“行业辅助只作用于有因果行业身份的股票”这一边界一致。

**方案 C：限制到 resolved universe。** 从三臂共同输入中删除 476 只股票或逐日删行。该方案改变冻结股票池、候选排序和基线业务逻辑，否决。

方案 B 对 D4 的非放宽式澄清已获批准：完整 artifact 中显式 `not_applicable_authority_unavailable` 不是 missing；真正缺 key、缺 reason 或未知 reason 仍按 D4 fail closed。

## 7. D4：consumer 合同缺口（USER_APPROVED_20260916，D3-B 澄清已批准）

当前 RD-Agent `score_weighted_strategy.py` 只要求并读取静态 `stock_sector_map`，循环中仅在映射和 coefficient 同时存在时调整；缺失时静默保留原 score。AIstock composer 虽接受 `stock_sector_map_by_date`，但 consumer 未消费它。

最小修复必须：

1. artifact 二选一：旧 schema 使用静态 map；新 schema 必须使用 by-date applicability，不得同时存在或隐式优先；
2. 每个 trade date 精确读取当日 applicability；
3. 新 schema 下缺 date/stock entry、unknown status/reason、applied 行缺 sector/coefficient、非有限值或重复 identity 时抛出 typed error；若 D3-B 获批，只有完整 entry 的 `not_applicable_authority_unavailable` 可不调整；
4. new-PIT 模式必须读回 signal 实际 source date，并要求它严格等于 frozen QE calendar 中 `trade_date=t` 的前一交易日 `s`；不得沿用 `_normalize_signal_scores()` 的“目标日期缺失时取最近日期”作为成功路径；
5. HMM disabled 时保持原行为；旧 snapshot 静态 map 行为不变；
6. 不新增 default map、数据库查询、latest model 或兼容代理；
7. 使用独立 RD-Agent worktree/branch/PR，禁止修改历史 workspace 文件。

三个 arm 必须由同一 consumer commit 执行：disabled 模式不得加载 HMM artifact；legacy-static 模式保持旧 schema、静态 map、直接 multiplier 及既有缺失行为逐行不变；new-PIT 模式才启用 D2、按日 mapping、source/trade date 闭合与严格 fail-closed。未知 schema/mode 不能猜测为 legacy。RD-Agent 源码合入、部署到目标节点、目标节点代码 commit/readback 和可能的重启是彼此独立状态；是否需要重启由 RD-Agent 工作流按实际 runtime impact 判定，且任何进程控制仍需用户明确授权。本设计本身不授权部署或重启。三臂实验开始前必须读回目标节点 consumer commit 与批准 commit 一致。

## 8. D5：两级评估与指标（USER_APPROVED_20260916）

### 8.1 Phase 1 只读诊断

在读取 tail 前先最小扩展现有 `hmm_evolution.evaluator`：旧 artifact 继续使用 legacy multiplier；只有 schema=`hmm_risk_qe_assistance_coefficients_v1` 且 `adjustment_mode=sign_safe_magnitude_v1` 时使用 D2 公式。两种模式必须显式分派，未知或缺失 mode fail closed，不得让新版落入旧乘法。扩展后的 raw/adjusted TopK 能力报告：

- changed TopK dates/count；
- adjusted-score changed rows/dates、rank changed rows/dates；
- entered/dropped stocks；
- raw/adjusted forward outcome 差异；
- applied/not-applicable 全窗与逐日分母、raw TopK 中 not-applicable 行数；
- 缺失/异常为 0；
- adapter artifact identity。

该诊断只验证新版确实改变了 consumer 输入及方向是否合理，不是 QE 正式收益结论。只有在全部共同 score 行都满足 `adjusted_score == raw_score`（float64 bitwise 相同）时，才终止为 `NO_CONSUMER_EFFECT`。TopK 或 rank 不变不能触发早停，因为 score 幅度仍可能影响权重和后续交易。

### 8.2 正式三臂 QE 回放

三臂通过现有 QE prediction replay 在同一冻结运行入口中分别执行，不重训 Alpha；并验证 source prediction hash、executable prediction panel hash、环境、策略、执行、数据和成本 identity 相同。每个 arm 报告：

- cost-after annualized return；
- exact-window cost-after compounded return：`prod(1 + daily_return) - 1`；
- information ratio；
- maximum drawdown；
- average turnover；
- total/average cost drag；
- daily holdings、orders、fills 的 hash 与相对 no-HMM 的 changed counts；
- completed dates、first/last trade date 和失败 reason。

比较同时给出 `new - no_HMM`、`new - old_HMM`、`old_HMM - no_HMM`。唯一主比较值为共同 423 日的 cost-after compounded return；差值使用 float64 计算，绝对值 `<=1e-12` 记为 numeric tie。年化收益、IR、最大回撤、换手、成本和持仓/成交变化都是解释性指标，不组成额外 AND 门。不因单一指标更高自动切换默认版本。

结果只作历史场景观察：

- `new - no_HMM > 1e-12`：`BENEFIT_VS_NO_HMM_OBSERVED`；
- `new - old_HMM > 1e-12`：同时记录 `SUPERIOR_TO_OLD_HMM_OBSERVED`；
- 只优于 no-HMM 而不优于 old-HMM：记录新版有辅助价值但未胜旧版；
- 两项均不为正：`NO_INCREMENTAL_BENEFIT`；
- 任一 numeric tie 如实记录，不向上取胜。

这些状态不等于统计显著、跨场景有效、可用于实盘或默认版本切换。

## 9. D6：结果裁决与停止条件（USER_APPROVED_20260916）

| 条件 | 终态 | 后续 |
|---|---|---|
| 资产/identity/PIT/consumer 任一不闭合 | `BLOCKED_CONTRACT` | 修复同一合同；不得跑近似实验 |
| 新版全部 adjusted score 与 raw score bitwise 相同 | `NO_CONSUMER_EFFECT` | 保留旧版，不运行正式回放 |
| 新版运行失败或结果非有限 | `NEW_ASSISTANCE_NOT_AVAILABLE` | 保留无 HMM/旧版；不自动开第二 candidate |
| 新版相对 no-HMM 的共同窗复合收益差不为正 | `NO_INCREMENTAL_BENEFIT` | 保留 v1.6 独立预测和旧 QE 选项，不接入新版 |
| 新版相对 no-HMM 的共同窗复合收益差严格为正 | `BENEFIT_VS_NO_HMM_OBSERVED` | 同时报告是否优于 old-HMM；仅形成历史场景候选，是否进入其他场景或 runtime 另行批准 |

正式运行结束后不得依据结果修改 0.02、窗口、mapping、策略、成本或指标再跑一轮。荐股和模拟盘不由本次结果自动继承。

### 9.1 Typed reason contract

| reason code | 精确触发 |
|---|---|
| `hmm_risk_qe_assistance_input_invalid` | raw prediction schema、窗口、日期/股票唯一性或 finite score 不成立 |
| `hmm_risk_qe_assistance_authority_identity_mismatch` | C-013 asset 的 canonical mapping/receipt identity 不等于 v1.6 冻结的 `e478...` identity |
| `hmm_risk_qe_assistance_pit_mapping_missing` | 任一正式 `(trade_date, stock)` 缺 applicability entry、出现未知/不受支持 reason、重复，或 resolved identity 逃逸 31 L1 projection |
| `hmm_risk_qe_assistance_state_missing` | 任一正式日期缺少 31-sector v1.6 state 或某 L1 state 不唯一/非法 |
| `hmm_risk_qe_assistance_formula_invalid` | coefficient、mode、formula hash 或 adjusted score 非有限/不符合 D2 |
| `hmm_risk_qe_assistance_prediction_replay_identity_mismatch` | source task/loop/cardinality/file hash/row hash 任一漂移 |
| `hmm_risk_qe_assistance_signal_calendar_mismatch` | new-PIT consumer 实际 source date 不等于 frozen QE calendar 中执行日的前一交易日，或触发最近日期 fallback |
| `hmm_risk_qe_assistance_consumer_contract_unsupported` | 目标 consumer 不支持 new schema/by-date map、代码 identity 不一致或错误地回落 legacy |
| `hmm_risk_qe_assistance_no_consumer_effect` | 全部共同 score 行的 adjusted score 与 raw score float64 bitwise 相同 |

这些 reason 均为失败或诚实无作用状态，不得捕获后改写成 legacy success、neutral、空 artifact 或成功 ACK。底层异常可作为 cause 持久化，但不能覆盖 primary reason。

若 D3-B 获批，`not_applicable_authority_unavailable` 是逐行 applicability 状态而非上述失败 reason；它必须计入完整分母并保持 raw score，不能被消费端日志或汇总省略。

## 10. Implementation Plan / 实施方案

D1-D6 已获批，只按以下三个连续步骤实施，不拆成平台项目：

1. 在 AIstock HMM-owned 模块新增一个纯函数 adapter/builder 和只读 CLI，消费冻结 v1.6 rows、exact `e478...` C-013 `HMMIndustryPitAdapter` 与唯一 Loop2 raw prediction identity，输出包含全行 applicability 的 canonical coefficient artifact；以显式 mode 最小扩展 Phase 1 evaluator，不修改 repository/API/UI，也不复制 PIT resolver。
2. 由 RD-Agent consumer owner 在独立 worktree/PR 最小修改当前正式 strategy template，支持新 schema 的 by-date applicability 与 fail-closed；旧 static schema 回归不变。
3. 两端源码合入后，先运行 Phase 1 changed-TopK；有真实 consumer effect 才在同一冻结 QE 场景执行三臂正式回放并生成一个结果报告。

不创建通用 adapter registry、任务调度器或第二个候选。exact authority 及其 171,089 个 causal-unavailable keys 已完成预检；源码只允许实施已批准 D3-B，不得保留运行时 A/B/C 选择开关。

## 11. Verification Plan / 验证计划

### 11.1 HMM-owned adapter

- v1.6 state 生成 0.98/1.00/1.02；
- 正/负/零 raw score 的符号安全公式；
- PIT map 按日期变化；
- source date `s` 通过 frozen QE calendar 唯一映射到下一交易日 `t`，并精确绑定 v1.6 `t` 行与 PIT `t` 行，且证明 v1.6 输入截止 `t-1`；
- 每日 map key 与当日 raw prediction stock key 精确相等，daily coefficients 每日恰为 31 L1；
- missing date/stock applicability、unknown status/reason、applied 行缺 sector/state、duplicate、non-finite、identity/hash drift fail closed；
- tail date 拒绝；
- 输入行顺序不改变 canonical artifact；
- 同一输入两个 fresh processes 输出 bitwise canonical 相同；
- 禁止静态 latest map、默认 1.0、L2/数据库 fallback。
- prediction replay source 必须为精确 task/loop/hash，多个或缺失 `pred.pkl` 均拒绝。
- 现有 C-013 adapter/manifest identity 被复用且持久化，不新增数据库或静态 latest map 读取。
- exact v1.6 C-013 resolver identity 为 `e478...`，并对 1,951,448 个 executable keys 达到 100% explicit outcomes；若批准 B，则必须精确复现 1,780,359 applied 与 171,089 typed not-applicable，输入行重排不改变分母/hash。
- causal unavailable、真正 missing、未知 reason、重复和 identity drift 的状态必须严格区分；只有批准的 explicit not-applicable 可以保持 raw score。

### 11.2 RD-Agent consumer-owner contract

- old static artifact 兼容行为不变；
- disabled/legacy-static/new-PIT 三种模式在同一 consumer commit 中显式分派；
- new by-date artifact 精确使用当日 applicability；resolved 行使用当日 sector，批准的 not-applicable 行保持 raw score 并计数；
- new-PIT 实际 signal source date 必须等于 frozen QE calendar 的 `previous_trade_date(t)`；缺日不得回退最近 signal；
- new schema 缺失映射不再静默成功；
- HMM disabled 不加载 artifact；
- 不同 date map 能产生预期 adjusted score；
- 同输入同配置可复现。
- 目标节点 consumer commit readback 与批准 commit 一致；部署、runtime effect 与重启状态分别报告。

### 11.3 正式回放前门禁

- changed files → ownership → module registry → test plans；
- HMM 定向 pytest、Ruff、py_compile、`git diff --check`；
- RD-Agent 精确模板测试；
- AIstock `validation_module_registry_l0`、`hmm_evolution_backend`/`hmm_risk_backend` 中直接受影响计划、`l0`；
- `python scripts/aistock_feature_workflow.py validate --design <本文件> --tier F2`；
- DESIGN-COMPLIANCE-001 逐项审核。

不因文档或单元测试通过而标记三臂实验完成。

## 12. DESIGN-COMPLIANCE-001

| 检查项 | 设计结论 |
|---|---|
| 禁止简化、subset、POC | 三臂、同场景、同原始预测和正式 QE 回放均为必要项；Phase 1 诊断不冒充结果 |
| 禁止静默错误 | 新 schema 对 date/stock/sector/state/coefficient/identity 全部 fail closed，不允许默认系数 |
| 禁止业务逻辑迁移 | 旧 QE HMM、v1.6 独立预测和新版 QE adapter 三个身份分离；不自动替换旧配置，不外推其他场景 |
| 禁止私增门禁/审批 | D1/D2/D4-D6 已按用户批准记录；D3 applicability 明确保留 pending，不由实现自行裁决；不新增人工运行门 |

## 13. Design Acceptance Index

| ID | 验收条款 |
|---|---|
| F-001 | 三臂共享同一 raw Alpha prediction、场景、数据、策略、执行、成本和共同日期 |
| F-002 | 旧 snapshot/preset/静态映射保持历史原样，不被新版覆盖或伪装升级 |
| F-003 | 新版公式对正、负、零 score 均保持 trending 上调、fading 下调 |
| F-004 | 新版 artifact 绑定 v1.6 model、exact `e478...` C-013 mapping/receipts/source row hashes、raw source file/row identity、日期和 formula hash |
| F-005 | PIT stock-sector applicability 按执行日覆盖全部 1,951,448 个 executable keys；resolved 与 causal unavailable 显式区分，真正缺失/重复/未知/非有限均 fail closed |
| F-006 | Phase 1 adjusted-score/rank/TopK 只作执行前诊断，只有全 score 无变化才早停，不冒充正式 QE 收益 |
| F-007 | 三臂正式回放同时报告收益、IR、回撤、换手、成本及持仓/成交差异 |
| F-008 | 以共同窗成本后复合收益作唯一主比较；无作用、失败或无增益时停止，不自动调参、换候选或替换旧版本 |
| F-009 | HMM 与 consumer owner 分开实施；三 arm 共享同一 consumer commit；禁止手改 workspace 或跨模块绕过 |
| F-010 | 无 tail、DDL/DML、runtime、依赖和进程动作，DESIGN-COMPLIANCE-001 逐项成立 |

### 13.1 精确 Decision Index

| decision | 精确内容 | 状态 |
|---|---|---|
| QE-HMM-3ARM-D1 | 锚点场景、唯一 Loop2 raw prediction、2024-07-02..2026-03-31 三臂共同窗口 | `USER_APPROVED_20260916` |
| QE-HMM-3ARM-D2 | `raw + (coefficient(state)-1)×abs(raw)`，系数 0.98/1.00/1.02，禁止负值反向的直接乘法 | `USER_APPROVED_20260916` |
| QE-HMM-3ARM-D3 | source `s`→next trade date `t`；exact `e478...` authority；resolved 行应用 D2，显式 causal unavailable 行保持 raw 并完整记账；禁止删 universe | `USER_APPROVED_20260916_OPTION_B` |
| QE-HMM-3ARM-D4 | RD-Agent consumer 独立 PR；同一 commit 显式支持 disabled/legacy-static/new-PIT，new schema missing fail closed，旧行为不变；显式 not-applicable 不等于 missing | `USER_APPROVED_20260916_D3B_CLARIFICATION` |
| QE-HMM-3ARM-D5 | Phase 1 score/rank/TopK 前检；仅全 score 无变化早停；正式三臂以共同窗成本后复合收益为唯一主比较 | `USER_APPROVED_20260916` |
| QE-HMM-3ARM-D6 | typed 终态、单候选停止、不自动切换或外推其他场景 | `USER_APPROVED_20260916` |

D1-D6 已获批，允许分别实施 HMM adapter 与 RD-Agent consumer 修复。源码 PR 合入后仍需独立实验授权；本设计不授权 DDL/DML、tail、runtime 或进程控制。

## 14. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | proposed HMM three-arm request/validator; existing QE prediction replay | `backend/tests/unified_engine/test_qe_prediction_replay.py`; artifact:`qe_20260502_131502_9b54/Loop2/pred.pkl#sha256=0957ae8a6527fb28ba337a449ce0f72dfe9f43513003492329d7e770aa9da8e2` | design_ready_for_user_decision | - |
| F-002 | `backend/services/quantevolver/experiment_config_builders.py`; existing snapshot asset | `backend/tests/hmm_evolution/test_candidate_artifact.py`; artifact:`snapshot:bbec3863-fb67-445f-938e-66f092d18696` | design_ready_for_user_decision | - |
| F-003 | proposed HMM adapter pure function | `backend/tests/hmm_risk/test_qe_assistance_adapter.py::test_sign_safe_adjustment_handles_positive_negative_and_zero_scores` | design_ready_for_user_decision | - |
| F-004 | proposed canonical coefficient artifact builder | `backend/tests/hmm_risk/test_qe_assistance_adapter.py::test_artifact_binds_model_mapping_source_window_and_formula_hashes`; artifact:`bundle=203effb6..., mapping=e478722f...` | design_ready_for_user_decision | - |
| F-005 | proposed adapter plus RD-Agent consumer-owner change | `backend/tests/hmm_risk/test_qe_assistance_adapter.py::test_missing_pit_mapping_fails_closed`; preflight:`1,780,359 resolved + 171,089 causal unavailable = 1,951,448 explicit outcomes`; artifact:`rdagent-consumer-owner-test-required` | design_ready_for_user_decision | - |
| F-006 | `backend/services/hmm_evolution/evaluator.py`; proposed three-arm CLI | `backend/tests/hmm_evolution/test_evaluator.py`; `backend/tests/hmm_risk/test_qe_assistance_three_arm.py::test_only_zero_adjusted_score_change_stops_before_qe_replay` | design_ready_for_user_decision | - |
| F-007 | proposed three-arm result comparator | `backend/tests/hmm_risk/test_qe_assistance_three_arm.py::test_result_requires_all_three_arms_and_cost_after_metrics` | design_ready_for_user_decision | - |
| F-008 | proposed result state machine | `backend/tests/hmm_risk/test_qe_assistance_three_arm.py::test_compounded_return_is_only_primary_comparison_and_does_not_auto_select` | design_ready_for_user_decision | - |
| F-009 | §3 owner boundary and independent PRs | `backend/tests/hmm_risk/test_qe_assistance_adapter.py`; artifact:`owner-review-required-before-experiment` | design_ready_for_user_decision | - |
| F-010 | §12 and §17 production gates | `backend/tests/hmm_risk/test_qe_assistance_adapter.py`; `python scripts/aistock_feature_workflow.py validate --design docs/architecture/hmm_phase2_qe_assistance_three_arm_f2_detailed_design_20260916.md --tier F2` | design_ready_for_user_decision | - |

## 15. Rollout / Rollback / 发布与回滚

- Rollout：仅在 D1-D6、HMM adapter PR、RD-Agent consumer PR 分别获批并合入后执行一次冻结三臂历史回放；不触及 production runtime。
- Rollback：代码尚未进入 consumer 默认路径，回滚仅是不启用新显式版本；旧 snapshot 和 no-HMM 配置保持可用。不得删除历史 workspace、旧 model 或实验记录。
- 结果回退：新版无增益时保留 v1.6 独立预测与旧 QE HMM 可选项，不用空结果、neutral 或旧结果冒充新版成功。

## 16. Risks / 风险与失败模式

- **归因风险**：旧两 arm 曾出现相同 target intent 但不同 return；通过共用 raw source、环境和实际持仓/成交 hash 约束。
- **prediction artifact 歧义**：Loop1 有 5 个 `pred.pkl`；正式合同只使用 cardinality=1 的 Loop2 artifact 并固定 hash，禁止人工从 Loop1 选择。
- **静态/PIT 口径差异**：旧版保留静态映射只作为历史 arm，新版使用 PIT；结果需明确这是版本差异的一部分，不声称只比较模型结构。
- **负 score 方向反转**：新版禁止直接 multiplier，使用 D2 符号安全公式。
- **consumer 缺口**：当前策略不读取 by-date mapping；未完成 owner PR、节点部署/readback 不一致或需要但未获授权重启时 fail closed，不做 workspace patch。
- **C-013 causal unavailable**：exact v1.6 `e478...` authority 已定位，171,089 个 executable keys 是正式 causal unavailable；已批准 D3-B 显式不适用语义。禁止用默认行业、index-membership、neutral/1.0、删股票或缩窗绕过。
- **signal 日期回退风险**：当前 consumer 在目标 source date 缺失时会取最近 signal；new-PIT 必须拒绝该路径并持久化 calendar mismatch，避免重复使用旧预测。
- **效果过小**：只有全部 adjusted score 与 raw score 相同才提前终止；TopK 不变但 score 已变时仍执行正式回放，避免漏掉权重效应。
- **场景外推**：单一 QE 场景结果不自动外推荐股、模拟盘或所有市场阶段。

## 17. Production Gates / 生产门禁

| gate | 状态 | 说明 |
|---|---|---|
| production_ddl_gate | `noop` | 不修改 schema |
| production_dml_gate | `noop` | 不写数据库 |
| backend_dependency_gate | `noop` | 复用现有依赖 |
| frontend_dependency_gate | `noop` | 无前端范围 |
| runtime_activation_gate | `noop` | 不切换默认配置或模型 identity |
| service_restart_gate | `noop_user_owned` | 本任务不控制进程 |
| tail_access_gate | `forbidden` | 截止 2026-03-31，不读取 2026-04-01 起 tail |

## 18. 当前状态

- intended changed files：仅本设计文档（当前为 untracked，未提交）；
- approved decisions：D1、D2、D3-B、D4 及其澄清、D5、D6（2026-09-16）；
- D3 preflight：exact `e478...` authority；1,780,359 resolved + 171,089 causal unavailable = 1,951,448 explicit outcomes；476 stocks；
- adapter/consumer source：未实施；
- QE replay：未执行；
- training/fits：0；
- tail accessed：false；
- database write：0；
- `production_ddl_gate=noop`；
- `production_dml_gate=noop`；
- dependency gates：noop；
- runtime impact：none；
- backend restart required：false；
- merge/cleanup：未执行。
