# HMM 演进与风险管理系统总体蓝图（唯一产品目标权威）

> **版本**: v2.41
> **日期**: 2026-07-16  
> **修订日期**: 2026-09-04
> **状态**: Phase 0 已完成；Phase 1 全部外部验收完成（F-006～F-010A verified）且 production v3 已激活。Phase 2 的 P2-3A～P2-4、C-012-RL1/HR1/RW1均保持不可变终态；历史证据同时包含真实模型弱点与低功效，RW1 fold符号变化不足以单独证明关系时变。当前唯一G2-A仍为HMM/jump因果market context加浅层监督式L1横截面scorer。用户已批准：Rank IC 0.02为唯一binding MBE；MDE只决定forward confirmation；research product与tail access双门解耦；`min_child_samples=310/min_leaf_distinct_dates=20`；forward effect failure使用one-sided 95% HAC上界`<=0`；market context跨5D/10D共享。真实因果OOF可先闭合experimental repository/API/UI，但不得冒充rotation capability；只有development达到MBE才进入research prediction capability，只有forward通过才成为advisory。其余窗口、feature minimum、horizon差异、模型参数、coverage、依赖与DDL仍pending，源码/battery/fit/product/DB/runtime均未执行。严格产品进度仍为`11/17=64.71%`，CAPABILITY_AVAILABLE、FULL_READY和真实API/UI均为0。本次DDL/DML/dependency/runtime均为`noop`（§13）。
> **范围**: HMM 快速演进、风险监控、滚动训练、数据隔离  
> **作者**: Kiro (Claude Code)
> **维护者**: AIstock HMM Evolution

---

## 1. 执行摘要

### 1.0 权威、最终产品目标与反过度工程边界

- 本文件是 HMM Evolution Phase 0-3 的**唯一产品目标蓝图**。Phase 1/2/3 实现级详细设计只能展开本蓝图已经定义的产品结果、业务语义和验收项，不得反向新增产品目标、改变优先级或把诊断/证据基础设施升级为独立交付目标；发生冲突时先修订并经用户确认本蓝图，再同步从属详细设计。
- AIstock 唯一开发规范仍是 `docs/standards/aistock_development_standard_v1.5_20260523.md`；“唯一产品目标蓝图”不复制或替代开发规范。
- Phase 2终极产品目标固定为：基于因果可用的`t-1/PIT`数据生成申万板块未来相对强弱与风险预测，并通过真实日度prediction、repository/read API和`/hmm-risk`页面向用户交付。当前第一优先级只闭合完整`rotation_L1`纵切；它必须输出连续`rotation_score`、可解释方向、typed unavailable/coverage及真实L1热力图。`rotation_L2|risk_L1|risk_L2`继续独立`NOT_AVAILABLE`，不阻塞首个L1能力，也不得被L1结果冒充。
- Phase 2 的主要产品验收单位固定为“交易日 × L1/L2横截面”：模型必须证明其因果输出在未见数据上区分板块相对走强、走弱与风险，而不是要求每个sector在单一窗口内各自取得三态结构合格证。逐sector结构证据仍是语义可用性依据，但不得继续以全局合取垄断产品交付。
- Phase 2 使用四层验收：第一层为fit/convergence/covariance/posterior等数值安全；第二层为逐sector semantic availability；第三层为预注册walk-forward与untouched holdout上的横截面产品有效性；第四层为coverage及其行业/规模/流动性代表性。四层证据按`rotation_L1`、`rotation_L2`、`risk_L1`、`risk_L2`四个能力分别闭合；一个能力通过不能掩盖另一个能力失败，也不得用底层局部失败把其他已通过能力改写成全产品成功。
- canonical authority 固定为一个 versioned **product bundle**，而不是强制一个 estimator 同时承担市场regime、L1/L2轮动排序与低频风险事件分类。bundle可包含共享PIT/identity、market-regime component、rotation component、risk-alert component和availability manifest；每个component必须有独立模型身份、因果边界和验收结果，禁止隐式fallback或相互替代。
- 顶层状态严格分为：`FULL_READY`（四个批准能力均通过各自产品与coverage合同）、`CAPABILITY_AVAILABLE`（至少一个明确命名的能力达到其批准capability状态）、`NOT_AVAILABLE`（没有达到最低批准产品能力）。G2-A另使用五个正交字段：research surface、rotation capability、forward power、forward confirmation和advisory status。真实OOF工程链`AVAILABLE_EXPERIMENTAL`不能推导`CAPABILITY_AVAILABLE`；development达到binding MBE才允许research prediction capability，forward通过才允许advisory。
- `HISTORICAL_CAUSAL_WALK_FORWARD`表示按预注册切片、逐日t-1/PIT输入和事后outcome完成的历史因果回放，不得写成untouched。`forward_power_status=UNAVAILABLE|INSUFFICIENT|SUFFICIENT`与`forward_confirmation=NOT_STARTED|PENDING_INSUFFICIENT_POWER|PENDING_INCONCLUSIVE|PASSED|FAILED`独立记录；功效不足或功效公式不可用均不阻止真实experimental surface和唯一GBDT，也不得伪造`PASSED`。effect failure仅由实际tail one-sided 95% HAC上置信界`<=0`触发，样例数值不得硬编码。
- legacy与autocycle、per-sector restart及旧B3两family/四level合取仅保留为历史研究事实，不再规定新`rotation_L1`产品必须复刻该模型类、seed选择或完整性合同。新component只消费新F2预注册的input/model identity；旧family不得作为静默fallback、ensemble成员或失败后的第二candidate。
- P2-3A 已证明“按 contemporaneous `excess_return_Nd` centroid 为 pooled K=3 jump state 命名”不能在冻结开发 folds 上产生稳定正向 10D 轮动预测。后续 sector 产品标签必须直接表示冻结模型对未来相对走强/走弱的预测，不得把描述性 hidden-state index、当前强弱或简单反转旧标签冒充预测语义。连续 `rotation_score`、离散 `trending/neutral/fading` 与 market `risk_on/risk_off` 必须分别建模和报告。
- P2-4 已证明当前P2-3C candidate不能满足原“L1/L2 rotation与risk全部合取”的完整产品合同；该结论只终结此candidate/合同，不外推为所有模型不可能。已消费holdout仅可作为历史样本外证据，后续模型不得据此调参后再次使用同一窗口作为untouched验收。
- HMM fit、seed选择、hash、fresh-process一致性、manifest和acceptance receipt只是在无法替代的范围内证明模型可复现、可验收的手段，不是用户功能，也不构成Phase完成度。
- **Phase 2训练数据平面固定为“已有versioned数据资产 → 最小immutable input bundle → 单一预注册component执行”**。优先只读复用已准入的`daily_pv.h5`、`daily_basic.h5`、`moneyflow.h5`、`sector_data.h5`与Qlib日频Bin；它们不是单独的模型authority，必须与C-013 industry PIT、canonical security identity、provider-absence/停牌等typed missing语义、calendar/benchmark及各自source hash在一个bundle identity内闭合。若新F2预注册特征缺少必需字段或authority，preflight必须报告精确缺口并停止；不得静默回退数据库、当前行业映射、默认值、旧candidate或近似字段。
- 新G2-A为防止tail泄漏，允许一个development role和一个独立sealed tail role；每个role对同一冻结source/model contract最多形成一个canonical成功对象，并位于repo外不可变artifact root。tail只能在candidate、horizon、feature、参数与阈值全部冻结后构建/读取；失败或中断不得留下可消费bundle，重试使用新临时路径并在完整write/readback后原子终态化，绝不覆盖已有成功对象。bundle只保存因果构造后的日度L1 feature panel、calendar/benchmark、typed availability与最小lineage，不复制无关原始历史表，不扩展为通用dataset/evidence/training平台，也不增加产品完成度。input bundle与最终canonical product bundle身份分离，前者不能冒充模型或产品能力。
- input preflight、development-only battery、唯一candidate、真实OOF research surface、全新尾部确认和最终prediction/API/UI是同一G2-A业务闭环中的受控动作，不得拆成新的产品阶段。research product gate只闭合真实工程链；tail access另要求development OOF Rank IC达到0.02。任何surface、receipt或模型文件都不得单独增加capability完成度。
- fresh process表示独立Python进程、独立数值环境校验、独立rolling fold切片/preprocess/fit和独立结果hash，不表示重新查询数据库或重建多年PIT/聚合面板。每个process必须只读回读同一immutable bundle；rolling长度、purge/embargo、horizon、模型参数和fit上限由新F2一次冻结，禁止跨process共享可变模型状态、结果后调参或扩大grid。
- 允许新增的持久化仅限：上述development/tail两个最小隔离role、最终模型/产品bundle、最小身份manifest、紧凑battery/acceptance/failure receipt，以及产品需要的日度预测。G2-A不提前保存预警、事件或扩展回溯报告；这些属于后续G2-B。默认禁止按fresh process重复物化或重算完整历史输入、为相同模型生成多份大体积JSON、建设通用evidence平台、通用训练平台或Phase 3调度器。
- 已存在的大体积历史artifact保持只读、不可改写；不为“清理历史”开启迁移工程，也不再把它们复制到新的artifact或数据库。
- 后续每个任务必须直接映射到F-011（模型可验收）、F-012（advisory-only生成）或F-013（预测/预警/API/UI/报告）之一。不能直接推动三者、也不是修复其真实blocker的任务，优先级为停止，不进入实现。
- **交付粒度固定为端到端业务闭环，不再把小功能设计成独立阶段**：同一闭环中的详细设计回填、源码、直接测试、601日预检、同合同程序缺陷修复、正式审核和状态同步必须在同一任务范围连续完成，不得分别创建“schema阶段”“adapter阶段”“预检阶段”“后端阶段”“API阶段”或“UI阶段”。代码合入、实验执行、生产动作和进程控制仍因授权边界分别报告，但它们只是同一业务闭环的受控动作，不是新增蓝图阶段。只有模型合同变化、生产DDL/DML或依赖、无法由一个owner安全修改的模块边界、或者当前scope无法修复的独立缺陷，才允许停止并拆出新任务；拆分必须说明其对F-011/F-012/F-013的直接必要性。

### 1.1 背景与动机

HMM 板块轮动模型自 2026-04-04 修复以来，已有 2 个生产可用版本和 18 个实验候选。但当前研发流程存在以下瓶颈：

1. **验证成本过高**: 每次 HMM 改进需完整 QE 回测（6-12 小时），资源占用重
2. **串行瓶颈**: 一次只能验证 1-2 个版本，迭代速度慢（1 天 1-2 轮）
3. **反馈滞后**: 问题在 QE 结束后才暴露，无法快速定位
4. **风险管理缺失**: HMM 风险门控已实现但未暴露给用户，无预警机制
5. **数据混用风险**: 研发使用回测数据，但真实场景需连接 t-1 行情库

### 1.2 核心目标

**Phase 0-3 目标与当前状态**:
- **严格验收进度**：Design Acceptance Matrix共17个独立验收行（F-001～F-016，另含F-010A），其中F-001～F-010A共11行verified；F-011～F-016尚未达到完整验收，当前为`11/17=64.71%`。该比例只用于进度报告，不是研究或发布门禁；Phase 4+尚未独立设计，不进入分母。
- **Phase 2产品结果口径**：canonical产品模型=`0`，F-011产品验收未通过，F-013真实预测/预警/API/UI未交付；已完成的fits、diagnostic、receipt和历史B3源码只作决策证据。该口径与`11/17`并列报告，不新增研究或发布门禁。
- ✅ 离线快速评估：BUG-788 后 forward-return 重试 55.4 秒完成；Loop1～Loop10 同口径单例 evaluation 各 69.3～99.3 秒，均小于 10 分钟，partial label/market evidence 以 degraded warning 显式保留。
- ✅ 批量对比筛选：batch-relative scorer/top-3、有界并发共享输入和 durable retry 已实现；既有 10 候选 batch `hmmb_e2ac69e2e21a474e9044afa34a8f580b` 10/10 succeeded、约 12 分 37 秒，pre-ST-PIT 兼容 batch `hmmb_66db955297e6440283097e6fdfb927ac` 9/9 succeeded、约 24 分 4 秒；严格冷热缓存分阶段 timing/RSS receipt 已在 §17.4.6 收官验收完成。
- ✅ 自动评估执行：独立 Windows worker service 自动消费真实 durable queue；不创建任务、不训练 HMM、不接入 FastAPI 或 Phase 3 scheduler。进程中断验收确认 evaluation lease 过期后 fail-closed 为 `timed_out`，旧终态不原地复活，只有显式 `retry-failed` 创建新 generation；retry batch 2/2 succeeded。31.6 分钟 durable supervision soak 已完成，production v3 worker 于 2026-07-22 受控重启并通过 healthy poll 验证。
- ⬜ 风险预警可视化：每日板块风险预警 + 状态热力图由 Phase 2 交付。
- ⬜ 滚动训练自动化：研究候选滚动训练与时效性监控由 Phase 3 交付。
- ✅ 数据环境隔离：Phase 0 数据源、Phase 1 source manifest 与只读 market repository 已完成。

**Phase 4+ 目标** (待独立设计):
- 接入 QE 和模拟盘（需独立审批）
- 实盘自动调仓（需充分回测验证）

### 1.3 核心约束

1. **研发与生产数据隔离**: 
   - 研发/回测: 使用固定历史数据（pred.pkl, label.pkl）
   - 生产/模拟盘: 连接实时数据库（t-1 kline_daily_raw）
   
2. **不修改现有流程**: 
   - QE 实验和模拟盘的 HMM 配置保持不变
   - 研发确认有效后再考虑接入
   
3. **只做展示和分析**:
   - 前期不介入实盘/模拟盘自动操作
   - 风险预警仅为建议，决策由人工判断
   
4. **不污染项目目录**:
   - 遵循 AIstock 文档规范（`docs/architecture/`）
   - 临时文件放 `tmp/` 或 `.codex_tmp/`
   - 不新增裸 `.sql` 文件，使用 Python schema bootstrap

5. **禁止简化版交付**:
   - 禁止以 schema-only、backend-only、mock-only、静态页面、placeholder scorer 或 POC 代替完整设计项；
   - 分阶段 PR 只能报告自身设计子集完成，不得提前宣称整个 Phase 完成；
   - 禁止用静默 fallback、空集合、默认 neutral 或静态成功掩盖失败。

### 1.4 Scope（当前批准范围）

- Phase 0 hardening：修复数据源真实运行契约、artifact 可信缓存、测试与 CI 覆盖。
- Phase 1：只读消费 QE 全部实验资产与 canonical market 最新共同完成数据，在
  `hmm_evolution.*` 中保存候选、评估任务和结果；输出 top-3 **研究推荐**；独立 worker service 自动消费 API 已登记的 durable queue。
- Phase 2：在 `hmm_risk.*` 中保存日度状态、预警与解释证据；UI 只展示分析结论。
- Phase 3：在独立候选注册表中进行滚动训练编排和时效性监控；不修改现有 QE/Paper 配置和生产 snapshot 状态。
- 所有阶段均不得改变 Selection、Advisory、Paper v2、MiniQMT、StrategyPackage 或 QE 的既有业务语义。

### 1.5 Non-goals（非目标与硬边界）

- 不自动启用、替换、下架或推广任何生产 HMM。
- 不向 `model_train_configs`、`model_train_snapshots`、`strategy_packages`、`paper_v2.*` 写入数据。
- 不把风险预警接入 `can_buy`、订单、持仓、调仓或任何交易决策链。
- 不把 top-3、保护率、一致性阈值升级为未经批准的研究淘汰门禁或生产准入门禁。
- QE task/loop 的配置、日志、模型、报告、pred/label、coefficient 等资产均可只读查看和取证；
  但不得修改、删除、重跑、执行配置或把 unverified asset 直接作为计算输入。
- Phase 0 pred/label 自动反序列化白名单保持不变；Phase 1 使用独立 QE asset reader 扩展只读
  inspection 范围，不回改 Phase 0 已验收契约。
- Phase 4+ 的 QE/Paper/实盘接入不属于本文实施范围，必须另立 F2 设计和审批。

### 1.6 当前实现状态与进入 Phase 1 的前置条件

PR #2227（merge `5cae5861853bd4be4a07699fb332224c2bdf54c2`）只代表最初的
Phase 0 代码进入 main；BUG-688～BUG-692 和 #2285 已完成运行契约、canonical schema、
artifact/cache、CI 与 Prediction Store 零副本 hardening。2026-07-17 又以当前高收益
QE loop 和强制只读 DB transaction 完成外部 integration receipt。

Phase 1 开发前的四项前置条件现均已满足：

1. Prediction Store 零副本真实 smoke 与两种数据源的受控只读 DB/QE smoke 均通过，且无生产 DB 写入。
2. HMM 数据源相关单元测试进入专用 CI/nox plan，不再依赖无关 `qe_data_contract_backend`。
3. artifact manifest、路径边界、原子写、缓存生命周期和反序列化信任边界完成直接测试。
4. 本文 Design Acceptance Index 中 `F-001` 至 `F-005` 均有实现与证据。

Phase 1 当前进度：

- P1-A 已完成 QE 全资产只读 reader、candidate registry、Python schema bootstrap、durable
  repository/state machine 与生产 schema verify；runtime 默认关闭。
- P1-B 已由 PR #2373（merge `b7d59e6a0209d94c71b28ea58008973e43dc16d1`）实现
  versioned evaluator、input adapter、latest-common/交易日 market repository、durable executor 和
  `hmm_recommendation_v1` scorer；BUG-736/BUG-737 由 PR #2377（merge
  `8372d043afc1a47fc1c8cd341c70adccc2b236c9`）完成旧诊断唯一计算路径迁移。
- F-007/F-009 已验证；P1-C 的 worker CLI、真实 API/UI 源码和 BUG-742～BUG-748 审计硬化已完成；BUG-772/PR #2471 修复冻结 legacy ST-PIT runtime artifact 重放，BUG-788/BUG-789 后 Loop1～Loop10 同口径 API/worker receipt 已完成。BUG-800/BUG-801/BUG-804 后 pre-ST-PIT 兼容 batch `hmmb_66db955297e6440283097e6fdfb927ac` 9/9 succeeded，path-free provenance、worker 单项失败存活和 immutable artifact replay 均通过真实运行。
- 2026-07-21 受控中断 PID 73948 后，两个已运行 evaluation 在 90 秒 lease 到期时按详细设计进入 `timed_out`，剩余 evaluation 由新 worker PID 37024 完成；显式 retry batch `hmmb_9e1d0eaf43d1432bb1cbbbba53cca5b6` 只包含两个超时项并 2/2 succeeded。该语义是 fail-closed terminalization + explicit retry，不是自动重新认领同一 evaluation。2026-07-22 严格冷热缓存分段receipt、真实UI/Playwright 18场景和31.6分钟worker bounded soak均已完成外部验收。
- 2026-07-18 用户已批准将 Phase 1 worker 从“仅人工有限命令”扩展为显式 `--serve` 独立服务；2026-07-19 已在 Windows 显式启动并完成首次真实 queue receipt。该 activation 不包含 Phase 3 自动滚动训练调度，也不改变 QE/Paper/生产 snapshot 隔离边界。

---

## 2. 总体架构

### 2.1 系统分层

```
┌─────────────────────────────────────────────────────────────────┐
│                    前端 UI 层（三大模块）                           │
├─────────────────────────────────────────────────────────────────┤
│  HMM Evolution Lab    │  HMM Risk Monitor   │  Research Training │
│  - 快速评估            │  - 风险预警面板       │  - 滚动窗口预览     │
│  - 候选对比            │  - 板块状态热力图     │  - 时效性监控       │
│  - 批量测试            │  - 历史事件追踪       │  - 研究训练任务     │
└─────────────────────────────────────────────────────────────────┘
                                 ↓
┌─────────────────────────────────────────────────────────────────┐
│                    业务服务层（四大服务）                           │
├─────────────────────────────────────────────────────────────────┤
│  HMMEvolutionService        │  HMMRiskMonitorService            │
│  - offline_evaluate()       │  - generate_daily_alerts()        │
│  - batch_compare()          │  - get_sector_heatmap()           │
│  - recommend_top_candidates()│ - backtest_gate_effectiveness()  │
│                            │                                    │
│  HMMRollingTrainService     │  HMMDataSourceService            │
│  - plan_rolling_schedule()  │  - get_backtest_data()           │
│  - trigger_retrain()        │  - get_realtime_data()           │
│  - monitor_staleness()      │  - validate_data_freshness()     │
└─────────────────────────────────────────────────────────────────┘
                                 ↓
┌─────────────────────────────────────────────────────────────────┐
│                    数据层（环境隔离）                               │
├─────────────────────────────────────────────────────────────────┤
│  研发/回测环境              │  实时只读数据环境                     │
│  - Prediction Store blob   │  - market.kline_daily_raw         │
│  - QE 全资产只读 reader     │  - latest common completed date   │
│  - QE workspace fallback   │                                    │
│  - pred.pkl (固定)         │  - market.trading_calendar       │
│  - label.pkl (ground truth)│  - market.sw_index_member        │
│                            │                                    │
│  元数据存储                 │  资产注册表（现有）                  │
│  - hmm_evolution.*         │  - model_train_configs           │
│  - hmm_risk.*              │  - model_train_snapshots         │
│  - hmm_evolution.candidate │  - 现有配置/快照表仅作隔离边界      │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 HMM 研究工作台 UI 信息架构

2026-07-18 用户已确认最终 UI 方案：

- 统一研究工作台包含“演进实验室 / 板块风险 / 滚动训练”三个一级页签；
- 最终态主入口 `/hmm` 默认进入 `/hmm-risk` 板块风险热力图；
- 板块风险页采用状态热力图、今日预警、固定详情区和状态分布，不使用抽屉式列表；
- 新模块不得复用 `paper-v2.css`、`pv2-*` 或
  `frontend/src/components/paper-v2/*`；基础组件采用 shadcn-compatible tokens；
- 视觉采用已确认的浅色研究工作台：neutral surface、深绿色 primary；HMM state 使用
  trending=绿色、neutral=灰色、fading=琥珀色，HIGH/OPPORTUNITY 通过红色/青色 severity accent
  表达。正式实现必须使用语义 token，不散落页面级硬编码色值；
- 页面不得直接显示原始 JSON、manifest/spec/error dump。审计信息必须转换为中文指标、
  结构化键值、证据表和独立详情页；未知结构化资产只显示 metadata/trust/hash 和“不支持可视化”状态，
  不以 raw dump 兜底；
- 页面必须具备 loading、empty、degraded、failed、stale 和 terminal 状态。图表依赖加载失败、
  API 失败或数据缺失必须显示稳定 reason code、中文说明和重试条件，不得永久 loading、空白或
  `console.error` 后继续；
- 三个页签按 Phase 真实完成情况注册。未实现模块不得以 disabled tab、静态截图、mock 数据或死页
  冒充可用；Phase 2 的 F-011～F-013 完成后才将 `/hmm` 默认入口切换到 `/hmm-risk`。

目标路由：

| 路由 | 页面职责 | 激活条件 |
|---|---|---|
| `/hmm` | HMM 研究主入口，最终重定向 `/hmm-risk` | Phase 2 风险页真实 API/UI 验收通过 |
| `/hmm-evolution` | 候选、评估、批次和 top-3 研究推荐 | P1-C / F-010 验收通过 |
| `/hmm-risk` | 能力感知的 L1/L2 板块状态热力图、预警、固定详情和状态分布 | G2-A真实rotation_L1纵切通过后先在DEV/安全端口可访问；未验收能力显式`NOT_AVAILABLE`，production/runtime activation独立授权，完整页面仍由F-011～F-013闭合 |
| `/hmm-research-training` | 滚动窗口、时效性、研究训练任务和隔离边界 | F-014 验收通过 |

### 2.3 数据流隔离设计

```python
# 数据源枚举
class HMMDataSourceMode(str, Enum):
    BACKTEST = "backtest"      # 研发：使用 QE artifact 固定数据
    REALTIME = "realtime"      # 分析：只读最新完成交易日数据

# 配置示例
研发环境:
{
    "data_source_mode": "backtest",
    "base_loop_ref": "qe_20260502_131502_9b54/Loop1",
    "artifact_source_preference": "prediction_store_first",
    "label_horizon_days": 10,
    "artifact_cache_dir": "tmp/hmm_evolution_cache/",
    "use_label_as_truth": true
}

实时分析环境:
{
    "data_source_mode": "realtime",
    "connection_profile": "hmm_evolution_ro",
    "as_of_policy": "latest_completed_trading_day",
    "candidate_id": "<hmm_evolution.candidate id>"
}
```

实时模式中的“最新”指本次所需 canonical 数据集的最新共同完成交易日，不得用自然日减一、
`CURRENT_DATE` 实现。API 先持久化 `preparation_queued` receipt；worker 在 `preparing` lease 内读取一次并固化 watermark、universe、artifact 与 market-return content hash，随后原子创建 evaluation/item。DB 连接必须复用
仓库同步 `get_conn()` 适配器，禁止伪造异步 context manager；若未来引入真正异步池，必须以
独立接口和直接集成测试交付。

---

## 3. 阶段划分与实施目标

### Phase 0: 数据基础与环境隔离（Week 1）

**目标**: 建立数据抽象层，确保研发与生产数据隔离

**交付物**:
```
backend/services/hmm_data_source/
  __init__.py
  base.py              # 抽象基类 HMMDataSourceInterface
  backtest_source.py   # 回测数据源（QE artifact）
  realtime_source.py   # 实时数据源（DB t-1）
  cache_manager.py     # QE artifact 缓存管理
  models.py            # Pydantic models
```

**当前状态**: Phase 0 hardening、Prediction Store 零副本路径和受控只读 DB/PIT sector
integration receipt 已完成；Phase 1 implementation unlocked。

**验收标准**:
- [x] 回测数据源优先按 task/LoopN 只读解析 Prediction Store content-addressed blob，
  缺 artifact 才通过任务节点和 QE workspace 文件接口下载白名单 artifact。
- [x] 已存在但损坏的 Prediction Store manifest/blob fail loud，不以 workspace fallback 掩盖。
- [x] 实时数据源使用 canonical market schema 和上一完成交易日，按候选/快照身份过滤。
- [x] 两种数据源均使用真实 DB/client 契约测试，不以错误 mock 代替。
- [x] artifact 具有可信 manifest、SHA256、行数、schema version、来源任务与质量状态。
- [x] workspace fallback cache 路径不可越界，写入原子化，具备跨进程互斥、TTL/max-size/clear 生命周期。
- [x] Realtime、Backtest、cache、isolation、integration 测试进入专用 CI 计划。
- [x] 当前授权 DB transaction 强制 `read_only=on`；以
  `qe_20260706_013235_bbd4/Loop8`、`as_of_date=2026-07-17` 完成
  trading-calendar/PIT sector integration receipt（4 passed）。

---
### Phase 1: HMM 离线评估与演进实验室（Week 2-3）

**目标**: 在固定历史输入上快速评估 HMM overlay，批量给出 top-3 研究推荐，最终有效性仍由 QE 终审。

**实现级权威**:

- `hmm_evolution_phase1_offline_evaluation_detailed_design_20260717.md` v2.0。

**数据库 Schema**:

- `hmm_evolution.schema_version`：bootstrap/version 审计；
- `hmm_evolution.candidate`：内容寻址的 coefficient 候选与 lifecycle；
- `hmm_evolution.offline_evaluation`：与 batch 解耦的内容校验评估、lease/fencing 和指标；
- `hmm_evolution.batch_test_run`：快速 durable receipt、`preparation_queued/preparing` 输入冻结、请求幂等、heartbeat、取消、超时和汇总状态；
- `hmm_evolution.batch_test_item`：batch/candidate/evaluation 关联以及 batch-relative 推荐分数和排名。

实际 bootstrap 必须为全部 schema/table/column 添加 `COMMENT ON`，并在事务内幂等执行；业务 service 不得隐式建表。

**评估契约**:

- v1 计算基准来自 `scripts/diagnostics/hmm_offline_diagnostic.py::compute_replacements`：同一交易日对 raw score 与 HMM-adjusted score 使用稳定排序，比较 raw TopK 与 adjusted TopK 的 entered/dropped 集合。
- QE task/loop 全部实验资产可通过独立只读 reader 列举、读取和取证；只有通过 manifest/parser
  trust contract 的声明资产可以进入 evaluator，其余资产为 inspection-only evidence。
- `net_label_return` 为逐日 `mean(entered label) - mean(dropped label)` 的均值；必须同时保存显式 `label_horizon_days`、有效日数、覆盖率和正值日比例。仅 horizon=10 时可显示别名 `net_label_10d`。
- `net_db_10d` 使用 `market.kline_daily_raw.close_li` 按交易日序列计算 10 个交易日远期收益，禁止自然日 shift。
- completed legacy QE loop 仅在找不到 immutable dataset namespace 时允许使用冻结
  `legacy_frozen_runtime_artifact_v1`：必须同时校验 source loop completed、pool SHA/ready/dirty、
  ST-PIT rule/config/source fingerprint 与 manifest 记录完全一致；任何漂移均 fail loud。该兼容只用于历史研究，
  不能把 live `shsz_st_pit_active_v1` 作为会随时间变化的隐式输入。
- 对 runtime artifact 产生前且被 immutable legacy manifest 单独批准的旧 loop，BUG-798 仅允许
  `legacy_allowlisted_compatibility_artifact_v1`：原 loop 继续提供 pred/label/config/pool，ST-PIT 从 receipt
  固定的 donor task/loop 零副本只读；必须核对 donor identity、artifact SHA256/size、原 config/pool hash、
  coverage/span/rule/scope/source fingerprint，并在 source manifest 如实记录 cross-loop provenance。未登记、
  任一漂移或窗口越界均 fail loud；禁止读取当前 live ST、近似重建或声称旧 loop 原运行时使用过 donor artifact。
- `hmm_recommendation_v1` 使用 batch 内 percentile、版本化权重、缺失值重归一化和稳定并列规则；分数与排名存于 `batch_test_item`，不污染可复用 evaluation。
- top-3 是研究推荐，不自动提交 QE、不修改生产配置、不淘汰未入选方向。
- 每次 market-required 结果必须使用 `hmm_evaluation_source_manifest_v3`，记录行情计算版本以及按日期/股票排序后的收益值和缺失证据内容 hash；worker 重读内容不一致时 fail loud。Phase 1 不额外固化永久行情副本，因此这里承诺的是“内容校验重放/漂移即失败”，不是无限期离线重建保证。
- neutral fallback、共同日期裁剪和缺失指标重加权必须标记 degraded 并在主 UI 可见；
  不得只写日志、只返回技术 context 或依赖 raw JSON 才能识别。

**当前实现状态**:

- P1-A/P1-B 后端基础、evaluator、executor、source manifest、market repository 和 scorer 已合入 main。
- 旧 `hmm_offline_diagnostic.py` 已复用唯一 evaluator/Phase 0 缓存/canonical 只读行情 repository，
  不再包含硬编码 DB 凭据、QE config 下载或宽泛异常吞错。
- API 端点、`/hmm-evolution` UI 和 worker CLI/service 源码属于 P1-C 且已实现；BUG-773～BUG-777 的 schema v2 已于 2026-07-20 完成生产 DDL 与 exact verify。2026-07-21 独立 Windows worker 已加载 BUG-800/BUG-801/BUG-804 后代码：pre-ST-PIT 兼容 batch `hmmb_66db955297e6440283097e6fdfb927ac` 9/9 succeeded，Prediction Store/QE workspace artifact hash、兼容 ST-PIT donor receipt、market content hash 与 read-only transaction 均保留，缺失 label/market evidence 显式 degraded。受控 worker 中断按 §12.3 状态机把过期 evaluation 终态化为 `timed_out`，显式 retry 新 generation 2/2 succeeded；不自动复活旧 evaluation。2026-07-22严格冷热缓存分段timing/RSS receipt、真实UI/Playwright 18场景和31.6分钟worker bounded soak均已完成外部验收。

**2026-07-19 性能事实（非通过回执）**：旧同步 preparation 路径的 API 202 前耗时约 66～69 秒；
batch `created_at → completed_at` 观测为 Loop1 146.638s、Loop2 222.352s、Loop3 304.339s、
Loop4 377.703s、Loop5 450.539s、Loop6 522.748s、Loop7 585.680s、Loop8 663.290s、
Loop9 10-candidate batch 990.449s、Loop10 125.938s。Loop8 加 API preparation 已超过 12 分钟；
Loop9 的 195.781s item duration 不能冒充单候选 request-to-terminal。BUG-775 后必须重新分别记录
receipt persist、preparation queue、source/universe/market freeze、evaluation compute、persist 和总耗时。

**API 端点**:
```
POST /api/v1/hmm-evolution/candidates/preview
POST /api/v1/hmm-evolution/candidates
GET  /api/v1/hmm-evolution/candidates
GET  /api/v1/hmm-evolution/qe-assets/{task_id}/{loop_name}
GET  /api/v1/hmm-evolution/qe-assets/{task_id}/{loop_name}/stat
GET  /api/v1/hmm-evolution/qe-assets/{task_id}/{loop_name}/content
POST /api/v1/hmm-evolution/evaluate
POST /api/v1/hmm-evolution/batch
GET  /api/v1/hmm-evolution/evaluations/{eval_id}
GET  /api/v1/hmm-evolution/batches
GET  /api/v1/hmm-evolution/batches/{batch_id}
POST /api/v1/hmm-evolution/batches/{batch_id}/cancel
POST /api/v1/hmm-evolution/batches/{batch_id}/retry-failed
```

**验收标准**:
- [ ] 单个评估在标准验收基准数据集、指定硬件上冷缓存 < 10 分钟。
- [ ] 批量 10 个候选复用只读输入、限制并发，在同一基准上 < 30 分钟。
- [x] 结果具备内容校验重放，行情/asset/universe 漂移时结构化失败，并具有 heartbeat、取消、幂等、lease 超时终态化和显式 retry 语义；若未来要求完全离线永久重建，必须另交付不可变行情 artifact。
- [x] 与至少 10 个历史 QE case 做方向性/排序一致性对照；差异只作为证据，不作为未经批准的淘汰门禁。
- [ ] 前端以中文表格、指标卡、对比图、固定证据区和独立详情页为主；不使用 Paper v2
  视觉依赖、抽屉式列表或原始 JSON/manifest/error dump 作为信息界面。

---

### Phase 2: 板块轮动预测与风险预警系统（原计划Week 4-5，当前按产品闭环推进）

**终极目标**：先按最新完成交易日生成真实可验收的L1板块未来相对强弱排序与状态视图，再独立扩展L2轮动和风险预警；全部结果只作研究分析，不接入任何交易决策。模型训练、battery、artifact或market regime单独可用均不是本阶段交付。

**历史实现与模型验收状态**：旧独立 F2 实现级详细设计
`docs/architecture/hmm_evolution_phase2_risk_monitoring_detailed_design_20260722.md` 已明确唯一的
versioned sector-state generator、候选/模型/系数身份、行情与行业映射共同水位、freshness/revision/dedupe、
事务失败收敛、worker stale/cancel/timeout 以及 exact schema/API contract。C-001-A/C-002-A/C-003-A 已获用户批准：
13 个 candidate 是 direct state producer并共享两个 L2 model identity，4 个 pooled candidate 明确是 coefficient-only；
L1/L2 使用成对独立 direct HMM，不聚合 posterior；回溯使用 5/10/20 连续证据与 5D excess q20 次级 oracle。
Decision C-004 明确冻结 `scripts/precompute_hmm_risk_gate.py`、`hmm_risk_gate_v1` 与现有 Selection/QE consumer，
本阶段不迁移、包装或退役旧业务逻辑。设计修订不等于 schema/backend/API/UI/runtime 已交付。

**旧B3历史摘要**：C-009/C-010输入闭包、B3 `5184/5184` fits、后续blocker/structure/refit诊断均已诚实完成，但没有两family完整candidate、model、READY或产品能力。相关参数、两family/四level合取、D1/P6/D5/D6和大规模artifact只保留为历史证据，不再构成新G2-A的active contract；不得重跑、调参、复用已消费窗口或继续解释历史mechanism制造进度。旧源码、`hmmlearn==0.3.3`和已验证Conda环境保持不变，不代表新GBDT依赖已获授权。

**当前唯一G2-A方向**：用户确认`HMM/jump market context + supervised nonlinear cross-sectional rotation scorer`，active入口为`docs/architecture/hmm_evolution_phase2_rotation_l1_g2a_detailed_design_20260903.md`。2026-09-04进一步批准Rank IC 0.02 binding MBE、MDE仅作forward功效状态、research/tail双门、310/20叶结构、HAC上界effect failure及horizon-independent market context。market regime只作为输入，不独立产品化；risk与L2不并行。rolling长度、确切日期、feature minimum、Ridge/horizon差异、其余GBDT参数、stability/coverage、`lightgbm`依赖和DDL仍待批准，因此不得开始源码、battery、fit、依赖安装或产品写入。

**G2-A最小DB/API/UI权威**：新详细设计§8～§9是首个`rotation_L1`纵切的唯一implementation-level入口。旧`sector_state_timeline.state_probabilities`面向HMM概率，不能填造GBDT概率；新G2-A推荐独立最小`hmm_risk.rotation_l1_prediction`表，保存连续score、派生state、availability/reason、model/input/mapping identity和revision。精确DDL仍待用户批准并须先在DEV验证。

G2-A只开放：

```text
GET /api/v1/hmm-risk/overview
GET /api/v1/hmm-risk/rotation-l1?trade_date=YYYY-MM-DD
```

`/hmm-risk`当前只允许真实L1轮动热力图：展示完整31-sector分母、连续score方向、`trending/neutral/fading`、feature贡献、unavailable原因、model/as-of/coverage、development OOF Rank IC/HAC区间、forward状态和研究声明。experimental surface必须显著区别于research prediction capability和advisory；非概率score不得显示为confidence或probability。API、renderer或数据失败必须有可见typed终止态，不得永久loading、空白、mock、静态矩阵、旧Ridge结果或market-only页面冒充成功。

旧详细设计中的`sector_state_timeline`、`daily_alert`、`risk_event`、多日timeline、alerts/events/jobs/reports端点、L2切换、7日历史、transition/severity和风险回测均保留为G2-B/G2-C未来合同；在`rotation_L1`真实纵切验收前不实现、不注册死路由，也不作为G2-A完成条件。所有阶段继续advisory-only，不产生`can_buy`、调仓、订单、持仓或profile更新。

**G2-A验收标准**：

- [ ] 唯一candidate完成development，research product gate与tail access gate分别给出结果；MDE不足不淘汰模型，tail gate失败不读取tail。
- [ ] 全部development因果OOF prediction与model/input/mapping hash回读一致；experimental surface不得推导capability。
- [ ] overview与rotation-l1 API由真实repository数据驱动，完整表达五轴状态、available/unavailable、OOF效果与HAC区间。
- [ ] 真实L1热力图、错误态和研究声明通过无mock浏览器验收；development未达MBE时显著展示`BELOW_BINDING_MBE`。
- [ ] tail通过时才升级advisory；inconclusive与failed按批准HAC公式分开，均不触发调参或第二candidate。
- [ ] Selection/Paper/QMT/QE及旧HMM gate均无写入或业务语义变化。

---

### Phase 3: HMM 滚动训练自动化（Week 6）

**目标**: 对研究候选执行可审计的滚动训练和时效性监控，产物只登记到 `hmm_evolution.*`，不自动进入生产注册表。

**已有基础**:
```python
# backend/services/hmm_training_service.py（仅允许复用纯计划函数）
def build_rolling_training_plan(
    trading_days: list[date],
    latest_completed_trade_date: date,
    train_window_years: float = 3.0,
    validation_window_months: int = 3,
) -> dict:
    """
    构建滚动训练计划：
    - 训练窗口: 3 年
    - 验证窗口: 3 个月
    - 自动调整到交易日
    """
```

**新增功能**:
```python
# backend/services/hmm_rolling_train/scheduler.py

class HMMRollingTrainScheduler:
    async def plan_monthly_retrain(
        self,
        candidate_template_id: str,
        schedule_id: str,
    ) -> RollingTrainPlan:
        """
        生成月度重训计划：
        1. 检查独立候选的最新训练水位与输入数据完成水位
        2. 按版本化 schedule policy 建立幂等训练任务
        3. 使用 latest - 3 months 作为验证集
        4. 训练完成后登记 hmm_evolution.candidate，不写生产 snapshot
        """
    
    async def check_model_staleness(
        self,
        candidate_id: str,
        latest_common_completed_trade_date: date,
        staleness_policy_id: str,
    ) -> StalenessReport:
        """
        按版本化策略检查模型时效性：
        - candidate 训练数据截止交易日 vs latest common completed trade date
        - 计算完成交易日年龄，不使用 worker 当前自然日
        - 返回 freshness evidence；不自动触发生产替换或研究淘汰
        """
```

**调度边界**:

- 本文不得直接复用 `backend.routers.hmm_training.rolling_training_tick`；该路径会触发既有 `model_train_configs/model_train_snapshots` 写入。
- 既有 Paper v2 设计规定 rolling retraining 为用户触发。若要启用无人值守月度调度，必须在 Phase 3 实施前提交并批准单独的调度语义修订，明确 scheduler ownership、leader election、misfire、重入、重试、取消和停用开关。
- schedule、candidate template、训练窗口和资源池必须来自独立 DB 配置；禁止在脚本中硬编码生产 config id 或 cron。
- 调度器失败只能影响独立研究任务；不得阻塞或改变 QE、Selection、Paper、MiniQMT。
- staleness 使用 candidate 的训练数据 watermark 与本次固化的 latest-common completed trade date；
  阈值属于版本化展示/调度策略，不得用 `date.today()`、自然日差或硬编码 90 天作为研究淘汰门禁。

**UI 契约**:

- `/hmm-research-training` 展示模型时效性、滚动窗口预览、研究训练任务、失败原因和隔离边界；
  不复用 `/paper-v2/model-hmm` 页面、`hmmTrainingApi` 的生产写入 contract 或 legacy 组件。
- 页面必须明确区分“只读窗口预览”“人工触发研究训练”“未来自动调度”和“生产模型状态”；
  研究训练完成只产生 `research_only` candidate，不出现“替换生产模型”或“应用到 Paper”动作。
- 训练任务失败显示 durable status、reason code、最近 heartbeat、失败阶段和可重试条件；
  不将日志异常、worker 退出或 artifact 缺失显示为完成或空结果。
- 自动调度未批准或未启用时显示明确的运行态状态，不提供假开关、不可用按钮或静态任务列表。

**验收标准**:
- [ ] 纯计划函数按真实交易日生成可重放窗口。
- [ ] 训练任务具备幂等键、heartbeat、超时、取消、失败上下文和并发上限。
- [ ] 新产物只进入独立 candidate registry，默认状态为 `research_only`。
- [ ] 研究训练 UI 使用真实 planner/task/candidate API，窗口、时效性和任务状态与持久化证据一致；
  禁止复制 Paper v2 写入路径或用静态计划冒充可执行训练。
- [ ] 未获得调度语义修订批准前，只交付预览与人工触发，不启用自动 cron。

---

### Phase 4+: 生产接入（待独立设计）

**范围**: 
- 接入 QE 实验（修改 qe_templates）
- 接入 Paper v2 模拟盘（修改 runtime_config）
- 实盘自动调仓（需充分回测）

**未来研究证据（非当前门禁）**:
- Phase 0-3 的设计验收矩阵无未批准缺口。
- 离线评估与 QE 的一致性、风险预警有效性和滚动训练稳定性形成可复查报告。
- 任何数值阈值必须在 Phase 4 独立设计中基于样本、成本、容量和失败代价重新批准；本文中的历史 90%/75%/3 个月仅作为研究假设，不构成自动准入条件。

**审批流程**:
- 需独立设计文档
- 需风险评估报告
- 需生产环境测试方案

---

## 4. 数据环境隔离详细设计

### 4.1 抽象接口

```python
# backend/services/hmm_data_source/base.py

from abc import ABC, abstractmethod
from datetime import date
import pandas as pd

class HMMDataSourceInterface(ABC):
    """HMM 数据源抽象接口"""
    
    @abstractmethod
    async def get_predictions(
        self,
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        """
        获取预测分数
        
        Returns:
            DataFrame with columns: [trade_date, symbol, score]
        """
        pass
    
    @abstractmethod
    async def get_labels(
        self,
        start_date: date,
        end_date: date,
        horizon_days: int = 10,
    ) -> pd.DataFrame:
        """
        获取未来收益标签
        
        Returns:
            DataFrame with columns: [trade_date, symbol, future_return]
        """
        pass
    
    @abstractmethod
    async def get_sector_mapping(
        self,
        trade_date: date,
    ) -> dict[str, str]:
        """
        获取股票板块映射
        
        Returns:
            {symbol: sector_code}
        """
        pass
    
    @property
    @abstractmethod
    def mode(self) -> str:
        """返回数据源模式: 'backtest' 或 'realtime'"""
        pass
```

### 4.2 回测数据源实现

- `base_loop_ref` 必须解析为 task/loop，并通过任务元数据解析 compute node；禁止依赖固定 `localhost:9000`。
- Phase 0 自动下载/反序列化必须调用仓库真实存在的 QE workspace 文件接口，白名单严格限制为
  `pred.pkl`、`label.pkl`；Phase 1 QE 全资产 reader 是独立 inspection-only contract，不扩大
  Phase 0 自动反序列化范围。
- 远端 manifest 必须先于反序列化校验；只有来源、sha256、size、row_count、schema_version 和 quality_status 均有效才允许进入缓存。
- 预测、标签必须标准化日期、symbol、数值类型、重复键和缺失值，并返回副本，禁止原地修改下载对象。
- 交易日历和行业映射使用一次批量查询，禁止逐日期 DB round trip。
- 客户端所有权明确：内部创建的 HTTP client 必须关闭，外部注入 client 由调用方管理。

### 4.3 实时数据源实现

- 实时分析输入只能来自已登记的独立 candidate/coefficient artifact 与 canonical market 表，不得猜测不存在的 `model_train_predictions`。
- `candidate_id` 必须进入所有预测查询或 artifact 解析；禁止混合不同模型、配置或窗口结果。
- `market.trading_calendar` 决定最新完成交易日和 N 日 horizon；行情字段使用 `ts_code`、`close_li`，行业映射使用 `market.sw_index_member` 的 PIT 生效区间。
- 当前仓库 DB 池是同步 psycopg2 context manager；service 可以用同步 repository，或明确在线程执行器中封装，禁止用 `async with` 伪装异步。
- `get_predictions` 与 `get_labels` 必须共享明确的 `as_of_date`，不得依赖 `CURRENT_DATE` 产生不可重放结果。
- 无有效 candidate、共同数据水位不足或标签尚未实现时 fail-fast，并返回稳定 reason code。

---

## 5. 文件结构与规范

### 5.1 新增文件清单

```
backend/
  db/
    init_hmm_evolution_schema.py         # Phase 1
    init_hmm_risk_schema.py              # Phase 2
  
  services/
    hmm_data_source/
      __init__.py
      base.py
      backtest_source.py
      realtime_source.py
      cache_manager.py
    
    hmm_evolution/
      __init__.py
      qe_asset_reader.py
      candidate_artifact.py
      errors.py
      service.py
      evaluator.py
      scorer.py
      models.py
      repository.py
      worker.py
    
    hmm_risk/
      __init__.py
      monitor_service.py
      alert_generator.py
      gate_backtester.py
    
    hmm_rolling_train/
      __init__.py
      scheduler.py
      staleness_checker.py
  
  routers/
    hmm_evolution.py
    hmm_risk.py

scripts/
  hmm_evolution/
    run_daily_alerts.py                 # 受控 runner；不硬编码 schedule/config

frontend/
  src/
    app/
      hmm/
        page.tsx                         # Phase 2 验收后默认重定向 /hmm-risk
      hmm-evolution/
        page.tsx
        batches/[batchId]/page.tsx
        evaluations/[evalId]/page.tsx
      hmm-risk/
        page.tsx
        alerts/[alertId]/page.tsx
      hmm-research-training/
        page.tsx
    components/
      hmm-research/
        HMMResearchNavigation.tsx
        EvidencePanel.tsx
        VisibleErrorState.tsx
      hmm-evolution/
      hmm-risk/
        SectorStateHeatmap.tsx
        SectorStateDetail.tsx
        DailyAlertList.tsx
      hmm-research-training/
    lib/
      hmm-research/
        contracts.ts
      hmm-evolution/api.ts
      hmm-risk/api.ts
      hmm-research-training/api.ts
    lib/navigation/nav-groups.ts       # 明确导航入口和中文标签

backend/tests/
  hmm_data_source/
  hmm_evolution/
  hmm_risk/
  hmm_rolling_train/

docs/
  architecture/
    hmm_evolution_and_risk_management_system_design_20260716.md  # 本文档
    hmm_evolution_phase1_offline_evaluation_detailed_design_20260717.md
```

### 5.2 不污染规范

**允许写入**:
- `docs/architecture/` - 架构设计文档
- `backend/` - 生产代码
- `frontend/` - 前端代码
- `backend/tests/`、`frontend/tests/` - 测试代码

**临时文件**:
- `tmp/hmm_evolution_cache/` - QE artifact 缓存
- `.codex_tmp/hmm_offline_diag/` - 离线诊断临时文件
- `tmp/handoff/hmm-ui-demo-*` - 用户确认前的静态 UI 演示；不得被正式页面 import 或作为验收证据

**禁止写入**:
- `docs/handoff/local/` - 仅本地临时文件
- 项目根目录 - 不新增散装文件
- 不新增裸 `.sql` 文件 - 使用 Python schema bootstrap

---

## 6. 风险与缓解

### 6.1 技术风险

| 风险 | 影响 | 概率 | 缓解措施 |
|------|------|------|---------|
| 离线评估与 QE 结果不一致 | HIGH | MEDIUM | 对比 10+ 历史 case，保存差异归因；不把单一阈值变成研究淘汰门禁 |
| 数据源契约漂移 | HIGH | HIGH | canonical schema contract test + 真实受控 smoke + 专用 CI |
| artifact 损坏或来源不可信 | HIGH | MEDIUM | 可信 manifest、原子写、来源校验、路径边界、fail-fast |
| 风险预警误报率高 | MEDIUM | MEDIUM | 展示样本量/误报/漏报/分阶段稳定性，不干预交易 |
| 滚动训练重复或越权写生产表 | HIGH | MEDIUM | 独立 registry、幂等任务、DB 最小权限、写表 allowlist |
| 状态热力图被误解为交易热度或买卖建议 | HIGH | MEDIUM | 明确色相=HMM 状态、数字=置信度；不定义隐式 heat score，不输出 can_buy |
| 图表依赖加载失败后永久 loading | MEDIUM | MEDIUM | 可见 reason code、终止状态、重试和结构化表证据；禁止 console-only error |
| 分阶段上线产生死页或静态占位 | MEDIUM | MEDIUM | 路由/页签按验收项真实注册，未完成模块不展示、不以 mock 冒充 |

### 6.2 业务风险

| 风险 | 影响 | 概率 | 缓解措施 |
|------|------|------|---------|
| 用户过度依赖预警 | HIGH | MEDIUM | UI 明确标注"仅供参考，最终决策需人工判断" |
| 预警被误解为交易指令 | HIGH | MEDIUM | UI/接口明确 advisory-only，测试断言无 RiskDecision/订单副作用 |
| 数据延迟导致预警失效 | LOW | LOW | 监控数据时效性，超时告警 |

---

## 7. 实施时间表

### Gate 0: Phase 0 hardening
- 修复 QE client/DB/canonical schema 和 candidate identity。
- 修复 artifact/cache 安全与容量边界。
- 补专用 CI、真实集成 smoke 和性能基准；完成后才允许 Gate 1。

### Gate 1: Phase 1 - 离线评估
- Day 1-2: 数据库 schema + 基础服务
- Day 3-4: 评估核心逻辑 + 评分算法
- Day 5-6: API 端点 + 单元测试
- Day 7-8: 前端 UI 开发
- Day 9-10: 端到端测试 + 历史 case 验证

### Gate 2: Phase 2 - 板块轮动预测、分析与风险预警

Gate 2 不再按技术层或小功能拆阶段，只保留三个可产生用户结果的纵向业务闭环。P2-1～P2-4、C-012/C-013编号继续作为设计与历史证据索引，不再作为独立任务队列。第一个闭环必须交付真实用户可访问结果；模型、bundle、receipt或backend-only均不是闭环终点。

1. **G2-A 输入权威到首个真实`rotation_L1`产品闭环（部分精确合同已批准）**：旧RW1保持终态且不得重跑。新G2-A只允许一个浅层监督式L1 scorer；HMM/jump market context在公共fold train上拟合一次，由5D/10D共享。battery选择horizon并报告功效，但MDE不淘汰GBDT。GBDT后先以无效果阈值的research gate闭合真实OOF repository/API/UI，再以development OOF Rank IC 0.02 tail gate决定是否消费尾部；forward通过才升级advisory，低功效/inconclusive/failed均不得调参或打开第二candidate。剩余精确参数、依赖和DDL未批准，源码、fit和产品均未执行。
2. **G2-B 首个产品到扩展分析与预警闭环（不可执行：无G2-A capability）**：G2-B只允许建立在已验收的真实G2-A canonical identity和API/UI上。当前不存在该前置能力，因此不得单独开发历史分析、transition/severity、预警、详情或UI，也不得用失败RW1的score、局部正向fold或静态数据伪造产品。
3. **G2-C 真实产品到受控日任务闭环（不可执行：无G2-B product）**：共同水位、幂等日任务、revision/dedupe、late-data和受控runner继续保留为未来产品集成合同；在没有已验收产品identity前不得建设或激活。

**当前唯一决策点**：方向与功效/交付状态机已经收敛；下一决定仅是对剩余确切日期、rolling长度、feature minimum、Ridge/horizon差异、其余GBDT参数、stability/coverage、依赖和最小DDL作一次性批准。批准前不得编码或实验；批准后同一G2-A连续完成实现、受控实验与真实纵切，不再建立独立diagnostic、schema、adapter、backend或UI小阶段。

每个闭环内部允许因用户授权边界存在多个动作状态，例如source merge、实验执行、用户重启和post-restart verify，但不得把动作状态改写为新的功能阶段或单独积累完成度。小型设计补充、测试、同范围BUG、审查修复和状态文档随当前闭环收敛。

### Gate 3: Phase 3 - 滚动训练
- 先交付独立候选 registry、计划预览和人工触发。
- 自动调度必须等待调度语义修订批准；不得以安装 APScheduler 或启用旧 tick 作为实现。

---

## 8. Design Acceptance Index（设计验收索引）

- **F-001 Phase 0 QE artifact 契约**：按 task/loop 优先只读复用 Prediction Store
  content-addressed artifact；缺失时解析节点并调用真实 workspace 文件接口，只接收可信
  manifest 白名单 artifact；损坏不得静默 fallback。
- **F-002 Phase 0 canonical DB 契约**：同步 DB adapter、`market.trading_calendar`、`market.sw_index_member`、`ts_code/close_li` 和 PIT 区间正确。
- **F-003 Phase 0 candidate identity**：所有预测、系数、标签和评估均绑定明确 candidate/source identity，不混用不同模型结果。
- **F-004 Phase 0 cache 安全**：路径不可越界、原子写、跨进程互斥、可信校验、安全清理、TTL/max-size/clear 生命周期完整。
- **F-005 Phase 0 测试与 CI**：Realtime/Backtest/cache/isolation/integration 有直接测试并进入专用 nox/CI；真实性能证据可追溯。
- **F-006 Phase 1 独立候选注册表**：QE 全资产只读 reader、研究候选只写
  `hmm_evolution.*`，artifact manifest 和生命周期可审计。
- **F-007 Phase 1 内容校验重放**：QE asset trust、latest-common watermark、输入、窗口、
  源 loop PIT 股票池、不可变 QE dataset ST-PIT universe、算法版本、指标定义和 hash 足以复算同一结果；
  新评估禁止 `prediction_artifact_all`，历史 v1 仅可只读展示、不得重试。
- **F-008 Phase 1 批处理状态机**：幂等、heartbeat、取消、超时、并发上限、部分失败和结构化错误完整。
- **F-009 Phase 1 推荐语义**：top-3 仅为研究推荐，公式版本化，不自动淘汰方向或修改 QE/Paper。
- **F-010 Phase 1 API/UI**：真实 QE asset/candidate/evaluation/batch API、中文演进实验室、共享
  HMM 研究导航、动态 horizon、主视图 degraded warning、固定证据区和独立详情页完整；禁止
  Paper v2 依赖、抽屉式列表和 raw JSON 主视图；全局左侧导航在 HMM 路由常驻，逐日明确区分
  “当日无调整”和“证据缺失”，并结构化显示缺失股票与原因。
- **F-010A Phase 1 自动评估 worker service**：显式独立进程自动消费 API 已登记的 durable queue；
  canonical env、poll bounds、idle wait、SIGINT/SIGTERM、lease/fencing recovery 和 fail-loud exit 完整；
  不创建 batch、不嵌入 FastAPI、不触发 QE 或 Phase 3 训练。
- **F-011 Phase 2 canonical product bundle与分层能力验收**：共享PIT/identity、market context、rotation L1/L2、risk L1/L2、横截面产品有效性和coverage/availability完整。当前首要子项是直接监督的`rotation_L1`非线性横截面component。Rank IC 0.02是唯一binding MBE，MDE只描述forward功效；research surface、rotation capability、forward confirmation与advisory状态正交。research gate不含效果阈值，tail access另要求development达到MBE；任何experimental surface不得冒充capability。
- **F-012 Phase 2 advisory-only**：无 `RiskDecision`、`can_buy`、订单、持仓、配置或调仓副作用。
- **F-013 Phase 2 轮动/风险分析与 UI 证据**：`/hmm-risk` 为最终默认首页，market regime、已验收L1/L2相对强弱/状态热力图、
  今日预警、固定详情、状态分布、横截面Rank IC/spread、命中/误报/漏报、availability/coverage及阶段稳定性完整；能力、状态、置信度、severity
  语义分离，未验收能力显示typed `NOT_AVAILABLE`，不擅自新增 heat score 或硬门禁。
- **F-014 Phase 3 独立训练候选与 UI**：只复用纯滚动窗口计划，训练产物仅进入独立 registry，
  默认 `research_only`；训练页真实展示窗口、时效性、任务状态和隔离边界，不复用 Paper v2 写入路径。
- **F-015 Phase 3 调度安全**：人工触发默认不变；自动调度需独立批准，并具备 ownership、leader、misfire、重入和停用语义。
- **F-016 全阶段隔离与生产边界**：不修改既有 QE/Paper/StrategyPackage/生产 snapshot，不启服务、不隐式 DDL，生产门禁逐 PR 明确。

## 9. Implementation Plan（实施方案）

1. **P0-A 数据契约修复**：处理 F-001/F-002/F-003，先让两种数据源真实可运行。
2. **P0-B artifact/cache hardening**：处理 F-004，并补可信 manifest 与容量边界。
3. **P0-C 验证与文档收敛**：处理 F-005，修正文档、专用 CI、受控 smoke 和 benchmark。
4. **P1-A asset/schema/repository（已完成外部验收）**：交付 F-006/F-008 的 QE 全资产只读 reader、Python bootstrap、repository 和任务状态机。
5. **P1-B evaluator（已完成外部验收）**：既有诊断脚本已迁入唯一纯计算逻辑；BUG-768～BUG-774、
   BUG-788、BUG-798、BUG-800 和 BUG-804 补齐源 loop 股票池 ∩ QE ST-PIT universe、全股票收益证据、
   逐日状态、内容校验重放和 pre-ST-PIT allowlisted compatibility；F-007/F-009 已验证。
6. **P1-C API/UI（外部验收已完成）**：F-010 API/UI、worker CLI/service 和 BUG-742～BUG-748 审计修复已实现；schema v2 worker、10-case、10/9 候选性能、进程中断 fail-closed 与显式 retry receipt 已完成；2026-07-22 严格冷热缓存分段 timing/RSS benchmark matrix、真实 UI/Playwright 18 场景与 worker bounded soak 全部完成（Phase 1 详细设计 §17.4.6）。
7. **P2 板块状态预测、分析与风险预警**：旧P2-3A～P2-4、HR1与RW1保持历史终态。当前只推进一个端到端G2-A：复用既有PIT/immutable bundle，在development内完成有界battery与唯一浅层GBDT；research gate通过后用真实OOF闭合最终repository/API/UI但不推导capability，tail gate通过后才消费全新尾部，forward通过才升级advisory。禁止并行模型、重跑旧request、复用holdout、结果后调参、单独产品化market regime、mock/静态API/UI、通用平台或用experimental surface冒充预测能力。
8. **P3 研究训练**：F-014 research-only rolling candidate 与 F-015 manual-first/automation boundary 只有跨阶段方向；必须先建立独立实现级 F2 设计、Design Acceptance Index 和验证矩阵。自动调度仍未批准，不得直接进入代码或复用旧 production training tick。

每个业务闭环使用一个稳定Feature/BUG范围并在PR body中列设计项、实现引用、验证证据、生产门禁与未批准缺口；同范围的小型源码、测试、修复和文档不得人为拆成多个feature阶段。因不可变merge identity、实验授权或runtime授权产生的多个动作仍归属于同一闭环，并分别报告状态；任一动作不得被误报为闭环或Phase完成。Phase 0 BUG修复走issue workflow；Phase 1-3新能力走feature workflow。
branch/commit/push/PR/CI 可按流程继续，但所有 PR 必须在 merge 前停止，并取得用户对该 PR 的明确确认；不得自动合入或执行 merge-aftercare。

本文对 Phase 2/3 给出跨阶段权威边界和已确认 UI 契约。Phase 2 从属详细设计只有明确标记为
用户已批准且直接映射 F-011/F-012/F-013 的精确条目可进入实现；历史D3-D7、D1、P6/D5/D6继续说明旧B3为什么blocked，不再规定新canonical产品必须复制两family四level全局合取。
REMEDIATION-DIAG-02、REFIT-03、TRAIN-STABILITY-DIAG-01与TRANSITION-DWELL-B只保留为历史诊断证据。后续不得以继续解释历史mechanism、扩大artifact schema或增加fresh-process矩阵替代产品验收精确合同、单一spike和预测/预警纵切。
Phase 3仍须独立
实现级详细设计。开始对应代码前必须先具备对应 F2 validator PASS 与正式审核结论；这是
DESIGN-COMPLIANCE-001 的设计完整性要求，不是每次研究操作的产品审批流。

## 10. Verification Plan（验证方案）

- **静态小门**：changed-file Ruff/TypeScript lint、`git diff --check`、allowed scope、guardrail scan。
- **数据源 contract test**：使用真实同步连接接口的 fake repository；断言 canonical 表/字段、交易日 horizon、candidate 过滤和 fail-fast reason code。
- **受控 integration smoke**：只读测试 DB/QE workspace；禁止生产写、DDL 和生产端口，保存 compact receipt。
- **artifact 安全测试**：远端 manifest 不匹配、metadata 缺失、恶意 `..`/反斜杠、并发半写、reparse point、安全 clear、超限淘汰。
- **评估 oracle**：与 `hmm_offline_diagnostic.compute_replacements` 固定 fixture 对齐，再用至少 10 个历史 case 比较 QE 方向/排序差异。
- **性能基准**：记录硬件、输入行数、候选数、冷/热缓存、阶段耗时、峰值内存；单候选 <10 分钟、10 候选 <30 分钟仅在标准验收基准上判定。
- **风险副作用测试**：调用 Phase 2 API/job 后断言 Selection/Paper/QMT 表和 `RiskDecision` 输出不变。
- **滚动训练隔离测试**：DB 写表 allowlist 仅含 `hmm_evolution.*`；生产 config/snapshot 表写权限撤销时研究训练仍能正确失败并保留审计。
- **UI 证据**：安全验证端口上的真实 API 页面/E2E 或截图；不得启用 8001/3000/19080。
- **UI 信息架构证据**：断言最终 `/hmm` 默认 `/hmm-risk`、三个一级页签、L1/L2、7 日热力图、
  固定详情区、预警和训练隔离页面；未实现阶段不得注册死页或静态占位。
- **UI 失败证据**：renderer load、API 4xx/5xx、empty/degraded/stale、polling timeout 和 reason code
  均有可见终止态；不得依赖控制台、抽屉或 raw JSON 才能定位。
- **广域回归**：交给 Validation Center/CI/nightly 去重执行；本地只保留直接修复点最小门。
- **G2-A可预测性battery**：只在development日期运行预注册5D/10D、固定特征与一个线性对照；验证tail未读、选择规则固定、HAC功效输入闭合、MBE/MDE分离及低基线不自动淘汰GBDT。
- **唯一GBDT候选**：验证单一参数profile、rolling/purge/embargo、按日横截面预处理、typed missing、双fresh-process一致性、无early-stopping/调参/grid/第二candidate。
- **全新尾部与产品纵切**：真实causal OOF先验证prediction→repository→API→L1热力图且状态最多experimental；tail只在candidate冻结且development达到MBE后读取。功效不足不阻断research surface，forward未通过不得写advisory、模型fallback或静态成功页面。

## 11. Design Acceptance Matrix（设计验收矩阵）

本表记录 v2.41 设计验收状态；`implementation_refs` 和 `test_or_evidence` 中的“目标”不是完成声明，每个业务闭环必须将对应行替换为真实引用和结果证据后才能报告完成，闭环内部的单个PR、实验或动作不得单独增加产品完成度。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `backtest_source.py`; `prediction_store_resolver.py`; BUG-688/#2260; #2285 | `python -m pytest backend/tests/hmm_data_source/test_backtest_source.py -q`；`qe_20260706_013235_bbd4/Loop8` prediction-store-only receipt，2,260,161 rows，zero-copy/no HMM cache；h20 label 不作为 10 日 label 证据 | verified | 无 |
| F-002 | `db_repository.py`; `realtime_source.py`; BUG-689/#2266；BUG-773 | `python -m pytest backend/tests/hmm_data_source/test_realtime_source.py backend/tests/hmm_evolution/test_market_repository.py -q`；read-only transaction、trading-day horizon、显式 DOUBLE PRECISION division；BUG-773后Loop1～Loop10与pre-ST-PIT真实receipt；旧整数除法污染receipt只保留known-invalid且不用于验收 | verified | 无 |
| F-003 | `realtime_source.py`; `models.py`; BUG-689/#2266 | `python -m pytest backend/tests/hmm_data_source/test_realtime_source.py backend/tests/hmm_evolution/test_service.py -q`；candidate identity、隐式 latest 拒绝、filter contract | verified | 无 |
| F-004 | `cache_manager.py`; `artifact_manifest.py`; `prediction_store_resolver.py`; BUG-690/#2270 | `python -m pytest backend/tests/hmm_data_source/test_cache_manager.py backend/tests/hmm_data_source/test_backtest_source.py -q`；路径/原子/跨进程锁/容量/reparse/corruption fail-loud | verified | 无 |
| F-005 | `noxfile.py`; `ci_change_classifier.py`; BUG-691/#2273 | `python -m nox -s hmm_data_source_backend`；`backend/tests/hmm_data_source/test_realtime_source.py` 与 integration receipt | verified | 无 |
| F-006 | Phase 1 详细设计 §5.3/§6/§10/§11；AIstock QE asset reader + candidate bootstrap/repository；RD-Agent PR #4 | `python -m pytest backend/tests/hmm_evolution/test_qe_asset_reader.py backend/tests/hmm_evolution/test_candidate_artifact.py backend/tests/hmm_evolution/test_repository_integration.py -q`；真实 Loop8 complete catalog/zero-copy receipt；2026-07-20 DEV/production `hmm_evolution_v2` exact verify；2026-07-21 pre-ST-PIT 兼容 batch 9/9 succeeded | verified | 无 |
| F-007 | Phase 1 详细设计 §7/§8；`evaluator.py`、`input_adapter.py`、`market_repository.py`、`source_manifest.py`、`universe.py`；BUG-772～BUG-774、BUG-788、BUG-798、BUG-800、BUG-804 | `python -m pytest backend/tests/hmm_evolution/test_market_repository.py backend/tests/hmm_evolution/test_input_adapter.py backend/tests/hmm_evolution/test_universe.py -q`；Loop1～Loop10 market hash/read-only/zero-copy receipt；pre-ST-PIT batch `hmmb_66db955297e6440283097e6fdfb927ac` 9/9 succeeded，path-free donor receipt 与 artifact/source/market hash 回读通过；设计边界仍是不承诺永久离线重建、行情内容漂移 fail loud | verified | 无 |
| F-008 | Phase 1 详细设计 §10～§13；durable batch/evaluation/item repository、worker/input adapter/executor；BUG-742/BUG-743/BUG-801 | `python -m pytest backend/tests/hmm_evolution/test_worker.py backend/tests/hmm_evolution/test_input_adapter.py backend/tests/hmm_evolution/test_repository_integration.py -q`；10 候选 `hmmb_e2ac69e2e21a474e9044afa34a8f580b` 10/10 succeeded；中断 batch `hmmb_39fe2314e09041a9a056467a87d4fb46` fail-closed 为 2 timed_out + 1 succeeded；retry batch `hmmb_9e1d0eaf43d1432bb1cbbbba53cca5b6` 2/2 succeeded；详细设计 §17.4.6 benchmark purpose 隔离下 zerocopy 1c/10c 与 fallback cold/warm 全 matrix 分阶段 receipt | verified | 无 |
| F-009 | Phase 1 详细设计 §9；`scorer.py`、`repository.py::_apply_recommendations_with_cursor()`；BUG-776 | `python -m pytest backend/tests/hmm_evolution/test_scorer.py backend/tests/hmm_evolution/test_repository_integration.py -q`；`metric_availability_ratio` 明确替代误导性的 confidence 展示；历史受 BUG-773 影响的推荐只读不复用 | verified | 无 |
| F-010 | Phase 1 详细设计 §14/§15；真实 QE asset/candidate/evaluation/batch API、共享 HMM 导航、演进 UI；BUG-744～BUG-748、BUG-770～BUG-772、BUG-788/BUG-789 | `python -m pytest backend/tests/hmm_evolution/test_api.py backend/tests/hmm_evolution/test_qe_workspace_client_catalog.py backend/tests/hmm_evolution/test_frontend_contract.py -q`；2026-07-21 Loop1～Loop10 同口径 evaluation 全部 succeeded，单例 69.3～99.3 秒，degraded evidence 显式；详细设计 §17.4.6 真实 UI/Playwright 18 场景（8011/3011，无 mock，生产端口守卫）全过 + 18 张截图 | verified | 无 |
| F-010A | Phase 1 详细设计 §5.1/§13.5/§18～§21；`worker_service.py` + `hmm_evolution_worker.py --serve` + UI worker 文案 | `python -m pytest backend/tests/hmm_evolution/test_worker_service.py backend/tests/hmm_evolution/test_worker_cli.py -q`：22 passed；2026-07-21 受控中断旧 PID 73948，新 PID 37024 保持服务，过期 lease 明确 timed_out，显式 retry 2/2 succeeded，活动队列归零；详细设计 §17.4.6 31.6 分钟 bounded soak 六类事件 durable 监督记录 | verified | 无 |
| F-011 | 父蓝图v2.41；`hmm_evolution_phase2_rotation_l1_g2a_detailed_design_20260903.md` v1.1 D1～D6；旧Phase 2详细设计与RW1源码仅作历史参考 | 目标`backend/tests/hmm_risk/test_rotation_l1_gbdt.py`；旧RW1 failure保持不可变 | APPROVED_BY_USER_POWER_AND_DELIVERY_SPLIT_REMAINING_EXACT_CONTRACT_PENDING | 已批准Rank IC 0.02、MDE角色、双门、310/20叶结构、forward失败公式和共享market context；剩余窗口、参数、coverage、依赖待批准，CAPABILITY_AVAILABLE仍为0 |
| F-012 | Phase 2 F2 详细设计 §14：advisory-only service boundary | `backend/tests/hmm_risk/test_isolation.py`（目标路径，断言 Selection/Paper/QMT 无写入） | DESIGN_READY_USER_APPROVED | 用户明确批准 legacy producer/consumer 冻结与 advisory-only 隔离；源码与结果证据待实现 PR 回填 |
| F-013 | 新G2-A详细设计D6：真实OOF prediction、最小repository/read API、真实L1热力图；旧Phase 2设计§9～§11仅保留后续G2-B/G2-C合同 | `backend/tests/hmm_risk/test_rotation_l1_prediction.py`、`backend/tests/hmm_risk/test_api.py`、`frontend/tests/hmm-risk/hmm-risk.spec.ts`（目标路径，必须真实数据且无mock） | APPROVED_BY_USER_REAL_OOF_EXPERIMENTAL_SURFACE_WITHOUT_CAPABILITY_DRIFT | research gate通过可闭合真实experimental surface；development未达MBE时rotation capability仍NOT_AVAILABLE，forward未通过时advisory仍NOT_AVAILABLE |
| F-014 | 本文 Phase 3 UI/隔离方向；research-only rolling candidate + `/hmm-research-training` | `backend/tests/hmm_training/test_rolling_research_training.py`、`frontend/tests/hmm-training/hmm-training.spec.ts`（目标路径，尚未建立） | APPROVED_BY_USER_DIRECTION_ONLY_PENDING_IMPLEMENTATION_LEVEL_DESIGN | 用户批准跨阶段方向；不得从父蓝图直接编码，身份、训练任务、artifact、状态机、API/UI 和验证合同待独立设计 |
| F-015 | manual-first 与未来 scheduler 安全边界 | `backend/tests/hmm_training/test_scheduler_contract.py`（目标路径，尚未建立） | APPROVED_BY_USER_MANUAL_FIRST_DIRECTION_AUTOMATION_NOT_APPROVED | 用户批准 manual-first 边界；自动调度需另行明确业务语义，不得安装 scheduler 或启用旧 tick 冒充实现 |
| F-016 | 全阶段 isolation guard | `tests/aistock_validation/test_hmm_evolution_isolation.py`（目标路径：scope/production-gate/side-effect） | APPROVED_BY_USER_DESIGN_READY_PENDING_PHASE_IMPLEMENTATION | 用户明确批准隔离语义；结果证据由对应 Phase 实现 PR 回填 |

### 11.1 历史决策日志

2026-08-12至2026-09-04的逐次审核、实验终态、artifact identity和方向变更已迁至`docs/architecture/hmm_evolution_phase2_decision_log_20260812_20260904.md`。该日志只保存历史，不定义active模型合同；本表和当前G2-A详细设计继续是现行验收权威。

## 12. Rollout / Rollback（发布与回滚）

- Phase 0 hardening 先合入，不启用新 runtime；发现回归可回滚对应代码 PR，缓存格式升级须保留版本目录并支持安全重建。
- Phase 1/2 API 和 UI 默认不注册到生产调度；schema bootstrap 与运行时代码分开 gate，DDL 未显式授权时报告 `production_ddl_pending`。
- Phase 2 job 首次发布只允许人工受控执行；确认幂等、水位和副作用测试后，才可登记只读日任务。
- 每个 PR 的 merge 都必须由用户逐 PR 明确确认；该确认与 production DDL、dependency install、runtime activation 授权分别记录。
- Phase 3 默认仅预览/人工触发；自动调度若后续获批，必须有全局 disable switch、单实例 ownership 和逐 schedule 停用能力。
- 回滚任何阶段不得删除历史评估、预警或候选审计；使用 lifecycle status 标记 retired/invalid，并保留输入 manifest。
- 任何回滚都不得修改已存在的生产 HMM、StrategyPackage、Paper portfolio 或交易记录。

## 13. Production Gates（生产门禁）

- `production_ddl_gate`：Phase 0 为 `noop`；Phase 1/2 schema PR 默认为 `pending`，仅在用户显式授权后 `applied_and_verified`。
- `production_backend_dependency_gate`：无新依赖时 `noop`；新增 scheduler/serialization 依赖必须独立列出并安装验证。
- `production_frontend_dependency_gate`：默认 `noop`；禁止为新页面传播 legacy Paper v2 UI 依赖。
- `runtime_activation_gate`：代码合入与 runtime activation 分离；不得自动启动 8001/3000/19080 或注册生产 scheduler。Phase 1 worker service 必须作为独立进程显式启动，不得挂入 FastAPI startup。
- runtime flag 是 deployment switch/kill switch；首次生产启用只需一次操作授权，不得为 API、
  worker、导航或每次研究评估创建额外产品审批流。
- `data_write_gate`：只允许 `hmm_evolution.*`、`hmm_risk.*`；其它 schema 写入为 fail-closed。
- `design_compliance_gate`：每个实现 PR 必须执行 DESIGN-COMPLIANCE-001，四项控制均有证据：`no_simplified_delivery`、`no_silent_error`、`no_business_semantic_drift`、`no_unrequested_gate_or_approval`。

当前 gate truth（2026-07-22 production v3 activation 后）：

- `hmm_evolution_v1/v2` production：`applied_and_verified`（历史事实）。`hmm_evolution_v2` 于 2026-07-20 按 DEV-first 流程完成：DEV `127.0.0.1:5433/aistock_dev` 幂等 bootstrap、exact schema/comment verify 与两项真实 PostgreSQL integration 通过；生产 `127.0.0.1:5432/aistock` 在活动 batch 为 0 时单事务升级，`schema_version` v2 从 0→1，`request_payload JSONB NOT NULL DEFAULT '{}'` 与 preparation 状态约束回读正确。迁移前后 10 candidate、44 evaluation、25 batch、44 item 的受保护内容摘要保持不变，repository read smoke 通过。
- `hmm_evolution_v3`（`offline_evaluation.execution_purpose`/`benchmark_id`、`batch_test_run.execution_purpose`/`benchmark_id`、新表 `hmm_evolution.performance_receipt` 与 `hmm_evolution.worker_runtime_status` 及配套约束/索引/COMMENT/schema_version row）：DEV 与 production 均为 `applied_and_verified`。Production 于活动 batch 为 0 时执行单事务 bootstrap + transaction 内 `verify_schema`，随后独立只读 exact verify 通过；17 candidate、97 evaluation、53 batch、98 item 的迁移前后受保护内容摘要分别保持一致，新表在 runtime activation 前均为 0 行。
- `production_ddl_gate`：`applied_verified`（2026-07-22 获得精确授权 `GO PRODUCTION DDL HMM EVOLUTION V3` 后执行并回读）。
- `production_runtime_activation_gate`：`applied_verified`（2026-07-22 获得独立 worker restart 授权后，旧 PID 37024 退出；owner `service-aistock-hmm-v3-20260722` 的新 worker 启动，`/workers` 返回 200、`health=healthy`、`runtime_status=running`、连续失败 0，活动 batch 0）。
- 2026-07-22 Phase 1 production v3 activation的frontend/backend dependency gates：`noop`；Phase 2/3 scheduler仍未启用。
- 代码合入、生产 DDL 与 production runtime activation 仍是三个独立事实；本次三者均已分别取得证据，但不表示 Phase 2/3 已启用或 HMM 已接入 QE/Paper 生产链。
- Phase 2 controlled training dependency：仓库已声明`hmmlearn==0.3.3`，用户授权的Conda `AIstock`环境已完成no-deps安装与import/version/thread smoke且NumPy保持`2.4.0`；这不等于production backend runtime依赖已切换，也不授权服务重启或runtime activation。
- 本次v2.13父蓝图状态同步：`production_ddl_gate=noop`、`production_frontend_dependency_gate=noop`、`production_backend_dependency_gate=noop`、`runtime_activation=noop`、数据库与runtime无变化。

---

## 14. 附录

### 14.1 参考文档

- `hmm_evolution_phase1_offline_evaluation_detailed_design_20260717.md` - Phase 1 实现级详细设计
- `hmm_evolution_phase2_risk_monitoring_detailed_design_20260722.md` - Phase 2 实现级详细设计与当前模型验收权威状态
- `hmm_evolution_phase2_rotation_l1_g2a_detailed_design_20260903.md` - 新G2-A `rotation_L1`一次性F2详细设计；功效/交付双门、binding MBE、310/20叶结构、forward失败公式与共享market context已批准，其余精确D1～D6合同仍待用户批准
- `docs/architecture/research_pipeline_and_mcp_gateway_design_v2.md` - Research Pipeline 架构
- `docs/analysis/hmm_offline_diagnostic_qe_20260502_131502_9b54.md` - 离线诊断示例
- `docs/analysis/hmm_risk_gate_validation_20260517.md` - 风险门控验证
- `backend/tests/test_hmm_rolling_training.py` - 滚动训练测试

### 14.2 相关 Issue

- BUG-663: sector_data 恢复依赖安全
- BUG-312: HMM runtime cache issue
- BUG-076: HMM coefficients path

### 14.3 变更历史

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| v2.41 | 2026-09-04 | 按用户批准解耦research product与tail access；Rank IC 0.02成为唯一binding MBE，MDE只决定forward功效状态；批准310/20叶结构、HAC上界effect failure及跨horizon共享market context；真实OOF可闭合experimental surface但不得冒充capability。逐次历史迁至`hmm_evolution_phase2_decision_log_20260812_20260904.md`，更早版本保留于Git历史。 |

---

**文档归档路径**: `docs/architecture/hmm_evolution_and_risk_management_system_design_20260716.md`
