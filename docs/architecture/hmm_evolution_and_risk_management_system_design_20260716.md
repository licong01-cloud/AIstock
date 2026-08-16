# HMM 演进与风险管理系统总体蓝图（唯一产品目标权威）

> **版本**: v2.26
> **日期**: 2026-07-16  
> **修订日期**: 2026-08-16
> **状态**: Phase 0 已完成；Phase 1 全部外部验收完成（F-006～F-010A verified）且 production v3 已激活。Phase 2 历史 B3/P6/TRAIN-STABILITY/TRANSITION-DWELL-B 继续保持 blocked 事实。P2-3A market-regime + pooled sector-relative jump v2 已完成正式开发区间执行：market component 通过并在完整开发区间使用 `lambda=4.0`，L1 relative 在 `296/456` fits 时因六个 lambda 的三折 spread coverage 均不足而以 `hmm_risk_jump_selection_unavailable` fail closed，L2 未执行；18 个 L1 lambda×fold 的正式 spread 全为负，Rank IC 仅 3/18 为正，故该候选正式为 `NOT_AVAILABLE_FOR_PROMOTION`，未访问 holdout、未选择完整候选、未写 model/READY/DB/runtime。2026-08-16 用户批准下一方向及 C-011-P2-3B-D1～D6 精确合同：保留 market regime 分解，sector 预测改为直接面向未来相对收益的低维、透明、train-only 横截面 Ridge 模型；不得靠放宽 5-sector/80% 覆盖、反转旧 hidden label、扩大 seed/lambda 或只交付 market 子集修复。精确设计已具备实施依据，但源码与184-fit spike尚未实施。F-011继续blocked，F-012设计已批准，F-013尚未交付；严格进度仍为`11/17=64.71%`，canonical模型、FULL_READY、COVERAGE_AVAILABLE和真实预测/API/UI仍为0。本次DDL/DML/dependency/runtime均为`noop`（§13）
> **范围**: HMM 快速演进、风险监控、滚动训练、数据隔离  
> **作者**: Kiro (Claude Code)
> **维护者**: AIstock HMM Evolution

---

## 1. 执行摘要

### 1.0 权威、最终产品目标与反过度工程边界

- 本文件是 HMM Evolution Phase 0-3 的**唯一产品目标蓝图**。Phase 1/2/3 实现级详细设计只能展开本蓝图已经定义的产品结果、业务语义和验收项，不得反向新增产品目标、改变优先级或把诊断/证据基础设施升级为独立交付目标；发生冲突时先修订并经用户确认本蓝图，再同步从属详细设计。
- AIstock 唯一开发规范仍是 `docs/standards/aistock_development_standard_v1.5_20260523.md`；“唯一产品目标蓝图”不复制或替代开发规范。
- Phase 2 最终产品结果固定为：基于因果可用的 t-1/PIT 数据生成申万 L1/L2 板块状态预测，识别状态转移并形成可解释风险/机会预警，提供历史时序、误报/漏报和稳定性分析，并通过真实 API/UI 展示；结果只作研究分析，不进入交易决策链。
- Phase 2 的主要产品验收单位固定为“交易日 × L1/L2横截面”：模型必须证明其因果输出在未见数据上区分板块相对走强、走弱与风险，而不是要求每个sector在单一窗口内各自取得三态结构合格证。逐sector结构证据仍是语义可用性依据，但不得继续以全局合取垄断产品交付。
- Phase 2 使用四层验收：第一层为fit/convergence/covariance/posterior等数值安全；第二层为逐sector semantic availability；第三层为预注册walk-forward与untouched holdout上的横截面产品有效性；第四层为coverage及其行业/规模/流动性代表性。上层通过不能掩盖下层失败，底层局部失败也不得被改写成全产品成功。
- 完成状态严格分为：`FULL_READY`（canonical模型覆盖全部批准L1/L2范围）、`COVERAGE_AVAILABLE`（仅在用户批准的coverage与代表性合同、产品指标均通过时，输出已验收sector并对其余项显式返回typed不足）、`NOT_AVAILABLE`（产品指标或coverage合同不满足）。`COVERAGE_AVAILABLE`不是FULL_READY，禁止neutral补态、隐藏分母或无条件fallback。
- legacy与autocycle继续作为历史研究family；后续产品只允许一个经预注册样本外产品协议选定的canonical authority。两family共同交付、竞争择一或形成组合必须由精确F2合同裁决，不能因历史存在而默认要求全部READY，也不能由实现自行淘汰任一方向。
- 允许评估预注册、严格train-only的per-sector restart selection：选择规则必须在validation前冻结，D6/holdout失败后不得换seed、refit或扩大grid，全部identity进入receipt。它与validation-driven stitching严格区分，但尚未成为active模型合同。
- P2-3A 已证明“按 contemporaneous `excess_return_Nd` centroid 为 pooled K=3 jump state 命名”不能在冻结开发 folds 上产生稳定正向 10D 轮动预测。后续 sector 产品标签必须直接表示冻结模型对未来相对走强/走弱的预测，不得把描述性 hidden-state index、当前强弱或简单反转旧标签冒充预测语义。连续 `rotation_score`、离散 `trending/neutral/fading` 与 market `risk_on/risk_off` 必须分别建模和报告。
- HMM fit、seed选择、hash、fresh-process一致性、manifest和acceptance receipt只是在无法替代的范围内证明模型可复现、可验收的手段，不是用户功能，也不构成Phase完成度。
- 允许新增的持久化仅限：最终模型/模型集、最小身份manifest、紧凑selection/acceptance/failure receipt，以及产品需要的日度预测、预警、事件和回溯报告。默认禁止重复物化完整历史输入、为相同模型生成多份大体积JSON、建设通用evidence平台、通用训练平台或Phase 3调度器；确有不可替代需要时必须先证明其直接解除F-011/F-012/F-013 blocker，并由用户确认蓝图范围变化。
- 已存在的大体积历史artifact保持只读、不可改写；不为“清理历史”开启迁移工程，也不再把它们复制到新的artifact或数据库。
- 后续每个任务必须直接映射到F-011（模型可验收）、F-012（advisory-only生成）或F-013（预测/预警/API/UI/报告）之一。不能直接推动三者、也不是修复其真实blocker的任务，优先级为停止，不进入实现。

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
| `/hmm-risk` | L1/L2 板块状态热力图、预警、固定详情和状态分布 | F-011～F-013 验收通过 |
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

### Phase 2: HMM 风险监控与预警系统（Week 4-5）

**目标**: 按最新完成交易日生成可解释风险预警和板块状态视图，不接入任何交易决策。

**当前实现与模型验收状态**：独立 F2 实现级详细设计
`docs/architecture/hmm_evolution_phase2_risk_monitoring_detailed_design_20260722.md` 已明确唯一的
versioned sector-state generator、候选/模型/系数身份、行情与行业映射共同水位、freshness/revision/dedupe、
事务失败收敛、worker stale/cancel/timeout 以及 exact schema/API contract。C-001-A/C-002-A/C-003-A 已获用户批准：
13 个 candidate 是 direct state producer并共享两个 L2 model identity，4 个 pooled candidate 明确是 coefficient-only；
L1/L2 使用成对独立 direct HMM，不聚合 posterior；回溯使用 5/10/20 连续证据与 5D excess q20 次级 oracle。
Decision C-004 明确冻结 `scripts/precompute_hmm_risk_gate.py`、`hmm_risk_gate_v1` 与现有 Selection/QE consumer，
本阶段不迁移、包装或退役旧业务逻辑。设计修订不等于 schema/backend/API/UI/runtime 已交付。

C-009证券身份/causal circ-mv/provider-absence NA与C-010 full-universe contributor、双层coverage、逐feature cross-section
政策源码和601日formal preflight已闭合，当前blocker不是输入覆盖。C-008-B3的D3-01-A/D3-02-B/D3-03-A、
D4-01-A/D4-02-A/D4-03-B、D5-01-B/D5-02-B、D6-01-B和D7-01-A均已批准并进入Slice 0源码；仓库固定
`hmmlearn==0.3.3`，用户授权的Conda `AIstock`环境保持NumPy `2.4.0`不变并完成dependency/import/thread identity验证。

formal producer `e2c01bae156281d551b084156fec4a09ed5a84ee`在冻结input上完成两次fresh-process、`5184/5184`
fits；formal canonical SHA-256=`e7992f87fb555eb26d6c2ef1ad9d45863954edd83fbfcc39f5ae01765cf3939f`。
D5严格train-only、未读取validation/future utility且selection后未refit；只有`legacy_covfix:L1/seed=43`被选中，
随后`801980.SI`在D6 hard temporal structure evidence失败。其余三个family/level无eligible D5 candidate，两family均
blocked，model/READY、database与runtime write/action均为false。

`C-008-B3-FORMAL-BLOCKER-DIAG-01`又按formal rejection summaries完成150 rejected+24 controls、348/348 targeted fits
和3-entry D6 no-refit replay；producer=`ac3687c2e56d000a1fae6d196a8334e46060b07b`、canonical SHA-256=
`10287e845f07bf3d9c15a68e5d09ad14e54613348824ac2af568f0244a1cffe8`。证据证明blocker跨initialization、likelihood、
covariance与train structure，不支持只换seed、只放宽阈值、排除family/sector或使用validation reselect。

`C-008-B3-REMEDIATION-DIAG-02`已按批准合同完成：producer=`b2456424b859f1635635129aa6a826a677f4fdec`、canonical=
`48157a4255e9d19b814b26b90b18ec38769e28fd0a18e58403edb83fc660bb58`；324/324 train profiles、163/163 completed entries与
11/11 initialization sources闭合，0 HMM refit/D5/D6/validation/model/READY/DB/runtime。证据只确认唯一zero-variance profile、
46个train-structure failed entries、4个跨8-seed persistent sector identity与6个statistic-insufficient groups，不批准模型修订。

F-011当前为`APPROVED_BY_USER_SOURCE_IMPLEMENTED_FORMAL_EXECUTED_C010_A5_MERGED_D1_REFIT_03_DIAGNOSTIC_COMPLETE_INCONCLUSIVE_D5_COMPAT_PENDING_BLOCKED_MODEL_ACCEPTANCE`。
用户已选择`C-008-B3-REMEDIATION-D1-B`显式inactive-dimension identity设计方向，D1-A为`NOT_SELECTED`；P1已实现exact-zero authority、
固定20→19 projection、identity20 control与受控report。REFIT-03已在current authority冻结bundle上完成两fresh-process、三角色、
seeds 42..49的48/48真实fits：19D treatment和20D harness均16/16 `fit_completed`且descriptive covariance accepted，matched 20D 16/16 covariance failed；raw exact evidence
呈三seed inactive-only、五seed active/cross-role的mixed pattern，因此mechanism保持inconclusive，D5 readiness=false。下一设计决策必须明确
D1-B只作为level-local engineering robustness而不宣称唯一统计因果机制，并决定D5 19/20维score comparability；当前推荐effective-dimension
`L_final/(N*d_i)`且仍要求131/131 eligibility、train-only min/median/mean selection与公式/identity/131-entry完整性直接测试。正式mixed-dimension
writer/parser、2096-fit affected-level训练、D5/D6、READY、runtime/API/UI均后置。F-012保持设计批准，F-013继续等待完整READY model set；
generator/job/API/UI/runtime均未实施。

**数据库 Schema**：父设计不复制一份可能漂移的简化 DDL。唯一 implementation-level schema contract 位于
Phase 2 详细设计 §8，精确定义 `daily_generation_run`、`sector_state_timeline`、`daily_alert`、`risk_event`、
`retrospective_report`、三个 current views、所有 columns/types/nullability/check/FK/unique/index/comment 与 exact verify。

`sector_state_timeline` 是热力图的权威数据源；`daily_alert` 只保存满足规则的当日预警；
`risk_event` 聚合连续多日事件生命周期。三者不得相互替代或由前端临时推导持久化事实。
迟到数据必须插入 `revision+1` 并通过 `supersedes_*` 指向上一版；读取当前态取同一 dedupe key
最大 revision，禁止原地覆盖历史证据。

`hmm_state` 仅允许 `trending/neutral/fading`。`severity` 是基于前后状态和规则版本得到的
`NONE/HIGH/MEDIUM/OPPORTUNITY`，不是第四种 HMM 状态。`state_confidence` 仅在候选产物提供
可验证的状态概率或经批准的版本化定义时写入；否则必须为 null，并在 UI 显示“未提供”，不得从
severity、收益、颜色或任意 score 拼出伪置信度。

**预警语义**:

- `trending -> fading`：HIGH；持续 `fading`：MEDIUM；`fading -> trending`：OPPORTUNITY。
- 规则仅产生分析记录和解释证据，不产生 `can_buy`、调仓、订单或 profile 更新。
- 新域不复用或迁移 `hmm_risk_gate_v1` 的 artifact 解析、状态计算或 Selection provider；candidate adapters 只按 C-001-A capability matrix 与 C-002-A direct state-model-set 实现。
- 每日任务必须先校验 market/sector/候选 coefficient 的共同完成水位；缺数据时记录 failed/partial 和 reason code，不得用中性状态或旧日结果伪装成功。
- 同一 `candidate + trade_date + sector + rule_version` 幂等；迟到数据通过 revision 重算并保留历史，不覆盖审计轨迹。
- L1/L2 归属使用 `as_of_date` 对应的 PIT 申万映射和 `mapping_snapshot_hash`；不得用当前成员关系
  回填历史热力图。

**API 端点**:

```text
GET  /api/v1/hmm-risk/overview
GET  /api/v1/hmm-risk/heatmap?sector_level=L1&start_date=...&end_date=...&candidate_id=...
GET  /api/v1/hmm-risk/alerts?trade_date=...&sector_level=L1&candidate_id=...
GET  /api/v1/hmm-risk/sectors/{sector_code}/timeline
GET  /api/v1/hmm-risk/events/{event_id}
POST /api/v1/hmm-risk/jobs/daily/preview
POST /api/v1/hmm-risk/jobs/daily/run
GET  /api/v1/hmm-risk/jobs/{run_id}
POST /api/v1/hmm-risk/jobs/{run_id}/cancel
GET  /api/v1/hmm-risk/reports
GET  /api/v1/hmm-risk/reports/{report_id}
```

preview 为零写入；run 只写 `hmm_risk.*`，必须幂等并由受控 runner/worker 执行。普通页面读取不增加
审批或确认链。

**UI 契约**:

- `/hmm-risk` 是最终 HMM 工作台默认首页，顶部保留“演进实验室 / 板块风险 / 滚动训练”三个
  一级页签；只有已完成真实 API/UI 验收的页签才注册为可用导航。
- 主视图按申万 L1/L2 切换，默认显示最近 7 个完整交易日；热力图行是板块、列是交易日，
  基础填充色只表达 `trending/neutral/fading`，单元格数字在 `state_confidence` 非 null 时表达
  状态置信度。HIGH/MEDIUM/OPPORTUNITY 使用边框、角标和文字表达，不得伪装成新的 HMM 状态。
  颜色不是唯一语义载体，必须同时提供文字、图例和可访问标签。
- “状态热力图”是分类状态的图形化表达，不新增或暗示交易吸引力、资金强弱、可买性或
  未经定义的 `heat_score`。若未来增加独立板块热度指标，必须先定义数据源、公式、版本和验证，
  不得从 severity/confidence 临时拼接。
- 点击热力图单元格在页面下方固定详情区更新 sector、trade date、candidate、state、confidence、
  transition、rule version、watermark、revision 和可读解释；不打开抽屉或侧滑列表。
- 今日预警使用页面内卡片/表格，固定展示 HIGH/MEDIUM/OPPORTUNITY、证据完整度和原因；
  详情使用独立路由或固定页面区域，不显示 raw evidence JSON。
- 顶部固定显示共同数据水位、candidate、rule version 和研究分析声明；数据不足时热力图整体
  进入 degraded/failed 状态并标明缺失域，不用旧日数据或 neutral 色块伪装成功。
- 图表 renderer 加载失败时显示 `hmm_risk_chart_renderer_unavailable`、中文原因和重试动作；
  可同时展示同一响应的结构化状态表作为可访问证据，但不得把表格 fallback 标记为热力图成功。

**验收标准**:
- [ ] 日度预警任务可重跑、可审计、可解释，失败不影响 Selection/Paper/QMT。
- [ ] 热力图显示数据水位、candidate、rule version、状态置信度和缺失原因。
- [ ] 申万 L1/L2、最近 7 日、单元格选择、固定详情区、今日预警和状态分布均由真实 API 数据驱动；
  禁止用静态矩阵、mock-only 页面或硬编码预警冒充完成。
- [ ] renderer/API/数据失败均有可见 reason code、中文解释和终止状态；不得永久 loading 或空白。
- [ ] UI 明示“仅供研究分析，不构成交易决策”。
- [ ] 风险回测报告展示命中率、误报、漏报、样本量和分阶段稳定性，不设置未经批准的保护率硬门禁。

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

### Gate 2: Phase 2 - 板块状态预测、分析与风险预警

Gate 2 不再按“先建设完整平台、最后才验证产品”的横向基础设施顺序执行，改为以下纵向业务闭环；每一步都必须产生可被下一步直接消费的真实结果：

1. **P2-1 当前11个D6失败根因闭合（已完成）**：正式artifact与独立重算证明11项均非程序/数据/NA缺陷，而是selected seed 43在validation窗口的hard-state缺失、极低占用、transition不足或单run过度集中；未复制多GB artifact、未扩大seed、未放宽阈值、未写READY。
2. **P2-2 既有证据紧凑聚合与范式决策（已完成）**：既有两个TRANSITION-DWELL-B child artifact已完成零refit聚合并把唯一P2-3A候选收敛到market-regime + pooled sector-relative jump；该步骤没有选择K、seed/family或写model/READY。
3. **P2-3 产品验收精确合同与单一spike（P2-3A已失败，P2-3B精确设计已批准）**：P2-3A正式开发执行已以真实横截面产品指标判定 `NOT_AVAILABLE_FOR_PROMOTION`，不得重跑相同grid、降门禁、反转label或把market局部成功当完整候选。用户已批准P2-3B方向及D1～D6精确合同：market regime保留，L1/L2 sector层只评估一个直接预测10D未来相对收益的低维透明 pooled Ridge模型，并以连续rotation score和横截面forecast state服务产品。下一步是按已批准合同实施源码和184-fit候选；禁止与t-emission/shared-prior/深度模型并行或建设通用训练平台。
4. **P2-4 canonical产品模型与状态闭合**：以样本外横截面Rank IC、trending-fading spread、风险事件precision/recall、分阶段稳定性及coverage代表性裁决canonical family/model。精确公式、horizon、事件标签、阈值和多重试验处理必须在实现前获批；不得用likelihood/occupancy单独宣称产品有效，也不得访问最终holdout做选择。只有全部canonical范围通过才为FULL_READY；coverage路径必须使用独立可见状态，失败时为NOT_AVAILABLE。
5. **P2-5 最小离线预测纵切**：使用通过P2-4的同一canonical输出生成一个完整交易日的market regime、L1/L2相对强弱/状态、transition、severity、availability和可读原因；先验证真实预测业务oracle，不先建设通用job平台。
6. **P2-6 历史分析与产品化纵切**：对批准窗口生成横截面指标、状态/预警时序、命中/误报/漏报、覆盖率与偏差，并在最小schema/repository/API/UI中展示同一结果；不得交付backend-only、静态页面或隐藏不可用sector。
7. **P2-7 受控日任务与集成验收**：产品纵切验收后才补幂等日任务、revision/late-data和受控runner；不提前建设通用调度器，Phase 3仍独立。

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
- **F-011 Phase 2 canonical模型、分层验收与预警状态机**：数值安全、逐sector语义可用性、横截面产品有效性、coverage状态、共同水位、规则版本、dedupe/revision、解释证据和迟到数据重算完整；FULL_READY/COVERAGE_AVAILABLE/NOT_AVAILABLE不可混用。
- **F-012 Phase 2 advisory-only**：无 `RiskDecision`、`can_buy`、订单、持仓、配置或调仓副作用。
- **F-013 Phase 2 风险分析与 UI 证据**：`/hmm-risk` 为最终默认首页，market regime、L1/L2相对强弱/状态热力图、
  今日预警、固定详情、状态分布、横截面Rank IC/spread、命中/误报/漏报、coverage及阶段稳定性完整；状态/置信度/severity
  语义分离，不擅自新增 heat score 或硬门禁。
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
7. **P2 板块状态预测、分析与风险预警**：既有B3/P6/D5/D6/TRAIN-STABILITY/TRANSITION-DWELL-B全部保留为历史决策证据且READY=0。P2-3A jump spike已被真实产品指标判定NOT_AVAILABLE；P2-3B direct cross-sectional predictor精确合同已闭合并获用户批准，后续只实施这一条最小候选。通过后按FULL_READY/COVERAGE_AVAILABLE/NOT_AVAILABLE进入P2-4～P2-7；失败则保持NOT_AVAILABLE并停止，不自动开启第三模型方向。禁止扩展通用动态维度/evidence/训练/调度平台、重复完整输入物化，或用diagnostic、fit、market子集和coverage状态冒充产品完成。每个PR合入仍须用户逐PR确认。
8. **P3 研究训练**：F-014 research-only rolling candidate 与 F-015 manual-first/automation boundary 只有跨阶段方向；必须先建立独立实现级 F2 设计、Design Acceptance Index 和验证矩阵。自动调度仍未批准，不得直接进入代码或复用旧 production training tick。

每个实现 PR 只承担一个可验证 slice，并在 PR body 中列设计项、实现引用、验证证据、生产门禁与未批准缺口；
slice 不得被误报为整个 Phase 完成。Phase 0 BUG 修复走 issue workflow；Phase 1-3 新能力走 feature workflow。
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

## 11. Design Acceptance Matrix（设计验收矩阵）

本表记录 v2.19 设计验收状态；`implementation_refs` 和 `test_or_evidence` 中的“目标”不是完成声明，每个实现 PR 必须将对应行替换为真实引用和证据后才能报告该设计项完成。

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
| F-011 | 父蓝图v2.26；Phase 2详细设计§4.3.4.2～§4.3.4.3；历史B3与P2-3A源码/receipts只作输入 | P2-3A v2 failure receipt canonical `034fdf3c…12ec`：296/456 fits、L1 18/18 spread<0、无holdout/model/READY；18-fit exact replay hash闭合；C-011-P2-3B-D1～D6用户批准记录 | DESIGN_READY_USER_APPROVED | canonical模型=0，FULL_READY=0，COVERAGE_AVAILABLE未建立。P2-3B精确合同已批准但源码与184-fit spike未实施；不得降coverage、翻转旧label、复用market子集、并行模型或用fit/receipt增加产品完成度 |
| F-012 | Phase 2 F2 详细设计 §14：advisory-only service boundary | `backend/tests/hmm_risk/test_isolation.py`（目标路径，断言 Selection/Paper/QMT 无写入） | DESIGN_READY_USER_APPROVED | 用户明确批准 legacy producer/consumer 冻结与 advisory-only 隔离；源码与结果证据待实现 PR 回填 |
| F-013 | Phase 2 F2 详细设计 §9～§11：C-003-A report + `/hmm-risk` 默认首页 | `backend/tests/hmm_risk/test_retrospective_report.py`、`frontend/tests/hmm-risk/hmm-risk.spec.ts`（目标路径：L1/L2/7 日 heatmap、固定详情、预警、renderer/error、report denominator） | USER_APPROVED_PENDING_UPSTREAM_MODEL_SET | 用户明确批准 API/UI 合同；真实验收依赖 F-011 READY model set，源码未实施 |
| F-014 | 本文 Phase 3 UI/隔离方向；research-only rolling candidate + `/hmm-research-training` | `backend/tests/hmm_training/test_rolling_research_training.py`、`frontend/tests/hmm-training/hmm-training.spec.ts`（目标路径，尚未建立） | APPROVED_BY_USER_DIRECTION_ONLY_PENDING_IMPLEMENTATION_LEVEL_DESIGN | 用户批准跨阶段方向；不得从父蓝图直接编码，身份、训练任务、artifact、状态机、API/UI 和验证合同待独立设计 |
| F-015 | manual-first 与未来 scheduler 安全边界 | `backend/tests/hmm_training/test_scheduler_contract.py`（目标路径，尚未建立） | APPROVED_BY_USER_MANUAL_FIRST_DIRECTION_AUTOMATION_NOT_APPROVED | 用户批准 manual-first 边界；自动调度需另行明确业务语义，不得安装 scheduler 或启用旧 tick 冒充实现 |
| F-016 | 全阶段 isolation guard | `tests/aistock_validation/test_hmm_evolution_isolation.py`（目标路径：scope/production-gate/side-effect） | APPROVED_BY_USER_DESIGN_READY_PENDING_PHASE_IMPLEMENTATION | 用户明确批准隔离语义；结果证据由对应 Phase 实现 PR 回填 |

### 11.1 2026-08-12 蓝图偏离全面审核

| 审核维度 | 蓝图目标 | 实时事实 | 偏离结论 | 本次修订与后续强制边界 |
|---|---|---|---|---|
| 产品目标权威 | Phase 2交付L1/L2板块状态预测、风险/机会预警、历史分析与真实API/UI | 从属Phase 2详细设计比父蓝图更新，后续任务曾由诊断章节驱动 | **存在权威倒置** | 本文件升级为唯一产品目标蓝图；从属设计只能展开F-011/F-012/F-013，冲突先修父蓝图 |
| 模型研发 | 训练和验收用于获得可解释、可复现且能预测横截面轮动的canonical模型 | P6/D6/train-stability/TRANSITION-DWELL-B均诚实保留，但旧合同把逐sector结构、level和family串成全局合取 | **目标一致、验收单位偏离** | 数值/D6证据降为安全与逐sector语义层；产品主验收改为日期×横截面样本外效果和coverage，历史结果不反写成功 |
| 预测与预警 | READY后生成日度状态、transition、severity、alerts和retrospective report | 当前源码无`state_generator`、`alert_state_machine`、job/router或`/hmm-risk` UI | **核心产品纵切尚未开始** | READY后立即做单日离线预测与历史分析，再以同一结果完成最小API/UI纵切 |
| 证据与物化 | 只保留可复现和READY所需的最小身份、selection、acceptance/failure证据 | 已形成多份数百MB至GB级preflight/candidate/fresh-process JSON与大量DIAG/REFIT正文 | **存在过渡工程化** | 冻结既有artifact；禁止复制、迁移或建设通用evidence平台；新证据默认紧凑摘要和内容寻址引用 |
| 实施顺序 | 业务结果优先且分片不得冒充Phase完成 | 旧计划按schema/repository/job/API/UI横向分层，可能延后真实预测验证 | **顺序偏离** | 改为P2-1～P2-7纵向闭环：blocker→模型→READY→预测→分析→API/UI→日任务 |
| 历史数据 | PIT/训练/validation数据只服务因果模型和结果复现 | 存在把冻结输入、历史drift和artifact修复作为连续主线的风险 | **边界需收紧** | 不开展历史数据迁移/清理工程；仅在当前模型输入事实错误时修复，并直接关联F-011 |
| 验收与门禁 | fail-closed、因果、产品有效且覆盖可见 | 单sector D4/D6已偏宽，但sector×level×family全局合取长期阻断产品 | **系统合取需修订** | 保留D3/D4与逐sector D6诚实性；允许预注册train-only per-sector restart候选，family由产品holdout裁决；三种完成状态分离且coverage不得冒充FULL_READY |
| 进度口径 | 只有17项产品验收行可增加完成度 | fit、诊断、BUG和receipt数量一度占据主要状态篇幅 | **报告重心偏离** | 严格进度仍为11/17；诊断、fits、artifact、PR数量只作支撑事实，不计产品完成度 |

审核结论：总体业务方向未迁移，模型合同的fail-closed边界仍正确；但权威层级、实施顺序和证据投入已出现明显偏离。v2.20已完成目标收敛，后续不得自行恢复“先扩诊断/平台、后做产品”的顺序。任何声称必须新增基础设施的任务，须在用户确认前同时给出其直接解除的F-011/F-012/F-013 blocker、最小替代方案、额外成本和不做的业务影响。

### 11.2 DESIGN-COMPLIANCE-001 正式审核

- **禁止简化交付：PASS**。COVERAGE_AVAILABLE是经批准的独立产品状态，不是FULL_READY别名；它必须同时通过产品指标与coverage代表性合同并显式展示不可用sector。任何局部fit、spike、单family或少量sector不得声明Phase 2完成。
- **禁止静默错误：PASS**。历史11个D6失败继续保留typed evidence；逐sector unavailable、coverage分母/偏差与NOT_AVAILABLE必须可见，禁止neutral补态、删sector、validation reselect或用空结果伪造预测成功。
- **禁止业务逻辑迁移：PASS（用户批准范围调整）**。2026-08-14用户明确把产品主验收单位调整为日期×横截面预测有效性，并批准四层验收和三状态语义；PIT、因果、advisory-only及数值fail-closed不变。旧B3仍按旧合同保持blocked，不被新语义追认。
- **禁止未经确认的门禁和审批：PASS**。本蓝图只批准方向与状态机，不擅自设定Rank IC、spread、precision/recall、coverage或holdout阈值；这些精确值必须进入从属F2设计并由用户确认，不形成runtime人工审批。

正式结论：`PASS_BLUEPRINT_PRODUCT_ACCEPTANCE_REALIGNED_EXACT_SPIKE_CONTRACT_PENDING_NO_IMPLEMENTATION_AUTHORIZED`。该结论批准产品验收方向、分层状态与单一spike顺序，不表示历史D6失败已解决、任何family/canonical模型已选定、FULL_READY/COVERAGE_AVAILABLE已达成，亦不授权fit、源码、DDL或runtime。

### 11.4 2026-08-16 P2-3A 失败与 P2-3B 方向审核

- **P2-3A事实：PASS_FAIL_CLOSED**。v2正式执行在L1 lambda selection处以typed reason停止；296个已完成fit均非执行异常，L2未启动，holdout/model/READY/DB/runtime均未触碰。该结果只能写`NOT_AVAILABLE_FOR_PROMOTION`。
- **数据与程序归因：PASS_NO_DATA_OR_EXECUTOR_BUG**。三fold Rank IC覆盖均为116/116；18个selected-fit replay的centers/path/hash完全闭合。spread不可用由横截面trending/fading组不足导致，不是日期、provider或未来收益大面积缺失。
- **产品方向：FAIL_CURRENT_CANDIDATE**。正式min-5 spread在18/18个lambda×fold为负，Rank IC仅3/18为正；即便仅诊断性把每侧样本降至1，主流lambda仍不形成稳定正向结果。因此不得通过放宽80%/5-sector阈值或反转旧label继续P2-3A。
- **下一方向：PASS_EXACT_DESIGN_USER_APPROVED**。用户批准market regime + direct cross-sectional predictor作为唯一P2-3B方向，并批准从属F2设计中的target、模型参数、selection、state projection、停止条件和184-fit成本。它直接优化未来相对收益并输出rotation score/forecast state；批准的是确定性离线模型合同，不是runtime人工门禁，源码和spike仍未实施。
- **反过度工程：PASS**。P2-3B只允许一个低维透明模型、既有数据reader和紧凑candidate receipt；不并行t-emission/shared-prior/deep model，不建通用训练/证据平台，不迁移历史artifact。

正式结论：`PASS_BLUEPRINT_P2_3_A_NOT_AVAILABLE_P2_3_B_EXACT_DESIGN_USER_APPROVED_NOT_IMPLEMENTED`。这不是P2-3B源码、184-fit结果或产品成功声明。

### 11.3 2026-08-14 产品验收方向正式审核

本轮执行三轮文档审核。第一轮修复旧交付顺序、F-011矩阵和提案中残留的两family active合取、穷尽证明、子集READY及spike失败自动降级；第二轮确认历史B3与新C-011 authority分离；第三轮通过F2 validator并形成以下结论：

- **验收单位：PASS**。日期×canonical L1/L2横截面是产品主验收；D3/D4与逐sector D6继续承担数值安全和semantic availability，不被删除，也不再以sector×level×family合取替代产品效果。
- **状态闭包：PASS**。FULL_READY、COVERAGE_AVAILABLE、NOT_AVAILABLE互斥；coverage状态需要独立产品指标与代表性合同，不能因time-box/spike失败、120/131或单family自动成功。
- **选择隔离：PASS_DIRECTION_ONLY**。允许提出预注册train-only per-sector restart，但公式、schedule、tie-break与receipt未批准前不能执行；validation/D6/holdout失败后不得reselection。
- **family与范式：PASS_DIRECTION_ONLY**。legacy/autocycle角色由同一untouched holdout产品协议裁决；只允许一个spike，市场regime+sector relative strength优先，具体estimator由零refit失败聚合决定。没有批准A/B/C并行、HSMM/HDP-HMM或通用平台。
- **经济指标：PENDING_EXACT_CONTRACT**。Rank IC、spread、precision/recall、coverage与稳定性是必须覆盖的产品维度，但公式、horizon、阈值、事件标签、purge/embargo与多重试验处理未由本文自行设置。
- **历史真相：PASS**。旧B3继续blocked，11个D6 failure、model/READY=0与所有artifact identity不变；新蓝图不追认任何历史局部结果为产品成功。

审核结论为`PASS_BLUEPRINT_DIRECTION_COMPLETE_EXACT_F2_CONTRACT_REQUIRED_BEFORE_IMPLEMENTATION`。这不是新增人工审批，而是FEATURE-WORKFLOW-001要求用户批准方向后闭合精确设计的既有流程；在精确合同前禁止模型实现或fit。

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
| v2.26 | 2026-08-16 | 回填P2-3A zero-event v2正式执行：market完成并进入lambda=4.0 full-development，L1在296/456 fits处因六个lambda均无三fold spread coverage而typed停止，L2未运行；formal report canonical=`034fdf3c…12ec`。18个L1 selected-fit只读replay hash全部闭合，证明主要为neutral集中/trending不足，且18/18正式spread为负、Rank IC仅3/18为正；因此候选为NOT_AVAILABLE，不降80%/5-sector、不反转label、不交付market子集。用户批准下一唯一方向market regime + direct low-dimensional cross-sectional predictor及C-011-P2-3B-D1～D6精确Ridge/target/state/selection/fit-count合同；源码、184-fit spike、holdout、model/READY、DDL/DML/dependency/runtime均未实施。严格进度仍为11/17。 |
| v2.25 | 2026-08-14 | 用户批准Phase 2产品验收方向调整：主要验收单位由逐sector三态结构全局合取改为日期×横截面的板块轮动与风险预测；建立数值安全、逐sector语义可用性、样本外产品有效性、coverage四层合同，并分离FULL_READY/COVERAGE_AVAILABLE/NOT_AVAILABLE。允许预注册train-only per-sector restart作为候选；legacy/autocycle的canonical角色改由预注册产品holdout裁决。下一步先零refit聚合现有child evidence，再只执行一个spike；市场regime+sector相对强弱为首要结构候选，具体estimator由失败类型决定。旧B3及其11个D6失败不反写成功，精确指标/coverage/spike合同、fit、selection、model/READY、DDL/DML/dependency/runtime均未授权；严格进度仍为11/17。 |
| v2.24 | 2026-08-13 | 回填`TRANSITION-DWELL-B`源码合入、BUG-1068 receipt lifecycle修复及正式受控实验：treatment producer `29417ceb…f8996fe`、冻结source `2ae9df85…be7fa`，双fresh-process完成`2096/2096` fits，entry/model/profile hashes bitwise一致；完整候选seed为0，状态`diagnostic_complete_no_complete_candidate`，parent body canonical `b6312171…582db`、完整对象canonical `e5f355fc…d4b54`。实验未执行selection/D5/D6、未写model/READY/DB/runtime。审核确认任务严格位于P2-2最小模型机制验证，未建设通用平台或迁移产品方向；下一步仅脚本化聚合既有child evidence并形成精确模型决策，不自动调参或重跑。严格进度仍为11/17。 |
| v2.23 | 2026-08-12 | 用户确认`TRANSITION-DWELL-B`精确合同：`hmm_risk_c008_b3_transition_dwell_b_v1`按train-only KMeans transition构造3×3 Dirichlet prior，self center `[0.50,0.90]`、`tau=8.0`，transition prior项纳入MAP objective；expected dwell只诊断，D4/D5/D6/hard authority与两family完整性不变。批准合同不授权源码、2096-fit实验、selection、D6或READY；严格进度仍为11/17。 |
| v2.22 | 2026-08-12 | 回填`TRAIN-STABILITY-DIAG-01`正式结果：producer `7d57d57e…d190`、1048/1048 profiles、131/131 source comparisons、canonical `9c449e04…c5b1`，8个seed双窗口stable sector为108/108/97/103/109/105/104/106且完整seed为0；0 refit/selection/D6/model/READY/DB/runtime。明确不采用会清空候选集的D5-only stability gate，下一步只提交待用户批准的`TRANSITION-DWELL-B`精确候选；该候选使用train-only transition MAP prior双向约束，不修改D4/D5/D6/hard authority。源码、2096-fit受控实验、正式启用与合入分别授权；严格进度仍为11/17。 |
| v2.21 | 2026-08-12 | 完成P2-1根因闭合：11项D6失败均为完整182日输入下的真实跨阶段hard-state结构不足，独立重算与正式receipt 0差异，排除程序和数据/NA缺陷。P2-2不直接修改HMM或D5，先提出待用户批准的`TRAIN-STABILITY-DIAG-01`：复用冻结8×131模型与source identities，以只读constructor内存重建train输入并逐hash闭合后，对两个互斥182-observation train-only窗口零refit重算hard结构；source drift时0 profile fail closed。根据结果在D5 eligibility修订与transition/dwell模型修订间二选一。未实施诊断、fit、selection、D6、model/READY、DDL/DML/dependency/runtime；严格进度仍为11/17。 |
| v2.20 | 2026-08-12 | 全面审核蓝图与Phase 2实时状态：确立本文件为唯一产品目标蓝图，固定Phase 2最终结果为因果L1/L2板块状态预测、风险/机会预警、历史效果分析和真实API/UI；回填P6 2096 fits、D5 seed43、BUG-1029后D6 120/131 accepted与11 failures。登记权威倒置、横向平台优先和大体积证据投入三类偏离，改为P2-1～P2-7产品纵向顺序；冻结既有历史artifact，禁止通用evidence/训练/调度平台、重复完整输入物化和用diagnostic/receipt增加完成度。严格进度仍为11/17；DDL/DML/dependency/runtime均noop。 |
| v2.19 | 2026-08-06 | 回填REFIT-03真实执行：producer `b474170f…`、48/48 fits、report canonical `7e8a1755…76b9`、两process payload `53574f62…088f` bitwise equal；19D treatment/20D harness各16/16 `fit_completed`且descriptive covariance accepted，matched 20D 16/16 covariance failed且呈三seed inactive-only、五seed active/cross-role mixed pattern；formal acceptance=false。机制保持inconclusive、D5 readiness=false。新增待用户决定的D1 level-local engineering解释与D5 effective-dimension方案A；获批后才实现mixed-dimension artifact/parser及公式/identity/131-entry完整性直接测试，随后运行受影响L2 2096 fits。未执行selection/D6/model/READY/DDL/DML/dependency/runtime，严格进度仍为`11/17=64.71%`。 |
| v2.18 | 2026-08-03 | 用户批准REFIT-02-A后完成最小runner、current-A5 authority、same-sector matched receipt、historical drift receipt与v4 immutable writer/readback源码，并通过正式代码审核。源码仍位于独立worktree，尚未commit/PR/merge；真实32-fit、D5/D6、mixed-dimension artifact/parser、model/READY、DDL/DML/runtime均未执行，严格进度仍为`11/17=64.71%`。 |
| v2.17 | 2026-08-03 | 收敛BUG-962与REFIT-01真实结果：C-010-A5 authority/readback程序缺陷已修复，但current-A5 `train_observation_sha256`与历史frozen train core不同，REFIT-01在0 fits处inconclusive且不可继续。新增待用户批准REFIT-02-A：同一current-A5的801207 19D treatment、801207 identity20 matched negative与801011 identity20 harness，两fresh processes固定48 terminal attempts/32真实fits；历史payload仅作只读drift审计。设计审核通过但未实施源码/fit/D5/D6/model/READY，严格进度仍为`11/17=64.71%`，DDL/DML/dependency/runtime均noop。 |
| v2.16 | 2026-07-31 | 回填D1-B P1最小必要源码：共享artifact-neutral train-only HMM核心、exact-zero authority、full20→19固定projection、identity20 frozen-payload control、16-attempt no-early-stop process、双process report与immutable canonical writer；32 fits、D5/D6、model/READY、DB/runtime均未执行。后续优先级保持P2真实受控训练，不提前实现P4/P9。严格进度仍为`11/17=64.71%`，DDL/dependency/runtime均noop。 |
| v2.15 | 2026-07-31 | 记录用户选择`C-008-B3-REMEDIATION-D1-B`、D1-A不采用；保持源码、32-fit执行与D5 19/20维comparability为独立pending。将Phase 2后续改为evidence-first：最小必要模型机制→32-fit真实受控训练→D5兼容决策→必要mixed-dimension artifact/parser→受影响`autocycle_all_core:L2` 2096-fit正式训练→该level D5/D6→其余blocker→两family READY→产品功能。明确不先建设通用动态维度框架、训练平台、调度器、API/UI或占位READY；分阶段执行不改变完整验收合同。严格进度仍为`11/17=64.71%`，本次仅文档修订，DDL/DML/dependency/runtime均noop。 |
| v2.14 | 2026-07-31 | 同步Phase 2 REMEDIATION-DIAG-02正式执行：producer `b2456424…`、canonical `48157a42…bb58`、324/324 profiles、163/163 completed entries与11/11 initialization sources闭合，0 HMM refit/D5/D6/model/READY/DB/runtime。登记D1-A/B精确设计、mixed-dimension level/family artifact、runtime inactive-observation receipt、32-fit controlled-refit结果分类与D5 19/20维comparability blocker；A/B和执行均未获批准。F-011与严格进度仍为blocked和`11/17=64.71%`，不把diagnostic/design completion计为交付。本次仅文档修订，DDL/DML/dependency/runtime均noop。 |
| v2.13 | 2026-07-31 | 同步Phase 2最新权威状态：D3-D7精确合同、C-009/C-010输入政策与Slice 0源码已完成；formal producer `e2c01bae…`完成5184/5184 fits但两family blocked、READY=0；blocker diagnostic producer `ac3687c2…`完成348/348 targeted fits与3-entry D6 replay。F-011更新为source implemented/formal executed/diagnosed/blocked，F-012保持design ready，F-013保持upstream pending；登记`C-008-B3-REMEDIATION-DIAG-02`为待用户确认的no-fit提案。修复F-002、Phase 1收官状态与F-011/F-012/F-013状态枚举过期，并区分Phase 1 production activation dependency gate与Phase 2 controlled-training依赖事实；按矩阵17个独立验收行记录严格进度`11/17=64.71%`。本次仅文档状态同步，DDL/DML/dependency/runtime均noop。 |
| v2.12 | 2026-07-25 | 同步 Phase 2 C-008-B3 最新权威状态：回填 STRUCTURAL-A、D3-01-A、D3-02-B、固定环境 D5-02-B、D7-01-A、DIAG-02 与 D4-02-DIAG-03；登记 DIAG-03 否定统一 `[1e-4,200]` zero-anomaly covariance 候选但未批准替代阈值；F-011 修正为 blocked、F-013 修正为 upstream pending。同步 Phase 3 父蓝图与实现就绪边界：F-014 等待独立实现级 F2 设计，F-015 自动调度未批准，防止从父蓝图直接编码。无源码、DDL、依赖或 runtime 变化。 |
| v2.11 | 2026-07-22 | 回填用户批准 C-001-A/C-002-A/C-003-A：17 个 candidate 明确分为 13 个 direct state producer（两个 L2 model SHA）与 4 个 coefficient-only non-state producer；采用成对独立 direct L1/L2 `hmm_risk_state_model_set_v1`，禁止 posterior aggregation；批准 5/10/20 连续回溯证据、5D excess q20 次级 oracle、90% coverage 与 OPPORTUNITY 单列；F-011～F-013 更新为 `DESIGN_READY_USER_APPROVED`。Phase 2 源码、DDL、UI、runtime 仍未实施。 |
| v2.10 | 2026-07-22 | 修复 Phase 2 正式审核 NEED-FIX：撤回 `DESIGN_READY`，登记 C-001 semantic state、C-002 L1/L2 source/aggregation、C-003 retrospective oracle 为用户待确认；移除自创 `neutral -> fading=HIGH`、L2 加权派生 L1、固定 5D/bottom-quantile oracle；Decision C-004 冻结 legacy gate，无迁移；补齐 exact schema/API、事务失败回执、worker stale/cancel/timeout 与 event resolution；登记所有 PR 合入均须用户逐 PR 明确确认。Phase 2 源码、DDL、UI、runtime 仍未实施。 |
| v2.9 | 2026-07-22 | 完成 Phase 2 独立 F2 实现级详细设计并通过 feature validator：固化唯一 state generator、candidate/model/input identity、共同水位、PIT mapping、L2 direct/L1 derived posterior、revision/dedupe/late-data cascade、durable job、advisory-only isolation、真实 API/UI/report、legacy v1 migration 与四个 implementation slices；F-011/F-012/F-013 更新为 `design_ready`，但源码、DDL、UI、runtime 仍未实施。 |
| v2.8 | 2026-07-22 | 回填 production v3 activation receipt：经精确授权完成 `hmm_evolution_v3` 单事务 DDL 与独立 exact verify，17 candidate、97 evaluation、53 batch、98 item 的受保护内容摘要不变；随后经独立授权重启 HMM worker，durable `/workers` 状态为 healthy/running、连续失败 0、活动 batch 0。将 `production_ddl_gate` 与 `production_runtime_activation_gate` 分别更新为 `applied_verified`；dependency gates 保持 `noop`，Phase 2/3 与 QE/Paper 接入仍未启用。 |
| v2.7 | 2026-07-22 | 按只读复核 NEED-FIX 修正 §13 gate truth：`hmm_evolution_v1/v2` production `applied_and_verified` 保留为历史事实；明确 `hmm_evolution_v3`（execution_purpose/benchmark_id 列、performance_receipt、worker_runtime_status）DEV `applied_and_verified`、production `pending`（未授权、未执行）；`production_ddl_gate=pending`、`production_runtime_activation_gate=pending`、dependency gates `noop`；登记代码合入≠生产 DDL 已执行≠生产 runtime 已加载 v3，v3 生产 DDL 须单独获得 `GO PRODUCTION DDL HMM EVOLUTION V3`。 |
| v2.6 | 2026-07-22 | 回填 Phase 1 收官外部验收（详细设计 §17.4.6）：登记用户"Loop1 + h10 spec"fallback 裁决；zerocopy 1c/10c 与 fallback cold/warm 全 benchmark matrix（分阶段 timing/peak RSS/cache evidence/result_hash 确定性）；真实 UI/Playwright 18 场景全过；worker 31.6 分钟 bounded soak 六类事件；F-008/F-010/F-010A 标记 verified，Phase 1（F-006～F-010A）全部 verified。 |
| v2.5 | 2026-07-21 | 回填 BUG-800/BUG-801/BUG-804 后 pre-ST-PIT 9/9 真实回执；按 Phase 1 详细设计修正 worker 中断语义为 lease 过期 fail-closed `timed_out` + 显式 retry 新 generation，并记录 3 候选中断批次与 2/2 retry；保留严格冷热缓存、长期监督和真实 UI/Playwright 缺口。 |
| v2.4 | 2026-07-21 | 回填 schema v2 worker、BUG-788/BUG-789 与 Loop1～Loop10 真实运行 receipt；登记 BUG-798，并定义 donor identity、config/pool/artifact/coverage 全固定且 provenance 显式的 pre-ST-PIT cross-loop compatibility，禁止 live/近似/静默 fallback。 |
| v2.3 | 2026-07-20 | 回填 BUG-775 `hmm_evolution_v2` DEV-first 生产 DDL receipt：exact schema/constraint/index/comment verify、受保护数据摘要不变、repository read smoke 通过；明确 API/worker 未重启，运行时激活仍与 DDL 分开报告 |
| v2.2 | 2026-07-19 | 对齐 BUG-773～BUG-777：修正 BIGINT 行情收益、加入版本化 value/missing-evidence content hash 与历史 known-invalid 语义、API durable preparation receipt、`metric_availability_ratio`、真实 end-to-end 性能口径、legacy ST-PIT 兼容约束、Phase 2 state-generator F2 前置和可核验验收矩阵；schema v2 生产 DDL 独立 pending |
| v2.1 | 2026-07-19 | 回填 BUG-772/PR #2471 和真实 Loop10 v2 外部 receipt：Windows 独立 worker 首次 activation、API 202、冻结 ST-PIT runtime artifact 重放、约 121 秒终态成功；保留 10-case、10 候选、lease recovery 与真实 UI/Playwright 缺口 |
| v2.0 | 2026-07-18 | 批准 Phase 1 自动评估 worker service，新增 F-010A；明确只消费 durable queue、独立进程、canonical env、信号收敛和 fail-loud 退出，并与 Phase 3 滚动训练调度隔离 |
| v1.9 | 2026-07-18 | 对齐 P1-C 当前进度与 BUG-742～BUG-748 审计修复；将 F-008/F-010 明确标为源码完成、外部验收待补，保留 10-case、性能、实机 UI/Playwright 与 runtime activation 缺口 |
| v1.8 | 2026-07-18 | 固化用户确认的 HMM 研究工作台：三个一级页签、最终默认风险热力图、固定详情区；禁止 Paper v2 风格、抽屉式列表和 raw JSON 展示；补 Phase 2/3 UI、失败态与分阶段真实激活契约 |
| v1.7 | 2026-07-18 | 同步 P1-B 实际完成状态：回填 PR #2373/#2377、F-007/F-009 验证证据、旧诊断唯一计算路径迁移和 P1-C 剩余边界；runtime 仍未启用 |
| v1.6 | 2026-07-17 | 完成 P1-A 收尾：repository 受管事务、真实 dev PostgreSQL 并发/CAS/lease 验收、RD-Agent 全资产只读目录 PR #4、真实 Loop8 pred/label 零副本 receipt、生产 schema gate 回填 |
| v1.5 | 2026-07-17 | 完成研究隔离复审：允许 QE 全资产只读、固化 latest-common market watermark、移除多余审批语义、强化 degraded warning 与禁止简化版 |
| v1.4 | 2026-07-17 | 完成 Phase 1 实现级详细设计；拆分 reusable evaluation 与 batch-relative ranking；显式 label horizon；批准 `hmm_recommendation_v1` |
| v1.3 | 2026-07-17 | 完成高收益 QE Prediction Store-only + 强制只读 DB/PIT sector 外部验收；F-001/F-002 更新为 verified，Phase 1 解锁 |
| v1.2 | 2026-07-17 | 增加 Prediction Store 零副本复用、显式 fallback 与真实 store-only smoke |
| v1.1 | 2026-07-17 | 增补 F2 scope/non-goals/DAI/矩阵/发布回滚/生产门禁；Phase 0 改为 hardening 前置 gate；收紧 Phase 1-3 隔离语义 |
| v1.0 | 2026-07-16 | 初始版本，定义 Phase 0-3 |

---

**文档归档路径**: `docs/architecture/hmm_evolution_and_risk_management_system_design_20260716.md`
