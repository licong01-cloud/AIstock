# HMM 演进与风险管理系统详细设计

> **版本**: v2.0
> **日期**: 2026-07-16  
> **修订日期**: 2026-07-18
> **状态**: Phase 0 已完成；Phase 1 P1-A/P1-B 已完成，P1-C API/UI/worker 源码与审计硬化已完成，自动评估 worker service 源码已实现；受控 benchmark、真实 UI 与 runtime activation 待外部验收
> **范围**: HMM 快速演进、风险监控、滚动训练、数据隔离  
> **作者**: Kiro (Claude Code)
> **维护者**: AIstock HMM Evolution

---

## 1. 执行摘要

### 1.1 背景与动机

HMM 板块轮动模型自 2026-04-04 修复以来，已有 2 个生产可用版本和 18 个实验候选。但当前研发流程存在以下瓶颈：

1. **验证成本过高**: 每次 HMM 改进需完整 QE 回测（6-12 小时），资源占用重
2. **串行瓶颈**: 一次只能验证 1-2 个版本，迭代速度慢（1 天 1-2 轮）
3. **反馈滞后**: 问题在 QE 结束后才暴露，无法快速定位
4. **风险管理缺失**: HMM 风险门控已实现但未暴露给用户，无预警机制
5. **数据混用风险**: 研发使用回测数据，但真实场景需连接 t-1 行情库

### 1.2 核心目标

**Phase 0-3 目标与当前状态**:
- 🟡 离线快速评估：evaluator/replay、API/UI/worker 源码已实现；标准基准上的单候选 <10 分钟外部验收待完成。
- 🟡 批量对比筛选：batch-relative scorer/top-3、有界并发共享输入和 lease recovery 源码已实现；10+ 候选真实 benchmark 待完成。
- 🟡 自动评估执行：独立 worker service 源码已实现，可自动消费已有 durable queue；不创建任务、不训练 HMM、不接入 FastAPI 或 Phase 3 scheduler，CI 与运行验收待完成。
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
- F-007/F-009 已验证；P1-C 的 worker CLI、真实 API/UI 源码和 BUG-742～BUG-748 审计硬化已完成；10-case 对照、性能 benchmark、实机 UI/Playwright 和 runtime activation 尚未完成。
- 2026-07-18 用户已批准将 Phase 1 worker 从“仅人工有限命令”扩展为显式 `--serve` 独立服务；该批准不包含 Phase 3 自动滚动训练调度，也不等于生产进程已经启动。

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
│                    业务服务层（六大服务）                           │
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
`CURRENT_DATE` 或 worker 执行时动态漂移实现。请求入队时解析并固化 watermark。DB 连接必须复用
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

- `hmm_evolution_phase1_offline_evaluation_detailed_design_20260717.md` v1.4。

**数据库 Schema**:

- `hmm_evolution.schema_version`：bootstrap/version 审计；
- `hmm_evolution.candidate`：内容寻址的 coefficient 候选与 lifecycle；
- `hmm_evolution.offline_evaluation`：与 batch 解耦的可重放评估、lease/fencing 和指标；
- `hmm_evolution.batch_test_run`：请求幂等、heartbeat、取消、超时和汇总状态；
- `hmm_evolution.batch_test_item`：batch/candidate/evaluation 关联以及 batch-relative 推荐分数和排名。

实际 bootstrap 必须为全部 schema/table/column 添加 `COMMENT ON`，并在事务内幂等执行；业务 service 不得隐式建表。

**评估契约**:

- v1 计算基准来自 `scripts/diagnostics/hmm_offline_diagnostic.py::compute_replacements`：同一交易日对 raw score 与 HMM-adjusted score 使用稳定排序，比较 raw TopK 与 adjusted TopK 的 entered/dropped 集合。
- QE task/loop 全部实验资产可通过独立只读 reader 列举、读取和取证；只有通过 manifest/parser
  trust contract 的声明资产可以进入 evaluator，其余资产为 inspection-only evidence。
- `net_label_return` 为逐日 `mean(entered label) - mean(dropped label)` 的均值；必须同时保存显式 `label_horizon_days`、有效日数、覆盖率和正值日比例。仅 horizon=10 时可显示别名 `net_label_10d`。
- `net_db_10d` 使用 `market.kline_daily_raw.close_li` 按交易日序列计算 10 个交易日远期收益，禁止自然日 shift。
- `hmm_recommendation_v1` 使用 batch 内 percentile、版本化权重、缺失值重归一化和稳定并列规则；分数与排名存于 `batch_test_item`，不污染可复用 evaluation。
- top-3 是研究推荐，不自动提交 QE、不修改生产配置、不淘汰未入选方向。
- 每次结果必须可由 `source_manifest + candidate manifest + evaluation_spec + evaluator_version + input_hash` 重放。
- neutral fallback、共同日期裁剪和缺失指标重加权必须标记 degraded 并在主 UI 可见；
  不得只写日志、只返回技术 context 或依赖 raw JSON 才能识别。

**当前实现状态**:

- P1-A/P1-B 后端基础、evaluator、executor、source manifest、market repository 和 scorer 已合入 main。
- 旧 `hmm_offline_diagnostic.py` 已复用唯一 evaluator/Phase 0 缓存/canonical 只读行情 repository，
  不再包含硬编码 DB 凭据、QE config 下载或宽泛异常吞错。
- API 端点、`/hmm-evolution` UI 和人工 worker CLI 源码属于 P1-C 且已实现；真实 benchmark、
  实机 UI/Playwright 和 runtime activation 仍未完成，当前不存在生产 runtime activation。

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
- [ ] 结果可重放，失败具有结构化错误、heartbeat、取消和幂等语义。
- [ ] 与至少 10 个历史 QE case 做方向性/排序一致性对照；差异只作为证据，不作为未经批准的淘汰门禁。
- [ ] 前端以中文表格、指标卡、对比图、固定证据区和独立详情页为主；不使用 Paper v2
  视觉依赖、抽屉式列表或原始 JSON/manifest/error dump 作为信息界面。

---

### Phase 2: HMM 风险监控与预警系统（Week 4-5）

**目标**: 按最新完成交易日生成可解释风险预警和板块状态视图，不接入任何交易决策。

**数据库 Schema**:
```sql
CREATE SCHEMA IF NOT EXISTS hmm_risk;

CREATE TABLE hmm_risk.sector_state_timeline (
    state_id TEXT PRIMARY KEY,
    trade_date DATE NOT NULL,
    as_of_date DATE NOT NULL,
    candidate_id TEXT NOT NULL,
    sector_level TEXT NOT NULL CHECK (sector_level IN ('L1', 'L2')),
    sector_code TEXT NOT NULL,
    hmm_state TEXT NOT NULL CHECK (hmm_state IN ('trending', 'neutral', 'fading')),
    state_confidence DOUBLE PRECISION NULL CHECK (
        state_confidence IS NULL OR state_confidence BETWEEN 0.0 AND 1.0
    ),
    confidence_definition_version TEXT NULL,
    transition_type TEXT NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('NONE', 'HIGH', 'MEDIUM', 'OPPORTUNITY')),
    rule_version TEXT NOT NULL,
    mapping_snapshot_hash TEXT NOT NULL,
    source_manifest JSONB NOT NULL,
    evidence JSONB NOT NULL,
    dedupe_key TEXT NOT NULL,
    revision INT NOT NULL DEFAULT 1,
    supersedes_state_id TEXT NULL REFERENCES hmm_risk.sector_state_timeline(state_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (dedupe_key, revision)
);

CREATE TABLE hmm_risk.daily_alert (
    alert_id TEXT PRIMARY KEY,
    state_id TEXT NOT NULL REFERENCES hmm_risk.sector_state_timeline(state_id),
    trade_date DATE NOT NULL,
    as_of_date DATE NOT NULL,
    candidate_id TEXT NOT NULL,
    sector_level TEXT NOT NULL CHECK (sector_level IN ('L1', 'L2')),
    sector_code TEXT NOT NULL,
    hmm_state TEXT NOT NULL CHECK (hmm_state IN ('trending', 'neutral', 'fading')),
    severity TEXT NOT NULL CHECK (severity IN ('HIGH', 'MEDIUM', 'OPPORTUNITY')),
    transition_type TEXT NOT NULL,
    rule_version TEXT NOT NULL,
    source_manifest JSONB NOT NULL,
    evidence JSONB NOT NULL,
    dedupe_key TEXT NOT NULL,
    revision INT NOT NULL DEFAULT 1,
    supersedes_alert_id TEXT NULL REFERENCES hmm_risk.daily_alert(alert_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (dedupe_key, revision)
);

CREATE TABLE hmm_risk.risk_event (
    event_id TEXT PRIMARY KEY,
    candidate_id TEXT NOT NULL,
    sector_level TEXT NOT NULL CHECK (sector_level IN ('L1', 'L2')),
    sector_code TEXT NOT NULL,
    event_type TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('open', 'resolved')),
    opened_trade_date DATE NOT NULL,
    last_trade_date DATE NOT NULL,
    resolved_trade_date DATE NULL,
    rule_version TEXT NOT NULL,
    first_alert_id TEXT NOT NULL REFERENCES hmm_risk.daily_alert(alert_id),
    latest_alert_id TEXT NOT NULL REFERENCES hmm_risk.daily_alert(alert_id),
    evidence_summary JSONB NOT NULL,
    dedupe_key TEXT NOT NULL,
    revision INT NOT NULL DEFAULT 1,
    supersedes_event_id TEXT NULL REFERENCES hmm_risk.risk_event(event_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (dedupe_key, revision)
);
```

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
- 可以复用 `hmm_risk_gate_v1` 的 artifact 解析与状态计算，不得复用会生成 `RiskDecision(can_buy=False)` 的 Selection provider。
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

### Gate 2: Phase 2 - 风险监控
- Day 1-2: 数据库 schema + 预警生成逻辑
- Day 3-4: 板块热力图 + 时间线追踪
- Day 5-6: 风险门控回测
- Day 7-8: 前端 UI 开发
- Day 9-10: 定时任务 + 集成测试

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
- **F-007 Phase 1 评估可重放**：QE asset trust、latest-common watermark、输入、窗口、
  universe、算法版本、指标定义和 hash 足以复算同一结果。
- **F-008 Phase 1 批处理状态机**：幂等、heartbeat、取消、超时、并发上限、部分失败和结构化错误完整。
- **F-009 Phase 1 推荐语义**：top-3 仅为研究推荐，公式版本化，不自动淘汰方向或修改 QE/Paper。
- **F-010 Phase 1 API/UI**：真实 QE asset/candidate/evaluation/batch API、中文演进实验室、共享
  HMM 研究导航、动态 horizon、主视图 degraded warning、固定证据区和独立详情页完整；禁止
  Paper v2 依赖、抽屉式列表和 raw JSON 主视图。
- **F-010A Phase 1 自动评估 worker service**：显式独立进程自动消费 API 已登记的 durable queue；
  canonical env、poll bounds、idle wait、SIGINT/SIGTERM、lease/fencing recovery 和 fail-loud exit 完整；
  不创建 batch、不嵌入 FastAPI、不触发 QE 或 Phase 3 训练。
- **F-011 Phase 2 预警状态机**：共同数据水位、规则版本、dedupe/revision、解释证据和迟到数据重算完整。
- **F-012 Phase 2 advisory-only**：无 `RiskDecision`、`can_buy`、订单、持仓、配置或调仓副作用。
- **F-013 Phase 2 风险分析与 UI 证据**：`/hmm-risk` 为最终默认首页，L1/L2 状态热力图、
  今日预警、固定详情、状态分布、命中/误报/漏报/样本量和阶段稳定性完整；状态/置信度/severity
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
5. **P1-B evaluator（已完成）**：从既有诊断脚本抽取纯计算逻辑，交付 F-007/F-009；旧诊断已迁到唯一 evaluator。
6. **P1-C API/UI（源码与审计硬化已完成，外部验收待完成）**：F-010 API/UI、人工 worker CLI 和 BUG-742～BUG-748 审计修复已实现；F-010A 增加独立自动评估 worker service；继续补 10-case/性能 benchmark、真实 API/UI/Playwright 和首次 runtime activation 证据。
7. **P2 风险分析**：依次交付 F-011/F-012/F-013；不得触碰交易 provider 接线。
8. **P3 研究训练**：先交付 F-014；F-015 自动调度部分等待独立语义批准。

每个实现 PR 只承担一个可验证子集，并在 PR body 中列设计项、实现引用、验证证据、生产门禁与未批准缺口。Phase 0 BUG 修复走 issue workflow；Phase 1-3 新能力走 feature workflow。

本文对 Phase 2/3 给出跨阶段权威边界和已确认 UI 契约，但尚不等于 Phase 2/3 的实现级详细设计已完成。
开始对应代码前必须补充 schema/repository/state machine/API/worker/UI/test 的从属详细设计并通过 F2
validator；这是防止简化版和业务语义偏移的设计完整性要求，不是每次研究操作的产品审批流。

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

本表记录 v1.8 设计验收状态；`implementation_refs` 和 `test_or_evidence` 中的“目标”不是完成声明，每个实现 PR 必须将对应行替换为真实引用和证据后才能报告该设计项完成。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `backtest_source.py`; `prediction_store_resolver.py`; BUG-688/#2260; #2285；本验收 PR | unit contract + `qe_20260706_013235_bbd4/Loop8` prediction-store-only external receipt；2,260,161 rows；zero-copy/no HMM cache；h20 label 不作为 10 日 label 证据 | verified | 无 |
| F-002 | `db_repository.py`; `realtime_source.py`; BUG-689/#2266；本验收 PR | read-only transaction assertion；`as_of=2026-07-17`；completed date `2026-07-16`；PIT mapping 5,864 symbols / 131 L2 codes；Backtest/Realtime parity | verified | 无 |
| F-003 | `realtime_source.py`; `models.py`; BUG-689/#2266 | candidate identity、隐式 latest 拒绝、filter contract tests | verified | 无 |
| F-004 | `cache_manager.py`; `artifact_manifest.py`; `prediction_store_resolver.py`; BUG-690/#2270 | 路径/原子/跨进程锁/容量/reparse/corruption fail-loud tests | verified | 无 |
| F-005 | `noxfile.py`; `ci_change_classifier.py`; BUG-691/#2273；Phase 0 README/详细设计/验收清单；本验收 PR | `hmm_data_source_backend`、coverage/JUnit、4-test read-only integration、compact timing/RSS receipt | verified | 无 |
| F-006 | Phase 1 详细设计 §5.3/§6/§10/§11；AIstock QE asset reader + candidate bootstrap/repository；RD-Agent PR #4 complete catalog endpoint | 真实 `qe_20260706_013235_bbd4/Loop8`：221 unique relative assets、complete catalog、pred/label 原位只读 receipt；unit isolation/manifest/path/hash/write-allowlist；生产 schema verify；runtime rollout 状态见 §13 | verified | 无 |
| F-007 | Phase 1 详细设计 §7/§8；`backend/services/hmm_evolution/{evaluator,input_adapter,market_repository,source_manifest,executor}.py`；`backend/services/hmm_data_source/{backtest_source,cache_manager}.py`；`scripts/diagnostics/hmm_offline_diagnostic.py`；PR #2373/#2377 | `backend/tests/hmm_evolution/test_{evaluator,input_adapter,market_repository,source_manifest,executor,legacy_oracle,legacy_diagnostic}.py`；tie/h10/h20/mixed horizon/latest-common/read-only transaction/coverage/result hash；HMM/Data Source matrix 178 passed / 8 skipped；dev PostgreSQL read-only smoke | verified | 无 |
| F-008 | Phase 1 详细设计 §10～§13；durable batch/evaluation/item repository、worker/input adapter/executor；BUG-742/BUG-743 | `python -m pytest backend/tests/hmm_evolution/test_worker.py backend/tests/hmm_evolution/test_input_adapter.py backend/tests/hmm_evolution/test_repository_integration.py -q`；既有 dev PostgreSQL 8-worker receipt | approved_by_user_implementation_complete_external_acceptance_pending | 用户明确批准先合入审计修复、外部验收另行执行；仍需 dev PostgreSQL 真实双候选并发、进程中断 lease recovery 和 10 候选耗时 receipt，未标记 verified |
| F-009 | Phase 1 详细设计 §9；`backend/services/hmm_evolution/scorer.py`、`repository.py::_apply_recommendations_with_cursor()`；PR #2373 | `test_scorer.py`、repository integration；singleton/percentile/tie/missing renormalization/coverage-only unranked/stable top-3；排名仅写 batch item，无淘汰阈值或交易副作用 | verified | 无 |
| F-010 | Phase 1 详细设计 §14/§15；真实 QE asset/candidate/evaluation/batch API、共享 HMM 导航、演进 UI；BUG-744～BUG-748 | `python -m pytest backend/tests/hmm_evolution/test_api.py backend/tests/hmm_evolution/test_qe_workspace_client_catalog.py backend/tests/hmm_evolution/test_frontend_contract.py -q`；`frontend/tests/hmm-evolution/hmm-evolution.spec.ts` | approved_by_user_implementation_complete_external_acceptance_pending | 用户明确批准先合入审计修复、外部验收另行执行；仍需真实 API 页面截图、完整 Playwright、10-case、性能 benchmark 和首次 runtime activation；风险/训练页不得用静态占位冒充完成 |
| F-010A | Phase 1 详细设计 §5.1/§13.5/§18～§21；`worker_service.py` + `hmm_evolution_worker.py --serve` + UI worker 文案 | `python -m pytest backend/tests/hmm_evolution/test_worker_service.py backend/tests/hmm_evolution/test_worker_cli.py -q`：22 passed；连续 drain、idle interruptible wait、poll bounds、signal shutdown、known/unknown failure propagation、无 task creation/FastAPI/scheduler | approved_by_user_implementation_complete_external_acceptance_pending | 源码和直接测试已完成；CI、首次 service activation、进程重启与真实 queue receipt 待回填 |
| F-011 | 目标：hmm_risk alert state machine | 目标：watermark/dedupe/revision test | approved_by_user_for_implementation | 实现证据由 Phase 2 PR 回填 |
| F-012 | 目标：advisory-only service boundary | 目标：Selection/Paper/QMT 无副作用 test | approved_by_user_for_implementation | 实现证据由 Phase 2 PR 回填 |
| F-013 | 本文 §2.2/Phase 2 UI 契约；目标 risk evidence/report + `/hmm-risk` 默认首页 | 目标：真实 L1/L2/7 日 heatmap、固定详情、预警、状态分布、renderer/error、指标/样本量/阶段稳定性 test | approved_by_user_for_implementation | 实现证据由 Phase 2 PR 回填；不新增交易 heat score |
| F-014 | 本文 Phase 3 UI/隔离契约；目标 research-only rolling candidate + `/hmm-research-training` | 目标：DB write allowlist、planner/artifact、真实窗口/时效性/任务 UI、无生产 snapshot 写入 test | approved_by_user_for_implementation | 实现证据由 Phase 3 PR 回填 |
| F-015 | 目标：manual-first scheduler contract | 目标：ownership/misfire/reentry/disable test | approved_by_user_for_implementation | 自动调度实现仍需独立用户批准 |
| F-016 | 目标：全阶段 isolation guard | 目标：scope/production-gate/side-effect evidence | approved_by_user_for_implementation | 实现证据由各阶段 PR 回填 |

## 12. Rollout / Rollback（发布与回滚）

- Phase 0 hardening 先合入，不启用新 runtime；发现回归可回滚对应代码 PR，缓存格式升级须保留版本目录并支持安全重建。
- Phase 1/2 API 和 UI 默认不注册到生产调度；schema bootstrap 与运行时代码分开 gate，DDL 未显式授权时报告 `production_ddl_pending`。
- Phase 2 job 首次发布只允许人工受控执行；确认幂等、水位和副作用测试后，才可登记只读日任务。
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

当前 P1-A/P1-B gate receipt（2026-07-18）：`production_ddl_gate=applied_and_verified`
（`hmm_evolution_v1`：5 tables / 115 columns / 41 constraints / 7 non-constraint
indexes，业务表为空）；P1-B 未新增 DDL/依赖；`runtime_activation_gate=pending`；dependency gates 均为 `noop`。

---

## 14. 附录

### 14.1 参考文档

- `hmm_evolution_phase1_offline_evaluation_detailed_design_20260717.md` - Phase 1 实现级详细设计
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
