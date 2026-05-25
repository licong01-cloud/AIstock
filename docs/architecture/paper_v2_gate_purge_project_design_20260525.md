# Paper v2 / Selection / MiniQMT StrategyPackage Gate Purge Project Design (2026-05-25)

> 状态：详细设计草案，等待用户批准后合入 `main`。
> 变更类型：项目级功能整改，不走 Issue/BUG 流程。
> 适用范围：StrategyPackage、Selection Center、Paper Trading v2、AIstock LocalSim、MiniQMT SIM、Paper v2 UI、相关测试与迁移。
> 非目标：本设计不实现代码、不关闭既有 Issue、不启用实盘、不重启生产 `8001`。

## 1. 背景和问题

Paper v2 从最初设计到当前实现，逐步叠加了 StrategyPackage 生命周期、Paper 状态、未来实盘治理、运行时数据检查、broker 检查、execution policy 检查、HMM/ST PIT 检查等多层逻辑。结果是一个本应简单的流程：

```text
QE 生成策略包 -> 资产检查合格 -> 选股 -> AIstock 模拟盘 / MiniQMT 模拟盘
```

被拆成了大量门禁：

- `SELECTION_ENABLED`
- `PAPER_ENABLED`
- `PAPER_RUNNING`
- `PAPER_PASSED`
- `PAPER_FAILED`
- `paper_ready`
- `paper_enabled execution policy`
- `paper_candidate runtime variant`
- Selection health runnable
- ST PIT contract runnable
- HMM snapshot/manual coefficient gate
- broker/source/execution algo/TDX/DB/minute/pre_close 等平台能力门禁

这些门禁的复杂度已经超过模拟盘业务逻辑本身，导致用户无法顺畅执行选股和模拟盘验证，也导致代码复杂度、UI 分支和测试状态机持续膨胀。

用户最新明确要求：

1. 策略包资产检查合格，就必须可以执行选股、AIstock 模拟盘和 MiniQMT 模拟盘。
2. 不需要任何 StrategyPackage 级别的 Paper/Selection 门禁。
3. `PAPER_ENABLED / PAPER_RUNNING / PAPER_PASSED` 等状态没有意义，必须删除。
4. 平台能力不能变成策略包门禁。
5. 未来实盘只允许从模拟盘验证过的策略包进入，QE 策略包不能直接进入实盘；但 QE 策略包可以直接进入模拟盘。
6. 当前不通过 Issue 流程处理，先按项目变更设计方案推进，方案批准后再进入实现和合入。

## 2. 权威原则

本设计作为 2026-05-25 之后 Paper v2 gate purge 的权威整改设计。它覆盖旧文档中的冲突表述，尤其是：

- 旧文档中将 `PAPER_ENABLED/PAPER_RUNNING/PAPER_PASSED/PAPER_FAILED` 作为 StrategyPackage 生命周期状态的描述全部废止。
- 旧文档中将 `paper_enabled=true` execution policy 作为 Paper v2 / MiniQMT 准入条件的描述全部废止。
- 旧文档中将 `paper_candidate=true` runtime variant 作为 Selection/Paper 准入条件的描述全部废止。
- 旧文档中将 HMM/ST PIT/行业黑名单/交易日/TDX/DB/MiniQMT/broker/execution algo 作为 StrategyPackage 门禁的描述全部废止。

保留并强化以下既有边界契约：

- StrategyPackage 只保存可复现 alpha core：因子、feature schema、模型、权重、训练/回测证据、source lineage、artifact hash/URI。
- HMM、行业黑名单、交易日、行情源、broker、执行算法、停牌、涨跌停、昨收、分钟线、资金、账户、实盘审批全部属于平台运行能力。
- 平台运行能力只能在本次 run/session/preflight 中 fail-fast、warning、skip 或 wait，不能提前阻止资产合格策略包进入选股和模拟盘。
- MiniQMT 是 broker order/trade authority；AIstock 是策略、分仓、evidence、账本和归因 authority。
- 实盘准入必须单独走 LiveApproval，不得复用 Paper 状态。

## 3. 目标业务流程

### 3.1 选股

目标路径：

```text
选择资产合格 StrategyPackage
  -> 指定 trade_date / top_k / runtime profile
  -> Selection Center 生成 SelectionRun / DailySelectionEvidence
  -> 输出候选、排除原因、股票名称、入池价、当前价等用户有价值信息
```

硬准入只允许：StrategyPackage asset eligibility。

不得存在：

- `SELECTION_ENABLED` 状态门禁。
- `PAPER_ENABLED/PAPER_RUNNING/PAPER_PASSED` 状态门禁。
- UI checkbox 因 package health 被禁用。
- 选股按钮因 package health 被禁用。
- HMM 必须手工选择 snapshot。
- ST PIT / 行业黑名单 / 数据源预检提前挡包。

### 3.2 AIstock 模拟盘

目标路径：

```text
选择资产合格 StrategyPackage
  -> 创建 Paper v2 portfolio
  -> 平台自动解析或选择 runtime profile / execution profile
  -> 每日或盘中运行时自动执行 runtime preflight
  -> 成功则生成 selection / target / order intent / fill / ledger / snapshot
  -> 失败则记录本次 run/session 错误，不改变策略包资格
```

硬准入只允许：StrategyPackage asset eligibility。

不得存在：

- `enable-paper` 必经流程。
- `paper_ready=true` 才能创建模拟盘。
- `paper_enabled=true` policy 才能创建模拟盘。
- `paper_candidate=true` runtime variant 才能创建模拟盘。
- 交易时段限制阻止盘中恢复或启动。

### 3.3 MiniQMT 模拟盘

目标路径：

```text
选择资产合格 StrategyPackage
  -> 创建 MiniQMT SIM strategy / portfolio binding
  -> MiniQMT 连接可用时提交 order intent
  -> MiniQMT 返回 order/trade/account facts
  -> AIstock 做 strategy lot / cash / reconciliation / attribution
```

硬准入只允许：StrategyPackage asset eligibility。

MiniQMT 连接、SIM 模式、账户、broker reject 是订单提交时的 broker runtime check，不是 StrategyPackage 门禁。

不得存在：

- MiniQMT UI 只列 `PAPER_ENABLED/PAPER_RUNNING/PAPER_PASSED` 包。
- 创建 MiniQMT 组合必须选择 `paper_enabled` execution policy。
- MiniQMT 模拟盘撮合使用 TDX 数据。
- MiniQMT 无法登录导致全天不能启动策略；应允许盘中恢复。

### 3.4 未来实盘

未来实盘是独立流程：

```text
资产合格 StrategyPackage
  -> 完成规定模拟盘运行证据
  -> 生成 live approval request
  -> 人工审批 + MiniQMT live 明确授权
  -> live run/session
```

要求：

- QE 策略包不能直接进入实盘。
- Paper v2 run evidence 是实盘前置证据，不是 StrategyPackage 状态。
- 不得引入 `PAPER_PASSED` 作为实盘资格代理。

## 4. 唯一保留的 StrategyPackage 硬准入

唯一硬准入命名为：`StrategyPackageAssetEligibility`。

### 4.1 资产合格检查项

| 检查项 | 是否硬阻断 | 说明 |
|---|---:|---|
| package record 存在 | 是 | 不存在不能运行。 |
| package 未 `RETIRED` / 未 deleted / 未 quarantined | 是 | 已退役或隔离不能进入新运行。 |
| package 来源是 QE / 受认可 QE 打包路径 | 是 | 避免手工伪造包进入运行链路。 |
| source QE / backtest 已成功或已批准 | 是 | 失败 QE 实验不能进入选股或模拟盘。 |
| frozen manifest 可解析 | 是 | manifest 是 alpha core 合约。 |
| `manifest_sha256` 与 payload 一致 | 是 | 防止 manifest 漂移。 |
| manifest package_id 与 record package_id 一致 | 是 | 防止错绑。 |
| alpha core hash / locked core hash 一致 | 是 | 防止资产替换。 |
| 因子/模型/score loader/必要 artifact 存在且可加载 | 是 | 缺核心资产不能运行。 |
| feature schema / model metadata 基础字段存在 | 是 | 防止无法推理。 |
| metrics_summary 缺失 | 否 | 展示 warning；不阻断。 |
| seed/regime stability 不充分 | 否 | 不是模拟盘准入。随机种子训练效果好也可以模拟盘。 |
| original fixed-weight retest 缺失 | 否 | 不是模拟盘准入。 |
| protected asset ledger 缺失 | 否 | 关键 artifact 可加载即可运行；ledger 只做治理 warning。 |
| runtime variant candidate 缺失 | 否 | 删除 `paper_candidate` 门禁。 |
| HMM config/snapshot/coefficient 缺失 | 否 | HMM 是 runtime；运行时自动计算/缓存或本次 run 失败。 |
| 行业黑名单配置缺失/错误 | 否 | runtime profile 问题；不影响包资格。 |
| ST PIT/suspend/limit/pre_close/minute 数据缺失 | 否 | 本次 run 数据问题；不影响包资格。 |
| broker/MiniQMT/TDX/DB 连接问题 | 否 | runtime/broker 问题；不影响包资格。 |
| execution algo 不存在 | 否 | run-time fail-fast；不影响包资格。 |

### 4.2 AssetEligibilityResult

建议后端统一返回以下结构：

```json
{
  "package_id": "pkg_xxx",
  "manifest_sha256": "...",
  "alpha_core_sha256": "...",
  "eligible": true,
  "status": "ELIGIBLE",
  "blockers": [],
  "warnings": [],
  "checks": [
    {"name": "manifest_identity", "status": "PASS", "severity": "hard"},
    {"name": "artifact_loadability", "status": "PASS", "severity": "hard"}
  ],
  "legacy_status": "PAPER_ENABLED",
  "legacy_status_normalized_to": "BACKTEST_APPROVED",
  "evaluated_at": "2026-05-25T...+08:00"
}
```

约束：

- `eligible=false` 只能由 4.1 中硬阻断项触发。
- `warnings` 不得阻止 Selection / AIstock Paper / MiniQMT Paper。
- `legacy_status` 只用于迁移期展示，不得进入业务判断。

## 5. 必须删除或降级的门禁清单

### 5.1 必须删除的 StrategyPackage 状态

| 当前状态 | 处理 |
|---|---|
| `SELECTION_ENABLED` | 不再作为准入状态；迁移为 `BACKTEST_APPROVED` 或 legacy display。 |
| `PAPER_ENABLED` | 删除；不能表示可模拟盘。资产合格即可模拟盘。 |
| `PAPER_RUNNING` | 删除；运行状态属于 `paper_v2.run/session`。 |
| `PAPER_PASSED` | 删除；验证结果属于 `paper_v2 evidence` 或 future `LiveApproval`。 |
| `PAPER_FAILED` | 删除；失败属于具体 run/session，不污染 StrategyPackage。 |

目标状态机：

```text
DRAFT -> ASSET_VALIDATED -> BACKTEST_APPROVED -> RETIRED
```

说明：

- `BACKTEST_APPROVED` 是策略包可进入选股和模拟盘的唯一业务状态前提，但最终还必须通过 `StrategyPackageAssetEligibility`。
- 历史 `PAPER_*` 信息不得丢弃，可保存在 status event 历史和 paper_v2 run/session/evidence 中，但不能作为当前状态。

### 5.2 必须从主路径移除的 API / service 门禁

| 现有逻辑 | 处理 |
|---|---|
| `/strategy-packages/{id}/enable-paper` | 从 UI 和主业务路径删除；兼容期可返回 deprecated/no-op，不得改变状态或执行门禁。 |
| `/strategy-packages/{id}/enable-selection` | 从 UI 和主业务路径删除；资产合格即可选股。 |
| `/strategy-packages/{id}/governance-eligibility` 的 `paper_ready` | 仅保留为未来 live/governance read-only 审计；Paper/Selection 不得调用它判断。 |
| `/paper-simulation-admission` 的 allowed statuses | 改为 `asset-eligibility`；不返回 `PAPER_*` allowed 状态。 |
| `StrategyPackageValidator.validate_manifest_identity_for_paper_trading()` status whitelist | 改为调用 asset eligibility 或只校验 manifest identity，不再校验 Paper status。 |
| `SelectionPackageHealthService.require_runnable()` | 删除硬阻断语义；改为 warning summary 或 runtime diagnostics。 |
| `ensure_policy_can_enter_paper()` 对 `paper_enabled` 的依赖 | 删除 `paper_enabled` 作为准入；只校验 policy schema / algo registry / source evidence。 |
| `runtime_variant.paper_candidate` | 从 Selection/Paper 主路径删除；runtime variant 只可作为可选版本化 runtime profile，不是准入。 |
| coldstart sentinel 要求 `PAPER_ENABLED` | 改为 asset eligibility，或移出主链路作为诊断工具。 |
| QMT package binding status whitelist | 改为 asset eligibility。 |

### 5.3 必须从 UI 移除的门禁

| UI 位置 | 处理 |
|---|---|
| Paper v2 首页 workflow 统计 `PAPER_ENABLED/PAPER_RUNNING/PAPER_PASSED` | 改为 asset eligible package count。 |
| StrategyPackage 页面“标记可用于选股/模拟盘”按钮 | 删除。 |
| StrategyPackage 页面 `PAPER_CREATABLE_STATUSES` / `SELECTION_RUNNABLE_STATUSES` | 删除。 |
| Selection 页面 `packageHealthRunnable()` 禁用 checkbox | 删除禁用；只展示 warning。 |
| Selection 页面 `selectedPackageBlocked` 禁用运行按钮 | 删除。 |
| Selection 页面“策略包健康预检阻断” | 改为“运行时风险提示”，不阻断。 |
| MiniQMT 页面只列 `PAPER_ENABLED/PAPER_RUNNING/PAPER_PASSED` | 改为列 asset eligible package。 |
| MiniQMT 页面要求 `paper_enabled` execution policy | 删除；允许平台默认或用户显式选择任意可校验 policy。 |
| MiniQMT 创建按钮因 `!policyId` 禁用 | 删除；policy 可为空，由平台默认解析。 |
| Governance 页面 `paper_ready` disabled enable action | Paper v2 不再链接此页面；若保留，改为 live governance read-only。 |
| Workflow 文案“用 PAPER_ENABLED 策略包冻结模拟盘实例” | 改为“资产合格策略包可直接创建模拟盘”。 |

## 6. 平台 runtime 检查分类

以下检查必须保留，但它们不是 StrategyPackage 门禁。

### 6.1 本次运行 fail-fast / wait / skip

| 检查 | 行为 | 不允许行为 |
|---|---|---|
| 交易日 | 非交易日显示等待/跳过，首页展示最近/下一交易日。 | 不得让策略包不可用。 |
| trading calendar cache | 缓存缺失时自动刷新；DB 不覆盖下月完整日期时警告。 | 不得 weekday fallback。 |
| TDX 实时/历史价格 | 当前日期使用 TDX 当前价/最新收盘；盘前 `current_price<=0 && pre_close>0` 可用 pre_close 作为 entry price；历史日期用 PIT 截止日价格。 | 不得伪造价格。 |
| DB minute bars | 缺失则本次 LocalSim run fail-fast。 | 不得降级日频或假成交。 |
| suspend_d/stk_limit/pre_close | 缺失则本次需要它的 run fail-fast 或标记数据未就绪。 | 不得改变 StrategyPackage 状态。 |
| HMM | 选择模型后自动计算当日参数并缓存；同日复用缓存。 | 不得要求每天手工生成 snapshot。 |
| 行业黑名单 | runtime profile 配置；配置错误本次 run 失败或提示。 | 不得成为策略包门禁。 |
| execution algo | 运行前校验 algo registry；缺失本次 run fail-fast。 | 不得要求 `paper_enabled` 状态。 |
| duplicate run/session lock | 防止重复写账，返回清晰错误或幂等结果。 | 不得污染包状态。 |
| broker connection | MiniQMT 下单时未连接则 wait/fail 本次 tick。 | 不得从包列表隐藏策略包。 |
| MiniQMT SIM mode | 下单前必须确认 SIM；非 SIM 阻止订单提交。 | 不得阻止创建策略绑定或选择包。 |
| no fake success | 缺数据、缺模型、缺连接必须失败或等待。 | 不得返回空结果伪装成功。 |

### 6.2 未来实盘专属 gate

| Gate | 所属层 | 是否影响当前模拟盘 |
|---|---|---:|
| 模拟盘运行证据 | LiveApproval | 否 |
| 人工实盘审批 | LiveApproval | 否 |
| MiniQMT live 显式授权 | Broker live safety | 否 |
| live account/broker compatibility | LiveApproval / broker runtime | 否 |
| real order kill switch | Broker live safety | 否 |

## 7. 现状差距和代码定位

以下是当前 `origin/main` 的主要残留点，实施时必须逐项关闭。

| 模块 | 当前残留 | 目标处理 |
|---|---|---|
| `backend/services/strategy_package/models.py:24` | `PackageStatus` 仍定义 `SELECTION_ENABLED/PAPER_*` | 精简状态枚举。 |
| `backend/services/strategy_package/service.py:62` | 状态机仍含 `PAPER_ENABLED -> PAPER_RUNNING -> PAPER_PASSED/PAPER_FAILED` | 删除 Paper 状态迁移。 |
| `backend/services/strategy_package/service.py:82` | `PAPER_SIMULATION_ALLOWED_STATUSES` | 删除，改 asset eligibility。 |
| `backend/services/strategy_package/service.py:413` | `PAPER_ENABLED` 转换前执行 admission | 删除 enable-paper 主路径。 |
| `backend/services/strategy_package/service.py:1008` | `governance_eligibility.paper_ready` | 改 live/governance read-only，不参与 Paper。 |
| `backend/services/strategy_package/service.py:1238` | manifest gate 包含 allowed Paper statuses | 改仅资产/manifest 检查。 |
| `backend/services/strategy_package/validators.py:47` | paper trading validator 校验 package status | 删除 Paper status 白名单。 |
| `backend/services/selection_center/service.py:213` | Selection status whitelist | 改 asset eligibility。 |
| `backend/services/selection_center/service.py:236` | `require_runnable()` 硬阻断 | 改 warning/run diagnostics。 |
| `backend/services/selection_center/service.py:351` | runtime variant 必须 `paper_candidate` | 删除。 |
| `backend/services/paper_trading_v2/service.py:406` | policy list display 检查 `paper_enabled` | 删除 gate，保留 policy diagnostics。 |
| `backend/services/paper_trading_v2/service.py:492` | activate policy 要求 `paper_enabled` | 删除。 |
| `backend/services/paper_trading_v2/service.py:1145` | Paper runtime variant 必须 `paper_candidate` | 删除。 |
| `backend/services/paper_trading_v2/service.py:1350` | create portfolio policy 要求 `paper_enabled` | 删除。 |
| `backend/services/paper_trading_v2/coldstart_sentinel.py:40` | sentinel 只允许 `PAPER_ENABLED/PAPER_RUNNING/PAPER_PASSED` | 改 asset eligibility 或诊断隔离。 |
| `backend/services/simulation_runtime/selection.py:450` | simulation selection status whitelist | 改 asset eligibility。 |
| `backend/services/qmt_strategy_ledger/package_binding.py:53` | QMT binding status whitelist | 改 asset eligibility。 |
| `frontend/src/app/paper-v2/miniqmt-sim/page.tsx:103` | MiniQMT UI 只列 Paper 状态包 | 改 asset eligible。 |
| `frontend/src/app/paper-v2/miniqmt-sim/page.tsx:126` | 只自动选择 `paper_enabled` policy | 删除。 |
| `frontend/src/app/paper-v2/miniqmt-sim/page.tsx:189` | `!policyId` 禁用创建 | 删除。 |
| `frontend/src/app/paper-v2/miniqmt-sim/page.tsx:192` | 非 `paper_enabled` policy option disabled | 删除。 |
| `frontend/src/app/paper-v2/packages/page.tsx:24` | `SELECTION_RUNNABLE_STATUSES` | 删除。 |
| `frontend/src/app/paper-v2/packages/page.tsx:27` | `PAPER_CREATABLE_STATUSES` | 删除。 |
| `frontend/src/app/paper-v2/packages/page.tsx:306` | “标记可用于选股”按钮 | 删除。 |
| `frontend/src/app/paper-v2/packages/page.tsx:307` | “标记可用于模拟盘”按钮 | 删除。 |
| `frontend/src/app/paper-v2/selection/page.tsx:27` | `packageHealthRunnable()` | 不得作为 disabled 判断。 |
| `frontend/src/app/paper-v2/selection/page.tsx:215` | 运行前抛出 package health error | 删除硬阻断。 |
| `frontend/src/app/paper-v2/selection/page.tsx:336` | `selectedPackageBlocked` 禁用运行 | 删除。 |
| `frontend/src/app/paper-v2/selection/page.tsx:345` | checkbox disabled | 删除。 |
| `frontend/src/app/strategy-package-governance/page.tsx:163` | `paper_ready` 禁用 enable action | 移出 Paper v2 主路径。 |
| `frontend/src/lib/paper-v2/format.ts:217` | workflow 文案引用 `PAPER_ENABLED` | 改文案和 reachable 逻辑。 |

## 8. 后端详细设计

### 8.1 新增 StrategyPackageAssetEligibilityService

建议新增或重构为：

```text
backend/services/strategy_package/asset_eligibility.py
```

核心接口：

```python
class StrategyPackageAssetEligibilityService:
    def evaluate(self, package_id: str, *, surface: str | None = None) -> AssetEligibilityResult: ...
    def require_eligible(self, package_id: str, *, surface: str | None = None) -> AssetEligibilityResult: ...
    def list_eligible(self, *, limit: int = 200) -> list[AssetEligibilityResult]: ...
```

`surface` 只能影响 warning 文案，不能增加硬门禁。允许值：

- `selection`
- `aistock_paper`
- `miniqmt_paper`
- `live_candidate`

其中 `live_candidate` 也不能直接批准实盘，只能作为 LiveApproval 输入摘要。

### 8.2 StrategyPackage 状态精简

目标枚举：

```python
class PackageStatus(str, Enum):
    DRAFT = "DRAFT"
    ASSET_VALIDATED = "ASSET_VALIDATED"
    BACKTEST_APPROVED = "BACKTEST_APPROVED"
    RETIRED = "RETIRED"
```

迁移策略：

1. 先在代码里提供 legacy normalization，保证旧 DB 数据可读。
2. 数据迁移把旧状态映射为：

```text
SELECTION_ENABLED -> BACKTEST_APPROVED
PAPER_ENABLED     -> BACKTEST_APPROVED
PAPER_RUNNING     -> BACKTEST_APPROVED
PAPER_PASSED      -> BACKTEST_APPROVED
PAPER_FAILED      -> BACKTEST_APPROVED 或 RETIRED? 默认 BACKTEST_APPROVED，具体失败证据留在 paper_v2 run/session。
```

3. `RETIRED` 仍保留为不可新运行状态。
4. status event 历史不删除，用于审计旧状态变迁。
5. 迁移后业务代码不得再引用 `PAPER_*`。

### 8.3 Router/API 调整

新增：

```text
GET /api/v1/strategy-packages/{package_id}/asset-eligibility
GET /api/v1/strategy-packages/asset-eligible
```

调整：

- `GET /selection-center/selectable-packages` 返回 asset eligible packages，并附带 `asset_eligibility` 和 runtime warnings。
- `POST /selection-center/runs` 内部只调用 asset eligibility。
- `POST /paper-v2/portfolios` 内部只调用 asset eligibility。
- MiniQMT create/bind API 内部只调用 asset eligibility。

废弃：

- `POST /strategy-packages/{id}/enable-selection`
- `POST /strategy-packages/{id}/enable-paper`
- `POST /strategy-packages/{id}/execution-policies/{policy_id}/enable-paper`

兼容策略：

- 第一阶段可保留 endpoint，但返回 deprecated/no-op，不再写状态，不再阻断。
- UI 和测试必须不再依赖这些 endpoint。
- 第二阶段可以删除 endpoint，或保留只读兼容直到前端和外部脚本全部迁移。

### 8.4 Selection Center 后端

要求：

- `list_selectable_packages()` 列出 asset eligible packages。
- `_resolve_packages()` 只校验 asset eligibility。
- `package_health` 改为 runtime diagnostics，不得 raise 阻断 asset eligible package。
- ST PIT、行业黑名单、HMM、data_source 的问题只在本次 selection run 中 fail-fast 或记录 warning。
- HMM 启用时：选择 HMM model config 后自动计算当日参数并缓存；不能要求用户每天手动生成 snapshot。
- 当前日期选股价格：用 TDX 当前 quote；盘前 `current_price <= 0 && pre_close > 0` 时 entry price 使用 pre_close，source 标记 `TDX latest close / pre_close`。
- 历史日期选股价格：以 target trade date 的前一完成交易日及以前数据计算因子，并使用 PIT 截止日收盘价作为入池价。

### 8.5 AIstock Paper 后端

要求：

- portfolio 创建只要求 asset eligibility。
- 不再要求 `PAPER_ENABLED`。
- 不再要求 `paper_ready`。
- 不再要求 `paper_enabled execution policy`。
- 不再要求 `paper_candidate runtime variant`。
- 如果用户不选 execution policy，平台从 manifest/backtest context 或默认 runtime profile 解析执行策略，并在 run 前校验 algo 可用。
- 如果用户显式选择 execution policy，只校验：package_id/manifest hash 匹配、policy schema、algo registry、source evidence；不得校验 `paper_enabled`。
- readiness 可以保留为可选诊断，但 run 按钮不能要求用户先手动通过 readiness；点击运行时自动执行必要 preflight。
- run failure 写入 `paper_v2.run/errors/events`，不改变 StrategyPackage 状态。

### 8.6 MiniQMT SIM 后端

要求：

- MiniQMT strategy/portfolio binding 只要求 asset eligibility。
- 创建 binding 不要求 broker 当前在线；下单 tick 时若 broker 不在线，session 记录 waiting/fail，本次不下单。
- 下单前必须确认 SIM mode；非 SIM 必须阻止真实下单，这是 broker runtime safety，不是 StrategyPackage 门禁。
- MiniQMT 成交、拒单、资金、持仓只以 MiniQMT 返回为权威；不得用 TDX/DB/LocalSim 补成交。
- 盘中可以启动、恢复、tick；不得有“只能非交易时段创建/切换”的硬限制。
- MiniQMT 与 AIstock 分仓 reconciliation 问题记录为 runtime issue，不污染策略包资格。

### 8.7 Error taxonomy

现有大量平台错误被包裹为 `STRATEGY_PACKAGE_VALIDATION_ERROR`，容易让用户误以为策略包不合格。整改后错误码分层：

| 错误码 | 用途 |
|---|---|
| `STRATEGY_PACKAGE_ASSET_INVALID` | 唯一表示策略包资产不合格。 |
| `RUNTIME_PROFILE_INVALID` | runtime profile 配置错误。 |
| `MARKET_DATA_UNAVAILABLE` | 本次运行缺行情/分钟线/昨收/涨跌停/停牌数据。 |
| `HMM_RUNTIME_UNAVAILABLE` | HMM 自动计算或缓存失败。 |
| `BROKER_UNAVAILABLE` | MiniQMT/TDX/DB/broker 暂不可用。 |
| `EXECUTION_ALGO_UNAVAILABLE` | 执行算法不存在或不可初始化。 |
| `RUN_ALREADY_EXISTS` | 当日 run/session 幂等或重复写账限制。 |
| `LIVE_APPROVAL_REQUIRED` | 未来实盘专用，不用于模拟盘。 |

## 9. 前端详细设计

### 9.0 UI 同步整改总原则

本项目必须把 UI 作为同一整改范围，不允许只改后端、不改前端。过去的核心问题之一就是：UI 没有提供任何真正可调整的平台配置能力，却在策略包进入选股或模拟盘时用旧状态、旧 health、旧 governance 字段阻拦用户，最终形成“用户无处修改、但处处报错”的死流程。

本次 UI 整改必须遵守：

1. 后端删除的门禁，UI 必须同步删除；不得继续用旧字段在前端隐藏包、禁用按钮、禁用 option 或显示“必须先启用”的流程。
2. 资产合格包必须在 Selection、AIstock Paper、MiniQMT SIM 三类页面都可见、可选、可提交。
3. UI 只能因为以下原因禁用按钮：
   - 正在执行请求，避免重复提交；
   - 最小必填表单为空，例如未选择任何策略包、trade_date 为空、top_k 非法；
   - destructive action 的确认文本未输入；
   - 真实下单或未来实盘路径缺少显式安全授权。
4. UI 不得因为以下原因禁用 Selection/Paper/MiniQMT 模拟盘入口：
   - `PAPER_ENABLED/PAPER_RUNNING/PAPER_PASSED/PAPER_FAILED`；
   - `SELECTION_ENABLED`；
   - `paper_ready=false`；
   - `paper_enabled=false` execution policy；
   - `paper_candidate=false` runtime variant；
   - package health warning；
   - ST PIT / HMM / 行业黑名单 / 交易日 / 行情源 / broker runtime warning。
5. 平台 runtime 风险可以展示为 warning / diagnostics，但必须附带“仍可运行，失败会记录为本次 run/session 错误”的清晰说明。
6. UI 错误必须区分“策略包资产不合格”和“本次运行条件不足”，不得把 runtime failure 显示为策略包不可用。
7. UI 要减少旧流程噪音：删除 enable-selection、enable-paper、paper_ready governance、paper-enabled policy、PAPER lifecycle badge 等会误导用户的展示。
8. UI 要保留真正有用的信息：资产合格检查、artifact 缺失原因、runtime warning、交易日状态、HMM 自动计算/缓存状态、行情价格来源、MiniQMT 连接与 SIM 安全状态、run/session 错误和下一步动作。

### 9.0.1 UI 删除/保留矩阵

| UI 功能/展示 | 处理 | 说明 |
|---|---|---|
| “标记可用于选股” | 删除 | 资产合格即可选股。 |
| “标记可用于模拟盘” | 删除 | 资产合格即可创建模拟盘。 |
| `PAPER_ENABLED/PAPER_RUNNING/PAPER_PASSED/PAPER_FAILED` 状态 badge | 从 Paper v2 主路径删除 | 历史状态只能在高级审计中显示为 legacy。 |
| `paper_ready` 卡片/按钮禁用 | 从 Paper v2 主路径删除 | 仅未来 live governance 只读可见。 |
| `paper_enabled` execution policy ready/disabled 展示 | 删除 | policy 是否可用于模拟盘不再由该字段决定。 |
| `paper_candidate` runtime variant 展示 | 从 Selection/Paper 主路径删除 | runtime variant 不再作为准入。 |
| StrategyPackage health blocked notice | 改为 runtime warning | 不禁用选择和运行。 |
| Selection checkbox disabled | 删除 | 只有资产不合格包不进入列表；列表内包都可选。 |
| Selection run button 因 package health disabled | 删除 | 只因 busy/表单缺失禁用。 |
| MiniQMT 创建按钮因 `!policyId` disabled | 删除 | policy 可为空，平台默认解析。 |
| MiniQMT package list 只显示 Paper 状态包 | 删除 | 使用 asset eligible packages。 |
| Workflow step “用 PAPER_ENABLED 策略包冻结模拟盘实例” | 删除 | 改为“资产合格策略包可直接创建模拟盘”。 |
| AssetEligibilityCard | 保留/新增 | 展示唯一硬准入及 blockers。 |
| RuntimeDiagnosticsPanel | 保留/新增 | 展示非阻断 warning、可重试错误和下一步动作。 |
| BrokerStatusPanel | 保留/简化 | MiniQMT 连接、SIM mode、账户状态属于运行时，不隐藏策略包。 |
| HMM runtime panel | 保留/简化 | 选择 model config；snapshot 可选；展示自动计算/缓存状态。 |
| Selection result price/source columns | 保留/增强 | 展示股票名称、入池价、当前价、成交量、昨收、价格来源。 |

### 9.0.2 UI 可运行性验收

每个 UI 页面必须通过“无门禁走通”验收：

1. 给定一个 asset eligible package，即使 legacy status 是 `BACKTEST_APPROVED`，也能在页面中看到并选择。
2. 不需要点击任何 enable-selection / enable-paper / governance 页面。
3. 不需要先创建或启用 paper-enabled execution policy。
4. 不需要先创建 paper_candidate runtime variant。
5. Selection 页面可以直接点击运行；若 runtime 数据缺失，后端返回本次 run error，UI 显示错误和下一步，不把包变成不可用。
6. AIstock Paper 页面可以直接创建 portfolio；policy 为空时由平台默认解析。
7. MiniQMT SIM 页面可以直接创建配置；MiniQMT 未连接时显示 broker unavailable/waiting，但不隐藏策略包、不要求换包。
8. 页面不得出现让用户无处修改但必须满足的阻断文案，例如“请先启用 Paper”“Required paper-enabled policy”“策略包健康预检阻断”。

### 9.1 Paper v2 首页

- ready package count 改为 asset eligible count。
- workflow step 从“enable selection / enable paper”改为：

```text
1. QE 生成资产合格策略包
2. 运行选股
3. 创建 AIstock 或 MiniQMT 模拟盘
4. 启动/恢复 run/session
5. 查看运行证据和错误
```

- 不显示 `PAPER_ENABLED` 文案。

### 9.2 StrategyPackage 页面

- 删除“标记可用于选股”。
- 删除“标记可用于模拟盘”。
- 删除 `PAPER_CREATABLE_STATUSES` / `SELECTION_RUNNABLE_STATUSES`。
- 显示 `AssetEligibilityCard`：
  - 合格/不合格。
  - blockers。
  - warnings。
  - alpha core artifact 列表。
- 合格包直接显示：
  - “运行选股”。
  - “创建 AIstock 模拟盘”。
  - “创建 MiniQMT 模拟盘”。

### 9.3 Selection 页面

- 包列表显示所有 asset eligible packages。
- checkbox 不得因 health/risk warning 禁用。
- 运行按钮只因以下情况禁用：
  - 正在运行。
  - 未选择包。
  - 必填表单缺失，例如 trade_date/top_k 基础输入。
- package health、ST PIT、HMM、行业黑名单只显示 warning panel。
- 点击运行后由后端返回本次 run 结果或失败原因。

### 9.4 AIstock Paper 创建页

- StrategyPackage 下拉使用 asset eligible packages。
- execution policy 下拉允许为空，默认平台自动解析。
- 不显示 `paper_enabled` / `未启用` 文案。
- HMM config 只要求选择 model config；snapshot 可选；无 snapshot 时后端自动计算/缓存。
- readiness 改为可选诊断按钮，不是 run 前置按钮。

### 9.5 MiniQMT SIM 页面

- StrategyPackage 下拉使用 asset eligible packages。
- 创建组合不要求 `policyId`。
- execution policy option 不得因 `paper_enabled` disabled。
- MiniQMT connection/SIM status 显示为 broker runtime status：
  - 未连接：可以创建配置，但不能提交订单；tick 会等待/失败。
  - 非 SIM：禁止实际订单提交，并突出显示安全错误。
- 页面文案明确：MiniQMT 是订单/成交权威，TDX 不参与 MiniQMT 撮合。

### 9.6 Governance 页面

- `strategy-package-governance` 不再作为 Paper v2 流程入口。
- 如果保留，只改名/定位为“未来实盘治理证据”。
- `paper_ready` 字段改为 `live_governance_ready` 或仅显示 legacy label。
- 不再提供“启用 Paper”主操作。

## 10. 数据迁移设计

### 10.1 package_status 迁移

迁移 SQL 逻辑：

```sql
UPDATE strategy_pkg.package
SET package_status = 'BACKTEST_APPROVED'
WHERE package_status IN (
  'SELECTION_ENABLED',
  'PAPER_ENABLED',
  'PAPER_RUNNING',
  'PAPER_PASSED',
  'PAPER_FAILED'
);
```

要求：

- 执行前输出受影响行数和 package_id 样本。
- 执行后验证无 `SELECTION_ENABLED/PAPER_*` 当前状态。
- status event 历史保留。
- 如果存在 check constraint 或 enum，需要迁移 constraint/enum。
- 生产 DDL/DML 必须单独报告 `production_ddl_gate` 和数据迁移证据。

### 10.2 `paper_enabled` / `paper_candidate` 字段

分两阶段：

1. 功能删除阶段：业务代码和 UI 不再读取这些字段作为门禁。
2. schema 清理阶段：评估删除或保留 legacy 字段。

推荐：

- `strategy_pkg.validated_execution_policy.paper_enabled` 保留一版作为 legacy column，但不读不写、不显示为准入；后续 migration 删除。
- `strategy_pkg.runtime_variant.paper_candidate` 保留一版作为 legacy column，但 Selection/Paper 主路径不使用；后续 migration 删除或改名为 `legacy_paper_candidate`。

如果用户要求“物理字段也立即删除”，实现阶段必须增加 DDL migration、repository/model/API/test 同步删除，并在合入后执行生产 DDL gate。

## 11. 实施分阶段计划

### Phase 0：冻结设计与基线扫描

- 合入本设计文档。
- 生成当前门禁残留 grep baseline。
- 确认生产 `8001` 不触碰。

验证：

```powershell
rg -n "PAPER_ENABLED|PAPER_RUNNING|PAPER_PASSED|PAPER_FAILED|paper_ready|paper_candidate|paper_enabled|enable-paper|packageHealthRunnable" backend frontend tests -S
```

### Phase 1：后端 asset eligibility 和状态机精简

- 新增 `StrategyPackageAssetEligibilityService`。
- 精简 `PackageStatus`。
- 迁移 legacy status normalization。
- 替换 StrategyPackage validator/status whitelist。
- 删除 `enable-paper` / `enable-selection` 主路径依赖。

验证：

- asset eligibility unit tests。
- manifest drift / missing artifact / retired package negative tests。
- grep 确认 backend services 不再引用 `PAPER_*` 作为业务状态。

### Phase 2：Selection Center gate purge

- Selection list/run 使用 asset eligibility。
- `package_health` 从 hard block 改 warning/diagnostics。
- 删除 UI/后端 Selection package health hard block。
- HMM model config 自动计算/缓存链路纳入 runtime failure。

验证：

- asset eligible package 可直接选股。
- 包含 warning 的 package 仍可点击运行。
- 缺关键 runtime 数据时 run fail-fast，错误码不是 `STRATEGY_PACKAGE_ASSET_INVALID`。

### Phase 3：AIstock Paper gate purge

- portfolio create 只要求 asset eligibility。
- execution policy 不再要求 `paper_enabled`。
- runtime variant 不再要求 `paper_candidate`。
- readiness 改可选诊断，run 自动 preflight。

验证：

- BACKTEST_APPROVED / legacy migrated package 可直接创建 portfolio。
- 无 policyId 可创建，运行前平台解析默认 policy。
- 缺行情/分钟线/昨收只导致本次 run fail-fast。

### Phase 4：MiniQMT SIM gate purge

- MiniQMT UI/API 使用 asset eligible packages。
- 创建 binding 不要求 broker 当前连接和 `paper_enabled` policy。
- tick/order submit 前保留 SIM safety 和 broker runtime checks。
- 确认 MiniQMT 不使用 TDX 撮合。

验证：

- 未连接 MiniQMT 时可创建策略配置；tick 返回 broker unavailable/waiting。
- 连接后盘中可恢复运行。
- fake broker E2E 覆盖 order/trade/reconciliation。

### Phase 5：UI 全链路清理

- 删除 Paper v2 页面旧状态文案和按钮。
- 删除 disabled 门禁。
- Governance 页面移出 Paper 主路径。
- 错误展示按新 taxonomy。

验证：

- Playwright：选股/AIstock Paper/MiniQMT 页面都能选择 asset eligible package。
- Playwright：不出现 `PAPER_ENABLED`、`Required paper-enabled policy`、`策略包健康预检阻断` 等旧文案。
- UI console/pageerror/requestfailed clean。

### Phase 6：迁移、测试和设计合规复核

- 执行 DB migration 或生成待执行 migration。
- 更新 backend/frontend tests。
- 生成 validation record。
- 做 DESIGN-COMPLIANCE-001 item-by-item matrix。

验证：

- L0-L4 自动化通过。
- L5 MiniQMT SIM 在交易时段或受控环境验证；如无法当天完成，明确为实盘前阻断，不影响模拟盘功能合入。

## 12. 验证矩阵

| 层级 | 验证项 | 命令/方法 | 通过标准 |
|---|---|---|---|
| L0 grep | 删除旧业务门禁 | `rg -n "PAPER_ENABLED|PAPER_RUNNING|PAPER_PASSED|PAPER_FAILED" backend/services frontend/src/app/paper-v2 frontend/src/lib/paper-v2 -S` | 不得在业务判断/UI 主路径命中；迁移/历史记录除外。 |
| L0 grep | 删除 UI 禁用门禁 | `rg -n "packageHealthRunnable|selectedPackageBlocked|Required paper-enabled|paper_ready" frontend/src/app/paper-v2 frontend/src/app/strategy-package-governance -S` | Paper v2 主路径不命中。 |
| L1 unit | Asset eligibility | pytest strategy_package asset tests | 合格包通过；缺 manifest/hash/artifact/retired 失败。 |
| L1 unit | Status normalization | pytest migration/status tests | legacy `PAPER_*` 映射到 `BACKTEST_APPROVED`。 |
| L2 API | Selection direct run | TestClient / dev API | asset eligible package 无 enable-selection 可运行。 |
| L2 API | Paper create | TestClient / dev API | asset eligible package 无 enable-paper 可创建 portfolio。 |
| L2 API | MiniQMT create | TestClient / fake broker | asset eligible package 无 paper policy 可创建 binding。 |
| L2 negative | Runtime data missing | mocked provider | 返回 `MARKET_DATA_UNAVAILABLE` 等 runtime error，不返回 package gate error。 |
| L2 HMM | Auto coefficient/cache | mocked HMM service | 首次自动计算，二次命中缓存，无手工 snapshot。 |
| L3 UI | Selection UI | Playwright | 包不被禁用，运行按钮不因 health 阻断。 |
| L3 UI | AIstock Paper UI | Playwright | policy 可为空，页面无 `paper_enabled` gate 文案。 |
| L3 UI | MiniQMT UI | Playwright | asset eligible packages 可选，创建按钮不要求 policyId。 |
| L3 UI | No dead-end gate UX | Playwright + text scan | 不出现“先启用 Paper / Required paper-enabled policy / 策略包健康预检阻断”等用户无法在 UI 内解决的阻断流程。 |
| L3 UI | Runtime warning is non-blocking | Playwright | 有 HMM/ST PIT/broker/data warning 时，包仍可选择，运行按钮仍可点击，失败归属本次 run/session。 |
| L3 UI | Useful simplified display | Playwright + API fixture | 页面保留 asset eligibility、runtime diagnostics、HMM 自动计算/缓存、交易日状态、MiniQMT broker status、价格来源；删除 legacy paper lifecycle 噪音。 |
| L4 integration | LocalSim full day | dev backend + DB/fake data | selection/target/order/ledger/snapshot 完整，runtime 缺失 fail-fast。 |
| L4 integration | MiniQMT fake broker | fake broker E2E | order/trade/account/reconciliation 全链路。 |
| L4 dual backend | Same strategy LocalSim vs MiniQMT fake | oracle compare | selection evidence 一致；执行差异只来自 broker/fill。 |
| L5 manual | Real MiniQMT SIM | 用户确认交易时段执行 | 可盘中启动/恢复；SIM 下单/成交/对账证据完整。 |
| Compliance | DESIGN-COMPLIANCE-001 | validation matrix | 每个设计项有实现位置和测试证据。 |

## 13. 验收标准

实现完成后，必须同时满足：

1. 资产合格 StrategyPackage 可直接进入选股、AIstock 模拟盘、MiniQMT 模拟盘。
2. Paper v2 主路径没有 `PAPER_ENABLED/PAPER_RUNNING/PAPER_PASSED/PAPER_FAILED` 业务判断。
3. Paper v2 主路径没有 `paper_ready` gate。
4. Paper v2 主路径没有 `paper_enabled execution policy` gate。
5. Paper v2 主路径没有 `paper_candidate runtime variant` gate。
6. Selection UI 不再因为 package health 禁用包或运行按钮。
7. MiniQMT UI 不再只列 Paper 状态包，不再要求 paper-enabled policy。
8. UI 不再暴露 enable-selection / enable-paper 作为主路径操作。
9. UI 不再出现用户无法通过页面配置解决、但会阻断进入 Selection/Paper/MiniQMT 的 dead-end gate 文案。
10. UI 保留并简化真正有用的信息：asset eligibility、runtime diagnostics、HMM 自动计算/缓存、交易日状态、价格来源、MiniQMT broker status、run/session 错误和下一步动作。
11. HMM 不需要手工生成 snapshot；选择模型后自动计算/缓存。
12. 平台数据缺失只影响本次 run/session，不改变 StrategyPackage 状态。
13. MiniQMT 模拟盘不使用 TDX/DB/LocalSim 补成交。
14. 未来实盘仍必须走 LiveApproval，不能由 QE 包直接进入实盘。
15. 所有旧状态数据完成迁移或有明确兼容处理。
16. validation record 证明 L0-L4 通过；L5 MiniQMT SIM 若未完成，明确标记为实盘前验证项，不阻断模拟盘 gate purge。

## 14. 风险和缓解

| 风险 | 影响 | 缓解 |
|---|---|---|
| 旧测试大量依赖 `PAPER_ENABLED` fixtures | 测试失败多 | 统一更新 fixture 为 `BACKTEST_APPROVED` + asset eligibility。 |
| DB 里已有旧状态 | API 解析失败 | 先做 normalization，再做迁移。 |
| 外部脚本调用 enable-paper | 兼容问题 | endpoint 保留 deprecated/no-op 一版，日志提示迁移。 |
| 删除 `paper_enabled` 字段影响 repository | DDL 风险 | 功能上先不读不写，物理删除放 schema cleanup 阶段。 |
| runtime failure 被误报为 package invalid | 用户误解 | 新 error taxonomy 强制区分 asset vs runtime。 |
| 过度删除导致实盘安全边界丢失 | 高风险 | LiveApproval 和 MiniQMT live explicit authorization 保持独立，不进入模拟盘 gate。 |
| UI 放开选择后运行失败变多 | 正常暴露 runtime 问题 | run/preflight 错误必须清晰，可重试，不污染包。 |

## 15. 交付和合入流程

本项目不走 Issue/BUG 流程，按项目变更处理：

1. 先提交本设计文档到独立 docs 分支。
2. 用户批准后，合入 `main`。
3. 代码实现另开独立 feature worktree/branch。
4. 实现按 Phase 1-6 分阶段提交。
5. 每阶段提交必须有对应验证。
6. 最终合入前执行 DESIGN-COMPLIANCE-001 逐条复核。
7. 合入后报告：
   - branch / commit
   - changed files
   - tests/validation
   - production_ddl_gate
   - production_frontend_dependency_gate
   - production_backend_dependency_gate
   - production `8001` 是否触碰

## 16. 当前设计审批点

请用户审批以下关键选择：

1. `PAPER_ENABLED/PAPER_RUNNING/PAPER_PASSED/PAPER_FAILED` 从 StrategyPackage 状态机删除。
2. `SELECTION_ENABLED` 不再作为选股准入状态；实际使用 asset eligibility。
3. `enable-paper` / `enable-selection` 从 UI 和主业务路径移除；兼容 endpoint 可 no-op 一版。
4. `paper_enabled` execution policy 和 `paper_candidate` runtime variant 不再作为任何 Selection/Paper/MiniQMT 门禁。
5. 平台能力全部降级为 runtime run/session checks，不影响包准入。
6. 未来实盘只保留 LiveApproval 独立 gate，必须有模拟盘运行证据和人工审批。
7. UI 必须同步删除所有旧门禁入口、disabled 逻辑和 dead-end gate 文案；不得出现后端已放开但前端继续阻断的情况。
8. UI 必须同步简化展示，只保留资产合格、运行时诊断、交易日/HMM/价格/MiniQMT 状态和 run/session 证据等对用户有操作意义的信息。

审批后即可按本设计启动实现项目。
