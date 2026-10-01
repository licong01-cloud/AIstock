# Advisory QE 交付消费兼容预检 F2 详细设计

> 版本：v1.0
>
> 日期：2026-09-13
>
> 状态：IMPLEMENTED_LOCAL_VERIFIED_PR_PENDING
>
> 归属：Selection Center / Advisory
>
> 上位蓝图：`advisory_strategy_conditioned_model_blueprint_v1_20260710.md` v3.55

## 1. 背景与目标

QE 是模型、因子、seed、组合和回测实验的唯一所有者；Advisory 只消费 QE 正式交付的 StrategyPackage/预测，并把它绑定为独立 Program 后提供荐股业务能力。Advisory 已支持全市场、单核心指数和多核心指数并集，但当前在创建 Program 或切换 Binding 前，没有一个只读入口同时核对 StrategyPackage 身份、包内冻结股票池证据、目标 Advisory 股票池以及 Advisory model descriptor 阶段。

现存 StrategyPackage 多为旧版包，未声明 `universe_selection`。这类包仍可沿用既有 Selection 候选后置过滤能力，但不得宣称为“QE 已在目标指数池内训练/推理”。未来 QE 包可能在冻结 `backtest_context.daily_strategy.custom_params.universe_selection` 中携带股票池身份；Advisory 必须使用公开交付字段进行分类，不读取或修改 QE 内部状态。

本切片提供绑定前只读预检，使用户在创建或更新荐股任务前得到可审计结论：精确匹配、仅后置过滤兼容、旧包身份未声明、身份不匹配、交付合同不完整，以及 descriptor 当前阶段。

## 2. 范围

1. 新增 Advisory 自有只读交付预检服务与 API。
2. 校验 package id、manifest hash、生命周期和资产准入摘要。
3. 从 StrategyPackage 已冻结公开字段提取股票池证据，并与 Advisory 目标股票池比较。
4. 对多处股票池证据做一致性检查；冲突或非法形状 fail closed。
5. 对既有 active binding 检查 descriptor 文件是否存在；对尚未创建/变化后的 binding 明确返回“绑定后需要”，不冒充完整解析。
6. 页面在创建 Program 和应用新 Binding 前调用预检，展示结论；只有硬阻断结论禁止继续。
7. 保持旧包与既有全市场/指数后置过滤路径兼容，不改变原 score、rank、策略包状态或 Program 数据。

## 3. 非目标与边界

- 不运行、重试或修改任何 QE 实验，不修改 `backend/services/quantevolver/**`、QE router/registry/profile/dataset/composer。
- 不修改 Selection Center、StrategyPackage 公共模型或数据库 schema。
- 不把 Advisory 后置指数过滤声明为 QE 指数池训练、推理或 Alpha 增强。
- 不创建 StrategyPackage，不改变包状态，不写 Program/Binding，不生成 descriptor。
- 不验证模型经济效果，不补跑 seed、LOO、组合、因子或回测。
- 不在预检中完整解析模型 bundle；完整 descriptor/package/Selection run 一致性仍发生在发布时。
- 不执行 DDL/DML、后端重启、Worker/Scheduler/WSL/远端进程控制。

## 4. 架构

```text
StrategyPackage 公开冻结记录 ─┐
                              ├─ AdvisoryDeliveryPreflightService ─ API ─ UI
Advisory 目标 universe ───────┤
可选 Program active binding ──┘
```

服务只读调用 `StrategyPackageService.get_package()` 与资产 eligibility 摘要；可选 Program 只用于判断本次请求是否与 active binding 完全相同。预检不落库，不调用 QE，不启动 Selection，不读取行情。

## 5. Contracts / 合同

### 5.1 请求

`POST /api/v1/advisory/delivery-preflight`

```json
{
  "package_id": "pkg_xxx",
  "universe_selection": {"mode": "single_index", "pool_ids": ["csi300"]},
  "target_count": 20,
  "program_id": "advp_optional"
}
```

- `package_id` 必填且只允许一个原生 StrategyPackage。
- `universe_selection` 复用 Advisory 已有规范化合同。
- `target_count` 必填且为 1～100；它决定默认 review policy，并参与新旧 Binding 身份判断。
- `program_id` 可选；仅在编辑既有 Program 时用于 active-binding descriptor 阶段判断。
- Pydantic 拒绝未知字段，防止调用方误以为未实现参数生效。

### 5.2 响应

```json
{
  "schema_version": "advisory_delivery_preflight_v1",
  "overall_status": "READY_WITH_MODEL | READY_BASELINE_ONLY | BLOCKED",
  "package": {
    "package_id": "pkg_xxx",
    "manifest_sha256": "...",
    "package_status": "SELECTION_ENABLED",
    "source_type": "qe_experiment",
    "source_id": "...",
    "asset_eligible": true
  },
  "universe_compatibility": {
    "status": "EXACT_UNIVERSE_MATCHED | FILTER_ONLY_COMPATIBLE | LEGACY_UNIVERSE_UNSPECIFIED | PACKAGE_IDENTITY_MISMATCH | DELIVERY_CONTRACT_INCOMPLETE",
    "requested": {"mode": "single_index", "pool_ids": ["csi300"]},
    "source_declared": {"mode": "stock_universe", "pool_ids": []},
    "evidence_paths": ["backtest_context.daily_strategy.custom_params.universe_selection"]
  },
  "policy_compatibility": {
    "status": "ACTIVE_POLICY_MATCH | NEW_POLICY_BINDING_REQUIRED",
    "requested_target_count": 20,
    "active_target_count": 20,
    "package_backtest_topk": 50,
    "package_policy_authority": "DIAGNOSTIC_ONLY_NOT_ADVISORY_RUNTIME_AUTHORITY"
  },
  "model_compatibility": {
    "status": "DESCRIPTOR_FILE_PRESENT | MODEL_DESCRIPTOR_UNAVAILABLE | REQUIRED_AFTER_BINDING | NOT_APPLICABLE",
    "validation_stage": "PRESENCE_ONLY | PUBLICATION_FULL_RESOLUTION | NOT_APPLICABLE"
  },
  "blockers": [],
  "warnings": []
}
```

响应不返回本地绝对路径、凭据或 QE 内部配置。

## 6. 股票池证据与分类

### 6.1 权威顺序

1. `manifest.backtest_context.daily_strategy.custom_params.universe_selection`：冻结实验/回测股票池证据。
2. `manifest.source_evidence.custom_params.universe_selection`：只作交叉核对；若它单独出现而主证据缺失，则交付合同不完整，不得提升为运行时权威。
3. `canonical_pit_binding`：只增加 PIT release 身份置信度，不定义股票池。

若两处 `universe_selection` 同时存在，规范化后必须完全一致；否则为 `DELIVERY_CONTRACT_INCOMPLETE`。未知 mode、非法 pool、缺少必需 pool 或多余字段同样为合同不完整。没有任何声明时为 `LEGACY_UNIVERSE_UNSPECIFIED`，不是精确匹配。

### 6.2 兼容矩阵

| 源包声明 | Advisory 请求 | 结论 | 是否阻断 |
|---|---|---|---|
| 与请求完全相同 | 任意合法模式 | `EXACT_UNIVERSE_MATCHED` | 否 |
| `stock_universe` | 单指数/指数并集 | `FILTER_ONLY_COMPATIBLE` | 否 |
| 指数并集 | 其相同或真子集 | `FILTER_ONLY_COMPATIBLE` | 否 |
| 单指数/指数并集 | 请求含源集合之外股票池或全市场 | `PACKAGE_IDENTITY_MISMATCH` | 是 |
| 无声明的旧包 | 任意合法请求 | `LEGACY_UNIVERSE_UNSPECIFIED` | 否，警告 |
| 声明非法或多处冲突 | 任意请求 | `DELIVERY_CONTRACT_INCOMPLETE` | 是 |

`FILTER_ONLY_COMPATIBLE` 只表示源候选可被 Advisory 进一步缩小；它不等价于在目标股票池内重新训练或重新推理。旧包未声明时保留现有业务兼容性，但页面必须展示身份未知。

## 7. Package 与资产检查

- 包不存在：现有 typed `DataUnavailableError` 映射为 404。
- `RETIRED`：`BLOCKED`，blocker=`PACKAGE_RETIRED`。
- manifest record id/hash 与 payload 不一致、资产 eligibility hard blocker：`BLOCKED`。
- 其它既有非退休状态不增加新的私有生命周期门禁；状态原样展示，资产警告原样压缩为 warning。
- 预检不调用严格 live governance，也不改变 selectable-packages 的现行准入语义。

## 8. Descriptor 阶段

1. 新建 Program、未提供 `program_id`：`REQUIRED_AFTER_BINDING`。descriptor 路径依赖新的 binding version，绑定前不存在是正常状态。
2. 编辑 Program 且 package/universe/target_count 与 active binding/current Program 不完全相同：`REQUIRED_AFTER_BINDING`。
3. 编辑 Program 且 package/universe/target_count 与当前身份完全相同：若配置了 model root，则只检查 exact program/binding descriptor 文件存在性；存在为 `DESCRIPTOR_FILE_PRESENT`，缺失为 `MODEL_DESCRIPTOR_UNAVAILABLE`。
4. 未配置 model root：`MODEL_DESCRIPTOR_UNAVAILABLE`。

文件存在只表示 presence；完整 manifest/package/Selection run/feature schema/bundle 校验仍由 publication resolver 执行。descriptor 缺失不阻断 Selection baseline，overall 为 `READY_BASELINE_ONLY`。

## 9. UI 行为

- 创建区和策略绑定管理器分别保存最近一次预检结果。
- package 或 universe 变化时使旧结果失效；点击创建/应用时先重新预检。
- `BLOCKED`：显示 blocker 并停止确认框与写请求。
- `READY_BASELINE_ONLY`：显示“基线可用、Advisory 模型未就绪/绑定后需要”，继续原确认与写请求。
- `READY_WITH_MODEL`：显示精确 active descriptor presence，但仍注明完整解析发生在正式发布。
- 状态和股票池结论使用明确中文，不给收益承诺。

## 10. 失败语义

- 包/资产/身份硬错误：typed blocker，fail closed。
- 旧包缺少股票池声明：兼容警告，不静默标 exact。
- descriptor 不存在：模型角色 typed unavailable，baseline 继续。
- 预检 API 网络/服务失败：页面不执行后续创建/应用，避免绕过本次显式用户检查；直接 API 的既有 create/apply 行为不在本切片内改变。

## 11. Implementation Plan / 实施计划

1. 新增纯只读 Advisory service，先完成 package/asset、universe、policy 与 descriptor 阶段分类。
2. 新增依赖注入 API 和 typed frontend client。
3. 在 Program create 与 Binding apply 写请求前调用预检并展示结果。
4. 完成 backend/API/UI 定向测试，覆盖硬阻断不写入和 baseline-only 兼容。
5. 更新验收矩阵，执行多轮审核、F2 validator 与 DESIGN-COMPLIANCE-001。

## 12. Verification Plan / 验证计划

### 12.1 Backend

- exact、全市场到指数 filter-only、并集到子集 filter-only、受限源扩大请求 mismatch。
- legacy unspecified、非法 shape、双证据冲突。
- retired、asset blocker、正常警告。
- 新 binding required-after-binding、target_count 变化触发新 policy identity、active binding descriptor presence/missing、未配置 root。
- 服务零写操作、零 QE 调用。

### 12.2 API/UI

- API 请求/响应、404/400/blocked 契约。
- 创建和 apply 先调用预检；blocked 不发送写请求。
- baseline-only 仍可确认并继续；package/universe 变化后重新预检。
- 页面展示 exact/filter-only/legacy/mismatch/incomplete 与 descriptor 阶段。

### 12.3 边界与回归

- Advisory backend/watchlist 定向测试。
- frontend type/lint 与 Advisory Playwright 定向测试。
- changed-file 扫描确认无 QE/Selection/StrategyPackage 公共源码变化。
- `git diff --check`、F2 validator、DESIGN-COMPLIANCE-001。

## 13. Risks / 风险

1. 把全市场包后置过滤误报为指数专属包：强制独立 `FILTER_ONLY_COMPATIBLE` 状态和 UI 说明。
2. 旧包无身份导致全部业务中断：保留 `LEGACY_UNIVERSE_UNSPECIFIED` baseline 兼容，同时禁止标 exact。
3. source_evidence 被误作运行时权威：缺 primary frozen backtest evidence 时 fail closed。
4. 只改 target_count 却复用旧 descriptor：target_count 纳入当前 policy/binding 身份判断。
5. 文件 presence 被误报为完整模型可用：响应固定区分 `PRESENCE_ONLY` 与 publication full resolution。
6. 预检扩张为 QE 或包治理平台：只使用现有公开记录和最小响应，不新增表、任务、审批或 UI 平台。

## 14. 发布与回滚

源码合入、后端重启和运行时 readback 分别报告。合入后由用户重启后端；随后用一个现有 legacy 包验证兼容警告，并在未来 QE 正式交付带股票池身份的包后验证 exact 正路径。无 DDL/DML。

回滚仅回退本 PR。预检没有持久化数据，因此无需数据修复；既有 Program/Binding 不受影响。

## 15. Production Gates / 生产门禁

1. Backend/API/UI 定向测试和前端类型/lint 通过。
2. F2 validator 与 DESIGN-COMPLIANCE-001 逐项通过。
3. changed-file 中没有 QE/Selection/StrategyPackage 公共源码，且无实验、DDL/DML或后端进程控制。
4. 当前旧包只按实际返回 legacy/filter-only，不伪造 exact 正路径；future exact 由合同夹具覆盖并等待正式包运行时 readback。
5. 合入后仍由用户执行后端重启，运行时验证与 source merge 分开。

## 16. Design Acceptance Index

| ID | Requirement |
|---|---|
| F-001 | 预检只消费 StrategyPackage 公开冻结身份，不运行或修改 QE |
| F-002 | 包身份、生命周期与资产 hard blocker 可审计且 fail closed |
| F-003 | 股票池证据按冻结字段提取、交叉核对并区分 exact/filter-only/legacy/mismatch/incomplete |
| F-004 | target_count/policy 与 descriptor 绑定前、active presence、publication full resolution 阶段不混淆 |
| F-005 | API 只读、无数据库写入或运行时副作用，响应不泄露本地路径或敏感配置 |
| F-006 | UI 在创建与 Binding apply 前预检，硬阻断停止写请求，baseline-only 可继续 |
| F-007 | 旧包和既有 baseline 行为兼容，后置过滤不冒充 QE 指数池训练/推理 |
| F-008 | 修改范围仅含 Advisory 源码/API/UI/测试和文档，无 QE/Selection/StrategyPackage 公共源码变更 |

## 17. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `backend/services/advisory_delivery_preflight.py` | `backend/tests/watchlist/test_advisory_delivery_preflight.py`; forbidden-path scan | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-002 | package lifecycle + `asset_eligibility.summarize` classifier | `python -m pytest backend/tests/watchlist/test_advisory_delivery_preflight.py -q -p no:cacheprovider` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-003 | `_classify_universe`、`_is_filter_only_compatible` | `backend/tests/watchlist/test_advisory_delivery_preflight.py` exact/filter/legacy/mismatch/incomplete/secondary-only tests | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-004 | `_policy_compatibility`、`_model_compatibility` | `backend/tests/watchlist/test_advisory_delivery_preflight.py` target-count/new/active/presence/missing tests | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-005 | `POST /advisory/delivery-preflight` | `backend/tests/watchlist/test_advisory_delivery_preflight_api.py` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-006 | `frontend/src/lib/api/advisory.ts`、`frontend/src/app/paper-v2/advisory/page.tsx` | `playwright test tests/paper-v2/paper-v2-advisory-ui.spec.ts`：17 passed；frontend type/lint | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-007 | legacy baseline-only warning + blocked mutation guard | `backend/tests/watchlist/test_advisory_delivery_preflight.py`；`frontend/tests/paper-v2/paper-v2-advisory-ui.spec.ts` | IMPLEMENTED_LOCAL_VERIFIED | none |
| F-008 | current branch changed-file scope | `backend/tests/watchlist/test_advisory_delivery_preflight.py`；`frontend/tests/paper-v2/paper-v2-advisory-ui.spec.ts`；`git diff --name-only origin/main`；`git diff --check` | IMPLEMENTED_LOCAL_VERIFIED | none |

## 18. DESIGN-COMPLIANCE-001 交付检查

- [x] 上位蓝图、设计、实现和测试逐项映射，无未声明的删减。
- [x] 真正使用 StrategyPackage 记录，不以 mock 或 UI 字符串冒充业务实现。
- [x] exact/filter-only/legacy/mismatch/incomplete 与 policy/descriptor 阶段均有直接测试。
- [x] API/UI/服务完整交付，而不是仅后端局部能力。
- [x] 旧包兼容与硬错误 fail closed 同时成立，无隐藏 fallback。
- [x] F2 validator、定向回归、静态检查、changed-file 边界和多轮审核通过。
- [x] merge、后端重启、运行时 readback 分开报告；本任务不越权重启或执行生产操作。
