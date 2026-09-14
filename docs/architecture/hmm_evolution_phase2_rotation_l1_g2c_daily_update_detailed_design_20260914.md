# HMM Evolution Phase 2 G2-C 受控日更新详细设计

> **版本**：v0.2
> **日期**：2026-09-14
> **设计层级**：F2
> **状态**：DESIGN_APPROVED_EXISTING_ATOMIC_ACTION_VERIFIED
> **批准记录**：2026-09-14 用户批准 G2C-D1～D6 及文档提交；DEV/生产 DML、r4 candidate 兼容、外部触发、runtime activation 与进程控制仍未授权。
> **父蓝图**：`hmm_evolution_and_risk_management_system_design_20260716.md`
> **终极目标**：让已经真实激活的 G2-A v1.6 L1 板块轮动预测，对正式 candidate 已覆盖的每个交易日可靠生成；不重新训练模型，不建设通用调度平台。当前月度 candidate 不能据此被描述为“每日收盘即有新数据”。

## 0. Background（背景）、权威与设计动机

G2-A v1.6 已完成正式零 fit development、生产写入、单日 `2026-08-31` 预测、repository/API/UI readback 和用户重启后的运行态验证。当前五轴状态为：

- `research_surface_status=AVAILABLE_EXPERIMENTAL`；
- `rotation_l1_capability_status=RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED`；
- `forward_power_status=INSUFFICIENT`；
- `forward_confirmation=PENDING_INSUFFICIENT_POWER`；
- `advisory_status=NOT_AVAILABLE`。

当前正式单日入口已经具备零 fit、无 target、显式 release、显式 `trade_date/as_of_date`、31-sector denominator、typed failure、数据库 target 校验、幂等 writer 和 receipt。G2-C 不复制这些能力，只把它们组织成一个可重复执行的日更新动作。

2026-09-14 只读连续性预检还确认：

- `20260905` v3 direct-v2 candidate 可被当前正式 reader 消费；
- `20260912-r4` candidate 虽为 `CANDIDATE_READY/PASS`，但其 `source_freeze=true`、`full_history_content_hash=true` 与当前 reader 已批准的 `false/false` 合同不同，已 typed fail closed；
- `2026-03-02..2026-03-31` 的 22 个连续 canonical 交易日全部完成 dry-run：每个日期 31/31 available、固定 model hash，22 个日期各有因果 input/mapping hash；相邻 `as_of_date` 无断裂；
- `2026-03-30` 同请求复跑的 canonical row hash 与完整 receipt hash 均 bitwise 相同；
- 23 次成功 receipt 全部 `model_fit_count=0`、`target_columns_read=false`、`tail_accessed=false`、`database_write_performed=false`、`runtime_action_performed=false`；
- 单日期冷启动需要校验约 5 GB 组件内容，首次实测约 15 分钟；OS cache 热态连续日期约 65～77 秒；该事实只用于运行设计，不授权省略 identity 校验或建立通用缓存平台。

## 1. Scope（范围）与非目标

### 1.1 本设计只包含

1. 复用现有 `scripts/hmm_risk/run_rotation_l1_product.py` 的零 fit 单日执行；
2. 将现有单进程、单日期 CLI 作为受控 `--once` 原子动作，绑定一份显式配置和一个明确交易日；
3. 复用现有 `hmm_risk.rotation_l1_prediction` writer/repository；
4. 输入就绪检查、幂等、revision/dedupe、readback 和 typed failure；
5. 受控运行的源码、测试、DEV 验证和后续独立 runtime activation。

### 1.2 明确不包含

- 不训练、refit、选择 seed、搜索参数或修改 v1.6 score/state/MBE；
- 不读取 sealed tail outcome，不改变 forward/advisory 状态；
- 不新建通用 scheduler、queue、registry、feature store、evidence 平台或训练平台；
- 不修改 direct-v2 数据生产、QE、Selection、Paper、Advisory 或其他业务模块；
- 不以旧预测、前值、neutral、空结果或较旧 release 补充失败日期；
- 不自动执行 DDL/DML、依赖安装、runtime activation 或进程启停。

## 2. Architecture（架构）与数据流

```text
显式 direct-v2 candidate + frozen v1.6 authorities
                         │
                  现有 product CLI 的 G2-C --once 调用
                         │
        既有 run_rotation_l1_product.py（0 fit）
                         │
         既有 prediction writer/repository
                         │
           31-row readback -> 既有 API/UI
```

G2-C 不新增第二个 runner，也不得实现预测公式、数据 reader 或第二 writer。现有 `run_rotation_l1_product.py` 已经是一次进程只处理一个显式日期的原子动作；本阶段只验证它可被安全重复调用。何时启用外部日调度、由哪个 target 调用、是否重启均在合入后另行授权。

本文的“`--once`”描述的是 one-process/one-date/one-receipt 的执行语义，不要求也不授权新增一个字面上的 `--once` CLI 参数。

## 3. Contracts（合同）：Proposed D1～D6

### G2C-D1：显式 release 与共同水位

每次请求必须显式给出一个绝对、非 symlink/junction 的 direct-v2 candidate root，不搜索 `latest`，不回退 `/home/lc999/data`、数据库或旧 candidate。reader 必须验证 profile、cutoff、状态、组件、receipt、31 个 SW L1 指数、CSI300、PIT universe、suspend/provider/security/industry authority 和 release 内部路径。

`trade_date=t` 必须存在于同一 release canonical calendar；`as_of_date` 必须是其严格前一交易日，且所有输入仅可读取至 `t-1`。candidate cutoff 早于目标交易日时沿用正式 reader 的 `hmm_risk_rotation_l1_input_bundle_source_range_incomplete`，不得缩短日期或复用上一日预测；本设计不为同一失败另造平行 reason code。

G2-C 的执行频率与数据 release 的可用频率必须分开报告。当前 direct-v2 是月度 candidate，因此本合同只能保证“已覆盖日期可逐日生成/补齐”，不能保证尚未进入 candidate 的最新交易日可在收盘后立即生成。若产品要求真正 T+1 日更，必须由数据准备 owner 另行提供同一正式 reader 可消费的日频增量 release；本任务不得通过数据库查询、旧资产或跨 release 拼接伪造该能力。

未来月度 candidate 若使用 `source_freeze=true/full_history_content_hash=true`，必须先由用户批准 reader 合同变化，并验证其声明哈希；本设计不把更强或不同的 state 自动解释为兼容，也不改写 candidate。当前可执行基线仍为已经真实消费的 `20260905` v3 release。

### G2C-D2：零 fit 日更新

日更新只调用既有 v1.6 product executor：

- `model_fit_count=0`；
- `target_columns_read=false`；
- `tail_accessed=false`；
- 固定 model/formula/development acceptance/input/mapping authority；
- 只从 t-1 moneyflow source 形成 `moneyflow_intensity_delta_5d` 横截面 average-rank centered score；
- 31 个 sector 全部形成明确 `available|unavailable`，不得删除失败 sector。

新增入口不得复制预测公式或直接读取 H5/pandas 绕过正式 reader。它只负责参数闭合、调用、状态和 receipt。

### G2C-D3：幂等、revision 与 readback

沿用现有唯一 writer/repository 和表：

- 相同 `(trade_date, sector, model/input/mapping identity)` 与相同业务 payload 重试为幂等成功；
- 相同日期但不同 authority 或不同业务 payload 不覆盖旧行，必须使用现有 revision/reclosure 语义或 typed conflict；
- 每次成功必须回读 31 行、唯一 sector、model/input/mapping hash、as-of、状态与 canonical row hash；
- `write complete`、`readback verified`、`API visible` 和 `runtime active` 分别报告，不互相推导。

失败不得删除、手工修改或覆盖旧预测。

### G2C-D4：执行方式与触发边界

首版直接复用现有单日期 CLI 作为窄范围 `--once` 动作，不再新增包装 runner，不注册 FastAPI startup job，不启动常驻 worker，不引入 cron/leader election/通用 queue。调用必须显式接收：

- candidate root；
- development acceptance 与两个 child；
- v1.4 reference/input 与当前 v1.6 input；
- security/provider/industry authorities；
- `trade_date/as_of_date`；
- output receipt；
- 可选且需单独授权的数据库 target。

后续是否由现有外部调度设施每日调用 `--once` 属于独立 runtime activation；不由源码合入自动启用。用户后端及调度进程控制权保持为用户所有。

### G2C-D5：失败、重试与时效

状态最小集合：

```text
NOT_STARTED -> INPUT_READY -> GENERATED -> WRITTEN -> READBACK_VERIFIED
                    \-> FAILED_TYPED
```

以下均 fail closed 并保留具体 reason：release 未就绪、日期/共同水位不完整、authority/hash 不一致、31-sector denominator 不足、输入非有限、mapping 缺失、score/state 无效、数据库 target 不符、write/readback conflict。

确定性输入或 authority 失败不得自动重试。临时数据库连接失败只允许由外部 operator 对同一不可变请求重新执行；幂等性由现有 writer 验证。首版不引入内部 retry loop 或 next-run fallback。

冷启动耗时与读取字节保留在本次离线诊断结果中，不为此扩展正式产品 receipt schema；在用户批准明确 SLA 前不新增产品 promotion gate。当前约 15 分钟的冷启动只证明可运行，不证明适合高频或盘中调度；G2-C 目标仅为日频收盘后更新。

### G2C-D6：验收与产品语义

源码实施前先完成 development 已消费区间内至少 20 个连续交易日的零写入 dry-run；该验证只证明工程连续性，不冒充 forward/tail 证据。验收至少覆盖：

- 每日 31 行与完整 sector denominator；
- `as_of=t-1`；
- model hash 固定，input/mapping hash 按日期因果变化；
- 同一请求复跑 canonical row hash 一致；
- 停牌、provider absence、缺失与非有限值使用现有 typed 语义；
- `model_fit_count=0`、`target_columns_read=false`、`tail_accessed=false`、`database_write_performed=false`；
- 旧 release、未知 state schema、跨 release 组件和过期 candidate 均不得回退成功。

实现后执行 changed files → ownership → module registry → test plans、定向 pytest、Ruff、py_compile、HMM 模块计划、L0、F2 validator 和 `git diff --check`。生产 DML、runtime activation 和用户重启后的 API/UI readback仍为独立授权。

## 4. Implementation Plan（实施方案）

批准后优先采用零业务源码变化：若现有单日 CLI 已能由受控外部触发满足 D1～D6，则不增加包装入口或重复 executor。只有以下真实缺口允许源码修复：

1. 无法在一次请求中持久化完整 typed receipt；
2. 幂等或 readback 与既有 repository 合同不一致；
3. 当前 release/date/authority 参数无法显式传入；
4. 运行失败会静默复用旧日期或旧 identity；
5. 已批准 candidate state 与正式 reader 存在经用户裁定后的兼容缺口。

任何实现均限制在 HMM-owned source、CLI、直接测试和本设计。若需要修改数据生产、共享 scheduler 或其他业务模块，停止并拆交相应 owner，不扩大本任务。

实施顺序保持一个业务闭环，不拆成多个小PR：

1. 先以连续development日期dry-run闭合现有执行器的真实行为与性能；
2. 用户批准D1～D6后，先以现有CLI及直接测试闭合合同；只有证明存在上列真实缺口才在同一feature分支作最小修复；
3. 完成最多三轮代码审核/修复，运行所属模块门禁并创建一个源码PR；
4. 合入后只在现有DEV数据库执行真实write/readback；
5. 生产DML、runtime activation和用户重启后的API/UI验证继续分别授权。

## 5. Verification Plan（验证方案）

验证分为四层且不能互相代报：

1. **离线连续性**：至少20个连续development日期dry-run、同日复跑、错误candidate反例；
2. **源码合同**：直接pytest、Ruff、py_compile、scope/ownership/module/test-plan、HMM模块、L0、F2 validator；
3. **DEV业务读写**：31行写入、幂等重试、冲突/revision、repository/API readback；
4. **生产运行态**：仅在单独授权后执行目标DML/activation，由用户重启后核验runtime identity和真实API/UI。

历史回放不进入forward指标，不读取outcome，不改变五轴能力状态。

## 6. Risks / Failure Modes（风险与失败模式）

| 风险 | 防护与真实代价 |
|---|---|
| 月度candidate晚于运行日仍未就绪 | 返回既有typed `hmm_risk_rotation_l1_input_bundle_source_range_incomplete`；代价是只能补齐candidate已覆盖日期，不能声称T+1日更，不用旧值伪造成功 |
| 新candidate state语义与reader不同 | 保持fail closed并请求数据消费合同裁定；不因`PASS`字符串自动兼容 |
| 单日冷启动读取量大 | 首次实测约15分钟；先保留完整identity校验，设计日频而非盘中；不先建缓存平台 |
| 重复触发 | 现有writer幂等与readback；不同payload显式冲突或revision，不覆盖旧行 |
| 调度误启或多实例 | 首版无内建scheduler/常驻worker；runtime activation单独授权 |
| 页面显示research为advisory | 继续使用五轴状态并显示forward未确认/功效不足；runner不能提升advisory |

## 7. Design Acceptance Index

| design_item | decision | 精确内容 | 当前状态 |
|---|---|---|---|
| F-001 | G2C-D1 | 显式单 release、canonical t/t-1、无 latest/旧路径 fallback；月度覆盖不冒充T+1日更；新 freeze 语义须另批 | APPROVED_BY_USER_VERIFIED |
| F-002 | G2C-D2 | 复用 v1.6 executor，零 fit、零 target、零 tail | APPROVED_BY_USER_VERIFIED |
| F-003 | G2C-D3 | 复用唯一 writer/repository，幂等、revision/conflict、31-row readback | APPROVED_BY_USER_VERIFIED |
| F-004 | G2C-D4 | 复用现有单日期CLI作为`--once`；无第二runner、常驻worker、startup job或新scheduler | APPROVED_BY_USER_VERIFIED |
| F-005 | G2C-D5 | typed failure；无内部 retry/fallback；耗时先诊断后裁定 SLA | APPROVED_BY_USER_VERIFIED |
| F-006 | G2C-D6 | 至少20连续development日期dry-run + 所属模块验证；生产动作独立 | APPROVED_BY_USER_DIAGNOSTIC_VERIFIED |

## 8. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `backend/services/hmm_risk/rotation_l1_input_bundle.py`现有显式reader | artifact: `F:/Dev/AIstock_validation_clean/hmm_g2a_v16_continuity_380a8f24_20260914/.2026-03-30.receipt.json.failure.d0f163dbd1b54c118430c1f5cf1edd7f.json`；`backend/tests/hmm_risk/test_rotation_l1_input_bundle.py` | VERIFIED_EXISTING_BEHAVIOR | 无 |
| F-002 | `scripts/hmm_risk/run_rotation_l1_product.py`、`backend/services/hmm_risk/rotation_l1_prediction.py` | artifact: `F:/Dev/AIstock_validation_clean/hmm_g2a_v16_continuity_380a8f24_20260914`；`backend/tests/hmm_risk/test_run_rotation_l1_product.py` | VERIFIED_EXISTING_BEHAVIOR | 无 |
| F-003 | `backend/services/hmm_risk/rotation_l1_prediction.py`唯一repository | `backend/tests/hmm_risk/test_rotation_l1_prediction.py`；artifact: `F:/Dev/AIstock_validation_clean/hmm_rotation_g2a_v16_product_validation_20260914/surface_validation_receipt.json` | VERIFIED_EXISTING_BEHAVIOR | 无 |
| F-004 | `scripts/hmm_risk/run_rotation_l1_product.py`现有单日期CLI | `backend/tests/hmm_risk/test_run_rotation_l1_product.py`；无startup/scheduler静态检查 | VERIFIED_EXISTING_BEHAVIOR | 无 |
| F-005 | 既有typed executor/failure receipt | `backend/tests/hmm_risk/test_run_rotation_l1_product.py`；artifact: `F:/Dev/AIstock_validation_clean/hmm_g2a_v16_continuity_380a8f24_20260914/.2026-03-30.receipt.json.failure.d0f163dbd1b54c118430c1f5cf1edd7f.json` | VERIFIED_EXISTING_BEHAVIOR | 无 |
| F-006 | 本设计§5 | artifact: `F:/Dev/AIstock_validation_clean/hmm_g2a_v16_continuity_380a8f24_20260914`；`backend/tests/hmm_risk/test_rotation_l1_prediction.py` | VERIFIED_DIAGNOSTIC | 无 |

## 9. Rollout / Rollback（发布与回滚）

- 源码合入不自动启用日更新；runtime默认保持当前v1.6单日产品状态。
- 首次DEV运行只处理一个显式日期；通过幂等/readback后再验证连续日期。
- 生产首次运行、外部调度接入和后端重启分别授权；未授权时保持inactive。
- 回滚停用外部触发并恢复上一份已验证runtime receipt，不删除历史预测、不改写模型或candidate。
- forward FAILED时沿用G2-A合同停止新增预测；不由G2-C覆盖该状态。

## 10. Production Gates

| gate | 本次状态 |
|---|---|
| production_ddl_gate | noop；复用既有表 |
| production_dml_gate | noop；连续性验证仅dry-run |
| production_backend_dependency_gate | noop |
| production_frontend_dependency_gate | noop |
| runtime_activation_gate | pending future authorization |
| backend_restart_owner | user |

当前设计/诊断任务`runtime_impact=none`。未来若修改backend运行时源码，必须按实际changed files重新生成runtime contract，不能由本表降级。

## 11. DESIGN-COMPLIANCE-001 设计审核清单

1. **禁止简化交付**：`--once` 是日更新的完整原子动作，不被描述为自动 scheduler 已上线；数据库、API/UI和runtime分别验证。
2. **禁止静默错误**：无旧值、默认 state、neutral、前填、空 200、旧 release 或跨 release fallback。
3. **禁止改变业务逻辑**：v1.6 score、state、MBE、model/input/mapping identity、forward与advisory状态不变。
4. **禁止私增门禁审批**：只保留现有 authority/causal/readback 保护；性能先记录，不在用户批准前形成新 gate。

## 12. 当前任务停止位置

本次长任务已完成连续日只读诊断、蓝图现状同步和本设计的正式审核；现有 HMM 源码已经满足批准的原子执行合同，因此不新增包装 runner。文档提交、推送和创建 PR 已于 2026-09-14 获授权；PR 合入、DEV/生产数据库写入、r4 合同变化、tail、外部触发、runtime activation、cleanup 与进程控制仍须分别授权。
