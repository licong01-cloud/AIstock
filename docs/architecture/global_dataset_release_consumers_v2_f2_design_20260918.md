# 全局数据集 Release 消费者与通用行业上下文 F2 设计

> Feature ID: `global_dataset_release_consumers_v2`<br>
> Feature tier: F2<br>
> Date: 2026-09-18<br>
> Status: approved by user for implementation, validation, PR and merge; backend restart remains user-owned

## Background

AIstock 已有 repo-external `active_dataset_profile.json`、immutable candidate、节点路径映射和 QE resolve-once binding。当前 profile v1 只把 QE 作为一等消费者；既有 v2 只增加 QE sector-policy pins。HMM BUG-1510 reader 又把共享 `l2_code_id` 错当成 131 项数组的稠密下标。

R7 的整数 ID 来自 AIstock release-scoped 申万 L2 全目录，允许稀疏且可大于 130；稳定行业身份是 `801xxx.SI` 文本代码。R7 已被 active profile、历史实验和三节点副本引用，禁止原地修改。正确方案是构建 immutable successor，加入通用行业上下文，并通过一个 profile 指针统一切换新任务使用的数据 release。

## Scope

- 定义通用、release-scoped 的申万 L2 显式映射，不创建 HMM 专用 ID。
- 定义同源 market context 与 PIT 股票行业成员区间。
- HMM/QE 按显式 `l2_code_id -> canonical_l2_code` 映射读取，支持稀疏 ID。
- 增加 active profile v3；v1/v2 严格向后兼容。
- Windows、WSL、node1 仅维护 candidate-root 节点映射，各消费者从固定相对路径派生输入。
- 提供 create-exclusive successor 构建、验证、部署和 profile 激活 preflight。

## Non-goals

- 不修改 HMM 模型、特征、阈值、窗口、seed 或系数算法。
- 不修改 QE、Selection、Advisory 的业务语义。
- 不重编号或改写 R7 `sector_data.h5`、`static_factors.parquet`。
- 不重导 day、minute、factor、index、suspend 或股票池。
- 不创建 HMM 私有映射、占位行业或 silent fallback。
- 不执行训练、回测、实验、DDL/DML、依赖安装或服务控制。

## Architecture

### Immutable release 与 mutable pointer

candidate 内容不可变；`active_dataset_profile.json` 是唯一 mutable production pointer。successor 完整验证后才允许原子替换。任务创建时持久化 binding；历史任务 retry/resume 不重新读取 active profile。

### 通用行业身份

`l2_code_id` 是 release 内共享 join key，保留 R7 原值。`canonical_l2_code` 是稳定业务身份。通用映射必须逐项显式列出 ID 与文本代码，不得以数组位置表达身份。正式 131 个 member-backed SW2021 L2 通过 authority-backed 文本代码 projection 表达，不重新编号。

### 通用 sector-context component

组件根：`components/sector_context_candidate_v1/`

- `sector_code_map.json`
- `market_context.parquet`
- `sector_membership_spans.parquet`
- `component_receipt.json`

receipt 绑定上述文件、R7 `sector_data.h5`、R7 manifest、C-013 authority、日期范围和源定义。successor manifest 钉住 receipt 与每个文件的字节 SHA256。

### 消费者与节点解析

active profile v3 保留 controller/node candidate roots，新增通用 component pins 与 consumer requirements。v1/v2 继续按原合同读取。Windows、WSL、node1 从各自 candidate root 派生相同相对路径；不得分别维护 HMM/QE/Selection/Advisory 绝对路径。

这里的“一键切换”只覆盖共享 QE/HMM release 的消费者入口：QE 在创建任务时冻结 direct-v2 binding，HMM 读取同一个 active profile，RD-Agent dispatcher 为新任务从同一个 node binding 派生 day/minute/factor/manifest/sector-context 环境。已创建任务继续使用其持久化 binding，retry/resume 不漂移。Selection Center 的线上信号、Advisory 的 CAS snapshot/冻结 request 和实时数据库数据属于各自独立 authority，不得因 QE release 激活而被重写；只有它们显式消费共享 Qlib release 的新建任务才使用该绑定。

## Contracts

### `sector_code_map.json`

Schema: `aistock_release_sw_l2_code_map_v1`。

严格字段：

- `schema_version`
- `mapping_authority`: `authority_id`, `authority_sha256`
- `entries`: 按 `l2_code_id` 升序，每行包含 `l2_code_id`, `canonical_l2_code`
- `member_backed_codes`: 131 个按 canonical code 排序的正式文本代码
- `code_map_digest`
- `member_backed_digest`

约束：ID/code 分别唯一；代码符合 `801[0-9]{3}.SI`；所有 `sector_data.h5` 非负 ID 必须存在；所有 member-backed code 必须存在于 entries；不要求 ID 连续。

### `market_context.parquet`

字段严格为 `trade_date`, `sw_daily_total_vol`。definition 为 `sum_market_sw_daily_vol_all_rows_v1`。日期唯一升序，值 finite 且大于 0，覆盖 HMM 正式所需历史及 `2024-07-01..2026-08-28`。只允许数据准备 build-time 访问冻结源，消费者 runtime 禁止查询数据库。

### `sector_membership_spans.parquet`

字段严格为 `instrument`, `start_date`, `end_date`, `l2_code_id`。按股票与日期排序；区间是交易日闭区间；同股不重叠；ID 必须存在于 code map；目标窗口每个有效股票-日期必须闭合。

### Active profile v3

Schema: `aistock_active_dataset_profile_v3`。v1/v2 继续严格兼容。v3 新增：

- `components.sector_context_pins`
- `consumers.hmm`
- `consumers.selection`
- `consumers.advisory`

consumer 只声明 required component names，不复制路径或 hash。节点路径从 candidate root 与固定相对组件合同派生。

## Design Acceptance Index

| ID | 要求 |
|---|---|
| F-001 | R7 与历史 candidate 保持不可变，successor 使用新 generation/revision/manifest identity。 |
| F-002 | `l2_code_id` 保持 AIstock 共享 release 编号，稳定身份为申万文本代码。 |
| F-003 | 通用 code map 显式支持稀疏 ID，禁止数组位置映射。 |
| F-004 | 131 member-backed L2 来自正式 authority 文本 projection，不重新编号。 |
| F-005 | HMM reader 能读取 R7 ID，包括大于 130 的合法共享 ID。 |
| F-006 | QE blacklist 与 HMM 使用同一 code-map digest 和 membership identity。 |
| F-007 | market context 定义、覆盖、唯一性和正有限值全部 fail closed。 |
| F-008 | membership spans 无重叠、无歧义并覆盖目标窗口。 |
| F-009 | successor manifest/receipt 钉住新增资产与 R7 原组件 hash。 |
| F-010 | active profile v3 统一解析 Windows/WSL/node1 路径和 consumer requirements；新建 RD-Agent 任务自动取得同一 release binding，v1/v2 不回归。 |
| F-011 | 新任务读取新 generation；历史 retry/resume 使用旧 binding。 |
| F-012 | QE、Selection、Advisory 原数据 authority 与业务语义不因 HMM 兼容改变；独立 CAS/冻结 request 不被全局 profile 重写。 |
| F-013 | 激活前完成三节点 hash、dataset identity 与消费者最小读取；任一失败禁止激活。 |
| F-014 | 不执行 DDL/DML、训练、实验、依赖安装或服务控制。 |
| F-015 | 源码合入、数据构建、profile 激活、backend restart、runtime verification 分开报告。 |

## Implementation Plan

1. 新增通用 sector-context schema/validator/builder。
2. 修改 HMM frozen reader 与 QE sector policy reader，共用显式映射合同。
3. 扩展 profile v3 parser、runtime binding 和 activation preflight，保持 v1/v2 严格兼容。
4. 增加 successor CLI：校验基线 identity，复用原组件，生成 sidecar/receipt/manifest/profile candidate。
5. 定向测试、F2 validator、多轮静态审核、失败语义审核和 DESIGN-COMPLIANCE-001。
6. PR/CI/merge/close-sync，同步 main。
7. 从 active R7 构建 successor，三节点部署和只读验收。
8. 原子激活 profile，不重启 backend；等待用户重启后再做 runtime verification。

## Verification Plan

- map validator：稀疏 ID、>130、重复 ID/code、未知 ID、authority projection。
- HMM reader：R7 编号、hash pin、527 个目标交易日、无 DB fallback。
- QE sector policy：显式 code-to-ID、membership spans、blacklist semantics 不变。
- profile v1/v2 回归与 v3 required-component fail closed。
- successor builder create-exclusive、基线 hash 不变、manifest deterministic。
- Windows/WSL/node1 文件 SHA256 一致。
- QE/HMM/Selection/Advisory 最小 file-only 读取，不运行训练或回测。
- `git diff --check`、changed-file lint/compile、相关小矩阵、L0、CI。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | successor builder create-exclusive contract | `backend/tests/dataset_release/test_release_successor.py` | verified | none |
| F-002 | common sector map validator | `backend/tests/dataset_release/test_shared_sector_context.py` | verified | none |
| F-003 | explicit sparse ID map contract | `backend/tests/dataset_release/test_shared_sector_context.py` | verified | none |
| F-004 | member-backed authority projection | `backend/tests/dataset_release/test_shared_sector_context.py`; `X:/AIstock_dataset_candidates/backtest_dataset_candidates/20260831-qe_hmm_full_v2-direct-20260918-r8-candidate/components/sector_context_candidate_v1/component_receipt.json` | verified | none |
| F-005 | HMM frozen reader | `backend/tests/hmm_data_source/test_precompute_hmm_coefficients.py` | verified | none |
| F-006 | QE sector policy shared map reader | `backend/tests/quantevolver/test_qe_sector_risk_overlay_direct_v2_dataset_binding.py` | verified | none |
| F-007 | market-context validator | `backend/tests/dataset_release/test_shared_sector_context.py` | verified | none |
| F-008 | membership span validator | `backend/tests/dataset_release/test_shared_sector_context.py` | verified | none |
| F-009 | deterministic component receipt and manifest | `backend/tests/dataset_release/test_release_successor.py` | verified | none |
| F-010 | active profile v3 node/consumer resolver and dispatcher launch binding | `backend/tests/quantevolver/test_qe_active_dataset_profile.py`; `backend/tests/test_dispatch_service_env.py` | verified | none |
| F-011 | resolve-once persisted binding | `backend/tests/quantevolver/test_qe_active_dataset_profile.py`; `backend/tests/test_dispatch_service_env.py` | verified | none |
| F-012 | unchanged QE/Selection/Advisory authority boundaries | `backend/tests/quantevolver/test_qe_active_dataset_profile.py`; `backend/tests/test_dispatch_service_env.py` | verified | none |
| F-013 | activation preflight and cross-node receipt | `artifact:F:/Dev/RD-Agent-state/dataset_profiles/candidates/qe_hmm_full_v2_20260831_r8_successor_receipt.json` | verified | none |
| F-014 | zero-write/process safety | `python -m pytest backend/tests/dataset_release/test_release_successor.py backend/tests/dataset_release/test_shared_sector_context.py -q` | verified | none |
| F-015 | separated source/data/profile/runtime states | `artifact:F:/Dev/RD-Agent-state/dataset_profiles/candidates/qe_hmm_full_v2_20260831_r8_successor_receipt.json` | verified | none |

## Rollout / Rollback

源码先合入，数据 successor 后构建。successor 验收后原子替换 active profile；已有任务不迁移。backend restart 由用户执行。回滚仅允许原子切回上一份已验证 profile，不覆盖 candidate、不改写历史 binding。

## Risks / Failure Modes

- DB 分类目录相对 R7 漂移：以 H5/PIT 全行映射闭合检查 fail closed。
- member-backed projection 不是 131：fail closed。
- market context 缺交易日或非正值：fail closed。
- profile v3 某消费者缺组件：激活前拒绝。
- 远端字节不一致：禁止激活。
- runtime 仍缓存旧环境：报告 pending user backend restart，不误报为运行态生效。

## Production Gates

- DEV/production DDL/DML: noop
- dependency install: noop
- training/backtest/experiment: noop
- source merge: authorized after green review/CI
- dataset successor build/deploy/profile activation: authorized after all preflight gates pass
- backend restart: user-owned, forbidden in this task
- process control: noop

## DESIGN-COMPLIANCE-001

1. 禁止简化交付：全部 15 项验收必须闭合，否则明确 BLOCKED。
2. 禁止静默错误：映射、覆盖、hash、节点或消费者缺口全部 typed fail closed。
3. 禁止改变业务逻辑：仅统一数据身份与适配编号，不改模型、策略或研究语义。
4. 禁止私增审批：只保留输入正确性、既有授权和用户拥有的 backend restart 边界。
