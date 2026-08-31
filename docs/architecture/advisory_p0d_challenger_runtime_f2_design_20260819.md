# Advisory P0-D Challenger Runtime F2 设计

> Feature tier: F2
> 状态：源码与本地验证通过，等待 PR/合入；运行时 descriptor 发布、后端重启与正式激活不在本次自动执行范围。

## 1. Background / 背景

P0-D 已产出精确的 `advisory_meta_label_bundle_v1`：模型根据 Selection Top20 的候选级 PIT 特征输出 take/skip 概率，离线结果相对 Selection Top5 有正增量，但 AUC 接近随机且 PBO 为 0.40，因此只能作为 `EXPERIMENTAL_MODEL / UNCALIBRATED` challenger。当前实时 `AdvisoryModelShadowService` 只支持旧 M1/M5 quality bundle；若直接发布 descriptor，会把元标签 bundle 送入错误加载器，不能形成真实的前向模型排序。

本功能补齐“精确 descriptor 解析、精确元标签 bundle 加载、同源 PIT 特征构建、Top20 入场优先级重排、forward observation 留痕”闭环。它不改变正式荐股结果，而是为后续按真实交易日累计 challenger 胜率、收益和稳定性提供可执行输入。

## 2. Scope / 范围

- descriptor v2 明确声明 `model_role=meta_label_take_skip_confidence`、精确 bundle 身份、Selection 运行语义、shadow policy、Alpha component roles 与终端权重。
- 从 `<AISTOCK_ADVISORY_MODEL_ROOT>/meta_label_bundles/<bundle_id>` 精确加载；禁止扫描 latest、模糊匹配或回退旧 bundle。
- 使用与旧 shadow 相同的 persisted Selection、PIT 时钟、数据库只读快照和共享特征构建器，对 Selection Top20 一次性构建实时特征。
- 使用 bundle 内冻结的 fresh HMM continuation state，严格从其 cutoff 向 decision date 续推。
- take probability 只产生 entry priority rank；`selection_exit_rank` 始终等于原 `selection_effective_rank`。
- forward observation 冻结 descriptor/model role/bundle 身份，保留 baseline 与 challenger 两条独立结果。
- 旧 descriptor v1 和 M1/M5 shadow 行为保持兼容。

允许修改：

- `backend/services/advisory_model_first/model_binding_resolution.py`
- `scripts/advisory_publish_program_model_descriptor.py`
- `backend/services/advisory_model_first/meta_label_bundle.py`
- `backend/services/advisory_model_first/model_inference.py`
- `backend/services/advisory_forward/service.py`
- `backend/tests/advisory_model_first/test_dynamic_model_binding.py`
- 本设计文档与 `advisory_strategy_conditioned_model_blueprint_v1_20260710.md` 的当前下一步

## 3. Non-goals / 非目标与边界

- 不发布生产 descriptor，不修改 `AISTOCK_ADVISORY_MODEL_ROOT`，不重启后端。
- 不替换或重排 Selection 正式结果，不改变 Advisory review、持仓退出、止损止盈或每日 publication。
- 不把 P0-D 宣称为 champion，不增加收益门槛、人工审批或自动激活门禁。
- 不新增 DB DDL/DML、API/UI、通用 ModelOps、artifact registry、缓存平台或历史证据归档。
- 不重新训练 P0-D，不扫描其他 bundle，不以旧 M1/M5 模型静默替代元标签模型。
- P0-D 不承担 M3 outcome 或 price-range 子模型职责；这些子结果在该角色下返回明确的 typed unavailable。

## 4. Architecture / 架构

```text
frozen Program + binding + Selection run
                  |
                  v
descriptor resolver v1/v2 -- identity mismatch --> typed unavailable
          | legacy quality role
          |----------------------> existing M1/M5 exact loader/scorer
          |
          | meta-label role
          v
exact meta_label_bundles/<bundle_id>
  manifest + feature schema + fresh HMM + model.txt
          |
          v
Selection Top20 -> shared PIT feature builder -> take probability
          |
          v
entry_priority_rank Top5 challenger observation

Selection rank / exit rank / formal publication remain unchanged
```

分派只能由已哈希 descriptor 的 `model_role` 决定。每个角色有独立 loader、runtime validator 和 scorer；任何 schema、身份、文件 hash、特征顺序、HMM cutoff 或概率异常均 fail closed，不能跨角色回退。

## 5. Contracts / API、数据与运行契约

### 5.1 Descriptor v2

`advisory_program_model_binding_v2` 在 v1 身份字段基础上新增：

- `model_role`: 固定为 `meta_label_take_skip_confidence`。
- `shadow_policy_sha256`: 必须等于 bundle manifest 的冻结 shadow policy。
- `candidate_projection.terminal_weights`: key 必须与 `component_roles` 的两个 Alpha id 完全一致、值为正且和为 1。

descriptor 仍原子写入 `program_bindings/<program_id>/<binding_version_id>.json`，全体字段进入 `descriptor_sha256`。v1 descriptor 继续推导为 legacy quality role，不发生格式迁移。

### 5.2 Exact bundle

- bundle 路径由 model root、固定目录名与 64 位 `bundle_id` 组合，realpath 必须位于 model root 内。
- `manifest.json` 文件 SHA256 必须等于 descriptor 的 `bundle_manifest_sha256`。
- 继续执行现有 manifest 全文件 size/hash readback。
- manifest 必须精确匹配 program、binding、package manifest、style、feature schema、shadow policy、model role、experimental status 与 calibration state。
- `fresh_hmm_models.json` 所有模型必须声明同一 `continuation_cutoff`；空模型或多 cutoff 为 bundle invalid。

### 5.3 Candidate 与 rank

- 输入组固定为 persisted Selection 连续 Top20；不得使用正式荐股 Top5 作为模型输入。
- Alpha leg 分数和权重逐候选与 descriptor 冻结权重核对。
- `advisory_model_rank = entry_priority_rank`，`advisory_model_score = take_probability`。
- `selection_exit_rank = selection_effective_rank`，不得被模型 rank 覆盖。
- 返回 `take_probability / skip_probability / advisory_model_confidence / model_status / calibration_state`。

### 5.4 Forward 与失败语义

- forward publication 冻结 `model_role` 与 descriptor/bundle identity；baseline publication 不依赖 challenger 成功。
- meta-label 成功状态仍为 `EXPERIMENTAL_SHADOW`，便于复用现有 observation 存储与后续结算统计。
- M3 outcome、price range 在此角色返回各自 typed unavailable，不影响元标签候选排序成功状态。
- 不完整 Top20、未来时钟、bundle 损坏、HMM 续推失败、必需特征缺失或无效概率均返回明确 reason code，不回退 Selection 排序冒充模型结果。

### 5.5 Descriptor 安全切换与回滚

- 已存在 descriptor 的切换必须显式传入 `expected_current_descriptor_sha256`；发布器在同一跨进程独占锁内重新读取当前文件并执行 compare-and-swap。当前 descriptor 不存在、hash 无效或与预期不符时返回结构化冲突，目标文件保持不变。
- 新 descriptor 在进入锁前完成 schema、Program/binding、package、bundle 与 candidate projection 校验；切换使用同目录临时文件、文件 `fsync`、原子 `os.replace` 和替换后精确 bytes readback。
- 每次成功切换前，将当前 descriptor 的精确 bytes 写入 `program_bindings/<program_id>/.descriptor_history/<binding_version_id>/<descriptor_sha256>.json`。历史快照不可变；同 hash 不同 bytes、缺失快照或损坏快照均 fail closed。
- rollback 不是删除 descriptor。它必须显式传入当前 descriptor hash 与目标历史 hash，并复用同一 CAS、原子替换、readback 和反向快照逻辑，因此回滚后仍可恢复到回滚前版本。
- 首次发布继续兼容现有 CLI，且在同一独占锁下保持“相同 bytes 幂等、不同 bytes 拒绝覆盖”。切换/回滚只由显式 CLI 参数触发，不扫描 latest，不自动发布、不重启后端。
- 成功 receipt 必须返回 operation、目标路径、previous/current descriptor hash 和 rollback snapshot 路径；失败不得输出 `ok=true`。

## 6. Verification Plan / 验证方案

1. descriptor v1 回归、v2 原子发布、CAS 切换、不可变快照、精确回滚、并发冲突，以及身份/角色/权重/shadow policy fail-closed。
2. exact meta-label loader 的路径、manifest 文件 hash、全文件 readback、统一 HMM cutoff 验证。
3. 真实结构 fixture 下完成 Top20 特征到概率重排，断言 entry rank 改变而 exit rank 不变。
4. 断言 meta-label 路径不调用旧 quality loader/scorer，不触发 outcome/price-range loader。
5. 断言 baseline/forward 在模型 unavailable 时仍独立可用，成功时冻结 model role 与 bundle identity。
6. 运行定向 pytest、F2 validator、ownership/classifier/static 最小门禁，并执行至少两轮源码审查修复。

## 7. Risks / 风险与失败模式

| 风险 | 处理 |
|---|---|
| descriptor v2 误伤旧 M1/M5 | v1 保持原字段与推导角色，增加回归测试 |
| 元标签误用正式 Top5 | runtime 明确要求连续 Top20，与训练 candidate group 一致 |
| 模型 rank 改写退出逻辑 | 输出单独 `selection_exit_rank` 并测试不变量 |
| HMM 使用未来状态 | bundle cutoff 后仅按交易日顺序续推；decision date 必须晚于 cutoff |
| bundle 角色串线 | descriptor model role、bundle schema/model role 双重一致性校验 |
| outcome 子模型错误绑定 | meta-label 角色不加载 legacy child bundle，返回 typed unavailable |
| 模型效果被夸大 | 保留 experimental/uncalibrated；不自动激活、不替换 baseline |
| 并发发布导致 lost update | Program/binding 粒度跨进程独占锁与 expected-current hash CAS；冲突时目标不变 |
| 覆盖旧 descriptor 后无法回滚 | 切换前保存内容寻址不可变快照；rollback 复用同一 CAS 与 readback，不以删除文件降级 |
| 替换后磁盘 readback 异常 | 在锁内恢复刚保存的旧 bytes 并返回明确失败；禁止把未核验写入报告为成功 |

## 8. Rollout / Rollback

代码合入后仍不产生运行时变化。后续 rollout 必须由用户分别确认：

1. 将已验证 bundle 放在配置 model root 的精确目录（现有资产满足时只读核验）。
2. 用户在源码合入后重启后端，并核对 source/runtime SHA；源码激活与 descriptor 激活分开报告。
3. 用户单独授权后，使用 expected-current hash 安全发布精确 v2 descriptor，并核对 receipt/readback。resolver 每次推理读取 exact descriptor，因此切换会在下一次荐股调用生效，不以第二次后端重启作为生效门禁。
4. 立即执行只读业务 readback，并先观察 forward challenger，不改变 baseline publication。

rollback 使用发布时产生的精确不可变快照和当前 descriptor hash 执行 CAS 恢复；恢复在下一次荐股调用生效，不要求额外重启。禁止删除 descriptor 作为正常回滚。无需 DB 回滚，也不修改 Selection、Program binding 或模型资产。

## 9. Production Gates / 生产门禁

| 动作 | 本任务状态 | 独立授权 |
|---|---|---|
| 源码、测试、PR | 自动执行 | 已授权 |
| 合入 PR | 等待 | 用户确认 |
| 发布/替换运行时 descriptor | 禁止自动执行 | 用户确认 |
| 源码合入后的后端重启 | 禁止自动执行 | 用户执行；不与 descriptor 激活合并报告 |
| DB DDL/DML | 不需要 | 不适用 |
| 替换正式荐股排序 | 不允许 | 新设计与新授权 |

## 10. Design Acceptance Index

| ID | requirement |
|---|---|
| F-701 | descriptor v2 精确声明 meta-label role、bundle、Selection semantics、shadow policy、component roles 与权重 |
| F-702 | exact loader 校验路径、manifest 文件 hash、全文件 readback、bundle identity 与单一 HMM cutoff |
| F-703 | runtime 只对 persisted Selection 连续 Top20 使用共享 PIT 特征和冻结 HMM 续推 |
| F-704 | take probability 决定 entry priority，selection exit rank 保持不变，状态固定 experimental/uncalibrated |
| F-705 | v1 M1/M5 路径兼容；不同 model role 不交叉 loader/scorer 或 silent fallback |
| F-706 | forward 冻结 model role/descriptor/bundle，baseline 独立；不适用 child model 返回 typed unavailable |
| F-707 | 无 DB/API/UI/正式策略变更、无自动 descriptor 发布/激活/重启、无额外平台工程 |
| F-708 | F2 validator、定向测试与重复源码审核全部通过后才满足合入条件 |
| F-709 | 已存在 descriptor 仅能在跨进程锁内以 expected-current hash CAS 切换；冲突不改文件且返回结构化失败 |
| F-710 | 切换前保存内容寻址不可变快照；rollback 复用 CAS、原子替换、替换后 readback 并保留反向恢复能力 |
| F-711 | 现有首次发布 CLI 保持兼容；切换/回滚必须显式参数触发并返回可核验 receipt；descriptor 在下一次推理生效但不自动执行、不控制后端进程 |

## 11. Implementation Plan / 实施方案

1. 扩展 descriptor resolver/publisher，兼容 v1 并增加严格 v2 契约、跨进程锁、CAS 切换、不可变快照与回滚。
2. 增加 exact meta-label runtime loader 与运行时 bundle 校验。
3. 在 shadow service 按 model role 分派 candidate frame、特征、scorer 和 child 状态。
4. 扩展 forward frozen resolution；补齐 descriptor、loader、inference、forward 测试。
5. 执行 validator、定向测试、静态门禁和多轮审核修复；创建 PR 后等待用户合入。

## 12. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-701 | `model_binding_resolution.py` | `backend/tests/advisory_model_first/test_dynamic_model_binding.py` | pass | none |
| F-702 | `meta_label_bundle.py` | `backend/tests/advisory_model_first/test_meta_label_bundle.py` | pass | none |
| F-703 | `model_inference.py` | `backend/tests/advisory_model_first/test_meta_label_runtime_inference.py` | pass | none |
| F-704 | `model_inference.py` | `backend/tests/advisory_model_first/test_meta_label_runtime_inference.py` | pass | none |
| F-705 | `model_binding_resolution.py`, `model_inference.py` | `backend/tests/advisory_model_first/test_dynamic_model_binding.py`; `backend/tests/advisory_model_first/test_model_inference.py` | pass | none |
| F-706 | `advisory_forward/service.py` | `backend/tests/advisory_model_first/test_forward_publication.py` | pass | none |
| F-707 | changed-file boundary | `backend/tests/advisory_model_first/test_meta_label_runtime_inference.py`; `python scripts/aistock_feature_workflow.py validate --design docs/architecture/advisory_p0d_challenger_runtime_f2_design_20260819.md --tier F2` | pass | none |
| F-708 | source and workflow gates | `python -m nox -s advisory_modeling_backend`; `python -m nox -s l0`; `python -m nox -s platform_api_backend` | pass | none |
| F-709 | `backend/services/advisory_model_first/model_binding_resolution.py` | `backend/tests/advisory_model_first/test_dynamic_model_binding.py`; `rtk pytest backend/tests/advisory_model_first/test_dynamic_model_binding.py -q` | pass | none |
| F-710 | `backend/services/advisory_model_first/model_binding_resolution.py` | `backend/tests/advisory_model_first/test_dynamic_model_binding.py`; exact rotate/rollback/reverse-snapshot/corrupt-snapshot tests | pass | none |
| F-711 | `scripts/advisory_publish_program_model_descriptor.py` | `backend/tests/advisory_model_first/test_dynamic_model_binding.py`; CLI compatibility/receipt/conflict/two-process tests | pass | none |

## 13. DESIGN-COMPLIANCE-001

1. 无简化版：真实 exact bundle、真实共享 PIT 特征、真实 fresh HMM 续推和真实 LightGBM scorer 均进入运行路径。
2. 无静默错误：角色、身份、文件、时钟、候选组、特征和概率全部 fail closed；禁止跨角色回退。
3. 无业务偏移：只改变 challenger 新入场优先级，Selection 正式排序和退出 rank 不变。
4. 无额外门禁：experimental 结论不升级为审批、收益门槛或自动激活；源码合入、源码重启和已有 descriptor 激活动作仍按原授权边界分别由用户确认。

审核记录：

- Design Round 1：将范围从“写 descriptor”修正为 role-aware exact loader/scorer 闭环，避免元标签 bundle 被旧 M1/M5 loader 误读。
- Source Round 1：补齐 v1/v2 兼容、Top20 固定候选组、dual-rank 输出和 child model typed unavailable。
- Source Round 2：未知 descriptor schema 改为配置无效；训练词表外行业编码同步置位 missing indicator，禁止静默 NaN。
- Source Round 3：补齐 Selection 必须恰好为连续 Top20、feature schema/version/vocabulary 精确一致，以及 rank/clock/confidence 输出不变量；19 只候选或小数 rank 均 fail closed。
- Source Round 4：补齐 expected-current hash CAS、跨进程锁、替换后 readback、不可变历史快照、精确 rollback、反向快照及兼容 CLI receipt；竞争进程只能一个成功。
- Source Round 5：修正损坏 rollback 快照的错误归因，限制 history/lock realpath 不得逃逸，并验证冲突、缺失和损坏路径均不修改当前 descriptor。
- Runtime Semantics Round：确认 resolver 每次推理读取 exact descriptor；将 rollout 修正为源码重启后单独授权切换，切换和 rollback 均在下一次荐股调用生效，不虚构第二次重启门禁。
- Real bundle smoke：后端同款 `AIstock` 环境加载 `e555903e...`，核对 103 个模型特征、107 个 HMM 模型、统一 cutoff `2026-02-02`，真实 LightGBM 输出 take `0.5535642729` / skip `0.4464357271`。
