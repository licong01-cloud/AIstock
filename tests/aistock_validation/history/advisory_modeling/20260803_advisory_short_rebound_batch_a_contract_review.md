# Advisory SHORT_REBOUND Batch A 合同实现审核记录

> 日期：2026-08-03
> 范围：`backend/services/advisory_modeling` 静态合同与纯函数、不可变工件读回、定向测试和 Validation catalog
> 明确未执行：数据库读取或写入、DDL/DML、WSL 训练、服务控制、runtime activation、真实模型推理、代码合入

## 1. 交付边界

本轮只完成详细设计 Batch A。Batch B 多年 snapshot/feature materialization、Batch C WSL 训练、Batch D 影子推理运行均未开始，不能从本记录推导整个 Phase 2/3 已完成。

## 2. 审核与修复

### 第一轮

1. `SplitPlanV1` 缺少日历位置和 label-as-of。修复为逐 fold 保存精确日期集合、日历位置、上海收盘换算后的 UTC cutoff、窗口边界和 identity hash。
2. bundle 将制品完整状态与模型能力状态混在一个字段。拆分为固定 `RESEARCH_BUNDLE_COMPLETE` 与 `MODEL_UNAVAILABLE/RERANK_READY`，可准确表达 `MODEL_NO_FORMAL_OOS`。
3. feature source revision hash 可由调用方任意填写。改为 canonical payload 自动计算并拒绝冲突值。
4. 纯公式核被命名为完整 feature builder。更名为 `ShortReboundFeatureFormulaKernelV1`，避免冒充 Batch B 数据构建能力。

### 第二轮

1. 60 个 eligible test 日期被错误要求在交易日历上连续。改为保存 60 个精确 calendar positions，允许合法稀疏 eligible 日期。
2. 2/3/5 年历史不足时会截短窗口却标为完整。新增 typed coverage 状态，保留准确 fold 报告但不声明可训练。
3. 模型选择可只提交 12 个预登记候选的子集。改为要求完整候选结果集合，失败候选也必须有显式结果，禁止静默缩小搜索空间。
4. query template 保存字符串而非精确 SQL bytes。改为 canonical base64 + raw SHA-256，并冻结参数、结果 schema 和 repository commit。
5. required feature family 被额外禁止重叠。删除该非设计限制，仅要求每个 family 内唯一且引用已声明特征。
6. 专用 feature snapshot/model bundle reader 未解析 manifest。补充严格 manifest readback、identity 和 file hash 闭合。

### 第三轮

1. `fold_training_as_of` 使用 UTC 日末，扩大了可用性窗口。改为前一交易日上海 15:00 收盘并转换为 UTC。
2. fitted market regime 的零方差输入会阻断合同构造。允许保存零方差统计，分类时明确返回 `UNAVAILABLE`。
3. artifact root 在解析前未检查原始路径链。新增逐级 symlink/reparse 检查，继续要求 root 已存在、绝对、repo-external、非 WSL。
4. 多 Alpha 公式核接受非正权重。按既有 StrategyPackage `component_weight > 0` 合同拒绝，并以正权重和归一化。
5. Validation catalog 缺少新 nox command allowlist。补充 `plan_catalog.py` 映射后 catalog integrity 重新通过。

### 第四轮

1. experiment result 只能记录 0 个或全部 3 个 seed，无法保存部分失败事实。改为允许注册 seed 的 canonical 有序子集，并要求所有不完整结果携带 reason code。
2. experiment registry/selection 错误使用裸 `ValueError`。改为 `MODEL_EXPERIMENT_REGISTRY_MISMATCH` 或 `MODEL_SELECTION_NOT_UNIQUE` typed failure。
3. 不可变目录发布只使用普通 rename。改为 Windows `MoveFileExW + WRITE_THROUGH`、POSIX create-if-absent rename + parent fsync，并保留并发 exact readback。
4. SQL bytes 未验证 UTF-8。补充解码验证，避免 hash 正确但模板不可执行。
5. 首次 Win32 ctypes 签名误用了 Python 类型，定向测试立即暴露 4 个发布失败；修正为 `c_wchar_p/c_uint32/c_int` 后全部复验通过，未保留失败路径。

### 第五轮

1. 最终 `SplitPlanV1` 可缺少 observation/label/member closure hash。新增五个 fold 的显式 evidence closure 输入；仅外层 split 本身不足时可生成不含 fold 的准确 coverage report。
2. label 结果缺少 `(decision_as_of_trade_date, target_trade_date, canonical_signal_scope_hash, label_policy_hash)` 排序组身份。新增 canonical `RankingGroupIdentityV1` 并强制与 active label policy 一致。
3. dataset request 缺少 `multi_alpha_parent_contract_version`。补入请求 semantic hash，避免父包合同版本分叉。
4. primary fold count 与 `best_iteration` 数量可矛盾。改为逐项一致，最多五个；primary seed 未完成时禁止伪造 primary fold evidence。

## 3. DESIGN-COMPLIANCE-001

| 检查项 | 结论 | 证据 |
|---|---|---|
| 无简化版/子集冒充 | PASS | 公式 registry 保存完整 payload；Batch B/C/D 均明确未完成，纯公式核不命名为完整 builder |
| 无静默错误 | PASS | coverage、typed reasons、hash/readback、零方差和 exact retry 均显式返回状态或异常 |
| 无业务语义偏移 | PASS | label、split、experiment、selection、bundle 和 shadow tie 逐项按 F2 设计冻结 |
| 无未经确认门禁/审批 | PASS | 无角色、审批、人工 ACK、策略包二次准入、DDL/DML 或 runtime gate |

## 4. 验证结果

- `python -m nox -s advisory_modeling_backend`：24 passed。
- `python -m nox -s validation_catalog_integrity`：6 passed，catalog findings 0。
- `python -m pytest backend/tests/test_validation_module_ownership.py -q -p no:cacheprovider`：8 passed。
- `python scripts/aistock_module_ownership_scan.py --changed-only --include-untracked --fail-on-unmapped --fail-on-ambiguous`：变更登记前 21/21 mapped，0 unmapped，0 ambiguous；最终文件集合在合入前重新执行。
