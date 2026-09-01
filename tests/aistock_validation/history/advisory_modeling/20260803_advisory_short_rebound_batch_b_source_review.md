# Advisory SHORT_REBOUND Batch B 源码复审记录

## 范围

- 任务：Phase 2/3 SHORT_REBOUND 重排模型 Batch B，多年 base snapshot、PIT 特征、训练导出与 2/3/5 年 view。
- 工作树：`F:\Dev\AIstock_worktrees\advisory-phase2-batch-b-dataset-20260803`。
- 隔离边界：未修改 Selection、Paper v2、模拟盘、QE consumer；未启动 WSL 训练；未执行 DDL/DML、服务启停或 runtime activation。
- 真实数据库动作：仅对 `.env` 指向的数据源执行单个历史交易日的只读 source smoke；临时 spool 位于系统临时目录并已清理。

## 审核与修复

| 轮次 | 发现 | 修复 | 证据 |
|---|---|---|---|
| 1 | bridge 请求包含正式 R4 明确拒绝的 `CENSORED` maturity | 只请求可形成训练标签的 `COMPLETE`、`TERMINAL`；正常成熟标签继续接受 `outcome_event_status=NONE` | `test_historical_driver_uses_exact_program_and_full_outcome_bridge_scope` |
| 2 | 多 Alpha component set 按整段 JSON 排序，且未闭合缺腿与准入权重 | 固定按 `component_id` 排序；评分 key 与 admitted component 双向相等；逐腿权重与有限数校验 | `test_multi_alpha_leg_resolution_closes_order_membership_and_weight` |
| 3 | 行业/市场指标可能使用不同有效子集，但成员 hash 记录完整 universe | 每个横截面确定一个共同有效成员集；所有指标使用同一分母；hash 同时覆盖请求集合与有效集合 | `test_complete_builder_snapshot_and_training_export_are_deterministic` |
| 4 | feature snapshot 未读回 `feature_source_revisions.parquet` | 校验 source Parquet schema、非空 revision rows 和 revision set hash | `test_complete_builder_snapshot_and_training_export_are_deterministic` |
| 5 | 成熟标签的 `NONE/BARRIER/TERMINAL` 事件语义未进入训练文件 | 三个主 projection 必须有一致合法事件状态，并写入 `label_outcome_event_status` | 同上，断言真实 `NONE` 读回 |
| 6 | 相同 Batch B request 重试会以相同 operation key 携带新 row version，触发正式服务 scope conflict | 调用前查询同 batch durable operation；COMPLETED 精确复用，其他状态结构化停止，不重写 operation | `test_historical_driver_reuses_completed_durable_operations_on_exact_retry` |
| 7 | feature revision 把本次抽取时间当作历史 `available_at`，导致 PIT 声明不实并破坏 exact retry | 对齐共享 Historical Range 合同，显式使用 `RETROSPECTIVE_DB_CONTENT_HASH`，不冒充 formal availability event；revision 只由稳定内容和 cutoff 身份决定 | feature source contract tests；只读 source smoke |
| 8 | 480/720/1200 阈值按 eligible 日期而非 LambdaRank 可训练的 modelable 日期判断 | 三个窗口均按每 fold `MODELABLE` fit dates 判断，eligible/modelable 继续分别报告 | `materialize_training_export` 合同与模块测试 |
| 9 | market-state range SQL 直接读取当前 `stock_basic.list_status`，会把后来退市状态投射到早期日期 | 仅用逐日 `cal_date` 与 `list_date/delist_date` 派生 `LISTED/DELISTED/NOT_LISTED` | `test_feature_contracts_close_all_frozen_formulas_queries_and_regime_fields` |
| 10 | amount、turnover、moneyflow 的 5/20 日窗口会分别压缩缺失日，产生不同日期分母 | 以行情交易日为唯一窗口键；逐日对齐，缺日保留 null + missing flag，不跨日补值；横截面也要求当日价格/资金流 | `test_liquidity_and_moneyflow_windows_do_not_compress_missing_dates` |
| 11 | `selected_labels` 和 `outcome_source_evidence` 缺失会被误报为普通样本不足 | 两个 role 均为 base 必需角色；每个已选 label 与 source evidence 精确闭合，训练 label closure hash 同时覆盖 label 与 calculation evidence | `test_complete_builder_snapshot_and_training_export_are_deterministic` |

## 真实只读 source smoke

- 日期：`2026-07-31`。
- 冻结模板：8/8 成功。
- 关键行数：PIT universe 4,908；daily market 4,908；decision market/state 各 4,908；daily basic/moneyflow 4,903；suspend 6；industry membership 5,939。
- 数据库目标身份：全体 revision 只有一个 target hash。
- 该 smoke 只证明实际 schema、只读事务、PIT date join 和 spool 读回可用；不冒充多年 materialization、正式数据质量结论或 WSL 训练。

## DESIGN-COMPLIANCE-001

1. **禁止简化交付**：Batch B 源码包含完整 Existing Program 输入、SEALED base 验证、8 个冻结 source 模板、四阶段/多 Alpha/HMM/risk 特征、feature snapshot、成熟标签、单份 WSL 可读训练文件和 2/3/5 年 view；真实多年物化仍保持独立执行状态。
2. **禁止静默错误**：缺失 source/schema/component/stage/label、非终态 operation、PIT identity 冲突和样本不足均产生 typed error 或显式 coverage receipt；没有基线冒充模型结果。
3. **禁止业务语义偏移**：ranking group 使用共享 `stable_signal_semantics_hash`；主标签保持 5 日净超额收益/MFE/MAE；Top20、2/3/5 年窗口、五折 split 和 retrospective research scope 未改变。
4. **禁止私增门禁审批**：没有角色、人工审批、策略包二次准入或运行时 DDL 门禁。仓库 clean/source identity、显式 roots 和只读事务属于本次物化输入与安全边界，不改变荐股业务准入。

## 状态边界

- 源码与本地合同：`python -m nox -s advisory_modeling_backend` 为 34 passed；F2 validator 为 24/24、0 warnings；ownership 21/21 mapped、0 ambiguous；classifier 为 `targeted_ci_required`、0 unmapped；guardrail 0 finding；validation catalog 6 passed、0 finding。
- DEV/生产多年 Historical Range、Phase 1R DML 与 artifact 物化：未执行。
- WSL LightGBM 训练、模型 bundle、shadow inference：不属于 Batch B，未执行。
- PR、合入、close-sync、生产运行时：均保持独立状态。
