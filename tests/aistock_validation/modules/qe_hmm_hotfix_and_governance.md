# QE/HMM 热修复与治理测试矩阵

日期：2026-05-08
状态：设计期测试矩阵
关联设计：`docs/architecture/qe_hmm_hotfix_and_governance_detailed_design_20260508.md`

## 1. 覆盖目标

本测试矩阵覆盖两个近期 QE/HMM 整改项：

1. backtest-only 并行模式必须隔离 recorder，不允许 target symlink 写入 source `mlruns`。
2. 新建容量参数化 ScoreWeighted V2 策略资产，旧策略保持 legacy 5M cap，新策略 DB/UI 可选。

同时，本矩阵要求后续 SOTA/StrategyPackage/模型库治理开发在设计期同步测试，支持自动化流水线验证。

## 2. L0 静态与规范检查

| 编号 | 检查项 | 期望 |
| --- | --- | --- |
| L0-001 | `git status --short --branch` | 在独立 feature branch/worktree，不在 dirty `main` |
| L0-002 | `git diff --check` | 无 whitespace/error |
| L0-003 | `python scripts/aistock_guardrail_scan.py --changed-only --fail-on-severity P1` | 无新增 P0/P1 违规 |
| L0-004 | 搜索 `os.symlink` / `New-Item -ItemType SymbolicLink` / `ln -s` 涉及 `mlruns` | backtest-only target 不再 symlink 到 source `mlruns` |
| L0-005 | 搜索 `score_weighted_strategy_v2.py` diff | 旧策略文件未被直接改行为 |
| L0-006 | 搜索 DB migration | 新表/新字段均有 `COMMENT ON TABLE/COLUMN` |
| L0-007 | 搜索生产端口操作 | 无 kill/restart/reload `8001` |

## 3. Recorder Isolation L1 单元测试

| 编号 | 测试名建议 | 场景 | 期望 |
| --- | --- | --- | --- |
| RI-L1-001 | `test_backtest_only_target_mlruns_rejects_symlink` | target `LoopX/mlruns` 是 symlink | fail-fast，错误码 `QE_BACKTEST_TARGET_MLRUNS_IS_SYMLINK` |
| RI-L1-002 | `test_backtest_only_rejects_same_source_target_realpath` | source/target realpath 相同 | fail-fast，错误码 `QE_BACKTEST_SOURCE_TARGET_REALPATH_COLLISION` |
| RI-L1-003 | `test_backtest_only_rejects_target_under_source_mlruns` | target 目录位于 source `mlruns` 下 | fail-fast |
| RI-L1-004 | `test_backtest_only_uses_source_params_dir_not_source_mlruns_for_writes` | source params 单独目录，target mlruns 独立 | target recorder 写入 target；source 文件列表/mtime 不变 |
| RI-L1-005 | `test_qe_recorder_isolation_manifest_written` | 正常 backtest-only 启动 | 写入 `qe_recorder_isolation.json`，status=`passed` |
| RI-L1-006 | `test_qe_current_recorder_points_to_target_recorder` | backtest-only 完成 | `qe_current_recorder.json` recorder_id 为 target recorder |
| RI-L1-007 | `test_malformed_metric_retry_requires_isolation_passed` | 模拟空 metric 文件 | 未隔离时不 retry，隔离通过后按显式策略 retry/失败 |

## 4. Recorder Isolation L2/L3 集成测试

| 编号 | 场景 | 步骤 | 期望 |
| --- | --- | --- | --- |
| RI-L2-001 | 两个 target loop 复用同一 source model | temp source params + target A/B 并行启动 runner mock | A/B target realpath 不同，source 不变 |
| RI-L2-002 | 旧式 symlink workspace 回归 | 构造 legacy symlink `mlruns` | runner 拒绝执行，不写 metrics |
| RI-L2-003 | cross-node payload | source `mlruns_params.tar.gz` 通过 API/payload 到 target | target 解压到 source_model，不创建 source symlink |
| RI-L3-001 | dev-port QE backtest-only smoke | 用非生产端口创建小样本 backtest-only 任务 | loop complete，metrics 非空，isolation manifest passed |
| RI-L3-002 | 并行 business oracle | 同一 source 启动 2-4 个 backtest-only loops | 无 malformed metric；每个 loop 独立 recorder |

## 5. Capacity Strategy L1 单元测试

| 编号 | 测试名建议 | 场景 | 期望 |
| --- | --- | --- | --- |
| CS-L1-001 | `test_capacity_strategy_has_new_strategy_id_and_file` | 加载新策略注册配置 | strategy_id=`score_weighted_topk_v2_capacity_v1`，source_file 新文件 |
| CS-L1-002 | `test_legacy_score_weighted_v2_defaults_unchanged` | 比较旧策略 default_config/source hash | 旧 `score_weighted_topk_v2` 不变 |
| CS-L1-003 | `test_capacity_strategy_defaults_include_max_single_order_value` | 读取新 default_config | `max_single_order_value=1000000000.0` |
| CS-L1-004 | `test_capacity_strategy_param_schema_exposes_capacity_fields` | 读取 param_schema | 暴露 `max_single_order_value/max_weight/max_position_ratio` |
| CS-L1-005 | `test_config_composer_accepts_capacity_strategy_params` | QE config composer 生成 conf | 参数进入 strategy kwargs/effective config |
| CS-L1-006 | `test_strategy_package_contract_accepts_capacity_strategy_id` | StrategyPackage runtime contract | 新 strategy_id 映射到 score_weighted_v2 family |
| CS-L1-007 | `test_legacy_manifest_missing_capacity_uses_5m_default` | 旧 StrategyPackage manifest | 旧包仍用 `5_000_000.0` default |
| CS-L1-008 | `test_new_manifest_uses_explicit_capacity_value` | 新 manifest 显式参数 | Paper target value 使用 manifest 参数 |

## 6. Capacity Strategy L2/L3 集成测试

| 编号 | 场景 | 步骤 | 期望 |
| --- | --- | --- | --- |
| CS-L2-001 | strategy catalog API | dev DB 或 mocked repository 查询新策略 | 新策略可见，旧策略仍可见 |
| CS-L2-002 | QE UI 参数渲染 | 选择新策略 | UI 显示并可编辑容量参数 |
| CS-L2-003 | QE config create smoke | 创建 custom loop 使用新策略 | DB requested/effective config 都含容量参数 |
| CS-L2-004 | capacity business oracle | 相同 pred/score 下比较旧/新策略 | 旧策略出现 5M clip，新策略主要受 `max_weight`/`max_position_ratio` 控制 |
| CS-L3-001 | HMM QE 小样本 | 新策略 + HMM snapshot 创建小样本回测 | capacity audit 可解释 final cash/holding |

## 7. SOTA/StrategyPackage/模型库治理测试要求

长期治理分支必须按阶段补齐：

| 阶段 | 必测项 |
| --- | --- |
| Phase 0 术语与 schema | SOTA candidate、promotion review、StrategyPackage lifecycle 状态枚举一致 |
| Phase 1 手工加入 SOTA | QE loop 不再自动加入正式 SOTA；按钮创建 review record |
| Phase 2 资产冻结 | 模型权重、因子 schema、feature order、training recipe、seed contract 复制并 hash；不依赖 QE workspace |
| Phase 3 原始配置复测 | Mode A 使用 frozen weights + original config；结果写 validation_run，不覆盖原始指标 |
| Phase 4 seed contract | 同 manifest + 同 master_seed 两次运行 NAV 差异 < 0.01bp，持仓 100% 一致；非确定性模型记录 nondeterministic flags |
| Phase 5 模型库 | model_template/spec/trial/artifact/lifecycle_event 四层记录完整，artifact 不因 QE 清理丢失 |
| Phase 6 runtime variant | variant 不改 frozen core；Selection/Paper run 记录 variant hash |
| Phase 7 rolling validation | 每个 rolling window 产生独立 model_version 和 seed/run evidence |

## 8. 必须保留的验证记录

每次实现完成后应在 `tests/aistock_validation/history/qe/` 或相应模块目录写入验证记录，至少包括：

```text
任务/分支/commit
修改文件
测试命令和结果
是否触碰生产 8001
是否触碰受保护资产
DB 写入范围
source/target mlruns realpath 摘要
新旧策略 source hash/default_config 对比
残留风险
```

## 9. 推荐本地命令模板

```powershell
# Python 编译
python -m py_compile scripts/qrun_limit.py scripts/qrun_limit_minute.py backend/services/quantevolver/config_composer.py backend/services/strategy_package/backtest_contract.py backend/services/strategy_package/runtime.py

# 后端定向测试
pytest backend/tests -q -p no:cacheprovider -k "backtest_only or recorder or score_weighted or capacity or strategy_package"

# 前端类型/构建，按实际项目脚本选择
cd frontend
npm run typecheck
npm run build
cd ..

# Guardrail
git diff --check
python scripts/aistock_guardrail_scan.py --changed-only --fail-on-severity P1
```

## 10. 人工确认 gate

以下动作不能由 agent 自行完成，必须用户确认：

- 重启或 reload 生产 `8001`。
- 对生产 DB 执行实际写入型资产注册。
- 删除或归档历史 QE workspace、source `mlruns`、模型权重、HMM snapshot。
- 将长期治理分支合入 `main`。
- 将新策略资产加入 Paper v2 可用列表或未来实盘候选列表。
