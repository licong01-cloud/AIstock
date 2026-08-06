# QE read-only workspace access regression

- Module: qe
- Level: L3
- Date: 2026-08-06T13:48:57（续作验证更新 2026-08-06 晚间）
- Git commit: cf829a0c（续作提交前工作树状态）
- Operator: lc999

## Scope

- Changed files: backend/services/quantevolver/config_composer.py、qe_dataset_contract.py、scripts/qe_build_frozen_risk_policy.py、scripts/qe_build_frozen_suspend_filter.py（新增）、scripts/export_suspend_d_candidate.py（新增）、scripts/qrun_limit.py、scripts/qrun_limit_minute.py、aistock_models/aistock_models/gats_industry_provider.py、backend/tests/unified_engine/test_qe_config_truth.py、test_qe_data_plane_zero_db.py、test_qe_frozen_suspend_filter.py（新增）、test_qrun_mlflow_metric_retry.py、两份设计文档、file_ownership.yaml、aistock_runtime_targets_v1.yaml
- Impacted flows: QE/多 Alpha 新装配（行业 provider、风险策略 artifact、停牌过滤 artifact）、qrun 预初始化冻结重建、工作区 payload 静态扫描
- Business goal: QE/多 Alpha 训练/预测/回测/组合计算数据面零数据库；解除 suspend 离线数据缺口导致的 fail-closed 阻断
- Out of scope: 生产数据 symlink 切换、后端重启、正式演进实验、DDL/DML
- Protected assets reviewed: 生产 qlib bin/H5/Parquet 未改；冻结候选目录只读引用；无进程控制

## Environment

- Backend port: 未触碰（8011/8012/8001 均未重启）
- Frontend port: 未触碰
- TDX port: 未触碰
- Conda/env: 本地 miniconda python（pytest 9.0.2）；WSL/node1 rdagent-gpu（pandas 2.2.3、pyarrow 21.0.0、qlib 0.9.6.99）
- Database: 导出阶段只读事务读 market.suspend_d / market.trading_calendar；运行态验证全程投毒 psycopg2（PYTHONPATH 注入即抛模块）
- Browser/headless: 不适用

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No high-risk path/secret/fallback/asset finding | nox -s validation_module_registry_l0 -> pass；nox -s guardrail_changed_files -> pass | PASS |
| Backend tests | QE zero-DB / config-truth / mlflow-retry / 新 suspend 构建器测试全绿 | pytest 4 文件 -> 163 passed, 38 skipped（torch/qlib 预存 importorskip）；新文件 18 passed | PASS |
| 真实 ST-PIT 跨度证明（非 fixture） | 风险策略 artifact 由真实冻结 all.txt 重建 | WSL 真实数据：span_count=5410, trade_dates=1917, status=frozen, fingerprint=94c9d82d… | PASS |
| suspend 边界矩阵 | 正常日/停牌日/零停牌已导出日/连续停牌/复牌/窗口边界/符号归一/BJ 剔除 | 单测 18 项 + 真实 artifact：4 个零停牌日（2022-11-29、2023-11-01、2023-11-27、2023-11-29）显式空列表；逐日计数=manifest 回执 | PASS |
| 投毒 DB 冒烟 | psycopg2 即抛环境下新 workspace 完成 qrun 预初始化装配 | WSL+node1 各一次：PYTHONPATH 投毒后两个构建器均成功产出 artifact | PASS |
| 双节点一致性 | 同候选数据集产出字节一致 artifact | WSL 与 node1 artifact SHA256 同为 0e3ed7490746a160d0f7f4977463d7ba7b6b302f127013fde4bb4a90dc4bb2cc | PASS |
| Asset safety | No protected asset modified silently | 生产 bin/H5/Parquet 未改；候选目录另行构建；git diff --check OK | PASS |

## Commands

```bash
python -m pytest backend/tests/unified_engine/test_qe_frozen_suspend_filter.py backend/tests/unified_engine/test_qe_config_truth.py backend/tests/unified_engine/test_qe_data_plane_zero_db.py backend/tests/unified_engine/test_qrun_mlflow_metric_retry.py -q
python -m nox -s validation_module_registry_l0
python -m nox -s guardrail_changed_files
python -m nox -s qe_read_backend
python -m nox -s qe_sector_risk_overlay_backend
python scripts/aistock_feature_workflow.py validate --design docs/architecture/qe_efficient_gats_l2_industry_embedding_f1_design_20260710.md --tier F1
# WSL/node1 真实数据+投毒 DB 冒烟：/tmp/bug989_smoke_ws_20260806、/tmp/bug989_smoke_ws_node1（rdagent-gpu env，PYTHONPATH 注入即抛 psycopg2）
```

## Evidence

- API calls: 无（纯离线/计算节点验证）
- DB checks: 导出阶段只读；运行态 psycopg2 投毒未触发（证明零导入）
- Log files: /tmp/bug989_regression2.log、/tmp/bug989_nox_*.log、/tmp/bug989_f1val.log
- Playwright report/trace: 不适用
- Screenshots: 不适用
- Business output summary: suspend artifact 1917 交易日键全覆盖、29804 行、4 零停牌日显式空列表；风险策略 artifact 5410 跨度；双节点字节一致

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| gate5 静态扫描误报 qe_build_frozen_suspend_filter.py 含 market.* SQL | 模块 docstring 中 "from market.suspend_d" 字样命中 MARKET_SQL_RE | 改写 docstring 措辞（不出现 FROM market. 模式） | 163 passed 复跑通过 |

## Result

- Final status: 验证全绿；suspend 离线数据缺口阻断解除（候选数据集 + 冻结装配链路）；等待 PR 更新与 CI
- Remaining risks: 生产 symlink 未切换（候选数据集需原子切换计划+授权）；完整 qrun 训练冒烟未跑（停止边界禁止正式实验）
- Need production backend restart: no
- Need dev service restart: no
