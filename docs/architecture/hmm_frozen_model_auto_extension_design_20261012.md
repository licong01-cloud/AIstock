# HMM 冻结模型自动延伸详细设计

日期：2026-10-12；tier=F2；用户已批准自动延伸方向，源码和运行态验收分离。

## 背景与终极目标

主线仍是申万二级行业的收益辅助、风险辅助及真实消费验证。数据更新不应要求每月改源码、迁移模型、改环境变量或重启后端。本包只解除已有模型的输入/系数更新耦合，不以资产更新冒充预测价值或 QE 验收。

## 范围与非目标

allowed_write_scope：

- `backend/services/hmm_data_source/coefficient_extension.py`
- `backend/services/hmm_training_service.py`
- `backend/routers/hmm_training.py`
- `backend/tests/hmm_data_source/test_coefficient_extension.py`
- 本详细设计。

不修改 QE、Selection、Advisory、模型算法、特征、参数、窗口、seed、阈值、数据候选、active profile、全局工作流或测试计划。不训练、不读取效果评估 tail、不启动实验。生产写入、启用策略和用户进程控制本次不执行。

## 架构与接口契约

1. 新增 `POST /api/v1/hmm-training/configs/{config_id}/coefficient-extension`，一次性登记固定 snapshot、preset、系数起点及 enabled；对应 GET 返回配置。这里的“订阅”是持久化应用配置，不是每月人工批准。
2. policy 钉住模型文件 SHA 及模型/参数配置摘要；排除运行资产字段但不排除模型参数。后续 data generation/cutoff 不写回训练身份。权重、参数或 preset 变化必须明确失败，不自动换模型/重训。
3. 复用现有 HMM APScheduler 的一分钟调度周期和 `model_train_daily_coefficient_jobs`。新模式 `frozen_window_extension_v1` 不走旧数据库行情规划，也不改变旧每日/滚动训练模式。
4. 每次新的任务重新读取共享 active profile 固定指针，解析并冻结 manifest、六个已有共享输入的相对路径/SHA、正式 `wsl2-5080` 节点、market-volume 定义及文件交易日历。worker 只运行现有 BUG-1510 file-only 生成器，不读取 market 数据库。源码无月度目录或版本常量；不靠修改进程环境变量记录或发现月度状态。
5. 同一 model/config/preset/input/window/generator-source 的幂等键只创建一次持久任务。config 行锁保护任务创建，job 的 PostgreSQL session advisory lock 防止重复 worker；进程退出释放锁，RUNNING 可恢复，COMPLETED/FAILED 不重跑。恢复可重复零-fit 推断，不能重新训练或把 FAILED 当成功。
6. 历史模式保持固定 `coefficient_start`，延伸到冻结日历最后可用日，不从新增日冷启动滤波。不改变已有预处理/因果滤波算法。
7. 一次生成覆盖窗口；对已登记、同 snapshot/preset 且在合法起点以后的历史窗口，以及新 profile 明示的默认 QE `test_start..backtest_end` 窗口，从真实输出中裁切并逐日期验证，登记新 input identity 的独立资产。这样原 QE 严格精确窗口查找无需修改；信号 cutoff 与组合 backtest_end 不混用。新完整窗口也登记，未知窗口不伪造。起点早于模型训练/校准截止的历史不能被自动改标成无泄漏，也不因数据延伸自动重训。
8. 注册到既有 config 的 `coefficient_windows`；每项携带真实 model/input identity、文件 SHA/bytes 和固定 filename。旧记录和旧文件保留，不只改旧 manifest 标签。生成器先写独立 staging；逐日期验证后用同目录 fsync + 不覆盖 hard-link 原子发布，读回后才事务追加登记，并检查 SQL RETURNING 的配置读回。异常不留下半写入的正式文件；已写入但未登记的文件不冒充完成。
9. 任务执行/发布前后检查 policy、模型、active identity；并发 profile 切换则保持明确失败，下一调度周期以新输入生成新任务。不用旧目录 fallback，不由 HMM 激活数据集。
10. QE Composer 仍只读已登记资产，不在 compose/retry/clone 时启动 WSL。消费前只校验相关身份/文件，生成时才核验实际六个输入，不能每分钟全量扫描数据。

## Design Acceptance Index

- F-001：模型与动态输入身份分离，固定快照/参数/preset，不自动重训。
- F-002：active profile 更新自动构造同源 frozen input，无月度硬编码或 fallback。
- F-003：固定起点、完整日历和已有窗口裁切；自然无报价保留完整 membership 和显式 no-overlay。
- F-004：不可变输出、SHA/readback、旧资产保护及原 QE 精确窗口登记兼容。
- F-005：复用持久任务、互斥、失败状态和重启恢复；不依赖环境变量记录过程状态。
- F-006：一次启用后自动调度，普通 QE 读路径不触发计算；服务启停仍归用户。

## 实施方案

一个业务包完成纯文件输入/输出合同、既有持久任务与登记接线、HMM 路由/调度和直接矩阵；不拆成独立小阶段。无需新表/DDL、复制数据或新增平台。滚动重训继续沿既有独立入口，本包不以模型年龄或自然行情增加重训门禁。

## 验证方案与可合入标准

定向 pytest 覆盖新 release、固定 model hash、非连续行业编号沿原 reader、完整日历、裁切和未知窗口、缺文件/漂移/非有限值、停发 no-overlay、重复任务/重启恢复、原资产不覆盖、策略停用/参数漂移、worker 失败/错误回读及原 Composer 读合同。运行一次相关 HMM data-source 小矩阵、Ruff、py_compile、fresh-process import、module registry L0、L0 和 F2 validator。至少两轮源码审核，发现问题只重跑受影响节点。CI/Nightly承担间接跨模块矩阵，不跑 fits/QE。

## 风险与失败模式

更新数据可改变历史事实，所以新数据任务重新以原固定起点推断并保留旧产物；不是宣称模型效果自动提升。新行业、特征不兼容、必要历史上下文不足保持 typed failure，由真实原因决定是否需要新训练；正常停发不等于 membership 消失。输入验证不能推导经济价值。

## 发布与回滚

源码合入后仅具备能力。用户加载新源码并通过已有 config API 一次启用指定 snapshot/preset；将来月度数据更新不再改环境变量/源码或重启。关闭 policy 只停止后续延伸，不删除资产或改变训练/实验结果。源合入、启用配置、任务执行、资产登记、QE smoke 和经济验证分别报告。

## Production gates

production_ddl_gate=noop；production_dml_gate=not_executed；DEV_transaction_validation=passed_and_rolled_back；dependency gates=noop；dataset_write=false；profile_activation=false；training=false；QE_experiment=false；process_control=false。新增任务登记和启用配置的生产动作待目标级授权；未执行这些动作不能报告当前 QE 已解锁。runtime_impact=backend、target=backend-main；此次源码加载由用户负责。一次加载后，正常数据更新不需要再重启。

## 本次验证及审核结果

- 相关正式计划 `python -m nox -s hmm_data_source_backend`：159 passed、1 skipped、6 deselected，coverage=77.14%。跳过项未写成通过；新增自动延伸单元矩阵通过。
- 显式既有 `aistock_dev` 上 `test_existing_dev_transaction_policy_job_and_registration_readback --run-integration`：1 passed；启用配置、幂等 job、重复窗口登记和完成状态均读回，事务回滚后测试 config/job 数量为0。无生产写入、无建库/DDL。
- `validation_module_registry_l0`：8 passed；本次实际5文件 ownership 全映射、无歧义；L0 blocking=0；Ruff、UTF-8/AST/py_compile、git diff --check通过。
- Windows Conda AIstock 与 WSL rdagent-gpu Python 3.10.19 fresh-process 导入通过，均加载本任务 worktree，未启动 scheduler/backend。
- 真实 profile 固定指针 `F:/Dev/RD-Agent-state/dataset_profiles/active_dataset_profile.json` 的只读计划通过：generation=`20260928-v15-unified-moneyflow1`、manifest=`2225e1ea28f099f4972b6a465e4aa093d3767592e2651484b79700586bf358fc`、6输入、正式 market-volume 定义。固定 snapshot=`ecee7c40-6764-49ad-bc0f-1c6c6b390504`，模型 SHA=`7223c32648792443020cbc02e9ded3102e81d30330640e623d4c2ef0ddf8f665`；当前配置校准截止2026-05-19，合法计划2026-05-20..2026-08-31，共73日。此项只在内存构造，未启用 policy、未推断/生成系数。
- 第一轮审核修复持久 plan 只比较 key 的不足和半写入输出风险；第二轮修复真实节点名、缺少 market-volume 定义和新默认组合末日窗口；第三轮复核固定权重/参数、停发语义、原 registry/文件保护、失败状态、SQL读回和模块边界，无阻断 finding。
- 源码可合入与生产激活分离：正式启用、首个真实自动任务、当前资产登记及 QE smoke 均尚未执行，不能据此声称预测价值改善或 QE 完整历史已支持。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| F-001 | backend/services/hmm_data_source/coefficient_extension.py | backend/tests/hmm_data_source/test_coefficient_extension.py::test_model_contract_drift_never_selects_or_trains_another_model | passed | none |
| F-002 | backend/services/hmm_data_source/coefficient_extension.py | backend/tests/hmm_data_source/test_coefficient_extension.py::test_monthly_release_extends_same_model_and_keeps_old_exact_windows；真实 profile 只读计划 | passed | none |
| F-003 | backend/services/hmm_data_source/coefficient_extension.py | backend/tests/hmm_data_source/test_coefficient_extension.py::test_crop_preserves_legal_unavailable_and_does_not_materialize_stock_day_panel | passed | none |
| F-004 | backend/services/hmm_training_service.py | backend/tests/hmm_data_source/test_coefficient_extension.py::test_immutable_publish_is_idempotent_and_matches_existing_qe_reader；DEV事务回滚读回 | passed | none |
| F-005 | backend/services/hmm_training_service.py | backend/tests/hmm_data_source/test_coefficient_extension.py::test_worker_lock_crash_recovery_and_failed_job_stop；DEV 幂等 job | passed | none |
| F-006 | backend/routers/hmm_training.py | backend/tests/hmm_data_source/test_coefficient_extension.py::test_scheduler_reconciles_new_release_without_reenrollment_or_retraining；Windows/WSL fresh import | passed | none |

## DESIGN-COMPLIANCE-001

不交付占位、空成功或假 READY；不吞掉输入/生成/登记异常；不改变模型、PIT、停发和消费语义；不引入模型年龄、资源、效果显著性或每月人工审批门禁。源码验收不代表实际启用、真实 QE smoke 或收益改善。
