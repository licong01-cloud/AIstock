# Alpha158 catalog seeding and custom evolution toggle

- Module: qe
- Level: L2
- Date: 2026-04-30T00:26:51
- Git commit: 629364d
- Operator: lc999

## Scope

- Changed files: `scripts/seed_alpha158_factor_catalog.py`, `backend/routers/quantevolver_evolution.py`, `backend/services/quantevolver/experiment_config_builders.py`, `backend/services/quantevolver/qe_evolution_service.py`, `frontend/src/app/quantevolver/evolution/page.tsx`, `rdagent_assets/alpha158_factors/*.py`, `rdagent_assets/qe_factors/{RESI5,WVMA5,RSQR5,KLEN,RSQR10,CORR5,CORD5,CORR10,ROC60,RESI10,VSTD5,RSQR60,CORR60,WVMA60,STD5,RSQR20,CORD60,CORD10,CORR20,KLOW}.py`
- Impacted flows: QE 因子库注册、官方指标/分类/相关性后续 UI 计算入口、QE 自定义演进 custom_evo per-loop Alpha158 基线开关、backtest-only 一致性校验。
- Business goal: 20 个 QE 当前 Alpha158 基线因子以独立因子身份入库；不预计算因子值/官方指标；自定义演进每个 loop 可独立启用/禁用原始 20 Alpha158 因子集。
- Out of scope: 因子值计算、4 窗口官方指标计算、正式 LLM 分类评级、相关性计算、真实 QE 训练/回测提交、浏览器 E2E 点击。
- Protected assets reviewed: 未修改既有 QE/RD-Agent 实验 workspace、模型权重、StrategyPackage manifest、HMM snapshot、paper ledger；仅按用户要求新增 Alpha158 因子源/执行代码文件和 DB catalog/meta/classification stub。

## Environment

- Backend port: 未启动/未重启 8001；仅本地 Python/DB 校验。
- Frontend port: 未启动；执行 Next production build。
- TDX port: 未使用。
- Conda/env: 当前 `python`/Node/npm 环境。
- Database: 本地 PostgreSQL/TimescaleDB，连接信息来自 `.env`。
- Browser/headless: 未运行 Playwright。

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | Python 语法、生成因子代码、TS 类型、Next build 通过 | `py_compile` 通过；`npx tsc --noEmit --pretty false` 通过；`npm run build` 通过 | PASS |
| Backend config path | `disable_alpha158=True` 进入 `ExperimentConfig.build_custom_params()`；默认 False 不改变旧 custom_params | Python snippet: `builder_alpha158_toggle_ok` | PASS |
| API schema/guard | custom loop schema 接受 `disable_alpha158`；禁止放在 nested `strategy_params` | Python snippets: `custom_evo_schema_disable_alpha158_ok`, `nested_disable_alpha158_rejected_ok` | PASS |
| DB seeding | 20 个 Alpha158 因子 catalog/meta/classification stub 完整，eligible，且未计算 metrics | `catalog_count=20`, `meta_count=20`, `classification_count=20`, `metrics_count=0`, `eligible_count=20` | PASS |
| Generated factor files | 20 个 `rdagent_assets/qe_factors/*.py` 存在且可编译 | `python -m py_compile rdagent_assets/qe_factors/...` 通过；`missing_qe_files=[]` | PASS |
| Existing targeted pytest | 非 HMM 的 custom_evo 最小配置兼容 | `2 passed in 0.41s` | PASS |
| Wider pytest smoke | 现有 HMM 相关测试不应因本变更新增回归 | 16 failures all at `services.hmm_training_service` relative import beyond top-level package | BLOCKED / pre-existing dirty-worktree risk |

## Commands

```bash
python -m py_compile backend/routers/quantevolver_evolution.py backend/services/quantevolver/experiment_config_builders.py backend/services/quantevolver/qe_evolution_service.py scripts/seed_alpha158_factor_catalog.py
python scripts/seed_alpha158_factor_catalog.py
python -m py_compile rdagent_assets/qe_factors/RESI5.py rdagent_assets/qe_factors/WVMA5.py rdagent_assets/qe_factors/RSQR5.py rdagent_assets/qe_factors/KLEN.py rdagent_assets/qe_factors/RSQR10.py rdagent_assets/qe_factors/CORR5.py rdagent_assets/qe_factors/CORD5.py rdagent_assets/qe_factors/CORR10.py rdagent_assets/qe_factors/ROC60.py rdagent_assets/qe_factors/RESI10.py rdagent_assets/qe_factors/VSTD5.py rdagent_assets/qe_factors/RSQR60.py rdagent_assets/qe_factors/CORR60.py rdagent_assets/qe_factors/WVMA60.py rdagent_assets/qe_factors/STD5.py rdagent_assets/qe_factors/RSQR20.py rdagent_assets/qe_factors/CORD60.py rdagent_assets/qe_factors/CORD10.py rdagent_assets/qe_factors/CORR20.py rdagent_assets/qe_factors/KLOW.py
python -m pytest backend/tests/unified_engine/test_experiment_config.py::TestBuildConfigFromCustomEvoLoop::test_minimal backend/tests/unified_engine/test_ab_comparison.py::TestABComparisonPath4::test_minimal_config_custom_params_identical -q
python -m pytest backend/tests/unified_engine/test_ab_comparison.py backend/tests/unified_engine/test_experiment_config.py -q
npx tsc --noEmit --pretty false
npm run build
```

## Evidence

- API calls: 未调用运行中 API；用 Pydantic schema snippet 验证 request field。
- DB checks: `catalog_by_source=[('alpha158', 20, 20, 20, 20)]`; `alpha158_meta_count=20`; `classification_count=20`; `metrics_count=0`; `eligible_count=20`。
- Log files: 无服务日志；本记录为本次验证证据。
- Playwright report/trace: 未运行。
- Screenshots: 未生成。
- Business output summary: UI 可配置每个 custom_evo loop 是否启用原始 Alpha158 20 因子集；后端将禁用态写入 custom_params 并在 backtest-only 模式校验源模型 Alpha158 设置一致；20 个 Alpha158 因子已可在因子库后续计算官方指标/分类/相关性。

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| 首次 seed DB 失败：`value too long for type character varying(20)` | classification stub 的 `factor_dimension='alpha158_price_volume'` 长度超过本地 schema 限制 | 改为 `factor_dimension='price_volume'` | 重新运行 seed 输出 20/20/20 成功 |
| `python -m pytest backend/tests/unified_engine/test_ab_comparison.py backend/tests/unified_engine/test_experiment_config.py -q` 有 16 个失败 | 当前脏工作区已有 HMM config 改动导致 `_resolve_hmm_config_json()` 导入 `services.hmm_training_service` 时出现 `attempted relative import beyond top-level package`；失败集中于 HMM 相关用例 | 本任务未修改该 HMM 路径；改跑与本次 Alpha158/custom_evo 相关且不触发 HMM 的最小测试 | `2 passed in 0.41s`; `py_compile`、builder/schema snippets、TS/Next build 通过 |

## Result

- Final status: PASS with noted HMM-test blocker unrelated to this task.
- Remaining risks: 未做浏览器点击 E2E；未实际提交 custom_evo 任务到 compute node；未计算 Alpha158 因子值/官方指标/正式 LLM 评级/相关性（符合本次范围）。
- Need production backend restart: yes, for API/schema/service changes to take effect on 8001.
- Need dev service restart: frontend/backend running实例需要重启或重新构建后才能看到 UI/API 新字段。
