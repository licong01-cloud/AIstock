# QE 默认窗口动态 HMM Runtime 支持验证

## Scope

- 模块：HMM / QE ConfigComposer
- 目标：让 NEW1/NEW2 动态 HMM 在 QE 默认窗口 `test_start=2024-07-01`, `test_end=2026-03-10`, `backtest_end=2026-03-03` 下可直接解析系数文件。
- 未执行：未启动正式 QE 实验，未运行 RD-Agent 训练/回测。

## Changes Validated

- `ConfigComposer` 动态 HMM fallback 现在可接收/解析 `hmm_config_json`，并传给 `scripts/precompute_hmm_coefficients.py`。
- QE create/evolution/config builder 路径会携带 HMM DB `config_json`，避免动态 HMM 文件未命中时缺少 `method/horizon_weights/coefficient_lambda/coefficient_bounds/confidence_scale`。
- 修复 WSL conda activate chain 在 `wsl bash -c` 中使用临时 shell 变量导致空展开的问题。
- 为 NEW1/NEW2 生成 QE 默认窗口 `preset_A` 系数文件。
- DB `model_train_configs.config_json` 已标记 `qe_default_supported=true` 并记录两个 coefficient windows。

## Generated Artifacts

- `backend/data/hmm_models/442fd70a-47b5-41ca-b4f5-96f52b81742e/2026-04-29/coefficients_preset_A_2024-07-01_2026-03-03.json`
- `backend/data/hmm_models/f3fe9433-ea86-4a16-a44b-989e1398c1b2/2026-04-29/coefficients_preset_A_2024-07-01_2026-03-03.json`

## Commands

```powershell
python -m py_compile backend/services/quantevolver/config_composer.py backend/services/quantevolver/experiment_config.py backend/services/quantevolver/experiment_config_builders.py backend/routers/quantevolver.py backend/routers/quantevolver_evolution.py backend/tests/unified_engine/test_multi_alpha_command_generation.py
python -m pytest backend/tests/unified_engine/test_qe_config_truth.py backend/tests/unified_engine/test_multi_alpha_command_generation.py -q
```

## Results

```text
py_compile: passed
pytest: 72 passed in 11.75s
DEFAULT_SPLIT 2024-07-01 2026-03-10 2026-03-03
HIT HMM_DYNAMIC_PUP_w20_50_conf_0p075_PIT1Y__n3_diag qe_default_supported True windows 2 days 404 2024-07-01 2026-03-03 stock_map 5815
HIT HMM_DYNAMIC_PUP_w20_50_conf_0p10_PIT1Y__n3_diag qe_default_supported True windows 2 days 404 2024-07-01 2026-03-03 stock_map 5815
```

## Residual Risks

- NEW1/NEW2 的原始训练/验证切分仍是 PIT1Y 版本定义；QE 默认窗口前半段若用于正式无泄漏结论，需要另训 default-window strict-embargo 版本。
- `preset_B` 仍未为 NEW1/NEW2 提供；QE 应选择 `preset_A`。
- 未启动正式 QE 实验，本次验证覆盖到 HMM 系数解析/预计算/默认窗口命中层。
