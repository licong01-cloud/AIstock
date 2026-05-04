# HMM 模型训练当前状态与续接指南（2026-05-03）

本文是下次继续更新 HMM 模型时的入口文档，覆盖当前生产可选版本、已下架版本、训练/预计算脚本、归一化状态、QE 验证结果和下一步建议。当前结论基于本地 DB `aistock`、HMM registry、`qe_20260502_131502_9b54` 四个 loop 的 QE 结果，以及 2026-05-02 离线诊断产物。

## 当前结论

- 当前唯一确认对 QE 有正向收益的主线是 old covfix：`HMM_COVFIX_w3_raw_same_params__n3_diag_rw3_nozscore`。
- 最新 dynamic PUP strict 0.10 / 0.075 两个版本已验证收益不佳，已从 `sector_hmm` 可选列表下架，但保留历史 DB 记录和模型资产用于追溯。
- 已新增 3 个 QE shadow loop 候选（2026-05-02）和 5 个 old covfix 系数 remap 候选（2026-05-04）用于验证；验证后 QE 可选列表只保留 Loop2 old covfix 与 Loop10 penalty-only 两个版本，其余测试候选已软下架以避免 UI 混淆。
- 当前 old covfix 主线没有做传统 z-score 归一化；它使用相对化观测量（收益率、超额收益、成交量占比、涨停占比、资金流占比等），不是直接使用股票价格、行业指数点位或成交额绝对值。
- 原始日线/分钟线数据层不应归一化；HMM 训练输入层后续应系统验证 train-only z-score、winsor+zscore、robust zscore、板块横截面 rank/zscore 等版本。
- 仅修改 HMM registry / DB 记录 / HMM 模型资产时，生产 FastAPI 后端 `8001` 不需要重启；前端刷新页面即可重新读取可选列表。

## 2026-05-04 old covfix remap 候选注册

本次新增 5 个只读预计算候选，目的是在已验证最佳的 old covfix 基线上做系数强弱 ablation，而不是替换 HMM 训练逻辑。source coefficient 状态解释如下：

```text
source coeff < 1.0   fading
source coeff = 1.0   neutral
source coeff > 1.0   trending
```

新增 QE 可选项：

```text
Config ID                             Snapshot ID                            名称
------------------------------------  ------------------------------------  --------------------------------------------------------------
ce4952c1-4b0d-46a7-81f2-ae1d4a249555  6ea64754-003d-48d8-ad9e-d0e7857716c8  HMM_TEST_old_covfix_penalty_only_f096_b000__qe20260504
82a40d27-0e96-48a1-882a-4d182a58b931  377a8447-ee26-44a8-8ead-7338f525e0f2  HMM_TEST_old_covfix_boost_only_p105__qe20260504
22d53160-7195-4e69-86ec-76c19c615a69  5a8ce90e-50bb-4fbd-8cd8-e3b95c9dffa0  HMM_TEST_old_covfix_penalty094_boost103__qe20260504
ea0db9d3-69bf-489e-aa55-c74b6340e68d  afa6acd9-f766-4394-970e-451d1a39bb06  HMM_TEST_old_covfix_penalty095_boost104__qe20260504
518ddf2d-e4a0-4bf0-8572-7cea429e27d5  8ddb5d29-8097-4aef-b110-f2f94f54ca4b  HMM_TEST_old_covfix_penalty095_boost106__qe20260504
```

系数映射：

```text
名称后缀                         fading  neutral  trending  用途
-------------------------------  ------  -------  --------  ----------------------------------------------
penalty_only_f096_b000           0.96    1.00     1.00      保留风险惩罚，去掉趋势增强
boost_only_p105                  1.00    1.00     1.05      保留趋势增强，去掉风险惩罚
penalty094_boost103              0.94    1.00     1.03      强惩罚 + 软增强
penalty095_boost104              0.95    1.00     1.04      中惩罚 + 中增强
penalty095_boost106              0.95    1.00     1.06      中惩罚 + 偏强增强
```

验证状态：

- 2026-05-04 10:25 后，`model_train_configs.model_type='sector_hmm'` 只保留 2 个 QE 可选 HMM：Loop2 old covfix baseline 与 Loop10 penalty-only best。
- 其余 7 个测试候选已软下架到 `sector_hmm_disabled_superseded_by_loop2_loop10_20260504`，仅保留历史 DB 记录、snapshot 和模型资产。
- 5 个新增 snapshot 均为 `completed`，`sector_count=131`，每个都有 `models.json` 和 `coefficients_preset_A_2024-07-01_2026-04-27.json`。
- 5 个新增 coefficient artifact 都覆盖 442 个交易日、首尾日 131 个行业、`stock_sector_map=5847`。
- `ConfigComposer._resolve_hmm_coefficients_json` 已对 5 个 snapshot 做 fail-fast 本地解析验证，均能命中预计算 artifact。
- 生产后端 `8001` 未重启；`/api/v1/hmm-training/configs?model_type=sector_hmm` 和 snapshots API 已确认可返回 5 个新增候选。

## 2026-05-04 sector-factor gate 与 HMM 输入预处理实验

本轮继续以 `qe_20260502_131502_9b54` Loop1 的非 HMM 设置为固定基线，分别围绕 Loop2 old covfix 与 Loop10 penalty-only 做两条隐藏实验线。生产/QE 默认 HMM 下拉仍只保留 Loop2 与 Loop10 两个 `sector_hmm` 版本；下面候选都注册为 hidden `model_type`，只能通过自定义 QE loop 的 `snapshot_id` 显式引用。

### sector-factor gate / confirmation 候选

hidden `model_type='sector_hmm_experimental_stacking_20260504'`。已创建远端 QE 任务 `qe_20260504_110457_5400`，远端节点 `rdagent-node1`，并行度 4，loop 设置包括 no-HMM、Loop2、Loop10 以及 8 个 sector-factor gate/confirmation 候选。任务创建使用 dev backend `8011`，DB/API 行在 UI 使用的 `8001` 可见；截至 2026-05-04 12:01，Loop1-4 正在远端 V25 分钟线回测中，后续 loop 由任务内并行调度继续提交。

```text
base  方向                 Snapshot ID                            名称
----  -------------------  ------------------------------------  ----------------------------------------------------
L2    boost_confirm        17809fe6-bcaf-487e-9205-d11b47fe08f9  HMM_EXP_L2_sf_boost_confirm_tfcore_c70__qe20260504
L2    penalty_confirm      9761439e-06d1-4303-a6a7-1a4836c8b3f8  HMM_EXP_L2_sf_penalty_confirm_tfcore_c30__qe20260504
L2    both_confirm         b45f6571-19b0-4e0a-9a20-ab182e59a68a  HMM_EXP_L2_sf_both_confirm_tfcore_c70c30__qe20260504
L2    risk_only_overlay    decfdc2c-f395-4cda-aac6-8636c5fcde50  HMM_EXP_L2_sf_risk_only_tfcore_p098_c30__qe20260504
L10   boost_confirm        040570a9-3a34-4201-8057-42299ec92c3e  HMM_EXP_L10_sf_boost_confirm_tfcore_c70__qe20260504
L10   penalty_confirm      f405daee-f922-449d-bf37-ca91b2fd9995  HMM_EXP_L10_sf_penalty_confirm_tfcore_c30__qe20260504
L10   both_confirm         9a5c67d6-3fbc-41ee-93b1-36031ae181ad  HMM_EXP_L10_sf_both_confirm_tfcore_c70c30__qe20260504
L10   risk_only_overlay    b19d4beb-8e77-4ddc-a30f-d9f07e7fcda2  HMM_EXP_L10_sf_risk_only_tfcore_p098_c30__qe20260504
```

离线 TopK 替换归因的初步排序显示，L10 `penalty_confirm` / `both_confirm` 对 holdout TopK 替换质量最好，但这仍不是完整分钟线 QE 结论，必须等待 `qe_20260504_110457_5400` 的真实回测结果。

### HMM 输入预处理候选

hidden `model_type='sector_hmm_experimental_preprocess_20260504'`。这些版本不是改原始日线/分钟线数据，而是在 legacy 7 维 HMM observation 层做输入预处理后重新训练 HMM，并为 Loop2 / Loop10 两套 coefficient map 各注册一份预计算 snapshot。2026-05-04 11:55 重新注册后已修正两个可用性问题：DB `model_path` 存为 Windows 可读路径，且 `config_json.coefficient_windows` 明确登记 `preset_A / 2024-07-01 / 2026-04-27`，使 strict-no-leakage QE 解析可以 fail-fast 命中本地 artifact。

```text
base  预处理模式             Snapshot ID                            名称
----  ---------------------  ------------------------------------  -------------------------------------------------
L2    train-only zscore      71e966b4-6f7e-4767-b012-a19798df73bc  HMM_EXP_L2_preproc_train_zscore__qe20260504
L10   train-only zscore      d2a56dad-b777-4fd6-964a-0420241b444f  HMM_EXP_L10_preproc_train_zscore__qe20260504
L2    winsor(1/99)+zscore    fef38650-e591-4145-a62f-cfab9e2c10eb  HMM_EXP_L2_preproc_winsor01_zscore__qe20260504
L10   winsor(1/99)+zscore    acc27436-6e87-43fe-8e25-78261b80d47f  HMM_EXP_L10_preproc_winsor01_zscore__qe20260504
L2    robust zscore          a72f7e35-b52a-4969-b1e7-1b1ec21270b0  HMM_EXP_L2_preproc_robust_zscore__qe20260504
L10   robust zscore          d40c97fd-40ff-4ea5-9089-a3650ab26afe  HMM_EXP_L10_preproc_robust_zscore__qe20260504
L2    sector CS rank         b49a82e1-1fe3-466d-8b70-1632e267c442  HMM_EXP_L2_preproc_sector_cs_rank__qe20260504
L10   sector CS rank         c5647469-52b0-4d2c-a224-2f3a54b27d18  HMM_EXP_L10_preproc_sector_cs_rank__qe20260504
L2    sector CS zscore       3b9ef5f6-e16c-4328-be2d-86447542b690  HMM_EXP_L2_preproc_sector_cs_zscore__qe20260504
L10   sector CS zscore       9ae55e28-0227-48bf-af8c-dcedae275609  HMM_EXP_L10_preproc_sector_cs_zscore__qe20260504
```

验证状态：

- 10 个 snapshot 均为 `completed`，`sector_count=131`。
- 每个 snapshot 都有 `models.json` 与 `coefficients_preset_A_2024-07-01_2026-04-27.json`，覆盖 442 个交易日，首尾日 131 个行业，`stock_sector_map=5847`。
- `ConfigComposer._precompute_hmm_coefficients` 已用 L2 train-zscore snapshot 做真实解析 smoke：strict window 通过，返回 442 天系数。
- 预处理 QE payload 已生成：`.codex_tmp/hmm_preprocess_custom_evo_payload_dev8011_20260504.json`，13 loops（no-HMM + Loop2 + Loop10 + 10 个预处理候选），远端节点 `rdagent-node1`，并行度 4。
- 由于 `qe_20260504_110457_5400` 当前 4 个 V25 回测进程已占用约 73/78Gi 内存，未立即叠加启动第二个 p4 任务，避免 swap 污染回测结果；本地延迟提交器 `.codex_tmp/launch_hmm_preprocess_after_sector_task.ps1` 会在 sector-factor 任务所有 loop 不再 `running/not_found` 后自动提交预处理 QE 任务。

离线 TopK 替换归因的初步排序显示，L10 `train_zscore`、L10 `winsor01_zscore`、L10 `sector_cs_rank` 优于 Loop10 baseline；L2 预处理版本暂未在 TopK 归因上超过 Loop2 baseline。最终仍以完整 QE 分钟线回测的年化收益、回撤、Sharpe、换手、交易成本和 TopK 进出归因为准。

## QE 对比基线

任务：`qe_20260502_131502_9b54`

```text
Loop  配置                               年化收益  最大回撤   Sharpe   IC      RankIC
----  ---------------------------------  --------  --------  -------  ------  ------
L1    No HMM                             46.21%    -16.58%   1.9942   0.0787  0.1131
L2    old covfix w3 HMM                  47.56%    -15.59%   2.0645   0.0787  0.1131
L3    dynamic PUP strict conf=0.10 HMM   45.76%    -16.52%   1.9877   0.0787  0.1131
L4    duplicate of L3                    45.76%    -16.52%   1.9877   0.0787  0.1131
```

解释：

- L2 是当前保留主线，收益、回撤、Sharpe 均略优于 no-HMM。
- L3/L4 低于 no-HMM，也低于 old covfix，因此 dynamic PUP strict 0.10 已下架。
- strict 0.075 此前也验证不佳，已与 0.10 一起下架。

## 当前 HMM registry 状态

### 生产/QE 可选版本（`model_type='sector_hmm'`）

```text
角色        Config ID                             Snapshot ID                            名称
----------  ------------------------------------  ------------------------------------  -------------------------------------------------------------
保留基线    b99c907b-873a-4173-a4ee-5eab266f8c49  bbec3863-fb67-445f-938e-66f092d18696  HMM_COVFIX_w3_raw_same_params__n3_diag_rw3_nozscore
当前最佳    ce4952c1-4b0d-46a7-81f2-ae1d4a249555  6ea64754-003d-48d8-ad9e-d0e7857716c8  HMM_TEST_old_covfix_penalty_only_f096_b000__qe20260504
```

### 已下架版本（历史保留，不再出现在 QE HMM 可选列表）

```text
Config ID                             Snapshot ID                            名称                                                         当前 model_type
------------------------------------  ------------------------------------  -----------------------------------------------------------  ----------------------------------------
5a3183b6-39bc-45dd-8b3d-d2027c476e62  d11dc38e-84f0-4e5c-80e7-42cb5d978d40  HMM_DYNAMIC_PUP_w20_50_conf_0p10_STRICT_DEFAULT__n3_diag       sector_hmm_disabled_ineffective_20260502
8ef81e6b-263d-4acd-93ff-4a20526b2d13  c1c81aa0-aae2-4942-881c-4baafbd2f160  HMM_DYNAMIC_PUP_w20_50_conf_0p075_STRICT_DEFAULT__n3_diag      sector_hmm_disabled_ineffective_20260502
82a40d27-0e96-48a1-882a-4d182a58b931  377a8447-ee26-44a8-8ead-7338f525e0f2  HMM_TEST_old_covfix_boost_only_p105__qe20260504                sector_hmm_disabled_superseded_by_loop2_loop10_20260504
22d53160-7195-4e69-86ec-76c19c615a69  5a8ce90e-50bb-4fbd-8cd8-e3b95c9dffa0  HMM_TEST_old_covfix_penalty094_boost103__qe20260504            sector_hmm_disabled_superseded_by_loop2_loop10_20260504
ea0db9d3-69bf-489e-aa55-c74b6340e68d  afa6acd9-f766-4394-970e-451d1a39bb06  HMM_TEST_old_covfix_penalty095_boost104__qe20260504            sector_hmm_disabled_superseded_by_loop2_loop10_20260504
518ddf2d-e4a0-4bf0-8572-7cea429e27d5  8ddb5d29-8097-4aef-b110-f2f94f54ca4b  HMM_TEST_old_covfix_penalty095_boost106__qe20260504            sector_hmm_disabled_superseded_by_loop2_loop10_20260504
90e2771e-3245-45c0-b8ad-471b10b24391  89753fae-0c3c-4c75-9282-c20d7d833ffa  HMM_TEST_old_covfix_primary_b020_p005__qe20260502              sector_hmm_disabled_superseded_by_loop2_loop10_20260504
14fd8dd6-896d-4a7d-b8be-ec6a7cf44c95  78a4ecf7-4cca-4b67-af66-3d59573587eb  HMM_TEST_hyb_old_primary_turnover_flow_core_c70__qe20260502    sector_hmm_disabled_superseded_by_loop2_loop10_20260504
94ba4a64-998d-4897-ace2-f0fe06133935  28335a3c-64d8-4ce8-944e-25e48a68f77c  HMM_TEST_sf_turnover_fast_q20_b010_p005__qe20260502            sector_hmm_disabled_superseded_by_loop2_loop10_20260504
```

下架方式是软下架：只改变 DB 中 `model_train_configs.model_type`，不删除历史模型目录和历史 snapshot，避免破坏 QE 历史追溯。

## 当前可选候选含义

```text
候选名称                                                        用途/假设
--------------------------------------------------------------  ----------------------------------------------------------------------
HMM_TEST_old_covfix_primary_b020_p005__qe20260502                old covfix 方向不变，只把系数映射弱化为 trending=1.020 / fading=0.995
HMM_TEST_hyb_old_primary_turnover_flow_core_c70__qe20260502      old covfix primary + 高 RankIC 板块 turnover/flow 因子确认，当前主推荐候选
HMM_TEST_sf_turnover_fast_q20_b010_p005__qe20260502              纯板块因子 ablation，用于判断 sector factor 本身是否有增益
HMM_TEST_old_covfix_penalty_only_f096_b000__qe20260504           old covfix 状态不变，trending 从 1.05 降到 1.00，只验证 fading=0.96 的保护是否贡献收益
HMM_TEST_old_covfix_boost_only_p105__qe20260504                  old covfix 状态不变，fading 从 0.96 升到 1.00，只验证 trending=1.05 是否贡献收益
HMM_TEST_old_covfix_penalty094_boost103__qe20260504              old covfix 状态不变，增强风险惩罚到 0.94，同时把趋势增强降到 1.03
HMM_TEST_old_covfix_penalty095_boost104__qe20260504              old covfix 状态不变，中等风险惩罚 0.95 + 中等趋势增强 1.04
HMM_TEST_old_covfix_penalty095_boost106__qe20260504              old covfix 状态不变，中等风险惩罚 0.95 + 偏强趋势增强 1.06
```

上述待测候选都只支持：

```text
preset       preset_A
窗口         test_start=2024-07-01, backtest_end=2026-04-27, test_end=2026-04-28
日期数       442 个交易日
行业数       131 个 L2 行业
stock map    5847 个股票到行业映射
保护         strict_no_leakage=true, precomputed_only=true
```

## 关键文件与资产位置

### HMM 训练/预计算主脚本

```text
路径                                                              用途
----------------------------------------------------------------  ------------------------------------------------------------------
backend/services/hmm_training_service.py                           后端 HMM Training Center 服务；创建配置、触发训练、列出快照、预计算系数
backend/routers/hmm_training.py                                    HMM API 路由；/api/v1/hmm-training/*
scripts/hmm_train_script.py                                        后端通过 WSL 调用的 HMM 训练入口，封装 RD-Agent HMM 训练模块
scripts/precompute_hmm_coefficients.py                             根据已训练 HMM 模型和窗口预生成 daily sector coefficients
F:/Dev/RD-Agent-main/model_training/hmm/train_sector_hmm.py         RD-Agent HMM 7维 observation 训练主逻辑
F:/Dev/RD-Agent-main/model_training/hmm/config.py                   HMMTrainConfig；默认支持 zscore=True，但平台入口当前默认 False
F:/Dev/RD-Agent-main/model_training/hmm/precompute_coefficients.py  RD-Agent 侧旧版预计算/验证辅助
```

### 当前模型资产

```text
角色        路径
----------  --------------------------------------------------------------------------------------------------------------
old covfix  backend/data/hmm_models/b99c907b-873a-4173-a4ee-5eab266f8c49/2026-04-27/models.json
候选 1      backend/data/hmm_models/90e2771e-3245-45c0-b8ad-471b10b24391/2026-05-02/models.json
候选 2      backend/data/hmm_models/14fd8dd6-896d-4a7d-b8be-ec6a7cf44c95/2026-05-02/models.json
候选 3      backend/data/hmm_models/94ba4a64-998d-4897-ace2-f0fe06133935/2026-05-02/models.json
remap 1     backend/data/hmm_models/ce4952c1-4b0d-46a7-81f2-ae1d4a249555/2026-05-04/models.json
remap 2     backend/data/hmm_models/82a40d27-0e96-48a1-882a-4d182a58b931/2026-05-04/models.json
remap 3     backend/data/hmm_models/22d53160-7195-4e69-86ec-76c19c615a69/2026-05-04/models.json
remap 4     backend/data/hmm_models/ea0db9d3-69bf-489e-aa55-c74b6340e68d/2026-05-04/models.json
remap 5     backend/data/hmm_models/518ddf2d-e4a0-4bf0-8572-7cea429e27d5/2026-05-04/models.json
下架 0.10   backend/data/hmm_models/5a3183b6-39bc-45dd-8b3d-d2027c476e62/2026-04-29/models.json
下架 0.075  backend/data/hmm_models/8ef81e6b-263d-4acd-93ff-4a20526b2d13/2026-04-29/models.json
```

注意：`backend/data/hmm_models` 在 `.gitignore` 的 `data/` 规则下，通常不是 git 跟踪资产；迁移或复现实验时必须同时考虑 DB registry 和本地模型文件。

### 当前候选注册备份/脚本

```text
路径                                                                                   用途
-------------------------------------------------------------------------------------  ------------------------------------------------------------
.codex_tmp/hmm_registry_updates/hmm_registry_update_before_20260502_193953.json         注册/下架前 DB 备份
.codex_tmp/hmm_registry_updates/register_hmm_qe_candidates_20260502.py                  本次本地注册脚本，未进入 git，供人工追溯
scripts/register_hmm_remap_qe_candidates_20260504.py                                    2026-05-04 old covfix remap 候选注册脚本，不内嵌 DB 密码
.codex_tmp/hmm_registry_updates/hmm_remap_registry_result_20260504_005555.json           2026-05-04 注册结果本地证据
```

`.codex_tmp` 是本地临时目录，不应作为长期唯一证据；长期结论以本文档和 DB 当前状态为准。

## HMM observation 与归一化状态

当前 old covfix 的 7 维 observation：

```text
特征名              计算方式/含义
------------------  ------------------------------------------------------------
daily_return        sw2_pct_change / 100
excess_return_Nd    板块收益率 - CSI300 收益率的 N 日均值
volume_ratio        sw2_vol / 全市场 sw_daily vol
limit_up_ratio      行业内涨停占比
volatility_Nd       N 日收益率波动率
net_mf_ratio        sw2_mf_net_amt / sw2_amount
elg_net_mf_ratio    (sw2_mf_buy_elg_amt - sw2_mf_sell_elg_amt) / sw2_amount
```

当前状态：

```text
版本/逻辑                              是否 z-score / preprocess                         说明
------------------------------------  -----------------------------------------------  ----------------------------------------------
old covfix 保留版                      否                                               config_json.zscore=false，models.json 无 zscore_mean
3 个新增 QE 测试候选                   否                                               复制 old covfix 模型，仅替换预计算系数
已下架 dynamic PUP strict 0.10/0.075   是                                               models.json 内有 preprocess: winsor + zscore，但 QE 效果差
RD-Agent HMM 训练模块                  支持                                             cfg.zscore=True 时保存 zscore_mean/zscore_std
平台 HMM 训练入口                      默认不启用                                       scripts/hmm_train_script.py 使用 config_dict.get("zscore", False)
```

重要判断：

- 当前 HMM 没有直接吃原始股票价格或行业指数点位，因此已经规避了绝对价格/点位数量级问题。
- 当前 HMM 仍可能受到特征方差、资金流异常值、涨停占比尖峰、volume_ratio 行业规模暴露影响。
- 后续优化应在 HMM 训练输入层增加可复现实验版本，而不是改写原始日线/分钟线数据。

## HMM 训练流程

手工滚动训练大致链路：

```text
前端/接口选择 config
  -> backend/routers/hmm_training.py
  -> HMMTrainingService.trigger_rolling_training / run_training
  -> scripts/hmm_train_script.py
  -> WSL rdagent-gpu 环境
  -> F:/Dev/RD-Agent-main/model_training/hmm/train_sector_hmm.py
  -> 输出 models.json
  -> HMMTrainingService._precompute_coefficients_for_snapshot
  -> scripts/precompute_hmm_coefficients.py
  -> 输出 coefficients_preset_A_*.json 等预计算系数
  -> 插入 model_train_snapshots completed
```

QE 使用链路：

```text
QE UI 选择 HMM snapshot_id
  -> quantevolver / quantevolver_evolution 解析 snapshot_id
  -> HMMTrainingService.get_snapshot 读取 model_path
  -> ConfigComposer._resolve_hmm_coefficients_json
  -> 优先命中 model_path 同目录 coefficients_{preset}_{test_start}_{backtest_end}.json
  -> 写入 QE workspace 的 hmm_sector_coefficients.json
  -> 策略按 stock_sector_map + daily_coefficients 调整 score 后重排 TopK
```

## API/DB 续接检查命令

查看当前可选 HMM：

```powershell
Invoke-RestMethod -Uri 'http://127.0.0.1:8001/api/v1/hmm-training/configs?model_type=sector_hmm'
```

查看指定 config 快照：

```powershell
Invoke-RestMethod -Uri 'http://127.0.0.1:8001/api/v1/hmm-training/configs/<config_id>/snapshots'
```

直接查 DB：

```sql
SELECT c.config_id, c.model_type, c.display_name,
       s.snapshot_id, s.status, s.sector_count, s.model_path
FROM model_train_configs c
LEFT JOIN model_train_snapshots s ON s.config_id = c.config_id
WHERE c.model_type LIKE 'sector_hmm%'
ORDER BY c.model_type, c.created_at DESC;
```

## 相关分析文档

```text
文档                                                                                              内容
------------------------------------------------------------------------------------------------  ------------------------------------------------------------
docs/analysis/hmm_offline_diagnostic_qe_20260502_131502_9b54.md                                   QE 4 loop HMM/no-HMM 离线诊断
docs/analysis/hmm_offline_optimization_qe_20260502_131502_9b54.md                                 old covfix coefficient mapping 离线搜索
docs/analysis/hmm_sector_factor_overlay_replacement_qe_20260502_131502_9b54.md                    板块因子 overlay / hybrid 候选验证
docs/analysis/hmm_sector_factor_rankic_validation_20260502.md                                     板块因子 RankIC 验证
docs/analysis/hmm_latest_one_year_sector_rotation_rough_check_qe_20260502_131502_9b54.md           1 年板块轮动粗检
docs/analysis/hmm_training_current_status_20260503.md                                             本文档：当前状态和续接入口
```

## 下一步建议

优先顺序：

```text
优先级  动作
------  --------------------------------------------------------------------------------------------------
P0      用当前 3 个 2026-05-02 待测候选 + 5 个 2026-05-04 remap 候选 + old covfix + no-HMM 做 QE shadow loop 完整对比，确认是否真实超过旧最佳版本
P0      不要删除 old covfix；它是当前唯一确认有效 HMM 基线
P0      sector-factor 不建议直接替代 old covfix；若要继续，应作为 old covfix/remap 的二阶段 gating/confirmation 做增量验证
P1      新增 old covfix + train-only zscore 版本，验证 zscore 是否提升而不是直接替换生产基线
P1      新增 winsor+zscore / robust zscore 版本，重点处理资金流和涨停占比尖峰
P1      针对 volume_ratio 测试 rolling z 或 cross-sectional rank，减少大行业规模暴露
P2      针对 5D/10D/20D 分 horizon 训练或分 coefficient mapping，避免一个 HMM 版本硬套所有预测周期
P2      若板块因子 overlay 在 QE 中有效，再考虑将高 RankIC 板块因子正式进入 HMM emission/gating，而不只是后处理系数
```

下次开始时建议先做三件事：

1. 读取本文档和 `docs/codex_project_memory.md`。
2. 调用 `8001` 的 HMM configs/snapshots API，确认 DB 可选列表没有被其他流程改变。
3. 若要训练新版本，先复制 old covfix config 为新 config，显式写入 preprocess/zscore 策略，不覆盖已有 config/snapshot。
