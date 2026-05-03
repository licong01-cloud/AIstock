# HMM 模型训练当前状态与续接指南（2026-05-03）

本文是下次继续更新 HMM 模型时的入口文档，覆盖当前生产可选版本、已下架版本、训练/预计算脚本、归一化状态、QE 验证结果和下一步建议。当前结论基于本地 DB `aistock`、HMM registry、`qe_20260502_131502_9b54` 四个 loop 的 QE 结果，以及 2026-05-02 离线诊断产物。

## 当前结论

- 当前唯一确认对 QE 有正向收益的主线是 old covfix：`HMM_COVFIX_w3_raw_same_params__n3_diag_rw3_nozscore`。
- 最新 dynamic PUP strict 0.10 / 0.075 两个版本已验证收益不佳，已从 `sector_hmm` 可选列表下架，但保留历史 DB 记录和模型资产用于追溯。
- 已新增 3 个 QE shadow loop 候选进入 HMM/QE 可选列表，均基于 old covfix 模型或板块因子 overlay 的离线 coefficient artifact，尚未完成完整 QE 回测验证。
- 当前 old covfix 主线没有做传统 z-score 归一化；它使用相对化观测量（收益率、超额收益、成交量占比、涨停占比、资金流占比等），不是直接使用股票价格、行业指数点位或成交额绝对值。
- 原始日线/分钟线数据层不应归一化；HMM 训练输入层后续应系统验证 train-only z-score、winsor+zscore、robust zscore、板块横截面 rank/zscore 等版本。
- 仅修改 HMM registry / DB 记录 / HMM 模型资产时，生产 FastAPI 后端 `8001` 不需要重启；前端刷新页面即可重新读取可选列表。

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
待测候选    90e2771e-3245-45c0-b8ad-471b10b24391  89753fae-0c3c-4c75-9282-c20d7d833ffa  HMM_TEST_old_covfix_primary_b020_p005__qe20260502
待测候选    14fd8dd6-896d-4a7d-b8be-ec6a7cf44c95  78a4ecf7-4cca-4b67-af66-3d59573587eb  HMM_TEST_hyb_old_primary_turnover_flow_core_c70__qe20260502
待测候选    94ba4a64-998d-4897-ace2-f0fe06133935  28335a3c-64d8-4ce8-944e-25e48a68f77c  HMM_TEST_sf_turnover_fast_q20_b010_p005__qe20260502
```

### 已下架版本（历史保留，不再出现在 QE HMM 可选列表）

```text
Config ID                             Snapshot ID                            名称                                                         当前 model_type
------------------------------------  ------------------------------------  -----------------------------------------------------------  ----------------------------------------
5a3183b6-39bc-45dd-8b3d-d2027c476e62  d11dc38e-84f0-4e5c-80e7-42cb5d978d40  HMM_DYNAMIC_PUP_w20_50_conf_0p10_STRICT_DEFAULT__n3_diag       sector_hmm_disabled_ineffective_20260502
8ef81e6b-263d-4acd-93ff-4a20526b2d13  c1c81aa0-aae2-4942-881c-4baafbd2f160  HMM_DYNAMIC_PUP_w20_50_conf_0p075_STRICT_DEFAULT__n3_diag      sector_hmm_disabled_ineffective_20260502
```

下架方式是软下架：只改变 DB 中 `model_train_configs.model_type`，不删除历史模型目录和历史 snapshot，避免破坏 QE 历史追溯。

## 当前可选候选含义

```text
候选名称                                                        用途/假设
--------------------------------------------------------------  ----------------------------------------------------------------------
HMM_TEST_old_covfix_primary_b020_p005__qe20260502                old covfix 方向不变，只把系数映射弱化为 trending=1.020 / fading=0.995
HMM_TEST_hyb_old_primary_turnover_flow_core_c70__qe20260502      old covfix primary + 高 RankIC 板块 turnover/flow 因子确认，当前主推荐候选
HMM_TEST_sf_turnover_fast_q20_b010_p005__qe20260502              纯板块因子 ablation，用于判断 sector factor 本身是否有增益
```

三者都只支持：

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
P0      用当前 3 个待测候选 + old covfix + no-HMM 做 QE shadow loop 完整对比，确认是否真实超过旧最佳版本
P0      不要删除 old covfix；它是当前唯一确认有效 HMM 基线
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
