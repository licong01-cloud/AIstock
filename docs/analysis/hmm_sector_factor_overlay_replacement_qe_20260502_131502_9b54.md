# HMM 板块因子系数 Top50 Replacement 离线验证（2026-05-02）

## 结论摘要

- 本次继续保持策略和程序运行逻辑不变，只读取现有 QE no-HMM `pred.pkl` / `label.pkl`、本地市场数据和上一步板块因子候选。
- 目标是把高 RankIC 板块因子转成 sector coefficient，并测试 sector-only 与 old covfix + sector-factor hybrid 两类候选。
- 网格结果按 `holdout` 段排序；`train_pre_holdout` 只用于观察稳定性，不作为可直接上线依据。
- 该结果仍不是完整 QE 回测，不能替代 n_drop、已有持仓、停牌/涨跌停和分钟执行后的真实组合收益。

## 核心对比

```text
Type                Candidate                                                 NetLabel10D  NetDB10D  NetDB20D  ChangedDays
------------------  --------------------------------------------------------  -----------  --------  --------  -----------
best_sector_factor  sf_turnover_fast_q20_b0p010_p0p005                        1.37%        0.90%     1.62%     85         
best_old_covfix     old_covfix_primary_b020_p005                              1.17%        2.46%     2.76%     96         
best_hybrid         hyb_old_primary_b020_p005_confirm_turnover_flow_core_c70  1.78%        2.15%     5.31%     31         
best_hybrid_return  hyb_old_high_db10_b010_p005_confirm_turnover_fast_c60     0.42%        5.72%     6.45%     35         
```

- `best_hybrid` 是更均衡的候选：Label10D、DB10D、DB20D 都为正，且相对 old covfix 减少替换频率。
- `best_hybrid_return` 是收益型备选：DB10D/DB20D 更高，但 Label10D 较弱且更可能受少数高收益 replacement 影响，暂不作为唯一主候选。

## Holdout Top 候选

```text
Candidate                                                   Days  ChgDays  Enter/Day  UniqueEnter  NetLabel10D  NetDB5D  NetDB10D  NetDB20D  PosDay
----------------------------------------------------------  ----  -------  ---------  -----------  -----------  -------  --------  --------  ------
hyb_old_primary_b020_p005_confirm_turnover_flow_core_c70    239   31       0.13       28           1.78%        1.44%    2.15%     5.31%     7.5%  
hyb_old_primary_b020_p005_confirm_best5_core_c70            239   31       0.13       28           1.78%        1.44%    2.15%     5.31%     7.5%  
hyb_old_high_db10_b010_p005_confirm_turnover_flow_core_c70  239   27       0.11       27           1.66%        1.38%    2.06%     5.63%     5.9%  
hyb_old_high_db10_b010_p005_confirm_best5_core_c70          239   27       0.11       27           1.66%        1.38%    2.06%     5.63%     5.9%  
sf_turnover_fast_q20_b0p010_p0p005                          240   85       0.38       83           1.37%        0.21%    0.90%     1.62%     18.3% 
sf_turnover_flow_core_q20_b0p010_p0p005                     240   99       0.44       96           1.36%        0.10%    1.10%     0.66%     22.1% 
sf_best5_core_q20_b0p010_p0p005                             240   99       0.44       96           1.36%        0.10%    1.10%     0.66%     22.1% 
sf_long_flow_tier_q20_b0p010_p0p010                         240   111      0.53       110          1.36%        -0.38%   -0.49%    -1.14%    22.5% 
sf_turnover_flow_core_q20_b0p020_p0p010                     240   156      0.82       159          1.30%        0.20%    0.63%     0.98%     34.2% 
sf_best5_core_q20_b0p020_p0p010                             240   156      0.82       159          1.30%        0.20%    0.63%     0.98%     34.2% 
sf_turnover_flow_core_q20_b0p020_p0p005                     240   136      0.68       130          1.28%        0.42%    0.74%     1.08%     30.4% 
sf_best5_core_q20_b0p020_p0p005                             240   136      0.68       130          1.28%        0.42%    0.74%     1.08%     30.4% 
old_covfix_primary_b020_p005                                239   96       0.44       85           1.17%        0.32%    2.46%     2.76%     20.9% 
sf_turnover_flow_core_q20_b0p010_p0p010                     240   133      0.61       133          1.16%        0.09%    0.82%     0.02%     28.7% 
sf_best5_core_q20_b0p010_p0p010                             240   133      0.61       133          1.16%        0.09%    0.82%     0.02%     28.7% 
```

## Full Period Top 候选

```text
Candidate                                                   Days  ChgDays  Enter/Day  UniqueEnter  NetLabel10D  NetDB5D  NetDB10D  NetDB20D  PosDay
----------------------------------------------------------  ----  -------  ---------  -----------  -----------  -------  --------  --------  ------
old_covfix_high_db10_b010_p005                              442   160      0.40       147          2.00%        -0.06%   2.21%     3.20%     20.4% 
hyb_old_primary_b020_p005_confirm_turnover_flow_core_c70    442   72       0.18       69           1.94%        -0.12%   1.25%     2.45%     9.7%  
hyb_old_primary_b020_p005_confirm_best5_core_c70            442   72       0.18       69           1.94%        -0.12%   1.25%     2.45%     9.7%  
hyb_old_high_db10_b010_p005_confirm_turnover_fast_c50       442   100      0.24       92           1.90%        -0.00%   2.59%     2.97%     12.7% 
hyb_old_high_db10_b010_p005_confirm_turnover_flow_core_c50  442   101      0.24       91           1.84%        -0.17%   2.56%     3.54%     12.9% 
hyb_old_high_db10_b010_p005_confirm_best5_core_c50          442   101      0.24       91           1.84%        -0.17%   2.56%     3.54%     12.9% 
old_covfix_primary_b020_p005                                442   203      0.57       191          1.76%        -0.07%   1.70%     2.22%     25.8% 
hyb_old_primary_b020_p005_confirm_turnover_flow_core_c50    442   118      0.30       110          1.76%        -0.46%   2.07%     3.96%     14.9% 
hyb_old_primary_b020_p005_confirm_best5_core_c50            442   118      0.30       110          1.76%        -0.46%   2.07%     3.96%     14.9% 
hyb_old_primary_b020_p005_confirm_turnover_flow_core_c60    442   90       0.22       87           1.65%        -0.83%   0.78%     1.97%     11.3% 
hyb_old_primary_b020_p005_confirm_best5_core_c60            442   90       0.22       87           1.65%        -0.83%   0.78%     1.97%     11.3% 
sf_turnover_fast_q20_b0p010_p0p005                          443   195      0.49       178          1.63%        -0.25%   1.22%     2.79%     23.9% 
```

## 最佳候选进入股票样本：`hyb_old_primary_b020_p005_confirm_turnover_flow_core_c70`

```text
Symbol     Sector     EnterDays  MeanLabel10D  MeanDB10D  AvgRawRank  AvgAdjRank
---------  ---------  ---------  ------------  ---------  ----------  ----------
688296.SH  801104.SI  3          4.05%         5.18%      51.7        46.3      
688355.SH  801072.SI  3          0.62%         2.19%      53.3        48.0      
605255.SH  801093.SI  2          4.89%         7.10%      51.0        50.0      
603937.SH  801055.SI  2          -0.44%        2.60%      52.5        50.0      
603968.SH  801034.SI  2          0.27%         1.79%      53.0        43.5      
300865.SZ  801074.SI  2          0.91%         0.02%      51.0        50.0      
688056.SH  801072.SI  2          -1.16%        -0.64%     53.0        47.5      
688557.SH  801072.SI  1          28.25%        33.97%     51.0        49.0      
002474.SZ  801103.SI  1          17.25%        18.70%     53.0        50.0      
688619.SH  801101.SI  1          14.93%        17.56%     54.0        45.0      
002132.SZ  801072.SI  1          29.19%        17.45%     54.0        48.0      
603955.SH  801723.SI  1          16.71%        16.25%     51.0        49.0      
002664.SZ  801093.SI  1          10.59%        15.63%     51.0        50.0      
300614.SZ  801971.SI  1          15.51%        13.53%     51.0        50.0      
301201.SZ  801156.SI  1          11.20%        13.08%     51.0        50.0      
688229.SH  801103.SI  1          1.88%         11.31%     53.0        44.0      
000632.SZ  801231.SI  1          6.06%         9.91%      51.0        50.0      
300935.SZ  801104.SI  1          11.99%        9.75%      58.0        48.0      
002909.SZ  801034.SI  1          9.11%         8.96%      55.0        49.0      
601857.SH  801963.SI  1          10.90%        8.80%      51.0        50.0      
```

## 产物

- 候选摘要：`.codex_tmp\hmm_offline_diag\qe_20260502_131502_9b54\sector_factor_overlay\sector_factor_overlay_candidate_summary.csv`
- replacement 明细：`.codex_tmp\hmm_offline_diag\qe_20260502_131502_9b54\sector_factor_overlay\sector_factor_overlay_replacements.csv`
- 日度摘要：`.codex_tmp\hmm_offline_diag\qe_20260502_131502_9b54\sector_factor_overlay\sector_factor_overlay_daily_summary.csv`
- 候选元数据：`.codex_tmp\hmm_offline_diag\qe_20260502_131502_9b54\sector_factor_overlay\sector_factor_overlay_candidate_metadata.csv`
- 最佳候选 JSON：`.codex_tmp\hmm_offline_diag\qe_20260502_131502_9b54\sector_factor_overlay\candidate_coefficients`
- 板块分数：`.codex_tmp\hmm_offline_diag\qe_20260502_131502_9b54\sector_factor_overlay\sector_factor_overlay_group_scores.csv`

## 下一步判断

- 若 hybrid 在 holdout 同时改善 `NetLabel10D` 和 `NetDB10D/20D`，优先把 hybrid 作为 QE shadow loop 候选。
- 若 sector-only 高于 old covfix 的 label 但低于 DB10/20，应谨慎：它可能提升 label ranking，但未必转化为组合收益。
- 若 old covfix 仍显著更强，则保留 old covfix 主路径，把板块因子放入 HMM emission/gating，而不是直接替代 coefficient。