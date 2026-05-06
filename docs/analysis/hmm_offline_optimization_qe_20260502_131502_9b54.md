# HMM 离线优化搜索 - qe_20260502_131502_9b54

## 约束
- 只使用现有 pred.pkl、label.pkl、HMM coefficients 和回测 artifact；未修改策略或程序代码。
- 不补第三版本基线；L4 已确认与 L3 重复。
- 搜索只给出下一轮 coefficient 生成/变换候选，不能替代完整 QE 回测。
- 为避免完全样本内过拟合，使用 train=2024-07-01~2025-06-30、test=2025-07-01~2026-04-27。
- 为加速搜索，仅使用 raw rank 前 150 的股票重放 Top50；原始 HMM 替换中 L2 最大进入 raw rank=86，L3 最大进入 raw rank=68，因此该近似覆盖当前有效替换范围。

## 搜索规模
- L2 old covfix: 100 个变体
- L3 dynamic PUP: 248 个变体

## L2_old_covfix 候选
```text
Variant                              FullNet  TrainNet  TestNet   EnterN  AvgEnter  Changed  Robust
-----------------------------------  -------  --------  --------  ------  --------  -------  -------
original                               0.93%     1.21%     0.46%    1115      2.52      421    0.69%
sign_b0.020_p0.005_band0.001           1.76%     1.77%     1.72%     253      0.57      203    2.16%
sign_b0.025_p0.005_band0.001           1.62%     1.72%     1.40%     291      0.66      218    1.80%
sign_b0.015_p0.005_band0.001           1.40%     1.51%     1.13%     204      0.46      177    1.48%
sign_b0.010_p0.005_band0.001           2.00%     2.60%     0.90%     176      0.40      160    1.40%
sign_b0.030_p0.005_band0.001           1.26%     1.38%     0.98%     326      0.74      223    1.30%
sign_b0.005_p0.005_band0.001           1.50%     1.82%     0.91%     148      0.33      140    1.28%
sign_b0.060_p0.020_band0.001           1.00%     1.08%     0.85%     824      1.86      386    1.10%
sign_b0.040_p0.005_band0.001           0.90%     0.87%     0.95%     410      0.93      245    1.10%
sign_b0.005_p0.060_band0.001           0.87%     0.85%     0.92%    1305      2.95      432    1.07%
sign_b0.015_p0.000_band0.001           1.50%     0.68%     3.78%     106      0.24       88    1.05%
sign_b0.000_p0.005_band0.001           1.27%     1.53%     0.73%     116      0.26      110    1.05%
sign_b0.020_p0.020_band0.001           1.14%     1.37%     0.72%     551      1.25      355    1.01%
```

## L3_dynamic_pup 候选
```text
Variant                              FullNet  TrainNet  TestNet   EnterN  AvgEnter  Changed  Robust
-----------------------------------  -------  --------  --------  ------  --------  -------  -------
original                               0.06%     0.64%    -0.98%     713      1.61      383   -0.97%
```

## DB 价格口径复核（候选短名单）
```text
Source          Variant                              EnterN  DropN  NetLabel10D  NetDB5D   NetDB10D  NetDB20D
--------------  -----------------------------------  ------  -----  -----------  --------  --------  --------
L2_old_covfix   sign_b0.010_p0.005_band0.001             176    176       +2.00%    -0.06%    +2.21%    +3.20%
L2_old_covfix   sign_b0.020_p0.005_band0.001             253    253       +1.76%    -0.07%    +1.70%    +2.22%
L2_old_covfix   sign_b0.025_p0.005_band0.001             291    291       +1.62%    -0.07%    +1.39%    +2.97%
L2_old_covfix   sign_b0.005_p0.005_band0.001             148    148       +1.50%    -0.12%    +1.77%    +3.36%
L2_old_covfix   original                                1115   1115       +0.93%    +0.22%    +0.88%    +1.58%
L3_dynamic_pup  original                                 713    713       +0.06%    -0.79%    -0.53%    -1.08%
```

## 初步优化结论
- L2 old covfix 的离散状态信号仍是最有价值的起点；变体搜索显示可以通过重新映射 boost/penalty 强度提升 Top50 替换 label。
- L3 dynamic PUP 即使做 boost-only、阈值、非对称缩放、分位数稀疏化，稳健候选仍弱于 L2；不建议继续用简单缩放抢救该版本。
- 下一步候选应聚焦 old covfix 的 coefficient mapping，而不是继续扩大 dynamic PUP 搜索。

## 推荐候选
- Primary: `sign_b0.020_p0.005_band0.001`，即 old covfix 方向不变，但映射为 trending=`1.020`、fading=`0.995`、neutral=`1.000`；它在 train/test 都为正，稳健性最好。
- Alternate: `sign_b0.010_p0.005_band0.001`，DB10D/DB20D 更强，但 test 段 label 较弱，适合作为第二候选。
- Sparse: `sign_b0.005_p0.005_band0.001`，替换更少，DB20D 强，但可能对组合收益影响过小。

## 候选 coefficient artifact（未注册、未改策略）
- Primary: `F:\Dev\AIstock\.codex_tmp\hmm_offline_diag\qe_20260502_131502_9b54\optimization\candidate_coefficients\primary_balanced_b0p020_p0p005.json`
- Alternate: `F:\Dev\AIstock\.codex_tmp\hmm_offline_diag\qe_20260502_131502_9b54\optimization\candidate_coefficients\high_db10_b0p010_p0p005.json`
- Sparse: `F:\Dev\AIstock\.codex_tmp\hmm_offline_diag\qe_20260502_131502_9b54\optimization\candidate_coefficients\sparse_b0p005_p0p005.json`
- 这些文件只是离线候选产物，尚未写入 HMM registry，也没有被任何 QE/交易策略引用。

## 产物
- 全量网格: `F:\Dev\AIstock\.codex_tmp\hmm_offline_diag\qe_20260502_131502_9b54\optimization\candidate_grid_label_metrics.csv`
- 候选短名单: `F:\Dev\AIstock\.codex_tmp\hmm_offline_diag\qe_20260502_131502_9b54\optimization\candidate_shortlist_label_metrics.csv`
- DB 复核明细: `F:\Dev\AIstock\.codex_tmp\hmm_offline_diag\qe_20260502_131502_9b54\optimization\selected_candidate_replacements_with_db_returns.csv`
- DB 复核摘要: `F:\Dev\AIstock\.codex_tmp\hmm_offline_diag\qe_20260502_131502_9b54\optimization\selected_candidate_db_return_summary.csv`
