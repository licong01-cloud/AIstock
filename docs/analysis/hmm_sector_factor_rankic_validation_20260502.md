# HMM 板块因子 RankIC 离线验证报告（2026-05-02）

## 结论摘要

- 本次没有修改交易策略或 HMM 运行时代码，只用本地 DB 中 `market.sw_daily`、`market.sector_data`、`market.daily_basic` 和因子库指标做只读离线验证。
- 验证目标是把因子库里可板块化的高 RankIC 因子迁移成板块特征，先看 5D/10D 板块未来收益 RankIC 和 top-bottom spread，再决定是否进入 HMM emission / 校准模型。
- 方向选择使用训练段均值 RankIC，持出段从 `test_start` 开始，避免用全样本符号直接挑方向。
- 该报告是第一轮实践验证；如果某个特征在 5D/10D 持出段 RankIC 和 spread 同时为正，才值得进入下一轮 HMM 候选特征集合。

## 数据健康

```text
Item                 Value                  
-------------------  -----------------------
sector_days          804                    
sector_count_latest  124                    
daily_rankic_rows    55296                  
summary_rows         100                    
eval_window          2024-01-01 ~ 2026-04-30
holdout_window       2025-05-01 ~ 2026-04-30
```

## 可板块化因子来源

```text
StockFactor                        Cat       StockRankIC  BestH  SectorFeatures                                                                              
---------------------------------  --------  -----------  -----  --------------------------------------------------------------------------------------------
neg_mf_main_net_amt_std_5d         MF        0.0570       20     sf_mf_net_ratio_std_5d_neg,sf_flow_stability_5d                                             
m_turnover_mf_divergence           MF/LIQ    0.0442       20     sf_turnover_mf_divergence_10d                                                               
dynamic_flow_volatility_sentiment  MF/VOL    0.0518       20     sf_dynamic_flow_vol_sentiment                                                               
small_order_flow_intensity         MF        0.0452       20     sf_small_buy_intensity_5d,sf_small_net_ratio_5d                                             
m_free_turnover_ind_neutral        LIQ       0.0541       20     sf_turnover_pctile_250d_neg,sf_turnover_zscore_60d_neg                                      
m_ind_rel_turnover                 LIQ       0.0427       20     sf_turnover_pctile_120d_neg,sf_turnover_ma5_ma20_neg                                        
m_intraday_range_ratio_5d          VOL       0.0589       20     sf_intraday_range_5d_neg                                                                    
m_atr_percentile_250d              VOL/STAT  0.0550       20     sf_atr14_pctile_250d_neg                                                                    
m_sw2_vol_ratio_to_sector          VOL       -0.0477      20     sf_range_vs_market_10d,sf_vol_vs_market_20d                                                 
m_max_return_20d                   MOM       0.0536       20     sf_max_ret_20d_neg                                                                          
m_mom_weighted_strength_20d        MOM       -0.0532      20     sf_amount_weighted_mom_20d                                                                  
sector_breadth_extension           BREADTH   NA           NA     sf_breadth_1d,sf_breadth_5d,sf_excess_breadth_5d,sf_median_stock_ret_5d,sf_dispersion_5d_neg
```

## 5D 持出段候选

```text
Feature                      Dir  TrainIC  TestIC  IC_t  Pos    TBSpread  TB_t   TBWin
---------------------------  ---  -------  ------  ----  -----  --------  -----  -----
sf_turnover_pctile_250d_neg  +    0.0587   0.0555  5.15  61.6%  0.246%    2.21   56.5%
sf_turnover_pctile_120d_neg  +    0.0473   0.0514  4.83  60.3%  0.181%    1.62   54.9%
sf_turnover_zscore_60d_neg   +    0.0521   0.0399  3.81  59.9%  0.164%    1.56   52.7%
sf_turnover_ma5_ma20_neg     +    0.0264   0.0342  3.18  57.8%  0.208%    1.94   52.7%
sf_atr14_pctile_250d_neg     +    0.0579   0.0303  2.19  54.9%  -0.054%   -0.38  46.0%
sf_mf_net_ratio_std_5d_neg   +    0.0004   0.0297  2.62  56.1%  0.220%    2.10   59.9%
sf_small_net_ratio_5d        +    0.0088   0.0257  3.08  61.6%  0.179%    2.38   56.1%
sf_amount_weighted_mom_20d   -    0.0480   0.0226  1.59  49.8%  -0.115%   -0.79  46.8%
sf_breadth_10d               -    0.0246   0.0207  1.67  54.4%  0.017%    0.13   48.1%
sf_flow_stability_5d         -    0.0145   0.0172  1.40  49.8%  0.040%    0.35   49.8%
sf_median_stock_ret_5d       -    0.0274   0.0167  1.14  51.1%  -0.012%   -0.08  48.1%
sf_breadth_5d                -    0.0307   0.0132  1.00  50.6%  0.011%    0.08   52.5%
```

## 10D 持出段候选

```text
Feature                        Dir  TrainIC  TestIC  IC_t  Pos    TBSpread  TB_t   TBWin
-----------------------------  ---  -------  ------  ----  -----  --------  -----  -----
sf_turnover_pctile_120d_neg    +    0.0485   0.0661  6.01  63.8%  0.395%    2.66   54.7%
sf_turnover_pctile_250d_neg    +    0.0675   0.0656  6.08  62.5%  0.414%    2.89   55.2%
sf_turnover_zscore_60d_neg     +    0.0570   0.0537  4.76  61.6%  0.347%    2.40   55.2%
sf_turnover_ma5_ma20_neg       +    0.0298   0.0494  4.95  63.4%  0.486%    4.03   61.2%
sf_mf_net_ratio_std_5d_neg     +    0.0060   0.0435  3.70  55.2%  0.434%    2.91   54.3%
sf_median_stock_ret_5d         -    0.0179   0.0324  2.19  56.0%  0.278%    1.42   49.1%
sf_atr14_pctile_250d_neg       +    0.0847   0.0297  2.26  56.0%  -0.138%   -0.74  53.0%
sf_breadth_5d                  -    0.0213   0.0209  1.56  53.4%  0.243%    1.38   51.5%
sf_excess_breadth_5d           -    0.0213   0.0209  1.56  53.4%  0.243%    1.38   51.5%
sf_flow_stability_5d           -    0.0042   0.0155  1.32  54.3%  0.066%    0.45   51.7%
sf_breadth_10d                 -    0.0159   0.0142  1.12  50.4%  0.052%    0.30   46.1%
sf_dynamic_flow_vol_sentiment  +    0.0940   0.0102  0.73  50.0%  -0.197%   -1.13  43.5%
```

## 产物

- `summary`: `.codex_tmp\sector_factor_rankic_20260502\sector_factor_summary.csv`
- `daily_rankic`: `.codex_tmp\sector_factor_rankic_20260502\sector_factor_daily_rankic.csv`
- `source_factor_map`: `.codex_tmp\sector_factor_rankic_20260502\sector_factor_source_map.csv`

## 初步可用信号

```text
Feature                        Horizon  TestIC   IC_t   TBSpread  TB_t   Action        Note                                                   
-----------------------------  -------  -------  -----  --------  -----  ------------  -------------------------------------------------------
sf_turnover_pctile_250d_neg    5D       0.0555   5.15   0.246%    2.21   core          5D/10D both stable; first HMM emission candidate       
sf_turnover_pctile_120d_neg    10D      0.0661   6.01   0.395%    2.66   core          strong 10D holdout RankIC and positive spread          
sf_turnover_ma5_ma20_neg       10D      0.0494   4.95   0.486%    4.03   core          best 10D top-bottom spread among turnover features     
sf_mf_net_ratio_std_5d_neg     10D      0.0435   3.70   0.434%    2.91   core          money-flow stability works after sector migration      
sf_small_net_ratio_5d          5D       0.0257   3.08   0.179%    2.38   secondary     positive 5D IC and spread; useful money-flow companion 
sf_flow_tier_strength_10d      20D      0.0657   7.10   0.782%    4.87   long-horizon  20D signal is strong; use only for long-horizon branch 
sf_dynamic_flow_vol_sentiment  10D      0.0102   0.73   -0.197%   -1.13  hold          train IC high but holdout spread negative; needs gating
sf_atr14_pctile_250d_neg       10D      0.0297   2.26   -0.138%   -0.74  hold          IC positive but spread negative; not standalone        
sf_max_ret_20d_neg             10D      -0.0036  -0.28  -0.716%   -4.07  reject        holdout spread is materially negative                  
```

## 20D 参考候选

```text
Feature                        Dir  TrainIC  TestIC  IC_t  Pos    TBSpread  TB_t   TBWin
-----------------------------  ---  -------  ------  ----  -----  --------  -----  -----
sf_turnover_pctile_120d_neg    +    0.0532   0.0711  6.25  61.3%  0.384%    1.87   55.0%
sf_flow_tier_strength_10d      -    0.0032   0.0657  7.10  66.7%  0.782%    4.87   61.7%
sf_turnover_pctile_250d_neg    +    0.0812   0.0606  5.16  58.1%  0.266%    1.26   48.2%
sf_turnover_zscore_60d_neg     +    0.0670   0.0551  4.81  57.7%  0.315%    1.54   55.0%
sf_turnover_ma5_ma20_neg       +    0.0230   0.0493  4.51  64.0%  0.488%    2.54   58.6%
sf_atr14_pctile_250d_neg       +    0.1079   0.0228  1.97  51.8%  -0.566%   -2.71  46.8%
sf_dynamic_flow_vol_sentiment  +    0.1195   0.0193  1.70  51.8%  -0.486%   -2.44  41.0%
sf_median_stock_ret_5d         -    0.0426   0.0189  1.25  53.2%  -0.090%   -0.31  50.5%
sf_flow_stability_5d           -    0.0058   0.0097  0.82  51.4%  -0.183%   -0.83  52.3%
sf_breadth_5d                  -    0.0363   0.0034  0.25  51.4%  -0.300%   -1.17  46.6%
sf_excess_breadth_5d           -    0.0363   0.0034  0.25  51.4%  -0.300%   -1.17  46.6%
sf_breadth_1d                  -    0.0071   0.0016  0.11  48.6%  0.001%    0.00   47.3%
```

## 对 HMM 优化的直接含义

- 当前第一优先级不应继续微调 latest dynamic PUP 的系数缩放，而应先把 HMM 的板块状态输入换成有持出段证据的板块因子。
- 第一组候选是换手拥挤/降温类：`sf_turnover_pctile_250d_neg`、`sf_turnover_pctile_120d_neg`、`sf_turnover_zscore_60d_neg`、`sf_turnover_ma5_ma20_neg`。
- 第二组候选是资金流稳定类：`sf_mf_net_ratio_std_5d_neg`、`sf_small_net_ratio_5d`，20D 可额外观察 `sf_flow_tier_strength_10d`。
- 暂不建议把 `sf_dynamic_flow_vol_sentiment`、`sf_atr14_pctile_250d_neg`、`sf_max_ret_20d_neg` 作为单独强信号接入：它们存在持出段 spread 弱或为负的问题。
- 5D/10D 的可用信号明显强于 1D，说明 HMM 更适合作为中短周期板块状态/系数校准器，而不是直接做明日板块涨幅排名器。
- 后续实践建议保持策略不变，先离线生成基于这些板块因子的候选 sector coefficient，并复用现有 Top50 replacement 诊断框架验证进入/剔除股票的净收益。

## 解释口径

- `Dir=+` 表示沿用该板块特征原始方向；`Dir=-` 表示训练段显示需要取反。
- `TestIC` 是方向调整后的持出段日均 RankIC；`TBSpread` 是每天 top20% 板块未来收益均值减 bottom20% 后再求均值。
- 该验证使用当前静态申万二级成分映射计算资金流和个股 breadth，历史成分迁移会带来小幅噪声；后续若进入生产特征，应升级为按 `in_date/out_date` 的 PIT 成分映射。