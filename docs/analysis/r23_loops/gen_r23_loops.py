"""R23 loop generator — 基石整合(α1上树/扩种子) + 最优腿可部署级锁定(n=6) + α3稳健模型搜索.

依据 R21+R22 全覆盖复盘:
  α5 MARG10 已双模型×6seed锁定 → 本轮不跑(出策略包).
  α7 Flow 三模型(LSTM/conservative/TCN)齐全但 MDD 均 -0.18~-0.19(信号本性)→ 不靠加模型, 留给组合层/风控.
  α1 PLUS 是 leaderboard 最强腿(23f≈1.08 / 26f≈1.12 @topk25) 但仅 LSTM, 从未上树 → 本轮补树+扩种子.
  α4 VOL12×TCN(R22 0.80/CV0.04/MDD-0.148 最优防御腿) n=3 → 扩到 n=6 锁定.
  α6 FundVal×TCN(R22 0.85/ICIR0.88 最优) + ×golden(0.75) → 各扩到 n=6 锁定.
  α3 FM12+ LSTM(变动0.80-0.97) golden(稳但低0.69) → 补 TCN 找"稳且高" + conservative 第二树.

GPU(cuda): G1 α1_PLUS26×LSTM扩种子 / G2 α4_VOL12×TCN锁 / G3 α6_FundVal×TCN锁 / G4 α3_FM12×TCN搜
CPU(树):   H1 α1_PLUS3×LGBM_C(上树) / H2 α1_PLUS3×golden(上树2) / H3 α3_FM12×conservative / H4 α6_FundVal×golden锁
锁定配置 topk25/nd2/h20/V25_1_SMALL_CAP/filtered_pool_20260428/no-HMM/10M (与 R22 一致).
"""
import json, os

# α1 锚: 真实因子集(数仓 ddb6 L12 / 1f70 L2 复核)
PLUS3_23 = ["neg_momentum_price_volume","m_turnover_percentile_250d","neg_high_amount_turnover_momentum_5d",
    "industry_stock_momentum_diff_10d","neg_Composite_Factor_Multi_Dim","m_intraday_range_60d_min_ratio",
    "m_ind_pb_rel_mom","m_ind_neutral_rev_5d","m_turnover_abnormal_20d","m_turnover_accel","m_turnover_zscore_60d",
    "m_atr_compression","m_free_turnover_ind_neutral","dynamic_flow_volatility_sentiment",
    "neg_volatility_breakout_momentum_v2","ChipWinnerRateEliteBuyIntensity","LargeOrder_Cost_Interaction",
    "m_ind_rel_turnover","m_turnover_breakout_ratio","m_volume_contraction","neg_PriceMomentum20D",
    "m_free_turnover_rate","Industry_Volatility_Liquidity_Cross_Factor"]
PLUS26 = PLUS3_23 + ["m_intraday_range_ratio_5d","neg_TurnoverVolatilityEnhancement","m_tech_atr_ratio_14d"]

FM12_PLUS = ["FundamentalEpsIndustryMomentum","bb_cp_momentum","roe_stability_score","book_value_price_ratio",
    "neg_funds_flow_efficiency_ratio","dynamic_valuation_factor","neg_momentum_price_volume","m_turnover_percentile_250d",
    "neg_high_amount_turnover_momentum_5d","industry_stock_momentum_diff_10d","m_ind_pb_rel_mom","m_ind_neutral_rev_5d",
    "m_turnover_abnormal_20d","m_turnover_zscore_60d","m_atr_compression","m_free_turnover_ind_neutral",
    "dynamic_flow_volatility_sentiment","neg_volatility_breakout_momentum_v2","ChipWinnerRateEliteBuyIntensity",
    "LargeOrder_Cost_Interaction","m_ind_rel_turnover","m_free_turnover_rate","Industry_Volatility_Liquidity_Cross_Factor",
    "neg_TurnoverVolatilityEnhancement"]
VOL12 = ["m_atr_compression","m_vol_of_vol_20d","m_idio_vol_60d","neg_momentum_volatility_ratio",
    "liquidity_adjusted_volatility","neg_volatility_breakout_momentum_v2","neg_volatility_10D","m_intraday_range_compress",
    "m_tech_atr_ratio_14d","m_ind_residual_vol_ratio","neg_turnover_adjusted_volatility","conditional_momentum_volatility"]
FUNDVAL12 = ["neg_PBTurnoverInteractionStd","Value_PBInv_Momentum_20D","neg_Value_Liquidity_Adjustment",
    "roe_stability_score","neg_Market_Cap_Adjusted_Momentum","dynamic_valuation_factor","Price_Deviation_Historical_High",
    "DividendToFreeTurnover_Ratio","Price_ChipNormalized_Position","book_value_price_ratio","cost_pressure_winner_rate",
    "Valuation_Cost_Deviation"]

LSTM="__seed_LSTM_10D_hs64_d02__"; TCN="__seed_TCN_10D_d02__"
LGBMC="__seed_LGBModel_conservative_v1__"; GOLDEN="__seed_LGBModel_golden_v1__"
GPU="wsl2-5080"; CPU="rdagent-node1"

RISK = {"enabled":True,"providers":["st_pit"],"hard_actions":["block_buy","force_exit"],
    "score_overlay":{"enabled":False,"positive_multiplier_cap":1.1,"negative_multiplier_floor":0.7},
    "policy_version":"stock_event_risk_policy_v1","st_universe_key":"shsz_st_pit_active_v1",
    "strict_data_ready":True,"visible_time_mode":"next_trading_session"}
SP = {"topk":25,"n_drop":2,"max_n_drop":2,"max_weight":0.05,"stock_pool":"filtered_pool_20260428","risk_policy":RISK,
    "initial_cash":10000000,"label_horizon":20,"weight_method":"softmax","unfilled_handler":"TAIL_SUBSTITUTE",
    "max_position_ratio":0.95,"unfilled_backup_depth":15,"max_single_order_value":5000000,"sector_blacklist_enabled":True}
def ep(dev): return {"device":dev,"min_cost":5,"max_buckets":12,"tolerance_bps":10,"commission_rate":0.00025,
    "late_model_path":"/home/lc999/data/rl_models/v25/v25_late_net_joint_fixed.pt",
    "early_model_path":"/home/lc999/data/rl_models/v25/v25_early_net_joint_fixed.pt"}
def loop(idx,label,node,model,dev,fac,grp,seed,obj):
    return {"label":label,"node_id":node,"model_id":model,"factor_keys":list(fac),"strategy_id":"score_weighted_topk_v2",
        "label_horizon":20,"runtime_flags":{"group":grp,"loop_desc":label,"objectives":obj,"random_seed":seed,
        "seed_policy":"fixed","design_round":"R23","archive_policy":"AUTO"},"execution_algo":"V25_1_SMALL_CAP",
        "strategy_params":dict(SP),"execution_algo_params":ep(dev),"stock_pool":"filtered_pool_20260428",
        "backtest_only":False,"loop_index":idx}

# (group, factors, model, device, seeds, objectives) — 种子去碰撞: 见下注释
GPU_SPEC=[("a1_plus26_lstm",PLUS26,LSTM,"cuda",[333,555,777],["alpha1_anchor","seed_expand"]),   # R14已跑888/2026, 用新种子
          ("a4_vol_tcn",VOL12,TCN,"cuda",[888,7,88],["alpha4_lock","defensive_leg"]),            # R22用42/2024/2026 → n=6
          ("a6_fundval_tcn",FUNDVAL12,TCN,"cuda",[888,7,88],["alpha6_lock","best_model"]),       # R22用42/2024/2026 → n=6
          ("a3_fm12_tcn",FM12_PLUS,TCN,"cuda",[42,2024,2026],["alpha3_model_search","stable_high"])]  # FM12×TCN全新
CPU_SPEC=[("a1_plus3_lgbmc",PLUS3_23,LGBMC,"cpu",[999,111,333],["alpha1_tree","ensemble_feasibility"]),   # α1从未上树
          ("a1_plus3_golden",PLUS3_23,GOLDEN,"cpu",[42,2024,2026],["alpha1_tree","ensemble_feasibility"]), # α1树变体2
          ("a3_fm12_conservative",FM12_PLUS,LGBMC,"cpu",[999,111,333],["alpha3_tree2","ensemble_diversity"]), # golden已, 补conservative
          ("a6_fundval_golden",FUNDVAL12,GOLDEN,"cpu",[888,7,88],["alpha6_lock","tree_side"])]    # R22用42/2024/2026 → n=6

def build(spec,node):
    out=[]; i=1
    for grp,fac,model,dev,seeds,obj in spec:
        short=grp.split("_")[0].upper()
        for s in seeds:
            out.append(loop(i,f"R23-{short} {grp} s{s}",node,model,dev,fac,grp,s,obj)); i+=1
    return out

gpu=build(GPU_SPEC,GPU); cpu=build(CPU_SPEC,CPU)
outdir=os.path.dirname(os.path.abspath(__file__))
json.dump(gpu,open(os.path.join(outdir,"r23_gpu_loops.json"),"w",encoding="utf-8"),ensure_ascii=False,indent=1)
json.dump(cpu,open(os.path.join(outdir,"r23_cpu_loops.json"),"w",encoding="utf-8"),ensure_ascii=False,indent=1)
assert len(gpu)==12 and len(cpu)==12
for arr,dev in [(gpu,"cuda"),(cpu,"cpu")]:
    for k,lp in enumerate(arr):
        assert lp["loop_index"]==k+1 and lp["execution_algo_params"]["device"]==dev and len(lp["factor_keys"]) in (10,12,23,24,26)
print("GPU:")
for lp in gpu: print(f"  L{lp['loop_index']:2d} {lp['runtime_flags']['group']:20} s{lp['runtime_flags']['random_seed']:<5} {len(lp['factor_keys'])}f {lp['model_id']}")
print("CPU:")
for lp in cpu: print(f"  L{lp['loop_index']:2d} {lp['runtime_flags']['group']:20} s{lp['runtime_flags']['random_seed']:<5} {len(lp['factor_keys'])}f {lp['model_id']}")
print("OK",outdir)
