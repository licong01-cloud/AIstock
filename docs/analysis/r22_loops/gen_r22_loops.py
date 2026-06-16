"""R22 loop generator — 锁定 α5(MARG10) + 决定 α4(VOL12)去留 + 补集成第二模型(TCN/树).

GPU(cuda, 序列): G1 MARG10×LSTM锁 / G2 VOL12×TCN救 / G3 FundVal×TCN集成 / G4 Flow×TCN集成
CPU(cpu, 树):   H1 MARG10×LGBM_C锁 / H2 VOL12×CatBoost救 / H3 FundVal×golden / H4 FM12+×golden
因子集与 R21 一致(零漂移). 锁定配置 topk25/nd2/h20/V25_1_SMALL_CAP/filtered_pool_20260428/no-HMM/10M.
"""
import json, os

FM12_PLUS = ["FundamentalEpsIndustryMomentum","bb_cp_momentum","roe_stability_score","book_value_price_ratio",
    "neg_funds_flow_efficiency_ratio","dynamic_valuation_factor","neg_momentum_price_volume","m_turnover_percentile_250d",
    "neg_high_amount_turnover_momentum_5d","industry_stock_momentum_diff_10d","m_ind_pb_rel_mom","m_ind_neutral_rev_5d",
    "m_turnover_abnormal_20d","m_turnover_zscore_60d","m_atr_compression","m_free_turnover_ind_neutral",
    "dynamic_flow_volatility_sentiment","neg_volatility_breakout_momentum_v2","ChipWinnerRateEliteBuyIntensity",
    "LargeOrder_Cost_Interaction","m_ind_rel_turnover","m_free_turnover_rate","Industry_Volatility_Liquidity_Cross_Factor",
    "neg_TurnoverVolatilityEnhancement"]
FLOW12 = ["dynamic_flow_volatility_sentiment","LargeOrder_Cost_Interaction","neg_Composite_Factor_Multi_Dim",
    "ChipWinnerRateEliteBuyIntensity","neg_Elite_Sell_Size_Adjusted","neg_mf_main_net_amt_std_5d",
    "bid_ask_spread_change_factor","SmallOrderIntensityBreakoutFactor",
    "Industry_Extra_Large_Buy_Individual_Extra_Large_Sell_Strength_Ratio","small_order_flow_intensity",
    "MF_Stability_Enhanced_5D","m_turnover_mf_divergence"]
MARG10 = ["m_md_rz_rq_sentiment","dynamic_flow_volatility_sentiment","ChipWinnerRateEliteBuyIntensity",
    "sentiment_order_imbalance","m_turnover_mf_divergence","m_gap_frequency_20d","neg_Composite_Factor_Multi_Dim",
    "dynamic_valuation_factor","bid_ask_spread_change_factor","small_order_flow_intensity"]
VOL12 = ["m_atr_compression","m_vol_of_vol_20d","m_idio_vol_60d","neg_momentum_volatility_ratio",
    "liquidity_adjusted_volatility","neg_volatility_breakout_momentum_v2","neg_volatility_10D","m_intraday_range_compress",
    "m_tech_atr_ratio_14d","m_ind_residual_vol_ratio","neg_turnover_adjusted_volatility","conditional_momentum_volatility"]
FUNDVAL12 = ["neg_PBTurnoverInteractionStd","Value_PBInv_Momentum_20D","neg_Value_Liquidity_Adjustment",
    "roe_stability_score","neg_Market_Cap_Adjusted_Momentum","dynamic_valuation_factor","Price_Deviation_Historical_High",
    "DividendToFreeTurnover_Ratio","Price_ChipNormalized_Position","book_value_price_ratio","cost_pressure_winner_rate",
    "Valuation_Cost_Deviation"]

LSTM="__seed_LSTM_10D_hs64_d02__"; TCN="__seed_TCN_10D_d02__"
LGBMC="__seed_LGBModel_conservative_v1__"; GOLDEN="__seed_LGBModel_golden_v1__"; CAT="__seed_CatBoost_10D__"
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
        "seed_policy":"fixed","design_round":"R22","archive_policy":"AUTO"},"execution_algo":"V25_1_SMALL_CAP",
        "strategy_params":dict(SP),"execution_algo_params":ep(dev),"stock_pool":"filtered_pool_20260428",
        "backtest_only":False,"loop_index":idx}

# (group, factors, model, device, seeds, objectives)
GPU_SPEC=[("a5_marg_lstm",MARG10,LSTM,"cuda",[888,7,88],["alpha5_lock","margin_sentiment"]),
          ("a4_vol_tcn",VOL12,TCN,"cuda",[42,2024,2026],["alpha4_salvage","volatility_domain"]),
          ("a6_fundval_tcn",FUNDVAL12,TCN,"cuda",[42,2024,2026],["alpha6_ensemble","fundamental_value"]),
          ("a7_flow_tcn",FLOW12,TCN,"cuda",[42,2024,2026],["alpha7_ensemble","micro_flow"])]
CPU_SPEC=[("a5_marg_lgbmc",MARG10,LGBMC,"cpu",[999,111,333],["alpha5_lock","margin_sentiment"]),
          ("a4_vol_catboost",VOL12,CAT,"cpu",[42,2024,2026],["alpha4_salvage","volatility_domain"]),
          ("a6_fundval_golden",FUNDVAL12,GOLDEN,"cpu",[42,2024,2026],["alpha6_alt_tree","fundamental_value"]),
          ("a3_fm12_golden",FM12_PLUS,GOLDEN,"cpu",[42,2024,2026],["alpha3_tree","fundamental_momentum"])]

def build(spec,node):
    out=[]; i=1
    for grp,fac,model,dev,seeds,obj in spec:
        short=grp.split("_")[0].upper()
        for s in seeds:
            out.append(loop(i,f"R22-{short} {grp} s{s}",node,model,dev,fac,grp,s,obj)); i+=1
    return out

gpu=build(GPU_SPEC,GPU); cpu=build(CPU_SPEC,CPU)
outdir=os.path.dirname(os.path.abspath(__file__))
json.dump(gpu,open(os.path.join(outdir,"r22_gpu_loops.json"),"w",encoding="utf-8"),ensure_ascii=False,indent=1)
json.dump(cpu,open(os.path.join(outdir,"r22_cpu_loops.json"),"w",encoding="utf-8"),ensure_ascii=False,indent=1)
assert len(gpu)==12 and len(cpu)==12
for arr,dev in [(gpu,"cuda"),(cpu,"cpu")]:
    for k,lp in enumerate(arr):
        assert lp["loop_index"]==k+1 and lp["execution_algo_params"]["device"]==dev and len(lp["factor_keys"]) in (10,12,24)
print("GPU:");
for lp in gpu: print(f"  L{lp['loop_index']:2d} {lp['runtime_flags']['group']:18} s{lp['runtime_flags']['random_seed']:<5} {len(lp['factor_keys'])}f {lp['model_id']}")
print("CPU:");
for lp in cpu: print(f"  L{lp['loop_index']:2d} {lp['runtime_flags']['group']:18} s{lp['runtime_flags']['random_seed']:<5} {len(lp['factor_keys'])}f {lp['model_id']}")
print("OK",outdir)
