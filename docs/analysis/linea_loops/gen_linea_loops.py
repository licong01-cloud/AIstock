"""Line A (R21) loop generator — 多Alpha夯实+补样本.

生成两节点各 12 loops:
- GPU(wsl2-5080, LSTM): G1 FM12+ consolidate / G2 MARG10 probe / G3 VOL12 probe / G4 Flow12 production
- CPU(rdagent-node1, LGBM_C): H1 Flow12 / H2 FundVal12 / H3 MARG10 / H4 VOL12

因子集与 loop 结构取自真实已运行实验(edaf/433d/0daa)+ MA2 工件, 零漂移.
factor_keys 键经核实: experiment_config_builders.py:557 优先读取.
"""
import json
import os

# ---------- 因子集(精确, 来自真实 loop config) ----------
FM12_PLUS = [  # 24f, 源 R19A 0daa L8 (α3 基本面动量)
    "FundamentalEpsIndustryMomentum", "bb_cp_momentum", "roe_stability_score",
    "book_value_price_ratio", "neg_funds_flow_efficiency_ratio", "dynamic_valuation_factor",
    "neg_momentum_price_volume", "m_turnover_percentile_250d", "neg_high_amount_turnover_momentum_5d",
    "industry_stock_momentum_diff_10d", "m_ind_pb_rel_mom", "m_ind_neutral_rev_5d",
    "m_turnover_abnormal_20d", "m_turnover_zscore_60d", "m_atr_compression",
    "m_free_turnover_ind_neutral", "dynamic_flow_volatility_sentiment", "neg_volatility_breakout_momentum_v2",
    "ChipWinnerRateEliteBuyIntensity", "LargeOrder_Cost_Interaction", "m_ind_rel_turnover",
    "m_free_turnover_rate", "Industry_Volatility_Liquidity_Cross_Factor", "neg_TurnoverVolatilityEnhancement",
]
FLOW12 = [  # 12f, 源 MA1 edaf L1 (α7 微观资金流)
    "dynamic_flow_volatility_sentiment", "LargeOrder_Cost_Interaction", "neg_Composite_Factor_Multi_Dim",
    "ChipWinnerRateEliteBuyIntensity", "neg_Elite_Sell_Size_Adjusted", "neg_mf_main_net_amt_std_5d",
    "bid_ask_spread_change_factor", "SmallOrderIntensityBreakoutFactor",
    "Industry_Extra_Large_Buy_Individual_Extra_Large_Sell_Strength_Ratio", "small_order_flow_intensity",
    "MF_Stability_Enhanced_5D", "m_turnover_mf_divergence",
]
MARG10 = [  # 10f, 源 MA2 工件/R20B (α5 融资融券情绪)
    "m_md_rz_rq_sentiment", "dynamic_flow_volatility_sentiment", "ChipWinnerRateEliteBuyIntensity",
    "sentiment_order_imbalance", "m_turnover_mf_divergence", "m_gap_frequency_20d",
    "neg_Composite_Factor_Multi_Dim", "dynamic_valuation_factor", "bid_ask_spread_change_factor",
    "small_order_flow_intensity",
]
VOL12 = [  # 12f, 源 MA2 工件/R20B (α4 波动率)
    "m_atr_compression", "m_vol_of_vol_20d", "m_idio_vol_60d", "neg_momentum_volatility_ratio",
    "liquidity_adjusted_volatility", "neg_volatility_breakout_momentum_v2", "neg_volatility_10D",
    "m_intraday_range_compress", "m_tech_atr_ratio_14d", "m_ind_residual_vol_ratio",
    "neg_turnover_adjusted_volatility", "conditional_momentum_volatility",
]
FUNDVAL12 = [  # 12f, 源 MA1 433d L11 (α6 基本面估值)
    "neg_PBTurnoverInteractionStd", "Value_PBInv_Momentum_20D", "neg_Value_Liquidity_Adjustment",
    "roe_stability_score", "neg_Market_Cap_Adjusted_Momentum", "dynamic_valuation_factor",
    "Price_Deviation_Historical_High", "DividendToFreeTurnover_Ratio", "Price_ChipNormalized_Position",
    "book_value_price_ratio", "cost_pressure_winner_rate", "Valuation_Cost_Deviation",
]

RISK_POLICY = {
    "enabled": True, "providers": ["st_pit"], "hard_actions": ["block_buy", "force_exit"],
    "score_overlay": {"enabled": False, "positive_multiplier_cap": 1.1, "negative_multiplier_floor": 0.7},
    "policy_version": "stock_event_risk_policy_v1", "st_universe_key": "shsz_st_pit_active_v1",
    "strict_data_ready": True, "visible_time_mode": "next_trading_session",
}
STRATEGY_PARAMS = {
    "topk": 25, "n_drop": 2, "max_n_drop": 2, "max_weight": 0.05,
    "stock_pool": "filtered_pool_20260428", "risk_policy": RISK_POLICY,
    "initial_cash": 10000000, "label_horizon": 20, "weight_method": "softmax",
    "unfilled_handler": "TAIL_SUBSTITUTE", "max_position_ratio": 0.95,
    "unfilled_backup_depth": 15, "max_single_order_value": 5000000, "sector_blacklist_enabled": True,
}


def exec_params(device):
    return {
        "device": device, "min_cost": 5, "max_buckets": 12, "tolerance_bps": 10,
        "commission_rate": 0.00025,
        "late_model_path": "/home/lc999/data/rl_models/v25/v25_late_net_joint_fixed.pt",
        "early_model_path": "/home/lc999/data/rl_models/v25/v25_early_net_joint_fixed.pt",
    }


def make_loop(idx, label, node_id, model_id, device, factors, group, seed, objectives):
    return {
        "label": label,
        "node_id": node_id,
        "model_id": model_id,
        "factor_keys": list(factors),
        "strategy_id": "score_weighted_topk_v2",
        "label_horizon": 20,
        "runtime_flags": {
            "group": group, "loop_desc": label, "objectives": objectives,
            "random_seed": seed, "seed_policy": "fixed", "design_round": "R21", "archive_policy": "AUTO",
        },
        "execution_algo": "V25_1_SMALL_CAP",
        "strategy_params": dict(STRATEGY_PARAMS),
        "execution_algo_params": exec_params(device),
        "stock_pool": "filtered_pool_20260428",
        "backtest_only": False,
        "loop_index": idx,
    }


LSTM = "__seed_LSTM_10D_hs64_d02__"
LGBMC = "__seed_LGBModel_conservative_v1__"
GPU = "wsl2-5080"
CPU = "rdagent-node1"

# ---------- GPU 12 loops (LSTM) ----------
gpu = []
i = 1
for s in [7, 88, 111]:  # G1 α3 FM12+ consolidate n=3->n=6
    gpu.append(make_loop(i, f"LA-G1 FM12+ LSTM h20 s{s}", GPU, LSTM, "cuda", FM12_PLUS,
                         "a3_fm12_lstm", s, ["alpha3_consolidate", "fundamental_momentum", "multi_alpha_sourcing"])); i += 1
for s in [42, 2024, 2026]:  # G2 α5 MARG10 LSTM probe (GPU gap)
    gpu.append(make_loop(i, f"LA-G2 MARG10 LSTM h20 s{s}", GPU, LSTM, "cuda", MARG10,
                         "a5_marg_lstm", s, ["alpha5_discovery", "margin_sentiment", "multi_alpha_sourcing"])); i += 1
for s in [42, 2024, 2026]:  # G3 α4 VOL12 LSTM probe (GPU gap)
    gpu.append(make_loop(i, f"LA-G3 VOL12 LSTM h20 s{s}", GPU, LSTM, "cuda", VOL12,
                         "a4_vol_lstm", s, ["alpha4_discovery", "volatility_domain", "multi_alpha_sourcing"])); i += 1
for s in [7, 88, 111]:  # G4 α7 Flow12 LSTM production n=5->n=8
    gpu.append(make_loop(i, f"LA-G4 Flow12 LSTM h20 s{s}", GPU, LSTM, "cuda", FLOW12,
                         "a7_flow_lstm", s, ["alpha7_production", "micro_flow", "multi_alpha_sourcing"])); i += 1

# ---------- CPU 12 loops (LGBM_C) ----------
cpu = []
i = 1
for s in [7, 88, 111]:  # H1 α7 Flow12 LGBM_C n=5->n=8
    cpu.append(make_loop(i, f"LA-H1 Flow12 LGBMc h20 s{s}", CPU, LGBMC, "cpu", FLOW12,
                         "a7_flow_lgbmc", s, ["alpha7_production", "micro_flow", "multi_alpha_sourcing"])); i += 1
for s in [7, 88, 111]:  # H2 α6 FundVal12 LGBM_C n=5->n=8
    cpu.append(make_loop(i, f"LA-H2 FundVal12 LGBMc h20 s{s}", CPU, LGBMC, "cpu", FUNDVAL12,
                         "a6_fundval_lgbmc", s, ["alpha6_production", "fundamental_value", "multi_alpha_sourcing"])); i += 1
for s in [888, 7, 88]:  # H3 α5 MARG10 LGBM_C extend (R20B 4seed -> n=7)
    cpu.append(make_loop(i, f"LA-H3 MARG10 LGBMc h20 s{s}", CPU, LGBMC, "cpu", MARG10,
                         "a5_marg_lgbmc", s, ["alpha5_discovery", "margin_sentiment", "multi_alpha_sourcing"])); i += 1
for s in [888, 7, 88]:  # H4 α4 VOL12 LGBM_C extend (R20B 4seed -> n=7)
    cpu.append(make_loop(i, f"LA-H4 VOL12 LGBMc h20 s{s}", CPU, LGBMC, "cpu", VOL12,
                         "a4_vol_lgbmc", s, ["alpha4_discovery", "volatility_domain", "multi_alpha_sourcing"])); i += 1

outdir = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(outdir, "linea_gpu_lstm_loops.json"), "w", encoding="utf-8") as f:
    json.dump(gpu, f, ensure_ascii=False, indent=1)
with open(os.path.join(outdir, "linea_cpu_lgbmc_loops.json"), "w", encoding="utf-8") as f:
    json.dump(cpu, f, ensure_ascii=False, indent=1)

# 校验
assert len(gpu) == 12 and len(cpu) == 12
for arr, dev in [(gpu, "cuda"), (cpu, "cpu")]:
    for k, lp in enumerate(arr):
        assert lp["loop_index"] == k + 1
        assert lp["execution_algo_params"]["device"] == dev
        assert len(lp["factor_keys"]) in (10, 12, 24)
print("GPU loops:", len(gpu), "| seeds/groups:")
for lp in gpu:
    print(f"  L{lp['loop_index']:2d} {lp['runtime_flags']['group']:16s} s{lp['runtime_flags']['random_seed']:<6} {len(lp['factor_keys'])}f  {lp['model_id']}")
print("CPU loops:", len(cpu), "| seeds/groups:")
for lp in cpu:
    print(f"  L{lp['loop_index']:2d} {lp['runtime_flags']['group']:16s} s{lp['runtime_flags']['random_seed']:<6} {len(lp['factor_keys'])}f  {lp['model_id']}")
print("OK ->", outdir)
