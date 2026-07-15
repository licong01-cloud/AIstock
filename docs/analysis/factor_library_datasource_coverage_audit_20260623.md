# AIstock 因子库数据源覆盖审计（2026-06-23）

> 任务：BUG-483 / GitHub Issue #1488。读为主；未执行任何因子库元数据回填、DB DDL/DML、服务启动或重启。

## 0. 审计口径与证据

| 项目 | 值 |
| --- | --- |
| 连接库 | aistock / user=postgres / search_path="$user", public |
| 因子目录总数 | 778 |
| 可用因子数 | 575 |
| rdagent_task_sync 总数/可用 | 548 / 377 |
| 可用因子 data_source NULL | 551 |
| 可用因子 factor_type NULL | 555 |
| 本轮实际分类对象 | 575 |
| unknown/低置信待复核 | unknown=1, low_confidence=0 |
| 高相关(\|corr\|>=0.8)样本对 | 992 |

说明：`data_source/factor_type/mechanism` 是本轮从 `code_text/expression/formula_hint` 的精确字段字面量、`factor_name` 与既有 `qe_factor_classification` 辅助信息推断出的 **proposal**，不是已写回元数据。`daily_pv.h5` 只作为索引/对齐文件时不计入 `price_volume`；只有公式实际引用 `open/close/high/low/volume/amount/$close` 等字段才计入。推断不出的因子显式标为 `unknown`，不静默猜测。

## 1. 原始数据源清单（可用原料全集）

覆盖范围包含 `Tushare DatasetSpec`、`market.data_stats_config / DATASET_TABLE_MAP` 中的 TDX_DB 表，以及 QE/RDAgent 导出的 H5/parquet 原料。`latest/coverage` 来自只读 `market.dataset_date_refresh_audit`；缺失表示该源未纳入审计表或非按日数据。

| 数据源 | 来源 | API/接口 | 落地表/文件 | 模式 | 主键/日期 | 关键字段（截断） | 最新 | 覆盖率 | 质量 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| qlib::bak_basic.h5 | Qlib/RDAgent exported feature store | local H5/parquet | qlib_snapshots/* | file | datetime,instrument | bb_pe_dyn, bb_total_assets, bb_liquid_assets, bb_fixed_assets, bb_reserved, bb_reserved_pershare, bb_eps, bb_bvps, bb_undp, bb_per_undp, bb_rev_yoy, bb_profit_yoy | - | - | - |
| qlib::cyq_perf.h5 | Qlib/RDAgent exported feature store | local H5/parquet | qlib_snapshots/* | file | datetime,instrument | cp_his_low, cp_his_high, cp_cost_5pct, cp_cost_15pct, cp_cost_50pct, cp_cost_85pct, cp_cost_95pct, cp_weight_avg, cp_winner_rate | - | - | - |
| qlib::daily_basic.h5 | Qlib/RDAgent exported feature store | local H5/parquet | qlib_snapshots/* | file | datetime,instrument | db_close, db_turnover_rate, db_turnover_rate_f, db_volume_ratio, db_pe, db_pe_ttm, db_pb, db_ps, db_ps_ttm, db_dv_ratio, db_dv_ttm, db_total_share | - | - | - |
| qlib::daily_pv.h5 | Qlib/RDAgent exported feature store | local H5/parquet | qlib_snapshots/* | file | datetime,instrument | open, close, high, low, volume, factor, amount | - | - | - |
| qlib::moneyflow.h5 | Qlib/RDAgent exported feature store | local H5/parquet | qlib_snapshots/* | file | datetime,instrument | mf_sm_buy_vol, mf_sm_buy_amt, mf_sm_sell_vol, mf_sm_sell_amt, mf_md_buy_vol, mf_md_buy_amt, mf_md_sell_vol, mf_md_sell_amt, mf_lg_buy_vol, mf_lg_buy_amt, mf_lg_sell_vol, mf_lg_sell_amt | - | - | - |
| qlib::sector_data.h5 | Qlib/RDAgent exported feature store | local H5/parquet | qlib_snapshots/* | file | datetime,instrument | sw2_open, sw2_high, sw2_low, sw2_close, sw2_pct_change, sw2_vol, sw2_amount, sw2_pe, sw2_pb, sw2_total_mv, sw2_mf_buy_sm_amt, sw2_mf_sell_sm_amt | - | - | - |
| qlib::static_factors.parquet | Qlib/RDAgent exported feature store | local H5/parquet | qlib_snapshots/* | file | datetime,instrument | static_factors | - | - | - |
| kline_daily_raw | TDX_DB/data_stats | - | market.kline_daily_raw | table | trade_date | open_li, high_li, low_li, close_li, volume_hand, amount_li, adjust_type, source | 2026-06-22 | - | ok |
| kline_minute_raw | TDX_DB/data_stats | - | market.kline_minute_raw | table | trade_time | freq, open_li, high_li, low_li, close_li, volume_hand, amount_li, adjust_type, source | 2026-06-22 | - | ok |
| sector_data | TDX_DB/data_stats | - | market.sector_data | table | trade_date | sw2_open, sw2_high, sw2_low, sw2_close, sw2_pct_change, sw2_vol, sw2_amount, sw2_pe, sw2_pb, sw2_total_mv, sw2_mf_buy_sm_amt, sw2_mf_sell_sm_amt | 2026-06-22 | - | ok |
| stock_moneyflow_ts | TDX_DB/data_stats | - | market.moneyflow_ts | table | trade_date | buy_sm_vol, buy_sm_amount, sell_sm_vol, sell_sm_amount, buy_md_vol, buy_md_amount, sell_md_vol, sell_md_amount, buy_lg_vol, buy_lg_amount, sell_lg_vol, sell_lg_amount | 2026-06-22 | - | ok |
| anns_d | TDX_DB/data_stats_config | - | market.anns | table | ann_date | name, title, url, rec_time, local_path, file_ext, file_size, file_hash, download_status, created_at, updated_at, first_seen_at | - | - | - |
| cyq_chips | TDX_DB/data_stats_config | - | market.cyq_chips | table | trade_date | price, percent, created_at, updated_at | - | - | - |
| index_basic | TDX_DB/data_stats_config | - | market.index_basic | table | list_date | name, fullname, market, publisher, index_type, category, base_date, base_point, weight_rule, desc, exp_date | - | - | - |
| sina_board_daily | TDX_DB/data_stats_config | - | market.sina_board_daily | table | trade_date | cate_type, board_code, board_name, pct_chg, amount, net_inflow, turnover, ratioamount, meta | - | - | - |
| sina_board_intraday | TDX_DB/data_stats_config | - | market.sina_board_intraday | table | ts | cate_type, board_code, board_name, pct_chg, amount, net_inflow, turnover, ratioamount, meta | - | - | - |
| stock_universe_pit_events | TDX_DB/data_stats_config | - | market.stock_universe_pit_events | table | action_date | event_id, universe_key, event_kind, source, source_pub_date, source_imp_date, source_effective_date, st_type, st_reason, st_explain, terminal, rule_version | - | - | - |
| stock_universe_pit_spans | TDX_DB/data_stats_config | - | market.stock_universe_pit_spans | table | eligible_end | universe_key, eligible_start, entry_reason, exit_reason, base_list_date, ipo_eligible_date, entry_event_date, exit_event_date, terminal_exit, rule_version, generated_at, metadata | - | - | - |
| stock_universe_pit_state | TDX_DB/data_stats_config | - | market.stock_universe_pit_state | table | end_date | universe_key, rule_version, scope, start_date, status, dirty, source_fingerprint, source_fingerprint_sha256, last_build_summary, last_error, generated_at, updated_at | - | - | - |
| trading_calendar | TDX_DB/data_stats_config | - | market.trading_calendar | table | cal_date | is_trading | - | - | - |
| xtquant_pershare_index | TDX_DB/data_stats_config | - | market.xtquant_pershare_index | table | report_date | ann_date, s_fa_ocfps, s_fa_bps, s_fa_eps_basic, s_fa_eps_diluted, s_fa_undistributedps, s_fa_surpluscapitalps, adjusted_earnings_per_share, du_return_on_equity, sales_gross_profit, equity_roe, net_roe | - | - | - |
| adj_factor | Tushare DatasetSpec | adj_factor | market.adj_factor | by_date | trade_date, ts_code | adj_factor | 2026-06-22 | - | ok |
| bak_basic | Tushare DatasetSpec | bak_basic | market.bak_basic | by_date | trade_date, ts_code | name, industry, area, pe_dyn, total_assets, liquid_assets, fixed_assets, reserved, reserved_pershare, eps, bvps, list_date | 2026-06-22 | - | ok |
| cyq_perf | Tushare DatasetSpec | cyq_perf | market.cyq_perf | by_date | trade_date, ts_code | his_low, his_high, cost_5pct, cost_15pct, cost_50pct, cost_85pct, cost_95pct, weight_avg, winner_rate | 2026-06-22 | - | ok |
| daily_basic | Tushare DatasetSpec | daily_basic | market.daily_basic | by_date | trade_date, ts_code | close, turnover_rate, turnover_rate_f, volume_ratio, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm, total_share | 2026-06-22 | - | ok |
| index_daily | Tushare DatasetSpec | index_daily | market.index_daily | by_code | ts_code, trade_date | close, open, high, low, pre_close, change, pct_chg, vol, amount | 2026-06-22 | - | ok |
| margin_detail | Tushare DatasetSpec | margin_detail | market.margin_detail | by_date | trade_date, ts_code | rzye, rqye, rzmre, rqyl, rzche, rqchl, rqmcl, rzrqye | 2026-06-18 | - | ok |
| stk_limit | Tushare DatasetSpec | stk_limit | market.stk_limit | by_date | trade_date, ts_code | pre_close, up_limit, down_limit | 2026-06-22 | - | ok |
| stock_basic | Tushare DatasetSpec | stock_basic | market.stock_basic | single_call | ts_code | symbol, name, area, industry, fullname, enname, market, exchange, curr_type, list_status, list_date, delist_date | - | - | - |
| stock_st | Tushare DatasetSpec | stock_st | market.stock_st | by_date | ts_code, ann_date | start_date, end_date, market, exchange | 2026-06-22 | - | ok |
| stock_st_events | Tushare DatasetSpec | st | market.stock_st_events | by_date | ts_code, pub_date, imp_date, st_type | name, st_reason, st_explain | 2026-06-22 | - | empty_valid |
| suspend_d | Tushare DatasetSpec | suspend_d | market.suspend_d | by_date | trade_date, ts_code, suspend_type | suspend_timing | 2026-06-24 | - | ok |
| sw_daily | Tushare DatasetSpec | sw_daily | market.sw_daily | by_code | ts_code, trade_date | name, open, low, high, close, change, pct_change, vol, amount, pe, pb, float_mv | 2026-06-22 | - | ok |
| sw_index_classify | Tushare DatasetSpec | index_classify | market.sw_index_classify | single_call | index_code | industry_name, parent_code, level, industry_code, is_pub, src | - | - | - |
| sw_index_member | Tushare DatasetSpec | index_member_all | market.sw_index_member | by_code | l2_code, ts_code, in_date | l1_code, l1_name, l2_name, l3_code, l3_name, name, out_date, is_new | - | - | - |
| tushare_express_raw | Tushare DatasetSpec | express_vip | market.tushare_express_raw | by_period | source_record_key, source_row_hash | source_api, fetch_params, ts_code, ann_date, report_period, raw_payload, first_seen_at, last_seen_at, observed_at | 2026-06-22 | - | empty_valid |
| tushare_fina_indicator_raw | Tushare DatasetSpec | fina_indicator_vip | market.tushare_fina_indicator_raw | by_period | source_record_key, source_row_hash | source_api, fetch_params, ts_code, ann_date, report_period, raw_payload, first_seen_at, last_seen_at, observed_at | 2026-06-22 | - | empty_valid |
| tushare_forecast_raw | Tushare DatasetSpec | forecast_vip | market.tushare_forecast_raw | by_period | source_record_key, source_row_hash | source_api, fetch_params, ts_code, ann_date, report_period, raw_payload, first_seen_at, last_seen_at, observed_at | 2026-06-22 | - | empty_valid |

## 2. 覆盖矩阵：数据源 × 因子数/质量/生产腿

下表的“因子数”采用 component 视角：多源因子会按实际字段同时计入多个原料源；`multi_source` 行是主归类为多源复合的因子数量，用于观察复合拥挤度，不代表一个独立原始数据集。质量使用最新 `qe_eval_v2` 指标，优先 out_sample，其次 full/latest。

| data_source | 含义 | 因子数 | 有指标 | mean \|IC\| | mean \|ICIR\| | 平均覆盖率 | 质量桶 | 高相关对 | 当前生产腿暴露 | 状态 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| price_volume | 行情/价量 | 334 | 334 | 0.0207 | 0.2173 | 97.7% | strong=50, usable=141, weak=121, low_cov=22 | 502 | PLUS3 / FM12 / VOL | 拥挤 |
| daily_basic | 估值/换手/市值 | 199 | 199 | 0.0178 | 0.1738 | 94.8% | strong=28, usable=48, weak=90, low_cov=33 | 299 | FundVal / FUND_GROWTH（部分） | 拥挤 |
| moneyflow | 资金流/L2大中小单 | 146 | 146 | 0.0111 | 0.1624 | 97.1% | strong=9, usable=24, weak=99, low_cov=14 | 256 | Flow / FLOW_ACCEL | 拥挤 |
| cyq_perf | 筹码成本/胜率 | 110 | 110 | 0.0149 | 0.1459 | 96.7% | strong=7, usable=40, weak=54, low_cov=9 | 171 | 未进入当前6腿主 roster；曾作为筹码候选 | 拥挤 |
| margin_detail | 融资融券 | 6 | 6 | 0.0127 | 0.1856 | 95.0% | strong=0, usable=2, weak=3, low_cov=1 | 0 | MARG | 欠开发 |
| bak_basic | 扩展基本面/股东人数 | 93 | 93 | 0.0097 | 0.1088 | 91.2% | strong=0, usable=10, weak=54, low_cov=29 | 60 | FundVal / HOLDER / FUND_GROWTH（部分） | 质量弱 |
| sector_data | 申万行业/行业相对 | 99 | 99 | 0.0145 | 0.1678 | 96.6% | strong=17, usable=17, weak=55, low_cov=10 | 29 | 当前无独立行业腿 | 拥挤 |
| financial_event_raw | 财报快报/预告/财务指标原始事件 | 0 | 0 | - | - | - | strong=0, usable=0, weak=0, low_cov=0 | 0 | 当前无独立事件/财报腿 | 完全空白 |
| stock_basic | 股票静态属性/行业 | 0 | 0 | - | - | - | strong=0, usable=0, weak=0, low_cov=0 | 0 | 当前无独立静态行业/上市属性腿 | 完全空白 |
| dividend | 分红/股息 | 11 | 11 | 0.0062 | 0.0953 | 89.7% | strong=0, usable=0, weak=8, low_cov=3 | 9 | 当前无独立分红腿 | 质量弱 |
| shareholder | 股东/持有人行为 | 4 | 4 | 0.0048 | 0.1202 | 98.7% | strong=0, usable=0, weak=4, low_cov=0 | 0 | HOLDER（若使用 holder_num/股东行为） | 欠开发 |
| multi_source | 多源复合 | 329 | 329 | 0.0149 | 0.1671 | 96.1% | strong=32, usable=88, weak=170, low_cov=39 | 54 | 按组成源映射 | 拥挤 |
| unknown | 未知/待人工复核 | 1 | 1 | 0.0459 | 0.4426 | 75.2% | strong=0, usable=0, weak=0, low_cov=1 | 1 | 不可判定 | 元数据黑洞 |

### 2.1 字段利用率（基于回填 proposal 的 evidence_fields）

这张表回答“字段是否已被任何可用因子实际引用”。因子数多但字段集中，说明方向可能拥挤；字段存在但未用，说明有待开发空间。

| data_source | 已用字段 | 可用字段 | 字段利用率 | 已用字段样例 | 未用字段样例 |
| --- | --- | --- | --- | --- | --- |
| price_volume | 7 | 7 | 100.0% | amount, close, factor, high, low, open, volume | - |
| daily_basic | 16 | 16 | 100.0% | db_circ_mv, db_close, db_dv_ratio, db_dv_ttm, db_float_share, db_free_share, db_pb, db_pe, db_pe_ttm, db_ps | - |
| moneyflow | 15 | 18 | 83.3% | mf_elg_buy_amt, mf_elg_buy_vol, mf_elg_sell_amt, mf_elg_sell_vol, mf_lg_buy_amt, mf_lg_buy_vol, mf_lg_sell_amt, mf_lg_sell_vol, mf_md_buy_amt, mf_md_buy_vol | mf_md_sell_vol, mf_net_vol, mf_sm_sell_vol |
| cyq_perf | 9 | 9 | 100.0% | cp_cost_15pct, cp_cost_50pct, cp_cost_5pct, cp_cost_85pct, cp_cost_95pct, cp_his_high, cp_his_low, cp_weight_avg, cp_winner_rate | - |
| bak_basic | 13 | 15 | 86.7% | bb_bvps, bb_eps, bb_fixed_assets, bb_gpr, bb_holder_num, bb_liquid_assets, bb_npr, bb_pe_dyn, bb_per_undp, bb_profit_yoy | bb_reserved, bb_reserved_pershare |
| sector_data | 22 | 22 | 100.0% | sw2_amount, sw2_close, sw2_high, sw2_low, sw2_mf_buy_elg_amt, sw2_mf_buy_elg_vol, sw2_mf_buy_lg_amt, sw2_mf_buy_md_amt, sw2_mf_buy_sm_amt, sw2_mf_net_amt | - |
| margin_detail | 6 | 8 | 75.0% | rqmcl, rqye, rzche, rzmre, rzrqye, rzye | rqchl, rqyl |
| dividend | 2 | 2 | 100.0% | db_dv_ratio, db_dv_ttm | - |
| shareholder | 1 | 2 | 50.0% | bb_holder_num | holder_num |

## 3. factor_type 质量分布

| factor_type | 因子数 | 有指标 | mean \|IC\| | mean \|ICIR\| | 平均覆盖率 | 质量桶 |
| --- | --- | --- | --- | --- | --- | --- |
| moneyflow | 113 | 113 | 0.0110 | 0.1628 | 97.9% | strong=7, usable=21, weak=78 |
| valuation | 69 | 69 | 0.0144 | 0.1469 | 87.5% | strong=2, usable=9, weak=25 |
| chip_cost | 60 | 60 | 0.0163 | 0.1608 | 98.8% | strong=6, usable=25, weak=28 |
| turnover | 59 | 59 | 0.0275 | 0.2665 | 98.8% | strong=18, usable=22, weak=18 |
| volatility | 59 | 59 | 0.0274 | 0.2507 | 98.8% | strong=11, usable=35, weak=11 |
| industry_relative | 42 | 42 | 0.0176 | 0.2011 | 97.6% | strong=10, usable=11, weak=20 |
| momentum | 39 | 39 | 0.0265 | 0.2414 | 99.2% | strong=13, usable=14, weak=11 |
| price_volume | 38 | 38 | 0.0198 | 0.1969 | 99.7% | strong=2, usable=22, weak=14 |
| quality | 27 | 27 | 0.0089 | 0.0901 | 91.4% | strong=0, usable=4, weak=16 |
| size | 15 | 15 | 0.0175 | 0.1378 | 99.7% | strong=2, usable=5, weak=8 |
| statistical | 12 | 12 | 0.0206 | 0.2582 | 97.6% | strong=3, usable=3, weak=5 |
| correlation | 11 | 11 | 0.0175 | 0.2230 | 98.0% | strong=0, usable=7, weak=3 |
| dividend | 11 | 11 | 0.0062 | 0.0953 | 89.7% | strong=0, usable=0, weak=8 |
| growth | 8 | 8 | 0.0045 | 0.0708 | 88.2% | strong=0, usable=0, weak=6 |
| margin | 6 | 6 | 0.0127 | 0.1856 | 95.0% | strong=0, usable=2, weak=3 |
| shareholder | 4 | 4 | 0.0048 | 0.1202 | 98.7% | strong=0, usable=0, weak=4 |
| machine_learning | 2 | 2 | 0.0245 | 0.2190 | 89.4% | strong=0, usable=1, weak=0 |

## 4. 缺口结论与补因子建议

| 方向 | 状态 | 建议 |
| --- | --- | --- |
| price_volume | 拥挤 | 因子数过多，后续只做跨源复合或替换，不再扩同质变体。 |
| daily_basic | 拥挤 | 因子数过多，后续只做跨源复合或替换，不再扩同质变体。 |
| moneyflow | 拥挤 | 因子数过多，后续只做跨源复合或替换，不再扩同质变体。 |
| cyq_perf | 拥挤 | 因子数过多，后续只做跨源复合或替换，不再扩同质变体。 |
| margin_detail | 欠开发 | 因子数少；补 3-5 个不同机制的稳健变体，避免只换窗口。 |
| bak_basic | 质量弱 | 现有因子多但信号弱；建议从经济机制重构而非堆窗口。 |
| sector_data | 拥挤 | 因子数过多，后续只做跨源复合或替换，不再扩同质变体。 |
| financial_event_raw | 完全空白 | 已有原料但无可归类因子；优先设计低相关新因子。 |
| stock_basic | 完全空白 | 已有原料但无可归类因子；优先设计低相关新因子。 |
| dividend | 质量弱 | 现有因子多但信号弱；建议从经济机制重构而非堆窗口。 |
| shareholder | 欠开发 | 因子数少；补 3-5 个不同机制的稳健变体，避免只换窗口。 |
| multi_source | 拥挤 | 因子数过多，后续只做跨源复合或替换，不再扩同质变体。 |
| unknown | 元数据黑洞 | 无法进入覆盖矩阵；先人工复核名称/代码后再决定是否回填。 |

### 4.1 核心结论

1. **价量/资金流方向已经拥挤**：当前 6 腿价量族已塌成约 1.70 维，且 `price_volume`、`moneyflow` 的因子数与高相关对都偏高；后续不应再主要靠换窗口/换模型扩张。
2. **最值得补的是不同 DGP 的慢变量/事件变量**：`financial_event_raw`、`dividend`、`shareholder`、`industry_relative` 的生产腿暴露不足，和价量共识的相关性预期更低。
3. **`margin_detail` 是可扩但需控量的 P1 方向**：已有 MARG 暴露，但因子数量与机制变体不足；需要先确认覆盖率、T+1 对齐和边际 Sharpe。
4. **`cyq_perf` 有独特性但易与反转/拥挤混淆**：建议只补少量成本压力/获利盘机制，必须接相关矩阵筛冗余。
5. **元数据黑洞本身是阻塞项**：可用因子中 `data_source/factor_type` NULL 较多，报告附录给出回填 proposal；实际写库必须单独审批。

## 5. 正交性优先级（下一轮新因子开发排序）

| 优先级 | 方向 | 正交性理由 | 建议首批因子 |
| --- | --- | --- | --- |
| P0 | financial_event_raw / growth / quality | 财报预告、业绩快报、fina_indicator 与现有价量共识 DGP 差异最大；当前几乎无成熟生产腿，最可能提供正交 alpha。 | 先做盈利预期修正、预告兑现偏离、ROE/毛利率稳定性变化；必须严格 PIT/公告日滞后。 |
| P0 | dividend / shareholder | 分红与股东结构是慢变量/治理类信号，和 6 腿价量族相关性预期低。 | 做股息率变化、分红稳定性、股东人数变化/拥挤度；避免与简单估值重复。 |
| P1 | industry_relative / sector_data | 行业相对强弱可把个股动量拆成行业 beta 与个股 residual，有助于降低同质价量拥挤。 | 补行业内相对估值/资金流/动量残差，优先中性化后测试边际 Sharpe。 |
| P1 | margin_detail | 融资融券是杠杆资金行为，已有 MARG 腿但因子数量仍少，可补机制变体。 | 做融资余额变化、融券压力、融资买入/偿还差分，先验证覆盖率和 T+1 对齐。 |
| P2 | cyq_perf | 筹码成本有独特 DGP，但与价量反转/拥挤可能相关；不宜盲目扩张。 | 只做少量成本压力/获利盘释放变体，必须用相关矩阵筛冗余。 |
| P3 | moneyflow / price_volume | 当前生产腿已覆盖且高拥挤；新增同类窗口变体收益有限。 | 更多做替换/正交化，不再单纯新增 rolling window。 |

## 6. 高相关/同质化证据样本

| factor_a | factor_b | corr | source_a | source_b |
| --- | --- | --- | --- | --- |
| price_volume_corr | CORR20 | 0.983 | price_volume | price_volume |
| BookToPrice_Ratio | Inverse_Price_to_Book_Ratio | 0.999 | multi_source | daily_basic |
| BookToPrice_Ratio | Valuation_Chip_Support | 0.995 | multi_source | multi_source |
| ValueSizeComposite | BookToPrice_Ratio | 0.992 | price_volume | multi_source |
| asset_turnover_efficiency | BookToPrice_Ratio | 0.823 | multi_source | multi_source |
| book_value_price_ratio | BookToPrice_Ratio | 0.999 | multi_source | multi_source |
| cp_cost_50pct_div_bb_bvps | BookToPrice_Ratio | -0.983 | multi_source | multi_source |
| market_breadth_enhanced_valuation_factor | BookToPrice_Ratio | 0.995 | multi_source | multi_source |
| BookToPrice_Ratio | value_pe_pb_combined | 0.996 | multi_source | daily_basic |
| PriceVolumeDivergence_5D | CORD5 | 0.998 | price_volume | price_volume |
| Price_Volume_Divergence_10D | CORD10 | 0.996 | price_volume | price_volume |
| ChipProfitPressureFactor | CostMidpointPriceRatio | -0.868 | multi_source | multi_source |
| chip_pressure_winner_cost | ChipProfitPressureFactor | 0.841 | multi_source | multi_source |
| ChipProfitPressureFactor | cost_pressure_deviation | 0.806 | multi_source | multi_source |
| ChipProfitPressureFactor | cp_cost_pressure_test | 0.840 | multi_source | multi_source |
| factor_chip_concentration_price | ChipProfitPressureFactor | 0.827 | multi_source | multi_source |
| elg_flow_structure | ChipWinnerRateEliteBuyIntensity | 0.826 | price_volume | multi_source |
| price_volume_correlation_5d | CORR5 | 0.993 | price_volume | price_volume |
| Chip_Cost_Revenue_Growth_Product | RevenueGrowth_IndustrySize_Interaction_Factor | 0.919 | multi_source | multi_source |
| Chip_Cost_Revenue_Growth_Product | ValueGrowthCrossover | 0.855 | multi_source | multi_source |

## 7. unknown / 待人工复核清单

| factor_name | source | data_source | factor_type | mechanism | confidence |
| --- | --- | --- | --- | --- | --- |
| neg_Composite_Factor_Multi_Dim | manual | unknown | volatility | momentum | medium |

## 8. 回填 proposal（门控，不执行）

本节是候选映射表，供后续经用户审批后通过 factor_library MCP/后端写接口回填。**本 PR 不执行写库、DDL 或 DML。**

| factor_name | source | proposed_data_source | proposed_factor_type | mechanism | components | confidence | IC | ICIR | coverage | evidence_fields |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| BVPS20DayChange | rdagent_task_sync | bak_basic | valuation | value_premium | bak_basic | high | -0.0029 | -0.0708 | 100.0% | bb_bvps |
| BVPS_Winner_Rate_Combination_Factor | rdagent_task_sync | multi_source | chip_cost | value_premium | cyq_perf,bak_basic | high | -0.0122 | -0.1147 | 100.0% | bb_bvps,cp_winner_rate |
| BookToPrice_Ratio | rdagent_task_sync | multi_source | valuation | value_premium | bak_basic,daily_basic,price_volume | high | 0.0123 | 0.0809 | 100.0% | bb_bvps,close,db_close |
| CORD10 | alpha158 | price_volume | price_volume | reversal | price_volume | high | -0.0216 | -0.2626 | 99.8% | close,high,low,open,volume |
| CORD5 | alpha158 | price_volume | price_volume | microstructure | price_volume | high | -0.0208 | -0.2675 | 99.9% | close,high,low,open,volume |
| CORD60 | alpha158 | price_volume | price_volume | reversal | price_volume | high | -0.0148 | -0.1574 | 98.7% | close,high,low,open,volume |
| CORR10 | alpha158 | price_volume | price_volume | crowding | price_volume | high | -0.0280 | -0.3321 | 99.8% | close,high,low,open,volume |
| CORR20 | alpha158 | price_volume | price_volume | crowding | price_volume | high | -0.0224 | -0.2543 | 99.6% | close,high,low,open,volume |
| CORR5 | alpha158 | price_volume | price_volume | reversal | price_volume | high | -0.0245 | -0.3391 | 99.9% | close,high,low,open,volume |
| CORR60 | alpha158 | price_volume | price_volume | momentum | price_volume | high | -0.0104 | -0.1101 | 98.7% | close,high,low,open,volume |
| ChipProfitPressureFactor | rdagent_task_sync | multi_source | chip_cost | crowding | cyq_perf,price_volume | high | -0.0217 | -0.2283 | 100.0% | close,cp_cost_50pct,cp_winner_rate |
| ChipWinRateIndustryFlow | rdagent_task_sync | multi_source | industry_relative | momentum | cyq_perf,sector_data | high | 0.0063 | 0.0599 | 99.7% | cp_winner_rate,sw2_mf_net_amt,sw2_total_mv |
| ChipWinnerRateEliteBuyIntensity | rdagent_task_sync | multi_source | chip_cost | reversal | cyq_perf,moneyflow,price_volume | high | -0.0310 | -0.3215 | 100.0% | amount,cp_winner_rate,mf_elg_buy_amt |
| Chip_Cost_Liquidity_Interaction | rdagent_task_sync | multi_source | chip_cost | crowding | cyq_perf,price_volume | high | -0.0156 | -0.1565 | 100.0% | close,cp_cost_50pct,volume |
| Chip_Cost_Revenue_Growth_Product | rdagent_task_sync | multi_source | chip_cost | quality | cyq_perf,bak_basic | high | 0.0050 | 0.0547 | 100.0% | bb_rev_yoy,cp_cost_50pct |
| Chip_Cost_Sector_Deviation | rdagent_task_sync | multi_source | chip_cost | reversal | cyq_perf,sector_data,daily_basic,price_volume | high | 0.0032 | 0.0449 | 99.7% | close,cp_cost_50pct,db_close,sw2_pct_change |
| Chip_Volume_Interaction | rdagent_task_sync | multi_source | chip_cost | crowding | cyq_perf,price_volume | high | -0.0268 | -0.2391 | 100.0% | cp_winner_rate,volume |
| Chip_WinRate_CostDev | rdagent_task_sync | multi_source | chip_cost | crowding | cyq_perf,price_volume | high | -0.0237 | -0.2149 | 100.0% | close,cp_weight_avg,cp_winner_rate |
| Chip_Winning_Rate_Sector_Condition | rdagent_task_sync | multi_source | chip_cost | momentum | cyq_perf,sector_data | high | 0.0027 | 0.0284 | 99.7% | cp_winner_rate,sw2_pct_change |
| ChipsDispersionLargeFlowFactor | rdagent_task_sync | multi_source | chip_cost | momentum | cyq_perf,moneyflow | high | 0.0114 | 0.1819 | 100.0% | cp_cost_15pct,cp_cost_85pct,cp_weight_avg,mf_lg_buy_amt,mf_lg_sell_amt |
| ChipsPressure85Pct | rdagent_task_sync | multi_source | chip_cost | crowding | cyq_perf,daily_basic | high | -0.0160 | -0.1582 | 100.0% | cp_cost_85pct,db_close |
| ChipsWinRateMoneyFlowStrength | rdagent_task_sync | multi_source | moneyflow | momentum | cyq_perf,moneyflow | high | 0.0058 | 0.0686 | 100.0% | cp_winner_rate,mf_net_amt |
| ChipsWinRateVolumeCross | rdagent_task_sync | multi_source | chip_cost | reversal | cyq_perf,daily_basic,price_volume | high | -0.0435 | -0.3222 | 100.0% | cp_winner_rate,db_float_share,volume |
| Composite_MoneyFlow_Turnover_Momentum | rdagent_task_sync | multi_source | moneyflow | momentum | daily_basic,price_volume | high | -0.0038 | -0.0488 | 100.0% | close,db_turnover_rate |
| Conditional_PB_Deviation_Factor | rdagent_task_sync | sector_data | valuation | value_premium | sector_data | high | 0.0094 | 0.1559 | 99.5% | sw2_pb |
| CostDeviation_FreeFloat_Ratio | rdagent_task_sync | multi_source | valuation | reversal | cyq_perf,daily_basic,price_volume | high | -0.0196 | -0.2015 | 100.0% | close,cp_cost_50pct,db_free_share |
| CostDistributionSkew | rdagent_task_sync | cyq_perf | chip_cost | reversal | cyq_perf | high | 0.0050 | 0.0779 | 98.9% | cp_cost_15pct,cp_cost_50pct,cp_cost_85pct |
| CostMidpointPriceRatio | rdagent_task_sync | multi_source | chip_cost | reversal | cyq_perf,price_volume | high | 0.0232 | 0.2183 | 100.0% | close,cp_cost_50pct |
| CostPressureExtraLargeFlowCross | rdagent_task_sync | multi_source | chip_cost | reversal | cyq_perf,moneyflow,daily_basic,price_volume | high | -0.0044 | -0.1603 | 100.0% | amount,cp_cost_50pct,db_close,mf_elg_buy_amt,mf_elg_sell_amt,mf_net_amt |
| CostRange_Price_Position | rdagent_task_sync | multi_source | chip_cost | reversal | cyq_perf,daily_basic | high | -0.0201 | -0.1962 | 100.0% | cp_cost_5pct,cp_cost_95pct,db_close |
| Cost_Flow_Interaction_Factor | rdagent_task_sync | multi_source | moneyflow | momentum | cyq_perf,daily_basic | high | -0.0033 | -0.0632 | 100.0% | cp_cost_50pct,db_close,db_total_mv |
| DailyVolatilityIndustryRatio | rdagent_task_sync | multi_source | industry_relative | reversal | sector_data,price_volume | high | -0.0412 | -0.4567 | 99.7% | close,high,low,sw2_close,sw2_high,sw2_low |
| DividendAdjustedNetInflow | rdagent_task_sync | multi_source | dividend | crowding | moneyflow,daily_basic,dividend,price_volume | high | -0.0046 | -0.1470 | 94.7% | amount,db_dv_ratio,mf_elg_buy_amt,mf_elg_sell_amt |
| DividendToFreeTurnover_Ratio | rdagent_task_sync | multi_source | dividend | value_premium | daily_basic,dividend | high | 0.0134 | 0.0806 | 94.7% | db_dv_ratio,db_turnover_rate_f |
| Dividend_Rate_Chip_Median_Relative_Value | rdagent_task_sync | multi_source | dividend | value_premium | cyq_perf,daily_basic,dividend | high | 0.0044 | 0.0301 | 94.7% | cp_cost_50pct,db_dv_ratio |
| Dividend_Rate_Cost_Weight_Ratio_Factor | rdagent_task_sync | multi_source | dividend | value_premium | cyq_perf,daily_basic,dividend | high | 0.0044 | 0.0296 | 94.7% | cp_weight_avg,db_dv_ratio |
| Dividend_Yield_to_Fixed_Assets_Ratio | rdagent_task_sync | multi_source | dividend | value_premium | bak_basic,daily_basic,dividend | high | 0.0047 | 0.0693 | 94.7% | bb_fixed_assets,db_dv_ratio |
| DynPE_ChipWin_Product | rdagent_task_sync | multi_source | valuation | value_premium | cyq_perf,bak_basic | high | -0.0121 | -0.1624 | 100.0% | bb_pe_dyn,cp_winner_rate |
| DynamicValueQualityScore | rdagent_task_sync | bak_basic | quality | value_premium | bak_basic | high | 0.0035 | 0.0674 | 79.1% | bb_pe_dyn,bb_profit_yoy |
| Dynamic_Money_Flow_Valuation_Ratio | rdagent_task_sync | multi_source | valuation | value_premium | moneyflow,bak_basic | high | 0.0028 | 0.0315 | 79.1% | bb_pe_dyn,mf_net_amt |
| Dynamic_RiskAdj_Composite_5D | rdagent_task_sync | price_volume | volatility | momentum | price_volume | high | -0.0046 | -0.0357 | 75.2% | amount,close,factor,high,low,open,volume |
| ELG_SELL_FLOW_LIQUIDITY_RATIO | rdagent_task_sync | multi_source | turnover | reversal | moneyflow,daily_basic | high | -0.0172 | -0.1725 | 100.0% | db_turnover_rate_f,mf_elg_sell_vol |
| EPS_Adjusted_Cost_Position | rdagent_task_sync | multi_source | valuation | value_premium | cyq_perf,bak_basic,price_volume | high | -0.0103 | -0.2125 | 99.8% | bb_eps,close,cp_weight_avg |
| EPS_Winner_Rate_Product_Factor | rdagent_task_sync | multi_source | chip_cost | quality | cyq_perf,bak_basic | high | -0.0053 | -0.0428 | 100.0% | bb_eps,cp_winner_rate |
| Earnings_Industry_Volume_Ratio_Factor | rdagent_task_sync | multi_source | valuation | value_premium | sector_data,bak_basic | high | 0.0005 | 0.0044 | 99.7% | bb_eps,sw2_vol |
| ElgBuyAmount_IndustrySell_Ratio_Factor | rdagent_task_sync | multi_source | moneyflow | momentum | moneyflow,sector_data | high | -0.0328 | -0.4012 | 100.0% | mf_elg_buy_amt,sw2_mf_sell_elg_amt |
| ElgFlow_Concentration_Ratio | rdagent_task_sync | multi_source | moneyflow | crowding | moneyflow,sector_data | high | -0.0008 | -0.0270 | 100.0% | mf_elg_buy_amt,mf_elg_sell_amt,sw2_mf_net_amt |
| ElgNetInflowEfficiency_IndustryLargeBuy_Ratio_Factor | rdagent_task_sync | multi_source | moneyflow | reversal | moneyflow,sector_data,price_volume | high | -0.0037 | -0.1205 | 100.0% | amount,mf_elg_buy_amt,mf_elg_sell_amt,sw2_mf_buy_lg_amt |
| EliteFlowRatio_CirculatingShares_Product | rdagent_task_sync | multi_source | moneyflow | momentum | moneyflow,daily_basic,price_volume | high | -0.0042 | -0.1055 | 100.0% | amount,db_float_share,mf_elg_buy_amt,mf_elg_sell_amt |
| EliteFlow_Industry_Strength | rdagent_task_sync | multi_source | moneyflow | momentum | moneyflow,sector_data | high | -0.0041 | -0.1536 | 100.0% | mf_elg_buy_amt,mf_elg_sell_amt,sw2_mf_buy_elg_amt,sw2_mf_sell_elg_amt |
| Elite_5Day_Net_Inflow_MV_Ratio | rdagent_task_sync | daily_basic | moneyflow | momentum | daily_basic | high | -0.0161 | -0.2593 | 100.0% | db_total_mv |
| Elite_Buy_Industry_Relative_Strength_Factor | rdagent_task_sync | multi_source | moneyflow | momentum | moneyflow,sector_data | high | -0.0328 | -0.4006 | 100.0% | mf_elg_buy_amt,sw2_mf_buy_elg_amt |
| ExtraLargeOrderNetFlow_Cumulative | rdagent_task_sync | multi_source | moneyflow | momentum | moneyflow,daily_basic | high | -0.0161 | -0.2573 | 100.0% | db_circ_mv,mf_elg_buy_amt,mf_elg_sell_amt |
| Extra_Large_Order_Flow_Industry_Relative_Strength_Factor | rdagent_task_sync | multi_source | moneyflow | momentum | moneyflow,sector_data,price_volume | high | -0.0056 | -0.1807 | 100.0% | close,mf_elg_buy_amt,mf_elg_sell_amt,sw2_mf_buy_elg_amt,sw2_mf_sell_elg_amt |
| FinancialHealthScoreFactor | rdagent_task_sync | bak_basic | quality | quality | bak_basic | high | 0.0036 | 0.0787 | 100.0% | bb_npr,bb_rev_yoy |
| FlowMomentumSync_5D | rdagent_task_sync | price_volume | correlation | momentum | price_volume | high | -0.0173 | -0.2309 | 100.0% | amount,close,factor,high,low,open,volume |
| Flow_Price_Ratio_Large_Buy | rdagent_task_sync | multi_source | moneyflow | reversal | moneyflow,price_volume | high | -0.0261 | -0.2746 | 100.0% | amount,mf_lg_buy_amt |
| Free_Float_Turnover_to_Industry_MV_Ratio_Factor | rdagent_task_sync | multi_source | turnover | liquidity_premium | sector_data,daily_basic | high | -0.0303 | -0.3208 | 99.7% | db_turnover_rate_f,sw2_total_mv |
| FundFlowAssetTurnoverRatio | rdagent_task_sync | multi_source | quality | momentum | moneyflow,bak_basic,daily_basic | high | -0.0096 | -0.1171 | 100.0% | bb_total_assets,db_turnover_rate_f,mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt |
| FundamentalEpsIndustryMomentum | rdagent_task_sync | multi_source | industry_relative | momentum | sector_data,bak_basic | high | -0.0003 | -0.0034 | 99.3% | bb_eps,sw2_close |
| Fundamental_Liquidity_Cross_Factor | rdagent_task_sync | multi_source | turnover | value_premium | bak_basic,daily_basic | high | -0.0316 | -0.2263 | 100.0% | bb_bvps,db_turnover_rate |
| Fundamental_Strength_Industry_Liquidity_Factor | rdagent_task_sync | multi_source | quality | quality | sector_data,bak_basic | high | 0.0003 | 0.0026 | 99.7% | bb_npr,sw2_amount |
| GPR_Industry_Direction_Synergy_Factor | rdagent_task_sync | multi_source | quality | quality | sector_data,bak_basic | high | 0.0003 | 0.0034 | 99.7% | bb_gpr,sw2_pct_change |
| GrossMargin_IndustryPE_Ratio | rdagent_task_sync | multi_source | valuation | value_premium | sector_data,bak_basic | high | -0.0038 | -0.0345 | 99.6% | bb_gpr,sw2_pe |
| GrossProfitRateVolumeAnomaly_Interaction | rdagent_task_sync | multi_source | moneyflow | reversal | bak_basic,price_volume | high | -0.0157 | -0.2193 | 99.7% | bb_gpr,volume |
| Gross_Margin_Industry_Valuation_Interaction | rdagent_task_sync | multi_source | quality | value_premium | sector_data,bak_basic | high | -0.0015 | -0.0121 | 99.7% | bb_gpr,sw2_pb |
| HistoricalPositionVolumeInteraction | rdagent_task_sync | multi_source | price_volume | reversal | cyq_perf,price_volume | high | -0.0164 | -0.2146 | 100.0% | close,cp_his_high,cp_his_low,volume |
| HistoricalPricePosition | rdagent_task_sync | multi_source | price_volume | reversal | cyq_perf,daily_basic,price_volume | high | -0.0204 | -0.1665 | 100.0% | close,cp_his_high,cp_his_low,db_close |
| IndustryEliteFlowRelativeStrength | rdagent_task_sync | multi_source | moneyflow | momentum | moneyflow,sector_data,daily_basic | high | 0.0025 | 0.0206 | 99.7% | db_circ_mv,mf_elg_buy_amt,mf_elg_sell_amt,sw2_mf_buy_elg_amt,sw2_total_mv |
| IndustryMomentumChipAvgCostRatio | rdagent_task_sync | multi_source | industry_relative | momentum | cyq_perf,sector_data,price_volume | high | 0.0204 | 0.2287 | 97.2% | close,cp_weight_avg,sw2_close |
| IndustryMomentumExcessReturnCross | rdagent_task_sync | multi_source | industry_relative | momentum | sector_data,price_volume | high | 0.0125 | 0.2070 | 99.3% | close,sw2_close,sw2_mf_net_amt |
| IndustryMomentum_LiquidityRatio_Product | rdagent_task_sync | multi_source | turnover | momentum | sector_data,bak_basic | high | -0.0048 | -0.0372 | 99.7% | bb_liquid_assets,bb_total_assets,sw2_pct_change |
| IndustryMomentum_VolumeWeighted | rdagent_task_sync | sector_data | industry_relative | momentum | sector_data | high | -0.0017 | -0.0141 | 99.7% | sw2_pct_change,sw2_vol |
| IndustryNetInflow_Volume_Ratio | rdagent_task_sync | multi_source | industry_relative | reversal | sector_data,price_volume | high | -0.0132 | -0.1356 | 100.0% | sw2_mf_net_vol,volume |
| IndustryRelativeMomentum | rdagent_task_sync | multi_source | industry_relative | momentum | sector_data,daily_basic | high | -0.0046 | -0.0806 | 99.4% | db_close,sw2_close |
| IndustryRelativeValuationEarningsFactor | rdagent_task_sync | multi_source | valuation | value_premium | sector_data,bak_basic,daily_basic | high | -0.0020 | -0.0181 | 99.7% | bb_eps,db_close,sw2_close |
| IndustryTotalMVCap_Momentum | rdagent_task_sync | sector_data | size | momentum | sector_data | high | -0.0091 | -0.0675 | 99.4% | sw2_total_mv |
| IndustryTurnoverAdjustedValueFactor | rdagent_task_sync | sector_data | valuation | value_premium | sector_data | high | 0.0036 | 0.0413 | 75.0% | sw2_amount |
| IndustryVolumeAdjustedValuationMomentum | rdagent_task_sync | multi_source | valuation | momentum | sector_data,daily_basic | high | -0.0369 | -0.2847 | 74.8% | db_pe_ttm,sw2_vol |
| Industry_Adjusted_Dividend_Yield_Ratio | rdagent_task_sync | multi_source | dividend | value_premium | sector_data,daily_basic,dividend | high | 0.0003 | 0.0021 | 94.5% | db_dv_ratio,sw2_pe |
| Industry_ElgFlow_Relative_Strength_Factor | rdagent_task_sync | multi_source | moneyflow | momentum | moneyflow,sector_data | high | -0.0118 | -0.2655 | 99.9% | mf_elg_buy_amt,mf_elg_sell_amt,sw2_mf_buy_elg_amt |
| Industry_Excess_Return | rdagent_task_sync | multi_source | industry_relative | momentum | sector_data,price_volume | high | 0.0029 | 0.0222 | 99.7% | close,sw2_pct_change |
| Industry_Extra_Large_Buy_Individual_Extra_Large_Sell_Strength_Ratio | rdagent_task_sync | multi_source | moneyflow | momentum | moneyflow,sector_data | high | 0.0243 | 0.3094 | 77.3% | mf_elg_sell_amt,sw2_mf_buy_elg_amt |
| Industry_Intraday_Strength_Ratio_Factor | rdagent_task_sync | sector_data | industry_relative | reversal | sector_data | high | -0.0055 | -0.0393 | 99.7% | sw2_close,sw2_high |
| Industry_Momentum | rdagent_task_sync | sector_data | industry_relative | momentum | sector_data | high | -0.0189 | -0.1366 | 98.4% | sw2_pct_change |
| Industry_Relative_Money_Flow | rdagent_task_sync | multi_source | moneyflow | momentum | moneyflow,sector_data,daily_basic | high | 0.0061 | 0.0899 | 99.7% | db_total_mv,mf_net_amt,sw2_mf_net_amt,sw2_total_mv |
| Industry_Turnover_SmallBuy_Flow_Ratio_Factor | rdagent_task_sync | multi_source | moneyflow | momentum | moneyflow,sector_data | high | 0.0175 | 0.1256 | 99.6% | mf_sm_buy_vol,sw2_amount |
| Industry_Valuation_Cost_Deviation_Factor | rdagent_task_sync | multi_source | chip_cost | value_premium | cyq_perf,sector_data,daily_basic | high | 0.0049 | 0.0298 | 99.7% | cp_cost_95pct,db_close,sw2_pb |
| Industry_Volatility_Liquidity_Cross_Factor | rdagent_task_sync | multi_source | turnover | liquidity_premium | sector_data,daily_basic | high | -0.0497 | -0.3217 | 99.7% | db_turnover_rate_f,sw2_high,sw2_low |
| InstitutionalToRetailMoneyFlowRatio | rdagent_task_sync | multi_source | moneyflow | momentum | moneyflow,price_volume | high | 0.0065 | 0.2483 | 100.0% | amount,mf_elg_buy_amt,mf_elg_sell_amt,mf_sm_buy_amt,mf_sm_sell_amt |
| InversePBToCostDistributionRatio | rdagent_task_sync | multi_source | valuation | value_premium | cyq_perf,daily_basic | high | 0.0101 | 0.0639 | 99.8% | cp_cost_5pct,cp_cost_95pct,db_pb |
| InversePE_LargeBuy_Product | rdagent_task_sync | moneyflow | valuation | reversal | moneyflow | high | -0.0296 | -0.2424 | 75.2% | mf_lg_buy_amt |
| Inverse_Price_to_Book_Ratio | rdagent_task_sync | daily_basic | valuation | value_premium | daily_basic | high | 0.0123 | 0.0804 | 99.8% | db_pb |
| KLEN | alpha158 | price_volume | price_volume | reversal | price_volume | high | -0.0466 | -0.3155 | 100.0% | close,high,low,open,volume |
| KLOW | alpha158 | price_volume | price_volume | reversal | price_volume | high | -0.0260 | -0.2690 | 100.0% | close,high,low,open,volume |
| LargeMoneyFlowConcentration | rdagent_task_sync | moneyflow | moneyflow | crowding | moneyflow | high | -0.0043 | -0.1517 | 100.0% | mf_elg_buy_amt,mf_elg_sell_amt,mf_net_amt |
| LargeNetInflowIntensity | rdagent_task_sync | multi_source | moneyflow | reversal | moneyflow,daily_basic | high | -0.0026 | -0.0527 | 100.0% | db_circ_mv,mf_elg_buy_amt,mf_elg_sell_amt |
| LargeOrderFlowRatio | rdagent_task_sync | moneyflow | moneyflow | reversal | moneyflow | high | -0.0151 | -0.1955 | 100.0% | mf_elg_buy_amt,mf_lg_buy_amt,mf_net_amt |
| LargeOrder_Cost_Interaction | rdagent_task_sync | multi_source | moneyflow | reversal | cyq_perf,moneyflow | high | -0.0367 | -0.3496 | 100.0% | cp_weight_avg,mf_lg_buy_amt |
| LargeOrder_Flow_Strength | rdagent_task_sync | multi_source | moneyflow | reversal | moneyflow,price_volume | high | -0.0047 | -0.0991 | 100.0% | mf_elg_buy_amt,mf_elg_sell_amt,volume |
| LargeSmallOrderImbalance | rdagent_task_sync | moneyflow | moneyflow | reversal | moneyflow | high | -0.0292 | -0.2791 | 100.0% | mf_lg_buy_amt,mf_sm_sell_amt |
| Large_Cap_Momentum_Bias | rdagent_task_sync | multi_source | size | momentum | daily_basic,price_volume | high | -0.0229 | -0.1965 | 100.0% | close,db_circ_mv |
| LiqAdj_MainNet_5D | rdagent_task_sync | multi_source | turnover | reversal | moneyflow,daily_basic,price_volume | high | -0.0142 | -0.2840 | 100.0% | amount,close,db_turnover_rate,factor,high,low,mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt,open,volume |
| LiquidAssets_VolumeRatio_Interaction_Factor | rdagent_task_sync | multi_source | quality | liquidity_premium | bak_basic,daily_basic | high | 0.0011 | 0.0092 | 92.5% | bb_liquid_assets,db_volume_ratio |
| Liquid_Assets_Market_Cap_Ratio_Momentum | rdagent_task_sync | multi_source | quality | momentum | bak_basic,daily_basic | high | 0.0384 | 0.2974 | 100.0% | bb_liquid_assets,db_total_mv |
| LiquidityAdj_DividendYield_Vol | rdagent_task_sync | multi_source | dividend | value_premium | sector_data,daily_basic,dividend | high | 0.0013 | 0.0096 | 94.5% | db_dv_ratio,sw2_vol |
| Liquidity_Vol_to_PS_Ratio | rdagent_task_sync | daily_basic | valuation | liquidity_premium | daily_basic | high | -0.0021 | -0.0162 | 92.5% | db_ps |
| LowCostSupportLevel | rdagent_task_sync | multi_source | chip_cost | reversal | cyq_perf,price_volume | high | 0.0231 | 0.2343 | 100.0% | close,cp_cost_5pct |
| Low_Percentile_Cost_Support_Ratio_Factor | rdagent_task_sync | multi_source | chip_cost | reversal | cyq_perf,daily_basic | high | 0.0205 | 0.2084 | 100.0% | cp_cost_5pct,db_close |
| MF_ElgNetRatio_TurnoverRatio | rdagent_task_sync | multi_source | turnover | crowding | daily_basic,price_volume | high | -0.0061 | -0.1898 | 100.0% | amount,close,db_turnover_rate,factor,high,low,open,volume |
| MF_MainNetAmtRatio_5D_Mom | rdagent_task_sync | multi_source | moneyflow | momentum | moneyflow,price_volume | high | 0.0037 | 0.1843 | 100.0% | amount,close,factor,high,low,mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt,open,volume |
| MF_MainNetRatio_Momentum_5D | rdagent_task_sync | multi_source | moneyflow | momentum | moneyflow,price_volume | high | 0.0013 | 0.0755 | 99.8% | amount,close,factor,high,low,mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt,open,volume |
| MF_Main_Net_Amt_Momentum_5D | rdagent_task_sync | price_volume | moneyflow | momentum | price_volume | high | 0.0043 | 0.2067 | 100.0% | amount,close,factor,high,low,open,volume |
| MF_Stability_Enhanced_5D | rdagent_task_sync | price_volume | quality | reversal | price_volume | high | -0.0228 | -0.1913 | 98.4% | amount,close,factor,high,low,open,volume |
| MOM_TURN | rdagent_task_sync | multi_source | momentum | reversal | daily_basic,price_volume | high | -0.0373 | -0.2821 | 100.0% | close,db_turnover_rate |
| MainFlowMomentumProduct_5D | rdagent_task_sync | price_volume | moneyflow | momentum | price_volume | high | -0.0031 | -0.0498 | 100.0% | amount,close,factor,high,low,open,volume |
| MainFundActivityFactor | rdagent_task_sync | multi_source | turnover | reversal | moneyflow,daily_basic,price_volume | high | -0.0145 | -0.3022 | 100.0% | amount,db_turnover_rate_f,mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt |
| MainNetFlowToEPS_Ratio | rdagent_task_sync | multi_source | moneyflow | momentum | moneyflow,bak_basic | high | -0.0035 | -0.1136 | 99.8% | bb_eps,mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt |
| MainNetFlowTurnoverAdjustedFactor | rdagent_task_sync | multi_source | turnover | momentum | moneyflow,daily_basic | high | -0.0027 | -0.0560 | 100.0% | db_turnover_rate,mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt |
| MainNetInflowRatio_to_ChipCost85_Ratio | rdagent_task_sync | cyq_perf | moneyflow | momentum | cyq_perf | high | -0.0054 | -0.1277 | 100.0% | cp_cost_85pct |
| Main_Flow_Chip_Cost_Interaction_Factor | rdagent_task_sync | cyq_perf | moneyflow | momentum | cyq_perf | high | -0.0011 | -0.0220 | 100.0% | cp_weight_avg |
| Main_Force_Net_Inflow_Industry_Price_Volatility_Adjustment | rdagent_task_sync | multi_source | industry_relative | momentum | moneyflow,sector_data,price_volume | high | -0.0064 | -0.1695 | 99.7% | amount,mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt,sw2_high,sw2_low |
| Medium_Order_Buy_Low_Cost_Intensity | rdagent_task_sync | multi_source | chip_cost | reversal | cyq_perf,moneyflow | high | -0.0373 | -0.3503 | 100.0% | cp_cost_15pct,mf_md_buy_amt |
| MidOrder_Flow_Cost_Deviation_Factor | rdagent_task_sync | multi_source | moneyflow | reversal | cyq_perf,moneyflow,daily_basic | high | -0.0033 | -0.0751 | 100.0% | cp_cost_85pct,db_circ_mv,db_close,mf_md_buy_amt,mf_md_sell_amt |
| MomentumReversal_5D_20V | rdagent_task_sync | price_volume | volatility | momentum | price_volume | high | -0.0338 | -0.2685 | 100.0% | amount,close,factor,high,low,open,volume |
| Momentum_Flow_Divergence_Correlation_Factor | rdagent_task_sync | multi_source | correlation | reversal | moneyflow,price_volume | high | -0.0020 | -0.0449 | 100.0% | close,mf_md_buy_amt,mf_md_sell_amt |
| Momentum_Shareholder_Concentration_Interaction_Factor | rdagent_task_sync | multi_source | shareholder | momentum | bak_basic,shareholder,price_volume | high | -0.0071 | -0.2019 | 100.0% | bb_holder_num,close |
| Momentum_Strength_10D | rdagent_task_sync | price_volume | volatility | reversal | price_volume | high | -0.0317 | -0.2778 | 100.0% | close |
| MoneyFlow_Industry_Relative_Strength | rdagent_task_sync | multi_source | moneyflow | momentum | moneyflow,sector_data | high | -0.0067 | -0.1473 | 100.0% | mf_net_amt,sw2_mf_net_amt |
| Money_Flow_Industry_Turnover_Ratio | rdagent_task_sync | multi_source | turnover | momentum | moneyflow,sector_data | high | 0.0043 | 0.0524 | 99.7% | mf_net_amt,sw2_amount |
| PB_WinRate_Interaction | rdagent_task_sync | multi_source | valuation | value_premium | cyq_perf,daily_basic | high | -0.0039 | -0.0286 | 99.8% | cp_winner_rate,db_pb |
| PE_Industry_Deviation | rdagent_task_sync | multi_source | valuation | value_premium | sector_data,daily_basic | high | -0.0060 | -0.0713 | 78.4% | db_pe,sw2_pe |
| PS_TTM_to_Market_Cap_Ratio | rdagent_task_sync | daily_basic | valuation | value_premium | daily_basic | high | 0.0034 | 0.0259 | 100.0% | db_ps_ttm,db_total_mv |
| PV_Synergy_Momentum_5D | rdagent_task_sync | price_volume | momentum | momentum | price_volume | high | -0.0273 | -0.3306 | 100.0% | amount,close,factor,high,low,open,volume |
| PerShareUndistributedProfit_PB_Ratio_Factor | rdagent_task_sync | multi_source | valuation | value_premium | bak_basic,daily_basic | high | 0.0028 | 0.0224 | 99.8% | bb_per_undp,db_pb |
| PriceLowCostDeviation_BookValue_Ratio | rdagent_task_sync | multi_source | valuation | value_premium | cyq_perf,bak_basic,price_volume | high | -0.0281 | -0.2706 | 100.0% | bb_bvps,close,cp_cost_15pct |
| PriceStrength_10D | rdagent_task_sync | price_volume | momentum | reversal | price_volume | high | -0.0211 | -0.1703 | 100.0% | close,high,low |
| PriceStrength_MVAdj_10D | rdagent_task_sync | multi_source | size | momentum | daily_basic,price_volume | high | 0.0155 | 0.1131 | 100.0% | amount,close,db_circ_mv,factor,high,low,open,volume |
| PriceStrength_Turnover_20D | rdagent_task_sync | multi_source | turnover | reversal | daily_basic,price_volume | high | -0.0414 | -0.3046 | 100.0% | amount,close,db_turnover_rate,factor,high,low,open,volume |
| PriceVolumeDivergence_5D | rdagent_task_sync | price_volume | correlation | reversal | price_volume | high | -0.0216 | -0.2744 | 100.0% | amount,close,factor,high,low,open,volume |
| Price_ChipNormalized_Position | rdagent_task_sync | multi_source | price_volume | reversal | cyq_perf,price_volume | high | -0.0221 | -0.1811 | 100.0% | close,cp_his_high,cp_his_low |
| Price_Deviation_Historical_High | rdagent_task_sync | multi_source | momentum | reversal | cyq_perf,price_volume | high | -0.0198 | -0.1645 | 100.0% | close,cp_his_high |
| Price_Flow_Reversal_5D | rdagent_task_sync | multi_source | moneyflow | reversal | moneyflow,price_volume | high | -0.0064 | -0.1480 | 99.9% | close,high,low,mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt,mf_md_buy_amt,mf_md_sell_amt,mf_net_amt,mf_sm_buy_amt,mf_sm_sell_amt |
| Price_Volume_Convergence_10D | rdagent_task_sync | price_volume | momentum | momentum | price_volume | high | -0.0223 | -0.3021 | 100.0% | amount,close,factor,high,low,open,volume |
| Price_Volume_Divergence_10D | rdagent_task_sync | price_volume | correlation | reversal | price_volume | high | -0.0203 | -0.2541 | 100.0% | close,volume |
| Price_to_Book_Ratio_Industry_Relative_Momentum | rdagent_task_sync | multi_source | valuation | reversal | sector_data,daily_basic,price_volume | high | -0.0353 | -0.2987 | 99.5% | close,db_pb,sw2_pb |
| Profit_Growth_Chip_Cost_Ratio_Factor | rdagent_task_sync | multi_source | chip_cost | quality | cyq_perf,bak_basic | high | 0.0015 | 0.0298 | 100.0% | bb_profit_yoy,cp_cost_15pct |
| Profit_WinRate_Product | rdagent_task_sync | multi_source | quality | quality | cyq_perf,bak_basic | high | -0.0046 | -0.0417 | 100.0% | bb_npr,cp_winner_rate |
| Profitability_PriceLow_Interaction_Factor | rdagent_task_sync | multi_source | quality | reversal | cyq_perf,bak_basic,price_volume | high | -0.0019 | -0.0158 | 100.0% | bb_npr,close,cp_his_low |
| RESI10 | alpha158 | price_volume | price_volume | reversal | price_volume | high | -0.0132 | -0.1116 | 99.8% | close,high,low,open,volume |
| RESI5 | alpha158 | price_volume | price_volume | reversal | price_volume | high | -0.0054 | -0.0456 | 99.9% | close,high,low,open,volume |
| ROC60 | alpha158 | price_volume | price_volume | reversal | price_volume | high | 0.0359 | 0.2415 | 99.9% | close,high,low,open,volume |
| RSQR10 | alpha158 | price_volume | price_volume | momentum | price_volume | high | -0.0085 | -0.1015 | 99.8% | close,high,low,open,volume |
| RSQR20 | alpha158 | price_volume | price_volume | momentum | price_volume | high | -0.0027 | -0.0336 | 99.6% | close,high,low,open,volume |
| RSQR5 | alpha158 | price_volume | price_volume | momentum | price_volume | high | -0.0094 | -0.1220 | 99.9% | close,high,low,open,volume |
| RSQR60 | alpha158 | price_volume | price_volume | momentum | price_volume | high | -0.0042 | -0.0484 | 98.7% | close,high,low,open,volume |
| RetailFundFlowValuationFactor | rdagent_task_sync | multi_source | moneyflow | crowding | moneyflow,daily_basic | high | 0.0026 | 0.0552 | 100.0% | db_ps_ttm,mf_sm_buy_amt,mf_sm_sell_amt |
| RetailOutflowSizeRatio | rdagent_task_sync | multi_source | moneyflow | reversal | moneyflow,daily_basic,price_volume | high | -0.0078 | -0.1519 | 100.0% | amount,close,db_circ_mv,factor,high,low,mf_sm_buy_amt,mf_sm_sell_amt,open,volume |
| Retail_Sentiment_Support_Factor | rdagent_task_sync | multi_source | moneyflow | reversal | cyq_perf,moneyflow | high | -0.0263 | -0.2462 | 100.0% | cp_his_low,mf_sm_buy_amt |
| RevenueGrowthPriceRatio | rdagent_task_sync | multi_source | growth | value_premium | bak_basic,price_volume | high | 0.0051 | 0.0841 | 100.0% | bb_rev_yoy,close |
| RevenueGrowth_IndustrySize_Interaction_Factor | rdagent_task_sync | multi_source | growth | quality | sector_data,bak_basic,price_volume | high | 0.0061 | 0.0721 | 99.7% | bb_rev_yoy,close,factor,sw2_total_mv |
| RevenueYOY_Momentum | manual | bak_basic | momentum | momentum | bak_basic | high | 0.0018 | 0.0630 | 100.0% | bb_rev_yoy |
| STD5 | alpha158 | price_volume | price_volume | reversal | price_volume | high | -0.0413 | -0.2828 | 99.9% | close,high,low,open,volume |
| SW2_MOM5 | rdagent_task_sync | sector_data | industry_relative | momentum | sector_data | high | -0.0184 | -0.1333 | 100.0% | sw2_close |
| SectorMomentumFlowAdjustedFactor | rdagent_task_sync | multi_source | moneyflow | momentum | moneyflow,sector_data,daily_basic | high | -0.0098 | -0.1807 | 99.7% | db_close,mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt,mf_md_buy_amt,mf_md_sell_amt,mf_sm_buy_amt,mf_sm_sell_amt,sw2_pct_change |
| SectorRelativeMomentumFactor | rdagent_task_sync | multi_source | industry_relative | momentum | sector_data,daily_basic | high | -0.0166 | -0.2166 | 99.7% | db_close,sw2_pct_change |
| Shareholder_Count_to_Circulating_Shares_Ratio | rdagent_task_sync | multi_source | shareholder | crowding | bak_basic,daily_basic,shareholder | high | -0.0015 | -0.0120 | 100.0% | bb_holder_num,db_float_share |
| SizeAdj_RSI_14D | rdagent_task_sync | multi_source | price_volume | reversal | daily_basic,price_volume | high | -0.0319 | -0.2748 | 100.0% | amount,close,db_circ_mv,factor,high,low,open,volume |
| SizeAdjustedTurnover | rdagent_task_sync | multi_source | turnover | liquidity_premium | daily_basic,price_volume | high | -0.0437 | -0.2729 | 100.0% | amount,close,db_circ_mv,db_turnover_rate,factor,high,low,open,volume |
| Size_Log_MV | rdagent_task_sync | multi_source | size | liquidity_premium | daily_basic,price_volume | high | -0.0141 | -0.0895 | 100.0% | amount,close,db_circ_mv,factor,high,low,open,volume |
| SmallBuy_Volume_CostPosition_MarketCap_Adjusted | rdagent_task_sync | multi_source | moneyflow | reversal | cyq_perf,moneyflow,daily_basic,price_volume | high | -0.0237 | -0.1641 | 100.0% | close,cp_cost_50pct,db_circ_mv,mf_sm_buy_vol |
| SmallOrderFlowValuationAdjustment | rdagent_task_sync | multi_source | moneyflow | crowding | moneyflow,daily_basic | high | 0.0064 | 0.1361 | 80.3% | db_pe,db_pe_ttm,db_total_mv,mf_sm_buy_amt,mf_sm_sell_amt |
| SmallOrderIntensityBreakoutFactor | rdagent_task_sync | multi_source | size | reversal | cyq_perf,moneyflow,daily_basic,price_volume | high | -0.0420 | -0.2856 | 99.9% | close,cp_his_high,db_total_mv,mf_sm_buy_amt |
| SmallOrder_Flow_Price_Level_Factor | rdagent_task_sync | multi_source | moneyflow | reversal | cyq_perf,moneyflow,daily_basic | high | 0.0034 | 0.0565 | 100.0% | cp_his_high,db_close,db_total_mv,mf_sm_buy_amt,mf_sm_sell_amt |
| Stock_Industry_Return_Diff | rdagent_task_sync | multi_source | industry_relative | momentum | sector_data,price_volume | high | -0.0164 | -0.2141 | 99.7% | close,sw2_pct_change |
| TotalMoneyFlow_IndustryFlow_Intensity_Factor | rdagent_task_sync | multi_source | moneyflow | momentum | moneyflow,sector_data | high | 0.0014 | 0.1010 | 100.0% | mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt,mf_md_buy_amt,mf_md_sell_amt,mf_sm_buy_amt,mf_sm_sell_amt,sw2_mf_net_vol |
| TurnoverRateAdjustedPE | rdagent_task_sync | daily_basic | valuation | value_premium | daily_basic | high | 0.0187 | 0.3148 | 78.6% | db_pe,db_turnover_rate |
| Turnover_Low_Chip_Ratio | rdagent_task_sync | multi_source | turnover | reversal | cyq_perf,daily_basic | high | -0.0466 | -0.2885 | 100.0% | cp_cost_5pct,db_close,db_turnover_rate |
| Turnover_NetFlow_Adj | rdagent_task_sync | multi_source | moneyflow | momentum | moneyflow,daily_basic,price_volume | high | 0.0050 | 0.0497 | 100.0% | amount,db_turnover_rate,mf_net_amt |
| UltraLargeOrderBuyVolumeLowPriceSupportFactor | rdagent_task_sync | multi_source | moneyflow | reversal | cyq_perf,moneyflow | high | -0.0291 | -0.2998 | 100.0% | cp_his_low,mf_elg_buy_vol |
| Ultra_Large_Flow_Intensity | rdagent_task_sync | multi_source | moneyflow | momentum | moneyflow,daily_basic | high | -0.0023 | -0.0468 | 100.0% | db_total_mv,mf_elg_buy_amt,mf_elg_sell_amt |
| Ultra_Large_Net_Volume_Industry_Strength | rdagent_task_sync | multi_source | size | momentum | moneyflow,sector_data,daily_basic | high | -0.0010 | -0.0327 | 99.7% | db_circ_mv,mf_elg_buy_vol,mf_elg_sell_vol,sw2_vol |
| VPR | rdagent_task_sync | price_volume | momentum | momentum | price_volume | high | -0.0124 | -0.1245 | 100.0% | close,volume |
| VSTD5 | alpha158 | price_volume | price_volume | liquidity_premium | price_volume | high | -0.0105 | -0.1901 | 99.9% | close,high,low,open,volume |
| ValuationDeviationPremium | rdagent_task_sync | multi_source | quality | value_premium | sector_data,daily_basic | high | -0.0063 | -0.0722 | 78.4% | db_pe,sw2_pe |
| Valuation_Chip_Support | rdagent_task_sync | multi_source | valuation | value_premium | cyq_perf,daily_basic | high | 0.0104 | 0.0679 | 99.8% | cp_cost_50pct,db_close,db_pb |
| Valuation_Cost_Deviation | rdagent_task_sync | multi_source | valuation | value_premium | cyq_perf,daily_basic,price_volume | high | 0.0252 | 0.2077 | 80.3% | close,cp_weight_avg,db_pe,db_pe_ttm |
| ValueGrowthCrossover | rdagent_task_sync | multi_source | growth | value_premium | bak_basic,daily_basic | high | 0.0066 | 0.0950 | 78.6% | bb_rev_yoy,db_pe |
| ValueMomentumFlow_RankComposite | rdagent_task_sync | price_volume | statistical | momentum | price_volume | high | -0.0250 | -0.2075 | 75.2% | amount,close,factor,high,low,open,volume |
| ValueMomentumInteraction | rdagent_task_sync | multi_source | valuation | momentum | daily_basic,price_volume | high | -0.0340 | -0.3249 | 99.8% | close,db_pb |
| ValueSizeComposite | rdagent_task_sync | price_volume | valuation | value_premium | price_volume | high | 0.0103 | 0.0657 | 99.8% | amount,close,factor,high,low,open,volume |
| Value_PBInv_Momentum_20D | rdagent_task_sync | price_volume | valuation | value_premium | price_volume | high | 0.0403 | 0.2992 | 100.0% | amount,close,factor,high,low,open,volume |
| Value_PBInv_Momentum_VolAdj_20D | rdagent_task_sync | price_volume | volatility | value_premium | price_volume | high | 0.0299 | 0.2856 | 98.7% | amount,close,factor,high,low,open,volume |
| VolAdj_Momentum_10D | rdagent_task_sync | price_volume | volatility | momentum | price_volume | high | -0.0268 | -0.2538 | 100.0% | amount,close,factor,high,low,open,volume |
| Volatility_Adjusted_Momentum_14D | rdagent_task_sync | price_volume | volatility | momentum | price_volume | high | -0.0337 | -0.2874 | 100.0% | close |
| Volatility_Adjusted_Turnover | rdagent_task_sync | multi_source | turnover | liquidity_premium | daily_basic,price_volume | high | -0.0092 | -0.0694 | 100.0% | close,db_turnover_rate |
| VolumePriceDivergence | rdagent_task_sync | price_volume | momentum | reversal | price_volume | high | -0.0126 | -0.1861 | 100.0% | amount,close,factor,high,low,open,volume |
| Volume_IndustryAmount_Ratio | rdagent_task_sync | multi_source | turnover | liquidity_premium | sector_data,price_volume | high | -0.0235 | -0.1856 | 99.7% | sw2_amount,volume |
| Volume_Industry_Relative | rdagent_task_sync | multi_source | turnover | liquidity_premium | sector_data,price_volume | high | -0.0255 | -0.2607 | 99.7% | sw2_vol,volume |
| WVMA5 | alpha158 | price_volume | price_volume | reversal | price_volume | high | -0.0077 | -0.1528 | 99.9% | close,high,low,open,volume |
| WVMA60 | alpha158 | price_volume | price_volume | reversal | price_volume | high | -0.0100 | -0.1755 | 98.7% | close,high,low,open,volume |
| WinRate_VolumeRatio_Ratio | rdagent_task_sync | multi_source | chip_cost | crowding | cyq_perf,daily_basic | high | -0.0038 | -0.0414 | 92.5% | cp_winner_rate,db_volume_ratio |
| asset_turnover_efficiency | rdagent_task_sync | multi_source | quality | value_premium | bak_basic,daily_basic | high | 0.0077 | 0.0492 | 100.0% | bb_total_assets,db_total_mv |
| bb_cp_growth_cost | rdagent_task_sync | multi_source | chip_cost | value_premium | cyq_perf,bak_basic | high | 0.0026 | 0.0839 | 100.0% | bb_eps,bb_profit_yoy,cp_cost_50pct |
| bb_cp_momentum | rdagent_task_sync | multi_source | momentum | momentum | cyq_perf,bak_basic,price_volume | high | -0.0225 | -0.2482 | 79.1% | bb_pe_dyn,close,cp_cost_50pct |
| bb_cp_quality | rdagent_task_sync | multi_source | chip_cost | quality | cyq_perf,bak_basic | high | -0.0009 | -0.0071 | 100.0% | bb_eps,cp_cost_15pct,cp_cost_85pct,cp_weight_avg |
| bb_pe_dyn_inv_rank | rdagent_task_sync | bak_basic | valuation | value_premium | bak_basic | high | 0.0037 | 0.0226 | 79.1% | bb_pe_dyn |
| bb_pe_dyn_to_pb | rdagent_task_sync | multi_source | valuation | value_premium | bak_basic,daily_basic | high | 0.0027 | 0.0799 | 99.8% | bb_pe_dyn,db_pb |
| bb_valuation_growth_dynamic_composite | rdagent_task_sync | bak_basic | growth | value_premium | bak_basic | high | 0.0029 | 0.0692 | 85.2% | bb_pe_dyn,bb_profit_yoy |
| bid_ask_spread_change_factor | rdagent_task_sync | price_volume | volatility | liquidity_premium | price_volume | high | -0.0372 | -0.2198 | 100.0% | close,high,low |
| bollinger_flow_volatility_composite | rdagent_task_sync | multi_source | momentum | momentum | moneyflow,price_volume | high | 0.0053 | 0.1152 | 99.1% | amount,close,factor,high,low,mf_elg_buy_amt,mf_elg_sell_amt,open,volume |
| book_value_price_ratio | rdagent_task_sync | multi_source | valuation | value_premium | bak_basic,price_volume | high | 0.0126 | 0.0828 | 100.0% | bb_bvps,close |
| bvps_to_historical_high_ratio | rdagent_task_sync | multi_source | valuation | value_premium | cyq_perf,bak_basic | high | 0.0008 | 0.0062 | 100.0% | bb_bvps,cp_his_high |
| chip_concentration | rdagent_task_sync | cyq_perf | chip_cost | crowding | cyq_perf | high | -0.0006 | -0.0063 | 100.0% | cp_cost_5pct,cp_cost_95pct,cp_weight_avg |
| chip_concentration_index | rdagent_task_sync | cyq_perf | chip_cost | microstructure | cyq_perf | high | -0.0027 | -0.0288 | 100.0% | cp_cost_15pct,cp_cost_85pct,cp_weight_avg |
| chip_concentration_price_breakthrough_momentum | rdagent_task_sync | multi_source | chip_cost | crowding | cyq_perf,price_volume | high | -0.0351 | -0.2944 | 100.0% | close,cp_cost_15pct,cp_cost_50pct,cp_cost_85pct,cp_cost_95pct,cp_his_high |
| chip_concentration_price_position | rdagent_task_sync | multi_source | chip_cost | reversal | cyq_perf,price_volume | high | -0.0046 | -0.0856 | 100.0% | close,cp_cost_15pct,cp_cost_50pct,cp_his_low |
| chip_concentration_width | rdagent_task_sync | cyq_perf | chip_cost | crowding | cyq_perf | high | -0.0061 | -0.0400 | 100.0% | cp_cost_15pct,cp_cost_85pct |
| chip_distribution_shape | rdagent_task_sync | multi_source | chip_cost | crowding | cyq_perf,price_volume | high | 0.0055 | 0.0704 | 99.8% | close,cp_cost_50pct,cp_cost_5pct,cp_cost_95pct,cp_weight_avg |
| chip_liquidity_interaction | rdagent_task_sync | multi_source | chip_cost | crowding | cyq_perf,daily_basic | high | -0.0383 | -0.3289 | 92.5% | cp_winner_rate,db_turnover_rate,db_volume_ratio |
| chip_median_deviation | rdagent_task_sync | cyq_perf | chip_cost | crowding | cyq_perf | high | -0.0102 | -0.1115 | 100.0% | cp_cost_50pct,cp_weight_avg |
| chip_mid_spread | rdagent_task_sync | cyq_perf | chip_cost | crowding | cyq_perf | high | -0.0078 | -0.0526 | 100.0% | cp_cost_15pct,cp_cost_50pct |
| chip_pressure_breakout_factor | rdagent_task_sync | multi_source | chip_cost | reversal | cyq_perf,price_volume | high | -0.0203 | -0.1835 | 100.0% | close,cp_cost_85pct |
| chip_pressure_release_signal | rdagent_task_sync | multi_source | chip_cost | reversal | cyq_perf,price_volume | high | -0.0208 | -0.2060 | 100.0% | close,cp_his_high,cp_weight_avg |
| chip_pressure_winner_cost | rdagent_task_sync | multi_source | chip_cost | reversal | cyq_perf,price_volume | high | -0.0239 | -0.2448 | 100.0% | close,cp_weight_avg,cp_winner_rate |
| chip_shape_skewness | rdagent_task_sync | cyq_perf | chip_cost | microstructure | cyq_perf | high | 0.0086 | 0.1001 | 100.0% | cp_cost_50pct,cp_cost_5pct,cp_cost_95pct |
| chip_support_intensity_free_turnover_weighted | rdagent_task_sync | multi_source | chip_cost | reversal | cyq_perf,daily_basic,price_volume | high | -0.0485 | -0.3891 | 99.8% | close,cp_cost_50pct,cp_cost_5pct,db_turnover_rate_f |
| close_return_5d_vol_adj | rdagent_task_sync | price_volume | volatility | momentum | price_volume | high | -0.0285 | -0.2363 | 100.0% | amount,close,factor,high,low,open,volume |
| composite_sentiment_liquidity | rdagent_task_sync | multi_source | moneyflow | crowding | moneyflow,daily_basic,price_volume | high | -0.0051 | -0.0935 | 100.0% | amount,close,db_circ_mv,db_turnover_rate,mf_net_amt,volume |
| conditional_momentum_volatility | rdagent_task_sync | price_volume | volatility | momentum | price_volume | high | -0.0395 | -0.3220 | 100.0% | close |
| cost_concentration_index | rdagent_task_sync | cyq_perf | chip_cost | microstructure | cyq_perf | high | 0.0048 | 0.0857 | 100.0% | cp_cost_15pct,cp_cost_5pct,cp_cost_85pct,cp_cost_95pct |
| cost_deviation | rdagent_task_sync | multi_source | chip_cost | reversal | cyq_perf,price_volume | high | -0.0269 | -0.2443 | 100.0% | close,cp_weight_avg |
| cost_pressure_deviation | rdagent_task_sync | multi_source | chip_cost | reversal | cyq_perf,price_volume | high | -0.0208 | -0.2017 | 100.0% | close,cp_cost_15pct,cp_cost_50pct,cp_cost_85pct |
| cost_pressure_winner_rate | rdagent_task_sync | multi_source | chip_cost | crowding | cyq_perf,price_volume | high | -0.0307 | -0.2665 | 100.0% | close,cp_his_high,cp_his_low,cp_winner_rate |
| cost_spread_normalized_ma | rdagent_task_sync | cyq_perf | chip_cost | liquidity_premium | cyq_perf | high | 0.0020 | 0.0242 | 100.0% | cp_cost_5pct,cp_cost_95pct,cp_weight_avg |
| cp_cost_50pct_div_bb_bvps | rdagent_task_sync | multi_source | chip_cost | value_premium | cyq_perf,bak_basic | high | -0.0135 | -0.1002 | 100.0% | bb_bvps,cp_cost_50pct |
| cp_cost_position | rdagent_task_sync | multi_source | chip_cost | reversal | cyq_perf,price_volume | high | -0.0237 | -0.2223 | 100.0% | close,cp_cost_5pct,cp_cost_95pct |
| cp_cost_pressure_test | rdagent_task_sync | multi_source | chip_cost | reversal | cyq_perf,price_volume | high | -0.0223 | -0.2193 | 100.0% | close,cp_cost_15pct,cp_cost_50pct,cp_cost_85pct,cp_his_high,cp_his_low |
| cp_cost_stability | rdagent_task_sync | cyq_perf | chip_cost | microstructure | cyq_perf | high | 0.0178 | 0.1582 | 86.6% | cp_cost_15pct,cp_cost_85pct,cp_weight_avg |
| cp_winner_rate_momentum | rdagent_task_sync | cyq_perf | chip_cost | reversal | cyq_perf | high | -0.0118 | -0.1088 | 100.0% | cp_winner_rate |
| db_hl_range_10d_div_db_turnover_rate_f | rdagent_task_sync | multi_source | turnover | liquidity_premium | daily_basic,price_volume | high | 0.0083 | 0.0578 | 100.0% | db_turnover_rate_f,high,low |
| db_mf_volatility_coupling | rdagent_task_sync | multi_source | volatility | momentum | moneyflow,price_volume | high | 0.0075 | 0.2053 | 100.0% | amount,high,low,mf_elg_buy_amt,mf_elg_sell_amt |
| db_turnover_rate_mom | rdagent_task_sync | daily_basic | turnover | reversal | daily_basic | high | -0.0322 | -0.3825 | 100.0% | db_turnover_rate |
| db_value_turnover | rdagent_task_sync | daily_basic | valuation | value_premium | daily_basic | high | -0.0292 | -0.3882 | 75.2% | db_pe_ttm,db_turnover_rate |
| defensive_low_vol_high_turnover | rdagent_task_sync | multi_source | turnover | reversal | daily_basic,price_volume | high | -0.0342 | -0.2838 | 100.0% | amount,close,db_turnover_rate,factor,high,low,open,volume |
| divergence_volatility_momentum | rdagent_task_sync | price_volume | volatility | momentum | price_volume | high | 0.0220 | 0.3233 | 100.0% | amount,close,factor,high,low,open,volume |
| dividend_flow_risk_adjusted | rdagent_task_sync | multi_source | dividend | quality | moneyflow,daily_basic,dividend,price_volume | high | -0.0139 | -0.2689 | 76.7% | amount,close,db_dv_ttm,factor,high,low,mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt,open,volume |
| dividend_flow_risk_adjusted_v2 | rdagent_task_sync | multi_source | dividend | quality | moneyflow,daily_basic,dividend,price_volume | high | -0.0114 | -0.2309 | 76.7% | amount,close,db_dv_ttm,factor,high,low,mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt,open,volume |
| dividend_flow_volatility_adjusted | rdagent_task_sync | multi_source | dividend | quality | moneyflow,daily_basic,dividend,price_volume | high | -0.0048 | -0.0981 | 76.7% | amount,close,db_dv_ttm,factor,high,low,mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt,open,volume |
| dividend_growth_interaction | rdagent_task_sync | multi_source | dividend | quality | bak_basic,daily_basic,dividend | high | 0.0050 | 0.0823 | 94.7% | bb_rev_yoy,db_dv_ratio |
| dynamic_ensemble_flow_volatility_valuation | rdagent_task_sync | multi_source | machine_learning | momentum | cyq_perf,moneyflow,daily_basic,price_volume | high | 0.0205 | 0.1670 | 99.8% | amount,close,cp_winner_rate,db_pb,db_turnover_rate,factor,high,low,mf_elg_buy_amt,mf_elg_sell_amt,open,volume |
| dynamic_flow_volatility_sentiment | rdagent_task_sync | multi_source | moneyflow | momentum | moneyflow,daily_basic,price_volume | high | 0.0424 | 0.3507 | 99.7% | amount,db_pb,db_turnover_rate,mf_elg_buy_amt,mf_elg_sell_amt |
| dynamic_pe_inv_elg_net_zscore | rdagent_task_sync | multi_source | valuation | value_premium | moneyflow,bak_basic,price_volume | high | 0.0036 | 0.0296 | 79.1% | amount,bb_pe_dyn,mf_elg_buy_amt,mf_elg_sell_amt |
| dynamic_pe_inv_momentum | rdagent_task_sync | bak_basic | valuation | momentum | bak_basic | high | 0.0220 | 0.2151 | 78.9% | bb_pe_dyn |
| dynamic_pe_inv_momentum_breakout | rdagent_task_sync | multi_source | valuation | momentum | bak_basic,price_volume | high | 0.0231 | 0.3983 | 78.9% | bb_pe_dyn,close |
| dynamic_pe_inv_momentum_trend | rdagent_task_sync | bak_basic | valuation | value_premium | bak_basic | high | -0.0122 | -0.1855 | 76.6% | bb_pe_dyn |
| dynamic_pe_inv_momentum_turnover_ratio | rdagent_task_sync | multi_source | valuation | value_premium | bak_basic,daily_basic | high | 0.0234 | 0.2282 | 78.9% | bb_pe_dyn,db_turnover_rate |
| dynamic_pe_inv_trend_strength | rdagent_task_sync | multi_source | valuation | momentum | bak_basic,price_volume | high | 0.0089 | 0.1193 | 78.7% | bb_pe_dyn,close |
| dynamic_pe_momentum_rank | rdagent_task_sync | bak_basic | valuation | value_premium | bak_basic | high | 0.0277 | 0.2774 | 78.7% | bb_pe_dyn |
| dynamic_pe_profit_growth_weighted_quantile | rdagent_task_sync | bak_basic | growth | value_premium | bak_basic | high | 0.0015 | 0.0237 | 100.0% | bb_pe_dyn,bb_profit_yoy |
| dynamic_val_chip_elasticity_ratio | rdagent_task_sync | multi_source | valuation | value_premium | cyq_perf,bak_basic | high | 0.0024 | 0.0172 | 79.1% | bb_pe_dyn,cp_cost_15pct,cp_cost_85pct,cp_weight_avg |
| dynamic_val_holder_concentration_conditional | rdagent_task_sync | multi_source | shareholder | crowding | bak_basic,shareholder | high | -0.0012 | -0.0284 | 94.6% | bb_holder_num,bb_pe_dyn |
| dynamic_valuation_factor | rdagent_task_sync | multi_source | valuation | value_premium | bak_basic,daily_basic | high | 0.0215 | 0.1441 | 79.1% | bb_pe_dyn,db_pb,db_total_mv |
| dynamic_weighted_elg_momentum | rdagent_task_sync | price_volume | volatility | momentum | price_volume | high | -0.0342 | -0.2808 | 99.5% | close |
| earnings_growth | rdagent_task_sync | bak_basic | growth | quality | bak_basic | high | 0.0058 | 0.0712 | 100.0% | bb_rev_yoy |
| elg_buy_cost_ratio | rdagent_task_sync | multi_source | chip_cost | reversal | cyq_perf,moneyflow | high | -0.0364 | -0.3652 | 100.0% | cp_cost_50pct,mf_elg_buy_amt |
| elg_flow_high_price_interaction | rdagent_task_sync | multi_source | moneyflow | momentum | cyq_perf,moneyflow | high | -0.0022 | -0.0422 | 100.0% | cp_his_high,mf_elg_buy_amt,mf_elg_sell_amt |
| elg_flow_structure | rdagent_task_sync | price_volume | moneyflow | crowding | price_volume | high | -0.0234 | -0.3023 | 99.5% | amount,close,factor,high,low,open,volume |
| elg_net_buy_to_turnover | rdagent_task_sync | multi_source | turnover | momentum | moneyflow,daily_basic | high | -0.0030 | -0.0663 | 100.0% | db_turnover_rate_f,mf_elg_buy_amt,mf_elg_sell_amt |
| elg_net_flow_strength | rdagent_task_sync | moneyflow | moneyflow | momentum | moneyflow | medium | -0.0045 | -0.1114 | 100.0% | - |
| elg_net_flow_volatility_adjusted_5d | rdagent_task_sync | price_volume | volatility | reversal | price_volume | high | -0.0170 | -0.3390 | 99.0% | amount,close,factor,high,low,open,volume |
| elg_share_change_5d | rdagent_task_sync | price_volume | moneyflow | microstructure | price_volume | high | -0.0100 | -0.2631 | 73.8% | amount,close,factor,high,low,open,volume |
| factor_chip_concentration_price | rdagent_task_sync | multi_source | chip_cost | reversal | cyq_perf,price_volume | high | -0.0188 | -0.1872 | 99.3% | close,cp_cost_50pct,cp_cost_85pct |
| factor_large_order_net_ratio | rdagent_task_sync | multi_source | moneyflow | momentum | moneyflow,price_volume | high | -0.0053 | -0.1341 | 100.0% | amount,mf_lg_buy_amt,mf_lg_sell_amt |
| fixed_assets_market_cap_ratio | rdagent_task_sync | multi_source | quality | value_premium | bak_basic,daily_basic | high | 0.0055 | 0.0423 | 100.0% | bb_fixed_assets,db_total_mv |
| flow_cost_pressure | rdagent_task_sync | multi_source | moneyflow | momentum | cyq_perf,moneyflow,daily_basic,price_volume | high | 0.0053 | 0.0527 | 100.0% | close,cp_weight_avg,db_total_mv,mf_net_amt |
| flow_tier_strength | rdagent_task_sync | multi_source | moneyflow | reversal | moneyflow,price_volume | high | -0.0293 | -0.2863 | 100.0% | amount,mf_elg_buy_amt,mf_lg_buy_amt |
| fund_flow_acceleration | rdagent_task_sync | moneyflow | moneyflow | momentum | moneyflow | high | 0.0054 | 0.0572 | 100.0% | mf_net_amt |
| fund_flow_acceleration_momentum | rdagent_task_sync | moneyflow | moneyflow | momentum | moneyflow | high | 0.0072 | 0.0804 | 100.0% | mf_net_amt |
| fund_flow_net_amt_strength | rdagent_task_sync | moneyflow | moneyflow | momentum | moneyflow | high | 0.0058 | 0.0615 | 100.0% | mf_net_amt |
| gpr_minus_pe_ttm | rdagent_task_sync | multi_source | quality | value_premium | bak_basic,daily_basic | high | 0.0043 | 0.0343 | 75.2% | bb_gpr,db_pe_ttm |
| gross_margin_elg_flow_divergence | rdagent_task_sync | multi_source | quality | quality | moneyflow,bak_basic,daily_basic | high | -0.0026 | -0.0314 | 100.0% | bb_gpr,db_total_mv,mf_elg_buy_amt,mf_elg_sell_amt |
| gross_profit_liquidity_interaction | rdagent_task_sync | multi_source | valuation | value_premium | bak_basic,daily_basic | high | -0.0189 | -0.2291 | 92.5% | bb_gpr,db_circ_mv,db_volume_ratio |
| high_freq_order_imbalance_volatility | rdagent_task_sync | multi_source | volatility | reversal | moneyflow,price_volume | high | -0.0022 | -0.0781 | 100.0% | close,mf_lg_buy_amt,mf_lg_sell_amt,mf_net_amt |
| industry_adjusted_turnover_momentum | rdagent_task_sync | multi_source | turnover | momentum | sector_data,daily_basic,price_volume | high | -0.0118 | -0.2129 | 99.8% | close,db_turnover_rate,sw2_close |
| industry_stock_momentum_diff_10d | rdagent_task_sync | multi_source | industry_relative | momentum | sector_data,price_volume | high | 0.0352 | 0.4432 | 100.0% | close,sw2_close |
| industry_valuation_efficiency | rdagent_task_sync | multi_source | quality | value_premium | sector_data,bak_basic | high | -0.0060 | -0.0622 | 78.8% | bb_pe_dyn,sw2_pe |
| inst_attention | rdagent_task_sync | multi_source | chip_cost | crowding | cyq_perf,moneyflow | high | -0.0075 | -0.2638 | 83.8% | cp_winner_rate,mf_elg_buy_amt,mf_elg_sell_amt |
| institutional_net_buy_intensity | rdagent_task_sync | multi_source | size | momentum | moneyflow,daily_basic | high | -0.0033 | -0.0512 | 100.0% | db_circ_mv,mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt |
| inv_dyn_pe_to_median_cost | rdagent_task_sync | multi_source | valuation | value_premium | cyq_perf,bak_basic | high | 0.0053 | 0.0302 | 79.1% | bb_pe_dyn,cp_cost_50pct |
| large_net_inflow_momentum | rdagent_task_sync | multi_source | moneyflow | momentum | moneyflow,price_volume | high | -0.0013 | -0.0332 | 100.0% | amount,mf_net_amt |
| large_order_flow_relative_strength | rdagent_task_sync | multi_source | moneyflow | momentum | moneyflow,daily_basic,price_volume | high | -0.0022 | -0.0452 | 100.0% | amount,db_turnover_rate_f,mf_elg_buy_amt,mf_elg_sell_amt |
| large_order_inflow_strength | rdagent_task_sync | multi_source | moneyflow | crowding | moneyflow,daily_basic | high | -0.0466 | -0.3000 | 100.0% | db_circ_mv,mf_lg_buy_amt |
| large_order_net_buy_ratio | rdagent_task_sync | moneyflow | moneyflow | microstructure | moneyflow | high | -0.0128 | -0.3151 | 39.2% | mf_lg_buy_amt,mf_lg_sell_amt,mf_net_amt |
| large_order_net_inflow_strength | rdagent_task_sync | multi_source | moneyflow | momentum | moneyflow,price_volume | high | -0.0063 | -0.1363 | 100.0% | mf_elg_buy_vol,mf_elg_sell_vol,mf_lg_buy_vol,mf_lg_sell_vol,volume |
| lg_net_flow_momentum | rdagent_task_sync | moneyflow | volatility | momentum | moneyflow | high | 0.0124 | 0.1831 | 100.0% | mf_lg_buy_amt,mf_lg_sell_amt |
| liquidity_adjusted_flow | rdagent_task_sync | multi_source | moneyflow | momentum | moneyflow,daily_basic | high | 0.0052 | 0.0539 | 100.0% | db_circ_mv,db_turnover_rate_f,mf_net_amt |
| liquidity_adjusted_volatility | rdagent_task_sync | multi_source | turnover | liquidity_premium | daily_basic,price_volume | high | 0.0004 | 0.0027 | 100.0% | close,db_turnover_rate |
| liquidity_mismatch_pressure | rdagent_task_sync | multi_source | moneyflow | reversal | moneyflow,daily_basic | high | -0.0039 | -0.0604 | 100.0% | db_circ_mv,mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt,mf_sm_buy_amt,mf_sm_sell_amt |
| liquidity_sentiment_score | rdagent_task_sync | multi_source | turnover | momentum | moneyflow,daily_basic,price_volume | high | 0.0077 | 0.1586 | 100.0% | amount,db_turnover_rate,mf_sm_buy_amt,mf_sm_sell_amt |
| liquidity_turnover_5d | rdagent_task_sync | daily_basic | turnover | liquidity_premium | daily_basic | high | -0.0413 | -0.2455 | 100.0% | db_turnover_rate |
| log_value_ratio | rdagent_task_sync | multi_source | valuation | value_premium | daily_basic,price_volume | high | -0.0019 | -0.0166 | 75.2% | close,db_pe_ttm |
| lstm_temporal_flow_valuation_signal | rdagent_task_sync | multi_source | volatility | momentum | moneyflow,price_volume | high | -0.0039 | -0.0295 | 100.0% | amount,close,factor,high,low,mf_elg_buy_amt,mf_elg_sell_amt,open,volume |
| m_amount_concentration | manual | price_volume | statistical | reversal | price_volume | high | 0.0242 | 0.4387 | 99.9% | amount |
| m_amount_concentration_10d | manual | price_volume | turnover | liquidity_premium | price_volume | high | 0.0275 | 0.5017 | 100.0% | amount |
| m_atr_14d_inv | manual | price_volume | volatility | liquidity_premium | price_volume | high | 0.0350 | 0.1841 | 100.0% | close,high,low |
| m_atr_accel_negative | manual | price_volume | volatility | reversal | price_volume | high | 0.0379 | 0.3793 | 99.8% | close,high,low |
| m_atr_compression | manual | price_volume | volatility | reversal | price_volume | high | 0.0381 | 0.3795 | 99.8% | close,high,low |
| m_atr_contraction | JoinQuant | price_volume | volatility | reversal | price_volume | high | -0.0439 | -0.3936 | 100.0% | close,high,low |
| m_atr_percentile_250d | manual | price_volume | statistical | reversal | price_volume | high | 0.0400 | 0.3036 | 97.0% | close,high,low |
| m_bbwidth_percentile_250d | manual | price_volume | price_volume | reversal | price_volume | high | 0.0323 | 0.3115 | 98.0% | close |
| m_bbwidth_shrink | manual | price_volume | price_volume | reversal | price_volume | high | 0.0152 | 0.1844 | 99.9% | close |
| m_beta_60d | manual | multi_source | industry_relative | momentum | sector_data,price_volume | high | 0.0076 | 0.0786 | 100.0% | close,sw2_pct_change |
| m_beta_change_20d | manual | multi_source | industry_relative | momentum | sector_data,price_volume | high | 0.0076 | 0.1253 | 100.0% | close,sw2_pct_change |
| m_chip_cost_momentum_fusion | manual | multi_source | chip_cost | momentum | cyq_perf,price_volume | high | -0.0308 | -0.2508 | 100.0% | close,cp_weight_avg,cp_winner_rate |
| m_chip_moneyflow_divergence | manual | multi_source | moneyflow | momentum | cyq_perf,moneyflow | high | 0.0160 | 0.1817 | 100.0% | cp_winner_rate,mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt |
| m_chip_value_quality_fusion | manual | multi_source | chip_cost | quality | cyq_perf,bak_basic,daily_basic | high | -0.0089 | -0.0746 | 75.2% | bb_profit_yoy,cp_winner_rate,db_pe_ttm |
| m_close_location_value | manual | price_volume | price_volume | reversal | price_volume | high | -0.0222 | -0.1978 | 100.0% | close,high,low |
| m_close_slope_20d | manual | price_volume | correlation | reversal | price_volume | high | -0.0315 | -0.2323 | 99.6% | close |
| m_conditional_momentum_20d | manual | price_volume | volatility | momentum | price_volume | high | 0.0361 | 0.3339 | 100.0% | close |
| m_consecutive_narrow_range | manual | price_volume | price_volume | reversal | price_volume | high | 0.0157 | 0.2134 | 100.0% | high,low,open |
| m_corr_decay_5_20 | manual | multi_source | industry_relative | momentum | sector_data,price_volume | high | -0.0102 | -0.1673 | 99.8% | close,sw2_pct_change |
| m_corr_volume_return_20d | manual | price_volume | correlation | reversal | price_volume | high | -0.0126 | -0.1608 | 99.9% | close,volume |
| m_cp_underwater_pressure | manual | multi_source | chip_cost | reversal | cyq_perf,price_volume | high | -0.0196 | -0.2009 | 100.0% | close,cp_weight_avg,cp_winner_rate |
| m_cp_winner_delta_20d | manual | cyq_perf | chip_cost | reversal | cyq_perf | high | 0.0147 | 0.1384 | 99.9% | cp_winner_rate |
| m_donchian_width_compress | manual | price_volume | volatility | reversal | price_volume | high | 0.0274 | 0.2535 | 99.5% | close,high,low |
| m_downside_vol_ratio_20d | manual | price_volume | volatility | reversal | price_volume | high | -0.0291 | -0.2828 | 100.0% | close |
| m_drawdown_from_high | JoinQuant | price_volume | price_volume | reversal | price_volume | high | -0.0218 | -0.1678 | 100.0% | close,high |
| m_earnings_quality_composite | manual | multi_source | quality | quality | sector_data,bak_basic,daily_basic | high | 0.0089 | 0.1100 | 75.0% | bb_gpr,bb_rev_yoy,db_pe_ttm,sw2_pe |
| m_elg_ratio_flat_price | manual | multi_source | moneyflow | microstructure | moneyflow,price_volume | high | -0.0023 | -0.0259 | 99.9% | amount,close,mf_elg_buy_amt |
| m_excess_return_consistency_20d | manual | multi_source | industry_relative | momentum | sector_data,price_volume | high | -0.0148 | -0.2422 | 100.0% | close,sw2_pct_change |
| m_five_source_alpha | manual | multi_source | moneyflow | momentum | cyq_perf,moneyflow,sector_data,daily_basic,price_volume | high | -0.0183 | -0.1240 | 75.0% | close,cp_winner_rate,db_pe_ttm,mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt,sw2_pe |
| m_free_turnover_ind_neutral | manual | multi_source | turnover | liquidity_premium | sector_data,daily_basic | high | 0.0463 | 0.3695 | 99.9% | db_turnover_rate_f,sw2_amount,sw2_total_mv |
| m_free_turnover_percentile_250d | manual | daily_basic | turnover | reversal | daily_basic | high | -0.0453 | -0.4579 | 95.6% | db_turnover_rate_f |
| m_free_turnover_rate | manual | daily_basic | turnover | liquidity_premium | daily_basic | high | 0.0501 | 0.3259 | 100.0% | db_turnover_rate_f |
| m_gap_frequency_20d | manual | price_volume | price_volume | microstructure | price_volume | high | 0.0214 | 0.1203 | 100.0% | close,open |
| m_high_low_channel_pos | manual | price_volume | price_volume | reversal | price_volume | high | -0.0290 | -0.2472 | 100.0% | close |
| m_holder_concentration_change | manual | multi_source | shareholder | crowding | bak_basic,shareholder | high | 0.0094 | 0.2385 | 100.0% | bb_holder_num |
| m_idio_vol_60d | manual | multi_source | industry_relative | reversal | sector_data,price_volume | high | 0.0277 | 0.1650 | 100.0% | close,sw2_pct_change |
| m_ind_flow_deviate | manual | multi_source | moneyflow | momentum | moneyflow,sector_data,price_volume | high | 0.0096 | 0.1075 | 100.0% | amount,mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt,sw2_amount,sw2_mf_buy_elg_amt,sw2_mf_buy_lg_amt,sw2_mf_sell_elg_amt,sw2_mf_sell_lg_amt |
| m_ind_flow_residual_mom | manual | multi_source | moneyflow | momentum | moneyflow,sector_data,price_volume | high | -0.0035 | -0.0540 | 99.8% | amount,mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt,sw2_amount,sw2_mf_net_amt |
| m_ind_neutral_rev_5d | manual | multi_source | industry_relative | reversal | sector_data,price_volume | high | 0.0327 | 0.4000 | 98.4% | close,sw2_pct_change |
| m_ind_pb_rel_mom | manual | multi_source | valuation | reversal | sector_data,daily_basic | high | 0.0344 | 0.4170 | 99.1% | db_pb,sw2_pb |
| m_ind_rel_turnover | manual | multi_source | turnover | crowding | sector_data,daily_basic | high | 0.0363 | 0.3611 | 100.0% | db_turnover_rate,sw2_amount,sw2_total_mv |
| m_ind_residual_rev_turnover | manual | multi_source | turnover | reversal | sector_data,daily_basic,price_volume | high | 0.0114 | 0.0850 | 99.8% | close,db_turnover_rate,sw2_pct_change |
| m_ind_residual_vol_ratio | manual | multi_source | industry_relative | reversal | sector_data,price_volume | high | 0.0338 | 0.3420 | 99.9% | close,sw2_pct_change |
| m_industry_mf_large_divergence | manual | sector_data | industry_relative | momentum | sector_data | high | 0.0062 | 0.0478 | 100.0% | sw2_mf_buy_elg_amt,sw2_mf_buy_lg_amt,sw2_mf_sell_elg_amt,sw2_mf_sell_lg_amt,sw2_pct_change |
| m_industry_mf_strength_10d | manual | sector_data | industry_relative | momentum | sector_data | high | -0.0039 | -0.0339 | 99.7% | sw2_mf_net_amt,sw2_total_mv |
| m_industry_pb_deviation | manual | multi_source | valuation | value_premium | sector_data,daily_basic | high | 0.0170 | 0.3078 | 99.5% | db_pb,sw2_pb |
| m_industry_pe_deviation | manual | multi_source | valuation | value_premium | sector_data,daily_basic | high | -0.0066 | -0.0761 | 75.0% | db_pe_ttm,sw2_pe |
| m_industry_relative_mf_divergence | manual | multi_source | moneyflow | momentum | moneyflow,sector_data,daily_basic | high | -0.0103 | -0.1196 | 100.0% | db_circ_mv,mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt,sw2_mf_net_amt,sw2_total_mv |
| m_industry_reversal_20d | manual | sector_data | industry_relative | reversal | sector_data | high | 0.0158 | 0.1102 | 100.0% | sw2_close |
| m_industry_value_momentum_fusion | manual | multi_source | industry_relative | momentum | sector_data,price_volume | high | -0.0282 | -0.2147 | 99.6% | close,sw2_close,sw2_pe |
| m_industry_vol_ratio | manual | multi_source | turnover | liquidity_premium | sector_data,price_volume | high | -0.0291 | -0.5370 | 100.0% | sw2_vol,volume |
| m_intraday_range_60d_min_ratio | manual | price_volume | volatility | reversal | price_volume | high | 0.0450 | 0.4590 | 100.0% | high,low,open |
| m_intraday_range_compress | manual | price_volume | volatility | reversal | price_volume | high | 0.0329 | 0.3514 | 99.9% | high,low,open |
| m_intraday_range_ratio_5d | manual | price_volume | volatility | reversal | price_volume | high | 0.0474 | 0.2752 | 100.0% | close,high,low |
| m_keltner_squeeze | manual | price_volume | price_volume | reversal | price_volume | high | 0.0094 | 0.1677 | 100.0% | close,high,low |
| m_max_drawdown_20d | manual | price_volume | volatility | reversal | price_volume | high | 0.0056 | 0.0349 | 100.0% | close |
| m_max_return_20d | manual | price_volume | momentum | reversal | price_volume | high | 0.0401 | 0.2644 | 100.0% | close |
| m_md_leverage_ratio | manual | multi_source | margin | crowding | margin_detail,daily_basic | high | -0.0095 | -0.0751 | 98.5% | db_circ_mv,rzye |
| m_md_net_repay_rate_10d | manual | margin_detail | margin | reversal | margin_detail | high | 0.0210 | 0.3438 | 98.7% | rzche,rzmre,rzye |
| m_md_rqyl_vol_corr_20d | manual | price_volume | correlation | reversal | price_volume | high | 0.0100 | 0.1889 | 79.2% | volume |
| m_md_rz_chip_diverge | manual | cyq_perf | chip_cost | reversal | cyq_perf | high | -0.0083 | -0.0957 | 99.9% | cp_winner_rate |
| m_md_rz_rq_sentiment | manual | margin_detail | margin | momentum | margin_detail | high | 0.0093 | 0.1939 | 77.2% | rqye,rzye |
| m_md_rzmre_intensity_10d | manual | multi_source | margin | momentum | margin_detail,price_volume | high | 0.0066 | 0.1088 | 98.6% | amount,rzmre |
| m_md_rzrqye_mom_20d | manual | margin_detail | margin | momentum | margin_detail | high | -0.0247 | -0.3267 | 98.5% | rzrqye |
| m_md_short_pressure | manual | multi_source | margin | crowding | margin_detail,daily_basic,price_volume | high | 0.0050 | 0.0652 | 98.5% | close,db_circ_mv,rqmcl |
| m_mf_big_order_persistence | manual | moneyflow | moneyflow | momentum | moneyflow | high | -0.0115 | -0.2433 | 100.0% | mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt |
| m_mf_retail_panic_10d | manual | moneyflow | moneyflow | reversal | moneyflow | high | 0.0151 | 0.2998 | 99.9% | mf_sm_buy_amt,mf_sm_sell_amt |
| m_mf_smart_money_ratio | manual | multi_source | moneyflow | momentum | moneyflow,price_volume | high | -0.0178 | -0.3206 | 99.9% | amount,mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt |
| m_mkt_sensitivity_asymmetry | manual | multi_source | industry_relative | reversal | sector_data,price_volume | high | -0.0198 | -0.1040 | 11.1% | close,sw2_pct_change |
| m_ml_cross_sectional_skew | manual | price_volume | statistical | reversal | price_volume | high | 0.0171 | 0.2363 | 99.9% | close |
| m_ml_pca_momentum_vol | manual | price_volume | volatility | momentum | price_volume | high | -0.0276 | -0.2357 | 99.9% | close |
| m_ml_rank_ic_adaptive | manual | price_volume | volatility | momentum | price_volume | high | -0.0111 | -0.0732 | 99.9% | close |
| m_ml_residual_mom_20d | manual | multi_source | industry_relative | momentum | sector_data,daily_basic,price_volume | high | 0.0008 | 0.0055 | 99.9% | close,db_total_mv,sw2_pct_change |
| m_mom_acceleration_10d | manual | price_volume | momentum | momentum | price_volume | high | -0.0249 | -0.2066 | 99.8% | close |
| m_mom_residual_20d | manual | multi_source | industry_relative | reversal | sector_data,price_volume | high | -0.0389 | -0.4618 | 99.9% | close,sw2_pct_change |
| m_mom_reversal_ratio_5_20 | manual | price_volume | momentum | reversal | price_volume | high | 0.0072 | 0.1322 | 99.9% | close |
| m_mom_volume_divergence_10d | manual | price_volume | statistical | reversal | price_volume | high | 0.0064 | 0.0700 | 99.9% | close,volume |
| m_mom_weighted_strength_20d | manual | price_volume | momentum | reversal | price_volume | high | -0.0440 | -0.3424 | 99.9% | amount,close |
| m_momentum_profit_confirmed | manual | multi_source | statistical | momentum | bak_basic,price_volume | high | -0.0314 | -0.2752 | 100.0% | bb_profit_yoy,close |
| m_net_inflow_direction_5d | manual | moneyflow | moneyflow | momentum | moneyflow | high | -0.0115 | -0.2485 | 100.0% | mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt |
| m_overnight_return_5d | manual | price_volume | momentum | momentum | price_volume | high | 0.0054 | 0.0673 | 100.0% | close,open |
| m_positive_return_ratio_20d | manual | price_volume | momentum | reversal | price_volume | high | -0.0176 | -0.1988 | 100.0% | close |
| m_price_efficiency_10d | manual | price_volume | quality | momentum | price_volume | high | -0.0085 | -0.1030 | 100.0% | close |
| m_profit_yoy_change | manual | bak_basic | growth | quality | bak_basic | high | 0.0014 | 0.0512 | 100.0% | bb_profit_yoy |
| m_quad_source_alpha | manual | multi_source | moneyflow | momentum | cyq_perf,moneyflow,daily_basic,price_volume | high | -0.0197 | -0.1585 | 75.2% | close,cp_winner_rate,db_pe_ttm,mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt |
| m_r_squared_60d | manual | multi_source | industry_relative | reversal | sector_data,price_volume | high | -0.0161 | -0.1905 | 100.0% | close,sw2_pct_change |
| m_range_compress_vol_expand | manual | price_volume | statistical | momentum | price_volume | high | -0.0068 | -0.0969 | 99.9% | high,low,open,volume |
| m_rank_interaction_pe_mom | manual | multi_source | valuation | value_premium | daily_basic,price_volume | high | -0.0148 | -0.1098 | 75.2% | close,db_pe_ttm |
| m_regime_vol_reversal | manual | price_volume | volatility | reversal | price_volume | high | 0.0179 | 0.1659 | 100.0% | close,high,low |
| m_residual_vol_change_20d | manual | multi_source | industry_relative | reversal | sector_data,price_volume | high | 0.0302 | 0.3418 | 100.0% | close,sw2_pct_change |
| m_ret_r2_neg_20d | manual | price_volume | correlation | reversal | price_volume | high | 0.0101 | 0.1342 | 100.0% | close |
| m_return_autocorr_5d | manual | price_volume | momentum | reversal | price_volume | high | 0.0074 | 0.1021 | 99.8% | close |
| m_return_kurtosis_20d | manual | price_volume | statistical | reversal | price_volume | high | 0.0090 | 0.1929 | 100.0% | close |
| m_rs_momentum | JoinQuant | price_volume | momentum | momentum | price_volume | high | -0.0332 | -0.2250 | 99.8% | close |
| m_sector_mf_divergence_lg | manual | multi_source | moneyflow | momentum | moneyflow,sector_data | high | 0.0084 | 0.0956 | 99.9% | mf_lg_buy_amt,mf_lg_sell_amt,sw2_mf_buy_lg_amt,sw2_mf_sell_lg_amt |
| m_sector_mf_sm_md_ratio | manual | sector_data | industry_relative | momentum | sector_data | high | 0.0071 | 0.1004 | 100.0% | sw2_mf_buy_md_amt,sw2_mf_buy_sm_amt,sw2_mf_sell_md_amt,sw2_mf_sell_sm_amt |
| m_sector_momentum_spread | manual | multi_source | industry_relative | reversal | sector_data,price_volume | high | 0.0310 | 0.4053 | 99.9% | close,sw2_close |
| m_sector_relative_vol_strength | manual | multi_source | industry_relative | reversal | sector_data,price_volume | high | -0.0310 | -0.5301 | 99.8% | sw2_vol,volume |
| m_size_float_ratio | manual | daily_basic | size | liquidity_premium | daily_basic | high | 0.0048 | 0.0634 | 100.0% | db_free_share,db_total_share |
| m_size_mv_change_20d | manual | daily_basic | size | reversal | daily_basic | high | 0.0448 | 0.3243 | 100.0% | db_total_mv |
| m_size_nonlinear_mv | manual | daily_basic | size | liquidity_premium | daily_basic | high | 0.0181 | 0.1100 | 100.0% | db_total_mv |
| m_smart_money_ratio_5d | manual | multi_source | moneyflow | momentum | moneyflow,price_volume | high | -0.0182 | -0.3297 | 100.0% | amount,mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt |
| m_stealth_accumulation_5d | manual | multi_source | moneyflow | momentum | moneyflow,price_volume | high | -0.0068 | -0.1431 | 99.9% | close,mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt |
| m_stock_vs_industry_mom_20d | manual | multi_source | industry_relative | reversal | sector_data,price_volume | high | -0.0380 | -0.4573 | 100.0% | close,sw2_close |
| m_sw2_elg_individual_diverge | manual | multi_source | moneyflow | momentum | moneyflow,sector_data,price_volume | high | -0.0034 | -0.0606 | 99.9% | amount,mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt,sw2_amount,sw2_mf_buy_elg_amt,sw2_mf_sell_elg_amt |
| m_sw2_elg_net_vol_pct | manual | sector_data | industry_relative | crowding | sector_data | high | -0.0109 | -0.1352 | 100.0% | sw2_mf_buy_elg_vol,sw2_mf_sell_elg_vol,sw2_vol |
| m_sw2_gap_open_relative | manual | multi_source | industry_relative | momentum | sector_data,price_volume | high | 0.0080 | 0.1790 | 99.9% | close,open,sw2_close,sw2_open |
| m_sw2_net_vol_momentum | manual | sector_data | industry_relative | momentum | sector_data | high | 0.0037 | 0.0370 | 100.0% | sw2_mf_net_vol,sw2_vol |
| m_sw2_stock_excess_vol | manual | multi_source | industry_relative | reversal | sector_data,price_volume | high | 0.0289 | 0.2682 | 99.8% | close,sw2_pct_change |
| m_sw2_vol_ratio_to_sector | manual | multi_source | industry_relative | reversal | sector_data,price_volume | high | -0.0325 | -0.2992 | 99.9% | high,low,sw2_high,sw2_low |
| m_tail_risk_5pct | manual | price_volume | price_volume | reversal | price_volume | high | -0.0154 | -0.0836 | 100.0% | close |
| m_tech_atr_ratio_14d | manual | price_volume | volatility | liquidity_premium | price_volume | high | 0.0416 | 0.2151 | 100.0% | close,high,low |
| m_tech_bollinger_width_20d | manual | price_volume | volatility | reversal | price_volume | high | 0.0337 | 0.2084 | 99.9% | close |
| m_tech_macd_signal | manual | price_volume | price_volume | reversal | price_volume | high | -0.0322 | -0.2449 | 100.0% | close |
| m_tech_obv_change_10d | manual | price_volume | price_volume | momentum | price_volume | high | -0.0234 | -0.3245 | 100.0% | close,volume |
| m_tech_rsi_14d | manual | price_volume | price_volume | reversal | price_volume | high | 0.0362 | 0.2842 | 100.0% | close |
| m_turnover_abnormal_20d | manual | daily_basic | turnover | reversal | daily_basic | high | 0.0393 | 0.4183 | 99.9% | db_turnover_rate |
| m_turnover_accel | manual | daily_basic | turnover | reversal | daily_basic | high | 0.0381 | 0.4104 | 99.8% | db_turnover_rate |
| m_turnover_acceleration | manual | daily_basic | turnover | reversal | daily_basic | high | 0.0128 | 0.1730 | 99.8% | db_turnover_rate_f |
| m_turnover_autocorr_5d | manual | daily_basic | turnover | reversal | daily_basic | high | 0.0086 | 0.1670 | 99.9% | db_turnover_rate |
| m_turnover_breakout_ratio | manual | daily_basic | turnover | reversal | daily_basic | high | -0.0480 | -0.5380 | 99.9% | db_turnover_rate |
| m_turnover_mean_up_std_down | manual | daily_basic | turnover | reversal | daily_basic | high | -0.0170 | -0.2832 | 99.8% | db_turnover_rate |
| m_turnover_mf_divergence | manual | multi_source | moneyflow | momentum | moneyflow,daily_basic | high | 0.0243 | 0.1866 | 100.0% | db_turnover_rate,mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt |
| m_turnover_percentile_250d | manual | daily_basic | turnover | reversal | daily_basic | high | 0.0456 | 0.4727 | 100.0% | db_turnover_rate |
| m_turnover_zscore_60d | manual | daily_basic | turnover | reversal | daily_basic | high | 0.0380 | 0.3838 | 100.0% | db_turnover_rate_f |
| m_up_down_vol_asymmetry | manual | price_volume | volatility | reversal | price_volume | high | -0.0187 | -0.2755 | 100.0% | close,volume |
| m_value_lowliq_composite | manual | daily_basic | valuation | value_premium | daily_basic | high | 0.0209 | 0.1040 | 75.2% | db_pe_ttm,db_turnover_rate |
| m_value_momentum_quality_3d | manual | multi_source | quality | value_premium | bak_basic,daily_basic,price_volume | high | -0.0166 | -0.1427 | 75.2% | bb_profit_yoy,close,db_pe_ttm |
| m_vol_compress_composite | manual | price_volume | volatility | reversal | price_volume | high | -0.0405 | -0.3557 | 100.0% | close,high,low,open |
| m_vol_of_vol_20d | manual | price_volume | volatility | reversal | price_volume | high | 0.0314 | 0.2187 | 100.0% | close |
| m_vol_price_diverge | manual | multi_source | price_volume | reversal | daily_basic,price_volume | high | 0.0083 | 0.0886 | 100.0% | close,db_turnover_rate |
| m_vol_ratio_5d_20d | manual | price_volume | volatility | reversal | price_volume | high | 0.0187 | 0.2426 | 99.8% | close |
| m_volume_contraction | JoinQuant | price_volume | volatility | reversal | price_volume | high | -0.0450 | -0.4283 | 100.0% | volume |
| m_volume_ma_ratio | manual | price_volume | volatility | reversal | price_volume | high | -0.0384 | -0.4136 | 100.0% | volume |
| m_volume_median_accel_10d | manual | price_volume | volatility | reversal | price_volume | high | -0.0303 | -0.3626 | 99.8% | volume |
| m_volume_price_divergence_10d | manual | price_volume | statistical | reversal | price_volume | high | -0.0309 | -0.4872 | 100.0% | close,volume |
| main_net_inflow_market_cap_ratio | rdagent_task_sync | daily_basic | size | momentum | daily_basic | high | -0.0027 | -0.0428 | 100.0% | db_total_mv |
| market_breadth_enhanced_valuation_factor | rdagent_task_sync | multi_source | valuation | value_premium | bak_basic,daily_basic,price_volume | high | 0.0099 | 0.0579 | 79.1% | bb_pe_dyn,close,db_pb |
| market_sentiment_turnover_news | rdagent_task_sync | multi_source | turnover | reversal | daily_basic,price_volume | high | -0.0125 | -0.2048 | 100.0% | close,db_turnover_rate,volume |
| market_state_volatility_regime | rdagent_task_sync | multi_source | volatility | reversal | daily_basic,price_volume | high | -0.0283 | -0.3083 | 100.0% | amount,close,db_turnover_rate,factor,high,low,open,volume |
| mf_close_divergence | rdagent_task_sync | multi_source | moneyflow | reversal | moneyflow,price_volume | high | 0.0043 | 0.0474 | 100.0% | close,mf_net_amt |
| mf_elg_net_amt_ratio_5d | rdagent_task_sync | multi_source | moneyflow | momentum | moneyflow,price_volume | high | -0.0200 | -0.3649 | 100.0% | amount,mf_elg_buy_amt,mf_elg_sell_amt |
| mf_elg_net_amt_ratio_5d_change | rdagent_task_sync | price_volume | moneyflow | momentum | price_volume | high | 0.0040 | 0.2176 | 96.0% | amount,close,factor,high,low,open,volume |
| mf_elg_share_in_main_amt | rdagent_task_sync | multi_source | moneyflow | crowding | moneyflow,price_volume | high | -0.0069 | -0.2470 | 99.9% | amount,close,factor,high,low,mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt,open,volume |
| mf_elg_share_in_main_amt_5d | rdagent_task_sync | multi_source | moneyflow | microstructure | moneyflow,price_volume | high | -0.0061 | -0.2321 | 100.0% | amount,close,factor,high,low,mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt,open,volume |
| mf_hierarchical_flow | rdagent_task_sync | multi_source | moneyflow | momentum | moneyflow,price_volume | high | -0.0069 | -0.1429 | 100.0% | amount,mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt,mf_md_buy_amt,mf_md_sell_amt,mf_sm_buy_amt,mf_sm_sell_amt |
| mf_hierarchical_intensity | rdagent_task_sync | multi_source | statistical | momentum | moneyflow,price_volume | high | -0.0063 | -0.1332 | 100.0% | amount,mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt,mf_md_buy_amt,mf_md_sell_amt,mf_sm_buy_amt,mf_sm_sell_amt |
| mf_institutional_market_breadth | rdagent_task_sync | multi_source | statistical | momentum | moneyflow,price_volume | high | -0.0141 | -0.3041 | 100.0% | amount,close,mf_elg_buy_amt,mf_elg_sell_amt |
| mf_institutional_net_strength | rdagent_task_sync | multi_source | moneyflow | crowding | moneyflow,price_volume | high | -0.0200 | -0.3654 | 100.0% | amount,mf_elg_buy_amt,mf_elg_sell_amt |
| mf_large_net_ratio | rdagent_task_sync | moneyflow | moneyflow | crowding | moneyflow | high | -0.0039 | -0.1238 | 100.0% | mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt,mf_net_amt |
| mf_lg_net_ratio | rdagent_task_sync | multi_source | moneyflow | crowding | moneyflow,price_volume | high | -0.0053 | -0.1341 | 100.0% | amount,mf_lg_buy_amt,mf_lg_sell_amt |
| mf_rolling_intensity_normalized | rdagent_task_sync | multi_source | moneyflow | momentum | moneyflow,daily_basic | high | 0.0093 | 0.0932 | 100.0% | db_circ_mv,mf_net_amt |
| micro_flow_order_diff | rdagent_task_sync | multi_source | moneyflow | microstructure | moneyflow,daily_basic | high | -0.0020 | -0.0320 | 100.0% | db_circ_mv,mf_lg_buy_amt,mf_lg_sell_amt,mf_sm_buy_amt,mf_sm_sell_amt |
| micro_reversal_order_impact | rdagent_task_sync | multi_source | size | reversal | moneyflow,daily_basic | high | -0.0016 | -0.0506 | 96.0% | db_circ_mv,mf_elg_buy_amt,mf_elg_sell_amt |
| microstructure_valuation_composite_factor | rdagent_task_sync | multi_source | valuation | microstructure | moneyflow,bak_basic | high | -0.0081 | -0.2217 | 79.0% | bb_pe_dyn,mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt |
| ml_enhanced_flow_valuation_composite | rdagent_task_sync | multi_source | valuation | momentum | moneyflow,daily_basic,price_volume | high | 0.0159 | 0.1271 | 99.8% | amount,close,db_pb,factor,high,low,mf_elg_buy_amt,mf_elg_sell_amt,open,volume |
| ml_nonlinear_fusion_input | rdagent_task_sync | multi_source | machine_learning | momentum | cyq_perf,moneyflow,bak_basic,daily_basic,price_volume | high | -0.0286 | -0.2709 | 79.1% | amount,bb_pe_dyn,close,cp_winner_rate,db_pb,db_turnover_rate,db_turnover_rate_f,high,low,mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt,volume |
| momentum_enhanced_acceleration_entropy | rdagent_task_sync | multi_source | momentum | momentum | cyq_perf,moneyflow,price_volume | high | 0.0049 | 0.0559 | 100.0% | close,cp_cost_15pct,cp_cost_50pct,cp_cost_5pct,cp_cost_85pct,cp_cost_95pct,mf_net_amt |
| momentum_volume_ratio | rdagent_task_sync | multi_source | momentum | reversal | daily_basic,price_volume | high | -0.0471 | -0.3564 | 92.5% | close,db_volume_ratio |
| multi_level_flow_game_enhanced | rdagent_task_sync | moneyflow | moneyflow | microstructure | moneyflow | high | -0.0089 | -0.0752 | 100.0% | mf_elg_buy_amt,mf_md_buy_vol,mf_sm_sell_amt |
| neg_Composite_Factor_Multi_Dim | manual | unknown | volatility | momentum | unknown | medium | 0.0459 | 0.4426 | 75.2% | - |
| neg_Elite_Sell_Size_Adjusted | manual | multi_source | size | reversal | moneyflow,daily_basic | high | 0.0368 | 0.2965 | 100.0% | db_total_mv,mf_elg_sell_amt |
| neg_HighLowMomentum | manual | price_volume | volatility | reversal | price_volume | high | 0.0275 | 0.3198 | 99.9% | high,low |
| neg_Market_Cap_Adjusted_Momentum | manual | multi_source | size | momentum | daily_basic,price_volume | high | 0.0416 | 0.3165 | 100.0% | close,db_circ_mv |
| neg_Momentum_5d | manual | price_volume | turnover | liquidity_premium | price_volume | high | 0.0401 | 0.3004 | 100.0% | close |
| neg_PBTurnoverInteractionStd | manual | daily_basic | volatility | reversal | daily_basic | high | 0.0393 | 0.2341 | 99.8% | db_pb,db_turnover_rate |
| neg_PrecomputedLiquidityTurnoverStd | manual | daily_basic | turnover | liquidity_premium | daily_basic | medium | 0.0453 | 0.2806 | 100.0% | - |
| neg_PriceMomentum20D | manual | price_volume | momentum | momentum | price_volume | high | 0.0448 | 0.3235 | 100.0% | close |
| neg_PriceStrength_10D | manual | price_volume | momentum | reversal | price_volume | high | 0.0211 | 0.1703 | 100.0% | close,high,low |
| neg_PriceVolatility_5D | manual | price_volume | volatility | reversal | price_volume | high | 0.0410 | 0.2774 | 100.0% | close |
| neg_Price_Momentum_10D | manual | price_volume | momentum | reversal | price_volume | high | 0.0423 | 0.3206 | 100.0% | close |
| neg_Smart_Money_Trend_Factor | manual | price_volume | statistical | momentum | price_volume | high | 0.0355 | 0.3533 | 100.0% | amount,close |
| neg_TurnoverVolatilityEnhancement | manual | multi_source | turnover | reversal | daily_basic,price_volume | high | 0.0457 | 0.2814 | 100.0% | close,db_turnover_rate,high,low |
| neg_Value_Liquidity_Adjustment | manual | daily_basic | turnover | value_premium | daily_basic | medium | 0.0338 | 0.3129 | 99.8% | - |
| neg_VolAdjMomentum_10D | manual | price_volume | volatility | reversal | price_volume | high | 0.0314 | 0.2647 | 100.0% | close |
| neg_Volatility_Adjusted_Momentum_20D | manual | price_volume | volatility | momentum | price_volume | high | 0.0355 | 0.2885 | 100.0% | amount,close,factor,high,low,open,volume |
| neg_VolumeWeightedMomentum | manual | price_volume | momentum | reversal | price_volume | high | 0.0287 | 0.3550 | 100.0% | close,volume |
| neg_Volume_Weighted_Price_Momentum | manual | price_volume | momentum | momentum | price_volume | high | 0.0307 | 0.2549 | 100.0% | close,volume |
| neg_adaptive_volatility_momentum_20d | manual | price_volume | volatility | momentum | price_volume | high | 0.0280 | 0.2391 | 100.0% | close |
| neg_composite_score | manual | multi_source | moneyflow | momentum | daily_basic,price_volume | high | 0.0507 | 0.3352 | 100.0% | close,db_turnover_rate |
| neg_dynamic_pe_momentum_factor | manual | bak_basic | valuation | momentum | bak_basic | high | 0.0344 | 0.2883 | 79.1% | bb_pe_dyn |
| neg_funds_flow_efficiency_ratio | manual | moneyflow | moneyflow | momentum | moneyflow | high | 0.0206 | 0.4029 | 74.1% | mf_elg_buy_amt,mf_elg_buy_vol,mf_sm_buy_amt,mf_sm_buy_vol |
| neg_gross_margin_times_turnover | manual | multi_source | quality | quality | bak_basic,daily_basic | high | 0.0366 | 0.2639 | 100.0% | bb_gpr,db_turnover_rate |
| neg_high_amount_turnover_momentum_5d | manual | multi_source | turnover | momentum | daily_basic,price_volume | high | 0.0355 | 0.4623 | 47.6% | amount,close,db_turnover_rate,factor,high,low,open,volume |
| neg_mf_main_net_amt_std_5d | manual | price_volume | moneyflow | momentum | price_volume | high | 0.0415 | 0.3307 | 100.0% | amount,close,factor,high,low,open,volume |
| neg_momentum_5d_volume_weighted | manual | price_volume | momentum | reversal | price_volume | high | 0.0308 | 0.2547 | 100.0% | close,volume |
| neg_momentum_price_volume | manual | price_volume | momentum | momentum | price_volume | high | 0.0444 | 0.4987 | 100.0% | close,volume |
| neg_momentum_volatility_ratio | manual | price_volume | volatility | momentum | price_volume | high | 0.0358 | 0.2950 | 100.0% | close |
| neg_momentum_volume_weighted | manual | price_volume | momentum | momentum | price_volume | high | 0.0390 | 0.3220 | 100.0% | close,volume |
| neg_price_volume_momentum | manual | price_volume | momentum | momentum | price_volume | high | 0.0411 | 0.3138 | 100.0% | close,volume |
| neg_size_adjusted_turnover | manual | daily_basic | turnover | liquidity_premium | daily_basic | medium | 0.0467 | 0.2877 | 100.0% | - |
| neg_size_adjusted_turnover_5d | manual | multi_source | turnover | reversal | daily_basic,price_volume | high | 0.0395 | 0.2378 | 100.0% | amount,close,db_circ_mv,db_turnover_rate,factor,high,low,open,volume |
| neg_turnover_adjusted_volatility | manual | multi_source | turnover | reversal | daily_basic,price_volume | high | 0.0457 | 0.2618 | 100.0% | close,db_turnover_rate |
| neg_valuation_liquidity_interaction | manual | daily_basic | valuation | value_premium | daily_basic | high | 0.0265 | 0.1542 | 78.6% | db_pe,db_turnover_rate |
| neg_vol_adjusted_momentum | manual | price_volume | momentum | momentum | price_volume | high | 0.0406 | 0.3246 | 99.3% | close |
| neg_vol_adjusted_nonlinear_momentum | manual | price_volume | volatility | momentum | price_volume | high | 0.0312 | 0.2737 | 100.0% | close,volume |
| neg_volatility_10D | manual | price_volume | volatility | reversal | price_volume | high | 0.0430 | 0.2547 | 100.0% | close |
| neg_volatility_20d | manual | price_volume | volatility | reversal | price_volume | high | 0.0378 | 0.2053 | 100.0% | close |
| neg_volatility_breakout_momentum_v2 | manual | price_volume | momentum | momentum | price_volume | high | 0.0493 | 0.3653 | 100.0% | close,volume |
| neg_volume_momentum_5d | manual | price_volume | momentum | momentum | price_volume | high | 0.0323 | 0.3837 | 100.0% | volume |
| neg_volume_ratio_5d | manual | price_volume | turnover | liquidity_premium | price_volume | high | 0.0211 | 0.2721 | 100.0% | volume |
| order_flow_imbalance_volatility_adjusted | rdagent_task_sync | multi_source | volatility | microstructure | moneyflow,price_volume | high | -0.0078 | -0.2796 | 83.8% | close,mf_elg_buy_vol,mf_elg_sell_vol |
| order_imbalance_microstructure_factor | rdagent_task_sync | multi_source | moneyflow | microstructure | moneyflow,daily_basic | high | -0.0067 | -0.1593 | 99.9% | db_circ_mv,mf_lg_buy_vol,mf_lg_sell_vol |
| oscillating_market_stability_enhancer | rdagent_task_sync | multi_source | moneyflow | momentum | cyq_perf,moneyflow,daily_basic,price_volume | high | 0.0070 | 0.1014 | 100.0% | close,cp_cost_15pct,cp_cost_85pct,db_turnover_rate,high,low,mf_net_amt |
| pb_elg_net_momentum | rdagent_task_sync | multi_source | valuation | momentum | moneyflow,daily_basic,price_volume | high | -0.0075 | -0.2151 | 99.8% | amount,db_pb,mf_elg_buy_amt,mf_elg_sell_amt |
| pe_dyn_to_cost_ratio | rdagent_task_sync | multi_source | valuation | value_premium | cyq_perf,bak_basic | high | 0.0016 | 0.0303 | 100.0% | bb_pe_dyn,cp_cost_50pct |
| pe_moneyflow_product | rdagent_task_sync | multi_source | moneyflow | momentum | moneyflow,daily_basic | high | 0.0018 | 0.0177 | 78.6% | db_pe,db_total_mv,mf_net_amt |
| pe_ttm_cost_divergence | rdagent_task_sync | multi_source | valuation | value_premium | cyq_perf,daily_basic | high | 0.0012 | 0.0119 | 60.6% | cp_weight_avg,db_pe_ttm |
| peg_ratio | rdagent_task_sync | multi_source | growth | value_premium | bak_basic,daily_basic | high | -0.0064 | -0.0997 | 42.2% | bb_profit_yoy,db_pe |
| price_convexity_elg_flow_synergy | rdagent_task_sync | multi_source | moneyflow | momentum | moneyflow,price_volume | high | -0.0042 | -0.0908 | 99.9% | close,mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt |
| price_flow_divergence | rdagent_task_sync | multi_source | momentum | reversal | moneyflow,daily_basic,price_volume | high | -0.0409 | -0.3088 | 100.0% | close,db_total_mv,mf_net_amt |
| price_momentum_flow_10d | rdagent_task_sync | price_volume | moneyflow | momentum | price_volume | high | 0.0072 | 0.1478 | 100.0% | amount,close,factor,high,low,open,volume |
| price_momentum_flow_adj_10d | rdagent_task_sync | price_volume | volatility | momentum | price_volume | high | 0.0038 | 0.0926 | 100.0% | amount,close,factor,high,low,open,volume |
| price_strength_10d | rdagent_task_sync | price_volume | momentum | reversal | price_volume | medium | -0.0423 | -0.3206 | 100.0% | - |
| price_strength_main_flow_5d | rdagent_task_sync | multi_source | moneyflow | momentum | moneyflow,price_volume | high | -0.0131 | -0.2803 | 100.0% | amount,close,factor,high,low,mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt,open,volume |
| price_strength_volume_adjusted | rdagent_task_sync | price_volume | turnover | momentum | price_volume | high | -0.0430 | -0.3431 | 100.0% | volume |
| price_to_historical_low_ratio | rdagent_task_sync | multi_source | valuation | reversal | cyq_perf,price_volume | high | -0.0103 | -0.1086 | 100.0% | close,cp_his_low |
| price_volume_corr | rdagent_task_sync | price_volume | correlation | reversal | price_volume | high | -0.0244 | -0.2743 | 100.0% | close,volume |
| price_volume_correlation_5d | rdagent_task_sync | price_volume | correlation | reversal | price_volume | high | -0.0247 | -0.3421 | 100.0% | close,volume |
| price_volume_divergence | rdagent_task_sync | price_volume | price_volume | reversal | price_volume | high | 0.0134 | 0.1968 | 100.0% | close,volume |
| price_volume_divergence_signal | rdagent_task_sync | price_volume | momentum | reversal | price_volume | high | -0.0107 | -0.2192 | 100.0% | close,volume |
| price_volume_synergy_momentum | rdagent_task_sync | price_volume | turnover | reversal | price_volume | high | -0.0315 | -0.2834 | 100.0% | amount,close,volume |
| profit_efficiency | rdagent_task_sync | bak_basic | quality | quality | bak_basic | high | -0.0033 | -0.0340 | 100.0% | bb_gpr,bb_npr |
| profit_stability_score | rdagent_task_sync | bak_basic | quality | quality | bak_basic | high | -0.0018 | -0.0182 | 100.0% | bb_eps,bb_gpr,bb_profit_yoy,bb_rev_yoy |
| profitability_liquidity_adjusted | rdagent_task_sync | multi_source | turnover | quality | bak_basic,daily_basic | high | -0.0081 | -0.0696 | 99.9% | bb_bvps,bb_eps,db_turnover_rate_f |
| quality_capex_efficiency | rdagent_task_sync | multi_source | quality | quality | bak_basic,daily_basic | high | 0.0034 | 0.0830 | 100.0% | bb_fixed_assets,bb_rev_yoy,bb_undp,db_total_mv |
| quality_structure_composite | rdagent_task_sync | multi_source | quality | quality | cyq_perf,bak_basic,price_volume | high | -0.0133 | -0.1355 | 15.7% | bb_bvps,bb_eps,bb_gpr,bb_npr,bb_profit_yoy,bb_rev_yoy,close,cp_cost_50pct,cp_cost_5pct,cp_cost_95pct |
| retail_sentiment | rdagent_task_sync | moneyflow | moneyflow | crowding | moneyflow | high | 0.0052 | 0.1001 | 100.0% | mf_sm_buy_amt,mf_sm_sell_amt |
| revenue_growth_elg_flow_momentum_product | rdagent_task_sync | multi_source | moneyflow | momentum | moneyflow,bak_basic,price_volume | high | -0.0023 | -0.0852 | 100.0% | bb_rev_yoy,mf_elg_buy_vol,mf_elg_sell_vol,volume |
| roe_change_momentum | rdagent_task_sync | multi_source | quality | momentum | bak_basic,daily_basic | high | -0.0160 | -0.3092 | 99.9% | bb_npr,db_turnover_rate |
| roe_stability_score | rdagent_task_sync | multi_source | quality | quality | bak_basic,daily_basic | high | 0.0113 | 0.1056 | 99.9% | bb_npr,db_turnover_rate |
| sentiment_adjusted_flow_residual | rdagent_task_sync | multi_source | moneyflow | microstructure | moneyflow,daily_basic,price_volume | high | 0.0046 | 0.1001 | 99.7% | amount,close,db_pb,db_turnover_rate,factor,high,low,mf_elg_buy_amt,mf_elg_sell_amt,open,volume |
| sentiment_momentum_enhancement | rdagent_task_sync | multi_source | momentum | crowding | daily_basic,price_volume | high | -0.0306 | -0.2742 | 99.9% | amount,close,db_turnover_rate,factor,high,low,open,volume |
| sentiment_order_imbalance | rdagent_task_sync | multi_source | moneyflow | microstructure | moneyflow,daily_basic | high | 0.0010 | 0.0698 | 100.0% | db_circ_mv,mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt,mf_md_buy_amt,mf_md_sell_amt,mf_sm_buy_amt,mf_sm_sell_amt |
| sentiment_winner_rate | rdagent_task_sync | cyq_perf | chip_cost | crowding | cyq_perf | high | -0.0221 | -0.2007 | 100.0% | cp_winner_rate |
| short_term_reversal_5d | rdagent_task_sync | price_volume | volatility | reversal | price_volume | high | 0.0282 | 0.2285 | 100.0% | close |
| size_adjusted_turnover_momentum_5d | rdagent_task_sync | multi_source | turnover | liquidity_premium | daily_basic,price_volume | high | -0.0277 | -0.3068 | 100.0% | amount,close,db_circ_mv,db_turnover_rate,factor,high,low,open,volume |
| size_elg_flow_synergy | rdagent_task_sync | multi_source | correlation | momentum | moneyflow,price_volume | high | -0.0180 | -0.3165 | 99.9% | amount,close,factor,high,low,mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt,open,volume |
| small_order_flow_industry_intensity | rdagent_task_sync | multi_source | moneyflow | momentum | moneyflow,sector_data | high | 0.0027 | 0.0777 | 100.0% | mf_sm_buy_amt,mf_sm_sell_amt,sw2_mf_net_amt |
| small_order_flow_intensity | rdagent_task_sync | multi_source | moneyflow | momentum | moneyflow,price_volume | high | 0.0310 | 0.2971 | 100.0% | amount,mf_sm_buy_amt |
| tech_vol_adjusted_momentum | rdagent_task_sync | price_volume | momentum | momentum | price_volume | high | -0.0334 | -0.3092 | 99.4% | close |
| turnover_adjusted_momentum_10d | rdagent_task_sync | multi_source | turnover | reversal | daily_basic,price_volume | high | -0.0305 | -0.2656 | 100.0% | close,db_turnover_rate |
| turnover_adjusted_volatility_20D | rdagent_task_sync | multi_source | turnover | liquidity_premium | daily_basic,price_volume | high | 0.0044 | 0.0307 | 100.0% | amount,close,db_turnover_rate,factor,high,low,open,volume |
| turnover_anomaly | rdagent_task_sync | daily_basic | turnover | liquidity_premium | daily_basic | high | -0.0359 | -0.3898 | 100.0% | db_turnover_rate |
| turnover_anomaly_sentiment | rdagent_task_sync | daily_basic | turnover | reversal | daily_basic | high | -0.0353 | -0.3829 | 100.0% | db_turnover_rate |
| turnover_elg_risk_adjusted | rdagent_task_sync | multi_source | moneyflow | reversal | moneyflow,daily_basic | high | -0.0197 | -0.3179 | 99.9% | db_turnover_rate,mf_elg_buy_amt,mf_elg_sell_amt,mf_lg_buy_amt,mf_lg_sell_amt |
| turnover_volatility_adjusted_valuation | rdagent_task_sync | multi_source | valuation | value_premium | bak_basic,daily_basic | high | 0.0218 | 0.2221 | 78.9% | bb_pe_dyn,db_turnover_rate |
| undistributed_profit_adjusted_turnover | rdagent_task_sync | multi_source | turnover | liquidity_premium | bak_basic,daily_basic | high | -0.0177 | -0.1982 | 100.0% | bb_per_undp,db_turnover_rate |
| undistributed_profit_market_cap_ratio | rdagent_task_sync | multi_source | size | value_premium | bak_basic,daily_basic | high | 0.0036 | 0.0272 | 100.0% | bb_undp,db_total_mv |
| valuation_chip_conditional | rdagent_task_sync | multi_source | valuation | value_premium | cyq_perf,daily_basic | high | 0.0005 | 0.0045 | 100.0% | cp_cost_15pct,cp_cost_85pct,db_pe_ttm |
| valuation_reversal_adjusted | rdagent_task_sync | multi_source | valuation | reversal | daily_basic,price_volume | high | -0.0202 | -0.1898 | 75.2% | close,db_pe_ttm,high,low |
| value_divergence_reversal | rdagent_task_sync | multi_source | valuation | reversal | daily_basic,price_volume | high | 0.0196 | 0.2595 | 99.8% | amount,close,db_pb,factor,high,low,open,volume |
| value_flow_risk_adjusted | rdagent_task_sync | multi_source | moneyflow | momentum | cyq_perf,moneyflow,daily_basic,price_volume | high | -0.0030 | -0.0886 | 99.8% | amount,cp_winner_rate,db_pb,mf_elg_buy_amt,mf_elg_sell_amt |
| value_momentum_reversal | rdagent_task_sync | daily_basic | valuation | value_premium | daily_basic | high | 0.0188 | 0.2011 | 74.6% | db_pe_ttm |
| value_pe_inv_momentum | rdagent_task_sync | daily_basic | valuation | value_premium | daily_basic | high | 0.0318 | 0.2602 | 74.6% | db_pe_ttm |
| value_pe_pb_combined | rdagent_task_sync | daily_basic | valuation | value_premium | daily_basic | high | 0.0102 | 0.0601 | 80.2% | db_pb,db_pe,db_pe_ttm |
| value_reversal_bb_pe_dyn | rdagent_task_sync | bak_basic | valuation | reversal | bak_basic | high | 0.0043 | 0.0244 | 80.0% | bb_pe_dyn |
| value_turnover_interaction | rdagent_task_sync | multi_source | valuation | value_premium | bak_basic,daily_basic | high | -0.0281 | -0.3871 | 79.1% | bb_pe_dyn,db_turnover_rate |
| vol_adj_momentum | rdagent_task_sync | price_volume | volatility | momentum | price_volume | high | -0.0254 | -0.3265 | 100.0% | close,volume |
| volatility_adaptive_flow_momentum | rdagent_task_sync | multi_source | volatility | momentum | moneyflow,price_volume | high | 0.0004 | 0.0158 | 100.0% | amount,close,factor,high,low,mf_elg_buy_amt,mf_elg_sell_amt,open,volume |
| volatility_adjusted_cost_concentration | rdagent_task_sync | multi_source | chip_cost | microstructure | cyq_perf,daily_basic | high | 0.0173 | 0.1253 | 100.0% | cp_cost_15pct,cp_cost_85pct,cp_weight_avg,db_turnover_rate |
| volatility_adjusted_industry_momentum | rdagent_task_sync | multi_source | industry_relative | reversal | sector_data,price_volume | high | -0.0351 | -0.4437 | 98.7% | close,sw2_close |
| volatility_adjusted_momentum_fundamental | rdagent_task_sync | multi_source | volatility | momentum | bak_basic,price_volume | high | -0.0179 | -0.1787 | 100.0% | bb_gpr,close,volume |
| volatility_adjusted_value_reversal | rdagent_task_sync | bak_basic | valuation | value_premium | bak_basic | high | 0.0189 | 0.1254 | 59.5% | bb_pe_dyn |
| volatility_breakout_momentum | rdagent_task_sync | price_volume | momentum | momentum | price_volume | high | 0.0058 | 0.0390 | 100.0% | close,volume |
| volatility_filtered_flow_residual | rdagent_task_sync | multi_source | moneyflow | reversal | moneyflow,daily_basic,price_volume | high | -0.0074 | -0.2595 | 98.5% | amount,close,db_pb,mf_elg_buy_amt,mf_elg_sell_amt |
| volatility_inverse_weighted_momentum_reversal | rdagent_task_sync | price_volume | volatility | momentum | price_volume | high | -0.0102 | -0.1018 | 100.0% | amount,close,factor,high,low,open,volume |
| volatility_regime_flow_residual | rdagent_task_sync | multi_source | volatility | reversal | moneyflow,price_volume | high | -0.0004 | -0.0147 | 100.0% | amount,close,factor,high,low,mf_elg_buy_amt,mf_elg_sell_amt,open,volume |
| volume_price_momentum | rdagent_task_sync | price_volume | momentum | momentum | price_volume | high | -0.0125 | -0.1184 | 100.0% | close,volume |
| volume_ratio | rdagent_task_sync | daily_basic | turnover | reversal | daily_basic | high | -0.0254 | -0.3142 | 92.5% | db_volume_ratio |
| winner_adjusted_flow_ratio | rdagent_task_sync | multi_source | moneyflow | momentum | cyq_perf,moneyflow,daily_basic,price_volume | high | 0.0051 | 0.1717 | 99.8% | amount,cp_winner_rate,db_pb,mf_elg_buy_amt,mf_elg_sell_amt |
| winner_cost_interaction | rdagent_task_sync | multi_source | chip_cost | reversal | cyq_perf,price_volume | high | -0.0236 | -0.2243 | 100.0% | close,cp_cost_5pct,cp_cost_95pct,cp_winner_rate |

## 9. 门控与后续动作

- 本轮只写 `docs/analysis/factor_library_datasource_coverage_audit_20260623.md` 与 BUG/Issue 登记文件。
- 未调用任何 factor_library 写接口；未执行 DB DDL/DML；未启动/重启服务。
- 回填执行建议拆成独立任务：先人工抽样审核 `low/medium confidence`，再按 `factor_name -> data_source/factor_type` 分批写入，并记录可回滚快照。
- 新因子开发建议先走 `financial_event_raw/dividend/shareholder/industry_relative`，每个方向小批量 3-5 个机制因子，先验证与现有 6 腿的相关性和边际 Sharpe。

_Generated at 2026-06-22T17:10:41.238933+00:00 from read-only DB queries. Passwords and secrets were not printed._
