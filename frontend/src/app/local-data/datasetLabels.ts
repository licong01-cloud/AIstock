/** Display names only. Source keys, API parameters and completeness rules stay unchanged. */
const names: Record<string, string> = {
  kline_daily_raw: "日线行情（未复权）",
  kline_daily_raw_go: "日线行情（Go直连）",
  kline_minute_raw: "分钟行情（未复权）",
  kline_daily: "日线行情",
  adj_factor: "复权因子",
  daily_basic: "股票每日指标",
  bak_basic: "备用基础信息",
  stock_basic: "股票基础信息",
  stock_st: "ST股票标记",
  stock_st_events: "ST风险警示事件",
  stock_universe_pit_events: "股票池历史资格事件",
  stock_universe_pit_spans: "股票池历史资格区间",
  stock_universe_pit_state: "股票池历史资格状态",
  stock_moneyflow_ts: "个股资金流",
  moneyflow_ts: "个股资金流",
  cyq_perf: "筹码分布绩效",
  cyq_chips: "每日筹码分布",
  stk_limit: "每日涨跌停价格",
  suspend_d: "每日停复牌信息",
  index_basic: "指数基础信息",
  index_daily: "指数日线行情",
  sw_sector: "申万行业板块同步",
  sw_index_classify: "申万行业分类",
  sw_index_member: "申万行业历史成分股",
  sw_daily: "申万行业指数日线",
  sector_data: "个股申万二级行业特征",
  margin_detail: "融资融券明细",
  etf_share_size: "ETF每日份额及规模",
  etf_basic_snapshots: "ETF基础信息快照",
  dividend: "分红送转",
  anns_d: "上市公司公告",
  anns_metadata: "公告元数据",
  tushare_forecast_raw: "业绩预告原始数据",
  tushare_express_raw: "业绩快报原始数据",
  tushare_fina_indicator_raw: "财务指标原始数据",
  report_rc: "券商研报盈利预测",
  news_realtime: "实时新闻",
  trading_calendar: "交易日历",
  symbol_dim: "证券基础目录",
  xtquant_pershare_index: "每股主要指标",
  sina_board_daily: "新浪板块日线",
  sina_board_intraday: "新浪板块盘中行情",
  stk_shock: "个股异常波动",
  stk_high_shock: "个股严重异常波动",
  stk_alert: "个股重点提示",
  idx_anns: "指数公司公告",
  stk_seasoned: "增发进程",
  vix_index: "VIX美国日频指数",
  stock_daily_afterhours: "股票盘后成交量额",
  _auto_retry_stale: "数据同步自动重试",
  _data_freshness_check: "数据新鲜度检查",
  _weekend_compensation: "周末数据补偿",
  _stk_seasoned_project_review: "增发项目定期复查",
};

export function datasetLabel(value: unknown, fallbackName?: string): string {
  const key = String(value ?? "").trim();
  if (!key || key === "—") return fallbackName || "未指定数据集";
  const lower = key.toLowerCase();
  let name = names[lower];
  const morning = /^_(.+)_morning$/.exec(lower);
  if (!name && morning && names[morning[1]]) name = `${names[morning[1]]}·晨间补采`;
  if (!name && lower === "_vix_index_midday") name = "VIX美国日频指数·午间补采";
  return `${name || (fallbackName && /[\u3400-\u9fff]/.test(fallbackName) ? fallbackName : "未知数据集")}（${key}）`;
}
