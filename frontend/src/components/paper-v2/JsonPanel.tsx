import { formatNumber, formatPercent, shortHash, statusLabel } from "@/lib/paper-v2/format";

const LABELS: Record<string, string> = {
  algo_code: "执行算法",
  annual_return: "年化收益",
  auto_switch_to_live: "回放后切换实时",
  avg_fill_price: "成交均价",
  cash: "现金",
  cash_buffer_pct: "现金保留比例",
  completed_at: "完成时间",
  config_json: "配置内容",
  config_sha256: "配置哈希",
  created_at: "创建时间",
  created_by: "创建人",
  current_trade_date: "当前交易日",
  data_source: "数据源",
  end_date: "结束日期",
  error_code: "错误代码",
  excluded_candidate_count: "剔除股票数",
  filled_quantity: "已成交数量",
  initial_cash: "初始资金",
  live_data_source: "实时数据源",
  manifest_sha256: "策略包 Manifest 哈希",
  market_value: "持仓市值",
  max_drawdown: "最大回撤",
  message: "说明",
  mode: "模式",
  nav: "净值",
  order_id: "订单ID",
  order_intent_count: "订单意图数量",
  package_id: "策略包ID",
  package_name: "策略包名称",
  policy_json: "策略内容",
  portfolio_id: "模拟盘ID",
  profile_version_id: "配置版本ID",
  quantity: "委托数量",
  rank_ic: "Rank IC",
  reason: "原因",
  run_id: "运行ID",
  sharpe: "夏普比率",
  side: "方向",
  start_date: "开始日期",
  status: "状态",
  symbol: "股票代码",
  target_count: "目标持仓数",
  top_k: "选股数量",
  trade_date: "交易日",
  tradable_candidate_count: "可交易候选数",
};

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function labelForKey(key: string): string {
  if (LABELS[key]) return LABELS[key];
  if (key.endsWith("_json")) return labelForKey(key.replace(/_json$/, "")) + "内容";
  return key
    .replace(/_/g, " ")
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function simpleValue(value: unknown, key?: string): string {
  if (value === null || value === undefined || value === "") return "未设置";
  if (typeof value === "boolean") return value ? "是" : "否";
  if (typeof value === "number") {
    if (key && (key.endsWith("_pct") || key.endsWith("_rate"))) return formatPercent(value);
    if (Number.isInteger(value)) return formatNumber(value, 0);
    return formatNumber(value, 4);
  }
  const text = String(value);
  if (key === "status" || key === "side" || key === "mode") return `${statusLabel(text)}（${text}）`;
  if (/_id$/.test(key || "") && text.length > 20) return `${shortHash(text)}（${text}）`;
  if (/sha256|hash/i.test(key || "") && text.length > 20) return `${shortHash(text)}（${text}）`;
  return text;
}

function ReadableValue({ value, fieldKey }: { value: unknown; fieldKey?: string }) {
  if (Array.isArray(value)) {
    if (value.length === 0) return <span className="pv2-muted">无</span>;
    if (value.every((item) => !isPlainObject(item) && !Array.isArray(item))) {
      return (
        <div className="pv2-chip-row">
          {value.map((item, index) => <span className="pv2-chip" key={`${fieldKey || "item"}-${index}`}>{simpleValue(item)}</span>)}
        </div>
      );
    }
    return (
      <div className="pv2-readable-list">
        {value.map((item, index) => (
          <details className="pv2-readable-item" key={`${fieldKey || "item"}-${index}`} open={index === 0}>
            <summary>第 {index + 1} 项</summary>
            <ReadableValue value={item} />
          </details>
        ))}
      </div>
    );
  }

  if (isPlainObject(value)) {
    const entries = Object.entries(value).filter(([, item]) => item !== undefined);
    if (entries.length === 0) return <span className="pv2-muted">无配置</span>;
    return (
      <div className="pv2-readable-table">
        {entries.map(([key, item]) => (
          <div className="pv2-readable-row" key={key}>
            <div className="pv2-readable-key">{labelForKey(key)}</div>
            <div className="pv2-readable-value"><ReadableValue value={item} fieldKey={key} /></div>
          </div>
        ))}
      </div>
    );
  }

  return <span>{simpleValue(value, fieldKey)}</span>;
}

export default function JsonPanel({ value }: { value: unknown }) {
  return (
    <div className="pv2-readable-panel">
      <ReadableValue value={value} />
    </div>
  );
}
