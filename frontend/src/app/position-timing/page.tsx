"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import MetricCard from "@/components/trading-console/MetricCard";
import SectionCard from "@/components/trading-console/SectionCard";

const API_BASE = (process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1").replace(/\/+$/, "");

type Intent = {
  canonical_symbol: string;
  planned_full_notional_cny: string | number;
  desired_target_exposure: string | number;
  updated_at: string;
};

type IntentRow = {
  canonical_symbol: string;
  display_name?: string | null;
  primary_source_role: "HOLDING" | "WATCHLIST";
  source_roles: Array<"HOLDING" | "WATCHLIST">;
  pre_action_qty: number;
  intent?: Intent | null;
  normalization_reason?: string | null;
  analysis_selected: boolean;
  analysis_effective: boolean;
  analysis_locked: boolean;
  analysis_reason_code: string;
};

type IntentList = {
  items: IntentRow[];
  scope_warnings: Array<{ canonical_symbol: string; reason_code: string }>;
};

type Trigger = {
  trigger_id: string;
  branch: string;
  side: "BUY" | "SELL" | "NONE";
  operator: "LTE" | "GTE" | "ALWAYS" | "NEVER";
  trigger_price_raw?: string | number | null;
  guard_action: string;
  planned_delta_qty: number;
  planned_leg_notional_cny: string | number;
  reason_code: string;
};

type CostScenario = {
  scenario: string;
  requested_parent_order_count: number;
  status: string;
  effective_parent_order_count: number;
  total_cost_cny: string | number;
  total_cost_bps: string | number;
};

type TimingCard = {
  card_id: string;
  canonical_symbol: string;
  display_name?: string | null;
  primary_source_role: string;
  source_roles: string[];
  decision_trade_date: string;
  target_trade_date: string;
  valid_until: string;
  pre_action_qty: number;
  t1_sellable_qty: number;
  planned_full_notional_cny?: string | number | null;
  desired_target_exposure?: string | number | null;
  requested_delta_qty: number;
  requested_leg_notional_cny: string | number;
  action: "OPEN" | "ADD" | "HOLD" | "REDUCE" | "EXIT" | "WAIT" | "UNAVAILABLE";
  execution_window: string;
  reference_price_raw?: string | number | null;
  tradability_status: string;
  st_flag?: boolean | null;
  delist_flag?: boolean | null;
  delist_context_status: string;
  reason_codes: string[];
  triggers: Trigger[];
  selection_context_status: string;
  hmm_context_status: string;
  evidence_tier: string;
  cost_estimate?: {
    small_trade_cost_heavy: boolean;
    display_disclosure: string;
    scenarios: CostScenario[];
  } | null;
  trigger_cost_estimates: Record<string, { small_trade_cost_heavy: boolean; scenarios: CostScenario[] }>;
};

type CardSet = {
  card_set_id: string;
  decision_trade_date: string;
  target_trade_date: string;
  cards: TimingCard[];
};

type CurrentCards = { status: string; card_set?: CardSet | null };
type Evidence = {
  product_evidence_tier: string;
  event_counts: Record<string, number>;
  l2_runtime_status: string;
  hmm_runtime_role: string;
  selection_runtime_role: string;
  cost_disclosure: {
    min_commission_scope_verification: string;
    thresholds_cny: Record<string, number>;
  };
  outcome_evidence: {
    status: string;
    coverage_counts?: {
      matured: number;
      pending: number;
      unavailable: number;
      materialization_missing: number;
    };
    paired_matured?: { count: number; mean_net_lift_bps?: string | number | null };
    intervention_intent?: { count: number; mean_net_lift_bps?: string | number | null };
  };
};

type AlertEdge = {
  card_id: string;
  canonical_symbol: string;
  status: string;
  system_edge_eligibility: boolean;
  already_alerted: boolean;
  trigger_id?: string;
  eligibility_identity?: string;
  quote_price_raw?: string | number;
  quote_open_raw?: string | number;
  quote_observed_at?: string;
  alert_evaluated_at?: string;
  quote_source?: string;
  position_snapshot_sha256: string;
  intent_snapshot_sha256: string;
  trigger?: Trigger;
};

type AlertPoll = { status: string; items: AlertEdge[] };

type Draft = { notional: string; exposure: string };

const ACTION_LABEL: Record<TimingCard["action"], string> = {
  OPEN: "新开仓",
  ADD: "加仓",
  HOLD: "持有",
  REDUCE: "减仓",
  EXIT: "退出",
  WAIT: "等待",
  UNAVAILABLE: "暂不可建议",
};

const REASON_LABEL: Record<string, string> = {
  TARGET_DAY_TRADABILITY_RECHECK_REQUIRED: "目标交易日仍需按实时行情复核",
  STOP_LOSS_TRIGGERED: "已触发冻结硬止损规则",
  WATCHLIST_EXPIRED: "已确认终止上市，建议退出",
  CONFIRMED_DELISTING_BUY_UNAVAILABLE: "已确认终止上市，不可新买",
  SIZING_INPUT_UNAVAILABLE: "尚未设置计划满仓金额",
  TARGET_QUANTITY_BELOW_BOARD_MINIMUM: "目标金额不足最小交易单位",
  DELTA_BELOW_BOARD_LOT: "仓位变化不足一个合法交易单位",
  TARGET_EXPOSURE_ALREADY_SATISFIED: "当前数量已满足目标暴露",
  RISK_GUARD_HOLD_CURRENT_POSITION: "冻结风险规则未触发，且未设置再平衡意图",
  SMALL_TRADE_COST_HEAVY: "小额交易成本偏重（仅提示）",
  DECISION_DAY_SUSPENDED_TARGET_DAY_RECHECK: "决策日停牌，目标日必须重新核验",
  DAILY_BAR_UNAVAILABLE: "日线数据不可用",
  DELIST_PIT_UNAVAILABLE: "退市事实晚于决策时点，不可使用",
  UNSUPPORTED_BJ_FIRST_RELEASE: "首发暂不支持北交所标的",
  UNSUPPORTED_SYMBOL: "无法识别或暂不支持的证券代码",
};

function reasonLabel(code: string): string {
  return REASON_LABEL[code] ? `${REASON_LABEL[code]}（${code}）` : code;
}

function money(value: string | number | null | undefined): string {
  if (value === null || value === undefined || value === "") return "-";
  const parsed = Number(value);
  return Number.isFinite(parsed) ? `¥${parsed.toLocaleString("zh-CN", { maximumFractionDigits: 2 })}` : "-";
}

function actionTone(action: TimingCard["action"]): string {
  if (action === "EXIT" || action === "REDUCE") return "#b42318";
  if (action === "OPEN" || action === "ADD") return "#067647";
  if (action === "UNAVAILABLE") return "#b54708";
  return "inherit";
}

function yesNoUnknown(value: boolean | null | undefined): string {
  if (value === true) return "是";
  if (value === false) return "否";
  return "未知";
}

async function requestJson<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}/position-timing${path}`, {
    ...init,
    headers: { "Content-Type": "application/json", ...(init?.headers || {}) },
  });
  const payload = await response.json().catch(() => null);
  if (!response.ok) {
    const detail = payload?.detail || payload;
    throw new Error(detail?.message || detail?.error_code || `HTTP ${response.status}`);
  }
  return payload as T;
}

export default function PositionTimingPage() {
  const [intentRows, setIntentRows] = useState<IntentRow[]>([]);
  const [current, setCurrent] = useState<CurrentCards | null>(null);
  const [evidence, setEvidence] = useState<Evidence | null>(null);
  const [drafts, setDrafts] = useState<Record<string, Draft>>({});
  const [loading, setLoading] = useState(true);
  const [savingSymbol, setSavingSymbol] = useState<string | null>(null);
  const [savingScopeSymbol, setSavingScopeSymbol] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [scopeWarnings, setScopeWarnings] = useState<IntentList["scope_warnings"]>([]);
  const [alertEdges, setAlertEdges] = useState<AlertEdge[]>([]);

  const loadReadModels = useCallback(async () => {
    const [intentPayload, cardsPayload, evidencePayload] = await Promise.all([
      requestJson<IntentList>("/intents"),
      requestJson<CurrentCards>("/cards/current"),
      requestJson<Evidence>("/evidence"),
    ]);
    setIntentRows(intentPayload.items);
    setScopeWarnings(intentPayload.scope_warnings || []);
    setCurrent(cardsPayload);
    setEvidence(evidencePayload);
    setDrafts((previous) => {
      const next = { ...previous };
      for (const row of intentPayload.items) {
        if (!next[row.canonical_symbol]) {
          next[row.canonical_symbol] = {
            notional: row.intent ? String(row.intent.planned_full_notional_cny) : "",
            exposure: row.intent ? String(row.intent.desired_target_exposure) : row.pre_action_qty > 0 ? "1" : "0.25",
          };
        }
      }
      return next;
    });
  }, []);

  const initialize = useCallback(async () => {
    setLoading(true);
    setError(null);
    setNotice(null);
    try {
      try {
        const result = await requestJson<{ status: string }>("/materialize", { method: "POST" });
        setNotice(
          result.status === "MATERIALIZED"
            ? "已生成最新 T+1 日频行动卡。"
            : result.status === "ALREADY_MATERIALIZED"
              ? "已读取本决策日的不可变行动卡。"
              : `未签发新卡：${result.status}。`,
        );
      } catch (materializeError) {
        setNotice(`本次未生成新卡：${materializeError instanceof Error ? materializeError.message : "数据尚未就绪"}`);
      }
      await loadReadModels();
    } catch (loadError) {
      setError(loadError instanceof Error ? loadError.message : "加载失败");
    } finally {
      setLoading(false);
    }
  }, [loadReadModels]);

  useEffect(() => {
    void initialize();
  }, [initialize]);

  const cards = useMemo(() => current?.card_set?.cards || [], [current]);
  const actionCounts = useMemo(() => {
    return cards.reduce<Record<string, number>>((counts, card) => {
      counts[card.action] = (counts[card.action] || 0) + 1;
      return counts;
    }, {});
  }, [cards]);

  const saveIntent = useCallback(
    async (symbol: string) => {
      const draft = drafts[symbol];
      const notional = Number(draft?.notional);
      if (!draft || !Number.isFinite(notional) || notional <= 0) {
        setError(`${symbol} 的计划满仓金额必须大于 0。`);
        return;
      }
      setSavingSymbol(symbol);
      setError(null);
      try {
        await requestJson(`/intents/${encodeURIComponent(symbol)}`, {
          method: "PUT",
          body: JSON.stringify({
            planned_full_notional_cny: draft.notional,
            desired_target_exposure: draft.exposure,
          }),
        });
        setNotice(`${symbol} 意图已保存；已签发卡片不改写，新值进入下一决策日。`);
        const payload = await requestJson<IntentList>("/intents");
        setIntentRows(payload.items);
        setScopeWarnings(payload.scope_warnings || []);
      } catch (saveError) {
        setError(saveError instanceof Error ? saveError.message : "保存失败");
      } finally {
        setSavingSymbol(null);
      }
    },
    [drafts],
  );

  const saveAnalysisScope = useCallback(async (symbol: string, analysisEnabled: boolean) => {
    setSavingScopeSymbol(symbol);
    setError(null);
    try {
      const result = await requestJson<{ status: string; analysis_reason_code: string }>(
        `/analysis-scope/${encodeURIComponent(symbol)}`,
        { method: "PUT", body: JSON.stringify({ analysis_enabled: analysisEnabled }) },
      );
      setNotice(
        result.analysis_reason_code === "HOLDING_ALWAYS_INCLUDED"
          ? `${symbol} 是真实持仓，始终纳入择时分析。`
          : `${symbol} 分析范围已${analysisEnabled ? "启用" : "关闭"}；只影响下一次尚未签发的卡片。`,
      );
      const payload = await requestJson<IntentList>("/intents");
      setIntentRows(payload.items);
      setScopeWarnings(payload.scope_warnings || []);
    } catch (scopeError) {
      setError(scopeError instanceof Error ? scopeError.message : "更新分析范围失败");
    } finally {
      setSavingScopeSymbol(null);
    }
  }, []);

  const pollAlerts = useCallback(async () => {
    if (typeof document !== "undefined" && document.visibilityState !== "visible") return;
    try {
      const payload = await requestJson<AlertPoll>("/alerts/poll");
      setAlertEdges(payload.items || []);
      for (const edge of payload.items || []) {
        if (!edge.system_edge_eligibility || edge.already_alerted || !edge.trigger_id || !edge.eligibility_identity) continue;
        try {
          const claim = await requestJson<{ granted: boolean }>(
            `/alerts/${encodeURIComponent(edge.trigger_id)}/claim`,
            {
              method: "POST",
              body: JSON.stringify({
                card_id: edge.card_id,
                eligibility_identity: edge.eligibility_identity,
                quote_price_raw: edge.quote_price_raw,
                quote_open_raw: edge.quote_open_raw,
                quote_observed_at: edge.quote_observed_at,
                alert_evaluated_at: edge.alert_evaluated_at,
                quote_source: edge.quote_source,
                position_snapshot_sha256: edge.position_snapshot_sha256,
                intent_snapshot_sha256: edge.intent_snapshot_sha256,
              }),
            },
          );
          if (claim.granted) {
            setAlertEdges((currentEdges) => currentEdges.map((item) => (
              item.card_id === edge.card_id && item.trigger_id === edge.trigger_id
                ? { ...item, already_alerted: true }
                : item
            )));
            setNotice(
              `${edge.canonical_symbol} 已到达冻结${edge.trigger?.side === "SELL" ? "卖出" : "买入"}条件：${edge.trigger?.branch || edge.trigger_id}。请人工核对后决策。`,
            );
          }
        } catch (claimError) {
          const message = claimError instanceof Error ? claimError.message : "CLAIM_UNAVAILABLE";
          setAlertEdges((currentEdges) => currentEdges.map((item) => (
            item.card_id === edge.card_id && item.trigger_id === edge.trigger_id
              ? { ...item, status: `CLAIM_UNAVAILABLE: ${message}`, system_edge_eligibility: false }
              : item
          )));
        }
      }
    } catch (pollError) {
      setAlertEdges((currentEdges) => currentEdges.length ? currentEdges : [{
        card_id: "poll-unavailable",
        canonical_symbol: "-",
        status: pollError instanceof Error ? `QUOTE_UNAVAILABLE: ${pollError.message}` : "QUOTE_UNAVAILABLE",
        system_edge_eligibility: false,
        already_alerted: false,
        position_snapshot_sha256: "",
        intent_snapshot_sha256: "",
      }]);
    }
  }, []);

  const hasValidTriggers = current?.status === "VALID_TODAY" && cards.some((card) => card.triggers.length > 0);

  useEffect(() => {
    if (!hasValidTriggers) {
      setAlertEdges([]);
      return undefined;
    }
    void pollAlerts();
    const timer = window.setInterval(() => void pollAlerts(), 60_000);
    const onVisibility = () => {
      if (document.visibilityState === "visible") void pollAlerts();
    };
    document.addEventListener("visibilitychange", onVisibility);
    return () => {
      window.clearInterval(timer);
      document.removeEventListener("visibilitychange", onVisibility);
    };
  }, [hasValidTriggers, pollAlerts]);

  return (
    <main className="pv2-shell">
      <section className="pv2-hero">
        <div className="pv2-hero-top">
          <div>
            <div className="pv2-kicker">Position Timing Advice</div>
            <h1>持仓与自选择时建议</h1>
            <p>
              面向人工交易的 T+1 日频行动卡。系统只给出买入、卖出、持有或等待建议，
              <strong>不生成订单、不自动交易</strong>；页面可见时每分钟复核一次冻结到价条件。
            </p>
          </div>
          <button className="pv2-button-primary" type="button" onClick={() => void initialize()} disabled={loading}>
            {loading ? "读取中" : "刷新并物化"}
          </button>
        </div>
      </section>

      {error ? (
        <div className="pv2-card" role="alert" style={{ borderColor: "#f04438", marginBottom: 16 }}>
          <strong>读取失败：</strong> {error}
        </div>
      ) : null}
      {notice ? (
        <div className="pv2-card" aria-live="polite" style={{ marginBottom: 16 }}>
          {notice}
        </div>
      ) : null}

      <div className="pv2-grid pv2-grid-4">
        <MetricCard label="卡片状态" value={current?.status || (loading ? "LOADING" : "-")} hint={current?.card_set?.card_set_id || "尚无卡片"} />
        <MetricCard label="目标交易日" value={current?.card_set?.target_trade_date || "-"} hint={`决策日 ${current?.card_set?.decision_trade_date || "-"}`} />
        <MetricCard label="买入方向" value={(actionCounts.OPEN || 0) + (actionCounts.ADD || 0)} hint="OPEN + ADD" tone="success" />
        <MetricCard label="卖出方向" value={(actionCounts.REDUCE || 0) + (actionCounts.EXIT || 0)} hint="REDUCE + EXIT" tone="warning" />
      </div>

      <SectionCard title="盘中到价提醒" eyebrow="one-minute read-only quote polling">
        <p>仅在页面可见且卡片于今日有效时轮询；报价陈旧、未来偏斜或持仓/意图变化都会显式停止弹窗。</p>
        {alertEdges.length === 0 ? (
          <p>{hasValidTriggers ? "尚无可展示的触发边。" : "当前没有今日有效的冻结触发条件。"}</p>
        ) : (
          <div className="pv2-grid pv2-grid-2" data-testid="timing-alert-list">
            {alertEdges.map((edge) => (
              <div className="pv2-card" key={`${edge.card_id}-${edge.trigger_id || edge.status}`}>
                <strong>{edge.canonical_symbol}</strong> · <span className="pv2-mono">{edge.status}</span>
                {edge.trigger ? <p>{edge.trigger.branch}；现价 {money(edge.quote_price_raw)}</p> : null}
                {edge.already_alerted ? <p>提醒发送权已授予；此处保留非模态可见记录，不重复弹窗。</p> : null}
              </div>
            ))}
          </div>
        )}
      </SectionCard>

      <SectionCard title="当前 / 最近行动卡" eyebrow="immutable T+1 advice">
        {cards.length === 0 ? (
          <p>{loading ? "正在读取…" : "当前没有行动卡。请确认持仓、自选池和日频数据已就绪。"}</p>
        ) : (
          <div className="pv2-grid pv2-grid-2">
            {cards.map((card) => {
              const baseCost = card.cost_estimate?.scenarios.find((scenario) => scenario.scenario === "ONE_PARENT_ORDER_BASE");
              return (
                <article key={card.card_id} className="pv2-card" data-testid={`timing-card-${card.canonical_symbol}`}>
                  <div className="pv2-card-head">
                    <div>
                      <div className="pv2-eyebrow">{card.primary_source_role} · {card.canonical_symbol}</div>
                      <h2>{card.display_name || card.canonical_symbol}</h2>
                    </div>
                    <strong style={{ color: actionTone(card.action), fontSize: 22 }}>{ACTION_LABEL[card.action]}</strong>
                  </div>
                  <div className="pv2-grid pv2-grid-3">
                    <MetricCard label="当前 / 可卖" value={`${card.pre_action_qty} / ${card.t1_sellable_qty} 股`} hint={card.execution_window} />
                    <MetricCard label="计划变化" value={`${card.requested_delta_qty > 0 ? "+" : ""}${card.requested_delta_qty} 股`} hint={money(card.requested_leg_notional_cny)} />
                    <MetricCard label="参考价" value={money(card.reference_price_raw)} hint={card.tradability_status} />
                  </div>
                  {card.triggers.length ? (
                    <div className="pv2-table-wrap" style={{ marginTop: 12 }}>
                      <table className="pv2-table">
                        <thead><tr><th>冻结分支</th><th>执行条件</th><th>数量</th><th>费用</th><th>结果</th></tr></thead>
                        <tbody>
                          {card.triggers.map((trigger) => (
                            <tr key={trigger.trigger_id}>
                              <td>{trigger.branch}</td>
                              <td>
                                共享 guard = {trigger.guard_action}
                                {trigger.operator === "NEVER" ? "" : `；现价 ${trigger.operator} ${money(trigger.trigger_price_raw)}`}
                              </td>
                              <td>{trigger.planned_delta_qty > 0 ? "+" : ""}{trigger.planned_delta_qty}</td>
                              <td>
                                {money(card.trigger_cost_estimates?.[trigger.trigger_id]?.scenarios?.[0]?.total_cost_cny)}
                                {card.trigger_cost_estimates?.[trigger.trigger_id]?.small_trade_cost_heavy ? "（成本偏重）" : ""}
                              </td>
                              <td>{reasonLabel(trigger.reason_code)}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : null}
                  <p style={{ marginTop: 12 }}><strong>原因：</strong> {card.reason_codes.map(reasonLabel).join(" / ") || "无需交易"}</p>
                  <p>
                    <strong>费用：</strong> {baseCost?.status === "AVAILABLE" ? `${money(baseCost.total_cost_cny)}（${Number(baseCost.total_cost_bps).toFixed(3)} bps）` : "无交易腿"}
                    {card.cost_estimate ? `；${card.cost_estimate.display_disclosure}` : ""}
                    {card.cost_estimate?.small_trade_cost_heavy ? "；小额交易成本偏重（仅提示，不拦截）" : ""}
                  </p>
                  {card.cost_estimate ? (
                    <p>
                      <strong>主建议腿拆单敏感性：</strong>{" "}
                      {card.cost_estimate.scenarios.map((scenario) => (
                        <span key={scenario.scenario} style={{ marginRight: 12 }}>
                          {scenario.requested_parent_order_count} 单：
                          {scenario.status === "AVAILABLE" ? money(scenario.total_cost_cny) : "数量不支持"}
                        </span>
                      ))}
                    </p>
                  ) : null}
                  <p><strong>风险事实：</strong> ST {yesNoUnknown(card.st_flag)}；已确认退市 {yesNoUnknown(card.delist_flag)}（{card.delist_context_status}）</p>
                  <p><strong>上下文：</strong> Selection {card.selection_context_status}；HMM {card.hmm_context_status}（均不改变本卡方向）</p>
                  <p className="pv2-mono" style={{ fontSize: 12 }}>card_id: {card.card_id}</p>
                </article>
              );
            })}
          </div>
        )}
      </SectionCard>

      <SectionCard title="下一决策日意图" eyebrow="timing-owned user input">
        <p>真实持仓始终分析；自选股须显式勾选。范围和意图更新只影响下一张尚未签发的卡片。</p>
        {scopeWarnings.length ? (
          <div className="pv2-card" role="status" style={{ marginBottom: 12 }}>
            <strong>分析范围提示：</strong>{" "}
            {scopeWarnings.map((item) => (
              <span key={item.canonical_symbol} style={{ marginRight: 12 }}>
                {item.canonical_symbol} {item.reason_code}{" "}
                <button
                  className="pv2-button"
                  type="button"
                  disabled={savingScopeSymbol === item.canonical_symbol}
                  onClick={() => void saveAnalysisScope(item.canonical_symbol, false)}
                >
                  移除失效选择
                </button>
              </span>
            ))}
          </div>
        ) : null}
        <div className="pv2-table-wrap">
          <table className="pv2-table">
            <thead><tr><th>标的</th><th>来源</th><th>择时分析</th><th>当前数量</th><th>计划满仓金额</th><th>目标暴露</th><th>操作</th></tr></thead>
            <tbody>
              {intentRows.map((row) => {
                const draft = drafts[row.canonical_symbol] || { notional: "", exposure: "0.25" };
                return (
                  <tr key={row.canonical_symbol}>
                    <td><strong>{row.display_name || row.canonical_symbol}</strong><br /><span className="pv2-mono">{row.canonical_symbol}</span></td>
                    <td>
                      {row.primary_source_role}{row.source_roles.length > 1 ? " + WATCHLIST" : ""}
                      {row.normalization_reason ? <><br /><span style={{ color: "#b42318" }}>{row.normalization_reason}</span></> : null}
                    </td>
                    <td>
                      <label>
                        <input
                          type="checkbox"
                          checked={row.analysis_effective}
                          disabled={row.analysis_locked || Boolean(row.normalization_reason) || savingScopeSymbol === row.canonical_symbol}
                          onChange={(event) => void saveAnalysisScope(row.canonical_symbol, event.target.checked)}
                          aria-label={`${row.canonical_symbol} 纳入择时分析`}
                        />{" "}
                        {row.analysis_locked ? "持仓始终分析" : row.analysis_selected ? "已选择" : "未选择"}
                      </label>
                      <br /><span className="pv2-mono">{row.analysis_reason_code}</span>
                    </td>
                    <td>{row.pre_action_qty}</td>
                    <td>
                      <input
                        className="pv2-input"
                        type="number"
                        min="1"
                        step="1000"
                        value={draft.notional}
                        placeholder="例如 100000"
                        disabled={Boolean(row.normalization_reason)}
                        onChange={(event) => setDrafts((currentDrafts) => ({ ...currentDrafts, [row.canonical_symbol]: { ...draft, notional: event.target.value } }))}
                        aria-label={`${row.canonical_symbol} 计划满仓金额`}
                      />
                    </td>
                    <td>
                      <select
                        className="pv2-select"
                        value={draft.exposure}
                        disabled={Boolean(row.normalization_reason)}
                        onChange={(event) => setDrafts((currentDrafts) => ({ ...currentDrafts, [row.canonical_symbol]: { ...draft, exposure: event.target.value } }))}
                        aria-label={`${row.canonical_symbol} 目标暴露`}
                      >
                        <option value="0">0%</option><option value="0.25">25%</option><option value="0.5">50%</option><option value="1">100%</option>
                      </select>
                    </td>
                    <td>
                      <button className="pv2-button" type="button" disabled={Boolean(row.normalization_reason) || savingSymbol === row.canonical_symbol} onClick={() => void saveIntent(row.canonical_symbol)}>
                        {savingSymbol === row.canonical_symbol ? "保存中" : "保存意图"}
                      </button>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </SectionCard>

      <SectionCard title="证据与口径" eyebrow="research evidence is not stock confidence">
        <div className="pv2-grid pv2-grid-4">
          <MetricCard label="卡片证据层" value={evidence?.product_evidence_tier || "-"} hint="不显示个股胜率" />
          <MetricCard label="已签发事件" value={evidence?.event_counts?.CARD_ISSUED || 0} hint="append-only CARD_ISSUED" />
          <MetricCard label="已评价 horizon" value={evidence?.outcome_evidence?.coverage_counts?.matured || 0} hint={`待到期 ${evidence?.outcome_evidence?.coverage_counts?.pending || 0} · 不可用 ${evidence?.outcome_evidence?.coverage_counts?.unavailable || 0}`} />
          <MetricCard label="L2 状态" value={evidence?.l2_runtime_status || "-"} hint="不阻塞规则卡" />
        </div>
        <p>
          prospective outcome：{evidence?.outcome_evidence?.status || "-"}；配对成熟样本 {evidence?.outcome_evidence?.paired_matured?.count || 0}，
          动作意图样本 {evidence?.outcome_evidence?.intervention_intent?.count || 0}；物化缺失 {evidence?.outcome_evidence?.coverage_counts?.materialization_missing || 0}。
          这些是冻结政策的反事实结果，不代表用户实际成交。
        </p>
        <p>
          最低佣金按父订单估算，券商聚合口径为 <span className="pv2-mono">{evidence?.cost_disclosure?.min_commission_scope_verification || "-"}</span>。
          满仓 / 半仓 / 四分之一仓基准阈值：
          {money(evidence?.cost_disclosure?.thresholds_cny?.["1.00"])} / {money(evidence?.cost_disclosure?.thresholds_cny?.["0.50"])} / {money(evidence?.cost_disclosure?.thresholds_cny?.["0.25"])}。
        </p>
      </SectionCard>
    </main>
  );
}
