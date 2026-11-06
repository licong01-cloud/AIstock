"use client";

import { useCallback, useEffect, useRef, useState } from "react";

export type IntradayPlanRef = { plan_id: string; revision: number };
type PolicyId = "FIRST_OBSERVABLE_V1" | "PRICE_TOUCH_V1" | "BOUNDED_REVERSION_V1";
type Draft = {
  source_kind: "MANUAL" | "ADVISORY"; source_ref: string; source_revision: string;
  symbol: string; side: "BUY" | "SELL"; target_trade_date: string; valid_from: string; valid_until: string;
  lifecycle: "ACTIVE" | "CANCELLED" | "COMPLETED"; policy_id: PolicyId; allow_split: boolean;
  target_quantity: number | null; quantity_source_ref: string | null;
  price_constraint: { mode: string; intervals: Array<{ lower: string; upper: string }>; source_ref?: string;
    source_sha256?: string; valid_from?: string; valid_until?: string; price_basis: "raw_cny" };
};
type Plan = Draft & IntradayPlanRef;
type Decision = {
  decision_id: string; opportunity_id: string; as_of: string; valid_until: string; data_as_of: string | null;
  target_fraction_numerator: number; target_fraction_denominator: number; target_quantity: number | null;
  reason_codes: string[];
};
type View = { plan: Plan; status: string; reason_codes: string[]; decision: Decision | null; already_alerted: boolean };
type BatchItem = IntradayPlanRef & { status: string; reason_codes: string[]; decision: Decision | null; evaluated_at: string };
type Props = {
  apiBase: string; planRefs?: IntradayPlanRef[];
  refreshSourcePlans?: () => Promise<IntradayPlanRef[]>;
  onStatus?: (items: BatchItem[]) => void;
};
const storageKey = "position_timing_intraday_plan_refs_v1";
const policyNames: Record<PolicyId, string> = {
  FIRST_OBSERVABLE_V1: "简单执行基准", PRICE_TOUCH_V1: "到价机会", BOUNDED_REVERSION_V1: "有限等待与分批",
};
const states: Record<string, string> = { WAIT: "等待", OPPORTUNITY: "当前机会", UNAVAILABLE: "数据或来源不可用", EXPIRED: "已失效", DEFERRED: "本轮待处理" };
const reasonNames: Record<string, string> = {
  FIRST_OBSERVABLE: "首个可观察机会", PRICE_CLOSE_IN_CONSTRAINT: "最新收盘价进入指定区间",
  BOUNDED_REVERSION: "分钟回落或冲高后出现转折", TIME_BUDGET_RELEASE: "有限等待结束",
  MINUTE_STALE: "分钟数据陈旧", TRADABILITY_UNKNOWN: "交易条件尚未核实", WAITING_FOR_BAR: "等待完成的分钟线",
  SESSION_BREAK: "当前不在连续竞价提醒时段", WAITING_FOR_CONTIGUOUS_WARMUP: "等待连续分钟数据",
  SOURCE_UNVERIFIED: "来源计划尚未核实", PLAN_CANCELLED: "计划已取消", DECISION_EXPIRED: "机会有效期已过",
  INPUT_REVISION_CONFLICT: "已使用的分钟数据发生修订，请重新修订计划", DEFERRED_BUDGET: "将在下一轮优先评估",
  LIMIT_UP_BUY_BLOCKED: "涨停阻挡买入", LIMIT_DOWN_SELL_BLOCKED: "跌停阻挡卖出", SUSPENDED: "股票停牌",
  WAITING_FOR_REVERSION: "等待形态条件", WAITING_FOR_PRICE: "等待价格进入区间", NO_OBSERVED_TRADE: "该分钟尚无成交",
};
const reasons = (values: string[]) => values.map((value) => reasonNames[value] || value).join("；");
const localTime = (value: string | null) => value ? new Date(value).toLocaleString("zh-CN", { timeZone: "Asia/Shanghai", hour12: false }) : "尚无数据";
const key = () => crypto.randomUUID().replaceAll("-", "");

async function abortable<T>(pending: Promise<T>, signal: AbortSignal): Promise<T> {
  let cancel: () => void = () => undefined;
  try {
    return await Promise.race([pending, new Promise<never>((_, reject) => {
      cancel = () => reject(new Error("来源计划刷新已取消或超时"));
      if (signal.aborted) cancel();
      else signal.addEventListener("abort", cancel, { once: true });
    })]);
  } finally { signal.removeEventListener("abort", cancel); }
}

async function request<T>(base: string, path: string, body?: unknown, method = "POST", signal?: AbortSignal): Promise<T> {
  const response = await fetch(`${base.replace(/\/+$/, "")}/position-timing/intraday${path}`, {
    method: body === undefined ? "GET" : method, headers: { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body), signal,
  });
  const value = await response.json();
  if (!response.ok) throw new Error(value.detail?.error_code || `请求失败 (${response.status})`);
  return value as T;
}

export default function IntradayTimingPanel({ apiBase, planRefs, refreshSourcePlans, onStatus }: Props) {
  const [manualRefs, setManualRefs] = useState<IntradayPlanRef[]>([]);
  const [views, setViews] = useState<Record<string, View>>({});
  const [failures, setFailures] = useState<Record<string, BatchItem>>({});
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const [busy, setBusy] = useState(false);
  const [editing, setEditing] = useState<IntradayPlanRef | null>(null);
  const [symbol, setSymbol] = useState("");
  const [side, setSide] = useState<"BUY" | "SELL">("BUY");
  const [tradeDate, setTradeDate] = useState(() => new Date().toLocaleDateString("sv-SE", { timeZone: "Asia/Shanghai" }));
  const [startTime, setStartTime] = useState("09:30");
  const [endTime, setEndTime] = useState("14:57");
  const [policy, setPolicy] = useState<PolicyId>("FIRST_OBSERVABLE_V1");
  const [split, setSplit] = useState(false);
  const [quantity, setQuantity] = useState("");
  const [priceMode, setPriceMode] = useState("NONE");
  const [intervalText, setIntervalText] = useState("");
  const refs = planRefs ?? manualRefs;
  const latest = useRef({ refs, refreshSourcePlans, onStatus });
  latest.current = { refs, refreshSourcePlans, onStatus };
  const queue = useRef<IntradayPlanRef[]>([]);
  const controller = useRef<AbortController | null>(null);
  const running = useRef(false);
  const generation = useRef(0);
  const queuedRefresh = useRef(false);
  const refreshRef = useRef<() => Promise<void>>(async () => undefined);
  const invalidate = useCallback(() => {
    generation.current++;
    controller.current?.abort();
  }, []);

  useEffect(() => {
    if (planRefs !== undefined) return;
    try {
      const stored: unknown = JSON.parse(localStorage.getItem(storageKey) || "[]");
      if (Array.isArray(stored)) setManualRefs(stored.filter((r) => r && typeof r.plan_id === "string" && /^[A-Za-z0-9][A-Za-z0-9_-]{0,95}$/.test(r.plan_id) && Number.isInteger(r.revision) && r.revision > 0));
    } catch { setError("本地计划引用无法读取，请重新创建或修订计划。"); }
  }, [planRefs]);

  const remember = useCallback((next: IntradayPlanRef[]) => {
    setManualRefs(next);
    try { localStorage.setItem(storageKey, JSON.stringify(next)); }
    catch { setError("计划已保存，但浏览器未能保存引用；请记下计划编号。"); }
  }, []);

  const refresh = useCallback(async () => {
    if (document.hidden) return;
    if (running.current) { queuedRefresh.current = true; return; }
    running.current = true;
    setBusy(true);
    const token = ++generation.current;
    const abort = new AbortController();
    controller.current = abort;
    const timeout = window.setTimeout(() => abort.abort(), 25000);
    try {
      const current = latest.current.refreshSourcePlans ? await abortable(latest.current.refreshSourcePlans(), abort.signal) : latest.current.refs;
      if (token !== generation.current || abort.signal.aborted) return;
      const byId = new Map(current.map((r) => [r.plan_id, r]));
      const waiting = queue.current.filter((r) => byId.get(r.plan_id)?.revision === r.revision);
      const remaining = current.filter((r) => !waiting.some((q) => q.plan_id === r.plan_id));
      const chosen = [...waiting, ...remaining].slice(0, 50);
      if (chosen.length === 0) return;
      const batch = await request<{ items: BatchItem[] }>(apiBase, "/evaluations", { plans: chosen, idempotency_key: key() }, "POST", abort.signal);
      if (token !== generation.current || abort.signal.aborted) return;
      latest.current.onStatus?.(batch.items);
      const chosenIds = new Set(chosen.map((r) => r.plan_id));
      queue.current = [...batch.items.filter((r) => r.status === "DEFERRED").map(({ plan_id, revision }) => ({ plan_id, revision })),
        ...current.filter((r) => !chosenIds.has(r.plan_id)),
        ...chosen.filter((r) => !batch.items.some((item) => item.plan_id === r.plan_id && item.status === "DEFERRED"))];
      const query = chosen.map((ref) => `plan_ids=${encodeURIComponent(ref.plan_id)}`).join("&");
      const updated = await request<{ items: View[] }>(apiBase, `/alerts?${query}`, undefined, "GET", abort.signal);
      if (token !== generation.current || abort.signal.aborted) return;
      const newViews = Object.fromEntries(updated.items.filter((view) => view.plan && byId.get(view.plan.plan_id)?.revision === view.plan.revision).map((view) => [view.plan.plan_id, view]));
      setViews((old) => ({ ...old, ...newViews }));
      setFailures((old) => ({ ...old, ...Object.fromEntries(batch.items.map((r) => [r.plan_id, r])) }));
      setError("");
      for (const item of batch.items) {
        const view = newViews[item.plan_id];
        const decision = view?.decision;
        if (item.status !== "OPPORTUNITY" || view?.status !== "OPPORTUNITY" || !decision || view.already_alerted || Date.now() >= Date.parse(decision.valid_until)) continue;
        if (token !== generation.current || abort.signal.aborted) return;
        const claim = await request<{ authorized: boolean }>(apiBase, `/alerts/${decision.opportunity_id}/claim`, {
          plan_id: item.plan_id, expected_revision: item.revision, decision_id: decision.decision_id, idempotency_key: key(),
        }, "POST", abort.signal);
        if (claim.authorized && token === generation.current && !document.hidden && !abort.signal.aborted) {
          setNotice(`${view.plan.symbol} ${view.plan.side === "BUY" ? "买入" : "卖出"}机会：${reasons(decision.reason_codes)}。请自行确认交易条件。`);
          setViews((old) => ({ ...old, [item.plan_id]: { ...view, already_alerted: true } }));
        }
      }
    } catch (exception) {
      if (token === generation.current) setError(exception instanceof Error ? exception.message : "分钟建议读取失败");
    } finally {
      window.clearTimeout(timeout);
      running.current = false;
      setBusy(false);
      if (queuedRefresh.current) {
        queuedRefresh.current = false;
        window.setTimeout(() => void refreshRef.current(), 0);
      }
    }
  }, [apiBase]);
  refreshRef.current = refresh;

  const refsIdentity = refs.map((r) => `${r.plan_id}:${r.revision}`).join(",");
  useEffect(() => {
    invalidate();
    const start = window.setTimeout(() => void refresh(), 0);
    const timer = window.setInterval(() => void refresh(), 60000);
    const visibility = () => {
      if (document.hidden) { queuedRefresh.current = false; invalidate(); setNotice(""); }
      else void refresh();
    };
    document.addEventListener("visibilitychange", visibility);
    return () => { window.clearTimeout(start); window.clearInterval(timer); queuedRefresh.current = false; invalidate(); document.removeEventListener("visibilitychange", visibility); };
  }, [refsIdentity, refresh, invalidate]);

  async function save(lifecycle: Draft["lifecycle"] = "ACTIVE") {
    setBusy(true); setError("");
    try {
      const intervals = intervalText.split(/\n/).filter((s) => s.trim()).map((line) => {
        const parts = line.trim().split(/\s*[-～~]\s*/);
        if (parts.length !== 2 || !parts.every((v) => /^\d+(\.\d{1,2})?$/.test(v))) throw new Error("价格区间请每行填写：10.00-10.50");
        return { lower: parts[0], upper: parts[1] };
      });
      const intervalHash = Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(JSON.stringify(intervals))))).map((v) => v.toString(16).padStart(2, "0")).join("");
      const validFrom = `${tradeDate}T${startTime}:00+08:00`, validUntil = `${tradeDate}T${endTime}:00+08:00`;
      const draft: Draft = { source_kind: "MANUAL", source_ref: "USER_SUPPLIED", source_revision: key(), symbol: symbol.trim().toUpperCase(), side,
        target_trade_date: tradeDate, valid_from: validFrom, valid_until: validUntil, lifecycle, policy_id: policy,
        allow_split: policy === "BOUNDED_REVERSION_V1" && split,
        target_quantity: quantity ? Number(quantity) : null, quantity_source_ref: quantity ? "USER_SUPPLIED_REMAINING_QUANTITY" : null,
        price_constraint: { mode: priceMode, intervals, price_basis: "raw_cny", source_ref: "USER_SUPPLIED", source_sha256: intervalHash,
          valid_from: validFrom, valid_until: validUntil } };
      const id = editing?.plan_id || `manual_${key()}`;
      const result = await request<{ plan: Plan }>(apiBase, `/plans/${id}`, { expected_revision: editing?.revision || 0, idempotency_key: key(), plan: draft }, "PUT");
      const next = [...latest.current.refs.filter((r) => r.plan_id !== id), { plan_id: id, revision: result.plan.revision }];
      remember(next); setEditing(null); setNotice("计划已保存；下一轮将依据已完成分钟线评估。");
    } catch (exception) { setError(exception instanceof Error ? exception.message : "计划保存失败"); }
    finally { setBusy(false); }
  }

  function edit(plan: Plan) {
    invalidate();
    setEditing({ plan_id: plan.plan_id, revision: plan.revision }); setSymbol(plan.symbol); setSide(plan.side); setTradeDate(plan.target_trade_date);
    const clock = (value: string) => new Date(value).toLocaleTimeString("en-GB", { timeZone: "Asia/Shanghai", hour12: false }).slice(0, 5);
    setStartTime(clock(plan.valid_from)); setEndTime(clock(plan.valid_until)); setPolicy(plan.policy_id); setSplit(plan.allow_split);
    setQuantity(plan.target_quantity?.toString() || ""); setPriceMode(plan.price_constraint.mode);
    setIntervalText(plan.price_constraint.intervals.map((v) => `${v.lower}-${v.upper}`).join("\n"));
  }

  return <section className="rounded-lg border border-border bg-card p-4 text-card-foreground space-y-4" aria-label="分钟择时建议">
    <div className="flex items-center justify-between gap-3"><div><h2 className="text-lg font-semibold">分钟择时建议</h2><p className="text-sm text-muted-foreground">针对已确定的买卖计划，每分钟评估一次。规则建议仅供人工参考，成交进度未知。</p></div>
      <button className="rounded border px-3 py-2" type="button" disabled={busy} onClick={() => void refresh()}>{busy ? "评估中" : "刷新分钟建议"}</button></div>
    {notice && <div role="status" aria-live="polite" className="rounded border border-primary p-3">{notice}</div>}
    {error && <div role="alert" className="rounded border border-destructive p-3">分钟建议：{error}</div>}
    {planRefs === undefined && <form className="grid gap-3 md:grid-cols-3" onSubmit={(event) => { event.preventDefault(); void save(); }}>
      <label>股票代码<input className="block w-full rounded border bg-background p-2" aria-label="分钟计划股票代码" required placeholder="000001.SZ" value={symbol} onChange={(e) => setSymbol(e.target.value)} /></label>
      <label>交易方向<select className="block w-full rounded border bg-background p-2" value={side} onChange={(e) => setSide(e.target.value as "BUY" | "SELL")}><option value="BUY">买入</option><option value="SELL">卖出</option></select></label>
      <label>交易日期<input className="block w-full rounded border bg-background p-2" required type="date" value={tradeDate} onChange={(e) => setTradeDate(e.target.value)} /></label>
      <label>开始时间<input className="block w-full rounded border bg-background p-2" type="time" required value={startTime} onChange={(e) => setStartTime(e.target.value)} /></label>
      <label>截止时间<input className="block w-full rounded border bg-background p-2" type="time" required value={endTime} onChange={(e) => setEndTime(e.target.value)} /></label>
      <label>剩余计划股数（可空）<input className="block w-full rounded border bg-background p-2" type="number" min="1" step="1" value={quantity} onChange={(e) => setQuantity(e.target.value)} /></label>
      <label>择时规则<select className="block w-full rounded border bg-background p-2" value={policy} onChange={(e) => setPolicy(e.target.value as PolicyId)}>{Object.entries(policyNames).map(([value, name]) => <option key={value} value={value}>{name}</option>)}</select></label>
      <label>价格条件<select className="block w-full rounded border bg-background p-2" value={priceMode} onChange={(e) => setPriceMode(e.target.value)}>{[["NONE", "不使用区间"], ["REFERENCE", "仅展示区间"], ["PREFERRED", "偏好区间"], ["HARD", "硬性区间"]].map(([value, name]) => <option key={value} value={value}>{name}</option>)}</select></label>
      <label>价格区间（元，每行一段）<textarea className="block w-full rounded border bg-background p-2" placeholder="10.00-10.50" value={intervalText} onChange={(e) => setIntervalText(e.target.value)} /></label>
      {policy === "BOUNDED_REVERSION_V1" && <label><input type="checkbox" checked={split} onChange={(e) => setSplit(e.target.checked)} /> 最多三批建议；下一批需明确更新剩余计划</label>}
      <div className="flex flex-wrap gap-2 md:col-span-3"><button className="rounded bg-primary px-3 py-2 text-primary-foreground" disabled={busy} type="submit">{editing ? "保存剩余计划修订" : "创建分钟计划"}</button>
        {editing && <><button className="rounded border px-3 py-2" disabled={busy} type="button" onClick={() => void save("CANCELLED")}>取消该计划</button><button className="rounded border px-3 py-2" type="button" onClick={() => setEditing(null)}>返回新建</button></>}</div>
    </form>}
    {refs.length === 0 && <p className="text-sm text-muted-foreground">尚无明确的盘中买卖计划。</p>}
    <div className="grid gap-3 md:grid-cols-2">{refs.map((ref) => {
      const view = views[ref.plan_id]?.plan.revision === ref.revision ? views[ref.plan_id] : null;
      const failure = failures[ref.plan_id]?.revision === ref.revision ? failures[ref.plan_id] : null;
      const decision = view?.decision;
      const expired = !!decision && Date.now() >= Date.parse(decision.valid_until);
      const status = failure && failure.status !== "OPPORTUNITY" ? failure.status : expired ? "UNAVAILABLE" : view?.status || "WAIT";
      return <article className="rounded border border-border p-3 space-y-2" key={`${ref.plan_id}:${ref.revision}`}>
        <strong>{view?.plan.symbol || ref.plan_id} {view?.plan.side === "SELL" ? "卖出" : view?.plan.side === "BUY" ? "买入" : ""}</strong><p>{states[status] || status} · {view ? policyNames[view.plan.policy_id] : "待读取"}</p>
        <p className="text-sm">{reasons(failure && failure.status !== "OPPORTUNITY" ? failure.reason_codes : expired ? ["DECISION_EXPIRED"] : view?.reason_codes || ["WAITING_FOR_BAR"])}</p>
        {decision && <><p>建议累计比例 {decision.target_fraction_numerator}/{decision.target_fraction_denominator}；{decision.target_quantity === null ? "股数未知" : `累计目标 ${decision.target_quantity} 股`}</p><p className="text-sm">分钟数据截至 {localTime(decision.data_as_of)}；有效至 {localTime(decision.valid_until)}</p></>}
        {view && view.plan.price_constraint.mode !== "NONE" && <p>价格条件：{view.plan.price_constraint.intervals.map((v) => `${v.lower}～${v.upper}`).join(" / ")} 元（{view.plan.price_constraint.mode}）</p>}
        {view?.already_alerted && <p className="text-sm text-muted-foreground">本批提醒已授权；当前状态继续展示，尚未确认成交。</p>}
        {view && planRefs === undefined && <button className="rounded border px-2 py-1" type="button" onClick={() => edit(view.plan)}>修订或取消计划</button>}
      </article>;
    })}</div>
  </section>;
}
