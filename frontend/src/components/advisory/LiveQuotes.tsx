"use client";
import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { read, type Episode } from "@/lib/api/advisoryWorkbench";
import { bps, day, gain, number, text, time } from "./workbench/shared";

type Recommendation = { segment_id: string | null; start_date: string | null; end_date: string | null; known_through_date?: string | null; base_price: number | null; end_price: number | null; return_bps: number | null; valuation_date?: string | null; state: string; reason_code: string | null };
type Quote = { symbol: string; price: number | null; kind: string; trade_date: string | null; observed_at: string; reason_code?: string | null; today_return_bps?: number | null; recommendation?: Recommendation };
type Quotes = Record<string, Quote>;
const Context = createContext<{ items: Quotes; register: (symbol: string) => () => void }>({ items: {}, register: () => () => undefined });
const canonical = (symbol: string) => symbol.trim().toUpperCase().replace(/^(SH|SZ|BJ)(\d{6})$/, "$2.$1");
const valid = (symbol: string) => /^\d{6}\.(SH|SZ|BJ)$/.test(canonical(symbol));
const kindText: Record<string, string> = { LIVE: "实时", PAUSED: "午间最新成交", CLOSE: "最近收盘", STALE_CLOSE: "较早收盘", NO_TRADES: "暂无成交快照" };
const reasonText: Record<string, string> = { CALENDAR_UNAVAILABLE: "交易日历不可用", TDX_TIMEOUT: "TDX行情超时", TDX_PRICE_MISSING: "TDX暂无有效价格", TDX_CLOSE_MISSING: "TDX暂无收盘价", TDX_DUPLICATE_SYMBOL: "TDX代码重复", QUOTE_BUSY: "行情繁忙，稍后重试", RETURN_READ_UNAVAILABLE: "行情或名单身份不可用，请刷新" };

export function QuoteProvider({ children, programId, listVersionId }: { children: ReactNode; programId?: string; listVersionId?: string | null }) {
  const counts = useRef(new Map<string, number>());
  const [symbols, setSymbols] = useState("");
  const [items, setItems] = useState<Quotes>({});
  const quoteScope = useRef<string>();
  const debounce = useRef<ReturnType<typeof setTimeout>>();
  const register = useCallback((symbol: string) => {
    if (!valid(symbol)) return () => undefined;
    const update = () => {
      clearTimeout(debounce.current);
      debounce.current = setTimeout(() => setSymbols([...counts.current.keys()].sort().join(",")), 75);
    };
    counts.current.set(symbol, (counts.current.get(symbol) || 0) + 1); update();
    return () => {
      const count = (counts.current.get(symbol) || 1) - 1;
      if (count) counts.current.set(symbol, count); else counts.current.delete(symbol);
      update();
    };
  }, []);
  useEffect(() => () => clearTimeout(debounce.current), []);
  useEffect(() => {
    if (!symbols) { setItems({}); return; }
    let stopped = false, busy = false;
    let controller: AbortController | null = null;
    const wanted = symbols.split(",");
    const scope = JSON.stringify([programId || null, listVersionId || null]);
    const sameScope = quoteScope.current === scope;
    quoteScope.current = scope;
    // Clearing visible quotes changes row heights and can feed back into the observer.
    // Keep overlapping securities only within this exact program/list identity.
    setItems(previous => sameScope ? Object.fromEntries(wanted.filter(s => previous[s]).map(s => [s, previous[s]])) : {});
    const refresh = async () => {
      if (stopped || document.hidden || busy) return;
      busy = true; controller = new AbortController();
      const active = controller;
      for (let offset = 0; offset < wanted.length && !active.signal.aborted && !stopped; offset += 100) {
        const chunk = wanted.slice(offset, offset + 100);
        const timeout = setTimeout(() => active.abort("timeout"), 14000);
        try {
          const query = `symbols=${encodeURIComponent(chunk.join(","))}`;
          const url = programId && listVersionId ? `/advisory/workbench/programs/${encodeURIComponent(programId)}/returns?list_version_id=${encodeURIComponent(listVersionId)}&${query}` : `/advisory/workbench/quotes?${query}`;
          const data = await read<{ schema_version: string; return_schema_version?: string; program_id?: string; list_version_id?: string; items: Quotes }>(url, active.signal);
          if (active.signal.aborted || stopped) break;
          if (data.schema_version !== "advisory_display_quotes_v1" || !data.items) throw new Error("Invalid quote response");
          if (programId && listVersionId && (data.program_id !== programId || data.list_version_id !== listVersionId || data.return_schema_version !== "advisory_recommendation_returns_v1")) throw new Error("Invalid recommendation identity");
          const next: Quotes = {};
          for (const symbol of chunk) {
            const q = data.items[symbol];
            next[symbol] = q?.symbol === symbol && (q.price == null || (Number.isFinite(q.price) && q.price > 0))
              ? q : { symbol, price: null, kind: "UNAVAILABLE", trade_date: null, observed_at: "" };
          }
          setItems(previous => ({ ...previous, ...next }));
        } catch {
          if (stopped || (active.signal.aborted && active.signal.reason !== "timeout")) break;
          const failed = active.signal.aborted ? wanted.slice(offset) : chunk;
          setItems(previous => ({ ...previous, ...Object.fromEntries(failed.map(symbol => [symbol, { symbol, price: null, kind: "UNAVAILABLE", trade_date: null, observed_at: "", reason_code: programId && listVersionId ? "RETURN_READ_UNAVAILABLE" : "TDX_UNAVAILABLE" }])) }));
        } finally { clearTimeout(timeout); }
      }
      busy = false;
    };
    const visibility = () => { if (document.hidden) controller?.abort(); else void refresh(); };
    void refresh();
    const timer = setInterval(() => void refresh(), 15000);
    document.addEventListener("visibilitychange", visibility);
    window.addEventListener("focus", visibility);
    return () => { stopped = true; controller?.abort(); clearInterval(timer); document.removeEventListener("visibilitychange", visibility); window.removeEventListener("focus", visibility); };
  }, [symbols, programId, listVersionId]);
  const value = useMemo(() => ({ items, register }), [items, register]);
  return <Context.Provider value={value}>{children}</Context.Provider>;
}

function useQuote(symbol: string) {
  symbol = canonical(symbol);
  const { items, register } = useContext(Context);
  const ref = useRef<HTMLSpanElement>(null);
  useEffect(() => {
    const node = ref.current;
    if (!node || !valid(symbol)) return;
    let release: (() => void) | undefined;
    const observer = new IntersectionObserver(entries => {
      if (entries[0].isIntersecting) { release ??= register(symbol); }
      else { release?.(); release = undefined; }
    }, { rootMargin: "200px" });
    observer.observe(node);
    return () => { observer.disconnect(); release?.(); };
  }, [symbol, register]);
  return { ref, quote: items[symbol] };
}

export function QuoteCell({ symbol }: { symbol: string }) {
  const { ref, quote } = useQuote(symbol);
  const unavailable = !valid(symbol) ? "证券代码待核验" : !quote ? "读取行情中…" : reasonText[quote.reason_code || ""] || "TDX行情暂不可用";
  return <span ref={ref} data-testid="advisory-current-price" style={{ display: "inline-block", minWidth: 100 }}>
    {quote?.price != null ? <><strong>¥{number(quote.price, 2)}</strong><small style={{ display: "block" }}>{kindText[quote.kind] || "TDX快照"} · {day(quote.trade_date)}</small><small style={{ display: "block" }}>获取 {time(quote.observed_at)}</small></> : <><span>—</span><small style={{ display: "block" }}>{unavailable}</small></>}
  </span>;
}

export function ReferenceChange({ symbol, price, date }: { symbol: string; price?: number | null; date?: string | null }) {
  const { ref, quote } = useQuote(symbol);
  const ready = price != null && Number.isFinite(price) && price > 0 && quote?.price != null && !!date && !!quote.trade_date && quote.trade_date >= date.slice(0, 10);
  const value = ready ? (quote!.price! / price! - 1) * 10000 : null;
  const reason = !date || price == null || !Number.isFinite(price) || price <= 0 ? "缺少推荐参考价" : quote?.price == null ? "等待有效行情" : "行情早于参考日";
  return <span ref={ref} className={gain(value)} data-testid="advisory-reference-change">{bps(value)}<small style={{ display: "block" }}>{value == null ? reason : `较 ${day(date)} 参考价 · 未复权`}</small></span>;
}

export function EpisodeReturn({ episode }: { episode?: Episode | null }) {
  const value = episode?.return_bps;
  const reason = !episode || episode.progress === "PENDING_ENTRY" ? "策略过程基准尚未确认"
    : episode.progress === "PENDING_EXIT" ? "待退出结算" : text(episode.performance_reason_code, "参考价格待核验");
  return <span className={gain(value)}>{bps(value)}<small style={{ display: "block" }}>{value == null ? reason
    : `${episode?.return_kind === "REALIZED" ? "已结束参考收益" : "浮动参考收益"} · ${day(episode?.return_kind === "REALIZED" ? episode.effective_exit_date : episode?.mark_date || episode?.metric_as_of_trade_date)}`}</small></span>;
}

const returnReasons: Record<string, string> = { CONTINUITY_UNKNOWN: "推荐连续性待核验", NOT_RECOMMENDED: "不属于本次连续推荐", OPEN_NOT_YET_AVAILABLE: "开盘基准尚未形成", BASE_OPEN_MISSING: "缺少首次生效日开盘价", BASE_SUSPENDED: "首次生效日停牌，无开盘基准", END_OPEN_MISSING: "缺少移出生效日开盘价", QUOTE_NOT_VALID_FOR_SEGMENT: "等待本段有效行情" };
export function RecommendationValue({ symbol, field }: { symbol: string; field: "start" | "base" | "return" }) {
  const { ref, quote } = useQuote(symbol);
  const r = quote?.recommendation;
  const reason = r ? returnReasons[r.reason_code || ""] || "数据待核验" : quote ? "推荐涨跌暂不可用，请刷新" : "读取推荐涨跌中…";
  const value = field === "return" ? r?.return_bps : field === "base" ? r?.base_price : null;
  return <span ref={ref} data-testid={`advisory-recommendation-${field}`} className={field === "return" ? gain(value) : undefined} style={{ display: "inline-block", minWidth: 105 }}>
    {field === "start" ? day(r?.start_date) : field === "base" ? value == null ? "—" : `¥${number(value, 2)}` : bps(value)}
    <small style={{ display: "block" }}>{field === "start" ? r?.start_date ? "本次连续推荐" : reason : value == null ? reason : field === "base" ? "首次生效日开盘·未复权" : r?.state === "ENDED" ? `已结束·冻结于 ${day(r.end_date)} 开盘` : `未复权·价格变化 ${day(r?.valuation_date)}`}</small>
    {field === "return" && r?.known_through_date && r.state !== "ENDED" && <small style={{ display: "block" }}>名单截至 {day(r.known_through_date)}</small>}
  </span>;
}

export function TodayChange({ symbol }: { symbol: string }) {
  const { ref, quote } = useQuote(symbol);
  const value = quote?.today_return_bps;
  return <span ref={ref} data-testid="advisory-today-change" className={gain(value)} style={{ display: "inline-block", minWidth: 100 }}>{bps(value)}<small style={{ display: "block" }}>{value == null ? "缺少有效行情或昨收" : `${day(quote?.trade_date)} · 较昨收`}</small></span>;
}
