"use client";
import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { read, type Episode } from "@/lib/api/advisoryWorkbench";
import { bps, day, gain, number, text, time } from "./workbench/shared";

type Quote = { symbol: string; price: number | null; kind: string; trade_date: string | null; observed_at: string; reason_code?: string | null };
type Quotes = Record<string, Quote>;
const Context = createContext<{ items: Quotes; register: (symbol: string) => () => void }>({ items: {}, register: () => () => undefined });
const canonical = (symbol: string) => symbol.trim().toUpperCase().replace(/^(SH|SZ|BJ)(\d{6})$/, "$2.$1");
const valid = (symbol: string) => /^\d{6}\.(SH|SZ|BJ)$/.test(canonical(symbol));
const kindText: Record<string, string> = { LIVE: "实时", PAUSED: "午间最新成交", CLOSE: "最近收盘", STALE_CLOSE: "较早收盘", NO_TRADES: "暂无成交快照" };
const reasonText: Record<string, string> = { CALENDAR_UNAVAILABLE: "交易日历不可用", TDX_TIMEOUT: "TDX行情超时", TDX_PRICE_MISSING: "TDX暂无有效价格", TDX_CLOSE_MISSING: "TDX暂无收盘价", TDX_DUPLICATE_SYMBOL: "TDX代码重复", QUOTE_BUSY: "行情繁忙，稍后重试" };

export function QuoteProvider({ children }: { children: ReactNode }) {
  const counts = useRef(new Map<string, number>());
  const [symbols, setSymbols] = useState("");
  const [items, setItems] = useState<Quotes>({});
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
    // Scope quotes to mounted visible securities; a page change cannot reuse another page's response.
    setItems(previous => Object.fromEntries(wanted.filter(s => previous[s]).map(s => [s, previous[s]])));
    const refresh = async () => {
      if (stopped || document.hidden || busy) return;
      busy = true; controller = new AbortController();
      const active = controller;
      for (let offset = 0; offset < wanted.length && !active.signal.aborted && !stopped; offset += 100) {
        const chunk = wanted.slice(offset, offset + 100);
        const timeout = setTimeout(() => active.abort("timeout"), 14000);
        try {
          const data = await read<{ schema_version: string; items: Quotes }>(`/advisory/workbench/quotes?symbols=${encodeURIComponent(chunk.join(","))}`, active.signal);
          if (active.signal.aborted || stopped) break;
          if (data.schema_version !== "advisory_display_quotes_v1" || !data.items) throw new Error("Invalid quote response");
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
          setItems(previous => ({ ...previous, ...Object.fromEntries(failed.map(symbol => [symbol, { symbol, price: null, kind: "UNAVAILABLE", trade_date: null, observed_at: "" }])) }));
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
  }, [symbols]);
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
  const reason = !episode || episode.progress === "PENDING_ENTRY" ? "待进入确认，尚无过程收益"
    : episode.progress === "PENDING_EXIT" ? "待退出结算" : text(episode.performance_reason_code, "参考价格待核验");
  return <span className={gain(value)}>{bps(value)}<small style={{ display: "block" }}>{value == null ? reason
    : `${episode?.return_kind === "REALIZED" ? "已结束参考收益" : "浮动参考收益"} · ${day(episode?.return_kind === "REALIZED" ? episode.effective_exit_date : episode?.mark_date || episode?.metric_as_of_trade_date)}`}</small></span>;
}
