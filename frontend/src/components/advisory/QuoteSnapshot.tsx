"use client";
import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { read } from "@/lib/api/advisoryWorkbench";
import { time } from "./workbench/shared";
import styles from "./workbench/workbench.module.css";

type Recommendation = { segment_id: string | null; start_date: string | null; end_date: string | null; consecutive_days?: number | null; known_through_date?: string | null; base_price: number | null; end_price: number | null; return_bps: number | null; valuation_date?: string | null; state: string; reason_code: string | null };
export type Quote = { symbol: string; price: number | null; kind: string; trade_date: string | null; observed_at: string; reason_code?: string | null; today_return_bps?: number | null; recommendation?: Recommendation; snapshot_error?: boolean };
export type Quotes = Record<string, Quote>;
type Snapshot = { refresh: () => void; loading: boolean; symbols: string[] };
export const QuoteContext = createContext<{ items: Quotes; register: (symbol: string) => () => void; snapshot?: Snapshot }>({ items: {}, register: () => () => undefined });
class InvalidIdentity extends Error {}

export function SnapshotQuoteProvider({ children, programId, listVersionId }: { children: ReactNode; programId?: string; listVersionId?: string | null }) {
  const counts = useRef(new Map<string, number>());
  const cache = useRef<Quotes>({});
  const [items, setItems] = useState<Quotes>({});
  const [symbols, setSymbols] = useState("");
  const [revision, setRevision] = useState(0), [loading, setLoading] = useState(false);
  const lastRevision = useRef(0);
  const queue = useRef(new Set<string>()), pending = useRef(new Set<string>());
  const running = useRef(false), alive = useRef(true);
  const active = useRef<AbortController | null>(null);
  const debounce = useRef<ReturnType<typeof setTimeout>>();
  const register = useCallback((symbol: string) => {
    const update = () => {
      clearTimeout(debounce.current);
      debounce.current = setTimeout(() => setSymbols([...counts.current.keys()].sort().join(",")), 75);
    };
    counts.current.set(symbol, (counts.current.get(symbol) || 0) + 1); update();
    return () => {
      const remaining = (counts.current.get(symbol) || 1) - 1;
      if (remaining) counts.current.set(symbol, remaining); else counts.current.delete(symbol);
      update();
    };
  }, []);
  useEffect(() => () => clearTimeout(debounce.current), []);
  useEffect(() => {
    alive.current = true;
    const queued = queue.current, pendingSymbols = pending.current;
    return () => { alive.current = false; active.current?.abort(); queued.clear(); pendingSymbols.clear(); };
  }, []);
  useEffect(() => {
    const wanted = symbols ? symbols.split(",") : [];
    const manual = lastRevision.current !== revision;
    lastRevision.current = revision;
    const missing = wanted.filter(symbol => !pending.current.has(symbol) && (manual || !cache.current[symbol]));
    for (const symbol of missing) { pending.current.add(symbol); queue.current.add(symbol); }
    if (running.current || !queue.current.size) return;
    running.current = true;
    const merge = (next: Quotes) => {
      if (!alive.current) return;
      const combined = { ...cache.current, ...next };
      // Bound session memory without evicting the currently displayed pages.
      const removable = Object.keys(combined).filter(symbol => !counts.current.has(symbol));
      let excess = Object.keys(combined).length - 2048;
      for (const symbol of removable) { if (excess-- <= 0) break; delete combined[symbol]; }
      cache.current = combined; setItems(combined);
    };
    const unavailable = (symbol: string, identityInvalid = false): Quote => {
      const previous = cache.current[symbol];
      if (!identityInvalid && previous?.price != null) return { ...previous, snapshot_error: true };
      return { symbol, price: null, kind: "UNAVAILABLE", trade_date: null, observed_at: "", snapshot_error: true, reason_code: programId && listVersionId ? "RETURN_READ_UNAVAILABLE" : "TDX_UNAVAILABLE" };
    };
    setLoading(true);
    void (async () => {
      while (queue.current.size && alive.current) {
        const chunk = [...queue.current].slice(0, 100);
        for (const symbol of chunk) queue.current.delete(symbol);
        // A separate controller per batch prevents one timeout from cancelling later batches.
        const batch = new AbortController();
        active.current = batch;
        const timeout = setTimeout(() => batch.abort("timeout"), 14000);
        try {
          const query = `symbols=${encodeURIComponent(chunk.join(","))}`;
          const url = programId && listVersionId ? `/advisory/workbench/programs/${encodeURIComponent(programId)}/returns?list_version_id=${encodeURIComponent(listVersionId)}&${query}` : `/advisory/workbench/quotes?${query}`;
          const data = await read<{ schema_version: string; return_schema_version?: string; program_id?: string; list_version_id?: string; items: Quotes }>(url, batch.signal);
          if (!alive.current) break;
          if (programId && listVersionId && (data.program_id !== programId || data.list_version_id !== listVersionId || data.return_schema_version !== "advisory_recommendation_returns_v1")) throw new InvalidIdentity();
          if (data.schema_version !== "advisory_display_quotes_v1" || !data.items) throw new Error("Invalid quote response");
          const next: Quotes = {};
          for (const symbol of chunk) {
            const q = data.items[symbol];
            next[symbol] = q && q.symbol !== symbol ? unavailable(symbol, true)
              : q?.symbol === symbol && q.price != null && Number.isFinite(q.price) && q.price > 0
              ? { ...q, snapshot_error: false }
              : q?.symbol === symbol && q.price === null && !cache.current[symbol]?.price
                ? { ...q, snapshot_error: true } : unavailable(symbol);
          }
          merge(next);
        } catch (error) {
          if (!alive.current) break;
          merge(Object.fromEntries(chunk.map(symbol => [symbol, unavailable(symbol, error instanceof InvalidIdentity)])));
        } finally {
          clearTimeout(timeout);
          for (const symbol of chunk) pending.current.delete(symbol);
        }
      }
      running.current = false; active.current = null;
      if (alive.current) setLoading(false);
    })();
  }, [symbols, revision, programId, listVersionId]);
  const refresh = useCallback(() => setRevision(value => value + 1), []);
  const value = useMemo(() => ({ items, register, snapshot: { refresh, loading, symbols: symbols ? symbols.split(",") : [] } }), [items, register, refresh, loading, symbols]);
  return <QuoteContext.Provider value={value}>{children}</QuoteContext.Provider>;
}

export function QuoteRefresh() {
  const { items, snapshot } = useContext(QuoteContext);
  if (!snapshot) return null;
  const rows = snapshot.symbols.map(symbol => items[symbol]).filter(Boolean);
  const latest = rows.map(q => q.observed_at).filter(Boolean).sort().at(-1);
  const failed = rows.some(q => q.snapshot_error);
  return <div className={styles.quoteToolbar} data-testid="advisory-quote-toolbar">
    <div><strong>行情快照</strong><span>首次读取后保留，点击按钮更新</span><small>本页最近获取：{latest ? time(latest) : "尚未获取"}</small>
      {failed && <small role="alert">部分行情未能更新；有旧值的保留上次快照，无有效数据的显示“—”。</small>}
    </div>
    <button onClick={snapshot.refresh} disabled={snapshot.loading || !snapshot.symbols.length} aria-busy={snapshot.loading}>{snapshot.loading ? "获取行情中…" : "刷新行情"}</button>
  </div>;
}
