"use client";

import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ChevronDown, ChevronRight, RefreshCw, XCircle } from "lucide-react";
import {
  DurableChild,
  DurableEvent,
  DurableEventsPage,
  MultiAlphaApiError,
  multiAlphaRequest,
} from "./multiAlphaEvolutionAdapter";

type Props = {
  apiBase: string;
  runId: string;
  refreshToken?: number;
  busy?: boolean;
  onCancelAttempt: (attemptId: string) => void | Promise<void>;
  onSelectRecoveryChild: (childId: string) => void;
};

type SortColumn = "ordinal" | "child_key" | "child_kind" | "status" | "phase" | "updated_at";

const TERMINAL = new Set(["succeeded", "failed", "cancelled", "not_computable", "not_recovered"]);

function text(value: unknown): string {
  return value == null ? "" : String(value);
}

function numeric(value: unknown): number | null {
  if (value == null || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

export function compareChildValues(left: DurableChild, right: DurableChild, column: SortColumn, direction: "asc" | "desc"): number {
  let comparison = 0;
  if (column === "ordinal") {
    const a = numeric(left.ordinal);
    const b = numeric(right.ordinal);
    comparison = a == null && b == null ? 0 : a == null ? 1 : b == null ? -1 : a - b;
  } else {
    const a = text(left[column]).toLocaleLowerCase();
    const b = text(right[column]).toLocaleLowerCase();
    comparison = a.localeCompare(b, "zh-CN", { numeric: true, sensitivity: "base" });
  }
  if (comparison === 0) comparison = left.child_id.localeCompare(right.child_id);
  return direction === "asc" ? comparison : -comparison;
}

function formatTime(value?: string | null): string {
  if (!value) return "-";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString("zh-CN");
}

function errorText(error: unknown): string {
  if (error instanceof MultiAlphaApiError) return `${error.reasonCode}: ${error.message} context=${JSON.stringify(error.context)}`;
  return error instanceof Error ? error.message : String(error);
}

function manifestSummary(value?: Record<string, unknown> | null): string {
  if (!value) return "-";
  const keys = Object.keys(value);
  return keys.length ? `${keys.length} fields: ${keys.slice(0, 4).join(", ")}` : "{}";
}

export default function MultiAlphaChildGrid({ apiBase, runId, refreshToken, busy, onCancelAttempt, onSelectRecoveryChild }: Props) {
  const [children, setChildren] = useState<DurableChild[]>([]);
  const [events, setEvents] = useState<DurableEvent[]>([]);
  const [lastEventId, setLastEventId] = useState(0);
  const lastEventIdRef = useRef(0);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [statusFilter, setStatusFilter] = useState("all");
  const [nodeFilter, setNodeFilter] = useState("all");
  const [errorOnly, setErrorOnly] = useState(false);
  const [query, setQuery] = useState("");
  const [sort, setSort] = useState<{ column: SortColumn; direction: "asc" | "desc" }>({ column: "ordinal", direction: "asc" });

  useEffect(() => { lastEventIdRef.current = lastEventId; }, [lastEventId]);

  const load = useCallback(async () => {
    if (!runId) return;
    setLoading(true);
    setError(null);
    try {
      const childPage = await multiAlphaRequest<{ children: DurableChild[] }>(`${apiBase}/multi-alpha/combine-backtest/runs/${encodeURIComponent(runId)}/children?include_attempts=true`);
      setChildren(childPage.children || []);

      let cursor = 0;
      const allEvents: DurableEvent[] = [];
      while (true) {
        const page = await multiAlphaRequest<DurableEventsPage>(`${apiBase}/multi-alpha/combine-backtest/runs/${encodeURIComponent(runId)}/events?after_event_id=${cursor}&limit=1000`);
        allEvents.push(...page.events);
        if (!page.has_more) {
          cursor = page.next_event_id;
          break;
        }
        if (page.next_event_id <= cursor) throw new Error(`durable event cursor did not advance: ${cursor} -> ${page.next_event_id}`);
        cursor = page.next_event_id;
      }
      setEvents(allEvents);
      setLastEventId(cursor);
      lastEventIdRef.current = cursor;
    } catch (caught) {
      setError(errorText(caught));
    } finally {
      setLoading(false);
    }
  }, [apiBase, runId]);

  useEffect(() => { void load(); }, [load, refreshToken]);

  useEffect(() => {
    if (!runId || typeof EventSource === "undefined") return;
    let source: EventSource | null = null;
    let closed = false;
    const connect = () => {
      if (closed || document.visibilityState !== "visible") return;
      source = new EventSource(`${apiBase}/multi-alpha/combine-backtest/runs/${encodeURIComponent(runId)}/events/stream?after_event_id=${lastEventIdRef.current}`);
      source.addEventListener("durable_event", (message) => {
        try {
          const event = JSON.parse((message as MessageEvent).data) as DurableEvent;
          setEvents((current) => current.some((item) => item.event_id === event.event_id) ? current : [...current, event]);
          setLastEventId((current) => Math.max(current, event.event_id));
        } catch (caught) {
          setError(`durable_event_parse_failed: ${caught instanceof Error ? caught.message : String(caught)}`);
        }
      });
      source.addEventListener("stream_end", () => source?.close());
      source.addEventListener("stream_error", (message) => {
        setError(`durable_event_stream_failed: ${(message as MessageEvent).data}`);
        source?.close();
      });
    };
    const onVisibility = () => {
      if (document.visibilityState === "hidden") {
        source?.close();
        source = null;
      } else if (!source) {
        connect();
      }
    };
    connect();
    document.addEventListener("visibilitychange", onVisibility);
    return () => {
      closed = true;
      source?.close();
      document.removeEventListener("visibilitychange", onVisibility);
    };
  }, [apiBase, runId]);

  const nodes = useMemo(() => Array.from(new Set(children.flatMap((child) => (child.attempts || []).map((attempt) => text(attempt.node_id)).filter(Boolean)))).sort(), [children]);
  const rows = useMemo(() => children.filter((child) => {
    const attempts = child.attempts || [];
    if (statusFilter !== "all" && child.status !== statusFilter && !attempts.some((attempt) => attempt.status === statusFilter)) return false;
    if (nodeFilter !== "all" && !attempts.some((attempt) => attempt.node_id === nodeFilter)) return false;
    if (errorOnly && !child.error_code && !attempts.some((attempt) => attempt.error_code)) return false;
    const haystack = JSON.stringify({ child, attempts }).toLocaleLowerCase();
    return !query.trim() || haystack.includes(query.trim().toLocaleLowerCase());
  }).sort((left, right) => compareChildValues(left, right, sort.column, sort.direction)), [children, errorOnly, nodeFilter, query, sort, statusFilter]);

  const toggleSort = (column: SortColumn) => setSort((current) => current.column === column ? { column, direction: current.direction === "asc" ? "desc" : "asc" } : { column, direction: "asc" });

  return (
    <div data-testid="multi-alpha-child-grid" style={{ display: "grid", gap: 10 }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 8, flexWrap: "wrap" }}>
        <div><strong style={{ color: "#0f172a", fontSize: 13 }}>Child / Attempt 权威明细</strong><div style={{ color: "#64748b", fontSize: 10, marginTop: 2 }}>DB children={children.length} · events={events.length} · cursor={lastEventId}</div></div>
        <button onClick={() => void load()} disabled={loading} style={{ padding: "6px 9px", border: "1px solid #cbd5e1", borderRadius: 6, background: "#fff", cursor: loading ? "wait" : "pointer", fontSize: 11 }}><RefreshCw size={12} /> {loading ? "读取中…" : "刷新明细"}</button>
      </div>
      {error && <div style={{ padding: 8, borderRadius: 6, border: "1px solid #fca5a5", background: "#fef2f2", color: "#991b1b", fontSize: 11 }}>{error}</div>}
      <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
        <input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="搜索 child / attempt / remote ID / error" style={{ minWidth: 260, padding: "6px 8px", border: "1px solid #cbd5e1", borderRadius: 6, fontSize: 11 }} />
        <select value={statusFilter} onChange={(event) => setStatusFilter(event.target.value)} style={{ padding: "6px 8px", border: "1px solid #cbd5e1", borderRadius: 6, fontSize: 11 }}><option value="all">全部状态</option>{Array.from(new Set(children.flatMap((child) => [text(child.status), ...(child.attempts || []).map((item) => text(item.status))]).filter(Boolean))).sort().map((item) => <option key={item}>{item}</option>)}</select>
        <select value={nodeFilter} onChange={(event) => setNodeFilter(event.target.value)} style={{ padding: "6px 8px", border: "1px solid #cbd5e1", borderRadius: 6, fontSize: 11 }}><option value="all">全部节点</option>{nodes.map((item) => <option key={item}>{item}</option>)}</select>
        <label style={{ fontSize: 11, color: "#475569", display: "inline-flex", alignItems: "center", gap: 4 }}><input type="checkbox" checked={errorOnly} onChange={(event) => setErrorOnly(event.target.checked)} />仅错误</label>
      </div>
      <div style={{ overflowX: "auto", border: "1px solid #e2e8f0", borderRadius: 8 }}>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 10 }}>
          <thead><tr style={{ background: "#f8fafc" }}><th style={{ width: 30 }} />{(["ordinal","child_key","child_kind","status","phase","updated_at"] as SortColumn[]).map((column) => <th key={column} onClick={() => toggleSort(column)} style={{ padding: 7, textAlign: "left", cursor: "pointer", color: "#475569", whiteSpace: "nowrap" }}>{column}{sort.column === column ? (sort.direction === "asc" ? " ▲" : " ▼") : ""}</th>)}<th style={{ padding: 7, textAlign: "left" }}>selected / disposition</th><th style={{ padding: 7, textAlign: "left" }}>error / artifact</th><th style={{ padding: 7 }}>动作</th></tr></thead>
          <tbody>{rows.length === 0 ? <tr><td colSpan={10} style={{ padding: 14, textAlign: "center", color: "#64748b" }}>无匹配 child；若上方有读取错误，该状态不代表暂无数据。</td></tr> : rows.map((child) => {
            const isExpanded = expanded.has(child.child_id);
            return <React.Fragment key={child.child_id}>
              <tr style={{ borderTop: "1px solid #e2e8f0" }}>
                <td style={{ padding: 5 }}><button onClick={() => setExpanded((current) => { const next = new Set(current); if (next.has(child.child_id)) next.delete(child.child_id); else next.add(child.child_id); return next; })} style={{ border: 0, background: "transparent", cursor: "pointer" }}>{isExpanded ? <ChevronDown size={13} /> : <ChevronRight size={13} />}</button></td>
                <td style={{ padding: 7 }}>{text(child.ordinal) || "-"}</td><td style={{ padding: 7, fontFamily: "monospace" }}><b>{child.child_key}</b><div style={{ color: "#64748b" }}>{child.child_id}</div></td><td style={{ padding: 7 }}>{child.child_kind || "-"}</td><td style={{ padding: 7 }}>{child.status || "-"}</td><td style={{ padding: 7 }}>{child.phase || "-"}</td><td style={{ padding: 7, whiteSpace: "nowrap" }}>{formatTime(child.updated_at)}</td><td style={{ padding: 7, fontFamily: "monospace" }}>{child.selected_attempt_id || "-"}<div>{child.execution_disposition || "-"}</div></td><td style={{ padding: 7, color: child.error_code ? "#b91c1c" : "#475569" }}>{child.error_code || "-"}<div>{manifestSummary(child.artifact_manifest_json)}</div></td><td style={{ padding: 7 }}><button onClick={() => onSelectRecoveryChild(child.child_id)} style={{ padding: "4px 6px", border: "1px solid #bfdbfe", borderRadius: 4, background: "#eff6ff", color: "#1d4ed8", fontSize: 10 }}>选择恢复</button></td>
              </tr>
              {isExpanded && <tr><td colSpan={10} style={{ padding: 10, background: "#f8fafc" }}>
                {(child.attempts || []).length === 0 ? <div style={{ color: "#64748b" }}>暂无 attempt 记录；该信息与读取失败分开显示。</div> : <div style={{ display: "grid", gap: 7 }}>{(child.attempts || []).map((attempt) => <div key={attempt.attempt_id} style={{ padding: 8, border: `1px solid ${attempt.selected ? "#60a5fa" : "#cbd5e1"}`, background: "#fff", borderRadius: 6 }}>
                  <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}><b style={{ fontFamily: "monospace" }}>{attempt.attempt_id}{attempt.selected ? " · selected" : ""}</b>{attempt.attempt_id && !TERMINAL.has(text(attempt.status)) && <button disabled={busy} onClick={() => void onCancelAttempt(attempt.attempt_id)} style={{ border: "1px solid #fca5a5", color: "#b91c1c", background: "#fef2f2", borderRadius: 4, fontSize: 10 }}><XCircle size={11} /> cancel attempt</button>}</div>
                  <div style={{ marginTop: 5, display: "grid", gridTemplateColumns: "repeat(4,minmax(0,1fr))", gap: 5, fontFamily: "monospace", color: "#475569" }}>
                    <span>no={text(attempt.attempt_no) || "-"}</span><span>status={attempt.status || "-"}</span><span>phase={attempt.phase || "-"}</span><span>mode={attempt.retry_mode || "-"}</span>
                    <span>kind={attempt.execution_kind || "-"}</span><span>node={attempt.node_id || "-"}</span><span>qe_task={attempt.qe_task_id || "-"}</span><span>qe_loop={text(attempt.qe_loop_id) || "-"}</span>
                    <span>heartbeat={formatTime(attempt.heartbeat_at)}</span><span>lease={formatTime(attempt.lease_expires_at)}</span><span>started={formatTime(attempt.started_at)}</span><span>finished={formatTime(attempt.finished_at)}</span>
                    <span>source={attempt.source_attempt_id || "-"}</span><span>retry_of={attempt.retry_of_attempt_id || "-"}</span><span>error={attempt.error_code || "-"}</span><span>artifact={manifestSummary(attempt.artifact_manifest_json)}</span>
                  </div>
                  <details style={{ marginTop: 6 }}><summary style={{ cursor: "pointer", color: "#1d4ed8" }}>完整身份、制品与错误 JSON</summary><pre style={{ maxHeight: 280, overflow: "auto", whiteSpace: "pre-wrap", wordBreak: "break-word", fontSize: 9 }}>{JSON.stringify(attempt, null, 2)}</pre></details>
                </div>)}</div>}
                <details style={{ marginTop: 8 }}><summary style={{ cursor: "pointer", color: "#1d4ed8" }}>完整 child JSON</summary><pre style={{ whiteSpace: "pre-wrap", wordBreak: "break-word", fontSize: 9 }}>{JSON.stringify({ ...child, attempts: undefined }, null, 2)}</pre></details>
              </td></tr>}
            </React.Fragment>;
          })}</tbody>
        </table>
      </div>
      <details><summary style={{ cursor: "pointer", fontSize: 11, fontWeight: 700, color: "#334155" }}>Durable DB events（{events.length}）</summary><div style={{ maxHeight: 280, overflow: "auto", display: "grid", gap: 4, marginTop: 6 }}>{events.slice().reverse().map((event) => <div key={event.event_id} style={{ padding: 6, border: "1px solid #e2e8f0", borderRadius: 5, fontSize: 9, fontFamily: "monospace" }}>#{event.event_id} · {event.event_type || "-"} · child={event.child_id || "-"} · attempt={event.attempt_id || "-"} · {formatTime(event.created_at)}{event.reason_code ? ` · reason=${event.reason_code}` : ""}</div>)}</div></details>
    </div>
  );
}
