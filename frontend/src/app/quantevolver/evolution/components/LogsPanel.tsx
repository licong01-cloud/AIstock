"use client";

import React, { useEffect, useRef, useSyncExternalStore, useState } from "react";
import { Terminal } from "lucide-react";
import LogTerminal from "../../components/LogTerminal";
import { QEReadCache } from "../../components/read-cache";

export function createLogStore(initial: string[] = []) {
  let lines = initial.slice(-200);
  const listeners = new Set<() => void>();
  return {
    getSnapshot: () => lines,
    subscribe: (listener: () => void) => { listeners.add(listener); return () => { listeners.delete(listener); }; },
    append: (added: string[]) => {
      lines = [...lines, ...added].slice(-200);
      listeners.forEach(listener => listener());
    },
  };
}

interface LogsPanelProps {
  store: ReturnType<typeof createLogStore>;
  collapsed: boolean;
  onToggle: () => void;
}

const cardStyle: React.CSSProperties = {
  backgroundColor: "#0f172a",
  borderRadius: "12px",
  boxShadow: "0 4px 12px rgba(0, 0, 0, 0.05)",
  display: "flex",
  flexDirection: "column",
  overflow: "hidden",
  border: "1px solid rgba(255, 255, 255, 0.2)",
  flexShrink: 0,
};

export default React.memo(function LogsPanel({ store, collapsed, onToggle }: LogsPanelProps) {
  const logs = useSyncExternalStore(store.subscribe, store.getSnapshot, store.getSnapshot);
  const containerRef = useRef<HTMLDivElement>(null);
  const [locked, setLocked] = React.useState(false);

  useEffect(() => {
    if (locked) return;
    const el = containerRef.current;
    if (el) {
      el.scrollTop = el.scrollHeight;
    }
  }, [logs, locked]);

  return (
    <div style={cardStyle}>
      <div
        style={{ padding: "8px 16px", backgroundColor: "#020617", borderBottom: "1px solid #1e293b", display: "flex", alignItems: "center", justifyContent: "space-between", cursor: "pointer", userSelect: "none" }}
        onClick={onToggle}
      >
        <div style={{ display: "flex", alignItems: "center", gap: "8px", color: "#94a3b8", fontSize: "12px", fontFamily: "monospace" }}>
          <Terminal size={14} />
          Live Logs
          <span style={{ color: "#64748b", fontSize: "11px" }}>({logs.length} 行)</span>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
          <button
            onClick={e => { e.stopPropagation(); setLocked(v => !v); }}
            title={locked ? "解锁自动滚动" : "锁定滚动（可上翻查看历史）"}
            style={{
              background: locked ? "#1e40af" : "transparent",
              border: "1px solid " + (locked ? "#3b82f6" : "#334155"),
              borderRadius: "4px",
              color: locked ? "#93c5fd" : "#64748b",
              fontSize: "11px",
              padding: "2px 8px",
              cursor: "pointer",
              fontFamily: "monospace",
            }}
          >
            {locked ? "🔒 已锁定" : "🔓 跟随"}
          </button>
          <span style={{ color: "#94a3b8", fontSize: "12px" }}>{collapsed ? "\u25B6" : "\u25BC"}</span>
        </div>
      </div>
      {!collapsed && (
        <div
          ref={containerRef}
          style={{ height: "300px", overflowY: "auto", padding: "16px", fontFamily: "'Fira Code', Consolas, monospace", fontSize: "12px", color: "#4ade80", lineHeight: 1.6 }}
        >
          {logs.map((log, i) => (
            <div key={i}>{log}</div>
          ))}
          <div style={{ animation: "pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite", marginTop: "4px" }}>_</div>
        </div>
      )}
    </div>
  );
});

/** Operator-owned log reads, never a background database/HTTP poller. */
export const BackendLogPanel = React.memo(function BackendLogPanel() {
  const [open, setOpen] = useState(false);
  const [level, setLevel] = useState("");
  const [lines, setLines] = useState<string[]>([]);
  const [truncated, setTruncated] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const end = useRef<HTMLDivElement>(null);
  const reads = useRef(new QEReadCache());
  const load = React.useCallback(async () => {
    const url = `${process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1"}/quantevolver/evolution/system/logs?tail=150&level=${level}`;
    try {
      const result = await reads.current.read(url, url, false);
      if (!result.ok) throw new Error(result.message || "日志响应无效");
      setLines(result.lines || []); setError(null);
      setTruncated(result.truncated === true);
    } catch (reason) { if ((reason as Error).name !== "AbortError") setError(String(reason)); }
  }, [level]);
  useEffect(() => {
    if (open) void load();
    return () => reads.current.clear();
  }, [open, load]);
  return <div style={{ border: "1px solid #e2e8f0", borderRadius: 8, padding: 8 }}>
    <button onClick={() => setOpen(value => !value)}>后端日志（按需读取）{open ? " ▼" : " ▶"}</button>
    {open && <>
      <select value={level} onChange={event => setLevel(event.target.value)} aria-label="后端日志级别">
        {["", "ERROR", "WARN", "INFO"].map(value => <option key={value} value={value}>{value || "全部"}</option>)}
      </select>
      <button onClick={() => void load()}>刷新日志</button>
      {error && <div role="alert">{error}</div>}
      {truncated && <div>仅在日志末尾 1 MiB 内筛选；更早记录未加载。</div>}
      <LogTerminal logs={lines} logsEndRef={end} maxHeight={280} fontSize={11} />
    </>}
  </div>;
});
