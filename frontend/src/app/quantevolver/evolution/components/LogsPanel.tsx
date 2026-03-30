"use client";

import React, { useEffect, useRef } from "react";
import { Terminal } from "lucide-react";

interface LogsPanelProps {
  logs: string[];
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

export default React.memo(function LogsPanel({ logs, collapsed, onToggle }: LogsPanelProps) {
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
