"use client";

import React from "react";

interface LogTerminalProps {
  logs: string[];
  logsEndRef: React.RefObject<HTMLDivElement>;
  /** 最大高度（px），默认 300 */
  maxHeight?: number;
  /** 字体大小（px），默认 12 */
  fontSize?: number;
  /** 状态文本（显示在终端上方），不传则不显示 */
  statusLabel?: string;
  /** 状态颜色圆点色值 */
  statusColor?: string;
  /** 状态是否 pulse 动画 */
  statusPulse?: boolean;
  /** 副标题（如 experiment_id） */
  statusSub?: string;
  /** 关闭按钮回调，不传则不显示关闭按钮 */
  onClose?: () => void;
}

function getLineColor(line: string): string {
  if (line.startsWith("[Error]") || line.startsWith("[ERROR]")) return "#f87171";
  if (line.startsWith("[System]") || line.startsWith("[INFO]")) return "#4ade80";
  if (line.startsWith("[WARN]")) return "#fbbf24";
  return "#cbd5e1";
}

export default function LogTerminal({
  logs,
  logsEndRef,
  maxHeight = 300,
  fontSize = 12,
  statusLabel,
  statusColor,
  statusPulse = false,
  statusSub,
  onClose,
}: LogTerminalProps) {
  return (
    <div>
      {/* 状态栏 + 关闭按钮 */}
      {(statusLabel || onClose) && (
        <div style={{
          display: "flex", alignItems: "center", justifyContent: "space-between",
          marginBottom: 6,
        }}>
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            {statusColor && (
              <div style={{
                width: 8, height: 8, borderRadius: "50%",
                backgroundColor: statusColor,
                animation: statusPulse ? "pulse 1.5s infinite" : "none",
              }} />
            )}
            {statusLabel && (
              <span style={{ fontSize: 13, fontWeight: 600, color: "#1e293b" }}>
                {statusLabel}
              </span>
            )}
            {statusSub && (
              <span style={{ fontSize: 11, color: "#64748b" }}>({statusSub})</span>
            )}
          </div>
          {onClose && (
            <button onClick={onClose}
              style={{
                padding: "2px 8px", fontSize: 10, border: "1px solid #d1d5db",
                borderRadius: 4, background: "#fff", cursor: "pointer",
              }}>
              关闭日志
            </button>
          )}
        </div>
      )}

      {/* 终端主体 */}
      <div style={{
        backgroundColor: "#0f172a",
        borderRadius: 8,
        padding: fontSize >= 12 ? 16 : 10,
        maxHeight,
        overflowY: "auto",
        fontFamily: "'Fira Code', Consolas, monospace",
        fontSize,
        lineHeight: 1.6,
        color: "#e2e8f0",
        border: "1px solid #334155",
      }}>
        {logs.map((line, idx) => (
          <div key={idx} style={{ color: getLineColor(line) }}>
            {line}
          </div>
        ))}
        <div ref={logsEndRef} />
      </div>
    </div>
  );
}
