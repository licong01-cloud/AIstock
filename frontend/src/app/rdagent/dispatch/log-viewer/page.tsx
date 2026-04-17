"use client";

import React, { Suspense, useCallback, useEffect, useRef, useState } from "react";
import { useSearchParams } from "next/navigation";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

/* ───── ANSI SGR → CSS 颜色映射 ───── */
const ANSI_COLORS: Record<number, string> = {
  30: "#4b5563", 31: "#ef4444", 32: "#22c55e", 33: "#eab308",
  34: "#3b82f6", 35: "#a855f7", 36: "#06b6d4", 37: "#d1d5db",
  90: "#6b7280", 91: "#f87171", 92: "#4ade80", 93: "#facc15",
  94: "#60a5fa", 95: "#c084fc", 96: "#22d3ee", 97: "#f3f4f6",
};
const ANSI_BG_COLORS: Record<number, string> = {
  40: "#4b5563", 41: "#991b1b", 42: "#166534", 43: "#854d0e",
  44: "#1e40af", 45: "#7e22ce", 46: "#0e7490", 47: "#d1d5db",
  100: "#6b7280", 101: "#dc2626", 102: "#16a34a", 103: "#ca8a04",
  104: "#2563eb", 105: "#9333ea", 106: "#0891b2", 107: "#f3f4f6",
};

interface SpanStyle {
  color?: string;
  backgroundColor?: string;
  fontWeight?: string;
  fontStyle?: string;
  textDecoration?: string;
  opacity?: number;
}

/** 将含 ANSI 转义码的文本转换为带颜色的 React 元素 */
function ansiToSpans(text: string): React.ReactNode[] {
  const nodes: React.ReactNode[] = [];
  // 匹配 ESC[ ... m 序列（支持 \x1b 和 \033）
  const regex = /\x1b\[([0-9;]*)m/g;
  let lastIndex = 0;
  let style: SpanStyle = {};
  let key = 0;

  let match: RegExpExecArray | null;
  while ((match = regex.exec(text)) !== null) {
    // 添加转义码前的普通文本
    if (match.index > lastIndex) {
      const segment = text.slice(lastIndex, match.index);
      nodes.push(<span key={key++} style={{ ...style }}>{segment}</span>);
    }
    lastIndex = regex.lastIndex;

    // 解析 SGR 参数
    const params = match[1] ? match[1].split(";").map(Number) : [0];
    for (const code of params) {
      if (code === 0) {
        style = {};
      } else if (code === 1) {
        style = { ...style, fontWeight: "bold" };
      } else if (code === 2) {
        style = { ...style, opacity: 0.7 };
      } else if (code === 3) {
        style = { ...style, fontStyle: "italic" };
      } else if (code === 4) {
        style = { ...style, textDecoration: "underline" };
      } else if (code === 22) {
        style = { ...style, fontWeight: undefined, opacity: undefined };
      } else if (code === 23) {
        style = { ...style, fontStyle: undefined };
      } else if (code === 24) {
        style = { ...style, textDecoration: undefined };
      } else if (code >= 30 && code <= 37 || code >= 90 && code <= 97) {
        style = { ...style, color: ANSI_COLORS[code] };
      } else if (code === 39) {
        style = { ...style, color: undefined };
      } else if (code >= 40 && code <= 47 || code >= 100 && code <= 107) {
        style = { ...style, backgroundColor: ANSI_BG_COLORS[code] };
      } else if (code === 49) {
        style = { ...style, backgroundColor: undefined };
      }
    }
  }

  // 添加剩余文本
  if (lastIndex < text.length) {
    const segment = text.slice(lastIndex);
    if (Object.keys(style).length > 0) {
      nodes.push(<span key={key++} style={{ ...style }}>{segment}</span>);
    } else {
      nodes.push(<span key={key++}>{segment}</span>);
    }
  }

  return nodes.length > 0 ? nodes : [<span key={0}>{text}</span>];
}

export default function LogViewerPage() {
  return (
    <Suspense fallback={<div style={{ padding: 24, color: "#9ca3af" }}>加载中...</div>}>
      <LogViewerContent />
    </Suspense>
  );
}

function LogViewerContent() {
  const searchParams = useSearchParams();
  const taskId = searchParams.get("task_id") || "";

  const [lines, setLines] = useState<string[]>([]);
  const [taskInfo, setTaskInfo] = useState<Record<string, any> | null>(null);
  const [autoScroll, setAutoScroll] = useState(true);
  const [searchText, setSearchText] = useState("");
  const containerRef = useRef<HTMLDivElement>(null);
  const abortRef = useRef<AbortController | null>(null);

  const [taskError, setTaskError] = useState<string | null>(null);

  // 加载任务信息
  useEffect(() => {
    if (!taskId) return;
    fetch(`${API_BASE}/dispatch/tasks/${taskId}`)
      .then(r => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then(setTaskInfo)
      .catch(e => setTaskError(`加载任务信息失败: ${e.message}`));
  }, [taskId]);

  // SSE 日志流 or 历史日志
  useEffect(() => {
    if (!taskId) return;

    const isRunning = taskInfo?.status === "running" || taskInfo?.status === "paused";

    if (isRunning) {
      // SSE 实时流
      const controller = new AbortController();
      abortRef.current = controller;

      (async () => {
        try {
          const res = await fetch(`${API_BASE}/dispatch/tasks/${taskId}/logs/stream`, {
            signal: controller.signal,
          });
          if (!res.ok) {
            setLines(prev => [...prev, `[SSE 连接失败: HTTP ${res.status}]`]);
            return;
          }
          if (!res.body) {
            setLines(prev => [...prev, "[SSE 连接失败: 响应无 body]"]);
            return;
          }
          const reader = res.body.getReader();
          const decoder = new TextDecoder();
          let buffer = "";

          while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            buffer += decoder.decode(value, { stream: true });
            const parts = buffer.split("\n\n");
            buffer = parts.pop() || "";

            for (const part of parts) {
              for (const rawLine of part.split("\n")) {
                if (rawLine.startsWith("data: ")) {
                  try {
                    const d = JSON.parse(rawLine.slice(6));
                    if (d.line !== undefined) {
                      setLines(prev => [...prev, d.line]);
                    }
                    if (d.event === "task_ended") {
                      setLines(prev => [...prev, `\n--- 任务已结束: ${d.status} ---`]);
                    }
                  } catch (parseErr) {
                    // SSE data 非 JSON（协议层异常），显示原始内容
                    setLines(prev => [...prev, `[SSE 解析错误] ${rawLine}`]);
                  }
                }
              }
            }
          }
        } catch (e: any) {
          if (e.name !== "AbortError") {
            setLines(prev => [...prev, `\n[连接断开: ${e.message}]`]);
          }
        }
      })();

      return () => controller.abort();
    } else {
      // 加载完整历史日志
      fetch(`${API_BASE}/dispatch/tasks/${taskId}/logs/full?limit=10000`)
        .then(r => {
          if (!r.ok) throw new Error(`HTTP ${r.status}`);
          return r.json();
        })
        .then(d => setLines(d.lines || []))
        .catch(e => setLines([`[加载日志失败: ${e.message}]`]));
    }
  }, [taskId, taskInfo?.status]);

  // 自动滚动
  useEffect(() => {
    if (autoScroll && containerRef.current) {
      containerRef.current.scrollTop = containerRef.current.scrollHeight;
    }
  }, [lines, autoScroll]);

  const handleDownload = () => {
    const blob = new Blob([lines.join("\n")], { type: "text/plain" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `dispatch_${taskId}.log`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const elapsed = taskInfo?.started_at
    ? (() => {
        const start = new Date(taskInfo.started_at).getTime();
        const end = taskInfo.finished_at ? new Date(taskInfo.finished_at).getTime() : Date.now();
        const mins = Math.floor((end - start) / 60000);
        return mins >= 60 ? `${Math.floor(mins / 60)}h${mins % 60}m` : `${mins}m`;
      })()
    : "-";

  return (
    <div style={{
      background: "#0f172a", color: "#e2e8f0", minHeight: "100vh",
      display: "flex", flexDirection: "column", fontFamily: "Consolas, monospace",
    }}>
      {/* 任务信息加载错误 */}
      {taskError && (
        <div style={{ padding: "6px 16px", background: "#7f1d1d", color: "#fca5a5", fontSize: 12 }}>
          {taskError}
        </div>
      )}

      {/* 状态栏 */}
      <div style={{
        display: "flex", alignItems: "center", gap: 16, padding: "8px 16px",
        background: "#1e293b", borderBottom: "1px solid #334155", fontSize: 12,
        flexShrink: 0,
      }}>
        <span style={{ fontWeight: 600 }}>
          {taskInfo?.task_name || taskId}
        </span>
        <span style={{ color: "#94a3b8" }}>
          {taskInfo?.node_id || "-"}
        </span>
        <span style={{
          padding: "1px 8px", borderRadius: 4,
          background: taskInfo?.status === "running" ? "#166534" : "#374151",
          color: taskInfo?.status === "running" ? "#86efac" : "#9ca3af",
        }}>
          {taskInfo?.status || "loading"}
        </span>
        <span style={{ color: "#94a3b8" }}>{elapsed}</span>
        <span style={{ color: "#64748b" }}>{lines.length} 行</span>
      </div>

      {/* 日志内容 */}
      <div
        ref={containerRef}
        style={{
          flex: 1, overflow: "auto", padding: "8px 16px",
          fontSize: 12, lineHeight: 1.7,
        }}
      >
        {(searchText
          ? lines
              .map((line, idx) => ({ line, origIdx: idx }))
              .filter(({ line }) => line.toLowerCase().includes(searchText.toLowerCase()))
          : lines.map((line, idx) => ({ line, origIdx: idx }))
        ).map(({ line, origIdx }) => (
          <div key={origIdx} style={{ whiteSpace: "pre-wrap", wordBreak: "break-all" }}>
            <span style={{ color: "#475569", marginRight: 8, userSelect: "none" }}>
              {String(origIdx + 1).padStart(4)}
            </span>
            {ansiToSpans(line)}
          </div>
        ))}
        {lines.length === 0 && (
          <div style={{ color: "#475569", textAlign: "center", marginTop: 40 }}>
            等待日志...
          </div>
        )}
      </div>

      {/* 底部工具栏 */}
      <div style={{
        display: "flex", alignItems: "center", gap: 8, padding: "6px 16px",
        background: "#1e293b", borderTop: "1px solid #334155", flexShrink: 0,
      }}>
        <label style={{ display: "flex", alignItems: "center", gap: 4, fontSize: 12, cursor: "pointer" }}>
          <input
            type="checkbox"
            checked={autoScroll}
            onChange={e => setAutoScroll(e.target.checked)}
          />
          自动滚动
        </label>
        <input
          style={{
            flex: 1, maxWidth: 300, padding: "4px 8px", fontSize: 12,
            background: "#0f172a", border: "1px solid #334155", borderRadius: 4,
            color: "#e2e8f0", outline: "none",
          }}
          placeholder="搜索日志..."
          value={searchText}
          onChange={e => setSearchText(e.target.value)}
        />
        <button
          onClick={handleDownload}
          style={{
            padding: "4px 12px", fontSize: 12, borderRadius: 4,
            border: "1px solid #475569", background: "transparent",
            color: "#94a3b8", cursor: "pointer",
          }}
        >
          下载
        </button>
        <button
          onClick={() => setLines([])}
          style={{
            padding: "4px 12px", fontSize: 12, borderRadius: 4,
            border: "1px solid #475569", background: "transparent",
            color: "#94a3b8", cursor: "pointer",
          }}
        >
          清屏
        </button>
      </div>
    </div>
  );
}
