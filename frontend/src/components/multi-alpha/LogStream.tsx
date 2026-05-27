"use client";

import React, {
  useState,
  useRef,
  useEffect,
  useCallback,
  useMemo,
} from "react";

const API = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

const MAX_LOG_LINES = 2000;
const RECONNECT_DELAY = 3000;
const MAX_RECONNECT = 20;

export interface LogEntry {
  nodeId: string;
  groupName: string;
  text: string;
  ts: number;
}

export interface LogStreamProps {
  experimentId: string;
  className?: string;
}

/** Extract [nodeId][groupName] prefix from a raw SSE line. */
function parseNodeTag(
  raw: string
): { nodeId: string; groupName: string; text: string } {
  const m = raw.match(/^\[([^\]]+)\]\[([^\]]+)\]\s*(.*)/);
  if (m) return { nodeId: m[1], groupName: m[2], text: m[3] };
  return { nodeId: "master", groupName: "", text: raw };
}

const NODE_COLORS: Record<string, string> = {
  master: "text-gray-400",
  wsl2: "text-emerald-400",
  rdagent: "text-sky-400",
};

function nodeColor(id: string): string {
  for (const [key, cls] of Object.entries(NODE_COLORS)) {
    if (id.startsWith(key) || id.includes(key)) return cls;
  }
  const hue =
    [...id].reduce((acc, c) => ((acc << 5) - acc + c.charCodeAt(0)) | 0, 0) %
    360;
  return `text-[hsl(${hue},70%,65%)]`;
}

export default function LogStream({
  experimentId,
  className,
}: LogStreamProps) {
  const [entries, setEntries] = useState<LogEntry[]>([]);
  const [nodeFilter, setNodeFilter] = useState<string>("all");
  const [paused, setPaused] = useState(false);
  const [connected, setConnected] = useState(false);
  const [autoScroll, setAutoScroll] = useState(true);
  const containerRef = useRef<HTMLDivElement>(null);
  const esRef = useRef<EventSource | null>(null);
  const abortRef = useRef(false);
  const reconnectRef = useRef(0);

  const appendEntry = useCallback((entry: LogEntry) => {
    setEntries((prev) => {
      const next = [...prev, entry];
      return next.length > MAX_LOG_LINES ? next.slice(-MAX_LOG_LINES) : next;
    });
  }, []);

  const discoveredNodes = useMemo(() => {
    const s = new Set<string>();
    entries.forEach((e) => s.add(e.nodeId));
    return [...s];
  }, [entries]);

  const filtered = useMemo(() => {
    if (nodeFilter === "all") return entries;
    return entries.filter((e) => e.nodeId === nodeFilter);
  }, [entries, nodeFilter]);

  // Auto-scroll
  useEffect(() => {
    if (autoScroll && !paused && containerRef.current) {
      containerRef.current.scrollTop = containerRef.current.scrollHeight;
    }
  }, [filtered, autoScroll, paused]);

  // Pause auto-scroll when user scrolls up
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const onScroll = () => {
      const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 40;
      setAutoScroll(atBottom);
    };
    el.addEventListener("scroll", onScroll, { passive: true });
    return () => el.removeEventListener("scroll", onScroll);
  }, []);

  // SSE connection
  useEffect(() => {
    abortRef.current = false;

    const connect = () => {
      if (abortRef.current) return;
      const url = `${API}/quantevolver/experiments/${experimentId}/multi-node-logs`;
      const es = new EventSource(url);
      esRef.current = es;

      es.onopen = () => {
        setConnected(true);
        reconnectRef.current = 0;
      };

      es.onmessage = (ev) => {
        if (!ev.data) return;
        const parsed = parseNodeTag(ev.data);
        appendEntry({
          nodeId: parsed.nodeId,
          groupName: parsed.groupName,
          text: parsed.text,
          ts: Date.now(),
        });
      };

      es.onerror = () => {
        es.close();
        if (esRef.current === es) esRef.current = null;
        setConnected(false);

        if (abortRef.current) return;
        reconnectRef.current += 1;
        if (reconnectRef.current <= MAX_RECONNECT) {
          setTimeout(connect, RECONNECT_DELAY);
        }
      };
    };

    connect();

    return () => {
      abortRef.current = true;
      esRef.current?.close();
      esRef.current = null;
    };
  }, [experimentId, appendEntry]);

  return (
    <div className={`flex flex-col ${className || ""}`}>
      {/* Toolbar */}
      <div className="flex items-center gap-3 border-b border-slate-700 px-3 py-2 text-xs">
        <span className="flex items-center gap-1">
          <span
            className={`inline-block h-2 w-2 rounded-full ${
              connected ? "bg-green-500" : "bg-red-500"
            }`}
          />
          {connected ? "已连接" : "未连接"}
        </span>

        <label className="flex items-center gap-1">
          <span className="text-gray-400">节点:</span>
          <select
            value={nodeFilter}
            onChange={(e) => setNodeFilter(e.target.value)}
            className="rounded border border-slate-600 bg-slate-800 px-2 py-0.5 text-xs"
          >
            <option value="all">全部 ({entries.length})</option>
            {discoveredNodes.map((n) => (
              <option key={n} value={n}>
                {n}
              </option>
            ))}
          </select>
        </label>

        <button
          onClick={() => setPaused((p) => !p)}
          className="rounded border border-slate-600 px-2 py-0.5 hover:bg-slate-700"
        >
          {paused ? "▶ 继续" : "⏸ 暂停"}
        </button>

        <span className="ml-auto text-gray-500">
          {filtered.length} 行
          {nodeFilter !== "all" && ` / ${entries.length}`}
        </span>
      </div>

      {/* Log body */}
      <div
        ref={containerRef}
        className="flex-1 overflow-auto bg-gray-950 p-2 font-mono text-xs leading-5"
        style={{ maxHeight: 480 }}
      >
        {filtered.length === 0 && (
          <div className="text-gray-600">等待日志...</div>
        )}
        {filtered.map((e, i) => (
          <div key={i} className="whitespace-pre-wrap break-all">
            <span className={`${nodeColor(e.nodeId)} mr-1`}>
              [{e.nodeId}]
            </span>
            {e.groupName && (
              <span className="mr-1 text-gray-500">[{e.groupName}]</span>
            )}
            <span className="text-gray-300">{e.text}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
