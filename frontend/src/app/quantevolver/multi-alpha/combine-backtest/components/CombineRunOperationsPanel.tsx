"use client";

import React, { useCallback, useEffect, useMemo, useState } from "react";
import { Database, FileText, RefreshCw, RotateCcw, Trash2 } from "lucide-react";
import type { Loop } from "../../../evolution/components/TopologyPanel";

export type CombineRunLoop = Loop & {
  run_id?: string;
  raw_status?: string | null;
  phase?: string | null;
  progress?: Record<string, unknown>;
  reason?: Record<string, unknown>;
  heartbeat_at?: string | null;
  retryable?: boolean;
  deletable?: boolean;
  scheme_results?: Array<{
    weighting_scheme?: string | null;
    cagr?: number | null;
    sharpe?: number | null;
    max_drawdown?: number | null;
    calmar?: number | null;
    turnover?: number | null;
    pred_persisted?: boolean;
    skipped?: boolean;
    skipped_reason?: string | null;
  }>;
};

type RetryDraft = {
  run_id: string;
  retryable: boolean;
  exact: boolean;
  source: string;
  assumptions: string[];
  payload: Record<string, unknown>;
};

type RunLogs = {
  run_id: string;
  status: string;
  phase?: string | null;
  progress?: Record<string, unknown>;
  heartbeat_at?: string | null;
  reason?: Record<string, unknown>;
  history_available: boolean;
  events: Array<Record<string, unknown>>;
  files: Array<{ path: string; size: number; updated_at: string; tail: string }>;
};

type ArchiveStatus = {
  run_id: string;
  archive_status: "archived" | "not_archived" | string;
  archive_run?: Record<string, unknown> | null;
};

type Props = {
  apiBase: string;
  loop?: CombineRunLoop;
  onChanged: (newRunId?: string) => void | Promise<void>;
};

const cardStyle: React.CSSProperties = {
  backgroundColor: "#fff",
  border: "1px solid #e2e8f0",
  borderRadius: 8,
  padding: 16,
};

const buttonStyle: React.CSSProperties = {
  padding: "6px 12px",
  borderRadius: 6,
  border: "1px solid #cbd5e1",
  backgroundColor: "#fff",
  color: "#475569",
  fontSize: 12,
  fontWeight: 700,
  cursor: "pointer",
  display: "inline-flex",
  alignItems: "center",
  gap: 6,
};

function normalizeError(detail: unknown, fallback: string): string {
  if (typeof detail === "string") return detail;
  if (detail && typeof detail === "object") {
    const value = detail as Record<string, unknown>;
    const code = value.reason_code ? `reason_code=${String(value.reason_code)}` : "";
    const message = String(value.message || value.detail || fallback);
    return [code, message].filter(Boolean).join(": ");
  }
  return fallback;
}

async function apiRequest<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, init);
  const json = await response.json().catch(() => ({}));
  if (!response.ok || json.status !== "success") {
    throw new Error(normalizeError(json.detail || json.message, `HTTP ${response.status}`));
  }
  return json.data as T;
}

function formatTime(value?: string | null): string {
  if (!value) return "-";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString("zh-CN");
}

function formatPct(value?: number | null): string {
  return typeof value === "number" && Number.isFinite(value) ? `${(value * 100).toFixed(2)}%` : "-";
}

function formatNumber(value?: number | null): string {
  return typeof value === "number" && Number.isFinite(value) ? value.toFixed(3) : "-";
}

function runIdOf(loop?: CombineRunLoop): string {
  const value = loop?.run_id || loop?.config_json?.runtime_flags?.run_id;
  return String(value || "").trim();
}

function progressText(progress?: Record<string, unknown>): string {
  if (!progress || Object.keys(progress).length === 0) return "-";
  const completed = progress.completed;
  const total = progress.total;
  const pending = progress.pending;
  if (completed != null || total != null || pending != null) {
    return `completed=${completed ?? "-"} / total=${total ?? "-"} / pending=${pending ?? "-"}`;
  }
  return JSON.stringify(progress);
}

export default function CombineRunOperationsPanel({ apiBase, loop, onChanged }: Props) {
  const runId = runIdOf(loop);
  const [logs, setLogs] = useState<RunLogs | null>(null);
  const [logsError, setLogsError] = useState<string | null>(null);
  const [archiveStatus, setArchiveStatus] = useState<ArchiveStatus | null>(null);
  const [archiveError, setArchiveError] = useState<string | null>(null);
  const [retryDraft, setRetryDraft] = useState<RetryDraft | null>(null);
  const [retryPayloadText, setRetryPayloadText] = useState("");
  const [message, setMessage] = useState<{ ok: boolean; text: string } | null>(null);
  const [loading, setLoading] = useState(false);
  const [busy, setBusy] = useState<string | null>(null);

  const loadEvidence = useCallback(async () => {
    if (!runId) return;
    setLoading(true);
    const [logsResult, archiveResult] = await Promise.allSettled([
      apiRequest<RunLogs>(`${apiBase}/multi-alpha/combine-backtest/runs/${encodeURIComponent(runId)}/logs?tail_lines=300`),
      apiRequest<ArchiveStatus>(`${apiBase}/multi-alpha/combine-backtest/runs/${encodeURIComponent(runId)}/archive-status`),
    ]);
    if (logsResult.status === "fulfilled") {
      setLogs(logsResult.value);
      setLogsError(null);
    } else {
      setLogsError(logsResult.reason instanceof Error ? logsResult.reason.message : String(logsResult.reason));
    }
    if (archiveResult.status === "fulfilled") {
      setArchiveStatus(archiveResult.value);
      setArchiveError(null);
    } else {
      setArchiveStatus(null);
      setArchiveError(archiveResult.reason instanceof Error ? archiveResult.reason.message : String(archiveResult.reason));
    }
    setLoading(false);
  }, [apiBase, runId]);

  useEffect(() => {
    setLogs(null);
    setLogsError(null);
    setArchiveStatus(null);
    setArchiveError(null);
    setRetryDraft(null);
    setRetryPayloadText("");
    setMessage(null);
    void loadEvidence();
  }, [loadEvidence]);

  useEffect(() => {
    if (!runId || loop?.raw_status !== "running") return;
    const timer = window.setInterval(() => {
      if (document.visibilityState === "visible") void loadEvidence();
    }, 5000);
    const onVisibility = () => {
      if (document.visibilityState === "visible") void loadEvidence();
    };
    document.addEventListener("visibilitychange", onVisibility);
    return () => {
      window.clearInterval(timer);
      document.removeEventListener("visibilitychange", onVisibility);
    };
  }, [loadEvidence, loop?.raw_status, runId]);

  const openRetryDraft = useCallback(async () => {
    if (!runId) return;
    setBusy("retry-draft");
    setMessage(null);
    try {
      const draft = await apiRequest<RetryDraft>(`${apiBase}/multi-alpha/combine-backtest/runs/${encodeURIComponent(runId)}/retry-draft`);
      setRetryDraft(draft);
      setRetryPayloadText(JSON.stringify(draft.payload, null, 2));
    } catch (error) {
      setMessage({ ok: false, text: `重试配置读取失败: ${error instanceof Error ? error.message : String(error)}` });
    } finally {
      setBusy(null);
    }
  }, [apiBase, runId]);

  const submitRetry = useCallback(async () => {
    if (!runId || !retryDraft) return;
    setBusy("retry");
    setMessage(null);
    try {
      const originalText = JSON.stringify(retryDraft.payload, null, 2);
      const body = retryDraft.exact && retryPayloadText.trim() === originalText.trim()
        ? {}
        : { payload: JSON.parse(retryPayloadText) };
      const result = await apiRequest<{ run_id: string }>(
        `${apiBase}/multi-alpha/combine-backtest/runs/${encodeURIComponent(runId)}/retry`,
        { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) },
      );
      setMessage({ ok: true, text: `已创建重试任务: ${result.run_id}` });
      setRetryDraft(null);
      setRetryPayloadText("");
      await onChanged(result.run_id);
    } catch (error) {
      setMessage({ ok: false, text: `重试提交失败: ${error instanceof Error ? error.message : String(error)}` });
    } finally {
      setBusy(null);
    }
  }, [apiBase, onChanged, retryDraft, retryPayloadText, runId]);

  const deleteRun = useCallback(async () => {
    if (!runId) return;
    if (!window.confirm(`确认删除终态组合回测 ${runId}？\n将删除源结果行和该 run workspace；QE Archive 历史副本不删除。`)) return;
    setBusy("delete");
    setMessage(null);
    try {
      await apiRequest(`${apiBase}/multi-alpha/combine-backtest/runs/${encodeURIComponent(runId)}?cleanup_workspace=true`, { method: "DELETE" });
      setMessage({ ok: true, text: `已删除 ${runId}` });
      await onChanged();
    } catch (error) {
      setMessage({ ok: false, text: `删除失败: ${error instanceof Error ? error.message : String(error)}` });
    } finally {
      setBusy(null);
    }
  }, [apiBase, onChanged, runId]);

  const archiveRun = useCallback(async (dryRun: boolean) => {
    if (!runId) return;
    if (!dryRun && !window.confirm(`确认将 ${runId} 写入 QE Archive？源组合回测结果不会删除。`)) return;
    setBusy(dryRun ? "archive-preview" : "archive-write");
    setMessage(null);
    try {
      const report = await apiRequest<Record<string, unknown>>(
        `${apiBase}/multi-alpha/combine-backtest/runs/${encodeURIComponent(runId)}/archive`,
        { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ dry_run: dryRun }) },
      );
      setMessage({ ok: true, text: `${dryRun ? "入仓预览" : "入仓"}完成: ${JSON.stringify(report)}` });
      await loadEvidence();
    } catch (error) {
      setMessage({ ok: false, text: `${dryRun ? "入仓预览" : "入仓"}失败: ${error instanceof Error ? error.message : String(error)}` });
    } finally {
      setBusy(null);
    }
  }, [apiBase, loadEvidence, runId]);

  const displayedEvents = useMemo(() => (logs?.events || []).slice(-30).reverse(), [logs?.events]);
  const isTerminal = ["succeeded", "failed", "partial_failed"].includes(String(loop?.raw_status || ""));

  if (!loop || !runId) {
    return <div style={{ ...cardStyle, color: "#64748b", fontSize: 13 }}>请选择一个组合配置查看运行证据。</div>;
  }

  return (
    <div style={{ display: "grid", gap: 12 }}>
      {message && (
        <div style={{ ...cardStyle, backgroundColor: message.ok ? "#f0fdf4" : "#fef2f2", borderColor: message.ok ? "#86efac" : "#fca5a5", color: message.ok ? "#166534" : "#991b1b", fontSize: 12 }}>
          {message.text}
        </div>
      )}

      <div style={cardStyle}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 12 }}>
          <div>
            <div style={{ fontSize: 12, color: "#64748b", fontWeight: 700 }}>原始 combine run</div>
            <div style={{ marginTop: 4, fontFamily: "monospace", color: "#0f172a", fontWeight: 800 }}>{runId}</div>
          </div>
          <button onClick={() => void loadEvidence()} disabled={loading} style={{ ...buttonStyle, cursor: loading ? "wait" : "pointer" }}>
            <RefreshCw size={12} /> {loading ? "刷新中" : "刷新证据"}
          </button>
        </div>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 10, marginTop: 14 }}>
          {[
            ["状态", loop.raw_status || loop.status || "-"],
            ["阶段", loop.phase || logs?.phase || "-"],
            ["进度", progressText((loop.progress || logs?.progress) as Record<string, unknown>)],
            ["最后心跳", formatTime(loop.heartbeat_at || logs?.heartbeat_at)],
            ["数仓", archiveStatus?.archive_status || (archiveError ? "读取失败" : "读取中")],
            ["日志历史", logs?.history_available ? `${logs.events.length} events` : "历史事件文件不可用"],
          ].map(([label, value]) => (
            <div key={label} style={{ padding: 10, borderRadius: 6, backgroundColor: "#f8fafc", border: "1px solid #e2e8f0" }}>
              <div style={{ fontSize: 10, color: "#64748b", fontWeight: 700 }}>{label}</div>
              <div style={{ marginTop: 4, fontSize: 12, color: "#0f172a", fontFamily: "monospace", wordBreak: "break-word" }}>{value}</div>
            </div>
          ))}
        </div>
        {archiveError && <div style={{ marginTop: 8, fontSize: 11, color: "#b91c1c" }}>数仓状态读取失败: {archiveError}</div>}
        {logsError && <div style={{ marginTop: 8, fontSize: 11, color: "#b91c1c" }}>日志读取失败: {logsError}</div>}
      </div>

      <div style={cardStyle}>
        <div style={{ fontSize: 13, fontWeight: 800, color: "#0f172a", marginBottom: 10 }}>操作</div>
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
          <button onClick={() => void openRetryDraft()} disabled={!loop.retryable || busy != null} style={{ ...buttonStyle, opacity: loop.retryable ? 1 : 0.5, cursor: loop.retryable && !busy ? "pointer" : "not-allowed", color: "#1d4ed8", borderColor: "#bfdbfe", backgroundColor: "#eff6ff" }}>
            <RotateCcw size={12} /> {loop.raw_status === "succeeded" ? "按原配置再跑" : "重试配置"}
          </button>
          <button onClick={() => void archiveRun(true)} disabled={busy != null || !isTerminal} style={{ ...buttonStyle, color: "#7c3aed", borderColor: "#ddd6fe", backgroundColor: "#f5f3ff", opacity: isTerminal ? 1 : 0.5, cursor: isTerminal && !busy ? "pointer" : "not-allowed" }}>
            <Database size={12} /> 入仓预览
          </button>
          <button onClick={() => void archiveRun(false)} disabled={busy != null || !isTerminal || archiveStatus?.archive_status === "archived"} style={{ ...buttonStyle, color: "#047857", borderColor: "#86efac", backgroundColor: "#ecfdf5", opacity: isTerminal && archiveStatus?.archive_status !== "archived" ? 1 : 0.5, cursor: isTerminal && !busy && archiveStatus?.archive_status !== "archived" ? "pointer" : "not-allowed" }}>
            <Database size={12} /> {archiveStatus?.archive_status === "archived" ? "已入仓" : "写入数仓"}
          </button>
          <button onClick={() => void deleteRun()} disabled={!loop.deletable || busy != null} style={{ ...buttonStyle, color: "#b91c1c", borderColor: "#fca5a5", backgroundColor: "#fef2f2", opacity: loop.deletable ? 1 : 0.5, cursor: loop.deletable && !busy ? "pointer" : "not-allowed" }}>
            <Trash2 size={12} /> 删除终态 run
          </button>
        </div>
      </div>

      {retryDraft && (
        <div style={{ ...cardStyle, borderColor: retryDraft.exact ? "#86efac" : "#fbbf24" }}>
          <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "center" }}>
            <div>
              <div style={{ fontSize: 13, fontWeight: 800, color: "#0f172a" }}>重试配置</div>
              <div style={{ marginTop: 3, fontSize: 11, color: retryDraft.exact ? "#047857" : "#92400e" }}>
                {retryDraft.exact ? "完整冻结快照，可按原配置重放。" : "历史 run 无完整快照；下列假设已显式列出，可编辑 JSON 后提交。"}
              </div>
            </div>
            <button onClick={() => { setRetryDraft(null); setRetryPayloadText(""); }} style={buttonStyle}>关闭</button>
          </div>
          {retryDraft.assumptions.length > 0 && (
            <ul style={{ margin: "10px 0", paddingLeft: 20, color: "#92400e", fontSize: 11, lineHeight: 1.6 }}>
              {retryDraft.assumptions.map((item) => <li key={item}>{item}</li>)}
            </ul>
          )}
          <textarea
            value={retryPayloadText}
            onChange={(event) => setRetryPayloadText(event.target.value)}
            spellCheck={false}
            style={{ width: "100%", minHeight: 260, boxSizing: "border-box", padding: 10, border: "1px solid #cbd5e1", borderRadius: 6, fontFamily: "monospace", fontSize: 11, color: "#0f172a", backgroundColor: "#f8fafc" }}
          />
          <div style={{ marginTop: 10, display: "flex", justifyContent: "flex-end" }}>
            <button onClick={() => void submitRetry()} disabled={busy != null} style={{ ...buttonStyle, color: "#1d4ed8", borderColor: "#93c5fd", backgroundColor: "#eff6ff" }}>
              <RotateCcw size={12} /> {busy === "retry" ? "提交中..." : "创建新 run"}
            </button>
          </div>
        </div>
      )}

      <div style={cardStyle}>
        <div style={{ fontSize: 13, fontWeight: 800, color: "#0f172a", marginBottom: 10 }}>Scheme 结果与资产</div>
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
            <thead><tr style={{ backgroundColor: "#f8fafc" }}>
              {["Scheme", "状态", "CAGR", "Sharpe", "MaxDD", "Calmar", "Prediction", "原因"].map((label) => <th key={label} style={{ padding: 8, textAlign: "left", color: "#475569", borderBottom: "1px solid #e2e8f0" }}>{label}</th>)}
            </tr></thead>
            <tbody>
              {(loop.scheme_results || []).length === 0 ? (
                <tr><td colSpan={8} style={{ padding: 10, color: "#64748b" }}>尚无持久化 scheme 结果。</td></tr>
              ) : (loop.scheme_results || []).map((row) => (
                <tr key={String(row.weighting_scheme)} style={{ borderBottom: "1px solid #f1f5f9" }}>
                  <td style={{ padding: 8, fontFamily: "monospace", fontWeight: 700 }}>{row.weighting_scheme || "-"}</td>
                  <td style={{ padding: 8, color: row.skipped ? "#b91c1c" : "#047857" }}>{row.skipped ? "不可计算" : "已计算"}</td>
                  <td style={{ padding: 8 }}>{formatPct(row.cagr)}</td>
                  <td style={{ padding: 8 }}>{formatNumber(row.sharpe)}</td>
                  <td style={{ padding: 8 }}>{formatPct(row.max_drawdown)}</td>
                  <td style={{ padding: 8 }}>{formatNumber(row.calmar)}</td>
                  <td style={{ padding: 8 }}>{row.pred_persisted ? "已持久化" : "-"}</td>
                  <td style={{ padding: 8, color: row.skipped_reason ? "#b91c1c" : "#64748b", maxWidth: 320, wordBreak: "break-word" }}>{row.skipped_reason || "-"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div style={cardStyle}>
        <div style={{ fontSize: 13, fontWeight: 800, color: "#0f172a", marginBottom: 10 }}>运行原因</div>
        <pre style={{ margin: 0, padding: 10, borderRadius: 6, backgroundColor: "#0f172a", color: "#e2e8f0", fontSize: 11, overflowX: "auto", whiteSpace: "pre-wrap", wordBreak: "break-word" }}>
          {JSON.stringify(loop.reason || logs?.reason || {}, null, 2)}
        </pre>
      </div>

      <div style={cardStyle}>
        <div style={{ fontSize: 13, fontWeight: 800, color: "#0f172a", marginBottom: 10 }}>运行事件</div>
        {displayedEvents.length === 0 ? (
          <div style={{ color: "#64748b", fontSize: 12 }}>{logs?.history_available === false ? "该历史 run 在事件持久化功能上线前创建；当前仅有 DB 状态快照。" : "暂无事件。"}</div>
        ) : (
          <div style={{ display: "grid", gap: 6 }}>
            {displayedEvents.map((event, index) => (
              <div key={`${String(event.recorded_at || event.heartbeat_at || index)}-${index}`} style={{ padding: 8, border: "1px solid #e2e8f0", borderRadius: 6, backgroundColor: "#f8fafc", fontSize: 11 }}>
                <div style={{ display: "flex", justifyContent: "space-between", gap: 10, color: "#475569" }}>
                  <strong style={{ color: "#0f172a" }}>{String(event.phase || event.event || "event")}</strong>
                  <span>{formatTime(String(event.recorded_at || event.heartbeat_at || ""))}</span>
                </div>
                <div style={{ marginTop: 4, color: "#64748b", whiteSpace: "pre-wrap", wordBreak: "break-word" }}>{String(event.message || JSON.stringify(event.progress || event.reason || event))}</div>
              </div>
            ))}
          </div>
        )}
      </div>

      <div style={cardStyle}>
        <div style={{ fontSize: 13, fontWeight: 800, color: "#0f172a", marginBottom: 10, display: "flex", alignItems: "center", gap: 6 }}><FileText size={14} /> Workspace 文本日志</div>
        {(logs?.files || []).length === 0 ? (
          <div style={{ color: "#64748b", fontSize: 12 }}>本机 workspace 中没有可读取的文本日志；远端缺失日志不会被伪造成空成功。</div>
        ) : (logs?.files || []).map((file) => (
          <details key={file.path} style={{ marginBottom: 8, border: "1px solid #e2e8f0", borderRadius: 6, backgroundColor: "#f8fafc" }}>
            <summary style={{ padding: 8, cursor: "pointer", fontSize: 11, color: "#0f172a", fontFamily: "monospace" }}>
              {file.path} · {file.size} bytes · {formatTime(file.updated_at)}
            </summary>
            <pre style={{ margin: 0, padding: 10, borderTop: "1px solid #e2e8f0", backgroundColor: "#0f172a", color: "#e2e8f0", fontSize: 10, overflowX: "auto", whiteSpace: "pre-wrap", wordBreak: "break-word", maxHeight: 360 }}>{file.tail}</pre>
          </details>
        ))}
      </div>
    </div>
  );
}
