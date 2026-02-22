"use client";

import React, { useEffect, useRef, useState, useCallback } from "react";

const API = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

type StepState = "pending" | "active" | "done" | "failed" | "skipped";

type SampleRow = Record<string, string>;

type Step = {
  key: string;
  label: string;
  state: StepState;
  logs: string[];
  error: string | null;
  success: boolean | null;
  result_sample?: SampleRow[] | null;
  original_sample?: SampleRow[] | null;
  original_error?: string | null;
};

type JobProgress = {
  ok: boolean;
  job_id: string;
  factor_name: string;
  factor_source: string;
  status: string;
  error_message: string | null;
  llm_retry_count: number;
  max_llm_retries: number;
  created_at: string | null;
  started_at: string | null;
  completed_at: string | null;
  updated_at: string | null;
  steps: Step[];
};

type Props = {
  jobId: string;
  factorName: string;
  onClose: () => void;
};

const STEP_ICON: Record<StepState, string> = {
  pending: "○",
  active: "◉",
  done: "✓",
  failed: "✗",
  skipped: "—",
};

const STEP_COLOR: Record<StepState, { dot: string; label: string; bg: string; border: string }> = {
  pending:  { dot: "text-gray-300",   label: "text-gray-400",  bg: "bg-gray-50",    border: "border-gray-200" },
  active:   { dot: "text-blue-500",   label: "text-blue-700",  bg: "bg-blue-50",    border: "border-blue-300" },
  done:     { dot: "text-green-500",  label: "text-green-700", bg: "bg-green-50",   border: "border-green-300" },
  failed:   { dot: "text-red-500",    label: "text-red-700",   bg: "bg-red-50",     border: "border-red-300" },
  skipped:  { dot: "text-gray-300",   label: "text-gray-400",  bg: "bg-gray-50",    border: "border-gray-200" },
};

function fmtTime(ts: string | null): string {
  if (!ts) return "-";
  return new Date(ts).toLocaleString("zh-CN", { timeZone: "Asia/Shanghai" });
}

export function ProgressPanel({ jobId, factorName, onClose }: Props) {
  const [progress, setProgress] = useState<JobProgress | null>(null);
  const [error, setError] = useState<string | null>(null);
  const logRef = useRef<HTMLDivElement>(null);
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const isFinished = progress?.status === "SUCCESS" || progress?.status === "FAILED";

  const fetchProgress = useCallback(async () => {
    try {
      const r = await fetch(`${API}/quantevolver/factor-transformation/jobs/${jobId}/progress`);
      const d = await r.json();
      if (d.ok) {
        setProgress(d);
        setError(null);
      } else {
        setError(d.detail || "获取进度失败");
      }
    } catch (e) {
      setError(`网络错误: ${e}`);
    }
  }, [jobId]);

  useEffect(() => {
    fetchProgress();
    timerRef.current = setInterval(() => {
      if (!isFinished) fetchProgress();
    }, 2000);
    return () => {
      if (timerRef.current) clearInterval(timerRef.current);
    };
  }, [fetchProgress, isFinished]);

  // 自动滚动日志到底部
  useEffect(() => {
    if (logRef.current) {
      logRef.current.scrollTop = logRef.current.scrollHeight;
    }
  }, [progress]);

  // 停止轮询
  useEffect(() => {
    if (isFinished && timerRef.current) {
      clearInterval(timerRef.current);
      timerRef.current = null;
    }
  }, [isFinished]);

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center"
      style={{ backgroundColor: "rgba(0,0,0,0.6)" }}
    >
      <div
        className="rounded-2xl shadow-2xl flex flex-col"
        style={{ width: "min(92vw, 900px)", maxHeight: "88vh", background: "#ffffff" }}
      >
        {/* 标题栏 */}
        <div
          style={{
            background: "linear-gradient(135deg, #7c3aed 0%, #2563eb 100%)",
            borderRadius: "16px 16px 0 0",
            padding: "16px 24px",
            color: "#fff",
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            flexShrink: 0,
          }}
        >
          <div>
            <div style={{ fontWeight: 700, fontSize: 16 }}>因子改造进度</div>
            <div style={{ fontSize: 12, opacity: 0.85, marginTop: 2, fontFamily: "monospace" }}>
              {factorName}
            </div>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
            {!isFinished && (
              <span style={{ fontSize: 12, opacity: 0.8, display: "flex", alignItems: "center", gap: 4 }}>
                <span style={{ display: "inline-block", width: 8, height: 8, borderRadius: "50%", background: "#4ade80", animation: "pulse 1.5s infinite" }} />
                改造中…
              </span>
            )}
            {progress?.status === "SUCCESS" && (
              <span style={{ fontSize: 12, background: "rgba(74,222,128,0.2)", padding: "2px 10px", borderRadius: 20, color: "#4ade80", fontWeight: 600 }}>
                ✓ 改造成功
              </span>
            )}
            {progress?.status === "FAILED" && (
              <span style={{ fontSize: 12, background: "rgba(248,113,113,0.2)", padding: "2px 10px", borderRadius: 20, color: "#f87171", fontWeight: 600 }}>
                ✗ 改造失败
              </span>
            )}
            <button
              onClick={onClose}
              style={{
                background: "rgba(255,255,255,0.15)", border: "none", color: "#fff",
                borderRadius: 8, padding: "4px 12px", cursor: "pointer", fontSize: 14, fontWeight: 600,
              }}
            >
              关闭
            </button>
          </div>
        </div>

        {/* 内容区 */}
        <div style={{ flex: 1, overflow: "hidden", display: "flex", flexDirection: "column", padding: 20, gap: 16 }}>
          {error && (
            <div style={{ background: "#fef2f2", border: "1px solid #fca5a5", borderRadius: 8, padding: "10px 14px", color: "#dc2626", fontSize: 13 }}>
              {error}
            </div>
          )}

          {/* 时间信息 */}
          {progress && (
            <div style={{ display: "flex", gap: 20, fontSize: 12, color: "#6b7280" }}>
              <span>创建: {fmtTime(progress.created_at)}</span>
              <span>开始: {fmtTime(progress.started_at)}</span>
              {progress.completed_at && <span>完成: {fmtTime(progress.completed_at)}</span>}
            </div>
          )}

          {/* 流程图 */}
          {progress && (
            <div style={{ display: "flex", alignItems: "center", gap: 0, flexWrap: "nowrap", overflowX: "auto", paddingBottom: 4 }}>
              {progress.steps.map((step, idx) => {
                const col = STEP_COLOR[step.state] || STEP_COLOR.pending;
                return (
                  <React.Fragment key={step.key}>
                    <div
                      style={{
                        display: "flex", flexDirection: "column", alignItems: "center",
                        minWidth: 90, padding: "8px 6px",
                        background: step.state === "active" ? "#eff6ff" : step.state === "done" ? "#f0fdf4" : step.state === "failed" ? "#fef2f2" : "#f9fafb",
                        borderRadius: 10,
                        border: `1.5px solid ${step.state === "active" ? "#93c5fd" : step.state === "done" ? "#86efac" : step.state === "failed" ? "#fca5a5" : "#e5e7eb"}`,
                      }}
                    >
                      <span style={{ fontSize: 18, lineHeight: 1 }} className={col.dot}>
                        {STEP_ICON[step.state]}
                      </span>
                      <span style={{ fontSize: 11, marginTop: 4, textAlign: "center", fontWeight: step.state === "active" ? 700 : 400, color: step.state === "active" ? "#1d4ed8" : step.state === "done" ? "#15803d" : step.state === "failed" ? "#dc2626" : "#9ca3af" }}>
                        {step.label}
                      </span>
                    </div>
                    {idx < progress.steps.length - 1 && (
                      <div style={{ width: 20, height: 2, background: "#e5e7eb", flexShrink: 0 }} />
                    )}
                  </React.Fragment>
                );
              })}
            </div>
          )}

          {/* 步骤日志 */}
          <div style={{ flex: 1, overflow: "hidden", display: "flex", flexDirection: "column", gap: 8 }}>
            <div style={{ fontSize: 13, fontWeight: 600, color: "#374151" }}>改造日志</div>
            <div
              ref={logRef}
              style={{
                flex: 1, overflowY: "auto", background: "#0f172a", borderRadius: 10,
                padding: "12px 14px", fontFamily: "monospace", fontSize: 12,
                color: "#e2e8f0", minHeight: 200, maxHeight: 380,
              }}
            >
              {!progress && (
                <div style={{ color: "#64748b" }}>正在加载进度...</div>
              )}
              {progress && progress.steps.map((step) => {
                if (step.state === "pending" || step.state === "skipped") return null;
                const hasContent = step.logs.length > 0 || step.error || step.result_sample || step.original_sample;
                if (!hasContent) return null;
                const isExecStep = step.key === "EXECUTION_TESTING";
                const resultCols = step.result_sample && step.result_sample.length > 0 ? Object.keys(step.result_sample[0]) : [];
                const origCols = step.original_sample && step.original_sample.length > 0 ? Object.keys(step.original_sample[0]) : [];
                return (
                  <div key={step.key} style={{ marginBottom: 12 }}>
                    <div style={{
                      color: step.state === "done" ? "#4ade80" : step.state === "failed" ? "#f87171" : step.state === "active" ? "#60a5fa" : "#94a3b8",
                      fontWeight: 700, marginBottom: 4,
                    }}>
                      [{step.state === "done" ? "✓" : step.state === "failed" ? "✗" : "▶"}] {step.label}
                    </div>
                    {step.logs.filter(log =>
                      !log.startsWith("── 改造后因子值") && !log.startsWith("── 原始因子值") && !log.startsWith("  ")
                    ).map((log, i) => (
                      <div key={i} style={{ paddingLeft: 16, color: "#cbd5e1", lineHeight: 1.6 }}>
                        {log}
                      </div>
                    ))}
                    {step.error && (
                      <div style={{ paddingLeft: 16, color: "#f87171", marginTop: 4, whiteSpace: "pre-wrap", wordBreak: "break-all" }}>
                        错误: {step.error}
                      </div>
                    )}
                    {/* 执行测试：原始 vs 改造后因子值对比表格 */}
                    {isExecStep && (step.result_sample || step.original_sample) && (
                      <div style={{ marginTop: 10, paddingLeft: 0 }}>
                        <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
                          {/* 改造后因子值 */}
                          {step.result_sample && step.result_sample.length > 0 && (
                            <div style={{ flex: 1, minWidth: 280 }}>
                              <div style={{ fontSize: 11, color: "#4ade80", fontWeight: 700, marginBottom: 4 }}>
                                ▶ 改造后因子值（新数据服务）
                              </div>
                              <div style={{ overflowX: "auto" }}>
                                <table style={{ borderCollapse: "collapse", fontSize: 10, width: "100%" }}>
                                  <thead>
                                    <tr>
                                      {resultCols.map(col => (
                                        <th key={col} style={{ padding: "2px 6px", background: "#1e293b", color: "#94a3b8", textAlign: "left", borderBottom: "1px solid #334155", whiteSpace: "nowrap" }}>
                                          {col}
                                        </th>
                                      ))}
                                    </tr>
                                  </thead>
                                  <tbody>
                                    {step.result_sample.map((row, i) => (
                                      <tr key={i} style={{ background: i % 2 === 0 ? "#0f172a" : "#1e293b" }}>
                                        {resultCols.map(col => (
                                          <td key={col} style={{ padding: "2px 6px", color: "#e2e8f0", borderBottom: "1px solid #1e293b", whiteSpace: "nowrap" }}>
                                            {row[col]}
                                          </td>
                                        ))}
                                      </tr>
                                    ))}
                                  </tbody>
                                </table>
                              </div>
                            </div>
                          )}
                          {/* 原始因子值 */}
                          {step.original_sample && step.original_sample.length > 0 && (
                            <div style={{ flex: 1, minWidth: 280 }}>
                              <div style={{ fontSize: 11, color: "#60a5fa", fontWeight: 700, marginBottom: 4 }}>
                                ▶ 原始因子值（h5文件方式）
                              </div>
                              <div style={{ overflowX: "auto" }}>
                                <table style={{ borderCollapse: "collapse", fontSize: 10, width: "100%" }}>
                                  <thead>
                                    <tr>
                                      {origCols.map(col => (
                                        <th key={col} style={{ padding: "2px 6px", background: "#1e293b", color: "#94a3b8", textAlign: "left", borderBottom: "1px solid #334155", whiteSpace: "nowrap" }}>
                                          {col}
                                        </th>
                                      ))}
                                    </tr>
                                  </thead>
                                  <tbody>
                                    {step.original_sample.map((row, i) => (
                                      <tr key={i} style={{ background: i % 2 === 0 ? "#0f172a" : "#1e293b" }}>
                                        {origCols.map(col => (
                                          <td key={col} style={{ padding: "2px 6px", color: "#e2e8f0", borderBottom: "1px solid #1e293b", whiteSpace: "nowrap" }}>
                                            {row[col]}
                                          </td>
                                        ))}
                                      </tr>
                                    ))}
                                  </tbody>
                                </table>
                              </div>
                            </div>
                          )}
                          {/* 原始因子执行失败提示 */}
                          {!step.original_sample && step.original_error && (
                            <div style={{ flex: 1, minWidth: 280 }}>
                              <div style={{ fontSize: 11, color: "#f59e0b", fontWeight: 700, marginBottom: 4 }}>
                                ⚠ 原始因子执行失败（不影响改造结论）
                              </div>
                              <div style={{ fontSize: 10, color: "#fbbf24", paddingLeft: 4, whiteSpace: "pre-wrap", wordBreak: "break-all" }}>
                                {step.original_error.slice(0, 300)}
                              </div>
                            </div>
                          )}
                        </div>
                      </div>
                    )}
                  </div>
                );
              })}
              {progress?.error_message && (
                <div style={{ marginTop: 8, color: "#f87171", fontWeight: 700, whiteSpace: "pre-wrap", wordBreak: "break-all" }}>
                  [最终错误] {progress.error_message}
                </div>
              )}
              {progress?.status === "SUCCESS" && (
                <div style={{ marginTop: 8, color: "#4ade80", fontWeight: 700 }}>
                  ✓ 因子改造完成！
                </div>
              )}
              {!isFinished && progress && (
                <div style={{ color: "#60a5fa", marginTop: 8 }}>
                  ▶ 改造进行中，每2秒自动刷新...
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
