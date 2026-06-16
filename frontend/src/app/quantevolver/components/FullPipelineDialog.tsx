"use client";

import React, { useEffect, useState, useRef, useCallback } from "react";

const API = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

type Phase = "ic_metrics" | "transform" | "analyze";
type PhaseStatus = "pending" | "running" | "done" | "error";

interface FactorStatus {
  factor_name: string;
  status: "pending" | "running" | "success" | "failed" | "skipped";
  message?: string;
  elapsed?: number;
}

interface PhaseSummary {
  success: number;
  failed: number;
  skipped?: number;
  inserted?: number;
  elapsed?: number;
}

interface FullPipelineDialogProps {
  open: boolean;
  taskIds?: string[];
  factorNames?: string[];
  cacheStartDate: string;
  cacheEndDate: string;
  onClose: () => void;
  onComplete: () => void;
}

const PHASE_LABELS: Record<Phase, string> = {
  ic_metrics: "IC指标计算",
  transform: "因子代码改造",
  analyze: "LLM分析分类",
};

const PHASE_ICONS: Record<PhaseStatus, string> = {
  pending: "⏸",
  running: "▶",
  done: "✅",
  error: "❌",
};

function formatTime(seconds: number): string {
  if (seconds < 60) return `${Math.floor(seconds)}秒`;
  const m = Math.floor(seconds / 60);
  const s = Math.floor(seconds % 60);
  return `${m}分${s}秒`;
}

export default function FullPipelineDialog({ open, taskIds, factorNames, cacheStartDate, cacheEndDate, onClose, onComplete }: FullPipelineDialogProps) {
  const [phases, setPhases] = useState<Record<Phase, PhaseStatus>>({
    ic_metrics: "pending",
    transform: "pending",
    analyze: "pending",
  });
  const [currentPhase, setCurrentPhase] = useState<Phase | null>(null);
  const [phaseProgress, setPhaseProgress] = useState({ current: 0, total: 0 });
  const [factorStatuses, setFactorStatuses] = useState<FactorStatus[]>([]);
  const [phaseSummaries, setPhaseSummaries] = useState<Record<string, PhaseSummary>>({});
  const [totalTime, setTotalTime] = useState(0);
  const [pipelineStatus, setPipelineStatus] = useState<"idle" | "running" | "done" | "error">("idle");
  const [phaseList, setPhaseList] = useState<Phase[]>(["ic_metrics", "transform", "analyze"]);
  const [totalFactors, setTotalFactors] = useState(0);
  const [currentFactorName, setCurrentFactorName] = useState<string | null>(null);
  const [pipelineSummary, setPipelineSummary] = useState<Record<string, any> | null>(null);

  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const abortRef = useRef<AbortController | null>(null);
  const tableEndRef = useRef<HTMLDivElement | null>(null);
  const onCompleteRef = useRef(onComplete);
  onCompleteRef.current = onComplete;

  // Auto-scroll to latest factor entry
  useEffect(() => {
    tableEndRef.current?.scrollIntoView({ behavior: "smooth", block: "nearest" });
  }, [factorStatuses]);

  // Timer
  useEffect(() => {
    if (pipelineStatus === "running") {
      const t0 = Date.now();
      timerRef.current = setInterval(() => setTotalTime((Date.now() - t0) / 1000), 1000);
    }
    return () => { if (timerRef.current) clearInterval(timerRef.current); };
  }, [pipelineStatus]);

  const handleEvent = useCallback((event: any) => {
    const type = event.type;
    if (type === "pipeline_start") {
      setTotalFactors(event.factor_count || 0);
      if (event.phases) setPhaseList(event.phases);
    } else if (type === "phase_start") {
      const ph = event.phase as Phase;
      setCurrentPhase(ph);
      setPhases(prev => ({ ...prev, [ph]: "running" }));
      setPhaseProgress({ current: 0, total: event.total_tasks || event.total || event.pending || 0 });
      // Reset factor statuses for transform/analyze phases
      if (ph === "transform") {
        const pending = event.pending || 0;
        setPhaseProgress({ current: 0, total: pending });
      } else if (ph === "analyze") {
        setPhaseProgress({ current: 0, total: event.total || 0 });
      }
      setFactorStatuses([]);
      setCurrentFactorName(null);
    } else if (type === "task_progress") {
      // Phase 1: IC metrics progress
      setPhaseProgress({ current: event.current, total: event.total });
      if (event.status === "computing") {
        setCurrentFactorName(event.task_id?.slice(0, 12) || "");
      } else {
        setFactorStatuses(prev => {
          const updated = prev.filter(f => f.factor_name !== event.task_id);
          updated.push({
            factor_name: event.task_id || "",
            status: event.status === "done" ? "success" : "failed",
            message: event.status === "done" ? `+${event.inserted}条, 跳过${event.skipped}` : event.error,
          });
          return updated;
        });
      }
    } else if (type === "factor_progress") {
      // Phase 2/3: per-factor progress
      setPhaseProgress({ current: event.current, total: event.total });
      if (event.status === "transforming") {
        setCurrentFactorName(event.factor_name);
        setFactorStatuses(prev => {
          const existing = prev.find(f => f.factor_name === event.factor_name);
          if (existing) return prev;
          return [...prev, { factor_name: event.factor_name, status: "running" }];
        });
      } else {
        setCurrentFactorName(null);
        setFactorStatuses(prev => {
          const updated = prev.filter(f => f.factor_name !== event.factor_name);
          updated.push({
            factor_name: event.factor_name,
            status: event.status === "done" || event.status === "success" ? "success" : "failed",
            message: event.error || undefined,
            elapsed: event.elapsed,
          });
          return updated;
        });
      }
    } else if (type === "phase_complete") {
      const ph = event.phase as Phase;
      setPhases(prev => ({ ...prev, [ph]: event.failed > 0 && event.success === 0 ? "error" : "done" }));
      setPhaseSummaries(prev => ({
        ...prev,
        [ph]: {
          success: event.success || 0,
          failed: event.failed || 0,
          skipped: event.skipped,
          inserted: event.inserted,
          elapsed: event.elapsed,
        },
      }));
    } else if (type === "pipeline_complete") {
      setPipelineSummary(event.summary || {});
    } else if (type === "error") {
      // Non-fatal error within a phase
      if (event.factor_name) {
        setFactorStatuses(prev => {
          const updated = prev.filter(f => f.factor_name !== event.factor_name);
          updated.push({ factor_name: event.factor_name, status: "failed", message: event.message || event.error });
          return updated;
        });
      }
    }
  }, []);

  // Start SSE connection
  useEffect(() => {
    if (!open || ((taskIds?.length || 0) === 0 && (factorNames?.length || 0) === 0)) return;

    // Reset state
    setPhases({ ic_metrics: "pending", transform: "pending", analyze: "pending" });
    setCurrentPhase(null);
    setPhaseProgress({ current: 0, total: 0 });
    setFactorStatuses([]);
    setPhaseSummaries({});
    setTotalTime(0);
    setPipelineStatus("running");
    setPipelineSummary(null);

    const controller = new AbortController();
    abortRef.current = controller;

    fetch(`${API}/quantevolver/factors/full-pipeline-stream`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(
        taskIds?.length
          ? { task_ids: taskIds, skip_completed: true, start_date: cacheStartDate, end_date: cacheEndDate }
          : { factor_names: factorNames, skip_completed: true, start_date: cacheStartDate, end_date: cacheEndDate }
      ),
      signal: controller.signal,
    }).then(async (res) => {
      if (!res.ok) {
        setPipelineStatus("error");
        return;
      }
      const reader = res.body!.getReader();
      const decoder = new TextDecoder();
      let buffer = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });

        const lines = buffer.split("\n");
        buffer = lines.pop()!;
        for (const line of lines) {
          if (!line.startsWith("data: ")) continue;
          try {
            const event = JSON.parse(line.slice(6));
            handleEvent(event);
          } catch { /* skip malformed */ }
        }
      }
      setPipelineStatus("done");
      onCompleteRef.current();
    }).catch((err) => {
      if (err.name !== "AbortError") {
        setPipelineStatus("error");
      }
    });

    return () => {
      controller.abort();
      if (timerRef.current) clearInterval(timerRef.current);
    };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, JSON.stringify(taskIds), JSON.stringify(factorNames), cacheStartDate, cacheEndDate]);

  const handleClose = () => {
    if (abortRef.current) abortRef.current.abort();
    onClose();
  };

  if (!open) return null;

  const progressPct = phaseProgress.total > 0 ? Math.round((phaseProgress.current / phaseProgress.total) * 100) : 0;

  return (
    <div style={{
      position: "fixed", inset: 0, zIndex: 9999,
      background: "rgba(0,0,0,0.5)", display: "flex", alignItems: "center", justifyContent: "center",
    }} onClick={(e) => { if (e.target === e.currentTarget && pipelineStatus !== "running") handleClose(); }}>
      <div style={{
        background: "#fff", borderRadius: 16, width: 700, maxHeight: "85vh",
        display: "flex", flexDirection: "column", boxShadow: "0 20px 60px rgba(0,0,0,0.2)",
      }}>
        {/* Header */}
        <div style={{
          padding: "16px 20px", borderBottom: "1px solid #e5e7eb",
          display: "flex", alignItems: "center", justifyContent: "space-between",
        }}>
          <div>
            <strong style={{ fontSize: 15 }}>全流程批处理</strong>
            <span style={{ fontSize: 11, color: "#6b7280", marginLeft: 8 }}>
              {taskIds?.length
                ? `${taskIds.length} 个Task, 共 ${totalFactors} 个因子`
                : `${factorNames?.length || 0} 个因子`}
              {` · 官方窗口 ${cacheStartDate} ~ ${cacheEndDate}`}
            </span>
          </div>
          <button onClick={handleClose} style={{
            border: "none", background: "none", fontSize: 18, cursor: "pointer", color: "#9ca3af",
            padding: "4px 8px", borderRadius: 4,
          }} title="关闭">&times;</button>
        </div>

        {/* Stepper */}
        <div style={{
          padding: "16px 20px", display: "flex", alignItems: "center", justifyContent: "center", gap: 0,
        }}>
          {phaseList.map((ph, idx) => {
            const st = phases[ph];
            const isCurrent = currentPhase === ph;
            const summary = phaseSummaries[ph];
            return (
              <React.Fragment key={ph}>
                {idx > 0 && (
                  <div style={{
                    flex: 1, height: 2, maxWidth: 80,
                    background: st === "done" || st === "error" ? (st === "done" ? "#22c55e" : "#ef4444") : "#e5e7eb",
                    transition: "background 0.3s",
                  }} />
                )}
                <div style={{ display: "flex", flexDirection: "column", alignItems: "center", minWidth: 100 }}>
                  <div style={{
                    width: 36, height: 36, borderRadius: "50%", display: "flex", alignItems: "center", justifyContent: "center",
                    fontSize: 16, fontWeight: 700, transition: "all 0.3s",
                    background: st === "done" ? "#dcfce7" : st === "error" ? "#fee2e2" : isCurrent ? "#ede9fe" : "#f3f4f6",
                    color: st === "done" ? "#16a34a" : st === "error" ? "#dc2626" : isCurrent ? "#7c3aed" : "#9ca3af",
                    border: isCurrent ? "2px solid #7c3aed" : st === "done" ? "2px solid #22c55e" : "2px solid transparent",
                    boxShadow: isCurrent ? "0 0 0 3px rgba(124,58,237,0.15)" : "none",
                  }}>
                    {PHASE_ICONS[st]}
                  </div>
                  <span style={{
                    fontSize: 11, fontWeight: 600, marginTop: 6,
                    color: st === "done" ? "#16a34a" : st === "error" ? "#dc2626" : isCurrent ? "#7c3aed" : "#9ca3af",
                  }}>
                    {PHASE_LABELS[ph]}
                  </span>
                  {summary && (
                    <span style={{ fontSize: 10, color: "#6b7280", marginTop: 2 }}>
                      {summary.success}成功
                      {(summary.failed || 0) > 0 && <span style={{ color: "#dc2626" }}> {summary.failed}失败</span>}
                      {(summary.skipped || 0) > 0 && <span> {summary.skipped}跳过</span>}
                      {summary.elapsed != null && <span> ({formatTime(summary.elapsed)})</span>}
                    </span>
                  )}
                </div>
              </React.Fragment>
            );
          })}
        </div>

        {/* Progress bar + current phase info */}
        {currentPhase && phases[currentPhase] === "running" && (
          <div style={{ padding: "0 20px 12px" }}>
            <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 6 }}>
              <span style={{ fontSize: 12, fontWeight: 600, color: "#374151" }}>
                {PHASE_LABELS[currentPhase]}
                {phaseProgress.total > 0 && ` ${phaseProgress.current}/${phaseProgress.total}`}
                {currentPhase === "transform" && phaseSummaries.transform === undefined && phaseProgress.total > 0 && (
                  <span style={{ fontWeight: 400, color: "#9ca3af", marginLeft: 4 }}>
                    (跳过 {totalFactors - phaseProgress.total} 个已完成)
                  </span>
                )}
              </span>
              <span style={{ fontSize: 11, color: "#7c3aed", fontWeight: 600 }}>{progressPct}%</span>
            </div>
            <div style={{ height: 6, background: "#e5e7eb", borderRadius: 3, overflow: "hidden" }}>
              <div style={{
                height: "100%", borderRadius: 3, transition: "width 0.3s",
                background: "linear-gradient(90deg, #7c3aed, #2563eb)",
                width: `${progressPct}%`,
              }} />
            </div>
            {currentFactorName && (
              <div style={{ fontSize: 11, color: "#6b7280", marginTop: 4 }}>
                当前: <span style={{ fontFamily: "monospace", fontWeight: 600, color: "#374151" }}>{currentFactorName}</span>
              </div>
            )}
          </div>
        )}

        {/* Factor status table */}
        <div style={{
          flex: 1, overflow: "auto", padding: "0 20px", minHeight: 120, maxHeight: 340,
        }}>
          {factorStatuses.length > 0 && (
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
              <thead>
                <tr style={{ borderBottom: "1px solid #e5e7eb", position: "sticky", top: 0, background: "#fff" }}>
                  <th style={{ padding: "6px 8px", textAlign: "left", color: "#6b7280", fontWeight: 500 }}>名称</th>
                  <th style={{ padding: "6px 8px", textAlign: "center", color: "#6b7280", fontWeight: 500, width: 80 }}>状态</th>
                  <th style={{ padding: "6px 8px", textAlign: "right", color: "#6b7280", fontWeight: 500, width: 80 }}>耗时</th>
                  <th style={{ padding: "6px 8px", textAlign: "left", color: "#6b7280", fontWeight: 500 }}>备注</th>
                </tr>
              </thead>
              <tbody>
                {factorStatuses.map((f, idx) => (
                  <tr key={`${f.factor_name}-${idx}`} style={{ borderBottom: "1px solid #f3f4f6" }}>
                    <td style={{ padding: "5px 8px", fontFamily: "monospace", fontWeight: 600, color: "#374151", maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}
                      title={f.factor_name}>
                      {f.factor_name.length > 20 ? f.factor_name.slice(0, 8) + "..." + f.factor_name.slice(-8) : f.factor_name}
                    </td>
                    <td style={{ padding: "5px 8px", textAlign: "center" }}>
                      {f.status === "success" && <span style={{ color: "#16a34a" }}>✅ 成功</span>}
                      {f.status === "failed" && <span style={{ color: "#dc2626" }}>❌ 失败</span>}
                      {f.status === "running" && <span style={{ color: "#7c3aed" }}>⏳ 处理中...</span>}
                      {f.status === "skipped" && <span style={{ color: "#d97706" }}>⏭ 跳过</span>}
                      {f.status === "pending" && <span style={{ color: "#9ca3af" }}>⏸ 等待</span>}
                    </td>
                    <td style={{ padding: "5px 8px", textAlign: "right", color: "#6b7280", fontFamily: "monospace" }}>
                      {f.elapsed != null ? `${f.elapsed}s` : "-"}
                    </td>
                    <td style={{ padding: "5px 8px", color: f.status === "failed" ? "#dc2626" : "#6b7280", maxWidth: 180, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}
                      title={f.message || ""}>
                      {f.message || "-"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
          {factorStatuses.length === 0 && pipelineStatus === "running" && (
            <div style={{ textAlign: "center", padding: 24, color: "#9ca3af", fontSize: 12 }}>
              等待处理开始...
            </div>
          )}
          <div ref={tableEndRef} />
        </div>

        {/* Pipeline complete summary */}
        {pipelineStatus === "done" && pipelineSummary && (
          <div style={{
            margin: "0 20px 12px", padding: 12, borderRadius: 8,
            background: "#f0fdf4", border: "1px solid #bbf7d0", fontSize: 12,
          }}>
            <strong style={{ color: "#16a34a" }}>全流程完成</strong>
            <span style={{ marginLeft: 12, color: "#374151" }}>
              IC指标 +{pipelineSummary.ic_inserted || 0}条
              {(pipelineSummary.transform_success || 0) > 0 && ` | 改造成功 ${pipelineSummary.transform_success}`}
              {(pipelineSummary.transform_failed || 0) > 0 && <span style={{ color: "#dc2626" }}> 失败 {pipelineSummary.transform_failed}</span>}
              {` | 分析成功 ${pipelineSummary.analyze_success || 0}`}
              {(pipelineSummary.analyze_failed || 0) > 0 && <span style={{ color: "#dc2626" }}> 失败 {pipelineSummary.analyze_failed}</span>}
            </span>
          </div>
        )}

        {pipelineStatus === "error" && (
          <div style={{
            margin: "0 20px 12px", padding: 12, borderRadius: 8,
            background: "#fef2f2", border: "1px solid #fecaca", fontSize: 12, color: "#dc2626",
          }}>
            <strong>流程异常中断</strong> — 已完成的步骤不受影响，可安全重新执行
          </div>
        )}

        {/* Footer */}
        <div style={{
          padding: "12px 20px", borderTop: "1px solid #e5e7eb",
          display: "flex", alignItems: "center", justifyContent: "space-between",
        }}>
          <span style={{ fontSize: 12, color: "#6b7280" }}>
            已用时: {formatTime(totalTime)}
          </span>
          <button onClick={handleClose} style={{
            padding: "6px 20px", fontSize: 12, borderRadius: 6, border: "none",
            background: pipelineStatus === "done" ? "#22c55e" : "#e5e7eb",
            color: pipelineStatus === "done" ? "#fff" : "#374151",
            fontWeight: 600, cursor: "pointer",
          }}>
            {pipelineStatus === "running" ? "中断并关闭" : "关闭"}
          </button>
        </div>
      </div>
    </div>
  );
}
