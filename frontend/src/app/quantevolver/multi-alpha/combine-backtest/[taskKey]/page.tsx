"use client";

import React, { Suspense, useCallback, useEffect, useMemo, useState } from "react";
import { Activity, ArrowLeft, DownloadCloud, PackagePlus, RefreshCw, Trash2 } from "lucide-react";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { PaperV2ApiError, strategyPackageApi } from "@/lib/paper-v2/api";
import LoopDetailPanel from "../../../evolution/components/LoopDetailPanel";
import type { Loop } from "../../../evolution/components/TopologyPanel";
import type { DataSourceAdapter } from "../../../components/EvolutionTrajectory";
import CombineDiagnosticsPanel, { type CombineDiagnosticsLoop } from "../components/CombineDiagnosticsPanel";

const API = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";
const DELETE_APPROVAL_MESSAGE = "删除属于写操作，当前设计实现为只读查询；如需启用删除端点，请单独审批写入范围。";

type CombineTask = {
  task_id: string;
  task_name: string;
  task_type: "multi_alpha_combine";
  status: string;
  current_loop: number;
  max_loops: number;
  created_at: string;
  updated_at: string;
  roster_hash: string;
  normalize_method: string;
  walk_forward_signature: string;
  available_schemes?: string[];
  default_scheme?: string;
  phase?: string | null;
  running_count?: number;
};

type CombineTaskDetail = {
  task: CombineTask;
  loops: Loop[];
  scheme: string;
  available_schemes: string[];
  scheme_warning?: Record<string, any> | null;
};

const cardStyle: React.CSSProperties = {
  backgroundColor: "#ffffff",
  borderRadius: "12px",
  boxShadow: "0 4px 12px rgba(0, 0, 0, 0.05)",
  display: "flex",
  flexDirection: "column",
  overflow: "hidden",
  border: "1px solid rgba(255, 255, 255, 0.2)",
};

const headerStyle: React.CSSProperties = {
  padding: "16px 20px",
  borderBottom: "1px solid #f1f5f9",
  backgroundColor: "#f8fafc",
  display: "flex",
  justifyContent: "space-between",
  alignItems: "center",
};

function normalizeError(detail: any, fallback: string): string {
  if (!detail) return fallback;
  if (typeof detail === "string") return detail;
  if (typeof detail === "object") {
    const context = typeof detail.context === "object" && detail.context !== null ? detail.context : null;
    const reasonCode = detail.reason_code || context?.reason_code;
    const code = reasonCode ? `reason_code=${reasonCode}` : "";
    const message = detail.message || detail.detail || fallback;
    const contextPreview = context ? ` context=${JSON.stringify(context)}` : "";
    return `${[code, message].filter(Boolean).join(": ")}${contextPreview}`;
  }
  return fallback;
}

async function fetchJson<T>(url: string): Promise<T> {
  const response = await fetch(url);
  const json = await response.json().catch(() => ({}));
  if (!response.ok || json.status !== "success") {
    throw new Error(normalizeError(json.detail || json.message, `HTTP ${response.status}`));
  }
  return json.data as T;
}

function safeDecode(value: string): string {
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function formatTime(value?: string | null): string {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString("zh-CN", { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" });
}

function formatPct(value: any, digits = 2): string {
  return typeof value === "number" && Number.isFinite(value) ? `${(value * 100).toFixed(digits)}%` : "-";
}

function formatNum(value: any, digits = 2): string {
  return typeof value === "number" && Number.isFinite(value) ? value.toFixed(digits) : "-";
}

function apiErrorMessage(error: unknown): string {
  if (error instanceof PaperV2ApiError) {
    const reasonCode = typeof error.context?.reason_code === "string" ? `reason_code=${error.context.reason_code}: ` : "";
    const context = error.context ? ` context=${JSON.stringify(error.context)}` : "";
    return `${reasonCode}${error.message}${context}`;
  }
  if (error instanceof Error) return error.message;
  return String(error || "unknown error");
}

function exportJson(filename: string, payload: unknown) {
  const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename.replace(/[^a-zA-Z0-9_.-]+/g, "_");
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(url);
}

function metricValue(loop: Loop | undefined, key: string): any {
  return loop?.metrics_json?.[key];
}

function extractTopk(loop: Loop | undefined): number | null {
  const topk = loop?.config_json?.strategy_params?.topk ?? loop?.config_json?.backtest_config?.topk;
  const parsed = Number(topk);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}

function exportRunId(loop: Loop | undefined): string | null {
  const value = loop?.config_json?.runtime_flags?.run_id ?? (loop as any)?.run_id;
  const text = String(value || "").trim();
  return text || null;
}

function combineStatusInfo(status: string): { color: string; bgColor: string; label: string } {
  switch (status === "succeeded" ? "completed" : status) {
    case "running":
      return { label: "运行中", color: "#0369a1", bgColor: "#e0f2fe" };
    case "completed":
      return { label: "已完成", color: "#047857", bgColor: "#d1fae5" };
    case "failed":
    case "partial_failed":
      return { label: status === "partial_failed" ? "部分失败" : "已失败", color: "#b91c1c", bgColor: "#fee2e2" };
    case "pending":
      return { label: "等待调度", color: "#64748b", bgColor: "#f1f5f9" };
    default:
      return { label: status || "-", color: "#64748b", bgColor: "#f1f5f9" };
  }
}

type PageProps = { params: { taskKey: string } };

function MultiAlphaCombineBacktestDetailContent({ params }: PageProps) {
  const taskKey = safeDecode(params.taskKey);
  const searchParams = useSearchParams();
  const requestedTab = searchParams.get("tab");
  const [detail, setDetail] = useState<CombineTaskDetail | null>(null);
  const [selectedScheme, setSelectedScheme] = useState("");
  const [selectedLoopIndex, setSelectedLoopIndex] = useState<number | null>(null);
  const [rightPanelView, setRightPanelView] = useState<"loop" | "trajectory">("trajectory");
  const [pageTab, setPageTab] = useState<"detail" | "diagnostics">(requestedTab === "diagnostics" ? "diagnostics" : "detail");
  const [detailTab, setDetailTab] = useState("overview");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [packageExporting, setPackageExporting] = useState(false);
  const [packageExportMessage, setPackageExportMessage] = useState<{ ok: boolean; text: string; packageId?: string } | null>(null);
  const [lastLoadedAt, setLastLoadedAt] = useState<string | null>(null);
  const selectionRunId = searchParams.get("selection_run_id");

  const loadDetail = useCallback(async () => {
    setLoading(true);
    setError(null);
    const query = selectedScheme ? `?scheme=${encodeURIComponent(selectedScheme)}` : "";
    try {
      const data = await fetchJson<CombineTaskDetail>(`${API}/multi-alpha/combine/tasks/${encodeURIComponent(taskKey)}${query}`);
      setDetail(data);
      setSelectedScheme(data.scheme || selectedScheme || "ic_weighted");
      setSelectedLoopIndex((current) => {
        if (current && data.loops.some((loop) => loop.loop_index === current)) return current;
        return data.loops.find((loop) => loop.is_sota)?.loop_index ?? data.loops[0]?.loop_index ?? null;
      });
      setLastLoadedAt(new Date().toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit", second: "2-digit" }));
    } catch (exc) {
      setDetail(null);
      setError(exc instanceof Error ? exc.message : String(exc));
    } finally {
      setLoading(false);
    }
  }, [selectedScheme, taskKey]);

  useEffect(() => {
    void loadDetail();
  }, [loadDetail]);

  useEffect(() => {
    if (requestedTab === "diagnostics") {
      setPageTab("diagnostics");
    }
  }, [requestedTab]);

  const loops = useMemo(() => detail?.loops || [], [detail?.loops]);
  const task = detail?.task || null;
  const activeLoopData = useMemo(() => {
    if (selectedLoopIndex == null) return undefined;
    return loops.find((loop) => loop.loop_index === selectedLoopIndex);
  }, [loops, selectedLoopIndex]);
  const prevLoopData = useMemo(() => {
    if (selectedLoopIndex == null) return undefined;
    return loops.find((loop) => loop.loop_index === selectedLoopIndex - 1);
  }, [loops, selectedLoopIndex]);
  const dataSourceAdapter = useMemo<DataSourceAdapter>(() => ({
    basePath: "/multi-alpha/combine",
    taskType: "multi_alpha_combine",
    scheme: detail?.scheme || selectedScheme,
  }), [detail?.scheme, selectedScheme]);
  const statusInfo = combineStatusInfo(task?.status || "pending");
  const completedCount = loops.filter((loop) => loop.status === "completed").length;
  const failedCount = loops.filter((loop) => loop.status === "failed").length;
  const runningCount = loops.filter((loop) => loop.status === "running").length;
  const bestLoop = loops.find((loop) => loop.is_sota);
  const exportLoop = bestLoop || loops.find((loop) => loop.status === "completed") || activeLoopData;
  const packageExportRunId = exportRunId(exportLoop);
  const packageExportTopk = extractTopk(exportLoop);
  const packageExportScheme = detail?.scheme || selectedScheme;
  const packageExportDisabledReason = !detail
    ? "详情尚未加载"
    : task?.status !== "completed"
      ? "仅已完成的 combine-backtest task 可导出策略包"
      : packageExportScheme !== "ic_weighted"
        ? "S1 仅支持 ic_weighted scheme 导出"
        : !packageExportRunId
          ? "缺少可审计的 combine_backtest_run_id"
          : packageExportTopk == null
            ? "缺少 TopK，拒绝使用默认值"
            : null;
  const canExportPackage = !packageExportDisabledReason && !packageExporting;

  const exportStrategyPackage = useCallback(async () => {
    if (packageExportDisabledReason || !packageExportRunId || packageExportTopk == null) {
      setPackageExportMessage({ ok: false, text: packageExportDisabledReason || "导出条件不完整" });
      return;
    }
    setPackageExporting(true);
    setPackageExportMessage(null);
    try {
      const pkg = await strategyPackageApi.createFromMultiAlphaCombineRun({
        combine_backtest_run_id: packageExportRunId,
        weighting_scheme: packageExportScheme,
        topk: packageExportTopk,
        secondary_topk: [],
        weight_policy: { mode: "frozen_backtest_terminal_weights" },
        confirmation: "MULTI_ALPHA_PACKAGE_PROMOTE",
      });
      setPackageExportMessage({ ok: true, text: `已导出 StrategyPackage: ${pkg.package_id}`, packageId: pkg.package_id });
    } catch (exc) {
      setPackageExportMessage({ ok: false, text: `导出策略包失败: ${apiErrorMessage(exc)}` });
    } finally {
      setPackageExporting(false);
    }
  }, [packageExportDisabledReason, packageExportRunId, packageExportScheme, packageExportTopk]);

  return (
    <div style={{ minHeight: "100vh", backgroundColor: "#f1f5f9", padding: "24px 32px", overflow: "hidden", display: "flex", flexDirection: "column" }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "24px" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
          <Link href="/quantevolver/multi-alpha/combine-backtest" style={{ display: "flex", alignItems: "center", gap: 6, padding: "6px 12px", borderRadius: 6, border: "1px solid #e2e8f0", backgroundColor: "#fff", cursor: "pointer", fontSize: 13, color: "#475569", textDecoration: "none" }}>
            <ArrowLeft size={14} /> 返回
          </Link>
          <div>
            <h1 style={{ margin: 0, fontSize: "24px", fontWeight: 700, color: "#0f172a", display: "flex", alignItems: "center", gap: "8px" }}>
              <Activity color="#3b82f6" size={28} />
              {task?.task_name || "多Alpha 组合回测详情"}
            </h1>
            <p style={{ margin: "4px 0 0", color: "#64748b", fontSize: "14px", fontFamily: "monospace" }}>
              {taskKey}
            </p>
          </div>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: "10px" }}>
          <select
            value={selectedScheme}
            onChange={(event) => { setSelectedScheme(event.target.value); setRightPanelView("trajectory"); }}
            disabled={!detail?.available_schemes?.length}
            style={{ padding: "7px 10px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "13px", color: "#475569", backgroundColor: "#fff" }}
          >
            {(detail?.available_schemes?.length ? detail.available_schemes : [selectedScheme]).map((scheme) => (
              <option key={scheme} value={scheme}>{scheme}</option>
            ))}
          </select>
          <button
            onClick={() => void loadDetail()}
            style={{ padding: "6px 14px", backgroundColor: "#fff", color: "#64748b", border: "1px solid #e2e8f0", borderRadius: "6px", fontSize: "12px", fontWeight: 600, cursor: "pointer", display: "flex", alignItems: "center", gap: "6px" }}
          >
            <RefreshCw size={12} /> 刷新
            <span style={{ fontSize: "10px", color: "#94a3b8", fontWeight: 400 }}>{lastLoadedAt || "手动"}</span>
          </button>
          <button
            onClick={() => detail && exportJson(`${taskKey}_${detail.scheme}.json`, detail)}
            disabled={!detail}
            style={{ padding: "6px 14px", backgroundColor: "#eff6ff", color: "#1d4ed8", border: "1px solid #bfdbfe", borderRadius: "6px", fontSize: "12px", fontWeight: 600, cursor: detail ? "pointer" : "not-allowed", display: "flex", alignItems: "center", gap: "6px" }}
          >
            <DownloadCloud size={12} /> 导出 JSON
          </button>
          <button
            onClick={() => void exportStrategyPackage()}
            disabled={!canExportPackage}
            title={packageExportDisabledReason || "从多Alpha combine run 一步导出 StrategyPackage"}
            style={{ padding: "6px 14px", backgroundColor: canExportPackage ? "#ecfdf5" : "#f8fafc", color: canExportPackage ? "#047857" : "#94a3b8", border: `1px solid ${canExportPackage ? "#86efac" : "#e2e8f0"}`, borderRadius: "6px", fontSize: "12px", fontWeight: 700, cursor: canExportPackage ? "pointer" : "not-allowed", display: "flex", alignItems: "center", gap: "6px" }}
          >
            <PackagePlus size={12} /> {packageExporting ? "导出中..." : "导出为策略包"}
          </button>
          <button
            onClick={() => alert(DELETE_APPROVAL_MESSAGE)}
            title={DELETE_APPROVAL_MESSAGE}
            style={{ padding: "6px 14px", backgroundColor: "#fff", color: "#ef4444", border: "1px solid #fca5a5", borderRadius: "6px", fontSize: "12px", fontWeight: 600, cursor: "not-allowed", display: "flex", alignItems: "center", gap: "6px", opacity: 0.65 }}
          >
            <Trash2 size={12} /> 删除
          </button>
        </div>
      </div>

      {error && (
        <div style={{ marginBottom: 12, padding: "10px 12px", backgroundColor: "#fef2f2", border: "1px solid #ef4444", borderRadius: 8, fontSize: 12, color: "#991b1b" }}>
          {error}
        </div>
      )}

      {detail?.scheme_warning && (
        <div style={{ marginBottom: 12, padding: "10px 12px", backgroundColor: "#fffbeb", border: "1px solid #fde68a", borderRadius: 8, fontSize: 12, color: "#92400e" }}>
          默认 ic_weighted 不可用，当前展示 {detail.scheme}；reason_code={detail.scheme_warning.reason_code || "combine_ui_default_scheme_unavailable"}
        </div>
      )}

      {packageExportMessage && (
        <div style={{ marginBottom: 12, padding: "10px 12px", backgroundColor: packageExportMessage.ok ? "#f0fdf4" : "#fef2f2", border: `1px solid ${packageExportMessage.ok ? "#86efac" : "#ef4444"}`, borderRadius: 8, fontSize: 12, color: packageExportMessage.ok ? "#166534" : "#991b1b" }}>
          {packageExportMessage.text}
          {packageExportMessage.packageId && (
            <Link href={`/paper-v2/packages?package_id=${encodeURIComponent(packageExportMessage.packageId)}`} style={{ marginLeft: 12, color: "#047857", fontWeight: 800 }}>
              打开策略包列表
            </Link>
          )}
        </div>
      )}

      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))", gap: "12px", marginBottom: "16px" }}>
        {[
          { label: "状态", value: statusInfo.label, color: statusInfo.color },
          { label: "配置数", value: `${task?.current_loop ?? 0}/${task?.max_loops ?? 0}`, color: "#0f172a" },
          { label: "完成/失败/运行", value: `${completedCount}/${failedCount}/${runningCount}`, color: "#64748b" },
          { label: "最佳 CAGR", value: formatPct(metricValue(bestLoop, "annualized_return")), color: "#059669" },
          { label: "最佳 Sharpe", value: formatNum(metricValue(bestLoop, "sharpe"), 2), color: "#3b82f6" },
          { label: "更新时间", value: formatTime(task?.updated_at), color: "#64748b" },
        ].map((item) => (
          <div key={item.label} style={{ textAlign: "center", padding: "16px", backgroundColor: "#f8fafc", borderRadius: "8px", border: "1px solid #e2e8f0" }}>
            <div style={{ fontSize: "11px", color: "#64748b", fontWeight: 600, textTransform: "uppercase" }}>{item.label}</div>
            <div style={{ fontSize: "20px", fontWeight: 700, color: item.color, fontFamily: "monospace", marginTop: 4 }}>{item.value}</div>
          </div>
        ))}
      </div>

      <div style={{ display: "flex", gap: 8, marginBottom: 16 }}>
        {[
          { key: "detail" as const, label: "配置详情" },
          { key: "diagnostics" as const, label: "诊断" },
        ].map((tab) => (
          <button
            key={tab.key}
            onClick={() => setPageTab(tab.key)}
            style={{
              padding: "8px 14px",
              borderRadius: 8,
              border: `1px solid ${pageTab === tab.key ? "#2563eb" : "#cbd5e1"}`,
              backgroundColor: pageTab === tab.key ? "#eff6ff" : "#fff",
              color: pageTab === tab.key ? "#1d4ed8" : "#475569",
              fontSize: 13,
              fontWeight: 800,
              cursor: "pointer",
            }}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {pageTab === "diagnostics" ? (
        <div style={{ flex: 1, minHeight: 0, overflowY: "auto" }}>
          <CombineDiagnosticsPanel
            apiBase={API}
            taskKey={taskKey}
            task={task}
            loops={loops as CombineDiagnosticsLoop[]}
            selectedScheme={detail?.scheme || selectedScheme}
            selectionRunId={selectionRunId}
          />
        </div>
      ) : (
      <div style={{ display: "flex", flex: 1, gap: "16px", minHeight: 0 }}>
        <div style={{ ...cardStyle, flex: "0 0 420px" }}>
          <div style={headerStyle}>
            <h2 style={{ margin: 0, fontSize: "16px", fontWeight: 700, color: "#1e293b", display: "flex", alignItems: "center", gap: "8px" }}>
              <Activity color="#a855f7" size={20} />
              配置列表
            </h2>
          </div>
          <div style={{ flex: 1, overflowY: "auto", padding: "24px", backgroundColor: "#fafaf9" }}>
            {loading && <div style={{ textAlign: "center", color: "#94a3b8", fontSize: "14px", marginTop: "40px" }}>加载配置中...</div>}
            {!loading && loops.length === 0 && <div style={{ textAlign: "center", color: "#94a3b8", fontSize: "14px", marginTop: "40px" }}>暂无配置记录</div>}
            <div style={{ display: "flex", flexDirection: "column", gap: "12px" }}>
              {loops.map((loop) => {
                const isActive = selectedLoopIndex === loop.loop_index;
                const label = loop.config_json?.label || `配置 ${loop.loop_index}`;
                return (
                  <div
                    key={loop.loop_id}
                    onClick={() => { setSelectedLoopIndex(loop.loop_index); setRightPanelView("loop"); }}
                    style={{ padding: "12px 16px", borderRadius: "8px", backgroundColor: "#ffffff", border: `1px solid ${isActive ? "#60a5fa" : loop.is_sota ? "#f59e0b" : "#e2e8f0"}`, boxShadow: isActive ? "0 4px 6px -1px rgba(59, 130, 246, 0.1), 0 2px 4px -1px rgba(59, 130, 246, 0.06)" : "0 1px 2px 0 rgba(0, 0, 0, 0.05)", cursor: "pointer", transition: "all 0.2s" }}
                  >
                    <div style={{ fontWeight: 700, color: "#1e293b", fontSize: "14px", display: "flex", justifyContent: "space-between", gap: 8 }}>
                      <span>配置 {loop.loop_index}</span>
                      <div style={{ display: "flex", gap: "4px", alignItems: "center" }}>
                        {loop.is_sota && <span style={{ fontSize: "10px", color: "#d97706", backgroundColor: "#fef3c7", padding: "2px 6px", borderRadius: "4px" }}>最优配置</span>}
                        <span style={{ fontSize: "10px", color: loop.status === "completed" ? "#22c55e" : loop.status === "running" ? "#3b82f6" : "#ef4444", backgroundColor: loop.status === "completed" ? "#f0fdf4" : loop.status === "running" ? "#dbeafe" : "#fef2f2", padding: "2px 6px", borderRadius: "4px" }}>{loop.status}</span>
                      </div>
                    </div>
                    <div title={label} style={{ marginTop: "8px", padding: "8px 10px", backgroundColor: "#f8fafc", border: "1px solid #e2e8f0", borderRadius: "6px", color: "#475569", fontSize: "11px", lineHeight: 1.5 }}>
                      <span style={{ color: "#0f172a", fontWeight: 700, marginRight: "4px" }}>说明</span>{label}
                    </div>
                    <div style={{ display: "flex", gap: "8px", marginTop: "6px", fontSize: "11px", color: "#475569", fontFamily: "monospace" }}>
                      <span>CAGR:{formatPct(loop.metrics_json?.annualized_return, 1)}</span>
                      <span>Sh:{formatNum(loop.metrics_json?.sharpe, 2)}</span>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        </div>

        <LoopDetailPanel
          activeLoopData={activeLoopData}
          prevLoopData={prevLoopData}
          rightPanelView={rightPanelView}
          onSetRightPanelView={setRightPanelView}
          detailTab={detailTab}
          onSetDetailTab={setDetailTab}
          enhancedMetrics={null}
          activeTaskId={taskKey}
          activeTask={task || undefined}
          configDiffLines={[]}
          onSyncAssets={() => undefined}
          taskType="multi_alpha_combine"
          dataSourceAdapter={dataSourceAdapter}
          loops={loops}
          onLoopSelect={(loopIndex) => { setSelectedLoopIndex(loopIndex); setRightPanelView("loop"); }}
        />
      </div>
      )}
    </div>
  );
}

export default function MultiAlphaCombineBacktestDetailPage({ params }: PageProps) {
  return (
    <Suspense fallback={<div style={{ padding: 24, color: "#475569" }}>Loading combine-backtest detail...</div>}>
      <MultiAlphaCombineBacktestDetailContent params={params} />
    </Suspense>
  );
}
