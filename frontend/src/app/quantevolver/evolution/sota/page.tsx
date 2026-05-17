"use client";

import React, { useCallback, useEffect, useMemo, useState } from "react";
import { PaperV2ApiError, strategyPackageApi } from "@/lib/paper-v2/api";
import type { CandidateStrategyPackage, JsonObject, StrategyPackage } from "@/lib/paper-v2/types";

function formatDate(value?: string | null): string {
  if (!value) return "-";
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? value : d.toLocaleString("zh-CN");
}

function shortId(value?: string | null): string {
  if (!value) return "-";
  return value.length > 18 ? `${value.slice(0, 10)}...${value.slice(-6)}` : value;
}

function metricValue(snapshot: JsonObject): string {
  const keys = ["ic", "IC", "rank_ic", "Rank_IC", "sharpe", "Sharpe", "annualized_return"];
  for (const key of keys) {
    const value = snapshot[key];
    if (typeof value === "number" && Number.isFinite(value)) return `${key}=${value.toFixed(4)}`;
  }
  return Object.keys(snapshot).length ? `${Object.keys(snapshot).length} fields` : "-";
}

function hasPackageManifest(candidate: CandidateStrategyPackage): boolean {
  const manifest = candidate.snapshot_config?.strategy_package_manifest;
  return typeof manifest === "object" && manifest !== null && !Array.isArray(manifest);
}

function sourceLabel(candidate: CandidateStrategyPackage): string {
  if (candidate.source_type === "qe_experiment") return `QE experiment ${shortId(candidate.source_experiment_id || candidate.source_id)}`;
  if (candidate.source_type === "qe_evolution_loop") return `QE loop ${shortId(candidate.source_loop_id || candidate.source_id)}`;
  if (candidate.source_type === "candidate_strategy_package") return `Candidate clone ${shortId(candidate.source_id)}`;
  return `${candidate.source_type}:${shortId(candidate.source_id)}`;
}

function apiErrorMessage(error: unknown): string {
  if (error instanceof PaperV2ApiError) return error.message;
  if (error instanceof Error) return error.message;
  return String(error || "unknown error");
}

export default function EvolutionSotaPage() {
  const [items, setItems] = useState<CandidateStrategyPackage[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [busyId, setBusyId] = useState<string | null>(null);

  const fetchCandidates = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const candidates = await strategyPackageApi.candidateList("ACTIVE", 200);
      setItems(candidates);
    } catch (e) {
      setError(apiErrorMessage(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchCandidates();
  }, [fetchCandidates]);

  const summary = useMemo(() => {
    const bySource = new Map<string, number>();
    let manifestReady = 0;
    for (const item of items) {
      bySource.set(item.source_type, (bySource.get(item.source_type) || 0) + 1);
      if (hasPackageManifest(item)) manifestReady += 1;
    }
    return { total: items.length, manifestReady, bySource };
  }, [items]);

  const createPackage = async (candidate: CandidateStrategyPackage) => {
    if (!hasPackageManifest(candidate)) {
      setMessage("该候选缺少 strategy_package_manifest 快照。需要后续 QE source snapshot assembler 补齐后，才能从候选创建正式 StrategyPackage。");
      return;
    }
    setBusyId(candidate.candidate_id);
    setMessage(null);
    try {
      const pkg: StrategyPackage = await strategyPackageApi.createFromCandidate(candidate.candidate_id);
      setMessage(`已创建 StrategyPackage: ${pkg.package_id}`);
      await fetchCandidates();
    } catch (e) {
      setMessage(`创建 StrategyPackage 失败: ${apiErrorMessage(e)}`);
    } finally {
      setBusyId(null);
    }
  };

  const cloneCandidate = async (candidate: CandidateStrategyPackage) => {
    setBusyId(candidate.candidate_id);
    setMessage(null);
    try {
      const cloned = await strategyPackageApi.cloneCandidate(candidate.candidate_id, {
        created_by: "candidate_strategy_page",
        display_name: `${candidate.display_name} copy`,
      });
      setMessage(`已克隆候选策略包: ${cloned.candidate_id}`);
      await fetchCandidates();
    } catch (e) {
      setMessage(`克隆失败: ${apiErrorMessage(e)}`);
    } finally {
      setBusyId(null);
    }
  };

  const deleteCandidate = async (candidate: CandidateStrategyPackage) => {
    if (!window.confirm(`确认删除候选策略包 ${candidate.display_name || candidate.candidate_id}？不会删除 QE 源、数仓归档或已创建的 StrategyPackage。`)) return;
    setBusyId(candidate.candidate_id);
    setMessage(null);
    try {
      await strategyPackageApi.deleteCandidate(candidate.candidate_id, {
        deleted_by: "candidate_strategy_page",
        delete_reason: "Deleted from Candidate Strategy Packages page",
      });
      setMessage(`已删除候选策略包: ${candidate.candidate_id}`);
      await fetchCandidates();
    } catch (e) {
      setMessage(`删除失败: ${apiErrorMessage(e)}`);
    } finally {
      setBusyId(null);
    }
  };

  const refreshCandidateSnapshot = async (candidate: CandidateStrategyPackage) => {
    setBusyId(candidate.candidate_id);
    setMessage(null);
    try {
      const refreshed = await strategyPackageApi.refreshCandidateSnapshot(candidate.candidate_id, {
        refreshed_by: "candidate_strategy_page",
      });
      const ready = hasPackageManifest(refreshed) ? "manifest ready" : "snapshot only";
      setMessage(`Refreshed candidate snapshot: ${refreshed.candidate_id} (${ready})`);
      await fetchCandidates();
    } catch (e) {
      setMessage(`Refresh snapshot failed: ${apiErrorMessage(e)}`);
    } finally {
      setBusyId(null);
    }
  };

  return (
    <div style={{ padding: "24px", display: "flex", flexDirection: "column", gap: "16px" }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 16 }}>
        <div>
          <h1 style={{ margin: 0, fontSize: "22px" }}>候选策略包</h1>
          <div style={{ marginTop: 6, color: "#64748b", fontSize: 13 }}>
            原 SOTA 展示页已改为用户显式创建的候选策略包池；不会根据历史推荐标记自动晋升。
          </div>
        </div>
        <button onClick={fetchCandidates} style={{ padding: "8px 14px", borderRadius: "8px", border: "1px solid #cbd5e1", background: "#fff", cursor: "pointer" }}>
          刷新
        </button>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))", gap: "12px" }}>
        {[
          { label: "候选总数", value: summary.total, color: "#2563eb" },
          { label: "可直接建包", value: summary.manifestReady, color: "#16a34a" },
          { label: "QE 实验来源", value: summary.bySource.get("qe_experiment") || 0, color: "#f59e0b" },
          { label: "QE Loop 来源", value: summary.bySource.get("qe_evolution_loop") || 0, color: "#ec4899" },
        ].map((s) => (
          <div key={s.label} style={{ padding: "14px 16px", backgroundColor: "#fff", borderRadius: "10px", border: "1px solid #e2e8f0", textAlign: "center" }}>
            <div style={{ fontSize: "11px", color: "#64748b", fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.05em" }}>{s.label}</div>
            <div style={{ fontSize: "22px", fontWeight: 700, color: s.color, fontFamily: "monospace", marginTop: "4px" }}>{s.value}</div>
          </div>
        ))}
      </div>

      {message && <div style={{ padding: "10px 12px", borderRadius: "8px", background: "#f8fafc", border: "1px solid #cbd5e1", color: "#334155" }}>{message}</div>}
      {loading && <div>加载中...</div>}
      {error && <div style={{ color: "#dc2626" }}>加载失败：{error}</div>}

      {!loading && !error && (
        <div style={{ overflowX: "auto", border: "1px solid #e2e8f0", borderRadius: "10px", background: "#fff" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "13px" }}>
            <thead>
              <tr style={{ background: "#f8fafc", textAlign: "left" }}>
                <th style={{ padding: "10px", width: "28px" }} />
                <th style={{ padding: "10px" }}>候选</th>
                <th style={{ padding: "10px" }}>来源</th>
                <th style={{ padding: "10px" }}>状态</th>
                <th style={{ padding: "10px" }}>指标快照</th>
                <th style={{ padding: "10px" }}>完整性</th>
                <th style={{ padding: "10px" }}>创建时间</th>
                <th style={{ padding: "10px" }}>操作</th>
              </tr>
            </thead>
            <tbody>
              {items.map((candidate) => {
                const manifestReady = hasPackageManifest(candidate);
                const isBusy = busyId === candidate.candidate_id;
                return (
                  <React.Fragment key={candidate.candidate_id}>
                    <tr style={{ borderTop: "1px solid #f1f5f9", cursor: "pointer" }} onClick={() => setExpandedId(expandedId === candidate.candidate_id ? null : candidate.candidate_id)}>
                      <td style={{ padding: "10px", fontSize: "10px", color: "#94a3b8" }}>{expandedId === candidate.candidate_id ? "▼" : "▶"}</td>
                      <td style={{ padding: "10px" }}>
                        <div style={{ fontWeight: 700, color: "#0f172a" }}>{candidate.display_name || candidate.candidate_id}</div>
                        <div style={{ fontFamily: "monospace", fontSize: 11, color: "#64748b" }}>{candidate.candidate_id}</div>
                      </td>
                      <td style={{ padding: "10px", color: "#475569" }}>{sourceLabel(candidate)}</td>
                      <td style={{ padding: "10px" }}>
                        <span style={{ padding: "3px 8px", borderRadius: "999px", background: "#f0fdf4", border: "1px solid #bbf7d0", color: "#166534", fontSize: 11, fontWeight: 700 }}>
                          {candidate.status}
                        </span>
                      </td>
                      <td style={{ padding: "10px", fontFamily: "monospace" }}>{metricValue(candidate.metric_snapshot)}</td>
                      <td style={{ padding: "10px", color: manifestReady ? "#16a34a" : "#d97706" }}>
                        {manifestReady ? "manifest ready" : "snapshot only"}
                      </td>
                      <td style={{ padding: "10px", color: "#64748b" }}>{formatDate(candidate.created_at)}</td>
                      <td style={{ padding: "10px" }} onClick={(e) => e.stopPropagation()}>
                        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                          <button disabled={isBusy || !manifestReady} onClick={() => createPackage(candidate)} title={manifestReady ? "从候选创建正式 StrategyPackage" : "缺少 strategy_package_manifest 快照"} style={{ padding: "6px 10px", borderRadius: 7, border: "1px solid #16a34a", background: "#f0fdf4", color: "#166534", cursor: manifestReady && !isBusy ? "pointer" : "not-allowed", opacity: manifestReady ? 1 : 0.55 }}>
                            建正式包
                          </button>
                          <button disabled={isBusy} onClick={() => cloneCandidate(candidate)} style={{ padding: "6px 10px", borderRadius: 7, border: "1px solid #cbd5e1", background: "#fff", color: "#334155", cursor: isBusy ? "not-allowed" : "pointer" }}>
                            克隆
                          </button>
                          <button disabled={isBusy} onClick={() => refreshCandidateSnapshot(candidate)} title="Refresh server-side QE manifest snapshot" style={{ padding: "6px 10px", borderRadius: 7, border: "1px solid #bfdbfe", background: "#eff6ff", color: "#1d4ed8", cursor: isBusy ? "not-allowed" : "pointer" }}>
                            刷新快照
                          </button>
                          <button disabled={isBusy} onClick={() => deleteCandidate(candidate)} style={{ padding: "6px 10px", borderRadius: 7, border: "1px solid #fecaca", background: "#fff1f2", color: "#be123c", cursor: isBusy ? "not-allowed" : "pointer" }}>
                            删除
                          </button>
                        </div>
                      </td>
                    </tr>
                    {expandedId === candidate.candidate_id && (
                      <tr>
                        <td colSpan={8} style={{ padding: "12px 20px", backgroundColor: "#f8fafc", borderTop: "1px solid #e2e8f0" }}>
                          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "16px" }}>
                            <div>
                              <div style={{ fontSize: 11, fontWeight: 700, color: "#64748b", textTransform: "uppercase", marginBottom: 8 }}>配置快照</div>
                              <pre style={{ margin: 0, maxHeight: 220, overflow: "auto", fontSize: 11, background: "#fff", border: "1px solid #e2e8f0", borderRadius: 8, padding: 10 }}>
                                {JSON.stringify(candidate.snapshot_config, null, 2)}
                              </pre>
                            </div>
                            <div>
                              <div style={{ fontSize: 11, fontWeight: 700, color: "#64748b", textTransform: "uppercase", marginBottom: 8 }}>完整性 / 审计</div>
                              <pre style={{ margin: 0, maxHeight: 220, overflow: "auto", fontSize: 11, background: "#fff", border: "1px solid #e2e8f0", borderRadius: 8, padding: 10 }}>
                                {JSON.stringify({ completeness: candidate.completeness, eligibility: candidate.eligibility, audit_context: candidate.audit_context }, null, 2)}
                              </pre>
                            </div>
                          </div>
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                );
              })}
              {items.length === 0 && (
                <tr>
                  <td colSpan={8} style={{ padding: "20px", textAlign: "center", color: "#94a3b8" }}>
                    暂无候选策略包。请在 QE experiment 或 QE loop 详情页点击“加入候选策略包”。
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
