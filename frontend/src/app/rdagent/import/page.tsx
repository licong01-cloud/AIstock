"use client";

import { useEffect, useState } from "react";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

interface Candidate {
  task_run_id: string;
  scenario: string | null;
  loop_id: number;
  action: string | null;
  has_result: boolean | null;
  best_workspace_id: string;
  workspace_path: string;
  manifest_path: string | null;
  summary_path: string | null;
  ic_mean: number | null;
  ann_return: number | null;
  mdd: number | null;
  turnover: number | null;
  multi_score: number | null;
  workspace_role?: string | null;
  experiment_type?: string | null;
  has_signals: boolean;
}

interface CandidateDetailResponse {
  candidate: {
    task_run_id: string;
    loop_id: number;
    workspace_id: string;
    workspace_path: string;
    workspace_role?: string | null;
    experiment_type?: string | null;
    manifest_path?: string | null;
    summary_path?: string | null;
    manifest_json?: any;
    summary_json?: any;
  };
}

interface ImportResult {
  ok: boolean;
  strategy_id: string;
  strategy_version_id: string;
  artifact_root_path: string;
  strategy_kind: string;
  output_mode: string;
}

export default function RDagentImportPage() {
  const [candidates, setCandidates] = useState<Candidate[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [importing, setImporting] = useState(false);
  const [selected, setSelected] = useState<Candidate | null>(null);
  const [strategyName, setStrategyName] = useState("");
  const [strategyKind, setStrategyKind] = useState("portfolio");
  const [outputMode, setOutputMode] = useState("target_weight");
  const [enabled, setEnabled] = useState(true);
  const [lastResult, setLastResult] = useState<ImportResult | null>(null);

  const [detailOpen, setDetailOpen] = useState(false);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [detail, setDetail] = useState<CandidateDetailResponse | null>(null);

  useEffect(() => {
    loadCandidates();
  }, []);

  async function loadCandidates() {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`${API_BASE}/rdagent/candidates?mode=signals`);
      if (!res.ok) throw new Error(`加载候选失败: ${res.status}`);
      const data = await res.json();
      setCandidates(data.candidates || data.items || []);
    } catch (e: any) {
      setError(e?.message || "加载失败");
    } finally {
      setLoading(false);
    }
  }

  function openImportDialog(c: Candidate) {
    setSelected(c);
    setStrategyName(
      `rdagent_${c.task_run_id.slice(0, 8)}_loop${c.loop_id}_${c.best_workspace_id.slice(0, 8)}`,
    );
    setLastResult(null);
  }

  async function openDetail(c: Candidate) {
    setDetailOpen(true);
    setDetail(null);
    setDetailError(null);
    setDetailLoading(true);
    try {
      const qs = new URLSearchParams({
        task_run_id: c.task_run_id,
        loop_id: String(c.loop_id),
        workspace_id: c.best_workspace_id,
      });
      const res = await fetch(`${API_BASE}/rdagent/candidates/detail?${qs.toString()}`);
      if (!res.ok) throw new Error(`加载详情失败: ${res.status}`);
      const data = (await res.json()) as CandidateDetailResponse;
      setDetail(data);
    } catch (e: any) {
      setDetailError(e?.message || "加载详情失败");
    } finally {
      setDetailLoading(false);
    }
  }

  function fmtMetric(v: number | null | undefined) {
    if (v === null || v === undefined) return "-";
    if (!Number.isFinite(v)) return "-";
    return v.toFixed(6);
  }

  async function doImport() {
    if (!selected) return;
    setImporting(true);
    setError(null);
    try {
      const body = {
        task_run_id: selected.task_run_id,
        loop_id: selected.loop_id,
        workspace_id: selected.best_workspace_id,
        strategy_name: strategyName || undefined,
        strategy_kind: strategyKind || undefined,
        output_mode: outputMode || undefined,
        enabled,
      };
      const res = await fetch(`${API_BASE}/rdagent/import`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!res.ok) {
        const payload = await res.json().catch(() => null);
        throw new Error(payload?.detail || `导入失败: ${res.status}`);
      }
      const data: ImportResult = await res.json();
      setLastResult(data);
      alert("导入成功，已写入策略库和 signals 表");
      setSelected(null);
    } catch (e: any) {
      setError(e?.message || "导入失败");
    } finally {
      setImporting(false);
    }
  }

  return (
    <main style={{ padding: 24 }}>
      <section
        style={{
          background: "linear-gradient(135deg, #22c55e 0%, #16a34a 100%)",
          borderRadius: 16,
          padding: 20,
          color: "#fff",
          marginBottom: 16,
        }}
      >
        <h1 style={{ margin: 0, fontSize: 24 }}>RD-Agent 候选导入</h1>
        <p style={{ marginTop: 8, opacity: 0.9, fontSize: 13 }}>
          从 RD-Agent registry.sqlite 发现含 signals 的 workspace，并手动选择导入到 AIstock
        </p>
      </section>

      {error && (
        <div
          style={{
            padding: 12,
            background: "#fee2e2",
            border: "1px solid #fecaca",
            borderRadius: 8,
            marginBottom: 16,
            color: "#b91c1c",
          }}
        >
          {error}
        </div>
      )}

      <section
        style={{
          background: "#fff",
          borderRadius: 12,
          padding: 20,
          boxShadow: "0 1px 3px rgba(0,0,0,0.08)",
        }}
      >
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            marginBottom: 16,
          }}
        >
          <h2 style={{ margin: 0 }}>含 signals 的候选 workspace</h2>
          <button
            type="button"
            onClick={loadCandidates}
            style={{
              padding: "6px 12px",
              borderRadius: 6,
              border: "1px solid #e5e7eb",
              background: "#f9fafb",
              cursor: "pointer",
              fontSize: 13,
            }}
          >
            刷新
          </button>
        </div>

        {loading ? (
          <div>加载中...</div>
        ) : candidates.length === 0 ? (
          <div style={{ padding: 40, textAlign: "center", color: "#6b7280" }}>
            未在 registry 中发现含 signals 的 workspace。
          </div>
        ) : (
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead>
                <tr style={{ background: "#f9fafb" }}>
                  <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #e5e7eb" }}>task_run</th>
                  <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #e5e7eb" }}>loop</th>
                  <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #e5e7eb" }}>action</th>
                  <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #e5e7eb" }}>信息</th>
                  <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #e5e7eb" }}>信息系数（IC）</th>
                  <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #e5e7eb" }}>年化收益（ANN）</th>
                  <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #e5e7eb" }}>最大回撤（MDD）</th>
                  <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #e5e7eb" }}>workspace</th>
                  <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #e5e7eb" }}>signals</th>
                  <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #e5e7eb" }}>操作</th>
                </tr>
              </thead>
              <tbody>
                {candidates.map((c) => (
                  <tr key={`${c.task_run_id}-${c.loop_id}-${c.best_workspace_id}`} style={{ borderBottom: "1px solid #e5e7eb" }}>
                    <td style={{ padding: 12 }}>{c.task_run_id}</td>
                    <td style={{ padding: 12 }}>{c.loop_id}</td>
                    <td style={{ padding: 12 }}>{c.action || "-"}</td>
                    <td style={{ padding: 12, fontSize: 12, color: "#374151" }}>
                      <div>角色: {c.workspace_role ?? "-"}</div>
                      <div>类型: {c.experiment_type ?? "-"}</div>
                    </td>
                    <td style={{ padding: 12, fontSize: 12 }}>{fmtMetric(c.ic_mean)}</td>
                    <td style={{ padding: 12, fontSize: 12 }}>{fmtMetric(c.ann_return)}</td>
                    <td style={{ padding: 12, fontSize: 12 }}>{fmtMetric(c.mdd)}</td>
                    <td style={{ padding: 12, fontSize: 12, color: "#6b7280" }}>
                      {c.workspace_path}
                    </td>
                    <td style={{ padding: 12 }}>
                      {c.has_signals ? (
                        <span
                          style={{
                            padding: "4px 8px",
                            borderRadius: 999,
                            backgroundColor: "#dcfce7",
                            color: "#166534",
                            fontSize: 12,
                          }}
                        >
                          已检测到
                        </span>
                      ) : (
                        <span
                          style={{
                            padding: "4px 8px",
                            borderRadius: 999,
                            backgroundColor: "#fee2e2",
                            color: "#991b1b",
                            fontSize: 12,
                          }}
                        >
                          未发现
                        </span>
                      )}
                    </td>
                    <td style={{ padding: 12 }}>
                      <button
                        type="button"
                        onClick={() => openDetail(c)}
                        style={{
                          padding: "6px 12px",
                          borderRadius: 6,
                          border: "1px solid #e5e7eb",
                          background: "#f9fafb",
                          cursor: "pointer",
                          fontSize: 12,
                          marginRight: 8,
                        }}
                      >
                        查看
                      </button>
                      <button
                        type="button"
                        onClick={() => openImportDialog(c)}
                        style={{
                          padding: "6px 12px",
                          borderRadius: 6,
                          border: "none",
                          background: "#4f46e5",
                          color: "#fff",
                          cursor: "pointer",
                          fontSize: 12,
                        }}
                      >
                        导入
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      {detailOpen && (
        <div
          style={{
            position: "fixed",
            inset: 0,
            background: "rgba(0,0,0,0.45)",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            zIndex: 1000,
          }}
        >
          <div
            style={{
              background: "#fff",
              borderRadius: 12,
              padding: 24,
              width: "92%",
              maxWidth: 900,
              maxHeight: "90vh",
              overflow: "auto",
            }}
          >
            <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "center" }}>
              <h2 style={{ marginTop: 0, marginBottom: 12 }}>候选详情（待导入信息预览）</h2>
              <button
                type="button"
                onClick={() => setDetailOpen(false)}
                style={{
                  padding: "6px 12px",
                  borderRadius: 6,
                  border: "1px solid #e5e7eb",
                  background: "#f9fafb",
                  cursor: "pointer",
                  fontSize: 13,
                }}
              >
                关闭
              </button>
            </div>

            {detailError && (
              <div
                style={{
                  padding: 12,
                  background: "#fee2e2",
                  border: "1px solid #fecaca",
                  borderRadius: 8,
                  marginBottom: 16,
                  color: "#b91c1c",
                }}
              >
                {detailError}
              </div>
            )}

            {detailLoading ? (
              <div>加载中...</div>
            ) : !detail ? (
              <div style={{ color: "#6b7280" }}>暂无数据</div>
            ) : (
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
                <div style={{ border: "1px solid #e5e7eb", borderRadius: 10, padding: 12 }}>
                  <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 8 }}>基本信息</div>
                  <div style={{ fontSize: 12, color: "#374151" }}>
                    <div>task_run_id: {detail.candidate.task_run_id}</div>
                    <div>loop_id: {detail.candidate.loop_id}</div>
                    <div>workspace_id: {detail.candidate.workspace_id}</div>
                    <div>workspace_role: {detail.candidate.workspace_role ?? "-"}</div>
                    <div>experiment_type: {detail.candidate.experiment_type ?? "-"}</div>
                    <div style={{ marginTop: 6, color: "#6b7280" }}>
                      workspace_path: {detail.candidate.workspace_path}
                    </div>
                  </div>
                </div>

                <div style={{ border: "1px solid #e5e7eb", borderRadius: 10, padding: 12 }}>
                  <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 8 }}>文件入口</div>
                  <div style={{ fontSize: 12, color: "#374151" }}>
                    <div>manifest: {detail.candidate.manifest_path ?? "-"}</div>
                    <div>summary: {detail.candidate.summary_path ?? "-"}</div>
                  </div>
                </div>

                <div style={{ border: "1px solid #e5e7eb", borderRadius: 10, padding: 12, gridColumn: "1 / -1" }}>
                  <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 8 }}>summary.json（预览）</div>
                  <pre
                    style={{
                      margin: 0,
                      fontSize: 12,
                      background: "#f9fafb",
                      border: "1px solid #eef2f7",
                      padding: 10,
                      borderRadius: 8,
                      overflow: "auto",
                      maxHeight: 280,
                    }}
                  >
                    {JSON.stringify(detail.candidate.summary_json ?? null, null, 2)}
                  </pre>
                </div>

                <div style={{ border: "1px solid #e5e7eb", borderRadius: 10, padding: 12, gridColumn: "1 / -1" }}>
                  <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 8 }}>manifest.json（预览）</div>
                  <pre
                    style={{
                      margin: 0,
                      fontSize: 12,
                      background: "#f9fafb",
                      border: "1px solid #eef2f7",
                      padding: 10,
                      borderRadius: 8,
                      overflow: "auto",
                      maxHeight: 280,
                    }}
                  >
                    {JSON.stringify(detail.candidate.manifest_json ?? null, null, 2)}
                  </pre>
                </div>
              </div>
            )}
          </div>
        </div>
      )}

      {selected && (
        <div
          style={{
            position: "fixed",
            inset: 0,
            background: "rgba(0,0,0,0.45)",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            zIndex: 1000,
          }}
        >
          <div
            style={{
              background: "#fff",
              borderRadius: 12,
              padding: 24,
              width: "90%",
              maxWidth: 520,
              maxHeight: "90vh",
              overflow: "auto",
            }}
          >
            <h2 style={{ marginTop: 0, marginBottom: 12 }}>导入 RD-Agent 候选</h2>
            <p style={{ fontSize: 13, color: "#4b5563", marginBottom: 12 }}>
              task_run={selected.task_run_id}, loop={selected.loop_id}, workspace={
                selected.best_workspace_id
              }
            </p>

            <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
              <label style={{ fontSize: 13 }}>
                策略名称
                <input
                  type="text"
                  value={strategyName}
                  onChange={(e) => setStrategyName(e.target.value)}
                  style={{
                    width: "100%",
                    marginTop: 4,
                    padding: "6px 10px",
                    borderRadius: 6,
                    border: "1px solid #e5e7eb",
                  }}
                />
              </label>

              <label style={{ fontSize: 13 }}>
                策略形态
                <select
                  value={strategyKind}
                  onChange={(e) => setStrategyKind(e.target.value)}
                  style={{
                    width: "100%",
                    marginTop: 4,
                    padding: "6px 10px",
                    borderRadius: 6,
                    border: "1px solid #e5e7eb",
                  }}
                >
                  <option value="portfolio">portfolio（组合）</option>
                  <option value="single_symbol">single_symbol（单标的）</option>
                </select>
              </label>

              <label style={{ fontSize: 13 }}>
                输出模式
                <select
                  value={outputMode}
                  onChange={(e) => setOutputMode(e.target.value)}
                  style={{
                    width: "100%",
                    marginTop: 4,
                    padding: "6px 10px",
                    borderRadius: 6,
                    border: "1px solid #e5e7eb",
                  }}
                >
                  <option value="target_weight">target_weight</option>
                  <option value="topk">topk</option>
                </select>
              </label>

              <label style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 13 }}>
                <input
                  type="checkbox"
                  checked={enabled}
                  onChange={(e) => setEnabled(e.target.checked)}
                />
                <span>导入后启用该策略</span>
              </label>
            </div>

            {lastResult && (
              <div
                style={{
                  marginTop: 12,
                  padding: 10,
                  borderRadius: 8,
                  background: "#ecfdf5",
                  color: "#166534",
                  fontSize: 12,
                }}
              >
                <div>导入成功：</div>
                <div>strategy_id: {lastResult.strategy_id}</div>
                <div>strategy_version_id: {lastResult.strategy_version_id}</div>
                <div>artifact_root: {lastResult.artifact_root_path}</div>
                <div>
                  kind={lastResult.strategy_kind}, output={lastResult.output_mode}
                </div>
              </div>
            )}

            <div
              style={{
                display: "flex",
                justifyContent: "flex-end",
                gap: 8,
                marginTop: 16,
              }}
            >
              <button
                type="button"
                onClick={() => setSelected(null)}
                disabled={importing}
                style={{
                  padding: "6px 12px",
                  borderRadius: 6,
                  border: "1px solid #e5e7eb",
                  background: "#f9fafb",
                  cursor: "pointer",
                  fontSize: 13,
                }}
              >
                取消
              </button>
              <button
                type="button"
                onClick={doImport}
                disabled={importing}
                style={{
                  padding: "6px 12px",
                  borderRadius: 6,
                  border: "none",
                  background: "#16a34a",
                  color: "#fff",
                  cursor: "pointer",
                  fontSize: 13,
                }}
              >
                {importing ? "导入中..." : "确认导入"}
              </button>
            </div>
          </div>
        </div>
      )}
    </main>
  );
}
